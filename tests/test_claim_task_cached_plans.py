"""claim_task() runs through a per-queue function with static (plan-cached) SQL.

Every behaviour scenario runs three times: through the generated
``absurd.ct_<queue>`` function with ``plan_cache_mode`` forced to a custom plan
and to a generic plan (the plan PL/pgSQL caches once a statement has run a few
times), and through claim_task()'s dynamic fallback (the per-queue function
dropped, as for a queue created before the function existed).  All must produce
the same observable result.
"""

import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import psycopg.conninfo
import pytest
from psycopg import sql

BASE = datetime(2024, 8, 1, 12, 0, tzinfo=timezone.utc)
TESTS = Path(__file__).resolve().parent
MIGRATIONS = TESTS.parent / "sql" / "migrations"
# sql/absurd.sql exactly as released in 0.5.0 (git show 550d3b9:sql/absurd.sql).
ABSURD_0_5_0 = TESTS / "fixtures" / "absurd-0.5.0.sql"


def claim_fn_exists(client, queue):
    row = client.conn.execute(
        """
        select count(*)
        from pg_proc p
        where p.proname = %s and p.pronamespace = 'absurd'::regnamespace
        """,
        (f"ct_{queue}",),
    ).fetchone()
    return row[0] == 1


def drop_claim_fn(client, queue):
    client.conn.execute(
        sql.SQL("drop function absurd.{}(text, integer, integer)").format(
            sql.Identifier(f"ct_{queue}")
        )
    )


@pytest.fixture(params=("custom_plan", "generic_plan", "fallback"))
def make_queue(request, client):
    def make(name, storage_mode=None):
        client.create_queue(name, storage_mode=storage_mode)
        assert claim_fn_exists(client, name)
        if request.param == "fallback":
            drop_claim_fn(client, name)
            assert not claim_fn_exists(client, name)
        else:
            mode = "force_" + request.param
            client.conn.execute(
                "select set_config('plan_cache_mode', %s, false)", (mode,)
            )
        return name

    return make


def run_states(client, queue, task_id):
    return [r["state"] for r in client.get_runs(queue, task_id)]


def install_sentinel(conn, queue):
    """Replace ct_<queue> with a function that reveals it was the one called."""
    conn.execute(
        f"""
        create or replace function absurd.{_ident(f"ct_{queue}")} (
          p_worker_id text, p_claim_timeout integer default 30, p_qty integer default 1
        )
          returns table (
            run_id uuid, task_id uuid, attempt integer, task_name text,
            params jsonb, retry_strategy jsonb, max_attempts integer,
            headers jsonb, wake_event text, event_payload jsonb
          )
          language sql
        as $$
          select null::uuid, null::uuid, 0,
                 'sentinel ' || p_worker_id || p_claim_timeout || p_qty,
                 null::jsonb, null::jsonb, null::integer, null::jsonb,
                 null::text, null::jsonb
        $$
        """
    )


def _ident(name):
    return '"' + name.replace('"', '""') + '"'


def claim_fn_owner(conn, queue):
    row = conn.execute(
        """
        select pg_get_userbyid(p.proowner)
        from pg_proc p
        where p.proname = %s and p.pronamespace = 'absurd'::regnamespace
        """,
        (f"ct_{queue}",),
    ).fetchone()
    return row[0] if row else None


def table_owner(conn, queue):
    return conn.execute(
        "select pg_get_userbyid(relowner) from pg_class "
        "where relnamespace = 'absurd'::regnamespace and relname = %s",
        (f"t_{queue}",),
    ).fetchone()[0]


@pytest.fixture
def admin(db_dsn):
    """A committing superuser session, for state other sessions must see.

    The shared test database is rolled back per test; anything this fixture's
    users create (queues, roles) is removed again by the test.
    """
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        yield conn


@pytest.fixture
def new_database(db_dsn):
    """Create empty databases on the test server; they are dropped afterwards.

    Used where a test needs a schema of its own (an older release, or
    privileges revoked schema-wide) that must not leak into the shared one.
    """
    created = []

    def create(name):
        with psycopg.connect(db_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("create database {}").format(sql.Identifier(name)))
        created.append(name)
        return psycopg.conninfo.make_conninfo(db_dsn, dbname=name)

    try:
        yield create
    finally:
        with psycopg.connect(db_dsn, autocommit=True) as conn:
            for name in created:
                conn.execute(
                    sql.SQL("drop database if exists {} with (force)").format(
                        sql.Identifier(name)
                    )
                )


def drop_roles(conn, *roles):
    for role in roles:
        conn.execute(sql.SQL("drop owned by {}").format(sql.Identifier(role)))
        conn.execute(sql.SQL("drop role {}").format(sql.Identifier(role)))


def as_role(db_dsn, role):
    conn = psycopg.connect(db_dsn, autocommit=True)
    conn.execute(sql.SQL("set role {}").format(sql.Identifier(role)))
    return conn


# -- lifecycle of the generated function ------------------------------------


@pytest.mark.parametrize("storage_mode", ("unpartitioned", "partitioned"))
def test_create_queue_creates_and_drop_queue_removes_claim_function(
    client, storage_mode
):
    queue = f"cf_{storage_mode}"
    client.create_queue(queue, storage_mode=storage_mode)
    assert claim_fn_exists(client, queue)

    # create_queue on an existing queue is idempotent.
    client.create_queue(queue, storage_mode=storage_mode)
    assert claim_fn_exists(client, queue)

    client.drop_queue(queue)
    assert not claim_fn_exists(client, queue)


def claim_fn_version(conn, queue):
    """The pg_proc row of ct_<queue>: oid and the xmin of its current version."""
    return conn.execute(
        "select p.oid, p.xmin::text from pg_proc p "
        "where p.proname = %s and p.pronamespace = 'absurd'::regnamespace",
        (f"ct_{queue}",),
    ).fetchone()


@pytest.mark.parametrize("storage_mode", ("unpartitioned", "partitioned"))
def test_repeated_create_queue_does_not_rewrite_claim_function(admin, storage_mode):
    # Replacing an unchanged function would write a new pg_proc row version
    # (and invalidate every session's cached claim plans) on each create_queue.
    queue = f"cf_norewrite_{storage_mode}"
    try:
        admin.execute("select absurd.create_queue(%s, %s)", (queue, storage_mode))
        before = claim_fn_version(admin, queue)
        assert before is not None

        admin.execute("select absurd.create_queue(%s, %s)", (queue, storage_mode))
        admin.execute("select absurd.ensure_claim_function(%s)", (queue,))
        assert claim_fn_version(admin, queue) == before

        # A function that differs from the generated one is still replaced.
        install_sentinel(admin, queue)
        planted = claim_fn_version(admin, queue)
        admin.execute("select absurd.create_queue(%s, %s)", (queue, storage_mode))
        assert claim_fn_version(admin, queue) != planted
        prosrc = admin.execute(
            "select prosrc from pg_proc where oid = %s", (planted[0],)
        ).fetchone()[0]
        assert "sentinel" not in prosrc
    finally:
        admin.execute("select absurd.drop_queue(%s)", (queue,))


def test_concurrent_create_queue_on_an_existing_queue(admin, db_dsn):
    # Two transactions repeating create_queue for an existing queue must not
    # both try to replace ct_<queue>: the second would wait for the first and
    # then fail with "tuple concurrently updated".
    queue = "cf_concurrent"
    admin.execute("select absurd.create_queue(%s)", (queue,))
    try:
        with psycopg.connect(db_dsn) as first, psycopg.connect(db_dsn) as second:
            first.execute("select absurd.create_queue(%s)", (queue,))  # open txn
            second_pid = second.info.backend_pid
            outcome = {}

            def repeat_create():
                try:
                    second.execute("select absurd.create_queue(%s)", (queue,))
                    second.commit()
                    outcome["ok"] = True
                except psycopg.Error as exc:  # reported below
                    second.rollback()
                    outcome["error"] = exc

            worker = threading.Thread(target=repeat_create, daemon=True)
            worker.start()
            # Give the second transaction time to block on the first, if it is
            # going to.
            deadline = time.monotonic() + 5
            while worker.is_alive() and time.monotonic() < deadline:
                waiting = admin.execute(
                    "select wait_event_type = 'Lock' from pg_stat_activity where pid = %s",
                    (second_pid,),
                ).fetchone()
                if waiting and waiting[0]:
                    break
                time.sleep(0.05)
            first.commit()
            worker.join(10)
            assert not worker.is_alive()
            assert outcome == {"ok": True}, outcome.get("error")
    finally:
        admin.execute("select absurd.drop_queue(%s)", (queue,))


def test_claim_function_is_static_sql_and_does_not_widen_privileges(client):
    queue = "cf_static"
    client.create_queue(queue)

    row = client.conn.execute(
        """
        select p.prosrc, p.prosecdef, p.proacl, p.proowner,
               (select relowner from pg_class where oid = 'absurd.t_cf_static'::regclass)
        from pg_proc p
        where p.proname = 'ct_cf_static' and p.pronamespace = 'absurd'::regnamespace
        """
    ).fetchone()
    prosrc, secdef, acl, owner, table_owner = row

    # No dynamic SQL: that is what makes plpgsql cache the plans.
    assert "execute" not in prosrc.lower()
    assert "format(" not in prosrc.lower()
    # Same privilege model as the rest of Absurd: invoker rights, default ACL,
    # owned by whoever owns the queue tables.
    assert secdef is False
    assert acl is None
    assert owner == table_owner


def test_claim_function_name_limits(client):
    # The longest name validate_queue_name() accepts still gets a function.
    longest = "q" * 53
    client.create_queue(longest)
    assert claim_fn_exists(client, longest)

    # A longer name is refused rather than given a truncated function name.
    with pytest.raises(psycopg.errors.RaiseException, match="too long"):
        client.conn.execute("select absurd.ensure_claim_function(%s)", ("l" * 54,))
    client.conn.rollback()


def test_claim_task_dispatches_to_per_queue_function_and_falls_back(client):
    queue = "cf_dispatch"
    client.create_queue(queue)
    client.spawn_task(queue, "real", {})

    # Replace the generated function with a sentinel: claim_task must call it.
    install_sentinel(client.conn, queue)
    rows = client.claim_tasks(queue, worker="w", claim_timeout=7, qty=3)
    assert [r["task_name"] for r in rows] == ["sentinel w73"]

    # Without the function the dynamic body serves the claim.
    drop_claim_fn(client, queue)
    rows = client.claim_tasks(queue, worker="w")
    assert [r["task_name"] for r in rows] == ["real"]


def test_unknown_queue_still_raises_undefined_table(client):
    with pytest.raises(psycopg.errors.UndefinedTable):
        client.claim_tasks("cf_missing_queue")


def test_migration_creates_claim_functions_for_existing_queues(client):
    migration = migration_sql()
    queues = ["cf_mig_a", "cf_mig_b"]
    for q in queues:
        client.create_queue(q)
    client.create_queue("cf_mig_p", storage_mode="partitioned")
    queues.append("cf_mig_p")
    for q in queues:
        drop_claim_fn(client, q)  # a queue as created by the previous release

    client.conn.execute(migration)

    for q in queues:
        assert claim_fn_exists(client, q)
        client.spawn_task(q, "t", {})
        assert [r["task_name"] for r in client.claim_tasks(q)] == ["t"]


def migration_sql():
    return next(
        p
        for p in sorted(MIGRATIONS.glob("*.sql"))
        if "ensure_claim_function" in p.read_text()
    ).read_text()


# -- privileges and ownership -------------------------------------------------


def test_claim_task_uses_per_queue_function_only_when_caller_may_execute_it(
    admin, db_dsn
):
    # Installations that revoke PUBLIC EXECUTE and grant workers EXECUTE on the
    # API functions grant nothing on ct_<queue>; claim_task() is SECURITY
    # INVOKER, so dispatching anyway would fail with permission denied.
    queue = "cf_priv"
    role = "cf_priv_worker"
    admin.execute("select absurd.create_queue(%s)", (queue,))
    try:
        admin.execute(sql.SQL("create role {}").format(sql.Identifier(role)))
        admin.execute(
            sql.SQL("grant usage on schema absurd to {}").format(sql.Identifier(role))
        )
        for prefix in "trcew":
            admin.execute(
                sql.SQL(
                    "grant select, insert, update, delete on absurd.{} to {}"
                ).format(sql.Identifier(f"{prefix}_{queue}"), sql.Identifier(role))
            )
        ct = sql.SQL("absurd.{}(text, integer, integer)").format(
            sql.Identifier(f"ct_{queue}")
        )
        # The marker lets the test see which path served a claim.
        install_sentinel(admin, queue)
        admin.execute(sql.SQL("revoke execute on function {} from public").format(ct))

        spawn = lambda name: admin.execute(  # noqa: E731
            "select absurd.spawn_task(%s, %s, '{}')", (queue, name)
        )

        def claim():
            with as_role(db_dsn, role) as worker:
                return [
                    r[3]
                    for r in worker.execute(
                        "select * from absurd.claim_task(%s, 'w', 60, 1)", (queue,)
                    ).fetchall()
                ]

        # No EXECUTE on ct_<queue>: served by the dynamic statements.
        spawn("slow-1")
        assert claim() == ["slow-1"]

        # With EXECUTE it takes the generated function.
        admin.execute(
            sql.SQL("grant execute on function {} to {}").format(
                ct, sql.Identifier(role)
            )
        )
        spawn("ignored")
        assert claim() == ["sentinel w601"]

        # Revoking it again goes back to the dynamic statements.
        admin.execute(
            sql.SQL("revoke execute on function {} from {}").format(
                ct, sql.Identifier(role)
            )
        )
        assert claim() == ["ignored"]
    finally:
        admin.execute("select absurd.drop_queue(%s)", (queue,))
        drop_roles(admin, role)


def test_claim_task_ignores_per_queue_function_with_other_signature(client):
    queue = "cf_sig"
    client.create_queue(queue)
    client.spawn_task(queue, "real", {})
    drop_claim_fn(client, queue)
    client.conn.execute(
        sql.SQL(
            "create function absurd.{}(text) returns void language sql as 'select'"
        ).format(sql.Identifier(f"ct_{queue}"))
    )
    assert [r["task_name"] for r in client.claim_tasks(queue)] == ["real"]


def test_claim_task_ignores_per_queue_function_owned_by_an_unrelated_role(
    admin, db_dsn
):
    # A role that merely holds CREATE on schema absurd can create ct_<queue> for
    # a queue that has none.  claim_task() runs it with the caller's privileges,
    # so it must not be dispatched to.  The check is limited to this case: the
    # function of the table owner (who can already do the same through a
    # trigger) is, and so is one handed to the table owner.
    queue, planter = "cf_plant", "cf_planter"
    admin.execute("select absurd.create_queue(%s)", (queue,))
    try:
        admin.execute(sql.SQL("create role {}").format(sql.Identifier(planter)))
        admin.execute(
            sql.SQL("grant usage, create on schema absurd to {}").format(
                sql.Identifier(planter)
            )
        )
        ct = sql.SQL("absurd.{}(text, integer, integer)").format(
            sql.Identifier(f"ct_{queue}")
        )
        spawn = lambda name: admin.execute(  # noqa: E731
            "select absurd.spawn_task(%s, %s, '{}')", (queue, name)
        )
        claim = lambda: [  # noqa: E731
            r[3]
            for r in admin.execute(
                "select * from absurd.claim_task(%s, 'w', 60, 1)", (queue,)
            ).fetchall()
        ]

        admin.execute(sql.SQL("drop function {}").format(ct))
        with as_role(db_dsn, planter) as conn:
            install_sentinel(conn, queue)
        assert claim_fn_owner(admin, queue) == planter
        assert table_owner(admin, queue) != planter

        spawn("served by the statements in claim_task")
        assert claim() == ["served by the statements in claim_task"]

        # Handed to the owner of the queue's tables, it is used.
        admin.execute(
            sql.SQL("alter function {} owner to {}").format(
                ct, sql.Identifier(table_owner(admin, queue))
            )
        )
        spawn("ignored")
        assert claim() == ["sentinel w601"]
    finally:
        admin.execute("select absurd.drop_queue(%s)", (queue,))
        drop_roles(admin, planter)


def test_claim_function_is_owned_by_the_queue_owner_across_roles(admin, db_dsn):
    # A queue owner that is neither a superuser nor the role that installs or
    # migrates Absurd must be able to create_queue (idempotently) and
    # drop_queue, which replace and drop ct_<queue>.
    queue = "cf_own"
    owner, migrator, other = "cf_own_owner", "cf_own_migrator", "cf_own_other"
    try:
        admin.execute(
            sql.SQL("create role {} superuser").format(sql.Identifier(migrator))
        )
        for role in (owner, other):
            admin.execute(sql.SQL("create role {}").format(sql.Identifier(role)))
            admin.execute(
                sql.SQL("grant usage, create on schema absurd to {}").format(
                    sql.Identifier(role)
                )
            )
            admin.execute(
                sql.SQL(
                    "grant select, insert, update, delete on absurd.queues to {}"
                ).format(sql.Identifier(role))
            )

        with as_role(db_dsn, owner) as conn:
            conn.execute("select absurd.create_queue(%s)", (queue,))
            assert claim_fn_owner(conn, queue) == owner
            # The queue as created by the previous release: no function.
            conn.execute(
                sql.SQL("drop function absurd.{}(text, integer, integer)").format(
                    sql.Identifier(f"ct_{queue}")
                )
            )
        assert claim_fn_owner(admin, queue) is None

        # A different role applies the migration; it must not end up owning
        # the queue's function.
        with as_role(db_dsn, migrator) as conn:
            conn.execute(migration_sql())
        assert table_owner(admin, queue) == owner
        assert claim_fn_owner(admin, queue) == owner

        with as_role(db_dsn, owner) as conn:
            conn.execute("select absurd.create_queue(%s)", (queue,))  # replaces it
            assert claim_fn_owner(conn, queue) == owner

        # A role that owns neither the tables nor the function, and cannot
        # become the owner, neither breaks nor replaces it.
        with as_role(db_dsn, other) as conn:
            conn.execute("select absurd.ensure_claim_function(%s)", (queue,))
        assert claim_fn_owner(admin, queue) == owner

        with as_role(db_dsn, owner) as conn:
            conn.execute("select absurd.drop_queue(%s)", (queue,))
        assert claim_fn_owner(admin, queue) is None

        # ... and without a function it creates none (the caller could not give
        # it to the table owner), leaving claim_task() on its dynamic path.
        with as_role(db_dsn, owner) as conn:
            conn.execute("select absurd.create_queue(%s)", (queue,))
            conn.execute(
                sql.SQL("drop function absurd.{}(text, integer, integer)").format(
                    sql.Identifier(f"ct_{queue}")
                )
            )
        with as_role(db_dsn, other) as conn:
            conn.execute("select absurd.ensure_claim_function(%s)", (queue,))
        assert claim_fn_owner(admin, queue) is None
        admin.execute("select absurd.spawn_task(%s, 't', '{}')", (queue,))
        assert [
            r[3]
            for r in admin.execute("select * from absurd.claim_task(%s, 'w')", (queue,))
        ] == ["t"]
    finally:
        admin.execute("select absurd.drop_queue(%s)", (queue,))
        drop_roles(admin, owner, other, migrator)


# -- queue names that are hostile to quoting ----------------------------------


@pytest.mark.parametrize(
    "queue",
    ("dq$fn$x", "dq$body$x", "dq$$x", "dq'x", 'dq"x', "dq\\x", "$fn$'\"$$"),
)
def test_hostile_queue_names_get_a_working_claim_function(client, queue):
    # validate_queue_name() only checks length, so any of these is a valid
    # queue name and must not break out of the generated CREATE FUNCTION.
    client.create_queue(queue)
    assert claim_fn_exists(client, queue)
    spawn = client.spawn_task(queue, "t", {})
    assert [r["run_id"] for r in client.claim_tasks(queue)] == [spawn.run_id]
    assert client.claim_tasks(queue) == []

    migration = migration_sql()
    drop_claim_fn(client, queue)
    client.conn.execute(migration)  # the backfill creates it again
    assert claim_fn_exists(client, queue)

    client.drop_queue(queue)
    assert not claim_fn_exists(client, queue)


def test_migration_renames_existing_partitions_to_year_unique_tags(client):
    queue = "cf_mig_tags"
    client.create_queue(queue, storage_mode="partitioned")
    weekly = r"^[trcw]_cf_mig_tags_[0-9]{6}$"
    names = [
        r[0]
        for r in client.conn.execute(
            "select relname from pg_class where relname ~ %s order by 1", (weekly,)
        )
    ]
    assert len(names) >= 4 and len(names) % 4 == 0

    # Partitions as named by 0.5.0: the tag was one year digit plus the week.
    for name in names:
        client.conn.execute(
            sql.SQL("alter table absurd.{} rename to {}").format(
                sql.Identifier(name), sql.Identifier(name[:-6] + name[-3:])
            )
        )

    client.conn.execute(migration_sql())

    renamed = [
        r[0]
        for r in client.conn.execute(
            "select relname from pg_class where relname ~ %s order by 1", (weekly,)
        )
    ]
    assert renamed == names
    # No overlapping partition is created next to the existing ones.
    client.conn.execute("select absurd.ensure_partitions(%s)", (queue,))
    count = client.conn.execute(
        "select count(*) from pg_class where relname ~ '^[trcw]_cf_mig_tags_[0-9]+$'"
    ).fetchone()[0]
    assert count == len(names)


@pytest.mark.parametrize("plan_cache_mode", ("force_custom_plan", "force_generic_plan"))
def test_repeated_polls_on_one_session(client, plan_cache_mode):
    # Behavioural companion to the static-SQL assertion above: many polls on a
    # single session (plpgsql plan cache warm) keep returning the right answer.
    client.conn.execute(
        "select set_config('plan_cache_mode', %s, false)", (plan_cache_mode,)
    )
    client.conn.commit()
    queue = "cf_poll"
    client.create_queue(queue)
    for _ in range(25):
        assert client.claim_tasks(queue) == []
    spawn = client.spawn_task(queue, "late", {})
    rows = client.claim_tasks(queue)
    assert [r["run_id"] for r in rows] == [spawn.run_id]
    for _ in range(25):
        assert client.claim_tasks(queue) == []


# -- behaviour, identical on both paths --------------------------------------


@pytest.mark.parametrize("storage_mode", ("unpartitioned", "partitioned"))
def test_claim_orders_by_available_at_then_run_id_and_honours_qty(
    make_queue, client, storage_mode
):
    queue = make_queue(f"ord_{storage_mode}", storage_mode)

    client.set_fake_now(BASE + timedelta(seconds=2))
    a = client.spawn_task(queue, "a", {"n": "a"})
    client.set_fake_now(BASE)
    b = client.spawn_task(queue, "b", {"n": "b"})
    client.set_fake_now(BASE + timedelta(seconds=1))
    c = client.spawn_task(queue, "c", {"n": "c"})
    client.set_fake_now(BASE + timedelta(seconds=10))

    first = client.claim_tasks(queue, worker="w1", claim_timeout=60, qty=2)
    assert [r["run_id"] for r in first] == [b.run_id, c.run_id]
    assert [r["task_name"] for r in first] == ["b", "c"]
    assert [r["attempt"] for r in first] == [1, 1]
    assert first[0]["params"] == {"n": "b"}

    for spawn in (b, c):
        run = client.get_run(queue, spawn.run_id)
        assert run["state"] == "running"
        assert run["claimed_by"] == "w1"
        assert run["claim_expires_at"] == BASE + timedelta(seconds=70)
        assert run["started_at"] == BASE + timedelta(seconds=10)
        assert run["available_at"] == BASE + timedelta(seconds=10)
        task = client.get_task(queue, spawn.task_id)
        assert task["state"] == "running"
        assert task["attempts"] == 1
        assert task["first_started_at"] == BASE + timedelta(seconds=10)
        assert task["last_attempt_run"] == spawn.run_id

    second = client.claim_tasks(queue, worker="w2", qty=5)
    assert [r["run_id"] for r in second] == [a.run_id]
    assert client.claim_tasks(queue) == []


@pytest.mark.parametrize("storage_mode", ("unpartitioned", "partitioned"))
def test_claim_breaks_available_at_ties_by_run_id(make_queue, client, storage_mode):
    queue = make_queue(f"tie_{storage_mode}", storage_mode)
    client.set_fake_now(BASE)
    spawns = [client.spawn_task(queue, f"t{i}", {}) for i in range(6)]
    run_ids = sorted(s.run_id for s in spawns)
    available = {client.get_run(queue, r)["available_at"] for r in run_ids}
    assert available == {BASE}

    client.set_fake_now(BASE + timedelta(seconds=1))
    # Which runs are picked, and in which order they come back, is decided by
    # run_id alone since every available_at is the same.
    first = client.claim_tasks(queue, qty=2)
    assert [r["run_id"] for r in first] == run_ids[:2]
    second = client.claim_tasks(queue, qty=3)
    assert [r["run_id"] for r in second] == run_ids[2:5]
    third = client.claim_tasks(queue, qty=3)
    assert [r["run_id"] for r in third] == run_ids[5:]


def test_claim_defaults_for_empty_worker_and_null_timeout(make_queue, client):
    queue = make_queue("defaults")
    client.set_fake_now(BASE)
    spawn = client.spawn_task(queue, "t", {})

    client.conn.execute(
        "select * from absurd.claim_task(%s, %s, %s, %s)", (queue, "", None, None)
    )
    run = client.get_run(queue, spawn.run_id)
    assert run["claimed_by"] == "worker"
    assert run["claim_expires_at"] == BASE + timedelta(seconds=30)


def test_claim_timeout_zero_leaves_no_lease(make_queue, client):
    queue = make_queue("zero_timeout")
    client.set_fake_now(BASE)
    spawn = client.spawn_task(queue, "t", {})

    rows = client.claim_tasks(queue, claim_timeout=0)
    assert len(rows) == 1
    assert client.get_run(queue, spawn.run_id)["claim_expires_at"] is None

    # Without a lease there is nothing to expire, however much time passes.
    client.set_fake_now(BASE + timedelta(days=30))
    assert client.claim_tasks(queue) == []
    assert client.get_run(queue, spawn.run_id)["state"] == "running"


def test_expired_lease_sweep_is_bounded_by_qty_and_retries(make_queue, client):
    queue = make_queue("expired")
    client.set_fake_now(BASE)
    spawns = [client.spawn_task(queue, f"t{i}", {}) for i in range(5)]
    claimed = client.claim_tasks(queue, claim_timeout=1, qty=5)
    assert len(claimed) == 5

    client.set_fake_now(BASE + timedelta(seconds=10))

    # qty=1 sweeps at most one expired lease.
    client.claim_tasks(queue, worker="sweeper", qty=1)
    failed = [s for s in spawns if "failed" in run_states(client, queue, s.task_id)]
    assert len(failed) == 1
    run = client.get_runs(queue, failed[0].task_id)[0]
    assert run["failure_reason"]["name"] == "$ClaimTimeout"
    assert run["failure_reason"]["workerId"] == "worker"
    assert run["failure_reason"]["attempt"] == 1

    client.claim_tasks(queue, worker="sweeper", qty=2)
    failed = [s for s in spawns if "failed" in run_states(client, queue, s.task_id)]
    assert len(failed) == 3


def test_expired_lease_sweep_skips_locked_runs(make_queue, client, db_dsn):
    queue = make_queue("expired_locked")
    client.set_fake_now(BASE)
    spawn = client.spawn_task(queue, "t", {})
    client.claim_tasks(queue, claim_timeout=1)
    client.conn.commit()  # make the claimed state visible to the other session

    try:
        with psycopg.connect(db_dsn) as other:
            other.execute(
                sql.SQL("select 1 from absurd.{} where run_id = %s for update").format(
                    sql.Identifier(f"r_{queue}")
                ),
                (spawn.run_id,),
            )
            client.set_fake_now(BASE + timedelta(seconds=10))
            # The locked expired run is skipped rather than waited for.
            assert client.claim_tasks(queue, qty=1) == []
            assert client.get_run(queue, spawn.run_id)["state"] == "running"
    finally:
        client.conn.rollback()
        client.drop_queue(queue)
        client.conn.commit()


def test_max_delay_cancels_unstarted_task_before_claiming(make_queue, client):
    queue = make_queue("max_delay")
    client.set_fake_now(BASE)
    doomed = client.spawn_task(queue, "doomed", {}, {"cancellation": {"max_delay": 5}})
    client.set_fake_now(BASE + timedelta(seconds=4))
    ok = client.spawn_task(queue, "ok", {}, {"cancellation": {"max_delay": 5}})

    client.set_fake_now(BASE + timedelta(seconds=6))
    rows = client.claim_tasks(queue)
    assert [r["run_id"] for r in rows] == [ok.run_id]

    task = client.get_task(queue, doomed.task_id)
    assert task["state"] == "cancelled"
    assert task["cancelled_at"] == BASE + timedelta(seconds=6)
    assert run_states(client, queue, doomed.task_id) == ["cancelled"]


def test_max_duration_cancels_started_task(make_queue, client):
    queue = make_queue("max_duration")
    client.set_fake_now(BASE)
    spawn = client.spawn_task(queue, "long", {}, {"cancellation": {"max_duration": 30}})
    assert len(client.claim_tasks(queue, claim_timeout=3600)) == 1

    client.set_fake_now(BASE + timedelta(seconds=29))
    client.claim_tasks(queue)
    assert client.get_task(queue, spawn.task_id)["state"] == "running"

    client.set_fake_now(BASE + timedelta(seconds=30))
    assert client.claim_tasks(queue) == []
    task = client.get_task(queue, spawn.task_id)
    assert task["state"] == "cancelled"
    run = client.get_runs(queue, spawn.task_id)[0]
    assert run["state"] == "cancelled"
    assert run["claimed_by"] is None
    assert run["claim_expires_at"] is None


def test_claim_cleans_up_expired_wait_registration(make_queue, client):
    queue = make_queue("wait_cleanup")
    client.set_fake_now(BASE)
    spawn = client.spawn_task(queue, "t", {})
    claimed = client.claim_tasks(queue)[0]

    wait = client.await_event(
        queue, spawn.task_id, claimed["run_id"], "step", "evt", timeout_seconds=10
    )
    assert wait["should_suspend"] is True
    waits = f"select count(*) from absurd.w_{queue}"
    assert client.conn.execute(waits).fetchone()[0] == 1

    # Before the timeout nothing wakes the run.
    client.set_fake_now(BASE + timedelta(seconds=5))
    assert client.claim_tasks(queue) == []
    assert client.conn.execute(waits).fetchone()[0] == 1

    client.set_fake_now(BASE + timedelta(seconds=11))
    rows = client.claim_tasks(queue)
    assert [r["run_id"] for r in rows] == [claimed["run_id"]]
    assert rows[0]["wake_event"] == "evt"
    assert rows[0]["event_payload"] is None
    assert rows[0]["attempt"] == 1
    assert client.conn.execute(waits).fetchone()[0] == 0


# -- the real generated function under a restricted role ------------------------

# What a worker role needs to claim, with every other function and table
# privilege withheld: the claim function itself, the functions it and the
# cancellation and expired-lease paths call, and the queue tables they touch.
WORKER_FUNCTIONS = (
    "absurd.claim_task(text, text, integer, integer)",
    "absurd.cancel_task(text, uuid)",
    "absurd.fail_run(text, uuid, jsonb, timestamptz)",
    "absurd.current_time()",
    "absurd.retry_delay_seconds(jsonb, integer)",
    "absurd.portable_uuidv7()",
)
WORKER_TABLES = (
    ("t", "select, update"),
    ("r", "select, insert, update"),
    ("w", "select, delete"),
)


def test_restricted_worker_role_runs_the_generated_function(new_database):
    # A deployment that withholds PUBLIC EXECUTE from every Absurd function and
    # grants a worker role only what claiming needs.  With EXECUTE on
    # ct_<queue> the REAL generated function serves the claim, cancellation
    # sweep, expired-lease sweep and wait cleanup under that role's privileges;
    # without it claim_task() serves the same claims itself.
    dsn = new_database("cf_restricted")
    queue, role = "rq", "cf_restricted_worker"
    ct = f"absurd.ct_{queue}(text, integer, integer)"
    with psycopg.connect(dsn, autocommit=True) as admin:
        # No function, including the ones create_queue() generates later, is
        # executable by PUBLIC.
        admin.execute(
            "alter default privileges revoke execute on functions from public"
        )
        admin.execute((TESTS.parent / "sql" / "absurd.sql").read_text())
        admin.execute("select absurd.create_queue(%s)", (queue,))
        admin.execute(sql.SQL("create role {}").format(sql.Identifier(role)))
        try:
            admin.execute(
                sql.SQL("grant usage on schema absurd to {}").format(
                    sql.Identifier(role)
                )
            )
            for fn in (*WORKER_FUNCTIONS, ct):
                admin.execute(
                    sql.SQL("grant execute on function {} to {}").format(
                        sql.SQL(fn), sql.Identifier(role)
                    )
                )
            for prefix, privileges in WORKER_TABLES:
                admin.execute(
                    sql.SQL("grant {} on absurd.{} to {}").format(
                        sql.SQL(privileges),
                        sql.Identifier(f"{prefix}_{queue}"),
                        sql.Identifier(role),
                    )
                )

            def scenario(expect_generated_calls):
                """Exercise every statement of the claim path as the worker."""
                admin.execute("truncate absurd.w_rq, absurd.r_rq, absurd.t_rq cascade")
                spawn = lambda name, options="{}": admin.execute(  # noqa: E731
                    "select task_id, run_id from absurd.spawn_task(%s, %s, '{}', %s)",
                    (queue, name, options),
                ).fetchone()

                with psycopg.connect(dsn) as worker:
                    # Counting calls of ct_<queue> per transaction tells the two
                    # paths apart.  Only a superuser may enable it, so do that
                    # before dropping to the worker role.
                    worker.execute("set track_functions = 'all'")
                    worker.execute(sql.SQL("set role {}").format(sql.Identifier(role)))
                    worker.execute("set absurd.fake_now = '2030-01-01T00:00:00Z'")
                    worker.commit()
                    ct_calls = lambda: worker.execute(  # noqa: E731
                        "select coalesce(pg_stat_get_xact_function_calls("
                        f"'{ct}'::regprocedure), 0)"
                    ).fetchone()[0]

                    # Claim: run, task and defaults.
                    admin.execute("set absurd.fake_now = '2030-01-01T00:00:00Z'")
                    first_task, first_run = spawn("first", '{"max_attempts": 3}')
                    rows = worker.execute(
                        "select run_id, task_name, attempt from "
                        "absurd.claim_task(%s, 'w1', 60, 1)",
                        (queue,),
                    ).fetchall()
                    assert [(r[0], r[1], r[2]) for r in rows] == [
                        (first_run, "first", 1)
                    ]
                    assert ct_calls() == expect_generated_calls
                    worker.commit()
                    row = admin.execute(
                        "select r.state, r.claimed_by, r.claim_expires_at, t.state, t.attempts "
                        "from absurd.r_rq r join absurd.t_rq t using (task_id) "
                        "where r.run_id = %s",
                        (first_run,),
                    ).fetchone()
                    assert row[:2] == ("running", "w1")
                    assert row[2].isoformat().startswith("2030-01-01T00:01:00")
                    assert row[3:] == ("running", 1)

                    # Cancellation sweep: a task past its max_delay.
                    doomed_task, _ = spawn(
                        "doomed", '{"cancellation": {"max_delay": 1}}'
                    )
                    worker.execute("set absurd.fake_now = '2030-01-01T00:00:10Z'")
                    assert (
                        worker.execute(
                            "select * from absurd.claim_task(%s, 'w1', 60, 1)", (queue,)
                        ).fetchall()
                        == []
                    )
                    worker.commit()
                    assert admin.execute(
                        "select state from absurd.t_rq where task_id = %s",
                        (doomed_task,),
                    ).fetchone() == ("cancelled",)

                    # Expired-lease sweep: the first run's 60 s lease lapses and
                    # the sweep fails it, which retries the task.
                    worker.execute("set absurd.fake_now = '2030-01-01T00:02:00Z'")
                    rows = worker.execute(
                        "select task_name, attempt from absurd.claim_task(%s, 'w2', 60, 1)",
                        (queue,),
                    ).fetchall()
                    assert rows == [("first", 2)]
                    worker.commit()
                    assert admin.execute(
                        "select state, failure_reason->>'name' from absurd.r_rq "
                        "where run_id = %s",
                        (first_run,),
                    ).fetchone() == ("failed", "$ClaimTimeout")

                    # Wait cleanup: the retried run waits for an event with a
                    # timeout; once it passes the claim wakes it and deletes the
                    # registration.
                    retry_run = admin.execute(
                        "select run_id from absurd.r_rq where task_id = %s "
                        "and state = 'running'",
                        (first_task,),
                    ).fetchone()[0]
                    admin.execute("set absurd.fake_now = '2030-01-01T00:02:00Z'")
                    admin.execute(
                        "select * from absurd.await_event(%s, %s, %s, 'step', 'evt', 10)",
                        (queue, first_task, retry_run),
                    )
                    assert admin.execute(
                        "select count(*) from absurd.w_rq"
                    ).fetchone() == (1,)
                    worker.execute("set absurd.fake_now = '2030-01-01T00:02:11Z'")
                    rows = worker.execute(
                        "select run_id, wake_event from absurd.claim_task(%s, 'w3', 60, 1)",
                        (queue,),
                    ).fetchall()
                    assert rows == [(retry_run, "evt")]
                    worker.commit()
                    assert admin.execute(
                        "select count(*) from absurd.w_rq"
                    ).fetchone() == (0,)

            scenario(expect_generated_calls=1)

            # Without EXECUTE on ct_<queue> the same claims are served by
            # claim_task() itself; the function is never called.
            admin.execute(
                sql.SQL("revoke execute on function {} from {}").format(
                    sql.SQL(ct), sql.Identifier(role)
                )
            )
            scenario(expect_generated_calls=0)
        finally:
            admin.execute(sql.SQL("drop owned by {}").format(sql.Identifier(role)))
            admin.execute(sql.SQL("drop role {}").format(sql.Identifier(role)))


# -- upgrading a real 0.5.0 database ---------------------------------------------


def install_0_5_0(new_database, name):
    dsn = new_database(name)
    conn = psycopg.connect(dsn, autocommit=True)
    conn.execute(ABSURD_0_5_0.read_text())
    assert conn.execute("select absurd.get_schema_version()").fetchone() == ("0.5.0",)
    return conn


def schema_snapshot(conn):
    """Everything the migration may change, as plain data."""
    return {
        "version": conn.execute("select absurd.get_schema_version()").fetchone(),
        "relations": conn.execute(
            "select c.relname, c.relkind, c.relowner from pg_class c "
            "where c.relnamespace = 'absurd'::regnamespace order by 1"
        ).fetchall(),
        "functions": conn.execute(
            "select p.oid::regprocedure::text, md5(pg_get_functiondef(p.oid)) "
            "from pg_proc p where p.pronamespace = 'absurd'::regnamespace order by 1"
        ).fetchall(),
        "queues": conn.execute("select * from absurd.queues order by 1").fetchall(),
    }


@pytest.mark.parametrize(
    ("queue_bytes", "storage_mode"),
    ((57, "partitioned"), (54, "unpartitioned")),
    ids=("57-byte-partitioned", "54-byte-unpartitioned"),
)
def test_migration_refuses_queues_over_the_new_name_limit_and_changes_nothing(
    new_database, queue_bytes, storage_mode
):
    # 0.5.0 accepted queue names of up to 57 bytes.  The year-unique weekly
    # partition name of a 57-byte queue is 66 bytes, which PostgreSQL would
    # truncate to 63 so that different weeks collide, and main's functions
    # reject every name over 53 bytes.
    with install_0_5_0(new_database, "cf_mig_long") as conn:
        long_queue = "q" * queue_bytes
        conn.execute("select absurd.create_queue('short_ok', 'partitioned')")
        conn.execute("select absurd.create_queue(%s, %s)", (long_queue, storage_mode))
        conn.execute("select absurd.spawn_task(%s, 't', '{}')", (long_queue,))
        if storage_mode == "partitioned":
            # The 0.5.0 names are valid 63-byte identifiers, one per week.
            names = [
                r[0]
                for r in conn.execute(
                    "select relname from pg_class where relname ~ %s",
                    (f"^[trcw]_{long_queue}_[0-9]{{3}}$",),
                )
            ]
            assert len(names) >= 8 and {len(n) for n in names} == {63}

        before = schema_snapshot(conn)
        with pytest.raises(psycopg.errors.CheckViolation) as exc:
            conn.execute(migration_sql())

        message = str(exc.value)
        assert f'"{long_queue}" ({queue_bytes} bytes, {storage_mode})' in message
        assert "53 bytes" in message
        assert "short_ok" not in message
        assert "drop it with absurd.drop_queue()" in exc.value.diag.message_hint
        assert "Nothing has been changed" in exc.value.diag.message_hint
        # Nothing was changed: not the version, a definition, a partition name,
        # nor a claim function.
        assert schema_snapshot(conn) == before
        assert conn.execute(
            "select count(*) from absurd.t_" + long_queue
        ).fetchone() == (1,)

        # The documented remediation: drop (0.5.0's drop_queue does not validate
        # the name), recreate under a shorter name, then migrate.
        conn.execute("select absurd.drop_queue(%s)", (long_queue,))
        conn.execute(migration_sql())
        assert conn.execute("select absurd.get_schema_version()").fetchone() == (
            "main",
        )
        conn.execute("select absurd.create_queue(%s, %s)", ("q" * 53, storage_mode))


def test_migration_twice_on_a_populated_partitioned_queue_from_0_5_0(new_database):
    with install_0_5_0(new_database, "cf_mig_twice") as conn:
        queue = "busy"
        conn.execute("select absurd.create_queue(%s, 'partitioned')", (queue,))
        # Spread work over several weeks of the partition window.
        runs = []
        for week in range(-1, 4):
            moment = datetime(2030, 1, 7, 12, tzinfo=timezone.utc) + timedelta(
                weeks=week
            )
            conn.execute(
                "select set_config('absurd.fake_now', %s, false)", (moment.isoformat(),)
            )
            conn.execute("select absurd.ensure_partitions(%s)", (queue,))
            for i in range(3):
                runs.append(
                    conn.execute(
                        "select run_id from absurd.spawn_task(%s, %s, '{}')",
                        (queue, f"w{week}-{i}"),
                    ).fetchone()[0]
                )
        conn.execute(
            "select set_config('absurd.fake_now', %s, false)",
            (datetime(2030, 1, 22, 12, tzinfo=timezone.utc).isoformat(),),
        )
        claimed = conn.execute(
            "select run_id from absurd.claim_task(%s, 'w', 600, 4)", (queue,)
        ).fetchall()
        assert len(claimed) == 4

        def populated_partitions():
            return conn.execute(
                "select count(distinct tableoid) from absurd.r_" + queue
            ).fetchone()[0]

        def partition_names():
            return [
                r[0]
                for r in conn.execute(
                    "select relname from pg_class "
                    "where relname ~ '^[trcw]_busy_[0-9]+$' order by 1"
                )
            ]

        old_names = partition_names()
        assert populated_partitions() >= 4
        assert all(len(n.rsplit("_", 1)[1]) == 3 for n in old_names)

        migration = migration_sql()
        conn.execute(migration)
        after_first = schema_snapshot(conn)
        names = partition_names()
        assert all(len(n.rsplit("_", 1)[1]) == 6 for n in names)
        assert len(names) == len(old_names)

        conn.execute(migration)  # idempotent
        assert schema_snapshot(conn) == after_first

        # Data is untouched and the queue works: ensure_partitions finds every
        # week's partition by its new name and adds none beside it.
        assert conn.execute("select count(*) from absurd.r_" + queue).fetchone() == (
            len(runs),
        )
        conn.execute("select absurd.ensure_partitions(%s)", (queue,))
        conn.execute("select absurd.ensure_partitions()")
        assert partition_names() == names

        # ... and a later week gets a new partition, with work claimable from it.
        conn.execute(
            "select set_config('absurd.fake_now', %s, false)",
            (datetime(2030, 3, 4, 12, tzinfo=timezone.utc).isoformat(),),
        )
        conn.execute("select absurd.ensure_partitions(%s)", (queue,))
        assert len(partition_names()) > len(names)
        later = conn.execute(
            "select run_id from absurd.spawn_task(%s, 'later', '{}')", (queue,)
        ).fetchone()[0]
        got = conn.execute(
            "select run_id from absurd.claim_task(%s, 'w', 600, 50)", (queue,)
        ).fetchall()
        assert (later,) in got
        assert claim_fn_exists_conn(conn, queue)


def test_migration_refuses_partition_renames_onto_another_queue_and_changes_nothing(
    new_database, postgres_container
):
    # Partition t_jobs_<3-digit tag> of a partitioned "jobs" queue is renamed
    # to t_jobs_<IYYYIW>, the task table of a valid 0.5.0 queue "jobs_<IYYYIW>".
    # The migration must refuse before changing anything, in both ways it is
    # applied, and run once the documented remediation is done.
    dbname = "cf_mig_collide"
    year, week, _ = datetime.now(timezone.utc).isocalendar()
    tag = f"{year}{week:02d}"
    with install_0_5_0(new_database, dbname) as conn:
        conn.execute("select absurd.create_queue('jobs', 'partitioned')")
        conn.execute("select absurd.create_queue(%s)", (f"jobs_{tag}",))
        conn.execute("select absurd.spawn_task(%s, 't', '{}')", (f"jobs_{tag}",))
        old_partition = f"t_jobs_{tag[3:]}"
        assert conn.execute(
            "select to_regclass(%s) is not null", (f"absurd.{old_partition}",)
        ).fetchone() == (True,)

        before = schema_snapshot(conn)
        with pytest.raises(psycopg.errors.DuplicateTable) as exc:
            conn.execute(migration_sql())
        assert f"{old_partition} -> t_jobs_{tag}" in str(exc.value)
        assert f"{old_partition}_pkey -> t_jobs_{tag}_pkey" in str(exc.value)
        assert "Nothing has been changed" in exc.value.diag.message_hint
        assert schema_snapshot(conn) == before

        failed = _apply_like_absurdctl(postgres_container, dbname, migration_sql())
        assert failed.returncode != 0
        assert f"{old_partition} -> t_jobs_{tag}" in failed.stderr, failed.stderr
        assert schema_snapshot(conn) == before

        # The documented remediation: drain, drop and recreate the other queue
        # under another name, then migrate.
        conn.execute("select absurd.drop_queue(%s)", (f"jobs_{tag}",))
        conn.execute("select absurd.create_queue('jobs_current')")
        rerun = _apply_like_absurdctl(postgres_container, dbname, migration_sql())
        assert rerun.returncode == 0, rerun.stderr
        assert conn.execute("select absurd.get_schema_version()").fetchone() == (
            "main",
        )
        assert conn.execute(
            "select to_regclass(%s) is not null", (f"absurd.t_jobs_{tag}",)
        ).fetchone() == (True,)
        conn.execute("select absurd.spawn_task('jobs', 't', '{}')")
        assert [
            r[3] for r in conn.execute("select * from absurd.claim_task('jobs', 'w')")
        ] == ["t"]


def test_migration_renames_a_partition_created_concurrently(new_database, admin):
    # absurdctl applies the migration statement by statement.  A 0.5.0
    # ensure_partitions() that creates old-style partitions in a transaction
    # still uncommitted when the rename statement starts must not leave them
    # behind under their old tags once both have committed.
    dsn = new_database("cf_mig_concurrent")
    migration = migration_sql()
    split = migration.index("-- Make weekly partition tags year-unique")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(ABSURD_0_5_0.read_text())
        conn.execute("select absurd.create_queue('busy', 'partitioned')")
        conn.execute(migration[:split])

        with psycopg.connect(dsn) as maintainer, psycopg.connect(dsn) as migrator:
            maintainer.execute(
                "select set_config('absurd.fake_now', %s, false)",
                (datetime(2031, 6, 2, 12, tzinfo=timezone.utc).isoformat(),),
            )
            maintainer.execute("select absurd.ensure_partitions('busy')")
            thread, migrated = _start(migrator, migration[split:], None)
            _finished_or_waiting(admin, thread, migrator.info.backend_pid)
            maintainer.commit()
            thread.join(30)
            assert not thread.is_alive()
            assert migrated == {"ok": True}, migrated.get("error")

        assert conn.execute("select absurd.get_schema_version()").fetchone() == (
            "main",
        )
        assert (
            conn.execute(
                "select relname from pg_class where relname ~ '^[trcw]_busy_[0-9]{3}$'"
            ).fetchall()
            == []
        )
        assert (
            conn.execute(
                "select count(*) from pg_class where relname ~ '^t_busy_2031[0-9]{2}$'"
            ).fetchone()[0]
            > 0
        )


def _old_style_partitions(conn, queue):
    return conn.execute(
        "select relname from pg_class where relname ~ %s order by 1",
        (f"^[trcw]_{queue}_[0-9]{{3}}$",),
    ).fetchall()


def test_migration_renames_partitions_of_a_queue_created_concurrently(
    new_database, admin
):
    # A partitioned queue created (with partitions under the still old tag)
    # in a transaction uncommitted when the rename statement starts must have
    # its partitions renamed too.
    dsn = new_database("cf_mig_newqueue")
    migration = migration_sql()
    split = migration.index("-- Make weekly partition tags year-unique")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(ABSURD_0_5_0.read_text())
        conn.execute("select absurd.create_queue('busy', 'partitioned')")
        conn.execute(migration[:split])

        with psycopg.connect(dsn) as creator, psycopg.connect(dsn) as migrator:
            creator.execute("select absurd.create_queue('late', 'partitioned')")
            assert _old_style_partitions(creator, "late")
            thread, migrated = _start(migrator, migration[split:], None)
            _finished_or_waiting(admin, thread, migrator.info.backend_pid)
            creator.commit()
            thread.join(30)
            assert not thread.is_alive()
            assert migrated == {"ok": True}, migrated.get("error")

        assert _old_style_partitions(conn, "late") == []
        assert _old_style_partitions(conn, "busy") == []
        conn.execute("select absurd.ensure_partitions()")
        conn.execute("select absurd.spawn_task('late', 't', '{}')")
        assert [
            r[3] for r in conn.execute("select * from absurd.claim_task('late', 'w')")
        ] == ["t"]


def test_migration_in_one_transaction_and_a_policy_change_do_not_deadlock(
    new_database, admin
):
    # Applied as one transaction, the migration holds the partition locks of
    # the rename while its claim function backfill locks queue rows.  0.5.0's
    # set_queue_policy() locks the queue row and then the partitions.
    dsn = new_database("cf_mig_policy")
    migration = migration_sql()
    split = migration.index("-- Create the claim function for every existing queue")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(ABSURD_0_5_0.read_text())
        conn.execute("select absurd.create_queue('busy', 'partitioned')")

    with psycopg.connect(dsn) as migrator, psycopg.connect(dsn) as policy:
        for c in (migrator, policy):
            c.execute("set deadlock_timeout = '100ms'")
            c.execute("set statement_timeout = '20s'")
            c.commit()
        migrator.execute(migration[:split])
        thread, changed = _start(
            policy,
            "select absurd.set_queue_policy('busy', %s)",
            ('{"default_partition": "disabled"}',),
        )
        _finished_or_waiting(admin, thread, policy.info.backend_pid)
        migrator.execute(migration[split:])
        migrator.commit()
        thread.join(30)
        assert not thread.is_alive()
        assert changed == {"ok": True}, changed.get("error")

    with psycopg.connect(dsn, autocommit=True) as conn:
        assert conn.execute("select absurd.get_schema_version()").fetchone() == (
            "main",
        )
        assert claim_fn_exists_conn(conn, "busy")
        assert conn.execute(
            "select default_partition from absurd.queues where queue_name = 'busy'"
        ).fetchone() == ("disabled",)


def test_migration_refuses_a_colliding_queue_created_concurrently(new_database, admin):
    # A queue created while the rename statement starts can own a new
    # partition name ("busy_<IYYYIW>" beside partitioned "busy").  The rename
    # must wait for it and fail instead of renaming a partition onto the name
    # the new queue then silently shares; the next run reports the collision.
    dsn = new_database("cf_mig_racecollide")
    year, week, _ = datetime.now(timezone.utc).isocalendar()
    tag = f"{year}{week:02d}"
    migration = migration_sql()
    split = migration.index("-- Make weekly partition tags year-unique")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(ABSURD_0_5_0.read_text())
        conn.execute("select absurd.create_queue('busy', 'partitioned')")
        conn.execute(migration[:split])
        before = _old_style_partitions(conn, "busy")
        assert before

        with psycopg.connect(dsn) as creator, psycopg.connect(dsn) as migrator:
            creator.execute("select absurd.create_queue(%s)", (f"busy_{tag}",))
            thread, migrated = _start(migrator, migration[split:], None)
            _finished_or_waiting(admin, thread, migrator.info.backend_pid)
            creator.commit()
            thread.join(30)
            assert not thread.is_alive()
            assert isinstance(migrated.get("error"), psycopg.errors.DuplicateTable), (
                migrated
            )

        assert conn.execute("select absurd.get_schema_version()").fetchone() == (
            "0.5.0",
        )
        assert _old_style_partitions(conn, "busy") == before
        with pytest.raises(psycopg.errors.DuplicateTable) as exc:
            conn.execute(migration)
        assert f"t_busy_{tag[3:]} -> t_busy_{tag}" in str(exc.value)


def test_migration_refuses_a_long_queue_name_created_concurrently(new_database, admin):
    # Applied as one transaction, 0.5.0's create_queue() can add a 57-byte
    # partitioned queue after the length preflight ran.  Its year-unique
    # partition names would be truncated; the rename statement refuses.
    dsn = new_database("cf_mig_racelong")
    migration = migration_sql()
    split = migration.index("-- Make weekly partition tags year-unique")
    long_queue = "q" * 57
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(ABSURD_0_5_0.read_text())
        conn.execute("select absurd.create_queue('busy', 'partitioned')")

    with psycopg.connect(dsn) as creator, psycopg.connect(dsn) as migrator:
        migrator.execute(migration[:split])
        creator.execute("select absurd.create_queue(%s, 'partitioned')", (long_queue,))
        thread, migrated = _start(migrator, migration[split:], None)
        _finished_or_waiting(admin, thread, migrator.info.backend_pid)
        creator.commit()
        thread.join(30)
        assert not thread.is_alive()
        assert isinstance(migrated.get("error"), psycopg.errors.CheckViolation), (
            migrated
        )

    with psycopg.connect(dsn, autocommit=True) as conn:
        assert conn.execute("select absurd.get_schema_version()").fetchone() == (
            "0.5.0",
        )
        assert _old_style_partitions(conn, long_queue)


def claim_fn_exists_conn(conn, queue):
    return conn.execute(
        "select count(*) from pg_proc where proname = %s "
        "and pronamespace = 'absurd'::regnamespace",
        (f"ct_{queue}",),
    ).fetchone() == (1,)


# -- concurrent regeneration ---------------------------------------------------


def _start(conn, statement, params):
    """Run statement on conn in a thread and commit; returns (thread, outcome)."""
    outcome = {}

    def run():
        try:
            conn.execute(statement, params)
            conn.commit()
            outcome["ok"] = True
        except psycopg.Error as exc:  # reported by the caller
            conn.rollback()
            outcome["error"] = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, outcome


def _finished_or_waiting(admin, thread, pid, timeout_seconds=5.0):
    """Wait until the thread is done or its session waits for a lock."""
    deadline = time.monotonic() + timeout_seconds
    while thread.is_alive() and time.monotonic() < deadline:
        row = admin.execute(
            "select wait_event_type = 'Lock' from pg_stat_activity where pid = %s",
            (pid,),
        ).fetchone()
        if row and row[0]:
            return
        time.sleep(0.02)
    assert not thread.is_alive(), "neither finished nor waited for a lock"


@pytest.mark.parametrize("state", ("missing", "outdated", "current"))
def test_concurrent_regeneration_of_a_claim_function(admin, db_dsn, state):
    # ensure_claim_function() (here through the migration's direct call and
    # create_queue()) creates or replaces ct_<queue> when it is missing or
    # differs from the generated one.  Two transactions doing that for the same
    # queue must not both write pg_proc: the second would wait for the first
    # and then fail with a unique violation (missing) or "tuple concurrently
    # updated" (outdated).
    queue = f"cf_regen_{state}"
    admin.execute("select absurd.create_queue(%s)", (queue,))
    try:
        if state == "missing":
            admin.execute(
                sql.SQL("drop function absurd.{}(text, integer, integer)").format(
                    sql.Identifier(f"ct_{queue}")
                )
            )
        elif state == "outdated":
            install_sentinel(admin, queue)
        before = claim_fn_version(admin, queue)

        with psycopg.connect(db_dsn) as first, psycopg.connect(db_dsn) as second:
            first.execute("select absurd.ensure_claim_function(%s)", (queue,))
            thread, outcome = _start(second, "select absurd.create_queue(%s)", (queue,))
            _finished_or_waiting(admin, thread, second.info.backend_pid)
            first.commit()
            thread.join(10)
            assert not thread.is_alive()
            assert outcome == {"ok": True}, outcome.get("error")

        after = claim_fn_version(admin, queue)
        assert after is not None
        if state == "current":
            assert after == before  # neither transaction rewrote it
        prosrc = admin.execute(
            "select prosrc from pg_proc where oid = %s", (after[0],)
        ).fetchone()[0]
        assert "sentinel" not in prosrc
        assert claim_fn_owner(admin, queue) == table_owner(admin, queue)
        admin.execute("select absurd.spawn_task(%s, 't', '{}')", (queue,))
        assert [
            r[3]
            for r in admin.execute("select * from absurd.claim_task(%s, 'w')", (queue,))
        ] == ["t"]
    finally:
        admin.execute("select absurd.drop_queue(%s)", (queue,))


@pytest.mark.parametrize("state", ("missing", "outdated"))
def test_drop_queue_and_claim_function_regeneration_do_not_race(admin, db_dsn, state):
    # drop_queue() and ensure_claim_function() for the same queue must not
    # leave a claim function behind for a dropped queue, nor deadlock.  A
    # session reading the queue's checkpoint table stalls drop_queue() part way
    # through, so the regeneration arrives while the drop is in progress.
    queue = f"cf_dropregen_{state}"
    admin.execute("select absurd.create_queue(%s)", (queue,))
    try:
        if state == "missing":
            admin.execute(
                sql.SQL("drop function absurd.{}(text, integer, integer)").format(
                    sql.Identifier(f"ct_{queue}")
                )
            )
        else:
            install_sentinel(admin, queue)

        with (
            psycopg.connect(db_dsn) as reader,
            psycopg.connect(db_dsn) as dropper,
            psycopg.connect(db_dsn) as regenerator,
        ):
            for conn in (dropper, regenerator):
                conn.execute("set deadlock_timeout = '100ms'")
                conn.execute("set statement_timeout = '10s'")
                conn.commit()
            reader.execute(
                sql.SQL("select from absurd.{} limit 0").format(
                    sql.Identifier(f"c_{queue}")
                )
            )
            drop_thread, dropped = _start(
                dropper, "select absurd.drop_queue(%s)", (queue,)
            )
            _finished_or_waiting(admin, drop_thread, dropper.info.backend_pid)
            assert drop_thread.is_alive(), dropped
            regen_thread, regenerated = _start(
                regenerator, "select absurd.ensure_claim_function(%s)", (queue,)
            )
            _finished_or_waiting(admin, regen_thread, regenerator.info.backend_pid)
            reader.rollback()
            drop_thread.join(10)
            regen_thread.join(10)
            assert not drop_thread.is_alive() and not regen_thread.is_alive()
            assert dropped == {"ok": True}, dropped.get("error")
            assert regenerated == {"ok": True}, regenerated.get("error")

        assert claim_fn_owner(admin, queue) is None
        assert admin.execute(
            "select count(*) from absurd.queues where queue_name = %s", (queue,)
        ).fetchone() == (0,)
        assert admin.execute(
            "select to_regclass(%s)", (f"absurd.t_{queue}",)
        ).fetchone() == (None,)
    finally:
        admin.execute(
            sql.SQL("drop function if exists absurd.{}(text, integer, integer)").format(
                sql.Identifier(f"ct_{queue}")
            )
        )
        admin.execute("select absurd.drop_queue(%s)", (queue,))


@pytest.mark.parametrize("storage_mode", ("unpartitioned", "partitioned"))
def test_repeated_create_queue_and_drop_queue_do_not_deadlock(
    admin, db_dsn, storage_mode
):
    # create_queue() for an existing queue and drop_queue() must take the
    # queue's row and its tables in the same order.  A session holding a lock
    # on the run table stalls create_queue() at its index DDL, so drop_queue()
    # arrives while the creation is in progress.
    queue = f"cf_createdrop_{storage_mode[0]}"
    admin.execute("select absurd.create_queue(%s, %s)", (queue, storage_mode))
    try:
        with (
            psycopg.connect(db_dsn) as reader,
            psycopg.connect(db_dsn) as creator,
            psycopg.connect(db_dsn) as dropper,
        ):
            for conn in (creator, dropper):
                conn.execute("set deadlock_timeout = '100ms'")
                conn.execute("set statement_timeout = '10s'")
                conn.commit()
            reader.execute(
                sql.SQL("lock table absurd.{} in row exclusive mode").format(
                    sql.Identifier(f"r_{queue}")
                )
            )
            create_thread, created = _start(
                creator, "select absurd.create_queue(%s, %s)", (queue, storage_mode)
            )
            _finished_or_waiting(admin, create_thread, creator.info.backend_pid)
            assert create_thread.is_alive(), created
            drop_thread, dropped = _start(
                dropper, "select absurd.drop_queue(%s)", (queue,)
            )
            _finished_or_waiting(admin, drop_thread, dropper.info.backend_pid)
            reader.rollback()
            create_thread.join(10)
            drop_thread.join(10)
            assert not create_thread.is_alive() and not drop_thread.is_alive()
            assert created == {"ok": True}, created.get("error")
            assert dropped == {"ok": True}, dropped.get("error")

        # The creation (of an existing queue) came first, then the drop.
        assert admin.execute(
            "select count(*) from absurd.queues where queue_name = %s", (queue,)
        ).fetchone() == (0,)
        for prefix in "trcew":
            assert admin.execute(
                "select to_regclass(%s)", (f"absurd.{prefix}_{queue}",)
            ).fetchone() == (None,)
        assert claim_fn_owner(admin, queue) is None
    finally:
        admin.execute("select absurd.drop_queue(%s)", (queue,))


def test_ensure_queue_tables_refuses_a_queue_without_registry_row(admin):
    with pytest.raises(psycopg.errors.RaiseException, match="does not exist"):
        admin.execute("select absurd.ensure_queue_tables('cf_unregistered')")
    assert admin.execute(
        "select to_regclass('absurd.t_cf_unregistered')"
    ).fetchone() == (None,)


def test_queue_manager_without_update_on_the_registry_manages_queues(admin, db_dsn):
    # Locking the queue's row needs UPDATE on absurd.queues.  A role that may
    # only insert and delete registry rows still creates (on claim_task()'s
    # dynamic statements), recreates and drops its queues.
    queue, role = "cf_noupdate", "cf_noupdate_owner"
    admin.execute(sql.SQL("create role {}").format(sql.Identifier(role)))
    try:
        admin.execute(
            sql.SQL("grant usage, create on schema absurd to {}").format(
                sql.Identifier(role)
            )
        )
        admin.execute(
            sql.SQL("grant select, insert, delete on absurd.queues to {}").format(
                sql.Identifier(role)
            )
        )
        with as_role(db_dsn, role) as conn:
            conn.execute("select absurd.create_queue(%s)", (queue,))
            conn.execute("select absurd.create_queue(%s)", (queue,))
            assert claim_fn_owner(conn, queue) is None
            conn.execute("select absurd.spawn_task(%s, 't', '{}')", (queue,))
            assert [
                r[3]
                for r in conn.execute(
                    "select * from absurd.claim_task(%s, 'w')", (queue,)
                )
            ] == ["t"]
            conn.execute("select absurd.drop_queue(%s)", (queue,))
        assert admin.execute(
            "select to_regclass(%s)", (f"absurd.t_{queue}",)
        ).fetchone() == (None,)
    finally:
        admin.execute("select absurd.drop_queue(%s)", (queue,))
        drop_roles(admin, role)


def test_queue_owner_drops_a_queue_whose_claim_function_another_role_owns(
    admin, db_dsn
):
    # claim_task() ignores a ct_<queue> owned by a role other than the owner
    # of the queue's tables, and ensure_claim_function() cannot replace it.
    # Such a function must not stop the queue owner from dropping the queue;
    # it is left to its owner.
    queue, owner, planter = "cf_drop_foreign", "cf_drop_owner", "cf_drop_planter"
    try:
        for role in (owner, planter):
            admin.execute(sql.SQL("create role {}").format(sql.Identifier(role)))
            admin.execute(
                sql.SQL("grant usage, create on schema absurd to {}").format(
                    sql.Identifier(role)
                )
            )
        admin.execute(
            sql.SQL(
                "grant select, insert, update, delete on absurd.queues to {}"
            ).format(sql.Identifier(owner))
        )
        with as_role(db_dsn, owner) as conn:
            conn.execute("select absurd.create_queue(%s)", (queue,))
            conn.execute(
                sql.SQL("drop function absurd.{}(text, integer, integer)").format(
                    sql.Identifier(f"ct_{queue}")
                )
            )
        with as_role(db_dsn, planter) as conn:
            install_sentinel(conn, queue)
        assert claim_fn_owner(admin, queue) == planter
        assert table_owner(admin, queue) == owner

        with as_role(db_dsn, owner) as conn:
            conn.execute("select absurd.drop_queue(%s)", (queue,))

        assert admin.execute(
            "select count(*) from absurd.queues where queue_name = %s", (queue,)
        ).fetchone() == (0,)
        for prefix in "trcew":
            assert admin.execute(
                "select to_regclass(%s)", (f"absurd.{prefix}_{queue}",)
            ).fetchone() == (None,)
        assert claim_fn_owner(admin, queue) == planter
    finally:
        admin.execute(
            sql.SQL("drop function if exists absurd.{}(text, integer, integer)").format(
                sql.Identifier(f"ct_{queue}")
            )
        )
        admin.execute("select absurd.drop_queue(%s)", (queue,))
        drop_roles(admin, owner, planter)


# -- upgrading restricted installations ------------------------------------------


def test_restricted_queue_owner_creates_queues_after_the_upgrade(new_database):
    # An installation whose installing role withholds PUBLIC EXECUTE and grants
    # a queue-managing role EXECUTE on the 0.5.0 functions.  The migration adds
    # ensure_claim_function(), on which that role has no EXECUTE.  It must
    # still create queues (on claim_task()'s dynamic statements) until it is
    # granted EXECUTE on the new function.
    dsn = new_database("cf_upgrade_revoked")
    role = "cf_upgrade_queue_owner"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(
            "alter default privileges revoke execute on functions from public"
        )
        admin.execute(ABSURD_0_5_0.read_text())
        admin.execute(sql.SQL("create role {}").format(sql.Identifier(role)))
        try:
            ident = sql.Identifier(role)
            admin.execute(
                sql.SQL("grant usage, create on schema absurd to {}").format(ident)
            )
            admin.execute(
                sql.SQL(
                    "grant select, insert, update, delete on absurd.queues to {}"
                ).format(ident)
            )
            admin.execute(
                sql.SQL("grant execute on all functions in schema absurd to {}").format(
                    ident
                )
            )
            with as_role(dsn, role) as conn:
                conn.execute("select absurd.create_queue('before')")

            admin.execute(migration_sql())
            assert admin.execute(
                "select has_function_privilege(%s, "
                "'absurd.ensure_claim_function(text)', 'execute')",
                (role,),
            ).fetchone() == (False,)

            with as_role(dsn, role) as conn:
                conn.execute("select absurd.create_queue('before')")
                conn.execute("select absurd.create_queue('after')")
                conn.execute("select absurd.create_queue('after_p', 'partitioned')")
                for queue in ("after", "after_p"):
                    assert claim_fn_owner(conn, queue) is None
                    conn.execute("select absurd.spawn_task(%s, 't', '{}')", (queue,))
                    assert [
                        r[3]
                        for r in conn.execute(
                            "select * from absurd.claim_task(%s, 'w')", (queue,)
                        )
                    ] == ["t"]

            # Granted EXECUTE on the new function, it gets the claim functions.
            admin.execute(
                sql.SQL(
                    "grant execute on function absurd.ensure_claim_function(text) to {}"
                ).format(ident)
            )
            with as_role(dsn, role) as conn:
                conn.execute("select absurd.create_queue('after')")
                assert claim_fn_owner(conn, "after") == role
        finally:
            admin.execute(sql.SQL("drop owned by {}").format(sql.Identifier(role)))
            admin.execute(sql.SQL("drop role {}").format(sql.Identifier(role)))


def _apply_like_absurdctl(postgres_container, dbname, sql_text, *, options=None):
    """Apply SQL the way absurdctl migrate does: psql -v ON_ERROR_STOP=1 -f -.

    Each statement commits on its own; psql stops at the first error.
    """
    env = ["-e", f"PGOPTIONS={options}"] if options else []
    return subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            *env,
            postgres_container.get_wrapped_container().id,
            "psql",
            "-U",
            postgres_container.username,
            "-d",
            dbname,
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            "-",
        ],
        input=sql_text,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _definitions(conn):
    """Function definitions and relations of schema absurd, minus queue objects."""
    return {
        "functions": conn.execute(
            "select p.oid::regprocedure::text, pg_get_functiondef(p.oid) "
            "from pg_proc p where p.pronamespace = 'absurd'::regnamespace "
            "and p.proname !~ '^ct_' order by 1"
        ).fetchall(),
        "queues_table": conn.execute(
            "select attname, format_type(atttypid, atttypmod), attnotnull "
            "from pg_attribute where attrelid = 'absurd.queues'::regclass "
            "and attnum > 0 and not attisdropped order by attnum"
        ).fetchall(),
    }


def test_migration_failing_late_does_not_claim_main_and_reruns(
    new_database, postgres_container
):
    # absurdctl migrate applies the file with ON_ERROR_STOP but without a
    # transaction.  A statement failing late (here: renaming a partition that
    # another session reads, under lock_timeout) must leave the database
    # reporting 0.5.0, so that the next migrate applies the file again, and
    # must not leave the new partition tag beside the old partition names.
    dbname = "cf_mig_late"
    dsn = new_database(dbname)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(ABSURD_0_5_0.read_text())
        conn.execute("select absurd.create_queue('busy', 'partitioned')")
        conn.execute("select absurd.spawn_task('busy', 't', '{}')")

        def old_partitions():
            return [
                r[0]
                for r in conn.execute(
                    "select relname from pg_class "
                    "where relname ~ '^[trcw]_busy_[0-9]{3}$' order by 1"
                )
            ]

        partitions = old_partitions()
        assert partitions

        with psycopg.connect(dsn) as reader:
            reader.execute(
                sql.SQL("select from absurd.{} limit 0").format(
                    sql.Identifier(partitions[-1])
                )
            )
            failed = _apply_like_absurdctl(
                postgres_container,
                dbname,
                migration_sql(),
                options="-c lock_timeout=200ms",
            )
        assert failed.returncode != 0
        assert "lock timeout" in failed.stderr, failed.stderr

        assert conn.execute("select absurd.get_schema_version()").fetchone() == (
            "0.5.0",
        )
        assert old_partitions() == partitions
        # The partition tag still matches the partition names.
        assert (
            len(conn.execute("select absurd.partition_week_tag(now())").fetchone()[0])
            == 3
        )
        conn.execute("select absurd.ensure_partitions('busy')")
        assert old_partitions() == partitions

        rerun = _apply_like_absurdctl(postgres_container, dbname, migration_sql())
        assert rerun.returncode == 0, rerun.stderr
        assert conn.execute("select absurd.get_schema_version()").fetchone() == (
            "main",
        )
        assert old_partitions() == []
        assert claim_fn_exists_conn(conn, "busy")
        assert [
            r[3] for r in conn.execute("select * from absurd.claim_task('busy', 'w')")
        ] == ["t"]

        fresh_dsn = new_database("cf_mig_late_fresh")
        with psycopg.connect(fresh_dsn, autocommit=True) as fresh:
            fresh.execute((TESTS.parent / "sql" / "absurd.sql").read_text())
            assert _definitions(conn) == _definitions(fresh)
