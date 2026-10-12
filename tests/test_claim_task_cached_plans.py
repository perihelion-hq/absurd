"""claim_task() runs through a per-queue function with static (plan-cached) SQL.

Every behaviour scenario runs twice: once through the generated
``absurd.ct_<queue>`` function and once through claim_task()'s dynamic
fallback (the per-queue function dropped, as for a queue created before the
function existed).  Both paths must produce the same observable result.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

BASE = datetime(2024, 8, 1, 12, 0, tzinfo=timezone.utc)
MIGRATIONS = Path(__file__).resolve().parent.parent / "sql" / "migrations"


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


@pytest.fixture(params=("per_queue", "fallback"))
def make_queue(request, client):
    def make(name, storage_mode=None):
        client.create_queue(name, storage_mode=storage_mode)
        assert claim_fn_exists(client, name)
        if request.param == "fallback":
            drop_claim_fn(client, name)
            assert not claim_fn_exists(client, name)
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

    # A name whose function identifier would exceed 63 bytes (only reachable
    # for queues created under older, longer limits) is skipped, never
    # truncated, and claim_task() keeps working through its dynamic path.
    client.conn.execute("select absurd.ensure_claim_function(%s)", ("l" * 61,))
    count = client.conn.execute(
        "select count(*) from pg_proc where proname like 'ct\\_l%'"
    ).fetchone()[0]
    assert count == 0


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


def test_repeated_polls_on_one_session(client):
    # Behavioural companion to the static-SQL assertion above: many polls on a
    # single session (plpgsql plan cache warm, generic plans in use) keep
    # returning the right answer.
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
