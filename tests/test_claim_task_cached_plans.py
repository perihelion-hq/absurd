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
    client.conn.execute(
        f"""
        create or replace function absurd.ct_{queue} (
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
    migration = next(
        p
        for p in sorted(MIGRATIONS.glob("*.sql"))
        if "ensure_claim_function" in p.read_text()
    )
    queues = ["cf_mig_a", "cf_mig_b"]
    for q in queues:
        client.create_queue(q)
    client.create_queue("cf_mig_p", storage_mode="partitioned")
    queues.append("cf_mig_p")
    for q in queues:
        drop_claim_fn(client, q)  # a queue as created by the previous release

    client.conn.execute(migration.read_text())

    for q in queues:
        assert claim_fn_exists(client, q)
        client.spawn_task(q, "t", {})
        assert [r["task_name"] for r in client.claim_tasks(q)] == ["t"]


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
