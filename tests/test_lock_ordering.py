import threading
import time

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb


def _set_application_name(conn, name):
    conn.execute(sql.SQL("set application_name = {}").format(sql.Literal(name)))


def _wait_for_lock_wait(db_dsn, application_name, timeout_seconds=5.0):
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        conn.execute("set timezone to 'UTC'")
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            row = conn.execute(
                """
                select wait_event_type
                from pg_stat_activity
                where application_name = %s
                """,
                (application_name,),
            ).fetchone()
            if row and row[0] == "Lock":
                return
            time.sleep(0.01)

    raise AssertionError(f"{application_name} did not block on a lock")


def _create_running_task(db_dsn, queue, *, options=None):
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        conn.execute("set timezone to 'UTC'")
        conn.execute("select absurd.create_queue(%s)", (queue,))

        spawn_row = conn.execute(
            """
            select task_id, run_id
            from absurd.spawn_task(%s, %s, %s, %s)
            """,
            (queue, "lock-order-task", Jsonb({"value": 1}), Jsonb(options or {})),
        ).fetchone()
        assert spawn_row is not None
        task_id, run_id = spawn_row

        claim_row = conn.execute(
            """
            select run_id
            from absurd.claim_task(%s, %s, %s, %s)
            """,
            (queue, "worker", 60, 1),
        ).fetchone()
        assert claim_row is not None
        assert claim_row[0] == run_id

    return task_id, run_id


def test_cancel_and_complete_run_do_not_deadlock(db_dsn):
    queue = "lock_order_complete"
    task_id, run_id = _create_running_task(db_dsn, queue)

    lock_conn = None
    cancel_done = threading.Event()
    cancel_result = {}

    try:
        lock_conn = psycopg.connect(db_dsn)
        lock_conn.execute("set timezone to 'UTC'")
        _set_application_name(lock_conn, "absurd-lock-order-locker-complete")
        lock_conn.execute("set deadlock_timeout = '100ms'")

        lock_conn.execute(
            sql.SQL("select 1 from absurd.{r} where run_id = %s for update").format(
                r=sql.Identifier(f"r_{queue}")
            ),
            (run_id,),
        )

        def cancel_worker():
            try:
                with psycopg.connect(db_dsn, autocommit=True) as cancel_conn:
                    cancel_conn.execute("set timezone to 'UTC'")
                    _set_application_name(
                        cancel_conn, "absurd-lock-order-cancel-complete"
                    )
                    cancel_conn.execute("set deadlock_timeout = '100ms'")
                    cancel_conn.execute("set statement_timeout = '5s'")
                    cancel_conn.execute(
                        "select absurd.cancel_task(%s, %s)",
                        (queue, task_id),
                    )
            except Exception as exc:  # pragma: no cover - surfaced below
                cancel_result["exc"] = exc
            finally:
                cancel_done.set()

        t = threading.Thread(target=cancel_worker, daemon=True)
        t.start()

        _wait_for_lock_wait(db_dsn, "absurd-lock-order-cancel-complete")

        lock_conn.execute("set statement_timeout = '5s'")
        lock_conn.execute(
            "select absurd.complete_run(%s, %s, %s)",
            (queue, run_id, None),
        )
        lock_conn.commit()

        assert cancel_done.wait(5.0), "cancel_task did not finish"
        if "exc" in cancel_result:
            raise cancel_result["exc"]

        with psycopg.connect(db_dsn, autocommit=True) as check_conn:
            task_state = check_conn.execute(
                sql.SQL("select state from absurd.{t} where task_id = %s").format(
                    t=sql.Identifier(f"t_{queue}")
                ),
                (task_id,),
            ).fetchone()[0]
            run_state = check_conn.execute(
                sql.SQL("select state from absurd.{r} where run_id = %s").format(
                    r=sql.Identifier(f"r_{queue}")
                ),
                (run_id,),
            ).fetchone()[0]

        assert task_state == "completed"
        assert run_state == "completed"
    finally:
        if lock_conn is not None:
            try:
                lock_conn.rollback()
            except Exception:
                pass
            lock_conn.close()

        with psycopg.connect(db_dsn, autocommit=True) as cleanup_conn:
            cleanup_conn.execute("set timezone to 'UTC'")
            cleanup_conn.execute("select absurd.drop_queue(%s)", (queue,))


def test_cancel_and_fail_run_do_not_deadlock(db_dsn):
    queue = "lock_order_fail"
    task_id, run_id = _create_running_task(db_dsn, queue, options={"max_attempts": 1})

    lock_conn = None
    cancel_done = threading.Event()
    cancel_result = {}

    try:
        lock_conn = psycopg.connect(db_dsn)
        lock_conn.execute("set timezone to 'UTC'")
        _set_application_name(lock_conn, "absurd-lock-order-locker-fail")
        lock_conn.execute("set deadlock_timeout = '100ms'")

        lock_conn.execute(
            sql.SQL("select 1 from absurd.{r} where run_id = %s for update").format(
                r=sql.Identifier(f"r_{queue}")
            ),
            (run_id,),
        )

        def cancel_worker():
            try:
                with psycopg.connect(db_dsn, autocommit=True) as cancel_conn:
                    cancel_conn.execute("set timezone to 'UTC'")
                    _set_application_name(cancel_conn, "absurd-lock-order-cancel-fail")
                    cancel_conn.execute("set deadlock_timeout = '100ms'")
                    cancel_conn.execute("set statement_timeout = '5s'")
                    cancel_conn.execute(
                        "select absurd.cancel_task(%s, %s)",
                        (queue, task_id),
                    )
            except Exception as exc:  # pragma: no cover - surfaced below
                cancel_result["exc"] = exc
            finally:
                cancel_done.set()

        t = threading.Thread(target=cancel_worker, daemon=True)
        t.start()

        _wait_for_lock_wait(db_dsn, "absurd-lock-order-cancel-fail")

        lock_conn.execute("set statement_timeout = '5s'")
        lock_conn.execute(
            "select absurd.fail_run(%s, %s, %s, %s)",
            (queue, run_id, Jsonb({"message": "boom"}), None),
        )
        lock_conn.commit()

        assert cancel_done.wait(5.0), "cancel_task did not finish"
        if "exc" in cancel_result:
            raise cancel_result["exc"]

        with psycopg.connect(db_dsn, autocommit=True) as check_conn:
            task_state = check_conn.execute(
                sql.SQL("select state from absurd.{t} where task_id = %s").format(
                    t=sql.Identifier(f"t_{queue}")
                ),
                (task_id,),
            ).fetchone()[0]
            run_state = check_conn.execute(
                sql.SQL("select state from absurd.{r} where run_id = %s").format(
                    r=sql.Identifier(f"r_{queue}")
                ),
                (run_id,),
            ).fetchone()[0]

        assert task_state == "failed"
        assert run_state == "failed"
    finally:
        if lock_conn is not None:
            try:
                lock_conn.rollback()
            except Exception:
                pass
            lock_conn.close()

        with psycopg.connect(db_dsn, autocommit=True) as cleanup_conn:
            cleanup_conn.execute("set timezone to 'UTC'")
            cleanup_conn.execute("select absurd.drop_queue(%s)", (queue,))


@pytest.mark.parametrize("storage_mode", ("unpartitioned", "partitioned"))
def test_cleanup_tasks_and_late_extend_claim_do_not_deadlock(db_dsn, storage_mode):
    """cleanup_tasks() must lock a terminal task's runs before the task.

    A late extend_claim() for a run that has already completed locks that run
    and then its task (the run-before-task order of complete_run() and
    fail_run()) before it reports that the run is no longer running.  Cleanup
    used to lock the task first and its runs only when deleting them, so the
    two could each hold what the other waits for.

    The task has two attempts.  A session holding the first, failed attempt's
    run row (as any run-first operation on it does) makes the interleaving
    deterministic: the late extension of the second attempt arrives while
    cleanup is busy with the task.
    """
    queue = f"lock_order_cleanup_{storage_mode[0]}"
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        conn.execute("select absurd.create_queue(%s, %s)", (queue, storage_mode))
        first_run = conn.execute(
            "select run_id from absurd.spawn_task(%s, 't', '{}', %s)",
            (queue, Jsonb({"max_attempts": 2})),
        ).fetchone()[0]
        assert conn.execute(
            "select run_id from absurd.claim_task(%s, 'w', 60, 1)", (queue,)
        ).fetchone() == (first_run,)
        conn.execute(
            "select absurd.fail_run(%s, %s, %s, null)",
            (queue, first_run, Jsonb({"message": "first attempt"})),
        )
        second_run = conn.execute(
            "select run_id from absurd.claim_task(%s, 'w', 60, 1)", (queue,)
        ).fetchone()[0]
        assert second_run != first_run
        conn.execute("select absurd.complete_run(%s, %s, null)", (queue, second_run))

    lock_conn = None
    results = {}
    done = {}

    def start(name, statement, params):
        done[name] = threading.Event()

        def run():
            try:
                with psycopg.connect(db_dsn, autocommit=True) as conn:
                    _set_application_name(conn, f"absurd-cleanup-order-{name}")
                    conn.execute("set deadlock_timeout = '100ms'")
                    conn.execute("set statement_timeout = '10s'")
                    results[name] = conn.execute(statement, params).fetchone()
            except Exception as exc:  # surfaced below
                results[name] = exc
            finally:
                done[name].set()

        threading.Thread(target=run, daemon=True).start()

    def finished_or_waiting(name, timeout_seconds=5.0):
        deadline = time.monotonic() + timeout_seconds
        with psycopg.connect(db_dsn, autocommit=True) as conn:
            while time.monotonic() < deadline:
                if done[name].is_set():
                    return
                row = conn.execute(
                    "select wait_event_type from pg_stat_activity "
                    "where application_name = %s",
                    (f"absurd-cleanup-order-{name}",),
                ).fetchone()
                if row and row[0] == "Lock":
                    return
                time.sleep(0.01)
        raise AssertionError(f"{name} neither finished nor waited for a lock")

    try:
        lock_conn = psycopg.connect(db_dsn)
        _set_application_name(lock_conn, "absurd-cleanup-order-locker")
        lock_conn.execute(
            sql.SQL("select 1 from absurd.{r} where run_id = %s for update").format(
                r=sql.Identifier(f"r_{queue}")
            ),
            (first_run,),
        )

        start("cleanup", "select absurd.cleanup_tasks(%s, 0, 10)", (queue,))
        finished_or_waiting("cleanup")
        start(
            "extend",
            "select absurd.extend_claim(%s, %s, %s)",
            (queue, second_run, 60),
        )
        finished_or_waiting("extend")

        lock_conn.rollback()

        for name in ("cleanup", "extend"):
            assert done[name].wait(10.0), f"{name} did not finish"
        for name in ("cleanup", "extend"):
            assert not isinstance(results[name], psycopg.errors.DeadlockDetected), (
                name,
                results[name],
            )

        # Cleanup skips a task while one of its runs is locked; the late
        # extension is refused because its run is no longer running.
        assert results["cleanup"] == (0,), results["cleanup"]
        assert isinstance(results["extend"], psycopg.errors.RaiseException), results[
            "extend"
        ]
        assert "is not currently running" in str(results["extend"])

        # Once nothing holds the task's runs, the next batch removes it.
        with psycopg.connect(db_dsn, autocommit=True) as conn:
            assert conn.execute(
                "select absurd.cleanup_tasks(%s, 0, 10)", (queue,)
            ).fetchone() == (1,)
            for prefix in "trcw":
                assert conn.execute(
                    sql.SQL("select count(*) from absurd.{}").format(
                        sql.Identifier(f"{prefix}_{queue}")
                    )
                ).fetchone() == (0,)
    finally:
        if lock_conn is not None:
            try:
                lock_conn.rollback()
            except Exception:
                pass
            lock_conn.close()

        with psycopg.connect(db_dsn, autocommit=True) as cleanup_conn:
            cleanup_conn.execute("select absurd.drop_queue(%s)", (queue,))
