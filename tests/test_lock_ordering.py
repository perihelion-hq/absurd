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


def _completed_tasks(conn, queue, count):
    """Spawn, claim and complete count tasks, oldest completion first."""
    task_ids = []
    for i in range(count):
        task_id, run_id = conn.execute(
            "select task_id, run_id from absurd.spawn_task(%s, %s, '{}')",
            (queue, f"t{i}"),
        ).fetchone()
        assert conn.execute(
            "select run_id from absurd.claim_task(%s, 'w', 60, 1)", (queue,)
        ).fetchone() == (run_id,)
        conn.execute("select absurd.complete_run(%s, %s, null)", (queue, run_id))
        task_ids.append((task_id, run_id))
    return task_ids


def _remaining_tasks(conn, queue):
    return {
        r[0]
        for r in conn.execute(
            sql.SQL("select task_id from absurd.{}").format(
                sql.Identifier(f"t_{queue}")
            )
        )
    }


@pytest.mark.parametrize("storage_mode", ("unpartitioned", "partitioned"))
@pytest.mark.parametrize("locked", ("task", "run"))
def test_cleanup_tasks_moves_past_a_locked_oldest_task(db_dsn, storage_mode, locked):
    """A locked oldest task must not stall cleanup.

    Skipped tasks do not count against the limit: with p_limit = 1 and the
    oldest eligible task (or its run) locked elsewhere, cleanup deletes the
    next eligible task instead of returning 0 on every call.
    """
    queue = f"cleanup_skip_{locked}_{storage_mode[0]}"
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        conn.execute("select absurd.create_queue(%s, %s)", (queue, storage_mode))
        try:
            (oldest, oldest_run), (following, _), (newest, _) = _completed_tasks(
                conn, queue, 3
            )
            with psycopg.connect(db_dsn) as locker:
                if locked == "task":
                    statement = "select 1 from absurd.{} where task_id = %s for update"
                    table, key = f"t_{queue}", oldest
                else:
                    statement = "select 1 from absurd.{} where run_id = %s for update"
                    table, key = f"r_{queue}", oldest_run
                locker.execute(sql.SQL(statement).format(sql.Identifier(table)), (key,))

                with psycopg.connect(db_dsn) as cleaner:
                    assert cleaner.execute(
                        "select absurd.cleanup_tasks(%s, 0, 1)", (queue,)
                    ).fetchone() == (1,)
                    # While the cleanup is uncommitted, it holds no lock on
                    # the skipped task or its runs, nor on the newest task.
                    for table, column, key in (
                        (f"t_{queue}", "task_id", oldest),
                        (f"r_{queue}", "run_id", oldest_run),
                        (f"t_{queue}", "task_id", newest),
                    ):
                        if key == (oldest if locked == "task" else oldest_run):
                            continue  # held by the locker
                        conn.execute(
                            sql.SQL(
                                "select 1 from absurd.{} where {} = %s for update nowait"
                            ).format(sql.Identifier(table), sql.Identifier(column)),
                            (key,),
                        )
                    cleaner.commit()
                assert _remaining_tasks(conn, queue) == {oldest, newest}
                locker.rollback()

            assert conn.execute(
                "select absurd.cleanup_tasks(%s, 0, 10)", (queue,)
            ).fetchone() == (2,)
            assert _remaining_tasks(conn, queue) == set()
        finally:
            conn.execute("select absurd.drop_queue(%s)", (queue,))


@pytest.mark.parametrize("storage_mode", ("unpartitioned", "partitioned"))
def test_concurrent_cleanups_delete_different_tasks(db_dsn, storage_mode):
    """A cleanup moves on past the tasks a concurrent cleanup is deleting."""
    queue = f"cleanup_concurrent_{storage_mode[0]}"
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        conn.execute("select absurd.create_queue(%s, %s)", (queue, storage_mode))
        try:
            (oldest, _), (following, _), (newest, _) = _completed_tasks(conn, queue, 3)
            with psycopg.connect(db_dsn) as first:
                assert first.execute(
                    "select absurd.cleanup_tasks(%s, 0, 1)", (queue,)
                ).fetchone() == (1,)
                assert conn.execute(
                    "select absurd.cleanup_tasks(%s, 0, 1)", (queue,)
                ).fetchone() == (1,)
                first.commit()
            assert _remaining_tasks(conn, queue) == {newest}
        finally:
            conn.execute("select absurd.drop_queue(%s)", (queue,))


@pytest.mark.parametrize("storage_mode", ("unpartitioned", "partitioned"))
def test_cleanup_tasks_past_locked_tasks_deletes_no_more_than_the_limit(
    db_dsn, storage_mode
):
    """A larger page read after skipped tasks still deletes at most p_limit."""
    queue = f"cleanup_cap_{storage_mode[0]}"
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        conn.execute("select absurd.create_queue(%s, %s)", (queue, storage_mode))
        try:
            tasks = [task_id for task_id, _ in _completed_tasks(conn, queue, 6)]
            with psycopg.connect(db_dsn) as locker:
                locker.execute(
                    sql.SQL(
                        "select 1 from absurd.{} where task_id = any(%s) for update"
                    ).format(sql.Identifier(f"t_{queue}")),
                    (tasks[:2],),
                )
                assert conn.execute(
                    "select absurd.cleanup_tasks(%s, 0, 2)", (queue,)
                ).fetchone() == (2,)
                assert _remaining_tasks(conn, queue) == {*tasks[:2], *tasks[4:]}
                locker.rollback()
        finally:
            conn.execute("select absurd.drop_queue(%s)", (queue,))


def test_cleanup_tasks_bounds_its_pages_behind_a_long_locked_prefix(db_dsn):
    """A long locked prefix must not grow a small cleanup's pages to backlog size.

    Each page's runs are locked (and released again with the page's
    subtransaction) before locked tasks are rejected.  With the oldest 2100 of
    4200 eligible tasks locked and p_limit = 1, the page that reaches the first
    unlocked task holds at most 1000 tasks, so at most 999 tasks after it have
    their runs locked; an unbounded doubling page would reach about 2000 of
    them.  A row a rolled-back subtransaction locked keeps that locker in its
    xmax, so a run whose xmax changed during the call was locked by it.
    """
    queue = "cleanup_page_cap"
    total, locked = 4200, 2100
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        conn.execute("select absurd.create_queue(%s)", (queue,))
        try:
            conn.execute(
                sql.SQL(
                    """
                    do $$
                    declare r record;
                    begin
                      for i in 1..{total} loop
                        perform absurd.spawn_task({queue}, 't' || i, '{{}}');
                      end loop;
                      for r in select * from absurd.claim_task({queue}, 'w', 60, {total})
                      loop
                        perform absurd.complete_run({queue}, r.run_id, null);
                      end loop;
                    end $$
                    """
                ).format(total=sql.Literal(total), queue=sql.Literal(queue))
            )
            ordered = sql.SQL(
                "select t.task_id from absurd.{t} t "
                "join absurd.{r} r on r.run_id = t.last_attempt_run "
                "order by r.completed_at, t.task_id"
            ).format(t=sql.Identifier(f"t_{queue}"), r=sql.Identifier(f"r_{queue}"))
            tasks = [row[0] for row in conn.execute(ordered)]
            assert len(tasks) == total
            runs_xmax = sql.SQL(
                "select run_id, xmax::text from absurd.{} where task_id = any(%s)"
            ).format(sql.Identifier(f"r_{queue}"))
            later = tasks[locked + 1 :]
            before = dict(conn.execute(runs_xmax, (later,)).fetchall())
            with psycopg.connect(db_dsn) as locker:
                locker.execute(
                    sql.SQL(
                        "select 1 from absurd.{} where task_id = any(%s) for update"
                    ).format(sql.Identifier(f"t_{queue}")),
                    (tasks[:locked],),
                )
                assert conn.execute(
                    "select absurd.cleanup_tasks(%s, 0, 1)", (queue,)
                ).fetchone() == (1,)
                assert _remaining_tasks(conn, queue) == set(tasks) - {tasks[locked]}
                after = dict(conn.execute(runs_xmax, (later,)).fetchall())
                locked_after = sum(after[run] != before[run] for run in before)
                assert 0 < locked_after < 1000, locked_after
                locker.rollback()
        finally:
            conn.execute("select absurd.drop_queue(%s)", (queue,))
