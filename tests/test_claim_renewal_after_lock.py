import threading
import time

import psycopg
import pytest

from test_lock_ordering import (
    _create_running_task,
    _set_application_name,
    _wait_for_lock_wait,
)

# A renewal that waits on a row lock must count from when it writes the claim, not from before the
# wait: a time read first renews to an expiry already in the past once the wait outlasts the
# extension, and the run loses a claim it was actively renewing.
HOLD_SECONDS = 2.5
EXTEND_SECONDS = 1

_EXTEND = "select absurd.extend_claim(%(queue)s, %(run)s, %(seconds)s)"
_CHECKPOINT = (
    "select absurd.set_task_checkpoint_state("
    "%(queue)s, %(task)s, 'renewal', '{}'::jsonb, %(run)s, %(seconds)s)"
)


@pytest.mark.parametrize(
    ("renewal", "held"),
    [
        (_EXTEND, "select 1 from absurd.r_{queue} where run_id = %(run)s for update"),
        (_EXTEND, "select 1 from absurd.t_{queue} where task_id = %(task)s for update"),
        (_CHECKPOINT, "select 1 from absurd.r_{queue} where run_id = %(run)s for update"),
        (
            _CHECKPOINT,
            "select 1 from absurd.c_{queue} where task_id = %(task)s "
            "and checkpoint_name = 'renewal' for update",
        ),
    ],
    ids=["extend-run-row", "extend-task-row", "checkpoint-run-row", "checkpoint-row"],
)
def test_renewal_after_a_lock_wait_counts_from_the_write(db_dsn, renewal, held, request):
    # One queue per case, so each claims its own task.
    queue = "renewal_" + request.node.callspec.id.replace("-", "_")
    task_id, run_id = _create_running_task(db_dsn, queue)
    names = {"queue": queue, "task": task_id, "run": run_id, "seconds": EXTEND_SECONDS}
    with psycopg.connect(db_dsn, autocommit=True) as setup:
        setup.execute(_CHECKPOINT, names)

    failure = []
    with psycopg.connect(db_dsn) as holder:
        holder.execute(held.format(queue=queue), names)

        def renew():
            try:
                with psycopg.connect(db_dsn, autocommit=True) as conn:
                    _set_application_name(conn, "renewal-after-lock")
                    conn.execute(renewal, names)
            except Exception as error:  # surfaced below
                failure.append(error)

        worker = threading.Thread(target=renew)
        worker.start()
        _wait_for_lock_wait(db_dsn, "renewal-after-lock")
        time.sleep(HOLD_SECONDS)
        holder.rollback()
        worker.join(timeout=10)
    assert not failure and not worker.is_alive()

    with psycopg.connect(db_dsn, autocommit=True) as conn:
        live = conn.execute(
            f"select claim_expires_at > clock_timestamp() from absurd.r_{queue} where run_id = %s",
            (run_id,),
        ).fetchone()
        # The database is shared by the session; leave no queue behind for listing tests.
        conn.execute("select absurd.drop_queue(%s)", (queue,))
    assert live == (True,)
