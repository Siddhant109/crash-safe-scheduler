from scheduler.job_store import JobStore
from scheduler.worker import Worker
from scheduler.worker_state import WorkerState


def test_worker_shutdown_prevents_new_claim(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    worker = Worker(
        worker_id="worker-1",
        store=store,
    )

    worker.shutdown()

    assert worker.state == WorkerState.STOPPING

    result = worker.run_once()

    assert result is False

    job = store.get_job(job_id)

    assert job["status"].value == "PENDING"

def test_shutdown_is_idempotent(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    worker = Worker(
        worker_id="worker-1",
        store=store,
    )

    worker.shutdown()
    worker.shutdown()

    assert worker.state == WorkerState.STOPPING

class ShutdownExecutor:
    def __init__(self, worker):
        self.worker = worker

    def execute(self, job, idempotency_key):
        self.worker.shutdown()

        return {
            "job_id": job["id"],
            "status": "completed",
        }

def test_worker_finishes_active_job_before_stopping(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    worker = Worker(
        worker_id="worker-1",
        store=store,
    )

    worker.executor = ShutdownExecutor(worker)

    result = worker.run_once()

    assert result is True
    assert worker.state == WorkerState.STOPPED

    job = store.get_job(job_id)

    assert job["status"].value == "SUCCESS"

from tests.helpers import FailingExecutor

def test_execution_failure_is_retried(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
        max_attempts=3,
    )

    worker = Worker(
        worker_id="worker-1",
        store=store,
        executor=FailingExecutor(),
    )

    result = worker.run_once()

    assert result is True

    job = store.get_job(job_id)

    assert job["status"].value == "RETRYING"
    assert job["attempt_count"] == 1

import time

from scheduler.job_state import JobStatus


def test_crashed_worker_job_is_recovered(tmp_path):
    db_path = tmp_path / "scheduler.db"

    store = JobStore(db_path)

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    crashed_worker = JobStore(db_path)

    claimed = crashed_worker.claim_job(
        worker_id="worker-1",
        lease_seconds=0.5,
    )

    assert claimed is not None
    assert claimed["id"] == job_id
    assert claimed["status"] == JobStatus.RUNNING

    # Simulate worker process disappearing.
    time.sleep(0.6)

    recovery_store = JobStore(db_path)

    recovered = recovery_store.recover_expired_jobs()

    assert recovered == 1

    job = recovery_store.get_job(job_id)

    assert job["status"] == JobStatus.PENDING
    assert job["worker_id"] is None
    assert job["lease_until"] is None

def test_recovered_job_can_be_claimed_by_new_worker(tmp_path):
    db_path = tmp_path / "scheduler.db"

    store = JobStore(db_path)

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    first = store.claim_job(
        worker_id="worker-1",
        lease_seconds=0.5,
    )

    assert first["id"] == job_id

    import time
    time.sleep(0.6)

    store.recover_expired_jobs()

    second = store.claim_job(
        worker_id="worker-2",
        lease_seconds=30,
    )

    assert second is not None
    assert second["id"] == job_id
    assert second["worker_id"] == "worker-2"
    assert second["lease_generation"] == (
        first["lease_generation"] + 1
    )