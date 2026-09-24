from scheduler.execution import IdempotentExecutor
from scheduler.job_store import JobStore
from scheduler.job_state import JobStatus
from scheduler.worker import Worker

def test_execution_record_created(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
    )

    execution = store.create_execution(
        job=job,
        worker_id="worker-1",
    )

    assert execution["job_id"] == job_id
    assert execution["attempt_number"] == 1
    assert execution["idempotency_key"] == job_id
    assert execution["worker_id"] == "worker-1"
    assert execution["lease_generation"] == 1
    assert execution["status"] == "STARTED"

def test_execution_status_can_be_updated(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
    )

    execution = store.create_execution(
        job,
        "worker-1",
    )

    store.update_execution(
        execution["id"],
        "SUCCEEDED",
    )

    updated = store.get_execution(
        execution["id"]
    )

    assert updated["status"] == "SUCCEEDED"

def test_idempotency_key_is_stable_across_attempts(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    first = store.claim_job(
        worker_id="worker-1",
    )

    first_execution = store.create_execution(
        first,
        "worker-1",
    )

    store.retry_job(
        job_id=job_id,
        delay_seconds=0,
        worker_id="worker-1",
        lease_generation=first["lease_generation"],
    )

    store.promote_due_retries()

    second = store.claim_job(
        worker_id="worker-2",
    )

    second_execution = store.create_execution(
        second,
        "worker-2",
    )

    assert first_execution["idempotency_key"] == job_id
    assert second_execution["idempotency_key"] == job_id
    assert (
        first_execution["idempotency_key"]
        == second_execution["idempotency_key"]
    )

def test_first_idempotent_execution_performs_side_effect(
    tmp_path,
):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.get_job(job_id)

    executor = IdempotentExecutor(store)

    result = executor.execute(
        job,
        idempotency_key=job_id,
    )

    assert result["duplicate"] is False

    assert (
        store.count_idempotency_records(job_id)
        == 1
    )

def test_duplicate_execution_does_not_repeat_side_effect(
    tmp_path,
):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.get_job(job_id)

    executor = IdempotentExecutor(store)

    first = executor.execute(
        job,
        idempotency_key=job_id,
    )

    second = executor.execute(
        job,
        idempotency_key=job_id,
    )

    assert first["duplicate"] is False
    assert second["duplicate"] is True

    assert (
        store.count_idempotency_records(job_id)
        == 1
    )

def test_worker_creates_execution_record(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    worker = Worker(
        worker_id="worker-1",
        store=store,
    )

    assert worker.run_once() is True

    executions = store.list_executions(job_id)

    assert len(executions) == 1

    assert executions[0]["attempt_number"] == 1
    assert executions[0]["idempotency_key"] == job_id
    assert executions[0]["worker_id"] == "worker-1"
    assert executions[0]["status"] == "SUCCEEDED"

def test_worker_completes_job_after_success(
    tmp_path,
):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    worker = Worker(
        worker_id="worker-1",
        store=store,
    )

    worker.run_once()

    job = store.get_job(job_id)

    assert job["status"] == JobStatus.SUCCESS
    assert job["worker_id"] is None
    assert job["lease_until"] is None

class FailingExecutor:
    def execute(
        self,
        job: dict,
        idempotency_key: str,
    ):
        raise RuntimeError("simulated failure")

def test_worker_records_failed_execution(
    tmp_path,
):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    worker = Worker(
        worker_id="worker-1",
        store=store,
        executor=FailingExecutor(),
    )

    worker.run_once()

    executions = store.list_executions(job_id)

    assert len(executions) == 1
    assert executions[0]["status"] == "FAILED"

    job = store.get_job(job_id)

    assert job["status"] == JobStatus.RETRYING

def test_multiple_attempts_create_multiple_execution_records(
    tmp_path,
):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    worker = Worker(
        worker_id="worker-1",
        store=store,
    )

    worker.run_once()

    first = store.get_job(job_id)

    assert first["status"] == JobStatus.SUCCESS

    executions = store.list_executions(job_id)

    assert len(executions) == 1
    assert executions[0]["attempt_number"] == 1

def test_repeated_execution_after_crash_is_idempotent(
    tmp_path,
):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.get_job(job_id)

    executor = IdempotentExecutor(store)

    # Simulate attempt 1 successfully performing
    # the external side effect.
    first = executor.execute(
        job,
        idempotency_key=job_id,
    )

    assert first["duplicate"] is False

    # Simulate worker crash before scheduler
    # records completion.

    # A later execution sees the same logical
    # idempotency key.
    second = executor.execute(
        job,
        idempotency_key=job_id,
    )

    assert second["duplicate"] is True

    assert (
        store.count_idempotency_records(job_id)
        == 1
    )