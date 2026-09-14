import pytest

from scheduler.retry import calculate_backoff
from scheduler.job_store import JobStore
from scheduler.job_state import JobStatus

def test_backoff_increases_exponentially():
    assert calculate_backoff(1) == 1
    assert calculate_backoff(2) == 2
    assert calculate_backoff(3) == 4
    assert calculate_backoff(4) == 8

def test_backoff_has_maximum():
    assert calculate_backoff(
        10,
        base_delay=1,
        max_delay=60,
    ) == 60

def test_backoff_rejects_invalid_attempt():
    with pytest.raises(ValueError):
        calculate_backoff(0)

def test_claim_increments_attempt_count(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
    )

    job = store.claim_job(worker_id="worker-1")

    assert job["id"] == job_id
    assert job["attempt_count"] == 1

def test_failed_job_enters_retrying(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
    )

    job = store.claim_job(worker_id="worker-1")

    assert job["attempt_count"] == 1

    store.retry_job(
        job_id,
        delay_seconds=10,
        worker_id="worker-1",
        lease_generation=job["lease_generation"]
    )

    retrying_job = store.get_job(job_id)

    assert retrying_job["status"] == JobStatus.RETRYING
    assert retrying_job["attempt_count"] == 1

def test_retry_backoff_prevents_immediate_claim(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
    )

    job = store.claim_job(worker_id="worker-1")

    store.retry_job(
        job_id,
        delay_seconds=60,
        worker_id="worker-1",
        lease_generation=job["lease_generation"]
    )

    claimed = store.claim_job(worker_id="worker-1")

    assert claimed is None

def test_retry_with_zero_delay_becomes_claimable(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
    )

    job = store.claim_job(worker_id="worker-1")

    store.retry_job(
        job_id,
        delay_seconds=0,
        worker_id="worker-1",
        lease_generation=job["lease_generation"]
    )

    store.transition_job(
        job_id,
        JobStatus.PENDING,
    )

    job = store.claim_job(worker_id="worker-1")

    assert job["id"] == job_id
    assert job["attempt_count"] == 2

def test_job_fails_after_max_attempts(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
        max_attempts=2,
    )

    first = store.claim_job(worker_id="worker-1")

    assert first["attempt_count"] == 1

    store.retry_job(
        job_id,
        delay_seconds=0,
        worker_id="worker-1",
        lease_generation=first["lease_generation"]
    )

    store.transition_job(
        job_id,
        JobStatus.PENDING,
    )

    second = store.claim_job(worker_id="worker-1")

    assert second["attempt_count"] == 2

    store.retry_job(
        job_id,
        delay_seconds=0,
        worker_id="worker-1",
        lease_generation=second["lease_generation"]
    )

    job = store.get_job(job_id)

    assert job["status"] == JobStatus.FAILED