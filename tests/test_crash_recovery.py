import time

from scheduler.job_store import JobStore
from scheduler.job_state import JobStatus

def test_expired_running_job_is_recovered(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
        lease_seconds=0.05,
    )

    assert job["status"] == JobStatus.RUNNING
    assert job["worker_id"] == "worker-1"

    time.sleep(0.1)

    recovered = store.recover_expired_jobs()

    assert recovered == 1

    updated = store.get_job(job_id)

    assert updated["status"] == JobStatus.PENDING
    assert updated["worker_id"] is None
    assert updated["lease_until"] is None
    assert updated["lease_generation"] == 1

def test_non_expired_job_is_not_recovered(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    store.claim_job(
        worker_id="worker-1",
        lease_seconds=30,
    )

    recovered = store.recover_expired_jobs()

    assert recovered == 0

    job = store.get_job(job_id)

    assert job["status"] == JobStatus.RUNNING
    assert job["worker_id"] == "worker-1"

def test_recovered_job_can_be_claimed_by_new_worker(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    first = store.claim_job(
        worker_id="worker-1",
        lease_seconds=0.05,
    )

    assert first["lease_generation"] == 1

    time.sleep(0.1)

    store.recover_expired_jobs()

    second = store.claim_job(
        worker_id="worker-2",
        lease_seconds=30,
    )

    assert second is not None
    assert second["id"] == job_id
    assert second["status"] == JobStatus.RUNNING
    assert second["worker_id"] == "worker-2"
    assert second["lease_generation"] == 2
    assert second["attempt_count"] == 2

def test_current_worker_can_complete_job(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
        lease_seconds=30,
    )

    completed = store.complete_job(
        job_id=job_id,
        worker_id="worker-1",
        lease_generation=job["lease_generation"],
    )

    assert completed is True

    updated = store.get_job(job_id)

    assert updated["status"] == JobStatus.SUCCESS
    assert updated["worker_id"] is None
    assert updated["lease_until"] is None

def test_stale_worker_cannot_complete_recovered_job(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    worker_a_job = store.claim_job(
        worker_id="worker-A",
        lease_seconds=0.05,
    )

    assert worker_a_job["lease_generation"] == 1

    time.sleep(0.1)

    store.recover_expired_jobs()

    worker_b_job = store.claim_job(
        worker_id="worker-B",
        lease_seconds=30,
    )

    assert worker_b_job["lease_generation"] == 2

    completed = store.complete_job(
        job_id=job_id,
        worker_id="worker-A",
        lease_generation=1,
    )

    assert completed is False

    current = store.get_job(job_id)

    assert current["status"] == JobStatus.RUNNING
    assert current["worker_id"] == "worker-B"
    assert current["lease_generation"] == 2

def test_stale_worker_cannot_retry_recovered_job(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    worker_a_job = store.claim_job(
        worker_id="worker-A",
        lease_seconds=0.05,
    )

    time.sleep(0.1)

    store.recover_expired_jobs()

    worker_b_job = store.claim_job(
        worker_id="worker-B",
        lease_seconds=30,
    )

    retry_result = store.retry_job(
        job_id=job_id,
        delay_seconds=10,
        worker_id="worker-A",
        lease_generation=worker_a_job["lease_generation"],
    )

    assert retry_result is False

    current = store.get_job(job_id)

    assert current["status"] == JobStatus.RUNNING
    assert current["worker_id"] == "worker-B"
    assert current["lease_generation"] == worker_b_job["lease_generation"]

def test_current_worker_can_retry(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
        lease_seconds=30,
    )

    retry_result = store.retry_job(
        job_id=job_id,
        delay_seconds=10,
        worker_id="worker-1",
        lease_generation=job["lease_generation"],
    )

    assert retry_result is True

    updated = store.get_job(job_id)

    assert updated["status"] == JobStatus.RETRYING
    assert updated["worker_id"] is None
    assert updated["lease_until"] is None

def test_expired_worker_cannot_complete(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
        lease_seconds=0.05,
    )

    time.sleep(0.1)

    completed = store.complete_job(
        job_id=job_id,
        worker_id="worker-1",
        lease_generation=job["lease_generation"],
    )

    assert completed is False

    current = store.get_job(job_id)

    assert current["status"] == JobStatus.RUNNING

def test_wrong_generation_cannot_complete(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
        lease_seconds=30,
    )

    completed = store.complete_job(
        job_id=job_id,
        worker_id="worker-1",
        lease_generation=job["lease_generation"] + 1,
    )

    assert completed is False

    current = store.get_job(job_id)

    assert current["status"] == JobStatus.RUNNING
    assert current["worker_id"] == "worker-1"

def test_recovery_only_recovers_expired_jobs(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    expired_id = store.create_job(
        "email",
        {"name": "expired"},
    )

    healthy_id = store.create_job(
        "email",
        {"name": "healthy"},
    )

    store.claim_job(
        worker_id="worker-1",
        lease_seconds=0.05,
    )

    time.sleep(0.1)

    # Claim the second job with a separate worker.
    healthy_job = store.claim_job(
        worker_id="worker-2",
        lease_seconds=30,
    )

    assert healthy_job["id"] == healthy_id

    recovered = store.recover_expired_jobs()

    assert recovered == 1

    expired_job = store.get_job(expired_id)
    healthy_job = store.get_job(healthy_id)

    assert expired_job["status"] == JobStatus.PENDING
    assert healthy_job["status"] == JobStatus.RUNNING
    assert healthy_job["worker_id"] == "worker-2"