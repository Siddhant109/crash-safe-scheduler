from datetime import datetime, timezone

from scheduler.job_store import JobStore
from scheduler.job_state import JobStatus


def test_claim_creates_lease(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
        lease_seconds=30,
    )

    assert job is not None
    assert job["id"] == job_id
    assert job["status"] == JobStatus.RUNNING
    assert job["worker_id"] == "worker-1"
    assert job["lease_until"] is not None
    assert job["lease_generation"] == 1
    assert job["attempt_count"] == 1

    lease_until = datetime.fromisoformat(
        job["lease_until"]
    )

    assert lease_until > datetime.now(timezone.utc)

def test_running_job_cannot_be_claimed_by_second_worker(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    first = store.claim_job(
        worker_id="worker-1",
    )

    second = store.claim_job(
        worker_id="worker-2",
    )

    assert first is not None
    assert second is None

def test_correct_worker_can_heartbeat(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
        lease_seconds=30,
    )

    old_lease = job["lease_until"]

    success = store.heartbeat(
        job_id=job_id,
        worker_id="worker-1",
        lease_generation=job["lease_generation"],
        lease_seconds=60,
    )

    assert success is True

    updated = store.get_job(job_id)

    assert updated["lease_until"] != old_lease

def test_wrong_worker_cannot_heartbeat(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
    )

    old_lease = job["lease_until"]

    success = store.heartbeat(
        job_id=job_id,
        worker_id="worker-2",
        lease_generation=job["lease_generation"],
    )

    assert success is False

    updated = store.get_job(job_id)

    assert updated["lease_until"] == old_lease

def test_wrong_lease_generation_cannot_heartbeat(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
    )

    success = store.heartbeat(
        job_id=job_id,
        worker_id="worker-1",
        lease_generation=job["lease_generation"] + 1,
    )

    assert success is False

import time


def test_expired_lease_is_detected(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    store.claim_job(
        worker_id="worker-1",
        lease_seconds=0.05,
    )

    assert store.is_lease_expired(job_id) is False

    time.sleep(0.1)

    assert store.is_lease_expired(job_id) is True

def test_expired_worker_cannot_heartbeat(tmp_path):
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

    success = store.heartbeat(
        job_id=job_id,
        worker_id="worker-1",
        lease_generation=job["lease_generation"],
    )

    assert success is False

def test_retry_clears_lease(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
    )

    assert job["worker_id"] == "worker-1"
    assert job["lease_until"] is not None

    store.retry_job(
        job_id,
        delay_seconds=10,
        worker_id="worker-1",
        lease_generation=job["lease_generation"]
    )

    updated = store.get_job(job_id)

    assert updated["status"] == JobStatus.RETRYING
    assert updated["worker_id"] is None
    assert updated["lease_until"] is None

def test_success_clears_lease(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
    )

    assert job["worker_id"] == "worker-1"
    assert job["lease_until"] is not None

    store.transition_job(
        job_id,
        JobStatus.SUCCESS,
    )

    updated = store.get_job(job_id)

    assert updated["status"] == JobStatus.SUCCESS
    assert updated["worker_id"] is None
    assert updated["lease_until"] is None

def test_lease_generation_increments_on_claim(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    first = store.claim_job(
        worker_id="worker-1",
    )

    assert first["lease_generation"] == 1

    # Manually make it claimable again for this isolated test.
    with store._connect() as connection:
        connection.execute(
            """
            UPDATE jobs
            SET
                status = ?,
                worker_id = NULL,
                lease_until = NULL
            WHERE id = ?
            """,
            (
                JobStatus.PENDING.value,
                job_id,
            ),
        )

    second = store.claim_job(
        worker_id="worker-2",
    )

    assert second["lease_generation"] == 2

import pytest


def test_claim_rejects_empty_worker_id(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    with pytest.raises(ValueError):
        store.claim_job(worker_id="")


def test_claim_rejects_invalid_lease_duration(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    with pytest.raises(ValueError):
        store.claim_job(
            worker_id="worker-1",
            lease_seconds=0,
        )


def test_heartbeat_rejects_invalid_lease_duration(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
    )

    with pytest.raises(ValueError):
        store.heartbeat(
            job_id=job_id,
            worker_id="worker-1",
            lease_generation=job["lease_generation"],
            lease_seconds=0,
        )