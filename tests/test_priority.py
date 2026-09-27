from scheduler.job_priority import JobPriority
from scheduler.job_store import JobStore
from scheduler.job_state import JobStatus

def test_default_priority_is_normal(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.get_job(job_id)

    assert job["priority"] == JobPriority.NORMAL

def test_priority_is_persisted(tmp_path):
    db_path = tmp_path / "scheduler.db"

    store = JobStore(db_path)

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
        priority=JobPriority.CRITICAL,
    )

    new_store = JobStore(db_path)

    job = new_store.get_job(job_id)

    assert job["priority"] == JobPriority.CRITICAL

def test_higher_priority_job_is_claimed_first(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    normal_id = store.create_job(
        "normal",
        {},
        priority=JobPriority.NORMAL,
    )

    critical_id = store.create_job(
        "critical",
        {},
        priority=JobPriority.CRITICAL,
    )

    job = store.claim_job(
        worker_id="worker-1",
    )

    assert job["id"] == critical_id
    assert job["id"] != normal_id
    assert job["priority"] == JobPriority.CRITICAL

def test_jobs_are_claimed_by_priority(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    low_id = store.create_job(
        "low",
        {},
        priority=JobPriority.LOW,
    )

    normal_id = store.create_job(
        "normal",
        {},
        priority=JobPriority.NORMAL,
    )

    high_id = store.create_job(
        "high",
        {},
        priority=JobPriority.HIGH,
    )

    critical_id = store.create_job(
        "critical",
        {},
        priority=JobPriority.CRITICAL,
    )

    first = store.claim_job("worker-1")
    second = store.claim_job("worker-1")
    third = store.claim_job("worker-1")
    fourth = store.claim_job("worker-1")

    assert first["id"] == critical_id
    assert second["id"] == high_id
    assert third["id"] == normal_id
    assert fourth["id"] == low_id

def test_same_priority_preserves_fifo_order(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    first_id = store.create_job(
        "first",
        {},
        priority=JobPriority.HIGH,
    )

    second_id = store.create_job(
        "second",
        {},
        priority=JobPriority.HIGH,
    )

    third_id = store.create_job(
        "third",
        {},
        priority=JobPriority.HIGH,
    )

    first = store.claim_job("worker-1")
    second = store.claim_job("worker-1")
    third = store.claim_job("worker-1")

    assert first["id"] == first_id
    assert second["id"] == second_id
    assert third["id"] == third_id

import pytest


def test_invalid_priority_is_rejected(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    with pytest.raises(ValueError):
        store.create_job(
            "email",
            {},
            priority=99,
        )