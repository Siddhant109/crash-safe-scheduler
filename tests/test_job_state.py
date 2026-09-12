from scheduler.job_state import JobStatus, can_transition


def test_pending_can_transition_to_running():
    assert can_transition(
        JobStatus.PENDING,
        JobStatus.RUNNING,
    )

def test_pending_cannot_transition_to_success():
    assert not can_transition(
        JobStatus.PENDING,
        JobStatus.SUCCESS,
    )

def test_running_can_complete():
    assert can_transition(
        JobStatus.RUNNING,
        JobStatus.SUCCESS,
    )


def test_running_can_retry():
    assert can_transition(
        JobStatus.RUNNING,
        JobStatus.RETRYING,
    )


def test_running_can_fail():
    assert can_transition(
        JobStatus.RUNNING,
        JobStatus.FAILED,
    )

def test_retrying_returns_to_pending():
    assert can_transition(
        JobStatus.RETRYING,
        JobStatus.PENDING,
    )

def test_retrying_returns_to_pending():
    assert can_transition(
        JobStatus.RETRYING,
        JobStatus.PENDING,
    )

from scheduler.job_state import JobStatus
from scheduler.job_store import JobStore


def test_job_can_transition_to_running(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
    )

    store.transition_job(
        job_id,
        JobStatus.RUNNING,
    )

    job = store.get_job(job_id)

    assert job["status"] == JobStatus.RUNNING

import pytest


def test_invalid_transition_is_rejected(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
    )

    with pytest.raises(ValueError, match="Invalid transition"):
        store.transition_job(
            job_id,
            JobStatus.SUCCESS,
        )

def test_invalid_transition_does_not_change_state(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
    )

    with pytest.raises(ValueError):
        store.transition_job(
            job_id,
            JobStatus.SUCCESS,
        )

    job = store.get_job(job_id)

    assert job["status"] == JobStatus.PENDING

def test_job_lifecycle(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
    )

    store.transition_job(job_id, JobStatus.RUNNING)
    store.transition_job(job_id, JobStatus.RETRYING)
    store.transition_job(job_id, JobStatus.PENDING)
    store.transition_job(job_id, JobStatus.RUNNING)
    store.transition_job(job_id, JobStatus.SUCCESS)

    job = store.get_job(job_id)

    assert job["status"] == JobStatus.SUCCESS

def test_success_cannot_become_running(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
    )

    store.transition_job(job_id, JobStatus.RUNNING)
    store.transition_job(job_id, JobStatus.SUCCESS)

    with pytest.raises(ValueError, match="Invalid transition"):
        store.transition_job(job_id, JobStatus.RUNNING)