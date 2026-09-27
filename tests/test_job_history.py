from scheduler.job_history import JobEventType
from scheduler.job_store import JobStore


def test_job_creation_creates_history_event(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    history = store.get_job_history(job_id)

    assert len(history) == 1
    assert history[0]["event_type"] == JobEventType.CREATED
    assert history[0]["job_id"] == job_id

def test_claim_creates_history_event(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    job = store.claim_job(
        worker_id="worker-1",
    )

    history = store.get_job_history(job_id)

    assert len(history) == 2

    assert history[0]["event_type"] == JobEventType.CREATED
    assert history[1]["event_type"] == JobEventType.CLAIMED

    assert history[1]["worker_id"] == "worker-1"
    assert history[1]["attempt_number"] == 1

from tests.helpers import FailingExecutor
from scheduler.worker import Worker


def test_execution_failure_is_recorded(tmp_path):
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

    history = store.get_job_history(job_id)

    event_types = [
        event["event_type"]
        for event in history
    ]

    assert JobEventType.CREATED in event_types
    assert JobEventType.CLAIMED in event_types
    assert JobEventType.EXECUTION_STARTED in event_types
    assert JobEventType.EXECUTION_FAILED in event_types
    assert JobEventType.RETRY_SCHEDULED in event_types

def test_successful_execution_is_recorded(tmp_path):
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

    history = store.get_job_history(job_id)

    event_types = [
        event["event_type"]
        for event in history
    ]

    assert event_types == [
        JobEventType.CREATED,
        JobEventType.CLAIMED,
        JobEventType.EXECUTION_STARTED,
        JobEventType.EXECUTION_SUCCEEDED,
        JobEventType.COMPLETED,
    ]

import time


def test_recovery_creates_history_event(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    claimed = store.claim_job(
        worker_id="worker-1",
        lease_seconds=0.5,
    )

    assert claimed is not None

    time.sleep(0.6)

    recovered = store.recover_expired_jobs()

    assert recovered == 1

    history = store.get_job_history(job_id)

    event_types = [
        event["event_type"]
        for event in history
    ]

    assert JobEventType.RECOVERED in event_types

    recovery_event = [
        event
        for event in history
        if event["event_type"] == JobEventType.RECOVERED
    ][0]

    assert recovery_event["worker_id"] == "worker-1"
    assert recovery_event["details"]["reason"] == "lease_expired"

def test_history_is_persistent(tmp_path):
    db_path = tmp_path / "scheduler.db"

    store = JobStore(db_path)

    job_id = store.create_job(
        "email",
        {},
    )

    store.claim_job(worker_id="worker-1")

    new_store = JobStore(db_path)

    history = new_store.get_job_history(job_id)

    assert len(history) == 2
    assert history[0]["event_type"] == JobEventType.CREATED
    assert history[1]["event_type"] == JobEventType.CLAIMED