import threading

from scheduler.job_store import JobStore
from scheduler.worker import Worker


def test_many_workers_cannot_claim_same_job(tmp_path):
    db_path = tmp_path / "scheduler.db"

    setup_store = JobStore(db_path)

    job_id = setup_store.create_job(
        "email",
        {"to": "test@example.com"},
    )

    results = []
    lock = threading.Lock()

    def claim(worker_id):
        store = JobStore(db_path)

        result = store.claim_job(
            worker_id=worker_id,
            lease_seconds=30,
        )

        with lock:
            results.append(result)

    threads = [
        threading.Thread(
            target=claim,
            args=(f"worker-{i}",),
        )
        for i in range(20)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    successful = [
        result
        for result in results
        if result is not None
    ]

    assert len(successful) == 1
    assert successful[0]["id"] == job_id

def test_many_workers_claim_distinct_jobs(tmp_path):
    db_path = tmp_path / "scheduler.db"

    setup_store = JobStore(db_path)

    job_ids = {
        setup_store.create_job(
            "job",
            {"number": i},
        )
        for i in range(20)
    }

    results = []
    lock = threading.Lock()

    def claim(worker_id):
        store = JobStore(db_path)

        result = store.claim_job(
            worker_id=worker_id,
        )

        with lock:
            results.append(result)

    threads = [
        threading.Thread(
            target=claim,
            args=(f"worker-{i}",),
        )
        for i in range(20)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    successful = [
        result
        for result in results
        if result is not None
    ]

    claimed_ids = {
        result["id"]
        for result in successful
    }

    assert len(successful) == 20
    assert len(claimed_ids) == 20
    assert claimed_ids == job_ids

def test_concurrency_limit_holds_under_heavy_race(tmp_path):
    db_path = tmp_path / "scheduler.db"

    setup_store = JobStore(db_path)

    for i in range(20):
        setup_store.create_job(
            "job",
            {"number": i},
        )

    results = []
    lock = threading.Lock()

    def claim(worker_id):
        store = JobStore(db_path)

        result = store.claim_job(
            worker_id=worker_id,
            max_concurrent_jobs=3,
        )

        with lock:
            results.append(result)

    threads = [
        threading.Thread(
            target=claim,
            args=(f"worker-{i}",),
        )
        for i in range(20)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    successful = [
        result
        for result in results
        if result is not None
    ]

    assert len(successful) == 3

import time

from scheduler.job_state import JobStatus


def test_stale_worker_cannot_complete_after_recovery(tmp_path):
    db_path = tmp_path / "scheduler.db"

    store = JobStore(db_path)

    job_id = store.create_job(
        "email",
        {},
    )

    first = store.claim_job(
        worker_id="worker-1",
        lease_seconds=0.5,
    )

    generation_1 = first["lease_generation"]

    time.sleep(0.6)

    recovered = store.recover_expired_jobs()

    assert recovered == 1

    second = store.claim_job(
        worker_id="worker-2",
        lease_seconds=30,
    )

    assert second["lease_generation"] == generation_1 + 1

    stale_completion = store.complete_job(
        job_id=job_id,
        worker_id="worker-1",
        lease_generation=generation_1,
    )

    assert stale_completion is False

    job = store.get_job(job_id)

    assert job["status"] == JobStatus.RUNNING
    assert job["worker_id"] == "worker-2"

def test_stale_worker_cannot_heartbeat_new_generation(tmp_path):
    db_path = tmp_path / "scheduler.db"

    store = JobStore(db_path)

    job_id = store.create_job(
        "email",
        {},
    )

    first = store.claim_job(
        worker_id="worker-1",
        lease_seconds=0.5,
    )

    time.sleep(0.6)

    store.recover_expired_jobs()

    second = store.claim_job(
        worker_id="worker-2",
        lease_seconds=30,
    )

    result = store.heartbeat(
        job_id=job_id,
        worker_id="worker-1",
        lease_generation=first["lease_generation"],
        lease_seconds=30,
    )

    assert result is False

    current = store.get_job(job_id)

    assert current["worker_id"] == "worker-2"
    assert current["lease_generation"] == second["lease_generation"]

def test_attempt_count_never_exceeds_max_attempts(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "job",
        {},
        max_attempts=2,
    )

    first = store.claim_job(
        worker_id="worker-1",
    )

    assert first["attempt_count"] == 1

    retried = store.retry_job(
        job_id=job_id,
        delay_seconds=0,
        worker_id="worker-1",
        lease_generation=first["lease_generation"],
    )

    assert retried is True

    store.promote_due_retries()

    second = store.claim_job(
        worker_id="worker-2",
    )

    assert second["attempt_count"] == 2

    failed = store.retry_job(
        job_id=job_id,
        delay_seconds=0,
        worker_id="worker-2",
        lease_generation=second["lease_generation"],
    )

    assert failed is True

    final = store.get_job(job_id)

    assert final["status"] == JobStatus.FAILED
    assert final["attempt_count"] == 2

from scheduler.execution import IdempotentExecutor


def test_idempotency_prevents_duplicate_side_effects(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "payment",
        {"amount": 100},
    )

    job = store.get_job(job_id)

    executor = IdempotentExecutor(store)

    results = [
        executor.execute(
            job,
            idempotency_key=job_id,
        )
        for _ in range(10)
    ]

    assert results[0]["duplicate"] is False

    for result in results[1:]:
        assert result["duplicate"] is True

    assert (
        store.count_idempotency_records(job_id)
        == 1
    )

def test_idempotency_is_atomic_under_race(tmp_path):
    db_path = tmp_path / "scheduler.db"

    setup_store = JobStore(db_path)

    job_id = setup_store.create_job(
        "payment",
        {"amount": 100},
    )

    job = setup_store.get_job(job_id)

    results = []
    lock = threading.Lock()

    def execute():
        store = JobStore(db_path)
        executor = IdempotentExecutor(store)

        result = executor.execute(
            job,
            idempotency_key=job_id,
        )

        with lock:
            results.append(result)

    threads = [
        threading.Thread(target=execute)
        for _ in range(10)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert (
        setup_store.count_idempotency_records(job_id)
        == 1
    )

    successful = [
        result
        for result in results
        if result["duplicate"] is False
    ]

    assert len(successful) == 1

def test_recovery_is_idempotent(tmp_path):
    db_path = tmp_path / "scheduler.db"

    store = JobStore(db_path)

    job_id = store.create_job(
        "job",
        {},
    )

    store.claim_job(
        worker_id="worker-1",
        lease_seconds=0.5,
    )

    time.sleep(0.6)

    first = store.recover_expired_jobs()
    second = store.recover_expired_jobs()

    assert first == 1
    assert second == 0

    history = store.get_job_history(job_id)

    recovered_events = [
        event
        for event in history
        if event["event_type"].value == "RECOVERED"
    ]

    assert len(recovered_events) == 1

from scheduler.job_history import JobEventType


def test_successful_job_has_consistent_history(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "email",
        {},
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

import pytest


def test_invalid_lease_is_rejected(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    store.create_job("job", {})

    with pytest.raises(ValueError):
        store.claim_job(
            worker_id="worker-1",
            lease_seconds=0,
        )


def test_empty_worker_id_is_rejected(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    store.create_job("job", {})

    with pytest.raises(ValueError):
        store.claim_job(
            worker_id="",
        )


def test_invalid_concurrency_limit_is_rejected(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    store.create_job("job", {})

    with pytest.raises(ValueError):
        store.claim_job(
            worker_id="worker-1",
            max_concurrent_jobs=0,
        )