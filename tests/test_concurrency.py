import threading

from scheduler.job_store import JobStore
from scheduler.job_state import JobStatus
from scheduler.job_priority import JobPriority

def test_concurrency_limit_blocks_additional_claim(
    tmp_path,
):
    store = JobStore(tmp_path / "scheduler.db")

    job1 = store.create_job(
        "job",
        {"number": 1},
    )

    job2 = store.create_job(
        "job",
        {"number": 2},
    )

    job3 = store.create_job(
        "job",
        {"number": 3},
    )

    first = store.claim_job(
        worker_id="worker-1",
        max_concurrent_jobs=2,
    )

    second = store.claim_job(
        worker_id="worker-2",
        max_concurrent_jobs=2,
    )

    third = store.claim_job(
        worker_id="worker-3",
        max_concurrent_jobs=2,
    )

    assert first is not None
    assert second is not None
    assert third is None

    assert first["id"] == job1
    assert second["id"] == job2

    remaining = store.get_job(job3)

    assert remaining["status"] == JobStatus.PENDING

def test_completion_frees_concurrency_slot(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job1 = store.create_job("job", {})
    job2 = store.create_job("job", {})
    job3 = store.create_job("job", {})

    first = store.claim_job(
        worker_id="worker-1",
        max_concurrent_jobs=2,
    )

    second = store.claim_job(
        worker_id="worker-2",
        max_concurrent_jobs=2,
    )

    assert first is not None
    assert second is not None

    blocked = store.claim_job(
        worker_id="worker-3",
        max_concurrent_jobs=2,
    )

    assert blocked is None

    completed = store.complete_job(
        job_id=first["id"],
        worker_id="worker-1",
        lease_generation=first["lease_generation"],
    )

    assert completed is True

    third = store.claim_job(
        worker_id="worker-3",
        max_concurrent_jobs=2,
    )

    assert third is not None
    assert third["id"] == job3

def test_concurrency_limit_is_atomic_under_race(
    tmp_path,
):
    db_path = tmp_path / "scheduler.db"

    setup_store = JobStore(db_path)

    job_ids = [
        setup_store.create_job(
            "job",
            {"number": i},
        )
        for i in range(5)
    ]

    results = []
    lock = threading.Lock()

    def claim(worker_id):
        store = JobStore(db_path)

        result = store.claim_job(
            worker_id=worker_id,
            max_concurrent_jobs=1,
        )

        with lock:
            results.append(result)

    threads = [
        threading.Thread(
            target=claim,
            args=(f"worker-{i}",),
        )
        for i in range(5)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    successful_claims = [
        result
        for result in results
        if result is not None
    ]

    assert len(successful_claims) == 1

def test_no_concurrency_limit_preserves_existing_behavior(
    tmp_path,
):
    store = JobStore(tmp_path / "scheduler.db")

    for _ in range(5):
        store.create_job("job", {})

    claimed = []

    for i in range(5):
        job = store.claim_job(
            worker_id=f"worker-{i}",
        )

        if job is not None:
            claimed.append(job)

    assert len(claimed) == 5

def test_priority_is_respected_with_concurrency_limit(
    tmp_path,
):
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

    high_id = store.create_job(
        "high",
        {},
        priority=JobPriority.HIGH,
    )

    first = store.claim_job(
        worker_id="worker-1",
        max_concurrent_jobs=2,
    )

    second = store.claim_job(
        worker_id="worker-2",
        max_concurrent_jobs=2,
    )

    assert first["id"] == critical_id
    assert second["id"] == high_id

    third = store.claim_job(
        worker_id="worker-3",
        max_concurrent_jobs=2,
    )

    assert third is None

    normal = store.get_job(normal_id)

    assert normal["status"] == JobStatus.PENDING