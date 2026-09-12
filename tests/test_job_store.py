from scheduler.job_store import JobStore
from scheduler.job_state import JobStatus
import threading

def test_create_and_get_job(tmp_path):
    db_path = tmp_path / "scheduler.db"

    store = JobStore(db_path)

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
    )

    job = store.get_job(job_id)

    assert job is not None
    assert job["id"] == job_id
    assert job["type"] == "send_email"
    assert job["payload"] == {"user_id": 42}
    assert job["status"] == "PENDING"

def test_job_survives_new_store_instance(tmp_path):
    db_path = tmp_path / "scheduler.db"

    first_store = JobStore(db_path)

    job_id = first_store.create_job(
        "generate_invoice",
        {"order_id": 123},
    )

    second_store = JobStore(db_path)

    job = second_store.get_job(job_id)

    assert job is not None
    assert job["id"] == job_id
    assert job["type"] == "generate_invoice"
    assert job["payload"] == {"order_id": 123}

def test_get_nonexistent_job_returns_none(tmp_path):
    db_path = tmp_path / "scheduler.db"

    store = JobStore(db_path)

    job = store.get_job("does-not-exist")

    assert job is None

def test_list_jobs(tmp_path):
    db_path = tmp_path / "scheduler.db"

    store = JobStore(db_path)

    first_id = store.create_job(
        "send_email",
        {"user_id": 1},
    )

    second_id = store.create_job(
        "generate_invoice",
        {"order_id": 2},
    )

    jobs = store.list_jobs()

    assert len(jobs) == 2
    assert jobs[0]["id"] == first_id
    assert jobs[1]["id"] == second_id

def test_nested_json_payload(tmp_path):
    db_path = tmp_path / "scheduler.db"

    store = JobStore(db_path)

    payload = {
        "user": {
            "id": 42,
            "preferences": {
                "email": True,
                "sms": False,
            },
        },
        "items": [1, 2, 3],
    }

    job_id = store.create_job("notification", payload)

    job = store.get_job(job_id)

    assert job["payload"] == payload

def test_claim_job_moves_pending_to_running(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
    )

    job = store.claim_job()

    assert job is not None
    assert job["id"] == job_id
    assert job["status"] == JobStatus.RUNNING

    stored_job = store.get_job(job_id)

    assert stored_job["status"] == JobStatus.RUNNING

def test_claim_job_only_claims_pending_jobs(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
    )

    store.transition_job(
        job_id,
        JobStatus.RUNNING,
    )

    store.transition_job(
        job_id,
        JobStatus.SUCCESS,
    )

    claimed_job = store.claim_job()

    assert claimed_job is None

def test_claim_jobs_in_creation_order(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    first_id = store.create_job(
        "job_a",
        {},
    )

    second_id = store.create_job(
        "job_b",
        {},
    )

    first = store.claim_job()
    second = store.claim_job()

    assert first["id"] == first_id
    assert second["id"] == second_id

def test_only_one_worker_can_claim_job(tmp_path):
    store = JobStore(tmp_path / "scheduler.db")

    job_id = store.create_job(
        "send_email",
        {"user_id": 42},
    )

    results = []

    def claim():
        worker_store = JobStore(tmp_path / "scheduler.db")
        result = worker_store.claim_job()
        results.append(result)

    thread_a = threading.Thread(target=claim)
    thread_b = threading.Thread(target=claim)

    thread_a.start()
    thread_b.start()

    thread_a.join()
    thread_b.join()

    successful_claims = [
        result
        for result in results
        if result is not None
    ]

    assert len(successful_claims) == 1
    assert successful_claims[0]["id"] == job_id