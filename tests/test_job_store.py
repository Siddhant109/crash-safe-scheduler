from scheduler.job_store import JobStore


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