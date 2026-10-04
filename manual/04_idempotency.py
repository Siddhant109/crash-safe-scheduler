from scheduler.job_store import JobStore


store = JobStore()

job_id = store.create_job(
    job_type="payment",
    payload={
        "amount": 500,
        "customer": "customer-123",
    },
)

job = store.get_job(job_id)

key = job["id"]

print("First execution:")

result1 = store.execute_idempotent_side_effect(
    idempotency_key=key,
    job=job,
)

print(result1)

print("\nSecond execution:")

result2 = store.execute_idempotent_side_effect(
    idempotency_key=key,
    job=job,
)

print(result2)

print("\nIdempotency record count:")

print(
    store.count_idempotency_records(key)
)