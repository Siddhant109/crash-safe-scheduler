import time

from scheduler.job_store import JobStore
from scheduler.worker import Worker


store = JobStore()

job_id = store.create_job(
    job_type="long_running_task",
    payload={"message": "important task"},
)

worker_a = Worker(
    worker_id="worker-A",
    store=store,
    lease_seconds=2,
)

print("Worker A claiming job...")

job = store.claim_job(
    worker_id="worker-A",
    lease_seconds=2,
)

print("\nWorker A claimed:")
print(job)

print("\nCurrent job:")
print(store.get_job(job_id))

print("\nSimulating worker A crash...")

time.sleep(3)

print("\nLease expired:")
print(store.is_lease_expired(job_id))

print("\nRecovering jobs...")

recovered = store.recover_expired_jobs()

print("Recovered:", recovered)

print("\nAfter recovery:")
print(store.get_job(job_id))

worker_b = Worker(
    worker_id="worker-B",
    store=store,
    lease_seconds=30,
)

print("\nWorker B claiming recovered job...")

worker_b.run_once()

print("\nFinal job:")
print(store.get_job(job_id))

result = store.complete_job(
    job_id=job_id,
    worker_id="worker-A",
    lease_generation=1,
)

print(result)