import time

from scheduler.job_store import JobStore
from scheduler.worker import Worker


store = JobStore()

job = store.claim_job(
    worker_id="worker-A",
    lease_seconds=5,
)

print("Claimed:", job)

time.sleep(2)

heartbeat_result = store.heartbeat(
    job_id=job["id"],
    worker_id="worker-A",
    lease_generation=job["lease_generation"],
    lease_seconds=5,
)

print("Heartbeat:", heartbeat_result)

print(store.get_job(job["id"]))

result = store.complete_job(
    job_id=job["id"],
    worker_id="worker-A",
    lease_generation=1,
)

print(result)