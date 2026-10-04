from scheduler.job_store import JobStore
from scheduler.worker import Worker
from scheduler.job_priority import JobPriority


store = JobStore()

low_job = store.create_job(
    job_type="low_job",
    payload={"message": "low"},
    priority=JobPriority.LOW,
)

critical_job = store.create_job(
    job_type="critical_job",
    payload={"message": "critical"},
    priority=JobPriority.CRITICAL,
)

normal_job = store.create_job(
    job_type="normal_job",
    payload={"message": "normal"},
    priority=JobPriority.NORMAL,
)

worker = Worker(
    worker_id="worker-1",
    store=store,
)

print("Running worker 1...")
worker.run_once()

print("\nRunning worker 2...")
worker.run_once()

print("\nRunning worker 3...")
worker.run_once()

print("\nFinal jobs:")

for job in store.list_jobs():
    print(
        job["id"],
        job["type"],
        job["priority"],
        job["status"],
    )