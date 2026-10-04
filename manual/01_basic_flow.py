from scheduler.job_store import JobStore
from scheduler.worker import Worker

store = JobStore()

job_id = store.create_job(
    job_type="hello",
    payload={"message": "Hello from the scheduler"},
)

print("Created job:", job_id)

print("\nBefore execution:")
print(store.get_job(job_id))

worker = Worker(
    worker_id="worker-1",
    store=store,
)

print("\nRunning worker...")
worker.run_once()

print("\nAfter execution:")
print(store.get_job(job_id))

print("\nJob history:")
for event in store.get_job_history(job_id):
    print(event)