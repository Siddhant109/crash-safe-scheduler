import time

from scheduler.job_store import JobStore
from scheduler.worker import Worker


store = JobStore()

job_id = store.create_job(
    job_type="hey",
    payload={"message": "Hey from scheduler"},
)

print("Created job:", job_id)

worker = Worker(
    worker_id="worker-1",
    store=store,
)

print("\nRunning worker...")
worker.run_once()

history = store.get_job_history(job_id)

for index, event in enumerate(history, start=1):
    print(
        index,
        event["event_type"],
        event["worker_id"],
        event["attempt_number"],
        event["details"],
    )