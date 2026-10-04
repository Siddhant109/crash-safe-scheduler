import time

from scheduler.job_store import JobStore
from scheduler.worker import Worker


store = JobStore()

worker = Worker(
    worker_id="worker-1",
    store=store,
)

print(worker.state)

worker.shutdown()

print(worker.state)

result = worker.run_once()

print("run_once result:", result)

print(worker.state)