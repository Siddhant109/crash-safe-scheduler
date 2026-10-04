from scheduler.job_store import JobStore
from scheduler.worker import Worker
from tests.helpers import FailingExecutor


store = JobStore()

job_id = store.create_job(
    job_type="payment",
    payload={"amount": 500},
    max_attempts=3,
)

worker = Worker(
    worker_id="worker-1",
    store=store,
    executor=FailingExecutor(),
)

print("Initial:")
print(store.get_job(job_id))

print("\nRunning failing worker...")

worker.run_once()

print("\nAfter failure:")
print(store.get_job(job_id))

print("\nHistory:")
for event in store.get_job_history(job_id):
    print(event)

# After the backoff period
store.promote_due_retries()
print(store.get_job(job_id))