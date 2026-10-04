import time

from scheduler.job_store import JobStore
from scheduler.worker import Worker


store = JobStore()

job1 = store.create_job(
    job_type="task",
    payload={"id": 1},
)

job2 = store.create_job(
    job_type="task",
    payload={"id": 2},
)

job3 = store.create_job(
    job_type="task",
    payload={"id": 3},
)

worker1 = Worker(
    worker_id="worker-1",
    store=store,
    max_concurrent_jobs=2,
)

worker2 = Worker(
    worker_id="worker-2",
    store=store,
    max_concurrent_jobs=2,
)

worker3 = Worker(
    worker_id="worker-3",
    store=store,
    max_concurrent_jobs=2,
)