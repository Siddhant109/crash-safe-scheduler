import time

from scheduler.job_store import JobStore


class Worker:
    def __init__(
        self,
        worker_id: str,
        store: JobStore,
    ):
        self.worker_id = worker_id
        self.store = store

    def run_once(self) -> bool:
        job = self.store.claim_job()

        if job is None:
            return False

        print(
            f"[{self.worker_id}] "
            f"Executing job {job['id']}"
        )

        self.execute(job)

        return True

    def execute(self, job: dict) -> None:
        print(
            f"[{self.worker_id}] "
            f"Job {job['id']} completed"
        )