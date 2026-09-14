import time

from scheduler.job_store import JobStore
from scheduler.job_state import JobStatus
from scheduler.retry import calculate_backoff


class Worker:
    def __init__(
        self,
        worker_id: str,
        store: JobStore,
        lease_seconds: float = 30.0
    ):
        self.worker_id = worker_id
        self.store = store
        self.lease_seconds = lease_seconds

    def run_once(self) -> bool:
        job = self.store.claim_job(
            worker_id=self.worker_id,
            lease_seconds=self.lease_seconds,
        )

        if job is None:
            return False

        print(
            f"[{self.worker_id}] "
            f"Executing job {job['id']}"
        )

        try:
            self.execute(job)

        except Exception as exc:
            print(
                f"[{self.worker_id}] "
                f"Job {job['id']} failed: {exc}"
            )

            delay = calculate_backoff(
                job["attempt_count"]
            )

            self.store.retry_job(
                job["id"],
                delay,
            )

        else:
            self.store.transition_job(
                job["id"],
                JobStatus.SUCCESS,
            )

        return True

    def execute(self, job: dict) -> None:
        print(
            f"[{self.worker_id}] "
            f"Job {job['id']} completed"
        )