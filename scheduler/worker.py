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
            delay = calculate_backoff(
                job["attempt_count"]
            )

            retried = self.store.retry_job(
                job_id=job["id"],
                delay_seconds=delay,
                worker_id=self.worker_id,
                lease_generation=job["lease_generation"],
            )

            if not retried:
                print(
                    f"[{self.worker_id}] "
                    f"Could not retry job {job['id']}; "
                    f"lease is no longer valid"
                )

        else:
            completed = self.store.complete_job(
                job_id=job["id"],
                worker_id=self.worker_id,
                lease_generation=job["lease_generation"],
            )

            if not completed:
                print(
                    f"[{self.worker_id}] "
                    f"Could not complete job {job['id']}; "
                    f"lease is no longer valid"
                )

        return True

    def execute(self, job: dict) -> None:
        print(
            f"[{self.worker_id}] "
            f"Job {job['id']} completed"
        )