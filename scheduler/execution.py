from enum import StrEnum


class ExecutionStatus(StrEnum):
    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

class IdempotentExecutor:
    def __init__(self, store):
        self.store = store

    def execute(
        self,
        job: dict,
        idempotency_key: str,
    ) -> dict:
        return self.store.execute_idempotent_side_effect(
            idempotency_key=idempotency_key,
            job=job,
        )