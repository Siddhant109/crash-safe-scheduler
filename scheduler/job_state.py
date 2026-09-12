from enum import StrEnum


class JobStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"

ALLOWED_TRANSITIONS = {
    JobStatus.PENDING: {
        JobStatus.RUNNING,
    },
    JobStatus.RUNNING: {
        JobStatus.SUCCESS,
        JobStatus.RETRYING,
        JobStatus.FAILED,
    },
    JobStatus.RETRYING: {
        JobStatus.PENDING,
    },
    JobStatus.SUCCESS: set(),
    JobStatus.FAILED: set(),
}

def can_transition(
    current: JobStatus,
    target: JobStatus,
) -> bool:
    return target in ALLOWED_TRANSITIONS[current]