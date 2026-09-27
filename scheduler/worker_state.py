from enum import StrEnum


class WorkerState(StrEnum):
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"