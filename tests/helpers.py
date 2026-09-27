class FailingExecutor:
    def __init__(self, error: Exception | None = None):
        self.error = error or RuntimeError("simulated failure")

    def execute(self, job, idempotency_key):
        raise self.error