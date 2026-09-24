import sqlite3
from pathlib import Path
import json
import uuid
from datetime import datetime, timezone, timedelta
from scheduler.job_state import JobStatus, can_transition

class JobStore:
    def __init__(
        self,
        db_path: str | Path | None = None,
    ):
        if db_path is None:
            db_path = (
                Path(__file__).resolve().parent.parent
                / "scheduler.db"
            )

        self.db_path = str(db_path)
        self._initialize_database()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.db_path,
            timeout=5.0,
        )

        connection.row_factory = sqlite3.Row

        connection.execute("PRAGMA journal_mode=WAL")

        return connection

    def _initialize_database(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    available_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    worker_id TEXT,
                    lease_until TEXT,
                    lease_generation INTEGER NOT NULL DEFAULT 0
                )
                """
            )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS job_executions (
                    id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    attempt_number INTEGER NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    lease_generation INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,

                    FOREIGN KEY (job_id) REFERENCES jobs(id),

                    UNIQUE(job_id, attempt_number)
                )
                """
            )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS idempotency_records (
                    idempotency_key TEXT PRIMARY KEY,
                    result TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )

    def create_job(self, job_type: str, payload: dict, max_attempts: int = 3) -> str:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        job_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO jobs (
                    id,
                    type,
                    payload,
                    status,
                    attempt_count,
                    max_attempts,
                    available_at,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    job_type,
                    json.dumps(payload),
                    JobStatus.PENDING.value,
                    0,
                    max_attempts,
                    now,
                    now,
                    now,
                ),
            )

        return job_id

    def get_job(self, job_id: str) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    id,
                    type,
                    payload,
                    status,
                    created_at,
                    updated_at,
                    attempt_count,
                    max_attempts,
                    available_at,
                    worker_id,
                    lease_until,
                    lease_generation
                FROM jobs
                WHERE id = ?
                """,
                (job_id,),
            ).fetchone()

        if row is None:
            return None

        return {
            "id": row["id"],
            "type": row["type"],
            "payload": json.loads(row["payload"]),
            "status": JobStatus(row["status"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "attempt_count": row["attempt_count"],
            "max_attempts": row["max_attempts"],
            "available_at": row["available_at"],
            "worker_id": row["worker_id"],
            "lease_until": row["lease_until"],
            "lease_generation": row["lease_generation"]
        }

    def list_jobs(self) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    id,
                    type,
                    payload,
                    status,
                    created_at,
                    updated_at,
                    attempt_count,
                    max_attempts,
                    available_at,
                    worker_id,
                    lease_until,
                    lease_generation
                FROM jobs
                ORDER BY created_at
                """
            ).fetchall()

        return [
            {
                "id": row["id"],
                "type": row["type"],
                "payload": json.loads(row["payload"]),
                "status": row["status"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "attempt_count": row["attempt_count"],
                "max_attempts": row["max_attempts"],
                "available_at": row["available_at"],
                "worker_id": row["worker_id"],
                "lease_until": row["lease_until"],
                "lease_generation": row["lease_generation"]
            }
            for row in rows
        ]

    def transition_job(
        self,
        job_id: str,
        target_status: JobStatus,
    ) -> None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT status
                FROM jobs
                WHERE id = ?
                """,
                (job_id,),
            ).fetchone()

            if row is None:
                raise ValueError(f"Job not found: {job_id}")

            current_status = JobStatus(row["status"])

            if not can_transition(current_status, target_status):
                raise ValueError(
                    f"Invalid transition: "
                    f"{current_status} -> {target_status}"
                )

            now = datetime.now(timezone.utc).isoformat()

            if current_status == JobStatus.RUNNING:
                connection.execute(
                    """
                    UPDATE jobs
                    SET
                        status = ?,
                        worker_id = NULL,
                        lease_until = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        target_status.value,
                        now,
                        job_id,
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE jobs
                    SET
                        status = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        target_status.value,
                        now,
                        job_id,
                    ),
                )

    def claim_job(self,  worker_id: str, lease_seconds: float = 30.0) -> dict | None:
        if not worker_id:
            raise ValueError("worker_id cannot be empty")

        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than 0")

        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        lease_until = (
            now + timedelta(seconds=lease_seconds)
        ).isoformat()

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id
                FROM jobs
                WHERE status = ?
                    AND available_at <= ?
                ORDER BY created_at
                LIMIT 1
                """,
                (JobStatus.PENDING.value,
                 now_iso),
            ).fetchone()

            if row is None:
                return None

            job_id = row["id"]

            cursor = connection.execute(
                """
                UPDATE jobs
                SET
                    status = ?,
                    attempt_count = attempt_count + 1,
                    worker_id = ?,
                    lease_until = ?,
                    lease_generation = lease_generation + 1,
                    updated_at = ?
                WHERE id = ?
                AND status = ?
                AND available_at <= ?
                """,
                (
                    JobStatus.RUNNING.value,
                    worker_id,
                    lease_until,
                    now_iso,
                    job_id,
                    JobStatus.PENDING.value,
                    now_iso
                ),
            )

            if cursor.rowcount != 1:
                return None

            claimed_row = connection.execute(
                """
                SELECT
                    id,
                    type,
                    payload,
                    status,
                    created_at,
                    updated_at,
                    attempt_count,
                    max_attempts,
                    available_at,
                    worker_id,
                    lease_until,
                    lease_generation
                FROM jobs
                WHERE id = ?
                """,
                (job_id,),
            ).fetchone()

        return {
            "id": claimed_row["id"],
            "type": claimed_row["type"],
            "payload": json.loads(claimed_row["payload"]),
            "status": JobStatus(claimed_row["status"]),
            "created_at": claimed_row["created_at"],
            "updated_at": claimed_row["updated_at"],
            "attempt_count": claimed_row["attempt_count"],
            "max_attempts": claimed_row["max_attempts"],
            "available_at": claimed_row["available_at"],
            "worker_id": claimed_row["worker_id"],
            "lease_until": claimed_row["lease_until"],
            "lease_generation": claimed_row["lease_generation"]
        }

    def retry_job(
        self,
        job_id: str,
        delay_seconds: float,
        worker_id: str,
        lease_generation: int,
    ) -> bool:
        if delay_seconds < 0:
            raise ValueError("delay_seconds cannot be negative")

        if not worker_id:
            raise ValueError("worker_id cannot be empty")

        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        available_at = (
            now + timedelta(seconds=delay_seconds)
        ).isoformat()

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    attempt_count,
                    max_attempts
                FROM jobs
                WHERE id = ?
                AND status = ?
                AND worker_id = ?
                AND lease_generation = ?
                AND lease_until IS NOT NULL
                AND lease_until > ?
                """,
                (
                    job_id,
                    JobStatus.RUNNING.value,
                    worker_id,
                    lease_generation,
                    now_iso,
                ),
            ).fetchone()

            if row is None:
                return False

            if row["attempt_count"] >= row["max_attempts"]:
                cursor = connection.execute(
                    """
                    UPDATE jobs
                    SET
                        status = ?,
                        worker_id = NULL,
                        lease_until = NULL,
                        updated_at = ?
                    WHERE id = ?
                    AND status = ?
                    AND worker_id = ?
                    AND lease_generation = ?
                    AND lease_until IS NOT NULL
                    AND lease_until > ?
                    """,
                    (
                        JobStatus.FAILED.value,
                        now_iso,
                        job_id,
                        JobStatus.RUNNING.value,
                        worker_id,
                        lease_generation,
                        now_iso,
                    ),
                )

                return cursor.rowcount == 1

            cursor = connection.execute(
                """
                UPDATE jobs
                SET
                    status = ?,
                    available_at = ?,
                    worker_id = NULL,
                    lease_until = NULL,
                    updated_at = ?
                WHERE id = ?
                AND status = ?
                AND worker_id = ?
                AND lease_generation = ?
                AND lease_until IS NOT NULL
                AND lease_until > ?
                """,
                (
                    JobStatus.RETRYING.value,
                    available_at,
                    now_iso,
                    job_id,
                    JobStatus.RUNNING.value,
                    worker_id,
                    lease_generation,
                    now_iso,
                ),
            )

            return cursor.rowcount == 1

    def promote_due_retries(self) -> int:
        now = datetime.now(timezone.utc).isoformat()

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE jobs
                SET
                    status = ?,
                    updated_at = ?
                WHERE status = ?
                AND available_at <= ?
                """,
                (
                    JobStatus.PENDING.value,
                    now,
                    JobStatus.RETRYING.value,
                    now,
                ),
            )

            return cursor.rowcount

    def heartbeat(
        self,
        job_id: str,
        worker_id: str,
        lease_generation: int,
        lease_seconds: float = 30.0,
    ) -> bool:
        if not worker_id:
            raise ValueError("worker_id cannot be empty")

        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than 0")

        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        new_lease_until = (
            now + timedelta(seconds=lease_seconds)
        ).isoformat()

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE jobs
                SET
                    lease_until = ?,
                    updated_at = ?
                WHERE id = ?
                AND status = ?
                AND worker_id = ?
                AND lease_generation = ?
                AND lease_until > ?
                """,
                (
                    new_lease_until,
                    now_iso,
                    job_id,
                    JobStatus.RUNNING.value,
                    worker_id,
                    lease_generation,
                    now_iso,
                ),
            )

            return cursor.rowcount == 1

    def is_lease_expired(self, job_id: str) -> bool:
        job = self.get_job(job_id)

        if job is None:
            raise ValueError(f"Job not found: {job_id}")

        if job["status"] != JobStatus.RUNNING:
            return False

        if job["lease_until"] is None:
            return True

        lease_until = datetime.fromisoformat(job["lease_until"])
        now = datetime.now(timezone.utc)

        return lease_until <= now

    def recover_expired_jobs(self) -> int:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE jobs
                SET
                    status = ?,
                    worker_id = NULL,
                    lease_until = NULL,
                    updated_at = ?
                WHERE status = ?
                AND lease_until IS NOT NULL
                AND lease_until <= ?
                """,
                (
                    JobStatus.PENDING.value,
                    now_iso,
                    JobStatus.RUNNING.value,
                    now_iso,
                ),
            )

            return cursor.rowcount

    def complete_job(
        self,
        job_id: str,
        worker_id: str,
        lease_generation: int,
    ) -> bool:
            now = datetime.now(timezone.utc)
            now_iso = now.isoformat()

            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    UPDATE jobs
                    SET
                        status = ?,
                        worker_id = NULL,
                        lease_until = NULL,
                        updated_at = ?
                    WHERE id = ?
                    AND status = ?
                    AND worker_id = ?
                    AND lease_generation = ?
                    AND lease_until IS NOT NULL
                    AND lease_until > ?
                    """,
                    (
                        JobStatus.SUCCESS.value,
                        now_iso,
                        job_id,
                        JobStatus.RUNNING.value,
                        worker_id,
                        lease_generation,
                        now_iso,
                    ),
                )

                return cursor.rowcount == 1

    def create_execution(
        self,
        job: dict,
        worker_id: str,
    ) -> dict:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()

        execution_id = str(uuid.uuid4())

        attempt_number = job["attempt_count"]
        idempotency_key = job["id"]

        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO job_executions (
                    id,
                    job_id,
                    attempt_number,
                    idempotency_key,
                    worker_id,
                    lease_generation,
                    status,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    execution_id,
                    job["id"],
                    attempt_number,
                    idempotency_key,
                    worker_id,
                    job["lease_generation"],
                    "STARTED",
                    now_iso,
                    now_iso,
                ),
            )

        return {
            "id": execution_id,
            "job_id": job["id"],
            "attempt_number": attempt_number,
            "idempotency_key": idempotency_key,
            "worker_id": worker_id,
            "lease_generation": job["lease_generation"],
            "status": "STARTED",
            "created_at": now_iso,
            "updated_at": now_iso,
        }

    def update_execution(
        self,
        execution_id: str,
        status: str,
    ) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE job_executions
                SET
                    status = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    now_iso,
                    execution_id,
                ),
            )

            if cursor.rowcount != 1:
                raise ValueError(
                    f"Execution not found: {execution_id}"
                )

    def get_execution(
        self,
        execution_id: str,
    ) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    id,
                    job_id,
                    attempt_number,
                    idempotency_key,
                    worker_id,
                    lease_generation,
                    status,
                    created_at,
                    updated_at
                FROM job_executions
                WHERE id = ?
                """,
                (execution_id,),
            ).fetchone()

        if row is None:
            return None

        return dict(row)

    def list_executions(
        self,
        job_id: str,
    ) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    id,
                    job_id,
                    attempt_number,
                    idempotency_key,
                    worker_id,
                    lease_generation,
                    status,
                    created_at,
                    updated_at
                FROM job_executions
                WHERE job_id = ?
                ORDER BY attempt_number
                """,
                (job_id,),
            ).fetchall()

        return [dict(row) for row in rows]

    def execute_idempotent_side_effect(
        self,
        idempotency_key: str,
        job: dict,
    ) -> dict:
        now_iso = datetime.now(timezone.utc).isoformat()

        with self._connect() as connection:
            existing = connection.execute(
                """
                SELECT
                    result
                FROM idempotency_records
                WHERE idempotency_key = ?
                """,
                (idempotency_key,),
            ).fetchone()

            if existing is not None:
                return {
                    "result": json.loads(existing["result"]),
                    "duplicate": True,
                }

            result = {
                "job_id": job["id"],
                "job_type": job["type"],
                "message": "side effect executed",
            }

            connection.execute(
                """
                INSERT INTO idempotency_records (
                    idempotency_key,
                    result,
                    created_at
                )
                VALUES (?, ?, ?)
                """,
                (
                    idempotency_key,
                    json.dumps(result),
                    now_iso,
                ),
            )

            return {
                "result": result,
                "duplicate": False,
            }

    def count_idempotency_records(
        self,
        idempotency_key: str,
    ) -> int:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM idempotency_records
                WHERE idempotency_key = ?
                """,
                (idempotency_key,),
            ).fetchone()

        return row["count"]