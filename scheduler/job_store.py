import sqlite3
from pathlib import Path
import json
import uuid
from datetime import datetime, timezone
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
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def create_job(self, job_type: str, payload: dict) -> str:
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
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    job_type,
                    json.dumps(payload),
                    JobStatus.PENDING.value,
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
                    updated_at
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
                    updated_at
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

            connection.execute(
                """
                UPDATE jobs
                SET status = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    target_status.value,
                    now,
                    job_id,
                ),
            )

    def claim_job(self) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id
                FROM jobs
                WHERE status = ?
                ORDER BY created_at
                LIMIT 1
                """,
                (JobStatus.PENDING.value,),
            ).fetchone()

            if row is None:
                return None

            job_id = row["id"]
            now = datetime.now(timezone.utc).isoformat()

            cursor = connection.execute(
                """
                UPDATE jobs
                SET status = ?, updated_at = ?
                WHERE id = ?
                AND status = ?
                """,
                (
                    JobStatus.RUNNING.value,
                    now,
                    job_id,
                    JobStatus.PENDING.value,
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
                    updated_at
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
        }