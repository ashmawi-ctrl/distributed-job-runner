import json
import sqlite3
import time
import uuid
from pathlib import Path

from .models import Job, JobStatus


class LeaseOwnershipError(RuntimeError):
    pass


class QueueStore:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=5,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    available_at REAL NOT NULL,
                    lease_owner TEXT,
                    lease_expires_at REAL,
                    last_error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_claimable
                ON jobs(status, available_at, created_at);
                """
            )

    def enqueue(
        self,
        *,
        idempotency_key: str,
        kind: str,
        payload: dict,
        max_attempts: int = 3,
        now: float | None = None,
    ) -> tuple[Job, bool]:
        if not idempotency_key.strip():
            raise ValueError("idempotency_key cannot be empty")
        if not kind.strip():
            raise ValueError("kind cannot be empty")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        timestamp = time.time() if now is None else now
        job_id = str(uuid.uuid4())

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM jobs WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                connection.execute("COMMIT")
                return _row_to_job(existing), False

            connection.execute(
                """
                INSERT INTO jobs (
                    id,
                    idempotency_key,
                    kind,
                    payload_json,
                    status,
                    attempts,
                    max_attempts,
                    available_at,
                    lease_owner,
                    lease_expires_at,
                    last_error,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, 0, ?, ?, NULL, NULL, NULL, ?, ?)
                """,
                (
                    job_id,
                    idempotency_key,
                    kind,
                    json.dumps(payload, separators=(",", ":"), sort_keys=True),
                    JobStatus.QUEUED.value,
                    max_attempts,
                    timestamp,
                    timestamp,
                    timestamp,
                ),
            )
            row = connection.execute(
                "SELECT * FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            connection.execute("COMMIT")

        assert row is not None
        return _row_to_job(row), True

    def get(self, job_id: str) -> Job | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
        return _row_to_job(row) if row is not None else None

    def claim_next(
        self,
        *,
        worker_id: str,
        lease_seconds: float,
        now: float | None = None,
    ) -> Job | None:
        if not worker_id.strip():
            raise ValueError("worker_id cannot be empty")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than 0")

        timestamp = time.time() if now is None else now

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT id
                FROM jobs
                WHERE status IN (?, ?)
                  AND available_at <= ?
                ORDER BY available_at ASC, created_at ASC
                LIMIT 1
                """,
                (
                    JobStatus.QUEUED.value,
                    JobStatus.RETRY.value,
                    timestamp,
                ),
            ).fetchone()

            if row is None:
                connection.execute("COMMIT")
                return None

            job_id = str(row["id"])
            connection.execute(
                """
                UPDATE jobs
                SET status = ?,
                    attempts = attempts + 1,
                    lease_owner = ?,
                    lease_expires_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    JobStatus.PROCESSING.value,
                    worker_id,
                    timestamp + lease_seconds,
                    timestamp,
                    job_id,
                ),
            )
            claimed = connection.execute(
                "SELECT * FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            connection.execute("COMMIT")

        assert claimed is not None
        return _row_to_job(claimed)

    def heartbeat(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_seconds: float,
        now: float | None = None,
    ) -> Job:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than 0")
        timestamp = time.time() if now is None else now

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE jobs
                SET lease_expires_at = ?,
                    updated_at = ?
                WHERE id = ?
                  AND status = ?
                  AND lease_owner = ?
                  AND lease_expires_at > ?
                """,
                (
                    timestamp + lease_seconds,
                    timestamp,
                    job_id,
                    JobStatus.PROCESSING.value,
                    worker_id,
                    timestamp,
                ),
            )
            if cursor.rowcount != 1:
                raise LeaseOwnershipError(
                    "worker does not hold an active lease for this job"
                )

        job = self.get(job_id)
        assert job is not None
        return job

    def complete(
        self,
        job_id: str,
        *,
        worker_id: str,
        now: float | None = None,
    ) -> Job:
        timestamp = time.time() if now is None else now

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE jobs
                SET status = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_error = NULL,
                    updated_at = ?
                WHERE id = ?
                  AND status = ?
                  AND lease_owner = ?
                  AND lease_expires_at > ?
                """,
                (
                    JobStatus.SUCCEEDED.value,
                    timestamp,
                    job_id,
                    JobStatus.PROCESSING.value,
                    worker_id,
                    timestamp,
                ),
            )
            if cursor.rowcount != 1:
                raise LeaseOwnershipError(
                    "worker does not hold an active lease for this job"
                )

        job = self.get(job_id)
        assert job is not None
        return job

    def fail(
        self,
        job_id: str,
        *,
        worker_id: str,
        error: str,
        base_retry_seconds: float = 2.0,
        now: float | None = None,
    ) -> Job:
        if base_retry_seconds < 0:
            raise ValueError("base_retry_seconds cannot be negative")
        timestamp = time.time() if now is None else now

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                connection.execute("ROLLBACK")
                raise KeyError(job_id)

            job = _row_to_job(row)
            if (
                job.status is not JobStatus.PROCESSING
                or job.lease_owner != worker_id
                or job.lease_expires_at is None
                or job.lease_expires_at <= timestamp
            ):
                connection.execute("ROLLBACK")
                raise LeaseOwnershipError(
                    "worker does not hold an active lease for this job"
                )

            if job.attempts >= job.max_attempts:
                next_status = JobStatus.FAILED
                available_at = timestamp
            else:
                next_status = JobStatus.RETRY
                available_at = timestamp + (
                    base_retry_seconds * (2 ** (job.attempts - 1))
                )

            connection.execute(
                """
                UPDATE jobs
                SET status = ?,
                    available_at = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    next_status.value,
                    available_at,
                    error,
                    timestamp,
                    job_id,
                ),
            )
            updated = connection.execute(
                "SELECT * FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            connection.execute("COMMIT")

        assert updated is not None
        return _row_to_job(updated)

    def recover_expired_leases(self, *, now: float | None = None) -> int:
        timestamp = time.time() if now is None else now

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE jobs
                SET status = CASE
                        WHEN attempts >= max_attempts THEN ?
                        ELSE ?
                    END,
                    available_at = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_error = CASE
                        WHEN attempts >= max_attempts
                        THEN ?
                        ELSE ?
                    END,
                    updated_at = ?
                WHERE status = ?
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at <= ?
                """,
                (
                    JobStatus.FAILED.value,
                    JobStatus.RETRY.value,
                    timestamp,
                    "worker lease expired after final attempt",
                    "worker lease expired before completion",
                    timestamp,
                    JobStatus.PROCESSING.value,
                    timestamp,
                ),
            )
            return cursor.rowcount


def _row_to_job(row: sqlite3.Row) -> Job:
    return Job(
        id=str(row["id"]),
        idempotency_key=str(row["idempotency_key"]),
        kind=str(row["kind"]),
        payload=json.loads(str(row["payload_json"])),
        status=JobStatus(str(row["status"])),
        attempts=int(row["attempts"]),
        max_attempts=int(row["max_attempts"]),
        available_at=float(row["available_at"]),
        lease_owner=(
            str(row["lease_owner"]) if row["lease_owner"] is not None else None
        ),
        lease_expires_at=(
            float(row["lease_expires_at"])
            if row["lease_expires_at"] is not None
            else None
        ),
        last_error=(
            str(row["last_error"]) if row["last_error"] is not None else None
        ),
        created_at=float(row["created_at"]),
        updated_at=float(row["updated_at"]),
    )
