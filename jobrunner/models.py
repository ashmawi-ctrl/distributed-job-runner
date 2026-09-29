from dataclasses import dataclass
from enum import StrEnum


class JobStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    RETRY = "retry"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True)
class Job:
    id: str
    idempotency_key: str
    kind: str
    payload: dict
    status: JobStatus
    attempts: int
    max_attempts: int
    available_at: float
    lease_owner: str | None
    lease_expires_at: float | None
    last_error: str | None
    created_at: float
    updated_at: float
