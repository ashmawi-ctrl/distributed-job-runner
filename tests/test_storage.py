from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from jobrunner.models import JobStatus
from jobrunner.storage import LeaseOwnershipError, QueueStore


def test_duplicate_idempotency_key_returns_existing_job(
    tmp_path: Path,
) -> None:
    store = QueueStore(tmp_path / "jobs.db")

    first, created_first = store.enqueue(
        idempotency_key="order-1001",
        kind="echo",
        payload={"message": "hello"},
        now=100.0,
    )
    second, created_second = store.enqueue(
        idempotency_key="order-1001",
        kind="echo",
        payload={"message": "different"},
        now=200.0,
    )

    assert created_first is True
    assert created_second is False
    assert second.id == first.id
    assert second.payload == {"message": "hello"}


def test_two_workers_cannot_claim_same_job(tmp_path: Path) -> None:
    database = tmp_path / "jobs.db"
    QueueStore(database).enqueue(
        idempotency_key="job-1",
        kind="echo",
        payload={"message": "hello"},
        now=100.0,
    )

    def claim(worker_id: str):
        return QueueStore(database).claim_next(
            worker_id=worker_id,
            lease_seconds=30,
            now=100.0,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(claim, ["worker-a", "worker-b"]))

    claimed = [job for job in results if job is not None]
    assert len(claimed) == 1
    assert claimed[0].status is JobStatus.PROCESSING
    assert claimed[0].attempts == 1


def test_expired_lease_is_recovered_and_stale_owner_cannot_complete(
    tmp_path: Path,
) -> None:
    store = QueueStore(tmp_path / "jobs.db")
    job, _ = store.enqueue(
        idempotency_key="job-1",
        kind="echo",
        payload={"message": "hello"},
        now=100.0,
    )
    claimed = store.claim_next(
        worker_id="worker-a",
        lease_seconds=10,
        now=100.0,
    )
    assert claimed is not None

    assert store.recover_expired_leases(now=111.0) == 1

    reclaimed = store.claim_next(
        worker_id="worker-b",
        lease_seconds=10,
        now=111.0,
    )
    assert reclaimed is not None
    assert reclaimed.id == job.id
    assert reclaimed.lease_owner == "worker-b"

    with pytest.raises(LeaseOwnershipError):
        store.complete(
            job.id,
            worker_id="worker-a",
            now=112.0,
        )


def test_failure_uses_exponential_backoff_then_terminal_failure(
    tmp_path: Path,
) -> None:
    store = QueueStore(tmp_path / "jobs.db")
    job, _ = store.enqueue(
        idempotency_key="job-1",
        kind="echo",
        payload={"message": "hello"},
        max_attempts=2,
        now=100.0,
    )

    first = store.claim_next(
        worker_id="worker-a",
        lease_seconds=10,
        now=100.0,
    )
    assert first is not None

    retry = store.fail(
        job.id,
        worker_id="worker-a",
        error="temporary",
        base_retry_seconds=5,
        now=101.0,
    )
    assert retry.status is JobStatus.RETRY
    assert retry.available_at == 106.0

    assert (
        store.claim_next(
            worker_id="worker-b",
            lease_seconds=10,
            now=105.0,
        )
        is None
    )

    second = store.claim_next(
        worker_id="worker-b",
        lease_seconds=10,
        now=106.0,
    )
    assert second is not None

    failed = store.fail(
        job.id,
        worker_id="worker-b",
        error="still broken",
        base_retry_seconds=5,
        now=107.0,
    )
    assert failed.status is JobStatus.FAILED
    assert failed.last_error == "still broken"


def test_expired_final_attempt_does_not_get_requeued_forever(
    tmp_path: Path,
) -> None:
    store = QueueStore(tmp_path / "jobs.db")
    job, _ = store.enqueue(
        idempotency_key="job-1",
        kind="echo",
        payload={"message": "hello"},
        max_attempts=1,
        now=100.0,
    )

    claimed = store.claim_next(
        worker_id="worker-a",
        lease_seconds=5,
        now=100.0,
    )
    assert claimed is not None
    assert claimed.attempts == 1

    store.recover_expired_leases(now=106.0)
    recovered = store.get(job.id)

    assert recovered is not None
    assert recovered.status is JobStatus.FAILED
