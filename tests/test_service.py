from pathlib import Path

from jobrunner.models import JobStatus
from jobrunner.service import JobWorker
from jobrunner.storage import QueueStore


def test_worker_completes_successful_job(tmp_path: Path) -> None:
    store = QueueStore(tmp_path / "jobs.db")
    job, _ = store.enqueue(
        idempotency_key="job-1",
        kind="echo",
        payload={"message": "hello"},
        now=100.0,
    )

    seen: list[str] = []
    worker = JobWorker(
        store,
        worker_id="worker-a",
        handler=lambda claimed: seen.append(claimed.id),
        lease_seconds=30,
    )

    result = worker.work_once(now=100.0)

    assert result is not None
    assert result.status is JobStatus.SUCCEEDED
    assert seen == [job.id]


def test_worker_schedules_retry_when_handler_raises(tmp_path: Path) -> None:
    store = QueueStore(tmp_path / "jobs.db")
    job, _ = store.enqueue(
        idempotency_key="job-1",
        kind="echo",
        payload={"message": "hello"},
        now=100.0,
    )

    def fail(_job) -> None:
        raise RuntimeError("dependency unavailable")

    worker = JobWorker(
        store,
        worker_id="worker-a",
        handler=fail,
        lease_seconds=30,
        base_retry_seconds=4,
    )

    result = worker.work_once(now=100.0)

    assert result is not None
    assert result.status is JobStatus.RETRY
    assert result.available_at == 104.0
    assert result.last_error == "RuntimeError: dependency unavailable"
    assert result.id == job.id
