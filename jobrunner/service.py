from collections.abc import Callable

from .models import Job
from .storage import QueueStore

JobHandler = Callable[[Job], None]


class JobWorker:
    def __init__(
        self,
        store: QueueStore,
        *,
        worker_id: str,
        handler: JobHandler,
        lease_seconds: float = 30.0,
        base_retry_seconds: float = 2.0,
    ) -> None:
        if not worker_id.strip():
            raise ValueError("worker_id cannot be empty")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than 0")
        if base_retry_seconds < 0:
            raise ValueError("base_retry_seconds cannot be negative")

        self.store = store
        self.worker_id = worker_id
        self.handler = handler
        self.lease_seconds = lease_seconds
        self.base_retry_seconds = base_retry_seconds

    def work_once(self, *, now: float | None = None) -> Job | None:
        self.store.recover_expired_leases(now=now)
        job = self.store.claim_next(
            worker_id=self.worker_id,
            lease_seconds=self.lease_seconds,
            now=now,
        )
        if job is None:
            return None

        try:
            self.handler(job)
        except Exception as exc:
            return self.store.fail(
                job.id,
                worker_id=self.worker_id,
                error=f"{type(exc).__name__}: {exc}",
                base_retry_seconds=self.base_retry_seconds,
                now=now,
            )

        return self.store.complete(
            job.id,
            worker_id=self.worker_id,
            now=now,
        )
