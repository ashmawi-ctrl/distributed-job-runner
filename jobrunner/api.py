import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field

from .models import Job
from .storage import QueueStore


class CreateJobRequest(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=200)
    kind: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1, le=20)


class RecoverResponse(BaseModel):
    recovered: int


def create_app(db_path: str | Path = "jobs.db") -> FastAPI:
    app = FastAPI(
        title="Distributed Job Runner",
        version="0.1.0",
    )
    store = QueueStore(db_path)
    app.state.store = store

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/jobs")
    def create_job(
        request: CreateJobRequest,
        response: Response,
    ) -> dict[str, Any]:
        job, created = store.enqueue(
            idempotency_key=request.idempotency_key,
            kind=request.kind,
            payload=request.payload,
            max_attempts=request.max_attempts,
        )
        response.status_code = 201 if created else 200
        return {
            "created": created,
            "job": _job_dict(job),
        }

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, Any]:
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return _job_dict(job)

    @app.post("/maintenance/recover", response_model=RecoverResponse)
    def recover_expired() -> RecoverResponse:
        return RecoverResponse(
            recovered=store.recover_expired_leases()
        )

    return app


def _job_dict(job: Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "idempotency_key": job.idempotency_key,
        "kind": job.kind,
        "payload": job.payload,
        "status": job.status.value,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "available_at": job.available_at,
        "lease_owner": job.lease_owner,
        "lease_expires_at": job.lease_expires_at,
        "last_error": job.last_error,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }


app = create_app(os.getenv("JOB_RUNNER_DB", "jobs.db"))
