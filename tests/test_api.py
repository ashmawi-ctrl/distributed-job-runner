from pathlib import Path

from fastapi.testclient import TestClient

from jobrunner.api import create_app


def test_create_job_is_idempotent_and_inspectable(tmp_path: Path) -> None:
    client = TestClient(create_app(tmp_path / "jobs.db"))
    payload = {
        "idempotency_key": "order-1001",
        "kind": "echo",
        "payload": {"message": "hello"},
        "max_attempts": 3,
    }

    created = client.post("/jobs", json=payload)
    duplicate = client.post("/jobs", json=payload)

    assert created.status_code == 201
    assert duplicate.status_code == 200
    assert created.json()["created"] is True
    assert duplicate.json()["created"] is False

    job_id = created.json()["job"]["id"]
    inspected = client.get(f"/jobs/{job_id}")

    assert inspected.status_code == 200
    assert inspected.json()["status"] == "queued"
    assert inspected.json()["payload"] == {"message": "hello"}


def test_missing_job_returns_404(tmp_path: Path) -> None:
    client = TestClient(create_app(tmp_path / "jobs.db"))

    response = client.get("/jobs/missing")

    assert response.status_code == 404
