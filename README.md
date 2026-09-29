# Distributed Job Runner

[![quality](https://github.com/ashmawi-ctrl/distributed-job-runner/actions/workflows/quality.yml/badge.svg)](https://github.com/ashmawi-ctrl/distributed-job-runner/actions/workflows/quality.yml)

A small backend system for executing durable jobs with explicit ownership, retries, and crash recovery.

The project focuses on a failure mode that appears in queue-backed services: a worker can claim work and disappear before it finishes. A reliable runner needs to distinguish queued work from leased work, make ownership visible, and recover abandoned jobs without allowing two workers to complete the same lease.

The first implementation uses FastAPI, SQLite, and a polling worker. SQLite keeps the concurrency model inspectable while the state machine and lease semantics are developed.

## Job states

```text
queued
  |
  v
processing -------> succeeded
  |
  +--- handler error ---> retry ---> processing
  |
  +--- max attempts ---> failed
  |
  +--- lease expires --> retry / failed
```

A worker increments the attempt count when it claims a job. The lease stores both an owner ID and an expiry time.

Completion, heartbeat, and failure transitions require the current active lease owner. A stale worker cannot finish a job after another worker has recovered and reclaimed it.

## Why leases?

A status such as `processing` is not enough to prove a worker is still alive.

A time-bounded lease gives the queue a recovery rule:

- before expiry, only the lease owner may mutate the running job
- after expiry, the ownership is no longer trusted
- recoverable jobs return to `retry`
- jobs that already consumed their final attempt become `failed`

This is deliberately different from blindly re-running every abandoned job forever.

## API

Run:

```bash
pip install -e ".[dev]"
uvicorn jobrunner.api:app --reload
```

Create a job:

```http
POST /jobs
Content-Type: application/json

{
  "idempotency_key": "order-1001",
  "kind": "echo",
  "payload": {
    "message": "hello"
  },
  "max_attempts": 3
}
```

Submitting the same idempotency key again returns the existing logical job instead of creating a duplicate.

Inspect it:

```http
GET /jobs/{job_id}
```

Recover expired leases:

```http
POST /maintenance/recover
```

## Worker

Run one poll:

```bash
job-runner-worker --db jobs.db --once
```

Or keep polling:

```bash
job-runner-worker \
  --db jobs.db \
  --worker-id worker-a \
  --lease-seconds 30 \
  --poll-seconds 1
```

The first version exposes two deliberately small built-in handlers:

- `echo`
- bounded `sleep`

It does not execute arbitrary shell commands.

## Retry behavior

Failed handlers use exponential backoff:

```text
attempt 1 -> base delay
attempt 2 -> base delay * 2
attempt 3 -> base delay * 4
```

A job moves to `failed` once its configured attempt budget is exhausted.

## Concurrency test

The storage integration suite opens the same SQLite file from two worker instances and races them for one queued job.

The expected result is exactly one successful claim.

This is backed by `BEGIN IMMEDIATE` around claim selection and mutation rather than by an in-process Python lock.

## Project structure

```text
jobrunner/
  api.py
  handlers.py
  models.py
  service.py
  storage.py
  worker.py

tests/
  test_api.py
  test_service.py
  test_storage.py
```

## Quality

```bash
make install
make quality
```

## Docker

```bash
docker build -t distributed-job-runner .
docker run --rm -p 8000:8000 -v "$PWD/data:/data" distributed-job-runner
```

A second container can use the same mounted database and run:

```bash
job-runner-worker --db /data/jobs.db
```

For a real multi-host deployment, SQLite would not be the intended coordination backend.

## Engineering workflow

The first durable lease implementation is tracked through:

- [Issue #1](https://github.com/ashmawi-ctrl/distributed-job-runner/issues/1)
- branch `feat/durable-job-leases`
- storage concurrency and ownership regression tests
- API integration tests
- GitHub Actions
- reviewable pull request

## Deliberate limitations

- SQLite coordination only
- polling rather than push delivery
- no scheduled/cron jobs
- no priority queues
- no cancellation
- no worker heartbeat loop for long-running handlers yet
- no PostgreSQL `SKIP LOCKED` backend
- no distributed tracing

The v1 goal is to make ownership and recovery semantics explicit before introducing more infrastructure.

## Next changes

- worker heartbeat loop for jobs longer than one lease interval
- PostgreSQL storage backend with row-level locking
- dead-letter inspection API
- job cancellation semantics
- structured metrics for queue depth, retries, and lease recovery

## License

MIT
