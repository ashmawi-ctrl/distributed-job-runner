# Distributed Job Runner

A small backend system for executing durable jobs with explicit ownership, retries, and crash recovery.

The project focuses on a failure mode that appears in queue-backed services: a worker can claim work and disappear before it finishes. A reliable runner needs to distinguish queued work from leased work, make ownership visible, and recover abandoned jobs without allowing two workers to complete the same lease.

The first implementation is intentionally narrow: a FastAPI control plane, a SQLite-backed queue, and a worker process with time-bounded leases. Later changes can add richer scheduling and a production database backend.
