import argparse
import socket
import time
import uuid
from pathlib import Path

from .handlers import execute_job
from .service import JobWorker
from .storage import QueueStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="job-runner-worker",
        description="Poll and execute durable jobs.",
    )
    parser.add_argument("--db", type=Path, default=Path("jobs.db"))
    parser.add_argument("--worker-id")
    parser.add_argument("--lease-seconds", type=float, default=30.0)
    parser.add_argument("--retry-seconds", type=float, default=2.0)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--once", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.poll_seconds < 0:
        print("error: poll-seconds cannot be negative")
        return 2

    worker_id = args.worker_id or (
        f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
    )

    try:
        worker = JobWorker(
            QueueStore(args.db),
            worker_id=worker_id,
            handler=execute_job,
            lease_seconds=args.lease_seconds,
            base_retry_seconds=args.retry_seconds,
        )
    except ValueError as exc:
        print(f"error: {exc}")
        return 2

    while True:
        result = worker.work_once()

        if args.once:
            return 0

        if result is None:
            time.sleep(args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
