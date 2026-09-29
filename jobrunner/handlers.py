import time

from .models import Job


class UnsupportedJobKindError(ValueError):
    pass


def execute_job(job: Job) -> None:
    if job.kind == "echo":
        message = job.payload.get("message")
        if not isinstance(message, str):
            raise ValueError("echo job requires a string 'message'")
        print(message)
        return

    if job.kind == "sleep":
        seconds = job.payload.get("seconds")
        if not isinstance(seconds, (int, float)):
            raise ValueError("sleep job requires numeric 'seconds'")
        if not 0 <= float(seconds) <= 10:
            raise ValueError("sleep seconds must be between 0 and 10")
        time.sleep(float(seconds))
        return

    raise UnsupportedJobKindError(f"unsupported job kind: {job.kind}")
