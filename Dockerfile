FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY jobrunner ./jobrunner

RUN pip install --no-cache-dir .

ENV JOB_RUNNER_DB=/data/jobs.db

VOLUME ["/data"]

CMD ["uvicorn", "jobrunner.api:app", "--host", "0.0.0.0", "--port", "8000"]
