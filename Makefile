.PHONY: install lint test quality run worker

install:
	python -m pip install -e ".[dev]"

lint:
	ruff check jobrunner tests

test:
	pytest

quality: lint test

run:
	uvicorn jobrunner.api:app --reload

worker:
	job-runner-worker --db jobs.db
