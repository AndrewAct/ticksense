.PHONY: up down restart logs lint format typecheck test coverage \
        build flink-submit flink-logs flink-venv ingest ingest-logs \
        dbt-compile dbt-run dbt-test replay-sample help

# ── Local stack ──────────────────────────────────────────────────────────────

up:
	docker compose up -d --wait

down:
	docker compose down -v

restart:
	docker compose restart

logs:
	docker compose logs -f

# ── Code quality ─────────────────────────────────────────────────────────────

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff format .
	uv run ruff check --fix .

typecheck:
	uv run mypy ingest/src api/src replay/src

# ── Testing ───────────────────────────────────────────────────────────────────

test:
	uv run pytest

coverage:
	uv run pytest --cov --cov-report=term-missing

# ── dbt ───────────────────────────────────────────────────────────────────────

dbt-compile:
	uv run dbt compile --project-dir dbt

dbt-run:
	uv run dbt run --project-dir dbt

dbt-test:
	uv run dbt test --project-dir dbt

# ── Utilities ─────────────────────────────────────────────────────────────────

replay-sample:
	uv run python -m replay.main --events 10000 --dry-run

build:
	docker compose build --no-cache

# ── Ingest ────────────────────────────────────────────────────────────────────

ingest:
	uv run python -m ingest.main

ingest-logs:
	docker compose logs -f ingest

# ── Flink ─────────────────────────────────────────────────────────────────────

flink-submit:
	docker compose run --rm flink-init

flink-logs:
	docker compose logs -f flink-jobmanager flink-taskmanager flink-init

flink-venv:
	@echo "Requires: make up (flink-jobmanager must be running)"
	python3.10 -m venv flink/.venv
	docker compose cp flink-jobmanager:/usr/local/lib/python3.10/dist-packages/pyflink \
	  flink/.venv/lib/python3.10/site-packages/pyflink
	flink/.venv/bin/pip install --upgrade pip --quiet
	flink/.venv/bin/pip install "py4j==0.10.9.7" "cloudpickle==2.2.1" "python-dateutil>=2.8.0" --quiet
	@echo "flink/.venv ready — select flink/.venv/bin/python as interpreter in your IDE"

help:
	@grep -E '^[a-zA-Z_-]+:' Makefile | awk -F: '{printf "  %-20s\n", $$1}'
