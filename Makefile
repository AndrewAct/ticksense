.PHONY: up down restart logs lint format typecheck test coverage \
        build dbt-compile dbt-run dbt-test replay-sample help

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

help:
	@grep -E '^[a-zA-Z_-]+:' Makefile | awk -F: '{printf "  %-20s\n", $$1}'
