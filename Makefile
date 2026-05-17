.PHONY: up down restart logs lint format typecheck test coverage \
        build flink-submit flink-logs flink-venv ingest ingest-logs \
        dbt-compile dbt-run dbt-test dbt-freshness replay-sample watch-cdc \
        load-gen help

# ── Local stack ──────────────────────────────────────────────────────────────

up:
	docker compose up -d --wait

down:
	docker compose down -v

restart:
	docker compose restart

logs:
	docker compose logs -f

watch-cdc:
	@echo "Watching normalized.symbol_config — Ctrl+C to stop"; \
	while true; do \
		printf "\n\033[1m$$(date -u '+%H:%M:%S UTC')\033[0m\n"; \
		docker compose exec -T trino trino --execute \
			"SELECT symbol, status, exchange FROM iceberg.normalized.symbol_config ORDER BY symbol" \
			2>/dev/null || echo "(table not yet available — waiting for first checkpoint)"; \
		sleep 10; \
	done

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

DBT     = uv run --with dbt-trino dbt --no-send-anonymous-usage-stats
DBTOPTS = --project-dir dbt --profiles-dir dbt

dbt-compile:
	$(DBT) compile $(DBTOPTS)

dbt-run:
	$(DBT) run $(DBTOPTS)

dbt-test:
	$(DBT) test $(DBTOPTS)

dbt-freshness:
	$(DBT) source freshness $(DBTOPTS)

# ── Utilities ─────────────────────────────────────────────────────────────────

replay-sample:
	uv run python -m replay.main --events 10000 --dry-run

build:
	docker compose build --no-cache

# ── Load testing ──────────────────────────────────────────────────────────────

load-gen:
	docker run --rm \
		--network ticksense_default \
		-v $(CURDIR)/k6:/scripts:ro \
		-e K6_PROMETHEUS_RW_SERVER_URL=http://prometheus:9090/api/v1/write \
		-e 'K6_PROMETHEUS_RW_TREND_STATS=p(50),p(95),p(99),max' \
		grafana/k6:latest \
		run --out experimental-prometheus-rw /scripts/script.js

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
