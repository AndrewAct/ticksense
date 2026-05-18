.PHONY: up down restart logs lint format typecheck test coverage \
        build flink-submit flink-logs flink-venv ingest ingest-logs \
        dbt-compile dbt-run dbt-test dbt-freshness replay-sample watch-cdc \
        load-gen load-test-full help

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

# make load-gen: runs k6 only — no prerequisite checks.
# Use `make load-test-full` for a fully-orchestrated test with all checks.
load-gen:
	docker run --rm \
		--network ticksense_default \
		-v $(CURDIR)/k6:/scripts:ro \
		-e K6_PROMETHEUS_RW_SERVER_URL=http://prometheus:9090/api/v1/write \
		-e 'K6_PROMETHEUS_RW_TREND_STATS=p(50),p(95),p(99),max' \
		grafana/k6:latest \
		run --out experimental-prometheus-rw /scripts/script.js

# make load-test-full: end-to-end load test with full prerequisite verification.
#
# WHY this exists — `make load-gen` alone always silently produces 100% 404s
# unless the full data pipeline is ready beforehand. The prerequisites are:
#
#   1. Flink jobs RUNNING — Flink writes to normalized.* and raw.*.
#      flink-init only *submits* the jobs; they need a few seconds to reach
#      RUNNING state before any messages are processed.
#
#   2. ingest producing to Kafka — normalized.book_ticker stays empty until
#      the WebSocket ingest is active. ingest runs outside Docker because
#      apache-flink cannot be installed in the same uv workspace as the host
#      Python 3.13 environment. This target auto-starts it in the background
#      if it is not already running, logging to /tmp/ticksense-ingest.log.
#
#   3. Flink first checkpoint complete (~60 s) — Iceberg files are written
#      on checkpoint, not on every message. Nothing appears in normalized.*
#      until the first checkpoint commits.
#
#   4. dbt run AFTER Flink has data — the Docker dbt-runner executes once at
#      stack startup, before Flink has written anything. mart_liquidity and
#      mart_ohlcv are empty until dbt is re-run against live Flink output.
#
#   5. API poller warm — the API background poller must complete at least one
#      full cycle after dbt runs before the ReadModel has data. The poller
#      starts immediately on API startup and refreshes every 30 s; after
#      `dbt run` populates the marts the next poll cycle picks it up.
#
load-test-full:
	@echo ""
	@echo "╔═══════════════════════════════════════════╗"
	@echo "║   TickSense — Full Load Test Pipeline     ║"
	@echo "╚═══════════════════════════════════════════╝"
	@echo ""
	@echo "── Step 1/6  Flink jobs RUNNING ────────────"
	@echo "   (normalize + ohlcv_1m + CDC must all be in RUNNING state)"
	@_flink_running() { \
	    curl -sf http://localhost:8081/jobs/overview 2>/dev/null \
	    | python3 -c 'import sys,json; d=json.load(sys.stdin); print(sum(1 for j in d["jobs"] if j["state"]=="RUNNING"))' \
	    2>/dev/null || echo 0; \
	}; \
	_ts=$$(date +%s); \
	until [ "$$(_flink_running)" -ge 2 ]; do \
	    if [ $$(( $$(date +%s) - $$_ts )) -ge 180 ]; then \
	        echo "   ERROR: Flink jobs did not reach RUNNING within 3 min"; \
	        echo "   Debug: docker compose logs flink-jobmanager flink-init"; \
	        exit 1; \
	    fi; \
	    echo "   waiting for Flink jobs RUNNING (currently $$(_flink_running))..."; \
	    sleep 5; \
	done
	@echo "   ✓ Flink jobs RUNNING"
	@echo ""
	@echo "── Step 2/6  Verify ingest ──────────────────"
	@echo "   (ingest runs as a Docker service; a local process is also supported)"
	@if docker compose ps ingest 2>/dev/null | grep -q "Up"; then \
	    echo "   Docker ingest service is running — OK"; \
	elif pgrep -f "ingest.main" > /dev/null 2>&1; then \
	    echo "   host ingest already running (PID $$(pgrep -f 'ingest.main'))"; \
	else \
	    echo "   Starting ingest in background → logs at /tmp/ticksense-ingest.log"; \
	    uv run python -m ingest.main >> /tmp/ticksense-ingest.log 2>&1 & \
	    echo "   ingest started (PID $$!)"; \
	fi
	@echo ""
	@echo "── Step 3/6  Wait for normalized data ──────"
	@echo "   (book_ticker: Flink first checkpoint ~60 s)"
	@echo "   (ohlcv_1m:    1-minute window must close + checkpoint ~120 s)"
	@_ts=$$(date +%s); \
	until docker compose exec -T trino trino \
	    --execute "SELECT count(*) FROM iceberg.normalized.book_ticker" \
	    2>/dev/null | grep -qE "[1-9]"; do \
	    if [ $$(( $$(date +%s) - $$_ts )) -ge 180 ]; then \
	        echo "   ERROR: normalized.book_ticker still empty after 3 min — is ingest running?"; \
	        exit 1; \
	    fi; \
	    echo "   normalized.book_ticker still empty — retry in 15 s..."; \
	    sleep 15; \
	done
	@echo "   ✓ normalized.book_ticker has rows"
	@_ts=$$(date +%s); \
	until docker compose exec -T trino trino \
	    --execute "SELECT count(*) FROM iceberg.normalized.ohlcv_1m WHERE window_start >= NOW() - INTERVAL '2' HOUR" \
	    2>/dev/null | grep -qE "[1-9]"; do \
	    if [ $$(( $$(date +%s) - $$_ts )) -ge 300 ]; then \
	        echo "   ERROR: no recent OHLCV bars after 5 min — check: docker compose logs flink-jobmanager"; \
	        exit 1; \
	    fi; \
	    echo "   normalized.ohlcv_1m has no recent data (waiting for 1-min window to close) — retry in 15 s..."; \
	    sleep 15; \
	done
	@echo "   ✓ normalized.ohlcv_1m has recent rows"
	@echo ""
	@echo "── Step 4/6  Run dbt ───────────────────────"
	@echo "   (WHY: the Docker dbt-runner ran once at startup before Flink had data;"
	@echo "    mart_liquidity / mart_ohlcv are empty until we re-run dbt now)"
	$(DBT) run $(DBTOPTS)
	@echo "   ✓ dbt run complete"
	@echo ""
	@echo "── Step 5/6  Verify marts + API ohlcv ready ─"
	@docker compose exec -T trino trino \
	    --execute "SELECT count(*) FROM iceberg.marts.mart_liquidity" \
	    2>/dev/null | grep -qE "[1-9]" || \
	    (echo "   ERROR: mart_liquidity still empty after dbt run — check dbt logs" && exit 1)
	@echo "   ✓ mart_liquidity has rows"
	@docker compose exec -T trino trino \
	    --execute "SELECT count(*) FROM iceberg.marts.mart_ohlcv" \
	    2>/dev/null | grep -qE "[1-9]" || \
	    (echo "   ERROR: mart_ohlcv still empty after dbt run — check dbt logs" && exit 1)
	@echo "   ✓ mart_ohlcv has rows"
	@echo "   Waiting for /ready (ReadModel ohlcv populated, poller refreshes every 60 s)..."
	@until curl -sf http://localhost:8000/ready 2>/dev/null; do \
	    echo "   API not ready — retry in 10 s..."; \
	    sleep 10; \
	done
	@echo "   ✓ API ready (ReadModel populated)"
	@echo ""
	@echo "── Step 6/6  k6 load test ──────────────────"
	@echo "   (k6 connects to api:8000 on ticksense_default Docker network)"
	@echo "   (ingest keeps running after test — kill with: pkill -f ingest.main)"
	@echo ""
	$(MAKE) load-gen

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
