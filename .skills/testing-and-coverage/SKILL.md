---
name: testing-and-coverage
description: Use when adding or modifying any application code. No feature is complete without tests.
---

# Testing and Coverage

## Rule

No feature is complete without tests. Coverage must not decrease. Target: ≥80% now, ≥90% once stable.

## Test layout

```
tests/
  unit/          pure logic, no I/O, fast
  integration/   Kafka, Postgres, FastAPI with real containers
  e2e/           full pipeline: ingest → Kafka → Flink → Iceberg → query
```

## Tools

- pytest + pytest-asyncio (`asyncio_mode = "auto"`)
- pytest-cov for coverage
- testcontainers[redpanda,postgres] for integration tests
- httpx AsyncClient for FastAPI endpoint tests

## Required test types per change

| Change | Unit | Integration | Negative | Idempotency |
|---|---|---|---|---|
| Parser / schema validation | ✓ | | ✓ | |
| Kafka producer | ✓ | ✓ | ✓ | ✓ |
| Kafka consumer | ✓ | ✓ | ✓ | ✓ |
| Iceberg write | ✓ | ✓ | | ✓ |
| FastAPI endpoint | ✓ | ✓ | ✓ | |
| Airflow DAG task | ✓ | | | ✓ |
| dbt model | | ✓ | | |
| Order book state machine | ✓ | | ✓ | ✓ |

## What to always test for Kafka code

- Happy path: valid message produced and consumed
- Malformed input: routed to DLQ, not crashed
- Duplicate message: deduplicated correctly, not double-inserted
- Retry: same message twice yields same result (idempotency)

## What to always test for order book logic

- Snapshot initialization
- Valid sequential diff application
- Out-of-order diff (lower `lastUpdateId` than current state)
- Sequence gap detection (triggers re-snapshot)
- Empty level removal (qty = 0 removes the price level)

## Report when done

State: tests added, commands run (`make test coverage`), coverage result, known gaps.
