---
name: ci-cd-quality-gates
description: Use when modifying CI workflows, adding GitHub Actions steps, or confirming quality gates are satisfied before a PR.
---

# CI / CD Quality Gates

## Required GitHub Actions jobs

```
lint       ruff check + ruff format --check
typecheck  mypy across all workspace members
test       pytest (unit + integration with testcontainers)
coverage   fail below 80%
```

## PR checklist (all must pass)

- [ ] `make lint` — ruff check and format
- [ ] `make typecheck` — mypy strict
- [ ] `make test` — pytest
- [ ] Coverage did not decrease
- [ ] `docker compose config` passes (if Docker files changed)
- [ ] `dbt compile` passes (if dbt models changed)

## When modifying application code

Update CI in the same PR. CI that doesn't reflect current behavior is worse than no CI.

## Secret hygiene

- Repository must have GitHub secret scanning enabled
- `git-secrets` pre-commit hook recommended
- Never commit `.env`; only `.env.example` is tracked
- Workflow files must not contain hardcoded credentials or API keys

## Docker build check

Add to CI to catch Dockerfile errors before deployment:
```yaml
- name: Validate Docker Compose
  run: docker compose config
- name: Build images
  run: docker compose build --no-cache
```

## Future gates (add when relevant)

- Trivy container vulnerability scan
- dbt test in CI (requires Trino or DuckDB adapter)
- Great Expectations checkpoint

## Anti-patterns

- `--no-verify` on git commits to skip hooks
- Merging with failing CI ("I'll fix it in the next PR")
- Coverage exemptions (`# pragma: no cover`) without code comment explaining why
- Secrets in `env:` blocks of workflow files (use GitHub Actions secrets)
