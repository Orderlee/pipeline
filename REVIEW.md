# REVIEW.md — VLM Data Pipeline Review Standards

> Standards for automated Claude PR reviews and manual reviews

---

## Critical (must request changes)

- Missing DuckDB writer tag: a DB-writing asset lacks `tags={"duckdb_writer": "true"}`
- Import layer violation: importing in the wrong direction (e.g., L5 → L3)
- Hardcoded credentials / API keys / passwords
- MinIO bucket name violates the fixed set of 4: `vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`
- Label JSON stored in `vlm-processed` (source of truth is `vlm-labels` only)
- Production/test environment isolation violation (mixed paths, ports, or DuckDB files)

## Warning (strong recommendation)

- No unit test written for a new asset/op
- `@op+@job` used without checking whether `@asset` is a viable replacement
- Per-file fail-forward pattern not followed (a single file failure halts the entire run)
- Sensor does not catch `OSError/PermissionError` (will crash on NAS failures)
- ruff line-length exceeds 120
- `raw_key` includes a `YYYY/MM` prefix (forbidden)
- Archive move logic missing suffix collision handling

## Info (advisory)

- Function/module is too large; splitting is recommended (existing pattern: separate `assets.py` + `helpers.py`)
- Missing docstrings on newly written public functions
- Magic numbers should be replaced with constants/config
- Check whether the MinIO key builder consolidation pattern in `lib/key_builders.py` is followed

## Review Scope Exclusions

- PRs with only `docs/**` or `*.md` changes are not subject to review
- Changes under `.cursor/**` or `.agent/**` are ignored
- PRs with only `tests/**` changes — check test quality only

## Deployment Impact Check

For dev → main PRs (release PRs), additionally verify:
- `docker/Dockerfile` changes and their image build impact
- `docker-compose.yaml` changes and their volume/network impact
- Whether `.env.example` is synced when env variables are added or changed
- Rollback compatibility for any `scripts/deploy/` changes
