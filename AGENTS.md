# AGENTS.md — VLM Data Pipeline

This document is a **short map** for agents.
For detailed design, plans, and operational references, consult the `docs/` documentation system first.

## Documents to Read First

1. `README.md` — human-facing overview and operational flow
2. `docs/index.md` — full documentation table of contents
3. Sub-indexes matching the nature of your task:
   - `docs/design-docs/index.md`
   - `docs/exec-plans/index.md`
   - `docs/references/index.md`
4. Agent routing, effort, and escalation rules: `docs/references/multi-agent.md`

## Project in One Line

CCTV/security footage ingestion → deduplication → Gemini (Vertex) labeling → YOLO-World detection → training dataset build.
Stack: Dagster + DuckDB + MinIO + MotherDuck.

## Key Paths

- `src/vlm_pipeline/` — pipeline package
- `docker/` — Compose, workspace, env
- `scripts/` — operational/validation scripts
- `docs/` — design, execution plans, operational reference documents

## Environment Summary

| Item | Production (`main`) | Test (`dev`) |
|------|----------------------|--------------|
| Incoming host path | `/home/user/mou/incoming` | `/home/user/mou/staging/incoming` |
| DuckDB | `/data/pipeline.duckdb` | `/data/staging.duckdb` |
| Dagster UI | `3030` | `3031` |
| env | `docker/.env` | `docker/.env.test` |

Production and test share the same compose service definitions; they differ only by branch and env file.

## Mandatory Rules

- DuckDB enforces a single-writer principle.
- MinIO buckets are fixed: `vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`.
- The label JSON source of truth is `vlm-labels`.
- GCP auto-bootstrap manifests compact as `pending -> processed -> completed (summary)`; after `_DONE`, only one source unit/signature summary remains instead of per-chunk processed manifests.
- Major design decisions and operational rules must be documented in `docs/`, not left only in chat.
- For new tasks, explore in order: `AGENTS.md -> docs/index.md -> relevant sub-index`.

## Documentation Operating Principles

- `README.md`: product and operational overview
- `AGENTS.md`: agent entry point
- `docs/`: record system for design, plans, and reference documents
- Local-only notes and IDE settings are supplementary only, not the core source of truth.

## Local Supplementary Material

- `CLAUDE.md` can be used as a local detailed summary.
- Long-form reference material not tracked by git (e.g., `CLAUDE2.md`) may exist, but core decisions should be migrated to `docs/` where possible.
