---
description: Scoped to the DuckDB → PostgreSQL migration project. Within this project, Codex is elevated to primary author for DB-specific code (DDL/SQL/cutover/backfill). Deprecate when stop conditions are met.
---

> Multi-agent routing, effort, and escalation policy: [`docs/references/multi-agent.md`](../../../docs/references/multi-agent.md)
> Topology decision document: [`docs/references/db_migration_topology.md`](../../../docs/references/db_migration_topology.md)
> Migration plan body: `/home/user/.claude/plans/db-splendid-crayon.md`

# 🎯 Skill Purpose (Trigger)

Scoped to this project (DuckDB → PostgreSQL migration), Codex's role is **elevated from validator (spec §2.3 default) to primary author**. Rationale: cutover to PG is a one-way event with potential for data loss, and the correctness of raw SQL/DDL benefits more from ensemble strength than site-wide glue code.

Trigger in the following situations:

- **DDL authoring/modification** — `sql/schema_postgres.sql`, `sql/migrations/postgres/*.sql`
- **PG raw SQL authoring** — SQL strings inside `resources/postgres_*.py`, `ON CONFLICT`/`RETURNING`/`%s` placeholder conversion
- **Data migration scripts** — `scripts/migrate_duckdb_to_postgres.py`, backfill SQL, cutover transaction sequences
- **MotherDuck sync rewrite** — when `local_duckdb_to_motherduck_sync.py` source changes from DuckDB → PG with ATTACH/READ_ONLY modifications
- **User explicitly requests `/codex_db_migration` or "have Codex write this directly"**

**Automatic trigger rules by file/keyword** — route to this skill if any of the following apply:
- File path contains `schema_postgres`, `_pg.py`, `pg_*.py`, `migration`, `backfill`, `cutover`
- Diff contains keyword changes: `CREATE`/`ALTER`/`DROP`/`COPY`/`INSERT INTO`/`ATTACH`/`ON CONFLICT`
- File path contains `postgres_*.py` resources mixin

## Cases Where Codex Is NOT the Primary Author (Sonnet authors)

Even within this project, the following are authored by Sonnet with Codex doing a standard review (`codex_collab` pattern):

- Pg*Resource Python wrapper skeleton (connect/pool/context manager — outside of SQL)
- Dagster `@asset`, `@op`, `@sensor`, `@schedule` glue
- `tests/conftest.py` PG fixture setup (raw SQL itself imports from schema files, so that's OK)
- Environment variables, env_utils, `definitions_production.py` resource wiring
- docker-compose service definitions, `.env.example`
- Operational documentation, runbooks, CLAUDE.md updates

# 🛠️ Dependencies

- **Sub-agent:** `.claude/agents/codex.md` — `Agent(subagent_type="codex", ...)`. **read-only sandbox remains in place** (this skill elevates authorship, not write permissions).
- **MCP server:** `codex` in project-scope `.mcp.json` (`@nayagamez/codex-cli-mcp`). Exposed tools: `mcp__codex__codex`, `mcp__codex__codex-reply`.
- **Codex CLI:** npm binary `/home/user/.local/codex-npm/node_modules/.bin/codex` (snap version is not compatible with MCP).
- **Reference schema (source of truth):** [`src/vlm_pipeline/sql/schema.sql`](../../../src/vlm_pipeline/sql/schema.sql) — DuckDB DDL.
- **Reference mixins (PG porting targets):** [`src/vlm_pipeline/resources/duckdb_*.py`](../../../src/vlm_pipeline/resources/) — 11 files.
- **Reference backfill source:** [`src/python/local_duckdb_to_motherduck_sync.py`](../../../src/python/local_duckdb_to_motherduck_sync.py).

# 📝 Action Steps

## 1. Classify the Change Scope

The main agent classifies the received request into one of the following three types:

- **T1 — Codex-leads-DDL**: creating or modifying schema/DDL/SQL/migration files. → T1 prompt in §2.
- **T2 — Pattern B for cutover SQL**: used only at two points in this project — the final cutover transaction sequence and dual-write window race analysis. → Delegate to `codex_arbitration` skill.
- **T3 — Codex-reviews-glue**: Dagster glue/test/config changes. Raw SQL is only imported from schema files. → Standard `codex_collab` pattern, effort=high (or medium for <50 LoC).

If the automatic trigger rules (see §🎯 above) point to T1, route to T1. When more than one applies, T1 takes priority.

## 2. T1 — Codex-leads-DDL Prompt Template

```python
Agent(
  subagent_type="codex",
  description="codex DB-specific code authoring",
  prompt="""
  Ask kind: primary authorship — codex is the author for this diff
  Effort: ultra
  Project context: DuckDB → PostgreSQL migration (project-scoped override per
    .agent/skill/codex_db_migration/SKILL.md). Codex is the primary author for this task.
  Target: <absolute file path(s) — new or modified>
  DuckDB reference: <absolute path of duckdb_*.py mixin or schema.sql section + line range>
  Goal: <1-line — e.g. "produce PG DDL for raw_files with parity to schema.sql:10-30">
  Parity requirements:
    - column types per project type-mapping (VARCHAR→TEXT, BIGINT→BIGINT, DOUBLE→DOUBLE PRECISION,
      JSON→TEXT for now (JSONB after audit pass, in a follow-up PR), TIMESTAMP→TIMESTAMP, BOOLEAN→BOOLEAN)
    - PRIMARY KEY / UNIQUE / FOREIGN KEY / DEFAULT clauses preserved as-is
    - DEFAULT CURRENT_TIMESTAMP identical
    - INSERT OR REPLACE → INSERT ... ON CONFLICT (<pk>) DO UPDATE SET col=EXCLUDED.col, ...
    - All placeholders in psycopg2 style (%s), DuckDB ?-style forbidden
  Constraints:
    - PostgreSQL 15+ syntax
    - Idempotent (CREATE ... IF NOT EXISTS, ALTER TABLE IF EXISTS ... ADD COLUMN IF NOT EXISTS)
    - One SQL statement per object (multi-statement blocks separated into sequences)
    - Avoid vendor lock-in (stay within the range Alembic autogenerate can handle)
    - Preserve operational data (DROP/TRUNCATE forbidden — new migrations use forward-only ALTER only)

  Deliverable:
    1. Full file content (or unified diff if modifying existing)
    2. Parity table: | column | duckdb type | pg type | notes (reason if intentionally changed) |
    3. List of DuckDB features intentionally omitted (e.g. HASH(), unnest, etc.) + reasons
    4. Index/constraint additions that would be valuable in PG (optional, "none" if not applicable)

  DO NOT modify any files. Read-only proposal only.
  """
)
```

**Main agent's role after the call (applier, not re-decider)**:
- Output the Codex response to the user as-is. Surface the parity table at the top.
- Do not rewrite SQL to make it "better". If there are questions, follow up in the same thread with `mcp__codex__codex-reply`.
- If the main agent has its own opinion, show it separately in one line (without taking sides).

## 3. Verification (Main Agent Runs After Codex Authors)

```bash
# (a) Syntax validation — throwaway PG container
docker run --rm -d --name pg-validate -e POSTGRES_PASSWORD=test \
  -p 25432:5432 postgres:15
sleep 3
docker cp src/vlm_pipeline/sql/schema_postgres.sql pg-validate:/tmp/
docker exec pg-validate psql -U postgres -f /tmp/schema_postgres.sql -v ON_ERROR_STOP=on
docker exec pg-validate psql -U postgres -c '\dt'    # verify 15 tables
docker exec pg-validate psql -U postgres -c "SELECT count(*) FROM information_schema.table_constraints WHERE constraint_type='FOREIGN KEY'"
docker rm -f pg-validate

# (b) Unit tests (from the point PG fixtures are introduced)
pytest tests/unit -q -k postgres

# (c) Dagster definitions load (if resources were changed)
docker exec docker-dagster-code-server-1 python3 -c "
from vlm_pipeline.definitions import defs
print(len(list(defs.get_asset_graph().all_asset_keys)))"
```

If a syntax error occurs, **do not have the main agent fix the SQL**. Instead, pass the error message as-is to Codex via `mcp__codex__codex-reply` and request a rewrite. If the second attempt also fails, trigger §🚫 §7.2 Opus escalation.

## 4. User Approval

Use `AskUserQuestion` with:
- "Apply" — main agent applies with `Edit`/`Write` as-is
- "Modify then apply" — main agent applies only the user-specified changes
- "Ask Codex again" — follow-up maintaining threadId
- "Cancel"

## 5. Commit After Applying (When Requested by User)

Conventional commit format. Note Codex authorship in the footer:

```
feat(db): add schema_postgres.sql with parity to schema.sql

Codex-authored: yes
Skill: codex_db_migration
```

# 🔁 Hand-off Rules

| Phase | Who | What |
|---|---|---|
| Pre-PR (DDL/SQL changes) | Main | (a) throwaway PG syntax check, (b) surface parity table to user, (c) unit tests for changed raw SQL |
| Pre-PR (T2 cutover SQL) | Main | Pattern B result + sample run on staging snapshot |
| Pre-merge to `dev` | Opus one-time review | Full integrated diff + parity + test results |
| Pre-promote to `main` | Opus one-time review | + cutover plan + rollback SQL + operator runbook review |
| Two models disagree | Opus | spec §7.2 trigger |

Opus is called only at the two merge gates and on opinion conflicts — not in every PR loop. Cost constrained.

# 🚫 Constraints

- **The codex sub-agent is still read-only.** This skill elevates authorship semantically — it does not grant write permissions. The main agent applies changes.
- **DROP/TRUNCATE forbidden.** Migration to PG uses forward-only ALTER only. Data preservation is the priority.
- **JSONB changes go in a separate PR.** The `schema_postgres.sql` draft starts with `TEXT` → after JSON column audit passes, apply `ALTER COLUMN ... TYPE JSONB USING <col>::jsonb` in a separate migration.
- **Block secrets exposure.** Filter to ensure `MOTHERDUCK_TOKEN`, `POSTGRES_PASSWORD`, `.env`, credentials, etc. do not appear in the prompt.
- **Pattern B (T2) applies only to two cases**: the final cutover SQL and dual-write race analysis. Do not trigger it on every PR (cost explosion).
- **`low` effort is forbidden.** Same rule as spec §3.3. If the change is trivial, skip the Codex call entirely.
- **Production-impacting work goes to staging first.** dispatch / sensor / migration / asset changes should be tested at `:3031` before production.
- **No direct commits to main/dev.** Always use a feature/fix branch.

# 🛑 Stop Conditions

This skill is deprecated when **both** of the following are true:

1. PG cutover has been merged to main and has been running in production for ≥ 2 weeks without incident.
2. DuckDB code paths are gated off, or the read path has been completely removed from the pipeline.

Deprecation procedure:

```bash
# Move the skill file to the deprecated directory
mv .agent/skill/codex_db_migration .agent/skill/_deprecated/codex_db_migration_$(date +%Y-%m-%d)
# Clean up cross-references in AGENTS.md, CLAUDE.md, multi-agent.md
```

After deprecation, effort policy automatically reverts to the `multi-agent.md` §3.3 defaults, and Codex's role reverts to validator per §2.3.

# 📚 Reference — Key Decisions for This Project

- **Type mapping**: `VARCHAR`→`TEXT`, `DOUBLE`→`DOUBLE PRECISION`, `JSON`→`TEXT` (initial) → `JSONB` (follow-up PR). All others are 1:1.
- **`INSERT OR REPLACE` sites** (4): `duckdb_labeling.py:712,729`, `duckdb_ingest_metadata.py:165`, `duckdb_dedup.py:120`. All converted to `ON CONFLICT ... DO UPDATE` based on PK.
- **10 FKs** (backfill order): `raw_files → video_metadata → labels → processed_clips → image_metadata → image_labels → datasets → dataset_clips → dispatch_requests/staging_model_configs/dispatch_pipeline_runs/labeling_specs/labeling_configs/requester_config_map/classification_datasets`.
- **Introspection rewrite**: `duckdb_constraints()` (`duckdb_migration.py:690-700`) → `pg_constraint` JOIN `pg_attribute` via `unnest(conkey)`.
- **Topology**: staging adds `vlm_pipeline_staging` DB to the existing `postgres:15` instance; production uses a new `postgres-pipeline` container with `vlm_pipeline` DB. See [`docs/references/db_migration_topology.md`](../../../docs/references/db_migration_topology.md) for details.
