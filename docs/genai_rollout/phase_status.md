# GenAI Studio Rollout — Phase Status

This file is read each time the cron wakes up to determine the next phase.
Used for cross-session context handoff.

## Current State

- **current_phase**: 6 (= production task / main PR)
- **completed_phases**: [0, 1, 2, 3, 3.5, 4, 5]
- **last_commit**: db6ab680 (Phase 5)
- **last_qa_passed_at**: 2026-05-10 07:30 (Phase 5)
- **next_phase_scheduled_at**: user key entry + main PR timing (manual trigger)

## Progression Rules (v2 — updated to OS-level scheduling)

Previous version: used Claude Code in-process `CronCreate` → unreliable because it required REPL idle + session alive. **Switched to OS-level after two consecutive missed triggers.**

New mechanism:
1. **Notification**: `scripts/genai_phase_check.sh` sends a desktop notification + stdout when 4 hours have elapsed since `last_qa_passed_at`. Duplicate prevention via `/tmp/genai_phase_check.lock`.
2. **User crontab (one line)**:
   ```
   */30 * * * * /home/user/work_p/Datapipeline-Data-data_pipeline_test/scripts/genai_phase_check.sh
   ```
   (Checks every 30 minutes. Notification fires only once per phase.)
3. **User trigger**: on receiving notification, open Claude Code session and say "start Phase N".
4. **New session**: reads this file and begins work for `current_phase`.

On phase completion:
- Add number to `completed_phases`
- Update `current_phase` to next number
- Update `last_commit` / `last_qa_passed_at` (the script uses these as the baseline for the next 4-hour countdown)
- `/tmp/genai_phase_check.lock` expires automatically (next phase notification becomes possible)

QA failure → do not update; await user intervention.

## Phase 0 — Pre-decisions (complete)

- API keys: leave blank and proceed with code (user fills in later)
- Higgsfield access path: fal.ai adopted
- Basic auth: simple user/pass env-based
- 4-hour interval policy: option A (any time, regardless of night)
- Rest meaning: load balancing / token conservation

## Phase 1 — Complete (2026-05-08)

- [x] Write Postgres migration `002_genai.sql` (Codex T1, idempotency passed)
- [x] `postgres_dedup.py` candidate SELECT with new 3 columns + `find_project_genai_pairs` added
- [x] `postgres_ingest_raw.py` conditional columns (has_source_type/genai_engine/label_policy)
- [x] `ops_register.py` manifest pass-through + fail-loud pre-validation
- [x] **New** `postgres_genai.py` mixin (added per Codex review recommendation) — defines write-side methods (insert_genai_batch/jobs_batch, update_*, recompute_batch_status, get/list/find_pending_*) that Phase 2/3 will depend on
- [x] PG smoke test: idempotency / GenAI pair JOIN / CHECK / FK CASCADE / mixin SQL simulation all passed
- [x] Syntax check passed

### Known Follow-ups (to be addressed when entering Phase 2/3)

- INGEST sensor itself not modified — Phase 3 orchestrator must comply with the fact that sensor_incoming.py only looks at `{manifest_dir}/pending/*.json`. Orchestrator must emit GenAI result manifests into the same queue directory.
- `find_project_genai_pairs` inner JOIN: text-to-* scenario (input_asset_id IS NULL) does not apply to the user-specified (N images + prompt) scenario → kept as-is.
- CHECK constraint enum naming: inline CHECK on `genai_batches` is unnamed. Adding a 5th engine later will be awkward to ALTER. Named constraint recommended in a follow-up migration.

## Phase 2 — Complete (2026-05-08, ~22:50 KST)

- [x] `postgres_dedup.py` candidate query: add source_type filter (R5: prevent GenAI rows from mixing into the regular path)
- [x] `build/assets.py` `_build_project`: add GenAI early-return branch + new `_build_project_genai`
- [x] Immediate application of 6 Codex review issues:
  - HIGH (2) `_genai_pairs_or_empty` bare except separated — catch AttributeError only, raise others (prevent silent dataset 'completed' regression)
  - HIGH (5) partial failure → build_status `failed`/`partial`/`completed` differentiated + `skipped_pairs` field in manifest
  - MED (1) folder_prefix invariant check (cross-batch contamination defense)
  - MED (3) output_media mismatch validation (batch.output_media vs raw_files.media_type)
  - MED (6) `dataset.config` JSON enriched with `batch_ids`/`engines`/`prompt_hashes`/`source_unit_name` (Phase 3 rebuild dependency)
  - LOW (4) prompt PII avoidance — default `prompt_hash` only in manifest; plaintext only when `GENAI_MANIFEST_INCLUDE_PROMPT=1`
  - manifest `schema_version: 1` added
- [x] PG smoke: source_type filter (cam_test → 1 row, genai_b1 → 0 row), GenAI pairs JOIN 1 row correct

### Follow-ups to Handle Before Entering Phase 3 (Codex remaining)

- LOW (7) **operator task**: check `raw_files.source_type` distribution in prod — any values other than `NULL`/`camera`/`nas_upload`?
  ```sql
  SELECT source_type, COUNT(*) FROM raw_files
  WHERE ingest_status='completed' GROUP BY source_type;
  ```
- (8) Decision before Phase 3: rebuild API (`rebuild_genai_dataset(batch_id|dataset_id)`) — current `_build_project_genai` creates a new dataset_id each call → accumulation in datasets table. Separate entry point needed if idempotent rebuild is required.
- Phase 1 follow-ups (CHECK constraint naming, sensor manifest queue location) remain valid.

## Phase 3 — Complete (2026-05-09, ~23:30 KST)

- [x] `docker/genai/` FastAPI container skeleton
  - `app.py` — POST /genai/batches, GET list/detail, basic auth + upload limits
  - `db/pg.py` — psycopg2 thin layer (1:1 mirror of PostgresGenAIMixin SQL)
  - `adapters/{base,kling,__init__}.py` — Protocol + Kling adapter (JWT, mocking mode)
  - `jobs/{submit,finalize}.py` — N-image fan-out + per-job NAS atomic write
  - `storage/{nas_writer,manifest}.py` — atomic write + manifest builder
  - `templates/{base,index,batches,batch_detail}.html` + `static/style.css`
  - `Dockerfile` + `requirements.txt` (fastapi, uvicorn, jinja2, psycopg2-binary, PyJWT, requests)
- [x] `defs/genai/{__init__,sensor}.py` — Dagster `genai_poll_sensor` (Postgres polling, default STOPPED)
  - Registered in sensors list of `definitions_production.py`
- [x] `defs/ingest/ops_register.py` — manifest items[seq] → record._genai_batch_id/_genai_seq_in_batch pass-through
- [x] `defs/ingest/ops_normalize.py` — `_link_genai_asset_if_any` helper + calls `update_genai_job_assets` after INSERT
- [x] `docker/docker-compose.yaml` — `genai` service added (`profiles: ["genai"]`, port 8088, NAS volume mount)
- [x] `docker/.env.test` — `KLING_*`, `FAL_KEY`, `OPENAI_API_KEY`, `GENAI_*` blank placeholders
- [x] OS-level scheduling — `scripts/genai_phase_check.sh` (confirmed working: notify-send + stdout)

### Phase 3 Mocking Mode Behavior

When API keys are not set, `KlingAdapter.is_mock=True` is auto-detected:
- `submit()` → returns `kling-mock-<uuid>` provider_job_id
- `poll()` → 'done' + mock URL after 2 seconds
- `download_result()` → mini mp4 placeholder bytes

→ End-to-end validation possible (UI submit → NAS atomic write → ingest sensor → build asset → vlm-dataset paired manifest) even before the user fills in keys.

### Follow-ups Before Entering Phase 4

- Codex review feedback (apply immediately on receipt)
- End-to-end dry-run: N=2 batch (mock) on top of staging Postgres + NAS + MinIO
- After user fills in `KLING_ACCESS_KEY/SECRET_KEY`, run verify command to confirm actual call

## Phase 3.5 — Complete (2026-05-10, ~06:50 KST)

Codex Q3 HIGH follow-up: switch sensor from importing adapter directly to going through HTTP API.

- `docker/genai/app.py` new internal API:
  - `GET /internal/jobs/pending?limit=N` — pending list for async engines (is_synchronous=False)
  - `POST /internal/jobs/{job_id}/poll` — calls adapter poll(); separates finalize into BackgroundTasks when 'done' (Codex HIGH #2 — avoids race between sensor timeout 60s and download timeout 300s)
  - Auth: `GENAI_INTERNAL_TOKEN` header (separate from basic auth)
- `defs/genai/sensor.py` rewritten: only calls internal API via requests, zero adapter imports
- `x-runtime-dagster-env` in compose adds `GENAI_INTERNAL_BASE/TOKEN/POLL_INTERVAL/BATCH`

## Phase 4 — Complete (2026-05-10, ~06:50 KST)

3 new adapters + 2-tab UI. All support mocking mode (placeholder results when keys not set).

- `adapters/nanobanana.py` — Vertex AI gemini-2.5-flash-image (synchronous). google-genai SDK. Falls back to mocking when Vertex SA not present. Safety block / text-only response fallback explicitly handled.
- `adapters/gpt_image.py` — OpenAI gpt-image-1 (synchronous). openai SDK `images.edit`. Falls back to mocking when `OPENAI_API_KEY` not set.
- `adapters/higgsfield.py` — via fal.ai (asynchronous). fal-client SDK. Falls back to mocking when `FAL_KEY` not set. ⚠ Operator: verify real model ID `fal-ai/higgsfield/i2v-soul` (Codex HIGH #1).
- `adapters/__init__.py` — 4 engines registered + `ENGINE_TAB` mapping + `engines_by_tab()` helper.
- `jobs/submit.py` — sync engine path activated (NotImplementedError → `finalize_sync_results` bulk call).
- `jobs/finalize.py` — new `finalize_sync_results` (in-memory bytes → NAS atomic write + manifest).
- `templates/index.html` — 2-tab UI (Image→Video / Image→Image), JS tab switching.
- `static/style.css` — `.tab-bar/.tab-btn/.tab-panel` styles.
- `requirements.txt` — google-genai, openai, fal-client added.
- `docker-compose.yaml` — `./app/credentials:/app/credentials:ro` mount, GEMINI env vars, GENAI_INTERNAL_TOKEN added.
- `.env.test{,.example}` — `GENAI_INTERNAL_TOKEN`, 4-engine default `GENAI_ENGINES_ENABLED`, GEMINI_PROJECT/LOCATION, HIGGSFIELD_FAL_MODEL placeholder.

### Codex Review Applied

- HIGH #1 (Higgsfield model ID verification) — env-driven injection; code comment says "check fal.ai dashboard". Actual verification is an operator task.
- HIGH #2 (finalize blocking) — BackgroundTasks separation makes it safe regardless of sensor timeout.
- MED #4 (dynamic pending SQL) — `_async_engines()` helper filters adapters with `is_synchronous=False`.
- MED #5 (nanobanana fallback) — block_reason / finish_reason / text_excerpt explicitly exposed.

### Phase 4 Mocking Mode Behavior (all engines)

- Kling/Higgsfield: submit → mock provider_id; poll returns 'done' + placeholder MP4 after 2 seconds
- Nanobanana/GPT Image: placeholder PNG returned at submit time (synchronous); poll not called

→ End-to-end validation of all 4 engines possible even before the user fills in keys.

## Phase 5 — Complete (2026-05-10, ~07:30 KST)

Production hardening: quota / rate-limit / cost monitoring / partial failure retry / operator guide.

- `docker/genai/limits.py` new: `check_rate_limit` (sliding 60s in-memory) + `check_daily_quota` (PG aggregation, options_json::jsonb extraction). 0 = off.
- `docker/genai/db/aggregates.py` new: `cost_summary(range)` — done/failed/cost_units aggregation by engine.
- `app.py`:
  - POST `/genai/batches`: rate-limit + daily quota check (429 response)
  - GET `/genai/costs?range=day|week|month` + `templates/costs.html` (range tabs, totals, by_engine, limits status)
  - POST `/genai/jobs/{job_id}/retry`: retry a single failed job. **Atomic CAS** (`UPDATE ... WHERE status='failed' RETURNING`) blocks race conditions (Codex HIGH #3). Reverts status on external API failure.
- `jobs/submit.py`: auto-inject `input_total_bytes` into options_json (for quota aggregation).
- `jobs/finalize.py` `_maybe_write_outputs_manifest`: **PG advisory lock** (`pg_try_advisory_xact_lock(hashtext('genai_manifest:'+batch_id))`) serializes multiple writers for manifest race prevention (Codex HIGH #8).
- `templates/`: base.html nav adds Costs; batch_detail.html adds retry button on failed jobs (HTMX).
- `static/style.css`: range-tabs, dl grid styles.
- `docker-compose.yaml` + `.env.test{,.example}`: `GENAI_RATE_LIMIT_PER_MIN/DAILY_BATCH_LIMIT/DAILY_BYTES_LIMIT` placeholder (default 0).
- **New** `docs/genai_rollout/operator_guide.md` — main promotion checklist, end-to-end validation, troubleshooting, prohibited actions (CI rsync conflict with host manual edits).

### Codex Review Applied (HIGH 2 + MED 4)

| Level | Issue | Fix |
|-------|-------|-----|
| HIGH #3 | retry race — duplicate external API calls | `UPDATE ... WHERE status='failed' RETURNING` atomic CAS; returns 409 on empty RETURNING |
| HIGH #8 | manifest multiple writer race | `pg_try_advisory_xact_lock` serializes single writer |
| MED #2 | options_json regex parsing vulnerability | `options_json::jsonb ->> 'input_total_bytes'` safe casting, invalid JSON protection |
| MED #1 | rate-limit multi-container distribution | single-container policy documented in operator_guide + redis/PG migration recommended when scaling workers |
| MED #7 | operator_guide prod edit risk | Added CLAUDE.md prohibited actions (host manual edits → CI loss) at end of guide |
| MED #4 | retry sync engine + ingest dedup | Currently deduped by raw_files.checksum UNIQUE. Consider `batch_id+seq+kind` composite UNIQUE later (Phase 6 follow-up) |
| LOW #5/#6 | anonymous quota / cost OR index | Current volume accommodated. When data grows: split source IP + separate `submitted_at > since` simple filter |

### Handed off to Phase 6 (production)

- Operator key entry + end-to-end real call validation for all 4 engines
- Higgsfield model ID (`fal-ai/higgsfield/i2v-soul`) verification on fal.ai dashboard
- 24-hour staging stabilization → main PR
