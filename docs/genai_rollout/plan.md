# GenAI Studio Integration Plan (final · v3)

**Target environment**: staging (dev branch) — `/home/user/work_p/Datapipeline-Data-data_pipeline_test`
**DB policy**: Postgres single source of truth (DuckDB for read-only queries via `postgres_scanner` extension only)
**Written**: 2026-05-08 / Opus + Codex consensus (v3 — fan-out batch + paired dataset)

---

## 1. Core Logic

```
User submits once
  · N images uploaded
  · 1 prompt
  · 1 engine
        │
        ▼
  ┌─────────── 1 batch_id ───────────┐
  │  job_1 → image[1] → result[1]   │
  │  job_2 → image[2] → result[2]   │
  │   …                              │
  │  job_N → image[N] → result[N]   │
  └──────────────────────────────────┘

All N originals + N generated outputs recorded in Postgres + loaded as paired entries in dataset
```

- **Fan-out unit**: 1 batch = N jobs (1:N)
- **Paired preservation**: each job's (input image, output {video|image}) is tracked as a pair within the dataset
- **Output type**: determined automatically per engine — Kling/Higgsfield = MP4, Nanobanana/GPT Image = PNG

## 2. UI — 2-Tab Structure

```
┌────────────────────────────────────────────────────────┐
│  GenAI Studio                                          │
│  ┌──────────────┬──────────────┐                       │
│  │ Image→Video  │ Image→Image  │                       │
│  └──────┬───────┴──────────────┘                       │
│         │                                              │
│  Engine ▼ ◯ Kling  ◯ Higgsfield                        │
│  Image drop zone [upload N images]                     │
│  Prompt [______________________________]               │
│  Options (resolution/duration, per-engine)             │
│  [Submit] → batch_id issued                            │
│                                                        │
│  Batch list / status ── pending|running|succeeded|     │
│                         partial_success|failed         │
│  └ Per-job progress (N rows, seq_in_batch, status)     │
└────────────────────────────────────────────────────────┘

Tab "Image→Video":  Kling, Higgsfield      → output MP4
Tab "Image→Image":  Nanobanana, GPT Image  → output PNG
```

## 3. 4-Engine Matrix

| Engine | Input | Output | Auth | Sync/Async | New secrets |
|--------|-------|--------|------|------------|-------------|
| **Kling** | image + prompt | MP4 | JWT (AK+SK) | async (submit→poll) | `KLING_ACCESS_KEY`, `KLING_SECRET_KEY` |
| **Higgsfield** | image + prompt | MP4 | fal.ai or replicate | async | `FAL_KEY` or `REPLICATE_API_TOKEN` |
| **Nanobanana** (Gemini 2.5 Flash Image) | image + prompt | PNG/JPG | Vertex SA | sync | **0** — reuse existing `your-gcp-project` |
| **GPT Image** (`gpt-image-1`) | image (optional) + prompt | PNG | OpenAI API key | sync | `OPENAI_API_KEY` |

> All 4 engines unified: input = **images + prompt**. Video input option removed.
> Reference-image scenario: consolidated to Nanobanana or GPT Image. (Imagen line not adopted — avoiding 2026-06-30 discontinuation)

## 4. Data Flow (full)

```
   Internal network (10.0.0.10)
   ┌──────────────────────────────────────────────────────────────┐
   │  GenAI Studio UI    POST /genai/batches  (multipart, N files)│
   │           │                                                  │
   │           ▼  (1) Postgres TXN                                │
   │   ┌──────────────────────────────────────┐                   │
   │   │ INSERT genai_batches (batch_id, ...) │                   │
   │   │ INSERT genai_jobs × N                │                   │
   │   └──────────────────────────────────────┘                   │
   │           │                                                  │
   │           ▼  (2) Atomic write of N originals to NAS          │
   │   /nas/staging/incoming/genai/<batch_id>/originals/          │
   │     · 001.png  002.png  …  N.png                             │
   │     · _manifest.json   {kind:"originals", batch_id, jobs:[]} │
   │           │                                                  │
   │           ▼  (3) Adapter call                                │
   │   ┌────────────────────────────────────────┐                 │
   │   │ for seq in 1..N:                       │                 │
   │   │   adapter.submit(image[seq], prompt)   │                 │
   │   │   → save genai_jobs.provider_job_id    │                 │
   │   └────────────────────────────────────────┘                 │
   └────────────────┬─────────────────────────────────────────────┘
                    │
                    │ (4) Dagster sensor polling (Postgres)
                    │     Only async engines (Kling, Higgs) are polled.
                    │     Sync engines (Nano, GPT) get result at submit time.
                    ▼
   ┌──────────────────────────────────────────────────────────────┐
   │  External APIs:  Kling · fal.ai · Vertex · OpenAI            │
   │  Download result (CDN/blob URL → bytes, backend directly)    │
   └──────┬───────────────────────────────────────────────────────┘
          │ (5) Atomic write of N results to NAS
          ▼
   /nas/staging/incoming/genai/<batch_id>/outputs/
     · 001.{mp4|png}  002.{mp4|png}  …
     · _manifest.json  {kind:"outputs", batch_id,
                        items:[{seq, provider_job_id, ...}]}
                       │
                       ▼  [existing NAS sensor auto-picks up — both originals and results]
   ┌──────────────────────────────────────────────────────────────┐
   │  INGEST sensor (~25 line modification)                       │
   │   Branch by kind in _manifest.json:                          │
   │                                                              │
   │   originals/ → raw_files INSERT:                             │
   │     source_type   = 'genai_source'           ★new column     │
   │     genai_engine  = batch.engine             ★new column     │
   │     label_policy  = 'none'                   ★new column     │
   │     media_type    = 'image'                                  │
   │     source_unit_name = 'genai_<batch_id>'                    │
   │   → UPDATE genai_jobs SET input_asset_id = asset_id          │
   │                       WHERE batch_id=? AND seq=?             │
   │                                                              │
   │   outputs/ → raw_files INSERT:                               │
   │     source_type   = 'genai_output'                           │
   │     genai_engine  = batch.engine                             │
   │     label_policy  = 'none'                                   │
   │     media_type    = 'video' | 'image' (from extension)       │
   │     source_unit_name = 'genai_<batch_id>'                    │
   │   → UPDATE genai_jobs SET output_asset_id = asset_id,        │
   │                       status = 'done'                        │
   │                       WHERE batch_id=? AND seq=?             │
   │                                                              │
   │   When all batch rows are filled:                            │
   │     UPDATE genai_batches SET status = derived_status         │
   │           (succeeded|partial_success|failed)                 │
   └────────────────┬─────────────────────────────────────────────┘
                    ▼
   ┌──────────────────────────────────────────────────────────────┐
   │  build/assets.py _build_project (2-way CASE branch)          │
   │                                                              │
   │  CASE 1 — regular (camera|nas_upload, label_policy='required'):│
   │     existing logic unchanged                                 │
   │                                                              │
   │  CASE 2 — GenAI (source_type IN                              │
   │           ('genai_source','genai_output'),                   │
   │           label_policy='none'):                              │
   │     SELECT j.batch_id, j.seq_in_batch,                       │
   │            src.raw_key, src.media_type,                      │
   │            out.raw_key, out.media_type,                      │
   │            b.engine, b.prompt                                │
   │       FROM genai_jobs j                                      │
   │       JOIN raw_files src ON src.asset_id=j.input_asset_id    │
   │       JOIN raw_files out ON out.asset_id=j.output_asset_id   │
   │       JOIN genai_batches b ON b.batch_id=j.batch_id          │
   │      WHERE src.source_unit_name = <folder>                   │
   │        AND j.status = 'done'                                 │
   │                                                              │
   │     for each pair:                                           │
   │       copy src → vlm-dataset/<folder>/images/<seq>.png       │
   │       copy out → vlm-dataset/<folder>/                       │
   │              {videos|generated_images}/<seq>.{mp4|png}       │
   │     manifest entry per pair:                                 │
   │       {pair_id, batch_id, seq, source_key, generated_key,    │
   │        engine, prompt, label_free:true,                      │
   │        provider_job_id}                                      │
   └──────────────────────────────────────────────────────────────┘
```

## 5. DB Schema Changes — Postgres Only

**Target**: `src/vlm_pipeline/sql/migrations/postgres/002_genai.sql` (forward-only, idempotent, applied in Phase 1)

```sql
BEGIN;

-- raw_files enhancements
ALTER TABLE raw_files ADD COLUMN IF NOT EXISTS source_type   TEXT DEFAULT 'camera';
ALTER TABLE raw_files ADD COLUMN IF NOT EXISTS genai_engine  TEXT;
ALTER TABLE raw_files ADD COLUMN IF NOT EXISTS label_policy  TEXT DEFAULT 'required';
-- + CHECK constraints (PG 15 compatible DO $$ pattern, idempotent)
-- + indexes (idx_raw_files_source_type, idx_raw_files_label_policy, idx_raw_files_genai_engine)

-- batch (one submit unit)
CREATE TABLE IF NOT EXISTS genai_batches (
    batch_id      TEXT PRIMARY KEY,
    engine        TEXT NOT NULL CHECK (engine IN ('kling','higgsfield','nanobanana','gpt_image')),
    output_media  TEXT NOT NULL CHECK (output_media IN ('video','image')),
    prompt        TEXT NOT NULL,
    options_json  TEXT,
    requested_by  TEXT,
    status        TEXT DEFAULT 'pending'
                    CHECK (status IN ('pending','running','succeeded','partial_success','failed','cancelled')),
    n_total       INTEGER NOT NULL CHECK (n_total >= 1),
    n_succeeded   INTEGER DEFAULT 0,
    n_failed      INTEGER DEFAULT 0,
    submitted_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at  TIMESTAMP
);

-- individual job (1 image → 1 result)
CREATE TABLE IF NOT EXISTS genai_jobs (
    job_id            TEXT PRIMARY KEY,
    batch_id          TEXT NOT NULL REFERENCES genai_batches(batch_id) ON DELETE CASCADE,
    seq_in_batch      INTEGER NOT NULL CHECK (seq_in_batch >= 1),
    input_asset_id    TEXT REFERENCES raw_files(asset_id),
    output_asset_id   TEXT REFERENCES raw_files(asset_id),
    provider_job_id   TEXT,
    status            TEXT DEFAULT 'pending'
                        CHECK (status IN ('pending','submitted','running','done','failed')),
    error_message     TEXT,
    cost_units        DOUBLE PRECISION,
    submitted_at      TIMESTAMP,
    completed_at      TIMESTAMP,
    UNIQUE(batch_id, seq_in_batch)
);

-- indexes: idx_genai_jobs_batch, idx_genai_jobs_status, idx_genai_batches_status
COMMIT;
```

> Separate `genai_pairs` table **not needed** — 1 job = 1 input + 1 output, so 2 FKs in `genai_jobs` are sufficient.

## 6. Directory Tree

### 6-1. Code (repo)

```
docker/
  genai/                                     ★ new container (FastAPI + Jinja2 + HTMX)
    Dockerfile
    requirements.txt
    app.py                  entry point, routes
    templates/
      base.html
      index.html            (2 tabs + drop zone + prompt)
      batches.html          (batch list + status)
      batch_detail.html     (N jobs in batch + pair preview)
    static/
      htmx.min.js
      style.css
    adapters/
      __init__.py
      base.py               BaseGenAIAdapter Protocol
                            (submit / poll / download_result)
      kling.py              JWT (AK+SK) signing + async
      higgsfield.py         fal.ai or replicate
      nanobanana.py         Vertex (gemini-2.5-flash-image), sync
      gpt_image.py          OpenAI (gpt-image-1), sync
    jobs/
      submit.py             POST /genai/batches handler
                            (Postgres TXN + NAS originals + adapter call)
      finalize.py           NAS result settlement + manifest write
    storage/
      nas_writer.py         atomic write (.partial → rename)
      manifest.py           _manifest.json builder
    db/
      pg.py                 Postgres connection (reuse PIPELINE_DB_DSN)

src/vlm_pipeline/
  defs/
    ingest/sensor.py        (modification ~25 lines: _manifest.json originals/outputs branch)
    build/assets.py         (modification ~50 lines: CASE branch regular vs genai pairs)
    genai/                  ★ new Dagster module
      __init__.py
      sensor.py             genai_poll_sensor (Kling/Higgs polling)
      ops.py                job status → genai_jobs/genai_batches UPDATE
  resources/
    postgres_dedup.py       (modified: candidate SELECT + find_project_genai_pairs)  [Phase 1 applied]
    postgres_genai.py       ★ new mixin (all write-side methods)                     [Phase 1 applied]
    postgres.py             (modified: register PostgresGenAIMixin)                  [Phase 1 applied]
  sql/migrations/postgres/
    002_genai.sql           ★ new migration                                          [Phase 1 applied]
tests/
  unit/genai/               ★ new
    test_kling_adapter.py
    test_submit_endpoint.py
    test_build_genai_pairs.py
  integration/test_genai_e2e.py  ★ new
```

### 6-2. NAS Runtime (staging)

```
/home/user/mou/staging/                         (host)
└── incoming/                                  → /nas/staging/incoming in container
    │
    ├── <existing NAS source folders>          [existing — no impact]
    │
    └── genai/                                 ★ GenAI dedicated root
        │
        ├── <batch_id_1>/                      (e.g., kling_b2f4-1a3c-...)
        │   ├── originals/                     ── N originals uploaded by user
        │   │   ├── 001.png
        │   │   ├── 002.png
        │   │   ├── 003.png
        │   │   └── _manifest.json             {kind: "originals", batch_id, ...}
        │   │
        │   ├── outputs/                       ── N results downloaded from external API
        │   │   ├── 001.mp4
        │   │   ├── 002.mp4
        │   │   ├── 003.mp4
        │   │   └── _manifest.json             {kind: "outputs", batch_id, ...}
        │   │
        │   └── failed/                        (optional) JSONL failure log
        │       └── jobs.jsonl
        │
        ├── <batch_id_2>/                      (different batch — independent)
        │   ├── originals/
        │   ├── outputs/
        │   └── …

/home/user/mou/staging/archive/                 (NAS sensor moves here after processing)
└── genai/
    └── <batch_id_1>/{originals,outputs}/
```

**File settlement order (within one batch)**:

```
T+0   /genai/<batch>/originals/001.png.partial    ← backend starts writing
T+1   /genai/<batch>/originals/001.png            ← atomic rename
T+2   … 002.png, 003.png same
T+3   /genai/<batch>/originals/_manifest.json     ← written last (sensor pickup trigger)
T+10s ↓ NAS sensor detects originals → raw_files INSERT
T+30s ↓ backend starts downloading external API results
T+5m  /genai/<batch>/outputs/001.mp4.partial
T+5m  /genai/<batch>/outputs/001.mp4
T+15m … 002.mp4, 003.mp4
T+15m /genai/<batch>/outputs/_manifest.json      ← outputs settlement signal
T+15m ↓ NAS sensor detects outputs → raw_files INSERT + genai_jobs FK update
```

### 6-3. MinIO Persistence (5 fixed buckets)

```
MinIO (staging :9002)
│
├── vlm-raw/                                   ── uploaded by INGEST sensor
│   └── genai_<batch_id>/                      (source_unit_name = sanitized folder)
│       ├── originals/{001.png, 002.png, 003.png}
│       └── outputs/{001.mp4, 002.mp4, 003.mp4}
│
├── vlm-labels/                                [existing — GenAI data does not enter; label_policy='none']
├── vlm-processed/                             [existing — no impact]
│
├── vlm-dataset/                               ── loaded by build asset
│   └── genai_<batch_id>/                      (project / source_unit_name)
│       ├── images/                            ── N original images
│       │   └── {001.png, 002.png, 003.png}
│       ├── videos/                            ── Image→Video results (Kling/Higgs)
│       │   └── {001.mp4, 002.mp4, 003.mp4}
│       ├── generated_images/                  ── Image→Image results (Nano/GPT)
│       └── manifest.json                      {project, pairs:[…], counts:{pairs, images, videos, generated_images}}
│
└── vlm-classification/                        [existing — no impact]
```

### 6-4. Postgres — Row-Level Relationships

```
genai_batches  (batch_id PK)
   │ 1
   │
   │ N
genai_jobs     (job_id PK, batch_id FK ON DELETE CASCADE)
   │            ─┬─ input_asset_id  ──→ raw_files.asset_id (source_type='genai_source')
   │             └─ output_asset_id ──→ raw_files.asset_id (source_type='genai_output')
   │
raw_files
   ├ source_type   = 'genai_source' | 'genai_output' | 'camera' | 'nas_upload'
   ├ genai_engine  = 'kling' | 'higgsfield' | 'nanobanana' | 'gpt_image' | NULL
   ├ label_policy  = 'none' | 'required'
   └ source_unit_name = 'genai_<batch_id>'    ← folder key for build asset
```

## 7. Result Storage = Automated Logic (no user download)

```
[A] Automated storage  ← adopted in v3
┌─────────────┐    ┌──────────────┐   server-to-server  ┌──────────┐
│ User        │    │ FastAPI      │      HTTP           │ Kling/   │
│ browser     │───▶│ backend      │────────────────────▶│ Higgs/   │
│             │    │ (genai       │                     │ Vertex/  │
│ click Submit│    │  container)  │                     │ OpenAI   │
└─────────────┘    └──────┬───────┘                     └────┬─────┘
                          │                                  │
                          │  ◀ download result bytes ───────┘
                          │     (backend directly)
                          ▼
                   /nas/staging/incoming/genai/<batch>/outputs/
                          │
                          ▼  [NAS sensor pickup]
                   raw_files + vlm-raw + vlm-dataset
                          │
                          ▼
                   user browser shows "done" status only
                   (preview link if needed = MinIO presigned URL)
```

- Nothing is saved to the user's PC disk
- Sync engines (Nano/GPT): submit response contains base64 PNG → decoded and saved to NAS
- Async engines (Kling/Higgs): submit → poll → CDN URL in completion response → download bytes → save to NAS
- NAS path: write as `.partial`, then atomic rename; `_manifest.json` written last

## 8. Existing Module Modifications

| File | Change | Phase |
|------|--------|-------|
| `sql/migrations/postgres/002_genai.sql` (new) | raw_files enhancements + genai_batches + genai_jobs | 1 ✓ |
| `resources/postgres_genai.py` (new mixin) | all write-side methods | 1 ✓ |
| `resources/postgres_dedup.py` | candidate SELECT + `find_project_genai_pairs` | 1 ✓ |
| `resources/postgres_ingest_raw.py` | conditional new columns | 1 ✓ |
| `resources/postgres.py` | register mixin | 1 ✓ |
| `defs/ingest/ops_register.py` | manifest pass-through + fail-loud | 1 ✓ |
| `defs/build/assets.py` `_build_project` | CASE branch (regular vs genai) | 2 |
| `defs/ingest/sensor.py` | recognize `_manifest.json` originals/outputs | 3 |
| `defs/genai/{sensor,ops}.py` (new) | Dagster polling sensor | 3 |
| `docker/genai/` (new container) | FastAPI UI + 4 adapters | 3-4 |
| `docker/docker-compose.yaml` | add genai service | 3 |
| `docker/.env.test` (not git-tracked) | 4 secrets + GENAI_* | 3 |

> `resources/duckdb_dedup.py`, `resources/duckdb_migration.py` **not modified** — DuckDB auto-reflects via `postgres_scanner`.

## 9. Security / Operations

- **Internal network (10.0.0.10) only** — restrict genai service port binding to host LAN IP in compose
- **Basic Auth** — FastAPI Depends or nginx sidecar; credentials in `.env.test`
- **Upload limits** — file ≤ 50MB each, ≤ 20 files per batch, extension whitelist (png/jpg/webp), daily quota
- **Provenance** — `genai_batches.prompt` + `genai_jobs.provider_job_id` persisted; echoed in dataset manifest
- **Partial failure policy** — if some of N fail: batch=`partial_success`; only successful pairs enter dataset. Failed jobs retryable from UI

## 10. Phased Rollout

### Phase 0 — Pre-decisions (code = 0)
- [x] API keys left blank; proceed with code (user fills in later)
- [x] Higgsfield access path: fal.ai adopted
- [x] basic auth: simple user/pass env-based
- [x] 4-hour interval policy: option A (any time, regardless of night)
- [x] Rest meaning: load balancing / token conservation

### Phase 1 — Postgres Migration + Ingest Skeleton (complete, commit 51433041)
- [x] Write `002_genai.sql` (Codex T1 pattern)
- [x] `postgres_dedup.py` candidate SELECT + new `find_project_genai_pairs`
- [x] `postgres_ingest_raw.py` conditional columns
- [x] `postgres_genai.py` mixin (all methods that Phase 2/3 depend on)
- [x] `ops_register.py` manifest pass-through + fail-loud pre-validation
- [x] PG smoke validation (idempotent / GenAI pair JOIN / CHECK / FK CASCADE)

### Phase 2 — Build Asset CASE Branch
- [ ] `case_genai_pairs()` branch in `_build_project`
- [ ] Finalize dataset manifest entry schema
- [ ] Verify split_dataset downstream compatibility

### Phase 3 — First Adapter (Kling) + FastAPI Container Skeleton
- [ ] `docker/genai/` (FastAPI + Jinja2 + HTMX, starting with 1 tab 1 engine)
- [ ] `BaseGenAIAdapter` Protocol
- [ ] **Kling adapter** + Image→Video tab only
- [ ] Dagster `genai_poll_sensor`
- [ ] End-to-end: UI submit (3 images) → 1 batch + 3 jobs → adapter → NAS → ingest → build → verify 3 pairs in vlm-dataset

### Phase 4 — Remaining 3 Engines
- [ ] Nanobanana (reuse Vertex SA → fast) — Image→Image tab
- [ ] GPT Image (OpenAI key) — Image→Image tab
- [ ] Higgsfield (fal.ai or replicate) — Image→Video tab
- [ ] 2-tab UI complete

### Phase 5 — Production Hardening
- [ ] quota / rate-limit
- [ ] cost monitoring (`genai_batches.cost_units` aggregation)
- [ ] partial failure retry UI
- [ ] staging stabilization → main PR

## 11. Risks & Mitigations

| # | Risk | Mitigation |
|---|------|------------|
| R1 | NAS partial-write race | `.partial → rename` atomic + `_manifest.json` written last |
| R2 | Higgsfield direct API not guaranteed | fal.ai or replicate routing confirmed; absorbed by adapter |
| R3 | External API cost runaway | Upload size/count limits + daily quota + `cost_units` tracking |
| R4 | Kling async polling long duration (>30 min) | sensor timeout + `genai_jobs.status='failed'` + UI retry |
| R5 | GenAI data mixed into regular dataset build | `source_type` filter for explicit separation (CASE branch, Phase 2) |
| R6 | Dataset consistency on partial failure | Only successful pairs included in manifest |
| R7 | DuckDB read cannot see Postgres new columns/tables | `postgres_scanner` auto-reflects; verify with SELECT at end of Phase 1 |
| R8 | Same original image reused as input for multiple batches | checksum dedup auto-creates 1 row; `genai_jobs.input_asset_id` supports multiple references |
| R9 | API keys not set (Phase 3+ end-to-end not possible) | mocking/skip mode; user runs verify command once after entering keys |

## 12. Validation Checklist (staging)

- [x] Postgres migration idempotent re-apply OK (Phase 1)
- [ ] DuckDB `postgres_scanner` can SELECT `genai_batches`/`genai_jobs`
- [ ] N=3 batch end-to-end: 3 originals + 3 results all correctly linked in raw_files + genai_jobs
- [ ] Partial failure (1 of N=3 force-failed) → batch=`partial_success`, only 2 successful pairs enter dataset
- [ ] 1 end-to-end success per engine across all 4 tabs
- [ ] Dataset manifest has per-pair entry with `engine`/`prompt`/`provider_job_id` echoed
- [ ] Basic auth blocked
- [ ] 422 on quota exceeded
- [ ] No Postgres conflicts between sensor polling and other Dagster jobs
- [ ] split_dataset downstream compatibility

---

## Appendix: v1 → v3 Evolution Summary

| Item | v1 | v2 | v3 |
|------|----|----|----|
| DB migration | Both DuckDB + Postgres | Postgres only | Postgres only |
| Submit unit | 1 job per submit | 1 job per submit | **1 batch = N jobs** (fan-out) |
| Input mode | Varied per engine | Varied per engine | **N images + 1 prompt unified** |
| Original storage | UI temporary | UI temporary | **full ingest → raw_files (for training)** |
| Pair tracking | None | None | **genai_jobs 2 FKs** |
| UI | 1 screen radio | 1 screen radio | **2 tabs (Image→Video / Image→Image)** |
| New columns | `transfer_tool='kling'` reused | `genai_engine` + `label_policy` | **+ `source_type`** |
| New tables | 0 | `genai_jobs` | **`genai_batches` + `genai_jobs`** |
| Build branch | label-free single | label-free single | **CASE: regular vs genai pairs** |
| Sensor concurrency | `duckdb_writer` tag serialization | Postgres native | Postgres native |
| Imagen | included | Imagen 4 + Imagen 3 fallback | **removed** (2026-06-30 discontinuation) |
