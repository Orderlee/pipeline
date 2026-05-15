# CLAUDE.md — VLM Data Pipeline

> What can be inferred from reading the code is omitted. Only rules, environment details, and operational context that cannot be determined from code alone are recorded here.

---

## Project Summary

A **Dagster-based media data pipeline** that ingests CCTV/security footage → deduplicates → labels with Gemini → runs YOLO detection → builds training datasets.

---

## 🤖 AI Agent Core Action Rules

- **Skill Discovery First:** When given a task, always search the `.agent/skill/` directory via the system tools before writing any code from scratch.
- If a relevant skill document is found, read the instructions in that `SKILL.md` fully and follow its rules exactly.
- **Agent Team Routing:** For non-trivial tasks, check the routing table in [`docs/references/agent-teams.md`](docs/references/agent-teams.md) before handling it directly. This project defines 4 sub-agents (`dagster-impl`, `codex`, `pipeline-explorer`, `deploy-auditor`) by task type, and the multi-agent operating policy is in [`docs/references/multi-agent.md`](docs/references/multi-agent.md).

---

## Build & Run

```bash
# Install dependencies (editable)
pip install -e ".[dev]"

# Local tests
pytest tests/unit -q
pytest tests/integration -q

# Docker (production — main branch)
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker && docker compose up -d

# Docker (staging — dev branch)
cd /home/user/work_p/Datapipeline-Data-data_pipeline_test/docker && docker compose up -d

# Dagster UI
#   production : http://10.0.0.10:3030  (main)
#   staging    : http://10.0.0.10:3031  (dev)

# DuckDB query (directly from host)
python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files;"
```

---

## Dual Environment (Production vs Staging)

The two environments are fully isolated as **independent git clones + independent docker compose stacks**.

| Item | Production | Staging |
|------|-----------|---------|
| Dagster UI | `http://10.0.0.10:3030` | `http://10.0.0.10:3031` |
| Git repo (host) | `/home/user/work_p/Datapipeline-Data-data_pipeline` | `/home/user/work_p/Datapipeline-Data-data_pipeline_test` |
| Git branch | **`main`** (stable) | **`dev`** (validation) |
| Compose project | `docker` | `pipeline-test` |
| Container name prefix | `docker-dagster-*` | `pipeline-test-dagster-*` |
| DuckDB (container) | `/data/pipeline.duckdb` | `/data/staging.duckdb` |
| MinIO endpoint | `http://10.0.0.36:9000` | `http://10.0.0.36:9002` |
| MinIO Console | `:9001` | `:9003` |
| Incoming (host) | `/home/user/mou/incoming` | `/home/user/mou/staging/incoming` |
| Archive (host) | `/home/user/mou/archive` | `/home/user/mou/staging/archive` |
| DAGSTER_HOME (container) | `/app/dagster_home` | `/app/dagster_home` (same; only the host path differs) |
| env file | `docker/.env` | `docker/.env.test` |
| dispatch-agent integration | `host.docker.internal:8080` | `host.docker.internal:8081` |

Both repos maintain independent `.git` directories; branch-based deployment is handled automatically by CI/CD (see next section).

---

## Branch Strategy & Deployment (CI/CD)

### Branch Roles

- **`dev`** — tracked by staging (3031). Entry point for new features, experiments, and refactoring.
- **`main`** — tracked by production (3030). Merges only after sufficient validation on `dev`.

### Automated Deployment (GitHub Actions, self-hosted runner)

| Workflow | Trigger | Deploy Target | Runner Label |
|----------|---------|---------------|--------------|
| [`deploy-test.yml`](.github/workflows/deploy-test.yml) | `push` → `dev` | staging repo | `self-hosted, linux, test` |
| [`deploy-production.yml`](.github/workflows/deploy-production.yml) | `push` → `main` | production repo | `self-hosted, linux, production` |

Both workflows run via [`scripts/deploy/deploy-stack.sh`](scripts/deploy/deploy-stack.sh). Key steps:

1. **test job** — `pytest tests/unit` (can be bypassed with `skip_tests=true` on workflow_dispatch; emergency use only)
2. **detect_image_rebuild job** — rebuilds the image only when changes are detected in `Dockerfile` / `docker/app/` / `configs/` / `scripts/` / `gcp/` / `split_dataset/` / `src/python/`
3. **deploy job** — aligns host code to the deployed SHA:
   - **(a) rsync** `-a --delete` workspace → DEPLOY_ROOT sync (`src/`, `configs/`, `gcp/`, `scripts/`, `split_dataset/` + parts of `docker/app/` + compose/Dockerfile). `dagster_home/`, `dagster_home_staging/`, `credentials/`, `docker/data/` are excluded from rsync — runtime state is preserved.
   - **(b) git hard-reset** `git -C ${DEPLOY_REPO_ROOT} fetch origin && reset --hard ${GITHUB_SHA}` — aligns the host git tree (`.git/HEAD`, `git log`, `git status`) to the exact deployed commit. **rsync updates src files but does not touch `.git`**, so without this step the host's `git log` would appear stale forever. Only tracked files are reset; untracked items like `dagster_home/` remain intact.
4. Restore env files (`.env` / `.env.test` — host-stored copies are preserved)
5. `docker compose up -d` stack restart + HEALTHCHECK_URL response verification (prod `:3030/server_info`, staging `:3031/server_info`)
6. AI deploy analysis (Claude CLI, best-effort; failure here does not fail the deployment)

> ✅ **Single Source of Truth Principle**: After deployment, `host git HEAD == src inside container image == running code` always matches. Host src is not mounted into the container (only the result of `COPY src/` at image build time is used), so editing host src has no effect on container behavior — if an immediate change is needed, run `docker compose build` followed by a restart.

> ⚠️ **Fork distinction**: Both workflows have the condition `if: github.repository == 'Orderlee/Datapipeline-Data-data_pipeline'` — CI is not triggered when a PR targets `upstream` (upstream-org).

### Recommended Deployment Flow

1. Branch `feature/*` from `dev`
2. PR → merge to `dev` → **automated staging deployment** (3–10 min)
3. End-to-end validation on staging (3031): sensor tick, dispatch run, MinIO artifacts
4. PR `dev` → `main` → merge → **automated production deployment**

### Hotfix (Emergency Production Fix)

1. Branch `fix/*` from `main`
2. PR → merge to `main` → immediate production deployment
3. After completion, back-merge `main` → `dev` to prevent drift

### Manual Deployment / CI Bypass

- GitHub Actions UI → `Run workflow` button on the relevant workflow (`skip_tests` option available)
- If CI is unavailable, run directly on host:

```bash
# PROD
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git pull origin main --ff-only
cd docker && docker compose restart dagster dagster-daemon dagster-code-server

# STAGING
cd /home/user/work_p/Datapipeline-Data-data_pipeline_test
git pull origin dev --ff-only
cd docker && docker compose restart
```

### Drift Detection

```bash
# Byte-level comparison of src/ between the two repos (differences at dev ≠ main points are expected)
diff -rq /home/user/work_p/Datapipeline-Data-data_pipeline/src \
         /home/user/work_p/Datapipeline-Data-data_pipeline_test/src

# Check whether each repo is in sync with its branch HEAD
git -C /home/user/work_p/Datapipeline-Data-data_pipeline status            # main clean?
git -C /home/user/work_p/Datapipeline-Data-data_pipeline_test status       # dev clean?
```

### Forbidden Actions

- **Never manually edit** `src/`, `configs/`, `scripts/`, or compose files on the host — they will be overwritten by the next CI deployment's `rsync --delete` + `git reset --hard`. All changes must go through git commit → push.
- **No force-push to `main`** (CI not triggered + history corruption)
- `.env` / `.env.test` are not git-tracked. When changed, edit directly on the host and restart Dagster for the affected environment.
- Debug edits made on staging without committing to `dev` will be lost on the next deployment.

---

## Coding Rules

- **Python 3.10+**, formatter/linter: `ruff` (line-length 120)
- **Dagster**: prefer `@asset`; use `@op+@job` only when necessary
- **Import layers** — there is a 5-layer comment in the code. Importing from a lower layer into a higher layer is forbidden.
  - L1-2: `lib/` (pure Python, includes key_builders) → L3: `ops` → L4: `assets/sensors` → L5: `definitions.py`
  - `lib/spec_config.py` handles pure tag parsing only. DB-dependent functions belong in `defs/spec/config_resolver.py`.
  - MinIO key builders are consolidated in `lib/key_builders.py`. Each `defs/` module delegates via a thin wrapper.
- **Module split rules** — large files are split into domain-specific submodules:
  - `defs/process/`: `assets.py` (routing) + `helpers.py` + `captioning.py` + `frame_extract.py` + `raw_frames.py`
  - `defs/label/`: `assets.py` (routing) + `label_helpers.py` + `timestamp.py` + `artifact_*.py`
  - `resources/`: `duckdb_base.py` + `duckdb_phash.py` + `duckdb_migration.py` + `duckdb_ingest_*.py`
- **Commits**: conventional commits (`feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`)
  - Focus on **"what and why"** rather than "how it was changed" (see `.gitmessage.txt`)
- **Error handling**: per-file fail-forward — one file failing should not stop the rest
- **Tests**: pytest, in-memory DuckDB fixture, mocked MinIO (`moto[s3]`), shared fixtures in `tests/conftest.py`

---

## Core Operational Rules (Not Visible in Code)

### DuckDB Concurrency
- DuckDB = single-file write lock ⇒ `tags={"duckdb_writer": "true"}` is mandatory
- `run_coordinator` limits `duckdb_writer` tag concurrency to 1

### NFS/NAS Failure Handling
- Sensors catching `OSError/PermissionError/TimeoutError` → graceful skip, retry on next tick
- Recommended settings for NAS latency:
  - `AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES=20`
  - `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK=3`
  - `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS=300`

### File Error Policy
- `file_missing`, `empty_file`, `ffprobe_failed` → **no DB insert + no archive move**
- Tracking is recorded only in the JSONL failure log (`<manifest_dir>/failed/*.jsonl`)
- Transient errors (DuckDB lock) → retry manifest auto-created; not recorded as failed rows

### Archive Move
- `source_unit_type=directory` with all files successful → move the entire folder to archive
- Chunked manifest → accumulative per-file moves (prevents premature folder move)
- Archive folder name collision → automatic `__2`, `__3` suffix branching
- Only files whose archive move has completed retain `ingest_status=completed`

### MinIO Bucket/Path Policy
- `vlm-raw` · `vlm-labels` · `vlm-processed` · `vlm-dataset` · `vlm-classification` (5 fixed buckets)
- `raw_key = <source_unit_name>/<rel_path>` — `YYYY/MM` prefix is forbidden
- Event JSON source of truth is `vlm-labels` only. Duplicate storage in `vlm-processed` is forbidden.
- Classification results: **original file copy** in `vlm-classification/<folder_prefix>/{video|image}/<class>/<file>` format (no JSON/DB ingestion)

### Staging Reset (Clean Re-test)
1. Stop staging containers:
   `docker stop pipeline-test-dagster-1 pipeline-test-dagster-daemon-1 pipeline-test-dagster-code-server-1`
2. Delete all objects from the 5 staging MinIO buckets (`vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`, `vlm-classification`) — via `:9003` console or `mc rm --recursive --force local/<bucket>`
3. Delete `Datapipeline-Data-data_pipeline_test/docker/data/staging.duckdb` (owned by root; `docker run --rm -v ... alpine rm -f` is recommended)
4. Delete contents of `Datapipeline-Data-data_pipeline_test/docker/app/dagster_home/storage/` (resets run, sensor, and schedule state)
5. Restart: `cd .../data_pipeline_test/docker && docker compose up -d`
- ⚠️ The staging incoming/archive source folders (`/home/user/mou/staging/incoming`, `/home/user/mou/staging/archive`) must **never be deleted** unless explicitly requested.

---

## Service Network & Volumes (Easy to Miss in Code)

- Docker network: `pipeline-network`
- **Host ↔ Container path mappings** (bind mounts in compose):
  - `/home/user/mou/incoming` → `/nas/incoming`
  - `/home/user/mou/archive` → `/nas/archive`
  - `/home/user/mou/staging` → `/nas/staging` (staging only)
  - Code → runtime path: **no mount**. The container uses only the src copied in via `COPY src/ /src/vlm/` at image build time (`/src/vlm`, `/src/python`). Host src changes are not reflected in the container until the next `docker compose build`.
- DuckDB **actual host path**: `./docker/data/pipeline.duckdb`
- YOLO server: dedicated to GPU 1 (`cuda:1`, `NVIDIA_VISIBLE_DEVICES=1`)
- Places365 model cache: `/data/models/places365` (auto_download=false; only the fixed cache is used)
- `PYTHONPATH` (container): `/:/src/python:/src/vlm`

---

## Frequently Used Scripts

| Script | Purpose |
|--------|---------|
| `scripts/query_local_duckdb.py` | Local DuckDB read queries (includes lock-avoidance fallback) |
| `scripts/backfill_video_metadata.py` | Backfill missing video_metadata |
| `scripts/cleanup_duplicate_assets.py` | Clean up checksum duplicates |
| `scripts/recompute_archive_checksums.py` | Re-hash archive originals |
| `scripts/reupload_minio_from_archive.py` | Re-upload to MinIO from archive |
| `scripts/staging_test_dispatch.py` | Staging dispatch test |
| `scripts/verify_mvp.sh` | E2E validation |

---

## Label Studio Integration

- compose: `docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml up -d`
- LS UI: `http://<HOST>:8084` (default 8080, but `LS_PORT=8084` is used due to conflict with dispatch-agent)
- Required env: `LS_API_KEY` (issued from LS account settings), `WEBHOOK_HOST` (IP accessible from LS → webhook)
- Sensor `ls_task_create_sensor`: must be manually turned ON in the Dagster UI (default=STOPPED)
- Webhook registration (per project): `python src/gemini/ls_webhook.py register --project <id>`
- When presigned URLs expire (default 7 days): `python src/gemini/ls_tasks.py renew --project-name <name>`
- Slack notifications/slash commands are activated when `SLACK_WEBHOOK_URL` and `SLACK_SIGNING_SECRET` are set

---

## GCS External Ingestion

- Buckets: `source-a-rtsp-bucket` (primary), `source-b-event-bucket`, `source-c-event-bucket`
- Script: `gcp/download_from_gcs_rclone.py`
- Dagster schedule: `gcs_download_schedule` (daily at 04:00 KST)
- Zero-byte file recovery: `GCS_ZERO_BYTE_RETRIES` (default 2)

---

## Caveats When Replacing DuckDB Files

1. **Always stop the service** before replacing
2. Check whether a `.wal` file exists → if present, back it up and delete it
3. A stale WAL re-applied to a new DB will cause corruption

---

## Gemini / Vertex AI

- Project: `your-gcp-project`, region: `us-central1`
- Default model: `gemini-2.5-flash`
- Credential priority: `GEMINI_GOOGLE_APPLICATION_CREDENTIALS` → `GOOGLE_APPLICATION_CREDENTIALS` → `GEMINI_SERVICE_ACCOUNT_JSON`
- Videos over 450 MB → preview mp4 is auto-generated (to stay under Vertex's 524 MB limit)

---

## YOLO-World

- Model: `yolov8l-worldv2.pt` (`/data/models/yolo/`)
- Dependency trap: without the `clip` package the container fails to start → `git+https://github.com/ultralytics/CLIP.git` is required
- Health check: `GET /health` → `model_loaded=true`
