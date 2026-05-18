> ⚠️ **HISTORICAL SNAPSHOT — DO NOT TREAT AS THE LIVE ARCHITECTURE.**
>
> This document is frozen at **2026-02-20** (DuckDB primary, 8-table schema).
> The pipeline has since migrated to PostgreSQL primary (18 tables) — Phase 6 completed 2026-05-13.
>
> For the live architecture state, consult:
> - [`docs/references/db_migration_topology.md`](../references/db_migration_topology.md) — current PG topology decision
> - [`docs/exec-plans/active/production-pg-rollout-plan.md`](../exec-plans/active/production-pg-rollout-plan.md) — phase-by-phase rollout (Phase 6 = current)
> - [`docs/references/dbeaver-pg-duckdb-test-guide.md`](../references/dbeaver-pg-duckdb-test-guide.md) — operator validation guide
> - [`CLAUDE.md`](../../CLAUDE.md) — coding rules and operational invariants
>
> Kept on file as a reference for cross-period diffs and pre-migration design rationale; do not extend.

---

┌─────────────────────────────────────────────────────────────────────────────┐
│               VLM Data Pipeline — MVP Architecture (2026-02-20)             │
└─────────────────────────────────────────────────────────────────────────────┘

  Pipeline stages:
    ingested_raw_files → dedup_results → labeled_files → processed_clips → built_dataset → motherduck_sync(optional)

  Key changes (2026-02-13 refactoring):
    - data_pipeline_job (file_organize → nas_scan → minio_upload) removed
    - Consolidated into mvp_stage_job (INGEST → DEDUP → LABEL → PROCESS → BUILD → SYNC)
    - incoming_manifest_sensor + auto_bootstrap_manifest_sensor linked triggers
    - (2026-02-19) Added auto_bootstrap stabilization gate (blocks manifest creation while data is being copied)
    - (2026-02-19) source_unit_type=directory ingest now supports folder-level archive moves
    - (2026-02-20) Added environment classification metadata to video_metadata (indoor/outdoor + day/night)
    - (2026-02-20) Places365 CUDA-first + heuristic fallback introduced (`video_env.py`)
    - (2026-02-20) torch/torchvision cu124 installed in Docker runtime; GPU usage verified
    - (2026-02-20) Places365 model cache path fixed at `/data/models/places365`, auto download disabled
    - motherduck_sync_job standalone execution path preserved
    - NAS folder tree (PostgreSQL) feature retained separately (nas_folder_tree_job)
    - Legacy/artifact cleanup: `src/rust_scanner`, `src/python/scanner`, `docker/airflow/logs` removed


┌─────────────────────────────────────────────────────────────────────────────┐
│                       DuckDB Schema (Strict 8 Tables)                       │
└─────────────────────────────────────────────────────────────────────────────┘

┌─ raw_files ─────────────────────────────────┐
│ (PK) asset_id        VARCHAR   UUID         │
│      source_path     VARCHAR   source path (NAS) │
│      original_name   VARCHAR   original filename │
│      media_type      VARCHAR   image/video  │
│      file_size       BIGINT    bytes        │
│ (UK) checksum        VARCHAR   SHA-256      │
│      phash           VARCHAR   pHash hex    │
│      dup_group_id    VARCHAR   similarity group │
│      archive_path    VARCHAR   NAS archive  │
│      raw_bucket      VARCHAR   vlm-raw      │
│      raw_key         VARCHAR   MinIO key    │
│      ingest_batch_id VARCHAR   batch group  │
│      transfer_tool   VARCHAR   manual       │
│      ingest_status   VARCHAR   state machine │
│      error_message   VARCHAR   error message │
│      created_at      TIMESTAMP auto         │
│      updated_at      TIMESTAMP auto         │
├─────────────────────────────────────────────┤
│  1:1 ──> image_metadata     (asset_id)      │
│  1:1 ──> video_metadata     (asset_id)      │
│  1:N ──> labels             (asset_id)      │
│  1:N ──> processed_clips    (source_asset_id)│
└─────────────────────────────────────────────┘

┌─ image_metadata ───────────────────────────┐
│ (PK/FK) asset_id    VARCHAR  → raw_files   │
│         width       INT      pixels        │
│         height      INT      pixels        │
│         color_mode  VARCHAR  RGB           │
│         bit_depth   INT      8             │
│         codec       VARCHAR  jpeg/png/heic │
│         has_alpha   BOOLEAN  false         │
│         orientation INT      1-8           │
│         extracted_at TIMESTAMP             │
└────────────────────────────────────────────┘

┌─ video_metadata ───────────────────────────┐
│ (PK/FK) asset_id    VARCHAR  → raw_files   │
│         width       INT      pixels        │
│         height      INT      pixels        │
│         duration_sec FLOAT   seconds       │
│         fps         FLOAT    frames/sec    │
│         codec       VARCHAR  h264/h265     │
│         bitrate     INT      bps           │
│         frame_count INT      total frames  │
│         has_audio   BOOLEAN  false         │
│         environment_type VARCHAR indoor/outdoor │
│         daynight_type   VARCHAR day/night       │
│         outdoor_score   FLOAT   0.0~1.0         │
│         avg_brightness  FLOAT   average brightness │
│         env_method      VARCHAR places365_cuda/heuristic │
│         extracted_at TIMESTAMP             │
└────────────────────────────────────────────┘

┌─ labels ────────────────────────────────────┐
│ (PK) label_id    VARCHAR  UUID              │
│ (FK) asset_id    VARCHAR  → raw_files       │
│      labels_bucket VARCHAR vlm-labels       │
│      labels_key  VARCHAR  MinIO key         │
│      label_format VARCHAR COCO/labelme/yolo │
│      label_tool  VARCHAR  pre-built/CVAT    │
│      event_count INT      annotation count  │
│      label_status VARCHAR state machine     │
│      created_at  TIMESTAMP                  │
└─────────────────────────────────────────────┘

┌─ processed_clips ───────────────────────────┐
│ (PK) clip_id         VARCHAR   UUID         │
│ (FK) source_asset_id VARCHAR   → raw_files  │
│ (FK) source_label_id VARCHAR   → labels     │
│      event_index     INT       image: 0     │
│ (UK) checksum        VARCHAR   SHA-256      │
│      file_size       BIGINT    bytes        │
│      processed_bucket VARCHAR  vlm-processed│
│      clip_key        VARCHAR   MinIO key    │
│      label_key       VARCHAR   label MinIO key │
│      width           INT       pixels ★absorbed │
│      height          INT       pixels ★absorbed │
│      codec           VARCHAR   codec ★absorbed  │
│      process_status  VARCHAR   state machine │
│      created_at      TIMESTAMP auto         │
└─────────────────────────────────────────────┘

┌─ datasets ──────────────────────────────────┐
│ (PK) dataset_id     VARCHAR   UUID          │
│      name           VARCHAR   name          │
│      version        VARCHAR   v0.1          │
│      config         JSON      config options │
│      split_ratio    JSON      80/10/10      │
│      dataset_bucket VARCHAR   vlm-dataset   │
│      dataset_prefix VARCHAR   path prefix   │
│      build_status   VARCHAR   state machine │
│      created_at     TIMESTAMP auto          │
└─────────────────────────────────────────────┘

┌─ dataset_clips ─────────────────────────────┐
│ (PK/FK) dataset_id VARCHAR  → datasets      │
│ (PK/FK) clip_id    VARCHAR  → processed_clips│
│         split      VARCHAR  train/val/test  │
│         dataset_key VARCHAR MinIO key        │
└─────────────────────────────────────────────┘

Table relationship summary:
  raw_files ──1:1──> image_metadata     (asset_id)
  raw_files ──1:1──> video_metadata     (asset_id)
  raw_files ──1:N──> labels             (asset_id)
  raw_files ──1:N──> processed_clips    (source_asset_id)
  labels    ──1:N──> processed_clips    (source_label_id)
  processed_clips ──1:N──> dataset_clips (clip_id)
  datasets  ──1:N──> dataset_clips      (dataset_id)

  ★ clip_metadata table removed — width/height/codec absorbed into processed_clips


┌─────────────────────────────────────────────────────────────────────────────┐
│                       Dagster Structure (2026-02-13)                        │
└─────────────────────────────────────────────────────────────────────────────┘

  Jobs:
    mvp_stage_job        — full pipeline (INGEST → DEDUP → LABEL → PROCESS → BUILD → SYNC)
    ingest_job           — INGEST standalone run
    dedup_job            — DEDUP standalone run
    label_job            — LABEL standalone run
    process_build_job    — PROCESS + BUILD run
    motherduck_sync_job  — MotherDuck sync standalone run
    nas_folder_tree_job  — NAS folder tree → PostgreSQL (retained)

  Sensors:
    incoming_manifest_sensor — polls .manifests/pending/*.json → triggers mvp_stage_job
                               graceful skip on NFS failure (retry after 30 sec)
                               cursor-based deduplication
    auto_bootstrap_manifest_sensor — scans /nas/incoming media as source units (folder/file)
                                      auto-generates manifest only when stabilization (cycles/age) criteria are met

  Schedules:
    nas_folder_tree_schedule — weekday 18:00 NAS folder tree scan
    (mvp_stage schedule removed — sensor-based trigger only)

  Removed items:
    data_pipeline_job    — file_organize → nas_scan → minio_upload (removed)
    related assets       — nas_scan, minio_upload, file_organize (removed)
    (motherduck_sync reconnected and operational)


┌─────────────────────────────────────────────────────────────────────────────┐
│                       Code Structure (2026-02-13)                           │
└─────────────────────────────────────────────────────────────────────────────┘

  docker/app/dagster_defs.py          Dagster main definitions (for Docker container)
  src/vlm_pipeline/                   MVP pipeline package (standalone runnable)
   ├ definitions.py                   Dagster Definitions assembly (Layer 5)
   ├ resources/
   │  ├ config.py                     PipelineConfig (Pydantic BaseSettings)
   │  ├ duckdb.py                     DuckDBResource (ConfigurableResource)
   │  └ minio.py                      MinIOResource (ConfigurableResource)
   ├ defs/
   │  ├ ingest/
   │  │  ├ assets.py                  ingested_raw_files @asset (Layer 4)
   │  │  ├ ops.py                     register_incoming, normalize_and_archive, ingest_summary @op (Layer 3)
   │  │  └ sensor.py                  incoming_manifest_sensor (Layer 4)
   │  ├ dedup/
   │  │  └ assets.py                  dedup_results @asset (Layer 4)
   │  ├ label/
   │  │  └ assets.py                  labeled_files @asset (Layer 4)
   │  ├ process/
   │  │  └ assets.py                  processed_clips @asset (Layer 4)
   │  └ build/
   │     └ assets.py                  built_dataset @asset (Layer 4)
   ├ lib/                             pure Python utilities (Layer 1-2)
   │  ├ sanitizer.py                  file/path segment normalization (Korean→romanized, ASCII-safe)
   │  ├ validator.py                  file validation (extension, size, readability)
   │  ├ checksum.py                   SHA-256 checksum
   │  ├ file_loader.py                1-pass image loading + metadata extraction
   │  ├ video_loader.py               ffprobe + video environment metadata extraction linkage
   │  ├ video_env.py                  Places365 (CUDA-first) + heuristic voting
   │  └ phash.py                      pHash calculation + Hamming distance
   └ sql/
      ├ schema.sql                    Strict 8 tables DDL
      └ migration.sql                 asset_catalog → raw_files migration

  src/python/common/                  existing shared utilities (legacy, gradual migration target)
  tests/
   ├ unit/                            unit tests
   ├ integration/                     integration tests
   └ e2e/                             E2E tests
  Removed:
    - `src/rust_scanner/` (legacy Rust scanner)
    - `src/python/scanner/` (legacy Python scanner remnants)
    - `docker/airflow/logs/` (large runtime log artifacts)

  Import layers (5-layer):
    Layer 1: lib/ (pure Python, no external dependencies)
    Layer 2: lib/ (external library dependencies: PIL, imagehash, etc.)
    Layer 3: ops (Dagster @op, imports lib/ + resources/)
    Layer 4: assets/sensors (Dagster @asset/@sensor, imports ops)
    Layer 5: definitions.py (assembles everything)


┌─────────────────────────────────────────────────────────────────────────────┐
│                       Environment Variables (docker-compose.yml)            │
└─────────────────────────────────────────────────────────────────────────────┘

  DATAOPS_DUCKDB_PATH          /data/pipeline.duckdb
  MINIO_ENDPOINT               http://minio:9000
  MINIO_ACCESS_KEY             minioadmin
  MINIO_SECRET_KEY             minioadmin
  INCOMING_DIR                 /nas/incoming
  ARCHIVE_DIR                  /nas/archive
  DATAOPS_MANIFEST_DIR         /nas/incoming/.manifests
  DATAOPS_ASSET_PATH_PREFIX_FROM  (multiple prefixes separated by semicolons)
  DATAOPS_ASSET_PATH_PREFIX_TO    (canonical path substitution target)
  MOTHERDUCK_TOKEN             MotherDuck API token
  MOTHERDUCK_DB                target DB name for sync (default: pipeline_db)
  MOTHERDUCK_SYNC_ENABLED      true/false (optional sync on/off)
  MOTHERDUCK_SYNC_DRY_RUN      true/false
  MOTHERDUCK_SHARE_UPDATE      MANUAL | AUTOMATIC
  MOTHERDUCK_SYNC_TIMEOUT_SEC  sync subprocess timeout (default: 600 sec)
  INCOMING_SENSOR_INTERVAL_SEC incoming manifest sensor interval (sec) (default: 180)
  AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC auto bootstrap sensor interval (sec) (default: 180)
  AUTO_BOOTSTRAP_STABLE_CYCLES consecutive stabilization cycles for source unit (default: 2)
  AUTO_BOOTSTRAP_STABLE_AGE_SEC wait time after last modification (sec) (default: 120)
  AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK upper bound on detailed-scan units per auto bootstrap tick (default: 5)
  AUTO_BOOTSTRAP_MAX_READY_UNITS_PER_TICK upper bound on manifest-generation target units per tick (default: 5)
  AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS pending backlog protection upper bound (default: 200)
  AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK upper bound on new manifests created per tick (default: 20)
  AUTO_BOOTSTRAP_MAX_FILES_PER_MANIFEST auto manifest split size (default: 100)
  AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES top-level directory count per discovery tick (0=unlimited). Recommend 15–30 on NAS latency
  DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS    sensor gRPC timeout (sec). Default 180. Raise to 300+ on NAS latency
  INCOMING_SENSOR_MAX_IN_FLIGHT_RUNS incoming enqueue backpressure upper bound (default: 2)
  INCOMING_SENSOR_MAX_RETRY_PER_MANIFEST failed manifest auto-retry upper bound (default: 3)
  INGEST_FAILURE_LOG_DIR       ingest failure JSONL log path (default: <manifest_dir>/failed)
  INGEST_TRANSIENT_RETRY_MAX_ATTEMPTS max attempts for transient retry manifest (default: 3)
  GCS_ZERO_BYTE_RETRIES        zero-byte file recovery retries after GCS download (default: 2)
  VIDEO_ENV_MODEL_DIR          Places365 model cache path (`/data/models/places365`)
  VIDEO_ENV_AUTO_DOWNLOAD      false (production: fixed cache only)
  VIDEO_ENV_USE_PLACES365      true/false (default true)
  VIDEO_ENV_SAMPLE_COUNT       sample frame count per video (default 3)
  VIDEO_DAY_NIGHT_THRESHOLD    day/night threshold (default 90)
  VIDEO_ENV_TOP_K              Places365 top-k average (default 10)


┌─────────────────────────────────────────────────────────────────────────────┐
│                       State Machine (Status Transitions)                    │
└─────────────────────────────────────────────────────────────────────────────┘

  raw_files.ingest_status (retained rows only):
    pending → uploading → completed
    ※ duplicate/validation-failed/unmoved files are not retained in raw_files (excluded from sync)

  labels.label_status:
    pending → completed
    pending → failed

  processed_clips.process_status:
    pending → completed
    pending → failed

  datasets.build_status:
    pending → building → completed
    building → failed


┌─────────────────────────────────────────────────────────────────────────────┐
│                       Operational Rules                                     │
└─────────────────────────────────────────────────────────────────────────────┘

  DuckDB concurrency:
    - DuckDB is single-file write lock based
    - Apply tags={"duckdb_writer": "true"} to Dagster jobs
    - run_coordinator: max_concurrent_runs=4 + tag_concurrency_limits(duckdb_writer=true, limit=1)

  NFS failure handling:
    - graceful skip on OSError/PermissionError/TimeoutError in sensor
    - auto-retry on next polling cycle
      (`incoming_manifest_sensor=INCOMING_SENSOR_INTERVAL_SEC(default 180 sec)`,
       `auto_bootstrap_manifest_sensor=AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC(default 180 sec)`)

  Incoming/Archive operations:
    - auto_bootstrap generates manifest after source unit stabilization based on signature (file_count/total_size/max_mtime)
    - auto_bootstrap uses lightweight unit discovery then detailed scan per scan window instead of full file scan
      (`AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK`)
    - ticks with many ready units generate manifests only up to the upper bound; remainder deferred
      (`AUTO_BOOTSTRAP_MAX_READY_UNITS_PER_TICK`)
    - if a source unit has many files, chunk manifests are split by `AUTO_BOOTSTRAP_MAX_FILES_PER_MANIFEST`
    - chunk manifests are enqueued/deduplicated per `source_unit_dispatch_key`
    - pending backlog protection:
      - auto_bootstrap stops generating if pending count reaches `AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS`
      - only `AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK` new manifests created per tick
    - `source_unit_type=directory` manifest moves entire folder to `/nas/archive/<source_unit_name>` on full success
    - chunked manifests (`source_unit_chunk_count>1`) use per-file cumulative moves to prevent collision/premature folder moves
    - in `source_unit_type=directory`, if `failed=0, skipped>0`, only successful files are partially moved
    - on archive target folder name collision, path automatically branches with `__2`, `__3` suffix
    - legacy archive (`YYYY/MM`) structure retained as lookup fallback only
    - duplicate/validation-failed/move-failed files remain in incoming + DB records are cleaned up
    - file errors (`file_missing`, `empty_file`, `ffprobe_failed`, etc.) do not create failed rows
      in `raw_files/video_metadata/image_metadata`
    - file errors are not moved to archive; tracked only in `<manifest_dir>/failed/<manifest_id>.jsonl`
    - transient errors (DuckDB lock/conflict) are retried via a separate retry manifest instead of failed row:
      `retry_<original_manifest_id>_<UTC timestamp>.json`
      (`retry_of_manifest_id`, `retry_attempt`, `retry_reason=transient_db_lock`)
    - retry manifest generation stops when `INGEST_TRANSIENT_RETRY_MAX_ATTEMPTS` is exceeded
    - on archive move exception, immediately recheck destination file existence;
      if file exists, recover as `completed + archive_path`
    - only files actually moved to archive maintain `raw_files.ingest_status=completed`
    - `duplicate_skipped_in_manifest:*` notice is recorded only on the duplicate-related `asset_id`
    - if an existing duplicate marker exists, merge-accumulate the filename list; preserve regular error messages

  MotherDuck sync operations:
    - `pipeline__motherduck_sync` accepts both env/run_config (enabled/db/dry_run/share_update/timeout_sec)
    - subprocess timeout causes run failure to prevent queue stagnation
    - graceful skip if `MOTHERDUCK_TOKEN` is not set
    - default sync target DB is `pipeline_db`
    - on sync, legacy tables (`clip_metadata`) are auto-removed before syncing MVP tables

  DEDUP operations:
    - pHash calculation input source priority: `archive_path` -> `source_path` -> MinIO(`raw_bucket/raw_key`)
    - designed so pHash/dup_group_id can continue to be updated after archive move
    - on pHash success, `error_message` is cleared (removes residual `phash_source_missing` messages)

  MinIO bucket policy:
    - vlm-raw: original media (INGEST stage)
    - vlm-labels: label JSON (LABEL stage)
    - vlm-processed: processed output (PROCESS stage)
    - vlm-dataset: final dataset (BUILD stage)
    - raw_key is stored after ASCII-safe normalization of both filename and folder segments

  DuckDB path:
    - inside container: /data/pipeline.duckdb
    - host actual file: ./docker/data/pipeline.duckdb
    - priority 1: DATAOPS_DUCKDB_PATH (or DUCKDB_PATH)
    - priority 2: /data/pipeline.duckdb if in container environment
    - priority 3: configs/global.yaml duckdb_path


┌─────────────────────────────────────────────────────────────────────────────┐
│               Incident Response Playbook (for improved troubleshooting)     │
└─────────────────────────────────────────────────────────────────────────────┘

  1) Classify the incident first:
    - File-level errors: `file_missing`, `empty_file`, `ffprobe_failed`
    - Transient system errors: DuckDB lock/conflict, timeout
    - Data integrity errors: `raw_files` vs `video_metadata` count mismatch

  2) Immediately contain contamination:
    - If queue flooding is seen, reduce generation rate first via sensor interval/limits
      (`INCOMING_SENSOR_INTERVAL_SEC`, `AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC`,
       `AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS`, `AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK`)
    - File errors are blocked from DB insertion + unmoved policy to prevent further contamination

  3) Evidence-based verification (SQL + logs):
    - Check counts by status/error and prioritize accordingly
    - For failure details, trace root cause from JSONL logs (`<manifest_dir>/failed/*.jsonl`) rather than DB
    - For GCS ingestion issues, check zero-byte file detection results first

  4) Separate reprocessing paths:
    - File errors: restore/replace original file then re-ingest with new manifest
    - Transient errors: consume auto-generated retry manifests first
    - Archive exceptions: check destination existence then determine if `completed` recovery is feasible

  5) Completion criteria (required):
    - Local DuckDB: verify `failed`, `missing metadata` against targets
    - Re-verify with same query after MotherDuck sync
    - Record pre/post metrics (e.g., raw/video/failed) together with WORKLOG


┌─────────────────────────────────────────────────────────────────────────────┐
│               Incident Type Immediate Checklist (SQL/Commands)              │
└─────────────────────────────────────────────────────────────────────────────┘

  Common:
    - Read queries (with lock-avoidance fallback):
      - `python3 scripts/query_local_duckdb.py --sql "<SQL>"`
    - Local DB path defaults:
      - host: `./docker/data/pipeline.duckdb`
      - container: `/data/pipeline.duckdb`
    - manifest default paths:
      - `pending=/nas/incoming/.manifests/pending`
      - `failed=/nas/incoming/.manifests/failed`

  1) `raw_files` vs `video_metadata` count mismatch
    - Check:
      - `python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) AS raw_files FROM raw_files;"`
      - `python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) AS video_metadata FROM video_metadata;"`
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT COUNT(*) AS missing_video_meta FROM raw_files rf LEFT JOIN video_metadata vm ON rf.asset_id = vm.asset_id WHERE rf.media_type='video' AND vm.asset_id IS NULL;\"`
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT rf.asset_id, rf.ingest_status, rf.source_path, rf.archive_path FROM raw_files rf LEFT JOIN video_metadata vm ON rf.asset_id = vm.asset_id WHERE rf.media_type='video' AND vm.asset_id IS NULL ORDER BY rf.created_at DESC LIMIT 30;\"`
    - Action:
      - Run missing metadata backfill (script):
        - host: `python3 scripts/backfill_video_metadata.py --db ./docker/data/pipeline.duckdb --statuses completed --log-every 20`
        - container: `python3 /src/vlm/scripts/backfill_video_metadata.py --db /data/pipeline.duckdb --statuses completed --log-every 20`
    - Completion criteria:
      - `missing_video_meta=0`

  2) `failed` spike (file errors / other errors)
    - Check:
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT ingest_status, COUNT(*) FROM raw_files GROUP BY 1 ORDER BY 1;\"`
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT COALESCE(error_message,'(null)') AS error_message, COUNT(*) AS cnt FROM raw_files WHERE ingest_status='failed' GROUP BY 1 ORDER BY cnt DESC LIMIT 30;\"`
      - `ls -lt /nas/incoming/.manifests/failed/*.jsonl 2>/dev/null | head`
    - Action:
      - File errors (`file_missing`, `empty_file`, `ffprobe_failed`) are not DB-insert targets;
        restore/verify the source file first then re-ingest
      - Re-ingest via re-issuing the original manifest or using the retry manifest path
    - Completion criteria:
      - No recurrence of the same error in new runs + only failure logs remain with no DB contamination

  3) DuckDB lock/conflict (`Could not set lock on file`, `Conflicting lock is held`)
    - Check:
      - `lsof ./docker/data/pipeline.duckdb`
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT COUNT(*) FROM raw_files;\"`
      - `ls -1 /nas/incoming/.manifests/pending/retry_*.json 2>/dev/null | tail -n 20`
    - Action:
      - Wait for any in-progress writer run to finish; only clean up long-running stale runs
      - Verify transient errors generate retry manifests instead of failed rows
      - For queue overload, immediately mitigate using backpressure values in item 5 below
    - Completion criteria:
      - Lock errors absorbed into retry manifests; raw_files failed count does not increase

  4) Archive move failure / missed move
    - Check:
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT asset_id, source_path, archive_path, error_message FROM raw_files WHERE ingest_status='failed' AND error_message LIKE 'archive_move_failed%' ORDER BY updated_at DESC LIMIT 30;\"`
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT asset_id, source_path, archive_path, error_message FROM raw_files WHERE error_message='archive_source_missing' ORDER BY updated_at DESC LIMIT 30;\"`
    - Action:
      - First verify archive existence (same as `dest.exists()` policy):
        - `find /nas/archive -type f -name '<filename>' | head`
      - If archive exists: recover as `completed + archive_path`
      - If archive does not exist: reprocess from incoming source (re-issue manifest)
    - Completion criteria:
      - All archive-existing cases cleaned up to `completed`; no orphan rows

  5) Pending queue overload (sensor interval / generation rate imbalance)
    - Check:
      - `find /nas/incoming/.manifests/pending -maxdepth 1 -name '*.json' | wc -l`
      - `ls -lt /nas/incoming/.manifests/pending/*.json 2>/dev/null | head -n 20`
    - Action (recommended defaults):
      - `INCOMING_SENSOR_INTERVAL_SEC=180`
      - `AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC=180`
      - `AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS=200`
      - `AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK=20`
      - `INCOMING_SENSOR_MAX_IN_FLIGHT_RUNS=2`
    - Completion criteria:
      - Pending backlog recovers to stable range; lock/conflict recurrence frequency decreases

  6) GCS download zero-byte files
    - Check:
      - `find /nas/incoming/gcp -type f \\( -iname '*.mp4' -o -iname '*.mov' -o -iname '*.avi' -o -iname '*.mkv' -o -iname '*.jpg' -o -iname '*.jpeg' -o -iname '*.png' \\) -size 0 | head -n 30`
      - `rg -n \"zero-byte\" gcp/download_from_gcs_rclone.py`
    - Action:
      - Run with increased retries (if needed):
        - `python3 gcp/download_from_gcs_rclone.py --download --mode date-folders --download-dir /nas/incoming/gcp --buckets source-a-rtsp-bucket --zero-byte-retries 4`
      - Verify that failed folders remain without `_DONE` and stay as re-download targets
    - Completion criteria:
      - Zero zero-byte media files + target folders have proper `_DONE` created

  7) Local vs MotherDuck mismatch
    - Check:
      - Local:
        - `python3 scripts/query_local_duckdb.py --sql \"SELECT COUNT(*) AS raw_files FROM raw_files;\"`
        - `python3 scripts/query_local_duckdb.py --sql \"SELECT COUNT(*) AS video_metadata FROM video_metadata;\"`
        - `python3 scripts/query_local_duckdb.py --sql \"SELECT COUNT(*) AS failed FROM raw_files WHERE ingest_status='failed';\"`
      - MotherDuck:
        - Run `motherduck_sync_job` then re-verify with the same count SQL
    - Action:
      - Clean up local anomalies first before sync (policy-violating failed rows / missing metadata)
      - After sync, verify both local/cloud simultaneously with the same query
    - Completion criteria:
      - Core metrics `raw_files`, `video_metadata`, `failed` match local=motherduck


┌─────────────────────────────────────────────────────────────────────────────┐
│                       Coding Standards                                      │
└─────────────────────────────────────────────────────────────────────────────┘

  - Python 3.11+, ruff formatter/linter
  - Dagster: @asset > @op+@job (declarative first)
  - ConfigurableResource: DuckDBResource, MinIOResource
  - Pydantic BaseSettings: PipelineConfig (auto env var binding)
  - Tests: pytest, in-memory DuckDB fixture, mocked MinIO
  - Commit messages: conventional commits (feat:, fix:, refactor:, test:, docs:)
  - Error handling: per-file fail-forward (continue processing remaining files even if one fails)


┌─────────────────────────────────────────────────────────────────────────────┐
│                       Validation History (2026-02-12 ~ 2026-03-06)         │
└─────────────────────────────────────────────────────────────────────────────┘

  [DONE] MinIO endpoint console/API separation fix + environment unification (2026-03-06)
    - Issues:
      - Mixed MinIO access ports caused unstable upload/verification paths
      - Confusion between console port (9001) and S3 API port (9000)
    - Actions:
      - `docker/docker-compose.yaml`, `.env.example`, `docker/.env`
        - Unified to `MINIO_ENDPOINT=http://10.0.0.36:9000`
      - Restarted/recreated Dagster/app runtime to use the same endpoint
    - Verification:
      - Confirmed consistent 9000 endpoint usage in runtime env vars and upload paths

  [DONE] INGEST MinIO upload re-enabled + raw_key prefix removed (2026-03-06)
    - Issues:
      - MinIO upload was temporarily disabled; path policy needed simplification to `{project}/...`
    - Actions:
      - `src/vlm_pipeline/defs/ingest/ops.py`
        - Re-enabled MinIO upload calls (memory/file object/fallback)
        - raw_key now generated based on `source_unit_name/rel_path` (YYYY/MM removed)
        - Restored log to `MinIO upload complete`
    - Verification:
      - New sample keys are in `source-a-rtsp-bucket/...`, `source-b-event-bucket/...` form

  [DONE] MinIO path regression root cause analysis (RCA) + re-upload recovery established (2026-03-06)
    - Issues:
      - Some uploads continued to `vlm-raw/2026/03/...` even after code fix
    - Root cause:
      - Misconfigured DuckDB path in `docker/.env` caused some processes to reference a different DB
      - Residual reupload processes running with old config continued uploading with old key policy
    - Actions:
      - Corrected `docker/.env` to `DATAOPS_DUCKDB_PATH=/data/pipeline.duckdb`,
        `DUCKDB_PATH=/data/pipeline.duckdb`
      - Terminated residual upload processes + cleaned bucket then re-uploaded
      - Used `scripts/reupload_minio_from_archive.py` to re-upload aligned to archive
    - Verification:
      - New upload keys converging to `vlm-raw/{project}/...`
      - Confirmed no `2026/03` prefix recurrence via sample/progress logs

  [DONE] INGEST error file non-insert/non-move + JSONL failure log separation (2026-03-05)
    - Issues:
      - File-level errors were accumulating in `raw_files.failed`, contaminating operational metrics/sync integrity
    - Actions:
      - `src/vlm_pipeline/defs/ingest/ops.py`
        - Removed DB failed row insertion on file errors in register/normalize stages
      - `src/vlm_pipeline/defs/ingest/assets.py`
        - Added JSONL failure log storage based on `INGEST_FAILURE_LOG_DIR`
        - Schema: timestamp, manifest_id, source_path, rel_path, media_type,
          stage, error_code, error_message, retryable
    - Verification:
      - Confirmed file errors are not inserted into DB + not moved; recorded in failure log file only

  [DONE] DuckDB transient lock auto-retry (manifest) applied (2026-03-05)
    - Issues:
      - lock/conflict errors accumulating as failed rows made reprocessing manual
    - Actions:
      - `src/vlm_pipeline/defs/ingest/ops.py`
        - Transient marker-based classification (`Could not set lock on file`, `Conflicting lock is held`, etc.)
      - `src/vlm_pipeline/defs/ingest/assets.py`
        - Auto-generates retry manifest for transient-failed files only
          (`retry_of_manifest_id`, `retry_attempt`, `retry_reason=transient_db_lock`)
        - `INGEST_TRANSIENT_RETRY_MAX_ATTEMPTS` (default 3) applied
      - `src/vlm_pipeline/defs/ingest/sensor.py`
        - Loads retry manifest fields / propagates tags
    - Verification:
      - Confirmed transient cases are separated into retry manifests instead of failed rows

  [DONE] Queue backlog relief + GCS zero-byte recovery + operational integrity recovery (2026-03-05)
    - Actions (queue/sensor):
      - Applied default 180 sec to `INCOMING_SENSOR_INTERVAL_SEC`, `AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC`
      - Applied `AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS` (default 200),
        `AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK` (default 20)
    - Actions (GCS):
      - Added zero-byte media detection/delete/re-download loop to `gcp/download_from_gcs_rclone.py`
      - Linked `GCS_ZERO_BYTE_RETRIES` (default 2) with Dagster `zero_byte_retries` config
    - Operational data correction:
      - Backed up `/data/pipeline.duckdb.bak_20260305_015527`, cleaned and recovered error rows
      - Result: both local/motherduck at `raw_files=8016`, `video_metadata=8016`, `failed=0`

  [DONE] auto_bootstrap scan window + cursor v3 applied (2026-03-04)
    - Issues:
      - Sensor delays occurred as full per-tick scan time grew with increasing incoming source units
    - Actions:
      - Introduced lightweight unit discovery + partial scan in `src/vlm_pipeline/defs/ingest/sensor.py`
      - Extended cursor payload to v3 (`scan_offset`, `units`)
      - Added `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK`, `AUTO_BOOTSTRAP_MAX_READY_UNITS_PER_TICK`
      - Exposed `scan_window`, `deferred_ready`, `scan_elapsed` in skip/log messages
    - Verification:
      - Confirmed scan window/deferred items output in sensor skip reason and info logs

  [DONE] Archive root path unification + done marker search improvement (2026-03-04)
    - Actions:
      - Unified archive target paths to `/nas/archive` root in `src/vlm_pipeline/defs/ingest/assets.py`
      - Aligned done marker search in `gcp/download_from_gcs_rclone.py`:
        `archive/<bucket>/<folder>` first + legacy `archive/<YYYY>/<MM>/...` fallback retained
      - Applied same policy to recovery move path in `scripts/recover_uploading.py`

  [DONE] Duplicate marker merge policy applied (2026-03-04)
    - Actions:
      - Accumulate-merge duplicate marker filenames per asset in
        `src/vlm_pipeline/resources/duckdb.py::mark_duplicate_skipped_assets()`
      - Protect existing `error_message` that is not a duplicate marker from being overwritten

  [DONE] GCP test scope removed + schedule adjusted (2026-03-04)
    - Actions:
      - Removed `TEMP_FORCE_TEST_SCOPE` from `src/vlm_pipeline/defs/gcp/assets.py`
      - Changed `gcs_download_schedule` to `0 */6 * * *` in `src/vlm_pipeline/definitions.py`

  [DONE] INGEST MinIO upload temporary skip mode (2026-03-04)
    - Actions:
      - Commented out MinIO upload calls in `src/vlm_pipeline/defs/ingest/ops.py`
      - Explicitly logged as `MinIO upload skipped (disabled)`

  [DONE] auto_bootstrap large unit splitting + incoming enqueue improvement (2026-03-03)
    - Issues:
      - Single manifest generation for source units with many files caused pending/processing delays
      - Deduplication guard per same source unit conflicted with chunk-based processing, delaying enqueue
    - Actions:
      - Introduced `AUTO_BOOTSTRAP_MAX_FILES_PER_MANIFEST` (default 100)
      - Extended manifest metadata:
        - `source_unit_dispatch_key`
        - `source_unit_total_file_count`
        - `source_unit_chunk_index`
        - `source_unit_chunk_count`
      - `incoming_manifest_sensor` uses `source_unit_dispatch_key` for run_key/deduplication decisions
      - Added `INCOMING_SENSOR_MAX_IN_FLIGHT_RUNS`-based backpressure guard
      - Duplicate pending manifests: keep only latest 1, move superseded to processed
    - Verification:
      - Confirmed chunk manifest generation logs for large source units
      - Confirmed backpressure log (`in_flight`, `limit`) exposure

  [DONE] Chunked manifest archive + legacy path cleanup (2026-03-03)
    - Actions:
      - In `src/vlm_pipeline/defs/ingest/assets.py`,
        chunked manifests use per-file cumulative moves to unit path instead of full folder move
      - Removed `update_minio_uploader/`, `update_python_scanner/`
      - Removed legacy sync fallback from `src/vlm_pipeline/defs/sync/assets.py`

  [DONE] MinIO raw_key folder name Romanization applied (2026-03-03)
    - Issues:
      - Filenames were normalized but Korean folder names remained in raw_key
    - Actions:
      - Added `sanitize_path_component()` to `src/vlm_pipeline/lib/sanitizer.py`
      - Applied `source_unit_name` + `rel_path` directory segment normalization in
        `src/vlm_pipeline/defs/ingest/ops.py`
    - Result:
      - Both filenames and folder names in MinIO upload keys are unified to ASCII-safe paths

  [DONE] Duplicate notice message scope fix (2026-02-25)
    - Issues:
      - `duplicate_skipped_in_manifest:*` was being recorded on all successful files in the same manifest
    - Actions:
      - Changed update target in `docker/app/dagster_defs.py` from all `successful_assets`
        to only the `duplicate_skips[].duplicate_asset_id` set
      - Retained existing safety guard that only records when `error_message` is empty/NULL
    - Verification:
      - `python3 -m py_compile docker/app/dagster_defs.py` passed

  [PENDING] Existing DB contamination cleanup (2026-02-25)
    - Goal:
      - Clean up `duplicate_skipped_in_manifest:*` messages accumulated from past runs
    - Status:
      - Deferred due to DuckDB write lock held during execution (`pipeline-dagster-1`, python PID 446290)
    - Planned SQL:
      - `UPDATE raw_files SET error_message = NULL, updated_at = CURRENT_TIMESTAMP
         WHERE error_message LIKE 'duplicate_skipped_in_manifest:%';`

  [DONE] Incoming/Archive + DB retention policy update (2026-02-23)
    - Policy changes:
      - Even if `skipped` occurs due to duplicates, if `failed=0` successful files are partially archive-moved
      - Files not moved to archive are removed from `raw_files/image_metadata/video_metadata`
      - Only files actually moved to archive retain `ingest_status=completed`
    - Operational cleanup:
      - Removed duplicate residual records (duplicate_of); retained only completed rows in `raw_files`
      - Deleted `_bak_` tables (`_bak_raw_files`, `_bak_labels`, `_bak_processed_clips`)
    - MotherDuck alignment:
      - Switched default DB to `pipeline_db`
      - `pipeline_db` sync complete; old DB deleted

  [DONE] Video environment metadata + CUDA GPU usage applied (2026-02-20 morning)
    - Added extended columns to video_metadata:
      - environment_type, daynight_type, outdoor_score, avg_brightness, env_method
    - Added `video_env.py`:
      - Places365 CUDA-first inference + heuristic fallback
    - Container:
      - Installed torch/torchvision cu124; applied `gpus: all` to app/dagster
      - Confirmed `torch.cuda.is_available() == True` on both app/dagster
    - Model cache:
      - `VIDEO_ENV_MODEL_DIR=/data/models/places365`
      - `VIDEO_ENV_AUTO_DOWNLOAD=false`
      - Confirmed model reuse without re-download after container recreation
    - Tests:
      - `pytest tests/unit` 46 passed
      - `pytest tests/integration/test_ingest_pipeline.py` 3 passed

  [DONE] Incoming stabilization gate + folder archive move (2026-02-19)
    - auto_bootstrap manifest generation conditions:
      - `AUTO_BOOTSTRAP_STABLE_CYCLES` + `AUTO_BOOTSTRAP_STABLE_AGE_SEC`
      - Generated only after source unit signature stabilization is confirmed
    - Manifest extensions:
      - `source_unit_type`, `source_unit_path`, `source_unit_name`, `stable_signature`
    - Ingest extensions:
      - `source_unit_type=directory`: moves entire folder to archive on full success
      - Auto-avoids archive collision with suffix (`__N`)
      - Bulk updates `raw_files.archive_path` per file after move

  [DONE] Phase A Dual Path alignment
    - compose/.env root: /nas/datasets/projects
    - parallel root: /nas/incoming

  [DONE] Phase B Strict 8 migration
    - Migration applied: 8-table operation (image_metadata, video_metadata added)
    - clip_metadata operational table removed → absorbed into processed_clips

  [DONE] Phase C Code alignment
    - dagster_defs.py multi-root fallback strengthened
    - verify_mvp.sh updated to Strict 8 + Dual Path standards

  [DONE] Phase D Verification
    - INGEST: raw_files=179, video_metadata=179
    - DEDUP: pHash computed and dup_group updated for 3 synthetic images
    - LABEL: labels.asset_id linked via raw_key stem matching (stem_matched=1)
    - PROCESS/BUILD: processed_clips, datasets, dataset_clips created
    - E2E: scripts/verify_mvp.sh success

  [DONE] Refactoring (2026-02-13)
    - Removed data_pipeline_job (file_organize, nas_scan, minio_upload)
    - Consolidated into mvp_stage_job
    - incoming_manifest_sensor/auto_bootstrap_manifest_sensor → mvp_stage_job trigger
    - Reconnected motherduck_sync asset/job
    - Added cursor-based deduplication
    - Full refactoring of src/vlm_pipeline/ (definitions, assets, sensor integrity)

  [DONE] MotherDuck sync failure recovery (2026-02-13)
    - Root cause: `pipeline__motherduck_sync` step stall (run hang) blocking `duckdb_writer=true` queue
    - Action: Injected sync op config into sensor run_config + added `timeout_sec` (default 600 sec)
    - Action: Force fail on timeout (`TimeoutExpired` handling) to prevent infinite wait
    - Operational recovery: Cleaned up stale STARTED runs; queued runs completed normally

  [DONE] Codebase slimming/cleanup (2026-02-13)
    - Applied request criteria: cleaned up unnecessary files excluding `gcp/`, `split_dataset/`, `*.md`
    - Updated `scripts/setup.sh` to pyproject/editable install standard
    - Cleaned global cache/meta files: `__pycache__`, `.pytest_cache`, `.DS_Store`, `._.DS_Store`, `*.pyc`
    - Added runtime artifact paths to `.gitignore`:
      - `docker/airflow/logs/`
      - `docker/app/dagster_home/storage/`
      - `docker/app/dagster_home/history/`
    - Verification:
      - `pytest -q tests/unit` → 43 passed
      - `pytest -q tests/integration` → 6 passed

  [DONE] MotherDuck column integrity recovery (2026-02-13)
    - Issues: `raw_files.phash`, `raw_files.dup_group_id`, `raw_files.raw_bucket` missing
    - Root cause:
      - DEDUP only referenced `source_path`; pHash computation failed after archive move
      - `raw_bucket` NULL residuals from past ingest data
    - Actions:
      - Added `archive_path` priority + MinIO fallback to DEDUP
      - Strengthened `raw_bucket` in INGEST insert/update
      - Added bytes input support to `compute_phash`
      - Backfilled existing data + reran `dedup_job`/`motherduck_sync_job`
    - Result:
      - Local: `phash_filled=12/12`, `dup_group_filled=8/12`, `raw_bucket_null=0/12`
      - MotherDuck: `phash_filled=12/12`, `dup_group_filled=8/12`, `raw_bucket_null=0/12`

  [DONE] clip_metadata removal (2026-02-13)
    - Request: Delete unnecessary clip-series legacy table
    - Actions:
      - Added legacy drop step to sync script
      - `LEGACY_TABLES_TO_DROP = ['clip_metadata']`
      - Reran `motherduck_sync_job` to remove `clip_metadata` from MotherDuck
    - Result:
      - Confirmed `clip_metadata` does not exist in both Local/MotherDuck


┌─────────────────────────────────────────────────────────────────────────────┐
│                         GCP Download Operations (migrated)                  │
└─────────────────────────────────────────────────────────────────────────────┘

  Overview:
    - Buckets: `source-a-rtsp-bucket`
    - Single execution entry:
      - `gcp/download_from_gcs_rclone.py`
      - `gcp/download_from_gcs.sh` (Python engine wrapper)
    - Supported modes:
      - `date-folders`: download by top-level date folder (YYYYMMDD / YYYY-MM-DD)
      - `legacy-range`: existing folder/pattern-based download
    - Backends:
      - `auto|rclone|gcloud`

  Dagster integration:
    - asset: `pipeline/gcs_download_to_incoming`
    - job: `gcs_download_job`
    - schedule: `gcs_download_schedule` (`0 */6 * * *`, `Asia/Seoul`)
    - default download path: `/nas/incoming/gcp`
    - failure policy: if both `rclone/gcloud` are not installed under `auto`, fail immediately
    - zero-byte recovery options:
      - run config: `zero_byte_retries`
      - env: `GCS_ZERO_BYTE_RETRIES` (default 2)
      - behavior: after folder download completes, if zero-byte media found, delete file and re-download folder
        (if retries exceeded, mark that folder as failed, no `_DONE` created)

  Execution examples:
    - date-folder mode
      - `python3 /gcp/download_from_gcs_rclone.py --mode date-folders --download-dir /nas/incoming/gcp`
    - legacy mode
      - `python3 /gcp/download_from_gcs_rclone.py --mode legacy-range --download-dir /nas/datasets/projects/source-a-data`

  Auth/permission check:
    - `gcloud auth list`
    - `gsutil ls gs://source-a-rtsp-bucket/`
    - Required permissions: `storage.objects.list`, `storage.objects.get`


┌─────────────────────────────────────────────────────────────────────────────┐
│                Operational Troubleshooting Runbook (2026-03-16)             │
└─────────────────────────────────────────────────────────────────────────────┘

  1) Symptom: Production pipeline stall (Dagster server recovery)
    - Typical symptoms:
      - Dagster UI inaccessible due to port conflict and stalled processes.
      - Repeated `LOCATION_ERROR`.
    - Actions:
      - Clean up port conflicts and stalled processes to restore UI access.
      - Accessible at http://10.0.0.10:3030/
    - On recurrence, check:
      - `docker logs pipeline-dagster-1 | tail -n 100` for errors.
      - `docker compose restart dagster` to reinitialize the service.
      - `ss -tln | grep 3030` to check port listening status.

  2) Symptom: Sensor operation delay (insufficient data detection sensor scan limit)
    - Typical symptoms:
      - Data detection sensor delays as more folders accumulate.
      - Inadequate `_DONE` marker file verification.
    - Actions:
      - Increased data detection sensor scan limit by `3x (30->100)`.
      - Strengthened stability by accurately determining data copy completion via `_DONE` marker file check.
    - On recurrence, check:
      - `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK` value in `.env` file.
      - Whether `_DONE` file exists inside the date folder in question.

  3) Symptom: Config schema mismatch (DagsterInvalidConfigError)
    - Typical symptoms:
      - Config schema mismatch at code level.
      - Increased system load and reduced log transparency.
    - Actions:
      - Removed unsupported fields (`jpeg_quality`, `max_frames_per_video`, `overwrite_existing`).
    - On recurrence, check:
      - Look for 'unexpected field' in error logs.
      - Fix `run_config` section in `src/vlm_pipeline/defs/.../sensor.py`.

  Future management points:
    - Marker file management: Check that the `_DONE` file is created inside the folder when copying data externally.
    - Monitoring: Periodically verify that data is correctly loaded into DuckDB from running `ingest_job` logs.
    - Server load: Since the scan limit was raised, sensor run time may need adjustment when incoming data volume is very high (thousands or more).


┌─────────────────────────────────────────────────────────────────────────────┐
│                Operational Troubleshooting Runbook (2026-03-12)             │
└─────────────────────────────────────────────────────────────────────────────┘

  1) Symptom: Download stalls at ~10-11% in staging MinIO Console with `Unexpected response, download incomplete`
    - Representative case:
      - `vlm-labels/tmp_data/fall_down/detections/20250711_am_pk_fc_00000000_00014000_00000002.json`
      - Download fails from Console UI.
    - Actual cause:
      - The object itself is not corrupted;
        likely a `Console(9003) -> API(9002)` path issue or browser session/response handling problem.
      - Direct `head/get` to staging API showed the object exists normally and is downloadable.
    - Diagnostic sequence:
      - A. First verify the target is on `staging` endpoint, not `prod`
      - B. Directly verify with `boto3.head_object()` or `get_object()`
      - C. Verify actual downloadability via presigned URL or `download_file()`
      - D. If this passes, do not treat as object corruption; isolate as Console/browser issue
    - Operational standards:
      - Do not recreate or delete objects solely because of Console download failure
      - Distinguish the roles: `9002 = staging API`, `9003 = staging Console`
    - Verification points:
      - If direct API download works, the object is intact
      - Treat problem as a Console layer issue and provide workaround download first

  2) Symptom: Re-testing staging but new tests mix with previous run results
    - Typical symptoms:
      - Old raw/labels/processed objects remain in MinIO
      - Old rows remain in `staging.duckdb`
      - New incoming may not be picked up as expected due to sensor cursor, run history, run_key records
    - Actual cause:
      - Unlike production, staging frequently needs "complete reset after experiment,"
        but without a clear procedure to empty MinIO/DB/Dagster runtime state at once,
        partial state remains, making reproduction difficult.
    - Recommended reset sequence:
      - A. Stop `dagster-staging`
      - B. Delete all objects from staging MinIO endpoint (`10.0.0.36:9002`):
        - `vlm-raw`
        - `vlm-labels`
        - `vlm-processed`
        - `vlm-dataset`
      - C. Delete `docker/data/staging.duckdb`
      - D. Delete entire `docker/data/dagster_home_staging/storage`
      - E. Restart `dagster-staging` if needed
    - Never delete together:
      - `/home/user/mou/staging/incoming`
      - `/home/user/mou/staging/archive`
      Input source folders are retained unless explicitly requested
    - Verification points:
      - staging MinIO object count `0`
      - `staging.duckdb` absent
      - Dagster staging runtime DB absent

  3) Symptom: `clip_to_frame` repeats `ffmpeg_clip_extract_failed: File '/tmp/tmp....mp4' already exists. Overwrite? [y/N]`
    - Typical symptoms:
      - `/tmp/tmp....mp4 already exists` error occurs repeatedly when generating multiple event clips for the same source asset.
      - Logs may misleadingly suggest a timestamp or source video issue.
    - Actual cause:
      - Clip output temp path was being pre-created with `NamedTemporaryFile(delete=False)`.
      - ffmpeg asks for overwrite confirmation when the output file already exists;
        in non-interactive execution, defaults to `N` and exits.
      - Root cause: `pre-creating output temp file` implementation.
    - Permanent fix:
      - Pass only "a temp path string that does not yet exist" to ffmpeg output.
      - Clean up partial temp files on failure.
    - Diagnostic sequence:
      - A. Check if stderr contains `Overwrite? [y/N]`
      - B. Check output temp creation method before ffmpeg input/timestamp
      - C. Verify whether `-y` was added or non-existent temp path approach was applied
    - Operational standards:
      - Avoid pre-creating output temp paths in bulk clip generation code
      - On recurrence, check both the label row count and clip attempt count since the same asset logs may repeat when it has multiple events

  4) Symptom: `400 Request payload size exceeds the limit: 524288000 bytes` error in `clip_timestamp`
    - Representative log:
      - `AUTO LABEL failed: asset_id=...: 400 Request payload size exceeds the limit: 524288000 bytes.`
    - Actual cause:
      - Large original video was loaded directly into memory and placed in the Gemini request payload.
      - Videos exceeding 500MB directly hit the Vertex AI request size limit.
    - Permanent fix:
      - For videos `>450MB`, generate a preview mp4 before calling Gemini:
        - Remove audio
        - Reduce resolution
        - Reduce fps/bitrate
      - If first preview is still too large, retry with lower settings
    - Operational interpretation:
      - Preview path affects not only "error avoidance" but also "input token/audio cost structure"
      - Therefore, when estimating costs, separate the two scenarios:
        - raw source basis
        - current pipeline preview basis
    - Verification points:
      - Large files should no longer immediately reproduce the 524288000 bytes error
      - Confirm preview usage path appears in logs

  5) Operational reference: When Gemini/clip/YOLO cost and time for staging incoming batch need recalculation
    - Reference document:
      - `STAGING_INCOMING_LABELING_REPORT.md`
    - Current document scope:
      - Target: 38 videos loaded in `/home/user/mou/staging/incoming` at the time
      - Gemini 2.5 Pro cost/time
      - Gemini 2.5 Flash cost/time
      - USD/KRW conversion
      - Clip image extraction settings and measured time
      - YOLO-World measured time
    - Operational standards:
      - Current pipeline default model is `gemini-2.5-flash`
      - Report includes `gemini-2.5-pro` for comparison
      - Exchange rate is based on document creation date; only update exchange rate for next batch calculation
    - Recalculation sequence:
      - A. Re-aggregate staging incoming file count/total duration/total size
      - B. Re-aggregate audio count, 450MB/500MB/1GB+ overages
      - C. Re-verify official Gemini unit pricing and current runtime settings (YOLO device, batch, imgsz)
      - D. Recalculate costs using the formula Appendix in the document

  6) Operational principle: Event JSON is source of truth only in `vlm-labels`; do not duplicate-store in `vlm-processed`
    - Background:
      - If `clip_to_frame` duplicates already-existing event JSON in `vlm-labels` to `vlm-processed/.../events/*.json`,
        there are two data sources, complicating path policy and integrity management.
    - Current policy:
      - Gemini event JSON:
        - `vlm-labels/<raw_parent>/events/<video_stem>.json`
      - YOLO detection JSON:
        - `vlm-labels/<raw_parent>/detections/<image_or_clip_stem>.json`
    - Permanent fix:
      - `clip_to_frame` does not copy event JSON to `vlm-processed`
      - When build/dataset stage needs it, read label source from `vlm-labels`
    - Operational meaning:
      - Path purpose is clear when humans browse buckets,
        and the source-of-truth is fixed in one place when downstream looks for label source

┌─────────────────────────────────────────────────────────────────────────────┐
│                Operational Troubleshooting Runbook (2026-03-11)             │
└─────────────────────────────────────────────────────────────────────────────┘

  1) Symptom: `Another SENSOR daemon is still sending heartbeats` repeatedly in `dagster-staging` logs
    - Representative logs:
      - `Another SENSOR daemon is still sending heartbeats`
      - `Another ASSET daemon is still sending heartbeats`
      - `Another QUEUED_RUN_COORDINATOR daemon is still sending heartbeats`
    - Actual cause:
      - Staging shared the same instance state as production `dagster`.
      - Not only `DAGSTER_HOME` needs to be separated; run/event/schedule/local storage must also be separated.
      - When storage is shared, daemon heartbeats recognize each other's processes as "duplicate daemons."
    - Permanent fix:
      - `dagster-staging` must use `DAGSTER_HOME=/app/dagster_home_staging`.
      - Maintain a dedicated staging `dagster.yaml` with all of:
        - `run_storage`
        - `event_log_storage`
        - `schedule_storage`
        - `local_artifact_storage`
        separated to `/data/dagster_home_staging/storage`.
      - Staging DuckDB also separated to `/data/staging.duckdb`.
    - Verification points:
      - After restart, heartbeat collision logs should no longer appear.
      - Sensor ticks should cycle normally in the staging instance.

  2) Symptom: Staging sensor keeps skipping with `DuckDB not found: /data/staging.duckdb`
    - Actual cause:
      - Staging service starts based on `/data/staging.duckdb`; if file is absent, all sensors skip.
      - This state occurs immediately when production/staging DBs are separated but the initial file is not prepared.
    - Operational recovery:
      - To reproduce production state in staging:
        - Clone `pipeline.duckdb -> staging.duckdb`
      - To start fresh staging test:
        - Delete existing `staging.duckdb`
        - Create new DB with schema only re-applied
    - Standards established in this work:
      - While in use, can be started as a production clone.
      - When verification reset is needed, recreate as empty DB with data removed, schema retained.
      - Delete backup files immediately when no longer needed.
    - Verification points:
      - Basic table queries for `raw_files`, `video_metadata`, etc. must succeed.
      - `DuckDB not found` logs should no longer appear.

  3) Symptom: Files exist in host `/home/user/mou/staging/incoming` but staging container cannot see them
    - Typical symptoms:
      - Inside container, `/nas/staging/incoming` only has `.manifests`, no actual videos.
      - `incoming_manifest_sensor` may keep appearing as `no pending`.
    - Actual cause:
      - Default common volume only connects `/home/user/mou/incoming -> /nas/incoming`.
      - Staging-specific paths `/home/user/mou/staging/incoming`, `/home/user/mou/staging/archive` were not separately mounted.
    - Permanent fix:
      - Must add the following bind mounts to `dagster-staging` service:
        - `/home/user/mou/staging/incoming -> /nas/staging/incoming`
        - `/home/user/mou/staging/archive -> /nas/staging/archive`
    - Diagnostic sequence:
      - A. Verify inside container that `/nas/staging/incoming/tmp_data/...` files are actually visible
      - B. Check if `.manifests/pending`, `.manifests/processed` are created
      - C. Check `auto_bootstrap_manifest_sensor`, `incoming_manifest_sensor` logs
    - Operational notes:
      - `waiting for copy stabilization` log is not a failure; it may be waiting for stabilization conditions.
      - `incoming_manifest_sensor skipped: backpressure ... in_flight=2` means the next manifest is queued due to run limit.

  4) Symptom: `clip_timestamp` in staging `auto_labeling_job` fails with `Gemini credentials not found`
    - Representative log:
      - `FileNotFoundError: Gemini credentials not found. Set GEMINI_GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS, or GEMINI_SERVICE_ACCOUNT_JSON.`
    - Actual cause:
      - Credential JSON file exists in container but staging env does not point to that path.
      - Typically, only production `.env` is correct and the same setting is missing from `.env.staging`.
    - Permanent fix:
      - Maintain at minimum these values in staging env:
        - `GEMINI_PROJECT`
        - `GEMINI_LOCATION`
        - `GEMINI_GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/<service-account>.json`
      - If not using `GEMINI_SERVICE_ACCOUNT_JSON`, leave it empty to reduce priority conflicts.
    - Diagnostic sequence:
      - A. Verify credential file exists inside container
      - B. Verify `resolve_gemini_credentials_path()` returns actual file path
      - C. Re-queue `auto_labeling_job` for retry
    - Verification points:
      - `clip_timestamp` should subsequently proceed without credential error.

  5) Operational principle: Hide manual import/experimental assets from production Dagster; expose only in staging
    - Targets:
      - `manual_label_import`
      - `yolo_image_detection`
      - `yolo_detection_sensor`
    - Implementation standard:
      - Branch via env flag in `src/vlm_pipeline/definitions.py`
      - Env vars used:
        - `ENABLE_MANUAL_LABEL_IMPORT`
        - `ENABLE_YOLO_DETECTION`
    - Operational defaults:
      - production: both `false`
      - staging: both `true`
    - Verification points:
      - Corresponding asset/job/sensor should not be visible in production UI/Definitions.
      - Should remain exposed in staging.

  6) Symptom: YOLO service is up but detection is not ready due to model file or dependency issues
    - Current server stack standard:
      - Current implementation is Ultralytics YOLO-World server.
      - Not immediately compatible with MMYOLO `.pth` checkpoints.
    - Operational standards established:
      - Model used: `yolov8l-worldv2.pt`
      - Location: `docker/data/models/yolo/yolov8l-worldv2.pt`
      - env:
        - `YOLO_MODEL_PATH=/data/models/yolo/yolov8l-worldv2.pt`
        - `YOLO_DEFAULT_CLASSES=...`
    - Additional caveats:
      - If `clip` dependency is missing when starting YOLO-World, container can fail at boot.
      - Therefore YOLO image requires `git`, `git+https://github.com/ultralytics/CLIP.git`.
    - Verification points:
      - `pipeline-yolo-1` is `healthy`
      - `/health` shows `model_loaded=true`
      - Model path in `/info` or logs matches expected value

  7) Symptom: `git switch` is blocked after running staging once
    - Typical symptoms:
      - Checkout blocked due to local changes in `runs.db`, `schedules.db`
      - Checkout blocked due to untracked runtime files like `.nux/nux.yaml`, `.telemetry/id.yaml`
    - Actual cause:
      - Staging runtime files are created under the git working tree.
    - Permanent fix:
      - Staging Dagster storage should be kept at `/data/dagster_home_staging/storage`, not inside the repo.
      - Do not switch branches while staging container is running; check staging state and runtime files before switching.
    - Operational notes:
      - Even after storage separation, non-essential files like `.nux`, `.telemetry` may be recreated.
      - Check `git status` before branch switch; safely stop staging container first if needed.

┌─────────────────────────────────────────────────────────────────────────────┐
│                Operational Troubleshooting Runbook (2026-03-10)             │
└─────────────────────────────────────────────────────────────────────────────┘

  1) Symptom: Gemini code exists but `vertexai` / `google.cloud.aiplatform` import fails inside container
    - Typical symptoms:
      - `ModuleNotFoundError: No module named 'google.cloud'`
      - `ModuleNotFoundError: No module named 'vertexai'`
    - Actual cause:
      - Existing Gemini code existed as a separate utility under `src/gemini`,
        not connected to the pipeline common library and Docker dependencies.
    - Permanent fix:
      - Common modules:
        - `src/vlm_pipeline/lib/gemini.py`
        - `src/vlm_pipeline/lib/gemini_prompts.py`
      - Compatibility wrapper:
        - `src/gemini/video.py`
        - `src/gemini/gemini/gemini_api.py`
        - `src/gemini/assets/config.py`
      - Add to dependencies:
        - `docker/app/requirements.txt`
        - `pyproject.toml`
        - add `google-cloud-aiplatform`
      - Rebuild Docker image then restart `app`, `dagster`
    - Additional caveats:
      - Omitting `VIDEO_PROMPT`, `IMAGE_PROMPT` exports from initial wrapper
        can break old import paths.
      - Wrapper must export at minimum these names as-is:
        - `ENV_AUTH`
        - `PROMPT`
        - `PROMPT_VIDEO`
        - `IMAGE_PROMPT`
        - `VIDEO_PROMPT`
    - Verification:
      - `docker exec pipeline-app-1 python3 -c "import vertexai"`
      - `docker exec pipeline-dagster-1 python3 -c "from gemini.assets.config import VIDEO_PROMPT"`
      - Both paths must succeed.

  2) Symptom: Intermediate paths like `vlm-processed/_tmp/...` or `/frames/` remain after video frame extraction rollout
    - Finalized storage rules for this work:
      - Bucket: `vlm-processed`
      - key:
        - `vlm-processed/<raw-prefix>/<video-stem>/<video-stem>_00000001.jpg`
      - Example:
        - `source-a-rtsp-bucket/20251128/fire_235_156/fire_235_156_00000001.jpg`
    - Prohibited rules:
      - No `_tmp/...` prefix
      - No `/frames/` subdirectory
      - No filenames without original stem like `frame_0001...jpg`
    - Implementation principles:
      - Plan frame count/timing using `video_metadata.duration_sec`, `fps`, `frame_count`
      - Actual decoding uses ffmpeg
      - Results stored as rows in `image_metadata`
    - If path rules changed, accompanying cleanup steps:
      - A. Cancel in-progress extraction runs
      - B. Clean up `vlm-processed` bucket
      - C. Delete `video_frame` rows in `image_metadata`
      - D. Reset `video_metadata.frame_extract_status/count/error/extracted_at`
      - E. Re-run extraction
    - Operational notes:
      - `image_metadata.codec` removed as unnecessary for JPEG frames.
      - Key frame extraction metadata: `image_key`, `frame_index`, `frame_sec`, `checksum`, `file_size`.

  3) Symptom: `auto_bootstrap_manifest_sensor` fails with 180-second timeout
    - Representative errors:
      - `DagsterUserCodeUnreachableError`
      - `Deadline Exceeded`
    - Actual cause:
      - Too many source units scanned in one tick
      - **NAS/NFS latency** causing discovery/scan I/O to exceed 180 seconds
      - Hidden entries included in discovery
      - Cursor advancement and scan budget not managed conservatively relative to actual throughput
    - Actions taken:
      - Excluded hidden entries (`.Trash-1000`, `.DS_Store`, etc.)
      - Applied safety cap of `10` processed units per tick
      - **Split discovery into per-tick upper bound**: `AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES` (0=unlimited, recommend 15–30 on NAS latency)
      - Discovery budget capped at 60 seconds to provide gRPC headroom
      - Added `discovery_elapsed`, `scan_elapsed`, `processed_units`, `budget` logs
    - **Recommended settings on NAS latency**:
      - `AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES=20` (or 15–30)
      - `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK=3`
      - `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS=300` (Dagster daemon/user code server setting)
    - Operational recovery criteria:
      - Latest sensor tick must finish within gRPC timeout
      - `Deadline Exceeded` must not recur in daemon logs
    - Standard diagnostic sequence on recurrence:
      - A. Check if hidden entries are mixed into discovery
      - B. Check max units per tick, discovery max top entries values
      - C. Check processed_units/budget in recent tick logs
      - D. If NAS latency, apply recommended settings above and raise `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS`

  4) Symptom: What to consider together when full `image_metadata` deletion and `vlm-processed` wipe are needed
    - Actually performed in this work:
      - Stop Dagster jobs
      - Full DELETE of `image_metadata`
      - Wipe `vlm-processed` MinIO bucket
    - Caveats:
      - Wiping `vlm-processed` may also remove processed clip objects, not just extracted frames.
      - Wiping `image_metadata` removes all existing frame/image metadata.
      - If `video_metadata.frame_extract_status='completed'` remains, re-extraction may be blocked.
    - Therefore, if the goal is "full reset then re-extract," also consider:
      - A. Delete `image_metadata`
      - B. Clean up `vlm-processed` bucket
      - C. Reset `video_metadata.frame_extract_*`
      - D. Re-run extraction
    - In this operation, only the user-requested scope was executed first;
      subsequent path/filename change work also reset `video_metadata.frame_extract_*` together.

  5) Dagster operational structure notes (as of 2026-03-10)
    - Automatic flows:
      - `incoming/autobootstrap -> ingest_job`
      - `dedup_backlog_sensor -> dedup_job`
      - `video_frame_extract_sensor -> video_frame_extract_job`
    - Lineage principle:
      - `processed_clips` direct upstream is `labeled_files`
      - `motherduck_sync` connects all actual sync target assets as upstream
    - Future code:
      - sensor/job/asset for frame extraction based on `processed_clips` exists only as commented blocks in code
      - Manual extraction design based on `archive_path_prefixes` is also only commented-out design and currently inactive


┌─────────────────────────────────────────────────────────────────────────────┐
│                Operational Troubleshooting Runbook (2026-03-05)             │
└─────────────────────────────────────────────────────────────────────────────┘

  1) Symptom: `raw_files` table does not exist in `ingested_raw_files`
    - Representative logs:
      - `Catalog Error: Table with name raw_files does not exist`
      - `SELECT * FROM raw_files ...`
    - Root cause:
      - DuckDB schema initialization not guaranteed at ingest start time.
      - Immediate query occurs when DB file exists but tables do not.
    - Permanent fix (code):
      - `src/vlm_pipeline/resources/duckdb.py`
        - Added `DuckDBResource.ensure_schema()`
      - `src/vlm_pipeline/defs/ingest/assets.py`
        - Call `db.ensure_schema()` immediately after `ingested_raw_files()` starts
    - Immediate operational recovery:
      - Apply schema manually if needed:
        - `python3 - <<'PY'`
        - `import duckdb; from pathlib import Path`
        - `ddl = Path('/src/vlm/vlm_pipeline/sql/schema.sql').read_text(encoding='utf-8')`
        - `conn = duckdb.connect('/data/pipeline.duckdb'); conn.execute(ddl); conn.close()`
        - `PY`

  2) Symptom: `QUEUED` accumulation + long-running `STARTED` holding slot
    - Root cause:
      - Concurrency slot contention based on `duckdb_writer=true` tag.
      - If one long-running `STARTED` holds the slot, enqueue keeps accumulating.
    - Permanent fix (code/config):
      - Operate `stuck_run_guard_sensor`:
        - Auto-cancel long-stalled `STARTED` runs
        - Optional auto-requeue
      - `incoming_manifest_sensor` per-tick generation upper bound:
        - `INCOMING_SENSOR_MAX_NEW_RUN_REQUESTS_PER_TICK`
    - Immediate operational recovery:
      - Cancel/mark stuck runs as canceled, then drain queued.
      - Restart Dagster if needed:
        - `docker compose up -d --force-recreate dagster`

  3) Standard diagnostic sequence on recurrence
    - A. Check run states:
      - Verify count/run_id for `STARTED`, `CANCELING`, `QUEUED`
    - B. Check DB schema:
      - Verify `raw_files` is included in `SHOW TABLES` result
    - C. Check sensor states:
      - Verify `incoming_manifest_sensor`, `stuck_run_guard_sensor` are RUNNING
    - D. Action sequence:
      - (1) Ensure schema → (2) Clean stuck runs → (3) Normalize queue → (4) Confirm new runs succeed

  4) Recommended operational environment variables
    - `INCOMING_SENSOR_MAX_NEW_RUN_REQUESTS_PER_TICK=2`
    - `STUCK_RUN_GUARD_ENABLED=true`
    - `STUCK_RUN_GUARD_INTERVAL_SEC=120`
    - `STUCK_RUN_GUARD_TIMEOUT_SEC=10800`
    - `STUCK_RUN_GUARD_MAX_CANCELS_PER_TICK=1`
    - `STUCK_RUN_GUARD_AUTO_REQUEUE_ENABLED=true`
    - `STUCK_RUN_GUARD_MAX_REQUEUES_PER_TICK=1`
    - `STUCK_RUN_GUARD_TARGET_JOBS=mvp_stage_job,ingest_job,motherduck_sync_job`


┌─────────────────────────────────────────────────────────────────────────────┐
│                Operational Troubleshooting Runbook (2026-03-09)             │
└─────────────────────────────────────────────────────────────────────────────┘

  1) Symptom: Duplicates remain in `raw_files` despite checksum-based deduplication
    - Typical symptoms:
      - Multiple rows with the same checksum but different `raw_key`
      - `archive_path` appears different but actual file content is identical
      - Duplicate marker in `error_message` of some keeper rows is incomplete
    - Actual cause:
      - Application logic uses `find_by_checksum()` for global deduplication,
        but the production DuckDB table had no `UNIQUE(checksum)` constraint.
      - Schema was split within the repository:
        - `src/python/common/schema.sql` -> `checksum VARCHAR`
        - `src/vlm_pipeline/sql/schema.sql` -> `checksum VARCHAR UNIQUE`
      - Due to `CREATE TABLE IF NOT EXISTS` approach, incorrectly created tables were not auto-corrected by current code.
      - Additionally, `350` checksum drift cases found from archive-source re-hashing,
        leaving room for app-level deduplication query misses.
    - Diagnostic sequence:
      - A. Separate `raw_key` duplicates from `checksum` duplicates.
      - B. Check `archive_path` duplicates separately.
      - C. Verify that `UNIQUE(checksum)` constraint actually exists in the production DB.
      - D. Do not trust DB checksum as-is; re-hash archive source files.
    - Recovery procedure performed:
      - Re-hashed `946` suspected cases using `scripts/recompute_archive_checksums.py`.
      - Results:
        - unchanged `596`
        - checksum changed `350`
        - missing `0`
      - Cleaned duplicate groups using `scripts/cleanup_duplicate_assets.py`.
      - DB backup before cleanup:
        - `docker/data/pipeline.pre_dedup_20260309_063521.duckdb`
      - State before cleanup:
        - `raw_files=15922`
        - checksum duplicate groups `456`
        - duplicate rows `490`
      - State after cleanup:
        - `raw_files=15432`
        - checksum duplicates `0`
        - `raw_key` duplicates `0`
        - `archive_path` duplicates `0`
    - Operational principle:
      - Checksum dedup must align both "code logic" and "DB constraint" simultaneously.
      - If either is missing, duplicates can accumulate.

  2) Symptom: `duplicate_skipped_in_manifest:<filename>` marker is incomplete in some duplicate groups
    - Expected rule:
      - When a duplicate occurs, the surviving keeper row's `error_message`
        should contain the skipped filename for each occurrence.
    - Problem cause:
      - When duplicates with the same basename are mixed multiple times,
        the marker merge process was condensing the count.
    - Actions:
      - `src/vlm_pipeline/defs/ingest/duplicate.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - Fixed marker merge logic at both points to preserve basename repetition as-is.
    - Verification:
      - Cross-referenced `456` duplicate groups before cleanup against backup DB.
      - After cleanup, surviving keeper row marker mismatch `0`.
    - Operational meaning:
      - Duplicate skip history can now be reconstructed from `error_message` alone.

  3) Symptom: `YYYY/MM` prefix mixed into MinIO raw paths
    - Representative examples:
      - `vlm-raw/2026/03/source-b-event-bucket/...`
      - `vlm-raw/2026/03/source-c-event-bucket/...`
    - Cause:
      - Past ingest logic prepended `datetime.now().strftime("%Y/%m")` to `raw_key`.
    - Current correct rule:
      - `raw_key = <source_unit_name>/<rel_path>`
      - Examples:
        - `source-b-event-bucket/20260204/fire_1_131000.mp4`
        - `source-c-event-bucket/2026-01-26/falldown_229_438.mp4`
    - Recovery procedure:
      - Copy/move old prefix objects to new prefix
      - Update DuckDB `raw_files.raw_key` with same standard
      - Delete old prefix objects
    - Recurrence prevention:
      - Current ingest path in `src/vlm_pipeline/defs/ingest/ops.py` does not create `YYYY/MM` prefix.
      - Exception may still occur if a separate script directly manipulates MinIO keys.

  4) Symptom: archive / MinIO / DuckDB / MotherDuck counts differ from each other
    - Actual pattern observed in this issue:
      - Immediately after duplicate cleanup:
        - DuckDB `15432`
        - MinIO `15432`
        - MotherDuck `15922`
        - actual archive file count `15487`
      - i.e., local DB and MinIO matched, but archive and MotherDuck were lagging.
    - Alignment method:
      - A. Check all files in archive not present in DB
      - B. Classify excess files into three types:
        - operational marker (`_DONE`)
        - junk files (`.DS_Store`)
        - actual data files
      - C. Apply rules:
        - `_DONE` -> retain
        - `.DS_Store` -> delete
        - actual data files -> determine duplicate by checksum
          - if duplicate: delete
          - if unique: reflect in MinIO + DuckDB + MotherDuck
    - Results of this run:
      - archive excess `55`
      - `_DONE` `49`
      - `.DS_Store` `4`
      - actual media `2`
      - `weapon_298_020.mp4` -> unique, reflected as `completed`
      - `weapon_298_03.mp4` -> unique but corrupted MP4, reflected as `failed`
    - MotherDuck sync:
      - `python3 /src/python/local_duckdb_to_motherduck_sync.py --db pipeline_db --local-db-path /data/pipeline.duckdb --share-update MANUAL --tables raw_files video_metadata`
    - Final operational reference values:
      - DuckDB `raw_files=15434`
      - MinIO objects `15434`
      - MotherDuck `raw_files=15434`
      - archive data file count `15434`
      - archive total physical file count `15483`
      - difference cause: `_DONE` marker `49` intentionally retained
    - Operational rule:
      - "Integrity" should be compared against archive data file count,
        not archive total physical file count.

  5) Symptom: WAL replay issue on restart after DuckDB file replacement
    - Cause:
      - DB file was replaced but existing `pipeline.duckdb.wal` remained,
        causing stale WAL to attempt re-application to new DB.
    - Immediate action:
      - Stop `app`
      - Store stale WAL separately:
        - `docker/data/pipeline.duckdb.wal.pre_dedup_stale_20260309_0640`
    - Permanent fix:
      - Added to `scripts/cleanup_duplicate_assets.py`:
        - temp DB `CHECKPOINT`
        - WAL backup/move before and after DB swap
    - Operational principle:
      - DuckDB file replacement must always be performed with service stopped,
        and existing WAL presence must be checked together.

  6) Symptom: `vlm-labels`, `vlm-processed`, `vlm-dataset` buckets not auto-created
    - Cause:
      - MinIO does not auto-create buckets on write.
      - `ensure_bucket()` helper existed in code but was not called in actual upload/copy paths.
    - Actions:
      - Strengthened `src/vlm_pipeline/resources/minio.py`.
      - Modified to call `_ensure_bucket_once()` before `upload()`, `upload_fileobj()`, `copy()`.
      - Used ensured bucket cache + lock to reduce repeated call overhead within the same process.
    - Result:
      - Current buckets:
        - `vlm-raw`
        - `vlm-labels`
        - `vlm-processed`
        - `vlm-dataset`
    - Operational principle:
      - Subsequent label/process/build stages now guarantee their buckets on first write.

  7) Symptom: 2 `STARTED` runs in Dagster holding slot for over 1 hour
    - Confirmed runs:
      - `06349bfd-d236-4e39-b2d9-c9447c48d4ca`
      - `975ca406-3ae2-46d3-8b2a-bf2984850039`
    - Actual state:
      - Shown as `STARTED` in UI/instance but no actual worker process inside container.
      - Last event also stopped at duplicate skip log.
      - This state was holding sensor backpressure (`in_flight`).
    - Actions:
      - Processed `CANCELING` -> `CANCELED` via DagsterInstance API.
    - Verification:
      - active runs `0`
      - Sensor restored to state where it can issue new runs.

  8) Today's operational checkpoint
    - Local DuckDB, MinIO, MotherDuck, archive data file counts are all `15434`.
    - `video_metadata` is `15433`.
    - `failed raw_files` is `1` (`weapon_298_03.mp4`, ffprobe failure).
    - checksum duplicates `0`.
    - `raw_key` duplicates `0`.
    - archive `_DONE` markers `49` intentionally retained.
    - Key files added/modified today:
      - `scripts/recompute_archive_checksums.py`
      - `scripts/cleanup_duplicate_assets.py`
      - `src/vlm_pipeline/defs/ingest/duplicate.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/resources/minio.py`


┌─────────────────────────────────────────────────────────────────────────────┐
│                 Staging Architecture and Troubleshooting Runbook (2026-03-13) │
└─────────────────────────────────────────────────────────────────────────────┘

  1) Staging-specific Dispatch logic and dynamic pipeline branching
    - Target: staging pipeline
    - Goal: "conditional processing (dispatch) + dynamic parallel pipeline execution" differentiated from production
    - Precise pipeline flow:
      - [Stage 1: Source wait] Data enters `incoming` and waits (auto Ingest disabled)
      - [Stage 2: Dispatch request] Detect specific JSON file (`.dispatch/pending/`)
      - [Stage 3: Ingest & first move simultaneous] Load folder metadata into DuckDB, upload to MinIO (`vlm-raw`), then immediately move from `incoming` to final `archive`
      - [Stage 4: AI pipeline dynamic execution] Run subsequent pipelines (YOLO, Gemini, etc.) sequentially according to JSON's `run_mode` (`gemini`, `yolo`, `both`)
    - Extraction settings: supports multiple frame extraction settings such as `current`, `dense`
    - History tracking: `staging_dispatch_requests` DB table added

  2) Symptom: `raw_ingest` stage stalls for over 1 hour (Stuck)
    - Symptom: Pipeline stalls when uploading large files over 1GB despite copy completion in `incoming` directory (18+ min/file)
    - Cause: Using SpooledTemporaryFile for 1-pass loading optimization caused local disk I/O and memory copy, failing to leverage boto3's native multipart parallel upload advantage and creating a bottleneck with single-stream upload
    - Actions:
      - Disabled tempfile creation by setting `include_file_stream=False` in `load_video_once`
      - Added `upload_file` method to `minio.py` to enable boto3 S3 native `upload_file` (multi-threaded multipart chunk concurrent read)
      - Switched `_upload_single` in `ops.py` to directly reference NAS source path for parallel surging upload

  --- 2026-03-17 auto-recorded (detailed summary) ---
  1) `.agent/skill` recovery and reconnection of local automation tools
    - Problem: `.agent/skill` folder disappeared, making local automation tools like `daily_worklog`, `duckdb_staging_wiper`, `staging_reset` unusable.
    - Cause: When converting `.agent/` to gitignore and un-tracking, the skill directory was actually deleted locally as well.
    - Actions:
      - Restored the entire `.agent/skill/` from previous commit history to make the local-only skill set usable again.
      - Recovered file and directory structure so `daily_worklog`, `duckdb_staging_wiper`, `staging_reset` scripts work again.
      - Going forward, `.agent` is excluded from repository tracking but continues to be used for local work automation.
    - Related files:
      - `.agent/skill/daily_worklog/SKILL.md`
      - `.agent/skill/daily_worklog/scripts/daily_worklog.py`
      - `.agent/skill/duckdb_staging_wiper/SKILL.md`
      - `.agent/skill/duckdb_staging_wiper/scripts/wipe_staging_db.sh`
      - `.agent/skill/staging_reset/SKILL.md`
      - `.agent/skill/staging_reset/scripts/reset_staging.sh`
  2) Linear MCP and daily log format cleanup
    - Problem: Difficult to use Linear MCP as shared tool outside Cursor IDE; auto-generated WORKLOG/CLAUDE records were file-listing-centric and hard to read as actual problem-solving history.
    - Cause: MCP config was not reflected in the project shared master, and daily worklog automation script was commit-listing-centric rather than problem/cause/action-centric.
    - Actions:
      - Organized shared MCP config reuse across multiple IDEs using `.agent/mcp/mcp_config.json` and sync script.
      - Confirmed direction to manually correct and improve automation for `WORKLOG.md` and `CLAUDE.md` to be problem/cause/action-centric format going forward.
      - Cleaned up `.env.example` with empty values and Vertex prompt normal exclusion standard to reduce deployment/documentation confusion.
    - Related files:
      - `.agent/mcp/mcp_config.json`
      - `.agent/mcp/sync_mcp.sh`
      - `.env.example`
      - `WORKLOG.md`
      - `CLAUDE.md`
  3) Verification points on recurrence
    - If `.agent/`-related issues reoccur, check both the git un-tracking commit and whether the local directory was actually deleted.
    - If daily worklog automation reverts to simple summaries, first check whether the `.agent/skill/daily_worklog/scripts/daily_worklog.py` template changed.

  --- 2026-03-18 auto-recorded (detailed summary) ---
  1) Staging spec-based architecture newly established
    - Problem: To safely validate auto labeling requirements and experimental conditions in staging, a spec-centric execution layer separated from the production path was needed.
    - Cause: Existing structure was oriented around shared ingest/label/process flows, making it difficult to accommodate spec-unit execution conditions, QA scenarios, and separate sensors in staging.
    - Actions:
      - Added `definitions_staging.py`, `duckdb_spec.py`, `defs/spec/assets.py`, `defs/spec/sensor.py` to configure staging-specific spec flow.
      - Strengthened compose and app config so staging Dagster definitions and workspace can be loaded separately at Docker level.
      - Extended DuckDB schema and labeling resources to understand the spec execution context as well.
    - Related files:
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/spec/assets.py`
      - `src/vlm_pipeline/defs/spec/sensor.py`
      - `src/vlm_pipeline/resources/duckdb_spec.py`
      - `docker/app/dagster_defs_staging.py`
      - `docker/app/workspace_staging.yaml`
  2) Staging QA scripts and operational support tools added
    - Problem: To repeatedly validate the staging spec flow, there were insufficient scripts and documentation to manually verify dispatch/QA/wait logic.
    - Cause: Verification procedures were not organized into official scripts and documentation, making the same test hard to reproduce without human dependency.
    - Actions:
      - Added scripts for staging QA execution, incoming drop, and expected condition waiting.
      - Wrote spec requirements, UI, and integrated design documents together as reference points for future staging restructuring.
    - Related files:
      - `scripts/staging_qa_run_cycle.sh`
      - `scripts/staging_qa_safe_drop_incoming.sh`
      - `scripts/staging_qa_wait_expect.py`
      - `scripts/staging_test_dispatch.py`
      - `auto_labeling_requirements_spec.md`
      - `auto_labeling_unified_spec.md`

  --- 2026-03-19 auto-recorded (detailed summary) ---
  1) Staging-specific spec asset/sensor separation
    - Problem: Staging experimental spec flow was too close to the production shared path, making responsibility boundaries unclear during repeated tests.
    - Cause: Shared spec asset/sensor and staging experimental flow shared the same layer, making it difficult to independently handle run tags and sensor behavior needed only in staging.
    - Actions:
      - Added `defs/spec/staging_assets.py`, `defs/spec/staging_sensor.py` to separate staging-specific execution path.
      - Adjusted `definitions_staging.py`, `defs/dispatch/sensor.py`, `lib/spec_config.py` to interpret staging spec execution config in a separated manner.
    - Related files:
      - `src/vlm_pipeline/defs/spec/staging_assets.py`
      - `src/vlm_pipeline/defs/spec/staging_sensor.py`
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `src/vlm_pipeline/lib/spec_config.py`
  2) Frame-level verification and bench script reinforcement
    - Problem: Insufficient tools to separately verify video frame extraction and YOLO integration quality in staging.
    - Cause: Inadequate frame-unit extraction helpers, bench scripts, and unit tests made it difficult to quickly isolate frame generation stage issues.
    - Actions:
      - Strengthened `video_frames.py` and process/label/yolo assets; added bench scripts and unit tests.
      - Moved spec md drafts to cleanup targets while simplifying structure to focus on actual code paths.
    - Related files:
      - `scripts/staging_video_extract_yolo_bench.py`
      - `src/vlm_pipeline/lib/video_frames.py`
      - `tests/unit/test_video_frames.py`

  --- 2026-03-20 auto-recorded (detailed summary) ---
  1) Staging Vertex chunking and event frame caption adoption
    - Problem: Criteria for selecting representative frames for captioning in long videos or event segments were weak, and helpers for improving staging VQA quality were insufficient.
    - Cause: Event-level caption and frame/image-level caption flows were not finely separated, and the helper layer leveraging VertexAI exclusively for staging was insufficient.
    - Actions:
      - Introduced and extended `staging_vertex.py`; reflected event frame caption, relevance judgment, and chunking processing in label/process assets.
      - Updated DuckDB storage layer and schema accordingly; added related helpers/unit tests.
    - Related files:
      - `src/vlm_pipeline/lib/staging_vertex.py`
      - `src/vlm_pipeline/defs/label/assets.py`
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_labeling.py`
      - `tests/unit/test_staging_vertex_helpers.py`
  2) Local-only asset tracking scope cleanup
    - Problem: Local-only assets like `doc_2` notes and `.agent/skill` remaining in the collaboration repository history could appear mixed with actual code changes.
    - Cause: Local-only files remained in Git tracking scope, and the boundary between the team repository and personal environment could blur again.
    - Actions:
      - Added `doc_2` to `.gitignore`.
      - Removed `.agent/skill` tracking from the repository to re-establish as local-only operation.

  --- 2026-03-23 auto-recorded (detailed summary) ---
  1) Production DuckDB `image_metadata__migrated` error response
    - Problem: `image_metadata__migrated does not exist` error repeatedly occurring in production Dagster `clip_to_frame` stage, causing consecutive run failures from the middle.
    - Cause: Judged not to be a `clip_to_frame` logic issue alone; likely the production DuckDB `image_metadata` migration or catalog state became confused, leaving temporary table references.
    - Actions:
      - Reduced runtime risk behavior of `ensure_schema()` and loosened schema correction logic.
      - Added `scripts/repair_image_metadata_schema.py` for production DB state inspection/recovery.
      - Proceeded with production Dagster restart, stale run cleanup, and re-execution.
      - Left production DB file corruption possibility as a separate risk with offline recovery path prepared.
    - Related files:
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/sql/schema.sql`
      - `scripts/repair_image_metadata_schema.py`
  2) Staging dispatch JSON format and branching rules overhaul
    - Problem: Dispatch JSON format passed in staging tests differed from what code expected, making it difficult to create desired execution flows per `bbox`, `captioning`, `image classification` combination.
    - Cause: Existing was `outputs`/`run_mode`-centric, while actual test input had changed to `categories`, `classes`, `labeling_method`-centric.
    - Actions:
      - Added `staging_dispatch.py` to interpret JSON based on `categories`, `classes`, `labeling_method`.
      - Redefined routing rules: `bbox`/`image classification` only → YOLO-only; `captioning+bbox` → VertexAI captioning + YOLO together.
      - Added branching: if `필요없음` ("not needed") or `라벨링필요없음` ("no labeling needed") string values are present in the dispatch JSON, only perform archive move and MinIO upload, skip labeling.
    - Related files:
      - `src/vlm_pipeline/lib/staging_dispatch.py`
      - `src/vlm_pipeline/lib/env_utils.py`
      - `src/vlm_pipeline/lib/spec_config.py`
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
  3) Staging ingest flow redefined based on trigger JSON
    - Problem: Even without trigger JSON, auto-scan of incoming or `archive_pending` bypass path was active in staging, potentially causing unintended ingest/uploads.
    - Cause: auto-bootstrap, dispatch, and ingest sensor intervening simultaneously would process inputs without trigger JSON, or `archive_pending` was being unnecessarily maintained in staging.
    - Actions:
      - Blocked auto-bootstrap path so staging only waits in `incoming` when no trigger JSON is present.
      - Excluded `archive_pending` from staging dispatch flow.
      - Organized so only `incoming -> archive -> MinIO upload` flow runs when trigger JSON is present.
      - Repeatedly initialized `.dispatch`, `.manifests`, staging MinIO, DuckDB for iterative verification.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/sensor_incoming_mover.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`
      - `src/vlm_pipeline/defs/ingest/assets.py`
      - `src/vlm_pipeline/defs/ingest/sensor_bootstrap.py`
      - `src/vlm_pipeline/defs/ingest/sensor_incoming.py`
  4) Staging YOLO execution order guarantee
    - Problem: `raw_ingest` and `yolo_image_detection` started almost simultaneously, causing ordering issue where `YOLO: no target images` log appeared before frame generation.
    - Cause: In staging too, the shared YOLO asset was directly connected to job selection, allowing it to start regardless of frame generation completion.
    - Actions:
      - Separated staging-specific YOLO asset to ensure `raw_ingest -> clip/frame generation -> yolo_image_detection` order.
      - Verified behavior by restarting staging Dagster, re-running canceled runs, and re-submitting trigger JSON.
    - Related files:
      - `src/vlm_pipeline/defs/yolo/assets.py`
      - `src/vlm_pipeline/defs/yolo/staging_assets.py`
      - `src/vlm_pipeline/definitions_staging.py`

  --- 2026-03-25 auto-recorded (detailed) ---
  1) Production/schema layer maintenance
    - Problem: Changes included important modifications to DuckDB schema correction or catalog stability, requiring reduction of schema mismatch recurrence risk during runtime.
    - Cause: DuckDB-related changes go beyond simple column additions; `ensure_schema`, ingest storage paths, and schema DDL are interlinked, so partial fixes can leave different states in production/test environments.
    - Actions:
      - Organized DuckDB-related resources, ingest storage logic, and schema definitions together to consistently align runtime schema correction flow.
      - Prepared manual recovery procedures and inspection scripts for immediate use during operational incidents.
    - Related files:
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/sql/schema.sql`
  2) Staging dispatch and ingest flow cleanup
    - Problem: trigger JSON presence, incoming wait, archive move, MinIO upload order in staging could behave differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON can be auto-processed, or archive order can deviate from expectations.
    - Actions:
      - Adjusted dispatch-related sensor, archive helper, and ingest sensor together to clearly separate trigger conditions and move order in staging.
      - Organized branching rules so wait/move/upload flow differs based on trigger JSON presence.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor_run_status.py`
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`
      - `src/vlm_pipeline/defs/ingest/sensor_bootstrap.py`
      - `src/vlm_pipeline/defs/ingest/sensor_incoming.py`
      - `... and 1 more`
  3) Staging spec module and sensor separation
    - Problem: Mixing staging experimental spec flow with shared pipeline flow could blur test repeatability and responsibility boundaries.
    - Cause: If staging-specific sensor, asset, and spec config are insufficiently separated, production and test paths share the same job/asset definitions.
    - Actions:
      - Separated staging-specific definitions, spec asset, and sensor layers to maintain test flow independently.
      - Corrected spec-related DB storage structure and config interpretation paths together.
    - Related files:
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/spec/assets.py`
      - `src/vlm_pipeline/defs/spec/sensor.py`
      - `src/vlm_pipeline/defs/spec/staging_assets.py`
      - `src/vlm_pipeline/defs/spec/staging_sensor.py`
  4) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests are present, YOLO could run too early or conflict with other routing.
    - Cause: YOLO must run after frame generation, but could independently start first due to dispatch selection or shared asset connection structure.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per bbox, image classification, captioning combination.
    - Related files:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/lib/video_frames.py`
  5) Local-only asset and documentation/automation cleanup
    - Problem: Local notes, personal skills, and env example files mixed with shared repository history could make collaboration scope ambiguous.
    - Cause: Files meaningful only in personal environments accumulate unnecessary changes in repository history, and automation documents can deviate from actual operational state.
    - Actions:
      - Organized `.agent`, `doc_2`, env examples, and daily worklog-related files as tracked/untracked per purpose.
      - Calibrated automation scripts and document format to actual operational/test experience.
    - Related files:
      - `.env.example`
  Key file changes:
    - `docker/docker-compose.yaml`
    - `src/vlm_pipeline/definitions.py`
    - `src/vlm_pipeline/defs/dispatch/sensor.py`
    - `src/vlm_pipeline/defs/ingest/assets.py`
    - `src/vlm_pipeline/defs/ingest/ops.py`
    - `src/vlm_pipeline/defs/label/assets.py`
    - `src/vlm_pipeline/defs/process/assets.py`
    - `src/vlm_pipeline/defs/spec/assets.py`
    - `src/vlm_pipeline/defs/spec/sensor.py`
    - `src/vlm_pipeline/defs/spec/staging_assets.py`
    - `src/vlm_pipeline/defs/spec/staging_sensor.py`
    - `src/vlm_pipeline/defs/sync/sensor.py`
    - `... and 3 more`
  Related commit summary:
    - [7593105a] fix: add pre-labeled source stem inference pattern
    - [b89f4a79] feat: staging dispatch refactoring and pre-labeled data ingestion path addition
    - [9837bf41] refactor and stabilize production/staging ingest/dispatch structure

  --- 2026-03-26 auto-recorded (detailed) ---
  1) Production/schema layer maintenance
    - Problem: Changes included important modifications to DuckDB schema correction or catalog stability, requiring reduction of schema mismatch recurrence risk during runtime.
    - Cause: DuckDB-related changes go beyond simple column additions; `ensure_schema`, ingest storage paths, and schema DDL are interlinked, so partial fixes can leave different states in production/test environments.
    - Actions:
      - Organized DuckDB-related resources, ingest storage logic, and schema definitions together to consistently align runtime schema correction flow.
      - Prepared manual recovery procedures and inspection scripts for immediate use during operational incidents.
    - Related files:
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/resources/duckdb_labeling.py`
      - `src/vlm_pipeline/sql/schema.sql`
  2) Staging dispatch and ingest flow cleanup
    - Problem: trigger JSON presence, incoming wait, archive move, MinIO upload order in staging could behave differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON can be auto-processed, or archive order can deviate from expectations.
    - Actions:
      - Adjusted dispatch-related sensor, archive helper, and ingest sensor together to clearly separate trigger conditions and move order in staging.
      - Organized branching rules so wait/move/upload flow differs based on trigger JSON presence.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/defs/dispatch/staging_agent_sensor.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`
  3) Staging spec module and sensor separation
    - Problem: Mixing staging experimental spec flow with shared pipeline flow could blur test repeatability and responsibility boundaries.
    - Cause: If staging-specific sensor, asset, and spec config are insufficiently separated, production and test paths share the same job/asset definitions.
    - Actions:
      - Separated staging-specific definitions, spec asset, and sensor layers to maintain test flow independently.
      - Corrected spec-related DB storage structure and config interpretation paths together.
    - Related files:
      - `src/vlm_pipeline/definitions_staging.py`
  4) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests are present, YOLO could run too early or conflict with other routing.
    - Cause: YOLO must run after frame generation, but could independently start first due to dispatch selection or shared asset connection structure.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per bbox, image classification, captioning combination.
    - Related files:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/defs/yolo/assets.py`
      - `src/vlm_pipeline/lib/video_frames.py`
  5) Local-only asset and documentation/automation cleanup
    - Problem: Local notes, personal skills, and env example files mixed with shared repository history could make collaboration scope ambiguous.
    - Cause: Files meaningful only in personal environments accumulate unnecessary changes in repository history, and automation documents can deviate from actual operational state.
    - Actions:
      - Organized `.agent`, `doc_2`, env examples, and daily worklog-related files as tracked/untracked per purpose.
      - Calibrated automation scripts and document format to actual operational/test experience.
    - Related files:
      - `.env.example`
      - `.gitignore`
  Key file changes:
    - `docker/app/dagster.yaml`
    - `docker/docker-compose.yaml`
    - `src/vlm_pipeline/definitions.py`
    - `src/vlm_pipeline/defs/dedup/assets.py`
    - `src/vlm_pipeline/defs/dedup/sensor.py`
    - `src/vlm_pipeline/defs/dispatch/staging_agent_sensor.py`
    - `src/vlm_pipeline/defs/ingest/ops.py`
    - `src/vlm_pipeline/defs/process/assets.py`
    - `src/vlm_pipeline/defs/process/sensor.py`
    - `src/vlm_pipeline/defs/sync/sensor.py`
    - `src/vlm_pipeline/defs/yolo/assets.py`
    - `src/vlm_pipeline/resources/duckdb_base.py`
    - `... and 3 more`
  Related commit summary:
    - [e4ddebee] feat: staging API integration and labeling pipeline stabilization
    - [a7586ce3] Merge pull request #26 from hoonikooni/feature/sanghoon

  --- 2026-03-30 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: trigger JSON presence, incoming wait, archive move, MinIO upload order in staging could behave differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON can be auto-processed, or archive order can deviate from expectations.
    - Actions:
      - Adjusted dispatch-related sensor, archive helper, and ingest sensor together to clearly separate trigger conditions and move order in staging.
      - Organized branching rules so wait/move/upload flow differs based on trigger JSON presence.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/production_agent_sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor_incoming_mover.py`
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/defs/dispatch/staging_agent_sensor.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`
      - `... and 4 more`
  2) Staging Vertex/VQA captioning structure reinforcement
    - Problem: In staging, the storage responsibility for video event captions and image/VQA captions could be unseparated, or frame selection criteria could be unclear.
    - Cause: Captioning-related changes touch both label/event and image/frame levels; if routing and storage column meanings are not organized together, downstream queries become confused.
    - Actions:
      - Modified Vertex helper and process asset together to separate responsibility of event caption and image caption.
      - Organized frame relevance judgment, top-1 selection, and image caption storage flow by staging standard.
    - Related files:
      - `src/vlm_pipeline/defs/label/assets.py`
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/lib/staging_vertex.py`
  3) Staging spec module and sensor separation
    - Problem: Mixing staging experimental spec flow with shared pipeline flow could blur test repeatability and responsibility boundaries.
    - Cause: If staging-specific sensor, asset, and spec config are insufficiently separated, production and test paths share the same job/asset definitions.
    - Actions:
      - Separated staging-specific definitions, spec asset, and sensor layers to maintain test flow independently.
      - Corrected spec-related DB storage structure and config interpretation paths together.
    - Related files:
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/spec/assets.py`
      - `src/vlm_pipeline/resources/duckdb_spec.py`
  4) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests are present, YOLO could run too early or conflict with other routing.
    - Cause: YOLO must run after frame generation, but could independently start first due to dispatch selection or shared asset connection structure.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per bbox, image classification, captioning combination.
    - Related files:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/defs/yolo/assets.py`
  Key file changes:
    - `src/vlm_pipeline/definitions.py`
    - `src/vlm_pipeline/defs/dispatch/production_agent_sensor.py`
    - `src/vlm_pipeline/defs/dispatch/sensor.py`
    - `src/vlm_pipeline/defs/dispatch/staging_agent_sensor.py`
    - `src/vlm_pipeline/defs/ingest/assets.py`
    - `src/vlm_pipeline/defs/label/assets.py`
    - `src/vlm_pipeline/defs/process/assets.py`
    - `src/vlm_pipeline/defs/spec/assets.py`
    - `src/vlm_pipeline/defs/yolo/assets.py`
    - `src/vlm_pipeline/resources/duckdb_spec.py`
  Related commit summary:
    - [7db48860] feat: organize production/staging labeling storage flow

  --- 2026-03-31 auto-recorded (detailed) ---
  1) Production/schema layer maintenance
    - Problem: Changes included important modifications to DuckDB schema correction or catalog stability, requiring reduction of schema mismatch recurrence risk during runtime.
    - Cause: DuckDB-related changes go beyond simple column additions; `ensure_schema`, ingest storage paths, and schema DDL are interlinked, so partial fixes can leave different states in production/test environments.
    - Actions:
      - Organized DuckDB-related resources, ingest storage logic, and schema definitions together to consistently align runtime schema correction flow.
      - Prepared manual recovery procedures and inspection scripts for immediate use during operational incidents.
    - Related files:
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/sql/schema.sql`
  2) Staging dispatch and ingest flow cleanup
    - Problem: trigger JSON presence, incoming wait, archive move, MinIO upload order in staging could behave differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON can be auto-processed, or archive order can deviate from expectations.
    - Actions:
      - Adjusted dispatch-related sensor, archive helper, and ingest sensor together to clearly separate trigger conditions and move order in staging.
      - Organized branching rules so wait/move/upload flow differs based on trigger JSON presence.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/lib/staging_dispatch.py`
  3) Staging Vertex/VQA captioning structure reinforcement
    - Problem: In staging, the storage responsibility for video event captions and image/VQA captions could be unseparated, or frame selection criteria could be unclear.
    - Cause: Captioning-related changes touch both label/event and image/frame levels; if routing and storage column meanings are not organized together, downstream queries become confused.
    - Actions:
      - Modified Vertex helper and process asset together to separate responsibility of event caption and image caption.
      - Organized frame relevance judgment, top-1 selection, and image caption storage flow by staging standard.
    - Related files:
      - `src/vlm_pipeline/defs/label/assets.py`
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/lib/staging_vertex.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/sql/schema.sql`
  4) Staging spec module and sensor separation
    - Problem: Mixing staging experimental spec flow with shared pipeline flow could blur test repeatability and responsibility boundaries.
    - Cause: If staging-specific sensor, asset, and spec config are insufficiently separated, production and test paths share the same job/asset definitions.
    - Actions:
      - Separated staging-specific definitions, spec asset, and sensor layers to maintain test flow independently.
      - Corrected spec-related DB storage structure and config interpretation paths together.
    - Related files:
      - `src/vlm_pipeline/defs/spec/sensor.py`
  5) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests are present, YOLO could run too early or conflict with other routing.
    - Cause: YOLO must run after frame generation, but could independently start first due to dispatch selection or shared asset connection structure.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per bbox, image classification, captioning combination.
    - Related files:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/defs/yolo/assets.py`
      - `src/vlm_pipeline/lib/env_utils.py`
  Key file changes:
    - `docker/app/dagster.yaml`
    - `docker/docker-compose.yaml`
    - `scripts/cleanup_duplicate_assets.py`
    - `src/vlm_pipeline/defs/dedup/assets.py`
    - `src/vlm_pipeline/defs/dedup/sensor.py`
    - `src/vlm_pipeline/defs/ingest/ops.py`
    - `src/vlm_pipeline/defs/label/assets.py`
    - `src/vlm_pipeline/defs/process/assets.py`
    - `src/vlm_pipeline/defs/process/sensor.py`
    - `src/vlm_pipeline/defs/spec/sensor.py`
    - `src/vlm_pipeline/defs/yolo/assets.py`
    - `src/vlm_pipeline/resources/duckdb_base.py`
    - `... and 3 more`
  Related commit summary:
    - [793023d5] fix: support GCP raw key and MinIO path cleanup
    - [59094df7] chore: exclude local documentation files from git tracking
    - [cfd66af9] chore: add local documentation ignore rules and remove manual ingest test script
    - [d04360fa] refactor: organize production/test labeling flow and catalog
    - [3051a6cf] Merge branch 'dev' into dev

  --- 2026-04-01 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: trigger JSON presence, incoming wait, archive move, MinIO upload order in staging could behave differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON can be auto-processed, or archive order can deviate from expectations.
    - Actions:
      - Adjusted dispatch-related sensor, archive helper, and ingest sensor together to clearly separate trigger conditions and move order in staging.
      - Organized branching rules so wait/move/upload flow differs based on trigger JSON presence.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/lib/staging_dispatch.py`
  2) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests are present, YOLO could run too early or conflict with other routing.
    - Cause: YOLO must run after frame generation, but could independently start first due to dispatch selection or shared asset connection structure.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per bbox, image classification, captioning combination.
    - Related files:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/defs/yolo/assets.py`
      - `src/vlm_pipeline/lib/env_utils.py`
      - `src/vlm_pipeline/lib/video_frames.py`
  3) Local-only asset and documentation/automation cleanup
    - Problem: Local notes, personal skills, and env example files mixed with shared repository history could make collaboration scope ambiguous.
    - Cause: Files meaningful only in personal environments accumulate unnecessary changes in repository history, and automation documents can deviate from actual operational state.
    - Actions:
      - Organized `.agent`, `doc_2`, env examples, and daily worklog-related files as tracked/untracked per purpose.
      - Calibrated automation scripts and document format to actual operational/test experience.
    - Related files:
      - `.gitignore`
  Key file changes:
    - `docker/docker-compose.yaml`
    - `src/vlm_pipeline/defs/label/assets.py`
    - `src/vlm_pipeline/defs/process/assets.py`
    - `src/vlm_pipeline/defs/sam/assets.py`
    - `src/vlm_pipeline/defs/yolo/assets.py`
    - `src/vlm_pipeline/resources/duckdb_dedup.py`
    - `src/vlm_pipeline/resources/duckdb_labeling.py`
  Related commit summary:
    - [56581ab9] feat: SAM3/YOLO/labeling pipeline expansion and staging dispatch improvement

  --- 2026-04-02 auto-recorded (detailed) ---
  1) Production/schema layer maintenance
    - Problem: Changes included important modifications to DuckDB schema correction or catalog stability, requiring reduction of schema mismatch recurrence risk during runtime.
    - Cause: DuckDB-related changes go beyond simple column additions; `ensure_schema`, ingest storage paths, and schema DDL are interlinked, so partial fixes can leave different states in production/test environments.
    - Actions:
      - Organized DuckDB-related resources, ingest storage logic, and schema definitions together to consistently align runtime schema correction flow.
      - Prepared manual recovery procedures and inspection scripts for immediate use during operational incidents.
    - Related files:
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
  2) Staging Vertex/VQA captioning structure reinforcement
    - Problem: In staging, the storage responsibility for video event captions and image/VQA captions could be unseparated, or frame selection criteria could be unclear.
    - Cause: Captioning-related changes touch both label/event and image/frame levels; if routing and storage column meanings are not organized together, downstream queries become confused.
    - Actions:
      - Modified Vertex helper and process asset together to separate responsibility of event caption and image caption.
      - Organized frame relevance judgment, top-1 selection, and image caption storage flow by staging standard.
    - Related files:
      - `src/vlm_pipeline/defs/label/assets.py`
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
  3) Staging spec module and sensor separation
    - Problem: Mixing staging experimental spec flow with shared pipeline flow could blur test repeatability and responsibility boundaries.
    - Cause: If staging-specific sensor, asset, and spec config are insufficiently separated, production and test paths share the same job/asset definitions.
    - Actions:
      - Separated staging-specific definitions, spec asset, and sensor layers to maintain test flow independently.
      - Corrected spec-related DB storage structure and config interpretation paths together.
    - Related files:
      - `src/vlm_pipeline/defs/spec/config_resolver.py`
      - `src/vlm_pipeline/lib/spec_config.py`
  4) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests are present, YOLO could run too early or conflict with other routing.
    - Cause: YOLO must run after frame generation, but could independently start first due to dispatch selection or shared asset connection structure.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per bbox, image classification, captioning combination.
    - Related files:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/defs/yolo/assets.py`
  5) Local-only asset and documentation/automation cleanup
    - Problem: Local notes, personal skills, and env example files mixed with shared repository history could make collaboration scope ambiguous.
    - Cause: Files meaningful only in personal environments accumulate unnecessary changes in repository history, and automation documents can deviate from actual operational state.
    - Actions:
      - Organized `.agent`, `doc_2`, env examples, and daily worklog-related files as tracked/untracked per purpose.
      - Calibrated automation scripts and document format to actual operational/test experience.
    - Related files:
      - `.gitignore`
  Key file changes:
    - `docker/docker-compose.yaml`
    - `src/vlm_pipeline/defs/dispatch/production_agent_sensor.py`
    - `src/vlm_pipeline/defs/dispatch/staging_agent_sensor.py`
    - `src/vlm_pipeline/defs/label/assets.py`
    - `src/vlm_pipeline/defs/process/assets.py`
    - `src/vlm_pipeline/defs/sam/assets.py`
    - `src/vlm_pipeline/defs/yolo/assets.py`
    - `src/vlm_pipeline/resources/duckdb_base.py`
    - `src/vlm_pipeline/resources/duckdb_ingest.py`
    - `src/vlm_pipeline/resources/duckdb_ingest_dispatch.py`
    - `src/vlm_pipeline/resources/duckdb_ingest_metadata.py`
    - `src/vlm_pipeline/resources/duckdb_ingest_raw.py`
    - `... and 2 more`
  Related commit summary:
    - [9b400226] feat: pipeline module split refactoring and documentation structure improvement

  --- 2026-04-03 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: trigger JSON presence, incoming wait, archive move, MinIO upload order in staging could behave differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON can be auto-processed, or archive order can deviate from expectations.
    - Actions:
      - Adjusted dispatch-related sensor, archive helper, and ingest sensor together to clearly separate trigger conditions and move order in staging.
      - Organized branching rules so wait/move/upload flow differs based on trigger JSON presence.
    - Related files:
      - `src/vlm_pipeline/defs/ingest/archive.py`
  Key file changes:
    - `docs/design-docs/duckdb-lock-contention-analysis.md`
    - `docs/exec-plans/duckdb-lock-fix-plan.md`
    - `src/vlm_pipeline/defs/ingest/assets.py`
    - `src/vlm_pipeline/defs/ingest/ops.py`
    - `src/vlm_pipeline/resources/duckdb_ingest_dispatch.py`
    - `src/vlm_pipeline/resources/duckdb_migration.py`
  Related commit summary:
    - [b94657e6] fix: DuckDB lock contention mitigation and operational recovery script addition
    - [0b9c3df9] feat: add ingest compaction/rehydration module, separate clip window, and organize operational scripts

  --- 2026-04-06 auto-recorded (detailed) ---
  1) Production/schema layer maintenance
    - Problem: Changes included important modifications to DuckDB schema correction or catalog stability, requiring reduction of schema mismatch recurrence risk during runtime.
    - Cause: DuckDB-related changes go beyond simple column additions; `ensure_schema`, ingest storage paths, and schema DDL are interlinked, so partial fixes can leave different states in production/test environments.
    - Actions:
      - Organized DuckDB-related resources, ingest storage logic, and schema definitions together to consistently align runtime schema correction flow.
      - Prepared manual recovery procedures and inspection scripts for immediate use during operational incidents.
    - Related files:
      - `src/vlm_pipeline/sql/schema.sql`
  2) Local-only asset and documentation/automation cleanup
    - Problem: Local notes, personal skills, and env example files mixed with shared repository history could make collaboration scope ambiguous.
    - Cause: Files meaningful only in personal environments accumulate unnecessary changes in repository history, and automation documents can deviate from actual operational state.
    - Actions:
      - Organized `.agent`, `doc_2`, env examples, and daily worklog-related files as tracked/untracked per purpose.
      - Calibrated automation scripts and document format to actual operational/test experience.
    - Related files:
      - `.env.example`
  Key file changes:
    - `docker/docker-compose.labelstudio.yaml`
    - `docker/docker-compose.yaml`
    - `docs/design-docs/duckdb-lock-contention-analysis.md`
    - `docs/exec-plans/duckdb-lock-fix-plan.md`
    - `src/vlm_pipeline/defs/ls/sensor.py`
    - `src/vlm_pipeline/resources/duckdb_ingest_dispatch.py`
    - `src/vlm_pipeline/resources/duckdb_migration.py`
    - `src/vlm_pipeline/sql/schema.sql`
  Related commit summary:
    - [5db1d0eb] chore: apply operational reset report and test environment settings
    - [0bf00031] Merge pull request #32 from hoonikooni/feature/sanghoon
    - [1b7d3ec3] docs: add Label Studio integration section to CLAUDE.md, organize runbook
    - [35e4d262] fix: remove ls sensor op context type hint — Dagster compatibility
    - [b1a7f312] Merge remote-tracking branch 'upstream/dev' into feature/sanghoon
    - [c8661a93] feat: Label Studio integration phase 1 — schema/config/implementation added

  --- 2026-04-07 auto-recorded (detailed) ---
  1) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests are present, YOLO could run too early or conflict with other routing.
    - Cause: YOLO must run after frame generation, but could independently start first due to dispatch selection or shared asset connection structure.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per bbox, image classification, captioning combination.
    - Related files:
      - `src/vlm_pipeline/defs/yolo/assets.py`
      - `src/vlm_pipeline/defs/yolo/staging_assets.py`
      - `src/vlm_pipeline/lib/env_utils.py`
  2) Local-only asset and documentation/automation cleanup
    - Problem: Local notes, personal skills, and env example files mixed with shared repository history could make collaboration scope ambiguous.
    - Cause: Files meaningful only in personal environments accumulate unnecessary changes in repository history, and automation documents can deviate from actual operational state.
    - Actions:
      - Organized `.agent`, `doc_2`, env examples, and daily worklog-related files as tracked/untracked per purpose.
      - Calibrated automation scripts and document format to actual operational/test experience.
    - Related files:
      - `.env.example`
  Key file changes:
    - `docker/app/dagster_home/dagster.yaml`
    - `docker/app/dagster_home_staging/dagster.yaml`
    - `docker/docker-compose.labelstudio.yaml`
    - `docker/docker-compose.yaml`
    - `src/vlm_pipeline/defs/ingest/ops.py`
    - `src/vlm_pipeline/defs/sam/assets.py`
    - `src/vlm_pipeline/defs/sync/sensor.py`
    - `src/vlm_pipeline/defs/yolo/assets.py`
    - `src/vlm_pipeline/defs/yolo/staging_assets.py`
  Related commit summary:
    - [17bb4270] feat: DuckDB writer tag granularization and label/SAM3/YOLO/Docker maintenance

  --- 2026-04-08 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: trigger JSON presence, incoming wait, archive move, MinIO upload order in staging could behave differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON can be auto-processed, or archive order can deviate from expectations.
    - Actions:
      - Adjusted dispatch-related sensor, archive helper, and ingest sensor together to clearly separate trigger conditions and move order in staging.
      - Organized branching rules so wait/move/upload flow differs based on trigger JSON presence.
    - Related files:
      - `src/vlm_pipeline/defs/ingest/archive.py`
  2) Staging spec module and sensor separation
    - Problem: Mixing staging experimental spec flow with shared pipeline flow could blur test repeatability and responsibility boundaries.
    - Cause: If staging-specific sensor, asset, and spec config are insufficiently separated, production and test paths share the same job/asset definitions.
    - Actions:
      - Separated staging-specific definitions, spec asset, and sensor layers to maintain test flow independently.
      - Corrected spec-related DB storage structure and config interpretation paths together.
    - Related files:
      - `src/vlm_pipeline/defs/spec/config_resolver.py`
      - `src/vlm_pipeline/lib/spec_config.py`
  3) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests are present, YOLO could run too early or conflict with other routing.
    - Cause: YOLO must run after frame generation, but could independently start first due to dispatch selection or shared asset connection structure.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per bbox, image classification, captioning combination.
    - Related files:
      - `src/vlm_pipeline/defs/yolo/assets.py`
      - `src/vlm_pipeline/defs/yolo/sensor.py`
      - `src/vlm_pipeline/lib/env_utils.py`
  Key file changes:
    - `src/vlm_pipeline/defs/ingest/assets.py`
    - `src/vlm_pipeline/defs/ingest/ops.py`
    - `src/vlm_pipeline/defs/ingest/sensor.py`
    - `src/vlm_pipeline/defs/sam/assets.py`
    - `src/vlm_pipeline/defs/sam/detection_assets.py`
    - `src/vlm_pipeline/defs/sam/sensor.py`
    - `src/vlm_pipeline/defs/sam/staging_detection_assets.py`
    - `src/vlm_pipeline/defs/yolo/assets.py`
    - `src/vlm_pipeline/defs/yolo/sensor.py`
    - `src/vlm_pipeline/resources/duckdb_ingest_metadata.py`
    - `src/vlm_pipeline/resources/duckdb_labeling.py`
    - `src/vlm_pipeline/resources/duckdb_video_metadata.py`
  Related commit summary:
    - [d7e11cdb] feat: add NAS health sensor, strengthen ingest, and organize runbook/timestamps
    - [a3577695] feat: detection common module, SAM/YOLO sensor separation, and documentation cleanup

  --- 2026-04-09 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: trigger JSON presence, incoming wait, archive move, MinIO upload order in staging could behave differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON can be auto-processed, or archive order can deviate from expectations.
    - Actions:
      - Adjusted dispatch-related sensor, archive helper, and ingest sensor together to clearly separate trigger conditions and move order in staging.
      - Organized branching rules so wait/move/upload flow differs based on trigger JSON presence.
    - Related files:
      - `src/vlm_pipeline/defs/ingest/archive.py`
  2) Staging Vertex/VQA captioning structure reinforcement
    - Problem: In staging, the storage responsibility for video event captions and image/VQA captions could be unseparated, or frame selection criteria could be unclear.
    - Cause: Captioning-related changes touch both label/event and image/frame levels; if routing and storage column meanings are not organized together, downstream queries become confused.
    - Actions:
      - Modified Vertex helper and process asset together to separate responsibility of event caption and image caption.
      - Organized frame relevance judgment, top-1 selection, and image caption storage flow by staging standard.
    - Related files:
      - `src/vlm_pipeline/lib/staging_vertex.py`
  3) Local-only asset and documentation/automation cleanup
    - Problem: Local notes, personal skills, and env example files mixed with shared repository history could make collaboration scope ambiguous.
    - Cause: Files meaningful only in personal environments accumulate unnecessary changes in repository history, and automation documents can deviate from actual operational state.
    - Actions:
      - Organized `.agent`, `doc_2`, env examples, and daily worklog-related files as tracked/untracked per purpose.
      - Calibrated automation scripts and document format to actual operational/test experience.
    - Related files:
      - `.env.example`
      - `.gitignore`
  Key file changes:
    - `src/vlm_pipeline/defs/ingest/assets.py`
    - `src/vlm_pipeline/defs/ingest/ops.py`
    - `src/vlm_pipeline/defs/ingest/sensor.py`
    - `src/vlm_pipeline/defs/ls/sensor.py`
    - `src/vlm_pipeline/resources/duckdb_ingest_raw.py`
  Related commit summary:
    - [181a8fbc] Merge pull request #36 from upstream-org/feature/sanghoon
    - [310ab685] Merge branch 'dev' of https://github.com/upstream-org/Datapipeline-Data-data_pipeline into feature/sanghoon
    - [789e21ea] feat: Label Studio operational prep — presigned URL auto-renewal, DuckDB lock retry, operational docs
    - [5d0c6e37] Merge pull request #35 from Orderlee/dev
    - [93d326c2] feat: Gemini JSON parsing reinforcement, ingest/label/caption cleanup, docs/test supplement

  --- 2026-04-10 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: trigger JSON presence, incoming wait, archive move, MinIO upload order in staging could behave differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON can be auto-processed, or archive order can deviate from expectations.
    - Actions:
      - Adjusted dispatch-related sensor, archive helper, and ingest sensor together to clearly separate trigger conditions and move order in staging.
      - Organized branching rules so wait/move/upload flow differs based on trigger JSON presence.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/webhook_server.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`
      - `src/vlm_pipeline/defs/ingest/sensor_helpers.py`
      - `src/vlm_pipeline/defs/ingest/sensor_incoming.py`
  Key file changes:
    - `docker/docker-compose.labelstudio.yaml`
    - `docker/docker-compose.yaml`
    - `src/vlm_pipeline/defs/gcp/assets.py`
  Related commit summary:
    - [c2fc43a5] feat: dispatch webhook/GCP/ingest reinforcement and Docker/MLOps documentation

  --- 2026-04-13 auto-recorded (detailed) ---
  1) Production/schema layer maintenance
    - Problem: Changes included important modifications to DuckDB schema correction or catalog stability, requiring reduction of schema mismatch recurrence risk during runtime.
    - Cause: DuckDB-related changes go beyond simple column additions; `ensure_schema`, ingest storage paths, and schema DDL are interlinked, so partial fixes can leave different states in production/test environments.
    - Actions:
      - Organized DuckDB-related resources, ingest storage logic, and schema definitions together to consistently align runtime schema correction flow.
      - Prepared manual recovery procedures and inspection scripts for immediate use during operational incidents.
    - Related files:
      - `scripts/repair_image_metadata_schema.py`
      - `src/python/common/schema.sql`
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/resources/duckdb_labeling.py`
      - `src/vlm_pipeline/resources/duckdb_spec.py`
      - `... and 1 more`
  2) Staging dispatch and ingest flow cleanup
    - Problem: trigger JSON presence, incoming wait, archive move, MinIO upload order in staging could behave differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON can be auto-processed, or archive order can deviate from expectations.
    - Actions:
      - Adjusted dispatch-related sensor, archive helper, and ingest sensor together to clearly separate trigger conditions and move order in staging.
      - Organized branching rules so wait/move/upload flow differs based on trigger JSON presence.
    - Related files:
      - `scripts/reupload_minio_from_archive.py`
      - `src/vlm_pipeline/defs/dispatch/__init__.py`
      - `src/vlm_pipeline/defs/dispatch/agent_sensor_common.py`
      - `src/vlm_pipeline/defs/dispatch/production_agent_sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor_incoming_mover.py`
      - `... and 10 more`
  3) Staging Vertex/VQA captioning structure reinforcement
    - Problem: In staging, the storage responsibility for video event captions and image/VQA captions could be unseparated, or frame selection criteria could be unclear.
    - Cause: Captioning-related changes touch both label/event and image/frame levels; if routing and storage column meanings are not organized together, downstream queries become confused.
    - Actions:
      - Modified Vertex helper and process asset together to separate responsibility of event caption and image caption.
      - Organized frame relevance judgment, top-1 selection, and image caption storage flow by staging standard.
    - Related files:
      - `src/python/common/schema.sql`
      - `src/vlm_pipeline/defs/label/assets.py`
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/lib/staging_vertex.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/sql/schema.sql`
  4) Staging spec module and sensor separation
    - Problem: Mixing staging experimental spec flow with shared pipeline flow could blur test repeatability and responsibility boundaries.
    - Cause: If staging-specific sensor, asset, and spec config are insufficiently separated, production and test paths share the same job/asset definitions.
    - Actions:
      - Separated staging-specific definitions, spec asset, and sensor layers to maintain test flow independently.
      - Corrected spec-related DB storage structure and config interpretation paths together.
    - Related files:
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/spec/__init__.py`
      - `src/vlm_pipeline/defs/spec/assets.py`
      - `src/vlm_pipeline/defs/spec/config_resolver.py`
      - `src/vlm_pipeline/defs/spec/sensor.py`
      - `src/vlm_pipeline/defs/spec/staging_assets.py`
      - `... and 3 more`
  5) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests are present, YOLO could run too early or conflict with other routing.
    - Cause: YOLO must run after frame generation, but could independently start first due to dispatch selection or shared asset connection structure.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per bbox, image classification, captioning combination.
    - Related files:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/defs/yolo/__init__.py`
      - `src/vlm_pipeline/defs/yolo/assets.py`
      - `src/vlm_pipeline/defs/yolo/sensor.py`
      - `src/vlm_pipeline/defs/yolo/staging_assets.py`
      - `src/vlm_pipeline/lib/env_utils.py`
      - `... and 1 more`
  Key file changes:
    - `docker/app/dagster_home/dagster.yaml`
    - `docker/app/{ => dagster_home_staging}/dagster.yaml`
    - `docker/docker-compose.dev.yaml`
    - `docker/docker-compose.labelstudio.yaml`
    - `docker/docker-compose.yaml`
    - `docs/design-docs/duckdb-lock-contention-analysis.md`
    - `docs/exec-plans/duckdb-lock-fix-plan.md`
    - `scripts/cleanup_duplicate_assets.py`
    - `src/python/common/schema.sql`
    - `src/python/local_duckdb_to_motherduck_sync.py`
    - `src/vlm_pipeline/definitions.py`
    - `src/vlm_pipeline/defs/build/assets.py`
    - `... and 40 more`
  Related commit summary:
    - [61010847] fix: exclude dagster_home from rsync to avoid permission errors
    - [12a4678f] fix: run docker compose up from production directory
    - [eea0d97f] chore: stop tracking WORKLOG.md and add to .gitignore
    - [0c483ed3] fix: restore production .env in deploy workflow
    - [23b3e1fb] Merge remote-tracking branch 'origin/dev' into codex-main-sync
    - [6cdf3a39] fix: avoid unnecessary production image rebuilds

  --- 2026-04-14 auto-recorded (detailed) ---
  1) Production/schema layer maintenance
    - Problem: Changes included important modifications to DuckDB schema correction or catalog stability, requiring reduction of schema mismatch recurrence risk during runtime.
    - Cause: DuckDB-related changes go beyond simple column additions; `ensure_schema`, ingest storage paths, and schema DDL are interlinked, so partial fixes can leave different states in production/test environments.
    - Actions:
      - Organized DuckDB-related resources, ingest storage logic, and schema definitions together to consistently align runtime schema correction flow.
      - Prepared manual recovery procedures and inspection scripts for immediate use during operational incidents.
    - Related files:
      - `src/vlm_pipeline/resources/duckdb_labeling.py`
      - `src/vlm_pipeline/resources/duckdb_spec.py`
      - `src/vlm_pipeline/sql/schema.sql`
  2) Staging dispatch and ingest flow cleanup
    - Problem: trigger JSON presence, incoming wait, archive move, MinIO upload order in staging could behave differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON can be auto-processed, or archive order can deviate from expectations.
    - Actions:
      - Adjusted dispatch-related sensor, archive helper, and ingest sensor together to clearly separate trigger conditions and move order in staging.
      - Organized branching rules so wait/move/upload flow differs based on trigger JSON presence.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/agent_sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor_run_status.py`
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/defs/dispatch/staging_agent_sensor.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`
      - `src/vlm_pipeline/defs/ingest/sensor_bootstrap.py`
      - `... and 2 more`
  3) Staging Vertex/VQA captioning structure reinforcement
    - Problem: In staging, the storage responsibility for video event captions and image/VQA captions could be unseparated, or frame selection criteria could be unclear.
    - Cause: Captioning-related changes touch both label/event and image/frame levels; if routing and storage column meanings are not organized together, downstream queries become confused.
    - Actions:
      - Modified Vertex helper and process asset together to separate responsibility of event caption and image caption.
      - Organized frame relevance judgment, top-1 selection, and image caption storage flow by staging standard.
    - Related files:
      - `src/vlm_pipeline/defs/label/assets.py`
      - `src/vlm_pipeline/lib/staging_vertex.py`
      - `src/vlm_pipeline/sql/schema.sql`
  4) Staging spec module and sensor separation
    - Problem: Mixing staging experimental spec flow with shared pipeline flow could blur test repeatability and responsibility boundaries.
    - Cause: If staging-specific sensor, asset, and spec config are insufficiently separated, production and test paths share the same job/asset definitions.
    - Actions:
      - Separated staging-specific definitions, spec asset, and sensor layers to maintain test flow independently.
      - Corrected spec-related DB storage structure and config interpretation paths together.
    - Related files:
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/spec/__init__.py`
      - `src/vlm_pipeline/defs/spec/assets.py`
      - `src/vlm_pipeline/defs/spec/sensor.py`
      - `src/vlm_pipeline/defs/spec/staging_assets.py`
      - `src/vlm_pipeline/defs/spec/staging_sensor.py`
      - `... and 2 more`
  5) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests are present, YOLO could run too early or conflict with other routing.
    - Cause: YOLO must run after frame generation, but could independently start first due to dispatch selection or shared asset connection structure.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per bbox, image classification, captioning combination.
    - Related files:
      - `src/vlm_pipeline/defs/yolo/assets.py`
      - `src/vlm_pipeline/defs/yolo/staging_assets.py`
      - `src/vlm_pipeline/lib/env_utils.py`
  Key file changes:
    - `docker/app/dagster_home_staging/dagster.yaml`
    - `docker/docker-compose.dev.yaml`
    - `docker/docker-compose.yaml`
    - `scripts/query_local_duckdb.py`
    - `src/vlm_pipeline/defs/dispatch/agent_sensor.py`
    - `src/vlm_pipeline/defs/dispatch/staging_agent_sensor.py`
    - `src/vlm_pipeline/defs/gcp/assets.py`
    - `src/vlm_pipeline/defs/ingest/assets.py`
    - `src/vlm_pipeline/defs/label/assets.py`
    - `src/vlm_pipeline/defs/ls/sensor.py`
    - `src/vlm_pipeline/defs/sam/detection_assets.py`
    - `src/vlm_pipeline/defs/sam/staging_detection_assets.py`
    - `... and 11 more`
  Related commit summary:
    - [ffe553e0] refactor: production/test runtime separation and deployment/dispatch/docs cleanup
    - [1a267afc] feat: LS integration multi-media type support (--auto-detect)

  --- 2026-04-15 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: In staging, the order of trigger JSON presence check, incoming wait, archive move, and MinIO upload could operate differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON could be auto-processed, or the archive before/after order could diverge from expectations.
    - Actions:
      - Coordinated dispatch-related sensor, archive helper, and ingest sensor together to clearly separate the desired trigger conditions and movement order in staging.
      - Organized branching rules so that wait/move/upload flow differs based on presence of trigger JSON.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/agent_sensor.py`
  2) Staging Vertex/VQA captioning structure reinforcement
    - Problem: In staging, the storage responsibility for video event captions and image/VQA captions was not separated, or frame selection criteria could be unclear.
    - Cause: Captioning-related changes touch both label/event level and image/frame level, so without simultaneously cleaning up routing and storage column semantics, confusion arises in subsequent queries.
    - Actions:
      - Modified Vertex helper and process asset together to separate responsibilities of event caption and image caption.
      - Organized frame relevance judgment, top-1 selection, and image caption storage flow based on staging standards.
  Key file changes:
    - `src/vlm_pipeline/defs/dispatch/agent_sensor.py`
  Related commit summary:
    - [de4b2953] chore: clean up unused test/dispatch/staging remnant files
    - [38b6391f] fix: confirm Dagster workspace_prod location_name as prod_defs

  --- 2026-04-16 auto-recorded (detailed) ---
  1) Production/schema layer cleanup
    - Problem: DuckDB schema correction or catalog stability included important changes, requiring reduction of schema mismatch recurrence risk during runtime.
    - Cause: DuckDB-related changes go beyond simple column additions — `ensure_schema`, ingest storage paths, and schema DDL are interconnected, so fixing only part can leave different states in production/test environments.
    - Actions:
      - Cleaned up DuckDB-related resources, ingest storage logic, and schema definitions together to consistently align the runtime schema correction flow.
      - Added manual recovery procedures and inspection scripts as needed, prepared for immediate use during production incidents.
    - Related files:
      - `src/vlm_pipeline/resources/duckdb_labeling.py`
      - `src/vlm_pipeline/resources/duckdb_spec.py`
      - `src/vlm_pipeline/sql/schema.sql`
  2) Staging dispatch and ingest flow cleanup
    - Problem: In staging, the order of trigger JSON presence check, incoming wait, archive move, and MinIO upload could operate differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON could be auto-processed, or the archive before/after order could diverge from expectations.
    - Actions:
      - Coordinated dispatch-related sensor, archive helper, and ingest sensor together to clearly separate the desired trigger conditions and movement order in staging.
      - Organized branching rules so that wait/move/upload flow differs based on presence of trigger JSON.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/agent_sensor_common.py`
      - `src/vlm_pipeline/defs/dispatch/production_agent_sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor_run_status.py`
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/defs/dispatch/staging_agent_sensor.py`
      - `... and 5 more`
  3) Staging Vertex/VQA captioning structure reinforcement
    - Problem: In staging, the storage responsibility for video event captions and image/VQA captions was not separated, or frame selection criteria could be unclear.
    - Cause: Captioning-related changes touch both label/event level and image/frame level, so without simultaneously cleaning up routing and storage column semantics, confusion arises in subsequent queries.
    - Actions:
      - Modified Vertex helper and process asset together to separate responsibilities of event caption and image caption.
      - Organized frame relevance judgment, top-1 selection, and image caption storage flow based on staging standards.
    - Related files:
      - `src/vlm_pipeline/defs/label/assets.py`
      - `src/vlm_pipeline/lib/staging_vertex.py`
      - `src/vlm_pipeline/lib/{staging_vertex.py => vertex_chunking.py}`
      - `src/vlm_pipeline/sql/schema.sql`
  4) Staging spec module and sensor separation
    - Problem: Mixing staging experimental spec flows with shared pipeline flows could blur test repeatability and responsibility boundaries.
    - Cause: If staging-dedicated sensor, asset, and spec config are not sufficiently separated, production paths and test paths share the same job/asset definitions.
    - Actions:
      - Separated staging-dedicated definitions, spec asset, and sensor layers to maintain test flows independently.
      - Corrected spec-related DB storage structure and config resolution paths together.
    - Related files:
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/spec/__init__.py`
      - `src/vlm_pipeline/defs/spec/assets.py`
      - `src/vlm_pipeline/defs/spec/config_resolver.py`
      - `src/vlm_pipeline/defs/spec/sensor.py`
      - `src/vlm_pipeline/defs/spec/staging_assets.py`
      - `... and 3 more`
  5) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests exist, YOLO could execute too early or conflict with other routing.
    - Cause: YOLO must execute after frame generation, but due to dispatch selection or shared asset connection structure, it could start independently first.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per combination of bbox, image classification, and captioning.
    - Related files:
      - `src/vlm_pipeline/defs/yolo/assets.py`
      - `src/vlm_pipeline/defs/yolo/staging_assets.py`
      - `src/vlm_pipeline/lib/env_utils.py`
  Key file changes:
    - `docker/app/dagster_home_staging/dagster.yaml`
    - `docker/docker-compose.dev.yaml`
    - `docker/docker-compose.labelstudio.yaml`
    - `docker/docker-compose.yaml`
    - `src/vlm_pipeline/definitions.py`
    - `src/vlm_pipeline/defs/dispatch/production_agent_sensor.py`
    - `src/vlm_pipeline/defs/dispatch/sensor.py`
    - `src/vlm_pipeline/defs/dispatch/staging_agent_sensor.py`
    - `src/vlm_pipeline/defs/gcp/assets.py`
    - `src/vlm_pipeline/defs/ingest/assets.py`
    - `src/vlm_pipeline/defs/label/assets.py`
    - `src/vlm_pipeline/defs/ls/sensor.py`
    - `... and 13 more`
  Related commit summary:
    - [2c427b95] merge: dev → main — CI/CD integration + deployment stabilization + refactoring
    - [ca2e59ef] merge: origin/main → dev — resolve Claude App auto-generated workflow conflict
    - [2c7299bf] fix: prevent compose project name collision and safe deploy restart
    - [9d3fb43a] refactor: consolidate build_asset_job + detection asset factory (Phase 3)
    - [22788cc6] refactor: naming cleanup — definitions_production / correct residual names (Phase 2)
    - [ce644517] refactor: remove dead code and inline agent_sensor_common (Phase 1)

  --- 2026-04-17 auto-recorded (detailed) ---
  1) Production/schema layer cleanup
    - Problem: DuckDB schema correction or catalog stability included important changes, requiring reduction of schema mismatch recurrence risk during runtime.
    - Cause: DuckDB-related changes go beyond simple column additions — `ensure_schema`, ingest storage paths, and schema DDL are interconnected, so fixing only part can leave different states in production/test environments.
    - Actions:
      - Cleaned up DuckDB-related resources, ingest storage logic, and schema definitions together to consistently align the runtime schema correction flow.
      - Added manual recovery procedures and inspection scripts as needed, prepared for immediate use during production incidents.
    - Related files:
      - `src/vlm_pipeline/sql/schema.sql`
  2) Staging dispatch and ingest flow cleanup
    - Problem: In staging, the order of trigger JSON presence check, incoming wait, archive move, and MinIO upload could operate differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON could be auto-processed, or the archive before/after order could diverge from expectations.
    - Actions:
      - Coordinated dispatch-related sensor, archive helper, and ingest sensor together to clearly separate the desired trigger conditions and movement order in staging.
      - Organized branching rules so that wait/move/upload flow differs based on presence of trigger JSON.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/production_agent_sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor_run_status.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`
      - `src/vlm_pipeline/defs/ingest/sensor_bootstrap.py`
  3) Staging Vertex/VQA captioning structure reinforcement
    - Problem: In staging, the storage responsibility for video event captions and image/VQA captions was not separated, or frame selection criteria could be unclear.
    - Cause: Captioning-related changes touch both label/event level and image/frame level, so without simultaneously cleaning up routing and storage column semantics, confusion arises in subsequent queries.
    - Actions:
      - Modified Vertex helper and process asset together to separate responsibilities of event caption and image caption.
      - Organized frame relevance judgment, top-1 selection, and image caption storage flow based on staging standards.
    - Related files:
      - `src/vlm_pipeline/defs/label/assets.py`
      - `src/vlm_pipeline/sql/schema.sql`
  4) Staging spec module and sensor separation
    - Problem: Mixing staging experimental spec flows with shared pipeline flows could blur test repeatability and responsibility boundaries.
    - Cause: If staging-dedicated sensor, asset, and spec config are not sufficiently separated, production paths and test paths share the same job/asset definitions.
    - Actions:
      - Separated staging-dedicated definitions, spec asset, and sensor layers to maintain test flows independently.
      - Corrected spec-related DB storage structure and config resolution paths together.
    - Related files:
      - `src/vlm_pipeline/defs/spec/__init__.py`
      - `src/vlm_pipeline/defs/spec/config_resolver.py`
      - `src/vlm_pipeline/lib/spec_config.py`
  5) Local-only assets and documentation/automation cleanup
    - Problem: Local notes, personal skills, and env example files mixed into the shared repository history could blur collaboration scope.
    - Cause: Tracking files that are only meaningful in a personal environment accumulates unnecessary changes in repository history, and automation documentation can diverge from actual production state.
    - Actions:
      - Organized `.agent`, `doc_2`, env examples, and daily worklog-related files appropriately into tracked/untracked targets.
      - Corrected automation scripts and documentation format to match actual production/test experience.
    - Related files:
      - `.gitignore`
  Key file changes:
    - `docker/docker-compose.dev.yaml`
    - `docker/docker-compose.yaml`
    - `src/vlm_pipeline/definitions.py`
    - `src/vlm_pipeline/defs/dispatch/production_agent_sensor.py`
    - `src/vlm_pipeline/defs/dispatch/sensor.py`
    - `src/vlm_pipeline/defs/ingest/assets.py`
    - `src/vlm_pipeline/defs/ingest/ops.py`
    - `src/vlm_pipeline/defs/label/assets.py`
    - `src/vlm_pipeline/defs/ls/sensor.py`
    - `src/vlm_pipeline/defs/sync/assets.py`
    - `src/vlm_pipeline/resources/duckdb_ingest_dispatch.py`
    - `src/vlm_pipeline/resources/duckdb_ingest_raw.py`
    - `... and 2 more`
  Related commit summary:
    - [8745e4fb] chore(docker): permission isolation — parameterize DOCKER_USER/host bind paths + exclude dagster runtime state from tracking
    - [d1f08d45] merge: dev → main — deploy refactor batch + gitignore reinforcement
    - [19ec9406] chore(gitignore): exclude date-suffix one-time recovery scripts from tracking
    - [d34d8a75] refactor(ingest): split assets.py/archive.py (preserve orchestration logic)
    - [05d57b76] refactor(ingest): split ops.py/sensor_bootstrap.py (preserve cursor format bytes)
    - [533713bf] refactor(process): split helpers.py/frame_extract.py into domain-specific submodules

  --- 2026-04-20 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: In staging, the order of trigger JSON presence check, incoming wait, archive move, and MinIO upload could operate differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON could be auto-processed, or the archive before/after order could diverge from expectations.
    - Actions:
      - Coordinated dispatch-related sensor, archive helper, and ingest sensor together to clearly separate the desired trigger conditions and movement order in staging.
      - Organized branching rules so that wait/move/upload flow differs based on presence of trigger JSON.
  2) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests exist, YOLO could execute too early or conflict with other routing.
    - Cause: YOLO must execute after frame generation, but due to dispatch selection or shared asset connection structure, it could start independently first.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per combination of bbox, image classification, and captioning.
    - Related files:
      - `src/vlm_pipeline/defs/yolo/assets.py`
  Key file changes:
    - `docker/docker-compose.yaml`
    - `src/vlm_pipeline/defs/ls/sensor.py`
    - `src/vlm_pipeline/defs/sam/detection_assets.py`
    - `src/vlm_pipeline/defs/yolo/assets.py`
    - `src/vlm_pipeline/resources/duckdb_ingest_dispatch.py`
  Related commit summary:
    - [d3a5ea66] fix(dispatch): mark started_at IS NULL steps as 'skipped'
    - [b2cf2185] feat(ls): folder normalization + add attach-predictions command
    - [69781984] feat(sam3): switch primary bbox engine — always start + Failure on unready + bbox_status transition
    - [14c89ce7] feat(gemini): structured output schema + JSON repair/retry + credentials fallback detail
    - [7e4395db] feat(validator): macOS metafile filter + apply to ingest/sensor scan
    - [2bfcc7d5] chore: verify Claude PR Review

  --- 2026-04-21 auto-recorded (detailed) ---
  1) Production/schema layer cleanup
    - Problem: DuckDB schema correction or catalog stability included important changes, requiring reduction of schema mismatch recurrence risk during runtime.
    - Cause: DuckDB-related changes go beyond simple column additions — `ensure_schema`, ingest storage paths, and schema DDL are interconnected, so fixing only part can leave different states in production/test environments.
    - Actions:
      - Cleaned up DuckDB-related resources, ingest storage logic, and schema definitions together to consistently align the runtime schema correction flow.
      - Added manual recovery procedures and inspection scripts as needed, prepared for immediate use during production incidents.
    - Related files:
      - `src/vlm_pipeline/sql/schema.sql`
  2) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests exist, YOLO could execute too early or conflict with other routing.
    - Cause: YOLO must execute after frame generation, but due to dispatch selection or shared asset connection structure, it could start independently first.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per combination of bbox, image classification, and captioning.
    - Related files:
      - `src/vlm_pipeline/defs/yolo/assets.py`
  Key file changes:
    - `docker/docker-compose.yaml`
    - `src/vlm_pipeline/definitions.py`
    - `src/vlm_pipeline/defs/build/assets.py`
    - `src/vlm_pipeline/defs/ls/sensor.py`
    - `src/vlm_pipeline/defs/sam/detection_assets.py`
    - `src/vlm_pipeline/defs/yolo/assets.py`
    - `src/vlm_pipeline/resources/duckdb_dedup.py`
    - `src/vlm_pipeline/resources/minio.py`
    - `src/vlm_pipeline/sql/schema.sql`
  Related commit summary:
    - [df2811e0] feat(ls): auto-split project by category based on labeling_method + categories
    - [6a6d028e] chore(compose): connect dagster service to external pipeline-network
    - [b9299d9d] feat(ls): image mode (SAM3 COCO ↔ RectangleLabels) + Dagster GraphQL trigger + sensor prefix fix
    - [2f9d6448] feat(build): per-project timestamp+Bbox dataset build + new classification asset
    - [ccccda81] refactor(pipeline): split dispatch_stage → post_review_clip_job + extract SAM3 common logic

  --- 2026-04-22 auto-recorded (detailed) ---
  1) Production/schema layer cleanup
    - Problem: DuckDB schema correction or catalog stability included important changes, requiring reduction of schema mismatch recurrence risk during runtime.
    - Cause: DuckDB-related changes go beyond simple column additions — `ensure_schema`, ingest storage paths, and schema DDL are interconnected, so fixing only part can leave different states in production/test environments.
    - Actions:
      - Cleaned up DuckDB-related resources, ingest storage logic, and schema definitions together to consistently align the runtime schema correction flow.
      - Added manual recovery procedures and inspection scripts as needed, prepared for immediate use during production incidents.
    - Related files:
      - `src/vlm_pipeline/sql/schema.sql`
  2) Staging dispatch and ingest flow cleanup
    - Problem: In staging, the order of trigger JSON presence check, incoming wait, archive move, and MinIO upload could operate differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON could be auto-processed, or the archive before/after order could diverge from expectations.
    - Actions:
      - Coordinated dispatch-related sensor, archive helper, and ingest sensor together to clearly separate the desired trigger conditions and movement order in staging.
      - Organized branching rules so that wait/move/upload flow differs based on presence of trigger JSON.
  3) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests exist, YOLO could execute too early or conflict with other routing.
    - Cause: YOLO must execute after frame generation, but due to dispatch selection or shared asset connection structure, it could start independently first.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per combination of bbox, image classification, and captioning.
    - Related files:
      - `src/vlm_pipeline/defs/yolo/assets.py`
  4) Local-only assets and documentation/automation cleanup
    - Problem: Local notes, personal skills, and env example files mixed into the shared repository history could blur collaboration scope.
    - Cause: Tracking files that are only meaningful in a personal environment accumulates unnecessary changes in repository history, and automation documentation can diverge from actual production state.
    - Actions:
      - Organized `.agent`, `doc_2`, env examples, and daily worklog-related files appropriately into tracked/untracked targets.
      - Corrected automation scripts and documentation format to match actual production/test experience.
    - Related files:
      - `.gitignore`
  Key file changes:
    - `docker/docker-compose.yaml`
    - `src/vlm_pipeline/definitions.py`
    - `src/vlm_pipeline/defs/build/assets.py`
    - `src/vlm_pipeline/defs/ls/sensor.py`
    - `src/vlm_pipeline/defs/sam/detection_assets.py`
    - `src/vlm_pipeline/defs/yolo/assets.py`
    - `src/vlm_pipeline/resources/duckdb_dedup.py`
    - `src/vlm_pipeline/resources/duckdb_ingest_dispatch.py`
    - `src/vlm_pipeline/resources/minio.py`
    - `src/vlm_pipeline/sql/schema.sql`
  Related commit summary:
    - [bc69eb99] chore(gitignore): exclude local agent/env templates/LS runtime state from tracking
    - [b2d0c80e] docs(classification): document vlm-classification bucket policy
    - [fa4a4ce0] fix(ci): Claude PR Review max-turns 5 → 15
    - [2cdaccf4] Merge pull request #12 from Orderlee/dev
    - [f01d84e7] chore(compose): connect yolo/sam3 to pipeline-network (shared dev stack)
    - [f8c464b1] feat(ls): per-batch date project + video/image split + synonym-based category normalization

  --- 2026-04-23 auto-recorded (detailed) ---
  1) merge: dev → main — fix pr-to-upstream.sh cross-fork existing PR detection
    - Problem: Classified as an item requiring recurrence prevention or structural correction among the day's changes.
    - Cause: Detected as troubleshooting-natured work based on commit message and changed files.
    - Actions:
      - Reflected in runbook items based on related commits and changed files.
      - Reference commit: `07216011`
  2) fix(tools): fix pr-to-upstream.sh cross-fork existing PR detection bug
    - Problem: Classified as an item requiring recurrence prevention or structural correction among the day's changes.
    - Cause: `gh pr list --head "owner:branch"` format returned empty results for cross-fork PRs, creating a risk of attempting duplicate creation even when a PR was already open. Fixed by querying with `--head <branch>` only and filtering with `headRepositoryOwner.login`.

SRC_OWNER / SRC_BRANCH constant separation so SRC_HEAD (for `gh pr create`) and `--head` filter (for `gh pr list`) each use the correct format.

Actual PR #41 (Orderlee:ma...
    - Actions:
      - Reflected in runbook items based on related commits and changed files.
      - Reference commit: `4c8cbce1`
  3) Infrastructure/config file change: src/vlm_pipeline/defs/build/sensor.py
    - Problem: Classified as an item requiring recurrence prevention or structural correction among the day's changes.
    - Cause: Detected as troubleshooting-natured work based on commit message and changed files.
    - Actions:
      - Reflected in runbook items based on related commits and changed files.
  4) Infrastructure/config file change: src/vlm_pipeline/resources/duckdb_dedup.py
    - Problem: Classified as an item requiring recurrence prevention or structural correction among the day's changes.
    - Cause: Detected as troubleshooting-natured work based on commit message and changed files.
    - Actions:
      - Reflected in runbook items based on related commits and changed files.
  5) Infrastructure/config file change: src/vlm_pipeline/resources/minio.py
    - Problem: Classified as an item requiring recurrence prevention or structural correction among the day's changes.
    - Cause: Detected as troubleshooting-natured work based on commit message and changed files.
    - Actions:
      - Reflected in runbook items based on related commits and changed files.
  Key file changes:
    - `src/vlm_pipeline/defs/build/assets.py`
    - `src/vlm_pipeline/defs/build/sensor.py`
    - `src/vlm_pipeline/resources/duckdb_dedup.py`
    - `src/vlm_pipeline/resources/minio.py`
  Related commit summary:
    - [07216011] merge: dev → main — fix pr-to-upstream.sh cross-fork existing PR detection
    - [4c8cbce1] fix(tools): fix pr-to-upstream.sh cross-fork existing PR detection bug
    - [df7f7479] merge: dev → main — Slack finalize → build_dataset auto-trigger sensor
    - [9acbb4ed] feat(build): auto-trigger build_dataset via LS finalize detection sensor
    - [0c08216e] merge: dev → main — add tools/pr-to-upstream.sh helper
    - [e0978030] feat(tools): helper script for creating PR to upstream (upstream-org/dev)

  --- 2026-04-24 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: In staging, the order of trigger JSON presence check, incoming wait, archive move, and MinIO upload could operate differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON could be auto-processed, or the archive before/after order could diverge from expectations.
    - Actions:
      - Coordinated dispatch-related sensor, archive helper, and ingest sensor together to clearly separate the desired trigger conditions and movement order in staging.
      - Organized branching rules so that wait/move/upload flow differs based on presence of trigger JSON.
    - Related files:
      - `src/vlm_pipeline/defs/ingest/sensor_bootstrap.py`
  Related commit summary:
    - [ea8c3119] feat(ingest): defer MinIO upload in auto_bootstrap path + 'archived' status
    - [f3ee252f] Merge pull request #15 from Orderlee/feature/auto-ingest-and-tmp-rename
    - [d0281a37] feat(ingest): auto archive+ingest of incoming + handle .tmp rename

  --- 2026-04-27 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: In staging, the order of trigger JSON presence check, incoming wait, archive move, and MinIO upload could operate differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON could be auto-processed, or the archive before/after order could diverge from expectations.
    - Actions:
      - Coordinated dispatch-related sensor, archive helper, and ingest sensor together to clearly separate the desired trigger conditions and movement order in staging.
      - Organized branching rules so that wait/move/upload flow differs based on presence of trigger JSON.
    - Related files:
      - `src/vlm_pipeline/defs/ingest/sensor_bootstrap.py`
  Key file changes:
    - `src/vlm_pipeline/defs/ls/sensor.py`
  Related commit summary:
    - [e93b6ad4] Merge pull request #19 from Orderlee/dev
    - [59fc6743] Merge pull request #18 from Orderlee/fix/ls-sync-video-sot-on-cdd83c9
    - [f33c8ba6] fix(ls_sync): merge empty review and new events in video branch with SOT=human submit
    - [ad8574bf] Merge pull request #17 from Orderlee/main

  --- 2026-04-28 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: In staging, the order of trigger JSON presence check, incoming wait, archive move, and MinIO upload could operate differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON could be auto-processed, or the archive before/after order could diverge from expectations.
    - Actions:
      - Coordinated dispatch-related sensor, archive helper, and ingest sensor together to clearly separate the desired trigger conditions and movement order in staging.
      - Organized branching rules so that wait/move/upload flow differs based on presence of trigger JSON.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/archive_dispatch_sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `src/vlm_pipeline/defs/dispatch/service.py`
  Key file changes:
    - `docker/docker-compose.labelstudio.yaml`
    - `docker/docker-compose.yaml`
    - `src/vlm_pipeline/definitions.py`
    - `src/vlm_pipeline/defs/build/sensor.py`
    - `src/vlm_pipeline/defs/dispatch/archive_dispatch_sensor.py`
    - `src/vlm_pipeline/defs/dispatch/sensor.py`
  Related commit summary:
    - [964f13d6] Merge pull request #30 from Orderlee/dev
    - [8aeb8b19] Merge pull request #29 from Orderlee/fix/ls-tasks-image-state
    - [1cdd460b] fix(ls_tasks): call update_review_state in image mode too — align with video
    - [f9a78139] fix(dispatch): incorporate gemini-code-assist review — clean up silent fail / count race
    - [4744b47a] feat(dispatch): new sensor for archive data trigger + upload_label_job (Phase 2b MVP)
    - [1007f5bc] Merge pull request #27 from Orderlee/dev

  --- 2026-04-29 auto-recorded (detailed) ---
  1) Production/schema layer cleanup
    - Problem: DuckDB schema correction or catalog stability included important changes, requiring reduction of schema mismatch recurrence risk during runtime.
    - Cause: DuckDB-related changes go beyond simple column additions — `ensure_schema`, ingest storage paths, and schema DDL are interconnected, so fixing only part can leave different states in production/test environments.
    - Actions:
      - Cleaned up DuckDB-related resources, ingest storage logic, and schema definitions together to consistently align the runtime schema correction flow.
      - Added manual recovery procedures and inspection scripts as needed, prepared for immediate use during production incidents.
    - Related files:
      - `scripts/repair_image_metadata_schema.py`
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/resources/duckdb_labeling.py`
      - `src/vlm_pipeline/resources/duckdb_spec.py`
      - `src/vlm_pipeline/sql/schema.sql`
  2) Staging dispatch and ingest flow cleanup
    - Problem: In staging, the order of trigger JSON presence check, incoming wait, archive move, and MinIO upload could operate differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON could be auto-processed, or the archive before/after order could diverge from expectations.
    - Actions:
      - Coordinated dispatch-related sensor, archive helper, and ingest sensor together to clearly separate the desired trigger conditions and movement order in staging.
      - Organized branching rules so that wait/move/upload flow differs based on presence of trigger JSON.
    - Related files:
      - `scripts/migrate_legacy/build_pilot_manifest.py`
      - `scripts/reupload_minio_from_archive.py`
      - `src/vlm_pipeline/defs/dispatch/__init__.py`
      - `src/vlm_pipeline/defs/dispatch/archive_dispatch_sensor.py`
      - `src/vlm_pipeline/defs/dispatch/production_agent_sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `... and 9 more`
  3) Staging Vertex/VQA captioning structure reinforcement
    - Problem: In staging, the storage responsibility for video event captions and image/VQA captions was not separated, or frame selection criteria could be unclear.
    - Cause: Captioning-related changes touch both label/event level and image/frame level, so without simultaneously cleaning up routing and storage column semantics, confusion arises in subsequent queries.
    - Actions:
      - Modified Vertex helper and process asset together to separate responsibilities of event caption and image caption.
      - Organized frame relevance judgment, top-1 selection, and image caption storage flow based on staging standards.
    - Related files:
      - `src/vlm_pipeline/defs/label/assets.py`
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/sql/schema.sql`
  4) Staging spec module and sensor separation
    - Problem: Mixing staging experimental spec flows with shared pipeline flows could blur test repeatability and responsibility boundaries.
    - Cause: If staging-dedicated sensor, asset, and spec config are not sufficiently separated, production paths and test paths share the same job/asset definitions.
    - Actions:
      - Separated staging-dedicated definitions, spec asset, and sensor layers to maintain test flows independently.
      - Corrected spec-related DB storage structure and config resolution paths together.
    - Related files:
      - `src/vlm_pipeline/defs/spec/__init__.py`
      - `src/vlm_pipeline/defs/spec/config_resolver.py`
      - `src/vlm_pipeline/lib/spec_config.py`
      - `src/vlm_pipeline/resources/duckdb_spec.py`
  5) YOLO execution conditions and order adjustment
    - Problem: When bbox/image classification requests exist, YOLO could execute too early or conflict with other routing.
    - Cause: YOLO must execute after frame generation, but due to dispatch selection or shared asset connection structure, it could start independently first.
    - Actions:
      - Adjusted YOLO-related assets and routing conditions to ensure execution after frame generation.
      - Clearly organized staging branching rules per combination of bbox, image classification, and captioning.
    - Related files:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/defs/yolo/__init__.py`
      - `src/vlm_pipeline/defs/yolo/assets.py`
      - `src/vlm_pipeline/defs/yolo/sensor.py`
      - `src/vlm_pipeline/lib/env_utils.py`
      - `src/vlm_pipeline/lib/video_frames.py`
  Key file changes:
    - `docker/app/dagster_home/dagster.yaml`
    - `docker/docker-compose.dev.yaml`
    - `docker/docker-compose.labelstudio.yaml`
    - `docker/docker-compose.yaml`
    - `docs/design-docs/duckdb-lock-contention-analysis.md`
    - `docs/exec-plans/duckdb-lock-fix-plan.md`
    - `scripts/cleanup_duplicate_assets.py`
    - `scripts/query_local_duckdb.py`
    - `src/python/local_duckdb_to_motherduck_sync.py`
    - `src/vlm_pipeline/definitions.py`
    - `src/vlm_pipeline/defs/build/assets.py`
    - `src/vlm_pipeline/defs/build/sensor.py`
    - `... and 33 more`
  Related commit summary:
    - [df1c8477] Merge pull request #38 from Orderlee/fix/ls-finalize-residual-auto-recover
    - [3ef9916f] fix(ls): auto-correct finalize residual rows + guard against reviewed regression
    - [2726d1f3] Merge pull request #37 from Orderlee/dev
    - [80bf3b30] Merge pull request #36 from Orderlee/fix/ls-sync-state-label-keys-sync
    - [abd02e28] fix(ls_sync,ls_webhook): auto-accumulate label_keys in state.json
    - [0e135c32] Merge pull request #35 from Orderlee/refactor/label-split-codex

  --- 2026-04-30 auto-recorded (detailed) ---
  1) Merge pull request #40 from Orderlee/fix/build-dataset-self-healing
    - Problem: Classified as an item requiring recurrence prevention or structural correction among the day's changes.
    - Cause: fix(build): build_dataset self-healing (remove swallow + run_key attempt + RetryPolicy)
    - Actions:
      - Reflected in runbook items based on related commits and changed files.
      - Reference commit: `7c179250`
  2) fix(build): build_dataset self-healing — remove swallow + sensor run_key + RetryPolicy
    - Problem: Classified as an item requiring recurrence prevention or structural correction among the day's changes.
    - Cause: Incident consistency fix where build_dataset_single_job was permanently stuck on transient lock (DuckDB IO Error etc.). Discovered in proj#37 smart-city case — a single lock escalated to empty build + permanent sensor dedup.

Three combined fixes:

A) build_dataset asset (assets.py)
   - Removed the pattern of swallowing after per-folder try/except. Fail-forward to the end, but
     er...
    - Actions:
      - Reflected in runbook items based on related commits and changed files.
      - Reference commit: `ce48918d`
  3) Infrastructure/config file change: src/vlm_pipeline/defs/build/sensor.py
    - Problem: Classified as an item requiring recurrence prevention or structural correction among the day's changes.
    - Cause: Detected as troubleshooting-natured work based on commit message and changed files.
    - Actions:
      - Reflected in runbook items based on related commits and changed files.
  Key file changes:
    - `src/vlm_pipeline/defs/build/assets.py`
    - `src/vlm_pipeline/defs/build/sensor.py`
  Related commit summary:
    - [c820ec10] Merge pull request #41 from Orderlee/dev
    - [7c179250] Merge pull request #40 from Orderlee/fix/build-dataset-self-healing
    - [ce48918d] fix(build): build_dataset self-healing — remove swallow + sensor run_key + RetryPolicy

  --- 2026-05-06 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: In staging, the order of trigger JSON presence check, incoming wait, archive move, and MinIO upload could operate differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON could be auto-processed, or the archive before/after order could diverge from expectations.
    - Actions:
      - Coordinated dispatch-related sensor, archive helper, and ingest sensor together to clearly separate the desired trigger conditions and movement order in staging.
      - Organized branching rules so that wait/move/upload flow differs based on presence of trigger JSON.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/sensor_run_status.py`
  2) Local-only assets and documentation/automation cleanup
    - Problem: Local notes, personal skills, and env example files mixed into the shared repository history could blur collaboration scope.
    - Cause: Tracking files that are only meaningful in a personal environment accumulates unnecessary changes in repository history, and automation documentation can diverge from actual production state.
    - Actions:
      - Organized `.agent`, `doc_2`, env examples, and daily worklog-related files appropriately into tracked/untracked targets.
      - Corrected automation scripts and documentation format to match actual production/test experience.
    - Related files:
      - `.gitignore`
  Key file changes:
    - `docker/docker-compose.yaml`
    - `scripts/migrate_duckdb_to_postgres.py`
    - `src/python/local_duckdb_to_motherduck_sync.py`
    - `src/vlm_pipeline/defs/build/sensor.py`
    - `src/vlm_pipeline/defs/label/sensor.py`
    - `src/vlm_pipeline/defs/ls/sensor.py`
    - `src/vlm_pipeline/defs/process/sensor.py`
    - `src/vlm_pipeline/defs/sync/assets.py`
    - `src/vlm_pipeline/defs/sync/sensor.py`
  Related commit summary:
    - [0be93f71] test(ci): trigger review on non-md change
    - [6003c57e] test(ci): trigger claude-review workflow to verify npm pre-install fix
    - [da389ef0] fix(ci): claude-code-action native install bypass via npm pre-install (#49)
    - [43384ae6] fix(ci): claude-code-action native install bypass via npm pre-install
    - [b335de7c] chore: staging ops hygiene (Claude review fix + .env.test untrack + duckdb_writer release) (#48)
    - [c594e058] refactor(env_utils): build_duckdb_writer_tags returns {} in PG primary mode

  --- 2026-05-07 auto-recorded (detailed) ---
  1) Infrastructure/config file change: docs/references/dbeaver-pg-duckdb-test-guide.md
    - Problem: Classified as an item requiring recurrence prevention or structural correction among the day's changes.
    - Cause: Detected as troubleshooting-natured work based on commit message and changed files.
    - Actions:
      - Reflected in runbook items based on related commits and changed files.
  Key file changes:
    - `docs/references/dbeaver-pg-duckdb-test-guide.md`
  Related commit summary:
    - [e9cdcdb4] docs: add production PG rollout plan + DBeaver PG/DuckDB test guide

  --- 2026-05-08 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: In staging, the order of trigger JSON presence check, incoming wait, archive move, and MinIO upload could operate differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON could be auto-processed, or the archive before/after order could diverge from expectations.
    - Actions:
      - Coordinated dispatch-related sensor, archive helper, and ingest sensor together to clearly separate the desired trigger conditions and movement order in staging.
      - Organized branching rules so that wait/move/upload flow differs based on presence of trigger JSON.
    - Related files:
      - `src/vlm_pipeline/defs/dispatch/sensor_run_status.py`
  2) Local-only assets and documentation/automation cleanup
    - Problem: Local notes, personal skills, and env example files mixed into the shared repository history could blur collaboration scope.
    - Cause: Tracking files that are only meaningful in a personal environment accumulates unnecessary changes in repository history, and automation documentation can diverge from actual production state.
    - Actions:
      - Organized `.agent`, `doc_2`, env examples, and daily worklog-related files appropriately into tracked/untracked targets.
      - Corrected automation scripts and documentation format to match actual production/test experience.
    - Related files:
      - `.gitignore`
  Key file changes:
    - `docker/docker-compose.yaml`
    - `docs/references/dbeaver-pg-duckdb-test-guide.md`
    - `scripts/migrate_duckdb_to_postgres.py`
    - `src/python/local_duckdb_to_motherduck_sync.py`
    - `src/vlm_pipeline/defs/build/assets.py`
    - `src/vlm_pipeline/defs/build/sensor.py`
    - `src/vlm_pipeline/defs/label/sensor.py`
    - `src/vlm_pipeline/defs/ls/sensor.py`
    - `src/vlm_pipeline/defs/process/sensor.py`
    - `src/vlm_pipeline/defs/sync/assets.py`
    - `src/vlm_pipeline/defs/sync/sensor.py`
    - `src/vlm_pipeline/resources/minio.py`
  Related commit summary:
    - [a5636138] Merge pull request #51 from Orderlee/dev
    - [2d6efe93] chore(tests): untrack test_build_copy_outdated for CI host pydantic skew
    - [7dc2b611] fix(build): refresh stale dataset objects when source changes

  --- 2026-05-13 auto-recorded (detailed) ---
  1) Staging dispatch and ingest flow cleanup
    - Problem: In staging, the order of trigger JSON presence check, incoming wait, archive move, and MinIO upload could operate differently from test intent.
    - Cause: When dispatch sensor, bootstrap sensor, and ingest/archive paths intervene simultaneously, inputs without JSON could be auto-processed, or the archive before/after order could diverge from expectations.
    - Actions:
      - Coordinated dispatch-related sensor, archive helper, and ingest sensor together to clearly separate the desired trigger conditions and movement order in staging.
      - Organized branching rules so that wait/move/upload flow differs based on presence of trigger JSON.
    - Related files:
      - `docker/genai/storage/manifest.py`
      - `scripts/{ => archive}/migrate_legacy/build_pilot_manifest.py`
      - `src/vlm_pipeline/defs/dispatch/sensor_incoming_mover.py`
  2) Staging Vertex/VQA captioning structure reinforcement
    - Problem: In staging, the storage responsibility for video event captions and image/VQA captions was not separated, or frame selection criteria could be unclear.
    - Cause: Captioning-related changes touch both label/event level and image/frame level, so without simultaneously cleaning up routing and storage column semantics, confusion arises in subsequent queries.
    - Actions:
      - Modified Vertex helper and process asset together to separate responsibilities of event caption and image caption.
      - Organized frame relevance judgment, top-1 selection, and image caption storage flow based on staging standards.
  Key file changes:
    - `docker/docker-compose.labelstudio.yaml`
    - `docker/docker-compose.yaml`
    - `docs/references/dbeaver-pg-duckdb-test-guide.md`
    - `scripts/migrate_duckdb_to_postgres.py`
    - `src/vlm_pipeline/definitions.py`
    - `src/vlm_pipeline/defs/build/assets.py`
    - `src/vlm_pipeline/defs/genai/sensor.py`
    - `src/vlm_pipeline/defs/ingest/ops.py`
    - `src/vlm_pipeline/defs/sam/detection_assets.py`
    - `src/vlm_pipeline/defs/sam/sensor.py`
    - `src/vlm_pipeline/resources/duckdb_ingest_metadata.py`
  Related commit summary:
    - [c2426ac3] feat(infra): migrate MinIO endpoint 10.0.0.36 → 10.0.0.51
    - [b21df67b] Merge pull request #68 from Orderlee/dev
    - [c2251615] Merge pull request #61 from Orderlee/fix/migrate-duckdb-pg-explicit-cols
    - [8f0214be] Merge pull request #67 from Orderlee/dev
    - [f8c53db0] Merge pull request #65 from Orderlee/fix/ls-presign-import
    - [c10814d5] Merge pull request #66 from Orderlee/dev

  --- 2026-05-15 자동 기록 (상세) ---
  1) 인프라/설정 파일 변경: docker/.env.test.example
    - 문제: 당일 변경 중 재발 방지 또는 구조 보정이 필요한 항목으로 분류됨.
    - 원인: 커밋 메시지와 변경 파일 기준으로 트러블슈팅 성격의 작업으로 감지됨.
    - 조치:
      - 관련 커밋과 변경 파일을 기준으로 런북 항목에 반영함.
  2) 인프라/설정 파일 변경: docker/docker-compose.labelstudio.yaml
    - 문제: 당일 변경 중 재발 방지 또는 구조 보정이 필요한 항목으로 분류됨.
    - 원인: 커밋 메시지와 변경 파일 기준으로 트러블슈팅 성격의 작업으로 감지됨.
    - 조치:
      - 관련 커밋과 변경 파일을 기준으로 런북 항목에 반영함.
  3) 인프라/설정 파일 변경: docker/docker-compose.yaml
    - 문제: 당일 변경 중 재발 방지 또는 구조 보정이 필요한 항목으로 분류됨.
    - 원인: 커밋 메시지와 변경 파일 기준으로 트러블슈팅 성격의 작업으로 감지됨.
    - 조치:
      - 관련 커밋과 변경 파일을 기준으로 런북 항목에 반영함.
  4) 인프라/설정 파일 변경: docs/references/dbeaver-pg-duckdb-test-guide.md
    - 문제: 당일 변경 중 재발 방지 또는 구조 보정이 필요한 항목으로 분류됨.
    - 원인: 커밋 메시지와 변경 파일 기준으로 트러블슈팅 성격의 작업으로 감지됨.
    - 조치:
      - 관련 커밋과 변경 파일을 기준으로 런북 항목에 반영함.
  핵심 파일 변경:
    - `docker/docker-compose.labelstudio.yaml`
    - `docker/docker-compose.yaml`
    - `docs/references/dbeaver-pg-duckdb-test-guide.md`
  관련 커밋 요약:
    - [dc47a8d5] Merge pull request #69 from Orderlee/feat/migrate-minio-to-10.0.0.51
