# Operational Troubleshooting Runbook

Diagnosis and immediate action guide organized by problem type.

Current branch-based runtime baseline:
- `main` = production
- `dev` = test
- Legacy `staging` references remaining in this document refer to the current **test data plane** (`/data/staging.duckdb`, `/home/user/mou/staging/...`) unless noted otherwise.

**Common tools:**
```bash
# DuckDB read (lock-avoidance)
python3 scripts/query_local_duckdb.py --sql "<SQL>"

# Host DB path:      ./docker/data/pipeline.duckdb
# Container DB path: /data/pipeline.duckdb
# Manifest path:     /nas/incoming/.manifests/pending | failed
```

---

## 1. Dagster Server / Process

### Dagster UI Unreachable (Port Conflict, LOCATION_ERROR)
```bash
docker logs pipeline-dagster-1 | tail -n 100
docker compose restart dagster
ss -tln | grep 3030
```

### Test Daemon Heartbeat Collision
- **Cause:** Test shares the same Dagster runtime storage as production
- **Permanent fix:**
  - Test runtime uses a separate `DAGSTER_HOME`/storage from production
  - Runtime storage is isolated outside the git working tree
- **Verification:** No heartbeat collision logs after restart, sensor ticks cycling normally

### STARTED/CANCELING Runs Blocking for a Long Time (Backpressure Issue)
- **Cause:** `duckdb_writer=true` (legacy dispatch) or `duckdb_raw_writer` / `duckdb_label_writer` / `duckdb_yolo_writer` slot contention. Worker process is gone but `STARTED` / `CANCELING` runs linger in the UI.
- **Immediate action:**
  ```bash
  # 1) Check for abnormal run statuses
  docker exec pipeline-dagster-1 bash -lc "python3 - <<'PY'
  from dagster import DagsterInstance
  from dagster._core.storage.dagster_run import RunsFilter, DagsterRunStatus

  inst = DagsterInstance.get()
  runs = inst.get_runs(
      filters=RunsFilter(statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.CANCELING]),
      limit=20,
  )
  for run in runs:
      print(run.run_id, run.job_name, run.status)
  PY"

  # 2) Dry-run first to check which runs would be terminated
  docker exec pipeline-dagster-1 bash -lc "
    python3 /src/vlm/scripts/repair_stale_dagster_runs.py --dry-run \
      41c4fe3d-0072-4a33-9af2-4acf0055c5f6 \
      3dc73214-d91b-48fb-8e67-aff3f8a7a717 \
      8e455842-55d5-4c4f-b90e-4200d682cf70
  "

  # 3) Run actual repair
  docker exec pipeline-dagster-1 bash -lc "
    python3 /src/vlm/scripts/repair_stale_dagster_runs.py \
      41c4fe3d-0072-4a33-9af2-4acf0055c5f6 \
      3dc73214-d91b-48fb-8e67-aff3f8a7a717 \
      8e455842-55d5-4c4f-b90e-4200d682cf70
  "

  # 4) After 30–60 seconds, verify the queue relaunches
  docker exec pipeline-dagster-daemon-1 bash -lc "tail -n 200 /opt/dagster/logs/daemon.log | grep -E 'Launching run|QueuedRunCoordinator|backpressure' | tail -n 50"
  ```
- **Permanent fix:**
  ```
  STUCK_RUN_GUARD_ENABLED=true
  STUCK_RUN_GUARD_INTERVAL_SEC=120
  STUCK_RUN_GUARD_TIMEOUT_SEC=10800
  STUCK_RUN_GUARD_ORPHANED_RUN_TIMEOUT_SEC=900
  STUCK_RUN_GUARD_AUTO_REQUEUE_ENABLED=true
  STUCK_RUN_GUARD_TARGET_JOBS=mvp_stage_job,ingest_job,motherduck_sync_job
  ```
- **Disk / build cache check:** If you see `database or disk is full` or `disk I/O error`, check the following before cleaning up stale runs:
  ```bash
  df -h /
  docker system df
  docker image prune -f
  docker builder prune -f
  ```

### git switch Blocked (Legacy Test Runtime File Collision)
- **Cause:** Runtime files such as `runs.db`, `schedules.db`, `.nux/`, `.telemetry/` created inside the git working tree
- **Permanent fix:** Keep test Dagster storage outside the git working tree. Stop the test container before switching branches.

---

## 2. DuckDB

### raw_files Table Does Not Exist
- **Typical error:** `Catalog Error: Table with name raw_files does not exist`
- **Cause:** DB file exists but tables have not been created; queried immediately
- **Immediate recovery:**
  ```bash
  python3 - <<'PY'
  import duckdb; from pathlib import Path
  ddl = Path('/src/vlm/vlm_pipeline/sql/schema.sql').read_text(encoding='utf-8')
  conn = duckdb.connect('/data/pipeline.duckdb'); conn.execute(ddl); conn.close()
  PY
  ```
- **Permanent fix:** Call `db.ensure_schema()` immediately after `ingested_raw_files()` starts

### DuckDB Lock / Conflict
- **Typical errors:** `Could not set lock on file`, `Conflicting lock is held`
- **Diagnosis:**
  ```bash
  lsof ./docker/data/pipeline.duckdb
  python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files;"
  ls -1 /nas/incoming/.manifests/pending/retry_*.json 2>/dev/null | tail -n 20
  ```
- **Action:** Wait for writer run to finish → confirm transient errors are auto-captured by retry manifests → adjust backpressure values if queue is overloaded
- **Resolution criteria:** Errors absorbed via retry manifests, no continued increase in `raw_files.failed`

### WAL Problem When Replacing DuckDB File
- **Cause:** Existing `.wal` remains after DB file replacement and stale WAL is replayed
- **Immediate action:** Stop the application → move stale WAL to a separate path
- **Operational rule:** Always replace DB files with the service stopped; always check whether an existing WAL is present

### Checksum Duplicates (Missing UNIQUE Constraint)
- **Cause:** Tables created with `CREATE TABLE IF NOT EXISTS` before the current `UNIQUE(checksum)` constraint was introduced do not have it applied automatically
- **Diagnosis sequence:**
  1. Investigate `raw_key` duplicates and `checksum` duplicates separately
  2. Check `archive_path` duplicates separately
  3. Verify whether `UNIQUE(checksum)` actually exists in the production DB
  4. Do not trust DB checksums blindly — re-hash from archive source files
- **Recovery:**
  ```bash
  python3 scripts/recompute_archive_checksums.py  # recompute checksums from archive
  python3 scripts/cleanup_duplicate_assets.py      # clean up duplicate groups
  # ※ Always back up the DB before running
  ```

### image_metadata Migration Error
- **Typical error:** `image_metadata__migrated does not exist`
- **Cause:** Risky runtime behavior in `ensure_schema()`, schema catalog state inconsistency
- **Action:**
  ```bash
  python3 scripts/repair_image_metadata_schema.py
  # Then restart the production Dagster, clean up stale runs, and re-run
  ```

### Test DuckDB Not Found
- **Typical error:** `DuckDB not found: /data/staging.duckdb`
- **Recovery options:**
  - Reproduce production state: copy `pipeline.duckdb` → `staging.duckdb`
  - Fresh test re-run: delete `staging.duckdb` and create a new DB with only the schema applied

---

## 3. Sensor / auto_bootstrap

### auto_bootstrap 180-Second gRPC Timeout
- **Typical errors:** `DagsterUserCodeUnreachableError`, `Deadline Exceeded`
- **Cause:** NAS/NFS latency causes discovery/scan I/O to exceed the limit, including hidden entries
- **Recommended settings for NAS latency:**
  ```
  AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES=20
  AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK=3
  DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS=300
  ```
- **On recurrence, check:**
  1. Whether hidden entry discovery is included (`.Trash-1000`, `.DS_Store`, etc.)
  2. Values of `max_units_per_tick` and `discovery_max_top_entries`
  3. `processed_units/budget/discovery_elapsed/scan_elapsed` in recent tick logs

### Sensor Scan Delay (Folder Growth)
- **Cause:** Insufficient scan budget, missing `_DONE` marker check
- **Action:**
  ```
  AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK=100  # (increase from default if needed)
  ```
- **Check:** `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK` value in `.env`, presence of `_DONE` files inside date folders

### Pending Queue Overload
- **Diagnosis:**
  ```bash
  find /nas/incoming/.manifests/pending -maxdepth 1 -name '*.json' | wc -l
  ls -lt /nas/incoming/.manifests/pending/*.json 2>/dev/null | head -n 20
  ```
- **Recommended defaults:**
  ```
  INCOMING_SENSOR_INTERVAL_SEC=180
  AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC=180
  AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS=200
  AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK=20
  INCOMING_SENSOR_MAX_IN_FLIGHT_RUNS=2
  ```
- **Resolution criteria:** Pending backlog stabilizes, lock/conflict recurrence frequency decreases

---

## 4. MinIO

### Endpoint Mix-up (Production 9000/9001, Test 9002/9003)
- **Cause:** Confusion between Console port and S3 API port
- **Action:** Keep production at `MINIO_ENDPOINT=http://10.0.0.51:9000` and test at `MINIO_ENDPOINT=http://10.0.0.51:9002`
- `9001 = production Console`, `9003 = test Console`

### Console Download Failure (Test)
- **Cause:** Not object corruption. Issue with `Console(9003) -> API(9002)` routing or browser session.
- **Diagnosis sequence:**
  1. Confirm the object exists at the test endpoint (`9002`)
  2. Verify directly with `boto3.head_object()` / `get_object()`
  3. Confirm actual download via presigned URL or `download_file()`
- **Operational rule:** Do not recreate or delete objects based solely on a Console download failure

### Bucket Not Auto-Created
- **Cause:** MinIO does not auto-create buckets on write. `ensure_bucket()` helper is not called in the upload/copy path.
- **Permanent fix:** Call `_ensure_bucket_once()` before every `upload()`, `upload_fileobj()`, and `copy()`

### `YYYY/MM` Prefix Mixed into raw_key
- **Cause:** Legacy ingest logic prepended a `datetime.now().strftime("%Y/%m")` prefix
- **Current correct rule:** `raw_key = <source_unit_name>/<rel_path>` (e.g. `source-b-event-bucket/20260204/fire_1_131000.mp4`)
- **Recovery:**
  ```bash
  python3 scripts/reupload_minio_from_archive.py  # re-upload aligned to archive baseline
  # Also update DuckDB raw_files.raw_key to the same convention
  ```

### Frame Path Rules
- **Correct path:** `vlm-processed/<raw-prefix>/<video-stem>/<video-stem>_00000001.jpg`
- **Forbidden:** `_tmp/...` prefix, `/frames/` subdirectory, filenames without the original stem
- **Cleanup required after path change:**
  1. Cancel in-progress extraction runs
  2. Clean up the `vlm-processed` bucket
  3. Delete `video_frame` rows from `image_metadata`
  4. Reset `video_metadata.frame_extract_*`
  5. Re-run extraction

---

## 5. Ingest

### raw_files vs video_metadata Count Mismatch
- **Diagnosis:**
  ```bash
  python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files;"
  python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM video_metadata;"
  python3 scripts/query_local_duckdb.py --sql "
    SELECT COUNT(*) AS missing FROM raw_files rf
    LEFT JOIN video_metadata vm ON rf.asset_id = vm.asset_id
    WHERE rf.media_type='video' AND vm.asset_id IS NULL;"
  ```
- **Action:**
  ```bash
  # Host
  python3 scripts/backfill_video_metadata.py --db ./docker/data/pipeline.duckdb --statuses completed --log-every 20
  # Container
  python3 /src/vlm/scripts/backfill_video_metadata.py --db /data/pipeline.duckdb --statuses completed --log-every 20
  ```
- **Resolution criteria:** `missing_video_meta=0`

### Spike in Failed Records
- **Diagnosis:**
  ```bash
  python3 scripts/query_local_duckdb.py --sql "
    SELECT ingest_status, COUNT(*) FROM raw_files GROUP BY 1 ORDER BY 1;"
  python3 scripts/query_local_duckdb.py --sql "
    SELECT COALESCE(error_message,'(null)') AS msg, COUNT(*) AS cnt
    FROM raw_files WHERE ingest_status='failed' GROUP BY 1 ORDER BY cnt DESC LIMIT 30;"
  ls -lt /nas/incoming/.manifests/failed/*.jsonl 2>/dev/null | head
  ```
- **Action:** File errors (`file_missing`, `empty_file`, `ffprobe_failed`) are excluded from DB insertion — recover the source file and re-ingest
- **Resolution criteria:** Same error does not recur, only failure logs remain with no DB corruption

### Archive Move Failure
- **Diagnosis:**
  ```bash
  python3 scripts/query_local_duckdb.py --sql "
    SELECT asset_id, source_path, archive_path, error_message
    FROM raw_files WHERE ingest_status='failed' AND error_message LIKE 'archive_move_failed%'
    ORDER BY updated_at DESC LIMIT 30;"
  find /nas/archive -type f -name '<filename>' | head
  ```
- **Action:**
  - Archive exists → recover as `completed + archive_path`
  - Archive does not exist → re-issue manifest for reprocessing
- **Resolution criteria:** Files confirmed in archive have status `completed`, no orphan rows

---

## 6. GCS Download

### Zero-Byte Files
- **Diagnosis:**
  ```bash
  find /nas/incoming/gcp -type f \( -iname '*.mp4' -o -iname '*.mov' -o -iname '*.jpg' \) -size 0 | head -n 30
  ```
- **Action:**
  ```bash
  python3 gcp/download_from_gcs_rclone.py \
    --download --mode date-folders \
    --download-dir /nas/incoming/gcp \
    --buckets source-a-rtsp-bucket \
    --zero-byte-retries 4
  ```
- **Resolution criteria:** Zero zero-byte media files, target folder has a valid `_DONE` marker

### Auth / Permission Check
```bash
gcloud auth list
gsutil ls gs://source-a-rtsp-bucket/
# Required permissions: storage.objects.list, storage.objects.get
```

---

## 7. Gemini / VertexAI

### vertexai Import Failure
- **Typical error:** `ModuleNotFoundError: No module named 'vertexai'`
- **Cause:** `google-cloud-aiplatform` not installed or missing from Docker dependency
- **Permanent fix:**
  - Add `google-cloud-aiplatform` to `docker/app/requirements.txt` and `pyproject.toml`
  - Rebuild Docker image, then restart `app` and `dagster`
- **Verification:**
  ```bash
  docker exec pipeline-app-1 python3 -c "import vertexai"
  docker exec pipeline-dagster-1 python3 -c "from gemini.assets.config import VIDEO_PROMPT"
  ```

### Gemini Credentials Not Found (Test)
- **Typical error:** `FileNotFoundError: Gemini credentials not found`
- **Cause:** Credential path missing from `docker/.env.test`
- **Permanent fix:** Keep at least the following in `docker/.env.test`:
  ```
  GEMINI_PROJECT=<project>
  GEMINI_LOCATION=<location>
  GEMINI_GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/<service-account>.json
  ```

### Large File Payload Exceeded
- **Typical error:** `400 Request payload size exceeds the limit: 524288000 bytes`
- **Cause:** Source video (>500 MB) included directly in the request payload
- **Permanent fix:** For videos >450 MB, generate a preview mp4 before calling Gemini (audio removed, reduced resolution/fps/bitrate)
- **When estimating cost:** Separate the raw-source scenario from the current-pipeline-preview scenario

### ffmpeg Temp File Overwrite Error
- **Typical error:** `ffmpeg_clip_extract_failed: File '/tmp/tmp....mp4' already exists. Overwrite? [y/N]`
- **Cause:** Clip output temp path pre-created with `NamedTemporaryFile(delete=False)` → ffmpeg prompts for overwrite, gets non-interactive `N`
- **Permanent fix:** Pass only a temp path string that does not yet exist to ffmpeg output. Clean up partial temp files on failure.

---

## 8. YOLO

### Model / Dependency Issues
- **Model in use:** `yolov8l-worldv2.pt` (`docker/data/models/yolo/yolov8l-worldv2.pt`)
- **Required env vars:**
  ```
  YOLO_MODEL_PATH=/data/models/yolo/yolov8l-worldv2.pt
  YOLO_DEFAULT_CLASSES=...
  ```
- **Missing CLIP dependency:** YOLO-World image requires `git+https://github.com/ultralytics/CLIP.git`
- **Verification:**
  ```bash
  # pipeline-yolo-1 is healthy
  # /health returns model_loaded=true
  # /info model path matches expected value
  ```

### YOLO Execution Order Problem
- **Cause:** YOLO asset fires before frame generation completes
- **Permanent fix:** Isolate legacy test-only YOLO asset to guarantee order `raw_ingest -> clip/frame generation -> yolo_image_detection`

---

## 9. Data Consistency

### Local vs MotherDuck Mismatch
- **Diagnosis:**
  ```bash
  # Local
  python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files;"
  python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM video_metadata;"
  python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files WHERE ingest_status='failed';"
  # MotherDuck: re-run the same queries after running motherduck_sync_job
  ```
- **Action:** Clean up local anomalies first → run sync → verify simultaneously in local/cloud with the same queries
- **Manual MotherDuck sync:**
  ```bash
  python3 /src/python/local_duckdb_to_motherduck_sync.py \
    --db pipeline_db --local-db-path /data/pipeline.duckdb \
    --share-update MANUAL --tables raw_files video_metadata
  ```

### Archive / MinIO / DuckDB / MotherDuck Count Mismatch
- **Reconciliation approach:**
  1. Full audit of files in archive that are not in the DB
  2. Classify excess files into three categories: operational markers (`_DONE`) / junk files (`.DS_Store`) / actual data
  3. Rules: `_DONE` → keep, `.DS_Store` → delete, actual data → determine duplicates by checksum
- **Operational standard:** "Consistent" means the **archive data file count** matches, not the total physical file count in archive

### Manual MotherDuck Sync
```bash
python3 /src/python/local_duckdb_to_motherduck_sync.py \
  --db pipeline_db --local-db-path /data/pipeline.duckdb \
  --share-update MANUAL --tables raw_files video_metadata
```

---

## 10. Test Reset

Full reset sequence for a clean test re-run:
```bash
# 1. Stop test runtime
# 2. Delete all test MinIO objects (endpoint: 10.0.0.51:9002)
#    - vlm-raw, vlm-labels, vlm-processed, vlm-dataset
# 3. Delete test DuckDB
rm docker/data/staging.duckdb
# 4. Delete test Dagster runtime DB
rm -rf docker/data/dagster_home_staging/storage
# 5. Restart
```

**Never delete:**
- `/home/user/mou/staging/incoming`
- `/home/user/mou/staging/archive`

**Required test data plane volume mounts:**
```yaml
- /home/user/mou/staging/incoming:/nas/staging/incoming
- /home/user/mou/staging/archive:/nas/staging/archive
```

---

## 11. Label Studio

### Initial Startup Sequence
```bash
docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml up -d
# Access LS → create admin account → issue API key from Account Settings
# Set LS_API_KEY and WEBHOOK_HOST in .env, then restart ls-webhook
```

### Webhook Registration (Once per Project After Creation)
```bash
python src/gemini/ls_webhook.py register --project <project_id>
python src/gemini/ls_webhook.py list  # confirm registration
```

### Presigned URL Expiry (Default 7 Days)
```bash
python src/gemini/ls_tasks.py renew --project-name <project_name>
```

### ls_task_create_sensor Not Running
- Dagster UI → Sensors → manually turn ON `ls_task_create_sensor` (default=STOPPED)
- Job fails if `LS_API_KEY` is not set → check `.env`

### LS → MinIO Access Failure (Presigned URL Error)
- Presigned URLs are generated based on `MINIO_ENDPOINT` → verify the LS container can reach that address
- `docker exec pipeline-labelstudio-1 curl -I <presigned_url>`

---

## 11. NAS (CIFS) Failure Response

### Symptom: Pipeline Hang (archive_finalize Stalls, File Access Timeout)

**Diagnosis:**
```bash
# Check NAS network connectivity
timeout 3 ping -c 3 10.0.0.51

# Test NAS file access (5-second timeout)
timeout 5 stat /home/user/mou/incoming/
timeout 5 ls /home/user/mou/archive/

# CIFS connection statistics (reconnect count, open files, errors)
cat /proc/fs/cifs/Stats

# Kernel CIFS error logs
sudo dmesg | grep -i cifs | tail -20
```

**Warning signs:**
- `open on server` value is negative → CIFS session state corruption
- Reconnect count increasing rapidly → SMB service instability
- `stat`/`ls` commands time out → NAS I/O hang

**Immediate action: CIFS remount**
```bash
sudo umount -l /home/user/mou/incoming
sudo umount -l /home/user/mou/archive
sudo umount -l /home/user/mou/staging
sudo mount -a
# Verify
timeout 5 ls /home/user/mou/incoming/
```

**Pipeline protection mechanisms:**
- `raw_ingest` runs a NAS health check at startup (5-second timeout, skip on failure)
- `archive_finalize` applies a timeout to `shutil.move` (`ARCHIVE_MOVE_TIMEOUT_SEC`, default 300 seconds)
- On timeout, skip the archive move and keep uploads already completed as `completed` → continue to subsequent steps

### Recommended CIFS Mount Options

Adding the following to the current `/etc/fstab` base options speeds up NAS failure detection and recovery:

```
# Recommended additional options
soft,echo_interval=30,actimeo=1,closetimeo=1
```

| Option | Current | Recommended | Effect |
|------|------|------|------|
| `soft` | Not set | Add | Returns an error when unresponsive (hard mount waits indefinitely) |
| `echo_interval` | 60 | 30 | Halves the connection loss detection interval |
| `actimeo` | Not set | 1 | 1-second file attribute cache (already applied) |

**How to apply (requires sudo):**
```bash
# After editing /etc/fstab
sudo mount -o remount /home/user/mou/incoming
sudo mount -o remount /home/user/mou/archive
```

### Related Environment Variables

| Variable | Default | Purpose |
|------|--------|------|
| `ARCHIVE_MOVE_TIMEOUT_SEC` | 300 | archive shutil.move timeout (seconds) |
| `INGEST_META_WORKERS` | 4 | Parallel workers for checksum/ffprobe (max 8) |
| `INGEST_UPLOAD_WORKERS` | 4 | Parallel workers for MinIO uploads (max 16) |
