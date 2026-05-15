# Label Studio Operations Guide

> Production MinIO → Label Studio integration operational procedures

---

## Architecture summary

```
MinIO (vlm-processed | vlm-raw)
    ↓ --auto-detect (video first, image fallback)
    ↓ presigned URL
Label Studio (project/task creation with label_config per media_type)
    ↓ annotation complete
ls-webhook (receives annotation)
    ↓
MinIO (vlm-labels/*/events/*.json updated)
DuckDB (labels table updated)
```

**Supported buckets & media types:**

| Bucket | Purpose | prefix convention | Media |
|------|------|-------------|--------|
| `vlm-processed` | Clips/frames after Gemini labeling | `{folder}/clips` | video, image |
| `vlm-raw` | Urgent raw footage/images | `{folder}` as-is | video, image |

**Automated path:**
1. dispatch complete → `dispatch_requests.status='completed'`, target bucket recorded in `bucket` column
2. `ls_task_create_sensor` detects → runs `ls_tasks.py create --bucket <bucket> --auto-detect`
3. Media type auto-detected → LS project/task created with label_config per media_type
4. If video, Gemini prediction automatically attached (references vlm-labels JSON)
5. Labeler completes annotation → webhook calls `ls_sync`
6. MinIO JSON + DuckDB labels synchronized

---

## Startup procedure

### 1. Prerequisites

- Main Docker Compose must be running (postgres, pipeline-network)
- LS-related environment variables must be set in `docker/.env`

### 2. Environment variable setup

Add the following to `docker/.env`:

```bash
# === Label Studio ===
LS_API_KEY=<LS account Settings → Access Token>
LS_PORT=8084
WEBHOOK_HOST=ls-webhook
LS_WEBHOOK_PORT=8003
LS_TASK_SENSOR_INTERVAL_SEC=60

# (optional) Slack notifications
SLACK_WEBHOOK_URL=
SLACK_SIGNING_SECRET=
```

> `LS_API_KEY`: The sequence is to start LS first, create an account, issue a token, then set it.

### 3. Compose startup

```bash
cd docker

# postgres + network first
docker compose up -d postgres

# Start LS + webhook (local image datapipeline:gpu-cu124 must exist)
docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml up -d

# Start only LS/DB first without webhook (if image pull fails)
docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml up -d postgres labelstudio
```

`ls-webhook` uses `image: datapipeline:gpu-cu124` (local build). Since it is not on Docker Hub and `pull access denied` will appear, build the image from the repository root with `docker compose -f docker/docker-compose.yaml build` then `up` again.

### 4. LS initial setup

1. Access `http://<HOST>:8084`
2. Create account (signup)
3. Top right → Account & Settings → Copy **Access Token**
4. Set `LS_API_KEY=<token>` in `docker/.env`
5. Restart `ls-webhook` container:
   ```bash
   docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml restart ls-webhook
   ```

### 5. Activate Dagster sensor

Dagster UI (`http://<HOST>:3030`) → Sensors → `ls_task_create_sensor` → **ON**

---

## Operational scenarios

### A. Pipeline automated flow (vlm-processed) — no manual work

```
Footage collection → Gemini labeling → clip cutting → stored in vlm-processed
    → dispatch_requests (status='completed', bucket='vlm-processed')
    → ls_task_create_sensor auto-detects
    → ls_tasks.py --bucket vlm-processed create --prefix {folder}/clips --auto-detect
    → LS project + task auto-created + Gemini prediction attached
```

### B. Urgent raw footage (vlm-raw) — manual dispatch registration

For raw footage not going through the pipeline, manually register a dispatch request and the sensor handles it automatically.

```bash
# Insert dispatch request into DuckDB
python3 scripts/query_local_duckdb.py --sql "
  INSERT INTO dispatch_requests (request_id, folder_name, bucket, status, ls_task_status)
  VALUES ('manual-$(date +%s)', 'S-OIL/falldown', 'vlm-raw', 'completed', 'pending')
"
```

Or run directly without the sensor:

```bash
python src/gemini/ls_tasks.py \
  --bucket vlm-raw \
  create --prefix S-OIL/falldown --auto-detect
```

### C. Image data

For prefixes with no video, `--auto-detect` automatically falls back to image.
Image label config (`<Image>` + `<Choices>` + `<RectangleLabels>`) is applied.

```bash
python src/gemini/ls_tasks.py \
  --bucket vlm-processed \
  create --prefix vanguardhealthcarevhc/falldown/image --auto-detect
```

---

## Project naming convention

In `--auto-detect` mode, `build_project_name()` generates a project name **including the client name** from the prefix.

| prefix | Generated project name |
|--------|---------------------|
| `vanguardhealthcarevhc/falldown/clips` | `vanguardhealthcarevhc__falldown` |
| `hyundai_v2/01_27_collection_data` | `hyundai_v2__01_27_collection_data` |
| `S-OIL/smoke` | `S-OIL__smoke` |

Structural directory names such as `clips`, `image`, `images`, `frames`, `video`, `videos` are automatically skipped.

---

## Presigned URL renewal

The default validity period for MinIO presigned URLs is **7 days**.

### Automatic renewal (schedule)

`ls_presign_renew_schedule` runs **daily at 05:00 KST** and
automatically renews URLs expiring within 1 day.

Visible in Dagster UI → Schedules.

### Manual renewal

```bash
# Specific project
python src/gemini/ls_tasks.py renew --project-name <project_name>

# All projects
python src/gemini/ls_tasks.py renew --all-projects
```

---

## Webhook management

### Automatic registration

When **creating a new project** with `ls_tasks.py create`, the webhook is automatically registered.
`WEBHOOK_HOST` must be correctly configured.

### Manual registration/verification

```bash
# Register webhook for a specific project
python src/gemini/ls_webhook.py register --project <project_id>

# List registered webhooks
python src/gemini/ls_webhook.py list
```

---

## Label synchronization (ls_sync)

On annotation completion, the webhook automatically calls `ls_sync`.

### Manual synchronization

```bash
python src/gemini/ls_sync.py --project <project_id> --api-key <token>

# dry-run (check changes only)
python src/gemini/ls_sync.py --project <project_id> --api-key <token> --dry-run
```

### Responding to DuckDB lock errors

`ls_sync` automatically retries with exponential backoff on DuckDB write (up to 5 times).
Auto-recovery works in most cases even when running concurrently with a Dagster run.

---

## Slack integration (optional)

When `SLACK_WEBHOOK_URL` and `SLACK_SIGNING_SECRET` are configured:

- Slack notification sent on annotation completion
- Slash commands available:
  - `/sync-list` — list of projects awaiting confirmation
  - `/sync-approve <project_id>` — confirm `label_status='completed'`

---

## Labeling interface

In `--auto-detect` mode, the label config is automatically selected based on the detected media_type.

### Video (media_type="video")

```xml
<View>
  <TimelineLabels name="videoLabels" toName="video">
    <Label value="fall" background="#e74c3c"/>
    <Label value="fight" background="#e67e22"/>
    <Label value="smoke" background="#95a5a6"/>
    <Label value="fire" background="#e74c3c"/>
    <Label value="unsafe_act" background="#f39c12"/>
  </TimelineLabels>
  <Video name="video" value="$video" timelineHeight="120" />
</View>
```

### Image (media_type="image")

```xml
<View>
  <Image name="image" value="$image" />
  <Choices name="imageClass" toName="image" choice="multiple">
    <Choice value="fall" />
    <Choice value="fight" />
    <Choice value="smoke" />
    <Choice value="fire" />
    <Choice value="unsafe_act" />
  </Choices>
  <RectangleLabels name="bbox" toName="image">
    <Label value="fall" background="#e74c3c"/>
    <Label value="fight" background="#e67e22"/>
    <Label value="smoke" background="#95a5a6"/>
    <Label value="fire" background="#e74c3c"/>
    <Label value="unsafe_act" background="#f39c12"/>
  </RectangleLabels>
</View>
```

To change label types: modify `_label_config_for_media()` in `src/gemini/ls_tasks.py`
or edit directly in LS UI → Project Settings → Labeling Interface

---

## Troubleshooting

| Symptom | Cause | Resolution |
|------|------|------|
| `pull access denied for datapipeline` | `ls-webhook` uses a local-only image | Run `docker compose -f docker/docker-compose.yaml build` then `up`, or run only `up -d postgres labelstudio` |
| `pipeline-labelstudio-1` missing / cannot connect | Pipeline stack is down | `docker compose … up -d postgres labelstudio` or start the full stack |
| `OperationalError: [Errno -3] Try again` (Postgres) | LS is attached only to a different Docker network than Postgres | Check that `labelstudio` in `docker-compose.labelstudio.yaml` uses both `default`+`pipeline-network`, and that `POSTGRE_HOST=postgres` |
| Video/image not visible in LS | Presigned URL expired | `ls_tasks.py renew --project-name <name>` |
| Webhook annotation not received | Webhook not registered or WEBHOOK_HOST error | Check with `ls_webhook.py list`, register with `register` if needed |
| "database is locked" error | Simultaneous DuckDB write with Dagster | Wait for auto-retry (up to 30 seconds). If persistent, check Dagster run |
| Sensor not creating LS tasks | Sensor is STOPPED | Turn `ls_task_create_sensor` ON in Dagster UI |
| LS_API_KEY error | Token expired or not set | LS UI → Settings → issue new token |
| `--auto-detect` but no media | Incorrect prefix path or files not uploaded | Verify bucket/prefix in MinIO console |
| vlm-raw task not auto-created | Manual registration not done in dispatch_requests | Refer to **Scenario B** above for DB insert |

---

## Duplicate prevention

`ls_tasks.py create` indexes existing task media stems and **automatically skips already-registered files**.
Running multiple times with the same prefix is safe — no duplicate tasks are created.

---

## CLI reference

```bash
# vlm-processed clips (same as pipeline automated flow)
python src/gemini/ls_tasks.py create --prefix hyundai_v2/01_27_collection_data

# vlm-raw original footage (auto-detect)
python src/gemini/ls_tasks.py --bucket vlm-raw create --prefix S-OIL/falldown --auto-detect

# vlm-processed images (auto-detect)
python src/gemini/ls_tasks.py create --prefix vanguardhealthcarevhc/falldown/image --auto-detect

# Renew URL for a specific project
python src/gemini/ls_tasks.py renew --project-name S-OIL__smoke

# Renew URL for all projects
python src/gemini/ls_tasks.py renew --all-projects
```

---

## Related files

| File | Role |
|------|------|
| `src/gemini/ls_tasks.py` | Task creation, presigned URL renewal, `--auto-detect` media detection |
| `src/gemini/ls_webhook.py` | Webhook receiver server, Slack integration |
| `src/gemini/ls_sync.py` | annotation → MinIO/DuckDB synchronization |
| `src/gemini/ls_import.py` | Manual prediction import |
| `src/vlm_pipeline/defs/ls/sensor.py` | Dagster sensor/schedule, dynamic `--bucket` branching |
| `src/vlm_pipeline/resources/duckdb_ingest_dispatch.py` | dispatch_requests table (includes bucket column) |
| `docker/docker-compose.labelstudio.yaml` | LS + webhook Docker configuration |
