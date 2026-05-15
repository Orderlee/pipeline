# Auto Labeling Functional Specification

## 1. Feature Overview

- Auto labeling is performed based on the `labeling_method`, `categories`, and `classes` defined in the `spec.json` received alongside incoming data. **The execution order is fixed: timestamp → captioning → frame → bbox**, and the captioning step is never skipped.
- If the labeling definition is not yet finalized, the item is classified as pending. Once confirmed via the Slack bot, processing resumes automatically.
- The detailed processing behavior for each task is determined by a per-requester config (parameter JSON). **The config is looked up not in `ingest_router`, but at the start of `auto_labeling` execution (first task, step 3-5).**

## 2. Data Flow (End-to-End)

```
incoming_nas
→ raw_ingest (file validation, MinIO upload, NAS archive move, metadata registration, duplicate detection)
→ ingest_router (spec matching, DB state branching based on whether labeling definition is present)
  ├→ [confirmed] ingest_status = "ready_for_labeling"
  │         → auto_labeling (timestamp → captioning → frame → bbox)
  └→ [pending] ingest_status = "pending_spec"
            → Slack bot confirmation
            → spec_resolve_sensor (DB state transition + auto_labeling trigger)

Physical file movement is completed in raw_ingest.
All branching after ingest_router involves only DB state (ingest_status) transitions.
"pending" refers to raw_files.ingest_status = "pending_spec", not a separate folder.
```

### Data Arrival Order

- **Data arrives first**
  - No spec present, or `labeling_method` not yet finalized → `ingest_status = "pending_spec"` → resumed by sensor once spec is confirmed
- **Spec arrives first**
  - Stored in `labeling_specs` and held → matched by `ingest_router` after data arrives and `raw_ingest` completes

## 3. Detailed Logic per Asset

### 3-1. labeling_spec_ingest

```yaml
asset: labeling_spec_ingest
input:
  - External API / Slackbot / other responses
process:
  - External API / Slackbot / other call → receive spec.json
  - Parse spec.json
    - spec_id
    - requester_id
    - team_id
    - source_unit_name (NAS incoming folder name — matching key to raw_files)
    - categories → auto-derive classes table applied (e.g. smoke → ["smoke"])
    - classes
    - labeling_method (JSON array, e.g. ["timestamp","captioning","bbox"]; [] or null if not yet finalized)
  - INSERT into labeling_specs table
    - labeling_method is empty ([] / null) → spec_status = "pending"
    - labeling_method has 1 or more task codes → spec_status = "active"
output:
  - New row in labeling_specs table
error_handling:
  - External collection failure → up to 3 retries; on failure, recorded as Dagster run error
  - spec.json parse failure → skip that spec, log to Dagster
```

### 3-2. config_sync

```yaml
asset: config_sync
trigger: Manual materialize from Dagster UI
input:
  - JSON files under config/parameters/ folder
process:
  - Scan folder → collect list of JSON files
  - Each filename = config_id
  - Compare against labeling_configs table
    - New file → INSERT
    - Content changed → UPDATE (version + 1)
    - File deleted → is_active = false (row not deleted from DB)
output:
  - labeling_configs table synchronized
error_handling:
  - JSON parse failure → skip that file, warning log
  - Folder not found → record SkipReason, retry on next run
```

### 3-3. ingest_router

> **Design**: The router performs **only the pending vs. ready branching**. It does **not** look up `requester_config_map` / `labeling_configs`. Config is looked up **at the start of the first auto_labeling task (step 3-5)** so that any config changes made after routing are reflected.

```yaml
asset: ingest_router
input:
  - raw_files (ingest_status = "completed")
  - labeling_specs (matched on source_unit_name)
process:
  - Match raw_files ↔ labeling_specs (on source_unit_name)
  - Set raw_files.spec_id
  - Check spec.labeling_method (JSON array)
    - Empty ([] / null) → Branch A (pending)
    - 1 or more elements → Branch B (ready queue)
  - Branch A (pending)
    - ingest_status = "pending_spec"
  - Branch B (auto_labeling queue)
    - ingest_status = "ready_for_labeling"
    - (no config lookup — performed in step 3-5)
output:
  - raw_files.spec_id, ingest_status updated
error_handling:
  - Spec match failure (no spec with same source_unit_name, etc.) → pending_spec or separate handling per policy
```

**Note**: When `spec_resolve_sensor` transitions status to `ready_for_labeling`, it also **does not look up the config**. Config is always looked up in step 3-5.

### 3-4. pending_ingest

```yaml
asset: pending_ingest
input:
  - raw_files (ingest_status = "pending_spec")
process:
  - Aggregate list of data in pending state
  - Record waiting-status metadata
output:
  - Summary of pending count
resolution_path:
  - Slack bot /labeling-confirm → UPDATE labeling_specs (labeling_method, categories, classes, spec_status = 'pending_resolved')
  - spec_resolve_sensor detects → DB state transition + trigger auto_labeling_routed_job
```

### 3-5. clip_timestamp_routed (Event Interval Identification)

> **Config lookup timing**: At **the start of this asset**, it reads `requester_id` and `team_id` from `labeling_specs` using the `spec_id` of the target `raw_files`, and determines the complete `config_json` according to the priority order below. On failure, **this run fails**. Steps 3-6 through 3-8 within the same run use the **same `config_json`** (same `config_id`).

```yaml
asset: clip_timestamp_routed (= timestamp_auto)
input:
  - raw_files (ingest_status = "ready_for_labeling")
  - labeling_specs (look up requester_id, team_id by spec_id)
  - requester_config_map
  - labeling_configs
process:
  - [prerequisite] Config lookup (priority order)
    1. requester_id + scope = "personal" → requester_config_map
    2. team_id + scope = "team" → requester_config_map
    3. config_id = "_fallback" → labeling_configs
    4. If none found → run fails (error log)
  - Load parameters from config_json.timestamp section
    - detection_model
    - confidence_threshold
    - min_event_duration_sec
    - max_event_duration_sec
    - merge_gap_sec
    - pre_event_buffer_sec
    - post_event_buffer_sec
  - Fetch list of target videos
  - Model inference → generate list of event intervals
  - Merge adjacent intervals based on merge_gap_sec
  - Apply pre/post buffer
  - Filter by min/max duration
  - INSERT into labels table
  - Upload label JSON → MinIO (vlm-labels)
output:
  - labels table rows
  - Label JSON in vlm-labels bucket
error_handling:
  - Model inference failure → skip that video, log to Dagster
  - Zero events detected → complete normally (warning log)
  - Timeout → log to Dagster, retry on next run
```

### 3-6. clip_captioning_routed (Caption Generation)

```yaml
asset: clip_captioning_routed (= captioning_auto)
input:
  - labels (label_status = "completed" AND caption_text IS NULL)
  - raw_files (access to source video)
  - labeling_configs (captioning section of config_json confirmed in step 3-5)
process:
  - Load config_json using the same rules as step 3-5, then load captioning section parameters
    - model
    - language
    - max_caption_length
    - generate_summary
    - include_object_list
  - Extract video segment for each event interval
  - Generate caption with AI model
  - UPDATE labels.caption_text
  - Update label JSON → MinIO (vlm-labels)
output:
  - labels.caption_text updated
  - Label JSON updated in vlm-labels bucket
error_handling:
  - Caption generation failure → skip that interval, log to Dagster
  - Empty caption returned → warning log, caption_text remains null
```

### 3-7. clip_to_frame_routed (Frame Extraction)

```yaml
asset: clip_to_frame_routed (= frame_extraction_auto)
input:
  - labels (event intervals with completed captions)
  - raw_files (source video)
  - labeling_configs (frame_extraction section of the same config_json as step 3-5)
process:
  - Load config parameters
    - frame_interval_sec
    - max_frames_per_clip
    - output_format
    - output_quality
    - resize_width
    - resize_height
    - keep_aspect_ratio
    - extract_key_frames_only
  - Cut event intervals into clip videos
  - Extract frame images from clips at configured intervals
  - INSERT into processed_clips table
  - INSERT into image_metadata table
  - Upload artifacts → MinIO (vlm-processed)
output:
  - processed_clips table rows
  - image_metadata table rows
  - Clips + frames in vlm-processed bucket
error_handling:
  - Corrupted video → skip that clip, log to Dagster
  - Insufficient disk/memory → halt processing, preserve partial results
```

### 3-8. bbox_labeling (Object Detection + Bounding Box)

```yaml
asset: bbox_labeling (= bbox_labeling_auto)
input:
  - image_metadata (images with completed frame extraction)
  - labeling_specs (classes: detection target classes)
  - labeling_configs (bbox section of the same config_json as step 3-5)
process:
  - Load config parameters
    - detection_model
    - confidence_threshold
    - nms_threshold
    - max_detections_per_image
  - target_classes = intersection(spec.classes, config.target_classes) (if defined)
  - Load frame images
  - Object detection model inference
    - bbox
    - class
    - confidence
  - INSERT/UPDATE labels table (object_count, bbox info)
  - Upload bbox label JSON → MinIO (vlm-labels)
output:
  - labels table (including bbox info)
  - Bbox label JSON in vlm-labels bucket
error_handling:
  - Model inference failure → skip that image, log to Dagster
  - Zero objects detected → complete normally (object_count = 0)
```

## 4. Sensor

### spec_resolve_sensor

- **[Role]** Detects `spec_status = 'pending_resolved'` in `labeling_specs` → DB state transition + triggers `auto_labeling`
- **[Behavior]**
  1. Periodic polling (60-second interval)
  2. Query: `spec_status = 'pending_resolved' AND retry_count < 3`
  3. On match:
     - `labeling_specs.retry_count += 1`
     - Update related `raw_files.ingest_status` → `'ready_for_labeling'`
     - Trigger `auto_labeling_routed_job` (RunRequest)
     - `spec_status` is not changed (remains `pending_resolved`)
  4. On job success:
     - Job internally updates `spec_status` → `'active'`
  5. On job failure:
     - `spec_status = 'pending_resolved'` is maintained → re-detected on next polling cycle
     - If `retry_count >= 3` → transition `spec_status` → `'failed'` + warning log
- **[Trigger cause]** Slack bot calls `/labeling-confirm` → UPDATE `labeling_specs` → `spec_status = 'pending_resolved'`
- **[Notes]** The sensor directly performs DB state transitions internally (no need to re-run `ingest_router`). **Config is not looked up** (performed in step 3-5). Data has already been archived by `raw_ingest`, so no physical file movement occurs.

## 5. Slack Bot Commands

- `/labeling-confirm <spec_id> --method <task1,task2,...> --categories <cat1,cat2> --classes <cls1,cls2>`  
  → UPDATE `labeling_specs` (stores `labeling_method` as JSON array, updates `categories`, `classes`, `spec_status = 'pending_resolved'`)  
  - Example: `--method timestamp,captioning,bbox` → `["timestamp","captioning","bbox"]`
- `/labeling-pending [--requester <requester_id>]` → Returns list of specs in pending state
- `/labeling-status <spec_id>` → Returns current status of the spec, count of linked data records, and processing progress

## 6. DB Table Additions

### 6-1. New Tables

```
┌─────────────────────────────────────────────────────────┐
│ labeling_specs                                          │
├──────────────────┬──────────┬───────────────────────────┤
│ Column           │ Type     │ Description               │
├──────────────────┼──────────┼───────────────────────────┤
│ spec_id (PK)     │ VARCHAR  │ Unique spec identifier    │
│ requester_id     │ VARCHAR  │ Requester identifier      │
│ team_id          │ VARCHAR  │ Team identifier           │
│ source_unit_name │ VARCHAR  │ NAS incoming folder name (matching key to raw_files) │
│ categories       │ JSON     │ List of labeling target categories │
│ classes          │ JSON     │ List of labeling target classes    │
│ labeling_method  │ JSON     │ e.g. ["timestamp","captioning","bbox"]; [] if not finalized │
│ spec_status      │ VARCHAR  │ pending | pending_resolved | active | failed │
│ retry_count      │ INTEGER  │ Sensor re-trigger count (default 0, max 3) │
│ created_at       │ TIMESTAMP│ Creation timestamp        │
│ updated_at       │ TIMESTAMP│ Last modified timestamp   │
└──────────────────┴──────────┴───────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ labeling_configs                                        │
├──────────────────┬──────────┬───────────────────────────┤
│ Column           │ Type     │ Description               │
├──────────────────┼──────────┼───────────────────────────┤
│ config_id (PK)   │ VARCHAR  │ Unique config identifier  │
│ config_json      │ JSON     │ timestamp, captioning, frame_extraction, bbox │
│ version          │ INTEGER  │ Version (incremented on change) │
│ is_active        │ BOOLEAN  │ Active flag (FALSE when file is deleted) │
│ created_at       │ TIMESTAMP│ Initial registration timestamp │
│ updated_at       │ TIMESTAMP│ Last modified timestamp   │
└──────────────────┴──────────┴───────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ requester_config_map                                    │
├──────────────────┬──────────┬───────────────────────────┤
│ Column           │ Type     │ Description               │
├──────────────────┼──────────┼───────────────────────────┤
│ requester_id (PK)│ VARCHAR  │ Requester or team identifier │
│ config_id (PK)   │ VARCHAR  │ FK → labeling_configs     │
│ scope (PK)       │ VARCHAR  │ "personal" | "team"       │
│ team_id          │ VARCHAR  │ Team identifier           │
└──────────────────┴──────────┴───────────────────────────┘

Config lookup priority (step 3-5 and subsequent tasks within the same run):
  1. requester_config_map: requester_id + scope = "personal" → personal config
  2. requester_config_map: team_id + scope = "team" → team default config
  3. labeling_configs: direct lookup of config_id = "_fallback"
  4. If none found → error (run fails)
```

### 6-2. Changes to Existing Tables

```
┌─────────────────────────────────────────────────────────┐
│ raw_files (showing changes only)                        │
├──────────────────┬──────────┬────────────────────────────┤
│ Column           │ Type     │ Description                │
├──────────────────┼──────────┼────────────────────────────┤
│ + spec_id        │ VARCHAR  │ FK → labeling_specs        │
│ + source_unit_name│ VARCHAR │ NAS incoming folder name (stored from manifest at raw_ingest time) │
└──────────────────┴──────────┴────────────────────────────┘

ingest_status value extensions:
  Existing: "pending" | "completed"
  Added:    "pending_spec" | "ready_for_labeling"
```

### 6-3. Table Relationships

- `labeling_specs.source_unit_name` ←→ `raw_files.source_unit_name` (1:N) — one spec (per folder) links all files in that folder
- `labeling_specs` ←──── `raw_files.spec_id` (1:N) — `ingest_router` sets `spec_id` after matching
- `labeling_configs` ←── `requester_config_map.config_id` (1:N)
- `requester_config_map.requester_id` ──→ `labeling_specs.requester_id` (logical reference)
- `requester_config_map.team_id` ──→ `labeling_specs.team_id` (logical reference)

> **requester_config_map registration**: Defined separately via operational procedures (manual SQL, internal management UI, scripts, etc.). This document only specifies the lookup priority.

## 7. External Integrations

- External API / Slackbot / other: receive spec.json
- Slack API: receive/respond to bot commands
- MotherDuck: cloud synchronization
- MinIO: `vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset` buckets

## 8. Operational Notes

- `config_sync` is executed via **manual materialize** from the Dagster UI.
- The `_fallback` config file is guaranteed to exist through operational procedures (no code-level guard).
- If `spec_resolve_sensor`'s `retry_count` reaches 3 or more, `spec_status` is set to `'failed'`, requiring manual intervention.
- Physical file movement (MinIO upload, NAS archive) is performed only in `raw_ingest`. All subsequent steps involve only DB state transitions.
- `source_unit_name` is the NAS incoming folder name and is stored in `raw_files` during `raw_ingest` from the value present in the manifest.
- **`ingest_router` does not read config.** Config failures surface during the `auto_labeling` run (at the start of step 3-5).
