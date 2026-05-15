# Auto Labeling: Spec vs. As-Is Logic Gaps and Required Changes

**Reference documents:**  
- `docs/logic/Auto_Labeling_기능_명세서.md`  
- `docs/logic/Auto_Labeling_요구사항_명세서.md`  
- `docs/logic/Auto_Labeling_화면_명세서.md`  

**Target code:** `src/vlm_pipeline/` (spec, label, process, yolo, definitions_staging)

---

## 1. Summary

| Category | Spec Requirement | Current Implementation | Action |
|----------|-----------------|----------------------|--------|
| Config lookup timing | `ingest_router` and `spec_resolve_sensor` do not look up config. Lookup happens **only at the start of step 3-5 (`clip_timestamp_routed`)** | `ingest_router` and `spec_resolve_sensor` call `resolve_config_for_requester` and `update_spec_resolved_config` | **Fix** |
| `spec_status = 'active'` | Updated **inside the job** when the job **succeeds** | `spec_resolve_sensor` immediately calls `update_spec_status(spec_id, "active")` | **Fix** |
| `spec_resolve_sensor` `retry_count` | Increment `retry_count += 1` on match; set `spec_status = 'failed'` if `retry_count >= 3` | `spec_resolve_sensor` does not increment `retry_count`; no `failed` transition on failure | **Fix** |
| `spec_resolve_sensor` trigger | On detecting `pending_resolved`, **directly trigger `auto_labeling_routed_job` via RunRequest** | Only updates state; triggering is handled by `ready_for_labeling_sensor` | **Review** (if two sensors are retained, clarify role division in spec) |
| captioning must not be skipped | **captioning step is never skipped** in the standard pipeline | `clip_captioning_routed` is skipped if `captioning` is not in `requested_outputs` | **Fix** |
| Step 3-5 config load | At **the start of `clip_timestamp_routed`**: resolve `config_json` via `spec_id → labeling_specs → requester_config_map/labeling_configs` | Uses `resolved_config_id` from run tag; no config DB lookup inside the asset | **Fix** |
| `labeling_spec_ingest` | Receive spec.json → parse → INSERT into `labeling_specs` | Manual materialize returns empty result only; no actual receive/INSERT | **Fix** (when external integration is established) |
| `config_sync` | Deleted file → `is_active=false`, change → `version+1` | All files upserted with `version=1` fixed; no deletion handling | **Fix** |
| `clip_captioning_routed` / `clip_to_frame_routed` / `bbox_labeling` | Process backlog based on `spec_id` and config; reflect config parameters | TODO status, no actual processing | **Fix** |

---

## 2. Data Flow & Router

### 2.1. Remove Config Lookup from ingest_router (Functional Spec 3-3)

**Spec:**  
> The router performs **only the pending vs. ready branching**. It does **not** look up `requester_config_map` / `labeling_configs`. Config is looked up **at the start of the first auto_labeling task (step 3-5)**.

**Current behavior:**  
In `defs/spec/assets.py`, `ingest_router()` calls:
- `db.resolve_config_for_requester(spec.get("requester_id"), spec.get("team_id"))`
- `db.update_spec_resolved_config(spec["spec_id"], config_id, scope or "fallback")`

This **determines and persists config during the router stage**.

**Required changes:**  
- `ingest_router` should only check whether `labeling_method` is empty or has 1+ elements, and set only `pending_spec` / `ready_for_labeling`.  
- Remove calls to `resolve_config_for_requester` and `update_spec_resolved_config`.  
- Spec match failure, missing method, bbox without classes, etc. should all go to `pending_spec` per the spec; config-missing failures should only surface in step 3-5.

---

### 2.2. Remove Config Lookup from spec_resolve_sensor and Fix retry/active Logic (Functional Spec 4)

**Spec:**  
- No config lookup (performed in step 3-5).  
- On match: `retry_count += 1`, set related `raw_files` → `ready_for_labeling`, **do not change `spec_status` (keep as `pending_resolved`)**.  
- **On job success**: update `spec_status → 'active'` **inside the job**.  
- On job failure: keep `pending_resolved`; **if `retry_count >= 3`** → transition `spec_status → 'failed'` + warning log.  
- Trigger: **RunRequest for `auto_labeling_routed_job`**.

**Current behavior:**  
In `defs/spec/sensor.py`, `spec_resolve_sensor()`:
- Calls `resolve_config_for_requester`, `update_spec_resolved_config`, then calls `update_spec_status(spec_id, "active")`.  
- Does not increment `retry_count`.  
- Does not return RunRequest (triggering is delegated to `ready_for_labeling_sensor`).

**Required changes:**  
1. Remove all config-related calls from `spec_resolve_sensor`.  
2. Increment `retry_count += 1` when processing `pending_resolved` (or once per match as per the spec).  
3. Do not change `spec_status` in the sensor; keep it as `pending_resolved`.  
4. If `retry_count >= 3`, set `spec_status = 'failed'` for that spec and do not issue RunRequest.  
5. **Trigger**: per spec, `spec_resolve_sensor` should directly return `RunRequest(job_name="auto_labeling_routed_job", ...)`.  
   - If the current structure (triggering via `ready_for_labeling_sensor`, with `spec_resolve_sensor` handling only state transitions) is retained, document the divergence from the spec and consider updating the spec to reflect a "two-stage sensor" model.  
6. Move the `spec_status = 'active'` update to **when `auto_labeling_routed_job` completes successfully** (inside the job's last step or a dedicated op).  
7. On job failure, handle `retry_count >= 3` → `spec_status = 'failed'` transition in a job failure handler or the next sensor tick.

---

## 3. Config Lookup Timing (Step 3-5)

### 3.1. Look Up Config at the Start of clip_timestamp_routed (Functional Spec 3-5)

**Spec:**  
> At **the start of this asset**, read `requester_id` and `team_id` from `labeling_specs` using the target `raw_files`' `spec_id`, then:  
> 1) `requester_config_map` (requester_id + personal), 2) team + team, 3) `config_id = "_fallback"`, 4) if none found, run fails.  
> Steps 3-6 through 3-8 within the same run use the **same `config_json`**.

**Current behavior:**  
- `clip_timestamp_routed` uses `resolved_config_id` from run tags; it does not read `labeling_specs`, `requester_config_map`, or `labeling_configs` from within the asset.  
- Config is currently resolved in `ingest_router`/`spec_resolve_sensor` and passed via tags.

**Required changes:**  
- At the **start of `clip_timestamp_routed`** (or in a common initialization step at the start of the run):  
  - Obtain `spec_id` (from tag or backlog query) → look up `requester_id` and `team_id` from `labeling_specs`.  
  - Call `resolve_config_for_requester(requester_id, team_id)` to determine `config_id` and `scope`.  
  - Load `config_json` from `labeling_configs`; fail the run if not found.  
- Pass the resolved `config_id`/`config_json` to downstream assets (`clip_captioning_routed`, `clip_to_frame_routed`, `bbox_labeling`) via run tag or run_config to guarantee "same config within the same run".  
- Remove config resolution from `ingest_router`/`spec_resolve_sensor`; consolidate to only step 3-5.

---

## 4. Standard Execution Order and captioning Must Not Be Skipped

### 4.1. timestamp → captioning → frame → bbox, captioning Always Runs (Requirement FR-002, Functional Spec 1 and 3-6)

**Spec:**  
> Standard `auto_labeling` executes sequentially in the order **timestamp → captioning → frame → bbox**, and **the captioning step is never skipped**.

**Current behavior:**  
- `clip_captioning_routed`: skips if `"captioning" not in requested`.  
- If captioning is absent from `requested_outputs`, captioning is not executed at all.

**Required changes:**  
- In "standard" execution (or when the spec has a `labeling_method`), **always run the captioning step regardless of `requested_outputs`**.  
  - Example: even if `requested_outputs` does not include captioning, in a standard pipeline run `clip_captioning_routed` and handle "fill caption only, skip bbox" etc. internally.  
- Alternatively, clearly define the condition: if captioning is included in `labeling_method`, always run the captioning step regardless of `requested_outputs`.

---

## 5. Asset-level Implementation Status

### 5.1. labeling_spec_ingest (Functional Spec 3-1)

**Spec:**  
- External API / Slackbot / etc. → receive spec.json → parse (spec_id, requester_id, team_id, source_unit_name, categories, classes, labeling_method) → INSERT into `labeling_specs`.  
- If `labeling_method` is []/null → `spec_status = "pending"`; if 1 or more elements → `"active"`.

**Current behavior:**  
- On manual materialize, logs only and returns `{"specs_upserted": 0}`. No actual spec reception, parsing, or INSERT.

**Required changes:**  
- Once the ingestion path (Flex/Slack/API, etc.) for `spec.json` is defined, receive the payload, parse it, and call `db.upsert_labeling_spec(...)`.  
- Apply the categories → classes auto-derivation table (from the functional spec and screen spec).  
- Set `spec_status` to `pending`/`active` based on whether `labeling_method` is present.

---

### 5.2. config_sync (Functional Spec 3-2)

**Spec:**  
- New file → INSERT, **content changed → UPDATE (version + 1)**, **file deleted → is_active = false** (do not delete the DB row).

**Current behavior:**  
- `config_sync` calls `upsert_labeling_config(..., version=1, is_active=True)` for all discovered JSON files.  
- No version increment; no `is_active=false` handling for deleted files.

**Required changes:**  
- Compare the list of `config_id`s currently in the DB against the current directory file list.  
  - If absent from the directory, UPDATE that `config_id` to `is_active=false`.  
- If a file's content differs from the existing DB record, UPDATE with `version + 1`.  
- New files: INSERT (version=1, is_active=true).

---

### 5.3. clip_captioning_routed (Functional Spec 3-6)

**Spec:**  
- Uses `labels` (label_status=completed, caption_text IS NULL), `raw_files`, and **the captioning section of `config_json` resolved by the same rules as step 3-5**.  
- Load parameters (model, language, max_caption_length, etc.), generate captions, UPDATE `labels.caption_text`, update MinIO.

**Current behavior:**  
- Backlog query by `spec_id` (timestamp_status=completed) and reuse of existing `clip_captioning` logic is TODO.  
- Returns 0 records without actual processing.

**Required changes:**  
- Implement backlog query using `spec_id` (from tag or passed from upstream) + completed timestamp.  
- Use the captioning section of the `config_json` confirmed in step 3-5 to load parameters.  
- Reuse the existing caption generation, DB, and MinIO update logic, now driven by spec/config.

---

### 5.4. clip_to_frame_routed (Functional Spec 3-7)

**Spec:**  
- Event intervals with completed captions, uses the `frame_extraction` section of the same `config_json` as step 3-5.  
- Applies `frame_interval_sec`, `max_frames_per_clip`, `output_format`, `output_quality`, etc.

**Current behavior:**  
- Backlog query by `spec_id` (timestamp_status=completed), clip creation + frame extraction, and `frame_status` update are TODO.  
- Skips if `bbox not in requested`; no actual processing.

**Required changes:**  
- Backlog query using `spec_id` + completed caption.  
- Load `config_json.frame_extraction` parameters.  
- Implement clip creation, frame extraction, `processed_clips`/`image_metadata` INSERT, and MinIO upload (reuse/adapt existing `clip_to_frame` logic).

---

### 5.5. bbox_labeling (Functional Spec 3-8)

**Spec:**  
- Uses `image_metadata` (frame extraction complete), `labeling_specs.classes`, and the `bbox` section of the same `config_json` as step 3-5.  
- `target_classes = intersection(spec.classes, config.target_classes)` (if defined).  
- Applies `detection_model`, `confidence_threshold`, `nms_threshold`, etc.

**Current behavior:**  
- Backlog query by `spec_id` + `frame_status=completed`, and `target_classes = intersection(spec.classes, config.bbox.target_classes)` are TODO.  
- No actual processing.

**Required changes:**  
- Backlog query using `spec_id` + completed frame extraction images.  
- Determine `target_classes` as the intersection of `spec.classes` and `config.bbox.target_classes`.  
- Run object detection (YOLO etc.) with `config.bbox` parameters, then INSERT/UPDATE `labels` and upload to MinIO.

---

## 6. DB & Schema

### 6.1. requester_config_map PK (Functional Spec 6-1)

**Spec:**  
- (`requester_id`, `config_id`, `scope`) is the PK.

**Current behavior:**  
- `schema.sql` uses `map_id VARCHAR PRIMARY KEY`, which differs from the spec's PK structure.

**Required changes:**  
- Either adopt (`requester_id`, `config_id`, `scope`) as a composite PK per the spec, or retain the existing `map_id` and reflect "implementation uses `map_id` PK" in the spec.  
- Lookup logic already filters by `requester_id`/`team_id`, so a PK change only requires a data migration.

---

### 6.2. raw_files Extension

**Spec:**  
- Add `spec_id` and `source_unit_name` to `raw_files`.  
- Add `pending_spec` and `ready_for_labeling` to `ingest_status` values.

**Current behavior:**  
- `duckdb_base` adds `spec_id` and `source_unit_name` via ALTER.  
- Code uses `pending_spec` and `ready_for_labeling` for `ingest_status` values.  
- `schema.sql` documents the strict 8-table definition with a spec-specific comment note.

**Required changes:**  
- Maintain as-is. If needed, add a comment in `schema.sql` listing the extended columns and `ingest_status` values for `raw_files`.

---

## 7. Slack Bot & Interface

**Spec (Screen Spec 3-4 through 3-6):**  
- Defines the behavior and I/O format for `/labeling-confirm`, `/labeling-pending`, and `/labeling-status`.

**Current behavior:**  
- No Slack bot implementation in the pipeline code.  
- `spec_resolve_sensor` assumes "Slack has already UPDATE'd `labeling_specs` to `pending_resolved`".

**Required changes:**  
- When implementing the Slack bot as a separate service/script, follow the I/O format from the screen spec.  
- `/labeling-confirm` only needs to UPDATE `labeling_specs` (`labeling_method`, `categories`, `classes`, `spec_status = 'pending_resolved'`); it does not look up config (consistent with the spec).

---

## 8. Notifications & Alerts (Screen Spec 5)

**Spec:**  
- On `auto_labeling` failure and `spec_status → failed` (retry exceeded 3 times), **Slack #data-labeling notification is required**.  
- Completion / new pending events are "nice to have".

**Current behavior:**  
- No Slack notification code for these events.

**Required changes:**  
- At the point where `spec_status` is set to `failed` (job failure or sensor failure handling), add a Slack webhook or bot API call.  
- Match notification message format to screen spec section 5.

---

## 9. Change Checklist (by Priority)

| # | Item | File (example) | Priority |
|---|------|----------------|----------|
| 1 | Remove config lookup and `update_spec_resolved_config` from `ingest_router` | `defs/spec/assets.py` | High |
| 2 | Remove config lookup from `spec_resolve_sensor`; keep `spec_status` as `pending_resolved`; increment `retry_count`; transition to `failed` on `retry_count >= 3` | `defs/spec/sensor.py` | High |
| 3 | Update `spec_status='active'` only inside the job on job success | `defs/spec/sensor.py`, job completion handler location | High |
| 4 | Config lookup at the start of `clip_timestamp_routed` (step 3-5) and propagation of config within the same run | `defs/label/assets.py`, resource (`duckdb_spec`) | High |
| 5 | Enforce captioning step in standard pipeline | `defs/process/assets.py` (`clip_captioning_routed`), `definitions_staging` | Medium |
| 6 | `config_sync`: increment version, set `is_active=false` on deletion | `defs/spec/assets.py`, `resources/duckdb_spec.py` | Medium |
| 7 | `clip_captioning_routed`: implement backlog processing and caption generation based on `spec_id` and config | `defs/process/assets.py` | Medium |
| 8 | `clip_to_frame_routed`: implement backlog based on `spec_id` and config; apply `frame_extraction` parameters | `defs/process/assets.py` | Medium |
| 9 | `bbox_labeling`: backlog based on `spec_id` and config; classes intersection; apply bbox parameters | `defs/yolo/assets.py` | Medium |
| 10 | `labeling_spec_ingest`: parse and INSERT external spec on receipt (after integration path is defined) | `defs/spec/assets.py` | Low |
| 11 | Determine whether `spec_resolve_sensor` should return RunRequest, or document role separation with `ready_for_labeling_sensor` | `defs/spec/sensor.py`, docs | Low |
| 12 | Slack notification on `auto_labeling` failure and spec `failed` | Separate notification module or sensor/job hook | Low |

---

## 10. Reference: Current Job & Asset Dependency Graph

- **auto_labeling_routed_job**  
  - `clip_timestamp_routed` → `clip_captioning_routed` → `clip_to_frame_routed` → `bbox_labeling`
- **Dependencies:**  
  - `clip_captioning_routed` deps = [`clip_timestamp_routed`]  
  - `clip_to_frame_routed` deps = [`clip_timestamp_routed`] (per spec, frame should follow captioning completion; consider adding `clip_captioning_routed` to deps)  
  - `bbox_labeling` deps = [`clip_to_frame_routed`]

Per the spec, the order is timestamp → captioning → frame → bbox, so `clip_to_frame_routed` should have `clip_captioning_routed` in its deps.  
If `deps=["clip_timestamp_routed"]` is the only dependency, frame extraction can run before captioning completes — a dependency fix is recommended.

---

*Document created: 2026-03-19. It is recommended to update this document alongside any changes to the spec or code.*
