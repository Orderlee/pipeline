# Auto Labeling Requirements Specification

## 1. Purpose

- Minimize manual work after data ingestion and accelerate training data production by performing a first-pass automated labeling using AI models.
    - Manual tasks targeted for automation:
        - Identifying and extracting event intervals (Timestamp Labeling)
        - Inserting captions for event intervals (Captioning)
        - Extracting images from the relevant video (Clip → Frame)
        - Labeling objects in extracted images (Bbox Labeling)

## 2. Scope

- [In scope]: timestamp identification, captioning, frame extraction, bbox generation, config-based parameter management
- [Out of scope]: model training and deployment, integration with manual labeling tools (CVAT/Label Studio, etc.)

## 3. Users

- AI Engineer: config requests, result review
- Data Engineer: pipeline operation, config management
- PM: pending status, ingestion spec status, operational and monitoring management (API cost/time, etc.)

## 4. Functional Requirements

- **`[Spec]*`**
    - FR-001: When data is ingested, `spec.json` must be received alongside it and stored in the `labeling_specs` table.
    - FR-002: Auto labeling must be performed reflecting spec metadata including `labeling_method`. **Standard auto labeling executes sequentially in the order timestamp → captioning → frame → bbox, and the captioning step is never skipped.**
    - FR-003: If `labeling_method` is not yet finalized, the item must be classified as pending.
    - FR-004: When the labeling definition for a pending spec is confirmed, `auto_labeling` must be triggered automatically.
- **`[Pending Resolution]*`**
    - FR-005: It must be possible to confirm the labeling definition for a pending spec via the Slack bot.
    - FR-006: It must be possible to retrieve the current list of pending specs via the Slack bot.
    - FR-007: It must be possible to query the processing status of a specific spec via the Slack bot.
- **`[Config]*`**
    - FR-008: Per-requester configs must be registrable and retrievable.
    - FR-009: Adding a JSON file to `config/parameters/` must synchronize it to the DB (manually triggered).
    - FR-010: Processing must be possible using `_fallback` (default values) even without a registered config.
- **`[Auto Labeling Tasks]*`**
    - FR-011: Timestamp identification — the start/end timestamp of event intervals in a video must be automatically identified using an AI model.
    - FR-012: Captioning — captions must be generated for identified event intervals using an AI model. (This step precedes frame extraction.)
    - FR-013: Frame extraction — event intervals must be cut into clips and frame images extracted at intervals defined in the config.
    - FR-014: Bbox labeling — objects must be detected in extracted frame images and bounding boxes generated using an AI model.
- **`[Result Storage]*`**
    - FR-015: Auto labeling results (labels, processed_clips, frames, bbox) must be stored in DuckDB + MinIO.
    - FR-016: Processing status (success/failure/pending counts) must be queryable from the MotherDuck dashboard.
- **`[Reprocessing]*`**
    - FR-017: It must be possible to reprocess the same data with a different config.
    - FR-018: On reprocessing, existing results must be preserved and the new results saved as a new version.
