# Dispatch labeling_method System and Skip Import Design Proposal

## Purpose

- Consolidate the `labeling_method` values used in dispatch trigger JSON and external communication into a new canonical system.
- Document the basis for redefining `skip` — not as a simple labeling omission, but as "import of an already-existing JSON artifact".
- This document is a design proposal for future implementation and does not include any code changes, schema changes, or test changes at this stage.

## Current Structure Summary

- Dispatch and downstream assets currently operate primarily on the following legacy values:
  - `timestamp`
  - `captioning`
  - `bbox`
  - `image_classification`
  - `video_classification`
- The current dispatch normalization path accepts `labeling_method`, `outputs`, and `run_mode`, and converts them internally to an `outputs` string and run tags.
- Currently, `skip` is treated similarly to `archive_only` in practice — close in meaning to "perform ingest only, without model inference".
- Separate paths already exist for importing pre-existing JSON artifacts:
  - `manual_label_import`
  - `prelabeled_import`
  - `archive_only_artifact_import`
- Applying the new contract therefore requires consolidating the dispatch parser, output helper, ingest import path, and Gemini/YOLO asset gating together.

## Problem Definition

- The external contract's `labeling_method` and the current internal execution vocabulary differ.
- The current `skip` cannot express "JSON import for data whose labeling is already complete".
- `captioning_image`, `classification_video`, and `classification_image` have defined external semantics, but the equivalent concepts do not exist directly in the internal asset graph.
- Both production and staging use dispatch, so the external contract must be shared, with only internal profile differences maintained.

## New labeling_method Contract

- The canonical values for external input are fixed to the following 7:
  - `timestamp_video`
  - `captioning_video`
  - `captioning_image`
  - `classification_video`
  - `classification_image`
  - `bbox`
  - `skip`
- These values serve as the source of truth for trigger JSON and external communication.
- Backward-compatible inputs are converted to new canonical values only at the ingress stage:
  - `timestamp` -> `timestamp_video`
  - `captioning` -> `captioning_video`
  - `image_classification` -> `classification_image`
  - `video_classification` -> `classification_video`
- `skip` is not mixed with other methods.
- Ultimately, the `outputs`, `labeling_method` in the DB, and `requested_outputs` in run tags are all unified to the new canonical strings.

## Model Routing Criteria

- Uses Vertex AI / Gemini:
  - `timestamp_video`
  - `captioning_video`
  - `captioning_image`
  - `classification_video`
- Uses YOLO-World:
  - `bbox`
  - `classification_image`
- `captioning_image` is defined as image captioning of a representative frame extracted from a video event — not raw image captioning.
- `classification_video` requires a new Gemini-based classification path that produces video-unit classification results.
- `classification_image` is defined as a path that generates image-level multi-class summarization based on YOLO detection results.

## Internal Execution Mapping

- `captioning_video` automatically includes `timestamp_video`.
- `captioning_image` automatically includes `timestamp_video + captioning_video`.
- `bbox` and `classification_image` reuse the same YOLO inference results where possible.
- `classification_video` requires a new Gemini classification asset or an equivalent dedicated processing path.
- `classification_image` produces two outputs based on YOLO detection results:
  - Image-level multi-class summarization
  - Bbox evidence artifact
- The existing asset graph is reused as much as possible; only the execution gate logic and run tag interpretation are updated to use the new canonical method.

## skip Processing Design

- `skip` is no longer simply `archive_only`.
- Its meaning is fixed to: "do not run labeling models; instead read already-existing JSON artifacts inside the incoming folder and load them into DB + MinIO".
- `skip` does not execute Gemini or YOLO inference.
- Raw ingest, archive, and local artifact import continue to be performed.
- Implementation favors reusing or extending the following existing import paths rather than building an entirely new path:
  - `manual_label_import`
  - `prelabeled_import`
  - `archive_only_artifact_import`
- `skip` import is designed to support both folder-convention-based and payload-shape-based approaches.

## Storage Format and Load Targets

- Event/timestamp/caption video JSON
  - `vlm-labels` + `labels`
- Bbox JSON (SAM3 primary)
  - `vlm-labels` + `image_labels`
- Image caption JSON
  - `vlm-labels` + `image_metadata` caption series
- Video classification
  - `vlm-classification/<folder_prefix>/video/<class>/<file>` — source copy only. No JSON/DB load.
- Image classification (SAM3)
  - `vlm-classification/<folder_prefix>/image/<class>/<file>` — source/frame copy only. No JSON/DB load.
- `classification_image` differs from bbox in that it preserves image-level multi-class results in a class folder structure (if a single image spans multiple classes, it is copied to each folder).

## Production / Staging Rollout Scope

- The external contract is used identically across production and staging.
- Per-profile differences are maintained only in asset registration and catalog composition differences.
- Key points to address together during actual implementation:
  - dispatch parser
  - env/output helper
  - ingest import path
  - Gemini/YOLO asset gating
- Even if the internal asset combinations differ between production and staging, the `labeling_method` interpretation rules seen from the outside must be identical.

## Test Scenarios

- Verify all 7 new canonical `labeling_method` values are parsed correctly
- Verify legacy aliases are converted to new canonical values at ingress
- Verify `captioning_video` requests automatically include `timestamp_video`
- Verify `captioning_image` requests automatically include `timestamp_video + captioning_video`
- Verify `bbox` and `classification_image` are both routed to YOLO-based paths
- Verify `classification_video` is routed to the Gemini classification path
- Verify `skip` is treated as artifact import rather than archive-only
- Verify that event/bbox/image_caption/video_classification/image_classification JSON within `skip` are loaded to their respective correct storage targets
- Verify both production and staging interpret the same external contract

## Implementation Order

1. Normalize dispatch input and consolidate canonical `labeling_method` conversion logic
2. Unify run tags / DB stored values to the new canonical vocabulary
3. Reflect `timestamp_video`, `captioning_video`, `captioning_image`, `classification_video` gating in the Gemini path
4. Reflect `bbox`, `classification_image` gating in the YOLO path and save image-level classification summary
5. Extend `skip` by reusing existing import paths
6. Update production/staging definitions and tests to use the new contract

## Confirmed Base Assumptions

- This document is the reference for future implementation; it does not immediately change current code behavior.
- `captioning_image` means event-based frame captioning.
- `classification_video` is implemented using Gemini.
- `classification_image` is implemented as image-level multi-class summarization based on YOLO-World detection.
- `skip` means importing existing JSON artifacts from inside the incoming folder; it is not used with the simple archive-only meaning.
- README and other documents are not modified in this phase.
