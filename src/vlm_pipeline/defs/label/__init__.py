"""LABEL domain — Gemini event labeling, video classification, artifact import.

Modules:
    assets                  @asset entry points (clip_timestamp, classification_video)
    label_helpers           Shared private helpers (Gemini analyzer, video prep, classification)
    timestamp               clip_timestamp MVP and routed implementations
    import_support          Event label file parsing and import
    artifact_import_support Main artifact import orchestrator
    artifact_bbox           Bbox/detection JSON import
    artifact_caption        Image caption JSON import
    artifact_classification Image classification JSON import
    manual_import           Manual label import asset
    sensor                  Label-stage sensors
"""
