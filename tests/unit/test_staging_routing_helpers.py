from __future__ import annotations

from vlm_pipeline.lib.env_utils import (
    YOLO_OUTPUTS,
    dispatch_folder_for_source_unit,
    dispatch_raw_key_prefix_folder,
    is_dispatch_yolo_only_requested,
    parse_outputs_raw,
)


def test_dispatch_yolo_only_requested_normalizes_image_classification_alias() -> None:
    tags = {"requested_outputs": "bbox,image classification"}

    assert parse_outputs_raw(tags["requested_outputs"]) == ["bbox", "classification_image"]


def test_dispatch_yolo_only_run_detects_non_spec_bbox_request() -> None:
    tags = {"requested_outputs": "bbox"}

    assert is_dispatch_yolo_only_requested(tags) is True


def test_dispatch_yolo_only_run_is_false_for_mixed_captioning_and_bbox() -> None:
    tags = {"requested_outputs": "timestamp_video,captioning_video,bbox"}

    assert is_dispatch_yolo_only_requested(tags) is False


def test_dispatch_yolo_only_run_is_disabled_for_spec_runs() -> None:
    tags = {"spec_id": "spec-1", "requested_outputs": "bbox"}

    assert is_dispatch_yolo_only_requested(tags) is False


def test_parse_outputs_raw_normalizes_yolo_aliases() -> None:
    assert parse_outputs_raw("image classification,bbox") == ["classification_image", "bbox"]
    assert "classification_image" in YOLO_OUTPUTS


def test_dispatch_folder_for_source_unit_prefers_original() -> None:
    tags = {"folder_name": "romanized_name", "folder_name_original": "원본폴더"}
    assert dispatch_folder_for_source_unit(tags) == "원본폴더"


def test_dispatch_raw_key_prefix_folder_prefers_sanitized_tag() -> None:
    tags = {"folder_name": "tmp_data", "folder_name_original": "TMP_Data"}
    assert dispatch_raw_key_prefix_folder(tags) == "tmp_data"


def test_dispatch_raw_key_prefix_folder_falls_back_to_sanitize_original() -> None:
    tags = {"folder_name_original": "My Folder"}
    assert dispatch_raw_key_prefix_folder(tags) == "my_folder"
