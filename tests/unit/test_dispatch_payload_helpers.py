from __future__ import annotations

import pytest

from vlm_pipeline.lib.dispatch_payload import format_dispatch_storage_list, parse_dispatch_request_payload
from vlm_pipeline.lib.env_utils import CATEGORY_TO_CLASSES


def test_parse_dispatch_request_uses_labeling_method_as_primary_outputs() -> None:
    parsed = parse_dispatch_request_payload(
        {
            "labeling_method": ["captioning", "bbox"],
            "categories": ["smoke", "falldown"],
            "classes": ["smoke", "person_fallen"],
        }
    )

    assert parsed["labeling_method"] == ["timestamp_video", "captioning_video", "bbox"]
    assert parsed["outputs_str"] == "timestamp_video,captioning_video,bbox"
    assert parsed["categories"] == ["smoke", "falldown"]
    assert parsed["classes"] == ["smoke", "person_fallen"]


def test_parse_dispatch_request_derives_classes_from_categories_when_missing() -> None:
    parsed = parse_dispatch_request_payload(
        {
            "labeling_method": ["bbox"],
            "categories": ["smoke", "falldown"],
            "classes": [],
        }
    )

    expected_classes: list[str] = []
    for category in ("smoke", "falldown"):
        for phrase in CATEGORY_TO_CLASSES[category]:
            if phrase not in expected_classes:
                expected_classes.append(phrase)

    assert parsed["labeling_method"] == ["bbox"]
    assert parsed["classes"] == expected_classes


def test_parse_dispatch_request_normalizes_image_classification_alias() -> None:
    parsed = parse_dispatch_request_payload(
        {
            "labeling_method": ["image classification"],
        }
    )

    assert parsed["labeling_method"] == ["classification_image"]
    assert parsed["outputs_str"] == "classification_image"


def test_parse_dispatch_request_treats_not_required_marker_as_archive_only() -> None:
    parsed = parse_dispatch_request_payload(
        {
            "outputs": ["필요없음"],
            "categories": ["smoke"],
        }
    )

    assert parsed["archive_only"] is True
    assert parsed["labeling_method"] == ["skip"]
    assert parsed["outputs_str"] == "skip"


def test_parse_dispatch_request_treats_skip_marker_as_archive_only_case_insensitive() -> None:
    parsed = parse_dispatch_request_payload(
        {
            "labeling_method": ["SKIP"],
            "categories": ["smoke"],
            "classes": ["smoke"],
        }
    )

    assert parsed["archive_only"] is True
    assert parsed["labeling_method"] == ["skip"]
    assert parsed["outputs_str"] == "skip"
    assert parsed["classes"] == ["smoke"]


def test_parse_dispatch_request_treats_ff_as_archive_only() -> None:
    parsed = parse_dispatch_request_payload(
        {
            "labeling_method": ["ff"],
        }
    )

    assert parsed["archive_only"] is True
    assert parsed["labeling_method"] == ["skip"]
    assert parsed["outputs_str"] == "skip"


def test_parse_dispatch_request_treats_ff_uppercase_as_archive_only() -> None:
    parsed = parse_dispatch_request_payload(
        {
            "labeling_method": ["FF"],
        }
    )

    assert parsed["archive_only"] is True
    assert parsed["labeling_method"] == ["skip"]
    assert parsed["outputs_str"] == "skip"


def test_parse_dispatch_request_falls_back_to_legacy_outputs() -> None:
    parsed = parse_dispatch_request_payload(
        {
            "outputs": ["timestamp", "captioning"],
        }
    )

    assert parsed["labeling_method"] == ["timestamp_video", "captioning_video"]
    assert parsed["outputs_str"] == "timestamp_video,captioning_video"


def test_parse_dispatch_request_rejects_missing_routing_fields() -> None:
    with pytest.raises(ValueError, match="missing_labeling_method_or_outputs_or_run_mode"):
        parse_dispatch_request_payload({"categories": ["smoke"]})


def test_parse_dispatch_request_ignores_prompts_field_when_present() -> None:
    parsed = parse_dispatch_request_payload(
        {
            "labeling_method": ["timestamp"],
            "prompts": ["  Detect smoke  ", "Detect smoke", "", None, "Detect violence"],
        }
    )

    assert parsed["labeling_method"] == ["timestamp_video"]
    assert "prompts" not in parsed


def test_parse_dispatch_request_allows_timestamp_without_prompts() -> None:
    parsed = parse_dispatch_request_payload(
        {
            "labeling_method": ["timestamp"],
        }
    )

    assert parsed["labeling_method"] == ["timestamp_video"]
    assert parsed["outputs_str"] == "timestamp_video"


def test_parse_dispatch_request_ignores_prompts_for_bbox_only() -> None:
    parsed = parse_dispatch_request_payload(
        {
            "labeling_method": ["bbox"],
            "prompts": ["Detect smoke"],
        }
    )

    assert parsed["labeling_method"] == ["bbox"]
    assert "prompts" not in parsed


def test_parse_dispatch_request_rejects_mixed_valid_and_invalid_labeling_method() -> None:
    with pytest.raises(ValueError, match="invalid_labeling_method"):
        parse_dispatch_request_payload(
            {
                "labeling_method": ["bbox", "not_real_method"],
            }
        )


def test_parse_dispatch_request_rejects_classification_video_without_classes_or_categories() -> None:
    with pytest.raises(ValueError, match="classification_video_requires_categories_or_classes"):
        parse_dispatch_request_payload(
            {
                "labeling_method": ["classification_video"],
            }
        )


# ---------------------------------------------------------------------------
# output_media 계약 (2026-09-21 유령 video LS 프로젝트 회귀)
#
# comfy_local batch b4fe9a6f-339(이미지 1장) 을 [captioning_image, bbox] 로 promote 했는데
# `_OUTPUT_DEPENDENCIES["captioning_image"] = [timestamp_video, captioning_video]` 가
# 의존성으로 두 video 메서드를 붙여, 이미지뿐인 배치에 빈 video 프로젝트가 생겼다.
# 그 의존성은 video 원본 전제이므로 payload 가 image 를 선언하면 적용하지 않는다.
# ---------------------------------------------------------------------------


def test_parse_dispatch_request_does_not_expand_video_deps_for_image_media() -> None:
    parsed = parse_dispatch_request_payload(
        {
            "labeling_method": ["captioning_image", "bbox"],
            "output_media": "image",
            "categories": ["smoke"],
            "classes": ["smoke"],
        }
    )

    assert parsed["labeling_method"] == ["bbox", "captioning_image"]
    assert parsed["outputs_str"] == "bbox,captioning_image"


def test_parse_dispatch_request_keeps_video_deps_when_media_absent_or_video() -> None:
    expanded = ["timestamp_video", "captioning_video", "bbox", "captioning_image"]

    without_media = parse_dispatch_request_payload(
        {"labeling_method": ["captioning_image", "bbox"], "categories": ["smoke"]}
    )
    video_media = parse_dispatch_request_payload(
        {"labeling_method": ["captioning_image", "bbox"], "output_media": "video", "categories": ["smoke"]}
    )

    assert without_media["labeling_method"] == expanded
    assert video_media["labeling_method"] == expanded


def test_parse_dispatch_request_rejects_explicit_video_method_on_image_media() -> None:
    with pytest.raises(ValueError, match="video_labeling_method_on_image_media"):
        parse_dispatch_request_payload(
            {
                "labeling_method": ["timestamp_video", "bbox"],
                "output_media": "image",
            }
        )


def test_parse_dispatch_request_rejects_video_run_mode_on_image_media() -> None:
    with pytest.raises(ValueError, match="no_labeling_method_for_image_media"):
        parse_dispatch_request_payload({"run_mode": "gemini", "output_media": "image"})


def test_parse_dispatch_request_allows_skip_marker_on_image_media() -> None:
    parsed = parse_dispatch_request_payload(
        {"labeling_method": ["skip"], "output_media": "image", "categories": ["smoke"]}
    )

    assert parsed["archive_only"] is True
    assert parsed["labeling_method"] == ["skip"]


def test_format_dispatch_storage_list_uses_readable_comma_string() -> None:
    assert format_dispatch_storage_list(["person_fallen", "smoke", "smoke", "  gun  "]) == ("person_fallen, smoke, gun")
