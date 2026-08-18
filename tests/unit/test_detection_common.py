"""detection_common + target_classes 단위 테스트."""

from __future__ import annotations

from vlm_pipeline.defs.spec.target_classes import TargetClassResolution, resolve_target_classes
from vlm_pipeline.lib.detection_common import (
    flush_image_labels,
    normalize_classes,
    parse_tag_list,
    stable_image_label_id,
)


# ---------------------------------------------------------------------------
# normalize_classes
# ---------------------------------------------------------------------------


class TestNormalizeClasses:
    def test_basic(self):
        assert normalize_classes(["Person", " fire ", "SMOKE"]) == ["person", "fire", "smoke"]

    def test_dedup_preserves_order(self):
        assert normalize_classes(["car", "truck", "Car", "car"]) == ["car", "truck"]

    def test_empty_and_none(self):
        assert normalize_classes(None) == []
        assert normalize_classes([]) == []
        assert normalize_classes([""]) == []
        assert normalize_classes(["", None, "  "]) == []

    def test_mixed_types(self):
        assert normalize_classes([42, "fire", True]) == ["42", "fire", "true"]


# ---------------------------------------------------------------------------
# parse_tag_list
# ---------------------------------------------------------------------------


class TestParseTagList:
    def test_csv_string(self):
        assert parse_tag_list("person,car,truck") == ["person", "car", "truck"]

    def test_json_array_string(self):
        assert parse_tag_list('["Person", "Fire"]') == ["person", "fire"]

    def test_list_input(self):
        assert parse_tag_list(["Person", "Fire"]) == ["person", "fire"]

    def test_empty(self):
        assert parse_tag_list("") == []
        assert parse_tag_list(None) == []

    def test_invalid_json_falls_back_to_csv(self):
        assert parse_tag_list("[not json") == ["[not json"]


# ---------------------------------------------------------------------------
# stable_image_label_id
# ---------------------------------------------------------------------------


class TestStableImageLabelId:
    def test_deterministic(self):
        id1 = stable_image_label_id("img-1", "labels/key.json")
        id2 = stable_image_label_id("img-1", "labels/key.json")
        assert id1 == id2

    def test_different_inputs(self):
        id1 = stable_image_label_id("img-1", "key-a.json")
        id2 = stable_image_label_id("img-1", "key-b.json")
        assert id1 != id2

    def test_returns_hex_string(self):
        result = stable_image_label_id("img-1", "key.json")
        assert isinstance(result, str)
        assert len(result) == 40
        int(result, 16)


# ---------------------------------------------------------------------------
# flush_image_labels
# ---------------------------------------------------------------------------


class TestFlushImageLabels:
    def test_success(self):
        class FakeDB:
            def batch_insert_image_labels(self, rows):
                return len(rows)

        class FakeContext:
            class log:
                messages = []

                @classmethod
                def debug(cls, msg):
                    cls.messages.append(("debug", msg))

                @classmethod
                def error(cls, msg):
                    cls.messages.append(("error", msg))

        rows = [{"image_label_id": "id1"}, {"image_label_id": "id2"}]
        flush_image_labels(FakeDB(), rows, FakeContext, tool_name="TEST")
        assert any("TEST" in msg for _, msg in FakeContext.log.messages)
        assert all(level != "error" for level, _ in FakeContext.log.messages)

    def test_error_is_logged(self):
        class FakeDB:
            def batch_insert_image_labels(self, rows):
                raise RuntimeError("db error")

        class FakeContext:
            class log:
                errors = []

                @classmethod
                def debug(cls, msg):
                    pass

                @classmethod
                def error(cls, msg):
                    cls.errors.append(msg)

        flush_image_labels(FakeDB(), [{"id": "1"}], FakeContext, tool_name="SAM3")
        assert len(FakeContext.log.errors) == 1
        assert "SAM3" in FakeContext.log.errors[0]


# ---------------------------------------------------------------------------
# resolve_target_classes
# ---------------------------------------------------------------------------


class TestResolveTargetClasses:
    def test_no_tags(self):
        result = resolve_target_classes(None, None)
        assert result.classes == []
        assert result.class_source == "server_default"

    def test_empty_tags(self):
        result = resolve_target_classes({}, None)
        assert result.classes == []
        assert result.class_source == "server_default"

    def test_classes_from_tags(self):
        tags = {"classes": "Person,Fire"}
        result = resolve_target_classes(tags, None)
        assert result.classes == ["person", "fire"]
        assert result.class_source == "dispatch_tags"

    def test_result_is_target_class_resolution(self):
        result = resolve_target_classes(None, None)
        assert isinstance(result, TargetClassResolution)
