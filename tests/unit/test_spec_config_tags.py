"""Tests for vlm_pipeline.lib.spec_config — pure tag-parsing functions."""

from __future__ import annotations

from vlm_pipeline.lib.spec_config import (
    is_standard_spec_run,
    is_unscoped_mvp_autolabel_run,
    parse_requested_outputs,
)


class TestIsUnscopedMvpAutolabelRun:
    def test_none_tags(self):
        assert is_unscoped_mvp_autolabel_run(None) is True

    def test_empty_tags(self):
        assert is_unscoped_mvp_autolabel_run({}) is True

    def test_with_spec_id(self):
        assert is_unscoped_mvp_autolabel_run({"spec_id": "abc"}) is False

    def test_with_dispatch_archive_only(self):
        assert is_unscoped_mvp_autolabel_run({"dispatch_archive_only": "true"}) is False

    def test_with_folder_name(self):
        assert is_unscoped_mvp_autolabel_run({"folder_name": "test_folder"}) is False

    def test_with_run_mode(self):
        assert is_unscoped_mvp_autolabel_run({"run_mode": "full"}) is False

    def test_with_requested_outputs(self):
        assert is_unscoped_mvp_autolabel_run({"requested_outputs": "captioning,bbox"}) is False

    def test_irrelevant_tags_still_mvp(self):
        assert is_unscoped_mvp_autolabel_run({"some_other_tag": "value"}) is True


class TestParseRequestedOutputs:
    def test_none_tags(self):
        assert parse_requested_outputs(None) == []

    def test_empty_tags(self):
        assert parse_requested_outputs({}) == []

    def test_requested_outputs_key(self):
        result = parse_requested_outputs({"requested_outputs": "captioning,bbox"})
        assert "captioning_video" in result
        assert "bbox" in result

    def test_outputs_key_fallback(self):
        result = parse_requested_outputs({"outputs": "captioning"})
        assert "captioning_video" in result

    def test_labeling_method_fallback(self):
        result = parse_requested_outputs({"labeling_method": "bbox"})
        assert "bbox" in result


class TestIsStandardSpecRun:
    def test_none_tags(self):
        assert is_standard_spec_run(None) is False

    def test_empty_tags(self):
        assert is_standard_spec_run({}) is False

    def test_with_spec_id(self):
        assert is_standard_spec_run({"spec_id": "spec-123"}) is True

    def test_with_empty_spec_id(self):
        assert is_standard_spec_run({"spec_id": ""}) is False

    def test_with_whitespace_spec_id(self):
        assert is_standard_spec_run({"spec_id": "  "}) is False
