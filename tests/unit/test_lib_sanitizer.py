"""Tests for vlm_pipeline.lib.sanitizer — filename/path sanitization."""

from __future__ import annotations

from vlm_pipeline.lib.sanitizer import sanitize_filename, sanitize_path_component


class TestSanitizePathComponent:
    def test_ascii_passthrough(self):
        assert sanitize_path_component("hello_world") == "hello_world"

    def test_spaces_to_underscores(self):
        assert sanitize_path_component("hello world") == "hello_world"

    def test_special_chars_removed(self):
        result = sanitize_path_component("test@#$%file")
        assert "@" not in result
        assert "#" not in result

    def test_uppercase_to_lower(self):
        assert sanitize_path_component("HelloWorld") == "helloworld"

    def test_empty_returns_unnamed(self):
        assert sanitize_path_component("") == "unnamed"

    def test_multiple_underscores_collapsed(self):
        result = sanitize_path_component("a___b")
        assert "__" not in result

    def test_korean_romanized(self):
        result = sanitize_path_component("테스트")
        assert result.isascii()
        assert len(result) > 0


class TestSanitizeFilename:
    def test_preserves_extension(self):
        result = sanitize_filename("test.mp4")
        assert result.endswith(".mp4")

    def test_normalizes_jpeg_extension(self):
        result = sanitize_filename("photo.jpeg")
        assert result.endswith(".jpg")

    def test_normalizes_tiff_extension(self):
        result = sanitize_filename("image.tiff")
        assert result.endswith(".tif")

    def test_lowercase_extension(self):
        result = sanitize_filename("video.MP4")
        assert result.endswith(".mp4")

    def test_sanitizes_stem(self):
        result = sanitize_filename("Hello World (1).mp4")
        assert " " not in result
        assert result.endswith(".mp4")
