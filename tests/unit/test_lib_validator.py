"""Tests for vlm_pipeline.lib.validator — incoming file validation."""

from __future__ import annotations

import tempfile
from pathlib import Path

from vlm_pipeline.lib.validator import detect_media_type, validate_incoming


class TestValidateIncoming:
    def test_valid_mp4(self):
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
            f.write(b"fake video content")
            f.flush()
            path = Path(f.name)
        try:
            result = validate_incoming(path)
            assert result.ok is True
            assert result.level == "PASS"
        finally:
            path.unlink(missing_ok=True)

    def test_valid_jpg(self):
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
            f.write(b"fake image content")
            f.flush()
            path = Path(f.name)
        try:
            result = validate_incoming(path)
            assert result.ok is True
        finally:
            path.unlink(missing_ok=True)

    def test_missing_file(self):
        result = validate_incoming("/nonexistent/path/file.mp4")
        assert result.ok is False
        assert result.message == "file_missing"

    def test_unsupported_extension(self):
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"text content")
            f.flush()
            path = Path(f.name)
        try:
            result = validate_incoming(path)
            assert result.ok is False
            assert "unsupported_ext" in result.message
        finally:
            path.unlink(missing_ok=True)

    def test_empty_file(self):
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
            path = Path(f.name)
        try:
            result = validate_incoming(path)
            assert result.ok is False
            assert result.message == "empty_file"
        finally:
            path.unlink(missing_ok=True)

    def test_heic_warns(self):
        with tempfile.NamedTemporaryFile(suffix=".heic", delete=False) as f:
            f.write(b"fake heic content")
            f.flush()
            path = Path(f.name)
        try:
            result = validate_incoming(path)
            assert result.ok is True
            assert result.level == "WARN"
            assert "heic" in result.message
        finally:
            path.unlink(missing_ok=True)


class TestDetectMediaType:
    def test_image_extensions(self):
        for ext in [".jpg", ".jpeg", ".png", ".bmp", ".webp", ".heic"]:
            assert detect_media_type(f"file{ext}") == "image"

    def test_video_extensions(self):
        for ext in [".mp4", ".avi", ".mov", ".mkv", ".webm"]:
            assert detect_media_type(f"file{ext}") == "video"

    def test_unknown_extension(self):
        assert detect_media_type("file.txt") == "unknown"

    def test_case_insensitive(self):
        assert detect_media_type("file.MP4") == "video"
        assert detect_media_type("file.JPG") == "image"
