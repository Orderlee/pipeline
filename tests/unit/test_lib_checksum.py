"""Tests for vlm_pipeline.lib.checksum — SHA-256 checksum utilities."""

from __future__ import annotations

import tempfile
from pathlib import Path

from vlm_pipeline.lib.checksum import sha256_bytes, sha256sum


class TestSha256Bytes:
    def test_known_hash(self):
        result = sha256_bytes(b"hello")
        assert result == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    def test_empty_bytes(self):
        result = sha256_bytes(b"")
        assert result == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    def test_deterministic(self):
        assert sha256_bytes(b"test") == sha256_bytes(b"test")


class TestSha256Sum:
    def test_file_hash(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            f.write(b"hello")
            f.flush()
            path = Path(f.name)

        try:
            result = sha256sum(path)
            assert result == sha256_bytes(b"hello")
        finally:
            path.unlink(missing_ok=True)

    def test_matches_bytes_version(self):
        data = b"test data for checksum"
        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            f.write(data)
            f.flush()
            path = Path(f.name)

        try:
            assert sha256sum(path) == sha256_bytes(data)
        finally:
            path.unlink(missing_ok=True)
