"""GCS 다운로드 스크립트의 incoming _DONE 스킵 로직 단위 테스트."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "gcp"))
from download_from_gcs_rclone import (
    DONE_MARKER_FILENAME,
    _incoming_done_marker_exists,
)


@pytest.fixture
def incoming_dir(tmp_path: Path) -> Path:
    """tmp_path 아래에 incoming 구조를 생성."""
    d = tmp_path / "incoming" / "gcp" / "source-c-event-bucket"
    d.mkdir(parents=True)
    return d


class TestIncomingDoneMarkerExists:
    def test_returns_true_when_done_marker_present(self, incoming_dir: Path) -> None:
        date_dir = incoming_dir / "20260409"
        date_dir.mkdir()
        (date_dir / DONE_MARKER_FILENAME).write_text("completed_at=2026-04-09T12:00:00Z\n")

        result = _incoming_done_marker_exists(str(incoming_dir), "20260409")
        assert result is True

    def test_returns_false_when_no_done_marker(self, incoming_dir: Path) -> None:
        date_dir = incoming_dir / "20260409"
        date_dir.mkdir()
        (date_dir / "video.mp4").write_bytes(b"\x00" * 100)

        result = _incoming_done_marker_exists(str(incoming_dir), "20260409")
        assert result is False

    def test_returns_false_when_folder_missing(self, incoming_dir: Path) -> None:
        result = _incoming_done_marker_exists(str(incoming_dir), "20260409")
        assert result is False

    def test_returns_false_when_download_dir_missing(self, tmp_path: Path) -> None:
        result = _incoming_done_marker_exists(str(tmp_path / "nonexistent"), "20260409")
        assert result is False

    def test_ignores_done_marker_in_wrong_folder(self, incoming_dir: Path) -> None:
        other = incoming_dir / "20260410"
        other.mkdir()
        (other / DONE_MARKER_FILENAME).write_text("done")

        result = _incoming_done_marker_exists(str(incoming_dir), "20260409")
        assert result is False
