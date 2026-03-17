from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from vlm_pipeline.defs.label import assets as label_assets


def test_prepare_gemini_video_for_request_returns_original_when_under_safe_bytes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    video_path = tmp_path / "small.mp4"
    video_path.write_bytes(b"0" * 1024)
    monkeypatch.setenv("GEMINI_SAFE_VIDEO_BYTES", "2048")

    resolved_path, temp_path = label_assets._prepare_gemini_video_for_request(
        video_path,
        duration_sec=10.0,
    )

    assert resolved_path == video_path
    assert temp_path is None


def test_prepare_gemini_video_for_request_retries_with_smaller_preview(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    video_path = tmp_path / "large.avi"
    video_path.write_bytes(b"1" * 4096)

    monkeypatch.setenv("GEMINI_SAFE_VIDEO_BYTES", "1024")
    monkeypatch.setenv("GEMINI_MAX_REQUEST_BYTES", "2048")
    monkeypatch.setenv("GEMINI_PREVIEW_TARGET_BYTES", "1536")

    calls: list[list[str]] = []

    def fake_run(cmd: list[str], capture_output: bool, check: bool) -> SimpleNamespace:
        del capture_output, check
        calls.append(cmd)
        output_path = Path(cmd[-1])
        if len(calls) == 1:
            output_path.write_bytes(b"a" * 4096)
        else:
            output_path.write_bytes(b"b" * 512)
        return SimpleNamespace(returncode=0, stderr=b"")

    monkeypatch.setattr(label_assets.subprocess, "run", fake_run)

    resolved_path, temp_path = label_assets._prepare_gemini_video_for_request(
        video_path,
        duration_sec=120.0,
    )

    assert len(calls) == 2
    assert resolved_path == temp_path
    assert resolved_path is not None
    assert resolved_path != video_path
    assert resolved_path.suffix == ".mp4"
    assert resolved_path.exists()
    assert resolved_path.stat().st_size == 512

    label_assets._cleanup_temp(resolved_path)
