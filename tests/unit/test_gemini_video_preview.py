from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from vlm_pipeline.defs.label import helpers_gemini


def test_prepare_gemini_video_for_request_returns_original_when_under_safe_bytes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    video_path = tmp_path / "small.mp4"
    video_path.write_bytes(b"0" * 1024)
    monkeypatch.setenv("GEMINI_SAFE_VIDEO_BYTES", "2048")

    resolved_path, temp_path = helpers_gemini.prepare_gemini_video_for_request(
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
        vf_arg = cmd[cmd.index("-vf") + 1]
        # helpers_gemini.prepare_gemini_video_for_request() 는 primary(기본 width) 시도가
        # request_limit 을 넘으면 640/480 fallback 두 개를 *병렬로* 시도하고, 그중 성공한
        # 것 중 가장 큰 width 를 채택한다 (17bb427 — 순차 단일 재시도가 아님).
        if "w=960" in vf_arg:
            output_path.write_bytes(b"a" * 4096)
        elif "w=640" in vf_arg:
            output_path.write_bytes(b"b" * 512)
        elif "w=480" in vf_arg:
            output_path.write_bytes(b"c" * 256)
        else:
            raise AssertionError(f"unexpected -vf width in cmd: {vf_arg}")
        return SimpleNamespace(returncode=0, stderr=b"")

    monkeypatch.setattr(helpers_gemini.subprocess, "run", fake_run)

    resolved_path, temp_path = helpers_gemini.prepare_gemini_video_for_request(
        video_path,
        duration_sec=120.0,
    )

    assert len(calls) == 3
    assert resolved_path == temp_path
    assert resolved_path is not None
    assert resolved_path != video_path
    assert resolved_path.suffix == ".mp4"
    assert resolved_path.exists()
    # width=640 fallback (512 bytes) 이 width=480 fallback (256 bytes) 보다 우선 채택된다.
    assert resolved_path.stat().st_size == 512

    helpers_gemini.cleanup_temp_path(resolved_path)
