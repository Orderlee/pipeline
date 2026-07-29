"""scene_backfill_helpers.process_one_video 아웃컴 검증 — env_backfill_helpers 대응 테스트.

Layer 3 헬퍼라 DB/dagster 없이 순수 함수 테스트만.
design: docs/design-docs/camera-angle-grouping-2026-07-29.md §3.2, §7.

Qwen(env 그룹: subject_scale/occlusion_state/environment_type/daynight_type/weather +
env_method) 과 Gemini(angle 그룹: camera_angle + angle_method) 는 서로 독립된 라벨러다 —
이 파일의 핵심은 "한쪽이 실패해도 다른 쪽 결과는 보존된다"는 실패 독립성 검증이다.
"""

from __future__ import annotations

from vlm_pipeline.defs.ingest.scene_backfill_helpers import (
    OUTCOME_DONE,
    OUTCOME_FAILED,
    OUTCOME_SKIP,
    _terminal_marker_result,
    process_one_video,
)

_QWEN_SUCCESS_RESULT = {
    "subject_scale": "subject_legible",
    "occlusion_state": "unoccluded",
    "environment_type": "outdoor",
    "daynight_type": "day",
    "weather": "clear",
    "env_method": "qwen2.5-vl",
}

_QWEN_NO_FRAMES_RESULT = {
    "subject_scale": None,
    "occlusion_state": None,
    "environment_type": None,
    "daynight_type": None,
    "weather": None,
    "env_method": None,
}

_ANGLE_SUCCESS_RESULT = {"camera_angle": "oblique_view", "angle_method": "gemini-2.5-flash"}
_ANGLE_NO_FRAMES_RESULT = {"camera_angle": None, "angle_method": None}


def _both_deferred_video(asset_id: str, archive_path: str) -> dict:
    return {
        "asset_id": asset_id,
        "archive_path": archive_path,
        "angle_method": "deferred",
        "env_method": "deferred",
        "camera_angle": None,
        "subject_scale": None,
        "occlusion_state": None,
        "environment_type": None,
        "daynight_type": None,
        "weather": None,
    }


def _must_not_be_called(*_a, **_k):
    raise AssertionError("이미 완료된 그룹의 classify_fn 은 호출되면 안 됨")


# ─── archive_path 없음 — 두 그룹 모두 시도조차 못함 ──────────────────────────────


def test_process_one_video_skips_when_archive_missing() -> None:
    outcome, result, err = process_one_video(
        _both_deferred_video("a1", "/nas/archive/a1.mp4"),
        path_exists_fn=lambda _p: False,
        qwen_classify_fn=_must_not_be_called,
        angle_classify_fn=_must_not_be_called,
    )
    assert outcome == OUTCOME_SKIP
    assert result == {
        "camera_angle": None,
        "subject_scale": None,
        "occlusion_state": None,
        "environment_type": None,
        "daynight_type": None,
        "weather": None,
        "env_method": "deferred_missing_archive",
        "angle_method": "deferred_missing_archive",
    }
    assert "archive_path 없음" in err


# ─── 양쪽 다 성공 ─────────────────────────────────────────────────────────────


def test_process_one_video_both_succeed_returns_done() -> None:
    outcome, result, err = process_one_video(
        _both_deferred_video("a2", "/nas/archive/a2.mp4"),
        path_exists_fn=lambda _p: True,
        qwen_classify_fn=lambda *_a, **_k: dict(_QWEN_SUCCESS_RESULT),
        angle_classify_fn=lambda *_a, **_k: dict(_ANGLE_SUCCESS_RESULT),
    )
    assert outcome == OUTCOME_DONE
    assert err is None
    assert result == {
        "camera_angle": "oblique_view",
        "subject_scale": "subject_legible",
        "occlusion_state": "unoccluded",
        "environment_type": "outdoor",
        "daynight_type": "day",
        "weather": "clear",
        "env_method": "qwen2.5-vl",
        "angle_method": "gemini-2.5-flash",
    }


# ─── 실패 독립성 — 핵심 회귀 테스트 ────────────────────────────────────────────


def test_process_one_video_gemini_fails_qwen_succeeds_preserves_qwen_and_keeps_angle_deferred() -> None:
    """Gemini 가 죽어도 Qwen 결과는 그대로 기록되고 angle_method 만 'deferred' 로 남아
    재시도돼야 한다."""

    def _gemini_boom(*_a, **_k):
        raise TimeoutError("vertex unreachable")

    outcome, result, err = process_one_video(
        _both_deferred_video("a3", "/nas/archive/a3.mp4"),
        path_exists_fn=lambda _p: True,
        qwen_classify_fn=lambda *_a, **_k: dict(_QWEN_SUCCESS_RESULT),
        angle_classify_fn=_gemini_boom,
    )
    assert outcome == OUTCOME_DONE
    assert result["subject_scale"] == "subject_legible"
    assert result["occlusion_state"] == "unoccluded"
    assert result["environment_type"] == "outdoor"
    assert result["daynight_type"] == "day"
    assert result["weather"] == "clear"
    assert result["env_method"] == "qwen2.5-vl"
    # camera_angle 계열은 손대지 않고 그대로(원래 None/'deferred') 보존 — 다음 tick 재시도.
    assert result["camera_angle"] is None
    assert result["angle_method"] == "deferred"
    assert "gemini" in err
    assert "vertex unreachable" in err


def test_process_one_video_qwen_fails_gemini_succeeds_preserves_gemini_and_keeps_env_deferred() -> None:
    """반대 케이스 — Qwen 이 죽어도 Gemini 결과는 그대로 기록되고 env_method 만 'deferred'
    로 남아 재시도돼야 한다."""

    def _qwen_boom(*_a, **_k):
        raise TimeoutError("qwen down")

    outcome, result, err = process_one_video(
        _both_deferred_video("a4", "/nas/archive/a4.mp4"),
        path_exists_fn=lambda _p: True,
        qwen_classify_fn=_qwen_boom,
        angle_classify_fn=lambda *_a, **_k: dict(_ANGLE_SUCCESS_RESULT),
    )
    assert outcome == OUTCOME_DONE
    assert result["camera_angle"] == "oblique_view"
    assert result["angle_method"] == "gemini-2.5-flash"
    # env 계열은 손대지 않고 그대로(원래 None/'deferred') 보존 — 다음 tick 재시도.
    assert result["subject_scale"] is None
    assert result["occlusion_state"] is None
    assert result["environment_type"] is None
    assert result["daynight_type"] is None
    assert result["weather"] is None
    assert result["env_method"] == "deferred"
    assert "qwen" in err
    assert "qwen down" in err


def test_process_one_video_both_fail_returns_failed_and_stays_deferred() -> None:
    def _qwen_boom(*_a, **_k):
        raise TimeoutError("qwen down")

    def _gemini_boom(*_a, **_k):
        raise TimeoutError("vertex unreachable")

    outcome, result, err = process_one_video(
        _both_deferred_video("a5", "/nas/archive/a5.mp4"),
        path_exists_fn=lambda _p: True,
        qwen_classify_fn=_qwen_boom,
        angle_classify_fn=_gemini_boom,
    )
    assert outcome == OUTCOME_FAILED
    assert result is None
    assert "qwen down" in err
    assert "vertex unreachable" in err


# ─── 프레임 추출 실패(no-frames) — 그룹별 터미널 마커 ────────────────────────────


def test_process_one_video_qwen_no_frames_terminal_marks_only_env_group() -> None:
    outcome, result, err = process_one_video(
        _both_deferred_video("a6", "/nas/archive/a6.mp4"),
        path_exists_fn=lambda _p: True,
        qwen_classify_fn=lambda *_a, **_k: dict(_QWEN_NO_FRAMES_RESULT),
        angle_classify_fn=lambda *_a, **_k: dict(_ANGLE_SUCCESS_RESULT),
    )
    # Gemini 는 성공했으므로 DONE(부분 성공) — env 그룹만 터미널 마커.
    assert outcome == OUTCOME_DONE
    assert result["env_method"] == "deferred_no_frames"
    assert result["subject_scale"] is None
    assert result["camera_angle"] == "oblique_view"
    assert result["angle_method"] == "gemini-2.5-flash"
    assert "qwen" in err


def test_process_one_video_angle_no_frames_terminal_marks_only_angle_group() -> None:
    outcome, result, err = process_one_video(
        _both_deferred_video("a7", "/nas/archive/a7.mp4"),
        path_exists_fn=lambda _p: True,
        qwen_classify_fn=lambda *_a, **_k: dict(_QWEN_SUCCESS_RESULT),
        angle_classify_fn=lambda *_a, **_k: dict(_ANGLE_NO_FRAMES_RESULT),
    )
    assert outcome == OUTCOME_DONE
    assert result["angle_method"] == "deferred_no_frames"
    assert result["camera_angle"] is None
    assert result["env_method"] == "qwen2.5-vl"
    assert result["subject_scale"] == "subject_legible"
    assert "gemini" in err


def test_process_one_video_both_no_frames_returns_skip_with_both_terminal_markers() -> None:
    outcome, result, err = process_one_video(
        _both_deferred_video("a8", "/nas/archive/a8.mp4"),
        path_exists_fn=lambda _p: True,
        qwen_classify_fn=lambda *_a, **_k: dict(_QWEN_NO_FRAMES_RESULT),
        angle_classify_fn=lambda *_a, **_k: dict(_ANGLE_NO_FRAMES_RESULT),
    )
    assert outcome == OUTCOME_SKIP
    assert result["env_method"] == "deferred_no_frames"
    assert result["angle_method"] == "deferred_no_frames"
    assert result["camera_angle"] is None
    assert result["subject_scale"] is None
    assert "qwen" in err
    assert "gemini" in err


# ─── 이미 한쪽이 완료된 행 — 완료된 쪽 classify_fn 은 호출조차 안 됨 ───────────────


def test_process_one_video_only_calls_gemini_when_env_already_resolved() -> None:
    video = _both_deferred_video("a9", "/nas/archive/a9.mp4")
    video.update(
        env_method="qwen2.5-vl",
        subject_scale="subject_legible",
        occlusion_state="unoccluded",
        environment_type="outdoor",
        daynight_type="day",
        weather="clear",
    )
    outcome, result, err = process_one_video(
        video,
        path_exists_fn=lambda _p: True,
        qwen_classify_fn=_must_not_be_called,
        angle_classify_fn=lambda *_a, **_k: dict(_ANGLE_SUCCESS_RESULT),
    )
    assert outcome == OUTCOME_DONE
    assert err is None
    assert result["camera_angle"] == "oblique_view"
    assert result["angle_method"] == "gemini-2.5-flash"
    # env 계열은 기존 값 그대로 pass-through.
    assert result["env_method"] == "qwen2.5-vl"
    assert result["subject_scale"] == "subject_legible"


def test_process_one_video_only_calls_qwen_when_angle_already_resolved() -> None:
    video = _both_deferred_video("a10", "/nas/archive/a10.mp4")
    video.update(angle_method="gemini-2.5-flash", camera_angle="level_view")
    outcome, result, err = process_one_video(
        video,
        path_exists_fn=lambda _p: True,
        qwen_classify_fn=lambda *_a, **_k: dict(_QWEN_SUCCESS_RESULT),
        angle_classify_fn=_must_not_be_called,
    )
    assert outcome == OUTCOME_DONE
    assert err is None
    assert result["env_method"] == "qwen2.5-vl"
    assert result["subject_scale"] == "subject_legible"
    # angle 계열은 기존 값 그대로 pass-through.
    assert result["angle_method"] == "gemini-2.5-flash"
    assert result["camera_angle"] == "level_view"


# ─── SCENE_FRAMES env → Qwen frames kwarg (Gemini 는 항상 1프레임, frames 인자 없음) ────


def test_process_one_video_resolves_frames_from_scene_frames_env(monkeypatch) -> None:
    monkeypatch.setenv("SCENE_FRAMES", "3")
    captured: dict = {}

    def _fake_qwen(archive_path, *, frames):
        captured["archive_path"] = archive_path
        captured["frames"] = frames
        return dict(_QWEN_SUCCESS_RESULT)

    process_one_video(
        _both_deferred_video("a11", "/nas/archive/a11.mp4"),
        path_exists_fn=lambda _p: True,
        qwen_classify_fn=_fake_qwen,
        angle_classify_fn=lambda *_a, **_k: dict(_ANGLE_SUCCESS_RESULT),
    )
    assert captured["archive_path"] == "/nas/archive/a11.mp4"
    assert captured["frames"] == 3


def test_process_one_video_defaults_frames_to_one(monkeypatch) -> None:
    monkeypatch.delenv("SCENE_FRAMES", raising=False)
    captured: dict = {}

    def _fake_qwen(_archive_path, *, frames):
        captured["frames"] = frames
        return dict(_QWEN_SUCCESS_RESULT)

    process_one_video(
        _both_deferred_video("a12", "/nas/archive/a12.mp4"),
        path_exists_fn=lambda _p: True,
        qwen_classify_fn=_fake_qwen,
        angle_classify_fn=lambda *_a, **_k: dict(_ANGLE_SUCCESS_RESULT),
    )
    assert captured["frames"] == 1


def test_process_one_video_angle_classify_fn_called_without_frames_kwarg() -> None:
    """Gemini 는 1프레임 고정이라 frames kwarg 를 받지 않는다(다중프레임 금지)."""
    captured: dict = {}

    def _fake_angle(archive_path):
        captured["archive_path"] = archive_path
        captured["args"] = "positional-only, no frames kwarg"
        return dict(_ANGLE_SUCCESS_RESULT)

    process_one_video(
        _both_deferred_video("a13", "/nas/archive/a13.mp4"),
        path_exists_fn=lambda _p: True,
        qwen_classify_fn=lambda *_a, **_k: dict(_QWEN_SUCCESS_RESULT),
        angle_classify_fn=_fake_angle,
    )
    assert captured["archive_path"] == "/nas/archive/a13.mp4"


# ─── _terminal_marker_result: 그룹 재배치(env=5축, angle=camera_angle 단독) 검증 ────────


def test_terminal_marker_result_both_deferred_nulls_everything() -> None:
    """가장 흔한 케이스 — 신규 ingest 행은 angle_method/env_method 둘 다 'deferred'."""
    video = {"asset_id": "b1", "angle_method": "deferred", "env_method": "deferred"}
    marker = _terminal_marker_result(video, "deferred_missing_archive")
    assert marker == {
        "camera_angle": None,
        "subject_scale": None,
        "occlusion_state": None,
        "environment_type": None,
        "daynight_type": None,
        "weather": None,
        "env_method": "deferred_missing_archive",
        "angle_method": "deferred_missing_archive",
    }


def test_terminal_marker_result_preserves_env_group_when_only_angle_deferred() -> None:
    """env_method 는 이제 subject_scale/occlusion_state 까지 포함한 Qwen 5축 전체의
    provenance 다 — camera_angle 만 Gemini 로 분리됐기 때문이다. angle_method 만 deferred
    인 행이 archive 유실로 SKIP 되어도 이미 채워진 Qwen 5축 값은 보존돼야 한다."""
    video = {
        "asset_id": "b2",
        "angle_method": "deferred",
        "env_method": "qwen2.5-vl",
        "subject_scale": "subject_legible",
        "occlusion_state": "unoccluded",
        "environment_type": "indoor",
        "daynight_type": "night",
        "weather": "clear",
    }
    marker = _terminal_marker_result(video, "deferred_missing_archive")
    assert marker["angle_method"] == "deferred_missing_archive"
    assert marker["camera_angle"] is None
    # env 그룹(Qwen 5축)은 손대지 않고 그대로 보존
    assert marker["env_method"] == "qwen2.5-vl"
    assert marker["subject_scale"] == "subject_legible"
    assert marker["occlusion_state"] == "unoccluded"
    assert marker["environment_type"] == "indoor"
    assert marker["daynight_type"] == "night"
    assert marker["weather"] == "clear"


def test_terminal_marker_result_preserves_camera_angle_when_only_env_deferred() -> None:
    """대칭 케이스 — angle 그룹(camera_angle 단독)이 이미 채워져 있고 env_method 만
    'deferred' 인 행."""
    video = {
        "asset_id": "b3",
        "angle_method": "gemini-2.5-flash",
        "env_method": "deferred",
        "camera_angle": "oblique_view",
    }
    marker = _terminal_marker_result(video, "deferred_no_frames")
    assert marker["env_method"] == "deferred_no_frames"
    assert marker["subject_scale"] is None
    assert marker["occlusion_state"] is None
    assert marker["environment_type"] is None
    assert marker["daynight_type"] is None
    assert marker["weather"] is None
    # angle 그룹(camera_angle 단독)은 손대지 않고 그대로 보존
    assert marker["angle_method"] == "gemini-2.5-flash"
    assert marker["camera_angle"] == "oblique_view"


def test_process_one_video_missing_archive_preserves_resolved_env_group_end_to_end() -> None:
    """process_one_video 전체 경로에서도 이미 채워진 Qwen 5축 보존이 동작하는지(archive
    유실 케이스)."""
    video = {
        "asset_id": "b4",
        "archive_path": "/nas/archive/legacy/b4.mp4",
        "angle_method": "deferred",
        "env_method": "heuristic",
        "subject_scale": "subject_marginal",
        "occlusion_state": "truncated",
        "environment_type": "outdoor",
        "daynight_type": "day",
        "weather": "clear",
    }
    outcome, result, _err = process_one_video(
        video,
        path_exists_fn=lambda _p: False,
        qwen_classify_fn=_must_not_be_called,
        angle_classify_fn=_must_not_be_called,
    )
    assert outcome == OUTCOME_SKIP
    assert result["angle_method"] == "deferred_missing_archive"
    assert result["camera_angle"] is None
    assert result["env_method"] == "heuristic"
    assert result["subject_scale"] == "subject_marginal"
    assert result["occlusion_state"] == "truncated"
    assert result["environment_type"] == "outdoor"
    assert result["daynight_type"] == "day"
    assert result["weather"] == "clear"
