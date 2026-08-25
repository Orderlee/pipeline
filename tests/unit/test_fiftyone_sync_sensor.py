from __future__ import annotations

import json
from typing import Any

import pytest

pytest.importorskip("dagster")

import requests  # noqa: E402
from dagster import RunRequest, SkipReason, build_sensor_context  # noqa: E402

import vlm_pipeline.defs.viz.sensor as sensor_mod  # noqa: E402
from vlm_pipeline.defs.viz.helpers import decide_targets, decode_cursor, encode_cursor  # noqa: E402
from vlm_pipeline.defs.viz.sensor import fiftyone_sync_sensor  # noqa: E402


def _run_sensor(context):
    """SensorDefinition 을 직접 호출(dunder ``__call__``)하면 이 dagster 버전에서
    ``required_resource_keys`` 전체를 raw_fn 에 키워드로 강제 주입해 TypeError 가 난다
    (raw_fn 이 ``context`` 하나만 받는 이 repo 관례와 충돌 — 실 daemon 실행 경로인
    ``evaluate_tick``/``wrap_sensor_evaluation`` 은 어노테이션 기반이라 문제 없음).
    ``_evaluation_fn`` 은 그 실제 경로와 동일하게 동작하고 job 해석 없이 리스트를 돌려준다."""
    return list(fiftyone_sync_sensor._evaluation_fn(context))


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeHealthSession:
    """requests.Session 대역 — /health GET 만 지원. 컨텍스트매니저(with) 로 사용됨."""

    def __init__(self, payload: dict[str, Any] | None = None, *, raises: bool = False) -> None:
        self._payload = payload or {}
        self._raises = raises

    def __enter__(self) -> "_FakeHealthSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def get(self, url: str, *, timeout: int) -> _FakeResponse:
        if self._raises:
            raise requests.ConnectionError("analysis-sync unreachable")
        return _FakeResponse(self._payload)


def _patch_health(
    monkeypatch: pytest.MonkeyPatch, payload: dict[str, Any] | None = None, *, raises: bool = False
) -> None:
    monkeypatch.setattr(sensor_mod.requests, "Session", lambda: _FakeHealthSession(payload, raises=raises))


class _FakeDB:
    def __init__(self, snapshot: dict[str, Any]) -> None:
        self._snapshot = snapshot

    def fiftyone_sync_snapshot(self) -> dict[str, Any]:
        return dict(self._snapshot)


_BASE_SNAPSHOT = {
    "frame_n": 100,
    "caption_n": 50,
    "prompt_n": 10,
    "bank_n": 2,
    "bank_latest": "2026-08-01T00:00:00+00:00",
}


# ---------------------------------------------------------------------------
# decide_targets / cursor 순수 헬퍼
# ---------------------------------------------------------------------------


def test_decide_targets_first_tick_returns_frames_only() -> None:
    assert decide_targets(None, _BASE_SNAPSHOT, True) == ["frames"]


def test_decide_targets_frame_change_includes_frames() -> None:
    prev = dict(_BASE_SNAPSHOT)
    cur = dict(_BASE_SNAPSHOT)
    cur["frame_n"] = prev["frame_n"] + 1
    assert decide_targets(prev, cur, True) == ["frames"]


def test_decide_targets_caption_change_includes_frames() -> None:
    prev = dict(_BASE_SNAPSHOT)
    cur = dict(_BASE_SNAPSHOT)
    cur["caption_n"] = prev["caption_n"] + 1
    assert decide_targets(prev, cur, True) == ["frames"]


def test_decide_targets_prompt_change_includes_prompts_when_enabled() -> None:
    prev = dict(_BASE_SNAPSHOT)
    cur = dict(_BASE_SNAPSHOT)
    cur["prompt_n"] = prev["prompt_n"] + 5
    assert decide_targets(prev, cur, True) == ["prompts"]


def test_decide_targets_bank_n_change_includes_prompts() -> None:
    prev = dict(_BASE_SNAPSHOT)
    cur = dict(_BASE_SNAPSHOT)
    cur["bank_n"] = prev["bank_n"] + 1
    assert decide_targets(prev, cur, True) == ["prompts"]


def test_decide_targets_bank_latest_change_includes_prompts() -> None:
    prev = dict(_BASE_SNAPSHOT)
    cur = dict(_BASE_SNAPSHOT)
    cur["bank_latest"] = "2026-08-20T00:00:00+00:00"
    assert decide_targets(prev, cur, True) == ["prompts"]


def test_decide_targets_prompts_disabled_suppresses_prompts_target() -> None:
    prev = dict(_BASE_SNAPSHOT)
    cur = dict(_BASE_SNAPSHOT)
    cur["prompt_n"] = prev["prompt_n"] + 5
    assert decide_targets(prev, cur, False) == []


def test_decide_targets_decrease_counts_as_change() -> None:
    prev = dict(_BASE_SNAPSHOT)
    cur = dict(_BASE_SNAPSHOT)
    cur["frame_n"] = prev["frame_n"] - 1
    assert decide_targets(prev, cur, True) == ["frames"]


def test_decide_targets_no_change_returns_empty() -> None:
    prev = dict(_BASE_SNAPSHOT)
    cur = dict(_BASE_SNAPSHOT)
    assert decide_targets(prev, cur, True) == []


def test_decide_targets_both_frames_and_prompts_change() -> None:
    prev = dict(_BASE_SNAPSHOT)
    cur = dict(_BASE_SNAPSHOT)
    cur["frame_n"] = prev["frame_n"] + 1
    cur["prompt_n"] = prev["prompt_n"] + 1
    assert decide_targets(prev, cur, True) == ["frames", "prompts"]


def test_cursor_roundtrip() -> None:
    encoded = encode_cursor(_BASE_SNAPSHOT)
    assert decode_cursor(encoded) == _BASE_SNAPSHOT


def test_decode_cursor_none_when_missing() -> None:
    assert decode_cursor(None) is None
    assert decode_cursor("") is None


def test_decode_cursor_none_when_invalid_json() -> None:
    assert decode_cursor("{not json") is None


def test_decode_cursor_none_when_not_a_dict() -> None:
    assert decode_cursor("[1, 2, 3]") is None


# ---------------------------------------------------------------------------
# fiftyone_sync_sensor (dagster 배선)
# ---------------------------------------------------------------------------


def test_sensor_skips_when_url_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FIFTYONE_SYNC_API_URL", raising=False)
    context = build_sensor_context(resources={"db": _FakeDB(_BASE_SNAPSHOT)})

    results = _run_sensor(context)

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)


def test_sensor_skips_when_health_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIFTYONE_SYNC_API_URL", "http://analysis-sync:8010")
    _patch_health(monkeypatch, raises=True)
    context = build_sensor_context(resources={"db": _FakeDB(_BASE_SNAPSHOT)})

    results = _run_sensor(context)

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)


def test_sensor_skips_and_holds_cursor_when_busy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIFTYONE_SYNC_API_URL", "http://analysis-sync:8010")
    _patch_health(monkeypatch, payload={"ok": True, "busy": True})
    initial_cursor = encode_cursor(_BASE_SNAPSHOT)
    context = build_sensor_context(
        cursor=initial_cursor,
        resources={"db": _FakeDB(_BASE_SNAPSHOT)},
    )

    results = _run_sensor(context)

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
    assert context.cursor == initial_cursor


def test_sensor_first_tick_requests_frames_only_and_advances_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIFTYONE_SYNC_API_URL", "http://analysis-sync:8010")
    _patch_health(monkeypatch, payload={"ok": True, "busy": False})
    context = build_sensor_context(resources={"db": _FakeDB(_BASE_SNAPSHOT)})

    results = _run_sensor(context)

    assert len(results) == 1
    run_request = results[0]
    assert isinstance(run_request, RunRequest)
    assert run_request.run_config == {"ops": {"trigger_fiftyone_sync": {"config": {"targets": ["frames"]}}}}
    assert run_request.tags["fiftyone_sync_targets"] == "frames"
    assert context.cursor == encode_cursor(_BASE_SNAPSHOT)


def test_sensor_prompt_change_includes_prompts_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIFTYONE_SYNC_API_URL", "http://analysis-sync:8010")
    monkeypatch.delenv("FIFTYONE_SYNC_PROMPTS_ENABLED", raising=False)
    _patch_health(monkeypatch, payload={"ok": True, "busy": False})

    prev_snapshot = dict(_BASE_SNAPSHOT)
    new_snapshot = dict(_BASE_SNAPSHOT)
    new_snapshot["prompt_n"] = prev_snapshot["prompt_n"] + 100

    context = build_sensor_context(
        cursor=encode_cursor(prev_snapshot),
        resources={"db": _FakeDB(new_snapshot)},
    )

    results = _run_sensor(context)

    assert len(results) == 1
    run_request = results[0]
    assert isinstance(run_request, RunRequest)
    assert run_request.run_config == {"ops": {"trigger_fiftyone_sync": {"config": {"targets": ["prompts"]}}}}
    assert context.cursor == encode_cursor(new_snapshot)


def test_sensor_kill_switch_excludes_prompts_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIFTYONE_SYNC_API_URL", "http://analysis-sync:8010")
    monkeypatch.setenv("FIFTYONE_SYNC_PROMPTS_ENABLED", "false")
    _patch_health(monkeypatch, payload={"ok": True, "busy": False})

    prev_snapshot = dict(_BASE_SNAPSHOT)
    new_snapshot = dict(_BASE_SNAPSHOT)
    new_snapshot["prompt_n"] = prev_snapshot["prompt_n"] + 100

    context = build_sensor_context(
        cursor=encode_cursor(prev_snapshot),
        resources={"db": _FakeDB(new_snapshot)},
    )

    results = _run_sensor(context)

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
    assert context.cursor == encode_cursor(new_snapshot)


def test_sensor_no_change_skips_and_refreshes_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIFTYONE_SYNC_API_URL", "http://analysis-sync:8010")
    _patch_health(monkeypatch, payload={"ok": True, "busy": False})

    context = build_sensor_context(
        cursor=encode_cursor(_BASE_SNAPSHOT),
        resources={"db": _FakeDB(_BASE_SNAPSHOT)},
    )

    results = _run_sensor(context)

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
    assert context.cursor == encode_cursor(_BASE_SNAPSHOT)
    assert json.loads(context.cursor) == _BASE_SNAPSHOT
