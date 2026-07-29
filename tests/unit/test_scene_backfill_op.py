"""video_scene_backfill op 병렬화(ThreadPoolExecutor) 검증.

process_one_video 자체(판정 로직/실패 독립성)는 test_scene_backfill_helpers.py 가 이미
검증하므로, 여기서는 scene_backfill.process_one_video 를 monkeypatch 로 대체해 op 레벨
오케스트레이션(동시성 분기/카운터 집계/DB write 정확성/fail-forward)만 검증한다.

build_op_context 사용 이유: test_frame_embedding_active_default.py 참고 —
Dagster 1.13 는 임의 dummy context 직접 호출을 거부하고 build_op_context 만 지원한다.
"""

from __future__ import annotations

import threading

import pytest
from dagster import build_op_context

from vlm_pipeline.defs.ingest import scene_backfill
from vlm_pipeline.defs.ingest.scene_backfill import video_scene_backfill
from vlm_pipeline.defs.ingest.scene_backfill_helpers import (
    OUTCOME_DONE,
    OUTCOME_FAILED,
    OUTCOME_SKIP,
)

_DONE_RESULT = {"camera_angle": "level_view", "env_method": "gemini-2.5-flash", "angle_method": "dav2"}
_SKIP_TERMINAL_RESULT = {"camera_angle": None, "env_method": "deferred_missing_archive", "angle_method": None}


class _DummyDB:
    """find_deferred_scene_videos / update_video_scene mock. 스레드 안전 카운팅."""

    def __init__(self, candidates: list[dict], *, fail_writes_for: set[str] | None = None) -> None:
        self._candidates = candidates
        self._fail_writes_for = fail_writes_for or set()
        self._lock = threading.Lock()
        self.write_calls: list[str] = []
        self.write_payloads: dict[str, dict] = {}

    def find_deferred_scene_videos(self, *, limit: int) -> list[dict]:
        return self._candidates

    def update_video_scene(self, asset_id: str, **kwargs) -> None:
        with self._lock:
            self.write_calls.append(asset_id)
            self.write_payloads[asset_id] = kwargs
        if asset_id in self._fail_writes_for:
            raise RuntimeError(f"simulated db write failure for {asset_id}")


def _video(asset_id: str) -> dict:
    return {"asset_id": asset_id}


def _run_op(context_config: dict, db: _DummyDB, monkeypatch: pytest.MonkeyPatch, *, archive_mounted: bool = True):
    """archive mount preflight 를 기본 True 로 우회 — 실제 호스트에 /nas/data/archive 가
    없을 수 있으므로(dagster 컨테이너 밖) 매 테스트가 개별로 os.path.isdir 를 패치할 필요는 없다."""
    monkeypatch.setattr(scene_backfill.os.path, "isdir", lambda _p: archive_mounted)
    with build_op_context(op_config=context_config) as ctx:
        return video_scene_backfill(ctx, db)


# ─── concurrency=1 — 기존 순차 경로, 스레드풀 미생성 ──────────────────────────────


def test_concurrency_1_never_instantiates_thread_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    class _BoomPool:
        def __init__(self, *a, **k) -> None:
            raise AssertionError("concurrency=1 이면 ThreadPoolExecutor 가 생성되면 안 됨")

    monkeypatch.setattr(scene_backfill, "ThreadPoolExecutor", _BoomPool)
    monkeypatch.setattr(
        scene_backfill,
        "process_one_video",
        lambda video: (OUTCOME_DONE, dict(_DONE_RESULT), None),
    )

    db = _DummyDB([_video("a1"), _video("a2")])
    result = _run_op({"limit": 10, "concurrency": 1}, db, monkeypatch)

    assert result["done"] == 2
    assert result["failed"] == 0
    assert result["concurrency"] == 1
    assert sorted(db.write_calls) == ["a1", "a2"]


def test_concurrency_1_matches_original_sequential_semantics(monkeypatch: pytest.MonkeyPatch) -> None:
    behaviors = {
        "a1": (OUTCOME_DONE, dict(_DONE_RESULT), None),
        "a2": (OUTCOME_SKIP, dict(_SKIP_TERMINAL_RESULT), "archive_path 없음: /nas/archive/a2.mp4"),
        "a3": (OUTCOME_FAILED, None, "qwen: boom; gemini: boom"),
    }
    monkeypatch.setattr(scene_backfill, "process_one_video", lambda video: behaviors[video["asset_id"]])

    db = _DummyDB([_video(k) for k in behaviors])
    result = _run_op({"limit": 10, "concurrency": 1}, db, monkeypatch)

    assert result["done"] == 1
    assert result["skipped"] == 1
    assert result["skipped_terminal"] == 1
    assert result["failed"] == 1
    assert result["total"] == 3
    assert sorted(db.write_calls) == ["a1", "a2"]  # a3(FAILED) never writes


# ─── concurrency=4 — 8건 처리, 순차 실행과 동일 집계 + 중복 write 없음 ────────────


def _eight_video_behaviors() -> dict[str, tuple]:
    return {
        "a1": (OUTCOME_DONE, dict(_DONE_RESULT), None),
        "a2": (OUTCOME_DONE, dict(_DONE_RESULT), "gemini: partial issue"),  # partial + done
        "a3": (OUTCOME_SKIP, dict(_SKIP_TERMINAL_RESULT), "archive_path 없음: /nas/archive/a3.mp4"),
        "a4": (OUTCOME_FAILED, None, "qwen: boom; gemini: boom"),
        "a5": (OUTCOME_DONE, dict(_DONE_RESULT), None),
        "a6": (OUTCOME_SKIP, None, "처리 대상 축 없음(선택 쿼리 불일치 방어)"),  # defensive fallback, no write
        "a7": (OUTCOME_DONE, dict(_DONE_RESULT), None),
        "a8": (OUTCOME_DONE, dict(_DONE_RESULT), None),  # db write itself will fail → counts as failed
    }


def test_concurrency_4_aggregation_matches_sequential(monkeypatch: pytest.MonkeyPatch) -> None:
    behaviors = _eight_video_behaviors()
    monkeypatch.setattr(scene_backfill, "process_one_video", lambda video: behaviors[video["asset_id"]])

    db_seq = _DummyDB([_video(k) for k in behaviors], fail_writes_for={"a8"})
    result_seq = _run_op({"limit": 10, "concurrency": 1}, db_seq, monkeypatch)

    db_par = _DummyDB([_video(k) for k in behaviors], fail_writes_for={"a8"})
    result_par = _run_op({"limit": 10, "concurrency": 4}, db_par, monkeypatch)

    core_keys = ("done", "partial", "failed", "skipped", "skipped_terminal", "total")
    for key in core_keys:
        assert result_seq[key] == result_par[key], f"{key} diverged: seq={result_seq[key]} par={result_par[key]}"

    # 실측 기반 기대값 — a1,a2,a5,a7 done / a8 db write 실패로 failed 전환 / a4 classify 실패
    assert result_par["done"] == 4
    assert result_par["partial"] == 1
    assert result_par["failed"] == 2
    assert result_par["skipped"] == 2
    assert result_par["skipped_terminal"] == 1
    assert result_par["total"] == 8
    assert result_par["concurrency"] == 4

    # 모든 8건이 실제로 처리 결과에 반영됐는지 (fail-forward 로 일부가 조용히 누락되지 않음)
    assert result_par["done"] + result_par["failed"] + result_par["skipped"] == result_par["total"]

    # DB write 는 write 대상(a1,a2,a3,a5,a7,a8) 각 정확히 1회 — 중복 write 없음
    expected_writes = ["a1", "a2", "a3", "a5", "a7", "a8"]
    assert sorted(db_par.write_calls) == expected_writes
    assert len(db_par.write_calls) == len(expected_writes)  # 중복 없음(집합 크기와 길이 일치 이중검증)


# ─── worker 예외 — 한 항목이 터져도 나머지는 계속 처리(fail-forward) ─────────────


def test_worker_exception_does_not_abort_other_videos(monkeypatch: pytest.MonkeyPatch) -> None:
    def _flaky(video: dict):
        if video["asset_id"] == "boom":
            raise RuntimeError("unexpected classify crash")
        return OUTCOME_DONE, dict(_DONE_RESULT), None

    monkeypatch.setattr(scene_backfill, "process_one_video", _flaky)

    candidates = [_video(a) for a in ("a1", "a2", "boom", "a3", "a4")]
    db = _DummyDB(candidates)

    result = _run_op({"limit": 10, "concurrency": 4}, db, monkeypatch)

    assert result["total"] == 5
    assert result["done"] == 4
    assert result["failed"] == 1
    # 나머지 4건은 정상적으로 DB write 까지 완료됨 — boom 은 write 대상 자체가 아님
    assert sorted(db.write_calls) == ["a1", "a2", "a3", "a4"]


# ─── concurrency 상한 — 16 초과 설정 시 16으로 clamp ───────────────────────────


def test_concurrency_above_16_is_clamped(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_max_workers: list[int] = []
    real_executor = scene_backfill.ThreadPoolExecutor

    class _RecordingExecutor(real_executor):
        def __init__(self, max_workers=None, *a, **k):
            captured_max_workers.append(max_workers)
            super().__init__(max_workers=max_workers, *a, **k)

    monkeypatch.setattr(scene_backfill, "ThreadPoolExecutor", _RecordingExecutor)
    monkeypatch.setattr(
        scene_backfill,
        "process_one_video",
        lambda video: (OUTCOME_DONE, dict(_DONE_RESULT), None),
    )

    db = _DummyDB([_video("a1"), _video("a2")])
    result = _run_op({"limit": 10, "concurrency": 999}, db, monkeypatch)

    assert result["concurrency"] == 16
    assert captured_max_workers == [16]
    assert result["done"] == 2


def test_concurrency_zero_or_negative_is_clamped_to_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """op_config 로 0/음수를 넣는 방어적 케이스 — ThreadPoolExecutor(max_workers<=0) 는
    ValueError 이므로 최소 1로 clamp 해야 런이 죽지 않는다."""
    monkeypatch.setattr(
        scene_backfill,
        "process_one_video",
        lambda video: (OUTCOME_DONE, dict(_DONE_RESULT), None),
    )
    db = _DummyDB([_video("a1")])
    result = _run_op({"limit": 10, "concurrency": 0}, db, monkeypatch)
    assert result["concurrency"] == 1
    assert result["done"] == 1


# ─── archive mount preflight 는 동시성 분기보다 먼저 확인된다 ───────────────────


def test_archive_mount_missing_raises_before_any_processing(monkeypatch: pytest.MonkeyPatch) -> None:
    def _must_not_be_called(video):
        raise AssertionError("archive mount 미확인 상태에서 process_one_video 가 호출되면 안 됨")

    monkeypatch.setattr(scene_backfill, "process_one_video", _must_not_be_called)

    db = _DummyDB([_video("a1")])
    with pytest.raises(RuntimeError, match="archive mount not available"):
        _run_op({"limit": 10, "concurrency": 4}, db, monkeypatch, archive_mounted=False)
