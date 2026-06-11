"""ls_webhook review state 관리 + debounce 스케줄러."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path

from vlm_pipeline.lib.slack_notify import send_slack_alert


def _default_ls_state_path() -> Path:
    # 소스 트리(`/src/vlm/gemini/`)는 read-only bind mount 이므로 쓰기 가능 경로로 폴백.
    override = os.environ.get("LS_STATE_FILE")
    if override:
        return Path(override)
    dagster_home = os.environ.get("DAGSTER_HOME")
    if dagster_home:
        return Path(dagster_home) / "ls_review_state.json"
    return Path("/tmp/ls_review_state.json")


STATE_FILE = _default_ls_state_path()

# project_id별 sync 동시 실행 방지 (단일 webhook 컨테이너 가정).
_SYNC_IN_FLIGHT: set[int] = set()
_SYNC_GUARD_LOCK = threading.Lock()

# Submit 폭주 시 debounce: 마지막 수신 후 N초 idle이면 1회만 실행. 그 전 재수신은 timer reset.
_SYNC_DEBOUNCE_TIMERS: dict[int, threading.Timer] = {}
_SYNC_DEBOUNCE_LOCK = threading.Lock()
_SYNC_DEBOUNCE_SEC = float(os.getenv("LS_WEBHOOK_DEBOUNCE_SEC", "15"))


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def schedule_sync_debounced(project_id: int, project_title: str) -> None:
    """debounce timer로 run_sync_and_notify 스케줄. 0 이하면 즉시 실행."""
    from gemini.ls_webhook_finalize import run_sync_and_notify

    if _SYNC_DEBOUNCE_SEC <= 0:
        run_sync_and_notify(project_id, project_title)
        return
    with _SYNC_DEBOUNCE_LOCK:
        existing = _SYNC_DEBOUNCE_TIMERS.pop(project_id, None)
        if existing:
            existing.cancel()

        def _fire() -> None:
            with _SYNC_DEBOUNCE_LOCK:
                _SYNC_DEBOUNCE_TIMERS.pop(project_id, None)
            # finalize 직전 큐잉돼 있던 RECV 가 finalize 후 _fire 되어 ls_sync 가
            # review_status='finalized' 를 'reviewed' 로 회귀시키는 race 방어.
            # /webhook RECV 핸들러의 가드와 동일 정책으로 _fire 시점에도 재검사.
            current = load_state().get(str(project_id), {}).get("status")
            if current == "finalized":
                msg = (
                    f"⚠️ *{project_title}* (id={project_id}) debounce timer fire 직전 "
                    f"finalized 확인 — sync skip (회귀 방지)."
                )
                print(f"[DEBOUNCE-GUARD] {msg}")
                send_slack_alert(msg)
                return
            run_sync_and_notify(project_id, project_title)

        timer = threading.Timer(_SYNC_DEBOUNCE_SEC, _fire)
        timer.daemon = True
        _SYNC_DEBOUNCE_TIMERS[project_id] = timer
        timer.start()
        print(f"[DEBOUNCE] project {project_id} sync {_SYNC_DEBOUNCE_SEC:.1f}초 후 예약 (중복 수신 시 리셋)")
