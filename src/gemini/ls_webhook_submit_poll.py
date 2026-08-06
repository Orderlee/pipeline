"""LS 최종 submit(F1) 폴링 → sync + finalize 자동 트리거.

내부 LS의 최종검수자가 프로젝트 submit 버튼을 누르면, Slack `/sync-approve` 와
동일한 확정 체인(ls_sync → finalize_labels → Dagster post_review)을 자동 실행한다.

설계 노트 (2026-07-22):
- LS 포크 코드는 무수정 — submit API 는 self-contained 유지(외주 GCP LS 무영향),
  감지는 파이프라인 쪽 폴링으로만 한다. 웹훅 체계와도 독립(웹훅 복구 여부 무관).
- 기존 안전장치를 인터록으로 재사용:
    * sync 성공만이 state.status='pending_finalize' 를 남긴다 → 그때만 finalize 진행.
      sync 실패 시 state 불변 → 다음 tick 재시도 (쿨다운 적용, Slack 알림 스팸 방지).
    * 이미 'finalized' 인 프로젝트의 재제출은 기존 정책대로 무시.
    * state 에 없는 프로젝트(ls_tasks.py create 미경유 — 수동 생성 등)는 Slack 경로와
      동일하게 대상 제외 (finalize_project 자체가 state 필수).
- Slack /sync-approve 는 제거하지 않고 수동 폴백으로 병행한다 (둘 다 멱등).

환경변수:
    ENABLE_LS_SUBMIT_FINALIZE       기본 "1" — "0"/"false" 로 비활성화
    LS_SUBMIT_POLL_INTERVAL_SEC     폴링 주기(초), 기본 60
    LS_SUBMIT_RETRY_COOLDOWN_SEC    프로젝트별 실패 재시도 간격(초), 기본 900
"""

from __future__ import annotations

import os
import threading
import time
from typing import Callable

import requests

from gemini.ls_webhook_env import LS_URL
from gemini.ls_webhook_finalize import finalize_project, run_sync_and_notify
from gemini.ls_webhook_state import load_state

POLL_ENABLED = os.environ.get("ENABLE_LS_SUBMIT_FINALIZE", "1").strip().lower() not in {"0", "false", "no"}
POLL_INTERVAL_SEC = int(os.environ.get("LS_SUBMIT_POLL_INTERVAL_SEC", "60"))
RETRY_COOLDOWN_SEC = int(os.environ.get("LS_SUBMIT_RETRY_COOLDOWN_SEC", "900"))

# 프로젝트별 마지막 처리 시도 시각 — sync 실패가 반복될 때 매 tick Slack 알림이
# 쏟아지지 않게 쿨다운. 프로세스 메모리라 재시작 시 초기화되는 것으로 충분.
_last_attempt_at: dict[int, float] = {}


def poll_once(resolve_headers: Callable[[], dict]) -> dict:
    """전 프로젝트 1회 스캔. 반환값은 테스트/로그용 카운터."""
    counts = {"submitted": 0, "finalized": 0, "skipped": 0, "cooldown": 0}
    headers = resolve_headers()

    resp = requests.get(f"{LS_URL}/api/projects/", headers=headers, params={"page_size": 1000}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    projects = data.get("results", data) if isinstance(data, dict) else data

    for project in projects:
        pid = int(project["id"])
        title = project.get("title", str(pid))
        entry = load_state().get(str(pid), {})

        # Slack 경로와 동일한 대상 조건: 파이프라인이 아는(state 존재) + 미확정 프로젝트만.
        if not entry or entry.get("status") == "finalized":
            counts["skipped"] += 1
            continue

        try:
            sr = requests.get(f"{LS_URL}/api/projects/{pid}/submit-state", headers=headers, timeout=15)
            if sr.status_code != 200 or not sr.json().get("is_submitted"):
                counts["skipped"] += 1
                continue
        except requests.RequestException:
            counts["skipped"] += 1
            continue

        counts["submitted"] += 1
        now = time.monotonic()
        if now - _last_attempt_at.get(pid, -RETRY_COOLDOWN_SEC) < RETRY_COOLDOWN_SEC:
            counts["cooldown"] += 1
            continue
        _last_attempt_at[pid] = now

        print(f"[SUBMIT-POLL] '{title}' (id={pid}) 최종 submit 감지 → sync 실행", flush=True)
        run_sync_and_notify(pid, title)

        # 인터록: sync 성공만 pending_finalize 를 남긴다 — 실패면 finalize 하지 않고 재시도.
        if load_state().get(str(pid), {}).get("status") == "pending_finalize":
            finalize_project(pid)
            counts["finalized"] += 1
            print(f"[SUBMIT-POLL] '{title}' (id={pid}) finalize 완료", flush=True)
        else:
            print(f"[SUBMIT-POLL] '{title}' (id={pid}) sync 미완료 — {RETRY_COOLDOWN_SEC}s 후 재시도", flush=True)

    return counts


def start_submit_poller(resolve_headers: Callable[[], dict]) -> threading.Thread | None:
    """serve 프로세스에서 데몬 스레드로 기동. 비활성화면 None."""
    if not POLL_ENABLED:
        print("[INFO] LS submit 폴러 비활성 (ENABLE_LS_SUBMIT_FINALIZE=0)")
        return None

    def _loop() -> None:
        while True:
            try:
                poll_once(resolve_headers)
            except Exception as exc:  # 폴러는 어떤 예외에도 죽지 않는다 — 다음 tick 재시도
                print(f"[SUBMIT-POLL][ERROR] {exc}", flush=True)
            time.sleep(POLL_INTERVAL_SEC)

    thread = threading.Thread(target=_loop, daemon=True, name="ls-submit-poller")
    thread.start()
    print(
        f"[INFO] LS submit 폴러 가동 — 주기 {POLL_INTERVAL_SEC}s, 실패 쿨다운 {RETRY_COOLDOWN_SEC}s "
        f"(끄기: ENABLE_LS_SUBMIT_FINALIZE=0)"
    )
    return thread
