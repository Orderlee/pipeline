"""GenAI polling sensor — 비동기 엔진(Kling, Higgsfield) 의 외부 job 상태 확인.

Phase 3.5: 어댑터 코드를 직접 import 하지 않고 docker/genai 컨테이너의
internal API 를 HTTP 경유로 호출 (Codex Q3 — Dagster 이미지에 어댑터 코드 COPY
또는 mount 안 해도 됨).

흐름:
  1) GET  http://genai:8088/internal/jobs/pending  →  N개 pending job
  2) 각 job 에 POST /internal/jobs/<job_id>/poll → genai 컨테이너가 어댑터 호출
     + 'done' 시 NAS finalize 까지 일괄 처리
  3) sensor 는 결과 카운트만 로깅 + SkipReason

genai 컨테이너 다운 시 SkipReason 으로 기록만 하고 다음 tick. 동기 엔진은
submit 시점에 결과 처리되므로 polling 대상 아님.
"""

from __future__ import annotations

import os

import requests
from dagster import DefaultSensorStatus, SkipReason, sensor

from vlm_pipeline.lib.env_utils import int_env

_POLL_INTERVAL = int_env("GENAI_POLL_INTERVAL_SECONDS", 30)
_POLL_BATCH = int_env("GENAI_POLL_BATCH", 20)
_GENAI_INTERNAL_BASE = os.getenv("GENAI_INTERNAL_BASE", "http://genai:8088").rstrip("/")
_GENAI_INTERNAL_TIMEOUT = int_env("GENAI_INTERNAL_TIMEOUT", 60)


def _internal_token() -> str | None:
    return (os.getenv("GENAI_INTERNAL_TOKEN") or "").strip() or None


@sensor(
    name="genai_poll_sensor",
    minimum_interval_seconds=_POLL_INTERVAL,
    # RUNNING 기본 — 과거 STOPPED 기본이라 Dagster storage 초기화/신규 배포마다 꺼져
    # 비동기 batch(Kling/Veo/Higgsfield)가 'running' 에 영구 stuck 되는 사고가 반복됨.
    # GENAI_INTERNAL_TOKEN 미설정 / genai 컨테이너 다운 시엔 아래에서 SkipReason 으로
    # graceful skip 하므로 RUNNING 기본이어도 무해. 운영자가 명시적으로 끄면 유지됨.
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "비동기 GenAI jobs (Kling/Veo/Higgsfield) 의 외부 API 상태 확인 + stale batch "
        "status reconcile. docker/genai 컨테이너의 /internal/* 를 HTTP 경유로 호출."
    ),
)
def genai_poll_sensor(context):
    token = _internal_token()
    if not token:
        yield SkipReason("GENAI_INTERNAL_TOKEN 미설정 — sensor 비활성. .env.test 확인.")
        return

    headers = {"X-Internal-Token": token}

    # 0a) deferred(pending) job drain — Kling 동시성 한도로 미뤄둔 job 을 슬롯 빈
    # 만큼 제출 (1303 회피). pending 유무와 무관하게 매 tick 시도.
    drained = {}
    try:
        dr = requests.post(
            f"{_GENAI_INTERNAL_BASE}/internal/jobs/submit-pending",
            headers=headers,
            timeout=_GENAI_INTERNAL_TIMEOUT,
        )
        dr.raise_for_status()
        drained = dr.json()
    except Exception as exc:
        context.log.warning(f"submit-pending drain 실패: {exc}")

    # 0b) stale batch reconcile (안전망) — 모든 job 이 끝났는데 batch.status 가
    # running/pending 으로 남은 케이스 보정. 비용 거의 0 (단일 SQL recompute).
    reconciled = 0
    try:
        rr = requests.post(
            f"{_GENAI_INTERNAL_BASE}/internal/batches/reconcile-stale",
            headers=headers,
            timeout=_GENAI_INTERNAL_TIMEOUT,
        )
        rr.raise_for_status()
        reconciled = int(rr.json().get("reconciled", 0))
    except Exception as exc:
        context.log.warning(f"reconcile-stale 실패: {exc}")

    # 1) pending list 조회
    try:
        r = requests.get(
            f"{_GENAI_INTERNAL_BASE}/internal/jobs/pending",
            params={"limit": _POLL_BATCH},
            headers=headers,
            timeout=_GENAI_INTERNAL_TIMEOUT,
        )
        r.raise_for_status()
        pending = r.json().get("pending", [])
    except Exception as exc:
        yield SkipReason(f"genai 컨테이너 조회 실패 ({_GENAI_INTERNAL_BASE}): {exc}")
        return

    if not pending:
        d = ""
        if drained and (drained.get("submitted") or drained.get("deferred")):
            d = f" drained(submitted={drained.get('submitted', 0)} deferred={drained.get('deferred', 0)})"
        yield SkipReason(f"폴링 대상 GenAI jobs 없음 (reconciled={reconciled}{d})")
        return

    # 2) 각 job poll → finalize
    counts = {"done": 0, "failed": 0, "running": 0, "error": 0, "noop": 0}
    for job in pending:
        try:
            pr = requests.post(
                f"{_GENAI_INTERNAL_BASE}/internal/jobs/{job['job_id']}/poll",
                headers=headers,
                timeout=_GENAI_INTERNAL_TIMEOUT,
            )
            pr.raise_for_status()
            res_status = (pr.json().get("status") or "").lower()
            if res_status == "done":
                counts["done"] += 1
            elif res_status == "failed":
                counts["failed"] += 1
            elif res_status == "running":
                counts["running"] += 1
            elif res_status in ("noop", "no_provider_id"):
                counts["noop"] += 1
            else:
                counts["error"] += 1
        except Exception as exc:
            context.log.warning(f"poll 실패 job={job.get('job_id')} engine={job.get('engine')}: {exc}")
            counts["error"] += 1

    yield SkipReason(
        f"polled={len(pending)} done={counts['done']} failed={counts['failed']} "
        f"running={counts['running']} noop={counts['noop']} error={counts['error']} "
        f"reconciled={reconciled} "
        f"drained(submitted={drained.get('submitted', 0)} deferred={drained.get('deferred', 0)})"
    )
