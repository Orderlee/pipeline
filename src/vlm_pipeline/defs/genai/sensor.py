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


_POLL_INTERVAL = int(os.getenv("GENAI_POLL_INTERVAL_SECONDS", "30"))
_POLL_BATCH = int(os.getenv("GENAI_POLL_BATCH", "20"))
_GENAI_INTERNAL_BASE = os.getenv("GENAI_INTERNAL_BASE", "http://genai:8088").rstrip("/")
_GENAI_INTERNAL_TIMEOUT = int(os.getenv("GENAI_INTERNAL_TIMEOUT", "60"))


def _internal_token() -> str | None:
    return (os.getenv("GENAI_INTERNAL_TOKEN") or "").strip() or None


@sensor(
    name="genai_poll_sensor",
    minimum_interval_seconds=_POLL_INTERVAL,
    default_status=DefaultSensorStatus.STOPPED,   # 운영자가 Dagster UI 에서 켜야 함
    description=(
        "비동기 GenAI jobs (Kling/Higgsfield) 의 외부 API 상태 확인. "
        "docker/genai 컨테이너의 /internal/jobs/* 를 HTTP 경유로 호출."
    ),
)
def genai_poll_sensor(context):
    token = _internal_token()
    if not token:
        yield SkipReason("GENAI_INTERNAL_TOKEN 미설정 — sensor 비활성. .env.test 확인.")
        return

    headers = {"X-Internal-Token": token}

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
        yield SkipReason("폴링 대상 GenAI jobs 없음")
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
            context.log.warning(
                f"poll 실패 job={job.get('job_id')} engine={job.get('engine')}: {exc}"
            )
            counts["error"] += 1

    yield SkipReason(
        f"polled={len(pending)} done={counts['done']} failed={counts['failed']} "
        f"running={counts['running']} noop={counts['noop']} error={counts['error']}"
    )
