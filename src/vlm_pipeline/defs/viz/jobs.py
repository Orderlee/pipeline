"""fiftyone_sync_job — analysis-sync HTTP API 순차 호출 (frames/labels/prompts).

analysis-sync 계약(부모 조정 — dagster/analysis 양쪽 구현자가 공유):
  POST /sync/{target} -> 202 {"job_id": ...} | 409 {"error": "busy", "current": {...}}
  GET  /status        -> {"busy": bool, "current": {...}|null, "last": {job_id,target,state,...}|null}
  last.result 는 {"target","dry_run","added","refreshed","remaining","warnings"}.

op 은 target 순서대로 POST 후 15s 간격으로 /status 를 폴링하고, done 인데 remaining>0 이면
(analysis-sync 가 1회 호출로 전량을 못 끝낸 경우) 같은 target 을 재-POST 한다. failed 는
RuntimeError(tail 포함)로 fail-fast. FIFTYONE_SYNC_API_URL 미설정을 조용한 성공으로 삼키지
않는다 — 이 repo 의 "부재에 기댄 안전" 반복 버그 형태를 피하기 위해서다.
"""

from __future__ import annotations

import os
import time
from typing import Any

import requests
from dagster import DefaultScheduleStatus, Field, ScheduleDefinition, job, op

from vlm_pipeline.lib.env_utils import int_env

_POLL_INTERVAL_SECONDS = 15
_BUSY_RETRY_SECONDS = 30
_REQUEST_TIMEOUT_SECONDS = 15


def _sync_base_url() -> str:
    return (os.getenv("FIFTYONE_SYNC_API_URL", "") or "").strip().rstrip("/")


def _auth_headers() -> dict[str, str]:
    token = (os.getenv("FIFTYONE_SYNC_TOKEN", "") or "").strip()
    return {"X-Internal-Token": token} if token else {}


def _post_sync_start(
    session: requests.Session,
    base_url: str,
    target: str,
    headers: dict[str, str],
    deadline: float,
) -> str:
    """POST /sync/{target} → job_id. 409(busy) 는 시한 내에서 30s 대기 후 재시도."""
    while True:
        if time.monotonic() > deadline:
            raise RuntimeError(f"fiftyone sync timeout waiting to start target={target!r}")
        response = session.post(
            f"{base_url}/sync/{target}",
            json={},
            headers=headers,
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code == 202:
            return str(response.json()["job_id"])
        if response.status_code == 409:
            time.sleep(_BUSY_RETRY_SECONDS)
            continue
        response.raise_for_status()
        raise RuntimeError(f"unexpected /sync/{target} response status={response.status_code}")


def _poll_until_terminal(
    session: requests.Session,
    base_url: str,
    job_id: str,
    headers: dict[str, str],
    deadline: float,
) -> dict[str, Any]:
    """GET /status?job_id= 를 해당 job 이 terminal state 가 될 때까지 폴링.

    ``last`` 가 아니라 job_id 조회(``job``)를 본다 — A 완료 직후 다른 잡 B 가 시작되면
    ``last`` 는 B 로 덮여 A 의 종결이 영영 안 보이는 경합이 있다(analysis-sync 는 이력을
    보관하고 ?job_id= 로 돌려준다).
    """
    while True:
        if time.monotonic() > deadline:
            raise RuntimeError(f"fiftyone sync timeout polling job_id={job_id!r}")
        response = session.get(
            f"{base_url}/status",
            params={"job_id": job_id},
            headers=headers,
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        record = payload.get("job") or {}
        if record.get("job_id") == job_id:
            state = record.get("state")
            if state == "done":
                return dict(record.get("result") or {})
            if state == "failed":
                raise RuntimeError(f"fiftyone sync failed job_id={job_id!r} tail={record.get('tail')!r}")
        time.sleep(_POLL_INTERVAL_SECONDS)


def _drive_target_sync(
    session: requests.Session,
    base_url: str,
    target: str,
    headers: dict[str, str],
    deadline: float,
    log,
) -> dict[str, Any]:
    """target 하나를 완료(remaining<=0)까지 몰아붙인다 — 필요시 재-POST.

    무진전 가드: 직전 호출이 added/refreshed 0 인데 remaining 이 그대로면(예: 메모리 하한이
    계속 바닥) 재-POST 는 같은 실패를 반복할 뿐이다 — 조용히 돌지 말고 크게 실패한다.
    """
    prev_remaining: int | None = None
    while True:
        job_id = _post_sync_start(session, base_url, target, headers, deadline)
        result = _poll_until_terminal(session, base_url, job_id, headers, deadline)
        log.info(f"fiftyone sync target={target} job_id={job_id} result={result}")
        remaining = int(result.get("remaining") or 0)
        if remaining <= 0:
            return result
        progressed = int(result.get("added") or 0) + int(result.get("refreshed") or 0)
        if progressed == 0 and prev_remaining is not None and remaining >= prev_remaining:
            raise RuntimeError(
                f"fiftyone sync target={target}: 무진전 (remaining={remaining}, "
                f"warnings={result.get('warnings')!r}) — 재-POST 중단"
            )
        prev_remaining = remaining
        log.info(f"fiftyone sync target={target}: remaining={remaining} — 재-POST")


@op(
    name="trigger_fiftyone_sync",
    description="analysis-sync HTTP API 순차 호출 — target 별 202 폴링, done+remaining>0 이면 재-POST.",
    config_schema={"targets": Field([str], default_value=[])},
)
def trigger_fiftyone_sync(context) -> dict[str, Any]:
    base_url = _sync_base_url()
    if not base_url:
        raise RuntimeError("FIFTYONE_SYNC_API_URL 미설정 — fiftyone sync 트리거 불가.")

    targets: list[str] = list(context.op_config.get("targets") or [])
    if not targets:
        context.log.warning("trigger_fiftyone_sync: targets 비어있음 — no-op")
        return {"results": {}}

    # deadline 은 run 전체 예산(모든 target 공유)이다 — 앞 target 이 오래 걸리면 뒤 target 이
    # 그 run 에서 타임아웃할 수 있고, 그 실패는 다음 sensor 발화/스케줄에서 재시도된다(의도).
    timeout_seconds = int_env("FIFTYONE_SYNC_TIMEOUT_SECONDS", 7200)
    deadline = time.monotonic() + timeout_seconds
    headers = _auth_headers()

    results: dict[str, dict[str, Any]] = {}
    with requests.Session() as session:
        for target in targets:
            results[target] = _drive_target_sync(session, base_url, target, headers, deadline, context.log)

    return {"results": results}


@job(
    name="fiftyone_sync_job",
    description="analysis-sync HTTP 트리거 — fiftyone_sync_sensor(frames/prompts) 및 label 갱신 스케줄이 공유.",
)
def fiftyone_sync_job():
    trigger_fiftyone_sync()


# frames 를 labels 앞에 두는 이유: (1) MinIO 장애 등으로 sensor 발화 시점에 add 를 못 한
# 프레임을 매일 캐치업(PG 카운트가 안 변하면 sensor 는 재발화하지 않는다), (2) 그렇게
# 추가된 신규 프레임까지 이어지는 labels 재적재가 커버한다.
fiftyone_label_refresh_schedule = ScheduleDefinition(
    name="fiftyone_label_refresh_schedule",
    job=fiftyone_sync_job,
    cron_schedule="0 3 * * *",
    execution_timezone="Asia/Seoul",
    default_status=DefaultScheduleStatus.RUNNING,
    run_config={"ops": {"trigger_fiftyone_sync": {"config": {"targets": ["frames", "labels"]}}}},
)
