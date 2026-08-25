"""fiftyone_sync_sensor — image_embeddings/prompt_banks 스냅샷 diff → analysis-sync HTTP 트리거.

실제 FiftyOne 증분 동기화는 analysis-sync 컨테이너(내부 포트 8010, 호스트 포트 없음)가 수행한다.
이 sensor 는 변화 감지 + RunRequest 생성만 담당 — 실행/폴링은 fiftyone_sync_job
(jobs.py 의 trigger_fiftyone_sync op)이 HTTP 로 analysis-sync 를 호출해 처리한다.

default_status=RUNNING 근거는 genai/sensor.py(:39-43) 의 주석과 동일한 논리다: URL 미설정 /
analysis-sync 도달 불가 / busy 상황 전부 아래에서 SkipReason 으로 graceful skip 하므로
RUNNING 기본이어도 무해하다. 반대로 STOPPED 를 기본값으로 두면 Dagster storage 초기화나
신규 배포마다 꺼져 있어 운영자가 수동으로 켜지 않는 한 FiftyOne 반영이 조용히 멈춘다
(dispatch_sensor/production_agent_dispatch_sensor 가 이 문제로 실제 사고를 낸 전례가 있다).

커서는 RunRequest 성공 여부와 무관하게 전진한다(의도된 트레이드오프): frames 는 set-diff
자기치유라 커서 유실이 무해하고, prompts 는 실패 시 다음 뱅크 변화 전까지 재시도되지 않지만
그때는 운영자가 fiftyone_sync_job 을 UI 에서 targets=["prompts"] 로 수동 발화하면 된다.
"""

from __future__ import annotations

import hashlib
import os

import requests
from dagster import DefaultSensorStatus, RunRequest, SkipReason, sensor

from vlm_pipeline.defs.viz.helpers import decide_targets, decode_cursor, encode_cursor
from vlm_pipeline.lib.env_utils import bool_env, int_env

_HEALTH_TIMEOUT_SECONDS = 5
_JOB_NAME = "fiftyone_sync_job"


def _sync_base_url() -> str:
    return (os.getenv("FIFTYONE_SYNC_API_URL", "") or "").strip().rstrip("/")


def _prompts_enabled() -> bool:
    return bool_env("FIFTYONE_SYNC_PROMPTS_ENABLED", True)


def _fetch_health(session: requests.Session, base_url: str) -> dict:
    response = session.get(f"{base_url}/health", timeout=_HEALTH_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


@sensor(
    name="fiftyone_sync_sensor",
    job_name=_JOB_NAME,
    minimum_interval_seconds=int_env("FIFTYONE_SYNC_POLL_INTERVAL_SECONDS", 300),
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"db"},
    description=(
        "image_embeddings(frame/caption/prompt) + prompt_banks 카운트 diff 로 변화 감지 → "
        "fiftyone_sync_job(analysis-sync HTTP 트리거, frames/prompts). labels 재적재와 frames "
        "일일 캐치업은 fiftyone_label_refresh_schedule(매일 03:00 KST) 담당."
    ),
)
def fiftyone_sync_sensor(context):
    base_url = _sync_base_url()
    if not base_url:
        yield SkipReason("FIFTYONE_SYNC_API_URL 미설정 — sensor 비활성.")
        return

    with requests.Session() as session:
        try:
            health = _fetch_health(session, base_url)
        except Exception as exc:  # noqa: BLE001 — analysis-sync 다운/네트워크 문제는 다음 tick 재시도
            yield SkipReason(f"analysis-sync 도달 불가 ({base_url}): {exc}")
            return

    if health.get("busy"):
        yield SkipReason("analysis-sync busy — 커서 전진 금지(다음 tick 재시도).")
        return

    snapshot = context.resources.db.fiftyone_sync_snapshot()
    prev = decode_cursor(context.cursor)
    targets = decide_targets(prev, snapshot, _prompts_enabled())
    new_cursor = encode_cursor(snapshot)

    if not targets:
        context.update_cursor(new_cursor)
        yield SkipReason("image_embeddings/prompt_banks 변화 없음 — sync 트리거 스킵.")
        return

    run_key = f"fo-sync-{hashlib.sha1(new_cursor.encode('utf-8')).hexdigest()[:12]}"
    yield RunRequest(
        run_key=run_key,
        run_config={"ops": {"trigger_fiftyone_sync": {"config": {"targets": targets}}}},
        tags={"fiftyone_sync_targets": ",".join(targets)},
    )
    context.update_cursor(new_cursor)
