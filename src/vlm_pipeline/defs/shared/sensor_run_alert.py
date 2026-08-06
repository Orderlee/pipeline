"""Run 실패 Slack 알림 — job 필터 없는 전역 센서.

`defs/dispatch/sensor_run_status.py` 의 dispatch_run_failure_sensor 는
`_resolve_dispatch_request_id()` 가 dispatch 태그 없는 run 을 전부 None 으로 걸러내서
dispatch_stage_job / archive-only ingest_job 만 처리한다. 그래서 SAM3 detection,
embedding, clip, build_dataset, GCS download 실패는 DB 에도 Slack 에도 안 남고
Dagster UI 를 직접 봐야만 알 수 있었다. 이 센서는 그 공백만 메운다 — 필터 없이
FAILURE 를 받아 Slack 으로 알린다 (DB 상태 변경은 하지 않는다).

CANCELED 는 대상 아님: 운영자 수동 취소와 stuck_run_guard 자동 취소가 섞여 있어
알림 가치보다 소음이 크다.

실측 근거 (2026-07-29, prod runs.db): 최근 90일 FAILURE 18건 ≈ 0.2건/일.
쿨다운·중복억제 없이 그대로 보내도 소음이 되지 않는 빈도라 throttle 을 넣지 않았다.
빈도가 올라가면 sensor_nas_health 의 NAS_ALERT_COOLDOWN_SEC 패턴을 그대로 가져오면 된다.
"""

from __future__ import annotations

import os

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunStatusSensorContext,
    run_status_sensor,
)

from vlm_pipeline.lib.slack_notify import send_slack_alert

_ERROR_MAX_CHARS = 500


def build_failure_message(
    *,
    job_name: str,
    run_id: str,
    error: str | None,
    base_url: str,
) -> str:
    """Slack 에 보낼 실패 알림 본문. 순수 함수 — 테스트 대상."""
    lines = [
        f"[Run 실패] {job_name}",
        f"run: {base_url.rstrip('/')}/runs/{run_id}",
    ]
    detail = (error or "").strip()
    if detail:
        if len(detail) > _ERROR_MAX_CHARS:
            detail = detail[:_ERROR_MAX_CHARS] + " …(생략)"
        lines.append(f"```{detail}```")
    return "\n".join(lines)


def _dagster_ui_base_url() -> str:
    # prod Dagster UI. staging(:3031) 이나 IP 변경 시 env 로 덮어쓴다.
    return os.getenv("DAGSTER_UI_BASE_URL", "http://10.0.0.10:3030")


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    default_status=DefaultSensorStatus.RUNNING,
    monitor_all_code_locations=False,
)
def run_failure_alert_sensor(context: RunStatusSensorContext) -> None:
    run = context.dagster_run
    # failure_event 는 FAILURE 센서에서 채워지지만, Dagster 버전/경로에 따라
    # 비어 있을 수 있어 방어적으로 읽는다 (알림 누락 < 센서 크래시).
    event = getattr(context, "failure_event", None)
    error = getattr(event, "message", None) if event is not None else None

    message = build_failure_message(
        job_name=run.job_name,
        run_id=run.run_id,
        error=error,
        base_url=_dagster_ui_base_url(),
    )
    if send_slack_alert(message):
        context.log.info(f"run_failure_alert: Slack 발송 job={run.job_name} run_id={run.run_id}")
    else:
        # webhook 미설정이거나 호출 실패 — 알림은 best-effort 라 run 처리에 영향 주지 않는다.
        context.log.warning(
            f"run_failure_alert: Slack 발송 실패(또는 SLACK_WEBHOOK_URL 미설정) "
            f"job={run.job_name} run_id={run.run_id}"
        )
