"""nas_health_sensor — NAS CIFS 마운트 응답 모니터링.

주기적으로 incoming/archive 경로에 stat을 시도하여 NAS 접근 가능 여부를 확인한다.
타임아웃 또는 에러 발생 시 경고 로그를 남기고, 연속 실패 시 Slack 알림(설정 시)을 보낸다.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from pathlib import Path

from dagster import DefaultSensorStatus, SkipReason, sensor

from vlm_pipeline.defs.shared.sensor_cursor_utils import read_dict_cursor, write_dict_cursor
from vlm_pipeline.lib.env_utils import int_env
from vlm_pipeline.lib.slack_notify import send_slack_alert
from vlm_pipeline.resources.config import PipelineConfig

NAS_PROBE_TIMEOUT_SEC: int = 5
NAS_HEALTH_CONSECUTIVE_FAIL_ALERT: int = 3
NAS_ALERT_COOLDOWN_SEC: int = 600

# TEST-2: 실패-streak / 알림 쿨다운 상태는 sensor cursor(DB 영속)에 저장한다.
# 이전에는 모듈 전역(_consecutive_failures/_last_alert_ts)이라 dagster-daemon 재시작
# (= 매 prod 배포)마다 리셋 → 진행 중이던 실패 streak 이 소실되고 Slack 알림이 지연됐다.


def _probe_path(path: str, timeout_sec: int = NAS_PROBE_TIMEOUT_SEC) -> tuple[bool, str]:
    target = Path(path)

    def _stat():
        target.stat()
        return True

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_stat)
            future.result(timeout=timeout_sec)
        return True, "ok"
    except FuturesTimeoutError:
        return False, f"timeout ({timeout_sec}s)"
    except OSError as exc:
        return False, str(exc)


@sensor(
    minimum_interval_seconds=int_env("NAS_HEALTH_INTERVAL_SEC", 60, 30),
    default_status=DefaultSensorStatus.RUNNING,
    description="NAS CIFS 마운트 응답 모니터링 — 연속 실패 시 Slack 알림",
)
def nas_health_sensor(context):
    cursor = read_dict_cursor(context)
    try:
        consecutive_failures = int(cursor.get("consecutive_failures", 0))
    except (TypeError, ValueError):
        consecutive_failures = 0
    try:
        last_alert_ts = float(cursor.get("last_alert_ts", 0.0))
    except (TypeError, ValueError):
        last_alert_ts = 0.0

    config = PipelineConfig()
    paths_to_check = {
        "incoming": config.incoming_dir,
        "archive": config.archive_dir,
    }

    results: dict[str, tuple[bool, str]] = {}
    for name, path in paths_to_check.items():
        ok, detail = _probe_path(path)
        results[name] = (ok, detail)

    all_ok = all(ok for ok, _ in results.values())

    if all_ok:
        if consecutive_failures > 0:
            context.log.info(f"nas_health: NAS 복구 확인 (연속 실패 {consecutive_failures}회 후 정상)")
            if consecutive_failures >= NAS_HEALTH_CONSECUTIVE_FAIL_ALERT:
                send_slack_alert(f"[NAS 복구] incoming/archive 접근 정상화 (연속 {consecutive_failures}회 실패 후)")
        write_dict_cursor(context, {"consecutive_failures": 0, "last_alert_ts": last_alert_ts})
        return SkipReason("nas_health: 정상")

    consecutive_failures += 1
    failure_detail = ", ".join(f"{name}={detail}" for name, (ok, detail) in results.items() if not ok)
    context.log.warning(f"nas_health: NAS 접근 실패 ({consecutive_failures}회 연속) — {failure_detail}")

    now = time.time()
    if consecutive_failures >= NAS_HEALTH_CONSECUTIVE_FAIL_ALERT and (now - last_alert_ts) > NAS_ALERT_COOLDOWN_SEC:
        alert_sent = send_slack_alert(
            f"[NAS 경고] 연속 {consecutive_failures}회 접근 실패\n"
            f"상세: {failure_detail}\n"
            f"조치: NAS 서버 상태 확인 또는 `sudo umount -l && sudo mount -a` 필요"
        )
        if alert_sent:
            last_alert_ts = now
            context.log.info("nas_health: Slack 알림 발송 완료")

    write_dict_cursor(context, {"consecutive_failures": consecutive_failures, "last_alert_ts": last_alert_ts})
    return SkipReason(f"nas_health: 실패 ({consecutive_failures}회 연속) — {failure_detail}")
