"""nas_health_sensor — NAS CIFS 마운트 응답 모니터링.

주기적으로 incoming/archive 경로에 stat을 시도하여 NAS 접근 가능 여부를 확인한다.
타임아웃 또는 에러 발생 시 경고 로그를 남기고, 연속 실패 시 Slack 알림(설정 시)을 보낸다.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from pathlib import Path

from dagster import DefaultSensorStatus, SkipReason, sensor

from vlm_pipeline.lib.env_utils import int_env
from vlm_pipeline.resources.config import PipelineConfig

NAS_PROBE_TIMEOUT_SEC: int = 5
NAS_HEALTH_CONSECUTIVE_FAIL_ALERT: int = 3

_consecutive_failures: int = 0
_last_alert_ts: float = 0.0
NAS_ALERT_COOLDOWN_SEC: int = 600


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


def _send_slack_alert(message: str) -> bool:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return False
    try:
        payload = json.dumps({"text": message}).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
        return True
    except Exception:  # noqa: BLE001
        return False


@sensor(
    minimum_interval_seconds=int_env("NAS_HEALTH_INTERVAL_SEC", 60, 30),
    default_status=DefaultSensorStatus.RUNNING,
    description="NAS CIFS 마운트 응답 모니터링 — 연속 실패 시 Slack 알림",
)
def nas_health_sensor(context):
    global _consecutive_failures, _last_alert_ts  # noqa: PLW0603

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
        if _consecutive_failures > 0:
            context.log.info(
                f"nas_health: NAS 복구 확인 (연속 실패 {_consecutive_failures}회 후 정상)"
            )
            if _consecutive_failures >= NAS_HEALTH_CONSECUTIVE_FAIL_ALERT:
                _send_slack_alert(
                    f"[NAS 복구] incoming/archive 접근 정상화 "
                    f"(연속 {_consecutive_failures}회 실패 후)"
                )
        _consecutive_failures = 0
        return SkipReason("nas_health: 정상")

    _consecutive_failures += 1
    failure_detail = ", ".join(
        f"{name}={detail}" for name, (ok, detail) in results.items() if not ok
    )
    context.log.warning(
        f"nas_health: NAS 접근 실패 ({_consecutive_failures}회 연속) — {failure_detail}"
    )

    now = time.time()
    if (
        _consecutive_failures >= NAS_HEALTH_CONSECUTIVE_FAIL_ALERT
        and (now - _last_alert_ts) > NAS_ALERT_COOLDOWN_SEC
    ):
        alert_sent = _send_slack_alert(
            f"[NAS 경고] 연속 {_consecutive_failures}회 접근 실패\n"
            f"상세: {failure_detail}\n"
            f"조치: NAS 서버 상태 확인 또는 `sudo umount -l && sudo mount -a` 필요"
        )
        if alert_sent:
            _last_alert_ts = now
            context.log.info("nas_health: Slack 알림 발송 완료")

    return SkipReason(f"nas_health: 실패 ({_consecutive_failures}회 연속) — {failure_detail}")
