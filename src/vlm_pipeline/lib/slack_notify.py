"""Slack webhook 알림 — 공용 헬퍼.

운영 모니터링 sensor 들이 동일한 webhook 호출 로직을 가지고 있어 한 곳으로 통합.
SLACK_WEBHOOK_URL 미설정 또는 호출 실패 시 silent return False (sensor 안정성 우선).

L2: 순수 stdlib (json + urllib.request) 만 의존. dagster/defs import 없음.
"""

from __future__ import annotations

import json
import os
import urllib.request


def send_slack_alert(message: str, *, timeout: int = 10) -> bool:
    """Slack incoming webhook 으로 메시지 전송.

    Args:
        message: text 필드로 전송할 문자열.
        timeout: 호출 timeout (초). Default 10.

    Returns:
        True 면 전송 성공, False 면 webhook 미설정 또는 호출 실패.
        예외는 호출자에게 전파하지 않는다 (best-effort 알림).
    """
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
        with urllib.request.urlopen(req, timeout=timeout):
            pass
        return True
    except Exception:  # noqa: BLE001
        return False
