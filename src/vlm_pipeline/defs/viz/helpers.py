"""fiftyone_sync_sensor 순수 헬퍼 — dagster 비의존, 단위 테스트 가능.

embed/helpers.py 관례 미러: sensor 결정 로직은 여기(순수 함수)에 두고, sensor.py 는
HTTP/dagster 배선만 담당한다.
"""

from __future__ import annotations

import json
from typing import Any


def decide_targets(
    prev: dict[str, Any] | None,
    cur: dict[str, Any],
    prompts_enabled: bool,
) -> list[str]:
    """DB 스냅샷 diff(prev vs cur) → analysis-sync 에 트리거할 target 목록.

    - prev=None(첫 tick, 커서 없음) → ``["frames"]`` 만. prompts(뱅크) 재빌드는 실제 변화
      이벤트가 있을 때만 트리거한다 — 배포 직후 커서 부재로 615k 문장 전체를 재빌드하는
      서프라이즈를 방지한다.
    - frame_n 또는 caption_n 변화 → "frames" 포함.
    - prompt_n / bank_n / bank_latest 변화 & prompts_enabled=True → "prompts" 포함.
    - 감소도 변화로 취급한다(재임베딩/뱅크 축소 등). analysis-sync 의 sync 는 set-diff
      기반 자기치유라 과다 트리거는 무해하다.
    """
    if prev is None:
        return ["frames"]

    targets: list[str] = []

    if cur.get("frame_n") != prev.get("frame_n") or cur.get("caption_n") != prev.get("caption_n"):
        targets.append("frames")

    if prompts_enabled and (
        cur.get("prompt_n") != prev.get("prompt_n")
        or cur.get("bank_n") != prev.get("bank_n")
        or cur.get("bank_latest") != prev.get("bank_latest")
    ):
        targets.append("prompts")

    return targets


def encode_cursor(snapshot: dict[str, Any]) -> str:
    """스냅샷 dict → JSON 문자열 (``sort_keys=True`` — 안정적 run_key 해시를 위해 고정)."""
    return json.dumps(dict(snapshot), sort_keys=True)


def decode_cursor(raw: str | None) -> dict[str, Any] | None:
    """JSON 문자열 → dict. 커서 부재/파싱 실패 시 ``None`` (빈 dict 와 구분 — 첫 tick 신호).

    ``None`` 은 "아직 커서가 없다"(첫 tick)를 의미하고, decide_targets 가 이를 보고
    prompts 트리거를 억제한다. 빈 dict ``{}`` 로 접었다면 그 구분이 사라진다.
    """
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None
