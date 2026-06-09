"""Dagster sensor cursor JSON 직렬화 / 역직렬화 헬퍼.

여러 sensor (process / build / ingest / label / detection 등) 가 동일한 패턴으로
``context.update_cursor(json.dumps(dict_state, sort_keys=True))`` 을 호출하고
``context.cursor`` 를 dict 로 다시 읽어들인다 — 이를 한 곳에 통합.

L4: defs/shared. dagster 의존 (SensorEvaluationContext type hint) — TYPE_CHECKING 가드 사용.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dagster import SensorEvaluationContext


def read_dict_cursor(context: SensorEvaluationContext) -> dict[str, Any]:
    """``context.cursor`` 를 dict 로 역직렬화. 빈 cursor / 파싱 실패 → 빈 dict.

    sensor 안에서 ``cursor = json.loads(context.cursor) if context.cursor else {}`` 패턴 대체.
    """
    raw = context.cursor
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def write_dict_cursor(context: SensorEvaluationContext, cursor: Mapping[str, Any]) -> None:
    """dict cursor 를 JSON 직렬화하여 ``context.update_cursor`` 호출.

    ``json.dumps(cursor, sort_keys=True)`` 와 byte-identical (sensor 안정성 위해 sort_keys 고정).
    """
    context.update_cursor(json.dumps(dict(cursor), sort_keys=True))
