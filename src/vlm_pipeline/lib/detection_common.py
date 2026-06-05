"""Detection 모델(YOLO, SAM3 등) 공통 헬퍼.

YOLO/SAM3 양쪽에서 동일하게 복제되어 있던 로직을 하나로 통합한다:
- 클래스 정규화, 태그 파싱
- image_label_id 생성
- DB flush
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from hashlib import sha1


# ---------------------------------------------------------------------------
# 클래스 정규화 / 태그 파싱
# ---------------------------------------------------------------------------


def normalize_classes(values: Iterable[object] | None) -> list[str]:
    """중복 제거 + lowercase + strip. 순서 보존."""
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values or []:
        rendered = str(value or "").strip().lower()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return normalized


def parse_tag_list(raw_value: object) -> list[str]:
    """Dagster run tag 값(str, list, JSON array 문자열)을 클래스 리스트로 변환."""
    if isinstance(raw_value, list):
        return normalize_classes(raw_value)

    rendered = str(raw_value or "").strip()
    if not rendered:
        return []

    try:
        if rendered.startswith("["):
            parsed = json.loads(rendered)
            if isinstance(parsed, list):
                return normalize_classes(parsed)
    except Exception:
        pass

    return normalize_classes(rendered.split(","))


# ---------------------------------------------------------------------------
# image_label_id 생성
# ---------------------------------------------------------------------------


def stable_image_label_id(image_id: str, labels_key: str) -> str:
    """Deterministic label ID — 재실행 시 동일 행을 덮어쓸 수 있도록 한다."""
    return sha1(f"{image_id}|{labels_key}".encode()).hexdigest()


# ---------------------------------------------------------------------------
# DB flush
# ---------------------------------------------------------------------------


def flush_image_labels(db, rows: list[dict], context, *, tool_name: str = "detection") -> None:
    """image_labels 버퍼를 DB에 일괄 삽입한다."""
    try:
        count = db.batch_insert_image_labels(rows)
        context.log.debug(f"{tool_name} image_labels flush: {count}건")
    except Exception as exc:
        context.log.error(f"{tool_name} image_labels flush 실패: {exc}")
