"""Video classification helpers — prompt build, response parsing, dispatch candidate resolution."""

from __future__ import annotations

import json
from hashlib import sha1

from vlm_pipeline.lib.gemini import load_clean_json


def stable_video_classification_label_id(asset_id: str, label_key: str, predicted_class: str) -> str:
    token = "|".join([str(asset_id), str(label_key), str(predicted_class or "").strip().lower()])
    return sha1(token.encode("utf-8")).hexdigest()


def build_video_classification_prompt(candidate_classes: list[str]) -> str:
    rendered_candidates = ", ".join(str(value).strip().lower() for value in candidate_classes if str(value).strip())
    return (
        "You are classifying a CCTV video into exactly one class.\n\n"
        f"Allowed classes: [{rendered_candidates}]\n\n"
        "Task:\n"
        "Choose the single best matching class based only on visible evidence in the video.\n\n"
        "Return JSON only in this shape:\n"
        '{"predicted_class":"...", "rationale":"..."}\n\n'
        "Rules:\n"
        "- predicted_class must be exactly one value from Allowed classes.\n"
        "- rationale must be a short English explanation.\n"
        "- Do not include markdown fences or extra explanation."
    )


def parse_video_classification_response(payload_text: str, candidate_classes: list[str]) -> dict[str, str | None]:
    payload = load_clean_json(payload_text)
    if not isinstance(payload, dict):
        raise ValueError("video_classification_response_not_object")

    predicted_class = str(payload.get("predicted_class") or "").strip().lower()
    allowed = {str(value).strip().lower() for value in candidate_classes if str(value).strip()}
    if not predicted_class or predicted_class not in allowed:
        raise ValueError("video_classification_invalid_predicted_class")

    rationale = str(payload.get("rationale") or "").strip() or None
    return {
        "predicted_class": predicted_class,
        "rationale": rationale,
    }


def resolve_dispatch_video_class_candidates(tags) -> list[str]:
    raw_categories = str(tags.get("categories") or "").strip()
    if raw_categories:
        try:
            parsed = json.loads(raw_categories) if raw_categories.startswith("[") else raw_categories.split(",")
        except Exception:
            parsed = raw_categories.split(",")
        normalized = []
        seen = set()
        for value in parsed:
            rendered = str(value or "").strip().lower()
            if not rendered or rendered in seen:
                continue
            seen.add(rendered)
            normalized.append(rendered)
        if normalized:
            return normalized

    raw_classes = str(tags.get("classes") or "").strip()
    if raw_classes:
        try:
            parsed = json.loads(raw_classes) if raw_classes.startswith("[") else raw_classes.split(",")
        except Exception:
            parsed = raw_classes.split(",")
        normalized = []
        seen = set()
        for value in parsed:
            rendered = str(value or "").strip().lower()
            if not rendered or rendered in seen:
                continue
            seen.add(rendered)
            normalized.append(rendered)
        return normalized
    return []


def find_dispatch_video_classification_candidates(
    db,
    *,
    folder_name: str,
    limit: int,
) -> list[dict[str, object]]:
    """dispatch ``classification_video`` 대상 후보 조회 — backend 도메인 메서드 위임.

    PG 와 DuckDB 양쪽 backend 호환. 기존엔 raw ``conn.execute(...)`` 로 직접 호출했으나
    psycopg2 connection 에는 ``execute()`` 메서드가 없어 PG 환경에서 실패함.
    backend 별 placeholder/cursor 차이는 resources/{postgres,duckdb}_labeling.py 에서
    각각 처리한다.
    """
    return db.find_dispatch_video_classification_candidates(
        folder_name=folder_name,
        limit=limit,
    )
