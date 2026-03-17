"""INGEST 중복 처리 유틸리티.

assets.py에서 분리된 중복 파일 관련 헬퍼.
"""

from __future__ import annotations

from pathlib import Path


def error_code_from_message(error_message: str, default_code: str = "unknown_error") -> str:
    message = str(error_message or "").strip()
    if not message:
        return default_code
    lowered = message.lower()
    if "could not set lock on file" in lowered or "conflicting lock is held" in lowered:
        return "duckdb_lock_conflict"
    prefix = message.split(":", 1)[0].strip().lower()
    if not prefix:
        return default_code
    normalized = []
    for ch in prefix:
        if ch.isalnum() or ch in {"_", "-"}:
            normalized.append(ch)
        elif ch.isspace():
            normalized.append("_")
    return "".join(normalized).strip("_") or default_code


def collect_duplicate_asset_file_map(records: list[dict]) -> dict[str, list[str]]:
    """normalize 단계 duplicate_of 대상 asset_id별 중복 파일명(들)을 수집.

    같은 basename 이 여러 번 중복될 수 있으므로 set이 아니라 list로 보존한다.
    """
    target_files: dict[str, list[str]] = {}
    for rec in records:
        db_record = rec.get("record")
        if not isinstance(db_record, dict):
            continue
        error_message = str(db_record.get("error_message") or "")
        if not error_message.startswith("duplicate_of:"):
            continue
        duplicate_asset_id = error_message.replace("duplicate_of:", "", 1).strip()
        if duplicate_asset_id:
            file_name = str(db_record.get("original_name") or "").strip()
            if not file_name:
                file_name = Path(str(rec.get("path") or "")).name
            if not file_name:
                file_name = "unknown_file"
            target_files.setdefault(duplicate_asset_id, []).append(file_name)

    return {asset_id: file_names for asset_id, file_names in target_files.items() if file_names}
