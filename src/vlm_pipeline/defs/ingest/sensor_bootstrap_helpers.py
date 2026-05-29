"""auto_bootstrap sensor 공통 유틸 — dir 순회, dispatch 트리거, done marker, gcp 판정, chunking."""

from __future__ import annotations

import json
from pathlib import Path


def _iter_sorted_dir_entries(path: Path) -> list[Path]:
    try:
        entries = list(path.iterdir())
    except OSError:
        return []
    return sorted(entries, key=lambda row: row.name)


def _load_dispatch_requested_folders(dispatch_pending_dir: Path) -> set[str]:
    """auto_bootstrap 에서 제외할 dispatch-soup 폴더 이름 집합.

    pending/ JSON: 아직 dispatch_sensor 가 처리 안 한 요청 → dispatch_sensor 가 처리할 것.
    processed/ JSON: dispatch_sensor 가 manifest 생성한 요청 → ingest_job 이 처리 중 (또는 archive 완료).
      ingest_job 이 archive_finalize 까지 진행되는 동안 incoming/<folder>/ 는 그대로 존재하므로,
      이 윈도우 동안 auto_bootstrap_sensor 가 같은 folder 를 picking 하면 race condition.
      2026-05-20 appdata NVENC 검증에서 실측: dispatch upload→archive 사이 ~40s 윈도우에서 발생.
      checksum dedup 이 데이터 손상은 막지만 CPU/GPU 낭비 + raw_key suffix `_2` 부여 등 부작용.
    archive 완료 후엔 incoming/ 에서 folder 가 사라져 auto_bootstrap 의 discovery 가 자연 제외.
    즉 processed/ 의 누적 JSON 은 archive 가 끝난 folder 라 auto_bootstrap 노출 없음 — 무한 누적 무해.
    """
    requested_folders: set[str] = set()
    if not dispatch_pending_dir.exists():
        return requested_folders

    # pending + processed 둘 다 읽음. processed/ 는 형제 디렉토리.
    json_dirs = [dispatch_pending_dir]
    processed_dir = dispatch_pending_dir.parent / "processed"
    if processed_dir.exists():
        json_dirs.append(processed_dir)

    for json_dir in json_dirs:
        for request_path in sorted(json_dir.glob("*.json")):
            try:
                payload = json.loads(request_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            folder_name = str(payload.get("folder_name", "")).strip()
            if folder_name:
                requested_folders.add(folder_name)
    return requested_folders


def _has_allowed_direct_file(dir_path: Path, allowed_exts: set[str]) -> bool:
    for entry in _iter_sorted_dir_entries(dir_path):
        if entry.name.startswith("."):
            continue
        try:
            if not entry.is_file():
                continue
        except OSError:
            continue
        if entry.suffix.lower() in allowed_exts:
            return True
    return False


def _source_unit_sort_key(unit: dict) -> tuple[int, str]:
    unit_type = str(unit.get("unit_type", ""))
    scan_recursive = bool(unit.get("scan_recursive", True))
    if unit_type == "file":
        priority = 0
    elif not scan_recursive:
        priority = 1
    else:
        priority = 2
    return priority, str(unit.get("unit_key", ""))


def _has_done_marker(unit_path: str, marker_name: str) -> bool:
    return (Path(unit_path) / marker_name).exists()


def _is_gcp_unit_path(unit_path: str, incoming_dir: Path) -> bool:
    gcp_root = (incoming_dir / "gcp").resolve()
    try:
        Path(unit_path).resolve().relative_to(gcp_root)
        return True
    except Exception:
        return False


def _chunk_files(unit_files: list[dict], max_files_per_manifest: int) -> list[list[dict]]:
    if max_files_per_manifest <= 0:
        max_files_per_manifest = 1
    if not unit_files:
        return []
    return [
        unit_files[index : index + max_files_per_manifest]
        for index in range(0, len(unit_files), max_files_per_manifest)
    ]
