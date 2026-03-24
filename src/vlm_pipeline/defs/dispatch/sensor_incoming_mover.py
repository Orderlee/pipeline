"""Dispatch JSON 존재 여부만 점검하는 센서.

dispatch_sensor가 incoming 폴더를 직접 읽도록 유지한다.
"""

import json
from pathlib import Path

from dagster import SensorEvaluationContext, SkipReason, sensor

from vlm_pipeline.resources.config import PipelineConfig


# 무시할 디렉토리/파일 패턴
_IGNORE_NAMES = {".dispatch", ".manifests", ".DS_Store", ".Trash", "__MACOSX"}


def _load_requested_folders(dispatch_pending_dir: Path) -> set[str]:
    requested_folders: set[str] = set()
    if not dispatch_pending_dir.exists():
        return requested_folders

    for request_path in sorted(dispatch_pending_dir.glob("*.json")):
        try:
            payload = json.loads(request_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        folder_name = str(payload.get("folder_name", "")).strip()
        if folder_name:
            requested_folders.add(folder_name)
    return requested_folders


@sensor(
    name="incoming_to_pending_sensor",
    description="dispatch JSON 존재 여부만 점검하고 dispatch_sensor가 incoming을 직접 처리",
    minimum_interval_seconds=15,
)
def incoming_to_pending_sensor(context: SensorEvaluationContext):
    """dispatch JSON은 dispatch_sensor가 incoming을 직접 읽도록 남겨둔다."""
    config = PipelineConfig()
    incoming_dir = Path(config.incoming_dir)
    dispatch_pending_dir = incoming_dir / ".dispatch" / "pending"

    if not incoming_dir.exists():
        yield SkipReason("incoming 디렉토리 없음")
        return

    requested_folders = _load_requested_folders(dispatch_pending_dir)

    if not requested_folders:
        yield SkipReason("대기 중인 dispatch request 없음")
        return

    available_folders = {
        entry.name
        for entry in sorted(incoming_dir.iterdir())
        if not entry.name.startswith(".")
        and entry.name not in _IGNORE_NAMES
        and entry.is_dir()
    }
    matched = sorted(requested_folders & available_folders)
    if not matched:
        yield SkipReason("dispatch request와 매칭되는 incoming 폴더 없음")
        return

    context.log.info(
        "dispatch JSON 감지: incoming 직접 처리 "
        f"({', '.join(matched)})"
    )
