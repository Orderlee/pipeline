"""incoming → archive_pending 자동 이동 sensor.

incoming 디렉토리에 새 폴더가 감지되면 archive_pending으로 이동한다.
dispatch_sensor와 분리하여 타임아웃 문제를 방지한다.
"""

import os
import shutil
from pathlib import Path

from dagster import SensorEvaluationContext, SkipReason, sensor

from vlm_pipeline.lib.env_utils import IS_STAGING
from vlm_pipeline.resources.config import PipelineConfig


# 무시할 디렉토리/파일 패턴
_IGNORE_NAMES = {".dispatch", ".manifests", ".DS_Store", ".Trash", "__MACOSX"}


@sensor(
    name="incoming_to_pending_sensor",
    description="incoming에 새 폴더 감지 → archive_pending으로 자동 이동",
    minimum_interval_seconds=15,
)
def incoming_to_pending_sensor(context: SensorEvaluationContext):
    """incoming 디렉토리를 폴링하여 새 폴더를 archive_pending으로 이동."""
    if not IS_STAGING:
        return

    config = PipelineConfig()
    incoming_dir = Path(config.incoming_dir)
    archive_pending_dir = Path(config.archive_pending_dir)

    if not incoming_dir.exists():
        yield SkipReason("incoming 디렉토리 없음")
        return

    archive_pending_dir.mkdir(parents=True, exist_ok=True)

    moved_count = 0
    for entry in sorted(incoming_dir.iterdir()):
        # 숨김 파일/폴더, 시스템 폴더 무시
        if entry.name.startswith(".") or entry.name in _IGNORE_NAMES:
            continue
        # 폴더만 처리
        if not entry.is_dir():
            continue

        dest = archive_pending_dir / entry.name
        if dest.exists():
            context.log.info(f"archive_pending에 이미 존재, 스킵: {entry.name}")
            continue

        try:
            # 같은 파일시스템이면 os.rename (즉시), 아니면 shutil.move
            try:
                os.rename(str(entry), str(dest))
            except OSError:
                shutil.move(str(entry), str(dest))
            moved_count += 1
            context.log.info(f"incoming → archive_pending 이동: {entry.name}")
        except Exception as e:
            context.log.warning(f"이동 실패: {entry.name} → {e}")

    if moved_count == 0:
        yield SkipReason("이동할 새 폴더 없음")
    else:
        context.log.info(f"총 {moved_count}개 폴더 archive_pending으로 이동 완료")
