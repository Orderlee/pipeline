"""Archive Dispatch Sensor (Phase 2b).

`.dispatch/pending/*.json` 중 `from_archived=true` 플래그가 있는 요청만 처리한다.
해당 요청은 이미 auto_bootstrap 단계에서 archive 로 이동 + DB 등록까지 끝난
폴더에 대해 MinIO 업로드 및 라벨링을 수행하라는 신호이다.

기존 `dispatch_sensor` 와 같은 폴더를 공유하되 `from_archived` 플래그로 분기:
- 기존 sensor: `from_archived` 가 없거나 `False` 인 요청 (incoming → archive + upload + label)
- 본 sensor: `from_archived=True` 요청 (archive → upload + label)

prod 배포 시 piaspace-agent 가 동일 schema (단지 flag 만 추가) 로 보낼 수 있어
운영 통합이 자연스럽다.
"""

from __future__ import annotations

import json
from pathlib import Path

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    sensor,
)

from vlm_pipeline.defs.dispatch.service import (
    move_dispatch_file,
    prepare_dispatch_request,
    resolve_archive_paths_from_folder,
    write_archive_dispatch_manifest,
)
from vlm_pipeline.lib.env_utils import is_duckdb_lock_conflict
from vlm_pipeline.resources.config import PipelineConfig


def build_archive_dispatch_sensor(*, jobs) -> SensorDefinition:
    """Builder — definitions.py 에서 upload_label_job 객체 주입."""
    return sensor(
        name="archive_dispatch_sensor",
        description=(
            "from_archived=true dispatch JSON 처리: archive 경로의 파일을 MinIO 로 "
            "업로드하고 라벨링 단계 (upload_label_job) 트리거"
        ),
        minimum_interval_seconds=30,
        default_status=DefaultSensorStatus.RUNNING,
        required_resource_keys={"db"},
        jobs=jobs,
    )(_archive_dispatch_sensor_fn)


def _archive_dispatch_sensor_fn(context: SensorEvaluationContext):
    config = PipelineConfig()
    incoming_dir = Path(config.incoming_dir)
    pending_dir = incoming_dir / ".dispatch" / "pending"
    processed_dir = incoming_dir / ".dispatch" / "processed"
    failed_dir = incoming_dir / ".dispatch" / "failed"

    if not pending_dir.exists():
        return

    db_resource = getattr(context.resources, "db", None)
    if db_resource is None:
        return

    try:
        db_resource.ensure_runtime_schema()
    except Exception as exc:
        if is_duckdb_lock_conflict(exc):
            context.log.warning(f"DuckDB lock 충돌 — 다음 tick 재시도: {exc}")
            return
        raise

    requests = sorted(fpath for fpath in pending_dir.glob("*.json") if fpath.is_file())

    for req_file in requests:
        try:
            req_data = json.loads(req_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            # 깨진 JSON 은 기존 dispatch_sensor 가 failed 로 옮길 것이므로 여기서는 skip.
            continue

        # from_archived 분기 — 본 sensor 의 책임 영역만
        if req_data.get("from_archived") is not True:
            continue

        try:
            prepared = prepare_dispatch_request(req_data, incoming_dir=incoming_dir)
        except Exception as exc:
            context.log.warning(
                f"archive_dispatch: invalid payload {req_file.name}: {exc}"
            )
            move_dispatch_file(req_file, failed_dir, context=context)
            continue

        try:
            archive_rows = resolve_archive_paths_from_folder(db_resource, prepared.folder_name)
        except Exception as exc:
            if is_duckdb_lock_conflict(exc):
                context.log.warning(
                    f"archive_dispatch: DB lock 충돌 — 다음 tick 에서 재시도 "
                    f"(request_id={prepared.request_id}, folder={prepared.folder_name}): {exc}"
                )
                # JSON 그대로 두고 다음 tick 에 재시도. failed 로 옮기지 않음.
                return
            context.log.error(
                f"archive_dispatch: DB lookup 실패 (request_id={prepared.request_id}, "
                f"folder={prepared.folder_name}): {exc}"
            )
            move_dispatch_file(req_file, failed_dir, context=context)
            continue
        if not archive_rows:
            context.log.warning(
                f"archive_dispatch: no archived rows for folder={prepared.folder_name} "
                f"(request_id={prepared.request_id})"
            )
            move_dispatch_file(req_file, failed_dir, context=context)
            continue

        try:
            manifest_path = write_archive_dispatch_manifest(
                config, prepared, archive_rows
            )
        except Exception as exc:
            context.log.warning(
                f"archive_dispatch: manifest write failed {req_file.name}: {exc}"
            )
            move_dispatch_file(req_file, failed_dir, context=context)
            continue

        run_key = f"archive-dispatch-{prepared.request_id}"
        tags = {
            "manifest_path": str(manifest_path),
            "from_archived": "true",
            "dispatch_request_id": prepared.request_id,
            "folder_name": prepared.folder_name,
            "folder_name_original": prepared.folder_name,
            "labeling_method": prepared.storage_labeling_method,
            "categories": prepared.storage_categories,
            "classes": prepared.storage_classes,
        }

        context.log.info(
            f"archive_dispatch: trigger upload_label_job — request_id={prepared.request_id} "
            f"folder={prepared.folder_name} files={len(archive_rows)}"
        )
        move_dispatch_file(req_file, processed_dir, context=context)

        yield RunRequest(run_key=run_key, tags=tags)
