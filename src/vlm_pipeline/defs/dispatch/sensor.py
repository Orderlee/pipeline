"""STAGING-only dispatch sensor."""

import json
import shutil
from datetime import datetime
from pathlib import Path
from uuid import uuid4
import os

from dagster import RunRequest, SensorEvaluationContext, sensor

from vlm_pipeline.lib.env_utils import IS_STAGING, VALID_OUTPUTS, resolve_outputs
from vlm_pipeline.lib.sanitizer import sanitize_path_component
from vlm_pipeline.lib.validator import ALLOWED_EXTENSIONS
from vlm_pipeline.resources.config import PipelineConfig

# run_mode → outputs 변환 (하위 호환)
_RUN_MODE_TO_OUTPUTS = {
    "gemini": ["timestamp", "captioning"],
    "yolo": ["bbox"],
    "both": ["timestamp", "captioning", "bbox"],
}

# output → 파이프라인 단계 매핑
_OUTPUT_TO_STEPS = {
    "bbox": [
        ("archive_move", 1),
        ("frame_extract", 2),
        ("yolo_detect", 3),
    ],
    "timestamp": [
        ("archive_move", 1),
        ("gemini_timestamp", 2),
    ],
    "captioning": [
        ("archive_move", 1),
        ("gemini_timestamp", 2),
        ("gemini_caption", 3),
        ("frame_extract", 4),
    ],
}


def _create_dispatch_table_if_not_exists(db_conn) -> None:
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS staging_dispatch_requests (
            request_id           VARCHAR PRIMARY KEY,
            folder_name          VARCHAR,
            run_mode             VARCHAR,
            outputs              VARCHAR,
            image_profile        VARCHAR,
            status               VARCHAR DEFAULT 'pending',
            archive_pending_path VARCHAR,
            archive_path         VARCHAR,
            max_frames_per_video INTEGER,
            jpeg_quality         INTEGER,
            confidence_threshold DOUBLE,
            iou_threshold        DOUBLE,
            requested_by         VARCHAR,
            requested_at         TIMESTAMP,
            processed_at         TIMESTAMP,
            completed_at         TIMESTAMP,
            error_message        TEXT,
            created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS staging_pipeline_runs (
            run_id               VARCHAR PRIMARY KEY,
            request_id           VARCHAR,
            folder_name          VARCHAR,
            step_name            VARCHAR NOT NULL,
            step_order           INTEGER DEFAULT 0,
            step_status          VARCHAR DEFAULT 'pending',
            model_name           VARCHAR,
            model_version        VARCHAR,
            applied_params       VARCHAR,
            input_count          INTEGER DEFAULT 0,
            output_count         INTEGER DEFAULT 0,
            error_count          INTEGER DEFAULT 0,
            started_at           TIMESTAMP,
            completed_at         TIMESTAMP,
            error_message        TEXT,
            created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

@sensor(
    name="dispatch_sensor",
    description="Staging-only dispatch JSON 처리 및 archive 이동 후 파이프라인 트리거",
    minimum_interval_seconds=30,
    required_resource_keys={"db"},
    job_name="dispatch_stage_job",
)
def dispatch_sensor(context: SensorEvaluationContext):
    if not IS_STAGING:
        return

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

    db_resource.ensure_schema()
    
    with db_resource.connect() as conn:
        _create_dispatch_table_if_not_exists(conn)

    requests = []
    for fpath in pending_dir.glob("*.json"):
        if fpath.is_file():
            requests.append(fpath)

    requests.sort()  # Time based or simple alphabetic sorting

    for req_file in requests:
        try:
            req_data = json.loads(req_file.read_text())
        except json.JSONDecodeError:
            _move_dispatch_file(req_file, failed_dir)
            continue

        request_id = req_data.get("request_id")
        folder_name = req_data.get("folder_name")

        # outputs 필드 우선, run_mode 하위 호환
        outputs_list = req_data.get("outputs")
        run_mode = req_data.get("run_mode")

        if isinstance(outputs_list, list) and outputs_list:
            # JSON 배열 → 유효한 것만 필터
            valid_outputs = [o.strip().lower() for o in outputs_list if str(o).strip().lower() in VALID_OUTPUTS]
            if not valid_outputs:
                _record_failed_request(db_resource, request_id or str(req_file.stem), req_data, "invalid_outputs")
                _move_dispatch_file(req_file, failed_dir)
                continue
            outputs_str = ",".join(valid_outputs)
        elif run_mode:
            # 기존 run_mode → outputs 변환
            valid_outputs = _RUN_MODE_TO_OUTPUTS.get(run_mode, [])
            if not valid_outputs:
                _record_failed_request(db_resource, request_id or str(req_file.stem), req_data, f"invalid_run_mode:{run_mode}")
                _move_dispatch_file(req_file, failed_dir)
                continue
            outputs_str = ",".join(valid_outputs)
        else:
            _record_failed_request(db_resource, request_id or str(req_file.stem), req_data, "missing_outputs_or_run_mode")
            _move_dispatch_file(req_file, failed_dir)
            continue

        image_profile = req_data.get("image_profile", "current")
        requested_by = req_data.get("requested_by")
        requested_at_str = req_data.get("requested_at")
        
        if not request_id or not folder_name:
            _record_failed_request(db_resource, request_id or str(req_file.stem), req_data, "missing_request_id_or_folder")
            _move_dispatch_file(req_file, failed_dir)
            continue

        # archive_pending에서 폴더 확인 (incoming_to_pending_sensor가 이미 이동함)
        archive_pending_dir = Path(config.archive_pending_dir)
        archive_pending_folder_path = archive_pending_dir / folder_name
        if not archive_pending_folder_path.is_dir():
            _record_failed_request(db_resource, request_id, req_data, "folder_not_in_archive_pending")
            _move_dispatch_file(req_file, failed_dir)
            continue
        
        # Check DB to prevent dups
        with db_resource.connect() as conn:
            existing = conn.execute("SELECT status FROM staging_dispatch_requests WHERE folder_name = ? AND status IN ('running', 'archive_moved')", [folder_name]).fetchone()
            if existing:
                _record_failed_request(db_resource, request_id, req_data, "folder_dispatch_in_flight")
                _move_dispatch_file(req_file, failed_dir)
                continue

            existing_req = conn.execute("SELECT status FROM staging_dispatch_requests WHERE request_id = ?", [request_id]).fetchone()
            if existing_req:
                _record_failed_request(db_resource, request_id, req_data, "duplicate_request_id")
                _move_dispatch_file(req_file, failed_dir)
                continue

        # archive_pending에서 파일 스캔 (이미 incoming_to_pending_sensor가 이동 완료)
        allowed_exts = {ext.lower() for ext in ALLOWED_EXTENSIONS}
        files = []
        for root, dirs, names in os.walk(archive_pending_folder_path):
            root_path = Path(root)
            for name in names:
                path_obj = root_path / name
                if path_obj.suffix.lower() not in allowed_exts:
                    continue
                try:
                    stat = path_obj.stat()
                except OSError:
                    continue
                try:
                    rel_path = str(path_obj.relative_to(archive_pending_folder_path))
                except Exception:
                    rel_path = path_obj.name
                    
                files.append({
                    "path": str(path_obj),
                    "size": int(stat.st_size),
                    "rel_path": rel_path
                })
                
        if not files:
            _record_failed_request(db_resource, request_id, req_data, "no_media_files_in_archive_pending")
            _move_dispatch_file(req_file, failed_dir)
            continue

        now = datetime.now()
        manifest_id = f"dispatch_{request_id}_{now:%Y%m%d_%H%M%S}"
        manifest_filename = f"{manifest_id}.json"
        
        manifest = {
            "manifest_id": manifest_id,
            "generated_at": now.isoformat(),
            "source_dir": str(archive_pending_dir),
            "source_unit_type": "directory",
            "source_unit_path": str(archive_pending_folder_path),
            "source_unit_name": folder_name,
            "source_unit_total_file_count": len(files),
            "file_count": len(files),
            "transfer_tool": "dispatch_sensor",
            "files": files,
        }
        
        manifest_dir = Path(config.manifest_dir) / "dispatch"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = manifest_dir / manifest_filename
        
        try:
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            _record_failed_request(db_resource, request_id, req_data, f"failed_to_write_manifest:{e}")
            _move_dispatch_file(req_file, failed_dir)
            continue

        archive_dest = Path(config.archive_dir) / folder_name

        # ── model_configs에서 기본 파라미터 조회 ──
        model_defaults = {}
        with db_resource.connect() as conn:
            for output_key in valid_outputs:
                row = conn.execute(
                    "SELECT model_name, model_version, default_max_frames, "
                    "default_jpeg_quality, default_confidence, default_iou "
                    "FROM staging_model_configs WHERE output_type = ? AND is_active = TRUE",
                    [output_key],
                ).fetchone()
                if row:
                    model_defaults[output_key] = {
                        "model_name": row[0],
                        "model_version": row[1],
                        "default_max_frames": row[2],
                        "default_jpeg_quality": row[3],
                        "default_confidence": row[4],
                        "default_iou": row[5],
                    }

        # ── 실제 적용 파라미터 결정: trigger JSON 값 우선, 없으면 model_configs 기본값 ──
        bbox_defaults = model_defaults.get("bbox", {})
        applied_max_frames = (
            req_data.get("max_frames_per_video")
            or bbox_defaults.get("default_max_frames")
        )
        applied_jpeg_quality = (
            req_data.get("jpeg_quality")
            or bbox_defaults.get("default_jpeg_quality")
        )
        applied_confidence = (
            req_data.get("confidence_threshold")
            or bbox_defaults.get("default_confidence")
        )
        applied_iou = (
            req_data.get("iou_threshold")
            or bbox_defaults.get("default_iou")
        )

        # Record success
        now = datetime.now()
        with db_resource.connect() as conn:
            conn.execute(
                """
                INSERT INTO staging_dispatch_requests (
                    request_id, folder_name, run_mode, outputs, image_profile,
                    status, archive_pending_path, archive_path,
                    max_frames_per_video, jpeg_quality, confidence_threshold, iou_threshold,
                    requested_by, requested_at, processed_at
                ) VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    request_id, folder_name, run_mode, outputs_str, image_profile,
                    str(archive_pending_folder_path), str(archive_dest),
                    int(applied_max_frames) if applied_max_frames is not None else None,
                    int(applied_jpeg_quality) if applied_jpeg_quality is not None else None,
                    float(applied_confidence) if applied_confidence is not None else None,
                    float(applied_iou) if applied_iou is not None else None,
                    requested_by, requested_at_str, now,
                ]
            )

            # ── pipeline_runs에 단계별 레코드 생성 ──
            seen_steps = set()
            for output_key in valid_outputs:
                steps = _OUTPUT_TO_STEPS.get(output_key, [])
                md = model_defaults.get(output_key, {})
                for step_name, step_order in steps:
                    if step_name in seen_steps:
                        continue
                    seen_steps.add(step_name)
                    conn.execute(
                        """
                        INSERT INTO staging_pipeline_runs (
                            run_id, request_id, folder_name,
                            step_name, step_order, step_status,
                            model_name, model_version, applied_params
                        ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                        """,
                        [
                            str(uuid4()), request_id, folder_name,
                            step_name, step_order,
                            md.get("model_name"),
                            md.get("model_version"),
                            json.dumps({
                                "max_frames": applied_max_frames,
                                "jpeg_quality": applied_jpeg_quality,
                                "confidence": applied_confidence,
                                "iou": applied_iou,
                            }, default=str) if step_name in ("frame_extract", "yolo_detect") else None,
                        ]
                    )

        _move_dispatch_file(req_file, processed_dir)

        # RunRequest tags 구성 — 이미지 추출 파라미터는 값이 있을 때만 전달
        # folder_name은 sanitize된 버전 사용 (raw_key가 sanitize_path_component로 정규화되므로)
        sanitized_folder = sanitize_path_component(folder_name)
        run_tags = {
            "folder_name": sanitized_folder,
            "folder_name_original": folder_name,
            "outputs": outputs_str,
            "run_mode": run_mode or "",
            "image_profile": image_profile,
            "manifest_path": str(manifest_path),
        }
        # 선택적 이미지 추출 파라미터 (없으면 asset의 config 기본값 사용)
        if applied_max_frames is not None:
            run_tags["max_frames_per_video"] = str(int(applied_max_frames))
        if applied_jpeg_quality is not None:
            run_tags["jpeg_quality"] = str(int(applied_jpeg_quality))
        if applied_confidence is not None:
            run_tags["confidence_threshold"] = str(float(applied_confidence))
        if applied_iou is not None:
            run_tags["iou_threshold"] = str(float(applied_iou))
        
        yield RunRequest(
            run_key=request_id,
            job_name="dispatch_stage_job",
            tags=run_tags,
        )

def _move_dispatch_file(file_path: Path, target_dir: Path):
    target_dir.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(file_path), str(target_dir / file_path.name))
    except Exception:
        pass

def _record_failed_request(db, request_id: str, data: dict, error_msg: str):
    try:
        outputs_list = data.get("outputs")
        outputs_str = ",".join(outputs_list) if isinstance(outputs_list, list) else ""
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO staging_dispatch_requests (
                    request_id, folder_name, run_mode, outputs, image_profile,
                    max_frames_per_video, jpeg_quality,
                    confidence_threshold, iou_threshold,
                    status, error_message, processed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'failed', ?, ?)
                ON CONFLICT (request_id) DO UPDATE SET 
                    status = 'failed', error_message = excluded.error_message
                """,
                [
                    request_id, data.get("folder_name"), data.get("run_mode"),
                    outputs_str, data.get("image_profile"),
                    data.get("max_frames_per_video"),
                    data.get("jpeg_quality"),
                    data.get("confidence_threshold"),
                    data.get("iou_threshold"),
                    error_msg, datetime.now()
                ]
            )
    except Exception:
        pass
