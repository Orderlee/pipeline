"""Dispatch JSON 센서 — `.dispatch/pending` 처리 후 dispatch_stage_job 트리거."""

import json
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from dagster import RunRequest, SensorEvaluationContext, sensor

from vlm_pipeline.lib.sanitizer import sanitize_path_component
from vlm_pipeline.lib.staging_dispatch import parse_dispatch_request_payload
from vlm_pipeline.defs.ingest.archive import resolve_unique_directory
from vlm_pipeline.resources.config import PipelineConfig

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
            labeling_method      VARCHAR,
            categories           VARCHAR,
            classes              VARCHAR,
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
    description="dispatch JSON 처리 후 dispatch_stage_job 트리거 (archive 이동은 raw_ingest에서 수행)",
    minimum_interval_seconds=30,
    required_resource_keys={"db"},
    job_name="dispatch_stage_job",
)
def dispatch_sensor(context: SensorEvaluationContext):
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
            _move_dispatch_file(req_file, failed_dir, context=context)
            continue

        request_id = req_data.get("request_id")
        folder_name = req_data.get("folder_name")

        try:
            dispatch_payload = parse_dispatch_request_payload(req_data)
        except ValueError as exc:
            _record_failed_request(db_resource, request_id or str(req_file.stem), req_data, str(exc))
            _move_dispatch_file(req_file, failed_dir, context=context)
            continue

        valid_outputs = dispatch_payload["labeling_method"]
        outputs_str = dispatch_payload["outputs_str"]
        run_mode = dispatch_payload["run_mode"]
        categories = dispatch_payload["categories"]
        classes = dispatch_payload["classes"]
        labeling_method = dispatch_payload["labeling_method"]
        archive_only = bool(dispatch_payload.get("archive_only"))
        image_profile = req_data.get("image_profile", "current")
        requested_by = req_data.get("requested_by")
        requested_at_str = req_data.get("requested_at")
        
        if not request_id or not folder_name:
            _record_failed_request(db_resource, request_id or str(req_file.stem), req_data, "missing_request_id_or_folder")
            _move_dispatch_file(req_file, failed_dir, context=context)
            continue

        incoming_folder_path = incoming_dir / folder_name
        if not incoming_folder_path.is_dir():
            _record_failed_request(db_resource, request_id, req_data, "folder_not_in_incoming")
            _move_dispatch_file(req_file, failed_dir, context=context)
            continue
        
        # Check DB to prevent dups
        with db_resource.connect() as conn:
            existing = conn.execute("SELECT status FROM staging_dispatch_requests WHERE folder_name = ? AND status IN ('running', 'archive_moved')", [folder_name]).fetchone()
            if existing:
                _record_failed_request(db_resource, request_id, req_data, "folder_dispatch_in_flight")
                _move_dispatch_file(req_file, failed_dir, context=context)
                continue

            existing_req = conn.execute("SELECT status FROM staging_dispatch_requests WHERE request_id = ?", [request_id]).fetchone()
            if existing_req:
                _record_failed_request(db_resource, request_id, req_data, "duplicate_request_id")
                _move_dispatch_file(req_file, failed_dir, context=context)
                continue

        archive_dir = Path(config.archive_dir)
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_dest = resolve_unique_directory(archive_dir / folder_name)

        now = datetime.now()
        manifest_id = f"dispatch_{request_id}_{now:%Y%m%d_%H%M%S}"
        manifest_filename = f"{manifest_id}.json"
        
        manifest = {
            "manifest_id": manifest_id,
            "generated_at": now.isoformat(),
            "source_dir": str(incoming_dir),
            "source_unit_type": "directory",
            "source_unit_path": str(incoming_folder_path),
            "source_unit_name": folder_name,
            "source_unit_total_file_count": 0,
            "file_count": 0,
            "transfer_tool": "dispatch_sensor",
            "archive_requested": True,
            "categories": categories,
            "classes": classes,
            "labeling_method": labeling_method,
            "files": [],
        }
        
        manifest_dir = Path(config.manifest_dir) / "dispatch"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = manifest_dir / manifest_filename
        
        try:
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            _record_failed_request(db_resource, request_id, req_data, f"failed_to_write_manifest:{e}")
            _move_dispatch_file(req_file, failed_dir, context=context)
            continue

        context.log.info(
            "dispatch manifest 생성 완료(archive 이동/파일 인덱싱은 raw_ingest에서 수행): "
            f"request_id={request_id} folder={folder_name} manifest={manifest_path.name}"
        )

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
                    request_id, folder_name, run_mode, outputs, labeling_method,
                    categories, classes, image_profile,
                    status, archive_pending_path, archive_path,
                    max_frames_per_video, jpeg_quality, confidence_threshold, iou_threshold,
                    requested_by, requested_at, processed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    request_id,
                    folder_name,
                    run_mode,
                    outputs_str,
                    json.dumps(labeling_method, ensure_ascii=False),
                    json.dumps(categories, ensure_ascii=False),
                    json.dumps(classes, ensure_ascii=False),
                    image_profile,
                    None, str(archive_dest),
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
                                "categories": categories,
                                "classes": classes,
                                "labeling_method": labeling_method,
                            }, default=str) if step_name in ("frame_extract", "yolo_detect") else None,
                        ]
                    )

        _move_dispatch_file(req_file, processed_dir, context=context)

        # RunRequest tags 구성 — 이미지 추출 파라미터는 값이 있을 때만 전달
        # folder_name은 sanitize된 버전 사용 (raw_key가 sanitize_path_component로 정규화되므로)
        sanitized_folder = sanitize_path_component(folder_name)
        common_tags = {
            "folder_name": sanitized_folder,
            "folder_name_original": folder_name,
            "image_profile": image_profile,
            "manifest_path": str(manifest_path),
        }
        if categories:
            common_tags["categories"] = json.dumps(categories, ensure_ascii=False)
        if classes:
            common_tags["classes"] = json.dumps(classes, ensure_ascii=False)

        if archive_only:
            context.log.info(
                "dispatch archive-only 요청 감지: request_id=%s folder=%s -> ingest_job만 실행",
                request_id,
                folder_name,
            )
            yield RunRequest(
                run_key=request_id,
                job_name="ingest_job",
                tags={**common_tags, "dispatch_archive_only": "true"},
            )
            continue

        run_tags = {
            **common_tags,
            "outputs": outputs_str,
            "requested_outputs": outputs_str,
            "run_mode": run_mode or "",
            "labeling_method": outputs_str,
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

def _move_dispatch_file(file_path: Path, target_dir: Path, *, context=None) -> bool:
    target_dir.mkdir(parents=True, exist_ok=True)
    destination = target_dir / file_path.name
    suffix = 2
    while destination.exists():
        destination = target_dir / f"{file_path.stem}__{suffix}{file_path.suffix}"
        suffix += 1
    try:
        file_path.replace(destination)
        if context is not None:
            context.log.info(f"dispatch JSON 이동 완료: {file_path} -> {destination}")
        return True
    except Exception as primary_exc:
        try:
            shutil.copy2(str(file_path), str(destination))
            file_path.unlink(missing_ok=True)
            if context is not None:
                context.log.info(f"dispatch JSON 복사+삭제 완료: {file_path} -> {destination}")
            return True
        except Exception as fallback_exc:
            if context is not None:
                context.log.warning(
                    "dispatch JSON 이동 실패: "
                    f"{file_path} -> {destination} "
                    f"(rename_err={primary_exc}, fallback_err={fallback_exc})"
                )
            return False

def _record_failed_request(db, request_id: str, data: dict, error_msg: str):
    try:
        try:
            dispatch_payload = parse_dispatch_request_payload(data)
            outputs_str = dispatch_payload["outputs_str"]
            labeling_method = dispatch_payload["labeling_method"]
            categories = dispatch_payload["categories"]
            classes = dispatch_payload["classes"]
            run_mode = dispatch_payload["run_mode"] or data.get("run_mode")
        except ValueError:
            outputs_list = data.get("outputs")
            outputs_str = ",".join(outputs_list) if isinstance(outputs_list, list) else ""
            labeling_method = data.get("labeling_method") if isinstance(data.get("labeling_method"), list) else []
            categories = data.get("categories") if isinstance(data.get("categories"), list) else []
            classes = data.get("classes") if isinstance(data.get("classes"), list) else []
            run_mode = data.get("run_mode")
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO staging_dispatch_requests (
                    request_id, folder_name, run_mode, outputs, labeling_method,
                    categories, classes, image_profile,
                    max_frames_per_video, jpeg_quality,
                    confidence_threshold, iou_threshold,
                    status, error_message, processed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'failed', ?, ?)
                ON CONFLICT (request_id) DO UPDATE SET 
                    status = 'failed', error_message = excluded.error_message
                """,
                [
                    request_id,
                    data.get("folder_name"),
                    run_mode,
                    outputs_str,
                    json.dumps(labeling_method, ensure_ascii=False),
                    json.dumps(categories, ensure_ascii=False),
                    json.dumps(classes, ensure_ascii=False),
                    data.get("image_profile"),
                    data.get("max_frames_per_video"),
                    data.get("jpeg_quality"),
                    data.get("confidence_threshold"),
                    data.get("iou_threshold"),
                    error_msg, datetime.now()
                ]
            )
    except Exception:
        pass
