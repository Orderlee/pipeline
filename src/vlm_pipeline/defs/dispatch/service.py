"""Dispatch sensor/service helpers."""

from __future__ import annotations

import json
import shutil
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from dagster import RunRequest, SensorEvaluationContext
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

from vlm_pipeline.defs.ingest.archive import resolve_unique_directory
from vlm_pipeline.lib.sanitizer import sanitize_path_component
from vlm_pipeline.lib.dispatch_payload import format_dispatch_storage_list, parse_dispatch_request_payload
from vlm_pipeline.lib.yolo_thresholds import (
    resolve_active_class_confidence_thresholds,
    resolve_effective_request_confidence_threshold,
)
from vlm_pipeline.resources.config import PipelineConfig

_OUTPUT_TO_STEPS = {
    "bbox": [
        ("archive_move", 1),
        ("frame_extract", 2),
        ("yolo_detect", 3),
    ],
    "timestamp_video": [
        ("archive_move", 1),
        ("gemini_timestamp", 2),
    ],
    "captioning_video": [
        ("archive_move", 1),
        ("gemini_timestamp", 2),
        ("gemini_caption", 3),
        ("frame_extract", 4),
    ],
    "captioning_image": [
        ("archive_move", 1),
        ("gemini_timestamp", 2),
        ("gemini_caption", 3),
        ("frame_extract", 4),
        ("gemini_image_caption", 5),
    ],
    "classification_video": [
        ("archive_move", 1),
        ("gemini_video_classification", 2),
    ],
    "classification_image": [
        ("archive_move", 1),
        ("frame_extract", 2),
        ("yolo_detect", 3),
        ("image_classification", 4),
    ],
}

_DISPATCH_RUN_JOBS = {"dispatch_stage_job", "ingest_job"}
DispatchDuplicatePolicy = Literal["reject", "accept_noop"]
DispatchInFlightPolicy = Literal["reject", "defer"]
DispatchIngressStatus = Literal["run_request", "rejected", "duplicate_noop", "deferred"]


@dataclass(frozen=True)
class DispatchAppliedParams:
    max_frames_per_video: int | None
    jpeg_quality: int | None
    confidence_threshold: float | None
    iou_threshold: float | None


@dataclass(frozen=True)
class PreparedDispatchRequest:
    request_id: str
    folder_name: str
    incoming_folder_path: Path
    run_mode: str
    outputs_str: str
    labeling_method: list[str]
    categories: list[str]
    classes: list[str]
    image_profile: str
    requested_by: str | None
    requested_at: str | None
    archive_only: bool
    storage_outputs: str
    storage_labeling_method: str
    storage_categories: str
    storage_classes: str
    # GenAI promote 경로용 provenance pass-through. None 이면 manifest 에 안 들어가고
    # ops_register 는 DB DEFAULT(source_type='camera', label_policy='required') 적용.
    # ops_register.py:17-19 의 _VALID_* enum 으로 fail-loud validate.
    source_type: str | None = None
    genai_engine: str | None = None
    label_policy: str | None = None
    genai_batch_id: str | None = None
    items: list[dict] | None = None
    # archive 경로 override — archive_dir 기준 상대 경로. 예: "genai/abc123".
    # None 이면 기존 동작(archive_dir / folder_name). GenAI promote 가 평탄한 archive/
    # 루트 대신 archive/genai/<batch_id>/ 형태로 묶으려고 추가됨 (2026-05-29).
    archive_path: str | None = None
    # category → description (자연어 설명) 매핑. Gemini prompt 에 inject 되어 검출 정확도
    # 향상. promote 폼의 hybrid preset 이 자동 채움. 빈 dict / 미입력 = 기존 prompt 동작
    # (regression 0). 2026-06-02 등록.
    gemini_descriptions: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class DispatchIngressRequest:
    payload: Mapping[str, Any]
    fallback_request_id: str
    duplicate_policy: DispatchDuplicatePolicy
    in_flight_policy: DispatchInFlightPolicy


@dataclass(frozen=True)
class DispatchIngressResult:
    status: DispatchIngressStatus
    request_id: str
    reason: str
    prepared: PreparedDispatchRequest | None = None
    run_request: RunRequest | None = None


def resolve_dispatch_request_id_from_tags(tags: Mapping[str, Any]) -> str | None:
    request_id = str(tags.get("dispatch_request_id") or tags.get("dagster/run_key") or "").strip()
    return request_id or None


def resolve_dispatch_folder_name_from_tags(tags: Mapping[str, Any]) -> str | None:
    folder_name = str(tags.get("folder_name_original") or tags.get("folder_name") or "").strip()
    return folder_name or None


def has_active_dispatch_run(context: SensorEvaluationContext, folder_name: str) -> bool:
    normalized_folder = str(folder_name or "").strip()
    if not normalized_folder:
        return False
    try:
        active_runs = context.instance.get_runs(
            filters=RunsFilter(statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED]),
            limit=100,
        )
    except Exception:
        return False

    for run in active_runs:
        if run.job_name not in _DISPATCH_RUN_JOBS:
            continue
        tags = getattr(run, "tags", {}) or {}
        if resolve_dispatch_folder_name_from_tags(tags) == normalized_folder:
            return True
    return False


def move_dispatch_file(file_path: Path, target_dir: Path, *, context=None) -> bool:
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


def prepare_dispatch_request(req_data: Mapping[str, Any], *, incoming_dir: Path) -> PreparedDispatchRequest:
    request_id = str(req_data.get("request_id") or "").strip()
    folder_name = str(req_data.get("folder_name") or "").strip()
    if not request_id or not folder_name:
        raise ValueError("missing_request_id_or_folder")

    payload = parse_dispatch_request_payload(req_data)
    labeling_method = payload["labeling_method"]
    categories = payload["categories"]
    classes = payload["classes"]

    # GenAI promote 경로 pass-through — payload 에 있으면 manifest 까지 전파.
    # 일반 dispatch 요청은 이 필드 비어있어 무해 (None 이면 manifest 에 키 안 들어감).
    # validate 는 ops_register.py 의 fail-loud (CHECK constraint 와 동기화된 enum).
    src_type = str(req_data.get("source_type") or "").strip() or None
    g_engine = str(req_data.get("genai_engine") or "").strip() or None
    lpolicy = str(req_data.get("label_policy") or "").strip() or None
    g_batch = str(req_data.get("batch_id") or "").strip() or None
    items_raw = req_data.get("items")
    items_norm: list[dict] | None = None
    if isinstance(items_raw, list):
        items_norm = [it for it in items_raw if isinstance(it, dict)]

    # archive_path override — path traversal/절대경로 차단. 빈 컴포넌트('a//b'), '..',
    # 절대경로('/foo') 거부. None 이면 무시. 정규화는 PurePosixPath 로 강제 — Windows
    # path 차단 (운영 환경 linux 전제).
    raw_arch_path = str(req_data.get("archive_path") or "").strip()
    arch_path: str | None = None
    if raw_arch_path:
        from pathlib import PurePosixPath
        pp = PurePosixPath(raw_arch_path)
        bad_parts = [p for p in pp.parts if p in ("", ".", "..")]
        if pp.is_absolute() or bad_parts or "\\" in raw_arch_path:
            raise ValueError(f"invalid_archive_path:{raw_arch_path!r}")
        arch_path = str(pp)

    # gemini_descriptions: category → description 매핑. Gemini prompt 에 inject.
    # 입력 형식: dict 직접 또는 JSON string (`gemini_descriptions_json`). 둘 다 지원해
    # promote 폼 (multipart string) + CLI (JSON) 양쪽에서 보낼 수 있게.
    g_desc_raw = req_data.get("gemini_descriptions")
    if g_desc_raw is None:
        g_desc_raw = req_data.get("gemini_descriptions_json")
    g_descriptions: dict[str, str] = {}
    if g_desc_raw not in (None, ""):
        if isinstance(g_desc_raw, str):
            try:
                parsed = json.loads(g_desc_raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid_gemini_descriptions_json: {exc}")
        else:
            parsed = g_desc_raw
        if not isinstance(parsed, dict):
            raise ValueError("invalid_gemini_descriptions")
        for k, v in parsed.items():
            if not isinstance(k, str):
                raise ValueError("invalid_gemini_descriptions_key")
            ks = k.strip()
            if not ks:
                continue
            vs = v.strip() if isinstance(v, str) else ""
            g_descriptions[ks] = vs

    return PreparedDispatchRequest(
        request_id=request_id,
        folder_name=folder_name,
        incoming_folder_path=incoming_dir / folder_name,
        run_mode=str(payload["run_mode"] or ""),
        outputs_str=str(payload["outputs_str"] or ""),
        labeling_method=labeling_method,
        categories=categories,
        classes=classes,
        image_profile=str(req_data.get("image_profile") or "current"),
        requested_by=str(req_data.get("requested_by") or "").strip() or None,
        requested_at=str(req_data.get("requested_at") or "").strip() or None,
        archive_only=bool(payload.get("archive_only")),
        storage_outputs=format_dispatch_storage_list(labeling_method),
        storage_labeling_method=format_dispatch_storage_list(labeling_method),
        storage_categories=format_dispatch_storage_list(categories),
        storage_classes=format_dispatch_storage_list(classes),
        source_type=src_type,
        genai_engine=g_engine,
        label_policy=lpolicy,
        genai_batch_id=g_batch,
        items=items_norm,
        archive_path=arch_path,
        gemini_descriptions=g_descriptions,
    )


def resolve_dispatch_applied_params(
    req_data: Mapping[str, Any],
    model_defaults: Mapping[str, Mapping[str, Any]],
) -> DispatchAppliedParams:
    bbox_defaults = model_defaults.get("bbox", {})
    jpeg_quality = req_data.get("jpeg_quality") or bbox_defaults.get("default_jpeg_quality")
    confidence = req_data.get("confidence_threshold") or bbox_defaults.get("default_confidence")
    iou = req_data.get("iou_threshold") or bbox_defaults.get("default_iou")
    return DispatchAppliedParams(
        max_frames_per_video=None,
        jpeg_quality=int(jpeg_quality) if jpeg_quality is not None else None,
        confidence_threshold=float(confidence) if confidence is not None else None,
        iou_threshold=float(iou) if iou is not None else None,
    )


def write_dispatch_manifest(
    config: PipelineConfig,
    prepared: PreparedDispatchRequest,
) -> tuple[Path, Path]:
    archive_dir = Path(config.archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)
    # archive_path override 가 있으면 그 경로(상대) 로 archive_dest. 기본은 folder_name.
    # 예: GenAI promote → archive_path="genai/abc" → archive_dir/genai/abc/
    # 일반 dispatch (archive_path 없음) → archive_dir/folder_name/ (기존 동작).
    if prepared.archive_path:
        archive_dest_base = archive_dir / prepared.archive_path
    else:
        archive_dest_base = archive_dir / prepared.folder_name
    archive_dest = resolve_unique_directory(archive_dest_base)
    # 부모 디렉토리(예: archive/genai/) 미존재 시 archive_move 가 mkdir 한다.
    archive_dest.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    manifest_id = f"dispatch_{prepared.request_id}_{now:%Y%m%d_%H%M%S}"
    manifest = {
        "manifest_id": manifest_id,
        "generated_at": now.isoformat(),
        "source_dir": str(Path(config.incoming_dir)),
        "source_unit_type": "directory",
        "source_unit_path": str(prepared.incoming_folder_path),
        "source_unit_name": prepared.folder_name,
        "source_unit_total_file_count": 0,
        "file_count": 0,
        "transfer_tool": "dispatch_sensor",
        "archive_requested": True,
        "categories": prepared.categories,
        "classes": prepared.classes,
        "labeling_method": prepared.labeling_method,
        "files": [],
    }
    # GenAI promote: provenance 필드를 manifest 에 통과. ops_register.py:191-211 이
    # source_type/genai_engine/label_policy 를 raw_files 에 INSERT 하고, batch_id+items
    # 매핑(ops_register.py:43-48) 으로 genai_jobs 와 연결한다. None 이면 키 자체 생략 →
    # ops_register 는 DB DEFAULT 적용 (camera/required).
    if prepared.source_type:
        manifest["source_type"] = prepared.source_type
    if prepared.genai_engine:
        manifest["genai_engine"] = prepared.genai_engine
    if prepared.label_policy:
        manifest["label_policy"] = prepared.label_policy
    if prepared.genai_batch_id:
        manifest["batch_id"] = prepared.genai_batch_id
    if prepared.items:
        manifest["items"] = prepared.items
    if prepared.archive_path:
        # 트레이서빌리티 — manifest 에도 기록. archive_finalize 의 archive_dest 와 일관.
        manifest["archive_path"] = prepared.archive_path
    if prepared.gemini_descriptions:
        manifest["gemini_descriptions"] = prepared.gemini_descriptions
    manifest_dir = Path(config.manifest_dir) / "dispatch"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"{manifest_id}.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    return manifest_path, archive_dest


def resolve_archive_paths_from_folder(
    db_resource,
    folder_name: str,
) -> list[dict[str, Any]]:
    """Phase 2b: archive 에 이미 옮겨진 raw_files 조회 — backend 도메인 메서드 위임.

    DB 오류는 raise — sensor 가 lock 충돌 등 transient error 와 "데이터 없음"을
    구분해서 재시도 또는 fail-fast 결정하도록 한다.
    """
    return db_resource.list_archived_raw_files_for_folder(folder_name)


def write_archive_dispatch_manifest(
    config: PipelineConfig,
    prepared: PreparedDispatchRequest,
    archive_rows: list[dict[str, Any]],
) -> Path:
    """Phase 2b: archive 데이터 dispatch 용 manifest.

    `source_unit_path` 를 archive 폴더(공통 부모) 로 두고 `archive_requested=False`,
    `upload_enabled=True`, `from_archived=True` 플래그를 명시한다. files 는 DB lookup
    결과 그대로 (재스캔 없이) 채운다.
    """
    if not archive_rows:
        raise ValueError("no_archive_rows")

    archive_paths = [r["archive_path"] for r in archive_rows]
    archive_root = str(Path(archive_paths[0]).parent.parent)
    archive_unit_dir = str(Path(archive_paths[0]).parent)

    now = datetime.now()
    manifest_id = f"archive_dispatch_{prepared.request_id}_{now:%Y%m%d_%H%M%S}"
    files = []
    for row in archive_rows:
        archive_path = str(row["archive_path"])
        rel_path = Path(archive_path).name
        files.append(
            {
                "path": archive_path,
                "size": int(row.get("file_size") or 0),
                "rel_path": rel_path,
                "asset_id": row["asset_id"],
                "raw_key": row["raw_key"],
                "media_type": row.get("media_type"),
            }
        )

    manifest = {
        "manifest_id": manifest_id,
        "generated_at": now.isoformat(),
        "source_dir": archive_root,
        "source_unit_type": "directory",
        "source_unit_path": archive_unit_dir,
        "source_unit_name": prepared.folder_name,
        "source_unit_total_file_count": len(files),
        "file_count": len(files),
        "transfer_tool": "archive_dispatch_sensor",
        "archive_requested": False,
        "upload_enabled": True,
        "from_archived": True,
        "request_id": prepared.request_id,
        "categories": prepared.categories,
        "classes": prepared.classes,
        "labeling_method": prepared.labeling_method,
        "image_profile": prepared.image_profile,
        "requested_by": prepared.requested_by,
        "files": files,
    }
    if prepared.gemini_descriptions:
        manifest["gemini_descriptions"] = prepared.gemini_descriptions
    manifest_dir = Path(config.manifest_dir) / "dispatch"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"{manifest_id}.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    return manifest_path


def build_dispatch_request_record(
    prepared: PreparedDispatchRequest,
    *,
    archive_dest: Path,
    applied_params: DispatchAppliedParams,
    processed_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "request_id": prepared.request_id,
        "folder_name": prepared.folder_name,
        "run_mode": prepared.run_mode,
        "outputs": prepared.storage_outputs,
        "labeling_method": prepared.storage_labeling_method,
        "categories": prepared.storage_categories,
        "classes": prepared.storage_classes,
        "image_profile": prepared.image_profile,
        "status": "running",
        "archive_pending_path": None,
        "archive_path": str(archive_dest),
        "max_frames_per_video": None,
        "jpeg_quality": applied_params.jpeg_quality,
        "confidence_threshold": applied_params.confidence_threshold,
        "iou_threshold": applied_params.iou_threshold,
        "requested_by": prepared.requested_by,
        "requested_at": prepared.requested_at,
        "processed_at": processed_at or datetime.now(),
    }


def build_dispatch_pipeline_rows(
    prepared: PreparedDispatchRequest,
    *,
    model_defaults: Mapping[str, Mapping[str, Any]],
    applied_params: DispatchAppliedParams,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_steps: set[str] = set()
    applied_params_payload = {
        "jpeg_quality": applied_params.jpeg_quality,
        "confidence": applied_params.confidence_threshold,
        "iou": applied_params.iou_threshold,
        "categories": prepared.categories,
        "classes": prepared.classes,
        "labeling_method": prepared.labeling_method,
    }
    if "bbox" in prepared.labeling_method and applied_params.confidence_threshold is not None:
        class_confidence_thresholds = resolve_active_class_confidence_thresholds(
            prepared.classes,
            applied_params.confidence_threshold,
        )
        applied_params_payload["class_confidence_thresholds"] = class_confidence_thresholds
        applied_params_payload["effective_request_confidence_threshold"] = (
            resolve_effective_request_confidence_threshold(
                applied_params.confidence_threshold,
                class_confidence_thresholds,
            )
        )
    applied_params_json = json.dumps(applied_params_payload, default=str)
    for output_key in prepared.labeling_method:
        steps = _OUTPUT_TO_STEPS.get(output_key, [])
        defaults = model_defaults.get(output_key, {})
        for step_name, step_order in steps:
            if step_name in seen_steps:
                continue
            seen_steps.add(step_name)
            rows.append(
                {
                    "run_id": str(uuid4()),
                    "request_id": prepared.request_id,
                    "folder_name": prepared.folder_name,
                    "step_name": step_name,
                    "step_order": step_order,
                    "step_status": "pending",
                    "model_name": defaults.get("model_name"),
                    "model_version": defaults.get("model_version"),
                    "applied_params": (applied_params_json if step_name in {"frame_extract", "yolo_detect"} else None),
                }
            )
    return rows


def build_dispatch_run_request(
    prepared: PreparedDispatchRequest,
    *,
    manifest_path: Path,
    applied_params: DispatchAppliedParams,
) -> RunRequest:
    sanitized_folder = sanitize_path_component(prepared.folder_name)
    common_tags = {
        "dispatch_request_id": prepared.request_id,
        "folder_name": sanitized_folder,
        "folder_name_original": prepared.folder_name,
        "image_profile": prepared.image_profile,
        "manifest_path": str(manifest_path),
    }
    if prepared.categories:
        common_tags["categories"] = json.dumps(prepared.categories, ensure_ascii=False)
    if prepared.classes:
        common_tags["classes"] = json.dumps(prepared.classes, ensure_ascii=False)
    if prepared.gemini_descriptions:
        common_tags["gemini_descriptions"] = json.dumps(
            prepared.gemini_descriptions, ensure_ascii=False
        )

    if prepared.archive_only:
        return RunRequest(
            run_key=prepared.request_id,
            job_name="ingest_job",
            tags={**common_tags, "dispatch_archive_only": "true"},
        )

    run_tags = {
        **common_tags,
        "outputs": prepared.outputs_str,
        "requested_outputs": prepared.outputs_str,
        "run_mode": prepared.run_mode,
        "labeling_method": prepared.outputs_str,
    }
    if applied_params.jpeg_quality is not None:
        run_tags["jpeg_quality"] = str(applied_params.jpeg_quality)
    if applied_params.confidence_threshold is not None:
        run_tags["confidence_threshold"] = str(applied_params.confidence_threshold)
    if applied_params.iou_threshold is not None:
        run_tags["iou_threshold"] = str(applied_params.iou_threshold)
    return RunRequest(
        run_key=prepared.request_id,
        job_name="dispatch_stage_job",
        tags=run_tags,
    )


def process_dispatch_ingress_request(
    context: SensorEvaluationContext,
    *,
    db_resource,
    config: PipelineConfig,
    ingress_request: DispatchIngressRequest,
) -> DispatchIngressResult:
    request_id = (
        str(ingress_request.payload.get("request_id") or ingress_request.fallback_request_id or "").strip()
        or ingress_request.fallback_request_id
    )

    try:
        prepared = prepare_dispatch_request(ingress_request.payload, incoming_dir=Path(config.incoming_dir))
    except ValueError as exc:
        return DispatchIngressResult(
            status="rejected",
            request_id=request_id,
            reason=str(exc),
        )

    if not prepared.incoming_folder_path.is_dir():
        return DispatchIngressResult(
            status="rejected",
            request_id=prepared.request_id,
            reason="folder_not_in_incoming",
            prepared=prepared,
        )

    existing_rows = db_resource.get_in_flight_dispatch_requests(prepared.folder_name)
    existing_status = db_resource.get_dispatch_request_status(prepared.request_id)

    if existing_rows and has_active_dispatch_run(context, prepared.folder_name):
        if ingress_request.in_flight_policy == "defer":
            return DispatchIngressResult(
                status="deferred",
                request_id=prepared.request_id,
                reason="folder_dispatch_in_flight",
                prepared=prepared,
            )
        return DispatchIngressResult(
            status="rejected",
            request_id=prepared.request_id,
            reason="folder_dispatch_in_flight",
            prepared=prepared,
        )

    if existing_rows:
        for row in existing_rows:
            stale_request_id = str(row.get("request_id") or "").strip()
            if not stale_request_id:
                continue
            db_resource.close_dispatch_request(
                stale_request_id,
                status="canceled",
                error_message="stale_dispatch_request_without_active_run",
            )

    if existing_status:
        if ingress_request.duplicate_policy == "accept_noop":
            return DispatchIngressResult(
                status="duplicate_noop",
                request_id=prepared.request_id,
                reason="duplicate_request_id_noop",
                prepared=prepared,
            )
        return DispatchIngressResult(
            status="rejected",
            request_id=prepared.request_id,
            reason="duplicate_request_id",
            prepared=prepared,
        )

    try:
        manifest_path, archive_dest = write_dispatch_manifest(config, prepared)
    except Exception as exc:  # noqa: BLE001
        return DispatchIngressResult(
            status="rejected",
            request_id=prepared.request_id,
            reason=f"failed_to_write_manifest:{exc}",
            prepared=prepared,
        )

    context.log.info(
        "dispatch manifest 생성 완료(archive 이동/파일 인덱싱은 raw_ingest에서 수행): "
        f"request_id={prepared.request_id} folder={prepared.folder_name} manifest={manifest_path.name}"
    )

    model_defaults = db_resource.get_active_staging_model_configs(prepared.labeling_method)
    applied_params = resolve_dispatch_applied_params(ingress_request.payload, model_defaults)

    db_resource.insert_dispatch_request(
        build_dispatch_request_record(
            prepared,
            archive_dest=archive_dest,
            applied_params=applied_params,
        )
    )
    db_resource.insert_dispatch_pipeline_runs(
        build_dispatch_pipeline_rows(
            prepared,
            model_defaults=model_defaults,
            applied_params=applied_params,
        )
    )
    run_request = build_dispatch_run_request(
        prepared,
        manifest_path=manifest_path,
        applied_params=applied_params,
    )
    return DispatchIngressResult(
        status="run_request",
        request_id=prepared.request_id,
        reason="run_request_created",
        prepared=prepared,
        run_request=run_request,
    )


def record_failed_dispatch_request(db, request_id: str, data: Mapping[str, Any], error_message: str) -> None:
    try:
        try:
            prepared = prepare_dispatch_request(data, incoming_dir=Path("/"))
            outputs = prepared.storage_outputs
            labeling_method = prepared.storage_labeling_method
            categories = prepared.storage_categories
            classes = prepared.storage_classes
            run_mode = prepared.run_mode
        except ValueError:
            outputs_list = data.get("outputs") if isinstance(data.get("outputs"), list) else []
            labeling_list = data.get("labeling_method") if isinstance(data.get("labeling_method"), list) else []
            category_list = data.get("categories") if isinstance(data.get("categories"), list) else []
            class_list = data.get("classes") if isinstance(data.get("classes"), list) else []
            outputs = format_dispatch_storage_list(labeling_list or outputs_list)
            labeling_method = format_dispatch_storage_list(labeling_list)
            categories = format_dispatch_storage_list(category_list)
            classes = format_dispatch_storage_list(class_list)
            run_mode = str(data.get("run_mode") or "")

        db.upsert_failed_dispatch_request(
            {
                "request_id": request_id,
                "folder_name": data.get("folder_name"),
                "run_mode": run_mode,
                "outputs": outputs,
                "labeling_method": labeling_method,
                "categories": categories,
                "classes": classes,
                "image_profile": data.get("image_profile"),
                "max_frames_per_video": data.get("max_frames_per_video"),
                "jpeg_quality": data.get("jpeg_quality"),
                "confidence_threshold": data.get("confidence_threshold"),
                "iou_threshold": data.get("iou_threshold"),
                "error_message": error_message,
                "processed_at": datetime.now(),
            }
        )
    except Exception:
        pass
