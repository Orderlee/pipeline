"""BUILD @asset — 프로젝트별 타임스탬프+Bbox 데이터셋 조립.

프로젝트(source_unit_name) 단위로 timestamp JSON 있는 video와
bbox JSON 있는 image만 `vlm-dataset/<folder>/` 로 server-side copy.

Layer 4: Dagster @asset.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import PurePosixPath
from uuid import uuid4

from dagster import asset

from vlm_pipeline.lib.key_builders import (
    build_gemini_label_key,
    build_sam3_detection_key,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

DATASET_BUCKET = "vlm-dataset"
LABELS_BUCKET = "vlm-labels"


def _require_ls_finalized() -> bool:
    """DATASET_REQUIRE_LS_FINALIZED — 기본 0 (필터 OFF, 레거시 호환)."""
    return os.getenv("DATASET_REQUIRE_LS_FINALIZED", "0") == "1"


def _copy_if_absent(minio: MinIOResource, src_bucket: str, src_key: str,
                    dst_key: str, log,
                    dst_bucket: str = DATASET_BUCKET) -> bool:
    """이미 있으면 skip. 신규 copy 시 True."""
    if minio.exists(dst_bucket, dst_key):
        return False
    minio.copy(src_bucket, src_key, dst_bucket, dst_key)
    log.debug(f"copy: {src_bucket}/{src_key} → {dst_bucket}/{dst_key}")
    return True


def _minio_prefix_from_key(key: str) -> str:
    """raw_key/image_key 의 첫 경로 세그먼트 = MinIO 상의 sanitized folder prefix."""
    return PurePosixPath(str(key or "").strip()).parts[0] if key else ""


def _rel_stem_path(raw_key: str, folder_prefix: str) -> str:
    """folder_prefix 이하의 경로에서 확장자 제외 상대 경로 (subcategory 보존)."""
    path = PurePosixPath(str(raw_key or "").strip())
    parts = list(path.parts)
    if parts and parts[0] == folder_prefix:
        parts = parts[1:]
    # drop trailing 'image/<name>' → '<subdir>/<name>' (image_metadata 경로 보정)
    if len(parts) >= 2 and parts[-2] == "image":
        parts = parts[:-2] + [parts[-1]]
    if not parts:
        return path.stem
    rel = PurePosixPath(*parts)
    return str(rel.with_suffix(""))


def _build_project(context, db: DuckDBResource, minio: MinIOResource,
                   folder: str) -> dict:
    """단일 프로젝트 빌드. 요약 dict 반환."""
    log = context.log

    # ---- 1. 후보 수집 (DB) ----
    require_finalized = _require_ls_finalized()
    videos = db.find_project_video_candidates(folder, require_ls_finalized=require_finalized)
    images = db.find_project_image_candidates(folder, require_ls_finalized=require_finalized)
    clips = db.find_project_clip_candidates(folder)
    log.info(
        f"[{folder}] DB 후보: videos={len(videos)} images={len(images)} clips={len(clips)} "
        f"(require_ls_finalized={require_finalized})"
    )

    if not videos and not images and not clips:
        log.info(f"[{folder}] 후보 없음 — skip")
        return {
            "folder": folder,
            "videos": 0,
            "images": 0,
            "clips": 0,
            "skipped": True,
        }

    # ---- 2. MinIO prefix 결정 (raw_key 기반 sanitized folder) ----
    sample_key = (
        videos[0]["raw_key"]
        if videos
        else (images[0]["image_key"] if images else clips[0]["source_raw_key"])
    )
    folder_prefix = _minio_prefix_from_key(sample_key)
    if not folder_prefix:
        log.error(f"[{folder}] raw_key/image_key에서 folder prefix 추출 실패 — skip")
        return {"folder": folder, "error": "no_folder_prefix"}

    # ---- 3. MinIO 실존 확인 (folder 전체 한 번 list, recursive) ----
    label_keys = set(minio.list_keys(LABELS_BUCKET, f"{folder_prefix}/"))
    ts_key_count = sum(1 for k in label_keys if "/events/" in k)
    bbox_key_count = sum(1 for k in label_keys if "/sam3_segmentations/" in k)
    log.info(
        f"[{folder}] prefix={folder_prefix} MinIO labels: "
        f"total={len(label_keys)} events={ts_key_count} sam3={bbox_key_count}"
    )

    # ---- 4. dataset row 등록 ----
    dataset_id = str(uuid4())
    db.insert_dataset({
        "dataset_id": dataset_id,
        "name": folder,
        "version": "v1",
        "config": json.dumps({"schema": "project_flat", "with_timestamp": True, "with_bbox": True}),
        "split_ratio": None,
        "dataset_bucket": DATASET_BUCKET,
        "dataset_prefix": folder_prefix,
        "build_status": "building",
    })

    # ---- 5. Videos + Timestamps ----
    video_entries: list[dict] = []
    video_copies_new = ts_copies_new = 0
    for video in videos:
        raw_key = video["raw_key"]
        expected_ts_key = build_gemini_label_key(raw_key)
        if expected_ts_key not in label_keys:
            continue

        ext = PurePosixPath(raw_key).suffix or ".mp4"
        rel_stem = _rel_stem_path(raw_key, folder_prefix)
        dataset_video_key = f"{folder_prefix}/videos/{rel_stem}{ext}"
        dataset_ts_key = f"{folder_prefix}/timestamps/{rel_stem}.json"

        try:
            if _copy_if_absent(minio, video["raw_bucket"], raw_key, dataset_video_key, log):
                video_copies_new += 1
            if _copy_if_absent(minio, LABELS_BUCKET, expected_ts_key, dataset_ts_key, log):
                ts_copies_new += 1
        except Exception as exc:
            log.error(f"[{folder}] video copy 실패: {raw_key}: {exc}")
            continue

        video_entries.append({
            "rel_stem": rel_stem,
            "dataset_video_key": dataset_video_key,
            "dataset_timestamp_key": dataset_ts_key,
            "source_raw_bucket": video["raw_bucket"],
            "source_raw_key": raw_key,
        })

    # ---- 5b. Clips (LS 라벨링 완료된 event 단위 clip video) ----
    clip_entries: list[dict] = []
    clip_copies_new = 0
    for clip in clips:
        clip_key = clip["clip_key"]
        src_bucket = clip["processed_bucket"]
        ext = PurePosixPath(clip_key).suffix or ".mp4"

        # clip_key에서 folder_prefix 제거 + 'clips/' 세그먼트 drop → '<subcat>/<stem>'
        path = PurePosixPath(str(clip_key).strip())
        parts = list(path.parts)
        if parts and parts[0] == folder_prefix:
            parts = parts[1:]
        parts = [p for p in parts if p != "clips"]
        if not parts:
            rel_stem = path.stem
        else:
            rel_stem = str(PurePosixPath(*parts).with_suffix(""))

        dataset_clip_key = f"{folder_prefix}/clips/{rel_stem}{ext}"

        try:
            if _copy_if_absent(minio, src_bucket, clip_key, dataset_clip_key, log):
                clip_copies_new += 1
        except Exception as exc:
            log.error(f"[{folder}] clip copy 실패: {clip_key}: {exc}")
            continue

        clip_entries.append({
            "rel_stem": rel_stem,
            "dataset_clip_key": dataset_clip_key,
            "source_clip_bucket": src_bucket,
            "source_clip_key": clip_key,
            "source_raw_key": clip.get("source_raw_key"),
            "event_index": clip.get("event_index"),
            "clip_start_sec": clip.get("clip_start_sec"),
            "clip_end_sec": clip.get("clip_end_sec"),
        })

    # ---- 6. Images + Bboxes ----
    image_entries: list[dict] = []
    image_copies_new = bbox_copies_new = 0
    for image in images:
        image_key = image["image_key"]
        expected_bbox_key = build_sam3_detection_key(image_key)
        if expected_bbox_key not in label_keys:
            continue

        ext = PurePosixPath(image_key).suffix or ".jpg"
        rel_stem = _rel_stem_path(image_key, folder_prefix)
        dataset_image_key = f"{folder_prefix}/images/{rel_stem}{ext}"
        dataset_bbox_key = f"{folder_prefix}/bboxes/{rel_stem}.json"

        try:
            if _copy_if_absent(minio, image["image_bucket"], image_key, dataset_image_key, log):
                image_copies_new += 1
            if _copy_if_absent(minio, LABELS_BUCKET, expected_bbox_key, dataset_bbox_key, log):
                bbox_copies_new += 1
        except Exception as exc:
            log.error(f"[{folder}] image copy 실패: {image_key}: {exc}")
            continue

        image_entries.append({
            "rel_stem": rel_stem,
            "dataset_image_key": dataset_image_key,
            "dataset_bbox_key": dataset_bbox_key,
            "source_image_bucket": image["image_bucket"],
            "source_image_key": image_key,
        })

    # ---- 7. manifest.json ----
    manifest = {
        "project": folder,
        "folder_prefix": folder_prefix,
        "built_at": datetime.utcnow().isoformat() + "Z",
        "bucket": DATASET_BUCKET,
        "prefix": folder_prefix,
        "counts": {
            "videos": len(video_entries),
            "timestamps": len(video_entries),
            "clips": len(clip_entries),
            "images": len(image_entries),
            "bboxes": len(image_entries),
        },
        "videos": video_entries,
        "clips": clip_entries,
        "images": image_entries,
    }
    minio.upload(
        DATASET_BUCKET,
        f"{folder_prefix}/manifest.json",
        json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        "application/json",
    )

    db.update_dataset_status(dataset_id, "completed")

    summary = {
        "folder": folder,
        "folder_prefix": folder_prefix,
        "dataset_id": dataset_id,
        "videos": len(video_entries),
        "clips": len(clip_entries),
        "images": len(image_entries),
        "video_copies_new": video_copies_new,
        "timestamp_copies_new": ts_copies_new,
        "clip_copies_new": clip_copies_new,
        "image_copies_new": image_copies_new,
        "bbox_copies_new": bbox_copies_new,
    }
    log.info(f"[{folder}] 완료: {summary}")
    return summary


@asset(
    deps=["clip_to_frame"],
    description=(
        "프로젝트별 timestamp JSON + bbox JSON + 각 원본을 vlm-dataset 버킷에 조립. "
        "완료 판별은 MinIO JSON 실존 기반. split 없음, 전체 복사. 멱등."
    ),
    group_name="build",
)
def build_dataset(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    projects = db.find_projects_for_dataset_build()
    if not projects:
        context.log.info("BUILD 대상 프로젝트 없음")
        return {"projects": 0, "summaries": []}

    minio.ensure_bucket(DATASET_BUCKET)

    summaries: list[dict] = []
    for proj in projects:
        folder = proj["folder"]
        try:
            summaries.append(_build_project(context, db, minio, folder))
        except Exception as exc:
            context.log.error(f"[{folder}] 빌드 실패: {exc}")
            summaries.append({"folder": folder, "error": str(exc)})

    total = {
        "projects": len(summaries),
        "total_videos": sum(s.get("videos", 0) for s in summaries),
        "total_clips": sum(s.get("clips", 0) for s in summaries),
        "total_images": sum(s.get("images", 0) for s in summaries),
    }
    context.add_output_metadata(total)
    context.log.info(f"BUILD 전체 완료: {total}")
    return {**total, "summaries": summaries}
