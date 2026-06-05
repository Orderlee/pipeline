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

from dagster import Field, RetryPolicy, asset

from vlm_pipeline.lib.dataset_lineage import make_build_lineage
from vlm_pipeline.lib.key_builders import (
    build_gemini_label_key,
    build_sam3_detection_key,
)
from vlm_pipeline.resources.postgres import PostgresResource
from vlm_pipeline.resources.minio import MinIOResource

from vlm_pipeline.defs.build.build_helpers import (
    LABELS_BUCKET,
    _dataset_bucket_value,
    _ensure_dataset_root,
    _copy_if_outdated,
    _minio_prefix_from_key,
    _rel_stem_path,
    _write_dataset_artifact,
)
from vlm_pipeline.defs.build.build_genai import (
    _build_project_genai,
    _genai_pairs_or_empty,
)


def _require_ls_finalized() -> bool:
    """DATASET_REQUIRE_LS_FINALIZED — 기본 0 (필터 OFF, 레거시 호환)."""
    return os.getenv("DATASET_REQUIRE_LS_FINALIZED", "0") == "1"


def _build_project(context, db: PostgresResource, minio: MinIOResource, folder: str) -> dict:
    """단일 프로젝트 빌드. 요약 dict 반환.

    CASE 분기:
      - GenAI (source_unit_name='genai_<batch_id>', label_policy='none'):
          genai_jobs JOIN 으로 (input image, output video|image) pair 단위 copy
      - 일반 (camera|nas_upload, label_policy='required'):
          기존 timestamp/bbox JSON 필요 경로
    """
    log = context.log

    # ---- 0. GenAI pair path (있으면 early return) ----
    genai_pairs = _genai_pairs_or_empty(db, folder, log=log)
    if genai_pairs:
        return _build_project_genai(context, db, minio, folder, genai_pairs)

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
    sample_key = videos[0]["raw_key"] if videos else (images[0]["image_key"] if images else clips[0]["source_raw_key"])
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

    # ---- 4. dataset row 등록 (Phase 3-D lineage 메타 포함) ----
    dataset_id = str(uuid4())
    config_dict = {"schema": "project_flat", "with_timestamp": True, "with_bbox": True}
    db.insert_dataset(
        {
            "dataset_id": dataset_id,
            "name": folder,
            "version": "v1",
            "config": json.dumps(config_dict),
            "split_ratio": None,
            "dataset_bucket": _dataset_bucket_value(),
            "dataset_prefix": folder_prefix,
            "build_status": "building",
            **make_build_lineage(config_dict),
        }
    )

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
            if _copy_if_outdated(minio, video["raw_bucket"], raw_key, dataset_video_key, log):
                video_copies_new += 1
            if _copy_if_outdated(minio, LABELS_BUCKET, expected_ts_key, dataset_ts_key, log):
                ts_copies_new += 1
        except Exception as exc:
            log.error(f"[{folder}] video copy 실패: {raw_key}: {exc}")
            continue

        video_entries.append(
            {
                "rel_stem": rel_stem,
                "dataset_video_key": dataset_video_key,
                "dataset_timestamp_key": dataset_ts_key,
                "source_raw_bucket": video["raw_bucket"],
                "source_raw_key": raw_key,
            }
        )

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
            if _copy_if_outdated(minio, src_bucket, clip_key, dataset_clip_key, log):
                clip_copies_new += 1
        except Exception as exc:
            log.error(f"[{folder}] clip copy 실패: {clip_key}: {exc}")
            continue

        clip_entries.append(
            {
                "rel_stem": rel_stem,
                "dataset_clip_key": dataset_clip_key,
                "source_clip_bucket": src_bucket,
                "source_clip_key": clip_key,
                "source_raw_key": clip.get("source_raw_key"),
                "event_index": clip.get("event_index"),
                "clip_start_sec": clip.get("clip_start_sec"),
                "clip_end_sec": clip.get("clip_end_sec"),
            }
        )

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
            if _copy_if_outdated(minio, image["image_bucket"], image_key, dataset_image_key, log):
                image_copies_new += 1
            if _copy_if_outdated(minio, LABELS_BUCKET, expected_bbox_key, dataset_bbox_key, log):
                bbox_copies_new += 1
        except Exception as exc:
            log.error(f"[{folder}] image copy 실패: {image_key}: {exc}")
            continue

        image_entries.append(
            {
                "rel_stem": rel_stem,
                "dataset_image_key": dataset_image_key,
                "dataset_bbox_key": dataset_bbox_key,
                "source_image_bucket": image["image_bucket"],
                "source_image_key": image_key,
            }
        )

    # ---- 7. manifest.json ----
    manifest = {
        "project": folder,
        "folder_prefix": folder_prefix,
        "built_at": datetime.utcnow().isoformat() + "Z",
        "bucket": _dataset_bucket_value(),
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
    _write_dataset_artifact(
        minio,
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
        "완료 판별은 MinIO JSON 실존 기반. split 없음, 전체 복사. 멱등. "
        "config.folder 주어지면 해당 프로젝트만, 없으면 전체 순회."
    ),
    group_name="build",
    config_schema={"folder": Field(str, is_required=False)},
    # transient error (DuckDB lock conflict, MinIO 일시 장애 등) 자동 재시도.
    # delay=60s 면 lock 보유한 다른 writer 가 풀릴 충분한 시간 + sensor interval 과 정렬.
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def build_dataset(
    context,
    db: PostgresResource,
    minio: MinIOResource,
) -> dict:
    folder_filter = context.op_config.get("folder") if context.op_config else None
    if folder_filter:
        projects = [{"folder": folder_filter}]
        context.log.info(f"BUILD 단일 프로젝트 모드: folder={folder_filter}")
    else:
        projects = db.find_projects_for_dataset_build()
    if not projects:
        context.log.info("BUILD 대상 프로젝트 없음")
        return {"projects": 0, "summaries": []}

    _ensure_dataset_root(minio)

    summaries: list[dict] = []
    errors: list[tuple[str, Exception]] = []
    for proj in projects:
        folder = proj["folder"]
        try:
            summaries.append(_build_project(context, db, minio, folder))
        except Exception as exc:
            # 폴더별 fail-forward 정책 유지: 다른 폴더 처리 계속.
            # 단, run 종료 시 errors 가 있으면 escalate 하여 job=FAILURE 표시.
            # 이렇게 해야 sensor cursor 가 success 로 박지 않고 RetryPolicy/재 finalize
            # 시 같은 후보를 다시 시도할 수 있다 (이전엔 swallow → SUCCESS → 영구 dedup).
            context.log.error(f"[{folder}] 빌드 실패: {exc}")
            summaries.append({"folder": folder, "error": str(exc)})
            errors.append((folder, exc))

    total = {
        "projects": len(summaries),
        "total_videos": sum(s.get("videos", 0) for s in summaries),
        "total_clips": sum(s.get("clips", 0) for s in summaries),
        "total_images": sum(s.get("images", 0) for s in summaries),
        "failed_folders": [f for f, _ in errors],
    }
    context.add_output_metadata(total)
    context.log.info(f"BUILD 전체 완료: {total}")

    if errors:
        first_folder, first_exc = errors[0]
        raise RuntimeError(
            f"build_dataset 실패 {len(errors)}/{len(summaries)} folder. 첫 실패: {first_folder} → {first_exc}"
        )
    return {**total, "summaries": summaries}
