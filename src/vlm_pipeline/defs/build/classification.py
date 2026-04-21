"""BUILD @asset — 카테고리별 폴더 트리로 원본 video + bbox image 복사.

프로젝트(source_unit_name) 단위로:
  - video: Gemini event JSON 의 category 별 폴더에 원본(vlm-raw)을 서버사이드 복사
  - image: SAM3 COCO JSON 의 categories 별 폴더에 bbox 추출본(vlm-processed)을 복사
  - fallback: dispatch.labeling_method='classification_image' 이고 image_metadata 비어있는
    video 에 대해 프레임 추출 + SAM3 호출 후 classification 루프에 재진입

출력 버킷: vlm-classification/<folder_prefix>/{video,image}/<category>/<rel_stem>.<ext>

Layer 4: Dagster @asset.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import PurePosixPath
from uuid import uuid4

from dagster import asset

from vlm_pipeline.defs.build.assets import (
    _copy_if_absent,
    _minio_prefix_from_key,
    _rel_stem_path,
    _require_ls_finalized,
)
from vlm_pipeline.defs.process.helpers import _materialize_video_path
from vlm_pipeline.defs.process.raw_frames import extract_raw_video_frames
from vlm_pipeline.lib.detection_common import parse_tag_list
from vlm_pipeline.lib.file_loader import cleanup_temp_path
from vlm_pipeline.lib.sam3 import get_sam3_client
from vlm_pipeline.lib.sam3_labeling import run_sam3_and_build_label_row
from vlm_pipeline.lib.sanitizer import sanitize_path_component
from vlm_pipeline.lib.vertex_chunking import normalize_gemini_events
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

CLASSIFICATION_BUCKET = "vlm-classification"
LABELS_BUCKET = "vlm-labels"
PROCESSED_BUCKET = "vlm-processed"


# ---------------------------------------------------------------------------
# JSON 파싱
# ---------------------------------------------------------------------------

def _extract_video_categories(json_bytes: bytes) -> set[str]:
    """Gemini event JSON 에서 unique category 세트 추출."""
    try:
        payload = json.loads(json_bytes.decode("utf-8"))
    except Exception:
        return set()
    events = normalize_gemini_events(payload)
    return {
        str(e.get("category") or "").strip()
        for e in events
        if e.get("category")
    }


def _extract_image_categories(json_bytes: bytes) -> set[str]:
    """SAM3 COCO JSON 에서 categories[].name 세트 추출.

    COCO 형식 우선, 없으면 detections[].prompt_class fallback.
    """
    try:
        data = json.loads(json_bytes.decode("utf-8"))
    except Exception:
        return set()
    names: set[str] = set()
    for c in (data.get("categories") or []):
        name = str(c.get("name") or "").strip()
        if name:
            names.add(name)
    if not names:
        for d in (data.get("detections") or []):
            name = str(d.get("prompt_class") or "").strip()
            if name:
                names.add(name)
    annotations = data.get("annotations") or []
    if not names and annotations:
        # categories 가 id 만 있고 name 이 누락된 케이스를 최소 대비
        cat_by_id: dict[int, str] = {}
        for c in (data.get("categories") or []):
            cid = c.get("id")
            cname = str(c.get("name") or "").strip()
            if cid is not None and cname:
                cat_by_id[int(cid)] = cname
        for ann in annotations:
            cid = ann.get("category_id")
            if cid is None:
                continue
            name = cat_by_id.get(int(cid))
            if name:
                names.add(name)
    return names


# ---------------------------------------------------------------------------
# 카테고리별 복사
# ---------------------------------------------------------------------------

def _copy_video_per_category(
    minio: MinIOResource,
    video: dict,
    categories: set[str],
    folder_prefix: str,
    log,
) -> tuple[int, list[str]]:
    """하나의 video 를 각 category 폴더에 복사. (신규 copy 수, safe_cat 리스트)."""
    raw_key = video["raw_key"]
    ext = PurePosixPath(raw_key).suffix or ".mp4"
    rel_stem = _rel_stem_path(raw_key, folder_prefix)
    new_copies = 0
    safe_cats: list[str] = []
    for cat in categories:
        safe_cat = sanitize_path_component(cat)
        if not safe_cat or safe_cat == "unnamed":
            continue
        safe_cats.append(safe_cat)
        dst_key = f"{folder_prefix}/video/{safe_cat}/{rel_stem}{ext}"
        try:
            if _copy_if_absent(
                minio, video["raw_bucket"], raw_key, dst_key, log,
                dst_bucket=CLASSIFICATION_BUCKET,
            ):
                new_copies += 1
        except Exception as exc:
            log.error(
                f"classification video copy 실패: raw_key={raw_key} cat={safe_cat}: {exc}"
            )
    return new_copies, safe_cats


def _copy_image_per_category(
    minio: MinIOResource,
    image: dict,
    categories: set[str],
    folder_prefix: str,
    log,
) -> tuple[int, list[str]]:
    """하나의 image 를 각 category 폴더에 복사."""
    image_key = image["image_key"]
    ext = PurePosixPath(image_key).suffix or ".jpg"
    rel_stem = _rel_stem_path(image_key, folder_prefix)
    new_copies = 0
    safe_cats: list[str] = []
    for cat in categories:
        safe_cat = sanitize_path_component(cat)
        if not safe_cat or safe_cat == "unnamed":
            continue
        safe_cats.append(safe_cat)
        dst_key = f"{folder_prefix}/image/{safe_cat}/{rel_stem}{ext}"
        try:
            if _copy_if_absent(
                minio, image["image_bucket"], image_key, dst_key, log,
                dst_bucket=CLASSIFICATION_BUCKET,
            ):
                new_copies += 1
        except Exception as exc:
            log.error(
                f"classification image copy 실패: image_key={image_key} cat={safe_cat}: {exc}"
            )
    return new_copies, safe_cats


# ---------------------------------------------------------------------------
# Fallback: raw video → frame 추출 + SAM3 호출 + image_metadata/image_labels insert
# ---------------------------------------------------------------------------

def _fallback_classify_video(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
    video: dict,
    dispatch_row: dict | None,
    folder_prefix: str,
) -> list[dict]:
    """image_metadata 가 없는 video 에 대해 프레임 추출 + SAM3 호출 → image 후보 반환."""
    log = context.log
    asset_id = video["asset_id"]
    raw_bucket = video.get("raw_bucket") or "vlm-raw"
    raw_key = video["raw_key"]

    prompt_classes = parse_tag_list((dispatch_row or {}).get("categories"))
    if not prompt_classes:
        log.warning(
            f"[fallback] dispatch.categories 비어있음 — asset={asset_id} skip"
        )
        return []

    duration_sec = float(video.get("duration_sec") or 0.0)
    if duration_sec <= 0:
        log.warning(f"[fallback] invalid duration — asset={asset_id} skip")
        return []

    fps_raw = video.get("fps")
    fps = float(fps_raw) if fps_raw else None
    frame_count = int(video.get("frame_count") or 0)

    client = get_sam3_client()
    if not client.is_ready():
        if not client.wait_until_ready(max_wait_sec=60):
            log.error(f"[fallback] SAM3 서버 미준비 — asset={asset_id} skip")
            return []

    temp_video_path = None
    uploaded_frame_keys: list[str] = []
    try:
        video_path, temp_video_path = _materialize_video_path(
            minio,
            {"raw_bucket": raw_bucket, "raw_key": raw_key},
        )

        frame_rows, uploaded_frame_keys = extract_raw_video_frames(
            minio,
            asset_id=asset_id,
            video_path=video_path,
            raw_key=raw_key,
            duration_sec=duration_sec,
            fps=fps,
            frame_count=frame_count,
            max_frames=16,
            jpeg_quality=90,
            image_profile="current",
            frame_interval_sec=None,
        )
        db.replace_video_frame_metadata(
            asset_id, frame_rows, image_role="raw_video_frame",
        )

        candidates: list[dict] = []
        label_rows: list[dict] = []
        detected_at = datetime.now()

        for row in frame_rows:
            image_id = row["image_id"]
            image_key = row["image_key"]
            try:
                img_bytes = minio.download(PROCESSED_BUCKET, image_key)
            except Exception as exc:
                log.error(f"[fallback] image download 실패: {image_key}: {exc}")
                continue

            try:
                label_row, _ann_count = run_sam3_and_build_label_row(
                    client=client,
                    minio=minio,
                    image_id=image_id,
                    image_key=image_key,
                    image_bytes=img_bytes,
                    image_width=row.get("width"),
                    image_height=row.get("height"),
                    prompts=prompt_classes,
                    class_source="classification_fallback",
                    detected_at=detected_at,
                    labels_bucket=LABELS_BUCKET,
                )
            except Exception as exc:
                log.error(f"[fallback] SAM3 segment 실패: {image_key}: {exc}")
                continue

            label_rows.append(label_row)
            candidates.append({
                "image_id": image_id,
                "image_bucket": PROCESSED_BUCKET,
                "image_key": image_key,
                "source_asset_id": asset_id,
                "labels_key_list": [label_row["labels_key"]],
            })

        if label_rows:
            db.batch_insert_image_labels(label_rows)

        log.info(
            f"[fallback] asset={asset_id} frames={len(frame_rows)} "
            f"image_candidates={len(candidates)}"
        )
        return candidates

    except Exception as exc:
        log.error(f"[fallback] 전체 실패: asset={asset_id}: {exc}")
        return []
    finally:
        cleanup_temp_path(temp_video_path)


# ---------------------------------------------------------------------------
# 프로젝트 단위 빌드
# ---------------------------------------------------------------------------

def _build_classification_for_project(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
    folder: str,
    dispatch_row: dict | None,
) -> dict:
    log = context.log
    require_finalized = _require_ls_finalized()

    videos = db.find_project_classification_videos(
        folder, require_ls_finalized=require_finalized
    )
    images = db.find_project_classification_images(
        folder, require_ls_finalized=require_finalized
    )
    log.info(
        f"[{folder}] classification 후보: videos={len(videos)} images={len(images)} "
        f"(require_ls_finalized={require_finalized})"
    )

    sample_key = (
        videos[0]["raw_key"] if videos
        else (images[0]["image_key"] if images else None)
    )
    if sample_key is None:
        # fallback 확인: 영상 후보가 있을 수 있음
        missing_videos = db.find_project_video_candidates_missing_images(folder)
        if missing_videos:
            sample_key = missing_videos[0]["raw_key"]

    if not sample_key:
        log.info(f"[{folder}] 후보 없음 — skip")
        return {"folder": folder, "videos": 0, "images": 0, "categories": 0, "skipped": True}

    folder_prefix = _minio_prefix_from_key(sample_key)
    if not folder_prefix:
        log.error(f"[{folder}] folder_prefix 추출 실패 — skip")
        return {"folder": folder, "error": "no_folder_prefix"}

    # ---- dataset row 기록 ----
    dataset_id = str(uuid4())
    labeling_method = (dispatch_row or {}).get("labeling_method") or ""
    db.insert_classification_dataset({
        "dataset_id": dataset_id,
        "name": folder,
        "folder_prefix": folder_prefix,
        "config": json.dumps({
            "labeling_method": labeling_method,
            "require_ls_finalized": require_finalized,
        }),
        "classification_bucket": CLASSIFICATION_BUCKET,
        "build_status": "building",
    })

    # ---- Video 복사 ----
    video_entries: list[dict] = []
    video_copies_new = 0
    all_categories: set[str] = set()

    for video in videos:
        cats: set[str] = set()
        for labels_key in (video.get("labels_key_list") or []):
            if not labels_key:
                continue
            try:
                data = minio.download(LABELS_BUCKET, labels_key)
            except Exception as exc:
                log.warning(f"video labels download 실패: {labels_key}: {exc}")
                continue
            cats |= _extract_video_categories(data)
        if not cats:
            continue
        new_copies, safe_cats = _copy_video_per_category(
            minio, video, cats, folder_prefix, log,
        )
        video_copies_new += new_copies
        all_categories.update(safe_cats)
        video_entries.append({
            "asset_id": video["asset_id"],
            "raw_key": video["raw_key"],
            "categories": sorted(safe_cats),
        })

    # ---- Fallback: classification_image 이고 이미지 없으면 ----
    fallback_images: list[dict] = []
    fallback_used = False
    if (
        "classification_image" in labeling_method
        and not images
    ):
        missing_videos = db.find_project_video_candidates_missing_images(folder)
        if missing_videos:
            log.info(
                f"[{folder}] fallback 진입: classification_image 이고 image 후보 없음 "
                f"(missing_videos={len(missing_videos)})"
            )
            fallback_used = True
            for mv in missing_videos:
                fallback_images.extend(
                    _fallback_classify_video(
                        context, db, minio, mv, dispatch_row, folder_prefix,
                    )
                )

    # ---- Image 복사 (원본 images + fallback 생성분) ----
    all_images = list(images) + fallback_images
    image_entries: list[dict] = []
    image_copies_new = 0

    for image in all_images:
        cats: set[str] = set()
        for labels_key in (image.get("labels_key_list") or []):
            if not labels_key:
                continue
            try:
                data = minio.download(LABELS_BUCKET, labels_key)
            except Exception as exc:
                log.warning(f"image labels download 실패: {labels_key}: {exc}")
                continue
            cats |= _extract_image_categories(data)
        if not cats:
            continue
        new_copies, safe_cats = _copy_image_per_category(
            minio, image, cats, folder_prefix, log,
        )
        image_copies_new += new_copies
        all_categories.update(safe_cats)
        image_entries.append({
            "image_id": image["image_id"],
            "image_key": image["image_key"],
            "categories": sorted(safe_cats),
        })

    # ---- manifest.json ----
    manifest = {
        "project": folder,
        "folder_prefix": folder_prefix,
        "built_at": datetime.utcnow().isoformat() + "Z",
        "bucket": CLASSIFICATION_BUCKET,
        "prefix": folder_prefix,
        "labeling_method": labeling_method,
        "fallback_used": fallback_used,
        "counts": {
            "videos": len(video_entries),
            "images": len(image_entries),
            "categories": len(all_categories),
        },
        "categories": sorted(all_categories),
        "videos": video_entries,
        "images": image_entries,
    }
    minio.upload(
        CLASSIFICATION_BUCKET,
        f"{folder_prefix}/manifest.json",
        json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        "application/json",
    )

    db.update_classification_dataset_status(
        dataset_id,
        "completed",
        video_count=len(video_entries),
        image_count=len(image_entries),
        category_count=len(all_categories),
    )

    summary = {
        "folder": folder,
        "folder_prefix": folder_prefix,
        "dataset_id": dataset_id,
        "videos": len(video_entries),
        "images": len(image_entries),
        "categories": len(all_categories),
        "video_copies_new": video_copies_new,
        "image_copies_new": image_copies_new,
        "fallback_used": fallback_used,
    }
    log.info(f"[{folder}] classification 완료: {summary}")
    return summary


# ---------------------------------------------------------------------------
# Dagster asset
# ---------------------------------------------------------------------------

@asset(
    deps=["clip_to_frame"],
    description=(
        "프로젝트별 카테고리 폴더로 원본 video + bbox image 복사. "
        "Gemini event category → video, SAM3 COCO categories → image. "
        "dispatch.labeling_method='classification_image' 이고 image_metadata 비어있으면 "
        "raw video 에서 프레임 추출 + SAM3 호출 fallback. 멱등."
    ),
    group_name="build",
)
def build_classification(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    projects = db.find_projects_for_classification_build()
    if not projects:
        context.log.info("CLASSIFICATION 대상 프로젝트 없음")
        return {"projects": 0, "summaries": []}

    minio.ensure_bucket(CLASSIFICATION_BUCKET)

    summaries: list[dict] = []
    for proj in projects:
        folder = proj["folder_name"]
        dispatch_row = {
            "labeling_method": proj.get("labeling_method") or "",
            "categories": proj.get("categories"),
            "classes": proj.get("classes"),
        }
        try:
            summaries.append(
                _build_classification_for_project(
                    context, db, minio, folder, dispatch_row,
                )
            )
        except Exception as exc:
            context.log.error(f"[{folder}] classification 빌드 실패: {exc}")
            summaries.append({"folder": folder, "error": str(exc)})

    total = {
        "projects": len(summaries),
        "total_videos": sum(s.get("videos", 0) for s in summaries),
        "total_images": sum(s.get("images", 0) for s in summaries),
        "total_categories": sum(s.get("categories", 0) for s in summaries),
    }
    context.add_output_metadata(total)
    context.log.info(f"CLASSIFICATION 전체 완료: {total}")
    return {**total, "summaries": summaries}
