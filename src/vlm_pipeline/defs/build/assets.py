"""BUILD @asset — 프로젝트별 타임스탬프+Bbox 데이터셋 조립.

프로젝트(source_unit_name) 단위로 timestamp JSON 있는 video와
bbox JSON 있는 image만 `vlm-dataset/<folder>/` 로 server-side copy.

Layer 4: Dagster @asset.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path, PurePosixPath
from uuid import uuid4

from dagster import Field, RetryPolicy, asset

from vlm_pipeline.lib.key_builders import (
    build_gemini_label_key,
    build_sam3_detection_key,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

DATASET_BUCKET = "vlm-dataset"
LABELS_BUCKET = "vlm-labels"
RAW_BUCKET = "vlm-raw"

# vlm-dataset NFS migration (B-narrow):
#   DATASET_STORAGE=fs (default) → NFS path (NFS_DATASET_ROOT 기준)
#   DATASET_STORAGE=minio        → 옛 MinIO 모드 (rollback용)
# DB의 dataset_bucket 값은 _dataset_bucket_value() 로 일관 결정.
DATASET_STORAGE = os.getenv("DATASET_STORAGE", "fs").lower()
NFS_DATASET_ROOT = os.getenv("NFS_DATASET_ROOT", "/nas/vlm_datasets")

# GenAI pair 빌드 — 출력 미디어별 dataset 하위 디렉토리.
_GENAI_OUTPUT_DIR = {"video": "videos", "image": "generated_images"}


def _require_ls_finalized() -> bool:
    """DATASET_REQUIRE_LS_FINALIZED — 기본 0 (필터 OFF, 레거시 호환)."""
    return os.getenv("DATASET_REQUIRE_LS_FINALIZED", "0") == "1"


def _dataset_bucket_value() -> str:
    """DB에 저장할 dataset_bucket 컬럼 값. fs 모드면 'fs', 아니면 'vlm-dataset'."""
    return "fs" if DATASET_STORAGE == "fs" else DATASET_BUCKET


def _ensure_dataset_root(minio: MinIOResource) -> None:
    """dataset 저장소 root 보장 (NFS 폴더 또는 MinIO 버킷)."""
    if DATASET_STORAGE == "fs":
        Path(NFS_DATASET_ROOT).mkdir(parents=True, exist_ok=True)
    else:
        minio.ensure_bucket(DATASET_BUCKET)


def _copy_to_nfs_if_outdated(minio: MinIOResource, src_bucket: str, src_key: str,
                             dst_path: Path, log) -> bool:
    """MinIO src → NFS dst_path. src size != dst size 면 재copy. atomic (tmp + rename)."""
    if dst_path.exists():
        src_head = minio.head(src_bucket, src_key)
        if src_head is not None and src_head.get("size") == dst_path.stat().st_size:
            return False
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    data = minio.download(src_bucket, src_key)
    tmp = dst_path.with_suffix(dst_path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, dst_path)
    log.debug(f"copy: {src_bucket}/{src_key} → fs:{dst_path}")
    return True


def _write_dataset_artifact(minio: MinIOResource, dst_key: str, data: bytes,
                            content_type: str) -> None:
    """dataset artifact (manifest.json 등) 저장. fs/minio 분기."""
    if DATASET_STORAGE == "fs":
        dst_path = Path(NFS_DATASET_ROOT) / dst_key
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst_path.with_suffix(dst_path.suffix + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, dst_path)
    else:
        minio.upload(DATASET_BUCKET, dst_key, data, content_type)


def _copy_if_outdated(minio: MinIOResource, src_bucket: str, src_key: str,
                      dst_key: str, log,
                      dst_bucket: str = DATASET_BUCKET) -> bool:
    # DATASET_STORAGE=fs (default) → NFS write
    # DATASET_STORAGE=minio        → MinIO server-side copy (legacy)
    if DATASET_STORAGE == "fs":
        dst_path = Path(NFS_DATASET_ROOT) / dst_key
        return _copy_to_nfs_if_outdated(minio, src_bucket, src_key, dst_path, log)
    # src ETag != dst ETag 일 때만 copy. dst 없거나 stale 이면 True.
    # dst 존재만 확인하던 기존 방식은 LS 재submit 으로 src 가 갱신돼도
    # vlm-dataset 에 옛 Gemini auto 버전이 영구히 남는 회귀를 일으켰다.
    dst_head = minio.head(dst_bucket, dst_key)
    if dst_head is not None:
        src_head = minio.head(src_bucket, src_key)
        if src_head is not None and src_head["etag"] == dst_head["etag"]:
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


def _genai_pairs_or_empty(db, folder: str, log=None) -> list[dict]:
    """find_project_genai_pairs 가 없는 백엔드(legacy DuckDB)면 빈 리스트.

    AttributeError 만 graceful 흡수 (legacy 백엔드 호환).
    그 외 예외(DB 연결 / SQL 오류)는 surface — silent swallow 하면 GenAI 폴더가
    일반 path 로 fallthrough 해서 빈 dataset 이 'completed' 로 마킹되는 위험.
    """
    fn = getattr(db, "find_project_genai_pairs", None)
    if not callable(fn):
        return []
    try:
        return fn(folder) or []
    except AttributeError:
        # 마이그레이션 미적용 등 메서드 자체 결손
        return []
    except Exception as exc:
        # DB 연결 오류 등은 명시적으로 재발생 — 일반 path 로 가지 않게 한다.
        if log is not None:
            log.error(f"[{folder}] find_project_genai_pairs 실패: {exc}")
        raise


def _genai_manifest_include_prompt() -> bool:
    """GENAI_MANIFEST_INCLUDE_PROMPT — 1 시 manifest 에 prompt 평문 포함.
    기본 0 (PII 회피, hash 만 저장)."""
    return os.getenv("GENAI_MANIFEST_INCLUDE_PROMPT", "0") == "1"


def _prompt_hash(prompt: str | None) -> str | None:
    if not prompt:
        return None
    import hashlib
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16]


def _build_project_genai(context, db, minio: MinIOResource,
                         folder: str, pairs: list[dict]) -> dict:
    """GenAI fan-out batch 의 paired dataset 빌드 (label-free).

    pairs 의 각 row = 1 batch_id + 1 seq_in_batch + (input image, output {video|image}).
    원본은 vlm-raw 에서 vlm-dataset/<folder_prefix>/images/<seq>.<ext> 로,
    결과는 vlm-dataset/<folder_prefix>/{videos|generated_images}/<seq>.<ext> 로 copy.

    실패 정책:
      - copy 실패 / output_media mismatch → skipped_pairs 누적, manifest 에 기록
      - 최종 성공 pair 0 건 → build_status='failed', dataset_id 반환
      - skipped > 0 & 일부 성공 → build_status='partial'
    """
    log = context.log

    # ---- 1. folder_prefix 추출 + invariant 검사 ----
    sample_input_key = pairs[0]["input_raw_key"]
    folder_prefix = _minio_prefix_from_key(sample_input_key)
    if not folder_prefix:
        log.error(f"[{folder}] genai input_raw_key 에서 folder prefix 추출 실패 — skip")
        return {"folder": folder, "error": "no_folder_prefix"}

    # 모든 pair 가 같은 folder_prefix 인지 확인 (서로 다른 batch 가 같은 source_unit_name 으로
    # 들어왔을 때 cross-contamination 방어)
    foreign = [
        p["job_id"] for p in pairs
        if _minio_prefix_from_key(p["input_raw_key"]) != folder_prefix
    ]
    if foreign:
        log.error(
            f"[{folder}] folder_prefix invariant 위반: {len(foreign)} pairs "
            f"prefix != {folder_prefix!r} (e.g. {foreign[:3]}) — skip"
        )
        return {"folder": folder, "error": "folder_prefix_mismatch", "foreign_count": len(foreign)}

    # ---- 2. batch metadata 수집 (config 보강용) ----
    batch_ids = sorted({p["batch_id"] for p in pairs})
    engines = sorted({p.get("engine") for p in pairs if p.get("engine")})
    prompt_hashes = sorted({_prompt_hash(p.get("prompt")) for p in pairs if p.get("prompt")})

    log.info(
        f"[{folder}] GenAI pair build: pairs={len(pairs)} prefix={folder_prefix} "
        f"batches={batch_ids} engines={engines}"
    )

    # ---- 3. dataset row 등록 ----
    dataset_id = str(uuid4())
    db.insert_dataset({
        "dataset_id": dataset_id,
        "name": folder,
        "version": "v1",
        "config": json.dumps({
            "schema": "genai_paired",
            "label_free": True,
            "with_timestamp": False,
            "with_bbox": False,
            "batch_ids": batch_ids,
            "engines": engines,
            "prompt_hashes": prompt_hashes,
            "source_unit_name": folder,
        }),
        "split_ratio": None,
        "dataset_bucket": _dataset_bucket_value(),
        "dataset_prefix": folder_prefix,
        "build_status": "building",
    })

    # ---- 4. pair 단위 copy ----
    include_prompt = _genai_manifest_include_prompt()
    pair_entries: list[dict] = []
    skipped_entries: list[dict] = []
    image_copies_new = 0
    output_copies_new = 0
    output_video_count = 0
    output_image_count = 0
    for pair in pairs:
        seq = int(pair["seq_in_batch"])
        input_raw_bucket = pair["input_raw_bucket"] or RAW_BUCKET
        input_raw_key = pair["input_raw_key"]
        output_raw_bucket = pair["output_raw_bucket"] or RAW_BUCKET
        output_raw_key = pair["output_raw_key"]

        in_ext = PurePosixPath(input_raw_key).suffix or ".png"
        out_ext = PurePosixPath(output_raw_key).suffix or ".bin"

        # output_media: batch.output_media (video|image)
        output_media = (pair.get("output_media") or "").lower()
        out_dir = _GENAI_OUTPUT_DIR.get(output_media)
        if not out_dir:
            log.error(f"[{folder}] pair {seq}: unknown output_media={output_media!r} — skip")
            skipped_entries.append({"job_id": pair["job_id"], "seq": seq,
                                     "reason": f"unknown_output_media:{output_media}"})
            continue

        # batch.output_media vs raw_files.media_type cross-validation
        actual_media = (pair.get("output_media_type") or "").lower()
        if actual_media and actual_media != output_media:
            log.error(
                f"[{folder}] pair {seq}: output_media mismatch "
                f"batch={output_media!r} vs raw_files.media_type={actual_media!r} — skip"
            )
            skipped_entries.append({"job_id": pair["job_id"], "seq": seq,
                                     "reason": f"output_media_mismatch:{output_media}!={actual_media}"})
            continue

        seq_str = f"{seq:03d}"
        dataset_image_key = f"{folder_prefix}/images/{seq_str}{in_ext}"
        dataset_output_key = f"{folder_prefix}/{out_dir}/{seq_str}{out_ext}"

        try:
            if _copy_if_outdated(minio, input_raw_bucket, input_raw_key, dataset_image_key, log):
                image_copies_new += 1
            if _copy_if_outdated(minio, output_raw_bucket, output_raw_key, dataset_output_key, log):
                output_copies_new += 1
        except Exception as exc:
            log.error(f"[{folder}] genai pair {seq} copy 실패: {exc}")
            skipped_entries.append({"job_id": pair["job_id"], "seq": seq,
                                     "reason": f"copy_failed:{type(exc).__name__}"})
            continue

        if output_media == "video":
            output_video_count += 1
        else:
            output_image_count += 1

        entry = {
            "pair_id": pair["job_id"],
            "batch_id": pair["batch_id"],
            "seq": seq,
            "source_key": dataset_image_key,
            "generated_key": dataset_output_key,
            "engine": pair.get("engine"),
            "prompt_hash": _prompt_hash(pair.get("prompt")),
            "label_free": True,
            "provider_job_id": pair.get("provider_job_id"),
            "cost_units": pair.get("cost_units"),
            "source_raw_bucket": input_raw_bucket,
            "source_raw_key": input_raw_key,
            "output_raw_bucket": output_raw_bucket,
            "output_raw_key": output_raw_key,
        }
        if include_prompt:
            entry["prompt"] = pair.get("prompt")
        pair_entries.append(entry)

    # ---- 5. build_status 결정 ----
    if not pair_entries:
        # 전부 실패 — manifest 작성 후 failed 마킹
        final_status = "failed"
    elif skipped_entries:
        final_status = "partial"
    else:
        final_status = "completed"

    # ---- 6. manifest.json ----
    manifest = {
        "project": folder,
        "folder_prefix": folder_prefix,
        "schema": "genai_paired",
        "schema_version": 1,
        "label_free": True,
        "built_at": datetime.utcnow().isoformat() + "Z",
        "bucket": _dataset_bucket_value(),
        "prefix": folder_prefix,
        "build_status": final_status,
        "counts": {
            "pairs": len(pair_entries),
            "images": len(pair_entries),
            "videos": output_video_count,
            "generated_images": output_image_count,
            "skipped": len(skipped_entries),
        },
        "batch_ids": batch_ids,
        "engines": engines,
        "pairs": pair_entries,
        "skipped_pairs": skipped_entries,
    }
    _write_dataset_artifact(
        minio,
        f"{folder_prefix}/manifest.json",
        json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        "application/json",
    )

    db.update_dataset_status(dataset_id, final_status)

    summary = {
        "folder": folder,
        "folder_prefix": folder_prefix,
        "dataset_id": dataset_id,
        "schema": "genai_paired",
        "build_status": final_status,
        "pairs": len(pair_entries),
        "skipped": len(skipped_entries),
        "videos": output_video_count,
        "generated_images": output_image_count,
        "image_copies_new": image_copies_new,
        "output_copies_new": output_copies_new,
    }
    log.info(f"[{folder}] genai {final_status}: {summary}")
    return summary


def _build_project(context, db: DuckDBResource, minio: MinIOResource,
                   folder: str) -> dict:
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
        "dataset_bucket": _dataset_bucket_value(),
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
            if _copy_if_outdated(minio, video["raw_bucket"], raw_key, dataset_video_key, log):
                video_copies_new += 1
            if _copy_if_outdated(minio, LABELS_BUCKET, expected_ts_key, dataset_ts_key, log):
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
            if _copy_if_outdated(minio, src_bucket, clip_key, dataset_clip_key, log):
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
            if _copy_if_outdated(minio, image["image_bucket"], image_key, dataset_image_key, log):
                image_copies_new += 1
            if _copy_if_outdated(minio, LABELS_BUCKET, expected_bbox_key, dataset_bbox_key, log):
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
    db: DuckDBResource,
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
            f"build_dataset 실패 {len(errors)}/{len(summaries)} folder. "
            f"첫 실패: {first_folder} → {first_exc}"
        )
    return {**total, "summaries": summaries}
