"""BUILD domain 공유 헬퍼 — storage 분기·copy·경로 유틸.

Layer 3: 순수 Python + MinIOResource 사용. Dagster @asset 없음.
assets.py / build_genai.py 양쪽에서 import.
"""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath

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


def _dataset_bucket_value() -> str:
    """DB에 저장할 dataset_bucket 컬럼 값. fs 모드면 'fs', 아니면 'vlm-dataset'."""
    return "fs" if DATASET_STORAGE == "fs" else DATASET_BUCKET


def _ensure_dataset_root(minio: MinIOResource) -> None:
    """dataset 저장소 root 보장 (NFS 폴더 또는 MinIO 버킷)."""
    if DATASET_STORAGE == "fs":
        Path(NFS_DATASET_ROOT).mkdir(parents=True, exist_ok=True)
    else:
        minio.ensure_bucket(DATASET_BUCKET)


def _copy_to_nfs_if_outdated(minio: MinIOResource, src_bucket: str, src_key: str, dst_path: Path, log) -> bool:
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


def _write_dataset_artifact(minio: MinIOResource, dst_key: str, data: bytes, content_type: str) -> None:
    """dataset artifact (manifest.json 등) 저장. fs/minio 분기."""
    if DATASET_STORAGE == "fs":
        dst_path = Path(NFS_DATASET_ROOT) / dst_key
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst_path.with_suffix(dst_path.suffix + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, dst_path)
    else:
        minio.upload(DATASET_BUCKET, dst_key, data, content_type)


def _copy_if_outdated(
    minio: MinIOResource, src_bucket: str, src_key: str, dst_key: str, log, dst_bucket: str = DATASET_BUCKET
) -> bool:
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
