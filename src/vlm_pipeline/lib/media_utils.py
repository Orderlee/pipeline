"""공통 미디어 파일 유틸리티 — archive/MinIO 에서 로컬 경로 확보."""

from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any


def materialize_video_path(
    minio: Any,
    candidate: dict[str, Any],
) -> tuple[Path, Path | None]:
    """archive_path 우선, MinIO fallback으로 비디오를 로컬에 확보.

    Returns:
        (video_path, temp_path): temp_path가 None이면 archive를 직접 사용한 것.
    """
    archive_path = Path(str(candidate.get("archive_path") or "").strip())
    if archive_path.exists():
        return archive_path, None
    raw_bucket = str(candidate.get("raw_bucket") or "vlm-raw")
    raw_key = str(candidate.get("raw_key") or "")
    suffix = Path(raw_key).suffix or ".mp4"
    tmp = NamedTemporaryFile(delete=False, suffix=suffix)
    try:
        minio.download_fileobj(raw_bucket, raw_key, tmp)
    finally:
        tmp.close()
    return Path(tmp.name), Path(tmp.name)
