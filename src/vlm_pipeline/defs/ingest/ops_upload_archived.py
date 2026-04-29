"""Phase 2b: archive 경로의 파일을 MinIO 로 업로드 (raw_files 재INSERT 없음).

`archive_dispatch_sensor` 가 만든 manifest (`from_archived=True`) 를 받아
- archive_path 의 파일을 vlm-raw 로 업로드
- 재인코딩 필요 시 reencode_to_tmp 사용
- raw_files.ingest_status: 'archived' → 'completed', raw_bucket='vlm-raw' 마크

기존 `normalize_and_archive` 와 달리 register/dedup/ffprobe 를 다시 하지 않는다
(이미 Phase 2a 단계에서 raw_files 에 row 가 있고 video_metadata 추출은
별도 라벨링 stage 또는 follow-up PR 에서 통합 예정).
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from vlm_pipeline.lib.video_reencode import (
    STANDARD_PRESET_NAME,
    needs_reencode,
    reencode_to_tmp,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .ops_common import (
    DEFAULT_RAW_BUCKET,
    DEFAULT_REENCODE_THREADS,
    DEFAULT_REENCODE_WORKERS,
    DEFAULT_UPLOAD_WORKERS,
    DEFAULT_VIDEO_CONTENT_TYPE,
    MAX_UPLOAD_WORKERS,
)


def upload_archived_files(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
    manifest: dict,
) -> dict[str, Any]:
    """archive 경로 파일을 MinIO 로 업로드하고 raw_files 상태를 'completed' 로 마크.

    Returns:
        {uploaded: int, failed: int, skipped: int, asset_ids: [...]}
    """
    files = manifest.get("files") or []
    if not files:
        context.log.info("upload_archived: empty files list")
        return {"uploaded": 0, "failed": 0, "skipped": 0, "asset_ids": [], "total": 0}

    try:
        max_workers = int(os.getenv("INGEST_UPLOAD_WORKERS", str(DEFAULT_UPLOAD_WORKERS)))
    except ValueError:
        max_workers = DEFAULT_UPLOAD_WORKERS
    max_workers = max(1, min(MAX_UPLOAD_WORKERS, max_workers))

    try:
        reencode_workers = max(
            1,
            min(
                MAX_UPLOAD_WORKERS,
                int(os.getenv("INGEST_REENCODE_WORKERS", str(DEFAULT_REENCODE_WORKERS))),
            ),
        )
        reencode_threads = max(
            1, int(os.getenv("INGEST_REENCODE_THREADS", str(DEFAULT_REENCODE_THREADS)))
        )
    except ValueError:
        reencode_workers = DEFAULT_REENCODE_WORKERS
        reencode_threads = DEFAULT_REENCODE_THREADS

    def _upload_one(entry: dict[str, Any]) -> dict[str, Any]:
        archive_path = str(entry.get("path") or "").strip()
        asset_id = str(entry.get("asset_id") or "").strip()
        raw_key = str(entry.get("raw_key") or "").strip()
        media_type = str(entry.get("media_type") or "video").strip().lower()

        if not archive_path or not asset_id or not raw_key:
            return {"asset_id": asset_id, "ok": False, "reason": "missing_fields"}
        if not Path(archive_path).is_file():
            return {"asset_id": asset_id, "ok": False, "reason": f"missing_file:{archive_path}"}

        tmp_path: Path | None = None
        try:
            upload_src = archive_path
            if media_type == "video":
                meta = (
                    db.get_video_metadata(asset_id)
                    if hasattr(db, "get_video_metadata")
                    else None
                )
                reencode_needed = False
                if meta and meta.get("reencode_required") is not None:
                    reencode_needed = bool(meta.get("reencode_required"))
                else:
                    try:
                        rq, _rsn = needs_reencode(meta or {}, Path(archive_path))
                        reencode_needed = bool(rq)
                    except Exception:
                        reencode_needed = False
                if reencode_needed:
                    tmp_path = reencode_to_tmp(Path(archive_path), threads=reencode_threads)
                    upload_src = str(tmp_path)

            content_type = (
                DEFAULT_VIDEO_CONTENT_TYPE if media_type == "video" else "image/jpeg"
            )
            minio.upload_file(DEFAULT_RAW_BUCKET, raw_key, upload_src, content_type=content_type)
            return {
                "asset_id": asset_id,
                "ok": True,
                "raw_key": raw_key,
                "reencoded": tmp_path is not None,
            }
        except Exception as exc:  # noqa: BLE001
            return {"asset_id": asset_id, "ok": False, "reason": str(exc)}
        finally:
            if tmp_path is not None:
                try:
                    Path(tmp_path).unlink(missing_ok=True)
                except OSError:
                    pass

    has_reencode_candidates = any(
        str(e.get("media_type", "")).lower() == "video" for e in files
    )
    effective_workers = reencode_workers if has_reencode_candidates else max_workers

    uploaded_count = 0
    failed_count = 0
    skipped_count = 0
    asset_ids: list[str] = []

    with ThreadPoolExecutor(max_workers=effective_workers) as pool:
        futures = {pool.submit(_upload_one, entry): entry for entry in files}
        for fut in as_completed(futures):
            result = fut.result()
            if result.get("ok"):
                asset_id = result["asset_id"]
                # DB 상태 업데이트가 끝난 뒤에야 success 로 집계 — 업로드는 됐지만
                # status 갱신 실패한 케이스를 명확히 failed_count 로 표기.
                try:
                    db.update_raw_file_status(
                        asset_id,
                        "completed",
                        raw_bucket=DEFAULT_RAW_BUCKET,
                    )
                    if result.get("reencoded") and hasattr(
                        db, "update_video_reencode_applied"
                    ):
                        db.update_video_reencode_applied(
                            asset_id, reencode_preset=STANDARD_PRESET_NAME
                        )
                    uploaded_count += 1
                    asset_ids.append(asset_id)
                except Exception as exc:  # noqa: BLE001
                    failed_count += 1
                    context.log.warning(
                        f"upload_archived: status update failed asset_id={asset_id}: {exc} "
                        f"(MinIO 업로드 성공했으나 DB 미반영 — 후속 정리 필요)"
                    )
            else:
                reason = result.get("reason") or "unknown"
                if reason.startswith("missing_"):
                    skipped_count += 1
                else:
                    failed_count += 1
                context.log.warning(
                    f"upload_archived 실패: asset_id={result.get('asset_id')} reason={reason}"
                )

    summary = {
        "uploaded": uploaded_count,
        "failed": failed_count,
        "skipped": skipped_count,
        "asset_ids": asset_ids,
        "total": len(files),
    }
    context.log.info(f"upload_archived 완료: {summary}")
    return summary
