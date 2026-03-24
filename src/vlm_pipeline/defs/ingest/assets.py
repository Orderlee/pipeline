"""INGEST @asset — NAS 미디어 → 검증 → 정규화 → MinIO vlm-raw + DuckDB raw_files.

Layer 4: Dagster @asset, 같은 도메인의 ops.py만 import.
"""

import json
import os
import shutil
from datetime import datetime
from pathlib import Path

from dagster import AssetKey, asset

from vlm_pipeline.lib.phash import compute_phash
from vlm_pipeline.lib.validator import ALLOWED_EXTENSIONS
from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .archive import (
    archive_uploaded_assets,
    complete_uploaded_assets_in_archive,
    complete_uploaded_assets_without_archive,
    prepare_manifest_for_archive_upload,
    should_archive_manifest,
)
from .duplicate import collect_duplicate_asset_file_map
from .manifest import (
    build_retry_manifest,
    maybe_write_archive_done_marker,
    write_ingest_failure_logs,
)
from .ops import ingest_summary, normalize_and_archive, register_incoming


def _hydrate_manifest_files(context, manifest: dict) -> dict:
    """files가 비어 있으면 source_unit_path를 기준으로 실제 파일 목록을 지연 생성한다."""
    if manifest.get("files"):
        return manifest

    source_unit_path_raw = str(manifest.get("source_unit_path", "")).strip()
    source_unit_path = Path(source_unit_path_raw) if source_unit_path_raw else None
    if source_unit_path is None or not source_unit_path.exists():
        return manifest

    allowed_exts = {ext.lower() for ext in ALLOWED_EXTENSIONS}
    files: list[dict] = []

    def _scan_recursive(base: Path, rel_prefix: str = "") -> None:
        try:
            for entry in os.scandir(base):
                if entry.is_dir(follow_symlinks=False):
                    sub_rel = f"{rel_prefix}{entry.name}/" if rel_prefix else f"{entry.name}/"
                    _scan_recursive(Path(entry.path), sub_rel)
                elif entry.is_file(follow_symlinks=False):
                    if Path(entry.name).suffix.lower() in allowed_exts:
                        rel = f"{rel_prefix}{entry.name}" if rel_prefix else entry.name
                        files.append({"path": entry.path, "size": 0, "rel_path": rel})
        except OSError:
            return

    _scan_recursive(source_unit_path)
    manifest["files"] = files
    manifest["file_count"] = len(files)
    manifest["source_unit_total_file_count"] = len(files)
    context.log.info(
        "manifest 파일 인덱싱 완료: "
        f"source_unit_path={source_unit_path}, file_count={len(files)}"
    )
    return manifest


def _resolve_dedup_image_bytes(target: dict, minio: MinIOResource) -> tuple[bytes, str]:
    """inline DEDUP 입력 소스 우선순위: archive_path -> source_path -> MinIO."""
    for path_key in ("archive_path", "source_path"):
        local_path = target.get(path_key)
        if local_path and Path(local_path).is_file():
            return Path(local_path).read_bytes(), path_key

    raw_bucket = target.get("raw_bucket")
    raw_key = target.get("raw_key")
    if raw_bucket and raw_key:
        return minio.download(raw_bucket, raw_key), "minio"

    raise FileNotFoundError(
        f"No source available: archive_path={target.get('archive_path')}, "
        f"source_path={target.get('source_path')}, "
        f"raw_bucket={target.get('raw_bucket')}, raw_key={target.get('raw_key')}"
    )


def _load_inline_dedup_targets(
    db: DuckDBResource,
    *,
    prioritized_asset_ids: list[str],
    limit: int,
) -> list[dict]:
    """현재 manifest 자산을 우선 처리하고, 남는 슬롯은 기존 backlog로 채운다."""
    normalized_limit = max(1, int(limit))
    prioritized_targets: list[dict] = []
    prioritized_set = {str(asset_id).strip() for asset_id in prioritized_asset_ids if str(asset_id).strip()}

    with db.connect() as conn:
        columns = ["asset_id", "raw_bucket", "raw_key", "archive_path", "source_path"]

        if prioritized_set:
            placeholders = ", ".join("?" * len(prioritized_set))
            rows = conn.execute(
                f"""
                SELECT asset_id, raw_bucket, raw_key, archive_path, source_path
                FROM raw_files
                WHERE asset_id IN ({placeholders})
                  AND media_type = 'image'
                  AND ingest_status = 'completed'
                  AND phash IS NULL
                ORDER BY created_at
                """,
                list(prioritized_set),
            ).fetchall()
            prioritized_targets = [dict(zip(columns, row)) for row in rows]

        remaining_limit = max(0, normalized_limit - len(prioritized_targets))
        if remaining_limit <= 0:
            return prioritized_targets

        params: list[object] = []
        exclude_sql = ""
        if prioritized_set:
            placeholders = ", ".join("?" * len(prioritized_set))
            exclude_sql = f"AND asset_id NOT IN ({placeholders})"
            params.extend(list(prioritized_set))

        rows = conn.execute(
            f"""
            SELECT asset_id, raw_bucket, raw_key, archive_path, source_path
            FROM raw_files
            WHERE media_type = 'image'
              AND ingest_status = 'completed'
              AND phash IS NULL
              {exclude_sql}
            ORDER BY created_at
            LIMIT ?
            """,
            [*params, remaining_limit],
        ).fetchall()
        backlog_targets = [dict(zip(columns, row)) for row in rows]

    return prioritized_targets + backlog_targets


def _mark_inline_dedup_failure(db: DuckDBResource, asset_id: str, error_message: str) -> None:
    with db.connect() as conn:
        conn.execute(
            """
            UPDATE raw_files
            SET error_message = ?, updated_at = ?
            WHERE asset_id = ?
            """,
            [f"phash_failed:{error_message}", datetime.now(), asset_id],
        )


def _run_inline_dedup(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
    uploaded: list[dict],
) -> dict:
    """INGEST 내부 hard-gate DEDUP.

    현재 manifest에서 성공적으로 업로드된 이미지 자산을 우선 처리하고,
    남는 슬롯이 있으면 기존 phash backlog도 함께 정리한다.
    """
    prioritized_asset_ids = [
        str(item["asset_id"])
        for item in uploaded
        if str(item.get("media_type") or "").strip().lower() == "image"
    ]
    if not prioritized_asset_ids:
        return {"computed": 0, "similar_found": 0, "failed": 0, "gated_failed": 0}

    limit = max(len(prioritized_asset_ids), 200)
    threshold = 5
    targets = _load_inline_dedup_targets(
        db,
        prioritized_asset_ids=prioritized_asset_ids,
        limit=limit,
    )
    if not targets:
        return {"computed": 0, "similar_found": 0, "failed": 0, "gated_failed": 0}

    prioritized_set = set(prioritized_asset_ids)
    computed = 0
    similar_found = 0
    failed = 0
    gated_failed = 0

    for target in targets:
        asset_id = str(target["asset_id"])
        try:
            image_bytes, source_label = _resolve_dedup_image_bytes(target, minio)
            context.log.debug(f"inline DEDUP source: {asset_id} via {source_label}")

            phash_hex = compute_phash(image_bytes)
            db.update_phash(asset_id, phash_hex)
            db.clear_error_message(asset_id)
            computed += 1

            candidates = db.find_similar_phash(
                phash_hex=phash_hex,
                threshold=threshold,
                exclude_asset_id=asset_id,
            )
            if candidates:
                best = candidates[0]
                other_asset_id = str(best["asset_id"])
                dist = int(best["distance"])
                group_id = f"dup_{min(asset_id, other_asset_id)}_{max(asset_id, other_asset_id)}"
                db.update_dup_group(asset_id, group_id)
                db.update_dup_group(other_asset_id, group_id)
                similar_found += 1
                context.log.warning(
                    f"inline DEDUP 유사 이미지 발견: {asset_id} ↔ {other_asset_id} (distance={dist})"
                )
        except Exception as exc:  # noqa: BLE001
            failed += 1
            if asset_id in prioritized_set:
                gated_failed += 1
            _mark_inline_dedup_failure(db, asset_id, str(exc))
            context.log.error(f"inline DEDUP 실패: {asset_id}: {exc}")

    return {
        "computed": computed,
        "similar_found": similar_found,
        "failed": failed,
        "gated_failed": gated_failed,
    }


@asset(
    name="raw_ingest",
    description="NAS 미디어 검증·정규화 → MinIO(vlm-raw) 업로드 + 메타데이터 등록 + pHash 중복 검출",
    group_name="ingest",
    deps=[AssetKey(["pipeline", "incoming_nas"])],
)
def raw_ingest(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """INGEST asset — manifest 기반 미디어 파일 수집.

    sensor에서 run_config tags로 manifest_path를 전달받음.
    manifest가 없으면 DuckDB 상태 요약만 반환.
    """
    config = PipelineConfig()
    db.ensure_schema()

    manifest_path = context.run.tags.get("manifest_path")

    if manifest_path and Path(manifest_path).exists():
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    else:
        context.log.warning("manifest_path가 없거나 파일이 존재하지 않습니다. 상태 요약만 반환합니다.")
        with db.connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM raw_files").fetchone()[0]
            completed = conn.execute(
                "SELECT COUNT(*) FROM raw_files WHERE ingest_status = 'completed'"
            ).fetchone()[0]
        return {"total": int(total), "success": int(completed), "failed": 0, "skipped": 0}

    archive_requested = should_archive_manifest(manifest, config=config)
    archive_unit_dir_hint = None
    archive_prepared_for_upload = False
    if archive_requested:
        manifest, archive_unit_dir_hint, archive_prepared_for_upload = prepare_manifest_for_archive_upload(
            context,
            manifest,
            archive_dir=config.archive_dir,
        )
        if archive_prepared_for_upload and manifest_path:
            Path(manifest_path).write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    manifest = _hydrate_manifest_files(context, manifest)
    if manifest_path:
        Path(manifest_path).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    ingest_rejections: list[dict] = []
    retry_candidates: list[dict] = []

    records = register_incoming(
        context, db, manifest,
        ingest_rejections=ingest_rejections,
    )

    target_archive_dir = config.archive_dir

    uploaded = normalize_and_archive(
        context, db, minio, records, target_archive_dir,
        ingest_rejections=ingest_rejections,
        retry_candidates=retry_candidates,
    )

    if archive_requested:
        if archive_prepared_for_upload:
            archived = complete_uploaded_assets_in_archive(
                context=context,
                db=db,
                manifest=manifest,
                uploaded=uploaded,
            )
        else:
            archived, archive_unit_dir_hint = archive_uploaded_assets(
                context=context, db=db, manifest=manifest,
                uploaded=uploaded, archive_dir=target_archive_dir,
                ingest_rejections=ingest_rejections,
            )
    else:
        archived = complete_uploaded_assets_without_archive(
            context=context,
            db=db,
            manifest=manifest,
            uploaded=uploaded,
        )
        archive_unit_dir_hint = None

    retry_manifest_path = build_retry_manifest(
        context=context, config=config,
        manifest=manifest, retry_candidates=retry_candidates,
    )
    failure_log_path = write_ingest_failure_logs(
        context=context, config=config,
        manifest=manifest, ingest_rejections=ingest_rejections,
    )

    duplicate_targets = collect_duplicate_asset_file_map(records)
    if duplicate_targets:
        duplicate_files_count = sum(
            1
            for rec in records
            if str((rec.get("record") or {}).get("error_message") or "").startswith("duplicate_of:")
        )
        updated_assets = db.mark_duplicate_skipped_assets(duplicate_targets)
        context.log.warning(
            "중복 스킵 이력 반영(자산별): "
            f"duplicate_assets={len(duplicate_targets)}, "
            f"duplicate_files={duplicate_files_count}, "
            f"updated_assets={updated_assets}"
        )

    dedup_summary = _run_inline_dedup(context, db, minio, uploaded)

    archive_done_marker_path = maybe_write_archive_done_marker(
        context=context, db=db, manifest=manifest,
        manifest_dir=config.manifest_dir,
        manifest_path=manifest_path,
        archive_dir=target_archive_dir,
        archive_unit_dir_hint=archive_unit_dir_hint,
    )

    summary = ingest_summary(context, archived, records)
    uploaded_count = len(uploaded)
    archived_count = len(archived)
    archive_missing_count = max(0, uploaded_count - archived_count)
    summary.update(
        {
            "uploaded_count": uploaded_count,
            "archived_count": archived_count,
            "archive_missing_count": archive_missing_count,
            "archive_requested": archive_requested,
            "ingest_rejection_count": len(ingest_rejections),
            "retry_candidate_count": len(retry_candidates),
            "retry_manifest_created": bool(retry_manifest_path),
            "archive_done_marker_present": bool(archive_done_marker_path),
            "dedup_computed": dedup_summary["computed"],
            "dedup_similar_found": dedup_summary["similar_found"],
            "dedup_failed": dedup_summary["failed"],
        }
    )
    output_metadata = {
        "uploaded_count": uploaded_count,
        "archived_count": archived_count,
        "archive_missing_count": archive_missing_count,
        "archive_requested": archive_requested,
        "ingest_rejection_count": len(ingest_rejections),
        "retry_candidate_count": len(retry_candidates),
        "retry_manifest_created": bool(retry_manifest_path),
        "archive_done_marker_present": bool(archive_done_marker_path),
        "dedup_computed": dedup_summary["computed"],
        "dedup_similar_found": dedup_summary["similar_found"],
        "dedup_failed": dedup_summary["failed"],
    }
    if retry_manifest_path is not None:
        output_metadata["retry_manifest_path"] = str(retry_manifest_path)
    if failure_log_path is not None:
        output_metadata["failure_log_path"] = str(failure_log_path)
    if archive_done_marker_path is not None:
        output_metadata["archive_done_marker_path"] = str(archive_done_marker_path)
    context.add_output_metadata(output_metadata)

    if dedup_summary["gated_failed"] > 0:
        raise RuntimeError(
            "inline DEDUP failed for current manifest assets: "
            f"failed={dedup_summary['gated_failed']}, "
            f"computed={dedup_summary['computed']}, "
            f"similar_found={dedup_summary['similar_found']}"
        )

    manifest_file = Path(manifest_path)
    processed_dir = Path(config.manifest_dir) / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(manifest_file), str(processed_dir / manifest_file.name))
    except Exception as exc:
        context.log.warning(f"manifest 이동 실패: {exc}")

    return summary
