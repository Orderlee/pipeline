"""Inline DEDUP — INGEST 내부 pHash 기반 중복 검출."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from vlm_pipeline.lib.phash import compute_phash
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

INLINE_DEDUP_BACKLOG_MIN: int = 200
INLINE_DEDUP_PHASH_THRESHOLD: int = 5


def resolve_dedup_image_bytes(target: dict, minio: MinIOResource) -> tuple[bytes, str]:
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


def load_inline_dedup_targets(
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


def mark_inline_dedup_failure(db: DuckDBResource, asset_id: str, error_message: str) -> None:
    with db.connect() as conn:
        conn.execute(
            """
            UPDATE raw_files
            SET error_message = ?, updated_at = ?
            WHERE asset_id = ?
            """,
            [f"phash_failed:{error_message}", datetime.now(), asset_id],
        )


def run_inline_dedup(
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

    limit = max(len(prioritized_asset_ids), INLINE_DEDUP_BACKLOG_MIN)
    threshold = INLINE_DEDUP_PHASH_THRESHOLD
    targets = load_inline_dedup_targets(
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
            image_bytes, source_label = resolve_dedup_image_bytes(target, minio)
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
            mark_inline_dedup_failure(db, asset_id, str(exc))
            context.log.error(f"inline DEDUP 실패: {asset_id}: {exc}")

    return {
        "computed": computed,
        "similar_found": similar_found,
        "failed": failed,
        "gated_failed": gated_failed,
    }
