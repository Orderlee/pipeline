"""DEDUP @asset — 비동기 pHash 유사도 검출.

Layer 4: Dagster @asset.
★ INGEST와 독립 — 실패해도 데이터 무결성 보장.
"""

from __future__ import annotations

from pathlib import Path

from dagster import Field, asset

from vlm_pipeline.lib.phash import compute_phash
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


def _resolve_image_bytes(
    target: dict,
    minio: MinIOResource,
) -> tuple[bytes, str]:
    """pHash 입력 소스 우선순위: 1) archive_path 2) source_path 3) MinIO fallback.

    Returns (image_bytes, source_label).
    """
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


@asset(
    deps=["ingested_raw_files"],
    description="비동기 pHash 유사도 검출 — INGEST 완료 후 자동 트리거",
    group_name="dedup",
    config_schema={
        "limit": Field(int, default_value=200),
        "threshold": Field(int, default_value=5),
    },
)
def dedup_results(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """pHash 계산 → Hamming distance ≤ threshold 검출 → dup_group_id 부여.

    DuckDB에서 phash IS NULL인 이미지를 조회하고, 로컬 파일(archive_path → source_path)
    우선으로 이미지를 읽어 pHash를 계산한다. 로컬 파일이 없으면 MinIO fallback.
    """
    cfg = context.op_config
    limit = int(cfg.get("limit", 200))
    threshold = int(cfg.get("threshold", 5))

    targets = db.find_phash_null(limit=limit)
    if not targets:
        context.log.info("pHash 계산 대상 없음")
        return {"computed": 0, "similar_found": 0}

    computed = 0
    similar_found = 0

    for target in targets:
        asset_id = target["asset_id"]

        try:
            image_bytes, source_label = _resolve_image_bytes(target, minio)
            context.log.debug(f"pHash source: {asset_id} via {source_label}")

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
                other_asset_id = best["asset_id"]
                dist = best["distance"]
                group_id = f"dup_{min(asset_id, other_asset_id)}_{max(asset_id, other_asset_id)}"
                db.update_dup_group(asset_id, group_id)
                db.update_dup_group(other_asset_id, group_id)
                similar_found += 1
                context.log.warning(
                    f"유사 이미지 발견: {asset_id} ↔ {other_asset_id} (distance={dist})"
                )

        except Exception as e:
            context.log.error(f"pHash 계산 실패: {asset_id}: {e}")

    summary = {"computed": computed, "similar_found": similar_found}
    context.add_output_metadata(summary)
    context.log.info(f"DEDUP 완료: {summary}")
    return summary
