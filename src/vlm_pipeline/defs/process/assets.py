"""PROCESS @asset — INGEST+LABEL 모두 completed인 건 대상.

Layer 4: Dagster @asset.
MVP: pass-through (이미지 그대로 S3 copy + PIL 메타 추출).
"""

from __future__ import annotations

from io import BytesIO
from uuid import uuid4

from dagster import Field, asset
from PIL import Image

from vlm_pipeline.lib.checksum import sha256_bytes
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@asset(
    deps=["ingested_raw_files", "labeled_files"],
    description="PROCESS — vlm-raw → vlm-processed S3 copy + PIL 메타 → processed_clips",
    group_name="process",
    config_schema={"limit": Field(int, default_value=1000)},
)
def processed_clips(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """INGEST+LABEL 모두 completed → vlm-processed로 S3 copy + processed_clips INSERT.

    MVP: pass-through (이미지 그대로, width/height/codec 포함).
    """
    candidates = db.find_processable()
    if not candidates:
        context.log.info("PROCESS 대상 없음")
        return {"processed": 0, "failed": 0}

    limit = int(context.op_config.get("limit", 1000))
    candidates = candidates[:limit]

    processed = 0
    failed = 0

    for cand in candidates:
        asset_id = cand["asset_id"]
        raw_bucket = cand["raw_bucket"]
        raw_key = cand["raw_key"]
        media_type = cand["media_type"]
        label_id = cand["label_id"]
        labels_key = cand["labels_key"]

        try:
            clip_id = str(uuid4())

            # PIL로 width, height, codec 추출 (이미지만)
            clip_key = raw_key  # pass-through: 동일 키
            width, height, codec = None, None, None
            file_bytes = None
            if media_type == "image":
                file_bytes = minio.download(raw_bucket, raw_key)
                with Image.open(BytesIO(file_bytes)) as img:
                    width, height = img.size
                    codec = (img.format or "jpeg").lower()

            # vlm-raw → vlm-processed S3 copy
            minio.copy(raw_bucket, raw_key, "vlm-processed", clip_key)

            # 라벨도 vlm-processed로 복사
            if labels_key:
                label_dest_key = labels_key
                minio.copy("vlm-labels", labels_key, "vlm-processed", label_dest_key)

            checksum = sha256_bytes(file_bytes) if file_bytes else None
            file_size = len(file_bytes) if file_bytes else None

            # processed_clips INSERT
            db.insert_processed_clip({
                "clip_id": clip_id,
                "source_asset_id": asset_id,
                "source_label_id": label_id,
                "event_index": 0,
                "checksum": checksum,
                "file_size": file_size,
                "processed_bucket": "vlm-processed",
                "clip_key": clip_key,
                "label_key": labels_key,
                "width": width,
                "height": height,
                "codec": codec,
                "process_status": "completed",
            })
            processed += 1

        except Exception as e:
            context.log.error(f"PROCESS 실패: {asset_id}: {e}")
            failed += 1

    summary = {"processed": processed, "failed": failed}
    context.add_output_metadata(summary)
    context.log.info(f"PROCESS 완료: {summary}")
    return summary
