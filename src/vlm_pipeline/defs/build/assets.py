"""BUILD @asset — 데이터셋 조립 (train/val/test 분할).

Layer 4: Dagster @asset.
"""

from __future__ import annotations

import json
from pathlib import Path
import random
from uuid import uuid4

from dagster import asset

from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@asset(
    deps=["clip_to_frame"],
    description="processed clips → train/val/test로 분할 → MinIO(vlm-dataset) 데이터셋 조립",
    group_name="build",
)
def build_dataset(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """데이터셋 조립 — train/val/test 80:10:10 분할."""

    candidates = db.find_dataset_candidates()
    if not candidates:
        context.log.info("BUILD 대상 없음")
        return {"dataset_id": None, "total": 0, "train": 0, "val": 0, "test": 0}

    # 데이터셋 생성
    dataset_id = str(uuid4())
    dataset_name = "mvp-dataset"
    dataset_version = "v0.1"
    dataset_prefix = f"{dataset_name}/{dataset_version}"
    split_ratio = {"train": 0.8, "val": 0.1, "test": 0.1}

    db.insert_dataset({
        "dataset_id": dataset_id,
        "name": dataset_name,
        "version": dataset_version,
        "config": json.dumps({"media_type": "image", "pass_through": True}),
        "split_ratio": json.dumps(split_ratio),
        "dataset_bucket": "vlm-dataset",
        "dataset_prefix": dataset_prefix,
        "build_status": "building",
    })

    # 셔플 후 분할
    random.shuffle(candidates)
    total = len(candidates)
    train_end = int(total * split_ratio["train"])
    val_end = train_end + int(total * split_ratio["val"])

    splits = {
        "train": candidates[:train_end],
        "val": candidates[train_end:val_end],
        "test": candidates[val_end:],
    }

    dataset_clips_batch: list[dict] = []
    split_counts = {"train": 0, "val": 0, "test": 0}

    for split_name, split_clips in splits.items():
        for clip in split_clips:
            clip_id = clip["clip_id"]
            clip_key = clip["clip_key"]
            label_key = clip.get("label_key")
            metadata_key = None

            # vlm-processed → vlm-dataset S3 copy
            dataset_clip_key = f"{dataset_prefix}/{split_name}/{clip_key.split('/')[-1]}"
            try:
                minio.copy("vlm-processed", clip_key, "vlm-dataset", dataset_clip_key)

                # 이벤트 라벨 JSON은 vlm-labels를 원본으로 유지한다.
                if label_key:
                    dataset_label_key = f"{dataset_prefix}/{split_name}/{label_key.split('/')[-1]}"
                    minio.copy("vlm-labels", label_key, "vlm-dataset", dataset_label_key)

                metadata_payload = {
                    "clip_id": clip_id,
                    "source_asset_id": clip.get("source_asset_id"),
                    "source_label_id": clip.get("source_label_id"),
                    "label_source": clip.get("label_source"),
                    "review_status": clip.get("review_status"),
                    "label_format": clip.get("label_format"),
                    "object_count": clip.get("object_count"),
                    "caption_text": clip.get("caption_text"),
                    "clip_start_sec": clip.get("clip_start_sec"),
                    "clip_end_sec": clip.get("clip_end_sec"),
                    "data_source": clip.get("data_source"),
                }
                if any(value not in (None, "", 0) for value in metadata_payload.values()):
                    clip_stem = Path(dataset_clip_key).stem
                    metadata_key = f"{dataset_prefix}/{split_name}/{clip_stem}.meta.json"
                    minio.upload(
                        "vlm-dataset",
                        metadata_key,
                        json.dumps(metadata_payload, ensure_ascii=False, indent=2).encode("utf-8"),
                        "application/json",
                    )

            except Exception as e:
                context.log.error(f"S3 copy 실패: {clip_key}: {e}")
                continue

            dataset_clips_batch.append({
                "dataset_id": dataset_id,
                "clip_id": clip_id,
                "split": split_name,
                "dataset_key": dataset_clip_key,
                "metadata_key": metadata_key,
            })
            split_counts[split_name] += 1

    # dataset_clips 배치 INSERT
    db.insert_dataset_clips_batch(dataset_clips_batch)

    # 데이터셋 상태 업데이트
    db.update_dataset_status(dataset_id, "completed")

    summary = {
        "dataset_id": dataset_id,
        "total": total,
        **split_counts,
    }
    context.add_output_metadata(summary)
    context.log.info(f"BUILD 완료: {summary}")
    return summary
