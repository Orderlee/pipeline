"""BUILD @asset — 데이터셋 조립 (train/val/test 분할).

Layer 4: Dagster @asset.
"""

from __future__ import annotations

import json
import random
from uuid import uuid4

from dagster import asset

from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@asset(
    deps=["processed_clips"],
    description="BUILD — processed_clips → train/val/test 분할 → vlm-dataset",
    group_name="build",
)
def built_dataset(
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

            # vlm-processed → vlm-dataset S3 copy
            dataset_clip_key = f"{dataset_prefix}/{split_name}/{clip_key.split('/')[-1]}"
            try:
                minio.copy("vlm-processed", clip_key, "vlm-dataset", dataset_clip_key)

                # 라벨도 복사
                if label_key:
                    dataset_label_key = f"{dataset_prefix}/{split_name}/{label_key.split('/')[-1]}"
                    minio.copy("vlm-processed", label_key, "vlm-dataset", dataset_label_key)

            except Exception as e:
                context.log.error(f"S3 copy 실패: {clip_key}: {e}")
                continue

            dataset_clips_batch.append({
                "dataset_id": dataset_id,
                "clip_id": clip_id,
                "split": split_name,
                "dataset_key": dataset_clip_key,
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
