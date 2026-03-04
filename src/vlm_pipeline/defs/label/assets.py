"""LABEL @asset — 라벨 등록 (INGEST와 별도).

Layer 4: Dagster @asset.
정규 운영: CVAT/LS에서 어노테이션 → JSON → vlm-labels + labels INSERT.
MVP: load_sample_labels로 기구축 샘플 JSON 로딩.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from dagster import asset

from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@asset(
    deps=["ingested_raw_files"],
    description="라벨 등록 — raw_key stem 매칭으로 labels 테이블 INSERT",
    group_name="label",
)
def labeled_files(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """MVP 샘플 라벨 로딩 — 기구축 라벨 JSON을 vlm-labels에 등록.

    매칭 규칙: label_stem ↔ raw_key basename stem.
    label_key = raw_key.rsplit('.', 1)[0] + '.json'
    """
    config = PipelineConfig()

    # 라벨 디렉토리 탐색 (incoming 하위에 labels/ 또는 annotations/ 디렉토리)
    label_dirs = [
        Path(config.incoming_dir) / "labels",
        Path(config.incoming_dir) / "annotations",
    ]

    loaded = 0
    skipped = 0
    not_matched = 0

    for label_dir in label_dirs:
        if not label_dir.exists():
            continue

        for json_path in sorted(label_dir.glob("*.json")):
            try:
                label_stem = json_path.stem

                # raw_key의 basename stem으로 매칭
                matching_asset = db.find_by_raw_key_stem(label_stem)
                if not matching_asset:
                    context.log.warning(f"매칭 raw_key 없음: {json_path.name}")
                    not_matched += 1
                    continue

                # 라벨 JSON 읽기
                label_data = json.loads(json_path.read_text(encoding="utf-8"))

                # vlm-labels 버킷에 업로드
                raw_key = matching_asset["raw_key"]
                label_key = raw_key.rsplit(".", 1)[0] + ".json"
                label_bytes = json.dumps(label_data, ensure_ascii=False).encode("utf-8")
                minio.upload("vlm-labels", label_key, label_bytes, "application/json")

                # labels 테이블 INSERT
                event_count = len(label_data.get("annotations", label_data.get("events", [])))
                db.insert_label({
                    "label_id": str(uuid4()),
                    "asset_id": matching_asset["asset_id"],
                    "labels_bucket": "vlm-labels",
                    "labels_key": label_key,
                    "label_format": _detect_label_format(label_data),
                    "label_tool": "pre-built",
                    "event_count": event_count,
                    "label_status": "completed",
                })
                loaded += 1

            except Exception as e:
                context.log.error(f"라벨 로딩 실패: {json_path}: {e}")
                skipped += 1

    summary = {"loaded": loaded, "skipped": skipped, "not_matched": not_matched}
    context.add_output_metadata(summary)
    context.log.info(f"LABEL 완료: {summary}")
    return summary


def _detect_label_format(data: dict) -> str:
    """라벨 JSON 포맷 자동 감지."""
    if "images" in data and "annotations" in data:
        return "coco"
    if "shapes" in data:
        return "labelme"
    if isinstance(data, list):
        return "yolo"
    return "custom"
