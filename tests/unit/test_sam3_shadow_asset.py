from __future__ import annotations

import json

import pytest

pytest.importorskip("dagster")

from tests.helpers.dagster_dummies import DummyContext  # noqa: E402
from vlm_pipeline.defs.sam import assets as sam_assets  # noqa: E402
from vlm_pipeline.lib.yolo_thresholds import resolve_active_class_confidence_thresholds  # noqa: E402


class _DummyContext(DummyContext):
    def __init__(self) -> None:
        super().__init__(
            op_config={
                "processed_clip_frame_limit": 1,
                "raw_video_frame_limit": 0,
                "max_per_source_unit": 10,
                "score_threshold": 0.0,
                "max_masks_per_prompt": 10,
                "benchmark_id": "bench-unit",
            },
        )


class _DummyDB:
    def __init__(self) -> None:
        self.inserted_rows: list[dict] = []

    def ensure_runtime_schema(self) -> None:
        return None

    def find_sam3_shadow_candidates(
        self,
        *,
        image_role: str,
        limit: int,
        max_per_source_unit: int,
    ) -> list[dict]:
        assert max_per_source_unit == 10
        if image_role == "processed_clip_frame":
            assert limit == 1
            return [
                {
                    "image_id": "image-1",
                    "source_asset_id": "asset-1",
                    "source_clip_id": "clip-1",
                    "image_bucket": "vlm-processed",
                    "image_key": "unit/image/frame_0001.jpg",
                    "image_role": "processed_clip_frame",
                    "source_unit_name": "unit",
                    "raw_key": "unit/video/sample.mp4",
                    "yolo_labels_bucket": "vlm-labels",
                    "yolo_labels_key": "unit/detections/frame_0001.json",
                }
            ]
        raise AssertionError(f"unexpected image_role query: {image_role}")

    def batch_insert_image_labels(self, rows: list[dict]) -> int:
        self.inserted_rows.extend(rows)
        return len(rows)


class _DummyMinIO:
    def __init__(self) -> None:
        self.uploaded_payloads: dict[tuple[str, str], bytes] = {}

    def ensure_bucket(self, bucket: str) -> None:
        assert bucket == "vlm-labels"

    def download(self, bucket: str, key: str) -> bytes:
        if (bucket, key) == ("vlm-processed", "unit/image/frame_0001.jpg"):
            return b"image-bytes"
        if (bucket, key) == ("vlm-labels", "unit/detections/frame_0001.json"):
            return json.dumps(
                {
                    "annotations": [
                        {"category_id": 1, "bbox": [10.0, 20.0, 30.0, 40.0]},
                    ],
                    "categories": [
                        {"id": 1, "name": "fire"},
                    ],
                    "meta": {
                        "requested_classes": ["fire"],
                        "class_source": "dispatch_categories_derived",
                        "elapsed_ms": 12.5,
                    },
                }
            ).encode("utf-8")
        raise AssertionError(f"unexpected download: bucket={bucket} key={key}")

    def upload(self, bucket: str, key: str, data: bytes, content_type: str) -> None:
        assert bucket == "vlm-labels"
        self.uploaded_payloads[(bucket, key)] = data

    def download_json(self, bucket: str, key: str):
        return json.loads(self.download(bucket, key).decode("utf-8"))

    def upload_json(self, bucket: str, key: str, payload, **_kwargs) -> None:
        self.upload(bucket, key, json.dumps(payload).encode("utf-8"), "application/json")


class _DummySAM3Client:
    def wait_until_ready(self, max_wait_sec: int = 300) -> bool:
        assert max_wait_sec == 120
        return True

    def segment(
        self,
        image_bytes: bytes,
        *,
        prompts: list[str],
        filename: str = "image.jpg",
        score_threshold: float = 0.0,
        max_masks_per_prompt: int = 50,
        per_prompt_score_thresholds: dict[str, float] | None = None,
    ) -> dict:
        assert image_bytes == b"image-bytes"
        assert prompts == ["fire"]
        assert filename == "frame_0001.jpg"
        assert score_threshold == 0.0
        assert max_masks_per_prompt == 10
        assert per_prompt_score_thresholds == resolve_active_class_confidence_thresholds(prompts, score_threshold)
        return {
            "detections": [
                {
                    "prompt_class": "fire",
                    "score": 0.91,
                    "mask_bbox": [10.0, 20.0, 40.0, 60.0],
                    "model_box": [10.0, 20.0, 40.0, 60.0],
                    "mask_rle": {"size": [10, 10], "counts": [100]},
                    "area": 1200,
                }
            ],
            "elapsed_ms": 25.0,
            "per_prompt_latency_ms": {"fire": 21.0},
            "device": "cpu",
            "gpu_memory_peak_gb": 0.0,
        }


def test_run_sam3_shadow_compare_uploads_artifacts_and_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    context = _DummyContext()
    db = _DummyDB()
    minio = _DummyMinIO()

    monkeypatch.setattr(sam_assets, "get_sam3_client", lambda: _DummySAM3Client())

    summary = sam_assets._run_sam3_shadow_compare(context, db, minio)

    assert summary["processed_images"] == 1
    assert summary["failed_images"] == 0
    assert summary["pair_rows"] == 1
    assert ("vlm-labels", "unit/sam3_segmentations/frame_0001.json") in minio.uploaded_payloads
    assert ("vlm-labels", "benchmarks/sam3_vs_yolo/bench-unit/summary.json") in minio.uploaded_payloads
    assert ("vlm-labels", "benchmarks/sam3_vs_yolo/bench-unit/pairs.csv") in minio.uploaded_payloads
    assert len(db.inserted_rows) == 1
    assert db.inserted_rows[0]["label_tool"] == "sam3"
    assert db.inserted_rows[0]["label_format"] == "coco"

    sam3_artifact_bytes = minio.uploaded_payloads[("vlm-labels", "unit/sam3_segmentations/frame_0001.json")]
    sam3_artifact = json.loads(sam3_artifact_bytes.decode("utf-8"))
    assert isinstance(sam3_artifact.get("images"), list)
    assert isinstance(sam3_artifact.get("annotations"), list)
    assert isinstance(sam3_artifact.get("categories"), list)
    assert sam3_artifact["meta"]["model"] == "sam3.1"
    assert sam3_artifact["meta"]["benchmark_id"] == "bench-unit"
