"""SAM3 detection asset 전체 흐름 테스트 — MinIO 업로드 + DB insert + COCO 구조 검증."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

pytest.importorskip("dagster")

from tests.helpers.dagster_dummies import DummyContext, DummyLogPermissive  # noqa: E402
from vlm_pipeline.defs.sam import detection_assets as sam_det_assets  # noqa: E402


class _DummyContext(DummyContext):
    def __init__(self) -> None:
        super().__init__(
            op_config={
                "limit": 10,
                "score_threshold": 0.0,
                "max_masks_per_prompt": 10,
            },
            tags={"classes": "fire,smoke"},
        )


class _DummyDB:
    def __init__(self, candidates: list[dict] | None = None) -> None:
        self.inserted_rows: list[dict] = []
        self.bbox_status_calls: list[tuple[str, str]] = []
        self._candidates = (
            candidates
            if candidates is not None
            else [
                {
                    "image_id": "image-1",
                    "source_asset_id": "asset-1",
                    "source_clip_id": "clip-1",
                    "image_bucket": "vlm-processed",
                    "image_key": "unit/image/frame_0001.jpg",
                    "width": 640,
                    "height": 480,
                }
            ]
        )

    def find_sam3_pending_images(
        self,
        *,
        limit: int,
        folder_name: str | None,
        spec_id: str | None,
    ) -> list[dict]:
        assert limit == 10
        return self._candidates

    def batch_insert_image_labels(self, rows: list[dict]) -> int:
        self.inserted_rows.extend(rows)
        return len(rows)

    def update_bbox_status(self, asset_id: str, status: str, *, completed_at=None) -> None:
        self.bbox_status_calls.append((asset_id, status))


class _DummyMinIO:
    def __init__(self) -> None:
        self.uploaded_payloads: dict[tuple[str, str], bytes] = {}

    def ensure_bucket(self, bucket: str) -> None:
        assert bucket == "vlm-labels"

    def download(self, bucket: str, key: str) -> bytes:
        assert bucket == "vlm-processed"
        assert key == "unit/image/frame_0001.jpg"
        return b"image-bytes"

    def upload(self, bucket: str, key: str, data: bytes, content_type: str) -> None:
        self.uploaded_payloads[(bucket, key)] = data

    def upload_json(self, bucket: str, key: str, payload, content_type: str = "application/json") -> None:
        import json

        self.uploaded_payloads[(bucket, key)] = json.dumps(payload).encode()

    def exists(self, bucket: str, key: str) -> bool:
        return (bucket, key) in self.uploaded_payloads


class _DummySAM3Client:
    api_url = "http://sam3:8002"

    def wait_until_ready(self, max_wait_sec: int = 300) -> bool:
        return True

    def health(self) -> dict:
        return {"device": "cpu", "gpu_memory_peak_gb": 0.0}

    def segment(
        self,
        image_bytes: bytes,
        *,
        prompts: list[str],
        filename: str = "image.jpg",
        score_threshold: float = 0.0,
        max_masks_per_prompt: int = 50,
        per_prompt_score_thresholds: dict | None = None,
    ) -> dict:
        assert image_bytes == b"image-bytes"
        assert set(prompts) == {"fire", "smoke"}
        return {
            "detections": [
                {
                    "prompt_class": "fire",
                    "score": 0.92,
                    "mask_bbox": [10.0, 20.0, 50.0, 70.0],
                    "model_box": [10.0, 20.0, 50.0, 70.0],
                },
                {
                    "prompt_class": "smoke",
                    "score": 0.85,
                    "mask_bbox": [100.0, 200.0, 180.0, 280.0],
                    "model_box": [100.0, 200.0, 180.0, 280.0],
                },
            ],
            "elapsed_ms": 30.0,
            "per_prompt_latency_ms": {"fire": 15.0, "smoke": 15.0},
            "device": "cpu",
            "gpu_memory_peak_gb": 0.0,
        }


def test_sam3_detection_uploads_coco_and_inserts_db(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_SAM3_DETECTION", "true")
    monkeypatch.setattr(sam_det_assets, "get_sam3_client", lambda: _DummySAM3Client())

    context = _DummyContext()
    db = _DummyDB()
    minio = _DummyMinIO()

    summary = sam_det_assets._run_sam3_image_detection(context, db, minio)

    assert summary["processed"] == 1
    assert summary["failed"] == 0
    assert summary["total_detections"] == 2
    assert summary["label_tool"] == "sam3"

    sam3_key = "unit/sam3_segmentations/frame_0001.json"
    assert ("vlm-labels", sam3_key) in minio.uploaded_payloads

    coco = json.loads(minio.uploaded_payloads[("vlm-labels", sam3_key)].decode("utf-8"))
    assert isinstance(coco.get("images"), list)
    assert isinstance(coco.get("annotations"), list)
    assert isinstance(coco.get("categories"), list)
    assert len(coco["annotations"]) == 2
    assert coco["meta"]["model"] == "sam3.1"

    for ann in coco["annotations"]:
        bbox = ann["bbox"]
        assert len(bbox) == 4
        assert bbox[2] > 0 and bbox[3] > 0

    assert len(db.inserted_rows) == 1
    row = db.inserted_rows[0]
    assert row["label_format"] == "coco"
    assert row["label_tool"] == "sam3"
    assert row["object_count"] == 2
    assert row["labels_key"] == sam3_key


def test_sam3_detection_skips_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_SAM3_DETECTION", "false")

    context = _DummyContext()
    # SAM3 계열 output(bbox/segmentation)이 없는 run으로 설정해 should_run_any_output=False 유도.
    context.run = SimpleNamespace(tags={"outputs": "captioning_video"})
    db = _DummyDB()
    minio = _DummyMinIO()

    summary = sam_det_assets._run_sam3_image_detection(context, db, minio)

    assert summary.get("skipped") is True
    assert summary["processed"] == 0


def test_sam3_detection_updates_bbox_status_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_SAM3_DETECTION", "true")
    monkeypatch.setattr(sam_det_assets, "get_sam3_client", lambda: _DummySAM3Client())

    context = _DummyContext()
    db = _DummyDB()
    minio = _DummyMinIO()

    summary = sam_det_assets._run_sam3_image_detection(context, db, minio)

    assert summary["processed"] == 1
    assert db.bbox_status_calls == [("asset-1", "completed")]


def test_sam3_detection_raises_when_server_not_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    """SAM3 서버 wait_until_ready 실패 시 silent return이 아니라 Failure로 승격되어야 한다.
    과거 bug: 서버 미준비에서 dict 반환 → step SUCCESS 마킹 → bbox_status=pending 방치.
    """
    from dagster import Failure

    monkeypatch.setenv("ENABLE_SAM3_DETECTION", "true")

    class _NotReadyClient:
        api_url = "http://sam3:8002"

        def wait_until_ready(self, max_wait_sec: int = 300) -> bool:
            return False

    monkeypatch.setattr(sam_det_assets, "get_sam3_client", lambda: _NotReadyClient())

    # Asset가 raise 전 log.error도 호출하므로 permissive log 사용.
    context = _DummyContext()
    context.log = DummyLogPermissive()
    db = _DummyDB()
    minio = _DummyMinIO()

    with pytest.raises(Failure) as exc_info:
        sam_det_assets._run_sam3_image_detection(context, db, minio)
    assert "SAM3" in str(exc_info.value.description) or "sam3" in str(exc_info.value.description).lower()


def test_pending_image_query_includes_directly_ingested_stills():
    """정지 이미지(source_image)도 SAM3 후보여야 한다.

    2026-09-21 실측: comfy_local 합성본을 promote 했더니 image_role='source_image' 로
    적재됐는데 find_pending_images 가 프레임 두 종류만 뽑고 있어 SAM3 후보에서 빠졌다.
    → COCO JSON 이 안 생김 → image LS task 의 재료가 없음 → 검수자에게 영원히 도달 못 함.

    이 관문은 라벨러 게이트보다 **앞**이라 게이트를 열어도 도달하지 못한다. 쿼리 문자열을
    직접 검사하는 이유는, 이 필터가 조용히 좁혀지면 증상이 "아무 일도 안 일어남"이라
    런타임에서 알아채기 어렵기 때문이다.
    """
    import inspect

    from vlm_pipeline.resources.postgres_detection import PostgresDetectionMixin

    src = inspect.getsource(PostgresDetectionMixin.find_pending_images)
    assert "'source_image'" in src, "정지 이미지가 detection 후보에서 빠졌다"
    assert "'processed_clip_frame'" in src and "'raw_video_frame'" in src, "기존 프레임 역할 회귀"
