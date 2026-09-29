from __future__ import annotations

import json

import pytest

pytest.importorskip("dagster")

from tests.helpers.dagster_dummies import DummyContext  # noqa: E402
from vlm_pipeline.defs.yolo import assets as yolo_assets  # noqa: E402
from vlm_pipeline.lib.yolo_thresholds import YOLO_CLASS_CONFIDENCE_THRESHOLDS  # noqa: E402

# 임계값은 운영 중 튜닝된다(2026-09: person_fallen 0.79, fire 0.74). 픽스처 신뢰도를
# 숫자로 얼리면 튜닝마다 깨지므로 정본 테이블에서 파생시킨다.
_PF_THRESHOLD = YOLO_CLASS_CONFIDENCE_THRESHOLDS["person_fallen"]
_FIRE_THRESHOLD = YOLO_CLASS_CONFIDENCE_THRESHOLDS["fire"]


class _DummyContext(DummyContext):
    def __init__(self) -> None:
        super().__init__(
            op_config={
                "limit": 10,
                "confidence_threshold": 0.25,
                "iou_threshold": 0.45,
                "batch_size": 4,
            },
            tags={"requested_outputs": "bbox,classification_image"},
        )


class _DummyDB:
    def __init__(self) -> None:
        self.include_image_classification: bool | None = None
        self.inserted_rows: list[dict] = []

    def find_yolo_pending_images(
        self,
        *,
        limit: int,
        folder_name: str | None,
        spec_id: str | None,
        include_image_classification: bool,
    ) -> list[dict]:
        self.include_image_classification = include_image_classification
        assert limit == 10
        assert folder_name == "unit"
        assert spec_id is None
        return [
            {
                "image_id": "image-1",
                "source_clip_id": "clip-1",
                "image_bucket": "vlm-processed",
                "image_key": "unit/image/frame_0001.jpg",
                "width": 1920,
                "height": 1080,
            }
        ]

    def batch_insert_image_labels(self, rows: list[dict]) -> int:
        self.inserted_rows.extend(rows)
        return len(rows)


class _DummyMinIO:
    def __init__(self) -> None:
        self.uploaded_payloads: dict[tuple[str, str], bytes] = {}

    def download(self, bucket: str, key: str) -> bytes:
        assert bucket == "vlm-processed"
        assert key == "unit/image/frame_0001.jpg"
        return b"image-bytes"

    def upload(self, bucket: str, key: str, data: bytes, content_type: str) -> None:
        self.uploaded_payloads[(bucket, key)] = data

    # 제품은 raw upload 가 아니라 upload_json 을 쓴다 (assets.py:261,293).
    # 검증부가 bytes 를 decode 하므로 여기서 직렬화해 둔다.
    def upload_json(self, bucket: str, key: str, payload, **kwargs) -> None:
        self.uploaded_payloads[(bucket, key)] = json.dumps(payload).encode("utf-8")


class _DummyClient:
    def __init__(self) -> None:
        self.api_url = "http://yolo.test"
        self.last_conf: float | None = None
        self.last_iou: float | None = None
        self.last_classes: list[str] | None = None

    def wait_until_ready(self, max_wait_sec: int = 120) -> bool:
        assert max_wait_sec == 120
        return True

    def health(self) -> dict:
        return {"device": "cpu", "classes_count": 2, "gpu_memory": {"free_gb": 0.0}}

    def detect_batch(
        self,
        image_bytes_list: list[bytes],
        *,
        conf: float,
        iou: float,
        classes: list[str] | None = None,
    ) -> list[dict]:
        self.last_conf = conf
        self.last_iou = iou
        self.last_classes = classes
        assert image_bytes_list == [b"image-bytes"]
        return [
            {
                "detections": [
                    {"class": "person_fallen", "confidence": _PF_THRESHOLD - 0.01, "bbox": [10.0, 20.0, 40.0, 80.0]},
                    {"class": "fire", "confidence": _FIRE_THRESHOLD, "bbox": [100.0, 120.0, 160.0, 220.0]},
                ],
                "image_size": [1920, 1080],
                "elapsed_ms": 37.5,
            }
        ]


def test_run_yolo_image_detection_skips_when_env_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_YOLO_DETECTION", "false")
    context = _DummyContext()
    db = _DummyDB()
    minio = _DummyMinIO()

    summary = yolo_assets._run_yolo_image_detection(
        context,
        db,
        minio,
        folder_name_override="unit",
        target_classes_override=["person_fallen"],
        class_source_override="explicit_target_classes",
    )

    assert summary == {
        "processed": 0,
        "failed": 0,
        "total_detections": 0,
        "skipped": True,
    }
    assert db.include_image_classification is None
    assert minio.uploaded_payloads == {}


def test_run_yolo_image_detection_filters_person_fallen_before_storing(monkeypatch: pytest.MonkeyPatch) -> None:
    context = _DummyContext()
    db = _DummyDB()
    minio = _DummyMinIO()
    client = _DummyClient()

    # assets.py:97 의 ENABLE_YOLO_DETECTION 가드 — 켜지 않으면 첫 줄에서 skip 반환한다.
    monkeypatch.setenv("ENABLE_YOLO_DETECTION", "true")
    monkeypatch.setattr(yolo_assets, "get_yolo_client", lambda: client)

    summary = yolo_assets._run_yolo_image_detection(
        context,
        db,
        minio,
        folder_name_override="unit",
        target_classes_override=["person_fallen", "fire"],
        class_source_override="explicit_target_classes",
    )

    # store_image_classification = spec_id 없음 AND requested_outputs 에 classification_image
    # (assets.py:123). 이 컨텍스트는 둘 다 만족하므로 True 가 맞다.
    assert db.include_image_classification is True
    assert client.last_conf == 0.25
    assert client.last_iou == 0.45
    assert client.last_classes == ["person_fallen", "fire"]
    assert summary["processed"] == 1
    assert summary["failed"] == 0
    assert summary["total_detections"] == 1
    assert summary["effective_request_confidence_threshold"] == 0.25
    assert summary["class_confidence_thresholds"] == {
        "person_fallen": _PF_THRESHOLD,
        "fire": _FIRE_THRESHOLD,
    }

    detection_payload = json.loads(
        minio.uploaded_payloads[("vlm-labels", "unit/detections/frame_0001.json")].decode("utf-8")
    )
    assert detection_payload["meta"]["effective_request_confidence_threshold"] == 0.25
    assert detection_payload["meta"]["elapsed_ms"] == 37.5
    assert detection_payload["meta"]["class_confidence_thresholds"] == {
        "fire": _FIRE_THRESHOLD,
        "person_fallen": _PF_THRESHOLD,
    }
    assert detection_payload["annotations"] == [
        {
            "id": 1,
            "image_id": 1,
            "category_id": 2,
            "bbox": [100.0, 120.0, 60.0, 100.0],
            "area": 6000.0,
            "iscrowd": 0,
            "segmentation": [],
            "score": _FIRE_THRESHOLD,
        }
    ]
    assert detection_payload["categories"] == [
        {"id": 1, "name": "person_fallen", "supercategory": "object"},
        {"id": 2, "name": "fire", "supercategory": "object"},
    ]

    # 분류 산출물도 같이 올라간다(include_image_classification=True). 핵심은
    # 임계값 필터가 분류까지 전파돼 걸러진 person_fallen 이 빠지는 것.
    classification_payload = json.loads(
        minio.uploaded_payloads[("vlm-labels", "unit/image_classifications/frame_0001.json")].decode("utf-8")
    )
    assert classification_payload["predicted_classes"] == ["fire"]
    assert classification_payload["class_counts"] == {"fire": 1}
    # bbox(coco) 1행 + 분류 1행. 개수만 세면 어느 쪽이 빠졌는지 안 보여서 format 으로 본다.
    assert [r["label_format"] for r in db.inserted_rows] == ["coco", "image_classification_json"]
