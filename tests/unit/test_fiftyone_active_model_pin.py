"""docker/analysis/fiftyone_pgvector.py 는 serving 코드라 vlm_pipeline.lib 를 import 못 함 →
DEFAULT_MODEL 상수 + _active_model_name() 폴백이 lib 의 STOCK_PE_CORE_MODEL_NAME 과
같은 값을 쓰는지 pin (drift 가드).

+ attach_labels()/add_caption_clusters() 의 **캡션 모달리티 skip 가드** 회귀 테스트
  (2026-08-19): 정본 `frames` 는 프레임+캡션 혼합이라 프레임 전용 필드가 캡션 문서로 새면
  DQ 자동 배제(= "캡션엔 image_id 가 없다")가 영구히 깨진다.
"""

from __future__ import annotations

import pathlib
import sys
import types

from vlm_pipeline.lib.embedding_model_name import STOCK_PE_CORE_MODEL_NAME


def _load_fp(monkeypatch):
    d = str(pathlib.Path("docker/analysis").resolve())
    if d not in sys.path:
        sys.path.insert(0, d)
    import fiftyone_pgvector as fp

    return fp


def test_default_model_constant_matches_lib(monkeypatch) -> None:
    fp = _load_fp(monkeypatch)
    assert fp.DEFAULT_MODEL == STOCK_PE_CORE_MODEL_NAME


def test_active_model_name_falls_back_to_constant_on_db_error(monkeypatch) -> None:
    fp = _load_fp(monkeypatch)

    def _boom():
        raise RuntimeError("no pg")

    monkeypatch.setattr(fp, "_pg_conn", _boom)
    # _active_model_name must swallow errors and return the stock constant (search must never
    # break just because the pointer table is missing/unreachable).
    assert fp._active_model_name() == fp.DEFAULT_MODEL


class _FakeSample:
    """dict 백엔드 가짜 FiftyOne 샘플 — `sample[f]` get/set + save() 만 있으면 충분하다."""

    def __init__(self, **fields):
        self._d = dict(fields)
        self.filepath = fields.get("filepath", "/tmp/x.jpg")
        self.saved = 0

    def __getitem__(self, key):
        try:
            return self._d[key]
        except KeyError:
            raise KeyError(key) from None

    def __setitem__(self, key, value):
        self._d[key] = value

    def save(self):
        self.saved += 1


class _FakeDataset(list):
    pass


def _stub_fiftyone_and_io(fp, monkeypatch):
    """fiftyone 미설치 CI 에서도 돌게 — 모듈 스텁 + DB/MinIO 조회 전부 무력화."""
    monkeypatch.setitem(sys.modules, "fiftyone", types.SimpleNamespace(Detections=lambda **kw: kw))
    for name in (
        "_fetch_frame_asset_ids",
        "_fetch_asset_captions",
        "_fetch_video_env",
        "_fetch_sam3_label_refs",
        "_fetch_image_keys",
    ):
        monkeypatch.setattr(fp, name, lambda *a, **k: {})
    monkeypatch.setattr(fp, "_minio_client", lambda *a, **k: None)


def test_attach_labels_skips_caption_modality(monkeypatch) -> None:
    """image_id 없는 캡션 문서에 프레임 전용 필드(detection_class/normalized_class)를 쓰지 않는다.

    normalize_class(None) 이 문자열 "none"(truthy) 을 돌려주므로 가드 없이는 캡션 전건이
    normalized_class="none" 을 획득한다 — 클래스 분포 오염 + DQ 자동 배제 파괴.
    """
    fp = _load_fp(monkeypatch)
    _stub_fiftyone_and_io(fp, monkeypatch)

    caption = _FakeSample(modality="caption", caption="a man walks")  # image_id 없음
    frame = _FakeSample(modality="frame", image_id="img-1")
    fp.attach_labels(_FakeDataset([caption, frame]))

    assert caption.saved == 0, "캡션 문서가 저장됨 — 프레임 전용 필드가 새어 들어갔다"
    for field in ("detection_class", "normalized_class", "daynight", "environment", "project"):
        assert field not in caption._d, f"캡션 문서에 프레임 전용 필드 {field} 기록됨"
    # 정상 프레임은 그대로 처리된다 (가드가 프레임을 잡아먹지 않는지)
    assert frame.saved == 1
    assert frame._d["normalized_class"] == fp.normalize_class("none")


def test_add_caption_clusters_skips_caption_modality(monkeypatch) -> None:
    """caption_cluster 도 '이 프레임 출처 영상의 캡션 군집'이라 캡션 문서엔 쓰지 않는다."""
    fp = _load_fp(monkeypatch)
    _stub_fiftyone_and_io(fp, monkeypatch)
    monkeypatch.setattr(fp, "_load_caption_embeddings", lambda *a, **k: [])

    caption = _FakeSample(modality="caption")  # image_id 없음
    frame = _FakeSample(modality="frame", image_id="img-1")
    fp.add_caption_clusters(_FakeDataset([caption, frame]))

    assert caption.saved == 0 and "caption_cluster" not in caption._d
    assert frame._d["caption_cluster"] == "none"


def test_normalize_class_none_is_truthy_string(monkeypatch) -> None:
    """가드가 필요한 이유의 근거를 고정 — None 이 그대로 흐르지 않고 "none" 으로 굳는다."""
    fp = _load_fp(monkeypatch)
    assert fp.normalize_class(None) == "none"
