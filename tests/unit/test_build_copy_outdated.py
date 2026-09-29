"""build/_copy_if_outdated — dst stale 감지 + 재copy 회귀 테스트.

src ETag 와 dst ETag 가 다르면 dst 가 이미 존재해도 재copy 해야 한다.
이전 _copy_if_absent 는 dst 존재만 보고 skip 하여, LS 재submit 으로
src 가 갱신된 timestamp/bbox JSON 을 vlm-dataset 이 영구히 stale 로
들고 있는 회귀를 일으켰다.
"""

from __future__ import annotations

import logging

import pytest

from vlm_pipeline.defs.build import build_helpers
from vlm_pipeline.defs.build.assets import _copy_if_outdated


@pytest.fixture(autouse=True)
def _force_minio_dataset_storage(monkeypatch: pytest.MonkeyPatch) -> None:
    """DATASET_STORAGE 기본값은 'fs'(NFS write) 라 dst_bucket 인자가 무시된다.

    이 파일은 legacy MinIO server-side copy 분기(DATASET_STORAGE=minio)의
    ETag 기반 stale 감지를 검증하므로 그 분기를 강제한다.
    """
    monkeypatch.setattr(build_helpers, "DATASET_STORAGE", "minio")


def _put(minio, bucket: str, key: str, body: bytes) -> str:
    minio.upload(bucket, key, body, content_type="application/json")
    head = minio.head(bucket, key)
    assert head is not None
    return head["etag"]


def test_copy_when_dst_missing(mock_minio):
    src_etag = _put(mock_minio, "vlm-labels", "p/events/a.json", b'[{"e":1}]')

    copied = _copy_if_outdated(
        mock_minio,
        "vlm-labels",
        "p/events/a.json",
        "p/timestamps/a.json",
        logging.getLogger("t"),
        dst_bucket="vlm-dataset",
    )

    assert copied is True
    dst = mock_minio.head("vlm-dataset", "p/timestamps/a.json")
    assert dst is not None and dst["etag"] == src_etag


def test_skip_when_etag_matches(mock_minio):
    body = b'[{"e":1}]'
    _put(mock_minio, "vlm-labels", "p/events/a.json", body)
    _put(mock_minio, "vlm-dataset", "p/timestamps/a.json", body)

    copied = _copy_if_outdated(
        mock_minio,
        "vlm-labels",
        "p/events/a.json",
        "p/timestamps/a.json",
        logging.getLogger("t"),
        dst_bucket="vlm-dataset",
    )

    assert copied is False


def test_refresh_when_src_overwritten(mock_minio):
    """LS 재submit 시나리오 — src 가 갱신된 뒤 build 재실행."""
    log = logging.getLogger("t")

    # 1차 build: dst 가 src 와 동일 (Gemini auto 결과 첫 카피)
    old_body = b'[{"event":"old","count":3}]'
    _put(mock_minio, "vlm-labels", "p/events/a.json", old_body)
    _copy_if_outdated(
        mock_minio,
        "vlm-labels",
        "p/events/a.json",
        "p/timestamps/a.json",
        log,
        dst_bucket="vlm-dataset",
    )
    dst_old = mock_minio.head("vlm-dataset", "p/timestamps/a.json")

    # 사람이 LS 에서 submit → src 덮어쓰기
    new_body = b"[]"  # FP 확정 → 0 events
    new_etag = _put(mock_minio, "vlm-labels", "p/events/a.json", new_body)
    assert new_etag != dst_old["etag"]

    # 2차 build: 회귀 케이스 — 반드시 재copy 되어야 함
    copied = _copy_if_outdated(
        mock_minio,
        "vlm-labels",
        "p/events/a.json",
        "p/timestamps/a.json",
        log,
        dst_bucket="vlm-dataset",
    )

    assert copied is True
    assert mock_minio.download("vlm-dataset", "p/timestamps/a.json") == new_body
    dst_new = mock_minio.head("vlm-dataset", "p/timestamps/a.json")
    assert dst_new["etag"] == new_etag
