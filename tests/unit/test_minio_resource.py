"""MinIOResource — ensure_bucket 이중 호출 방어 / upload 분기 검증 (moto)."""

from __future__ import annotations

from io import BytesIO


def test_minio_ensure_bucket_once_idempotent(mock_minio, monkeypatch):
    call_count = {"n": 0}
    real_head = mock_minio.client.head_bucket

    def counting_head(**kwargs):
        call_count["n"] += 1
        return real_head(**kwargs)

    monkeypatch.setattr(mock_minio.client, "head_bucket", counting_head)

    # _ensured_buckets 캐시에 'vlm-raw' 없도록 초기화
    mock_minio._ensured_buckets.clear()

    mock_minio._ensure_bucket_once("vlm-raw")
    mock_minio._ensure_bucket_once("vlm-raw")
    mock_minio._ensure_bucket_once("vlm-raw")

    assert call_count["n"] == 1
    assert "vlm-raw" in mock_minio._ensured_buckets

    mock_minio._ensure_bucket_once("")
    assert call_count["n"] == 1


def test_minio_upload_bytes_vs_fileobj(mock_minio):
    mock_minio.upload("vlm-raw", "path/bytes.bin", b"hello", content_type="application/octet-stream")
    assert mock_minio.download("vlm-raw", "path/bytes.bin") == b"hello"

    fileobj = BytesIO(b"world!")
    mock_minio.upload("vlm-raw", "path/stream.bin", fileobj, content_type="application/octet-stream")
    assert mock_minio.download("vlm-raw", "path/stream.bin") == b"world!"

    mock_minio.upload("vlm-raw", "path/ba.bin", bytearray(b"ba"))
    assert mock_minio.download("vlm-raw", "path/ba.bin") == b"ba"
