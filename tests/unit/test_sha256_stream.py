"""lib.checksum.sha256_stream — 스트림 SHA-256 + 바이트 수.

NAS 원본이 사라진 코호트의 checksum 을 MinIO 객체에서 백필할 때 쓴다
(`scripts/backfill_checksums_from_minio.py`). 바이트 수를 함께 돌려주는 이유는
부분 다운로드를 `raw_files.file_size` 와 대조해 걸러내기 위함이다 — 그 가드가 없으면
잘린 바이트의 해시가 정답인 양 DB 에 박힌다.

hashlib/io 만 의존 — dagster import 없음.
"""

from __future__ import annotations

import hashlib
import io

from vlm_pipeline.lib.checksum import sha256_stream, sha256sum


def test_matches_hashlib_and_reports_length():
    data = b"hello world\n" * 1000
    digest, nbytes = sha256_stream(io.BytesIO(data))
    assert digest == hashlib.sha256(data).hexdigest()
    assert nbytes == len(data)


def test_empty_stream():
    digest, nbytes = sha256_stream(io.BytesIO(b""))
    assert digest == hashlib.sha256(b"").hexdigest()
    assert nbytes == 0


def test_chunk_size_does_not_change_result():
    data = bytes(range(256)) * 500
    big, _ = sha256_stream(io.BytesIO(data), chunk_size=1024 * 1024)
    tiny, n = sha256_stream(io.BytesIO(data), chunk_size=7)  # 청크 경계가 결과를 바꾸면 안 된다
    assert big == tiny
    assert n == len(data)


def test_agrees_with_file_based_sha256sum(tmp_path):
    """기존 sha256sum(경로) 과 같은 값이어야 한다 — 백필된 checksum 이 정상 수집분과 비교 가능해야 하므로."""
    data = b"\x00\x01\x02sample bytes\xff" * 321
    path = tmp_path / "f.bin"
    path.write_bytes(data)
    digest, nbytes = sha256_stream(io.BytesIO(data))
    assert digest == sha256sum(path)
    assert nbytes == path.stat().st_size
