"""SHA-256 체크섬 — 정확 중복 검출 + 변경 감지.

Layer 1: 순수 Python, Dagster 의존 없음.
"""

import hashlib
from pathlib import Path


def sha256sum(file_path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    """파일의 SHA-256 체크섬 계산.

    Args:
        file_path: 파일 경로.
        chunk_size: 읽기 청크 크기 (기본 1MB).

    Returns:
        SHA-256 hex digest 문자열.
    """
    path = Path(file_path)
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    """바이트 데이터의 SHA-256 체크섬 계산 (메모리 내)."""
    return hashlib.sha256(data).hexdigest()
