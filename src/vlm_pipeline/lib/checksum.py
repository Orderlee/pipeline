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


def sha256_stream(fileobj, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    """읽기 가능한 스트림의 SHA-256 + 총 바이트 수.

    파일 경로가 아니라 스트림을 받는 이유: NAS 원본이 사라진 코호트의 checksum 을
    MinIO 객체에서 백필해야 하는데, 전체를 메모리에 올리지 않고 흘려 읽기 위함이다.
    바이트 수를 함께 돌려주어 호출자가 `raw_files.file_size` 와 대조할 수 있게 한다.

    Returns:
        (hex digest, 읽은 총 바이트 수)
    """
    h = hashlib.sha256()
    total = 0
    for chunk in iter(lambda: fileobj.read(chunk_size), b""):
        h.update(chunk)
        total += len(chunk)
    return h.hexdigest(), total
