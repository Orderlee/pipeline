"""Perceptual Hash — pHash 유사도 검출.

Layer 1: 순수 Python, Dagster 의존 없음.
"""

from io import BytesIO
from pathlib import Path

import imagehash
from PIL import Image


def compute_phash(path_or_bytes: str | Path | bytes) -> str:
    """이미지 perceptual hash 계산.

    Args:
        path_or_bytes: 파일 경로 또는 바이트 데이터.

    Returns:
        16자리 hex 문자열.
    """
    if isinstance(path_or_bytes, bytes):
        img = Image.open(BytesIO(path_or_bytes))
    else:
        img = Image.open(path_or_bytes)
    return str(imagehash.phash(img))


def hamming_distance(hash1: str, hash2: str) -> int:
    """두 pHash 간 Hamming distance 계산.

    Args:
        hash1: 16자리 hex 문자열.
        hash2: 16자리 hex 문자열.

    Returns:
        비트 차이 수.
    """
    if len(hash1) != len(hash2):
        raise ValueError("hash_length_mismatch")
    b1 = bin(int(hash1, 16))[2:].zfill(len(hash1) * 4)
    b2 = bin(int(hash2, 16))[2:].zfill(len(hash2) * 4)
    return sum(c1 != c2 for c1, c2 in zip(b1, b2))
