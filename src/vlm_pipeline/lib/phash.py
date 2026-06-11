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
    with img:
        return str(imagehash.phash(img))
