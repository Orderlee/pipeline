from io import BytesIO
from pathlib import Path

import imagehash
from PIL import Image


def compute_phash(path_or_bytes: str | Path | bytes) -> str:
    if isinstance(path_or_bytes, bytes):
        img = Image.open(BytesIO(path_or_bytes))
    else:
        img = Image.open(path_or_bytes)
    with img:
        return str(imagehash.phash(img))


def hamming_distance(hash1: str, hash2: str) -> int:
    if len(hash1) != len(hash2):
        raise ValueError("hash_length_mismatch")
    b1 = bin(int(hash1, 16))[2:].zfill(len(hash1) * 4)
    b2 = bin(int(hash2, 16))[2:].zfill(len(hash2) * 4)
    return sum(c1 != c2 for c1, c2 in zip(b1, b2))
