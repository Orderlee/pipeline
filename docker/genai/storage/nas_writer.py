"""NAS atomic write helper.

`.partial` 으로 쓰고 fsync 후 atomic rename → sensor 가 partial 파일을 픽업하지
않도록 보장. _manifest.json 은 항상 마지막에 작성 — sensor 의 픽업 트리거.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path


def atomic_write_bytes(target: Path, data: bytes) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        prefix=target.name + ".", suffix=".partial", dir=target.parent,
    )
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, target)
    except Exception:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


def atomic_write_json(target: Path, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    atomic_write_bytes(target, body)
