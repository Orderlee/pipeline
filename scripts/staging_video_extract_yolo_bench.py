#!/usr/bin/env python3
"""Backward-compatible wrapper for the canonical test extract + YOLO bench helper."""

from __future__ import annotations

import os
from pathlib import Path
import sys


def main() -> None:
    target = Path(__file__).with_name("test_video_extract_yolo_bench.py")
    os.execv(sys.executable, [sys.executable, str(target), *sys.argv[1:]])


if __name__ == "__main__":
    main()
