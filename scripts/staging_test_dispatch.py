#!/usr/bin/env python3
"""Backward-compatible wrapper for `scripts/test_dispatch.py`."""

from __future__ import annotations

import runpy
from pathlib import Path


if __name__ == "__main__":
    runpy.run_path(str(Path(__file__).with_name("test_dispatch.py")), run_name="__main__")
