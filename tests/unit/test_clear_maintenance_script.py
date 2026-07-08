"""scripts/clear_maintenance.sh 정적 검증 (no exec — 구조/내용만)."""

from __future__ import annotations

import os
import pathlib

_SCRIPT = pathlib.Path("scripts/clear_maintenance.sh")


def test_script_exists_and_executable():
    assert _SCRIPT.exists(), "clear_maintenance.sh 누락"
    assert os.access(_SCRIPT, os.X_OK), "실행권한 없음 (chmod +x)"


def test_script_hits_both_targets_and_endpoints():
    text = _SCRIPT.read_text()
    assert "set -euo pipefail" in text
    assert "/maintenance/exit" in text
    assert "/warmup" in text
    assert "SAM3_API_URL" in text
    assert "EMBEDDING_API_URL" in text
    # 인자 없으면 all 기본
    assert "all" in text
