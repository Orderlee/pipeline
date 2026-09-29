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


def test_no_dead_default_endpoints():
    """기본 URL 이 도달 가능한 주소여야 한다 (회귀 가드).

    구 기본값 `http://10.0.0.10:{8002,8000}` 은 IP 도 포트도 죽어 있었고,
    `curl -sf` 가 타임아웃을 WARN 으로 삼켜서 "아무것도 안 했는데 성공처럼" 보였다.
    2026-07-06 IP 개편 이후 10.0.0.x 대역은 전부 도달 불가.
    """
    text = _SCRIPT.read_text()
    assert "10.0.0." not in text, "죽은 10.0.0.x 대역이 남아있다"
    assert "SAM3_API_URL:-http://localhost:8002" in text
    # embedding-service 는 컨테이너 8003 → 호스트 8004 매핑. 8000 은 옛 오기.
    assert "EMBEDDING_API_URL:-http://localhost:8004" in text


def test_exit_is_forced():
    """서빙이 owner 불일치 exit 를 409 로 거부하므로, 운영자 강제 해제는 force 를 실어야 한다.

    force 없이 부르면 owner 를 모르는 복구 상황에서 거부당해 "해제했다"는 착각이 생긴다.
    """
    text = _SCRIPT.read_text()
    assert "force=true" in text, "강제 해제인데 force 플래그가 없다"


def test_serving_failure_is_not_silently_successful():
    """서빙 호출 실패 시 non-zero 로 끝나야 한다 — 조용한 no-op 방지."""
    text = _SCRIPT.read_text()
    assert "FAILURES" in text, "실패 카운터 없음"
    assert "exit 1" in text, "실패해도 exit 0 이면 운영자가 성공으로 오인한다"
    assert "--max-time" in text, "타임아웃 없으면 무한 대기 가능"
