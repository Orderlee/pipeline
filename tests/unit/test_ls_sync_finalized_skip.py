"""ls_sync_db.upsert_video_labels / update_image_labels_in_db 의 finalized guard 검증.

Follow-up #4 (codex) 권고: FinalizedLabelsSkip / FinalizedImageSkip 패스가
실제로 write_json 호출 전에 raise 되는지 mock PG 로 회귀 방지.

dagster 의존 없는 순수 stdlib + unittest.mock 만 사용 — gitignored 화이트리스트
대상이 아니므로 이 파일은 .gitignore 에 명시적으로 등록되어야 추적된다.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# src/gemini 가 PYTHONPATH 에 없는 경우 직접 sys.path 삽입 (host pytest 호환).
_GEMINI_DIR = Path(__file__).resolve().parents[2] / "src" / "gemini"
if str(_GEMINI_DIR) not in sys.path:
    sys.path.insert(0, str(_GEMINI_DIR))

# psycopg2 가 host 에 미설치인 경우 통째로 skip — 컨테이너/CI 환경에서만 의미 있음.
psycopg2 = pytest.importorskip("psycopg2", reason="psycopg2 미설치 — 컨테이너/CI 에서만 검증")

from ls_sync_db import (  # noqa: E402
    FinalizedImageSkip,
    FinalizedLabelsSkip,
    update_image_labels_in_db,
    upsert_video_labels,
)


def _build_conn_mock(finalized_count: int) -> MagicMock:
    """psycopg2 connection mock.

    finalized count 만 임의 제어 — cursor.fetchone() 의 첫 호출이 (finalized_count,) 반환.
    이후 호출은 not reached (guard 가 raise 하면 더 진행 안 함).
    """
    cur = MagicMock()
    # fetchone() 의 결과를 finalized SELECT 1회만 정의.
    cur.fetchone.return_value = (finalized_count,)
    # cursor() context manager 흉내.
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def test_upsert_video_labels_raises_finalized_labels_skip() -> None:
    """labels 테이블에 finalized row 가 있으면 FinalizedLabelsSkip 가 raise 되어야."""
    conn = _build_conn_mock(finalized_count=1)
    new_events = [{"timestamp": (0.0, 5.0), "caption": "x"}]

    with pytest.raises(FinalizedLabelsSkip) as exc_info:
        upsert_video_labels(
            dsn="postgresql://unused",
            labels_bucket="vlm-labels",
            labels_key="folder/events/clip.json",
            asset_id="asset-1",
            new_events=new_events,
            conn=conn,
        )

    assert exc_info.value.labels_key == "folder/events/clip.json"
    # guard 만 호출되고 DELETE/INSERT 로 진행 안 됐는지 — execute 호출 횟수가 1 (finalized SELECT) 이어야.
    cur = conn.cursor.return_value
    assert cur.execute.call_count == 1, f"DELETE 까지 진행됨: {cur.execute.call_args_list}"
    # 트랜잭션 정리 — rollback 호출.
    assert conn.rollback.called, "guard raise 전에 rollback() 이 호출되지 않음"


def test_upsert_video_labels_no_finalized_proceeds() -> None:
    """finalized=0 면 raise 없이 정상 flow — DELETE/INSERT 진입."""
    cur = MagicMock()
    # 1차: finalized SELECT → 0, 2차: COUNT(*) → 0 (delete count), 이후 INSERT.
    cur.fetchone.side_effect = [(0,), (0,)]
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cur

    # event 1개 — INSERT 1번 실행.
    new_events = [{"timestamp": (0.0, 5.0), "caption": "test"}]
    deleted, inserted = upsert_video_labels(
        dsn="postgresql://unused",
        labels_bucket="vlm-labels",
        labels_key="folder/events/clip.json",
        asset_id="asset-1",
        new_events=new_events,
        conn=conn,
    )

    assert deleted == 0
    assert inserted == 1
    # finalized SELECT + COUNT + DELETE + INSERT = 4
    assert cur.execute.call_count >= 4
    assert conn.commit.called, "정상 path 에서 commit() 호출되어야"


def test_update_image_labels_raises_finalized_image_skip() -> None:
    """image_labels 테이블에 finalized row 가 있으면 FinalizedImageSkip 가 raise."""
    conn = _build_conn_mock(finalized_count=1)

    with pytest.raises(FinalizedImageSkip) as exc_info:
        update_image_labels_in_db(
            dsn="postgresql://unused",
            labels_key="folder/sam3/img.json",
            object_count=3,
            conn=conn,
        )

    assert exc_info.value.labels_key == "folder/sam3/img.json"
    cur = conn.cursor.return_value
    assert cur.execute.call_count == 1, "guard 만 호출되고 UPDATE 로 진행되지 않아야"
    assert conn.rollback.called, "guard raise 전에 rollback() 이 호출되지 않음"


def test_finalized_skip_exceptions_inherit_runtime_error() -> None:
    """두 exception 이 RuntimeError 상속 — caller 가 except Exception 등 광범위 catch 시 의도대로 동작."""
    assert issubclass(FinalizedLabelsSkip, RuntimeError)
    assert issubclass(FinalizedImageSkip, RuntimeError)
    # labels_key attribute 보유 — debugging / logging 시 사용.
    assert FinalizedLabelsSkip("k").labels_key == "k"
    assert FinalizedImageSkip("k").labels_key == "k"
