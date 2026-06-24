"""ls_sync_db.replace_image_label_annotations() 단위 검증 (mock PG).

image_label_id 기준 DELETE→INSERT 재투영 + annotation_id 멱등성(sha1) 회귀 방지.
실제 PG 없이 cursor mock 으로 execute 호출 시퀀스를 검증한다.
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

_GEMINI_DIR = Path(__file__).resolve().parents[2] / "src" / "gemini"
if str(_GEMINI_DIR) not in sys.path:
    sys.path.insert(0, str(_GEMINI_DIR))

from ls_sync_db import replace_image_label_annotations  # noqa: E402


def _conn_mock(existing_count: int) -> MagicMock:
    cur = MagicMock()
    cur.fetchone.return_value = (existing_count,)  # 첫 COUNT(*) 결과
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def _boxes(n: int) -> list[dict]:
    return [
        {
            "box_index": i,
            "category": "fire",
            "bbox_x": 1.0 * i,
            "bbox_y": 2.0,
            "bbox_w": 3.0,
            "bbox_h": 4.0,
            "score": 0.5,
        }
        for i in range(n)
    ]


def test_delete_then_insert_per_box_returns_counts() -> None:
    conn = _conn_mock(existing_count=3)
    deleted, inserted = replace_image_label_annotations(conn, "ilid-1", "img-1", _boxes(2))
    assert (deleted, inserted) == (3, 2)
    cur = conn.cursor.return_value
    # COUNT(1) + DELETE(1) + INSERT(2) = 4
    assert cur.execute.call_count == 4
    sqls = [c.args[0] for c in cur.execute.call_args_list]
    assert "DELETE FROM image_label_annotations" in sqls[1]
    assert all("INSERT INTO image_label_annotations" in s for s in sqls[2:])


def test_annotation_id_is_idempotent_sha1() -> None:
    conn = _conn_mock(existing_count=0)
    replace_image_label_annotations(conn, "ilid-XYZ", "img-1", _boxes(1))
    cur = conn.cursor.return_value
    insert_call = cur.execute.call_args_list[2]  # COUNT, DELETE, then INSERT
    params = insert_call.args[1]
    expected = hashlib.sha1(b"ilid-XYZ|0").hexdigest()
    assert params[0] == expected  # annotation_id 결정적
    assert params[1] == "ilid-XYZ"  # image_label_id
    assert params[3] == 0  # box_index


def test_empty_boxes_deletes_only() -> None:
    conn = _conn_mock(existing_count=5)
    deleted, inserted = replace_image_label_annotations(conn, "ilid-2", None, [])
    assert (deleted, inserted) == (5, 0)
    cur = conn.cursor.return_value
    assert cur.execute.call_count == 2  # COUNT + DELETE, no INSERT
