"""Detection sensor 순수 헬퍼 (no Dagster dependency).

DB 읽기 / cursor 파싱 로직만 담는다. Dagster 의존 코드는
defs/shared/detection_sensor_factory.py 에 위치.
"""

from __future__ import annotations

import json

from vlm_pipeline.lib.sensor_db import open_sensor_read_connection


# ---------------------------------------------------------------------------
# Backlog snapshot (Postgres read-only)
# ---------------------------------------------------------------------------


def read_backlog_snapshot(label_tool: str) -> dict[str, int | str | None]:
    """processed_clip_frame 중 *label_tool* 결과가 없는 이미지 수를 읽는다."""
    conn = None
    try:
        conn = open_sensor_read_connection()
        with conn.cursor() as cur:
            # 테이블 존재성 — try-except 로 mode-agnostic.
            try:
                cur.execute("SELECT 1 FROM image_labels LIMIT 0")
            except Exception:
                return {"backlog_count": 0, "state_token": "no_table"}

            cur.execute(
                """
                SELECT
                    COUNT(*) AS backlog_count,
                    CAST(MAX(im.extracted_at) AS TEXT) AS latest_extracted_at
                FROM image_metadata im
                WHERE im.image_role IN ('processed_clip_frame', 'raw_video_frame')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM image_labels il
                      WHERE il.image_id = im.image_id
                        AND il.label_tool = %s
                  )
                """,
                (label_tool,),
            )
            row = cur.fetchone()
        backlog_count = int(row[0]) if row and row[0] is not None else 0
        latest = str(row[1]) if row and row[1] is not None else None
        state_token = f"count={backlog_count}|latest={latest or ''}"
        return {
            "backlog_count": backlog_count,
            "latest_extracted_at": latest,
            "state_token": state_token,
        }
    finally:
        if conn is not None:
            conn.close()


# ---------------------------------------------------------------------------
# Cursor 파싱
# ---------------------------------------------------------------------------


def parse_sensor_cursor(raw_cursor: str | None) -> tuple[int | None, str | None, int]:
    """센서 cursor JSON을 (last_count, last_state_token, event_seq) 로 파싱."""
    if not raw_cursor:
        return None, None, 0
    try:
        payload = json.loads(raw_cursor)
    except json.JSONDecodeError:
        return None, None, 0
    if not isinstance(payload, dict):
        return None, None, 0
    try:
        last_count = int(payload.get("last_count"))
    except (TypeError, ValueError):
        last_count = None
    try:
        event_seq = int(payload.get("event_seq", 0))
    except (TypeError, ValueError):
        event_seq = 0
    last_state_token = payload.get("last_state_token")
    return last_count, str(last_state_token) if last_state_token else None, max(0, event_seq)
