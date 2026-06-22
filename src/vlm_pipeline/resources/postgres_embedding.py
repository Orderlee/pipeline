"""PG EMBEDDING 도메인 — image_embeddings (pgvector) 조회/적재.

postgres_detection.py 패턴 미러. self.connect() 컨텍스트(자동 commit/rollback/재시도)
를 사용하므로 명시 commit 불필요. vector 컬럼은 '[v1,v2,...]' 텍스트 리터럴로 바인딩
(psycopg2 vector 어댑터 불필요).
"""

from __future__ import annotations

import json
from typing import Any

from vlm_pipeline.resources.postgres_base import PostgresBaseMixin

_EMBEDDING_INSERT_SQL = """
INSERT INTO image_embeddings
  (embedding_id, entity_type, entity_id, image_id, model_name, dim, embedding,
   source_bucket, source_key, bbox, asset_id, text_content)
VALUES (%(embedding_id)s, %(entity_type)s, %(entity_id)s, %(image_id)s, %(model_name)s, %(dim)s,
        %(embedding)s::vector, %(source_bucket)s, %(source_key)s, %(bbox)s,
        %(asset_id)s, %(text_content)s)
ON CONFLICT (entity_type, entity_id, model_name) DO UPDATE SET
  embedding = EXCLUDED.embedding,
  dim = EXCLUDED.dim,
  image_id = EXCLUDED.image_id,
  source_bucket = EXCLUDED.source_bucket,
  source_key = EXCLUDED.source_key,
  bbox = EXCLUDED.bbox,
  asset_id = EXCLUDED.asset_id,
  text_content = EXCLUDED.text_content,
  created_at = now()
"""

_PENDING_FRAMES_SQL = """
SELECT im.image_id, im.image_bucket, im.image_key, im.image_role
FROM image_metadata im
WHERE im.image_bucket IS NOT NULL AND im.image_key IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM image_embeddings e
    WHERE e.entity_type = 'frame' AND e.entity_id = im.image_id AND e.model_name = %(model_name)s
)
{role_filter}
ORDER BY im.image_id
LIMIT %(limit)s
"""

_BACKLOG_COUNT_SQL = """
SELECT count(*)
FROM image_metadata im
WHERE im.image_bucket IS NOT NULL AND im.image_key IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM image_embeddings e
    WHERE e.entity_type = 'frame' AND e.entity_id = im.image_id AND e.model_name = %(model_name)s
)
{role_filter}
"""

_ROLE_FILTER = "AND im.image_role = ANY(%(roles)s)"

_VIDEO_ROLE_FILTER = "AND im.image_role = ANY(%(video_roles)s)"

_PENDING_VIDEOS_SQL = """
SELECT im.source_asset_id
FROM image_metadata im
LEFT JOIN image_embeddings fe
  ON fe.entity_type = 'frame' AND fe.entity_id = im.image_id AND fe.model_name = %(frame_model_name)s
WHERE im.source_asset_id IS NOT NULL {role_filter}
AND NOT EXISTS (SELECT 1 FROM image_embeddings ve
                WHERE ve.entity_type = 'video' AND ve.entity_id = im.source_asset_id AND ve.model_name = %(video_model_name)s)
GROUP BY im.source_asset_id
HAVING count(*) = count(fe.embedding) AND count(fe.embedding) > 0
ORDER BY im.source_asset_id LIMIT %(limit)s
"""

_VIDEO_BACKLOG_COUNT_SQL = """
SELECT count(*) FROM (
    SELECT im.source_asset_id
    FROM image_metadata im
    LEFT JOIN image_embeddings fe
      ON fe.entity_type = 'frame' AND fe.entity_id = im.image_id AND fe.model_name = %(frame_model_name)s
    WHERE im.source_asset_id IS NOT NULL {role_filter}
    AND NOT EXISTS (SELECT 1 FROM image_embeddings ve
                    WHERE ve.entity_type = 'video' AND ve.entity_id = im.source_asset_id AND ve.model_name = %(video_model_name)s)
    GROUP BY im.source_asset_id
    HAVING count(*) = count(fe.embedding) AND count(fe.embedding) > 0
) t
"""

_VIDEO_AGGREGATE_SQL = """
INSERT INTO image_embeddings
  (embedding_id, entity_type, entity_id, image_id, model_name, dim, embedding,
   source_bucket, source_key, bbox, asset_id, text_content)
SELECT
  'video|' || im.source_asset_id || '|' || %(video_model_name)s,
  'video',
  im.source_asset_id,
  NULL,
  %(video_model_name)s,
  1024,
  l2_normalize(avg(fe.embedding)),
  NULL,
  NULL,
  NULL,
  im.source_asset_id,
  NULL
FROM image_metadata im
JOIN image_embeddings fe
  ON fe.entity_type = 'frame'
 AND fe.entity_id = im.image_id
 AND fe.model_name = %(frame_model_name)s
WHERE im.source_asset_id = ANY(%(asset_ids)s) {role_filter}
GROUP BY im.source_asset_id
ON CONFLICT (entity_type, entity_id, model_name) DO UPDATE SET
  embedding = EXCLUDED.embedding,
  dim = EXCLUDED.dim,
  asset_id = EXCLUDED.asset_id,
  created_at = now()
"""

_PENDING_CAPTIONS_SQL = """
SELECT labels.label_id, labels.asset_id, labels.caption_text
FROM labels
WHERE labels.caption_text IS NOT NULL AND labels.caption_text <> ''
AND NOT EXISTS (
    SELECT 1 FROM image_embeddings e
    WHERE e.entity_type = 'caption' AND e.entity_id = labels.label_id AND e.model_name = %(model_name)s
)
ORDER BY labels.label_id
LIMIT %(limit)s
"""

_CAPTION_BACKLOG_COUNT_SQL = """
SELECT count(*)
FROM labels
WHERE labels.caption_text IS NOT NULL AND labels.caption_text <> ''
AND NOT EXISTS (
    SELECT 1 FROM image_embeddings e
    WHERE e.entity_type = 'caption' AND e.entity_id = labels.label_id AND e.model_name = %(model_name)s
)
"""

_ALL_CAPTIONS_SQL = """
SELECT labels.label_id, labels.asset_id, labels.caption_text
FROM labels
WHERE labels.caption_text IS NOT NULL AND labels.caption_text <> ''
ORDER BY labels.label_id
LIMIT %(limit)s OFFSET %(offset)s
"""


def _vector_literal(values: Any) -> str:
    """list[float] → pgvector 텍스트 리터럴 '[v1,v2,...]'."""
    return "[" + ",".join(repr(float(x)) for x in values) + "]"


def _to_insert_param(row: dict[str, Any]) -> dict[str, Any]:
    """임베딩 row dict → executemany 파라미터 (vector 리터럴 + bbox json + 기본 None).

    frame rows: image_id set, asset_id/text_content None (setdefault).
    caption rows: image_id=None allowed, asset_id/text_content set.
    """
    p = dict(row)
    p["embedding"] = _vector_literal(row["embedding"])
    p["bbox"] = json.dumps(row["bbox"]) if row.get("bbox") is not None else None
    p.setdefault("source_bucket", None)
    p.setdefault("source_key", None)
    p.setdefault("image_id", None)
    p.setdefault("asset_id", None)
    p.setdefault("text_content", None)
    return p


class PostgresEmbeddingMixin:
    """image_embeddings 조회/적재 메서드."""

    def find_pending_frame_embeddings(
        self, model_name: str, limit: int = 500, image_roles: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """해당 model_name 임베딩이 아직 없는 프레임(image_metadata)을 조회."""
        params: dict[str, Any] = {"model_name": model_name, "limit": limit}
        role_filter = ""
        if image_roles:
            role_filter = _ROLE_FILTER
            params["roles"] = list(image_roles)
        sql = _PENDING_FRAMES_SQL.format(role_filter=role_filter)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return PostgresBaseMixin._cursor_to_dicts(cur)

    def count_frame_backlog(self, model_name: str, image_roles: list[str] | None = None) -> int:
        """미임베딩 프레임 수 (sensor backlog 판단용)."""
        params: dict[str, Any] = {"model_name": model_name}
        role_filter = ""
        if image_roles:
            role_filter = _ROLE_FILTER
            params["roles"] = list(image_roles)
        sql = _BACKLOG_COUNT_SQL.format(role_filter=role_filter)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return int(cur.fetchone()[0])

    def find_pending_caption_embeddings(self, model_name: str, limit: int = 500) -> list[dict[str, Any]]:
        """해당 model_name caption 임베딩이 아직 없는 labels 행을 조회."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_PENDING_CAPTIONS_SQL, {"model_name": model_name, "limit": limit})
                return PostgresBaseMixin._cursor_to_dicts(cur)

    def find_all_caption_embeddings(self, limit: int = 500, offset: int = 0) -> list[dict[str, Any]]:
        """force_reembed 용: NOT EXISTS 가드 없이 모든 caption 을 조회 (model_name 무관).

        기존 row 는 batch_insert_embeddings 의 ON CONFLICT upsert 로 덮어쓴다.
        offset 을 사용해 호출측이 전체를 페이지 순회할 수 있다.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_ALL_CAPTIONS_SQL, {"limit": limit, "offset": offset})
                return PostgresBaseMixin._cursor_to_dicts(cur)

    def count_caption_backlog(self, model_name: str) -> int:
        """미임베딩 caption 수 (sensor backlog 판단용)."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_CAPTION_BACKLOG_COUNT_SQL, {"model_name": model_name})
                return int(cur.fetchone()[0])

    def find_pending_video_embeddings(
        self,
        video_model_name: str,
        frame_model_name: str,
        limit: int = 500,
        video_roles: list[str] | None = None,
    ) -> list[str]:
        """frame embeddings 는 있지만 video embedding 이 없는 source_asset_id 목록 반환."""
        params: dict[str, Any] = {
            "video_model_name": video_model_name,
            "frame_model_name": frame_model_name,
            "limit": limit,
        }
        role_filter = ""
        if video_roles:
            role_filter = _VIDEO_ROLE_FILTER
            params["video_roles"] = list(video_roles)
        sql = _PENDING_VIDEOS_SQL.format(role_filter=role_filter)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return [str(row[0]) for row in cur.fetchall()]

    def count_video_backlog(
        self,
        video_model_name: str,
        frame_model_name: str,
        video_roles: list[str] | None = None,
    ) -> int:
        """미임베딩 video 수 (sensor backlog 판단용)."""
        params: dict[str, Any] = {
            "video_model_name": video_model_name,
            "frame_model_name": frame_model_name,
        }
        role_filter = ""
        if video_roles:
            role_filter = _VIDEO_ROLE_FILTER
            params["video_roles"] = list(video_roles)
        sql = _VIDEO_BACKLOG_COUNT_SQL.format(role_filter=role_filter)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return int(cur.fetchone()[0])

    def aggregate_video_embeddings_framepool(
        self,
        video_model_name: str,
        frame_model_name: str,
        asset_ids: list[str],
        video_roles: list[str] | None = None,
    ) -> int:
        """SQL-side L2-normalized mean of frame embeddings → video embedding upsert.

        pgvector 0.8.2 avg(vector) + l2_normalize(vector) 를 활용해 Python 메모리로
        벡터를 끌어오지 않고 집계한다. 반환값은 upsert 된 행 수.
        """
        if not asset_ids:
            return 0
        params: dict[str, Any] = {
            "video_model_name": video_model_name,
            "frame_model_name": frame_model_name,
            "asset_ids": list(asset_ids),
        }
        role_filter = ""
        if video_roles:
            role_filter = _VIDEO_ROLE_FILTER
            params["video_roles"] = list(video_roles)
        sql = _VIDEO_AGGREGATE_SQL.format(role_filter=role_filter)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0

    def batch_insert_embeddings(self, rows: list[dict[str, Any]]) -> int:
        """임베딩 row 배치 upsert (executemany + ON CONFLICT). 재실행 안전."""
        if not rows:
            return 0
        payload = [_to_insert_param(r) for r in rows]
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(_EMBEDDING_INSERT_SQL, payload)
        return len(payload)
