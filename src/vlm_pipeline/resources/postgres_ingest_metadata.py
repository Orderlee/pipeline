"""PG INGEST 도메인 — image/video metadata CRUD (DuckDBIngestMetadataMixin 1:1 포팅).

핵심 변환:
  - ``INSERT OR REPLACE INTO image_metadata`` → ``INSERT ... ON CONFLICT (image_id) DO UPDATE``
  - ``regexp_extract(col, pattern)`` → ``substring(col FROM pattern)``
  - DuckDB BEGIN/COMMIT/ROLLBACK 명시 → ``connect()`` ctxmgr 의 자동 commit/rollback
  - ``SELECT *`` 후 ``conn.description`` → cursor.description (cursor 살아있는 동안 캡처)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from vlm_pipeline.resources.postgres_video_metadata import PostgresVideoMetadataMixin


class PostgresIngestMetadataMixin(PostgresVideoMetadataMixin):
    """image_metadata, video_metadata CRUD 및 프레임/캡션/프로세스 쿼리 mixin."""

    @staticmethod
    def _normalized_image_caption_text(payload: dict[str, Any]) -> Any:
        image_caption_text = payload.get("image_caption_text")
        if image_caption_text not in (None, ""):
            return image_caption_text
        return payload.get("caption_text")

    # ------------------------------------------------------------------
    # image_metadata
    # ------------------------------------------------------------------

    def insert_image_metadata(self, asset_id: str, meta: dict) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO image_metadata (
                        image_id, source_asset_id, source_clip_id, image_bucket, image_key,
                        image_role, frame_index, frame_sec, checksum, file_size,
                        width, height, color_mode, bit_depth,
                        has_alpha, orientation, image_caption_text, image_caption_score,
                        image_caption_bucket, image_caption_key, image_caption_generated_at, extracted_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        meta.get("image_id") or asset_id,
                        asset_id,
                        meta.get("source_clip_id"),
                        meta.get("image_bucket", "vlm-raw"),
                        meta.get("image_key"),
                        meta.get("image_role", "source_image"),
                        meta.get("frame_index"),
                        meta.get("frame_sec"),
                        meta.get("checksum"),
                        meta.get("file_size"),
                        meta.get("width"),
                        meta.get("height"),
                        meta.get("color_mode", "RGB"),
                        meta.get("bit_depth", 8),
                        meta.get("has_alpha", False),
                        meta.get("orientation", 1),
                        self._normalized_image_caption_text(meta),
                        meta.get("image_caption_score"),
                        meta.get("image_caption_bucket"),
                        meta.get("image_caption_key"),
                        meta.get("image_caption_generated_at"),
                        meta.get("extracted_at", datetime.now()),
                    ),
                )

    def find_image_metadata_by_image_key(self, image_key: str) -> dict[str, Any] | None:
        normalized_key = self._norm_str(image_key)
        if not normalized_key:
            return None
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM image_metadata
                    WHERE image_key = %s
                    LIMIT 1
                    """,
                    (normalized_key,),
                )
                return self._cursor_to_dict(cur)

    def find_image_metadata_by_image_id(self, image_id: str) -> dict[str, Any] | None:
        normalized_id = self._norm_str(image_id)
        if not normalized_id:
            return None
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM image_metadata
                    WHERE image_id = %s
                    LIMIT 1
                    """,
                    (normalized_id,),
                )
                return self._cursor_to_dict(cur)

    def find_image_metadata_by_stem(
        self,
        stem: str,
        *,
        source_unit_name: str | None = None,
    ) -> dict[str, Any] | None:
        normalized_stem = self._norm_str(stem)
        if not normalized_stem:
            return None
        with self.connect() as conn:
            # DuckDB regexp_extract(im.image_key, '[^/]+$') → PG substring(...)
            query = """
                SELECT im.*
                FROM image_metadata im
                JOIN raw_files r ON r.asset_id = im.source_asset_id
                WHERE substring(im.image_key FROM '[^/]+$') LIKE %s || '.%%'
            """
            params: list[Any] = [normalized_stem]
            if source_unit_name:
                query += " AND r.source_unit_name = %s"
                params.append(str(source_unit_name))
            query += " ORDER BY im.extracted_at DESC LIMIT 1"
            with conn.cursor() as cur:
                cur.execute(query, params)
                return self._cursor_to_dict(cur)

    def upsert_image_metadata_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        payload_rows = []
        for row in rows:
            payload_rows.append(
                (
                    row.get("image_id") or str(uuid4()),
                    row.get("source_asset_id"),
                    row.get("source_clip_id"),
                    row.get("image_bucket", "vlm-processed"),
                    row.get("image_key"),
                    row.get("image_role", "processed_clip_frame"),
                    row.get("frame_index"),
                    row.get("frame_sec"),
                    row.get("checksum"),
                    row.get("file_size"),
                    row.get("width"),
                    row.get("height"),
                    row.get("color_mode", "RGB"),
                    row.get("bit_depth", 8),
                    row.get("has_alpha", False),
                    row.get("orientation", 1),
                    self._normalized_image_caption_text(row),
                    row.get("image_caption_score"),
                    row.get("image_caption_bucket"),
                    row.get("image_caption_key"),
                    row.get("image_caption_generated_at"),
                    row.get("extracted_at", datetime.now()),
                )
            )
        # DuckDB INSERT OR REPLACE → PG ON CONFLICT (image_id) DO UPDATE.
        #
        # *Invariant*: caller 는 deterministic ``image_id`` 를 전달해야 한다.
        # ``defs/label/artifact_resolve.py`` 의 ``_stable_id(asset_id, clip_id, image_key)``
        # 가 SHA1 기반이라 같은 (asset_id, source_clip_id, image_key) 는 같은 image_id
        # 를 만들고, 따라서 PK 충돌 ↔ (image_bucket, image_key) UNIQUE 충돌 ↔
        # (asset, clip, role, frame_index) UNIQUE 충돌이 동치다.
        #
        # 만약 future caller 가 ``uuid4()`` 같은 비결정적 image_id 를 사용하면서
        # 같은 image_key 를 재삽입하면 PG 가 secondary UNIQUE 위반으로 hard error
        # 를 낸다 (DuckDB era 는 silent replace). fail-loud 라 데이터 손상은 없지만,
        # 발생 시 caller 측을 _stable_id 기반으로 수정해야 한다.
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO image_metadata (
                        image_id, source_asset_id, source_clip_id, image_bucket, image_key,
                        image_role, frame_index, frame_sec, checksum, file_size,
                        width, height, color_mode, bit_depth,
                        has_alpha, orientation, image_caption_text, image_caption_score,
                        image_caption_bucket, image_caption_key, image_caption_generated_at, extracted_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (image_id) DO UPDATE SET
                        source_asset_id = EXCLUDED.source_asset_id,
                        source_clip_id = EXCLUDED.source_clip_id,
                        image_bucket = EXCLUDED.image_bucket,
                        image_key = EXCLUDED.image_key,
                        image_role = EXCLUDED.image_role,
                        frame_index = EXCLUDED.frame_index,
                        frame_sec = EXCLUDED.frame_sec,
                        checksum = EXCLUDED.checksum,
                        file_size = EXCLUDED.file_size,
                        width = EXCLUDED.width,
                        height = EXCLUDED.height,
                        color_mode = EXCLUDED.color_mode,
                        bit_depth = EXCLUDED.bit_depth,
                        has_alpha = EXCLUDED.has_alpha,
                        orientation = EXCLUDED.orientation,
                        image_caption_text = EXCLUDED.image_caption_text,
                        image_caption_score = EXCLUDED.image_caption_score,
                        image_caption_bucket = EXCLUDED.image_caption_bucket,
                        image_caption_key = EXCLUDED.image_caption_key,
                        image_caption_generated_at = EXCLUDED.image_caption_generated_at,
                        extracted_at = EXCLUDED.extracted_at
                    """,
                    payload_rows,
                )
        return len(payload_rows)

    def update_image_caption_metadata(
        self,
        image_id: str,
        *,
        image_caption_text: str,
        caption_score: float | None,
        caption_bucket: str,
        caption_key: str,
        generated_at: datetime | None,
    ) -> None:
        normalized_id = self._norm_str(image_id)
        if not normalized_id:
            return
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE image_metadata
                    SET image_caption_text = %s,
                        image_caption_score = %s,
                        image_caption_bucket = %s,
                        image_caption_key = %s,
                        image_caption_generated_at = %s,
                        extracted_at = COALESCE(extracted_at, CURRENT_TIMESTAMP)
                    WHERE image_id = %s
                    """,
                    (
                        image_caption_text,
                        caption_score,
                        caption_bucket,
                        caption_key,
                        generated_at,
                        normalized_id,
                    ),
                )

    def replace_video_frame_metadata(
        self,
        source_asset_id: str,
        frames: list[dict[str, Any]],
        *,
        image_role: str = "video_frame",
    ) -> int:
        return self._replace_frame_metadata(
            source_asset_id=source_asset_id,
            source_clip_id=None,
            image_role=image_role,
            frames=frames,
        )

    def replace_processed_clip_frame_metadata(
        self,
        source_asset_id: str,
        source_clip_id: str,
        frames: list[dict[str, Any]],
    ) -> int:
        return self._replace_frame_metadata(
            source_asset_id=source_asset_id,
            source_clip_id=source_clip_id,
            image_role="processed_clip_frame",
            frames=frames,
        )

    def _replace_frame_metadata(
        self,
        *,
        source_asset_id: str,
        source_clip_id: str | None,
        image_role: str,
        frames: list[dict[str, Any]],
    ) -> int:
        normalized_id = self._norm_str(source_asset_id)
        if not normalized_id:
            return 0
        normalized_clip_id = self._norm_str(source_clip_id) or None

        rows: list[tuple] = []
        for frame in frames:
            rows.append(
                (
                    frame.get("image_id") or str(uuid4()),
                    normalized_id,
                    frame.get("source_clip_id", normalized_clip_id),
                    frame.get("image_bucket", "vlm-processed"),
                    frame.get("image_key"),
                    frame.get("image_role", image_role),
                    frame.get("frame_index"),
                    frame.get("frame_sec"),
                    frame.get("checksum"),
                    frame.get("file_size"),
                    frame.get("width"),
                    frame.get("height"),
                    frame.get("color_mode", "RGB"),
                    frame.get("bit_depth", 8),
                    frame.get("has_alpha", False),
                    frame.get("orientation", 1),
                    self._normalized_image_caption_text(frame),
                    frame.get("image_caption_score"),
                    frame.get("image_caption_bucket"),
                    frame.get("image_caption_key"),
                    frame.get("image_caption_generated_at"),
                    frame.get("extracted_at", datetime.now()),
                )
            )

        # connect() ctxmgr 의 outer transaction 안에서 DELETE + INSERT 가
        # atomic 하게 묶인다. 예외 시 자동 ROLLBACK.
        with self.connect() as conn:
            with conn.cursor() as cur:
                if normalized_clip_id is not None:
                    cur.execute(
                        """
                        DELETE FROM image_metadata
                        WHERE source_asset_id = %s
                          AND image_role = %s
                          AND source_clip_id = %s
                        """,
                        (normalized_id, image_role, normalized_clip_id),
                    )
                else:
                    cur.execute(
                        """
                        DELETE FROM image_metadata
                        WHERE source_asset_id = %s
                          AND image_role = %s
                          AND source_clip_id IS NULL
                        """,
                        (normalized_id, image_role),
                    )
                if rows:
                    cur.executemany(
                        """
                        INSERT INTO image_metadata (
                            image_id, source_asset_id, source_clip_id, image_bucket, image_key,
                            image_role, frame_index, frame_sec, checksum, file_size,
                            width, height, color_mode, bit_depth,
                            has_alpha, orientation, image_caption_text, image_caption_score,
                            image_caption_bucket, image_caption_key, image_caption_generated_at, extracted_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        rows,
                    )
        return len(rows)
