"""PG DEDUP 도메인 — phash, dup_group, similar 검색 등 (DuckDBDedupMixin 1:1 포팅).

핵심 변환:
  - ``INSERT OR REPLACE INTO labels`` → ``ON CONFLICT (label_id) DO UPDATE``
  - DuckDB ``LIST(DISTINCT col)`` → PG ``array_agg(DISTINCT col)``
  - DuckDB ``ANY_VALUE(col)`` → PG 15 호환 ``(array_agg(col))[1]`` (PG 16+ 면 ANY_VALUE 가능하지만 PG 15 fallback)
  - DuckDB ``substr(col, 1, n)`` → PG ``substr(col, 1, n)`` (동일)
  - ``processed_clips`` upsert 는 이미 PG-style ``ON CONFLICT`` 였음 (변경 없음)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4


_LABELS_INSERT_SQL = """
INSERT INTO labels (
    label_id, asset_id, labels_bucket, labels_key,
    label_format, label_tool, label_source, review_status,
    event_index, event_count, timestamp_start_sec, timestamp_end_sec,
    caption_text, object_count, label_status, created_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (label_id) DO UPDATE SET
    asset_id = EXCLUDED.asset_id,
    labels_bucket = EXCLUDED.labels_bucket,
    labels_key = EXCLUDED.labels_key,
    label_format = EXCLUDED.label_format,
    label_tool = EXCLUDED.label_tool,
    label_source = EXCLUDED.label_source,
    review_status = EXCLUDED.review_status,
    event_index = EXCLUDED.event_index,
    event_count = EXCLUDED.event_count,
    timestamp_start_sec = EXCLUDED.timestamp_start_sec,
    timestamp_end_sec = EXCLUDED.timestamp_end_sec,
    caption_text = EXCLUDED.caption_text,
    object_count = EXCLUDED.object_count,
    label_status = EXCLUDED.label_status,
    created_at = EXCLUDED.created_at
"""


class PostgresDedupMixin:
    """DEDUP 관련 PostgreSQL 메서드 mixin."""

    def find_phash_null(self, limit: int = 100) -> list[dict]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT asset_id, raw_bucket, raw_key, archive_path, source_path
                    FROM raw_files
                    WHERE phash IS NULL
                      AND media_type = 'image'
                      AND ingest_status = 'completed'
                    ORDER BY created_at
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "archive_path", "source_path"]
            return [dict(zip(columns, row)) for row in rows]

    def find_inline_dedup_targets(
        self,
        *,
        prioritized_asset_ids: list[str],
        limit: int,
    ) -> list[dict]:
        """Inline DEDUP 대상 조회 — 현재 manifest 자산 우선, 남는 슬롯은 backlog.

        prioritized_asset_ids 가 raw_files 에 존재(media_type=image, ingest=completed, phash NULL)하면
        그 자산들을 먼저 반환. 그 외 슬롯은 동일 조건의 backlog 로 채움(prioritized 는 제외).
        """
        normalized_limit = max(1, int(limit))
        prioritized_set = {
            str(asset_id).strip()
            for asset_id in (prioritized_asset_ids or [])
            if str(asset_id).strip()
        }
        columns = ["asset_id", "raw_bucket", "raw_key", "archive_path", "source_path"]
        with self.connect() as conn:
            with conn.cursor() as cur:
                prioritized_targets: list[dict] = []
                if prioritized_set:
                    placeholders = ", ".join(["%s"] * len(prioritized_set))
                    cur.execute(
                        f"""
                        SELECT asset_id, raw_bucket, raw_key, archive_path, source_path
                        FROM raw_files
                        WHERE asset_id IN ({placeholders})
                          AND media_type = 'image'
                          AND ingest_status = 'completed'
                          AND phash IS NULL
                        ORDER BY created_at
                        """,
                        tuple(prioritized_set),
                    )
                    prioritized_targets = [
                        dict(zip(columns, row)) for row in cur.fetchall()
                    ]

                remaining_limit = max(0, normalized_limit - len(prioritized_targets))
                if remaining_limit <= 0:
                    return prioritized_targets

                params: list[Any] = []
                exclude_sql = ""
                if prioritized_set:
                    placeholders = ", ".join(["%s"] * len(prioritized_set))
                    exclude_sql = f"AND asset_id NOT IN ({placeholders})"
                    params.extend(prioritized_set)
                params.append(remaining_limit)

                cur.execute(
                    f"""
                    SELECT asset_id, raw_bucket, raw_key, archive_path, source_path
                    FROM raw_files
                    WHERE media_type = 'image'
                      AND ingest_status = 'completed'
                      AND phash IS NULL
                      {exclude_sql}
                    ORDER BY created_at
                    LIMIT %s
                    """,
                    tuple(params),
                )
                backlog_targets = [
                    dict(zip(columns, row)) for row in cur.fetchall()
                ]

        return prioritized_targets + backlog_targets

    def mark_inline_dedup_failure(self, asset_id: str, error_message: str) -> None:
        """Inline DEDUP 실패시 raw_files.error_message 에 phash_failed:<msg> 기록."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE raw_files
                    SET error_message = %s, updated_at = %s
                    WHERE asset_id = %s
                    """,
                    (f"phash_failed:{error_message}", datetime.now(), asset_id),
                )

    def update_phash(self, asset_id: str, phash: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE raw_files SET phash = %s, updated_at = %s WHERE asset_id = %s",
                    (phash, datetime.now(), asset_id),
                )

    def clear_error_message(self, asset_id: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE raw_files
                    SET error_message = NULL, updated_at = %s
                    WHERE asset_id = %s
                      AND error_message LIKE 'phash_%%'
                    """,
                    (datetime.now(), asset_id),
                )

    def update_dup_group(self, asset_id: str, group_id: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE raw_files SET dup_group_id = %s, updated_at = %s WHERE asset_id = %s",
                    (group_id, datetime.now(), asset_id),
                )

    def find_similar_phash(
        self,
        phash_hex: str,
        threshold: int = 5,
        exclude_asset_id: str | None = None,
        prefix_hex_len: int = 2,
    ) -> list[dict]:
        if not phash_hex:
            return []

        normalized = str(phash_hex).strip().lower()
        try:
            target_int = int(normalized, 16)
        except ValueError:
            return []

        candidates = self._phash_prefix_candidates(
            phash_hex=normalized,
            threshold=threshold,
            prefix_hex_len=prefix_hex_len,
        )
        normalized_prefix_len = max(1, min(4, int(prefix_hex_len)))
        all_prefix_count = 1 << (normalized_prefix_len * 4)

        with self.connect() as conn:
            where_parts = ["phash IS NOT NULL"]
            params: list[Any] = []

            if exclude_asset_id:
                where_parts.append("asset_id != %s")
                params.append(exclude_asset_id)

            if candidates and len(candidates) < all_prefix_count:
                placeholders = ", ".join(["%s"] * len(candidates))
                where_parts.append(
                    f"lower(substr(phash, 1, {normalized_prefix_len})) IN ({placeholders})"
                )
                params.extend(candidates)

            query = (
                "SELECT asset_id, phash FROM raw_files "
                f"WHERE {' AND '.join(where_parts)}"
            )
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

        result: list[dict[str, Any]] = []
        for asset_id, other_phash in rows:
            if not other_phash:
                continue
            try:
                distance = (target_int ^ int(str(other_phash), 16)).bit_count()
            except ValueError:
                continue
            if distance <= threshold:
                result.append(
                    {"asset_id": asset_id, "phash": other_phash, "distance": distance}
                )
        result.sort(key=lambda row: (row["distance"], str(row["asset_id"])))
        return result

    def insert_label(self, label: dict) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _LABELS_INSERT_SQL,
                    (
                        label.get("label_id") or str(uuid4()),
                        label.get("asset_id"),
                        label.get("labels_bucket", "vlm-labels"),
                        label.get("labels_key"),
                        label.get("label_format"),
                        label.get("label_tool", "pre-built"),
                        label.get("label_source", "manual"),
                        label.get("review_status", "pending"),
                        label.get("event_index", 0),
                        label.get("event_count"),
                        label.get("timestamp_start_sec"),
                        label.get("timestamp_end_sec"),
                        label.get("caption_text"),
                        label.get("object_count", 0),
                        label.get("label_status", "completed"),
                        label.get("created_at", datetime.now()),
                    ),
                )

    def find_processable(
        self,
        folder_name: str | None = None,
        *,
        spec_id: str | None = None,
    ) -> list[dict]:
        with self.connect() as conn:
            where_clauses = [
                "r.ingest_status IN ('completed', 'ready_for_labeling')",
                "l.label_status = 'completed'",
                """
                  NOT EXISTS (
                      SELECT 1
                      FROM processed_clips pc
                      WHERE pc.source_label_id = l.label_id
                        AND pc.process_status = 'completed'
                  )
                """.strip(),
            ]
            params: list[Any] = []
            if folder_name:
                where_clauses.append("r.raw_key LIKE %s")
                params.append(f"{folder_name}/%")
            if spec_id:
                columns = self._table_columns(conn, "raw_files")
                if "spec_id" not in columns:
                    return []
                where_clauses.append("r.spec_id = %s")
                params.append(str(spec_id))

            where_sql = "\n                  AND ".join(where_clauses)
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        r.asset_id,
                        r.raw_bucket,
                        r.raw_key,
                        r.media_type,
                        r.archive_path,
                        r.source_path,
                        l.label_id,
                        l.labels_bucket,
                        l.labels_key,
                        l.label_source,
                        l.review_status,
                        l.event_index,
                        l.timestamp_start_sec,
                        l.timestamp_end_sec,
                        l.caption_text,
                        l.object_count,
                        vm.duration_sec AS video_duration_sec,
                        vm.width AS video_width,
                        vm.height AS video_height,
                        vm.codec AS video_codec
                    FROM raw_files r
                    JOIN labels l ON r.asset_id = l.asset_id
                    LEFT JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE {where_sql}
                    """,
                    params,
                )
                rows = cur.fetchall()
            columns = [
                "asset_id", "raw_bucket", "raw_key", "media_type",
                "archive_path", "source_path",
                "label_id", "labels_bucket", "labels_key",
                "label_source", "review_status", "event_index",
                "timestamp_start_sec", "timestamp_end_sec",
                "caption_text", "object_count",
                "video_duration_sec",
                "video_width", "video_height", "video_codec",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def insert_processed_clip(self, clip: dict) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO processed_clips (
                        clip_id, source_asset_id, source_label_id, event_index,
                        clip_start_sec, clip_end_sec, checksum, file_size,
                        processed_bucket, clip_key, label_key, data_source, caption_text,
                        width, height, codec, duration_sec, fps, frame_count,
                        image_extract_status, image_extract_count, image_extract_error, image_extracted_at,
                        process_status, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (clip_id) DO UPDATE SET
                        source_asset_id = EXCLUDED.source_asset_id,
                        source_label_id = EXCLUDED.source_label_id,
                        event_index = EXCLUDED.event_index,
                        clip_start_sec = EXCLUDED.clip_start_sec,
                        clip_end_sec = EXCLUDED.clip_end_sec,
                        checksum = EXCLUDED.checksum,
                        file_size = EXCLUDED.file_size,
                        processed_bucket = EXCLUDED.processed_bucket,
                        clip_key = EXCLUDED.clip_key,
                        label_key = EXCLUDED.label_key,
                        data_source = EXCLUDED.data_source,
                        caption_text = EXCLUDED.caption_text,
                        width = EXCLUDED.width,
                        height = EXCLUDED.height,
                        codec = EXCLUDED.codec,
                        duration_sec = EXCLUDED.duration_sec,
                        fps = EXCLUDED.fps,
                        frame_count = EXCLUDED.frame_count,
                        image_extract_status = EXCLUDED.image_extract_status,
                        image_extract_count = EXCLUDED.image_extract_count,
                        image_extract_error = EXCLUDED.image_extract_error,
                        image_extracted_at = EXCLUDED.image_extracted_at,
                        process_status = EXCLUDED.process_status,
                        created_at = EXCLUDED.created_at
                    """,
                    (
                        clip.get("clip_id") or str(uuid4()),
                        clip.get("source_asset_id"),
                        clip.get("source_label_id"),
                        clip.get("event_index", 0),
                        clip.get("clip_start_sec"),
                        clip.get("clip_end_sec"),
                        clip.get("checksum"),
                        clip.get("file_size"),
                        clip.get("processed_bucket", "vlm-processed"),
                        clip.get("clip_key"),
                        clip.get("label_key"),
                        clip.get("data_source", "manual"),
                        clip.get("caption_text"),
                        clip.get("width"),
                        clip.get("height"),
                        clip.get("codec"),
                        clip.get("duration_sec"),
                        clip.get("fps"),
                        clip.get("frame_count"),
                        clip.get("image_extract_status", "pending"),
                        clip.get("image_extract_count", 0),
                        clip.get("image_extract_error"),
                        clip.get("image_extracted_at"),
                        clip.get("process_status", "completed"),
                        clip.get("created_at", datetime.now()),
                    ),
                )

    def find_processed_clip_by_checksum(
        self,
        checksum: str,
        *,
        source_asset_id: str | None = None,
        exclude_clip_id: str | None = None,
    ) -> dict[str, Any] | None:
        normalized_checksum = str(checksum or "").strip()
        if not normalized_checksum:
            return None

        with self.connect() as conn:
            where_clauses = ["checksum = %s"]
            params: list[Any] = [normalized_checksum]
            if source_asset_id:
                where_clauses.append("source_asset_id = %s")
                params.append(str(source_asset_id))
            if exclude_clip_id:
                where_clauses.append("clip_id != %s")
                params.append(str(exclude_clip_id))

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        clip_id,
                        source_asset_id,
                        source_label_id,
                        event_index,
                        clip_start_sec,
                        clip_end_sec,
                        checksum,
                        clip_key,
                        process_status
                    FROM processed_clips
                    WHERE {' AND '.join(where_clauses)}
                    ORDER BY created_at
                    LIMIT 1
                    """,
                    params,
                )
                row = cur.fetchone()
            if row is None:
                return None
            columns = [
                "clip_id",
                "source_asset_id",
                "source_label_id",
                "event_index",
                "clip_start_sec",
                "clip_end_sec",
                "checksum",
                "clip_key",
                "process_status",
            ]
            return dict(zip(columns, row))

    def find_projects_for_dataset_build(self) -> list[dict]:
        """source_unit_name 별 video/image 카운트."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.source_unit_name AS folder,
                        COUNT(DISTINCT CASE WHEN r.media_type = 'video' THEN r.asset_id END) AS video_count,
                        COUNT(DISTINCT im.image_id) AS image_count
                    FROM raw_files r
                    LEFT JOIN image_metadata im ON im.source_asset_id = r.asset_id
                    WHERE r.source_unit_name IS NOT NULL
                      AND r.source_unit_name <> ''
                    GROUP BY r.source_unit_name
                    HAVING COUNT(DISTINCT CASE WHEN r.media_type = 'video' THEN r.asset_id END) > 0
                        OR COUNT(DISTINCT im.image_id) > 0
                    ORDER BY folder
                    """
                )
                rows = cur.fetchall()
            return [
                {"folder": row[0], "video_count": row[1], "image_count": row[2]}
                for row in rows
            ]

    def find_project_video_candidates(
        self, folder: str, require_ls_finalized: bool = False
    ) -> list[dict]:
        finalized_filter = (
            """
                  AND EXISTS (
                      SELECT 1 FROM labels l
                      JOIN raw_files r2 ON r2.asset_id = l.asset_id
                      WHERE r2.source_unit_name = r.source_unit_name
                        AND l.review_status = 'finalized'
                  )
            """
            if require_ls_finalized
            else ""
        )
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT r.asset_id, r.raw_bucket, r.raw_key,
                           r.source_type, r.genai_engine, r.label_policy
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND r.source_unit_name = %s
                      AND r.raw_key IS NOT NULL
                      AND (r.source_type IS NULL OR r.source_type IN ('camera','nas_upload'))
                      {finalized_filter}
                    """,
                    (folder,),
                )
                rows = cur.fetchall()
            return [
                {
                    "asset_id": row[0],
                    "raw_bucket": row[1],
                    "raw_key": row[2],
                    "source_type": row[3],
                    "genai_engine": row[4],
                    "label_policy": row[5],
                }
                for row in rows
            ]

    def find_project_image_candidates(
        self, folder: str, require_ls_finalized: bool = False
    ) -> list[dict]:
        if require_ls_finalized:
            sql = """
                SELECT DISTINCT im.image_id, im.image_bucket, im.image_key, im.source_asset_id,
                                r.source_type, r.genai_engine, r.label_policy
                FROM image_metadata im
                JOIN raw_files r ON r.asset_id = im.source_asset_id
                JOIN image_labels il ON il.image_id = im.image_id
                WHERE r.source_unit_name = %s
                  AND im.image_key IS NOT NULL
                  AND im.image_bucket IS NOT NULL
                  AND il.review_status = 'finalized'
                  AND (r.source_type IS NULL OR r.source_type IN ('camera','nas_upload'))
            """
        else:
            sql = """
                SELECT im.image_id, im.image_bucket, im.image_key, im.source_asset_id,
                       r.source_type, r.genai_engine, r.label_policy
                FROM image_metadata im
                JOIN raw_files r ON r.asset_id = im.source_asset_id
                WHERE r.source_unit_name = %s
                  AND im.image_key IS NOT NULL
                  AND im.image_bucket IS NOT NULL
                  AND (r.source_type IS NULL OR r.source_type IN ('camera','nas_upload'))
            """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (folder,))
                rows = cur.fetchall()
            return [
                {
                    "image_id": row[0],
                    "image_bucket": row[1],
                    "image_key": row[2],
                    "source_asset_id": row[3],
                    "source_type": row[4],
                    "genai_engine": row[5],
                    "label_policy": row[6],
                }
                for row in rows
            ]

    def find_project_clip_candidates(self, folder: str) -> list[dict]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT pc.clip_id, pc.processed_bucket, pc.clip_key,
                           pc.source_asset_id, pc.source_label_id,
                           pc.event_index, pc.clip_start_sec, pc.clip_end_sec,
                           r.raw_key AS source_raw_key, r.raw_bucket AS source_raw_bucket
                    FROM processed_clips pc
                    JOIN raw_files r ON r.asset_id = pc.source_asset_id
                    WHERE r.source_unit_name = %s
                      AND pc.process_status = 'completed'
                      AND pc.clip_key IS NOT NULL
                    """,
                    (folder,),
                )
                rows = cur.fetchall()
            columns = [
                "clip_id", "processed_bucket", "clip_key",
                "source_asset_id", "source_label_id",
                "event_index", "clip_start_sec", "clip_end_sec",
                "source_raw_key", "source_raw_bucket",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def find_project_genai_pairs(self, folder: str) -> list[dict]:
        # GenAI label-free pair candidates: (input image, output video|image) per job.
        # Build asset uses this for source_unit_name='genai_<batch_id>' folders.
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT j.job_id, j.batch_id, j.seq_in_batch,
                           j.provider_job_id, j.cost_units,
                           b.engine, b.prompt, b.output_media,
                           src.asset_id   AS input_asset_id,
                           src.raw_bucket AS input_raw_bucket,
                           src.raw_key    AS input_raw_key,
                           src.media_type AS input_media_type,
                           out.asset_id   AS output_asset_id,
                           out.raw_bucket AS output_raw_bucket,
                           out.raw_key    AS output_raw_key,
                           out.media_type AS output_media_type
                      FROM genai_jobs j
                      JOIN genai_batches b ON b.batch_id = j.batch_id
                      JOIN raw_files src ON src.asset_id = j.input_asset_id
                      JOIN raw_files out ON out.asset_id = j.output_asset_id
                     WHERE src.source_unit_name = %s
                       AND j.status = 'done'
                       AND src.ingest_status = 'completed'
                       AND out.ingest_status = 'completed'
                       AND src.raw_key IS NOT NULL
                       AND out.raw_key IS NOT NULL
                     ORDER BY j.batch_id, j.seq_in_batch
                    """,
                    (folder,),
                )
                rows = cur.fetchall()
            columns = [
                "job_id", "batch_id", "seq_in_batch",
                "provider_job_id", "cost_units",
                "engine", "prompt", "output_media",
                "input_asset_id", "input_raw_bucket",
                "input_raw_key", "input_media_type",
                "output_asset_id", "output_raw_bucket",
                "output_raw_key", "output_media_type",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def insert_dataset(self, dataset: dict) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO datasets (
                        dataset_id, name, version, config, split_ratio,
                        dataset_bucket, dataset_prefix, build_status, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        dataset.get("dataset_id") or str(uuid4()),
                        dataset.get("name"),
                        dataset.get("version"),
                        dataset.get("config"),
                        dataset.get("split_ratio"),
                        dataset.get("dataset_bucket", "vlm-dataset"),
                        dataset.get("dataset_prefix"),
                        dataset.get("build_status", "pending"),
                        dataset.get("created_at", datetime.now()),
                    ),
                )

    def update_dataset_status(self, dataset_id: str, status: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE datasets SET build_status = %s WHERE dataset_id = %s",
                    (status, dataset_id),
                )

    def find_projects_ready_to_build(self) -> list[str]:
        """LS 검수 finalized 라벨이 있고 아직 완료된 dataset이 없는 folder."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT r.source_unit_name AS folder
                    FROM raw_files r
                    WHERE r.source_unit_name IS NOT NULL
                      AND r.source_unit_name <> ''
                      AND (
                        EXISTS (
                            SELECT 1 FROM labels l
                            WHERE l.asset_id = r.asset_id
                              AND l.review_status = 'finalized'
                        )
                        OR EXISTS (
                            SELECT 1 FROM image_labels il
                            JOIN image_metadata im ON im.image_id = il.image_id
                            WHERE im.source_asset_id = r.asset_id
                              AND il.review_status = 'finalized'
                        )
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM datasets d
                        WHERE d.name = r.source_unit_name
                          AND d.build_status = 'completed'
                      )
                    ORDER BY folder
                    """
                )
                rows = cur.fetchall()
            return [row[0] for row in rows]

    # ------------------------------------------------------------------
    # Classification build 쿼리
    # ------------------------------------------------------------------

    def find_projects_for_classification_build(self) -> list[dict]:
        """dispatch_requests.labeling_method 에 classification_* 가 포함된 프로젝트.

        DuckDB ``ANY_VALUE(col)`` → PG 15 호환 ``(array_agg(col))[1]``.
        ``LATEST one row per group`` 의미는 그대로 유지 (group 의 첫 번째 row 값을 가져옴).
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT folder_name,
                           (array_agg(labeling_method))[1] AS labeling_method,
                           (array_agg(categories))[1]      AS categories,
                           (array_agg(classes))[1]         AS classes,
                           MAX(created_at)                 AS latest_at
                    FROM dispatch_requests
                    WHERE labeling_method LIKE '%%classification_%%'
                      AND (status IS NULL OR status NOT IN ('failed', 'canceled'))
                      AND folder_name IS NOT NULL
                    GROUP BY folder_name
                    ORDER BY folder_name
                    """
                )
                rows = cur.fetchall()
            columns = ["folder_name", "labeling_method", "categories", "classes", "latest_at"]
            return [dict(zip(columns, row)) for row in rows]

    def find_project_classification_videos(
        self, folder: str, require_ls_finalized: bool = False
    ) -> list[dict]:
        """video 후보 + labels_key 리스트.

        DuckDB ``LIST(DISTINCT col)`` → PG ``array_agg(DISTINCT col)``.
        psycopg2 가 PG ARRAY 를 Python list 로 자동 변환하므로 호출자 코드는 변경 없음.
        """
        finalized_filter = (
            "AND l.review_status = 'finalized'" if require_ls_finalized else ""
        )
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT r.asset_id, r.raw_bucket, r.raw_key,
                           array_agg(DISTINCT l.labels_key) AS labels_key_list
                    FROM raw_files r
                    JOIN labels l ON l.asset_id = r.asset_id
                    WHERE r.source_unit_name = %s
                      AND r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND r.raw_key IS NOT NULL
                      AND l.label_format = 'gemini_event_json'
                      AND l.labels_key IS NOT NULL
                      {finalized_filter}
                    GROUP BY r.asset_id, r.raw_bucket, r.raw_key
                    """,
                    (folder,),
                )
                rows = cur.fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "labels_key_list"]
            return [dict(zip(columns, row)) for row in rows]

    def find_project_classification_images(
        self, folder: str, require_ls_finalized: bool = False
    ) -> list[dict]:
        finalized_filter = (
            "AND il.review_status = 'finalized'" if require_ls_finalized else ""
        )
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT im.image_id, im.image_bucket, im.image_key, im.source_asset_id,
                           array_agg(DISTINCT il.labels_key) AS labels_key_list
                    FROM image_metadata im
                    JOIN raw_files r ON r.asset_id = im.source_asset_id
                    JOIN image_labels il ON il.image_id = im.image_id
                    WHERE r.source_unit_name = %s
                      AND im.image_key IS NOT NULL
                      AND im.image_bucket IS NOT NULL
                      AND il.label_tool = 'sam3'
                      AND il.labels_key IS NOT NULL
                      {finalized_filter}
                    GROUP BY im.image_id, im.image_bucket, im.image_key, im.source_asset_id
                    """,
                    (folder,),
                )
                rows = cur.fetchall()
            columns = [
                "image_id", "image_bucket", "image_key",
                "source_asset_id", "labels_key_list",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def find_latest_dispatch_for_folder(self, folder: str) -> dict | None:
        """최신 dispatch_requests row (labeling_method, categories, classes)."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT labeling_method, categories, classes
                    FROM dispatch_requests
                    WHERE folder_name = %s
                      AND (status IS NULL OR status NOT IN ('failed', 'canceled'))
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (folder,),
                )
                row = cur.fetchone()
            if not row:
                return None
            return {"labeling_method": row[0], "categories": row[1], "classes": row[2]}

    def find_project_video_candidates_missing_images(self, folder: str) -> list[dict]:
        """image_metadata row 가 전혀 없는 video (classification_image fallback 대상)."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT r.asset_id, r.raw_bucket, r.raw_key,
                           vm.duration_sec, vm.fps, vm.frame_count
                    FROM raw_files r
                    LEFT JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.source_unit_name = %s
                      AND r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND r.raw_key IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM image_metadata im
                          WHERE im.source_asset_id = r.asset_id
                      )
                    """,
                    (folder,),
                )
                rows = cur.fetchall()
            columns = [
                "asset_id", "raw_bucket", "raw_key",
                "duration_sec", "fps", "frame_count",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def insert_classification_dataset(self, row: dict) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO classification_datasets (
                        dataset_id, name, folder_prefix, config,
                        classification_bucket, video_count, image_count, category_count,
                        build_status, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row.get("dataset_id") or str(uuid4()),
                        row.get("name"),
                        row.get("folder_prefix"),
                        row.get("config"),
                        row.get("classification_bucket", "vlm-classification"),
                        int(row.get("video_count") or 0),
                        int(row.get("image_count") or 0),
                        int(row.get("category_count") or 0),
                        row.get("build_status", "pending"),
                        row.get("created_at", datetime.now()),
                    ),
                )

    def update_classification_dataset_status(
        self,
        dataset_id: str,
        status: str,
        video_count: int | None = None,
        image_count: int | None = None,
        category_count: int | None = None,
    ) -> None:
        sets = ["build_status = %s"]
        params: list[Any] = [status]
        if video_count is not None:
            sets.append("video_count = %s")
            params.append(int(video_count))
        if image_count is not None:
            sets.append("image_count = %s")
            params.append(int(image_count))
        if category_count is not None:
            sets.append("category_count = %s")
            params.append(int(category_count))
        params.append(dataset_id)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE classification_datasets SET {', '.join(sets)} WHERE dataset_id = %s",
                    params,
                )
