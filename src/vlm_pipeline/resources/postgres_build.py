"""PG BUILD 도메인 — dataset-build 및 classification-build 쿼리 모음.

defs/build/ 가 소비하는 쿼리만 포함. process/labels 와 dedup 과는 무관.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4


class PostgresBuildMixin:
    """DATASET / CLASSIFICATION BUILD 관련 PostgreSQL 메서드 mixin."""

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
            return [{"folder": row[0], "video_count": row[1], "image_count": row[2]} for row in rows]

    def find_project_video_candidates(self, folder: str, require_ls_finalized: bool = False) -> list[dict]:
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

    def find_project_image_candidates(self, folder: str, require_ls_finalized: bool = False) -> list[dict]:
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
                "clip_id",
                "processed_bucket",
                "clip_key",
                "source_asset_id",
                "source_label_id",
                "event_index",
                "clip_start_sec",
                "clip_end_sec",
                "source_raw_key",
                "source_raw_bucket",
            ]
            return self._rows_to_dicts(rows, columns)

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
                "job_id",
                "batch_id",
                "seq_in_batch",
                "provider_job_id",
                "cost_units",
                "engine",
                "prompt",
                "output_media",
                "input_asset_id",
                "input_raw_bucket",
                "input_raw_key",
                "input_media_type",
                "output_asset_id",
                "output_raw_bucket",
                "output_raw_key",
                "output_media_type",
            ]
            return self._rows_to_dicts(rows, columns)

    def insert_dataset(self, dataset: dict) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO datasets (
                        dataset_id, name, version, config, split_ratio,
                        dataset_bucket, dataset_prefix, build_status, created_at,
                        spec_hash, git_sha, build_started_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        dataset.get("spec_hash"),
                        dataset.get("git_sha"),
                        dataset.get("build_started_at"),
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
            return self._rows_to_dicts(rows, columns)

    def find_project_classification_videos(self, folder: str, require_ls_finalized: bool = False) -> list[dict]:
        """video 후보 + labels_key 리스트.

        DuckDB ``LIST(DISTINCT col)`` → PG ``array_agg(DISTINCT col)``.
        psycopg2 가 PG ARRAY 를 Python list 로 자동 변환하므로 호출자 코드는 변경 없음.
        """
        finalized_filter = "AND l.review_status = 'finalized'" if require_ls_finalized else ""
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
            return self._rows_to_dicts(rows, columns)

    def find_project_classification_images(self, folder: str, require_ls_finalized: bool = False) -> list[dict]:
        finalized_filter = "AND il.review_status = 'finalized'" if require_ls_finalized else ""
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
                "image_id",
                "image_bucket",
                "image_key",
                "source_asset_id",
                "labels_key_list",
            ]
            return self._rows_to_dicts(rows, columns)

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
                "asset_id",
                "raw_bucket",
                "raw_key",
                "duration_sec",
                "fps",
                "frame_count",
            ]
            return self._rows_to_dicts(rows, columns)

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
