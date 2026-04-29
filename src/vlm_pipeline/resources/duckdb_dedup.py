"""DuckDB DEDUP 도메인 — phash, dup_group, similar 검색 등."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4


class DuckDBDedupMixin:
    """DEDUP 관련 DuckDB 메서드 mixin."""

    def find_phash_null(self, limit: int = 100) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT asset_id, raw_bucket, raw_key, archive_path, source_path
                FROM raw_files
                WHERE phash IS NULL
                  AND media_type = 'image'
                  AND ingest_status = 'completed'
                ORDER BY created_at
                LIMIT ?
                """,
                [limit],
            ).fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "archive_path", "source_path"]
            return [dict(zip(columns, row)) for row in rows]

    def update_phash(self, asset_id: str, phash: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE raw_files SET phash = ?, updated_at = ? WHERE asset_id = ?",
                [phash, datetime.now(), asset_id],
            )

    def clear_error_message(self, asset_id: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE raw_files
                SET error_message = NULL, updated_at = ?
                WHERE asset_id = ?
                  AND error_message LIKE 'phash_%'
                """,
                [datetime.now(), asset_id],
            )

    def update_dup_group(self, asset_id: str, group_id: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE raw_files SET dup_group_id = ?, updated_at = ? WHERE asset_id = ?",
                [group_id, datetime.now(), asset_id],
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
                where_parts.append("asset_id != ?")
                params.append(exclude_asset_id)

            if candidates and len(candidates) < all_prefix_count:
                placeholders = ", ".join(["?"] * len(candidates))
                where_parts.append(
                    f"lower(substr(phash, 1, {normalized_prefix_len})) IN ({placeholders})"
                )
                params.extend(candidates)

            query = (
                "SELECT asset_id, phash FROM raw_files "
                f"WHERE {' AND '.join(where_parts)}"
            )
            rows = conn.execute(query, params).fetchall()

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
            conn.execute(
                """
                INSERT OR REPLACE INTO labels (
                    label_id, asset_id, labels_bucket, labels_key,
                    label_format, label_tool, label_source, review_status,
                    event_index, event_count, timestamp_start_sec, timestamp_end_sec,
                    caption_text, object_count, label_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
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
                ],
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
                where_clauses.append("r.raw_key LIKE ?")
                params.append(f"{folder_name}/%")
            if spec_id:
                columns = self._table_columns(conn, "raw_files")
                if "spec_id" not in columns:
                    return []
                where_clauses.append("r.spec_id = ?")
                params.append(str(spec_id))

            where_sql = "\n                  AND ".join(where_clauses)
            rows = conn.execute(
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
            ).fetchall()
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
            conn.execute(
                """
                INSERT INTO processed_clips (
                    clip_id, source_asset_id, source_label_id, event_index,
                    clip_start_sec, clip_end_sec, checksum, file_size,
                    processed_bucket, clip_key, label_key, data_source, caption_text,
                    width, height, codec, duration_sec, fps, frame_count,
                    image_extract_status, image_extract_count, image_extract_error, image_extracted_at,
                    process_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                [
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
                ],
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
            where_clauses = ["checksum = ?"]
            params: list[Any] = [normalized_checksum]
            if source_asset_id:
                where_clauses.append("source_asset_id = ?")
                params.append(str(source_asset_id))
            if exclude_clip_id:
                where_clauses.append("clip_id != ?")
                params.append(str(exclude_clip_id))

            row = conn.execute(
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
            ).fetchone()
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
        """source_unit_name 별 video/image 카운트. raw_files.source_unit_name 기준."""
        with self.connect() as conn:
            rows = conn.execute(
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
                HAVING video_count > 0 OR image_count > 0
                ORDER BY folder
                """
            ).fetchall()
            return [
                {"folder": row[0], "video_count": row[1], "image_count": row[2]}
                for row in rows
            ]

    def find_project_video_candidates(
        self, folder: str, require_ls_finalized: bool = False
    ) -> list[dict]:
        """해당 folder의 ingest 완료 video asset 목록 + raw_key.

        require_ls_finalized=True 면 labels.review_status='finalized' 표지가
        있는 dispatch_requests 와 join 하여 후보를 한정한다 (영상 단위 EXISTS
        가 아니라 folder 단위 검사).

        주의:
          이전엔 `EXISTS labels WHERE asset_id=r.asset_id AND review_status=
          'finalized'` 라는 영상 단위 필터를 썼다. 이 방식은 빈 검수 영상
          (사람이 fp_case 등에서 의도적으로 이벤트 없음으로 확정한 영상)을
          dataset 에서 제외시키는 부작용이 있었다 — ls_sync 의 SOT 정책상
          빈 검수도 MinIO 에 [] events JSON 으로 저장되지만 DB labels 에는
          row 가 없으므로 EXISTS 가 false 로 빠지기 때문.

          빈 검수도 정상 데이터(AI 팀 후처리에서 normal 처리)이므로 dataset
          에 포함되어야 한다. build_dataset 의 step 3 (MinIO label_keys 검사)
          가 events JSON 실존을 보장하므로, 영상 단위 EXISTS 필터는 제거하고
          require_ls_finalized=True 일 때 folder 단위로 "최소 한 영상이라도
          finalized 됐는가" 만 검사한다.
        """
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
            rows = conn.execute(
                f"""
                SELECT r.asset_id, r.raw_bucket, r.raw_key
                FROM raw_files r
                JOIN video_metadata vm ON vm.asset_id = r.asset_id
                WHERE r.media_type = 'video'
                  AND r.ingest_status = 'completed'
                  AND r.source_unit_name = ?
                  AND r.raw_key IS NOT NULL
                  {finalized_filter}
                """,
                [folder],
            ).fetchall()
            return [
                {"asset_id": row[0], "raw_bucket": row[1], "raw_key": row[2]}
                for row in rows
            ]

    def find_project_image_candidates(
        self, folder: str, require_ls_finalized: bool = False
    ) -> list[dict]:
        """해당 folder의 image_metadata 목록 + image_key.

        require_ls_finalized=True 면 image_labels.review_status='finalized' 인
        image만 반환 (DISTINCT image).
        """
        if require_ls_finalized:
            sql = """
                SELECT DISTINCT im.image_id, im.image_bucket, im.image_key, im.source_asset_id
                FROM image_metadata im
                JOIN raw_files r ON r.asset_id = im.source_asset_id
                JOIN image_labels il ON il.image_id = im.image_id
                WHERE r.source_unit_name = ?
                  AND im.image_key IS NOT NULL
                  AND im.image_bucket IS NOT NULL
                  AND il.review_status = 'finalized'
            """
        else:
            sql = """
                SELECT im.image_id, im.image_bucket, im.image_key, im.source_asset_id
                FROM image_metadata im
                JOIN raw_files r ON r.asset_id = im.source_asset_id
                WHERE r.source_unit_name = ?
                  AND im.image_key IS NOT NULL
                  AND im.image_bucket IS NOT NULL
            """
        with self.connect() as conn:
            rows = conn.execute(sql, [folder]).fetchall()
            return [
                {
                    "image_id": row[0],
                    "image_bucket": row[1],
                    "image_key": row[2],
                    "source_asset_id": row[3],
                }
                for row in rows
            ]

    def find_project_clip_candidates(self, folder: str) -> list[dict]:
        """해당 folder의 processed_clips 완료 row. LS 라벨링 완료된 event 단위 clip."""
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT pc.clip_id, pc.processed_bucket, pc.clip_key,
                       pc.source_asset_id, pc.source_label_id,
                       pc.event_index, pc.clip_start_sec, pc.clip_end_sec,
                       r.raw_key AS source_raw_key, r.raw_bucket AS source_raw_bucket
                FROM processed_clips pc
                JOIN raw_files r ON r.asset_id = pc.source_asset_id
                WHERE r.source_unit_name = ?
                  AND pc.process_status = 'completed'
                  AND pc.clip_key IS NOT NULL
                """,
                [folder],
            ).fetchall()
            columns = [
                "clip_id", "processed_bucket", "clip_key",
                "source_asset_id", "source_label_id",
                "event_index", "clip_start_sec", "clip_end_sec",
                "source_raw_key", "source_raw_bucket",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def insert_dataset(self, dataset: dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO datasets (
                    dataset_id, name, version, config, split_ratio,
                    dataset_bucket, dataset_prefix, build_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    dataset.get("dataset_id") or str(uuid4()),
                    dataset.get("name"),
                    dataset.get("version"),
                    dataset.get("config"),
                    dataset.get("split_ratio"),
                    dataset.get("dataset_bucket", "vlm-dataset"),
                    dataset.get("dataset_prefix"),
                    dataset.get("build_status", "pending"),
                    dataset.get("created_at", datetime.now()),
                ],
            )

    def update_dataset_status(self, dataset_id: str, status: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE datasets SET build_status = ? WHERE dataset_id = ?",
                [status, dataset_id],
            )

    def find_projects_ready_to_build(self) -> list[str]:
        """LS 검수 finalized 라벨이 있고 아직 완료된 dataset이 없는 folder.

        `build_dataset_on_finalize_sensor` 전용. `labels.review_status='finalized'`
        또는 `image_labels.review_status='finalized'` 가 하나라도 있는 프로젝트 중,
        `datasets.build_status='completed'` 로 마감된 기존 row가 없는 것만 반환.
        """
        with self.connect() as conn:
            rows = conn.execute(
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
            ).fetchall()
            return [row[0] for row in rows]

    # ------------------------------------------------------------------
    # Classification build 쿼리
    # ------------------------------------------------------------------

    def find_projects_for_classification_build(self) -> list[dict]:
        """dispatch_requests.labeling_method 에 classification_* 가 포함된 프로젝트.

        labeling_method 는 comma-separated 로 저장됨. 최근 dispatch 우선.
        """
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT folder_name,
                       ANY_VALUE(labeling_method) AS labeling_method,
                       ANY_VALUE(categories)      AS categories,
                       ANY_VALUE(classes)         AS classes,
                       MAX(created_at)            AS latest_at
                FROM dispatch_requests
                WHERE labeling_method LIKE '%classification_%'
                  AND (status IS NULL OR status NOT IN ('failed', 'canceled'))
                  AND folder_name IS NOT NULL
                GROUP BY folder_name
                ORDER BY folder_name
                """
            ).fetchall()
            columns = ["folder_name", "labeling_method", "categories", "classes", "latest_at"]
            return [dict(zip(columns, row)) for row in rows]

    def find_project_classification_videos(
        self, folder: str, require_ls_finalized: bool = False
    ) -> list[dict]:
        """video 후보 + 해당 video 의 gemini_event_json labels_key 리스트."""
        finalized_filter = (
            "AND l.review_status = 'finalized'" if require_ls_finalized else ""
        )
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT r.asset_id, r.raw_bucket, r.raw_key,
                       LIST(DISTINCT l.labels_key) AS labels_key_list
                FROM raw_files r
                JOIN labels l ON l.asset_id = r.asset_id
                WHERE r.source_unit_name = ?
                  AND r.media_type = 'video'
                  AND r.ingest_status = 'completed'
                  AND r.raw_key IS NOT NULL
                  AND l.label_format = 'gemini_event_json'
                  AND l.labels_key IS NOT NULL
                  {finalized_filter}
                GROUP BY r.asset_id, r.raw_bucket, r.raw_key
                """,
                [folder],
            ).fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "labels_key_list"]
            return [dict(zip(columns, row)) for row in rows]

    def find_project_classification_images(
        self, folder: str, require_ls_finalized: bool = False
    ) -> list[dict]:
        """image 후보 + 해당 image 의 SAM3 COCO labels_key 리스트."""
        finalized_filter = (
            "AND il.review_status = 'finalized'" if require_ls_finalized else ""
        )
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT im.image_id, im.image_bucket, im.image_key, im.source_asset_id,
                       LIST(DISTINCT il.labels_key) AS labels_key_list
                FROM image_metadata im
                JOIN raw_files r ON r.asset_id = im.source_asset_id
                JOIN image_labels il ON il.image_id = im.image_id
                WHERE r.source_unit_name = ?
                  AND im.image_key IS NOT NULL
                  AND im.image_bucket IS NOT NULL
                  AND il.label_tool = 'sam3'
                  AND il.labels_key IS NOT NULL
                  {finalized_filter}
                GROUP BY im.image_id, im.image_bucket, im.image_key, im.source_asset_id
                """,
                [folder],
            ).fetchall()
            columns = [
                "image_id", "image_bucket", "image_key",
                "source_asset_id", "labels_key_list",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def find_latest_dispatch_for_folder(self, folder: str) -> dict | None:
        """최신 dispatch_requests row (labeling_method, categories, classes)."""
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT labeling_method, categories, classes
                FROM dispatch_requests
                WHERE folder_name = ?
                  AND (status IS NULL OR status NOT IN ('failed', 'canceled'))
                ORDER BY created_at DESC
                LIMIT 1
                """,
                [folder],
            ).fetchone()
            if not row:
                return None
            return {"labeling_method": row[0], "categories": row[1], "classes": row[2]}

    def find_project_video_candidates_missing_images(self, folder: str) -> list[dict]:
        """image_metadata row 가 전혀 없는 video (classification_image fallback 대상)."""
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT r.asset_id, r.raw_bucket, r.raw_key,
                       vm.duration_sec, vm.fps, vm.frame_count
                FROM raw_files r
                LEFT JOIN video_metadata vm ON vm.asset_id = r.asset_id
                WHERE r.source_unit_name = ?
                  AND r.media_type = 'video'
                  AND r.ingest_status = 'completed'
                  AND r.raw_key IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM image_metadata im
                      WHERE im.source_asset_id = r.asset_id
                  )
                """,
                [folder],
            ).fetchall()
            columns = [
                "asset_id", "raw_bucket", "raw_key",
                "duration_sec", "fps", "frame_count",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def insert_classification_dataset(self, row: dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO classification_datasets (
                    dataset_id, name, folder_prefix, config,
                    classification_bucket, video_count, image_count, category_count,
                    build_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
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
                ],
            )

    def update_classification_dataset_status(
        self,
        dataset_id: str,
        status: str,
        video_count: int | None = None,
        image_count: int | None = None,
        category_count: int | None = None,
    ) -> None:
        sets = ["build_status = ?"]
        params: list[Any] = [status]
        if video_count is not None:
            sets.append("video_count = ?")
            params.append(int(video_count))
        if image_count is not None:
            sets.append("image_count = ?")
            params.append(int(image_count))
        if category_count is not None:
            sets.append("category_count = ?")
            params.append(int(category_count))
        params.append(dataset_id)
        with self.connect() as conn:
            conn.execute(
                f"UPDATE classification_datasets SET {', '.join(sets)} WHERE dataset_id = ?",
                params,
            )

