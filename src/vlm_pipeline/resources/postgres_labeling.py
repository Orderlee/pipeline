"""PG LABELING 도메인 — auto-label, clip image extract, image_labels CRUD.

DuckDBLabelingMixin 1:1 포팅. 핵심 변환:
  - 2× ``INSERT OR REPLACE INTO image_labels`` → ``ON CONFLICT (image_label_id) DO UPDATE``
  - 윈도우 함수 (``ROW_NUMBER() OVER`` 등) PG 호환
  - 트랜잭션 명시 호출 → ``connect()`` ctxmgr 자동 관리
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4


_IMAGE_LABELS_INSERT_SQL = """
INSERT INTO image_labels (
    image_label_id, image_id, source_clip_id,
    labels_bucket, labels_key, label_format,
    label_tool, label_source, review_status,
    label_status, object_count, created_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (image_label_id) DO UPDATE SET
    image_id = EXCLUDED.image_id,
    source_clip_id = EXCLUDED.source_clip_id,
    labels_bucket = EXCLUDED.labels_bucket,
    labels_key = EXCLUDED.labels_key,
    label_format = EXCLUDED.label_format,
    label_tool = EXCLUDED.label_tool,
    label_source = EXCLUDED.label_source,
    review_status = EXCLUDED.review_status,
    label_status = EXCLUDED.label_status,
    object_count = EXCLUDED.object_count,
    created_at = EXCLUDED.created_at
"""


class PostgresLabelingMixin:
    """Gemini auto-labeling / clip image extraction / image_labels 관련 메서드."""

    # ── auto-label (video_metadata) ──

    def find_auto_label_pending_videos(self, limit: int = 50, folder_name: str | None = None) -> list[dict]:
        with self.connect() as conn:
            query_cond = "AND r.raw_key LIKE %s" if folder_name else ""
            params: list[Any] = [max(1, int(limit))]
            if folder_name:
                params.insert(0, f"{folder_name}/%")
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        r.asset_id,
                        r.raw_bucket,
                        r.raw_key,
                        r.archive_path,
                        r.source_path,
                        vm.duration_sec,
                        vm.fps,
                        vm.frame_count
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND COALESCE(vm.auto_label_status, 'pending') = 'pending'
                      {query_cond}
                    ORDER BY r.created_at
                    LIMIT %s
                    """,
                    params,
                )
                rows = cur.fetchall()
            columns = [
                "asset_id", "raw_bucket", "raw_key", "archive_path",
                "source_path", "duration_sec", "fps", "frame_count",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def update_auto_label_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        label_key: str | None = None,
        labeled_at: datetime | None = None,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE video_metadata
                    SET auto_label_status = %s,
                        auto_label_error = %s,
                        auto_label_key = COALESCE(%s, auto_label_key),
                        auto_labeled_at = COALESCE(%s, auto_labeled_at)
                    WHERE asset_id = %s
                    """,
                    (status, error, label_key, labeled_at, asset_id),
                )

    def update_timestamp_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        label_key: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        """video_metadata stage column. 컬럼 없으면 no-op."""
        with self.connect() as conn:
            if "timestamp_status" not in self._table_columns(conn, "video_metadata"):
                return
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE video_metadata
                    SET timestamp_status = %s,
                        timestamp_error = %s,
                        timestamp_label_key = COALESCE(%s, timestamp_label_key),
                        timestamp_completed_at = COALESCE(%s, timestamp_completed_at)
                    WHERE asset_id = %s
                    """,
                    (status, error, label_key, completed_at, asset_id),
                )

    def update_caption_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        with self.connect() as conn:
            if "caption_status" not in self._table_columns(conn, "video_metadata"):
                return
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE video_metadata
                    SET caption_status = %s, caption_error = %s, caption_completed_at = COALESCE(%s, caption_completed_at)
                    WHERE asset_id = %s
                    """,
                    (status, error, completed_at, asset_id),
                )

    def update_frame_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        with self.connect() as conn:
            if "frame_status" not in self._table_columns(conn, "video_metadata"):
                return
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE video_metadata
                    SET frame_status = %s, frame_error = %s, frame_completed_at = COALESCE(%s, frame_completed_at)
                    WHERE asset_id = %s
                    """,
                    (status, error, completed_at, asset_id),
                )

    def update_bbox_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        with self.connect() as conn:
            if "bbox_status" not in self._table_columns(conn, "video_metadata"):
                return
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE video_metadata
                    SET bbox_status = %s, bbox_error = %s, bbox_completed_at = COALESCE(%s, bbox_completed_at)
                    WHERE asset_id = %s
                    """,
                    (status, error, completed_at, asset_id),
                )

    def find_ready_for_labeling_timestamp_backlog(
        self, spec_id: str, limit: int = 50
    ) -> list[dict]:
        """Staging spec flow: ready_for_labeling + spec_id, timestamp 미완료 비디오."""
        with self.connect() as conn:
            cols = self._table_columns(conn, "raw_files")
            if "spec_id" not in cols:
                return []
            vm_cols = self._table_columns(conn, "video_metadata")
            if "timestamp_status" not in vm_cols:
                return []
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.asset_id, r.raw_bucket, r.raw_key, r.archive_path, r.source_path,
                        vm.duration_sec, vm.fps, vm.frame_count
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'ready_for_labeling'
                      AND r.spec_id = %s
                      AND COALESCE(vm.timestamp_status, 'pending') = 'pending'
                    ORDER BY r.created_at
                    LIMIT %s
                    """,
                    (spec_id, max(1, int(limit))),
                )
                rows = cur.fetchall()
            columns = [
                "asset_id", "raw_bucket", "raw_key", "archive_path", "source_path",
                "duration_sec", "fps", "frame_count",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def find_timestamp_pending_by_folder(
        self, folder_name: str, limit: int = 50
    ) -> list[dict]:
        """Dispatch flow: folder_name(source_unit_name) 기준 timestamp 미완료 비디오."""
        with self.connect() as conn:
            vm_cols = self._table_columns(conn, "video_metadata")
            ts_filter = "AND COALESCE(vm.timestamp_status, 'pending') = 'pending'" if "timestamp_status" in vm_cols else ""
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        r.asset_id, r.raw_bucket, r.raw_key, r.archive_path, r.source_path,
                        vm.duration_sec, vm.fps, vm.frame_count
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND r.source_unit_name = %s
                      {ts_filter}
                    ORDER BY r.created_at
                    LIMIT %s
                    """,
                    (folder_name, max(1, int(limit))),
                )
                rows = cur.fetchall()
            columns = [
                "asset_id", "raw_bucket", "raw_key", "archive_path", "source_path",
                "duration_sec", "fps", "frame_count",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def find_ready_for_labeling_caption_backlog(
        self, spec_id: str, limit: int = 100
    ) -> list[dict]:
        with self.connect() as conn:
            cols = self._table_columns(conn, "raw_files")
            if "spec_id" not in cols:
                return []
            vm_cols = self._table_columns(conn, "video_metadata")
            required_vm_cols = {"timestamp_status", "timestamp_label_key", "caption_status"}
            if not required_vm_cols.issubset(vm_cols):
                return []
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.asset_id,
                        r.raw_bucket,
                        r.raw_key,
                        vm.timestamp_label_key,
                        vm.duration_sec
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'ready_for_labeling'
                      AND r.spec_id = %s
                      AND COALESCE(vm.timestamp_status, 'pending') = 'completed'
                      AND COALESCE(vm.caption_status, 'pending') = 'pending'
                      AND COALESCE(vm.timestamp_label_key, '') <> ''
                    ORDER BY COALESCE(vm.timestamp_completed_at, r.created_at), r.asset_id
                    LIMIT %s
                    """,
                    (spec_id, max(1, int(limit))),
                )
                rows = cur.fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "timestamp_label_key", "duration_sec"]
            return [dict(zip(columns, row)) for row in rows]

    def find_caption_pending_by_folder(
        self, folder_name: str, limit: int = 100
    ) -> list[dict]:
        with self.connect() as conn:
            vm_cols = self._table_columns(conn, "video_metadata")
            required_vm_cols = {"timestamp_status", "timestamp_label_key", "caption_status"}
            if not required_vm_cols.issubset(vm_cols):
                return []
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.asset_id,
                        r.raw_bucket,
                        r.raw_key,
                        vm.timestamp_label_key,
                        vm.duration_sec
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND r.source_unit_name = %s
                      AND COALESCE(vm.timestamp_status, 'pending') = 'completed'
                      AND COALESCE(vm.caption_status, 'pending') = 'pending'
                      AND COALESCE(vm.timestamp_label_key, '') <> ''
                    ORDER BY COALESCE(vm.timestamp_completed_at, r.created_at), r.asset_id
                    LIMIT %s
                    """,
                    (folder_name, max(1, int(limit))),
                )
                rows = cur.fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "timestamp_label_key", "duration_sec"]
            return [dict(zip(columns, row)) for row in rows]

    def find_captioning_pending_videos(self, limit: int = 100, folder_name: str | None = None) -> list[dict]:
        """Gemini JSON 생성 완료(generated) 후 아직 DB 정규화가 안 된 video."""
        with self.connect() as conn:
            query_cond = "AND r.raw_key LIKE %s" if folder_name else ""
            params: list[Any] = [max(1, int(limit))]
            if folder_name:
                params.insert(0, f"{folder_name}/%")

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        r.asset_id,
                        r.raw_bucket,
                        r.raw_key,
                        vm.auto_label_key,
                        vm.duration_sec
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND vm.auto_label_status = 'generated'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM labels l
                          WHERE l.asset_id = r.asset_id
                            AND l.label_tool = 'gemini'
                            AND l.label_source = 'auto'
                            AND COALESCE(l.labels_key, '') = COALESCE(vm.auto_label_key, '')
                      )
                      {query_cond}
                    ORDER BY vm.auto_labeled_at
                    LIMIT %s
                    """,
                    params,
                )
                rows = cur.fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "auto_label_key", "duration_sec"]
            return [dict(zip(columns, row)) for row in rows]

    def replace_gemini_labels(
        self,
        asset_id: str,
        labels_key: str,
        rows: list[dict[str, Any]],
    ) -> int:
        normalized_asset_id = str(asset_id or "").strip()
        normalized_labels_key = str(labels_key or "").strip()
        if not normalized_asset_id or not normalized_labels_key:
            return 0

        payload_rows: list[tuple] = []
        for row in rows:
            payload_rows.append(
                (
                    row.get("label_id") or str(uuid4()),
                    normalized_asset_id,
                    row.get("labels_bucket", "vlm-labels"),
                    normalized_labels_key,
                    row.get("label_format", "gemini_event_json"),
                    row.get("label_tool", "gemini"),
                    row.get("label_source", "auto"),
                    row.get("review_status", "auto_generated"),
                    row.get("event_index", 0),
                    row.get("event_count"),
                    row.get("timestamp_start_sec"),
                    row.get("timestamp_end_sec"),
                    row.get("caption_text"),
                    row.get("object_count", 0),
                    row.get("label_status", "completed"),
                    row.get("created_at", datetime.now()),
                )
            )

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM labels
                    WHERE asset_id = %s
                      AND label_tool = 'gemini'
                      AND label_source = 'auto'
                      AND labels_key = %s
                    """,
                    (normalized_asset_id, normalized_labels_key),
                )
                if payload_rows:
                    cur.executemany(
                        """
                        INSERT INTO labels (
                            label_id, asset_id, labels_bucket, labels_key,
                            label_format, label_tool, label_source, review_status,
                            event_index, event_count, timestamp_start_sec, timestamp_end_sec,
                            caption_text, object_count, label_status, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        payload_rows,
                    )
        return len(payload_rows)

    # ── clip media meta (processed_clips) ──

    def update_processed_clip_media_meta(
        self,
        clip_id: str,
        duration_sec: float | None,
        fps: float | None,
        frame_count: int | None,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE processed_clips
                    SET duration_sec = %s,
                        fps = %s,
                        frame_count = %s
                    WHERE clip_id = %s
                    """,
                    (duration_sec, fps, frame_count, clip_id),
                )

    def update_processed_clip_status(self, clip_id: str, status: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE processed_clips
                    SET process_status = %s
                    WHERE clip_id = %s
                    """,
                    (status, clip_id),
                )

    def update_clip_image_extract_status(
        self,
        clip_id: str,
        status: str,
        *,
        count: int = 0,
        error: str | None = None,
        extracted_at: datetime | None = None,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE processed_clips
                    SET image_extract_status = %s,
                        image_extract_count = %s,
                        image_extract_error = %s,
                        image_extracted_at = %s
                    WHERE clip_id = %s
                    """,
                    (status, count, error, extracted_at, clip_id),
                )

    def find_clip_image_extract_pending(self, limit: int = 200, folder_name: str | None = None) -> list[dict[str, Any]]:
        """processed_clips에서 image_extract_status = 'pending'인 video clip 조회."""
        with self.connect() as conn:
            query_cond = "AND r.raw_key LIKE %s" if folder_name else ""
            params: list[Any] = [max(1, int(limit))]
            if folder_name:
                params.insert(0, f"{folder_name}/%")

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        pc.clip_id,
                        pc.source_asset_id,
                        pc.processed_bucket,
                        pc.clip_key,
                        pc.duration_sec,
                        pc.fps,
                        pc.frame_count,
                        pc.clip_start_sec,
                        pc.clip_end_sec,
                        r.raw_key
                    FROM processed_clips pc
                    JOIN raw_files r ON r.asset_id = pc.source_asset_id
                    WHERE pc.process_status = 'completed'
                      AND r.media_type = 'video'
                      AND COALESCE(pc.image_extract_status, 'pending') = 'pending'
                      {query_cond}
                    ORDER BY pc.created_at
                    LIMIT %s
                    """,
                    params,
                )
                rows = cur.fetchall()
            columns = [
                "clip_id", "source_asset_id", "processed_bucket", "clip_key",
                "duration_sec", "fps", "frame_count",
                "clip_start_sec", "clip_end_sec", "raw_key",
            ]
            return [dict(zip(columns, row)) for row in rows]

    # ── detection 대상 이미지 조회 (공통) ──

    def find_pending_images(
        self,
        label_tool: str,
        limit: int = 500,
        folder_name: str | None = None,
        spec_id: str | None = None,
        include_classification_tool: str | None = None,
    ) -> list[dict[str, Any]]:
        """image_labels에 아직 *label_tool* detection 결과가 없는 processed_clip_frame/raw_video_frame 조회."""
        with self.connect() as conn:
            raw_file_columns = self._table_columns(conn, "raw_files")
            query_filters: list[str] = []
            params: list[Any] = []

            if spec_id:
                if "spec_id" not in raw_file_columns:
                    return []
                query_filters.append("AND r.spec_id = %s")
                params.append(spec_id)
            elif folder_name:
                query_filters.append("AND r.raw_key LIKE %s")
                params.append(f"{folder_name}/%")
            query_cond = "\n".join(query_filters)
            params.append(max(1, int(limit)))

            # f-string 으로 라벨 도구 이름을 그대로 SQL 에 박지 말고 placeholder 로 — 보안 + 인덱스 활용.
            missing_detection = (
                "NOT EXISTS ("
                "SELECT 1 FROM image_labels il "
                "WHERE il.image_id = im.image_id AND il.label_tool = %s)"
            )
            label_tool_params = [label_tool]
            if include_classification_tool:
                missing_cls = (
                    "NOT EXISTS ("
                    "SELECT 1 FROM image_labels il "
                    "WHERE il.image_id = im.image_id AND il.label_tool = %s)"
                )
                pending_clause = f"({missing_detection} OR {missing_cls})"
                label_tool_params.append(include_classification_tool)
            else:
                pending_clause = missing_detection

            # label_tool 파라미터를 query 앞부분(WHERE pending_clause) 위치에 배치.
            final_params = label_tool_params + params

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        im.image_id,
                        im.source_asset_id,
                        im.source_clip_id,
                        im.image_bucket,
                        im.image_key,
                        im.width,
                        im.height,
                        im.frame_index,
                        im.frame_sec
                    FROM image_metadata im
                    JOIN raw_files r ON r.asset_id = im.source_asset_id
                    WHERE im.image_role IN ('processed_clip_frame', 'raw_video_frame')
                      AND {pending_clause}
                      {query_cond}
                    ORDER BY im.extracted_at
                    LIMIT %s
                    """,
                    final_params,
                )
                rows = cur.fetchall()
            columns = [
                "image_id", "source_asset_id", "source_clip_id",
                "image_bucket", "image_key", "width", "height",
                "frame_index", "frame_sec",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def find_yolo_pending_images(
        self,
        limit: int = 500,
        folder_name: str | None = None,
        spec_id: str | None = None,
        include_image_classification: bool = False,
    ) -> list[dict[str, Any]]:
        return self.find_pending_images(
            label_tool="yolo-world",
            limit=limit,
            folder_name=folder_name,
            spec_id=spec_id,
            include_classification_tool="yolo-world-classification" if include_image_classification else None,
        )

    def find_sam3_pending_images(
        self,
        limit: int = 500,
        folder_name: str | None = None,
        spec_id: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.find_pending_images(
            label_tool="sam3",
            limit=limit,
            folder_name=folder_name,
            spec_id=spec_id,
        )

    def find_dispatch_video_classification_candidates(
        self, *, folder_name: str, limit: int
    ) -> list[dict[str, Any]]:
        """dispatch `classification_video` 대상 후보 조회 — source_unit_name 매칭.

        labels 테이블에 이미 `video_classification_json` 레이블이 있는 raw_files 는 제외.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.asset_id,
                        r.raw_bucket,
                        r.raw_key,
                        r.archive_path,
                        r.source_path,
                        vm.duration_sec,
                        vm.fps,
                        vm.frame_count
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND r.source_unit_name = %s
                      AND NOT EXISTS (
                          SELECT 1
                          FROM labels l
                          WHERE l.asset_id = r.asset_id
                            AND l.label_format = 'video_classification_json'
                      )
                    ORDER BY r.created_at
                    LIMIT %s
                    """,
                    [folder_name, max(1, int(limit))],
                )
                rows = cur.fetchall()
        columns = [
            "asset_id", "raw_bucket", "raw_key", "archive_path",
            "source_path", "duration_sec", "fps", "frame_count",
        ]
        return [dict(zip(columns, row)) for row in rows]

    def find_sam3_shadow_candidates(
        self,
        *,
        image_role: str,
        limit: int,
        max_per_source_unit: int,
    ) -> list[dict[str, Any]]:
        """YOLO bbox 결과가 있는 이미지 중 SAM3 shadow benchmark 대상 조회."""
        normalized_role = str(image_role or "").strip().lower()
        if normalized_role not in {"processed_clip_frame", "raw_video_frame"}:
            return []

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH latest_yolo_labels AS (
                        SELECT
                            il.image_id,
                            il.labels_bucket,
                            il.labels_key,
                            il.created_at,
                            ROW_NUMBER() OVER (
                                PARTITION BY il.image_id
                                ORDER BY il.created_at DESC, il.image_label_id DESC
                            ) AS rn
                        FROM image_labels il
                        WHERE il.label_tool = 'yolo-world'
                    ),
                    ranked_candidates AS (
                        SELECT
                            im.image_id,
                            im.source_asset_id,
                            im.source_clip_id,
                            im.image_bucket,
                            im.image_key,
                            im.image_role,
                            im.width,
                            im.height,
                            im.frame_index,
                            im.frame_sec,
                            im.extracted_at,
                            r.source_unit_name,
                            r.raw_key,
                            yl.labels_bucket AS yolo_labels_bucket,
                            yl.labels_key AS yolo_labels_key,
                            ROW_NUMBER() OVER (
                                PARTITION BY r.source_unit_name
                                ORDER BY COALESCE(im.extracted_at, CURRENT_TIMESTAMP), im.image_id
                            ) AS source_rank
                        FROM image_metadata im
                        JOIN raw_files r ON r.asset_id = im.source_asset_id
                        JOIN latest_yolo_labels yl
                          ON yl.image_id = im.image_id
                         AND yl.rn = 1
                        WHERE im.image_role = %s
                    )
                    SELECT
                        image_id,
                        source_asset_id,
                        source_clip_id,
                        image_bucket,
                        image_key,
                        image_role,
                        width,
                        height,
                        frame_index,
                        frame_sec,
                        extracted_at,
                        source_unit_name,
                        raw_key,
                        yolo_labels_bucket,
                        yolo_labels_key
                    FROM ranked_candidates
                    WHERE source_rank <= %s
                    ORDER BY source_unit_name, COALESCE(extracted_at, CURRENT_TIMESTAMP), image_id
                    LIMIT %s
                    """,
                    (
                        normalized_role,
                        max(1, int(max_per_source_unit)),
                        max(1, int(limit)),
                    ),
                )
                rows = cur.fetchall()
            columns = [
                "image_id",
                "source_asset_id",
                "source_clip_id",
                "image_bucket",
                "image_key",
                "image_role",
                "width",
                "height",
                "frame_index",
                "frame_sec",
                "extracted_at",
                "source_unit_name",
                "raw_key",
                "yolo_labels_bucket",
                "yolo_labels_key",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def batch_insert_image_labels(self, labels: list[dict]) -> int:
        """image_labels 배치 INSERT (PK 충돌 시 UPDATE)."""
        if not labels:
            return 0
        payload_rows: list[tuple] = []
        for label in labels:
            payload_rows.append((
                label.get("image_label_id") or str(uuid4()),
                label.get("image_id"),
                label.get("source_clip_id"),
                label.get("labels_bucket", "vlm-labels"),
                label.get("labels_key"),
                label.get("label_format"),
                label.get("label_tool"),
                label.get("label_source"),
                label.get("review_status", "pending"),
                label.get("label_status", "pending"),
                label.get("object_count", 0),
                label.get("created_at", datetime.now()),
            ))
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(_IMAGE_LABELS_INSERT_SQL, payload_rows)
        return len(payload_rows)

    # ── image_labels (YOLO scaffold) ──

    def insert_image_label(self, label: dict) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _IMAGE_LABELS_INSERT_SQL,
                    (
                        label.get("image_label_id") or str(uuid4()),
                        label.get("image_id"),
                        label.get("source_clip_id"),
                        label.get("labels_bucket", "vlm-labels"),
                        label.get("labels_key"),
                        label.get("label_format"),
                        label.get("label_tool"),
                        label.get("label_source"),
                        label.get("review_status", "pending"),
                        label.get("label_status", "pending"),
                        label.get("object_count", 0),
                        label.get("created_at", datetime.now()),
                    ),
                )
