"""DuckDB LABELING 도메인 — auto-label, clip image extract, image_labels CRUD."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4


class DuckDBLabelingMixin:
    """Gemini auto-labeling / clip image extraction / image_labels 관련 메서드."""

    # ── auto-label (video_metadata) ──

    def find_auto_label_pending_videos(self, limit: int = 50, folder_name: str | None = None) -> list[dict]:
        with self.connect() as conn:
            query_cond = "AND r.raw_key LIKE ?" if folder_name else ""
            params = [max(1, int(limit))]
            if folder_name:
                params.insert(0, f"{folder_name}/%")
            rows = conn.execute(
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
                LIMIT ?
                """,
                params,
            ).fetchall()
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
            conn.execute(
                """
                UPDATE video_metadata
                SET auto_label_status = ?,
                    auto_label_error = ?,
                    auto_label_key = COALESCE(?, auto_label_key),
                    auto_labeled_at = COALESCE(?, auto_labeled_at)
                WHERE asset_id = ?
                """,
                [status, error, label_key, labeled_at, asset_id],
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
        """video_metadata stage column (staging spec flow). 컬럼 없으면 no-op."""
        with self.connect() as conn:
            if "timestamp_status" not in self._table_columns(conn, "video_metadata"):
                return
            conn.execute(
                """
                UPDATE video_metadata
                SET timestamp_status = ?,
                    timestamp_error = ?,
                    timestamp_label_key = COALESCE(?, timestamp_label_key),
                    timestamp_completed_at = COALESCE(?, timestamp_completed_at)
                WHERE asset_id = ?
                """,
                [status, error, label_key, completed_at, asset_id],
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
            conn.execute(
                """
                UPDATE video_metadata
                SET caption_status = ?, caption_error = ?, caption_completed_at = COALESCE(?, caption_completed_at)
                WHERE asset_id = ?
                """,
                [status, error, completed_at, asset_id],
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
            conn.execute(
                """
                UPDATE video_metadata
                SET frame_status = ?, frame_error = ?, frame_completed_at = COALESCE(?, frame_completed_at)
                WHERE asset_id = ?
                """,
                [status, error, completed_at, asset_id],
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
            conn.execute(
                """
                UPDATE video_metadata
                SET bbox_status = ?, bbox_error = ?, bbox_completed_at = COALESCE(?, bbox_completed_at)
                WHERE asset_id = ?
                """,
                [status, error, completed_at, asset_id],
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
            rows = conn.execute(
                """
                SELECT
                    r.asset_id, r.raw_bucket, r.raw_key, r.archive_path, r.source_path,
                    vm.duration_sec, vm.fps, vm.frame_count
                FROM raw_files r
                JOIN video_metadata vm ON vm.asset_id = r.asset_id
                WHERE r.media_type = 'video'
                  AND r.ingest_status = 'ready_for_labeling'
                  AND r.spec_id = ?
                  AND COALESCE(vm.timestamp_status, 'pending') = 'pending'
                ORDER BY r.created_at
                LIMIT ?
                """,
                [spec_id, max(1, int(limit))],
            ).fetchall()
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
            rows = conn.execute(
                f"""
                SELECT
                    r.asset_id, r.raw_bucket, r.raw_key, r.archive_path, r.source_path,
                    vm.duration_sec, vm.fps, vm.frame_count
                FROM raw_files r
                JOIN video_metadata vm ON vm.asset_id = r.asset_id
                WHERE r.media_type = 'video'
                  AND r.ingest_status = 'completed'
                  AND r.source_unit_name = ?
                  {ts_filter}
                ORDER BY r.created_at
                LIMIT ?
                """,
                [folder_name, max(1, int(limit))],
            ).fetchall()
            columns = [
                "asset_id", "raw_bucket", "raw_key", "archive_path", "source_path",
                "duration_sec", "fps", "frame_count",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def find_ready_for_labeling_caption_backlog(
        self, spec_id: str, limit: int = 100
    ) -> list[dict]:
        """Staging spec flow: timestamp 완료 후 caption 정규화가 남은 비디오."""
        with self.connect() as conn:
            cols = self._table_columns(conn, "raw_files")
            if "spec_id" not in cols:
                return []
            vm_cols = self._table_columns(conn, "video_metadata")
            required_vm_cols = {"timestamp_status", "timestamp_label_key", "caption_status"}
            if not required_vm_cols.issubset(vm_cols):
                return []
            rows = conn.execute(
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
                  AND r.spec_id = ?
                  AND COALESCE(vm.timestamp_status, 'pending') = 'completed'
                  AND COALESCE(vm.caption_status, 'pending') = 'pending'
                  AND COALESCE(vm.timestamp_label_key, '') <> ''
                ORDER BY COALESCE(vm.timestamp_completed_at, r.created_at), r.asset_id
                LIMIT ?
                """,
                [spec_id, max(1, int(limit))],
            ).fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "timestamp_label_key", "duration_sec"]
            return [dict(zip(columns, row)) for row in rows]

    def find_caption_pending_by_folder(
        self, folder_name: str, limit: int = 100
    ) -> list[dict]:
        """Dispatch flow: folder 기준 timestamp 완료 후 caption 정규화가 남은 비디오."""
        with self.connect() as conn:
            vm_cols = self._table_columns(conn, "video_metadata")
            required_vm_cols = {"timestamp_status", "timestamp_label_key", "caption_status"}
            if not required_vm_cols.issubset(vm_cols):
                return []
            rows = conn.execute(
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
                  AND r.source_unit_name = ?
                  AND COALESCE(vm.timestamp_status, 'pending') = 'completed'
                  AND COALESCE(vm.caption_status, 'pending') = 'pending'
                  AND COALESCE(vm.timestamp_label_key, '') <> ''
                ORDER BY COALESCE(vm.timestamp_completed_at, r.created_at), r.asset_id
                LIMIT ?
                """,
                [folder_name, max(1, int(limit))],
            ).fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "timestamp_label_key", "duration_sec"]
            return [dict(zip(columns, row)) for row in rows]

    def find_captioning_pending_videos(self, limit: int = 100, folder_name: str | None = None) -> list[dict]:
        """Gemini JSON 생성 완료(generated) 후 아직 DB 정규화가 안 된 video."""
        with self.connect() as conn:
            query_cond = "AND r.raw_key LIKE ?" if folder_name else ""
            params = [max(1, int(limit))]
            if folder_name:
                params.insert(0, f"{folder_name}/%")

            rows = conn.execute(
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
                LIMIT ?
                """,
                params,
            ).fetchall()
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

        payload_rows = []
        for row in rows:
            payload_rows.append(
                [
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
                ]
            )

        with self.connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    """
                    DELETE FROM labels
                    WHERE asset_id = ?
                      AND label_tool = 'gemini'
                      AND label_source = 'auto'
                      AND labels_key = ?
                    """,
                    [normalized_asset_id, normalized_labels_key],
                )
                if payload_rows:
                    conn.executemany(
                        """
                        INSERT INTO labels (
                            label_id, asset_id, labels_bucket, labels_key,
                            label_format, label_tool, label_source, review_status,
                            event_index, event_count, timestamp_start_sec, timestamp_end_sec,
                            caption_text, object_count, label_status, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        payload_rows,
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
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
            conn.execute(
                """
                UPDATE processed_clips
                SET duration_sec = ?,
                    fps = ?,
                    frame_count = ?
                WHERE clip_id = ?
                """,
                [duration_sec, fps, frame_count, clip_id],
            )

    def update_processed_clip_status(self, clip_id: str, status: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE processed_clips
                SET process_status = ?
                WHERE clip_id = ?
                """,
                [status, clip_id],
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
            conn.execute(
                """
                UPDATE processed_clips
                SET image_extract_status = ?,
                    image_extract_count = ?,
                    image_extract_error = ?,
                    image_extracted_at = ?
                WHERE clip_id = ?
                """,
                [status, count, error, extracted_at, clip_id],
            )

    def find_clip_image_extract_pending(self, limit: int = 200, folder_name: str | None = None) -> list[dict[str, Any]]:
        """processed_clips에서 image_extract_status = 'pending'인 video clip 조회."""
        with self.connect() as conn:
            query_cond = "AND r.raw_key LIKE ?" if folder_name else ""
            params = [max(1, int(limit))]
            if folder_name:
                params.insert(0, f"{folder_name}/%")

            rows = conn.execute(
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
                LIMIT ?
                """,
                params,
            ).fetchall()
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
        """image_labels에 아직 *label_tool* detection 결과가 없는 processed_clip_frame/raw_video_frame 조회.

        ``include_classification_tool`` 을 지정하면 해당 tool의 결과도 누락된 이미지를 포함한다.
        """
        with self.connect() as conn:
            raw_file_columns = self._table_columns(conn, "raw_files")
            query_filters: list[str] = []
            params: list[Any] = []

            if spec_id:
                if "spec_id" not in raw_file_columns:
                    return []
                query_filters.append("AND r.spec_id = ?")
                params.append(spec_id)
            elif folder_name:
                query_filters.append("AND r.raw_key LIKE ?")
                params.append(f"{folder_name}/%")
            query_cond = "\n".join(query_filters)
            params.append(max(1, int(limit)))

            missing_detection = (
                f"NOT EXISTS ("
                f"SELECT 1 FROM image_labels il "
                f"WHERE il.image_id = im.image_id AND il.label_tool = '{label_tool}')"
            )
            if include_classification_tool:
                missing_cls = (
                    f"NOT EXISTS ("
                    f"SELECT 1 FROM image_labels il "
                    f"WHERE il.image_id = im.image_id AND il.label_tool = '{include_classification_tool}')"
                )
                pending_clause = f"({missing_detection} OR {missing_cls})"
            else:
                pending_clause = missing_detection

            rows = conn.execute(
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
                LIMIT ?
                """,
                params,
            ).fetchall()
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
        """image_labels에 아직 YOLO detection 결과가 없는 processed_clip_frame/raw_video_frame 조회."""
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
        """image_labels에 아직 SAM3 detection 결과가 없는 processed_clip_frame/raw_video_frame 조회."""
        return self.find_pending_images(
            label_tool="sam3",
            limit=limit,
            folder_name=folder_name,
            spec_id=spec_id,
        )

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
            rows = conn.execute(
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
                    WHERE im.image_role = ?
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
                WHERE source_rank <= ?
                ORDER BY source_unit_name, COALESCE(extracted_at, CURRENT_TIMESTAMP), image_id
                LIMIT ?
                """,
                [
                    normalized_role,
                    max(1, int(max_per_source_unit)),
                    max(1, int(limit)),
                ],
            ).fetchall()
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
        """image_labels 배치 INSERT."""
        if not labels:
            return 0
        payload_rows = []
        for label in labels:
            payload_rows.append([
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
            ])
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO image_labels (
                    image_label_id, image_id, source_clip_id,
                    labels_bucket, labels_key, label_format,
                    label_tool, label_source, review_status,
                    label_status, object_count, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload_rows,
            )
        return len(payload_rows)

    # ── image_labels (YOLO scaffold) ──

    def insert_image_label(self, label: dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO image_labels (
                    image_label_id, image_id, source_clip_id,
                    labels_bucket, labels_key, label_format,
                    label_tool, label_source, review_status,
                    label_status, object_count, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
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
                ],
            )
