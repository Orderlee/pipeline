"""PG LABELING 도메인 — auto-label, clip image extract.

DuckDBLabelingMixin 1:1 포팅. 핵심 변환:
  - 윈도우 함수 (``ROW_NUMBER() OVER`` 등) PG 호환
  - 트랜잭션 명시 호출 → ``connect()`` ctxmgr 자동 관리

Detection 도메인 (image_labels CRUD + detection 대상 조회) 은
``postgres_detection.py`` 의 ``PostgresDetectionMixin`` 으로 분리.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from .postgres_detection import PostgresDetectionMixin


class PostgresLabelingMixin(PostgresDetectionMixin):
    """Gemini auto-labeling / clip image extraction / image_labels 관련 메서드."""

    # ── auto-label (video_metadata) ──

    def find_auto_label_pending_videos(self, limit: int = 50, folder_name: str | None = None) -> list[dict]:
        with self.connect() as conn:
            query_cond, folder_params = self._folder_filter(folder_name)
            params: list[Any] = folder_params + [max(1, int(limit))]
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
                "asset_id",
                "raw_bucket",
                "raw_key",
                "archive_path",
                "source_path",
                "duration_sec",
                "fps",
                "frame_count",
            ]
            return self._rows_to_dicts(rows, columns)

    _VALID_STAGES = frozenset({"timestamp", "caption", "frame", "bbox", "auto_label"})

    def _update_video_metadata_stage_status(
        self,
        asset_id: str,
        *,
        stage: str,
        status: str,
        error: str | None = None,
        label_key: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        if stage not in self._VALID_STAGES:
            raise ValueError(f"Invalid stage {stage!r}; must be one of {sorted(self._VALID_STAGES)}")
        # auto_label 은 legacy 컬럼명 ({stage}_label_key/{stage}_completed_at 패턴 미준수) 사용.
        if stage == "auto_label":
            label_key_col, completed_at_col = "auto_label_key", "auto_labeled_at"
        else:
            label_key_col, completed_at_col = f"{stage}_label_key", f"{stage}_completed_at"
        label_key_clause = f", {label_key_col} = COALESCE(%s, {label_key_col})" if label_key is not None else ""
        sql = (
            f"UPDATE video_metadata SET {stage}_status = %s, {stage}_error = %s"
            f"{label_key_clause}"
            f", {completed_at_col} = COALESCE(%s, {completed_at_col}) WHERE asset_id = %s"
        )
        params = [status, error]
        if label_key is not None:
            params.append(label_key)
        params.extend([completed_at, asset_id])
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

    def update_auto_label_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        label_key: str | None = None,
        labeled_at: datetime | None = None,
    ) -> None:
        self._update_video_metadata_stage_status(
            asset_id,
            stage="auto_label",
            status=status,
            error=error,
            label_key=label_key,
            completed_at=labeled_at,
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
        self._update_video_metadata_stage_status(
            asset_id, stage="timestamp", status=status, error=error, label_key=label_key, completed_at=completed_at
        )

    def update_caption_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        self._update_video_metadata_stage_status(
            asset_id, stage="caption", status=status, error=error, completed_at=completed_at
        )

    def update_frame_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        self._update_video_metadata_stage_status(
            asset_id, stage="frame", status=status, error=error, completed_at=completed_at
        )

    def update_bbox_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        self._update_video_metadata_stage_status(
            asset_id, stage="bbox", status=status, error=error, completed_at=completed_at
        )

    def find_ready_for_labeling_timestamp_backlog(self, spec_id: str, limit: int = 50) -> list[dict]:
        """Staging spec flow: ready_for_labeling + spec_id, timestamp 미완료 비디오."""
        with self.connect() as conn:
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
                "asset_id",
                "raw_bucket",
                "raw_key",
                "archive_path",
                "source_path",
                "duration_sec",
                "fps",
                "frame_count",
            ]
            return self._rows_to_dicts(rows, columns)

    def find_timestamp_pending_by_folder(self, folder_name: str, limit: int = 50) -> list[dict]:
        """Dispatch flow: folder_name(source_unit_name) 기준 timestamp 미완료 비디오."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.asset_id, r.raw_bucket, r.raw_key, r.archive_path, r.source_path,
                        vm.duration_sec, vm.fps, vm.frame_count
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND r.source_unit_name = %s
                      AND COALESCE(vm.timestamp_status, 'pending') = 'pending'
                    ORDER BY r.created_at
                    LIMIT %s
                    """,
                    (folder_name, max(1, int(limit))),
                )
                rows = cur.fetchall()
            columns = [
                "asset_id",
                "raw_bucket",
                "raw_key",
                "archive_path",
                "source_path",
                "duration_sec",
                "fps",
                "frame_count",
            ]
            return self._rows_to_dicts(rows, columns)

    def find_ready_for_labeling_caption_backlog(self, spec_id: str, limit: int = 100) -> list[dict]:
        with self.connect() as conn:
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
            return self._rows_to_dicts(rows, columns)

    def find_caption_pending_by_folder(self, folder_name: str, limit: int = 100) -> list[dict]:
        with self.connect() as conn:
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
            return self._rows_to_dicts(rows, columns)

    def find_captioning_pending_videos(self, limit: int = 100, folder_name: str | None = None) -> list[dict]:
        """Gemini JSON 생성 완료(generated) 후 아직 DB 정규화가 안 된 video."""
        with self.connect() as conn:
            query_cond, folder_params = self._folder_filter(folder_name)
            params: list[Any] = folder_params + [max(1, int(limit))]

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
            return self._rows_to_dicts(rows, columns)

    def replace_gemini_labels(
        self,
        asset_id: str,
        labels_key: str,
        rows: list[dict[str, Any]],
    ) -> int:
        normalized_asset_id = self._norm_str(asset_id)
        normalized_labels_key = self._norm_str(labels_key)
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
            query_cond, folder_params = self._folder_filter(folder_name)
            params: list[Any] = folder_params + [max(1, int(limit))]

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
                "clip_id",
                "source_asset_id",
                "processed_bucket",
                "clip_key",
                "duration_sec",
                "fps",
                "frame_count",
                "clip_start_sec",
                "clip_end_sec",
                "raw_key",
            ]
            return self._rows_to_dicts(rows, columns)
