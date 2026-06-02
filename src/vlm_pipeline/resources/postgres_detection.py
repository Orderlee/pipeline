"""PG DETECTION 도메인 — image_labels CRUD + detection 대상 이미지 조회.

분리 출처: postgres_labeling.py L514-818 (byte-identical 이동).
PostgresLabelingMixin 이 이 Mixin 을 상속하므로 기존 caller 변경 없음.
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


class PostgresDetectionMixin:
    """Detection 대상 이미지 조회 + image_labels CRUD 메서드."""

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
            query_filters: list[str] = []
            params: list[Any] = []

            if spec_id:
                query_filters.append("AND r.spec_id = %s")
                params.append(spec_id)
            elif folder_name:
                query_filters.append("AND r.raw_key LIKE %s")
                params.append(f"{folder_name}/%")
            query_cond = "\n".join(query_filters)
            params.append(max(1, int(limit)))

            # f-string 으로 라벨 도구 이름을 그대로 SQL 에 박지 말고 placeholder 로 — 보안 + 인덱스 활용.
            missing_detection = (
                "NOT EXISTS (SELECT 1 FROM image_labels il WHERE il.image_id = im.image_id AND il.label_tool = %s)"
            )
            label_tool_params = [label_tool]
            if include_classification_tool:
                missing_cls = (
                    "NOT EXISTS (SELECT 1 FROM image_labels il WHERE il.image_id = im.image_id AND il.label_tool = %s)"
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
                "image_id",
                "source_asset_id",
                "source_clip_id",
                "image_bucket",
                "image_key",
                "width",
                "height",
                "frame_index",
                "frame_sec",
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

    def find_dispatch_video_classification_candidates(self, *, folder_name: str, limit: int) -> list[dict[str, Any]]:
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
            "asset_id",
            "raw_bucket",
            "raw_key",
            "archive_path",
            "source_path",
            "duration_sec",
            "fps",
            "frame_count",
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
            payload_rows.append(
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
                )
            )
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
