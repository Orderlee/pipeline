"""PG TRAIN domain — frozen-trainset candidate queries + version writer.

Read path joins the human-confirmed bbox projection (image_label_annotations,
migration 011) through finalized image_labels. AL contribution is the intersection
of an AL-queue membership and existence in image_label_annotations; today that
table is empty so al_confirmed_count is honestly 0 (design §7.2). No model-derived
labels are ever selected (no auto_generated, no vlm-classification).
"""

from __future__ import annotations

import json
from datetime import datetime
from uuid import uuid4


class PostgresTrainMixin:
    """Train-dataset snapshot queries. Mixed into PostgresResource."""

    def find_sam3_finalized_bbox_candidates(self, folder_name: str | None = None) -> list[dict]:
        """One row per human-finalized bbox (image_label_annotations ∩ finalized image_labels)."""
        where_folder = ""
        params: list = []
        if folder_name:
            where_folder = "AND r.source_unit_name = %s"
            params.append(folder_name)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT im.image_id, im.image_bucket, im.image_key, im.source_asset_id,
                           r.source_unit_name, ila.category, ila.box_index,
                           ila.bbox_x, ila.bbox_y, ila.bbox_w, ila.bbox_h
                    FROM image_label_annotations ila
                    JOIN image_labels il ON il.image_label_id = ila.image_label_id
                    JOIN image_metadata im ON im.image_id = ila.image_id
                    JOIN raw_files r ON r.asset_id = im.source_asset_id
                    WHERE il.review_status = 'finalized'
                      AND im.image_key IS NOT NULL
                      AND im.image_bucket IS NOT NULL
                      {where_folder}
                    ORDER BY im.image_id, ila.box_index
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            columns = [
                "image_id",
                "image_bucket",
                "image_key",
                "source_asset_id",
                "source_unit_name",
                "category",
                "box_index",
                "bbox_x",
                "bbox_y",
                "bbox_w",
                "bbox_h",
            ]
            return self._rows_to_dicts(rows, columns)

    def find_al_confirmed_image_ids(self, image_ids: list[str]) -> set[str]:
        """Subset of image_ids that have ≥1 human-confirmed annotation box.

        AL-queue membership ∩ image_label_annotations. Honestly empty while the
        per-box projection table has 0 rows (design §7.2).
        """
        ids = [str(i) for i in (image_ids or []) if i]
        if not ids:
            return set()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ila.image_id
                    FROM image_label_annotations ila
                    WHERE ila.image_id = ANY(%s)
                    """,
                    (ids,),
                )
                rows = cur.fetchall()
            return {str(r[0]) for r in rows}

    def train_dataset_version_exists(self, task: str, content_checksum: str) -> bool:
        """True if a sealed version with this (task, content_checksum) already exists."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM train_dataset_versions WHERE task = %s AND content_checksum = %s LIMIT 1",
                    (task, content_checksum),
                )
                return cur.fetchone() is not None

    def insert_train_dataset_version(self, row: dict) -> None:
        """Insert a sealed train_dataset_versions row; idempotent on (task, content_checksum)."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO train_dataset_versions (
                        train_dataset_version_id, created_at, task, source_spec, class_map,
                        group_key_field, split_assignment_key, split_ratios, manifest_key,
                        content_checksum, ls_count, al_confirmed_count, per_class_counts,
                        total_count, seed, upstream_dataset_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (task, content_checksum) DO NOTHING
                    """,
                    (
                        row.get("train_dataset_version_id") or str(uuid4()),
                        row.get("created_at", datetime.utcnow()),
                        row.get("task"),
                        json.dumps(row.get("source_spec") or {}),
                        json.dumps(row.get("class_map") or {}),
                        row.get("group_key_field"),
                        row.get("split_assignment_key"),
                        json.dumps(row.get("split_ratios") or {}),
                        row.get("manifest_key"),
                        row.get("content_checksum"),
                        int(row.get("ls_count") or 0),
                        int(row.get("al_confirmed_count") or 0),
                        json.dumps(row.get("per_class_counts") or {}),
                        int(row.get("total_count") or 0),
                        int(row.get("seed") or 0),
                        row.get("upstream_dataset_id"),
                    ),
                )
