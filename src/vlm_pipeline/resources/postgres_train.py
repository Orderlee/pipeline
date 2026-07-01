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

from vlm_pipeline.resources.postgres_base import PostgresBaseMixin


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

    def find_finalized_timestamp_events(self, folder_name: str | None = None) -> list[dict]:
        """One row per human-finalized timestamp event (labels, review_status='finalized').

        For pseudo-label QA: GT event intervals per video (labels_key). labels has no
        category column → QA is event-level (single 'event' class). raw_files join scopes
        to a folder. Only rows with both timestamps present.
        """
        where_folder = ""
        params: list = []
        if folder_name:
            where_folder = "AND r.source_unit_name = %s"
            params.append(folder_name)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT l.labels_key, l.asset_id,
                           l.timestamp_start_sec, l.timestamp_end_sec
                    FROM labels l
                    JOIN raw_files r ON r.asset_id = l.asset_id
                    WHERE l.review_status = 'finalized'
                      AND l.labels_key IS NOT NULL
                      AND l.timestamp_start_sec IS NOT NULL
                      AND l.timestamp_end_sec IS NOT NULL
                      {where_folder}
                    ORDER BY l.labels_key, l.timestamp_start_sec
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            columns = ["labels_key", "asset_id", "timestamp_start_sec", "timestamp_end_sec"]
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
        """Insert a sealed train_dataset_versions row; idempotent on (task, content_checksum).

        dataset_catalog_id (migration 016 FK) is nullable here by design: it links a frozen
        snapshot back to exactly ONE dataset_catalog row, so callers only set it for
        single-source DVC builds (unambiguous). Multi-source Tier-2 builds (see
        freeze_multi_project_trainset) leave it NULL and record per-source lineage in
        source_spec instead — a single FK column can't represent N sources.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO train_dataset_versions (
                        train_dataset_version_id, created_at, task, source_spec, class_map,
                        group_key_field, split_assignment_key, split_ratios, manifest_key,
                        content_checksum, ls_count, al_confirmed_count, per_class_counts,
                        total_count, seed, upstream_dataset_id, dataset_catalog_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        row.get("dataset_catalog_id"),
                    ),
                )

    def get_model_registry_row(self, model_version_id: str) -> dict:
        """Read one model_registry row (eval-gate input). JSONB cols come back as dicts."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT model_version_id, model, version, train_dataset_version_id,
                           train_method, eval_config, checkpoint_key, status
                    FROM model_registry WHERE model_version_id = %s
                    """,
                    (model_version_id,),
                )
                rows = cur.fetchall()
            columns = [
                "model_version_id", "model", "version", "train_dataset_version_id",
                "train_method", "eval_config", "checkpoint_key", "status",
            ]
            dicts = self._rows_to_dicts(rows, columns)
        return dicts[0] if dicts else {}

    def update_model_registry_eval(
        self,
        model_version_id: str,
        *,
        metrics: dict,
        incumbent_metrics: dict,
        incumbent_source: str,
        eval_config: dict,
        status: str,
    ) -> None:
        """Write the eval-gate outcome back to model_registry (Section D)."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE model_registry
                    SET metrics = %s, incumbent_metrics = %s, incumbent_source = %s,
                        eval_config = %s, status = %s
                    WHERE model_version_id = %s
                    """,
                    (
                        json.dumps(metrics or {}),
                        json.dumps(incumbent_metrics or {}),
                        incumbent_source,
                        json.dumps(eval_config or {}),
                        status,
                        model_version_id,
                    ),
                )

    # ── DVC dataset_catalog helpers (Section J — curation layer index) ──

    def insert_catalog_row(self, row: dict) -> str:
        """Insert a dataset_catalog row; idempotent on the DVC UNIQUE key.

        Returns the dataset_catalog_id of the inserted OR pre-existing row.
        status comes from row (caller sets 'available' / 'pending_missing_dvc_objects').

        On conflict, self-heals a 'pending_missing_dvc_objects' row to the freshly-observed
        status (e.g. once the MinIO objects show up) — mirrors scripts/dvc/ingest_to_catalog.py's
        DO UPDATE (Codex BUG2 there). 'available'/'pinned' rows are protected by the WHERE guard
        so a stale re-ingest can never demote them.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dataset_catalog (
                        dataset_catalog_id, task, dataset_name, status,
                        data_repo_id, data_repo_url, git_rev, git_short_rev, git_ref, git_tag,
                        commit_subject, commit_message, commit_author_name, commit_author_email,
                        committed_at,
                        dvc_file_path, dvc_out_path, dvc_md5, dvc_size_bytes, dvc_nfiles,
                        dvc_remote_name, dvc_remote_url,
                        train_dataset_version_id, content_checksum, mlflow_run_id
                    ) VALUES (
                        %(dataset_catalog_id)s, %(task)s, %(dataset_name)s, %(status)s,
                        %(data_repo_id)s, %(data_repo_url)s, %(git_rev)s, %(git_short_rev)s,
                        %(git_ref)s, %(git_tag)s,
                        %(commit_subject)s, %(commit_message)s, %(commit_author_name)s,
                        %(commit_author_email)s, %(committed_at)s,
                        %(dvc_file_path)s, %(dvc_out_path)s, %(dvc_md5)s, %(dvc_size_bytes)s,
                        %(dvc_nfiles)s, %(dvc_remote_name)s, %(dvc_remote_url)s,
                        %(train_dataset_version_id)s, %(content_checksum)s, %(mlflow_run_id)s
                    )
                    ON CONFLICT (data_repo_id, git_rev, dvc_file_path, dvc_out_path) DO UPDATE
                        SET status = EXCLUDED.status
                        WHERE dataset_catalog.status = 'pending_missing_dvc_objects'
                    RETURNING dataset_catalog_id
                    """,
                    {
                        "dataset_catalog_id": row["dataset_catalog_id"],
                        "task": row["task"],
                        "dataset_name": row["dataset_name"],
                        "status": row.get("status", "available"),
                        "data_repo_id": row["data_repo_id"],
                        "data_repo_url": row.get("data_repo_url"),
                        "git_rev": row["git_rev"],
                        "git_short_rev": row.get("git_short_rev"),
                        "git_ref": row.get("git_ref"),
                        "git_tag": row.get("git_tag"),
                        "commit_subject": row.get("commit_subject"),
                        "commit_message": row.get("commit_message"),
                        "commit_author_name": row.get("commit_author_name"),
                        "commit_author_email": row.get("commit_author_email"),
                        "committed_at": row.get("committed_at"),
                        "dvc_file_path": row["dvc_file_path"],
                        "dvc_out_path": row["dvc_out_path"],
                        "dvc_md5": row.get("dvc_md5"),
                        "dvc_size_bytes": row.get("dvc_size_bytes"),
                        "dvc_nfiles": row.get("dvc_nfiles"),
                        "dvc_remote_name": row.get("dvc_remote_name"),
                        "dvc_remote_url": row.get("dvc_remote_url"),
                        "train_dataset_version_id": row.get("train_dataset_version_id"),
                        "content_checksum": row.get("content_checksum"),
                        "mlflow_run_id": row.get("mlflow_run_id"),
                    },
                )
                inserted = cur.fetchone()
                if inserted is not None:
                    new_id = inserted[0]
                else:
                    # ON CONFLICT → no RETURNING row; fetch the pre-existing id by UNIQUE key.
                    cur.execute(
                        """
                        SELECT dataset_catalog_id FROM dataset_catalog
                        WHERE data_repo_id = %(data_repo_id)s AND git_rev = %(git_rev)s
                          AND dvc_file_path = %(dvc_file_path)s AND dvc_out_path = %(dvc_out_path)s
                        """,
                        {
                            "data_repo_id": row["data_repo_id"],
                            "git_rev": row["git_rev"],
                            "dvc_file_path": row["dvc_file_path"],
                            "dvc_out_path": row["dvc_out_path"],
                        },
                    )
                    new_id = cur.fetchone()[0]
            conn.commit()
        return str(new_id)

    def get_catalog_by_alias(self, task: str, alias: str = "current") -> dict | None:
        """Resolve a task+alias to its pinned dataset_catalog row (or None)."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.* FROM dataset_catalog_aliases a
                    JOIN dataset_catalog c ON c.dataset_catalog_id = a.dataset_catalog_id
                    WHERE a.task = %(task)s AND a.alias = %(alias)s
                    """,
                    {"task": task, "alias": alias},
                )
                rows = PostgresBaseMixin._cursor_to_dicts(cur)
        return rows[0] if rows else None

    def pin_alias(
        self,
        *,
        task: str,
        alias: str,
        dataset_catalog_id: str,
        pinned_by: str,
        pin_reason: str | None = None,
    ) -> dict:
        """Transactionally pin task+alias to a catalog id (NOT a raw UPDATE).

        One connection / one commit: read previous alias target → UPSERT the single
        (task, alias) alias row → append a pin_event with previous_dataset_catalog_id →
        flip the newly-pinned row to status='pinned'. Atomic so alias + audit never drift.
        """
        import uuid as _uuid

        with self.connect() as conn:
            with conn.cursor() as cur:
                # 가드(Codex BUG3): 카탈로그 행이 존재하고 같은 task 여야 함. aliases FK 는 존재만
                # 검사하고 task 일치는 강제하지 않으므로, 다른 task 의 catalog_id 로 pin 하면 빌더가
                # 엉뚱한 데이터셋을 학습할 수 있음. FOR UPDATE 로 status flip 대상 행도 잠근다.
                cur.execute(
                    "SELECT task, status FROM dataset_catalog WHERE dataset_catalog_id = %(dataset_catalog_id)s "
                    "FOR UPDATE",
                    {"dataset_catalog_id": dataset_catalog_id},
                )
                crow = cur.fetchone()
                if crow is None:
                    raise ValueError(f"dataset_catalog_id {dataset_catalog_id!r} not found")
                if crow[0] != task:
                    raise ValueError(
                        f"task mismatch: alias task={task!r} but catalog {dataset_catalog_id!r} is task={crow[0]!r}"
                    )
                # M-5: fast-fail instead of letting a pending/invalid row get pinned (fails later,
                # opaquely, at `dvc get` time). Only available/pinned rows have real bytes to pull.
                if crow[1] not in ("available", "pinned"):
                    raise ValueError(
                        f"dataset_catalog_id {dataset_catalog_id!r} has status={crow[1]!r}, "
                        "must be 'available' or 'pinned' to pin"
                    )
                # FOR UPDATE(Codex BUG2): 기존 alias 행을 잠가 동시 pin 을 직렬화 →
                # pin_events.previous_dataset_catalog_id 가 일관(두 pin 이 같은 선행자를 주장하지 않음).
                cur.execute(
                    "SELECT dataset_catalog_id FROM dataset_catalog_aliases "
                    "WHERE task = %(task)s AND alias = %(alias)s FOR UPDATE",
                    {"task": task, "alias": alias},
                )
                prev = cur.fetchone()
                previous_id = prev[0] if prev else None

                cur.execute(
                    """
                    INSERT INTO dataset_catalog_aliases
                        (task, alias, dataset_catalog_id, pinned_by, pin_reason, pinned_at)
                    VALUES (%(task)s, %(alias)s, %(dataset_catalog_id)s, %(pinned_by)s,
                            %(pin_reason)s, CURRENT_TIMESTAMP)
                    ON CONFLICT (task, alias) DO UPDATE SET
                        dataset_catalog_id = EXCLUDED.dataset_catalog_id,
                        pinned_by = EXCLUDED.pinned_by,
                        pin_reason = EXCLUDED.pin_reason,
                        pinned_at = EXCLUDED.pinned_at
                    """,
                    {
                        "task": task,
                        "alias": alias,
                        "dataset_catalog_id": dataset_catalog_id,
                        "pinned_by": pinned_by,
                        "pin_reason": pin_reason,
                    },
                )
                cur.execute(
                    """
                    INSERT INTO dataset_catalog_pin_events
                        (event_id, task, alias, dataset_catalog_id, previous_dataset_catalog_id,
                         pinned_by, pin_reason, pinned_at)
                    VALUES (%(event_id)s, %(task)s, %(alias)s, %(dataset_catalog_id)s,
                            %(previous_dataset_catalog_id)s, %(pinned_by)s, %(pin_reason)s,
                            CURRENT_TIMESTAMP)
                    """,
                    {
                        "event_id": str(_uuid.uuid4()),
                        "task": task,
                        "alias": alias,
                        "dataset_catalog_id": dataset_catalog_id,
                        "previous_dataset_catalog_id": previous_id,
                        "pinned_by": pinned_by,
                        "pin_reason": pin_reason,
                    },
                )
                cur.execute(
                    "UPDATE dataset_catalog SET status = 'pinned' "
                    "WHERE dataset_catalog_id = %(dataset_catalog_id)s",
                    {"dataset_catalog_id": dataset_catalog_id},
                )
            conn.commit()
        return {
            "task": task,
            "alias": alias,
            "dataset_catalog_id": dataset_catalog_id,
            "previous_dataset_catalog_id": str(previous_id) if previous_id else None,
        }


def insert_candidate_model_version(
    db,
    *,
    model: str,
    version: str,
    train_dataset_version_id: str,
    train_method: str,
    checkpoint_key: str,
    artifact_checksum: str,
    git_sha: str,
    training_image_digest: str,
    training_config: dict,
    env_lock_key: str,
    mlflow_run_id: str | None = None,
) -> str:
    """Insert a status='candidate' model_registry row; return its model_version_id.

    This is the row Section D's eval gate UPDATEs to 'promotable' and Section E's
    promotion SELECTs. incumbent_source stays NULL until the gate runs. Mirrors
    PostgresTrainMixin.insert_train_dataset_version's connect/cursor/positional-%s/
    json.dumps idiom. Module-level (takes the db resource) so the trainer-register op
    and tests import it directly.
    """
    model_version_id = f"mv-{uuid4().hex[:12]}"
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO model_registry (
                    model_version_id, model, version, train_dataset_version_id, train_method,
                    git_sha, training_image_digest, training_config, env_lock_key,
                    checkpoint_key, artifact_checksum, status, mlflow_run_id, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'candidate', %s, %s)
                RETURNING model_version_id
                """,
                (
                    model_version_id,
                    model,
                    version,
                    train_dataset_version_id,
                    train_method,
                    git_sha,
                    training_image_digest,
                    json.dumps(training_config or {}),
                    env_lock_key,
                    checkpoint_key,
                    artifact_checksum,
                    mlflow_run_id,
                    datetime.utcnow(),
                ),
            )
            row = cur.fetchone()
    return row[0] if row else model_version_id
