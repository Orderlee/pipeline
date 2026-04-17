"""DuckDB INGEST 도메인 — raw_files CRUD 메서드."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4


class DuckDBIngestRawMixin:
    """raw_files 테이블 CRUD mixin."""

    def insert_raw_files_batch(self, records: list[dict]) -> int:
        if not records:
            return 0
        with self.connect() as conn:
            columns = self._table_columns(conn, "raw_files")
            has_source_unit = "source_unit_name" in columns
            has_spec_id = "spec_id" in columns

            base_cols = [
                "asset_id", "source_path", "original_name", "media_type",
                "file_size", "checksum", "archive_path", "raw_bucket", "raw_key",
                "ingest_batch_id", "transfer_tool", "ingest_status", "error_message",
                "created_at", "updated_at",
            ]
            if has_source_unit:
                base_cols.append("source_unit_name")
            if has_spec_id:
                base_cols.append("spec_id")

            placeholders = ", ".join("?" * len(base_cols))
            insert_cols = ", ".join(base_cols)
            sql = f"INSERT INTO raw_files ({insert_cols}) VALUES ({placeholders})"

            rows = []
            for rec in records:
                now = datetime.now()
                row = [
                    rec.get("asset_id") or str(uuid4()),
                    rec.get("source_path"),
                    rec.get("original_name"),
                    rec.get("media_type", "image"),
                    rec.get("file_size"),
                    rec.get("checksum"),
                    rec.get("archive_path"),
                    rec.get("raw_bucket", "vlm-raw"),
                    rec.get("raw_key"),
                    rec.get("ingest_batch_id"),
                    rec.get("transfer_tool", "manual"),
                    rec.get("ingest_status", "pending"),
                    rec.get("error_message"),
                    rec.get("created_at", now),
                    rec.get("updated_at", now),
                ]
                if has_source_unit:
                    row.append(rec.get("source_unit_name"))
                if has_spec_id:
                    row.append(rec.get("spec_id"))
                rows.append(row)

            conn.executemany(sql, rows)
            return len(rows)

    def find_existing_raw_keys(self, raw_keys: list[str]) -> set[str]:
        """주어진 raw_key 목록 중 DB에 이미 존재하는 것들을 반환."""
        if not raw_keys:
            return set()
        with self.connect() as conn:
            placeholders = ", ".join("?" for _ in raw_keys)
            rows = conn.execute(
                f"SELECT DISTINCT raw_key FROM raw_files WHERE raw_key IN ({placeholders})",
                raw_keys,
            ).fetchall()
            return {r[0] for r in rows}

    def find_completed_source_paths(self, source_paths: list[str]) -> set[str]:
        """이미 completed + archive_path 보유한 source_path만 반환 (batch)."""
        cleaned = [str(p) for p in source_paths if str(p or "").strip()]
        if not cleaned:
            return set()
        with self.connect() as conn:
            placeholders = ", ".join("?" for _ in cleaned)
            rows = conn.execute(
                f"""
                SELECT DISTINCT source_path
                FROM raw_files
                WHERE source_path IN ({placeholders})
                  AND ingest_status = 'completed'
                  AND archive_path IS NOT NULL
                """,
                cleaned,
            ).fetchall()
            return {r[0] for r in rows}

    def find_by_checksum(self, checksum: str, completed_only: bool = True) -> dict[str, Any] | None:
        query = "SELECT * FROM raw_files WHERE checksum = ?"
        params: list[Any] = [checksum]
        if completed_only:
            query += " AND ingest_status = 'completed'"

        with self.connect() as conn:
            result = conn.execute(query, params).fetchone()
            if result is None:
                return None
            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, result))

    def find_any_by_checksum(self, checksum: str) -> dict[str, Any] | None:
        return self.find_by_checksum(checksum, completed_only=False)

    def has_raw_file(self, asset_id: str) -> bool:
        normalized_id = str(asset_id or "").strip()
        if not normalized_id:
            return False
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM raw_files WHERE asset_id = ? LIMIT 1",
                [normalized_id],
            ).fetchone()
        return row is not None

    def has_failed_duplicate_for_source(self, source_path: str, duplicate_asset_id: str) -> bool:
        normalized_source = str(source_path or "").strip()
        normalized_target = str(duplicate_asset_id or "").strip()
        if not normalized_source or not normalized_target:
            return False
        duplicate_marker = f"duplicate_of:{normalized_target}"
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM raw_files
                WHERE source_path = ?
                  AND ingest_status = 'failed'
                  AND error_message = ?
                LIMIT 1
                """,
                [normalized_source, duplicate_marker],
            ).fetchone()
        return row is not None

    def update_raw_file_status(
        self,
        asset_id: str,
        status: str,
        error_message: str | None = None,
        archive_path: str | None = None,
        raw_bucket: str | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE raw_files
                SET ingest_status = ?,
                    error_message = ?,
                    archive_path = COALESCE(?, archive_path),
                    raw_bucket = COALESCE(?, raw_bucket),
                    updated_at = ?
                WHERE asset_id = ?
                """,
                [status, error_message, archive_path, raw_bucket, datetime.now(), asset_id],
            )

    def batch_update_spec_and_status(
        self, updates: list[dict]
    ) -> int:
        """raw_files.ingest_status( 및 spec_id) 배치 업데이트. updates: [{"asset_id", "ingest_status", "spec_id"?}]. spec_id 컬럼 있을 때만 spec_id 반영."""
        if not updates:
            return 0
        now = datetime.now()
        with self.connect() as conn:
            columns = self._table_columns(conn, "raw_files")
            has_spec_id = "spec_id" in columns
            for u in updates:
                aid = u.get("asset_id")
                if not aid:
                    continue
                status = u.get("ingest_status", "pending")
                if has_spec_id and "spec_id" in u:
                    conn.execute(
                        "UPDATE raw_files SET ingest_status = ?, spec_id = ?, updated_at = ? WHERE asset_id = ?",
                        [status, u.get("spec_id"), now, aid],
                    )
                else:
                    conn.execute(
                        "UPDATE raw_files SET ingest_status = ?, updated_at = ? WHERE asset_id = ?",
                        [status, now, aid],
                    )
            return len([x for x in updates if x.get("asset_id")])

    def batch_update_status(self, updates: list[dict]) -> int:
        """배치 상태 업데이트. updates: [{"asset_id", "status", "archive_path"?, "error_message"?}]"""
        if not updates:
            return 0
        now = datetime.now()
        rows = []
        for u in updates:
            rows.append((
                u["status"],
                u.get("error_message"),
                u.get("archive_path"),
                u.get("raw_bucket"),
                now,
                u["asset_id"],
            ))
        with self.connect() as conn:
            conn.executemany(
                """
                UPDATE raw_files
                SET ingest_status = ?,
                    error_message = ?,
                    archive_path = COALESCE(?, archive_path),
                    raw_bucket = COALESCE(?, raw_bucket),
                    updated_at = ?
                WHERE asset_id = ?
                """,
                rows,
            )
        return len(rows)

    def abort_in_progress_raw_files_for_dispatch(
        self,
        request_id: str,
        *,
        error_message: str,
    ) -> int:
        """Cancel/Failure 시 호출. 해당 dispatch 의 raw_files 중 'pending'/'uploading'
        상태인 row 만 'failed' 로 마감. 'completed'/'skipped'/'failed' 는 그대로 둔다."""
        normalized_request_id = str(request_id or "").strip()
        if not normalized_request_id:
            return 0
        like_pattern = f"dispatch_req_{normalized_request_id}_%"
        now = datetime.now()
        with self.connect() as conn:
            rows = conn.execute(
                """
                UPDATE raw_files
                SET ingest_status = 'failed',
                    error_message = ?,
                    updated_at = ?
                WHERE ingest_batch_id LIKE ?
                  AND ingest_status IN ('pending', 'uploading')
                RETURNING asset_id
                """,
                [error_message, now, like_pattern],
            ).fetchall()
            return len(rows)

    def count_unresolved_rows_for_source_unit(self, source_unit_path: str) -> int:
        normalized_path = str(source_unit_path or "").strip()
        if not normalized_path:
            return 0

        like_pattern = f"{normalized_path.rstrip('/')}/%"
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM raw_files
                WHERE (source_path = ? OR source_path LIKE ?)
                  AND NOT (
                      ingest_status IN ('completed', 'skipped')
                      OR (
                          ingest_status = 'failed'
                          AND error_message LIKE 'duplicate_of:%'
                      )
                  )
                """,
                [normalized_path, like_pattern],
            ).fetchone()
        return int(row[0]) if row else 0

    def count_raw_files_for_source_unit_name(self, source_unit_name: str) -> int:
        normalized_name = str(source_unit_name or "").strip()
        if not normalized_name:
            return 0
        with self.connect() as conn:
            columns = self._table_columns(conn, "raw_files")
            if "source_unit_name" not in columns:
                return 0
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM raw_files
                WHERE source_unit_name = ?
                """,
                [normalized_name],
            ).fetchone()
        return int(row[0]) if row else 0

    def mark_duplicate_skipped_assets(self, duplicate_asset_files: dict[str, list[str]]) -> int:
        normalized_items: dict[str, list[str]] = {}

        def _parse_file_names(payload: str) -> list[str]:
            names = [name.strip() for name in str(payload or "").split(",") if name.strip()]
            return names or ["unknown_file"]

        for asset_id, file_names in (duplicate_asset_files or {}).items():
            normalized_id = str(asset_id or "").strip()
            if not normalized_id:
                continue
            merged = normalized_items.setdefault(normalized_id, [])
            if isinstance(file_names, list):
                merged.extend(
                    str(file_name).strip() or "unknown_file"
                    for file_name in file_names
                )
            else:
                merged.extend(_parse_file_names(str(file_names)))

        if not normalized_items:
            return 0

        now = datetime.now()
        with self.connect() as conn:
            placeholders = ", ".join("?" * len(normalized_items))
            rows = conn.execute(
                f"SELECT asset_id, error_message FROM raw_files WHERE asset_id IN ({placeholders})",
                list(normalized_items.keys()),
            ).fetchall()

            updates: list[tuple] = []
            for asset_id, current_error_raw in rows:
                current_error = str(current_error_raw or "").strip()
                file_names = [
                    file_name
                    for file_name in normalized_items[asset_id]
                    if str(file_name).strip()
                ] or ["unknown_file"]
                if not current_error:
                    merged_files = list(file_names)
                elif current_error.startswith("duplicate_skipped_in_manifest:"):
                    existing_payload = current_error.replace("duplicate_skipped_in_manifest:", "", 1)
                    merged_files = _parse_file_names(existing_payload) + list(file_names)
                else:
                    continue

                marker = f"duplicate_skipped_in_manifest:{','.join(sorted(merged_files))}"
                if marker != current_error:
                    updates.append((marker, now, asset_id))

            if updates:
                conn.executemany(
                    "UPDATE raw_files SET error_message = ?, updated_at = ? WHERE asset_id = ?",
                    updates,
                )

        return len(updates)

    def delete_asset_for_reingest(self, asset_id: str) -> None:
        """실패/중단된 ingest 자산을 재처리 가능 상태로 정리 (트랜잭션 래핑)."""
        with self.connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    """
                    DELETE FROM dataset_clips
                    WHERE clip_id IN (
                        SELECT clip_id FROM processed_clips WHERE source_asset_id = ?
                    )
                    """,
                    [asset_id],
                )
                conn.execute("DELETE FROM image_metadata WHERE source_asset_id = ?", [asset_id])
                conn.execute("DELETE FROM processed_clips WHERE source_asset_id = ?", [asset_id])
                conn.execute("DELETE FROM labels WHERE asset_id = ?", [asset_id])
                conn.execute("DELETE FROM video_metadata WHERE asset_id = ?", [asset_id])
                conn.execute("DELETE FROM raw_files WHERE asset_id = ?", [asset_id])
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def list_raw_files_by_source_unit_name(self, source_unit_name: str) -> list[dict[str, Any]]:
        normalized_name = str(source_unit_name or "").strip()
        if not normalized_name:
            return []
        with self.connect() as conn:
            columns = self._table_columns(conn, "raw_files")
            if "source_unit_name" not in columns:
                return []
            rows = conn.execute(
                """
                SELECT *
                FROM raw_files
                WHERE source_unit_name = ?
                ORDER BY created_at, raw_key, asset_id
                """,
                [normalized_name],
            ).fetchall()
            result_columns = [desc[0] for desc in conn.description]
            return [dict(zip(result_columns, row)) for row in rows]

    def delete_failed_rows_by_error_filters(
        self,
        *,
        exact_errors: list[str] | None = None,
        like_patterns: list[str] | None = None,
    ) -> int:
        exact_values = [str(v).strip() for v in (exact_errors or []) if str(v).strip()]
        like_values = [str(v).strip() for v in (like_patterns or []) if str(v).strip()]
        if not exact_values and not like_values:
            return 0

        clauses: list[str] = []
        params: list[Any] = []
        if exact_values:
            placeholders = ", ".join(["?"] * len(exact_values))
            clauses.append(f"error_message IN ({placeholders})")
            params.extend(exact_values)
        for pattern in like_values:
            clauses.append("error_message LIKE ?")
            params.append(pattern)

        where_filter = " OR ".join(clauses)
        with self.connect() as conn:
            target_count = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM raw_files
                WHERE ingest_status = 'failed'
                  AND ({where_filter})
                """,
                params,
            ).fetchone()[0]
            if int(target_count) <= 0:
                return 0
            conn.execute(
                f"""
                DELETE FROM raw_files
                WHERE ingest_status = 'failed'
                  AND ({where_filter})
                """,
                params,
            )
            return int(target_count)

    def recover_archive_move_failed_asset(self, asset_id: str, archive_path: str) -> bool:
        normalized_asset_id = str(asset_id or "").strip()
        normalized_archive_path = str(archive_path or "").strip()
        if not normalized_asset_id or not normalized_archive_path:
            return False
        with self.connect() as conn:
            row = conn.execute(
                """
                UPDATE raw_files
                SET ingest_status = 'completed',
                    error_message = NULL,
                    archive_path = ?,
                    raw_bucket = COALESCE(raw_bucket, 'vlm-raw'),
                    updated_at = ?
                WHERE asset_id = ?
                  AND ingest_status = 'failed'
                RETURNING asset_id
                """,
                [normalized_archive_path, datetime.now(), normalized_asset_id],
            ).fetchone()
        return row is not None

    def list_completed_videos_for_spec_router(
        self, limit: int = 500
    ) -> list[dict[str, Any]]:
        """ingest_router용: ingest_status=completed, spec_id 미설정 비디오. source_unit_name 컬럼 있을 때만."""
        with self.connect() as conn:
            columns = self._table_columns(conn, "raw_files")
            if "source_unit_name" not in columns or "spec_id" not in columns:
                return []
            rows = conn.execute(
                """
                SELECT asset_id, COALESCE(source_unit_name, '') AS source_unit_name, raw_key
                FROM raw_files
                WHERE media_type = 'video'
                  AND ingest_status = 'completed'
                  AND (spec_id IS NULL OR spec_id = '')
                ORDER BY created_at
                LIMIT ?
                """,
                [max(1, int(limit))],
            ).fetchall()
            return [
                {"asset_id": r[0], "source_unit_name": r[1] or "", "raw_key": r[2]}
                for r in rows
            ]

    def find_by_raw_key_stem(self, stem: str, source_unit_name: str | None = None) -> dict[str, Any] | None:
        with self.connect() as conn:
            query = """
                SELECT * FROM raw_files
                WHERE regexp_extract(raw_key, '[^/]+$')
                      LIKE ? || '.%'
                  AND ingest_status = 'completed'
                """
            params: list[Any] = [stem]
            if source_unit_name:
                columns = self._table_columns(conn, "raw_files")
                if "source_unit_name" in columns:
                    query += " AND source_unit_name = ?"
                    params.append(str(source_unit_name))
            query += " LIMIT 1"
            result = conn.execute(query, params).fetchone()
            if result is None:
                return None
            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, result))
