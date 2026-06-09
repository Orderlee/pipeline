"""PG INGEST 도메인 — raw_files CRUD 메서드 (DuckDBIngestRawMixin 1:1 포팅).

변환 규칙:
  - placeholder ``?`` → ``%s``
  - cursor 패턴 사용
  - DuckDB ``regexp_extract(col, pattern)`` → PG ``substring(col FROM pattern)``
  - DuckDB BEGIN/COMMIT/ROLLBACK 명시 호출 → ``connect()`` ctxmgr 의 자동 commit/rollback 활용
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from .postgres_ingest_audit import PostgresIngestAuditMixin


class PostgresIngestRawMixin(PostgresIngestAuditMixin):
    """raw_files 테이블 CRUD mixin."""

    def insert_raw_files_batch(self, records: list[dict]) -> int:
        if not records:
            return 0
        with self.connect() as conn:
            sql = """
                INSERT INTO raw_files (
                    asset_id, source_path, original_name, media_type,
                    file_size, checksum, archive_path, raw_bucket, raw_key,
                    ingest_batch_id, transfer_tool, ingest_status, error_message,
                    created_at, updated_at,
                    source_unit_name, spec_id, source_type, genai_engine, label_policy
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            rows = []
            for rec in records:
                now = datetime.now()
                rows.append(
                    (
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
                        rec.get("source_unit_name"),
                        rec.get("spec_id"),
                        rec.get("source_type", "camera"),
                        rec.get("genai_engine"),
                        rec.get("label_policy", "required"),
                    )
                )

            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            return len(rows)

    def find_existing_raw_keys(self, raw_keys: list[str]) -> set[str]:
        """주어진 raw_key 목록 중 DB에 이미 존재하는 것들을 반환."""
        if not raw_keys:
            return set()
        with self.connect() as conn:
            placeholders = ", ".join(["%s"] * len(raw_keys))
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT DISTINCT raw_key FROM raw_files WHERE raw_key IN ({placeholders})",
                    raw_keys,
                )
                rows = cur.fetchall()
            return {r[0] for r in rows}

    def find_completed_source_paths(self, source_paths: list[str]) -> set[str]:
        """이미 completed + archive_path 보유한 source_path만 반환 (batch)."""
        cleaned = [str(p) for p in source_paths if str(p or "").strip()]
        if not cleaned:
            return set()
        with self.connect() as conn:
            placeholders = ", ".join(["%s"] * len(cleaned))
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT DISTINCT source_path
                    FROM raw_files
                    WHERE source_path IN ({placeholders})
                      AND ingest_status = 'completed'
                      AND archive_path IS NOT NULL
                    """,
                    cleaned,
                )
                rows = cur.fetchall()
            return {r[0] for r in rows}

    def find_by_checksum(self, checksum: str, completed_only: bool = True) -> dict[str, Any] | None:
        query = "SELECT * FROM raw_files WHERE checksum = %s"
        params: list[Any] = [checksum]
        if completed_only:
            query += " AND ingest_status = 'completed'"

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                if result is None:
                    return None
                columns = [desc[0] for desc in cur.description]
            return self._row_to_dict(result, columns)

    def find_any_by_checksum(self, checksum: str) -> dict[str, Any] | None:
        return self.find_by_checksum(checksum, completed_only=False)

    def has_raw_file(self, asset_id: str) -> bool:
        normalized_id = self._norm_str(asset_id)
        if not normalized_id:
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM raw_files WHERE asset_id = %s LIMIT 1",
                    (normalized_id,),
                )
                row = cur.fetchone()
        return row is not None

    def has_failed_duplicate_for_source(self, source_path: str, duplicate_asset_id: str) -> bool:
        normalized_source = self._norm_str(source_path)
        normalized_target = self._norm_str(duplicate_asset_id)
        if not normalized_source or not normalized_target:
            return False
        duplicate_marker = f"duplicate_of:{normalized_target}"
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM raw_files
                    WHERE source_path = %s
                      AND ingest_status = 'failed'
                      AND error_message = %s
                    LIMIT 1
                    """,
                    (normalized_source, duplicate_marker),
                )
                row = cur.fetchone()
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
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE raw_files
                    SET ingest_status = %s,
                        error_message = %s,
                        archive_path = COALESCE(%s, archive_path),
                        raw_bucket = COALESCE(%s, raw_bucket),
                        updated_at = %s
                    WHERE asset_id = %s
                    """,
                    (status, error_message, archive_path, raw_bucket, datetime.now(), asset_id),
                )

    def batch_update_spec_and_status(self, updates: list[dict]) -> int:
        """raw_files.ingest_status( 및 spec_id) 배치 업데이트."""
        if not updates:
            return 0
        now = datetime.now()
        with self.connect() as conn:
            with conn.cursor() as cur:
                for u in updates:
                    aid = u.get("asset_id")
                    if not aid:
                        continue
                    status = u.get("ingest_status", "pending")
                    if "spec_id" in u:
                        cur.execute(
                            "UPDATE raw_files SET ingest_status = %s, spec_id = %s, updated_at = %s WHERE asset_id = %s",
                            (status, u.get("spec_id"), now, aid),
                        )
                    else:
                        cur.execute(
                            "UPDATE raw_files SET ingest_status = %s, updated_at = %s WHERE asset_id = %s",
                            (status, now, aid),
                        )
            return len([x for x in updates if x.get("asset_id")])

    def batch_update_status(self, updates: list[dict]) -> int:
        """배치 상태 업데이트."""
        if not updates:
            return 0
        now = datetime.now()
        rows = []
        for u in updates:
            rows.append(
                (
                    u["status"],
                    u.get("error_message"),
                    u.get("archive_path"),
                    u.get("raw_bucket"),
                    now,
                    u["asset_id"],
                )
            )
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    UPDATE raw_files
                    SET ingest_status = %s,
                        error_message = %s,
                        archive_path = COALESCE(%s, archive_path),
                        raw_bucket = COALESCE(%s, raw_bucket),
                        updated_at = %s
                    WHERE asset_id = %s
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
        상태인 row 만 'failed' 로 마감."""
        normalized_request_id = self._norm_str(request_id)
        if not normalized_request_id:
            return 0
        like_pattern = f"dispatch_req_{normalized_request_id}_%"
        now = datetime.now()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE raw_files
                    SET ingest_status = 'failed',
                        error_message = %s,
                        updated_at = %s
                    WHERE ingest_batch_id LIKE %s
                      AND ingest_status IN ('pending', 'uploading')
                    RETURNING asset_id
                    """,
                    (error_message, now, like_pattern),
                )
                rows = cur.fetchall()
            return len(rows)

    def delete_asset_for_reingest(self, asset_id: str) -> None:
        """실패/중단된 ingest 자산을 재처리 가능 상태로 정리.

        ``connect()`` ctxmgr 가 outer transaction 을 자동 관리하므로
        DuckDB 처럼 명시적 BEGIN/COMMIT 호출 불필요.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM dataset_clips
                    WHERE clip_id IN (
                        SELECT clip_id FROM processed_clips WHERE source_asset_id = %s
                    )
                    """,
                    (asset_id,),
                )
                cur.execute("DELETE FROM image_metadata WHERE source_asset_id = %s", (asset_id,))
                cur.execute("DELETE FROM processed_clips WHERE source_asset_id = %s", (asset_id,))
                cur.execute("DELETE FROM labels WHERE asset_id = %s", (asset_id,))
                cur.execute("DELETE FROM video_metadata WHERE asset_id = %s", (asset_id,))
                cur.execute("DELETE FROM raw_files WHERE asset_id = %s", (asset_id,))

    def list_raw_files_by_source_unit_name(self, source_unit_name: str) -> list[dict[str, Any]]:
        normalized_name = self._norm_str(source_unit_name)
        if not normalized_name:
            return []
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM raw_files
                    WHERE source_unit_name = %s
                    ORDER BY created_at, raw_key, asset_id
                    """,
                    (normalized_name,),
                )
                return self._cursor_to_dicts(cur)

    def list_archived_raw_files_for_folder(self, folder_name: str) -> list[dict[str, Any]]:
        """Phase 2b: archive 에 이미 옮겨진 raw_files 조회.

        ``from_archived=True`` dispatch JSON 처리 시 folder_name 으로 raw_files 의
        archive-backed 행들을 lookup 해서 archive_path/raw_key/asset_id/media_type 반환.
        첫 archive dispatch upload 이후 행 상태가 ``completed`` 로 바뀌므로 재처리
        요청은 ``archived`` 와 ``completed`` 를 모두 허용한다.
        동일 folder 가 다중 source_unit_name (예: ``<folder>`` + ``<folder>/<sub>``) 으로
        저장된 경우 둘 다 매칭한다.
        """
        normalized = self._norm_str(folder_name)
        if not normalized:
            return []
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT asset_id, archive_path, raw_key, media_type, file_size, source_unit_name
                    FROM raw_files
                    WHERE (source_unit_name = %s OR source_unit_name LIKE %s)
                      AND ingest_status IN ('archived', 'completed')
                      AND archive_path IS NOT NULL
                    ORDER BY raw_key
                    """,
                    (normalized, f"{normalized}/%"),
                )
                rows = cur.fetchall()
        return [
            {
                "asset_id": r[0],
                "archive_path": r[1],
                "raw_key": r[2],
                "media_type": r[3],
                "file_size": r[4],
                "source_unit_name": r[5],
            }
            for r in rows
        ]

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
            placeholders = ", ".join(["%s"] * len(exact_values))
            clauses.append(f"error_message IN ({placeholders})")
            params.extend(exact_values)
        for pattern in like_values:
            clauses.append("error_message LIKE %s")
            params.append(pattern)

        where_filter = " OR ".join(clauses)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM raw_files
                    WHERE ingest_status = 'failed'
                      AND ({where_filter})
                    """,
                    params,
                )
                target_count = cur.fetchone()[0]
                if int(target_count) <= 0:
                    return 0
                cur.execute(
                    f"""
                    DELETE FROM raw_files
                    WHERE ingest_status = 'failed'
                      AND ({where_filter})
                    """,
                    params,
                )
            return int(target_count)

    def recover_archive_move_failed_asset(self, asset_id: str, archive_path: str) -> bool:
        normalized_asset_id = self._norm_str(asset_id)
        normalized_archive_path = self._norm_str(archive_path)
        if not normalized_asset_id or not normalized_archive_path:
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE raw_files
                    SET ingest_status = 'completed',
                        error_message = NULL,
                        archive_path = %s,
                        raw_bucket = COALESCE(raw_bucket, 'vlm-raw'),
                        updated_at = %s
                    WHERE asset_id = %s
                      AND ingest_status = 'failed'
                    RETURNING asset_id
                    """,
                    (normalized_archive_path, datetime.now(), normalized_asset_id),
                )
                row = cur.fetchone()
        return row is not None

    def list_completed_videos_for_spec_router(self, limit: int = 500) -> list[dict[str, Any]]:
        """ingest_router용: ingest_status=completed, spec_id 미설정 비디오."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT asset_id, COALESCE(source_unit_name, '') AS source_unit_name, raw_key
                    FROM raw_files
                    WHERE media_type = 'video'
                      AND ingest_status = 'completed'
                      AND (spec_id IS NULL OR spec_id = '')
                    ORDER BY created_at
                    LIMIT %s
                    """,
                    (max(1, int(limit)),),
                )
                rows = cur.fetchall()
            return [{"asset_id": r[0], "source_unit_name": r[1] or "", "raw_key": r[2]} for r in rows]

    def find_by_raw_key_stem(self, stem: str, source_unit_name: str | None = None) -> dict[str, Any] | None:
        with self.connect() as conn:
            # DuckDB regexp_extract(col, '[^/]+$') → PG substring(col FROM '[^/]+$')
            query = """
                SELECT * FROM raw_files
                WHERE substring(raw_key FROM '[^/]+$') LIKE %s || '.%%'
                  AND ingest_status = 'completed'
                """
            params: list[Any] = [stem]
            if source_unit_name:
                query += " AND source_unit_name = %s"
                params.append(str(source_unit_name))
            query += " LIMIT 1"
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                if result is None:
                    return None
                columns = [desc[0] for desc in cur.description]
            return self._row_to_dict(result, columns)
