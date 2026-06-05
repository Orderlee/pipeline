"""PG DEDUP 도메인 — phash, dup_group, similar 검색 등 (DuckDBDedupMixin 1:1 포팅).

핵심 변환:
  - ``INSERT OR REPLACE INTO labels`` → ``ON CONFLICT (label_id) DO UPDATE``
  - DuckDB ``LIST(DISTINCT col)`` → PG ``array_agg(DISTINCT col)``
  - DuckDB ``ANY_VALUE(col)`` → PG 15 호환 ``(array_agg(col))[1]`` (PG 16+ 면 ANY_VALUE 가능하지만 PG 15 fallback)
  - DuckDB ``substr(col, 1, n)`` → PG ``substr(col, 1, n)`` (동일)
  - ``processed_clips`` upsert 는 이미 PG-style ``ON CONFLICT`` 였음 (변경 없음)

Process/labels 쿼리는 ``postgres_process.py`` 로, build 쿼리는 ``postgres_build.py`` 로 분리됨.
이 모듈은 phash/dup_group 순수 dedup 메서드만 포함.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .postgres_build import PostgresBuildMixin
from .postgres_process import PostgresProcessMixin


class PostgresDedupMixin(PostgresProcessMixin, PostgresBuildMixin):
    """DEDUP 관련 PostgreSQL 메서드 mixin.

    phash/dup_group 순수 dedup 메서드를 직접 구현하고,
    ProcessMixin/BuildMixin 을 상속해 하나의 import 경로로 모든 메서드를 노출한다
    (기존 callers / conftest.py 가 PostgresDedupMixin 만 import 해도 동작하도록).
    """

    def find_phash_null(self, limit: int = 100) -> list[dict]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT asset_id, raw_bucket, raw_key, archive_path, source_path
                    FROM raw_files
                    WHERE phash IS NULL
                      AND media_type = 'image'
                      AND ingest_status = 'completed'
                    ORDER BY created_at
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "archive_path", "source_path"]
            return self._rows_to_dicts(rows, columns)

    def find_inline_dedup_targets(
        self,
        *,
        prioritized_asset_ids: list[str],
        limit: int,
    ) -> list[dict]:
        """Inline DEDUP 대상 조회 — 현재 manifest 자산 우선, 남는 슬롯은 backlog.

        prioritized_asset_ids 가 raw_files 에 존재(media_type=image, ingest=completed, phash NULL)하면
        그 자산들을 먼저 반환. 그 외 슬롯은 동일 조건의 backlog 로 채움(prioritized 는 제외).
        """
        normalized_limit = max(1, int(limit))
        prioritized_set = {str(asset_id).strip() for asset_id in (prioritized_asset_ids or []) if str(asset_id).strip()}
        columns = ["asset_id", "raw_bucket", "raw_key", "archive_path", "source_path"]
        with self.connect() as conn:
            with conn.cursor() as cur:
                prioritized_targets: list[dict] = []
                if prioritized_set:
                    placeholders = ", ".join(["%s"] * len(prioritized_set))
                    cur.execute(
                        f"""
                        SELECT asset_id, raw_bucket, raw_key, archive_path, source_path
                        FROM raw_files
                        WHERE asset_id IN ({placeholders})
                          AND media_type = 'image'
                          AND ingest_status = 'completed'
                          AND phash IS NULL
                        ORDER BY created_at
                        """,
                        tuple(prioritized_set),
                    )
                    prioritized_targets = self._rows_to_dicts(cur.fetchall(), columns)

                remaining_limit = max(0, normalized_limit - len(prioritized_targets))
                if remaining_limit <= 0:
                    return prioritized_targets

                params: list[Any] = []
                exclude_sql = ""
                if prioritized_set:
                    placeholders = ", ".join(["%s"] * len(prioritized_set))
                    exclude_sql = f"AND asset_id NOT IN ({placeholders})"
                    params.extend(prioritized_set)
                params.append(remaining_limit)

                cur.execute(
                    f"""
                    SELECT asset_id, raw_bucket, raw_key, archive_path, source_path
                    FROM raw_files
                    WHERE media_type = 'image'
                      AND ingest_status = 'completed'
                      AND phash IS NULL
                      {exclude_sql}
                    ORDER BY created_at
                    LIMIT %s
                    """,
                    tuple(params),
                )
                backlog_targets = self._rows_to_dicts(cur.fetchall(), columns)

        return prioritized_targets + backlog_targets

    def mark_inline_dedup_failure(self, asset_id: str, error_message: str) -> None:
        """Inline DEDUP 실패시 raw_files.error_message 에 phash_failed:<msg> 기록."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE raw_files
                    SET error_message = %s, updated_at = %s
                    WHERE asset_id = %s
                    """,
                    (f"phash_failed:{error_message}", datetime.now(), asset_id),
                )

    def update_phash(self, asset_id: str, phash: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE raw_files SET phash = %s, updated_at = %s WHERE asset_id = %s",
                    (phash, datetime.now(), asset_id),
                )

    def clear_error_message(self, asset_id: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE raw_files
                    SET error_message = NULL, updated_at = %s
                    WHERE asset_id = %s
                      AND error_message LIKE 'phash_%%'
                    """,
                    (datetime.now(), asset_id),
                )

    def update_dup_group(self, asset_id: str, group_id: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE raw_files SET dup_group_id = %s, updated_at = %s WHERE asset_id = %s",
                    (group_id, datetime.now(), asset_id),
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
                where_parts.append("asset_id != %s")
                params.append(exclude_asset_id)

            if candidates and len(candidates) < all_prefix_count:
                placeholders = ", ".join(["%s"] * len(candidates))
                where_parts.append(f"lower(substr(phash, 1, {normalized_prefix_len})) IN ({placeholders})")
                params.extend(candidates)

            query = f"SELECT asset_id, phash FROM raw_files WHERE {' AND '.join(where_parts)}"
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

        result: list[dict[str, Any]] = []
        for asset_id, other_phash in rows:
            if not other_phash:
                continue
            try:
                distance = (target_int ^ int(str(other_phash), 16)).bit_count()
            except ValueError:
                continue
            if distance <= threshold:
                result.append({"asset_id": asset_id, "phash": other_phash, "distance": distance})
        result.sort(key=lambda row: (row["distance"], str(row["asset_id"])))
        return result
