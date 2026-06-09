"""PG INGEST AUDIT 도메인 — raw_files DQ/audit 메서드 (count + mark)."""

from __future__ import annotations

from datetime import datetime


class PostgresIngestAuditMixin:  # pure mixin — no base; relies on self.connect() duck-typing
    """DQ inspection / duplicate 처리 등 audit 성격 메서드."""

    def count_unresolved_rows_for_source_unit(self, source_unit_path: str) -> int:
        normalized_path = self._norm_str(source_unit_path)
        if not normalized_path:
            return 0

        like_pattern = f"{normalized_path.rstrip('/')}/%"
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM raw_files
                    WHERE (source_path = %s OR source_path LIKE %s)
                      AND NOT (
                          ingest_status IN ('completed', 'skipped')
                          OR (
                              ingest_status = 'failed'
                              AND error_message LIKE 'duplicate_of:%%'
                          )
                      )
                    """,
                    (normalized_path, like_pattern),
                )
                row = cur.fetchone()
        return int(row[0]) if row else 0

    def count_raw_files_for_source_unit_name(self, source_unit_name: str) -> int:
        normalized_name = self._norm_str(source_unit_name)
        if not normalized_name:
            return 0
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM raw_files
                    WHERE source_unit_name = %s
                    """,
                    (normalized_name,),
                )
                row = cur.fetchone()
        return int(row[0]) if row else 0

    def count_completed_raw_files_without_archive(self) -> int:
        """Phase 3-C asset_check — raw_files.ingest_status='completed' 인데 archive_path NULL.

        정상은 0. >0 이면 archive 이동 누락 (orchestrator 가 archive 이동 전에 status
        토글한 케이스 등).
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM raw_files
                    WHERE ingest_status = 'completed'
                      AND archive_path IS NULL
                    """
                )
                row = cur.fetchone()
        return int(row[0]) if row else 0

    def count_cross_table_inconsistencies(self) -> dict[str, int]:
        """Phase 3-B inline DQ #5 — 파이프라인 중간 탈락 카운트.

        반환 dict:
          - ``ingest_completed_no_metadata``:
              video raw_files 가 ingest_status='completed' 인데 video_metadata 행이 없음.
              Phase A meta 추출이 통과했어도 DB insert 가 누락된 케이스 (transient PG
              에러 + retry race 등). 정상은 0.
          - ``timestamp_completed_no_label_key``:
              video_metadata.timestamp_status='completed' 인데 timestamp_label_key 가 NULL.
              clip_timestamp 가 status 만 업데이트하고 label_key set 을 빠뜨린 케이스.
              정상은 0.

        둘 다 read-only SELECT — 운영 트래픽 영향 미미 (≤ 10ms typical).
        """
        result = {
            "ingest_completed_no_metadata": 0,
            "timestamp_completed_no_label_key": 0,
        }
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM raw_files r
                    WHERE r.ingest_status = 'completed'
                      AND r.media_type = 'video'
                      AND NOT EXISTS (
                          SELECT 1 FROM video_metadata vm
                          WHERE vm.asset_id = r.asset_id
                      )
                    """
                )
                row = cur.fetchone()
                result["ingest_completed_no_metadata"] = int(row[0]) if row else 0

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM video_metadata
                    WHERE timestamp_status = 'completed'
                      AND timestamp_label_key IS NULL
                    """
                )
                row = cur.fetchone()
                result["timestamp_completed_no_label_key"] = int(row[0]) if row else 0
        return result

    def mark_duplicate_skipped_assets(self, duplicate_asset_files: dict[str, list[str]]) -> int:
        normalized_items: dict[str, list[str]] = {}

        def _parse_file_names(payload: str) -> list[str]:
            names = [name.strip() for name in str(payload or "").split(",") if name.strip()]
            return names or ["unknown_file"]

        for asset_id, file_names in (duplicate_asset_files or {}).items():
            normalized_id = self._norm_str(asset_id)
            if not normalized_id:
                continue
            merged = normalized_items.setdefault(normalized_id, [])
            if isinstance(file_names, list):
                merged.extend(str(file_name).strip() or "unknown_file" for file_name in file_names)
            else:
                merged.extend(_parse_file_names(str(file_names)))

        if not normalized_items:
            return 0

        now = datetime.now()
        with self.connect() as conn:
            placeholders = ", ".join(["%s"] * len(normalized_items))
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT asset_id, error_message FROM raw_files WHERE asset_id IN ({placeholders})",
                    list(normalized_items.keys()),
                )
                rows = cur.fetchall()

            updates: list[tuple] = []
            for asset_id, current_error_raw in rows:
                current_error = self._norm_str(current_error_raw)
                file_names = [file_name for file_name in normalized_items[asset_id] if str(file_name).strip()] or [
                    "unknown_file"
                ]
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
                with conn.cursor() as cur:
                    cur.executemany(
                        "UPDATE raw_files SET error_message = %s, updated_at = %s WHERE asset_id = %s",
                        updates,
                    )

        return len(updates)
