"""DuckDBResource — Dagster ConfigurableResource, 전 테이블 CRUD 통합."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from datetime import datetime
from itertools import combinations
from typing import Any
from uuid import uuid4

import duckdb
from dagster import ConfigurableResource


class DuckDBResource(ConfigurableResource):
    """DuckDB 통합 리소스 — 섹션별 CRUD 메서드 제공."""

    db_path: str  # PipelineConfig.dataops_duckdb_path → EnvVar로 주입

    @staticmethod
    def _is_lock_conflict(exc: Exception) -> bool:
        message = str(exc)
        return (
            "Could not set lock on file" in message
            and "Conflicting lock is held" in message
        )

    @staticmethod
    def _phash_prefix_candidates(phash_hex: str, threshold: int, prefix_hex_len: int = 2) -> list[str]:
        """prefix hamming prefilter 후보(prefix) 생성.

        전체 hamming <= threshold의 필요조건(prefix hamming <= threshold)을 이용한다.
        """
        if not phash_hex:
            return []
        normalized = str(phash_hex).strip().lower()
        prefix_hex_len = max(1, min(4, int(prefix_hex_len)))
        prefix = normalized[:prefix_hex_len]
        if len(prefix) < prefix_hex_len:
            return []

        try:
            base = int(prefix, 16)
        except ValueError:
            return []

        total_bits = prefix_hex_len * 4
        max_flip = max(0, min(int(threshold), total_bits))
        all_count = 1 << total_bits
        if max_flip >= total_bits:
            return [f"{value:0{prefix_hex_len}x}" for value in range(all_count)]

        values: set[int] = set()
        for flip_count in range(max_flip + 1):
            for bit_positions in combinations(range(total_bits), flip_count):
                candidate = base
                for pos in bit_positions:
                    candidate ^= 1 << pos
                values.add(candidate)

        return [f"{value:0{prefix_hex_len}x}" for value in sorted(values)]

    @contextmanager
    def connect(self):
        """DuckDB 커넥션 컨텍스트 매니저."""
        retry_count = max(0, int(os.getenv("DUCKDB_LOCK_RETRY_COUNT", "20")))
        base_delay_ms = max(10, int(os.getenv("DUCKDB_LOCK_RETRY_DELAY_MS", "100")))
        max_delay_ms = max(
            base_delay_ms,
            int(os.getenv("DUCKDB_LOCK_RETRY_MAX_DELAY_MS", "2000")),
        )

        conn = None
        for attempt in range(retry_count + 1):
            try:
                conn = duckdb.connect(self.db_path)
                break
            except Exception as exc:  # noqa: BLE001
                if not self._is_lock_conflict(exc) or attempt >= retry_count:
                    raise
                delay_ms = min(max_delay_ms, base_delay_ms * (2 ** attempt))
                time.sleep(delay_ms / 1000.0)

        if conn is None:
            raise RuntimeError("DuckDB connection failed unexpectedly")

        try:
            yield conn
        finally:
            conn.close()

    # ── INGEST 섹션 (Dev A) ────────────────────────────

    def insert_raw_files_batch(self, records: list[dict]) -> int:
        """raw_files 배치 INSERT. 반환: 삽입된 행 수."""
        if not records:
            return 0
        rows = []
        for rec in records:
            now = datetime.now()
            rows.append(
                [
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
            )

        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO raw_files (
                    asset_id, source_path, original_name, media_type,
                    file_size, checksum, archive_path, raw_bucket, raw_key,
                    ingest_batch_id, transfer_tool, ingest_status, error_message,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return len(rows)

    def insert_image_metadata(self, asset_id: str, meta: dict) -> None:
        """image_metadata INSERT (1:1 with raw_files)."""
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO image_metadata (
                    asset_id, width, height, color_mode, bit_depth,
                    codec, has_alpha, orientation, extracted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    asset_id,
                    meta.get("width"),
                    meta.get("height"),
                    meta.get("color_mode", "RGB"),
                    meta.get("bit_depth", 8),
                    meta.get("codec"),
                    meta.get("has_alpha", False),
                    meta.get("orientation", 1),
                    meta.get("extracted_at", datetime.now()),
                ],
            )

    def insert_video_metadata(self, asset_id: str, meta: dict) -> None:
        """video_metadata INSERT (1:1 with raw_files)."""
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO video_metadata (
                    asset_id, width, height, duration_sec, fps,
                    codec, bitrate, frame_count, has_audio,
                    environment_type, daynight_type, outdoor_score,
                    avg_brightness, env_method, extracted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    asset_id,
                    meta.get("width"),
                    meta.get("height"),
                    meta.get("duration_sec"),
                    meta.get("fps"),
                    meta.get("codec"),
                    meta.get("bitrate"),
                    meta.get("frame_count"),
                    meta.get("has_audio", False),
                    meta.get("environment_type"),
                    meta.get("daynight_type"),
                    meta.get("outdoor_score"),
                    meta.get("avg_brightness"),
                    meta.get("env_method"),
                    meta.get("extracted_at", datetime.now()),
                ],
            )

    def find_by_checksum(self, checksum: str, completed_only: bool = True) -> dict[str, Any] | None:
        """checksum으로 raw_files 조회. 존재하면 dict, 없으면 None.

        기본값은 completed 상태만 조회하여, 실패/재시도 잔재가
        신규 ingest를 영구 차단하지 않도록 한다.
        """
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
        """checksum으로 상태 무관 raw_files 조회."""
        return self.find_by_checksum(checksum, completed_only=False)

    def has_raw_file(self, asset_id: str) -> bool:
        """asset_id 기준 raw_files 존재 여부."""
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
        """동일 source_path에 동일 duplicate_of 실패 레코드가 이미 있는지 확인."""
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
        """raw_files.ingest_status 업데이트.

        raw_bucket이 전달되면 기존 NULL 값을 백필한다.
        """
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

    def mark_duplicate_skipped_assets(self, duplicate_asset_files: dict[str, str]) -> int:
        """기존 원본 자산(raw_files)에 duplicate 스킵 이력을 기록.

        - error_message가 비어있으면 새 marker를 기록한다.
        - 기존 error_message가 duplicate marker면 파일명을 누적(merge)한다.
        - 그 외 타입의 error_message는 덮어쓰지 않는다.
        """
        normalized_items: dict[str, set[str]] = {}

        def _parse_file_names(payload: str) -> set[str]:
            names = {name.strip() for name in str(payload or "").split(",") if name.strip()}
            return names or {"unknown_file"}

        for asset_id, file_name in (duplicate_asset_files or {}).items():
            normalized_id = str(asset_id or "").strip()
            if not normalized_id:
                continue
            normalized_items.setdefault(normalized_id, set()).update(_parse_file_names(file_name))

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
                file_names = normalized_items[asset_id]
                if not current_error:
                    merged_files = set(file_names)
                elif current_error.startswith("duplicate_skipped_in_manifest:"):
                    existing_payload = current_error.replace("duplicate_skipped_in_manifest:", "", 1)
                    merged_files = _parse_file_names(existing_payload) | set(file_names)
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
        """실패/중단된 ingest 자산을 재처리 가능 상태로 정리."""
        with self.connect() as conn:
            conn.execute(
                """
                DELETE FROM dataset_clips
                WHERE clip_id IN (
                    SELECT clip_id FROM processed_clips WHERE source_asset_id = ?
                )
                """,
                [asset_id],
            )
            conn.execute("DELETE FROM processed_clips WHERE source_asset_id = ?", [asset_id])
            conn.execute("DELETE FROM labels WHERE asset_id = ?", [asset_id])
            conn.execute("DELETE FROM image_metadata WHERE asset_id = ?", [asset_id])
            conn.execute("DELETE FROM video_metadata WHERE asset_id = ?", [asset_id])
            conn.execute("DELETE FROM raw_files WHERE asset_id = ?", [asset_id])

    def delete_failed_rows_by_error_filters(
        self,
        *,
        exact_errors: list[str] | None = None,
        like_patterns: list[str] | None = None,
    ) -> int:
        """raw_files failed row를 error_message 필터로 삭제한다."""
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
        """archive 파일 존재가 확인된 failed row를 completed로 복구."""
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

    # ── DEDUP 섹션 (Dev B) ─────────────────────────────

    def find_phash_null(self, limit: int = 100) -> list[dict]:
        """phash IS NULL인 이미지 raw_files 조회.

        archive_path, source_path를 포함하여 로컬 파일 우선 fallback을 지원한다.
        """
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT asset_id, raw_bucket, raw_key, archive_path, source_path
                FROM raw_files
                WHERE phash IS NULL
                  AND media_type = 'image'
                  AND ingest_status = 'completed'
                ORDER BY created_at
                LIMIT ?
                """,
                [limit],
            ).fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "archive_path", "source_path"]
            return [dict(zip(columns, row)) for row in rows]

    def update_phash(self, asset_id: str, phash: str) -> None:
        """raw_files.phash UPDATE."""
        with self.connect() as conn:
            conn.execute(
                "UPDATE raw_files SET phash = ?, updated_at = ? WHERE asset_id = ?",
                [phash, datetime.now(), asset_id],
            )

    def clear_error_message(self, asset_id: str) -> None:
        """pHash 성공 시 이전 phash_source_missing 등의 error_message를 정리."""
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE raw_files
                SET error_message = NULL, updated_at = ?
                WHERE asset_id = ?
                  AND error_message LIKE 'phash_%'
                """,
                [datetime.now(), asset_id],
            )

    def update_dup_group(self, asset_id: str, group_id: str) -> None:
        """raw_files.dup_group_id UPDATE."""
        with self.connect() as conn:
            conn.execute(
                "UPDATE raw_files SET dup_group_id = ?, updated_at = ? WHERE asset_id = ?",
                [group_id, datetime.now(), asset_id],
            )

    def find_similar_phash(
        self,
        phash_hex: str,
        threshold: int = 5,
        exclude_asset_id: str | None = None,
        prefix_hex_len: int = 2,
    ) -> list[dict]:
        """Hamming distance 기반 유사 이미지 검색.

        prefix prefilter 후 Python bit_count로 최종 거리 계산.
        """
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
                where_parts.append("asset_id != ?")
                params.append(exclude_asset_id)

            if candidates and len(candidates) < all_prefix_count:
                placeholders = ", ".join(["?"] * len(candidates))
                where_parts.append(
                    f"lower(substr(phash, 1, {normalized_prefix_len})) IN ({placeholders})"
                )
                params.extend(candidates)

            query = (
                "SELECT asset_id, phash FROM raw_files "
                f"WHERE {' AND '.join(where_parts)}"
            )
            rows = conn.execute(query, params).fetchall()

        result: list[dict[str, Any]] = []
        for asset_id, other_phash in rows:
            if not other_phash:
                continue
            try:
                distance = (target_int ^ int(str(other_phash), 16)).bit_count()
            except ValueError:
                continue
            if distance <= threshold:
                result.append(
                    {"asset_id": asset_id, "phash": other_phash, "distance": distance}
                )
        result.sort(key=lambda row: (row["distance"], str(row["asset_id"])))
        return result

    # ── LABEL 섹션 (Dev B) ─────────────────────────────

    def insert_label(self, label: dict) -> None:
        """labels 테이블 INSERT."""
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO labels (
                    label_id, asset_id, labels_bucket, labels_key,
                    label_format, label_tool, event_count, label_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    label.get("label_id") or str(uuid4()),
                    label.get("asset_id"),
                    label.get("labels_bucket", "vlm-labels"),
                    label.get("labels_key"),
                    label.get("label_format"),
                    label.get("label_tool", "pre-built"),
                    label.get("event_count"),
                    label.get("label_status", "completed"),
                    label.get("created_at", datetime.now()),
                ],
            )

    def find_by_raw_key_stem(self, stem: str) -> dict[str, Any] | None:
        """raw_key의 basename stem으로 raw_files 조회.

        예) stem='frame_001' → raw_key='2026/02/frame_001.jpg' 매칭.
        """
        with self.connect() as conn:
            result = conn.execute(
                """
                SELECT * FROM raw_files
                WHERE regexp_extract(raw_key, '[^/]+$')
                      LIKE ? || '.%'
                  AND ingest_status = 'completed'
                LIMIT 1
                """,
                [stem],
            ).fetchone()
            if result is None:
                return None
            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, result))

    # ── PROCESS 섹션 (Dev B) ───────────────────────────

    def find_processable(self) -> list[dict]:
        """INGEST+LABEL 모두 completed인 건 조회 (PROCESS 대상)."""
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT r.asset_id, r.raw_bucket, r.raw_key, r.media_type,
                       l.label_id, l.labels_bucket, l.labels_key
                FROM raw_files r
                JOIN labels l ON r.asset_id = l.asset_id
                WHERE r.ingest_status = 'completed'
                  AND l.label_status = 'completed'
                  AND r.asset_id NOT IN (
                      SELECT source_asset_id FROM processed_clips
                      WHERE process_status = 'completed'
                  )
                """
            ).fetchall()
            columns = [
                "asset_id", "raw_bucket", "raw_key", "media_type",
                "label_id", "labels_bucket", "labels_key",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def insert_processed_clip(self, clip: dict) -> None:
        """processed_clips INSERT (width/height/codec 포함)."""
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO processed_clips (
                    clip_id, source_asset_id, source_label_id, event_index,
                    checksum, file_size, processed_bucket, clip_key, label_key,
                    width, height, codec, process_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    clip.get("clip_id") or str(uuid4()),
                    clip.get("source_asset_id"),
                    clip.get("source_label_id"),
                    clip.get("event_index", 0),
                    clip.get("checksum"),
                    clip.get("file_size"),
                    clip.get("processed_bucket", "vlm-processed"),
                    clip.get("clip_key"),
                    clip.get("label_key"),
                    clip.get("width"),
                    clip.get("height"),
                    clip.get("codec"),
                    clip.get("process_status", "completed"),
                    clip.get("created_at", datetime.now()),
                ],
            )

    # ── BUILD 섹션 (Dev B) ─────────────────────────────

    def find_dataset_candidates(self) -> list[dict]:
        """process_status='completed'인 processed_clips 조회."""
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT clip_id, source_asset_id, processed_bucket, clip_key,
                       label_key, width, height, codec
                FROM processed_clips
                WHERE process_status = 'completed'
                """
            ).fetchall()
            columns = [
                "clip_id", "source_asset_id", "processed_bucket", "clip_key",
                "label_key", "width", "height", "codec",
            ]
            return [dict(zip(columns, row)) for row in rows]

    def insert_dataset(self, dataset: dict) -> None:
        """datasets 테이블 INSERT."""
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO datasets (
                    dataset_id, name, version, config, split_ratio,
                    dataset_bucket, dataset_prefix, build_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    dataset.get("dataset_id") or str(uuid4()),
                    dataset.get("name"),
                    dataset.get("version"),
                    dataset.get("config"),
                    dataset.get("split_ratio"),
                    dataset.get("dataset_bucket", "vlm-dataset"),
                    dataset.get("dataset_prefix"),
                    dataset.get("build_status", "pending"),
                    dataset.get("created_at", datetime.now()),
                ],
            )

    def update_dataset_status(self, dataset_id: str, status: str) -> None:
        """datasets.build_status UPDATE."""
        with self.connect() as conn:
            conn.execute(
                "UPDATE datasets SET build_status = ? WHERE dataset_id = ?",
                [status, dataset_id],
            )

    def insert_dataset_clips_batch(self, clips: list[dict]) -> None:
        """dataset_clips 배치 INSERT."""
        if not clips:
            return
        rows = [
            [
                clip["dataset_id"],
                clip["clip_id"],
                clip["split"],
                clip.get("dataset_key"),
            ]
            for clip in clips
        ]
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO dataset_clips (dataset_id, clip_id, split, dataset_key)
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )
