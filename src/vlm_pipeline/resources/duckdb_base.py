"""DuckDB 기반 코드 — connect(), ensure_schema(), lock 처리 등."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from itertools import combinations
from pathlib import Path

import duckdb

from vlm_pipeline.lib.env_utils import is_duckdb_lock_conflict


class DuckDBBaseMixin:
    """DuckDB 커넥션 관리 기반 mixin."""

    db_path: str

    @staticmethod
    def _is_lock_conflict(exc: Exception) -> bool:
        return is_duckdb_lock_conflict(exc)

    @staticmethod
    def _phash_prefix_candidates(phash_hex: str, threshold: int, prefix_hex_len: int = 2) -> list[str]:
        """prefix hamming prefilter 후보(prefix) 생성."""
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

    def ensure_schema(self) -> None:
        """필수 스키마(raw_files 포함)를 보장한다."""
        schema_path = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"
        if not schema_path.exists():
            raise FileNotFoundError(f"schema.sql not found: {schema_path}")

        ddl = schema_path.read_text(encoding="utf-8")
        with self.connect() as conn:
            conn.execute(ddl)
            self._migrate_image_metadata_table(conn)
            self._ensure_image_metadata_indexes(conn)
            self._ensure_video_metadata_frame_columns(conn)
            self._ensure_labels_columns(conn)
            self._ensure_processed_clips_columns(conn)
            self._ensure_image_labels_table(conn)
            self._ensure_staging_dispatch_columns(conn)
            self._ensure_staging_model_configs(conn)
            self._ensure_staging_pipeline_runs(conn)

    @staticmethod
    def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = ?
            """,
            [table_name],
        ).fetchone()
        return bool(row and row[0] > 0)

    @classmethod
    def _table_columns(cls, conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
        if not cls._table_exists(conn, table_name):
            return set()
        rows = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND table_name = ?
            """,
            [table_name],
        ).fetchall()
        return {str(row[0]) for row in rows}

    @classmethod
    def _migrate_image_metadata_table(cls, conn: duckdb.DuckDBPyConnection) -> None:
        columns = cls._table_columns(conn, "image_metadata")
        if not columns:
            return
        desired_columns = {
            "image_id",
            "source_asset_id",
            "source_clip_id",
            "image_bucket",
            "image_key",
            "image_role",
            "frame_index",
            "frame_sec",
            "checksum",
            "file_size",
            "width",
            "height",
            "color_mode",
            "bit_depth",
            "has_alpha",
            "orientation",
            "extracted_at",
        }
        if columns == desired_columns:
            return
        if "asset_id" not in columns:
            if "image_id" not in columns or "source_asset_id" not in columns:
                return

        def _col(name: str, fallback_sql: str = "NULL") -> str:
            return f"im.{name}" if name in columns else fallback_sql

        conn.execute("DROP TABLE IF EXISTS image_metadata__migrated")
        conn.execute(
            """
            CREATE TABLE image_metadata__migrated (
                image_id         VARCHAR PRIMARY KEY,
                source_asset_id  VARCHAR NOT NULL REFERENCES raw_files(asset_id),
                source_clip_id   VARCHAR REFERENCES processed_clips(clip_id),
                image_bucket     VARCHAR DEFAULT 'vlm-raw',
                image_key        VARCHAR,
                image_role       VARCHAR DEFAULT 'source_image',
                frame_index      INTEGER,
                frame_sec        DOUBLE,
                checksum         VARCHAR,
                file_size        BIGINT,
                width            INTEGER,
                height           INTEGER,
                color_mode       VARCHAR DEFAULT 'RGB',
                bit_depth        INTEGER DEFAULT 8,
                has_alpha        BOOLEAN DEFAULT FALSE,
                orientation      INTEGER DEFAULT 1,
                extracted_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (image_bucket, image_key),
                UNIQUE (source_asset_id, source_clip_id, image_role, frame_index)
            )
            """
        )
        conn.execute(
            f"""
            INSERT INTO image_metadata__migrated (
                image_id,
                source_asset_id,
                source_clip_id,
                image_bucket,
                image_key,
                image_role,
                frame_index,
                frame_sec,
                checksum,
                file_size,
                width,
                height,
                color_mode,
                bit_depth,
                has_alpha,
                orientation,
                extracted_at
            )
            SELECT
                COALESCE({_col("image_id")}, {_col("asset_id")}) AS image_id,
                COALESCE({_col("source_asset_id")}, {_col("asset_id")}) AS source_asset_id,
                {_col("source_clip_id")} AS source_clip_id,
                COALESCE(NULLIF({_col("image_bucket", "NULL")}, ''), NULLIF(rf.raw_bucket, ''), 'vlm-raw') AS image_bucket,
                COALESCE({_col("image_key")}, rf.raw_key) AS image_key,
                COALESCE(NULLIF({_col("image_role", "NULL")}, ''), 'source_image') AS image_role,
                {_col("frame_index")} AS frame_index,
                {_col("frame_sec")} AS frame_sec,
                COALESCE({_col("checksum")}, rf.checksum) AS checksum,
                COALESCE({_col("file_size")}, rf.file_size) AS file_size,
                {_col("width")} AS width,
                {_col("height")} AS height,
                {_col("color_mode", "'RGB'")} AS color_mode,
                {_col("bit_depth", "8")} AS bit_depth,
                {_col("has_alpha", "FALSE")} AS has_alpha,
                {_col("orientation", "1")} AS orientation,
                {_col("extracted_at", "CURRENT_TIMESTAMP")} AS extracted_at
            FROM image_metadata im
            LEFT JOIN raw_files rf ON rf.asset_id = COALESCE({_col("source_asset_id")}, {_col("asset_id")})
            """
        )
        conn.execute("DROP TABLE image_metadata")
        conn.execute("ALTER TABLE image_metadata__migrated RENAME TO image_metadata")

    @classmethod
    def _ensure_image_metadata_indexes(cls, conn: duckdb.DuckDBPyConnection) -> None:
        columns = cls._table_columns(conn, "image_metadata")
        if not {"image_id", "source_asset_id", "source_clip_id"}.issubset(columns):
            return
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_image_metadata_bucket_key
            ON image_metadata(image_bucket, image_key)
            """
        )
        conn.execute("DROP INDEX IF EXISTS idx_image_metadata_source_role_frame")
        conn.execute("DROP INDEX IF EXISTS idx_image_metadata_source_clip_role_frame")
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_image_metadata_source_clip_role_frame
            ON image_metadata(source_asset_id, source_clip_id, image_role, frame_index)
            """
        )

    @classmethod
    def _ensure_video_metadata_frame_columns(cls, conn: duckdb.DuckDBPyConnection) -> None:
        columns = cls._table_columns(conn, "video_metadata")
        if not columns:
            return

        alter_specs = {
            "frame_extract_status": "VARCHAR DEFAULT 'pending'",
            "frame_extract_count": "INTEGER DEFAULT 0",
            "frame_extract_error": "TEXT",
            "frame_extracted_at": "TIMESTAMP",
            "auto_label_status": "VARCHAR DEFAULT 'pending'",
            "auto_label_error": "TEXT",
            "auto_label_key": "VARCHAR",
            "auto_labeled_at": "TIMESTAMP",
        }
        for column_name, column_type in alter_specs.items():
            if column_name in columns:
                continue
            conn.execute(f"ALTER TABLE video_metadata ADD COLUMN {column_name} {column_type}")

        conn.execute(
            """
            UPDATE video_metadata
            SET frame_extract_status = COALESCE(frame_extract_status, 'pending'),
                frame_extract_count = COALESCE(frame_extract_count, 0),
                auto_label_status = COALESCE(auto_label_status, 'pending')
            WHERE frame_extract_status IS NULL
               OR frame_extract_count IS NULL
               OR auto_label_status IS NULL
            """
        )

    @classmethod
    def _ensure_labels_columns(cls, conn: duckdb.DuckDBPyConnection) -> None:
        columns = cls._table_columns(conn, "labels")
        if not columns:
            return

        alter_specs = {
            "label_source": "VARCHAR DEFAULT 'manual'",
            "review_status": "VARCHAR DEFAULT 'pending'",
            "event_index": "INTEGER DEFAULT 0",
            "timestamp_start_sec": "DOUBLE",
            "timestamp_end_sec": "DOUBLE",
            "caption_text": "TEXT",
            "object_count": "INTEGER DEFAULT 0",
        }
        for column_name, column_type in alter_specs.items():
            if column_name in columns:
                continue
            conn.execute(f"ALTER TABLE labels ADD COLUMN {column_name} {column_type}")

        conn.execute(
            """
            UPDATE labels
            SET label_source = COALESCE(label_source, 'manual'),
                review_status = COALESCE(review_status, 'pending'),
                event_index = COALESCE(event_index, 0),
                object_count = COALESCE(object_count, 0)
            WHERE label_source IS NULL
               OR review_status IS NULL
               OR event_index IS NULL
               OR object_count IS NULL
            """
        )

    @classmethod
    def _ensure_processed_clips_columns(cls, conn: duckdb.DuckDBPyConnection) -> None:
        columns = cls._table_columns(conn, "processed_clips")
        if not columns:
            return

        alter_specs = {
            "clip_start_sec": "DOUBLE",
            "clip_end_sec": "DOUBLE",
            "data_source": "VARCHAR DEFAULT 'manual'",
            "caption_text": "TEXT",
            "duration_sec": "DOUBLE",
            "fps": "DOUBLE",
            "frame_count": "INTEGER",
            "image_extract_status": "VARCHAR DEFAULT 'pending'",
            "image_extract_count": "INTEGER DEFAULT 0",
            "image_extract_error": "TEXT",
            "image_extracted_at": "TIMESTAMP",
        }
        for column_name, column_type in alter_specs.items():
            if column_name in columns:
                continue
            conn.execute(f"ALTER TABLE processed_clips ADD COLUMN {column_name} {column_type}")

        conn.execute(
            """
            UPDATE processed_clips
            SET data_source = COALESCE(data_source, 'manual'),
                image_extract_status = COALESCE(image_extract_status, 'pending'),
                image_extract_count = COALESCE(image_extract_count, 0)
            WHERE data_source IS NULL
               OR image_extract_status IS NULL
               OR image_extract_count IS NULL
            """
        )

    @classmethod
    def _ensure_image_labels_table(cls, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS image_labels (
                image_label_id   VARCHAR PRIMARY KEY,
                image_id         VARCHAR REFERENCES image_metadata(image_id),
                source_clip_id   VARCHAR REFERENCES processed_clips(clip_id),
                labels_bucket    VARCHAR DEFAULT 'vlm-labels',
                labels_key       VARCHAR,
                label_format     VARCHAR,
                label_tool       VARCHAR,
                label_source     VARCHAR,
                review_status    VARCHAR DEFAULT 'pending',
                label_status     VARCHAR DEFAULT 'pending',
                object_count     INTEGER DEFAULT 0,
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    # ── Staging 전용 테이블 보장 ──

    @classmethod
    def _ensure_staging_dispatch_columns(cls, conn: duckdb.DuckDBPyConnection) -> None:
        """staging_dispatch_requests에 YOLO 파라미터 컬럼이 없으면 추가."""
        columns = cls._table_columns(conn, "staging_dispatch_requests")
        if not columns:
            return  # 테이블 자체가 없으면 schema.sql의 CREATE TABLE이 처리

        alter_specs = {
            "max_frames_per_video": "INTEGER",
            "jpeg_quality": "INTEGER",
            "confidence_threshold": "DOUBLE",
            "iou_threshold": "DOUBLE",
            "completed_at": "TIMESTAMP",
        }
        for column_name, column_type in alter_specs.items():
            if column_name in columns:
                continue
            conn.execute(
                f"ALTER TABLE staging_dispatch_requests ADD COLUMN {column_name} {column_type}"
            )

    @classmethod
    def _ensure_staging_model_configs(cls, conn: duckdb.DuckDBPyConnection) -> None:
        """staging_model_configs 테이블 보장 + 기본 시드 데이터 삽입."""
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staging_model_configs (
                config_id            VARCHAR PRIMARY KEY,
                output_type          VARCHAR NOT NULL UNIQUE,
                model_name           VARCHAR NOT NULL,
                model_version        VARCHAR,
                default_max_frames   INTEGER DEFAULT 24,
                default_jpeg_quality INTEGER DEFAULT 90,
                default_confidence   DOUBLE DEFAULT 0.25,
                default_iou          DOUBLE DEFAULT 0.45,
                extra_params         VARCHAR,
                is_active            BOOLEAN DEFAULT TRUE,
                description          TEXT,
                updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        # 기본 시드 데이터: output_type별 모델 매핑
        seed_rows = [
            (
                "cfg_bbox",
                "bbox",
                "yolov8l-worldv2",
                "v2",
                24,
                90,
                0.25,
                0.45,
                None,
                True,
                "YOLO-World-L bbox detection — GPU 서버 호출",
            ),
            (
                "cfg_timestamp",
                "timestamp",
                "gemini-2.0-flash",
                "2.0",
                12,
                90,
                None,
                None,
                None,
                True,
                "Gemini 이벤트 구간(timestamp) 추출",
            ),
            (
                "cfg_captioning",
                "captioning",
                "gemini-2.0-flash",
                "2.0",
                12,
                90,
                None,
                None,
                None,
                True,
                "Gemini 캡셔닝 + clip 절단 + 프레임 추출",
            ),
        ]
        for row in seed_rows:
            conn.execute(
                """
                INSERT INTO staging_model_configs (
                    config_id, output_type, model_name, model_version,
                    default_max_frames, default_jpeg_quality,
                    default_confidence, default_iou,
                    extra_params, is_active, description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (config_id) DO NOTHING
                """,
                list(row),
            )

    @classmethod
    def _ensure_staging_pipeline_runs(cls, conn: duckdb.DuckDBPyConnection) -> None:
        """staging_pipeline_runs 테이블 보장."""
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staging_pipeline_runs (
                run_id               VARCHAR PRIMARY KEY,
                request_id           VARCHAR,
                folder_name          VARCHAR,
                step_name            VARCHAR NOT NULL,
                step_order           INTEGER DEFAULT 0,
                step_status          VARCHAR DEFAULT 'pending',
                model_name           VARCHAR,
                model_version        VARCHAR,
                applied_params       VARCHAR,
                input_count          INTEGER DEFAULT 0,
                output_count         INTEGER DEFAULT 0,
                error_count          INTEGER DEFAULT 0,
                started_at           TIMESTAMP,
                completed_at         TIMESTAMP,
                error_message        TEXT,
                created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
