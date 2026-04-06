"""DuckDB 스키마 마이그레이션 mixin — ALTER TABLE, 테이블 재구성, 인덱스 관리."""

from __future__ import annotations

from typing import ClassVar

import duckdb


class DuckDBMigrationMixin:
    """스키마 진화·마이그레이션 전담 mixin.

    ``_table_exists``, ``_table_columns``, ``_load_schema_ddl``, ``connect`` 등은
    ``DuckDBBaseMixin`` 에서 제공된다(MRO 기준).
    """

    _runtime_schema_ensured: ClassVar[bool] = False

    # ── Public entry-points ──

    def ensure_schema(self) -> None:
        """필수 스키마(raw_files 포함)를 보장한다."""
        ddl = self._load_schema_ddl(include_image_labels=False)
        with self.connect() as conn:
            conn.execute(ddl)
            self._ensure_image_metadata_columns(conn, backfill=True)
            self._ensure_image_metadata_indexes(conn, cleanup_legacy=True)
            self._ensure_video_metadata_frame_columns(conn, backfill=True)
            self._ensure_labels_columns(conn, backfill=True)
            self._ensure_processed_clips_columns(conn, backfill=True)
            self._ensure_image_labels_table(conn, restore_backup=True)
            self._ensure_staging_dispatch_columns(conn)
            self._ensure_staging_model_configs(conn)
            self._ensure_staging_pipeline_runs(conn)
            self._ensure_raw_files_spec_columns(conn)
            self._ensure_video_metadata_stage_columns(conn)
            self._ensure_video_metadata_reencode_columns(conn)

    def ensure_runtime_schema(self) -> None:
        """런타임 hot path용 경량 스키마 보장.

        컬럼 존재 여부와 필수 테이블 생성만 수행하고, 대량 backfill/repair/index 재생성은 하지 않는다.
        운영 배포 시 명시적 migration 단계에서 ensure_schema() 또는 별도 repair 스크립트를 사용한다.

        프로세스 레벨 ClassVar flag로 첫 호출 시만 DDL을 실행한다.
        code-server 재시작 시 자동 리셋된다.
        """
        if DuckDBMigrationMixin._runtime_schema_ensured:
            return
        ddl = self._load_schema_ddl(include_image_labels=False)
        with self.connect() as conn:
            conn.execute(ddl)
            self._ensure_image_metadata_columns(conn, backfill=False)
            self._ensure_video_metadata_frame_columns(conn, backfill=False)
            self._ensure_labels_columns(conn, backfill=False)
            self._ensure_processed_clips_columns(conn, backfill=False)
            self._ensure_image_labels_table(conn, restore_backup=False)
            self._ensure_staging_dispatch_columns(conn)
            self._ensure_staging_model_configs(conn)
            self._ensure_staging_pipeline_runs(conn)
            self._ensure_raw_files_spec_columns(conn)
            self._ensure_video_metadata_stage_columns(conn)
            self._ensure_video_metadata_reencode_columns(conn)
        DuckDBMigrationMixin._runtime_schema_ensured = True

    def repair_image_metadata_table(self) -> None:
        """운영 중단 상태에서 image_metadata를 명시적으로 재구성한다."""
        ddl = self._load_schema_ddl(include_image_labels=False)
        with self.connect() as conn:
            conn.execute(ddl)
            self._rebuild_image_metadata_table(conn)
            self._ensure_image_metadata_indexes(conn, cleanup_legacy=True)
            self._ensure_image_labels_table(conn, restore_backup=True)

    # ── Composite helpers ──

    @classmethod
    def _ensure_operational_schema(
        cls,
        conn: duckdb.DuckDBPyConnection,
        *,
        backfill: bool,
        restore_image_labels_backup: bool,
        ensure_indexes: bool,
    ) -> None:
        cls._ensure_runtime_schema_columns(
            conn,
            backfill=backfill,
            restore_image_labels_backup=restore_image_labels_backup,
        )
        cls._ensure_dispatch_support_schema(conn)
        if ensure_indexes:
            cls._ensure_image_metadata_indexes(conn, cleanup_legacy=True)

    @classmethod
    def _ensure_runtime_schema_columns(
        cls,
        conn: duckdb.DuckDBPyConnection,
        *,
        backfill: bool,
        restore_image_labels_backup: bool,
    ) -> None:
        cls._ensure_image_metadata_columns(conn, backfill=backfill)
        cls._ensure_video_metadata_frame_columns(conn, backfill=backfill)
        cls._ensure_video_metadata_reencode_columns(conn)
        cls._ensure_labels_columns(conn, backfill=backfill)
        cls._ensure_processed_clips_columns(conn, backfill=backfill)
        cls._ensure_image_labels_table(conn, restore_backup=restore_image_labels_backup)

    @classmethod
    def _ensure_dispatch_support_schema(cls, conn: duckdb.DuckDBPyConnection) -> None:
        cls._ensure_staging_dispatch_columns(conn)
        cls._ensure_staging_model_configs(conn)
        cls._ensure_staging_pipeline_runs(conn)
        cls._ensure_raw_files_spec_columns(conn)
        cls._ensure_video_metadata_stage_columns(conn)

    # ── image_metadata ──

    @classmethod
    def _ensure_image_metadata_columns(
        cls,
        conn: duckdb.DuckDBPyConnection,
        *,
        backfill: bool,
    ) -> None:
        columns = cls._table_columns(conn, "image_metadata")
        if not columns:
            return
        alter_specs = {
            "image_id": "VARCHAR",
            "source_asset_id": "VARCHAR",
            "source_clip_id": "VARCHAR",
            "image_bucket": "VARCHAR DEFAULT 'vlm-raw'",
            "image_key": "VARCHAR",
            "image_role": "VARCHAR DEFAULT 'source_image'",
            "frame_index": "INTEGER",
            "frame_sec": "DOUBLE",
            "checksum": "VARCHAR",
            "file_size": "BIGINT",
            "width": "INTEGER",
            "height": "INTEGER",
            "color_mode": "VARCHAR DEFAULT 'RGB'",
            "bit_depth": "INTEGER DEFAULT 8",
            "has_alpha": "BOOLEAN DEFAULT FALSE",
            "orientation": "INTEGER DEFAULT 1",
            "image_caption_text": "TEXT",
            "image_caption_score": "DOUBLE",
            "image_caption_bucket": "VARCHAR",
            "image_caption_key": "VARCHAR",
            "image_caption_generated_at": "TIMESTAMP",
            "extracted_at": "TIMESTAMP",
        }
        for column_name, column_type in alter_specs.items():
            if column_name in columns:
                continue
            conn.execute(f"ALTER TABLE image_metadata ADD COLUMN {column_name} {column_type}")

        if not backfill:
            return

        columns = cls._table_columns(conn, "image_metadata")
        image_id_sources = [name for name in ("image_id", "source_asset_id", "asset_id") if name in columns]
        source_asset_sources = [name for name in ("source_asset_id", "asset_id", "image_id") if name in columns]
        image_id_expr = "COALESCE(" + ", ".join(image_id_sources) + ")"
        source_asset_expr = "COALESCE(" + ", ".join(source_asset_sources) + ")"

        conn.execute(
            f"""
            UPDATE image_metadata
            SET image_id = {image_id_expr},
                source_asset_id = {source_asset_expr}
            WHERE image_id IS NULL
               OR source_asset_id IS NULL
            """
        )
        conn.execute(
            """
            UPDATE image_metadata AS im
            SET image_bucket = COALESCE(NULLIF(im.image_bucket, ''), NULLIF(rf.raw_bucket, ''), 'vlm-raw'),
                image_key = COALESCE(im.image_key, rf.raw_key),
                checksum = COALESCE(im.checksum, rf.checksum),
                file_size = COALESCE(im.file_size, rf.file_size)
            FROM raw_files AS rf
            WHERE rf.asset_id = im.source_asset_id
              AND (
                  im.image_bucket IS NULL
                  OR im.image_bucket = ''
                  OR im.image_key IS NULL
                  OR im.checksum IS NULL
                  OR im.file_size IS NULL
              )
            """
        )
        conn.execute(
            """
            UPDATE image_metadata
            SET image_bucket = COALESCE(NULLIF(image_bucket, ''), 'vlm-raw'),
                image_role = COALESCE(NULLIF(image_role, ''), 'source_image'),
                color_mode = COALESCE(color_mode, 'RGB'),
                bit_depth = COALESCE(bit_depth, 8),
                has_alpha = COALESCE(has_alpha, FALSE),
                orientation = COALESCE(orientation, 1),
                extracted_at = COALESCE(extracted_at, CURRENT_TIMESTAMP)
            WHERE image_bucket IS NULL
               OR image_bucket = ''
               OR image_role IS NULL
               OR image_role = ''
               OR color_mode IS NULL
               OR bit_depth IS NULL
               OR has_alpha IS NULL
               OR orientation IS NULL
               OR extracted_at IS NULL
            """
        )
        if {"image_caption_bucket", "image_caption_key"}.issubset(columns):
            conn.execute(
                """
                UPDATE image_metadata
                SET image_caption_bucket = COALESCE(NULLIF(image_caption_bucket, ''), 'vlm-labels')
                WHERE image_caption_key IS NOT NULL
                  AND image_caption_key <> ''
                  AND (image_caption_bucket IS NULL OR image_caption_bucket = '')
                """
            )
        if {"image_caption_generated_at", "image_caption_key", "extracted_at"}.issubset(columns):
            conn.execute(
                """
                UPDATE image_metadata
                SET image_caption_generated_at = COALESCE(image_caption_generated_at, extracted_at, CURRENT_TIMESTAMP)
                WHERE image_caption_key IS NOT NULL
                  AND image_caption_key <> ''
                  AND image_caption_generated_at IS NULL
                """
            )

    @classmethod
    def _create_image_metadata_table(cls, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        conn.execute(
            f"""
            CREATE TABLE {table_name} (
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
                image_caption_text TEXT,
                image_caption_score DOUBLE,
                image_caption_bucket VARCHAR,
                image_caption_key VARCHAR,
                image_caption_generated_at TIMESTAMP,
                extracted_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (image_bucket, image_key),
                UNIQUE (source_asset_id, source_clip_id, image_role, frame_index)
            )
            """
        )

    @classmethod
    def _image_metadata_col(
        cls,
        columns: set[str],
        name: str,
        *,
        alias: str = "im",
        fallback_sql: str = "NULL",
    ) -> str:
        if name not in columns:
            return fallback_sql
        return f"{alias}.{name}"

    @classmethod
    def _image_metadata_copy_select_sql(
        cls,
        columns: set[str],
        *,
        source_table: str,
        alias: str = "im",
    ) -> str:
        image_id_sources = [name for name in ("image_id", "source_asset_id", "asset_id") if name in columns]
        source_asset_sources = [name for name in ("source_asset_id", "asset_id", "image_id") if name in columns]
        if not image_id_sources or not source_asset_sources:
            raise RuntimeError(
                f"image_metadata repair failed: usable key columns not found in {source_table}"
            )

        image_id_expr = "COALESCE(" + ", ".join(f"{alias}.{name}" for name in image_id_sources) + ")"
        source_asset_expr = "COALESCE(" + ", ".join(f"{alias}.{name}" for name in source_asset_sources) + ")"
        return f"""
            SELECT
                {image_id_expr} AS image_id,
                {source_asset_expr} AS source_asset_id,
                {cls._image_metadata_col(columns, "source_clip_id", alias=alias)} AS source_clip_id,
                COALESCE(NULLIF({cls._image_metadata_col(columns, "image_bucket", alias=alias)}, ''), NULLIF(rf.raw_bucket, ''), 'vlm-raw') AS image_bucket,
                COALESCE({cls._image_metadata_col(columns, "image_key", alias=alias)}, rf.raw_key) AS image_key,
                COALESCE(NULLIF({cls._image_metadata_col(columns, "image_role", alias=alias)}, ''), 'source_image') AS image_role,
                {cls._image_metadata_col(columns, "frame_index", alias=alias)} AS frame_index,
                {cls._image_metadata_col(columns, "frame_sec", alias=alias)} AS frame_sec,
                COALESCE({cls._image_metadata_col(columns, "checksum", alias=alias)}, rf.checksum) AS checksum,
                COALESCE({cls._image_metadata_col(columns, "file_size", alias=alias)}, rf.file_size) AS file_size,
                {cls._image_metadata_col(columns, "width", alias=alias)} AS width,
                {cls._image_metadata_col(columns, "height", alias=alias)} AS height,
                COALESCE({cls._image_metadata_col(columns, "color_mode", alias=alias, fallback_sql="'RGB'")}, 'RGB') AS color_mode,
                COALESCE({cls._image_metadata_col(columns, "bit_depth", alias=alias, fallback_sql="8")}, 8) AS bit_depth,
                COALESCE({cls._image_metadata_col(columns, "has_alpha", alias=alias, fallback_sql="FALSE")}, FALSE) AS has_alpha,
                COALESCE({cls._image_metadata_col(columns, "orientation", alias=alias, fallback_sql="1")}, 1) AS orientation,
                COALESCE(
                    {cls._image_metadata_col(columns, "image_caption_text", alias=alias)},
                    {cls._image_metadata_col(columns, "caption_text", alias=alias)}
                ) AS image_caption_text,
                {cls._image_metadata_col(columns, "image_caption_score", alias=alias)} AS image_caption_score,
                CASE
                    WHEN {cls._image_metadata_col(columns, "image_caption_key", alias=alias)} IS NOT NULL
                     AND {cls._image_metadata_col(columns, "image_caption_key", alias=alias)} <> ''
                    THEN COALESCE(NULLIF({cls._image_metadata_col(columns, "image_caption_bucket", alias=alias)}, ''), 'vlm-labels')
                    ELSE NULL
                END AS image_caption_bucket,
                {cls._image_metadata_col(columns, "image_caption_key", alias=alias)} AS image_caption_key,
                CASE
                    WHEN {cls._image_metadata_col(columns, "image_caption_key", alias=alias)} IS NOT NULL
                     AND {cls._image_metadata_col(columns, "image_caption_key", alias=alias)} <> ''
                    THEN COALESCE(
                        {cls._image_metadata_col(columns, "image_caption_generated_at", alias=alias)},
                        {cls._image_metadata_col(columns, "extracted_at", alias=alias, fallback_sql="CURRENT_TIMESTAMP")},
                        CURRENT_TIMESTAMP
                    )
                    ELSE {cls._image_metadata_col(columns, "image_caption_generated_at", alias=alias)}
                END AS image_caption_generated_at,
                COALESCE({cls._image_metadata_col(columns, "extracted_at", alias=alias, fallback_sql="CURRENT_TIMESTAMP")}, CURRENT_TIMESTAMP) AS extracted_at
            FROM {source_table} AS {alias}
            LEFT JOIN raw_files AS rf ON rf.asset_id = {source_asset_expr}
        """

    @classmethod
    def _select_image_metadata_source_table(cls, conn: duckdb.DuckDBPyConnection) -> str | None:
        unreadable: list[str] = []
        for table_name in ("image_metadata", "image_metadata__migrated"):
            if not cls._table_exists(conn, table_name):
                continue
            try:
                conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchall()
            except Exception:  # noqa: BLE001
                unreadable.append(table_name)
                continue
            return table_name
        if unreadable:
            joined = ", ".join(unreadable)
            raise RuntimeError(f"image_metadata repair failed: unreadable source table(s): {joined}")
        return None

    @classmethod
    def _rebuild_image_metadata_table(cls, conn: duckdb.DuckDBPyConnection) -> None:
        source_table = cls._select_image_metadata_source_table(conn)
        if source_table is None:
            if not cls._table_exists(conn, "image_metadata"):
                cls._create_image_metadata_table(conn, "image_metadata")
            return

        columns = cls._table_columns(conn, source_table)
        conn.execute("DROP TABLE IF EXISTS image_metadata__repair")
        cls._create_image_metadata_table(conn, "image_metadata__repair")
        select_sql = cls._image_metadata_copy_select_sql(columns, source_table=source_table)
        conn.execute(
            f"""
            INSERT INTO image_metadata__repair (
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
                image_caption_text,
                image_caption_score,
                image_caption_bucket,
                image_caption_key,
                image_caption_generated_at,
                extracted_at
            )
            {select_sql}
            """
        )

        if cls._table_exists(conn, "image_labels"):
            conn.execute("DROP TABLE IF EXISTS image_labels__backup")
            conn.execute("CREATE TABLE image_labels__backup AS SELECT * FROM image_labels")
            conn.execute("DROP TABLE image_labels")

        if cls._table_exists(conn, "image_metadata"):
            conn.execute("DROP TABLE image_metadata")
        if source_table != "image_metadata__migrated" and cls._table_exists(conn, "image_metadata__migrated"):
            conn.execute("DROP TABLE image_metadata__migrated")

        cls._create_image_metadata_table(conn, "image_metadata")
        conn.execute(
            """
            INSERT INTO image_metadata
            SELECT * FROM image_metadata__repair
            """
        )
        conn.execute("DROP TABLE image_metadata__repair")
        if cls._table_exists(conn, "image_metadata__migrated"):
            conn.execute("DROP TABLE image_metadata__migrated")

    @classmethod
    def _ensure_image_metadata_indexes(
        cls,
        conn: duckdb.DuckDBPyConnection,
        *,
        cleanup_legacy: bool,
    ) -> None:
        columns = cls._table_columns(conn, "image_metadata")
        if not {"image_id", "source_asset_id", "source_clip_id"}.issubset(columns):
            return
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_image_metadata_bucket_key
            ON image_metadata(image_bucket, image_key)
            """
        )
        if cleanup_legacy:
            conn.execute("DROP INDEX IF EXISTS idx_image_metadata_source_role_frame")
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_image_metadata_source_clip_role_frame
            ON image_metadata(source_asset_id, source_clip_id, image_role, frame_index)
            """
        )

    # ── video_metadata ──

    @classmethod
    def _ensure_video_metadata_frame_columns(
        cls,
        conn: duckdb.DuckDBPyConnection,
        *,
        backfill: bool,
    ) -> None:
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

        if not backfill:
            return

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
    def _ensure_video_metadata_reencode_columns(cls, conn: duckdb.DuckDBPyConnection) -> None:
        """video_metadata에 재인코딩 관련 컬럼이 없으면 추가.

        원본 인코딩 정보(original_*) + 재인코딩 판정/적용 결과(reencode_*) 컬럼.
        """
        columns = cls._table_columns(conn, "video_metadata")
        if not columns:
            return
        alter_specs = {
            "original_codec":        "VARCHAR",
            "original_profile":      "VARCHAR",
            "original_has_b_frames": "BOOLEAN",
            "original_level_int":    "INTEGER",
            "reencode_required":     "BOOLEAN DEFAULT FALSE",
            "reencode_reason":       "VARCHAR",
            "reencode_applied":      "BOOLEAN DEFAULT FALSE",
            "reencode_preset":       "VARCHAR",
        }
        for column_name, column_type in alter_specs.items():
            if column_name in columns:
                continue
            conn.execute(f"ALTER TABLE video_metadata ADD COLUMN {column_name} {column_type}")

    @classmethod
    def _ensure_video_metadata_stage_columns(cls, conn: duckdb.DuckDBPyConnection) -> None:
        """video_metadata에 stage status 컬럼(timestamp/caption/frame/bbox)이 없으면 추가."""
        columns = cls._table_columns(conn, "video_metadata")
        if not columns:
            return
        alter_specs = {
            "timestamp_status": "VARCHAR DEFAULT 'pending'",
            "timestamp_error": "TEXT",
            "timestamp_label_key": "VARCHAR",
            "timestamp_completed_at": "TIMESTAMP",
            "caption_status": "VARCHAR DEFAULT 'pending'",
            "caption_error": "TEXT",
            "caption_completed_at": "TIMESTAMP",
            "frame_status": "VARCHAR DEFAULT 'pending'",
            "frame_error": "TEXT",
            "frame_completed_at": "TIMESTAMP",
            "bbox_status": "VARCHAR DEFAULT 'pending'",
            "bbox_error": "TEXT",
            "bbox_completed_at": "TIMESTAMP",
        }
        for column_name, column_type in alter_specs.items():
            if column_name in columns:
                continue
            conn.execute(f"ALTER TABLE video_metadata ADD COLUMN {column_name} {column_type}")

    # ── labels ──

    @classmethod
    def _ensure_labels_columns(
        cls,
        conn: duckdb.DuckDBPyConnection,
        *,
        backfill: bool,
    ) -> None:
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

        if not backfill:
            return

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

    # ── processed_clips ──

    @classmethod
    def _ensure_processed_clips_columns(
        cls,
        conn: duckdb.DuckDBPyConnection,
        *,
        backfill: bool,
    ) -> None:
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

        if not backfill:
            return

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

    # ── image_labels ──

    @classmethod
    def _ensure_image_labels_table(
        cls,
        conn: duckdb.DuckDBPyConnection,
        *,
        restore_backup: bool,
    ) -> None:
        if not cls._table_exists(conn, "image_labels"):
            image_ref_sql = "VARCHAR"
            if cls._image_metadata_has_image_id_constraint(conn):
                image_ref_sql = "VARCHAR REFERENCES image_metadata(image_id)"
            conn.execute(
                f"""
                CREATE TABLE image_labels (
                    image_label_id   VARCHAR PRIMARY KEY,
                    image_id         {image_ref_sql},
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
        if restore_backup and cls._table_exists(conn, "image_labels__backup"):
            conn.execute(
                """
                INSERT INTO image_labels
                SELECT * FROM image_labels__backup
                """
            )
            conn.execute("DROP TABLE image_labels__backup")

    @classmethod
    def _image_metadata_has_image_id_constraint(cls, conn: duckdb.DuckDBPyConnection) -> bool:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM duckdb_constraints()
            WHERE table_name = 'image_metadata'
              AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')
              AND array_length(constraint_column_names) = 1
              AND constraint_column_names[1] = 'image_id'
            """
        ).fetchone()
        return bool(row and row[0] > 0)

    # ── Staging 전용 테이블 보장 ──

    @classmethod
    def _ensure_staging_dispatch_columns(cls, conn: duckdb.DuckDBPyConnection) -> None:
        """staging_dispatch_requests에 YOLO 파라미터 컬럼이 없으면 추가."""
        columns = cls._table_columns(conn, "staging_dispatch_requests")
        if not columns:
            return

        alter_specs = {
            "labeling_method": "VARCHAR",
            "categories": "VARCHAR",
            "classes": "VARCHAR",
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

    # ── Staging spec flow (raw_files / video_metadata) ──

    @classmethod
    def _ensure_raw_files_spec_columns(cls, conn: duckdb.DuckDBPyConnection) -> None:
        """raw_files에 spec_id, source_unit_name 컬럼이 없으면 추가."""
        columns = cls._table_columns(conn, "raw_files")
        if not columns:
            return
        for column_name, column_type in (
            ("spec_id", "VARCHAR"),
            ("source_unit_name", "VARCHAR"),
        ):
            if column_name in columns:
                continue
            conn.execute(f"ALTER TABLE raw_files ADD COLUMN {column_name} {column_type}")
