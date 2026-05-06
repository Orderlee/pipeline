#!/usr/bin/env python3
from __future__ import annotations

"""Copy VLM pipeline data from DuckDB into PostgreSQL.

Examples:
    python3 scripts/migrate_duckdb_to_postgres.py \
        --source /data/pipeline.duckdb \
        --dsn "postgresql://user:password@localhost:5432/vlm"

    python3 scripts/migrate_duckdb_to_postgres.py \
        --source /data/staging.duckdb \
        --dsn "host=localhost port=5432 dbname=vlm user=vlm password=secret" \
        --apply --idempotent

    python3 scripts/migrate_duckdb_to_postgres.py \
        --source /data/pipeline.duckdb \
        --dsn "postgresql://user:password@localhost:5432/vlm" \
        --verify-only
"""

import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import duckdb


@dataclass(frozen=True)
class TableSpec:
    name: str
    pk_columns: tuple[str, ...]


@dataclass(frozen=True)
class FkRelationship:
    child_table: str
    child_column: str
    parent_table: str
    parent_column: str


TABLES: tuple[TableSpec, ...] = (
    TableSpec("raw_files", ("asset_id",)),
    TableSpec("video_metadata", ("asset_id",)),
    TableSpec("labels", ("label_id",)),
    TableSpec("processed_clips", ("clip_id",)),
    TableSpec("image_metadata", ("image_id",)),
    TableSpec("image_labels", ("image_label_id",)),
    TableSpec("datasets", ("dataset_id",)),
    TableSpec("dataset_clips", ("dataset_id", "clip_id")),
    TableSpec("dispatch_requests", ("request_id",)),
    TableSpec("staging_model_configs", ("config_id",)),
    TableSpec("dispatch_pipeline_runs", ("run_id",)),
    TableSpec("labeling_specs", ("spec_id",)),
    TableSpec("labeling_configs", ("config_id",)),
    TableSpec("requester_config_map", ("map_id",)),
    TableSpec("classification_datasets", ("dataset_id",)),
)

FK_RELATIONSHIPS: tuple[FkRelationship, ...] = (
    FkRelationship("video_metadata", "asset_id", "raw_files", "asset_id"),
    FkRelationship("labels", "asset_id", "raw_files", "asset_id"),
    FkRelationship("processed_clips", "source_asset_id", "raw_files", "asset_id"),
    FkRelationship("processed_clips", "source_label_id", "labels", "label_id"),
    FkRelationship("image_metadata", "source_asset_id", "raw_files", "asset_id"),
    FkRelationship("image_metadata", "source_clip_id", "processed_clips", "clip_id"),
    FkRelationship("image_labels", "image_id", "image_metadata", "image_id"),
    FkRelationship("image_labels", "source_clip_id", "processed_clips", "clip_id"),
    FkRelationship("dataset_clips", "dataset_id", "datasets", "dataset_id"),
    FkRelationship("dataset_clips", "clip_id", "processed_clips", "clip_id"),
)

CHECKSUM_TABLES: tuple[str, ...] = (
    "raw_files",
    "image_metadata",
    "processed_clips",
    "labels",
    "dispatch_pipeline_runs",
)

TABLE_BY_NAME: dict[str, TableSpec] = {table.name: table for table in TABLES}


def connect_with_attach_source(source_path: str) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB engine + source ATTACH as READ_ONLY.

    DuckDB 가 read-only 모드로 열리면 ATTACH 한 PG 에도 read-only 가 전파되어
    INSERT 가 거부된다. 우회: in-memory engine 으로 열어서 PG 는 read-write 로
    ATTACH 하고, source 만 ``READ_ONLY`` 로 ATTACH 한다 — 동시 read 안전 + PG write 가능.

    또한 source 에 lock 이 잡혀 있을 때 (다른 dagster 가 write 중) DuckDB 표준 retry 적용.
    """
    retry_count = max(0, int(os.getenv("DUCKDB_LOCK_RETRY_COUNT", "20")))
    delay_ms = max(50, int(os.getenv("DUCKDB_LOCK_RETRY_DELAY_MS", "100")))
    last_exc: Exception | None = None
    for attempt in range(retry_count + 1):
        try:
            con = duckdb.connect(":memory:")
            con.execute("PRAGMA disable_progress_bar")
            con.execute(f"ATTACH {_sql_literal(source_path)} AS src (READ_ONLY)")
            return con
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= retry_count:
                break
            time.sleep(delay_ms / 1000.0)
    if last_exc is None:
        raise RuntimeError("DuckDB connection failed unexpectedly")
    raise last_exc


def _count_rows(con: duckdb.DuckDBPyConnection, relation_sql: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {relation_sql}").fetchone()[0])


def _local_table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """source ATTACH catalog='src', schema='main'. DuckDB 의 ``information_schema.tables``
    는 모든 attached catalog 를 합쳐 보여주므로 ``table_catalog`` 로 필터.
    """
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_catalog = 'src'
          AND table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def _pg_table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """PG ATTACH catalog='pg', schema='public'. ``table_catalog`` 로 필터."""
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_catalog = 'pg'
          AND table_schema = 'public'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Copy all VLM pipeline tables from a DuckDB source into an existing PostgreSQL schema. "
            "The default mode is dry-run."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 scripts/migrate_duckdb_to_postgres.py \\
      --source /data/pipeline.duckdb \\
      --dsn "postgresql://user:password@localhost:5432/vlm"

  python3 scripts/migrate_duckdb_to_postgres.py \\
      --source /data/staging.duckdb \\
      --dsn "host=localhost port=5432 dbname=vlm user=vlm password=secret" \\
      --apply --idempotent

  python3 scripts/migrate_duckdb_to_postgres.py \\
      --source /data/pipeline.duckdb \\
      --dsn "postgresql://user:password@localhost:5432/vlm" \\
      --verify-only
""",
    )
    parser.add_argument("--source", required=True, help="Path to pipeline.duckdb or staging.duckdb.")
    parser.add_argument("--dsn", required=True, help="PostgreSQL DSN accepted by DuckDB's postgres extension.")

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Insert rows into PostgreSQL.")
    mode.add_argument("--dry-run", action="store_true", help="Only count rows and show the copy plan. This is default.")
    mode.add_argument("--verify-only", action="store_true", help="Only compare source and target counts/checksums.")

    parser.add_argument(
        "--idempotent",
        action="store_true",
        help="When applying, skip a table if source and target row counts already match.",
    )
    return parser.parse_args(argv)


def _selected_mode(args: argparse.Namespace) -> str:
    if args.verify_only:
        return "verify-only"
    if args.apply:
        return "apply"
    return "dry-run"


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _source_relation(table_name: str) -> str:
    """source ATTACH name='src', schema='main'."""
    return f'{_quote_ident("src")}.{_quote_ident("main")}.{_quote_ident(table_name)}'


def _pg_relation(table_name: str) -> str:
    return f'{_quote_ident("pg")}.{_quote_ident("public")}.{_quote_ident(table_name)}'


def _format_count(value: int | None) -> str:
    if value is None:
        return "-"
    return f"{value:,}"


def _format_checksum(value: str | None) -> str:
    return value if value else "<empty>"


def _sanitize_message(message: str, sensitive_value: str) -> str:
    if not sensitive_value:
        return message
    return message.replace(sensitive_value, "<dsn>")


def _install_and_load_postgres(con: duckdb.DuckDBPyConnection) -> None:
    print("[INFO] Preparing DuckDB postgres extension")
    install_exc: Exception | None = None

    try:
        con.execute("INSTALL postgres")
    except Exception as exc:  # noqa: BLE001
        install_exc = exc

    try:
        con.execute("LOAD postgres")
    except Exception as load_exc:  # noqa: BLE001
        if install_exc is not None:
            raise RuntimeError(
                "Failed to INSTALL/LOAD DuckDB postgres extension. "
                f"INSTALL error: {install_exc}. LOAD error: {load_exc}"
            ) from load_exc
        raise RuntimeError(f"Failed to LOAD DuckDB postgres extension: {load_exc}") from load_exc

    if install_exc is not None:
        print("[INFO] DuckDB postgres extension loaded from an existing installation")
    print("[DONE] DuckDB postgres extension ready")


def _attach_postgres(con: duckdb.DuckDBPyConnection, dsn: str) -> None:
    print("[INFO] Attaching PostgreSQL target as pg")
    try:
        con.execute(f"ATTACH {_sql_literal(dsn)} AS pg (TYPE POSTGRES)")
    except Exception as exc:  # noqa: BLE001
        message = _sanitize_message(str(exc), dsn)
        raise RuntimeError(f"Failed to attach PostgreSQL target through DuckDB: {message}") from exc
    print("[DONE] PostgreSQL target attached")


def _validate_source_tables(con: duckdb.DuckDBPyConnection) -> None:
    missing = [table.name for table in TABLES if not _local_table_exists(con, table.name)]
    if missing:
        missing_csv = ", ".join(missing)
        raise RuntimeError(f"DuckDB source is missing required tables in main schema: {missing_csv}")
    print(f"[DONE] DuckDB source contains all {len(TABLES)} required tables")


def _validate_pg_tables(con: duckdb.DuckDBPyConnection) -> None:
    missing = [table.name for table in TABLES if not _pg_table_exists(con, table.name)]
    if missing:
        missing_csv = ", ".join(missing)
        raise RuntimeError(
            "PostgreSQL target is missing required public tables: "
            f"{missing_csv}. Run the project schema bootstrap before migration."
        )
    print(f"[DONE] PostgreSQL target contains all {len(TABLES)} required tables")


def _fk_label(fk: FkRelationship) -> str:
    return f"{fk.child_table}.{fk.child_column} -> {fk.parent_table}.{fk.parent_column}"


def _orphan_count(con: duckdb.DuckDBPyConnection, fk: FkRelationship) -> int:
    child_relation = _source_relation(fk.child_table)
    parent_relation = _source_relation(fk.parent_table)
    child_column = _quote_ident(fk.child_column)
    parent_column = _quote_ident(fk.parent_column)
    sql = f"""
        SELECT COUNT(*)
        FROM {child_relation} c
        WHERE c.{child_column} IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM {parent_relation} p
              WHERE p.{parent_column} = c.{child_column}
          )
    """
    return int(con.execute(sql).fetchone()[0])


def _run_orphan_scan(con: duckdb.DuckDBPyConnection) -> bool:
    print("[INFO] Pre-flight orphan scan")
    print(f"[INFO] {'fk':<75} {'orphan_rows':>12}")
    print(f"[INFO] {'-' * 88}")

    failed = False
    for fk in FK_RELATIONSHIPS:
        count = _orphan_count(con, fk)
        prefix = "[DONE]" if count == 0 else "[ERROR]"
        print(f"{prefix} {_fk_label(fk):<75} {_format_count(count):>12}")
        failed = failed or count != 0

    if failed:
        print("[ERROR] Pre-flight orphan scan failed; refusing to continue.")
        return False

    print("[DONE] Pre-flight orphan scan passed")
    return True


def _insert_sql(table: TableSpec) -> str:
    conflict_columns = ", ".join(_quote_ident(column) for column in table.pk_columns)
    return f"""
        INSERT INTO {_pg_relation(table.name)}
        SELECT *
        FROM {_source_relation(table.name)}
        ON CONFLICT ({conflict_columns}) DO NOTHING
    """


def _print_copy_header() -> None:
    print(f"[INFO] {'table':<30} {'source_rows':>12} {'target_before':>14} {'target_after':>13}   action")
    print(f"[INFO] {'-' * 88}")


def _print_copy_row(
    prefix: str,
    table_name: str,
    source_count: int,
    target_before: int,
    target_after: int | None,
    action: str,
) -> None:
    print(
        f"{prefix} {table_name:<30} {_format_count(source_count):>12} "
        f"{_format_count(target_before):>14} {_format_count(target_after):>13}   {action}"
    )


def _copy_tables(con: duckdb.DuckDBPyConnection, *, apply_changes: bool, idempotent: bool) -> bool:
    mode_label = "APPLY" if apply_changes else "DRY-RUN"
    if idempotent:
        mode_label = f"{mode_label} idempotent"

    print(f"[INFO] Copy mode: {mode_label}")
    print("[PROGRESS] Counting and processing tables in FK order")
    _print_copy_header()

    total_source = 0
    total_target_before = 0
    total_target_after = 0
    skipped = 0
    inserted = 0

    for table in TABLES:
        source_count = _count_rows(con, _source_relation(table.name))
        target_before = _count_rows(con, _pg_relation(table.name))
        total_source += source_count
        total_target_before += target_before

        if idempotent and source_count == target_before:
            skipped += 1
            total_target_after += target_before
            _print_copy_row("[SKIP]", table.name, source_count, target_before, target_before, "row counts match")
            continue

        if not apply_changes:
            total_target_after += target_before
            _print_copy_row("[DRY-RUN]", table.name, source_count, target_before, None, "would insert")
            continue

        con.execute(_insert_sql(table))
        target_after = _count_rows(con, _pg_relation(table.name))
        inserted_for_table = max(0, target_after - target_before)
        inserted += inserted_for_table
        total_target_after += target_after
        _print_copy_row(
            "[DONE]",
            table.name,
            source_count,
            target_before,
            target_after,
            f"inserted {inserted_for_table:,}",
        )

    print(
        f"[SUMMARY] tables={len(TABLES)} skipped={skipped} source_rows={total_source:,} "
        f"target_before={total_target_before:,} target_after={total_target_after:,} inserted={inserted:,}"
    )

    if apply_changes:
        print("[DONE] Copy pass completed")
    else:
        print("[DONE] Dry-run completed; no rows were inserted")
    return True


def _sampled_pk_checksum(con: duckdb.DuckDBPyConnection, relation_sql: str, pk_column: str) -> str | None:
    pk = _quote_ident(pk_column)
    sql = f"""
        SELECT md5(string_agg(CAST({pk} AS VARCHAR), ',' ORDER BY {pk}))
        FROM (
            SELECT {pk}
            FROM {relation_sql}
            ORDER BY {pk}
            LIMIT 1000
        ) sub
    """
    return con.execute(sql).fetchone()[0]


def _print_verify_count_header() -> None:
    print(f"[INFO] {'table':<30} {'source_rows':>12} {'target_rows':>12}   status")
    print(f"[INFO] {'-' * 72}")


def _print_verify_count_row(table_name: str, source_count: int, target_count: int) -> bool:
    matches = source_count == target_count
    prefix = "[DONE]" if matches else "[ERROR]"
    status = "match" if matches else "mismatch"
    print(f"{prefix} {table_name:<30} {_format_count(source_count):>12} {_format_count(target_count):>12}   {status}")
    return matches


def _print_verify_checksum_header() -> None:
    print(f"[INFO] {'table':<30} {'source_checksum':<32} {'target_checksum':<32} status")
    print(f"[INFO] {'-' * 103}")


def _print_verify_checksum_row(table_name: str, source_checksum: str | None, target_checksum: str | None) -> bool:
    matches = source_checksum == target_checksum
    prefix = "[DONE]" if matches else "[ERROR]"
    status = "match" if matches else "mismatch"
    print(
        f"{prefix} {table_name:<30} {_format_checksum(source_checksum):<32} "
        f"{_format_checksum(target_checksum):<32} {status}"
    )
    return matches


def _verify_only(con: duckdb.DuckDBPyConnection) -> bool:
    print("[INFO] Verify-only mode: comparing row counts")
    _print_verify_count_header()

    ok = True
    for table in TABLES:
        source_count = _count_rows(con, _source_relation(table.name))
        target_count = _count_rows(con, _pg_relation(table.name))
        ok = _print_verify_count_row(table.name, source_count, target_count) and ok

    print("[INFO] Verify-only mode: comparing sampled primary-key checksums")
    _print_verify_checksum_header()

    for table_name in CHECKSUM_TABLES:
        table = TABLE_BY_NAME[table_name]
        pk_column = table.pk_columns[0]
        source_checksum = _sampled_pk_checksum(con, _source_relation(table.name), pk_column)
        target_checksum = _sampled_pk_checksum(con, _pg_relation(table.name), pk_column)
        ok = _print_verify_checksum_row(table.name, source_checksum, target_checksum) and ok

    if ok:
        print("[DONE] Verify-only checks passed")
    else:
        print("[ERROR] Verify-only checks found mismatches")
    return ok


def main() -> int:
    try:
        args = parse_args()
        mode = _selected_mode(args)
        source_path = Path(args.source).expanduser()

        if not source_path.exists():
            raise FileNotFoundError(f"DuckDB source does not exist: {source_path}")
        if not args.dsn.strip():
            raise ValueError("PostgreSQL DSN must not be empty")

        print("[INFO] DuckDB -> PostgreSQL migration")
        print(f"[INFO] Source: {source_path}")
        print(f"[INFO] Mode: {mode}")
        print("[INFO] Connecting to DuckDB engine and attaching source as READ_ONLY")

        con = connect_with_attach_source(str(source_path))
        try:
            _validate_source_tables(con)
            _install_and_load_postgres(con)
            _attach_postgres(con, args.dsn)
            _validate_pg_tables(con)

            if not _run_orphan_scan(con):
                return 1

            if mode == "verify-only":
                return 0 if _verify_only(con) else 1

            apply_changes = mode == "apply"
            return 0 if _copy_tables(con, apply_changes=apply_changes, idempotent=args.idempotent) else 1
        finally:
            con.close()

    except KeyboardInterrupt:
        print("[ERROR] Interrupted", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
