#!/usr/bin/env python3
"""Inspect or repair image_metadata schema state in a local DuckDB file.

Examples:
  python3 scripts/repair_image_metadata_schema.py --db-path /data/pipeline.duckdb
  python3 scripts/repair_image_metadata_schema.py --db-path /data/pipeline.duckdb --repair --yes
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from vlm_pipeline.resources.duckdb_base import DuckDBBaseMixin


class _RepairDuckDBResource(DuckDBBaseMixin):
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path


TABLES_TO_CHECK = (
    "image_metadata",
    "image_metadata__migrated",
    "image_labels",
    "image_labels__backup",
    "processed_clips",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect image_metadata/image_labels schema state and optionally repair it. "
            "Run only while Dagster DuckDB writer jobs are stopped."
        )
    )
    parser.add_argument(
        "--db-path",
        default="/data/pipeline.duckdb",
        help="Target DuckDB path. Defaults to /data/pipeline.duckdb.",
    )
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Rebuild image_metadata and recreate image_labels from backup/source state.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Acknowledge that the DB is backed up and writer jobs are stopped.",
    )
    return parser.parse_args()


def _table_exists(resource: _RepairDuckDBResource, table_name: str) -> bool:
    with resource.connect() as conn:
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


def _print_state(resource: _RepairDuckDBResource) -> None:
    print(f"\n[DB] {resource.db_path}")
    with resource.connect() as conn:
        rows = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name IN (?, ?, ?, ?, ?)
            ORDER BY table_name
            """,
            list(TABLES_TO_CHECK),
        ).fetchall()
        existing = {str(row[0]) for row in rows}
        print("[tables]")
        for table_name in TABLES_TO_CHECK:
            print(f"  - {table_name}: {'present' if table_name in existing else 'missing'}")

        for table_name in TABLES_TO_CHECK:
            if table_name not in existing:
                continue
            print(f"\n[{table_name}]")
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"  rows={int(count)}")
            except Exception as exc:  # noqa: BLE001
                print(f"  rows=<unreadable: {exc}>")

            try:
                columns = conn.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'main'
                      AND table_name = ?
                    ORDER BY ordinal_position
                    """,
                    [table_name],
                ).fetchall()
                print("  columns=" + ", ".join(str(row[0]) for row in columns))
            except Exception as exc:  # noqa: BLE001
                print(f"  columns=<unreadable: {exc}>")

        print("\n[constraints]")
        constraints = conn.execute(
            """
            SELECT table_name, constraint_type, constraint_name, constraint_column_names
            FROM duckdb_constraints()
            WHERE table_name IN ('image_metadata', 'image_labels')
            ORDER BY table_name, constraint_index
            """
        ).fetchall()
        if not constraints:
            print("  - none")
        else:
            for table_name, constraint_type, constraint_name, column_names in constraints:
                print(
                    "  - "
                    f"{table_name}: {constraint_type} "
                    f"{constraint_name or '<anonymous>'} "
                    f"{list(column_names) if column_names is not None else []}"
                )


def main() -> int:
    args = _parse_args()
    resource = _RepairDuckDBResource(db_path=str(Path(args.db_path).resolve()))

    print("[inspect-before]")
    _print_state(resource)

    if not args.repair:
        return 0

    if not args.yes:
        print(
            "\n[abort] --repair requires --yes after you stop Dagster writer jobs "
            "and back up the DB file."
        )
        return 2

    print("\n[repair] rebuilding image_metadata/image_labels ...")
    resource.repair_image_metadata_table()

    print("[inspect-after]")
    _print_state(resource)
    print("\n[done] repair completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
