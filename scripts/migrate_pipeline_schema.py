#!/usr/bin/env python3
"""Run the full DuckDB schema migration outside runtime hot paths.

Examples:
  python3 scripts/migrate_pipeline_schema.py --db-path /data/pipeline.duckdb
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


class _SchemaDuckDBResource(DuckDBBaseMixin):
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the full DuckDB schema migration. "
            "Use this before deploy/restart so runtime jobs can rely on ensure_runtime_schema()."
        )
    )
    parser.add_argument(
        "--db-path",
        default="/data/pipeline.duckdb",
        help="Target DuckDB path. Defaults to /data/pipeline.duckdb.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    resource = _SchemaDuckDBResource(db_path=str(Path(args.db_path).resolve()))
    print(f"[schema-migration] start db={resource.db_path}")
    resource.ensure_schema()
    print("[schema-migration] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
