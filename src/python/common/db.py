from pathlib import Path
from typing import Optional

import duckdb

from .config import get_settings


def connect(db_path: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    settings = get_settings()
    resolved = db_path or settings.duckdb_path
    return duckdb.connect(resolved)


def ensure_schema(con: duckdb.DuckDBPyConnection, schema_path: Optional[Path] = None) -> None:
    base_dir = Path(__file__).resolve().parent
    schema_file = schema_path or (base_dir / "schema.sql")
    if not schema_file.exists():
        raise FileNotFoundError(f"schema.sql not found at {schema_file}")
    con.execute(schema_file.read_text(encoding="utf-8"))
