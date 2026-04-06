#!/usr/bin/env python3
"""Fix: failed (DuckDB lock error) → completed for files that have raw_key + checksum."""
import duckdb

SOURCE_UNIT = "gcp/kkpolice-event-bucket/20260402"
LOCK_ERROR_PREFIX = "archive_finalize_failed:IO Error: Could not set lock"

db = duckdb.connect("/data/pipeline.duckdb", read_only=False)
print("Connected writable!")

before = db.execute(
    "SELECT ingest_status, COUNT(*) FROM raw_files WHERE source_unit_name = ? GROUP BY ingest_status",
    [SOURCE_UNIT],
).fetchall()
print(f"BEFORE: {before}")

db.execute(
    """
    UPDATE raw_files
    SET ingest_status = 'completed', error_message = NULL
    WHERE source_unit_name = ?
    AND ingest_status = 'failed'
    AND error_message LIKE ?
    AND raw_key IS NOT NULL AND raw_key != ''
    AND checksum IS NOT NULL AND checksum != ''
    """,
    [SOURCE_UNIT, LOCK_ERROR_PREFIX + "%"],
)
print("Updated failed -> completed (DuckDB lock errors only)")

after = db.execute(
    "SELECT ingest_status, COUNT(*) FROM raw_files WHERE source_unit_name = ? GROUP BY ingest_status",
    [SOURCE_UNIT],
).fetchall()
print(f"AFTER: {after}")

total = db.execute(
    "SELECT COUNT(*) FROM raw_files WHERE source_unit_name = ?", [SOURCE_UNIT]
).fetchone()
print(f"Total records: {total[0]}")

db.close()
print("Done")
