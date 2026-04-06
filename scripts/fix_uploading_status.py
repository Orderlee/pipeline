#!/usr/bin/env python3
"""Fix: uploading → completed for files that already have raw_key + checksum."""
import duckdb

SOURCE_UNIT = "gcp/kkpolice-event-bucket/20260402"

db = duckdb.connect("/data/pipeline.duckdb", read_only=False)
print("Connected writable!")

before = db.execute(
    "SELECT ingest_status, COUNT(*) FROM raw_files WHERE source_unit_name = ? GROUP BY ingest_status",
    [SOURCE_UNIT],
).fetchall()
print(f"BEFORE: {before}")

db.execute(
    """
    UPDATE raw_files SET ingest_status = 'completed'
    WHERE source_unit_name = ?
    AND ingest_status = 'uploading'
    AND raw_key IS NOT NULL AND raw_key != ''
    AND checksum IS NOT NULL AND checksum != ''
    """,
    [SOURCE_UNIT],
)
print("Updated uploading -> completed")

after = db.execute(
    "SELECT ingest_status, COUNT(*) FROM raw_files WHERE source_unit_name = ? GROUP BY ingest_status",
    [SOURCE_UNIT],
).fetchall()
print(f"AFTER: {after}")

db.close()
print("Done")
