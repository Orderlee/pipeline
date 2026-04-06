#!/usr/bin/env python3
"""One-shot cleanup: delete DB records for gcp/source-c-event-bucket/20260402 and gcp/source-b-event-bucket/20260402."""
import duckdb

SOURCE_UNITS = [
    "gcp/source-c-event-bucket/20260402",
    "gcp/source-b-event-bucket/20260402",
]

db = duckdb.connect("/data/pipeline.duckdb", read_only=False)
print("Connected writable!")

for su in SOURCE_UNITS:
    c = db.execute("SELECT COUNT(*) FROM raw_files WHERE source_unit_name = ?", [su]).fetchone()[0]
    print(f"BEFORE {su}: {c} raw_files")

vm = db.execute(
    "SELECT COUNT(*) FROM video_metadata WHERE asset_id IN "
    "(SELECT asset_id FROM raw_files WHERE source_unit_name = ANY(?))",
    [SOURCE_UNITS],
).fetchone()[0]
print(f"BEFORE video_metadata: {vm}")

db.execute(
    "DELETE FROM video_metadata WHERE asset_id IN "
    "(SELECT asset_id FROM raw_files WHERE source_unit_name = ANY(?))",
    [SOURCE_UNITS],
)
print("video_metadata deleted")

db.execute(
    "DELETE FROM raw_files WHERE source_unit_name = ANY(?)",
    [SOURCE_UNITS],
)
print("raw_files deleted")

for su in SOURCE_UNITS:
    c = db.execute("SELECT COUNT(*) FROM raw_files WHERE source_unit_name = ?", [su]).fetchone()[0]
    print(f"AFTER {su}: {c} records")

db.close()
print("DB cleanup complete")
