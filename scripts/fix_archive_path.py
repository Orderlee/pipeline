"""kkpolice-event-bucket/20260402__2 -> 20260402 archive_path 수정."""
import duckdb
import sys
import time

DB_PATH = "/data/pipeline.duckdb"
MAX_RETRIES = 30
RETRY_DELAY = 2.0

for attempt in range(MAX_RETRIES):
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        rows = conn.execute("""
            SELECT asset_id, archive_path
            FROM raw_files
            WHERE archive_path LIKE '%kkpolice-event-bucket/20260402__2/%'
        """).fetchall()
        conn.close()
        print(f"[read-only] Records with __2: {len(rows)}")
        for r in rows[:5]:
            print(f"  {r[0]}: {r[1]}")
        if len(rows) > 5:
            print(f"  ... and {len(rows)-5} more")
        break
    except Exception as e:
        if "lock" in str(e).lower() and attempt < MAX_RETRIES - 1:
            print(f"read-only lock retry {attempt+1}/{MAX_RETRIES}: {e}")
            time.sleep(RETRY_DELAY)
            continue
        print(f"read-only query failed: {e}")
        sys.exit(1)

if not rows:
    print("No records to update. Done.")
    sys.exit(0)

print(f"\nUpdating {len(rows)} records...")
for attempt in range(MAX_RETRIES):
    try:
        conn = duckdb.connect(DB_PATH)
        updated = conn.execute("""
            UPDATE raw_files
            SET archive_path = REPLACE(archive_path, 'kkpolice-event-bucket/20260402__2/', 'kkpolice-event-bucket/20260402/')
            WHERE archive_path LIKE '%kkpolice-event-bucket/20260402__2/%'
        """).fetchone()
        conn.close()
        print(f"Updated successfully. Rows affected: {updated}")
        break
    except Exception as e:
        if "lock" in str(e).lower() and attempt < MAX_RETRIES - 1:
            print(f"write lock retry {attempt+1}/{MAX_RETRIES}: {e}")
            time.sleep(RETRY_DELAY)
            continue
        print(f"Update failed: {e}")
        sys.exit(1)

# Verify
for attempt in range(MAX_RETRIES):
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        remaining = conn.execute("""
            SELECT COUNT(*) FROM raw_files
            WHERE archive_path LIKE '%kkpolice-event-bucket/20260402__2/%'
        """).fetchone()
        total = conn.execute("""
            SELECT COUNT(*) FROM raw_files
            WHERE archive_path LIKE '%kkpolice-event-bucket/20260402/%'
              AND source_unit_name = 'gcp/kkpolice-event-bucket/20260402'
        """).fetchone()
        conn.close()
        print(f"\nVerification:")
        print(f"  Remaining __2 records: {remaining[0]}")
        print(f"  Total 20260402 records: {total[0]}")
        break
    except Exception as e:
        if "lock" in str(e).lower() and attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY)
            continue
        print(f"Verify failed: {e}")
