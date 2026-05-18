---
name: duckdb_staging_wiper
description: Skill for wiping the staging environment DuckDB to maintain a clean test state
---

# DuckDB Staging Wiper Skill

This skill automates the process of completely deleting and resetting existing DuckDB data in the staging environment before running data pipeline tests.

## 📋 Core Features
- Delete the staging DuckDB file (`staging.duckdb`)
- Clean up related temporary files such as WAL (Write-Ahead Log) files
- Verify file absence after deletion

## 🛠 Usage

### 1. Automated Script Execution
This script runs on the host environment. `sudo` may be required if there are permission issues.

```bash
# Run the skill script
bash /home/user/work_p/Datapipeline-Data-data_pipeline/.agent/skill/duckdb_staging_wiper/scripts/wipe_staging_db.sh
```

### 2. Manual Reset (CLI)
To run commands directly, execute the following in order.

```bash
# 1. Set the runtime file path
STAGING_DB_PATH="/home/user/work_p/Datapipeline-Data-data_pipeline/docker/data/staging.duckdb"

# 2. Delete files
rm -f $STAGING_DB_PATH
rm -f ${STAGING_DB_PATH}.wal

# 3. Verify
ls -l $STAGING_DB_PATH
```

## ⚠️ Caveats
- **Protect production data**: This skill must target `staging.duckdb` only. Take care that `pipeline.duckdb` is not deleted.
- **Service interruption**: If a service (such as the staging Dagster) holds the DuckDB file open when it is deleted, an error may occur. It is best to stop the service before running this script.
