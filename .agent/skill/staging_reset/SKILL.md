# Staging Reset Skill

## Purpose
Completely resets the staging test environment.

## Reset Targets
1. **DuckDB staging DB** — delete all table data (schema preserved)
2. **MinIO staging buckets** — delete all objects in staging-related buckets
3. **NAS archive folder** — delete all contents under `/home/user/mou/staging/archive/`
4. **NAS archive_pending folder** — delete all contents under `/home/user/mou/staging/archive_pending/`
5. **NAS incoming manifests** — delete all contents under `/home/user/mou/staging/incoming/.manifests/dispatch/`
6. **Dagster staging run history** — optional (delete the storage folder)

## Usage
```bash
# Full reset
.agent/skill/staging_reset/scripts/reset_staging.sh

# Specific targets only
.agent/skill/staging_reset/scripts/reset_staging.sh --db-only
.agent/skill/staging_reset/scripts/reset_staging.sh --minio-only
.agent/skill/staging_reset/scripts/reset_staging.sh --files-only
```

## Caveats
- Confirm no runs are active in staging Dagster before executing
- Original test data (`/home/user/mou/staging/tmp_data_2`, `/home/user/mou/staging/GS건설`) is not deleted
