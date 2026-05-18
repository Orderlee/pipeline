# Dagster Lineage & Code Alignment Skill

## Overview
This skill ensures that the Dagster pipeline definitions are consistent, lineage is intact, and code follows the expected patterns (Staging vs Production). It proactively fixes errors found during lineage validation.

## Procedures

### 1. Code Consistency Check
- Ensure all imported assets in `src/vlm_pipeline/definitions.py` exist in their respective modules.
- Verify that environment-specific logic (e.g., `IS_STAGING`) correctly toggles sensors and jobs.
- Match Docker image tags in deployment scripts with the `-staging` suffix when in staging mode.

### 2. Dagster Lineage Validation
Use the following command to check for definition errors without running the pipeline:

**Local Check (requires venv):**
```bash
dagster asset list -f src/vlm_pipeline/definitions.py
```

**Docker-based Check (Recommended for Staging):**
Use these commands to validate lineage inside the actual project containers to ensure all environment variables and dependencies are consistent.

For **Staging**:
```bash
docker compose -f docker/docker-compose.yaml run --rm --no-deps dagster-staging /app/dg asset list -f /src/vlm/vlm_pipeline/definitions.py
```

For **Production**:
```bash
docker compose -f docker/docker-compose.yaml run --rm --no-deps dagster /app/dg asset list -f /src/vlm/vlm_pipeline/definitions.py
```

- **Lineage Breaks**: If an asset's upstream dependency is missing or misnamed, identify the source module and fix the export/import.
- **Resource Conflicts**: Ensure resources like `DuckDBResource` and `MinIOResource` are properly initialized for the current environment.
- **Path Mapping**: In Docker, the source code is mounted at `/src/vlm`, making the definition file available at `/src/vlm/vlm_pipeline/definitions.py`.

### 3. Automated Error Correction
When an error is detected in Dagster definitions:
1. **Analyze the Traceback**: Locate the specific line in `definitions.py` or the asset definition.
2. **Review Upstream/Downstream**: Check the `selection` parameter in `define_asset_job`. 
3. **Cross-Reference**: Check the actual asset definitions in `src/vlm_pipeline/defs/*/assets.py`.
4. **Apply Fix**: Update the code to match the correct lineage or naming convention.

### 4. Lineage Health Report
- Maintain a log of lineage checks in `WORKLOG.md` or a dedicated artifact.
- Flag any circular dependencies or disconnected assets.

## Usage
Run this skill whenever:
- A new asset is added.
- Pipeline configuration is changed.
- Moving from staging to production prep.
