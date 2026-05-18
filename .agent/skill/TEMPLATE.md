---
description: [Write a short one-line summary of the skill here. Example: Guide for staging data reset and restart scripts]
---

# 🎯 Skill Purpose (Trigger)
- [Describe the situation or user request (or error) that should prompt an AI to read this document]
- Example: "When the staging pipeline has stalled", "When a DuckDB lock error occurs"

# 🛠️ Dependencies
- **Target script:** `scripts/target_script_name.py`
- **Target container/DB:** `docker/data/staging.duckdb`
- **Target logs:** `/nas/staging/archive/failed/`

# 📝 Action Steps
The AI agent executing this skill must perform the steps below strictly in order.

1. **[Check status]** First, verify container status via `docker compose ps`.
2. **[Run script]** Execute the following command in the terminal: `python scripts/... --option`
3. **[Validate result]** After running the script, query DuckDB (`query_local_duckdb.py`) to confirm data was applied.

# 🚫 Constraints
- [State any actions that are absolutely prohibited]
- Example: "Never connect to the production DB (`pipeline.duckdb`)."
- Example: "Do not use this script together with `docker-compose down`."
