# Deployment Guide — Production/Staging Environment Separation

> Written: 2026-04-10

## Architecture overview

```
Local PC
  ├── dev branch work + pytest
  ├── local verification with docker-compose.dev.yaml
  ├── dev push  -> staging auto-deploy
  └── main push -> production auto-deploy
         │
         ▼
GitHub Actions
  ├── Unit test
  ├── Determines whether to rebuild image based on change scope
  └── Self-hosted runner deploys to prod/test root on the server respectively
```

## Branch strategy

| Branch | Purpose | Deployment |
|--------|------|------|
| `dev` | Staging environment deploy branch | Auto-deploys to staging on push |
| `main` | Production deploy branch | Auto-deploys to production on push |
| `feature/*` | Feature development | Merge into dev |

**Rules:**
- No direct commits/pushes from the production server
- Validate on staging via `dev` deploy before merging to `main`
- Emergency fix: direct push to `main` is allowed (manual deploy also possible via workflow_dispatch)

## Local development environment

### 1. Environment setup

```bash
cp .env.dev.example .env.dev
# Edit .env.dev — adjust to local paths
```

### 2. Local Docker run

```bash
docker compose -f docker/docker-compose.dev.yaml up -d
# Dagster UI: http://localhost:3030
# MinIO Console: http://localhost:9001
```

### 3. Run pytest

```bash
pip install -e ".[dev]"
pytest tests/unit -q
```

## Production server initial setup

### 1. Install self-hosted runner

```bash
bash scripts/deploy/setup-runner.sh
```

There are three ways to pass the token.

```bash
# 1) Paste into the prompt during execution
bash scripts/deploy/setup-runner.sh

# 2) Pass as argument
bash scripts/deploy/setup-runner.sh --token <registration-token>

# 3) Pass as environment variable (recommended)
RUNNER_TOKEN=<registration-token> bash scripts/deploy/setup-runner.sh
```

Obtain the token from GitHub repo Settings > Actions > Runners.
The default runner label `self-hosted,linux,deploy,production,test` is recommended.

### 2. Check runner status

```bash
# systemd service status
cd ~/actions-runner && sudo ./svc.sh status

# Check logs
journalctl -u actions.runner.*.service -f
```

### 3. Verify Docker permissions

```bash
# Runner user must be in the docker group
groups $USER | grep docker || sudo usermod -aG docker $USER
```

## Deployment flow

### Automatic deployment (normal)

1. `dev` push:
   - Sync to test root
   - Uses `docker/.env.test` or the server's test env file
   - Dagster health check `http://10.0.0.10:3031/server_info`
2. `main` push:
   - Sync to production root
   - Uses the server's production `.env`
   - Dagster health check `http://10.0.0.10:3030/server_info`
3. Common GitHub Actions behavior:
   - Unit test → abort deploy on failure
   - Rebuild image if change scope covers Docker/runtime areas
   - Restart dagster-code-server → wait 15 seconds
   - Restart dagster-daemon + dagster
   - Health check (up to 60 seconds)

### Manual deployment (emergency)

GitHub repo > Actions > "Deploy to Test" or "Deploy to Production" > "Run workflow"
- Use `skip_tests: true` option to skip tests

### Excluded from deployment

Pushes that only change the following paths do not trigger a deployment:
- `docs/**`, `*.md`, `tests/**`, `.cursor/**`, `.agent/**`

## Rollback

```bash
# Check available image tags
bash scripts/deploy/rollback.sh

# Rollback to a specific version
bash scripts/deploy/rollback.sh datapipeline:abc12345
```

The image tag from a previous deployment can be found in the "Deploy summary" of the GitHub Actions run log.

## Safe procedure when changing workspace code location

Changing the loader in [docker/app/workspace.yaml](../../docker/app/workspace.yaml) (`python_file` → `grpc_server` switch, `relative_path`/`attribute` change, `location_name` specification, etc.) changes the code location identifier recognized by Dagster. Queued runs that were enqueued with the old identifier will fail with `DagsterCodeLocationNotFoundError` when dequeued by the daemon. Always follow the steps below before deploying.

1. **Clear the queue** — Check for waiting runs via UI (`/runs` → Queued filter) or CLI:
   ```bash
   docker exec docker-dagster-1 dagster run list --status QUEUED --limit 100
   ```
   If not empty, review each run in the UI then Terminate. (Bulk CLI terminate risks killing important runs, so avoid it.)
2. **Deploy workspace file** — Commit and push the [docker/app/workspace.yaml](../../docker/app/workspace.yaml) change (or wait for the runner to sync).
3. **Restart containers** — in order: `dagster-code-server` → `dagster` → `dagster-daemon`.
4. **Verify registration** —
   ```bash
   curl -s http://127.0.0.1:3030/server_info | jq .
   ```
   The new location should appear in the response, and the `Deployment` tab in the UI should show `Loaded`.
5. **Monitor daemon logs for 10 minutes** —
   ```bash
   docker logs -f docker-dagster-daemon-1 2>&1 | grep -i "CodeLocationNotFound"
   ```
   Normal once the same error no longer appears.

## Caveats / notes

- **DuckDB**: Volume-mounted, so no impact from deployment
- **MinIO**: Docker named volume, safe across container restarts
- **Dagster run history**: Preserved in `dagster_home/storage/` volume
- **GPU services (YOLO, SAM3)**: Unrelated to pipeline code changes — no separate restart needed
- **NAS mount**: Host bind mount, unrelated to deployment
- **env files**: Production uses server-local `.env`, staging is managed from `docker/.env.test`
- **MinIO Console address**: production `9001`, staging `9003`
- **Application endpoint**: production `9000`, staging `9002`
