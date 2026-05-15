# Git Branch Strategy & Deployment Structure Guide

> Based on upstream-org/Datapipeline-Data-data_pipeline

## Current Structure

```text
Personal fork / feature branch
  -> PR -> upstream-org/dev (test)
  -> After validation, PR -> upstream-org/main (production)
```

| Branch | Role | Deployment |
|---|---|---|
| Personal fork / feature | Individual work, experiments, drafts | None |
| `upstream-org/dev` | Team integration + test validation | Test auto-deploy |
| `upstream-org/main` | Production release | Production auto-deploy |

Core principles:

- `dev = test`
- `main = production`
- Feature development is consolidated into `dev`; production promotion is done exclusively via `dev -> main` PR.
- The `staging` terminology found in older documents or scripts mostly refers to the current `test` environment and is a legacy expression.

---

## Quick Reference (Flash Card)

> What to check in 5 seconds when deploying directly from the Orderlee fork. Come here when you forget.

### Standard Feature Deployment — 4 Steps

```bash
# 1) Commit + push to dev → staging auto-deploy
git push origin dev

# 2) Wait 3–10 minutes, then validate on staging
#    http://10.0.0.10:3031/  (Dagster UI)
#    + manually check relevant sensor ticks, asset re-runs, MinIO artifacts, etc.

# 3) Promote to production
git checkout main && git merge --no-ff dev && git push origin main

# 4) After confirming production stability, contribute to upstream (upstream-org/dev)
./tools/pr-to-upstream.sh          # interactive (y/N confirmation)
# or to check only the commits being sent: ./tools/pr-to-upstream.sh --dry-run
```

### Hotfix (Emergency Production Fix) — 4 Steps

```bash
# 1) Branch off main for a fix branch
git checkout -b fix/<name> main

# 2) Fix + commit + push → create PR on GitHub
git push origin fix/<name>
# PR → main → merge → production deploys immediately

# 3) (Almost always required!) Back-merge to dev
git checkout dev && git merge main && git push origin dev
```

> ⚠️ **Never skip the hotfix back-merge** — the same bug will resurface in the next regular release (`dev → main`).

### Automatic vs Manual

| Step | Automatic | Owner |
|---|---|---|
| Commit & push | ❌ | Developer |
| Staging deploy (deploy-test.yml) | ✅ | GitHub Actions |
| Staging validation | ❌ | Human |
| dev → main merge | ❌ | Developer |
| Production deploy (deploy-production.yml) | ✅ | GitHub Actions |

### CI Trigger Rules (When CI runs and when it doesn't)

| Changed files | CI triggered? | Image rebuild? |
|---|---|---|
| `*.md` (root) / `docs/**` / `tests/**` / `.cursor/**` / `.agent/**` | ❌ (paths-ignore) | — |
| `src/vlm_pipeline/**` | ✅ | ❌ (rsync only, fast) |
| `Dockerfile` / `docker/app/**` / `configs/**` / `scripts/**` / `gcp/**` / `split_dataset/**` / `src/python/**` | ✅ | ✅ (slow) |
| `.env` / `.env.test` | Not tracked by git — cannot trigger. Edit directly on host + `docker compose restart` for the affected environment |

### URL Reference

| Purpose | URL |
|---|---|
| GitHub Actions | https://github.com/Orderlee/Datapipeline-Data-data_pipeline/actions |
| PROD Dagster UI | http://10.0.0.10:3030/ |
| STAGING Dagster UI | http://10.0.0.10:3031/ |
| PROD MinIO Console | http://10.0.0.51:9001/ (S3 :9000) |
| STAGING MinIO Console | http://10.0.0.51:9003/ (S3 :9002) |

### Common Mistakes

- **Directly editing** `src/`, `configs/`, `scripts/` on the staging host → wiped by `rsync --delete` on the next CI deploy. Always go through git commits.
- **Skipping the hotfix back-merge** → bug resurfaces in the next regular release.
- **Trying to put env changes in git** → `.env`/`.env.test` are in `.gitignore`. Edit on host only.
- **Directly modifying files while debugging on staging** → disappears on next deploy if not committed.
- **Forgetting to contribute upstream** → changes accumulate only in Orderlee/main while upstream-org/dev falls behind. Always run `tools/pr-to-upstream.sh` in step 4.

### Manual Deployment / CI Bypass (Emergency Only)

When CI is down, deploy directly from the host:

```bash
# PROD
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git pull origin main --ff-only
cd docker && docker compose restart dagster dagster-daemon dagster-code-server

# STAGING
cd /home/user/work_p/Datapipeline-Data-data_pipeline_test
git pull origin dev --ff-only
cd docker && docker compose restart
```

---

## Work Scenarios

### Scenario 1. Standard Feature Development

```bash
git fetch upstream
git checkout dev
git merge upstream/dev

git checkout -b feature/xxx
git push origin feature/xxx
```

Flow:

```text
PR: my-fork/feature/xxx -> upstream-org/dev
  -> test auto-deploy
  -> test validation
  -> dev -> main PR at release time
```

### Scenario 2. Emergency Bug Fix

```bash
git fetch upstream
git checkout -b hotfix/issue-name upstream/main
```

Flow:

```text
PR 1: hotfix/issue-name -> upstream-org/main
PR 2: hotfix/issue-name -> upstream-org/dev
```

Key points:

- Hotfixes are branched from `main`.
- If you only merge to `main` and omit `dev`, the bug may resurface at the next release.

### Scenario 3. Continuing Development on Top of a Teammate's Work

```bash
git fetch upstream
git checkout dev
git merge upstream/dev
git checkout -b feature/follow-up
```

Key point:

- The shared reference point across the team is always `upstream-org/dev`.
- Instead of chaining branches between forks directly, wait for the work to land in `dev` first, then build on top.

### Scenario 4. Release

```text
1. Validation complete on upstream-org/dev (test environment)
2. PR: upstream-org/dev -> upstream-org/main
3. Review and merge
4. main auto-deploys
```

Key point:

- The `dev -> main` PR is the production release record.
- Include included features, risk factors, and validation results in the PR description.

### Scenario 5. Experimental Work

```bash
git checkout -b experiment/xxx
git push origin experiment/xxx
```

Key point:

- Experiments stay in personal forks only.
- Open a PR to `upstream-org/dev` only when the results are sufficiently promising.

## Decision Tree

```text
Starting new work
  ├─ Is it an urgent production bug?
  │    -> hotfix based on main
  │    -> PR to both main and dev
  ├─ Is it a standard feature?
  │    -> Update dev to latest
  │    -> feature branch
  │    -> PR to upstream-org/dev
  └─ Is it an experiment?
       -> Personal fork only
       -> PR to upstream-org/dev if results are good
```

## Deployment Criteria

### Test Deployment

- Trigger: push to `upstream-org/dev`
- Target: test stack
- Base env: `docker/.env.test`
- Purpose: team integration validation

### Production Deployment

- Trigger: push to `upstream-org/main`
- Target: production stack
- Base env: `docker/.env`
- Purpose: live production promotion

## Self-Hosted Runner

The runner currently handles branch-based deployments.

Recommended labels:

```text
self-hosted, linux, deploy, production, test
```

Usage with `setup-runner.sh`:

```bash
bash scripts/deploy/setup-runner.sh --token <token>
```

## Operational Checkpoints

### dev -> test

- Confirm the test workflow runs after merging to `dev`
- Confirm the test Dagster health check passes
- Confirm only the test data plane (`/data/staging.duckdb`, `/nas/staging/...`) was touched

### main -> production

- Confirm the production workflow runs after merging to `main`
- Confirm the production Dagster health check passes
- Confirm only the production data plane (`/data/pipeline.duckdb`, `/nas/...`) was touched

## Team Rules

| Branch | Rule |
|---|---|
| `main` | PR required, review before merge, direct push forbidden |
| `dev` | PR recommended or required, minimize direct push |

## Compatibility Notes

- Names such as `/nas/staging`, `/data/staging.duckdb`, and `STAGING_ROOT` still appear in some scripts and data planes for legacy compatibility.
- However, the branch/deployment semantics are now `test`, not `staging`.
- New scripts and new documentation should use the `test` terminology wherever possible.
