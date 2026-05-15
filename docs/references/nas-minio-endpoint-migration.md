# NAS 10.0.0.36 → 10.0.0.51 Migration Guide

> **Purpose**: Complete process/checklist for migrating from existing `10.0.0.36` (EOL) → new ASUSTOR `10.0.0.51` (AS7216RDX-7561, 200TB).
>
> **Agreed big picture**: MinIO container migration + vlm-dataset NFS migration (B-narrow) + remaining CIFS shares NFS consolidation (Option B). LS stays as-is. Remaining 4 MinIO buckets continue operating as-is.

---

## 0. Background

- **10.0.0.36 NAS EOL** → consolidating to 10.0.0.51 (Lockerstor R Pro Gen2, ASUSTOR ADM 5)
- **10.0.0.36 current operational resources**:
  - MinIO prod (9000/9001) — 111 GiB / 26K objects
  - MinIO staging (9002/9003) — 11 GiB / 3K objects
  - CIFS shares: `incoming`, `archive`, `staging`, `external` (=`nas`), `AI_data` (=`nas_192tb`), `alarm`
- **10.0.0.51 final directory structure** (Option B confirmed):
  ```
  /volume1/data/                  ← NFS export (rw, Squash="map all users to admin")
    ├── incoming/                 ← old 10.0.0.36 /incoming
    ├── archive/                  ← old 10.0.0.36 /archive
    ├── staging/{incoming,archive}/
    ├── datasets/                 ← vlm-dataset NFS migration (B-narrow) target
    └── _tmp/                     ← atomic write staging area
  /volume1/minio_data_prod/       ← MinIO prod data (NAS local, not NFS)
  /volume1/minio_data_staging/    ← MinIO staging
  ```

## 1. Three tracks

| Track | Work | Responsible | Priority |
|------|------|------|---------|
| **A. MinIO migration** | 10.0.0.36 → 10.0.0.51 container + data + endpoint | User + admin | 🔥 Urgent (EOL) |
| **B. CIFS → NFS single-share consolidation** | Data migration of incoming/archive/staging etc. + host fstab/compose changes | admin (data) + user (fstab/compose) | 🟢 Depends on admin schedule |
| **C. vlm-dataset NFS migration (B-narrow)** | `defs/build/assets.py` branching + DB UPDATE | User | 🟡 After A complete, separate 1-2 weeks |

→ **Start with A**. Proceed with B in parallel once admin responds. C is a separate track after A completes.

---

## 2. Progress checklist

### Pre-requisites (complete)

- [x] NAS admin account issued
- [x] Docker Engine 28.1.1 installed (manual .apk → ADM UI)
- [x] Portainer CE installed (`https://10.0.0.51:9443`, port 8000 excluded)
- [x] NFS export rw permission + Squash mapping applied
- [x] Host `/etc/fstab` permanent registration (`10.0.0.51:/volume1/data → /home/user/mou/nas_200tb`)
- [x] Container root + `user` user write verification
- [x] MinIO image `docker save` on host complete (`/tmp/minio_image.tar.gz`, 59 MB)
- [x] 10.0.0.36 data metadata backup (`/tmp/10.0.0.36_{prod,staging}_du.txt`)
- [x] 6 subdirectories created inside 10.0.0.51 `/volume1/data` (incoming/archive/staging/{incoming,archive}/datasets/_tmp)

### Track A — MinIO migration

- [ ] **Step 1**. Transfer MinIO image to NAS
- [ ] **Step 2**. Create MinIO data directories (`/volume1/minio_data_{prod,staging}`)
- [ ] **Step 3**. Deploy 2 MinIO containers via Portainer Stack
- [ ] **Step 4**. Health check (`/minio/health/live` 200)
- [ ] **Step 5**. Create buckets (same names as 10.0.0.36)
- [ ] **Step 6**. `mc mirror` 10.0.0.36 → 10.0.0.51 (1-4 hours, background)
- [ ] **Step 7**. Verification (`mc du` diff)
- [ ] **Step 8**. `dev` branch PR — 22 files `10.0.0.36` → `10.0.0.51`
- [ ] **Step 9**. Staging endpoint change → e2e validation (1-2 weeks)
- [ ] **Step 10**. Prod cutover (30 min to 1 hour downtime)
- [ ] **Step 11**. Keep 10.0.0.36 MinIO in read-only mode (1-2 weeks, rollback safety net)
- [ ] **Step 12**. Delete 10.0.0.36 MinIO data + stop container

### Track B — CIFS share migration (admin dependent)

- [ ] Request admin to migrate 10.0.0.36 → 10.0.0.51 data
- [ ] Confirm data size of each share
- [ ] Coordinate migration schedule
- [ ] Data migration complete
- [ ] Remove 5 CIFS lines from host `/etc/fstab`
- [ ] Change bind mount source paths in `docker-compose.yaml` (keep targets)
- [ ] `dev` branch PR
- [ ] Cutover (brief mount interruption)

### Track C — vlm-dataset NFS migration (B-narrow)

- [ ] Add branching to `src/vlm_pipeline/defs/build/assets.py` (DATASET_BUCKET → NFS path)
- [ ] Migrate vlm-dataset data → `/nas_200tb/datasets/`
- [ ] DB UPDATE: `dataset_bucket='fs'`
- [ ] Change training machine read path (when applicable)
- [ ] E2E (build_dataset job)
- [ ] Validation + 1 week stabilization

---

## 3. Step-by-step commands

### Step 1 — Transfer MinIO image to NAS (5 minutes)

```bash
# From host (10.0.0.10) SSH session
gunzip -c /tmp/minio_image.tar.gz | ssh admin@10.0.0.51 'sudo docker load'

# If first SSH, register known_hosts in advance:
ssh admin@10.0.0.51 'echo ok'   # type yes
```

Verify:
```bash
ssh admin@10.0.0.51 'sudo docker images | grep minio'
```

### Step 2 — Data directories

```bash
# NAS SSH
sudo mkdir -p /volume1/minio_data_prod /volume1/minio_data_staging
sudo chmod 755 /volume1/minio_data_prod /volume1/minio_data_staging
```

### Step 3 — Deploy MinIO stack (Portainer)

`https://10.0.0.51:9443` → **Stacks → + Add stack** → name `minio`:

```yaml
services:
  minio-prod:
    image: minio/minio:latest
    container_name: minio-prod
    restart: unless-stopped
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
      MINIO_PROMETHEUS_AUTH_TYPE: public
    volumes: ["/volume1/minio_data_prod:/data"]
    command: server /data --console-address ":9001"

  minio-staging:
    image: minio/minio:latest
    container_name: minio-staging
    restart: unless-stopped
    ports: ["9002:9000", "9003:9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
      MINIO_PROMETHEUS_AUTH_TYPE: public
    volumes: ["/volume1/minio_data_staging:/data"]
    command: server /data --console-address ":9001"
```

**Deploy the stack**.

### Step 4 — Health check

```bash
curl -sf http://10.0.0.51:9000/minio/health/live && echo "prod OK"
curl -sf http://10.0.0.51:9002/minio/health/live && echo "staging OK"
```

### Step 5 — Create buckets

```bash
# From host (10.0.0.10)
docker exec docker-minio-1 mc alias set new-prod    http://10.0.0.51:9000 minioadmin minioadmin
docker exec docker-minio-1 mc alias set new-staging http://10.0.0.51:9002 minioadmin minioadmin

# Check actual bucket list on 10.0.0.36
docker exec docker-minio-1 mc ls old-prod
docker exec docker-minio-1 mc ls old-staging

# Create with same names (5 standard buckets)
for b in vlm-raw vlm-labels vlm-processed vlm-dataset vlm-classification; do
  docker exec docker-minio-1 mc mb new-prod/$b
  docker exec docker-minio-1 mc mb new-staging/$b
done
```

### Step 6 — `mc mirror` (background)

```bash
docker exec docker-minio-1 mc mirror --preserve old-prod    new-prod    2>&1 | tee /tmp/mirror-prod.log
docker exec docker-minio-1 mc mirror --preserve old-staging new-staging 2>&1 | tee /tmp/mirror-staging.log
```

> 1-4 hours (varies by network/disk IO). `--watch` option enables incremental tracking.

### Step 7 — Verification

```bash
docker exec docker-minio-1 mc du --recursive new-prod    > /tmp/10.0.0.51_prod_du.txt
docker exec docker-minio-1 mc du --recursive new-staging > /tmp/10.0.0.51_staging_du.txt
diff /tmp/10.0.0.36_prod_du.txt    /tmp/10.0.0.51_prod_du.txt
diff /tmp/10.0.0.36_staging_du.txt /tmp/10.0.0.51_staging_du.txt
```

### Step 8 — Endpoint change PR (`dev` branch)

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git checkout dev && git pull
git checkout -b feat/migrate-minio-to-10.0.0.51

# Bulk sed (review CLAUDE.md, docs separately)
grep -rlE "172\.168\.47\.36" docker/.env.test.example docker/.env.test.template \
  docker/docker-compose.yaml docker/docker-compose.labelstudio.yaml \
  src/ configs/ scripts/ 2>/dev/null | \
  grep -v -E "\.pyc|dagster_home" | \
  xargs sed -i 's|172\.168\.47\.36|10.0.0.51|g'

git diff
git commit -m "feat(infra): migrate MinIO endpoint 10.0.0.36 → 10.0.0.51"
git push -u origin feat/migrate-minio-to-10.0.0.51
gh pr create --base dev --title "feat(infra): migrate MinIO to 10.0.0.51"
```

### Step 9 — Staging e2e validation (1-2 weeks)

PR merged to `dev` → CI auto-deploy → verify on staging environment:
- sensor evaluate works normally
- dispatch run works normally
- MinIO console (`:9003`) object confirmed
- No `endpoint URL` errors in compute_logs

### Step 10 — Prod cutover sequence

> **Pre-announcement** (T-3d): Notify labelers/users of downtime

```bash
# T+0   Stop all Dagster sensors (UI or dagster CLI)
# T+5   Wait for in-flight runs to complete
# T+10  Stop containers
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
docker compose stop dagster dagster-daemon dagster-code-server

# T+15  Final delta mirror
docker exec docker-minio-1 mc mirror --preserve old-prod    new-prod
docker exec docker-minio-1 mc mirror --preserve old-staging new-staging

# Verification
docker exec docker-minio-1 mc du new-prod    | tail
docker exec docker-minio-1 mc du new-staging | tail

# T+25  Edit .env on host directly (not tracked by git)
sed -i 's|172\.168\.47\.36|10.0.0.51|g' /home/user/work_p/Datapipeline-Data-data_pipeline/docker/.env

# T+30  Merge main PR (Step 8 PR from dev → main) — triggers CI auto prod deploy
gh pr create --base main --head dev --title "feat(infra): migrate MinIO endpoint to 10.0.0.51"
# Or restart immediately
docker compose up -d

# T+40  Health check
curl -sf http://10.0.0.51:9000/minio/health/live && echo "prod OK"

# T+45  Restart sensors (Dagster UI)

# T+46  Bulk-renew LS presigned URLs (immediately restore labeler footage)
#       — existing task data.video values are 10.0.0.36 URLs and will be broken. Reissue with new endpoint.
#       — there is also an auto-renewal schedule (ls_presign_renew_job, daily 05:00 KST) but immediate recovery is recommended
docker exec docker-dagster-1 python /src/vlm/gemini/ls_tasks.py renew --project-name <project>
# Or for all active projects (check project names in Dagster UI or LS UI then repeat)
```

### Step 11 — 10.0.0.36 read-only mode (rollback safety net)

```bash
# On 10.0.0.36 (admin)
# Set MinIO container to --read-only or configure policy to read-only
# Or docker stop while preserving data
```

Monitor operations for 1-2 weeks, then proceed to Step 12 once confirmed safe.

### Step 12 — 10.0.0.36 cleanup

```bash
# From admin
docker stop minio-prod-old minio-staging-old
docker rm   minio-prod-old minio-staging-old
# Wait an additional 1 week before deleting data
```

---

## 4. NAS admin requests

### Immediate requests (Track A)

```
1. NFS export permission: /volume1/data → rw + Squash="map all users to admin"
   (complete)
2. Pre-create directories:
   /volume1/minio_data_prod
   /volume1/minio_data_staging
```

### Track B requests (separate)

```
Please move the following data from 10.0.0.36 to subdirectories in 10.0.0.51 /volume1/data:

| 10.0.0.36 share               | 10.0.0.51 location                      |
|--------------------------|-------------------------------------|
| /volume1/incoming         | /volume1/data/incoming/             |
| /volume1/archive          | /volume1/data/archive/              |
| /volume1/staging/incoming | /volume1/data/staging/incoming/     |
| /volume1/staging/archive  | /volume1/data/staging/archive/      |
| /volume1/external (=nas)  | (separate discussion if needed)     |
| /volume1/AI_data (=nas_192tb) | (separate discussion if needed)  |
| /volume1/alarm            | (uses separate account nas-alarm.cred) |

- Please let us know the data size of each share
- Please coordinate on migration schedule
- Also please provide a list of other docker containers on 10.0.0.36 (cannot check docker ps from outside)
```

---

## 5. Troubleshooting notes (issues encountered during 2026-05-12 progress)

| Symptom | Cause | Resolution |
|------|------|------|
| App Central auto-install stalled | NAS backend hang | Manual .apk upload (App Central → ⚙️ → Manual install) |
| ADM UI manual install also stalled | Browser session issue | Log out → new tab → retry |
| `apkg --install` option not found | ADM's `apkg` only supports download, install is GUI-only | Use manual install GUI |
| scp speed 30 KB/s | macOS (192.168.0.x) and NAS (10.0.0.x) on different subnets, routed through home router | Route via host (10.0.0.10) (same LAN) |
| Portainer port 8000 collision | ADM web UI uses 8000 | Exclude `-p 8000:8000`, use only 9443 |
| NFS mount read-only | Server-side export permission was read-only | NAS admin changed NFS permissions → rw |
| Possible container root write rejection | Squash mapping not applied | Squash → "map all users to admin" |
| New folder ownership `root:root` (old was `999:ping`) | Squash may be partial mapping | Verify with write test as user (UID 1000) |

---

## 6. Unresolved / risks

| Item | Status |
|------|------|
| 10.0.0.36 CIFS shares migration schedule (incoming/archive/staging/nas/nas_192tb/alarm) | Awaiting admin confirmation |
| Data size of each share | Awaiting admin confirmation |
| Other docker containers on 10.0.0.36 | Cannot check from outside, awaiting admin answer |
| NAS NIC 1GbE link (10GbE unused) | Separate inspection, low priority |
| 10.0.0.36 endpoint dependencies in dispatch-agent and other external services | Need to search + notify before migration |
| Squash mapping accuracy (root → admin) | Needs verification by user (UID 1000) write test |

---

## 7. Reference

### Environment information

| Item | Production | Staging |
|------|-----------|---------|
| Dagster UI | `http://10.0.0.10:3030` | `http://10.0.0.10:3031` |
| MinIO endpoint (after migration) | `http://10.0.0.51:9000` | `http://10.0.0.51:9002` |
| MinIO Console | `:9001` | `:9003` |
| NAS NFS export | `10.0.0.51:/volume1/data` |  |
| Host mount | `/home/user/mou/nas_200tb` |  |

### Credentials

- MinIO ROOT_USER / PASSWORD: `minioadmin` / `minioadmin` (recommended to change after migration)
- ADM admin: NAS administrator
- Host `user`: host (10.0.0.10) SSH

### Related documents

- [CLAUDE.md](../../CLAUDE.md) — dual environment structure, MinIO bucket/path policy
- [docs/references/dbeaver-pg-duckdb-test-guide.md](./dbeaver-pg-duckdb-test-guide.md) — staging Postgres connection guide
- [docs/runbook.md](../runbook.md) — general operations manual

### Memory (Claude session)

- `~/.claude/projects/.../memory/project_nas_200tb.md` — progress + history
- `~/.claude/projects/.../memory/project_hosts.md` — host IP/roles
