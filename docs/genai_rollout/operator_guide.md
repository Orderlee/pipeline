# GenAI Studio — Operator Guide (Phase 5)

Items for operators to verify immediately before staging stabilization + main promotion.

---

## 1. API Key Entry

Replace the following items in `docker/.env.test` with real values (not git-tracked; edit directly on the host).

| Key | Source | Notes |
|----|--------|-------|
| `KLING_ACCESS_KEY` / `KLING_SECRET_KEY` | klingai.com console | For JWT signing (HS256) |
| `FAL_KEY` | fal.ai dashboard | Higgsfield model calls |
| `OPENAI_API_KEY` | platform.openai.com | gpt-image-1 calls |
| `HIGGSFIELD_FAL_MODEL` | Verify exact model ID on fal.ai dashboard (Codex HIGH) | Default `fal-ai/higgsfield/i2v-soul` needs verification |
| `GEMINI_GOOGLE_APPLICATION_CREDENTIALS` | Reuse existing `/app/credentials/your-gcp-project-*.json` | For Nanobanana |
| `GENAI_BASIC_AUTH_USER` / `GENAI_BASIC_AUTH_PASS` | Operator-chosen | Internal network user auth. Returns 503 if not set (or bypass with `GENAI_AUTH_DISABLED=true`) |
| `GENAI_INTERNAL_TOKEN` | Arbitrary secret (32+ chars recommended) | For Dagster sensor ↔ genai container communication |

After entering keys:

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline_test/docker
docker compose --profile genai up -d genai
docker compose restart dagster dagster-code-server
```

## 2. End-to-End Validation (mocking → real keys)

### 2-1. Mocking Mode End-to-End

All flows can be validated even without keys set.

```bash
# Start containers
docker compose --profile genai up -d genai

# Access UI from internal network
http://10.0.0.10:8088/

# Or call directly via curl
curl -u user:pass -F "engine=kling" -F "prompt=cat jumping" \
  -F "files=@/path/to/img1.png" -F "files=@/path/to/img2.png" \
  http://10.0.0.10:8088/genai/batches
```

Verify:
- Rows created in Postgres `genai_batches` / `genai_jobs`
- Files atomically written to `/nas/staging/incoming/genai/<batch_id>/originals/`
- (Kling/Higgsfield) `outputs/<seq>.mp4` (placeholder) appears ~2 seconds later
- (Nanobanana/GPT Image) `outputs/<seq>.png` appears immediately on submit
- INGEST sensor picks up → `raw_files` contains `genai_source` / `genai_output` rows
- When build asset runs: paired manifest created at `vlm-dataset/genai_<batch_id>/`

### 2-2. Real Key Validation (once per engine)

Minimize cost by using 1 batch (1 image input) per engine:

```bash
# Kling
curl ... -F "engine=kling" -F "prompt=test" -F "files=@cat.png" /genai/batches
# After ~5-10 minutes (real API response): outputs/001.mp4 settled + status='done'

# Higgsfield
curl ... -F "engine=higgsfield" ...

# Nanobanana — synchronous, immediate response
curl ... -F "engine=nanobanana" ...

# GPT Image — synchronous, immediate response
curl ... -F "engine=gpt_image" ...
```

## 3. Dagster Sensor Activation

Switch `genai_poll_sensor` to **manually ON** in Dagster UI (`http://10.0.0.10:3031`).
It does not start automatically as default_status=STOPPED.

Status indicators:
- Normal: `SkipReason: no GenAI jobs to poll` every 30s
- Async batch in progress: `polled=N done=X failed=Y running=Z`
- Token not set: `GENAI_INTERNAL_TOKEN not set — sensor inactive`
- Container down: `genai container query failed`

## 4. Enable Operational Limits (optional)

Default is 0 = off. Assumed to be internal network trusted. Enable if exposed externally or concerned about cost runaway:

```env
GENAI_RATE_LIMIT_PER_MIN=10        # 10 requests per user per minute
GENAI_DAILY_BATCH_LIMIT=50         # 50 batches per user per day
GENAI_DAILY_BYTES_LIMIT=5368709120 # 5GB per user per day
```

Returns 429 when exceeded.

⚠ **Single-container assumption** (Codex Q1 MED) — `GENAI_RATE_LIMIT_PER_MIN` uses an in-memory
sliding window, so it is isolated per genai container instance. If scaling out workers, migrate to
a Postgres/Redis-based distributed rate state. Current staging/prod policy is single container →
can be used as-is.

## 5. Cost Monitoring

Via `/genai/costs?range=day|week|month` UI or:

```sql
-- 7-day aggregation by engine
SELECT b.engine,
       COUNT(j.*) FILTER (WHERE j.status='done') AS done,
       COUNT(j.*) FILTER (WHERE j.status='failed') AS failed,
       SUM(j.cost_units) AS total_cost
  FROM genai_jobs j
  JOIN genai_batches b ON b.batch_id = j.batch_id
 WHERE j.submitted_at > now() - interval '7 days'
 GROUP BY b.engine;
```

Accuracy of cost_units per real API call depends on the adapter's cost extraction logic. Precise calculation is a Phase 6 follow-up.

## 6. Partial Failure Retry

The `retry` button is visible in the batch detail UI (failed jobs only).
Or:

```bash
curl -u user:pass -X POST http://10.0.0.10:8088/genai/jobs/<job_id>/retry
```

The original input image must still be on NAS (returns 410 if already archived).
Retry issues a new provider_job_id + status='submitted'.

## 7. Main Promotion Checklist

After staging stabilization on dev, before creating the main PR:

- [ ] `KLING_*` / `FAL_KEY` / `OPENAI_API_KEY` / `GEMINI_*` all entered in prod `docker/.env`
- [ ] `GENAI_INTERNAL_TOKEN` differs between prod and staging
- [ ] `GENAI_BASIC_AUTH_*` filled in or `GENAI_AUTH_DISABLED=false` explicit
- [ ] `GENAI_RATE_LIMIT_PER_MIN` / `GENAI_DAILY_*` production policy decided
- [ ] `HIGGSFIELD_FAL_MODEL` replaced with verified ID
- [ ] End-to-end success once per engine, all 4 (real keys)
- [ ] Dagster `genai_poll_sensor` ON + 24 hours stable (SkipReason normal flow)
- [ ] `002_genai.sql` applied to Postgres + 4 indexes confirmed present
- [ ] `vlm-genai-jobs` MinIO bucket not used (NAS only — v3 decision)
- [ ] All Phase 5 checklist items in `docs/genai_rollout/plan.md` are ✓
- [ ] dev → main PR. Body contains "GenAI Studio v0.1 — 4 engines" + commit range (51433041 ~ HEAD)
- [ ] PR review passed + squash merge → auto-triggers deploy-production.yml

⚠ **Prohibited actions reminder** (CLAUDE.md): do not manually edit `src/` / `configs/` /
`scripts/` / `docker-compose*.yaml` / `Dockerfile` on the prod host — any such edits will be
lost by the next CI deployment's `rsync --delete` + `git reset --hard`. **Changes must go through
git commit → push → CI automatic deployment only.** `.env` / `.env.test` are the only files
that may be edited directly on the host (not git-tracked).

## 8. Troubleshooting

| Symptom | Cause / Action |
|---------|----------------|
| 503 on UI access | `GENAI_BASIC_AUTH_USER/PASS` not set. Or specify `GENAI_AUTH_DISABLED=true` |
| 401 on submit | Basic auth credentials mismatch |
| 415 on submit | File extension not in whitelist (png/jpg/webp only) |
| 413 on submit | File size / batch count exceeded |
| 429 on submit | Rate-limit or daily quota exceeded |
| Job status stuck at 'pending' | adapter.submit() failed or PG transaction rolled back — check `docker logs genai` |
| Job status stuck at 'submitted' | Dagster `genai_poll_sensor` is STOPPED. Manually turn ON in UI or call `/internal/jobs/.../poll` directly |
| `outputs/_manifest.json` not created | Not all jobs are in done/failed state. Check `genai_jobs` status |
| Build asset not picking up GenAI batch | source_unit_name='genai_<batch_id>' mismatch or other value. Or `find_project_genai_pairs` has input/output asset_id=NULL → check ops_register manifest items[].seq matching warn |
| nanobanana failing with "block_reason" | Safety filter — change prompt/image |
| Higgsfield submit 'model not found' | `HIGGSFIELD_FAL_MODEL` ID wrong. Check fal.ai dashboard |

## 9. Known Constraints / Phase 6 Follow-up Candidates

- `fal-ai/higgsfield/i2v-soul` model ID confirmation verification needed
- Vertex SA prod permission separation (currently staging/prod share `your-gcp-project`)
- BackgroundTask finalize may be missed on container restart — after actual production stabilization, consider extending sensor to also handle fallback finalize
- Partial retry cannot handle originals that have been archived — consider adding a path to recover from archive
- `GENAI_DAILY_BYTES_LIMIT` SQL uses regex parsing on options_json — separate column recommended later
