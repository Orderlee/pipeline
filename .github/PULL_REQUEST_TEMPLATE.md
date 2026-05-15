<!--
PR authoring guide: see docs/git-workflow-guide.md "Quick Reference (cheat card)" section
-->

## Target Branch

- [ ] **`dev`** — to be validated on staging (:3031). A separate `dev → main` PR is required after validation passes.
- [ ] **`main`** — one of the following:
  - [ ] Merging commits already validated on dev
  - [ ] Hotfix (branched from main) — **`main → dev` back-merge required** after merge (prevents regression in next release)

## What Changed

<!-- 1–3 sentences on what and why. Leave out the how — the diff shows that. -->

## Deploy Impact

- [ ] `src/vlm_pipeline/**` changes — rsync only, no image rebuild (fast)
- [ ] `Dockerfile` / `docker/app/**` / `configs/**` / `scripts/**` / `gcp/**` / `split_dataset/**` / `src/python/**` changes — **image rebuild required** (slow)
- [ ] Only `*.md` (root) / `docs/**` / `tests/**` changes — `paths-ignore` suppresses CI trigger (expected)
- [ ] Env change required — `.env`/`.env.test` are git-untracked. Edit on host directly + restart Dagster for the affected environment.

## Validation

- [ ] Feature confirmed locally or on staging (evidence/screenshot if applicable)
- [ ] Relevant unit tests pass (`pytest tests/unit -q`)
- [ ] If DuckDB schema/query changed, sample-checked with `scripts/query_local_duckdb.py`
- [ ] If new sensor/asset/schedule, tick log or run materialization confirmed

## Post-Deploy Checklist (do this yourself after merge)

- [ ] [Actions run](https://github.com/Orderlee/Datapipeline-Data-data_pipeline/actions) success
- [ ] Target environment Dagster UI returns 200 OK
  - PROD: http://10.0.0.10:3030/
  - STAGING: http://10.0.0.10:3031/
- [ ] Code location **LOADED** (no PythonError)
- [ ] Asset/sensor/schedule in the changed scope behaves correctly
- [ ] (Hotfix) `main → dev` back-merge complete
- [ ] (After production is stable post-main merge) Upstream contribution: `./tools/pr-to-upstream.sh`

## Related Issues / References

<!-- Linear / GitHub Issue links, reference docs, prior PRs, etc. -->
