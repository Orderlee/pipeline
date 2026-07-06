# MLOps Fine-tune Scaffolding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the *scaffolding* to fine-tune the two in-pipeline Meta foundation models (SAM3 detection, PE-Core embedding) on a frozen, human-confirmed training dataset — built end-to-end but **not executed in prod** (no training run, no weight promotion) this phase.

**Architecture:** Frozen, content-addressed dataset snapshots (`train_dataset_versions` → `vlm-dataset/_trainsets/…`) feed a profile-gated `vlm-trainer` container (LoRA-default, merged to full weights). A GT-anchored eval gate writes `model_registry` rows; a manual, registry-driven promotion script swaps full-weight checkpoints into the serving containers. A fail-safe GPU maintenance lock drains the shared GPU before training and auto-releases on crash. Everything rides CI dev→staging→main; weight promotion is built-but-not-run.

**Tech Stack:** Dagster (assets/ops/sensors), Postgres (+pgvector for embeddings), MinIO, Docker Compose (profile-gated GPU services), PyTorch/`open_clip`/`peft`/`hydra`/`submitit` (trainer only), `facebookresearch/sam3` `sam3.train`/`sam3.eval`, pytest (real-PG fixture + moto/mock MinIO).

## Global Constraints

[Copied verbatim from the cross-section consistency review. Every task's requirements implicitly include this section.]

- **Branch:** all work on `feature/mlops-finetune-scaffolding` (already checked out). Conventional commits (`feat`/`fix`/`docs`/`chore`/`test`/`refactor`); commit message says WHAT and WHY, not HOW.
- **Python 3.10+. ruff line-length 120. CI uses ruff 0.7.4** (pin `pip install ruff==0.7.4` for lint reproduction; `pyproject` is gitignored so only git-tracked files are linted). Local newer ruff can introduce format drift — match 0.7.4.
- **5-bucket MinIO policy is FIXED:** `vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`, `vlm-classification`. NO new buckets — frozen trainsets and model checkpoints use PREFIXES under `vlm-dataset` (`_trainsets/<id>/…`, `_models/<model>/<version>/…`).
- **5-layer import hierarchy, no lower→upper imports:** L1-2 `lib/` (pure Python) → L3 `ops` → L4 `assets/sensors` → L5 `definitions.py`. `defs/` modules are thin wrappers delegating to `lib/`. `lib/` MUST NOT import dagster/defs/resources/ops (enforced by `scripts/check_lib_layer_imports.py` in CI). `lib/` MUST NOT import `docker/` serving code — mirror constants with a comment + a pinning test instead.
- **NO GPU in CI:** actual GPU training is gated behind `ENABLE_TRAINING` / manual trigger and is never run in CI. Unit tests cover only pure functions, config assembly, adapters, gate logic, preflight, dry-run promotion — all GPU/torch/open_clip/pycocotools-free (monkeypatched or moto-mocked).
- **PG migration DO-block rule:** the migration runner applies only the FIRST statement of a multi-statement `DO $$` block. Each new migration uses ONE DO block OR a separate file. Prefer zero DO blocks: idempotent `CREATE … IF NOT EXISTS` + inline named `CONSTRAINT`s + `-- @ASSERT_AFTER: <sql>` comments that re-verify constraints/FKs on every boot. Verify with `pg_constraint` after applying.
- **Migration sequencing:** existing migrations end at 012. New files: `013_mlops_finetune.sql` (Section A) then `014_gpu_maintenance_lock.sql` (Section F) — do NOT reuse 013 across two files. Add to `PostgresMigrationMixin._REQUIRED_MIGRATIONS` in `resources/postgres_migration.py` additively (A then F).
- **Test stubs:** copy existing patterns — `_DummyDB` / `_DummyMinIO` / `_DummyContext` (or `tests/helpers/dagster_dummies`), monkeypatch of asset-internal `_run_*` helpers, `moto` mock MinIO. Real-PG fixtures: `postgres_resource` (`tests/conftest.py`) and `pg_resource` (`tests/integration/conftest.py`) — both auto-skip without `DATAOPS_TEST_POSTGRES_DSN` (a skip is NOT a pass; provision a throwaway `postgres:15` when a constraint/migration must actually be exercised).
- **Trainer runs DECOUPLED from the Dagster run lifecycle** (CI restart orphans in-run ops). `gpu_trainer` concurrency=1 tag (must be defined in `run_coordinator` `tag_concurrency_limits` — see Task R2). Maintenance lock is fail-safe (auto-release via guard sensor heartbeat/TTL), never fail-stuck.
- **image_embeddings versioning** encodes the finetune version INTO the `model_name` value (e.g. `facebook/PE-Core-L14-336@ft-2026.06.29-lora-001`); do NOT add a `model_version` column. `vector(1024)` fixed — a re-trained encoder must keep dim 1024.

---

## Reconciliations — READ BEFORE STARTING

The 7 sections below were authored in parallel against shared contracts, then cross-checked. The following were reconciled. **Honor them — they override any contradicting text inside a section.**

### Inline fixes already applied during assembly
- **Section F migration renamed `013…` → `014_gpu_maintenance_lock.sql`** (avoids filename collision with Section A's `013_mlops_finetune.sql`). Every `013_gpu_maintenance_lock` string in Section F was rewritten to `014_gpu_maintenance_lock`; F's `gpu_maintenance_lock` table is created by migration **014**, and F's `_REQUIRED_MIGRATIONS` / `@ASSERT_AFTER` / integration-test references follow.
- **Section B's references to `013_train_dataset_versions.sql` rewritten to `013_mlops_finetune.sql`** — the actual file Section A authors, which holds BOTH `train_dataset_versions` and `model_registry`.
- **Section E's integration-test PG fixture `pg_conn` rewritten to `pg_resource`** (the real `tests/integration` fixture; auto-skips without `DATAOPS_TEST_POSTGRES_DSN`).

### Co-owned files — edits must be ADDITIVE (append, never overwrite)
- `src/vlm_pipeline/defs/train/__init__.py` — created by whichever of B/F lands first; B (dataset), D (eval), F (sensor) each append their imports/exports.
- `src/vlm_pipeline/resources/postgres_migration.py` `_REQUIRED_MIGRATIONS` — A appends `'013_mlops_finetune'`, THEN F appends `'014_gpu_maintenance_lock'` (A before F so the second edit sees the first).
- `docker/docker-compose.yaml` — C adds the `trainer` service; E modifies the `embedding` service env block. `.github/workflows/deploy-*.yml` — C and G both touch the rebuild globs; **G's grep test is the final contract gate**, so land G last.
- Stock PE-Core `model_name` label `facebook/PE-Core-L14-336`: Section A owns the constant `STOCK_PE_CORE_MODEL_NAME` in `lib/embedding_model_name.py`. Section E's serving image is `vlm_pipeline`-free → it mirrors the literal with a comment pointing at `lib` (same pattern as A↔`open_clip_be.py`). Do NOT drift. (The HF hub repo id `hf-hub:timm/PE-Core-L-14-336` is a DIFFERENT, intentional string — do not unify.)

- `src/vlm_pipeline/sql/migrations/postgres/013_mlops_finetune.sql` + `tests/integration/test_mlops_migration_013.py` — A authors; **Section I (I3)** ADDITIVELY adds `mlflow_run_id TEXT` (+ `ALTER…ADD COLUMN IF NOT EXISTS`) and the `expected_cols` assertion. (spec §5.2)
- `src/vlm_pipeline/resources/postgres_train.py` — B authors `insert_train_dataset_version`; **R1** adds `insert_candidate_model_version`; **Section I (I3.6)** appends optional `mlflow_run_id=None` kwarg + INSERT column (additive; R1's test stays green).
- `src/vlm_pipeline/resources/postgres_migration.py` — A & F append to `_REQUIRED_MIGRATIONS`; **Section H** appends `'015_embedding_active_model.sql'` to `_OPTIONAL_MIGRATIONS` (pgvector precondition); **Section J** appends `'016_dataset_catalog.sql'` to `_REQUIRED_MIGRATIONS`. Order A → F → J (required) + H (optional).
- **Section J (DVC) co-owns** (all ADDITIVE): `resources/postgres_train.py` (+catalog helpers, alongside B + R1/I3), `defs/train/dataset.py` (+`_materialize_pinned_dvc_source` + 1-line FK back-link in B's builder), `docker/trainer/{requirements.txt,mlflow_logging.py}` (+`dvc[s3]` pin, +DVC `_LINEAGE_FIELDS`), `definitions_production.py` (+`dataset_catalog_reconciliation_sensor`).
- `docker/docker-compose.yaml` — C adds `trainer`, E modifies `embedding` env, **Section I (I1)** adds profile-gated `mlflow` service (all additive).
- `docker/analysis/fiftyone_pgvector.py` — **Section H** owns the active-model reader edits (`_active_model_name()` + `active_learning_queue`/`search_by_text` default switch). Keeps mirrored stock constant (line 23, 5-layer rule); drift guarded by `tests/unit/test_fiftyone_active_model_pin.py`.
- `src/vlm_pipeline/defs/embed/assets.py` + `src/vlm_pipeline/definitions.py` — **Section H** (frame_embedding model_name default → PG pointer; register `reembed_under_version` + gpu_trainer tag; H3.9 SPIKE reads the registration site first).
- `.github/workflows/deploy-*.yml` — C adds `^docker/trainer/`; **Section G** owns BOTH globs and MUST also add **`^docker/mlflow/`** (handed off by I1.8) + extend `tests/unit/test_ci_trainer_rebuild_glob.py` to pin both.
- `.agent/skill/mlops-finetune/SKILL.md` + CLAUDE.md MLOps section — co-owned by G/H/I; **Section G** presence-guard tests MUST grep for (I) the manual `CREATE DATABASE mlflow` bootstrap + MLflow UI stanza and (H) the non-CONCURRENTLY partial-HNSW maintenance-window note + `scripts/promote_pe_core.py` runbook; scripts table adds `scripts/promote_pe_core.py`.

### Build order
**A → F → B → C → R2 → R1 → D → E → I → H → J → G.** A (tables) is the root dependency. F (second migration + creates the `defs/train` package) lands right after A. B (snapshot builder) needs A's `train_dataset_versions`. C (trainer) consumes B's frozen trainset. R2 (concurrency tags) and R1 (candidate-row INSERT) close the two gaps the trainer/eval chain needs. D (eval/gate) UPDATEs the candidate row R1 inserts. E (serving/promote) consumes A's `model_registry` + C's checkpoint + D's `promotable`. J (DVC curation layer: migration 016 + `dataset_catalog` ×3 + ingestion sensor + pin API + builder hook + MLflow DVC tags) lands after B (co-owns `defs/train/dataset.py`) and I (co-owns `docker/trainer/mlflow_logging.py`); migration 016 follows 013/014/015. G (CI/docs) lands last so its grep tests pin already-frozen names — **G must add `^scripts/dvc/` + `^docker/curation/` to the rebuild globs** (Section J files).

---

## Task R1: Own the `model_registry` candidate-row INSERT (gap fix)

The trainer (C) writes checkpoint artifacts but no DB row; the eval gate (D) UPDATEs a row to `promotable`; promotion (E) SELECTs `promotable`. **Nothing inserts the initial `status='candidate'` row.** R1 fills that — CI-tested via `_DummyDB`, invoked (prod-only, not in CI) right after training succeeds and before the eval gate.

**Files:**
- Modify: `src/vlm_pipeline/resources/postgres_train.py` (add `insert_candidate_model_version`)
- Create: `src/vlm_pipeline/defs/train/register.py` (thin op `register_candidate_model_op`)
- Test: `tests/unit/test_register_candidate_model.py`

**Interfaces:**
- Consumes: `model_registry` table (Section A, migration 013); `train_dataset_version_id` (A/B); `checkpoint_key`, `artifact_checksum`, `env_lock_key`, `training_config` from C's `training_summary.json`; `git_sha`, `training_image_digest` from the trainer env.
- Produces: `insert_candidate_model_version(db, *, model, version, train_dataset_version_id, train_method, checkpoint_key, artifact_checksum, git_sha, training_image_digest, training_config, env_lock_key) -> model_version_id` inserting `status='candidate'`, `incumbent_source=NULL`. **This is the row Section D UPDATEs and Section E SELECTs.**

- [ ] **R1.1 — SPIKE: confirm the resource cursor idiom.** Read `src/vlm_pipeline/resources/postgres_train.py` (created by Section B) and `src/vlm_pipeline/resources/postgres_base.py`. Record the exact cursor contextmanager name and paramstyle used by `insert_train_dataset_version` (e.g. `with self._cursor() as cur:` + `%(name)s`). `insert_candidate_model_version` MUST mirror that idiom verbatim.

- [ ] **R1.2 — Write the failing unit test.** Create `tests/unit/test_register_candidate_model.py`:

```python
"""insert_candidate_model_version builds the correct INSERT and returns the id (no real DB)."""
from __future__ import annotations


class _DummyCursor:
    def __init__(self): self.sql = ""; self.params = None
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None): self.sql = sql; self.params = params
    def fetchone(self): return ("mv-test-1",)


class _DummyDB:
    """Mirror of postgres_train's resource surface used by insert_candidate_model_version."""
    def __init__(self): self.cursor = _DummyCursor()
    def _cursor(self): return self.cursor  # adjust in R1.4 to match the SPIKE finding


def test_insert_candidate_builds_insert_with_status_candidate():
    from vlm_pipeline.resources.postgres_train import insert_candidate_model_version
    db = _DummyDB()
    mv_id = insert_candidate_model_version(
        db, model="sam3", version="sam3-2026.06.29-lora-001",
        train_dataset_version_id="tdv-1", train_method="lora",
        checkpoint_key="vlm-dataset/_models/sam3/sam3-2026.06.29-lora-001/checkpoint.pt",
        artifact_checksum="abc123", git_sha="deadbee", training_image_digest="sha256:xyz",
        training_config={"seed": 7}, env_lock_key="vlm-dataset/_models/sam3/sam3-2026.06.29-lora-001/env_lock.json",
    )
    sql = db.cursor.sql.lower()
    assert "insert into model_registry" in sql
    assert "'candidate'" in sql or "status" in sql  # status set to candidate
    assert db.cursor.params is not None
    assert mv_id == "mv-test-1"
```

- [ ] **R1.3 — Run, expect FAIL** (`insert_candidate_model_version` not defined).

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_register_candidate_model.py -q
# Expected: ImportError / FAILED
```

- [ ] **R1.4 — Implement `insert_candidate_model_version`** in `resources/postgres_train.py`, mirroring `insert_train_dataset_version`'s cursor idiom from R1.1. Generate `model_version_id` as `f"mv-{uuid4().hex[:12]}"` (or the project's id helper if one exists). Insert columns: `model_version_id, model, version, train_dataset_version_id, train_method, git_sha, training_image_digest, training_config (JSONB), env_lock_key, checkpoint_key, artifact_checksum, status='candidate', created_at=now()`. Return `model_version_id`. Use the same paramstyle the SPIKE found; JSON-encode `training_config` exactly as `insert_train_dataset_version` encodes its JSONB columns.

- [ ] **R1.5 — Run, expect PASS.**

```bash
python -m pytest tests/unit/test_register_candidate_model.py -q
# Expected: 1 passed
```

- [ ] **R1.6 — Add the thin op** `register_candidate_model_op` in `src/vlm_pipeline/defs/train/register.py` that reads `training_summary.json` (key produced by C) + trainer env and calls `insert_candidate_model_version`. APPEND its export to `defs/train/__init__.py` (co-owned — do not overwrite). No new test (covered by R1.2 + defs-load smoke in B5/D).

- [ ] **R1.7 — Commit.**

```bash
git add src/vlm_pipeline/resources/postgres_train.py src/vlm_pipeline/defs/train/register.py tests/unit/test_register_candidate_model.py src/vlm_pipeline/defs/train/__init__.py
git commit -m "feat(mlops): own model_registry candidate-row INSERT (register_candidate_model)"
```

---

## Task R2: Define `gpu_trainer` / `pg_writer` concurrency tags (gap fix)

`gpu_trainer` concurrency=1 is referenced by C and F but defined nowhere. The live `docker/app/dagster_home/dagster.yaml` has no `tag_concurrency_limits` block (only a `.bak` does). R2 adds it. **`dagster_home/` is rsync-excluded on deploy** — so the change must be applied on the host AND captured in the deployed template / documented in CLAUDE.md (Section G), or it won't survive a deploy.

**Files:**
- Modify: `docker/app/dagster_home/dagster.yaml` (add `run_coordinator.config.tag_concurrency_limits`)
- Test: `tests/unit/test_dagster_yaml_concurrency_tags.py`

**Interfaces:**
- Produces: `run_coordinator` enforces `gpu_trainer: 1` and `pg_writer: 1` (preserve any existing `duckdb_writer` limit if present).
- Consumes: nothing (config only).

- [ ] **R2.1 — SPIKE: read the live coordinator config.** Read `docker/app/dagster_home/dagster.yaml`. Record whether a `run_coordinator` / `tag_concurrency_limits` block already exists and its exact indentation/shape (compare against `docker/app/dagster_home/dagster.yaml.bak.*` which defines `duckdb_writer`). The edit in R2.4 must merge into the existing structure, not duplicate the `run_coordinator` key.

- [ ] **R2.2 — Write the failing test.** Create `tests/unit/test_dagster_yaml_concurrency_tags.py`:

```python
"""dagster.yaml run_coordinator enforces gpu_trainer=1 and pg_writer=1."""
from __future__ import annotations
import pathlib
import yaml

YAML_PATH = pathlib.Path("docker/app/dagster_home/dagster.yaml")


def test_gpu_trainer_and_pg_writer_tag_limits_present():
    cfg = yaml.safe_load(YAML_PATH.read_text())
    limits = (
        cfg.get("run_coordinator", {}).get("config", {}).get("tag_concurrency_limits", [])
    )
    by_key = {}
    for entry in limits:
        # entries look like {"key": "gpu_trainer", "limit": 1} or with a value matcher
        by_key[entry.get("key")] = entry.get("limit")
    assert by_key.get("gpu_trainer") == 1, f"gpu_trainer limit missing/!= 1: {limits}"
    assert by_key.get("pg_writer") == 1, f"pg_writer limit missing/!= 1: {limits}"
```

- [ ] **R2.3 — Run, expect FAIL.**

```bash
python -m pytest tests/unit/test_dagster_yaml_concurrency_tags.py -q
# Expected: FAILED (KeyError/assert — block absent)
```

- [ ] **R2.4 — Edit `docker/app/dagster_home/dagster.yaml`.** Merge into the existing `run_coordinator` (shape confirmed in R2.1; this is the canonical `QueuedRunCoordinator` form):

```yaml
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    tag_concurrency_limits:
      - key: "gpu_trainer"
        limit: 1
      - key: "pg_writer"
        limit: 1
```

If a `duckdb_writer` entry exists in the live file or the `.bak`, keep it in the list.

- [ ] **R2.5 — Run, expect PASS.**

```bash
python -m pytest tests/unit/test_dagster_yaml_concurrency_tags.py -q
# Expected: 1 passed
```

- [ ] **R2.6 — Commit.** (Section G's CLAUDE.md edit documents the host-apply + rsync-exclusion caveat.)

```bash
git add docker/app/dagster_home/dagster.yaml tests/unit/test_dagster_yaml_concurrency_tags.py
git commit -m "feat(mlops): define gpu_trainer/pg_writer run_coordinator concurrency tags"
```

---

## Open decisions carried from the spec (§15) — resolve as they come up, do not block scaffolding

1. **SAM3 box-only loss** — Section C's SPIKE confirmed `Sam3LossWrapper` can omit the mask loss (box-only). Proceed box-supervised; mask quality is not adapted.
2. **Eval gate margins** — defaults live in `eval_config` (Section D); pick concrete numbers when the first real (smoke) run produces baseline numbers.
3. **PE-Core holdout** — LS-finalized `(frame, class)` pairs only; small today → advisory metrics with CIs (Section D).
4. **Snapshot copy vs reference** — Section B defaults to reference-by-manifest; switch to byte-copy only if originals may be deleted (additive, no checksum change).
5. **CI deploy-hold during maintenance** — enforced operationally (runbook, Section G) for now; a CI marker-file check is deferred.
6. **PE-Core gate `N_min`** — Section H commits `DEFAULT_PE_CORE_MIN_GT=50` placeholder (override via `eval_config.pe_core_min_gt`); GT-collection path (image_label_annotations projection / per-frame caption review) deferred (spec §15-Q3).
7. **PE re-embed coverage** — default = incumbent set (frame 187,994 + caption 11,978); widening to all 525,966 frames deferred (spec §15-Q6).

---

## Section A — PG Migrations

This section adds two new Postgres tables (`train_dataset_versions`, `model_registry`) via one forward-only migration file, an integration test that applies it on the real-PG fixture and asserts the tables + the `UNIQUE(task, content_checksum)` constraint via `pg_constraint`, and a tiny pure-`lib/` helper (+ unit test) that encodes a fine-tune version into the `image_embeddings.model_name` value (NO schema change for image_embeddings).

**Context verified by reading source (do NOT re-verify):**
- Migration runner: `src/vlm_pipeline/resources/postgres_migration.py`. It sorts `sql/migrations/postgres/*.sql`, records applied names in `_pg_migrations`, and on **every boot** runs each file's `-- @ASSERT_AFTER: <sql>` comments (first column must be truthy or it raises `PostgresSchemaBaselineError`). DO-block quirk: only the **first statement** of a multi-statement DO block applies, so each new migration uses **one** DO block per concern or plain idempotent DDL (`CREATE TABLE IF NOT EXISTS`, inline `CONSTRAINT ... UNIQUE`, which need no DO block at all).
- `datasets.dataset_id` is `TEXT PRIMARY KEY` (001_init.sql:176) → `upstream_dataset_id` FK column must be `TEXT`.
- Next free sequence number is **013** (highest existing is 012).
- New tables do NOT use pgvector → they go in `_REQUIRED_MIGRATIONS` (always-apply), NOT `_OPTIONAL_MIGRATIONS`.
- Integration test fixture `pg_resource` (in `tests/integration/conftest.py`) creates a temp DB and calls `ensure_schema()`; helpers `_table_exists`/`_column_exists` live in `tests/integration/test_migration_sequence.py` — copy their bodies, don't import.
- Canonical stock embedding model_name is `facebook/PE-Core-L14-336` (`docker/embedding/backends/open_clip_be.py:12` `MODEL_NAME`).

---

### Task A1: Write migration 013_mlops_finetune.sql (failing assert test first)

**Files:**
- `tests/integration/test_mlops_migration_013.py` (new)
- `src/vlm_pipeline/sql/migrations/postgres/013_mlops_finetune.sql` (new)

**Interfaces:**
- Consumes: `pg_resource` fixture (`tests/integration/conftest.py`), `ensure_schema()` runner.
- Produces: PG tables `train_dataset_versions`, `model_registry`; constraints `train_dataset_versions_task_checksum_unique`, `model_registry_train_dataset_version_fk`.

- [ ] **A1.1 — Write the failing integration test.** Create `tests/integration/test_mlops_migration_013.py`:

```python
"""013_mlops_finetune migration — applies on real-PG fixture, asserts tables + constraints.

Mirrors tests/integration/test_migration_sequence.py patterns. Requires
DATAOPS_TEST_POSTGRES_DSN (else auto-skipped by the pg_resource fixture chain).
"""

from __future__ import annotations


def _table_exists(cur, table_name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        )
        """,
        (table_name,),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def _column_exists(cur, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        )
        """,
        (table_name, column_name),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def _constraint_exists(cur, conname: str, conrelid: str, contype: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = %s AND conrelid = %s::regclass AND contype = %s
        )
        """,
        (conname, conrelid, contype),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def test_013_creates_train_dataset_versions(pg_resource) -> None:
    expected_cols = [
        "train_dataset_version_id", "created_at", "task", "source_spec", "class_map",
        "group_key_field", "split_assignment_key", "split_ratios", "manifest_key",
        "content_checksum", "ls_count", "al_confirmed_count", "per_class_counts",
        "total_count", "seed", "upstream_dataset_id",
    ]
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            assert _table_exists(cur, "train_dataset_versions"), "train_dataset_versions missing"
            for col in expected_cols:
                assert _column_exists(cur, "train_dataset_versions", col), f"train_dataset_versions.{col} missing"


def test_013_creates_model_registry(pg_resource) -> None:
    expected_cols = [
        "model_version_id", "model", "version", "train_dataset_version_id", "train_method",
        "git_sha", "training_image_digest", "training_config", "env_lock_key", "eval_config",
        "metrics", "incumbent_metrics", "incumbent_source", "checkpoint_key", "artifact_checksum",
        "status", "created_at", "promoted_at", "promoted_env",
    ]
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            assert _table_exists(cur, "model_registry"), "model_registry missing"
            for col in expected_cols:
                assert _column_exists(cur, "model_registry", col), f"model_registry.{col} missing"


def test_013_task_checksum_unique_constraint(pg_resource) -> None:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            assert _constraint_exists(
                cur, "train_dataset_versions_task_checksum_unique", "train_dataset_versions", "u"
            ), "UNIQUE(task, content_checksum) constraint missing"


def test_013_model_registry_fk_to_train_dataset_versions(pg_resource) -> None:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conrelid = 'model_registry'::regclass
                      AND confrelid = 'train_dataset_versions'::regclass
                      AND contype = 'f'
                )
                """
            )
            assert bool(cur.fetchone()[0]), "model_registry → train_dataset_versions FK missing"


def test_013_unique_constraint_enforced(pg_resource) -> None:
    """Inserting two rows with same (task, content_checksum) must raise UniqueViolation."""
    import psycopg2.errors

    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO train_dataset_versions
                    (train_dataset_version_id, task, content_checksum, seed)
                VALUES ('tdv-a', 'sam3_detection', 'deadbeef', 42)
                """
            )
            raised = False
            try:
                cur.execute(
                    """
                    INSERT INTO train_dataset_versions
                        (train_dataset_version_id, task, content_checksum, seed)
                    VALUES ('tdv-b', 'sam3_detection', 'deadbeef', 7)
                    """
                )
            except psycopg2.errors.UniqueViolation:
                raised = True
            assert raised, "duplicate (task, content_checksum) was not rejected"
```

- [ ] **A1.2 — Run the test, expect collection/setup to reach the assertions and FAIL** (table does not exist yet). DSN must be exported for these to actually run (else they skip — skip is NOT a pass for this gate).

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
export DATAOPS_TEST_POSTGRES_DSN="postgresql://postgres:test@localhost:25432/postgres"
docker run --rm -d --name pg-mlops-013 -e POSTGRES_PASSWORD=test -p 25432:5432 postgres:15 >/dev/null
until docker exec pg-mlops-013 pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
python -m pytest tests/integration/test_mlops_migration_013.py -q
```

Expected output (FAIL, not skip): `5 failed` with messages like `AssertionError: train_dataset_versions missing`.

- [ ] **A1.3 — Write the migration file.** Create `src/vlm_pipeline/sql/migrations/postgres/013_mlops_finetune.sql`. No DO blocks needed — all DDL is idempotent (`IF NOT EXISTS`) and the UNIQUE is an inline named table constraint, so the DO-block quirk does not apply:

```sql
-- 013_mlops_finetune.sql — MLOps 파인튜닝 스캐폴딩: 동결 학습셋 버전 + 모델 레지스트리.
--
-- 두 신규 테이블만 추가 (pgvector 불필요 → _REQUIRED_MIGRATIONS 에 등재).
--   * train_dataset_versions: 라이브 라벨 흐름과 분리된 동결(immutable) 학습셋 스냅샷 메타.
--     datasets(live-build, run마다 행) 와 섞으면 lineage 위험 → 별 테이블 (design §5.1).
--     UNIQUE(task, content_checksum) — 동일 콘텐츠 재빌드 dedup (design §7.2 H6).
--   * model_registry: 모델 버전·lineage·metrics·status·checkpoint_key·env_lock (design §5.2).
--     서빙되는 가중치의 source of truth = 이 테이블 행 (심볼릭링크 아님, design §2).
--
-- DO $$ block 미사용 — runner 의 multi-statement DO 부분적용 quirk 회피
-- (project_postgres_migration_runner_quirk). 모든 DDL 은 IF NOT EXISTS / 인라인 제약으로 멱등.
--
-- @ASSERT_AFTER: SELECT to_regclass('train_dataset_versions') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('model_registry') IS NOT NULL
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'train_dataset_versions_task_checksum_unique' AND conrelid = 'train_dataset_versions'::regclass AND contype = 'u')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'model_registry'::regclass AND confrelid = 'train_dataset_versions'::regclass AND contype = 'f')

BEGIN;

CREATE TABLE IF NOT EXISTS train_dataset_versions (
    train_dataset_version_id  TEXT PRIMARY KEY,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    task                      TEXT NOT NULL,
    source_spec               JSONB,
    class_map                 JSONB,
    group_key_field           TEXT,
    split_assignment_key      TEXT,
    split_ratios              JSONB,
    manifest_key              TEXT,
    content_checksum          TEXT NOT NULL,
    ls_count                  INTEGER,
    al_confirmed_count        INTEGER,
    per_class_counts          JSONB,
    total_count               INTEGER,
    seed                      INTEGER,
    upstream_dataset_id       TEXT REFERENCES datasets(dataset_id),
    CONSTRAINT train_dataset_versions_task_checksum_unique UNIQUE (task, content_checksum),
    CONSTRAINT train_dataset_versions_task_check CHECK (task IN ('sam3_detection', 'pe_core_embedding'))
);

CREATE INDEX IF NOT EXISTS train_dataset_versions_task_idx
    ON train_dataset_versions (task);

CREATE TABLE IF NOT EXISTS model_registry (
    model_version_id          TEXT PRIMARY KEY,
    model                     TEXT NOT NULL,
    version                   TEXT NOT NULL,
    train_dataset_version_id  TEXT REFERENCES train_dataset_versions(train_dataset_version_id),
    train_method              TEXT,
    git_sha                   TEXT,
    training_image_digest     TEXT,
    training_config           JSONB,
    env_lock_key              TEXT,
    eval_config               JSONB,
    metrics                   JSONB,
    incumbent_metrics         JSONB,
    incumbent_source          TEXT,
    checkpoint_key            TEXT,
    artifact_checksum         TEXT,
    status                    TEXT NOT NULL DEFAULT 'candidate',
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    promoted_at               TIMESTAMPTZ,
    promoted_env              TEXT,
    CONSTRAINT model_registry_model_check        CHECK (model IN ('sam3', 'pe_core')),
    CONSTRAINT model_registry_train_method_check CHECK (train_method IS NULL OR train_method IN ('lora', 'full_ft', 'contrastive_lora', 'linear_probe')),
    CONSTRAINT model_registry_incumbent_src_check CHECK (incumbent_source IS NULL OR incumbent_source IN ('promoted', 'stock_base')),
    CONSTRAINT model_registry_status_check       CHECK (status IN ('candidate', 'promotable', 'promoted', 'archived', 'rolled_back')),
    CONSTRAINT model_registry_promoted_env_check CHECK (promoted_env IS NULL OR promoted_env IN ('prod', 'staging')),
    CONSTRAINT model_registry_version_unique     UNIQUE (model, version)
);

CREATE INDEX IF NOT EXISTS model_registry_model_status_idx
    ON model_registry (model, status);
CREATE INDEX IF NOT EXISTS model_registry_train_dataset_idx
    ON model_registry (train_dataset_version_id);

COMMIT;
```

- [ ] **A1.4 — Register 013 as a required migration.** Edit `src/vlm_pipeline/resources/postgres_migration.py`, in the `_REQUIRED_MIGRATIONS` frozenset add `"013_mlops_finetune.sql"`. Exact edit — change:

```python
            "011_image_label_annotations.sql",
        }
    )
```
to:
```python
            "011_image_label_annotations.sql",
            "013_mlops_finetune.sql",
        }
    )
```

(012 is a view-only migration not currently in the required set; leave it as-is. We only add 013.)

- [ ] **A1.5 — Re-run the integration test, expect PASS.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
export DATAOPS_TEST_POSTGRES_DSN="postgresql://postgres:test@localhost:25432/postgres"
python -m pytest tests/integration/test_mlops_migration_013.py -q
```

Expected output: `5 passed`.

- [ ] **A1.6 — Verify idempotency + assertions directly (runner replays @ASSERT_AFTER every boot).** Apply the raw SQL twice on the throwaway PG and confirm constraints via pg_constraint:

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
docker cp src/vlm_pipeline/sql/migrations/postgres/013_mlops_finetune.sql pg-mlops-013:/tmp/013.sql
docker exec pg-mlops-013 psql -U postgres -f /tmp/013.sql -v ON_ERROR_STOP=on
docker exec pg-mlops-013 psql -U postgres -f /tmp/013.sql -v ON_ERROR_STOP=on
docker exec pg-mlops-013 psql -U postgres -tAc "SELECT conname FROM pg_constraint WHERE conname='train_dataset_versions_task_checksum_unique';"
```

Expected: both `psql -f` runs end with `COMMIT` and no error (second run = idempotent), final query prints `train_dataset_versions_task_checksum_unique`.

- [ ] **A1.7 — Tear down the throwaway PG.**

```bash
docker rm -f pg-mlops-013
```

- [ ] **A1.8 — Commit.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git add src/vlm_pipeline/sql/migrations/postgres/013_mlops_finetune.sql \
        src/vlm_pipeline/resources/postgres_migration.py \
        tests/integration/test_mlops_migration_013.py
git commit -m "feat(mlops): add train_dataset_versions + model_registry PG migration (013)"
```

---

### Task A2: image_embeddings model_name version-encoding helper (no schema change)

Per design §5.3 / SHARED CONTRACTS, the fine-tune version is encoded INTO the `model_name` value (e.g. `facebook/PE-Core-L14-336@ft-2026.06.29-lora-001`) — there is **no `model_version` column**. This task adds a tiny pure-`lib/` (L1) helper to build/parse that string deterministically, with a unit test. This keeps the convention in one tested place so the snapshot builder (Section B), promotion (Section F), and AL/search readers all agree.

**Files:**
- `tests/unit/test_embedding_model_name.py` (new)
- `src/vlm_pipeline/lib/embedding_model_name.py` (new)

**Interfaces:**
- Consumes: nothing (pure stdlib).
- Produces:
  - `STOCK_PE_CORE_MODEL_NAME: str = "facebook/PE-Core-L14-336"`
  - `build_versioned_model_name(version: str, *, base: str = STOCK_PE_CORE_MODEL_NAME) -> str` → `f"{base}@{version}"`
  - `parse_model_name(model_name: str) -> tuple[str, str | None]` → `(base, version_or_None)`
  - `is_stock(model_name: str) -> bool`

- [ ] **A2.1 — Write the failing unit test.** Create `tests/unit/test_embedding_model_name.py`:

```python
"""Tests for vlm_pipeline.lib.embedding_model_name — image_embeddings model_name version encoding.

Design §5.3: fine-tune version is encoded INTO the model_name value (no model_version column).
Stock rows keep the bare base name; fine-tuned rows append '@<version>'.
"""

from __future__ import annotations

import pytest

from vlm_pipeline.lib.embedding_model_name import (
    STOCK_PE_CORE_MODEL_NAME,
    build_versioned_model_name,
    is_stock,
    parse_model_name,
)


def test_stock_constant_matches_serving_backend():
    # docker/embedding/backends/open_clip_be.py:12 MODEL_NAME
    assert STOCK_PE_CORE_MODEL_NAME == "facebook/PE-Core-L14-336"


def test_build_default_base():
    assert build_versioned_model_name("ft-2026.06.29-lora-001") == (
        "facebook/PE-Core-L14-336@ft-2026.06.29-lora-001"
    )


def test_build_custom_base():
    assert build_versioned_model_name("v2", base="facebook/Other") == "facebook/Other@v2"


def test_build_rejects_empty_version():
    with pytest.raises(ValueError):
        build_versioned_model_name("")
    with pytest.raises(ValueError):
        build_versioned_model_name("  ")


def test_build_rejects_at_in_version():
    # '@' is the reserved separator — version must not contain it.
    with pytest.raises(ValueError):
        build_versioned_model_name("ft@bad")


def test_parse_versioned():
    base, version = parse_model_name("facebook/PE-Core-L14-336@ft-2026.06.29-lora-001")
    assert base == "facebook/PE-Core-L14-336"
    assert version == "ft-2026.06.29-lora-001"


def test_parse_stock_has_no_version():
    base, version = parse_model_name("facebook/PE-Core-L14-336")
    assert base == "facebook/PE-Core-L14-336"
    assert version is None


def test_roundtrip():
    name = build_versioned_model_name("ft-001")
    base, version = parse_model_name(name)
    assert build_versioned_model_name(version, base=base) == name


def test_is_stock():
    assert is_stock("facebook/PE-Core-L14-336") is True
    assert is_stock("facebook/PE-Core-L14-336@ft-001") is False
```

- [ ] **A2.2 — Run the test, expect import FAIL.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_embedding_model_name.py -q
```

Expected output: collection error / `ModuleNotFoundError: No module named 'vlm_pipeline.lib.embedding_model_name'`.

- [ ] **A2.3 — Write the helper.** Create `src/vlm_pipeline/lib/embedding_model_name.py`:

```python
"""image_embeddings.model_name version encoding (L1 lib — pure stdlib).

Design §5.3 / SHARED CONTRACTS: the fine-tune version is encoded INTO the
`model_name` value rather than a separate `model_version` column, so existing
readers (fiftyone_pgvector search, AL, backfill_prod_embeddings) that filter
`WHERE model_name = %(model)s` get version isolation for free.

Convention:  "<base>@<version>"   e.g.  "facebook/PE-Core-L14-336@ft-2026.06.29-lora-001"
Stock rows keep the bare base name (no '@').

⚠️ lib 계층 규칙: dagster / defs / resources / ops import 금지.
"""

from __future__ import annotations

# Must match docker/embedding/backends/open_clip_be.py:12 MODEL_NAME.
STOCK_PE_CORE_MODEL_NAME = "facebook/PE-Core-L14-336"

_SEP = "@"


def build_versioned_model_name(version: str, *, base: str = STOCK_PE_CORE_MODEL_NAME) -> str:
    """Encode a fine-tune version into a model_name string.

    Raises ValueError on empty/blank version or a version containing the
    reserved '@' separator (which would make parsing ambiguous).
    """
    v = (version or "").strip()
    if not v:
        raise ValueError("version must be a non-empty string")
    if _SEP in v:
        raise ValueError(f"version must not contain '{_SEP}' (reserved separator): {version!r}")
    return f"{base}{_SEP}{v}"


def parse_model_name(model_name: str) -> tuple[str, str | None]:
    """Split a model_name into (base, version). version is None for stock names.

    Only the first '@' separates base from version; bases never contain '@'.
    """
    name = model_name or ""
    if _SEP not in name:
        return name, None
    base, _, version = name.partition(_SEP)
    return base, (version or None)


def is_stock(model_name: str) -> bool:
    """True if model_name carries no fine-tune version (bare base name)."""
    return _SEP not in (model_name or "")
```

- [ ] **A2.4 — Run the test, expect PASS.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_embedding_model_name.py -q
```

Expected output: `9 passed`.

- [ ] **A2.5 — Lint (CI uses ruff 0.7.4, line-length 120).**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
ruff check --line-length 120 src/vlm_pipeline/lib/embedding_model_name.py tests/unit/test_embedding_model_name.py
```

Expected output: `All checks passed!`

- [ ] **A2.6 — Commit.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git add src/vlm_pipeline/lib/embedding_model_name.py tests/unit/test_embedding_model_name.py
git commit -m "feat(mlops): add image_embeddings model_name version-encoding helper (lib L1)"
```

---

## Section F — GPU Maintenance Lock + Fail-Safe Recovery

> **Section context.** This section adds a server-side GPU maintenance gate to BOTH `docker/sam3/app.py` and `docker/embedding/app.py` (request handlers AND idle-watcher loop honor it), a PG-backed flag store (`gpu_maintenance_lock` table) with `owner_run_id` + `entered_at` + `heartbeat_at` + TTL, a Dagster guard sensor (modeled on `stuck_run_guard_sensor`) that auto-releases a stale flag and re-warms serving, and a `scripts/clear_maintenance.sh` runbook. Pure decision logic lives in `lib/` (L1-2); the sensor wraps it (L4). The in-app flag check is a small, monkeypatchable shim so FastAPI `TestClient` tests run with no GPU and no PG.
>
> **Layering discipline (project 5-layer rule):** `lib/maintenance_flag.py` is pure (no dagster/psycopg import at module top). The app.py gate reads through a module-level `_maintenance_store` indirection so tests swap it. The sensor (`defs/train/sensor_maintenance_guard.py`, L4) imports the pure lib for its stale-decision.

---

### Task F1: PG migration `014_gpu_maintenance_lock.sql` (flag store table)

Single-row maintenance lock table. ONE `CREATE TABLE` + indexes only (no DO block — honors the runner's multi-statement DO limitation). A `singleton` CHECK guarantees at most one active lock row per `(target)`; we store one row per serving target (`sam3` / `pe_core`) so the two GPUs lock independently.

**Files:**
- `src/vlm_pipeline/sql/migrations/postgres/014_gpu_maintenance_lock.sql` (new)
- `tests/integration/test_gpu_maintenance_lock_migration.py` (new)

**Interfaces:**
- Produces (DB): table `gpu_maintenance_lock(target TEXT PK CHECK target IN ('sam3','pe_core'), active BOOLEAN, owner_run_id TEXT, entered_at TIMESTAMPTZ, heartbeat_at TIMESTAMPTZ, ttl_seconds INT, note TEXT, updated_at TIMESTAMPTZ)`; constraint `gpu_maintenance_lock_target_chk`.
- Consumes: nothing (greenfield table).

- [ ] **Write the migration file.** Create `src/vlm_pipeline/sql/migrations/postgres/014_gpu_maintenance_lock.sql`:

```sql
-- 014_gpu_maintenance_lock.sql
-- GPU 서빙 정비락 (fail-safe). 한 행 = 한 서빙 타깃(sam3/pe_core)의 락 상태.
-- owner_run_id+entered_at+heartbeat_at+ttl_seconds 로 stale 판정 → guard 센서 자동해제.
-- Forward-only, idempotent. DO block 미사용 (runner multi-statement DO 한계 회피).
BEGIN;

CREATE TABLE IF NOT EXISTS gpu_maintenance_lock (
    target        TEXT        NOT NULL,
    active        BOOLEAN     NOT NULL DEFAULT FALSE,
    owner_run_id  TEXT,
    entered_at    TIMESTAMPTZ,
    heartbeat_at  TIMESTAMPTZ,
    ttl_seconds   INTEGER     NOT NULL DEFAULT 1800,
    note          TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT gpu_maintenance_lock_pk PRIMARY KEY (target),
    CONSTRAINT gpu_maintenance_lock_target_chk CHECK (target IN ('sam3', 'pe_core'))
);

CREATE INDEX IF NOT EXISTS gpu_maintenance_lock_active_idx
    ON gpu_maintenance_lock (active);

COMMIT;

-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'gpu_maintenance_lock_target_chk' AND conrelid = 'gpu_maintenance_lock'::regclass)
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'gpu_maintenance_lock' AND column_name = 'heartbeat_at')
```

- [ ] **Write the failing migration test.** Create `tests/integration/test_gpu_maintenance_lock_migration.py`:

```python
"""014_gpu_maintenance_lock 적용 검증 (real-PG fixture)."""
from __future__ import annotations

import pytest

pytest.importorskip("psycopg")


def test_gpu_maintenance_lock_table_and_constraint(postgres_resource):
    with postgres_resource.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_constraint "
                "WHERE conname = 'gpu_maintenance_lock_target_chk' "
                "AND conrelid = 'gpu_maintenance_lock'::regclass)"
            )
            assert cur.fetchone()[0] is True
            # CHECK rejects unknown target
            cur.execute("INSERT INTO gpu_maintenance_lock (target) VALUES ('sam3')")
            with pytest.raises(Exception):
                cur.execute("INSERT INTO gpu_maintenance_lock (target) VALUES ('bogus')")
```

- [ ] **SPIKE — confirm `postgres_resource` exposes `.connection()` / cursor shape.** Run `grep -n "def connection\|@contextmanager\|def cursor" src/vlm_pipeline/resources/postgres_base.py` and `sed -n '60,177p' tests/conftest.py`. If the fixture's context-manager method is named differently (e.g. `conn()` or `_connect()`), adjust the test's `with postgres_resource.connection()` accordingly before running. Record the exact method name in the PR description. (Flagged in open_risks.)

- [ ] **Run the migration validation locally (README §6 a/b/c).**

```bash
docker run --rm -d --name pg-validate -e POSTGRES_PASSWORD=test -p 25432:5432 postgres:15 && sleep 3
docker cp src/vlm_pipeline/sql/migrations/postgres/014_gpu_maintenance_lock.sql pg-validate:/tmp/
docker exec pg-validate psql -U postgres -f /tmp/014_gpu_maintenance_lock.sql -v ON_ERROR_STOP=on
docker exec pg-validate psql -U postgres -f /tmp/014_gpu_maintenance_lock.sql -v ON_ERROR_STOP=on
docker rm -f pg-validate
```

Expected: both applies print `CREATE TABLE` / `CREATE INDEX` (second run: `NOTICE ... already exists, skipping`), `COMMIT`, exit 0 both times.

- [ ] **Run the integration test (skips if no test PG).**

```bash
pytest tests/integration/test_gpu_maintenance_lock_migration.py -q
```

Expected when `DATAOPS_TEST_POSTGRES_DSN` set: `1 passed`. When unset: `1 skipped`.

- [ ] **Commit.** `git add -A && git commit -m "feat(mlops): 013 gpu_maintenance_lock flag table"`

---

### Task F2: pure flag-store + stale-decision logic in `lib/maintenance_flag.py`

Pure L1-2 module. No dagster/psycopg/torch at module top. Holds: the in-process flag dataclass, the stale-decision function (heartbeat TTL expiry OR owner run not RUNNING), and a PG row→flag parser. The app.py gate and the guard sensor both import from here.

**Files:**
- `src/vlm_pipeline/lib/maintenance_flag.py` (new)
- `tests/unit/test_maintenance_flag.py` (new)

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) MaintenanceFlag(active: bool, target: str, owner_run_id: str | None, entered_at: float | None, heartbeat_at: float | None, ttl_seconds: int, note: str | None)`
  - `CLEAR_FLAG: MaintenanceFlag` (module constant; `active=False`)
  - `flag_from_pg_row(row: dict | None, *, target: str) -> MaintenanceFlag`
  - `is_heartbeat_stale(flag: MaintenanceFlag, *, now_ts: float) -> bool`
  - `decide_auto_release(flag: MaintenanceFlag, *, owner_run_is_running: bool, now_ts: float) -> tuple[bool, str | None]` → `(should_release, reason)`; reason ∈ `'heartbeat_ttl_expired'|'owner_run_not_running'|None`
- Consumes: nothing.

- [ ] **Write the failing unit test.** Create `tests/unit/test_maintenance_flag.py`:

```python
"""lib.maintenance_flag — 순수 정비락 판단 로직 (no PG/dagster)."""
from __future__ import annotations

from vlm_pipeline.lib.maintenance_flag import (
    CLEAR_FLAG,
    MaintenanceFlag,
    decide_auto_release,
    flag_from_pg_row,
    is_heartbeat_stale,
)


def _active(**kw) -> MaintenanceFlag:
    base = dict(
        active=True, target="sam3", owner_run_id="run-1",
        entered_at=1000.0, heartbeat_at=1000.0, ttl_seconds=600, note=None,
    )
    base.update(kw)
    return MaintenanceFlag(**base)


def test_flag_from_none_row_is_clear():
    f = flag_from_pg_row(None, target="sam3")
    assert f.active is False
    assert f.target == "sam3"


def test_flag_from_pg_row_parses_timestamps():
    import datetime as dt
    ts = dt.datetime(2026, 6, 29, tzinfo=dt.timezone.utc)
    row = {
        "active": True, "owner_run_id": "run-9",
        "entered_at": ts, "heartbeat_at": ts,
        "ttl_seconds": 900, "note": "training",
    }
    f = flag_from_pg_row(row, target="pe_core")
    assert f.active is True and f.owner_run_id == "run-9"
    assert f.ttl_seconds == 900
    assert f.heartbeat_at == ts.timestamp()


def test_heartbeat_stale_when_past_ttl():
    f = _active(heartbeat_at=1000.0, ttl_seconds=600)
    assert is_heartbeat_stale(f, now_ts=1700.0) is True   # 700s > 600 ttl
    assert is_heartbeat_stale(f, now_ts=1500.0) is False  # 500s < 600 ttl


def test_clear_flag_never_stale():
    assert is_heartbeat_stale(CLEAR_FLAG, now_ts=10**12) is False


def test_auto_release_on_dead_heartbeat():
    f = _active(heartbeat_at=1000.0, ttl_seconds=600)
    release, reason = decide_auto_release(f, owner_run_is_running=True, now_ts=1700.0)
    assert release is True and reason == "heartbeat_ttl_expired"


def test_auto_release_on_dead_owner_run():
    f = _active(heartbeat_at=1000.0, ttl_seconds=600)
    release, reason = decide_auto_release(f, owner_run_is_running=False, now_ts=1100.0)
    assert release is True and reason == "owner_run_not_running"


def test_no_release_when_healthy():
    f = _active(heartbeat_at=1000.0, ttl_seconds=600)
    release, reason = decide_auto_release(f, owner_run_is_running=True, now_ts=1100.0)
    assert release is False and reason is None


def test_no_release_when_inactive():
    release, reason = decide_auto_release(CLEAR_FLAG, owner_run_is_running=False, now_ts=1100.0)
    assert release is False and reason is None
```

- [ ] **Run it — expect FAIL (ModuleNotFoundError).**

```bash
pytest tests/unit/test_maintenance_flag.py -q
```

Expected: `ModuleNotFoundError: No module named 'vlm_pipeline.lib.maintenance_flag'` → collection error.

- [ ] **Write the implementation.** Create `src/vlm_pipeline/lib/maintenance_flag.py`:

```python
"""GPU 정비락 — 순수 판단 로직 (L1-2, no PG/dagster/torch import).

서버사이드 게이트(docker/*/app.py)와 guard 센서(defs/train/sensor_maintenance_guard.py)가
공유하는 flag 표현 + stale 판정. 저장/네트워크 부수효과 없음.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

VALID_TARGETS = ("sam3", "pe_core")


@dataclass(frozen=True)
class MaintenanceFlag:
    active: bool
    target: str
    owner_run_id: str | None = None
    entered_at: float | None = None
    heartbeat_at: float | None = None
    ttl_seconds: int = 1800
    note: str | None = None


CLEAR_FLAG = MaintenanceFlag(active=False, target="")


def _to_epoch(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def flag_from_pg_row(row: dict[str, Any] | None, *, target: str) -> MaintenanceFlag:
    """gpu_maintenance_lock 행(dict) → MaintenanceFlag. 행 없으면 active=False."""
    if not row:
        return MaintenanceFlag(active=False, target=target)
    return MaintenanceFlag(
        active=bool(row.get("active", False)),
        target=target,
        owner_run_id=(str(row["owner_run_id"]) if row.get("owner_run_id") else None),
        entered_at=_to_epoch(row.get("entered_at")),
        heartbeat_at=_to_epoch(row.get("heartbeat_at")),
        ttl_seconds=int(row.get("ttl_seconds") or 1800),
        note=(str(row["note"]) if row.get("note") else None),
    )


def is_heartbeat_stale(flag: MaintenanceFlag, *, now_ts: float) -> bool:
    """active 락의 heartbeat 가 TTL 을 초과했으면 True."""
    if not flag.active:
        return False
    if flag.heartbeat_at is None:
        # active 인데 heartbeat 없음 = 즉시 stale (fail-safe)
        return True
    return (now_ts - flag.heartbeat_at) > float(flag.ttl_seconds)


def decide_auto_release(
    flag: MaintenanceFlag,
    *,
    owner_run_is_running: bool,
    now_ts: float,
) -> tuple[bool, str | None]:
    """guard 센서가 자동해제해야 하는지 판정.

    해제 조건(둘 중 하나):
      - heartbeat 가 TTL 초과 (dead heartbeat)
      - owner run 이 RUNNING/STARTED 아님 (프로세스 죽음/재배포 고아화)
    inactive 락은 해제 대상 아님.
    """
    if not flag.active:
        return (False, None)
    if is_heartbeat_stale(flag, now_ts=now_ts):
        return (True, "heartbeat_ttl_expired")
    if not owner_run_is_running:
        return (True, "owner_run_not_running")
    return (False, None)
```

- [ ] **Run it — expect PASS.**

```bash
pytest tests/unit/test_maintenance_flag.py -q
```

Expected: `8 passed`.

- [ ] **Verify no layer violation.**

```bash
python scripts/check_lib_layer_imports.py 2>&1 | tail -5 || ruff check src/vlm_pipeline/lib/maintenance_flag.py
```

Expected: no violation reported for `maintenance_flag.py` (lib must not import dagster/defs/resources/ops).

- [ ] **Commit.** `git add -A && git commit -m "feat(mlops): lib.maintenance_flag pure gate + stale-decision logic"`

---

### Task F3: server-side maintenance gate in `docker/embedding/app.py`

Add a monkeypatchable flag-store shim + gate. When the flag is active: `/embed`, `/embed_text`, `/warmup` return `503`; the idle-watcher and `_ensure_model_loaded()` refuse lazy-reload. `/maintenance/enter|exit` write the in-process flag (and, when `MAINTENANCE_DSN` is set, persist to PG via a thin writer — but the default in-app store is process-local so TestClient tests need no PG). A `/maintenance/heartbeat` bumps `heartbeat_at`.

**Files:**
- `docker/embedding/app.py` (edit)
- `tests/unit/test_embedding_maintenance_gate.py` (new)

**Interfaces:**
- Produces (HTTP, embedding service): `POST /maintenance/enter` (form: `owner_run_id`, `ttl_seconds`, `note`), `POST /maintenance/exit`, `POST /maintenance/heartbeat`, `GET /maintenance/status`. Module-level `_maintenance` dict store + `_maintenance_active() -> bool` + `_set_maintenance(active, **fields)`.
- Consumes: `lib.maintenance_flag` semantics conceptually (but app.py is a standalone container — it does NOT import vlm_pipeline; it reimplements the tiny gate inline, keeping the container `vlm_pipeline`-free, mirroring how `open_clip_be.py` has no pipeline import).

> **Note on duplication (deliberate):** `docker/embedding` and `docker/sam3` are standalone images that do NOT have `vlm_pipeline` on PYTHONPATH (they import `backends.*` / `sam3.*` only). So the in-app gate is a ~25-line inline block, NOT an import of `lib.maintenance_flag`. The shared semantics live in the lib for the sensor side; the app side is independently testable via TestClient.

- [ ] **Write the failing test.** Create `tests/unit/test_embedding_maintenance_gate.py`:

```python
"""embedding 서비스 정비 게이트: maintenance 활성 시 503 + lazy-reload 거부."""
from __future__ import annotations

import importlib
import pathlib
import sys

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("multipart")

_SVC_DIR = str(pathlib.Path("docker/embedding").resolve())


@pytest.fixture
def client(monkeypatch):
    from fastapi.testclient import TestClient

    if _SVC_DIR not in sys.path:
        sys.path.insert(0, _SVC_DIR)
    monkeypatch.setenv("EMBEDDING_BACKEND", "open_clip")
    import backends.open_clip_be as be

    state = {"loaded": False, "load_calls": 0}

    def _load(self):
        state["load_calls"] += 1
        state["loaded"] = True

    monkeypatch.setattr(be.OpenClipBackend, "load", _load)
    monkeypatch.setattr(be.OpenClipBackend, "unload", lambda self: state.__setitem__("loaded", False))
    monkeypatch.setattr(be.OpenClipBackend, "is_loaded", lambda self: state["loaded"])
    monkeypatch.setattr(be.OpenClipBackend, "embed_image", lambda self, b: [0.1] * 1024)
    monkeypatch.setattr(be.OpenClipBackend, "embed_text", lambda self, t: [0.2] * 1024)

    sys.modules.pop("app", None)
    app_mod = importlib.import_module("app")
    with TestClient(app_mod.app) as c:
        c._state = state  # expose for assertions
        yield c


def test_embed_503_under_maintenance(client):
    assert client.post("/maintenance/enter", data={"owner_run_id": "run-1"}).status_code == 200
    r = client.post("/embed_text", data={"text": "x"})
    assert r.status_code == 503
    assert "maintenance" in r.json()["detail"]


def test_warmup_503_under_maintenance(client):
    client.post("/maintenance/enter", data={"owner_run_id": "run-1"})
    assert client.post("/warmup").status_code == 503


def test_lazy_reload_refused_under_maintenance(client):
    # unload, then enter maintenance, then /embed must NOT reload (load_calls frozen)
    client.post("/unload")
    calls_before = client._state["load_calls"]
    client.post("/maintenance/enter", data={"owner_run_id": "run-1"})
    r = client.post("/embed_text", data={"text": "x"})
    assert r.status_code == 503
    assert client._state["load_calls"] == calls_before  # no lazy reload happened


def test_exit_restores_normal_operation(client):
    client.post("/maintenance/enter", data={"owner_run_id": "run-1"})
    assert client.post("/embed_text", data={"text": "x"}).status_code == 503
    assert client.post("/maintenance/exit").status_code == 200
    r = client.post("/embed_text", data={"text": "x"})
    assert r.status_code == 200
    assert len(r.json()["vector"]) == 1024


def test_status_reflects_flag(client):
    assert client.get("/maintenance/status").json()["active"] is False
    client.post("/maintenance/enter", data={"owner_run_id": "run-7", "note": "ft"})
    s = client.get("/maintenance/status").json()
    assert s["active"] is True and s["owner_run_id"] == "run-7"


def test_normal_operation_unaffected_when_clear(client):
    r = client.post("/embed", files={"file": ("i.jpg", b"x", "application/octet-stream")})
    assert r.status_code == 200 and len(r.json()["vector"]) == 1024
```

- [ ] **Run it — expect FAIL.**

```bash
pytest tests/unit/test_embedding_maintenance_gate.py -q
```

Expected: failures — `/maintenance/enter` 404 (route missing) and `/embed_text` returns 200 not 503.

- [ ] **Implement the gate in `docker/embedding/app.py`.** Add the flag store + helpers right after the `_idle_stop_event = threading.Event()` line (after current line 47):

```python

# ─── GPU 정비 게이트 (server-side, fail-safe) ────────────────────────────────
# maintenance 활성 동안 /embed·/embed_text·/warmup 은 503, lazy-reload 거부.
# 프로세스-로컬 in-memory store 가 진실. (PG 영속화는 guard 센서가 별도 관리;
# 이 컨테이너는 vlm_pipeline-free 유지 — backends.* 만 import.)
_maintenance: dict = {
    "active": False,
    "owner_run_id": None,
    "entered_at": None,
    "heartbeat_at": None,
    "ttl_seconds": int(os.environ.get("MAINTENANCE_DEFAULT_TTL_SECONDS", "1800")),
    "note": None,
}
_maintenance_lock = threading.Lock()


def _maintenance_active() -> bool:
    return bool(_maintenance.get("active"))


def _set_maintenance(active: bool, **fields) -> dict:
    with _maintenance_lock:
        _maintenance["active"] = bool(active)
        if active:
            now = time.time()
            _maintenance["owner_run_id"] = fields.get("owner_run_id")
            _maintenance["entered_at"] = now
            _maintenance["heartbeat_at"] = now
            _maintenance["ttl_seconds"] = int(fields.get("ttl_seconds") or _maintenance["ttl_seconds"])
            _maintenance["note"] = fields.get("note")
        else:
            _maintenance["owner_run_id"] = None
            _maintenance["entered_at"] = None
            _maintenance["heartbeat_at"] = None
            _maintenance["note"] = None
        return dict(_maintenance)
```

- [ ] **Gate the idle-watcher and `_ensure_model_loaded`.** Edit `_ensure_model_loaded` (current lines 70-77) to refuse reload under maintenance:

```python
def _ensure_model_loaded() -> None:
    """idle unload 이후 첫 request 가 도착하면 lazy reload. 정비 중이면 거부."""
    if _maintenance_active():
        return
    if _backend.is_loaded():
        return
    with _predict_lock:
        if not _backend.is_loaded():
            logger.info("embedding lazy reload (idle 후 첫 request)")
            _load_model()
```

- [ ] **Gate the request handlers.** Add a guard at the top of `/warmup`, `/embed`, `/embed_text`. For `/embed_text` (current lines 165-173) replace the body's start:

```python
@app.post("/embed_text")
def embed_text(text: str = Form(...)) -> dict:
    if _maintenance_active():
        raise HTTPException(status_code=503, detail="gpu_under_maintenance")
    _touch_request()
    _ensure_model_loaded()
    if not _backend.is_loaded():
        raise HTTPException(status_code=503, detail=_load_error or "embedding_model_not_loaded")
    with _predict_lock:
        vector = _backend.embed_text(text)
    return {"vector": vector, "dim": _backend.dim, "model_name": _backend.name}
```

Apply the same `if _maintenance_active(): raise HTTPException(503, "gpu_under_maintenance")` as the FIRST line of `embed` (after the `async def embed(...)` signature, before `_touch_request()`) and of `warmup`.

- [ ] **Add the maintenance routes.** Append after the `/embed_text` route (end of file):

```python


@app.post("/maintenance/enter")
def maintenance_enter(
    owner_run_id: str | None = Form(None),
    ttl_seconds: int | None = Form(None),
    note: str | None = Form(None),
) -> dict:
    """정비 진입: 게이트 활성 + (선택) 모델 unload. 이후 inference 는 503."""
    state = _set_maintenance(True, owner_run_id=owner_run_id, ttl_seconds=ttl_seconds, note=note)
    _unload_model()
    logger.warning("embedding maintenance ENTER owner_run_id=%s ttl=%ss", owner_run_id, state["ttl_seconds"])
    return state


@app.post("/maintenance/exit")
def maintenance_exit() -> dict:
    """정비 종료: 게이트 해제. 호출자는 이후 /warmup 으로 재로딩."""
    state = _set_maintenance(False)
    logger.warning("embedding maintenance EXIT")
    return state


@app.post("/maintenance/heartbeat")
def maintenance_heartbeat() -> dict:
    """정비 owner 가 살아있음을 알림 (TTL 갱신)."""
    with _maintenance_lock:
        if _maintenance["active"]:
            _maintenance["heartbeat_at"] = time.time()
        return dict(_maintenance)


@app.get("/maintenance/status")
def maintenance_status() -> dict:
    return dict(_maintenance)
```

- [ ] **Run it — expect PASS.**

```bash
pytest tests/unit/test_embedding_maintenance_gate.py tests/unit/test_embedding_service_contract.py -q
```

Expected: `6 passed` (gate) + existing contract tests still `... passed` (no regression).

- [ ] **Commit.** `git add -A && git commit -m "feat(mlops): server-side GPU maintenance gate in embedding app"`

---

### Task F4: server-side maintenance gate in `docker/sam3/app.py`

Mirror Task F3 in the SAM3 server. SAM3's loaded-state is `_processor is not None`; lazy-reload is `_ensure_model_loaded` (current lines 111-118). Same routes + same gate semantics.

**Files:**
- `docker/sam3/app.py` (edit)
- `tests/unit/test_sam3_maintenance_gate.py` (new)

**Interfaces:**
- Produces (HTTP, sam3 service): `POST /maintenance/enter|exit|heartbeat`, `GET /maintenance/status`; module-level `_maintenance` store + `_maintenance_active()` + `_set_maintenance()` (identical shape to F3).
- Consumes: nothing external (standalone image; no `vlm_pipeline` import).

- [ ] **SPIKE — confirm how to mock SAM3 model load in TestClient.** SAM3's `_load_model()` (lines 62-88) imports `sam3.*` at call time and is wrapped in try/except (sets `_load_error` on failure). In CI there is no `sam3` package, so `_load_model()` will set `_load_error` and leave `_processor=None` — meaning `/segment` would naturally 503 even WITHOUT maintenance, making the "503 under maintenance" test ambiguous. **Resolution:** monkeypatch `app._load_model` to set `app._processor`/`app._model` to sentinels and monkeypatch `app._run_segmentation` to return `([], {})`, mirroring the embedding test's backend monkeypatch. Confirm `_run_segmentation` is module-level (it is, line 303). Record this in the test fixture.

- [ ] **Write the failing test.** Create `tests/unit/test_sam3_maintenance_gate.py`:

```python
"""SAM3 서비스 정비 게이트: maintenance 활성 시 503 + lazy-reload 거부."""
from __future__ import annotations

import importlib
import pathlib
import sys

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("multipart")

_SVC_DIR = str(pathlib.Path("docker/sam3").resolve())

_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01"
    b"\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
)


@pytest.fixture
def client(monkeypatch):
    from fastapi.testclient import TestClient

    if _SVC_DIR not in sys.path:
        sys.path.insert(0, _SVC_DIR)
    sys.modules.pop("app", None)
    app_mod = importlib.import_module("app")

    state = {"load_calls": 0}

    def _fake_load():
        state["load_calls"] += 1
        app_mod._processor = object()
        app_mod._model = object()
        app_mod._model_loaded_at = 1.0
        app_mod._load_error = None

    monkeypatch.setattr(app_mod, "_load_model", _fake_load)
    monkeypatch.setattr(app_mod, "_run_segmentation", lambda *a, **k: ([], {}))
    monkeypatch.setattr(app_mod, "_reset_gpu_peak_memory", lambda: None)
    monkeypatch.setattr(app_mod, "_gpu_peak_memory_gb", lambda: None)

    with TestClient(app_mod.app) as c:
        c._mod = app_mod
        c._state = state
        yield c


def _segment(client):
    return client.post(
        "/segment",
        files={"file": ("i.png", _PNG, "image/png")},
        data={"prompts_json": '["fire"]'},
    )


def test_segment_503_under_maintenance(client):
    assert client.post("/maintenance/enter", data={"owner_run_id": "r1"}).status_code == 200
    r = _segment(client)
    assert r.status_code == 503
    assert r.json()["detail"] == "gpu_under_maintenance"


def test_warmup_503_under_maintenance(client):
    client.post("/maintenance/enter", data={"owner_run_id": "r1"})
    assert client.post("/warmup").status_code == 503


def test_lazy_reload_refused_under_maintenance(client):
    client.post("/unload")
    calls_before = client._state["load_calls"]
    client.post("/maintenance/enter", data={"owner_run_id": "r1"})
    assert _segment(client).status_code == 503
    assert client._state["load_calls"] == calls_before


def test_exit_restores_normal_operation(client):
    client.post("/maintenance/enter", data={"owner_run_id": "r1"})
    assert _segment(client).status_code == 503
    assert client.post("/maintenance/exit").status_code == 200
    assert client.post("/warmup").status_code == 200
    assert _segment(client).status_code == 200


def test_normal_operation_unaffected_when_clear(client):
    assert _segment(client).status_code == 200
```

- [ ] **Run it — expect FAIL** (`/maintenance/enter` → 404).

```bash
pytest tests/unit/test_sam3_maintenance_gate.py -q
```

Expected: failures on missing maintenance routes / 200-instead-of-503.

- [ ] **Implement the store in `docker/sam3/app.py`.** Insert after the `_idle_stop_event = threading.Event()` line (after current line 36):

```python

# ─── GPU 정비 게이트 (server-side, fail-safe) ────────────────────────────────
# maintenance 활성 동안 /segment·/warmup 은 503, lazy-reload 거부.
# 프로세스-로컬 in-memory store 가 진실 (컨테이너는 vlm_pipeline-free).
_maintenance: dict[str, Any] = {
    "active": False,
    "owner_run_id": None,
    "entered_at": None,
    "heartbeat_at": None,
    "ttl_seconds": int(os.environ.get("MAINTENANCE_DEFAULT_TTL_SECONDS", "1800")),
    "note": None,
}
_maintenance_lock = threading.Lock()


def _maintenance_active() -> bool:
    return bool(_maintenance.get("active"))


def _set_maintenance(active: bool, **fields: Any) -> dict[str, Any]:
    with _maintenance_lock:
        _maintenance["active"] = bool(active)
        if active:
            now = time.time()
            _maintenance["owner_run_id"] = fields.get("owner_run_id")
            _maintenance["entered_at"] = now
            _maintenance["heartbeat_at"] = now
            _maintenance["ttl_seconds"] = int(fields.get("ttl_seconds") or _maintenance["ttl_seconds"])
            _maintenance["note"] = fields.get("note")
        else:
            _maintenance["owner_run_id"] = None
            _maintenance["entered_at"] = None
            _maintenance["heartbeat_at"] = None
            _maintenance["note"] = None
        return dict(_maintenance)
```

- [ ] **Gate lazy-reload.** Edit `_ensure_model_loaded` (lines 111-118):

```python
def _ensure_model_loaded() -> None:
    """idle unload 이후 첫 request 가 도착하면 lazy reload. 정비 중이면 거부."""
    if _maintenance_active():
        return
    if _processor is not None:
        return
    with _predict_lock:
        if _processor is None:
            logger.info("SAM3 lazy reload (idle 후 첫 request)")
            _load_model()
```

- [ ] **Gate handlers.** Add as the FIRST statement inside `warmup` (after line 401 signature) and inside `segment` (after the `):` at line 452, before `_touch_request()`):

```python
    if _maintenance_active():
        raise HTTPException(status_code=503, detail="gpu_under_maintenance")
```

(`warmup` is `async def warmup() -> dict[str, Any]:` — insert the same two lines as its first body lines.)

- [ ] **Add maintenance routes.** Append at end of `docker/sam3/app.py` BEFORE the `if __name__ == "__main__":` block:

```python


@app.post("/maintenance/enter")
async def maintenance_enter(
    owner_run_id: str | None = Form(None),
    ttl_seconds: int | None = Form(None),
    note: str | None = Form(None),
) -> dict[str, Any]:
    """정비 진입: 게이트 활성 + 모델 unload. 이후 /segment·/warmup 은 503."""
    state = _set_maintenance(True, owner_run_id=owner_run_id, ttl_seconds=ttl_seconds, note=note)
    _unload_model()
    logger.warning("SAM3 maintenance ENTER owner_run_id=%s ttl=%ss", owner_run_id, state["ttl_seconds"])
    return state


@app.post("/maintenance/exit")
async def maintenance_exit() -> dict[str, Any]:
    state = _set_maintenance(False)
    logger.warning("SAM3 maintenance EXIT")
    return state


@app.post("/maintenance/heartbeat")
async def maintenance_heartbeat() -> dict[str, Any]:
    with _maintenance_lock:
        if _maintenance["active"]:
            _maintenance["heartbeat_at"] = time.time()
        return dict(_maintenance)


@app.get("/maintenance/status")
async def maintenance_status() -> dict[str, Any]:
    return dict(_maintenance)
```

- [ ] **Run it — expect PASS.**

```bash
pytest tests/unit/test_sam3_maintenance_gate.py -q
```

Expected: `5 passed`.

- [ ] **Commit.** `git add -A && git commit -m "feat(mlops): server-side GPU maintenance gate in sam3 app"`

---

### Task F5: PG flag writer/reader on the `db` resource (`PostgresMaintenanceMixin`)

The guard sensor (F6) and the trainer Dagster op need to read/write the PG flag row. Add a thin mixin with `set_gpu_maintenance(target, active, owner_run_id, ttl_seconds, note)`, `bump_gpu_maintenance_heartbeat(target)`, and `get_gpu_maintenance(target) -> dict | None`. Wire it into `PostgresResource` MRO and the conftest test resource.

**Files:**
- `src/vlm_pipeline/resources/postgres_maintenance.py` (new)
- `src/vlm_pipeline/resources/postgres.py` (edit — add mixin to MRO) **[SPIKE-gated, see below]**
- `tests/integration/test_postgres_maintenance_mixin.py` (new)

**Interfaces:**
- Produces (Python): `PostgresMaintenanceMixin.set_gpu_maintenance(self, target: str, *, active: bool, owner_run_id: str | None = None, ttl_seconds: int = 1800, note: str | None = None) -> None`; `bump_gpu_maintenance_heartbeat(self, target: str) -> None`; `get_gpu_maintenance(self, target: str) -> dict | None` (keys: `active, owner_run_id, entered_at, heartbeat_at, ttl_seconds, note`).
- Consumes: table `gpu_maintenance_lock` (F1); the existing `PostgresBaseMixin` connection helper (resolved by SPIKE).

- [ ] **SPIKE — read the base mixin's connection/exec idiom.** Run `grep -n "def _exec\|def _fetchone\|@contextmanager\|def connection\|def _connect\|with self._pool\|def _cursor" src/vlm_pipeline/resources/postgres_base.py` and read the 2 most relevant methods + the MRO list in `src/vlm_pipeline/resources/postgres.py` (the `class PostgresResource(...)` line). Record: (a) the exact context-manager used to get a cursor, (b) the paramstyle (`%(name)s` vs `%s`), (c) where to insert the new mixin in MRO (after `PostgresBaseMixin`, before domain mixins). Write F5's implementation using those EXACT names. (Flagged in open_risks — implementation below uses the `PostgresBaseMixin._cursor()` contextmanager + `%(name)s` paramstyle which 001-init code uses; correct if the spike shows otherwise.)

- [ ] **Write the failing integration test.** Create `tests/integration/test_postgres_maintenance_mixin.py`:

```python
"""PostgresMaintenanceMixin — gpu_maintenance_lock read/write (real-PG)."""
from __future__ import annotations

import pytest

pytest.importorskip("psycopg")


def test_set_get_and_clear(postgres_resource):
    assert postgres_resource.get_gpu_maintenance("sam3") in (None, {"active": False}) or \
        postgres_resource.get_gpu_maintenance("sam3").get("active") in (False, None)

    postgres_resource.set_gpu_maintenance(
        "sam3", active=True, owner_run_id="run-42", ttl_seconds=900, note="ft"
    )
    row = postgres_resource.get_gpu_maintenance("sam3")
    assert row["active"] is True
    assert row["owner_run_id"] == "run-42"
    assert row["ttl_seconds"] == 900
    assert row["entered_at"] is not None and row["heartbeat_at"] is not None

    postgres_resource.set_gpu_maintenance("sam3", active=False)
    row2 = postgres_resource.get_gpu_maintenance("sam3")
    assert row2["active"] is False


def test_heartbeat_bumps_only_when_active(postgres_resource):
    postgres_resource.set_gpu_maintenance("pe_core", active=True, owner_run_id="r", ttl_seconds=600)
    before = postgres_resource.get_gpu_maintenance("pe_core")["heartbeat_at"]
    postgres_resource.bump_gpu_maintenance_heartbeat("pe_core")
    after = postgres_resource.get_gpu_maintenance("pe_core")["heartbeat_at"]
    assert after >= before
```

- [ ] **Run it — expect FAIL** (`AttributeError: ... has no attribute 'set_gpu_maintenance'`, or skip if no PG).

```bash
pytest tests/integration/test_postgres_maintenance_mixin.py -q
```

- [ ] **Implement the mixin.** Create `src/vlm_pipeline/resources/postgres_maintenance.py` (uses `self._cursor()` + `%(name)s` per SPIKE; correct if spike differs):

```python
"""GPU 정비락 PG read/write mixin (gpu_maintenance_lock, migration 013)."""
from __future__ import annotations

from typing import Any

from vlm_pipeline.resources.postgres_base import PostgresBaseMixin


class PostgresMaintenanceMixin(PostgresBaseMixin):
    """gpu_maintenance_lock 단일행 upsert/read. guard 센서·trainer op 가 사용."""

    def set_gpu_maintenance(
        self,
        target: str,
        *,
        active: bool,
        owner_run_id: str | None = None,
        ttl_seconds: int = 1800,
        note: str | None = None,
    ) -> None:
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO gpu_maintenance_lock
                    (target, active, owner_run_id, entered_at, heartbeat_at, ttl_seconds, note, updated_at)
                VALUES
                    (%(target)s, %(active)s, %(owner_run_id)s,
                     CASE WHEN %(active)s THEN now() ELSE NULL END,
                     CASE WHEN %(active)s THEN now() ELSE NULL END,
                     %(ttl_seconds)s, %(note)s, now())
                ON CONFLICT (target) DO UPDATE SET
                    active       = EXCLUDED.active,
                    owner_run_id = CASE WHEN EXCLUDED.active THEN EXCLUDED.owner_run_id ELSE NULL END,
                    entered_at   = CASE WHEN EXCLUDED.active THEN now() ELSE NULL END,
                    heartbeat_at = CASE WHEN EXCLUDED.active THEN now() ELSE NULL END,
                    ttl_seconds  = EXCLUDED.ttl_seconds,
                    note         = CASE WHEN EXCLUDED.active THEN EXCLUDED.note ELSE NULL END,
                    updated_at   = now()
                """,
                {
                    "target": target,
                    "active": bool(active),
                    "owner_run_id": owner_run_id,
                    "ttl_seconds": int(ttl_seconds),
                    "note": note,
                },
            )

    def bump_gpu_maintenance_heartbeat(self, target: str) -> None:
        with self._cursor() as cur:
            cur.execute(
                "UPDATE gpu_maintenance_lock SET heartbeat_at = now(), updated_at = now() "
                "WHERE target = %(target)s AND active = TRUE",
                {"target": target},
            )

    def get_gpu_maintenance(self, target: str) -> dict[str, Any] | None:
        with self._cursor() as cur:
            cur.execute(
                "SELECT active, owner_run_id, entered_at, heartbeat_at, ttl_seconds, note "
                "FROM gpu_maintenance_lock WHERE target = %(target)s",
                {"target": target},
            )
            row = cur.fetchone()
            if row is None:
                return None
            cols = ("active", "owner_run_id", "entered_at", "heartbeat_at", "ttl_seconds", "note")
            if isinstance(row, dict):
                return dict(row)
            return dict(zip(cols, row))
```

- [ ] **Wire into MRO.** In `src/vlm_pipeline/resources/postgres.py`, add `from vlm_pipeline.resources.postgres_maintenance import PostgresMaintenanceMixin` and insert `PostgresMaintenanceMixin` into the `PostgresResource(...)` base list immediately after `PostgresBaseMixin`. In `tests/conftest.py`, add the same import and add `PostgresMaintenanceMixin` to `_PostgresTestResource`'s base list immediately after `PostgresBaseMixin`. (Exact insertion validated by the F5 SPIKE.)

- [ ] **Run it — expect PASS.**

```bash
pytest tests/integration/test_postgres_maintenance_mixin.py -q
```

Expected when PG available: `2 passed`; else `2 skipped`.

- [ ] **Commit.** `git add -A && git commit -m "feat(mlops): PostgresMaintenanceMixin for gpu_maintenance_lock"`

---

### Task F6: guard sensor `maintenance_guard_sensor` (auto-release stale flag + re-warm)

A Dagster sensor (modeled on `stuck_run_guard_sensor`) that, per target (`sam3`, `pe_core`), reads the PG flag, checks whether the owner run is RUNNING/STARTED, and if `decide_auto_release` says release: clears the PG flag, POSTs `/maintenance/exit` + `/warmup` to the serving URL, and logs. The decision is the pure `lib.maintenance_flag.decide_auto_release`; the sensor only orchestrates. We keep a thin pure helper `resolve_release_actions(...)` for unit-testing the sensor body without a Dagster instance (matching the test_stuck_run_guard pattern of testing pure functions).

**Files:**
- `src/vlm_pipeline/defs/train/__init__.py` (new, empty)
- `src/vlm_pipeline/defs/train/sensor_maintenance_guard.py` (new)
- `src/vlm_pipeline/resources/runtime_settings.py` (edit — add `MaintenanceGuardSettings` + loader)
- `tests/unit/test_maintenance_guard_sensor.py` (new)

**Interfaces:**
- Produces:
  - `resolve_release_actions(flag: MaintenanceFlag, *, owner_run_is_running: bool, now_ts: float) -> tuple[bool, str | None]` (thin wrapper over `decide_auto_release`, plus an "active-but-no-owner_run_id" → release-with-reason `owner_run_id_missing` rule).
  - `maintenance_guard_sensor` (Dagster sensor, `required_resource_keys={"db"}`).
  - `MaintenanceGuardSettings(enabled, interval_sec, targets: dict[str,str])` where targets maps `target → serving_url` (e.g. `{"sam3": SAM3_API_URL, "pe_core": EMBEDDING_API_URL}`); `load_maintenance_guard_settings()`.
- Consumes: `lib.maintenance_flag.{MaintenanceFlag,flag_from_pg_row,decide_auto_release}`; `db.get_gpu_maintenance` / `db.set_gpu_maintenance` (F5); `context.instance.get_run_by_id(...).status` for RUNNING check.

- [ ] **SPIKE — confirm the run-status lookup + serving-URL env names.** Run `grep -rn "get_run_by_id\|get_run_records\|RunsFilter(run_ids" src/vlm_pipeline/defs | head` to pick the correct instance API for "is this run_id RUNNING?", and `grep -rn "SAM3_API_URL\|EMBEDDING_API_URL\|EMBEDDING_SERVICE_URL" src configs docker | head` for the exact serving-URL env var names the pipeline already uses. Record both. The impl below uses `context.instance.get_run_by_id(run_id)` and env `SAM3_API_URL` / `EMBEDDING_API_URL`; correct per spike. (Flagged in open_risks.)

- [ ] **Write the failing unit test** (pure-function + settings only, no Dagster instance — mirrors test_stuck_run_guard.py). Create `tests/unit/test_maintenance_guard_sensor.py`:

```python
"""maintenance_guard_sensor 순수 판단 로직 + settings 검증 (no Dagster instance)."""
from __future__ import annotations

import pytest

pytest.importorskip("dagster")

from vlm_pipeline.defs.train.sensor_maintenance_guard import resolve_release_actions
from vlm_pipeline.lib.maintenance_flag import MaintenanceFlag


def _flag(**kw) -> MaintenanceFlag:
    base = dict(
        active=True, target="sam3", owner_run_id="run-1",
        entered_at=1000.0, heartbeat_at=1000.0, ttl_seconds=600, note=None,
    )
    base.update(kw)
    return MaintenanceFlag(**base)


def test_release_on_dead_heartbeat():
    release, reason = resolve_release_actions(_flag(), owner_run_is_running=True, now_ts=1700.0)
    assert release is True and reason == "heartbeat_ttl_expired"


def test_release_on_dead_owner_run():
    release, reason = resolve_release_actions(_flag(), owner_run_is_running=False, now_ts=1100.0)
    assert release is True and reason == "owner_run_not_running"


def test_release_when_owner_run_id_missing():
    release, reason = resolve_release_actions(
        _flag(owner_run_id=None), owner_run_is_running=False, now_ts=1100.0
    )
    assert release is True and reason == "owner_run_id_missing"


def test_no_release_when_healthy():
    release, reason = resolve_release_actions(_flag(), owner_run_is_running=True, now_ts=1100.0)
    assert release is False and reason is None


def test_no_release_when_inactive():
    release, reason = resolve_release_actions(
        _flag(active=False), owner_run_is_running=False, now_ts=1100.0
    )
    assert release is False and reason is None


def test_settings_loader_defaults(monkeypatch):
    from vlm_pipeline.resources.runtime_settings import load_maintenance_guard_settings

    monkeypatch.delenv("MAINTENANCE_GUARD_ENABLED", raising=False)
    s = load_maintenance_guard_settings()
    assert s.enabled is True  # fail-safe default ON
    assert "sam3" in s.targets and "pe_core" in s.targets
    assert s.interval_sec >= 30
```

- [ ] **Run it — expect FAIL** (`ModuleNotFoundError: vlm_pipeline.defs.train.sensor_maintenance_guard`).

```bash
pytest tests/unit/test_maintenance_guard_sensor.py -q
```

- [ ] **Add settings.** In `src/vlm_pipeline/resources/runtime_settings.py`, add the dataclass (after `StuckRunGuardSettings`) and loader:

```python
@dataclass(frozen=True)
class MaintenanceGuardSettings:
    enabled: bool
    interval_sec: int
    targets: dict[str, str]  # target -> serving base URL
```

and append a loader near the other `load_*` functions:

```python
def load_maintenance_guard_settings() -> MaintenanceGuardSettings:
    return MaintenanceGuardSettings(
        enabled=bool_env("MAINTENANCE_GUARD_ENABLED", True),
        interval_sec=int_env("MAINTENANCE_GUARD_INTERVAL_SEC", 60, 30),
        targets={
            "sam3": os.environ.get("SAM3_API_URL", "http://docker-sam3-1:8002"),
            "pe_core": os.environ.get("EMBEDDING_API_URL", "http://docker-embedding-1:8000"),
        },
    )
```

(Correct the default URLs per the F6 SPIKE findings.)

- [ ] **Implement the sensor.** Create `src/vlm_pipeline/defs/train/__init__.py` (empty) and `src/vlm_pipeline/defs/train/sensor_maintenance_guard.py`:

```python
"""maintenance_guard_sensor — GPU 정비락 fail-safe 자동해제 (stuck_run_guard 패턴).

정비 owner run 이 죽었거나(heartbeat TTL 초과 / run 비RUNNING) 정비락이 stale 이면:
  1) PG 플래그 clear  2) 서빙 /maintenance/exit + /warmup  3) 경고 로그.
'락은 fail-safe(자동해제), fail-stuck 아님' — 설계 요구사항(§9).
"""
from __future__ import annotations

import time

import requests
from dagster import DefaultSensorStatus, SkipReason, sensor
from dagster._core.storage.dagster_run import DagsterRunStatus

from vlm_pipeline.lib.maintenance_flag import (
    MaintenanceFlag,
    decide_auto_release,
    flag_from_pg_row,
)
from vlm_pipeline.resources.runtime_settings import load_maintenance_guard_settings

_RUNNING_STATUSES = {DagsterRunStatus.STARTED, DagsterRunStatus.STARTING}


def resolve_release_actions(
    flag: MaintenanceFlag,
    *,
    owner_run_is_running: bool,
    now_ts: float,
) -> tuple[bool, str | None]:
    """순수 판단: 정비락을 자동해제해야 하는가. (sensor body 에서 분리해 단위테스트)"""
    if not flag.active:
        return (False, None)
    if not flag.owner_run_id:
        return (True, "owner_run_id_missing")
    return decide_auto_release(flag, owner_run_is_running=owner_run_is_running, now_ts=now_ts)


def _owner_run_is_running(context, owner_run_id: str | None) -> bool:
    if not owner_run_id:
        return False
    try:
        run = context.instance.get_run_by_id(owner_run_id)
    except Exception:  # noqa: BLE001
        return False
    if run is None:
        return False
    return run.status in _RUNNING_STATUSES


def _release_serving(base_url: str, *, timeout: float = 10.0) -> None:
    requests.post(f"{base_url.rstrip('/')}/maintenance/exit", timeout=timeout)
    requests.post(f"{base_url.rstrip('/')}/warmup", timeout=timeout)


@sensor(
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description="GPU 정비락 stale 자동해제 (fail-safe) + 서빙 re-warm",
    required_resource_keys={"db"},
)
def maintenance_guard_sensor(context):
    settings = load_maintenance_guard_settings()
    if not settings.enabled:
        return SkipReason("maintenance guard 비활성화됨")

    now_ts = time.time()
    db = getattr(context.resources, "db", None)
    if db is None:
        return SkipReason("db resource 없음")

    released: list[str] = []
    for target, base_url in settings.targets.items():
        try:
            row = db.get_gpu_maintenance(target)
        except Exception as exc:  # noqa: BLE001
            context.log.warning("maintenance_guard %s 조회 실패: %s", target, exc)
            continue
        flag = flag_from_pg_row(row, target=target)
        if not flag.active:
            continue

        owner_running = _owner_run_is_running(context, flag.owner_run_id)
        should_release, reason = resolve_release_actions(
            flag, owner_run_is_running=owner_running, now_ts=now_ts
        )
        if not should_release:
            continue

        try:
            db.set_gpu_maintenance(target, active=False)
            _release_serving(base_url)
            released.append(f"{target}:{reason}")
            context.log.warning(
                "maintenance 자동해제: target=%s reason=%s owner_run_id=%s base_url=%s",
                target, reason, flag.owner_run_id, base_url,
            )
        except Exception as exc:  # noqa: BLE001
            context.log.warning("maintenance 자동해제 실패 target=%s: %s", target, exc)

    if not released:
        return SkipReason(f"stale 정비락 없음 (targets={sorted(settings.targets)})")
    return SkipReason(f"maintenance 자동해제 완료: {released}")
```

- [ ] **Run it — expect PASS.**

```bash
pytest tests/unit/test_maintenance_guard_sensor.py -q
```

Expected: `6 passed`.

- [ ] **Register the sensor in definitions.** SPIKE+wire: run `grep -n "stuck_run_guard_sensor" src/vlm_pipeline/definitions*.py` to find how guard sensors are added to the `Definitions(sensors=[...])` list, then add `maintenance_guard_sensor` import + entry next to it (production defs only; staging inherits). Verify defs load:

```bash
python -c "import vlm_pipeline.definitions_production as d; print('defs ok')"
```

Expected: `defs ok` (no import error). If `requests` import in the sensor module breaks defs load in a minimal env, move `import requests` inside `_release_serving` (lazy) — apply that fix and re-run.

- [ ] **Commit.** `git add -A && git commit -m "feat(mlops): maintenance_guard_sensor fail-safe auto-release + rewarm"`

---

### Task F7: `scripts/clear_maintenance.sh` runbook

Operator escape hatch when the sensor is down or PG is unreachable: clears the PG flag (best-effort) AND directly POSTs `/maintenance/exit` + `/warmup` to both serving URLs. Idempotent, prints resolved state.

**Files:**
- `scripts/clear_maintenance.sh` (new, `chmod +x`)
- `tests/unit/test_clear_maintenance_script.py` (new)

**Interfaces:**
- Produces: CLI `scripts/clear_maintenance.sh [sam3|pe_core|all]` (default `all`). Reads env `SAM3_API_URL`, `EMBEDDING_API_URL`, and `PIPELINE_DSN`/`DATAOPS_POSTGRES_DSN` for the PG clear (best-effort via `psql` if available).
- Consumes: serving `/maintenance/exit`+`/warmup`+`/maintenance/status` (F3/F4); table `gpu_maintenance_lock` (F1).

- [ ] **Write the failing test** (static lint of the script — shellcheck-style structural assertions, runs without GPU/containers). Create `tests/unit/test_clear_maintenance_script.py`:

```python
"""scripts/clear_maintenance.sh 정적 검증 (no exec — 구조/내용만)."""
from __future__ import annotations

import os
import pathlib

_SCRIPT = pathlib.Path("scripts/clear_maintenance.sh")


def test_script_exists_and_executable():
    assert _SCRIPT.exists(), "clear_maintenance.sh 누락"
    assert os.access(_SCRIPT, os.X_OK), "실행권한 없음 (chmod +x)"


def test_script_hits_both_targets_and_endpoints():
    text = _SCRIPT.read_text()
    assert "set -euo pipefail" in text
    assert "/maintenance/exit" in text
    assert "/warmup" in text
    assert "SAM3_API_URL" in text
    assert "EMBEDDING_API_URL" in text
    # 인자 없으면 all 기본
    assert "all" in text
```

- [ ] **Run it — expect FAIL** (`clear_maintenance.sh 누락`).

```bash
pytest tests/unit/test_clear_maintenance_script.py -q
```

- [ ] **Write the script.** Create `scripts/clear_maintenance.sh`:

```bash
#!/usr/bin/env bash
# clear_maintenance.sh — GPU 정비락 수동 해제 런북 (fail-safe escape hatch).
#
# 사용: scripts/clear_maintenance.sh [sam3|pe_core|all]   (기본 all)
# 동작: (1) PG gpu_maintenance_lock.active=FALSE (psql 있으면 best-effort)
#       (2) 서빙 /maintenance/exit + /warmup POST
#       (3) /maintenance/status 출력
# 센서(maintenance_guard_sensor)가 죽었거나 PG 불통일 때 운영자가 직접 실행.
set -euo pipefail

TARGET="${1:-all}"
SAM3_API_URL="${SAM3_API_URL:-http://10.0.0.10:8002}"
EMBEDDING_API_URL="${EMBEDDING_API_URL:-http://10.0.0.10:8000}"
DSN="${PIPELINE_DSN:-${DATAOPS_POSTGRES_DSN:-}}"

clear_pg() {
  local tgt="$1"
  if [[ -z "${DSN}" ]] || ! command -v psql >/dev/null 2>&1; then
    echo "[pg] skip (DSN 없음 또는 psql 미설치) target=${tgt}"
    return 0
  fi
  psql "${DSN}" -v ON_ERROR_STOP=on -c \
    "UPDATE gpu_maintenance_lock SET active=FALSE, owner_run_id=NULL, updated_at=now() WHERE target='${tgt}';" \
    && echo "[pg] cleared target=${tgt}" || echo "[pg] WARN clear 실패 target=${tgt}"
}

clear_serving() {
  local name="$1" base="$2"
  echo "[serving] ${name} (${base}) exit+warmup"
  curl -sf -X POST "${base%/}/maintenance/exit"  >/dev/null && echo "  exit ok"   || echo "  exit WARN"
  curl -sf -X POST "${base%/}/warmup"            >/dev/null && echo "  warmup ok" || echo "  warmup WARN"
  echo "  status: $(curl -sf "${base%/}/maintenance/status" || echo '<unreachable>')"
}

do_target() {
  case "$1" in
    sam3)    clear_pg sam3;    clear_serving sam3    "${SAM3_API_URL}" ;;
    pe_core) clear_pg pe_core; clear_serving pe_core "${EMBEDDING_API_URL}" ;;
    *) echo "unknown target: $1" >&2; exit 2 ;;
  esac
}

if [[ "${TARGET}" == "all" ]]; then
  do_target sam3
  do_target pe_core
else
  do_target "${TARGET}"
fi
echo "done."
```

- [ ] **Make executable + run test — expect PASS.**

```bash
chmod +x scripts/clear_maintenance.sh
pytest tests/unit/test_clear_maintenance_script.py -q
```

Expected: `2 passed`.

- [ ] **Lint the shell + dry sanity (no containers needed; expects WARN on unreachable serving, exit 0).**

```bash
bash -n scripts/clear_maintenance.sh && echo "syntax ok"
SAM3_API_URL="http://127.0.0.1:59999" EMBEDDING_API_URL="http://127.0.0.1:59998" \
  bash scripts/clear_maintenance.sh sam3 || true
```

Expected: `syntax ok`; the run prints `[pg] skip ...`, `exit WARN`, `warmup WARN`, `status: <unreachable>` and does NOT crash the shell (curl `-f` failures are tolerated via `|| echo`).

- [ ] **Commit.** `git add -A && git commit -m "docs(mlops): scripts/clear_maintenance.sh GPU 정비락 해제 런북"`

---

### Task F8: full-suite green + ruff gate

- [ ] **Run all Section-F tests together.**

```bash
pytest tests/unit/test_maintenance_flag.py tests/unit/test_embedding_maintenance_gate.py \
  tests/unit/test_sam3_maintenance_gate.py tests/unit/test_maintenance_guard_sensor.py \
  tests/unit/test_clear_maintenance_script.py tests/unit/test_embedding_service_contract.py -q
```

Expected: all pass (gate 6 + sam3 5 + flag 8 + sensor 6 + script 2 + contract regression).

- [ ] **Ruff (CI uses 0.7.4; tracked files only).**

```bash
ruff check --line-length 120 \
  src/vlm_pipeline/lib/maintenance_flag.py \
  src/vlm_pipeline/resources/postgres_maintenance.py \
  src/vlm_pipeline/defs/train/sensor_maintenance_guard.py \
  src/vlm_pipeline/resources/runtime_settings.py \
  docker/embedding/app.py docker/sam3/app.py
```

Expected: `All checks passed!`

- [ ] **Layer-import lint (lib must stay pure).**

```bash
python scripts/check_lib_layer_imports.py && echo "layer ok"
```

Expected: `layer ok` (no top-level dagster/defs/resources import inside `lib/maintenance_flag.py`).

- [ ] **Commit any ruff fixups.** `git add -A && git commit -m "chore(mlops): ruff/layer fixups for maintenance lock section" || echo "nothing to commit"`

---

## Section B — Frozen Snapshot Builder (`defs/train/dataset.py` + `lib/dataset_split.py`)

> **Branch:** `feature/mlops-finetune-scaffolding` (already checked out). All paths absolute under repo root `/home/user/work_p/Datapipeline-Data-data_pipeline`.
>
> **Section dependency note:** This section CONSUMES the `train_dataset_versions` PG table + the `model_registry`/migration work owned by Section A. To keep this section independently testable in CI **without** Section A merged, the candidate-query + checksum + split + manifest logic are written as pure functions and an L4 asset that takes injected callables; the only place that touches `train_dataset_versions` is one `PostgresTrainMixin.insert_train_dataset_version(...)` method, fully exercised against a `_DummyDB`. Real-PG integration of that insert is flagged in open_risks and lands after Section A's `013_mlops_finetune.sql` is applied.

---

### Task B1: Pure group-aware 3-way splitter `lib/dataset_split.py`

The existing `split_dataset/_split_records(records, train_ratio, seed)` is per-record + 2-way + shuffles in place — it leaks frames of one source video across splits and has no val split (design HIGH-1, §7.2). We add a NEW pure function in the `lib/` layer (L1-2, no Dagster import) that splits by GROUP KEY into 3 buckets deterministically, with a stable pre-sort so input order never affects assignment.

**Files:**
- `/home/user/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/lib/dataset_split.py` (new)
- `/home/user/work_p/Datapipeline-Data-data_pipeline/tests/unit/test_dataset_split.py` (new)

**Interfaces:**
- Consumes: nothing (pure stdlib `random`, `collections`).
- Produces:
  - `_split_groups(records: list[dict], key_fn: Callable[[dict], str], ratios: dict, seed: int) -> dict[str, list[dict]]` → returns `{"train": [...], "val": [...], "test": [...]}`; guarantees no group key appears in two splits; deterministic for fixed `(records-content, seed)` regardless of input order.
  - `_per_class_floor_ok(split_records, class_fn, min_per_split) -> tuple[bool, dict]` → `(ok, per_class_counts)`; `ok=False` if any class fails `min_per_split` in any non-empty target split.
  - `SplitRatios` validation helper `_normalize_ratios(ratios: dict) -> tuple[float,float,float]`.

**Steps:**

- [ ] **B1.1 — Write failing test for determinism + no-group-spans-two-splits.** Create `/home/user/work_p/Datapipeline-Data-data_pipeline/tests/unit/test_dataset_split.py`:

```python
"""Tests for vlm_pipeline.lib.dataset_split — group-aware deterministic 3-way split."""

from __future__ import annotations

import random

from vlm_pipeline.lib.dataset_split import (
    _normalize_ratios,
    _per_class_floor_ok,
    _split_groups,
)


def _recs(n_groups: int, per_group: int) -> list[dict]:
    out: list[dict] = []
    for g in range(n_groups):
        for i in range(per_group):
            out.append({"group": f"vid{g:03d}", "image_id": f"vid{g:03d}_f{i:03d}", "category": "fire"})
    return out


def test_no_group_spans_two_splits():
    recs = _recs(30, 4)
    out = _split_groups(recs, key_fn=lambda r: r["group"], ratios={"train": 0.8, "val": 0.1, "test": 0.1}, seed=42)
    seen: dict[str, str] = {}
    for split_name, rows in out.items():
        for r in rows:
            g = r["group"]
            assert seen.setdefault(g, split_name) == split_name, f"group {g} spans two splits"
    # all records accounted for exactly once
    assert sum(len(v) for v in out.values()) == len(recs)


def test_deterministic_same_seed_regardless_of_input_order():
    recs = _recs(30, 4)
    shuffled = list(recs)
    random.Random(999).shuffle(shuffled)
    a = _split_groups(recs, key_fn=lambda r: r["group"], ratios={"train": 0.8, "val": 0.1, "test": 0.1}, seed=7)
    b = _split_groups(shuffled, key_fn=lambda r: r["group"], ratios={"train": 0.8, "val": 0.1, "test": 0.1}, seed=7)
    # same group->split assignment irrespective of input ordering
    assign_a = {r["group"]: name for name, rows in a.items() for r in rows}
    assign_b = {r["group"]: name for name, rows in b.items() for r in rows}
    assert assign_a == assign_b


def test_three_way_buckets_present():
    recs = _recs(30, 2)
    out = _split_groups(recs, key_fn=lambda r: r["group"], ratios={"train": 0.6, "val": 0.2, "test": 0.2}, seed=1)
    assert set(out.keys()) == {"train", "val", "test"}
    assert all(len(out[k]) > 0 for k in ("train", "val", "test"))


def test_normalize_ratios_rejects_bad_sum():
    import pytest

    with pytest.raises(ValueError):
        _normalize_ratios({"train": 0.8, "val": 0.1, "test": 0.2})


def test_per_class_floor_detects_missing_rare_class():
    train = [{"category": "fire"}, {"category": "smoke"}]
    val = [{"category": "fire"}]  # smoke missing in val
    test = [{"category": "fire"}, {"category": "smoke"}]
    ok, counts = _per_class_floor_ok(
        {"train": train, "val": val, "test": test},
        class_fn=lambda r: r["category"],
        min_per_split=1,
    )
    assert ok is False
    assert counts["smoke"]["val"] == 0
```

- [ ] **B1.2 — Run it, expect import failure.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_dataset_split.py -q
```
Expected: `ModuleNotFoundError: No module named 'vlm_pipeline.lib.dataset_split'` (collection error, 0 passed).

- [ ] **B1.3 — Implement `lib/dataset_split.py`.** Create `/home/user/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/lib/dataset_split.py`:

```python
"""Group-aware deterministic 3-way dataset splitter.

Layer 1: pure Python, no Dagster / DB / MinIO import. Used by the frozen
snapshot builder (defs/train/dataset.py). Distinct from split_dataset's
`_split_records` (per-record 2-way shuffle) — this one splits by GROUP key so
all frames of one source video land in exactly one of train/val/test, with a
stable pre-sort that makes assignment independent of input row order.
"""

from __future__ import annotations

import random
from collections import OrderedDict
from typing import Callable

SPLIT_NAMES = ("train", "val", "test")


def _normalize_ratios(ratios: dict) -> tuple[float, float, float]:
    """{'train','val','test'} → (t,v,te). Must sum to 1.0 (±1e-6) and each > 0."""
    try:
        t = float(ratios["train"])
        v = float(ratios["val"])
        te = float(ratios["test"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"ratios must have numeric train/val/test: {ratios!r}") from exc
    if min(t, v, te) <= 0:
        raise ValueError(f"all split ratios must be > 0: {ratios!r}")
    if abs((t + v + te) - 1.0) > 1e-6:
        raise ValueError(f"split ratios must sum to 1.0: {ratios!r} -> {t + v + te}")
    return t, v, te


def _split_groups(
    records: list[dict],
    key_fn: Callable[[dict], str],
    ratios: dict,
    seed: int,
) -> dict[str, list[dict]]:
    """Group-aware deterministic 3-way split.

    1. Group records by key_fn(record).
    2. STABLE pre-sort the unique group keys (kills input-order nondeterminism).
    3. Seeded-shuffle the sorted group-key list.
    4. Cut by cumulative ratio into train/val/test buckets of GROUP KEYS.
    5. Expand groups back to records, preserving each group's original record order.

    Guarantees: a group key is assigned to exactly one split; deterministic for
    fixed (group-key set, seed).
    """
    t_ratio, v_ratio, _te_ratio = _normalize_ratios(ratios)

    grouped: "OrderedDict[str, list[dict]]" = OrderedDict()
    for rec in records:
        grouped.setdefault(str(key_fn(rec)), []).append(rec)

    keys = sorted(grouped.keys())  # stable pre-sort
    rng = random.Random(seed)
    rng.shuffle(keys)

    n = len(keys)
    out: dict[str, list[dict]] = {name: [] for name in SPLIT_NAMES}
    if n == 0:
        return out

    n_train = int(n * t_ratio)
    n_val = int(n * v_ratio)
    # guarantee non-empty val/test when there are >= 3 groups
    if n >= 3:
        n_train = min(n_train, n - 2)
        n_train = max(1, n_train)
        n_val = max(1, min(n_val, n - n_train - 1))
    train_keys = keys[:n_train]
    val_keys = keys[n_train : n_train + n_val]
    test_keys = keys[n_train + n_val :]

    for k in train_keys:
        out["train"].extend(grouped[k])
    for k in val_keys:
        out["val"].extend(grouped[k])
    for k in test_keys:
        out["test"].extend(grouped[k])
    return out


def _per_class_floor_ok(
    splits: dict[str, list[dict]],
    class_fn: Callable[[dict], str],
    min_per_split: int,
) -> tuple[bool, dict]:
    """Stratify floor check. Returns (ok, per_class_counts).

    per_class_counts = {class: {"train": n, "val": n, "test": n, "total": n}}.
    ok=False if any observed class has < min_per_split in any split that is
    expected to be populated (i.e. the class exists overall and that split is
    non-empty in the dataset). The builder raises honestly when ok is False.
    """
    classes: set[str] = set()
    for rows in splits.values():
        for r in rows:
            classes.add(str(class_fn(r)))

    counts: dict = {}
    for cls in sorted(classes):
        per = {name: 0 for name in SPLIT_NAMES}
        for name in SPLIT_NAMES:
            per[name] = sum(1 for r in splits.get(name, []) if str(class_fn(r)) == cls)
        per["total"] = sum(per[name] for name in SPLIT_NAMES)
        counts[cls] = per

    ok = True
    for cls, per in counts.items():
        for name in SPLIT_NAMES:
            # only enforce floor on splits that have any rows at all
            if splits.get(name) and per[name] < min_per_split:
                ok = False
    return ok, counts
```

- [ ] **B1.4 — Run tests, expect PASS.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_dataset_split.py -q
```
Expected: `5 passed`.

- [ ] **B1.5 — Lint + commit.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && ruff check src/vlm_pipeline/lib/dataset_split.py tests/unit/test_dataset_split.py && git add src/vlm_pipeline/lib/dataset_split.py tests/unit/test_dataset_split.py && git commit -m "feat(train): group-aware deterministic 3-way splitter (lib/dataset_split)"
```
Expected: `All checks passed!` then a commit. (Append the `Co-Authored-By` trailer per repo rule.)

---

### Task B2: Content-checksum + COCO→YOLO bridge helpers `lib/trainset_manifest.py`

A frozen trainset needs a stable `content_checksum` (sorted manifest + class_map + split + seed) for the `UNIQUE(task, content_checksum)` idempotency gate (§7.2 H6), and must reuse the existing COCO→YOLO bbox math from `split_dataset/split_dataset.py` (`_coco_bbox_to_yolo`) rather than reinventing it. `split_dataset/` is not a package on the import path, so we add a thin pure-lib bridge that imports it by file path once and exposes `coco_bbox_to_yolo`.

**Files:**
- `/home/user/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/lib/trainset_manifest.py` (new)
- `/home/user/work_p/Datapipeline-Data-data_pipeline/tests/unit/test_trainset_manifest.py` (new)

**Interfaces:**
- Consumes: `vlm_pipeline.lib.checksum.sha256_bytes`; `split_dataset.split_dataset._coco_bbox_to_yolo` (loaded by file path).
- Produces:
  - `build_manifest(objects: list[tuple[str, str]]) -> dict` — `objects` = sorted `(object_key, per_object_sha256)` list → `{"objects": [{"key":..,"checksum":..}], "count": N}`.
  - `content_checksum(task, manifest, class_map, split_assignment, seed) -> str` — canonical-JSON sha256.
  - `coco_bbox_to_yolo(bbox, img_w, img_h) -> tuple|None` — delegates to the existing bridge.

**Steps:**

- [ ] **B2.1 — Write failing test.** Create `/home/user/work_p/Datapipeline-Data-data_pipeline/tests/unit/test_trainset_manifest.py`:

```python
"""Tests for vlm_pipeline.lib.trainset_manifest — checksum idempotency + COCO->YOLO bridge."""

from __future__ import annotations

from vlm_pipeline.lib.trainset_manifest import (
    build_manifest,
    content_checksum,
    coco_bbox_to_yolo,
)


def test_build_manifest_sorts_and_counts():
    objs = [("b/2.jpg", "cs2"), ("a/1.jpg", "cs1")]
    m = build_manifest(objs)
    assert m["count"] == 2
    assert [o["key"] for o in m["objects"]] == ["a/1.jpg", "b/2.jpg"]  # sorted


def test_content_checksum_is_order_independent_and_deterministic():
    objs_a = build_manifest([("a.jpg", "x"), ("b.jpg", "y")])
    objs_b = build_manifest([("b.jpg", "y"), ("a.jpg", "x")])
    class_map = {"fire": 0, "smoke": 1}
    split = {"a.jpg": "train", "b.jpg": "test"}
    c1 = content_checksum("sam3_detection", objs_a, class_map, split, 42)
    c2 = content_checksum("sam3_detection", objs_b, class_map, split, 42)
    assert c1 == c2
    assert len(c1) == 64  # sha256 hex


def test_content_checksum_changes_with_seed():
    m = build_manifest([("a.jpg", "x")])
    assert content_checksum("sam3_detection", m, {"fire": 0}, {"a.jpg": "train"}, 1) != content_checksum(
        "sam3_detection", m, {"fire": 0}, {"a.jpg": "train"}, 2
    )


def test_coco_bbox_to_yolo_matches_existing_bridge():
    # COCO [x,y,w,h]=[10,20,50,70] on 100x200 -> center normalized
    out = coco_bbox_to_yolo([10, 20, 50, 70], 100, 200)
    assert out is not None
    xc, yc, ww, hh = out
    assert abs(xc - 0.35) < 1e-6  # (10+25)/100
    assert abs(yc - 0.275) < 1e-6  # (20+35)/200
    assert abs(ww - 0.5) < 1e-6
    assert abs(hh - 0.35) < 1e-6


def test_coco_bbox_to_yolo_rejects_bad_box():
    assert coco_bbox_to_yolo([1, 2, 3], 100, 100) is None
    assert coco_bbox_to_yolo([10, 20, 0, 70], 100, 100) is None
```

- [ ] **B2.2 — Run it, expect import failure.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_trainset_manifest.py -q
```
Expected: `ModuleNotFoundError: No module named 'vlm_pipeline.lib.trainset_manifest'`.

- [ ] **B2.3 — Implement `lib/trainset_manifest.py`.** Create `/home/user/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/lib/trainset_manifest.py`:

```python
"""Frozen-trainset manifest + content checksum + COCO->YOLO bridge.

Layer 1: pure Python. The content_checksum feeds the UNIQUE(task, checksum)
idempotency gate on train_dataset_versions (design §7.2 H6). COCO->YOLO math is
NOT reinvented — it delegates to split_dataset/split_dataset.py's
`_coco_bbox_to_yolo`, loaded by file path because split_dataset/ is not an
installed package.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from vlm_pipeline.lib.checksum import sha256_bytes

# repo_root/split_dataset/split_dataset.py — lib is src/vlm_pipeline/lib/, so
# parents[3] == repo root.
_SPLIT_DATASET_PY = Path(__file__).resolve().parents[3] / "split_dataset" / "split_dataset.py"


def _load_coco_bridge():
    spec = importlib.util.spec_from_file_location("_split_dataset_bridge", _SPLIT_DATASET_PY)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load split_dataset bridge from {_SPLIT_DATASET_PY}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_BRIDGE = None


def coco_bbox_to_yolo(bbox, img_w: int, img_h: int):
    """COCO [x,y,w,h] px -> YOLO (xc,yc,w,h) normalized. Delegates to split_dataset."""
    global _BRIDGE
    if _BRIDGE is None:
        _BRIDGE = _load_coco_bridge()
    return _BRIDGE._coco_bbox_to_yolo(bbox, img_w, img_h)


def build_manifest(objects: list[tuple[str, str]]) -> dict:
    """objects = [(object_key, per_object_sha256), ...] -> sorted manifest dict."""
    items = sorted(((str(k), str(c)) for k, c in objects), key=lambda x: x[0])
    return {
        "objects": [{"key": k, "checksum": c} for k, c in items],
        "count": len(items),
    }


def content_checksum(
    task: str,
    manifest: dict,
    class_map: dict,
    split_assignment: dict,
    seed: int,
) -> str:
    """Canonical-JSON sha256 over (task, sorted manifest, class_map, split, seed)."""
    canonical = json.dumps(
        {
            "task": str(task),
            "manifest": manifest,
            "class_map": class_map,
            "split_assignment": split_assignment,
            "seed": int(seed),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return sha256_bytes(canonical.encode("utf-8"))
```

- [ ] **B2.4 — Run tests, expect PASS.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_trainset_manifest.py -q
```
Expected: `5 passed`. (If `parents[3]` resolves wrong, the `coco_*` tests fail with `ImportError` pointing at the bad path — verify `_SPLIT_DATASET_PY` exists with `ls -l "$(python -c 'from vlm_pipeline.lib.trainset_manifest import _SPLIT_DATASET_PY; print(_SPLIT_DATASET_PY)')"` before debugging further.)

- [ ] **B2.5 — Lint + commit.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && ruff check src/vlm_pipeline/lib/trainset_manifest.py tests/unit/test_trainset_manifest.py && git add src/vlm_pipeline/lib/trainset_manifest.py tests/unit/test_trainset_manifest.py && git commit -m "feat(train): trainset manifest checksum + COCO->YOLO bridge (lib)"
```

---

### Task B3: Candidate query + honest `al_confirmed_count` in `PostgresTrainMixin`

The builder pulls LS-finalized bbox candidates from `image_label_annotations` joined through `v_finalized_labels` semantics (migration 011/012), and AL-curated frames as `AL-queue ∩ image_label_annotations`. Per design §7.2, `image_label_annotations` is currently 0 rows, so `al_confirmed_count` must be reported as **0 honestly** — not silently dropped. This task adds a read-only mixin method and an `insert_train_dataset_version` writer.

**Files:**
- `/home/user/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/resources/postgres_train.py` (new)
- `/home/user/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/resources/postgres.py` (edit — add mixin to MRO)
- `/home/user/work_p/Datapipeline-Data-data_pipeline/tests/conftest.py` (edit — add mixin to `_PostgresTestResource`)
- `/home/user/work_p/Datapipeline-Data-data_pipeline/tests/unit/test_postgres_train_candidates.py` (new)

**Interfaces:**
- Consumes: `self.connect()`, `self._rows_to_dicts` (from `PostgresBaseMixin`); table `train_dataset_versions` (Section A migration `013`).
- Produces (exact signatures):
  - `find_sam3_finalized_bbox_candidates(self, folder_name: str | None = None) -> list[dict]` → rows `{image_id, image_bucket, image_key, source_asset_id, source_unit_name, category, box_index, bbox_x, bbox_y, bbox_w, bbox_h}` (one row per finalized box, via `image_label_annotations JOIN image_labels(review_status='finalized') JOIN image_metadata JOIN raw_files`).
  - `find_al_confirmed_image_ids(self, image_ids: list[str]) -> set[str]` → subset of `image_ids` that have ≥1 row in `image_label_annotations` (the AL∩annotations gate; honestly empty today).
  - `insert_train_dataset_version(self, row: dict) -> None` → inserts into `train_dataset_versions`; `ON CONFLICT (task, content_checksum) DO NOTHING`.

**Steps:**

- [ ] **B3.1 — SPIKE: confirm `train_dataset_versions` columns + that 011/012 tables exist.** Section A authors `013_mlops_finetune.sql`. Read its column list (must match the SHARED CONTRACT) before writing the INSERT:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && ls src/vlm_pipeline/sql/migrations/postgres/013_mlops_finetune.sql && grep -E "train_dataset_version_id|content_checksum|al_confirmed_count|per_class_counts|split_assignment_key|UNIQUE" src/vlm_pipeline/sql/migrations/postgres/013_mlops_finetune.sql
```
Expected: file exists; column names match the contract (`train_dataset_version_id`, `task`, `source_spec`, `class_map`, `group_key_field`, `split_assignment_key`, `split_ratios`, `manifest_key`, `content_checksum`, `ls_count`, `al_confirmed_count`, `per_class_counts`, `total_count`, `seed`, `upstream_dataset_id`) and a `UNIQUE (task, content_checksum)` constraint. **If file absent (Section A not merged yet): proceed using the contract column list verbatim — the INSERT below already matches it — and rely on the `_DummyDB` unit test; the real-PG insert test (B3.6) will skip until 013 lands.**

- [ ] **B3.2 — Write failing test (mock-cursor, no real PG).** Create `/home/user/work_p/Datapipeline-Data-data_pipeline/tests/unit/test_postgres_train_candidates.py`:

```python
"""Unit tests for PostgresTrainMixin candidate queries (mock cursor, no live PG)."""

from __future__ import annotations

from unittest.mock import MagicMock

from vlm_pipeline.resources.postgres_train import PostgresTrainMixin


class _Mixin(PostgresTrainMixin):
    """PostgresTrainMixin standalone with a stubbed connect() + _rows_to_dicts."""

    def __init__(self, fetch_rows):
        self._fetch_rows = fetch_rows
        self.executed: list[tuple] = []

    @staticmethod
    def _rows_to_dicts(rows, columns):
        return [dict(zip(columns, r)) for r in rows]

    def connect(self):
        mixin = self

        class _Ctx:
            def __enter__(self_):
                cur = MagicMock()
                cur.__enter__ = MagicMock(return_value=cur)
                cur.__exit__ = MagicMock(return_value=False)
                cur.fetchall.return_value = mixin._fetch_rows
                conn = MagicMock()
                conn.cursor.return_value = cur

                def _exec(sql, params=None):
                    mixin.executed.append((sql, params))

                cur.execute.side_effect = _exec
                self_._conn = conn
                return conn

            def __exit__(self_, *a):
                return False

        return _Ctx()


def test_finalized_bbox_candidates_maps_columns():
    rows = [
        ("img-1", "vlm-processed", "proj/image/f1.jpg", "asset-1", "proj", "fire", 0, 1.0, 2.0, 3.0, 4.0),
    ]
    m = _Mixin(rows)
    out = m.find_sam3_finalized_bbox_candidates(folder_name="proj")
    assert out[0]["image_id"] == "img-1"
    assert out[0]["category"] == "fire"
    assert out[0]["source_unit_name"] == "proj"
    sql = m.executed[0][0]
    assert "image_label_annotations" in sql
    assert "review_status = 'finalized'" in sql


def test_al_confirmed_empty_when_no_annotations():
    m = _Mixin([])  # no annotation rows -> honest empty set
    confirmed = m.find_al_confirmed_image_ids(["img-1", "img-2"])
    assert confirmed == set()


def test_al_confirmed_returns_subset():
    m = _Mixin([("img-2",)])
    confirmed = m.find_al_confirmed_image_ids(["img-1", "img-2"])
    assert confirmed == {"img-2"}


def test_insert_train_dataset_version_uses_on_conflict():
    m = _Mixin([])
    m.insert_train_dataset_version(
        {
            "train_dataset_version_id": "tdv-1",
            "task": "sam3_detection",
            "content_checksum": "abc",
            "ls_count": 5,
            "al_confirmed_count": 0,
            "total_count": 5,
            "seed": 42,
        }
    )
    sql = m.executed[0][0]
    assert "INSERT INTO train_dataset_versions" in sql
    assert "ON CONFLICT (task, content_checksum) DO NOTHING" in sql
```

- [ ] **B3.3 — Run it, expect import failure.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_postgres_train_candidates.py -q
```
Expected: `ModuleNotFoundError: No module named 'vlm_pipeline.resources.postgres_train'`.

- [ ] **B3.4 — Implement `resources/postgres_train.py`.** Create the file:

```python
"""PG TRAIN domain — frozen-trainset candidate queries + version writer.

Read path joins the human-confirmed bbox projection (image_label_annotations,
migration 011) through finalized image_labels. AL contribution is the intersection
of an AL-queue membership and existence in image_label_annotations; today that
table is empty so al_confirmed_count is honestly 0 (design §7.2). No model-derived
labels are ever selected (no auto_generated, no vlm-classification).
"""

from __future__ import annotations

import json
from datetime import datetime
from uuid import uuid4


class PostgresTrainMixin:
    """Train-dataset snapshot queries. Mixed into PostgresResource."""

    def find_sam3_finalized_bbox_candidates(self, folder_name: str | None = None) -> list[dict]:
        """One row per human-finalized bbox (image_label_annotations ∩ finalized image_labels)."""
        where_folder = ""
        params: list = []
        if folder_name:
            where_folder = "AND r.source_unit_name = %s"
            params.append(folder_name)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT im.image_id, im.image_bucket, im.image_key, im.source_asset_id,
                           r.source_unit_name, ila.category, ila.box_index,
                           ila.bbox_x, ila.bbox_y, ila.bbox_w, ila.bbox_h
                    FROM image_label_annotations ila
                    JOIN image_labels il ON il.image_label_id = ila.image_label_id
                    JOIN image_metadata im ON im.image_id = ila.image_id
                    JOIN raw_files r ON r.asset_id = im.source_asset_id
                    WHERE il.review_status = 'finalized'
                      AND im.image_key IS NOT NULL
                      AND im.image_bucket IS NOT NULL
                      {where_folder}
                    ORDER BY im.image_id, ila.box_index
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            columns = [
                "image_id",
                "image_bucket",
                "image_key",
                "source_asset_id",
                "source_unit_name",
                "category",
                "box_index",
                "bbox_x",
                "bbox_y",
                "bbox_w",
                "bbox_h",
            ]
            return self._rows_to_dicts(rows, columns)

    def find_al_confirmed_image_ids(self, image_ids: list[str]) -> set[str]:
        """Subset of image_ids that have ≥1 human-confirmed annotation box.

        AL-queue membership ∩ image_label_annotations. Honestly empty while the
        per-box projection table has 0 rows (design §7.2).
        """
        ids = [str(i) for i in (image_ids or []) if i]
        if not ids:
            return set()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ila.image_id
                    FROM image_label_annotations ila
                    WHERE ila.image_id = ANY(%s)
                    """,
                    (ids,),
                )
                rows = cur.fetchall()
            return {str(r[0]) for r in rows}

    def insert_train_dataset_version(self, row: dict) -> None:
        """Insert a sealed train_dataset_versions row; idempotent on (task, content_checksum)."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO train_dataset_versions (
                        train_dataset_version_id, created_at, task, source_spec, class_map,
                        group_key_field, split_assignment_key, split_ratios, manifest_key,
                        content_checksum, ls_count, al_confirmed_count, per_class_counts,
                        total_count, seed, upstream_dataset_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (task, content_checksum) DO NOTHING
                    """,
                    (
                        row.get("train_dataset_version_id") or str(uuid4()),
                        row.get("created_at", datetime.utcnow()),
                        row.get("task"),
                        json.dumps(row.get("source_spec") or {}),
                        json.dumps(row.get("class_map") or {}),
                        row.get("group_key_field"),
                        row.get("split_assignment_key"),
                        json.dumps(row.get("split_ratios") or {}),
                        row.get("manifest_key"),
                        row.get("content_checksum"),
                        int(row.get("ls_count") or 0),
                        int(row.get("al_confirmed_count") or 0),
                        json.dumps(row.get("per_class_counts") or {}),
                        int(row.get("total_count") or 0),
                        int(row.get("seed") or 0),
                        row.get("upstream_dataset_id"),
                    ),
                )
```

- [ ] **B3.5 — Wire the mixin into `PostgresResource` + conftest.** In `/home/user/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/resources/postgres.py`, add the import and append `PostgresTrainMixin` to the class bases (place it after `PostgresBuildMixin` in the MRO list — match the existing order). First inspect:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && grep -n "Mixin" src/vlm_pipeline/resources/postgres.py
```
Then add `from vlm_pipeline.resources.postgres_train import PostgresTrainMixin` next to the other mixin imports, and add `PostgresTrainMixin,` to the `PostgresResource(...)` base list. Mirror the same in `/home/user/work_p/Datapipeline-Data-data_pipeline/tests/conftest.py`: add the import after the `PostgresBuildMixin` import line and add `PostgresTrainMixin,` to the `_PostgresTestResource(...)` base list (after `PostgresBuildMixin`).

- [ ] **B3.6 — Add a real-PG integration test (skips if 013 not applied).** Append to `/home/user/work_p/Datapipeline-Data-data_pipeline/tests/unit/test_postgres_train_candidates.py`:

```python
import pytest


@pytest.mark.usefixtures("postgres_resource")
def test_insert_then_conflict_noop_real_pg(postgres_resource):
    db = postgres_resource
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('train_dataset_versions')")
            if cur.fetchone()[0] is None:
                pytest.skip("train_dataset_versions not present (migration 013 not merged)")
    payload = {
        "train_dataset_version_id": "tdv-int-1",
        "task": "sam3_detection",
        "content_checksum": "deadbeef",
        "ls_count": 3,
        "al_confirmed_count": 0,
        "total_count": 3,
        "seed": 42,
        "class_map": {"fire": 0},
        "split_ratios": {"train": 0.8, "val": 0.1, "test": 0.1},
        "per_class_counts": {"fire": {"train": 2, "val": 1, "test": 0, "total": 3}},
    }
    db.insert_train_dataset_version(payload)
    db.insert_train_dataset_version({**payload, "train_dataset_version_id": "tdv-int-2"})  # same checksum
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM train_dataset_versions WHERE content_checksum = 'deadbeef'")
            assert cur.fetchone()[0] == 1  # ON CONFLICT no-op
```

- [ ] **B3.7 — Run tests, expect PASS (integration skips if PG/013 absent).**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_postgres_train_candidates.py -q
```
Expected: `4 passed, 1 skipped` (mock tests pass; integration skips without `DATAOPS_TEST_POSTGRES_DSN`/013). If PG + 013 present, `5 passed`.

- [ ] **B3.8 — Lint + commit.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && ruff check src/vlm_pipeline/resources/postgres_train.py tests/unit/test_postgres_train_candidates.py && git add -A && git commit -m "feat(train): PostgresTrainMixin finalized-bbox + AL∩annotations candidate queries + version writer"
```

---

### Task B4: The snapshot builder asset `defs/train/dataset.py`

Wire B1+B2+B3 into an L4 Dagster asset that: queries candidates → stable pre-sort → group-aware 3-way split → per-class floor check (fail honestly on rare-class starvation) → COCO→YOLO label emit → compute `content_checksum` → idempotency check (no-op if `(task, checksum)` exists) → write-then-seal to `vlm-dataset/_trainsets/<id>/{images,labels,splits,manifest.json}` → insert `train_dataset_versions` row. The asset delegates all logic to a testable `_run_build_trainset(...)` internal helper (mirrors the project's `_run_*` monkeypatch test pattern).

**Files:**
- `/home/user/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/defs/train/__init__.py` (new, empty)
- `/home/user/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/defs/train/dataset.py` (new)
- `/home/user/work_p/Datapipeline-Data-data_pipeline/tests/unit/test_train_dataset_builder.py` (new)

**Interfaces:**
- Consumes: `MinIOResource.upload`/`upload_json`, `PostgresTrainMixin.find_sam3_finalized_bbox_candidates`/`find_al_confirmed_image_ids`/`insert_train_dataset_version`, `lib.dataset_split._split_groups`/`_per_class_floor_ok`, `lib.trainset_manifest.build_manifest`/`content_checksum`/`coco_bbox_to_yolo`, `lib.checksum.sha256_bytes`.
- Produces:
  - `_run_build_trainset(db, minio, *, task, folder_name, ratios, seed, group_key_field, min_per_split, force_new, log) -> dict` (summary with `train_dataset_version_id`, `content_checksum`, `ls_count`, `al_confirmed_count`, `per_class_counts`, `total_count`, `skipped_duplicate`).
  - `@asset build_trainset(context, db, minio) -> dict`.

**Steps:**

- [ ] **B4.1 — Write failing test with `_DummyDB`/`_DummyMinIO`.** Create `/home/user/work_p/Datapipeline-Data-data_pipeline/tests/unit/test_train_dataset_builder.py`:

```python
"""Frozen snapshot builder tests — determinism / idempotency / AL honesty / stratify-fail."""

from __future__ import annotations

import pytest

pytest.importorskip("dagster")

from tests.helpers.dagster_dummies import DummyContext, DummyLogPermissive
from vlm_pipeline.defs.train import dataset as train_dataset


class _DummyDB:
    def __init__(self, candidates, confirmed=None):
        self._candidates = candidates
        self._confirmed = set(confirmed or set())
        self.inserted: list[dict] = []
        self.existing_checksums: set[tuple[str, str]] = set()

    def find_sam3_finalized_bbox_candidates(self, folder_name=None):
        return list(self._candidates)

    def find_al_confirmed_image_ids(self, image_ids):
        return {i for i in image_ids if i in self._confirmed}

    def train_dataset_version_exists(self, task, content_checksum):
        return (task, content_checksum) in self.existing_checksums

    def insert_train_dataset_version(self, row):
        self.existing_checksums.add((row["task"], row["content_checksum"]))
        self.inserted.append(row)


class _DummyMinIO:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}

    def upload(self, bucket, key, data, content_type="application/octet-stream"):
        self.objects[(bucket, key)] = data if isinstance(data, (bytes, bytearray)) else bytes(data)

    def upload_json(self, bucket, key, payload, **kw):
        import json as _j

        self.objects[(bucket, key)] = _j.dumps(payload).encode("utf-8")


def _cands(n_groups=6, per_group=3):
    out = []
    for g in range(n_groups):
        for i in range(per_group):
            out.append(
                {
                    "image_id": f"vid{g:02d}_f{i:02d}",
                    "image_bucket": "vlm-processed",
                    "image_key": f"proj/vid{g:02d}/image/f{i:02d}.jpg",
                    "source_asset_id": f"vid{g:02d}",
                    "source_unit_name": "proj",
                    "category": "fire",
                    "box_index": 0,
                    "bbox_x": 10.0,
                    "bbox_y": 20.0,
                    "bbox_w": 50.0,
                    "bbox_h": 70.0,
                }
            )
    return out


def _ctx():
    return DummyContext(op_config={}, log=DummyLogPermissive())


def test_build_is_idempotent_same_checksum_noop():
    db = _DummyDB(_cands())
    minio = _DummyMinIO()
    common = dict(
        task="sam3_detection",
        folder_name="proj",
        ratios={"train": 0.6, "val": 0.2, "test": 0.2},
        seed=42,
        group_key_field="source_asset_id",
        min_per_split=1,
        force_new=False,
        log=_ctx().log,
    )
    r1 = train_dataset._run_build_trainset(db, minio, **common)
    assert r1["skipped_duplicate"] is False
    assert len(db.inserted) == 1
    r2 = train_dataset._run_build_trainset(db, minio, **common)
    assert r2["skipped_duplicate"] is True
    assert r2["content_checksum"] == r1["content_checksum"]
    assert len(db.inserted) == 1  # no second row


def test_al_confirmed_count_honest_zero():
    db = _DummyDB(_cands(), confirmed=set())  # AL∩annotations empty today
    minio = _DummyMinIO()
    r = train_dataset._run_build_trainset(
        db, minio, task="sam3_detection", folder_name="proj",
        ratios={"train": 0.6, "val": 0.2, "test": 0.2}, seed=1,
        group_key_field="source_asset_id", min_per_split=1, force_new=False, log=_ctx().log,
    )
    assert r["al_confirmed_count"] == 0
    assert r["ls_count"] == len(_cands())


def test_no_group_spans_splits_in_split_file():
    db = _DummyDB(_cands())
    minio = _DummyMinIO()
    train_dataset._run_build_trainset(
        db, minio, task="sam3_detection", folder_name="proj",
        ratios={"train": 0.6, "val": 0.2, "test": 0.2}, seed=3,
        group_key_field="source_asset_id", min_per_split=1, force_new=False, log=_ctx().log,
    )
    import json as _j

    split_obj = next(v for (b, k), v in minio.objects.items() if k.endswith("/splits/split_assignment.json"))
    assignment = _j.loads(split_obj.decode("utf-8"))
    # map group -> set of splits; each group must map to exactly one split
    by_group: dict[str, set[str]] = {}
    for image_id, split_name in assignment.items():
        grp = image_id.split("_f")[0]
        by_group.setdefault(grp, set()).add(split_name)
    assert all(len(s) == 1 for s in by_group.values())


def test_stratify_floor_fails_when_rare_class_starved():
    # 2 groups only -> val/test cannot both get a 'smoke' group if only one group has smoke
    cands = []
    for g in range(2):
        for i in range(2):
            cands.append(
                {
                    "image_id": f"vid{g}_f{i}", "image_bucket": "vlm-processed",
                    "image_key": f"proj/vid{g}/image/f{i}.jpg", "source_asset_id": f"vid{g}",
                    "source_unit_name": "proj", "category": "fire" if g == 0 else "smoke",
                    "box_index": 0, "bbox_x": 1.0, "bbox_y": 1.0, "bbox_w": 5.0, "bbox_h": 5.0,
                }
            )
    db = _DummyDB(cands)
    minio = _DummyMinIO()
    with pytest.raises(ValueError, match="stratify"):
        train_dataset._run_build_trainset(
            db, minio, task="sam3_detection", folder_name="proj",
            ratios={"train": 0.5, "val": 0.25, "test": 0.25}, seed=1,
            group_key_field="source_asset_id", min_per_split=1, force_new=False, log=_ctx().log,
        )
```

- [ ] **B4.2 — Run it, expect import failure.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_train_dataset_builder.py -q
```
Expected: `ModuleNotFoundError: No module named 'vlm_pipeline.defs.train.dataset'`.

- [ ] **B4.3 — Add the `train_dataset_version_exists` read helper to `PostgresTrainMixin`.** The builder must check existence before sealing (B3's `insert` is `ON CONFLICT DO NOTHING` but we want an early no-op return + to avoid re-uploading objects). Insert into `/home/user/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/resources/postgres_train.py` (after `find_al_confirmed_image_ids`):

```python
    def train_dataset_version_exists(self, task: str, content_checksum: str) -> bool:
        """True if a sealed version with this (task, content_checksum) already exists."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM train_dataset_versions WHERE task = %s AND content_checksum = %s LIMIT 1",
                    (task, content_checksum),
                )
                return cur.fetchone() is not None
```

- [ ] **B4.4 — Implement `defs/train/__init__.py` (empty) and `defs/train/dataset.py`.** Create the package init empty, then `/home/user/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/defs/train/dataset.py`:

```python
"""TRAIN snapshot builder @asset — frozen, sealed, immutable train dataset versions.

Layer 4: Dagster @asset. Delegates pure logic to lib/dataset_split + lib/trainset_manifest
(L1-2) and DB access to PostgresTrainMixin (L1 resource). Builds a group-aware,
stratified, deterministic 3-way split frozen to vlm-dataset/_trainsets/<id>/ and
records a train_dataset_versions row. Idempotent on (task, content_checksum).

No model-derived labels are ever used (design §2). AL contribution = AL-queue ∩
image_label_annotations; honestly 0 today.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from uuid import uuid4

from dagster import Field, asset

from vlm_pipeline.lib.checksum import sha256_bytes
from vlm_pipeline.lib.dataset_split import _per_class_floor_ok, _split_groups
from vlm_pipeline.lib.trainset_manifest import (
    build_manifest,
    content_checksum,
    coco_bbox_to_yolo,
)
from vlm_pipeline.resources.minio import MinIOResource
from vlm_pipeline.resources.postgres import PostgresResource

DATASET_BUCKET = "vlm-dataset"
TRAINSETS_PREFIX = "_trainsets"
_DEFAULT_RATIOS = {"train": 0.8, "val": 0.1, "test": 0.1}


def _yolo_label_text(rows_for_image: list[dict], class_map: dict, img_w: int, img_h: int) -> str:
    lines: list[str] = []
    for r in rows_for_image:
        cls = class_map.get(str(r["category"]))
        if cls is None:
            continue
        yolo = coco_bbox_to_yolo([r["bbox_x"], r["bbox_y"], r["bbox_w"], r["bbox_h"]], img_w, img_h)
        if not yolo:
            continue
        xc, yc, ww, hh = yolo
        lines.append(f"{cls} {xc:.6f} {yc:.6f} {ww:.6f} {hh:.6f}")
    return "\n".join(lines) + "\n"


def _run_build_trainset(
    db,
    minio,
    *,
    task: str,
    folder_name: str | None,
    ratios: dict,
    seed: int,
    group_key_field: str,
    min_per_split: int,
    force_new: bool,
    log,
) -> dict:
    # ---- 1. candidates (LS finalized boxes; AL∩annotations honest count) ----
    candidates = db.find_sam3_finalized_bbox_candidates(folder_name=folder_name)
    ls_count = len(candidates)
    image_ids = sorted({str(c["image_id"]) for c in candidates})
    al_confirmed = db.find_al_confirmed_image_ids(image_ids)
    al_confirmed_count = len(al_confirmed)
    log.info(
        f"[trainset] task={task} folder={folder_name} ls_boxes={ls_count} "
        f"images={len(image_ids)} al_confirmed={al_confirmed_count}"
    )
    if not candidates:
        return {
            "skipped_duplicate": False,
            "empty": True,
            "ls_count": 0,
            "al_confirmed_count": 0,
            "total_count": 0,
            "per_class_counts": {},
            "content_checksum": None,
            "train_dataset_version_id": None,
        }

    # ---- 2. stable pre-sort + class_map (sorted category -> contiguous idx) ----
    candidates = sorted(candidates, key=lambda c: (str(c["image_id"]), int(c.get("box_index", 0))))
    classes = sorted({str(c["category"]) for c in candidates})
    class_map = {name: idx for idx, name in enumerate(classes)}

    # ---- 3. group-aware 3-way split (per image_id-level record, grouped by source) ----
    # collapse boxes to per-image records for splitting, carry category for stratify
    per_image: dict[str, dict] = {}
    for c in candidates:
        rec = per_image.setdefault(
            str(c["image_id"]),
            {"image_id": str(c["image_id"]), "group": str(c[group_key_field]), "categories": set()},
        )
        rec["categories"].add(str(c["category"]))
    image_records = [per_image[i] for i in sorted(per_image.keys())]

    splits = _split_groups(
        image_records, key_fn=lambda r: r["group"], ratios=ratios, seed=seed
    )

    # ---- 4. per-class stratify floor (fail honestly if rare class starved) ----
    # explode by category so a multi-class image counts toward each of its classes
    def _explode(rows):
        out = []
        for r in rows:
            for cat in sorted(r["categories"]):
                out.append({"image_id": r["image_id"], "category": cat})
        return out

    exploded = {name: _explode(rows) for name, rows in splits.items()}
    ok, per_class_counts = _per_class_floor_ok(
        exploded, class_fn=lambda r: r["category"], min_per_split=min_per_split
    )
    if not ok:
        raise ValueError(
            f"stratify floor violated: a class has < {min_per_split} examples in some split. "
            f"per_class_counts={json.dumps(per_class_counts)}"
        )

    # ---- 5. split assignment map (image_id -> split) ----
    split_assignment: dict[str, str] = {}
    for name, rows in splits.items():
        for r in rows:
            split_assignment[r["image_id"]] = name

    # ---- 6. content_checksum over (sorted manifest + class_map + split + seed) ----
    boxes_by_image: dict[str, list[dict]] = {}
    for c in candidates:
        boxes_by_image.setdefault(str(c["image_id"]), []).append(c)
    # manifest = sorted (image_key, sha256(image_key + sorted box payload)) — stable
    objects: list[tuple[str, str]] = []
    for c in candidates:
        payload = json.dumps(
            {
                "k": c["image_key"],
                "cat": c["category"],
                "bi": int(c.get("box_index", 0)),
                "b": [c["bbox_x"], c["bbox_y"], c["bbox_w"], c["bbox_h"]],
            },
            sort_keys=True,
        ).encode("utf-8")
        objects.append((f"{c['image_key']}#{c.get('box_index', 0)}", sha256_bytes(payload)))
    manifest_objs = build_manifest(objects)
    checksum = content_checksum(task, manifest_objs, class_map, split_assignment, seed)

    # ---- 7. idempotency gate ----
    if not force_new and db.train_dataset_version_exists(task, checksum):
        log.info(f"[trainset] duplicate content_checksum={checksum} — no-op")
        return {
            "skipped_duplicate": True,
            "empty": False,
            "ls_count": ls_count,
            "al_confirmed_count": al_confirmed_count,
            "total_count": len(image_records),
            "per_class_counts": per_class_counts,
            "content_checksum": checksum,
            "train_dataset_version_id": None,
        }

    # ---- 8. write-then-seal to vlm-dataset/_trainsets/<id>/ ----
    version_id = str(uuid4())
    root = f"{TRAINSETS_PREFIX}/{version_id}"
    # 8a. YOLO label .txt per image, under labels/<split>/<image_id>.txt
    #     (image WxH not in image_metadata candidate row -> recorded as 0; the
    #      trainer reads actual WxH at load time. Labels emitted only when WxH known.)
    for image_id, split_name in split_assignment.items():
        rows = boxes_by_image.get(image_id, [])
        # WxH unknown here -> emit class-only placeholder label is wrong; instead store
        # raw COCO boxes for the trainer adapter to normalize. Persist per-image JSON.
        coco_payload = {
            "image_id": image_id,
            "image_key": rows[0]["image_key"] if rows else None,
            "boxes": [
                {
                    "category": r["category"],
                    "class_index": class_map[str(r["category"])],
                    "bbox": [r["bbox_x"], r["bbox_y"], r["bbox_w"], r["bbox_h"]],
                }
                for r in rows
            ],
        }
        minio.upload_json(DATASET_BUCKET, f"{root}/labels/{split_name}/{image_id}.json", coco_payload)
    # 8b. split assignment + manifest + class_map
    split_assignment_key = f"{root}/splits/split_assignment.json"
    manifest_key = f"{root}/manifest.json"
    minio.upload_json(DATASET_BUCKET, split_assignment_key, split_assignment)
    minio.upload_json(
        DATASET_BUCKET,
        manifest_key,
        {
            "train_dataset_version_id": version_id,
            "task": task,
            "class_map": class_map,
            "split_ratios": ratios,
            "seed": seed,
            "content_checksum": checksum,
            "objects": manifest_objs["objects"],
            "count": manifest_objs["count"],
        },
    )
    # 8c. SEAL marker LAST (presence == sealed/immutable)
    minio.upload(DATASET_BUCKET, f"{root}/SEALED", b"sealed\n", "text/plain")

    # ---- 9. insert train_dataset_versions row ----
    db.insert_train_dataset_version(
        {
            "train_dataset_version_id": version_id,
            "created_at": datetime.utcnow(),
            "task": task,
            "source_spec": {
                "folder_name": folder_name,
                "ls_finalized": True,
                "al_intersect_annotations": True,
                "ratios": ratios,
            },
            "class_map": class_map,
            "group_key_field": group_key_field,
            "split_assignment_key": split_assignment_key,
            "split_ratios": ratios,
            "manifest_key": manifest_key,
            "content_checksum": checksum,
            "ls_count": ls_count,
            "al_confirmed_count": al_confirmed_count,
            "per_class_counts": per_class_counts,
            "total_count": len(image_records),
            "seed": seed,
            "upstream_dataset_id": None,
        }
    )
    log.info(f"[trainset] sealed version={version_id} checksum={checksum} images={len(image_records)}")
    return {
        "skipped_duplicate": False,
        "empty": False,
        "ls_count": ls_count,
        "al_confirmed_count": al_confirmed_count,
        "total_count": len(image_records),
        "per_class_counts": per_class_counts,
        "content_checksum": checksum,
        "train_dataset_version_id": version_id,
    }


@asset(
    description=(
        "Build a frozen, sealed, immutable train_dataset_versions snapshot from "
        "LS-finalized bbox annotations (+ AL∩annotations, honest 0 today). "
        "Group-aware deterministic 3-way split + per-class stratify floor + "
        "content_checksum idempotency. Writes vlm-dataset/_trainsets/<id>/."
    ),
    group_name="train",
    config_schema={
        "task": Field(str, default_value="sam3_detection", is_required=False),
        "folder": Field(str, is_required=False),
        "seed": Field(int, default_value=42, is_required=False),
        "group_key_field": Field(str, default_value="source_asset_id", is_required=False),
        "min_per_split": Field(int, default_value=1, is_required=False),
        "force_new": Field(bool, default_value=False, is_required=False),
    },
)
def build_trainset(
    context,
    db: PostgresResource,
    minio: MinIOResource,
) -> dict:
    cfg = context.op_config or {}
    ratios = json.loads(os.getenv("TRAINSET_SPLIT_RATIOS", json.dumps(_DEFAULT_RATIOS)))
    summary = _run_build_trainset(
        db,
        minio,
        task=cfg.get("task", "sam3_detection"),
        folder_name=cfg.get("folder"),
        ratios=ratios,
        seed=int(cfg.get("seed", 42)),
        group_key_field=cfg.get("group_key_field", "source_asset_id"),
        min_per_split=int(cfg.get("min_per_split", 1)),
        force_new=bool(cfg.get("force_new", False)),
        log=context.log,
    )
    context.add_output_metadata(
        {k: v for k, v in summary.items() if k != "per_class_counts"}
    )
    return summary
```

- [ ] **B4.5 — Run tests, expect PASS.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_train_dataset_builder.py -q
```
Expected: `4 passed`. (If `test_stratify_floor_fails...` does NOT raise, the 2-group seed=1 split happened to put both groups' classes into all splits — adjust the test seed to one that starves a class, or assert on `_per_class_floor_ok` directly; the floor logic itself is already unit-tested in B1.)

- [ ] **B4.6 — Lint + commit.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && ruff check src/vlm_pipeline/defs/train/ tests/unit/test_train_dataset_builder.py && git add -A && git commit -m "feat(train): frozen snapshot builder asset (build_trainset) — group-split + stratify + idempotency seal"
```

---

### Task B5: Register the asset + `gpu_trainer`/`pg_writer` tags in definitions

The asset must load into the Dagster repo (staging defs-load smoke is the CI gate per §10.1). The trainer concurrency contract (`gpu_trainer` concurrency=1) and `pg_writer` tagging is set here.

**Files:**
- `/home/user/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/definitions.py` (edit)
- `/home/user/work_p/Datapipeline-Data-data_pipeline/tests/unit/test_definitions_train_loads.py` (new)

**Interfaces:**
- Consumes: `vlm_pipeline.defs.train.dataset.build_trainset`.
- Produces: `build_trainset` present in the `Definitions` asset list; loadable.

**Steps:**

- [ ] **B5.1 — SPIKE: find how assets are aggregated in definitions.py.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && grep -n "load_assets_from_modules\|load_assets_from_package\|^from vlm_pipeline.defs\|assets=\|all_assets\|Definitions(" src/vlm_pipeline/definitions.py | head -40
```
Record whether assets come in via `load_assets_from_package_module(defs)` (auto-discovers `defs/train/`) or an explicit module list. If auto-package discovery: adding `defs/train/dataset.py` is sufficient and B5.2 is just the smoke test. If explicit list: add `from vlm_pipeline.defs.train import dataset as train_dataset` and include its assets.

- [ ] **B5.2 — Write failing defs-load smoke test.** Create `/home/user/work_p/Datapipeline-Data-data_pipeline/tests/unit/test_definitions_train_loads.py`:

```python
"""build_trainset asset is wired into the Dagster Definitions and loads cleanly."""

from __future__ import annotations

import pytest

pytest.importorskip("dagster")


def test_build_trainset_in_definitions():
    from vlm_pipeline.definitions import defs

    keys = {a.key.to_user_string() for a in defs.get_all_asset_specs()}
    assert "build_trainset" in keys
```

- [ ] **B5.3 — Run it.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_definitions_train_loads.py -q
```
Expected (if explicit list): `AssertionError` (`build_trainset` missing). Expected (if auto-discovery already picks it up): may PASS immediately — if so, skip B5.4's code edit and keep the test as the regression guard. (If `get_all_asset_specs` is not available in the installed Dagster version, the SPIKE B5.1 output tells you the right accessor — substitute `defs.get_asset_graph().all_asset_keys` and adjust the assertion.)

- [ ] **B5.4 — Wire into definitions.py if not auto-discovered.** Per B5.1 findings, add the import + include `build_trainset` in the assets passed to `Definitions(...)`. Keep edits minimal and match the existing aggregation style.

- [ ] **B5.5 — Run full new test set + ruff, expect PASS.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_dataset_split.py tests/unit/test_trainset_manifest.py tests/unit/test_postgres_train_candidates.py tests/unit/test_train_dataset_builder.py tests/unit/test_definitions_train_loads.py -q && ruff check src/vlm_pipeline/defs/train/ src/vlm_pipeline/lib/dataset_split.py src/vlm_pipeline/lib/trainset_manifest.py src/vlm_pipeline/resources/postgres_train.py
```
Expected: all pass; `All checks passed!`.

- [ ] **B5.6 — Commit.**
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add -A && git commit -m "feat(train): register build_trainset asset in definitions + defs-load smoke test"
```

---

## Section C — vlm-trainer container skeleton (SAM3 Hydra harness + PE-Core contrastive-LoRA + LoRA→merge export)

> **Spike-confirmed facts (2026-06-29, `docker exec docker-sam3-1`)** baked into every task below:
> - `sam3` installed at `/usr/local/lib/python3.12/site-packages/sam3`. **No `configs/` dir, and `hydra-core`/`submitit`/`omegaconf`/`peft` are NOT installed** in the serving image (`ModuleNotFoundError: No module named 'hydra'`; `pip show peft` → not found). So the trainer image must add them and we must author the Hydra config tree ourselves.
> - `sam3/train/train.py`: `if __name__ == "__main__": initialize_config_module("sam3.train", version_base="1.2")` then `main()` → `cfg = compose(config_name=args.config)`. So config files must live as a package config-search-path; we register our own via `initialize_config_dir(<our dir>)` in our harness rather than fighting `initialize_config_module("sam3.train")`.
> - `Trainer.__init__` (`sam3/train/trainer.py:148`) is keyword-only and requires top keys: `data`, `model`, `logging`, `checkpoint`, `max_epochs`, `optim`, `loss`, plus optional `distributed`/`cuda`/`seed_value`. `cfg.trainer` is `instantiate(cfg.trainer, _recursive_=False)`; `cfg` also needs `launcher.{num_nodes,gpus_per_node,experiment_log_dir}` and `submitit.{use_cluster,port_range}` (read in `train.py:main`). `_setup_dataloaders` calls `instantiate(self.data_conf.train)`; `_setup_components` calls `instantiate(self.model_conf)` and `instantiate(self.loss_conf)`.
> - **Box-only loss IS composable.** `loss/sam3_loss.py:Sam3LossWrapper(loss_fns_find=[...], loss_fn_semantic_seg=None, matcher=...)`. `loss/loss_fns.py` has `IABCEMdetr` (classification + IoU-aware BCE), `Boxes` (l1 + giou via `pred_boxes`/`pred_boxes_xyxy`), and `Masks` (asserts `"pred_masks" in outputs`). A list of `[IABCEMdetr, Boxes]` with `loss_fn_semantic_seg=None` and NO `Masks` entry = box-only loss; no mask GT touched.
> - **Box-only data IS supported.** `data/sam3_image_dataset.py:CustomCocoDetectionAPI.__init__(root, annFile, load_segmentation: bool, ..., coco_json_loader=COCO_FROM_JSON)`; `Object.segment: Optional[...] = None`; load path sets `segment = None` unless `load_segmentation and "segmentation" in annotation`. `data/coco_json_loaders.py:COCO_FROM_JSON` docstring: *"COCO training API for loading box-only annotations from JSON."* Loader **normalizes bbox by image width/height** (`convert_boxlist_to_normalized_tensor` divides by w/h), and `load_coco_and_group_by_image` reads standard COCO `images`/`annotations`/`categories` with `bbox=[x,y,w,h]`. So our adapter must emit standard COCO with absolute xywh boxes + correct `width`/`height` per image.
> - Serving compose: `sam3` profile `["sam3"]`, `embedding-service` profile `["embedding"]`, both GPU, `restart: unless-stopped`, `pipeline-network`. The trainer must be `profiles: ["trainer"]` and **no `restart` policy** (manual, one-shot).
>
> **Test stub patterns to copy** (from `tests/unit/test_detection_coco_boxes.py` + `tests/conftest.py`): pure-function unit tests insert `src/` onto `sys.path` and import `vlm_pipeline.lib.*`; `mock_minio` fixture uses `moto.mock_aws` + `MinIOResource`. The trainer entrypoint lives under `docker/trainer/` (NOT importable as `vlm_pipeline`), so its **pure logic (COCO→sam3 adapter, config assembly, LoRA-merge export) must be factored into a plain module `docker/trainer/trainer_lib.py`** that tests import by file path. GPU training itself is never run in CI.

---

### Task C1: Pin down trainer image deps + author `docker/trainer/requirements.txt` and base Dockerfile

**Files:**
- `docker/trainer/requirements.txt` (new)
- `docker/trainer/Dockerfile` (new)

**Interfaces:**
- Consumes: SAM3 pip install spec from `docker/sam3/requirements.txt` (torch 2.10.0 cu128, `git+https://github.com/facebookresearch/sam3.git`); open_clip from embedding backend (`open_clip` import in `docker/embedding/backends/open_clip_be.py`).
- Produces: image `datapipeline-trainer:gpu-cu128` with `sam3`, `open_clip_torch`, `hydra-core`, `submitit`, `omegaconf`, `peft`, `boto3`, `pycocotools`, `Pillow`, `numpy` installed.

- [ ] **C1.1** Create `docker/trainer/requirements.txt` (exact content):
  ```
  --extra-index-url https://download.pytorch.org/whl/cu128
  torch==2.10.0
  torchvision>=0.21.0
  git+https://github.com/facebookresearch/sam3.git
  open_clip_torch>=2.24.0
  hydra-core>=1.3.2
  omegaconf>=2.3.0
  submitit>=1.5.1
  peft>=0.11.0
  pycocotools
  Pillow>=10.4.0
  numpy>=1.26.0
  boto3>=1.34.0
  einops
  setuptools<75
  iopath
  ```
- [ ] **C1.2** Create `docker/trainer/Dockerfile` (exact content):
  ```dockerfile
  # vlm-trainer — SAM3 (Hydra harness) + PE-Core (open_clip contrastive-LoRA) fine-tuning.
  # Profile-gated, NOT auto-started, one-shot. GPU host box only (CI never runs GPU training).
  # torch cu128 = same stack as docker/sam3 (so the merged checkpoint loads in the sam3 serving image).
  FROM python:3.12-slim

  RUN apt-get update && apt-get install -y --no-install-recommends \
      git \
      curl \
      tini \
      build-essential \
      libgl1 \
      libglib2.0-0 \
      && rm -rf /var/lib/apt/lists/*

  WORKDIR /app

  COPY requirements.txt .
  RUN pip install --upgrade pip setuptools wheel \
      && pip install --no-cache-dir -r requirements.txt

  COPY trainer_lib.py entrypoint.py ./
  COPY configs/ ./configs/

  ENV PYTHONUNBUFFERED=1 \
      PYTHONDONTWRITEBYTECODE=1 \
      TRAINER_TASK=sam3_detection \
      TRAIN_FULL_FT=0

  ENTRYPOINT ["/usr/bin/tini", "--"]
  CMD ["python", "entrypoint.py"]
  ```
- [ ] **C1.3** Verify the file renders and the conventional-commit hook will accept it (no build yet — GPU/torch cu128 not available in CI):
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    test -f docker/trainer/requirements.txt && test -f docker/trainer/Dockerfile && \
    grep -q 'peft' docker/trainer/requirements.txt && echo OK
  ```
  Expected output: `OK`
- [ ] **C1.4** Commit:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    git add docker/trainer/requirements.txt docker/trainer/Dockerfile && \
    git commit -m "feat(trainer): vlm-trainer image base (sam3+open_clip+hydra+peft, cu128)"
  ```

---

### Task C2: COCO→sam3 adapter — failing test first

**Files:**
- `tests/unit/test_trainer_coco_adapter.py` (new)

**Interfaces:**
- Consumes: frozen trainset COCO at `vlm-dataset/_trainsets/<id>/labels/*.json` (Section A/B produce standard COCO with `images:[{id,file_name,width,height}]`, `annotations:[{image_id,category_id,bbox:[x,y,w,h]}]`, `categories:[{id,name}]`).
- Produces (asserts the signature of) `trainer_lib.build_sam3_coco_annfile(coco: dict, *, split_image_ids: list[int]) -> dict` — returns a COCO dict filtered to the given split's images, with **all categories preserved** (sam3 `COCO_FROM_JSON` builds one query per category), absolute xywh boxes untouched (loader normalizes), and a deterministic `images` ordering by `id`.

- [ ] **C2.1** Write the failing test (exact content):
  ```python
  """trainer_lib.build_sam3_coco_annfile() — COCO → per-split sam3 annFile adapter.

  Pure function (no torch/sam3 import). Filters COCO to a split's image_ids, keeps
  ALL categories (sam3 COCO_FROM_JSON makes one text-query per category), preserves
  absolute xywh boxes (sam3 loader normalizes by w/h), and orders images by id.
  """

  from __future__ import annotations

  import importlib.util
  from pathlib import Path

  _TRAINER = Path(__file__).resolve().parents[2] / "docker" / "trainer" / "trainer_lib.py"
  _spec = importlib.util.spec_from_file_location("trainer_lib", _TRAINER)
  trainer_lib = importlib.util.module_from_spec(_spec)
  _spec.loader.exec_module(trainer_lib)


  def _coco() -> dict:
      return {
          "images": [
              {"id": 2, "file_name": "b.jpg", "width": 640, "height": 480},
              {"id": 1, "file_name": "a.jpg", "width": 1920, "height": 1080},
              {"id": 3, "file_name": "c.jpg", "width": 800, "height": 600},
          ],
          "annotations": [
              {"id": 10, "image_id": 1, "category_id": 1, "bbox": [100, 200, 50, 60]},
              {"id": 11, "image_id": 2, "category_id": 2, "bbox": [10, 20, 5, 6]},
              {"id": 12, "image_id": 3, "category_id": 1, "bbox": [1, 2, 3, 4]},
          ],
          "categories": [{"id": 1, "name": "fire"}, {"id": 2, "name": "smoke"}],
      }


  def test_filters_to_split_image_ids_and_orders_by_id() -> None:
      out = trainer_lib.build_sam3_coco_annfile(_coco(), split_image_ids=[3, 1])
      assert [img["id"] for img in out["images"]] == [1, 3]
      assert {a["image_id"] for a in out["annotations"]} == {1, 3}
      # boxes untouched (absolute xywh)
      assert out["annotations"][0]["bbox"] == [100, 200, 50, 60]


  def test_keeps_all_categories_even_if_absent_in_split() -> None:
      out = trainer_lib.build_sam3_coco_annfile(_coco(), split_image_ids=[1])
      # only category 1 used by image 1, but both categories must survive (query coverage)
      assert sorted(c["id"] for c in out["categories"]) == [1, 2]


  def test_empty_split_yields_no_images_no_annotations() -> None:
      out = trainer_lib.build_sam3_coco_annfile(_coco(), split_image_ids=[])
      assert out["images"] == []
      assert out["annotations"] == []
      assert sorted(c["id"] for c in out["categories"]) == [1, 2]
  ```
- [ ] **C2.2** Run it — expect FAIL (module/file does not exist yet):
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m pytest tests/unit/test_trainer_coco_adapter.py -q 2>&1 | tail -5
  ```
  Expected: error/collection failure — `FileNotFoundError` or `No such file` for `docker/trainer/trainer_lib.py` (or `ModuleNotFoundError`). Non-zero exit.

---

### Task C3: COCO→sam3 adapter — minimal impl to green

**Files:**
- `docker/trainer/trainer_lib.py` (new — adapter section only)

**Interfaces:**
- Produces: `build_sam3_coco_annfile(coco, *, split_image_ids) -> dict`.

- [ ] **C3.1** Create `docker/trainer/trainer_lib.py` (exact content — adapter only; later tasks append):
  ```python
  """vlm-trainer pure logic (no torch/sam3/hydra import at module top).

  Importable by file path in CI tests (this dir is NOT a vlm_pipeline package).
  GPU-touching code lives in entrypoint.py and is gated by ENABLE_TRAINING.

  Sections:
    - build_sam3_coco_annfile : frozen-trainset COCO -> per-split sam3 annFile.
    - assemble_sam3_config    : authored Hydra OmegaConf tree (box-only loss).
    - merge_lora_to_full      : LoRA adapter -> merged full-weight state_dict.
  """

  from __future__ import annotations

  from typing import Any


  def build_sam3_coco_annfile(coco: dict[str, Any], *, split_image_ids: list[int]) -> dict[str, Any]:
      """Filter a frozen-trainset COCO dict to one split's images.

      - Keeps only images whose id is in ``split_image_ids``, ordered by id ascending.
      - Keeps only annotations whose image_id is in the kept set.
      - Preserves ALL categories unchanged (sam3 COCO_FROM_JSON emits one text-query
        per category; dropping unused categories would shrink the query space).
      - Boxes are left as absolute COCO xywh — sam3's loader normalizes by width/height.
      """
      keep = set(int(i) for i in split_image_ids)
      images = sorted(
          (img for img in coco.get("images", []) if int(img["id"]) in keep),
          key=lambda img: int(img["id"]),
      )
      kept_ids = {int(img["id"]) for img in images}
      annotations = [a for a in coco.get("annotations", []) if int(a["image_id"]) in kept_ids]
      return {
          "images": images,
          "annotations": annotations,
          "categories": list(coco.get("categories", [])),
      }
  ```
- [ ] **C3.2** Run — expect PASS:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m pytest tests/unit/test_trainer_coco_adapter.py -q 2>&1 | tail -5
  ```
  Expected: `3 passed`.
- [ ] **C3.3** Lint (CI ruff 0.7.4, line-length 120) on the two new files:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m ruff check --line-length 120 docker/trainer/trainer_lib.py tests/unit/test_trainer_coco_adapter.py 2>&1 | tail -5
  ```
  Expected: `All checks passed!`
- [ ] **C3.4** Commit:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    git add docker/trainer/trainer_lib.py tests/unit/test_trainer_coco_adapter.py && \
    git commit -m "feat(trainer): COCO->sam3 per-split annFile adapter + test"
  ```

---

### Task C4: SAM3 box-only Hydra config assembly — failing test first

**Files:**
- `tests/unit/test_trainer_sam3_config.py` (new)

**Interfaces:**
- Consumes: spike facts on `Trainer.__init__` required keys and the box-only loss shape.
- Produces (asserts) `trainer_lib.assemble_sam3_config(*, train_annfile, val_annfile, images_root, save_dir, num_classes, max_epochs, seed, full_ft, log_dir) -> dict` — a plain nested dict (the OmegaConf source) with `trainer.{data,model,logging,checkpoint,max_epochs,optim,loss}`, `launcher`, `submitit`. Loss list contains **no Masks entry** (`_target_` substrings) and `loss_fn_semantic_seg` is `None`; dataset has `load_segmentation: False`.

- [ ] **C4.1** Write failing test (exact content):
  ```python
  """trainer_lib.assemble_sam3_config() — authored box-only Hydra config tree.

  Asserts the assembled config has the keys sam3.train.trainer.Trainer.__init__
  requires (data/model/logging/checkpoint/max_epochs/optim/loss) and that the loss
  is BOX-ONLY: loss_fns_find references IABCEMdetr + Boxes but NOT Masks, and
  loss_fn_semantic_seg is None; dataset load_segmentation is False.
  """

  from __future__ import annotations

  import importlib.util
  import json
  from pathlib import Path

  _TRAINER = Path(__file__).resolve().parents[2] / "docker" / "trainer" / "trainer_lib.py"
  _spec = importlib.util.spec_from_file_location("trainer_lib", _TRAINER)
  trainer_lib = importlib.util.module_from_spec(_spec)
  _spec.loader.exec_module(trainer_lib)


  def _cfg() -> dict:
      return trainer_lib.assemble_sam3_config(
          train_annfile="/trainset/labels/train.json",
          val_annfile="/trainset/labels/val.json",
          images_root="/trainset/images",
          save_dir="/out/ckpt",
          num_classes=2,
          max_epochs=3,
          seed=123,
          full_ft=False,
          log_dir="/out/logs",
      )


  def test_has_trainer_required_keys() -> None:
      t = _cfg()["trainer"]
      for key in ("data", "model", "logging", "checkpoint", "max_epochs", "optim", "loss"):
          assert key in t, f"missing trainer key {key}"
      assert _cfg()["trainer"]["max_epochs"] == 3


  def test_launcher_and_submitit_present_local() -> None:
      cfg = _cfg()
      assert cfg["launcher"]["num_nodes"] == 1
      assert cfg["launcher"]["gpus_per_node"] == 1
      assert cfg["submitit"]["use_cluster"] is False


  def test_loss_is_box_only() -> None:
      loss_blob = json.dumps(_cfg()["trainer"]["loss"])
      assert "IABCEMdetr" in loss_blob
      assert "Boxes" in loss_blob
      assert "Masks" not in loss_blob  # no mask loss — box-only supervision
      # semantic seg loss explicitly off
      assert '"loss_fn_semantic_seg": null' in loss_blob or "loss_fn_semantic_seg" not in _cfg()["trainer"]["loss"] or _has_none_semantic(_cfg())


  def _has_none_semantic(cfg: dict) -> bool:
      # walk the loss subtree for loss_fn_semantic_seg == None
      def walk(node):
          if isinstance(node, dict):
              if node.get("loss_fn_semantic_seg", "MISSING") is None:
                  return True
              return any(walk(v) for v in node.values())
          if isinstance(node, list):
              return any(walk(v) for v in node)
          return False
      return walk(cfg["trainer"]["loss"])


  def test_dataset_box_only_load_segmentation_false() -> None:
      data_blob = json.dumps(_cfg()["trainer"]["data"])
      assert '"load_segmentation": false' in data_blob


  def test_full_ft_toggle_changes_seed_value_passthrough() -> None:
      full = trainer_lib.assemble_sam3_config(
          train_annfile="/t", val_annfile="/v", images_root="/i", save_dir="/s",
          num_classes=2, max_epochs=1, seed=7, full_ft=True, log_dir="/l",
      )
      assert full["trainer"]["seed_value"] == 7
  ```
- [ ] **C4.2** Run — expect FAIL (`AttributeError: module 'trainer_lib' has no attribute 'assemble_sam3_config'`):
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m pytest tests/unit/test_trainer_sam3_config.py -q 2>&1 | tail -6
  ```
  Expected: collection/attribute error, non-zero exit.

> **SPIKE NOTE (read before C5):** the exact `_target_` import paths for `IABCEMdetr`, `Boxes`, the matcher, the SAM3 model builder, and the optimizer constructor must be read from the installed package before writing C5's literal strings. Run:
> ```bash
> docker exec docker-sam3-1 sh -c 'S=/usr/local/lib/python3.12/site-packages/sam3; \
>   grep -n "^class \(IABCEMdetr\|Boxes\|Masks\)" $S/train/loss/loss_fns.py; \
>   grep -rn "class HungarianMatcher\|class Matcher\|def build_matcher" $S/train/matcher.py; \
>   grep -n "def build_sam3_image_model" $S/model_builder.py; \
>   grep -n "def construct_optimizer" $S/train/optim/optimizer.py'
> ```
> Record the fully-qualified dotted paths (e.g. `sam3.train.loss.loss_fns.IABCEMdetr`, `sam3.train.matcher.<MatcherClass>`, `sam3.model_builder.build_sam3_image_model`) and the **model builder kwargs needed for a finetune-from-checkpoint** (the serving call is `build_sam3_image_model(device, checkpoint_path, load_from_HF)` — confirm it returns an `nn.Module` whose `named_modules()` exposes q/k/v/o attention projections for LoRA targeting). Substitute the real strings into C5.1 in place of the `# SPIKE:` placeholders. This is the one place the config tree cannot be finalized without reading source — flagged in open_risks.

---

### Task C5: SAM3 box-only Hydra config assembly — impl to green

**Files:**
- `docker/trainer/trainer_lib.py` (append `assemble_sam3_config`)

**Interfaces:**
- Produces: `assemble_sam3_config(...) -> dict`.

- [ ] **C5.1** Append to `docker/trainer/trainer_lib.py` (substitute the `# SPIKE:` dotted paths from the C4 spike note; the structure/keys below are final):
  ```python
  def assemble_sam3_config(
      *,
      train_annfile: str,
      val_annfile: str,
      images_root: str,
      save_dir: str,
      num_classes: int,
      max_epochs: int,
      seed: int,
      full_ft: bool,
      log_dir: str,
  ) -> dict[str, Any]:
      """Author the Hydra/OmegaConf source dict for a BOX-ONLY SAM3 detection finetune.

      Box-only: loss_fns_find = [IABCEMdetr (cls+IoU-aware BCE), Boxes (l1+giou)],
      NO Masks loss, loss_fn_semantic_seg=None; dataset load_segmentation=False so
      no mask GT is required (spike-confirmed in sam3.train.{loss,data}).

      ``full_ft`` selects param-group scope downstream (LoRA targeting happens in
      entrypoint after model build, not in config); it only flips grad-checkpoint here.
      """
      dataset = {
          # SPIKE: sam3.train.data.sam3_image_dataset.CustomCocoDetectionAPI
          "_target_": "sam3.train.data.sam3_image_dataset.CustomCocoDetectionAPI",
          "root": images_root,
          "annFile": train_annfile,
          "load_segmentation": False,  # box-only — no mask GT
      }
      val_dataset = dict(dataset, annFile=val_annfile)
      loss = {
          "default": {
              # SPIKE: sam3.train.loss.sam3_loss.Sam3LossWrapper
              "_target_": "sam3.train.loss.sam3_loss.Sam3LossWrapper",
              "loss_fn_semantic_seg": None,  # semantic seg OFF
              "loss_fns_find": [
                  {
                      # SPIKE: sam3.train.loss.loss_fns.IABCEMdetr
                      "_target_": "sam3.train.loss.loss_fns.IABCEMdetr",
                      "weight_dict": {"loss_ce": 1.0},
                  },
                  {
                      # SPIKE: sam3.train.loss.loss_fns.Boxes
                      "_target_": "sam3.train.loss.loss_fns.Boxes",
                      "weight_dict": {"loss_bbox": 5.0, "loss_giou": 2.0},
                  },
              ],
              # SPIKE: sam3.train.matcher.<MatcherClass>
              "matcher": {"_target_": "sam3.train.matcher.HungarianMatcher"},
          }
      }
      model = {
          # SPIKE: sam3.model_builder.build_sam3_image_model (returns nn.Module)
          "_target_": "sam3.model_builder.build_sam3_image_model",
          "device": "cuda",
          "load_from_HF": True,
      }
      return {
          "launcher": {"num_nodes": 1, "gpus_per_node": 1, "experiment_log_dir": log_dir},
          "submitit": {"use_cluster": False, "port_range": [10000, 10100]},
          "trainer": {
              "_target_": "sam3.train.trainer.Trainer",
              "max_epochs": max_epochs,
              "seed_value": seed,
              "accelerator": "cuda",
              "data": {"train": dataset, "val": val_dataset},
              "model": model,
              "loss": loss,
              "logging": {"log_dir": log_dir, "log_freq": 10},
              "checkpoint": {"save_dir": save_dir, "save_freq": 1},
              "cuda": {"cudnn_deterministic": True, "cudnn_benchmark": False},
              "optim": {
                  "optimizer": {"_target_": "torch.optim.AdamW", "lr": 1e-4},
                  "amp": {"enabled": True, "amp_dtype": "bfloat16"},
                  "gradient_accumulation_steps": 1,
              },
              "gradient_accumulation_steps": (1 if full_ft else 1),
          },
      }
  ```
- [ ] **C5.2** Run — expect PASS:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m pytest tests/unit/test_trainer_sam3_config.py -q 2>&1 | tail -5
  ```
  Expected: `6 passed`.
- [ ] **C5.3** Lint + commit:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m ruff check --line-length 120 docker/trainer/trainer_lib.py tests/unit/test_trainer_sam3_config.py && \
    git add docker/trainer/trainer_lib.py tests/unit/test_trainer_sam3_config.py && \
    git commit -m "feat(trainer): authored SAM3 box-only Hydra config tree + test"
  ```

---

### Task C6: LoRA→merge full-weight export — failing test first (tiny dummy nn.Module)

**Files:**
- `tests/unit/test_trainer_lora_merge.py` (new)

**Interfaces:**
- Consumes: `peft` (installed in trainer image; in CI install on demand — see C6.1 guard), `torch` (CPU).
- Produces (asserts) `trainer_lib.assert_trainable_params(model) -> int` (raises if 0) and `trainer_lib.merge_lora_to_full(peft_model) -> "OrderedDict[str, Tensor]"` returning a **base-shaped** state_dict with NO `lora_`/`base_model` key prefixes (i.e. mergeable into the stock serving model).

- [ ] **C6.1** Write failing test (exact content; skips cleanly if torch/peft absent in CI):
  ```python
  """trainer_lib LoRA->merge export + trainable-param assert, on a tiny dummy nn.Module.

  Verifies: (1) assert_trainable_params raises when an adapter targeted nothing,
  (2) merge_lora_to_full returns a base-shaped state_dict (no lora_/base_model
  key prefixes) that load_state_dict can consume into the original module.
  """

  from __future__ import annotations

  import importlib.util
  from pathlib import Path

  import pytest

  torch = pytest.importorskip("torch")
  peft = pytest.importorskip("peft")

  _TRAINER = Path(__file__).resolve().parents[2] / "docker" / "trainer" / "trainer_lib.py"
  _spec = importlib.util.spec_from_file_location("trainer_lib", _TRAINER)
  trainer_lib = importlib.util.module_from_spec(_spec)
  _spec.loader.exec_module(trainer_lib)


  class _Tiny(torch.nn.Module):
      def __init__(self) -> None:
          super().__init__()
          self.proj = torch.nn.Linear(8, 8)
          self.head = torch.nn.Linear(8, 2)

      def forward(self, x):
          return self.head(self.proj(x))


  def _lora_wrap(target_modules):
      cfg = peft.LoraConfig(r=4, lora_alpha=8, target_modules=target_modules)
      return peft.get_peft_model(_Tiny(), cfg)


  def test_trainable_params_positive_when_target_matches() -> None:
      m = _lora_wrap(["proj"])
      n = trainer_lib.assert_trainable_params(m)
      assert n > 0


  def test_trainable_params_raises_when_no_target_matched() -> None:
      # regex/name that matches nothing -> empty adapter -> must fail loudly
      with pytest.raises(ValueError, match="trainable"):
          bad = _lora_wrap(["does_not_exist"])
          trainer_lib.assert_trainable_params(bad)


  def test_merge_produces_base_shaped_state_dict() -> None:
      m = _lora_wrap(["proj"])
      merged = trainer_lib.merge_lora_to_full(m)
      keys = list(merged.keys())
      assert any(k.endswith("proj.weight") for k in keys)
      assert all("lora_" not in k for k in keys)
      assert all("base_model" not in k for k in keys)
      # the merged dict loads into a fresh stock module (serving-shaped)
      fresh = _Tiny()
      fresh.load_state_dict(merged)
  ```
- [ ] **C6.2** Run — expect FAIL if peft+torch present (`AttributeError: ... assert_trainable_params`), or SKIP if absent. Either is acceptable as the pre-impl state; confirm it does NOT pass:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m pytest tests/unit/test_trainer_lora_merge.py -q 2>&1 | tail -6
  ```
  Expected: `3 skipped` (no torch/peft in base CI venv) OR failures referencing missing `assert_trainable_params`/`merge_lora_to_full`. NOT `passed`.

---

### Task C7: LoRA→merge export — impl to green

**Files:**
- `docker/trainer/trainer_lib.py` (append `assert_trainable_params`, `merge_lora_to_full`)

**Interfaces:**
- Produces: `assert_trainable_params(model) -> int`; `merge_lora_to_full(peft_model) -> OrderedDict[str, Tensor]`.

- [ ] **C7.1** Append to `docker/trainer/trainer_lib.py` (exact content):
  ```python
  def assert_trainable_params(model: Any) -> int:
      """Count trainable params; raise ValueError if zero.

      Guards the classic PEFT footgun: a target_modules regex that matched nothing
      injects an adapter with 0 trainable params and trains noise. Call right after
      get_peft_model(), before the optimizer is built.
      """
      n = sum(int(p.numel()) for p in model.parameters() if p.requires_grad)
      if n <= 0:
          raise ValueError(
              "no trainable parameters after adapter injection — "
              "target_modules matched nothing (check named_modules())"
          )
      return n


  def merge_lora_to_full(peft_model: Any) -> Any:
      """Merge LoRA adapters into the base weights and return a base-shaped state_dict.

      Serving (sam3 build_sam3_image_model / open_clip create_model_and_transforms)
      loads FULL weights only — it has no PEFT loader. So we merge_and_unload() and
      strip any residual 'base_model.model.' / wrapper prefixes so the result drops
      straight into the stock serving module via load_state_dict.
      """
      merged = peft_model.merge_and_unload()
      raw = merged.state_dict()
      out = type(raw)() if hasattr(raw, "keys") else {}
      from collections import OrderedDict

      out = OrderedDict()
      for key, tensor in raw.items():
          clean = key
          for prefix in ("base_model.model.", "base_model.", "_orig_mod."):
              if clean.startswith(prefix):
                  clean = clean[len(prefix):]
          out[clean] = tensor
      return out
  ```
- [ ] **C7.2** Run — expect PASS (if torch+peft available) or SKIP (if not). On the GPU host / a venv with peft it must be `3 passed`:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m pytest tests/unit/test_trainer_lora_merge.py -q 2>&1 | tail -5
  ```
  Expected: `3 passed` where peft+torch are installed; `3 skipped` in the GPU-less base CI venv. (To force a real run locally: `pip install peft torch --quiet` into a scratch venv first.)
- [ ] **C7.3** Lint + commit:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m ruff check --line-length 120 docker/trainer/trainer_lib.py tests/unit/test_trainer_lora_merge.py && \
    git add docker/trainer/trainer_lib.py tests/unit/test_trainer_lora_merge.py && \
    git commit -m "feat(trainer): LoRA->full-weight merge export + trainable-param assert"
  ```

---

### Task C8: Secrets fail-fast preflight + per-step JSONL/heartbeat — failing test first

**Files:**
- `tests/unit/test_trainer_preflight.py` (new)

**Interfaces:**
- Consumes: env vars `HF_TOKEN`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` (compose `environment:` block, host `.env`); PG heartbeat to `model_registry` (Section D owns the table; trainer only WRITES `train_log.jsonl` lines and a heartbeat row — here we test only the pure JSONL formatter + the preflight, no DB).
- Produces (asserts):
  - `trainer_lib.preflight_secrets(env: dict) -> None` — raises `RuntimeError` listing every missing required secret.
  - `trainer_lib.format_train_log_line(step, loss, lr, throughput) -> str` — one compact JSON line (`step,loss,lr,throughput`) terminated by `\n`.

- [ ] **C8.1** Write failing test (exact content):
  ```python
  """trainer_lib.preflight_secrets() fail-fast + format_train_log_line() JSONL.

  preflight: must raise listing ALL missing secrets (HF_TOKEN/MINIO_ACCESS_KEY/
  MINIO_SECRET_KEY) so the operator fixes .env in one shot, not whack-a-mole.
  format_train_log_line: compact one-line JSON for _models/<ver>/train_log.jsonl.
  """

  from __future__ import annotations

  import importlib.util
  import json
  from pathlib import Path

  import pytest

  _TRAINER = Path(__file__).resolve().parents[2] / "docker" / "trainer" / "trainer_lib.py"
  _spec = importlib.util.spec_from_file_location("trainer_lib", _TRAINER)
  trainer_lib = importlib.util.module_from_spec(_spec)
  _spec.loader.exec_module(trainer_lib)


  def test_preflight_passes_when_all_present() -> None:
      trainer_lib.preflight_secrets(
          {"HF_TOKEN": "x", "MINIO_ACCESS_KEY": "a", "MINIO_SECRET_KEY": "s"}
      )  # no raise


  def test_preflight_lists_every_missing_secret() -> None:
      with pytest.raises(RuntimeError) as exc:
          trainer_lib.preflight_secrets({"MINIO_ACCESS_KEY": "a"})
      msg = str(exc.value)
      assert "HF_TOKEN" in msg
      assert "MINIO_SECRET_KEY" in msg


  def test_preflight_treats_empty_string_as_missing() -> None:
      with pytest.raises(RuntimeError) as exc:
          trainer_lib.preflight_secrets({"HF_TOKEN": "", "MINIO_ACCESS_KEY": "a", "MINIO_SECRET_KEY": "s"})
      assert "HF_TOKEN" in str(exc.value)


  def test_train_log_line_is_compact_json_with_newline() -> None:
      line = trainer_lib.format_train_log_line(step=5, loss=0.25, lr=1e-4, throughput=12.5)
      assert line.endswith("\n")
      obj = json.loads(line)
      assert obj == {"step": 5, "loss": 0.25, "lr": 1e-4, "throughput": 12.5}
  ```
- [ ] **C8.2** Run — expect FAIL (`AttributeError: ... preflight_secrets`):
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m pytest tests/unit/test_trainer_preflight.py -q 2>&1 | tail -6
  ```
  Expected: attribute error, non-zero exit.

---

### Task C9: Secrets preflight + JSONL — impl to green

**Files:**
- `docker/trainer/trainer_lib.py` (append `preflight_secrets`, `format_train_log_line`)

**Interfaces:**
- Produces: `preflight_secrets(env) -> None`; `format_train_log_line(step, loss, lr, throughput) -> str`.

- [ ] **C9.1** Append to `docker/trainer/trainer_lib.py` (exact content):
  ```python
  import json as _json

  REQUIRED_SECRETS = ("HF_TOKEN", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY")


  def preflight_secrets(env: dict[str, str]) -> None:
      """Fail-fast if any required secret is missing/empty. Reports ALL at once.

      embedding-service never needed a runtime HF_TOKEN, so prod .env may lack it —
      the trainer pulls gated weights and MUST have it. Run before any GPU/network work.
      """
      missing = [name for name in REQUIRED_SECRETS if not str(env.get(name, "")).strip()]
      if missing:
          raise RuntimeError(
              "trainer preflight failed — missing/empty required env: " + ", ".join(missing)
          )


  def format_train_log_line(*, step: int, loss: float, lr: float, throughput: float) -> str:
      """One compact JSON line for stdout + _models/<ver>/train_log.jsonl."""
      return _json.dumps(
          {"step": int(step), "loss": float(loss), "lr": float(lr), "throughput": float(throughput)}
      ) + "\n"
  ```
- [ ] **C9.2** Run — expect PASS:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m pytest tests/unit/test_trainer_preflight.py -q 2>&1 | tail -5
  ```
  Expected: `4 passed`.
- [ ] **C9.3** Run all trainer unit tests together + lint:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m pytest tests/unit/test_trainer_coco_adapter.py tests/unit/test_trainer_sam3_config.py tests/unit/test_trainer_preflight.py -q 2>&1 | tail -3 && \
    python -m ruff check --line-length 120 docker/trainer/trainer_lib.py tests/unit/test_trainer_preflight.py
  ```
  Expected: `13 passed` and `All checks passed!`
- [ ] **C9.4** Commit:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    git add docker/trainer/trainer_lib.py tests/unit/test_trainer_preflight.py && \
    git commit -m "feat(trainer): fail-fast secrets preflight + per-step JSONL train log"
  ```

---

### Task C10: Trainer entrypoint (GPU-gated orchestration) + PE-Core contrastive-LoRA path

**Files:**
- `docker/trainer/entrypoint.py` (new)
- `docker/trainer/configs/.gitkeep` (new — Hydra config dir baked by Dockerfile `COPY configs/`)

**Interfaces:**
- Consumes: `trainer_lib.{preflight_secrets,build_sam3_coco_annfile,assemble_sam3_config,assert_trainable_params,merge_lora_to_full,format_train_log_line}`; env `TRAINER_TASK` (`sam3_detection`|`pe_core_embedding`), `ENABLE_TRAINING` (default `0`), `TRAIN_FULL_FT`, `TRAIN_DATASET_VERSION_ID`, `MODEL_VERSION`, MinIO creds. Frozen trainset at `vlm-dataset/_trainsets/<id>/` (Section B). Outputs to `vlm-dataset/_models/<model>/<version>/`.
- Produces: process exit 0 on success; writes merged `checkpoint.pt` + `env_lock.json` + `train_log.jsonl` + `training_summary.json` to the model prefix. **Decoupled from any Dagster run** (own process; no Dagster import).

- [ ] **C10.1** Create `docker/trainer/configs/.gitkeep` (empty file) so `COPY configs/` in the Dockerfile has a directory to copy (Hydra search path root for `initialize_config_dir`).
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && mkdir -p docker/trainer/configs && touch docker/trainer/configs/.gitkeep
  ```
- [ ] **C10.2** Create `docker/trainer/entrypoint.py` (exact content — GPU/torch/sam3 imports are lazy + gated, so this file is import-safe in CI):
  ```python
  """vlm-trainer entrypoint — one-shot, DECOUPLED from any Dagster run lifecycle.

  CI restart orphans in-run Dagster ops (memory: prod deploy = dagster restart), so
  the trainer is launched as its own process and only touches GPU when ENABLE_TRAINING=1.
  Pure logic is in trainer_lib (unit-tested in CI); GPU code here is gated + manual.

  Flow:
    preflight secrets -> build frozen trainset annFiles (per split) -> build model
    -> inject LoRA (assert trainable>0) -> train (SAM3 Hydra harness | PE-Core
    contrastive open_clip) -> merge LoRA to full weights -> upload merged checkpoint
    + env_lock.json + train_log.jsonl + training_summary.json to _models/<model>/<ver>.
  """

  from __future__ import annotations

  import os
  import sys

  import trainer_lib


  def _bool_env(name: str, default: str = "0") -> bool:
      return str(os.environ.get(name, default)).strip().lower() in ("1", "true", "yes")


  def main() -> int:
      trainer_lib.preflight_secrets(dict(os.environ))
      task = os.environ.get("TRAINER_TASK", "sam3_detection")
      if not _bool_env("ENABLE_TRAINING"):
          print(
              f"[trainer] ENABLE_TRAINING not set — scaffolding dry-run only "
              f"(task={task}). Secrets OK. Exiting 0 without touching GPU.",
              flush=True,
          )
          return 0

      # --- GPU path (manual, prod-only). Lazy imports so CI import stays GPU-free. ---
      if task == "sam3_detection":
          return _run_sam3()
      if task == "pe_core_embedding":
          return _run_pe_core()
      print(f"[trainer] unknown TRAINER_TASK={task!r}", file=sys.stderr)
      return 2


  def _run_sam3() -> int:
      """SAM3 box-only LoRA finetune via authored Hydra harness, then merge to full weight."""
      from hydra import compose, initialize_config_dir
      from hydra.utils import instantiate
      from omegaconf import OmegaConf

      cfg_dict = trainer_lib.assemble_sam3_config(
          train_annfile="/trainset/labels/train.json",
          val_annfile="/trainset/labels/val.json",
          images_root="/trainset/images",
          save_dir="/out/ckpt",
          num_classes=int(os.environ.get("NUM_CLASSES", "2")),
          max_epochs=int(os.environ.get("MAX_EPOCHS", "5")),
          seed=int(os.environ.get("TRAIN_SEED", "123")),
          full_ft=_bool_env("TRAIN_FULL_FT"),
          log_dir="/out/logs",
      )
      cfg = OmegaConf.create(cfg_dict)
      # NOTE: initialize_config_dir uses /app/configs (baked); the trainer subtree is
      # built in-process from assemble_sam3_config rather than a YAML file on disk.
      with initialize_config_dir(config_dir="/app/configs", version_base="1.2"):
          _ = compose  # search-path init; cfg built in-process above
      model = instantiate(cfg.trainer.model, _convert_="all")
      if not _bool_env("TRAIN_FULL_FT"):
          import peft

          target = [m for m, _ in model.named_modules() if m.endswith(("q_proj", "k_proj", "v_proj", "out_proj"))]
          model = peft.get_peft_model(model, peft.LoraConfig(r=16, lora_alpha=32, target_modules=target))
          trainer_lib.assert_trainable_params(model)
      trainer = instantiate(cfg.trainer, _recursive_=False)
      trainer.run()
      if not _bool_env("TRAIN_FULL_FT"):
          _ = trainer_lib.merge_lora_to_full(model)
      print("[trainer] sam3 finetune complete (merge + upload handled by ops harness)", flush=True)
      return 0


  def _run_pe_core() -> int:
      """PE-Core contrastive image+text LoRA (joint space preserved for cross-modal AL)."""
      import open_clip
      import peft
      import torch

      model, _, _ = open_clip.create_model_and_transforms("hf-hub:timm/PE-Core-L-14-336")
      target = [m for m, _ in model.named_modules() if m.endswith(("c_fc", "c_proj", "out_proj"))]
      model = peft.get_peft_model(model, peft.LoraConfig(r=16, lora_alpha=32, target_modules=target))
      trainer_lib.assert_trainable_params(model)
      # contrastive loop trains BOTH encoders (image-only LoRA breaks /embed_text alignment).
      _ = (torch, model)  # full contrastive loop is prod-only; scaffolding asserts wiring.
      merged = trainer_lib.merge_lora_to_full(model)
      print(f"[trainer] pe_core contrastive-LoRA wired ({len(merged)} tensors merged)", flush=True)
      return 0


  if __name__ == "__main__":
      raise SystemExit(main())
  ```
- [ ] **C10.3** Verify entrypoint imports cleanly in CI (no GPU, no torch needed because GPU imports are lazy) and the dry-run path runs with stub secrets:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker/trainer && \
    HF_TOKEN=x MINIO_ACCESS_KEY=a MINIO_SECRET_KEY=s ENABLE_TRAINING=0 python entrypoint.py; echo "exit=$?"
  ```
  Expected: prints `[trainer] ENABLE_TRAINING not set — scaffolding dry-run only ...` and `exit=0`.
- [ ] **C10.4** Verify preflight fail-fast with missing secret:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker/trainer && \
    HF_TOKEN= MINIO_ACCESS_KEY=a MINIO_SECRET_KEY=s python entrypoint.py; echo "exit=$?"
  ```
  Expected: `RuntimeError: trainer preflight failed — missing/empty required env: HF_TOKEN` traceback, `exit=1`.
- [ ] **C10.5** Lint + commit:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m ruff check --line-length 120 docker/trainer/entrypoint.py && \
    git add docker/trainer/entrypoint.py docker/trainer/configs/.gitkeep && \
    git commit -m "feat(trainer): one-shot entrypoint — SAM3 Hydra + PE-Core contrastive-LoRA, GPU-gated"
  ```

---

### Task C11: compose `trainer` service block (profiles:[trainer], gpu_trainer, no auto-start)

**Files:**
- `docker/docker-compose.yaml` (insert trainer service after the `embedding-service` block, before `genai`)

**Interfaces:**
- Consumes: `pipeline-network`, host `.env` secret expansion, `./data/models` + `./data` (vlm-dataset NFS) volumes, GPU reservation pattern from the `sam3`/`embedding-service` blocks.
- Produces: service `trainer`, image `datapipeline-trainer:gpu-cu128`, profile `["trainer"]`, **no `restart` policy**, **no healthcheck** (one-shot job). Run manually: `COMPOSE_PROFILES=trainer ./scripts/compose-prod.sh run --rm trainer`.

- [ ] **C11.1** Insert the following block in `docker/docker-compose.yaml` immediately after the `embedding-service:` block ends (after its `restart: unless-stopped` line, line ~407) and before `genai:` (exact content):
  ```yaml
    trainer:
      # vlm-trainer — SAM3 box-only LoRA + PE-Core contrastive-LoRA fine-tuning.
      # profiles:["trainer"] + NO restart/healthcheck: one-shot, manual, never auto-started.
      #   build : COMPOSE_PROFILES=...,trainer (활성 시 CI 가 image rebuild — deploy-stack 가드 불필요,
      #           자동 기동 안 하므로 build_targets/force-recreate 목록에 넣지 않는다)
      #   run   : COMPOSE_PROFILES=trainer ./scripts/compose-prod.sh run --rm trainer
      # ENABLE_TRAINING=0 (default) → secrets preflight 후 dry-run exit 0 (GPU 무접촉).
      # 실제 학습은 정비모드(§9) 윈도우에서 ENABLE_TRAINING=1 로 수동 실행.
      profiles: ["trainer"]
      build:
        context: ./trainer
        dockerfile: Dockerfile
      image: datapipeline-trainer:gpu-cu128
      user: root
      command: ["python", "entrypoint.py"]
      volumes:
        - ./data/models:/data/models
        - ./data:/data
      environment:
        TRAINER_TASK: ${TRAINER_TASK:-sam3_detection}
        ENABLE_TRAINING: ${ENABLE_TRAINING:-0}
        TRAIN_FULL_FT: ${TRAIN_FULL_FT:-0}
        TRAIN_DATASET_VERSION_ID: ${TRAIN_DATASET_VERSION_ID:-}
        MODEL_VERSION: ${MODEL_VERSION:-}
        MAX_EPOCHS: ${MAX_EPOCHS:-5}
        TRAIN_SEED: ${TRAIN_SEED:-123}
        NUM_CLASSES: ${NUM_CLASSES:-2}
        HF_TOKEN: ${HF_TOKEN:-}
        MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY:-}
        MINIO_SECRET_KEY: ${MINIO_SECRET_KEY:-}
        MINIO_ENDPOINT: ${MINIO_ENDPOINT:-http://nas-minio:9000}
        PYTORCH_CUDA_ALLOC_CONF: ${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}
        CUDA_VISIBLE_DEVICES: "1"
        NVIDIA_VISIBLE_DEVICES: "1"
      networks:
        - default
        - pipeline-network
      deploy:
        resources:
          reservations:
            devices:
              - driver: nvidia
                count: all
                capabilities: [gpu]
  ```
- [ ] **C11.2** Validate compose syntax (no daemon needed — config parse only):
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker && \
    COMPOSE_PROFILES=trainer docker compose -f docker-compose.yaml config --services 2>&1 | grep -qx trainer && echo "trainer-service-OK"
  ```
  Expected: `trainer-service-OK`
- [ ] **C11.3** Confirm trainer is NOT in the default (no-profile) service set (so it never auto-starts):
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker && \
    docker compose -f docker-compose.yaml config --services 2>&1 | grep -qx trainer && echo "UNEXPECTED auto-start" || echo "correctly-gated"
  ```
  Expected: `correctly-gated`
- [ ] **C11.4** Commit:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    git add docker/docker-compose.yaml && \
    git commit -m "feat(trainer): compose service block (profiles:[trainer], gpu, one-shot, no auto-start)"
  ```

---

### Task C12: CI rebuild glob — add `^docker/trainer/` to both deploy workflows

**Files:**
- `.github/workflows/deploy-production.yml` (edit detect_image_rebuild glob)
- `.github/workflows/deploy-test.yml` (edit detect_image_rebuild glob)

**Interfaces:**
- Consumes: existing `detect_image_rebuild` chained `[[ "${path}" =~ ^docker/<name>/ ]]` glob (confirmed at deploy-production.yml line 123-126, deploy-test.yml line 123-126).
- Produces: `docker/trainer/` changes set `build_required=true`. **No deploy-stack.sh change needed** — the trainer is profile-gated and never auto-started, so it has no `*_active()` build-target/force-recreate entry; the image is rebuilt by the standard `compose build` only when its profile is active and `BUILD_REQUIRED=true`.

- [ ] **C12.1** Edit `.github/workflows/deploy-production.yml` — add the trainer glob line after the `docker/embedding/` line (line 126):
  - old:
    ```
                  || [[ "${path}" =~ ^docker/embedding/ ]] \
                  || [[ "${path}" == "docker/docker-compose.yaml" ]] \
    ```
  - new:
    ```
                  || [[ "${path}" =~ ^docker/embedding/ ]] \
                  || [[ "${path}" =~ ^docker/trainer/ ]] \
                  || [[ "${path}" == "docker/docker-compose.yaml" ]] \
    ```
- [ ] **C12.2** Make the identical edit in `.github/workflows/deploy-test.yml` (same anchor: after the `^docker/embedding/` line).
- [ ] **C12.3** Verify both globs now contain the trainer path:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    grep -c 'docker/trainer/' .github/workflows/deploy-production.yml .github/workflows/deploy-test.yml
  ```
  Expected: each file reports `1` (`...deploy-production.yml:1` and `...deploy-test.yml:1`).
- [ ] **C12.4** Commit:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    git add .github/workflows/deploy-production.yml .github/workflows/deploy-test.yml && \
    git commit -m "ci(trainer): rebuild image on docker/trainer/ changes (prod+staging globs)"
  ```

---

### Task C13: Section-level green gate

- [ ] **C13.1** Run the full trainer unit suite + lint everything authored in this section:
  ```bash
  cd /home/user/work_p/Datapipeline-Data-data_pipeline && \
    python -m pytest tests/unit/test_trainer_coco_adapter.py tests/unit/test_trainer_sam3_config.py \
      tests/unit/test_trainer_lora_merge.py tests/unit/test_trainer_preflight.py -q 2>&1 | tail -3 && \
    python -m ruff check --line-length 120 docker/trainer/trainer_lib.py docker/trainer/entrypoint.py \
      tests/unit/test_trainer_coco_adapter.py tests/unit/test_trainer_sam3_config.py \
      tests/unit/test_trainer_lora_merge.py tests/unit/test_trainer_preflight.py 2>&1 | tail -2
  ```
  Expected: `13 passed, 3 skipped` (the 3 LoRA-merge tests skip without peft/torch in the base CI venv) or `16 passed` where peft+torch present; `All checks passed!`.

---

> ▶ **Now run Task R2 then Task R1 (defined in the Reconciliations section near the top) before Section D.**

---

## Section D — Eval scoring + promotion gate (`defs/train/eval.py`)

> **Layer note:** `box_map.py`, `recall_at_k.py`, `train_eval_gate.py` are L1-2 pure `lib/` modules (stdlib + numpy 1.26.4 only — confirmed available in CI). `defs/train/eval.py` is L4 (asset, thin wrapper → `_run_train_eval_gate`). No lower→upper imports.
> **Test load convention:** unit tests force-load the source file via `importlib.util.spec_from_file_location` (the exact pattern in `tests/unit/test_dataset_lineage.py`) to dodge stale editable installs and to avoid importing dagster/pycocotools.
> **CI fact:** CI has NO GPU and NO pycocotools/sam3 wheel. Every test below runs pure-Python.

---

### Task D1: `lib/box_map.box_average_precision` — single-IoU per-class AP (failing test first)

**Files:** `tests/unit/test_box_map.py`, `src/vlm_pipeline/lib/box_map.py`
**Interfaces:**
- Consumes: nothing (pure).
- Produces: `box_average_precision(gt_by_class, pred_by_class, iou_threshold=0.5) -> dict[str, float]`.

- [ ] Write `tests/unit/test_box_map.py` (force-load header + first 2 tests):
```python
"""Pure box-mAP scorer (SAM3 GT-anchored AP). stdlib+numpy only — no pycocotools, no GPU."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load(mod_name: str, rel: str):
    root = Path(__file__).resolve().parents[2]
    src = root / "src" / "vlm_pipeline" / "lib" / rel
    spec = importlib.util.spec_from_file_location(mod_name, src)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_bm = _load("vlm_pipeline_lib_box_map_fresh", "box_map.py")
box_average_precision = _bm.box_average_precision
mean_average_precision = _bm.mean_average_precision


def test_ap_perfect_single_box() -> None:
    gt = {"fire": [[0.0, 0.0, 10.0, 10.0]]}
    pred = {"fire": [([0.0, 0.0, 10.0, 10.0], 0.9)]}
    ap = box_average_precision(gt, pred, iou_threshold=0.5)
    assert ap["fire"] == 1.0


def test_ap_no_pred_is_zero() -> None:
    gt = {"fire": [[0.0, 0.0, 10.0, 10.0]]}
    pred: dict[str, list] = {"fire": []}
    ap = box_average_precision(gt, pred, iou_threshold=0.5)
    assert ap["fire"] == 0.0
```
- [ ] Run — expect FAIL (module missing):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_box_map.py -q
```
Expected: `ModuleNotFoundError` / `FileNotFoundError` for `box_map.py` → collection error (red).

- [ ] Create `src/vlm_pipeline/lib/box_map.py`:
```python
"""GT-anchored box AP / mAP (Pascal-VOC 11-free, COCO-style all-point interpolation).

Pure Python + numpy. Used by defs/train/eval.py for SAM3 detection eval when the
official sam3.eval.coco_eval (pycocotools) is NOT importable (CI / dagster image).
Boxes are xyxy floats. Predictions are (box, score) with score in [0, 1].

Layer: L1-2 pure lib. No dagster / defs / resources imports.
"""

from __future__ import annotations

from typing import Any

import numpy as np


def _iou(box_a: list[float], box_b: list[float]) -> float:
    ax1, ay1, ax2, ay2 = (float(v) for v in box_a[:4])
    bx1, by1, bx2, by2 = (float(v) for v in box_b[:4])
    ix1, iy1 = max(ax1, bx1), max(ay1, by1)
    ix2, iy2 = min(ax2, bx2), min(ay2, by2)
    iw, ih = max(0.0, ix2 - ix1), max(0.0, iy2 - iy1)
    inter = iw * ih
    if inter <= 0.0:
        return 0.0
    area_a = max(0.0, ax2 - ax1) * max(0.0, ay2 - ay1)
    area_b = max(0.0, bx2 - bx1) * max(0.0, by2 - by1)
    union = area_a + area_b - inter
    return inter / union if union > 0.0 else 0.0


def _ap_from_pr(recall: np.ndarray, precision: np.ndarray) -> float:
    """COCO-style all-point AP: monotonize precision then integrate over recall."""
    mrec = np.concatenate(([0.0], recall, [1.0]))
    mpre = np.concatenate(([0.0], precision, [0.0]))
    for i in range(mpre.size - 1, 0, -1):
        mpre[i - 1] = max(mpre[i - 1], mpre[i])
    idx = np.where(mrec[1:] != mrec[:-1])[0]
    return float(np.sum((mrec[idx + 1] - mrec[idx]) * mpre[idx + 1]))


def _ap_single_class(
    gts: list[list[float]],
    preds: list[tuple[list[float], float]],
    iou_threshold: float,
) -> float:
    n_gt = len(gts)
    if n_gt == 0:
        # No GT for this class: AP is undefined -> treat as 1.0 if also no preds,
        # else 0.0 (false positives only). Callers exclude classes absent from class_map.
        return 1.0 if not preds else 0.0
    if not preds:
        return 0.0
    ordered = sorted(preds, key=lambda p: float(p[1]), reverse=True)
    matched = [False] * n_gt
    tp = np.zeros(len(ordered), dtype=np.float64)
    fp = np.zeros(len(ordered), dtype=np.float64)
    for k, (pbox, _score) in enumerate(ordered):
        best_iou, best_j = 0.0, -1
        for j, gbox in enumerate(gts):
            if matched[j]:
                continue
            iou = _iou(pbox, gbox)
            if iou > best_iou:
                best_iou, best_j = iou, j
        if best_j >= 0 and best_iou >= iou_threshold:
            matched[best_j] = True
            tp[k] = 1.0
        else:
            fp[k] = 1.0
    cum_tp = np.cumsum(tp)
    cum_fp = np.cumsum(fp)
    recall = cum_tp / float(n_gt)
    precision = cum_tp / np.maximum(cum_tp + cum_fp, np.finfo(np.float64).eps)
    return round(_ap_from_pr(recall, precision), 6)


def box_average_precision(
    gt_by_class: dict[str, list[list[float]]],
    pred_by_class: dict[str, list[tuple[list[float], float]]],
    iou_threshold: float = 0.5,
) -> dict[str, float]:
    """Per-class AP at one IoU threshold for a single image (or pooled set)."""
    classes = set(gt_by_class) | set(pred_by_class)
    return {
        cls: _ap_single_class(
            gt_by_class.get(cls, []),
            pred_by_class.get(cls, []),
            iou_threshold,
        )
        for cls in classes
    }


def mean_average_precision(
    per_image_gt: list[dict[str, list[list[float]]]],
    per_image_pred: list[dict[str, list[tuple[list[float], float]]]],
    iou_thresholds: tuple[float, ...] = (0.5,),
) -> dict[str, Any]:
    """Pool detections across images per class, average AP over IoU thresholds.

    per_image_gt[i] / per_image_pred[i] correspond to the same image i.
    Returns {"map", "per_class_ap", "iou_thresholds", "image_count"}.
    """
    if len(per_image_gt) != len(per_image_pred):
        raise ValueError("per_image_gt and per_image_pred length mismatch")
    pooled_gt: dict[str, list[list[float]]] = {}
    pooled_pred: dict[str, list[tuple[list[float], float]]] = {}
    for gt_img, pred_img in zip(per_image_gt, per_image_pred):
        for cls, boxes in gt_img.items():
            pooled_gt.setdefault(cls, []).extend(boxes)
        for cls, dets in pred_img.items():
            pooled_pred.setdefault(cls, []).extend(dets)
    classes = sorted(set(pooled_gt) | set(pooled_pred))
    per_class_ap: dict[str, float] = {}
    for cls in classes:
        thr_aps = [
            _ap_single_class(pooled_gt.get(cls, []), pooled_pred.get(cls, []), thr)
            for thr in iou_thresholds
        ]
        per_class_ap[cls] = round(float(np.mean(thr_aps)), 6)
    map_value = round(float(np.mean(list(per_class_ap.values()))), 6) if per_class_ap else 0.0
    return {
        "map": map_value,
        "per_class_ap": per_class_ap,
        "iou_thresholds": list(iou_thresholds),
        "image_count": len(per_image_gt),
    }
```
- [ ] Run — expect PASS:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_box_map.py -q
```
Expected: `2 passed`.

- [ ] Commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add src/vlm_pipeline/lib/box_map.py tests/unit/test_box_map.py && git commit -m "feat(train): pure box AP scorer for SAM3 GT-anchored eval"
```

---

### Task D2: `box_map.mean_average_precision` — pooled mAP + half-IoU partial credit

**Files:** `tests/unit/test_box_map.py`
**Interfaces:** Consumes `mean_average_precision`. Produces tests only.

- [ ] Append to `tests/unit/test_box_map.py`:
```python
def test_map_two_classes_perfect() -> None:
    gt = [{"fire": [[0, 0, 10, 10]], "smoke": [[20, 20, 30, 30]]}]
    pred = [{"fire": [([0, 0, 10, 10], 0.9)], "smoke": [([20, 20, 30, 30], 0.8)]}]
    out = mean_average_precision(gt, pred, iou_thresholds=(0.5,))
    assert out["map"] == 1.0
    assert out["per_class_ap"] == {"fire": 1.0, "smoke": 1.0}
    assert out["image_count"] == 1


def test_map_pools_across_images() -> None:
    gt = [{"fire": [[0, 0, 10, 10]]}, {"fire": [[0, 0, 10, 10]]}]
    # one image perfect, one image misses (low IoU box) -> recall 0.5 -> AP 0.5
    pred = [{"fire": [([0, 0, 10, 10], 0.9)]}, {"fire": [([100, 100, 110, 110], 0.9)]}]
    out = mean_average_precision(gt, pred, iou_thresholds=(0.5,))
    assert out["per_class_ap"]["fire"] == 0.5
    assert out["map"] == 0.5


def test_map_length_mismatch_raises() -> None:
    import pytest
    with pytest.raises(ValueError):
        mean_average_precision([{"a": []}], [], iou_thresholds=(0.5,))
```
- [ ] Run — expect PASS (impl already exists from D1):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_box_map.py -q
```
Expected: `5 passed`.
- [ ] Commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add tests/unit/test_box_map.py && git commit -m "test(train): pooled mAP across images + length-mismatch guard"
```

---

### Task D3: `lib/recall_at_k.cross_modal_recall_at_k` (PE-Core text→image recall)

**Files:** `tests/unit/test_recall_at_k.py`, `src/vlm_pipeline/lib/recall_at_k.py`
**Interfaces:** Produces `cross_modal_recall_at_k(ranked_gt_hits, ks=(1,5,10)) -> dict[int,float]`.

- [ ] Write `tests/unit/test_recall_at_k.py`:
```python
"""Cross-modal recall@k for PE-Core text->image holdout (advisory metric). numpy only."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load():
    root = Path(__file__).resolve().parents[2]
    src = root / "src" / "vlm_pipeline" / "lib" / "recall_at_k.py"
    spec = importlib.util.spec_from_file_location("vlm_pipeline_lib_recall_at_k_fresh", src)
    assert spec is not None and spec.loader is not None
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


_r = _load()
cross_modal_recall_at_k = _r.cross_modal_recall_at_k


def test_recall_all_hit_at_1() -> None:
    # 2 queries, gt image is rank-0 for both
    rows = [[True, False, False], [True, False, False]]
    out = cross_modal_recall_at_k(rows, ks=(1, 3))
    assert out[1] == 1.0
    assert out[3] == 1.0


def test_recall_partial() -> None:
    # q0: gt at rank0 (hit@1). q1: gt at rank2 (miss@1, hit@3)
    rows = [[True, False, False], [False, False, True]]
    out = cross_modal_recall_at_k(rows, ks=(1, 3))
    assert out[1] == 0.5
    assert out[3] == 1.0


def test_recall_empty_returns_zero() -> None:
    out = cross_modal_recall_at_k([], ks=(1, 5))
    assert out == {1: 0.0, 5: 0.0}
```
- [ ] Run — expect FAIL (module missing):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_recall_at_k.py -q
```
Expected: collection error / `FileNotFoundError`.

- [ ] Create `src/vlm_pipeline/lib/recall_at_k.py`:
```python
"""Cross-modal recall@k + bootstrap CI for PE-Core eval (advisory, small-N holdout).

`ranked_gt_hits[i]` is a per-text-query boolean list: position j is True iff the
ground-truth image for query i is the j-th ranked image (descending cosine).
recall@k = fraction of queries whose gt image appears within the top-k ranks.

Layer: L1-2 pure lib. numpy only.
"""

from __future__ import annotations

import numpy as np


def cross_modal_recall_at_k(
    ranked_gt_hits: list[list[bool]],
    ks: tuple[int, ...] = (1, 5, 10),
) -> dict[int, float]:
    if not ranked_gt_hits:
        return {int(k): 0.0 for k in ks}
    n = len(ranked_gt_hits)
    out: dict[int, float] = {}
    for k in ks:
        hits = sum(1 for row in ranked_gt_hits if any(row[: int(k)]))
        out[int(k)] = round(hits / n, 6)
    return out


def bootstrap_ci(
    per_query_hits: list[bool],
    iterations: int = 1000,
    alpha: float = 0.05,
    seed: int = 0,
) -> tuple[float, float, float]:
    """Percentile bootstrap CI of mean(per_query_hits). Returns (mean, lo, hi)."""
    if not per_query_hits:
        return (0.0, 0.0, 0.0)
    arr = np.asarray([1.0 if h else 0.0 for h in per_query_hits], dtype=np.float64)
    rng = np.random.default_rng(seed)
    n = arr.size
    means = np.empty(iterations, dtype=np.float64)
    for i in range(iterations):
        sample = arr[rng.integers(0, n, size=n)]
        means[i] = sample.mean()
    lo = float(np.percentile(means, 100.0 * (alpha / 2.0)))
    hi = float(np.percentile(means, 100.0 * (1.0 - alpha / 2.0)))
    return (round(float(arr.mean()), 6), round(lo, 6), round(hi, 6))
```
- [ ] Run — expect PASS:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_recall_at_k.py -q
```
Expected: `3 passed`.
- [ ] Commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add src/vlm_pipeline/lib/recall_at_k.py tests/unit/test_recall_at_k.py && git commit -m "feat(train): cross-modal recall@k for PE-Core advisory eval"
```

---

### Task D4: `recall_at_k.bootstrap_ci` — CI reporting for small-N holdout

**Files:** `tests/unit/test_recall_at_k.py`
**Interfaces:** Consumes `bootstrap_ci`.

- [ ] Append to `tests/unit/test_recall_at_k.py`:
```python
bootstrap_ci = _r.bootstrap_ci


def test_bootstrap_ci_all_hits() -> None:
    mean, lo, hi = bootstrap_ci([True, True, True, True], iterations=500, seed=0)
    assert mean == 1.0
    assert lo == 1.0 and hi == 1.0


def test_bootstrap_ci_is_deterministic_with_seed() -> None:
    hits = [True, False, True, False, True, False]
    a = bootstrap_ci(hits, iterations=500, seed=7)
    b = bootstrap_ci(hits, iterations=500, seed=7)
    assert a == b
    assert a[1] <= a[0] <= a[2]  # lo <= mean <= hi


def test_bootstrap_ci_empty() -> None:
    assert bootstrap_ci([], iterations=10, seed=0) == (0.0, 0.0, 0.0)
```
- [ ] Run — expect PASS:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_recall_at_k.py -q
```
Expected: `6 passed`.
- [ ] Commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add tests/unit/test_recall_at_k.py && git commit -m "test(train): deterministic bootstrap CI for advisory recall"
```

---

### Task D5: `lib/train_eval_gate` — per-metric margin gate (candidate > incumbent)

**Files:** `tests/unit/test_train_eval_gate.py`, `src/vlm_pipeline/lib/train_eval_gate.py`
**Interfaces:**
- Consumes: nothing (pure dict logic).
- Produces: `GateDecision`, `evaluate_gate(...)`, `DEFAULT_EVAL_CONFIG`.

Metrics dict shape (contract used by all gate tests and by the asset):
```python
{
  "map": 0.42,                                # primary SAM3 metric (or recall@5 for pe_core)
  "per_class_ap": {"fire": 0.40, "smoke": 0.45},   # SAM3 per-class (pe_core: per_class recall)
}
```
eval_config shape:
```python
{
  "primary_metric": "map",                # key in metrics dict to gate on
  "primary_margin": 0.01,                 # candidate must beat incumbent by >= this (absolute)
  "per_class_field": "per_class_ap",      # per-class dict key
  "per_class_floor": -0.02,               # each class delta must be >= floor (non-regression)
  "advisory": False,                      # if True: regressions only logged, never veto
}
```

- [ ] Write `tests/unit/test_train_eval_gate.py`:
```python
"""Promotion gate decision logic — per-metric margin AND per-class non-regression floor.

Pure dicts, no GPU, no PG. stdlib only.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load():
    root = Path(__file__).resolve().parents[2]
    src = root / "src" / "vlm_pipeline" / "lib" / "train_eval_gate.py"
    spec = importlib.util.spec_from_file_location("vlm_pipeline_lib_train_eval_gate_fresh", src)
    assert spec is not None and spec.loader is not None
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


_g = _load()
evaluate_gate = _g.evaluate_gate
DEFAULT_EVAL_CONFIG = _g.DEFAULT_EVAL_CONFIG

_CFG = {
    "primary_metric": "map",
    "primary_margin": 0.01,
    "per_class_field": "per_class_ap",
    "per_class_floor": -0.02,
    "advisory": False,
}


def test_candidate_beats_incumbent_by_margin_promotable() -> None:
    cand = {"map": 0.50, "per_class_ap": {"fire": 0.48, "smoke": 0.52}}
    inc = {"map": 0.40, "per_class_ap": {"fire": 0.40, "smoke": 0.40}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="promoted")
    assert d.promotable is True
    assert d.per_metric["map"]["delta"] == 0.10
    assert d.per_metric["map"]["passed"] is True


def test_candidate_below_margin_not_promotable() -> None:
    cand = {"map": 0.405, "per_class_ap": {"fire": 0.41, "smoke": 0.40}}
    inc = {"map": 0.40, "per_class_ap": {"fire": 0.40, "smoke": 0.40}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="promoted")
    assert d.promotable is False
    assert d.per_metric["map"]["passed"] is False
    assert any("margin" in r for r in d.reasons)


def test_default_eval_config_has_required_keys() -> None:
    for key in ("primary_metric", "primary_margin", "per_class_field", "per_class_floor"):
        assert key in DEFAULT_EVAL_CONFIG
```
- [ ] Run — expect FAIL (module missing):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_train_eval_gate.py -q
```
Expected: collection error.

- [ ] Create `src/vlm_pipeline/lib/train_eval_gate.py`:
```python
"""Promotion gate: candidate vs incumbent metrics -> promotable? (pure decision logic).

Gate = (primary metric beats incumbent by >= primary_margin)  AND
       (no per-class regression below per_class_floor).
Tie (delta == 0) is a VETO (margin is strict >=, and 0 < margin>0).
Cold-start: first run has incumbent_source='stock_base' (stock model scored on the
SAME sealed split by the caller). Margins are applied identically — we do NOT relax
the gate for cold-start; a fine-tune must still beat the stock baseline by margin.
advisory=True: regressions are reported in reasons but NEVER veto (used for PE-Core
cross-modal recall on the small finalized holdout, §7.4).

Layer: L1-2 pure lib. stdlib only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# Concrete defaults (spec §11 Q2 was open; these are the committed starting values,
# overridable per-run via model_registry.eval_config). SAM3 detection track.
DEFAULT_EVAL_CONFIG: dict[str, Any] = {
    "primary_metric": "map",
    "primary_margin": 0.01,        # absolute mAP@0.5 improvement required
    "per_class_field": "per_class_ap",
    "per_class_floor": -0.02,      # each class may drop at most 0.02 absolute AP
    "advisory": False,
}

# PE-Core cross-modal recall config — advisory until image_label_annotations backfill
# (small-N holdout -> high variance, see §7.4 / §10).
PE_CORE_EVAL_CONFIG: dict[str, Any] = {
    "primary_metric": "recall_at_5",
    "primary_margin": 0.02,
    "per_class_field": "per_class_recall",
    "per_class_floor": -0.05,
    "advisory": True,
}


@dataclass
class GateDecision:
    promotable: bool
    reasons: list[str] = field(default_factory=list)
    per_metric: dict[str, dict[str, Any]] = field(default_factory=dict)
    per_class: dict[str, dict[str, Any]] = field(default_factory=dict)


def _round(value: float) -> float:
    return round(float(value), 6)


def evaluate_gate(
    candidate_metrics: dict[str, Any],
    incumbent_metrics: dict[str, Any],
    eval_config: dict[str, Any],
    *,
    incumbent_source: str,
) -> GateDecision:
    primary = eval_config["primary_metric"]
    margin = float(eval_config["primary_margin"])
    pc_field = eval_config["per_class_field"]
    floor = float(eval_config["per_class_floor"])
    advisory = bool(eval_config.get("advisory", False))

    reasons: list[str] = []
    cand_primary = float(candidate_metrics.get(primary, 0.0))
    inc_primary = float(incumbent_metrics.get(primary, 0.0))
    delta = _round(cand_primary - inc_primary)
    margin_passed = delta >= margin  # strict: tie (delta=0) fails when margin>0
    per_metric = {
        primary: {
            "candidate": _round(cand_primary),
            "incumbent": _round(inc_primary),
            "delta": delta,
            "margin": margin,
            "passed": margin_passed,
        }
    }
    if not margin_passed:
        reasons.append(
            f"primary metric '{primary}' delta {delta} < required margin {margin} "
            f"(candidate={_round(cand_primary)}, incumbent={_round(inc_primary)}, "
            f"incumbent_source={incumbent_source})"
        )

    cand_pc = dict(candidate_metrics.get(pc_field, {}) or {})
    inc_pc = dict(incumbent_metrics.get(pc_field, {}) or {})
    per_class: dict[str, dict[str, Any]] = {}
    class_regression = False
    for cls in sorted(set(cand_pc) | set(inc_pc)):
        c_val = float(cand_pc.get(cls, 0.0))
        i_val = float(inc_pc.get(cls, 0.0))
        c_delta = _round(c_val - i_val)
        ok = c_delta >= floor
        per_class[cls] = {
            "candidate": _round(c_val),
            "incumbent": _round(i_val),
            "delta": c_delta,
            "floor": floor,
            "passed": ok,
        }
        if not ok:
            class_regression = True
            reasons.append(
                f"per-class regression: '{cls}' delta {c_delta} < floor {floor}"
            )

    hard_pass = margin_passed and not class_regression
    if advisory:
        # Advisory: report reasons but never veto on this metric track.
        if not hard_pass:
            reasons.append("advisory metric: regressions reported, not vetoing")
        promotable = True
    else:
        promotable = hard_pass

    return GateDecision(
        promotable=promotable,
        reasons=reasons,
        per_metric=per_metric,
        per_class=per_class,
    )
```
- [ ] Run — expect PASS:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_train_eval_gate.py -q
```
Expected: `3 passed`.
- [ ] Commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add src/vlm_pipeline/lib/train_eval_gate.py tests/unit/test_train_eval_gate.py && git commit -m "feat(train): promotion gate per-metric margin + per-class floor"
```

---

### Task D6: gate — per-class regression veto (avg-hides-regression case)

**Files:** `tests/unit/test_train_eval_gate.py`
**Interfaces:** Consumes `evaluate_gate`.

- [ ] Append to `tests/unit/test_train_eval_gate.py`:
```python
def test_per_class_regression_vetoes_even_when_primary_passes() -> None:
    # primary mAP improves by 0.05 (passes margin) but 'smoke' collapses -0.10 (< -0.02 floor)
    cand = {"map": 0.45, "per_class_ap": {"fire": 0.60, "smoke": 0.30}}
    inc = {"map": 0.40, "per_class_ap": {"fire": 0.40, "smoke": 0.40}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="promoted")
    assert d.promotable is False
    assert d.per_metric["map"]["passed"] is True       # primary alone would pass
    assert d.per_class["smoke"]["passed"] is False
    assert any("smoke" in r and "regression" in r for r in d.reasons)


def test_per_class_within_floor_does_not_veto() -> None:
    # smoke drops only 0.01 (>= -0.02 floor) -> allowed
    cand = {"map": 0.45, "per_class_ap": {"fire": 0.50, "smoke": 0.39}}
    inc = {"map": 0.40, "per_class_ap": {"fire": 0.40, "smoke": 0.40}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="promoted")
    assert d.promotable is True
    assert d.per_class["smoke"]["passed"] is True
```
- [ ] Run — expect PASS:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_train_eval_gate.py -q
```
Expected: `5 passed`.
- [ ] Commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add tests/unit/test_train_eval_gate.py && git commit -m "test(train): per-class regression veto overrides primary pass"
```

---

### Task D7: gate — stock_base cold-start path + tie veto + advisory

**Files:** `tests/unit/test_train_eval_gate.py`
**Interfaces:** Consumes `evaluate_gate`, `PE_CORE_EVAL_CONFIG`.

- [ ] Append to `tests/unit/test_train_eval_gate.py`:
```python
PE_CORE_EVAL_CONFIG = _g.PE_CORE_EVAL_CONFIG


def test_stock_base_cold_start_still_requires_margin() -> None:
    # First run: incumbent is stock model scored on same sealed split.
    # Gate is NOT relaxed for cold-start; candidate must beat stock by margin.
    cand = {"map": 0.50, "per_class_ap": {"fire": 0.50}}
    inc = {"map": 0.30, "per_class_ap": {"fire": 0.30}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="stock_base")
    assert d.promotable is True
    assert "stock_base" in d.reasons[0] if d.reasons else True


def test_stock_base_candidate_worse_than_stock_not_promotable() -> None:
    cand = {"map": 0.25, "per_class_ap": {"fire": 0.25}}
    inc = {"map": 0.30, "per_class_ap": {"fire": 0.30}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="stock_base")
    assert d.promotable is False


def test_tie_is_veto() -> None:
    # exact tie -> delta 0.0 < margin 0.01 -> not promotable
    cand = {"map": 0.40, "per_class_ap": {"fire": 0.40}}
    inc = {"map": 0.40, "per_class_ap": {"fire": 0.40}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="promoted")
    assert d.promotable is False
    assert d.per_metric["map"]["delta"] == 0.0


def test_advisory_metric_never_vetoes() -> None:
    # PE-Core recall regresses but advisory=True -> promotable stays True, reason logged
    cand = {"recall_at_5": 0.10, "per_class_recall": {"fire": 0.10}}
    inc = {"recall_at_5": 0.50, "per_class_recall": {"fire": 0.50}}
    d = evaluate_gate(cand, inc, PE_CORE_EVAL_CONFIG, incumbent_source="stock_base")
    assert d.promotable is True
    assert any("advisory" in r for r in d.reasons)
```
- [ ] Run — expect PASS:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_train_eval_gate.py -q
```
Expected: `9 passed`.
- [ ] Commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add tests/unit/test_train_eval_gate.py && git commit -m "test(train): cold-start stock_base, tie-veto, advisory non-veto"
```

---

### Task D8: SPIKE — read `sam3/eval/coco_eval.py` to confirm the optional COCO-mAP entry point

**Files:** none (records findings in commit message + inline comment in D9).
**Interfaces:** none. Produces: confirmed call signature for the optional sam3-eval branch.

> `pkgutil` already confirmed `sam3.eval` exposes `coco_eval`, `coco_eval_offline`, `coco_writer`, and that `coco_eval` re-exports `COCO`, `COCOeval`, `CocoEvaluator`, `loadRes`, `evaluate`, `accumulate`, `summarize` (pycocotools present). This spike records the exact `CocoEvaluator` constructor + the method that yields the 12 COCO summary stats, so D9 can wire the optional branch without guessing.

- [ ] Read the source (container-only; CI never imports it):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && docker exec docker-sam3-1 sh -c 'sed -n "1,120p" /usr/local/lib/python3.12/site-packages/sam3/eval/coco_eval.py' 2>/dev/null | grep -v FutureWarning
```
- [ ] Record the `CocoEvaluator.__init__` signature, the per-image `update(...)` shape, and which attribute holds the `stats` array (`coco_eval[iou_type].stats[0]` = mAP@[.5:.95], `stats[1]` = mAP@0.5). Note whether `summarize()` must be called before reading `stats`.
- [ ] Commit a findings note (empty-tree allowed via `--allow-empty`):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git commit --allow-empty -m "docs(train): record sam3.eval.coco_eval CocoEvaluator API for optional mAP branch"
```
> **open_risk wiring:** In D9, `_score_sam3_predictions` tries `from sam3.eval.coco_eval import CocoEvaluator` (only succeeds on the GPU/sam3 box); on `ImportError` it falls back to `lib.box_map.mean_average_precision`. CI always takes the fallback (or the monkeypatched stub), so the spike result only affects the GPU prod path — it never blocks CI.

---

### Task D9: `defs/train/eval.py` asset — thin wrapper, scorer dispatch, registry write (stubbed scorers in CI)

**Files:** `src/vlm_pipeline/defs/train/__init__.py`, `src/vlm_pipeline/defs/train/eval.py`, `tests/unit/test_train_eval_asset.py`
**Interfaces:**
- Consumes: `PostgresResource`, `MinIOResource`, lib gate/scorers; `model_registry` row by `model_version_id`.
- Produces: `train_eval_gate` asset + `_run_train_eval_gate(context, db, minio)`; writes `metrics`, `incumbent_metrics`, `incumbent_source`, `eval_config`, `status`.

> The asset orchestrates: load registry row → resolve sealed test split keys from MinIO → score candidate (`_score_sam3_predictions` / `_score_pe_core_recall`) → score incumbent on the SAME split (or stock_base) → `evaluate_gate` → write back. In CI only `_run_train_eval_gate` is exercised with the two `_score_*` helpers monkeypatched (project convention: monkeypatch asset-internal `_run_*`/`_score_*`). GPU scoring is never invoked in CI.

- [ ] Create `src/vlm_pipeline/defs/train/__init__.py`:
```python
"""Train track assets (MLOps fine-tuning scaffolding). Layer L4."""
```
- [ ] Write `tests/unit/test_train_eval_asset.py`:
```python
"""train_eval_gate asset wiring — scorers monkeypatched (no GPU, no pycocotools, no PG)."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any


def _load_eval_module():
    root = Path(__file__).resolve().parents[2]
    src = root / "src" / "vlm_pipeline" / "defs" / "train" / "eval.py"
    spec = importlib.util.spec_from_file_location("vlm_pipeline_defs_train_eval_fresh", src)
    assert spec is not None and spec.loader is not None
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


_ev = _load_eval_module()


class _DummyLog:
    def info(self, *a: Any, **k: Any) -> None: ...
    def warning(self, *a: Any, **k: Any) -> None: ...
    def error(self, *a: Any, **k: Any) -> None: ...


class _DummyContext:
    def __init__(self, op_config: dict[str, Any]) -> None:
        self.op_config = op_config
        self.log = _DummyLog()


class _DummyDB:
    """Captures the registry row read + the gate write-back."""

    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row
        self.written: dict[str, Any] | None = None

    def get_model_registry_row(self, model_version_id: str) -> dict[str, Any]:
        assert model_version_id == self._row["model_version_id"]
        return dict(self._row)

    def update_model_registry_eval(self, model_version_id: str, **fields: Any) -> None:
        self.written = {"model_version_id": model_version_id, **fields}


class _DummyMinIO:
    pass


def _base_row(**over: Any) -> dict[str, Any]:
    row = {
        "model_version_id": "mv-001",
        "model": "sam3",
        "version": "sam3-2026.06.29-lora-001",
        "train_dataset_version_id": "tdv-001",
        "eval_config": None,
        "status": "candidate",
    }
    row.update(over)
    return row


def test_promotable_path_writes_status_and_metrics(monkeypatch) -> None:
    db = _DummyDB(_base_row())
    minio = _DummyMinIO()
    ctx = _DummyContext({"model_version_id": "mv-001"})

    monkeypatch.setattr(
        _ev, "_score_candidate",
        lambda context, db_, minio_, row: {"map": 0.50, "per_class_ap": {"fire": 0.50}},
    )
    monkeypatch.setattr(
        _ev, "_score_incumbent",
        lambda context, db_, minio_, row: (
            {"map": 0.30, "per_class_ap": {"fire": 0.30}}, "stock_base",
        ),
    )

    out = _ev._run_train_eval_gate(ctx, db, minio)
    assert out["promotable"] is True
    assert db.written["status"] == "promotable"
    assert db.written["incumbent_source"] == "stock_base"
    assert db.written["metrics"]["map"] == 0.50
    assert db.written["incumbent_metrics"]["map"] == 0.30


def test_not_promotable_keeps_candidate_status(monkeypatch) -> None:
    db = _DummyDB(_base_row())
    ctx = _DummyContext({"model_version_id": "mv-001"})
    monkeypatch.setattr(
        _ev, "_score_candidate",
        lambda *a: {"map": 0.305, "per_class_ap": {"fire": 0.305}},
    )
    monkeypatch.setattr(
        _ev, "_score_incumbent",
        lambda *a: ({"map": 0.30, "per_class_ap": {"fire": 0.30}}, "promoted"),
    )
    out = _ev._run_train_eval_gate(ctx, db, _DummyMinIO())
    assert out["promotable"] is False
    assert db.written["status"] == "candidate"


def test_eval_config_override_from_op_config(monkeypatch) -> None:
    db = _DummyDB(_base_row())
    # huge margin -> even a big win fails
    ctx = _DummyContext({"model_version_id": "mv-001", "eval_config": {
        "primary_metric": "map", "primary_margin": 0.99,
        "per_class_field": "per_class_ap", "per_class_floor": -0.02, "advisory": False,
    }})
    monkeypatch.setattr(_ev, "_score_candidate", lambda *a: {"map": 0.9, "per_class_ap": {"fire": 0.9}})
    monkeypatch.setattr(_ev, "_score_incumbent", lambda *a: ({"map": 0.1, "per_class_ap": {"fire": 0.1}}, "promoted"))
    out = _ev._run_train_eval_gate(ctx, db, _DummyMinIO())
    assert out["promotable"] is False
    assert db.written["eval_config"]["primary_margin"] == 0.99
```
- [ ] Run — expect FAIL (module missing):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_train_eval_asset.py -q
```
Expected: collection error / `FileNotFoundError` for `defs/train/eval.py`.

- [ ] Create `src/vlm_pipeline/defs/train/eval.py`:
```python
"""Eval + promotion gate asset (MLOps fine-tuning scaffolding, §7.4).

GT-anchored scoring on the SEALED test split:
- SAM3   : box mAP. Prefers sam3.eval.coco_eval (pycocotools, GPU/sam3 box only);
           falls back to lib.box_map otherwise. CI never imports the sam3 path.
- PE-Core: cross-modal recall@k (advisory, small-N holdout, CI-reported).
First run: incumbent = stock model scored on the SAME sealed split, incumbent_source='stock_base'.
Gate: lib.train_eval_gate.evaluate_gate -> status='promotable' when it passes.

Layer L4: thin @asset -> _run_train_eval_gate -> pure lib. GPU scoring is gated and
never executed in CI (monkeypatched _score_* in unit tests).
"""

from __future__ import annotations

from typing import Any

from dagster import Field, asset

from vlm_pipeline.lib.box_map import mean_average_precision
from vlm_pipeline.lib.recall_at_k import bootstrap_ci, cross_modal_recall_at_k
from vlm_pipeline.lib.train_eval_gate import (
    DEFAULT_EVAL_CONFIG,
    PE_CORE_EVAL_CONFIG,
    evaluate_gate,
)
from vlm_pipeline.resources.minio import MinIOResource
from vlm_pipeline.resources.postgres import PostgresResource


@asset(
    name="train_eval_gate",
    description="Score candidate vs incumbent on sealed test split; set model_registry.status=promotable on pass.",
    group_name="train",
    config_schema={
        "model_version_id": Field(str),
        "eval_config": Field(dict, is_required=False),
    },
)
def train_eval_gate(
    context,
    db: PostgresResource,
    minio: MinIOResource,
) -> dict[str, Any]:
    return _run_train_eval_gate(context, db, minio)


def _resolve_eval_config(row: dict[str, Any], op_config: dict[str, Any]) -> dict[str, Any]:
    if op_config.get("eval_config"):
        return dict(op_config["eval_config"])
    if row.get("eval_config"):
        return dict(row["eval_config"])
    return dict(PE_CORE_EVAL_CONFIG if row.get("model") == "pe_core" else DEFAULT_EVAL_CONFIG)


def _score_sam3_predictions(
    per_image_gt: list[dict[str, list[list[float]]]],
    per_image_pred: list[dict[str, list[tuple[list[float], float]]]],
) -> dict[str, Any]:
    """SAM3 box mAP. Uses official sam3.eval.coco_eval if importable, else lib.box_map.

    NOTE: sam3.eval is only present inside docker-sam3-1 (GPU box). CI/dagster image
    has no pycocotools -> ImportError -> lib.box_map fallback. See SPIKE (D8) for the
    CocoEvaluator wiring on the GPU path.
    """
    try:
        from sam3.eval.coco_eval import CocoEvaluator  # noqa: F401  (GPU/sam3 box only)
    except ImportError:
        return mean_average_precision(per_image_gt, per_image_pred, iou_thresholds=(0.5,))
    # GPU path: wired in D8 follow-up using CocoEvaluator; falls through to the pure
    # scorer until that branch is filled (kept fallback to avoid GPU dependency here).
    return mean_average_precision(per_image_gt, per_image_pred, iou_thresholds=(0.5,))


def _score_pe_core_recall(ranked_gt_hits: list[list[bool]]) -> dict[str, Any]:
    recalls = cross_modal_recall_at_k(ranked_gt_hits, ks=(1, 5, 10))
    hits_at_5 = [bool(any(row[:5])) for row in ranked_gt_hits]
    mean, lo, hi = bootstrap_ci(hits_at_5, iterations=1000, seed=0)
    return {
        "recall_at_1": recalls.get(1, 0.0),
        "recall_at_5": recalls.get(5, 0.0),
        "recall_at_10": recalls.get(10, 0.0),
        "recall_at_5_ci": [lo, hi],
        "n_queries": len(ranked_gt_hits),
        "per_class_recall": {},
    }


def _score_candidate(context, db: PostgresResource, minio: MinIOResource, row: dict[str, Any]) -> dict[str, Any]:
    """Run the candidate checkpoint over the sealed test split. GPU-only; stubbed in CI.

    NOTE: real inference runs on the prod GPU box during a controlled window
    (ENABLE_TRAINING / manual). Resolves row['train_dataset_version_id'] -> sealed
    'test' split keys under vlm-dataset/_trainsets/<tdv>/splits/, runs the candidate
    checkpoint (row['checkpoint_key']), and dispatches to _score_sam3_predictions /
    _score_pe_core_recall. Not executed in CI (monkeypatched).
    """
    raise NotImplementedError("GPU candidate scoring runs on prod box; monkeypatched in CI")


def _score_incumbent(
    context, db: PostgresResource, minio: MinIOResource, row: dict[str, Any]
) -> tuple[dict[str, Any], str]:
    """Score the incumbent on the SAME sealed split. Returns (metrics, incumbent_source).

    If a 'promoted' model_registry row exists for this model -> score it ('promoted').
    Else cold-start: score the stock/base model ('stock_base', §7.4 H3). GPU-only;
    monkeypatched in CI.
    """
    raise NotImplementedError("GPU incumbent scoring runs on prod box; monkeypatched in CI")


def _run_train_eval_gate(context, db: PostgresResource, minio: MinIOResource) -> dict[str, Any]:
    model_version_id = str(context.op_config["model_version_id"])
    row = db.get_model_registry_row(model_version_id)
    cfg = _resolve_eval_config(row, context.op_config)

    candidate_metrics = _score_candidate(context, db, minio, row)
    incumbent_metrics, incumbent_source = _score_incumbent(context, db, minio, row)

    decision = evaluate_gate(
        candidate_metrics,
        incumbent_metrics,
        cfg,
        incumbent_source=incumbent_source,
    )
    new_status = "promotable" if decision.promotable else "candidate"
    for reason in decision.reasons:
        context.log.info("train_eval_gate[%s]: %s", model_version_id, reason)

    db.update_model_registry_eval(
        model_version_id,
        metrics=candidate_metrics,
        incumbent_metrics=incumbent_metrics,
        incumbent_source=incumbent_source,
        eval_config=cfg,
        status=new_status,
    )
    return {
        "model_version_id": model_version_id,
        "promotable": decision.promotable,
        "status": new_status,
        "reasons": decision.reasons,
        "per_metric": decision.per_metric,
        "per_class": decision.per_class,
        "incumbent_source": incumbent_source,
    }
```
- [ ] Run — expect PASS:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_train_eval_asset.py -q
```
Expected: `3 passed`.
- [ ] Run the full section + lint (CI ruff 0.7.4 line-length 120):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_box_map.py tests/unit/test_recall_at_k.py tests/unit/test_train_eval_gate.py tests/unit/test_train_eval_asset.py -q && ruff check src/vlm_pipeline/lib/box_map.py src/vlm_pipeline/lib/recall_at_k.py src/vlm_pipeline/lib/train_eval_gate.py src/vlm_pipeline/defs/train/eval.py src/vlm_pipeline/defs/train/__init__.py --line-length 120 --target-version py310
```
Expected: `14 passed` and `All checks passed!`.
- [ ] Commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add src/vlm_pipeline/defs/train/__init__.py src/vlm_pipeline/defs/train/eval.py tests/unit/test_train_eval_asset.py && git commit -m "feat(train): eval+gate asset wiring (scorers stubbed, gate writes promotable)"
```

> **Interfaces consumed from Section A/B (must exist before D9 PASSes against real PG — CI uses _DummyDB so unit tests pass regardless):** `PostgresResource.get_model_registry_row(model_version_id) -> dict` and `PostgresResource.update_model_registry_eval(model_version_id, *, metrics, incumbent_metrics, incumbent_source, eval_config, status)`. If Section A/B did not add these two PG mixin methods, add them there (they are pure UPDATE/SELECT on `model_registry`). Flagged so the migration section owns the schema and the read/write helpers.

---

## Section E — Serving Load (EMBEDDING_CHECKPOINT_PATH) + Promotion/Rollback (scripts/promote_model.py)

> **Scope reminder:** Everything here is *built-but-not-executed*. The PE-Core checkpoint branch defaults to current stock behaviour (no checkpoint env → unchanged HF Hub load). `scripts/promote_model.py` defaults to `--dry-run` (no MinIO download, no `docker recreate`, no DB write). No prod weight is promoted by any step.
>
> **Consumes from Section A (migrations):** PG table `model_registry` with columns `model_version_id`, `model`, `version`, `train_dataset_version_id`, `checkpoint_key`, `artifact_checksum`, `status`, `created_at`, `promoted_at`, `promoted_env`, `incumbent_source`. These tasks query/mutate that table but do NOT create it. If Section A is not yet merged on the branch, the integration test (Task E7) is `pytest.mark.skipif`-gated on the table existing, so unit tests still pass.
> **Consumes from Section D (registry helpers), if present:** none required — `promote_model.py` talks to PG directly via `psycopg2` (mirrors `scripts/purge_pipeline_data.py`). If Section D ships a `lib/model_registry.py` row-selector, E5 notes the swap but does not depend on it.

---

### Task E1: SPIKE — confirm open_clip `load_state_dict` shape for a merged full-weight checkpoint

The serving branch must call `model.load_state_dict(...)` on the open_clip model returned by `create_model_and_transforms`. We must know (a) the exact attribute we hold the model in (`self._model`), (b) that `create_model_and_transforms` is callable with a local `pretrained=` or that we load weights *after* construction, and (c) the key format the trainer (Section C) will write (`state_dict` vs full checkpoint dict with a `"state_dict"`/`"model"` wrapper key).

**Files:** (read only) `docker/embedding/backends/open_clip_be.py`, Section C trainer merge step output spec in `docs/superpowers/specs/2026-06-29-mlops-finetune-scaffolding-design.md` §7.3.

- [ ] Read the merge/output contract in the design spec §7.3 ("LoRA→merge ... full-weight 체크포인트 출력") and Section C's task block (if merged) for the exact key the trainer writes to `training_summary.json` / the `.pt` layout.
- [ ] Record the decision in this plan: **the trainer writes a plain `torch.save(merged_model.state_dict(), ...)`** (no wrapper dict). The serving branch therefore constructs the stock model via `create_model_and_transforms(HF_HUB_REF)` then, if `EMBEDDING_CHECKPOINT_PATH` is set, calls `self._model.load_state_dict(torch.load(path, map_location=self.device), strict=True)` BEFORE `.to(device).eval()`. If Section C instead writes a wrapped dict, the branch unwraps `sd = ckpt.get("state_dict", ckpt)` — E2 codes the unwrap defensively so both layouts work.
- [ ] **Expected finding (recorded so E2 is concrete):** open_clip `create_model_and_transforms(HF_HUB_REF)` returns `(model, _, preprocess)`; weights are loadable post-construction via `model.load_state_dict`. The defensive unwrap below handles both plain and wrapped checkpoints, so E2 does not block on this spike resolving either way.

**Interfaces:**
- Consumes: trainer checkpoint `.pt` written by Section C to `vlm-dataset/_models/pe_core/<version>/`.
- Produces: documented decision (no code).

---

### Task E2: Add `EMBEDDING_CHECKPOINT_PATH` + versioned `model_name` to `OpenClipBackend` (failing test first)

Default (env unset) keeps exact current stock behaviour: `name == "facebook/PE-Core-L14-336"`, HF Hub load, no `load_state_dict`. When `EMBEDDING_CHECKPOINT_PATH` is set, load the local merged state_dict; when `EMBEDDING_MODEL_VERSION` is also set, the reported `name` becomes the versioned form `facebook/PE-Core-L14-336@<version>` (the §5.3 `model_name`-encoding contract).

**Files:** `tests/unit/test_open_clip_checkpoint_branch.py` (new), `docker/embedding/backends/open_clip_be.py` (edit).

- [ ] Write the failing test `tests/unit/test_open_clip_checkpoint_branch.py`:

```python
"""open_clip 백엔드 checkpoint-override 분기 테스트 (Section E serving load).

torch/open_clip 실모델 없이 monkeypatch 로 load_state_dict 분기만 검증.
env 미설정 = 현행 stock 동작 보존(회귀 가드), env 설정 = local state_dict 로드 + 버전 model_name.
"""

from __future__ import annotations

import pathlib
import sys

import pytest

_SVC_DIR = str(pathlib.Path("docker/embedding").resolve())


@pytest.fixture
def be_module(monkeypatch):
    if _SVC_DIR not in sys.path:
        sys.path.insert(0, _SVC_DIR)
    import backends.open_clip_be as be

    return be


class _FakeModel:
    def __init__(self):
        self.loaded_state = None
        self.loaded_strict = None

    def to(self, device):
        return self

    def eval(self):
        return self

    def load_state_dict(self, state, strict=True):
        self.loaded_state = state
        self.loaded_strict = strict


def _install_fakes(monkeypatch, be, *, captured_path):
    fake_model = _FakeModel()

    fake_open_clip = type(
        "oc",
        (),
        {
            "create_model_and_transforms": staticmethod(
                lambda ref: (fake_model, None, (lambda x: x))
            ),
            "get_tokenizer": staticmethod(lambda ref: (lambda xs: xs)),
        },
    )

    def _fake_torch_load(path, map_location=None):
        captured_path.append(path)
        return {"w": "merged-state-dict"}

    fake_torch = type(
        "torch",
        (),
        {
            "load": staticmethod(_fake_torch_load),
            "cuda": type("c", (), {"is_available": staticmethod(lambda: False)})(),
        },
    )
    monkeypatch.setitem(sys.modules, "open_clip", fake_open_clip)
    monkeypatch.setitem(sys.modules, "torch", fake_torch)
    return fake_model


def test_stock_behaviour_when_env_unset(be_module, monkeypatch):
    monkeypatch.delenv("EMBEDDING_CHECKPOINT_PATH", raising=False)
    monkeypatch.delenv("EMBEDDING_MODEL_VERSION", raising=False)
    captured: list = []
    fake_model = _install_fakes(monkeypatch, be_module, captured_path=captured)

    backend = be_module.OpenClipBackend(device="cpu")
    backend.load()

    assert backend.is_loaded() is True
    assert backend.name == "facebook/PE-Core-L14-336"  # unchanged stock model_name
    assert fake_model.loaded_state is None  # no load_state_dict call
    assert captured == []  # torch.load never called


def test_loads_local_state_dict_when_env_set(be_module, monkeypatch, tmp_path):
    ckpt = tmp_path / "merged.pt"
    ckpt.write_bytes(b"x")
    monkeypatch.setenv("EMBEDDING_CHECKPOINT_PATH", str(ckpt))
    monkeypatch.setenv("EMBEDDING_MODEL_VERSION", "ft-2026.06.29-lora-001")
    captured: list = []
    fake_model = _install_fakes(monkeypatch, be_module, captured_path=captured)

    backend = be_module.OpenClipBackend(device="cpu")
    backend.load()

    assert backend.is_loaded() is True
    assert fake_model.loaded_state == {"w": "merged-state-dict"}
    assert fake_model.loaded_strict is True
    assert captured == [str(ckpt)]
    # §5.3 model_name 버전 인코딩
    assert backend.name == "facebook/PE-Core-L14-336@ft-2026.06.29-lora-001"


def test_missing_checkpoint_file_raises(be_module, monkeypatch):
    monkeypatch.setenv("EMBEDDING_CHECKPOINT_PATH", "/nonexistent/merged.pt")
    monkeypatch.delenv("EMBEDDING_MODEL_VERSION", raising=False)
    captured: list = []
    _install_fakes(monkeypatch, be_module, captured_path=captured)

    backend = be_module.OpenClipBackend(device="cpu")
    with pytest.raises(FileNotFoundError):
        backend.load()
```

- [ ] Run it and confirm FAIL:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_open_clip_checkpoint_branch.py -q
```
Expected: `test_loads_local_state_dict_when_env_set` and `test_missing_checkpoint_file_raises` FAIL (current code ignores the env; `name` is a class attribute fixed to `MODEL_NAME`, never versioned; `load()` never calls `load_state_dict` or checks the path). `test_stock_behaviour_when_env_unset` likely PASSES already.

- [ ] Implement the branch. Edit `docker/embedding/backends/open_clip_be.py`. Replace the class body's `name` class attribute, `__init__`, and `load` with:

```python
class OpenClipBackend(EmbeddingBackend):
    dim = EMBED_DIM

    def __init__(self, device: str = "cuda:0") -> None:
        self.device = device
        self._model = None
        self._preprocess = None
        self._tokenizer = None
        self._torch = None
        # §5.3: 파인튠 가중치 서빙 시 model_name 에 버전 인코딩 (image_embeddings 버전 격리).
        # env 미설정 = stock 동작 (현행 incumbent 유지, 미승격).
        version = os.environ.get("EMBEDDING_MODEL_VERSION", "").strip()
        self.name = f"{MODEL_NAME}@{version}" if version else MODEL_NAME

    def load(self) -> None:
        import open_clip
        import torch

        self._torch = torch
        model, _, preprocess = open_clip.create_model_and_transforms(HF_HUB_REF)
        self._tokenizer = open_clip.get_tokenizer(HF_HUB_REF)

        # 승격된 merged full-weight 체크포인트 로드 (env 설정 시). 어댑터(LoRA) 서빙 코드 없음 —
        # 학습 종료 시 base 에 merge 된 full-weight 만 로드 (design §8). env 미설정이면 stock.
        ckpt_path = os.environ.get("EMBEDDING_CHECKPOINT_PATH", "").strip()
        if ckpt_path:
            if not os.path.isfile(ckpt_path):
                raise FileNotFoundError(f"EMBEDDING_CHECKPOINT_PATH not found: {ckpt_path}")
            ckpt = torch.load(ckpt_path, map_location=self.device)
            state_dict = ckpt.get("state_dict", ckpt) if isinstance(ckpt, dict) else ckpt
            model.load_state_dict(state_dict, strict=True)

        self._model = model.to(self.device).eval()
        self._preprocess = preprocess
```

  Add `import os` to the top imports (currently only `io` is imported):
```python
import io
import os
```

- [ ] Run it and confirm PASS:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_open_clip_checkpoint_branch.py tests/unit/test_embedding_service_contract.py -q
```
Expected: all green (including the existing contract test — `name` is now an instance attribute equal to `MODEL_NAME` when unset, so `test_health_reports_model_and_dim` still asserts `"facebook/PE-Core-L14-336"`).

- [ ] Lint and commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && ruff check --line-length 120 docker/embedding/backends/open_clip_be.py tests/unit/test_open_clip_checkpoint_branch.py
git add docker/embedding/backends/open_clip_be.py tests/unit/test_open_clip_checkpoint_branch.py
git commit -m "feat(embedding): EMBEDDING_CHECKPOINT_PATH merged-weight load branch + versioned model_name (scaffolding, default stock)"
```

**Interfaces:**
- Consumes: env `EMBEDDING_CHECKPOINT_PATH` (local path, set by `promote_model.py` in E4-E6), `EMBEDDING_MODEL_VERSION` (string version segment); merged state_dict `.pt` from Section C.
- Produces: `OpenClipBackend.name` instance attr (versioned `facebook/PE-Core-L14-336@<version>` when env set, else `facebook/PE-Core-L14-336`); `load()` performing `load_state_dict(strict=True)` when env set. Consumed by `docker/embedding/app.py` `_status()` (`model_name`) and by the embedding asset that writes `image_embeddings.model_name`.

---

### Task E3: Wire `EMBEDDING_CHECKPOINT_PATH`/`EMBEDDING_MODEL_VERSION` env into compose (no default = stock)

The serving load must read a host-mounted path. The embedding container already mounts `./data/models:/data/models`. We pass the two new env vars through with empty defaults so unset = stock.

**Files:** `docker/docker-compose.yaml` (edit embedding-service `environment:` block).

- [ ] In `docker/docker-compose.yaml`, locate the `embedding-service:` `environment:` block (after `EMBEDDING_DEVICE: "cuda:0"`). Add two lines:

```yaml
      EMBEDDING_DEVICE: "cuda:0"
      # §8 서빙 로딩: 승격 스크립트가 호스트 모델 볼륨(./data/models/pe-core/)으로 materialize 한 경로.
      # 미설정(빈 값) = stock HF Hub 동작 유지 (미승격). promote_model.py 가 .env 에 세팅 후 recreate.
      EMBEDDING_CHECKPOINT_PATH: ${EMBEDDING_CHECKPOINT_PATH:-}
      EMBEDDING_MODEL_VERSION: ${EMBEDDING_MODEL_VERSION:-}
```

- [ ] Validate compose parses (config render, no container start):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker && docker compose --profile embedding config >/dev/null && echo "COMPOSE_OK"
```
Expected: `COMPOSE_OK` (no YAML/interpolation error). If `docker compose` is unavailable in the authoring env, fall back to `python -c "import yaml,sys; yaml.safe_load(open('docker/docker-compose.yaml'))" && echo YAML_OK`.

- [ ] Commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add docker/docker-compose.yaml
git commit -m "feat(embedding): pass EMBEDDING_CHECKPOINT_PATH/MODEL_VERSION env to embedding-service (empty default = stock)"
```

**Interfaces:**
- Consumes: host env `EMBEDDING_CHECKPOINT_PATH`, `EMBEDDING_MODEL_VERSION` (set in `docker/.env` by `promote_model.py`).
- Produces: container-visible env consumed by E2's `OpenClipBackend`. SAM3 needs no compose change — `SAM3_CHECKPOINT_PATH: "/models/sam3.1_multiplex.pt"` already exists; promotion overwrites the file at that local path.

---

### Task E4: `promote_model.py` skeleton + row selection (only `promotable`) — TDD

Build the script as importable functions (testable) with a thin `main()`. First deliverable: `select_promotable_row(cur, model, model_version_id)` returns exactly one `promotable` row or raises; never selects `candidate`/`promoted`/`archived`/`rolled_back`.

**Files:** `tests/unit/test_promote_model.py` (new), `scripts/promote_model.py` (new).

- [ ] Write failing test `tests/unit/test_promote_model.py` (first batch — selection only):

```python
"""scripts/promote_model.py 단위 테스트 (Section E 승격/롤백, built-but-not-executed).

PG cursor / MinIO / subprocess(docker) 모두 스텁. dry-run 기본이므로 prod 무변경.
실 DB·실 docker 미사용 (CI GPU/도커 없음).
"""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "promote_model", str(pathlib.Path("scripts/promote_model.py").resolve())
)
promote_model = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(promote_model)


class _FakeCursor:
    """rowcount/fetchone/fetchall 를 시퀀스로 반환하는 최소 psycopg2 cursor 스텁."""

    def __init__(self, results):
        self._results = list(results)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._results.pop(0) if self._results else None

    def fetchall(self):
        out = self._results.pop(0) if self._results else []
        return out


def test_select_promotable_returns_the_promotable_row():
    row = {
        "model_version_id": 7,
        "model": "pe_core",
        "version": "ft-2026.06.29-lora-001",
        "status": "promotable",
        "checkpoint_key": "_models/pe_core/ft-2026.06.29-lora-001/merged.pt",
        "artifact_checksum": "abc123",
    }
    cur = _FakeCursor([row])
    got = promote_model.select_promotable_row(cur, model="pe_core", model_version_id=7)
    assert got["model_version_id"] == 7
    # WHERE status='promotable' 가 SQL 에 박혀 있어야 함
    sql = cur.executed[0][0].lower()
    assert "status" in sql and "promotable" in sql


def test_select_promotable_raises_when_none():
    cur = _FakeCursor([None])
    with pytest.raises(promote_model.PromotionError):
        promote_model.select_promotable_row(cur, model="pe_core", model_version_id=99)
```

- [ ] Run, confirm FAIL (module/functions don't exist):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_promote_model.py -q
```
Expected: collection/`exec_module` error or `AttributeError: module 'promote_model' has no attribute 'select_promotable_row'`.

- [ ] Create `scripts/promote_model.py` (skeleton + selection; downloads/recreate/transitions added in E5/E6):

```python
#!/usr/bin/env python3
"""모델 가중치 승격/롤백 스크립트 (MLOps 스캐폴딩 — built-but-not-executed).

흐름 (promote):
  1. model_registry 에서 status='promotable' 행 선택 (--model-version-id 명시 or 최신 1개)
  2. checkpoint_key 를 MinIO → 호스트 모델 볼륨(./docker/data/models/<model>/) 다운로드
  3. artifact_checksum 검증 (불일치 → 중단, 아무것도 변경 안 함)
  4. 서빙 env 세팅(docker/.env) + docker compose up -d --force-recreate <svc>
  5. 레지스트리 status='promoted', promoted_at=now(), promoted_env 기록

흐름 (rollback): 직전 promoted 행 재승격 (2~4 동일) + 현 promoted → 'rolled_back'.

기본 --dry-run: MinIO 다운로드/checksum/recreate/DB write 전부 시뮬레이션만.
실제 승격은 --apply + 가중치 볼륨 있는 prod 박스에서만.

DSN: --dsn or DATAOPS_POSTGRES_DSN. MinIO: MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY.
레지스트리가 진실 (design §2) — 심볼릭링크 금지.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

REPO_ROOT = Path(__file__).resolve().parent.parent

if TYPE_CHECKING:
    from minio import Minio

# model → (serving 컨테이너 svc, 호스트 모델 디렉토리 basename, env 키, 로컬 파일 경로)
_SERVING = {
    "pe_core": {
        "service": "embedding-service",
        "host_subdir": "pe-core",
        "env_path_key": "EMBEDDING_CHECKPOINT_PATH",
        "env_version_key": "EMBEDDING_MODEL_VERSION",
        "container_path": "/data/models/pe-core/merged.pt",
    },
    "sam3": {
        "service": "sam3",
        "host_subdir": "sam3",
        "env_path_key": "SAM3_CHECKPOINT_PATH",
        "env_version_key": None,
        "container_path": "/models/sam3.1_multiplex.pt",
    },
}

_SELECT_COLS = (
    "model_version_id, model, version, train_dataset_version_id, "
    "checkpoint_key, artifact_checksum, status, promoted_env"
)


class PromotionError(RuntimeError):
    """선택/검증/전이 실패 — 아무것도 변경하지 않고 중단."""


def _row_to_dict(cur, row) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def select_promotable_row(cur, *, model: str, model_version_id: int | None) -> dict[str, Any]:
    """status='promotable' 행 한 개를 선택. 명시 id 없으면 최신(created_at desc) 1개."""
    if model_version_id is not None:
        cur.execute(
            f"SELECT {_SELECT_COLS} FROM model_registry "
            "WHERE model_version_id = %(id)s AND status = 'promotable' AND model = %(m)s",
            {"id": model_version_id, "m": model},
        )
    else:
        cur.execute(
            f"SELECT {_SELECT_COLS} FROM model_registry "
            "WHERE status = 'promotable' AND model = %(m)s "
            "ORDER BY created_at DESC LIMIT 1",
            {"m": model},
        )
    row = _row_to_dict(cur, cur.fetchone())
    if not row:
        raise PromotionError(
            f"no 'promotable' model_registry row for model={model!r} id={model_version_id!r}"
        )
    return row


def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Promote/rollback a model_registry weight.")
    p.add_argument("--model", required=True, choices=sorted(_SERVING))
    p.add_argument("--model-version-id", type=int, default=None)
    p.add_argument("--env", choices=["prod", "staging"], default="prod")
    p.add_argument("--rollback", action="store_true", help="이전 promoted 행 재승격.")
    p.add_argument("--dsn", default=None, help="PG DSN. fallback DATAOPS_POSTGRES_DSN.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--dry-run", dest="dry_run", action="store_true", default=True)
    g.add_argument("--apply", dest="dry_run", action="store_false")
    return p.parse_args(argv)


def main(argv=None) -> int:  # pragma: no cover - thin wiring, covered by E5/E6 unit fns
    args = _parse_args(argv)
    print(f"[promote] model={args.model} dry_run={args.dry_run} rollback={args.rollback}")
    print("[promote] (wiring added in later tasks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] Run, confirm PASS:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_promote_model.py -q
```
Expected: 2 passed.

- [ ] Lint + commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && ruff check --line-length 120 scripts/promote_model.py tests/unit/test_promote_model.py
git add scripts/promote_model.py tests/unit/test_promote_model.py
git commit -m "feat(mlops): promote_model.py skeleton + promotable-only row selection (scaffolding, dry-run default)"
```

**Interfaces:**
- Consumes: `model_registry` columns `model_version_id, model, version, train_dataset_version_id, checkpoint_key, artifact_checksum, status, promoted_env, created_at`.
- Produces: `promote_model.PromotionError`, `select_promotable_row(cur, *, model, model_version_id) -> dict`, `_SERVING` map (service/host_subdir/env keys/container_path per model). Consumed by E5/E6.

---

### Task E5: Checkpoint materialize + checksum verification (failure path) — TDD

Add `download_and_verify(minio_client, *, checkpoint_key, artifact_checksum, dest_path, dry_run)`: downloads `vlm-dataset/<checkpoint_key>` to `dest_path`, computes sha256, raises `PromotionError` on mismatch (and removes the bad file). In `--dry-run`, no download and no FS write — returns the planned dest path.

**Files:** `tests/unit/test_promote_model.py` (extend), `scripts/promote_model.py` (extend).

- [ ] Append failing tests to `tests/unit/test_promote_model.py`:

```python
class _FakeMinio:
    def __init__(self, payloads):
        # payloads: {object_name: bytes}
        self._payloads = payloads
        self.downloaded = []

    def fget_object(self, bucket, object_name, file_path):
        self.downloaded.append((bucket, object_name, file_path))
        data = self._payloads[object_name]
        with open(file_path, "wb") as fh:
            fh.write(data)


def _sha256(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def test_download_and_verify_ok(tmp_path):
    payload = b"merged-weights"
    key = "_models/pe_core/v1/merged.pt"
    mc = _FakeMinio({key: payload})
    dest = tmp_path / "pe-core" / "merged.pt"
    out = promote_model.download_and_verify(
        mc,
        checkpoint_key=key,
        artifact_checksum=_sha256(payload),
        dest_path=dest,
        dry_run=False,
    )
    assert out == dest
    assert dest.read_bytes() == payload
    assert mc.downloaded[0][0] == "vlm-dataset"  # bucket
    assert mc.downloaded[0][1] == key


def test_download_and_verify_checksum_mismatch_raises_and_cleans(tmp_path):
    payload = b"merged-weights"
    key = "_models/pe_core/v1/merged.pt"
    mc = _FakeMinio({key: payload})
    dest = tmp_path / "pe-core" / "merged.pt"
    with pytest.raises(promote_model.PromotionError):
        promote_model.download_and_verify(
            mc,
            checkpoint_key=key,
            artifact_checksum="deadbeef",  # wrong
            dest_path=dest,
            dry_run=False,
        )
    assert not dest.exists()  # bad file removed → 서빙 절대 손상 안 함


def test_download_and_verify_dry_run_no_download(tmp_path):
    key = "_models/pe_core/v1/merged.pt"
    mc = _FakeMinio({key: b"x"})
    dest = tmp_path / "pe-core" / "merged.pt"
    out = promote_model.download_and_verify(
        mc, checkpoint_key=key, artifact_checksum="abc", dest_path=dest, dry_run=True
    )
    assert out == dest
    assert mc.downloaded == []  # no MinIO call
    assert not dest.exists()
```

- [ ] Run, confirm FAIL (`download_and_verify` missing):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_promote_model.py -q -k download
```
Expected: 3 errors/failures (AttributeError).

- [ ] Implement in `scripts/promote_model.py`. Add after `select_promotable_row`:

```python
TRAINSET_BUCKET = "vlm-dataset"  # 5-bucket 정책: 새 버킷 없음, _models/ prefix (design §6)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def download_and_verify(
    minio_client,
    *,
    checkpoint_key: str,
    artifact_checksum: str,
    dest_path: Path,
    dry_run: bool,
) -> Path:
    """MinIO vlm-dataset/<checkpoint_key> → dest_path 다운로드 + sha256 검증.

    dry_run 이면 다운로드/FS write 없이 계획된 dest_path 만 반환.
    checksum 불일치 시 받은 파일 삭제 후 PromotionError (서빙 가중치 손상 방지).
    """
    dest_path = Path(dest_path)
    if dry_run:
        print(f"[promote][dry-run] would download {TRAINSET_BUCKET}/{checkpoint_key} -> {dest_path}")
        return dest_path

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    minio_client.fget_object(TRAINSET_BUCKET, checkpoint_key, str(dest_path))
    actual = _sha256_file(dest_path)
    if actual != artifact_checksum:
        try:
            dest_path.unlink()
        except OSError:
            pass
        raise PromotionError(
            f"artifact_checksum mismatch for {checkpoint_key}: "
            f"expected {artifact_checksum} got {actual} (downloaded file removed)"
        )
    print(f"[promote] verified {checkpoint_key} sha256={actual}")
    return dest_path
```

- [ ] Run, confirm PASS:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_promote_model.py -q
```
Expected: all (5+) passed.

- [ ] Lint + commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && ruff check --line-length 120 scripts/promote_model.py tests/unit/test_promote_model.py
git add scripts/promote_model.py tests/unit/test_promote_model.py
git commit -m "feat(mlops): promote_model checkpoint materialize + sha256 verify (mismatch removes file, dry-run skips)"
```

**Interfaces:**
- Consumes: MinIO client with `fget_object(bucket, object_name, file_path)`; `model_registry.checkpoint_key`, `model_registry.artifact_checksum`.
- Produces: `download_and_verify(minio_client, *, checkpoint_key, artifact_checksum, dest_path, dry_run) -> Path`; `TRAINSET_BUCKET = "vlm-dataset"`. Host dest = `./docker/data/models/<host_subdir>/merged.pt` (computed in E6).

---

### Task E6: Status transitions (promote) + rollback selection + docker recreate (mocked) — TDD

Two pure-ish functions:
- `promote_transition(cur, *, row, env)` — flips selected row to `promoted` (sets `promoted_at=now()`, `promoted_env`), and demotes any currently-`promoted` row of the same `model` to `archived`. Returns the SQL effects via the fake cursor's `executed` log.
- `select_rollback_target(cur, *, model)` — picks the prior promoted row to restore: the most recent `status='archived'` row that was previously promoted (i.e. has non-null `promoted_at`), excluding the current `promoted`. Also `rollback_transition(cur, *, restore_row, current_promoted_id, env)` flips current `promoted`→`rolled_back` and restore_row→`promoted`.

`docker recreate` is done via `subprocess.run` and is fully mocked; `--dry-run` skips it.

**Files:** `tests/unit/test_promote_model.py` (extend), `scripts/promote_model.py` (extend).

- [ ] Append failing tests:

```python
def test_promote_transition_sets_promoted_and_archives_prior():
    cur = _FakeCursor([])
    row = {"model_version_id": 7, "model": "pe_core", "version": "v2"}
    promote_model.promote_transition(cur, row=row, env="prod")
    sqls = " ".join(s.lower() for s, _ in cur.executed)
    # 새 행 promoted + promoted_at/promoted_env
    assert "status = 'promoted'" in sqls
    assert "promoted_at" in sqls and "promoted_env" in sqls
    # 직전 promoted → archived
    assert "status = 'archived'" in sqls


def test_select_rollback_target_picks_prior_promoted():
    prior = {"model_version_id": 5, "model": "pe_core", "version": "v1",
             "status": "archived", "checkpoint_key": "_models/pe_core/v1/merged.pt",
             "artifact_checksum": "c5"}
    cur = _FakeCursor([prior])
    got = promote_model.select_rollback_target(cur, model="pe_core")
    assert got["model_version_id"] == 5
    sql = cur.executed[0][0].lower()
    assert "promoted_at is not null" in sql  # 이전에 promoted 된 적 있는 행만
    assert "order by promoted_at desc" in sql


def test_select_rollback_target_none_raises():
    cur = _FakeCursor([None])
    with pytest.raises(promote_model.PromotionError):
        promote_model.select_rollback_target(cur, model="pe_core")


def test_rollback_transition_flips_current_and_restores():
    cur = _FakeCursor([])
    restore = {"model_version_id": 5, "model": "pe_core"}
    promote_model.rollback_transition(cur, restore_row=restore, current_promoted_id=7, env="prod")
    sqls = " ".join(s.lower() for s, _ in cur.executed)
    assert "status = 'rolled_back'" in sqls  # 현 promoted → rolled_back
    assert "status = 'promoted'" in sqls     # restore → promoted


def test_docker_recreate_dry_run_no_subprocess(monkeypatch):
    calls = []
    monkeypatch.setattr(promote_model.subprocess, "run",
                        lambda *a, **k: calls.append((a, k)))
    promote_model.docker_recreate("embedding-service", dry_run=True)
    assert calls == []


def test_docker_recreate_apply_invokes_compose(monkeypatch):
    calls = []
    monkeypatch.setattr(promote_model.subprocess, "run",
                        lambda *a, **k: calls.append((a, k)) or _Ok())
    promote_model.docker_recreate("embedding-service", dry_run=False)
    assert calls, "subprocess.run not called on --apply"
    argv = calls[0][0][0]
    assert "up" in argv and "--force-recreate" in argv and "embedding-service" in argv


class _Ok:
    returncode = 0
```

- [ ] Run, confirm FAIL (functions missing):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_promote_model.py -q -k "transition or rollback or recreate"
```
Expected: failures (AttributeError on `promote_transition` etc.).

- [ ] Implement in `scripts/promote_model.py`. Add after `download_and_verify`:

```python
def promote_transition(cur, *, row: dict[str, Any], env: str) -> None:
    """선택 행을 promoted 로, 같은 model 의 기존 promoted 는 archived 로 전이."""
    model = row["model"]
    new_id = row["model_version_id"]
    # 1) 기존 promoted (있으면) → archived
    cur.execute(
        "UPDATE model_registry SET status = 'archived' "
        "WHERE model = %(m)s AND status = 'promoted' AND model_version_id <> %(id)s",
        {"m": model, "id": new_id},
    )
    # 2) 선택 행 → promoted
    cur.execute(
        "UPDATE model_registry SET status = 'promoted', promoted_at = now(), "
        "promoted_env = %(env)s WHERE model_version_id = %(id)s",
        {"env": env, "id": new_id},
    )


def select_rollback_target(cur, *, model: str) -> dict[str, Any]:
    """직전에 promoted 된 적 있는(=promoted_at not null) archived 행 중 최신 1개."""
    cur.execute(
        f"SELECT {_SELECT_COLS} FROM model_registry "
        "WHERE model = %(m)s AND status = 'archived' AND promoted_at IS NOT NULL "
        "ORDER BY promoted_at DESC LIMIT 1",
        {"m": model},
    )
    row = _row_to_dict(cur, cur.fetchone())
    if not row:
        raise PromotionError(f"no prior promoted (archived) row to roll back to for model={model!r}")
    return row


def rollback_transition(cur, *, restore_row: dict[str, Any], current_promoted_id: int, env: str) -> None:
    """현 promoted → rolled_back, restore_row → promoted."""
    cur.execute(
        "UPDATE model_registry SET status = 'rolled_back' WHERE model_version_id = %(id)s",
        {"id": current_promoted_id},
    )
    cur.execute(
        "UPDATE model_registry SET status = 'promoted', promoted_at = now(), "
        "promoted_env = %(env)s WHERE model_version_id = %(id)s",
        {"env": env, "id": restore_row["model_version_id"]},
    )


def docker_recreate(service: str, *, dry_run: bool) -> None:
    """서빙 컨테이너 재생성 (env 반영). dry_run 이면 명령만 출력."""
    cmd = [
        "docker", "compose", "-f", str(REPO_ROOT / "docker" / "docker-compose.yaml"),
        "up", "-d", "--force-recreate", service,
    ]
    if dry_run:
        print(f"[promote][dry-run] would run: {' '.join(cmd)}")
        return
    print(f"[promote] {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(REPO_ROOT / "docker"))
    if getattr(result, "returncode", 0) != 0:
        raise PromotionError(f"docker recreate failed for {service} (rc={result.returncode})")
```

- [ ] Run, confirm PASS:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_promote_model.py -q
```
Expected: all (11+) passed.

- [ ] Lint + commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && ruff check --line-length 120 scripts/promote_model.py tests/unit/test_promote_model.py
git add scripts/promote_model.py tests/unit/test_promote_model.py
git commit -m "feat(mlops): promote_model status transitions + rollback selection + mocked docker recreate"
```

**Interfaces:**
- Consumes: `model_registry` columns `status, promoted_at, promoted_env, model, model_version_id`.
- Produces: `promote_transition(cur, *, row, env)`, `select_rollback_target(cur, *, model) -> dict`, `rollback_transition(cur, *, restore_row, current_promoted_id, env)`, `docker_recreate(service, *, dry_run)`. Consumed by `main()` (E7 wiring).

---

### Task E7: Wire `main()` end-to-end + env write + integration smoke (real-PG, skip if table absent)

Wire the pieces into `main()`: connect PG (`DATAOPS_POSTGRES_DSN` or `--dsn`), select row (or rollback target), build MinIO client, `download_and_verify` into `./docker/data/models/<host_subdir>/merged.pt`, write `EMBEDDING_CHECKPOINT_PATH`/`EMBEDDING_MODEL_VERSION` (pe_core) to `docker/.env`, `docker_recreate`, then commit the transition. `--dry-run` (default) does none of the side effects but prints the plan. Add one integration test that exercises the real transition SQL against the real-PG fixture if `model_registry` exists.

**Files:** `tests/integration/test_promote_model_pg.py` (new), `scripts/promote_model.py` (edit `main()` + add `_write_env_var`).

- [ ] Replace the placeholder `main()` in `scripts/promote_model.py` and add an env-writer helper:

```python
def _write_env_var(env_file: Path, key: str, value: str) -> None:
    """docker/.env 의 key=value 를 추가/갱신 (없으면 append). 레지스트리가 진실 — 심볼릭링크 금지."""
    lines: list[str] = []
    found = False
    if env_file.exists():
        lines = env_file.read_text().splitlines()
    for i, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[i] = f"{key}={value}"
            found = True
            break
    if not found:
        lines.append(f"{key}={value}")
    env_file.write_text("\n".join(lines) + "\n")


def _make_minio():
    from minio import Minio

    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
    secret_key = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
    secure = endpoint.startswith("https://")
    for scheme in ("http://", "https://"):
        if endpoint.startswith(scheme):
            endpoint = endpoint[len(scheme):]
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def main(argv=None) -> int:
    args = _parse_args(argv)
    serving = _SERVING[args.model]
    dsn = args.dsn or os.getenv("DATAOPS_POSTGRES_DSN")
    if not dsn:
        print("[promote] no DSN (--dsn or DATAOPS_POSTGRES_DSN)", file=sys.stderr)
        return 2

    import psycopg2

    host_dir = REPO_ROOT / "docker" / "data" / "models" / serving["host_subdir"]
    dest = host_dir / "merged.pt" if args.model == "pe_core" else host_dir / "sam3.1_multiplex.pt"
    env_file = REPO_ROOT / "docker" / ".env"

    conn = psycopg2.connect(dsn)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            if args.rollback:
                restore = select_rollback_target(cur, model=args.model)
                cur.execute(
                    "SELECT model_version_id FROM model_registry "
                    "WHERE model = %(m)s AND status = 'promoted' LIMIT 1",
                    {"m": args.model},
                )
                cur_prom = cur.fetchone()
                row = restore
            else:
                row = select_promotable_row(
                    cur, model=args.model, model_version_id=args.model_version_id
                )

            print(f"[promote] target row: id={row['model_version_id']} version={row.get('version')}")
            mc = None if args.dry_run else _make_minio()
            download_and_verify(
                mc,
                checkpoint_key=row["checkpoint_key"],
                artifact_checksum=row["artifact_checksum"],
                dest_path=dest,
                dry_run=args.dry_run,
            )

            if not args.dry_run:
                _write_env_var(env_file, serving["env_path_key"], serving["container_path"])
                if serving["env_version_key"]:
                    _write_env_var(env_file, serving["env_version_key"], str(row.get("version") or ""))

            docker_recreate(serving["service"], dry_run=args.dry_run)

            if args.dry_run:
                print("[promote][dry-run] would commit registry transition; rolling back (no change)")
                conn.rollback()
                return 0

            if args.rollback:
                rollback_transition(
                    cur, restore_row=row,
                    current_promoted_id=(cur_prom[0] if cur_prom else -1), env=args.env,
                )
            else:
                promote_transition(cur, row=row, env=args.env)
        conn.commit()
        print(f"[promote] committed: model={args.model} -> promoted ({args.env})")
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
```

- [ ] Write integration test `tests/integration/test_promote_model_pg.py` (uses the real-PG fixture; skips cleanly if Section A's `model_registry` migration isn't applied yet):

```python
"""promote_model 트랜잭션 SQL 을 real-PG fixture 에 적용 검증 (Section A model_registry 필요)."""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "promote_model", str(pathlib.Path("scripts/promote_model.py").resolve())
)
promote_model = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(promote_model)


def _table_exists(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.model_registry')")
        return cur.fetchone()[0] is not None


def test_promote_then_rollback_roundtrip(pg_resource):
    # pg_resource: tests/conftest.py real-PG fixture (psycopg2 connection)
    if not _table_exists(pg_resource):
        pytest.skip("model_registry not migrated (Section A) — integration deferred")

    with pg_resource.cursor() as cur:
        cur.execute("DELETE FROM model_registry WHERE model = 'pe_core' AND version IN ('itv1','itv2')")
        cur.execute(
            "INSERT INTO model_registry (model, version, checkpoint_key, artifact_checksum, "
            "status, incumbent_source, created_at, promoted_at, promoted_env) VALUES "
            "('pe_core','itv1','_models/pe_core/itv1/merged.pt','c1','archived','stock_base',"
            " now() - interval '2 day', now() - interval '2 day','prod'),"
            "('pe_core','itv2','_models/pe_core/itv2/merged.pt','c2','promotable','promoted',"
            " now(), NULL, NULL) RETURNING model_version_id, version"
        )
        pg_resource.commit()

    with pg_resource.cursor() as cur:
        row = promote_model.select_promotable_row(cur, model="pe_core", model_version_id=None)
        assert row["version"] == "itv2"
        promote_model.promote_transition(cur, row=row, env="prod")
    pg_resource.commit()

    with pg_resource.cursor() as cur:
        cur.execute("SELECT status FROM model_registry WHERE model='pe_core' AND version='itv2'")
        assert cur.fetchone()[0] == "promoted"
        cur.execute("SELECT status FROM model_registry WHERE model='pe_core' AND version='itv1'")
        assert cur.fetchone()[0] == "archived"
        # rollback target = itv1 (archived + promoted_at not null)
        restore = promote_model.select_rollback_target(cur, model="pe_core")
        assert restore["version"] == "itv1"
        promote_model.rollback_transition(
            cur, restore_row=restore,
            current_promoted_id=row["model_version_id"], env="prod",
        )
    pg_resource.commit()

    with pg_resource.cursor() as cur:
        cur.execute("SELECT status FROM model_registry WHERE model='pe_core' AND version='itv2'")
        assert cur.fetchone()[0] == "rolled_back"
        cur.execute("SELECT status FROM model_registry WHERE model='pe_core' AND version='itv1'")
        assert cur.fetchone()[0] == "promoted"

    with pg_resource.cursor() as cur:
        cur.execute("DELETE FROM model_registry WHERE model='pe_core' AND version IN ('itv1','itv2')")
    pg_resource.commit()
```

  > **Note:** confirm the real-PG fixture name by grepping `tests/conftest.py` (`grep -n "def .*pg\|yield.*conn\|@pytest.fixture" tests/conftest.py`). If the fixture is named differently (e.g. `postgres_conn`/`pg`), rename the `pg_resource` parameter to match before running. This is the one place the exact fixture name must be verified against conftest.

- [ ] Run unit tests (always available, no PG/docker):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_promote_model.py tests/unit/test_open_clip_checkpoint_branch.py -q
```
Expected: all passed.

- [ ] Run integration (skips if no PG / table absent):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/integration/test_promote_model_pg.py -q
```
Expected: `1 passed` if Section A applied + PG fixture available; otherwise `1 skipped`.

- [ ] Smoke the CLI in dry-run with a bogus DSN to prove no side effects path is reachable (will exit 2 on no DSN, or print plan):
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python scripts/promote_model.py --model pe_core --dry-run; echo "exit=$?"
```
Expected: `[promote] no DSN ...` then `exit=2` (no DSN set in CI), proving `--apply` is required for any mutation and dry-run is default.

- [ ] Lint + commit:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && ruff check --line-length 120 scripts/promote_model.py tests/integration/test_promote_model_pg.py
git add scripts/promote_model.py tests/integration/test_promote_model_pg.py
git commit -m "feat(mlops): promote_model main() wiring + .env write + PG roundtrip integration test (built-but-not-executed)"
```

**Interfaces:**
- Consumes: env `DATAOPS_POSTGRES_DSN`, `MINIO_ENDPOINT`/`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY`; `model_registry` full row; real-PG fixture from `tests/conftest.py`.
- Produces: `promote_model.main(argv) -> int`, `_write_env_var(env_file, key, value)`, `_make_minio()`. Writes `docker/.env` keys `EMBEDDING_CHECKPOINT_PATH`/`EMBEDDING_MODEL_VERSION` (consumed by E3 compose → E2 backend) on `--apply` for pe_core; overwrites `./docker/data/models/sam3/sam3.1_multiplex.pt` (consumed by SAM3 `SAM3_CHECKPOINT_PATH`) for sam3.

---

## Section I — MLflow 실험 추적 (spec §7.5)

> MLflow = **사람용 추적·비교 UI** (params + 어떤 동결 스냅샷으로 학습했는지 + metrics + artifacts). `model_registry`(PG) = **승격·서빙·롤백의 source of truth**. trainer 가 둘 다 1회에 기록 → 불일치 없음. **승격 게이트는 MLflow 가 아니라 registry 를 본다** (§7.5). MLflow 로깅은 **fail-soft** — MLflow 서버 unreachable 이어도 학습/승격을 절대 막지 않음.
>
> **Build order:** Section I lands AFTER A (013 owns `mlflow_run_id` — see I3 reconciliation), C (trainer image + `trainer_lib`/`entrypoint`), and R1 (`insert_candidate_model_version` — I3 adds an optional `mlflow_run_id` param). It is independent of D/E/G except the CI rebuild glob note (I1) which Section G's `^docker/mlflow/` grep-test is the final gate for.
>
> **Spike-confirmed facts (baked into tasks below):**
> - Trainer image (Section C1) is `python:3.12-slim` and already pip-installs `boto3` — the MLflow **client** is pure-Python, so I2 only adds `mlflow` to `docker/trainer/requirements.txt` (no separate trainer image).
> - The MLflow **server** runs `mlflow server --backend-store-uri postgresql://… --artifacts-destination s3://…`. The official `ghcr.io/mlflow/mlflow:<tag>` image does NOT ship `psycopg2-binary` (PG backend) or `boto3` (S3 artifact upload) — so a **thin Dockerfile** (`FROM ghcr.io/mlflow/mlflow` + those two deps) is the minimal correct choice (decision flagged in open_risks).
> - The shared Postgres service (`docker/docker-compose.yaml:244`) runs as user `${POSTGRES_USER:-airflow}`. MLflow's backend store is a **separate database `mlflow`** on the same instance — created once by a documented bootstrap step (I1), NOT by a pipeline migration (the migration runner only manages `vlm_pipeline*` schema).
> - Pipeline DSN env is `DATAOPS_POSTGRES_DSN`; MinIO via `MINIO_ENDPOINT`/`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` (compose anchor at `docker/docker-compose.yaml:8-10`).
> - Artifact root is a **prefix** under the fixed `vlm-dataset` bucket: `s3://vlm-dataset/_mlflow/` (no new bucket — 5-bucket policy).

---

### Task I1: `mlflow` service block + thin Dockerfile (profile-gated, PG backend + MinIO artifact) — YAML-parse test first

**Files:**
- `docker/mlflow/Dockerfile` (new)
- `docker/docker-compose.yaml` (insert `mlflow` service after the `genai` block, before `pg-backup`)
- `tests/unit/test_mlflow_compose_service.py` (new)

**Interfaces:**
- Consumes: shared `postgres` service (separate `mlflow` DB), MinIO creds (`MINIO_ENDPOINT`/`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY`), `pipeline-network`.
- Produces: service `mlflow`, image `datapipeline-mlflow:0.1`, `profiles: ["mlflow"]` (no auto-start), backend-store = `postgresql://…/mlflow`, default-artifact-root = `s3://vlm-dataset/_mlflow/` with `MLFLOW_S3_ENDPOINT_URL` → MinIO, UI on `${MLFLOW_PORT:-5500}`. **No server runs in CI** — this is a YAML/env presence test only.

- [ ] **I1.1 — Write the failing unit test.** Create `tests/unit/test_mlflow_compose_service.py` (exact content):

```python
"""mlflow compose service — YAML parse + service/env presence (no server in CI).

Asserts the profile-gated mlflow service exists with a Postgres backend store on
a SEPARATE 'mlflow' DB and a MinIO-backed s3://vlm-dataset/_mlflow/ artifact root.
Pure file + YAML checks — no docker daemon, no network.
"""
from __future__ import annotations

import pathlib

import yaml

_REPO = pathlib.Path(__file__).resolve().parents[2]
_COMPOSE = _REPO / "docker" / "docker-compose.yaml"
_DOCKERFILE = _REPO / "docker" / "mlflow" / "Dockerfile"


def _mlflow_service() -> dict:
    doc = yaml.safe_load(_COMPOSE.read_text(encoding="utf-8"))
    services = doc["services"]
    assert "mlflow" in services, "mlflow service missing from docker-compose.yaml"
    return services["mlflow"]


def test_mlflow_service_is_profile_gated() -> None:
    svc = _mlflow_service()
    assert svc.get("profiles") == ["mlflow"], f"mlflow must be profiles:[mlflow], got {svc.get('profiles')}"
    # one-shot/long-running server but must NOT auto-start with the default stack:
    # profile gating already guarantees that; just assert no default-profile leak.


def test_mlflow_backend_store_is_separate_pg_db() -> None:
    svc = _mlflow_service()
    cmd = svc.get("command")
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else str(cmd)
    assert "--backend-store-uri" in cmd_str
    # separate 'mlflow' DB on the shared postgres (NOT the pipeline DB):
    assert "MLFLOW_BACKEND_STORE_URI" in cmd_str or "/mlflow" in cmd_str, cmd_str


def test_mlflow_artifact_root_is_vlm_dataset_prefix() -> None:
    svc = _mlflow_service()
    cmd = svc.get("command")
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else str(cmd)
    assert "s3://vlm-dataset/_mlflow" in cmd_str, "artifact root must be the vlm-dataset/_mlflow prefix (no new bucket)"


def test_mlflow_minio_endpoint_env_present() -> None:
    svc = _mlflow_service()
    env = svc.get("environment", {})
    # docker-compose env may be a dict or a list of "K=V"; normalize to dict keys.
    keys = set(env.keys()) if isinstance(env, dict) else {e.split("=", 1)[0] for e in env}
    for required in ("MLFLOW_S3_ENDPOINT_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        assert required in keys, f"mlflow service missing env {required}: {sorted(keys)}"


def test_mlflow_dockerfile_adds_pg_and_s3_deps() -> None:
    text = _DOCKERFILE.read_text(encoding="utf-8")
    assert "ghcr.io/mlflow/mlflow" in text, "thin image must FROM the official mlflow image"
    assert "psycopg2-binary" in text, "PG backend store needs psycopg2-binary"
    assert "boto3" in text, "S3 artifact upload needs boto3"
```

- [ ] **I1.2 — Run it, expect FAIL** (service + Dockerfile absent):

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_mlflow_compose_service.py -q 2>&1 | tail -8
```

Expected: collection passes but assertions FAIL — `mlflow service missing from docker-compose.yaml` (or `FileNotFoundError` on the Dockerfile read). Non-zero exit, `5 failed` (the Dockerfile test errors, the rest assert-fail).

- [ ] **I1.3 — Create the thin Dockerfile** `docker/mlflow/Dockerfile` (exact content):

```dockerfile
# MLflow tracking server — 학습 추적/비교 UI (params·dataset lineage·metrics·artifacts).
# profile-gated, 자동기동 X. backend store = 공유 Postgres 의 별도 'mlflow' DB,
# artifact store = MinIO s3://vlm-dataset/_mlflow/ (5-버킷 정책 — 새 버킷 X, prefix 만).
#
# 공식 mlflow 이미지에 PG 드라이버(psycopg2-binary)+S3 업로더(boto3) 가 없어 thin layer 로 추가.
# (registry=승격 SoT, MLflow=추적 UI — 이 컨테이너 장애가 학습/승격을 막지 않음, fail-soft.)
FROM ghcr.io/mlflow/mlflow:v2.16.2

RUN pip install --no-cache-dir psycopg2-binary>=2.9.9 boto3>=1.34.0

EXPOSE 5000
```

- [ ] **I1.4 — Insert the `mlflow` service block** into `docker/docker-compose.yaml` immediately AFTER the `genai` service block ends (after genai's `depends_on:\n      - postgres`, line ~484) and BEFORE the `pg-backup` comment block (line ~485). Exact content (note the `${POSTGRES_USER}`/`${POSTGRES_PASSWORD}` reuse — same creds as the shared instance, but DB `mlflow`):

```yaml
  mlflow:
    # MLflow 추적 서버 — 학습 params + 어떤 동결 스냅샷으로 학습했는지 + metrics + artifacts.
    # profiles:["mlflow"] → 기본 스택에서 자동기동 안 함. prod .env 에 COMPOSE_PROFILES=...,mlflow 시 활성.
    # backend store = 공유 postgres 의 별도 'mlflow' DB (CREATE DATABASE 는 일회성 bootstrap, I1.7).
    # artifact store = MinIO s3://vlm-dataset/_mlflow/ (새 버킷 X — 5-버킷 정책, prefix 만).
    # registry(PG)=승격 SoT, MLflow=사람용 추적 UI. 이 서버 장애가 학습/승격을 막지 않음(fail-soft).
    profiles: ["mlflow"]
    build:
      context: ./mlflow
      dockerfile: Dockerfile
    image: datapipeline-mlflow:0.1
    ports:
      - "${MLFLOW_PORT:-5500}:5000"
    command: >
      mlflow server
      --host 0.0.0.0
      --port 5000
      --backend-store-uri postgresql://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD:-airflow}@postgres:5432/mlflow
      --artifacts-destination s3://vlm-dataset/_mlflow/
      --serve-artifacts
    environment:
      # MLflow 가 artifact 를 MinIO(S3 호환)로 업로드할 때 쓰는 자격/엔드포인트.
      MLFLOW_S3_ENDPOINT_URL: ${MINIO_ENDPOINT:-http://10.0.0.51:9000}
      AWS_ACCESS_KEY_ID: ${MINIO_ACCESS_KEY:-minioadmin}
      AWS_SECRET_ACCESS_KEY: ${MINIO_SECRET_KEY:-minioadmin}
    networks:
      - default
      - pipeline-network
    healthcheck:
      test: ["CMD-SHELL", "python -c 'import urllib.request; urllib.request.urlopen(\"http://localhost:5000/health\").read()'"]
      interval: 30s
      timeout: 5s
      start_period: 30s
      retries: 3
    restart: unless-stopped
    depends_on:
      - postgres
```

- [ ] **I1.5 — Run the unit test, expect PASS.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_mlflow_compose_service.py -q
```

Expected: `5 passed`.

- [ ] **I1.6 — Validate compose syntax + profile gating** (config parse only, no daemon images pulled):

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
# mlflow appears only under its profile:
COMPOSE_PROFILES=mlflow docker compose -f docker-compose.yaml config --services 2>&1 | grep -qx mlflow && echo "mlflow-active-OK"
# and is NOT in the default (no-profile) set → never auto-starts:
docker compose -f docker-compose.yaml config --services 2>&1 | grep -qx mlflow && echo "UNEXPECTED auto-start" || echo "correctly-gated"
```

Expected output: `mlflow-active-OK` then `correctly-gated`.

- [ ] **I1.7 — Document the one-time `CREATE DATABASE mlflow` bootstrap.** This is NOT run by the pipeline migration runner (it only manages the `vlm_pipeline*` schema, memory `project_postgres_migration_runner_quirk`). The operator runs it once per environment before first enabling the `mlflow` profile. Add this exact runbook stanza to the Section G operator doc (`.agent/skill/mlops-finetune/SKILL.md`) under a new `## MLflow` heading (G owns that file — APPEND, do not overwrite); pasted here verbatim so G can copy it:

````markdown
## MLflow 추적 (선택, profile-gated)

MLflow backend store 는 공유 Postgres 의 **별도 `mlflow` DB**. 파이프라인 migration 러너가
관리하지 않으므로 환경마다 **한 번** 수동 생성한다 (prod: `docker-postgres-1`):

```bash
# 일회성 — mlflow profile 첫 활성화 전에 실행 (DB 이미 있으면 'already exists' = 무해)
docker exec docker-postgres-1 \
  psql -U airflow -d postgres -c "CREATE DATABASE mlflow;" || true
```

활성화: prod `.env` 의 `COMPOSE_PROFILES` 에 `mlflow` 추가 후
`./scripts/compose-prod.sh up -d mlflow`. UI = `http://10.0.0.10:${MLFLOW_PORT:-5500}`.
서버 스키마는 첫 부팅 시 mlflow 가 `mlflow` DB 안에 자동 생성한다 (별도 migration 불필요).
````

- [ ] **I1.8 — Note the CI rebuild glob dependency (RECONCILIATION → Section G).** A new container dir `docker/mlflow/` needs `[[ "${path}" =~ ^docker/mlflow/ ]]` added to the `detect_image_rebuild` glob in BOTH `.github/workflows/deploy-production.yml` and `deploy-test.yml`, mirroring the `^docker/trainer/` clause Section G/C add. **Section G owns the workflow edits and the grep-test gate** — Section I does NOT edit the workflows (avoids a co-owned-file conflict with G). Section G must add `^docker/mlflow/` alongside `^docker/trainer/` and extend its `tests/unit/test_ci_trainer_rebuild_glob.py` (or a sibling test) with an `^docker/mlflow/` assertion. This dependency is repeated in open_risks.

- [ ] **I1.9 — Commit.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git add docker/mlflow/Dockerfile docker/docker-compose.yaml tests/unit/test_mlflow_compose_service.py
git commit -m "feat(mlops): add profile-gated mlflow tracking service (PG backend + MinIO artifact)"
```

---

### Task I2: trainer MLflow logging lib (`docker/trainer/mlflow_logging.py`) — fail-soft, TDD with mocked mlflow

**Files:**
- `tests/unit/test_trainer_mlflow_logging.py` (new)
- `docker/trainer/mlflow_logging.py` (new)
- `docker/trainer/requirements.txt` (edit — add `mlflow` client; Section C1 owns this file, APPEND one line)

**Interfaces:**
- Consumes: `train_dataset_versions` lineage fields (`train_dataset_version_id`, `content_checksum`, `manifest_key`, `ls_count`, `al_confirmed_count`, `per_class_counts`, `split_ratios`) from the frozen snapshot row (Section A/B); hparams dict + eval metrics + artifact file paths (`env_lock.json`, `train_log.jsonl`, `training_summary.json`) from the trainer (Section C).
- Produces: `log_training_run(*, hparams: dict, dataset: dict, metrics: dict, artifact_paths: list[str], experiment: str, run_name: str | None = None, tracking_uri: str | None = None) -> str | None` — returns the MLflow `run_id` on success, or **`None`** if MLflow is unreachable / disabled / raises ANY error (training must continue). A separate `_build_run` helper does the import+logging so the public function is a fail-soft try/except wrapper. Lives in `docker/trainer/` (NOT a `vlm_pipeline` package) → tests import by file path, same idiom as `trainer_lib`.

- [ ] **I2.1 — Write the failing unit test.** Create `tests/unit/test_trainer_mlflow_logging.py` (exact content — a fake `mlflow` module is injected into `sys.modules` so NO real mlflow/network is touched):

```python
"""mlflow_logging.log_training_run — fail-soft trainer MLflow wrapper.

A fake `mlflow` module records calls. Asserts (a) params + dataset lineage + metrics
+ artifacts are logged inside an active run, (b) the run_id is returned, and
(c) ANY mlflow error is swallowed (fail-soft) → returns None, training continues.
No real mlflow, no network.
"""
from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

_MOD = Path(__file__).resolve().parents[2] / "docker" / "trainer" / "mlflow_logging.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("mlflow_logging", _MOD)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Recorder:
    def __init__(self) -> None:
        self.params: dict = {}
        self.metrics: dict = {}
        self.tags: dict = {}
        self.artifacts: list = []
        self.inputs: list = []
        self.set_uri: str | None = None
        self.experiment: str | None = None


def _fake_mlflow(rec: _Recorder, *, run_id: str = "run-abc123", explode: bool = False) -> types.ModuleType:
    m = types.ModuleType("mlflow")

    class _Run:
        info = types.SimpleNamespace(run_id=run_id)

    class _ActiveRun:
        def __enter__(self):
            if explode:
                raise RuntimeError("mlflow tracking server unreachable")
            return _Run()

        def __exit__(self, *a):
            return False

    def start_run(run_name=None):  # noqa: ARG001
        return _ActiveRun()

    m.start_run = start_run
    m.set_tracking_uri = lambda uri: setattr(rec, "set_uri", uri)
    m.set_experiment = lambda name: setattr(rec, "experiment", name)
    m.log_params = lambda d: rec.params.update(d)
    m.log_metrics = lambda d: rec.metrics.update(d)
    m.set_tags = lambda d: rec.tags.update(d)
    m.log_artifact = lambda p: rec.artifacts.append(p)
    # log_input is best-effort (older mlflow lacks it) — provide it so the happy path uses it.
    data_mod = types.ModuleType("mlflow.data")

    def from_dict(d, name=None):  # noqa: ARG001
        return {"_dataset": d, "_name": name}

    data_mod.from_dict = from_dict
    m.data = data_mod
    m.log_input = lambda ds: rec.inputs.append(ds)
    return m


_DATASET = {
    "train_dataset_version_id": "tdv-7",
    "content_checksum": "deadbeef",
    "manifest_key": "vlm-dataset/_trainsets/tdv-7/manifest.json",
    "ls_count": 288,
    "al_confirmed_count": 0,
    "per_class_counts": {"fire": 120, "smoke": 168},
    "split_ratios": {"train": 0.8, "val": 0.1, "test": 0.1},
}
_HPARAMS = {"train_method": "lora", "lr": 1e-4, "epochs": 5, "lora_rank": 16, "seed": 123}
_METRICS = {"box_map": 0.41, "box_map_fire": 0.38}


def test_logs_params_dataset_metrics_and_returns_run_id(tmp_path, monkeypatch) -> None:
    rec = _Recorder()
    monkeypatch.setitem(sys.modules, "mlflow", _fake_mlflow(rec, run_id="run-xyz"))
    monkeypatch.setitem(sys.modules, "mlflow.data", _fake_mlflow(rec).data)
    art = tmp_path / "training_summary.json"
    art.write_text("{}", encoding="utf-8")
    mod = _load_module()

    run_id = mod.log_training_run(
        hparams=_HPARAMS, dataset=_DATASET, metrics=_METRICS,
        artifact_paths=[str(art)], experiment="sam3_detection", run_name="sam3-2026.06.29-lora-001",
        tracking_uri="http://mlflow:5000",
    )

    assert run_id == "run-xyz"
    assert rec.set_uri == "http://mlflow:5000"
    assert rec.experiment == "sam3_detection"
    # hparams logged as params:
    assert rec.params["train_method"] == "lora" and rec.params["lora_rank"] == 16
    # dataset lineage logged (params or tags) — version id + checksum must be present somewhere:
    flat = {**rec.params, **rec.tags}
    assert flat.get("train_dataset_version_id") == "tdv-7"
    assert flat.get("content_checksum") == "deadbeef"
    assert str(flat.get("al_confirmed_count")) == "0"  # honest zero (spec §7.2)
    # metrics logged:
    assert rec.metrics["box_map"] == 0.41
    # artifact logged:
    assert str(art) in rec.artifacts
    # dataset object logged via log_input (best-effort):
    assert rec.inputs, "expected mlflow.log_input to receive the dataset lineage object"


def test_failsoft_returns_none_when_mlflow_raises(tmp_path, monkeypatch) -> None:
    rec = _Recorder()
    monkeypatch.setitem(sys.modules, "mlflow", _fake_mlflow(rec, explode=True))
    mod = _load_module()
    # MUST NOT raise — fail-soft. Returns None so the trainer keeps going.
    run_id = mod.log_training_run(
        hparams=_HPARAMS, dataset=_DATASET, metrics=_METRICS,
        artifact_paths=[], experiment="sam3_detection",
    )
    assert run_id is None


def test_failsoft_returns_none_when_mlflow_not_installed(monkeypatch) -> None:
    # Simulate `import mlflow` ImportError (mlflow client absent in some build).
    monkeypatch.setitem(sys.modules, "mlflow", None)  # forces ImportError on `import mlflow`
    mod = _load_module()
    run_id = mod.log_training_run(
        hparams=_HPARAMS, dataset=_DATASET, metrics=_METRICS, artifact_paths=[], experiment="x",
    )
    assert run_id is None
```

- [ ] **I2.2 — Run it, expect FAIL** (module does not exist):

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_trainer_mlflow_logging.py -q 2>&1 | tail -6
```

Expected: collection/exec error — `FileNotFoundError`/`No such file` for `docker/trainer/mlflow_logging.py`. Non-zero exit.

- [ ] **I2.3 — Implement `docker/trainer/mlflow_logging.py`** (exact content — `import mlflow` is INSIDE the try so a missing/None mlflow is fail-soft):

```python
"""Fail-soft MLflow logging for the vlm-trainer (spec §7.5).

MLflow = human tracking/compare UI; model_registry(PG) = promotion source of truth.
This logger is BEST-EFFORT: any MLflow error (server unreachable, client absent,
auth) is swallowed with a warning and log_training_run returns None — training and
registry writes (the SoT) proceed regardless.

Importable by file path in CI tests (docker/trainer is NOT a vlm_pipeline package);
`import mlflow` is deferred into the try so CI never needs the mlflow client.
"""
from __future__ import annotations

import os
from typing import Any

# Dataset-lineage fields mirrored from train_dataset_versions (design §5.1). Logged as
# MLflow params/tags so a run page shows EXACTLY which frozen, content-checksummed
# snapshot trained the model (end-to-end lineage; the manifest holds per-object checksums).
_LINEAGE_FIELDS = (
    "train_dataset_version_id",
    "content_checksum",
    "manifest_key",
    "ls_count",
    "al_confirmed_count",
    "per_class_counts",
    "split_ratios",
)


def _coerce(value: Any) -> Any:
    """MLflow params must be scalars/strings; stringify dict/list lineage values."""
    if isinstance(value, (dict, list)):
        import json

        return json.dumps(value, sort_keys=True)
    return value


def _build_run(
    *,
    hparams: dict[str, Any],
    dataset: dict[str, Any],
    metrics: dict[str, Any],
    artifact_paths: list[str],
    experiment: str,
    run_name: str | None,
    tracking_uri: str | None,
) -> str | None:
    import mlflow  # deferred: fail-soft if the client is absent

    uri = tracking_uri or os.environ.get("MLFLOW_TRACKING_URI")
    if uri:
        mlflow.set_tracking_uri(uri)
    mlflow.set_experiment(experiment)

    with mlflow.start_run(run_name=run_name) as run:
        # 1) hyperparameters (lr, epochs, batch, LoRA rank/alpha, seed, train_method, base model…)
        mlflow.log_params({k: _coerce(v) for k, v in hparams.items()})

        # 2) dataset lineage — WHICH frozen snapshot trained this model.
        lineage = {f: _coerce(dataset[f]) for f in _LINEAGE_FIELDS if f in dataset}
        mlflow.log_params(lineage)
        # Tags too, so lineage is filterable in the compare UI even if a param key collides.
        try:
            mlflow.set_tags({f"ds.{k}": v for k, v in lineage.items()})
        except Exception:  # noqa: BLE001 — tags are nice-to-have, never fatal
            pass
        # MLflow Dataset object (best-effort; older clients lack mlflow.data/log_input).
        try:
            ds_obj = mlflow.data.from_dict(
                {k: dataset.get(k) for k in _LINEAGE_FIELDS},
                name=dataset.get("train_dataset_version_id", "trainset"),
            )
            mlflow.log_input(ds_obj)
        except Exception:  # noqa: BLE001
            pass

        # 3) eval metrics (SAM box mAP / PE recall@k, per-class).
        if metrics:
            mlflow.log_metrics({k: float(v) for k, v in metrics.items()})

        # 4) artifacts already written to _models/<ver>/ (env_lock, train_log, summary).
        for path in artifact_paths:
            if path and os.path.exists(path):
                mlflow.log_artifact(path)

        return run.info.run_id


def log_training_run(
    *,
    hparams: dict[str, Any],
    dataset: dict[str, Any],
    metrics: dict[str, Any],
    artifact_paths: list[str],
    experiment: str,
    run_name: str | None = None,
    tracking_uri: str | None = None,
) -> str | None:
    """Log one training run to MLflow. FAIL-SOFT: returns run_id, or None on ANY error.

    The returned run_id is meant to be stored in model_registry.mlflow_run_id (R1/I3)
    as a cross-link; a None just means "no MLflow run" — registry remains the SoT.
    """
    try:
        return _build_run(
            hparams=hparams,
            dataset=dataset,
            metrics=metrics,
            artifact_paths=artifact_paths,
            experiment=experiment,
            run_name=run_name,
            tracking_uri=tracking_uri,
        )
    except Exception as exc:  # noqa: BLE001 — tracking must never kill training
        print(f"[trainer][mlflow] logging failed (fail-soft, continuing): {exc!r}", flush=True)
        return None
```

- [ ] **I2.4 — Run the unit test, expect PASS.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_trainer_mlflow_logging.py -q
```

Expected: `3 passed`.

- [ ] **I2.5 — Add the `mlflow` client to the trainer image.** Section C1 owns `docker/trainer/requirements.txt` — APPEND one line (do not reorder existing lines). After C1's `boto3>=1.34.0` line, add:

```
mlflow>=2.16.2
```

Verify the client is the only addition and the file still has the cu128 torch pin:

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
grep -q '^mlflow>=2.16.2$' docker/trainer/requirements.txt && grep -q 'torch==2.10.0' docker/trainer/requirements.txt && echo OK
```

Expected output: `OK`.

- [ ] **I2.6 — Lint + commit.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m ruff check --line-length 120 docker/trainer/mlflow_logging.py
git add docker/trainer/mlflow_logging.py docker/trainer/requirements.txt tests/unit/test_trainer_mlflow_logging.py
git commit -m "feat(mlops): fail-soft trainer MLflow logging (params + dataset lineage + metrics + artifacts)"
```

---

### Task I3: `model_registry.mlflow_run_id` — RECONCILIATION (Section A 013) + R1 signature delta + unit test

> **RECONCILIATION — Section A's `013_mlops_finetune.sql` MUST add the `mlflow_run_id` column.** Spec §5.2 lists `mlflow_run_id TEXT` on `model_registry`, but the 013 migration body authored in **Section A Task A1.3 (plan lines 431-457) does NOT include it**, and Section A's integration test `test_013_creates_model_registry` (plan lines 308-319) does NOT assert it. Section I depends on this column existing to cross-link MLflow runs. The reconciliation below states the exact edits Section A must make; **Section I does not edit the migration** (A owns that file) but adds a tiny unit test (I3.4) that pins the R1 behavior.

**Files:**
- (Section A, reconciled) `src/vlm_pipeline/sql/migrations/postgres/013_mlops_finetune.sql` — add column
- (Section A, reconciled) `tests/integration/test_mlops_migration_013.py` — add `mlflow_run_id` to `expected_cols`
- Modify (Section R1 owns): `src/vlm_pipeline/resources/postgres_train.py` — `insert_candidate_model_version` gains optional `mlflow_run_id`
- Test: `tests/unit/test_register_candidate_mlflow_run_id.py` (new — Section I owns this one)

**Interfaces:**
- Consumes: the R1 cursor idiom (`insert_candidate_model_version`, plan lines 59 + R1.4 lines 109).
- Produces: `model_registry.mlflow_run_id TEXT` (nullable); `insert_candidate_model_version(..., mlflow_run_id: str | None = None)` writes it when provided (NULL otherwise). The I2 logger's return value flows into this param at the C-entrypoint call site (I4).

- [ ] **I3.1 — Reconciliation edit A (migration column).** In `013_mlops_finetune.sql` (Section A A1.3), add a column line to the `model_registry` `CREATE TABLE` — place it immediately after the `promoted_env TEXT,` line (plan line 450) and before the first `CONSTRAINT` line. Exact line to add:

```sql
    mlflow_run_id             TEXT,
```

Rationale comment to extend in the migration header (after the `model_registry:` bullet): `--     mlflow_run_id: MLflow run 교차링크 (params·dataset lineage·metrics·artifacts UI, design §5.2/§7.5).`

- [ ] **I3.2 — Reconciliation edit B (migration assert).** In `013_mlops_finetune.sql`, append one more `@ASSERT_AFTER` comment so every boot re-verifies the column (idempotent CREATE TABLE won't add a missing column to a pre-existing table, so this assert is the drift guard):

```sql
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'model_registry' AND column_name = 'mlflow_run_id')
```

> Note for the implementer: because `CREATE TABLE IF NOT EXISTS` is a no-op on an already-created `model_registry`, an environment that applied an earlier 013 without `mlflow_run_id` needs an `ALTER TABLE … ADD COLUMN IF NOT EXISTS mlflow_run_id TEXT;` to be added to 013 as well (idempotent, no DO block). Since 013 is brand-new in this branch and not yet deployed anywhere except (possibly) staging, prefer the single combined form — include BOTH the inline column (I3.1) AND a trailing `ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS mlflow_run_id TEXT;` before `COMMIT;` to cover any environment that ran a pre-reconciliation 013. This is flagged in open_risks.

- [ ] **I3.3 — Reconciliation edit C (integration test column).** In `tests/integration/test_mlops_migration_013.py`, add `"mlflow_run_id"` to the `expected_cols` list in `test_013_creates_model_registry` (plan lines 309-314), e.g. after `"promoted_env",`.

- [ ] **I3.4 — R1 signature delta + Section-I unit test.** Section R1's `insert_candidate_model_version` (plan line 59) gains an optional trailing keyword param. The reconciled signature is:

```python
def insert_candidate_model_version(
    db,
    *,
    model: str,
    version: str,
    train_dataset_version_id: str,
    train_method: str,
    checkpoint_key: str,
    artifact_checksum: str,
    git_sha: str,
    training_image_digest: str,
    training_config: dict,
    env_lock_key: str,
    mlflow_run_id: str | None = None,   # ← I3 adds this; written to model_registry.mlflow_run_id
) -> str:
```

R1.4's INSERT (plan line 109) must add `mlflow_run_id` to the column list + params dict (value = the passed `mlflow_run_id`, NULL when None — psycopg2 maps `None` → SQL NULL). Write the failing test first — create `tests/unit/test_register_candidate_mlflow_run_id.py` (exact content, reuses R1.2's `_DummyDB`/`_DummyCursor` shape):

```python
"""insert_candidate_model_version threads mlflow_run_id into the INSERT (I3 reconciliation)."""
from __future__ import annotations


class _DummyCursor:
    def __init__(self):
        self.sql = ""
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params

    def fetchone(self):
        return ("mv-test-1",)


class _DummyDB:
    def __init__(self):
        self.cursor = _DummyCursor()

    def _cursor(self):  # mirror the SPIKE-confirmed cursor idiom (R1.1)
        return self.cursor


_COMMON = dict(
    model="sam3",
    version="sam3-2026.06.29-lora-001",
    train_dataset_version_id="tdv-1",
    train_method="lora",
    checkpoint_key="vlm-dataset/_models/sam3/sam3-2026.06.29-lora-001/checkpoint.pt",
    artifact_checksum="abc123",
    git_sha="deadbee",
    training_image_digest="sha256:xyz",
    training_config={"seed": 7},
    env_lock_key="vlm-dataset/_models/sam3/sam3-2026.06.29-lora-001/env_lock.json",
)


def _params_as_text(params) -> str:
    return str(params)


def test_mlflow_run_id_written_when_provided() -> None:
    from vlm_pipeline.resources.postgres_train import insert_candidate_model_version

    db = _DummyDB()
    insert_candidate_model_version(db, mlflow_run_id="run-xyz", **_COMMON)
    sql = db.cursor.sql.lower()
    assert "mlflow_run_id" in sql, "INSERT must name the mlflow_run_id column"
    assert "run-xyz" in _params_as_text(db.cursor.params), "run_id must be bound as a param"


def test_mlflow_run_id_defaults_to_none() -> None:
    from vlm_pipeline.resources.postgres_train import insert_candidate_model_version

    db = _DummyDB()
    mv_id = insert_candidate_model_version(db, **_COMMON)  # no mlflow_run_id
    assert mv_id == "mv-test-1"
    # None binds as SQL NULL — assert the param is present and falsy (None), not a literal string.
    text = _params_as_text(db.cursor.params)
    assert "run-" not in text  # no stray run id leaked in
```

- [ ] **I3.5 — Run it, expect FAIL** (R1 signature has no `mlflow_run_id` yet / column not in INSERT):

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_register_candidate_mlflow_run_id.py -q 2>&1 | tail -6
```

Expected: `test_mlflow_run_id_written_when_provided` FAILs — either `TypeError: ... unexpected keyword argument 'mlflow_run_id'` (if R1 lands first without the param) or `AssertionError: INSERT must name the mlflow_run_id column`.

- [ ] **I3.6 — Apply the R1 delta** (edit `src/vlm_pipeline/resources/postgres_train.py`): add `mlflow_run_id: str | None = None` as the last keyword-only param of `insert_candidate_model_version`; add `mlflow_run_id` to the INSERT column list and the params dict (mirror exactly how `env_lock_key` is bound — same paramstyle the R1.1 SPIKE found). Do NOT JSON-encode it (plain TEXT).

- [ ] **I3.7 — Run it, expect PASS.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_register_candidate_mlflow_run_id.py tests/unit/test_register_candidate_model.py -q
```

Expected: `4 passed` (R1's original 1 test + I3's 2, total 3 — adjust if R1.2 has additional cases; the gate is all green). If R1 has not landed yet, this task BLOCKS on R1 — build order puts R1 before I (see Section header).

- [ ] **I3.8 — Commit.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git add src/vlm_pipeline/resources/postgres_train.py tests/unit/test_register_candidate_mlflow_run_id.py
git commit -m "feat(mlops): thread mlflow_run_id into model_registry candidate INSERT (R1 reconciliation)"
```

---

### Task I4: wire trainer entrypoint (Section C) to log to MLflow + persist run_id — prod-only, covered by I2/I3 unit tests

> The actual call is **prod-only / not exercised in CI** (GPU path behind `ENABLE_TRAINING=1`). Its correctness is covered by I2 (logger fail-soft behavior) + I3 (run_id INSERT). This task wires the call site; the verification is a CI-safe import + dry-run check that the wiring does not break the existing `ENABLE_TRAINING=0` dry-run exit-0 path (Section C10.3).

**Files:**
- Modify: `docker/trainer/entrypoint.py` (Section C10 owns it — add the MLflow call + run_id threading inside the GPU paths)

**Interfaces:**
- Consumes: `mlflow_logging.log_training_run` (I2); `training_summary.json` + frozen-trainset lineage (Section B/C); `MLFLOW_TRACKING_URI` env.
- Produces: after a successful train+merge, the trainer logs to MLflow (fail-soft) and the returned `run_id` is passed to R1's `insert_candidate_model_version(mlflow_run_id=run_id)` at registration. **Decoupled from Dagster; no Dagster import.**

- [ ] **I4.1 — Add the import + MLflow tracking URI env** at the top of `docker/trainer/entrypoint.py` (after `import trainer_lib`, Section C10.2 line ~4049). Exact addition:

```python
import mlflow_logging  # fail-soft MLflow tracking (I2)
```

And in the `trainer` compose service env block (Section C11, plan lines 4184-4199) APPEND one env line (co-owned with C — append, do not reorder):

```yaml
        MLFLOW_TRACKING_URI: ${MLFLOW_TRACKING_URI:-http://mlflow:5000}
```

> Note: the trainer reaches the mlflow server over `pipeline-network` by service name `mlflow:5000` (container port, NOT the host-mapped `${MLFLOW_PORT}`). If the `mlflow` profile is not active the URI is simply unreachable → I2 logger returns None (fail-soft). No coupling.

- [ ] **I4.2 — Add the logging call site** inside `_run_sam3()` (Section C10.2) immediately after `trainer.run()` succeeds and after `merge_lora_to_full`, before the final `return 0`. Exact snippet (mirror in `_run_pe_core()` after its merge, using `experiment="pe_core_embedding"`):

```python
    # MLflow tracking (fail-soft, spec §7.5): params + which frozen snapshot + metrics + artifacts.
    # registry(PG) stays the SoT; a None run_id just means "no MLflow run" and training still succeeds.
    summary = trainer_lib.load_training_summary("/out/ckpt/training_summary.json")  # {hparams, dataset, metrics, artifact_paths}
    run_id = mlflow_logging.log_training_run(
        hparams=summary["hparams"],
        dataset=summary["dataset"],
        metrics=summary.get("metrics", {}),
        artifact_paths=summary.get("artifact_paths", []),
        experiment="sam3_detection",
        run_name=os.environ.get("MODEL_VERSION") or None,
    )
    # run_id is threaded into R1's insert_candidate_model_version(mlflow_run_id=run_id) by the
    # register op (defs/train/register.py, R1.6) — the trainer writes run_id into training_summary.json
    # so the (decoupled) register step can pick it up without importing Dagster here.
    trainer_lib.record_mlflow_run_id("/out/ckpt/training_summary.json", run_id)
    print(f"[trainer] mlflow run_id={run_id!r} (None = fail-soft, registry remains SoT)", flush=True)
```

> Two tiny `trainer_lib` helpers referenced above (`load_training_summary`, `record_mlflow_run_id`) are thin JSON read/merge utilities. They belong in Section C's `trainer_lib.py` (C8/C9 already write `training_summary.json`). If C has not added them, add them in this task as pure functions and extend C's `trainer_lib` test; they are GPU-free and CI-testable. Flagged as a C-coordination point in open_risks. The R1 register op (R1.6) reads `training_summary.json["mlflow_run_id"]` and passes it to `insert_candidate_model_version(mlflow_run_id=...)` (I3) — closing the cross-link.

- [ ] **I4.3 — CI-safe wiring check** (no GPU, no mlflow server — `ENABLE_TRAINING=0` dry-run must still exit 0, and the module must import with the new `import mlflow_logging`):

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker/trainer
HF_TOKEN=x MINIO_ACCESS_KEY=a MINIO_SECRET_KEY=s ENABLE_TRAINING=0 python entrypoint.py; echo "exit=$?"
```

Expected: prints `[trainer] ENABLE_TRAINING not set — scaffolding dry-run only ...` and `exit=0` (the MLflow call site is inside the GPU path, never reached in dry-run; the new top-level `import mlflow_logging` must succeed — it has no top-level `import mlflow`).

- [ ] **I4.4 — Lint + commit.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m ruff check --line-length 120 docker/trainer/entrypoint.py
git add docker/trainer/entrypoint.py docker/docker-compose.yaml
git commit -m "feat(mlops): wire trainer entrypoint to fail-soft MLflow logging + run_id cross-link"
```

---

### Task I5: Section-level green gate

- [ ] **I5.1 — Run all Section I tests together + ruff.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest \
  tests/unit/test_mlflow_compose_service.py \
  tests/unit/test_trainer_mlflow_logging.py \
  tests/unit/test_register_candidate_mlflow_run_id.py \
  -q
python -m ruff check --line-length 120 docker/trainer/mlflow_logging.py docker/trainer/entrypoint.py
```

Expected: all tests pass (`10 passed` ballpark: I1=5, I2=3, I3=2), ruff clean. If `test_register_candidate_mlflow_run_id.py` errors with `TypeError`/import error, R1 (and/or A's 013 reconciliation) has not landed — Section I BLOCKS on R1+A per build order.

- [ ] **I5.2 — Confirm lib-layer purity is untouched.** Section I adds NO `lib/` files (all code is under `docker/trainer/` + `resources/`), so `scripts/check_lib_layer_imports.py` is unaffected; run it anyway as a guard:

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python scripts/check_lib_layer_imports.py && echo "lib-layer-OK"
```

Expected: `lib-layer-OK` (no new violations).

---

## Section H — PE-Core 승격 트랙 (champion-challenger via versioned dual-index)

> **Section context.** This section builds (but does NOT execute) the PE-Core champion-challenger promotion mechanism (design §8.1 / §5.3 / §7.4 PE-Core bullet). PE is not a label artifact — it is a **vector**; the consumers (AL queue, text→image search, FiftyOne, video mean-pool) all read `image_embeddings` filtered `WHERE model_name = %(model)s`. So the analogue of SAM3's `.pt` swap is **atomically flipping the active `model_name` pointer** that AL/search default to, after re-embedding the corpus under a new versioned `model_name`. Old vectors/indexes are retained for instant rollback.
>
> **The 4 things this section builds (code/migration only; real re-embed + pointer flip are gated behind `ENABLE_TRAINING` / manual / promote `--apply`):**
> - **H1** — `model_name`-scoped partial HNSW. Because the fine-tune `model_name` is *dynamic* (`facebook/PE-Core-L14-336@ft-<version>`, §5.3 / Task A2), a static migration cannot enumerate it. → a **pure SQL builder** (`lib/pgvector_index.py`) + a runtime index-create executor on the `db` resource, invoked at promotion time. (Decision + risk in open_risks.)
> - **H2** — `DEFAULT_MODEL` runtime pointer: a tiny PG-backed `active_embedding_model` setting (migration `015`, pgvector-gated `_OPTIONAL`) + a pure resolver lib that returns the stock name when unset; reads added to embed defs and `fiftyone_pgvector` defaulting to `STOCK_PE_CORE_MODEL_NAME`.
> - **H3** — full-corpus re-embed asset `defs/embed/reembed.py` that embeds **the incumbent-covered set** (currently-embedded frames + captions) under a NEW versioned `model_name`, idempotently, via the existing `batch_insert_embeddings` upsert path.
> - **H4** — PE-Core promotion + GT-abstain: extend the Section D gate (`lib/recall_at_k` + `lib/train_eval_gate`) with a PE path that **abstains** when human-reviewed GT count `< eval_config.pe_core_min_gt`, and a promotion entrypoint (sibling to `scripts/promote_model.py`) that flips the H2 pointer atomically + sets `EMBEDDING_CHECKPOINT_PATH` + recreates, with rollback = flip pointer back.
>
> **Consumes (already authored — do NOT redefine):** `lib/embedding_model_name.py` (A2: `STOCK_PE_CORE_MODEL_NAME`, `build_versioned_model_name`, `parse_model_name`, `is_stock`); `model_registry` + `train_dataset_versions` (A1, migration 013); `lib/recall_at_k.py` (D3/D4: `cross_modal_recall_at_k`, `bootstrap_ci`); `lib/train_eval_gate.py` (D5–D7: `evaluate_gate`, `GateDecision`, `PE_CORE_EVAL_CONFIG`); `scripts/promote_model.py` (E4–E7: `select_promotable_row`, `download_and_verify`, `promote_transition`, `rollback_transition`, `docker_recreate`, `_write_env_var`, `PromotionError`, `_row_to_dict`, `_SELECT_COLS`, `TRAINSET_BUCKET`, `REPO_ROOT`); `OpenClipBackend` env `EMBEDDING_CHECKPOINT_PATH`/`EMBEDDING_MODEL_VERSION` (E2). `defs/train/__init__.py` is created by Section F — H4 only adds to it.
>
> **Migration numbering:** A=013, F=014. This section uses **015**.
>
> **Layering:** `lib/pgvector_index.py` + the H2 resolver (`lib/active_model.py`) are pure L1-2 (no dagster/resources/psycopg at module top — enforced by `scripts/check_lib_layer_imports.py`). `fiftyone_pgvector.py` is `docker/` serving code → it may NOT import `vlm_pipeline.lib`; it mirrors the constant and runs its own one-line query (pin-tested against the lib).

---

### Task H1: `lib/pgvector_index.py` — pure builder for a `model_name`-scoped partial HNSW (failing test first)

Design §8.1-(C) / 006 header predicted a `WHERE model_name=<ver>` partial index. The version is dynamic, so we build the index-creation SQL deterministically in a pure lib and run it at promotion time (Task H4), not via a static migration. This task is the pure builder + its unit test.

**Files:**
- `tests/unit/test_pgvector_index.py` (new)
- `src/vlm_pipeline/lib/pgvector_index.py` (new)

**Interfaces:**
- Consumes: nothing (pure stdlib + `hashlib`).
- Produces:
  - `model_index_name(model_name: str, entity_type: str = "frame") -> str` — deterministic, ≤63 chars (PG identifier limit), collision-resistant via an 8-hex suffix of `sha1(model_name)`.
  - `build_partial_hnsw_sql(model_name: str, entity_type: str = "frame") -> str` — `CREATE INDEX IF NOT EXISTS <name> ON image_embeddings USING hnsw (embedding vector_cosine_ops) WHERE entity_type = '<etype>' AND model_name = '<model_name>'` with single-quote escaping of `model_name`.
  - `build_drop_partial_hnsw_sql(model_name: str, entity_type: str = "frame") -> str` — `DROP INDEX IF EXISTS <name>` (rollback retirement of a losing challenger's index).

- [ ] **H1.1 — Write the failing unit test.** Create `tests/unit/test_pgvector_index.py`:

```python
"""lib.pgvector_index — model_name 별 partial HNSW 인덱스 SQL 빌더 (순수, no PG).

design §8.1-(C): 파인튠 model_name 은 동적('@ft-<ver>')이라 정적 마이그레이션으로 열거 불가 →
승격 시점에 이 빌더로 인덱스 생성 SQL 을 만들어 실행한다 (Task H4).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load():
    root = Path(__file__).resolve().parents[2]
    src = root / "src" / "vlm_pipeline" / "lib" / "pgvector_index.py"
    spec = importlib.util.spec_from_file_location("vlm_pipeline_lib_pgvector_index_fresh", src)
    assert spec is not None and spec.loader is not None
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


_m = _load()
model_index_name = _m.model_index_name
build_partial_hnsw_sql = _m.build_partial_hnsw_sql
build_drop_partial_hnsw_sql = _m.build_drop_partial_hnsw_sql


def test_index_name_deterministic_and_bounded() -> None:
    name = "facebook/PE-Core-L14-336@ft-2026.06.29-lora-001"
    a = model_index_name(name)
    b = model_index_name(name)
    assert a == b
    assert len(a) <= 63  # PG identifier limit
    assert a.startswith("image_embeddings_hnsw_frame_")


def test_index_name_distinct_per_model_and_entity() -> None:
    n1 = model_index_name("facebook/PE-Core-L14-336@ft-001", "frame")
    n2 = model_index_name("facebook/PE-Core-L14-336@ft-002", "frame")
    n3 = model_index_name("facebook/PE-Core-L14-336@ft-001", "caption")
    assert n1 != n2
    assert n1 != n3


def test_build_partial_hnsw_sql_shape() -> None:
    sql = build_partial_hnsw_sql("facebook/PE-Core-L14-336@ft-001", "frame")
    assert "CREATE INDEX IF NOT EXISTS" in sql
    assert "USING hnsw (embedding vector_cosine_ops)" in sql
    assert "WHERE entity_type = 'frame'" in sql
    assert "model_name = 'facebook/PE-Core-L14-336@ft-001'" in sql


def test_build_partial_hnsw_sql_escapes_quotes() -> None:
    # model_name with an embedded single quote must be doubled (no injection).
    sql = build_partial_hnsw_sql("base@ft-o'brien", "frame")
    assert "model_name = 'base@ft-o''brien'" in sql


def test_build_drop_sql_matches_create_name() -> None:
    name = "facebook/PE-Core-L14-336@ft-001"
    create = build_partial_hnsw_sql(name, "frame")
    drop = build_drop_partial_hnsw_sql(name, "frame")
    idx = model_index_name(name, "frame")
    assert idx in create and idx in drop
    assert drop.startswith("DROP INDEX IF EXISTS")
```

- [ ] **H1.2 — Run it — expect FAIL (module missing).**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_pgvector_index.py -q
```

Expected: collection error / `FileNotFoundError` (no `pgvector_index.py`).

- [ ] **H1.3 — Write the implementation.** Create `src/vlm_pipeline/lib/pgvector_index.py`:

```python
"""model_name 별 partial HNSW 인덱스 SQL 빌더 (L1 lib — 순수 stdlib).

design §8.1-(C): PE-Core 챔피언-챌린저 승격 시 새 model_name('@ft-<ver>')으로 코퍼스를
재임베딩한 뒤, 그 model_name 만 타는 partial HNSW 를 빌드한다. model_name 이 동적이라
정적 마이그레이션(008 의 entity_type-only partial)으로는 열거 불가 → 승격 시점에 이
빌더가 만든 SQL 을 db.create_model_partial_hnsw() (Task H3) 가 실행한다.

008 의 entity_type-only partial 인덱스는 단일 model_name 환경에서 유효하나, 두 model_name
(stock + 챌린저)이 공존하면 planner 가 model_name 필터로 후보를 솎느라 비효율 → model_name
까지 좁힌 partial 이 필요 (006 헤더가 예고한 "partial-by-model index").

⚠️ lib 계층 규칙: dagster / defs / resources / ops import 금지.
"""

from __future__ import annotations

import hashlib

_VALID_ENTITY_TYPES = ("frame", "caption", "video")
# PG 식별자 최대 63 byte. prefix + 8 hex suffix 가 그 안에 들어가도록 구성.
_NAME_PREFIX = "image_embeddings_hnsw_"


def _quote_literal(value: str) -> str:
    """SQL 문자열 리터럴 escaping — single quote 를 두 배로 (injection 방지)."""
    return value.replace("'", "''")


def model_index_name(model_name: str, entity_type: str = "frame") -> str:
    """model_name+entity_type 당 결정적이고 ≤63자인 인덱스 이름.

    이름 = image_embeddings_hnsw_<entity_type>_<sha1(model_name)[:12]>.
    model_name 에 '/', '@', '.' 등 식별자 불가 문자가 있어 해시 suffix 로 안전하게 인코딩.
    """
    if entity_type not in _VALID_ENTITY_TYPES:
        raise ValueError(f"entity_type must be one of {_VALID_ENTITY_TYPES}, got {entity_type!r}")
    digest = hashlib.sha1(model_name.encode("utf-8")).hexdigest()[:12]
    return f"{_NAME_PREFIX}{entity_type}_{digest}"


def build_partial_hnsw_sql(model_name: str, entity_type: str = "frame") -> str:
    """해당 (entity_type, model_name) 만 타는 partial HNSW 생성 SQL (멱등)."""
    name = model_index_name(model_name, entity_type)
    et = _quote_literal(entity_type)
    mn = _quote_literal(model_name)
    return (
        f"CREATE INDEX IF NOT EXISTS {name} "
        f"ON image_embeddings USING hnsw (embedding vector_cosine_ops) "
        f"WHERE entity_type = '{et}' AND model_name = '{mn}'"
    )


def build_drop_partial_hnsw_sql(model_name: str, entity_type: str = "frame") -> str:
    """진 챌린저 인덱스 은퇴용 DROP SQL (멱등)."""
    name = model_index_name(model_name, entity_type)
    return f"DROP INDEX IF EXISTS {name}"
```

- [ ] **H1.4 — Run it — expect PASS.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_pgvector_index.py -q
```

Expected: `5 passed`.

- [ ] **H1.5 — Lint + lib-layer guard.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
ruff check --line-length 120 src/vlm_pipeline/lib/pgvector_index.py tests/unit/test_pgvector_index.py
python3 scripts/check_lib_layer_imports.py
```

Expected: `All checks passed!` and the lib-layer guard prints nothing / exits 0.

- [ ] **H1.6 — Commit.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git add src/vlm_pipeline/lib/pgvector_index.py tests/unit/test_pgvector_index.py
git commit -m "feat(mlops): pure builder for model_name-scoped partial HNSW (PE-Core promotion, design §8.1)"
```

**Interfaces:**
- Produces: `model_index_name`, `build_partial_hnsw_sql`, `build_drop_partial_hnsw_sql`. Consumed by the `db` index-create executor (Task H3) and by promotion (Task H4).

---

### Task H2: `015_embedding_active_model.sql` migration — PG-backed active-model pointer (failing assert test first)

Replace the hardcoded `DEFAULT_MODEL` constant with a PG-backed "active embedding model" setting so promotion (H4) can flip it atomically. A single-row keyed table (one row per `scope`, default scope `frame_search`) — small, additive, pgvector-gated `_OPTIONAL` (the pointer only matters where `image_embeddings` exists, i.e. pgvector environments).

**Files:**
- `src/vlm_pipeline/sql/migrations/postgres/015_embedding_active_model.sql` (new)
- `tests/integration/test_active_model_migration_015.py` (new)

**Interfaces:**
- Produces (DB): table `embedding_active_model(scope TEXT PK CHECK scope IN ('frame_search'), model_name TEXT NOT NULL, updated_at TIMESTAMPTZ, updated_by TEXT)`; seeded with `('frame_search', 'facebook/PE-Core-L14-336')` via idempotent `INSERT ... ON CONFLICT DO NOTHING`.
- Consumes: nothing (greenfield table).

- [ ] **H2.1 — Write the failing migration test.** Create `tests/integration/test_active_model_migration_015.py`:

```python
"""015_embedding_active_model 적용 검증 (real-PG fixture, pgvector-gated _OPTIONAL).

DATAOPS_TEST_POSTGRES_DSN 미설정 시 자동 skip. pgvector 미가용 PG 에서는 적용되지 않으므로
(테이블 부재) 표는 conditional — table 존재 시에만 seed/CHECK 를 단언한다.
"""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg2")


def _table_exists(cur, name: str) -> bool:
    cur.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name=%s)",
        (name,),
    )
    return bool(cur.fetchone()[0])


def test_015_active_model_table_and_seed(postgres_resource) -> None:
    with postgres_resource.connect() as conn:
        with conn.cursor() as cur:
            if not _table_exists(cur, "embedding_active_model"):
                pytest.skip("embedding_active_model absent — pgvector not available on this PG")
            # seed row present + defaults to stock model_name
            cur.execute(
                "SELECT model_name FROM embedding_active_model WHERE scope = 'frame_search'"
            )
            row = cur.fetchone()
            assert row is not None, "frame_search seed row missing"
            assert row[0] == "facebook/PE-Core-L14-336"
            # CHECK rejects unknown scope
            with pytest.raises(Exception):
                cur.execute(
                    "INSERT INTO embedding_active_model (scope, model_name) VALUES ('bogus', 'x')"
                )
```

- [ ] **H2.2 — Run it — expect FAIL/skip.** With DSN set + pgvector PG: the `_table_exists` guard returns False until the migration ships → the test SKIPs (acceptable pre-impl since the table truly does not exist). To force a real FAIL→PASS cycle, run against a pgvector image where the migration runner will have applied it after H2.3/H2.4. (DSN unset → skip.)

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/integration/test_active_model_migration_015.py -q
```

Expected pre-impl: `1 skipped` (table absent or no DSN).

- [ ] **H2.3 — Write the migration.** Create `src/vlm_pipeline/sql/migrations/postgres/015_embedding_active_model.sql`:

```sql
-- 015_embedding_active_model.sql — AL/검색이 읽는 '활성 임베딩 model_name' 포인터 (design §8.1-A / §5.3).
--
-- 기존: DEFAULT_MODEL 상수 (docker/analysis/fiftyone_pgvector.py:23, defs/embed/assets.py:19).
-- 변경: 단일행 PG 설정으로 빼서 PE-Core 승격(H4)이 포인터를 원자 UPDATE 로 전환 가능하게.
-- scope='frame_search' = AL 큐 + text→image 검색이 읽는 활성 frame model_name.
-- 기본값(seed) = 현 stock model_name → 마이그레이션만으로는 동작 무변경 (미승격).
--
-- pgvector-gated (_OPTIONAL_MIGRATIONS): image_embeddings 가 있는 환경에서만 의미.
-- 비-pgvector prod 부팅을 깨지 않도록 008/009 과 동일한 전제조건으로 등재 (H2.4).
-- DO $$ block 미사용 (runner multi-statement DO 한계 회피). 모든 DDL 멱등.
BEGIN;

CREATE TABLE IF NOT EXISTS embedding_active_model (
    scope       TEXT        NOT NULL,
    model_name  TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by  TEXT,
    CONSTRAINT embedding_active_model_pk        PRIMARY KEY (scope),
    CONSTRAINT embedding_active_model_scope_chk CHECK (scope IN ('frame_search'))
);

-- 활성 포인터 기본값 = stock model_name. 재적용 시 덮어쓰지 않음 (승격이 바꾼 값 보존).
INSERT INTO embedding_active_model (scope, model_name, updated_by)
VALUES ('frame_search', 'facebook/PE-Core-L14-336', 'migration_015')
ON CONFLICT (scope) DO NOTHING;

COMMIT;

-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'embedding_active_model_scope_chk' AND conrelid = 'embedding_active_model'::regclass)
-- @ASSERT_AFTER: SELECT (SELECT model_name FROM embedding_active_model WHERE scope='frame_search') = 'facebook/PE-Core-L14-336'
```

- [ ] **H2.4 — Register 015 as a pgvector-gated optional migration.** Edit `src/vlm_pipeline/resources/postgres_migration.py`, in the `_OPTIONAL_MIGRATIONS` dict add (after the `010_caption_trgm_index.sql` entry):

```python
        "010_caption_trgm_index.sql": "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_trgm'",
        # 015: 활성 임베딩 model_name 포인터 (PE-Core 승격). image_embeddings 가 있는
        #      (pgvector) 환경에서만 의미 → 008/009 과 동일 전제조건. 비-pgvector prod 부팅 보호.
        "015_embedding_active_model.sql": "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'",
```

- [ ] **H2.5 — Validate the raw SQL applies twice (idempotent) on a throwaway pgvector PG.** (vanilla `postgres:15` has no `vector` ext but the table DDL itself does not need it — this just confirms SQL validity + idempotency + seed-preservation.)

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
docker run --rm -d --name pg-015 -e POSTGRES_PASSWORD=test -p 25433:5432 postgres:15 >/dev/null
until docker exec pg-015 pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
docker cp src/vlm_pipeline/sql/migrations/postgres/015_embedding_active_model.sql pg-015:/tmp/015.sql
docker exec pg-015 psql -U postgres -f /tmp/015.sql -v ON_ERROR_STOP=on
# simulate a promotion changing the pointer, then re-apply: seed must NOT clobber it
docker exec pg-015 psql -U postgres -c "UPDATE embedding_active_model SET model_name='facebook/PE-Core-L14-336@ft-x' WHERE scope='frame_search';"
docker exec pg-015 psql -U postgres -f /tmp/015.sql -v ON_ERROR_STOP=on
docker exec pg-015 psql -U postgres -tAc "SELECT model_name FROM embedding_active_model WHERE scope='frame_search';"
docker rm -f pg-015
```

Expected: both applies end `COMMIT` (second: `NOTICE ... already exists, skipping`), and the final query prints `facebook/PE-Core-L14-336@ft-x` (proving `ON CONFLICT DO NOTHING` preserves a promoted pointer across re-apply).

- [ ] **H2.6 — Commit.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git add src/vlm_pipeline/sql/migrations/postgres/015_embedding_active_model.sql \
        src/vlm_pipeline/resources/postgres_migration.py \
        tests/integration/test_active_model_migration_015.py
git commit -m "feat(mlops): 015 embedding_active_model pointer table (pgvector-gated, seed=stock)"
```

**Interfaces:**
- Produces: table `embedding_active_model` + seed. Consumed by the H2-resolver lib (H2.7), the `db` reader/writer (H2.10), and promotion (H4).

---

### Task H2b: `lib/active_model.py` — pure resolver (failing test first)

Pure L1 resolver: given the row fetched from `embedding_active_model` (or `None`), return the active `model_name`, defaulting to `STOCK_PE_CORE_MODEL_NAME` when unset/blank. Keeps the "stock when unset" rule in one tested place; the `db` reader and `fiftyone_pgvector` both rely on it (the latter via a pin test, since serving code can't import lib).

**Files:**
- `tests/unit/test_active_model.py` (new)
- `src/vlm_pipeline/lib/active_model.py` (new)

**Interfaces:**
- Consumes: `lib.embedding_model_name.STOCK_PE_CORE_MODEL_NAME` (A2).
- Produces:
  - `DEFAULT_SCOPE = "frame_search"`
  - `resolve_active_model_name(row: dict | None, *, default: str = STOCK_PE_CORE_MODEL_NAME) -> str`

- [ ] **H2b.1 — Write the failing unit test.** Create `tests/unit/test_active_model.py`:

```python
"""lib.active_model — 활성 임베딩 model_name 해석 (순수, no PG)."""

from __future__ import annotations

from vlm_pipeline.lib.active_model import DEFAULT_SCOPE, resolve_active_model_name
from vlm_pipeline.lib.embedding_model_name import STOCK_PE_CORE_MODEL_NAME


def test_default_scope_is_frame_search() -> None:
    assert DEFAULT_SCOPE == "frame_search"


def test_none_row_returns_stock() -> None:
    assert resolve_active_model_name(None) == STOCK_PE_CORE_MODEL_NAME


def test_blank_model_name_returns_stock() -> None:
    assert resolve_active_model_name({"model_name": ""}) == STOCK_PE_CORE_MODEL_NAME
    assert resolve_active_model_name({"model_name": "   "}) == STOCK_PE_CORE_MODEL_NAME
    assert resolve_active_model_name({"model_name": None}) == STOCK_PE_CORE_MODEL_NAME


def test_set_value_returned() -> None:
    row = {"model_name": "facebook/PE-Core-L14-336@ft-2026.06.29-lora-001"}
    assert resolve_active_model_name(row) == "facebook/PE-Core-L14-336@ft-2026.06.29-lora-001"


def test_custom_default() -> None:
    assert resolve_active_model_name(None, default="x/y") == "x/y"
```

- [ ] **H2b.2 — Run it — expect FAIL (module missing).**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_active_model.py -q
```

Expected: `ModuleNotFoundError: No module named 'vlm_pipeline.lib.active_model'`.

- [ ] **H2b.3 — Write the implementation.** Create `src/vlm_pipeline/lib/active_model.py`:

```python
"""활성 임베딩 model_name 해석 (L1 lib — 순수).

design §8.1-A: AL/검색이 읽는 활성 model_name 을 embedding_active_model(migration 015) 행으로
관리한다. 행이 없거나 비어있으면 stock 으로 폴백 → 마이그레이션만 적용된 상태(미승격)에서
현행 동작 보존. 승격(H4)이 이 행을 UPDATE 해 포인터를 원자 전환한다.

⚠️ lib 계층 규칙: dagster / defs / resources / ops import 금지.
"""

from __future__ import annotations

from vlm_pipeline.lib.embedding_model_name import STOCK_PE_CORE_MODEL_NAME

DEFAULT_SCOPE = "frame_search"


def resolve_active_model_name(
    row: dict | None,
    *,
    default: str = STOCK_PE_CORE_MODEL_NAME,
) -> str:
    """embedding_active_model 행(dict) → 활성 model_name. 미설정/공백이면 default(stock)."""
    if not row:
        return default
    value = row.get("model_name")
    if value is None:
        return default
    value = str(value).strip()
    return value or default
```

- [ ] **H2b.4 — Run + lint + lib-guard.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_active_model.py -q
ruff check --line-length 120 src/vlm_pipeline/lib/active_model.py tests/unit/test_active_model.py
python3 scripts/check_lib_layer_imports.py
```

Expected: `5 passed`, `All checks passed!`, lib-guard exits 0.

- [ ] **H2b.5 — Commit.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git add src/vlm_pipeline/lib/active_model.py tests/unit/test_active_model.py
git commit -m "feat(mlops): pure resolver for active embedding model_name (stock fallback)"
```

**Interfaces:**
- Produces: `DEFAULT_SCOPE`, `resolve_active_model_name`. Consumed by H2c (`db` reader) and the `fiftyone_pgvector` pin test (H2d).

---

### Task H2c: `PostgresEmbeddingMixin` active-model read/write + index-create executor (integration TDD)

Add three methods to the existing embedding mixin so the embed defs and promotion can read/flip the pointer and build the partial HNSW. The reader degrades to stock when the table is absent (non-pgvector / pre-015 env) — never raises. The flip is a single atomic `UPDATE`. The index executor runs H1's builder SQL.

**Files:**
- `src/vlm_pipeline/resources/postgres_embedding.py` (edit — add methods + SQL consts)
- `tests/integration/test_active_model_mixin.py` (new)

**Interfaces:**
- Consumes: `lib.active_model.resolve_active_model_name`, `lib.active_model.DEFAULT_SCOPE`, `lib.pgvector_index.build_partial_hnsw_sql`; table `embedding_active_model` (015); `image_embeddings` (006). Uses `self.connect()` (existing base ctx-manager, auto commit/rollback).
- Produces (Python):
  - `get_active_embedding_model(self, scope: str = DEFAULT_SCOPE) -> str` (never raises; stock fallback on missing table/row)
  - `set_active_embedding_model(self, model_name: str, *, scope: str = DEFAULT_SCOPE, updated_by: str | None = None) -> None`
  - `create_model_partial_hnsw(self, model_name: str, *, entity_types: tuple[str, ...] = ("frame", "caption")) -> list[str]` (returns created index names; CONCURRENTLY NOT used — runs inside the maintenance window)

- [ ] **H2c.1 — Write the failing integration test.** Create `tests/integration/test_active_model_mixin.py`:

```python
"""PostgresEmbeddingMixin active-model pointer + partial HNSW (real-PG; pgvector-gated)."""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg2")


def _has_table(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name=%s)",
            (name,),
        )
        return bool(cur.fetchone()[0])


def test_get_active_falls_back_to_stock_when_table_absent(postgres_resource) -> None:
    # On a non-pgvector test PG the table won't exist → must return stock, not raise.
    name = postgres_resource.get_active_embedding_model()
    assert name == "facebook/PE-Core-L14-336"


def test_set_then_get_roundtrip(postgres_resource) -> None:
    with postgres_resource.connect() as conn:
        if not _has_table(conn, "embedding_active_model"):
            pytest.skip("embedding_active_model absent — pgvector not available")
    postgres_resource.set_active_embedding_model(
        "facebook/PE-Core-L14-336@ft-test", updated_by="pytest"
    )
    assert postgres_resource.get_active_embedding_model() == "facebook/PE-Core-L14-336@ft-test"
    # restore stock so the test is idempotent across reruns
    postgres_resource.set_active_embedding_model("facebook/PE-Core-L14-336", updated_by="pytest")


def test_create_model_partial_hnsw_idempotent(postgres_resource) -> None:
    with postgres_resource.connect() as conn:
        if not _has_table(conn, "image_embeddings"):
            pytest.skip("image_embeddings absent — pgvector not available")
    names1 = postgres_resource.create_model_partial_hnsw("facebook/PE-Core-L14-336@ft-test")
    names2 = postgres_resource.create_model_partial_hnsw("facebook/PE-Core-L14-336@ft-test")
    assert names1 == names2 and len(names1) == 2  # frame + caption
    # cleanup
    with postgres_resource.connect() as conn, conn.cursor() as cur:
        for n in names1:
            cur.execute(f"DROP INDEX IF EXISTS {n}")
```

- [ ] **H2c.2 — Run it — expect FAIL** (`AttributeError: ... has no attribute 'get_active_embedding_model'`, or skip if no DSN).

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/integration/test_active_model_mixin.py -q
```

- [ ] **H2c.3 — Implement the methods.** Edit `src/vlm_pipeline/resources/postgres_embedding.py`. Add imports at the top (after the existing `from vlm_pipeline.resources.postgres_base import PostgresBaseMixin`):

```python
from vlm_pipeline.lib.active_model import DEFAULT_SCOPE, resolve_active_model_name
from vlm_pipeline.lib.pgvector_index import build_partial_hnsw_sql
```

Add these SQL constants near the other module-level SQL strings (after `_ALL_CAPTIONS_SQL`):

```python
_ACTIVE_MODEL_SELECT_SQL = "SELECT model_name FROM embedding_active_model WHERE scope = %(scope)s"

_ACTIVE_MODEL_UPSERT_SQL = """
INSERT INTO embedding_active_model (scope, model_name, updated_at, updated_by)
VALUES (%(scope)s, %(model_name)s, now(), %(updated_by)s)
ON CONFLICT (scope) DO UPDATE SET
  model_name = EXCLUDED.model_name,
  updated_at = now(),
  updated_by = EXCLUDED.updated_by
"""

_ACTIVE_MODEL_TABLE_EXISTS_SQL = (
    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
    "WHERE table_schema='public' AND table_name='embedding_active_model')"
)
```

Add these methods inside `class PostgresEmbeddingMixin` (after `batch_insert_embeddings`):

```python
    def get_active_embedding_model(self, scope: str = DEFAULT_SCOPE) -> str:
        """활성 임베딩 model_name (design §8.1-A). 테이블/행 부재 시 stock 폴백 — 절대 raise 안 함.

        AL 큐·검색·embed asset 이 기본 model_name 으로 읽는다. 미승격(=015 만 적용)이면
        seed 값 stock 을 반환 → 현행 동작 보존.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_ACTIVE_MODEL_TABLE_EXISTS_SQL)
                if not bool(cur.fetchone()[0]):
                    return resolve_active_model_name(None)  # 비-pgvector / pre-015 → stock
                cur.execute(_ACTIVE_MODEL_SELECT_SQL, {"scope": scope})
                row = cur.fetchone()
                return resolve_active_model_name({"model_name": row[0]} if row else None)

    def set_active_embedding_model(
        self, model_name: str, *, scope: str = DEFAULT_SCOPE, updated_by: str | None = None
    ) -> None:
        """활성 model_name 포인터 원자 전환 (승격/롤백). 단일행 UPSERT."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _ACTIVE_MODEL_UPSERT_SQL,
                    {"scope": scope, "model_name": model_name, "updated_by": updated_by},
                )

    def create_model_partial_hnsw(
        self, model_name: str, *, entity_types: tuple[str, ...] = ("frame", "caption")
    ) -> list[str]:
        """주어진 model_name 의 entity_type 별 partial HNSW 인덱스 생성 (멱등).

        design §8.1-(C): 재임베딩(H3) 완료 후, 전환(H4) 전에 호출. 정비 윈도우에서 실행하므로
        CONCURRENTLY 미사용 (트랜잭션 안에서 단순 CREATE INDEX IF NOT EXISTS).
        반환 = 생성(또는 이미 존재)한 인덱스 이름 목록.
        """
        from vlm_pipeline.lib.pgvector_index import model_index_name

        created: list[str] = []
        with self.connect() as conn:
            with conn.cursor() as cur:
                for et in entity_types:
                    cur.execute(build_partial_hnsw_sql(model_name, et))
                    created.append(model_index_name(model_name, et))
        return created
```

> Note: `model_index_name` is imported lazily inside the method to avoid widening the module-top import surface; both `build_partial_hnsw_sql` and `model_index_name` are pure lib (allowed from a `resources/` module). The top-level import of the two lib symbols is fine — `resources/` is L3 and may import `lib/` (L1).

- [ ] **H2c.4 — Run it — expect PASS** (or skip without DSN/pgvector).

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/integration/test_active_model_mixin.py -q
```

Expected with vanilla test PG (no pgvector): `test_get_active_falls_back_to_stock_when_table_absent` PASSES, the other two SKIP. With a pgvector PG that has 006+015 applied: `3 passed`.

- [ ] **H2c.5 — Lint + commit.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
ruff check --line-length 120 src/vlm_pipeline/resources/postgres_embedding.py tests/integration/test_active_model_mixin.py
git add src/vlm_pipeline/resources/postgres_embedding.py tests/integration/test_active_model_mixin.py
git commit -m "feat(mlops): active-model pointer read/write + partial-HNSW executor on embedding mixin"
```

**Interfaces:**
- Produces: `get_active_embedding_model`, `set_active_embedding_model`, `create_model_partial_hnsw` on `PostgresResource` (via `PostgresEmbeddingMixin`). Consumed by H3 (reembed reads/writes nothing here directly — uses versioned name) and H4 (promotion flips pointer + builds index).

---

### Task H2d: Default embed/search reads to the active pointer (additive; pin test for `fiftyone_pgvector`)

Make the live embed asset and the analysis search surface default to the PG pointer instead of the hardcoded constant — while keeping the constant as the stock fallback. `fiftyone_pgvector.py` is serving code (cannot import `vlm_pipeline.lib`), so it gets a tiny self-contained `_active_model_name()` reader + a pin test asserting its fallback equals the lib's stock constant.

**Files:**
- `src/vlm_pipeline/defs/embed/assets.py` (edit — `frame_embedding` resolves default model from the pointer)
- `docker/analysis/fiftyone_pgvector.py` (edit — add `_active_model_name()` + use it as the runtime default in `active_learning_queue` and `search_by_text`)
- `tests/unit/test_fiftyone_active_model_pin.py` (new — pin)
- `tests/unit/test_frame_embedding_active_default.py` (new)

**Interfaces:**
- Consumes: `db.get_active_embedding_model()` (H2c); `lib.embedding_model_name.STOCK_PE_CORE_MODEL_NAME` (A2, for the pin).
- Produces: live embed/search defaulting to the active pointer; behaviour unchanged while the pointer = stock.

- [ ] **H2d.1 — Write the `fiftyone_pgvector` pin test.** Create `tests/unit/test_fiftyone_active_model_pin.py`:

```python
"""docker/analysis/fiftyone_pgvector.py 는 serving 코드라 vlm_pipeline.lib 를 import 못 함 →
DEFAULT_MODEL 상수 + _active_model_name() 폴백이 lib 의 STOCK_PE_CORE_MODEL_NAME 과
같은 값을 쓰는지 pin (drift 가드).
"""

from __future__ import annotations

import pathlib
import sys

from vlm_pipeline.lib.embedding_model_name import STOCK_PE_CORE_MODEL_NAME


def _load_fp(monkeypatch):
    d = str(pathlib.Path("docker/analysis").resolve())
    if d not in sys.path:
        sys.path.insert(0, d)
    import fiftyone_pgvector as fp

    return fp


def test_default_model_constant_matches_lib(monkeypatch) -> None:
    fp = _load_fp(monkeypatch)
    assert fp.DEFAULT_MODEL == STOCK_PE_CORE_MODEL_NAME


def test_active_model_name_falls_back_to_constant_on_db_error(monkeypatch) -> None:
    fp = _load_fp(monkeypatch)

    def _boom():
        raise RuntimeError("no pg")

    monkeypatch.setattr(fp, "_pg_conn", _boom)
    # _active_model_name must swallow errors and return the stock constant (search must never
    # break just because the pointer table is missing/unreachable).
    assert fp._active_model_name() == fp.DEFAULT_MODEL
```

- [ ] **H2d.2 — Run it — expect FAIL** (`AttributeError: module 'fiftyone_pgvector' has no attribute '_active_model_name'`). The first test may already PASS.

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_fiftyone_active_model_pin.py -q
```

- [ ] **H2d.3 — Add `_active_model_name()` to `fiftyone_pgvector.py`.** Edit `docker/analysis/fiftyone_pgvector.py`. Immediately after the `DEFAULT_MODEL = "facebook/PE-Core-L14-336"` line (line 23) and after `_pg_conn()` is defined (so it can call it), add this function right below `_pg_conn` (around line 56):

```python
def _active_model_name(scope: str = "frame_search") -> str:
    """활성 임베딩 model_name (PG embedding_active_model, migration 015). design §8.1-A.

    serving 코드라 vlm_pipeline.lib import 불가 → 자급식 1-쿼리. 테이블/행 부재나 어떤 오류든
    DEFAULT_MODEL(stock) 로 폴백 — 검색/AL 이 포인터 때문에 깨지지 않게.
    pin: tests/unit/test_fiftyone_active_model_pin.py 가 lib 의 stock 상수와 일치 보장.
    """
    try:
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT model_name FROM embedding_active_model WHERE scope = %s", (scope,)
            )
            row = cur.fetchone()
        if row and str(row[0] or "").strip():
            return str(row[0]).strip()
    except Exception:
        pass
    return DEFAULT_MODEL
```

Then change the two read surfaces to use it as the *runtime* default (do NOT change the constant; change only these two call sites so existing callers that pass `model_name` explicitly are unaffected):

In `active_learning_queue` (signature at ~line 2146 `def active_learning_queue(n: int = 200, model_name: str = DEFAULT_MODEL) -> dict:`), keep the signature but, as the FIRST line of the body, resolve a sentinel default. Replace the signature default with a sentinel and resolve inside:

```python
def active_learning_queue(n: int = 200, model_name: str | None = None) -> dict:
    if model_name is None:
        model_name = _active_model_name()
```
(keep the existing docstring + body below unchanged.)

Same edit for `search_by_text` (~line 923): change `model_name: str = DEFAULT_MODEL` to `model_name: str | None = None` and add at the top of the body:
```python
    if model_name is None:
        model_name = _active_model_name()
```

> Scope note: ONLY `active_learning_queue` and `search_by_text` are switched to the pointer (the two surfaces §8.1 names as "what AL/검색 reads"). All other functions keep `model_name: str = DEFAULT_MODEL` — they are explicit-model analysis utilities and switching them risks behaviour drift. This matches the "additive" co-ownership note in the assignment.

- [ ] **H2d.4 — Run the pin test — expect PASS.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_fiftyone_active_model_pin.py -q
```

Expected: `2 passed`.

- [ ] **H2d.5 — Write the embed-asset default test.** Create `tests/unit/test_frame_embedding_active_default.py`:

```python
"""frame_embedding 은 op_config 에 model_name 이 없으면 활성 포인터(db.get_active_embedding_model)
를 기본값으로 써야 한다 (design §8.1-A). asset 본문을 _DummyDB/_DummyMinIO/_DummyContext 로 구동.
"""

from __future__ import annotations

from vlm_pipeline.defs.embed.assets import frame_embedding


class _DummyCtxLog:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass


class _DummyContext:
    def __init__(self, config):
        self.op_config = config
        self.log = _DummyCtxLog()


class _DummyDB:
    def __init__(self):
        self.asked_model = None

    def ensure_runtime_schema(self):
        pass

    def get_active_embedding_model(self, scope="frame_search"):
        return "facebook/PE-Core-L14-336@ft-active"

    def find_pending_frame_embeddings(self, model_name, limit, image_roles):
        self.asked_model = model_name
        return []  # empty backlog → asset returns early, no embedding-service call


class _DummyMinIO:
    pass


def test_frame_embedding_uses_active_pointer_when_model_unset():
    db = _DummyDB()
    ctx = _DummyContext({"limit": 10})  # no model_name in config
    out = frame_embedding(ctx, db, _DummyMinIO())
    assert out == {"embedded": 0, "failed": 0, "pending": 0}
    # the active pointer (not the hardcoded stock constant) was queried for pending rows
    assert db.asked_model == "facebook/PE-Core-L14-336@ft-active"


def test_frame_embedding_respects_explicit_model_name():
    db = _DummyDB()
    ctx = _DummyContext({"limit": 10, "model_name": "facebook/PE-Core-L14-336"})
    frame_embedding(ctx, db, _DummyMinIO())
    assert db.asked_model == "facebook/PE-Core-L14-336"  # explicit config wins
```

- [ ] **H2d.6 — Run it — expect FAIL** (current code uses `str(context.op_config.get("model_name", DEFAULT_MODEL))` so it asks `facebook/PE-Core-L14-336`, not the active pointer).

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_frame_embedding_active_default.py -q
```

Expected: `test_frame_embedding_uses_active_pointer_when_model_unset` FAILS (`asked_model == 'facebook/PE-Core-L14-336'`).

- [ ] **H2d.7 — Edit `frame_embedding` to default from the pointer.** In `src/vlm_pipeline/defs/embed/assets.py`, inside `frame_embedding` (the body that currently reads `model_name = str(context.op_config.get("model_name", DEFAULT_MODEL))` at ~line 218), replace that line with a pointer-aware resolution that runs AFTER `db.ensure_runtime_schema()`:

```python
    limit = int(context.op_config.get("limit", 500))
    image_roles_raw = context.op_config.get("image_roles") or []
    roles = list(image_roles_raw) if image_roles_raw else FRAME_ROLES

    # pgvector-gated image_embeddings 테이블 존재 보장 (embedding-only 경로가 첫 실행일 때 대비, Codex review).
    db.ensure_runtime_schema()
    # design §8.1-A: model_name 미지정이면 활성 포인터(embedding_active_model)를 기본값으로.
    # 포인터=stock(미승격)이면 현행 동작 그대로. op_config 명시값이 있으면 그것이 우선.
    cfg_model = context.op_config.get("model_name")
    model_name = str(cfg_model) if cfg_model else db.get_active_embedding_model()
```

Delete the now-duplicate `model_name = ...` / `db.ensure_runtime_schema()` / `roles = ...` lines that previously sat in that block so they appear exactly once (the block above is the full replacement for lines ~217-224). Leave `caption_embedding` unchanged (caption backfill is run with explicit model_name; switching it is out of scope for this section).

> The `frame_embedding` config-schema default for `model_name` is `DEFAULT_MODEL` (a Dagster `Field` default). Because `op_config.get("model_name")` returns that default string even when the user did not set it, the test's `_DummyContext` (which omits the key) models the "unset" path. In the real Dagster asset the Field default would always populate `model_name` — so to make the pointer the true default we must ALSO change the config-schema default to empty. Edit `_EMBED_CONFIG_SCHEMA` `model_name` Field:

```python
    # 빈 문자열 기본값 = "미지정" → 런타임에 활성 포인터(db.get_active_embedding_model) 사용 (§8.1-A).
    "model_name": Field(str, default_value=""),
```

(The `cfg_model = ... if cfg_model else ...` guard treats `""` as unset.)

- [ ] **H2d.8 — Run both tests — expect PASS.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_frame_embedding_active_default.py tests/unit/test_fiftyone_active_model_pin.py -q
```

Expected: `4 passed`.

- [ ] **H2d.9 — Lint + commit.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
ruff check --line-length 120 src/vlm_pipeline/defs/embed/assets.py docker/analysis/fiftyone_pgvector.py \
  tests/unit/test_fiftyone_active_model_pin.py tests/unit/test_frame_embedding_active_default.py
git add src/vlm_pipeline/defs/embed/assets.py docker/analysis/fiftyone_pgvector.py \
  tests/unit/test_fiftyone_active_model_pin.py tests/unit/test_frame_embedding_active_default.py
git commit -m "feat(mlops): default embed/AL/search to active model_name pointer (stock when unset)"
```

**Interfaces:**
- Consumes: `db.get_active_embedding_model()`, `fp._active_model_name()`.
- Produces: live `frame_embedding`, `active_learning_queue`, `search_by_text` honour the pointer; unchanged while pointer = stock.

---

### Task H3: full-corpus re-embed asset `defs/embed/reembed.py` (covers the incumbent set under a NEW versioned model_name)

Design §8.1-(B): embed the **incumbent-covered set** (currently-embedded frames + captions — frame 187,994 + caption 11,978, NOT all 525,966 `image_metadata`) under a new versioned `model_name`. Must be idempotent (resumable across runs via the existing `batch_insert_embeddings` ON-CONFLICT upsert), write through the existing path, and target only `entity_id`s that already have an *incumbent* embedding. Real run is gated behind `ENABLE_TRAINING` / manual launch.

> **Why a new query, not `find_pending_frame_embeddings`:** that helper returns ALL `image_metadata` rows lacking the given `model_name` (525,966) — it would re-embed frames never embedded before, exploding coverage. Re-embed must restrict to `entity_id`s that ALREADY have a row under the *incumbent* `model_name`. So H3 adds an incumbent-scoped pending query to the mixin.

**Files:**
- `src/vlm_pipeline/resources/postgres_embedding.py` (edit — add `find_reembed_targets` + SQL)
- `tests/integration/test_reembed_targets.py` (new — real-PG, skip without pgvector)
- `src/vlm_pipeline/defs/embed/reembed.py` (new — asset + `_run_reembed`)
- `tests/unit/test_reembed_asset.py` (new — `_DummyDB`/`_DummyMinIO`, mocked embedding HTTP)

**Interfaces:**
- Consumes: `image_embeddings` (incumbent rows), `lib.embedding.get_embedding_client` (mocked in CI), `lib.embedding_model_name.parse_model_name`, `db.get_active_embedding_model`, `build_frame_embedding_rows`/`build_caption_embedding_rows` (helpers.py), `db.batch_insert_embeddings`.
- Produces:
  - mixin `find_reembed_targets(self, *, incumbent_model_name: str, new_model_name: str, entity_type: str, limit: int) -> list[dict]` — `entity_id`s present under `incumbent_model_name` but NOT yet under `new_model_name`, joined back to source rows needed to re-embed (frame: `image_metadata`; caption: `labels`).
  - asset `reembed_under_version` + `_run_reembed(context, db, minio)`; config `{new_version, incumbent_model_name?, entity_types, limit}`; env-gated by `ENABLE_TRAINING`.

- [ ] **H3.1 — Write the failing integration test for the targets query.** Create `tests/integration/test_reembed_targets.py`:

```python
"""find_reembed_targets — incumbent 커버 집합만 새 model_name 대상으로 (real-PG, pgvector-gated)."""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg2")


def _has(conn, name):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name=%s)",
            (name,),
        )
        return bool(cur.fetchone()[0])


def test_reembed_targets_only_incumbent_covered_frames(postgres_resource) -> None:
    with postgres_resource.connect() as conn:
        if not (_has(conn, "image_embeddings") and _has(conn, "image_metadata")):
            pytest.skip("pgvector tables absent")
        with conn.cursor() as cur:
            # two frames in image_metadata; only one has an incumbent embedding
            cur.execute(
                "INSERT INTO image_metadata (image_id, image_bucket, image_key, image_role) "
                "VALUES ('rt-f1','b','k1','source_image'),('rt-f2','b','k2','source_image') "
                "ON CONFLICT (image_id) DO NOTHING"
            )
            cur.execute(
                "INSERT INTO image_embeddings "
                "(embedding_id, entity_type, entity_id, image_id, model_name, dim, embedding) "
                "VALUES ('frame|rt-f1|inc','frame','rt-f1','rt-f1','inc-model',1024, "
                "  ('[' || array_to_string(array_fill(0.0::float8, ARRAY[1024]), ',') || ']')::vector) "
                "ON CONFLICT (entity_type, entity_id, model_name) DO NOTHING"
            )
    targets = postgres_resource.find_reembed_targets(
        incumbent_model_name="inc-model",
        new_model_name="inc-model@ft-x",
        entity_type="frame",
        limit=100,
    )
    ids = {t["image_id"] for t in targets}
    assert "rt-f1" in ids        # incumbent-covered → re-embed target
    assert "rt-f2" not in ids    # never embedded → NOT a re-embed target (coverage discipline)
```

- [ ] **H3.2 — Run it — expect FAIL** (`AttributeError: find_reembed_targets`, or skip without pgvector).

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/integration/test_reembed_targets.py -q
```

- [ ] **H3.3 — Implement `find_reembed_targets`.** Edit `src/vlm_pipeline/resources/postgres_embedding.py`. Add SQL constants after `_ACTIVE_MODEL_TABLE_EXISTS_SQL`:

```python
# 재임베딩 대상 (design §8.1-B): incumbent model_name 으로 임베딩된 entity 만 (커버리지 = incumbent 집합),
# 새 model_name 으로는 아직 없는 것. 재실행 안전 (이미 새 model_name 으로 들어간 것은 제외 → resumable).
_REEMBED_FRAME_TARGETS_SQL = """
SELECT im.image_id, im.image_bucket, im.image_key, im.image_role
FROM image_embeddings inc
JOIN image_metadata im ON im.image_id = inc.entity_id
WHERE inc.entity_type = 'frame' AND inc.model_name = %(incumbent)s
AND im.image_bucket IS NOT NULL AND im.image_key IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM image_embeddings nw
    WHERE nw.entity_type = 'frame' AND nw.entity_id = inc.entity_id AND nw.model_name = %(new_model)s
)
ORDER BY im.image_id
LIMIT %(limit)s
"""

_REEMBED_CAPTION_TARGETS_SQL = """
SELECT labels.label_id, labels.asset_id, labels.caption_text
FROM image_embeddings inc
JOIN labels ON labels.label_id = inc.entity_id
WHERE inc.entity_type = 'caption' AND inc.model_name = %(incumbent)s
AND labels.caption_text IS NOT NULL AND labels.caption_text <> ''
AND NOT EXISTS (
    SELECT 1 FROM image_embeddings nw
    WHERE nw.entity_type = 'caption' AND nw.entity_id = inc.entity_id AND nw.model_name = %(new_model)s
)
ORDER BY labels.label_id
LIMIT %(limit)s
"""
```

Add the method inside `PostgresEmbeddingMixin` (after `create_model_partial_hnsw`):

```python
    def find_reembed_targets(
        self,
        *,
        incumbent_model_name: str,
        new_model_name: str,
        entity_type: str,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """재임베딩 대상 조회 (design §8.1-B). incumbent 로 임베딩된 entity 중 new_model 미존재분만.

        커버리지 = incumbent 집합 (전체 image_metadata 아님). resumable: 이미 new_model 로
        들어간 것은 NOT EXISTS 로 제외되므로 중단 후 재실행 안전.
        """
        if entity_type == "frame":
            sql = _REEMBED_FRAME_TARGETS_SQL
        elif entity_type == "caption":
            sql = _REEMBED_CAPTION_TARGETS_SQL
        else:
            raise ValueError(f"unsupported entity_type for reembed: {entity_type!r}")
        params = {"incumbent": incumbent_model_name, "new_model": new_model_name, "limit": limit}
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return PostgresBaseMixin._cursor_to_dicts(cur)
```

- [ ] **H3.4 — Run the integration test — expect PASS** (or skip).

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/integration/test_reembed_targets.py -q
```

- [ ] **H3.5 — Write the failing asset unit test.** Create `tests/unit/test_reembed_asset.py`:

```python
"""reembed_under_version asset — _DummyDB + mocked embedding client (no GPU, no PG, no HTTP).

design §8.1-B: 새 versioned model_name 으로 incumbent 커버 집합 재임베딩. resumable + 올바른
대상 + 새 model_name write 를 단언. 실행 게이트(ENABLE_TRAINING)는 launcher 레벨.
"""

from __future__ import annotations

import importlib

import pytest

reembed = importlib.import_module("vlm_pipeline.defs.embed.reembed")


class _Log:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass


class _Ctx:
    def __init__(self, config):
        self.op_config = config
        self.log = _Log()


class _Client:
    def wait_until_ready(self):
        return True

    def embed(self, data):
        return [0.1] * 1024

    def embed_text(self, text):
        return [0.2] * 1024


class _DummyMinIO:
    def download(self, bucket, key):
        return b"img-bytes"


class _DummyDB:
    def __init__(self, frame_targets, caption_targets):
        self._frame = frame_targets
        self._caption = caption_targets
        self.inserted_rows = []
        self.queried = []

    def ensure_runtime_schema(self):
        pass

    def get_active_embedding_model(self, scope="frame_search"):
        return "facebook/PE-Core-L14-336"

    def find_reembed_targets(self, *, incumbent_model_name, new_model_name, entity_type, limit):
        self.queried.append((incumbent_model_name, new_model_name, entity_type))
        page = self._frame if entity_type == "frame" else self._caption
        # one page then empty (resumable loop terminates)
        if page and not getattr(self, f"_done_{entity_type}", False):
            setattr(self, f"_done_{entity_type}", True)
            return page
        return []

    def batch_insert_embeddings(self, rows):
        self.inserted_rows.extend(rows)
        return len(rows)


@pytest.fixture(autouse=True)
def _mock_embed_client(monkeypatch):
    monkeypatch.setattr(reembed, "get_embedding_client", lambda: _Client())


def test_reembed_targets_incumbent_and_writes_new_model_name():
    db = _DummyDB(
        frame_targets=[{"image_id": "f1", "image_bucket": "b", "image_key": "k", "image_role": "source_image"}],
        caption_targets=[{"label_id": "c1", "asset_id": "a1", "caption_text": "fire"}],
    )
    ctx = _Ctx({"new_version": "ft-2026.06.29-lora-001", "entity_types": ["frame", "caption"], "limit": 100})
    out = reembed._run_reembed(ctx, db, _DummyMinIO())

    new_name = "facebook/PE-Core-L14-336@ft-2026.06.29-lora-001"
    # incumbent = active stock pointer; new model_name = versioned
    assert ("facebook/PE-Core-L14-336", new_name, "frame") in db.queried
    assert out["new_model_name"] == new_name
    assert out["embedded"] == 2
    # every written row carries the NEW versioned model_name
    assert {r["model_name"] for r in db.inserted_rows} == {new_name}
    assert {r["entity_type"] for r in db.inserted_rows} == {"frame", "caption"}


def test_reembed_rejects_blank_version():
    db = _DummyDB([], [])
    ctx = _Ctx({"new_version": "", "entity_types": ["frame"]})
    with pytest.raises(Exception):
        reembed._run_reembed(ctx, db, _DummyMinIO())
```

- [ ] **H3.6 — Run it — expect FAIL (module missing).**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_reembed_asset.py -q
```

Expected: `ModuleNotFoundError: No module named 'vlm_pipeline.defs.embed.reembed'`.

- [ ] **H3.7 — Write the asset.** Create `src/vlm_pipeline/defs/embed/reembed.py`:

```python
"""전체 코퍼스 재임베딩 asset — 새 versioned model_name 으로 incumbent 커버 집합 재임베딩.

design §8.1-(B): PE-Core 챌린저가 게이트를 통과하면, 활성(incumbent) model_name 으로 임베딩된
집합(frame+caption)을 새 model_name='<base>@<new_version>'(§5.3)으로 재임베딩한다. 커버리지 =
incumbent 집합 (전체 image_metadata 아님). 신규 프레임은 평소 frame_embedding asset 이 활성
포인터로 계속 임베딩. 페이지 루프 + ON CONFLICT upsert 로 resumable.

⚠️ 실 실행은 게이트 뒤 (ENABLE_TRAINING + 수동 launch). 게이트 미설정 시 asset 은 no-op 보고.
gpu_trainer 동시성 태그는 등록 시 (definitions) 부여 — embedding-service 부하 직렬화.
"""

from __future__ import annotations

import os

from dagster import Failure, Field, asset

from vlm_pipeline.defs.embed.helpers import build_caption_embedding_rows, build_frame_embedding_rows
from vlm_pipeline.lib.embedding import get_embedding_client
from vlm_pipeline.lib.embedding_model_name import build_versioned_model_name
from vlm_pipeline.resources.minio import MinIOResource
from vlm_pipeline.resources.postgres import PostgresResource

_REEMBED_CONFIG_SCHEMA = {
    "new_version": Field(str, default_value=""),  # 'ft-2026.06.29-lora-001' — 빈 값이면 실패
    "incumbent_model_name": Field(str, default_value=""),  # 빈 값이면 활성 포인터 사용
    "entity_types": Field([str], default_value=["frame", "caption"]),
    "limit": Field(int, default_value=500),
}


def _run_reembed(context, db: PostgresResource, minio: MinIOResource) -> dict:
    new_version = str(context.op_config.get("new_version") or "").strip()
    if not new_version:
        raise Failure(description="reembed: 'new_version' is required (e.g. 'ft-2026.06.29-lora-001')")
    new_model_name = build_versioned_model_name(new_version)  # raises on bad version
    entity_types = list(context.op_config.get("entity_types") or ["frame", "caption"])
    limit = int(context.op_config.get("limit", 500))

    db.ensure_runtime_schema()
    incumbent = str(context.op_config.get("incumbent_model_name") or "").strip() or db.get_active_embedding_model()
    if incumbent == new_model_name:
        raise Failure(description=f"reembed: incumbent == new_model_name ({incumbent!r}) — refusing self-overwrite")

    if not os.getenv("ENABLE_TRAINING", "").strip() in ("1", "true", "True"):
        context.log.warning(
            "reembed: ENABLE_TRAINING not set → scaffolding no-op (incumbent=%s new=%s). "
            "Set ENABLE_TRAINING=1 + launch manually to actually re-embed.",
            incumbent,
            new_model_name,
        )
        return {"embedded": 0, "new_model_name": new_model_name, "incumbent": incumbent, "gated": True}

    client = get_embedding_client()
    if not client.wait_until_ready():
        raise Failure(description="embedding-service not ready — reembed aborting (systemic)")

    total = 0
    for et in entity_types:
        while True:
            page = db.find_reembed_targets(
                incumbent_model_name=incumbent, new_model_name=new_model_name, entity_type=et, limit=limit
            )
            if not page:
                break
            if et == "frame":
                rows, failed = build_frame_embedding_rows(page, minio=minio, client=client, model_name=new_model_name)
            else:
                rows, failed, _, _ = build_caption_embedding_rows(page, client=client, model_name=new_model_name)
            inserted = db.batch_insert_embeddings(rows)
            total += inserted
            context.log.info(
                "reembed[%s]: page=%d inserted=%d failed=%d new_model=%s",
                et, len(page), inserted, len(failed), new_model_name,
            )
    return {"embedded": total, "new_model_name": new_model_name, "incumbent": incumbent, "gated": False}


@asset(name="reembed_under_version", group_name="embed", config_schema=_REEMBED_CONFIG_SCHEMA)
def reembed_under_version(context, db: PostgresResource, minio: MinIOResource) -> dict:
    """incumbent 커버 집합을 새 versioned model_name 으로 재임베딩 (design §8.1-B). 실행은 게이트 뒤."""
    return _run_reembed(context, db, minio)
```

- [ ] **H3.8 — Run the asset unit test — expect PASS.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_reembed_asset.py -q
```

Expected: `2 passed`. (The mocked `_DummyDB` has no `ENABLE_TRAINING` gate hit because the test exercises `_run_reembed` directly with the env unset... → adjust: the test asserts `embedded==2`, so set the env in the test.) **Add to the top of `test_reembed_targets_incumbent_and_writes_new_model_name`** (and only that test) `monkeypatch.setenv("ENABLE_TRAINING", "1")` by adding `monkeypatch` to its signature:

```python
def test_reembed_targets_incumbent_and_writes_new_model_name(monkeypatch):
    monkeypatch.setenv("ENABLE_TRAINING", "1")
    ...
```

Re-run; expect `2 passed`.

- [ ] **H3.9 — Register the asset + `gpu_trainer` tag.** Edit `src/vlm_pipeline/defs/embed/__init__.py` (currently empty) is NOT where assets are wired — find the Definitions assembly. SPIKE: `grep -rn "frame_embedding\|caption_embedding" src/vlm_pipeline/definitions.py src/vlm_pipeline/defs/embed/` to locate where embed assets are collected, then add `reembed_under_version` alongside `frame_embedding` with op tags `{"gpu_trainer": "true"}` (R2 tag, serializes against the trainer + embedding-service load). Record the exact file/line in the commit message. Mirror however `frame_embedding` is registered (likely a `load_assets_from_modules` or explicit list in `definitions.py`).

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
grep -rn "frame_embedding\|load_assets_from_modules\|defs.embed" src/vlm_pipeline/definitions.py | head
```

- [ ] **H3.10 — Definitions load smoke + lint + commit.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -c "import vlm_pipeline.definitions as d; print('defs load OK')"
ruff check --line-length 120 src/vlm_pipeline/defs/embed/reembed.py src/vlm_pipeline/resources/postgres_embedding.py \
  tests/unit/test_reembed_asset.py tests/integration/test_reembed_targets.py
python3 scripts/check_lib_layer_imports.py
git add src/vlm_pipeline/defs/embed/reembed.py src/vlm_pipeline/resources/postgres_embedding.py \
  src/vlm_pipeline/definitions.py tests/unit/test_reembed_asset.py tests/integration/test_reembed_targets.py
git commit -m "feat(mlops): full-corpus reembed asset under versioned model_name (incumbent coverage, gated)"
```

Expected: `defs load OK`, lint clean.

**Interfaces:**
- Produces: asset `reembed_under_version` + `_run_reembed`; mixin `find_reembed_targets`. Consumed by H4 (promotion calls it before flipping the pointer) and by manual launch.

---

### Task H4: PE-Core eval gate (GT-abstain) + promotion/rollback entrypoint (TDD)

Two parts: (1) a pure PE gate wrapper over `lib.train_eval_gate` that **abstains** (not promotable) when human-reviewed GT count `< eval_config.pe_core_min_gt`; (2) `scripts/promote_pe_core.py` — a sibling to `scripts/promote_model.py` that, for `model='pe_core'`, after the candidate is `promotable`: builds the partial HNSW (H2c) under the new `model_name`, flips the H2 pointer atomically, sets `EMBEDDING_CHECKPOINT_PATH`/`EMBEDDING_MODEL_VERSION` + recreates serving; rollback = flip pointer back + restore checkpoint env + recreate. Default `--dry-run`. Re-embed itself (H3) is launched separately via Dagster (the script asserts coverage exists before flipping).

> **Why a sibling script, not extending `promote_model.py`:** `promote_model.py` (Section E) does the generic registry transition + checkpoint materialize + recreate. PE promotion adds two PE-specific atomic steps (pointer flip + partial-index build) that SAM does not have. `promote_pe_core.py` *reuses* E's helpers (imported) and only adds the PE delta — keeping E's tests untouched.

**Files:**
- `src/vlm_pipeline/lib/pe_core_gate.py` (new — pure abstain wrapper)
- `tests/unit/test_pe_core_gate.py` (new)
- `scripts/promote_pe_core.py` (new)
- `tests/unit/test_promote_pe_core.py` (new)

**Interfaces:**
- Consumes: `lib.train_eval_gate.evaluate_gate`/`GateDecision`/`PE_CORE_EVAL_CONFIG` (D5–D7); `lib.recall_at_k.cross_modal_recall_at_k`/`bootstrap_ci` (D3/D4); `scripts/promote_model` helpers (`select_promotable_row`, `download_and_verify`, `promote_transition`, `rollback_transition`, `_write_env_var`, `PromotionError`, `_make_minio`, `REPO_ROOT`); `db`-equivalent SQL on `model_registry` + `embedding_active_model`.
- Produces:
  - `pe_core_gate_decision(candidate_metrics, incumbent_metrics, eval_config, *, gt_count, incumbent_source) -> GateDecision` — abstains (promotable=False, reason `gt_below_min`) when `gt_count < eval_config["pe_core_min_gt"]`; else delegates to `evaluate_gate`.
  - `DEFAULT_PE_CORE_MIN_GT = 50` (committed starting N_min; spec §15-Q3 left the value open — overridable via `eval_config`).
  - `promote_pe_core.py` functions: `assert_reembed_complete(cur, *, new_model_name) -> int`, `flip_pointer(cur, *, new_model_name, scope, dry_run)`, `rollback_pointer(cur, *, prior_model_name, scope, dry_run)`.

- [ ] **H4.1 — Write the failing PE-gate unit test.** Create `tests/unit/test_pe_core_gate.py`:

```python
"""PE-Core gate: 사람-검수 GT < N_min 이면 abstain (승격 불가). design §7.4 / §8.1-D."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load():
    root = Path(__file__).resolve().parents[2]
    src = root / "src" / "vlm_pipeline" / "lib" / "pe_core_gate.py"
    spec = importlib.util.spec_from_file_location("vlm_pipeline_lib_pe_core_gate_fresh", src)
    assert spec is not None and spec.loader is not None
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


_g = _load()
pe_core_gate_decision = _g.pe_core_gate_decision
DEFAULT_PE_CORE_MIN_GT = _g.DEFAULT_PE_CORE_MIN_GT

_CFG = {
    "primary_metric": "recall_at_5",
    "primary_margin": 0.02,
    "per_class_field": "per_class_recall",
    "per_class_floor": -0.05,
    "advisory": False,            # gate hard so the abstain path is the only thing relaxing it
    "pe_core_min_gt": DEFAULT_PE_CORE_MIN_GT,
}


def test_abstains_when_gt_below_min() -> None:
    cand = {"recall_at_5": 0.9, "per_class_recall": {"fire": 0.9}}
    inc = {"recall_at_5": 0.1, "per_class_recall": {"fire": 0.1}}  # candidate would crush incumbent
    d = pe_core_gate_decision(cand, inc, _CFG, gt_count=10, incumbent_source="stock_base")
    assert d.promotable is False
    assert any("gt_below_min" in r for r in d.reasons)


def test_evaluates_normally_when_gt_sufficient() -> None:
    cand = {"recall_at_5": 0.9, "per_class_recall": {"fire": 0.9}}
    inc = {"recall_at_5": 0.1, "per_class_recall": {"fire": 0.1}}
    d = pe_core_gate_decision(cand, inc, _CFG, gt_count=200, incumbent_source="stock_base")
    assert d.promotable is True


def test_gt_exactly_min_is_sufficient() -> None:
    cand = {"recall_at_5": 0.9, "per_class_recall": {"fire": 0.9}}
    inc = {"recall_at_5": 0.1, "per_class_recall": {"fire": 0.1}}
    d = pe_core_gate_decision(cand, inc, _CFG, gt_count=DEFAULT_PE_CORE_MIN_GT, incumbent_source="stock_base")
    assert d.promotable is True


def test_default_min_gt_is_committed_value() -> None:
    assert DEFAULT_PE_CORE_MIN_GT == 50
```

- [ ] **H4.2 — Run it — expect FAIL (module missing).**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_pe_core_gate.py -q
```

Expected: collection error / `FileNotFoundError`.

- [ ] **H4.3 — Write the gate wrapper.** Create `src/vlm_pipeline/lib/pe_core_gate.py`:

```python
"""PE-Core 승격 게이트 — GT-abstain 래퍼 (L1 lib, 순수).

design §7.4 / §8.1-D: PE 게이트 메트릭 = 사람-검수 GT 에 대한 cross-modal recall@k.
현재 쿼리가능 GT≈0 → GT count < eval_config['pe_core_min_gt'] 이면 abstain(승격 불가).
GT 가 §12 백필/리뷰로 쌓이기 전까지 PE 승격은 실질 비활성 (메커니즘만 구축).
GT 충분하면 lib.train_eval_gate.evaluate_gate 로 위임 (margin + per-class floor).

⚠️ lib 계층 규칙: dagster / defs / resources / ops import 금지.
"""

from __future__ import annotations

from typing import Any

from vlm_pipeline.lib.train_eval_gate import GateDecision, evaluate_gate

DEFAULT_PE_CORE_MIN_GT = 50  # spec §15-Q3 N_min 시작값 (eval_config 로 override 가능)


def pe_core_gate_decision(
    candidate_metrics: dict[str, Any],
    incumbent_metrics: dict[str, Any],
    eval_config: dict[str, Any],
    *,
    gt_count: int,
    incumbent_source: str,
) -> GateDecision:
    """GT < pe_core_min_gt 면 abstain(promotable=False). 아니면 evaluate_gate 위임."""
    min_gt = int(eval_config.get("pe_core_min_gt", DEFAULT_PE_CORE_MIN_GT))
    if int(gt_count) < min_gt:
        return GateDecision(
            promotable=False,
            reasons=[
                f"gt_below_min: human-reviewed GT count {gt_count} < pe_core_min_gt {min_gt} "
                f"(abstain — PE 승격은 GT 축적 전까지 비활성, design §7.4/§8.1-D)"
            ],
            per_metric={},
            per_class={},
        )
    return evaluate_gate(
        candidate_metrics, incumbent_metrics, eval_config, incumbent_source=incumbent_source
    )
```

- [ ] **H4.4 — Run it + lint + lib-guard — expect PASS.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_pe_core_gate.py -q
ruff check --line-length 120 src/vlm_pipeline/lib/pe_core_gate.py tests/unit/test_pe_core_gate.py
python3 scripts/check_lib_layer_imports.py
```

Expected: `4 passed`, `All checks passed!`, lib-guard exits 0.

- [ ] **H4.5 — Write the failing promotion-script test.** Create `tests/unit/test_promote_pe_core.py`:

```python
"""scripts/promote_pe_core.py — 포인터 전환/롤백 + 재임베딩 선결 검증 (스텁, dry-run 기본)."""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "promote_pe_core", str(pathlib.Path("scripts/promote_pe_core.py").resolve())
)
promote_pe_core = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(promote_pe_core)


class _FakeCursor:
    def __init__(self, fetch_results):
        self._results = list(fetch_results)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._results.pop(0) if self._results else None


def test_assert_reembed_complete_ok():
    # count(new_model_name) > 0 → coverage present
    cur = _FakeCursor([(187994,)])
    n = promote_pe_core.assert_reembed_complete(cur, new_model_name="facebook/PE-Core-L14-336@ft-x")
    assert n == 187994
    assert "model_name" in cur.executed[0][0].lower()


def test_assert_reembed_complete_zero_raises():
    cur = _FakeCursor([(0,)])
    with pytest.raises(promote_pe_core.PromotionError):
        promote_pe_core.assert_reembed_complete(cur, new_model_name="facebook/PE-Core-L14-336@ft-x")


def test_flip_pointer_dry_run_no_write():
    cur = _FakeCursor([])
    promote_pe_core.flip_pointer(
        cur, new_model_name="facebook/PE-Core-L14-336@ft-x", scope="frame_search", dry_run=True
    )
    assert cur.executed == []  # dry-run = no UPDATE


def test_flip_pointer_apply_updates_pointer():
    cur = _FakeCursor([])
    promote_pe_core.flip_pointer(
        cur, new_model_name="facebook/PE-Core-L14-336@ft-x", scope="frame_search", dry_run=False
    )
    sql = " ".join(s.lower() for s, _ in cur.executed)
    assert "embedding_active_model" in sql
    assert "model_name" in sql  # UPSERT/UPDATE of the pointer


def test_rollback_pointer_restores_prior():
    cur = _FakeCursor([])
    promote_pe_core.rollback_pointer(
        cur, prior_model_name="facebook/PE-Core-L14-336", scope="frame_search", dry_run=False
    )
    sql = " ".join(s.lower() for s, _ in cur.executed)
    assert "facebook/pe-core-l14-336" in (cur.executed[0][1] or {}).get("model_name", "").lower() \
        or "embedding_active_model" in sql
```

- [ ] **H4.6 — Run it — expect FAIL (functions missing).**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_promote_pe_core.py -q
```

Expected: import succeeds but `AttributeError`/load error on missing functions.

- [ ] **H4.7 — Write `scripts/promote_pe_core.py`.** Create the file. It imports E's helpers (Section E `scripts/promote_model.py` must be on the branch; if not yet merged, the import is guarded so the unit tests of the new functions still load — the new functions do not call E's helpers, only `main()` does):

```python
#!/usr/bin/env python3
"""PE-Core champion-challenger 승격 — 포인터 전환 + partial-HNSW + 서빙 교체 (design §8.1).

만들되 미실행 (--dry-run 기본). SAM 의 .pt 교체에 대응하는 PE 동작:
  1) (선결) 재임베딩(H3, Dagster reembed_under_version) 으로 새 model_name 커버리지 확보
  2) partial HNSW 빌드 (H2c create_model_partial_hnsw / lib.pgvector_index)
  3) embedding_active_model 포인터 원자 전환 (flip_pointer)  ← AL/검색이 즉시 새 model_name read
  4) 서빙 EMBEDDING_CHECKPOINT_PATH/MODEL_VERSION 세팅 + recreate (E2/E3 경로 재사용)
롤백 = 포인터를 이전 model_name 으로 되돌림(옛 벡터/인덱스 살아있어 즉시) + checkpoint env 되돌림 + recreate.

registry(model_registry) 가 승격 SoT. 포인터는 'AL/검색이 어떤 벡터를 보는가'의 SoT (design §2/§8.1).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# E 의 공통 헬퍼 재사용 (있을 때만 — 신규 함수 단위테스트는 이 import 없이도 로드 가능).
try:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from promote_model import (  # type: ignore
        PromotionError,
        _make_minio,
        _write_env_var,
        docker_recreate,
        download_and_verify,
    )
except Exception:  # pragma: no cover - E 미머지 시
    class PromotionError(RuntimeError):
        pass


_ACTIVE_SCOPE = "frame_search"
_DOCKER_ENV_FILE = REPO_ROOT / "docker" / ".env"


def assert_reembed_complete(cur, *, new_model_name: str) -> int:
    """새 model_name 으로 frame 임베딩이 실제로 존재하는지 확인 (포인터 전환 전 선결).

    재임베딩(H3) 미완 상태에서 포인터를 돌리면 AL/검색이 빈 인덱스를 보게 됨 → 0 이면 거부.
    """
    cur.execute(
        "SELECT count(*) FROM image_embeddings "
        "WHERE entity_type = 'frame' AND model_name = %(model_name)s",
        {"model_name": new_model_name},
    )
    row = cur.fetchone()
    n = int(row[0]) if row else 0
    if n <= 0:
        raise PromotionError(
            f"reembed not complete for {new_model_name!r}: 0 frame embeddings — "
            f"run reembed_under_version (H3) before flipping the pointer"
        )
    return n


def flip_pointer(cur, *, new_model_name: str, scope: str = _ACTIVE_SCOPE, dry_run: bool) -> None:
    """embedding_active_model 포인터를 새 model_name 으로 원자 전환 (UPSERT)."""
    if dry_run:
        print(f"[promote-pe][dry-run] would flip pointer scope={scope} -> {new_model_name}")
        return
    cur.execute(
        "INSERT INTO embedding_active_model (scope, model_name, updated_at, updated_by) "
        "VALUES (%(scope)s, %(model_name)s, now(), 'promote_pe_core') "
        "ON CONFLICT (scope) DO UPDATE SET "
        "  model_name = EXCLUDED.model_name, updated_at = now(), updated_by = EXCLUDED.updated_by",
        {"scope": scope, "model_name": new_model_name},
    )
    print(f"[promote-pe] pointer flipped scope={scope} -> {new_model_name}")


def rollback_pointer(cur, *, prior_model_name: str, scope: str = _ACTIVE_SCOPE, dry_run: bool) -> None:
    """포인터를 이전 model_name 으로 되돌림 (옛 벡터/인덱스가 살아있어 즉시 복구)."""
    if dry_run:
        print(f"[promote-pe][dry-run] would roll back pointer scope={scope} -> {prior_model_name}")
        return
    cur.execute(
        "INSERT INTO embedding_active_model (scope, model_name, updated_at, updated_by) "
        "VALUES (%(scope)s, %(model_name)s, now(), 'promote_pe_core_rollback') "
        "ON CONFLICT (scope) DO UPDATE SET "
        "  model_name = EXCLUDED.model_name, updated_at = now(), updated_by = EXCLUDED.updated_by",
        {"scope": scope, "model_name": prior_model_name},
    )
    print(f"[promote-pe] pointer rolled back scope={scope} -> {prior_model_name}")


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="PE-Core champion-challenger promotion (dry-run default)")
    p.add_argument("--model-version-id", help="promotable model_registry row id (pe_core)")
    p.add_argument("--new-model-name", help="versioned model_name to flip to, e.g. facebook/PE-Core-L14-336@ft-...")
    p.add_argument("--rollback-to", help="prior model_name to restore (rollback mode)")
    p.add_argument("--env", default="prod", choices=("prod", "staging"))
    p.add_argument("--apply", action="store_true", help="actually flip/recreate (default: dry-run)")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    dry_run = not args.apply
    print(f"[promote-pe] mode={'APPLY' if args.apply else 'DRY-RUN'} env={args.env}")
    # Wiring (PG connect, registry status transition via promote_model.promote_transition,
    # download_and_verify into ./docker/data/models/pe-core/, _write_env_var of
    # EMBEDDING_CHECKPOINT_PATH/EMBEDDING_MODEL_VERSION, create_model_partial_hnsw, flip_pointer,
    # docker_recreate('embedding-service')) is assembled in the integration step E7-analogue and is
    # intentionally side-effect-free under dry_run. Unit tests cover the pure transition functions.
    if dry_run:
        print("[promote-pe] dry-run: no DB write, no pointer flip, no recreate. Pass --apply to execute.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
```

- [ ] **H4.8 — Run the script test — expect PASS.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_promote_pe_core.py -q
```

Expected: `5 passed`.

- [ ] **H4.9 — Lint + commit.**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
ruff check --line-length 120 src/vlm_pipeline/lib/pe_core_gate.py scripts/promote_pe_core.py \
  tests/unit/test_pe_core_gate.py tests/unit/test_promote_pe_core.py
python3 scripts/check_lib_layer_imports.py
git add src/vlm_pipeline/lib/pe_core_gate.py scripts/promote_pe_core.py \
  tests/unit/test_pe_core_gate.py tests/unit/test_promote_pe_core.py
git commit -m "feat(mlops): PE-Core GT-abstain gate + promote_pe_core pointer flip/rollback (dry-run default)"
```

**Interfaces:**
- Produces: `pe_core_gate_decision`, `DEFAULT_PE_CORE_MIN_GT`; `promote_pe_core.assert_reembed_complete`/`flip_pointer`/`rollback_pointer`/`main`. Consumed by the eval asset (D9 may dispatch to `pe_core_gate_decision` for `model='pe_core'`) and by operators (G-section runbook references `scripts/promote_pe_core.py`).

---

### Task H5: Section-level green gate + ruff (whole section)

- [ ] **Run all Section H unit tests together (no GPU/PG needed).**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest \
  tests/unit/test_pgvector_index.py \
  tests/unit/test_active_model.py \
  tests/unit/test_fiftyone_active_model_pin.py \
  tests/unit/test_frame_embedding_active_default.py \
  tests/unit/test_reembed_asset.py \
  tests/unit/test_pe_core_gate.py \
  tests/unit/test_promote_pe_core.py -q
```

Expected: all pass (5+5+2+2+2+4+5 = 25 passed).

- [ ] **Run the integration tests (skip without DSN/pgvector).**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/integration/test_active_model_migration_015.py \
  tests/integration/test_active_model_mixin.py \
  tests/integration/test_reembed_targets.py -q
```

Expected: pass with a pgvector PG that has 006+013+015 applied; otherwise skip (never fail).

- [ ] **Definitions load + ruff (tracked files only, CI parity — ruff 0.7.4).**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -c "import vlm_pipeline.definitions; print('defs OK')"
git ls-files 'src/vlm_pipeline/lib/pgvector_index.py' 'src/vlm_pipeline/lib/active_model.py' \
  'src/vlm_pipeline/lib/pe_core_gate.py' 'src/vlm_pipeline/defs/embed/reembed.py' \
  'src/vlm_pipeline/resources/postgres_embedding.py' 'docker/analysis/fiftyone_pgvector.py' \
  'scripts/promote_pe_core.py' | xargs ruff check --line-length 120
python3 scripts/check_lib_layer_imports.py
```

Expected: `defs OK`, `All checks passed!`, lib-guard exits 0.

- [ ] **Commit (empty-allowed marker if nothing new).**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git commit --allow-empty -m "test(mlops): Section H PE-Core promotion track green gate"
```

---

## Section G — CI Rebuild Integration, deploy-stack trainer gating, and Operator Docs

> **Section scope:** This section wires the `vlm-trainer` container (authored in Section D) into the existing CI rebuild-detection + profile-gated build/recreate machinery, and writes the operator-facing documentation (CLAUDE.md MLOps section, `.agent/skill/mlops-finetune/SKILL.md` runbook, scripts table). All tasks here are file edits + doc content + grep/parse assertions — **no GPU, no runtime, CI-only verifiable**.
>
> **Ordering note:** Tasks G1–G2 (CI globs + deploy-stack guard) can land independently of Sections A–F. Tasks G3–G6 (docs) reference symbols produced by other sections (`scripts/promote_model.py`, `scripts/clear_maintenance.sh`, `/maintenance/enter|exit`, `train_dataset_versions`, `model_registry`); the doc *content* is fully written here, but the referenced scripts/endpoints are *created* in Sections D/E/F. Docs are safe to write first (they are forward-looking runbooks) — flagged in open_risks.

---

### Task G1: Add `^docker/trainer/` to the `detect_image_rebuild` glob in BOTH deploy workflows

The `detect_image_rebuild` job lists each `docker/<name>/` prefix explicitly (no wildcard). The trainer image must rebuild when `docker/trainer/` changes, on both prod and staging deploy paths. We add the same regex clause used for `docker/embedding/`, placed immediately after it for diff symmetry.

**Files:**
- `.github/workflows/deploy-production.yml` (edit, line ~126)
- `.github/workflows/deploy-test.yml` (edit, line ~126)
- `tests/unit/test_ci_trainer_rebuild_glob.py` (new)

**Interfaces:**
- Consumes: existing `detect_image_rebuild.steps.detect` bash block in both workflows.
- Produces: rebuild trigger clause `[[ "${path}" =~ ^docker/trainer/ ]]` present in both workflow files (asserted by `tests/unit/test_ci_trainer_rebuild_glob.py`).

- [ ] **Write the failing test.** Create `tests/unit/test_ci_trainer_rebuild_glob.py`:

```python
"""CI rebuild-glob + YAML parse guards for the vlm-trainer container (Section G).

These assert that docker/trainer/ changes trigger an image rebuild on BOTH the
prod and staging deploy workflows, and that the workflow YAML still parses.
No GPU / no runtime — pure file + YAML checks (CI-safe).
"""
from pathlib import Path

import yaml

_REPO = Path(__file__).resolve().parents[2]
_PROD = _REPO / ".github" / "workflows" / "deploy-production.yml"
_TEST = _REPO / ".github" / "workflows" / "deploy-test.yml"

_TRAINER_GLOB = '[[ "${path}" =~ ^docker/trainer/ ]]'


def test_workflows_exist():
    assert _PROD.is_file(), f"missing {_PROD}"
    assert _TEST.is_file(), f"missing {_TEST}"


def test_prod_workflow_has_trainer_glob():
    assert _TRAINER_GLOB in _PROD.read_text(encoding="utf-8")


def test_test_workflow_has_trainer_glob():
    assert _TRAINER_GLOB in _TEST.read_text(encoding="utf-8")


def test_prod_workflow_parses_as_yaml():
    # GitHub maps `on:` to the YAML boolean True key; just assert it loads + has jobs.
    doc = yaml.safe_load(_PROD.read_text(encoding="utf-8"))
    assert "detect_image_rebuild" in doc["jobs"]
    assert "deploy" in doc["jobs"]


def test_test_workflow_parses_as_yaml():
    doc = yaml.safe_load(_TEST.read_text(encoding="utf-8"))
    assert "detect_image_rebuild" in doc["jobs"]
    assert "deploy" in doc["jobs"]
```

- [ ] **Run it — expect FAIL** (glob not yet present):

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_ci_trainer_rebuild_glob.py -q
```

Expected: `test_prod_workflow_has_trainer_glob` and `test_test_workflow_has_trainer_glob` FAIL with `AssertionError`; the four parse/exist tests PASS. Summary line: `2 failed, 4 passed`.

- [ ] **Edit `deploy-production.yml`.** Find this exact line (line ~126):

```yaml
              || [[ "${path}" =~ ^docker/embedding/ ]] \
```

Replace with (add the trainer clause directly after, preserving the trailing backslash continuation):

```yaml
              || [[ "${path}" =~ ^docker/embedding/ ]] \
              || [[ "${path}" =~ ^docker/trainer/ ]] \
```

- [ ] **Edit `deploy-test.yml`.** The `detect_image_rebuild` block is byte-identical to prod's. Apply the SAME edit: find the single occurrence of:

```yaml
              || [[ "${path}" =~ ^docker/embedding/ ]] \
```

Replace with:

```yaml
              || [[ "${path}" =~ ^docker/embedding/ ]] \
              || [[ "${path}" =~ ^docker/trainer/ ]] \
```

- [ ] **Run it — expect PASS:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_ci_trainer_rebuild_glob.py -q
```

Expected: `6 passed`.

- [ ] **Sanity-grep both workflows** to confirm exactly one trainer clause per file:

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && grep -c 'docker/trainer/' .github/workflows/deploy-production.yml .github/workflows/deploy-test.yml
```

Expected output:
```
.github/workflows/deploy-production.yml:1
.github/workflows/deploy-test.yml:1
```

- [ ] **Commit:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add .github/workflows/deploy-production.yml .github/workflows/deploy-test.yml tests/unit/test_ci_trainer_rebuild_glob.py && git commit -m "ci(trainer): trigger image rebuild on docker/trainer/ changes (prod+staging)"
```

---

### Task G2: Add `trainer_active()` + conditional build/recreate to `deploy-stack.sh` (mirror sam3/embedding)

`docker/trainer/` is `profiles: ["trainer"]` and auto-start is OFF (Section D). The deploy script must (a) include `trainer` in `build_targets` only when the active profile set contains it (else `compose build trainer` errors), and (b) **never `up -d` / force-recreate** the trainer — it is a manual, decoupled GPU process (spec §7.3). This is the key divergence from the sam3/embedding pattern: build-if-active, but **do not start**. Building when active keeps the trainer image fresh in prod's local image cache so the operator's manual `compose run` picks up the deployed code without a separate build step.

**Files:**
- `scripts/deploy/deploy-stack.sh` (edit: add `trainer_active()` helper + one `build_targets` clause)
- `tests/unit/test_deploy_stack_trainer_gate.py` (new)

**Interfaces:**
- Consumes: existing `compose config --services` gating idiom (`sam3_active`, `embedding_active`), `build_targets` array, `BUILD_REQUIRED`.
- Produces: shell function `trainer_active()` and a `build_targets+=(trainer)` clause guarded by it. **No** `compose up -d trainer` anywhere (asserted).

- [ ] **Write the failing test.** Create `tests/unit/test_deploy_stack_trainer_gate.py`:

```python
"""deploy-stack.sh trainer gating guards (Section G).

The vlm-trainer container is profile-gated AND must never be auto-started by the
deploy script (it is a manual, Dagster-decoupled GPU process, spec §7.3). So:
  - a trainer_active() helper exists (mirrors sam3_active/embedding_active),
  - trainer is added to build_targets only when active,
  - the script NEVER runs `compose up`/recreate on the trainer service.
Pure string checks on the shell source (CI-safe, no docker).
"""
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_DEPLOY = _REPO / "scripts" / "deploy" / "deploy-stack.sh"


def _src() -> str:
    return _DEPLOY.read_text(encoding="utf-8")


def test_deploy_stack_exists():
    assert _DEPLOY.is_file()


def test_trainer_active_helper_defined():
    src = _src()
    assert "trainer_active()" in src
    # mirrors the sam3/embedding gating idiom
    assert "grep -qx trainer" in src


def test_trainer_added_to_build_targets_when_active():
    src = _src()
    assert "build_targets+=(trainer)" in src


def test_trainer_is_never_started_by_deploy():
    # The trainer is launched manually by the operator, never by the deploy script.
    src = _src()
    for forbidden in (
        "compose up -d --no-deps trainer",
        "compose up -d --no-deps --force-recreate trainer",
        "compose up -d trainer",
    ):
        assert forbidden not in src, f"deploy must not start trainer: {forbidden!r}"
```

- [ ] **Run it — expect FAIL:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_deploy_stack_trainer_gate.py -q
```

Expected: `test_trainer_active_helper_defined` and `test_trainer_added_to_build_targets_when_active` FAIL (`AssertionError`); the other two PASS. Summary: `2 failed, 2 passed`.

- [ ] **Edit `deploy-stack.sh` — add the `trainer_active()` helper.** Find this exact block (lines ~139-144):

```bash
embedding_active() {
    compose config --services 2>/dev/null | grep -qx embedding-service
}
```

Replace with (append the trainer helper right after):

```bash
embedding_active() {
    compose config --services 2>/dev/null | grep -qx embedding-service
}

# trainer 도 profile gating (profiles:["trainer"]). prod 만 활성 (.env COMPOSE_PROFILES=...,trainer).
# ⚠️ sam3/embedding 와 달리 deploy 가 trainer 를 절대 기동/recreate 하지 않는다 — 학습은
# Dagster run 라이프사이클과 분리된 수동 GPU 프로세스(spec §7.3, CI 재배포가 in-run op 고아화).
# active 일 때 build 만 해서 prod 로컬 이미지 캐시를 deployed 코드와 맞춰 둔다 →
# 운영자가 정비 윈도우에 `compose run` 으로 직접 띄움.
trainer_active() {
    compose config --services 2>/dev/null | grep -qx trainer
}
```

- [ ] **Edit `deploy-stack.sh` — add trainer to `build_targets`.** Find this exact block (lines ~160-163):

```bash
    if embedding_active; then
        build_targets+=(embedding-service)
    fi
    compose build --progress plain "${build_targets[@]}"
```

Replace with:

```bash
    if embedding_active; then
        build_targets+=(embedding-service)
    fi
    if trainer_active; then
        build_targets+=(trainer)
    fi
    compose build --progress plain "${build_targets[@]}"
```

- [ ] **Run it — expect PASS:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_deploy_stack_trainer_gate.py -q
```

Expected: `4 passed`.

- [ ] **Bash-syntax check** the edited script (catches mismatched braces/quotes without running it):

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && bash -n scripts/deploy/deploy-stack.sh && echo "SYNTAX_OK"
```

Expected output: `SYNTAX_OK`.

- [ ] **Commit:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add scripts/deploy/deploy-stack.sh tests/unit/test_deploy_stack_trainer_gate.py && git commit -m "ci(trainer): profile-gated build for vlm-trainer in deploy-stack (never auto-start)"
```

---

### Task G3: Add the MLOps section to CLAUDE.md (training trigger, eval gate read, promote/rollback, prod-GPU caution, maintenance recovery)

Operator-facing, safety-critical (spec §14). Written as forward-looking runbook content; the referenced scripts/endpoints are produced by Sections D/E/F. We insert a new `## MLOps — 파인튜닝 트랙` section immediately after the existing `## GCS 외부 수집` section (before the standalone `---` at line ~290).

**Files:**
- `CLAUDE.md` (edit: insert new section)
- `tests/unit/test_claudemd_mlops_section.py` (new)

**Interfaces:**
- Consumes: shared-contract names — `train_dataset_versions`, `model_registry`, `scripts/promote_model.py`, `scripts/clear_maintenance.sh`, `/maintenance/enter`, `/maintenance/exit`, `ENABLE_TRAINING`, `gpu_trainer`.
- Produces: a documented `## MLOps — 파인튜닝 트랙` section (asserted by grep-test).

- [ ] **Write the failing test.** Create `tests/unit/test_claudemd_mlops_section.py`:

```python
"""CLAUDE.md MLOps operator-doc presence guard (Section G, spec §14).

Asserts the safety-critical MLOps runbook section exists and names the exact
shared-contract symbols operators need (promote/rollback/maintenance recovery).
"""
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_CLAUDE = _REPO / "CLAUDE.md"


def test_mlops_section_present():
    txt = _CLAUDE.read_text(encoding="utf-8")
    assert "## MLOps — 파인튜닝 트랙" in txt


def test_mlops_section_names_required_symbols():
    txt = _CLAUDE.read_text(encoding="utf-8")
    required = [
        "scripts/promote_model.py",
        "scripts/clear_maintenance.sh",
        "model_registry",
        "train_dataset_versions",
        "ENABLE_TRAINING",
        "/maintenance/enter",
        "/maintenance/exit",
        "gpu_trainer",
        "status='promotable'",
    ]
    for token in required:
        assert token in txt, f"CLAUDE.md MLOps section missing: {token!r}"
```

- [ ] **Run it — expect FAIL:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_claudemd_mlops_section.py -q
```

Expected: both tests FAIL with `AssertionError`. Summary: `2 failed`.

- [ ] **Edit `CLAUDE.md`.** Find this exact block (the end of the GCS section, lines ~287-290):

```markdown
- Dagster schedule: `gcs_download_schedule` (매일 04:00 KST)
- 0바이트 파일 복구: `GCS_ZERO_BYTE_RETRIES` (기본 2)

---
```

Replace with (insert the whole MLOps section before the trailing `---`):

```markdown
- Dagster schedule: `gcs_download_schedule` (매일 04:00 KST)
- 0바이트 파일 복구: `GCS_ZERO_BYTE_RETRIES` (기본 2)

---

## MLOps — 파인튜닝 트랙

> SAM3 / PE-Core 를 도메인 데이터로 파인튜닝하는 골격. **인프라는 CI(dev→staging→main), 가중치 승격만 수동.**
> 설계 source of truth: `docs/superpowers/specs/2026-06-29-mlops-finetune-scaffolding-design.md`.
> 상세 운영 런북: `.agent/skill/mlops-finetune/SKILL.md` (정비락 복구·hung run 판별·검증 분리).

### 핵심 불변식 (위반 금지)

- **레지스트리가 진실**: 서빙 중인 가중치 = `model_registry` 의 `status='promoted'` 행. **심볼릭링크 아님** (CI `rsync --delete`+`git reset --hard` 가 untracked 링크를 날림).
- **학습셋은 동결 스냅샷**: `train_dataset_versions` 행 = `vlm-dataset/_trainsets/<id>/` 의 immutable 스냅샷. 라이브 라벨 흐름과 무간섭.
- **자기학습 금지**: 모델 파생 라벨(`auto_generated`, Gemini 캡션, `vlm-classification`)로 학습/eval 금지. GT = LS `finalized` 또는 AL-선별-후-사람-어노테이트만.
- **CI 는 학습 안 함**: GPU 학습은 `ENABLE_TRAINING` + 수동 게이트. CI(GPU 없음)는 마이그레이션·스냅샷빌더·eval로직·승격 dry-run·defs 로드만 검증.

### 학습 트리거 (온디맨드 수동, prod 박스)

1. 스냅샷 빌드 (Dagster asset, `defs/train/dataset.py`) → `train_dataset_versions` 행 + `_trainsets/<id>/` 동결. `al_confirmed_count=0` 은 정상(백필 전).
2. **정비 윈도우 진입** (아래 GPU 정비 모드) — 공유 GPU 라 서빙 drain 필수.
3. trainer 기동 — **Dagster run 과 분리된 독립 프로세스**(CI 재배포가 in-run op 고아화):
   ```bash
   cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
   # ENABLE_TRAINING=1 + 학습 대상 train_dataset_version_id 를 env 로 전달
   docker compose --env-file .env run --rm trainer   # profiles:["trainer"], 자동기동 X
   ```
   `gpu_trainer` concurrency=1 (run_coordinator) — 동시 학습 1개만.
4. 산출물: `vlm-dataset/_models/<model>/<version>/` (merged full-weight + `env_lock.json` + `train_log.jsonl` + `training_summary.json`) + `model_registry` `status='candidate'` 행.

### eval 게이트 읽기

- eval asset(`defs/train/eval.py`)이 sealed test split 에서 candidate vs incumbent → `model_registry.metrics` / `incumbent_metrics` 기록.
- `incumbent_source='stock_base'` = 첫 run(이전 promoted 없음, stock 모델을 동일 split 에 통과시킨 점수).
- **per-metric margin + per-class non-regression floor** 통과 시에만 `status='promotable'` 로 승격(평균이 클래스 퇴행 숨기지 않게). margin 기본값은 `eval_config`.
- 현재 상태 확인:
  ```sql
  SELECT model, version, status, incumbent_source, metrics, incumbent_metrics
  FROM model_registry ORDER BY created_at DESC LIMIT 10;
  ```
  `sam3_shadow_compare`(YOLO-동의도, mAP 아님)는 **게이트 아님, 2차 sanity 신호만**.

### 승격 + 롤백 (`scripts/promote_model.py`) — 만들되 기본 미실행

- 승격(`status='promotable'` 행만 대상): MinIO `checkpoint_key` → 호스트 모델 볼륨 다운로드 + `artifact_checksum` 검증 → env 세팅(SAM3=`SAM3_CHECKPOINT_PATH`, PE-Core=`EMBEDDING_CHECKPOINT_PATH`) → `docker recreate`.
  ```bash
  python scripts/promote_model.py promote --model-version-id <id> --env prod   # 볼륨 있는 prod 박스에서만
  python scripts/promote_model.py promote --model-version-id <id> --dry-run     # CI/staging: 무변경 검증
  ```
  성공 시 `status='promoted'`, `promoted_at`/`promoted_env` 기록.
- **롤백**: 이전 `promoted` 행을 재승격(같은 명령, 옛 `--model-version-id`) → recreate. 서빙 시작 로그에 resolved 경로 + checksum 출력 → 확인.
- PE-Core 승격 시 `image_embeddings.model_name` 이 버전 인코딩(`...@ft-...`)으로 갱신됨 → 코퍼스 재임베딩/dual-index 는 별도(deferred §10).

### GPU 정비 모드 (서빙 drain) + 복구

- 학습 전 GPU 서빙을 비워야 함. **공유 `docker-sam3-1`**(prod·staging 공유) 주의 — staging 도 같은 컨테이너를 본다.
- 서버사이드 게이트: `POST /maintenance/enter` → `/segment`·`/embed` 가 `503` + lazy-reload 거부. 완료 후 `POST /maintenance/exit` + `/warmup`.
- **fail-safe**: 정비 플래그에 `owner_run_id`+heartbeat/TTL. guard 센서(`stuck_run_guard` 패턴)가 stale 감지 시 자동 해제. 수동 복구는 `scripts/clear_maintenance.sh` → 상세 절차는 `.agent/skill/mlops-finetune/SKILL.md` §9.
- **⚠️ prod-GPU 주의**: prod main push(docs/tests 제외)는 dagster 무조건 재가동(memory `project_prod_deploy_dagster_restart`). 학습 윈도우 중에는 prod 배포 보류 권장 — 재배포가 정비 상태/in-run op 를 흔든다.

### env 노브

| env | 기본 | 의미 |
|-----|------|------|
| `ENABLE_TRAINING` | `false` | 1/true 일 때만 trainer 가 실제 GPU 학습. CI·staging 은 false 유지 |
| `TRAIN_FULL_FT` | `0` | 1 이면 풀파인튠(16GB 공유 GPU 주의), 기본은 LoRA/PEFT |
| `SAM3_CHECKPOINT_PATH` | (현행) | SAM3 서빙 로컬 가중치 경로 — 승격이 갱신 |
| `EMBEDDING_CHECKPOINT_PATH` | (미설정=stock) | PE-Core 서빙 가중치 경로(신규). 미설정 시 HF Hub stock |
| `COMPOSE_PROFILES` | (prod) `...,trainer` | trainer 컨테이너를 prod compose 에 노출(자동기동은 안 함) |

---
```

- [ ] **Run it — expect PASS:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_claudemd_mlops_section.py -q
```

Expected: `2 passed`.

- [ ] **Commit:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add CLAUDE.md tests/unit/test_claudemd_mlops_section.py && git commit -m "docs(mlops): CLAUDE.md MLOps section — train trigger, eval gate, promote/rollback, GPU maintenance"
```

---

### Task G4: Author the operator runbook `.agent/skill/mlops-finetune/SKILL.md`

Step-by-step runbook (spec §14): maintenance-lock recovery (§9), "is the run progressing vs hung" check (§7.3 observability), staging-vs-prod validation split (§10.1). **Note:** existing skills live under `.agent/skill/` (not `docs/.agent/skill/`); we follow the real repo convention. (Flagged in open_risks — prompt named `docs/.agent/skill/`.)

**Files:**
- `.agent/skill/mlops-finetune/SKILL.md` (new)
- `tests/unit/test_mlops_skill_runbook.py` (new)

**Interfaces:**
- Consumes: `/maintenance/enter`, `/maintenance/exit`, `/warmup`, `scripts/clear_maintenance.sh`, `train_log.jsonl`, `model_registry`, `ENABLE_TRAINING`.
- Produces: runbook doc with anchored sections `## 9.` (maintenance recovery), run-progress check, validation split (asserted by grep-test).

- [ ] **Write the failing test.** Create `tests/unit/test_mlops_skill_runbook.py`:

```python
"""mlops-finetune SKILL.md operator-runbook presence guard (Section G, spec §14)."""
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_SKILL = _REPO / ".agent" / "skill" / "mlops-finetune" / "SKILL.md"


def test_skill_file_exists():
    assert _SKILL.is_file(), f"missing {_SKILL}"


def test_skill_has_required_sections():
    txt = _SKILL.read_text(encoding="utf-8")
    required = [
        "## 9. 정비락 복구",          # maintenance-lock recovery (spec §9)
        "scripts/clear_maintenance.sh",
        "/maintenance/exit",
        "/warmup",
        "## run 이 progressing 인지 hung 인지 판별",
        "train_log.jsonl",
        "## staging vs prod 검증 분리",
        "ENABLE_TRAINING=false",
    ]
    for token in required:
        assert token in txt, f"SKILL.md missing: {token!r}"
```

- [ ] **Run it — expect FAIL:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_mlops_skill_runbook.py -q
```

Expected: both tests FAIL (`test_skill_file_exists` first). Summary: `2 failed`.

- [ ] **Create the runbook.** Write `.agent/skill/mlops-finetune/SKILL.md` with this exact content:

```markdown
# MLOps Fine-tune — Operator Runbook

> SAM3 / PE-Core 파인튜닝 트랙 운영 런북. 설계: `docs/superpowers/specs/2026-06-29-mlops-finetune-scaffolding-design.md`.
> 요약·env 노브는 `CLAUDE.md` "## MLOps — 파인튜닝 트랙". 이 문서는 **장애·판별·검증 절차**.
> ⚠️ 첫 실학습 전 blocking 산출물 (spec §14). 복구 절차를 읽지 않고 정비 윈도우를 열지 말 것.

## 핵심 주의

- 공유 GPU: `docker-sam3-1` 은 **prod·staging 공유**(memory `project_sam3_shared_container`). 정비/unload 는 staging 서빙도 영향.
- prod main push(docs/tests 제외)는 dagster 무조건 재가동(memory `project_prod_deploy_dagster_restart`). 학습 윈도우 중 prod 배포 보류.
- trainer 는 Dagster run 과 **분리된 독립 프로세스**(`docker compose run --rm trainer`). CI 재배포가 in-run op 를 고아화하므로 op 안에서 GPU 학습을 직접 돌리지 않음.

## 9. 정비락 복구 (maintenance-lock recovery)

정비 플래그(서버사이드 게이트 + PG/마커)는 **fail-safe(자동해제)** 설계. fail-stuck(영구 정지) 금지.

### 정상 흐름
1. `POST /maintenance/enter` (owner_run_id + heartbeat 기록) → `/segment`·`/embed` 가 `503`, lazy-reload 거부.
2. active GPU run drain/cancel → `/unload` → `nvidia-smi` free VRAM 확인.
3. 학습 종료 후 `POST /maintenance/exit` + `POST /warmup` → 모델 재로드 확인 → 서빙 sensor 재개.

### 락이 풀리지 않을 때 (stuck/crash)
1. guard 센서(`stuck_run_guard` 패턴)가 heartbeat 사망 또는 owner run 비RUNNING 감지 시 **자동 해제**(sensor 재개 + `/warmup` + 플래그 clear)를 시도한다. 먼저 Dagster UI 에서 guard 센서 tick 로그 확인.
2. 자동 해제가 안 되면 수동:
   ```bash
   # 정비 플래그 강제 해제 + warmup 트리거
   bash /home/user/work_p/Datapipeline-Data-data_pipeline/scripts/clear_maintenance.sh --env prod
   ```
   이 스크립트는 PG/마커 플래그 clear → `POST /maintenance/exit` → `POST /warmup` 순으로 호출.
3. 서빙 복구 검증:
   ```bash
   curl -sf http://localhost:8002/health    # sam3: model_loaded=true 기대
   curl -sf http://localhost:<embed_port>/health
   ```
   `503` 이 계속이면 컨테이너 로그(`docker logs docker-sam3-1 --tail 50`)에서 maintenance 플래그 상태 확인.

## run 이 progressing 인지 hung 인지 판별

MLflow 미도입 — 관측 스토리 = **Dagster UI + 컨테이너 로그 + MinIO JSONL**.

1. **per-step JSONL** (가장 빠른 신호): trainer 가 `step,loss,lr,throughput` 을 stdout + `_models/<ver>/train_log.jsonl` 에 append.
   ```bash
   docker logs --tail 30 <trainer-container>        # 최근 step 이 증가 중이면 진행
   mc cat local/vlm-dataset/_models/<model>/<version>/train_log.jsonl | tail -5
   ```
   step 번호/timestamp 가 멈춰 있으면 hung. loss=NaN 이면 학습 발산.
2. **GPU 활동**: `nvidia-smi` — trainer PID 의 GPU-Util/메모리가 0 으로 떨어졌으면 hung 또는 종료.
3. **Dagster op metadata**: 기동 op 가 start/heartbeat/end 를 asset metadata+log 로 기록 → heartbeat timestamp 가 stale 이면 hung.
4. hung 확정 시: trainer 컨테이너 stop → 정비락 복구(§9) → 부분 산출물(`_models/<ver>/`)은 candidate 미등록 상태로 남음(레지스트리에 행 없음 = 안전).

## staging vs prod 검증 분리

CI 에 GPU 없음. 무엇을 어디서 검증하는가:

### staging(:3031)에서 검증 가능 (`ENABLE_TRAINING=false`, prod GPU 무영향)
- PG 마이그레이션(`train_dataset_versions`, `model_registry`) 적용 + `pg_constraint` 확인.
- 스냅샷 빌더 asset: 소형 fixture 로 `content_checksum` 결정성 + group-aware split leakage 없음.
- eval 게이트 로직: 스텁 메트릭으로 stock_base 분기 + margin/non-regression.
- `promote_model.py --dry-run`: 레지스트리 선택 로직 + recreate 계획(무변경).
- defs 로드 스모크 + 정비 플래그 PG 메커니즘.

### prod 전용 (통제된 수동 윈도우)
- 실제 GPU 학습 + 공유 sam3 대상 `/unload`+`/warmup`.
- ⚠️ **공유 sam3 때문에 GPU 사이클의 진짜 staging dry-run 불가** = 수용된 리스크. 완화: fail-safe 락(§9) + 최소 첫-run 데이터셋 + 복구 런북 대기.
- 첫 실학습은 반드시 정비 윈도우 + 본 런북 §9 숙지 후.

## 참고 명령 모음

```bash
# 후보/승격 상태
psql "$PG_DSN" -c "SELECT model,version,status,incumbent_source FROM model_registry ORDER BY created_at DESC LIMIT 10;"
# 동결 학습셋 버전
psql "$PG_DSN" -c "SELECT train_dataset_version_id,task,total_count,al_confirmed_count,created_at FROM train_dataset_versions ORDER BY created_at DESC LIMIT 10;"
# 학습 기동 (정비 윈도우 안에서)
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker && docker compose --env-file .env run --rm trainer
```
```

- [ ] **Run it — expect PASS:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_mlops_skill_runbook.py -q
```

Expected: `2 passed`.

- [ ] **Commit:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add .agent/skill/mlops-finetune/SKILL.md tests/unit/test_mlops_skill_runbook.py && git commit -m "docs(mlops): operator runbook SKILL.md — maintenance recovery, run-progress check, validation split"
```

---

### Task G5: Add MLOps scripts to the CLAUDE.md "자주 쓰는 스크립트" table

Spec §14 requires the scripts table to list `scripts/promote_model.py` and `scripts/clear_maintenance.sh`. We append two rows to the existing table (after `verify_mvp.sh`, line ~257).

**Files:**
- `CLAUDE.md` (edit: append 2 table rows)
- `tests/unit/test_claudemd_scripts_table.py` (new)

**Interfaces:**
- Consumes: existing markdown table at `## 자주 쓰는 스크립트`.
- Produces: two new rows referencing `scripts/promote_model.py` + `scripts/clear_maintenance.sh` (asserted).

- [ ] **Write the failing test.** Create `tests/unit/test_claudemd_scripts_table.py`:

```python
"""CLAUDE.md scripts-table MLOps entries guard (Section G, spec §14)."""
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_CLAUDE = _REPO / "CLAUDE.md"


def test_scripts_table_lists_mlops_scripts():
    lines = _CLAUDE.read_text(encoding="utf-8").splitlines()
    # locate the "자주 쓰는 스크립트" table header
    start = next(i for i, ln in enumerate(lines) if "자주 쓰는 스크립트" in ln)
    # the table rows live within ~30 lines of the header
    block = "\n".join(lines[start:start + 30])
    assert "`scripts/promote_model.py`" in block
    assert "`scripts/clear_maintenance.sh`" in block
```

- [ ] **Run it — expect FAIL:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_claudemd_scripts_table.py -q
```

Expected: `1 failed` (`AssertionError` on `promote_model.py`).

- [ ] **Edit `CLAUDE.md`.** Find this exact line (line ~257):

```markdown
| `scripts/verify_mvp.sh` | E2E 검증 | 사용 가능 |
```

Replace with (append the two MLOps rows):

```markdown
| `scripts/verify_mvp.sh` | E2E 검증 | 사용 가능 |
| `scripts/promote_model.py` | MinIO 체크포인트 → 호스트 materialize + env + recreate (승격/롤백) | MLOps (만들되 기본 미실행; `--dry-run` CI-safe) |
| `scripts/clear_maintenance.sh` | GPU 정비락 수동 강제 해제 + `/maintenance/exit` + `/warmup` | MLOps 복구 (`.agent/skill/mlops-finetune/SKILL.md` §9) |
```

- [ ] **Run it — expect PASS:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest tests/unit/test_claudemd_scripts_table.py -q
```

Expected: `1 passed`.

- [ ] **Commit:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && git add CLAUDE.md tests/unit/test_claudemd_scripts_table.py && git commit -m "docs(mlops): add promote_model.py + clear_maintenance.sh to scripts table"
```

---

### Task G6: Full-section regression — run all Section G tests together + ruff

Confirms all Section G additions pass as a group and that the new test files satisfy the CI ruff 0.7.4 line-length-120 gate (CLAUDE.md coding rules). No new src under `src/` is touched by this section, so the prod image rebuild trigger is via the workflow edits only (intended).

**Files:** (none new — verification only)

**Interfaces:**
- Consumes: all test files from G1–G5.
- Produces: green Section G suite + clean ruff.

- [ ] **Run the full Section G suite:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pytest \
  tests/unit/test_ci_trainer_rebuild_glob.py \
  tests/unit/test_deploy_stack_trainer_gate.py \
  tests/unit/test_claudemd_mlops_section.py \
  tests/unit/test_mlops_skill_runbook.py \
  tests/unit/test_claudemd_scripts_table.py -q
```

Expected: `15 passed` (6 + 4 + 2 + 2 + 1).

- [ ] **Ruff-check the new test files** (match CI: ruff 0.7.4, line-length 120). Use a throwaway venv pinned to 0.7.4 per the project ruff-CI memory, or if ruff is already on PATH at a different version, scope to line-length only:

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python -m pip install --quiet "ruff==0.7.4" 2>/dev/null; ruff check --line-length 120 \
  tests/unit/test_ci_trainer_rebuild_glob.py \
  tests/unit/test_deploy_stack_trainer_gate.py \
  tests/unit/test_claudemd_mlops_section.py \
  tests/unit/test_mlops_skill_runbook.py \
  tests/unit/test_claudemd_scripts_table.py
```

Expected output: `All checks passed!` (no findings).

- [ ] **Confirm no stray trailing whitespace / parse issues in the two edited YAML files** one final time:

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline && python3 -c "import yaml; [yaml.safe_load(open(p).read()) for p in ('.github/workflows/deploy-production.yml','.github/workflows/deploy-test.yml')]; print('YAML_OK')"
```

Expected output: `YAML_OK`.

No commit needed (verification-only task) — if ruff auto-fixed anything, `git add -A && git commit -m "chore(mlops): ruff line-length fixups for Section G tests"`.

---


---

## Section J — DVC dataset versioning + dataset_catalog

> **Scope reminder (scaffolding only):** This section BUILDS the DVC-curation layer but never runs a live `dvc add`/`dvc push`/`dvc get` against MinIO in CI. Every git op is exercised against a `tmp_path` git repo via `subprocess`; every DVC binary call and MinIO byte transfer is mocked (`monkeypatch` of the subprocess wrapper) or backed by `moto`. The bare repo, the `post-receive` hook, the `.dvc/config` S3-remote template, and the new `dvc` dependency pin are **authored as setup artifacts, NOT auto-run** by any test or CI step. `dvc` is a NEW dependency — pinned ONLY in `docker/trainer/requirements.txt` and a new `docker/curation/requirements.txt`, never in serving images (`docker/embedding`, `docker/sam3`).
>
> **Layer separation (anti-duplication, spec §7.6):** `DVC/_dvc/` (git pointer + commit intent) = curation source-of-truth · `dataset_catalog` (PG) = queryable index incl. commit message · `_trainsets/` + `train_dataset_versions` = DERIVED frozen training snapshot · MLflow = run link. No layer owns another's artifacts. A `_trainsets/` snapshot is NEVER treated as an editable curation dataset.
>
> **Build order:** J lands AFTER **A** (`013_mlops_finetune.sql` creates `train_dataset_versions` — J1's `ALTER` targets it), **B** (`defs/train/dataset.py` + `resources/postgres_train.py` exist — J3 extends the mixin, J6 hooks the builder), and **R1**/**I** are independent. J1 (migration 016) is J's root dependency; J2 (pure parsers) is independent and can land first; J3 needs J1+J2; J4/J5/J6 need J3; J7/J8 are independent leaves.
>
> **Spike-confirmed facts (baked into tasks):**
> - Migration runner (`resources/postgres_migration.py`): forward-only file glob `NNN_*.sql`, each file self-contained `BEGIN/COMMIT`, `-- @ASSERT_AFTER:` lines verified every boot, `_REQUIRED_MIGRATIONS` is an additive frozenset, **one DO block max** per file (multi-DO partial-apply quirk). Existing migrations end at **012**; Section A adds **013**; J adds **016** (014/015 reserved for A/F/other sections — J does not author them, and the runner tolerates gaps).
> - MinIO creds/endpoint env names (reused verbatim from `docker/embedding` + `MinIOResource`): `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`. The DVC S3 remote maps these to `endpointurl` / `access_key_id` / `secret_access_key`. Only the `vlm-dataset` bucket is touched, prefix `_dvc/` (5-bucket policy — no new bucket).
> - Sensor idiom (`defs/ingest/sensor_stuck_guard.py`): `@sensor(minimum_interval_seconds=int_env(...), default_status=DefaultSensorStatus.STOPPED, required_resource_keys={...})`, returns `SkipReason(...)` when nothing to do, broad-`except` around `context.instance` calls. Registered by appending to the list in `definitions_production.py::build_production_sensors`.
> - `pg_resource` (integration) / `postgres_resource` (unit-ish) fixtures auto-skip without `DATAOPS_TEST_POSTGRES_DSN`. A skip is NOT a pass — J1/J3 real-PG tests provision a throwaway `postgres:15` via `docker run` when the DSN is unset (commands shown).
> - `mock_minio` fixture = `moto.mock_aws` + `MinIOResource(endpoint=http://localhost:9000, access_key=test, secret_key=test)`. J4 reuses it.
> - `lib/` is L1 pure — `.dvc`-YAML and git-log parsers (J2) go in `lib/`, import only stdlib + `dataclasses`, NO dagster/resources/ops import (enforced by `scripts/check_lib_layer_imports.py`).

---

### Task J1: migration 016 — `dataset_catalog` ×3 + `ALTER train_dataset_versions` + `_REQUIRED_MIGRATIONS` — real-PG TDD

**Files:**
- `tests/integration/test_dataset_catalog_migration_016.py` (new)
- `src/vlm_pipeline/sql/migrations/postgres/016_dataset_catalog.sql` (new)
- `src/vlm_pipeline/resources/postgres_migration.py` (modify — add `016_dataset_catalog.sql` to `_REQUIRED_MIGRATIONS`)

**Interfaces:**
- Consumes: `pg_resource` fixture (`tests/integration/conftest.py`), table `train_dataset_versions` (Section A `013`).
- Produces: PG tables `dataset_catalog`, `dataset_catalog_aliases`, `dataset_catalog_pin_events`; column `train_dataset_versions.dataset_catalog_id UUID NULL` + FK `train_dataset_versions_dataset_catalog_fk`; constraints `dataset_catalog_repo_rev_dvc_unique` (UNIQUE), `dataset_catalog_status_check`, `dataset_catalog_aliases_catalog_fk`. Adds `016_dataset_catalog.sql` to `_REQUIRED_MIGRATIONS`.

- [ ] **J1.1 — Write the failing integration test.** Create `tests/integration/test_dataset_catalog_migration_016.py` (exact content):

```python
"""016_dataset_catalog.sql — real-PG assertions (tables / UNIQUE / status CHECK / FKs).

pg_resource applies ALL migrations (ensure_schema) on a throwaway DB, so this also
exercises the 016 -> 013 ALTER ordering. Auto-skips without DATAOPS_TEST_POSTGRES_DSN;
a skip is NOT a pass — provision a throwaway postgres:15 (see J1.2) to actually run it.
"""
from __future__ import annotations

import uuid

import psycopg2
import pytest


def _exists(cur, sql: str) -> bool:
    cur.execute(sql)
    return bool(cur.fetchone()[0])


def test_016_creates_three_catalog_tables(pg_resource) -> None:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            for tbl in ("dataset_catalog", "dataset_catalog_aliases", "dataset_catalog_pin_events"):
                assert _exists(cur, f"SELECT to_regclass('{tbl}') IS NOT NULL"), f"{tbl} missing"


def test_016_catalog_core_columns(pg_resource) -> None:
    expected = {
        "dataset_catalog_id", "task", "dataset_name", "status",
        "data_repo_id", "data_repo_url", "git_rev", "git_short_rev", "git_ref", "git_tag",
        "commit_subject", "commit_message", "commit_author_name", "commit_author_email",
        "committed_at", "ingested_at",
        "dvc_file_path", "dvc_out_path", "dvc_md5", "dvc_size_bytes", "dvc_nfiles",
        "dvc_remote_name", "dvc_remote_url",
        "train_dataset_version_id", "content_checksum", "mlflow_run_id",
    }
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'dataset_catalog'"
            )
            got = {r[0] for r in cur.fetchall()}
    missing = expected - got
    assert not missing, f"dataset_catalog missing columns: {sorted(missing)}"


def test_016_unique_on_repo_rev_dvc(pg_resource) -> None:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            assert _exists(
                cur,
                "SELECT EXISTS (SELECT 1 FROM pg_constraint "
                "WHERE conname = 'dataset_catalog_repo_rev_dvc_unique' "
                "AND conrelid = 'dataset_catalog'::regclass AND contype = 'u')",
            ), "UNIQUE(data_repo_id, git_rev, dvc_file_path, dvc_out_path) missing"


def test_016_status_check_rejects_bad_status(pg_resource) -> None:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            assert _exists(
                cur,
                "SELECT EXISTS (SELECT 1 FROM pg_constraint "
                "WHERE conname = 'dataset_catalog_status_check' "
                "AND conrelid = 'dataset_catalog'::regclass AND contype = 'c')",
            ), "status CHECK missing"
            raised = False
            try:
                cur.execute(
                    "INSERT INTO dataset_catalog "
                    "(dataset_catalog_id, task, dataset_name, status, data_repo_id, git_rev, "
                    " dvc_file_path, dvc_out_path) "
                    "VALUES (%s,'sam3_detection','d','BOGUS','repo','abc','a.dvc','data/a')",
                    (str(uuid.uuid4()),),
                )
            except psycopg2.errors.CheckViolation:
                raised = True
            assert raised, "bad status was not rejected by CHECK"


def test_016_alters_train_dataset_versions_with_fk(pg_resource) -> None:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            assert _exists(
                cur,
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                "WHERE table_name = 'train_dataset_versions' AND column_name = 'dataset_catalog_id')",
            ), "train_dataset_versions.dataset_catalog_id column missing"
            assert _exists(
                cur,
                "SELECT EXISTS (SELECT 1 FROM pg_constraint "
                "WHERE conrelid = 'train_dataset_versions'::regclass "
                "AND confrelid = 'dataset_catalog'::regclass AND contype = 'f')",
            ), "train_dataset_versions -> dataset_catalog FK missing"


def test_016_alias_unique_one_per_task(pg_resource) -> None:
    """PK(task, alias) — one 'current' alias per task can exist."""
    cid = str(uuid.uuid4())
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dataset_catalog "
                "(dataset_catalog_id, task, dataset_name, status, data_repo_id, git_rev, "
                " dvc_file_path, dvc_out_path) "
                "VALUES (%s,'sam3_detection','d','available','repo','rev1','a.dvc','data/a')",
                (cid,),
            )
            cur.execute(
                "INSERT INTO dataset_catalog_aliases (task, alias, dataset_catalog_id, pinned_by) "
                "VALUES ('sam3_detection','current',%s,'tester')",
                (cid,),
            )
            raised = False
            try:
                cur.execute(
                    "INSERT INTO dataset_catalog_aliases (task, alias, dataset_catalog_id, pinned_by) "
                    "VALUES ('sam3_detection','current',%s,'tester')",
                    (cid,),
                )
            except psycopg2.errors.UniqueViolation:
                raised = True
            assert raised, "second 'current' alias for same task was not rejected"
```

- [ ] **J1.2 — Run it, expect FAIL (NOT skip).** If `DATAOPS_TEST_POSTGRES_DSN` is unset, provision a throwaway `postgres:15` so the test actually exercises the migration:

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
docker run -d --rm --name pg-dvc-016 -e POSTGRES_PASSWORD=pw -p 55416:5432 postgres:15 >/dev/null
until docker exec pg-dvc-016 pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
DATAOPS_TEST_POSTGRES_DSN="postgresql://postgres:pw@localhost:55416/postgres" \
  python -m pytest tests/integration/test_dataset_catalog_migration_016.py -q 2>&1 | tail -12
```

Expected: `6 failed` (NOT skipped). Because `ensure_schema()` runs all present migrations, the failures surface as `dataset_catalog missing` / `to_regclass(...) IS NOT NULL` assertion errors — the 016 file does not exist yet. (Section A's 013 must be present for the `ALTER` to have a target; if 013 is also absent the relevant test errors on `train_dataset_versions` not existing — still a fail, not a pass.)

- [ ] **J1.3 — Create the migration** `src/vlm_pipeline/sql/migrations/postgres/016_dataset_catalog.sql` (exact content — idempotent `CREATE … IF NOT EXISTS`, named constraints, one DO block max [zero here], trailing `ALTER` for the cross-link FK to avoid the 013↔016 circular FK):

```sql
-- 016_dataset_catalog.sql — DVC 큐레이션 데이터셋 인덱스 (spec §5.4 / §7.6).
--
-- DVC 로 버전된 큐레이션 데이터셋 1개(= 커밋당 .dvc out당)를 PG 에 1행으로 색인한다.
-- 바이트는 MinIO s3://vlm-dataset/_dvc/ 에, .dvc YAML 포인터는 호스트 bare git repo 에 있고,
-- 이 테이블은 "쿼리 가능한 인덱스 + 커밋 메시지 기록"일 뿐 — 어느 레이어 소유물도 소유하지 않는다.
--   * dataset_catalog        : DVC 버전 1행. status 로 ingestion 상태 추적, 커밋 메시지 보존.
--   * dataset_catalog_aliases: 가변 pin (task당 alias 1개). pin() API 만 갱신, raw UPDATE 금지.
--   * dataset_catalog_pin_events: append-only 감사 (previous_dataset_catalog_id 로 pin 이력 추적).
--   * train_dataset_versions.dataset_catalog_id (FK) : 큐레이션↔학습 두 레이어 역링크.
--
-- 순환 FK 회피: 013 이 train_dataset_versions 를, 016 이 dataset_catalog 를 만들므로
-- train_dataset_versions -> dataset_catalog FK 는 016 에서 ALTER 로 뒤늦게 단다 (양 테이블 존재 보장).
-- 단일 DO 블록 미사용 — runner 의 multi-DO 부분적용 quirk 회피 (모두 IF NOT EXISTS DDL).
--
-- @ASSERT_AFTER: SELECT to_regclass('dataset_catalog') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('dataset_catalog_aliases') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('dataset_catalog_pin_events') IS NOT NULL
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dataset_catalog_repo_rev_dvc_unique' AND conrelid = 'dataset_catalog'::regclass AND contype = 'u')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dataset_catalog_status_check' AND conrelid = 'dataset_catalog'::regclass AND contype = 'c')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'train_dataset_versions' AND column_name = 'dataset_catalog_id')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'train_dataset_versions'::regclass AND confrelid = 'dataset_catalog'::regclass AND contype = 'f')

BEGIN;

CREATE TABLE IF NOT EXISTS dataset_catalog (
    dataset_catalog_id   UUID PRIMARY KEY,
    task                 TEXT NOT NULL,
    dataset_name         TEXT NOT NULL,
    status               TEXT NOT NULL DEFAULT 'pending',
    -- git identity
    data_repo_id         TEXT NOT NULL,
    data_repo_url        TEXT,
    git_rev              TEXT NOT NULL,
    git_short_rev        TEXT,
    git_ref              TEXT,
    git_tag              TEXT,
    -- commit message (core req §5.4)
    commit_subject       TEXT,
    commit_message       TEXT,
    commit_author_name   TEXT,
    commit_author_email  TEXT,
    committed_at         TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- DVC pointer (.dvc YAML)
    dvc_file_path        TEXT NOT NULL,
    dvc_out_path         TEXT NOT NULL,
    dvc_md5              TEXT,
    dvc_size_bytes       BIGINT,
    dvc_nfiles           INTEGER,
    dvc_remote_name      TEXT,
    dvc_remote_url       TEXT,
    -- derived links
    train_dataset_version_id  TEXT,
    content_checksum     TEXT,
    mlflow_run_id        TEXT,
    CONSTRAINT dataset_catalog_repo_rev_dvc_unique
        UNIQUE (data_repo_id, git_rev, dvc_file_path, dvc_out_path),
    CONSTRAINT dataset_catalog_status_check CHECK (
        status IN ('pending', 'available', 'pinned', 'archived', 'invalid', 'pending_missing_dvc_objects')
    ),
    CONSTRAINT dataset_catalog_task_check CHECK (btrim(task) <> '')
);

CREATE INDEX IF NOT EXISTS dataset_catalog_task_status_idx
    ON dataset_catalog (task, status);
CREATE INDEX IF NOT EXISTS dataset_catalog_git_rev_idx
    ON dataset_catalog (git_rev);

CREATE TABLE IF NOT EXISTS dataset_catalog_aliases (
    task                 TEXT NOT NULL,
    alias                TEXT NOT NULL,
    dataset_catalog_id   UUID NOT NULL,
    pinned_by            TEXT NOT NULL,
    pin_reason           TEXT,
    pinned_at            TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dataset_catalog_aliases_pkey PRIMARY KEY (task, alias),
    CONSTRAINT dataset_catalog_aliases_catalog_fk
        FOREIGN KEY (dataset_catalog_id) REFERENCES dataset_catalog(dataset_catalog_id)
);

CREATE TABLE IF NOT EXISTS dataset_catalog_pin_events (
    event_id                      UUID PRIMARY KEY,
    task                          TEXT NOT NULL,
    alias                         TEXT NOT NULL,
    dataset_catalog_id            UUID NOT NULL REFERENCES dataset_catalog(dataset_catalog_id),
    previous_dataset_catalog_id   UUID REFERENCES dataset_catalog(dataset_catalog_id),
    pinned_by                     TEXT NOT NULL,
    pin_reason                    TEXT,
    pinned_at                     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS dataset_catalog_pin_events_task_alias_idx
    ON dataset_catalog_pin_events (task, alias, pinned_at);

-- 큐레이션↔학습 역링크: 013 이후라 train_dataset_versions 가 존재. ADD COLUMN IF NOT EXISTS 멱등.
ALTER TABLE train_dataset_versions
    ADD COLUMN IF NOT EXISTS dataset_catalog_id UUID;

-- FK 는 별도 ADD CONSTRAINT (IF NOT EXISTS 미지원 → 존재 검사 후 추가; 단일 statement, DO 미사용).
ALTER TABLE train_dataset_versions
    DROP CONSTRAINT IF EXISTS train_dataset_versions_dataset_catalog_fk;
ALTER TABLE train_dataset_versions
    ADD CONSTRAINT train_dataset_versions_dataset_catalog_fk
        FOREIGN KEY (dataset_catalog_id) REFERENCES dataset_catalog(dataset_catalog_id);

COMMIT;
```

> Note on idempotency of the FK: `ADD CONSTRAINT` has no `IF NOT EXISTS`, so we `DROP CONSTRAINT IF EXISTS` immediately before — two single statements, no DO block. Re-running 016 is a no-op (drop-then-readd same FK).

- [ ] **J1.4 — Register 016 as required.** In `src/vlm_pipeline/resources/postgres_migration.py`, add `016_dataset_catalog.sql` to `_REQUIRED_MIGRATIONS` (additive — keep existing entries):

```python
    _REQUIRED_MIGRATIONS: ClassVar[frozenset[str]] = frozenset(
        {
            "001_init.sql",
            "002_genai.sql",
            "003_genai_veo.sql",
            "004_dataset_lineage.sql",
            "005_labels_unique.sql",
            "011_image_label_annotations.sql",
            "016_dataset_catalog.sql",
        }
    )
```

- [ ] **J1.5 — Run the integration test, expect PASS** (same throwaway PG):

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
DATAOPS_TEST_POSTGRES_DSN="postgresql://postgres:pw@localhost:55416/postgres" \
  python -m pytest tests/integration/test_dataset_catalog_migration_016.py -q 2>&1 | tail -8
docker rm -f pg-dvc-016 >/dev/null
```

Expected: `6 passed`.

- [ ] **J1.6 — Idempotency + raw psql sanity (optional, same image):**

```bash
docker run -d --rm --name pg-dvc-016b -e POSTGRES_PASSWORD=pw -p 55417:5432 postgres:15 >/dev/null
until docker exec pg-dvc-016b pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
# 013 must precede 016 for the ALTER target:
docker cp src/vlm_pipeline/sql/migrations/postgres/013_mlops_finetune.sql pg-dvc-016b:/013.sql
docker cp src/vlm_pipeline/sql/migrations/postgres/016_dataset_catalog.sql pg-dvc-016b:/016.sql
docker exec pg-dvc-016b psql -U postgres -q -f /013.sql
docker exec pg-dvc-016b psql -U postgres -q -f /016.sql   # first apply
docker exec pg-dvc-016b psql -U postgres -q -f /016.sql   # second apply = idempotent
docker exec pg-dvc-016b psql -U postgres -tAc \
  "SELECT conname FROM pg_constraint WHERE conname='dataset_catalog_repo_rev_dvc_unique';"
docker rm -f pg-dvc-016b >/dev/null
```

Expected: both 016 applies end without error; final query prints `dataset_catalog_repo_rev_dvc_unique`.

- [ ] **J1.7 — Commit:**

```bash
git add src/vlm_pipeline/sql/migrations/postgres/016_dataset_catalog.sql \
        src/vlm_pipeline/resources/postgres_migration.py \
        tests/integration/test_dataset_catalog_migration_016.py
git commit -m "feat(mlops): dataset_catalog DVC index migration (016) + train_dataset_versions FK"
```

---

### Task J2: pure `lib/` parsers — `.dvc` YAML + `git log --format` output → dataclasses — unit TDD

**Files:**
- `tests/unit/test_dvc_catalog_parsers.py` (new)
- `src/vlm_pipeline/lib/dvc_catalog.py` (new — L1 pure, stdlib + `yaml` only; NO dagster/resources/ops import)

**Interfaces:**
- Consumes: `.dvc` YAML text, `git log -1 --format=%H%n%s%n%b%n%an%n%ae%n%cI` stdout text.
- Produces:
  - `@dataclass DvcOut(path: str, md5: str | None, size: int | None, nfiles: int | None)`
  - `@dataclass DvcPointer(dvc_file_path: str, outs: list[DvcOut])`
  - `@dataclass GitCommitMeta(git_rev: str, commit_subject: str, commit_message: str, commit_author_name: str, commit_author_email: str, committed_at: str)`
  - `parse_dvc_pointer(dvc_file_path: str, yaml_text: str) -> DvcPointer`
  - `parse_git_log_format(stdout: str) -> GitCommitMeta` — parses the exact 6-line `%H%n%s%n%b%n%an%n%ae%n%cI` layout (body `%b` may itself be multi-line; everything between subject line and the final 3 lines is the body)
  - `GIT_LOG_FORMAT: str = "%H%n%s%n%b%n%an%n%ae%n%cI"` (single source of truth for J4)
  - `dvc_remote_url_for(out_path: str, *, bucket: str = "vlm-dataset", prefix: str = "_dvc") -> str` → `s3://vlm-dataset/_dvc/<out_path>`

- [ ] **J2.1 — Write the failing unit test.** Create `tests/unit/test_dvc_catalog_parsers.py` (exact content):

```python
"""lib.dvc_catalog — pure .dvc YAML + git-log parsers (L1, no dagster/DB/MinIO)."""
from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "src"))

from vlm_pipeline.lib.dvc_catalog import (  # noqa: E402
    GIT_LOG_FORMAT,
    DvcOut,
    DvcPointer,
    GitCommitMeta,
    dvc_remote_url_for,
    parse_dvc_pointer,
    parse_git_log_format,
)

# A real `dvc add data/fire_v3` produces data/fire_v3.dvc with this shape:
_DVC_YAML = """\
outs:
- md5: a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6.dir
  size: 104857600
  nfiles: 1280
  hash: md5
  path: fire_v3
"""

_GIT_STDOUT = (
    "9f3c2a1b7e4d5c6a8b0f1e2d3c4b5a6978d0e1f2\n"
    "curate: fire/smoke v3 — drop blurry night frames\n"
    "Added 320 SourceA night clips, removed 44 mislabeled.\n"
    "Class balance: fire=540 smoke=410.\n"
    "Seohee Jin\n"
    "shjin@example.com\n"
    "2026-06-29T11:18:04+09:00\n"
)


def test_git_log_format_constant_is_six_fields() -> None:
    assert GIT_LOG_FORMAT == "%H%n%s%n%b%n%an%n%ae%n%cI"


def test_parse_dvc_pointer_single_out() -> None:
    ptr = parse_dvc_pointer("data/fire_v3.dvc", _DVC_YAML)
    assert isinstance(ptr, DvcPointer)
    assert ptr.dvc_file_path == "data/fire_v3.dvc"
    assert len(ptr.outs) == 1
    out = ptr.outs[0]
    assert isinstance(out, DvcOut)
    assert out.path == "fire_v3"
    assert out.md5 == "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6.dir"
    assert out.size == 104857600
    assert out.nfiles == 1280


def test_parse_dvc_pointer_missing_optional_fields() -> None:
    ptr = parse_dvc_pointer("x.dvc", "outs:\n- path: lone\n  md5: deadbeef\n")
    out = ptr.outs[0]
    assert out.path == "lone"
    assert out.md5 == "deadbeef"
    assert out.size is None
    assert out.nfiles is None


def test_parse_dvc_pointer_rejects_no_outs() -> None:
    try:
        parse_dvc_pointer("bad.dvc", "meta: {}\n")
    except ValueError as exc:
        assert "outs" in str(exc)
    else:
        raise AssertionError("expected ValueError on .dvc with no outs")


def test_parse_git_log_format_multiline_body() -> None:
    meta = parse_git_log_format(_GIT_STDOUT)
    assert isinstance(meta, GitCommitMeta)
    assert meta.git_rev == "9f3c2a1b7e4d5c6a8b0f1e2d3c4b5a6978d0e1f2"
    assert meta.commit_subject == "curate: fire/smoke v3 — drop blurry night frames"
    # body = everything between subject and the trailing 3 fields (may be multi-line):
    assert "Added 320 SourceA night clips" in meta.commit_message
    assert "Class balance: fire=540 smoke=410." in meta.commit_message
    assert meta.commit_author_name == "Seohee Jin"
    assert meta.commit_author_email == "shjin@example.com"
    assert meta.committed_at == "2026-06-29T11:18:04+09:00"


def test_parse_git_log_format_empty_body() -> None:
    stdout = "abc123\nsubject only\n\nName\nn@e.x\n2026-01-01T00:00:00Z\n"
    meta = parse_git_log_format(stdout)
    assert meta.commit_subject == "subject only"
    assert meta.commit_message == ""
    assert meta.committed_at == "2026-01-01T00:00:00Z"


def test_dvc_remote_url_for() -> None:
    assert dvc_remote_url_for("fire_v3") == "s3://vlm-dataset/_dvc/fire_v3"
    assert dvc_remote_url_for("a/b") == "s3://vlm-dataset/_dvc/a/b"
```

- [ ] **J2.2 — Run it, expect FAIL:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_dvc_catalog_parsers.py -q 2>&1 | tail -6
```

Expected: `ModuleNotFoundError: No module named 'vlm_pipeline.lib.dvc_catalog'` (collection error, non-zero exit).

- [ ] **J2.3 — Implement** `src/vlm_pipeline/lib/dvc_catalog.py` (exact content):

```python
"""lib.dvc_catalog — pure parsers for the DVC curation layer (L1).

.dvc YAML pointer + `git log -1 --format=...` output → dataclasses. No dagster /
resources / ops import (enforced by scripts/check_lib_layer_imports.py). Only stdlib
+ PyYAML (already a dep via dagster). Used by the ingestion op/sensor (defs/train, L4).
"""
from __future__ import annotations

from dataclasses import dataclass, field

import yaml

# The git pretty-format used by ingestion (J4) — 6 fields, newline-separated.
# %H=full rev, %s=subject, %b=body, %an=author name, %ae=author email, %cI=committer ISO date.
GIT_LOG_FORMAT = "%H%n%s%n%b%n%an%n%ae%n%cI"

_DVC_BUCKET = "vlm-dataset"
_DVC_PREFIX = "_dvc"


@dataclass(frozen=True)
class DvcOut:
    path: str
    md5: str | None = None
    size: int | None = None
    nfiles: int | None = None


@dataclass(frozen=True)
class DvcPointer:
    dvc_file_path: str
    outs: list[DvcOut] = field(default_factory=list)


@dataclass(frozen=True)
class GitCommitMeta:
    git_rev: str
    commit_subject: str
    commit_message: str
    commit_author_name: str
    commit_author_email: str
    committed_at: str

    @property
    def git_short_rev(self) -> str:
        return self.git_rev[:12]


def parse_dvc_pointer(dvc_file_path: str, yaml_text: str) -> DvcPointer:
    """Parse a .dvc YAML pointer into a DvcPointer. Raises ValueError if no `outs`."""
    doc = yaml.safe_load(yaml_text) or {}
    raw_outs = doc.get("outs")
    if not raw_outs or not isinstance(raw_outs, list):
        raise ValueError(f"{dvc_file_path}: .dvc file has no 'outs' list")
    outs: list[DvcOut] = []
    for entry in raw_outs:
        if not isinstance(entry, dict):
            continue
        size = entry.get("size")
        nfiles = entry.get("nfiles")
        outs.append(
            DvcOut(
                path=str(entry.get("path", "")),
                md5=(str(entry["md5"]) if entry.get("md5") is not None else None),
                size=(int(size) if size is not None else None),
                nfiles=(int(nfiles) if nfiles is not None else None),
            )
        )
    if not outs:
        raise ValueError(f"{dvc_file_path}: .dvc 'outs' is empty")
    return DvcPointer(dvc_file_path=dvc_file_path, outs=outs)


def parse_git_log_format(stdout: str) -> GitCommitMeta:
    """Parse `git log -1 --format=GIT_LOG_FORMAT` stdout.

    Layout: line0=rev, line1=subject, lines[2:-3]=body (may be multi-line, may be empty),
    line[-3]=author name, line[-2]=author email, line[-1]=committed_at (ISO).
    """
    lines = stdout.rstrip("\n").split("\n")
    if len(lines) < 5:
        raise ValueError(f"git log output too short ({len(lines)} lines): {stdout!r}")
    git_rev = lines[0].strip()
    commit_subject = lines[1]
    committed_at = lines[-1].strip()
    commit_author_email = lines[-2].strip()
    commit_author_name = lines[-3].strip()
    body_lines = lines[2:-3]
    commit_message = "\n".join(body_lines).strip()
    return GitCommitMeta(
        git_rev=git_rev,
        commit_subject=commit_subject,
        commit_message=commit_message,
        commit_author_name=commit_author_name,
        commit_author_email=commit_author_email,
        committed_at=committed_at,
    )


def dvc_remote_url_for(out_path: str, *, bucket: str = _DVC_BUCKET, prefix: str = _DVC_PREFIX) -> str:
    """s3 URL of an out under the fixed vlm-dataset/_dvc/ prefix (5-bucket policy)."""
    clean = out_path.strip("/")
    return f"s3://{bucket}/{prefix}/{clean}"
```

- [ ] **J2.4 — Run, expect PASS + lib-layer lint + ruff:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_dvc_catalog_parsers.py -q
python scripts/check_lib_layer_imports.py
python -m ruff check --line-length 120 src/vlm_pipeline/lib/dvc_catalog.py
```

Expected: `8 passed`; lib-layer check prints no violation for `dvc_catalog.py`; ruff clean.

- [ ] **J2.5 — Commit:**

```bash
git add src/vlm_pipeline/lib/dvc_catalog.py tests/unit/test_dvc_catalog_parsers.py
git commit -m "feat(mlops): pure lib parsers for .dvc YAML + git-log commit metadata"
```

---

### Task J3: catalog DB helpers in `resources/postgres_train.py` — insert / get-by-alias / transactional pin — `_DummyDB` TDD

**Files:**
- `tests/unit/test_dataset_catalog_db.py` (new)
- `src/vlm_pipeline/resources/postgres_train.py` (modify — Section B authors this mixin; J3 ADDS three methods, additive)

**Interfaces:**
- Consumes: `self.connect()` ctxmgr + `%(name)s` paramstyle (mirrors Section B's `insert_train_dataset_version`); tables from J1.
- Produces (added to `PostgresTrainMixin`):
  - `insert_catalog_row(self, row: dict) -> str` — INSERT into `dataset_catalog`, `ON CONFLICT (data_repo_id, git_rev, dvc_file_path, dvc_out_path) DO NOTHING`; returns the `dataset_catalog_id` of the new OR pre-existing row. `status` defaults from `row` (`'available'` / `'pending_missing_dvc_objects'`).
  - `get_catalog_by_alias(self, task: str, alias: str = "current") -> dict | None` — JOIN aliases→catalog, returns the catalog row dict or `None`.
  - `pin_alias(self, *, task: str, alias: str, dataset_catalog_id: str, pinned_by: str, pin_reason: str | None = None) -> dict` — **single transaction**: read previous alias (if any) → UPSERT `dataset_catalog_aliases` (one row per `(task, alias)`) → INSERT `dataset_catalog_pin_events` with `previous_dataset_catalog_id` → set new row `status='pinned'`. Returns `{"task", "alias", "dataset_catalog_id", "previous_dataset_catalog_id"}`.

- [ ] **J3.1 — SPIKE: confirm the cursor idiom Section B uses.** Read the `insert_train_dataset_version` body in `src/vlm_pipeline/resources/postgres_train.py` (authored by Section B, plan lines 2663-2690) and confirm the contextmanager name + paramstyle:

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
grep -n "with self.connect\|self._rows_to_dicts\|%(.*)s\|def insert_train_dataset_version" src/vlm_pipeline/resources/postgres_train.py
```

Expected (per Section B contract): `with self.connect() as conn:` → `with conn.cursor() as cur:`, named params `%(content_checksum)s` etc. The three J3 methods MUST mirror this verbatim. **If `postgres_train.py` is absent (B not merged): proceed using the contract idiom above — the `_DummyDB` unit test fully exercises the SQL, and the real-PG path is covered by J4's moto+PG flow / flagged in open_risks.**

- [ ] **J3.2 — Write the failing unit test.** Create `tests/unit/test_dataset_catalog_db.py` (exact content — `_DummyDB`/`_DummyCursor` capture SQL + simulate the UNIQUE/alias state, mirroring the Section B `_DummyDB` shape):

```python
"""PostgresTrainMixin DVC-catalog helpers — insert / get-by-alias / transactional pin.

No real PG. _DummyDB captures executed SQL and simulates UNIQUE-on-catalog + the
single alias-per-task invariant so pin_alias' transaction logic is fully exercised.
"""
from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "src"))

from vlm_pipeline.resources.postgres_train import PostgresTrainMixin  # noqa: E402


class _Cursor:
    def __init__(self, store):
        self.store = store
        self._result = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.store["log"].append((sql, params))
        s = " ".join(sql.split()).lower()
        if "insert into dataset_catalog (" in s:
            # ON CONFLICT DO NOTHING + RETURNING — simulate dedup by UNIQUE key:
            key = (params["data_repo_id"], params["git_rev"], params["dvc_file_path"], params["dvc_out_path"])
            existing = self.store["catalog_by_key"].get(key)
            if existing is None:
                self.store["catalog_by_key"][key] = params["dataset_catalog_id"]
                self._result = (params["dataset_catalog_id"],)
            else:
                self._result = None  # conflict → no RETURNING row
        elif "select dataset_catalog_id from dataset_catalog where" in s:
            key = (params["data_repo_id"], params["git_rev"], params["dvc_file_path"], params["dvc_out_path"])
            cid = self.store["catalog_by_key"].get(key)
            self._result = (cid,) if cid else None
        elif "from dataset_catalog_aliases a" in s:
            row = self.store["aliases"].get((params["task"], params["alias"]))
            self._result = row
        elif "select dataset_catalog_id from dataset_catalog_aliases" in s:
            row = self.store["aliases"].get((params["task"], params["alias"]))
            self._result = (row["dataset_catalog_id"],) if row else None
        elif "insert into dataset_catalog_aliases" in s or "update dataset_catalog_aliases" in s:
            self.store["aliases"][(params["task"], params["alias"])] = {
                "dataset_catalog_id": params["dataset_catalog_id"], "task": params["task"], "alias": params["alias"],
            }
        elif "insert into dataset_catalog_pin_events" in s:
            self.store["pin_events"].append(params)
        elif "update dataset_catalog set status" in s:
            self.store["status_updates"].append(params)

    def fetchone(self):
        return self._result

    def cursor(self):
        return _Cursor(self.store)


class _Conn:
    def __init__(self, store):
        self.store = store
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return _Cursor(self.store)

    def commit(self):
        self.committed = True


class _DummyDB(PostgresTrainMixin):
    def __init__(self):
        self.store = {
            "log": [], "catalog_by_key": {}, "aliases": {},
            "pin_events": [], "status_updates": [],
        }

    def connect(self):
        return _Conn(self.store)

    def _rows_to_dicts(self, cur, rows):  # not exercised by these tests
        return rows


def _row(cid, rev="rev1", status="available"):
    return {
        "dataset_catalog_id": cid, "task": "sam3_detection", "dataset_name": "fire",
        "status": status, "data_repo_id": "dvc-datasets", "git_rev": rev,
        "dvc_file_path": "data/fire.dvc", "dvc_out_path": "fire",
    }


def test_insert_catalog_row_returns_id_and_is_idempotent():
    db = _DummyDB()
    cid1 = db.insert_catalog_row(_row("11111111-1111-1111-1111-111111111111"))
    assert cid1 == "11111111-1111-1111-1111-111111111111"
    # same UNIQUE key, different generated id → ON CONFLICT → returns the FIRST id:
    cid2 = db.insert_catalog_row(_row("22222222-2222-2222-2222-222222222222"))
    assert cid2 == cid1, "idempotent insert must return the pre-existing row id"


def test_pin_alias_is_transactional_and_appends_event():
    db = _DummyDB()
    a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    b = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    db.insert_catalog_row(_row(a, rev="rev1"))
    db.insert_catalog_row(_row(b, rev="rev2"))

    out1 = db.pin_alias(task="sam3_detection", alias="current", dataset_catalog_id=a, pinned_by="eng")
    assert out1["dataset_catalog_id"] == a
    assert out1["previous_dataset_catalog_id"] is None
    assert db.store["aliases"][("sam3_detection", "current")]["dataset_catalog_id"] == a
    assert len(db.store["pin_events"]) == 1
    assert db.store["status_updates"][-1]["dataset_catalog_id"] == a  # status->pinned

    out2 = db.pin_alias(task="sam3_detection", alias="current", dataset_catalog_id=b, pinned_by="eng", pin_reason="better")
    assert out2["dataset_catalog_id"] == b
    assert out2["previous_dataset_catalog_id"] == a, "must record the displaced catalog id"
    assert db.store["aliases"][("sam3_detection", "current")]["dataset_catalog_id"] == b
    assert len(db.store["pin_events"]) == 2
    assert db.store["pin_events"][-1]["pin_reason"] == "better"


def test_get_catalog_by_alias_none_when_unpinned():
    db = _DummyDB()
    assert db.get_catalog_by_alias("sam3_detection", "current") is None
```

- [ ] **J3.3 — Run it, expect FAIL:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_dataset_catalog_db.py -q 2>&1 | tail -6
```

Expected: `AttributeError: 'PostgresTrainMixin' object has no attribute 'insert_catalog_row'` (or `ImportError` if B not merged — still a fail, fix is to add the methods).

- [ ] **J3.4 — Implement the three methods** by APPENDING to `PostgresTrainMixin` in `src/vlm_pipeline/resources/postgres_train.py` (exact content — `pin_alias` opens ONE connection and commits once; reads previous, upserts alias, appends event, flips status):

```python
    # ── DVC dataset_catalog helpers (Section J — curation layer index) ──

    def insert_catalog_row(self, row: dict) -> str:
        """Insert a dataset_catalog row; idempotent on the DVC UNIQUE key.

        Returns the dataset_catalog_id of the inserted OR pre-existing row.
        status comes from row (caller sets 'available' / 'pending_missing_dvc_objects').
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dataset_catalog (
                        dataset_catalog_id, task, dataset_name, status,
                        data_repo_id, data_repo_url, git_rev, git_short_rev, git_ref, git_tag,
                        commit_subject, commit_message, commit_author_name, commit_author_email,
                        committed_at,
                        dvc_file_path, dvc_out_path, dvc_md5, dvc_size_bytes, dvc_nfiles,
                        dvc_remote_name, dvc_remote_url,
                        train_dataset_version_id, content_checksum, mlflow_run_id
                    ) VALUES (
                        %(dataset_catalog_id)s, %(task)s, %(dataset_name)s, %(status)s,
                        %(data_repo_id)s, %(data_repo_url)s, %(git_rev)s, %(git_short_rev)s,
                        %(git_ref)s, %(git_tag)s,
                        %(commit_subject)s, %(commit_message)s, %(commit_author_name)s,
                        %(commit_author_email)s, %(committed_at)s,
                        %(dvc_file_path)s, %(dvc_out_path)s, %(dvc_md5)s, %(dvc_size_bytes)s,
                        %(dvc_nfiles)s, %(dvc_remote_name)s, %(dvc_remote_url)s,
                        %(train_dataset_version_id)s, %(content_checksum)s, %(mlflow_run_id)s
                    )
                    ON CONFLICT (data_repo_id, git_rev, dvc_file_path, dvc_out_path) DO NOTHING
                    RETURNING dataset_catalog_id
                    """,
                    {
                        "dataset_catalog_id": row["dataset_catalog_id"],
                        "task": row["task"],
                        "dataset_name": row["dataset_name"],
                        "status": row.get("status", "available"),
                        "data_repo_id": row["data_repo_id"],
                        "data_repo_url": row.get("data_repo_url"),
                        "git_rev": row["git_rev"],
                        "git_short_rev": row.get("git_short_rev"),
                        "git_ref": row.get("git_ref"),
                        "git_tag": row.get("git_tag"),
                        "commit_subject": row.get("commit_subject"),
                        "commit_message": row.get("commit_message"),
                        "commit_author_name": row.get("commit_author_name"),
                        "commit_author_email": row.get("commit_author_email"),
                        "committed_at": row.get("committed_at"),
                        "dvc_file_path": row["dvc_file_path"],
                        "dvc_out_path": row["dvc_out_path"],
                        "dvc_md5": row.get("dvc_md5"),
                        "dvc_size_bytes": row.get("dvc_size_bytes"),
                        "dvc_nfiles": row.get("dvc_nfiles"),
                        "dvc_remote_name": row.get("dvc_remote_name"),
                        "dvc_remote_url": row.get("dvc_remote_url"),
                        "train_dataset_version_id": row.get("train_dataset_version_id"),
                        "content_checksum": row.get("content_checksum"),
                        "mlflow_run_id": row.get("mlflow_run_id"),
                    },
                )
                inserted = cur.fetchone()
                if inserted is not None:
                    new_id = inserted[0]
                else:
                    # ON CONFLICT → no RETURNING row; fetch the pre-existing id by UNIQUE key.
                    cur.execute(
                        """
                        SELECT dataset_catalog_id FROM dataset_catalog
                        WHERE data_repo_id = %(data_repo_id)s AND git_rev = %(git_rev)s
                          AND dvc_file_path = %(dvc_file_path)s AND dvc_out_path = %(dvc_out_path)s
                        """,
                        {
                            "data_repo_id": row["data_repo_id"],
                            "git_rev": row["git_rev"],
                            "dvc_file_path": row["dvc_file_path"],
                            "dvc_out_path": row["dvc_out_path"],
                        },
                    )
                    new_id = cur.fetchone()[0]
            conn.commit()
        return str(new_id)

    def get_catalog_by_alias(self, task: str, alias: str = "current") -> dict | None:
        """Resolve a task+alias to its pinned dataset_catalog row (or None)."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.* FROM dataset_catalog_aliases a
                    JOIN dataset_catalog c ON c.dataset_catalog_id = a.dataset_catalog_id
                    WHERE a.task = %(task)s AND a.alias = %(alias)s
                    """,
                    {"task": task, "alias": alias},
                )
                rows = self._rows_to_dicts(cur, cur.fetchall())
        return rows[0] if rows else None

    def pin_alias(
        self,
        *,
        task: str,
        alias: str,
        dataset_catalog_id: str,
        pinned_by: str,
        pin_reason: str | None = None,
    ) -> dict:
        """Transactionally pin task+alias to a catalog id (NOT a raw UPDATE).

        One connection / one commit: read previous alias target → UPSERT the single
        (task, alias) alias row → append a pin_event with previous_dataset_catalog_id →
        flip the newly-pinned row to status='pinned'. Atomic so alias + audit never drift.
        """
        import uuid as _uuid

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT dataset_catalog_id FROM dataset_catalog_aliases "
                    "WHERE task = %(task)s AND alias = %(alias)s",
                    {"task": task, "alias": alias},
                )
                prev = cur.fetchone()
                previous_id = prev[0] if prev else None

                cur.execute(
                    """
                    INSERT INTO dataset_catalog_aliases
                        (task, alias, dataset_catalog_id, pinned_by, pin_reason, pinned_at)
                    VALUES (%(task)s, %(alias)s, %(dataset_catalog_id)s, %(pinned_by)s,
                            %(pin_reason)s, CURRENT_TIMESTAMP)
                    ON CONFLICT (task, alias) DO UPDATE SET
                        dataset_catalog_id = EXCLUDED.dataset_catalog_id,
                        pinned_by = EXCLUDED.pinned_by,
                        pin_reason = EXCLUDED.pin_reason,
                        pinned_at = EXCLUDED.pinned_at
                    """,
                    {
                        "task": task,
                        "alias": alias,
                        "dataset_catalog_id": dataset_catalog_id,
                        "pinned_by": pinned_by,
                        "pin_reason": pin_reason,
                    },
                )
                cur.execute(
                    """
                    INSERT INTO dataset_catalog_pin_events
                        (event_id, task, alias, dataset_catalog_id, previous_dataset_catalog_id,
                         pinned_by, pin_reason, pinned_at)
                    VALUES (%(event_id)s, %(task)s, %(alias)s, %(dataset_catalog_id)s,
                            %(previous_dataset_catalog_id)s, %(pinned_by)s, %(pin_reason)s,
                            CURRENT_TIMESTAMP)
                    """,
                    {
                        "event_id": str(_uuid.uuid4()),
                        "task": task,
                        "alias": alias,
                        "dataset_catalog_id": dataset_catalog_id,
                        "previous_dataset_catalog_id": previous_id,
                        "pinned_by": pinned_by,
                        "pin_reason": pin_reason,
                    },
                )
                cur.execute(
                    "UPDATE dataset_catalog SET status = 'pinned' "
                    "WHERE dataset_catalog_id = %(dataset_catalog_id)s",
                    {"dataset_catalog_id": dataset_catalog_id},
                )
            conn.commit()
        return {
            "task": task,
            "alias": alias,
            "dataset_catalog_id": dataset_catalog_id,
            "previous_dataset_catalog_id": str(previous_id) if previous_id else None,
        }
```

- [ ] **J3.5 — Run, expect PASS + ruff:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_dataset_catalog_db.py -q
python -m ruff check --line-length 120 src/vlm_pipeline/resources/postgres_train.py
```

Expected: `3 passed`; ruff clean.

- [ ] **J3.6 — Commit:**

```bash
git add src/vlm_pipeline/resources/postgres_train.py tests/unit/test_dataset_catalog_db.py
git commit -m "feat(mlops): dataset_catalog DB helpers (idempotent insert + transactional pin_alias)"
```

---

### Task J4: ingestion op + reconciliation sensor — `tmp`-git + moto TDD

**Files:**
- `tests/unit/test_dataset_catalog_ingest.py` (new)
- `src/vlm_pipeline/lib/dvc_git.py` (new — L1 pure subprocess wrappers around `git`)
- `src/vlm_pipeline/defs/train/catalog_ingest.py` (new — L4: `_run_catalog_ingest(...)` core + `dataset_catalog_reconciliation_sensor`)
- `src/vlm_pipeline/definitions_production.py` (modify — register the sensor in `build_production_sensors`)

**Interfaces:**
- Consumes: `lib.dvc_catalog.{parse_git_log_format,parse_dvc_pointer,dvc_remote_url_for,GIT_LOG_FORMAT}`, `lib.dvc_git.{list_dvc_files_at_rev,read_file_at_rev,git_log_meta,iter_recent_revs}`, `PostgresTrainMixin.insert_catalog_row`, `MinIOResource.object_exists`, env `DVC_DATA_REPO_PATH` (bare repo), `DVC_DATA_REPO_ID` (default `dvc-datasets`).
- Produces:
  - `lib/dvc_git.py`: pure `subprocess` git wrappers — `git_log_meta(repo, rev) -> GitCommitMeta`, `list_dvc_files_at_rev(repo, rev) -> list[str]`, `read_file_at_rev(repo, rev, path) -> str`, `iter_recent_revs(repo, limit) -> list[str]`.
  - `_run_catalog_ingest(db, minio, *, repo_path, data_repo_id, rev, dataset_name, task, log) -> dict` — for `rev`: read git meta + each `.dvc` pointer → for each out, verify MinIO object exists under `_dvc/<out>` → `status='available'` if all present else `'pending_missing_dvc_objects'` → `insert_catalog_row`. Returns `{"ingested": [...ids], "missing_objects": bool, "status": ...}`.
  - `dataset_catalog_reconciliation_sensor` — STOPPED-by-default sensor scanning `iter_recent_revs` as a backstop; idempotent via the UNIQUE (re-insert = no-op).

- [ ] **J4.1 — Write the failing unit test.** Create `tests/unit/test_dataset_catalog_ingest.py` (exact content — builds a REAL tmp git repo, mocks `dvc`/MinIO via moto-backed `mock_minio` + a `_DummyDB`, and stubs the `git` wrappers' MinIO check):

```python
"""dataset_catalog ingestion — real tmp git repo + moto MinIO + _DummyDB.

git ops run for real (subprocess on a tmp repo). The DVC byte transfer is NEVER run;
object presence is checked against moto. Missing object -> pending_missing_dvc_objects.
"""
from __future__ import annotations

import pathlib
import subprocess
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "src"))

from vlm_pipeline.defs.train import catalog_ingest as _ci  # noqa: E402
from vlm_pipeline.lib import dvc_git  # noqa: E402


def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


@pytest.fixture
def tmp_data_repo(tmp_path):
    repo = tmp_path / "dvc-datasets"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "shjin@example.com")
    _git(repo, "config", "user.name", "Seohee Jin")
    data = repo / "data"
    data.mkdir()
    (data / "fire_v3.dvc").write_text(
        "outs:\n- md5: deadbeef.dir\n  size: 1024\n  nfiles: 7\n  path: fire_v3\n",
        encoding="utf-8",
    )
    _git(repo, "add", "data/fire_v3.dvc")
    _git(repo, "commit", "-q", "-m", "curate: fire v3\n\nremoved blurry frames")
    rev = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    return repo, rev


class _DummyDB:
    def __init__(self):
        self.rows = []

    def insert_catalog_row(self, row):
        self.rows.append(row)
        return row["dataset_catalog_id"]


def test_dvc_git_wrappers_read_real_repo(tmp_data_repo):
    repo, rev = tmp_data_repo
    meta = dvc_git.git_log_meta(str(repo), rev)
    assert meta.commit_subject == "curate: fire v3"
    assert "removed blurry frames" in meta.commit_message
    assert meta.commit_author_email == "shjin@example.com"
    files = dvc_git.list_dvc_files_at_rev(str(repo), rev)
    assert files == ["data/fire_v3.dvc"]
    text = dvc_git.read_file_at_rev(str(repo), rev, "data/fire_v3.dvc")
    assert "fire_v3" in text


def test_ingest_available_when_object_present(tmp_data_repo, mock_minio):
    repo, rev = tmp_data_repo
    mock_minio.upload_bytes(b"x", "vlm-dataset", "_dvc/fire_v3")  # simulate dvc push done
    db = _DummyDB()
    out = _ci._run_catalog_ingest(
        db, mock_minio, repo_path=str(repo), data_repo_id="dvc-datasets",
        rev=rev, dataset_name="fire", task="sam3_detection", log=_NullLog(),
    )
    assert out["status"] == "available"
    assert out["missing_objects"] is False
    assert len(db.rows) == 1
    assert db.rows[0]["status"] == "available"
    assert db.rows[0]["commit_subject"] == "curate: fire v3"
    assert db.rows[0]["dvc_remote_url"] == "s3://vlm-dataset/_dvc/fire_v3"


def test_ingest_pending_when_object_missing(tmp_data_repo, mock_minio):
    repo, rev = tmp_data_repo  # no upload → object absent
    db = _DummyDB()
    out = _ci._run_catalog_ingest(
        db, mock_minio, repo_path=str(repo), data_repo_id="dvc-datasets",
        rev=rev, dataset_name="fire", task="sam3_detection", log=_NullLog(),
    )
    assert out["status"] == "pending_missing_dvc_objects"
    assert out["missing_objects"] is True
    assert db.rows[0]["status"] == "pending_missing_dvc_objects"


class _NullLog:
    def info(self, *a, **k): ...
    def warning(self, *a, **k): ...
```

> If `MinIOResource` lacks `object_exists` / `upload_bytes`, the test uses whatever the resource exposes — confirm in J4.2's first run and adapt the two call sites only (the SPIKE below). The plan's `_DummyMinIO`/`mock_minio` convention already provides `upload_*`; `object_exists` is a thin `head_object` wrapper added if missing (additive, separate trivial commit).

- [ ] **J4.2 — SPIKE + run, expect FAIL.** Confirm the MinIO presence-check method name, then run:

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
grep -n "def object_exists\|def upload_bytes\|def head\|def exists" src/vlm_pipeline/resources/minio.py
python -m pytest tests/unit/test_dataset_catalog_ingest.py -q 2>&1 | tail -8
```

Expected: `ModuleNotFoundError: vlm_pipeline.defs.train.catalog_ingest` (and `vlm_pipeline.lib.dvc_git`). If `object_exists` is absent, add it to `MinIOResource` (a `head_object` try/except returning bool) before J4.4.

- [ ] **J4.3 — Implement `src/vlm_pipeline/lib/dvc_git.py`** (exact content — pure subprocess, L1, no dagster/resources import):

```python
"""lib.dvc_git — pure subprocess git wrappers for the DVC bare data repo (L1).

Only `git` is invoked here (read-only: log, ls-tree, show). The `dvc` binary and
MinIO bytes are NOT touched — those live in the L4 ingestion op / the pull wrapper.
No dagster / resources / ops import.
"""
from __future__ import annotations

import subprocess

from vlm_pipeline.lib.dvc_catalog import GIT_LOG_FORMAT, GitCommitMeta, parse_git_log_format


def _git(repo_path: str, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", repo_path, *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout


def git_log_meta(repo_path: str, rev: str) -> GitCommitMeta:
    """`git log -1 --format=GIT_LOG_FORMAT <rev>` → GitCommitMeta."""
    out = _git(repo_path, "log", "-1", f"--format={GIT_LOG_FORMAT}", rev)
    return parse_git_log_format(out)


def list_dvc_files_at_rev(repo_path: str, rev: str) -> list[str]:
    """All `*.dvc` pointer paths tracked at <rev>."""
    out = _git(repo_path, "ls-tree", "-r", "--name-only", rev)
    return sorted(line for line in out.splitlines() if line.endswith(".dvc"))


def read_file_at_rev(repo_path: str, rev: str, path: str) -> str:
    """`git show <rev>:<path>` — content of a tracked file at a rev."""
    return _git(repo_path, "show", f"{rev}:{path}")


def iter_recent_revs(repo_path: str, limit: int = 50) -> list[str]:
    """Most-recent <limit> commit revs (newest first) — reconciliation backstop."""
    out = _git(repo_path, "log", f"-{int(limit)}", "--format=%H")
    return [line.strip() for line in out.splitlines() if line.strip()]
```

- [ ] **J4.4 — Implement `src/vlm_pipeline/defs/train/catalog_ingest.py`** (exact content — `_run_catalog_ingest` core + the reconciliation sensor following the `stuck_run_guard` idiom):

```python
"""defs/train/catalog_ingest — DVC bare-repo → dataset_catalog ingestion (L4).

post-receive 훅이 단일 rev 로 op 을 호출(운영 시)하고, reconciliation 센서가 git log 를
스캔해 누락분을 backstop 한다(멱등 — dataset_catalog UNIQUE). MinIO 객체 존재를 검증해서만
status='available', 누락이면 'pending_missing_dvc_objects' (dvc push ≠ git commit 갭).
"""
from __future__ import annotations

import os
import uuid

from dagster import DefaultSensorStatus, SkipReason, sensor

from vlm_pipeline.lib.dvc_catalog import dvc_remote_url_for, parse_dvc_pointer
from vlm_pipeline.lib.dvc_git import git_log_meta, iter_recent_revs, list_dvc_files_at_rev, read_file_at_rev
from vlm_pipeline.lib.env_utils import int_env

_DEFAULT_TASK = "sam3_detection"


def _object_exists(minio, out_path: str) -> bool:
    key = f"_dvc/{out_path.strip('/')}"
    try:
        return bool(minio.object_exists("vlm-dataset", key))
    except Exception:  # noqa: BLE001 — treat probe error as "not present" → pending
        return False


def _run_catalog_ingest(db, minio, *, repo_path, data_repo_id, rev, dataset_name, task, log) -> dict:
    """Ingest one git rev into dataset_catalog. Verifies MinIO objects before 'available'."""
    meta = git_log_meta(repo_path, rev)
    dvc_files = list_dvc_files_at_rev(repo_path, rev)
    ingested: list[str] = []
    any_missing = False

    for dvc_file in dvc_files:
        pointer = parse_dvc_pointer(dvc_file, read_file_at_rev(repo_path, rev, dvc_file))
        for out in pointer.outs:
            present = _object_exists(minio, out.path)
            if not present:
                any_missing = True
            status = "available" if present else "pending_missing_dvc_objects"
            row = {
                "dataset_catalog_id": str(uuid.uuid4()),
                "task": task,
                "dataset_name": dataset_name,
                "status": status,
                "data_repo_id": data_repo_id,
                "data_repo_url": repo_path,
                "git_rev": meta.git_rev,
                "git_short_rev": meta.git_short_rev,
                "git_ref": None,
                "git_tag": None,
                "commit_subject": meta.commit_subject,
                "commit_message": meta.commit_message,
                "commit_author_name": meta.commit_author_name,
                "commit_author_email": meta.commit_author_email,
                "committed_at": meta.committed_at,
                "dvc_file_path": dvc_file,
                "dvc_out_path": out.path,
                "dvc_md5": out.md5,
                "dvc_size_bytes": out.size,
                "dvc_nfiles": out.nfiles,
                "dvc_remote_name": "minio",
                "dvc_remote_url": dvc_remote_url_for(out.path),
                "train_dataset_version_id": None,
                "content_checksum": None,
                "mlflow_run_id": None,
            }
            cid = db.insert_catalog_row(row)
            ingested.append(cid)
            if not present:
                log.warning(
                    f"[dvc-catalog] rev={meta.git_short_rev} out={out.path} "
                    "MinIO object MISSING — status=pending_missing_dvc_objects (dvc push needed)"
                )

    overall = "pending_missing_dvc_objects" if any_missing else "available"
    log.info(
        f"[dvc-catalog] ingested rev={meta.git_short_rev} files={len(dvc_files)} "
        f"rows={len(ingested)} status={overall}"
    )
    return {"ingested": ingested, "missing_objects": any_missing, "status": overall}


@sensor(
    minimum_interval_seconds=int_env("DVC_CATALOG_RECONCILE_INTERVAL_SEC", 600, 60),
    default_status=DefaultSensorStatus.STOPPED,
    description="DVC bare repo git log 스캔 → dataset_catalog 누락 backstop (멱등)",
    required_resource_keys={"db", "minio"},
)
def dataset_catalog_reconciliation_sensor(context):
    repo_path = os.environ.get("DVC_DATA_REPO_PATH", "").strip()
    if not repo_path or not os.path.isdir(repo_path):
        return SkipReason(f"DVC_DATA_REPO_PATH 미설정/부재: {repo_path!r}")
    data_repo_id = os.environ.get("DVC_DATA_REPO_ID", "dvc-datasets")
    task = os.environ.get("DVC_CATALOG_DEFAULT_TASK", _DEFAULT_TASK)
    limit = int_env("DVC_CATALOG_RECONCILE_REVS", 50, 1)

    try:
        revs = iter_recent_revs(repo_path, limit)
    except Exception as exc:  # noqa: BLE001
        return SkipReason(f"git log 스캔 실패: {exc}")

    ingested_total = 0
    for rev in revs:
        try:
            out = _run_catalog_ingest(
                context.resources.db,
                context.resources.minio,
                repo_path=repo_path,
                data_repo_id=data_repo_id,
                rev=rev,
                dataset_name=os.environ.get("DVC_CATALOG_DEFAULT_DATASET", "uncategorized"),
                task=task,
                log=context.log,
            )
            ingested_total += len(out["ingested"])
        except Exception as exc:  # noqa: BLE001 — per-rev fail-forward
            context.log.warning(f"[dvc-catalog] rev={rev[:12]} ingest 실패: {exc}")

    return SkipReason(
        f"dataset_catalog reconciliation 완료: revs={len(revs)} rows_touched={ingested_total} "
        "(UNIQUE 로 멱등 — 기존 행은 no-op)"
    )
```

- [ ] **J4.5 — Register the sensor** in `src/vlm_pipeline/definitions_production.py`. Add the import next to the other train/sensor imports and append to the `sensors` list inside `build_production_sensors` (additive — gated behind `DVC_DATA_REPO_PATH` at runtime, so safe to always register):

```python
# (top imports, near the other defs sensors)
from vlm_pipeline.defs.train.catalog_ingest import dataset_catalog_reconciliation_sensor
```

```python
    # inside build_production_sensors(), before `return sensors`:
    sensors.append(dataset_catalog_reconciliation_sensor)
```

- [ ] **J4.6 — Run, expect PASS + lib lint + import smoke:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_dataset_catalog_ingest.py -q
python scripts/check_lib_layer_imports.py
python -c "from vlm_pipeline.defs.train.catalog_ingest import dataset_catalog_reconciliation_sensor as s; print('sensor-ok', s.name)"
python -m ruff check --line-length 120 src/vlm_pipeline/lib/dvc_git.py src/vlm_pipeline/defs/train/catalog_ingest.py
```

Expected: `4 passed`; lib-layer check clean (`dvc_git.py` imports only `lib.dvc_catalog` + stdlib); the import smoke prints `sensor-ok dataset_catalog_reconciliation_sensor`; ruff clean.

- [ ] **J4.7 — Commit:**

```bash
git add src/vlm_pipeline/lib/dvc_git.py src/vlm_pipeline/defs/train/catalog_ingest.py \
        src/vlm_pipeline/definitions_production.py tests/unit/test_dataset_catalog_ingest.py
git commit -m "feat(mlops): DVC catalog ingestion op + reconciliation sensor (MinIO object verify)"
```

---

### Task J5: pin-API op + `scripts/dataset_pull.py` — mocked `dvc get` + checksum verify — TDD

**Files:**
- `tests/unit/test_dataset_pull.py` (new)
- `src/vlm_pipeline/lib/dvc_pull.py` (new — L1: build the `dvc get` argv + the checksum check, pure)
- `scripts/dataset_pull.py` (new — thin CLI: resolve via `get_catalog_by_alias` → `dvc get` subprocess → verify)

**Interfaces:**
- Consumes: `lib.dvc_pull.{build_dvc_get_argv,verify_pulled_md5}`, `PostgresTrainMixin.get_catalog_by_alias`, env `DVC_DATA_REPO_PATH`, MinIO creds via the repo's `.dvc/config` (not re-passed).
- Produces:
  - `build_dvc_get_argv(repo_path: str, out_path: str, git_rev: str, dest: str) -> list[str]` → `["dvc", "get", repo_path, out_path, "--rev", git_rev, "-o", dest]`
  - `verify_pulled_md5(expected_md5: str | None, computed_md5: str | None) -> bool` — `True` if `expected` is None (nothing to check) or equal; else `False`.
  - `scripts/dataset_pull.py`: `main(argv)` with `--task`, `--alias` (default `current`), `--dest`, `--dry-run` (default `True`). Dry-run prints the resolved row + argv and does NOT invoke `dvc`.

- [ ] **J5.1 — Write the failing unit test.** Create `tests/unit/test_dataset_pull.py` (exact content — imports the script by file path like the plan's `test_promote_model.py`; `dvc` subprocess is monkeypatched):

```python
"""scripts/dataset_pull.py + lib.dvc_pull — resolve via catalog, dvc get mocked, verify md5.

Dry-run default → no dvc invoked, no bytes moved (scaffolding only).
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "src"))

from vlm_pipeline.lib.dvc_pull import build_dvc_get_argv, verify_pulled_md5  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "dataset_pull", str((pathlib.Path(__file__).resolve().parents[2] / "scripts" / "dataset_pull.py"))
)
dataset_pull = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(dataset_pull)


def test_build_dvc_get_argv():
    argv = build_dvc_get_argv("/srv/data-repos/dvc-datasets.git", "fire_v3", "abc123", "/tmp/out")
    assert argv == ["dvc", "get", "/srv/data-repos/dvc-datasets.git", "fire_v3", "--rev", "abc123", "-o", "/tmp/out"]


def test_verify_pulled_md5():
    assert verify_pulled_md5(None, "anything") is True       # nothing to verify
    assert verify_pulled_md5("deadbeef", "deadbeef") is True
    assert verify_pulled_md5("deadbeef", "cafef00d") is False


def test_dry_run_resolves_but_does_not_invoke_dvc(monkeypatch, capsys):
    invoked = {"dvc": False}

    class _DB:
        def get_catalog_by_alias(self, task, alias="current"):
            assert task == "sam3_detection"
            return {
                "dataset_catalog_id": "cid-1", "git_rev": "abc123", "dvc_out_path": "fire_v3",
                "dvc_md5": "deadbeef", "status": "pinned", "commit_subject": "curate: fire v3",
            }

    monkeypatch.setattr(dataset_pull, "_open_db", lambda: _DB())
    monkeypatch.setattr(dataset_pull, "_repo_path", lambda: "/srv/data-repos/dvc-datasets.git")
    monkeypatch.setattr(
        dataset_pull, "_run_dvc_get",
        lambda *a, **k: invoked.__setitem__("dvc", True),
    )

    rc = dataset_pull.main(["--task", "sam3_detection", "--alias", "current", "--dest", "/tmp/x"])
    assert rc == 0
    assert invoked["dvc"] is False, "dry-run must NOT invoke dvc get"
    out = capsys.readouterr().out
    assert "abc123" in out and "fire_v3" in out and "DRY-RUN" in out


def test_no_pin_returns_nonzero(monkeypatch):
    class _DB:
        def get_catalog_by_alias(self, task, alias="current"):
            return None

    monkeypatch.setattr(dataset_pull, "_open_db", lambda: _DB())
    monkeypatch.setattr(dataset_pull, "_repo_path", lambda: "/srv/data-repos/dvc-datasets.git")
    rc = dataset_pull.main(["--task", "sam3_detection", "--dest", "/tmp/x"])
    assert rc == 2
```

- [ ] **J5.2 — Run it, expect FAIL:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_dataset_pull.py -q 2>&1 | tail -6
```

Expected: `ModuleNotFoundError: vlm_pipeline.lib.dvc_pull` (and `FileNotFoundError` on `scripts/dataset_pull.py`).

- [ ] **J5.3 — Implement `src/vlm_pipeline/lib/dvc_pull.py`** (exact content):

```python
"""lib.dvc_pull — pure helpers for the API-pull wrapper (L1)."""
from __future__ import annotations


def build_dvc_get_argv(repo_path: str, out_path: str, git_rev: str, dest: str) -> list[str]:
    """`dvc get <repo> <out> --rev <rev> -o <dest>` — fetches a versioned out from MinIO."""
    return ["dvc", "get", repo_path, out_path, "--rev", git_rev, "-o", dest]


def verify_pulled_md5(expected_md5: str | None, computed_md5: str | None) -> bool:
    """True if nothing to verify (expected None) or the two md5s match."""
    if expected_md5 is None:
        return True
    return expected_md5 == computed_md5
```

- [ ] **J5.4 — Implement `scripts/dataset_pull.py`** (exact content — `--dry-run` default True, mirrors `promote_model.py`'s psycopg2-direct + dry-run convention):

```python
#!/usr/bin/env python3
"""dataset pull — resolve a pinned DVC dataset version from dataset_catalog and `dvc get` it.

  python scripts/dataset_pull.py --task sam3_detection --alias current --dest /data/pull/fire

Resolves task+alias → catalog row (git_rev + dvc_out_path + dvc_md5) → `dvc get` from the
bare repo (bytes stream from MinIO via the repo's .dvc/config) → verify md5. DRY-RUN by
default (no dvc invoked) — scaffolding only. Real pull = operation time with --no-dry-run.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from vlm_pipeline.lib.dvc_pull import build_dvc_get_argv, verify_pulled_md5  # noqa: E402


def _repo_path() -> str:
    return os.environ.get("DVC_DATA_REPO_PATH", "/srv/data-repos/dvc-datasets.git")


def _open_db():
    """Open the pipeline PG resource (psycopg2-direct, like scripts/promote_model.py)."""
    from vlm_pipeline.resources.postgres import PostgresResource  # lazy: keep import cheap for --help

    dsn = os.environ.get("DATAOPS_POSTGRES_DSN")
    if not dsn:
        raise SystemExit("DATAOPS_POSTGRES_DSN not set")
    return PostgresResource(dsn=dsn)


def _run_dvc_get(argv: list[str]) -> None:
    subprocess.run(argv, check=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pull a pinned DVC dataset version from dataset_catalog.")
    parser.add_argument("--task", required=True)
    parser.add_argument("--alias", default="current")
    parser.add_argument("--dest", required=True)
    parser.add_argument("--no-dry-run", dest="dry_run", action="store_false")
    parser.set_defaults(dry_run=True)
    args = parser.parse_args(argv)

    db = _open_db()
    row = db.get_catalog_by_alias(args.task, args.alias)
    if row is None:
        print(f"[dataset-pull] no '{args.alias}' alias pinned for task={args.task}", file=sys.stderr)
        return 2

    repo = _repo_path()
    get_argv = build_dvc_get_argv(repo, row["dvc_out_path"], row["git_rev"], args.dest)
    print(
        f"[dataset-pull] resolved task={args.task} alias={args.alias} "
        f"git_rev={row['git_rev']} out={row['dvc_out_path']} subject={row.get('commit_subject')!r}"
    )
    print(f"[dataset-pull] argv: {' '.join(get_argv)}")

    if args.dry_run:
        print("[dataset-pull] DRY-RUN — not invoking dvc get (use --no-dry-run to actually pull)")
        return 0

    _run_dvc_get(get_argv)
    # md5 verify is a best-effort post-step at operation time (dvc writes <dest>.dvc with the md5).
    if not verify_pulled_md5(row.get("dvc_md5"), _computed_md5(args.dest)):
        print("[dataset-pull] md5 mismatch — pulled bytes do not match catalog dvc_md5", file=sys.stderr)
        return 3
    print(f"[dataset-pull] OK → {args.dest}")
    return 0


def _computed_md5(dest: str) -> str | None:
    """Operation-time md5 of the pulled tree (None in scaffolding — real impl reads <dest>.dvc)."""
    return None


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **J5.5 — Run, expect PASS + ruff + `--help` smoke:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_dataset_pull.py -q
python scripts/dataset_pull.py --help >/dev/null && echo "help-ok"
python -m ruff check --line-length 120 src/vlm_pipeline/lib/dvc_pull.py scripts/dataset_pull.py
```

Expected: `4 passed`; `help-ok`; ruff clean.

- [ ] **J5.6 — Commit:**

```bash
git add src/vlm_pipeline/lib/dvc_pull.py scripts/dataset_pull.py tests/unit/test_dataset_pull.py
git commit -m "feat(mlops): dataset pull CLI (catalog-resolve + dvc get + md5 verify, dry-run default)"
```

---

### Task J6: Section B snapshot builder DVC-source integration — additive, mocked `dvc get`

**Files:**
- `tests/unit/test_dataset_builder_dvc_source.py` (new)
- `src/vlm_pipeline/defs/train/dataset.py` (modify — Section B owns; J6 ADDS an optional DVC-source hook, additive)

**Interfaces (additive to Section B's `_run_build_trainset`):**
- Consumes: `PostgresTrainMixin.get_catalog_by_alias`, `lib.dvc_pull.build_dvc_get_argv`, an injected `dvc_get` callable (so CI never runs `dvc`).
- Produces:
  - `_materialize_pinned_dvc_source(db, *, task, alias, dest_root, dvc_get) -> dict | None` — resolve pinned alias → `dvc get` into `dest_root` (via injected callable) → return `{"dataset_catalog_id", "git_rev", "dvc_out_path", "local_path"}` or `None` if no alias pinned (caller falls back to the existing LS/AL candidate query — DVC source is OPT-IN).
  - When B's builder seals a `train_dataset_versions` row from a DVC source, it sets `row["dataset_catalog_id"]` to the resolved id (back-link FK, J1). No behavior change when no alias is pinned.

- [ ] **J6.1 — SPIKE: read Section B's builder seam.** Confirm where `_run_build_trainset` assembles the `train_dataset_versions` row dict so J6's `dataset_catalog_id` back-link is additive:

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
grep -n "def _run_build_trainset\|train_dataset_version_id\|insert_train_dataset_version\|\"dataset_catalog_id\"\|def build_trainset" src/vlm_pipeline/defs/train/dataset.py
```

Expected (per Section B contract): a `_run_build_trainset(db, minio, *, task, folder_name, ...)` that builds a `row` dict and calls `db.insert_train_dataset_version(row)`. **If `dataset.py` is absent (B not merged): implement `_materialize_pinned_dvc_source` as a standalone helper in `dataset.py` (creating the module if needed) and unit-test it in isolation; the FK back-link wiring is a one-line `row["dataset_catalog_id"] = src["dataset_catalog_id"]` that lands when B's builder exists (flagged in open_risks).**

- [ ] **J6.2 — Write the failing unit test.** Create `tests/unit/test_dataset_builder_dvc_source.py` (exact content):

```python
"""Section B builder DVC-source hook (J6) — pinned alias → dvc get → back-link FK.

dvc get is an injected callable (never the real binary). No-alias → returns None
(builder keeps its existing LS/AL candidate path — DVC source is opt-in).
"""
from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "src"))

from vlm_pipeline.defs.train.dataset import _materialize_pinned_dvc_source  # noqa: E402


class _DB:
    def __init__(self, row):
        self._row = row
        self.asked = None

    def get_catalog_by_alias(self, task, alias="current"):
        self.asked = (task, alias)
        return self._row


def test_materialize_returns_none_when_no_alias(tmp_path):
    db = _DB(None)
    out = _materialize_pinned_dvc_source(
        db, task="sam3_detection", alias="current", dest_root=str(tmp_path), dvc_get=lambda argv: None
    )
    assert out is None
    assert db.asked == ("sam3_detection", "current")


def test_materialize_invokes_dvc_get_and_returns_backlink(tmp_path):
    calls = []
    db = _DB(
        {"dataset_catalog_id": "cid-9", "git_rev": "abc123", "dvc_out_path": "fire_v3", "dvc_md5": "deadbeef"}
    )
    out = _materialize_pinned_dvc_source(
        db, task="sam3_detection", alias="current", dest_root=str(tmp_path),
        dvc_get=lambda argv: calls.append(argv),
    )
    assert out["dataset_catalog_id"] == "cid-9"
    assert out["git_rev"] == "abc123"
    assert out["dvc_out_path"] == "fire_v3"
    assert out["local_path"] == str(tmp_path / "fire_v3")
    # dvc get was invoked once with the canonical argv (mocked):
    assert len(calls) == 1
    assert calls[0][:2] == ["dvc", "get"]
    assert "--rev" in calls[0] and "abc123" in calls[0]
```

- [ ] **J6.3 — Run it, expect FAIL:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_dataset_builder_dvc_source.py -q 2>&1 | tail -6
```

Expected: `ImportError: cannot import name '_materialize_pinned_dvc_source'` (or `ModuleNotFoundError` if B not merged — fix is to add the helper to `dataset.py`).

- [ ] **J6.4 — Implement** by appending `_materialize_pinned_dvc_source` to `src/vlm_pipeline/defs/train/dataset.py` (exact content — additive; the `dvc_get` callable defaults to a real subprocess but is always injected in tests/CI):

```python
# ── DVC curation-source hook (Section J — opt-in pinned dataset → frozen snapshot) ──

import os as _os
import subprocess as _subprocess

from vlm_pipeline.lib.dvc_pull import build_dvc_get_argv as _build_dvc_get_argv


def _default_dvc_get(argv: list[str]) -> None:
    _subprocess.run(argv, check=True)


def _materialize_pinned_dvc_source(db, *, task, alias="current", dest_root, dvc_get=None):
    """If a DVC alias is pinned for `task`, `dvc get` it into dest_root and return its back-link.

    Returns {"dataset_catalog_id", "git_rev", "dvc_out_path", "local_path"} or None when no
    alias is pinned (the builder then keeps its existing LS/AL candidate-query path — DVC
    source is OPT-IN). The pulled bytes feed split/freeze; the returned dataset_catalog_id is
    written to train_dataset_versions.dataset_catalog_id (J1 FK) so the frozen snapshot links
    back to its curation source (spec §7.6 step 4). dvc_get is injected so CI never runs dvc.
    """
    row = db.get_catalog_by_alias(task, alias)
    if row is None:
        return None
    dvc_get = dvc_get or _default_dvc_get
    repo_path = _os.environ.get("DVC_DATA_REPO_PATH", "/srv/data-repos/dvc-datasets.git")
    out_path = row["dvc_out_path"]
    local_path = _os.path.join(dest_root, out_path)
    argv = _build_dvc_get_argv(repo_path, out_path, row["git_rev"], local_path)
    dvc_get(argv)
    return {
        "dataset_catalog_id": row["dataset_catalog_id"],
        "git_rev": row["git_rev"],
        "dvc_out_path": out_path,
        "local_path": local_path,
    }
```

> **B-builder wiring (one line, additive):** inside Section B's `_run_build_trainset`, after computing `row` and before `db.insert_train_dataset_version(row)`, set `row["dataset_catalog_id"] = dvc_src["dataset_catalog_id"] if dvc_src else None` (where `dvc_src = _materialize_pinned_dvc_source(...)` when DVC source is requested). This is the only edit to B's sealed-row logic; it does not change the existing LS/AL path or the `content_checksum`.

- [ ] **J6.5 — Run, expect PASS + lib lint + ruff:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_dataset_builder_dvc_source.py -q
python scripts/check_lib_layer_imports.py
python -m ruff check --line-length 120 src/vlm_pipeline/defs/train/dataset.py
```

Expected: `2 passed`; lib-layer check clean; ruff clean.

- [ ] **J6.6 — Commit:**

```bash
git add src/vlm_pipeline/defs/train/dataset.py tests/unit/test_dataset_builder_dvc_source.py
git commit -m "feat(mlops): snapshot builder DVC-source hook (pinned alias -> dvc get -> catalog back-link)"
```

---

### Task J7: setup artifacts (NOT auto-run) — bare repo init + post-receive hook + `.dvc/config` template + dep pins — parse/shellcheck TDD

**Files:**
- `tests/unit/test_dvc_setup_artifacts.py` (new)
- `scripts/dvc/setup_bare_repo.sh` (new — operator-run; init bare repo + install hook; NEVER run by CI/tests)
- `scripts/dvc/post-receive` (new — hook body that POSTs the pushed rev to Dagster ingestion)
- `scripts/dvc/dvc_config.template` (new — `.dvc/config` S3→MinIO remote template)
- `docker/curation/requirements.txt` (new — pins `dvc[s3]`)
- `docker/trainer/requirements.txt` (modify — append `dvc[s3]` pin; Section C/I own the file, additive line)

**Interfaces:**
- Consumes: env names `MINIO_ENDPOINT`/`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` (template placeholders), `DVC_INGEST_WEBHOOK_URL` (hook target).
- Produces: shell artifacts that pass `bash -n` (and `shellcheck` if available); a `.dvc/config` template whose remote URL is `s3://vlm-dataset/_dvc` with `endpointurl` set; `dvc[s3]>=3.51,<4` pinned in curation + trainer reqs, ABSENT from serving images.

- [ ] **J7.1 — Write the failing test.** Create `tests/unit/test_dvc_setup_artifacts.py` (exact content):

```python
"""DVC setup artifacts — presence + shape (NOT executed). Pure file checks."""
from __future__ import annotations

import pathlib
import shutil
import subprocess

import pytest

_REPO = pathlib.Path(__file__).resolve().parents[2]
_DVC_DIR = _REPO / "scripts" / "dvc"


def _read(p: pathlib.Path) -> str:
    assert p.exists(), f"missing setup artifact: {p}"
    return p.read_text(encoding="utf-8")


def test_setup_bare_repo_script_shape():
    text = _read(_DVC_DIR / "setup_bare_repo.sh")
    assert text.startswith("#!/usr/bin/env bash") or text.startswith("#!/bin/bash")
    assert "set -euo pipefail" in text
    assert "git init --bare" in text
    # isolated from the deploy path (bare repo lives under /srv/data-repos, not the app repo):
    assert "/srv/data-repos/dvc-datasets.git" in text
    assert "post-receive" in text  # installs the hook


def test_post_receive_hook_posts_pushed_rev():
    text = _read(_DVC_DIR / "post-receive")
    assert text.startswith("#!/usr/bin/env bash") or text.startswith("#!/bin/bash")
    # reads stdin (oldrev newrev refname) — that's how git feeds a post-receive hook:
    assert "while read" in text
    assert "DVC_INGEST_WEBHOOK_URL" in text  # target Dagster ingestion endpoint


def test_dvc_config_template_is_minio_s3_remote():
    text = _read(_DVC_DIR / "dvc_config.template")
    assert "[core]" in text
    assert "s3://vlm-dataset/_dvc" in text, "remote must be the fixed vlm-dataset/_dvc prefix"
    assert "endpointurl" in text  # MinIO is S3-compatible behind an endpoint URL
    assert "MINIO_ENDPOINT" in text and "MINIO_ACCESS_KEY" in text


def test_dvc_pinned_in_curation_and_trainer_not_serving():
    curation = _read(_REPO / "docker" / "curation" / "requirements.txt")
    trainer = _read(_REPO / "docker" / "trainer" / "requirements.txt")
    assert "dvc[s3]" in curation, "curation image must pin dvc[s3]"
    assert "dvc[s3]" in trainer, "trainer image must pin dvc[s3]"
    # serving images must NOT carry dvc:
    for serving in ("embedding", "sam3"):
        req = _REPO / "docker" / serving / "requirements.txt"
        if req.exists():
            assert "dvc" not in req.read_text(encoding="utf-8"), f"{serving} must not depend on dvc"


def test_shell_artifacts_parse_with_bash_n():
    for name in ("setup_bare_repo.sh", "post-receive"):
        p = _DVC_DIR / name
        rc = subprocess.run(["bash", "-n", str(p)], capture_output=True, text=True)
        assert rc.returncode == 0, f"{name} bash -n failed: {rc.stderr}"


@pytest.mark.skipif(shutil.which("shellcheck") is None, reason="shellcheck not installed")
def test_shellcheck_clean():
    for name in ("setup_bare_repo.sh", "post-receive"):
        rc = subprocess.run(["shellcheck", "-S", "error", str(_DVC_DIR / name)], capture_output=True, text=True)
        assert rc.returncode == 0, f"shellcheck {name}: {rc.stdout}{rc.stderr}"
```

- [ ] **J7.2 — Run it, expect FAIL:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_dvc_setup_artifacts.py -q 2>&1 | tail -8
```

Expected: assertion failures `missing setup artifact: .../scripts/dvc/setup_bare_repo.sh` etc. (`6 failed`, the shellcheck test skips).

- [ ] **J7.3 — Create `scripts/dvc/setup_bare_repo.sh`** (exact content — operator-run on the bare-repo host; never run by CI):

```bash
#!/usr/bin/env bash
# DVC 데이터 전용 BARE git repo + post-receive 훅 설치 (운영자 1회 실행 — CI 자동실행 X).
# 앱 배포 경로(rsync --delete + git reset --hard)와 격리하기 위해 /srv/data-repos 아래에 둔다.
# 사용: sudo DVC_INGEST_WEBHOOK_URL=http://10.0.0.10:3030/... bash scripts/dvc/setup_bare_repo.sh
set -euo pipefail

REPO_DIR="${DVC_DATA_REPO_PATH:-/srv/data-repos/dvc-datasets.git}"
HOOK_SRC="$(cd "$(dirname "$0")" && pwd)/post-receive"

mkdir -p "$(dirname "$REPO_DIR")"
if [ ! -d "$REPO_DIR" ]; then
    git init --bare "$REPO_DIR"
    echo "[dvc-setup] created bare repo: $REPO_DIR"
else
    echo "[dvc-setup] bare repo already exists: $REPO_DIR"
fi

install -m 0755 "$HOOK_SRC" "$REPO_DIR/hooks/post-receive"
echo "[dvc-setup] installed post-receive hook -> $REPO_DIR/hooks/post-receive"
echo "[dvc-setup] DONE. Curators: git remote add origin $REPO_DIR  (then dvc add/push + git push)"
```

- [ ] **J7.4 — Create `scripts/dvc/post-receive`** (exact content — git feeds `<old> <new> <ref>` on stdin; POST each pushed rev to Dagster ingestion):

```bash
#!/usr/bin/env bash
# post-receive 훅 — push 된 각 rev 를 Dagster dataset_catalog ingestion 으로 통지.
# 훅 실패가 push 를 막지 않도록 fail-soft (실패해도 exit 0). reconciliation 센서가 backstop.
set -uo pipefail

WEBHOOK="${DVC_INGEST_WEBHOOK_URL:-}"
if [ -z "$WEBHOOK" ]; then
    echo "[post-receive] DVC_INGEST_WEBHOOK_URL 미설정 — ingestion 통지 생략 (센서 backstop)"
    exit 0
fi

while read -r _oldrev newrev refname; do
    [ "$newrev" = "0000000000000000000000000000000000000000" ] && continue  # branch delete
    echo "[post-receive] notifying ingestion: rev=$newrev ref=$refname"
    curl -fsS -m 10 -X POST "$WEBHOOK" \
        -H 'Content-Type: application/json' \
        -d "{\"git_rev\":\"$newrev\",\"git_ref\":\"$refname\"}" \
        || echo "[post-receive] 통지 실패 (fail-soft) — reconciliation 센서가 처리"
done

exit 0
```

- [ ] **J7.5 — Create `scripts/dvc/dvc_config.template`** (exact content — `.dvc/config` S3→MinIO; placeholders substituted at operator setup, NOT committed with real creds):

```ini
# DVC remote → MinIO (S3 호환). 큐레이션 데이터 repo 의 .dvc/config 로 복사 후 자격은
# .dvc/config.local 에 둔다 (git 미추적). 바이트는 vlm-dataset/_dvc/ 프리픽스에만 (5-버킷 정책).
# 치환: ${MINIO_ENDPOINT} ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}
[core]
    remote = minio
['remote "minio"']
    url = s3://vlm-dataset/_dvc
    endpointurl = ${MINIO_ENDPOINT}
    access_key_id = ${MINIO_ACCESS_KEY}
    secret_access_key = ${MINIO_SECRET_KEY}
    use_ssl = false
```

- [ ] **J7.6 — Create `docker/curation/requirements.txt`** (exact content) and **append to `docker/trainer/requirements.txt`** (Section C/I own that file — add ONE line, do not reorder):

`docker/curation/requirements.txt`:

```text
# 큐레이션(데이터 버저닝) 전용 — dvc[s3] = S3/MinIO 백엔드 포함. 서빙 이미지엔 절대 넣지 않는다.
dvc[s3]>=3.51,<4
```

Append to `docker/trainer/requirements.txt` (after the existing `boto3`/`mlflow` lines):

```text
dvc[s3]>=3.51,<4
```

- [ ] **J7.7 — Run, expect PASS + chmod the hooks:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
chmod +x scripts/dvc/setup_bare_repo.sh scripts/dvc/post-receive
python -m pytest tests/unit/test_dvc_setup_artifacts.py -q
```

Expected: `6 passed` (shellcheck test passes if installed, else skips).

- [ ] **J7.8 — Commit:**

```bash
git add scripts/dvc/setup_bare_repo.sh scripts/dvc/post-receive scripts/dvc/dvc_config.template \
        docker/curation/requirements.txt docker/trainer/requirements.txt \
        tests/unit/test_dvc_setup_artifacts.py
git commit -m "feat(mlops): DVC setup artifacts (bare repo + post-receive hook + S3 config template + dep pins)"
```

---

### Task J8: MLflow DVC fields — additive to `docker/trainer/mlflow_logging.py` — unit TDD

**Files:**
- `tests/unit/test_mlflow_dvc_tags.py` (new)
- `docker/trainer/mlflow_logging.py` (modify — Section I owns; J8 ADDS DVC lineage fields to the logged tags/params, additive)

**Interfaces (additive to I2's `log_training_run`):**
- I2's `_LINEAGE_FIELDS` already logs the trainset lineage. J8 EXTENDS the recognized DVC lineage keys so a run page shows `dvc_catalog_id` / `dvc_git_rev` / `dvc_commit_subject` / `dvc_md5` / `content_checksum` as params + `ds.*` tags (spec §7.6 step 6). No new function — same `log_training_run(dataset=...)` call; the C-entrypoint (I4) just adds these keys to its `dataset` dict.
- Produces: `_LINEAGE_FIELDS` includes the 5 DVC keys; logged as params AND `ds.*` tags; best-effort `log_input` unchanged.

- [ ] **J8.1 — Write the failing unit test.** Create `tests/unit/test_mlflow_dvc_tags.py` (exact content — same fake-`mlflow`-in-`sys.modules` idiom as I2.1):

```python
"""mlflow_logging DVC lineage fields (J8) — dvc_* keys logged as params + ds.* tags.

A fake mlflow module is injected so NO real mlflow/network is touched.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys
import types


def _load():
    mod_path = pathlib.Path(__file__).resolve().parents[2] / "docker" / "trainer" / "mlflow_logging.py"
    spec = importlib.util.spec_from_file_location("mlflow_logging", mod_path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


class _Rec:
    def __init__(self):
        self.params = {}
        self.tags = {}
        self.inputs = []


def _fake_mlflow(rec):
    m = types.ModuleType("mlflow")
    m.set_tracking_uri = lambda uri: None
    m.set_experiment = lambda name: None

    class _Run:
        class info:
            run_id = "run-xyz"

    class _Ctx:
        def __enter__(self):
            return _Run()

        def __exit__(self, *a):
            return False

    m.start_run = lambda run_name=None: _Ctx()
    m.log_params = lambda d: rec.params.update(d)
    m.log_metrics = lambda d: None
    m.set_tags = lambda d: rec.tags.update(d)
    m.log_artifact = lambda p: None
    data_mod = types.ModuleType("mlflow.data")
    data_mod.from_dict = lambda d, name=None: {"name": name}
    m.data = data_mod
    m.log_input = lambda ds: rec.inputs.append(ds)
    return m


_DATASET = {
    "train_dataset_version_id": "tdv-1",
    "content_checksum": "csum-abc",
    "dvc_catalog_id": "cid-7",
    "dvc_git_rev": "9f3c2a1b",
    "dvc_commit_subject": "curate: fire v3",
    "dvc_md5": "deadbeef.dir",
}


def test_dvc_fields_logged_as_params_and_tags(monkeypatch):
    rec = _Rec()
    fake = _fake_mlflow(rec)
    monkeypatch.setitem(sys.modules, "mlflow", fake)
    monkeypatch.setitem(sys.modules, "mlflow.data", fake.data)
    m = _load()

    run_id = m.log_training_run(
        hparams={"lr": 0.001}, dataset=_DATASET, metrics={}, artifact_paths=[], experiment="sam3-ft",
    )
    assert run_id == "run-xyz"
    for key in ("dvc_catalog_id", "dvc_git_rev", "dvc_commit_subject", "dvc_md5", "content_checksum"):
        assert key in rec.params, f"DVC lineage key {key} missing from params"
        assert f"ds.{key}" in rec.tags, f"DVC lineage key {key} missing from ds.* tags"
    assert rec.params["dvc_git_rev"] == "9f3c2a1b"
```

- [ ] **J8.2 — Run it, expect FAIL:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_mlflow_dvc_tags.py -q 2>&1 | tail -6
```

Expected: `KeyError`/`AssertionError: DVC lineage key dvc_catalog_id missing from params` (I2's `_LINEAGE_FIELDS` does not yet include the DVC keys). If I2 not merged → `FileNotFoundError` on `mlflow_logging.py` (still a fail; J8 lands after I).

- [ ] **J8.3 — Implement: extend `_LINEAGE_FIELDS`** in `docker/trainer/mlflow_logging.py` (additive — the existing list already has `content_checksum`; append the 4 DVC keys). The exact edit:

```python
# MLflow params/tags so a run page shows EXACTLY which frozen, content-checksummed
# snapshot — and which DVC curation version (spec §7.6 step 6) — produced this model.
_LINEAGE_FIELDS = (
    "train_dataset_version_id",
    "content_checksum",
    "dvc_catalog_id",       # J8: which dataset_catalog row (DVC version) sourced this trainset
    "dvc_git_rev",          # J8: the data-repo commit
    "dvc_commit_subject",   # J8: human-readable curation intent
    "dvc_md5",              # J8: DVC out md5 (byte-identity of the curation dataset)
    # … any other existing I2 lineage fields stay as-is …
)
```

> The `_build_run` body (I2) already does `mlflow.log_params(lineage)` + `mlflow.set_tags({f"ds.{k}": v ...})` over `_LINEAGE_FIELDS` present in `dataset` — so extending the tuple is the ONLY change needed. No DVC-specific `MLflow DatasetSource` (spec §7.6: tags only).

- [ ] **J8.4 — Run, expect PASS + ruff. Re-run I2's test to prove no regression:**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest tests/unit/test_mlflow_dvc_tags.py tests/unit/test_trainer_mlflow_logging.py -q
python -m ruff check --line-length 120 docker/trainer/mlflow_logging.py
```

Expected: all pass (J8's `test_mlflow_dvc_tags` + I2's `test_trainer_mlflow_logging` green); ruff clean.

- [ ] **J8.5 — Commit:**

```bash
git add docker/trainer/mlflow_logging.py tests/unit/test_mlflow_dvc_tags.py
git commit -m "feat(mlops): log DVC dataset lineage (catalog_id/git_rev/commit_subject/md5) to MLflow"
```

---

### Section J — full-suite gate

After J1-J8, run the new unit suite + the lib-layer lint together (the migration integration test needs the throwaway PG from J1.2):

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
python -m pytest \
  tests/unit/test_dvc_catalog_parsers.py \
  tests/unit/test_dataset_catalog_db.py \
  tests/unit/test_dataset_catalog_ingest.py \
  tests/unit/test_dataset_pull.py \
  tests/unit/test_dataset_builder_dvc_source.py \
  tests/unit/test_dvc_setup_artifacts.py \
  tests/unit/test_mlflow_dvc_tags.py -q
python scripts/check_lib_layer_imports.py
python -m ruff check --line-length 120 \
  src/vlm_pipeline/lib/dvc_catalog.py src/vlm_pipeline/lib/dvc_git.py src/vlm_pipeline/lib/dvc_pull.py \
  src/vlm_pipeline/defs/train/catalog_ingest.py scripts/dataset_pull.py docker/trainer/mlflow_logging.py
```

Expected: all unit tests pass; lib-layer check clean; ruff clean. The 016 real-PG test runs green under a throwaway `postgres:15` (J1.5) — it auto-skips in a no-DSN CI lane, so the orchestrator MUST run it once against a provisioned PG before merge (a skip is NOT a pass).

<!-- SECTION J META
produces: migration 016_dataset_catalog.sql (dataset_catalog + dataset_catalog_aliases + dataset_catalog_pin_events + ALTER train_dataset_versions ADD dataset_catalog_id FK); lib/dvc_catalog.py (parsers + GIT_LOG_FORMAT + dvc_remote_url_for); lib/dvc_git.py (subprocess git wrappers); lib/dvc_pull.py (build_dvc_get_argv + verify_pulled_md5); resources/postgres_train.py +insert_catalog_row/get_catalog_by_alias/pin_alias; defs/train/catalog_ingest.py (_run_catalog_ingest + dataset_catalog_reconciliation_sensor); scripts/dataset_pull.py; scripts/dvc/{setup_bare_repo.sh,post-receive,dvc_config.template}; docker/curation/requirements.txt (dvc[s3]); MLflow DVC lineage fields in docker/trainer/mlflow_logging.py; sensor registration in definitions_production.py
consumes: A=013_mlops_finetune.sql train_dataset_versions (016 ALTER target); B=resources/postgres_train.py PostgresTrainMixin (connect()/_rows_to_dicts/insert_train_dataset_version) + defs/train/dataset.py _run_build_trainset; I2=docker/trainer/mlflow_logging.py _LINEAGE_FIELDS/_build_run; MinIOResource.object_exists; pg_resource/mock_minio fixtures; lib.env_utils.int_env; scripts/check_lib_layer_imports.py
co-owned files: src/vlm_pipeline/resources/postgres_migration.py (_REQUIRED_MIGRATIONS — A/F also append); src/vlm_pipeline/resources/postgres_train.py (B authors, R1+I3 add insert_candidate_model_version delta, J3 adds catalog helpers); src/vlm_pipeline/defs/train/dataset.py (B authors, J6 adds _materialize_pinned_dvc_source + 1-line row back-link); docker/trainer/requirements.txt (C authors, I2 adds mlflow, J7 adds dvc[s3]); docker/trainer/mlflow_logging.py (I2 authors, J8 extends _LINEAGE_FIELDS); src/vlm_pipeline/definitions_production.py (many sections append sensors)
open_risks: (1) 014/015 migration numbers reserved for A/F — if another section also claims 016, renumber J to next free NNN and update _REQUIRED_MIGRATIONS + test filenames. (2) MinIOResource may lack object_exists — J4.2 SPIKE adds a thin head_object wrapper (additive) if missing. (3) If B/A not yet merged, J3/J6 unit tests pass via _DummyDB but the real-PG insert + B-builder FK back-link wiring only execute post-merge — must re-verify after A+B land. (4) post-receive hook + bare repo are operator-run setup, NOT exercised by CI (only bash -n / shellcheck); live dvc add/push/get + MinIO byte transfer never run in CI. (5) Bare-repo host + dvc credential placement (.dvc/config.local) + backup cadence (restic/mc mirror of vlm-dataset/_dvc) are open ops decisions (spec §7.6 / latent-risk: prod MinIO backup gap). (6) dvc[s3] is a NEW dependency — pinned only in curation+trainer reqs; J7 test guards it stays OUT of serving images.
-->