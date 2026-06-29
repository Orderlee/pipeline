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
