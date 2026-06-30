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
