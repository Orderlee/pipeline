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
