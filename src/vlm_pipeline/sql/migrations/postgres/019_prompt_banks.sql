-- 019_prompt_banks.sql — APO 프롬프트 뱅크(userwatch 공급 + 우리 authored) 카탈로그.
--
-- 안 B 원칙: generation_prompts(018)와 물리적으로 분리한다. 뱅크는 LLM 지시문이 아니라
-- 임베딩 대상 문장 수만 개(PE-Core-L14-336 코사인, top-10 다수결)이므로 grain·소비방식이
-- 근본적으로 다르다 — 물리적 단일 테이블은 조회 실익이 없다.
--
-- ⚠️ 이전 결정 재확인: "프롬프트 뱅크 Postgres 레지스트리"는 2026-07 감사에서 보류 판정을 받았다
-- (행이 5개 수준이고 내구성은 MinIO 매니페스트가 동일하게 주는데 migration 은 재빌드+라벨링
-- 중단을 물림). 해제 조건은 "두 번째 소비자가 프로그램으로 조회해야 할 때"였다.
--
-- 이번 migration 은 그 해제 조건을 완전히 충족하지 못한 상태로 진행한다. 리스크를 낮추기 위한
-- 절충:
--   * userwatch 공급 52개 버전 중 로컬로 흡수(임베딩)한 것은 2개뿐이고 나머지는 NAS 원본 파일로만
--     존재한다 — prompt_banks 는 이들 각각에 "카탈로그 1행 + 원본 파일 포인터(origin_uri)"만
--     두고 문장 전체를 복제하지 않는다(sentence_storage='external_only'). 대량 뱅크는 지금도
--     파일이 SoT — 보류 판정의 전제(행 5개 수준, 대량 복제 안 함)를 깨지 않는다.
--   * bank_sentences 에 실제 행 단위로 들어가는 것은 우리가 직접 author/curate 한 문장뿐
--     (origin='human-pinpoint' 등, 오늘 기준 5행 — docker/data/fiftyone/sourceh_v2/work/banks/
--     sentences.jsonl 의 유일한 영속 사본). 그 파일은 .gitignore 대상(docker/data/)이고 analysis
--     컨테이너 재생성·디스크 정리(호스트 루트 98%)로 통보 없이 사라질 수 있는 위치였다 —
--     bank_sentences 이전은 "행 5개를 위한 과잉 migration"이 아니라 "유일한 원장을 미추적
--     파일에서 내구성 있는 저장소로 옮기는" 방어적 조치로 본다.
--   * 진짜 "두 번째 프로그램 소비자"는 아직 없다(사전 실측 open_question — APO 예측 자체가
--     labels/GT 보정 루프에 들어오지 않는다). 이 판단이 틀렸다면 강행 근거가 사라진다 —
--     이 파일이 이번 설계의 가장 약한 지점이다(weakest_point 참고).
--
-- ── 2026-08-10 개정 (적용 전 수정 — 어느 환경에도 적용된 적 없음) ────────────────────
--   * 보류 해제 조건("두 번째 소비자가 프로그램으로 조회")이 요구 R6(뱅크 문장 DB 관리) +
--     Phase 2 창으로 충족됐다. 위 weakest_point 는 해소된 것으로 본다.
--   * 전수 실측(`docker/analysis/prompt_bank_ledger.py inventory`)이 원안의 두 전제를 바꿨다:
--     ① 52버전 중 텍스트 보유는 35개뿐(13개는 벡터만, 4개는 빈 폴더)이라 `db_backed` 승격
--        대상은 35개다. 나머지는 `external_only` 로 남는다 — 옮길 텍스트가 존재하지 않는다.
--     ② 문장 총량이 506,247행/10.5MB 로 작아 "대량 복제 회피" 근거가 성립하지 않는다.
--        별도 membership 테이블 없이 bank_sentences 하나로 버전 diff 가 SQL 한 방이 된다.
--   * bank_sentences 의 UNIQUE 키를 content_hash → gidx 로 교정 (아래 제약 주석 참고).
--
-- Forward-only, idempotent, DO 블록 미사용.
--
-- @ASSERT_AFTER: SELECT to_regclass('prompt_banks') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('bank_sentences') IS NOT NULL
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'prompt_banks_version_source_unique' AND conrelid = 'prompt_banks'::regclass AND contype = 'u')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'bank_sentences'::regclass AND confrelid = 'prompt_banks'::regclass AND contype = 'f')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'bank_sentences_bank_gidx_unique' AND conrelid = 'bank_sentences'::regclass AND contype = 'u')

BEGIN;

CREATE TABLE IF NOT EXISTS prompt_banks (
    bank_id           UUID PRIMARY KEY,
    version_tag       TEXT NOT NULL,     -- 원문 그대로 보존, 파싱/정규화 없음 — 52개 버전 표기가
                                          -- 자체적으로 비일관적(대소문자, v-prefix 유무, '+' 빌드
                                          -- 메타데이터, 빈 폴더)이라 규칙을 발명하지 않는다.
    source            TEXT NOT NULL,     -- 'userwatch' | 'internal' | 'hybrid'
    sentence_storage  TEXT NOT NULL DEFAULT 'external_only',  -- 'external_only' | 'db_backed'
    origin_uri        TEXT NOT NULL,     -- NAS 경로 또는 MinIO 키 (원본 CSV/JSON/npz)
    embedding_npz_key TEXT,              -- 분석 파이프라인에 흡수된 경우 MinIO 키 (vec/cls[/prompt])
    model_name        TEXT,              -- 예: 'PE-Core-L14-336'
    sentence_count    INTEGER,
    class_counts      JSONB,             -- {"normal": N, "fire": N, ...} (선택, 있으면)
    parent_bank_id    UUID REFERENCES prompt_banks(bank_id),  -- 델타 뱅크 lineage
                                          -- (e.g. 'v1.0.8.0+night5' 의 parent = 'v1.0.8.0')
    eval_summary      JSONB,             -- {"top_k":10,"micro":0.846,...} 요약 스칼라만(전체 리포트 아님)
    checksum          TEXT,              -- 원본 파일 sha256 — userwatch 공급물은 매니페스트/체크섬을
                                          -- 발행하지 않아 대부분 NULL (사전 실측)
    ingested_by       TEXT,
    notes             TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT prompt_banks_version_source_unique UNIQUE (source, version_tag),
    CONSTRAINT prompt_banks_source_check CHECK (source IN ('userwatch', 'internal', 'hybrid')),
    CONSTRAINT prompt_banks_storage_check CHECK (sentence_storage IN ('external_only', 'db_backed'))
);

CREATE INDEX IF NOT EXISTS prompt_banks_source_idx ON prompt_banks (source);
CREATE INDEX IF NOT EXISTS prompt_banks_created_at_idx ON prompt_banks (created_at);

CREATE TABLE IF NOT EXISTS bank_sentences (
    sentence_id             UUID PRIMARY KEY,
    bank_id                 UUID NOT NULL REFERENCES prompt_banks(bank_id) ON DELETE CASCADE,
    -- sha256(공백정규화+소문자화 text)[:16] — sentences.jsonl 원장과 동일 알고리즘.
    -- ⚠️ 알려진 결함을 그대로 승계: class 가 해시에 미포함이라 같은 text 에 다른 class 를 부여하면
    -- 충돌한다(사전 실측). 이 migration 은 알고리즘을 고치지 않는다 — 원장 재계산이 필요한 별도
    -- 후속 작업으로 분리한다(범위 확대 방지).
    content_hash            TEXT NOT NULL,
    text                    TEXT NOT NULL,
    class_label             TEXT NOT NULL,
    -- 뱅크 안에서의 순번. 프레임 쪽 `winner_gidx_*` 가 가리키는 값이 이것이고, CSV 행 순서라
    -- 파생 불가 — 명시 저장이 유일한 방법이다. 사람이 author 한 문장은 뱅크 순번이 없어 NULL.
    gidx                    INTEGER,
    origin                  TEXT NOT NULL DEFAULT 'human-pinpoint',  -- 'human-pinpoint' | 'userwatch' | 'pruned-candidate' | ...
    adopted                 BOOLEAN NOT NULL DEFAULT FALSE,
    probe_target            TEXT,
    probe_exact_fixed       INTEGER,
    probe_exact_broken      INTEGER,
    probe_fire_recall_delta DOUBLE PRECISION,
    probe_smoke_recall_delta DOUBLE PRECISION,
    probe_verified          BOOLEAN NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- ⚠️ 원안은 UNIQUE(bank_id, content_hash) 였으나 **전수 실측이 반증**했다: userwatch 35개
    -- 뱅크에 같은 문장이 두 번 이상 들어간 경우가 152건 있다(뱅크는 문장의 집합이 아니라
    -- 반복 가능한 순서열이다). 그 제약으로 적재하면 152행이 조용히 사라지고, 사라진 행의
    -- gidx 가 winner_gidx 로 참조되면 프레임↔문장 조인이 깨진다. 실제 identity 는 gidx 다.
    -- (우리가 쓰는 v1.0.8.0/v1.0.8.4 자체는 뱅크내 중복 0건이라 현행 조인에는 영향 없음.)
    CONSTRAINT bank_sentences_bank_gidx_unique UNIQUE (bank_id, gidx),
    CONSTRAINT bank_sentences_text_check CHECK (btrim(text) <> '')
);

CREATE INDEX IF NOT EXISTS bank_sentences_class_label_idx ON bank_sentences (class_label);
CREATE INDEX IF NOT EXISTS bank_sentences_bank_id_idx ON bank_sentences (bank_id);
-- 버전 간 문장 diff("A 에는 있고 B 에는 없는 문장")의 조인 키. UNIQUE 는 아니다(위 참고).
CREATE INDEX IF NOT EXISTS bank_sentences_content_hash_idx ON bank_sentences (content_hash);

COMMIT;
