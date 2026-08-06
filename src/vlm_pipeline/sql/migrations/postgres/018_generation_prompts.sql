-- 018_generation_prompts.sql — Gemini 프롬프트 통합 관리 Phase 1 (안 B: 분리 테이블 + 공통 계보).
--
-- 사전 실측: "gemini timestamp 프롬프트가 DB 에 저장된다"는 전제는 틀렸다. VIDEO_EVENT_PROMPT 는
-- lib/gemini_prompts.py 의 코드 상수이고 build_video_event_prompt(categories=, descriptions=) 로
-- 런타임 조립된다. 조립 재료(categories/gemini_descriptions)는 dispatch Dagster run tag 의
-- JSON 문자열일 뿐이라 vlm_pipeline Postgres 에는 전혀 남지 않는다 — 실제로 Gemini 에 보낸
-- 최종 문자열은 지금까지 DB/MinIO 어디에도 영속화된 적이 없다. 이 migration 이 그 최초 저장소다.
--
-- 통합 원칙(안 B): APO 프롬프트 뱅크(019_prompt_banks.sql)와 물리적으로 같은 테이블에 두지 않는다
-- — 소비 방식이 근본적으로 다르다(LLM 지시문 1개 vs 임베딩 대상 문장 수만 개, top-k 코사인 다수결).
-- "통합"은 020_prompt_lineage_views.sql 의 뷰가 제공하는 단일 조회 인터페이스로 실현한다.
--
-- labels ↔ 프롬프트 링크 설계 — labels 테이블에는 FK 컬럼을 추가하지 않는다:
--   * captioning.py(clip_captioning) 의 replace_gemini_labels() 는 asset_id+labels_key 기준으로
--     기존 auto/gemini 라벨을 DELETE 후 재INSERT 한다. ls_sync_db.py(upsert_video_labels) 는
--     사람이 LS 에서 제출할 때 labels_key 기준으로 전량 DELETE 후 manual_review 로 재INSERT 한다
--     (원본 auto 행은 이 순간 사라진다 — 사전 실측 확정). labels 에 FK 를 두면 사람이 수정할
--     때마다 그 DELETE 에 링크가 함께 삭제된다 — 요구#3("사람이 수정하기도 하니까 통합관리")의
--     핵심을 정면으로 깨뜨린다.
--   * 대신 video_metadata.timestamp_generation_prompt_id (asset 당 1개, 아래에서 추가)로 간접
--     연결한다. video_metadata 는 그 DELETE+INSERT 사이클과 무관한 별도 테이블이라 사람이 몇 번을
--     고쳐도 "이 라벨을 만든 프롬프트가 무엇이었나"의 링크가 생존한다.
--   * asset 당 진행 중인 auto/gemini 라벨 세트는 항상 1개뿐이다(재생성 시 이전 세트를 먼저
--     지우고 새로 쓰는 replace-전략) — asset 당 포인터 1개로 grain 이 정확히 맞는다.
--   * 대가: 같은 asset 을 나중에 다른 프롬프트로 재-timestamp 하면 이전 포인터는 덮어써진다.
--     labels_key 자체도 재생성 시 같은 키를 덮어쓰므로 이미 겪는 한계다(key_builders.py) —
--     새로 만드는 문제가 아니라 기존 한계를 그대로 승계한다.
--
-- 소급 백필 불가: 과거 12,608개 labels 행이 어떤 프롬프트로 만들어졌는지는 DB/MinIO 어디에도
-- 남아있지 않다(사전 실측 확정, 재구성 근거 없음). 이 migration 은 그 공백을 허위로 채우지
-- 않는다 — video_metadata.timestamp_generation_prompt_id 는 신규 timestamp 실행분부터만
-- non-NULL 이 되고, 과거 행은 항상 NULL 로 남는다(백필 스크립트 없음, 의도적).
--
-- Forward-only, idempotent, DO 블록 미사용 — 016/017 과 동일 원칙(runner 의 multi-DO
-- 부분적용 quirk 회피, 005 사고 재발 방지).
--
-- @ASSERT_AFTER: SELECT to_regclass('generation_prompts') IS NOT NULL
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'generation_prompts_dedup_unique' AND conrelid = 'generation_prompts'::regclass AND contype = 'u')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'generation_prompts_type_check' AND conrelid = 'generation_prompts'::regclass AND contype = 'c')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'video_metadata' AND column_name = 'timestamp_generation_prompt_id')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'video_metadata'::regclass AND confrelid = 'generation_prompts'::regclass AND contype = 'f')

BEGIN;

CREATE TABLE IF NOT EXISTS generation_prompts (
    prompt_id             UUID PRIMARY KEY,
    -- 'video_event_timestamp' (본 요구의 대상) | 'video_classification' | 'event_frame_relevance'
    -- | 'event_frame_caption' | 'vqa_image' | 'vqa_video' — lib/gemini_prompts.py 의 4개 프롬프트
    -- 계열을 discriminator 로 구분. Phase 1 은 'video_event_timestamp' 만 실제로 기록한다
    -- (다른 3종은 write 경로 미배선 — 아래 요구 충족도 참고).
    prompt_type           TEXT NOT NULL,
    template_name         TEXT NOT NULL,          -- 코드 상수 식별자, e.g. 'VIDEO_EVENT_PROMPT' (lib/gemini_prompts.py)
    template_version      TEXT,                    -- 템플릿 본문 변경 시 사람이 수동 bump (신규 관례)
    model_name            TEXT NOT NULL DEFAULT 'gemini-2.5-flash',
    categories            JSONB,                   -- dispatch run tag 'categories' 원본
    category_descriptions JSONB,                   -- dispatch run tag 'gemini_descriptions' 원본
    rendered_prompt       TEXT NOT NULL,            -- 실제로 Gemini 에 보낸 최종 문자열 (요구#1 핵심)
    content_hash          TEXT NOT NULL,            -- sha256(rendered_prompt) hex — dedup 키
                                                     -- (한 run 의 N개 영상이 동일 문자열을 공유하므로
                                                     --  dedup 없으면 행 폭증)
    spec_id               TEXT REFERENCES labeling_specs(spec_id),  -- 현재 생산자 0개(사전 실측,
                                                     -- labeling_specs 는 죽은 배선) — 미래 배선 대비
                                                     -- 예약 컬럼. 대부분 NULL 로 유지될 것.
    dagster_run_id        TEXT,                     -- 이 rendered_prompt 를 최초로 만든 run (정보용,
                                                     -- dedup 재사용 시 갱신하지 않음 — 최초 관측만 기록)
    created_at            TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Phase 2 예약 컬럼 — 현재 이 컬럼을 채우는 job 은 없다(구현 안 됨, 자리만 예약).
    -- "이 프롬프트로 만든 라벨들이 LS 에서 얼마나/어떻게 수정됐는가"를 배치로 계산해 넣을 자리.
    human_edit_stats      JSONB,
    CONSTRAINT generation_prompts_dedup_unique UNIQUE (prompt_type, model_name, content_hash),
    CONSTRAINT generation_prompts_type_check CHECK (
        prompt_type IN (
            'video_event_timestamp', 'video_classification',
            'event_frame_relevance', 'event_frame_caption',
            'vqa_image', 'vqa_video'
        )
    ),
    CONSTRAINT generation_prompts_rendered_prompt_check CHECK (btrim(rendered_prompt) <> ''),
    CONSTRAINT generation_prompts_content_hash_check CHECK (btrim(content_hash) <> '')
);

CREATE INDEX IF NOT EXISTS generation_prompts_template_name_idx
    ON generation_prompts (template_name);
CREATE INDEX IF NOT EXISTS generation_prompts_created_at_idx
    ON generation_prompts (created_at);

-- asset 당 "현재 유효한" timestamp 생성 프롬프트 포인터. labels 의 DELETE+INSERT 사이클과
-- 무관하게 생존한다(파일 헤더 근거). ADD COLUMN 은 NULL 로만 채워지므로 과거 행은 그대로 NULL.
ALTER TABLE video_metadata
    ADD COLUMN IF NOT EXISTS timestamp_generation_prompt_id UUID;

-- FK 는 ADD CONSTRAINT IF NOT EXISTS 미지원 → DROP IF EXISTS 후 재추가 (016 과 동일 패턴,
-- 단일 statement, DO 미사용).
ALTER TABLE video_metadata
    DROP CONSTRAINT IF EXISTS video_metadata_generation_prompt_fk;
ALTER TABLE video_metadata
    ADD CONSTRAINT video_metadata_generation_prompt_fk
        FOREIGN KEY (timestamp_generation_prompt_id) REFERENCES generation_prompts(prompt_id);

CREATE INDEX IF NOT EXISTS video_metadata_generation_prompt_idx
    ON video_metadata (timestamp_generation_prompt_id) WHERE timestamp_generation_prompt_id IS NOT NULL;

COMMIT;
