-- 020_prompt_lineage_views.sql — "통합 관리"를 위한 단일 조회 인터페이스 (안 B).
--
-- 018/019 가 물리적으로 분리한 두 프롬프트 계열(generation_prompts / prompt_banks+bank_sentences)
-- 을 물리적으로 합치지 않고, 목적이 다른 두 VIEW 로 "한 화면"을 제공한다:
--
--   * v_prompt_catalog  — 요구#2 ("여기서 생성한 프롬프트와 APO 프롬프트를 어떻게 할지").
--     두 계열을 domain discriminator 로 나열하는 인벤토리. 물리적 병합이 아니라 브라우징용
--     공통 최소 스키마(domain/catalog_id/name/version_tag/model_name/item_count/created_at)
--     로의 UNION ALL.
--
--   * v_prompt_lineage  — 요구#3 ("사람이 수정하기도 하니까 통합관리"). generation_prompts 만
--     대상으로 한다(APO 뱅크는 대상이 아님 — 아래 한계 참고). video_metadata.
--     timestamp_generation_prompt_id 를 경유해 "이 asset 의 라벨을 만든 프롬프트가 무엇이었고,
--     현재 그 라벨이 사람 손을 거쳤는가(label_source='manual_review')"를 한 쿼리로 보여준다.
--
-- ⚠️ v_prompt_lineage 의 명시적 한계 (숨기지 않음):
--   1. "사람이 무엇을 어떻게 고쳤는지"의 필드 단위 diff 는 이 VIEW 만으로 얻을 수 없다.
--      human_edited=true 는 "고쳐졌다"는 사실만 알려준다. 고치기 전 원본 이벤트 배열은
--      vlm-labels 의 <label_key>.pseudo.json 스냅샷(write-once)에만 있고, VIEW 는 Postgres
--      쿼리라 MinIO 객체를 조인할 수 없다 — 진짜 diff 는 이 VIEW 결과 + 별도 MinIO GET
--      1회를 애플리케이션 레벨에서 조합해야 한다(Phase 2: generation_prompt_events 프로젝션
--      테이블을 만들면 순수 SQL diff 가 가능해지지만, 그건 timestamp.py 의 최초 auto-insert
--      경로를 추가로 건드려야 하는 더 큰 변경이라 이번 Phase 1 범위에서 제외했다).
--   2. APO 뱅크(prompt_banks/bank_sentences)에는 대응하는 human-edit 루프가 없다 — APO 예측은
--      우리 labels/GT 보정 흐름에 들어오지 않는(사전 실측 확정) 별도 제품(userwatch) 소비 경로다.
--      v_prompt_lineage 에 뱅크를 넣지 않은 것은 누락이 아니라 "그 루프가 존재하지 않는다"는
--      사실을 그대로 반영한 것이다.
--   3. 과거 12,608개 labels 행은 video_metadata.timestamp_generation_prompt_id 가 NULL이라
--      이 VIEW 에 아예 나타나지 않는다(JOIN 이 비게 됨) — 018 헤더에 명시한 소급 백필 불가와
--      동일한 이유.
--
-- ⚠️ CREATE OR REPLACE VIEW 는 컬럼 이름/순서/타입 변경을 허용하지 않는다(012 의 동일 주의사항).
--    향후 컬럼을 추가·재정렬하려면 새 migration 에서 DROP VIEW 후 재생성할 것.
--
-- Forward-only, idempotent, DO 블록 미사용.
--
-- @ASSERT_AFTER: SELECT to_regclass('v_prompt_catalog') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('v_prompt_lineage') IS NOT NULL

BEGIN;

CREATE OR REPLACE VIEW v_prompt_catalog AS
    SELECT
        'gemini_generation'::text AS domain,
        gp.prompt_id::text        AS catalog_id,
        gp.template_name          AS name,
        gp.template_version       AS version_tag,
        gp.model_name             AS model_name,
        NULL::integer             AS item_count,   -- rendered_prompt 는 문자열 1개, "개수" 개념 없음
        gp.created_at             AS created_at
    FROM generation_prompts gp

    UNION ALL

    SELECT
        'prompt_bank'::text,
        pb.bank_id::text,
        pb.source || ':' || pb.version_tag,
        pb.version_tag,
        pb.model_name,
        pb.sentence_count,
        pb.created_at
    FROM prompt_banks pb;

CREATE OR REPLACE VIEW v_prompt_lineage AS
    SELECT
        gp.prompt_id                                AS prompt_id,
        gp.prompt_type                               AS prompt_type,
        gp.template_name                             AS template_name,
        gp.template_version                          AS template_version,
        gp.model_name                                AS model_name,
        gp.categories                                AS categories,
        gp.rendered_prompt                           AS rendered_prompt,
        vm.asset_id                                  AS asset_id,
        l.labels_key                                 AS labels_key,
        l.label_id                                   AS label_id,
        l.event_index                                AS event_index,
        l.label_source                                AS label_source,
        l.review_status                               AS review_status,
        l.caption_text                                AS caption_text,
        l.timestamp_start_sec                         AS timestamp_start_sec,
        l.timestamp_end_sec                           AS timestamp_end_sec,
        (l.label_source = 'manual_review')            AS human_edited,
        gp.created_at                                 AS prompt_created_at,
        l.created_at                                  AS label_created_at
    FROM generation_prompts gp
    JOIN video_metadata vm ON vm.timestamp_generation_prompt_id = gp.prompt_id
    -- ⚠️ asset_id 로 조인하면 안 된다. labels 는 timestamp 이벤트 전용 테이블이 아니라
    -- video/image classification artifact import 도 같은 asset_id 로 행을 넣는다
    -- (defs/label/import_support.py 의 insert_label, label_format='video_classification_json').
    -- asset_id 조인은 그 분류 라벨까지 timestamp 프롬프트가 만든 것처럼 귀속시킨다.
    -- 스테이지가 실제로 기록한 labels_key 로 조인해 grain 을 정확히 맞춘다:
    --   * routed 경로 → video_metadata.timestamp_label_key
    --   * MVP 경로    → video_metadata.auto_label_key (상태 컬럼이 다르다)
    -- 사람이 LS 에서 수정하면 같은 labels_key 로 DELETE+재INSERT 되므로(018 헤더 근거)
    -- manual_review 행도 이 조인에 그대로 남는다 — human_edited 플래그가 계속 동작한다.
    -- labels_key_event_idx_unique UNIQUE(labels_key, event_index) 라 fan-out 위험도 없다.
    JOIN labels l ON l.labels_key = COALESCE(vm.timestamp_label_key, vm.auto_label_key);

COMMIT;
