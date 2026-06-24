-- 012_finalized_label_view.sql — LS 확정(finalized) 라벨 3종 통합 추적 VIEW.
--
-- Label Studio 가 확정한 라벨 3종을 한 곳에서 long-format 으로 조회한다:
--   * caption   : labels.caption_text            (video 이벤트 단위)
--   * timestamp : labels.timestamp_start/end_sec (video 이벤트 단위)
--   * bbox      : image_label_annotations         (image 박스 단위, migration 011)
-- 셋은 grain 이 달라 한 테이블로 합치지 않고 VIEW 로 union 한다. SoT 는 여전히
-- MinIO 의 events JSON / COCO JSON 이고, 이 뷰는 조회 편의용 파생물이다.
--
-- 컬럼은 3종 공통 superset 으로 정렬하고, 해당 없는 값은 NULL:
--   label_type | labels_key | asset_id | source_clip_id | image_id | category
--   | caption_text | timestamp_start_sec | timestamp_end_sec | score | created_at
-- 사용 예: SELECT label_type, count(*) FROM v_finalized_labels GROUP BY 1;
--
-- ⚠️ CREATE OR REPLACE VIEW 는 컬럼 이름/순서/타입 변경을 허용하지 않는다 (PostgreSQL 제약).
--    향후 컬럼을 추가·재정렬하려면 새 migration 에서 DROP VIEW v_finalized_labels 후 재생성할 것.
--
-- @ASSERT_AFTER: SELECT to_regclass('v_finalized_labels') IS NOT NULL

BEGIN;

CREATE OR REPLACE VIEW v_finalized_labels AS
    -- caption (video 이벤트)
    SELECT
        'caption'::text          AS label_type,
        l.labels_key             AS labels_key,
        l.asset_id               AS asset_id,
        NULL::text               AS source_clip_id,
        NULL::text               AS image_id,
        NULL::text               AS category,
        l.caption_text           AS caption_text,
        l.timestamp_start_sec    AS timestamp_start_sec,
        l.timestamp_end_sec      AS timestamp_end_sec,
        NULL::double precision   AS score,
        l.created_at             AS created_at
    FROM labels l
    WHERE l.review_status = 'finalized'
      AND l.caption_text IS NOT NULL
      AND btrim(l.caption_text) <> ''

    UNION ALL

    -- timestamp (video 이벤트)
    SELECT
        'timestamp'::text,
        l.labels_key,
        l.asset_id,
        NULL::text,
        NULL::text,
        NULL::text,
        NULL::text,
        l.timestamp_start_sec,
        l.timestamp_end_sec,
        NULL::double precision,
        l.created_at
    FROM labels l
    WHERE l.review_status = 'finalized'
      AND l.timestamp_start_sec IS NOT NULL

    UNION ALL

    -- bbox (image 박스, 확정 image_labels 의 박스 단위 projection)
    SELECT
        'bbox'::text,
        il.labels_key,
        NULL::text,
        il.source_clip_id,
        ila.image_id,
        ila.category,
        NULL::text,
        NULL::double precision,
        NULL::double precision,
        ila.score,
        ila.created_at
    FROM image_label_annotations ila
    JOIN image_labels il ON il.image_label_id = ila.image_label_id
    WHERE il.review_status = 'finalized';

COMMIT;
