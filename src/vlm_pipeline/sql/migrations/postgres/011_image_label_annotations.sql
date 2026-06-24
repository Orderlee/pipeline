-- 010_image_label_annotations.sql — per-box reviewed image label projection.
--
-- 사람이 Label Studio 에서 검수·확정(finalized)한 bbox ground-truth 를 박스 단위로
-- PostgreSQL 에 투영(projection)한다. MinIO 의 COCO JSON 이 여전히 full-fidelity
-- source of truth 이고, 이 테이블은 finalize/재검수 시점에 갱신되는 조회용 사본일 뿐이다.
-- (dataset class balance / split / 박스 단위 큐레이션 쿼리를 SQL 로 가능케 함)
--
-- 설계 메모 (Codex T1 authored, .agent/skill/codex_db_migration):
--   * box_index = COCO annotations 배열의 0-based 위치. write 경로는 image_label_id 별
--     전량 DELETE → 재 INSERT 라 box_index 는 버전-로컬 값이며 영속 식별자가 아니다.
--   * UNIQUE(image_label_id, box_index) — 005 의 duplicate 사고 교훈. 동시 finalize race 를
--     fast-fail 시켜 DELETE+INSERT 를 트랜잭션 안에서 안전하게 만든다.
--   * ON DELETE CASCADE — annotation 은 부모(image_labels)의 순수 파생물. 부모 삭제 시 함께
--     정리되는 것이 올바른 의미 (cleanup_*.py 의 DELETE FROM image_labels 와 정합).
--   * 단일 DO 블록도 사용하지 않음 — runner 의 multi-DO-block 부분적용 quirk 회피.
--
-- @ASSERT_AFTER: SELECT to_regclass('image_label_annotations') IS NOT NULL
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'image_label_annotations_label_box_unique' AND conrelid = 'image_label_annotations'::regclass AND contype = 'u')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'image_label_annotations'::regclass AND confrelid = 'image_labels'::regclass AND contype = 'f')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'image_label_annotations' AND indexname = 'image_label_annotations_category_idx')

BEGIN;

CREATE TABLE IF NOT EXISTS image_label_annotations (
    annotation_id  TEXT PRIMARY KEY,
    image_label_id TEXT NOT NULL REFERENCES image_labels(image_label_id) ON DELETE CASCADE,
    image_id       TEXT REFERENCES image_metadata(image_id),
    box_index      INTEGER NOT NULL,
    category       TEXT NOT NULL,
    bbox_x         DOUBLE PRECISION NOT NULL,
    bbox_y         DOUBLE PRECISION NOT NULL,
    bbox_w         DOUBLE PRECISION NOT NULL,
    bbox_h         DOUBLE PRECISION NOT NULL,
    score          DOUBLE PRECISION,
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT image_label_annotations_label_box_unique UNIQUE (image_label_id, box_index),
    CONSTRAINT image_label_annotations_box_index_check  CHECK (box_index >= 0),
    CONSTRAINT image_label_annotations_category_check   CHECK (btrim(category) <> ''),
    CONSTRAINT image_label_annotations_bbox_check       CHECK (bbox_x >= 0 AND bbox_y >= 0 AND bbox_w > 0 AND bbox_h > 0),
    CONSTRAINT image_label_annotations_score_check      CHECK (score IS NULL OR (score >= 0.0 AND score <= 1.0))
);

-- UNIQUE(image_label_id, box_index) 가 image_label_id 선행 btree 인덱스를 이미 제공하므로
-- 별도 image_label_id 단독 인덱스는 만들지 않는다 (중복).
CREATE INDEX IF NOT EXISTS image_label_annotations_category_idx
    ON image_label_annotations (category);
CREATE INDEX IF NOT EXISTS image_label_annotations_image_id_idx
    ON image_label_annotations (image_id);

COMMIT;
