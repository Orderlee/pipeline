-- ============================================================
-- VLM Data Pipeline — 8테이블 DDL (Strict 8)
-- ============================================================
-- 운영 테이블: raw_files, image_metadata, video_metadata, labels,
--            processed_clips, datasets, dataset_clips, image_labels

-- ============================================================
-- 1. raw_files: NAS 파일 스캔 및 수집 상태 관리
-- ============================================================
CREATE TABLE IF NOT EXISTS raw_files (
    asset_id        TEXT PRIMARY KEY,
    source_path     TEXT NOT NULL,
    original_name   TEXT,
    media_type      TEXT DEFAULT 'image',
    file_size       BIGINT,
    checksum        TEXT UNIQUE,
    phash           TEXT,
    dup_group_id    TEXT,
    archive_path    TEXT,
    raw_bucket      TEXT DEFAULT 'vlm-raw',
    raw_key         TEXT,
    ingest_batch_id TEXT,
    transfer_tool   TEXT DEFAULT 'manual',
    ingest_status   TEXT DEFAULT 'pending',
    error_message   TEXT,
    spec_id         TEXT,
    source_unit_name TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 2. video_metadata: 원본 비디오 메타데이터 (ffprobe)
-- ============================================================
CREATE TABLE IF NOT EXISTS video_metadata (
    asset_id        TEXT PRIMARY KEY REFERENCES raw_files(asset_id),
    width           INTEGER,
    height          INTEGER,
    duration_sec    DOUBLE PRECISION,
    fps             DOUBLE PRECISION,
    codec           TEXT,
    bitrate         BIGINT,
    frame_count     INTEGER,
    has_audio       BOOLEAN DEFAULT FALSE,
    environment_type TEXT,
    daynight_type    TEXT,
    outdoor_score    DOUBLE PRECISION,
    avg_brightness   DOUBLE PRECISION,
    env_method       TEXT,
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    frame_extract_status TEXT DEFAULT 'pending',
    frame_extract_count  INTEGER DEFAULT 0,
    frame_extract_error  TEXT,
    frame_extracted_at   TIMESTAMP,
    auto_label_status    TEXT DEFAULT 'pending',
    auto_label_error     TEXT,
    auto_label_key       TEXT,
    auto_labeled_at      TIMESTAMP,
    timestamp_status     TEXT DEFAULT 'pending',
    timestamp_error      TEXT,
    timestamp_label_key  TEXT,
    timestamp_completed_at TIMESTAMP,
    caption_status       TEXT DEFAULT 'pending',
    caption_error        TEXT,
    caption_completed_at TIMESTAMP,
    frame_status         TEXT DEFAULT 'pending',
    frame_error          TEXT,
    frame_completed_at   TIMESTAMP,
    bbox_status          TEXT DEFAULT 'pending',
    bbox_error           TEXT,
    bbox_completed_at    TIMESTAMP,
    -- 재인코딩: 원본 인코딩 정보 (감사/디버깅 목적)
    original_codec        TEXT,
    original_profile      TEXT,
    original_has_b_frames BOOLEAN,
    original_level_int    INTEGER,
    -- 재인코딩: 판정 결과
    reencode_required     BOOLEAN DEFAULT FALSE,
    reencode_reason       TEXT,
    -- 재인코딩: 적용 결과 (codec은 실제 저장 파일 기준으로 갱신됨)
    reencode_applied      BOOLEAN DEFAULT FALSE,
    reencode_preset       TEXT
);

-- ============================================================
-- 3. labels: 라벨 데이터 관리 (LABEL 단계에서 INSERT)
-- ============================================================
CREATE TABLE IF NOT EXISTS labels (
    label_id        TEXT PRIMARY KEY,
    asset_id        TEXT REFERENCES raw_files(asset_id),
    labels_bucket   TEXT DEFAULT 'vlm-labels',
    labels_key      TEXT,
    label_format    TEXT,
    label_tool      TEXT DEFAULT 'pre-built',
    label_source    TEXT DEFAULT 'manual',
    review_status   TEXT DEFAULT 'pending',
    event_index     INTEGER DEFAULT 0,
    event_count     INTEGER,
    timestamp_start_sec DOUBLE PRECISION,
    timestamp_end_sec   DOUBLE PRECISION,
    caption_text    TEXT,
    object_count    INTEGER DEFAULT 0,
    label_status    TEXT DEFAULT 'completed',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 4. processed_clips: 전처리 결과 (clip_metadata 흡수)
-- ============================================================
CREATE TABLE IF NOT EXISTS processed_clips (
    clip_id          TEXT PRIMARY KEY,
    source_asset_id  TEXT REFERENCES raw_files(asset_id),
    source_label_id  TEXT REFERENCES labels(label_id),
    event_index      INTEGER DEFAULT 0,
    clip_start_sec   DOUBLE PRECISION,
    clip_end_sec     DOUBLE PRECISION,
    checksum         TEXT UNIQUE,
    file_size        BIGINT,
    processed_bucket TEXT DEFAULT 'vlm-processed',
    clip_key         TEXT,
    label_key        TEXT,
    data_source      TEXT DEFAULT 'manual',
    caption_text     TEXT,
    width            INTEGER,
    height           INTEGER,
    codec            TEXT,
    duration_sec     DOUBLE PRECISION,
    fps              DOUBLE PRECISION,
    frame_count      INTEGER,
    image_extract_status  TEXT DEFAULT 'pending',
    image_extract_count   INTEGER DEFAULT 0,
    image_extract_error   TEXT,
    image_extracted_at    TIMESTAMP,
    process_status   TEXT DEFAULT 'pending',
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 5. image_metadata: 원본 이미지 + 비디오 프레임 메타데이터
-- ============================================================
CREATE TABLE IF NOT EXISTS image_metadata (
    image_id         TEXT PRIMARY KEY,
    source_asset_id  TEXT NOT NULL REFERENCES raw_files(asset_id),
    source_clip_id   TEXT REFERENCES processed_clips(clip_id),
    image_bucket     TEXT DEFAULT 'vlm-raw',
    image_key        TEXT,
    image_role       TEXT DEFAULT 'source_image',
    frame_index      INTEGER,
    frame_sec        DOUBLE PRECISION,
    checksum         TEXT,
    file_size        BIGINT,
    width            INTEGER,
    height           INTEGER,
    color_mode       TEXT DEFAULT 'RGB',
    bit_depth        INTEGER DEFAULT 8,
    has_alpha        BOOLEAN DEFAULT FALSE,
    orientation      INTEGER DEFAULT 1,
    image_caption_text TEXT,
    image_caption_score DOUBLE PRECISION,
    image_caption_bucket TEXT,
    image_caption_key TEXT,
    image_caption_generated_at TIMESTAMP,
    extracted_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (image_bucket, image_key),
    UNIQUE (source_asset_id, source_clip_id, image_role, frame_index)
);

-- ============================================================
-- 6. datasets: 데이터셋 구성 관리
-- ============================================================
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id        TEXT PRIMARY KEY,
    name              TEXT,
    version           TEXT,
    config            JSONB,
    split_ratio       JSONB DEFAULT '{"train":0.8,"val":0.1,"test":0.1}'::jsonb,
    dataset_bucket    TEXT DEFAULT 'vlm-dataset',
    dataset_prefix    TEXT,
    build_status      TEXT DEFAULT 'pending',
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Phase 3-D dataset lineage (migration 004) — 재현성 메타. dataset_prefix 영향 없음.
    spec_hash         TEXT,
    git_sha           TEXT,
    build_started_at  TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_datasets_spec_hash ON datasets(spec_hash);

-- ============================================================
-- 7. dataset_clips: 데이터셋-클립 연결 (M:N)
-- ============================================================
CREATE TABLE IF NOT EXISTS dataset_clips (
    dataset_id      TEXT REFERENCES datasets(dataset_id),
    clip_id         TEXT REFERENCES processed_clips(clip_id),
    split           TEXT,
    dataset_key     TEXT,
    PRIMARY KEY (dataset_id, clip_id)
);

-- ============================================================
-- 8. image_labels: 이미지 detection 라벨 (YOLO-world 등)
-- ============================================================
CREATE TABLE IF NOT EXISTS image_labels (
    image_label_id   TEXT PRIMARY KEY,
    image_id         TEXT REFERENCES image_metadata(image_id),
    source_clip_id   TEXT REFERENCES processed_clips(clip_id),
    labels_bucket    TEXT DEFAULT 'vlm-labels',
    labels_key       TEXT,
    label_format     TEXT,
    label_tool       TEXT,
    label_source     TEXT,
    review_status    TEXT DEFAULT 'pending',
    label_status     TEXT DEFAULT 'pending',
    object_count     INTEGER DEFAULT 0,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 9. dispatch_requests: Dispatch 요청 추적 (dispatch_sensor DDL과 동일)
--    trigger JSON → archive 이동 + 파이프라인 실행
-- ============================================================
CREATE TABLE IF NOT EXISTS dispatch_requests (
    request_id           TEXT PRIMARY KEY,
    folder_name          TEXT,
    run_mode             TEXT,
    outputs              TEXT,          -- 쉼표 구분: bbox, timestamp, captioning
    labeling_method      TEXT,
    categories           TEXT,
    classes              TEXT,
    image_profile        TEXT,
    status               TEXT DEFAULT 'pending',
    archive_pending_path TEXT,
    archive_path         TEXT,
    -- YOLO 이미지 추출 파라미터 (NULL이면 기본값 사용)
    max_frames_per_video INTEGER,
    jpeg_quality         INTEGER,
    confidence_threshold DOUBLE PRECISION,
    iou_threshold        DOUBLE PRECISION,
    -- 메타
    requested_by         TEXT,
    requested_at         TIMESTAMP,
    processed_at         TIMESTAMP,
    completed_at         TIMESTAMP,
    error_message        TEXT,
    ls_task_status       TEXT DEFAULT 'pending',  -- pending | created | skipped
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 10. staging_model_configs: output 타입별 모델 선택 + 기본 파라미터
--     outputs 값(bbox, timestamp, captioning)에 따라 사용 모델 결정
-- ============================================================
CREATE TABLE IF NOT EXISTS staging_model_configs (
    config_id            TEXT PRIMARY KEY,
    output_type          TEXT NOT NULL UNIQUE,  -- bbox | timestamp | captioning
    model_name           TEXT NOT NULL,         -- yolov8l-worldv2 | gemini-2.0-flash 등
    model_version        TEXT,
    -- 이미지 추출 기본 파라미터 (dispatch JSON에 값이 없을 때 사용)
    default_max_frames   INTEGER DEFAULT 24,
    default_jpeg_quality INTEGER DEFAULT 90,
    -- 모델별 기본 파라미터
    default_confidence   DOUBLE PRECISION DEFAULT 0.25,
    default_iou          DOUBLE PRECISION DEFAULT 0.45,
    -- 추가 설정 (JSON)
    extra_params         TEXT,                  -- JSON 문자열로 확장 파라미터 저장
    is_active            BOOLEAN DEFAULT TRUE,
    description          TEXT,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 11. dispatch_pipeline_runs: Dispatch 요청별 파이프라인 단계 진행 추적
--     archive 이동 → 모델 실행 → 완료 전 과정 기록
-- ============================================================
CREATE TABLE IF NOT EXISTS dispatch_pipeline_runs (
    run_id               TEXT PRIMARY KEY,
    request_id           TEXT,
    folder_name          TEXT,
    -- 단계 상태
    step_name            TEXT NOT NULL,         -- archive_move | frame_extract | yolo_detect | gemini_timestamp | gemini_caption | build_dataset
    step_order           INTEGER DEFAULT 0,
    step_status          TEXT DEFAULT 'pending', -- pending | running | completed | failed | skipped
    -- 모델 정보
    model_name           TEXT,
    model_version        TEXT,
    -- 사용된 파라미터 (실제 적용값 기록)
    applied_params       TEXT,                  -- JSON: {"max_frames":24,"jpeg_quality":90,...}
    -- 처리 통계
    input_count          INTEGER DEFAULT 0,
    output_count         INTEGER DEFAULT 0,
    error_count          INTEGER DEFAULT 0,
    -- 타이밍
    started_at           TIMESTAMP,
    completed_at         TIMESTAMP,
    error_message        TEXT,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- Spec flow (labeling_specs 등): greenfield는 위 CREATE에 컬럼 포함.
-- 기존 DB는 duckdb_base.ensure_schema() ALTER로 동일 스키마로 수렴.
-- ingest_status 허용값: pending, uploading, completed, failed, skipped,
--   pending_spec, ready_for_labeling
-- ============================================================

-- 12. labeling_specs: spec 수신 → 라우팅/재시도/완료 추적
CREATE TABLE IF NOT EXISTS labeling_specs (
    spec_id              TEXT PRIMARY KEY,
    requester_id         TEXT,
    team_id              TEXT,
    source_unit_name     TEXT,
    categories           JSONB,                     -- category array
    classes              JSONB,                     -- resolved class array (derived from categories)
    labeling_method      JSONB,                     -- ["timestamp","captioning","bbox"] or []
    spec_status          TEXT DEFAULT 'pending', -- pending | pending_resolved | active | completed | failed
    retry_count          INTEGER DEFAULT 0,
    resolved_config_id   TEXT,
    resolved_config_scope TEXT,                 -- personal | team | fallback
    last_error           TEXT,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 13. labeling_configs: config/parameters/*.json 동기화
CREATE TABLE IF NOT EXISTS labeling_configs (
    config_id            TEXT PRIMARY KEY,
    config_json          JSONB NOT NULL,
    version              INTEGER DEFAULT 1,
    is_active            BOOLEAN DEFAULT TRUE,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 14. requester_config_map: requester/team → config 매핑 (personal → team → _fallback)
CREATE TABLE IF NOT EXISTS requester_config_map (
    map_id               TEXT PRIMARY KEY,
    requester_id         TEXT,
    team_id              TEXT,
    scope                TEXT NOT NULL,          -- personal | team
    config_id            TEXT NOT NULL,
    is_active            BOOLEAN DEFAULT TRUE,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 15. classification_datasets: 카테고리별 폴더 복사 빌드 이력
-- ============================================================
CREATE TABLE IF NOT EXISTS classification_datasets (
    dataset_id             TEXT PRIMARY KEY,
    name                   TEXT,
    folder_prefix          TEXT,
    config                 JSONB,
    classification_bucket  TEXT DEFAULT 'vlm-classification',
    video_count            INTEGER DEFAULT 0,
    image_count            INTEGER DEFAULT 0,
    category_count         INTEGER DEFAULT 0,
    build_status           TEXT DEFAULT 'pending',
    created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 16. image_label_annotations: 검수 확정(finalized) bbox 박스 단위 projection
-- ============================================================
-- MinIO COCO JSON 이 source of truth, 이 테이블은 조회용 사본. (migration 010 참조)
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

CREATE INDEX IF NOT EXISTS image_label_annotations_category_idx
    ON image_label_annotations (category);
CREATE INDEX IF NOT EXISTS image_label_annotations_image_id_idx
    ON image_label_annotations (image_id);

-- ============================================================
-- 17. v_finalized_labels: LS 확정 라벨 3종(caption/timestamp/bbox) 통합 추적 VIEW
-- ============================================================
-- caption/timestamp = labels (video 이벤트), bbox = image_label_annotations (image 박스).
-- grain 이 달라 long-format union. SoT 는 MinIO JSON, 이 뷰는 조회용 파생물. (migration 012 참조)
CREATE OR REPLACE VIEW v_finalized_labels AS
    SELECT 'caption'::text AS label_type, l.labels_key, l.asset_id,
           NULL::text AS source_clip_id, NULL::text AS image_id, NULL::text AS category,
           l.caption_text, l.timestamp_start_sec, l.timestamp_end_sec,
           NULL::double precision AS score, l.created_at
    FROM labels l
    WHERE l.review_status = 'finalized' AND l.caption_text IS NOT NULL AND btrim(l.caption_text) <> ''
    UNION ALL
    SELECT 'timestamp'::text, l.labels_key, l.asset_id, NULL::text, NULL::text, NULL::text,
           NULL::text, l.timestamp_start_sec, l.timestamp_end_sec, NULL::double precision, l.created_at
    FROM labels l
    WHERE l.review_status = 'finalized' AND l.timestamp_start_sec IS NOT NULL
    UNION ALL
    SELECT 'bbox'::text, il.labels_key, NULL::text, il.source_clip_id, ila.image_id, ila.category,
           NULL::text, NULL::double precision, NULL::double precision, ila.score, ila.created_at
    FROM image_label_annotations ila
    JOIN image_labels il ON il.image_label_id = ila.image_label_id
    WHERE il.review_status = 'finalized';
