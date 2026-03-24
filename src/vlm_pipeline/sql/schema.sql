-- ============================================================
-- VLM Data Pipeline — 8테이블 DDL (Strict 8)
-- ============================================================
-- 운영 테이블: raw_files, image_metadata, video_metadata, labels,
--            processed_clips, datasets, dataset_clips, image_labels

-- ============================================================
-- 1. raw_files: NAS 파일 스캔 및 수집 상태 관리
-- ============================================================
CREATE TABLE IF NOT EXISTS raw_files (
    asset_id        VARCHAR PRIMARY KEY,
    source_path     VARCHAR NOT NULL,
    original_name   VARCHAR,
    media_type      VARCHAR DEFAULT 'image',
    file_size       BIGINT,
    checksum        VARCHAR UNIQUE,
    phash           VARCHAR,
    dup_group_id    VARCHAR,
    archive_path    VARCHAR,
    raw_bucket      VARCHAR DEFAULT 'vlm-raw',
    raw_key         VARCHAR,
    ingest_batch_id VARCHAR,
    transfer_tool   VARCHAR DEFAULT 'manual',
    ingest_status   VARCHAR DEFAULT 'pending',
    error_message   TEXT,
    spec_id         VARCHAR,
    source_unit_name VARCHAR,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 2. video_metadata: 원본 비디오 메타데이터 (ffprobe)
-- ============================================================
CREATE TABLE IF NOT EXISTS video_metadata (
    asset_id        VARCHAR PRIMARY KEY REFERENCES raw_files(asset_id),
    width           INTEGER,
    height          INTEGER,
    duration_sec    DOUBLE,
    fps             DOUBLE,
    codec           VARCHAR,
    bitrate         BIGINT,
    frame_count     INTEGER,
    has_audio       BOOLEAN DEFAULT FALSE,
    environment_type VARCHAR,
    daynight_type    VARCHAR,
    outdoor_score    DOUBLE,
    avg_brightness   DOUBLE,
    env_method       VARCHAR,
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    frame_extract_status VARCHAR DEFAULT 'pending',
    frame_extract_count  INTEGER DEFAULT 0,
    frame_extract_error  TEXT,
    frame_extracted_at   TIMESTAMP,
    auto_label_status    VARCHAR DEFAULT 'pending',
    auto_label_error     TEXT,
    auto_label_key       VARCHAR,
    auto_labeled_at      TIMESTAMP,
    timestamp_status     VARCHAR DEFAULT 'pending',
    timestamp_error      TEXT,
    timestamp_label_key  VARCHAR,
    timestamp_completed_at TIMESTAMP,
    caption_status       VARCHAR DEFAULT 'pending',
    caption_error        TEXT,
    caption_completed_at TIMESTAMP,
    frame_status         VARCHAR DEFAULT 'pending',
    frame_error          TEXT,
    frame_completed_at   TIMESTAMP,
    bbox_status          VARCHAR DEFAULT 'pending',
    bbox_error           TEXT,
    bbox_completed_at    TIMESTAMP
);

-- ============================================================
-- 3. labels: 라벨 데이터 관리 (LABEL 단계에서 INSERT)
-- ============================================================
CREATE TABLE IF NOT EXISTS labels (
    label_id        VARCHAR PRIMARY KEY,
    asset_id        VARCHAR REFERENCES raw_files(asset_id),
    labels_bucket   VARCHAR DEFAULT 'vlm-labels',
    labels_key      VARCHAR,
    label_format    VARCHAR,
    label_tool      VARCHAR DEFAULT 'pre-built',
    label_source    VARCHAR DEFAULT 'manual',
    review_status   VARCHAR DEFAULT 'pending',
    event_index     INTEGER DEFAULT 0,
    event_count     INTEGER,
    timestamp_start_sec DOUBLE,
    timestamp_end_sec   DOUBLE,
    caption_text    TEXT,
    object_count    INTEGER DEFAULT 0,
    label_status    VARCHAR DEFAULT 'completed',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 4. processed_clips: 전처리 결과 (clip_metadata 흡수)
-- ============================================================
CREATE TABLE IF NOT EXISTS processed_clips (
    clip_id          VARCHAR PRIMARY KEY,
    source_asset_id  VARCHAR REFERENCES raw_files(asset_id),
    source_label_id  VARCHAR REFERENCES labels(label_id),
    event_index      INTEGER DEFAULT 0,
    clip_start_sec   DOUBLE,
    clip_end_sec     DOUBLE,
    checksum         VARCHAR UNIQUE,
    file_size        BIGINT,
    processed_bucket VARCHAR DEFAULT 'vlm-processed',
    clip_key         VARCHAR,
    label_key        VARCHAR,
    data_source      VARCHAR DEFAULT 'manual',
    caption_text     TEXT,
    width            INTEGER,
    height           INTEGER,
    codec            VARCHAR,
    duration_sec     DOUBLE,
    fps              DOUBLE,
    frame_count      INTEGER,
    image_extract_status  VARCHAR DEFAULT 'pending',
    image_extract_count   INTEGER DEFAULT 0,
    image_extract_error   TEXT,
    image_extracted_at    TIMESTAMP,
    process_status   VARCHAR DEFAULT 'pending',
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 5. image_metadata: 원본 이미지 + 비디오 프레임 메타데이터
-- ============================================================
CREATE TABLE IF NOT EXISTS image_metadata (
    image_id         VARCHAR PRIMARY KEY,
    source_asset_id  VARCHAR NOT NULL REFERENCES raw_files(asset_id),
    source_clip_id   VARCHAR REFERENCES processed_clips(clip_id),
    image_bucket     VARCHAR DEFAULT 'vlm-raw',
    image_key        VARCHAR,
    image_role       VARCHAR DEFAULT 'source_image',
    frame_index      INTEGER,
    frame_sec        DOUBLE,
    checksum         VARCHAR,
    file_size        BIGINT,
    width            INTEGER,
    height           INTEGER,
    color_mode       VARCHAR DEFAULT 'RGB',
    bit_depth        INTEGER DEFAULT 8,
    has_alpha        BOOLEAN DEFAULT FALSE,
    orientation      INTEGER DEFAULT 1,
    caption_text     TEXT,
    image_caption_text TEXT,
    image_caption_score DOUBLE,
    extracted_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (image_bucket, image_key),
    UNIQUE (source_asset_id, source_clip_id, image_role, frame_index)
);

-- ============================================================
-- 6. datasets: 데이터셋 구성 관리
-- ============================================================
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id      VARCHAR PRIMARY KEY,
    name            VARCHAR,
    version         VARCHAR,
    config          JSON,
    split_ratio     JSON DEFAULT '{"train":0.8,"val":0.1,"test":0.1}',
    dataset_bucket  VARCHAR DEFAULT 'vlm-dataset',
    dataset_prefix  VARCHAR,
    build_status    VARCHAR DEFAULT 'pending',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 7. dataset_clips: 데이터셋-클립 연결 (M:N)
-- ============================================================
CREATE TABLE IF NOT EXISTS dataset_clips (
    dataset_id      VARCHAR REFERENCES datasets(dataset_id),
    clip_id         VARCHAR REFERENCES processed_clips(clip_id),
    split           VARCHAR,
    dataset_key     VARCHAR,
    PRIMARY KEY (dataset_id, clip_id)
);

-- ============================================================
-- 8. image_labels: 이미지 detection 라벨 (YOLO-world 등)
-- ============================================================
CREATE TABLE IF NOT EXISTS image_labels (
    image_label_id   VARCHAR PRIMARY KEY,
    image_id         VARCHAR REFERENCES image_metadata(image_id),
    source_clip_id   VARCHAR REFERENCES processed_clips(clip_id),
    labels_bucket    VARCHAR DEFAULT 'vlm-labels',
    labels_key       VARCHAR,
    label_format     VARCHAR,
    label_tool       VARCHAR,
    label_source     VARCHAR,
    review_status    VARCHAR DEFAULT 'pending',
    label_status     VARCHAR DEFAULT 'pending',
    object_count     INTEGER DEFAULT 0,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 9. staging_dispatch_requests: Dispatch 요청 추적 (dispatch_sensor DDL과 동일)
--    trigger JSON → archive 이동 + 파이프라인 실행
-- ============================================================
CREATE TABLE IF NOT EXISTS staging_dispatch_requests (
    request_id           VARCHAR PRIMARY KEY,
    folder_name          VARCHAR,
    run_mode             VARCHAR,
    outputs              VARCHAR,          -- 쉼표 구분: bbox, timestamp, captioning
    labeling_method      VARCHAR,
    categories           VARCHAR,
    classes              VARCHAR,
    image_profile        VARCHAR,
    status               VARCHAR DEFAULT 'pending',
    archive_pending_path VARCHAR,
    archive_path         VARCHAR,
    -- YOLO 이미지 추출 파라미터 (NULL이면 기본값 사용)
    max_frames_per_video INTEGER,
    jpeg_quality         INTEGER,
    confidence_threshold DOUBLE,
    iou_threshold        DOUBLE,
    -- 메타
    requested_by         VARCHAR,
    requested_at         TIMESTAMP,
    processed_at         TIMESTAMP,
    completed_at         TIMESTAMP,
    error_message        TEXT,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 10. staging_model_configs: output 타입별 모델 선택 + 기본 파라미터
--     outputs 값(bbox, timestamp, captioning)에 따라 사용 모델 결정
-- ============================================================
CREATE TABLE IF NOT EXISTS staging_model_configs (
    config_id            VARCHAR PRIMARY KEY,
    output_type          VARCHAR NOT NULL UNIQUE,  -- bbox | timestamp | captioning
    model_name           VARCHAR NOT NULL,         -- yolov8l-worldv2 | gemini-2.0-flash 등
    model_version        VARCHAR,
    -- 이미지 추출 기본 파라미터 (dispatch JSON에 값이 없을 때 사용)
    default_max_frames   INTEGER DEFAULT 24,
    default_jpeg_quality INTEGER DEFAULT 90,
    -- 모델별 기본 파라미터
    default_confidence   DOUBLE DEFAULT 0.25,
    default_iou          DOUBLE DEFAULT 0.45,
    -- 추가 설정 (JSON)
    extra_params         VARCHAR,                  -- JSON 문자열로 확장 파라미터 저장
    is_active            BOOLEAN DEFAULT TRUE,
    description          TEXT,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 11. staging_pipeline_runs: Dispatch 요청별 파이프라인 단계 진행 추적
--     archive 이동 → 모델 실행 → 완료 전 과정 기록
-- ============================================================
CREATE TABLE IF NOT EXISTS staging_pipeline_runs (
    run_id               VARCHAR PRIMARY KEY,
    request_id           VARCHAR,
    folder_name          VARCHAR,
    -- 단계 상태
    step_name            VARCHAR NOT NULL,         -- archive_move | frame_extract | yolo_detect | gemini_timestamp | gemini_caption | build_dataset
    step_order           INTEGER DEFAULT 0,
    step_status          VARCHAR DEFAULT 'pending', -- pending | running | completed | failed | skipped
    -- 모델 정보
    model_name           VARCHAR,
    model_version        VARCHAR,
    -- 사용된 파라미터 (실제 적용값 기록)
    applied_params       VARCHAR,                  -- JSON: {"max_frames":24,"jpeg_quality":90,...}
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
    spec_id              VARCHAR PRIMARY KEY,
    requester_id         VARCHAR,
    team_id              VARCHAR,
    source_unit_name     VARCHAR,
    categories           JSON,                     -- category array
    classes              JSON,                     -- resolved class array (derived from categories)
    labeling_method      JSON,                     -- ["timestamp","captioning","bbox"] or []
    spec_status          VARCHAR DEFAULT 'pending', -- pending | pending_resolved | active | completed | failed
    retry_count          INTEGER DEFAULT 0,
    resolved_config_id   VARCHAR,
    resolved_config_scope VARCHAR,                 -- personal | team | fallback
    last_error           TEXT,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 13. labeling_configs: config/parameters/*.json 동기화
CREATE TABLE IF NOT EXISTS labeling_configs (
    config_id            VARCHAR PRIMARY KEY,
    config_json          JSON NOT NULL,
    version              INTEGER DEFAULT 1,
    is_active            BOOLEAN DEFAULT TRUE,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 14. requester_config_map: requester/team → config 매핑 (personal → team → _fallback)
CREATE TABLE IF NOT EXISTS requester_config_map (
    map_id               VARCHAR PRIMARY KEY,
    requester_id         VARCHAR,
    team_id              VARCHAR,
    scope                VARCHAR NOT NULL,          -- personal | team
    config_id            VARCHAR NOT NULL,
    is_active            BOOLEAN DEFAULT TRUE,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
