┌─────────────────────────────────────────────────────────────────────────────┐
│               VLM Data Pipeline — MVP 아키텍처 (2026-02-20)                │
└─────────────────────────────────────────────────────────────────────────────┘

  파이프라인 단계:
    ingested_raw_files → dedup_results → labeled_files → processed_clips → built_dataset → motherduck_sync(옵션)

  핵심 변경 (2026-02-13 리팩토링):
    - data_pipeline_job (file_organize → nas_scan → minio_upload) 제거됨
    - mvp_stage_job으로 통합 (INGEST → DEDUP → LABEL → PROCESS → BUILD → SYNC)
    - incoming_manifest_sensor + auto_bootstrap_manifest_sensor 연계 트리거
    - (2026-02-19) auto_bootstrap 안정화 게이트 추가 (복사중 데이터 manifest 생성 차단)
    - (2026-02-19) source_unit_type=directory ingest 시 폴더 단위 archive 이동 지원
    - (2026-02-20) video_metadata에 환경 분류 메타 추가 (indoor/outdoor + day/night)
    - (2026-02-20) Places365 CUDA 우선 + heuristic fallback 도입 (`video_env.py`)
    - (2026-02-20) Docker 런타임에 torch/torchvision cu124 설치 및 GPU 실사용 검증 완료
    - (2026-02-20) Places365 모델 캐시 경로 `/data/models/places365` 고정, auto download 비활성화
    - motherduck_sync_job 단독 실행 경로 유지
    - NAS 폴더 트리 (PostgreSQL) 기능은 별도 유지 (nas_folder_tree_job)
    - 레거시/산출물 정리: `src/rust_scanner`, `src/python/scanner`, `docker/airflow/logs` 제거


┌─────────────────────────────────────────────────────────────────────────────┐
│                       DuckDB 스키마 (Strict 8 테이블)                       │
└─────────────────────────────────────────────────────────────────────────────┘

┌─ raw_files ─────────────────────────────────┐
│ (PK) asset_id        VARCHAR   UUID         │
│      source_path     VARCHAR   원본경로(NAS) │
│      original_name   VARCHAR   원본 파일명   │
│      media_type      VARCHAR   image/video  │
│      file_size       BIGINT    바이트        │
│ (UK) checksum        VARCHAR   SHA-256      │
│      phash           VARCHAR   pHash hex    │
│      dup_group_id    VARCHAR   유사 그룹     │
│      archive_path    VARCHAR   NAS archive  │
│      raw_bucket      VARCHAR   vlm-raw      │
│      raw_key         VARCHAR   MinIO 키      │
│      ingest_batch_id VARCHAR   배치 그룹     │
│      transfer_tool   VARCHAR   manual       │
│      ingest_status   VARCHAR   상태머신      │
│      error_message   VARCHAR   에러 메시지   │
│      created_at      TIMESTAMP 자동          │
│      updated_at      TIMESTAMP 자동          │
├─────────────────────────────────────────────┤
│  1:1 ──> image_metadata     (asset_id)      │
│  1:1 ──> video_metadata     (asset_id)      │
│  1:N ──> labels             (asset_id)      │
│  1:N ──> processed_clips    (source_asset_id)│
└─────────────────────────────────────────────┘

┌─ image_metadata ───────────────────────────┐
│ (PK/FK) asset_id    VARCHAR  → raw_files   │
│         width       INT      픽셀          │
│         height      INT      픽셀          │
│         color_mode  VARCHAR  RGB           │
│         bit_depth   INT      8             │
│         codec       VARCHAR  jpeg/png/heic │
│         has_alpha   BOOLEAN  false         │
│         orientation INT      1-8           │
│         extracted_at TIMESTAMP             │
└────────────────────────────────────────────┘

┌─ video_metadata ───────────────────────────┐
│ (PK/FK) asset_id    VARCHAR  → raw_files   │
│         width       INT      픽셀          │
│         height      INT      픽셀          │
│         duration_sec FLOAT   초             │
│         fps         FLOAT    프레임/초      │
│         codec       VARCHAR  h264/h265     │
│         bitrate     INT      bps           │
│         frame_count INT      총 프레임      │
│         has_audio   BOOLEAN  false         │
│         environment_type VARCHAR indoor/outdoor │
│         daynight_type   VARCHAR day/night       │
│         outdoor_score   FLOAT   0.0~1.0         │
│         avg_brightness  FLOAT   평균 명도       │
│         env_method      VARCHAR places365_cuda/heuristic │
│         extracted_at TIMESTAMP             │
└────────────────────────────────────────────┘

┌─ labels ────────────────────────────────────┐
│ (PK) label_id    VARCHAR  UUID              │
│ (FK) asset_id    VARCHAR  → raw_files       │
│      labels_bucket VARCHAR vlm-labels       │
│      labels_key  VARCHAR  MinIO 키           │
│      label_format VARCHAR COCO/labelme/yolo │
│      label_tool  VARCHAR  pre-built/CVAT    │
│      event_count INT      어노테이션 수      │
│      label_status VARCHAR 상태머신           │
│      created_at  TIMESTAMP                  │
└─────────────────────────────────────────────┘

┌─ processed_clips ───────────────────────────┐
│ (PK) clip_id         VARCHAR   UUID         │
│ (FK) source_asset_id VARCHAR   → raw_files  │
│ (FK) source_label_id VARCHAR   → labels     │
│      event_index     INT       이미지: 0     │
│ (UK) checksum        VARCHAR   SHA-256      │
│      file_size       BIGINT    바이트        │
│      processed_bucket VARCHAR  vlm-processed│
│      clip_key        VARCHAR   MinIO 키      │
│      label_key       VARCHAR   라벨 MinIO키  │
│      width           INT       픽셀 ★흡수   │
│      height          INT       픽셀 ★흡수   │
│      codec           VARCHAR   코덱 ★흡수   │
│      process_status  VARCHAR   상태머신      │
│      created_at      TIMESTAMP 자동          │
└─────────────────────────────────────────────┘

┌─ datasets ──────────────────────────────────┐
│ (PK) dataset_id     VARCHAR   UUID          │
│      name           VARCHAR   이름          │
│      version        VARCHAR   v0.1          │
│      config         JSON      구성 옵션     │
│      split_ratio    JSON      80/10/10      │
│      dataset_bucket VARCHAR   vlm-dataset   │
│      dataset_prefix VARCHAR   경로 접두사    │
│      build_status   VARCHAR   상태머신      │
│      created_at     TIMESTAMP 자동          │
└─────────────────────────────────────────────┘

┌─ dataset_clips ─────────────────────────────┐
│ (PK/FK) dataset_id VARCHAR  → datasets      │
│ (PK/FK) clip_id    VARCHAR  → processed_clips│
│         split      VARCHAR  train/val/test  │
│         dataset_key VARCHAR MinIO 키         │
└─────────────────────────────────────────────┘

테이블 관계 요약:
  raw_files ──1:1──> image_metadata     (asset_id)
  raw_files ──1:1──> video_metadata     (asset_id)
  raw_files ──1:N──> labels             (asset_id)
  raw_files ──1:N──> processed_clips    (source_asset_id)
  labels    ──1:N──> processed_clips    (source_label_id)
  processed_clips ──1:N──> dataset_clips (clip_id)
  datasets  ──1:N──> dataset_clips      (dataset_id)

  ★ clip_metadata 테이블 제거됨 — width/height/codec이 processed_clips에 흡수


┌─────────────────────────────────────────────────────────────────────────────┐
│                       Dagster 구조 (2026-02-13)                             │
└─────────────────────────────────────────────────────────────────────────────┘

  Jobs:
    mvp_stage_job        — 전체 파이프라인 (INGEST → DEDUP → LABEL → PROCESS → BUILD → SYNC)
    ingest_job           — INGEST 단독 실행
    dedup_job            — DEDUP 단독 실행
    label_job            — LABEL 단독 실행
    process_build_job    — PROCESS + BUILD 실행
    motherduck_sync_job  — MotherDuck sync 단독 실행
    nas_folder_tree_job  — NAS 폴더 트리 → PostgreSQL (기존 유지)

  Sensors:
    incoming_manifest_sensor — .manifests/pending/*.json 폴링 → mvp_stage_job 트리거
                               NFS 장애 시 graceful skip (30초 후 재시도)
                               cursor 기반 중복 방지
    auto_bootstrap_manifest_sensor — /nas/incoming 미디어를 source unit(폴더/파일)으로 스캔
                                      안정화(cycles/age) 충족 시에만 manifest 자동 생성

  Schedules:
    nas_folder_tree_schedule — 평일 18:00 NAS 폴더 트리 스캔
    (mvp_stage schedule 제거 — sensor 기반 트리거만 사용)

  제거된 항목:
    data_pipeline_job    — file_organize → nas_scan → minio_upload (제거됨)
    관련 assets          — nas_scan, minio_upload, file_organize (제거됨)
    (motherduck_sync는 재연결되어 운영 중)


┌─────────────────────────────────────────────────────────────────────────────┐
│                       코드 구조 (2026-02-13)                                │
└─────────────────────────────────────────────────────────────────────────────┘

  docker/app/dagster_defs.py          Dagster 메인 정의 (Docker 컨테이너용)
  src/vlm_pipeline/                   MVP 파이프라인 패키지 (독립 실행 가능)
   ├ definitions.py                   Dagster Definitions 조립 (Layer 5)
   ├ resources/
   │  ├ config.py                     PipelineConfig (Pydantic BaseSettings)
   │  ├ duckdb.py                     DuckDBResource (ConfigurableResource)
   │  └ minio.py                      MinIOResource (ConfigurableResource)
   ├ defs/
   │  ├ ingest/
   │  │  ├ assets.py                  ingested_raw_files @asset (Layer 4)
   │  │  ├ ops.py                     register_incoming, normalize_and_archive, ingest_summary @op (Layer 3)
   │  │  └ sensor.py                  incoming_manifest_sensor (Layer 4)
   │  ├ dedup/
   │  │  └ assets.py                  dedup_results @asset (Layer 4)
   │  ├ label/
   │  │  └ assets.py                  labeled_files @asset (Layer 4)
   │  ├ process/
   │  │  └ assets.py                  processed_clips @asset (Layer 4)
   │  └ build/
   │     └ assets.py                  built_dataset @asset (Layer 4)
   ├ lib/                             순수 Python 유틸리티 (Layer 1-2)
   │  ├ sanitizer.py                  파일/경로 세그먼트 정규화 (한글→로마자, ASCII-safe)
   │  ├ validator.py                  파일 검증 (확장자, 크기, 읽기 가능성)
   │  ├ checksum.py                   SHA-256 체크섬
   │  ├ file_loader.py                1-pass 이미지 로딩 + 메타 추출
   │  ├ video_loader.py               ffprobe + video 환경 메타 추출 연결
   │  ├ video_env.py                  Places365(CUDA 우선) + heuristic voting
   │  └ phash.py                      pHash 계산 + Hamming distance
   └ sql/
      ├ schema.sql                    Strict 8 테이블 DDL
      └ migration.sql                 asset_catalog → raw_files 마이그레이션

  src/python/common/                  기존 공유 유틸리티 (레거시, 점진적 전환 대상)
  tests/
   ├ unit/                            단위 테스트
   ├ integration/                     통합 테스트
   └ e2e/                             E2E 테스트
  제거 완료:
    - `src/rust_scanner/` (레거시 Rust 스캐너)
    - `src/python/scanner/` (레거시 파이썬 스캐너 잔여)
    - `docker/airflow/logs/` (대용량 런타임 로그 산출물)

  Import 계층 (5-layer):
    Layer 1: lib/ (순수 Python, 외부 의존성 없음)
    Layer 2: lib/ (외부 라이브러리 의존: PIL, imagehash 등)
    Layer 3: ops (Dagster @op, lib/ + resources/ import)
    Layer 4: assets/sensors (Dagster @asset/@sensor, ops import)
    Layer 5: definitions.py (모든 것을 조립)


┌─────────────────────────────────────────────────────────────────────────────┐
│                       환경변수 (docker-compose.yml)                         │
└─────────────────────────────────────────────────────────────────────────────┘

  DATAOPS_DUCKDB_PATH          /data/pipeline.duckdb
  MINIO_ENDPOINT               http://minio:9000
  MINIO_ACCESS_KEY             minioadmin
  MINIO_SECRET_KEY             minioadmin
  INCOMING_DIR                 /nas/incoming
  ARCHIVE_DIR                  /nas/archive
  DATAOPS_MANIFEST_DIR         /nas/incoming/.manifests
  DATAOPS_ASSET_PATH_PREFIX_FROM  (세미콜론으로 다중 prefix)
  DATAOPS_ASSET_PATH_PREFIX_TO    (canonical path 치환 대상)
  MOTHERDUCK_TOKEN             MotherDuck API 토큰
  MOTHERDUCK_DB                동기화 대상 DB명 (기본: pipeline_db)
  MOTHERDUCK_SYNC_ENABLED      true/false (옵션 sync on/off)
  MOTHERDUCK_SYNC_DRY_RUN      true/false
  MOTHERDUCK_SHARE_UPDATE      MANUAL | AUTOMATIC
  MOTHERDUCK_SYNC_TIMEOUT_SEC  sync subprocess timeout (기본: 600초)
  INCOMING_SENSOR_INTERVAL_SEC incoming manifest sensor 주기(초) (기본: 180)
  AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC auto bootstrap sensor 주기(초) (기본: 180)
  AUTO_BOOTSTRAP_STABLE_CYCLES source unit 안정화 연속 사이클 수 (기본: 2)
  AUTO_BOOTSTRAP_STABLE_AGE_SEC 마지막 수정 후 대기 시간(초) (기본: 120)
  AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK auto bootstrap 상세 스캔 unit 수 상한 (기본: 5)
  AUTO_BOOTSTRAP_MAX_READY_UNITS_PER_TICK tick당 manifest 생성 대상 unit 상한 (기본: 5)
  AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS pending backlog 보호 상한 (기본: 200)
  AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK tick당 신규 manifest 생성 상한 (기본: 20)
  AUTO_BOOTSTRAP_MAX_FILES_PER_MANIFEST auto manifest 분할 크기 (기본: 100)
  AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES discovery 틱당 상위 디렉터리 수 (0=무제한). NAS 지연 시 15~30 권장
  DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS    센서 gRPC 타임아웃(초). 기본 180. NAS 지연 시 300 등으로 상향
  INCOMING_SENSOR_MAX_IN_FLIGHT_RUNS incoming enqueue backpressure 상한 (기본: 2)
  INCOMING_SENSOR_MAX_RETRY_PER_MANIFEST failed manifest 자동 재시도 상한 (기본: 3)
  INGEST_FAILURE_LOG_DIR       ingest 실패 JSONL 로그 경로 (기본: <manifest_dir>/failed)
  INGEST_TRANSIENT_RETRY_MAX_ATTEMPTS transient retry manifest 최대 시도 (기본: 3)
  GCS_ZERO_BYTE_RETRIES        GCS 다운로드 후 0바이트 파일 복구 재시도 (기본: 2)
  VIDEO_ENV_MODEL_DIR          Places365 모델 캐시 경로 (`/data/models/places365`)
  VIDEO_ENV_AUTO_DOWNLOAD      false (운영: 고정 캐시만 사용)
  VIDEO_ENV_USE_PLACES365      true/false (기본 true)
  VIDEO_ENV_SAMPLE_COUNT       비디오당 샘플 프레임 수 (기본 3)
  VIDEO_DAY_NIGHT_THRESHOLD    day/night 임계값 (기본 90)
  VIDEO_ENV_TOP_K              Places365 top-k 평균 (기본 10)


┌─────────────────────────────────────────────────────────────────────────────┐
│                       상태머신 (Status Transitions)                         │
└─────────────────────────────────────────────────────────────────────────────┘

  raw_files.ingest_status (보존 행 기준):
    pending → uploading → completed
    ※ 중복/검증실패/미이동 파일은 raw_files에 보존하지 않음 (sync 대상 제외)

  labels.label_status:
    pending → completed
    pending → failed

  processed_clips.process_status:
    pending → completed
    pending → failed

  datasets.build_status:
    pending → building → completed
    building → failed


┌─────────────────────────────────────────────────────────────────────────────┐
│                       운영 규칙                                             │
└─────────────────────────────────────────────────────────────────────────────┘

  DuckDB 동시성:
    - DuckDB는 단일 파일 write lock 기반
    - Dagster job에 tags={"duckdb_writer": "true"} 적용
    - run_coordinator: max_concurrent_runs=4 + tag_concurrency_limits(duckdb_writer=true, limit=1)

  NFS 장애 대응:
    - sensor에서 OSError/PermissionError/TimeoutError 시 graceful skip
    - 다음 폴링 주기에 자동 재시도
      (`incoming_manifest_sensor=INCOMING_SENSOR_INTERVAL_SEC(기본 180초)`,
       `auto_bootstrap_manifest_sensor=AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC(기본 180초)`)

  Incoming/Archive 운영:
    - auto_bootstrap는 source unit 시그니처(file_count/total_size/max_mtime) 기준 안정화 후 manifest 생성
    - auto_bootstrap는 전수 파일 스캔 대신 unit 경량 탐색 후 scan window 단위로 상세 스캔
      (`AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK`)
    - ready unit이 많은 tick은 상한까지만 manifest 생성하고 나머지는 deferred 처리
      (`AUTO_BOOTSTRAP_MAX_READY_UNITS_PER_TICK`)
    - source unit 파일이 많은 경우 `AUTO_BOOTSTRAP_MAX_FILES_PER_MANIFEST` 기준으로 chunk manifest 분할 생성
    - chunk manifest는 `source_unit_dispatch_key` 단위로 enqueue/중복방지 처리
    - pending backlog 보호:
      - pending 개수가 `AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS` 이상이면 auto_bootstrap 생성 중단
      - 한 tick에서 `AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK` 만큼만 신규 manifest 생성
    - `source_unit_type=directory` manifest는 전 파일 성공 시 폴더째 `/nas/archive/<source_unit_name>`로 이동
    - chunked manifest(`source_unit_chunk_count>1`)는 파일 단위 누적 이동으로 충돌/조기 폴더 이동 방지
    - `source_unit_type=directory`에서 `failed=0, skipped>0`이면 성공 파일만 부분 이동
    - archive 대상 폴더명 충돌 시 `__2`, `__3` suffix로 경로 자동 분기
    - legacy archive(`YYYY/MM`) 구조는 조회 fallback으로만 유지
    - 중복/검증실패/이동실패 등 미이동 파일은 incoming 유지 + DB 레코드 정리
    - 파일 오류(`file_missing`, `empty_file`, `ffprobe_failed` 등)는
      `raw_files/video_metadata/image_metadata`에 failed row를 남기지 않음
    - 파일 오류는 archive로 이동하지 않고 incoming에 유지하며,
      추적은 `<manifest_dir>/failed/<manifest_id>.jsonl`에 기록
    - transient 오류(DuckDB lock/conflict)는 failed row 대신 retry manifest로 분리 재시도:
      `retry_<원manifest_id>_<UTC timestamp>.json`
      (`retry_of_manifest_id`, `retry_attempt`, `retry_reason=transient_db_lock`)
    - retry manifest 생성은 `INGEST_TRANSIENT_RETRY_MAX_ATTEMPTS` 초과 시 중단
    - archive 이동 예외 발생 시 목적지 파일 존재를 즉시 재확인하고,
      파일이 존재하면 `completed + archive_path`로 복구 처리
    - archive로 실제 이동 완료된 파일만 `raw_files.ingest_status=completed` 유지
    - `duplicate_skipped_in_manifest:*` 안내 메시지는 duplicate 관련 `asset_id`에만 기록
    - 기존 duplicate marker가 있으면 파일명 목록을 merge 누적, 일반 오류 메시지는 유지

  MotherDuck sync 운영:
    - `pipeline__motherduck_sync`는 env/run_config를 모두 수용 (enabled/db/dry_run/share_update/timeout_sec)
    - subprocess timeout 시 run을 실패 처리해 큐 정체를 방지
    - `MOTHERDUCK_TOKEN` 미설정 시 graceful skip
    - 기본 동기화 대상 DB는 `pipeline_db`
    - sync 시 legacy 테이블(`clip_metadata`) 자동 제거 후 MVP 테이블 동기화

  DEDUP 운영:
    - pHash 계산 입력 소스 우선순위: `archive_path` -> `source_path` -> MinIO(`raw_bucket/raw_key`)
    - archive 이동 이후에도 pHash/dup_group_id 갱신이 계속 가능하도록 설계
    - pHash 성공 시 `error_message` 정리(기존 `phash_source_missing` 잔여 메시지 제거)

  MinIO 버킷 정책:
    - vlm-raw: 원본 미디어 (INGEST 단계)
    - vlm-labels: 라벨 JSON (LABEL 단계)
    - vlm-processed: 가공 결과 (PROCESS 단계)
    - vlm-dataset: 최종 데이터셋 (BUILD 단계)
    - raw_key는 파일명 + 폴더 세그먼트 모두 ASCII-safe 정규화 후 저장

  DuckDB 경로:
    - 컨테이너 내부: /data/pipeline.duckdb
    - 호스트 실제 파일: ./docker/data/pipeline.duckdb
    - 1순위: DATAOPS_DUCKDB_PATH (또는 DUCKDB_PATH)
    - 2순위: 컨테이너 환경이면 /data/pipeline.duckdb
    - 3순위: configs/global.yaml duckdb_path


┌─────────────────────────────────────────────────────────────────────────────┐
│                 장애 대응 플레이북 (문제 해결력 강화용)                    │
└─────────────────────────────────────────────────────────────────────────────┘

  1) 장애 분류부터 고정:
    - 파일 자체 오류: `file_missing`, `empty_file`, `ffprobe_failed`
    - 일시 시스템 오류: DuckDB lock/conflict, timeout
    - 데이터 정합 오류: `raw_files` vs `video_metadata` count mismatch

  2) 즉시 오염 차단:
    - queue 폭주가 보이면 sensor 주기/상한으로 생성 속도를 먼저 낮춤
      (`INCOMING_SENSOR_INTERVAL_SEC`, `AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC`,
       `AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS`, `AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK`)
    - 파일 오류는 DB 미삽입 + 미이동 정책으로 추가 오염을 막음

  3) 근거 기반 확인(SQL + 로그):
    - status/error별 건수 확인 후 우선순위 결정
    - 실패 상세는 DB보다 JSONL 로그(`<manifest_dir>/failed/*.jsonl`)를 기준으로 원인 추적
    - GCS 수집 이슈는 0바이트 파일 탐지 결과를 먼저 확인

  4) 재처리 경로 분리:
    - 파일 오류: 원본 파일 복구/교체 후 새 manifest로 재수집
    - transient 오류: retry manifest 자동 생성분 우선 소비
    - archive 예외: 목적지 실존 여부 확인 후 `completed` 복구 가능 여부 먼저 판단

  5) 마감 기준(필수):
    - 로컬 DuckDB: `failed`, `missing metadata` 목표치 확인
    - MotherDuck sync 실행 후 동일 쿼리 재검증
    - pre/post 수치(예: raw/video/failed)를 WORKLOG와 함께 기록


┌─────────────────────────────────────────────────────────────────────────────┐
│               장애 유형별 즉시 실행 체크리스트 (SQL/명령어)                │
└─────────────────────────────────────────────────────────────────────────────┘

  공통:
    - 읽기 쿼리(락 회피 fallback 포함):
      - `python3 scripts/query_local_duckdb.py --sql "<SQL>"`
    - 로컬 DB 경로 기본값:
      - 호스트: `./docker/data/pipeline.duckdb`
      - 컨테이너: `/data/pipeline.duckdb`
    - manifest 기본 경로:
      - `pending=/nas/incoming/.manifests/pending`
      - `failed=/nas/incoming/.manifests/failed`

  1) `raw_files` vs `video_metadata` 개수 불일치
    - 확인:
      - `python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) AS raw_files FROM raw_files;"`
      - `python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) AS video_metadata FROM video_metadata;"`
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT COUNT(*) AS missing_video_meta FROM raw_files rf LEFT JOIN video_metadata vm ON rf.asset_id = vm.asset_id WHERE rf.media_type='video' AND vm.asset_id IS NULL;\"`
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT rf.asset_id, rf.ingest_status, rf.source_path, rf.archive_path FROM raw_files rf LEFT JOIN video_metadata vm ON rf.asset_id = vm.asset_id WHERE rf.media_type='video' AND vm.asset_id IS NULL ORDER BY rf.created_at DESC LIMIT 30;\"`
    - 조치:
      - 결손 메타 백필 실행(스크립트):
        - 호스트: `python3 scripts/backfill_video_metadata.py --db ./docker/data/pipeline.duckdb --statuses completed --log-every 20`
        - 컨테이너: `python3 /src/vlm/scripts/backfill_video_metadata.py --db /data/pipeline.duckdb --statuses completed --log-every 20`
    - 완료 조건:
      - `missing_video_meta=0`

  2) `failed` 급증(파일 오류/기타 오류)
    - 확인:
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT ingest_status, COUNT(*) FROM raw_files GROUP BY 1 ORDER BY 1;\"`
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT COALESCE(error_message,'(null)') AS error_message, COUNT(*) AS cnt FROM raw_files WHERE ingest_status='failed' GROUP BY 1 ORDER BY cnt DESC LIMIT 30;\"`
      - `ls -lt /nas/incoming/.manifests/failed/*.jsonl 2>/dev/null | head`
    - 조치:
      - 파일 오류(`file_missing`, `empty_file`, `ffprobe_failed`)는 DB 삽입 대상이 아니므로
        원본 파일 상태/복사 완료 여부를 먼저 복구 후 재수집
      - 재수집은 원본 manifest 재발행 또는 retry manifest 경로 사용
    - 완료 조건:
      - 신규 실행 기준 동일 오류 재발 없음 + 실패 로그만 남고 DB 오염 없음

  3) DuckDB lock/conflict (`Could not set lock on file`, `Conflicting lock is held`)
    - 확인:
      - `lsof ./docker/data/pipeline.duckdb`
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT COUNT(*) FROM raw_files;\"`
      - `ls -1 /nas/incoming/.manifests/pending/retry_*.json 2>/dev/null | tail -n 20`
    - 조치:
      - writer run이 진행 중이면 선종료를 기다리고, 장기 점유 run만 정리
      - transient 오류는 failed row 대신 retry manifest 자동 생성되는지 확인
      - queue 과적재 시 아래 5번(backpressure) 값으로 즉시 완화
    - 완료 조건:
      - lock 오류가 retry manifest로 흡수되고 raw_files failed 누적이 증가하지 않음

  4) archive 이동 실패/이동 누락
    - 확인:
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT asset_id, source_path, archive_path, error_message FROM raw_files WHERE ingest_status='failed' AND error_message LIKE 'archive_move_failed%' ORDER BY updated_at DESC LIMIT 30;\"`
      - `python3 scripts/query_local_duckdb.py --sql \"SELECT asset_id, source_path, archive_path, error_message FROM raw_files WHERE error_message='archive_source_missing' ORDER BY updated_at DESC LIMIT 30;\"`
    - 조치:
      - 먼저 archive 실존 여부 확인(`dest.exists()` 정책과 동일):
        - `find /nas/archive -type f -name '<파일명>' | head`
      - archive 실존 시 `completed + archive_path`로 복구
      - archive 미존재 시 incoming 원본 기준으로 재처리(manifest 재발행)
    - 완료 조건:
      - archive 존재 건의 상태가 `completed`로 정리되고 orphan row 없음

  5) pending queue 과적재(센서 주기/생성 속도 불균형)
    - 확인:
      - `find /nas/incoming/.manifests/pending -maxdepth 1 -name '*.json' | wc -l`
      - `ls -lt /nas/incoming/.manifests/pending/*.json 2>/dev/null | head -n 20`
    - 조치(권장 기본값):
      - `INCOMING_SENSOR_INTERVAL_SEC=180`
      - `AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC=180`
      - `AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS=200`
      - `AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK=20`
      - `INCOMING_SENSOR_MAX_IN_FLIGHT_RUNS=2`
    - 완료 조건:
      - pending backlog가 안정 구간으로 회복되고 lock/conflict 재발 빈도 감소

  6) GCS 다운로드 0바이트 파일
    - 확인:
      - `find /nas/incoming/gcp -type f \\( -iname '*.mp4' -o -iname '*.mov' -o -iname '*.avi' -o -iname '*.mkv' -o -iname '*.jpg' -o -iname '*.jpeg' -o -iname '*.png' \\) -size 0 | head -n 30`
      - `rg -n \"zero-byte\" gcp/download_from_gcs_rclone.py`
    - 조치:
      - 재시도 상향 실행(필요 시):
        - `python3 gcp/download_from_gcs_rclone.py --download --mode date-folders --download-dir /nas/incoming/gcp --buckets khon-kaen-rtsp-bucket --zero-byte-retries 4`
      - 실패 폴더는 `_DONE` 미생성 상태를 유지하고 재다운로드 대상에 남기는지 확인
    - 완료 조건:
      - 0바이트 미디어 파일 0건 + 대상 폴더 정상 `_DONE` 생성

  7) Local vs MotherDuck 불일치
    - 확인:
      - 로컬:
        - `python3 scripts/query_local_duckdb.py --sql \"SELECT COUNT(*) AS raw_files FROM raw_files;\"`
        - `python3 scripts/query_local_duckdb.py --sql \"SELECT COUNT(*) AS video_metadata FROM video_metadata;\"`
        - `python3 scripts/query_local_duckdb.py --sql \"SELECT COUNT(*) AS failed FROM raw_files WHERE ingest_status='failed';\"`
      - MotherDuck:
        - `motherduck_sync_job` 실행 후 동일 count SQL 재확인
    - 조치:
      - sync 전에 로컬 이상치부터 정리(실패 정책 위반 row/메타 결손)
      - sync 실행 후 동일 쿼리로 local/cloud 동시 검증
    - 완료 조건:
      - `raw_files`, `video_metadata`, `failed` 핵심 지표가 local=motherduck


┌─────────────────────────────────────────────────────────────────────────────┐
│                       코딩 표준                                             │
└─────────────────────────────────────────────────────────────────────────────┘

  - Python 3.11+, ruff formatter/linter
  - Dagster: @asset > @op+@job (선언적 우선)
  - ConfigurableResource: DuckDBResource, MinIOResource
  - Pydantic BaseSettings: PipelineConfig (환경변수 자동 바인딩)
  - 테스트: pytest, in-memory DuckDB fixture, mocked MinIO
  - 커밋 메시지: conventional commits (feat:, fix:, refactor:, test:, docs:)
  - 에러 처리: per-file fail-forward (한 파일 실패해도 나머지 계속 처리)


┌─────────────────────────────────────────────────────────────────────────────┐
│                       검증 이력 (2026-02-12 ~ 2026-03-06)                   │
└─────────────────────────────────────────────────────────────────────────────┘

  [DONE] MinIO endpoint 콘솔/API 분리 정정 + 환경 통일 (2026-03-06)
    - 이슈:
      - MinIO 접속 포트가 혼재되어 업로드/검증 경로가 불안정
      - 콘솔 포트(9001)와 S3 API 포트(9000) 혼동
    - 조치:
      - `docker/docker-compose.yaml`, `.env.example`, `docker/.env`
        - `MINIO_ENDPOINT=http://172.168.47.36:9000` 통일
      - Dagster/app 런타임에서 동일 endpoint 사용하도록 재기동/재생성
    - 검증:
      - 실행 환경변수와 업로드 경로에서 9000 endpoint 일관 사용 확인

  [DONE] INGEST MinIO 업로드 재활성화 + raw_key prefix 제거 (2026-03-06)
    - 이슈:
      - MinIO 업로드가 임시 비활성화 상태였고, 경로 정책을 `{project}/...`로 단순화 필요
    - 조치:
      - `src/vlm_pipeline/defs/ingest/ops.py`
        - MinIO 업로드 호출(메모리/파일객체/fallback) 재활성화
        - raw_key를 `source_unit_name/rel_path` 기준으로 생성 (YYYY/MM 제거)
        - 로그를 `MinIO 업로드 완료`로 복원
    - 검증:
      - 신규 sample key가 `khon-kaen-rtsp-bucket/...`, `adlibhotel-event-bucket/...` 형태로 생성됨

  [DONE] MinIO 경로 회귀 원인(RCA) + 재업로드 복구 절차 확립 (2026-03-06)
    - 이슈:
      - 코드 수정 후에도 일부 업로드가 `vlm-raw/2026/03/...`로 지속
    - 원인:
      - `docker/.env`의 DuckDB 경로 오설정으로 일부 프로세스가 다른 DB를 참조
      - 과거 설정으로 실행 중인 잔존 reupload 프로세스가 이전 key 정책으로 업로드 지속
    - 조치:
      - `docker/.env`를 `DATAOPS_DUCKDB_PATH=/data/pipeline.duckdb`,
        `DUCKDB_PATH=/data/pipeline.duckdb`로 정정
      - 잔존 업로드 프로세스 종료 + 버킷 정리 후 재업로드
      - `scripts/reupload_minio_from_archive.py`로 archive 기준 재정렬 업로드 수행
    - 검증:
      - 신규 업로드 key가 `vlm-raw/{project}/...`로 수렴
      - `2026/03` prefix 재발 없이 진행되는지 sample/progress 로그로 확인

  [DONE] INGEST 오류 파일 미삽입/미이동 + 실패 로그 JSONL 분리 (2026-03-05)
    - 이슈:
      - 파일 자체 오류가 `raw_files.failed`로 누적되어 운영 지표/동기화 정합 오염
    - 조치:
      - `src/vlm_pipeline/defs/ingest/ops.py`
        - register/normalize 단계 파일 오류 시 DB failed row 삽입 제거
      - `src/vlm_pipeline/defs/ingest/assets.py`
        - `INGEST_FAILURE_LOG_DIR` 기반 JSONL 실패 로그 저장
        - 스키마: timestamp, manifest_id, source_path, rel_path, media_type,
          stage, error_code, error_message, retryable
    - 검증:
      - 파일 오류 건은 DB 미삽입 + 미이동, 실패 로그 파일에만 기록 확인

  [DONE] DuckDB transient lock 자동 재시도(manifest) 적용 (2026-03-05)
    - 이슈:
      - lock/conflict 계열 오류가 failed row로 누적되어 재처리 루프가 수동화
    - 조치:
      - `src/vlm_pipeline/defs/ingest/ops.py`
        - transient marker 기반 분류(`Could not set lock on file`, `Conflicting lock is held` 등)
      - `src/vlm_pipeline/defs/ingest/assets.py`
        - transient 실패 파일만 retry manifest 자동 생성
          (`retry_of_manifest_id`, `retry_attempt`, `retry_reason=transient_db_lock`)
        - `INGEST_TRANSIENT_RETRY_MAX_ATTEMPTS`(기본 3) 적용
      - `src/vlm_pipeline/defs/ingest/sensor.py`
        - retry manifest 필드 로드/태그 전파
    - 검증:
      - transient 건은 failed row 대신 retry manifest로 분리되는 것 확인

  [DONE] queue 적체 완화 + GCS 0바이트 복구 + 운영 정합 복구 (2026-03-05)
    - 조치(큐/센서):
      - `INCOMING_SENSOR_INTERVAL_SEC`, `AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC` 기본 180초 적용
      - `AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS`(기본 200),
        `AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK`(기본 20) 적용
    - 조치(GCS):
      - `gcp/download_from_gcs_rclone.py`에 0바이트 미디어 탐지/삭제/재다운로드 루프 추가
      - `GCS_ZERO_BYTE_RETRIES`(기본 2), Dagster `zero_byte_retries` config 연동
    - 운영 데이터 보정:
      - `/data/pipeline.duckdb.bak_20260305_015527` 백업 후 오류 row 정리 및 복구 수행
      - 결과: local/motherduck 모두 `raw_files=8016`, `video_metadata=8016`, `failed=0`

  [DONE] auto_bootstrap scan window + cursor v3 적용 (2026-03-04)
    - 이슈:
      - incoming source unit 증가 구간에서 tick별 전체 스캔 시간이 길어져 sensor 지연 발생
    - 조치:
      - `src/vlm_pipeline/defs/ingest/sensor.py`에 경량 unit 탐색 + 부분 스캔 도입
      - cursor payload를 v3로 확장(`scan_offset`, `units`)
      - `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK`, `AUTO_BOOTSTRAP_MAX_READY_UNITS_PER_TICK` 추가
      - skip/log 메시지에 `scan_window`, `deferred_ready`, `scan_elapsed` 노출
    - 검증:
      - sensor skip reason과 info log에서 scan window/deferred 항목 출력 확인

  [DONE] Archive 루트 경로 단일화 + done marker 탐색 보강 (2026-03-04)
    - 조치:
      - `src/vlm_pipeline/defs/ingest/assets.py` archive 대상 경로를 `/nas/archive` 루트 기준으로 통일
      - `gcp/download_from_gcs_rclone.py` done marker 탐색을
        `archive/<bucket>/<folder>` 우선 + legacy `archive/<YYYY>/<MM>/...` fallback 유지로 정렬
      - `scripts/recover_uploading.py` 복구 이동 경로도 동일 정책 반영

  [DONE] duplicate marker merge 정책 반영 (2026-03-04)
    - 조치:
      - `src/vlm_pipeline/resources/duckdb.py::mark_duplicate_skipped_assets()`
        에서 동일 asset의 duplicate marker 파일명을 누적 병합
      - duplicate marker가 아닌 기존 `error_message`는 덮어쓰지 않도록 보호

  [DONE] GCP 테스트 스코프 제거 + 주기 조정 (2026-03-04)
    - 조치:
      - `src/vlm_pipeline/defs/gcp/assets.py`의 `TEMP_FORCE_TEST_SCOPE` 제거
      - `src/vlm_pipeline/definitions.py`에서 `gcs_download_schedule`을 `0 */6 * * *`로 변경

  [DONE] INGEST MinIO 업로드 임시 스킵 모드 (2026-03-04)
    - 조치:
      - `src/vlm_pipeline/defs/ingest/ops.py`에서 MinIO 업로드 호출을 주석 처리
      - 실행 로그를 `MinIO 업로드 스킵(비활성화)`로 명시

  [DONE] auto_bootstrap 대량 unit 분할 + incoming enqueue 개선 (2026-03-03)
    - 이슈:
      - source unit 파일이 매우 많을 때 단일 manifest 생성으로 pending/처리 지연 발생
      - 같은 source unit 기준 중복 방어가 chunk 단위 처리와 충돌해 enqueue가 지연됨
    - 조치:
      - `AUTO_BOOTSTRAP_MAX_FILES_PER_MANIFEST` 도입(기본 100)
      - manifest 메타 확장:
        - `source_unit_dispatch_key`
        - `source_unit_total_file_count`
        - `source_unit_chunk_index`
        - `source_unit_chunk_count`
      - `incoming_manifest_sensor`가 `source_unit_dispatch_key` 기준으로 run_key/중복 판단
      - `INCOMING_SENSOR_MAX_IN_FLIGHT_RUNS` 기반 backpressure 가드 추가
      - pending 중복 manifest는 최신 1개만 유지하고 superseded를 processed로 이동
    - 검증:
      - 대량 source unit에서 chunk manifest 생성 로그 확인
      - backpressure 로그(`in_flight`, `limit`) 노출 확인

  [DONE] chunked manifest 아카이브 + 레거시 경로 정리 (2026-03-03)
    - 조치:
      - `src/vlm_pipeline/defs/ingest/assets.py`에서
        chunked manifest는 폴더 전체 이동 대신 unit 경로로 누적 파일 이동
      - `update_minio_uploader/`, `update_python_scanner/` 제거
      - `src/vlm_pipeline/defs/sync/assets.py`에서 legacy sync fallback 제거

  [DONE] MinIO raw_key 폴더명 영문화 적용 (2026-03-03)
    - 이슈:
      - 파일명은 정규화되지만 폴더명이 한글인 경우 raw_key에 한글이 남는 문제
    - 조치:
      - `src/vlm_pipeline/lib/sanitizer.py`에 `sanitize_path_component()` 추가
      - `src/vlm_pipeline/defs/ingest/ops.py`에서
        `source_unit_name` + `rel_path` 디렉터리 세그먼트 정규화 반영
    - 결과:
      - MinIO 업로드 키에서 파일명/폴더명 모두 ASCII-safe 경로로 통일

  [DONE] duplicate 안내 메시지 범위 수정 (2026-02-25)
    - 이슈:
      - `duplicate_skipped_in_manifest:*`가 동일 manifest의 성공 파일 전체에 기록되던 문제
    - 조치:
      - `docker/app/dagster_defs.py`에서 업데이트 대상을 `successful_assets` 전량이 아니라
        `duplicate_skips[].duplicate_asset_id` 집합으로 변경
      - empty/NULL `error_message`일 때만 기록하는 기존 안전장치 유지
    - 검증:
      - `python3 -m py_compile docker/app/dagster_defs.py` 통과

  [PENDING] 기존 DB 오염값 정리 (2026-02-25)
    - 목표:
      - 과거 실행으로 누적된 `duplicate_skipped_in_manifest:*` 메시지 정리
    - 상태:
      - 실행 중 DuckDB write lock(`pipeline-dagster-1`, python PID 446290)으로 즉시 적용 보류
    - 예정 SQL:
      - `UPDATE raw_files SET error_message = NULL, updated_at = CURRENT_TIMESTAMP
         WHERE error_message LIKE 'duplicate_skipped_in_manifest:%';`

  [DONE] Incoming/Archive + DB 보존 정책 업데이트 (2026-02-23)
    - 정책 변경:
      - 중복으로 `skipped`가 발생해도 `failed=0`이면 성공 파일은 부분 archive 이동
      - archive로 이동되지 않은 파일은 `raw_files/image_metadata/video_metadata`에서 제거
      - archive로 실제 이동된 파일만 `ingest_status=completed` 유지
    - 운영 정리:
      - 중복 잔류 레코드(duplicate_of) 제거 후 `raw_files` 완료건만 유지
      - `_bak_` 테이블(`_bak_raw_files`, `_bak_labels`, `_bak_processed_clips`) 삭제
    - MotherDuck 정렬:
      - 기본 DB를 `pipeline_db`로 전환
      - `pipeline_db` 동기화 완료, 구 DB 삭제 완료

  [DONE] Video 환경 메타 + CUDA 실사용 적용 (2026-02-20 오전)
    - video_metadata 확장 컬럼 추가:
      - environment_type, daynight_type, outdoor_score, avg_brightness, env_method
    - `video_env.py` 추가:
      - Places365 CUDA 우선 추론 + heuristic fallback
    - 컨테이너:
      - torch/torchvision cu124 설치, app/dagster `gpus: all` 적용
      - app/dagster 모두 `torch.cuda.is_available() == True` 확인
    - 모델 캐시:
      - `VIDEO_ENV_MODEL_DIR=/data/models/places365`
      - `VIDEO_ENV_AUTO_DOWNLOAD=false`
      - 컨테이너 재생성 후 모델 재다운로드 없이 재사용 확인
    - 테스트:
      - `pytest tests/unit` 46 passed
      - `pytest tests/integration/test_ingest_pipeline.py` 3 passed

  [DONE] Incoming 안정화 게이트 + 폴더 archive 이동 (2026-02-19)
    - auto_bootstrap manifest 생성 조건:
      - `AUTO_BOOTSTRAP_STABLE_CYCLES` + `AUTO_BOOTSTRAP_STABLE_AGE_SEC`
      - source unit 시그니처 안정화 확인 후 생성
    - manifest 확장:
      - `source_unit_type`, `source_unit_path`, `source_unit_name`, `stable_signature`
    - ingest 확장:
      - `source_unit_type=directory`에서 전 파일 성공 시 폴더째 archive 이동
      - archive 충돌 시 suffix(`__N`)로 자동 회피
      - 이동 후 `raw_files.archive_path` 파일별 일괄 갱신

  [DONE] Phase A Dual Path 정렬
    - compose/.env 기준 루트: /nas/datasets/projects
    - 병행 루트: /nas/incoming

  [DONE] Phase B Strict 8 전환
    - migration 적용: 8테이블 운영 (image_metadata, video_metadata 추가)
    - clip_metadata 운영 테이블 제거 → processed_clips에 흡수

  [DONE] Phase C 코드 정렬
    - dagster_defs.py 다중 루트 fallback 보강
    - verify_mvp.sh를 Strict 8 + Dual Path 기준으로 갱신

  [DONE] Phase D 검증
    - INGEST: raw_files=179, video_metadata=179
    - DEDUP: synthetic 이미지 3건 pHash 계산 및 dup_group 갱신
    - LABEL: raw_key stem 매칭 labels.asset_id 연결 (stem_matched=1)
    - PROCESS/BUILD: processed_clips, datasets, dataset_clips 생성
    - E2E: scripts/verify_mvp.sh 성공

  [DONE] 리팩토링 (2026-02-13)
    - data_pipeline_job 제거 (file_organize, nas_scan, minio_upload)
    - mvp_stage_job으로 통합
    - incoming_manifest_sensor/auto_bootstrap_manifest_sensor → mvp_stage_job 트리거
    - motherduck_sync asset/job 재연결
    - cursor 기반 중복 방지 추가
    - src/vlm_pipeline/ 전체 리팩토링 (definitions, assets, sensor 정합)

  [DONE] MotherDuck sync 장애 복구 (2026-02-13)
    - 원인: `pipeline__motherduck_sync` step 정체(run hang)로 `duckdb_writer=true` 큐 블로킹
    - 조치: sensor run_config에 sync op 설정 주입 + `timeout_sec`(기본 600초) 추가
    - 조치: timeout 발생 시 강제 fail 처리(`TimeoutExpired` 핸들링)로 무한 대기 방지
    - 운영 복구: stale STARTED run 정리 후 queued run 정상 완료

  [DONE] 코드베이스 슬림화/정리 (2026-02-13)
    - 요청 기준 적용: `gcp/`, `split_dataset/`, `*.md` 제외하고 불필요 파일 정리
    - `scripts/setup.sh`를 pyproject/editable install 기준으로 갱신
    - 전역 캐시/메타파일 정리: `__pycache__`, `.pytest_cache`, `.DS_Store`, `._.DS_Store`, `*.pyc`
    - `.gitignore`에 런타임 산출물 경로 추가:
      - `docker/airflow/logs/`
      - `docker/app/dagster_home/storage/`
      - `docker/app/dagster_home/history/`
    - 검증:
      - `pytest -q tests/unit` → 43 passed
      - `pytest -q tests/integration` → 6 passed

  [DONE] MotherDuck 컬럼 정합 복구 (2026-02-13)
    - 이슈: `raw_files.phash`, `raw_files.dup_group_id`, `raw_files.raw_bucket` 누락
    - 원인:
      - DEDUP가 `source_path`만 참조해 archive 이동 후 pHash 계산 실패
      - 과거 ingest 데이터의 `raw_bucket` NULL 잔존
    - 조치:
      - DEDUP에 `archive_path` 우선 + MinIO fallback 추가
      - INGEST insert/update에서 `raw_bucket` 보강
      - `compute_phash` bytes 입력 지원 추가
      - 기존 데이터 백필 + `dedup_job`/`motherduck_sync_job` 재실행
    - 결과:
      - Local: `phash_filled=12/12`, `dup_group_filled=8/12`, `raw_bucket_null=0/12`
      - MotherDuck: `phash_filled=12/12`, `dup_group_filled=8/12`, `raw_bucket_null=0/12`

  [DONE] clip_metadata 제거 (2026-02-13)
    - 요청: 불필요한 clip 계열 legacy 테이블 삭제
    - 조치:
      - sync 스크립트에 legacy drop 단계 추가
      - `LEGACY_TABLES_TO_DROP = ['clip_metadata']`
      - `motherduck_sync_job` 재실행으로 MotherDuck에서 `clip_metadata` 제거
    - 결과:
      - Local/MotherDuck 모두 `clip_metadata` 미존재 확인


┌─────────────────────────────────────────────────────────────────────────────┐
│                         GCP 다운로드 운영 (migrated)                        │
└─────────────────────────────────────────────────────────────────────────────┘

  개요:
    - 버킷: `khon-kaen-rtsp-bucket`
    - 단일 실행 엔트리:
      - `gcp/download_from_gcs_rclone.py`
      - `gcp/download_from_gcs.sh` (Python 엔진 래퍼)
    - 지원 모드:
      - `date-folders`: top-level 날짜 폴더(YYYYMMDD / YYYY-MM-DD) 단위 다운로드
      - `legacy-range`: 기존 folder/pattern 기반 다운로드
    - 백엔드:
      - `auto|rclone|gcloud`

  Dagster 연계:
    - asset: `pipeline/gcs_download_to_incoming`
    - job: `gcs_download_job`
    - schedule: `gcs_download_schedule` (`0 */6 * * *`, `Asia/Seoul`)
    - 기본 다운로드 경로: `/nas/incoming/gcp`
    - 실패 정책: `auto`에서 `rclone/gcloud` 모두 미설치면 즉시 실패
    - 0바이트 복구 옵션:
      - run config: `zero_byte_retries`
      - env: `GCS_ZERO_BYTE_RETRIES` (기본 2)
      - 동작: 폴더 다운로드 완료 후 0바이트 미디어 발견 시 파일 삭제 후 폴더 재다운로드
        (재시도 초과 시 해당 폴더 실패 처리, `_DONE` 미생성)

  실행 예시:
    - date-folder 모드
      - `python3 /gcp/download_from_gcs_rclone.py --mode date-folders --download-dir /nas/incoming/gcp`
    - legacy 모드
      - `python3 /gcp/download_from_gcs_rclone.py --mode legacy-range --download-dir /nas/datasets/projects/khon_kaen_data`

  인증/권한 체크:
    - `gcloud auth list`
    - `gsutil ls gs://khon-kaen-rtsp-bucket/`
    - 필요 권한: `storage.objects.list`, `storage.objects.get`


┌─────────────────────────────────────────────────────────────────────────────┐
│                운영 트러블슈팅 런북 (2026-03-16 반영)                      │
└─────────────────────────────────────────────────────────────────────────────┘

  1) 증상: 생산 파이프라인 정체 현상 (Dagster 서버 복구)
    - 대표 현상:
      - 포트 충돌 및 정체된 프로세스로 인해 Dagster UI 접속 불가.
      - `LOCATION_ERROR` 반복.
    - 조치:
      - 포트 충돌 및 정체된 프로세스를 정리하여 UI 접속 정상화.
      - http://172.168.42.6:3030/ 주소로 접근 가능.
    - 재발 시 확인:
      - `docker logs pipeline-dagster-1 | tail -n 100`으로 에러 확인.
      - `docker compose restart dagster`로 서비스 초기화.
      - `ss -tln | grep 3030`으로 포트 리스닝 상태 확인.

  2) 증상: 센서 작동 지연 (데이터 감지 센서 스캔 한도 부족)
    - 대표 현상:
      - 폴더가 많아지면 데이터 감지 센서가 지연.
      - `_DONE` 마커 파일 확인 과정 미비.
    - 조치:
      - 데이터 감지 센서의 스캔 한도를 `3배(30->100)` 상향.
      - `_DONE` 마커 파일 확인 과정을 통해 데이터 복사 완료 여부를 정확히 판단하도록 안정성 강화.
    - 재발 시 확인:
      - `.env` 파일의 `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK` 값 확인.
      - 해당 날짜 폴더 안에 `_DONE` 파일이 있는지 확인.

  3) 증상: Config 스키마 불일치 (DagsterInvalidConfigError)
    - 대표 현상:
      - 코드 레벨에서 Config 스키마 불일치 문제 발생.
      - 시스템 부하 증가 및 로그 투명성 저하.
    - 조치:
      - 지원하지 않는 필드(`jpeg_quality`, `max_frames_per_video`, `overwrite_existing`) 제거.
    - 재발 시 확인:
      - 에러 로그에 표시된 'unexpected field'를 확인.
      - `src/vlm_pipeline/defs/.../sensor.py`의 `run_config` 부분 수정.

  향후 관리 포인트:
    - 마커 파일 관리: 외부에서 데이터를 복사할 때 반드시 해당 폴더 내에 `_DONE` 파일이 생성되도록 프로세스 점검.
    - 모니터링: 실행 중인 `ingest_job` 로그에서 데이터가 DuckDB에 정상 적재되는지 주기적으로 확인.
    - 서버 부하: 스캔 한도를 높였으므로 인입 데이터가 매우 많을 때(천 단위 이상)는 센서 실행 시간을 조정할 필요가 있음.


┌─────────────────────────────────────────────────────────────────────────────┐
│                운영 트러블슈팅 런북 (2026-03-12 반영)                      │
└─────────────────────────────────────────────────────────────────────────────┘

  1) 증상: staging MinIO Console에서 다운로드가 10~11% 근처에서 끊기며 `Unexpected response, download incomplete`가 발생
    - 대표 사례:
      - `vlm-labels/tmp_data/fall_down/detections/20250711_am_pk_fc_00000000_00014000_00000002.json`
      - Console UI에서 다운로드 시 실패.
    - 실제 원인:
      - 객체 자체가 손상된 것이 아니라,
        `Console(9003) -> API(9002)` 경로 또는 브라우저 세션/응답 처리 문제일 가능성이 높음.
      - staging API로 직접 `head/get` 해 보면 객체는 정상적으로 존재하고 다운로드도 가능했음.
    - 진단 순서:
      - A. 먼저 대상이 `prod`가 아니라 `staging` endpoint에 있는지 확인
      - B. `boto3.head_object()` 또는 `get_object()`로 직접 확인
      - C. presigned URL 또는 `download_file()`로 실제 다운로드 가능 여부를 확인
      - D. 이 단계가 통과하면 객체 손상으로 보지 말고 Console/브라우저 문제로 분리
    - 운영 기준:
      - Console 다운로드 실패만으로 객체를 재생성하거나 삭제하지 말 것
      - `9002 = staging API`, `9003 = staging Console` 역할을 구분해서 봐야 함
    - 확인 포인트:
      - API 직접 다운로드가 되면 객체는 정상
      - 문제는 Console 계층에 있다고 보고 우회 다운로드를 먼저 제공

  2) 증상: staging을 재테스트하려는데 이전 실행 결과 때문에 새 테스트가 섞여 보임
    - 대표 현상:
      - MinIO에 예전 raw/labels/processed 객체가 남아 있음
      - `staging.duckdb`에 예전 row가 남아 있음
      - sensor cursor, run history, run_key 기록 때문에 새 incoming이 예상대로 안 잡힐 수 있음
    - 실제 원인:
      - staging은 production과 달리 "실험 후 완전 초기화"가 자주 필요한 환경인데,
        MinIO/DB/Dagster runtime state를 한 번에 비우는 절차가 명확하지 않으면
        일부 상태만 남아서 재현이 어려워짐.
    - 권장 초기화 순서:
      - A. `dagster-staging` 중지
      - B. staging MinIO endpoint(`172.168.47.36:9002`)에서
        - `vlm-raw`
        - `vlm-labels`
        - `vlm-processed`
        - `vlm-dataset`
        객체 전부 삭제
      - C. `docker/data/staging.duckdb` 삭제
      - D. `docker/data/dagster_home_staging/storage` 전체 삭제
      - E. 필요 시 `dagster-staging` 재기동
    - 절대 같이 지우면 안 되는 것:
      - `/home/pia/mou/staging/incoming`
      - `/home/pia/mou/staging/archive`
      입력 원본 폴더는 명시 요청이 없는 한 유지
    - 확인 포인트:
      - staging MinIO 객체 수 `0`
      - `staging.duckdb` 없음
      - Dagster staging runtime DB 없음

  3) 증상: `clip_to_frame`에서 `ffmpeg_clip_extract_failed: File '/tmp/tmp....mp4' already exists. Overwrite? [y/N]`가 반복됨
    - 대표 현상:
      - 같은 source asset에 대해 여러 event clip 생성 시
        `/tmp/tmp....mp4 already exists` 오류가 연속 발생.
      - 로그만 보면 timestamp나 원본 영상 문제처럼 보일 수 있음.
    - 실제 원인:
      - clip 출력 temp 경로를 `NamedTemporaryFile(delete=False)`로 먼저 만들고 있었음.
      - ffmpeg는 이미 존재하는 출력 파일을 받으면 overwrite 여부를 묻는데,
        비대화식 실행이라 기본 `N` 처리되어 종료.
      - 즉 root cause는 `출력 temp 파일 선생성` 구현.
    - 영구 조치:
      - ffmpeg 출력에는 "아직 존재하지 않는 temp 경로 문자열"만 생성해서 전달.
      - partial temp file이 남으면 실패 시 cleanup.
    - 진단 순서:
      - A. stderr에 `Overwrite? [y/N]` 문구가 있는지 확인
      - B. ffmpeg input/timestamp보다 output temp 생성 방식을 먼저 확인
      - C. `-y` 추가 여부 또는 non-existent temp path 방식 적용 여부를 확인
    - 운영 기준:
      - 대량 clip 생성 코드에서는 output temp path 선생성을 피하는 것이 안전
      - 재발 시 asset_id별 이벤트가 여러 개인 경우 동일 asset 로그가 반복될 수 있으므로
        label row 수와 clip 시도 횟수도 함께 봐야 함

  4) 증상: `clip_timestamp`에서 `400 Request payload size exceeds the limit: 524288000 bytes` 오류가 발생
    - 대표 로그:
      - `AUTO LABEL 실패: asset_id=...: 400 Request payload size exceeds the limit: 524288000 bytes.`
    - 실제 원인:
      - Gemini 호출 시 큰 원본 영상을 그대로 메모리로 읽어 request payload에 실었음.
      - 500MB를 넘는 비디오는 Vertex AI request size 제한에 직접 걸림.
    - 영구 조치:
      - `>450MB` 영상은 Gemini 호출 전에 preview mp4를 먼저 생성:
        - 오디오 제거
        - 해상도 축소
        - fps/bitrate 축소
      - 첫 preview가 여전히 크면 더 낮은 설정으로 재시도
    - 운영 해석:
      - preview 경로는 "오류 회피"뿐 아니라 "입력 토큰/오디오 비용 구조"에도 영향을 줌
      - 따라서 비용 산정 시
        - raw source 기준
        - current pipeline preview 기준
        두 시나리오를 분리해서 봐야 함
    - 확인 포인트:
      - 대형 파일에서 더 이상 524288000 bytes 오류가 바로 재현되지 않아야 함
      - 로그에 preview 사용 경로가 찍히는지 확인

  5) 운영 참고: staging incoming 배치의 Gemini/clip/YOLO 비용과 시간을 다시 계산해야 할 때
    - 기준 문서:
      - `STAGING_INCOMING_LABELING_REPORT.md`
    - 현재 문서 범위:
      - 대상: `/home/pia/mou/staging/incoming` 당시 적재 영상 38개
      - Gemini 2.5 Pro 비용/시간
      - Gemini 2.5 Flash 비용/시간
      - USD/KRW 환산
      - clip 이미지 추출 설정 및 실측 시간
      - YOLO-World 실측 시간
    - 운영 기준:
      - 현재 pipeline 기본 모델은 `gemini-2.5-flash`
      - 보고서에는 비교용으로 `gemini-2.5-pro`도 함께 정리
      - 환율은 문서 생성일 기준 값이므로 다음 배치 계산 때는 환율만 갱신하면 됨
    - 재작성 순서:
      - A. staging incoming 파일 수/총 길이/총 크기 재집계
      - B. audio 포함 수, 450MB/500MB/1GB 초과 수 재집계
      - C. 공식 Gemini 단가와 현재 runtime 설정(YOLO device, batch, imgsz) 재확인
      - D. 문서의 산식 Appendix 기준으로 비용 재계산

  6) 운영 원칙: 이벤트 JSON은 `vlm-labels`만 source of truth로 유지하고 `vlm-processed`에는 중복 저장하지 않음
    - 배경:
      - `clip_to_frame`가 `vlm-labels`에 이미 존재하는 event JSON을 다시 `vlm-processed/.../events/*.json`로 복제하면
        데이터 원본이 둘이 되어 경로 정책과 정합성 관리가 복잡해짐.
    - 현재 정책:
      - Gemini event JSON:
        - `vlm-labels/<raw_parent>/events/<video_stem>.json`
      - YOLO detection JSON:
        - `vlm-labels/<raw_parent>/detections/<image_or_clip_stem>.json`
    - 영구 조치:
      - `clip_to_frame`는 event JSON을 `vlm-processed`에 복사하지 않음
      - build/dataset 단계에서 필요하면 라벨 원본은 `vlm-labels`에서 읽음
    - 운영 의미:
      - 사람이 버킷을 볼 때도 용도별 경로가 명확하고,
        downstream이 라벨 원본을 찾을 때 source-of-truth가 한 군데로 고정됨

┌─────────────────────────────────────────────────────────────────────────────┐
│                운영 트러블슈팅 런북 (2026-03-11 반영)                      │
└─────────────────────────────────────────────────────────────────────────────┘

  1) 증상: `dagster-staging` 로그에 `Another SENSOR daemon is still sending heartbeats`가 반복됨
    - 대표 로그:
      - `Another SENSOR daemon is still sending heartbeats`
      - `Another ASSET daemon is still sending heartbeats`
      - `Another QUEUED_RUN_COORDINATOR daemon is still sending heartbeats`
    - 실제 원인:
      - staging이 운영 `dagster`와 같은 인스턴스 상태를 공유.
      - `DAGSTER_HOME`만 분리된 것이 아니라, run/event/schedule/local storage도 분리되어야 함.
      - storage가 공유되면 daemon heartbeat가 서로 다른 프로세스를 "중복 daemon"으로 인식함.
    - 영구 조치:
      - `dagster-staging`은 반드시 `DAGSTER_HOME=/app/dagster_home_staging` 사용.
      - staging 전용 `dagster.yaml`을 두고,
        - `run_storage`
        - `event_log_storage`
        - `schedule_storage`
        - `local_artifact_storage`
        모두 `/data/dagster_home_staging/storage`로 분리.
      - staging DuckDB도 `/data/staging.duckdb`로 분리.
    - 확인 포인트:
      - 재기동 후 heartbeat 충돌 로그가 더 이상 나오지 않아야 함.
      - staging 인스턴스에서 sensor tick이 정상 순환해야 함.

  2) 증상: staging sensor가 `DuckDB not found: /data/staging.duckdb`로 계속 skip됨
    - 실제 원인:
      - staging 서비스는 `/data/staging.duckdb`를 기준으로 뜨는데, 파일이 없으면 sensor가 전부 skip.
      - 운영 DB와 staging DB를 분리한 뒤 초기 파일을 준비하지 않으면 바로 이 상태가 됨.
    - 운영 복구 방법:
      - 운영 상태를 staging에서 재현하고 싶으면:
        - `pipeline.duckdb -> staging.duckdb` 복제
      - 빈 staging으로 새로 테스트하고 싶으면:
        - 기존 `staging.duckdb` 삭제
        - 스키마만 다시 적용한 새 DB 생성
    - 이번 작업에서 정리한 기준:
      - 사용 중에는 운영 복제본으로 띄울 수 있음.
      - 검증용 초기화가 필요하면 데이터는 모두 지우고 스키마만 남긴 빈 DB로 재생성.
      - 백업 파일은 필요 없으면 즉시 삭제.
    - 확인 포인트:
      - `raw_files`, `video_metadata` 등 기본 테이블 조회가 가능해야 함.
      - `DuckDB not found` 로그가 더 이상 없어야 함.

  3) 증상: 호스트의 `/home/pia/mou/staging/incoming`에는 파일이 있는데 staging 컨테이너가 파일을 못 봄
    - 대표 현상:
      - 컨테이너 안 `/nas/staging/incoming`에 `.manifests`만 있고 실제 비디오는 없음.
      - `incoming_manifest_sensor`가 계속 `pending 없음`처럼 보일 수 있음.
    - 실제 원인:
      - 기본 공통 볼륨은 `/home/pia/mou/incoming -> /nas/incoming`만 연결.
      - staging 전용 경로 `/home/pia/mou/staging/incoming`, `/home/pia/mou/staging/archive`가 별도 마운트되지 않음.
    - 영구 조치:
      - `dagster-staging` 서비스에 아래 bind mount를 반드시 추가:
        - `/home/pia/mou/staging/incoming -> /nas/staging/incoming`
        - `/home/pia/mou/staging/archive -> /nas/staging/archive`
    - 진단 순서:
      - A. 컨테이너 안에서 `/nas/staging/incoming/tmp_data/...` 파일이 실제 보이는지 확인
      - B. `.manifests/pending`, `.manifests/processed`가 생성되는지 확인
      - C. `auto_bootstrap_manifest_sensor`, `incoming_manifest_sensor` 로그 확인
    - 운영 메모:
      - `복사 안정화 대기 중` 로그는 고장이 아니라 안정화 조건 대기일 수 있음.
      - `incoming_manifest_sensor skipped: backpressure ... in_flight=2`는 run 제한 때문에 다음 manifest가 대기 중인 상태임.

  4) 증상: staging `auto_labeling_job`의 `clip_timestamp`가 `Gemini credentials not found`로 실패
    - 대표 로그:
      - `FileNotFoundError: Gemini credentials not found. Set GEMINI_GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS, or GEMINI_SERVICE_ACCOUNT_JSON.`
    - 실제 원인:
      - credential JSON 파일은 컨테이너에 존재하지만, staging env가 그 경로를 가리키지 않음.
      - 운영 `.env`만 맞아 있고 `.env.staging`에 동일 설정이 빠져 있는 경우가 대표적.
    - 영구 조치:
      - staging env에 최소 아래 값을 유지:
        - `GEMINI_PROJECT`
        - `GEMINI_LOCATION`
        - `GEMINI_GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/<service-account>.json`
      - `GEMINI_SERVICE_ACCOUNT_JSON`를 쓰지 않는다면 빈 값으로 두어 우선순위 충돌을 줄임.
    - 진단 순서:
      - A. 컨테이너 안에서 credential 파일 존재 확인
      - B. `resolve_gemini_credentials_path()`가 실제 파일 경로를 반환하는지 확인
      - C. 그 뒤 `auto_labeling_job`을 다시 큐에 올려 재시도
    - 확인 포인트:
      - 이후에는 `clip_timestamp`가 credential error 없이 넘어가야 함.

  5) 운영 원칙: production Dagster에서는 수동 import/실험성 자산을 숨기고 staging에만 노출
    - 대상:
      - `manual_label_import`
      - `yolo_image_detection`
      - `yolo_detection_sensor`
    - 구현 기준:
      - `src/vlm_pipeline/definitions.py`에서 env flag로 분기
      - 사용 env:
        - `ENABLE_MANUAL_LABEL_IMPORT`
        - `ENABLE_YOLO_DETECTION`
    - 운영 기본값:
      - production: 둘 다 `false`
      - staging: 둘 다 `true`
    - 확인 포인트:
      - production UI/Definitions에서 해당 asset/job/sensor가 보이지 않아야 함.
      - staging에서는 계속 노출되어야 함.

  6) 증상: YOLO 서비스가 떠도 모델 파일이나 dependency 때문에 실제 detection 준비가 안 됨
    - 현재 서버 스택 기준:
      - 현재 구현은 Ultralytics YOLO-World 서버.
      - MMYOLO `.pth` 체크포인트와는 바로 호환되지 않음.
    - 이번에 확정한 운영 기준:
      - 사용 모델: `yolov8l-worldv2.pt`
      - 위치: `docker/data/models/yolo/yolov8l-worldv2.pt`
      - env:
        - `YOLO_MODEL_PATH=/data/models/yolo/yolov8l-worldv2.pt`
        - `YOLO_DEFAULT_CLASSES=...`
    - 추가 함정:
      - YOLO-World 구동 시 `clip` dependency가 빠지면 컨테이너가 부팅 단계에서 실패할 수 있음.
      - 따라서 YOLO 이미지에는 `git`, `git+https://github.com/ultralytics/CLIP.git`가 필요함.
    - 확인 포인트:
      - `pipeline-yolo-1`이 `healthy`
      - `/health`에서 `model_loaded=true`
      - `/info` 또는 로그에서 모델 경로가 기대값과 일치

  7) 증상: staging을 한 번 돌린 뒤 `git switch`가 막힘
    - 대표 현상:
      - `runs.db`, `schedules.db` local change 때문에 checkout 차단
      - `.nux/nux.yaml`, `.telemetry/id.yaml` 같은 untracked runtime 파일 때문에 checkout 차단
    - 실제 원인:
      - staging runtime 파일이 git working tree 아래에 생성됨.
    - 영구 조치:
      - staging Dagster storage는 repo 내부가 아니라 `/data/dagster_home_staging/storage`로 유지.
      - staging 컨테이너를 켠 채로 브랜치를 바꾸지 말고, 전환 직전에는 staging 상태와 runtime 파일을 확인.
    - 운영 메모:
      - storage 분리 후에도 `.nux`, `.telemetry` 같은 비핵심 파일은 다시 생길 수 있음.
      - 브랜치 전환 직전 `git status`를 보고, 필요하면 staging 컨테이너를 먼저 중지하는 것이 안전함.

┌─────────────────────────────────────────────────────────────────────────────┐
│                운영 트러블슈팅 런북 (2026-03-10 반영)                      │
└─────────────────────────────────────────────────────────────────────────────┘

  1) 증상: Gemini 코드는 있는데 컨테이너 안에서 `vertexai` / `google.cloud.aiplatform` import가 안 됨
    - 대표 현상:
      - `ModuleNotFoundError: No module named 'google.cloud'`
      - `ModuleNotFoundError: No module named 'vertexai'`
    - 실제 원인:
      - 기존 Gemini 코드가 `src/gemini` 아래 별도 유틸처럼 존재하고,
        파이프라인 공용 라이브러리와 Docker dependency에 연결되어 있지 않았음.
    - 영구 조치:
      - 공용 모듈:
        - `src/vlm_pipeline/lib/gemini.py`
        - `src/vlm_pipeline/lib/gemini_prompts.py`
      - 호환 wrapper:
        - `src/gemini/video.py`
        - `src/gemini/gemini/gemini_api.py`
        - `src/gemini/assets/config.py`
      - dependency 반영:
        - `docker/app/requirements.txt`
        - `pyproject.toml`
        - `google-cloud-aiplatform` 추가
      - Docker 이미지 재빌드 후 `app`, `dagster` 재기동
    - 추가 함정:
      - 초기 wrapper에서 `VIDEO_PROMPT`, `IMAGE_PROMPT` export를 빼먹으면
        예전 import 경로가 깨질 수 있음.
      - wrapper는 최소한 아래 이름들을 그대로 export해야 함:
        - `ENV_AUTH`
        - `PROMPT`
        - `PROMPT_VIDEO`
        - `IMAGE_PROMPT`
        - `VIDEO_PROMPT`
    - 검증:
      - `docker exec pipeline-app-1 python3 -c "import vertexai"`
      - `docker exec pipeline-dagster-1 python3 -c "from gemini.assets.config import VIDEO_PROMPT"`
      - 두 경로가 모두 성공해야 함.

  2) 증상: 비디오 프레임 추출 rollout 후 `vlm-processed/_tmp/...` 또는 `/frames/` 같은 중간 경로가 남음
    - 이번에 최종 확정한 저장 규칙:
      - 버킷: `vlm-processed`
      - key:
        - `vlm-processed/<raw-prefix>/<video-stem>/<video-stem>_00000001.jpg`
      - 예:
        - `khon-kaen-rtsp-bucket/20251128/fire_235_156/fire_235_156_00000001.jpg`
    - 금지 규칙:
      - `_tmp/...` prefix 사용 금지
      - `/frames/` 하위 폴더 사용 금지
      - `frame_0001...jpg`처럼 원본 stem이 없는 파일명 사용 금지
    - 구현 원칙:
      - `video_metadata.duration_sec`, `fps`, `frame_count`로 frame count/시점 계획
      - 실제 decode는 ffmpeg 사용
      - 결과는 `image_metadata`에 row 저장
    - 경로 규칙을 바꿨다면 같이 해야 하는 정리:
      - A. 진행 중 extraction run cancel
      - B. `vlm-processed` 버킷 정리
      - C. `image_metadata`의 `video_frame` row 삭제
      - D. `video_metadata.frame_extract_status/count/error/extracted_at` 초기화
      - E. extraction 재실행
    - 운영 메모:
      - `image_metadata.codec`는 JPEG frame 기준 불필요해서 제거함.
      - frame 추출 메타의 핵심은 `image_key`, `frame_index`, `frame_sec`, `checksum`, `file_size`.

  3) 증상: `auto_bootstrap_manifest_sensor`가 180초 timeout으로 실패
    - 대표 에러:
      - `DagsterUserCodeUnreachableError`
      - `Deadline Exceeded`
    - 실제 원인:
      - 한 tick에서 너무 많은 source unit 스캔
      - **NAS/NFS 지연**으로 discovery·스캔 I/O가 180초를 초과
      - hidden entry까지 discovery 포함
      - cursor 이동과 scan budget이 실제 처리량 기준으로 보수적으로 관리되지 않음
    - 이번 조치:
      - hidden 항목 제외 (`.Trash-1000`, `.DS_Store` 등)
      - tick당 처리 unit에 safety cap `10`
      - **Discovery를 틱당 상한으로 분할**: `AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES`(0=무제한, NAS 지연 시 15~30 권장)
      - Discovery 예산 60초 상한으로 gRPC 여유 확보
      - `discovery_elapsed`, `scan_elapsed`, `processed_units`, `budget` 로그 추가
    - **NAS 지연 시 권장 설정**:
      - `AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES=20` (또는 15~30)
      - `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK=3`
      - `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS=300` (Dagster daemon/user code 서버 설정)
    - 운영 복구 기준:
      - 최신 sensor tick이 gRPC 타임아웃 이내에 끝나야 함
      - daemon 로그에 `Deadline Exceeded`가 재발하지 않아야 함
    - 재발 시 표준 확인 순서:
      - A. hidden entry가 discovery에 섞였는지 확인
      - B. max units per tick, discovery max top entries 값 확인
      - C. 최근 tick 로그에서 processed_units/budget 확인
      - D. NAS 지연이면 위 권장 설정 적용 후 `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS` 상향

  4) 증상: `image_metadata` 전체 삭제, `vlm-processed` 전체 비움이 필요할 때 무엇을 같이 봐야 하는가
    - 이번 작업에서 실제 수행:
      - Dagster 작업 정지
      - `image_metadata` 전체 DELETE
      - MinIO `vlm-processed` 전체 비움
    - 주의점:
      - `vlm-processed`를 비우면 추출 프레임뿐 아니라 processed clip 객체도 같이 없어질 수 있음.
      - `image_metadata`를 비우면 기존 frame/image 메타가 전부 사라짐.
      - 그런데 `video_metadata.frame_extract_status='completed'`가 남아 있으면 재추출이 막힐 수 있음.
    - 따라서 "완전 초기화 후 다시 추출"이 목적이면 아래를 같이 고려:
      - A. `image_metadata` 삭제
      - B. `vlm-processed` 버킷 정리
      - C. `video_metadata.frame_extract_*` 초기화
      - D. extraction run 재실행
    - 이번 운영에서는 사용자 요청 범위만 먼저 수행하고,
      이후 경로 변경/파일명 변경 작업에서는 `video_metadata.frame_extract_*`도 함께 리셋함.

  5) Dagster 운영 구조 메모(2026-03-10 기준)
    - 자동 흐름:
      - `incoming/autobootstrap -> ingest_job`
      - `dedup_backlog_sensor -> dedup_job`
      - `video_frame_extract_sensor -> video_frame_extract_job`
    - lineage 원칙:
      - `processed_clips` 직접 upstream은 `labeled_files`
      - `motherduck_sync`는 실제 동기화 대상 asset 전체를 upstream으로 연결
    - future code:
      - `processed_clips` 기준 프레임 추출 sensor/job/asset은 코드에 주석 블록으로만 존재
      - `archive_path_prefixes` 기반 수동 추출도 주석 설계만 추가되어 있으며 현재 비활성


┌─────────────────────────────────────────────────────────────────────────────┐
│                운영 트러블슈팅 런북 (2026-03-05 반영)                      │
└─────────────────────────────────────────────────────────────────────────────┘

  1) 증상: `ingested_raw_files`에서 `raw_files` 테이블 미존재
    - 대표 로그:
      - `Catalog Error: Table with name raw_files does not exist`
      - `SELECT * FROM raw_files ...`
    - 핵심 원인:
      - ingest 시작 시점에 DuckDB 스키마 초기화가 보장되지 않음.
      - DB 파일만 있고 테이블이 없는 상태에서 즉시 조회가 발생.
    - 영구 조치(코드):
      - `src/vlm_pipeline/resources/duckdb.py`
        - `DuckDBResource.ensure_schema()` 추가
      - `src/vlm_pipeline/defs/ingest/assets.py`
        - `ingested_raw_files()` 시작 직후 `db.ensure_schema()` 호출
    - 운영 즉시 복구:
      - 필요 시 수동 스키마 적용:
        - `python3 - <<'PY'`
        - `import duckdb; from pathlib import Path`
        - `ddl = Path('/src/vlm/vlm_pipeline/sql/schema.sql').read_text(encoding='utf-8')`
        - `conn = duckdb.connect('/data/pipeline.duckdb'); conn.execute(ddl); conn.close()`
        - `PY`

  2) 증상: `QUEUED` 누적 + 장시간 `STARTED` 점유
    - 핵심 원인:
      - `duckdb_writer=true` 태그 기반 동시성 제한 슬롯 경쟁.
      - 장시간 `STARTED` 1건이 슬롯을 점유하면 enqueue만 계속 누적.
    - 영구 조치(코드/설정):
      - `stuck_run_guard_sensor` 운영:
        - 오래 정체된 `STARTED` 자동 cancel
        - 옵션으로 자동 requeue
      - `incoming_manifest_sensor` tick당 생성 상한:
        - `INCOMING_SENSOR_MAX_NEW_RUN_REQUESTS_PER_TICK`
    - 운영 즉시 복구:
      - stuck run cancel/canceled 처리 후 queued drain.
      - 필요 시 Dagster 재시작:
        - `docker compose up -d --force-recreate dagster`

  3) 재발 시 표준 진단 순서
    - A. run 상태 확인:
      - `STARTED`, `CANCELING`, `QUEUED` 개수/대상 run_id 확인
    - B. DB 스키마 확인:
      - `SHOW TABLES` 결과에 `raw_files` 포함 여부 확인
    - C. 센서 상태 확인:
      - `incoming_manifest_sensor`, `stuck_run_guard_sensor`가 RUNNING인지 확인
    - D. 조치 순서:
      - (1) 스키마 보장 → (2) stuck run 정리 → (3) queue 정상화 → (4) 신규 run 성공 확인

  4) 운영 환경변수(권장)
    - `INCOMING_SENSOR_MAX_NEW_RUN_REQUESTS_PER_TICK=2`
    - `STUCK_RUN_GUARD_ENABLED=true`
    - `STUCK_RUN_GUARD_INTERVAL_SEC=120`
    - `STUCK_RUN_GUARD_TIMEOUT_SEC=10800`
    - `STUCK_RUN_GUARD_MAX_CANCELS_PER_TICK=1`
    - `STUCK_RUN_GUARD_AUTO_REQUEUE_ENABLED=true`
    - `STUCK_RUN_GUARD_MAX_REQUEUES_PER_TICK=1`
    - `STUCK_RUN_GUARD_TARGET_JOBS=mvp_stage_job,ingest_job,motherduck_sync_job`


┌─────────────────────────────────────────────────────────────────────────────┐
│                운영 트러블슈팅 런북 (2026-03-09 반영)                      │
└─────────────────────────────────────────────────────────────────────────────┘

  1) 증상: checksum 기준으로는 중복이 없어야 하는데 `raw_files`에 duplicate가 남음
    - 대표 현상:
      - `raw_key`는 서로 다른데 checksum이 같은 row가 여러 건 존재
      - `archive_path`도 서로 다른 듯 보이지만 실제 파일 내용은 동일
      - 일부 keeper row의 `error_message`에 duplicate marker가 불완전
    - 실제 원인:
      - 애플리케이션 로직은 `find_by_checksum()`으로 전역 중복을 보지만,
        운영 DuckDB 테이블에는 `UNIQUE(checksum)` 제약이 없었음.
      - 저장소 내 스키마가 분리되어 있었음:
        - `src/python/common/schema.sql` -> `checksum VARCHAR`
        - `src/vlm_pipeline/sql/schema.sql` -> `checksum VARCHAR UNIQUE`
      - `CREATE TABLE IF NOT EXISTS` 방식이라, 예전에 잘못 만들어진 테이블은
        현재 코드로도 자동 교정되지 않았음.
      - 또한 archive 원본 기준 재해시 결과 checksum drift가 `350`건 확인되어,
        앱 레벨 중복 조회가 miss될 여지가 있었음.
    - 진단 순서:
      - A. `raw_key` 중복과 `checksum` 중복을 분리해서 본다.
      - B. `archive_path` 중복도 별도로 본다.
      - C. 운영 DB 제약 조건에 `UNIQUE(checksum)`이 실제로 존재하는지 확인한다.
      - D. DB checksum을 그대로 믿지 말고 archive 원본 파일을 다시 해시한다.
    - 이번 복구 절차:
      - `scripts/recompute_archive_checksums.py`로 의심 대상 `946`건 재해시.
      - 결과:
        - unchanged `596`
        - checksum changed `350`
        - missing `0`
      - `scripts/cleanup_duplicate_assets.py`로 duplicate group 정리.
      - 정리 전 DB 백업:
        - `docker/data/pipeline.pre_dedup_20260309_063521.duckdb`
      - 정리 전 상태:
        - `raw_files=15922`
        - checksum 중복 그룹 `456`
        - duplicate row `490`
      - 정리 후 상태:
        - `raw_files=15432`
        - checksum 중복 `0`
        - `raw_key` 중복 `0`
        - `archive_path` 중복 `0`
    - 운영 원칙:
      - checksum dedup은 "코드 로직"과 "DB 제약"이 동시에 맞아야 함.
      - 둘 중 하나라도 빠지면 duplicate가 누적될 수 있음.

  2) 증상: `duplicate_skipped_in_manifest:<filename>` marker가 일부 중복 그룹에서 불완전함
    - 기대 규칙:
      - duplicate가 발생하면 surviving keeper row의 `error_message`에
        skip된 파일명이 개수만큼 남아 있어야 함.
    - 문제 원인:
      - basename이 같은 duplicate가 여러 번 섞인 경우,
        marker merge 과정에서 개수가 축약되는 케이스가 있었음.
    - 조치:
      - `src/vlm_pipeline/defs/ingest/duplicate.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - 위 두 지점의 marker merge 로직을 수정해 basename 반복도 그대로 보존.
    - 검증:
      - 정리 전 duplicate 그룹 `456`개를 백업 DB 기준으로 대조.
      - 정리 후 surviving keeper row의 marker mismatch `0`.
    - 운영 의미:
      - 이후 duplicate skip 이력을 `error_message`만으로도 재구성 가능.

  3) 증상: MinIO raw 경로에 `YYYY/MM` prefix가 섞여 들어감
    - 대표 예:
      - `vlm-raw/2026/03/adlibhotel-event-bucket/...`
      - `vlm-raw/2026/03/kkpolice-event-bucket/...`
    - 원인:
      - 과거 ingest 로직이 `raw_key` 앞에 `datetime.now().strftime("%Y/%m")`
        값을 붙였기 때문.
    - 현재 정상 규칙:
      - `raw_key = <source_unit_name>/<rel_path>`
      - 예:
        - `adlibhotel-event-bucket/20260204/fire_1_131000.mp4`
        - `kkpolice-event-bucket/2026-01-26/falldown_229_438.mp4`
    - 복구 절차:
      - old prefix 객체를 새 prefix로 복사/이동
      - DuckDB `raw_files.raw_key`도 같은 기준으로 갱신
      - old prefix 객체 삭제
    - 재발 방지:
      - 현재 `src/vlm_pipeline/defs/ingest/ops.py` 기준 ingest 경로에서는
        `YYYY/MM` prefix를 만들지 않음.
      - 단, 별도 스크립트가 MinIO key를 직접 조작하면 예외는 발생할 수 있음.

  4) 증상: archive / MinIO / DuckDB / MotherDuck 개수가 서로 다름
    - 이번 이슈에서 확인한 실제 패턴:
      - 중복 정리 직후:
        - DuckDB `15432`
        - MinIO `15432`
        - MotherDuck `15922`
        - archive 실제 파일 수 `15487`
      - 즉, 로컬 DB와 MinIO는 맞는데 archive와 MotherDuck이 뒤처진 상태였음.
    - 정렬 방법:
      - A. archive에서 DB에 없는 파일을 전수 확인
      - B. 초과 파일을 세 종류로 분리
        - 운영 marker(`_DONE`)
        - 잡파일(`.DS_Store`)
        - 실제 데이터 파일
      - C. 규칙 적용
        - `_DONE` -> 유지
        - `.DS_Store` -> 삭제
        - 실제 데이터 파일 -> checksum으로 duplicate 판단
          - duplicate면 삭제
          - unique면 MinIO + DuckDB + MotherDuck에 반영
    - 이번 실행 결과:
      - archive 초과 `55`
      - `_DONE` `49`
      - `.DS_Store` `4`
      - 실제 미디어 `2`
      - `weapon_298_020.mp4` -> unique, `completed`로 반영
      - `weapon_298_03.mp4` -> unique but 깨진 MP4, `failed`로 반영
    - MotherDuck 동기화:
      - `python3 /src/python/local_duckdb_to_motherduck_sync.py --db pipeline_db --local-db-path /data/pipeline.duckdb --share-update MANUAL --tables raw_files video_metadata`
    - 최종 운영 기준값:
      - DuckDB `raw_files=15434`
      - MinIO objects `15434`
      - MotherDuck `raw_files=15434`
      - archive 데이터 파일 수 `15434`
      - archive 전체 물리 파일 수 `15483`
      - 차이 원인: `_DONE` marker `49`개 유지
    - 운영 규칙:
      - "정합"은 archive 전체 물리 파일 수가 아니라,
        archive 데이터 파일 수를 기준으로 비교해야 함.

  5) 증상: DuckDB 교체 후 재기동 시 WAL replay 문제 발생
    - 원인:
      - DB 파일을 교체했는데 기존 `pipeline.duckdb.wal`이 남아 있어
        stale WAL이 새 DB에 재적용되려 했음.
    - 즉시 조치:
      - `app` 중지
      - stale WAL을 별도 보관:
        - `docker/data/pipeline.duckdb.wal.pre_dedup_stale_20260309_0640`
    - 영구 조치:
      - `scripts/cleanup_duplicate_assets.py`에
        - temp DB `CHECKPOINT`
        - DB swap 전후 WAL 백업 이동
        절차를 추가.
    - 운영 원칙:
      - DuckDB 파일을 교체하는 작업은 항상 서비스 중지 상태에서 수행하고,
        기존 WAL 존재 여부를 같이 확인해야 함.

  6) 증상: `vlm-labels`, `vlm-processed`, `vlm-dataset` 버킷이 자동으로 안 생김
    - 원인:
      - MinIO는 write 시 bucket auto-create를 하지 않음.
      - 코드에는 `ensure_bucket()` helper가 있었지만 실제 upload/copy 경로에서
        호출되지 않았음.
    - 조치:
      - `src/vlm_pipeline/resources/minio.py` 보강.
      - `upload()`, `upload_fileobj()`, `copy()` 전에
        `_ensure_bucket_once()`를 호출하도록 수정.
      - 동일 프로세스 내 재호출 비용을 줄이기 위해
        ensured bucket cache + lock 사용.
    - 결과:
      - 현재 버킷:
        - `vlm-raw`
        - `vlm-labels`
        - `vlm-processed`
        - `vlm-dataset`
    - 운영 원칙:
      - 이후 label/process/build 단계는 첫 write 시 bucket을 스스로 보장함.

  7) 증상: Dagster에서 `STARTED` run 2개가 1시간 이상 점유
    - 확인된 run:
      - `06349bfd-d236-4e39-b2d9-c9447c48d4ca`
      - `975ca406-3ae2-46d3-8b2a-bf2984850039`
    - 실제 상태:
      - UI/instance에는 `STARTED`로 남아 있었지만
        컨테이너 내부에는 해당 run의 실제 worker 프로세스가 없었음.
      - 마지막 이벤트도 duplicate skip 로그에서 멈춰 있었음.
      - 이 상태가 sensor backpressure(`in_flight`)를 점유하고 있었음.
    - 조치:
      - DagsterInstance API로 `CANCELING` -> `CANCELED` 처리.
    - 확인:
      - active run `0`
      - 이후 sensor가 다시 신규 run을 발행할 수 있는 상태로 복구.

  8) 오늘 기준 운영 체크포인트
    - 로컬 DuckDB, MinIO, MotherDuck, archive 데이터 파일 수는 모두 `15434`.
    - `video_metadata`는 `15433`.
    - `failed raw_files`는 `1`건 (`weapon_298_03.mp4`, ffprobe failure).
    - checksum duplicate는 `0`.
    - `raw_key` duplicate는 `0`.
    - archive `_DONE` marker `49`개는 의도적으로 유지 중.
    - 오늘 추가/수정한 핵심 파일:
      - `scripts/recompute_archive_checksums.py`
      - `scripts/cleanup_duplicate_assets.py`
      - `src/vlm_pipeline/defs/ingest/duplicate.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/resources/minio.py`


┌─────────────────────────────────────────────────────────────────────────────┐
│                 Staging 아키텍처 및 트러블슈팅 런북 (2026-03-13 반영)      │
└─────────────────────────────────────────────────────────────────────────────┘

  1) Staging 전용 Dispatch 로직 및 파이프라인 동적 분기
    - 대상: staging 파이프라인
    - 목표: 운영과 차별화된 "조건부 처리(dispatch) + 동적 파이프라인 동시 실행"
    - 정확한 파이프라인 흐름:
      - [1단계: 원본 대기] `incoming`에 데이터 진입 후 대기 (자동 Ingest 비활성화)
      - [2단계: Dispatch 요청] 특정 JSON 파일 감지 (`.dispatch/pending/`)
      - [3단계: Ingest & 1차 이동 동시 실행] 해당 폴더의 메타데이터를 DuckDB에 적재, MinIO(`vlm-raw`) 업로드 후 `incoming`에서 최종 `archive`로 즉시 이동
      - [4단계: AI 파이프라인 동적 실행] JSON의 `run_mode`(`gemini`, `yolo`, `both`)에 맞춰 후속 파이프라인(YOLO, Gemini 등) 연속 처리
    - 추출 설정: `current`, `dense` 등 다중 프레임 추출 설정 지원
    - 이력 추적: `staging_dispatch_requests` DB 테이블 추가

  2) 증상: `raw_ingest` 단계에서 1시간 이상 정체 (Stuck)
    - 증상: `incoming` 디렉터리에 복사가 다 되었음에도 불구하고 1GB 이상의 대용량 파일 업로드 시 파이프라인이 정체됨 (18분/파일 이상 소요)
    - 원인: 1-pass 로딩 최적화를 위해 임시 파일(SpooledTemporaryFile)을 사용하면서 로컬 디스크 I/O와 메모리 복사가 발생, Boto3의 네이티브 멀티파트 병렬 업로드의 이점을 살리지 못하고 단일 스트림으로 업로드되어 병목이 발생함
    - 조치: 
      - `load_video_once`에서 `include_file_stream=False`로 비활성화하여 tempfile 생성을 막음
      - `minio.py`에 `upload_file` 메소드를 추가해 boto3의 S3 네이티브 `upload_file` (멀티스레딩 멀티파트 청크 동시 읽기) 기능 활성화
      - `ops.py`의 `_upload_single`에서 NAS 원본 경로를 직접 참조해 병렬 파도타기 업로드 수행으로 전환함

  --- 2026-03-17 자동 기록 (상세 정리) ---
  1) .agent/skill 복구 및 로컬 자동화 도구 재연결
    - 문제: `.agent/skill` 폴더가 사라지면서 `daily_worklog`, `duckdb_staging_wiper`, `staging_reset` 같은 로컬 자동화 도구를 더 이상 사용할 수 없게 됨.
    - 원인: `.agent/`를 gitignore 대상으로 전환하고 추적 해제하는 과정에서 로컬에서도 스킬 디렉터리가 실제 삭제된 상태로 반영되었음.
    - 조치:
      - 이전 커밋 이력에서 `.agent/skill/` 전체를 복원해 로컬 전용 스킬 세트를 다시 사용할 수 있게 정리.
      - `daily_worklog`, `duckdb_staging_wiper`, `staging_reset` 스크립트가 다시 동작하도록 파일과 디렉터리 구조를 복구.
      - 이후 `.agent`는 저장소 추적에서 제외하되, 로컬 작업 자동화에는 계속 쓰는 방향으로 운영 기준을 정함.
    - 관련 파일:
      - `.agent/skill/daily_worklog/SKILL.md`
      - `.agent/skill/daily_worklog/scripts/daily_worklog.py`
      - `.agent/skill/duckdb_staging_wiper/SKILL.md`
      - `.agent/skill/duckdb_staging_wiper/scripts/wipe_staging_db.sh`
      - `.agent/skill/staging_reset/SKILL.md`
      - `.agent/skill/staging_reset/scripts/reset_staging.sh`
  2) Linear MCP 및 일일 기록 포맷 정리
    - 문제: Cursor 외 IDE에서 Linear MCP를 공용으로 쓰기 어려웠고, 자동 생성되는 WORKLOG/CLAUDE 기록이 파일 나열 위주라 실제 문제 해결 이력을 읽기 어려웠음.
    - 원인: MCP 설정이 프로젝트 공용 마스터에 반영되지 않았고, daily worklog 자동화 스크립트도 문제/원인/조치보다 커밋 나열 중심으로 작성하고 있었음.
    - 조치:
      - `.agent/mcp/mcp_config.json`과 동기화 스크립트를 이용해 여러 IDE에서 공용 MCP 설정을 재사용할 수 있도록 정리.
      - `WORKLOG.md`와 `CLAUDE.md`는 이후 문제/원인/조치 중심으로 읽히는 포맷으로 수동 보정 및 자동화 개선 방향을 확정.
      - `.env.example`도 값 비우기와 Vertex prompt normal 제외 기준으로 정리해 배포/문서 혼선을 줄임.
    - 관련 파일:
      - `.agent/mcp/mcp_config.json`
      - `.agent/mcp/sync_mcp.sh`
      - `.env.example`
      - `WORKLOG.md`
      - `CLAUDE.md`
  3) 재발 시 확인 포인트
    - `.agent/` 관련 이슈가 다시 생기면 Git 추적 해제 커밋과 로컬 디렉터리 실제 삭제 여부를 함께 확인할 것.
    - daily worklog 자동화가 다시 단순 요약으로 흐르면 `.agent/skill/daily_worklog/scripts/daily_worklog.py` 템플릿 변경 여부를 우선 점검할 것.

  --- 2026-03-18 자동 기록 (상세 정리) ---
  1) staging spec 기반 아키텍처 신설
    - 문제: staging에서 auto labeling 요구사항과 실험 조건을 안전하게 검증하려면, 운영 경로와 분리된 spec 중심 실행 계층이 필요했음.
    - 원인: 기존 구조는 공용 ingest/label/process 흐름 위주라 staging에서 spec 단위 실행 조건, QA 시나리오, 별도 sensor를 담기 어려웠음.
    - 조치:
      - `definitions_staging.py`, `duckdb_spec.py`, `defs/spec/assets.py`, `defs/spec/sensor.py`를 추가해 staging 전용 spec 흐름을 구성.
      - staging용 Dagster definitions와 workspace를 Docker 레벨에서 별도로 로딩할 수 있도록 compose와 app 설정을 보강.
      - DuckDB schema와 labeling 자원도 spec 실행 컨텍스트를 이해할 수 있도록 함께 확장.
    - 관련 파일:
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/spec/assets.py`
      - `src/vlm_pipeline/defs/spec/sensor.py`
      - `src/vlm_pipeline/resources/duckdb_spec.py`
      - `docker/app/dagster_defs_staging.py`
      - `docker/app/workspace_staging.yaml`
  2) staging QA 스크립트 및 운영 보조 도구 추가
    - 문제: staging spec 흐름을 반복 검증하려면, dispatch/QA/대기 로직을 수동으로 확인할 수 있는 스크립트와 문서가 부족했음.
    - 원인: 검증 절차가 정식 스크립트와 문서로 묶여 있지 않아, 동일 테스트를 재현하는 데 사람 의존도가 높았음.
    - 조치:
      - staging QA 실행, incoming drop, 기대 조건 대기용 스크립트를 추가.
      - spec 요구사항, UI, 통합 설계 문서를 함께 작성해 이후 staging 개편 시 기준점으로 사용.
    - 관련 파일:
      - `scripts/staging_qa_run_cycle.sh`
      - `scripts/staging_qa_safe_drop_incoming.sh`
      - `scripts/staging_qa_wait_expect.py`
      - `scripts/staging_test_dispatch.py`
      - `auto_labeling_requirements_spec.md`
      - `auto_labeling_unified_spec.md`

  --- 2026-03-19 자동 기록 (상세 정리) ---
  1) staging 전용 spec asset/sensor 분리
    - 문제: staging 실험용 spec 흐름이 운영 공용 경로와 너무 가까워, 테스트 반복 시 책임 경계가 불명확해질 수 있었음.
    - 원인: 공용 spec asset/sensor와 staging 실험 흐름이 같은 레이어를 공유하면서, staging에서만 필요한 run tag와 sensor 동작을 독립적으로 다루기 어려웠음.
    - 조치:
      - `defs/spec/staging_assets.py`, `defs/spec/staging_sensor.py`를 추가해 staging 전용 실행 경로를 분리.
      - `definitions_staging.py`, `defs/dispatch/sensor.py`, `lib/spec_config.py`를 조정해 staging spec 실행 구성을 분리된 방식으로 해석하도록 보강.
    - 관련 파일:
      - `src/vlm_pipeline/defs/spec/staging_assets.py`
      - `src/vlm_pipeline/defs/spec/staging_sensor.py`
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `src/vlm_pipeline/lib/spec_config.py`
  2) frame 단위 검증 및 벤치 스크립트 보강
    - 문제: video frame 추출과 YOLO 연계 품질을 staging에서 별도로 검증할 도구가 부족했음.
    - 원인: 프레임 단위 추출 helper와 벤치 스크립트, 단위 테스트가 충분하지 않아 frame 생성 단계 문제를 빠르게 분리하기 어려웠음.
    - 조치:
      - `video_frames.py`와 process/label/yolo asset을 보강하고, 벤치 스크립트 및 unit test를 추가.
      - 문서 초안(spec md)은 정리 대상으로 돌리면서 실제 코드 경로 중심으로 구조를 단순화.
    - 관련 파일:
      - `scripts/staging_video_extract_yolo_bench.py`
      - `src/vlm_pipeline/lib/video_frames.py`
      - `tests/unit/test_video_frames.py`

  --- 2026-03-20 자동 기록 (상세 정리) ---
  1) staging Vertex chunking 및 event frame caption 도입
    - 문제: 긴 영상이나 event 구간에서 어떤 프레임을 대표 이미지로 삼아 캡셔닝할지 기준이 약했고, staging VQA 품질을 끌어올릴 helper가 부족했음.
    - 원인: event 레벨 caption과 frame/image 레벨 caption 흐름이 세밀하게 분리되지 않았고, VertexAI를 staging 전용으로 활용하는 helper 계층도 충분하지 않았음.
    - 조치:
      - `staging_vertex.py`를 도입·확장하고, label/process asset에서 event frame caption, relevance 판단, chunking 처리를 반영.
      - DuckDB 저장 계층과 schema도 맞춰 수정하고, 관련 helper/unit test를 추가.
    - 관련 파일:
      - `src/vlm_pipeline/lib/staging_vertex.py`
      - `src/vlm_pipeline/defs/label/assets.py`
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_labeling.py`
      - `tests/unit/test_staging_vertex_helpers.py`
  2) 로컬 전용 자산 추적 범위 정리
    - 문제: `doc_2` 메모와 `.agent/skill` 같은 로컬 전용 자산이 협업 저장소 이력에 계속 남아 있으면 실제 코드 변경과 섞여 보일 수 있었음.
    - 원인: 로컬 전용 파일이 Git 추적 범위에 남아 있었고, 팀 저장소와 개인 환경 경계가 다시 흐려질 가능성이 있었음.
    - 조치:
      - `.gitignore`에 `doc_2`를 추가.
      - `.agent/skill` 추적을 저장소에서 제거해 로컬 전용 운영으로 다시 정리.

  --- 2026-03-23 자동 기록 (상세 정리) ---
  1) 운영 DuckDB `image_metadata__migrated` 오류 대응
    - 문제: 운영 Dagster `clip_to_frame` 단계에서 `image_metadata__migrated does not exist` 오류가 반복되며 run이 중간부터 연속 실패함.
    - 원인: `clip_to_frame` 단일 로직 문제라기보다, 운영 DuckDB의 `image_metadata` 마이그레이션 또는 카탈로그 상태가 꼬여 임시 테이블 참조가 남는 쪽으로 판단됨.
    - 조치:
      - `ensure_schema()`의 런타임 위험 동작을 줄이고, 스키마 보정 로직을 완화.
      - 운영 DB 상태 점검/복구용 `scripts/repair_image_metadata_schema.py`를 추가.
      - 운영 Dagster 재기동, stale run 정리, 재실행까지 진행.
      - 다만 운영 DB 파일 자체 손상 가능성은 별도 리스크로 남겨 두고 오프라인 복구 경로를 준비.
    - 관련 파일:
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/sql/schema.sql`
      - `scripts/repair_image_metadata_schema.py`
  2) staging dispatch JSON 형식 및 분기 규칙 개편
    - 문제: staging 테스트에서 전달하는 dispatch JSON 형식이 실제 코드가 기대하는 값과 달라, `bbox`, `captioning`, `image classification` 조합별 원하는 실행 흐름을 만들기 어려웠음.
    - 원인: 기존은 `outputs`/`run_mode` 위주였고, 실제 테스트 입력은 `categories`, `classes`, `labeling_method` 중심으로 바뀌어 있었음.
    - 조치:
      - `staging_dispatch.py`를 추가해 `categories`, `classes`, `labeling_method` 기반으로 JSON을 해석.
      - `bbox`, `image classification`만 오면 YOLO-only, `captioning+bbox`면 VertexAI 캡셔닝 + YOLO가 함께 수행되도록 라우팅 규칙을 재정의.
      - `필요없음`, `라벨링필요없음` 값이 포함되면 archive 이동과 MinIO 업로드만 수행하고 라벨링은 생략하도록 분기 추가.
    - 관련 파일:
      - `src/vlm_pipeline/lib/staging_dispatch.py`
      - `src/vlm_pipeline/lib/env_utils.py`
      - `src/vlm_pipeline/lib/spec_config.py`
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
  3) staging ingest 흐름을 trigger JSON 기준으로 재정의
    - 문제: trigger JSON이 없어도 staging에서 incoming 자동 스캔이나 `archive_pending` 경유 경로가 살아 있어, 사용자가 의도하지 않은 ingest/업로드가 발생할 수 있었음.
    - 원인: auto-bootstrap, dispatch, ingest sensor가 동시에 개입하면서 trigger JSON 없는 입력까지 처리하거나, `archive_pending`이 staging에서 불필요하게 유지되고 있었음.
    - 조치:
      - trigger JSON이 없으면 staging에서는 `incoming`에서 대기만 하도록 auto-bootstrap 경로를 차단.
      - `archive_pending`은 staging dispatch 흐름에서 제외.
      - trigger JSON이 있을 때만 `incoming -> archive -> MinIO 업로드` 흐름을 타도록 정리.
      - `.dispatch`, `.manifests`, staging MinIO, DuckDB를 여러 차례 초기화하며 반복 검증.
    - 관련 파일:
      - `src/vlm_pipeline/defs/dispatch/sensor_incoming_mover.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`
      - `src/vlm_pipeline/defs/ingest/assets.py`
      - `src/vlm_pipeline/defs/ingest/sensor_bootstrap.py`
      - `src/vlm_pipeline/defs/ingest/sensor_incoming.py`
  4) staging YOLO 실행 순서 보장
    - 문제: `raw_ingest`와 `yolo_image_detection`이 거의 동시에 시작돼, frame 생성 전 `YOLO 대상 이미지 없음` 로그가 뜨는 순서 문제가 있었음.
    - 원인: staging에서도 공용 YOLO asset이 직접 job selection에 연결돼 있어, frame 생성 완료 여부와 무관하게 먼저 떠버릴 수 있었음.
    - 조치:
      - staging 전용 YOLO asset을 분리해 `raw_ingest -> clip/frame 생성 -> yolo_image_detection` 순서가 보장되도록 수정.
      - staging Dagster 재기동, canceled run 재실행, trigger JSON 재투입으로 동작 검증.
    - 관련 파일:
      - `src/vlm_pipeline/defs/yolo/assets.py`
      - `src/vlm_pipeline/defs/yolo/staging_assets.py`
      - `src/vlm_pipeline/definitions_staging.py`
