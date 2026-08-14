# 검증된 미해결 발견 백로그 — 2026-08-12 전수 감사

> 2026-08-12 멀티모델 감사(8 발견 트랙 + 적대적 검증, 2026-07-01 감사 후속)의 산출물 중
> **코드로 실증(CONFIRMED)됐으나 이번 정리 브랜치(`refactor/perf-audit-20260812`)에 싣지 않은 것**.
> 싣지 않은 이유: 상태머신/스키마/배포 거동을 바꾸므로 팀 게이트가 필요.
> 각 항목의 file:line 은 main@5e1aafb 기준 — 착수 전 재확인.
> (이번 브랜치에서 처리 완료: upsert_failed 터미널 가드(DISPATCH-5 Case A), find_projects_ready_to_build dead code,
>  CIFS 이중 stat/이중 listing, poll_once state 재파싱, dead method 18개, 구IP 일소.)

## A. 정합성 — 조용한 오염/유실 (우선)

| # | 발견 | 위치 | 수정 방향 |
|---|---|---|---|
| A1 | `build_dataset` 이 per-file 복사 실패를 삼키고 `completed` 마킹 → NOT EXISTS 게이트가 부분 데이터셋을 영구 잠금 | `defs/build/assets.py:130-137,204-211,248` | 실패 카운트 → 실패 있으면 `partial`/`failed`, 게이트가 재시도 가능하게 |
| A2 | `build_classification` 동형 — 복사 시도 전에 카테고리를 manifest 에 기록 + 실패 무시 `completed` | `defs/build/classification.py:114,126-127,148,160-161` | 복사 성공 후 append, 실패 시 `partial` |
| A3 | `flush_image_labels` 가 insert 실패를 삼키는데 호출자는 이미 processed/completed 마킹 | `lib/detection_common.py:71-75` + `defs/sam/detection_assets.py:287-309` | flush 성공 확정 후에만 카운터/버퍼 진행 |
| A4 | SAM3-4: ls_sync 가 reviewed DB 커밋 → MinIO 쓰기 순서 (실패 창에서 stale auto 박스가 GT 로 투영) | `src/gemini/ls_sync.py:325`, `ls_sync_db.py:272` | 순서 역전: MinIO write 성공 후 DB 전이 |
| A5 | SAM3-1 잔여 절반: shadow_compare 가 사람 검수 COCO 를 같은 MinIO 키로 덮어씀 (DB 가드는 2026-07 수정, MinIO 경로는 미수정. YOLO off 라 잠재) | `defs/sam/assets.py:138,172-189` | shadow 전용 키(`sam3_shadow/...`) + reviewed/finalized NOT EXISTS |
| A6 | FS 데이터셋 복사가 "같은 바이트 크기 = 무변경" 판정 → 같은 크기의 검수 수정본이 영원히 미반영 | `defs/build/build_helpers.py:42-54` | ETag/mtime 사이드카 비교 (MinIO 모드와 대칭) |

## B. 재처리/리퍼 — 터미널 고아 (감사 패턴 C/E 잔여)

| # | 발견 | 위치 | 수정 방향 |
|---|---|---|---|
| B1 | DISPATCH-4: 터미널(failed/canceled) request_id 재드롭이 `duplicate_request_id` 로 영구 거부 | `resources/postgres_ingest_dispatch.py:69-82`, `defs/dispatch/service.py:365` | 중복체크를 non-terminal 로 스코프 |
| B2 | SAM3-3: `ls_task_status='failed'` 터미널, 재픽업 없음 (LS 다운/키 만료가 영구 고아 생산) | `defs/ls/sensor.py:80,276` | bounded retry (`ls_task_attempts < 3`) |
| B3 | `frame_extract_status='processing'` 고아 — 리퍼 없음, 후보쿼리는 pending 만 | `defs/process/raw_frames.py:87`, `resources/postgres_video_metadata.py:374` | `stale_state_reaper_sensor` 에 video_metadata/processed_clips 분기 추가 |
| B4 | `auto_labeling_sensor` 가 run 실패 시(DB 무변화) cursor 동일성으로 영구 침묵 | `defs/label/sensor.py:95-99,166-169` | embed 센서 fix 미러링 (unchanged early-return 제거) |
| B5 | `has_active_dispatch_run` 이 NOT_STARTED/STARTING 누락 + 예외 시 fail-open → 라이브 디스패치가 stale 판정 취소될 수 있음 | `defs/dispatch/service.py:46-51` | 상태 보강 + fail-closed |

## C. 동시성/관측성

| # | 발견 | 위치 | 수정 방향 |
|---|---|---|---|
| C1 | DISPATCH-1: 폴더 최초 디스패치 TOCTOU — 두 센서 동시 진입 가능 | `service.py`, `postgres_ingest_dispatch.py:30-38` | `pg_advisory_xact_lock(hashtext(folder))` 또는 partial unique index |
| C2 | DISPATCH-3: from_archived 경로가 dispatch_requests 미생성 + run-status/stuck-guard 미커버 | `defs/dispatch/archive_dispatch_sensor.py:138`, `sensor_run_status.py:21` | row insert + `upload_label_job` 타깃 추가 |
| C3 | BUILD-3: `build_classification` 이 어떤 job/sensor 에도 미배선 (수동 materialize 전용) | `definitions_production.py:265` | finalize 센서 추가 또는 "수동 전용" 명시 |

## D. 성능 (측정 후 착수 권장)

| # | 발견 | 위치 | 수정 방향 |
|---|---|---|---|
| D1 | `find_pending_images` NOT EXISTS 안티조인에 인덱스 부재 (`image_labels` 는 PK 뿐) — tick 마다 실행 | `sql/schema_postgres.sql:203-216` | `CREATE INDEX CONCURRENTLY idx_image_labels_image_tool ON image_labels(image_id, label_tool)` — 단일문 마이그레이션(러너 DO-block 제약) |
| D2 | `resolve_matching_asset`/`find_by_raw_key_stem` 가 라벨 JSON 당 raw_files full scan (raw_key 인덱스 없음) | `resources/postgres_ingest_raw.py`(stem 질의) | expression index + anchored 매칭 |
| D3 | 수동 라벨 import 가 JSON 파일당 source_unit_dir 전체 rglob + 같은 파일 3회 읽기(bytes/upload/checksum) | `defs/label/artifact_resolve.py:36,74-83,225-245` | per-run 인덱스 1회 구축 + bytes 재사용 |
| D4 | 재인코딩 타임아웃 계산이 이미 아는 duration 을 ffprobe 재탐침 | `lib/video_reencode.py:292` | `known_duration_sec` 파라미터 스레딩 |
| D5 | raw_video 프레임 추출이 프레임당 ffmpeg 1 프로세스 (≤24/36 캡) | `defs/process/raw_frames.py:199-205` | 근접 타임스탬프만 배치 (sparse 는 per-seek 유지) |

## E. 잔여 dead-code 유력 (판별 곤란으로 보류)

- `PostgresGenAIMixin` 잔여 6메서드(~150줄) — `docker/genai/db/pg.py` 동명 함수와 이름 충돌로 참조 카운트 2. Dagster측 실사용은 `update_genai_job_assets`(getattr) 뿐일 가능성 — 호출 그래프 트레이싱 후 일괄 철거 후보.
- `delete_failed_rows_by_error_filters` (`postgres_ingest_audit.py`) — 참조 0 이나 파괴적 복구 헬퍼 형태라 보존.

## F. 인프라 잔재 (배포 영향으로 이 브랜치 제외)

- `docker/docker-compose.yaml:8,91,554` + `docker-compose.labelstudio.yaml:65-66` — `MINIO_ENDPOINT` fallback 이 폐기된 `10.0.0.51` (**.env 유실 시 조용히 활성화**되는 최고위험 잔재)
- `.github/workflows/deploy-*.yml:180,183` — step summary 의 구IP echo (표시용)
- `scripts/dvc/ingest_to_catalog.py:59` — 구IP fallback (DVC ingest 트랙 자체가 dormant)
- prod repo **미추적** `tests/unit/test_db_backend_parity.py:48,191` — 이 브랜치 머지 후 로컬 `pytest tests/unit` 에서 삭제된 `find_phash_null`/`list_labeling_configs` 호출로 실패. 해당 두 assert 제거 필요 (CI 는 미추적이라 무관)
