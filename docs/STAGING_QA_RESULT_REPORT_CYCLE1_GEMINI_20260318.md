# Staging QA 보고서 — Cycle 1-1: Gemini 모드 Dispatch (tmp_data_2)

**일시**: 2026-03-18  
**환경**: dagster-staging (port 3031)  
**테스트 데이터**: `staging/tmp_data_2` (fall_down 7, smoking 5, violence 5, weapon 7, gs건설 1 = 25 비디오)  
**Dispatch 설정**: `outputs: ["timestamp", "captioning"]`, `request_id: qa_cycle1_gemini_002`

---

## 1. 테스트 실행 요약

| 단계 | 결과 | 비고 |
|------|------|------|
| incoming 복사 | PASS | 25개 비디오 incoming에 복사 |
| incoming_to_pending_sensor | PASS | tmp_data_2 → archive_pending 이동 |
| dispatch trigger JSON 배치 | PASS | `.dispatch/pending/qa_cycle1_gemini_002.json` |
| dispatch_sensor 감지 | PASS | RunRequest 발행 (run: 38b7344e) |
| dispatch_stage_job 실행 | PASS | RUN_SUCCESS |
| raw_ingest | **PASS** | 25/25 성공, 0 실패, 55분 24초 |
| MinIO 업로드 | PASS | vlm-raw: 25 objects |
| archive 이동 | PASS | archive_pending → archive + _DONE marker |
| clip_timestamp_routed | **FAIL** | spec_id 없어서 스킵 |
| clip_captioning_routed | **FAIL** | requested_outputs 인식 실패 |
| yolo_image_detection | SKIP (정상) | outputs에 bbox 없음 |
| raw_video_to_frame | SKIP (정상) | outputs에 bbox 없음 |
| clip_to_frame_routed | SKIP (정상) | outputs에 bbox 없음 |

## 2. DB 검증 결과

| 테이블 | 건수 | 상태 |
|--------|------|------|
| raw_files | 25 (completed: 25) | PASS |
| video_metadata | 25 | PASS |
| labels | 0 | 후속 단계 미실행 |
| image_metadata | 0 | 후속 단계 미실행 |
| staging_dispatch_requests | 1 (running) | PASS |
| staging_pipeline_runs | 4 (all pending) | pipeline_runs 상태 미갱신 |

## 3. MinIO 검증 결과

| 버킷 | 객체 수 | 상태 |
|------|---------|------|
| vlm-raw | 25 | PASS |
| vlm-labels | 0 | 후속 단계 미실행 |
| vlm-processed | 0 | 후속 단계 미실행 |
| vlm-dataset | 0 | 후속 단계 미실행 |

## 4. 발견된 이슈

### 이슈 #1: dispatch_sensor NFS 타임아웃 (RESOLVED)
- **증상**: `os.walk`가 NFS에서 41초+ 소요 → sensor 전체 60초 초과 → gRPC timeout
- **영향**: DB에 기록 + trigger JSON processed로 이동했지만 RunRequest 미발행
- **조치**:
  1. `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS=180` 설정 (`.env.staging`)
  2. `dispatch_sensor`의 `os.walk`를 `os.scandir` 재귀로 변경 (stat() 호출 제거)
- **결과**: sensor 실행 시간 ~70초로 감소, 180초 timeout 내 정상 완료

### 이슈 #2: dispatch flow에서 후속 labeling 단계 전체 스킵 (OPEN)
- **증상**: `clip_timestamp_routed`, `clip_captioning_routed` 모두 스킵
- **원인**:
  - `clip_timestamp_routed`: `spec_id` tag 필수 → dispatch flow에서는 spec_id 미사용
  - `clip_captioning_routed`: `requested_outputs` tag를 `_`로 split → dispatch에서는 `outputs` tag에 `,` 구분
- **영향**: Gemini timestamp/captioning 처리가 실행되지 않음
- **필요 조치**: dispatch flow에서 `outputs` tag를 읽어 labeling 단계를 실행하도록 코드 수정 필요

### 이슈 #3: staging_pipeline_runs 상태 미갱신 (OPEN)
- **증상**: dispatch_sensor가 생성한 pipeline_runs 레코드가 모두 `pending` 상태 유지
- **원인**: 각 asset이 실행 후 pipeline_runs 상태를 갱신하는 로직 미구현
- **영향**: 파이프라인 진행 상태 추적 불가

### 이슈 #4: raw_ingest 소요 시간 과다 (KNOWN)
- **증상**: 25개 비디오(각 ~80MB) 업로드에 55분 24초 소요
- **원인**: NFS → MinIO 전송 병목 (단일 스레드 순차 업로드)
- **영향**: 대량 데이터 테스트 시 장시간 소요

## 5. 판정

| 항목 | 판정 |
|------|------|
| Dispatch 기본 흐름 (trigger → sensor → job) | **PASS** |
| raw_ingest (파일 등록 + MinIO 업로드 + archive 이동) | **PASS** |
| 후속 labeling 파이프라인 (timestamp, captioning) | **FAIL** |
| 전체 Cycle 1-1 | **PARTIAL PASS** |

## 6. 코드 변경 사항

| 파일 | 변경 내용 |
|------|-----------|
| `docker/.env.staging` | `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS=180` 추가 |
| `src/vlm_pipeline/defs/dispatch/sensor.py` | `os.walk` → `os.scandir` 재귀 변경 (NFS 성능 최적화) |

## 7. 다음 단계

1. **이슈 #2 수정**: dispatch flow에서 `outputs` tag 기반으로 labeling 단계 실행하도록 코드 수정
2. **이슈 #3 수정**: pipeline_runs 상태 갱신 로직 추가
3. 수정 후 Cycle 1-1 재테스트 (labeling 단계 포함)
4. Cycle 1-2 (YOLO 모드) 진행
