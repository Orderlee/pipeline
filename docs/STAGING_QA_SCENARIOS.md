# Auto Labeling Staging QA 시나리오

> 대상 환경: staging (`dagster-staging`, port 3031)
> 기준 문서: `auto_labeling_unified_spec.md`, `auto_labeling_spec.md`, `auto_labeling_requirements_spec.md`, `auto_labeling_ui_spec.md`
> 작성일: 2026-03-18

---

## 목차

1. [사전 조건](#1-사전-조건)
2. [Cycle 1: Dispatch 기본 흐름](#2-cycle-1-dispatch-기본-흐름)
3. [Cycle 2: Spec Flow — 데이터 먼저 도착](#3-cycle-2-spec-flow--데이터-먼저-도착)
4. [Cycle 3: Spec Flow — Spec 먼저 도착](#4-cycle-3-spec-flow--spec-먼저-도착)
5. [Cycle 4: Spec Flow — 동시 도착](#5-cycle-4-spec-flow--동시-도착)
6. [Cycle 5: Pending → Slack 확정 → 자동 재개](#6-cycle-5-pending--slack-확정--자동-재개)
7. [Cycle 6: Config 우선순위 검증](#7-cycle-6-config-우선순위-검증)
8. [Cycle 7: labeling_method 조합별 실행 검증](#8-cycle-7-labeling_method-조합별-실행-검증)
9. [Cycle 8: 이벤트 0건 처리](#9-cycle-8-이벤트-0건-처리)
10. [Cycle 9: 재시도 한도 초과 → failed](#10-cycle-9-재시도-한도-초과--failed)
11. [Cycle 10: 대량 파일 배치 처리](#11-cycle-10-대량-파일-배치-처리)
12. [Cycle 11: Config Sync 검증](#12-cycle-11-config-sync-검증)
13. [Cycle 12: DB 스키마 정합성 검증](#13-cycle-12-db-스키마-정합성-검증)
14. [Cycle 13: MinIO 버킷 정합성 검증](#14-cycle-13-minio-버킷-정합성-검증)
15. [Cycle 14: Sensor 동작 검증](#15-cycle-14-sensor-동작-검증)
16. [Cycle 15: 에러 복구 시나리오](#16-cycle-15-에러-복구-시나리오)
17. [Cycle 16: Dagster UI Lineage 검증](#17-cycle-16-dagster-ui-lineage-검증)
18. [Cycle 17: 재처리(Reprocessing) 검증](#18-cycle-17-재처리reprocessing-검증)
19. [Cycle 18: 엣지 케이스 종합](#19-cycle-18-엣지-케이스-종합)
20. [부록: 공통 검증 쿼리](#20-부록-공통-검증-쿼리)

---

## 1. 사전 조건

### 환경 초기화 체크리스트

| 항목 | 확인 방법 | 기대값 |
|------|-----------|--------|
| staging DuckDB | `SELECT COUNT(*) FROM raw_files` | 0 |
| staging MinIO | vlm-raw/labels/processed/dataset | 각 0 objects |
| .manifests | `ls .manifests/pending/` | 비어있음 |
| .dispatch | `ls .dispatch/pending/` | 비어있음 |
| dagster-staging | `docker ps` | Up, 에러 없음 |
| incoming 테스트 데이터 | `/home/pia/mou/staging/incoming/` | 테스트 폴더 존재 |

### 테스트 데이터 준비

```bash
# 테스트용 비디오 파일 (최소 3개, 다양한 크기)
# 예시 폴더 구조:
# /home/pia/mou/staging/incoming/test_smoke_001/
#   ├── smoke_video_01.mp4
#   ├── smoke_video_02.mp4
#   └── smoke_video_03.mp4
# /home/pia/mou/staging/incoming/test_fire_002/
#   ├── fire_video_01.mp4
#   └── fire_video_02.mp4
# /home/pia/mou/staging/incoming/test_mixed_003/
#   ├── weapon_video_01.mp4
#   ├── falldown_video_01.mp4
#   └── violence_video_01.mp4
```

---

## 2. Cycle 1: Dispatch 기본 흐름

> 목적: dispatch JSON → ingest → timestamp/yolo 기본 동작 확인

### 시나리오 1-1: Gemini 모드 dispatch

**입력:**
```json
// .dispatch/pending/test_gemini_001.json
{
  "folder_name": "test_smoke_001",
  "run_mode": "gemini",
  "outputs": "timestamp",
  "requested_by": "qa_tester"
}
```

**검증:**

| # | 검증 항목 | 쿼리/확인 방법 | 기대 결과 |
|---|-----------|----------------|-----------|
| 1 | dispatch 요청 기록 | `SELECT * FROM staging_dispatch_requests WHERE folder_name='test_smoke_001'` | 1 row, status='completed' |
| 2 | pipeline run 기록 | `SELECT * FROM staging_pipeline_runs WHERE folder_name='test_smoke_001'` | step별 row, step_status='completed' |
| 3 | raw_files 적재 | `SELECT COUNT(*) FROM raw_files WHERE source_unit_name='test_smoke_001'` | 파일 수와 일치 |
| 4 | video_metadata 적재 | `SELECT COUNT(*) FROM video_metadata vm JOIN raw_files rf ON vm.asset_id=rf.asset_id WHERE rf.source_unit_name='test_smoke_001'` | 비디오 파일 수와 일치 |
| 5 | MinIO raw 업로드 | vlm-raw 버킷 객체 확인 | 파일 수와 일치 |
| 6 | archive 이동 | `/home/pia/mou/staging/archive/test_smoke_001/` 존재 | 파일 이동 완료 |
| 7 | labels 생성 | `SELECT COUNT(*) FROM labels l JOIN raw_files rf ON l.asset_id=rf.asset_id WHERE rf.source_unit_name='test_smoke_001'` | >= 0 (이벤트 수에 따라) |
| 8 | vlm-labels JSON | vlm-labels 버킷 확인 | timestamp JSON 존재 |

### 시나리오 1-2: YOLO 모드 dispatch

**입력:**
```json
// .dispatch/pending/test_yolo_001.json
{
  "folder_name": "test_fire_002",
  "run_mode": "yolo",
  "outputs": "bbox",
  "max_frames_per_video": 10,
  "jpeg_quality": 85,
  "requested_by": "qa_tester"
}
```

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | raw_files 적재 | 파일 수 일치 |
| 2 | image_metadata 생성 | frame 추출 수 확인 |
| 3 | image_labels 생성 | YOLO detection 결과 존재 |
| 4 | vlm-processed 버킷 | frame 이미지 존재 |
| 5 | vlm-labels 버킷 | detection JSON 존재 |

### 시나리오 1-3: Both 모드 dispatch

**입력:**
```json
// .dispatch/pending/test_both_001.json
{
  "folder_name": "test_mixed_003",
  "run_mode": "both",
  "outputs": "timestamp,bbox",
  "requested_by": "qa_tester"
}
```

**검증:** 시나리오 1-1 + 1-2 검증 항목 모두 확인

---

## 3. Cycle 2: Spec Flow — 데이터 먼저 도착

> 목적: FR-003, FR-004 검증. 데이터가 먼저 들어오고, spec이 나중에 도착하는 시나리오

### 시나리오 2-1: 데이터 ingest → pending_spec → spec 도착 → ready_for_labeling

**단계:**

1. **데이터 ingest 실행** (dispatch 또는 수동 materialize)
   - `test_smoke_001` 폴더 데이터를 ingest
   - spec 없이 ingest만 실행

2. **ingest_router 실행**
   - Dagster UI에서 `ingest_router` materialize

3. **중간 검증:**

| # | 검증 항목 | 쿼리 | 기대 결과 |
|---|-----------|------|-----------|
| 1 | ingest_status | `SELECT ingest_status, COUNT(*) FROM raw_files WHERE source_unit_name='test_smoke_001' GROUP BY 1` | `pending_spec` |
| 2 | spec_id | `SELECT spec_id FROM raw_files WHERE source_unit_name='test_smoke_001' LIMIT 1` | NULL |
| 3 | pending_ingest 집계 | `pending_ingest` materialize 후 로그 확인 | pending 건수 > 0 |

4. **spec 삽입** (labeling_spec_ingest 또는 직접 DB INSERT)

```sql
INSERT INTO labeling_specs VALUES (
  'SPEC-QA-002-001', 'qa_tester', 'qa_team', 'test_smoke_001',
  '["smoke"]', '["smoke"]', '["timestamp", "captioning"]',
  'active', 0, NULL, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
);
```

5. **ingest_router 재실행**

6. **최종 검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | ingest_status 전이 | `ready_for_labeling` |
| 2 | spec_id 세팅 | `SPEC-QA-002-001` |
| 3 | ready_for_labeling_sensor 감지 | auto_labeling_routed_job 트리거 |
| 4 | timestamp 실행 | labels 생성 |
| 5 | captioning 실행 | labels.caption_text 갱신 |

---

## 4. Cycle 3: Spec Flow — Spec 먼저 도착

> 목적: spec이 먼저 등록되고 데이터가 나중에 들어오는 시나리오

### 시나리오 3-1: spec active → 데이터 도착 → 즉시 ready_for_labeling

**단계:**

1. **spec 먼저 등록:**

```sql
INSERT INTO labeling_specs VALUES (
  'SPEC-QA-003-001', 'qa_tester', 'qa_team', 'test_fire_002',
  '["fire"]', '["fire", "flame"]', '["timestamp", "bbox"]',
  'active', 0, '_fallback', 'fallback', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
);
```

2. **데이터 ingest 실행** (test_fire_002 폴더)

3. **ingest_router 실행**

4. **검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | ingest_status | `ready_for_labeling` (pending_spec 거치지 않음) |
| 2 | spec_id | `SPEC-QA-003-001` |
| 3 | resolved_config_id | `_fallback` |
| 4 | auto_labeling_routed_job | 자동 트리거 |
| 5 | timestamp 실행 | labels 생성 |
| 6 | frame_extraction 실행 | processed_clips + image_metadata 생성 |
| 7 | bbox 실행 | image_labels 생성 |
| 8 | captioning 미실행 | captioning 미요청이므로 skipped |

---

## 5. Cycle 4: Spec Flow — 동시 도착

> 목적: spec과 데이터가 모두 준비된 상태에서 한 번에 처리

### 시나리오 4-1: spec + 데이터 동시 존재 → ingest_router 한 번으로 완료

**단계:**

1. spec 등록 + 데이터 ingest 완료 상태에서 ingest_router 실행
2. `ready_for_labeling`까지 한 번에 전이되는지 확인

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | pending_spec 단계 없이 바로 전이 | ingest_status = `ready_for_labeling` |
| 2 | Dagster run 로그 | "spec 매칭 성공" 메시지 |

---

## 6. Cycle 5: Pending → Slack 확정 → 자동 재개

> 목적: FR-004, FR-005 검증. pending spec의 Slack 확정 → sensor 자동 재개

### 시나리오 5-1: labeling_method 미정 → pending → 확정 → 처리

**단계:**

1. **미정 spec 등록:**

```sql
INSERT INTO labeling_specs VALUES (
  'SPEC-QA-005-001', 'qa_tester', 'qa_team', 'test_smoke_001',
  '[]', '[]', '[]',
  'pending', 0, NULL, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
);
```

2. **데이터 ingest + ingest_router 실행**

3. **중간 검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | ingest_status | `pending_spec` |
| 2 | spec_status | `pending` |

4. **Slack 확정 시뮬레이션** (DB 직접 UPDATE):

```sql
UPDATE labeling_specs SET
  labeling_method = '["timestamp", "captioning", "bbox"]',
  categories = '["smoke"]',
  classes = '["smoke"]',
  spec_status = 'pending_resolved',
  updated_at = CURRENT_TIMESTAMP
WHERE spec_id = 'SPEC-QA-005-001';
```

5. **spec_resolve_sensor 대기** (최대 60초)

6. **최종 검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | spec_status 전이 | `active` |
| 2 | ingest_status 전이 | `ready_for_labeling` |
| 3 | resolved_config_id | 값 세팅됨 |
| 4 | auto_labeling_routed_job | 자동 트리거 |
| 5 | timestamp 실행 | labels 생성 |
| 6 | captioning 실행 | caption_text 갱신 |
| 7 | frame_extraction 실행 | processed_clips 생성 |
| 8 | bbox 실행 | image_labels 생성 |

### 시나리오 5-2: bbox 요청인데 categories 비어있음 → pending 유지

**단계:**

1. **spec 등록 (bbox 있지만 categories 없음):**

```sql
INSERT INTO labeling_specs VALUES (
  'SPEC-QA-005-002', 'qa_tester', 'qa_team', 'test_fire_002',
  '[]', '[]', '["bbox"]',
  'pending', 0, NULL, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
);
```

2. **ingest_router 실행**

3. **검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | ingest_status | `pending_spec` (bbox인데 classes 비어있으므로) |

4. **categories 추가 후 확정:**

```sql
UPDATE labeling_specs SET
  categories = '["fire"]',
  classes = '["fire", "flame"]',
  spec_status = 'pending_resolved',
  updated_at = CURRENT_TIMESTAMP
WHERE spec_id = 'SPEC-QA-005-002';
```

5. **sensor 감지 → 자동 처리 확인**

---

## 7. Cycle 6: Config 우선순위 검증

> 목적: FR-008, FR-010 검증. personal → team → fallback 우선순위

### 시나리오 6-1: _fallback config만 존재

**단계:**

1. `config/parameters/_fallback.json` 생성 후 `config_sync` materialize
2. spec 등록 (requester_config_map 없음)
3. ingest_router 실행

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | resolved_config_id | `_fallback` |
| 2 | resolved_config_scope | `fallback` |

### 시나리오 6-2: team config 존재

**단계:**

1. team config 등록:

```sql
INSERT INTO labeling_configs VALUES ('team_qa_config', '{"timestamp":{"confidence_threshold":0.8}}', 1, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
INSERT INTO requester_config_map VALUES ('map-001', NULL, 'qa_team', 'team', 'team_qa_config', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
```

2. spec 등록 (team_id='qa_team')
3. ingest_router 실행

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | resolved_config_id | `team_qa_config` |
| 2 | resolved_config_scope | `team` |

### 시나리오 6-3: personal config 존재 (최우선)

**단계:**

1. personal config 등록:

```sql
INSERT INTO labeling_configs VALUES ('personal_qa_config', '{"timestamp":{"confidence_threshold":0.9}}', 1, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
INSERT INTO requester_config_map VALUES ('map-002', 'qa_tester', NULL, 'personal', 'personal_qa_config', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
```

2. spec 등록 (requester_id='qa_tester', team_id='qa_team')
3. ingest_router 실행

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | resolved_config_id | `personal_qa_config` (team보다 우선) |
| 2 | resolved_config_scope | `personal` |

### 시나리오 6-4: config 전혀 없음 → 실패

**단계:**

1. labeling_configs, requester_config_map 비어있는 상태
2. spec 등록 + ingest_router 실행

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | spec_status | `failed` |
| 2 | last_error | `config_not_found` |

---

## 8. Cycle 7: labeling_method 조합별 실행 검증

> 목적: FR-011~FR-014 검증. 각 method 조합에 따른 내부 stage 실행 확인

### 시나리오 7-1: `["timestamp"]` only

| 실행 stage | 기대 |
|------------|------|
| timestamp | 실행 |
| captioning | skipped |
| frame_extraction | skipped |
| bbox | skipped |

### 시나리오 7-2: `["captioning"]` only

| 실행 stage | 기대 |
|------------|------|
| timestamp | 실행 (captioning의 전제) |
| captioning | 실행 |
| frame_extraction | skipped |
| bbox | skipped |

### 시나리오 7-3: `["bbox"]` only

| 실행 stage | 기대 |
|------------|------|
| timestamp | 실행 |
| captioning | skipped |
| frame_extraction | 실행 (bbox의 전제) |
| bbox | 실행 |

### 시나리오 7-4: `["timestamp", "captioning"]`

| 실행 stage | 기대 |
|------------|------|
| timestamp | 실행 |
| captioning | 실행 |
| frame_extraction | skipped |
| bbox | skipped |

### 시나리오 7-5: `["timestamp", "bbox"]`

| 실행 stage | 기대 |
|------------|------|
| timestamp | 실행 |
| captioning | skipped |
| frame_extraction | 실행 |
| bbox | 실행 |

### 시나리오 7-6: `["captioning", "bbox"]`

| 실행 stage | 기대 |
|------------|------|
| timestamp | 실행 |
| captioning | 실행 |
| frame_extraction | 실행 |
| bbox | 실행 |

### 시나리오 7-7: `["timestamp", "captioning", "bbox"]` (전체)

| 실행 stage | 기대 |
|------------|------|
| timestamp | 실행 |
| captioning | 실행 |
| frame_extraction | 실행 |
| bbox | 실행 |

**각 시나리오 공통 검증:**

| # | 검증 항목 | 확인 방법 |
|---|-----------|-----------|
| 1 | video_metadata.timestamp_status | 실행 → completed, 미실행 → skipped |
| 2 | video_metadata.caption_status | 실행 → completed, 미실행 → skipped |
| 3 | video_metadata.frame_status | 실행 → completed, 미실행 → skipped |
| 4 | video_metadata.bbox_status | 실행 → completed, 미실행 → skipped |
| 5 | spec_status | 모든 required stage 완료 시 `completed` |

---

## 9. Cycle 8: 이벤트 0건 처리

> 목적: timestamp 결과가 0건일 때 downstream 처리 확인

### 시나리오 8-1: 이벤트 없는 영상 → timestamp completed + downstream skipped

**입력:** 이벤트가 없는 정적 영상 (예: 빈 복도 영상)

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | timestamp_status | `completed` |
| 2 | labels 생성 | 0 rows |
| 3 | caption_status | `skipped` |
| 4 | frame_status | `skipped` |
| 5 | bbox_status | `skipped` |
| 6 | spec_status | `completed` (모든 stage done/skipped) |

---

## 10. Cycle 9: 재시도 한도 초과 → failed

> 목적: retry_count >= 3 시 spec_status='failed' 전환 확인

### 시나리오 9-1: 강제 실패 3회 → failed

**단계:**

1. spec 등록 (retry_count=2로 시작)

```sql
INSERT INTO labeling_specs VALUES (
  'SPEC-QA-009-001', 'qa_tester', 'qa_team', 'test_retry_folder',
  '["smoke"]', '["smoke"]', '["timestamp"]',
  'active', 2, '_fallback', 'fallback', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
);
```

2. 해당 spec의 auto_labeling_routed_job이 실패하도록 유도 (예: 잘못된 config)
3. ready_for_labeling_sensor가 retry_count를 3으로 증가

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | retry_count | 3 |
| 2 | spec_status | `failed` |
| 3 | 이후 sensor tick | 해당 spec 무시 |

---

## 11. Cycle 10: 대량 파일 배치 처리

> 목적: 10개 이상 파일이 포함된 폴더의 일괄 처리 안정성 확인

### 시나리오 10-1: 10+ 파일 폴더 dispatch

**입력:** 10개 이상 비디오가 있는 폴더

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | raw_files 전체 적재 | 파일 수 일치 |
| 2 | video_metadata 전체 적재 | 비디오 수 일치 |
| 3 | 부분 실패 없음 | ingest_status에 failed 없음 |
| 4 | MinIO 객체 수 | raw_files 수와 일치 |
| 5 | 처리 시간 | Dagster run 소요 시간 기록 |

---

## 12. Cycle 11: Config Sync 검증

> 목적: FR-009 검증. config/parameters/ 폴더 → DB 동기화

### 시나리오 11-1: 신규 config 파일 추가

**단계:**

1. `config/parameters/test_config.json` 생성
2. Dagster UI에서 `config_sync` materialize

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | labeling_configs | config_id='test_config' row 존재 |
| 2 | version | 1 |
| 3 | is_active | true |

### 시나리오 11-2: config 파일 수정

**단계:**

1. `test_config.json` 내용 변경
2. `config_sync` 재실행

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | version | 2 (증가) |
| 2 | config_json | 변경된 내용 반영 |

### 시나리오 11-3: config 파일 삭제

**단계:**

1. `test_config.json` 삭제
2. `config_sync` 재실행

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | is_active | false (hard delete 아님) |
| 2 | config_json | 이전 내용 보존 |

### 시나리오 11-4: _fallback.json 필수 존재

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | `config/parameters/_fallback.json` | 파일 존재 |
| 2 | config_sync 후 DB | config_id='_fallback' 존재 |

---

## 13. Cycle 12: DB 스키마 정합성 검증

> 목적: 모든 테이블과 컬럼이 스펙대로 존재하는지 확인

### 시나리오 12-1: 테이블 존재 확인

```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema='main' ORDER BY table_name;
```

**기대 테이블:**

| 테이블 | 용도 |
|--------|------|
| raw_files | 원본 파일 |
| video_metadata | 비디오 메타 + stage status |
| image_metadata | 이미지/프레임 메타 |
| labels | 라벨 데이터 |
| processed_clips | 가공 클립 |
| datasets | 데이터셋 |
| dataset_clips | 데이터셋-클립 연결 |
| image_labels | 이미지 detection 라벨 |
| labeling_specs | spec 관리 |
| labeling_configs | config 관리 |
| requester_config_map | requester-config 매핑 |
| staging_dispatch_requests | dispatch 요청 |
| staging_model_configs | 모델 설정 |
| staging_pipeline_runs | 파이프라인 실행 추적 |

### 시나리오 12-2: raw_files 추가 컬럼 확인

```sql
SELECT column_name FROM information_schema.columns
WHERE table_name='raw_files' AND column_name IN ('spec_id', 'source_unit_name');
```

**기대:** 두 컬럼 모두 존재

### 시나리오 12-3: video_metadata stage 컬럼 확인

```sql
SELECT column_name FROM information_schema.columns
WHERE table_name='video_metadata'
AND column_name LIKE '%_status' OR column_name LIKE '%_error' OR column_name LIKE '%_completed_at'
ORDER BY column_name;
```

**기대 컬럼:** `timestamp_status`, `timestamp_error`, `timestamp_label_key`, `timestamp_completed_at`, `caption_status`, `caption_error`, `caption_completed_at`, `frame_status`, `frame_error`, `frame_completed_at`, `bbox_status`, `bbox_error`, `bbox_completed_at`

---

## 14. Cycle 13: MinIO 버킷 정합성 검증

> 목적: 각 단계별 MinIO 버킷에 올바른 객체가 저장되는지 확인

### 시나리오 13-1: 단계별 버킷 확인

| 단계 | 버킷 | 기대 객체 |
|------|------|-----------|
| raw_ingest | vlm-raw | 원본 미디어 파일 |
| clip_timestamp_routed | vlm-labels | event timestamp JSON |
| clip_captioning_routed | vlm-labels | caption 갱신된 JSON |
| clip_to_frame_routed | vlm-processed | clip 영상 + frame 이미지 |
| bbox_labeling | vlm-labels | detection JSON |

### 시나리오 13-2: 객체 키 형식 확인

```python
# vlm-raw 키 형식: <source_unit_name>/<filename>
# vlm-labels 키 형식: <raw_parent>/events/<video_stem>.json
# vlm-processed 키 형식: <raw_parent>/<video_stem>/<video_stem>_NNNNN.jpg
```

---

## 15. Cycle 14: Sensor 동작 검증

> 목적: 각 sensor의 정상 동작 확인

### 시나리오 14-1: dispatch_sensor

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | .dispatch/pending/ JSON 감지 | 30초 이내 |
| 2 | dispatch_stage_job 트리거 | run 생성 |
| 3 | JSON → .dispatch/processed/ 이동 | 처리 후 이동 |

### 시나리오 14-2: incoming_to_pending_sensor

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | incoming 새 폴더 감지 | 15초 이내 |
| 2 | archive_pending 이동 | 폴더 이동 완료 |

### 시나리오 14-3: spec_resolve_sensor

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | pending_resolved 감지 | 60초 이내 |
| 2 | config resolve 수행 | resolved_config_id 세팅 |
| 3 | raw_files 전이 | ready_for_labeling |
| 4 | spec_status 전이 | active |

### 시나리오 14-4: ready_for_labeling_sensor

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | ready_for_labeling 감지 | 60초 이내 |
| 2 | auto_labeling_routed_job 트리거 | run 생성 |
| 3 | run tag | spec_id, requested_outputs 포함 |
| 4 | 중복 방지 | 같은 spec에 queued/running run 있으면 skip |

### 시나리오 14-5: stuck_run_guard_sensor

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | 비활성화 상태 확인 | skipped 로그 |

---

## 16. Cycle 15: 에러 복구 시나리오

> 목적: 다양한 에러 상황에서의 복구 동작 확인

### 시나리오 15-1: 손상된 비디오 파일

**입력:** 0바이트 또는 손상된 mp4 파일

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | 해당 파일 | raw_files에 미삽입 또는 failed |
| 2 | 나머지 파일 | 정상 처리 (fail-forward) |
| 3 | 실패 로그 | JSONL 로그에 기록 |

### 시나리오 15-2: MinIO 연결 실패 시뮬레이션

**방법:** staging MinIO 일시 중지 후 ingest 실행

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | Dagster run | 에러 로그 기록 |
| 2 | MinIO 복구 후 재실행 | 정상 처리 |

### 시나리오 15-3: spec.json 파싱 실패

**입력:** 잘못된 JSON 형식의 spec

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | 해당 spec | 스킵, Dagster 로그 기록 |
| 2 | 다른 spec | 정상 처리 |

---

## 17. Cycle 16: Dagster UI Lineage 검증

> 목적: Dagster UI에서 asset lineage가 올바르게 표시되는지 확인

### 시나리오 16-1: http://172.168.42.6:3031 접속

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | Lineage 탭 | asset 그래프 정상 렌더링 |
| 2 | 주요 asset 노출 | raw_ingest, labeling_spec_ingest, config_sync, ingest_router, pending_ingest, clip_timestamp_routed, clip_captioning_routed, clip_to_frame_routed, bbox_labeling |
| 3 | 의존성 화살표 | 스펙 DAG 구조와 일치 |
| 4 | Sensor 탭 | 5개 sensor 목록 표시 |
| 5 | Run 탭 | 실행 이력 표시 |

### 시나리오 16-2: asset 수동 materialize

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | config_sync materialize | 성공 |
| 2 | labeling_spec_ingest materialize | 성공 |
| 3 | ingest_router materialize | 성공 |

---

## 18. Cycle 17: 재처리(Reprocessing) 검증

> 목적: FR-017, FR-018 검증. 같은 데이터를 다른 config로 재처리

### 시나리오 17-1: config 변경 후 재처리

**단계:**

1. 첫 번째 처리 완료 (config A)
2. config 변경 (config B)
3. 동일 데이터에 새 spec 등록 + 재처리

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | 기존 결과 | 보존 (삭제되지 않음) |
| 2 | 새 결과 | 새 labels/processed_clips row 생성 |
| 3 | spec_id | 새 spec_id로 구분 |

---

## 19. Cycle 18: 엣지 케이스 종합

> 목적: 경계 조건과 비정상 입력 처리 확인

### 시나리오 18-1: 빈 폴더 dispatch

**입력:** 비디오 파일이 없는 빈 폴더

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | dispatch 처리 | 정상 완료 (0건 처리) |
| 2 | raw_files | 0 rows |

### 시나리오 18-2: 이미지만 있는 폴더 (비디오 없음)

**입력:** jpg/png만 있는 폴더

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | raw_files | 이미지 파일 적재 |
| 2 | auto_labeling | 비디오 없으므로 대상 없음 (스펙: video only) |

### 시나리오 18-3: 동일 폴더명 중복 dispatch

**입력:** 같은 folder_name으로 dispatch 2회

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | 두 번째 dispatch | 중복 처리 정책에 따라 동작 |
| 2 | raw_files | checksum 기반 중복 검출 |

### 시나리오 18-4: unsupported labeling_method

**입력:** `labeling_method: ["image_classification"]`

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | spec 처리 | reject 또는 에러 로그 |

### 시나리오 18-5: 매우 큰 비디오 (>500MB)

**입력:** 500MB 이상 비디오 파일

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | ingest | 정상 완료 (멀티파트 업로드) |
| 2 | timestamp | preview 생성 후 처리 (Gemini 제한 회피) |
| 3 | 처리 시간 | 기록 |

### 시나리오 18-6: 특수문자/한글 폴더명

**입력:** `테스트_화재_영상_001` 같은 한글 폴더명

**검증:**

| # | 검증 항목 | 기대 결과 |
|---|-----------|-----------|
| 1 | source_unit_name | 원본 폴더명 보존 |
| 2 | MinIO raw_key | ASCII-safe 정규화 |
| 3 | archive 이동 | 정상 완료 |

---

## 20. 부록: 공통 검증 쿼리

### A. 전체 현황 요약

```sql
SELECT
  (SELECT COUNT(*) FROM raw_files) AS raw_files,
  (SELECT COUNT(*) FROM video_metadata) AS video_metadata,
  (SELECT COUNT(*) FROM image_metadata) AS image_metadata,
  (SELECT COUNT(*) FROM labels) AS labels,
  (SELECT COUNT(*) FROM processed_clips) AS processed_clips,
  (SELECT COUNT(*) FROM image_labels) AS image_labels,
  (SELECT COUNT(*) FROM labeling_specs) AS labeling_specs,
  (SELECT COUNT(*) FROM labeling_configs) AS labeling_configs,
  (SELECT COUNT(*) FROM staging_dispatch_requests) AS dispatch_requests,
  (SELECT COUNT(*) FROM staging_pipeline_runs) AS pipeline_runs;
```

### B. ingest_status 분포

```sql
SELECT ingest_status, COUNT(*) AS cnt
FROM raw_files GROUP BY 1 ORDER BY 1;
```

### C. spec_status 분포

```sql
SELECT spec_status, COUNT(*) AS cnt
FROM labeling_specs GROUP BY 1 ORDER BY 1;
```

### D. video_metadata stage 진행률

```sql
SELECT
  rf.spec_id,
  COUNT(*) AS total,
  SUM(CASE WHEN vm.timestamp_status IN ('completed','skipped') THEN 1 ELSE 0 END) AS ts_done,
  SUM(CASE WHEN vm.caption_status IN ('completed','skipped') THEN 1 ELSE 0 END) AS cap_done,
  SUM(CASE WHEN vm.frame_status IN ('completed','skipped') THEN 1 ELSE 0 END) AS frm_done,
  SUM(CASE WHEN vm.bbox_status IN ('completed','skipped') THEN 1 ELSE 0 END) AS bbox_done
FROM raw_files rf
JOIN video_metadata vm ON rf.asset_id = vm.asset_id
WHERE rf.spec_id IS NOT NULL
GROUP BY rf.spec_id;
```

### E. MinIO 버킷별 객체 수

```python
import boto3
s3 = boto3.client('s3', endpoint_url='http://172.168.47.36:9002',
                   aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
for bucket in ['vlm-raw','vlm-labels','vlm-processed','vlm-dataset']:
    objs = s3.list_objects_v2(Bucket=bucket)
    print(f"{bucket}: {objs.get('KeyCount', 0)} objects")
```

### F. dispatch 요청 이력

```sql
SELECT request_id, folder_name, run_mode, outputs, status, error_message
FROM staging_dispatch_requests ORDER BY created_at DESC;
```

### G. pipeline run 이력

```sql
SELECT run_id, request_id, folder_name, step_name, step_status, error_message
FROM staging_pipeline_runs ORDER BY created_at DESC;
```

---

## QA 실행 순서 권장

| 순서 | Cycle | 우선순위 | 사전 조건 |
|------|-------|----------|-----------|
| 1 | Cycle 12: DB 스키마 | 필수 | 초기화 완료 |
| 2 | Cycle 16: Lineage | 필수 | dagster-staging 가동 |
| 3 | Cycle 11: Config Sync | 필수 | _fallback.json 준비 |
| 4 | Cycle 1: Dispatch 기본 | 필수 | 테스트 데이터 준비 |
| 5 | Cycle 14: Sensor 동작 | 필수 | Cycle 1 완료 |
| 6 | Cycle 2: 데이터 먼저 | 높음 | 초기화 후 재시작 |
| 7 | Cycle 3: Spec 먼저 | 높음 | 초기화 후 재시작 |
| 8 | Cycle 4: 동시 도착 | 높음 | 초기화 후 재시작 |
| 9 | Cycle 5: Pending 확정 | 높음 | 초기화 후 재시작 |
| 10 | Cycle 6: Config 우선순위 | 중간 | Config Sync 완료 |
| 11 | Cycle 7: Method 조합 | 중간 | 기본 흐름 검증 완료 |
| 12 | Cycle 8: 이벤트 0건 | 중간 | 기본 흐름 검증 완료 |
| 13 | Cycle 9: 재시도 한도 | 중간 | Sensor 동작 확인 |
| 14 | Cycle 10: 대량 배치 | 중간 | 기본 흐름 검증 완료 |
| 15 | Cycle 13: MinIO 정합 | 중간 | 기본 흐름 검증 완료 |
| 16 | Cycle 15: 에러 복구 | 낮음 | 기본 흐름 검증 완료 |
| 17 | Cycle 17: 재처리 | 낮음 | 전체 흐름 검증 완료 |
| 18 | Cycle 18: 엣지 케이스 | 낮음 | 전체 흐름 검증 완료 |

---

## 각 Cycle 완료 후 체크

- [ ] 검증 쿼리 결과 기록
- [ ] 실패 항목 이슈 등록
- [ ] Dagster run URL 기록
- [ ] 소요 시간 기록
- [ ] staging 환경 초기화 (다음 Cycle 독립 실행 필요 시)
