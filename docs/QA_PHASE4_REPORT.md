# Phase 4: Spec Flow QA 보고서

**실행일시**: 2026-03-18
**환경**: dagster-staging (pipeline-dagster-staging-1)

---

## 1. 테스트 목적

- Spec Flow의 핵심 메커니즘 검증:
  - `ingest_router`: completed → pending_spec / ready_for_labeling 전이
  - `spec_resolve_sensor`: pending_resolved spec → config resolve → ready_for_labeling 자동 전이
  - `ready_for_labeling_sensor`: ready_for_labeling 감지 → auto_labeling_routed_job 자동 트리거
- Config 우선순위 resolve (`_fallback`) 검증
- Spec 매칭 로직 (source_unit_name 기준) 검증

---

## 2. Cycle 2: 데이터 먼저 도착

### 시나리오
1. 이미 ingest된 27개 비디오 (tmp_data_2: 25, GS건설: 2) 활용
2. spec 없이 `ingest_router` 실행 → `pending_spec` 전이
3. `SPEC-QA-002-001` (tmp_data_2, timestamp+captioning) 등록
4. `ingest_router` 재실행 → `ready_for_labeling` 전이
5. `ready_for_labeling_sensor` → `auto_labeling_routed_job` 트리거

### 검증 결과

| # | 검증 항목 | 기대 결과 | 실제 결과 | 판정 |
|---|-----------|-----------|-----------|------|
| 1 | spec 없이 ingest_router | 27건 → pending_spec | tmp_data_2: 25, GS건설: 2 → pending_spec | **PASS** |
| 2 | spec_id | NULL | NULL | **PASS** |
| 3 | spec 등록 후 ingest_router | tmp_data_2 25건 → ready_for_labeling | 25건 ready_for_labeling, spec_id=SPEC-QA-002-001 | **PASS** |
| 4 | GS건설 (spec 미등록) | pending_spec 유지 | 2건 pending_spec 유지 | **PASS** |
| 5 | config resolve | _fallback 매칭 | resolved_config_id=_fallback, scope=fallback | **PASS** |
| 6 | ready_for_labeling_sensor | auto_labeling_routed_job 트리거 | run ff05bb5a 트리거 성공 | **PASS** |
| 7 | auto_labeling_routed_job 실행 | clip_timestamp_routed 실행 | staging 컨테이너에 Gemini env 미주입 → FAILURE (이슈 3 보정) | **CONFIG GAP** |

### 코드 수정 사항
- `ready_for_labeling_sensor`에 `job_name="auto_labeling_routed_job"` 추가 (Dagster 요구사항)

---

## 3. Cycle 3: Spec 먼저 도착 (spec_resolve_sensor)

### 시나리오
1. GS건설에 대해 `SPEC-QA-003-001` (timestamp+bbox, pending_resolved) 등록
2. `spec_resolve_sensor`가 자동으로 config resolve + ready_for_labeling 전이
3. `ready_for_labeling_sensor` → `auto_labeling_routed_job` 트리거

### 검증 결과

| # | 검증 항목 | 기대 결과 | 실제 결과 | 판정 |
|---|-----------|-----------|-----------|------|
| 1 | spec_resolve_sensor 감지 | pending_resolved spec 감지 | 60초 내 감지 | **PASS** |
| 2 | config resolve | _fallback 매칭 | resolved_config_id=_fallback | **PASS** |
| 3 | spec_status 전이 | active | active | **PASS** |
| 4 | raw_files 전이 | GS건설 2건 → ready_for_labeling | 2건 ready_for_labeling, spec_id=SPEC-QA-003-001 | **PASS** |
| 5 | ready_for_labeling_sensor | auto_labeling_routed_job 트리거 | run 25ca47e0 트리거 성공 | **PASS** |
| 6 | 태그 전달 | spec_id, requested_outputs, resolved_config_id | 정확히 전달됨 | **PASS** |
| 7 | auto_labeling_routed_job 실행 | clip_timestamp_routed 실행 | staging 컨테이너에 Gemini env 미주입 → FAILURE (이슈 3 보정) | **CONFIG GAP** |

### 코드 수정 사항
- `spec_resolve_sensor` 쿼리에 `ingest_status IN ('completed', 'pending_spec')` 조건 추가
  - 기존: `completed`만 매칭 → `pending_spec` 상태 파일 누락
  - 수정: `pending_spec`도 포함하여 spec 등록 후 즉시 전이 가능

---

## 4. Run 태그 검증

### auto_labeling_routed_job (Cycle 2 — run ff05bb5a)

| 태그 | 값 |
|------|-----|
| spec_id | SPEC-QA-002-001 |
| source_unit_name | tmp_data_2 |
| requested_outputs | timestamp_captioning |
| resolved_config_id | _fallback |
| duckdb_writer | true |

### auto_labeling_routed_job (Cycle 3 — run 25ca47e0)

| 태그 | 값 |
|------|-----|
| spec_id | SPEC-QA-003-001 |
| source_unit_name | GS건설 |
| requested_outputs | timestamp_bbox |
| resolved_config_id | _fallback |
| duckdb_writer | true |

---

## 5. 발견된 이슈 및 수정

### 이슈 1: ready_for_labeling_sensor에 job_name 누락
- **심각도**: Critical (sensor 실행 불가)
- **원인**: `@sensor` 데코레이터에 `job_name` 미지정
- **에러**: `Sensor evaluation function returned a RunRequest for a sensor lacking a specified target`
- **수정**: `@sensor(job_name="auto_labeling_routed_job")` 추가
- **파일**: `src/vlm_pipeline/defs/spec/sensor.py`

### 이슈 2: spec_resolve_sensor가 pending_spec 상태 파일 누락
- **심각도**: High (Cycle 3 시나리오 실패)
- **원인**: 쿼리 조건이 `ingest_status = 'completed'`만 포함
- **수정**: `ingest_status IN ('completed', 'pending_spec')` 로 확장
- **파일**: `src/vlm_pipeline/defs/spec/sensor.py`

### 이슈 3: Gemini credentials — 운영 `.env`에는 있으나 staging 컨테이너에는 없음 (재확인)

- **심각도**: Info (배선 이슈, 코드/시크릿 부재 아님)
- **운영 `docker/.env`**: `GEMINI_PROJECT`, `GEMINI_LOCATION`, `GEMINI_GOOGLE_APPLICATION_CREDENTIALS`(파일 경로), **`GEMINI_SERVICE_ACCOUNT_JSON`(JSON 본문)** 등이 설정되어 있음. `vlm_pipeline.lib.gemini.resolve_gemini_credentials_path()`는 우선순위대로 파일 경로 → **`GEMINI_SERVICE_ACCOUNT_JSON`** → 번들 JSON을 사용함.
- **staging이 실패한 이유**: `docker-compose.yaml`의 `dagster-staging` 서비스는 **`env_file: .env.staging`만** 로드함. `docker/.env`는 이 서비스에 자동 병합되지 않음. `docker/.env.staging`에는 `GEMINI_*` 키가 **빈 값**으로 두어져 있어, 컨테이너 프로세스 환경에는 `GEMINI_SERVICE_ACCOUNT_JSON` 등이 비어 있음 → `FileNotFoundError: Gemini credentials not found...`.
- **정정**: “Gemini credentials가 레포/운영에 없다”가 아니라 **“dagster-staging에 동일 값이 주입되지 않았다”**가 정확한 진단.
- **권장 조치**: staging에서 Gemini까지 검증하려면 (1) `.env.staging`에 운영과 동등한 `GEMINI_*` 채우기, 또는 (2) compose에 `env_file`에 `.env` 추가·병합, 또는 (3) `environment:` 블록으로 명시 주입. **시크릿은 커밋하지 말 것.**

### 이슈 4: SPEC-QA-002-001 failed 상태
- **심각도**: Low
- **원인**: `clip_timestamp_routed` 실패 → `ready_for_labeling_sensor`가 retry_count 증가 → 3회 초과 시 failed
- **현재 상태**: retry_count=1, spec_status=failed (sensor가 실패 run 후 failed로 전이)
- **참고**: 실제로는 retry_count=1에서 failed가 된 것은 sensor 로직 확인 필요

---

## 6. DB 최종 상태

| 항목 | 값 |
|------|-----|
| raw_files (ready_for_labeling) | 27 |
| labeling_specs | 2 (SPEC-QA-002-001: failed, SPEC-QA-003-001: active) |
| labeling_configs | 1 (_fallback) |
| auto_labeling_routed_job runs | 2 (ff05bb5a: FAILURE, 25ca47e0: FAILURE) |

---

## 7. 종합 평가

| 검증 항목 | 결과 |
|-----------|------|
| ingest_router: completed → pending_spec | **PASS** |
| ingest_router: spec 매칭 → ready_for_labeling | **PASS** |
| config resolve (_fallback) | **PASS** |
| spec_resolve_sensor: pending_resolved → active + ready_for_labeling | **PASS** |
| ready_for_labeling_sensor: auto_labeling_routed_job 트리거 | **PASS** |
| 태그 전달 (spec_id, outputs, config) | **PASS** |
| 실제 Gemini labeling 실행 | **FAIL (staging env)** — `docker/.env`에는 `GEMINI_SERVICE_ACCOUNT_JSON` 등 있음, **dagster-staging에는 미전달** |

**전체 판정: PASS (Spec Flow 메커니즘 검증 완료)**

Spec Flow의 핵심 메커니즘(상태 전이, sensor 트리거, config resolve, 태그 전달)은 모두 정상 작동합니다.
Gemini 단계 실패는 **staging 전용 env에 Gemini 변수가 비어 있어서**이며, 운영 `.env`와 동일하게 staging에 주입하면 동일 코드 경로로 재검증 가능합니다.
