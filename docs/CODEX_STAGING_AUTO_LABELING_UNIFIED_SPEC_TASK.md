# Codex 구현 할당: Staging 전용 Auto Labeling 통합 명세 적용

> 기준 문서: `auto_labeling_unified_spec.md`  
> 범위: **staging 관련 코드만** 수정. production 경로는 건드리지 않는다.  
> 테스트 통과 후 운영 코드에 적용 예정이므로, 스키마/리소스는 공용으로 추가하되 **새 asset·sensor·job은 IS_STAGING일 때만 등록**한다.

---

## 1. 목표

- `auto_labeling_unified_spec.md`의 데이터 모델·상태 전이·Dagster 구조를 **staging 환경에서만** 동작하도록 구현한다.
- 기존 staging 플로우(dispatch_sensor, dispatch_stage_job)는 유지하고, **spec 기반 플로우**를 추가한다.
- 새 테이블·컬럼은 스키마/duckdb_base에 추가하고, spec 관련 asset/sensor/job은 `definitions.py`에서 `if IS_STAGING:` 블록 안에서만 등록한다.

---

## 2. 수정 대상 파일 및 내용

### 2-1. 스키마·마이그레이션 (공용, backward compatible)

**`src/vlm_pipeline/sql/schema.sql`**

- 다음 테이블 추가 (기존 테이블 뒤에):
  - **labeling_specs**: spec_id PK, requester_id, team_id, source_unit_name, categories JSON, classes JSON, labeling_method JSON, spec_status VARCHAR, retry_count INT, resolved_config_id, resolved_config_scope, last_error TEXT, created_at, updated_at
  - **labeling_configs**: config_id PK, config_json JSON, version INT, is_active BOOLEAN, created_at, updated_at
  - **requester_config_map**: map_id PK, requester_id VARCHAR NULL, team_id VARCHAR NULL, scope VARCHAR, config_id VARCHAR, is_active BOOLEAN, created_at, updated_at
- **raw_files**에 컬럼 추가: `spec_id VARCHAR`, `source_unit_name VARCHAR`
- **raw_files.ingest_status** 허용값 문서화: pending, uploading, completed, failed, skipped, **pending_spec**, **ready_for_labeling**
- **video_metadata**에 stage status 컬럼 추가 (기존 auto_label_* 는 유지, 아래를 canonical으로 추가):
  - timestamp_status, timestamp_error, timestamp_label_key, timestamp_completed_at
  - caption_status, caption_error, caption_completed_at
  - frame_status, frame_error, frame_completed_at
  - bbox_status, bbox_error, bbox_completed_at

**`src/vlm_pipeline/resources/duckdb_base.py`**

- `ensure_schema()` 또는 테이블 생성 로직에서 위 세 테이블(labeling_specs, labeling_configs, requester_config_map) 생성 보장.
- raw_files에 spec_id, source_unit_name 컬럼 없으면 ALTER TABLE로 추가.
- video_metadata에 timestamp_status, caption_status, frame_status, bbox_status 등 위 컬럼들 없으면 ALTER TABLE로 추가.

(기존 migration.sql이 있으면 거기에 추가하거나, ensure_* 메서드로만 처리해도 됨.)

---

### 2-2. DuckDB 리소스

**`src/vlm_pipeline/resources/duckdb_ingest.py`**

- `insert_raw_files_batch`: 레코드에 `source_unit_name`, `spec_id` 키가 있으면 INSERT 컬럼/값에 포함. (raw_files 테이블에 해당 컬럼이 있을 때만)

**`src/vlm_pipeline/resources/duckdb.py`** (또는 신규 duckdb_spec.py)

- spec/config CRUD 메서드 추가 (staging spec 플로우용):
  - upsert_labeling_spec(spec_id, requester_id, team_id, source_unit_name, categories, classes, labeling_method, spec_status, ...)
  - get_labeling_spec_by_id(spec_id), list_specs_by_status(spec_status)
  - resolve_config_for_requester(requester_id, team_id) -> (config_id, scope)  # personal → team → _fallback
  - update_spec_resolved_config(spec_id, resolved_config_id, resolved_config_scope), update_spec_status(spec_id, spec_status, last_error=None)
  - labeling_configs: get_config(config_id), list_active_configs()
  - requester_config_map: get_active_map(requester_id, team_id) 등

**`src/vlm_pipeline/resources/duckdb_labeling.py`** (기존 라벨/비디오 메타 조회)

- video_metadata 조회 시 새 stage 컬럼(timestamp_status, caption_status, frame_status, bbox_status) 사용할 수 있도록 쿼리 확장 (필요 시).
- spec_id 기준으로 ready_for_labeling 인 raw_files 목록 조회 등 (ready_for_labeling_sensor용).

---

### 2-3. Ingest

**`src/vlm_pipeline/defs/ingest/ops.py`**

- manifest에서 `source_unit_name`을 읽어서, raw_files에 넣는 레코드에 `source_unit_name` 필드 포함. (이미 있으면 유지, 없으면 추가)

**`src/vlm_pipeline/defs/ingest/assets.py`**

- `raw_ingest` asset에서 register_incoming / normalize_and_archive 등으로 만들어진 레코드에 manifest의 `source_unit_name`이 들어가도록 전달. (ops에서 받은 manifest 정보를 raw_files insert 시 포함)

---

### 2-4. 신규 모듈 (Staging 전용 — IS_STAGING일 때만 등록)

**`src/vlm_pipeline/defs/spec/assets.py`** (신규 파일)

- **labeling_spec_ingest**: 외부 spec 수신 → labeling_specs upsert. labeling_method=[] 이면 spec_status='pending', 유효 method 있으면 'active'. categories → classes 파생 규칙 적용 (smoke→["smoke"], fire→["fire","flame"] 등).
- **config_sync**: config/parameters/*.json 을 labeling_configs와 동기화. _fallback.json 필수. Dagster UI 수동 materialize용.
- **ingest_router**: raw_ingest 완료 후 실행. raw_files.ingest_status='completed' 인 row + labeling_specs 기준으로 source_unit_name 매칭, spec 없음/method 미정/bbox인데 classes 비어 있음 → pending_spec, 아니면 config resolve 후 ready_for_labeling, spec_id·resolved_config_id·resolved_config_scope 저장.
- **pending_ingest**: ingest_status='pending_spec' 집계 asset, pending spec 수·pending file 수 출력.

**`src/vlm_pipeline/defs/spec/sensor.py`** (신규 파일)

- **spec_resolve_sensor**: 60초 주기. labeling_specs.spec_status='pending_resolved' 대상으로 config resolve 후 matching raw_files를 ready_for_labeling으로 전이, spec_status='active' 전이.
- **ready_for_labeling_sensor**: 60초 주기. raw_files.ingest_status='ready_for_labeling' + spec_id 기준 그룹으로 auto_labeling_routed_job 실행, run tag에 spec_id, source_unit_name, resolved_config_id, requested_outputs 전달. 동일 spec에 대해 queued/running run 있으면 스킵. retry_count >= 3 이면 spec_status='failed'.

---

### 2-5. 라우팅된 Auto Labeling (Staging 전용)

**`src/vlm_pipeline/defs/label/assets.py`** (기존)

- **clip_timestamp_routed** (신규 asset): ingest_status='ready_for_labeling' 이고 spec의 내부 실행 계획에 'timestamp' 포함인 경우만 실행. resolved_config_id로 config 로드, 이벤트 구간 추출, labels·vlm-labels 저장, video_metadata.timestamp_status 갱신. run tag에서 requested_outputs/spec_id 읽기.
- 기존 clip_timestamp는 그대로 두고, staging 정의에서만 clip_timestamp_routed 사용하거나, clip_timestamp가 context.run.tags에 spec_id/requested_outputs 있으면 라우팅 로직 타도록 분기 가능. (명세상 clip_timestamp_routed 로 신규 asset 추가 권장)

**`src/vlm_pipeline/defs/process/assets.py`**

- **clip_captioning_routed**, **clip_to_frame_routed**: requested_outputs에 captioning/bbox 포함 시 실행, timestamp_status 등 stage 상태 의존. (기존 clip_captioning, clip_to_frame와 유사하되 spec_id·resolved_config_id 기반으로 백로그 조회)

**`src/vlm_pipeline/defs/yolo/assets.py`**

- **bbox_labeling**: requested_outputs에 bbox 포함, frame_status='completed' 대상. spec.classes와 config.bbox.target_classes 교집합으로 target_classes 적용.

**`src/vlm_pipeline/lib/env_utils.py`**

- **labeling_method** 규칙: 사용자 요청은 timestamp, captioning, bbox 세 가지. frame_extraction은 bbox 요청 시 내부 stage로만 추가. resolve_outputs 확장 시 bbox → frame_extraction 자동 포함 반영.

---

### 2-6. Definitions (Staging 분기)

**`src/vlm_pipeline/definitions.py`**

- `if IS_STAGING:` 블록에서:
  - spec assets 추가: labeling_spec_ingest, config_sync, ingest_router, pending_ingest
  - spec sensors 추가: spec_resolve_sensor, ready_for_labeling_sensor
  - job 추가: auto_labeling_routed_job (clip_timestamp_routed, clip_captioning_routed, clip_to_frame_routed, bbox_labeling 선택)
- 기존 dispatch_sensor, dispatch_stage_job, incoming_to_pending_sensor는 그대로 유지.

---

## 3. 구현 시 유의사항

- **labeling_method** 저장 형식: 문자열이 아닌 **JSON array** (`["timestamp","captioning","bbox"]`). 미정은 `[]`.
- **frame_extraction**은 사용자 출력이 아니라 bbox 요청 시 내부 stage로만 추가.
- **classes**는 categories에서 파생 (외부에서 classes 직접 받지 않음). category → classes 매핑: smoke→["smoke"], fire→["fire","flame"], falldown→["person_fallen"], weapon→["knife","gun","weapon"], violence→["violence","fight"].
- config resolve 우선순위: personal → team → _fallback. 없으면 실패, spec_status='failed', last_error='config_not_found'.
- video_metadata stage 초기값: 아직 실행 전이면 pending, 요청되지 않은 stage는 job 시작 시 skipped.

---

## 4. 검수 체크리스트 (Cursor 측 검수용)

- [ ] schema.sql에 labeling_specs, labeling_configs, requester_config_map 테이블 및 raw_files/video_metadata 컬럼 추가 반영
- [ ] duckdb_base.py에서 위 테이블/컬럼 ensure 로직
- [ ] insert_raw_files_batch에 source_unit_name(, spec_id) 반영
- [ ] ingest ops/assets에서 manifest.source_unit_name → raw_files 저장
- [ ] defs/spec/assets.py, defs/spec/sensor.py 신규 생성 및 내용 일치
- [ ] clip_timestamp_routed 등 라우팅 asset이 run tag(spec_id, requested_outputs, resolved_config_id) 기반으로 동작
- [ ] definitions.py에서 IS_STAGING일 때만 새 assets/sensors/job 등록
- [ ] 기존 staging (dispatch_sensor, dispatch_stage_job) 동작 유지

---

## 5. 테스트

- 단위: labeling_method 정규화, categories→classes 파생, config resolve 우선순위.
- 통합: staging DB로 raw_ingest → ingest_router → ready_for_labeling_sensor → auto_labeling_routed_job 한 사이클 (가능하면).

완료 후 위 검수 체크리스트로 Cursor가 검수한다.
