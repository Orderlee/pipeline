# Auto Labeling 명세 대비 현행 로직 갭 및 수정 사항

**기준 문서:**  
- `docs/logic/Auto_Labeling_기능_명세서.md`  
- `docs/logic/Auto_Labeling_요구사항_명세서.md`  
- `docs/logic/Auto_Labeling_화면_명세서.md`  

**대상 코드:** `src/vlm_pipeline/` (spec, label, process, yolo, definitions_staging)

---

## 1. 요약

| 구분 | 명세 요구 | 현행 구현 | 조치 |
|------|-----------|-----------|------|
| Config 조회 시점 | ingest_router·spec_resolve_sensor는 config 조회 안 함. **3-5(clip_timestamp_routed) 시작 시에만** 조회 | ingest_router·spec_resolve_sensor에서 `resolve_config_for_requester`·`update_spec_resolved_config` 호출 | **수정** |
| spec_status = 'active' | job **성공 시 job 내부**에서 갱신 | spec_resolve_sensor에서 즉시 `update_spec_status(spec_id, "active")` | **수정** |
| spec_resolve_sensor retry_count | 매칭 시 `retry_count += 1`, retry_count ≥ 3이면 `spec_status = 'failed'` | spec_resolve_sensor에서 retry_count 미증가, 실패 시 failed 전환 없음 | **수정** |
| spec_resolve_sensor 트리거 | pending_resolved 감지 시 **RunRequest로 auto_labeling_routed_job 직접 트리거** | 상태만 갱신, 트리거는 ready_for_labeling_sensor가 담당 | **검토** (2개 센서 유지 시 명세와 역할 정리 필요) |
| captioning 생략 금지 | 표준 파이프라인에서 **captioning 단계는 생략하지 않음** | requested_outputs에 captioning 없으면 clip_captioning_routed 스킵 | **수정** |
| 3-5 config 로드 | clip_timestamp_routed **시작 시** spec_id → labeling_specs → requester_config_map/labeling_configs로 config_json 결정 | run tag의 resolved_config_id 사용, asset 내부에서 config DB 조회 없음 | **수정** |
| labeling_spec_ingest | spec.json 수신·파싱 → labeling_specs INSERT | 수동 materialize 시 빈 결과만 반환, 실제 수신/INSERT 없음 | **수정** (외부 연동 시) |
| config_sync | 삭제된 파일 → is_active=false, 변경 시 version+1 | 모든 파일 upsert 시 version=1 고정, 삭제 처리 없음 | **수정** |
| clip_captioning_routed / clip_to_frame_routed / bbox_labeling | spec_id·config 기반 백로그 처리 및 config 파라미터 반영 | TODO 상태, 실제 처리 없음 | **수정** |

---

## 2. 데이터 흐름·라우터

### 2.1. ingest_router에서 config 조회 제거 (기능 명세 3-3)

**명세:**  
> 라우터는 **pending vs ready 분기만** 수행. requester_config_map / labeling_configs **조회는 하지 않는다**. config는 **auto_labeling 첫 task(3-5)** 시작 시 조회.

**현행:**  
`defs/spec/assets.py`의 `ingest_router()`에서  
- `db.resolve_config_for_requester(spec.get("requester_id"), spec.get("team_id"))`  
- `db.update_spec_resolved_config(spec["spec_id"], config_id, scope or "fallback")`  
를 호출하여 **라우터 단계에서 config를 결정·저장**하고 있음.

**수정 방향:**  
- ingest_router에서는 `labeling_method`가 비어 있는지·1개 이상인지만 보고 `pending_spec` / `ready_for_labeling`만 설정.  
- `resolve_config_for_requester`, `update_spec_resolved_config` 호출 제거.  
- spec 매칭 실패·method 없음·bbox인데 classes 없음 등은 명세대로 pending_spec으로만 보내고, config 없음으로 인한 실패는 3-5에서만 발생하도록 함.

---

### 2.2. spec_resolve_sensor에서 config 조회 제거 및 retry/active 정리 (기능 명세 4)

**명세:**  
- config 조회는 하지 않음 (3-5에서 수행).  
- 매칭 시: `retry_count += 1`, 관련 raw_files → `ready_for_labeling`, **spec_status는 변경하지 않음(pending_resolved 유지)**.  
- job **성공 시**: job **내부**에서 `spec_status → 'active'` 갱신.  
- job 실패 시: `pending_resolved` 유지, **retry_count ≥ 3이면** `spec_status → 'failed'` + 경고 로그.  
- 트리거: **auto_labeling_routed_job** RunRequest.

**현행:**  
`defs/spec/sensor.py`의 `spec_resolve_sensor()`에서  
- `resolve_config_for_requester`, `update_spec_resolved_config` 호출 후 `update_spec_status(spec_id, "active")` 호출.  
- `retry_count` 증가 없음.  
- RunRequest 반환 없음 (트리거는 `ready_for_labeling_sensor`가 담당).

**수정 방향:**  
1. spec_resolve_sensor에서 config 관련 호출 전부 제거.  
2. pending_resolved 처리 시 `retry_count += 1` (또는 명세상 “매칭 시”에 맞춰 1회만 증가).  
3. `spec_status`는 sensor에서 바꾸지 않고 `pending_resolved` 유지.  
4. `retry_count >= 3`이면 해당 spec은 `spec_status = 'failed'`로 전환하고 RunRequest 하지 않음.  
5. **트리거**: 명세대로라면 spec_resolve_sensor가 곧바로 `RunRequest(job_name="auto_labeling_routed_job", ...)` 반환.  
   - 현행처럼 ready_for_labeling_sensor가 트리거하는 구조를 유지할 경우, “트리거는 ready_for_labeling_sensor, spec_resolve_sensor는 상태 전이만”으로 명세와의 차이를 문서에 명시하고, 필요 시 명세를 “두 단계 센서”로 수정 검토.  
6. `spec_status = 'active'` 설정은 **auto_labeling_routed_job이 성공 완료될 때** job 내부(마지막 step 또는 전용 op)에서 수행하도록 이전.  
7. job 실패 시 `retry_count >= 3`이면 `spec_status = 'failed'` 전환은 job 실패 핸들러 또는 다음 sensor tick에서 처리.

---

## 3. Config 조회 시점 (3-5)

### 3.1. clip_timestamp_routed 시작 시 config 조회 (기능 명세 3-5)

**명세:**  
> 본 asset **시작 시** 대상 raw_files의 spec_id로 labeling_specs에서 requester_id, team_id를 읽고,  
> 1) requester_config_map (requester_id + personal), 2) team + team, 3) config_id = "_fallback", 4) 없으면 run 실패.  
> 동일 run 내 3-6~3-8은 **동일 config_json** 사용.

**현행:**  
- `clip_timestamp_routed`는 run tag의 `resolved_config_id` 등을 사용하며, asset 내부에서 labeling_specs·requester_config_map·labeling_configs를 읽지 않음.  
- config는 현재 ingest_router/spec_resolve_sensor에서 미리 resolve되어 tag로 전달되는 구조.

**수정 방향:**  
- clip_timestamp_routed **시작 시** (또는 run 초입 공통 단계에서):  
  - spec_id(tag 또는 백로그 조회로 확보) → labeling_specs에서 requester_id, team_id 조회.  
  - `resolve_config_for_requester(requester_id, team_id)`로 config_id, scope 결정.  
  - labeling_configs에서 config_json 로드, 없으면 run 실패.  
- 결정된 config_id/config_json을 run tag 또는 run_config로 후속 asset(clip_captioning_routed, clip_to_frame_routed, bbox_labeling)에 전달하여 “동일 run 내 동일 config” 보장.  
- ingest_router/spec_resolve_sensor에서 config resolve를 제거한 뒤, 3-5에서만 위와 같이 조회하도록 통일.

---

## 4. 표준 실행 순서 및 captioning 생략 금지

### 4.1. timestamp → captioning → frame → bbox, captioning 항상 실행 (요구사항 FR-002, 기능 명세 1·3-6)

**명세:**  
> 표준 auto_labeling은 **timestamp → captioning → frame → bbox** 순으로 순차 실행되며, **captioning 단계는 생략하지 않는다**.

**현행:**  
- `clip_captioning_routed`: `"captioning" not in requested`이면 스킵.  
- requested_outputs에 captioning이 없으면 captioning이 아예 실행되지 않음.

**수정 방향:**  
- “표준” 실행(또는 spec에 labeling_method가 있는 경우)에서는 **requested_outputs와 무관하게 captioning 단계를 항상 실행**하도록 정책 반영.  
  - 예: `requested_outputs`에 captioning이 없어도, 표준 파이프라인일 때는 clip_captioning_routed를 실행하고, 내부에서 “caption만 채우고 bbox는 스킵” 등으로 처리.  
- 또는 labeling_method에 captioning이 포함된 경우에는 requested_outputs와 상관없이 captioning 단계를 수행하도록 조건을 명확히 정의.

---

## 5. Asset별 구현 상태

### 5.1. labeling_spec_ingest (기능 명세 3-1)

**명세:**  
- 외부 API/Slackbot 등 → spec.json 수신 → 파싱(spec_id, requester_id, team_id, source_unit_name, categories, classes, labeling_method) → labeling_specs INSERT.  
- labeling_method가 []/null이면 spec_status = "pending", 1개 이상이면 "active".

**현행:**  
- 수동 materialize 시 로그만 남기고 `{"specs_upserted": 0}` 반환. 실제 spec 수신·파싱·INSERT 없음.

**수정 방향:**  
- Flex/Slack/API 등에서 spec.json이 들어오는 경로가 정해지면, 해당 페이로드를 받아 파싱 후 `db.upsert_labeling_spec(...)` 호출.  
- categories → classes 자동 파생(기능 명세·화면 명세의 테이블) 적용.  
- spec_status는 labeling_method 유무에 따라 pending/active 설정.

---

### 5.2. config_sync (기능 명세 3-2)

**명세:**  
- 신규 파일 → INSERT, **내용 변경 → UPDATE (version + 1)**, **파일 삭제 → is_active = false** (DB row 삭제 안 함).

**현행:**  
- `config_sync`: 모든 발견된 JSON에 대해 `upsert_labeling_config(..., version=1, is_active=True)`.  
- version 증분 없음, 삭제된 파일에 대한 is_active=false 처리 없음.

**수정 방향:**  
- 기존 DB에 있던 config_id 목록과 현재 디렉터리 파일 목록 비교.  
  - 디렉터리에 없으면 해당 config_id는 is_active=false로 UPDATE.  
- 파일 내용이 기존과 다르면 version을 +1하여 UPDATE.  
- 신규 파일은 INSERT (version=1, is_active=true).

---

### 5.3. clip_captioning_routed (기능 명세 3-6)

**명세:**  
- labels (label_status=completed, caption_text IS NULL), raw_files, **3-5와 동일 규칙의 config_json captioning 섹션** 사용.  
- model, language, max_caption_length 등 파라미터 로드 후 캡션 생성, labels.caption_text UPDATE, MinIO 갱신.

**현행:**  
- “spec_id 기준 timestamp_status=completed 백로그 조회 후 기존 clip_captioning 로직 재사용”이 TODO.  
- 실제 처리 없이 스킵 시 0건 반환.

**수정 방향:**  
- spec_id(tag 또는 상위에서 전달) + timestamp 완료 백로그 조회 구현.  
- 3-5에서 확정한 config_json의 captioning 섹션을 사용해 파라미터 로드.  
- 기존 clip_captioning과 동일한 캡션 생성·DB·MinIO 갱신 로직을 spec/config 기반으로 재사용.

---

### 5.4. clip_to_frame_routed (기능 명세 3-7)

**명세:**  
- caption 완료된 이벤트 구간, 3-5와 동일 config_json의 frame_extraction 섹션 사용.  
- frame_interval_sec, max_frames_per_clip, output_format, output_quality 등 적용.

**현행:**  
- “spec_id 기준 timestamp_status=completed 백로그 조회 후 clip 생성 + frame 추출, frame_status 갱신” TODO.  
- bbox not in requested 시 스킵, 실제 처리 없음.

**수정 방향:**  
- spec_id + caption 완료 백로그 조회.  
- config_json.frame_extraction 파라미터 로드.  
- clip 생성·frame 추출·processed_clips/image_metadata INSERT·MinIO 업로드 구현 (기존 clip_to_frame 로직 재사용·조정).

---

### 5.5. bbox_labeling (기능 명세 3-8)

**명세:**  
- image_metadata(프레임 추출 완료), labeling_specs.classes, 3-5와 동일 config_json의 bbox 섹션.  
- target_classes = intersection(spec.classes, config.target_classes) (정의된 경우).  
- detection_model, confidence_threshold, nms_threshold 등 적용.

**현행:**  
- “spec_id·frame_status=completed 기준 백로그 조회, target_classes = intersection(spec.classes, config.bbox.target_classes)” TODO.  
- 실제 처리 없음.

**수정 방향:**  
- spec_id + frame 추출 완료 이미지 백로그 조회.  
- spec.classes와 config.bbox.target_classes 교집합으로 target_classes 확정.  
- config.bbox 파라미터로 YOLO 등 객체 감지 후 labels INSERT/UPDATE, MinIO 업로드.

---

## 6. DB·스키마

### 6.1. requester_config_map PK (기능 명세 6-1)

**명세:**  
- (requester_id, config_id, scope)가 PK.

**현행:**  
- schema.sql에 `map_id VARCHAR PRIMARY KEY` 등으로 되어 있어, 명세와 PK 구조가 다름.

**수정 방향:**  
- 명세대로 (requester_id, config_id, scope)를 복합 PK로 두거나, 기존 map_id를 유지하되 명세서에 “실제 구현은 map_id PK”라고 반영.  
- 조회 로직은 이미 requester_id/team_id 기준이므로, PK 변경 시 기존 데이터 마이그레이션만 고려하면 됨.

---

### 6.2. raw_files 확장

**명세:**  
- raw_files에 spec_id, source_unit_name 추가.  
- ingest_status에 pending_spec, ready_for_labeling 추가.

**현행:**  
- duckdb_base에서 spec_id, source_unit_name ALTER로 추가.  
- ingest_status 값은 코드에서 pending_spec, ready_for_labeling 사용.  
- schema.sql에는 Strict 8 테이블 정의 후 spec 전용 주석으로 안내.

**수정 방향:**  
- 유지. 필요 시 schema.sql에 raw_files 확장 컬럼·ingest_status 값 목록을 주석으로 명시.

---

## 7. Slack 봇·인터페이스

**명세 (화면 명세 3-4~3-6):**  
- /labeling-confirm, /labeling-pending, /labeling-status 동작·입출력 형식 정의.

**현행:**  
- 파이프라인 코드에는 Slack 봇 구현 없음.  
- spec_resolve_sensor는 “Slack이 labeling_specs를 pending_resolved로 UPDATE한 것”을 전제로 동작.

**수정 방향:**  
- Slack 봇은 별도 서비스/스크립트로 구현 시, 화면 명세의 입출력 포맷을 따르면 됨.  
- /labeling-confirm 시 labeling_specs의 labeling_method, categories, classes, spec_status = 'pending_resolved' UPDATE만 하면 되며, config 조회는 하지 않음(명세와 동일).

---

## 8. 알림·통지 (화면 명세 5)

**명세:**  
- auto_labeling 실패, spec_status → failed (retry 3회 초과) 시 **Slack #data-labeling 알림 필요**.  
- 완료/신규 pending은 “있으면 좋음”.

**현행:**  
- 해당 이벤트에 대한 Slack 알림 코드 없음.

**수정 방향:**  
- job 실패 시, spec_status를 failed로 바꾸는 지점에서 Slack webhook 또는 봇 API 호출 추가.  
- 알림 메시지 포맷은 화면 명세 5와 맞추면 됨.

---

## 9. 수정 사항 체크리스트 (우선순위)

| # | 항목 | 파일(예시) | 우선순위 |
|---|------|------------|----------|
| 1 | ingest_router에서 config 조회·update_spec_resolved_config 제거 | defs/spec/assets.py | 높음 |
| 2 | spec_resolve_sensor에서 config 조회 제거, spec_status는 pending_resolved 유지, retry_count 증가 및 retry_count≥3 시 failed 전환 | defs/spec/sensor.py | 높음 |
| 3 | spec_status='active'를 job 성공 시 job 내부에서만 갱신 | defs/spec/sensor.py, job 완료 처리 위치 | 높음 |
| 4 | clip_timestamp_routed 시작 시 config 조회(3-5) 및 동일 run 내 config 전달 | defs/label/assets.py, 리소스(duckdb_spec) | 높음 |
| 5 | 표준 파이프라인에서 captioning 단계 생략 금지 | defs/process/assets.py (clip_captioning_routed), definitions_staging | 중간 |
| 6 | config_sync: version 증분, 삭제 시 is_active=false | defs/spec/assets.py, resources/duckdb_spec.py | 중간 |
| 7 | clip_captioning_routed: spec_id·config 기반 백로그 처리 및 캡션 생성 구현 | defs/process/assets.py | 중간 |
| 8 | clip_to_frame_routed: spec_id·config 기반 백로그 및 frame_extraction 파라미터 적용 | defs/process/assets.py | 중간 |
| 9 | bbox_labeling: spec_id·config 기반 백로그, target_classes 교집합, bbox 파라미터 적용 | defs/yolo/assets.py | 중간 |
| 10 | labeling_spec_ingest: 외부 spec 수신 시 파싱·INSERT (연동 경로 확정 후) | defs/spec/assets.py | 낮음 |
| 11 | spec_resolve_sensor가 RunRequest 반환할지, ready_for_labeling_sensor와 역할 분리 문서화 | defs/spec/sensor.py, docs | 낮음 |
| 12 | auto_labeling 실패·spec failed 시 Slack 알림 | 별도 알림 모듈 또는 sensor/job 훅 | 낮음 |

---

## 10. 참고: 현재 Job·Asset 의존 관계

- **auto_labeling_routed_job**  
  - clip_timestamp_routed → clip_captioning_routed → clip_to_frame_routed → bbox_labeling  
- **의존성:**  
  - clip_captioning_routed deps = [clip_timestamp_routed]  
  - clip_to_frame_routed deps = [clip_timestamp_routed] (명세상은 captioning 완료 후 frame이므로, deps에 clip_captioning_routed 추가 검토)  
  - bbox_labeling deps = [clip_to_frame_routed]

명세상 순서는 timestamp → captioning → frame → bbox이므로, clip_to_frame_routed의 deps에 clip_captioning_routed를 넣는 것이 맞음.  
현재 `deps=["clip_timestamp_routed"]`만 있으면, captioning 완료 전에 frame이 돌 수 있으므로 의존성 수정 권장.

---

*문서 작성일: 2026-03-19. 명세서 또는 코드 변경 시 본 문서를 함께 갱신하는 것을 권장합니다.*
