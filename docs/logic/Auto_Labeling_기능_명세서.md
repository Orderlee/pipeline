# Auto Labeling 기능 명세서

## 1. 기능 개요

- 데이터 인입 시 함께 수신된 spec.json의 labeling_method, categories, classes를 기반으로 auto_labeling을 수행한다. **실행 순서는 timestamp → captioning → frame → bbox 로 고정**이며, captioning 단계는 생략하지 않는다.
- labeling 정의가 미정인 경우 pending으로 분류하고, Slack 봇을 통해 확정 시 자동 재개한다.
- 각 task의 세부 처리 방식은 requester별 config(parameter JSON)에 의해 결정된다. **config는 ingest_router가 아니라 auto_labeling 실행 직전(첫 task, 3-5)에서 조회한다.**

## 2. 데이터 흐름 (전체)

```
incoming_nas
→ raw_ingest (파일 검증, MinIO 업로드, NAS archive 이동, 메타 등록, 중복 검출)
→ ingest_router (spec 매칭, labeling 정의 여부에 따른 DB 상태 분기)
  ├→ [확정] ingest_status = "ready_for_labeling"
  │         → auto_labeling (timestamp → captioning → frame → bbox)
  └→ [미정] ingest_status = "pending_spec"
            → Slack 봇 확정
            → spec_resolve_sensor (DB 상태 전이 + auto_labeling 트리거)

물리적 파일 이동은 raw_ingest에서 완료된다.
ingest_router 이후의 분기는 DB 상태(ingest_status) 전이만 수행한다.
"pending"은 별도 폴더가 아니라 raw_files.ingest_status = "pending_spec" 상태를 의미한다.
```

### 데이터 도착 순서

- **데이터 먼저 도착**
  - spec 없음 또는 labeling_method 미확정 → `ingest_status = "pending_spec"` → 이후 spec 확정 시 sensor로 재개
- **spec 먼저 도착**
  - labeling_specs에 저장 후 대기 → 데이터 도착·raw_ingest 완료 후 ingest_router가 매칭

## 3. Asset별 상세 로직

### 3-1. labeling_spec_ingest

```yaml
asset: labeling_spec_ingest
input:
  - 외부 API/Slackbot/기타 응답
process:
  - 외부 API/Slackbot/기타 호출 → spec.json 수신
  - spec.json 파싱
    - spec_id
    - requester_id
    - team_id
    - source_unit_name (NAS incoming 폴더명 - raw_files 매칭 키)
    - categories → classes 자동 파생 테이블 적용 (smoke → ["smoke"] 등)
    - classes
    - labeling_method (JSON 배열, 예: ["timestamp","captioning","bbox"]; 미확정 시 [] 또는 null)
  - labeling_specs 테이블 INSERT
    - labeling_method가 비어 있음([] / null) → spec_status = "pending"
    - labeling_method에 1개 이상 task 코드가 있음 → spec_status = "active"
output:
  - labeling_specs 테이블 신규 row
error_handling:
  - 외부 수집 실패 → 최대 3회 재시도, 실패 시 Dagster run 에러로 기록
  - spec.json 파싱 실패 → 해당 spec 스킵, Dagster 로그 기록
```

### 3-2. config_sync

```yaml
asset: config_sync
trigger: Dagster UI에서 수동 materialize
input:
  - config/parameters/ 폴더의 JSON 파일들
process:
  - 폴더 스캔 → JSON 파일 목록 수집
  - 각 파일의 파일명 = config_id
  - labeling_configs 테이블과 비교
    - 신규 파일 → INSERT
    - 내용 변경 → UPDATE (version + 1)
    - 파일 삭제 → is_active = false (DB에서 삭제하지 않음)
output:
  - labeling_configs 테이블 동기화 완료
error_handling:
  - JSON 파싱 실패 → 해당 파일 스킵, 경고 로그
  - 폴더 미존재 → SkipReason 기록, 다음 실행 시 재시도
```

### 3-3. ingest_router

> **설계**: 라우터는 **pending vs ready 분기만** 수행한다. requester_config_map / labeling_configs **조회는 하지 않는다**. config는 **auto_labeling 첫 task(3-5)** 시작 시 조회하여, router 이후 config 변경이 반영되도록 한다.

```yaml
asset: ingest_router
input:
  - raw_files (ingest_status = "completed")
  - labeling_specs (source_unit_name 기준 매칭)
process:
  - raw_files ↔ labeling_specs 매칭 (source_unit_name 기준)
  - raw_files.spec_id 세팅
  - spec.labeling_method 확인 (JSON 배열)
    - 비어 있음([] / null) → 분기 A (pending)
    - 1개 이상 요소 있음 → 분기 B (ready 대기열)
  - 분기 A (pending)
    - ingest_status = "pending_spec"
  - 분기 B (auto_labeling 대기)
    - ingest_status = "ready_for_labeling"
    - (config 조회 없음 — 3-5에서 수행)
output:
  - raw_files.spec_id, ingest_status 갱신
error_handling:
  - spec 매칭 실패 (동일 source_unit_name의 spec 없음 등) → 정책에 따라 pending_spec 또는 별도 처리
```

**참고**: spec_resolve_sensor가 ready_for_labeling으로 바꿀 때도 **config는 조회하지 않는다**. 동일하게 3-5에서 조회한다.

### 3-4. pending_ingest

```yaml
asset: pending_ingest
input:
  - raw_files (ingest_status = "pending_spec")
process:
  - pending 상태 데이터 목록 집계
  - 대기 현황 메타데이터 기록
output:
  - pending 건수 summary
resolution_path:
  - Slack 봇 /labeling-confirm → labeling_specs UPDATE (labeling_method, categories, classes, spec_status = 'pending_resolved')
  - spec_resolve_sensor 감지 → DB 상태 전이 + auto_labeling_routed_job 트리거
```

### 3-5. clip_timestamp_routed (이벤트 구간 식별)

> **config 조회 시점**: 본 asset **시작 시** 대상 raw_files의 spec_id로 labeling_specs에서 requester_id, team_id를 읽고, 아래 우선순위로 config_json 전체를 결정한다. 실패 시 **본 run 실패**. 동일 run 내 3-6~3-8은 **동일한 config_json**(동일 config_id)을 사용한다.

```yaml
asset: clip_timestamp_routed (= timestamp_auto)
input:
  - raw_files (ingest_status = "ready_for_labeling")
  - labeling_specs (spec_id로 requester_id, team_id 조회)
  - requester_config_map
  - labeling_configs
process:
  - [선행] config 조회 (우선순위)
    1. requester_id + scope = "personal" → requester_config_map
    2. team_id + scope = "team" → requester_config_map
    3. config_id = "_fallback" → labeling_configs
    4. 없으면 → run 실패 (에러 로그)
  - config_json.timestamp 섹션에서 파라미터 로드
    - detection_model
    - confidence_threshold
    - min_event_duration_sec
    - max_event_duration_sec
    - merge_gap_sec
    - pre_event_buffer_sec
    - post_event_buffer_sec
  - 대상 영상 목록 조회
  - 모델 추론 → 이벤트 구간 리스트 생성
  - merge_gap_sec 기준 인접 구간 병합
  - pre/post buffer 적용
  - min/max duration 필터링
  - labels 테이블 INSERT
  - 라벨 JSON → MinIO(vlm-labels) 업로드
output:
  - labels 테이블 row
  - vlm-labels 버킷 라벨 JSON
error_handling:
  - 모델 추론 실패 → 해당 영상 스킵, Dagster 로그 기록
  - 이벤트 0건 감지 → 정상 완료 (경고 로그)
  - 타임아웃 → Dagster 로그 기록, 다음 실행 시 재시도
```

### 3-6. clip_captioning_routed (캡션 생성)

```yaml
asset: clip_captioning_routed (= captioning_auto)
input:
  - labels (label_status = "completed" AND caption_text IS NULL)
  - raw_files (원본 영상 접근)
  - labeling_configs (3-5에서 확정한 config_json의 captioning 섹션)
process:
  - 3-5와 동일 규칙으로 config_json 로드 후 captioning 섹션 파라미터 로드
    - model
    - language
    - max_caption_length
    - generate_summary
    - include_object_list
  - 이벤트 구간별 영상 구간 추출
  - AI 모델로 캡션 생성
  - labels.caption_text UPDATE
  - 라벨 JSON 갱신 → MinIO(vlm-labels)
output:
  - labels.caption_text 갱신
  - vlm-labels 버킷 라벨 JSON 갱신
error_handling:
  - 캡션 생성 실패 → 해당 구간 스킵, Dagster 로그 기록
  - 빈 캡션 반환 → 경고 로그, caption_text = null 유지
```

### 3-7. clip_to_frame_routed (frame 추출)

```yaml
asset: clip_to_frame_routed (= frame_extraction_auto)
input:
  - labels (caption 완료된 이벤트 구간)
  - raw_files (원본 영상)
  - labeling_configs (3-5와 동일 config_json의 frame_extraction 섹션)
process:
  - config 파라미터 로드
    - frame_interval_sec
    - max_frames_per_clip
    - output_format
    - output_quality
    - resize_width
    - resize_height
    - keep_aspect_ratio
    - extract_key_frames_only
  - 이벤트 구간을 clip 영상으로 절단
  - clip에서 설정 간격으로 frame 이미지 추출
  - processed_clips 테이블 INSERT
  - image_metadata 테이블 INSERT
  - 결과물 → MinIO(vlm-processed) 업로드
output:
  - processed_clips 테이블 row
  - image_metadata 테이블 row
  - vlm-processed 버킷 clip + frames
error_handling:
  - 영상 손상 → 해당 clip 스킵, Dagster 로그 기록
  - 디스크/메모리 부족 → 처리 중단, 부분 결과 보존
```

### 3-8. bbox_labeling (객체 감지 + bounding box)

```yaml
asset: bbox_labeling (= bbox_labeling_auto)
input:
  - image_metadata (frame 추출 완료된 이미지)
  - labeling_specs (classes: 감지 대상 클래스)
  - labeling_configs (3-5와 동일 config_json의 bbox 섹션)
process:
  - config 파라미터 로드
    - detection_model
    - confidence_threshold
    - nms_threshold
    - max_detections_per_image
  - target_classes = intersection(spec.classes, config.target_classes) (정의된 경우)
  - frame 이미지 로드
  - 객체 감지 모델 추론
    - bbox
    - class
    - confidence
  - labels 테이블 INSERT/UPDATE (object_count, bbox 정보)
  - bbox 라벨 JSON → MinIO(vlm-labels) 업로드
output:
  - labels 테이블 (bbox 정보 포함)
  - vlm-labels 버킷 bbox 라벨 JSON
error_handling:
  - 모델 추론 실패 → 해당 이미지 스킵, Dagster 로그 기록
  - 객체 0건 감지 → 정상 완료 (object_count = 0)
```

## 4. Sensor

### spec_resolve_sensor

- **[역할]** labeling_specs에서 spec_status = 'pending_resolved' 감지 → DB 상태 전이 + auto_labeling 트리거
- **[동작]**
  1. 주기적 폴링 (60초 간격)
  2. 쿼리: spec_status = 'pending_resolved' AND retry_count < 3
  3. 매칭 시:
     - labeling_specs.retry_count += 1
     - 관련 raw_files.ingest_status → 'ready_for_labeling' 갱신
     - auto_labeling_routed_job 트리거 (RunRequest)
     - spec_status는 변경하지 않음 (pending_resolved 유지)
  4. job 성공 시:
     - job 내부에서 spec_status → 'active' 갱신
  5. job 실패 시:
     - spec_status = 'pending_resolved' 유지 → 다음 폴링에서 재감지
     - retry_count >= 3이면 → spec_status → 'failed' 전환 + 경고 로그
- **[트리거 원인]** Slack 봇이 /labeling-confirm으로 labeling_specs UPDATE → spec_status = 'pending_resolved'
- **[주의]** sensor 내부에서 DB 상태 전이를 직접 수행한다 (ingest_router 재실행 불필요). **config 조회는 하지 않는다** (3-5에서 수행). 데이터는 이미 raw_ingest에서 archive 완료 상태이므로 물리적 이동 없음.

## 5. Slack 봇 명령어

- `/labeling-confirm <spec_id> --method <task1,task2,...> --categories <cat1,cat2> --classes <cls1,cls2>`  
  → labeling_specs UPDATE (labeling_method를 JSON 배열로 저장, categories, classes, spec_status = 'pending_resolved')  
  - 예: `--method timestamp,captioning,bbox` → `["timestamp","captioning","bbox"]`
- `/labeling-pending [--requester <requester_id>]` → pending 상태 spec 목록 반환
- `/labeling-status <spec_id>` → 해당 spec의 현재 상태, 연결된 데이터 건수, 처리 진행률 반환

## 6. DB 테이블 추가

### 6-1. 신규 테이블

```
┌─────────────────────────────────────────────────────────┐
│ labeling_specs                                          │
├──────────────────┬──────────┬───────────────────────────┤
│ 컬럼              │ 타입     │ 설명                      │
├──────────────────┼──────────┼───────────────────────────┤
│ spec_id (PK)     │ VARCHAR  │ spec 고유 식별자           │
│ requester_id     │ VARCHAR  │ 요청자 식별자              │
│ team_id          │ VARCHAR  │ 소속 팀 식별자              │
│ source_unit_name │ VARCHAR  │ NAS incoming 폴더명 (raw_files 매칭 키)  │
│ categories       │ JSON     │ 라벨링 대상 카테고리 목록   │
│ classes          │ JSON     │ 라벨링 대상 클래스 목록     │
│ labeling_method  │ JSON     │ 예: ["timestamp","captioning","bbox"]; 미확정 [] │
│ spec_status      │ VARCHAR  │ pending | pending_resolved | active | failed │
│ retry_count      │ INTEGER  │ sensor 재트리거 횟수 (기본 0, 최대 3) │
│ created_at       │ TIMESTAMP│ 생성 시각                   │
│ updated_at       │ TIMESTAMP│ 마지막 수정 시각            │
└──────────────────┴──────────┴───────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ labeling_configs                                        │
├──────────────────┬──────────┬───────────────────────────┤
│ 컬럼              │ 타입     │ 설명                      │
├──────────────────┼──────────┼───────────────────────────┤
│ config_id (PK)   │ VARCHAR  │ config 고유 식별자          │
│ config_json      │ JSON     │ timestamp, captioning, frame_extraction, bbox │
│ version          │ INTEGER  │ 버전 (변경 시 +1)           │
│ is_active        │ BOOLEAN  │ 활성 여부 (파일 삭제 시 FALSE) │
│ created_at       │ TIMESTAMP│ 최초 등록 시각              │
│ updated_at       │ TIMESTAMP│ 마지막 수정 시각            │
└──────────────────┴──────────┴───────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ requester_config_map                                    │
├──────────────────┬──────────┬───────────────────────────┤
│ 컬럼              │ 타입     │ 설명                      │
├──────────────────┼──────────┼───────────────────────────┤
│ requester_id (PK)│ VARCHAR  │ 요청자 또는 팀 식별자       │
│ config_id (PK)   │ VARCHAR  │ FK → labeling_configs      │
│ scope (PK)       │ VARCHAR  │ "personal" | "team"        │
│ team_id          │ VARCHAR  │ 소속 팀 식별자              │
└──────────────────┴──────────┴───────────────────────────┘

config 조회 우선순위 (3-5 및 동일 run 내 후속 task):
  1. requester_config_map: requester_id + scope = "personal" → 개인 config
  2. requester_config_map: team_id + scope = "team" → 팀 default config
  3. labeling_configs: config_id = "_fallback" 직접 조회
  4. 없으면 에러 (run 실패)
```

### 6-2. 기존 테이블 변경

```
┌─────────────────────────────────────────────────────────┐
│ raw_files (변경분만 표시)                                 │
├──────────────────┬──────────┬────────────────────────────┤
│ 컬럼              │ 타입     │ 설명                       │
├──────────────────┼──────────┼────────────────────────────┤
│ + spec_id        │ VARCHAR  │ FK → labeling_specs        │
│ + source_unit_name│ VARCHAR │ NAS incoming 폴더명 (manifest에서 저장) │
└──────────────────┴──────────┴────────────────────────────┘

ingest_status 값 확장:
  기존: "pending" | "completed"
  추가: "pending_spec" | "ready_for_labeling"
```

### 6-3. 테이블 관계

- labeling_specs.source_unit_name ←→ raw_files.source_unit_name (1:N) — 하나의 spec(폴더 단위)에 해당 폴더의 모든 파일이 연결됨
- labeling_specs ←──── raw_files.spec_id (1:N) — ingest_router가 매칭 후 spec_id 세팅
- labeling_configs ←── requester_config_map.config_id (1:N)
- requester_config_map.requester_id ──→ labeling_specs.requester_id (논리적 참조)
- requester_config_map.team_id ──→ labeling_specs.team_id (논리적 참조)

> **requester_config_map 등록**: 운영 절차(수동 SQL, 내부 관리 UI, 스크립트 등)로 별도 정의. 본 문서 범위는 조회 우선순위만 규정한다.

## 7. 외부 연동

- 외부 API/Slackbot/기타: spec.json 수신
- Slack API: 봇 명령어 수신/응답
- MotherDuck: 클라우드 동기화
- MinIO: vlm-raw, vlm-labels, vlm-processed, vlm-dataset 버킷

## 8. 운영 노트

- config_sync는 Dagster UI에서 **수동 materialize**로 실행한다.
- `_fallback` config 파일은 운영 절차로 존재를 보장한다 (코드 보호 없음).
- spec_resolve_sensor의 retry_count가 3 이상이면 `spec_status = 'failed'`로 전환되며, 수동 개입이 필요하다.
- 물리적 파일 이동(MinIO 업로드, NAS archive)은 raw_ingest에서만 수행한다. 이후 단계는 DB 상태 전이만 한다.
- `source_unit_name`은 NAS incoming 폴더명이며, manifest에 포함된 값을 raw_ingest 시 raw_files에 저장한다.
- **ingest_router는 config를 읽지 않는다.** config 실패는 auto_labeling run(3-5 시작 시)에서 드러난다.
