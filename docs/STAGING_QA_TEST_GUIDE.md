# Staging QA Test Guide

## 목적

이 문서는 현재 코드 로직 기준으로 staging 환경에서 dispatch 기반 QA를 어떻게 진행해야 하는지 정리한 가이드입니다.

테스트 범위는 아래까지입니다.

- `raw_ingest`
- `clip_timestamp`
- `clip_captioning`
- `clip_to_frame`
- `raw_video_to_frame`
- `yolo_image_detection`

`build_dataset` 는 `dispatch_stage_job` 에 포함되어 있지 않으므로 staging dispatch QA 범위에서 제외합니다.

## staging 로직 요약

staging 에서는 일반 운영 센서 흐름과 다르게 `dispatch` 중심으로 동작합니다.

기본(권장) ingress는 `staging_agent_dispatch_sensor` 입니다.

1. 테스트 폴더를 `/home/pia/mou/staging/incoming/<folder_name>` 에 넣습니다.
2. `piaspace-agent`가 pending 요청을 제공합니다.
3. `staging_agent_dispatch_sensor`가 API polling으로 요청을 가져옵니다.
4. 공통 dispatch 코어가 manifest 생성 + `staging_dispatch_requests`/`staging_pipeline_runs` 기록을 수행합니다.
5. `dispatch_stage_job` run request를 생성해 실행합니다.

파일 기반 trigger(`incoming/.dispatch/pending/*.json` + `dispatch_sensor`)는 **호환/레거시 경로**이며, staging QA의 기본 경로는 아닙니다.

`dispatch_stage_job` 의 분기는 `outputs` 값에 따라 아래처럼 갈립니다.

| outputs | 실제 실행 단계 |
| --- | --- |
| `["timestamp"]` | `raw_ingest` -> `clip_timestamp` |
| `["timestamp","captioning"]` | `raw_ingest` -> `clip_timestamp` -> `clip_captioning` -> `clip_to_frame` |
| `["bbox"]` | `raw_ingest` -> `raw_video_to_frame` -> `yolo_image_detection` |
| `["bbox","timestamp"]` | `raw_ingest` -> `clip_timestamp` + `raw_video_to_frame` -> `yolo_image_detection` |
| `["bbox","captioning"]` | `captioning` 이 `timestamp` 를 자동 포함하므로 사실상 `["bbox","timestamp","captioning"]` 과 동일 |
| `["bbox","timestamp","captioning"]` | `raw_ingest` -> `clip_timestamp` -> `clip_captioning` -> `clip_to_frame` + `raw_video_to_frame` -> `yolo_image_detection` |

## 사전 준비

### 1. 환경 초기화

이전 테스트 결과를 제거하려면 아래 스크립트를 먼저 실행합니다.

```bash
bash scripts/cleanup_staging_test.sh
```

### 2. 서비스 기동

**dagster-staging 이 멈추거나 `daemon_heartbeats` 오류가 나면** → `docs/STAGING_DAGSTER_RECOVERY.md` 참고 후 스토리지 초기화·재기동.

Gemini 만 볼 때:

```bash
docker compose -f docker/docker-compose.yaml --profile staging up -d postgres dagster-staging
```

YOLO 까지 볼 때:

```bash
docker compose -f docker/docker-compose.yaml --profile staging up -d postgres dagster-staging yolo
```

확인 포인트:

- Dagster UI: `http://localhost:3031`
- staging DuckDB: `/data/staging.duckdb` inside `dagster-staging`
- staging incoming root: `/home/pia/mou/staging/incoming`
- staging archive root: `/home/pia/mou/staging/archive`
- staging archive pending root(레거시 파일 ingress 보조): `/home/pia/mou/staging/archive_pending`

### 3. 외부 의존성 확인

테스트 전 아래 조건이 충족되어야 합니다.

- Gemini 인증이 유효해야 함
- staging MinIO `http://172.168.47.36:9002` 접근 가능해야 함
- `bbox` 테스트 시 YOLO 서버가 살아 있어야 함
- ffmpeg, ffprobe 가 컨테이너 안에서 실행 가능해야 함

## 추천 테스트 순서

권장 순서는 아래입니다.

1. `timestamp`
2. `timestamp + captioning`
3. `bbox`
4. `bbox + captioning`
5. `2개 폴더 동시 투입 + trigger 1개만 생성`
6. 실패 케이스

이 순서대로 보면 ingest -> Gemini -> frame extract -> YOLO 흐름을 단계적으로 확인할 수 있습니다.

## 테스트 실행 방법

### 방법 A. 제공된 스크립트 사용

이미 준비된 폴더가 `/home/pia/mou/staging/tmp_data_2`, `/home/pia/mou/staging/GS건설` 에 있다면 아래 스크립트를 쓰는 게 가장 빠릅니다.

`timestamp` 테스트:

```bash
python3 scripts/staging_test_dispatch.py --folder tmp_data_2 --round 1 --outputs timestamp
```

`timestamp + captioning` 테스트:

```bash
python3 scripts/staging_test_dispatch.py --folder tmp_data_2 --round 1 --outputs timestamp captioning
```

`bbox` 테스트:

```bash
python3 scripts/staging_test_dispatch.py --folder tmp_data_2 --round 1 --outputs bbox
```

`bbox + captioning` 테스트:

```bash
python3 scripts/staging_test_dispatch.py --folder GS건설 --round 1 --outputs bbox captioning
```

### 방법 B. 수동 trigger JSON 생성 (레거시 호환)

1. 테스트 폴더를 `/home/pia/mou/staging/incoming/<folder_name>` 에 복사합니다.
2. 아래 형태의 JSON 을 `/home/pia/mou/staging/incoming/.dispatch/pending/<request_id>.json` 로 저장합니다.

```json
{
  "request_id": "staging_manual_001",
  "folder_name": "tmp_data_2",
  "outputs": ["bbox", "timestamp", "captioning"],
  "requested_by": "qa",
  "requested_at": "2026-03-18T10:00:00",
  "max_frames_per_video": 24,
  "jpeg_quality": 90,
  "confidence_threshold": 0.25,
  "iou_threshold": 0.45
}
```

필수에 가까운 값은 아래입니다.

- `request_id`
- `folder_name`
- `outputs`

주의:

- 이 방식은 운영과 동일한 파일 ingress 검증이나 회귀 테스트에 유용합니다.
- staging 기본 경로(agent polling) 검증은 방법 A 또는 agent API 직접 호출로 진행하세요.

### 방법 C. 2개 폴더 동시 복사 + trigger 1개만 생성

질문하신 조건은 현재 로직상 아래처럼 테스트하면 됩니다.

1. `/home/pia/mou/staging/incoming/GS건설`
2. `/home/pia/mou/staging/incoming/tmp_data_2`

두 폴더를 모두 `incoming` 에 복사합니다.

이 상태에서 trigger(파일 또는 agent 요청)는 둘 중 하나만 생성합니다.

예시: `tmp_data_2` 만 실행하고 `GS건설` 은 대기 상태로 남기기

```json
{
  "request_id": "staging_dual_folder_001",
  "folder_name": "tmp_data_2",
  "outputs": ["timestamp", "captioning"],
  "requested_by": "qa",
  "requested_at": "2026-03-18T10:30:00"
}
```

위 JSON을 `/home/pia/mou/staging/incoming/.dispatch/pending/` 에 넣거나, agent 요청으로 동일 payload를 보내면 기대 동작은 아래와 같습니다.

- `tmp_data_2` 만 `dispatch_stage_job` 대상이 됨
- `tmp_data_2` 는 incoming 경로에서 manifest 생성 후 ingest 진행
- `tmp_data_2` 는 처리 중 `archive` 로 이동
- `GS건설` 은 trigger JSON 이 없으므로 처리되지 않음
- `GS건설` 은 `/home/pia/mou/staging/incoming/GS건설` 에 그대로 남아 있음

즉 현재 staging 로직은 "폴더가 incoming에 있다고 자동 실행"되지 않고, "dispatch 요청이 있는 폴더만 실행"됩니다.

## 케이스별 기대 결과

### 1. `outputs = ["timestamp"]`

기대 결과:

- `raw_files` 에 해당 폴더 파일이 `completed` 로 적재됨
- `video_metadata.auto_label_status = 'generated'`
- `vlm-labels/<folder_prefix>/events/*.json` 생성
- `labels`, `processed_clips`, `image_labels` 는 증가하지 않거나 해당 폴더 기준 변화가 없음

### 2. `outputs = ["timestamp", "captioning"]`

기대 결과:

- `timestamp` 결과 포함
- `video_metadata.auto_label_status = 'completed'`
- `labels` row 생성
- `processed_clips` row 생성
- `image_metadata.image_role = 'processed_clip_frame'` 생성
- `image_labels` 는 생성되지 않음

### 3. `outputs = ["bbox"]`

기대 결과:

- `raw_files` 적재 완료
- `video_metadata.frame_extract_status = 'completed'`
- `image_metadata.image_role = 'raw_video_frame'` 생성
- `image_labels` row 생성
- `labels`, `processed_clips` 는 생성되지 않음

### 4. `outputs = ["bbox", "timestamp", "captioning"]`

기대 결과:

- `timestamp + captioning + bbox` 결과가 모두 나와야 함
- `image_metadata` 에 아래 두 종류가 함께 생길 수 있음
- `processed_clip_frame`
- `raw_video_frame`
- `image_labels` 는 위 두 frame role 모두를 대상으로 들어갈 수 있음

### 5. `2개 폴더 동시 복사 + trigger 1개만 생성`

예시 조건:

- `incoming` 에 `GS건설`, `tmp_data_2` 두 폴더를 모두 복사
- trigger JSON 은 `tmp_data_2` 만 생성

기대 결과:

- `tmp_data_2` 는 처리 시작 후 최종적으로 `/home/pia/mou/staging/archive/tmp_data_2` 로 이동
- `GS건설` 은 `/home/pia/mou/staging/incoming/GS건설` 에 그대로 남아 있어야 함
- `tmp_data_2` 에 대해서만 `raw_files`, `video_metadata`, `labels`, `processed_clips`, `image_metadata`, `image_labels` 변화가 발생
- `GS건설` 에 대해서는 trigger 를 만들기 전까지 DB 변화가 없어야 함
- `GS건설` 폴더는 이후 별도 trigger JSON 을 넣으면 그 시점에 처리 가능해야 함

## 파일 기준 확인 포인트

성공 시 우선 확인할 파일 위치는 아래입니다.

- (파일 ingress일 때) trigger 처리 성공: `/home/pia/mou/staging/incoming/.dispatch/processed/<request_id>.json`
- (파일 ingress일 때) trigger 처리 실패: `/home/pia/mou/staging/incoming/.dispatch/failed/<request_id>.json`
- dispatch manifest: `/home/pia/mou/staging/incoming/.manifests/dispatch/`
- archive 이동 결과: `/home/pia/mou/staging/archive/<folder_name>/`

## DB 기준 확인 포인트

아래 명령으로 staging DB 를 직접 확인할 수 있습니다.

```bash
docker compose -f docker/docker-compose.yaml --profile staging exec -T dagster-staging \
python3 -c "import duckdb; conn=duckdb.connect('/data/staging.duckdb', read_only=True); print(conn.execute(\"SELECT COUNT(*) FROM raw_files\").fetchone()[0])"
```

### 기본 조회 SQL

최근 dispatch 요청:

```sql
SELECT request_id, folder_name, outputs, status, error_message, processed_at
FROM staging_dispatch_requests
ORDER BY created_at DESC
LIMIT 20;
```

폴더 기준 ingest 상태:

```sql
SELECT ingest_status, COUNT(*)
FROM raw_files
WHERE raw_key LIKE 'tmp_data_2/%'
GROUP BY 1
ORDER BY 1;
```

Gemini 상태:

```sql
SELECT COALESCE(vm.auto_label_status, 'null') AS auto_label_status, COUNT(*)
FROM raw_files r
JOIN video_metadata vm ON vm.asset_id = r.asset_id
WHERE r.raw_key LIKE 'tmp_data_2/%'
GROUP BY 1
ORDER BY 1;
```

captioning 결과:

```sql
SELECT COUNT(*)
FROM labels l
JOIN raw_files r ON r.asset_id = l.asset_id
WHERE r.raw_key LIKE 'tmp_data_2/%';
```

frame 추출 결과:

```sql
SELECT im.image_role, COUNT(*)
FROM image_metadata im
JOIN raw_files r ON r.asset_id = im.source_asset_id
WHERE r.raw_key LIKE 'tmp_data_2/%'
GROUP BY 1
ORDER BY 1;
```

YOLO 결과:

```sql
SELECT COUNT(*)
FROM image_labels il
JOIN image_metadata im ON im.image_id = il.image_id
JOIN raw_files r ON r.asset_id = im.source_asset_id
WHERE r.raw_key LIKE 'tmp_data_2/%';
```

2개 폴더 중 1개만 trigger 했는지 확인:

```sql
SELECT
  CASE
    WHEN r.raw_key LIKE 'tmp_data_2/%' THEN 'tmp_data_2'
    WHEN r.raw_key LIKE 'gsgeonseol/%' THEN 'GS건설'
    ELSE 'other'
  END AS folder_group,
  COUNT(*) AS row_count
FROM raw_files r
GROUP BY 1
ORDER BY 1;
```

기대 결과 예시:

- `tmp_data_2` 는 `row_count > 0`
- `GS건설` 는 `row_count = 0`

## 한글 폴더명 주의사항

폴더명은 raw key 생성 시 sanitize 됩니다.

예시:

- `tmp_data_2` -> `tmp_data_2`
- `GS건설` -> `gsgeonseol`

즉 `GS건설` 로 테스트했으면 SQL 에서는 보통 아래처럼 조회해야 합니다.

```sql
SELECT COUNT(*)
FROM raw_files
WHERE raw_key LIKE 'gsgeonseol/%';
```

## 실패 케이스 테스트

아래 케이스는 꼭 한 번씩 보는 것을 권장합니다.

| 케이스 | 방법 | 기대 결과 |
| --- | --- | --- |
| 잘못된 output | 아래 **「잘못된 output」 절** 참고 | `.dispatch/failed`, `error_message = 'invalid_outputs'` |
| 없는 폴더 요청 | `folder_name` 경로 없이 요청 생성 | `folder_not_in_incoming` |
| media 없는 폴더 | txt 만 있는 폴더로 요청 | ingest 단계 실패 로그 생성(실패 원인 확인) |
| 중복 request_id | 같은 `request_id` 재사용 | agent ingress는 `duplicate_request_id_noop` ack 후 run 미생성 |
| 동일 폴더 중복 실행 | 첫 실행이 끝나기 전에 같은 폴더로 재요청 | agent ingress는 ack 보류 후 다음 tick 재평가 |

### 「잘못된 output」 테스트 — 올바른 JSON (필수)

dispatch ingress 파서는 `outputs` 배열에서 **허용 키만 남기고** 나머지는 버립니다.  
`VALID_OUTPUTS` 에 없는 값만 있으면 `invalid_outputs` 로 실패합니다.

**공식 예시 (QA·문서·자동 생성 모두 이 형태만 사용):**

```json
"outputs": ["bad_output"]
```

**하지 말 것**

1. **`["bad_output", "not_real"]` 같이 무효값만 여러 개 나열** — 동작은 `invalid_outputs` 와 동일할 수 있으나, 가이드·보고서와 어긋나고 의미 없음. **한 개의 알 수 없는 키만 쓸 것.**
2. **유효값 + 무효값 혼합** (예: `["bad_output", "timestamp"]`) — 무효값은 제거되고 **`timestamp` 만 실행**됩니다. 이 경우 **`invalid_outputs` 가 아님**. 실패 케이스 QA로 쓰면 **오판**함.

## QA 판정 기준

성공 판정은 아래 순서로 보는 것을 권장합니다.

1. Dagster UI 에서 `dispatch_stage_job` run 이 성공했는지 확인
2. (파일 ingress일 때) trigger JSON 이 `.dispatch/processed` 로 이동했는지 확인
3. archive 폴더가 생성되었는지 확인
4. outputs 조합에 맞는 DB row 가 생겼는지 확인
5. MinIO 산출물과 frame role 이 기대와 맞는지 확인
6. lineage 확인이 끝나면 QA 결과 보고서 1건을 반드시 제출

`2개 폴더 동시 투입 + trigger 1개` 시나리오에서는 아래도 추가로 확인합니다.

7. trigger 를 생성한 폴더만 `archive` 로 이동했는지 확인
8. trigger 를 생성하지 않은 폴더는 `/home/pia/mou/staging/incoming/` 에 그대로 남아 있는지 확인
9. trigger 를 생성하지 않은 폴더는 DB row 가 아직 생성되지 않았는지 확인

## Lineage 종료 후 보고서 제출

lineage 확인이 끝났다는 것은 최소한 아래 흐름을 확인했다는 뜻으로 간주합니다.

- `incoming`
- dispatch 요청 수신(file 또는 agent)
- dispatch manifest 생성
- Dagster run 실행
- DB row 생성 여부 확인
- MinIO 또는 산출물 생성 여부 확인
- 최종 archive 이동 여부 확인

위 확인이 끝나면 반드시 QA 결과 보고서 1건을 제출해야 합니다.

이 문서 기준으로는 아래 원칙을 따릅니다.

- 보고서 제출 전에는 QA 완료로 간주하지 않음
- 케이스별로 1건씩 제출하거나, 같은 회차 결과를 묶어서 1건으로 제출 가능
- 최소한 `request_id`, `folder_name`, `outputs`, 결과, 이슈, 증빙을 포함해야 함

### 필수 보고 항목

- 테스트 일시
- 테스트 담당자
- `request_id`
- `folder_name`
- `outputs`
- 테스트 목적
- lineage 확인 결과
- 성공/실패 판정
- 확인한 증빙
- 발견 이슈
- 후속 조치 필요 여부

### 보고서 템플릿

아래 템플릿을 그대로 복사해서 사용하면 됩니다.

```md
# Staging QA Result Report

- 테스트 일시:
- 테스트 담당자:
- request_id:
- folder_name:
- outputs:
- 테스트 목적:

## 1. 실행 요약

- trigger JSON 생성 여부:
- Dagster run 결과:
- 최종 판정: PASS / FAIL / CONDITIONAL PASS

## 2. Lineage 확인 결과

- incoming 적재 확인:
- dispatch 요청 수신 확인:
- dispatch manifest 생성 확인:
- raw_files 적재 확인:
- video_metadata 상태 확인:
- labels 생성 확인:
- processed_clips 생성 확인:
- image_metadata 생성 확인:
- image_labels 생성 확인:
- MinIO 산출물 확인:
- archive 이동 확인:

## 3. 증빙

- 확인한 경로:
- 확인한 SQL:
- 확인한 Dagster run:
- 스크린샷 또는 로그 위치:

## 4. 이슈

- 이슈 여부:
- 상세 내용:
- 재현 가능 여부:
- 영향 범위:

## 5. 후속 조치

- 추가 확인 필요 여부:
- 개발 수정 필요 여부:
- 다음 액션:
```

### 보고서 제출 예외 없음

성공 케이스든 실패 케이스든 lineage 확인을 수행했다면 보고서는 반드시 제출합니다.

특히 아래 케이스도 보고 대상입니다.

- 정상 처리 성공
- trigger 실패
- 일부 단계만 성공
- 한 폴더만 처리되고 다른 폴더는 `incoming` 에 남는 케이스

## 현재 로직 기준 주의사항

### 1. staging 추적 테이블은 완료 상태까지 신뢰하면 안 됨

현재 코드 기준으로 `staging_dispatch_requests.status` 는 요청 수락 시 `running` 으로 기록되지만, 성공 후 `completed` 로 업데이트되는 로직은 없습니다.

`staging_pipeline_runs.step_status` 역시 `pending` row 는 만들어지지만 실제 step 완료/실패로 갱신하는 로직은 없습니다.

즉 QA 성공 여부는 아래를 기준으로 봐야 합니다.

- Dagster run 성공 여부
- downstream 테이블 생성 여부
- 산출물 생성 여부

### 2. `captioning` 은 `timestamp` 를 자동 포함함

`outputs` 에 `captioning` 만 넣어도 내부적으로 `timestamp` 가 함께 실행됩니다.

### 3. YOLO threshold 파라미터는 현재 저장만 되고 실제 추론에는 반영되지 않음

dispatch JSON 의 아래 값은 `staging_dispatch_requests` 와 `staging_pipeline_runs` 에는 기록됩니다.

- `confidence_threshold`
- `iou_threshold`

하지만 현재 `yolo_image_detection` asset 은 run tag 값을 읽지 않고 op config 기본값을 사용합니다.

따라서 현재 staging QA 에서는 이 두 값의 "실제 반영 여부" 테스트를 성공 기준으로 잡지 않는 것이 맞습니다.

### 4. frame 추출 파라미터는 경로별 반영 범위가 다름

- `raw_video_to_frame` 는 run tag 의 `max_frames_per_video`, `jpeg_quality` 를 읽습니다
- `clip_to_frame` 는 현재 run tag 를 읽지 않고 asset config 기본값을 사용합니다

즉 `bbox` 경로에서는 프레임 파라미터 변경 테스트가 가능하지만, `captioning` 경로에서는 같은 방식으로 검증하면 안 됩니다.

### 5. staging 는 일반 backlog 센서를 일부 끈 상태임

staging 에서는 아래 센서가 제외됩니다.

- `auto_labeling_sensor`
- `yolo_detection_sensor`
- `auto_bootstrap_manifest_sensor`
- `incoming_manifest_sensor`

즉 이 문서의 QA 는 "dispatch 기반 staging 검증" 에 맞춰 진행해야 합니다.

## 마무리 체크리스트

- `timestamp` 단독 성공
- `timestamp + captioning` 성공
- `bbox` 단독 성공
- `bbox + captioning` 성공
- 한글 폴더명 케이스 성공
- `2개 폴더 동시 투입 + trigger 1개` 케이스 성공
- invalid trigger 실패 케이스 확인
- archive, DB, Dagster UI 결과가 서로 일치하는지 확인
- lineage 확인 후 QA 결과 보고서 1건 제출 완료
