# Staging Agent API Polling Dispatch

## 목적

staging은 더 이상 `incoming/.dispatch/pending/*.json` 요청 파일을 만들지 않고,
` -agent:8081`를 Dagster sensor가 직접 polling해서 `dispatch_stage_job`을 시작한다.

이 변경은 `staging` ingress 전환을 중심으로 시작한다. 현재 `production`은 `production_agent_dispatch_sensor`를 기본 ingress로 사용하고, file-based `dispatch_sensor`를 fallback으로 유지한다.

## 현재 흐름

`incoming/.dispatch/pending/*.json`
-> `dispatch_sensor`
-> dispatch manifest 생성
-> `dispatch_stage_job`
-> `raw_ingest -> clip_timestamp -> clip_to_frame -> bbox`

## 변경 후 흐름

` -agent /api/staging/dispatch/pending`
-> `staging_agent_dispatch_sensor`
-> dispatch manifest 생성
-> `dispatch_stage_job`
-> `raw_ingest -> clip_timestamp -> clip_to_frame -> bbox`

요청 ingress JSON 파일만 제거하고, 내부 구현에 필요한 아래 파일은 유지한다.

- `manifest_dir/dispatch/*.json`
- retry manifest
- ingest failure JSONL

## Agent API 계약

### GET `/api/staging/dispatch/pending?limit=<n>`

```json
{
  "items": [
    {
      "delivery_id": "dlv_20260326_0001",
      "request": {
        "request_id": "req_staging_20260326_0001",
        "source_unit_name": "tmp_data_2",
        "labeling_method": ["timestamp", "captioning", "bbox"],
        "categories": ["falldown", "smoke"],
        "classes": ["person_fallen", "smoke"],
        "image_profile": "current",
        "requested_by": " -agent",
        "requested_at": "2026-03-26T15:00:00+09:00"
      }
    }
  ]
}
```

필드 규칙:

- `source_unit_name` -> staging 내부 `folder_name`
- `request_id` 없으면 `agent_<delivery_id>` 생성
- `classes` 없고 `categories`만 있으면 기존 파생 규칙 사용
- `image_profile` 없으면 `current`
- `requested_by` 없으면 ` -agent`

### POST `/api/staging/dispatch/ack`

```json
{
  "delivery_id": "dlv_20260326_0001",
  "status": "accepted",
  "request_id": "req_staging_20260326_0001",
  "message": "dispatch manifest created and run requested"
}
```

ack 규칙:

- `accepted`
  - manifest 생성, DB 기록, RunRequest 생성까지 성공
  - 이미 처리된 `request_id`라서 no-op 종료
- `rejected`
  - 필수값 누락
  - incoming 폴더 없음
  - canonical dispatch payload 검증 실패
- `ack 보류`
  - 같은 `folder_name`에 active dispatch run이 진행 중인 경우
  - staging 내부 transient 오류가 발생한 경우

## Staging sensor 동작

1. agent pending API polling
2. agent payload를 staging canonical dispatch payload로 정규화
3. 기존 dispatch service helper 재사용
   - `prepare_dispatch_request()`
   - `resolve_dispatch_applied_params()`
   - `write_dispatch_manifest()`
   - `build_dispatch_request_record()`
   - `build_dispatch_pipeline_rows()`
   - `build_dispatch_run_request()`
4. `staging_dispatch_requests`, `staging_pipeline_runs` 기록
5. `dispatch_stage_job` RunRequest 생성
6. agent ack 호출

## Production 비영향 범위

- production 기본 ingress는 `production_agent_dispatch_sensor`로 유지한다
- production `.dispatch/pending/*.json` fallback 정책도 그대로 유지한다
- 이번 문서의 직접 대상은 staging sensor, staging env, staging 운영 절차다

## 테스트 시나리오

- agent payload -> canonical dispatch payload 변환
- `classes` 비어 있을 때 `categories -> classes` 파생
- duplicate `request_id` -> accepted ack + no-op
- incoming 폴더 없음 -> rejected ack
- agent unreachable -> skip
- staging run이 기존 `raw_ingest -> clip_timestamp -> clip_to_frame -> bbox` 흐름을 그대로 타는지 확인
- production agent polling 기본 경로와 file-based fallback 회귀 없음 확인
