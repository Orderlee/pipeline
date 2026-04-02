# Staging 빈 Dispatch 요청 대기 처리

## 목적

`staging`의 `piaspace-agent` polling ingress에서 `labeling_method`, `outputs`, `run_mode`, `categories`, `classes`가 모두 비어 있는 요청은 reject하지 않고 **대기(waiting)** 로 처리한다.

이 경우:

- incoming 폴더는 그대로 유지
- dispatch manifest 생성 안 함
- `dispatch_stage_job` RunRequest 생성 안 함
- `staging_dispatch_requests`, `staging_pipeline_runs` row 생성 안 함

## waiting 판별 조건

아래 필드가 모두 비어 있으면 waiting 으로 본다.

- `labeling_method`
- `outputs`
- `run_mode`
- `categories`
- `classes`

`source_unit_name` 또는 `folder_name`은 반드시 있어야 하며, 없으면 기존대로 `rejected` 처리한다.

## 처리 규칙

1. agent payload 정규화
2. `source_unit_name`/incoming 폴더 존재 확인
3. 이미 생성된 정상 `request_id`가 있으면 기존 duplicate/no-op 처리
4. 위 watched fields가 모두 비어 있으면 waiting 처리
5. watched fields 중 하나라도 채워져 있으면 기존 dispatch 흐름 진행

## waiting 시 ack

```json
{
  "delivery_id": "dlv_20260326_0001",
  "status": "accepted",
  "request_id": "req_staging_20260326_0001",
  "message": "waiting_for_dispatch_params"
}
```

## 재개 방식

- waiting 상태는 DB row를 남기지 않는다.
- 나중에 같은 `request_id`로 `labeling_method` 또는 `outputs/run_mode`와 필요한 메타를 채워 다시 보내면 정상 dispatch로 진행한다.
- 이미 정상 dispatch가 생성된 뒤에는 기존 duplicate/no-op 규칙을 유지한다.

## 실패 유지

아래는 waiting 이 아니라 기존대로 실패 처리한다.

- `source_unit_name` 없음
- incoming 폴더 없음
- payload shape 오류
