# Staging 빈 Dispatch 요청 대기 처리

## Summary
`staging`의 `agent` polling ingress에서 `labeling_method`, `outputs`, `run_mode`, `categories`, `classes`가 모두 비어 있는 요청은 reject하지 않고 **대기(waiting)** 로 처리합니다. 대기 상태에서는 incoming 폴더를 그대로 두고, dispatch run/manifest/DB tracking row는 만들지 않습니다.

문서 파일 경로는 `docs/staging_agent_waiting_dispatch_plan.md`로 고정합니다.

## Implementation Changes
- `staging_agent_dispatch_sensor`에 **waiting 판별 함수**를 추가합니다.
  - 입력: agent request payload
  - 조건: `labeling_method`, `outputs`, `run_mode`, `categories`, `classes`가 모두 비어 있음
  - 결과: waiting 처리
- waiting 처리 동작은 아래로 고정합니다.
  - `prepare_dispatch_request()` 호출 안 함
  - `write_dispatch_manifest()` 호출 안 함
  - `build_dispatch_request_record()` / `build_dispatch_pipeline_rows()` 호출 안 함
  - `dispatch_stage_job` `RunRequest` 생성 안 함
  - `staging_dispatch_requests`, `staging_pipeline_runs`에 row 생성 안 함
  - incoming 폴더는 그대로 유지
  - sensor 로그에 `waiting_for_dispatch_params` 기록
- agent ack는 기존 endpoint를 그대로 사용합니다.
  - `status = "accepted"`
  - `message = "waiting_for_dispatch_params"`
- 같은 `request_id` 재전송 시 규칙은 아래로 고정합니다.
  - waiting 상태에서는 DB row가 없으므로 duplicate 처리하지 않음
  - 같은 `request_id`로 유효한 payload가 다시 오면 정상 dispatch로 진행
  - 이미 정상 dispatch가 생성된 뒤에는 기존 duplicate/no-op 규칙 유지
- 진짜 실패는 기존대로 유지합니다.
  - `source_unit_name` 없음 → `rejected`
  - incoming 폴더 없음 → `rejected`
  - payload shape 오류 → `rejected`

## Documentation
- 새 문서 파일 `docs/staging_agent_waiting_dispatch_plan.md`를 생성합니다.
- 문서에는 아래를 명시합니다.
  - waiting 대상 필드 조건
  - waiting 시 manifest/DB/run이 생성되지 않음
  - incoming 폴더는 그대로 유지됨
  - 재개는 같은 `request_id` 재전송
  - ack 예시:
    ```json
    {
      "delivery_id": "dlv_20260326_0001",
      "status": "accepted",
      "request_id": "req_staging_20260326_0001",
      "message": "waiting_for_dispatch_params"
    }
    ```

## Test Plan
- 빈 요청 waiting
  - `source_unit_name`만 있고 나머지 실행 메타가 모두 비어 있으면
  - `accepted + waiting_for_dispatch_params`
  - manifest 미생성
  - RunRequest 미생성
  - `staging_dispatch_requests` / `staging_pipeline_runs` row 없음
- 같은 `request_id` 재전송
  - 첫 요청은 waiting
  - 두 번째 요청에서 `labeling_method` 또는 `outputs/run_mode`와 필요한 메타가 채워지면 정상 dispatch 생성
- 실패 유지
  - `source_unit_name` 없음 → `rejected`
  - incoming 폴더 없음 → `rejected`
- 회귀 확인
  - 유효한 staging API payload는 기존처럼 바로 dispatch 실행
  - production dispatch sensor에는 영향 없음

## Assumptions
- 범위는 `staging only`입니다.
- waiting 판별은 실행 결정을 위한 메타 전체가 비어 있는 경우로 고정합니다.
- waiting 상태는 DB가 아니라 **incoming 폴더 유지 + ack message**로만 표현합니다.
- 별도 UI/DB 조회용 waiting 상태 테이블은 이번 범위에 추가하지 않습니다.
