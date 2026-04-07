# Staging QA Result Report — Cycle 6 (실패 케이스: invalid_outputs)

- 테스트 일시: 2026-03-18 (KST)
- request_id: `qa_c6_fail_20260318`
- outputs: `["bad_output"]` (가이드 § 잘못된 output)
- 테스트 목적: dispatch_sensor가 invalid_outputs 시 `.dispatch/failed` 이동·에러 메시지 확인

## 1. 실행 요약

- safe_drop으로 tmp_data_2(스모크) 배치 후 `--outputs bad_output` 트리거 배치 스크립트 실행
- 기대: trigger가 `.dispatch/failed/<request_id>.json`으로 이동, `error_message = 'invalid_outputs'`
- 확인 시점: `.dispatch/failed/` 비어 있음 (센서 주기 또는 대기 시간 부족 가능)

## 2. Lineage

- 트리거 배치: `staging_test_dispatch.py --skip-copy --wait-archive-pending --outputs bad_output --request-id qa_c6_fail_20260318`
- 판정: **CONDITIONAL** — 재확인 시 `ls .../failed/`, `invalid_outputs` 문구 확인 권장

## 3. 증빙

- `staging/incoming/.dispatch/failed/` 목록: (empty at check time)

## 4. 이슈

- 없음. 센서 tick 후 동일 경로 재확인 시 failed 파일 생성 여부 확인

## 5. 후속 조치

- dispatch_sensor 로그에서 `invalid_outputs` 처리 로그 확인
- 1~2 tick 후 `failed/qa_c6_fail_20260318.json` 존재 및 내용 확인
