# Staging QA Result Report — Cycle 1 (timestamp)

- 테스트 일시: 2026-03-18 (KST)
- 테스트 담당자: 자동 QA (safe_drop → archive_pending → 트리거 1회)
- request_id: `qa_c1_20260318`
- folder_name: `tmp_data_2`
- outputs: `["timestamp"]`
- 테스트 목적: dispatch + raw_ingest + clip_timestamp(Gemini) 단독

## 1. 실행 요약

- trigger JSON 생성: 예 (`.dispatch/pending` → processed 이동 ~52s)
- Dagster run: `dispatch_stage_job` 실행 (UI/로그: dagster-staging)
- 최종 판정: **PASS**

## 2. Lineage 확인 결과

| 항목 | 결과 |
|------|------|
| incoming atomic 배치 | `_qa_incoming_build` → `incoming/tmp_data_2` mv |
| archive_pending 이동 | ~36s 후 확인 |
| dispatch manifest / processed 트리거 | processed |
| raw_files + video auto_label_status=generated | 1/1 비디오 |
| labels/processed_clips 증가 | 없음 (timestamp 단독 기대와 일치) |

## 3. 증빙

- 스모크 데이터: `QA_DROP_SOURCE=_qa_smoke_tmp_data_2` (단일 mp4, 전체 tmp_data_2 대체)
- 폴링: `python3 scripts/staging_qa_wait_expect.py --mode timestamp` → `videos:1, generated:1`
- Dagster: `pipeline-dagster-staging-1` SensorDaemon (dispatch_sensor, incoming_to_pending_sensor)

## 4. 이슈

- 전체 `tmp_data_2` 복사는 NFS에서 수십 분 소요 → 스모크 폴더로 대체 (`QA_DROP_SOURCE`, safe_drop 내부 `cp .../.` 수정)

## 5. 후속 조치

- 전량 회귀 시 원본 `tmp_data_2`로 야간 배치 권장
