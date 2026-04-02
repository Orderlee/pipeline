# Staging QA Result Report — Cycle 2 (timestamp + captioning)

- 테스트 일시: 2026-03-18 (KST)
- 테스트 담당자: 자동 QA
- request_id: `qa_c2_20260318`
- folder_name: `tmp_data_2`
- outputs: `["timestamp", "captioning"]`
- 테스트 목적: raw_ingest → clip_timestamp → clip_captioning → clip_to_frame

## 1. 실행 요약

- trigger: processed (~61s)
- Dagster run: dispatch_stage_job 실행
- 최종 판정: **CONDITIONAL PASS** (lineage 일부 미완료)

## 2. Lineage 확인 결과

| 항목 | 결과 |
|------|------|
| incoming / archive_pending | OK |
| dispatch processed | OK |
| raw_files (tmp_data_2) | 1 |
| video_metadata.auto_label_status | `completed` (1) |
| labels | 0 (기대: >0) |
| processed_clips | 0 (기대: >0) |

captioning까지 완료( auto_label_status=completed )되었으나 labels / processed_clips 미생성. clip_to_frame 또는 labels 적재 경로 확인 필요.

## 3. 증빙

- DB: `labels`, `processed_clips` tmp_data_2 기준 0건
- wait_expect ts_cap: 7200s 타임아웃

## 4. 이슈

- captioning 경로에서 labels·processed_clips 생성 로직/조건 점검 필요

## 5. 후속 조치

- dispatch_stage_job 로그에서 clip_captioning / clip_to_frame 단계 성공 여부 확인
- labels·processed_clips 미생성 원인 조사
