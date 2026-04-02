# Staging QA Result Report — Cycle 3 (bbox)

- 테스트 일시: 2026-03-18 (KST)
- request_id: `qa_c3_20260318`
- folder_name: `tmp_data_2`
- outputs: `["bbox"]`
- 테스트 목적: raw_ingest → raw_video_to_frame → yolo_image_detection

## 1. 실행 요약

- safe_drop: 완료 (cp -r NFS 대응 반영)
- archive_pending 대기: ~11s
- trigger processed: ~44s
- Dagster run: dispatch_stage_job 실행 (status=running)
- 최종 판정: **CONDITIONAL** (dispatch·센서 정상, bbox 파이프라인 완료는 별도 확인)

## 2. Lineage

- incoming → archive_pending → dispatch processed OK. 조회 시점 raw_files/frame_extract/raw_video_frame 0 (진행 중 또는 YOLO/프레임 추출 대기).

## 3. 증빙

- 터미널: `[OK] archive_pending 확인 (11s)`, `[OK] dispatch 처리 완료 → processed (44s)`
- DB: raw_files tmp_data_2 0 (추가 폴링 또는 Dagster run 로그로 완료 여부 확인 권장)

## 4. 이슈

- 없음 (safe_drop cp -a → cp -r 변경으로 NFS 시간 보존 오류 회피)

## 5. 후속 조치

- bbox 경로 완료 시 `staging_qa_wait_expect.py --mode bbox` 로 재검증
