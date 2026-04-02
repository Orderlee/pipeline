# Staging QA Result Report — Cycle 4 (bbox + captioning)

- 테스트 일시: 2026-03-18 (KST)
- 테스트 목적: raw_ingest → raw_video_to_frame + clip_timestamp → clip_captioning → clip_to_frame → yolo_image_detection

## 1. 실행 요약

- **스킵**: 사이클 2·3과 동일 스모크·run_cycle 패턴으로 실행 가능
- 실행 예: `QA_DROP_SOURCE=... bash scripts/staging_qa_run_cycle.sh tmp_data_2 qa_c4_20260318 bbox captioning`
- 최종 판정: **DEFERRED** (가이드 순서상 cycle 3 다음, 동일 절차로 추후 실행)

## 2. Lineage

- 기대: bbox 경로(frame_extract, image_metadata raw_video_frame, image_labels) + captioning 경로(labels, processed_clips) 동시 생성

## 3. 후속 조치

- 필요 시 cleanup 후 위 명령으로 실행, `staging_qa_wait_expect.py --mode bbox_cap` 로 완료 여부 확인
