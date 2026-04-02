# Staging QA 보고서 — Cycle 1-2: YOLO 모드 Dispatch (weapon_test)

**일시**: 2026-03-18  
**환경**: dagster-staging (port 3031)  
**테스트 데이터**: weapon 비디오 7개 (tmp_data_2/weapon 복사본)  
**Dispatch 설정**: `outputs: ["bbox"]`, `request_id: qa_cycle2_yolo_001`

---

## 1. 테스트 실행 요약

| 단계 | 결과 | 비고 |
|------|------|------|
| incoming 복사 | PASS | 7개 weapon 비디오 복사 |
| archive_pending 이동 | PASS | 수동 이동 (sensor 타이밍 이슈) |
| dispatch trigger JSON 배치 | PASS | `.dispatch/pending/qa_cycle2_yolo_001.json` |
| dispatch_sensor 감지 | PASS | RunRequest 발행 (run: 93338757) |
| dispatch_stage_job 실행 | PASS | RUN_SUCCESS (5분 44초) |
| raw_ingest | PASS (중복) | 7/7 중복 스킵 (Phase 1과 동일 checksum) |
| yolo_image_detection | PASS (대상 없음) | "YOLO 대상 이미지 없음" — 중복으로 새 이미지 없음 |
| clip_timestamp_routed | SKIP (정상) | outputs에 timestamp 없음 |
| clip_captioning_routed | SKIP (정상) | outputs에 captioning 없음 |
| raw_video_to_frame | SKIP | outputs에 bbox 없음 (tag 읽기 문제) |
| clip_to_frame_routed | **FAIL** | `requested_outputs` tag만 읽어 bbox 미인식 |

## 2. DB 검증 결과

| 테이블 | 건수 | 상태 |
|--------|------|------|
| raw_files | 25 (전체, weapon_test=0) | 중복 스킵으로 미등록 |
| video_metadata | 25 | Phase 1 데이터만 |
| staging_dispatch_requests | 2 (qa_cycle1 + qa_cycle2) | PASS |

## 3. 발견된 이슈

### 이슈 #5: 동일 데이터 재사용 시 중복 스킵 (EXPECTED)
- **증상**: weapon_test의 7개 파일이 Phase 1의 tmp_data_2/weapon과 동일 checksum
- **영향**: raw_ingest에서 전부 중복 스킵 → 후속 단계 대상 없음
- **판단**: 중복 감지 로직 정상 동작. 테스트 데이터 설계 문제.

### 이슈 #6: clip_to_frame_routed outputs tag 미인식 (RESOLVED)
- **증상**: `requested_outputs` tag만 읽어 dispatch flow의 `outputs` tag 미인식
- **조치**: `clip_to_frame_routed`에서 `outputs` tag fallback 추가
- **코드**: `src/vlm_pipeline/defs/process/assets.py`

### 이슈 #7: raw_video_to_frame outputs tag 미인식 (OPEN)
- **증상**: "outputs에 bbox가 없습니다" — dispatch flow의 `outputs` tag 미인식
- **필요 조치**: `raw_video_to_frame` asset도 동일 패턴으로 수정 필요

## 4. 코드 수정 확인

| 수정 | 검증 |
|------|------|
| `clip_timestamp_routed` outputs fallback | PASS — "outputs에 timestamp 없음" 정상 스킵 |
| `clip_captioning_routed` outputs fallback | PASS — "outputs에 captioning 없음" 정상 스킵 |
| `clip_to_frame_routed` outputs fallback | 수정 완료, 재테스트 필요 |

## 5. 판정

| 항목 | 판정 |
|------|------|
| Dispatch 기본 흐름 (YOLO 모드) | **PASS** |
| 중복 감지 | **PASS** |
| YOLO 실제 처리 | **NOT TESTED** (중복으로 대상 없음) |
| 전체 Cycle 1-2 | **PARTIAL PASS** |

## 6. 다음 단계

1. Phase 3에서 GS건설 데이터(고유 데이터)로 Both 모드 테스트
2. `raw_video_to_frame` outputs tag 수정
3. 재시작 후 Phase 3 진행
