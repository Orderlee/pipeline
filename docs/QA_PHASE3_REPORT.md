# Phase 3: Cycle 1-3 — GS건설 Both 모드 + 한글 폴더 Dispatch QA 보고서

**실행일시**: 2026-03-18
**Run ID**: `9d40ec42-cc20-43ae-9860-ea9cf26b50e2`
**Job**: `dispatch_stage_job`
**최종 상태**: SUCCESS

---

## 1. 테스트 목적

- **한글 폴더명** (`GS건설`, `이천_자이더레브-1 B1`, `평촌_자이2단지` 등) 처리 검증
- **Both 모드** (`outputs: ["timestamp", "captioning", "bbox"]`) 전체 파이프라인 실행 검증
- 대용량 비디오(~1GB) MinIO 업로드 안정성 검증
- 중복 파일 탐지(checksum 기반) 정상 동작 검증

---

## 2. 테스트 데이터

| 파일명 | 경로 | 크기 | 결과 |
|--------|------|------|------|
| `2026-02-06-135810_...9.mp4` | `이천_자이더레브-1 B1/Gate3 지능형2/` | 961MB | completed |
| `2026-02-06-135815_...13.mp4` | `이천_자이더레브-1 B1/TC2-2/` | 1,012MB | completed |
| `2026-02-20-132722_...1.mp4` | `평촌_자이2단지/우리병원앞/` | 585MB | duplicate skipped |

---

## 3. 실행 타임라인

| 시각 (UTC) | 이벤트 |
|------------|--------|
| 10:35:19 | RUN_START |
| 10:35:22 | `raw_ingest` STEP_START |
| 10:35:22 | `yolo_image_detection` STEP_START → SUCCESS (408ms, outputs에 bbox 있으나 프레임 미생성 상태라 대상 없음) |
| 10:55:26 | 중복 스킵: `평촌_자이2단지/우리병원앞/...mp4` (기존 asset `8c3d9986`, Phase 1에서 ingest 완료) |
| 11:07:33 | MinIO 업로드 완료: `gsgeonseol/.../gate3_jineunghyeong2/...mp4` (961MB) |
| 11:07:45 | MinIO 업로드 완료: `gsgeonseol/.../tc2-2/...mp4` (1,012MB) |
| 11:08:57 | 중복 스킵 이력 반영: 1건 |
| 11:09:40 | archive `_DONE` marker 생성 |
| 11:09:40 | **INGEST 완료**: total=3, success=2, failed=0, skipped=1 |
| 11:09:42 | `raw_ingest` STEP_SUCCESS (34분 20초) |
| 11:09:46 | `clip_timestamp_routed` STEP_START → "대상 없음" → SUCCESS (123ms) |
| 11:09:46 | `raw_video_to_frame` STEP_START |
| 11:09:50 | `clip_captioning_routed` STEP_START → "스펙 백로그 연동 TODO" → SUCCESS (55ms) |
| 11:09:50 | `clip_to_frame_routed` STEP_START → "스펙 백로그 연동 TODO" → SUCCESS (66ms) |
| 11:11:01 | `raw_video_to_frame` 진행: [1/2] asset=d0fddcfd frames=12 |
| 11:11:25 | `raw_video_to_frame` 진행: [2/2] asset=81a39a40 frames=6 |
| 11:11:25 | **RAW VIDEO EXTRACT 완료**: processed=2, failed=0, frames_extracted=18 |
| 11:11:26 | RUN_SUCCESS (총 36분 6초) |

---

## 4. 검증 결과

### 4.1 DuckDB 상태

| 항목 | 값 |
|------|-----|
| GS건설 raw_files (completed) | 2 |
| GS건설 raw_files (skipped/dup) | 1 (Phase 1 중복) |
| GS건설 video_metadata | 2 |
| GS건설 image_metadata (프레임) | 18 |
| GS건설 labels | 0 |
| 전체 raw_files | 27 |
| 전체 video_metadata | 27 |
| 전체 image_metadata | 18 |

### 4.2 MinIO 객체

| 버킷 | 경로 | 객체 수 |
|------|------|---------|
| `vlm-raw` | `gsgeonseol/icheon_jaideorebeu-1_b1/gate3_jineunghyeong2/` | 1 (961MB) |
| `vlm-raw` | `gsgeonseol/icheon_jaideorebeu-1_b1/tc2-2/` | 1 (1,012MB) |
| `vlm-processed` | `gsgeonseol/.../gate3_jineunghyeong2/image/` | 12 프레임 |
| `vlm-processed` | `gsgeonseol/.../tc2-2/image/` | 6 프레임 |
| `vlm-labels` | `gsgeonseol/` | 0 (timestamp/captioning 미실행) |

### 4.3 파일 시스템

| 경로 | 상태 |
|------|------|
| `/nas/staging/archive/GS건설/` | 2개 비디오 이동 완료 + `_DONE` marker |
| `/nas/staging/archive_pending/GS건설/` | 중복 스킵 파일 1개 잔류 |
| `/nas/staging/incoming/GS건설/` | 없음 (정리 완료) |

### 4.4 한글 경로 처리

| 원본 한글 경로 | 정규화된 MinIO key |
|---------------|-------------------|
| `GS건설` | `gsgeonseol` |
| `이천_자이더레브-1 B1` | `icheon_jaideorebeu-1_b1` |
| `Gate3 지능형2` | `gate3_jineunghyeong2` |
| `TC2-2` | `tc2-2` |

한글→로마자 정규화(sanitizer)가 정상 작동하여 MinIO key가 ASCII-safe로 변환됨.

---

## 5. Asset 실행 분석

### 5.1 raw_ingest — PASS
- 3개 파일 중 2개 성공, 1개 중복 스킵
- 대용량(~1GB) MinIO 업로드 안정적 완료 (파일당 ~12분)
- archive 이동 + `_DONE` marker 생성 정상

### 5.2 clip_timestamp_routed — PARTIAL (대상 없음)
- `outputs` 태그에 `timestamp` 포함 확인 → 스킵 조건 통과
- 그러나 `find_timestamp_pending_by_folder("GS건설")` 결과 대상 0건
- 원인: `raw_files.source_unit_name`이 `GS건설`이지만, `video_metadata.timestamp_status`가 이미 없거나 `pending`이 아닌 상태
- **평가**: 태그 파싱 로직은 정상. 실제 Gemini timestamp 추출은 별도 sensor/trigger 필요

### 5.3 clip_captioning_routed — SKIP (TODO)
- `outputs`에 `captioning` 포함 확인 → 스킵 조건 통과
- 내부 로직이 "스펙 백로그 연동 TODO" 상태
- **평가**: 태그 인식 정상, 실제 captioning 로직 미구현

### 5.4 clip_to_frame_routed — SKIP (TODO)
- `outputs`에 `bbox` 포함 확인 → 스킵 조건 통과
- 내부 로직이 "스펙 백로그 연동 TODO" 상태
- **평가**: 태그 인식 정상, 실제 frame extraction 로직 미구현

### 5.5 raw_video_to_frame — PASS
- 2개 비디오에서 총 18프레임 추출 완료
  - `gate3_jineunghyeong2`: 12프레임 (7,352초 영상)
  - `tc2-2`: 6프레임 (3,703초 영상)
- `vlm-processed`에 JPEG 프레임 업로드 완료
- `video_metadata.frame_extract_status=completed` 갱신 완료

### 5.6 yolo_image_detection — SKIP (대상 없음)
- 실행 시점에 프레임이 아직 추출되지 않아 대상 0건
- raw_ingest와 병렬 실행되어 타이밍 이슈

---

## 6. 발견된 이슈

### 이슈 1: 중복 파일 archive_pending 잔류
- **심각도**: Low
- **설명**: 중복 스킵된 `평촌_자이2단지/우리병원앞/...mp4`가 `archive_pending`에 남아 있음
- **원인**: 중복 파일은 archive로 이동하지 않는 정책이지만, archive_pending에서도 정리되지 않음
- **권장 조치**: 중복 스킵 파일의 archive_pending 정리 로직 추가 검토

### 이슈 2: clip_timestamp_routed 대상 조회 실패
- **심각도**: Medium
- **설명**: `outputs`에 `timestamp`가 있지만 실제 대상 비디오가 0건으로 조회됨
- **원인 추정**: `find_timestamp_pending_by_folder`의 `source_unit_name` 매칭 또는 `timestamp_status` 필터 조건 확인 필요
- **권장 조치**: dispatch flow에서 ingest 직후 timestamp 대상이 올바르게 조회되는지 쿼리 검증

### 이슈 3: yolo_image_detection 타이밍 이슈
- **심각도**: Low
- **설명**: `yolo_image_detection`이 `raw_ingest`와 병렬 실행되어 프레임 추출 전에 완료됨
- **원인**: lineage 상 `yolo_image_detection`이 `raw_video_to_frame` 이후가 아닌 `raw_ingest` 이후에 실행
- **권장 조치**: YOLO가 프레임 추출 완료 후 실행되도록 dependency 조정 검토

### 이슈 4: NFS 기반 대용량 파일 처리 성능
- **심각도**: Info
- **설명**: 1GB 비디오 MinIO 업로드에 ~12분, 파일 복사에 ~33분 소요
- **권장 조치**: 운영 환경에서는 NFS 성능 모니터링 필요

---

## 7. 종합 평가

| 검증 항목 | 결과 |
|-----------|------|
| 한글 폴더명 처리 | **PASS** — sanitizer 정상 동작 |
| dispatch_sensor 트리거 | **PASS** — JSON 감지 → RunRequest 정상 |
| raw_ingest (MinIO 업로드) | **PASS** — 대용량 안정적 처리 |
| 중복 탐지 (checksum) | **PASS** — Phase 1 중복 정확 탐지 |
| archive 이동 + _DONE | **PASS** |
| raw_video_to_frame | **PASS** — 18프레임 추출 완료 |
| clip_timestamp_routed | **PARTIAL** — 태그 파싱 정상, 대상 조회 0건 |
| clip_captioning_routed | **SKIP** — TODO 상태 |
| clip_to_frame_routed | **SKIP** — TODO 상태 |
| yolo_image_detection | **SKIP** — 타이밍 이슈로 대상 없음 |

**전체 판정: PASS (조건부)**
- Ingest + 프레임 추출 핵심 경로는 정상
- Labeling assets(timestamp/captioning/bbox)는 TODO 또는 대상 조회 이슈로 실제 AI 처리 미실행
- Both 모드의 "모든 outputs 실행" 검증은 labeling 로직 완성 후 재검증 필요
