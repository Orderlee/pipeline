# Label Storage Policy

## 목적

`vlm-labels` 버킷 안에서 Gemini 이벤트 JSON과 YOLO detection JSON의 경로 규칙을 일관되게 유지한다.

- Gemini 이벤트 JSON은 `events/`
- 이미지 detection JSON은 `detections/`

기준으로 분리한다.

## 버킷별 역할

- `vlm-raw`
  - 원본 이미지/비디오 저장
- `vlm-processed`
  - clip 영상, 추출 이미지 저장
- `vlm-labels`
  - Gemini 이벤트 JSON, YOLO detection JSON 저장

## 표준 경로 정책

### 1. Gemini 이벤트 JSON

- 버킷: `vlm-labels`
- 경로 규칙:
  - `vlm-labels/<raw_parent>/events/<video_stem>.json`
- 기준:
  - `raw_parent`는 원본 `raw_key`의 parent prefix를 그대로 사용
  - `video_stem`은 원본 영상 파일명에서 확장자를 제거한 이름

예:
- raw key:
  - `tmp_data/fall_down/20250711_am_rs_ld_2.mp4`
- event JSON:
  - `vlm-labels/tmp_data/fall_down/events/20250711_am_rs_ld_2.json`

### 2. YOLO detection JSON

- 버킷: `vlm-labels`
- 경로 규칙:
  - `vlm-labels/<raw_parent>/detections/<image_or_clip_stem>.json`
- 기준:
  - `raw_parent`는 원본 `raw_key`의 parent prefix를 그대로 사용
  - `image_or_clip_stem`은 detection 대상 이미지 파일명 또는 clip 기반 이미지 파일명에서 확장자를 제거한 이름

예:
- image key:
  - `vlm-processed/tmp_data/fall_down/image/20250711_am_rs_ld_2_e001_00010000_00012000_00000001.jpg`
- detection JSON:
  - `vlm-labels/tmp_data/fall_down/detections/20250711_am_rs_ld_2_e001_00010000_00012000_00000001.json`

## 네이밍 규칙

### Gemini 이벤트 JSON

- 원본 영상 1개당 JSON 파일 1개
- 하나의 JSON 안에 이벤트 리스트를 저장
- 여러 이벤트가 있더라도 파일은 1개만 생성

예:
- `20250711_am_rs_ld_2.json`

### YOLO detection JSON

- detection 대상 이미지 1개당 JSON 파일 1개
- clip에서 여러 이미지가 추출되면 이미지 수만큼 JSON 생성

예:
- `20250711_am_rs_ld_2_e001_00010000_00012000_00000001.json`
- `20250711_am_rs_ld_2_e001_00010000_00012000_00000002.json`

## 예시

### 예시 1. raw video -> Gemini 이벤트 JSON

- raw:
  - `vlm-raw/tmp_data/fall_down/20250711_am_rs_ld_2.mp4`
- labels:
  - `vlm-labels/tmp_data/fall_down/events/20250711_am_rs_ld_2.json`

### 예시 2. raw video -> clip 생성

- raw:
  - `vlm-raw/adlib-hotel-202512/20251222/smoke_1_02002.mp4`
- processed clip:
  - `vlm-processed/adlib-hotel-202512/20251222/clips/smoke_1_02002_e001_00000000_00002000.mp4`

### 예시 3. clip image -> YOLO detection JSON

- processed image:
  - `vlm-processed/adlib-hotel-202512/20251222/image/smoke_1_02002_e001_00000000_00002000_00000001.jpg`
- labels:
  - `vlm-labels/adlib-hotel-202512/20251222/detections/smoke_1_02002_e001_00000000_00002000_00000001.json`

### 예시 4. 다른 이벤트 구간

- processed clip:
  - `vlm-processed/tmp_data/violence/clips/20250718_pm_al_sam_5_e002_00015000_00022000.mp4`
- processed image:
  - `vlm-processed/tmp_data/violence/image/20250718_pm_al_sam_5_e002_00015000_00022000_00000001.jpg`
- labels:
  - `vlm-labels/tmp_data/violence/detections/20250718_pm_al_sam_5_e002_00015000_00022000_00000001.json`

### 예시 5. Gemini 이벤트 JSON과 detection JSON의 공존

- event JSON:
  - `vlm-labels/tmp_data/smoking/events/20250814_am_be_dhc_1.json`
- detection JSON:
  - `vlm-labels/tmp_data/smoking/detections/20250814_am_be_dhc_1_e001_00005000_00008000_00000001.json`

### 예시 6. raw parent 유지 예시

- raw:
  - `vlm-raw/kkpolice-event-bucket/2026-01-26/violence_229_438.mp4`
- event JSON:
  - `vlm-labels/kkpolice-event-bucket/2026-01-26/events/violence_229_438.json`
- detection JSON:
  - `vlm-labels/kkpolice-event-bucket/2026-01-26/detections/violence_229_438_e001_00012000_00015000_00000001.json`

## `labels` 테이블 매핑 규칙

`labels`는 비디오 이벤트 라벨의 source of truth로 사용한다.

### 저장 대상

- Gemini가 생성한 이벤트 단위 라벨 row

### 기본 매핑

- `asset_id`
  - 원본 `raw_files.asset_id`
- `labels_bucket`
  - `vlm-labels`
- `labels_key`
  - `<raw_parent>/events/<video_stem>.json`
- `label_format`
  - `gemini_event_json`
- `label_tool`
  - `gemini`
- `label_source`
  - `auto`
- `review_status`
  - `auto_generated`
- `event_index`
  - JSON 배열 내 이벤트 순서
- `event_count`
  - 해당 JSON의 전체 이벤트 개수
- `timestamp_start_sec`
  - JSON `timestamp[0]`
- `timestamp_end_sec`
  - JSON `timestamp[1]`
- `caption_text`
  - `ko_caption` 우선, 없으면 `en_caption`
- `object_count`
  - 기본 `0`
- `label_status`
  - `completed`

### 중요 규칙

- 영상 1개당 JSON 파일은 1개여도, `labels` 테이블에는 이벤트 수만큼 row가 들어간다.
- 같은 영상의 여러 이벤트 row는 동일한 `labels_key`를 공유한다.

## `image_labels` 테이블 매핑 규칙

`image_labels`는 이미지 단위 detection 결과의 source of truth로 사용한다.

### 저장 대상

- YOLO-World가 생성한 이미지별 detection 결과 row

### 기본 매핑

- `image_id`
  - `image_metadata.image_id`
- `source_clip_id`
  - clip 기반 이미지면 `processed_clips.clip_id`
- `labels_bucket`
  - `vlm-labels`
- `labels_key`
  - `<raw_parent>/detections/<image_or_clip_stem>.json`
- `label_format`
  - `yolo_detection_json`
- `label_tool`
  - `yolo-world`
- `label_source`
  - `auto`
- `review_status`
  - `auto_generated`
- `label_status`
  - `completed`
- `object_count`
  - detection JSON 안의 bounding box 개수

### 중요 규칙

- detection JSON은 이미지 1개당 1개 파일을 기준으로 한다.
- clip에서 파생된 이미지가 여러 장이면 `image_labels`도 이미지 수만큼 row가 생성된다.
- `labels`와 달리 `image_labels`는 이벤트 row가 아니라 이미지 row 기준이다.

## 운영 기준

- `vlm-labels` 안에서는 `events/`와 `detections/`만 사용한다.
- `json/`, `od/`, `yolo/`처럼 의미가 겹치거나 축약된 폴더명은 사용하지 않는다.
- `raw_parent`는 원본 `vlm-raw` prefix를 그대로 유지한다.
- Gemini와 YOLO 결과는 같은 버킷을 사용하되, 폴더 역할을 명확히 구분한다.

## 향후 확장 규칙

추후 detection 모델이 늘어나면 `detections/` 아래에 모델명을 한 단계 더 둘 수 있다.

예:
- `vlm-labels/<raw_parent>/detections/yolo-world/<image_stem>.json`
- `vlm-labels/<raw_parent>/detections/owlvit/<image_stem>.json`

현재 표준안은 다음 두 경로를 기본으로 유지한다.

- `vlm-labels/<raw_parent>/events/<video_stem>.json`
- `vlm-labels/<raw_parent>/detections/<image_or_clip_stem>.json`
