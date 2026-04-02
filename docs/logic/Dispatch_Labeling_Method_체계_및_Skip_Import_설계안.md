# Dispatch labeling_method 체계 및 skip import 설계안

## 목적

- dispatch trigger JSON 및 외부 통신에서 사용하는 `labeling_method` 값을 새 canonical 체계로 정리한다.
- `skip`을 단순 라벨링 생략이 아니라 "이미 존재하는 JSON artifact 적재" 의미로 재정의하기 위한 기준을 문서화한다.
- 본 문서는 추후 구현을 위한 설계안이며, 현재 단계에서는 코드 변경, 스키마 변경, 테스트 변경을 포함하지 않는다.

## 현행 구조 요약

- 현재 dispatch와 downstream asset는 주로 아래 legacy 값을 기준으로 동작한다.
  - `timestamp`
  - `captioning`
  - `bbox`
  - `image_classification`
  - `video_classification`
- 현재 dispatch 정규화 경로는 `labeling_method`, `outputs`, `run_mode`를 받아 내부 `outputs` 문자열과 run tag로 변환한다.
- 현재 `skip`은 실질적으로 `archive_only`와 유사하게 처리되며, 모델 추론 없이 ingest만 수행하는 의미에 가깝다.
- 이미 존재하는 JSON artifact 적재를 위한 경로는 별도로 존재한다.
  - `manual_label_import`
  - `prelabeled_import`
  - `archive_only_artifact_import`
- 따라서 새 계약을 적용하려면 dispatch parser, output helper, ingest import 경로, Gemini/YOLO asset gating을 함께 정리해야 한다.

## 문제 정의

- 외부 계약의 `labeling_method`와 현재 내부 실행 vocabulary가 다르다.
- 현재 `skip`은 "라벨링이 이미 완료된 데이터의 JSON 적재"를 표현하지 못한다.
- `captioning_image`, `classification_video`, `classification_image`는 외부 요구 의미가 정해졌지만 내부 asset graph에는 동일 개념이 직접적으로 존재하지 않는다.
- production과 staging 모두 dispatch를 쓰고 있으므로, 외부 계약은 공통이어야 하고 내부 profile 차이만 유지되어야 한다.

## 새 labeling_method 계약

- 외부 입력 canonical 값은 아래 7개로 고정한다.
  - `timestamp_video`
  - `captioning_video`
  - `captioning_image`
  - `classification_video`
  - `classification_image`
  - `bbox`
  - `skip`
- 위 값은 trigger JSON과 외부 통신의 source of truth로 사용한다.
- 하위 호환 입력은 ingress 단계에서만 새 canonical 값으로 변환한다.
  - `timestamp` -> `timestamp_video`
  - `captioning` -> `captioning_video`
  - `image_classification` -> `classification_image`
  - `video_classification` -> `classification_video`
- `skip`은 다른 method와 혼합하지 않는다.
- 최종적으로 DB의 `outputs`, `labeling_method`, run tag의 `requested_outputs`는 새 canonical 문자열 기준으로 통일한다.

## 모델 라우팅 기준

- Vertex AI / Gemini 사용
  - `timestamp_video`
  - `captioning_video`
  - `captioning_image`
  - `classification_video`
- YOLO-World 사용
  - `bbox`
  - `classification_image`
- `captioning_image`는 raw image caption이 아니라, video 이벤트에서 생성된 대표 frame을 대상으로 하는 image caption으로 정의한다.
- `classification_video`는 video 단위 classification 결과를 생성하는 신규 Gemini 경로가 필요하다.
- `classification_image`는 YOLO detection 결과를 기반으로 이미지-level 다중분류 요약을 생성하는 경로로 정의한다.

## 내부 실행 매핑

- `captioning_video`는 `timestamp_video`를 자동 포함한다.
- `captioning_image`는 `timestamp_video + captioning_video`를 자동 포함한다.
- `bbox`와 `classification_image`는 가능하면 동일 YOLO inference 결과를 재사용한다.
- `classification_video`는 신규 Gemini classification asset 또는 그에 준하는 전용 처리 경로가 필요하다.
- `classification_image`는 YOLO detection 결과를 기반으로 아래 두 결과를 함께 남긴다.
  - 이미지-level 다중분류 요약
  - bbox evidence artifact
- 기존 asset graph는 최대한 재사용하되, 실행 여부 판단과 run tag 해석은 새 canonical method 기준으로 변경한다.

## skip 처리 설계

- `skip`은 더 이상 단순 `archive_only`가 아니다.
- 의미는 "라벨링 모델을 실행하지 않고, incoming 폴더 내부에 이미 존재하는 JSON artifact를 읽어 DB + MinIO에 적재"로 고정한다.
- `skip`에서는 Gemini와 YOLO 추론을 실행하지 않는다.
- raw ingest, archive, local artifact import는 계속 수행한다.
- 구현은 완전 신규 경로보다 아래 기존 import 경로를 재사용 또는 확장하는 방향으로 잡는다.
  - `manual_label_import`
  - `prelabeled_import`
  - `archive_only_artifact_import`
- `skip` import는 폴더 규약 기반과 payload shape 기반을 모두 지원하는 방향으로 설계한다.

## 저장 형식 및 적재 대상

- event/timestamp/caption video JSON
  - `vlm-labels` + `labels`
- bbox JSON
  - `vlm-labels` + `image_labels`
- image caption JSON
  - `vlm-labels` + `image_metadata` caption 계열
- video classification JSON
  - `vlm-labels` + `labels`
- image classification JSON
  - `vlm-labels` + `image_labels`
- `classification_image`는 bbox와 동일 처리로 뭉개지지 않고, "이미지-level 다중분류 요약 + bbox evidence"를 함께 보존하는 형태로 저장한다.

## production / staging 반영 범위

- 외부 계약은 production과 staging에서 동일하게 사용한다.
- profile별 차이는 asset 등록과 catalog 구성 차이만 유지한다.
- 실제 구현 시 함께 정리해야 하는 주요 지점은 아래와 같다.
  - dispatch parser
  - env/output helper
  - ingest import 경로
  - Gemini/YOLO asset gating
- production과 staging의 내부 asset 조합이 다를 수 있어도, 외부에서 보는 `labeling_method` 해석 규칙은 동일해야 한다.

## 테스트 시나리오

- 새 canonical `labeling_method` 7개가 모두 정상 파싱되는지 확인
- legacy alias가 ingress에서 새 canonical 값으로 변환되는지 확인
- `captioning_video` 요청 시 `timestamp_video`가 자동 포함되는지 확인
- `captioning_image` 요청 시 `timestamp_video + captioning_video`가 자동 포함되는지 확인
- `bbox`와 `classification_image`가 YOLO 계열로 함께 판정되는지 확인
- `classification_video`가 Gemini classification 경로로 라우팅되는지 확인
- `skip`이 archive-only가 아니라 artifact import 의미로 처리되는지 확인
- `skip`에서 event/bbox/image_caption/video_classification/image_classification JSON이 각각 올바른 저장 대상으로 적재되는지 확인
- production/staging 모두 동일 외부 계약을 해석하는지 확인

## 추후 구현 순서

1. dispatch 입력 정규화와 canonical `labeling_method` 변환 로직 정리
2. run tag / DB 저장값을 새 canonical vocabulary로 통일
3. Gemini 경로에서 `timestamp_video`, `captioning_video`, `captioning_image`, `classification_video` gating 반영
4. YOLO 경로에서 `bbox`, `classification_image` gating 반영 및 image-level classification summary 저장
5. `skip`을 기존 import 경로 재사용 방식으로 확장
6. production/staging definitions와 테스트를 새 계약 기준으로 정리

## 확정한 기본 가정

- 본 문서는 추후 구현 기준서이며 현재 코드 동작을 즉시 바꾸지 않는다.
- `captioning_image`는 이벤트 기반 frame caption을 의미한다.
- `classification_video`는 Gemini 기반으로 구현한다.
- `classification_image`는 YOLO-World detection 기반의 이미지-level 다중분류 요약으로 구현한다.
- `skip`은 incoming 내부 기존 JSON artifact import를 의미하며, 단순 archive-only 의미로 사용하지 않는다.
- README나 다른 문서는 이번 단계에서 수정하지 않는다.
