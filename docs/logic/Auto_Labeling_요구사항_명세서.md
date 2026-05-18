# Auto Labeling 요구사항 명세서

## 1. 목적

- 데이터 인입 후 수작업을 최소화하고 AI 모델을 활용한 1차 자동 라벨링을 수행하여 학습 데이터 생산 속도를 높인다.
    - 수작업 대상
        - 이벤트 구간 확인 및 추출(Timestamp Labeling)
        - 이벤트 구간 캡션 삽입(Captioning)
        - 해당 영상에서 이미지 추출(Clip → Frame)
        - 추출된 이미지에서 객체 라벨링(Bbox Labeling)

## 2. 범위

- [포함] : timestamp 식별, captioning, frame 추출, Bbox 생성, Config 기반 파라미터 관리
- [미포함] : 모델 학습 및 배포, 수동 라벨링 도구(CVAT/Label Studio 등) 연동

## 3. 사용자

- AI Engineer : Config 요청, 결과 확인
- Data Engineer : 파이프라인 운영, Config 관리
- PM : Pending 현황, 인입 Spec 현황, api 비용/시간 등 운영 및 모니터링 관리

## 4. 기능 요구사항

- **`[Spec]*`**
    - FR-001 : 데이터 인입 시 spec.json이 함께 수신되어 labeling_specs 테이블에 저장되어야 한다.
    - FR-002 : spec의 labeling_method 등 메타를 반영하여 auto_labeling을 수행할 수 있어야 한다. **표준 auto_labeling은 timestamp → captioning → frame → bbox 순으로 순차 실행되며, captioning 단계는 생략하지 않는다.**
    - FR-003 : labeling_method가 미정인 경우 pending으로 분류되어야 한다.
    - FR-004 : pending spec의 labeling 정의가 확정되면 auto_labeling이 자동으로 트리거되어야 한다.
- **`[Pending 해소]*`**
    - FR-005 : Slack 봇으로 pending spec의 labeling 정의를 확정할 수 있어야 한다.
    - FR-006 : Slack 봇으로 현재 pending 목록을 조회할 수 있어야 한다.
    - FR-007 : Slack 봇으로 특정 spec의 처리 상태를 조회할 수 있어야 한다.
- **`[Config]*`**
    - FR-008 : requester별 config를 등록하고 조회할 수 있어야 한다.
    - FR-009 : config/parameters/ 폴더에 JSON 파일을 추가하면 DB에 동기화되어야 한다.(수동)
    - FR-010 : config 등록 없이도 _fallback(default 값)으로 처리가 가능해야 한다.
- **`[Auto Labeling Task]*`**
    - FR-011 : timestamp 식별 — 영상에서 이벤트 구간의 시작/끝 timestamp를 AI 모델로 자동 식별할 수 있어야 한다.
    - FR-012 : captioning — 식별된 이벤트 구간에 대해 AI 모델로 캡션을 생성할 수 있어야 한다. (frame 추출 전 단계)
    - FR-013 : frame 추출 — 이벤트 구간을 clip으로 절단하고 config에 정의된 간격으로 frame 이미지를 추출할 수 있어야 한다.
    - FR-014 : bbox labeling — 추출된 frame 이미지에서 객체를 감지하고 AI 모델로 bounding box를 생성할 수 있어야 한다.
- **`[결과 저장]*`**
    - FR-015 : auto_labeling 결과(labels, processed_clips, frames, bbox)는 DuckDB + MinIO에 저장되어야 한다.
    - FR-016 : 처리 현황(성공/실패/pending 건수)을 MotherDuck 대시보드에서 조회할 수 있어야 한다
- **`[재처리]*`**
    - FR-017 : 동일 데이터를 다른 config로 재처리할 수 있어야 한다.
    - FR-018 : 재처리 시 기존 결과는 보존되고 새 버전으로 저장되어야 한다.
