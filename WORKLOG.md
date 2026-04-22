# WORKLOG












## 2026-04-21

### 1. 프레임 추출 안정화 및 이미지 캡션 메타 확장
- **문제**: 영상 말단 구간에서 프레임 추출이 비거나 불안정할 수 있었고, 이미지 캡션 결과도 텍스트만 남아 후속 추적에 필요한 저장 메타가 부족했음.
- **원인**: ffmpeg 프레임 추출은 요청 시점이 영상 끝에 가까우면 empty output이 날 수 있었고, image caption 저장은 bucket/key/generated_at 같은 정본 추적 필드가 충분하지 않았음.
- **조치**:
    - ffmpeg frame extract에 fallback seek 후보와 retry 흐름을 넣어 말단 구간 empty output 상황을 완화함.
    - image caption JSON을 별도 key로 저장하고 bucket, key, 생성 시각 메타를 image/process 경로에 함께 기록하도록 확장함.
    - process 중 실패 시 업로드한 frame/image caption JSON을 함께 정리하도록 rollback 경로도 보강함.
    - 관련 파일:
      - `src/vlm_pipeline/sql/schema.sql`

### 2. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.
    - 관련 파일:
      - `docker/docker-compose.yaml`

### 3. 당일 정리
- **변경 통계**:
    - 변경 파일 **16개**, +2366/-330줄.
- **관련 커밋**:
    - `df2811e0`: feat(ls): labeling_method + categories 기반 project 카테고리별 자동 분할
    - `6a6d028e`: chore(compose): dagster 서비스에 외부 pipeline-network 연결
    - `b9299d9d`: feat(ls): image mode(SAM3 COCO ↔ RectangleLabels) + Dagster GraphQL 트리거 + sensor prefix 수정
    - `2f9d6448`: feat(build): 프로젝트별 타임스탬프+Bbox 데이터셋 빌드 + classification asset 신설
    - `ccccda81`: refactor(pipeline): dispatch_stage → post_review_clip_job 분리 + SAM3 공통 로직 추출
- **서비스 상태**: 파이프라인 서비스 3개 컨테이너 중 3개 정상 가동.
- **작업 환경**: Cursor, VSCode

## 2026-04-20

### 1. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.
    - 관련 파일:
      - `docker/docker-compose.yaml`

### 2. 당일 정리
- **변경 통계**:
    - 변경 파일 **17개**, +513/-62줄.
- **관련 커밋**:
    - `d3a5ea66`: fix(dispatch): started_at IS NULL step을 'skipped'로 마감
    - `b2cf2185`: feat(ls): folder 정규화 + attach-predictions 명령 추가
    - `69781984`: feat(sam3): primary bbox 엔진 전환 — 항상 기동 + 미준비 시 Failure + bbox_status 전이
    - `14c89ce7`: feat(gemini): structured output schema + JSON repair/retry + credentials fallback 상세화
    - `7e4395db`: feat(validator): macOS 메타파일 필터 + ingest/sensor scan 적용
- **서비스 상태**: 파이프라인 서비스 3개 컨테이너 중 3개 정상 가동.
- **작업 환경**: Cursor, VSCode

## 2026-04-17

### 1. 프레임 추출 안정화 및 이미지 캡션 메타 확장
- **문제**: 영상 말단 구간에서 프레임 추출이 비거나 불안정할 수 있었고, 이미지 캡션 결과도 텍스트만 남아 후속 추적에 필요한 저장 메타가 부족했음.
- **원인**: ffmpeg 프레임 추출은 요청 시점이 영상 끝에 가까우면 empty output이 날 수 있었고, image caption 저장은 bucket/key/generated_at 같은 정본 추적 필드가 충분하지 않았음.
- **조치**:
    - ffmpeg frame extract에 fallback seek 후보와 retry 흐름을 넣어 말단 구간 empty output 상황을 완화함.
    - image caption JSON을 별도 key로 저장하고 bucket, key, 생성 시각 메타를 image/process 경로에 함께 기록하도록 확장함.
    - process 중 실패 시 업로드한 frame/image caption JSON을 함께 정리하도록 rollback 경로도 보강함.
    - 관련 파일:
      - `src/vlm_pipeline/sql/schema.sql`

### 2. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor_run_status.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`

### 3. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.
    - 관련 파일:
      - `docker/docker-compose.yaml`

### 4. 당일 정리
- **변경 통계**:
    - 변경 파일 **63개**, +10478/-8708줄.
- **관련 커밋**:
    - `8745e4fb`: chore(docker): permission 격리 — DOCKER_USER·호스트 bind 경로 파라미터화 + dagster runtime state 추적 제외
    - `d1f08d45`: merge: dev → main — refactor 배치 + gitignore 보강
    - `19ec9406`: chore(gitignore): 날짜 suffix 1회성 recovery 스크립트 추적 제외
    - `d34d8a75`: refactor(ingest): assets.py·archive.py 분할 (orchestration 로직 유지)
    - `05d57b76`: refactor(ingest): ops.py·sensor_bootstrap.py 분할 (cursor 포맷 바이트 보존)
- **서비스 상태**: 파이프라인 서비스 3개 컨테이너 중 3개 정상 가동.
- **작업 환경**: Cursor, VSCode

## 2026-04-16

### 1. 운영 ingest / process 공통 구조 정리
- **문제**: ingest와 process 공통 책임이 여러 모듈로 흩어져 있어, 운영 로직을 수정할 때 영향 범위와 설정 반영 지점을 한 번에 보기 어려웠음.
- **원인**: raw ingest 실행 흐름, runtime env 해석, DuckDB schema 보정과 조회 helper가 각각 다른 레이어에 퍼져 있어 공통 동작을 건드릴 때 수정 포인트가 많아졌음.
- **조치**:
    - raw ingest에서 상태 생성과 실제 실행 파이프라인을 분리해 진입 구조를 명확히 정리함.
    - runtime 설정 로더를 추가하고 ingest runtime policy, stuck guard가 같은 설정 해석 경로를 재사용하도록 맞춤.
    - DuckDB resource에 dispatch/model/raw/image 조회 및 insert helper를 보강하고 schema ensure 흐름을 운영 기준으로 정돈함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/ingest/assets.py`
      - `src/vlm_pipeline/defs/ingest/runtime_policy.py`
      - `src/vlm_pipeline/resources/runtime_settings.py`

### 2. 프레임 추출 안정화 및 이미지 캡션 메타 확장
- **문제**: 영상 말단 구간에서 프레임 추출이 비거나 불안정할 수 있었고, 이미지 캡션 결과도 텍스트만 남아 후속 추적에 필요한 저장 메타가 부족했음.
- **원인**: ffmpeg 프레임 추출은 요청 시점이 영상 끝에 가까우면 empty output이 날 수 있었고, image caption 저장은 bucket/key/generated_at 같은 정본 추적 필드가 충분하지 않았음.
- **조치**:
    - ffmpeg frame extract에 fallback seek 후보와 retry 흐름을 넣어 말단 구간 empty output 상황을 완화함.
    - image caption JSON을 별도 key로 저장하고 bucket, key, 생성 시각 메타를 image/process 경로에 함께 기록하도록 확장함.
    - process 중 실패 시 업로드한 frame/image caption JSON을 함께 정리하도록 rollback 경로도 보강함.
    - 관련 파일:
      - `src/vlm_pipeline/sql/schema.sql`

### 3. 수동 / 사전 라벨 import 경로 정리
- **문제**: 수동 라벨 import와 사전 라벨링 데이터 적재가 서로 다른 코드 경로에서 중복 구현되어 있었고, staging에서 기존 라벨 결과를 재적재하는 전용 진입점도 부족했음.
- **원인**: event label JSON 파싱, raw asset 매칭, label key 생성 로직이 manual import 안에 묶여 있었고, prelabeled 데이터는 raw ingest 이후 bbox/image caption까지 연결하는 공통 흐름이 정리되지 않았음.
- **조치**:
    - manual label import에서 공통 helper를 분리해 label 파일 순회, asset 매칭, event 추출, label 저장 규칙을 재사용 가능하게 정리함.
    - staging 전용 prelabeled import job을 추가해 raw ingest 후 event / bbox / image caption artifact import까지 한 경로에서 처리하게 구성함.
    - 사전 라벨링 이미지명에서 원본 raw stem을 찾는 정규식 패턴을 보강해 stem 추론 누락 케이스를 보완함.
    - 관련 파일:
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/label/prelabeled_import.py`

### 4. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor_run_status.py`
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`
      - `src/vlm_pipeline/lib/staging_dispatch.py`

### 5. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.
    - 관련 파일:
      - `docker/.env.staging`
      - `docker/docker-compose.yaml`
      - `src/vlm_pipeline/defs/ingest/runtime_policy.py`
      - `src/vlm_pipeline/resources/runtime_settings.py`

### 6. 운영 / 에이전트 문서 보강
- **문제**: 운영과 staging 구분, 파이프라인 흐름, 자동화 에이전트 참고 문서가 서로 다른 수준으로 흩어져 있어 작업자마다 참조 경로가 달라질 수 있었음.
- **원인**: 짧은 운영 컨텍스트와 장문 레퍼런스, 외부 에이전트용 진입 문서가 분리되어 있었지만 최근 변경사항이 한 번에 정리돼 있지 않았음.
- **조치**:
    - Codex/에이전트용 `AGENTS.md`를 추가해 운영 규칙, staging/prod 차이, 자주 쓰는 명령을 빠르게 확인할 수 있게 정리함.
    - `ANTIGRAVITY.md`와 `CLAUDE2.md`를 보강해 에이전트 진입점과 장문 레퍼런스를 역할별로 구분해 둠.
    - 관련 파일:
      - `AGENTS.md`

### 7. 당일 정리
- **변경 통계**:
    - 변경 파일 **121개**, +4680/-7673줄.
- **관련 커밋**:
    - `2c427b95`: merge: dev → main — CI/CD 통합 + 배포 안정화 + 리팩터링
    - `ca2e59ef`: merge: origin/main → dev — Claude App 자동생성 워크플로우 충돌 해소
    - `2c7299bf`: fix: compose 프로젝트명 충돌 방지 및 deploy 안전 재시작
    - `9d3fb43a`: refactor: build_asset_job + detection asset factory 통합 (Phase 3)
    - `22788cc6`: refactor: 네이밍 정리 — definitions_production / 잔재 명칭 정정 (Phase 2)
- **서비스 상태**: 파이프라인 서비스 3개 컨테이너 중 3개 정상 가동.
- **작업 환경**: Antigravity, Cursor, VSCode

## 2026-04-15

### 1. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.

### 2. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.

### 3. 당일 정리
- **변경 통계**:
    - 변경 파일 **42개**, +26/-2120줄.
- **관련 커밋**:
    - `de4b2953`: chore: 미사용 test·dispatch·staging 잔재 파일 정리
    - `38b6391f`: fix: Dagster workspace_prod location_name을 prod_defs로 확정
- **서비스 상태**: 파이프라인 서비스 3개 컨테이너 중 3개 정상 가동.
- **작업 환경**: Antigravity, Cursor, VSCode

## 2026-04-14

### 1. 운영 ingest / process 공통 구조 정리
- **문제**: ingest와 process 공통 책임이 여러 모듈로 흩어져 있어, 운영 로직을 수정할 때 영향 범위와 설정 반영 지점을 한 번에 보기 어려웠음.
- **원인**: raw ingest 실행 흐름, runtime env 해석, DuckDB schema 보정과 조회 helper가 각각 다른 레이어에 퍼져 있어 공통 동작을 건드릴 때 수정 포인트가 많아졌음.
- **조치**:
    - raw ingest에서 상태 생성과 실제 실행 파이프라인을 분리해 진입 구조를 명확히 정리함.
    - runtime 설정 로더를 추가하고 ingest runtime policy, stuck guard가 같은 설정 해석 경로를 재사용하도록 맞춤.
    - DuckDB resource에 dispatch/model/raw/image 조회 및 insert helper를 보강하고 schema ensure 흐름을 운영 기준으로 정돈함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/ingest/assets.py`
      - `src/vlm_pipeline/defs/ingest/runtime_policy.py`
      - `src/vlm_pipeline/resources/runtime_settings.py`

### 2. 프레임 추출 안정화 및 이미지 캡션 메타 확장
- **문제**: 영상 말단 구간에서 프레임 추출이 비거나 불안정할 수 있었고, 이미지 캡션 결과도 텍스트만 남아 후속 추적에 필요한 저장 메타가 부족했음.
- **원인**: ffmpeg 프레임 추출은 요청 시점이 영상 끝에 가까우면 empty output이 날 수 있었고, image caption 저장은 bucket/key/generated_at 같은 정본 추적 필드가 충분하지 않았음.
- **조치**:
    - ffmpeg frame extract에 fallback seek 후보와 retry 흐름을 넣어 말단 구간 empty output 상황을 완화함.
    - image caption JSON을 별도 key로 저장하고 bucket, key, 생성 시각 메타를 image/process 경로에 함께 기록하도록 확장함.
    - process 중 실패 시 업로드한 frame/image caption JSON을 함께 정리하도록 rollback 경로도 보강함.
    - 관련 파일:
      - `src/vlm_pipeline/sql/schema.sql`

### 3. 수동 / 사전 라벨 import 경로 정리
- **문제**: 수동 라벨 import와 사전 라벨링 데이터 적재가 서로 다른 코드 경로에서 중복 구현되어 있었고, staging에서 기존 라벨 결과를 재적재하는 전용 진입점도 부족했음.
- **원인**: event label JSON 파싱, raw asset 매칭, label key 생성 로직이 manual import 안에 묶여 있었고, prelabeled 데이터는 raw ingest 이후 bbox/image caption까지 연결하는 공통 흐름이 정리되지 않았음.
- **조치**:
    - manual label import에서 공통 helper를 분리해 label 파일 순회, asset 매칭, event 추출, label 저장 규칙을 재사용 가능하게 정리함.
    - staging 전용 prelabeled import job을 추가해 raw ingest 후 event / bbox / image caption artifact import까지 한 경로에서 처리하게 구성함.
    - 사전 라벨링 이미지명에서 원본 raw stem을 찾는 정규식 패턴을 보강해 stem 추론 누락 케이스를 보완함.
    - 관련 파일:
      - `src/vlm_pipeline/definitions_staging.py`

### 4. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/dispatch/sensor_run_status.py`
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`
      - `src/vlm_pipeline/lib/staging_dispatch.py`

### 5. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.
    - 관련 파일:
      - `docker/.env.staging`
      - `docker/docker-compose.yaml`
      - `src/vlm_pipeline/defs/ingest/runtime_policy.py`
      - `src/vlm_pipeline/resources/runtime_settings.py`

### 6. 운영 / 에이전트 문서 보강
- **문제**: 운영과 staging 구분, 파이프라인 흐름, 자동화 에이전트 참고 문서가 서로 다른 수준으로 흩어져 있어 작업자마다 참조 경로가 달라질 수 있었음.
- **원인**: 짧은 운영 컨텍스트와 장문 레퍼런스, 외부 에이전트용 진입 문서가 분리되어 있었지만 최근 변경사항이 한 번에 정리돼 있지 않았음.
- **조치**:
    - Codex/에이전트용 `AGENTS.md`를 추가해 운영 규칙, staging/prod 차이, 자주 쓰는 명령을 빠르게 확인할 수 있게 정리함.
    - `ANTIGRAVITY.md`와 `CLAUDE2.md`를 보강해 에이전트 진입점과 장문 레퍼런스를 역할별로 구분해 둠.
    - 관련 파일:
      - `AGENTS.md`

### 7. 당일 정리
- **변경 통계**:
    - 변경 파일 **89개**, +4258/-2630줄.
- **관련 커밋**:
    - `ffe553e0`: refactor: production/test 런타임 분리 및 배포·디스패치·문서 정리
    - `1a267afc`: feat: LS 연동 멀티 미디어 타입 지원 (--auto-detect)
- **서비스 상태**: 파이프라인 서비스 11개 컨테이너 중 11개 정상 가동.
- **작업 환경**: Antigravity, Cursor, VSCode

## 2026-04-13

### 1. 운영 ingest / process 공통 구조 정리
- **문제**: ingest와 process 공통 책임이 여러 모듈로 흩어져 있어, 운영 로직을 수정할 때 영향 범위와 설정 반영 지점을 한 번에 보기 어려웠음.
- **원인**: raw ingest 실행 흐름, runtime env 해석, DuckDB schema 보정과 조회 helper가 각각 다른 레이어에 퍼져 있어 공통 동작을 건드릴 때 수정 포인트가 많아졌음.
- **조치**:
    - raw ingest에서 상태 생성과 실제 실행 파이프라인을 분리해 진입 구조를 명확히 정리함.
    - runtime 설정 로더를 추가하고 ingest runtime policy, stuck guard가 같은 설정 해석 경로를 재사용하도록 맞춤.
    - DuckDB resource에 dispatch/model/raw/image 조회 및 insert helper를 보강하고 schema ensure 흐름을 운영 기준으로 정돈함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/ingest/assets.py`
      - `src/vlm_pipeline/defs/ingest/runtime_policy.py`
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/resources/runtime_settings.py`

### 2. 프레임 추출 안정화 및 이미지 캡션 메타 확장
- **문제**: 영상 말단 구간에서 프레임 추출이 비거나 불안정할 수 있었고, 이미지 캡션 결과도 텍스트만 남아 후속 추적에 필요한 저장 메타가 부족했음.
- **원인**: ffmpeg 프레임 추출은 요청 시점이 영상 끝에 가까우면 empty output이 날 수 있었고, image caption 저장은 bucket/key/generated_at 같은 정본 추적 필드가 충분하지 않았음.
- **조치**:
    - ffmpeg frame extract에 fallback seek 후보와 retry 흐름을 넣어 말단 구간 empty output 상황을 완화함.
    - image caption JSON을 별도 key로 저장하고 bucket, key, 생성 시각 메타를 image/process 경로에 함께 기록하도록 확장함.
    - process 중 실패 시 업로드한 frame/image caption JSON을 함께 정리하도록 rollback 경로도 보강함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/lib/video_frames.py`
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/sql/schema.sql`

### 3. 수동 / 사전 라벨 import 경로 정리
- **문제**: 수동 라벨 import와 사전 라벨링 데이터 적재가 서로 다른 코드 경로에서 중복 구현되어 있었고, staging에서 기존 라벨 결과를 재적재하는 전용 진입점도 부족했음.
- **원인**: event label JSON 파싱, raw asset 매칭, label key 생성 로직이 manual import 안에 묶여 있었고, prelabeled 데이터는 raw ingest 이후 bbox/image caption까지 연결하는 공통 흐름이 정리되지 않았음.
- **조치**:
    - manual label import에서 공통 helper를 분리해 label 파일 순회, asset 매칭, event 추출, label 저장 규칙을 재사용 가능하게 정리함.
    - staging 전용 prelabeled import job을 추가해 raw ingest 후 event / bbox / image caption artifact import까지 한 경로에서 처리하게 구성함.
    - 사전 라벨링 이미지명에서 원본 raw stem을 찾는 정규식 패턴을 보강해 stem 추론 누락 케이스를 보완함.
    - 관련 파일:
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/label/import_support.py`
      - `src/vlm_pipeline/defs/label/manual_import.py`
      - `src/vlm_pipeline/defs/label/prelabeled_import.py`

### 4. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor_run_status.py`
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`
      - `src/vlm_pipeline/lib/staging_dispatch.py`

### 5. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.
    - 관련 파일:
      - `docker/.env.staging`
      - `docker/docker-compose.yaml`
      - `src/vlm_pipeline/defs/ingest/runtime_policy.py`
      - `src/vlm_pipeline/defs/ingest/sensor_stuck_guard.py`
      - `src/vlm_pipeline/resources/runtime_settings.py`

### 6. 운영 / 에이전트 문서 보강
- **문제**: 운영과 staging 구분, 파이프라인 흐름, 자동화 에이전트 참고 문서가 서로 다른 수준으로 흩어져 있어 작업자마다 참조 경로가 달라질 수 있었음.
- **원인**: 짧은 운영 컨텍스트와 장문 레퍼런스, 외부 에이전트용 진입 문서가 분리되어 있었지만 최근 변경사항이 한 번에 정리돼 있지 않았음.
- **조치**:
    - Codex/에이전트용 `AGENTS.md`를 추가해 운영 규칙, staging/prod 차이, 자주 쓰는 명령을 빠르게 확인할 수 있게 정리함.
    - `ANTIGRAVITY.md`와 `CLAUDE2.md`를 보강해 에이전트 진입점과 장문 레퍼런스를 역할별로 구분해 둠.
    - 관련 파일:
      - `AGENTS.md`

### 7. GitHub Actions CI/CD 파이프라인 구축 및 수정
- **문제**: main 브랜치 push 시 자동 배포되는 "Deploy to Production" 워크플로우가 3회 연속 실패함.
- **원인**:
    - (Attempt 1) `COMPOSE_FILE` 경로와 `working-directory` 조합 오류로 `docker/docker/docker-compose.yaml`을 찾음.
    - (Attempt 2) `docker/.env`가 `.gitignore`에 포함되어 runner 워크스페이스에 존재하지 않아 `docker compose up` 실패.
    - (Attempt 3) runner 워크스페이스에서 `docker compose up` 실행 시 상대 경로 볼륨(`./data`, `./app/dagster_home`)이 runner 경로로 마운트되어 DuckDB·Dagster storage 접근 불가.
- **조치**:
    - runner 호스트의 production `.env`를 워크스페이스로 복사하는 `Restore production .env` step 추가.
    - 필수 환경변수 4개(`MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `DUCKDB_PATH`, `MOTHERDUCK_TOKEN`) 검증 step 추가.
    - checkout된 소스를 production 디렉토리로 `rsync` 동기화 후, `docker compose up`은 production 경로에서 실행하도록 변경 (`PROD_DIR` 환경변수 도입).
    - `dagster_home/`, `credentials/`는 런타임 상태이므로 rsync 대상에서 제외.
    - `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true` 추가로 Node.js 20 deprecation 경고 해소.
    - `WORKLOG.md`를 `.gitignore`에 추가하여 git 추적 제거.
    - 관련 파일:
      - `.github/workflows/deploy-production.yml`
      - `.gitignore`

### 8. 당일 정리
- **변경 통계**:
    - 변경 파일 **278개**, +48032/-6606줄.
- **관련 커밋**:
    - `61010847`: fix: exclude dagster_home from rsync to avoid permission errors
    - `12a4678f`: fix: run docker compose up from production directory
    - `eea0d97f`: chore: stop tracking WORKLOG.md and add to .gitignore
    - `0c483ed3`: fix: restore production .env in deploy workflow
    - `23b3e1fb`: Merge remote-tracking branch 'origin/dev' into codex-main-sync
- **서비스 상태**: 파이프라인 서비스 11개 컨테이너 중 11개 정상 가동.
- **작업 환경**: Antigravity, Cursor, VSCode

## 2026-04-10

### 1. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/ingest/archive.py`

### 2. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.
    - 관련 파일:
      - `docker/.env.staging`
      - `docker/docker-compose.yaml`

### 3. 당일 정리
- **변경 통계**:
    - 변경 파일 **12개**, +811/-31줄.
- **관련 커밋**:
    - `c2fc43a5`: feat: dispatch webhook·GCP·ingest 보강 및 Docker·MLOps 문서
- **서비스 상태**: 파이프라인 서비스 11개 컨테이너 중 11개 정상 가동.
- **작업 환경**: Antigravity, Cursor, VSCode

## 2026-04-09

### 1. 프레임 추출 안정화 및 이미지 캡션 메타 확장
- **문제**: 영상 말단 구간에서 프레임 추출이 비거나 불안정할 수 있었고, 이미지 캡션 결과도 텍스트만 남아 후속 추적에 필요한 저장 메타가 부족했음.
- **원인**: ffmpeg 프레임 추출은 요청 시점이 영상 끝에 가까우면 empty output이 날 수 있었고, image caption 저장은 bucket/key/generated_at 같은 정본 추적 필드가 충분하지 않았음.
- **조치**:
    - ffmpeg frame extract에 fallback seek 후보와 retry 흐름을 넣어 말단 구간 empty output 상황을 완화함.
    - image caption JSON을 별도 key로 저장하고 bucket, key, 생성 시각 메타를 image/process 경로에 함께 기록하도록 확장함.
    - process 중 실패 시 업로드한 frame/image caption JSON을 함께 정리하도록 rollback 경로도 보강함.

### 2. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/ingest/archive.py`

### 3. 당일 정리
- **변경 통계**:
    - 변경 파일 **29개**, +3421/-879줄.
- **관련 커밋**:
    - `181a8fbc`: Merge pull request #36 from TeamPIA/feature/sanghoon
    - `310ab685`: Merge branch 'dev' of https://github.com/TeamPIA/Datapipeline-Data-data_pipeline into feature/sanghoon
    - `789e21ea`: feat: Label Studio 운영 준비 — presigned URL 자동 갱신, DuckDB lock retry, 운영 문서
    - `5d0c6e37`: Merge pull request #35 from Orderlee/dev
    - `93d326c2`: feat: Gemini JSON 파싱 보강·ingest/라벨/캡션 정리 및 문서·테스트 보완
- **서비스 상태**: 파이프라인 서비스 10개 컨테이너 중 10개 정상 가동.
- **작업 환경**: Antigravity, Cursor, VSCode

## 2026-04-08

### 1. 수동 / 사전 라벨 import 경로 정리
- **문제**: 수동 라벨 import와 사전 라벨링 데이터 적재가 서로 다른 코드 경로에서 중복 구현되어 있었고, staging에서 기존 라벨 결과를 재적재하는 전용 진입점도 부족했음.
- **원인**: event label JSON 파싱, raw asset 매칭, label key 생성 로직이 manual import 안에 묶여 있었고, prelabeled 데이터는 raw ingest 이후 bbox/image caption까지 연결하는 공통 흐름이 정리되지 않았음.
- **조치**:
    - manual label import에서 공통 helper를 분리해 label 파일 순회, asset 매칭, event 추출, label 저장 규칙을 재사용 가능하게 정리함.
    - staging 전용 prelabeled import job을 추가해 raw ingest 후 event / bbox / image caption artifact import까지 한 경로에서 처리하게 구성함.
    - 사전 라벨링 이미지명에서 원본 raw stem을 찾는 정규식 패턴을 보강해 stem 추론 누락 케이스를 보완함.

### 2. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/ingest/archive.py`

### 3. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.

### 4. 당일 정리
- **변경 통계**:
    - 변경 파일 **68개**, +4181/-6244줄.
- **관련 커밋**:
    - `d7e11cdb`: feat: NAS 헬스 센서 추가·ingest 보강 및 runbook·타임스탬프 정리
    - `a3577695`: feat: 검출 공통 모듈·SAM/YOLO 센서 분리 및 문서 정리
- **서비스 상태**: 파이프라인 서비스 10개 컨테이너 중 10개 정상 가동.
- **작업 환경**: Cursor, VSCode

## 2026-04-07

### 1. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.
    - 관련 파일:
      - `docker/.env.staging`
      - `docker/docker-compose.yaml`
      - `src/vlm_pipeline/defs/ingest/sensor_stuck_guard.py`

### 2. 당일 정리
- **변경 통계**:
    - 변경 파일 **26개**, +2423/-269줄.
- **관련 커밋**:
    - `17bb4270`: feat: DuckDB writer 태그 세분화 및 라벨·SAM3·YOLO·Docker 정비
- **서비스 상태**: 파이프라인 서비스 10개 컨테이너 중 10개 정상 가동.
- **작업 환경**: Antigravity, Cursor, VSCode

## 2026-04-06

### 1. Label Studio 셋업 준비
- **목적**: Gemini 라벨 검수용 Label Studio 컨테이너 기동 준비
- **조치**:
    - `docker/.env`에 LS 관련 환경변수 추가 (`LS_PORT=8084`, `LS_API_KEY`, `WEBHOOK_HOST`, `LS_WEBHOOK_PORT=8003`)
    - 포트 `8080`은 `piaspace-agent`가 점유 중이므로 `8084`로 변경
    - `docker-compose.labelstudio.yaml`은 기존 파이프라인과 완전 분리 구조 확인 완료
- **다음 단계**:
    1. `docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml up -d labelstudio`
    2. `http://<HOST>:8084` 접속 → 계정 생성 → API key 발급
    3. `.env`에 `LS_API_KEY`, `WEBHOOK_HOST` 값 반영 후 `ls-webhook` 기동

## 2026-04-03

### 1. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/ingest/archive.py`

### 2. 운영 / 에이전트 문서 보강
- **문제**: 운영과 staging 구분, 파이프라인 흐름, 자동화 에이전트 참고 문서가 서로 다른 수준으로 흩어져 있어 작업자마다 참조 경로가 달라질 수 있었음.
- **원인**: 짧은 운영 컨텍스트와 장문 레퍼런스, 외부 에이전트용 진입 문서가 분리되어 있었지만 최근 변경사항이 한 번에 정리돼 있지 않았음.
- **조치**:
    - Codex/에이전트용 `AGENTS.md`를 추가해 운영 규칙, staging/prod 차이, 자주 쓰는 명령을 빠르게 확인할 수 있게 정리함.
    - `ANTIGRAVITY.md`와 `CLAUDE2.md`를 보강해 에이전트 진입점과 장문 레퍼런스를 역할별로 구분해 둠.
    - 관련 파일:
      - `AGENTS.md`

### 3. 당일 정리
- **변경 통계**:
    - 변경 파일 **25개**, +1739/-571줄.
- **관련 커밋**:
    - `b94657e6`: fix: DuckDB lock 경합 완화와 운영 복구 스크립트 추가
    - `0b9c3df9`: feat: ingest 컴팩션/재수화 모듈 추가, 클립 윈도우 분리 및 운영 스크립트 정비
- **서비스 상태**: 파이프라인 서비스 9개 컨테이너 중 9개 정상 가동.
- **작업 환경**: Cursor, VSCode

## 2026-04-02

### 전체 코드 리팩토링 (Phase 1-6)

**Phase 1: 계층 위반 수정 및 private import 정리**
- `lib/spec_config.py` 계층 위반 해소: DB 의존 함수(`resolve_and_persist_spec_config`, `load_persisted_spec_config`)를 `defs/spec/config_resolver.py`로 이동. `lib/spec_config.py`는 순수 태그 파싱만 유지, re-export로 하위 호환
- `lib/key_builders.py` 신규 생성: 9개 MinIO 키 빌더 함수를 공유 유틸로 추출. `defs/process`, `defs/yolo`, `defs/label`, `defs/sam`의 private `_build_*_key` 함수들이 thin wrapper로 위임

**Phase 2: 거대 파일 분할**
- `defs/process/assets.py` (2134줄 → 94줄): `helpers.py`(1069줄), `captioning.py`(270줄), `frame_extract.py`(757줄), `raw_frames.py`(230줄)로 분할
- `resources/duckdb_ingest.py` (1408줄 → 12줄): `duckdb_ingest_dispatch.py`, `duckdb_ingest_raw.py`, `duckdb_ingest_metadata.py` 3개 서브 mixin으로 분할
- `defs/label/artifact_import_support.py` (906줄 → 684줄): `artifact_bbox.py`, `artifact_caption.py`, `artifact_classification.py`로 분할
- `resources/duckdb_base.py` (875줄 → ~100줄): `duckdb_phash.py`, `duckdb_migration.py`로 분할

**Phase 3: 중복 패턴 통합**
- `defs/label/assets.py` MVP/routed 분리: `label_helpers.py`, `timestamp.py`로 추출. assets.py는 thin routing wrapper
- MinIO 키 빌더 패턴 `lib/key_builders.py`로 완전 통합

**Phase 4: 디렉토리 구조 개선**
- `defs/process/__init__.py`, `defs/label/__init__.py`, `resources/__init__.py`에 모듈 구조 문서화
- `defs/ingest/ops.py` 위치 유지 (실용적 판단), `definitions_common.py` 290줄 유지

**Phase 5: 테스트 커버리지 확대**
- `tests/conftest.py` 공통 fixture 생성 (in-memory DuckDB)
- 5개 새 테스트 파일 추가: `test_key_builders.py`, `test_spec_config_tags.py`, `test_lib_sanitizer.py`, `test_lib_checksum.py`, `test_lib_validator.py`
- 65개 새 테스트 추가 (기존 81 → 총 146 passed)

**Phase 6: 문서 동기화**
- `CLAUDE.md` 코딩 규칙 섹션에 모듈 분할 규칙 추가
- `WORKLOG.md` 리팩토링 기록 추가


## 2026-04-01

### 1. 운영 ingest / process 공통 구조 정리
- **문제**: ingest와 process 공통 책임이 여러 모듈로 흩어져 있어, 운영 로직을 수정할 때 영향 범위와 설정 반영 지점을 한 번에 보기 어려웠음.
- **원인**: raw ingest 실행 흐름, runtime env 해석, DuckDB schema 보정과 조회 helper가 각각 다른 레이어에 퍼져 있어 공통 동작을 건드릴 때 수정 포인트가 많아졌음.
- **조치**:
    - raw ingest에서 상태 생성과 실제 실행 파이프라인을 분리해 진입 구조를 명확히 정리함.
    - runtime 설정 로더를 추가하고 ingest runtime policy, stuck guard가 같은 설정 해석 경로를 재사용하도록 맞춤.
    - DuckDB resource에 dispatch/model/raw/image 조회 및 insert helper를 보강하고 schema ensure 흐름을 운영 기준으로 정돈함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/process/assets.py`

### 2. 프레임 추출 안정화 및 이미지 캡션 메타 확장
- **문제**: 영상 말단 구간에서 프레임 추출이 비거나 불안정할 수 있었고, 이미지 캡션 결과도 텍스트만 남아 후속 추적에 필요한 저장 메타가 부족했음.
- **원인**: ffmpeg 프레임 추출은 요청 시점이 영상 끝에 가까우면 empty output이 날 수 있었고, image caption 저장은 bucket/key/generated_at 같은 정본 추적 필드가 충분하지 않았음.
- **조치**:
    - ffmpeg frame extract에 fallback seek 후보와 retry 흐름을 넣어 말단 구간 empty output 상황을 완화함.
    - image caption JSON을 별도 key로 저장하고 bucket, key, 생성 시각 메타를 image/process 경로에 함께 기록하도록 확장함.
    - process 중 실패 시 업로드한 frame/image caption JSON을 함께 정리하도록 rollback 경로도 보강함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/lib/video_frames.py`

### 3. 수동 / 사전 라벨 import 경로 정리
- **문제**: 수동 라벨 import와 사전 라벨링 데이터 적재가 서로 다른 코드 경로에서 중복 구현되어 있었고, staging에서 기존 라벨 결과를 재적재하는 전용 진입점도 부족했음.
- **원인**: event label JSON 파싱, raw asset 매칭, label key 생성 로직이 manual import 안에 묶여 있었고, prelabeled 데이터는 raw ingest 이후 bbox/image caption까지 연결하는 공통 흐름이 정리되지 않았음.
- **조치**:
    - manual label import에서 공통 helper를 분리해 label 파일 순회, asset 매칭, event 추출, label 저장 규칙을 재사용 가능하게 정리함.
    - staging 전용 prelabeled import job을 추가해 raw ingest 후 event / bbox / image caption artifact import까지 한 경로에서 처리하게 구성함.
    - 사전 라벨링 이미지명에서 원본 raw stem을 찾는 정규식 패턴을 보강해 stem 추론 누락 케이스를 보완함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/label/import_support.py`
      - `src/vlm_pipeline/defs/label/prelabeled_import.py`

### 4. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/lib/staging_dispatch.py`

### 5. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.
    - 관련 파일:
      - `docker/docker-compose.yaml`

### 6. 당일 정리
- **변경 통계**:
    - 변경 파일 **39개**, +6261/-154줄.
- **관련 커밋**:
    - `56581ab9`: feat: SAM3·YOLO·라벨링 파이프라인 확장 및 스테이징 디스패치 개선
- **서비스 상태**: 파이프라인 서비스 9개 컨테이너 중 9개 정상 가동.
- **작업 환경**: Antigravity, VSCode

## 2026-03-31

### 1. 운영 ingest / process 공통 구조 정리
- **문제**: ingest와 process 공통 책임이 여러 모듈로 흩어져 있어, 운영 로직을 수정할 때 영향 범위와 설정 반영 지점을 한 번에 보기 어려웠음.
- **원인**: raw ingest 실행 흐름, runtime env 해석, DuckDB schema 보정과 조회 helper가 각각 다른 레이어에 퍼져 있어 공통 동작을 건드릴 때 수정 포인트가 많아졌음.
- **조치**:
    - raw ingest에서 상태 생성과 실제 실행 파이프라인을 분리해 진입 구조를 명확히 정리함.
    - runtime 설정 로더를 추가하고 ingest runtime policy, stuck guard가 같은 설정 해석 경로를 재사용하도록 맞춤.
    - DuckDB resource에 dispatch/model/raw/image 조회 및 insert helper를 보강하고 schema ensure 흐름을 운영 기준으로 정돈함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`

### 2. 프레임 추출 안정화 및 이미지 캡션 메타 확장
- **문제**: 영상 말단 구간에서 프레임 추출이 비거나 불안정할 수 있었고, 이미지 캡션 결과도 텍스트만 남아 후속 추적에 필요한 저장 메타가 부족했음.
- **원인**: ffmpeg 프레임 추출은 요청 시점이 영상 끝에 가까우면 empty output이 날 수 있었고, image caption 저장은 bucket/key/generated_at 같은 정본 추적 필드가 충분하지 않았음.
- **조치**:
    - ffmpeg frame extract에 fallback seek 후보와 retry 흐름을 넣어 말단 구간 empty output 상황을 완화함.
    - image caption JSON을 별도 key로 저장하고 bucket, key, 생성 시각 메타를 image/process 경로에 함께 기록하도록 확장함.
    - process 중 실패 시 업로드한 frame/image caption JSON을 함께 정리하도록 rollback 경로도 보강함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/sql/schema.sql`

### 3. 수동 / 사전 라벨 import 경로 정리
- **문제**: 수동 라벨 import와 사전 라벨링 데이터 적재가 서로 다른 코드 경로에서 중복 구현되어 있었고, staging에서 기존 라벨 결과를 재적재하는 전용 진입점도 부족했음.
- **원인**: event label JSON 파싱, raw asset 매칭, label key 생성 로직이 manual import 안에 묶여 있었고, prelabeled 데이터는 raw ingest 이후 bbox/image caption까지 연결하는 공통 흐름이 정리되지 않았음.
- **조치**:
    - manual label import에서 공통 helper를 분리해 label 파일 순회, asset 매칭, event 추출, label 저장 규칙을 재사용 가능하게 정리함.
    - staging 전용 prelabeled import job을 추가해 raw ingest 후 event / bbox / image caption artifact import까지 한 경로에서 처리하게 구성함.
    - 사전 라벨링 이미지명에서 원본 raw stem을 찾는 정규식 패턴을 보강해 stem 추론 누락 케이스를 보완함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/label/import_support.py`

### 4. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/lib/staging_dispatch.py`

### 5. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.
    - 관련 파일:
      - `docker/.env.staging`
      - `docker/docker-compose.yaml`

### 6. 운영 / 에이전트 문서 보강
- **문제**: 운영과 staging 구분, 파이프라인 흐름, 자동화 에이전트 참고 문서가 서로 다른 수준으로 흩어져 있어 작업자마다 참조 경로가 달라질 수 있었음.
- **원인**: 짧은 운영 컨텍스트와 장문 레퍼런스, 외부 에이전트용 진입 문서가 분리되어 있었지만 최근 변경사항이 한 번에 정리돼 있지 않았음.
- **조치**:
    - Codex/에이전트용 `AGENTS.md`를 추가해 운영 규칙, staging/prod 차이, 자주 쓰는 명령을 빠르게 확인할 수 있게 정리함.
    - `ANTIGRAVITY.md`와 `CLAUDE2.md`를 보강해 에이전트 진입점과 장문 레퍼런스를 역할별로 구분해 둠.
    - 관련 파일:
      - `AGENTS.md`
      - `ANTIGRAVITY.md`
      - `CLAUDE2.md`

### 7. 당일 정리
- **변경 통계**:
    - 변경 파일 **44개**, +1138/-3156줄.
- **관련 커밋**:
    - `793023d5`: fix: GCP raw key와 MinIO 경로 정리를 지원한다
    - `59094df7`: chore: 로컬 문서 파일을 git 추적 대상에서 제외한다
    - `cfd66af9`: chore: 로컬 문서 ignore 규칙을 추가하고 수동 ingest 테스트 스크립트를 제거한다
    - `d04360fa`: refactor: 운영·테스트 라벨링 흐름과 카탈로그를 정리한다
    - `3051a6cf`: Merge branch 'dev' into dev
- **서비스 상태**: 파이프라인 서비스 9개 컨테이너 중 9개 정상 가동.
- **작업 환경**: VSCode

## 2026-03-30

### 1. 운영 ingest / process 공통 구조 정리
- **문제**: ingest와 process 공통 책임이 여러 모듈로 흩어져 있어, 운영 로직을 수정할 때 영향 범위와 설정 반영 지점을 한 번에 보기 어려웠음.
- **원인**: raw ingest 실행 흐름, runtime env 해석, DuckDB schema 보정과 조회 helper가 각각 다른 레이어에 퍼져 있어 공통 동작을 건드릴 때 수정 포인트가 많아졌음.
- **조치**:
    - raw ingest에서 상태 생성과 실제 실행 파이프라인을 분리해 진입 구조를 명확히 정리함.
    - runtime 설정 로더를 추가하고 ingest runtime policy, stuck guard가 같은 설정 해석 경로를 재사용하도록 맞춤.
    - DuckDB resource에 dispatch/model/raw/image 조회 및 insert helper를 보강하고 schema ensure 흐름을 운영 기준으로 정돈함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/ingest/assets.py`
      - `src/vlm_pipeline/defs/ingest/runtime_policy.py`
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/resources/runtime_settings.py`

### 2. 수동 / 사전 라벨 import 경로 정리
- **문제**: 수동 라벨 import와 사전 라벨링 데이터 적재가 서로 다른 코드 경로에서 중복 구현되어 있었고, staging에서 기존 라벨 결과를 재적재하는 전용 진입점도 부족했음.
- **원인**: event label JSON 파싱, raw asset 매칭, label key 생성 로직이 manual import 안에 묶여 있었고, prelabeled 데이터는 raw ingest 이후 bbox/image caption까지 연결하는 공통 흐름이 정리되지 않았음.
- **조치**:
    - manual label import에서 공통 helper를 분리해 label 파일 순회, asset 매칭, event 추출, label 저장 규칙을 재사용 가능하게 정리함.
    - staging 전용 prelabeled import job을 추가해 raw ingest 후 event / bbox / image caption artifact import까지 한 경로에서 처리하게 구성함.
    - 사전 라벨링 이미지명에서 원본 raw stem을 찾는 정규식 패턴을 보강해 stem 추론 누락 케이스를 보완함.
    - 관련 파일:
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/label/prelabeled_import.py`

### 3. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`
      - `src/vlm_pipeline/lib/staging_dispatch.py`

### 4. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/ingest/runtime_policy.py`
      - `src/vlm_pipeline/resources/runtime_settings.py`

### 5. 운영 / 에이전트 문서 보강
- **문제**: 운영과 staging 구분, 파이프라인 흐름, 자동화 에이전트 참고 문서가 서로 다른 수준으로 흩어져 있어 작업자마다 참조 경로가 달라질 수 있었음.
- **원인**: 짧은 운영 컨텍스트와 장문 레퍼런스, 외부 에이전트용 진입 문서가 분리되어 있었지만 최근 변경사항이 한 번에 정리돼 있지 않았음.
- **조치**:
    - Codex/에이전트용 `AGENTS.md`를 추가해 운영 규칙, staging/prod 차이, 자주 쓰는 명령을 빠르게 확인할 수 있게 정리함.
    - `ANTIGRAVITY.md`와 `CLAUDE2.md`를 보강해 에이전트 진입점과 장문 레퍼런스를 역할별로 구분해 둠.
    - 관련 파일:
      - `CLAUDE2.md`

### 6. 당일 정리
- **변경 통계**:
    - 변경 파일 **33개**, +3334/-1506줄.
- **관련 커밋**:
    - `7db48860`: feat: 운영·스테이징 라벨링 적재 흐름 정비
- **서비스 상태**: 파이프라인 서비스 9개 컨테이너 중 9개 정상 가동.
- **작업 환경**: Antigravity, Cursor, VSCode

## 2026-03-27

- (당일 커밋/파일 변경 없음)
## 2026-03-26

### 1. 운영 ingest / process 공통 구조 정리
- **문제**: ingest와 process 공통 책임이 여러 모듈로 흩어져 있어, 운영 로직을 수정할 때 영향 범위와 설정 반영 지점을 한 번에 보기 어려웠음.
- **원인**: raw ingest 실행 흐름, runtime env 해석, DuckDB schema 보정과 조회 helper가 각각 다른 레이어에 퍼져 있어 공통 동작을 건드릴 때 수정 포인트가 많아졌음.
- **조치**:
    - raw ingest에서 상태 생성과 실제 실행 파이프라인을 분리해 진입 구조를 명확히 정리함.
    - runtime 설정 로더를 추가하고 ingest runtime policy, stuck guard가 같은 설정 해석 경로를 재사용하도록 맞춤.
    - DuckDB resource에 dispatch/model/raw/image 조회 및 insert helper를 보강하고 schema ensure 흐름을 운영 기준으로 정돈함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/resources/runtime_settings.py`

### 2. 프레임 추출 안정화 및 이미지 캡션 메타 확장
- **문제**: 영상 말단 구간에서 프레임 추출이 비거나 불안정할 수 있었고, 이미지 캡션 결과도 텍스트만 남아 후속 추적에 필요한 저장 메타가 부족했음.
- **원인**: ffmpeg 프레임 추출은 요청 시점이 영상 끝에 가까우면 empty output이 날 수 있었고, image caption 저장은 bucket/key/generated_at 같은 정본 추적 필드가 충분하지 않았음.
- **조치**:
    - ffmpeg frame extract에 fallback seek 후보와 retry 흐름을 넣어 말단 구간 empty output 상황을 완화함.
    - image caption JSON을 별도 key로 저장하고 bucket, key, 생성 시각 메타를 image/process 경로에 함께 기록하도록 확장함.
    - process 중 실패 시 업로드한 frame/image caption JSON을 함께 정리하도록 rollback 경로도 보강함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/lib/video_frames.py`
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/sql/schema.sql`

### 3. 수동 / 사전 라벨 import 경로 정리
- **문제**: 수동 라벨 import와 사전 라벨링 데이터 적재가 서로 다른 코드 경로에서 중복 구현되어 있었고, staging에서 기존 라벨 결과를 재적재하는 전용 진입점도 부족했음.
- **원인**: event label JSON 파싱, raw asset 매칭, label key 생성 로직이 manual import 안에 묶여 있었고, prelabeled 데이터는 raw ingest 이후 bbox/image caption까지 연결하는 공통 흐름이 정리되지 않았음.
- **조치**:
    - manual label import에서 공통 helper를 분리해 label 파일 순회, asset 매칭, event 추출, label 저장 규칙을 재사용 가능하게 정리함.
    - staging 전용 prelabeled import job을 추가해 raw ingest 후 event / bbox / image caption artifact import까지 한 경로에서 처리하게 구성함.
    - 사전 라벨링 이미지명에서 원본 raw stem을 찾는 정규식 패턴을 보강해 stem 추론 누락 케이스를 보완함.
    - 관련 파일:
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/label/import_support.py`
      - `src/vlm_pipeline/defs/label/manual_import.py`
      - `src/vlm_pipeline/defs/label/prelabeled_import.py`

### 4. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`

### 5. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.
    - 관련 파일:
      - `docker/.env.staging`
      - `docker/docker-compose.yaml`
      - `src/vlm_pipeline/defs/ingest/sensor_stuck_guard.py`
      - `src/vlm_pipeline/resources/runtime_settings.py`

### 6. 운영 / 에이전트 문서 보강
- **문제**: 운영과 staging 구분, 파이프라인 흐름, 자동화 에이전트 참고 문서가 서로 다른 수준으로 흩어져 있어 작업자마다 참조 경로가 달라질 수 있었음.
- **원인**: 짧은 운영 컨텍스트와 장문 레퍼런스, 외부 에이전트용 진입 문서가 분리되어 있었지만 최근 변경사항이 한 번에 정리돼 있지 않았음.
- **조치**:
    - Codex/에이전트용 `AGENTS.md`를 추가해 운영 규칙, staging/prod 차이, 자주 쓰는 명령을 빠르게 확인할 수 있게 정리함.
    - `ANTIGRAVITY.md`와 `CLAUDE2.md`를 보강해 에이전트 진입점과 장문 레퍼런스를 역할별로 구분해 둠.
    - 관련 파일:
      - `CLAUDE2.md`

### 7. 당일 정리
- **변경 통계**:
    - 변경 파일 **39개**, +2297/-795줄.
- **관련 커밋**:
    - `e4ddebee`: feat: 스테이징 API 연동과 라벨링 파이프라인 안정화
    - `a7586ce3`: Merge pull request #26 from hoonikooni/feature/sanghoon
- **서비스 상태**: 파이프라인 서비스 9개 컨테이너 중 9개 정상 가동.
- **작업 환경**: Cursor, VSCode

## 2026-03-25

### 1. 운영 ingest / process 공통 구조 정리
- **문제**: ingest와 process 공통 책임이 여러 모듈로 흩어져 있어, 운영 로직을 수정할 때 영향 범위와 설정 반영 지점을 한 번에 보기 어려웠음.
- **원인**: raw ingest 실행 흐름, runtime env 해석, DuckDB schema 보정과 조회 helper가 각각 다른 레이어에 퍼져 있어 공통 동작을 건드릴 때 수정 포인트가 많아졌음.
- **조치**:
    - raw ingest에서 상태 생성과 실제 실행 파이프라인을 분리해 진입 구조를 명확히 정리함.
    - runtime 설정 로더를 추가하고 ingest runtime policy, stuck guard가 같은 설정 해석 경로를 재사용하도록 맞춤.
    - DuckDB resource에 dispatch/model/raw/image 조회 및 insert helper를 보강하고 schema ensure 흐름을 운영 기준으로 정돈함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/ingest/assets.py`
      - `src/vlm_pipeline/defs/ingest/runtime_policy.py`
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/resources/runtime_settings.py`

### 2. 프레임 추출 안정화 및 이미지 캡션 메타 확장
- **문제**: 영상 말단 구간에서 프레임 추출이 비거나 불안정할 수 있었고, 이미지 캡션 결과도 텍스트만 남아 후속 추적에 필요한 저장 메타가 부족했음.
- **원인**: ffmpeg 프레임 추출은 요청 시점이 영상 끝에 가까우면 empty output이 날 수 있었고, image caption 저장은 bucket/key/generated_at 같은 정본 추적 필드가 충분하지 않았음.
- **조치**:
    - ffmpeg frame extract에 fallback seek 후보와 retry 흐름을 넣어 말단 구간 empty output 상황을 완화함.
    - image caption JSON을 별도 key로 저장하고 bucket, key, 생성 시각 메타를 image/process 경로에 함께 기록하도록 확장함.
    - process 중 실패 시 업로드한 frame/image caption JSON을 함께 정리하도록 rollback 경로도 보강함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/process/assets.py`
      - `src/vlm_pipeline/lib/video_frames.py`
      - `src/vlm_pipeline/resources/duckdb_base.py`
      - `src/vlm_pipeline/resources/duckdb_ingest.py`
      - `src/vlm_pipeline/sql/schema.sql`

### 3. 수동 / 사전 라벨 import 경로 정리
- **문제**: 수동 라벨 import와 사전 라벨링 데이터 적재가 서로 다른 코드 경로에서 중복 구현되어 있었고, staging에서 기존 라벨 결과를 재적재하는 전용 진입점도 부족했음.
- **원인**: event label JSON 파싱, raw asset 매칭, label key 생성 로직이 manual import 안에 묶여 있었고, prelabeled 데이터는 raw ingest 이후 bbox/image caption까지 연결하는 공통 흐름이 정리되지 않았음.
- **조치**:
    - manual label import에서 공통 helper를 분리해 label 파일 순회, asset 매칭, event 추출, label 저장 규칙을 재사용 가능하게 정리함.
    - staging 전용 prelabeled import job을 추가해 raw ingest 후 event / bbox / image caption artifact import까지 한 경로에서 처리하게 구성함.
    - 사전 라벨링 이미지명에서 원본 raw stem을 찾는 정규식 패턴을 보강해 stem 추론 누락 케이스를 보완함.
    - 관련 파일:
      - `src/vlm_pipeline/definitions_staging.py`
      - `src/vlm_pipeline/defs/label/import_support.py`
      - `src/vlm_pipeline/defs/label/manual_import.py`
      - `src/vlm_pipeline/defs/label/prelabeled_import.py`

### 4. Staging dispatch 서비스 분리 및 흐름 정리
- **문제**: staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.
- **원인**: dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.
- **조치**:
    - dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.
    - 중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.
    - dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.
    - 관련 파일:
      - `src/vlm_pipeline/defs/dispatch/sensor.py`
      - `src/vlm_pipeline/defs/dispatch/sensor_run_status.py`
      - `src/vlm_pipeline/defs/dispatch/service.py`
      - `src/vlm_pipeline/defs/ingest/archive.py`
      - `src/vlm_pipeline/lib/staging_dispatch.py`

### 5. Staging 환경값 및 운영 보조 설정 정리
- **문제**: staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.
- **원인**: staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.
- **조치**:
    - staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.
    - docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.
    - stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.
    - 관련 파일:
      - `docker/.env.staging`
      - `docker/docker-compose.yaml`
      - `src/vlm_pipeline/defs/ingest/runtime_policy.py`
      - `src/vlm_pipeline/defs/ingest/sensor_stuck_guard.py`
      - `src/vlm_pipeline/resources/runtime_settings.py`

### 6. 운영 / 에이전트 문서 보강
- **문제**: 운영과 staging 구분, 파이프라인 흐름, 자동화 에이전트 참고 문서가 서로 다른 수준으로 흩어져 있어 작업자마다 참조 경로가 달라질 수 있었음.
- **원인**: 짧은 운영 컨텍스트와 장문 레퍼런스, 외부 에이전트용 진입 문서가 분리되어 있었지만 최근 변경사항이 한 번에 정리돼 있지 않았음.
- **조치**:
    - Codex/에이전트용 `AGENTS.md`를 추가해 운영 규칙, staging/prod 차이, 자주 쓰는 명령을 빠르게 확인할 수 있게 정리함.
    - `ANTIGRAVITY.md`와 `CLAUDE2.md`를 보강해 에이전트 진입점과 장문 레퍼런스를 역할별로 구분해 둠.
    - 관련 파일:
      - `AGENTS.md`
      - `ANTIGRAVITY.md`
      - `CLAUDE2.md`

### 7. 당일 정리
- **변경 통계**:
    - 변경 파일 **40개**, +5596/-1201줄.
- **관련 커밋**:
    - `7593105a`: fix: 사전 라벨링 원본 stem 추론 패턴 추가
    - `b89f4a79`: feat: staging dispatch 리팩터링과 사전 라벨링 데이터 적재 경로 추가
    - `9837bf41`: 운영·스테이징 ingest/dispatch 구조를 리팩터링하고 안정화한다
- **서비스 상태**: 파이프라인 서비스 9개 컨테이너 중 8개 정상 가동.
- **작업 환경**: Cursor, VSCode

## 2026-03-24

### 1. 변경 통계 및 서비스
- 파이프라인 서비스 7개 컨테이너 중 6개 정상 가동.

## 2026-03-23

### 1. 운영 DuckDB 장애 대응 및 복구 준비
- **문제**: 운영 Dagster `clip_to_frame` 단계에서 `image_metadata__migrated does not exist` 오류가 반복 발생하며 `auto_labeling_job`이 중간부터 연속 실패함.
- **원인**: `clip_to_frame` 로직 자체의 단순 버그라기보다, 운영 DuckDB의 `image_metadata` 스키마 마이그레이션 과정에서 임시 테이블 참조가 남거나 카탈로그 상태가 꼬인 것으로 판단됨. 런타임 중 테이블 교체성 마이그레이션이 수행되는 구조도 장애 재발 가능성을 키우는 요인이었음.
- **조치**:
    - `src/vlm_pipeline/resources/duckdb_base.py`에서 `ensure_schema()`가 런타임에 위험한 테이블 교체를 하지 않도록 보정 로직을 완화.
    - `scripts/repair_image_metadata_schema.py`를 추가해 운영 DB 상태 점검 및 복구를 수동으로 수행할 수 있도록 함.
    - 운영 Dagster 재기동, stale run 정리, 재실행까지 진행.
    - 다만 운영 `pipeline.duckdb` 파일 자체 손상 가능성은 남아 있어, 필요 시 오프라인 복구 절차를 별도로 수행해야 하는 관리 포인트로 유지.

### 2. Staging 이미지/VQA 캡셔닝 책임 재정리
- **문제**: 이미지 단위 캡셔닝과 영상 이벤트 캡셔닝이 혼재되어 있어, 어떤 테이블이 정본인지와 후속 조회 책임이 불명확했음.
- **원인**: 기존에는 프레임용 설명과 영상 이벤트 설명이 서로 다른 엔티티인데도 저장 위치와 의미가 충분히 분리되지 않아, `image_metadata.caption_text` 하나에 여러 의미가 섞일 여지가 있었음.
- **조치**:
    - VLM 이벤트 캡션은 계속 `labels.caption_text`를 정본으로 유지.
    - 이미지/VQA 캡셔닝은 `image_metadata` 책임으로 정리하고, staging에서는 `image_caption_text`, `image_caption_score`를 사용하는 방향으로 반영.
    - VertexAI 기반 staging VQA는 prompt와 가장 유사한 프레임 1장만 캡셔닝하도록 `src/vlm_pipeline/lib/staging_vertex.py`, `src/vlm_pipeline/defs/process/assets.py`를 수정.

### 3. Staging dispatch JSON 및 라우팅 규칙 개편
- **문제**: staging 테스트 시 dispatch JSON 형식과 실제 라우팅 로직이 맞지 않았고, `bbox`, `captioning`, `image classification` 조합에 따라 원하는 경로로 흐르지 않는 문제가 있었음.
- **원인**: 기존 dispatch 입력은 `outputs`/`run_mode` 중심이었고, 새 테스트 입력 형식인 `categories`, `classes`, `labeling_method`를 직접 반영하지 못했음. 또한 `bbox`가 포함되면 무조건 YOLO-only로 꺾이는 규칙 때문에 `captioning+bbox` 혼합 시나리오가 지원되지 않았음.
- **조치**:
    - `src/vlm_pipeline/lib/staging_dispatch.py`를 추가·정리해 dispatch JSON을 `categories`, `classes`, `labeling_method` 기반으로 해석하도록 변경.
    - `bbox`, `image classification`만 요청되면 YOLO-only로 처리.
    - `captioning+bbox`가 함께 오면 VertexAI 캡셔닝과 YOLO가 같이 수행되도록 staging 라우팅 수정.
    - `categories` 또는 `labeling_method`에 `필요없음`, `라벨링필요없음` 같은 값이 포함되면, 라벨링 없이 `archive 이동 + raw_ingest(MinIO 업로드)`만 수행하도록 분기 추가.

### 4. Staging ingest/dispatch 흐름 정리
- **문제**: staging에서 trigger JSON이 없어도 `incoming` 폴더를 자동 스캔해 ingest가 시작되거나, `archive_pending`을 거치는 흐름이 사용자 의도와 다르게 동작함.
- **원인**: `auto_bootstrap_manifest_sensor`와 dispatch 경로가 동시에 살아 있어 JSON 없이도 ingest가 올라갈 수 있었고, `archive_pending` 단계도 테스트 의도보다 넓게 사용되고 있었음.
- **조치**:
    - staging에서는 trigger JSON이 없으면 `incoming`에서 대기만 하도록 auto-bootstrap 경로를 차단.
    - `archive_pending`은 staging dispatch 흐름에서 제외.
    - trigger JSON이 있을 때만 `incoming -> archive -> MinIO 업로드` 흐름을 타도록 정리.
    - `.dispatch`, `.manifests`, staging MinIO, DuckDB를 여러 차례 초기화하며 실제 동작을 반복 검증.

### 5. Staging 실행 순서 및 테스트 운영 정리
- **문제**: `raw_ingest`와 `yolo_image_detection`이 거의 동시에 시작돼, MinIO 업로드 및 프레임 생성이 끝나기 전에 YOLO가 먼저 떠서 `YOLO 대상 이미지 없음` 로그가 발생함.
- **원인**: staging에서도 공용 YOLO asset이 직접 job selection에 포함돼 있어, 실제 frame 생성 선행 여부와 상관없이 독립적으로 실행되는 구조였음.
- **조치**:
    - staging 전용 YOLO asset을 분리해 `raw_ingest -> clip/frame 생성 -> yolo_image_detection` 순서가 보장되도록 수정.
    - 여러 차례 staging Dagster 재기동, canceling/canceled run 정리, trigger JSON 생성, 재실행을 통해 검증.

### 6. Git 정리 및 배포 준비
- **문제**: staging 관련 테스트 산출물과 테스트 코드 추적 범위가 섞여 있어 워크트리 관리가 번잡해질 수 있었음.
- **원인**: 테스트 보조 파일이 Git 추적 범위에 남아 있었고, staging 전용 조정 사항도 한 커밋으로 정리할 필요가 있었음.
- **조치**:
    - `.gitignore`에 `tests/`를 추가하고 기존 추적을 해제.
    - 변경 사항을 `Harden staging dispatch flow and schema handling` 커밋(`4f93d3e9`)으로 정리.
    - `origin/dev`로 푸시 후 `Orderlee:dev -> TeamPIA:dev` PR 생성.

## 2026-03-20

### 1. Staging Vertex chunking 및 event frame caption 도입
- **문제**: staging에서 긴 영상이나 event 단위 이미지 선택을 VertexAI로 처리할 때, 단순 일괄 처리만으로는 원하는 프레임 선별과 캡셔닝 품질 확보가 어려웠음.
- **원인**: 기존 로직은 이벤트 단위 캡션/프레임 선택을 세밀하게 다루는 helper가 부족했고, DuckDB 스키마와 저장 로직도 event frame caption을 충분히 반영하지 못했음.
- **조치**:
    - `src/vlm_pipeline/lib/staging_vertex.py`를 도입·확장해 staging 전용 Vertex helper를 구성.
    - `src/vlm_pipeline/defs/label/assets.py`, `src/vlm_pipeline/defs/process/assets.py`를 수정해 event frame caption 추출과 chunking 처리를 반영.
    - `src/vlm_pipeline/resources/duckdb_base.py`, `duckdb_ingest.py`, `duckdb_labeling.py`, `duckdb_dedup.py`, `src/vlm_pipeline/sql/schema.sql`을 함께 수정해 저장 경로와 상태 관리 보완.
    - `tests/unit/test_auto_labeling_stabilization.py`, `tests/unit/test_staging_vertex_helpers.py`로 회귀 테스트 추가. (커밋: `a8c9f932`)

### 2. 로컬 메모/개인 도구 추적 범위 정리
- **문제**: 저장소에 로컬 메모(`doc_2`)와 개인용 `.agent` 스킬이 함께 추적되어 협업 저장소와 개인 작업 공간 경계가 흐려질 수 있었음.
- **원인**: 로컬 전용 자산이 Git 추적 범위에 남아 있었고, 실제 운영/배포 코드와 무관한 파일이 변경 이력에 섞이고 있었음.
- **조치**:
    - `.gitignore`에 `doc_2`를 추가해 워크스페이스 메모를 저장소 추적 대상에서 제외. (커밋: `93627582`)
    - `.agent/skill` 하위 스킬 파일 추적을 저장소에서 제거해 로컬 전용 자산으로 분리. (커밋: `71a38205`)
    - 이후 `.agent` 관련 자산은 로컬에서만 관리하고, 협업 브랜치에는 파이프라인 코드와 설정만 남기도록 정리.

### 3. 당일 정리
- **변경 통계**:
    - `a8c9f932`: 10개 파일, +1285/-42
    - `93627582`: 1개 파일, +1/-0
    - `71a38205`: 8개 파일, -1227
- **서비스 상태**: 파이프라인 주요 컨테이너 기동 상태 유지 확인.

## 2026-03-19

### 1. Staging 전용 spec 에셋/센서 분리
- **문제**: spec 기반 auto labeling 로직이 운영/공용 파이프라인과 staging 검증 흐름에 동시에 걸쳐 있어, staging 실험과 본 파이프라인 책임이 뒤섞일 수 있었음.
- **원인**: `definitions_staging.py`, spec sensor, dispatch sensor, labeling/process asset들이 충분히 staging 전용으로 분리되지 않아, 테스트 시나리오를 안전하게 반복하기 어려웠음.
- **조치**:
    - `src/vlm_pipeline/defs/spec/staging_assets.py`, `src/vlm_pipeline/defs/spec/staging_sensor.py`를 추가해 staging 전용 spec 에셋/센서 분리.
    - `src/vlm_pipeline/definitions_staging.py`, `src/vlm_pipeline/defs/spec/assets.py`, `src/vlm_pipeline/defs/spec/sensor.py`, `src/vlm_pipeline/defs/dispatch/sensor.py`를 정리해 staging에서 필요한 흐름만 명시적으로 연결.
    - `src/vlm_pipeline/lib/spec_config.py`를 추가해 spec 실행 설정과 tag 해석을 일관되게 처리. (커밋: `a7c52a2e`)

### 2. Video frame 추출 및 검증 기반 강화
- **문제**: staging에서 clip/frame 추출, YOLO 연계, labeling 입력 품질을 검증할 기준이 부족했음.
- **원인**: `video_frames` 단위 helper와 검증 스크립트가 충분하지 않아, frame 생성 단계를 따로 확인하거나 벤치마크하기 어려웠음.
- **조치**:
    - `scripts/staging_video_extract_yolo_bench.py`를 추가해 staging video extract/YOLO bench 경로를 마련.
    - `src/vlm_pipeline/lib/video_frames.py`, `src/vlm_pipeline/defs/process/assets.py`, `src/vlm_pipeline/defs/label/assets.py`, `src/vlm_pipeline/defs/yolo/assets.py`를 수정해 frame 단위 처리 보완.
    - `tests/unit/test_video_frames.py`를 추가해 frame 추출 단위 테스트 확보.

### 3. 정리
- **문제**: 3/18에 크게 추가된 spec 문서 자산이 계속 저장소에 남아, 실제 코드 변경과 문서 초안이 섞여 보일 수 있었음.
- **원인**: spec 문서가 한 차례 대량 추가된 뒤 staging 전용 코드 구조가 정리되면서 일부 초안 문서를 계속 유지할 필요가 줄어들었음.
- **조치**:
    - `auto_labeling_requirements_spec.md`, `auto_labeling_spec.md`, `auto_labeling_ui_spec.md`, `auto_labeling_unified_spec.md`를 정리 대상에 포함.
    - 당일 변경을 `feat(spec): staging 전용 spec 에셋/센서 분리 및 video_frames 단위 테스트 추가` 커밋(`a7c52a2e`)으로 정리.

## 2026-03-18

### 1. Staging spec 기반 아키텍처 확장
- **문제**: staging에서 auto labeling을 검증하려면, spec 단위 입력과 실행 조건을 분리한 전용 모듈이 필요했음.
- **원인**: 기존 구조는 공용 ingest/labeling 흐름 중심이어서, staging에서 요구되는 spec 기반 실험과 QA 시나리오를 구조적으로 담기 어려웠음.
- **조치**:
    - `src/vlm_pipeline/definitions_staging.py`를 확장하고 `docker/app/dagster_defs_staging.py`, `docker/app/workspace_staging.yaml`을 추가/보완해 staging 전용 Dagster 로드 경로를 마련.
    - `src/vlm_pipeline/defs/spec/assets.py`, `src/vlm_pipeline/defs/spec/sensor.py`, `src/vlm_pipeline/resources/duckdb_spec.py`를 추가해 spec 입력, sensor, DB 저장 계층을 신설.
    - `src/vlm_pipeline/resources/duckdb_base.py`, `duckdb_ingest.py`, `duckdb_labeling.py`, `src/vlm_pipeline/sql/schema.sql`을 함께 확장해 spec 실행에 필요한 테이블과 상태 컬럼을 추가. (커밋: `43481e5f`)

### 2. QA 스크립트 및 운영 보조 도구 추가
- **문제**: staging spec 검증과 수동 QA를 반복 수행하기 위한 보조 스크립트와 문서가 부족했음.
- **원인**: 테스트 시나리오가 문서화되지 않았고, 수동 실행/검증 과정이 운영자 경험에 많이 의존하고 있었음.
- **조치**:
    - `scripts/gemini_archive_filename_labeling.py`, `scripts/staging_qa_run_cycle.sh`, `scripts/staging_qa_safe_drop_incoming.sh`, `scripts/staging_qa_wait_expect.py`, `scripts/staging_test_dispatch.py`를 추가/보강.
    - `auto_labeling_requirements_spec.md`, `auto_labeling_spec.md`, `auto_labeling_ui_spec.md`, `auto_labeling_unified_spec.md`를 작성해 요구사항, UI, 통합 시나리오를 정리.
    - `docker/.env.staging`, `docker/docker-compose.yaml`을 수정해 staging 환경변수와 compose 설정을 정돈.

### 3. DuckDB 및 labeling/YOLO 경로 기반 정비
- **문제**: staging spec 실험을 하려면 ingest, labeling, YOLO 결과를 DuckDB에서 일관되게 추적할 수 있어야 했음.
- **원인**: 기존 DuckDB 자원과 labeling 자원이 spec 실행 컨텍스트를 충분히 알지 못했고, 관련 상태 관리도 부족했음.
- **조치**:
    - `src/vlm_pipeline/defs/label/assets.py`, `src/vlm_pipeline/defs/process/assets.py`, `src/vlm_pipeline/defs/yolo/assets.py`, `src/vlm_pipeline/lib/env_utils.py`를 수정해 spec 경로와 일반 경로를 함께 수용하도록 조정.
    - `src/vlm_pipeline/resources/duckdb.py`를 보완하고, `defs/ingest/ops.py`의 ingest 상태 기록도 정리.
    - 당일 변경을 `feat: staging spec 모듈, QA 스크립트, DuckDB 리소스 확장 및 파이프라인 설정 개선` 커밋(`43481e5f`)과 `feature/vlm-update` 병합(`2fc3299b`)으로 통합.

### 4. 당일 정리
- **변경 통계**:
    - `43481e5f`: 32개 파일, +3921/-189
    - `2fc3299b`: `feature/vlm-update` 병합
- **서비스 상태**: staging/운영 파이프라인 설정 변경 후 주요 서비스 기동 상태 확인.

## 2026-03-17

### 1. .agent/skill 복구
- **문제**: `.agent/skill` 폴더가 사라져 daily_worklog, duckdb_staging_wiper, staging_reset 등 스킬을 사용할 수 없는 상태가 됨.
- **원인**: `.agent/` 폴더를 gitignore로 push 제외·추적 해제하는 커밋(896cafd) 과정에서 로컬에서 `.agent/skill` 디렉터리가 삭제된 상태로 반영됨. 원격에는 이미 삭제 커밋이 올라가 이력만 남음.
- **조치**:
    - `git checkout 896cafd^ -- .agent/skill/` 로 해당 커밋 직전 이력에서 `.agent/skill` 전체 복원.
    - `dagster_lineage_fixer.md`, `daily_worklog`(SKILL.md + scripts), `duckdb_staging_wiper`, `staging_reset` 스킬 및 스크립트 복구 완료. 이후 `.agent`는 gitignore 유지로 로컬 전용 사용.

### 2. Linear MCP 및 WORKLOG 작성 방식 정리
- **문제**: (1) Cursor 외 VSCode·Windsurf 등 다른 IDE에서 Linear MCP를 쓸 수 없음. (2) WORKLOG 3/17 분이 자동 생성 형식(커밋 나열·문제 1~18 상세)으로만 되어 있어, 3/16 이전처럼 “어떤 문제 → 원인 → 어떻게 해결했는지” 구조로 읽기 어려움.
- **원인**: (1) Linear MCP는 Cursor 플러그인으로만 등록되어 있었고, 프로젝트 공용 MCP 마스터(`.agent/mcp/mcp_config.json`)에는 없어 `sync_mcp.sh`로 다른 IDE 설정에 반영되지 않음. (2) daily_worklog 스크립트가 “작업 요약 + 조치 한 줄” 위주로만 생성하도록 이전에 바뀌었고, “문제/원인/조치” 상세 해결방안 형식은 반영되지 않음.
- **조치**:
    - Linear MCP를 `.agent/mcp/mcp_config.json`에 추가(`command`: npx, `args`: `-y mcp-remote https://mcp.linear.app/mcp`). `bash .agent/mcp/sync_mcp.sh` 실행으로 `.cursor/mcp.json`, `.mcp.json`, `.vscode/mcp.json` 등에 동기화해 다른 IDE에서도 Linear 사용 가능하도록 함.
    - WORKLOG 3/17 분을 3/16과 동일하게 **문제** / **원인** / **조치** 구조로 수동 정리. daily_worklog 스크립트는 “해결방안을 문제·원인·조치로 상세 작성”하도록 변경해, 이후 자동 생성분도 동일 형식으로 나오게 함.

### 3. 당일 병합·정리
- **문제**: feature/vlm-update 브랜치와 .agent 추적 해제 등이 반영된 뒤, WORKLOG·env 예시 등 문서/설정이 이전 상태와 혼재할 수 있는 상황.
- **원인**: 병합 및 .agent gitignore 전환 과정에서 여러 커밋이 쌓였고, .env.example 등은 보안·정리 목적으로 값 비우기·vertex prompt normal 제외 등이 적용됨.
- **조치**:
    - feature/vlm-update 병합, .agent 폴더 gitignore 추적 해제 유지.
    - .env.example 값 비우기·vertex prompt normal 제외 반영.
    - 변경 파일 105개, +12062/-6995. 파이프라인 서비스 7개 컨테이너 정상 가동 확인.


## 2026-03-16

### 1. Dagster Instance 정지 문제 해결
- **문제**: Port 3030 충돌 및 `LOCATION_ERROR`로 인해 Dagster UI에 접근 불가.
- **원인**: 이전 프로세스 정체 및 서비스 로딩 단계에서의 코드 로딩 실패.
- **조치**: 
    - `docker compose restart dagster`를 통한 서비스 초기화.
    - 컨테이너 내 포트 리스닝 상태 확인 완료.

### 2. Dagster 구성 오류(Config Error) 수정
- **대상 파일**: `src/vlm_pipeline/defs/process/sensor.py`
- **수정 내용**: `video_frame_extract_sensor`에서 `clip_captioning` 오퍼레이션에 전달하던 지원하지 않는 필드(`jpeg_quality`, `max_frames_per_video`, `overwrite_existing`) 제거.
- **결과**: 데몬 로그의 불필요한 `DagsterInvalidConfigError` 제거 및 안정성 확보.

### 3. GCS Download Job 복구
- **문제**: 서비스 재시작 전 실행 중이던 `gcs_download_job`(`4ecde6a5`)이 프로세스 증발로 인해 멈춤.
- **조치**: 
    - SQLite DB 직접 접근을 통해 좀비 런(`STARTED`)을 `CANCELED`로 강제 업데이트.
    - `dagster job launch` 명령어로 동일 설정 재실행 (`b5e76263...`).

### 4. 데이터 수집 센서(Bootstrap Sensor) 최적화
- **문제**: 3/13~3/15 날짜 폴더가 존재함에도 센서가 매니페스트를 생성하지 않음.
- **원인**: 
    - 날짜 폴더 내 `_DONE` 마커 파일 부재.
    - 센서의 1회당 최대 스캔 유닛 수(`30`) 제한으로 인해 대규모 인입 시 지연 발생.
- **조치**: 
    - `.env` 파일 수정: `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK`을 **30 -> 100**으로 상향.
    - `AUTO_BOOTSTRAP_SAFE_MAX_UNITS_PER_TICK=100` 추가.
    - 서비스 재시작 후 최신 데이터 감지 및 `ingest_job` 자동 실행 확인.

---

### 🛠 Troubleshooting & Quick-Fix Reference (추후 문제 발생 시 참고)

#### 1. Dagster UI 접속 불가 (Port 3030 / LOCATION_ERROR)
- **현상**: 브라우저에서 접속되지 않거나, `LOCATION_ERROR`가 로그에 반복됨.
- **해결**:
    1. **로그 확인**: `docker logs pipeline-dagster-1 | tail -n 100` 명령어로 에러 확인.
    2. **완전 재시작**:
       ```bash
       cd ~/work_p/Datapipeline-Data-data_pipeline/docker
       docker compose restart dagster
       ```
    3. **포트 점유 확인**: `ss -tln | grep 3030`으로 0.0.0.0:3030이 LISTEN 상태인지 확인.

#### 2. 특정 작업(Run)이 'STARTED' 상태에서 멈추고 로그가 안 찍힐 때
- **현상**: 실제 프로세스는 죽었으나 UI상에서 계속 실행 중으로 표시되는 '좀비 런' 상태.
- **해결 (DB 강제 업데이트)**:
    ```bash
    docker exec pipeline-dagster-1 python3 -c "
    import sqlite3
    conn = sqlite3.connect('/app/dagster_home/storage/runs.db')
    cur = conn.cursor()
    cur.execute(\"UPDATE runs SET status = 'CANCELED' WHERE run_id = '여기에_RUN_ID_입력'\")
    conn.commit()
    "
    ```
- **재실행**: UI에서 'Re-execution'을 하거나, CLI로 수동 실행.

#### 3. 데이터가 인입되었는데 센서가 작동하지 않을 때
- **현상**: GCS 다운로드는 끝났는데 `ingest_job`이 생성되지 않음.
- **체크리스트**:
    1. **마커 파일**: 해당 날짜 폴더 안에 `_DONE` 파일이 있는지 확인. (`ls -a /nas/incoming/bucket/folder/`)
    2. **센서 로그**: `docker logs pipeline-dagster-1 | grep auto_bootstrap_manifest_sensor`
       - "복사 안정화 대기 중"이 뜨면 마커 파일이나 시간 지연(120초) 문제임.
       - "pending backlog 보호"가 뜨면 `/nas/incoming/.manifests/pending`에 처리 안 된 파일이 200개 이상 쌓인 것임.
    3. **스캔 속도 조절**: `.env` 파일의 `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK` 값을 조정 후 저장.

#### 4. DagsterInvalidConfigError 발생 시
- **원인**: Asset이나 Job의 `config` 스키마가 변경되었는데, Sensor나 Schedule에서 예전 방식의 config를 보낼 때 발생.
- **해결**: 에러 로그에 표시된 'unexpected field'를 확인하여 `src/vlm_pipeline/defs/.../sensor.py`의 `run_config` 부분을 수정.

## 2026-03-13
- **Staging 파이프라인 전용 아키텍처 및 Dispatch 기능 개발**:
  - 목표:
    - 운영(production) 환경과 분리된 staging 전용 데이터 처리 및 dispatch 흐름 구성.
    - `incoming -> ingest -> archive_pending` 형태의 초기 적재 흐름 구현.
  - 조치:
    - JSON 파일 기반 dispatch 트리거 체계 구축 (`archive_pending`에서 `archive`로의 이동 승인).
    - `run_mode` 파라미터(`gemini`, `yolo`, `both`)에 따라 추출 파이프라인 동적 실행 지원.
    - 기존 staging 센서의 실행을 막아 신규 dispatch 로직과 충돌 방지.
    - `current` 및 `dense` 등 다양한 이미지(프레임) 추출 프로필 지원 구현.
    - staging_dispatch 요청 이력을 관리할 신규 DB 테이블(`staging_dispatch_requests`) 추가.

- **Staging Pipeline `raw_ingest` 정체 현상 디버깅 및 I/O 병목 해소**:
  - 증상:
    - 1GB 이상의 대용량 영상 처리 중 `raw_ingest` 단계에서 구간이 18분 이상 정체되며(stuck) 파이프라인이 멈추는 현상 발생.
  - 실제 원인:
    - 파일 I/O나 좀비 프로세스가 아니라, 1-pass 로딩 최적화를 위해 만든 임시 파일(SpooledTemporaryFile) 처리 과정에서 디스크/메모리 병목이 발생. 
    - Boto3의 네이티브 멀티파트 병렬 파일 읽기(멀티스레드) 기능이 단일 스트림 객체 업로드 시 활성화되지 않는 한계 존재.
  - 조치 (`ops.py`, `minio.py` 수정):
    - `load_video_once`에서 `include_file_stream=False`로 전환해 로컬 tempfile 생성을 우회.
    - MinIO 클라이언트에 병렬 청크 단위 접근을 지원하는 `upload_file` 포인트를 신설.
    - NAS 상의 원본 파일을 boto3가 직접 다중 스레드로 병행 스트리밍하도록 파이프라인 구조 변경 완료.

## 2026-03-12
- **staging 재테스트용 상태를 완전히 초기화하는 절차를 정리하고 실제로 수행**:
  - 배경:
    - staging에서 `incoming -> ingest -> auto_labeling -> clip/image/yolo` 흐름을 다시 검증해야 했고,
      이전 테스트에서 쌓인 MinIO 객체, `staging.duckdb`, Dagster runtime state가 남아 있으면
      새 테스트 결과와 섞여 원인 추적이 어려운 상태였음.
  - 조치:
    - `dagster-staging`을 먼저 중지.
    - staging MinIO endpoint(`172.168.47.36:9002`)의
      - `vlm-raw`
      - `vlm-labels`
      - `vlm-processed`
      - `vlm-dataset`
      객체를 전부 삭제.
    - `docker/data/staging.duckdb`를 삭제.
    - `docker/data/dagster_home_staging/storage` 전체를 삭제해
      sensor cursor, run history, run_key 기록이 남지 않게 정리.
  - 운영 기준:
    - 이 초기화는 staging 파이프라인 상태만 비우는 절차이며,
      `/home/pia/mou/staging/incoming`, `/home/pia/mou/staging/archive` 원본 입력 폴더는 건드리지 않음.
    - 재테스트 전에 결과만 비우고 입력은 유지해야 할 때는 이 순서를 그대로 따르면 됨.
  - 검증:
    - staging MinIO 버킷 4개 객체 수 `0`.
    - `staging.duckdb` 없음.
    - staging Dagster runtime DB 파일(`runs.db`, `schedules.db`, run shard DB) 없음.

- **staging MinIO에서 콘솔 다운로드가 11%에서 끊기는 현상을 객체 손상과 분리해서 진단**:
  - 증상:
    - MinIO Console에서 예를 들어
      - `vlm-labels/tmp_data/fall_down/detections/20250711_am_pk_fc_00000000_00014000_00000002.json`
      파일을 받을 때
      - `Error: Unexpected response, download incomplete.`
      가 뜨며 11% 근처에서 중단됨.
  - 원인 분석:
    - 먼저 `prod` MinIO와 `staging` MinIO를 분리해서 확인.
    - 대상 객체는 staging API(`9002`)에 실제로 존재했고,
      `boto3`/직접 GET으로는 `200 OK`로 정상 다운로드됨.
    - 즉 파일 손상이나 업로드 실패가 아니라,
      `MinIO Console(9003) -> API(9002)` 다운로드 경로 또는 브라우저 세션 쪽 문제로 좁혀짐.
  - 결론:
    - staging MinIO가 고장난 것이 아니라,
      브라우저 기반 Console 다운로드 경로에서만 재현되는 문제였음.
  - 운영 기준:
    - 콘솔 다운로드 실패가 나오면 바로 객체를 지우지 말고,
      1. API endpoint로 `head/get` 확인
      2. presigned URL 또는 `boto3.download_file()`로 우회 다운로드
      3. 그 다음에야 객체 손상 여부를 판단
      순서로 확인해야 함.

- **`clip_to_frame`의 `ffmpeg_clip_extract_failed: File ... already exists. Overwrite? [y/N]` 문제를 근본 원인 기준으로 수정**:
  - 증상:
    - `clip_to_frame`에서 여러 asset이 다음 오류로 반복 실패:
      - `ffmpeg_clip_extract_failed: File '/tmp/tmp....mp4' already exists. Overwrite? [y/N] Not overwriting - exiting`
  - 실제 원인:
    - clip 출력용 temp mp4 경로를 `NamedTemporaryFile(delete=False)`로 먼저 생성하고 있었음.
    - ffmpeg는 이미 존재하는 출력 파일을 받으면 기본적으로 overwrite를 물어보는데,
      현재 호출은 비대화식 실행이라 자동으로 `N` 처리되어 종료.
    - 즉 timestamp, JSON, 원본 영상 문제가 아니라
      "출력 temp 파일을 미리 만들어둔 상태에서 ffmpeg에 넘긴 구현"이 직접 원인이었음.
  - 조치:
    - clip 추출 helper를 수정해 temp 파일을 미리 만들지 않고,
      "아직 존재하지 않는 temp 경로 문자열"만 생성해 ffmpeg 출력 경로로 넘기도록 변경.
    - 실패 시 partial temp 파일이 남아 있으면 cleanup 하도록 보강.
  - 운영 기준:
    - ffmpeg 출력 파일은 `-y`로 덮어쓰기 강제하거나,
      더 안전하게는 이번처럼 "존재하지 않는 temp 경로"를 쓰는 방식이 낫다.
    - 재발 시 원본 파일이나 timestamp부터 의심하기보다,
      temp output 생성 방식과 overwrite 옵션부터 먼저 확인하는 것이 빠름.

- **Gemini `400 Request payload size exceeds the limit: 524288000 bytes` 문제를 대용량 preview 경로로 해결**:
  - 증상:
    - `clip_timestamp` 단계에서 대형 원본 영상에 대해
      - `400 Request payload size exceeds the limit: 524288000 bytes`
      오류 발생.
  - 실제 원인:
    - Gemini 호출 시 원본 영상을 그대로 `read_bytes()` 해서 payload로 보내고 있었음.
    - 500MB를 넘는 영상은 Vertex AI request size 제한에 바로 걸림.
  - 조치:
    - 일정 크기(`>450MB`)를 넘는 영상은 Gemini 호출 전에
      - 오디오 제거
      - 해상도/프레임레이트/비트레이트 축소
      된 preview mp4를 생성한 뒤 그 파일을 Gemini에 보내도록 수정.
    - 첫 preview가 아직도 크면 더 낮은 설정으로 한 번 더 줄이는 fallback도 추가.
  - 운영 메모:
    - 이 방식은 실패 회피 목적일 뿐 아니라,
      Gemini 입력 비용 계산에서도 실제 원본 오디오 토큰이 빠지게 되므로
      `large-file preview path`와 `raw-source path`의 비용/시간 계산을 분리해서 봐야 함.
  - 검증:
    - staging 기준 동일 유형의 500MB 초과 오류는 더 이상 바로 재현되지 않도록 코드 보강 완료.

- **staging 비용/시간 산정 문서를 만들어 Gemini Pro/Flash, 이미지 추출, YOLO-World 처리량을 정리**:
  - 산출물:
    - `STAGING_INCOMING_LABELING_REPORT.md`
  - 포함 내용:
    - `/home/pia/mou/staging/incoming` 현재 적재 영상 `38개`, 총 `43.32h`, 총 `22.74GB`
    - `Gemini 2.5 Pro` 기준 비용/시간 추정
    - `Gemini 2.5 Flash` 기준 비용/시간 추정
    - 달러와 원화를 함께 표기한 환산표
    - 현재 clip 이미지 추출 설정값과 실측 시간
    - 현재 YOLO-World(`yolov8l-worldv2`) 실측 처리 시간
  - 운영 의미:
    - 향후 비슷한 staging 배치가 들어왔을 때
      "현재 런타임 기준으로 Gemini/clip/image/yolo가 어느 정도 비용과 시간이 드는지"
      다시 손계산하지 않고 문서를 바로 참고할 수 있게 됨.

- **라벨 저장 경로 정책을 정리하고 code path도 맞추기 시작함**:
  - 정책 정리:
    - Gemini 이벤트 JSON:
      - `vlm-labels/<raw_parent>/events/<video_stem>.json`
    - YOLO detection JSON:
      - `vlm-labels/<raw_parent>/detections/<image_or_clip_stem>.json`
  - 추가 조치:
    - `clip_to_frame`에서 더 이상 `vlm-processed/.../events/*.json`를 생성하지 않도록 수정.
    - build 단계에서는 라벨 원본을 `vlm-processed`가 아니라 `vlm-labels`에서 가져가도록 조정.
  - 운영 의미:
    - 이벤트 JSON과 detection JSON이 같은 버킷 안에서도 용도별 prefix로 분리되어,
      사람이 MinIO에서 직접 찾기 쉬워지고 downstream 정책도 단순해짐.
  - 참고:
    - 사용자 요청대로 이 경로 변경은 코드에만 반영했고,
      일부 서비스는 재시작 전이라 즉시 런타임 적용 상태는 아님.

## 2026-03-11
- **`dagster-staging`을 운영 인스턴스와 분리해 heartbeat 충돌을 해소**:
  - 증상:
    - `dagster-staging` 기동 직후
      - `Another SENSOR daemon is still sending heartbeats`
      - `Another ASSET daemon is still sending heartbeats`
      - `Another QUEUED_RUN_COORDINATOR daemon is still sending heartbeats`
      로그가 반복.
    - staging sensor들이 `DuckDB not found: /data/staging.duckdb`로 skip.
  - 원인:
    - staging이 운영 `dagster`와 같은 Dagster 인스턴스 상태를 공유하고 있었음.
    - `DAGSTER_HOME`과 run/schedule/event storage가 분리되지 않아 daemon heartbeat가 서로 충돌.
    - staging 전용 DB 파일도 준비되지 않은 상태였음.
  - 조치:
    - `docker/app/dagster_home_staging/dagster.yaml`을 추가해 staging run/event/schedule/local artifact storage를 `/data/dagster_home_staging/storage`로 분리.
    - `dagster-staging`은 `DAGSTER_HOME=/app/dagster_home_staging`, `DUCKDB_PATH=/data/staging.duckdb`를 사용하도록 고정.
    - `docker/data/staging.duckdb`를 준비해 staging 전용 DuckDB 인스턴스를 분리.
  - 검증:
    - heartbeat 충돌 로그가 사라짐.
    - `dagster-staging`이 `/data/staging.duckdb`를 인식하고 sensor tick을 정상 수행.

- **staging incoming 경로 미인식 문제를 해결하고 실제 ingest 진행까지 확인**:
  - 증상:
    - 호스트에는 `/home/pia/mou/staging/incoming` 아래 파일이 계속 이동 중이었지만,
      staging 컨테이너 안에서는 `.manifests`만 보이고 실제 비디오 파일은 보이지 않음.
    - 이 때문에 manifest 생성이 안 되거나, staging이 멈춘 것처럼 보였음.
  - 원인:
    - `dagster-staging` 서비스에 staging 전용 bind mount가 빠져 있었음.
    - 기본 공통 볼륨은 `/home/pia/mou/incoming -> /nas/incoming`, `/home/pia/mou/archive -> /nas/archive`만 연결하고 있었음.
  - 조치:
    - `dagster-staging`에
      - `/home/pia/mou/staging/incoming -> /nas/staging/incoming`
      - `/home/pia/mou/staging/archive -> /nas/staging/archive`
      마운트를 추가.
    - 컨테이너 재기동 후 `/nas/staging/incoming/tmp_data/...` 실제 파일 가시성 확인.
    - `.manifests/pending`, `.manifests/processed` 생성과 `incoming_manifest_sensor`/`auto_bootstrap_manifest_sensor` 로그를 함께 점검.
  - 관찰:
    - 초기에는 `복사 안정화 대기 중`, `backpressure: in_flight=2` 로그 때문에 정지처럼 보였지만,
      실제로는 안정화 기준과 in-flight limit 때문에 순차 처리 중이었음.
  - 검증:
    - 시점 기준 staging DB 적재 현황:
      - `raw_files=58`
      - `video_metadata=58`
      - `labels=80`
      - `processed_clips=80`
      - `image_metadata=0`
      - `image_labels=0`
    - 로그에서 `MinIO 업로드 완료: vlm-raw/tmp_data/...`가 연속 출력되는 것 확인.

- **staging `auto_labeling_job`의 Gemini credential 누락을 복구**:
  - 증상:
    - `clip_timestamp` 단계에서
      - `FileNotFoundError: Gemini credentials not found. Set GEMINI_GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS, or GEMINI_SERVICE_ACCOUNT_JSON.`
      오류로 `auto_labeling_job` 실패.
  - 원인:
    - credential JSON 파일은 컨테이너에 존재했지만,
      staging용 env에 `GEMINI_GOOGLE_APPLICATION_CREDENTIALS`, `GEMINI_PROJECT`, `GEMINI_LOCATION`이 빠져 있었음.
  - 조치:
    - `docker/.env.staging`에
      - `GEMINI_PROJECT=gmail-361002`
      - `GEMINI_LOCATION=us-central1`
      - `GEMINI_GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/gmail-361002-cbcf95afec4a.json`
      - `GEMINI_SERVICE_ACCOUNT_JSON=`
      추가.
    - staging 컨테이너 재생성 후 `resolve_gemini_credentials_path()` 결과가 위 경로로 나오는지 확인.
  - 검증:
    - `Gemini credentials not found` 오류는 재발하지 않음.
    - 시점 기준 `video_metadata.auto_label_status`:
      - `completed 34`
      - `failed 1`
      - `pending 23`

- **운영 Dagster UI에서 `manual_label_import`, `yolo_image_detection` 노출을 분리**:
  - 요구:
    - 운영 `pipeline-dagster-1`에서는 수동 import와 YOLO detection 자산/잡/센서를 숨기고,
      staging에서만 계속 노출하고 싶다는 요청이 있었음.
  - 조치:
    - `src/vlm_pipeline/definitions.py`에
      - `ENABLE_MANUAL_LABEL_IMPORT`
      - `ENABLE_YOLO_DETECTION`
      분기 추가.
    - 운영 env는 둘 다 `false`, staging env는 둘 다 `true`로 설정.
  - 검증:
    - 운영 `pipeline-dagster-1`:
      - `manual_label_import` asset/job 숨김
      - `yolo_image_detection` asset/job/sensor 숨김
    - staging `pipeline-dagster-staging-1`:
      - 두 기능 모두 계속 노출

- **YOLO-World 모델 위치와 기본 클래스 구성을 고정하고 서비스 기동을 정상화**:
  - 증상:
    - YOLO 서비스가 기대 모델 위치를 보지 못하거나, 컨테이너 내부 `clip` dependency 문제로 기동에 실패할 수 있는 상태였음.
    - 추가로 `.pth` 체크포인트는 현재 Ultralytics YOLO-World 서버 구조와 바로 호환되지 않는 상태였음.
  - 조치:
    - 현재 서버가 실제로 사용하는 모델을 `yolov8l-worldv2.pt`로 확정.
    - 모델 파일을 `docker/data/models/yolo/yolov8l-worldv2.pt`로 이동.
    - `YOLO_MODEL_PATH=/data/models/yolo/yolov8l-worldv2.pt`로 고정.
    - `YOLO_DEFAULT_CLASSES`를 추가해 기본 탐지 클래스를 comma-separated env로 관리.
    - YOLO 이미지에 `git`, `git+https://github.com/ultralytics/CLIP.git`를 반영해 부팅 dependency를 복구.
  - 검증:
    - `pipeline-yolo-1`이 `healthy` 상태로 올라옴.
    - `/health`에서 `model_loaded=true` 확인.

- **staging DB 초기화와 브랜치 전환 방해 요소를 정리**:
  - staging DB 초기화:
    - 사용자 요청에 따라 `staging.duckdb` 데이터를 모두 비우고 스키마만 유지한 새 DB로 재구성.
    - 이후 백업 파일도 삭제.
  - 브랜치 전환 문제:
    - `runs.db`, `schedules.db`, `.nux`, `.telemetry` 같은 staging 런타임 파일 때문에 `git switch`가 막힘.
  - 조치:
    - staging Dagster storage를 git 추적 경로 밖(`/data/dagster_home_staging/storage`)으로 이동.
    - staging 런타임 파일이 코드 작업 브랜치에 dirty change를 남기지 않도록 구조를 정리.
    - 필요 시 staging 컨테이너를 중지한 뒤 브랜치 전환하도록 운영 절차를 명확히 함.
  - 운영 메모:
    - staging에서 실제 파일 처리를 계속 돌릴 경우 `.nux`, `.telemetry` 같은 비핵심 런타임 파일은 다시 생길 수 있으므로,
      브랜치 전환 직전에는 컨테이너 상태와 untracked 파일을 같이 확인하는 것이 안전함.

## 2026-03-10
- **Gemini 모듈을 파이프라인 내부로 통합하고 Docker 의존성을 반영**:
  - 배경:
    - 기존 `src/gemini` 폴더가 파이프라인 코드와 분리되어 있었고, Docker 이미지에는 `google-cloud-aiplatform`이 설치되어 있지 않았음.
    - 컨테이너 안에서는 `vertexai`, `google.cloud.aiplatform` import가 실패하는 상태였음.
  - 조치:
    - 공용 Gemini 로직을 `src/vlm_pipeline/lib/gemini.py`, `src/vlm_pipeline/lib/gemini_prompts.py`로 이관.
    - 기존 진입점인
      - `src/gemini/video.py`
      - `src/gemini/gemini/gemini_api.py`
      - `src/gemini/assets/config.py`
      는 호환 wrapper로 변경해 기존 호출 경로를 유지.
    - `docker/app/requirements.txt`, `pyproject.toml`에 `google-cloud-aiplatform` 추가.
    - Docker 이미지 재빌드 후 `app`, `dagster`에 새 dependency 반영.
  - 추가 장애 및 해결:
    - 이미지 재빌드 후 `app`/`dagster` recreate 과정에서 임시 컨테이너 이름 충돌이 발생.
    - 충돌 컨테이너를 제거하고 `app`, `dagster`를 다시 올려 정상 상태 복구.
    - 초기 wrapper에는 `VIDEO_PROMPT`, `IMAGE_PROMPT` export가 누락되어 있어 추가 호환 패치 수행.
  - 검증:
    - `pipeline-app-1`, `pipeline-dagster-1` 내부에서
      - `vertexai`
      - `vlm_pipeline.lib.gemini`
      - 기존 `gemini.*` wrapper
      import가 모두 성공하는 것 확인.
    - 참고: Google 라이브러리에서 Python 3.10 EOL 관련 FutureWarning이 출력되지만 현재 실행에는 문제 없음.

- **`video_metadata` 기반 비디오 프레임 추출 기능 구현**:
  - 목표:
    - 비디오 원본을 다시 매번 분석하지 않고, `video_metadata.duration_sec`, `fps`, `frame_count`를 활용해 적응형 프레임을 추출.
    - 추출 결과는 MinIO `vlm-processed`에 저장하고, `image_metadata`에 메타를 적재.
  - 조치:
    - `image_metadata`를 1:1 이미지 테이블에서 다건 프레임 저장 구조로 확장.
    - `video_metadata.frame_extract_status`, `frame_extract_count`, `frame_extract_error`, `frame_extracted_at` 컬럼 추가.
    - `src/vlm_pipeline/lib/video_frames.py`에
      - duration/fps/frame_count 기반 frame timestamp planner
      - ffmpeg JPEG 추출 helper
      - deterministic key builder
      추가.
    - `src/vlm_pipeline/defs/process/assets.py`에 `extracted_video_frames` asset 추가.
    - `src/vlm_pipeline/definitions.py`에 `video_frame_extract_job` wiring 추가.
    - `src/vlm_pipeline/resources/duckdb_base.py`, `duckdb_ingest.py`에 migration/helper 로직 추가.
  - 적용 규칙:
    - duration 구간별 목표 프레임 수:
      - `<10s -> 3`
      - `10~30s -> 5`
      - `30~120s -> 8`
      - `>=120s -> 12`
    - low fps 영상은 과추출되지 않도록 cap 적용.
    - 구간은 기본적으로 영상 10%~90% 사이에서 균등 분할.
  - 검증:
    - Dagster definitions import 성공.
    - 운영 DB migration 이후 새 컬럼 생성 확인.
    - backlog 기준으로 추출 후보가 조회되는 것 확인.

- **프레임 저장 규칙을 여러 차례 보정하고 재실행 기준을 정립**:
  - 1차 문제:
    - 추출 이미지가 `vlm-processed/_tmp/...`에 먼저 올라간 뒤 복사되는 구조여서, 임시 prefix가 버킷에 남음.
  - 1차 해결:
    - `_tmp` 업로드 경로를 제거하고 최종 key로 바로 업로드하도록 수정.
    - 남아 있던 `_tmp` 객체 삭제 후 extraction run 재시작.
  - 2차 문제:
    - 저장 경로가
      - `vlm-processed/<prefix>/<video_stem>/frames/frame_0001_t0003500.jpg`
      형태라서, 원하는 구조보다 폴더가 한 단계 더 들어감.
  - 2차 해결:
    - `/frames/` segment 제거:
      - `vlm-processed/<prefix>/<video_stem>/frame_0001_t0003500.jpg`
    - 경로 정책 변경 후:
      - 진행 중 extraction/sync run 중지
      - `vlm-processed` 전체 삭제
      - `image_metadata.image_role='video_frame'` row 삭제
      - `video_metadata.frame_extract_*` 상태 초기화
      - extraction 재실행
  - 3차 문제:
    - 파일명이 `frame_0001...jpg` 형태라 원본 영상과의 연결이 약함.
  - 3차 해결:
    - 파일명 규칙을
      - `영상원본이름_00000001.jpg`
      - `영상원본이름_00000002.jpg`
      로 변경.
    - 샘플 run으로 확인 후 본 배치 재개.
  - 추가 정리:
    - `image_metadata.codec` 컬럼은 JPEG frame 저장 기준 실효성이 낮아 스키마/insert/migration에서 제거.
  - 운영 교훈:
    - 프레임 저장 경로 규칙을 바꾸면 DB만 고치는 것이 아니라,
      버킷 객체, `image_metadata`, `video_metadata.frame_extract_*` 상태를 함께 리셋해야 함.

- **자동 프레임 추출 센서 추가 및 향후 전환용 코드 주석화**:
  - 현재 활성 기능:
    - `video_frame_extract_sensor`를 추가해 `raw_files + video_metadata` backlog 기준으로 `video_frame_extract_job`가 자동 실행되도록 구성.
    - 후보 조회 우선순위를 `pending -> processing -> failed` 순으로 조정.
  - 향후 전환 준비:
    - `processed_clips` 완료 시 자동 추출하는 sensor/job/asset 설계를 코드에 주석 블록으로 추가.
    - `archive_path` prefix를 Dagster run config로 수동 지정해 프레임 추출하는 확장 설계도 주석으로 추가.
    - 현재 runtime wiring에는 연결하지 않고, future activation checklist만 코드에 남김.
  - 추가 스키마 준비:
    - `image_metadata.source_clip_id` 컬럼을 추가해 향후 `processed_clips` lineage도 담을 수 있게 확장.
  - 운영 의미:
    - 지금 동작은 유지하면서, 나중에 `raw_files` 기준 자동 추출에서 `processed_clips` 기준 자동 추출로 전환할 수 있는 준비를 완료함.

- **Dagster 분기/lineage 구조를 더 논리적으로 정리**:
  - 분기 조정:
    - `incoming_manifest_sensor`는 이제 `mvp_stage_job` 전체가 아니라 `ingest_job`만 자동 실행.
    - raw image dedup은 ingest와 분리해 backlog 전용 sensor로 독립 운영.
    - stuck guard는 writer job 전반을 대상으로 보강.
  - lineage 조정:
    - `processed_clips`는 `ingested_raw_files`와 `labeled_files`를 동시에 직접 물지 않고, `labeled_files`만 직접 upstream으로 보도록 정리.
    - `motherduck_sync`는 실제 동기화 대상 asset 전체를 upstream으로 명시해 graph가 현실과 맞도록 수정.
  - 결과:
    - 자동 흐름이
      - `gcp -> ingest -> dedup`
      - `ingest -> label -> process -> build`
      - `ingest -> extracted_video_frames`
      - `motherduck_sync <- 실제 생성 자산들`
      구조로 더 명확해짐.

- **`auto_bootstrap_manifest_sensor` 180초 timeout 문제 해결**:
  - 증상:
    - Dagster daemon에서
      - `DagsterUserCodeUnreachableError`
      - `Deadline Exceeded`
      가 발생하며 sensor tick이 180초 안에 끝나지 못함.
  - 원인:
    - 한 tick에서 너무 많은 unit을 스캔.
    - `.Trash-1000`, `.DS_Store` 등 hidden 항목까지 discovery 범위에 포함.
    - cursor를 실제 처리량보다 크게 이동시키는 구조 때문에 tick budget이 보수적으로 관리되지 못함.
  - 조치:
    - hidden 항목 discovery 제외.
    - 코드에서 `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK`보다 더 보수적인 safety cap `10` 적용.
    - 실제 처리한 unit 수만큼만 cursor를 전진.
    - discovery / scan elapsed, processed_units, budget 로그를 남기도록 보강.
  - 검증:
    - sensor tick이 180초 내에 완료되고 `Deadline Exceeded`가 재발하지 않음을 로그로 확인.

## 2026-03-09
- **MinIO raw 경로 prefix 정리 및 원인 확인**:
  - 증상:
    - `vlm-raw/2026/03/adlibhotel-event-bucket/...`
    - `vlm-raw/2026/03/kkpolice-event-bucket/...`
    - 형태의 객체가 존재해, 기대 경로인 `vlm-raw/<source_unit>/...`와 불일치.
  - 원인:
    - 과거 ingest 로직에서 `raw_key` 생성 시 `datetime.now().strftime("%Y/%m")`를 앞에 붙이는 코드가 존재했음.
    - 현재 `src/vlm_pipeline/defs/ingest/ops.py`는 이미 `source_unit_name/rel_path` 형태로 정리되어 있었지만, 과거 데이터가 남아 있었음.
  - 조치:
    - MinIO 객체를
      - `vlm-raw/2026/03/adlibhotel-event-bucket/...` -> `vlm-raw/adlibhotel-event-bucket/...`
      - `vlm-raw/2026/03/kkpolice-event-bucket/...` -> `vlm-raw/kkpolice-event-bucket/...`
      로 이동.
    - DuckDB `raw_files.raw_key`도 새 prefix로 일괄 갱신.
    - 기존 `2026/03/...` prefix 객체는 삭제.
  - 검증:
    - old prefix 잔존 객체 `0`.
    - `raw_key`도 old prefix 참조 `0` 확인.
    - 현재 ingest 코드 기준으로는 이후 신규 업로드 시 `YYYY/MM` prefix가 다시 붙지 않음.

- **전역 checksum 중복 조사 및 archive 원본 기준 재해시 수행**:
  - 배경:
    - MinIO 객체 키와 DB `raw_key`는 일치했지만, 동일 콘텐츠가 다른 key로 들어간 정황이 있어 전역 중복 여부를 다시 확인함.
  - 1차 확인 결과:
    - `raw_key` 중복 `0`.
    - `checksum` 중복 그룹 `182`, 총 `364`행을 먼저 확인.
    - `archive_path` 중복 그룹 `275`, 총 `583`행 확인.
  - 추가 조치:
    - archive 실제 파일을 기준으로 checksum을 다시 계산하는 스크립트 `scripts/recompute_archive_checksums.py` 추가.
    - 중복 의심 대상 `946`건을 archive 원본으로 재해시.
  - 재해시 결과:
    - 대상 `946`건
    - checksum 일치 `596`
    - archive 기준 checksum 변경 `350`
    - missing file `0`
    - file size 변경 `0`
  - 의미:
    - 단순히 DB checksum만 믿으면 안 되는 상태였고, archive 원본 기준 재검증이 필요했음.
    - 일부 duplicate miss는 기존 checksum 값이 잘못 저장되어 생긴 것으로 판단.

- **checksum 중복이 DB에 남은 근본 원인 분석**:
  - 기대 동작:
    - 같은 checksum이면 `raw_files`에 새 row가 들어가면 안 됨.
    - 현재 코드상 `find_by_checksum()`는 source unit 구분 없이 전체 `raw_files`를 조회함.
  - 실제 원인:
    - 운영 중이던 DuckDB에는 `UNIQUE(checksum)` 제약이 없었음.
    - 저장소에는 서로 다른 스키마가 공존:
      - `src/python/common/schema.sql`: `checksum VARCHAR`
      - `src/vlm_pipeline/sql/schema.sql`: `checksum VARCHAR UNIQUE`
    - `CREATE TABLE IF NOT EXISTS` 기반 초기화만 하고 있어, 예전에 잘못 생성된 테이블이 자동 수정되지 않았음.
    - 여기에 checksum 값 drift(`350`건)가 겹치면서 앱 레벨 중복 체크가 뚫린 케이스가 발생.
  - 결론:
    - "코드상 dedup"만으로는 부족했고, 실제 운영 DB 스키마 제약과 checksum 정확도까지 같이 맞춰야 했음.

- **전역 duplicate 정리 및 DB/스토리지 복구**:
  - 사전 백업:
    - 정리 전 DB를 `docker/data/pipeline.pre_dedup_20260309_063521.duckdb`로 백업.
  - 정리 전 상태:
    - `raw_files = 15922`
    - checksum 중복 그룹 `456`
    - duplicate row 총 `490`
    - `archive_path` 중복 그룹 `275`
  - 코드 보강:
    - `scripts/cleanup_duplicate_assets.py` 추가.
    - `src/vlm_pipeline/defs/ingest/duplicate.py` 수정.
    - `src/vlm_pipeline/resources/duckdb_ingest.py` 수정.
    - 목적:
      - duplicate marker(`duplicate_skipped_in_manifest`)에서 같은 basename이 여러 번 나와도 누락 없이 보존.
      - duplicate cleanup 시 기존 row 삭제 대신 "새 DB 재구성" 방식으로 안전하게 정리.
  - 운영 조치:
    - `dagster`, `app`를 순차 중지한 상태에서 정리 작업 수행.
    - duplicate row `490`건 삭제.
    - 해당 MinIO duplicate 객체 `490`개 삭제.
    - archive 실파일 duplicate `182`개 삭제.
  - 추가 장애 및 해결:
    - DB 교체 후 기존 `pipeline.duckdb.wal`이 남아 있어 stale WAL replay 문제 발생.
    - 해결:
      - `app` 중지 후 stale WAL을 `docker/data/pipeline.duckdb.wal.pre_dedup_stale_20260309_0640`로 분리 보관.
      - cleanup 스크립트에도 `CHECKPOINT` 후 DB swap, WAL 백업 이동 절차 추가.
  - 정리 후 검증:
    - `raw_files = 15432`
    - checksum 중복 그룹 `0`
    - `raw_key` 중복 그룹 `0`
    - `archive_path` 중복 그룹 `0`
    - MinIO objects `15432`
    - DB/MinIO 비교 시 `missing 0`, `extra 0`
    - 정리 전 duplicate 그룹 `456`개를 백업 DB와 비교했을 때 keeper marker mismatch `0`

- **archive / DuckDB / MinIO / MotherDuck 전체 정합 재확인 및 보정**:
  - 중복 정리 직후 카운트:
    - DuckDB `15432`
    - MinIO `15432`
    - MotherDuck `15922`
    - archive 실제 파일 수 `15487`
  - archive 추가 분석:
    - archive 쪽 초과 `55`개 확인.
    - 분류 결과:
      - `_DONE` marker `49`
      - `.DS_Store` `4`
      - 실제 미디어 파일 `2`
  - 처리 방식:
    - `.DS_Store` 4개 삭제.
    - `_DONE` marker는 운영 마커이므로 유지.
    - 실제 영상 2개는 checksum 확인 후 duplicate 여부 판단:
      - `weapon_298_020.mp4`: 중복 아님 -> MinIO/DuckDB 추가, `completed`
      - `weapon_298_03.mp4`: 중복 아님 + 손상 파일 -> MinIO/DuckDB 추가, `failed`
    - 이후 MotherDuck를 로컬 DuckDB 기준으로 `raw_files`, `video_metadata` 재동기화.
  - 최종 기준값:
    - DuckDB `raw_files = 15434`
    - DuckDB `video_metadata = 15433`
    - MinIO objects `15434`
    - MotherDuck `raw_files = 15434`
    - MotherDuck `video_metadata = 15433`
    - archive 데이터 파일 수 `15434`
    - archive 전체 물리 파일 수 `15483` (`_DONE` marker `49`개 포함)
    - status 분포: `completed 15433`, `failed 1`
    - checksum 중복 `0`
  - 운영 해석:
    - "데이터 파일" 기준 개수는 archive / MinIO / DuckDB / MotherDuck 모두 일치.
    - archive 전체 파일 수가 더 큰 이유는 `_DONE` marker를 보존했기 때문.

- **duplicate marker(`error_message`) 검증 및 보강**:
  - 요구사항:
    - 중복 데이터가 있다면 `error_message`에
      - `duplicate_skipped_in_manifest:<filename>`
      - 형태로 중복된 파일명 수만큼 남아 있어야 함.
  - 조사 결과:
    - 일부 그룹은 marker가 비어 있었고, 일부는 basename 중복 시 개수가 맞지 않는 문제가 있었음.
  - 조치:
    - duplicate marker merge 로직을 수정해 중복 basename도 보존되도록 변경.
    - cleanup 이후 surviving keeper row 기준으로 marker mismatch `0` 검증 완료.
  - 의미:
    - 이후 duplicate 발생 이력 추적 시 `error_message`만 봐도 어떤 파일이 skip되었는지 재구성 가능.

- **MinIO 버킷 자동 생성 보강**:
  - 문제:
    - `vlm-labels`, `vlm-processed`, `vlm-dataset`은 코드상 사용되지만, MinIO는 write 시 버킷을 자동 생성하지 않음.
    - 기존에는 `vlm-raw`만 존재하고 나머지는 선생성되지 않아 관련 작업 시 실패 가능성이 있었음.
  - 조치:
    - `src/vlm_pipeline/resources/minio.py` 수정.
    - `upload()`, `upload_fileobj()`, `copy()` 실행 전에 목적지 버킷을 `_ensure_bucket_once()`로 보장하도록 패치.
    - 동일 프로세스 내 중복 호출 비용을 줄이기 위해 bucket cache + lock 추가.
  - 검증:
    - 버킷 생성 후 현재 MinIO bucket:
      - `vlm-raw`
      - `vlm-labels`
      - `vlm-processed`
      - `vlm-dataset`
    - `app`, `dagster`, `minio` 재시작 후에도 정상 확인.

- **Dagster stale STARTED run 정리**:
  - 증상:
    - Dagster UI에서 2개 run이 1시간 이상 `STARTED`로 유지.
  - 조사 결과:
    - run id:
      - `06349bfd-d236-4e39-b2d9-c9447c48d4ca`
      - `975ca406-3ae2-46d3-8b2a-bf2984850039`
    - 두 run 모두 `mvp_stage_job`.
    - 마지막 이벤트는 duplicate skip 관련 로그만 남아 있고, 실제 worker 프로세스는 존재하지 않았음.
    - UI/DB에는 `STARTED`로 남아 있었지만 실제 실행은 종료된 dangling run 상태.
  - 조치:
    - DagsterInstance API로 두 run을 `CANCELED` 처리.
  - 검증:
    - active run `0`.
    - sensor backpressure(`in_flight=2`) 해소 확인.

- **서비스 재시작 및 코드 반영**:
  - 오늘 작업 중 `app`, `dagster`, `minio`를 여러 차례 재시작해 반영 상태를 확인.
  - 최종적으로 코드 반영 후 주요 서비스가 모두 `Up` 상태인 것을 재확인.

- **오늘 작업에서 사용/추가한 핵심 파일**:
  - 신규:
    - `scripts/recompute_archive_checksums.py`
    - `scripts/cleanup_duplicate_assets.py`
  - 수정:
    - `src/vlm_pipeline/defs/ingest/duplicate.py`
    - `src/vlm_pipeline/resources/duckdb_ingest.py`
    - `src/vlm_pipeline/resources/minio.py`

- **문제 해결 접근 방식 정리**:
  - 1. "개수 mismatch"와 "콘텐츠 duplicate"를 분리해서 봄.
    - 단순 row 수 비교만으로는 문제를 설명할 수 없어서 `raw_key`, `archive_path`, `checksum`을 각각 독립적으로 검사.
  - 2. DB 값만 믿지 않고 archive 원본을 기준으로 재검증.
    - checksum drift가 있었기 때문에 NAS 원본 파일을 다시 해시해서 판단 기준을 재구성.
  - 3. 운영 DB를 직접 부분 삭제하지 않고, 백업 후 재구성 방식으로 복구.
    - 실패 시 rollback 가능하도록 백업 DB와 stale WAL을 모두 별도 보관.
  - 4. 정리 후에도 "DB만 맞는 상태"에서 멈추지 않음.
    - MinIO, MotherDuck, archive 실제 물리 파일 수까지 같이 맞춰 최종 정합을 확보.
  - 5. 재발 방지 코드를 같이 반영.
    - duplicate marker 보강, cleanup script 개선, MinIO bucket auto-create를 함께 넣어 같은 종류의 장애가 다시 반복되지 않도록 함.

## 2026-03-06
- **MinIO 엔드포인트 오설정(9001) → S3 API 포트(9000) 정정**:
  - 증상:
    - MinIO 업로드/검증 시 API 호출이 불안정하거나 실패.
    - 콘솔 포트(`9001`)와 S3 API 포트(`9000`)가 혼용됨.
  - 원인:
    - 런타임 환경변수에서 `MINIO_ENDPOINT`가 콘솔 URL 기준으로 일부 참조됨.
  - 조치:
    - `docker/docker-compose.yaml`, `.env.example`, `docker/.env`의 MinIO endpoint를
      `http://172.168.47.36:9000`으로 통일.
  - 검증:
    - 컨테이너 환경변수/실행 로그 기준으로 동일 endpoint 사용 확인.

- **INGEST MinIO 업로드 재활성화 + raw_key 경로 정책 변경**:
  - 요구사항:
    - 기존 `vlm-raw/YYYY/MM/{project}/...` 대신 `vlm-raw/{project}/...`로 업로드.
  - 코드 수정:
    - `src/vlm_pipeline/defs/ingest/ops.py`
      - MinIO 업로드 호출(바이트 업로드/스트림 업로드/fallback) 재활성화.
      - `raw_key` 생성에서 `YYYY/MM` prefix 제거, `source_unit_name/rel_path`만 유지.
      - 로그를 `MinIO 업로드 완료`로 복원.
  - 검증:
    - 신규 샘플 key가 `khon-kaen-rtsp-bucket/...`, `adlibhotel-event-bucket/...` 형태인지 확인.

- **\"계속 YYYY/MM으로 업로드되는\" 회귀 원인 분석 및 복구**:
  - 증상:
    - 코드 수정 후에도 일부 업로드가 `vlm-raw/2026/03/...`로 계속 생성.
  - 접근:
    - 1단계: 코드 경로 점검 (`ops.py` raw_key 생성 로직 재확인).
    - 2단계: 런타임 환경 점검 (`DATAOPS_DUCKDB_PATH`, `DUCKDB_PATH` 실제값 확인).
    - 3단계: 데이터 근거 점검 (DuckDB `raw_files.raw_key` 샘플 조회).
    - 4단계: 프로세스 점검 (백그라운드 잔존 업로드 프로세스 여부 확인).
  - 원인:
    - `docker/.env`의 DuckDB 경로가 잘못되어 일부 프로세스가 다른 DB를 참조.
    - 과거 설정으로 실행 중이던 잔존 reupload 프로세스가 이전 key 규칙으로 업로드 지속.
  - 조치:
    - `docker/.env` 수정:
      - `DATAOPS_DUCKDB_PATH=/data/pipeline.duckdb`
      - `DUCKDB_PATH=/data/pipeline.duckdb`
    - app 컨테이너 재생성으로 환경변수 재적용.
    - 잔존 python/reupload 프로세스 종료 후 버킷 재정리.
  - 결과:
    - 신규 업로드 key는 `vlm-raw/{project}/...` 규칙으로 수렴.

- **Archive 재업로드 운영 도구 추가 및 정리 작업**:
  - 코드 추가:
    - `scripts/reupload_minio_from_archive.py`
      - `raw_files`의 `archive_path` + `raw_key`를 기준으로 MinIO 재업로드.
      - 기존 key 스킵, 진행률/실패 샘플 출력.
  - 운영 조치:
    - 테스트 업로드 및 검증 후, 불필요한 테스트 오브젝트/버킷 삭제.
    - 경로 정책 변경 이후 필요 구간 재업로드 수행.

- **문제 해결 접근 방식(재발 대응용)**:
  - 1단계(증상 고정):
    - \"코드 수정 후에도 이전 경로 업로드\"를 로그/오브젝트 key로 고정.
  - 2단계(가설 분리):
    - 코드 문제 vs 환경변수/DB 문제 vs 잔존 프로세스 문제로 분리.
  - 3단계(증거 수집):
    - 코드(`ops.py`) + env(`docker/.env`) + DB(`raw_key 샘플`) + 프로세스(`ps`)를 교차 확인.
  - 4단계(복구 실행):
    - 잘못된 env 수정 → 컨테이너 재생성 → 잔존 프로세스 종료 → 버킷 정리 → 재업로드.
  - 5단계(완료 기준):
    - 신규 key prefix가 `/{project}/`만 사용되고 `YYYY/MM` 재발이 없는지 확인.

## 2026-03-05
- **RUN 장애(ingested_raw_files / raw_files 테이블 미존재) 복구**:
  - 증상:
    - run_id `6e42571c-c99c-4733-9f81-87ce63f3fc73`에서 `ingested_raw_files` 단계 실패.
    - 에러: `Catalog Error: Table with name raw_files does not exist`.
  - 원인:
    - DuckDB 파일은 존재하지만 스키마 초기화가 보장되지 않은 상태에서
      `find_by_checksum()`, `has_raw_file()`가 즉시 `raw_files` 조회를 수행.
    - 특히 신규/초기화된 DB 경로에서 ingest 시작 시 테이블 생성 race가 발생 가능.
  - 코드 수정:
    - `src/vlm_pipeline/resources/duckdb.py`
      - `ensure_schema()` 추가.
      - `src/vlm_pipeline/sql/schema.sql`을 읽어 `CREATE TABLE IF NOT EXISTS` 일괄 실행.
    - `src/vlm_pipeline/defs/ingest/assets.py`
      - `ingested_raw_files()` 시작 직후 `db.ensure_schema()` 호출 추가.
  - 운영 조치:
    - Dagster 재시작(`docker compose up -d --force-recreate dagster`).
    - stuck run `6e42571c-...`를 `CANCELED`로 정리 후 후속 run 재개.
    - 신규 run `7bb0536f-...` `SUCCESS` 확인.
  - 검증:
    - `/data/pipeline.duckdb`에서 `raw_files`, `image_metadata`, `video_metadata`, `labels`,
      `processed_clips`, `datasets`, `dataset_clips` 생성 확인.
    - 이후 ingest 단계에서 동일 `raw_files not found` 재발 없음.

- **Queue/Started 적체 운영 안정화 + 자동 회복 경로 적용**:
  - 배경:
    - `duckdb_writer=true` 태그를 공유하는 job 간 동시성 제한으로 `QUEUED` 누적.
    - 장시간 `STARTED`가 슬롯을 점유하면 backlog가 빠르게 증가.
  - 코드/설정 수정:
    - `src/vlm_pipeline/defs/ingest/sensor.py`
      - `incoming_manifest_sensor` tick당 enqueue 상한:
        - `INCOMING_SENSOR_MAX_NEW_RUN_REQUESTS_PER_TICK` (기본 2)
      - `stuck_run_guard_sensor` 추가:
        - 오래 정체된 `STARTED`(target job + `duckdb_writer=true`) 자동 cancel.
        - 옵션으로 자동 requeue(`STUCK_RUN_GUARD_AUTO_REQUEUE_ENABLED`).
    - `src/vlm_pipeline/definitions.py`
      - `stuck_run_guard_sensor` 등록.
    - `.env.example` / `docker/.env`
      - stuck run guard 및 per-tick enqueue 관련 env 추가.
  - 운영 조치:
    - `QUEUED` run 일괄 정리 후 Dagster 재기동.
    - guard sensor 동작 로그 확인(`Checking for new runs for sensor: stuck_run_guard_sensor`).
  - 기대 효과:
    - 장시간 정체 run이 있어도 자동 정리되어 슬롯 회복.
    - backlog 유입 속도를 tick 상한으로 제어.

- **문제 접근 방식(재발 대응 포맷)**:
  - 1단계(증상 고정):
    - run_id 단위로 실패 step/첫 SQL 에러를 고정(`raw_files not found`).
  - 2단계(가설 분리):
    - 코드 문제(DDL 보장 누락) vs 운영 문제(STARTED/QUEUED 적체)로 분리.
  - 3단계(재현/검증):
    - 로그 + DB 테이블 존재 여부 + run 상태(STARTED/CANCELING/QUEUED) 동시 확인.
  - 4단계(영구 수정):
    - ingest 진입점에 schema 보장 코드 삽입(`db.ensure_schema()`).
  - 5단계(운영 복구):
    - stuck run 정리 → queue drain → daemon 재시작 → 후속 run 성공 확인.
  - 6단계(예방):
    - stuck guard + tick당 enqueue 상한으로 backlog와 슬롯 고갈을 구조적으로 완화.

- **INGEST 오류 파일 미삽입/미이동 정책 적용**:
  - 배경:
    - `file_missing`, `empty_file`, `ffprobe_failed` 같은 파일 자체 오류가 `raw_files`에 실패 행으로 누적되어
      운영 지표/동기화 정합을 오염시키는 문제 확인.
  - 코드 수정:
    - `src/vlm_pipeline/defs/ingest/ops.py`
      - `register_incoming()` 검증 실패 시 DB 삽입 제거.
      - `normalize_and_archive()` 메타 추출 실패 시 DB 실패행 삽입 제거.
      - `ingest_rejections` 수집 구조 추가(메모리 기반).
    - `src/vlm_pipeline/defs/ingest/assets.py`
      - `ingest_rejections`를 JSONL 파일로 저장:
        - 기본 경로: `<manifest_dir>/failed/<manifest_id>.jsonl`
        - 환경변수: `INGEST_FAILURE_LOG_DIR`

- **Transient(DuckDB lock) 오류 자동 재시도 경로 추가**:
  - 코드 수정:
    - `src/vlm_pipeline/defs/ingest/ops.py`
      - lock/conflict 계열 오류를 transient로 분류.
      - transient 실패 파일을 `retry_candidates`로 수집.
    - `src/vlm_pipeline/defs/ingest/assets.py`
      - ingest 종료 시 per-file retry manifest 자동 생성:
        - `retry_of_manifest_id`, `retry_attempt`, `retry_reason=transient_db_lock`
      - 재시도 상한 환경변수:
        - `INGEST_TRANSIENT_RETRY_MAX_ATTEMPTS` (기본 3)
    - `src/vlm_pipeline/defs/ingest/sensor.py`
      - retry manifest 필드를 로드/태그 전파하도록 보강.

- **Archive 이동 예외 복구 보강**:
  - 코드 수정:
    - `src/vlm_pipeline/defs/ingest/assets.py`
      - move 예외 발생 시 `dest.exists()` 즉시 재확인.
      - 목적지 파일이 있으면 `completed + archive_path`로 복구 처리.
      - 목적지 미존재일 때만 `archive_move_failed` 유지.

- **Sensor 큐 적체 재발 방지(backpressure) 강화**:
  - 배경:
    - queue/pending manifest 누적 시 처리 지연 및 lock 충돌 확률 증가.
  - 코드 수정 (`src/vlm_pipeline/defs/ingest/sensor.py`):
    - 센서 주기 환경변수화 + 기본 완화:
      - `INCOMING_SENSOR_INTERVAL_SEC` 기본 180초
      - `AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC` 기본 180초
    - pending 보호 상한:
      - `AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS` (기본 200)
    - tick당 manifest 생성 상한:
      - `AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK` (기본 20)

- **GCP 다운로드 0바이트 파일 방어 로직 추가**:
  - 배경:
    - GCS 원본은 정상인데 로컬 다운로드 결과가 간헐적으로 0바이트인 케이스 확인.
  - 코드 수정:
    - `gcp/download_from_gcs_rclone.py`
      - 폴더 다운로드 후 0바이트 미디어 파일 탐지.
      - 0바이트 파일 삭제 후 폴더 단위 재다운로드 재시도.
      - 재시도 후 잔존 시 해당 폴더 실패 처리(`_DONE` 미생성).
      - 신규 옵션/환경변수:
        - `--zero-byte-retries`
        - `GCS_ZERO_BYTE_RETRIES` (기본 2)
    - `src/vlm_pipeline/defs/gcp/assets.py`
      - Dagster asset config에 `zero_byte_retries` 추가 및 CLI 전달.

- **운영 데이터 보정 + MotherDuck 정합 복구 완료**:
  - 사전 백업:
    - `/data/pipeline.duckdb.bak_20260305_015527`
  - 보정 수행:
    - `archive_move_failed` 1건 복구(`completed` 전환).
    - failed row 94건 삭제:
      - `file_missing`, `empty_file`, `ffprobe_failed:%`,
        `IO Error: Could not set lock on file%`
  - 결과:
    - 로컬: `raw_files=8016`, `video_metadata=8016`, `failed=0`
    - MotherDuck sync 후 동일 상태 확인.

- **문제 해결 접근 회고(재사용 포맷)**:
  - 1단계(사실 확인): 증상 수치화(`status/error/count`) + lock holder/경로 실존 여부부터 확인.
  - 2단계(오염 차단): 실패 데이터가 운영 테이블에 남지 않도록 write-path를 먼저 차단.
  - 3단계(재처리 경로): 일시 오류는 재시도 큐(manifest), 영구 오류는 로그 분리(JSONL).
  - 4단계(큐 제어): 생성/소비 속도 균형을 상한값(`pending`, `per_tick`)으로 강제.
  - 5단계(정합 검증): 로컬→MotherDuck까지 동일 쿼리로 pre/post 비교 후 마감.

## 2026-03-04
- **auto_bootstrap 스캔 윈도우 + cursor v3 적용 (대량 incoming 부하 완화)**:
  - 배경:
    - tick마다 `/nas/incoming`를 전수 스캔하면서 source unit이 많은 구간에서 sensor 지연이 발생.
  - 코드 수정:
    - `src/vlm_pipeline/defs/ingest/sensor.py`
      - 경량 unit 탐색(`_discover_source_units`) + tick별 부분 스캔(`_select_units_for_tick`) 도입.
      - cursor 스키마를 v3로 확장하고 `scan_offset` 상태를 저장/복원.
      - tick당 스캔/생성 상한 신규 환경변수 추가:
        - `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK` (기본 5)
        - `AUTO_BOOTSTRAP_MAX_READY_UNITS_PER_TICK` (기본 5)
      - 로그에 `scan_window`, `deferred_ready`, `scan_elapsed` 노출 추가.

- **Archive 경로 정책 단일화 (`/nas/archive` 루트 기준)**:
  - `src/vlm_pipeline/defs/ingest/assets.py`
    - archive 경로를 월별(`/YYYY/MM`) 기준에서 archive 루트 기준으로 정렬.
    - source unit 폴더 충돌 시 `__N` suffix 분기 정책 유지.
  - `gcp/download_from_gcs_rclone.py`
    - archive done-marker 조회를 `archive/<bucket>/<folder>` 우선으로 수정.
    - legacy `archive/<YYYY>/<MM>/<bucket>/<folder>` fallback 유지.
  - `scripts/recover_uploading.py`
    - 복구 스크립트의 이동 대상 경로도 archive 루트 기준으로 정렬.

- **INGEST duplicate 안내 메시지 병합 정책 반영**:
  - `src/vlm_pipeline/resources/duckdb.py`
    - `mark_duplicate_skipped_assets()`가 기존 `duplicate_skipped_in_manifest:*` marker가 있으면
      파일명 목록을 merge 누적하도록 수정.
    - duplicate marker가 아닌 기존 `error_message`는 덮어쓰지 않도록 보호.

- **GCP 다운로드 설정 운영값 복귀 + schedule 완화**:
  - `src/vlm_pipeline/defs/gcp/assets.py`
    - 임시 테스트 스코프 강제 코드(`TEMP_FORCE_TEST_SCOPE`) 제거.
    - bucket/date-folder는 run config/env 기반으로만 주입되도록 정리.
  - `src/vlm_pipeline/definitions.py`
    - `gcs_download_schedule` cron을 `0 */6 * * *`로 조정.

- **INGEST MinIO 업로드 임시 스킵 모드 반영**:
  - `src/vlm_pipeline/defs/ingest/ops.py`
    - MinIO 업로드 호출을 주석 처리하고
      `MinIO 업로드 스킵(비활성화)` 로그로 실행 상태를 명시.

- **브랜치 동기화**:
  - upstream 병합 반영 후 `feature/yeongwoo` 최신화 (`a713906`).

## 2026-03-03
- **auto_bootstrap 대량 파일 manifest 분할 + enqueue 충돌 완화**:
  - 배경:
    - source unit 파일 수가 많을 때 단일 manifest로 생성되며 pending 폭증/처리 지연이 발생.
    - `incoming_manifest_sensor`에서 in-flight/backpressure 상태와 source unit 중복 판단이 겹치며 enqueue 지연이 발생.
  - 코드 수정:
    - `src/vlm_pipeline/defs/ingest/sensor.py`
      - `AUTO_BOOTSTRAP_MAX_FILES_PER_MANIFEST`(기본 100) 도입.
      - source unit 파일을 manifest chunk 단위로 분할 생성.
      - manifest 메타 확장:
        - `source_unit_dispatch_key`
        - `source_unit_total_file_count`
        - `source_unit_chunk_index`
        - `source_unit_chunk_count`
      - `incoming_manifest_sensor`에서 source unit 중복 판단/run_key 계산을
        `source_unit_dispatch_key` 우선으로 변경.
      - `INCOMING_SENSOR_MAX_IN_FLIGHT_RUNS`(기본 2) 기반 backpressure 보호 추가.
      - pending 중복 manifest 최신 1개만 선택, 나머지는
        `.manifests/processed/*.superseded*.json`으로 이동.

- **chunked manifest 아카이브 처리 보정**:
  - `src/vlm_pipeline/defs/ingest/assets.py`
    - chunked manifest(`source_unit_chunk_count > 1`)는 폴더 전체 이동 조건에서 제외.
    - chunk 업로드 성공 파일은 동일 unit archive 경로에 누적 이동하도록 보정.

- **레거시 코드 경로 정리**:
  - 삭제:
    - `update_minio_uploader/`
    - `update_python_scanner/`
  - `src/vlm_pipeline/defs/sync/assets.py`
    - legacy sync fallback 경로(`/update_minio_uploader/...`) 제거.

- **MinIO 업로드 경로 한글 폴더명 영문화 적용**:
  - `src/vlm_pipeline/lib/sanitizer.py`
    - `sanitize_path_component()` 추가 (한글 로마자화 + ASCII-safe slug).
  - `src/vlm_pipeline/defs/ingest/ops.py`
    - `raw_key` 생성 시 파일명뿐 아니라 폴더 세그먼트도 정규화:
      - `source_unit_name`
      - `rel_path`의 디렉터리 부분

- **운영 확인 메모**:
  - `incoming_manifest_sensor`가 RUNNING이어도 in-flight run이 limit 이상이면
    backpressure로 enqueue를 건너뛸 수 있음을 로그로 재확인.
  - 장시간 `STARTED` run 1건(`c8dfd278...`) 점유 시 신규 enqueue 지연 가능.

## 2026-02-27
- **MinIO endpoint 스키마 누락 오류 복구**:
  - 증상: `MINIO_ENDPOINT=minio:9000` 환경에서 `Invalid endpoint: minio:9000` 발생.
  - 코드 수정:
    - `src/vlm_pipeline/resources/minio.py`
      - endpoint 정규화 추가(`http://`/`https://` 없으면 `http://` 자동 보정).
      - 적용 위치: `boto3.client(..., endpoint_url=...)` 생성 경로.
  - 검증:
    - 단위 테스트: `tests/unit/test_minio_resource.py`에 endpoint 정규화 케이스 추가.
    - 런타임 확인: Dagster 컨테이너에서 `MINIO_ENDPOINT=minio:9000` 상태로
      `client.meta.endpoint_url == http://minio:9000` 확인.

- **INGEST 중복 이력 반영 로직(`duplicate_skipped_in_manifest`) 복구**:
  - 배경:
    - 리팩토링 이후 `vlm_pipeline` 경로에서 중복 감지는 유지되나,
      기존 원본 자산에 대한 duplicate skip 이력 반영이 누락되어 Worklog 정책과 불일치.
  - 코드 수정:
    - `src/vlm_pipeline/resources/duckdb.py`
      - `mark_duplicate_skipped_assets(duplicate_asset_ids, manifest_id)` 추가.
      - `raw_files.error_message`가 NULL/빈 문자열인 자산에만
        `duplicate_skipped_in_manifest:<manifest_id>` 기록.
    - `src/vlm_pipeline/defs/ingest/assets.py`
      - normalize 결과에서 `duplicate_of:<asset_id>` 대상 수집.
      - manifest 단위로 `mark_duplicate_skipped_assets()` 호출 및 로그 출력.
  - 검증:
    - 단위 테스트: `tests/unit/test_duckdb_resource.py`에
      "error_message 비어있는 자산만 업데이트" 케이스 추가.

## 2026-02-26
- **Docker 이미지 빌드 시 CLI 자동 설치 설정 추가 (재가동/재빌드 미수행)**:
  - `docker/Dockerfile`
    - `google-cloud-cli`(`gcloud`) 자동 설치 단계 추가
    - `rclone` 자동 설치 단계 추가
    - Google Cloud apt repository/key 등록 단계 추가
  - `docker/app/requirements.txt`
    - CLI 의존성은 pip가 아니라 Dockerfile에서 관리됨을 명시

- **Dagster GCS 다운로드 연동 추가**:
  - `docker/app/dagster_defs.py`
    - 신규 asset: `pipeline/gcs_download_to_incoming`
    - 신규 job: `gcs_download_job`
    - 신규 schedule: `gcs_download_schedule`
      - cron: `0 3 * * *`
      - timezone: `Asia/Seoul`
      - 기본 run_config:
        - `mode=date-folders`
        - `download_dir=/nas/incoming/gcp`
        - `backend=auto`
        - `skip_existing=true`
        - `dry_run=false`
  - 백엔드 실패 정책:
    - `auto` 모드에서 `rclone/gcloud`가 모두 없으면 즉시 실패(`RuntimeError`).
  - 다중 버킷 지원 추가:
    - 대상 버킷: `khon-kaen-rtsp-bucket`, `adlib-hotel-202512`
    - `gcs_download_schedule` 기본 config에 `buckets` + `bucket_subdir=true` 반영
    - 저장 경로: `/nas/incoming/gcp/<bucket>/<date-folder>/...`

- **incoming source unit 분류 확장**:
  - `docker/app/dagster_defs.py::_group_files_by_source_unit()`
    - 기본 규칙은 유지
    - `/nas/incoming/gcp/<date-folder>/...`는 `gcp/<date-folder>` 단위로 묶도록 확장
    - 목적: GCP 다운로드 데이터를 날짜 폴더 단위로 안정적으로 ingest

- **GCP 다운로드 스크립트 단일화**:
  - `gcp/download_from_gcs_rclone.py`
    - 단일 엔트리로 통합
    - 모드 분리:
      - `date-folders`
      - `legacy-range`
    - 공통 백엔드 정책 유지: `auto|rclone|gcloud`
  - `gcp/download_from_gcs.sh`
    - 단일 Python 엔진(`download_from_gcs_rclone.py`) 호출 래퍼로 정리

- **GCP 테스트 정비**:
  - `tests/unit/test_gcp_download_from_gcs_rclone.py`
    - mode 파싱 케이스(`date-folders`) 추가
  - `tests/unit/test_gcp_download_date_folders.py`
    - 테스트 대상을 단일 엔진(`download_from_gcs_rclone.py`)으로 전환

- **GCP 문서 루트 병합 (migrated)**:
  - `gcp/CLAUDE.md`, `gcp/worklog.md` 내용을 루트 문서로 병합
  - 출처 표기: `(migrated from gcp/worklog.md)`
  - 병합 후 GCP 개별 문서 삭제

## migrated from gcp/worklog.md
- **2026-02-10 - 데이터 파이프라인 연동 설정**:
  - Docker에서 GCP 실행 경로 마운트 적용 (`../gcp:/gcp:ro`)
  - 다운로드 기본 경로 NAS 지정
  - 파이프라인 접목 흐름 문서화
- **2026-02-02 - 초기 설정 TODO**:
  - GCP 프로젝트/서비스 계정/키 파일/접근 권한 및 다운로드 테스트 항목 정리

## 2026-02-25
- **`raw_files.error_message` 중복 안내 메시지 기록 범위 수정**:
  - 배경:
    - `duplicate_skipped_in_manifest:*`가 duplicate 관련 행만이 아니라
      동일 manifest의 성공 행 전반에 기록되는 현상 확인.
  - 코드 수정:
    - `docker/app/dagster_defs.py`
      - 조건: `if duplicate_skips and successful_assets:` → `if duplicate_skips:`
      - 대상: `successful_assets` 기반 갱신 제거
      - 변경: `duplicate_skips[].duplicate_asset_id` 집합만 조회/업데이트
      - 기존 보호 로직(`error_message`가 NULL/빈 문자열일 때만 기록) 유지
  - 정적 검증:
    - `python3 -m py_compile docker/app/dagster_defs.py` 통과

- **기존 DB 정리 SQL 적용 시도 (보류)**:
  - 목표:
    - 과거 오기입된 `duplicate_skipped_in_manifest:*` 메시지 정리
  - 시도:
    - 호스트 DuckDB 직접 접속: 파일 권한/락으로 실패
    - 컨테이너(`pipeline-dagster-1`) DuckDB 접속: 실행 중 writer lock 충돌로 실패
      - lock holder: `/usr/bin/python3.10` (PID 446290)
  - 예정 SQL:
    - `UPDATE raw_files`
      `SET error_message = NULL, updated_at = CURRENT_TIMESTAMP`
      `WHERE error_message LIKE 'duplicate_skipped_in_manifest:%';`
  - 상태:
    - Dagster writer lock 해제 후 재실행 필요

## 2026-02-23
- **INGEST/Archive 잔류 원인 분석 및 로그 추출**:
  - 증상: incoming 폴더 일부가 archive로 이동되지 않고 잔류.
  - 로그 분석:
    - run `e5775aa1-cf51-4d75-9c8e-f9d2aceb4138`에서 `expected=1600, success=1596, skipped=4` 확인.
    - `중복 건너뜀` 4건으로 인해 기존 "폴더 전체 성공 조건" 미충족.
  - 산출 로그:
    - `duplicate_lists/incoming_archive_debug_20260223.log`
    - `duplicate_lists/incoming_archive_core_20260223.log`
    - `duplicate_lists/incoming_archive_duplicates_20260223.log`

- **폴더 단위 archive 정책 변경 (중복 제외 부분 이동)**:
  - 요구사항: 중복 파일 때문에 전체 이동이 멈추지 않도록 개선.
  - 코드 수정:
    - `docker/app/dagster_defs.py`
      - 기존: 폴더 단위는 `success==expected && skipped==0`일 때만 전체 이동.
      - 변경: `failed==0 && skipped>0`이면 **성공 파일만 부분 archive 이동**.
      - 중복/스킵 파일은 incoming에 남김.
      - 부분 이동 후 빈 디렉토리 정리 로직 추가.
  - 적용:
    - Dagster 재기동 후 반영.

- **DB 보존 정책 변경 (이동된 파일만 DB 유지)**:
  - 요구사항:
    - archive로 실제 이동된 파일만 `ingest_status=completed`.
    - 이동되지 않은 파일(중복/검증실패/이동실패)은 DB에서 삭제.
  - 코드 수정:
    - `docker/app/dagster_defs.py`
      - `_delete_asset_rows(asset_id)` 유틸 추가(`raw_files`, `image_metadata`, `video_metadata` 정리).
      - 중복/검증실패 시 `raw_files` 미삽입.
      - 업로드 후 archive 이동 실패 시 MinIO 객체 best-effort 삭제 + DB 삭제.
      - 폴더 이동 실패/보류/부분이동 실패 건 DB 정리 + 카운트 보정.
  - 운영 반영:
    - 기존 잔여 중복 레코드 정리 수행.
    - 로컬 DuckDB 상태 확인: `raw_files = completed 1596`, `failed 0`, `duplicate_of 0`.

- **실데이터 정리 수행 (incoming → archive)**:
  - 대상: `/home/pia/mou/incoming/AX지원사업`
  - 결과:
    - 이동: 1596건 → `/home/pia/mou/archive/2026/02/AX지원사업`
    - 잔류: 중복 4건(incoming 유지)
    - 실패: 0건

- **MotherDuck 동기화 대상 DB 정렬 (`pipeline_db`)**:
  - 코드/설정 수정:
    - `docker/app/dagster_defs.py`: `DEFAULT_MOTHERDUCK_DB = "pipeline_db"`
    - `update_minio_uploader/local_duckdb_to_motherduck_sync.py`: `--db` 기본값 `pipeline_db`
    - `docker/.env`: `MOTHERDUCK_DB=pipeline_db`
  - 동기화 실행:
    - `pipeline_db` bootstrap + share 생성 + MVP 7테이블 sync 완료.
    - 검증: MotherDuck `pipeline_db` `raw_files = completed 1596`.

- **MotherDuck/로컬 정리 작업**:
  - `_bak_` 테이블 삭제(로컬 DuckDB + MotherDuck `pipeline_db`):
    - `_bak_raw_files`, `_bak_labels`, `_bak_processed_clips` 제거.
  - MotherDuck 구 DB 제거:
    - 구 DB 삭제(share 정리 후 drop 완료).

- **Grafana 복구/운영 조치**:
  - `pipeline-grafana-1` 포트 충돌(`3000`) 해소 및 기동 전환.
  - 기존 대시보드가 사라진 이슈는 볼륨 분기 원인으로 판명:
    - 복구 소스: `grafana_dp_grafana-data`
    - 복구 대상: `pipeline_grafana_data`
  - `MySQL`, `labeling` 데이터소스 및 라벨링 대시보드 복구 확인.
  - Provisioned 대시보드 `NAS Folder Tree Analytics` datasource를 `labeling` → `nas_tree`로 변경:
    - `docker/grafana/nas-folder-tree-dashboard.json` UID 치환 후 재기동/반영 확인.

## 2026-02-20 (오전)
- **Video 환경 메타데이터(실내/실외 + 주/야) INGEST 통합**:
  - 목적:
    - 기존 `video_metadata` 기술 메타(ffprobe)에 환경 분류 메타를 함께 적재.
    - `data_in_out_check.py`(Places365) + `data_in_out_check_2.py`(샘플 voting/heuristic) 로직 결합.
  - 코드 수정:
    - `src/vlm_pipeline/lib/video_env.py` 신규:
      - 비디오 샘플 프레임 추출(FFmpeg) + N개 샘플 voting.
      - CUDA 우선(`torch.cuda.is_available()`) Places365 추론.
      - 실패/미설치 시 heuristic fallback(ingest 중단 방지).
    - `src/vlm_pipeline/lib/video_loader.py`:
      - `load_video_once()`에서 `classify_video_environment()` 호출.
      - `video_metadata`에 아래 컬럼 추가 채움:
        - `environment_type`, `daynight_type`, `outdoor_score`, `avg_brightness`, `env_method`
    - `src/vlm_pipeline/resources/duckdb.py`:
      - `insert_video_metadata()` INSERT 컬럼/값 확장.
    - 스키마/마이그레이션:
      - `src/vlm_pipeline/sql/schema.sql`
      - `src/python/common/schema.sql`
      - `src/python/common/migration.sql`
      - `video_metadata` 확장 컬럼 추가 + 기존 DB `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` 반영.
  - 테스트:
    - `pytest tests/unit/test_video_loader.py tests/unit/test_duckdb_resource.py` 통과.
    - `pytest tests/unit` 전체 통과(`46 passed`).
    - `pytest tests/integration/test_ingest_pipeline.py` 통과.
  - 런타임 검증:
    - 샘플 mp4 기준 `load_video_once()` 호출 시 환경 메타 반환/기록 확인.

- **CUDA 실사용 경로 활성화 (torch/torchvision cu124 설치)**:
  - 코드 수정:
    - `docker/app/requirements.txt`:
      - `--extra-index-url https://download.pytorch.org/whl/cu124`
      - `torch==2.5.1+cu124`
      - `torchvision==0.20.1+cu124`
    - `docker/docker-compose.yaml`:
      - `app`, `dagster`에 `gpus: all` 명시.
  - 운영 조치:
    - `docker compose build app`
    - `docker compose up -d --force-recreate app dagster`
  - 검증:
    - app/dagster 컨테이너에서 공통 확인:
      - `torch_version=2.5.1+cu124`
      - `cuda_available=True`
      - `device_count=2` (`NVIDIA RTX A4000` 확인)
    - CUDA 텐서 연산(matmul) 수행 성공으로 실제 GPU 실행 확인.

- **Places365 모델 캐시 고정 + 재다운로드 정책 고정**:
  - 코드 수정:
    - `docker/docker-compose.yaml` (`app`, `dagster`):
      - `VIDEO_ENV_MODEL_DIR=/data/models/places365`
      - `VIDEO_ENV_AUTO_DOWNLOAD=false`
  - 효과:
    - 모델 파일(`wideresnet18_places365.pth.tar`, `wideresnet.py`, `IO_places365.txt`)을 `/data` 볼륨에 영구 저장.
    - 컨테이너 재생성 후에도 재다운로드 없이 재사용.
    - 운영 중 네트워크 상태에 덜 의존하는 추론 경로 확보.
  - 검증:
    - 환경변수 반영 확인(`app`, `dagster`).
    - `/data/models/places365` 파일 존재 확인.
    - 재호출 시 파일 mtime 불변(재다운로드 없음) 확인.

## 2026-02-19
- **Incoming 복사중 데이터 오처리 방지 + 폴더 단위 archive 이동 구현**:
  - 요구사항:
    - incoming에 폴더 단위로 데이터가 들어오면 전 파일 처리 성공 시 폴더째 archive로 이동.
    - 복사 중(예: 100개 중 20개만 복사됨)에는 ingest가 시작되지 않도록 안정화 대기.
  - 코드 수정:
    - `docker/app/dagster_defs.py`:
      - `auto_bootstrap_manifest_sensor`를 source unit(폴더/단일 파일) 기반으로 변경.
      - 안정화 게이트 추가:
        - `AUTO_BOOTSTRAP_STABLE_CYCLES`(기본 2회 연속 동일)
        - `AUTO_BOOTSTRAP_STABLE_AGE_SEC`(기본 120초 경과)
      - unit 시그니처(`file_count:total_size:max_mtime_ns`) 기반으로 복사 완료 판단 후 manifest 생성.
      - manifest 필드 확장:
        - `source_unit_type`, `source_unit_path`, `source_unit_name`, `stable_signature`
      - 폴더 단위 ingest 모드 추가:
        - 파일별 업로드/DB 완료 후 전 파일 성공일 때만 폴더 전체 이동.
        - archive 경로 충돌 시 `__2`, `__3` suffix 자동 부여.
        - 폴더 이동 후 `raw_files.archive_path`를 파일별 실제 경로로 일괄 업데이트.
      - 하위호환:
        - 기존 sensor cursor(`known_paths`)를 신규 cursor(`units`)와 함께 읽어 재생성 폭주 방지.
    - `docker/.env`:
      - `AUTO_BOOTSTRAP_STABLE_CYCLES=2`
      - `AUTO_BOOTSTRAP_STABLE_AGE_SEC=120`
  - 운영 정책(반영 결과):
    - 복사 안정화가 확인되기 전에는 manifest를 만들지 않음.
    - `source_unit_type=directory` manifest는 `success == file_count`일 때만 폴더 archive 이동.
    - 일부 실패/중복 발생 시 폴더는 incoming에 유지되어 재검토 가능.
  - 검증:
    - 정적 검증: `python3 -m py_compile docker/app/dagster_defs.py` 통과.

## 2026-02-13
- **INGEST 실운영 장애 대응 및 경로 정렬**:
  - `docker/.env`:
    - `INCOMING_HOST_PATH=/home/pia/mou/incoming`로 확정.
    - MotherDuck 동기화 운영값 추가:
      - `MOTHERDUCK_DB=pipeline_db`
      - `MOTHERDUCK_SYNC_ENABLED=true`
      - `MOTHERDUCK_SYNC_DRY_RUN=false`
      - `MOTHERDUCK_SHARE_UPDATE=MANUAL`
  - `docker/docker-compose.yaml` 기준 `app`, `dagster` 재기동 후 `/nas/incoming` 마운트 실파일 확인.
  - incoming 파일(한글/공백 포함 파일명 포함) 컨테이너 내 가시성 확인 완료.

- **manifest 생성/소비 흐름 재검증**:
  - `scripts/bootstrap_manifest.sh` 기준 host path → container path 치환 규칙 재확인.
  - manifest pending → processed 이동 동작 확인.
  - auto bootstrap sensor + incoming manifest sensor 연쇄 트리거 경로 점검 완료.

- **INGEST 실패 원인 수정 (MinIO import 오류)**:
  - 증상: `ModuleNotFoundError: No module named 'minio'`로 ingest step 실패.
  - 조치: `datapipeline:gpu-cu124` 이미지 재빌드 후 `app`, `dagster` 컨테이너 재생성.
  - 결과: ingest 재실행 성공, MinIO(`vlm-raw`) 업로드 정상화.

- **archive 이동 반영 누락 버그 수정**:
  - 문제: 파일은 `/nas/archive/YYYY/MM/`로 이동됐지만 `raw_files.archive_path`가 NULL로 남는 케이스 존재.
  - 수정 파일:
    - `docker/app/dagster_defs.py`:
      - INGEST 완료 UPDATE 시 `archive_path = COALESCE(?, archive_path)` 반영.
    - `src/vlm_pipeline/resources/duckdb.py`:
      - `update_raw_file_status()`에 `archive_path` 파라미터 추가.
      - UPDATE SQL에 `archive_path = COALESCE(?, archive_path)` 반영.
    - `src/vlm_pipeline/defs/ingest/ops.py`:
      - archive move 성공 시 `db.update_raw_file_status(..., archive_path=archived_path)` 전달.
  - 데이터 보정:
    - 기존 `ingest_status='completed' AND archive_path IS NULL` 행 대상 백필 수행.
    - 완료 후 NULL 건수 0 확인.

- **DuckDB 반영/락 상태 점검**:
  - `raw_files`, `image_metadata` 반영 건수 재확인 (배치 ingest 성공 기준).
  - Dagster `tag_concurrency_limits`(`duckdb_writer=true`, limit=1) 유지 상태 점검.
  - 운영 가이드:
    - 수시 조회는 읽기 전용 연결(`duckdb -readonly`) 권장.
    - Writer job(INGEST/DEDUP/SYNC)과 수동 조회 동시 실행 최소화 권장.

- **MotherDuck sync 단계 파이프라인 재연결**:
  - 기존 스크립트 `update_minio_uploader/local_duckdb_to_motherduck_sync.py`는 유지하고,
    Dagster 단계형 파이프라인에 optional sync asset을 재추가.
  - `docker/app/dagster_defs.py`:
    - `motherduck_sync` asset 추가 (`built_dataset` 이후 deps 연결).
    - `mvp_stage_job` 실행 순서 확장:
      - `ingested_raw_files → dedup_results → labeled_files → processed_clips → built_dataset → motherduck_sync`
    - 단독 실행용 `motherduck_sync_job` 추가.
    - `MOTHERDUCK_SYNC_ENABLED` 비활성 시 skip 처리, 토큰 미설정 시 graceful skip 처리.
    - `share_update` 값 검증(`MANUAL|AUTOMATIC`) 및 기본 MANUAL fallback.
  - `.env.example`:
    - MotherDuck sync 옵션 섹션 추가 (`MOTHERDUCK_TOKEN`, `MOTHERDUCK_DB`, sync on/off 등).
  - 검증:
    - Dagster Definitions에서 `pipeline/motherduck_sync`, `motherduck_sync_job` 로드 확인.
    - `motherduck_sync_job` 실행 성공 확인 (`local_rows/insert/update` 로그 출력).

- **MotherDuck 불일치 재발 대응 (스키마/데이터 강제 정렬)**:
  - 증상:
    - MotherDuck 구 DB가 과거 스키마/데이터를 유지 (`raw_files=627`, `image_metadata` 없음).
    - 로컬 Docker DuckDB(`raw_files=9`, `image_metadata=7`)와 불일치.
  - 원인:
    - 기존 `local_duckdb_to_motherduck_sync.py`가 `raw_files`만 부분 동기화.
  - 조치:
    - `update_minio_uploader/local_duckdb_to_motherduck_sync.py`를
      MVP 7테이블(`raw_files`, `image_metadata`, `video_metadata`, `labels`,
      `processed_clips`, `datasets`, `dataset_clips`) 단위
      `CREATE OR REPLACE TABLE ... AS SELECT * FROM local_db.<table>` 방식으로 변경.
    - attached/local catalog 판별 로직(`information_schema.tables`) 수정.
    - dry-run/실동기화 모두 검증 후 `motherduck_sync_job` 재실행 성공 확인.
  - 결과:
    - 로컬 vs MotherDuck 건수 일치 확인:
      - `raw_files 9/9`, `image_metadata 7/7`, `video_metadata 0/0`,
        `labels 0/0`, `processed_clips 0/0`, `datasets 1/1`, `dataset_clips 0/0`.

- **컨테이너 재기동 및 런타임 검증**:
  - `pipeline-dagster-1`, `pipeline-app-1` 재생성/재기동 후 정상 상태 확인.
  - MotherDuck 관련 환경변수 컨테이너 반영 확인.
  - ingest + archive + DuckDB + MotherDuck sync까지 단계별 smoke 확인 완료.

- **Dagster MotherDuck sync 정체 장애 복구/안정화**:
  - 증상:
    - `pipeline__motherduck_sync` 단계가 `STARTED` 상태에서 정체된 run 발생.
    - `duckdb_writer=true` tag concurrency(limit=1)로 인해 이후 queued run이 대기.
  - 코드 수정:
    - `docker/app/dagster_defs.py`:
      - `motherduck_sync` config에 `timeout_sec` 추가.
      - `subprocess.run(..., timeout=timeout_sec)` 적용 및 `TimeoutExpired` 예외 처리 추가.
      - `incoming_manifest_sensor`의 run_config에 `pipeline__motherduck_sync` 설정(`enabled/db/dry_run/share_update/timeout_sec`) 명시 주입.
    - `docker/.env`, `.env.example`:
      - `MOTHERDUCK_SYNC_TIMEOUT_SEC=600` 추가.
  - 운영 조치:
    - stale run(`f2de65fc-64d9-4398-a233-2a499a522c0e`)을 실패 처리해 큐 블로킹 해소.
    - Dagster 컨테이너 재기동 후 센서/큐 정상 처리 확인.
  - 검증:
    - run `7e5f5891-91a0-4cae-a28d-e6bc2b2503bc`에서
      `pipeline__motherduck_sync` 실행 및 `MotherDuck sync completed successfully` 로그 확인.
    - 해당 run 최종 상태 `RUN_SUCCESS` 확인(`2026-02-13 06:19:54 +0000`).

- **전체 코드 리팩토링/불필요 파일 정리 (요청 반영)**:
  - 정리 기준:
    - 제외: `gcp/`, `split_dataset/`, `*.md`
    - 대상: 레거시 코드/운영 산출물/캐시 파일
  - 코드/구조 정리:
    - `src/rust_scanner/` 삭제 (미사용 레거시 Rust 스캐너 제거).
    - `src/python/scanner/` 삭제 (레거시 파이썬 스캐너 잔존 디렉토리 정리).
    - `scripts/setup.sh` 최신 구조로 정리:
      - Rust 빌드/구식 실행 안내 제거
      - `pyproject` 기반 설치(`pip install -e`) 및 현행 compose/verify 명령으로 갱신
  - 산출물/캐시 정리:
    - `docker/airflow/logs/` 삭제 (컨테이너 권한으로 정리).
    - 프로젝트 전역 `__pycache__`, `.pytest_cache`, `.DS_Store`, `._.DS_Store`, `*.pyc` 정리
      (요청 제외 경로인 `split_dataset/`은 유지).
  - git ignore 강화:
    - `.gitignore`에 `docker/airflow/logs/`, `docker/app/dagster_home/storage/`,
      `docker/app/dagster_home/history/`, `._.DS_Store` 추가.
  - 검증:
    - 단위 테스트: `pytest -q tests/unit` → `43 passed`.
    - 통합 테스트: `pytest -q tests/integration` → `6 passed`.
    - 리팩토링 후 프로젝트 용량 약 `187MB → 32MB`로 감소.

- **phash/dup_group_id + raw_bucket 누락 이슈 수정 (MotherDuck 정합 복구)**:
  - 증상:
    - MotherDuck `raw_files`에서 `phash`, `dup_group_id`, `raw_bucket`가 다수 NULL로 확인.
    - 로컬 DuckDB에서도 동일 누락 확인되어 sync 문제가 아닌 upstream 데이터 누락으로 판별.
  - 원인:
    - DEDUP 로직이 `source_path`만 참조해 archive 이동 후 파일을 찾지 못함
      (`phash_source_missing:/nas/incoming/...` 누적).
    - 과거 ingest 경로에서 `raw_bucket`이 NULL로 저장된 데이터가 잔존.
  - 코드 수정:
    - `docker/app/dagster_defs.py`:
      - DEDUP 조회 컬럼 확장: `source_path`, `archive_path`, `raw_bucket`, `raw_key`.
      - pHash 입력 소스 우선순위 변경:
        1) `archive_path`
        2) `source_path`
        3) MinIO(`raw_bucket/raw_key`) 다운로드 fallback
      - pHash 성공 시 `error_message = NULL`로 정리.
      - INGEST INSERT/UPDATE에서 `raw_bucket` 보강:
        - duplicate/fail insert에 `raw_bucket` 저장
        - uploading insert에 `raw_bucket` 저장
        - completed update에 `raw_bucket = COALESCE(raw_bucket, 'vlm-raw')` 반영
    - `src/python/common/phash.py`:
      - `compute_phash(path_or_bytes)`로 확장해 bytes 입력(MinIO fallback) 지원.
  - 운영 조치:
    - `dedup_job` 재실행 (`limit=5000`, `threshold=5`) 성공.
      - 결과: `phash_updated=10`, `dup_group_updates=8`
    - 기존 데이터 `raw_bucket` 백필:
      - 조건: `raw_key` 존재 + `raw_bucket` NULL/빈값
      - 결과: `12 -> 0`
    - `motherduck_sync_job` 재실행 후 컨테이너 재시작.
  - 검증:
    - Local `raw_files`:
      - `phash_filled=12/12`, `dup_group_filled=8/12`, `raw_bucket_null=0/12`
    - MotherDuck `raw_files`:
      - `phash_filled=12/12`, `dup_group_filled=8/12`, `raw_bucket_null=0/12`
    - 샘플 행 비교에서 로컬/원격 `phash`, `dup_group_id`, `raw_bucket('vlm-raw')` 일치 확인.

- **clip_metadata 테이블 제거 및 재발 방지 반영**:
  - 요청:
    - 불필요한 `clip_metadata` 테이블 제거.
  - 현황 확인:
    - 로컬 DuckDB: 운영 테이블 목록에 없음.
    - MotherDuck 구 DB: `clip_metadata`가 잔존.
  - 코드 수정:
    - `update_minio_uploader/local_duckdb_to_motherduck_sync.py`:
      - `LEGACY_TABLES_TO_DROP = ['clip_metadata']` 추가.
      - sync 실행 시 legacy 테이블 존재하면 `DROP TABLE IF EXISTS` 자동 수행.
      - 로그 추가: `[DROP] ...`, `[SYNC] dropped_legacy_tables=...`.
  - 운영 조치:
    - 로컬 DB에 `DROP TABLE IF EXISTS clip_metadata` 실행.
    - `motherduck_sync_job` 재실행하여 MotherDuck에서 `clip_metadata` 삭제 반영.
      - 로그 확인: `[DROP] table=clip_metadata`.
  - 검증:
    - Local: `information_schema.tables` 조회 결과 `clip_metadata` 없음.
    - MotherDuck: 동일 조회 결과 `clip_metadata` 없음.

## 2026-02-12
- **MVP 재개 v2 (Dual Path + Strict 8) 구현 완료**:
  - `docker/docker-compose.yaml`, `docker/.env` 정렬:
    - `DATAOPS_PIPELINE_ROOT`, `DATAOPS_NAS_ROOT` -> `/nas/datasets/projects`
    - `DATAOPS_NAS_ROOTS` -> `/nas/datasets/projects;/nas/incoming`
    - `/nas/datasets/projects` bind mount 추가(`PROJECTS_HOST_PATH=/home/pia/mou/nas_192tb/datasets/projects`)
    - `/nas/archive`, `/nas/incoming` 추가 마운트 유지
  - `/nas/datasets` 상위 마운트는 nested mountpoint 생성을 위해 writable로 조정.

- **Strict 8 마이그레이션 적용 (Docker 내부만 사용)**:
  - 유지보수 모드 진입 후 `/data/pipeline.duckdb` 백업 생성:
    - `/data/pipeline.duckdb.bak.20260212_062510`
  - `src/python/common/schema.sql` + `src/python/common/migration.sql` 적용.
  - `clip_metadata` 운영 테이블 제거, `_bak_clip_metadata`로 보존.
  - 최종 운영 테이블:
    - `raw_files`, `image_metadata`, `video_metadata`, `labels`,
      `processed_clips`, `datasets`, `dataset_clips`

- **코드/스크립트 정렬**:
  - `update_python_scanner/run_scanner.py`:
    - 기본 루트 하드코딩(`/nas/datasets/projects/vietnam_data`) 제거
    - 기본 루트 `/nas/datasets/projects`로 고정
  - `docker/app/dagster_defs.py`:
    - 기본 스캔 루트 fallback에 `/nas/incoming` 병행 지원
  - `scripts/verify_mvp.sh`:
    - Strict 8(필수/금지 테이블) 검사 추가
    - Dual Path(`/nas/datasets/projects`, `/nas/incoming`) 검사 추가
    - DuckDB 조회를 `-readonly` + CSV scalar 파싱으로 변경(락/오탐 방지)

- **검증 결과**:
  - 런타임: `imagehash`, `PIL`, `korean_romanizer`, `pydantic_settings`, `ffprobe` OK
  - INGEST:
    - `/test_data/projects/vietnam_data/organized_videos/helemet_2` 스캔
    - `raw_files=179`, `video_metadata=179`
  - DEDUP:
    - synthetic 이미지 3건 등록 후 pHash/dup_group 갱신 확인
  - LABEL:
    - `raw_key` stem 매칭으로 `labels.asset_id` 연결 확인 (`stem_matched=1`)
  - PROCESS/BUILD:
    - `processed_clips=1`, `datasets=1`, `dataset_clips=1` 생성 확인
  - 회귀:
    - `nas_scan --dry-run`, `asset_catalog_uploader --dry-run` fatal 없음
  - E2E:
    - `scripts/verify_mvp.sh` 성공
  - 단계형 자산 실행:
    - `mvp_stage_job` 실행 시 `built_dataset`의 `INSERT OR REPLACE` 제약조건 오류 확인
    - `docker/app/dagster_defs.py`에서 `datasets` 저장을 `DELETE + INSERT`로 수정
    - 재실행 결과 `mvp_stage_job_success=True` 확인

- **복구 스모크**:
  - 최신 백업 DB를 임시 복제해 읽기 검증 수행.
  - 백업본에서 테이블 조회 성공으로 롤백 절차 유효성 확인.

- **프로젝트 구조 리팩토링 완료**:
  - 레거시 코드 삭제:
    - `src/python/scanner/` 폴더 전체 삭제 (AssetRecord, asset_catalog 기반 레거시)
    - `src/python/init_duckdb.py` 삭제 (update_python_scanner/init_duckdb.py로 대체됨)
    - `scripts/run_pipeline.sh`, `scripts/run_ingest.sh`, `scripts/run_minio.sh` 삭제 (없는 파일 참조)
    - `docker/airflow/dags_backup/` 삭제 대상 (권한 문제로 수동 삭제 필요)

  - 공유 유틸리티 모듈 생성 (`src/python/common/`):
    - `minio_client.py`: S3/MinIO 클라이언트 생성, 버킷 관리, 경로별 버킷 선택 로직 통합
    - `motherduck.py`: MotherDuck 연결, .env 로드, SQL 이스케이프 유틸리티 통합
    - `path_utils.py`: 로컬/Docker 경로 변환, 오브젝트 키 생성, 경로 정규화 로직 통합

  - `update_minio_uploader/` 리팩토링:
    - `asset_catalog_uploader.py`: 공유 모듈 import로 변경, 중복 함수 제거
    - `motherduck_asset_catalog_to_minio.py`: 공유 모듈 import로 변경, 중복 함수 제거
    - `clear_minio_buckets.py`: 공유 모듈 import로 변경, 중복 함수 제거
    - `local_duckdb_to_motherduck_sync.py`: 공유 모듈 import로 변경, 중복 함수 제거

  - `update_python_scanner/` dead code 제거:
    - `db_handler.py`: `_chunked_sequence()`, `get_all_checksums()` 제거 (미사용)
    - `scanner.py`: `scan_directory()`, `get_all_current_paths()` 제거 (미사용)
    - `config.py`: 코드 정리 및 docstring 추가

  - 문서 업데이트:
    - `CLAUDE.md`: 새 구조 반영 (공유 모듈 추가)
    - `WORKLOG.md`: 리팩토링 내역 추가

- **최종 폴더 구조**:
  ```
  Datapipeline-Data-data_pipeline/
  ├── src/python/
  │   └── common/           # 공유 유틸리티
  │       ├── config.py     # 설정 로더 (기존)
  │       ├── db.py         # DuckDB 연결 (기존)
  │       ├── schema.sql    # MVP 6테이블 스키마 (기존)
  │       ├── minio_client.py   # [신규] S3/MinIO 클라이언트
  │       ├── motherduck.py     # [신규] MotherDuck 연결
  │       └── path_utils.py     # [신규] 경로 유틸리티
  ├── update_python_scanner/    # NAS 스캐너 + 파일 정리
  ├── update_minio_uploader/    # MinIO 업로더
  ├── split_dataset/            # 데이터셋 분할
  ├── gcp/                      # GCP 다운로드
  └── docker/
      ├── app/              # Dagster 정의
      └── data/             # DuckDB 파일
  ```

- **추가 작업 (오후 세션, 경로/실행 정렬)**:
  - `docker/docker-compose.yaml` 바인드 마운트 fallback 정렬:
    - `PROJECTS_HOST_PATH` fallback을 `/home/pia/mou/nas_192tb/datasets/projects`로 통일
    - `INCOMING_HOST_PATH` fallback은 `/mnt/nas/incoming` 유지 (fallback 전용)
  - `docker/.env` 최종값 확인:
    - `PROJECTS_HOST_PATH=/home/pia/mou/nas_192tb/datasets/projects`
    - `ARCHIVE_HOST_PATH=/home/pia/mou/archive`
    - `INCOMING_HOST_PATH=/home/pia/mou/incoming`
  - 서비스 재기동:
    - `docker compose -f docker/docker-compose.yaml up -d app dagster`
    - 상태 확인: `pipeline-app-1`, `pipeline-dagster-1` 모두 `Up`
  - 스캐너 실행 이슈 가이드 정리:
    - 에러: `open .../projects/docker/docker-compose.yaml: no such file or directory`
    - 원인: repo 루트가 아닌 경로에서 상대 `-f docker/docker-compose.yaml` 사용
    - 해결: repo 루트에서 실행하거나 `-f`/`--project-directory` 절대경로 사용
      (운영 명령 재사용 시 절대경로 권장)

## 2026-02-11
- **Local/Docker 실행 동등성(Parity) 정리**:
  - 로컬/도커가 같은 DuckDB 파일을 사용하도록 기본값 통일.
  - `configs/global.yaml`:
    - `nas_root`: `/home/pia/mou/nas_192tb/datasets/projects`
    - `duckdb_path`: `./docker/data/pipeline.duckdb`
  - `docker/.env`에 `DATAOPS_DUCKDB_PATH=/data/pipeline.duckdb` 명시.
  - `docker-compose.yaml`의 app/dagster에서 `DUCKDB_PATH`, `DATAOPS_DUCKDB_PATH`를 공통 참조로 정렬.

- **컨테이너 DB 파일 연결 확정**:
  - 컨테이너 기준 `/data/pipeline.duckdb` 사용.
  - 호스트 기준 `./docker/data/pipeline.duckdb` 바인드 마운트 확인.
  - host/app/dagster에서 동일 inode 확인하여 같은 파일임을 검증.
  - 참고: `./data/pipeline.duckdb`는 동명이인 별도 파일(기본 운영 경로 아님).

- **Scanner 실행 경로 통일**:
  - `scripts/run_scanner.sh`를 legacy `scanner.run_scanner`에서
    `update_python_scanner.run_scanner`로 변경.
  - 로컬 기본 실행 시에도 `docker/data/pipeline.duckdb`를 사용하도록 환경변수 기본값 주입.

- **경로 정규화(canonical source_path) 적용**:
  - `update_python_scanner/models.py`에 `normalize_source_path()` 추가.
  - `raw_files.source_path` 저장 시 prefix 매핑 적용:
    - `DATAOPS_ASSET_PATH_PREFIX_FROM`
    - `DATAOPS_ASSET_PATH_PREFIX_TO`
  - `asset_id` 생성 기준도 canonical `source_path`로 통일.
  - 목적: 로컬 경로(`/home/...`, `/mnt/...`)와 도커 경로(`/nas/...`) 차이 제거.

- **업로더 실행 환경 경로 보정 추가**:
  - `update_minio_uploader/asset_catalog_uploader.py`에
    `resolve_local_path()` 및 CLI 옵션(`--path-prefix-from`, `--path-prefix-to`) 추가.
  - DB `source_path`가 현재 실행 환경에서 직접 접근 불가해도 prefix 치환으로 업로드 가능.

- **Dagster 기본 경로 fallback 강화**:
  - `docker/app/dagster_defs.py`에서 기본 NAS/DB 경로 해석 순서 정리:
    1) 환경변수
    2) 컨테이너 기본 경로(`/nas/datasets/projects`, `/data/pipeline.duckdb`)
    3) `configs/global.yaml`

- **검증 결과**:
  - 로컬 스캐너 dry-run 로그에서 설정 경로 반영 확인.
  - 도커 스캐너 dry-run 로그에서 `/data/pipeline.duckdb` 사용 확인.
  - 업로더 dry-run 정상 동작 확인.
  - 동시 write 시 DuckDB lock 충돌 가능성 확인(운영 시 순차 실행 필요).

- **MotherDuck 조직 공유 자동화 추가**:
  - 대상: `update_minio_uploader/local_duckdb_to_motherduck_sync.py`
  - 목적: MotherDuck DB 생성/동기화 시
    `Who has access = Anyone in my organization` 정책을 코드로 자동 보장.
  - 구현:
    - `CREATE SHARE IF NOT EXISTS ... FROM <db>
      (ACCESS ORGANIZATION, VISIBILITY DISCOVERABLE, UPDATE MANUAL|AUTOMATIC)` 실행.
    - 기본값: 공유 활성화(`--ensure-org-share=true`), `share_name=DB명`, `share_update=MANUAL`.
    - 옵션: `--no-ensure-org-share`, `--share-name`, `--share-update`.
    - 기존 옵션명 호환: `--share-on-create` alias 유지.
  - 검증:
    - `py_compile` 통과.
    - `--help` 출력에서 신규 옵션 노출 확인.

## 2026-02-10
- **MVP 6테이블 스키마 전환 완료**:
  - 기존 3테이블(`asset_catalog`, `scan_runs`, `dir_cache`) → MVP 6테이블(`raw_files`, `labels`, `processed_clips`, `clip_metadata`, `datasets`, `dataset_clips`)로 전환.
  - `scan_runs`/`dir_cache`는 Dagster가 대체하므로 삭제.
  - `schema.sql` 및 `src/python/common/schema.sql` 동시 갱신.

- **update_python_scanner 전면 리팩토링**:
  - `AssetRecord` → `RawFileRecord` 모델 변환 (`media_type`, `archive_path`, `ingest_batch_id`, `transfer_tool` 추가).
  - `db_handler.py`: 모든 SQL을 `raw_files` 기반으로 변경, `scan_runs`/`dir_cache` 관련 메서드 제거.
  - `scanner.py`: `source_mtime` 기반 변경 감지 → `file_size` + `checksum` 기반으로 전환.
  - `run_scanner.py`: `scan_runs` 기록 제거, `--batch-id` 파라미터 추가 (Dagster `run_id` 연동).
  - `ingest_status` 값 매핑: `scanned` → `pending`, `uploaded` → `completed`.

- **update_minio_uploader 전면 수정**:
  - `asset_catalog_uploader.py`, `motherduck_asset_catalog_to_minio.py`, `clear_minio_buckets.py` 3개 파일 모두 `asset_catalog` → `raw_files`, `minio_bucket`/`minio_key` → `raw_bucket`/`raw_key`로 변경.
  - `is_deleted` 필드 관련 조건 제거.

- **file_scan 모듈화 → `update_python_scanner/file_organizer/`**:
  - `file_scan/file_scan.py` → `file_organizer/file_scanner.py`로 이동.
  - `file_scan/filename_check.py` → `file_organizer/filename_normalizer.py`로 이동.
  - 스냅샷(JSON) 기반 신규 파일 감지 → DuckDB `raw_files` 비교 방식으로 전환 (단일 소스).
  - 파일명 정규화: 한글/유니코드 보존, 위험 문자(`< > : " / \ | ? *`)만 제거.
  - 파일 이동/이름변경 시 DB `source_path`도 즉시 업데이트 (중복 방지).
  - `is_already_organized_tree()` 조건 완화 (`i+4` → `i+3`).

- **Dagster 파이프라인 구성**:
  - 3단계 MVP 파이프라인 Asset 추가: `file_organize` → `nas_scan` → `minio_upload`.
  - `data_pipeline_job` + `data_pipeline_schedule` (월~금 18시) 정의.
  - `nas_folder_tree_job`에 `selection` 명시 (postgres 4개 asset만 포함하도록 수정).
  - `nas / vietnam_data` SourceAsset 제거 (postgres 폴더 트리와 중복).

- **파일명 정규화 로직 수정**:
  - 한글 파일명 보존으로 변경 (기존: 한글→로마자→slugify로 "현장카메라_1.mp4"→"1.mp4" 문제).
  - 위험 문자(`< > : " / \ | ? *`)만 제거, 공백→`_` 변환, 확장자 소문자 통일.
  - `romanize_korean()`, `slugify_base_name()` 제거 → `sanitize_filename()` 으로 대체.

- **파일 이동 시 DB 중복 방지**:
  - `db_handler.py`에 `update_source_path()` 메서드 추가.
  - `file_scanner.py`에서 파일 이름변경/이동 시 DB `source_path`도 즉시 업데이트.
  - 다음 스캔에서 이전 경로와 새 경로가 동시에 존재하는 중복 삽입 방지.
  - `is_already_organized_tree()` 조건 완화 (`i+4` → `i+3`, 카테고리 폴더 없이도 인정).

- **`file_scan/` 폴더 폐기**:
  - `file_scan/file_scan.py`, `file_scan/filename_check.py`는 `update_python_scanner/file_organizer/`로 완전 대체됨.
  - 기존 `file_scan/` 코드는 더 이상 사용하지 않음.

- **Docker 환경 설정**:
  - `docker-compose.yaml`에 `update_python_scanner`, `split_dataset` 볼륨 마운트 추가 (app, dagster 서비스).
  - `PYTHONPATH=/:/src/python` 환경변수 추가.
  - 테스트용 `/home/pia/test_data:/test_data` 볼륨 마운트 추가.

- **`split_dataset` 호환성 확인**:
  - `split_dataset.py`는 DuckDB 미사용 (MinIO만 사용) → 코드 변경 불필요.
  - Docker 컨테이너에서 실행 가능하도록 볼륨 마운트만 추가.

- **GCP 다운로드 스크립트 파이프라인 연동**:
  - `gcp/` 폴더 Docker 볼륨 마운트 추가 (app, dagster 서비스).
  - `download_from_gcs.sh` 다운로드 기본 경로를 NAS 경로로 변경 (`/nas/datasets/projects/khon_kaen_data`).
  - 파이프라인 접목 흐름: GCS 다운로드 → NAS 저장 → `file_organize` → `nas_scan` → `minio_upload`.
  - Python 코드(`download_from_gcs_rclone.py`)는 변경 없이 현재 환경 호환.

## 2026-02-04
- `pipeline-app-1` 컨테이너에서 `nas_folder_tree_to_postgres.py` 실행 및 검증 완료.
- **Docker 데이터 적재 이슈 해결**:
  - 증상: 스크립트 실행 시 데이터가 `pipeline-postgres-1`이 아닌 `kjh-dataops-postgres` 컨테이너로 잘못 적재됨.
  - 원인: 두 Docker 스택(`pipeline`, `kjh-dataops`)이 동일한 네트워크(`pipeline-network`)를 공유하고 있으며, 둘 다 서비스명이 `postgres`로 설정되어 있어 호스트 이름 해석이 모호했음 (Round-robin 또는 랜덤 접속 발생).
  - 해결: `docker-compose.yaml`에 정의된 네트워크 별칭(`pipeline-postgres`)을 사용하도록 `nas_folder_tree_to_postgres.py`의 기본 호스트 설정 변경.
- **Grafana 설정 가이드**:
  - Docker 내부망에서 Grafana가 올바른 DB를 바라보도록 Host URL을 `pipeline-postgres:5432`로 명시.

## 2026-02-03
- Split NAS folder tree data into a dedicated PostgreSQL DB (`nas_tree`) while keeping Airflow metadata in `airflow`.
- Added auto-create for `nas_tree` in the NAS folder tree scanner and event watcher.
- Updated the Airflow DAG to pass explicit Postgres host/port and `nas_tree` DB.
- Updated Grafana datasource to point at `pipeline-postgres:5432` and `nas_tree`.
- Resolved Postgres hostname collision by introducing a `pipeline-postgres` alias and updating Airflow SQLAlchemy connection.
- Reset and reinitialized Airflow/Postgres services to clear metadata issues and reserialized DAGs.

## 2026-01-29
- Added Dagster/DVC dependencies and rebuilt Docker image.
- Added Dagster service (port 3030) and Grafana service (port 3000).
- Implemented event-driven NAS watcher with snapshot save/restore and delta dir delete.
- Updated Dagster asset definitions to asset-based lineage and fixed API incompatibilities.
- Adjusted Airflow/Dagster schedules to run at 18:00 KST.
- Added conditional event watcher flags in Dagster op based on snapshot existence.
- Updated Airflow DAG to start the event watcher with conditional flags.
