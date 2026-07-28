# REVIEW.md — VLM Data Pipeline Review Standards

> Claude PR 자동 리뷰 및 수동 리뷰 기준 문서
> 기준 스택: **Dagster + PostgreSQL + MinIO** (2026-05-19 DuckDB → PG cutover 이후)

---

## Critical (반드시 수정 요청)

- **DB write 경로 위반**: asset/op 에서 `db: PostgresResource` 를 거치지 않고 `psycopg2` 로 직접 연결해 write.
  sensor 는 예외적으로 `lib/sensor_db.py`(`open_sensor_read_connection`)로 **read-only** 만 허용 — sensor 에서 write 금지
- **Import layer 위반**: `lib/`(L1-2) 안에서 `dagster` / `vlm_pipeline.defs` / `vlm_pipeline.resources` / `vlm_pipeline.ops` import.
  top-level 과 함수 내 lazy import 둘 다 금지, `TYPE_CHECKING` 가드 안에서만 허용.
  CI(`scripts/check_lib_layer_imports.py`) + pre-commit `lib-layer-imports` 훅이 차단
- 하드코딩된 credential / API 키 / 비밀번호
- MinIO 버킷 이름이 `vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`, `vlm-classification` **5개 고정** 규칙 위반
- 라벨 JSON을 `vlm-processed`에 중복 저장 (source of truth는 `vlm-labels`만)
- production/staging 환경 분리 위반 (경로, 포트, DSN/DB 이름 혼재)
- 모델 파생 라벨(`auto_generated`, Gemini 캡션, `vlm-classification`)을 학습/eval GT 로 사용 (자기학습 금지)

## Warning (강한 권고)

- 새 asset/op에 대한 unit test 미작성
- `@op+@job` 사용 시 `@asset`으로 대체 가능한지 미검토
- per-file fail-forward 패턴 미준수 (한 파일 실패가 전체를 중단)
- sensor에서 `OSError` / `PermissionError` / `TimeoutError`를 catch하지 않음 (NAS 장애 시 크래시)
- ruff line-length 120 초과 — CI `lint.yml`은 **ruff 0.7.4 고정**이며 `ruff check` + `ruff format --check` 둘 다 게이트
- `raw_key`에 `YYYY/MM` prefix 포함 (금지 규칙 — `raw_key = <source_unit_name>/<rel_path>`)
- archive 이동 로직에서 suffix(`__2`, `__3`) 충돌 처리 누락
- 새 migration 파일에 `DO $$ ... $$` 블록이 2개 이상 (runner 가 일부만 적용하는 알려진 한계 — 파일당 1개로 분리)

## Info (참고 의견)

- 함수/모듈이 너무 커서 분할 권장 (기존 패턴: `assets.py`(라우팅) + `helpers.py` 분리)
- 독스트링 누락 (새로 작성한 public 함수)
- 매직 넘버 대신 상수/config 사용 권장
- MinIO 키를 직접 조립하지 말고 `lib/key_builders.py` 빌더 사용
- pyright basic 모드 신규 에러 추가 여부 (`lint.yml` 의 pyright 잡 — `continue-on-error` 지만 baseline 은 유지)

## Review Scope Exclusions

- `docs/**`, `*.md` 변경만 있는 PR은 리뷰 대상 아님
- `.cursor/**`, `.agent/**` 변경은 무시
- `tests/**`만 변경된 PR은 테스트 품질만 확인

> 위 경로들은 `deploy-production.yml` / `deploy-test.yml` 의 `paths-ignore` 와 동일해서
> **배포도 트리거되지 않습니다** (`docs/**`, `*.md`, `tests/**`, `.cursor/**`, `.agent/**`,
> `.github/copilot-instructions.md`, `.github/workflows/claude*.yml`).

## Deployment Impact Check

dev → main PR (릴리즈 PR)의 경우 추가 확인:

- **이미지 재빌드 트리거 경로**에 해당하는지 — `detect_image_rebuild` 잡 기준:
  `docker/Dockerfile`, `docker/app/`, `configs/`, `scripts/`, `gcp/`, `split_dataset/`,
  `src/python/`, `src/vlm_pipeline/`, `src/gemini/`,
  `docker/{sam3,pg-backup,genai,embedding,trainer,mlflow,curation}/`,
  `docker/docker-compose.yaml`, 배포 workflow 파일 자체
  (⚠️ `src/vlm_pipeline/` 도 재빌드를 트리거합니다 — rsync 만 되는 것이 아님)
- `docker-compose.yaml` 변경 시 볼륨/네트워크/profile 영향
- env 변수 추가/변경 시 `.env.example` 동기화 여부
- `scripts/deploy/` 변경 시 롤백 호환성 (`scripts/deploy/rollback.sh`)
- **운영 중단 영향**: 재빌드 여부와 무관하게, 배포 잡이 실행되면 `dagster` / `dagster-daemon` /
  `dagster-code-server` 3개 컨테이너가 항상 stop → rm → recreate 됩니다.
  진행 중인 라벨링·수집 run 이 끊기므로 배포 타이밍을 확인할 것
