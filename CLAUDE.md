# CLAUDE.md — VLM Data Pipeline

> 코드를 읽으면 아는 것은 생략. 코드만으로는 알 수 없는 규칙·환경·운영 맥락만 기록.

---

## 프로젝트 한 줄 요약

CCTV/보안 영상을 수집 → 중복제거 → Gemini 라벨링 → YOLO 검출 → 학습 데이터셋 빌드하는 **Dagster 기반 미디어 데이터 파이프라인**.

---

## 🤖 AI Agent Core Action Rules

- **Skill Discovery First:** 사용자가 작업을 지시하면, 스스로 코드를 처음부터 짜기 전에 반드시 시스템 도구를 거쳐 `.agent/skill/` 디렉토리를 먼저 검색하세요.
- 요청과 관련된 스킬 문서가 발견되면, 해당 문서(`SKILL.md`)의 지침을 완벽하게 읽고 그 룰에 맞추어 작업을 수행하세요.

---

## 빌드 & 실행

```bash
# 의존성 설치 (editable)
pip install -e ".[dev]"

# 로컬 테스트
pytest tests/unit -q
pytest tests/integration -q

# Docker (production — main 브랜치)
cd /home/pia/work_p/Datapipeline-Data-data_pipeline/docker && docker compose up -d

# Docker (staging — dev 브랜치)
cd /home/pia/work_p/Datapipeline-Data-data_pipeline_test/docker && docker compose up -d

# Dagster UI
#   production : http://172.168.42.6:3030  (main)
#   staging    : http://172.168.42.6:3031  (dev)

# DuckDB 쿼리 (호스트에서 직접)
python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files;"
```

---

## 환경 이중 구조 (Production vs Staging)

두 환경은 **독립 git clone + 독립 docker compose 스택**으로 완전 분리됩니다.

| 항목 | Production | Staging |
|------|-----------|---------|
| Dagster UI | `http://172.168.42.6:3030` | `http://172.168.42.6:3031` |
| Git repo (호스트) | `/home/pia/work_p/Datapipeline-Data-data_pipeline` | `/home/pia/work_p/Datapipeline-Data-data_pipeline_test` |
| Git branch | **`main`** (안정) | **`dev`** (검증) |
| Compose project | `docker` | `pipeline-test` |
| 컨테이너 이름 prefix | `docker-dagster-*` | `pipeline-test-dagster-*` |
| DuckDB (컨테이너) | `/data/pipeline.duckdb` | `/data/staging.duckdb` |
| MinIO endpoint | `http://172.168.47.36:9000` | `http://172.168.47.36:9002` |
| MinIO Console | `:9001` | `:9003` |
| Incoming (호스트) | `/home/pia/mou/incoming` | `/home/pia/mou/staging/incoming` |
| Archive (호스트) | `/home/pia/mou/archive` | `/home/pia/mou/staging/archive` |
| DAGSTER_HOME (컨테이너) | `/app/dagster_home` | `/app/dagster_home` (동일, 호스트 경로만 다름) |
| env file | `docker/.env` | `docker/.env.test` |
| pia-agent 연동 | `host.docker.internal:8080` | `host.docker.internal:8081` |

두 repo는 각자 독립 `.git`을 보유하며, 브랜치 기준 배포는 CI/CD가 자동 수행합니다 (다음 섹션).

---

## 브랜치 전략 & 배포 (CI/CD)

### 브랜치 역할

- **`dev`** — 스테이징(3031)이 추적. 신기능·실험·리팩터링 진입점.
- **`main`** — 프로덕션(3030)이 추적. `dev`에서 충분히 검증된 뒤에만 머지.

### 자동 배포 (GitHub Actions, self-hosted runner)

| Workflow | 트리거 | 배포 대상 | Runner 라벨 |
|----------|-------|-----------|------------|
| [`deploy-test.yml`](.github/workflows/deploy-test.yml) | `push` → `dev` | 스테이징 repo | `self-hosted, linux, test` |
| [`deploy-production.yml`](.github/workflows/deploy-production.yml) | `push` → `main` | 프로덕션 repo | `self-hosted, linux, production` |

두 워크플로 모두 [`scripts/deploy/deploy-stack.sh`](scripts/deploy/deploy-stack.sh)로 실행. 주요 단계:

1. **test 잡** — `pytest tests/unit` (workflow_dispatch의 `skip_tests=true`로 우회 가능, 긴급시 전용)
2. **detect_image_rebuild 잡** — `Dockerfile` / `docker/app/` / `configs/` / `scripts/` / `gcp/` / `split_dataset/` / `src/python/` 변경 시에만 이미지 재빌드
3. **deploy 잡** — `rsync -a --delete`로 워크스페이스 → DEPLOY_ROOT 동기화 (`src/`, `configs/`, `gcp/`, `scripts/`, `split_dataset/` + `docker/app/` 일부 + compose/Dockerfile). `dagster_home/`, `dagster_home_staging/`, `credentials/`, `docker/data/`는 rsync 제외 — 런타임 상태 보존
4. env 파일 복원 (`.env` / `.env.test` — 호스트 저장분 유지)
5. `docker compose up -d` 스택 재기동 + HEALTHCHECK_URL 응답 검증 (prod `:3030/server_info`, staging `:3031/server_info`)
6. AI deploy 분석 (Claude CLI, best-effort, 실패해도 배포는 성공)

> ⚠️ **fork 구분**: 두 워크플로 모두 `if: github.repository == 'Orderlee/Datapipeline-Data-data_pipeline'` 조건 있음 — `upstream`(TeamPIA)으로 PR이 가면 CI 트리거되지 않음.

### 권장 배포 플로우

1. `feature/*` 브랜치를 `dev`에서 분기
2. PR → `dev` 머지 → **자동 스테이징 배포** (3-10분)
3. 스테이징(3031)에서 end-to-end 검증 (센서 tick, dispatch run, MinIO 결과물)
4. `dev` → `main` PR → 머지 → **자동 프로덕션 배포**

### 핫픽스 (프로덕션 긴급 수정)

1. `fix/*` 브랜치를 `main`에서 분기
2. PR → `main` 머지 → 프로덕션 즉시 배포
3. 완료 후 `main` → `dev` 백머지하여 drift 방지

### 수동 배포 / CI 우회

- GitHub Actions UI → 해당 워크플로 `Run workflow` 버튼 (`skip_tests` 옵션 사용 가능)
- CI 불가 시 호스트에서 직접:

```bash
# PROD
cd /home/pia/work_p/Datapipeline-Data-data_pipeline
git pull origin main --ff-only
cd docker && docker compose restart dagster dagster-daemon dagster-code-server

# STAGING
cd /home/pia/work_p/Datapipeline-Data-data_pipeline_test
git pull origin dev --ff-only
cd docker && docker compose restart
```

### Drift 감지

```bash
# 두 repo src/ 바이트 비교 (dev ≠ main 시점에는 차이 존재 = 정상)
diff -rq /home/pia/work_p/Datapipeline-Data-data_pipeline/src \
         /home/pia/work_p/Datapipeline-Data-data_pipeline_test/src

# 각 repo가 해당 브랜치 HEAD와 일치하는지
git -C /home/pia/work_p/Datapipeline-Data-data_pipeline status            # main clean?
git -C /home/pia/work_p/Datapipeline-Data-data_pipeline_test status       # dev clean?
```

### 금기사항

- 호스트에서 `src/`·`configs/`·`scripts/`·compose 파일 **수동 수정 금지** — 다음 CI 배포의 `rsync --delete`로 소실됨. 반드시 git commit → push 경로로 반영
- `main`에 force-push 금지 (CI 미트리거 + 히스토리 손상)
- `.env` / `.env.test`는 git 미추적. 변경 시 호스트에서 직접 편집 후 해당 환경 Dagster 재시작 필요
- 스테이징에서 디버깅용 수정 → `dev`에 commit하지 않으면 다음 배포로 사라짐

---

## 코딩 규칙

- **Python 3.10+**, formatter/linter: `ruff` (line-length 120)
- **Dagster**: `@asset` 우선, `@op+@job` 필요 시만
- **Import 계층** — 코드에 5-layer 주석 있음. 하위→상위 import 금지
  - L1-2: `lib/` (순수 Python, key_builders 포함) → L3: `ops` → L4: `assets/sensors` → L5: `definitions.py`
  - `lib/spec_config.py`는 순수 태그 파싱만. DB 의존 함수는 `defs/spec/config_resolver.py`에 위치
  - MinIO 키 빌더는 `lib/key_builders.py`에 통합. 각 `defs/` 모듈은 thin wrapper로 위임
- **모듈 분할 규칙** — 대형 파일은 도메인별 서브모듈로 분할
  - `defs/process/`: `assets.py`(라우팅) + `helpers.py` + `captioning.py` + `frame_extract.py` + `raw_frames.py`
  - `defs/label/`: `assets.py`(라우팅) + `label_helpers.py` + `timestamp.py` + `artifact_*.py`
  - `resources/`: `duckdb_base.py` + `duckdb_phash.py` + `duckdb_migration.py` + `duckdb_ingest_*.py`
- **커밋**: conventional commits (`feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`)
  - "어떻게 수정했다"보다 **"무엇과 왜 수정했는지"** (`.gitmessage.txt` 참고)
- **에러 처리**: per-file fail-forward — 한 파일 실패해도 나머지 계속 처리
- **테스트**: pytest, in-memory DuckDB fixture, mocked MinIO (`moto[s3]`), `tests/conftest.py` 공통 fixture

---

## 핵심 운영 규칙 (코드에 안 드러나는 것)

### DuckDB 동시성
- DuckDB = 단일 파일 write lock ⇒ `tags={"duckdb_writer": "true"}` 필수
- `run_coordinator`에서 `duckdb_writer` tag concurrency=1로 제한

### NFS/NAS 장애 대응
- sensor에서 `OSError/PermissionError/TimeoutError` → graceful skip, 다음 tick 재시도
- NAS 지연 시 권장 설정:
  - `AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES=20`
  - `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK=3`
  - `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS=300`

### 파일 오류 정책
- `file_missing`, `empty_file`, `ffprobe_failed` → **DB 미삽입 + archive 미이동**
- 추적은 JSONL 실패 로그(`<manifest_dir>/failed/*.jsonl`)에만 기록
- transient 오류(DuckDB lock) → retry manifest 자동 생성, failed row 아님

### Archive 이동
- `source_unit_type=directory`이고 모든 파일 성공 → 폴더째 archive 이동
- chunked manifest → 파일 단위 누적 이동 (조기 폴더 이동 방지)
- archive 폴더명 충돌 → `__2`, `__3` suffix 자동 분기
- archive 이동 완료된 파일**만** `ingest_status=completed` 유지

### MinIO 버킷/경로 정책
- `vlm-raw` · `vlm-labels` · `vlm-processed` · `vlm-dataset` · `vlm-classification` (5개 고정)
- `raw_key = <source_unit_name>/<rel_path>` — `YYYY/MM` prefix 금지
- 이벤트 JSON source of truth = `vlm-labels`만. `vlm-processed`에 중복 저장 금지
- classification 결과: `vlm-classification/<folder_prefix>/{video|image}/<class>/<file>` 형태의 **원본 복사** (JSON/DB 미적재)

### Staging 초기화 (깨끗한 재테스트)
1. 스테이징 컨테이너 중지:
   `docker stop pipeline-test-dagster-1 pipeline-test-dagster-daemon-1 pipeline-test-dagster-code-server-1`
2. staging MinIO 5개 버킷(`vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`, `vlm-classification`) 객체 전체 삭제 — `:9003` 콘솔 또는 `mc rm --recursive --force local/<bucket>`
3. `Datapipeline-Data-data_pipeline_test/docker/data/staging.duckdb` 삭제 (root 소유라 `docker run --rm -v ... alpine rm -f` 권장)
4. `Datapipeline-Data-data_pipeline_test/docker/app/dagster_home/storage/` 내용 삭제 (run·sensor·schedule 상태 초기화)
5. 재기동: `cd .../data_pipeline_test/docker && docker compose up -d`
- ⚠️ staging incoming/archive 원본 폴더(`/home/pia/mou/staging/incoming`, `/home/pia/mou/staging/archive`)는 명시 요청 없으면 **절대 삭제 금지**

---

## 서비스 네트워크 & 볼륨 (코드에서 놓치기 쉬운 것)

- Docker network: `pipeline-network`
- **호스트 ↔ 컨테이너 경로 매핑** (compose의 bind mount):
  - `/home/pia/mou/incoming` → `/nas/incoming`
  - `/home/pia/mou/archive` → `/nas/archive`
  - `/home/pia/mou/staging` → `/nas/staging` (staging only)
  - 코드→실행 경로: `../src` → `/src/vlm` (read-only)
- DuckDB **호스트 실제 경로**: `./docker/data/pipeline.duckdb`
- YOLO 서버: GPU 1번 전용 (`cuda:1`, `NVIDIA_VISIBLE_DEVICES=1`)
- Places365 모델 캐시: `/data/models/places365` (auto_download=false, 고정 캐시만 사용)
- `PYTHONPATH` (컨테이너): `/:/src/python:/src/vlm`

---

## 자주 쓰는 스크립트

| 스크립트 | 용도 |
|---------|------|
| `scripts/query_local_duckdb.py` | 로컬 DuckDB 읽기 쿼리 (lock 회피 fallback 포함) |
| `scripts/backfill_video_metadata.py` | video_metadata 결손 백필 |
| `scripts/cleanup_duplicate_assets.py` | checksum duplicate 정리 |
| `scripts/recompute_archive_checksums.py` | archive 원본 재해시 |
| `scripts/reupload_minio_from_archive.py` | archive 기준 MinIO 재업로드 |
| `scripts/staging_test_dispatch.py` | staging dispatch 테스트 |
| `scripts/verify_mvp.sh` | E2E 검증 |

---

## Label Studio 연동

- compose: `docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml up -d`
- LS UI: `http://<HOST>:8084` (기본 8080이나 piaspace-agent 충돌로 `LS_PORT=8084` 사용)
- 필수 env: `LS_API_KEY` (LS 계정 설정에서 발급), `WEBHOOK_HOST` (LS→webhook 접근 IP)
- sensor `ls_task_create_sensor`: Dagster UI에서 수동 ON 필요 (default=STOPPED)
- webhook 등록 (프로젝트별): `python src/gemini/ls_webhook.py register --project <id>`
- presigned URL 만료(기본 7일) 시: `python src/gemini/ls_tasks.py renew --project-name <name>`
- Slack 알림/slash command는 `SLACK_WEBHOOK_URL`, `SLACK_SIGNING_SECRET` 설정 시 활성화

---

## GCS 외부 수집

- 버킷: `khon-kaen-rtsp-bucket` (주), `adlibhotel-event-bucket`, `kkpolice-event-bucket`
- 스크립트: `gcp/download_from_gcs_rclone.py`
- Dagster schedule: `gcs_download_schedule` (매일 04:00 KST)
- 0바이트 파일 복구: `GCS_ZERO_BYTE_RETRIES` (기본 2)

---

## DuckDB 파일 교체 시 주의

1. **반드시 서비스 중지** 후 교체
2. 기존 `.wal` 파일 존재 여부 확인 → 있으면 백업 후 삭제
3. stale WAL이 새 DB에 재적용되면 corruption 발생

---

## Gemini / Vertex AI

- 프로젝트: `gmail-361002`, 리전: `us-central1`
- 기본 모델: `gemini-2.5-flash`
- credential 우선순위: `GEMINI_GOOGLE_APPLICATION_CREDENTIALS` → `GOOGLE_APPLICATION_CREDENTIALS` → `GEMINI_SERVICE_ACCOUNT_JSON`
- 450MB 초과 영상 → preview mp4 자동 생성 (Vertex 524MB 제한 회피)

---

## YOLO-World

- 모델: `yolov8l-worldv2.pt` (`/data/models/yolo/`)
- dependency 함정: `clip` 패키지 없으면 컨테이너 부팅 실패 → `git+https://github.com/ultralytics/CLIP.git` 필요
- health check: `GET /health` → `model_loaded=true`
