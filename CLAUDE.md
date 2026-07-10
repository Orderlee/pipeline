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
./scripts/compose-prod.sh up -d
# Docker (staging — dev 브랜치, staging clone 에서 실행)
./scripts/compose-staging.sh up -d

# ⚠️ 주의: 수동으로 `docker compose ...` 직접 호출 금지. 두 wrapper 가 다음을 보장:
#   - prod: `--env-file .env` 명시 → INCOMING/ARCHIVE_HOST_PATH 가 nas_200tb 로 정상 resolve (없으면 legacy /home/user/mou/incoming 로 silently revert)
#   - staging: `-p pipeline-test --env-file .env.test` 명시 → 프로젝트 이름 + 포트(:3031)+경로(/staging/) 모두 정상 (없으면 PROD 컨테이너 건드림)
# 두 케이스 다 2026-05-19 QA 중 실제 발생. CI deploy-stack.sh 는 이미 --env-file 사용 중 — 수동 ops 만 wrapper 필수.

# Dagster UI
#   production : http://10.0.0.10:3030  (main)
#   staging    : http://10.0.0.10:3031  (dev)

# DuckDB 쿼리 (호스트에서 직접)
python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files;"
```

---

## 환경 이중 구조 (Production vs Staging)

두 환경은 **독립 git clone + 독립 docker compose 스택**으로 완전 분리됩니다.

| 항목 | Production | Staging |
|------|-----------|---------|
| Dagster UI | `http://10.0.0.10:3030` | `http://10.0.0.10:3031` |
| Git repo (호스트) | `/home/user/work_p/Datapipeline-Data-data_pipeline` | `/home/user/work_p/Datapipeline-Data-data_pipeline_test` |
| Git branch | **`main`** (안정) | **`dev`** (검증) |
| Compose project | `docker` | `pipeline-test` |
| 컨테이너 이름 prefix | `docker-dagster-*` | `pipeline-test-dagster-*` |
| DuckDB (컨테이너) | `/data/pipeline.duckdb` | `/data/staging.duckdb` |
| MinIO endpoint | `http://10.0.0.36:9000` | `http://10.0.0.36:9002` |
| MinIO Console | `:9001` | `:9003` |
| Incoming (호스트) | `/home/user/mou/nas_200tb/incoming` | `/home/user/mou/nas_200tb/staging/incoming` |
| Archive (호스트) | `/home/user/mou/nas_200tb/archive` | `/home/user/mou/nas_200tb/staging/archive` |
| DAGSTER_HOME (컨테이너) | `/app/dagster_home` | `/app/dagster_home` (동일, 호스트 경로만 다름) |
| env file | `docker/.env` | `docker/.env.test` |
| dispatch-agent 연동 | `host.docker.internal:8080` | `host.docker.internal:8081` |

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
3. **deploy 잡** — 호스트 코드를 deployed SHA로 정렬:
   - **(a) rsync** `-a --delete` 워크스페이스 → DEPLOY_ROOT 동기화 (`src/`, `configs/`, `gcp/`, `scripts/`, `split_dataset/` + `docker/app/` 일부 + compose/Dockerfile). `dagster_home/`, `dagster_home_staging/`, `credentials/`, `docker/data/`는 rsync 제외 — 런타임 상태 보존
   - **(b) git hard-reset** `git -C ${DEPLOY_REPO_ROOT} fetch origin && reset --hard ${GITHUB_SHA}` — 호스트 git tree(`.git/HEAD`, `git log`, `git status`)를 deployed commit과 정확히 일치시킴. **rsync로 src 파일은 갱신되지만 `.git`은 안 건드리므로** 이 step이 없으면 호스트의 `git log`가 영원히 stale로 보임. tracked 파일만 reset되고 `dagster_home/` 등 untracked는 유지됨.
4. env 파일 복원 (`.env` / `.env.test` — 호스트 저장분 유지)
5. `docker compose up -d` 스택 재기동 + HEALTHCHECK_URL 응답 검증 (prod `:3030/server_info`, staging `:3031/server_info`)
6. AI deploy 분석 (Claude CLI, best-effort, 실패해도 배포는 성공)

> ✅ **단일 진리 원칙**: deploy 후 `호스트 git HEAD == 컨테이너 이미지 안 src == 실행 코드`가 항상 일치한다. 호스트 src는 컨테이너에 mount되지 않으므로 (이미지 빌드 시 `COPY src/` 결과만 사용), 호스트 src를 수정해도 컨테이너 동작은 변하지 않는다 — 변경 즉시 반영이 필요하면 `docker compose build` 후 재기동.

> ⚠️ **fork 구분**: 두 워크플로 모두 `if: github.repository == 'Orderlee/Datapipeline-Data-data_pipeline'` 조건 있음 — `upstream`(upstream-org)으로 PR이 가면 CI 트리거되지 않음.

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
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git pull origin main --ff-only
cd docker && docker compose restart dagster dagster-daemon dagster-code-server

# STAGING
cd /home/user/work_p/Datapipeline-Data-data_pipeline_test
git pull origin dev --ff-only
cd docker && docker compose restart
```

### Drift 감지

```bash
# 두 repo src/ 바이트 비교 (dev ≠ main 시점에는 차이 존재 = 정상)
diff -rq /home/user/work_p/Datapipeline-Data-data_pipeline/src \
         /home/user/work_p/Datapipeline-Data-data_pipeline_test/src

# 각 repo가 해당 브랜치 HEAD와 일치하는지
git -C /home/user/work_p/Datapipeline-Data-data_pipeline status            # main clean?
git -C /home/user/work_p/Datapipeline-Data-data_pipeline_test status       # dev clean?
```

### 금기사항

- 호스트에서 `src/`·`configs/`·`scripts/`·compose 파일 **수동 수정 금지** — 다음 CI 배포의 `rsync --delete` + `git reset --hard`로 소실됨. 반드시 git commit → push 경로로 반영
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
- **테스트**: pytest, Postgres fixture, mocked MinIO (`unittest.mock`), `tests/conftest.py` 공통 fixture

---

## 핵심 운영 규칙 (코드에 안 드러나는 것)

### DB write 동시성 (`duckdb_writer` 태그 — 이름만 legacy, 아직 유효)
- 메타데이터 SoT는 PostgreSQL (2026-05-19 cutover) — 단일 파일 write lock 제약 자체는 사라짐
- 단, `tags={"duckdb_writer": "true"}` + run_coordinator tag concurrency=1 게이트는 보수적 write 직렬화 안전마진으로 **현재도 유지** (`docker/app/dagster.yaml` `tag_concurrency_limits`, `definitions_common/jobs.py`의 10여 개 job에 태그 부착)
- 걷어내려면 동시 write 경합 검증 후 job 태그와 coordinator 설정을 **함께** 제거할 것 (한쪽만 지우면 무의미)

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

### `labels` 테이블 의미 (E2E 검증시 흔히 혼동)
- `labels` 는 **per-event** 레코드: `event_index`/`event_count`/`timestamp_start_sec`/`timestamp_end_sec`/`caption_text` 한 행 = Gemini 가 비디오 안에서 검출한 이벤트 1개. 한 비디오가 N events → N rows, **0 events → 0 rows**.
- 따라서 `SELECT COUNT(*) FROM labels WHERE asset_id IN (...) = 0` 은 **라벨링 실패 아님** — Gemini 가 해당 source 비디오들에서 카테고리 조건에 맞는 이벤트를 찾지 못한 정상 결과일 수 있음.
- **라벨링 stage 완료 지표**는 `labels` 행 수가 아니라 다음 셋:
  - `video_metadata.timestamp_status='completed'`
  - `video_metadata.timestamp_label_key` 세팅됨 (예: `<source>/events/<file>.json`)
  - MinIO `vlm-labels/<source>/events/*.json` 객체 존재 (이벤트 0개여도 빈 events array JSON 업로드됨)
- 동일 패턴: `bbox_status='completed'` + `image_labels` 행 존재로 bbox 단계 완료를 판단. `image_labels` 행이 0이면 bbox detect 가 검출 못한 상태 (정상 가능).
- 운영 디버깅시: Gemini 호출이 실제로 일어났는지 확인하려면 Dagster run 의 `clip_timestamp` step 실행 시간을 보자. 20 videos → 90~120s 이면 정상 (≈5s/video). 0s 면 skip 된 것.

### Staging 초기화 (깨끗한 재테스트)
1. 스테이징 컨테이너 중지:
   `docker stop pipeline-test-dagster-1 pipeline-test-dagster-daemon-1 pipeline-test-dagster-code-server-1`
2. staging MinIO 5개 버킷(`vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`, `vlm-classification`) 객체 전체 삭제 — `:9003` 콘솔 또는 `mc rm --recursive --force local/<bucket>`
3. `Datapipeline-Data-data_pipeline_test/docker/data/staging.duckdb` 삭제 (root 소유라 `docker run --rm -v ... alpine rm -f` 권장)
4. `Datapipeline-Data-data_pipeline_test/docker/app/dagster_home/storage/` 내용 삭제 (run·sensor·schedule 상태 초기화)
5. 재기동: `cd .../data_pipeline_test/docker && docker compose up -d`
- ⚠️ staging incoming/archive 원본 폴더(`/home/user/mou/nas_200tb/staging/incoming`, `/home/user/mou/nas_200tb/staging/archive`)는 명시 요청 없으면 **절대 삭제 금지**

---

## 서비스 네트워크 & 볼륨 (코드에서 놓치기 쉬운 것)

- Docker network: `pipeline-network`
- **호스트 ↔ 컨테이너 경로 매핑** (compose의 bind mount) — **현재는 NAS_200tb(10.0.0.51, NFS) 로 cutover된 상태** (이전 `/home/user/mou/incoming`·`archive` 직결 경로는 폐기):
  - **PROD**: `/home/user/mou/nas_200tb/incoming` → `/nas/incoming`, `/home/user/mou/nas_200tb/archive` → `/nas/archive`
  - **STAGING**: `/home/user/mou/nas_200tb/staging/incoming` → `/nas/incoming`, `/home/user/mou/nas_200tb/staging/archive` → `/nas/archive`
  - 컨테이너 환경변수 `INCOMING_HOST_PATH` / `ARCHIVE_HOST_PATH` 가 진실 — 의심되면 `docker exec <ctr> env | grep _HOST_PATH` 로 확인.
  - **운영자 주의**: `user` 유저는 NAS_200tb 상에서 quota 가 걸려있어 호스트에서 직접 `cp`/`mkdir` 시 "디스크 할당량 초과" 발생. 큰 파일을 incoming 에 넣을 땐 컨테이너(root) 경유 (`docker run --rm -v /home/user/mou/nas_200tb/...:/dst alpine cp ...`) 또는 quota 정리 필요.
  - 코드→실행 경로: **mount 없음**. 컨테이너는 이미지 빌드 시 Dockerfile `COPY src/ /src/vlm/`로 들어간 src만 사용 (`/src/vlm`, `/src/python`). 호스트 src 변경은 다음 `docker compose build` 전까지 컨테이너에 반영되지 않음.
- DuckDB **호스트 실제 경로**: `./docker/data/pipeline.duckdb`
- **GPU 할당 정책 (2026-05-22 업데이트)**:
  - **dagster 계열**: 호스트 GPU 0+1 둘 다 노출 (`CUDA_VISIBLE_DEVICES=0,1` + `NVIDIA_VISIBLE_DEVICES=0,1`).
    - Python torch (Places365) → default `cuda:0` = 호스트 GPU 0 (CUDA cores)
    - ffmpeg NVENC → `REENCODE_NVENC_GPU_INDICES` (default "0,1") round-robin → 양 GPU 의 NVENC unit 활용 (RTX A4000 NVENC unit GPU 당 1개)
  - **SAM3 (별도 컨테이너)**: 호스트 GPU 1 의 CUDA cores 만 사용 (`CUDA_VISIBLE_DEVICES=1`). 컨테이너 view 에서는 `cuda:0` 로 보이지만 호스트는 GPU 1.
    - workers=2 (`SAM3_WORKERS` env, compose default 2) — worker당 model ~3.7 GB. 2026-05-27 GPU1 OOM 인시던트로 4→2 하향 (`docker-compose.yaml` 주석 참조)
  - **YOLO (별도 컨테이너)**: 호스트 GPU 1 — 현재 `ENABLE_YOLO_DETECTION=false` 정책으로 비활성
  - **경합 분석**: dagster NVENC (GPU 0/1 의 NVENC unit) ↔ SAM3 (GPU 1 의 CUDA cores) — 별개 hardware unit 이라 같은 GPU 1 안에서도 동시 사용 OK
- Places365 모델 캐시: `/data/models/places365` (auto_download=false, 고정 캐시만 사용)
- `PYTHONPATH` (컨테이너): `/:/src/python:/src/vlm`

---

## 자주 쓰는 스크립트

| 스크립트 | 용도 | 상태 |
|---------|------|------|
| `scripts/query_local_duckdb.py` | 로컬 DuckDB 읽기 쿼리 (lock 회피 fallback 포함) | DuckDB legacy ⚠️ |
| `scripts/backfill_video_metadata.py` | video_metadata 결손 백필 | DuckDB legacy ⚠️ (guard 적용, `ALLOW_LEGACY_DUCKDB_SCRIPT=1` 필요) |
| `scripts/cleanup_duplicate_assets.py` | checksum duplicate 정리 | DuckDB legacy ⚠️ (guard 적용, `ALLOW_LEGACY_DUCKDB_SCRIPT=1` 필요) |
| `scripts/recompute_archive_checksums.py` | archive 원본 재해시 | DuckDB legacy ⚠️ (guard 적용, `ALLOW_LEGACY_DUCKDB_SCRIPT=1` 필요) |
| `scripts/reupload_minio_from_archive.py` | archive 기준 MinIO 재업로드 | DuckDB legacy ⚠️ (guard 적용, `ALLOW_LEGACY_DUCKDB_SCRIPT=1` 필요) |
| `scripts/staging_test_dispatch.py` | staging dispatch 테스트 | DuckDB legacy ⚠️ (guard 적용, `ALLOW_LEGACY_DUCKDB_SCRIPT=1` 필요) |
| `scripts/verify_mvp.sh` | E2E 검증 | 사용 가능 |
| `scripts/promote_model.py` | MinIO 체크포인트 → 호스트 materialize + env + recreate (승격/롤백) | MLOps (만들되 기본 미실행; `--dry-run` CI-safe) |
| `scripts/promote_pe_core.py` | PE-Core 포인터 전환 + partial-HNSW + 서빙 교체 (승격/롤백) | MLOps (만들되 기본 미실행; `--dry-run`) |
| `scripts/dataset_pull.py` | dataset_catalog pin 해석 → `dvc get` (DVC 버전 데이터셋 pull) | MLOps (기본 dry-run) |
| `scripts/clear_maintenance.sh` | GPU 정비락 수동 강제 해제 + `/maintenance/exit` + `/warmup` | MLOps 복구 (`.agent/skill/mlops-finetune/SKILL.md` §9) |

### Deprecated (scripts/archive/ 로 이동됨)

다음 일회성 스크립트는 사용 완료로 `scripts/archive/` 로 이동됨 (OPS-STALE-DUCKDB-SCRIPTS Stage 1):

- `scripts/archive/migrate_yolo_detection_json_to_coco.py` — YOLO JSON → COCO 마이그레이션 (완료)
- `scripts/archive/migrate_gcp_raw_keys.py` — GCP raw_key prefix 마이그레이션 (완료)
- `scripts/archive/fix_failed_status.py` — failed → completed 픽스 (완료)
- `scripts/archive/fix_uploading_status.py` — uploading → completed 픽스 (완료)
- `scripts/archive/recover_uploading.py` — uploading 복구 (완료)

---

## Label Studio 연동

- compose: `docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml up -d`
- LS UI: `http://<HOST>:8084` (기본 8080이나 dispatch-agent 충돌로 `LS_PORT=8084` 사용)
- 필수 env: `LS_API_KEY` (LS 계정 설정에서 발급), `WEBHOOK_HOST` (LS→webhook 접근 IP)
- sensor `ls_task_create_sensor`: Dagster UI에서 수동 ON 필요 (default=STOPPED)
- webhook 등록 (프로젝트별): `python src/gemini/ls_webhook.py register --project <id>`
- presigned URL 만료(기본 7일) 시: `python src/gemini/ls_tasks.py renew --project-name <name>`
- Slack 알림/slash command는 `SLACK_WEBHOOK_URL`, `SLACK_SIGNING_SECRET` 설정 시 활성화

---

## GCS 외부 수집

- 버킷: `source-a-rtsp-bucket` (주), `source-b-event-bucket`, `source-c-event-bucket`
- 스크립트: `gcp/download_from_gcs_rclone.py`
- Dagster schedule: `gcs_download_schedule` (매일 04:00 KST)
- 0바이트 파일 복구: `GCS_ZERO_BYTE_RETRIES` (기본 2)

---

## MLOps — 파인튜닝 트랙

> SAM3 / PE-Core 를 도메인 데이터로 파인튜닝하는 골격. **인프라는 CI(dev→staging→main), 가중치 승격만 수동.**
> 설계 source of truth: `docs/superpowers/specs/2026-06-29-mlops-finetune-scaffolding-design.md`.
> 상세 운영 런북: `.agent/skill/mlops-finetune/SKILL.md` (정비락 복구·hung run 판별·검증 분리).

### 핵심 불변식 (위반 금지)

- **레지스트리가 진실**: 서빙 중인 가중치 = `model_registry` 의 `status='promoted'` 행. **심볼릭링크 아님** (CI `rsync --delete`+`git reset --hard` 가 untracked 링크를 날림).
- **학습셋은 동결 스냅샷**: `train_dataset_versions` 행 = `vlm-dataset/_trainsets/<id>/` 의 immutable 스냅샷. 라이브 라벨 흐름과 무간섭.
- **자기학습 금지**: 모델 파생 라벨(`auto_generated`, Gemini 캡션, `vlm-classification`)로 학습/eval 금지. GT = LS `finalized` 또는 AL-선별-후-사람-어노테이트만.
- **CI 는 학습 안 함**: GPU 학습은 `ENABLE_TRAINING` + 수동 게이트. CI(GPU 없음)는 마이그레이션·스냅샷빌더·eval로직·승격 dry-run·defs 로드만 검증.

### 학습 트리거 (온디맨드 수동, prod 박스)

1. 스냅샷 빌드 (Dagster asset, `defs/train/dataset.py`) → `train_dataset_versions` 행 + `_trainsets/<id>/` 동결. `al_confirmed_count=0` 은 정상(백필 전).
2. **정비 윈도우 진입** (아래 GPU 정비 모드) — 공유 GPU 라 서빙 drain 필수.
3. trainer 기동 — **Dagster run 과 분리된 독립 프로세스**(CI 재배포가 in-run op 고아화):
   ```bash
   cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
   # ENABLE_TRAINING=1 + 학습 대상 train_dataset_version_id 를 env 로 전달
   docker compose --env-file .env run --rm trainer   # profiles:["trainer"], 자동기동 X
   ```
   `gpu_trainer` concurrency=1 (run_coordinator) — 동시 학습 1개만.
4. 산출물: `vlm-dataset/_models/<model>/<version>/` (merged full-weight + `env_lock.json` + `train_log.jsonl` + `training_summary.json`) + `model_registry` `status='candidate'` 행.

### eval 게이트 읽기

- eval asset(`defs/train/eval.py`)이 sealed test split 에서 candidate vs incumbent → `model_registry.metrics` / `incumbent_metrics` 기록.
- `incumbent_source='stock_base'` = 첫 run(이전 promoted 없음, stock 모델을 동일 split 에 통과시킨 점수).
- **per-metric margin + per-class non-regression floor** 통과 시에만 `status='promotable'` 로 승격(평균이 클래스 퇴행 숨기지 않게). margin 기본값은 `eval_config`.
- 현재 상태 확인:
  ```sql
  SELECT model, version, status, incumbent_source, metrics, incumbent_metrics
  FROM model_registry ORDER BY created_at DESC LIMIT 10;
  ```
  `sam3_shadow_compare`(YOLO-동의도, mAP 아님)는 **게이트 아님, 2차 sanity 신호만**.

### 승격 + 롤백 (`scripts/promote_model.py`) — 만들되 기본 미실행

- 승격(`status='promotable'` 행만 대상): MinIO `checkpoint_key` → 호스트 모델 볼륨 다운로드 + `artifact_checksum` 검증 → env 세팅(SAM3=`SAM3_CHECKPOINT_PATH`, PE-Core=`EMBEDDING_CHECKPOINT_PATH`) → `docker recreate`.
  ```bash
  python scripts/promote_model.py promote --model-version-id <id> --env prod   # 볼륨 있는 prod 박스에서만
  python scripts/promote_model.py promote --model-version-id <id> --dry-run     # CI/staging: 무변경 검증
  ```
  성공 시 `status='promoted'`, `promoted_at`/`promoted_env` 기록.
- **롤백**: 이전 `promoted` 행을 재승격(같은 명령, 옛 `--model-version-id`) → recreate. 서빙 시작 로그에 resolved 경로 + checksum 출력 → 확인.
- **PE-Core 승격은 다름** (`scripts/promote_pe_core.py`): 가중치는 벡터 → 재임베딩(`reembed_under_version` asset, gated) 으로 새 `model_name`(`...@ft-<ver>`) 커버리지 확보 → partial HNSW 빌드 → `embedding_active_model` 포인터 원자 전환(AL/검색이 즉시 새 벡터 read). GT(사람검수) < `pe_core_min_gt` 면 게이트가 abstain → GT 축적 전까지 PE 승격 비활성. 롤백 = 포인터를 옛 `model_name` 으로 (옛 벡터/인덱스 보존돼 즉시).

### GPU 정비 모드 (서빙 drain) + 복구

- 학습 전 GPU 서빙을 비워야 함. **공유 `docker-sam3-1`**(prod·staging 공유) 주의 — staging 도 같은 컨테이너를 본다.
- 서버사이드 게이트: `POST /maintenance/enter` → `/segment`·`/embed` 가 `503` + lazy-reload 거부. 완료 후 `POST /maintenance/exit` + `/warmup`.
- **fail-safe**: 정비 플래그에 `owner_run_id`+heartbeat/TTL. guard 센서(`stuck_run_guard` 패턴)가 stale 감지 시 자동 해제. 수동 복구는 `scripts/clear_maintenance.sh` → 상세 절차는 `.agent/skill/mlops-finetune/SKILL.md` §9.
- **⚠️ prod-GPU 주의**: prod main push(docs/tests 제외)는 dagster 무조건 재가동(memory `project_prod_deploy_dagster_restart`). 학습 윈도우 중에는 prod 배포 보류 권장 — 재배포가 정비 상태/in-run op 를 흔든다.

### DVC 큐레이션 데이터 버저닝 (선택)

- 큐레이션 데이터셋은 bare git repo(`/srv/data-repos/dvc-datasets.git`, 앱 배포 경로와 격리) + MinIO `vlm-dataset/_dvc/` (5-버킷 정책). 커밋 = `dataset_catalog` 1행(커밋 메시지 보존).
- pin: `dataset_catalog_aliases`(task당 alias 1개) — `pin_alias()` API 만 갱신. pull: `python scripts/dataset_pull.py --task <t> --alias current --dest <dir>` (기본 dry-run, `--no-dry-run` 으로 실 pull).
- 학습셋 빌더가 pinned alias 를 source 로 쓰면 `train_dataset_versions.dataset_catalog_id` 로 역링크 + MLflow 에 `dvc_*` lineage 기록.

### env 노브

| env | 기본 | 의미 |
|-----|------|------|
| `ENABLE_TRAINING` | `false` | 1/true 일 때만 trainer 가 실제 GPU 학습. CI·staging 은 false 유지 |
| `TRAIN_FULL_FT` | `0` | 1 이면 풀파인튠(16GB 공유 GPU 주의), 기본은 LoRA/PEFT |
| `SAM3_CHECKPOINT_PATH` | (현행) | SAM3 서빙 로컬 가중치 경로 — 승격이 갱신 |
| `EMBEDDING_CHECKPOINT_PATH` | (미설정=stock) | PE-Core 서빙 가중치 경로(신규). 미설정 시 HF Hub stock |
| `EMBEDDING_MODEL_VERSION` | (미설정) | PE-Core 서빙 model_name 버전 태그(`@ft-...`). 승격이 갱신 |
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` | trainer 학습 추적 서버. unreachable 시 fail-soft(레지스트리=SoT) |
| `COMPOSE_PROFILES` | (prod) `...,trainer` | trainer 컨테이너를 prod compose 에 노출(자동기동은 안 함) |

---

## DuckDB 파일 교체 시 주의

1. **반드시 서비스 중지** 후 교체
2. 기존 `.wal` 파일 존재 여부 확인 → 있으면 백업 후 삭제
3. stale WAL이 새 DB에 재적용되면 corruption 발생

---

## Gemini / Vertex AI

- 프로젝트: `your-gcp-project`, 리전: `us-central1`
- 기본 모델: `gemini-2.5-flash`
- credential 우선순위: `GEMINI_GOOGLE_APPLICATION_CREDENTIALS` → `GOOGLE_APPLICATION_CREDENTIALS` → `GEMINI_SERVICE_ACCOUNT_JSON`
- 450MB 초과 영상 → preview mp4 자동 생성 (Vertex 524MB 제한 회피)

---

## YOLO-World

- 모델: `yolov8l-worldv2.pt` (`/data/models/yolo/`)
- dependency 함정: `clip` 패키지 없으면 컨테이너 부팅 실패 → `git+https://github.com/ultralytics/CLIP.git` 필요
- health check: `GET /health` → `model_loaded=true`
