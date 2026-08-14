# CLAUDE.md — VLM Data Pipeline

> 코드를 읽으면 아는 것은 생략. 코드만으로는 알 수 없는 규칙·환경·운영 맥락만 기록.

---

## 프로젝트 한 줄 요약

CCTV/보안 영상을 수집 → 중복제거 → Gemini 라벨링 → SAM3 bbox 검출 → Label Studio 사람 검수 →
학습 데이터셋 빌드하는 **Dagster + PostgreSQL + MinIO 기반 미디어 데이터 파이프라인**.

> DuckDB write path 는 2026-05-19 에 Postgres 로 cutover 됐고, MotherDuck 동기화 코드는
> `scripts/archive/` 로 이동해 **live 코드에 존재하지 않습니다** (`grep motherduck src/` → 0 hits).
> bbox 는 `ENABLE_YOLO_DETECTION=false` 로 YOLO 대신 SAM3 가 담당합니다.

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
#   - prod: `-p docker --env-file .env` 명시 → NAS_DATA_ROOT 가 nas_primary 로 정상 resolve
#   - staging: `-p pipeline-test --env-file .env.test` 명시 → 프로젝트 이름 + 포트(:3031)+경로(/staging/) 모두 정상 (없으면 PROD 컨테이너 건드림)
# 두 케이스 다 2026-05-19 QA 중 실제 발생. CI deploy-stack.sh 는 이미 --env-file 사용 중 — 수동 ops 만 wrapper 필수.

# Dagster UI  (호스트 IP = 10.0.0.10 — 구 10.0.0.x 주소는 전부 죽었음)
#   production : http://10.0.0.10:3030  (main)
#   staging    : http://10.0.0.10:3031  (dev, 상시 기동 아님)

# DB 쿼리 (호스트에서 직접) — scripts/query_local_duckdb.py 는 scripts/archive/ 로 이동됨
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "SELECT COUNT(*) FROM raw_files;"
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
| **PostgreSQL** | `vlm_pipeline` @ `docker-postgres-1` (호스트 `:15433`) | `vlm_pipeline_staging` @ `pipeline-test-postgres-1` (호스트 `:15432`) |
| MinIO endpoint | `http://10.0.0.51:9000` | `http://10.0.0.51:9002` |
| MinIO Console | `http://10.0.0.51:9001` | `http://10.0.0.51:9003` |
| NAS 루트 (호스트) | `/home/user/mou/nas_primary` | `/home/user/mou/nas_primary/staging` |
| NAS 루트 (컨테이너) | `/nas/data` (단일 바인드) | `/nas/data` (단일 바인드) |
| DAGSTER_HOME (컨테이너) | `/app/dagster_home` | `/app/dagster_home` (동일, 호스트 경로만 다름) |
| env file | `docker/.env` | `docker/.env.test` (스테이징 clone 안에만 존재) |
| dispatch-agent 연동 | `host.docker.internal:8080` | `host.docker.internal:8081` |

두 repo는 각자 독립 `.git`을 보유하며, 브랜치 기준 배포는 CI/CD가 자동 수행합니다 (다음 섹션).

> ⚠️ **스테이징은 상시 기동이 아닙니다.** 필요할 때 `./scripts/compose-staging.sh up -d` 로 올리고
> 검증 후 내립니다. `:3031` 무응답 자체를 장애로 오인하지 마세요.
> (2026-07 기준 `pipeline-test-*` 컨테이너는 장기 정지 상태였음.)

---

## 브랜치 전략 & 배포 (CI/CD)

### 브랜치 역할

- **`dev`** — 스테이징(3031)이 추적. 신기능·실험·리팩터링 진입점.
- **`main`** — 프로덕션(3030)이 추적. `dev`에서 충분히 검증된 뒤에만 머지.

### 자동 배포 (GitHub Actions, self-hosted runner)

| Workflow | 트리거 | 배포 대상 | Runner 라벨 |
|----------|-------|-----------|------------|
| [`deploy-test.yml`](.github/workflows/deploy-test.yml) | `push` → `dev` (+`paths-ignore`) | 스테이징 repo | `self-hosted, linux, test` |
| [`deploy-production.yml`](.github/workflows/deploy-production.yml) | `push` → `main` (+`paths-ignore`) | 프로덕션 repo | `self-hosted, linux, production` |
| [`lint.yml`](.github/workflows/lint.yml) | `push`/`PR` → `dev`,`main` | – | `ubuntu-latest` |
| [`claude.yml`](.github/workflows/claude.yml) | `@claude` 코멘트 | – | `ubuntu-latest` |
| [`claude-review.yml`](.github/workflows/claude-review.yml) | `pull_request_target` | – | `ubuntu-latest` |

배포 워크플로 둘 다 [`scripts/deploy/deploy-stack.sh`](scripts/deploy/deploy-stack.sh)로 실행. 주요 단계:

1. **test 잡** — `scripts/check_lib_layer_imports.py` + `pytest tests/unit` + `pytest tests/integration`
   (PG 사이드카). `workflow_dispatch` 의 `skip_tests=true`로 우회 가능, 긴급시 전용
2. **detect_image_rebuild 잡** — 아래 경로 변경 시 이미지 재빌드:
   `docker/Dockerfile`, `docker/app/`, `configs/`, `scripts/`, `gcp/`, `split_dataset/`,
   `src/python/`, **`src/vlm_pipeline/`**, `src/gemini/`,
   `docker/{sam3,pg-backup,genai,embedding,trainer,mlflow,curation}/`,
   `docker/docker-compose.yaml`, 그리고 배포 workflow 파일 자체
3. **deploy 잡** — 호스트 코드를 deployed SHA로 정렬:
   - **(a) rsync** `-a --delete` 워크스페이스 → DEPLOY_ROOT 동기화 (`src/`, `configs/`, `gcp/`, `scripts/`, `split_dataset/` + `docker/app/` 일부 + compose/Dockerfile). `docker/app/` rsync 는 `dagster_home/`, `dagster_home_staging/`, `credentials/` 를 `--exclude`. `docker/data/` 는 애초에 rsync 소스가 아니고 gitignore 대상이라 양쪽 모두 안 건드림
   - **(b) git hard-reset** `git -C ${DEPLOY_REPO_ROOT} fetch origin && reset --hard ${GITHUB_SHA}` — 호스트 git tree(`.git/HEAD`, `git log`, `git status`)를 deployed commit과 정확히 일치시킴. **rsync로 src 파일은 갱신되지만 `.git`은 안 건드리므로** 이 step이 없으면 호스트의 `git log`가 영원히 stale로 보임. tracked 파일만 reset되고 `dagster_home/` 등 untracked는 유지됨.
4. env 파일 복원 + `REQUIRED_ENV_KEYS` 검증 (누락 시 hard fail), MinIO 키 자동 파생
5. `postgres` healthy 대기 → dagster 3종 stop/rm → code-server → daemon → dagster 순차 기동 →
   profile 별 조건부 build/recreate(`sam3`/`pg-backup`/`genai`/`embedding-service`/`trainer`) →
   HEALTHCHECK_URL 응답 검증 (prod `:3030/server_info`, staging `:3031/server_info`)
6. AI deploy 분석 (Claude CLI, best-effort, 실패해도 배포는 성공)

> ✅ **단일 진리 원칙**: deploy 후 `호스트 git HEAD == 컨테이너 이미지 안 src == 실행 코드`가 항상 일치한다.
> 호스트 src는 컨테이너에 mount되지 않으므로 (이미지 빌드 시 `COPY src/` 결과만 사용) **호스트에서 손으로**
> src를 고쳐도 컨테이너 동작은 안 바뀐다 — 즉시 반영은 `docker compose build` 후 재기동.
> 단, **CI 경로로 들어온 `src/vlm_pipeline/` 변경은 재빌드 트리거에 포함**되므로 자동 반영된다.

> ⚠️ **배포 = 라벨링 중단**: `docs/**`, `*.md`, `tests/**`, `.cursor/**`, `.agent/**`,
> `.github/copilot-instructions.md`, `.github/workflows/claude*.yml` 는 `paths-ignore` 로 배포를
> 아예 트리거하지 않는다. 그 밖의 `main` push 는 **이미지 재빌드 여부와 무관하게** dagster 3종을
> 항상 stop→rm→recreate 하므로 진행 중 run 이 끊긴다 (deploy-stack.sh 의 이 구간은 `BUILD_REQUIRED`
> 가드 밖에 있음).

> ⚠️ **fork 구분**: 두 워크플로 모두 `if: github.repository == 'Orderlee/Datapipeline-Data-data_pipeline'` 조건 있음 — self-hosted runner 도 `origin`(Orderlee)에만 등록돼 있어 `upstream`(upstream-org)으로 PR/머지가 가면 배포가 트리거되지 않음.

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
  - `resources/`: `postgres_base.py` + `postgres_migration.py` + `postgres_ingest_*.py` +
    도메인별 `postgres_{build,dedup,detection,embedding,genai,labeling,process,spec,train,...}.py`
    (`duckdb_*.py` 파일은 전부 제거됨)
- **커밋**: conventional commits (`feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`)
  - "어떻게 수정했다"보다 **"무엇과 왜 수정했는지"** (`.gitmessage.txt` 참고)
- **에러 처리**: per-file fail-forward — 한 파일 실패해도 나머지 계속 처리
- **테스트**: pytest, Postgres fixture, mocked MinIO (`unittest.mock`), `tests/conftest.py` 공통 fixture

---

## 핵심 운영 규칙 (코드에 안 드러나는 것)

### 동시성 (DuckDB 시절 규칙 폐기됨)
- **`duckdb_writer` 계열 태그는 더 이상 없다.** `build_asset_job(writer_tag=...)` 인자는 하위 호환용
  시그니처로만 남아 있고 아무 동작도 하지 않는다 (`definitions_production.py` 의 `# noqa: ARG001`).
  Postgres 는 파일 락이 아니라 커넥션 기반이라 writer-lane 직렬화가 불필요해짐.
- 현재 `run_coordinator` (`docker/app/dagster_home/dagster.yaml`):
  `max_concurrent_runs: 20`, `gpu_trainer` limit 1, `pg_writer` limit 1
- ⚠️ **`pg_writer` 는 설정만 있고 이 태그를 붙인 asset/op 이 하나도 없다** (현재 no-op).
  단위 테스트는 yaml 설정 존재만 검증하므로 초록색이어도 실제 직렬화는 안 걸린다.
  `gpu_trainer` 는 `defs/embed/reembed.py` 가 실제로 사용 중.

### NAS 장애 대응 (CIFS)
- sensor에서 `OSError/PermissionError/TimeoutError` → graceful skip, 다음 tick 재시도
- NAS 지연 시 권장 설정:
  - `AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES=20`
  - `AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK=3`
  - `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS=300`

### 파일 오류 정책
- `file_missing`, `empty_file`, `ffprobe_failed` → **DB 미삽입 + archive 미이동**
- 추적은 JSONL 실패 로그(`<manifest_dir>/failed/*.jsonl`)에만 기록
- transient 오류 → retry manifest 자동 생성, failed row 아님
- **중복 판정 2단계**: `raw_files.checksum` UNIQUE(정확 중복, 비디오·이미지 공통) +
  이미지 전용 pHash Hamming ≤ 5(근사 중복 → `dup_group_id` 부여, run 은 계속).
  단, 방금 업로드한 이미지의 phash **계산 자체가 실패**하면 `gated_failed` 로 run 전체가 실패한다.

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
3. staging Postgres 초기화 — `pipeline-test-postgres-1` 의 `vlm_pipeline_staging` DB.
   (구 `docker/data/staging.duckdb` 파일은 이제 write path 가 아니라 무관한 잔재)
4. `Datapipeline-Data-data_pipeline_test/docker/app/dagster_home/storage/` 내용 삭제 (run·sensor·schedule 상태 초기화)
   - ⚠️ **storage 를 지우면 센서 RUNNING/STOPPED 토글도 초기화된다.** `dispatch_sensor` 와
     `production_agent_dispatch_sensor` 는 **코드 기본값이 STOPPED** 이라, 재기동 후 UI 에서
     다시 켜지 않으면 자동 라벨링이 조용히 멈춘 상태가 된다.
5. 재기동: `./scripts/compose-staging.sh up -d` (스테이징 clone 에서)
- ⚠️ staging incoming/archive 원본 폴더(`/home/user/mou/nas_primary/staging/incoming`, `/home/user/mou/nas_primary/staging/archive`)는 명시 요청 없으면 **절대 삭제 금지**

---

## 서비스 네트워크 & 볼륨 (코드에서 놓치기 쉬운 것)

- Docker network: `pipeline-network`
- **호스트 ↔ 컨테이너 경로 매핑** (compose의 bind mount) — NAS_primary 는 **CIFS(vers=3.0) 로
  `//10.0.0.51/data`** 에서 마운트된다 (NFS 아님, 구 `10.0.0.51` 주소 아님).
  nas_secondary 는 별도 CIFS(`10.0.0.36`).

  | 호스트 | 컨테이너 | 비고 |
  |---|---|---|
  | `${NAS_DATA_ROOT}` = `/home/user/mou/nas_primary` (staging: `.../staging`) | `/nas/data` | **단일 부모 바인드** |
  | `${DATASETS_HOST_PATH}` = `/home/user/mou/nas_secondary/datasets` | `/nas/datasets` | rw |
  | `${PROJECTS_HOST_PATH}` | `/nas/datasets/projects` | ro |
  | `${DAGSTER_HOME_HOST_PATH}` = `./app/dagster_home` | `/app/dagster_home` | 런타임 상태 |
  | `${DOCKER_DATA_HOST_PATH}` = `./data` | `/data` | 모델 캐시·fiftyone |

  - incoming/archive/manifest 는 **별도 바인드가 아니라** 그 단일 바인드 안의 env 서브경로다:
    `INCOMING_DIR=/nas/data/incoming`, `ARCHIVE_DIR=/nas/data/archive`,
    `MANIFEST_DIR=/nas/data/incoming/.manifests`.
    한 마운트로 합친 이유는 archive 폴더 단위 이동이 `os.rename` fast-path 를 타야 하기 때문
    (쪼개면 `EXDEV` 로 전체 복사).
  - ⚠️ `.env` 의 `INCOMING_HOST_PATH` / `ARCHIVE_HOST_PATH` 는 **compose volumes 에서 더 이상
    참조되지 않는다** (참고용 잔재). 진실은 `NAS_DATA_ROOT` + `INCOMING_DIR`/`ARCHIVE_DIR`.
  - **운영자 주의**: `user` 유저는 NAS_primary 상에서 quota 가 걸려있어 호스트에서 직접 `cp`/`mkdir` 시 "디스크 할당량 초과" 발생. 큰 파일을 incoming 에 넣을 땐 컨테이너(root) 경유 (`docker run --rm -v /home/user/mou/nas_primary/...:/dst alpine cp ...`) 또는 quota 정리 필요.
  - 코드→실행 경로: **mount 없음**. 컨테이너는 이미지 빌드 시 Dockerfile `COPY src/ /src/vlm/`로 들어간 src만 사용 (`/src/vlm`, `/src/python`). 호스트에서 손으로 고친 src 는 `docker compose build` 전까지 반영되지 않음 (CI 배포는 재빌드 트리거에 `src/vlm_pipeline/` 이 포함돼 자동 반영).
- **GPU 할당 정책 (2026-05-22 업데이트)**:
  - **dagster 계열**: 호스트 GPU 0+1 둘 다 노출 (`CUDA_VISIBLE_DEVICES=0,1` + `NVIDIA_VISIBLE_DEVICES=0,1`).
    - Python torch (Places365) → default `cuda:0` = 호스트 GPU 0 (CUDA cores)
    - ffmpeg NVENC → `REENCODE_NVENC_GPU_INDICES` (default "0,1") round-robin → 양 GPU 의 NVENC unit 활용 (RTX A4000 NVENC unit GPU 당 1개)
  - **SAM3 (별도 컨테이너)**: 호스트 GPU 1 의 CUDA cores 만 사용 (`CUDA_VISIBLE_DEVICES=1`). 컨테이너 view 에서는 `cuda:0` 로 보이지만 호스트는 GPU 1.
    - **workers=3** (`SAM3_WORKERS`, prod `.env` 현재값) — process 3개 model 로드 ≈ 11.1 GB / 16 GB.
      2026-05-27 에 workers=4 (≈14.8 GB) 가 eng-b ComfyUI 등과 공유 시 파편화로 CUDA OOM(503) 발생 →
      2 로 완화 후 3 으로 재상향한 값이다. 올릴 때 이 히스토리 확인.
    - ⚠️ 정비 플래그(`/maintenance/enter`)는 **프로세스 메모리 기반**이라 uvicorn worker 3개에
      공유되지 않는다 — 한 worker 에 enter 를 걸어도 나머지 2개는 계속 요청을 받는다 (drain 미완).
  - **embedding-service**: 호스트 GPU **0** (`CUDA_VISIBLE_DEVICES=0`) — dagster torch/NVENC 와 GPU0 공유
  - **trainer**: 호스트 GPU 1 — SAM3 와 같은 GPU 라 학습 전 정비 drain 필요
  - **YOLO (별도 컨테이너)**: 호스트 GPU 1 — 현재 `ENABLE_YOLO_DETECTION=false` 정책으로 비활성 (컨테이너도 정지 상태)
  - **경합 분석**: dagster NVENC (GPU 0/1 의 NVENC unit) ↔ SAM3 (GPU 1 의 CUDA cores) — 별개 hardware unit 이라 같은 GPU 1 안에서도 동시 사용 OK
- Places365 모델 캐시: `/data/models/places365` (auto_download=false, 고정 캐시만 사용)
- `PYTHONPATH` (컨테이너): `/:/src/python:/src/vlm`
- **호스트 포트 ≠ 컨테이너 포트인 서비스** (`.env` 로 매핑되므로 착각하기 쉬움):
  `embedding-service` 8003→**8004**, `genai` 8088→**8089**, `mlflow` 5000→**5500**,
  `analysis` FiftyOne 5151→**5153** / Streamlit 8501→**8503**, `postgres` 5432→**15433**
- **`docker-analysis-1` 은 JupyterLab 만 자동 기동**한다. FiftyOne(:5153)·Streamlit(:8503) 은
  포트만 열려 있고 프로세스는 안 뜨므로 **컨테이너 재시작·recreate·호스트 재부팅 때마다
  `docker exec` 로 다시 띄워야 한다** (절차는 `docker/analysis/README.md`).
  컨테이너 `/workspace` 코드는 이미지에 2개 파일만 COPY 되고 나머지는 수동 복사본이라
  git 과 drift 한다 — 위 "단일 진리 원칙"은 `src/` 에만 적용되고 **이 컨테이너엔 적용되지 않는다**.
- ⚠️ **배포는 analysis 컨테이너를 건드리지 않는다** (2026-08-10 실측: `deploy-stack.sh` 에
  `analysis` 분기 0회, 재빌드 트리거에 `docker/analysis/` 0회, analysis 컨테이너가 dagster
  recreate 를 생존). 그래서 역방향 함정이 있다 — `docker/analysis/**` 는 `paths-ignore` **밖**이라
  main push 가 **dagster 3종만 recreate 시켜 라벨링을 끊고** analysis 에는 아무 효과가 없다.
  **분석 코드만 담은 main push 금지** — `dev` 로 보내거나 다른 배포와 묶고, 반영은 `docker cp`.
- prod MinIO 는 compose 의 `minio` 서비스가 아니라 **NAS 박스의 MinIO**(`10.0.0.51:9000`)다.
  로컬 `minio` 컨테이너는 prod 에서 기동하지 않는다.

---

## 자주 쓰는 스크립트

| 스크립트 | 용도 | 상태 |
|---------|------|------|
| ~~`scripts/query_local_duckdb.py`~~ | 로컬 DuckDB 읽기 쿼리 | **`scripts/archive/` 로 이동됨 ❌** — 대신 `docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "..."` |
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
| `scripts/repair_unsanitized_raw_keys.py` | 비정규 MinIO 키(`source-h/<한글>`) → 정본 `raw_key`(`source-h/<sanitize>`) 서버사이드 복사 | 복구 (기본 dry-run, `--apply`). source-h 804건 대기 중 |

### Deprecated (scripts/archive/ 로 이동됨)

다음 일회성 스크립트는 사용 완료로 `scripts/archive/` 로 이동됨 (OPS-STALE-DUCKDB-SCRIPTS Stage 1):

- `scripts/archive/migrate_yolo_detection_json_to_coco.py` — YOLO JSON → COCO 마이그레이션 (완료)
- `scripts/archive/migrate_gcp_raw_keys.py` — GCP raw_key prefix 마이그레이션 (완료)
- `scripts/archive/fix_failed_status.py` — failed → completed 픽스 (완료)
- `scripts/archive/fix_uploading_status.py` — uploading → completed 픽스 (완료)
- `scripts/archive/recover_uploading.py` — uploading 복구 (완료)
- `scripts/archive/backfill_vhc_sam3_bbox.py` — VHC 288건 SAM3 bbox 백필 (완료된 일회성, 제거된 duckdb 모듈 import 라 현행 미실행)
- `scripts/archive/run_scanner.sh` — legacy 스캐너 shim (대체: auto_bootstrap 센서 / `scripts/bootstrap_manifest.sh`)

---

## Label Studio 연동

- compose: `docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml up -d`
  (현재 prod 는 `docker-compose.labelstudio.local.yaml` 까지 얹어 커스텀 이미지
  `labelstudio-internal:1.23.0-c4` 로 기동 — 이 override 파일은 git 미추적)
- **compose project 가 `pipeline`** 이라 컨테이너 이름이 `pipeline-labelstudio-1` /
  `pipeline-ls-webhook-1` 이다 (파이프라인 본체의 `docker-*` prefix 와 다름)
- LS UI: `http://10.0.0.10:8084` (기본 8080이나 dispatch-agent 충돌로 `LS_PORT=8084` 사용)
- ⚠️ **LS 앱 DB 는 `pipeline-postgres-1` 의 `airflow` DB** — 파이프라인 DB(`docker-postgres-1`
  의 `vlm_pipeline`) 와 다른 인스턴스다. 공유 `pipeline-network` 에 `postgres` alias 를 가진
  컨테이너가 둘 있어 DNS round-robin 으로 엉뚱한 DB 에 붙는 사고가 있었으므로
  `POSTGRE_HOST=pipeline-postgres-1` 처럼 **컨테이너명을 명시**해야 한다
- 필수 env: `LS_API_KEY` (LS 계정 설정에서 발급), `WEBHOOK_HOST` (LS→webhook 접근 IP)
- sensor `ls_task_create_sensor`: **코드 기본값이 RUNNING** (`defs/ls/sensor.py` 의
  `default_status=DefaultSensorStatus.RUNNING`) — 수동 ON 불필요
- presign 자동 갱신 스케줄 `ls_presign_renew_schedule` (05:00 KST) 는 기본 STOPPED — 필요 시 UI 에서 활성
- 검수 흐름: LS submit(`/sync`) → `review_status='reviewed'` →
  Slack `/sync-approve <project_id>` → `'finalized'` + `image_label_annotations` 투영 →
  `post_review_clip_job`(clip 분할) + `build_dataset_on_finalize_sensor`
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
   # ENABLE_TRAINING=1 + 학습 대상 train_dataset_version_id 를 env 로 전달
   COMPOSE_PROFILES=trainer ./scripts/compose-prod.sh run --rm trainer
   ```
   `profiles:["trainer"]` 라 자동기동 안 함. 서비스명을 명시하는 `run` 은 profile 미포함이어도
   실행되지만, wrapper 사용 원칙에 맞춰 위 형태를 쓴다.
   실제 안전 게이트는 `ENABLE_TRAINING` — 미설정이면 dry-run 으로 끝난다.
   `gpu_trainer` concurrency=1 (run_coordinator) — 동시 학습 1개만.
   배포는 trainer 를 절대 기동/recreate 하지 않는다 (deploy-stack.sh 명시).
4. 산출물: `vlm-dataset/_models/<model>/<version>/` (merged full-weight + `env_lock.json` + `train_log.jsonl` + `training_summary.json`) + `model_registry` `status='candidate'` 행.

### eval 게이트 읽기

- eval asset(`defs/train/eval.py`)이 sealed test split 에서 candidate vs incumbent → `model_registry.metrics` / `incumbent_metrics` 기록.
- ⚠️ **현재 실제 채점은 미구현**: `_score_candidate()` / `_score_incumbent()` 가
  `NotImplementedError` 를 던진다. 게이트 판정 로직·상태 전이·레지스트리 기록만 구현돼 있고
  GPU 채점부는 테스트에서 monkeypatch 로만 검증된다. **eval 게이트는 아직 turnkey 가 아니다.**
- `incumbent_source='stock_base'` = 첫 run(이전 promoted 없음, stock 모델을 동일 split 에 통과시킨 점수).
- **per-metric margin + per-class non-regression floor** 통과 시에만 `status='promotable'` 로 승격(평균이 클래스 퇴행 숨기지 않게). margin 기본값은 `model_registry.eval_config` **JSONB 컬럼**(별도 테이블 아님).
- 현재 상태 확인:
  ```sql
  SELECT model, version, status, incumbent_source, metrics, incumbent_metrics
  FROM model_registry ORDER BY created_at DESC LIMIT 10;
  ```
  `sam3_shadow_compare`(YOLO-동의도, mAP 아님)는 **게이트 아님, 2차 sanity 신호만**.

### 승격 + 롤백 (`scripts/promote_model.py`) — 만들되 기본 미실행

- 승격(`status='promotable'` 행만 대상): MinIO `checkpoint_key` → 호스트 모델 볼륨 다운로드 + `artifact_checksum` 검증 → env 세팅(SAM3=`SAM3_CHECKPOINT_PATH`, PE-Core=`EMBEDDING_CHECKPOINT_PATH`) → `docker recreate`.
  ```bash
  # ⚠️ `promote` 서브커맨드는 없다. 플래그만 있고 --model 은 필수, 기본은 dry-run.
  python scripts/promote_model.py --model sam3 --model-version-id <id> --env prod --apply
  python scripts/promote_model.py --model sam3 --model-version-id <id>            # 기본 dry-run
  ```
  성공 시 `status='promoted'`, `promoted_at`/`promoted_env` 기록.
- ⚠️ **알려진 버그 (미수정)**: `--model-version-id` 가 `type=int` 로 선언돼 있는데
  `model_registry.model_version_id` 는 `TEXT`(`mv-3f9a2b1c4d5e` 형태)다 → 실제 ID 를 넘기면
  argparse 가 `invalid int value` 로 죽는다. `promote_pe_core.py` 는 이 버그 없음.
- ⚠️ **SAM3 env 주입은 사실상 no-op**: compose 의 `sam3` 서비스는 `SAM3_CHECKPOINT_PATH` 를
  `${...}` 치환 없이 리터럴 `/models/sam3.1_multiplex.pt` 로 박아뒀다. 승격이 동작하는 실제 이유는
  같은 고정 호스트 경로에 새 체크포인트 **바이트를 덮어쓰기** 때문이지 env 때문이 아니다.
  (PE-Core 쪽 `EMBEDDING_CHECKPOINT_PATH`/`EMBEDDING_MODEL_VERSION` 은 정상적으로 치환됨.)
- **롤백**: `--rollback` 은 **직전 `archived` + `promoted_at IS NOT NULL` 행을 자동 선택**한다 —
  임의의 옛 `--model-version-id` 를 지정해 되돌릴 수는 없다 (해당 인자는 rollback 분기에서 무시됨).
  서빙 시작 로그에 resolved 경로 + checksum 출력 → 확인.
- **PE-Core 승격은 다름** (`scripts/promote_pe_core.py`): 가중치는 벡터 → 재임베딩(`reembed_under_version` asset, gated) 으로 새 `model_name`(`...@ft-<ver>`) 커버리지 확보 → partial HNSW 빌드 → `embedding_active_model` 포인터 원자 전환(AL/검색이 즉시 새 벡터 read). GT(사람검수) < `pe_core_min_gt` 면 게이트가 abstain → GT 축적 전까지 PE 승격 비활성. 롤백 = 포인터를 옛 `model_name` 으로 (옛 벡터/인덱스 보존돼 즉시).

### GPU 정비 모드 (서빙 drain) + 복구

- 학습 전 GPU 서빙을 비워야 함. **공유 `docker-sam3-1`**(prod·staging 공유) 주의 — staging 도 같은 컨테이너를 본다.
- 서버사이드 게이트: `POST /maintenance/enter` → `/segment`·`/embed` 가 `503` + lazy-reload 거부. 완료 후 `POST /maintenance/exit` + `/warmup`.
- **fail-safe**: 정비 플래그(`gpu_maintenance_lock` 테이블)에 `owner_run_id`+heartbeat/TTL.
  `maintenance_guard_sensor` 가 stale 감지 시 자동 해제. 수동 복구는 `scripts/clear_maintenance.sh`
  → 상세 절차는 `.agent/skill/mlops-finetune/SKILL.md` §9.
- ⚠️ **`clear_maintenance.sh` 의 기본 URL 이 죽어 있다**: `SAM3_API_URL` 기본값이
  `http://10.0.0.10:8002` (도달 불가), `EMBEDDING_API_URL` 기본값은 IP 도 포트도 틀림
  (`:8000`, 실제 컨테이너 포트 8003). `curl -sf` 라 타임아웃이 WARN 으로 삼켜져 **아무것도 안 하고
  성공처럼 보인다.** 실행 전 반드시 env 를 명시할 것:
  ```bash
  SAM3_API_URL=http://localhost:8002 EMBEDDING_API_URL=http://localhost:8004 \
    scripts/clear_maintenance.sh all       # 인자는 positional [sam3|pe_core|all] — `--env prod` 아님
  ```
  `.agent/skill/mlops-finetune/SKILL.md` §9 의 예시 호출(`--env prod`)도 같은 이유로 틀렸다.
- **⚠️ prod-GPU 주의**: prod main push(docs/tests 제외)는 dagster 무조건 재가동(memory `project_prod_deploy_dagster_restart`). 학습 윈도우 중에는 prod 배포 보류 권장 — 재배포가 정비 상태/in-run op 를 흔든다.

### DVC 데이터셋 버저닝 (선택)

- 큐레이션 데이터셋은 bare git repo(`/srv/data-repos/dvc-datasets.git`, 앱 배포 경로와 격리) + MinIO `vlm-dataset/_dvc/` (5-버킷 정책). 커밋 = `dataset_catalog` 1행(커밋 메시지 보존).
- ⚠️ **아직 dagster 컨테이너에 배선되지 않았다**: `/srv/data-repos/` 는 호스트에 실재하지만
  compose 에 bind-mount 가 없고 `DVC_DATA_REPO_PATH` 도 미설정이라
  `dataset_catalog_reconciliation_sensor` 는 self-skip 상태다.
- ⚠️ `/srv/data-repos/dvc-ingest.env` (post-receive 훅이 source 하는 파일) 가 구 MinIO IP
  `10.0.0.51` 를 하드코딩하고 있어 **git push 기반 자동 카탈로그 ingest 는 현재 깨져 있을 가능성이 높다.**
- pin: `dataset_catalog_aliases`(task당 alias 1개) — `pin_alias()` API 만 갱신. pull: `python scripts/dataset_pull.py --task <t> --alias current --dest <dir>` (기본 dry-run, `--no-dry-run` 으로 실 pull).
  ⚠️ 실 pull 의 md5 검증은 스텁(`_computed_md5()` 가 항상 `None`)이라 성공해도 mismatch 로 exit 3 난다.
- 학습셋 빌더가 pinned alias 를 source 로 쓰면 `train_dataset_versions.dataset_catalog_id` 로 역링크 + MLflow 에 `dvc_*` lineage 기록.

### env 노브

| env | 기본 | 의미 |
|-----|------|------|
| `ENABLE_TRAINING` | `false` | 1/true 일 때만 trainer 가 실제 GPU 학습. CI·staging 은 false 유지 |
| `TRAIN_FULL_FT` | `0` | 1 이면 풀파인튠(16GB 공유 GPU 주의), 기본은 LoRA/PEFT |
| `SAM3_CHECKPOINT_PATH` | compose 에 **리터럴 하드코딩** | `/models/sam3.1_multiplex.pt` — `${}` 치환 아님. 승격이 `.env` 에 써도 컨테이너는 안 읽는다 (위 §승격 주의 참고) |
| `EMBEDDING_CHECKPOINT_PATH` | (미설정=stock) | PE-Core 서빙 가중치 경로. 미설정 시 HF Hub stock. compose 에서 정상 치환됨 |
| `EMBEDDING_MODEL_VERSION` | (미설정) | PE-Core 서빙 model_name 버전 태그(`@ft-...`). 승격이 갱신 |
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` | trainer 학습 추적 서버(compose 의 trainer 블록 기본값). unreachable 시 fail-soft(레지스트리=SoT) |
| `COMPOSE_PROFILES` | (prod 실제값) `sam3,backup,genai,embedding,analysis` | **`trainer`·`mlflow` 는 들어 있지 않다.** 이 변수는 `up -d`/전체 `build` 만 게이트하고, 서비스명을 명시한 `run --rm trainer` 는 게이트하지 않는다 |

> ⚠️ **MLflow 는 profile 밖에서 수동 기동된 상태**다 (`docker-mlflow-1` 가 떠 있지만
> `COMPOSE_PROFILES` 에 없음). 호스트 재부팅이나 profile 기반 전체 재기동 후에는 **자동으로
> 돌아오지 않고**, trainer 는 fail-soft 로 조용히 추적 없이 학습한다.
> backend store = PG `mlflow` DB, artifact = `s3://vlm-dataset/_mlflow/`.

---

## DuckDB (레거시 — 현재 write path 아님)

2026-05-19 Postgres cutover 이후 파일 기반 DuckDB 는 **운영 경로에서 제외**됐습니다.

- `docker/data/pipeline.duckdb` / `staging.duckdb` 는 남아 있어도 읽고 쓰지 않는 잔재입니다
- `.env` 의 `DUCKDB_PATH` / `DATAOPS_DUCKDB_PATH` 도 잔재 (다만 배포 스크립트의
  `REQUIRED_ENV_KEYS` 기본값에 아직 들어 있어 지우면 배포가 실패할 수 있음 — 건드리지 말 것)
- DuckDB 문법은 `pg_duckdb` extension 경유 **분석 쿼리에서만** 재사용합니다
- 옛 DuckDB 파일을 굳이 교체해야 한다면: 서비스 중지 → `.wal` 백업 후 삭제 → 교체
  (stale WAL 재적용 시 corruption)

---

## Gemini / Vertex AI

- 프로젝트: `your-gcp-project`, 리전: `us-central1`
- 기본 모델: `gemini-2.5-flash`
- credential 우선순위: `GEMINI_GOOGLE_APPLICATION_CREDENTIALS` → `GOOGLE_APPLICATION_CREDENTIALS` → `GEMINI_SERVICE_ACCOUNT_JSON`
- 450MB 초과 영상 → preview mp4 자동 생성 (Vertex 524MB 제한 회피)

---

## SAM3 (현재 기본 bbox 엔진)

- 컨테이너 `docker-sam3-1`, 호스트 포트 `8002`. **prod·staging 이 이 하나를 공유**
  (staging 은 `SAM3_API_URL=http://docker-sam3-1:8002` 로 참조, 자기 SAM3 를 안 만든다)
- 체크포인트 `/models/sam3.1_multiplex.pt`, workers 3, 호스트 GPU 1
- 엔드포인트: `/segment` `/health` `/info` `/warmup` `/unload` `/maintenance/{enter,exit,heartbeat,status}`
- 결과: COCO JSON → `vlm-labels/<source>/sam3_segmentations/<stem>.json` + `image_labels`
  (`label_tool='sam3'`, `label_format='coco'`, `review_status='auto_generated'`)
- 검수 전 스냅샷 `*.pseudo.json` 을 write-once 로 남긴다 — pseudo-label QA 가 이것만 읽는다
  (라이브 JSON 은 LS 검수가 덮어써서 pseudo==GT 오염이 났던 이력)

---

## 임베딩 / pgvector

- `embedding-service` 컨테이너, 호스트 포트 **`8004`** → 컨테이너 8003, 호스트 GPU 0
- 모델 PE-Core-L14-336 (`open_clip`, `hf-hub:timm/PE-Core-L-14-336`), 1024-d
- 벡터 → `image_embeddings` (pgvector). `entity_type` = `frame`/`caption`/`video`/`detection`,
  `UNIQUE(entity_type, entity_id, model_name)`
- 인덱스는 **entity_type 별 partial HNSW** (통합 인덱스는 제거됨)
- 서빙 모델 포인터 = `embedding_active_model` 테이블 단일 행. 파인튠 승격은 재임베딩 후
  이 포인터를 원자 전환하는 방식 (`scripts/promote_pe_core.py`)

---

## YOLO-World (레거시 — 현재 비활성)

- `ENABLE_YOLO_DETECTION=false`, `docker-yolo-1` 컨테이너도 정지 상태. bbox 는 SAM3 담당
- 모델: `yolov8l-worldv2.pt` (`/data/models/yolo/`)
- dependency 함정: `clip` 패키지 없으면 컨테이너 부팅 실패 → `git+https://github.com/ultralytics/CLIP.git` 필요
- health check: `GET /health` → `model_loaded=true`
- `sam3_shadow_compare` 는 기존 YOLO 라벨(`label_tool='yolo-world'`)이 있는 이미지만 비교 대상으로
  잡으므로, YOLO 를 끈 뒤 새로 들어온 데이터에서는 사실상 동작하지 않는다 (게이트 아님)

---

## GenAI Studio

- 컨테이너 `docker-genai-1`, 호스트 포트 **`8089`** → 컨테이너 8088. Basic Auth
- Kling / Veo 기반 생성형 증강 (`GENAI_ENGINES_ENABLED=kling,veo`;
  higgsfield·nanobanana·gpt_image 어댑터도 코드에는 있으나 prod 미활성)
- `genai_poll_sensor` 가 HTTP 로 내부 API 를 폴링 (Dagster 가 어댑터 코드를 직접 import 하지 않음)
- 생성물은 `/nas/data/genai_studio` 로 격리 — 일반 incoming 에 넣으면 auto-bootstrap 이
  카메라 영상으로 오인해 수집한다. `promote-to-labeling` 이 dispatch JSON 을 만들어 정식 편입
- 코드가 이미지에 COPY-baked 라 변경 시 재빌드 필요 (CI 는 `docker/genai/` 변경을 감지해 자동 재빌드)
