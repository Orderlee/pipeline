# 배포 가이드 — 운영/테스트 환경 분리

> 작성일: 2026-04-10 · 갱신 2026-10-07 (배포 단계·paths-ignore·주의사항을 `scripts/deploy/deploy-stack.sh`·워크플로 기준으로 정정)

## 아키텍처 개요

```
로컬 PC
  ├── dev 브랜치 작업 + pytest
  ├── docker-compose.dev.yaml로 로컬 확인
  ├── dev push  -> test 자동 배포
  └── main push -> production 자동 배포
         │
         ▼
GitHub Actions
  ├── Unit test
  ├── 변경 범위에 따라 이미지 재빌드 여부 판단
  └── Self-hosted runner가 서버의 prod/test 루트에 각각 배포
```

## 브랜치 전략

| 브랜치 | 용도 | 배포 |
|--------|------|------|
| `dev` | 테스트 환경 배포 브랜치 | push 시 test 자동 배포 |
| `main` | 운영 배포 브랜치 | push 시 production 자동 배포 |
| `feature/*` | 기능 개발 | dev에 merge |

**규칙:**
- 운영 서버에서 직접 커밋/push 금지
- `dev`에서 test 배포 검증 후 `main`에 merge
- 긴급 수정: `main`에 직접 push 가능 (workflow_dispatch로 수동 배포도 가능)

## 로컬 개발 환경

### 1. 환경 설정

```bash
cp .env.dev.example .env.dev
# .env.dev 편집 — 로컬 경로에 맞게 수정
```

### 2. 로컬 Docker 실행

```bash
docker compose -f docker/docker-compose.dev.yaml up -d
# Dagster UI: http://localhost:3030
# MinIO Console: http://localhost:9001
```

### 3. pytest 실행

```bash
# pyproject.toml 은 git 미추적 — fresh clone 에서 `pip install -e .` 불가. 의존성 갖춘 venv 에서:
PYTHONPATH=src python -m pytest tests/unit -q
```

## 운영 서버 초기 설정

### 1. Self-Hosted Runner 설치

```bash
bash scripts/deploy/setup-runner.sh
```

토큰 전달 방식은 3가지입니다.

```bash
# 1) 실행 중 프롬프트에 붙여넣기
bash scripts/deploy/setup-runner.sh

# 2) 인자로 전달
bash scripts/deploy/setup-runner.sh --token <registration-token>

# 3) 환경변수로 전달 (권장)
RUNNER_TOKEN=<registration-token> bash scripts/deploy/setup-runner.sh
```

GitHub repo Settings > Actions > Runners에서 토큰을 발급받아 입력합니다.
기본 runner label은 `self-hosted,linux,deploy,production,test`를 권장합니다.

### 2. Runner 상태 확인

```bash
# systemd 서비스 상태
cd ~/actions-runner && sudo ./svc.sh status

# 로그 확인
journalctl -u actions.runner.*.service -f
```

### 3. Docker 권한 확인

```bash
# runner 사용자가 docker 그룹에 포함되어야 함
groups $USER | grep docker || sudo usermod -aG docker $USER
```

## 배포 흐름

### 자동 배포 (일반)

1. `dev` push:
   - test root에 sync
   - `docker/.env.test` 또는 서버의 test env 파일 사용
   - Dagster health check `http://10.0.0.10:3031/server_info`
2. `main` push:
   - production root에 sync
   - 서버의 production `.env` 사용
   - Dagster health check `http://10.0.0.10:3030/server_info`
3. 공통 GitHub Actions 동작:
   - lib layer import 검사 + unit/integration test → 실패 시 배포 중단
   - `detect_image_rebuild` 경로(`src/vlm_pipeline/`, `docker/` 서비스 디렉토리 등) 변경 시 이미지 재빌드
   - 호스트 repo 를 `rsync --delete` + `git reset --hard <SHA>` 로 정렬 — 호스트 수동 수정은 소실
   - postgres healthy 대기 → dagster 3종 **stop/rm 후 재생성**(재빌드 여부와 무관 — 진행 중 run 이 끊긴다) → code-server → daemon → dagster 순
   - 재빌드 시 활성 profile 의 sam3·comfyui·genai·embedding-service 는 `--force-recreate`, analysis 4서비스는 `up -d` 로만 보증
   - Health check `/server_info`

### 수동 배포 (긴급)

GitHub repo > Actions > "Deploy to Test" 또는 "Deploy to Production" > "Run workflow" 클릭
- `skip_tests: true` 옵션으로 테스트 건너뛰기 가능

### 배포 제외 대상

다음 경로만 변경된 push는 배포를 트리거하지 않습니다:
- `docs/**`, `*.md`, `tests/**`, `.cursor/**`, `.agent/**`, `.github/copilot-instructions.md`, `.github/workflows/claude*.yml`, `docker/analysis/**`
- 정본은 `.github/workflows/deploy-{production,test}.yml` 의 `paths-ignore`. 그 밖의 `main` push 는 라벨링 run 을 끊는다.

## 롤백

```bash
# 사용 가능한 이미지 태그 확인
bash scripts/deploy/rollback.sh

# 특정 버전으로 롤백
bash scripts/deploy/rollback.sh datapipeline:abc12345
```

이전 배포의 이미지 태그는 GitHub Actions 실행 로그의 "Deploy summary"에서 확인할 수 있습니다.

## workspace code location 변경 시 안전 절차

[docker/app/workspace.yaml](../../docker/app/workspace.yaml) 의 loader(`python_file` → `grpc_server` 전환, `relative_path`/`attribute` 변경, `location_name` 지정 등)를 바꾸면 Dagster 가 인식하는 code location 식별자가 달라진다. 이전 식별자로 enqueue 돼 있던 queued run 은 daemon dequeue 시점에 `DagsterCodeLocationNotFoundError` 로 실패 처리되므로 배포 전 아래 순서를 반드시 지킬 것.

1. **큐 비우기** — UI(`/runs` → Queued 필터) 또는 CLI 로 대기 중인 run 확인:
   ```bash
   docker exec docker-dagster-1 dagster run list --status QUEUED --limit 100
   ```
   비어 있지 않으면 UI 에서 각 run 을 검토 후 Terminate. (CLI 일괄 terminate 는 중요한 run 을 함께 죽일 위험이 있어 지양.)
2. **workspace 파일 배포** — [docker/app/workspace.yaml](../../docker/app/workspace.yaml) 변경을 커밋·push (또는 runner 가 sync 하도록 대기).
3. **컨테이너 재시작** — `dagster-code-server` → `dagster` → `dagster-daemon` 순서.
4. **등록 확인** —
   ```bash
   curl -s http://127.0.0.1:3030/server_info | jq .
   ```
   응답에 새 location 이 보이고, 이어서 UI `Deployment` 탭에서 상태가 `Loaded` 여야 함.
5. **daemon 로그 10 분 모니터링** —
   ```bash
   docker logs -f docker-dagster-daemon-1 2>&1 | grep -i "CodeLocationNotFound"
   ```
   동일 에러가 더는 출력되지 않으면 정상.

## 주의사항

- **PostgreSQL**: 배포는 `up -d postgres` — compose 의 postgres 정의·`POSTGRES_IMAGE` 가 바뀌면 **recreate**(DB 재기동). prod 컨테이너는 pgvector 가 컨테이너 레이어에 설치돼 있어 recreate 시 소실 → 정의 변경 배포 금지(`CLAUDE.md` §2)
- **MinIO**: prod 는 NAS 박스의 MinIO(`10.0.0.51:9000`) — compose 의 `minio` 서비스는 prod 에서 쓰지 않는다
- **Dagster run history**: `dagster_home/storage/` 바인드로 보존(rsync 가 exclude)
- **GPU 서비스**: 이미지 재빌드가 일어나면 sam3·embedding-service 등이 force-recreate 된다(로드된 모델·정비 상태 초기화). trainer 는 배포가 절대 기동/재생성하지 않는다
- **NAS 마운트**: 호스트 바인드 마운트이므로 배포와 무관
- **env 파일**: production은 서버 로컬 `.env`, test는 `docker/.env.test` 기반으로 관리
- **MinIO Console 주소**: production `9001`, test `9003`
- **애플리케이션 endpoint**: production `9000`, test `9002`
