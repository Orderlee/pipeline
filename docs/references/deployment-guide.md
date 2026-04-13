# 배포 가이드 — 운영/테스트 환경 분리

> 작성일: 2026-04-10

## 아키텍처 개요

```
로컬 PC (dev 브랜치)
  ├── 코드 수정 + pytest
  ├── docker-compose.dev.yaml로 로컬 테스트
  └── git push → main 브랜치 merge
         │
         ▼
GitHub Actions (main push 트리거)
  ├── Unit test 실행
  └── Self-hosted runner가 운영 서버에서:
         ├── Docker 이미지 빌드
         ├── Dagster 서비스 순차 재시작
         └── Health check
         │
         ▼
운영 서버 (production만 실행)
  └── 코드 직접 수정 금지
```

## 브랜치 전략

| 브랜치 | 용도 | 배포 |
|--------|------|------|
| `dev` | 개발/테스트 (로컬 PC) | 없음 |
| `main` | 운영 배포 트리거 | push 시 자동 배포 |
| `feature/*` | 기능 개발 | dev에 merge |

**규칙:**
- 운영 서버에서 직접 커밋/push 금지
- `dev`에서 충분히 테스트 후 `main`에 merge
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
pip install -e ".[dev]"
pytest tests/unit -q
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

1. 로컬에서 `dev` 브랜치 작업 후 `main`에 merge
2. GitHub Actions 자동 실행:
   - Unit test → 실패 시 배포 중단
   - Docker 이미지 빌드 (git SHA 태깅)
   - dagster-code-server 재시작 → 15초 대기
   - dagster-daemon + dagster 재시작
   - Health check (최대 60초)

### 수동 배포 (긴급)

GitHub repo > Actions > "Deploy to Production" > "Run workflow" 클릭
- `skip_tests: true` 옵션으로 테스트 건너뛰기 가능

### 배포 제외 대상

다음 경로만 변경된 push는 배포를 트리거하지 않습니다:
- `docs/**`, `*.md`, `tests/**`, `.cursor/**`, `.agent/**`

## 롤백

```bash
# 사용 가능한 이미지 태그 확인
bash scripts/deploy/rollback.sh

# 특정 버전으로 롤백
bash scripts/deploy/rollback.sh datapipeline:abc12345
```

이전 배포의 이미지 태그는 GitHub Actions 실행 로그의 "Deploy summary"에서 확인할 수 있습니다.

## 주의사항

- **DuckDB**: 볼륨 마운트이므로 배포 시 영향 없음
- **MinIO**: Docker named volume이므로 컨테이너 재시작에 안전
- **Dagster run history**: `dagster_home/storage/` 볼륨으로 보존
- **GPU 서비스 (YOLO, SAM3)**: 파이프라인 코드 변경과 무관 — 별도 재시작 불필요
- **NAS 마운트**: 호스트 바인드 마운트이므로 배포와 무관
- **.env 파일**: git에 포함되지 않으므로 운영 서버에서 직접 관리

## Staging 정리 (선택)

운영 서버에서 staging을 완전히 제거하려면:

```bash
# 1. staging 서비스 중지
cd docker && docker compose --profile staging down

# 2. staging 데이터 정리 (선택)
rm -f docker/data/staging.duckdb
rm -rf docker/app/dagster_home_staging/storage/
```

`docker-compose.yaml`에서 staging 관련 서비스(`dagster-staging`, `dispatch-webhook-staging`)와
`x-staging-volumes`는 운영에 영향을 주지 않으므로 당장 제거하지 않아도 됩니다.
