# Git 브랜치 전략 & 배포 구조 가이드

> TeamPIA/Datapipeline-Data-data_pipeline 기준

## 현재 기준

```text
개인 포크/feature 브랜치
  -> PR -> TeamPIA/dev (test)
  -> 검증 완료 후 PR -> TeamPIA/main (production)
```

| 브랜치 | 역할 | 배포 |
|---|---|---|
| 개인 포크 / feature | 개인 작업, 실험, 초안 | 없음 |
| `TeamPIA/dev` | 팀 통합 + test 검증 | test 자동 배포 |
| `TeamPIA/main` | production 릴리즈 | production 자동 배포 |

기본 원칙:

- `dev = test`
- `main = production`
- 기능 개발은 `dev`로 모으고, production 반영은 `dev -> main` PR로만 진행합니다.
- 과거 문서나 스크립트에 남아 있는 `staging` 표기는 대부분 현재의 `test` 환경을 뜻하는 legacy 표현입니다.

---

## 빠른 참조 (체크카드)

> Orderlee 포크에서 직접 배포 중일 때 5초 안에 찾을 정보. 기억이 안 나면 여기만 보세요.

### 일반 기능 배포 — 4단계

```bash
# 1) 커밋 + dev 푸시 → 스테이징 자동 배포
git push origin dev

# 2) 3–10분 대기 후 스테이징에서 검증
#    http://172.168.42.6:3031/  (Dagster UI)
#    + 관련 센서 tick·asset 재실행·MinIO 결과물 등 수동 확인

# 3) 프로덕션 반영
git checkout main && git merge --no-ff dev && git push origin main

# 4) 프로덕션 안정 확인 후, upstream(TeamPIA/dev)으로 기여
./tools/pr-to-upstream.sh          # 대화형 (y/N 확인)
# 또는 보낼 커밋만 확인: ./tools/pr-to-upstream.sh --dry-run
```

### 핫픽스 (프로덕션 긴급 수정) — 4단계

```bash
# 1) main에서 fix 브랜치 분기
git checkout -b fix/<이름> main

# 2) 수정 + 커밋 + 푸시 → GitHub에서 PR 생성
git push origin fix/<이름>
# PR → main → 머지 → 프로덕션 즉시 배포

# 3) (거의 무조건!) 백머지하여 dev로 되돌려놓기
git checkout dev && git merge main && git push origin dev
```

> ⚠️ **핫픽스 백머지 생략 금지** — 다음 정기 릴리즈(`dev → main`)에서 같은 버그가 되살아남.

### 자동/수동 구분

| 단계 | 자동 | 주체 |
|---|---|---|
| 커밋·푸시 | ❌ | 개발자 |
| 스테이징 배포 (deploy-test.yml) | ✅ | GitHub Actions |
| 스테이징 검증 | ❌ | 사람 |
| dev → main 머지 | ❌ | 개발자 |
| 프로덕션 배포 (deploy-production.yml) | ✅ | GitHub Actions |

### CI 트리거 규칙 (언제 CI가 돌고 안 돌고)

| 바뀐 파일 | CI 트리거? | 이미지 재빌드? |
|---|---|---|
| `*.md` (루트) / `docs/**` / `tests/**` / `.cursor/**` / `.agent/**` | ❌ (paths-ignore) | — |
| `src/vlm_pipeline/**` | ✅ | ❌ (rsync만, 빠름) |
| `Dockerfile` / `docker/app/**` / `configs/**` / `scripts/**` / `gcp/**` / `split_dataset/**` / `src/python/**` | ✅ | ✅ (느림) |
| `.env` / `.env.test` | git 미추적 — 트리거 불가. 호스트에서 직접 편집 + 해당 환경 `docker compose restart` |

### 확인 URL 모음

| 용도 | URL |
|---|---|
| GitHub Actions | https://github.com/Orderlee/Datapipeline-Data-data_pipeline/actions |
| PROD Dagster UI | http://172.168.42.6:3030/ |
| STAGING Dagster UI | http://172.168.42.6:3031/ |
| PROD MinIO Console | http://172.168.47.36:9001/ (S3 :9000) |
| STAGING MinIO Console | http://172.168.47.36:9003/ (S3 :9002) |

### 자주 하는 실수

- 스테이징 `src/`·`configs/`·`scripts/` 를 **호스트에서 직접 수정** → 다음 CI 배포의 `rsync --delete`로 소실됨. 반드시 git 커밋 경로로
- **핫픽스 백머지 누락** → 다음 정기 릴리즈에서 버그 재발
- **env 변경을 git에 넣으려다 실패** → `.env`/`.env.test`는 `.gitignore`에 등록되어 있음. 호스트에서만 편집
- **스테이징에서 디버깅 중 직접 파일 수정** → commit 안 하면 다음 배포로 사라짐
- **upstream 기여 빠뜨림** → Orderlee/main에만 쌓이고 TeamPIA/dev 뒤처짐. 4단계의 `tools/pr-to-upstream.sh` 반드시 실행

### CI 우회 수동 배포 (장애 시 전용)

CI가 내려앉았을 때 호스트에서 직접:

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

---

## 작업 시나리오

### 시나리오 1. 일반 기능 개발

```bash
git fetch upstream
git checkout dev
git merge upstream/dev

git checkout -b feature/xxx
git push origin feature/xxx
```

흐름:

```text
PR: 내 포크/feature/xxx -> TeamPIA/dev
  -> test 자동 배포
  -> test 검증
  -> 릴리즈 시 dev -> main PR
```

### 시나리오 2. 긴급 버그 수정

```bash
git fetch upstream
git checkout -b hotfix/issue-name upstream/main
```

흐름:

```text
PR 1: hotfix/issue-name -> TeamPIA/main
PR 2: hotfix/issue-name -> TeamPIA/dev
```

핵심:

- hotfix는 `main` 기준으로 만듭니다.
- `main`에만 넣고 `dev`를 빼먹으면 다음 릴리즈 때 버그가 되살아날 수 있습니다.

### 시나리오 3. 팀원 작업 위에서 이어서 개발

```bash
git fetch upstream
git checkout dev
git merge upstream/dev
git checkout -b feature/follow-up
```

핵심:

- 팀 간 공유 기준점은 항상 `TeamPIA/dev`입니다.
- 포크끼리 직접 브랜치를 이어붙이는 대신, 먼저 `dev`에 반영된 뒤 그 위에서 이어갑니다.

### 시나리오 4. 릴리즈

```text
1. TeamPIA/dev 에서 test 검증 완료
2. PR: TeamPIA/dev -> TeamPIA/main
3. 리뷰 후 merge
4. main 자동 배포
```

핵심:

- `dev -> main` PR이 곧 production 릴리즈 기록입니다.
- PR 설명에 포함 기능, 위험 요소, 검증 결과를 함께 남깁니다.

### 시나리오 5. 실험성 작업

```bash
git checkout -b experiment/xxx
git push origin experiment/xxx
```

핵심:

- 실험은 개인 포크에서만 합니다.
- 충분히 의미가 있을 때만 `TeamPIA/dev`로 PR을 엽니다.

## 의사결정 트리

```text
새 작업 시작
  ├─ production 긴급 버그인가?
  │    -> main 기준 hotfix
  │    -> main + dev 둘 다 PR
  ├─ 일반 기능인가?
  │    -> dev 최신화
  │    -> feature 브랜치
  │    -> TeamPIA/dev PR
  └─ 실험인가?
       -> 개인 포크에서만 진행
       -> 결과 좋으면 TeamPIA/dev PR
```

## 배포 기준

### Test 배포

- 트리거: `TeamPIA/dev` push
- 대상: test stack
- 기준 env: `docker/.env.test`
- 목적: 팀 통합 검증

### Production 배포

- 트리거: `TeamPIA/main` push
- 대상: production stack
- 기준 env: `docker/.env`
- 목적: 실제 운영 반영

## Self-hosted runner 기준

runner는 현재 branch 기반 배포를 같이 처리합니다.

권장 label:

```text
self-hosted, linux, deploy, production, test
```

`setup-runner.sh` 사용 예:

```bash
bash scripts/deploy/setup-runner.sh --token <token>
```

## 운영 체크포인트

### dev -> test

- `dev` merge 후 test workflow가 실행되는지 확인
- test Dagster health check가 통과하는지 확인
- test 데이터 plane(`/data/staging.duckdb`, `/nas/staging/...`)만 건드렸는지 확인

### main -> production

- `main` merge 후 production workflow가 실행되는지 확인
- production Dagster health check가 통과하는지 확인
- production 데이터 plane(`/data/pipeline.duckdb`, `/nas/...`)만 건드렸는지 확인

## 팀 규칙

| 브랜치 | 규칙 |
|---|---|
| `main` | PR 필수, 리뷰 후 merge, direct push 금지 |
| `dev` | PR 권장 또는 필수, direct push 최소화 |

## 호환성 메모

- `/nas/staging`, `/data/staging.duckdb`, `STAGING_ROOT` 같은 이름은 아직 일부 스크립트와 데이터 plane에서 legacy 호환용으로 남아 있습니다.
- 하지만 브랜치/배포 의미는 이미 `staging`이 아니라 `test`입니다.
- 새 스크립트와 새 문서에서는 가능한 한 `test` 용어를 사용합니다.
