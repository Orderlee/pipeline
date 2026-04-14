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
