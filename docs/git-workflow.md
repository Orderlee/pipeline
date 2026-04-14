# Git 브랜치 전략 & 배포 구조 가이드

> TeamPIA/Datapipeline-Data-data_pipeline 기준

---

## 목표 구조

```
개인 포크/feature 브랜치  →  PR  →  TeamPIA/dev (staging)  →  PR  →  TeamPIA/main (production)
```

| 레이어 | 역할 | 누가 push |
|--------|------|-----------|
| 개인 포크 / feature 브랜치 | 개인 작업, 실험 | 각 팀원 |
| `TeamPIA/dev` | 팀 공유 + staging 검증 | PR 통해서만 |
| `TeamPIA/main` | production 배포 | `dev → main` PR 통해서만 |

---

## 작업 시나리오

### 시나리오 1. 일반 기능 개발

```bash
# 1. TeamPIA/dev 최신화
git fetch upstream
git checkout dev && git merge upstream/dev

# 2. feature 브랜치 생성 (내 포크에서)
git checkout -b feature/xxx

# 3. 작업 후 내 포크에 push
git push origin feature/xxx
```

```
PR: 내포크/feature/xxx → TeamPIA/dev
    → staging 자동 배포 → 검증
    → (릴리즈 타이밍) TeamPIA/dev → TeamPIA/main → production 자동 배포
```

**핵심:** feature 브랜치는 내 포크에서 만들고, 완성되면 `TeamPIA/dev`로만 PR.

---

### 시나리오 2. 긴급 버그 수정 (Hotfix)

```bash
# 1. main 기준으로 hotfix 브랜치 생성
git checkout -b hotfix/issue-name upstream/main

# 2. 수정 후 두 곳에 PR
```

```
PR ①: hotfix/issue-name → TeamPIA/main  (production 긴급 반영)
PR ②: hotfix/issue-name → TeamPIA/dev   (dev 동기화 — 빠뜨리면 다음 릴리즈 때 버그 부활)
```

**핵심:** hotfix는 `main` 기준으로 만들고, `main`과 `dev` 둘 다 PR.

---

### 시나리오 3. 두 팀원이 연관된 작업

> ywl이 ingest 모듈 리팩토링 중인데, 그 위에 새 기능 작업이 필요한 경우

```
1. ywl: Orderlee/feature/ingest-refactor → TeamPIA/dev PR 머지

2. 내 포크 dev 최신화 (ywl 작업 포함됨)
   git fetch upstream
   git merge upstream/dev

3. 이후 일반 시나리오와 동일
```

**핵심:** `TeamPIA/dev`가 팀원 간 코드 공유 지점. 포크끼리 직접 코드를 주고받지 않음.

---

### 시나리오 4. 릴리즈 (staging → production)

```
1. staging(TeamPIA/dev)에서 충분히 검증됨을 확인
2. PR: TeamPIA/dev → TeamPIA/main
   - PR description에 포함된 변경사항 목록 작성
   - 팀 리뷰 후 머지
3. TeamPIA/main 머지 → production 자동 배포
```

**핵심:** `dev → main` PR이 곧 릴리즈 결정. 이 PR이 팀 전체 변경 이력 문서가 됨.

---

### 시나리오 5. 실험적 작업

```bash
# 내 포크에서만 실험 브랜치 생성
git checkout -b experiment/xxx

# 결과가 좋으면 → TeamPIA/dev PR
# 결과가 나쁘면 → 브랜치 삭제, TeamPIA에 흔적 없음
```

**핵심:** 실험은 내 포크 안에서만. `TeamPIA/dev`를 오염시키지 않음.

---

### 의사결정 트리

```
새 작업 시작
    ├─ production 버그?  →  main 기준 hotfix  →  main + dev 둘 다 PR
    ├─ 일반 기능?        →  dev 최신화  →  feature 브랜치  →  TeamPIA/dev PR
    └─ 실험?             →  내 포크에서만  →  결과 좋으면 TeamPIA/dev PR

TeamPIA/dev 머지됨
    └─ staging 자동 배포  →  검증  →  릴리즈 타이밍에 dev→main PR  →  production 배포
```

---

## 현재 구조에서 이전 방법

### 현재 vs 목표

| | 현재 | 목표 |
|---|---|---|
| Production 배포 소스 | `Orderlee/main` | `TeamPIA/main` |
| Staging 배포 | 없음 (수동) | `TeamPIA/dev` push 시 자동 |
| CI Runner 등록 | Orderlee 포크에 등록 | TeamPIA repo에 등록 |
| `TeamPIA/main` 상태 | 94커밋 뒤처짐 | `dev`와 동기화 |

---

### Step 1. TeamPIA/main을 dev 기준으로 fast-forward

```bash
git fetch upstream
git checkout main
git merge upstream/dev --ff-only

# fast-forward 불가 시 (팀 동의 후)
git push upstream main --force-with-lease
```

---

### Step 2. Self-hosted Runner를 TeamPIA로 이전

GitHub UI:
```
TeamPIA org → Settings → Actions → Runners → New self-hosted runner
```

Production 서버에서:
```bash
./config.sh --url https://github.com/TeamPIA/Datapipeline-Data-data_pipeline \
            --token <새_토큰> \
            --labels production
```

> staging 서버가 별도로 있다면 `--labels staging`으로 추가 등록.

---

### Step 3. Production 배포 워크플로 확인

`TeamPIA/dev`의 `.github/workflows/deploy-production.yml` 트리거 확인:

```yaml
on:
  push:
    branches: [main]   # TeamPIA/main push 시 트리거 되는지 확인
```

---

### Step 4. Test 배포 워크플로 추가

`.github/workflows/deploy-test.yml` 신규 추가:

```yaml
name: Deploy to Test

on:
  push:
    branches: [dev]
    paths-ignore:
      - "docs/**"
      - "*.md"

jobs:
  deploy:
    runs-on: [self-hosted, linux, deploy, test]
    steps:
      - uses: actions/checkout@v4
      - name: Deploy test stack
        run: bash scripts/deploy/deploy-stack.sh
```

> test 서버 루트와 `docker/.env.test` 기준으로 세부 값 조정 필요.

---

### Step 5. 첫 정식 릴리즈 PR (dev → main)

```
PR: TeamPIA/dev → TeamPIA/main
    제목: "chore: production 배포 구조 이전 및 첫 정식 릴리즈"
```

이 PR 머지 시 `deploy-production.yml` 트리거 → **TeamPIA 기반 첫 production 배포** 완료.

---

### Step 6. Orderlee 포크 배포 비활성화

Step 5 정상 동작 확인 후:

```
Orderlee/Datapipeline-Data-data_pipeline
→ Settings → Actions → General → Disable Actions
```

또는 `deploy-production.yml` 파일 삭제.

---

### Step 7. 브랜치 보호 규칙 설정

```
TeamPIA repo → Settings → Branches → Add rule
```

| 브랜치 | 규칙 |
|--------|------|
| `main` | PR 필수, 최소 1명 리뷰 승인, direct push 금지 |
| `dev` | PR 필수, direct push 금지 (선택) |

---

### Step 8. 팀원 포크 upstream 재정비

```bash
git remote -v
# upstream이 TeamPIA를 가리켜야 함
# origin    https://github.com/<본인>/Datapipeline-Data-data_pipeline
# upstream  https://github.com/TeamPIA/Datapipeline-Data-data_pipeline

# upstream 없다면 추가
git remote add upstream https://github.com/TeamPIA/Datapipeline-Data-data_pipeline
```

---

### 이전 순서 요약

```
Step 1: TeamPIA/main fast-forward    ← git 작업, force push 주의
Step 2: Runner 이전                  ← production 서버 작업 필요
Step 3: 워크플로 파일 트리거 확인    ← branches: [main] 인지 확인
Step 4: Staging 워크플로 추가        ← 새 파일 작성
Step 5: dev → main 첫 PR 머지        ← 이 시점부터 TeamPIA가 production 담당
Step 6: Orderlee 배포 비활성화       ← Step 5 확인 후 진행
Step 7: 브랜치 보호 규칙             ← GitHub UI
Step 8: 팀원 포크 upstream 확인      ← 각 팀원 로컬
```

> **Step 2 (Runner 이전)와 Step 5 (첫 PR 머지)가 핵심 전환점.**
> 이 둘 사이에 production 배포 공백이 생기지 않도록 순서대로 진행.
