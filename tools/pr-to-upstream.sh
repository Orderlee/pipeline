#!/usr/bin/env bash
# Orderlee/main → TeamPIA/dev PR 생성 헬퍼.
#
# 전제:
#   - 현재 repo가 Orderlee 포크이며 원격 'upstream' = TeamPIA/Datapipeline-Data-data_pipeline
#   - gh CLI 인증됨 (gh auth status → 'repo' scope 포함)
#   - Orderlee/main 은 프로덕션(3030)에서 검증 완료된 상태
#
# 사용:
#   tools/pr-to-upstream.sh          # 실제 PR 생성
#   tools/pr-to-upstream.sh --dry-run  # 보낼 커밋만 확인

set -euo pipefail

UPSTREAM_REPO="TeamPIA/Datapipeline-Data-data_pipeline"
SRC_OWNER="Orderlee"
SRC_BRANCH="main"
SRC_HEAD="${SRC_OWNER}:${SRC_BRANCH}"   # gh pr create 용
DEST_BASE="dev"

DRY_RUN=0
[ "${1:-}" = "--dry-run" ] && DRY_RUN=1

cd "$(git rev-parse --show-toplevel)"

# 1. 원격 정의 검증
if ! git remote get-url upstream >/dev/null 2>&1; then
  echo "❌ 'upstream' 원격이 없습니다. 다음 명령으로 추가하세요:" >&2
  echo "   git remote add upstream https://github.com/$UPSTREAM_REPO.git" >&2
  exit 1
fi

# 2. gh 인증 확인
if ! gh auth status >/dev/null 2>&1; then
  echo "❌ gh CLI 인증이 필요합니다. 'gh auth login' 실행 후 재시도하세요." >&2
  exit 1
fi

# 3. 최신 upstream 가져오기
echo "=== upstream 최신화 ==="
git fetch upstream --quiet

# 4. 보낼 커밋 계산
echo
echo "=== upstream/$DEST_BASE 로 보낼 커밋 (upstream/$DEST_BASE..origin/main) ==="
COMMITS=$(git log --oneline "upstream/$DEST_BASE..origin/main")
if [ -z "$COMMITS" ]; then
  echo "(없음) — upstream/$DEST_BASE 와 origin/main 이 동일합니다. PR 불필요."
  exit 0
fi
COUNT=$(echo "$COMMITS" | wc -l | tr -d ' ')
echo "$COMMITS"
echo
echo "총 ${COUNT} 커밋"

# 5. 기존 열린 PR 검사 (중복 방지)
# gh pr list --head "owner:branch" 는 cross-fork 경우 빈 결과 반환.
# --head <branch> 만으로 조회 후 headRepositoryOwner.login 필터링.
EXISTING_URL=$(gh pr list \
  --repo "$UPSTREAM_REPO" \
  --head "$SRC_BRANCH" \
  --base "$DEST_BASE" \
  --state open \
  --json url,headRepositoryOwner \
  --jq ".[] | select(.headRepositoryOwner.login == \"$SRC_OWNER\") | .url" \
  2>/dev/null | head -1 || true)

if [ -n "$EXISTING_URL" ] && [ "$EXISTING_URL" != "null" ]; then
  echo
  echo "ℹ️ 이미 열린 PR이 있습니다: $EXISTING_URL"
  echo "   origin/$SRC_BRANCH 에 새 커밋이 있으면 위 PR이 자동으로 갱신됩니다."
  echo "   새 PR을 원하면 먼저 위 PR을 머지 또는 닫으세요."
  exit 0
fi

# 6. Dry-run이면 여기서 종료
if [ "$DRY_RUN" = "1" ]; then
  echo
  echo "(--dry-run) 실제 PR 생성 안 함."
  exit 0
fi

# 7. 사용자 확인
echo
read -rp "upstream 으로 PR을 생성하시겠습니까? (y/N) " ANS
case "$ANS" in
  y|Y) ;;
  *) echo "취소됨"; exit 0 ;;
esac

# 8. PR 생성
TITLE="sync: Orderlee/main → TeamPIA/dev ($(date +%Y-%m-%d)) — ${COUNT} commits"
BODY=$(cat <<EOF
Orderlee 포크 프로덕션(3030)에서 검증 완료된 변경사항을 upstream에 반영합니다.

## 포함 커밋 (${COUNT})

\`\`\`
${COMMITS}
\`\`\`

## 배포 검증
- PROD deploy-production.yml 성공
- STAGING deploy-test.yml 성공 (dev 단계에서 선행 검증)
- Dagster code location LOADED 확인
EOF
)

echo
echo "=== PR 생성 ==="
gh pr create \
  --repo "$UPSTREAM_REPO" \
  --head "$SRC_HEAD" \
  --base "$DEST_BASE" \
  --title "$TITLE" \
  --body "$BODY"
