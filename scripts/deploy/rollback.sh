#!/usr/bin/env bash
# =====================================================
# 운영 배포 롤백 스크립트
# 사용법: bash scripts/deploy/rollback.sh [IMAGE_TAG]
# 예시:   bash scripts/deploy/rollback.sh datapipeline:abc12345
#
# IMAGE_TAG 의 태그부는 CI 가 GITHUB_SHA 앞 8자로 붙인 값이라 곧 commit SHA 다
# (deploy-stack.sh 의 IMAGE_TAG). 그래서 이미지와 git tree 를 같은 인자로 되돌릴 수 있다.
# =====================================================
set -euo pipefail

# 전체 로직을 main() 안에 둔다. bash 는 스크립트를 지연 읽기하므로, 아래 git reset 이
# 실행 중인 이 파일 자신을 덮어쓰면 남은 줄을 엉뚱하게 읽는다. 함수는 호출 전에
# 통째로 파싱되므로 그 위험이 사라진다 (마지막 줄의 `main "$@"` 참고).
main() {
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE="${SCRIPT_DIR}/../compose-prod.sh"
IMAGE_TAG="${1:-}"

if [ -z "${IMAGE_TAG}" ]; then
    echo "사용 가능한 이미지 태그:"
    docker images --format '{{.Repository}}:{{.Tag}}  {{.CreatedAt}}' \
        | grep "^datapipeline:" \
        | head -10
    echo ""
    echo "사용법: $0 <IMAGE_TAG>"
    echo "예시:   $0 datapipeline:abc12345"
    exit 1
fi

echo "============================================="
echo " 롤백: ${IMAGE_TAG}"
echo "============================================="

docker tag "${IMAGE_TAG}" datapipeline:gpu-cu124
TARGET_IMAGE_ID="$(docker image inspect -f '{{.Id}}' datapipeline:gpu-cu124)"

# [1/4] 호스트 git tree 도 같이 되돌린다 — CLAUDE.md 의
# "호스트 git HEAD == 컨테이너 이미지 안 src == 실행 코드" 단일 진리 불변식 유지.
# 이미지만 되돌리면 운영자가 `git log` 로 실행 중인 코드를 오판한다.
# docker-compose.yaml 도 tracked 라 함께 되돌아가 compose 설정까지 일관되게 롤백된다.
echo "[1/4] git tree 정렬..."
GIT_ROLLED_BACK=false
TARGET_SHA="${IMAGE_TAG##*:}"
if ! git -C "${REPO_ROOT}" cat-file -e "${TARGET_SHA}^{commit}" 2>/dev/null; then
    echo "  [!] '${TARGET_SHA}' 는 commit 이 아님 — git tree 는 유지하고 컨테이너만 롤백한다."
elif [ -n "$(git -C "${REPO_ROOT}" status --porcelain --untracked-files=no)" ]; then
    # reset --hard 는 커밋 안 된 tracked 변경을 파괴한다. 장애 대응 중 남의 작업을
    # 날리는 게 stale git HEAD 보다 나쁘므로 건너뛴다 (untracked 는 애초에 영향 없음).
    echo "  [!] 커밋 안 된 tracked 변경이 있어 git reset 을 건너뛴다:"
    git -C "${REPO_ROOT}" status --short --untracked-files=no | sed 's/^/      /'
    echo "      → 컨테이너만 롤백된다. 정리 후 수동으로: git -C ${REPO_ROOT} reset --hard ${TARGET_SHA}"
else
    git -C "${REPO_ROOT}" reset --hard "${TARGET_SHA}"
    GIT_ROLLED_BACK=true
    echo "  git tree → $(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
fi

# [2/4] --force-recreate 필수.
# `docker compose up -d` 는 image SHA 변경을 항상 감지하지 못한다 (같은 tag 라 unchanged 로 판단)
# — deploy-stack.sh 의 sam3 force-recreate 주석과 같은 이유. 배포 경로는 stop+rm 으로 회피하는데
# 롤백에 그 단계가 없어서, 빼면 컨테이너가 옛 이미지 그대로 돌면서 "롤백 완료"만 출력한다.
echo "[2/4] code-server 재생성..."
"${COMPOSE}" up -d --no-deps --force-recreate dagster-code-server
sleep 15

echo "[3/4] daemon + webserver 재생성..."
"${COMPOSE}" up -d --no-deps --force-recreate dagster-daemon
"${COMPOSE}" up -d --no-deps --force-recreate dagster

# [4/4] 이미지 교체를 실제로 검증한다.
# HEALTHCHECK 만으로는 롤백 성공을 판정할 수 없다 — 되돌리려던 그 나쁜 코드도
# /server_info 에는 정상 응답한다. 컨테이너가 target image 를 물었는지 직접 확인.
echo "[4/4] 이미지 교체 검증..."
for svc in dagster-code-server dagster-daemon dagster; do
    cid="$("${COMPOSE}" ps -q "${svc}" 2>/dev/null || true)"
    if [ -z "${cid}" ]; then
        echo "::error::${svc} 컨테이너가 없음 — 롤백 실패"
        exit 1
    fi
    running_image="$(docker inspect -f '{{.Image}}' "${cid}")"
    if [ "${running_image}" != "${TARGET_IMAGE_ID}" ]; then
        echo "::error::${svc} 가 target image 를 물지 않음 (실행중=${running_image:7:12}, 기대=${TARGET_IMAGE_ID:7:12})"
        exit 1
    fi
    echo "  ${svc} → ${TARGET_IMAGE_ID:7:12} OK"
done

echo "Health check..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:3030/server_info > /dev/null 2>&1; then
        echo "롤백 완료 — Dagster 정상 응답 (${i}초)"
        if [ "${GIT_ROLLED_BACK}" = false ]; then
            echo "[!] git tree 는 되돌리지 않았다 — 호스트 git HEAD 와 실행 코드가 불일치 상태."
        fi
        echo "[!] origin 에는 아직 문제 커밋이 남아 있다. 다음 배포가 재적용하지 않도록"
        echo "    git revert + push 로 후속 조치할 것."
        exit 0
    fi
    sleep 2
done

echo "[!] Dagster가 60초 내 응답하지 않음 — 수동 확인 필요"
exit 1
}

main "$@"
