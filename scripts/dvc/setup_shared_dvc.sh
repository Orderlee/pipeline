#!/usr/bin/env bash
# 다중 엔지니어 공유 DVC 환경 부트스트랩 — 운영자 1회 실행 (sudo 필요, CI 자동 X).
#
#   sudo DVC_ENGINEERS="user eng-a eng-b" bash scripts/dvc/setup_shared_dvc.sh
#
# 만드는 것 (전부 멱등):
#   1) 전용 그룹 dvc-data + 멤버(DVC_ENGINEERS) 등록            ← bare repo 쓰기 권한
#   2) /srv/data-repos/dvc-datasets.git  group-shared bare repo  ← .dvc 포인터 공유 origin
#      (앱 배포 rsync --delete + git reset 와 격리된 /srv 하위)
#   3) post-receive 훅 설치 (push 통지 — 수신부 없으면 fail-soft no-op)
#   4) 공유 dvc[s3] venv (/srv/data-repos/dvc-venv) — 엔지니어가 dvc 직접 설치 안 해도 됨
#   5) 그룹 권한/ setgid 고정
#
# 바이트 remote = MinIO vlm-dataset/_dvc/ (이미 존재, 5-버킷 정책). 자격은 커밋하지 않고
# 각 엔지니어가 clone 후 .dvc/config.local (git 미추적) 에 공유 MinIO 키를 넣는다.
set -euo pipefail

[ "$(id -u)" -eq 0 ] || { echo "[dvc-shared] sudo 로 실행하세요: sudo bash $0"; exit 1; }

ENGINEERS="${DVC_ENGINEERS:-user eng-a eng-b}"
GROUP="${DVC_GROUP:-dvc-data}"
REPO_DIR="${DVC_DATA_REPO_PATH:-/srv/data-repos/dvc-datasets.git}"
PARENT="$(dirname "$REPO_DIR")"
HERE="$(cd "$(dirname "$0")" && pwd)"
HOOK_SRC="$HERE/post-receive"
VENV_DIR="${DVC_VENV_DIR:-$PARENT/dvc-venv}"

echo "[dvc-shared] group=$GROUP engineers='$ENGINEERS' repo=$REPO_DIR"

# 1) 전용 그룹 + 멤버
groupadd -f "$GROUP"
for u in $ENGINEERS; do
    if id "$u" >/dev/null 2>&1; then usermod -aG "$GROUP" "$u"; echo "  + $u → $GROUP"; else echo "  ! skip (no such user): $u"; fi
done

# 2) group-shared bare repo + 훅
mkdir -p "$PARENT"
if [ ! -d "$REPO_DIR" ]; then
    git init --bare --shared=group "$REPO_DIR"
    echo "  + created bare repo (shared=group): $REPO_DIR"
else
    echo "  = bare repo exists: $REPO_DIR"
fi
if [ -f "$HOOK_SRC" ]; then install -m 0775 "$HOOK_SRC" "$REPO_DIR/hooks/post-receive"; echo "  + post-receive hook"; fi

# git 'dubious ownership' 회피(필수): root 소유 bare repo 를 비-root 엔지니어(user/eng-a/eng-b)+훅의 git 이
# 신뢰하도록 system-wide 등록. 없으면 clone/push/훅의 git diff 가 거부돼 ingest 가 조용히 안 됨.
git config --system --get-all safe.directory 2>/dev/null | grep -qxF "$REPO_DIR" \
    || git config --system --add safe.directory "$REPO_DIR"
echo "  + git safe.directory (system): $REPO_DIR"

# 3) 공유 venv: dvc[s3] (엔지니어 push/pull) + psycopg2-binary (post-receive 훅의 카탈로그 INSERT)
if [ ! -x "$VENV_DIR/bin/dvc" ]; then
    # boto3 명시(Codex BUG1): dvc[s3] 는 dvc-s3→s3fs→aiobotocore 만 끌어옴, boto3 미포함 →
    # 훅의 _object_exists 가 ImportError→pending. psycopg2 = 훅의 카탈로그 INSERT.
    if python3 -m venv "$VENV_DIR" 2>/dev/null && \
       "$VENV_DIR/bin/pip" install -q --upgrade pip 'dvc[s3]>=3.51,<4' psycopg2-binary boto3; then
        echo "  + shared venv (dvc[s3]+psycopg2+boto3): $VENV_DIR/bin/dvc"
    else
        echo "  ! venv 설치 실패 (network?) — 재시도 필요"
    fi
else
    "$VENV_DIR/bin/python" -c 'import psycopg2, boto3' 2>/dev/null \
        || "$VENV_DIR/bin/pip" install -q psycopg2-binary boto3 || true
    echo "  = venv exists: $VENV_DIR"
fi

# 3b) post-receive 훅용 설정 파일 — DSN(컨테이너 호스트명 → 호스트 published 포트로 변환) + MinIO 자격.
#     자격을 훅/ git 에 안 박고 root 소유 0640 파일 하나로 (그룹 읽기). 훅이 이걸 source.
ENV_FILE="$(cd "$HERE/../../docker" && pwd)/.env"
INGEST_ENV="$PARENT/dvc-ingest.env"
PIPE_DSN="$(grep -E '^DATAOPS_POSTGRES_DSN=' "$ENV_FILE" | cut -d= -f2- || true)"
PIPE_DSN="${PIPE_DSN#\"}"; PIPE_DSN="${PIPE_DSN%\"}"   # .env 가 "..." 로 감싼 경우 제거 (Codex BUG3)
PIPE_DSN="${PIPE_DSN#\'}"; PIPE_DSN="${PIPE_DSN%\'}"   # '...' 도
# @host:port/ 앵커 치환(Codex BUG4): 비밀번호에 동일 substring 있어도 안 건드림. 컨테이너 호스트명 → 호스트 published 포트.
HOST_DSN="${PIPE_DSN//@docker-postgres-1:5432\//@localhost:15433/}"
{
    echo "# post-receive 훅 전용 (root 0640). git 미추적. setup_shared_dvc.sh 생성."
    echo "DATAOPS_POSTGRES_DSN='$HOST_DSN'"
    grep -E '^(MINIO_ENDPOINT|MINIO_ACCESS_KEY|MINIO_SECRET_KEY)=' "$ENV_FILE" || true
    echo "DVC_INGEST_VENV='$VENV_DIR'"
    echo "DVC_INGEST_SCRIPT='$HERE/ingest_to_catalog.py'"
    echo "DVC_DATA_REPO_ID='${DVC_DATA_REPO_ID:-dvc-datasets}'"
} > "$INGEST_ENV"
chgrp "$GROUP" "$INGEST_ENV"; chmod 0640 "$INGEST_ENV"
echo "  + hook env: $INGEST_ENV (DSN→localhost:15433, MinIO 자격)"

# 4) 그룹 소유/권한 고정 (setgid 로 새 객체가 그룹 상속)
chgrp -R "$GROUP" "$PARENT"
chmod -R g+rwX "$PARENT"
find "$PARENT" -type d -exec chmod g+s {} +

cat <<EOF

[dvc-shared] DONE.
⚠️ 그룹 멤버십은 '재로그인' 후 적용됩니다 (각 엔지니어 새 SSH 세션 또는 'newgrp $GROUP').

다음 단계(시드 .dvc/config + 검증)는 Claude 가 'sg $GROUP' 로 재로그인 없이 마무리할 수 있습니다.
엔지니어 사용법(시드 후):
  source $VENV_DIR/bin/activate            # 또는 자기 dvc 사용
  git clone $REPO_DIR && cd dvc-datasets
  dvc remote modify --local minio access_key_id  <MINIO_ACCESS_KEY>
  dvc remote modify --local minio secret_access_key <MINIO_SECRET_KEY>
  dvc add data/<dataset> && dvc push && git add . && git commit -m "curate: ..." && git push
  # 받는 쪽:  git pull && dvc pull
EOF
