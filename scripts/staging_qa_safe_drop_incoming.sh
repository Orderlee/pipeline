#!/usr/bin/env bash
# incoming 외부에 복사 완료 후 mv → 센서가 불완전 폴더를 잡지 않음
# 사용: bash scripts/staging_qa_safe_drop_incoming.sh tmp_data_2
set -euo pipefail
STAGING="${STAGING_ROOT:-staging}"
SRC_NAME="${1:?folder name under $STAGING e.g. tmp_data_2}"
# QA_DROP_SOURCE: 대용량 폴더 대신 동일 폴더명의 소형 스모크 디렉터리 절대경로
SRC="${QA_DROP_SOURCE:-$STAGING/$SRC_NAME}"
BUILD="$STAGING/_qa_incoming_build"
if [[ ! -d "$SRC" ]]; then
  echo "소스 없음: $SRC" >&2
  exit 1
fi
mkdir -p "$BUILD"
rm -rf "$BUILD/$SRC_NAME"
echo "[1/3] 복사 중: $SRC → $BUILD/$SRC_NAME (시간 소요 가능)"
mkdir -p "$BUILD/$SRC_NAME"
# NFS 등에서 cp -a 시간 보존 오류 회피
cp -r "$SRC"/* "$BUILD/$SRC_NAME/" 2>/dev/null || cp -r "$SRC"/. "$BUILD/$SRC_NAME/"
echo "[2/3] incoming 으로 이동 (atomic mv)"
rm -rf "$STAGING/incoming/$SRC_NAME"
mv "$BUILD/$SRC_NAME" "$STAGING/incoming/"
echo "[3/3] 완료. 약 30~60초 후 archive_pending 에 나타나는지 확인하세요."
