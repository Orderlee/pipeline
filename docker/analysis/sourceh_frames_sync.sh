#!/usr/bin/env bash
# source-h 프레임 데이터 증분 동기화 — 업로드가 더 진행된 뒤 **이 한 줄만** 다시 실행하면 된다.
#
#   ./docker/analysis/sourceh_frames_sync.sh            # scan → copy → embed (증분)
#   ./docker/analysis/sourceh_frames_sync.sh --build    # + score → build → report (클래스가 갖춰진 뒤)
#   COPY_LIMIT=1500 ./docker/analysis/sourceh_frames_sync.sh   # 배치 크기 제한 (업로드 중 부하 완화)
#
# 왜 컨테이너를 두 개 쓰나:
#   · scan/copy 는 **NAS 를 읽어야** 하는데 실행 중인 analysis 컨테이너엔 NAS 마운트가 없다
#     → NAS(:ro) + fiftyone 볼륨을 붙인 일회성 컨테이너로 처리 (compose 수정 = 컨테이너
#       recreate = 실행 중인 FiftyOne 앱 중단이라 피했다)
#   · embed/score/build 는 실행 중인 analysis 컨테이너에서 (embedding-service·mongo 연결 보유)
#
# 멱등: 원장(ledger.jsonl)·복사본 크기·embed.npz 키로 이미 처리한 것을 건너뛴다.
# NAS 원본은 **:ro** 로만 붙으므로 물리적으로 수정 불가.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAS="${SOURCEH_NAS_HOST:-/home/user/mou/nas_primary/source-h}"
IMG="${ANALYSIS_IMAGE:-datapipeline-analysis:latest}"
CONTAINER="${ANALYSIS_CONTAINER:-docker-analysis-1}"
SCRIPT=sourceh_frames_eval.py
LIMIT_ARG=()
[[ -n "${COPY_LIMIT:-}" ]] && LIMIT_ARG=(--limit "$COPY_LIMIT")

[[ -d "$NAS" ]] || { echo "NAS 경로 없음: $NAS" >&2; exit 1; }

oneshot() {  # NAS 를 읽어야 하는 스테이지
  docker run --rm --network pipeline-network \
    -v "$NAS:/nas/source-h:ro" \
    -v "$REPO/docker/data/fiftyone:/data/fiftyone" \
    -v "$REPO/docker/analysis:/ws:ro" \
    -e SOURCEH_NAS_ROOT=/nas/source-h \
    "$IMG" python3 "/ws/$SCRIPT" "$@"
}
inplace() {  # 실행 중인 analysis 컨테이너 (embedding-service/mongo 접근 필요)
  docker cp "$REPO/docker/analysis/$SCRIPT" "$CONTAINER:/workspace/" >/dev/null
  docker exec "$CONTAINER" python3 "/workspace/$SCRIPT" "$@"
}

echo "== NAS 현재 파일 수 =="
for d in normal falldown fire smoke helmet; do
  [[ -d "$NAS/$d" ]] && printf '   %-10s %s\n' "$d" "$(ls -1 "$NAS/$d" 2>/dev/null | wc -l)"
done

oneshot scan
oneshot copy "${LIMIT_ARG[@]}"
inplace embed "${LIMIT_ARG[@]}"

if [[ "${1:-}" == "--build" ]]; then
  inplace score
  inplace build
  inplace report
else
  echo
  echo "동기화 완료. 클래스(fire/smoke/helmet)가 갖춰진 뒤 --build 로 데이터셋까지 생성:"
  echo "   $0 --build"
fi

echo
echo "디스크: $(df -h / | tail -1 | awk '{print $4" 여유 ("$5" 사용)"}')"
