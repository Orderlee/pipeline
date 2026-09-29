#!/usr/bin/env bash
# probe 캐시를 **전 뱅크 버전**에 굽는다 — App 「문장 생성」·「프롬프트 프로브」 드롭다운이
# `dataset.info` 의 `probe_bank_<tag>` 로 채워지므로, 캐시가 없는 버전은 아예 고를 수 없다.
# (2026-08-28: 2뱅크만 구워져 있어 드롭다운에 v1.0.8.0·v1.0.8.4 만 떴다.)
# 뱅크당 ~11초. 31뱅크 ≈ 6분. gidx 파생 필드는 건드리지 않는다(probe_* 만 쓴다).
set -u
R=/data/fiftyone/frames_bank/report/sourcei_gt
LOG=$R/probecache_all.log
VERS=$(docker exec docker-analysis-1 python3 -c "
import os,re
d='/data/fiftyone/sourceh/prompts'
print(','.join(sorted(re.sub(r'\.npz$','',f) for f in os.listdir(d) if f.endswith('.npz'))))")
echo "대상: $VERS" | tee -a "$LOG"
IFS=',' read -ra A <<< "$VERS"
i=0
for v in "${A[@]}"; do
  i=$((i+1))
  echo "[$i/${#A[@]}] $v" | tee -a "$LOG"
  docker exec -e BANK_ATTACH="$v" -e COS_THREADS=3 docker-analysis-1 \
    sh -c "cd /workspace && nice -n 19 python3 prompt_geometry.py probecache --profile sourcei \
           >> $R/probecache_all.log 2>&1" || echo "  실패 $v" | tee -a "$LOG"
done
# 「전체」 = 전 뱅크 합집합. 라벨이 '전체'인데 2뱅크만 담고 있었으므로 같이 갱신한다.
# ⚠️ 합집합은 별개 뱅크다 — 후보 문장을 넓은 모수에서 보는 용도로만, 제품 성능으로 인용 금지.
echo "[all] 합집합 ${#A[@]}뱅크" | tee -a "$LOG"
docker exec -e BANK_ATTACH=all -e BANK_LIST="$VERS" -e COS_THREADS=3 docker-analysis-1 \
  sh -c "cd /workspace && nice -n 19 python3 prompt_geometry.py probecache --profile sourcei \
         >> $R/probecache_all.log 2>&1" || echo "  실패 all" | tee -a "$LOG"
echo "DONE" | tee -a "$LOG"
