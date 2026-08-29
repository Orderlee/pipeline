#!/usr/bin/env bash
# 재투영 자립 체인 — 세션·SSH 가 끊겨도 계속 돈다 (setsid + nohup 으로 띄울 것).
# full 로 시도하고 OOM-kill 되면 landmark 로 내려간다. 둘 다 실패하면 **좌표를 원복**해
# 공유 패널이 붕괴본으로 남지 않게 한다. 진행 상태는 STATE 파일 한 줄로 읽는다.
set -u
R=/data/fiftyone/frames_bank/report/sourcei_gt
HR=/home/user/work_p/Datapipeline-Data-data_pipeline/docker/data/fiftyone/frames_bank/report/sourcei_gt
STATE=$HR/chain.state
GOOD=$R/optbank/emb_viz_backup_20260828_031024.npz   # 재투영 이전(kNN5 확장) 정상 좌표
say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$HR/chain.log"; echo "$*" > "$STATE"; }

# ⚠️ 리다이렉트는 **컨테이너 안**에서 한다. 로그 파일이 root 소유라 호스트(user)가
#    ">> $HR/reproject.log" 를 열면 "허가 거부"로 즉시 실패한다(2026-08-28 실측).
run(){ docker exec -e RP_MODE="$1" -e RP_NN="$2" -e COS_THREADS=3 docker-analysis-1 \
         sh -c "cd /workspace && nice -n 19 python3 reproject_prompts.py --apply >> $R/reproject_$1.log 2>&1"; }

say "full 모드 시작"
if run full 10; then say "OK full"; exit 0; fi
say "full 실패(OOM 추정) — landmark 모드로 재시도"
if run landmark 10; then say "OK landmark"; exit 0; fi

say "둘 다 실패 — 좌표 원복"
docker exec docker-analysis-1 python3 -c "
import numpy as np, fiftyone as fo, fiftyone.brain as fob
z=np.load('$GOOD'); ds=fo.load_dataset('sourcei-prompts')
ids=[str(i) for i in z['sample_ids']]; P=z['points']
cur=ds.values('id')
m={i:P[k] for k,i in enumerate(ids)}
pts=np.stack([m.get(i, np.array([np.nan,np.nan],'float32')) for i in cur])
ds.delete_brain_run('emb_viz'); fob.compute_visualization(ds, points=pts, brain_key='emb_viz')
print('restored', len(pts))
" >> "$HR/chain.log" 2>&1 && say "원복 완료" || say "원복 실패 — 수동 확인 필요"
