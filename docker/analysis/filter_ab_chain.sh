#!/usr/bin/env bash
# 필터 설계 A/B 자립 체인 — 세션·SSH 끊겨도 계속 돈다 (setsid 로 띄울 것).
# 1) 문장 배경통계 해시 기준 재구축 (정렬 붕괴 복구, ~30~60분)
# 2) 필터 A/B 6변형 (집계 MEAN/MAX · 중복 대칭cos/방향성containment · 제거 OR/AND)
# 무거운 작업은 **직렬**로만 돈다 — 공유 호스트에서 오늘 OOM 2회.
set -u
R=/data/fiftyone/frames_bank/report/sourcei_gt
HR=/home/user/work_p/Datapipeline-Data-data_pipeline/docker/data/fiftyone/frames_bank/report/sourcei_gt
ST=$HR/filter_ab.state
say(){ echo "[$(date '+%m-%d %H:%M:%S')] $*" >> "$HR/filter_ab_chain.log"; echo "$*" > "$ST"; }
# 리다이렉트는 컨테이너 안에서 — 로그가 root 소유라 호스트 쓰기는 거부된다
run(){ docker exec -e COS_THREADS=3 -e AB_NBOOT="${AB_NBOOT:-2000}" docker-analysis-1 \
         sh -c "cd /workspace && nice -n 19 python3 $1 >> $R/$2 2>&1"; }

say "1/2 문장 통계 재구축 시작"
if run rebuild_sent_stats.py rebuild_stats.log; then say "OK 통계 재구축"; else
  say "실패 통계 재구축 — 중단 (A/B 는 이 산출물이 없으면 무의미)"; exit 1; fi

say "2/2 필터 A/B 시작"
if run filter_ab.py filter_ab.log; then say "OK 완료 — card.md 확인"; else
  say "실패 A/B — checkpoint.jsonl 까지는 보존됨"; exit 1; fi
say "DONE"
