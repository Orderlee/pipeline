#!/usr/bin/env python3
"""`sourcei.pred_margin_v1080` 복구 — 구 명명(`_v080`) 잔재를 정리하다 값까지 지웠다.

`stage_attach` 의 정의를 그대로 쓴다: 클래스별 per-frame 최고 코사인 M 을 만들고
`top1 − top2`. `_Pruner.best_of` 는 top-K 사다리의 0번 칸(= 그 클래스 전체 최고)이라
`bank_topk_stream` 결과로 정확히 재현된다.

현행 명명(`_v1080`)으로만 쓴다 — 구 명명을 되살릴 이유가 없고, 소비자 resolver
(`prompt_scores_export.resolve`)가 두 명명을 모두 받는다.
"""
import os, sys, time
sys.path.insert(0, "/workspace")
os.environ.setdefault("BANK_A", "v1.0.8.0"); os.environ.setdefault("BANK_B", "v1.0.8.0")
import numpy as np
import prompt_geometry as pg

pg.set_profile("sourcei")
VER = "v1.0.8.0"; TAG = pg.vtag(VER)
APPLY = "--apply" in sys.argv
T0 = time.time()
keys, X, gt, src = pg.load_matched()
bank = pg.load_bank(VER)
vals, idxs = pg.bank_topk_stream(X, bank)
cs = sorted(vals)
M = np.stack([vals[c][:, 0] for c in cs], axis=1)
order = np.sort(M, axis=1)
margin = (order[:, -1] - order[:, -2]).astype(np.float32)
print(f"[{time.time()-T0:.0f}s] 프레임 {len(keys):,} · margin 중앙값 {np.median(margin):.4f} "
      f"· 범위 [{margin.min():.4f}, {margin.max():.4f}]")
if not APPLY:
    print("DRY-RUN — --apply 로 기록"); sys.exit(0)
import fiftyone as fo
ds = fo.load_dataset("sourcei")
ids = pg.key_to_ids(ds, keys)
pairs = [(ids[i], i) for i in range(len(ids)) if ids[i]]
print(f"매칭 {len(pairs):,}/{len(keys):,}")
pg.set_values_batched(ds, f"pred_margin_{TAG}", pairs, lambda i: float(margin[i]))
print(f"기록 pred_margin_{TAG}")
