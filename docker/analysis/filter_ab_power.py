#!/usr/bin/env python3
"""검정력·동등성 — "유의하지 않음"이 **효과 없음인지 검정력 부족인지** 가른다.

짝비교에서 5변형 모두 유의하지 않게 나왔다. 그 자체로는 두 가지를 구분하지 못한다:
  (a) 진짜 차이가 없다        (b) 차이가 있어도 잡을 표본이 없다
그래서 셋을 낸다.
  · MDE(최소검출효과) — 관측 SD·n 에서 80% 검정력으로 잡히는 가장 작은 Δ
  · 비열등성 검정 — 마진 −0.02(§18 규약)에서 Δ 의 하한이 마진을 넘는가
  · TOST 동등성 — ±마진 안에 있다고 **말할 수 있는가** (양방향)
비열등성/동등성은 "유의하지 않다"보다 강한 주장이므로 결론을 여기서 낸다.
"""
import os, sys, json
sys.path.insert(0, "/workspace")
import numpy as np
from scipy import stats as sps

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
AB = f"{OUT}/filter_ab"
MARGIN = float(os.environ.get("AB_MARGIN", "0.02"))
D = json.load(open(f"{AB}/inference.json"))
CT = D["camera_table"]
BASE = "base"

# macro 가 정의된 카메라만 (G2 — 이벤트 클래스 부재 카메라는 macro 없음)
def vec(name, col=2):
    return np.array([r[col] for r in CT[name]], dtype=object)
ok = [i for i, r in enumerate(CT[BASE]) if r[2] is not None]
def v(name, col=2): return np.array([float(CT[name][i][col]) for i in ok])

print(f"카메라 {len(CT[BASE])} 중 macro 정의 {len(ok)} · 비열등성 마진 {MARGIN}")
rows = []
for name in CT:
    if name == BASE: continue
    dv = v(name) - v(BASE)
    n = len(dv); m = dv.mean(); sd = dv.std(ddof=1); se = sd / np.sqrt(n)
    tcrit = sps.t.ppf(.975, n - 1)
    # MDE: 80% 검정력, 양측 α=.05
    mde = (sps.t.ppf(.975, n - 1) + sps.t.ppf(.80, n - 1)) * se
    # 비열등성: H0 Δ ≤ −margin, 단측
    t_ni = (m + MARGIN) / se if se > 0 else np.inf
    p_ni = float(sps.t.sf(t_ni, n - 1)) if se > 0 else 0.0
    # TOST 동등성: 양쪽 단측
    p_lo = float(sps.t.sf((m + MARGIN) / se, n - 1)) if se > 0 else 0.0
    p_hi = float(sps.t.sf((MARGIN - m) / se, n - 1)) if se > 0 else 0.0
    p_tost = max(p_lo, p_hi)
    lo, hi = m - tcrit * se, m + tcrit * se
    rows.append(dict(variant=name, n_cam=n, delta=round(float(m), 5), sd=round(float(sd), 5),
                     ci95=[round(float(lo), 5), round(float(hi), 5)],
                     mde80=round(float(mde), 5),
                     noninferior=bool(lo > -MARGIN), p_noninferior=round(p_ni, 4),
                     equivalent=bool(p_tost < .05), p_tost=round(p_tost, 4),
                     verdict=("비열등(마진 안)" if lo > -MARGIN else
                              "열등 가능(하한이 마진 밖)")))
    print(f"{name:15} Δ {m:+.4f} SD {sd:.4f} · CI [{lo:+.4f},{hi:+.4f}] · MDE80 {mde:.4f} "
          f"· 비열등 {'O' if lo>-MARGIN else 'X'}(p {p_ni:.3f}) · 동등 {'O' if p_tost<.05 else 'X'}(p {p_tost:.3f})")

# 관측 Δ 규모 대비 검정력이 얼마나 부족한가 — 기준선 SD 로 필요 카메라 역산
base_sd = float(np.mean([r["sd"] for r in rows if r["sd"] > 0]))
need = {}
for eff in (0.005, 0.01, 0.02, 0.05):
    nn = 2
    while nn < 5000:
        se = base_sd / np.sqrt(nn)
        if (sps.t.ppf(.975, nn - 1) + sps.t.ppf(.80, nn - 1)) * se <= eff: break
        nn += 1
    need[eff] = nn
print(f"\n효과크기별 필요 카메라 수 (80% 검정력, 평균 SD {base_sd:.4f}):")
for k, nv in need.items(): print(f"   Δ={k:.3f} → 카메라 {nv}대")
json.dump(dict(margin=MARGIN, n_cam_macro=len(ok), mean_sd=round(base_sd, 5),
               variants=rows, cameras_needed=need),
          open(f"{AB}/power.json", "w"), ensure_ascii=False, indent=1)
print(f"\n→ {AB}/power.json")
