#!/usr/bin/env python3
"""sourcei GT **출처 감사** — 무엇이 사람 근거이고 무엇이 모델 파생인가.

`sourcei_build.py kind_of()` 는 근거 강도 순서(folder > filename > caption)를 값으로 남긴다.
그중 `caption` 은 **Gemini 파생 = 모델 라벨**이다(CLAUDE.md 자기학습 금지 대상).
지금까지의 모든 지표가 이 혼합 원장 위에서 계산됐으므로, 어느 지표가 어느 출처에
기대고 있는지 분해해야 해석이 성립한다.
"""
import json, collections
import numpy as np

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CL = ["normal", "falldown", "fire", "smoke"]
HUMAN = {"folder", "filename"}          # 사람이 정리한 디렉토리/파일명 근거
MODEL = {"caption"}                      # Gemini 파생

d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gs = np.array([str(x) for x in d["gt_source"]])
gt, cam = np.array(d["gt"]), np.array([str(x) for x in d["camera"]])
N = len(gt)

print(f"프레임 {N:,} · 카메라 {len(set(cam))}\n")
print("① 클래스별 출처 구성 (사람근거 = folder+filename)")
print(f"{'클래스':10} {'전체':>7} {'folder':>7} {'filename':>9} {'caption':>8} {'none':>6} {'사람근거%':>9}")
rows = []
for i, c in enumerate(CL):
    m = gt == i
    cnt = collections.Counter(gs[m])
    h = cnt.get("folder", 0) + cnt.get("filename", 0)
    pct = 100 * h / max(m.sum(), 1)
    print(f"{c:10} {m.sum():>7,} {cnt.get('folder',0):>7,} {cnt.get('filename',0):>9,} "
          f"{cnt.get('caption',0):>8,} {cnt.get('none',0):>6,} {pct:>8.1f}%")
    rows.append(dict(cls=c, n=int(m.sum()), folder=cnt.get("folder", 0),
                     filename=cnt.get("filename", 0), caption=cnt.get("caption", 0),
                     none=cnt.get("none", 0), human_pct=round(pct, 1)))
tot_h = int(np.isin(gs, list(HUMAN)).sum())
print(f"\n전체 사람근거 {tot_h:,}/{N:,} ({100*tot_h/N:.1f}%) · 모델파생(caption) "
      f"{int(np.isin(gs,list(MODEL)).sum()):,} ({100*np.isin(gs,list(MODEL)).mean():.1f}%)")

print("\n② 카메라 × (출처 · 클래스) — 교락 구조")
print(f"{'카메라':42} {'n':>6} {'사람%':>6} {'이벤트클래스':>14} {'macro가능':>9}")
cams = sorted(set(cam)); ctab = []
for c in cams:
    m = cam == c
    hp = 100 * np.isin(gs[m], list(HUMAN)).mean()
    ev = sorted({CL[i] for i in gt[m] if i > 0})
    ok = len(ev) > 0
    print(f"{c[:40]:42} {m.sum():>6,} {hp:>5.0f}% {','.join(ev) or '-':>14} {'O' if ok else 'X':>9}")
    ctab.append(dict(camera=c, n=int(m.sum()), human_pct=round(hp, 1),
                     events=ev, macro_ok=bool(ok)))

n_ok = sum(1 for r in ctab if r["macro_ok"])
hp_ok = np.mean([r["human_pct"] for r in ctab if r["macro_ok"]])
hp_no = np.mean([r["human_pct"] for r in ctab if not r["macro_ok"]])
print(f"\nmacro 가능 카메라 {n_ok}/{len(cams)} · 그 카메라 평균 사람근거 {hp_ok:.1f}% "
      f"vs 제외 카메라 {hp_no:.1f}%")

print("\n③ 지표별로 어느 출처에 기대는가")
ev_m = gt > 0
fp_m = gt == 0
print(f"   이벤트 F1 분모(이벤트 프레임 {int(ev_m.sum()):,}) → 사람근거 "
      f"{100*np.isin(gs[ev_m],list(HUMAN)).mean():.1f}%")
print(f"   normal 오탐 분모(normal 프레임 {int(fp_m.sum()):,}) → 사람근거 "
      f"{100*np.isin(gs[fp_m],list(HUMAN)).mean():.1f}% (나머지는 Gemini 캡션)")
print("\n   ⚠️ 두 지표가 **서로 다른 모수**에 서 있다. 이벤트 F1 은 사람 정리 근거,")
print("      오탐 예산(G4)은 사실상 Gemini 라벨 위에서 재고 있다.")

json.dump(dict(n=N, n_cameras=len(cams), by_class=rows, by_camera=ctab,
               human_total=tot_h, human_pct=round(100 * tot_h / N, 1),
               event_denom_human_pct=round(float(100 * np.isin(gs[ev_m], list(HUMAN)).mean()), 1),
               normal_denom_human_pct=round(float(100 * np.isin(gs[fp_m], list(HUMAN)).mean()), 1),
               macro_ok_cameras=n_ok),
          open(f"{OUT}/filter_ab/gt_provenance.json", "w"), ensure_ascii=False, indent=1)
print(f"\n→ {OUT}/filter_ab/gt_provenance.json")
