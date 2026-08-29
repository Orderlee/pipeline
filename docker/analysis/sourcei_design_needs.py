#!/usr/bin/env python3
"""'다음에 무슨 데이터가 얼마나 필요한가' 를 숫자로 — 검정력·교락·정보량.

deff≈232 가 뜻하는 것은 "프레임을 더 모아도 소용없다" 다. 그럼 무엇을 얼마나 모아야 하는지를
쌍대 차이의 **카메라 간 분산**에서 직접 역산한다(가정된 효과크기가 아니라 실측 분산 사용).
추가로 (1) 클래스×카메라 교락, (2) 이벤트 단위로 집계했을 때의 유효 표본, (3) 정보량이 가장 큰
라벨링 후보(규칙 불일치 셀)를 센다.
"""
import json, collections, itertools
import numpy as np

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CLASSES = ["normal", "falldown", "fire", "smoke"]
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
meta = np.load(f"{OUT}/frame_meta.npz", allow_pickle=True)
gt, cam, src, unit = d["gt"], d["camera"], d["gt_source"], d["unit"]
cams = np.unique(cam)
res = {}


def macro_f1(t, p, classes=(1, 2, 3)):
    f = []
    for c in classes:
        tp = ((p == c) & (t == c)).sum(); fp = ((p == c) & (t != c)).sum(); fn = ((p != c) & (t == c)).sum()
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1); f.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(f))


print("=== 1) 클래스 × 카메라 교락 — 카메라가 곧 클래스인가 ===")
rows = []
for c in cams:
    m = cam == c; cnt = np.bincount(gt[m], minlength=4)
    p = cnt / cnt.sum(); ent = -(p[p > 0] * np.log(p[p > 0])).sum() / np.log(4)
    rows.append((c, int(m.sum()), cnt.tolist(), float(ent), int((cnt > 0).sum())))
single = [r for r in rows if r[4] == 1]
print(f"  카메라 {len(cams)} 중 **단일 클래스만 있는 카메라 {len(single)}개** ({sum(r[1] for r in single):,} 프레임 = {sum(r[1] for r in single)/len(gt):.0%})")
for r in sorted(rows, key=lambda x: -x[1])[:6]:
    print(f"    {r[0][:42]:<44} n={r[1]:>5}  {dict(zip(CLASSES, r[2]))}  정규화엔트로피 {r[3]:.2f}")
# 클래스별로 몇 개 카메라에 존재하나 = 그 클래스 결론의 실질 표본
per_class_cams = {CLASSES[c]: int(sum(1 for cc in cams if (gt[cam == cc] == c).sum() > 0)) for c in range(4)}
print("  클래스가 존재하는 카메라 수:", per_class_cams, "← fire 결론의 실질 표본은 카메라", per_class_cams["fire"], "개")
res["confounding"] = dict(n_cams=len(cams), single_class_cams=len(single), per_class_cams=per_class_cams)

print("\n=== 2) 쌍대 차이의 카메라 간 분산 → 필요한 카메라 수 (검정력 역산) ===")
def paired_by_cam(pa, pb, min_n=50):
    """카메라별 macro-F1 차이. 이벤트 클래스가 없는 카메라는 macro-F1 이 정의되지 않아 제외."""
    ds = []
    for c in cams:
        m = cam == c
        cls = tuple(int(x) for x in np.unique(gt[m]) if x > 0)
        if m.sum() < min_n or not cls: continue
        ds.append(macro_f1(gt[m], pa[m], classes=cls) - macro_f1(gt[m], pb[m], classes=cls))
    return np.array(ds)


CASES = [("v1.0.8.1 vs v1.0.8.0 (뱅크)", d["topk__v1.0.8.1"], d["topk__v1.0.8.0"]),
         ("v1.0.8.0 vs v1.0.8.4 (뱅크)", d["topk__v1.0.8.0"], d["topk__v1.0.8.4"]),
         ("top-K vs argmax (규칙)", d["topk__v1.0.8.0"], d["argmax__v1.0.8.0"]),
         ("top-K vs 분포-IoU (규칙)", d["topk__v1.0.8.0"], d["wave__v1.0.8.0"])]
print(f"{'비교':<30}{'Δ평균':>8}{'카메라간 SD':>12}{'현재 n':>7}{'Δ=0.05 검출 필요 카메라':>24}")
res["power"] = {}
for lab, pa, pb in CASES:
    ds = paired_by_cam(pa, pb)
    sd = ds.std(ddof=1); mu = ds.mean()
    need = int(np.ceil((2.8 * sd / 0.05) ** 2)) if sd > 0 else 0     # 양측 α=.05, 검정력 80% → (1.96+0.84)=2.8
    need_own = int(np.ceil((2.8 * sd / max(abs(mu), 1e-9)) ** 2)) if sd > 0 else 0
    res["power"][lab] = dict(mean=float(mu), sd=float(sd), n_cams=len(ds), need_for_005=need, need_for_own_effect=need_own)
    print(f"{lab:<30}{mu:>+8.3f}{sd:>12.3f}{len(ds):>7}{need:>24}")
    print(f"{'':<30}(관측 효과 {mu:+.3f} 를 유의하게 만들려면 카메라 {need_own}개)")

print("\n=== 3) 이벤트 단위로 올리면 표본이 어떻게 되나 ===")
fie = meta["frame_in_event"].astype(int)
# 이벤트 키 = (카메라, event 시작 추정) — frame_in_event 가 1 로 리셋되는 지점으로 이벤트 경계 추정
ev_id = np.zeros(len(gt), dtype=np.int64); k = 0
order = np.lexsort((fie, cam))
prev_c, prev_f = None, None
for i in order:
    if cam[i] != prev_c or (prev_f is not None and fie[i] <= prev_f): k += 1
    ev_id[i] = k; prev_c, prev_f = cam[i], fie[i]
n_ev = len(np.unique(ev_id))
ev_cls = collections.Counter()
for e in np.unique(ev_id):
    m = ev_id == e; ev_cls[CLASSES[np.bincount(gt[m], minlength=4).argmax()]] += 1
print(f"  추정 이벤트 {n_ev:,}개 (프레임 {len(gt):,}) — 이벤트 다수결 클래스 분포 {dict(ev_cls)}")
print(f"  프레임/이벤트 중앙값 {np.median([np.sum(ev_id==e) for e in np.unique(ev_id)]):.0f}")
res["events"] = dict(n_events=int(n_ev), by_class={k: int(v) for k, v in ev_cls.items()})

print("\n=== 4) 정보량 최대 라벨링 후보 — 규칙/뱅크가 갈리는 프레임 (sourcei GT 기준 검증용이 아니라 frames 용) ===")
banks = [b for b in d["banks"]]
P = np.stack([d[f"topk__{b}"] for b in banks])
agree = (P == P[0]).all(axis=0)
print(f"  sourcei: 31뱅크 전부 일치 {agree.mean():.1%} / 갈리는 프레임 {(~agree).sum():,}")
print(f"    전부 일치 프레임 정확도 {(P[0][agree]==gt[agree]).mean():.3f} · 갈리는 프레임 {(P[0][~agree]==gt[~agree]).mean():.3f}")
res["disagreement"] = dict(all_agree_share=float(agree.mean()), acc_agree=float((P[0][agree] == gt[agree]).mean()),
                           acc_disagree=float((P[0][~agree] == gt[~agree]).mean()))
json.dump(res, open(f"{OUT}/design_needs.json", "w"), ensure_ascii=False, indent=1)
