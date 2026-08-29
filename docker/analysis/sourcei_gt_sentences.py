#!/usr/bin/env python3
"""sourcei(리테일형 실내 현장) 에서 어떤 프롬프트 문장이 GT 를 맞히고, 어떤 문장이 이벤트를 가로채는가.

전 텍스트 뱅크의 고유 문장(121,614)을 **하나의 합집합 뱅크**로 놓고(클래스 = 뱅크 간 다수결, 상충 문장은 제외)
프레임별 전역 top-10 을 내어 문장 단위 원장을 만든다:
  hit   = 문장이 top-10 에 들고 그 클래스 == GT   (그 클래스 프레임을 올바로 끌어당김)
  trap  = 문장이 top-10 에 들고 그 클래스 != GT   (다른 클래스 프레임을 가로챔 — normal 문장이 smoke 프레임을 잡는 것이 전형)
선택도 = hit / (hit + trap). 그 다음 hit 문장군 vs trap 문장군의 구문(1~3-gram) 대조로 "이 환경에서 써야 할/피해야 할 표현"을 뽑는다.
GT 잡음 주의: 이벤트 GT 는 영상 윈도우 라벨이라 hit/trap 의 절대값이 아니라 문장 간 상대 순위로만 읽는다.
"""
import os, sys, json, re, collections
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ.setdefault(_v, "6")
import numpy as np, psycopg2
sys.path.insert(0, "/workspace")
from prompt_cos_db import load_sentence_vectors
from sourcei_gt_rules import load_frames, CLASSES

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
K = 10
ids, F, gt, cam, src, unit = load_frames(); gt_i = np.array([CLASSES.index(g) for g in gt])
cur = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
h2c, SENT = load_sentence_vectors(cur)
cur.execute("""SELECT s.content_hash, MIN(s.text), s.class_label, COUNT(DISTINCT b.bank_id)
               FROM bank_sentences s JOIN prompt_banks b USING(bank_id)
               WHERE b.sentence_storage='db_backed' GROUP BY 1,3""")
cls_votes = collections.defaultdict(dict); text = {}
for h, t, c, n in cur:
    cls_votes[h][c] = n; text[h] = t
rows = []
for h, v in cls_votes.items():
    if h not in h2c: continue
    c = max(v, key=v.get)
    if c not in CLASSES or (len(v) > 1 and sorted(v.values())[-1] == sorted(v.values())[-2]): continue   # 자리표시자/동표 상충 제외
    rows.append((h2c[h], CLASSES.index(c), h))
cols = np.array([r[0] for r in rows]); lab = np.array([r[1] for r in rows]); hashes = [r[2] for r in rows]
print(f"합집합 문장 {len(rows):,} / 고유 {len(h2c):,}  클래스 분포 {np.bincount(lab, minlength=4).tolist()}")

hit = np.zeros(len(rows), np.int64); trap = np.zeros(len(rows), np.int64)
trap_by_gt = np.zeros((len(rows), 4), np.int64)       # 어느 GT 클래스 프레임을 가로챘나
hit_by_cam = collections.defaultdict(lambda: np.zeros(len(rows), np.int64))
pred = np.zeros(len(ids), np.int8); beat = collections.Counter()   # 누락 프레임에서 1등 문장
S_sub = SENT[cols]
for s in range(0, len(ids), 1000):
    S = F[s:s + 1000] @ S_sub.T
    part = np.argpartition(-S, K - 1, axis=1)[:, :K]
    for fi in range(S.shape[0]):
        f = s + fi; sel = part[fi]; g = gt_i[f]
        votes = np.bincount(lab[sel], minlength=4).astype(float)
        topc = np.full(4, -2.0)
        for c in range(4):
            mm = lab[sel] == c
            if mm.any(): topc[c] = S[fi, sel][mm].max()
        pred[f] = (votes + (topc + 2) / 10).argmax()
        ok = lab[sel] == g
        hit[sel[ok]] += 1; trap[sel[~ok]] += 1; trap_by_gt[sel[~ok], g] += 1
        hit_by_cam[cam[f]][sel[ok]] += 1
        if g > 0 and pred[f] == 0:
            beat[int(sel[S[fi, sel].argmax()])] += 1
    if (s // 1000) % 2 == 0: print(f"  {s + S.shape[0]:,}/{len(ids):,}", flush=True)
acc = (pred == gt_i).mean(); print(f"합집합 뱅크 top-K 정확도 {acc:.3f}")

sel_score = hit / np.maximum(hit + trap, 1)
appear = hit + trap
n_cams = np.array([sum(1 for c in hit_by_cam if hit_by_cam[c][j] > 0) for j in range(len(rows))])
out = {"n_sentences": len(rows), "union_topk_acc": float(acc), "per_class": {}}
for c in range(4):
    idx = np.where((lab == c) & (appear >= 20))[0]
    o = idx[np.argsort(-(sel_score[idx] * np.log1p(hit[idx])))]      # 선택도 × 규모
    good = [{"text": text[hashes[j]], "hit": int(hit[j]), "trap": int(trap[j]), "sel": float(sel_score[j]), "n_cam": int(n_cams[j])} for j in o[:25]]
    o2 = idx[np.argsort(-trap[idx])]
    bad = [{"text": text[hashes[j]], "hit": int(hit[j]), "trap": int(trap[j]), "sel": float(sel_score[j]),
            "trap_gt": {CLASSES[g]: int(trap_by_gt[j, g]) for g in range(4) if trap_by_gt[j, g] > 0}} for j in o2[:25]]
    out["per_class"][CLASSES[c]] = {"n_sent": int((lab == c).sum()), "n_active": int(len(idx)), "good": good, "bad": bad,
                                    "gt_frames": int((gt_i == c).sum())}
# 누락(이벤트→normal) 프레임에서 이긴 문장 = 이 환경의 블랙리스트 후보
out["beaters"] = [{"text": text[hashes[j]], "class": CLASSES[lab[j]], "n_missed_frames_won": n, "hit": int(hit[j]), "trap": int(trap[j])} for j, n in beat.most_common(30)]

# 구문 대조: 클래스별 hit-우세 문장 vs trap-우세 문장의 1~3-gram
STOP = set("a an the of in on at to with and or is are by for from into onto near under over as its their his her this that there".split())
def grams(t):
    w = re.findall(r"[a-z]+", t.lower()); g = set()
    for n in (1, 2, 3):
        for i in range(len(w) - n + 1):
            p = w[i:i + n]
            if n == 1 and p[0] in STOP: continue
            g.add(" ".join(p))
    return g
out["phrases"] = {}
for c in range(4):
    idx = np.where((lab == c) & (appear >= 10))[0]
    hw = collections.Counter(); tw = collections.Counter()
    for j in idx:
        for g_ in grams(text[hashes[j]]):
            hw[g_] += hit[j]; tw[g_] += trap[j]
    rowsp = []
    for g_ in set(hw) | set(tw):
        h_, t_ = hw[g_], tw[g_]
        if h_ + t_ < 200: continue
        rowsp.append((g_, int(h_), int(t_), h_ / (h_ + t_)))
    base = hit[idx].sum() / max(hit[idx].sum() + trap[idx].sum(), 1)
    rowsp.sort(key=lambda r: -r[3])
    out["phrases"][CLASSES[c]] = {"base_sel": float(base), "white": rowsp[:20], "black": sorted(rowsp, key=lambda r: r[3])[:20]}

# 카메라별 상위 문장 (현장 내부 편차)
out["per_camera_top"] = {}
for c_, hc in hit_by_cam.items():
    o = np.argsort(-hc)[:5]
    out["per_camera_top"][c_] = [{"text": text[hashes[j]], "class": CLASSES[lab[j]], "hit": int(hc[j])} for j in o if hc[j] > 0]

np.savez_compressed(f"{OUT}/sentence_ledger.npz", hit=hit, trap=trap, lab=lab, trap_by_gt=trap_by_gt, n_cams=n_cams,
                    hashes=np.array(hashes), text=np.array([text[h] for h in hashes]))
json.dump(out, open(f"{OUT}/sentences.json", "w"), ensure_ascii=False, indent=1)
for c in CLASSES:
    print("\n==", c, "good"); [print(f"  {r['sel']:.2f} hit={r['hit']:5d} trap={r['trap']:5d} cams={r['n_cam']:2d} | {r['text']}") for r in out["per_class"][c]["good"][:8]]
    print("==", c, "bad"); [print(f"  {r['sel']:.2f} hit={r['hit']:5d} trap={r['trap']:5d} {r['trap_gt']} | {r['text']}") for r in out["per_class"][c]["bad"][:6]]
print("\n== beaters"); [print(f"  {r['n_missed_frames_won']:4d} {r['class']:8s} | {r['text']}") for r in out["beaters"][:15]]
for c in CLASSES:
    print(f"\n== phrases {c} base={out['phrases'][c]['base_sel']:.2f}\n  white:", [f"{g} {s:.2f}" for g, h, t, s in out["phrases"][c]["white"][:12]], "\n  black:", [f"{g} {s:.2f}" for g, h, t, s in out["phrases"][c]["black"][:12]])
