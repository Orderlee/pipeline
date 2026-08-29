#!/usr/bin/env python3
"""§11 을 전 뱅크로 — frames 의 SAM3 fire/smoke 프레임 + 비화재 표본을 31뱅크 × 3규칙으로 채점.

FiftyOne 에 저장된 예측은 v1.0.8.0 하나뿐이라, 다른 버전은 여기서 직접 낸다.
커널은 sourcei 검증(sourcei_gt_rules.py)과 같다 — topk_vote / wave_iou / argmax, K=10, 80-bin, thr 0.15.
참조 라벨 = SAM3 검출(의사라벨). 조건 간·뱅크 간 상대 비교로만 읽는다.

표본: SAM3 fire 전부(1,578) + SAM3 smoke 전부(3,214) + 비화재(none/person) 20,000 무작위(seed 0,
frames_fire_conditions.py 와 같은 표본이라 §11 의 수치와 나란히 놓인다).
출력: csv/18_fire_by_bank.csv (뱅크×규칙: fire 재현율·오탐율·smoke→fire 혼동·조건별 재현율), fire_bank_preds.npz
"""
import os, sys, json, csv, collections
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ.setdefault(_v, "6")
import numpy as np, psycopg2
import fiftyone as fo
from fiftyone import ViewField as F
sys.path.insert(0, "/workspace")
from prompt_cos_db import load_banks, load_sentence_vectors, topk_vote, wave_iou, WAVE_THR, RULE_K

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CLASSES = ["normal", "falldown", "fire", "smoke"]
RNG = np.random.default_rng(0)

ds = fo.load_dataset("frames")
fr = ds.match(F("modality") == "frame")
ids, proj, ncls_raw, labs, boxes, emb = fr.values(["id", "project", "normalized_class", "detections.detections.label",
                                                    "detections.detections.bounding_box", "image_embedding"])
ncls = np.array([x or "none" for x in ncls_raw]); proj = np.array(proj)
fire_idx = np.where(ncls == "fire")[0]; sm_idx = np.where(ncls == "smoke")[0]
neg_idx = np.where(np.isin(ncls, ["none", "person"]))[0]
sub = np.concatenate([fire_idx, sm_idx, RNG.choice(neg_idx, size=min(20000, len(neg_idx)), replace=False)])
ref = np.array([{"fire": 2, "smoke": 3}.get(ncls[i], 0) for i in sub], dtype=np.int8)   # 약참조 (none/person→0)
Fm = np.asarray([emb[i] for i in sub], dtype=np.float32); Fm /= np.linalg.norm(Fm, axis=1, keepdims=True)
# 조건 축 (fire 프레임만 의미) — §11 과 같은 정의
def fire_area(i):
    L, B = labs[i], boxes[i]
    if not L: return 0.0
    a = [B[k][2] * B[k][3] for k, l in enumerate(L) if l == "fire"]; return max(a) if a else 0.0
def n_fire(i): return sum(1 for l in (labs[i] or []) if l == "fire")
def with_smoke(i): return "smoke" in (labs[i] or [])
area = np.array([fire_area(i) for i in sub]); nbox = np.array([n_fire(i) for i in sub]); wsm = np.array([with_smoke(i) for i in sub])
psub = proj[sub]
print(f"표본 {len(sub):,} = fire {len(fire_idx):,} + smoke {len(sm_idx):,} + neg {len(sub)-len(fire_idx)-len(sm_idx):,}")

cur = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
h2c, SENT = load_sentence_vectors(cur)
prepared = []
for b in load_banks(cur, None):
    if b["version"].startswith("v2."): continue
    if any(h not in h2c for h, _c, _g in b["rows"]): continue
    cls_local, gcols = {}, []
    for chash, cls, _g in b["rows"]:
        cls_local.setdefault(cls, []).append(len(gcols)); gcols.append(h2c[chash])
    if "normal" not in cls_local or "fire" not in cls_local: continue
    cs = sorted(cls_local); ev = [c for c in cs if c != "normal"]
    lab = np.empty(len(gcols), dtype=np.int32)
    for ci, c in enumerate(cs): lab[np.asarray(cls_local[c])] = ci
    to_ref = np.asarray([CLASSES.index(c) if c in CLASSES else -2 for c in cs], dtype=np.int8)
    ev_to_ref = np.asarray([CLASSES.index(c) if c in CLASSES else -2 for c in ev], dtype=np.int8)
    prepared.append(dict(version=b["version"], classes=cs, events=ev, lab=lab, to_ref=to_ref, ev_to_ref=ev_to_ref,
                         gcols=np.asarray(gcols), members={c: np.asarray(v) for c, v in cls_local.items()}))
print(f"뱅크 {len(prepared)}")

n = len(sub); RULES = ["argmax", "topk", "wave"]
P = {r: {p["version"]: np.empty(n, dtype=np.int8) for p in prepared} for r in RULES}
MARG = {p["version"]: np.empty(n, dtype=np.float32) for p in prepared}      # cos(fire)-cos(normal)
for s in range(0, n, 1000):
    S = Fm[s:s + 1000] @ SENT.T
    for p in prepared:
        Sb = S[:, p["gcols"]]; v = p["version"]
        P["topk"][v][s:s + 1000] = p["to_ref"][topk_vote(Sb, p["lab"], len(p["classes"]))]
        iou = wave_iou(Sb, p["members"]); I = np.stack([iou[c] for c in p["events"]], axis=1)
        P["wave"][v][s:s + 1000] = np.where((I < WAVE_THR).any(axis=1), p["ev_to_ref"][I.argmin(axis=1)], 0)
        per = np.stack([Sb[:, p["members"][c]].max(axis=1) for c in p["classes"]], axis=1)
        P["argmax"][v][s:s + 1000] = p["to_ref"][per.argmax(axis=1)]
        MARG[v][s:s + 1000] = per[:, p["classes"].index("fire")] - per[:, p["classes"].index("normal")]
    print(f"  {min(s + 1000, n):,}/{n:,}", flush=True)

isf, iss, isn = ref == 2, ref == 3, ref == 0
def rate(mask, pred, cls=2): return float((pred[mask] == cls).mean()) if mask.sum() else None
rows = []
for p in prepared:
    v = p["version"]
    for r in RULES:
        pr = P[r][v]
        row = dict(bank=v, rule=r, n_classes=len(p["classes"]),
                   fire_recall=rate(isf, pr), fire_or_smoke_recall=float((np.isin(pr[isf], [2, 3])).mean()),
                   fp_rate_nonfire=rate(isn, pr), smoke_to_fire=rate(iss, pr), smoke_recall=rate(iss, pr, 3),
                   rec_area_lt_0_001=rate(isf & (area < 0.001), pr), rec_area_0_001_0_01=rate(isf & (area >= 0.001) & (area < 0.01), pr),
                   rec_area_0_01_0_1=rate(isf & (area >= 0.01) & (area < 0.1), pr), rec_area_ge_0_1=rate(isf & (area >= 0.1), pr),
                   rec_1box=rate(isf & (nbox == 1), pr), rec_2plus_box=rate(isf & (nbox >= 2), pr),
                   rec_with_smoke=rate(isf & wsm, pr), rec_no_smoke=rate(isf & ~wsm, pr),
                   rec_fire_smoke_proj=rate(isf & (psub == "fire_smoke"), pr), rec_icce=rate(isf & (psub == "cohort-b"), pr),
                   rec_appdata=rate(isf & (psub == "appdata"), pr),
                   margin_neg_share=float((MARG[v][isf] < 0).mean()), rec_margin_neg=rate(isf & (MARG[v] < 0), pr), rec_margin_pos=rate(isf & (MARG[v] >= 0), pr))
        rows.append(row)
with open(f"{OUT}/csv/18_fire_by_bank.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
np.savez_compressed(f"{OUT}/fire_bank_preds.npz", sub=sub, ref=ref, area=area, nbox=nbox, wsm=wsm, proj=psub,
                    banks=np.array([p["version"] for p in prepared]),
                    **{f"{r}__{v}": P[r][v] for r in RULES for v in P[r]}, **{f"marg__{v}": MARG[v] for v in MARG})
print(f"→ 18_fire_by_bank.csv {len(rows)}행")
for r in RULES:
    xs = [x for x in rows if x["rule"] == r]
    print(f"\n== {r}: fire 재현율 평균 {np.mean([x['fire_recall'] for x in xs]):.3f} "
          f"[{min(x['fire_recall'] for x in xs):.3f}~{max(x['fire_recall'] for x in xs):.3f}]  오탐 {np.mean([x['fp_rate_nonfire'] for x in xs]):.3%}  smoke→fire {np.mean([x['smoke_to_fire'] for x in xs]):.3f}")
    for x in sorted(xs, key=lambda x: -x["fire_recall"])[:3] + sorted(xs, key=lambda x: x["fire_recall"])[:2]:
        print(f"    {x['bank']:<11} recall {x['fire_recall']:.3f}  fp {x['fp_rate_nonfire']:.3%}  smoke→fire {x['smoke_to_fire']:.2f}  점불꽃 {x['rec_area_lt_0_001'] or 0:.2f}  마진음수 {x['rec_margin_neg'] or 0:.2f}")
