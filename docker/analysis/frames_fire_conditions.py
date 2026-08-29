#!/usr/bin/env python3
"""frames 전체(188k, 21+현장)에서 **화재 프롬프트가 어떤 상황에서 잘 반응하는가**.

GT 가 없는 데이터라 **SAM3 검출(normalized_class, review_status=auto_generated)을 약한 참조**로 쓴다.
SAM3 는 프롬프트와 독립된 모달리티(bbox 세그멘터)라 둘의 일치/불일치는 정보가 있지만,
SAM3 자체가 오탐·누락을 가진 의사라벨이므로 **절대값이 아니라 조건 간 상대 비교**로만 읽는다
(sourcei GT 분석의 윈도우 라벨 잡음과 같은 위치의 경고). 자기학습 금지 원칙상 학습에는 못 쓰고 분석에만 쓴다.

산출:
  fire_frames.csv     — SAM3 fire 프레임 1건 1행: 프로젝트·박스 수·최대/합 면적·신뢰도·동반 클래스·프롬프트 예측·cos·마진
  fire_conditions.json — 조건별 프롬프트 fire 재현율(약참조) + 오탐(SAM3 none 인데 fire) 분포 + fire 문장 hit/trap 원장
  fire_sentence_ledger.csv — fire 문장별 hit/trap (SAM3 약참조), 어떤 문장이 어떤 상황에서 이기나
"""
import os, sys, json, csv, collections, re
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ.setdefault(_v, "6")
import numpy as np, psycopg2
import fiftyone as fo
from fiftyone import ViewField as F
sys.path.insert(0, "/workspace")
from prompt_cos_db import load_sentence_vectors

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"; os.makedirs(f"{OUT}/csv", exist_ok=True)
CLASSES = ["normal", "falldown", "fire", "smoke"]
K = 10; RNG = np.random.default_rng(0)

ds = fo.load_dataset("frames")
fr = ds.match(F("modality") == "frame")
fields = ["id", "project", "normalized_class", "detections.detections.label", "detections.detections.confidence",
          "detections.detections.bounding_box", "pred_v1_0_8_0.label", "top_prompt_v1_0_8_0", "pred_margin_v1080",
          "cos_best_fire", "cos_best_normal", "cos_best_smoke", "image_embedding", "minio_key"]
V = dict(zip(fields, fr.values(fields)))
n = len(V["id"]); print(f"frames {n:,}")
ncls = np.array([x or "none" for x in V["normalized_class"]])
pred = np.array([x or "none" for x in V["pred_v1_0_8_0.label"]])
proj = np.array(V["project"])


def det_stats(labels, confs, boxes, want):
    """want 클래스 박스의 (개수, 최대면적, 합면적, 최대신뢰도), 그리고 동반 클래스 집합."""
    if not labels: return 0, 0.0, 0.0, 0.0, set()
    idx = [i for i, l in enumerate(labels) if l == want]
    areas = [boxes[i][2] * boxes[i][3] for i in idx]
    others = {l for l in labels if l != want}
    return len(idx), (max(areas) if areas else 0.0), sum(areas), (max(confs[i] for i in idx) if idx else 0.0), others


# ── 1. SAM3 fire 프레임 조건표 ─────────────────────────────────────────
fire_idx = np.where(ncls == "fire")[0]
rows = []
for i in fire_idx:
    nb, amax, asum, cmax, others = det_stats(V["detections.detections.label"][i], V["detections.detections.confidence"][i],
                                             V["detections.detections.bounding_box"][i], "fire")
    cf, cn, cs = V["cos_best_fire"][i], V["cos_best_normal"][i], V["cos_best_smoke"][i]
    rows.append(dict(project=proj[i], n_fire_boxes=nb, max_box_area=amax, sum_box_area=asum, max_conf=cmax,
                     with_smoke="smoke" in others, with_person="person" in others,
                     prompt_pred=pred[i], hit_fire=int(pred[i] == "fire"), hit_fire_or_smoke=int(pred[i] in ("fire", "smoke")),
                     cos_fire=cf, cos_normal=cn, cos_smoke=cs, fire_minus_normal=(None if cf is None or cn is None else cf - cn),
                     margin=V["pred_margin_v1080"][i], top_prompt=V["top_prompt_v1_0_8_0"][i], minio_key=V["minio_key"][i]))
with open(f"{OUT}/csv/16_fire_frames_sam3.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
print(f"SAM3 fire 프레임 {len(rows):,} → 16_fire_frames_sam3.csv")


def rate(sel):
    sel = [r for r in sel]; return (len(sel), (sum(r["hit_fire"] for r in sel) / len(sel) if sel else None))


def bucket(vals, edges):
    out = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        out.append((f"{lo:g}~{hi:g}", [r for r, v in zip(rows, vals) if v is not None and lo <= v < hi]))
    return out


cond = {}
cond["by_project"] = {p: rate([r for r in rows if r["project"] == p]) for p in sorted({r["project"] for r in rows})}
cond["by_n_boxes"] = {("1" if r == 1 else "2~3" if r <= 3 else "4+"): None for r in [1, 2, 4]}
for k in cond["by_n_boxes"]:
    cond["by_n_boxes"][k] = rate([r for r in rows if (k == "1" and r["n_fire_boxes"] == 1) or (k == "2~3" and 2 <= r["n_fire_boxes"] <= 3) or (k == "4+" and r["n_fire_boxes"] >= 4)])
cond["by_max_area"] = {k: rate(v) for k, v in bucket([r["max_box_area"] for r in rows], [0, 0.001, 0.003, 0.01, 0.03, 0.1, 1.01])}
cond["by_conf"] = {k: rate(v) for k, v in bucket([r["max_conf"] for r in rows], [0, 0.5, 0.7, 0.8, 0.9, 1.01])}
cond["by_with_smoke"] = {"smoke 동반": rate([r for r in rows if r["with_smoke"]]), "smoke 없음": rate([r for r in rows if not r["with_smoke"]])}
cond["by_with_person"] = {"person 동반": rate([r for r in rows if r["with_person"]]), "person 없음": rate([r for r in rows if not r["with_person"]])}
cond["by_fire_minus_normal"] = {k: rate(v) for k, v in bucket([r["fire_minus_normal"] for r in rows], [-1, -0.02, 0, 0.02, 0.05, 0.1, 1])}

# ── 2. 오탐 측: SAM3 none/person 인데 프롬프트 fire ───────────────────
fp_idx = np.where((pred == "fire") & np.isin(ncls, ["none", "person"]))[0]
neg_idx = np.where(np.isin(ncls, ["none", "person"]))[0]
cond["fp_overall"] = {"n_fp": int(len(fp_idx)), "n_neg": int(len(neg_idx)), "fp_rate": float(len(fp_idx) / len(neg_idx))}
cond["fp_by_project"] = {}
for p in sorted(set(proj[neg_idx])):
    m = proj[neg_idx] == p; nfp = int((pred[neg_idx][m] == "fire").sum())
    cond["fp_by_project"][p] = (int(m.sum()), nfp, float(nfp / max(m.sum(), 1)))
cond["fp_top_sentences"] = collections.Counter(V["top_prompt_v1_0_8_0"][i] for i in fp_idx).most_common(25)
cond["fire_hit_top_sentences"] = collections.Counter(r["top_prompt"] for r in rows if r["hit_fire"]).most_common(25)
cond["fire_miss_top_sentences"] = collections.Counter(r["top_prompt"] for r in rows if r["prompt_pred"] == "normal").most_common(15)
# smoke→fire 혼동
sm_idx = np.where(ncls == "smoke")[0]
cond["sam3_smoke_pred"] = dict(collections.Counter(pred[sm_idx]))
cond["sam3_smoke_to_fire_top_sentences"] = collections.Counter(V["top_prompt_v1_0_8_0"][i] for i in sm_idx if pred[i] == "fire").most_common(15)

# ── 3. fire 문장 hit/trap 원장 (합집합 뱅크, 부분표본) ──────────────────
#  표본 = SAM3 fire 전부 + SAM3 smoke 전부 + none/person 20,000 무작위. 약참조 라벨 = SAM3 클래스(fall→falldown).
sub = np.concatenate([fire_idx, sm_idx, RNG.choice(neg_idx, size=min(20000, len(neg_idx)), replace=False)])
lab_map = {"fire": 2, "smoke": 3, "fall": 1, "none": 0, "person": 0, "patient": 0}
wlab = np.array([lab_map.get(ncls[i], 0) for i in sub], dtype=np.int8)
Fm = np.asarray([V["image_embedding"][i] for i in sub], dtype=np.float32); Fm /= np.linalg.norm(Fm, axis=1, keepdims=True)
cur = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
h2c, SENT = load_sentence_vectors(cur)
cur.execute("""SELECT s.content_hash, MIN(s.text), s.class_label, COUNT(DISTINCT b.bank_id)
               FROM bank_sentences s JOIN prompt_banks b USING(bank_id)
               WHERE b.sentence_storage='db_backed' GROUP BY 1,3""")
votes = collections.defaultdict(dict); text = {}
for h, t, c, nn in cur: votes[h][c] = nn; text[h] = t
srows = []
for h, v in votes.items():
    if h not in h2c: continue
    c = max(v, key=v.get)
    if c not in CLASSES or (len(v) > 1 and sorted(v.values())[-1] == sorted(v.values())[-2]): continue
    srows.append((h2c[h], CLASSES.index(c), h))
cols = np.array([r[0] for r in srows]); slab = np.array([r[1] for r in srows]); hashes = [r[2] for r in srows]
S_sub = SENT[cols]
hit = np.zeros(len(srows), np.int64); trap = np.zeros(len(srows), np.int64); trap_from = np.zeros((len(srows), 4), np.int64)
hit_proj = collections.defaultdict(collections.Counter)
for s in range(0, len(sub), 1000):
    S = Fm[s:s + 1000] @ S_sub.T
    part = np.argpartition(-S, K - 1, axis=1)[:, :K]
    for fi in range(S.shape[0]):
        g = wlab[s + fi]; sel = part[fi]; ok = slab[sel] == g
        hit[sel[ok]] += 1; trap[sel[~ok]] += 1; trap_from[sel[~ok], g] += 1
        if g == 2:
            for j in sel[ok]: hit_proj[j][proj[sub[s + fi]]] += 1
    if (s // 1000) % 5 == 0: print(f"  ledger {s + S.shape[0]:,}/{len(sub):,}", flush=True)
fire_s = np.where(slab == 2)[0]
led = []
for j in fire_s:
    if hit[j] + trap[j] == 0: continue
    led.append(dict(text=text[hashes[j]], hit=int(hit[j]), trap=int(trap[j]), selectivity=round(hit[j] / (hit[j] + trap[j]), 4),
                    trap_from_normal=int(trap_from[j, 0]), trap_from_smoke=int(trap_from[j, 3]), trap_from_falldown=int(trap_from[j, 1]),
                    hit_projects=";".join(f"{p}:{c}" for p, c in hit_proj[j].most_common(5))))
led.sort(key=lambda r: (-r["hit"], r["trap"]))
with open(f"{OUT}/csv/17_fire_sentence_ledger_frames.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=list(led[0].keys())); w.writeheader(); w.writerows(led)
print(f"fire 문장 원장 {len(led):,} → 17_fire_sentence_ledger_frames.csv")
# 구문 대조 (fire 문장 안에서 hit-우세 vs trap-우세)
STOP = set("a an the of in on at to with and or is are by for from into onto near under over as its their this that there".split())
def grams(t):
    w = re.findall(r"[a-z]+", t.lower()); g = set()
    for k in (1, 2, 3):
        for i in range(len(w) - k + 1):
            p = w[i:i + k]
            if k == 1 and p[0] in STOP: continue
            g.add(" ".join(p))
    return g
hw = collections.Counter(); tw = collections.Counter()
for r in led:
    for g_ in grams(r["text"]): hw[g_] += r["hit"]; tw[g_] += r["trap"]
base = sum(r["hit"] for r in led) / max(sum(r["hit"] + r["trap"] for r in led), 1)
ph = [(g_, hw[g_], tw[g_], hw[g_] / (hw[g_] + tw[g_])) for g_ in set(hw) | set(tw) if hw[g_] + tw[g_] >= 150]
ph.sort(key=lambda x: -x[3])
cond["fire_phrase_base"] = base; cond["fire_phrase_white"] = ph[:25]; cond["fire_phrase_black"] = sorted(ph, key=lambda x: x[3])[:25]
cond["ledger_sample"] = {"n_frames": int(len(sub)), "sam3_fire": int(len(fire_idx)), "sam3_smoke": int(len(sm_idx)), "neg_sampled": int(len(sub) - len(fire_idx) - len(sm_idx))}
json.dump(cond, open(f"{OUT}/fire_conditions.json", "w"), ensure_ascii=False, indent=1, default=str)
for k in ["by_project", "by_n_boxes", "by_max_area", "by_conf", "by_with_smoke", "by_with_person", "by_fire_minus_normal"]:
    print(f"\n== {k}"); [print(f"  {kk:<40} n={v[0]:>5}  fire재현율 {v[1] if v[1] is None else round(v[1], 3)}") for kk, v in cond[k].items()]
print("\n== fp", cond["fp_overall"]); print("== fp by project (n, nfp, rate)"); [print(f"  {p:<38} {v}") for p, v in sorted(cond["fp_by_project"].items(), key=lambda x: -x[1][2])[:12]]
print("\n== fire phrases white", [f"{g} {s:.2f}" for g, h, t, s in ph[:15]]); print("== black", [f"{g} {s:.2f}" for g, h, t, s in sorted(ph, key=lambda x: x[3])[:15]])
