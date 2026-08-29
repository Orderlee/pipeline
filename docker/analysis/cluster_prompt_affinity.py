#!/usr/bin/env python3
"""목적 2 — 클러스터(현장 내 시각 군집)별로 어떤 프롬프트가 '붙는가' (라벨 불필요).

입력: analysis.sentence_affinity — 문장(121,614) × 그룹(55 = project#wp16 군집) 의 mean/p90/max 코사인.
⚠️ 적재 범위: 9현장(vietnam·ktt_loc-d·sourcea·partner-d·source-o·vhc·yeonsei·sembcorp)만. fire_smoke·cohort-b·appdata·
loc-c·source-f 는 아직 없다(디스크 제약으로 잔여 적재 보류) — fire 가 많은 현장이 빠져 있다는 뜻.

핵심 지표 = **특이도(specificity)**: r(s,g) = mean_cos(s,g) − mean_g' mean_cos(s,g')
  · 모든 군집에 다 붙는 문장(일반 문장)은 r≈0 → 이벤트 검출에 쓸모 없다(어디서나 켜짐).
  · 특정 군집에서만 튀는 문장은 r 이 크다 → 그 군집의 시각 내용에 반응하는 문장.
  · 그 문장의 클래스가 이벤트(fire/smoke/falldown)면 "그 군집에서 이벤트를 잡는 문장" 후보,
    normal 이면 "그 군집에서 이벤트를 삼킬 위험이 있는 문장".
클래스 = 뱅크 간 다수결(같은 문장이 뱅크마다 다른 클래스일 수 있어, 상충은 별도 표기).
"""
import os, json, csv, collections
import numpy as np, psycopg2

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CLASSES = ["normal", "falldown", "fire", "smoke"]
conn = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"); cur = conn.cursor()

cur.execute("""WITH c AS (SELECT content_hash, class_label, count(*) n,
                          row_number() OVER (PARTITION BY content_hash ORDER BY count(*) DESC, class_label) rn,
                          count(*) OVER (PARTITION BY content_hash) n_cls
                   FROM bank_sentences GROUP BY 1,2)
               SELECT c.content_hash, c.class_label, c.n_cls, MIN(s.text) FROM c JOIN bank_sentences s USING(content_hash)
               WHERE rn=1 GROUP BY 1,2,3""")
cls, text, ncls = {}, {}, {}
for h, c, n, t in cur: cls[h] = c; text[h] = t; ncls[h] = n
print(f"문장 {len(cls):,}, 클래스 분포 {dict(collections.Counter(cls.values()))}")

cur.execute("SELECT DISTINCT group_key FROM analysis.sentence_affinity WHERE group_kind='cluster' ORDER BY 1")
groups = [g for (g,) in cur]; gi = {g: k for k, g in enumerate(groups)}
cur.execute("SELECT DISTINCT content_hash FROM analysis.sentence_affinity WHERE group_kind='cluster'")
hashes = [h for (h,) in cur]; hi = {h: k for k, h in enumerate(hashes)}
M = np.full((len(hashes), len(groups)), np.nan, np.float32); NF = np.zeros(len(groups), int)
with conn.cursor(name="aff") as c2:
    c2.itersize = 200000
    c2.execute("SELECT content_hash, group_key, n_frames, mean_cos FROM analysis.sentence_affinity WHERE group_kind='cluster'")
    for h, g, nf, mc in c2:
        M[hi[h], gi[g]] = mc; NF[gi[g]] = nf
print(f"친화도 행렬 {M.shape}, 결손 {np.isnan(M).mean():.2%}, 군집 프레임수 {NF.min()}~{NF.max()}, 현장 {sorted({g.split('#')[0] for g in groups})}")

row_mean = np.nanmean(M, 1, keepdims=True)
R = M - row_mean
colz = (R - np.nanmean(R, 0)) / (np.nanstd(R, 0) + 1e-9)
sent_cls = np.array([CLASSES.index(cls[h]) if cls[h] in CLASSES else -1 for h in hashes])
gen = float((np.nanstd(R, 1) < 0.005).mean())
print(f"군집 간 편차 SD < 0.005 인 '어디서나 같은' 문장 비율 {gen:.1%}")

rows_att, rows_top = [], []
for g in groups:
    j = gi[g]
    for ci, cname in enumerate(CLASSES):
        idx = np.where(sent_cls == ci)[0]
        v = colz[idx, j]; ok = ~np.isnan(v); idx, v = idx[ok], v[ok]
        if len(idx) == 0: continue
        o = np.argsort(-v)
        rows_att.append(dict(group=g, project=g.split("#")[0], n_frames=int(NF[j]), cls=cname, n_sentences=len(idx),
                             top20_specificity=round(float(v[o[:20]].mean()), 3), share_z_gt1=round(float((v > 1.0).mean()), 4),
                             median_specificity=round(float(np.median(v)), 3)))
        for r, k in enumerate(o[:8]):
            h = hashes[idx[k]]
            rows_top.append(dict(group=g, cls=cname, rank=r + 1, specificity_z=round(float(v[k]), 3),
                                 mean_cos=round(float(M[idx[k], j]), 4), sentence_global_mean=round(float(row_mean[idx[k], 0]), 4),
                                 class_conflict=("Y" if ncls[h] > 1 else ""), text=text[h]))
with open(f"{OUT}/csv/19_cluster_class_attachment.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=list(rows_att[0].keys())); w.writeheader(); w.writerows(rows_att)
with open(f"{OUT}/csv/20_cluster_top_sentences.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=list(rows_top[0].keys())); w.writeheader(); w.writerows(rows_top)

print("\n=== 클래스별로 가장 강하게 붙는 군집 (top20 특이도 z) ===")
summ = {}
for cname in CLASSES[1:]:
    rs = sorted([r for r in rows_att if r["cls"] == cname], key=lambda r: -r["top20_specificity"])
    summ[cname] = [(r["group"], r["top20_specificity"], r["n_frames"]) for r in rs[:5]]
    print(f"  {cname:<9}", " | ".join(f"{r['group']} {r['top20_specificity']:.2f} (n={r['n_frames']})" for r in rs[:5]))
for cname in ["fire", "smoke", "falldown"]:
    g0 = summ[cname][0][0]
    print(f"\n=== {cname} 특이도 1위 군집 {g0} — {cname} 문장 top5 ===")
    for r in [x for x in rows_top if x["group"] == g0 and x["cls"] == cname][:5]: print(f"  z={r['specificity_z']:.2f} cos={r['mean_cos']:.3f} {r['text'][:88]}")
    print(f"  --- 같은 군집에서 가장 튀는 normal 문장 (이벤트를 삼킬 후보) ---")
    for r in [x for x in rows_top if x["group"] == g0 and x["cls"] == "normal"][:3]: print(f"  z={r['specificity_z']:.2f} cos={r['mean_cos']:.3f} {r['text'][:88]}")
json.dump(dict(groups=len(groups), sentences=len(hashes), generic_share=gen, top_clusters=summ, projects=sorted({g.split('#')[0] for g in groups})),
          open(f"{OUT}/cluster_affinity.json", "w"), ensure_ascii=False, indent=1)
np.save(f"{OUT}/cluster_specificity_z.npy", colz.astype(np.float16)); json.dump(dict(groups=groups, hashes=hashes), open(f"{OUT}/cluster_specificity_keys.json", "w"))
