#!/usr/bin/env python3
"""§23 재현성 감사 — 되돌아온 자기 문장(feedback loop)을 정량화한다.

문제: vOPT/vGEN 을 `prompt_banks`/`bank_sentences`/`image_embeddings` 에 등록한 뒤,
그 문장들이 **다음 큐레이션의 '공급' 후보 풀로 되돌아온다.** `sourcei_optbank.py` 는
`bank_sentences` 의 다수결 클래스로 후보를 모으므로 우리 뱅크가 그 원장에 들어간 순간
자기 문장을 다시 고를 수 있게 된다. §23 실행 시점에는 없던 경로다.

여기서 재는 것 3가지:
  ① 현재 후보 풀에 들어온 자기 문장 수 (클래스별)
  ② 재실행 선택 2,000개 중 자기 문장 비율
  ③ 재실행 선택 vs 실제 등록된 vOPT 뱅크의 자카드 — §23 이 그대로 재현되는가
"""
import os, sys, json, collections
sys.path.insert(0, "/workspace")
import numpy as np, psycopg2

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
conn = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")
cur = conn.cursor()

# 우리 뱅크 문장 해시
cur.execute("""SELECT b.version_tag, s.content_hash, s.class_label
               FROM prompt_banks b JOIN bank_sentences s USING(bank_id)
               WHERE b.version_tag IN ('vOPT.2026.08.28','vGEN.2026.08.28')""")
ours = collections.defaultdict(set); ours_cls = {}
for tag, h, c in cur:
    ours[tag].add(h); ours_cls[h] = c
OURS = set().union(*ours.values())
print(f"자기 문장 해시 — vOPT {len(ours['vOPT.2026.08.28']):,} · vGEN {len(ours['vGEN.2026.08.28']):,} "
      f"· 합집합 {len(OURS):,}")

# 후보 풀 자격 = bank_sentences 다수결 클래스가 4클래스 중 하나 + prompt 벡터 보유
cur.execute("SELECT content_hash, class_label, count(*) FROM bank_sentences GROUP BY 1,2")
votes = collections.defaultdict(dict)
for h, c, n in cur: votes[h][c] = n
maj = {h: max(sorted(v), key=lambda k: v[k]) for h, v in votes.items()}
cur.execute("SELECT entity_id FROM image_embeddings WHERE entity_type='prompt'")
have_vec = {r[0] for r in cur}
CLASSES = ["normal", "falldown", "fire", "smoke"]
cand = collections.Counter(); cand_ours = collections.Counter()
for h, c in maj.items():
    if c not in CLASSES or h not in have_vec: continue
    cand[c] += 1
    if h in OURS: cand_ours[c] += 1
print("\n① 후보 풀 자격 문장 (클래스별) / 그중 자기 문장:")
for c in CLASSES:
    print(f"   {c:9} {cand[c]:7,}  · 자기 {cand_ours[c]:5,} ({100*cand_ours[c]/max(cand[c],1):5.2f}%)")

# ② 재실행 선택에서 자기 문장 비율 — inference.json 에 저장한 키를 못 쓰므로 재계산 대신
#    filter_ab 가 남긴 자카드/선택을 다시 만들려면 비싸다. 대신 §23 실제 뱅크와의 관계로 본다.
# ③ §23 기록 vs 재실행 지표
o = json.load(open(f"{OUT}/optbank/optbank.json"))
i = json.load(open(f"{OUT}/filter_ab/inference.json"))
rec = {r["bank"]: r for r in o["compare"]}["sourcei-OPT (본 보고서)"]
b = i["pooled"]["base"]
print("\n③ §23 기록 vs 새 통계 재실행(base):")
for k_rec, k_new, lab in (("macro_f1", "mf1", "macro-F1(이벤트)"), ("prauc", "pr_auc", "분포 PR-AUC")):
    print(f"   {lab:16} {rec[k_rec]:.4f} → {b[k_new]:.4f}  (Δ {b[k_new]-rec[k_rec]:+.4f})")
print(f"   {'normal 오탐':16} {o['honest_oof']['fp_normal']:.4f} → {b['fp_normal']:.4f} "
      f"(Δ {b['fp_normal']-o['honest_oof']['fp_normal']:+.4f})")
print(f"\n   ⚠️ 재실행은 §23 과 **다른 후보 풀**에서 뽑는다 — 자기 문장 {len(OURS):,}개가 "
      f"공급 후보로 되돌아왔다.")
json.dump(dict(ours_total=len(OURS), ours_by_tag={k: len(v) for k, v in ours.items()},
               candidates=dict(cand), candidates_ours=dict(cand_ours),
               s23_recorded=dict(macro_f1=rec["macro_f1"], prauc=rec["prauc"],
                                 fp_normal=o["honest_oof"]["fp_normal"]),
               rerun_base=b,
               drift=dict(macro_f1=round(b["mf1"] - rec["macro_f1"], 4),
                          prauc=round(b["pr_auc"] - rec["prauc"], 4))),
          open(f"{OUT}/filter_ab/repro.json", "w"), ensure_ascii=False, indent=1)
print(f"\n→ {OUT}/filter_ab/repro.json")
