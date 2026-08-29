#!/usr/bin/env python3
"""sourcei_optbank.py 후보 풀(maj 딕셔너리) 구성부만 떼어낸 A/B 검증 스크립트.
DB 읽기 전용. sourcei_optbank.py 전체(무거운 코사인/탐색)는 실행하지 않는다.

측정 항목:
  1) 클래스별 후보 수 (수정 전 vs 후)
  2) 클래스별 "자기 문장"(vOPT/vGEN 뱅크가 유일 근거인 content_hash) 수
     - 수정 전 maj 에 이 문장들이 몇 개 들어갔는지
     - 수정 후(공급 전용 집계) 에는 0 이어야 함
  3) vOPT 2,000문장 중 "공급 뱅크에도 존재하는" 문장 수 + 그 문장들이
     공급-전용 집계에서 살아남는지 (자격을 얻는지) 실측
"""
import collections, json, psycopg2

OUT_JSON = "/data/fiftyone/frames_bank/report/sourcei_gt/filter_ab/supply_only.json"
CLASSES = ["normal", "falldown", "fire", "smoke"]
SELF_BANKS = ["vOPT.2026.08.28", "vGEN.2026.08.28"]

conn = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")
cur = conn.cursor()

# ── bank_id → source, version_tag 매핑 ──────────────────────────────
cur.execute("SELECT bank_id, version_tag, source FROM prompt_banks")
bank_meta = {str(bid): (vt, src) for bid, vt, src in cur.fetchall()}
self_bank_ids = {bid for bid, (vt, src) in bank_meta.items() if vt in SELF_BANKS}
print(f"[meta] self bank_id: {self_bank_ids}")
print(f"[meta] source 분포: {collections.Counter(src for _, src in bank_meta.values())}")

# ── (수정 전) 전량 다수결: 모든 뱅크(userwatch+internal+hybrid) 합산 ──
cur.execute("SELECT bank_id, content_hash, class_label, count(*) FROM bank_sentences GROUP BY 1,2,3")
rows = cur.fetchall()  # (bank_id, content_hash, class_label, n)

votes_all = collections.defaultdict(dict)          # content_hash -> {class: n}  (전량)
votes_supply = collections.defaultdict(dict)        # content_hash -> {class: n}  (userwatch 만)
hash_banks = collections.defaultdict(set)           # content_hash -> {bank_id,...} 어느 뱅크에 존재하는지

for bank_id, h, c, n_ in rows:
    bank_id = str(bank_id)
    hash_banks[h].add(bank_id)
    votes_all[h][c] = votes_all[h].get(c, 0) + n_
    src = bank_meta.get(bank_id, (None, None))[1]
    if src == "userwatch":
        votes_supply[h][c] = votes_supply[h].get(c, 0) + n_

maj_before = {h: max(sorted(v), key=lambda k: v[k]) for h, v in votes_all.items()}
maj_after = {h: max(sorted(v), key=lambda k: v[k]) for h, v in votes_supply.items()}

# ── 1) 클래스별 후보 수 (수정 전 vs 후) ──────────────────────────────
cnt_before = collections.Counter(c for c in maj_before.values() if c in CLASSES)
cnt_after = collections.Counter(c for c in maj_after.values() if c in CLASSES)
print("\n[1] 클래스별 후보 자격 획득 문장 수 (maj 딕셔너리 크기)")
for c in CLASSES:
    print(f"  {c:<9} 수정전 {cnt_before.get(c,0):>7,}  →  수정후 {cnt_after.get(c,0):>7,}")

# ── 2) 클래스별 "자기 문장만" 근거인 content_hash 수 ────────────────
# = userwatch 뱅크에는 전혀 없고 vOPT/vGEN 에만 있는 해시
self_only_hashes = {h for h, banks in hash_banks.items() if banks and banks.issubset(self_bank_ids)}
print(f"\n[검증] 자기전용(self-only) content_hash 총 {len(self_only_hashes):,}개 (userwatch 어디에도 없음)")

self_in_before = collections.Counter(maj_before[h] for h in self_only_hashes if h in maj_before)
self_in_after = collections.Counter(maj_after[h] for h in self_only_hashes if h in maj_after)
print("[2] 클래스별 '자기 문장' 오염 수 — 수정 전 후보 자격을 얻은 self-only 문장 vs 수정 후")
for c in CLASSES:
    b = self_in_before.get(c, 0); a = self_in_after.get(c, 0)
    print(f"  {c:<9} 수정전 {b:>6,}  →  수정후 {a:>6,}  {'OK(0)' if a == 0 else '!! 여전히 오염'}")

# ── 3) vOPT 2,000문장 중 공급 뱅크에도 존재하는 문장 + 생존 여부 ────
vopt_id = next(bid for bid, (vt, src) in bank_meta.items() if vt == "vOPT.2026.08.28")
vgen_id = next(bid for bid, (vt, src) in bank_meta.items() if vt == "vGEN.2026.08.28")
cur.execute("SELECT content_hash, class_label FROM bank_sentences WHERE bank_id = %s", (vopt_id,))
vopt_rows = cur.fetchall()
print(f"\n[3] vOPT.2026.08.28 문장 수 = {len(vopt_rows)}")

n_has_supply = 0
n_supply_and_qualifies = 0
n_supply_but_dropped = 0
examples_dropped = []
for h, c in vopt_rows:
    banks = hash_banks.get(h, set())
    has_supply = any(bank_meta.get(b, (None, None))[1] == "userwatch" for b in banks)
    if has_supply:
        n_has_supply += 1
        if h in maj_after:
            n_supply_and_qualifies += 1
        else:
            n_supply_but_dropped += 1
            if len(examples_dropped) < 5: examples_dropped.append(h)

print(f"  공급 뱅크에도 존재하는 문장(중복) = {n_has_supply:,} / {len(vopt_rows):,}")
print(f"  → 그중 수정 후 집계에서도 후보 자격 유지 = {n_supply_and_qualifies:,}")
print(f"  → 그중 수정 후 집계에서 탈락(이상치, 있으면 안 됨) = {n_supply_but_dropped:,}")

# vGEN(internal) 은 정의상 100% 자기 문장이므로 공급 교집합은 0 이어야 함 — 대조군
cur.execute("SELECT content_hash FROM bank_sentences WHERE bank_id = %s", (vgen_id,))
vgen_hashes = [h for (h,) in cur.fetchall()]
vgen_has_supply = sum(1 for h in vgen_hashes if any(bank_meta.get(b, (None, None))[1] == "userwatch" for b in hash_banks.get(h, set())))
print(f"\n[대조군] vGEN.2026.08.28(internal) {len(vgen_hashes):,}문장 중 공급 교집합 = {vgen_has_supply:,} (0 근처가 정상)")

result = dict(
    self_bank_ids=sorted(self_bank_ids),
    candidates_before={c: cnt_before.get(c, 0) for c in CLASSES},
    candidates_after={c: cnt_after.get(c, 0) for c in CLASSES},
    self_only_hash_total=len(self_only_hashes),
    self_pollution_before={c: self_in_before.get(c, 0) for c in CLASSES},
    self_pollution_after={c: self_in_after.get(c, 0) for c in CLASSES},
    vopt_total=len(vopt_rows),
    vopt_dup_with_supply=n_has_supply,
    vopt_dup_survives_after=n_supply_and_qualifies,
    vopt_dup_dropped_after=n_supply_but_dropped,
    vgen_total=len(vgen_hashes),
    vgen_dup_with_supply=vgen_has_supply,
)
import os
os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
json.dump(result, open(OUT_JSON, "w"), ensure_ascii=False, indent=2)
print(f"\n→ {OUT_JSON}")
print("DONE")
