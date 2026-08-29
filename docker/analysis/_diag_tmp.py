import sys, collections
sys.path.insert(0,"/workspace")
import fiftyone as fo, psycopg2
from prompt_cos_db import load_sentence_vectors
ds = fo.load_dataset("sourcei-prompts")
ids, txts, vers = ds.values(["id", "text", "bank_version.label"])
print("샘플", len(ids))
uniq = set(t for t in txts if t)
print("텍스트 있음", sum(1 for t in txts if t), "· 고유", len(uniq), "· 빈 텍스트", sum(1 for t in txts if not t))
ph = [t for t in uniq if t.startswith("__") or "자리표시" in t or t.startswith("[")]
print("자리표시자 후보(고유):", len(ph), ph[:3])
cur = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
cur.execute("SELECT MIN(text), content_hash FROM bank_sentences GROUP BY content_hash")
t2h = {t: h for t, h in cur.fetchall() if t}
h2c, SENT = load_sentence_vectors(cur)
have = sum(1 for t in uniq if t2h.get(t) in h2c)
print(f"고유 텍스트 {len(uniq):,} 중 DB 벡터 보유 {have:,} ({have/len(uniq):.1%}) · 미보유 {len(uniq)-have:,}")
n_have = sum(1 for t in txts if t and t2h.get(t) in h2c)
print(f"샘플 기준 벡터 보유 {n_have:,}/{len(ids):,} ({n_have/len(ids):.1%})")
miss = [t for t in uniq if t2h.get(t) not in h2c]
print("미보유 예시:"); [print("   ", repr(t[:90])) for t in miss[:6]]
byv = collections.Counter()
for t, v in zip(txts, vers):
    if t and t2h.get(t) not in h2c: byv[v] += 1
print("미보유 샘플의 버전 상위:", byv.most_common(6))
