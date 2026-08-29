import sys, collections
sys.path.insert(0,"/workspace")
import fiftyone as fo, psycopg2
from prompt_cos_db import load_sentence_vectors
ds=fo.load_dataset("sourcei-prompts"); txts,vers=ds.values(["text","bank_version.label"])
cur=psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
cur.execute("SELECT MIN(text), content_hash FROM bank_sentences GROUP BY content_hash")
t2h={t:h for t,h in cur.fetchall() if t}
h2c,SENT=load_sentence_vectors(cur)
have=sum(1 for t in txts if t2h.get(t) in h2c)
print(f"샘플 {len(txts):,} · 벡터 보유 {have:,} ({have/len(txts):.1%}) · 미보유 {len(txts)-have:,}")
c=collections.Counter(v for t,v in zip(txts,vers) if t2h.get(t) not in h2c)
print("미보유 버전:", len(c))
for k,n in c.most_common(): print(f"   {k:<16} {n:>7,}")
print("DB prompt 벡터 총:", SENT.shape)
