#!/usr/bin/env python3
"""새 뱅크 2종(vOPT·vGEN)의 **문장·벡터를 Postgres 정본에 등록**한다.

왜 필요한가: compare 패널이 지금 "DB 문장 미보유(external_only)" 로 폴백 중이고,
`sourcei-prompts` 의 벡터 미보유 263,747건 안에 **우리가 만든 vGEN 2,000 · vOPT 500** 이
들어 있다. 이건 텍스트가 있으므로 **바로 만들어 넣을 수 있는** 유일한 부분이다
(나머지 246,644 는 `prompt:null` 벡터전용 공급 뱅크라 NAS 원본에서 회수해야 한다).

쓰는 곳 3개 — 019 스키마 규약을 그대로 따른다:
  · `image_embeddings` (entity_type='prompt', entity_id=content_hash)  ← 벡터
  · `prompt_banks`     (sentence_storage='db_backed')                  ← 뱅크 정본
  · `bank_sentences`   (bank_id, content_hash, class_label, text, gidx)

content_hash = sha256(공백정규화+소문자화)[:16] — `prompt_bank_ledger.content_hash` 와 동일.
⚠️ 알려진 결함 승계: class 가 해시에 안 들어가 같은 text·다른 class 는 충돌한다. 019 가
   고치지 않기로 한 규약이라 여기서도 발명하지 않는다. 등록 전 뱅크 내부 충돌을 검사한다.

기본 DRY-RUN. `--apply` 로 실제 쓰기.
"""
import os, sys, json, uuid, hashlib, re, time
sys.path.insert(0, "/workspace")
import numpy as np, psycopg2
from psycopg2.extras import execute_batch

BANKDIR = "/data/fiftyone/frames_bank/report/sourcei_gt/optbank"
DSN = "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"
MODEL = "facebook/PE-Core-L14-336"
APPLY = "--apply" in sys.argv
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

def norm_text(s): return re.sub(r"\s+", " ", str(s).strip().lower())
def content_hash(t): return hashlib.sha256(norm_text(t).encode("utf-8")).hexdigest()[:16]

BANKS = [
    # ⚠️ `source` 는 CHECK 제약으로 userwatch|internal|hybrid 만 허용된다 (실측).
    #    공급 문장을 섞은 vOPT 는 hybrid, 전량 생성인 vGEN 은 internal 이 정확한 값이다.
    dict(tag="vOPT.2026.08.28", npz=f"{BANKDIR}/optbank_vectors.npz", gidx0=2900000, source="hybrid",
         note="sourcei 전용 혼합 뱅크 (공급 75% + 생성 25%) — 보고서 §23"),
    dict(tag="vGEN.2026.08.28", npz=f"{BANKDIR}/genfull_bank.npz", gidx0=3100000, source="internal",
         note="sourcei 전용 전량 생성 뱅크 (공급 0) — 보고서 §25"),
]
# ⚠️ 저장된 뱅크 벡터는 **전역 문장평균 제거판**이다(§16 노브). DB 정본에는 그 변환을 하지 않은
#    **원본 인코더 출력**을 넣어야 다른 소비자가 같은 공간에서 쓴다. 원본은 생성 캐시에 있다.
RAW = {}
for f in ("/data/fiftyone/frames_bank/report/sourcei_gt/gen_vectors.npz",
          f"{BANKDIR}/genfull_vectors.npz", f"{BANKDIR}/gen_vectors.npz"):
    if os.path.exists(f):
        z = np.load(f, allow_pickle=True)
        RAW.update({str(t): v for t, v in zip(z["texts"], z["vecs"])})
log(f"원본 벡터 캐시 {len(RAW):,}문장")

conn = psycopg2.connect(DSN); cur = conn.cursor()
cur.execute("SELECT entity_id FROM image_embeddings WHERE entity_type='prompt'")
have = {r[0] for r in cur}
log(f"DB 기존 prompt 벡터 {len(have):,}")
cur.execute("SELECT MIN(text), content_hash FROM bank_sentences GROUP BY content_hash")
db_text = {h: t for t, h in cur.fetchall()}

plan = []
for b in BANKS:
    z = np.load(b["npz"], allow_pickle=True)
    texts = [str(x) for x in z["text"]]; cls = [str(x) for x in z["cls"]]
    hashes = [content_hash(t) for t in texts]
    dup = len(hashes) - len(set(hashes))
    need_vec = []
    for i, (t, h) in enumerate(zip(texts, hashes)):
        if h in have: continue
        v = RAW.get(t)
        if v is None:
            log(f"  ⚠️ {b['tag']}: 원본 벡터 없음 — {t[:60]!r}"); continue
        need_vec.append((h, np.asarray(v, dtype=np.float32)))
    # 같은 실행 안 중복 제거
    seen = set(); need_vec = [(h, v) for h, v in need_vec if not (h in seen or seen.add(h))]
    conflict = [h for h, t in zip(hashes, texts)
                if h in db_text and norm_text(db_text[h]) != norm_text(t)]
    plan.append(dict(bank=b, texts=texts, cls=cls, hashes=hashes,
                     need_vec=need_vec, dup=dup, conflict=len(conflict)))
    log(f"{b['tag']}: 문장 {len(texts):,} · 해시 중복 {dup} · 벡터 신규 {len(need_vec):,} · "
        f"해시 충돌(같은 해시 다른 텍스트) {len(conflict)}")

if not APPLY:
    log("DRY-RUN — --apply 로 실제 등록"); print("DONE"); sys.exit(0)

for p in plan:
    b = p["bank"]
    if p["need_vec"]:
        execute_batch(cur, """
            INSERT INTO image_embeddings (embedding_id, entity_type, entity_id, model_name, dim, embedding)
            VALUES (%s, 'prompt', %s, %s, 1024, %s)
            ON CONFLICT DO NOTHING""",
            [(f"prompt:{h}:{MODEL}", h, MODEL, "[" + ",".join(f"{x:.6f}" for x in v) + "]")
             for h, v in p["need_vec"]], page_size=200)
        log(f"  {b['tag']}: image_embeddings {len(p['need_vec']):,} 삽입")
    cur.execute("SELECT bank_id FROM prompt_banks WHERE version_tag=%s", (b["tag"],))
    row = cur.fetchone()
    if row:
        bank_id = row[0]
        cur.execute("DELETE FROM bank_sentences WHERE bank_id=%s", (bank_id,))
        log(f"  {b['tag']}: 기존 뱅크 재등록 (문장 삭제)")
    else:
        bank_id = str(uuid.uuid4())
        cur.execute("""INSERT INTO prompt_banks
            (bank_id, version_tag, source, sentence_storage, origin_uri, model_name,
             sentence_count, ingested_by, notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,'optbank_db_register.py',%s)""",
            (bank_id, b["tag"], b["source"], "db_backed", b["npz"], MODEL, len(p["texts"]), b["note"]))
        log(f"  {b['tag']}: prompt_banks 신규 {bank_id}")
    # sentence_id 는 기본값이 없어 직접 만든다. origin 은 자유 텍스트(제약 없음, 기존은 전부
    # 'userwatch') — 출처를 구분할 수 있게 뱅크별로 기록한다. adopted 는 선택된 문장이므로 참.
    src_tag = {"internal": "llm-generated", "hybrid": "mixed-curated"}.get(b["source"], "userwatch")
    execute_batch(cur, """
        INSERT INTO bank_sentences (sentence_id, bank_id, content_hash, class_label, text, gidx, origin, adopted)
        VALUES (%s,%s,%s,%s,%s,%s,%s,TRUE) ON CONFLICT DO NOTHING""",
        # ⚠️ `bank_sentences.gidx` 는 **뱅크-로컬 행 번호**(0..n-1)다. 전역 gidx 를 넣으면
        #    compare 패널이 `전역 gidx % 100000` 로 조인하다 0행이 나와 조용히
        #    `external_only` 폴백으로 떨어진다 (2026-08-28 실측). 전역값은 gidx0 + i.
        [(str(uuid.uuid4()), bank_id, h, c, t, i, src_tag)
         for i, (h, c, t) in enumerate(zip(p["hashes"], p["cls"], p["texts"]))], page_size=500)
    cur.execute("UPDATE prompt_banks SET sentence_count=%s, sentence_storage='db_backed' WHERE bank_id=%s",
                (len(p["texts"]), bank_id))
    log(f"  {b['tag']}: bank_sentences {len(p['texts']):,} 삽입")
conn.commit()

# ── 검증 ────────────────────────────────────────────────────────────
for p in plan:
    tag = p["bank"]["tag"]
    cur.execute("""SELECT count(*), count(DISTINCT s.content_hash),
                          count(e.entity_id)
                   FROM bank_sentences s JOIN prompt_banks b USING(bank_id)
                   LEFT JOIN image_embeddings e ON e.entity_type='prompt' AND e.entity_id=s.content_hash
                   WHERE b.version_tag=%s""", (tag,))
    n, nh, nv = cur.fetchone()
    log(f"검증 {tag}: 문장 {n:,} · 고유해시 {nh:,} · 벡터 연결 {nv:,} ({nv/max(n,1):.1%})")
# 왕복 검증: DB 벡터가 원본과 같은가
z = np.load(f"{BANKDIR}/genfull_bank.npz", allow_pickle=True)
ok = 0
for t in [str(x) for x in z["text"]][:5]:
    h = content_hash(t)
    cur.execute("SELECT embedding::text FROM image_embeddings WHERE entity_type='prompt' AND entity_id=%s", (h,))
    r = cur.fetchone()
    if not r: continue
    w = np.fromstring(r[0].strip("[]"), sep=",", dtype=np.float32); w /= np.linalg.norm(w)
    v = np.asarray(RAW[t], np.float32); v /= np.linalg.norm(v)
    c = float(v @ w); ok += c > 0.9999
    log(f"  왕복 cos {c:.8f}  {t[:52]!r}")
log(f"왕복 검증 {ok}/5 통과")
json.dump(dict(banks=[dict(tag=p["bank"]["tag"], n=len(p["texts"]), new_vectors=len(p["need_vec"]),
                           hash_dup=p["dup"], hash_conflict=p["conflict"]) for p in plan]),
          open(f"{BANKDIR}/db_register.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
