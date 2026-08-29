#!/usr/bin/env python3
"""새 뱅크 2종(vOPT·vGEN)을 **뱅크 npz 규약**으로 내보낸다 — probecache/attach 가 읽게.

규약(`repair_bank_prompts.load_bank`): `vec`(float32 N×1024) · `cls`(int64 N) · `prompt`(str N).

⚠️ 벡터는 **원본 인코더 출력**을 써야 한다. `optbank/*.npz` 에 저장된 건 전역 문장평균을
   제거한 판이라 프레임 임베딩과 같은 공간이 아니다 — 그걸 넣으면 코사인이 전부 어긋난다.
   원본은 DB `image_embeddings`(entity_type='prompt') 에 있고 왕복 cos 검증을 통과한 정본이다.
"""
import sys, os, hashlib, re, json
sys.path.insert(0, "/workspace")
import numpy as np, psycopg2

OUTDIR = "/data/fiftyone/sourceh/prompts"
CLASS_IDX = {"normal": 0, "falldown": 1, "fire": 2, "smoke": 3}
BANKDIR = "/data/fiftyone/frames_bank/report/sourcei_gt/optbank"
SRC = {"vOPT.2026.08.28": f"{BANKDIR}/optbank_vectors.npz",
       "vGEN.2026.08.28": f"{BANKDIR}/genfull_bank.npz"}
APPLY = "--apply" in sys.argv

def norm(s): return re.sub(r"\s+", " ", str(s).strip().lower())
def chash(t): return hashlib.sha256(norm(t).encode()).hexdigest()[:16]

cur = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
out = {}
for ver, path in SRC.items():
    z = np.load(path, allow_pickle=True)
    text = [str(x) for x in z["text"]]; cls = [str(x) for x in z["cls"]]
    hs = [chash(t) for t in text]
    cur.execute("""SELECT entity_id, embedding::text FROM image_embeddings
                   WHERE entity_type='prompt' AND entity_id = ANY(%s)""", (list(set(hs)),))
    got = {h: np.fromstring(v.strip("[]"), sep=",", dtype=np.float32) for h, v in cur.fetchall()}
    miss = [t for t, h in zip(text, hs) if h not in got]
    V = np.zeros((len(text), 1024), np.float32)
    for i, h in enumerate(hs):
        v = got.get(h)
        if v is not None: V[i] = v / max(np.linalg.norm(v), 1e-9)
    C = np.array([CLASS_IDX[c] for c in cls], np.int64)
    # 저장본과의 공간 차이를 눈으로 확인 — 같으면 잘못 고른 것이다
    stored = z["vecs"].astype(np.float32)
    st = stored / np.maximum(np.linalg.norm(stored, axis=1, keepdims=True), 1e-9)
    cos = float(np.mean(np.sum(V * st, axis=1)))
    print(f"{ver}: 문장 {len(text):,} · DB 벡터 확보 {len(text)-len(miss):,} · 미보유 {len(miss)} "
          f"· 저장본(중심제거)과 평균 cos {cos:.4f}")
    assert not miss, f"{ver}: DB 벡터 미보유 {len(miss)}건 — 등록부터 하라"
    out[ver] = (V, C, text)

if not APPLY:
    print("DRY-RUN — --apply 로 기록"); sys.exit(0)
for ver, (V, C, text) in out.items():
    p = f"{OUTDIR}/{ver}.npz"
    np.savez_compressed(p, vec=V, cls=C, prompt=np.array(text, dtype=object))
    print("기록", p, os.path.getsize(p) // 1024, "KiB")
