"""캡션을 **영어 기준**으로 재임베딩 — 배치·저부하·재개 가능.

## 왜 (2026-07-28 실측, 파일럿 검증됨)

PE-Core 텍스트 타워가 한국어를 사실상 못 읽는다. 의미가 다른 주제(낙상/화재/통상통행/
신호위반) 캡션들의 **판별격차**(같은주제 cos − 다른주제 cos):

  한국어  같은주제 0.9567  다른주제 0.9494  →  격차 **+0.0073**  (사실상 0 = 무작위)
  영어    같은주제 0.8536  다른주제 0.7699  →  격차 **+0.0837**  (11.5배)

절대 cosine 수준은 둘 다 높지만 **검색·클러스터링이 쓰는 것은 격차**다. 한국어 벡터로는
"사람이 쓰러짐"과 "오토바이가 지나감"을 구분할 수 없다. 전역 진단도 같은 방향:
한국어 캡션 임베딩 effective rank **1.5/1024**, 상위 1방향이 분산 94.6%,
무관한 캡션끼리 pairwise cos 0.951.

→ 표시는 한국어(`caption`) 그대로 두고, **임베딩은 영어 번역문 기준**으로 만든다.

## 무엇을 쓰는가

  `caption_en`            — Gemini 번역문 (참고·검증용, 표시는 여전히 `caption`)
  `caption_embedding_ko`  — 기존 한국어 기반 벡터 **보존** (A/B 비교용)
  `caption_embedding`     — **영어 기준 새 벡터** (이후 모든 지표의 기본)
  `caption_img_sim`       — 영어 벡터 ↔ `image_embedding` cosine 재계산

프레임 샘플의 `caption_embedding`(자기 영상 캡션 centroid)도 영어 벡터로 다시 만든다.

## 효율

11,978 캡션이 **고유 문장 6,999개**(중복 42%)다. 번역·임베딩은 고유 문장당 1회만 하고
디스크 캐시(`_caption_en.json`, `_en_vectors/*.npy`)에 남겨 중단 후 재개한다.

## 자원 예의
  번역은 Gemini API(네트워크), 임베딩은 embedding-service. 둘 다 병렬도 낮게.
  배치마다 `MemAvailable` 가드 + BLAS 캡 + `os.nice`.

env:
  RCE_DATASET      기본 'frames_captions'
  RCE_TR_WORKERS   번역 병렬     기본 6
  RCE_EM_WORKERS   임베딩 병렬   기본 4
  RCE_BATCH        샘플 배치     기본 5000
  RCE_LIMIT        고유문장 상한 기본 0(=전체)
  RCE_MIN_AVAIL_MB 기본 3000
  RCE_NICE         기본 10
"""

import os

_MAX_THREADS = int(os.environ.get("RCE_MAX_THREADS", str(max(1, (os.cpu_count() or 4) // 4))))
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "NUMBA_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ.setdefault(_v, str(_MAX_THREADS))

import gc
import hashlib
import json
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fiftyone as fo

import fiftyone_pgvector as fp

DATASET = os.getenv("RCE_DATASET", "frames_captions")
TR_WORKERS = int(os.getenv("RCE_TR_WORKERS", "6"))
EM_WORKERS = int(os.getenv("RCE_EM_WORKERS", "4"))
BATCH = int(os.getenv("RCE_BATCH", "5000"))
LIMIT = int(os.getenv("RCE_LIMIT", "0"))
MIN_AVAIL_MB = int(os.getenv("RCE_MIN_AVAIL_MB", "3000"))
NICE = int(os.getenv("RCE_NICE", "10"))

T0 = time.time()


def log(msg):
    print(f"[rce +{time.time() - T0:6.0f}s] {msg}", flush=True)


try:
    os.nice(NICE)
except Exception as exc:  # noqa: BLE001
    log(f"nice 실패: {exc!r}")

CACHE_DIR = os.path.join(fp.MEDIA_DIR, "captions")
TR_CACHE = os.path.join(CACHE_DIR, "_caption_en.json")
VEC_DIR = os.path.join(CACHE_DIR, "_en_vectors")
os.makedirs(VEC_DIR, exist_ok=True)


def mem_avail_mb() -> int:
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) // 1024
    except Exception:  # noqa: BLE001
        pass
    return 1 << 30


def wait_for_memory(tries: int = 20, sleep_s: int = 15):
    for i in range(tries):
        if mem_avail_mb() >= MIN_AVAIL_MB:
            return
        gc.collect()
        log(f"  ⏸ MemAvailable={mem_avail_mb()}MB < {MIN_AVAIL_MB}MB — 대기 {i + 1}/{tries}")
        time.sleep(sleep_s)
    raise RuntimeError("MemAvailable 하한 미달 지속 — 중단 (캐시부터 재개 가능)")


def batches(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def l2(v):
    a = np.asarray(v, dtype="float32")
    n = float(np.linalg.norm(a))
    return a / n if n > 0 else a


def key_of(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# ── 1. 고유 한국어 캡션 수집 ───────────────────────────────────────────────────
with fp._pg_conn() as conn, conn.cursor() as cur:
    cur.execute(
        """
        SELECT DISTINCT text_content FROM image_embeddings
        WHERE entity_type='caption' AND text_content IS NOT NULL AND text_content <> ''
        """
    )
    uniq = sorted({r[0] for r in cur.fetchall()})
if LIMIT:
    uniq = uniq[:LIMIT]
log(f"고유 한국어 캡션 {len(uniq)}개")

# ── 2. 번역 (디스크 캐시 → 재개 가능) ──────────────────────────────────────────
tr: dict[str, str] = {}
if os.path.exists(TR_CACHE):
    try:
        with open(TR_CACHE, encoding="utf-8") as fh:
            tr = json.load(fh)
        log(f"번역 캐시 {len(tr)}건 로드")
    except Exception as exc:  # noqa: BLE001 — 손상 캐시는 무시하고 새로 만든다
        log(f"번역 캐시 무시: {exc!r}")

todo = [t for t in uniq if t not in tr]
log(f"번역 필요 {len(todo)}건")


_HANGUL = re.compile(r"[가-힣]")


def translate(t):
    """Gemini 번역 + **검증·재시도**.

    ⚠️ `fp.translate_query_ko_en()` 을 쓰면 안 된다 — Vertex 호출이 실패하면 조용히
    `_dict_substitute()`(사전 단어치환)로 폴백해 "3명의 pedestrian 가 crosswalk 를
    건너는 모습" 같은 반쪽 번역을 반환한다. 실측 **19.9%** 가 이렇게 오염됐고, 그대로
    임베딩하면 한국어 붕괴를 그대로 물려받는다. 그래서 `_vertex_translate()` 를 직접
    호출하고(실패 시 None), **한글이 남은 결과는 실패로 간주**해 재시도한다.
    """
    for attempt in range(3):
        try:
            en = fp._vertex_translate(t)
        except Exception:  # noqa: BLE001 — per-item fail-forward
            en = None
        if en:
            en = en.strip()
            if en and not _HANGUL.search(en):
                return t, en
        time.sleep(1.5 * (attempt + 1))  # rate limit 백오프
    return t, None


ok = fail = 0
for bi, b in enumerate(batches(todo, 300), 1):
    wait_for_memory()
    with ThreadPoolExecutor(max_workers=TR_WORKERS) as ex:
        for t, en in ex.map(translate, b):
            if en:
                tr[t] = en
                ok += 1
            else:
                fail += 1
    with open(TR_CACHE, "w", encoding="utf-8") as fh:  # 배치마다 저장 → 중단 안전
        json.dump(tr, fh, ensure_ascii=False)
    log(f"  번역 batch {bi}/{(len(todo) + 299) // 300} ok={ok} fail={fail}")
log(f"번역 완료 (캐시 총 {len(tr)}건, 실패 {fail})")


# ── 3. 영어 문장 임베딩 (npy 캐시) ─────────────────────────────────────────────
def embed_en(en_text):
    dst = os.path.join(VEC_DIR, f"{key_of(en_text)}.npy")
    if os.path.exists(dst):
        try:
            return en_text, np.load(dst)
        except Exception:  # noqa: BLE001
            pass
    try:
        a = np.asarray(fp._embed_text(en_text), dtype="float32")
        np.save(dst, a)
        return en_text, a
    except Exception:  # noqa: BLE001 — per-item fail-forward
        return en_text, None


en_texts = sorted(set(tr.values()))
log(f"고유 영어 문장 {len(en_texts)}개 임베딩")
en_vec: dict[str, np.ndarray] = {}
e_ok = e_fail = 0
for bi, b in enumerate(batches(en_texts, 300), 1):
    wait_for_memory()
    with ThreadPoolExecutor(max_workers=EM_WORKERS) as ex:
        for t, v in ex.map(embed_en, b):
            if v is None:
                e_fail += 1
            else:
                en_vec[t] = l2(v)
                e_ok += 1
    log(f"  임베딩 batch {bi}/{(len(en_texts) + 299) // 300} ok={e_ok} fail={e_fail}")
log(f"영어 임베딩 완료 ok={e_ok} fail={e_fail}")

# 한국어 원문 → 영어 벡터
ko_vec = {ko: en_vec[en] for ko, en in tr.items() if en in en_vec}
log(f"한국어→영어벡터 매핑 {len(ko_vec)}건")

# ── 4. 데이터셋 반영 ───────────────────────────────────────────────────────────
ds = fo.load_dataset(DATASET)
ids, mods = ds.values(["id", "modality"])
cap_ids = [i for i, m in zip(ids, mods) if m == "caption"]
frame_ids = [i for i, m in zip(ids, mods) if m == "frame"]
log(f"{DATASET}: caption={len(cap_ids)} frame={len(frame_ids)}")

# 4a. 한국어 벡터 보존 — **pgvector(진리원본)에서 직접** 읽는다.
#
# ⚠️ 절대 `caption_embedding` 에서 복사하지 말 것. 이 스크립트를 두 번 돌리면
# 1회차가 `caption_embedding` 을 영어로 덮으므로, 2회차의 복사는 "영어를 한국어
# 백업으로" 저장해 **A/B 기준선을 파괴**한다 (실측 사고: 재실행 +20s 에 11,978건
# 전부 영어로 덮임). entity_id 로 pgvector 를 조회하면 몇 번 돌려도 안전하다.
ko_src: dict[str, list] = {}
with fp._pg_conn() as conn, conn.cursor() as cur:
    cur.execute(
        "SELECT entity_id, embedding FROM image_embeddings WHERE entity_type='caption'"
    )
    for eid, emb in cur.fetchall():
        ko_src[str(eid)] = fp._parse_vector(emb)
log(f"pgvector 한국어 캡션 벡터 {len(ko_src)}건 로드")

done = miss = 0
for b in batches(cap_ids, BATCH):
    wait_for_memory()
    eids = ds.select(b, ordered=True).values("entity_id")
    upd = {}
    for sid, eid in zip(b, eids):
        v = ko_src.get(str(eid))
        if v is None:
            miss += 1
        else:
            upd[sid] = v
    if upd:
        ds.set_values("caption_embedding_ko", upd, key_field="id")
        done += len(upd)
    del eids, upd
    gc.collect()
del ko_src
gc.collect()
log(f"caption_embedding_ko 보존 완료 ({done}건, 누락 {miss}) — pgvector 원본 기준")

# 4b. 캡션 샘플: caption_en + 영어 기준 caption_embedding
filled = missing = 0
for b in batches(cap_ids, BATCH):
    wait_for_memory()
    caps = ds.select(b, ordered=True).values("caption")
    en_upd, vec_upd = {}, {}
    for sid, ko in zip(b, caps):
        en = tr.get(ko or "")
        en_upd[sid] = en
        v = ko_vec.get(ko or "")
        if v is None:
            missing += 1
        else:
            vec_upd[sid] = v.tolist()
            filled += 1
    ds.set_values("caption_en", en_upd, key_field="id")
    if vec_upd:
        ds.set_values("caption_embedding", vec_upd, key_field="id")
    del caps, en_upd, vec_upd
    gc.collect()
log(f"캡션 영어 임베딩 반영: {filled} (누락 {missing})")

# 4b-2. 캡션 샘플의 `embedding`(모달리티 native = UMAP/유사검색 입력)도 영어로 교체.
# 이걸 빼면 caption_embedding 만 영어가 되고 joint UMAP·text_search 는 계속 한국어
# 벡터를 쓴다. 원본 한국어 벡터는 `caption_embedding_ko` 에 보존돼 있으므로 복구 가능.
# ⚠️ 이 시점부터 FiftyOne 의 캡션 `embedding` 은 pgvector 저장값과 달라진다(의도적).
synced = 0
for b in batches(cap_ids, BATCH):
    wait_for_memory()
    vals = ds.select(b, ordered=True).values("caption_embedding")
    upd = {sid: v for sid, v in zip(b, vals) if v}
    if upd:
        ds.set_values("embedding", upd, key_field="id")
        synced += len(upd)
    del vals, upd
    gc.collect()
log(f"캡션 embedding(native) 영어 동기화: {synced}")

# 4c. 프레임 샘플: 자기 영상 캡션의 **영어** centroid
by_asset = defaultdict(list)
with fp._pg_conn() as conn, conn.cursor() as cur:
    cur.execute(
        "SELECT asset_id, text_content FROM image_embeddings "
        "WHERE entity_type='caption' AND asset_id IS NOT NULL AND text_content IS NOT NULL"
    )
    for aid, txt in cur.fetchall():
        v = ko_vec.get(txt)
        if v is not None:
            by_asset[str(aid)].append(v)
centroid = {a: l2(np.mean(v, axis=0)) for a, v in by_asset.items()}
del by_asset
gc.collect()
log(f"asset 영어 centroid {len(centroid)}개")

f_filled = 0
for b in batches(frame_ids, BATCH):
    wait_for_memory()
    aids = ds.select(b, ordered=True).values("asset_id")
    upd = {}
    for sid, aid in zip(b, aids):
        c = centroid.get(str(aid)) if aid else None
        if c is not None:
            upd[sid] = c.tolist()
    if upd:
        ds.set_values("caption_embedding", upd, key_field="id")
        f_filled += len(upd)
    del aids, upd
    gc.collect()
log(f"프레임 caption_embedding(영어) 채움: {f_filled} / {len(frame_ids)}")

# ── 5. caption_img_sim 영어 기준 재계산 ────────────────────────────────────────
sim_n = 0
for b in batches(ids, BATCH):
    wait_for_memory()
    ce, ie = ds.select(b, ordered=True).values(["caption_embedding", "image_embedding"])
    upd = {}
    for sid, c, i in zip(b, ce, ie):
        if c and i:
            upd[sid] = float(l2(c) @ l2(i))
            sim_n += 1
        else:
            upd[sid] = None
    ds.set_values("caption_img_sim", upd, key_field="id")
    del ce, ie, upd
    gc.collect()
log(f"caption_img_sim(영어 기준) 재계산: 값 있는 샘플 {sim_n}")


# ── 6. A/B 리포트 (같은 표본에서 KO vs EN 판별력) ──────────────────────────────
def spread(A):
    n = len(A)
    S = A @ A.T
    off = (S.sum() - np.trace(S)) / (n * n - n)
    s = np.linalg.svd(A - A.mean(0), compute_uv=False)
    p = s / s.sum()
    return float(off), float(np.exp(-(p * np.log(p + 1e-12)).sum()))


try:
    sample = cap_ids[:3000]
    ko_v, en_v = ds.select(sample, ordered=True).values(
        ["caption_embedding_ko", "caption_embedding"]
    )
    pairs = [(k, e) for k, e in zip(ko_v, en_v) if k and e]
    K = np.stack([l2(k) for k, _ in pairs])
    E = np.stack([l2(e) for _, e in pairs])
    ko_off, ko_er = spread(K)
    en_off, en_er = spread(E)
    log(f"A/B (n={len(pairs)}): 한국어 pairwise={ko_off:.4f} eff.rank={ko_er:.1f}")
    log(f"A/B (n={len(pairs)}): 영어   pairwise={en_off:.4f} eff.rank={en_er:.1f}")
except Exception as exc:  # noqa: BLE001 — 리포트 실패는 무해
    log(f"A/B 리포트 skip: {exc!r}")

log(f"REEMBED DONE dataset={DATASET} avail={mem_avail_mb()}MB")
