"""`frames_captions` 에 모달리티별 명시 필드 3종 추가 — 배치·저부하·재개 가능.

## 왜 필요한가

통합 데이터셋의 `embedding` 은 **모달리티 혼합**이다: 프레임 샘플은 이미지 벡터,
캡션 샘플은 텍스트 벡터가 같은 필드명에 들어 있다 (검증: 저장벡터 vs 그 샘플 caption 의
`_embed_text` cosine → captions 1.0000 / frames_full 0.158). 어느 쪽인지 필드명으로
알 수 없어 사이드바에서 혼동된다. 그래서 **명시 필드 3종**을 만든다:

  - `image_embedding`   — 이미지 벡터 (프레임: 자기 자신 / 캡션: 키프레임을 새로 임베딩)
  - `caption_embedding` — 캡션 텍스트 벡터 (캡션: 자기 자신 / 프레임: 자기 영상 캡션 centroid)
  - `caption_img_sim`   — 위 둘의 cosine (둘 다 있는 샘플만, 나머지 None)

## 커버리지 현실 (실측)

  - 프레임 187,994개 중 캡션이 있는 것은 **264개(0.1%)** — 프레임 추출 대상과 Gemini 캡션
    대상이 거의 안 겹친다(asset 교집합 481). 프레임 쪽 `caption_embedding` 은 이 정도만 찬다.
  - 캡션 11,978개는 `backfill_caption_keyframes.py` 로 전부 실제 키프레임을 갖게 됐지만
    그 이미지들은 `image_embeddings` 에 없다 → **embedding-service `/embed` 로 새로 임베딩**.
    asset 당 1회만 호출해 그 asset 의 모든 캡션이 재사용한다(4,224회 ≈ 캡션 11,978건 커버).

## ⚠️ 해석 주의 — caption_img_sim 값을 신뢰하지 말 것

별도 조사(2026-07-28)에서 **캡션 임베딩이 붕괴**해 있음이 확인됐다: effective rank
**1.5/1024**, 상위 1방향이 분산 94.6%, 무관한 한국어 캡션끼리 pairwise cos **0.951**.
원인은 토큰 절단이 아니라 **PE-Core 텍스트 타워가 한국어를 못 읽는 것**(번역쌍 gap 0.014≈0,
KO→EN R@1 0.10~0.13, 같은 의미 영어 문장끼리는 0.567).

→ 이 스크립트는 필드를 **채우기만** 한다. 값의 판별력은 캡션을 **영어로 재임베딩**한 뒤에야
생긴다. 지금 값으로 순위·임계 판정을 하면 안 된다.

## 자원 예의
  배치 단위 처리 + `MemAvailable` 가드 + BLAS 캡 + `os.nice`.
  GPU(embedding-service)는 dagster torch 와 GPU0 공유 → 병렬도 낮게.

env:
  EFC_DATASET       기본 'frames_captions'
  EFC_BATCH         샘플 배치        기본 5000
  EFC_WORKERS       /embed 병렬      기본 3
  EFC_MIN_AVAIL_MB  메모리 하한      기본 3000
  EFC_NICE          기본 10
  EFC_SKIP_GPU      1=키프레임 임베딩 생략 (필드 분리만)
"""

import os

_MAX_THREADS = int(os.environ.get("EFC_MAX_THREADS", str(max(1, (os.cpu_count() or 4) // 4))))
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "NUMBA_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ.setdefault(_v, str(_MAX_THREADS))

import gc
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fiftyone as fo

import fiftyone_pgvector as fp

DATASET = os.getenv("EFC_DATASET", "frames_captions")
BATCH = int(os.getenv("EFC_BATCH", "5000"))
WORKERS = int(os.getenv("EFC_WORKERS", "3"))
MIN_AVAIL_MB = int(os.getenv("EFC_MIN_AVAIL_MB", "3000"))
NICE = int(os.getenv("EFC_NICE", "10"))
SKIP_GPU = os.getenv("EFC_SKIP_GPU", "0").strip() in ("1", "true", "yes")

T0 = time.time()


def log(msg):
    print(f"[efc +{time.time() - T0:6.0f}s] {msg}", flush=True)


try:
    os.nice(NICE)
except Exception as exc:  # noqa: BLE001
    log(f"nice 실패: {exc!r}")


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
    raise RuntimeError("MemAvailable 하한 미달 지속 — 중단 (재실행하면 캐시부터 재개)")


def batches(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def l2(v):
    a = np.asarray(v, dtype="float32")
    n = float(np.linalg.norm(a))
    return a / n if n > 0 else a


ds = fo.load_dataset(DATASET)
log(f"{DATASET} n={ds.count()}")

ids, mods = ds.values(["id", "modality"])
frame_ids = [i for i, m in zip(ids, mods) if m == "frame"]
cap_ids = [i for i, m in zip(ids, mods) if m == "caption"]
log(f"frame={len(frame_ids)} caption={len(cap_ids)}")

# ── 1. embedding → 모달리티별 명시 필드로 복사 ─────────────────────────────────
for name, subset in (("image_embedding", frame_ids), ("caption_embedding", cap_ids)):
    done = 0
    for b in batches(subset, BATCH):
        wait_for_memory()
        vals = ds.select(b, ordered=True).values("embedding")
        ds.set_values(name, dict(zip(b, vals)), key_field="id")
        done += len(b)
        del vals
        gc.collect()
    log(f"{name} 복사 완료 ({done}건)")

# ── 2. 프레임 샘플의 caption_embedding = 자기 영상 캡션 centroid ───────────────
# asset 당 캡션이 여러 개일 수 있어 L2 정규화 후 평균(centroid)을 쓴다.
cap_rows = fp._load_caption_embeddings()
by_asset = defaultdict(list)
for r in cap_rows:
    if r.get("asset_id"):
        by_asset[str(r["asset_id"])].append(l2(r["embedding"]))
asset_centroid = {a: l2(np.mean(v, axis=0)) for a, v in by_asset.items()}
del cap_rows, by_asset
gc.collect()
log(f"asset 캡션 centroid {len(asset_centroid)}개 준비")

filled = 0
for b in batches(frame_ids, BATCH):
    wait_for_memory()
    aids = ds.select(b, ordered=True).values("asset_id")
    upd = {}
    for sid, aid in zip(b, aids):
        c = asset_centroid.get(str(aid)) if aid else None
        if c is not None:
            upd[sid] = c.tolist()
    if upd:
        ds.set_values("caption_embedding", upd, key_field="id")
        filled += len(upd)
    del aids, upd
    gc.collect()
log(f"프레임 caption_embedding 채움: {filled} / {len(frame_ids)}")

# ── 3. 캡션 샘플의 image_embedding = 키프레임을 embedding-service 로 임베딩 ────
CACHE = os.path.join(fp.MEDIA_DIR, "captions", "_kf_vectors")
os.makedirs(CACHE, exist_ok=True)


def embed_keyframe(args):
    """asset 당 1회. 캐시(.npy)가 있으면 재사용 → 재개 가능."""
    aid, path = args
    dst = os.path.join(CACHE, f"{aid}.npy")
    if os.path.exists(dst):
        try:
            return aid, np.load(dst)
        except Exception:  # noqa: BLE001 — 손상 캐시는 다시 뽑는다
            pass
    try:
        with open(path, "rb") as fh:
            vec = fp._embed_image(fh.read(), filename=os.path.basename(path))
        a = np.asarray(vec, dtype="float32")
        np.save(dst, a)
        return aid, a
    except Exception:  # noqa: BLE001 — per-asset fail-forward
        return aid, None


if SKIP_GPU:
    log("EFC_SKIP_GPU=1 → 키프레임 임베딩 생략")
else:
    # asset → (대표 filepath, 그 asset 의 캡션 sample_id 들)
    cap_assets: dict[str, list[str]] = defaultdict(list)
    cap_path: dict[str, str] = {}
    for b in batches(cap_ids, BATCH):
        aids, fps = ds.select(b, ordered=True).values(["asset_id", "filepath"])
        for sid, aid, fpth in zip(b, aids, fps):
            if aid and fpth:
                cap_assets[str(aid)].append(sid)
                cap_path.setdefault(str(aid), fpth)
    log(f"캡션 asset {len(cap_assets)}개 → /embed 호출 대상")

    ok = fail = 0
    for ai, abatch in enumerate(batches(sorted(cap_assets), 200), 1):
        wait_for_memory()
        work = [(a, cap_path[a]) for a in abatch if a in cap_path]
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            res = list(ex.map(embed_keyframe, work))
        upd = {}
        for aid, vec in res:
            if vec is None:
                fail += 1
                continue
            ok += 1
            v = vec.tolist()
            for sid in cap_assets[aid]:
                upd[sid] = v
        if upd:
            ds.set_values("image_embedding", upd, key_field="id")
        del res, upd
        gc.collect()
        log(f"  keyframe embed batch {ai} asset ok={ok} fail={fail} avail={mem_avail_mb()}MB")
    log(f"캡션 image_embedding 완료 asset ok={ok} fail={fail}")

# ── 4. caption_img_sim = cosine(caption_embedding, image_embedding) ────────────
# 기존 값(330건)은 '자기영상 best-frame' 정의였다. 데이터셋 내 정의를 하나로 맞추기 위해
# 전량 재계산한다 (둘 다 없으면 None).
sim_done = 0
for b in batches(ids, BATCH):
    wait_for_memory()
    ce, ie = ds.select(b, ordered=True).values(["caption_embedding", "image_embedding"])
    upd = {}
    for sid, c, i in zip(b, ce, ie):
        if not c or not i:
            upd[sid] = None
            continue
        cv, iv = l2(c), l2(i)
        upd[sid] = float(cv @ iv)
        sim_done += 1
    ds.set_values("caption_img_sim", upd, key_field="id")
    del ce, ie, upd
    gc.collect()
log(f"caption_img_sim 계산 완료: 값 있는 샘플 {sim_done} / {len(ids)}")

log(f"ENRICH DONE dataset={DATASET} avail={mem_avail_mb()}MB")
