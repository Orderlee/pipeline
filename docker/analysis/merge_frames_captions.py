"""`frames_full`(이미지) + `captions`(텍스트) → 공유 임베딩 공간 단일 데이터셋 `frames`.

⚠️ 2026-08-19 개명으로 이 스크립트의 산출물 이름이 `frames_captions` → **`frames`(정본)** 이
됐다. 아래 "union 은 멱등하지 않다"가 그대로 유효하다 — 재실행하면 기존 `frames` 를
**삭제하고 처음부터** 만든다. enrich/reembed/뱅크평가가 얹은 필드도 같이 사라지므로,
전체 재빌드 의도가 아니면 실행하지 말 것. (`MFC_TARGET` 로 다른 이름에 만들 수 있다.)
※ 입력 `frames_full` 은 이 개명과 무관한 별개 데이터셋이다 — 정본 `frames` 가 아니다.

## 왜 이 방식인가 (2026-07-28 실측)

"통합" 은 세 가지로 읽히는데 데이터가 두 개를 죽인다:

  1. **프레임에 캡션 임베딩을 필드로 붙이기** — `frames_full` 187,994개 중 캡션이 있는
     프레임은 **264개(0.1%)** 뿐. 프레임 추출 대상과 Gemini 캡션 대상이 거의 겹치지 않아
     (asset 교집합 481) 쌍이 성립하지 않는다. → 무의미
  2. **캡션 키프레임을 프레임 샘플로 추가** — ffmpeg 로 뽑은 키프레임은 `image_embeddings`
     에 없어 벡터가 없다. 벡터 없는 샘플은 UMAP 에서 빠진다. → 선행 GPU 임베딩 필요
  3. **두 모달리티를 한 데이터셋에 union** ← 이것만 지금 가능

PE-Core-L14-336 은 이미지·텍스트를 **같은 1024-d 공간**에 넣는 CLIP 계열이라, union 하면
"캡션이 자기 의미의 이미지 근처에 놓이는가"를 한 화면에서 볼 수 있다. 이게 임베딩 유의미성의
직접적인 육안 검증이다. `modality` 필드로 Color by 하면 두 모달리티가 정렬돼 있는지
(섞임) 아니면 각자 섬을 이루는지(modality gap) 바로 드러난다.

> ⚠️ 결과 UMAP 은 **모달리티 혼합 지도**다. 이미지끼리의 거리와 이미지-텍스트 거리가
> 같은 척도로 보이지만 CLIP 계열은 일반적으로 modality gap 이 있어 두 덩어리로 갈리는
> 경우가 많다. 갈린다고 임베딩이 나쁜 게 아니다 — 정렬 정도는 `caption_img_sim` 같은
> 쌍별 지표로 봐야 한다.

## 자원 (호스트는 prod·타 사용자와 공유)

  - 복제는 **mongo 서버사이드** `clone()` — 188K 임베딩을 파이썬으로 왕복시키지 않는다
  - UMAP 은 샘플-fit → 배치 transform, PCA 는 IncrementalPCA
  - 배치마다 `MemAvailable` 확인 → 하한 밑이면 대기, 계속 낮으면 중단(재실행 시 재개)
  - BLAS 스레드 캡 + `os.nice`

env:
  MFC_TARGET        결과 데이터셋명   기본 'frames'  (2026-08-19 개명 전 'frames_captions')
  MFC_FRAMES        기본 'frames_full'
  MFC_CAPTIONS      기본 'captions'
  MFC_FIT           UMAP fit 샘플     기본 30000
  MFC_TBATCH        배치 크기         기본 10000
  MFC_MIN_AVAIL_MB  메모리 하한       기본 4000
  MFC_MAX_THREADS   BLAS 캡           기본 코어/4
  MFC_NICE          기본 10
  MFC_SKIP_VIZ      1=union 만 하고 UMAP 생략
"""

import os

_MAX_THREADS = int(os.environ.get("MFC_MAX_THREADS", str(max(1, (os.cpu_count() or 4) // 4))))
for _v in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "NUMBA_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
):
    os.environ.setdefault(_v, str(_MAX_THREADS))

import gc
import random
import time

import numpy as np

import fiftyone as fo
import fiftyone.brain as fob

TARGET = os.getenv("MFC_TARGET", "frames")
FRAMES = os.getenv("MFC_FRAMES", "frames_full")
CAPTIONS = os.getenv("MFC_CAPTIONS", "captions")
FIT = int(os.getenv("MFC_FIT", "30000"))
TBATCH = int(os.getenv("MFC_TBATCH", "10000"))
MIN_AVAIL_MB = int(os.getenv("MFC_MIN_AVAIL_MB", "4000"))
NICE = int(os.getenv("MFC_NICE", "10"))
SKIP_VIZ = os.getenv("MFC_SKIP_VIZ", "0").strip() in ("1", "true", "yes")

T0 = time.time()


def log(msg):
    print(f"[mfc +{time.time() - T0:6.0f}s] {msg}", flush=True)


try:
    os.nice(NICE)
except Exception as exc:  # noqa: BLE001
    log(f"nice 실패: {exc!r}")

try:
    import threadpoolctl

    _tp = threadpoolctl.threadpool_limits(_MAX_THREADS)
except Exception:  # noqa: BLE001
    _tp = None


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
    raise RuntimeError(f"MemAvailable 이 {MIN_AVAIL_MB}MB 밑에 머묾 — 중단 (재실행하면 처음부터)")


def batches(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


src_f = fo.load_dataset(FRAMES)
src_c = fo.load_dataset(CAPTIONS)
log(f"source: {FRAMES} n={src_f.count()} / {CAPTIONS} n={src_c.count()}")

if fo.dataset_exists(TARGET):
    log(f"기존 {TARGET} 삭제 (union 은 멱등하지 않아 재실행 시 처음부터)")
    fo.delete_dataset(TARGET)

# ── 1. 프레임 복제: mongo 서버사이드 (파이썬으로 임베딩 왕복 금지) ──────────────
wait_for_memory()
log(f"{FRAMES} → {TARGET} 서버사이드 복제 시작")
ds = src_f.clone(TARGET, persistent=True)
log(f"복제 완료 n={ds.count()} avail={mem_avail_mb()}MB")

# 복제된 brain run 은 union 전 기준이라 무효 → 제거하고 뒤에서 다시 만든다
for key in list(ds.list_brain_runs()):
    ds.delete_brain_run(key)
log("복제된 brain run 제거")

# ── 2. 캡션 추가 ───────────────────────────────────────────────────────────────
wait_for_memory()
# 추가 전 id 를 기억해 두면 모달리티 구분이 확실해진다. 필드 기반 판별은 위험하다:
# add_collection 후 프레임 샘플의 has_keyframe 이 null 로 쓰이면 `$exists:true` 에
# 걸려버리고, caption 필드는 프레임에도 있다(264건 non-empty).
frame_ids = set(ds.values("id"))
before = len(frame_ids)
ds.add_collection(src_c)
log(f"{CAPTIONS} 추가: {before} → {ds.count()}")

# ── 3. modality 필드 (Color by 로 두 모달리티 정렬 여부 확인) ──────────────────
all_ids = ds.values("id")
cap_ids = {sid for sid in all_ids if sid not in frame_ids}
log(f"modality: caption={len(cap_ids)} frame={len(all_ids) - len(cap_ids)}")
# 교차 검증 — 캡션 미디어는 media/captions/ 아래에 있다
_mismatch = sum(
    1
    for sid, fpth in zip(all_ids, ds.values("filepath"))
    if (("/captions/" in (fpth or "")) != (sid in cap_ids))
)
if _mismatch:
    log(f"  ⚠️ filepath 기준과 {_mismatch}건 불일치 — id 차집합 기준을 사용")
for id_batch in batches(all_ids, 20000):
    ds.set_values(
        "modality",
        {sid: ("caption" if sid in cap_ids else "frame") for sid in id_batch},
        key_field="id",
    )
    gc.collect()
log("modality 필드 완료")

if SKIP_VIZ:
    log(f"MFC_SKIP_VIZ=1 → UMAP 생략. UNION DONE n={ds.count()}")
    raise SystemExit(0)


# ── 4. 공유 공간 UMAP/PCA (배치) ───────────────────────────────────────────────
def embeddings_of(id_batch) -> np.ndarray:
    return np.asarray(ds.select(id_batch, ordered=True).values("embedding"), dtype="float32")


ordered_ids = ds.values("id")  # points= 는 이 순서에 정렬돼야 한다
n = len(ordered_ids)
log(f"UMAP/PCA 대상 n={n}")

try:
    import umap

    reducer = umap.UMAP(n_components=2, metric="cosine", low_memory=True, verbose=False)
    random.seed(42)
    fit_ids = [ordered_ids[i] for i in sorted(random.sample(range(n), min(FIT, n)))]
    wait_for_memory()
    Xf = embeddings_of(fit_ids)
    log(f"UMAP fit on {Xf.shape}")
    reducer.fit(Xf)
    del Xf, fit_ids
    gc.collect()

    pts = np.empty((n, 2), dtype="float32")
    off = 0
    for id_batch in batches(ordered_ids, TBATCH):
        wait_for_memory()
        X = embeddings_of(id_batch)
        pts[off : off + len(id_batch)] = reducer.transform(X)
        off += len(id_batch)
        del X
        gc.collect()
        log(f"  UMAP transform {off}/{n} avail={mem_avail_mb()}MB")
    fob.compute_visualization(ds, points=pts, brain_key="emb_viz")
    log("emb_viz 등록 (모달리티 혼합 공유공간)")
    del pts
    gc.collect()
except Exception as exc:  # noqa: BLE001 — 투영 실패해도 union 은 보존
    log(f"UMAP skipped: {exc!r}")

try:
    from sklearn.decomposition import IncrementalPCA

    ipca = IncrementalPCA(n_components=2)
    for id_batch in batches(ordered_ids, TBATCH):
        wait_for_memory()
        X = embeddings_of(id_batch)
        if len(X) >= 2:
            ipca.partial_fit(X)
        del X
        gc.collect()
    pts = np.empty((n, 2), dtype="float32")
    off = 0
    for id_batch in batches(ordered_ids, TBATCH):
        wait_for_memory()
        X = embeddings_of(id_batch)
        pts[off : off + len(id_batch)] = ipca.transform(X)
        off += len(id_batch)
        del X
        gc.collect()
    fob.compute_visualization(ds, points=pts, brain_key="emb_viz_pca")
    log("emb_viz_pca 등록")
    del pts
    gc.collect()
except Exception as exc:  # noqa: BLE001
    log(f"PCA skipped: {exc!r}")

log(f"MERGE DONE dataset={TARGET} n={ds.count()} avail={mem_avail_mb()}MB")
