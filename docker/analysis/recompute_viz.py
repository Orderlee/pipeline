"""임의 FiftyOne 데이터셋의 시각화(UMAP/PCA)만 배치로 재계산 — 데이터셋 재빌드 없음.

임베딩 필드 값이 바뀌었을 때(예: 캡션을 영어로 재임베딩) 기존 brain run 은 옛 벡터
기준이라 무효가 된다. 이 스크립트는 **샘플만 그대로 두고 투영만** 다시 만든다.

`points=` 는 samples 기본 순서에 정렬돼야 하므로(sample_ids 인자 없음) `values("id")`
순서로 배치를 만들어 같은 순서로 채운다. 200K 에서 전량 fit 은 메모리상 불가하므로
UMAP 은 샘플-fit → 배치 transform, PCA 는 IncrementalPCA 를 쓴다.

env: RV_DATASET(필수) RV_FIELD(embedding) RV_KEY(emb_viz) RV_FIT(30000)
     RV_TBATCH(10000) RV_MIN_AVAIL_MB(2000) RV_NICE(10) RV_PCA(1)
"""

import os

_MT = int(os.environ.get("RV_MAX_THREADS", str(max(1, (os.cpu_count() or 4) // 4))))
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "NUMBA_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ.setdefault(_v, str(_MT))

import gc
import random
import time

import numpy as np

import fiftyone as fo
import fiftyone.brain as fob

DATASET = os.environ["RV_DATASET"]
FIELD = os.getenv("RV_FIELD", "embedding")
KEY = os.getenv("RV_KEY", "emb_viz")
FIT = int(os.getenv("RV_FIT", "30000"))
TBATCH = int(os.getenv("RV_TBATCH", "10000"))
MIN_AVAIL_MB = int(os.getenv("RV_MIN_AVAIL_MB", "2000"))
DO_PCA = os.getenv("RV_PCA", "1") not in ("0", "false", "no")
T0 = time.time()


def log(m):
    print(f"[rv +{time.time() - T0:6.0f}s] {m}", flush=True)


try:
    os.nice(int(os.getenv("RV_NICE", "10")))
except Exception as exc:  # noqa: BLE001
    log(f"nice 실패: {exc!r}")


def avail():
    try:
        with open("/proc/meminfo") as fh:
            for ln in fh:
                if ln.startswith("MemAvailable:"):
                    return int(ln.split()[1]) // 1024
    except Exception:  # noqa: BLE001
        pass
    return 1 << 30


def wait_mem(tries=20, s=15):
    for i in range(tries):
        if avail() >= MIN_AVAIL_MB:
            return
        gc.collect()
        log(f"  ⏸ MemAvailable={avail()}MB < {MIN_AVAIL_MB}MB — 대기 {i + 1}/{tries}")
        time.sleep(s)
    raise RuntimeError("MemAvailable 하한 미달 지속 — 중단")


def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


ds = fo.load_dataset(DATASET)
ids = ds.values("id")
n = len(ids)
log(f"{DATASET} n={n} field={FIELD} key={KEY}")


def X(b):
    return np.asarray(ds.select(b, ordered=True).values(FIELD), dtype="float32")


def fill(transform):
    pts = np.empty((n, 2), dtype="float32")
    off = 0
    for b in chunks(ids, TBATCH):
        wait_mem()
        pts[off : off + len(b)] = transform(X(b))
        off += len(b)
        gc.collect()
    return pts


import umap  # noqa: E402 — 스레드 캡 이후에 import

red = umap.UMAP(n_components=2, metric="cosine", low_memory=True, verbose=False)
random.seed(42)
fit_ids = [ids[i] for i in sorted(random.sample(range(n), min(FIT, n)))]
wait_mem()
Xf = X(fit_ids)
log(f"UMAP fit on {Xf.shape}")
red.fit(Xf)
del Xf, fit_ids
gc.collect()
pts = fill(red.transform)
if KEY in ds.list_brain_runs():
    ds.delete_brain_run(KEY)
fob.compute_visualization(ds, points=pts, brain_key=KEY)
log(f"{KEY} 등록")
del pts
gc.collect()

if DO_PCA:
    from sklearn.decomposition import IncrementalPCA

    ip = IncrementalPCA(n_components=2)
    for b in chunks(ids, TBATCH):
        wait_mem()
        xb = X(b)
        if len(xb) >= 2:
            ip.partial_fit(xb)
        del xb
        gc.collect()
    pts = fill(ip.transform)
    pk = f"{KEY}_pca"
    if pk in ds.list_brain_runs():
        ds.delete_brain_run(pk)
    fob.compute_visualization(ds, points=pts, brain_key=pk)
    log(f"{pk} 등록")

log(f"RECOMPUTE DONE {DATASET} brain={ds.list_brain_runs()} avail={avail()}MB")
