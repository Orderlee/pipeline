"""frames_full(이미 add+라벨 완료)에 UMAP/PCA brain run 만 재개 — 전체 빌드 중 UMAP 단계만 재실행용.

env: FFB_DATASET(기본 frames_full), FFB_FIT(UMAP fit 샘플, 기본 50000)
완료 시 /tmp/umap_resume.done 생성(폴러 신호).
"""

import os
import time

import numpy as np

import fiftyone as fo
import fiftyone.brain as fob

T0 = time.time()


def log(m):
    print(f"[umap +{time.time() - T0:6.0f}s] {m}", flush=True)


DATASET = os.getenv("FFB_DATASET", "frames_full")
FIT = int(os.getenv("FFB_FIT", "50000"))

try:
    os.path.exists("/tmp/umap_resume.done") and os.remove("/tmp/umap_resume.done")
except OSError:
    pass

ds = fo.load_dataset(DATASET)
n = ds.count()
log(f"{DATASET} n={n} brain(before)={ds.list_brain_runs()}")

log("loading embeddings (ds order = points 정렬)...")
X = np.asarray(ds.values("embedding"), dtype="float32")
log(f"X={X.shape}")

import random  # noqa: E402

import umap  # noqa: E402 — umap-learn

random.seed(42)
reducer = umap.UMAP(n_components=2, metric="cosine", low_memory=True, verbose=True)
if n > FIT:
    idx = sorted(random.sample(range(n), FIT))
    log(f"UMAP fit on {FIT} sample...")
    reducer.fit(X[idx])
    pts = np.empty((n, 2), dtype="float32")
    for i in range(0, n, 20000):
        pts[i : i + 20000] = reducer.transform(X[i : i + 20000])
    log("transformed all (batched)")
else:
    pts = reducer.fit_transform(X)
    log("UMAP full fit")

fob.compute_visualization(ds, points=pts, brain_key="emb_viz")
log("emb_viz registered")
try:
    fob.compute_visualization(ds, embeddings="embedding", method="pca", brain_key="emb_viz_pca")
    log("emb_viz_pca registered")
except Exception as exc:  # noqa: BLE001
    log(f"pca skipped: {exc!r}")

log(f"UMAP_DONE brain={ds.list_brain_runs()}")
with open("/tmp/umap_resume.done", "w") as f:
    f.write("ok")
