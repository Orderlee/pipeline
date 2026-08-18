"""source-h-prompts UMAP 마무리 — 적재는 완료됐고 UMAP fit 에서 죽은 것을 이어서.

벡터는 몽고에 없으므로(슬림화) ds.values("gidx") 로 npz 에서 재구성:
gidx = 버전순번×GIDX_OFFSET + 뱅크-로컬 g → 정확 복원. PCA 64 → UMAP → brain + 워크스페이스.
"""
import glob
import os
import resource
import sys

resource.setrlimit(resource.RLIMIT_AS, (16 * 2**30, 16 * 2**30))
sys.path.insert(0, "/workspace")

_npzs = glob.glob("/data/fiftyone/sourceh/prompts/v*.npz")
_vers = [os.path.basename(p)[:-4] for p in _npzs]
_vers.sort(key=lambda v: tuple(int(x) for x in v.lstrip("v").split(".")))
os.environ["BANK_LIST"] = ",".join(_vers)

for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
           "NUMBA_NUM_THREADS"):
    os.environ.setdefault(_v, "8")

import fiftyone as fo
import numpy as np
import prompt_geometry as pg


def finish(dataset_name: str) -> None:
    ds = fo.load_dataset(dataset_name)
    n = ds.count()
    gidx_all = np.array(ds.values("gidx"), dtype=np.int64)
    assert len(gidx_all) == n
    pg.log(f"{dataset_name}: {n:,}행 — npz 에서 벡터 재구성")
    _, banks = None, {v: np.load(f"/data/fiftyone/sourceh/prompts/{v}.npz",
                                 allow_pickle=True) for v in pg.BANKS}
    E = np.zeros((n, 1024), dtype=np.float32)
    for vi, v in enumerate(pg.BANKS):
        goff = vi * pg.GIDX_OFFSET
        m = (gidx_all >= goff) & (gidx_all < goff + pg.GIDX_OFFSET)
        if m.any():
            E[m] = banks[v]["vec"][gidx_all[m] - goff]
    del banks
    from sklearn.decomposition import PCA
    E = PCA(n_components=64, svd_solver="randomized",
            random_state=42).fit_transform(E).astype(np.float32)
    pg.log(f"{dataset_name}: PCA → {E.shape}")
    import fiftyone.brain as fob
    import umap
    # init=random — spectral 은 23,929노드 컴포넌트에서 dense 4.3GB 를 만들다 죽는다 (실측)
    pts = umap.UMAP(n_components=2, metric="cosine", low_memory=True,
                    init="random", random_state=42).fit_transform(E)
    pg.log(f"{dataset_name}: UMAP 완료 — brain 기록")
    fob.compute_visualization(ds, points=pts, brain_key="emb_viz")

    sch = ds.get_field_schema()
    for wsname, color in (("prompts", "category.label"),
                          ("topk", "adopted.label"),
                          ("wave", "wave_role.label")):
        if color.split(".")[0] not in sch:
            continue
        space = fo.Space(children=[
            fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
            fo.Space(children=[fo.Panel(type="Embeddings",
                                        state={"brainResult": "emb_viz",
                                               "colorByField": color})]),
        ], orientation="horizontal")
        ds.save_workspace(wsname, space, description=f"문장 UMAP (색: {color})",
                          overwrite=True)
    ds.save()
    hit = ds.match({"match.label": "hit"}).count()
    pg.log(f"{dataset_name}: 완료 — 문장 {n:,} · GT 일치 {hit / n:.1%} · "
           f"워크스페이스 {ds.list_workspaces()}")


finish("source-h-prompts")
pg.log("=== sourceh finish 완료 ===")
