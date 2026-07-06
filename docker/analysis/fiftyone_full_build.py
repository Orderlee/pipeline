"""FiftyOne 'frames' 전체(대용량) 배치 빌드 — OOM 방지용.

build_fiftyone_dataset 가 ① 미디어 순차 다운로드 ② per-sample save(attach_labels) ③ UMAP 전체 fit
을 해서 188K 에선 느리거나 메모리 부담. 이 스크립트는:
  - 미디어 **병렬** 다운로드(ThreadPool)
  - add_samples **청크**
  - 라벨 **bulk set_values**(per-sample save 회피; detections 는 라벨된 소수만)
  - UMAP **샘플-fit → transform 배치**(메모리 bounded, 전체 투영) + PCA

env:
  FFB_LIMIT   (정수 / all|0|none) 기본 all
  FFB_DATASET (기본 'frames')
  FFB_WORKERS (미디어 다운로드 스레드, 기본 24)
  FFB_CHUNK   (add_samples 청크, 기본 5000)
  FFB_FIT     (UMAP fit 샘플 수, 기본 50000)
  FFB_LAUNCH  (1=앱 기동+keepalive, 0=빌드만) 기본 1
"""

import os
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import numpy as np

import fiftyone as fo
import fiftyone.brain as fob

import fiftyone_pgvector as fp

T0 = time.time()


def log(msg):
    print(f"[ffb +{time.time() - T0:6.0f}s] {msg}", flush=True)


_lim = os.getenv("FFB_LIMIT", "all").strip().lower()
LIMIT = None if _lim in ("0", "all", "none", "") else int(_lim)
DATASET = os.getenv("FFB_DATASET", "frames")
WORKERS = int(os.getenv("FFB_WORKERS", "24"))
CHUNK = int(os.getenv("FFB_CHUNK", "5000"))
FIT = int(os.getenv("FFB_FIT", "50000"))
LAUNCH = os.getenv("FFB_LAUNCH", "1").strip() not in ("0", "false", "no")

log(f"start LIMIT={'ALL' if LIMIT is None else LIMIT} DATASET={DATASET} WORKERS={WORKERS} CHUNK={CHUNK} FIT={FIT}")

# ── 1. rows ──
rows = fp.load_frame_embeddings(limit=LIMIT)
log(f"loaded rows={len(rows)}")

# ── 2. 미디어 병렬 다운로드 ──
mc = fp._minio_client()
media = fp.MEDIA_DIR
os.makedirs(media, exist_ok=True)


def _dl(r):
    ext = os.path.splitext(r["key"])[1] or ".jpg"
    lp = os.path.join(media, f"{r['image_id']}{ext}")
    if not os.path.exists(lp):
        try:
            mc.download_file(r["bucket"], r["key"], lp)
        except Exception:  # noqa: BLE001 — 누락/오류 skip
            return (r, None)
    return (r, lp)


ok_rows = []
done = 0
with ThreadPoolExecutor(max_workers=WORKERS) as ex:
    for r, lp in ex.map(_dl, rows):
        done += 1
        if lp:
            r["_local"] = lp
            ok_rows.append(r)
        if done % 20000 == 0:
            log(f"  media {done}/{len(rows)} (ok={len(ok_rows)})")
log(f"media done ok={len(ok_rows)} / {len(rows)}")

# ── 3. 데이터셋 빌드 (청크 add) ──
if fo.dataset_exists(DATASET):
    fo.delete_dataset(DATASET)
ds = fo.Dataset(DATASET, persistent=True)
batch = []
added = 0
for r in ok_rows:
    s = fo.Sample(filepath=r["_local"])
    s["image_id"] = r["image_id"]
    s["entity_id"] = r["entity_id"]
    s["embedding"] = r["embedding"]
    s["minio_key"] = f"{r['bucket']}/{r['key']}"
    aid = r.get("asset_id") or r.get("source_asset_id")
    if aid:
        s["asset_id"] = str(aid)
    batch.append(s)
    if len(batch) >= CHUNK:
        ds.add_samples(batch)
        added += len(batch)
        batch = []
        log(f"  added {added}")
if batch:
    ds.add_samples(batch)
    added += len(batch)
log(f"added total {added}")

# ── 4. 라벨 bulk set_values (per-sample save 회피) ──
try:
    ids, image_ids, asset_ids, filepaths = ds.values(["id", "image_id", "asset_id", "filepath"])
    frame_asset_ids = fp._fetch_frame_asset_ids([str(i) for i in image_ids if i])
    aids = [str(a) if a else str(frame_asset_ids.get(str(im), "") or "") for a, im in zip(asset_ids, image_ids)]
    captions_by_asset = fp._fetch_asset_captions([a for a in aids if a])
    env_by_asset = fp._fetch_video_env([a for a in aids if a])
    sam3_refs = fp._fetch_sam3_label_refs([str(i) for i in image_ids if i])

    det_class, norm, cap_d, dn_d, env_d, detections_d = {}, {}, {}, {}, {}, {}
    for sid, iid, aid, fpth in zip(ids, image_ids, aids, filepaths):
        iid = str(iid) if iid else ""
        cap_d[sid] = captions_by_asset.get(aid, "") if aid else ""
        dn, env = env_by_asset.get(aid, (None, None)) if aid else (None, None)
        dn_d[sid] = dn or "none"
        env_d[sid] = env or "none"
        dets = []
        for bucket, key in sam3_refs.get(iid, []):
            try:
                payload = fp._read_minio_json(bucket, key, mc=mc)
                if isinstance(payload, dict):
                    dets.extend(fp._detections_from_coco(payload, fpth))
            except Exception:  # noqa: BLE001
                continue
        if dets:
            detections_d[sid] = fo.Detections(detections=dets)
            dc = Counter(d.label for d in dets).most_common(1)[0][0]
        else:
            dc = "none"
        det_class[sid] = dc
        norm[sid] = fp.normalize_class(dc)
    ds.set_values("caption", cap_d, key_field="id")
    ds.set_values("daynight", dn_d, key_field="id")
    ds.set_values("environment", env_d, key_field="id")
    ds.set_values("detection_class", det_class, key_field="id")
    ds.set_values("normalized_class", norm, key_field="id")
    if detections_d:
        ds.set_values("detections", detections_d, key_field="id")
    log(f"labels set (detections on {len(detections_d)} frames)")
except Exception as exc:  # noqa: BLE001 — 라벨 실패해도 빌드 유지
    log(f"labels skipped: {exc!r}")

# ── 5. UMAP 샘플-fit → transform 배치 (+ PCA) ──
try:
    embs = ds.values("embedding")  # ds 순서 = points 정렬 보장
    X = np.asarray(embs, dtype="float32")
    n = len(X)
    log(f"UMAP: X={X.shape}")
    import umap  # umap-learn

    reducer = umap.UMAP(n_components=2, metric="cosine", low_memory=True, verbose=True)
    if n > FIT:
        import random

        random.seed(42)
        idx = sorted(random.sample(range(n), FIT))
        reducer.fit(X[idx])
        pts = np.empty((n, 2), dtype="float32")
        for i in range(0, n, 20000):
            pts[i : i + 20000] = reducer.transform(X[i : i + 20000])
        log(f"UMAP fit on {FIT} sample, transformed {n} (batched)")
    else:
        pts = reducer.fit_transform(X)
        log("UMAP full fit")
    try:
        fob.compute_visualization(ds, points=pts, brain_key="emb_viz")
        log("emb_viz registered (points=)")
    except Exception as exc:  # noqa: BLE001 — points= 미지원이면 표준 fit fallback
        log(f"points= 실패({exc!r}) → 표준 method=umap fallback")
        fob.compute_visualization(ds, embeddings="embedding", method="umap", brain_key="emb_viz")
    try:
        fob.compute_visualization(ds, embeddings="embedding", method="pca", brain_key="emb_viz_pca")
        log("emb_viz_pca registered")
    except Exception as exc:  # noqa: BLE001
        log(f"PCA skipped: {exc!r}")
except Exception as exc:  # noqa: BLE001
    log(f"UMAP skipped: {exc!r}")

log(f"BUILD DONE dataset={DATASET} samples={ds.count()}")

# ── 6. (옵션) 텍스트검색 인덱스 + 앱 기동 ──
if LAUNCH:
    try:
        fp.build_text_search_index(ds, brain_key="text_search")
        log("text_search index built")
    except Exception as exc:  # noqa: BLE001
        log(f"text_search skipped: {exc!r}")
    log("launching app on :5151")
    fo.launch_app(ds, address="0.0.0.0", port=5151)
    log("APP_LAUNCHED")
    time.sleep(10 ** 9)
