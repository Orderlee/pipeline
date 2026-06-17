"""분석 surface 헬퍼 — pgvector(image_embeddings) + MinIO + embedding-service ↔ FiftyOne.

노트북에서:
    import fiftyone_pgvector as fp
    rows = fp.load_frame_embeddings(limit=5000)
    ds = fp.build_fiftyone_dataset("frames", rows)        # UMAP 시각화 포함
    hits = fp.search_by_text("a fire on the street", k=20) # 텍스트→이미지 검색
    sim  = fp.search_by_image(rows[0]["image_id"], k=20)   # 이미지 유사 검색
    import fiftyone as fo; fo.launch_app(ds)                # :5151

⚠️ 컨테이너 빌드/기동 후 staging 에서 검증 필요 (로컬 미검증 scaffold).
환경변수: DATAOPS_POSTGRES_DSN, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, EMBEDDING_API_URL.
"""

from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from typing import Any

DEFAULT_MODEL = "facebook/PE-Core-L14-336"


# ── 연결 헬퍼 ──
def _pg_conn():
    import psycopg2

    dsn = os.environ["DATAOPS_POSTGRES_DSN"]
    return psycopg2.connect(dsn)


def _minio_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", ""),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", ""),
    )


def _parse_vector(raw: Any) -> list[float]:
    """pgvector 컬럼 반환값('[0.1,0.2,...]' 텍스트 또는 list) → list[float]."""
    if isinstance(raw, (list, tuple)):
        return [float(x) for x in raw]
    return [float(x) for x in json.loads(raw)]


def presigned_url(bucket: str, key: str, expires: int = 3600) -> str:
    return _minio_client().generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires
    )


# ── pgvector 로드 ──
def load_frame_embeddings(limit: int | None = None, model_name: str = DEFAULT_MODEL) -> list[dict]:
    """image_embeddings(frame) ⨝ image_metadata → [{entity_id, image_id, bucket, key, asset_id, embedding}]."""
    sql = """
        SELECT e.entity_id, e.image_id, im.image_bucket, im.image_key, im.source_asset_id, e.embedding
        FROM image_embeddings e
        JOIN image_metadata im ON im.image_id = e.image_id
        WHERE e.entity_type = 'frame' AND e.model_name = %(model)s
        ORDER BY e.entity_id
    """
    if limit:
        sql += "\nLIMIT %(limit)s"
    out: list[dict] = []
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"model": model_name, "limit": limit})
        for entity_id, image_id, bucket, key, asset_id, emb in cur.fetchall():
            out.append(
                {
                    "entity_id": entity_id,
                    "image_id": image_id,
                    "bucket": bucket,
                    "key": key,
                    "asset_id": asset_id,
                    "embedding": _parse_vector(emb),
                }
            )
    return out


# ── FiftyOne 데이터셋 ──
MEDIA_DIR = "/data/fiftyone/media"  # 영속 볼륨. FiftyOne 이 서빙할 로컬 이미지 캐시.


def _sample_value(sample, field: str, default: Any = None) -> Any:
    try:
        value = sample[field]
    except Exception:  # noqa: BLE001 — FiftyOne raises several field lookup exceptions across versions
        return default
    return default if value is None else value


def _fetch_frame_asset_ids(image_ids: list[str]) -> dict[str, str]:
    if not image_ids:
        return {}
    try:
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT image_id, source_asset_id
                FROM image_metadata
                WHERE image_id = ANY(%(image_ids)s)
                """,
                {"image_ids": list(dict.fromkeys(str(i) for i in image_ids if i))},
            )
            return {str(image_id): str(asset_id) for image_id, asset_id in cur.fetchall() if asset_id}
    except Exception as exc:  # noqa: BLE001 — analysis surface should fail forward
        print(f"attach_labels: frame asset lookup skipped: {exc}")
        return {}


def _fetch_asset_captions(asset_ids: list[str]) -> dict[str, str]:
    if not asset_ids:
        return {}
    try:
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (asset_id) asset_id, caption_text
                FROM labels
                WHERE asset_id = ANY(%(asset_ids)s)
                  AND caption_text IS NOT NULL
                  AND caption_text <> ''
                ORDER BY asset_id, created_at DESC NULLS LAST, event_index ASC
                """,
                {"asset_ids": list(dict.fromkeys(str(a) for a in asset_ids if a))},
            )
            return {str(asset_id): str(caption) for asset_id, caption in cur.fetchall() if caption}
    except Exception as exc:  # noqa: BLE001 — labels may be absent in older/staging DBs
        print(f"attach_labels: caption lookup skipped: {exc}")
        return {}


def _fetch_sam3_label_refs(image_ids: list[str]) -> dict[str, list[tuple[str, str]]]:
    if not image_ids:
        return {}
    refs: dict[str, list[tuple[str, str]]] = defaultdict(list)
    try:
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT image_id, COALESCE(labels_bucket, 'vlm-labels'), labels_key
                FROM image_labels
                WHERE image_id = ANY(%(image_ids)s)
                  AND label_tool = 'sam3'
                  AND labels_key IS NOT NULL
                ORDER BY image_id, created_at DESC NULLS LAST
                """,
                {"image_ids": list(dict.fromkeys(str(i) for i in image_ids if i))},
            )
            for image_id, bucket, key in cur.fetchall():
                if key:
                    refs[str(image_id)].append((str(bucket or "vlm-labels"), str(key)))
    except Exception as exc:  # noqa: BLE001 — analysis/staging may not have SAM3 rows yet
        print(f"attach_labels: SAM3 label lookup skipped: {exc}")
    return refs


def _read_minio_json(bucket: str, key: str, mc=None) -> Any:
    client = mc or _minio_client()
    obj = client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]
    try:
        return json.loads(body.read())
    finally:
        try:
            body.close()
        except Exception:  # noqa: BLE001 — best effort stream cleanup
            pass


def _coco_image_size(payload: dict[str, Any], filepath: str) -> tuple[float | None, float | None]:
    images = payload.get("images") or []
    if images:
        try:
            width = float(images[0].get("width") or 0)
            height = float(images[0].get("height") or 0)
            if width > 0 and height > 0:
                return width, height
        except (AttributeError, TypeError, ValueError):
            pass
    try:
        from PIL import Image

        with Image.open(filepath) as img:
            width, height = img.size
        return float(width), float(height)
    except Exception:  # noqa: BLE001 — invalid/missing local media just means no detections
        return None, None


def _normalize_coco_bbox(bbox: Any, width: float, height: float) -> list[float] | None:
    if not isinstance(bbox, (list, tuple)) or len(bbox) < 4 or width <= 0 or height <= 0:
        return None
    try:
        x, y, box_width, box_height = (float(v) for v in bbox[:4])
    except (TypeError, ValueError):
        return None
    if box_width <= 0 or box_height <= 0:
        return None
    rel_x = max(0.0, min(1.0, x / width))
    rel_y = max(0.0, min(1.0, y / height))
    rel_w = max(0.0, min(1.0 - rel_x, box_width / width))
    rel_h = max(0.0, min(1.0 - rel_y, box_height / height))
    if rel_w <= 0 or rel_h <= 0:
        return None
    return [rel_x, rel_y, rel_w, rel_h]


def _detections_from_coco(payload: dict[str, Any], filepath: str) -> list[Any]:
    import fiftyone as fo

    width, height = _coco_image_size(payload, filepath)
    if not width or not height:
        return []

    categories: dict[Any, str] = {}
    for category in payload.get("categories") or []:
        if not isinstance(category, dict):
            continue
        category_id = category.get("id")
        name = str(category.get("name") or category_id or "unknown")
        categories[category_id] = name
        categories[str(category_id)] = name

    detections = []
    for ann in payload.get("annotations") or []:
        if not isinstance(ann, dict):
            continue
        norm_bbox = _normalize_coco_bbox(ann.get("bbox"), width, height)
        if norm_bbox is None:
            continue
        category_id = ann.get("category_id")
        label = categories.get(category_id) or categories.get(str(category_id)) or str(category_id or "unknown")
        kwargs: dict[str, Any] = {"label": label, "bounding_box": norm_bbox}
        try:
            kwargs["confidence"] = float(ann["score"])
        except (KeyError, TypeError, ValueError):
            pass
        detections.append(fo.Detection(**kwargs))
    return detections


def attach_labels(ds):
    """Attach SAM3 detections, a single detection_class, and a representative video caption to frame samples.

    This is intentionally fail-forward: DB/MinIO/JSON issues skip the affected lookup or sample so notebook work
    remains usable against partially populated staging/prod datasets.
    """
    import fiftyone as fo

    samples = list(ds)
    image_ids = [str(_sample_value(s, "image_id")) for s in samples if _sample_value(s, "image_id")]
    frame_asset_ids = _fetch_frame_asset_ids(image_ids)
    for sample in samples:
        image_id = _sample_value(sample, "image_id")
        asset_id = _sample_value(sample, "asset_id") or frame_asset_ids.get(str(image_id))
        if asset_id:
            sample["asset_id"] = str(asset_id)

    asset_ids = [str(_sample_value(s, "asset_id")) for s in samples if _sample_value(s, "asset_id")]
    captions_by_asset = _fetch_asset_captions(asset_ids)
    sam3_refs_by_image = _fetch_sam3_label_refs(image_ids)
    mc = _minio_client()

    for sample in samples:
        try:
            image_id = str(_sample_value(sample, "image_id", ""))
            asset_id = _sample_value(sample, "asset_id")
            sample["caption"] = captions_by_asset.get(str(asset_id), "") if asset_id else ""

            detections = []
            for bucket, key in sam3_refs_by_image.get(image_id, []):
                try:
                    payload = _read_minio_json(bucket, key, mc=mc)
                    if isinstance(payload, dict):
                        detections.extend(_detections_from_coco(payload, sample.filepath))
                except Exception as exc:  # noqa: BLE001 — one broken label artifact should not block the dataset
                    print(f"attach_labels: skipped SAM3 labels image_id={image_id} key={key}: {exc}")
                    continue

            if detections:
                sample["detections"] = fo.Detections(detections=detections)
                sample["detection_class"] = Counter(d.label for d in detections).most_common(1)[0][0]
            else:
                sample["detection_class"] = "none"
            sample.save()
        except Exception as exc:  # noqa: BLE001 — per-sample fail-forward
            print(f"attach_labels: skipped sample image_id={_sample_value(sample, 'image_id')}: {exc}")
    return ds


def _load_caption_embeddings(model_name: str = DEFAULT_MODEL) -> list[dict[str, Any]]:
    sql = """
        SELECT entity_id, asset_id, text_content, embedding
        FROM image_embeddings
        WHERE entity_type = 'caption'
          AND model_name = %(model)s
          AND asset_id IS NOT NULL
        ORDER BY entity_id
    """
    rows: list[dict[str, Any]] = []
    try:
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, {"model": model_name})
            for entity_id, asset_id, text_content, emb in cur.fetchall():
                rows.append(
                    {
                        "entity_id": entity_id,
                        "asset_id": asset_id,
                        "text_content": text_content,
                        "embedding": _parse_vector(emb),
                    }
                )
    except Exception as exc:  # noqa: BLE001 — caption embedding table/columns may not exist yet
        print(f"add_caption_clusters: caption embeddings lookup skipped: {exc}")
    return rows


def add_caption_clusters(ds, k: int = 12, model_name: str = DEFAULT_MODEL):
    """Cluster caption embeddings and project the asset-level cluster label onto frame samples.

    Frames whose source video has no caption embedding receive ``caption_cluster='none'``.
    """
    samples = list(ds)
    image_ids = [str(_sample_value(s, "image_id")) for s in samples if _sample_value(s, "image_id")]
    frame_asset_ids = _fetch_frame_asset_ids(image_ids)
    for sample in samples:
        try:
            image_id = _sample_value(sample, "image_id")
            asset_id = _sample_value(sample, "asset_id") or frame_asset_ids.get(str(image_id))
            if asset_id:
                sample["asset_id"] = str(asset_id)
            sample["caption_cluster"] = "none"
            sample.save()
        except Exception as exc:  # noqa: BLE001 — per-sample fail-forward
            print(f"add_caption_clusters: default cluster skipped image_id={_sample_value(sample, 'image_id')}: {exc}")

    caption_rows = _load_caption_embeddings(model_name=model_name)
    if not caption_rows:
        return ds

    try:
        from sklearn.cluster import KMeans

        n_clusters = max(1, min(int(k), len(caption_rows)))
        labels = KMeans(n_clusters=n_clusters, random_state=42, n_init=10).fit_predict(
            [r["embedding"] for r in caption_rows]
        )
    except Exception as exc:  # noqa: BLE001 — sklearn may be unavailable in older analysis images
        print(f"add_caption_clusters: clustering skipped: {exc}")
        return ds

    clusters_by_asset: dict[str, list[int]] = defaultdict(list)
    for row, cluster_id in zip(caption_rows, labels, strict=False):
        asset_id = row.get("asset_id")
        if asset_id:
            clusters_by_asset[str(asset_id)].append(int(cluster_id))

    asset_cluster = {
        asset_id: f"cap{Counter(cluster_ids).most_common(1)[0][0]:02d}"
        for asset_id, cluster_ids in clusters_by_asset.items()
        if cluster_ids
    }
    for sample in samples:
        try:
            asset_id = _sample_value(sample, "asset_id")
            sample["caption_cluster"] = asset_cluster.get(str(asset_id), "none") if asset_id else "none"
            sample.save()
        except Exception as exc:  # noqa: BLE001 — per-sample fail-forward
            print(f"add_caption_clusters: cluster attach skipped image_id={_sample_value(sample, 'image_id')}: {exc}")
    return ds


def build_fiftyone_dataset(
    name: str,
    rows: list[dict],
    *,
    umap: bool = True,
    overwrite: bool = True,
    media_dir: str = MEDIA_DIR,
    labels: bool = True,
    caption_clusters: bool = True,
):
    """rows → FiftyOne 데이터셋.

    ⚠️ FiftyOne 은 ``Sample.filepath`` 를 **로컬 파일 경로**로 정규화하므로 presigned URL 을
    직접 쓰면 ``/workspace/http:/...`` 로 망가져 이미지가 안 뜬다. → MinIO 객체를 ``media_dir``
    (영속 볼륨)로 내려받아 그 로컬 경로를 filepath 로 사용한다. 다운로드 실패는 skip(fail-forward).
    """
    import os

    import fiftyone as fo
    import fiftyone.brain as fob

    if overwrite and fo.dataset_exists(name):
        fo.delete_dataset(name)
    os.makedirs(media_dir, exist_ok=True)
    mc = _minio_client()
    ds = fo.Dataset(name, persistent=True)
    samples = []
    skipped = 0
    for r in rows:
        ext = os.path.splitext(r["key"])[1] or ".jpg"
        local_path = os.path.join(media_dir, f"{r['image_id']}{ext}")
        if not os.path.exists(local_path):
            try:
                mc.download_file(r["bucket"], r["key"], local_path)
            except Exception:  # noqa: BLE001 — 객체 누락 등은 skip
                skipped += 1
                continue
        s = fo.Sample(filepath=local_path)
        s["image_id"] = r["image_id"]
        s["entity_id"] = r["entity_id"]
        s["embedding"] = r["embedding"]
        s["minio_key"] = f"{r['bucket']}/{r['key']}"
        if r.get("asset_id") or r.get("source_asset_id"):
            s["asset_id"] = r.get("asset_id") or r.get("source_asset_id")
        samples.append(s)
    ds.add_samples(samples)
    if skipped:
        print(f"build_fiftyone_dataset: skipped {skipped} (MinIO download failed)")
    if labels:
        try:
            attach_labels(ds)
        except Exception as exc:  # noqa: BLE001 — keep dataset build usable if metadata lookup fails
            print(f"build_fiftyone_dataset: label attachment skipped: {exc}")
    if caption_clusters:
        try:
            add_caption_clusters(ds)
        except Exception as exc:  # noqa: BLE001 — keep dataset build usable without caption clusters
            print(f"build_fiftyone_dataset: caption clusters skipped: {exc}")
    if umap and len(samples) >= 3:
        fob.compute_visualization(ds, embeddings="embedding", method="umap", brain_key="emb_viz")
    return ds


# ── 검색 (native PG, pgvector <=> cosine) ──
def _embed_text(query: str) -> list[float]:
    import requests

    url = os.environ.get("EMBEDDING_API_URL", "http://embedding-service:8003")
    resp = requests.post(f"{url.rstrip('/')}/embed_text", data={"text": query}, timeout=60)
    resp.raise_for_status()
    return resp.json()["vector"]


def _topk_by_vector(vec: list[float], k: int, model_name: str) -> list[dict]:
    lit = "[" + ",".join(repr(float(x)) for x in vec) + "]"
    sql = """
        SELECT e.entity_id, e.image_id, im.image_bucket, im.image_key,
               (e.embedding <=> %(q)s::vector) AS cosine_dist
        FROM image_embeddings e
        JOIN image_metadata im ON im.image_id = e.image_id
        WHERE e.entity_type = 'frame' AND e.model_name = %(model)s
        ORDER BY e.embedding <=> %(q)s::vector
        LIMIT %(k)s
    """
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"q": lit, "model": model_name, "k": k})
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def search_by_text(query: str, k: int = 20, model_name: str = DEFAULT_MODEL) -> list[dict]:
    """텍스트 → embedding-service /embed_text → frame pgvector cosine top-k (cross-modal text→image)."""
    return _topk_by_vector(_embed_text(query), k, model_name)


def search_images_by_caption(query: str, k: int = 20, model_name: str = DEFAULT_MODEL) -> list[dict]:
    """Cross-modal caption/text query → frame image embeddings."""
    return search_by_text(query, k=k, model_name=model_name)


def search_captions_by_text(query: str, k: int = 20, model_name: str = DEFAULT_MODEL) -> list[dict]:
    """텍스트 → caption embeddings cosine top-k."""
    vec = _embed_text(query)
    lit = "[" + ",".join(repr(float(x)) for x in vec) + "]"
    sql = """
        SELECT e.entity_id, e.asset_id, e.text_content,
               (e.embedding <=> %(q)s::vector) AS cosine_dist
        FROM image_embeddings e
        WHERE e.entity_type = 'caption' AND e.model_name = %(model)s
        ORDER BY e.embedding <=> %(q)s::vector
        LIMIT %(k)s
    """
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"q": lit, "model": model_name, "k": k})
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def search_by_image(image_id: str, k: int = 20, model_name: str = DEFAULT_MODEL) -> list[dict]:
    """주어진 프레임의 임베딩 기준 cosine top-k 유사 프레임."""
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT embedding FROM image_embeddings "
            "WHERE entity_type='frame' AND image_id=%(id)s AND model_name=%(model)s LIMIT 1",
            {"id": image_id, "model": model_name},
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"no embedding for image_id={image_id}")
    return _topk_by_vector(_parse_vector(row[0]), k, model_name)


# ── 클러스터 간 거리 (centroid cosine) — FiftyOne 의 UMAP 시각근접과 달리 정량 거리 ──
def _cluster_centroids(entity_type: str = "frame", k: int = 12, model_name: str = DEFAULT_MODEL):
    """entity_type 임베딩 KMeans(k) → (names, centroids, sizes). frame=dominant site, caption=대표 텍스트."""
    from collections import Counter

    import numpy as np
    from sklearn.cluster import KMeans

    if entity_type == "frame":
        sql = (
            "SELECT split_part(im.image_key,'/',1), e.embedding "
            "FROM image_embeddings e JOIN image_metadata im ON im.image_id=e.image_id "
            "WHERE e.entity_type='frame' AND e.model_name=%(model)s"
        )
    else:
        sql = (
            "SELECT left(text_content, 20), embedding FROM image_embeddings "
            "WHERE entity_type=%(et)s AND model_name=%(model)s"
        )
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"model": model_name, "et": entity_type})
        rows = cur.fetchall()
    if len(rows) < k:
        raise ValueError(f"not enough {entity_type} embeddings ({len(rows)}) for k={k}")
    meta = [r[0] for r in rows]
    x = np.array([_parse_vector(r[1]) for r in rows], dtype="float32")
    km = KMeans(n_clusters=k, random_state=42, n_init=10).fit(x)
    names, sizes = [], []
    for c in range(k):
        members = [meta[i] for i in range(len(km.labels_)) if km.labels_[i] == c]
        dom = Counter(members).most_common(1)[0][0] if members else "?"
        names.append(f"c{c}:{dom}")
        sizes.append(len(members))
    return names, km.cluster_centers_, sizes


def cluster_distance_matrix(entity_type: str = "frame", k: int = 12, model_name: str = DEFAULT_MODEL):
    """클러스터 centroid 간 cosine 거리행렬. returns (names, D(ndarray), sizes)."""
    from sklearn.metrics.pairwise import cosine_distances

    names, cent, sizes = _cluster_centroids(entity_type, k, model_name)
    return names, cosine_distances(cent), sizes


def cluster_distance_heatmap(
    entity_type: str = "frame", k: int = 12, model_name: str = DEFAULT_MODEL, save_html: str | None = None
):
    """클러스터 간 cosine 거리 heatmap (계층클러스터링 순서로 정렬 → 블록 구조 가시화).

    Jupyter: fp.cluster_distance_heatmap().show()  /  save_html 지정 시 HTML 파일로 저장해 브라우저로.
    """
    import numpy as np
    import plotly.graph_objects as go
    from scipy.cluster.hierarchy import leaves_list, linkage
    from scipy.spatial.distance import squareform

    names, dmat, sizes = cluster_distance_matrix(entity_type, k, model_name)
    order = leaves_list(linkage(squareform(dmat, checks=False), method="average"))
    dz = dmat[np.ix_(order, order)]
    labels = [f"{names[i]} (n={sizes[i]})" for i in order]
    fig = go.Figure(go.Heatmap(z=dz, x=labels, y=labels, colorscale="Viridis", zmin=0, colorbar={"title": "cosine"}))
    fig.update_layout(
        title=f"{entity_type} inter-cluster cosine distance (k={k}, hclust order)",
        width=860,
        height=780,
        xaxis={"tickangle": -45},
    )
    if save_html:
        fig.write_html(save_html, include_plotlyjs="cdn")
        print(f"saved: {save_html}")
    return fig


# ── PCA (FiftyOne 네이티브) / dendrogram / MDS / network / nearest / 투영 비교 ──
def add_pca_visualization(ds, brain_key: str = "emb_viz_pca"):
    """FiftyOne 데이터셋에 PCA 시각화 brain 추가 → App Embeddings 패널에서 UMAP 과 토글."""
    import fiftyone.brain as fob

    if ds.has_brain_run(brain_key):
        ds.delete_brain_run(brain_key)
    return fob.compute_visualization(ds, embeddings="embedding", method="pca", brain_key=brain_key)


def cluster_dendrogram(entity_type="frame", k=12, model_name=DEFAULT_MODEL, save_html=None):
    """centroid 계층 클러스터링 dendrogram (어느 클러스터끼리 가까운지 트리)."""
    import plotly.figure_factory as ff
    from scipy.cluster.hierarchy import linkage
    from scipy.spatial.distance import pdist

    names, cent, sizes = _cluster_centroids(entity_type, k, model_name)
    labels = [f"{n} (n={s})" for n, s in zip(names, sizes)]
    fig = ff.create_dendrogram(
        cent,
        labels=labels,
        distfun=lambda x: pdist(x, metric="cosine"),
        linkagefun=lambda d: linkage(d, method="average"),
    )
    fig.update_layout(title=f"{entity_type} cluster dendrogram (cosine, k={k})", width=920, height=520)
    if save_html:
        fig.write_html(save_html, include_plotlyjs="cdn")
        print(f"saved: {save_html}")
    return fig


def _centroid_mds(entity_type, k, model_name):
    import numpy as np
    from sklearn.manifold import MDS
    from sklearn.metrics.pairwise import cosine_distances

    names, cent, sizes = _cluster_centroids(entity_type, k, model_name)
    dmat = cosine_distances(cent)
    coords = MDS(n_components=2, dissimilarity="precomputed", random_state=42, normalized_stress="auto").fit_transform(
        dmat
    )
    return names, sizes, np.asarray(coords), dmat


def cluster_mds_scatter(entity_type="frame", k=12, model_name=DEFAULT_MODEL, save_html=None):
    """centroid MDS 2D scatter (UMAP 과 달리 쌍거리 충실; 점=클러스터, 크기=개수, 색=사이트)."""
    import plotly.express as px

    names, sizes, coords, _ = _centroid_mds(entity_type, k, model_name)
    sites = [n.split(":", 1)[-1] for n in names]
    fig = px.scatter(
        x=coords[:, 0],
        y=coords[:, 1],
        size=sizes,
        color=sites,
        text=names,
        title=f"{entity_type} centroid MDS (distance-faithful, k={k})",
    )
    fig.update_traces(textposition="top center")
    if save_html:
        fig.write_html(save_html, include_plotlyjs="cdn")
        print(f"saved: {save_html}")
    return fig


def cluster_network(entity_type="frame", k=12, threshold=0.25, model_name=DEFAULT_MODEL, save_html=None):
    """클러스터 네트워크: 노드=MDS 위치, 엣지=cosine<threshold (이웃 클러스터 군집)."""
    import plotly.graph_objects as go

    names, sizes, coords, dmat = _centroid_mds(entity_type, k, model_name)
    edge_x, edge_y = [], []
    for i in range(k):
        for j in range(i + 1, k):
            if dmat[i][j] < threshold:
                edge_x += [coords[i, 0], coords[j, 0], None]
                edge_y += [coords[i, 1], coords[j, 1], None]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=edge_x, y=edge_y, mode="lines", line={"color": "#ccc"}, hoverinfo="none"))
    fig.add_trace(
        go.Scatter(
            x=coords[:, 0],
            y=coords[:, 1],
            mode="markers+text",
            text=names,
            textposition="top center",
            marker={"size": [max(10, s**0.5) for s in sizes], "color": "#1f77b4"},
        )
    )
    fig.update_layout(
        title=f"{entity_type} cluster network (edge cosine<{threshold}, k={k})",
        showlegend=False,
        width=880,
        height=720,
    )
    if save_html:
        fig.write_html(save_html, include_plotlyjs="cdn")
        print(f"saved: {save_html}")
    return fig


def cluster_nearest_table(entity_type="frame", k=12, top=3, model_name=DEFAULT_MODEL):
    """각 클러스터의 최근접 이웃 클러스터 top-N (거리 랭킹). returns list[dict] + print."""
    names, dmat, sizes = cluster_distance_matrix(entity_type, k, model_name)
    out = []
    for i in range(k):
        nn = sorted((float(dmat[i][j]), j) for j in range(k) if j != i)[:top]
        out.append({"cluster": names[i], "n": sizes[i], "nearest": [(names[j], round(d, 3)) for d, j in nn]})
    for r in out:
        print(f"{r['cluster']} (n={r['n']}) → " + ", ".join(f"{nm} {ds}" for nm, ds in r["nearest"]))
    return out


def projection_comparison(entity_type="frame", k=12, sample=600, model_name=DEFAULT_MODEL, save_html=None):
    """UMAP / PCA / MDS 3-panel 비교 (subsample, KMeans cluster 색). PCA 는 explained variance 표기."""
    import numpy as np
    import plotly.express as px
    import plotly.graph_objects as go
    import umap
    from plotly.subplots import make_subplots
    from sklearn.cluster import KMeans
    from sklearn.decomposition import PCA
    from sklearn.manifold import MDS
    from sklearn.metrics.pairwise import cosine_distances

    if entity_type == "frame":
        sql = (
            "SELECT e.embedding FROM image_embeddings e WHERE e.entity_type='frame' "
            "AND e.model_name=%(m)s ORDER BY random() LIMIT %(n)s"
        )
    else:
        sql = (
            "SELECT embedding FROM image_embeddings WHERE entity_type=%(et)s "
            "AND model_name=%(m)s ORDER BY random() LIMIT %(n)s"
        )
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"m": model_name, "et": entity_type, "n": sample})
        x = np.array([_parse_vector(r[0]) for r in cur.fetchall()], dtype="float32")
    labels = KMeans(n_clusters=k, random_state=42, n_init=10).fit_predict(x)
    pca = PCA(n_components=2).fit(x)
    proj = {
        "UMAP (local)": umap.UMAP(n_components=2, random_state=42).fit_transform(x),
        f"PCA ({pca.explained_variance_ratio_.sum() * 100:.0f}% var)": pca.transform(x),
        "MDS (distance-faithful)": MDS(
            n_components=2, dissimilarity="precomputed", random_state=42, normalized_stress="auto"
        ).fit_transform(cosine_distances(x)),
    }
    palette = px.colors.qualitative.Alphabet
    fig = make_subplots(rows=1, cols=3, subplot_titles=list(proj.keys()))
    for ci, xy in enumerate(proj.values(), 1):
        for c in range(k):
            mask = labels == c
            fig.add_trace(
                go.Scatter(
                    x=xy[mask, 0],
                    y=xy[mask, 1],
                    mode="markers",
                    name=f"c{c}",
                    marker={"size": 4, "color": palette[c % len(palette)]},
                    showlegend=(ci == 1),
                ),
                row=1,
                col=ci,
            )
    fig.update_layout(
        title=f"{entity_type} 투영 비교 n={len(x)} k={k} — UMAP(국소)/PCA(선형·전역)/MDS(거리충실)",
        width=1320,
        height=500,
    )
    if save_html:
        fig.write_html(save_html, include_plotlyjs="cdn")
        print(f"saved: {save_html}")
    return fig


def cluster_composition(entity_type="frame", k=12, kind="sunburst", model_name=DEFAULT_MODEL, save_html=None):
    """구성/비율 시각화 (sunburst 또는 treemap). 계층: frame=site→cluster, caption=cluster. 셀 크기=개수.

    ※ 거리 아님 — '얼마나 많은가/비율'을 계층으로. kind='sunburst'|'treemap'.
    """
    import numpy as np
    import pandas as pd
    import plotly.express as px
    from sklearn.cluster import KMeans

    if entity_type == "frame":
        sql = (
            "SELECT split_part(im.image_key,'/',1), e.embedding FROM image_embeddings e "
            "JOIN image_metadata im ON im.image_id=e.image_id "
            "WHERE e.entity_type='frame' AND e.model_name=%(m)s"
        )
        path = ["site", "cluster"]
    else:
        sql = "SELECT '(caption)', embedding FROM image_embeddings WHERE entity_type=%(et)s AND model_name=%(m)s"
        path = ["cluster"]
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"m": model_name, "et": entity_type})
        rows = cur.fetchall()
    x = np.array([_parse_vector(r[1]) for r in rows], dtype="float32")
    lab = KMeans(n_clusters=k, random_state=42, n_init=10).fit_predict(x)
    df = pd.DataFrame({"site": [r[0] for r in rows], "cluster": [f"c{c}" for c in lab], "count": 1})
    agg = df.groupby(["site", "cluster"], as_index=False)["count"].sum()
    fn = px.treemap if kind == "treemap" else px.sunburst
    fig = fn(agg, path=path, values="count", title=f"{entity_type} 구성 ({'/'.join(path)}, n={len(rows)}, k={k})")
    fig.update_traces(textinfo="label+value+percent parent")
    fig.update_layout(width=900, height=720)
    if save_html:
        fig.write_html(save_html, include_plotlyjs="cdn")
        print(f"saved: {save_html}")
    return fig
