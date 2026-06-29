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
import shutil
from collections import Counter, defaultdict
from typing import Any

DEFAULT_MODEL = "facebook/PE-Core-L14-336"

# ── 분류 택소노미 정규화 ──
# 여러 표현을 단일 클래스 이름으로 통일. detection_class 는 원본 보존, normalized_class 에 정규 이름 저장.
NORMALIZED_CLASS_MAP: dict[str, str] = {
    "person on the ground": "fall",
    "fallen person": "fall",
    "person lying down": "fall",
    "patient": "patient",  # 'patient' 는 쓰러짐(fall)과 별개 — 라벨이 있으므로 독립 클래스로 유지
    "fire": "fire",
    "flame": "fire",
    "open flame": "fire",
    "smoke": "smoke",
    "smoke cloud": "smoke",
    "none": "none",
}


def normalize_class(label: str | None) -> str:
    """원본 라벨 → 정규화 클래스. strip+casefold 후 매핑 조회; 없으면 casefolded 라벨."""
    raw = str(label or "none").strip()
    if not raw:
        return "none"
    key = raw.casefold()
    return NORMALIZED_CLASS_MAP.get(key, key)


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


def _project_of(image_key) -> str:
    """프로젝트 = image_key 첫 세그먼트(source_unit_name). raw_key=<source>/<rel> 정책 기준."""
    return (str(image_key or "").strip().split("/", 1)[0]) or "none"


def _fetch_image_keys(image_ids: list[str]) -> dict[str, str]:
    """image_id → image_key. fail-forward (구/스테이징 DB 결손 허용)."""
    if not image_ids:
        return {}
    try:
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT image_id, image_key FROM image_metadata WHERE image_id = ANY(%(ids)s)",
                {"ids": list(dict.fromkeys(str(i) for i in image_ids if i))},
            )
            return {str(image_id): str(key) for image_id, key in cur.fetchall() if key}
    except Exception as exc:  # noqa: BLE001 — image_key lookup optional
        print(f"attach_project: image_key lookup skipped: {exc}")
        return {}


def attach_project(ds):
    """기존 데이터셋에 sample['project'] 채움 — DB 조회 + 벌크 set_values(라벨/미디어 불변), 앱 재기동 불필요.

    image_key 없으면 minio_key(=bucket/source/...) 2번째 세그먼트로 fallback.
    fiftyone_full_build 의 set_values(key_field='id') 패턴과 동일 — 대용량에서 per-sample save 회피.
    """
    sids = ds.values("id")
    image_ids = ds.values("image_id")
    minio_keys = ds.values("minio_key")
    image_keys = _fetch_image_keys([i for i in image_ids if i])
    proj_by_sid: dict[str, str] = {}
    for sid, iid, mk in zip(sids, image_ids, minio_keys):
        proj = _project_of(image_keys.get(str(iid))) if iid else "none"
        if proj == "none" and mk:
            parts = str(mk).split("/")
            if len(parts) >= 2 and parts[1]:
                proj = parts[1]
        proj_by_sid[sid] = proj
    ds.set_values("project", proj_by_sid, key_field="id")
    from collections import Counter as _C

    print(f"attach_project: {len(proj_by_sid)} samples; project dist top10={_C(proj_by_sid.values()).most_common(10)}")
    return ds


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
    env_by_asset = _fetch_video_env(asset_ids)
    sam3_refs_by_image = _fetch_sam3_label_refs(image_ids)
    image_keys = _fetch_image_keys(image_ids)
    mc = _minio_client()

    for sample in samples:
        try:
            image_id = str(_sample_value(sample, "image_id", ""))
            asset_id = _sample_value(sample, "asset_id")
            sample["caption"] = captions_by_asset.get(str(asset_id), "") if asset_id else ""
            dn, env = env_by_asset.get(str(asset_id), (None, None)) if asset_id else (None, None)
            sample["daynight"] = dn or "none"
            sample["environment"] = env or "none"
            sample["project"] = _project_of(image_keys.get(image_id))

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
            # normalized_class: 원본 detection_class 를 보존하면서 정규 분류 이름으로 정규화
            detection_class = _sample_value(sample, "detection_class", "none")
            sample["normalized_class"] = normalize_class(detection_class)
            sample.save()
        except Exception as exc:  # noqa: BLE001 — per-sample fail-forward
            print(f"attach_labels: skipped sample image_id={_sample_value(sample, 'image_id')}: {exc}")
    return ds


def _fetch_video_env(asset_ids):
    """asset_id → (daynight_type, environment_type) from video_metadata. fail-forward."""
    ids = list({a for a in asset_ids if a})
    if not ids:
        return {}
    out: dict[str, tuple] = {}
    try:
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT asset_id, daynight_type, environment_type FROM video_metadata WHERE asset_id = ANY(%s)",
                (ids,),
            )
            for aid, dn, env in cur.fetchall():
                out[str(aid)] = (dn, env)
    except Exception as exc:  # noqa: BLE001 — video_metadata lookup optional
        print(f"attach_labels: video env lookup skipped: {exc}")
    return out


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


def _fetch_asset_keyframe(asset_ids: list[str]) -> dict[str, tuple]:
    """asset_id → (image_bucket, image_key) — 그 영상의 대표 프레임 1개 (캡션 키프레임용)."""
    ids = list({str(a) for a in asset_ids if a})
    if not ids:
        return {}
    out: dict[str, tuple] = {}
    try:
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (source_asset_id) source_asset_id, image_bucket, image_key
                FROM image_metadata
                WHERE source_asset_id = ANY(%s)
                  AND image_role IN ('raw_video_frame', 'processed_clip_frame', 'source_image')
                  AND image_bucket IS NOT NULL AND image_key IS NOT NULL
                ORDER BY source_asset_id, frame_index NULLS LAST, image_id
                """,
                (ids,),
            )
            for aid, bucket, key in cur.fetchall():
                out[str(aid)] = (bucket, key)
    except Exception as exc:  # noqa: BLE001 — analysis surface fail-forward
        print(f"caption keyframe lookup skipped: {exc}")
    return out


def load_caption_embeddings(limit: int | None = None, model_name: str = DEFAULT_MODEL) -> list[dict]:
    """caption 임베딩 + 대표 키프레임(bucket/key) → FiftyOne 'captions' 빌드용."""
    rows = _load_caption_embeddings(model_name=model_name)
    if limit:
        rows = rows[:limit]
    keyframes = _fetch_asset_keyframe([r.get("asset_id") for r in rows])
    for row in rows:
        bucket, key = keyframes.get(str(row.get("asset_id")), (None, None))
        row["bucket"], row["key"] = bucket, key
    return rows


def build_caption_fiftyone_dataset(
    name: str = "captions",
    rows: list[dict] | None = None,
    *,
    umap: bool = True,
    overwrite: bool = True,
    media_dir: str = MEDIA_DIR,
    k: int = 12,
):
    """caption 임베딩 → FiftyOne 'captions' 데이터셋 (캡션 임베딩 공간을 UMAP 으로 탐색).

    캡션은 텍스트라 미디어가 없어, 그 캡션이 속한 영상(asset)의 대표 프레임을 키프레임으로 쓴다.
    캡션마다 고유 filepath(키프레임 심볼릭링크) → 같은 영상 캡션들이 같은 이미지를 공유해도 충돌 없음.
    fields: caption(텍스트), embedding(캡션 임베딩), caption_cluster(capNN), asset_id. 키프레임 없으면 skip.
    """
    import re

    import fiftyone as fo
    import fiftyone.brain as fob

    if rows is None:
        rows = load_caption_embeddings()
    if overwrite and fo.dataset_exists(name):
        fo.delete_dataset(name)
    cap_dir = os.path.join(media_dir, "captions")
    os.makedirs(cap_dir, exist_ok=True)
    mc = _minio_client()

    cluster_label: dict = {}
    if rows:
        try:
            from sklearn.cluster import KMeans

            n_clusters = max(1, min(int(k), len(rows)))
            labels = KMeans(n_clusters=n_clusters, random_state=42, n_init=10).fit_predict(
                [r["embedding"] for r in rows]
            )
            for row, cluster_id in zip(rows, labels, strict=False):
                cluster_label[row["entity_id"]] = f"cap{int(cluster_id):02d}"
        except Exception as exc:  # noqa: BLE001 — sklearn optional
            print(f"build_caption_fiftyone_dataset: clustering skipped: {exc}")

    # caption↔이미지 정합: entity_id → 자기영상 best-frame cosine (둘 다 임베딩된 캡션만). 없으면 필드 None.
    sim_map = fetch_caption_image_sim()

    # 키프레임 없는 캡션도 임베딩 점은 보이도록 placeholder 사용 (전 캡션 UMAP 커버리지).
    placeholder = os.path.join(media_dir, "_caption_placeholder.jpg")
    if not os.path.exists(placeholder):
        try:
            from PIL import Image

            Image.new("RGB", (320, 240), (40, 40, 40)).save(placeholder)
        except Exception as exc:  # noqa: BLE001 — placeholder optional
            print(f"build_caption_fiftyone_dataset: placeholder create failed: {exc}")

    ds = fo.Dataset(name, persistent=True)
    samples = []
    no_keyframe = 0
    asset_kf: dict = {}  # asset_id -> local keyframe path, or False if no/failed
    for row in rows:
        asset = str(row.get("asset_id"))
        bucket, key = row.get("bucket"), row.get("key")
        local = asset_kf.get(asset, None)
        if local is None and bucket and key:
            ext = os.path.splitext(key)[1] or ".jpg"
            cand = os.path.join(media_dir, f"{asset}{ext}")
            if os.path.exists(cand):
                local = cand
            else:
                try:
                    mc.download_file(bucket, key, cand)
                    local = cand
                except Exception:  # noqa: BLE001 — missing object → placeholder
                    local = False
            asset_kf[asset] = local
        target = local if local else placeholder
        if not local:
            no_keyframe += 1
        safe = re.sub(r"[^A-Za-z0-9_.-]", "_", str(row["entity_id"]))
        ext = os.path.splitext(target)[1] or ".jpg"
        cap_path = os.path.join(cap_dir, f"{safe}{ext}")
        if not os.path.exists(cap_path):
            try:
                os.symlink(os.path.abspath(target), cap_path)
            except FileExistsError:
                pass
            except Exception:  # noqa: BLE001 — symlink unsupported → copy
                try:
                    shutil.copyfile(target, cap_path)
                except Exception:  # noqa: BLE001
                    continue
        sample = fo.Sample(filepath=cap_path)
        sample["entity_id"] = str(row["entity_id"])
        sample["asset_id"] = asset
        sample["caption"] = row.get("text_content") or ""
        sample["embedding"] = row["embedding"]
        sample["caption_cluster"] = cluster_label.get(row["entity_id"], "none")
        sample["has_keyframe"] = bool(local)
        _sim = sim_map.get(str(row["entity_id"]))
        sample["caption_img_sim"] = float(_sim) if _sim is not None else None
        samples.append(sample)
    ds.add_samples(samples)
    if no_keyframe:
        print(f"build_caption_fiftyone_dataset: {no_keyframe} captions use placeholder (no video keyframe)")
    if umap and len(samples) >= 3:
        fob.compute_visualization(ds, embeddings="embedding", method="umap", brain_key="emb_viz")
        try:  # PCA(emb_viz_pca) 도 함께 — FiftyOne Embeddings 패널 선형/전역 투영 (UMAP 보완)
            fob.compute_visualization(ds, embeddings="embedding", method="pca", brain_key="emb_viz_pca")
        except Exception as exc:  # noqa: BLE001 — PCA 투영 optional
            print(f"PCA viz skipped: {exc}")
    return ds


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
        s["project"] = _project_of(r.get("key"))
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
        try:  # PCA(emb_viz_pca) 도 함께 — FiftyOne Embeddings 패널 선형/전역 투영 (UMAP 보완)
            fob.compute_visualization(ds, embeddings="embedding", method="pca", brain_key="emb_viz_pca")
        except Exception as exc:  # noqa: BLE001 — PCA 투영 optional
            print(f"PCA viz skipped: {exc}")
    return ds


# ── 검색 (native PG, pgvector <=> cosine) ──
def _embed_text(query: str) -> list[float]:
    import requests

    url = os.environ.get("EMBEDDING_API_URL", "http://embedding-service:8003")
    resp = requests.post(f"{url.rstrip('/')}/embed_text", data={"text": query}, timeout=60)
    resp.raise_for_status()
    return resp.json()["vector"]


def _embed_image(file_bytes: bytes, filename: str = "upload.jpg") -> list[float]:
    """업로드 이미지 바이트 → embedding-service /embed → 벡터 반환.

    /embed 는 multipart/form-data, 필드명 'file'. 응답 JSON 키: "vector" 또는 "embedding".
    _embed_text 와 동일한 URL/에러 패턴 사용.
    """
    import requests

    url = os.environ.get("EMBEDDING_API_URL", "http://embedding-service:8003")
    resp = requests.post(
        f"{url.rstrip('/')}/embed",
        files={"file": (filename, file_bytes)},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    # /embed_text 와 동일하게 "vector" 우선, fallback "embedding"
    return data.get("vector") or data["embedding"]


def search_by_uploaded_image(
    file_bytes: bytes,
    k: int = 12,
    model_name: str = DEFAULT_MODEL,
    *,
    source: str | None = None,
    image_role: str | None = None,
    daynight_type: str | None = None,
    environment_type: str | None = None,
) -> list[dict]:
    """업로드 이미지 바이트 → _embed_image → pgvector cosine top-k 유사 프레임.

    반환 dict shape = search_by_image 와 동일:
      entity_id / image_id / image_bucket / image_key / cosine_dist
    """
    vec = _embed_image(file_bytes)
    return _topk_by_vector(
        vec, k, model_name,
        source=source, image_role=image_role,
        daynight_type=daynight_type, environment_type=environment_type,
    )


def _topk_by_vector(
    vec: list[float],
    k: int,
    model_name: str,
    *,
    source: str | None = None,
    image_role: str | None = None,
    daynight_type: str | None = None,
    environment_type: str | None = None,
    ef_search: int = 100,
    candidate_multiplier: int = 3,
) -> list[dict]:
    """pgvector cosine top-k 검색. 필터 없으면 기존 동작 유지(하위호환).

    필터 파라미터:
      source          — image_key LIKE '{source}%' (prefix 매칭)
      image_role      — image_metadata.image_role 등호 필터
      daynight_type   — video_metadata.daynight_type 등호 (video_metadata JOIN 추가)
      environment_type — video_metadata.environment_type 등호 (video_metadata JOIN 추가)
      ef_search       — hnsw.ef_search SET LOCAL 값 (필터 활성 시에만 사용)
      candidate_multiplier — LIMIT = k * candidate_multiplier, 이후 cosine 재정렬 top-k
    """
    lit = "[" + ",".join(repr(float(x)) for x in vec) + "]"
    has_filter = any(v is not None for v in (source, image_role, daynight_type, environment_type))

    if not has_filter:
        # 필터 없음 → 기존 동작 그대로 (SET LOCAL 없음, EXACT ANN)
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

    # 필터 활성: SET LOCAL + JOIN + 조건부 WHERE + candidate_multiplier
    need_vm_join = daynight_type is not None or environment_type is not None
    cand_k = k * candidate_multiplier

    # 동적 WHERE 조각 조립 (psycopg2 파라미터 바인딩만 사용, 값 인젝션 없음)
    where_clauses: list[str] = [
        "e.entity_type = 'frame'",
        "e.model_name = %(model)s",
    ]
    params: dict = {"q": lit, "model": model_name, "k": k, "cand_k": cand_k, "ef": ef_search}

    if source is not None:
        where_clauses.append("im.image_key LIKE %(source_prefix)s")
        params["source_prefix"] = source + "%"  # prefix-only LIKE; suffix 와일드카드 금지
    if image_role is not None:
        where_clauses.append("im.image_role = %(image_role)s")
        params["image_role"] = image_role
    if daynight_type is not None:
        where_clauses.append("vm.daynight_type = %(daynight_type)s")
        params["daynight_type"] = daynight_type
    if environment_type is not None:
        where_clauses.append("vm.environment_type = %(environment_type)s")
        params["environment_type"] = environment_type

    vm_join = (
        "\n  JOIN video_metadata vm ON vm.asset_id = im.source_asset_id"
        if need_vm_join else ""
    )
    where_sql = "\n  AND ".join(where_clauses)

    # SET LOCAL은 psycopg2 기본 트랜잭션(autocommit=False) 안에서 유효
    set_sql = (
        "SET LOCAL hnsw.iterative_scan = 'relaxed_order'; "
        "SET LOCAL hnsw.ef_search = %(ef)s; "
        "SET LOCAL hnsw.max_scan_tuples = 50000;"
    )
    main_sql = f"""
        WITH cand AS MATERIALIZED (
          SELECT e.entity_id, e.image_id, im.image_bucket, im.image_key,
                 (e.embedding <=> %(q)s::vector) AS cosine_dist
          FROM image_embeddings e
          JOIN image_metadata im ON im.image_id = e.image_id{vm_join}
          WHERE {where_sql}
          ORDER BY e.embedding <=> %(q)s::vector
          LIMIT %(cand_k)s
        )
        SELECT * FROM cand ORDER BY cosine_dist LIMIT %(k)s
    """

    with _pg_conn() as conn, conn.cursor() as cur:
        # SET LOCAL 후 동일 커서로 SELECT — 같은 트랜잭션 내 유효
        cur.execute(set_sql, params)
        cur.execute(main_sql, params)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── 쿼리 KO→EN 번역 (cross-lingual 검색) ──
# PE-Core 텍스트 인코더는 영어 중심 — '화재'(한국어)는 degenerate, 'fire'(영어)는 잘 임베딩됨
# (cos(fire,화재)≈0.74로 부족). 벡터 검색 전 '쿼리'를 영어로 번역 → 한국어/영어가 같은 임베딩 → 동일/유사 결과.
# ※ 이미 적재된 DB 임베딩은 절대 안 건드림 — '쿼리 텍스트'만 번역해 검색에 사용 (DB 재임베딩 없음).
# ※ 캡션 키워드(pg_trgm) 검색은 한국어 원문 유지; 이건 텍스트→이미지/캡션 '벡터' 경로에만 적용.
# 번역 우선순위: ① 한글 없으면 무변경 → ② 도메인 사전 완전일치(즉시·결정적, '화재'==='fire')
#   → ③ Vertex Gemini 일반 번역(임의 한국어 문장 커버, in-process 캐시) → ④ Vertex 불가 시 사전 부분치환.
_KO_EN_QUERY_MAP: dict[str, str] = {
    "화재": "fire", "불": "fire", "불꽃": "flame", "화염": "flame", "연기": "smoke",
    "낙상": "fall", "넘어짐": "falling", "넘어진": "fallen person", "넘어지": "falling", "넘어": "falling",
    "쓰러짐": "collapse", "쓰러진": "collapsed person", "쓰러지": "collapsing",
    "누워": "lying down", "눕": "lying down", "바닥": "floor",
    "사람": "person", "사람이": "a person", "환자": "patient", "노인": "elderly person",
    "아이": "child", "어린이": "child", "휠체어": "wheelchair",
    "간호사": "nurse", "의료진": "medical staff", "간병인": "caregiver",
    "싸움": "fight", "폭행": "assault", "주먹": "fist", "칼": "knife", "흉기": "weapon", "총": "gun",
    "도로": "road", "차량": "vehicle", "자동차": "car", "오토바이": "motorcycle", "트럭": "truck",
    "자전거": "bicycle", "보행자": "pedestrian", "횡단보도": "crosswalk",
    "야간": "night", "밤": "night", "주간": "daytime", "낮": "daytime",
    "실내": "indoor", "실외": "outdoor", "복도": "hallway", "계단": "stairs", "주차장": "parking lot",
    "폭발": "explosion", "구조": "rescue", "응급": "emergency", "사고": "accident",
}


def _has_hangul(s: str) -> bool:
    """문자열에 한글(완성형 음절/자모/호환자모)이 하나라도 있으면 True."""
    return any(
        "가" <= ch <= "힣"  # 완성형 음절 (가-힣)
        or "ᄀ" <= ch <= "ᇿ"  # 한글 자모
        or "㄰" <= ch <= "㆏"  # 호환 자모
        for ch in s
    )


def _dict_substitute(q: str) -> str:
    """도메인 사전 부분치환 (Vertex 불가 시 fallback). 긴 키 우선, 매핑 없는 토큰은 그대로."""
    out = q
    for ko, en in sorted(_KO_EN_QUERY_MAP.items(), key=lambda kv: -len(kv[0])):
        if ko in out:
            out = out.replace(ko, " " + en + " ")
    out = " ".join(out.split())
    return out or q


# Vertex Gemini 번역기 — lazy init + in-process 캐시. 미설치/미인증/실패 시 None 반환 → 사전 fallback.
#   creds/project/location 은 메인 파이프라인과 동일 env (GOOGLE_APPLICATION_CREDENTIALS / GEMINI_PROJECT / GEMINI_LOCATION).
#   ENABLE_VERTEX_QUERY_TRANSLATION=0 으로 끄면 사전만 사용.
_VERTEX_TRANSLATE_CACHE: dict[str, str] = {}
_vertex_client: Any = None
_vertex_client_tried = False
_VERTEX_TRANSLATE_PROMPT = (
    "You translate Korean CCTV/security image-search queries into English. "
    "Output ONLY a short, natural English search phrase (no quotes, no explanation, no trailing period). "
    "Keep it concise like an image caption keyword phrase.\n\nKorean: {q}\nEnglish:"
)


def _vertex_translation_enabled() -> bool:
    return os.getenv("ENABLE_VERTEX_QUERY_TRANSLATION", "1").strip().lower() not in ("0", "false", "no", "off", "")


def _get_vertex_client() -> Any:
    """genai.Client(vertexai=True) lazy 생성. 한 번 실패하면 재시도하지 않고 None 고정."""
    global _vertex_client, _vertex_client_tried
    if _vertex_client is not None:
        return _vertex_client
    if _vertex_client_tried:
        return None
    _vertex_client_tried = True
    try:
        from google import genai  # lazy — SDK 없으면 ImportError → 사전 fallback

        project = (os.getenv("GEMINI_PROJECT") or "your-gcp-project").strip()
        location = (os.getenv("GEMINI_LOCATION") or "us-central1").strip()
        _vertex_client = genai.Client(vertexai=True, project=project, location=location)
    except Exception as exc:  # SDK 미설치 / 인증 실패 등 → 사전 fallback
        print(f"[ko_en] Vertex 번역기 init 실패 — 사전 fallback 사용: {exc!r}", flush=True)
        _vertex_client = None
    return _vertex_client


def _vertex_translate(q: str) -> str | None:
    """Vertex Gemini 로 한국어 쿼리 → 영어. 캐시 hit/성공 시 영어, 그 외 None."""
    if not _vertex_translation_enabled():
        return None
    if q in _VERTEX_TRANSLATE_CACHE:
        return _VERTEX_TRANSLATE_CACHE[q]
    client = _get_vertex_client()
    if client is None:
        return None
    model = (os.getenv("GEMINI_TRANSLATE_MODEL") or "gemini-2.5-flash").strip()
    try:
        resp = client.models.generate_content(model=model, contents=_VERTEX_TRANSLATE_PROMPT.format(q=q))
        out = (getattr(resp, "text", "") or "").strip().strip('"').strip()
        if out:
            _VERTEX_TRANSLATE_CACHE[q] = out
            return out
    except Exception as exc:  # API 오류/타임아웃 등 → 사전 fallback
        print(f"[ko_en] Vertex 번역 실패(q={q!r}) — 사전 fallback: {exc!r}", flush=True)
    return None


def translate_query_ko_en(query: str) -> str:
    """검색 쿼리의 한국어를 영어로 번역 (벡터검색 cross-lingual 정합).

    ① 한글 없으면 무변경(영어/언어중립) → ② 도메인 사전 완전일치(즉시·결정적, '화재'==='fire')
    → ③ Vertex Gemini 일반 번역(임의 한국어 문장, 캐시) → ④ Vertex 불가 시 사전 부분치환 fallback.
    ※ DB 임베딩은 건드리지 않음 — 쿼리 텍스트만 번역.
    예: '화재'→'fire', '주차장에서 두 사람이 다투고 있다'→'Two people fighting in parking lot'.
    """
    q = (query or "").strip()
    if not q:
        return query
    if not _has_hangul(q):
        return q  # 영어/숫자 등 한글 없는 쿼리 무변경
    if q in _KO_EN_QUERY_MAP:
        return _KO_EN_QUERY_MAP[q]  # 도메인 용어 완전일치 — 결정적, API 호출 없음
    v = _vertex_translate(q)
    if v:
        return v
    return _dict_substitute(q)  # Vertex 불가 시 사전 부분치환


def search_by_text(
    query: str,
    k: int = 20,
    model_name: str = DEFAULT_MODEL,
    *,
    source: str | None = None,
    image_role: str | None = None,
    daynight_type: str | None = None,
    environment_type: str | None = None,
) -> list[dict]:
    """텍스트 → embedding-service /embed_text → frame pgvector cosine top-k (cross-modal text→image).

    한국어 도메인 용어는 영어로 치환 후 임베딩 (PE-Core 영어중심 → '화재'=='fire' 동일 결과).
    """
    return _topk_by_vector(
        _embed_text(translate_query_ko_en(query)), k, model_name,
        source=source, image_role=image_role,
        daynight_type=daynight_type, environment_type=environment_type,
    )


def search_captions_by_text(query: str, k: int = 20, model_name: str = DEFAULT_MODEL) -> list[dict]:
    """텍스트 → caption embeddings cosine top-k (한국어 도메인 용어는 영어로 치환 후 임베딩)."""
    vec = _embed_text(translate_query_ko_en(query))
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


def search_captions_by_keyword(query: str, k: int = 20, threshold: float = 0.05) -> list[dict]:
    """한국어 캡션 키워드 검색 — ILIKE 부분일치 + pg_trgm trigram 매칭.

    labels 테이블에서 caption_text ILIKE '%query%' 또는 trigram 유사도(%%  연산자) 매칭.
    반환: [{entity_id, asset_id, caption_text, event_index, substring_hit, lexical_score}]
    threshold: pg_trgm.similarity_threshold — 짧은 한국어 형태소 대비 낮게(0.05) 설정.
    """
    try:
        # ILIKE 패턴에서 % 와 _ 를 이스케이프 (SQL 와일드카드 오염 방지)
        escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        pat = "%" + escaped + "%"  # substring 매칭 패턴

        select_sql = """
            SELECT l.label_id    AS entity_id,
                   l.asset_id,
                   l.caption_text,
                   l.event_index,
                   (l.caption_text ILIKE %(pat)s)                    AS substring_hit,
                   similarity(l.caption_text, %(q)s)                 AS lexical_score
            FROM labels l
            WHERE l.caption_text IS NOT NULL
              AND l.caption_text <> ''
              AND (l.caption_text ILIKE %(pat)s OR l.caption_text %% %(q)s)
            ORDER BY substring_hit DESC, lexical_score DESC, l.created_at DESC
            LIMIT %(k)s
        """
        params = {"thr": threshold, "pat": pat, "q": query, "k": k}
        with _pg_conn() as conn, conn.cursor() as cur:
            # SET LOCAL: 트랜잭션 종료 시 원래 값 복원 (다른 세션에 영향 없음)
            cur.execute("SET LOCAL pg_trgm.similarity_threshold = %(thr)s", {"thr": threshold})
            cur.execute(select_sql, params)
            cols = [c.name for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:  # noqa: BLE001 — 캡션 키워드 검색 fail-forward
        print(f"search_captions_by_keyword: 실패 (pg_trgm 미설치 또는 DB 오류): {exc}")
        return []


def search_captions(
    query: str,
    k: int = 20,
    model_name: str = DEFAULT_MODEL,
    *,
    mode: str = "keyword",
    semantic_query: str | None = None,
) -> list[dict]:
    """캡션 통합 검색 — keyword / semantic / hybrid 모드.

    mode='keyword'  — search_captions_by_keyword 만 호출 (embedding-service 미사용, 한국어 기본).
    mode='semantic' — caption 벡터 kNN (_embed_text(semantic_query or query) → entity_type='caption').
    mode='hybrid'   — keyword + semantic 결과를 RRF(rrf_k=60) 로 융합.

    ⚠️ PE-Core 텍스트 인코더는 영어 중심 — 한국어 semantic 쿼리는 degenerate 결과 가능.
       hybrid/semantic 시 semantic_query 를 영어로 주는 것을 권장.
    """
    if mode == "keyword":
        return search_captions_by_keyword(query, k=k)

    if mode == "semantic":
        return search_captions_by_text(semantic_query or query, k=k, model_name=model_name)

    if mode == "hybrid":
        # keyword 결과: [{entity_id=label_id, ...}]
        kw_hits = search_captions_by_keyword(query, k=k)
        # semantic 결과: [{entity_id, asset_id, text_content, cosine_dist}]
        sem_hits = search_captions_by_text(semantic_query or query, k=k, model_name=model_name)

        # label_id 기준 RRF 융합 (rrf_k=60)
        rrf_k = 60
        kw_rank: dict[str, int] = {
            str(h.get("entity_id")): i + 1 for i, h in enumerate(kw_hits)
        }
        sem_rank: dict[str, int] = {
            str(h.get("entity_id")): i + 1 for i, h in enumerate(sem_hits)
        }
        # 모든 고유 label_id 수집
        all_ids: set[str] = set(kw_rank) | set(sem_rank)

        # kw_hits 와 sem_hits 를 label_id 로 인덱싱 (메타데이터 병합용)
        kw_by_id = {str(h.get("entity_id")): h for h in kw_hits}
        sem_by_id = {str(h.get("entity_id")): h for h in sem_hits}

        rrf_rows: list[dict] = []
        for lid in all_ids:
            kr = kw_rank.get(lid, 0)
            sr = sem_rank.get(lid, 0)
            # RRF: 존재하는 랭크만 기여 (rank=0 → 기여 없음)
            score = (1.0 / (rrf_k + kr) if kr > 0 else 0.0) + (1.0 / (rrf_k + sr) if sr > 0 else 0.0)
            # 메타데이터: keyword 행 우선, 없으면 semantic 행에서 가져옴
            kw_h = kw_by_id.get(lid, {})
            sem_h = sem_by_id.get(lid, {})
            row: dict = {
                "entity_id": lid,
                "asset_id": kw_h.get("asset_id") or sem_h.get("asset_id"),
                "caption_text": kw_h.get("caption_text") or sem_h.get("text_content"),
                "event_index": kw_h.get("event_index"),
                "substring_hit": kw_h.get("substring_hit", False),
                "lexical_score": kw_h.get("lexical_score", 0.0),
                "cosine_dist": sem_h.get("cosine_dist"),
                "rrf_score": round(score, 6),
            }
            rrf_rows.append(row)

        rrf_rows.sort(key=lambda r: r["rrf_score"], reverse=True)
        return rrf_rows[:k]

    # 알 수 없는 mode → keyword fallback
    print(f"search_captions: 알 수 없는 mode={mode!r}, keyword 로 fallback.")
    return search_captions_by_keyword(query, k=k)


def search_by_image(
    image_id: str,
    k: int = 20,
    model_name: str = DEFAULT_MODEL,
    *,
    source: str | None = None,
    image_role: str | None = None,
    daynight_type: str | None = None,
    environment_type: str | None = None,
) -> list[dict]:
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
    return _topk_by_vector(
        _parse_vector(row[0]), k, model_name,
        source=source, image_role=image_role,
        daynight_type=daynight_type, environment_type=environment_type,
    )


# ── caption ↔ image 정합 (cross-modal alignment) ──
# caption(text) 임베딩이 자기 영상의 frame(image) 임베딩에 얼마나 가까운지 cosine 으로 측정.
#   matched    = 캡션 ↔ 자기 asset 프레임 (mean/best own-asset frame cosine)
#   mismatched = 캡션 ↔ 무작위 다른 asset 프레임 (chance baseline 분포)
#   gap = matched_mean - mismatched_mean. 클수록 캡션 임베딩이 자기 이미지에 잘 정렬됨.
# ⚠️ caption+frame 둘 다 임베딩된 asset 만 측정 가능. 캡션이 한국어로 임베딩된 동안은
#    텍스트 인코더가 degenerate → matched≈mismatched(gap≈0). 영어 재임베딩 후 분리됨.
_CAP_FRM_CTE = """
    cap AS (
        SELECT entity_id, asset_id, embedding
        FROM image_embeddings
        WHERE entity_type = 'caption' AND model_name = %(m)s AND asset_id IS NOT NULL
    ),
    frm AS (
        SELECT im.source_asset_id AS asset_id, e.embedding
        FROM image_embeddings e
        JOIN image_metadata im ON im.image_id = e.image_id
        WHERE e.entity_type = 'frame' AND e.model_name = %(m)s AND im.source_asset_id IS NOT NULL
    )
"""


def _caption_matched_rows(model_name: str = DEFAULT_MODEL) -> list[tuple]:
    """[(entity_id, asset_id, matched_mean, matched_best, n_frames)] — 캡션별 자기영상 프레임 cosine.

    cosine_sim = 1 - (caption_emb <=> frame_emb). 같은 asset 내 join 이라 bounded.
    caption+frame 둘 다 임베딩된 캡션만 반환 (현재 prod 매칭 universe ~330).
    """
    sql = f"""
        WITH {_CAP_FRM_CTE}
        SELECT c.entity_id, c.asset_id,
               avg(1 - (c.embedding <=> f.embedding))::float8 AS matched_mean,
               max(1 - (c.embedding <=> f.embedding))::float8 AS matched_best,
               count(*)::int AS n_frames
        FROM cap c JOIN frm f ON f.asset_id = c.asset_id
        GROUP BY c.entity_id, c.asset_id
    """
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"m": model_name})
        return [(str(eid), str(aid), float(mm), float(mb), int(nf)) for eid, aid, mm, mb, nf in cur.fetchall()]


def _caption_mismatch_sims(model_name: str = DEFAULT_MODEL, n: int = 2000) -> list[float]:
    """무작위 cross-asset (caption, frame) 쌍의 cosine 유사도 — chance baseline 분포.

    표본 캡션마다 LATERAL 로 '다른 asset' 프레임 1개를 무작위 선택 → same-asset 드롭/빈 결과 없이 n 쌍 보장.
    """
    sql = f"""
        WITH {_CAP_FRM_CTE}
        SELECT (1 - (c.embedding <=> f.embedding))::float8 AS sim
        FROM (SELECT embedding, asset_id FROM cap ORDER BY random() LIMIT %(n)s) c
        CROSS JOIN LATERAL (
            SELECT embedding FROM frm WHERE frm.asset_id <> c.asset_id ORDER BY random() LIMIT 1
        ) f
    """
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"m": model_name, "n": n})
        return [float(r[0]) for r in cur.fetchall()]


def caption_image_alignment(model_name: str = DEFAULT_MODEL, baseline_n: int = 2000) -> dict:
    """캡션↔이미지 정합 측정. returns dict(matched, matched_best, mismatched, n_matched, n_assets, *_mean, gap)."""
    rows = _caption_matched_rows(model_name)
    matched = [r[2] for r in rows]  # 캡션별 자기영상 frame cosine 평균
    matched_best = [r[3] for r in rows]
    mismatched = _caption_mismatch_sims(model_name, baseline_n)
    n_assets = len({r[1] for r in rows})

    def _mean(xs: list[float]) -> float:
        return sum(xs) / len(xs) if xs else float("nan")

    mm, mim = _mean(matched), _mean(mismatched)
    return {
        "matched": matched,
        "matched_best": matched_best,
        "mismatched": mismatched,
        "n_matched": len(matched),
        "n_assets": n_assets,
        "matched_mean": mm,
        "mismatched_mean": mim,
        "gap": (mm - mim) if (matched and mismatched) else float("nan"),
    }


def fetch_caption_image_sim(model_name: str = DEFAULT_MODEL) -> dict[str, float]:
    """entity_id → 자기영상 best-frame cosine 유사도 (caption+frame 둘 다 임베딩된 캡션만). FiftyOne 필드용."""
    try:
        return {r[0]: r[3] for r in _caption_matched_rows(model_name)}
    except Exception as exc:  # noqa: BLE001 — 정합 필드 optional
        print(f"fetch_caption_image_sim skipped: {exc}")
        return {}


# ── kNN 라벨 의심 스코어링 (데이터 품질) ──
# 임베딩 공간에서 k개 이웃 중 다른 라벨이 많으면 라벨 오류/혼동 후보로 표시.
# caption-contradiction: 라벨='none' 이지만 캡션에 사건 키워드가 포함된 경우 추가 신호.

import re

DQ_FULL_MATRIX_MAX_N = 20000

_CONTRADICTION_EN_KEYWORDS = frozenset({"fall", "fallen", "lying", "fire", "flame", "smoke"})
_CONTRADICTION_KO_KEYWORDS = frozenset({"넘어", "쓰러", "눕", "화재", "연기", "낙상"})

_CLASS_CONTRADICTION_KEYWORDS: dict[str, tuple[frozenset[str], frozenset[str]]] = {
    "fall": (frozenset({"fall", "fallen", "lying"}), frozenset({"넘어", "쓰러", "눕", "낙상"})),
    "fire": (frozenset({"fire", "flame"}), frozenset({"화재"})),
    "smoke": (frozenset({"smoke"}), frozenset({"연기"})),
}


def _english_keyword_in_text(text: str, keyword: str) -> bool:
    return re.search(rf"\b{re.escape(keyword)}\b", text, flags=re.IGNORECASE) is not None


def _caption_keyword_classes(caption: str | None) -> set[str]:
    text = str(caption or "")
    if not text:
        return set()
    classes: set[str] = set()
    for cls, (english_kws, korean_kws) in _CLASS_CONTRADICTION_KEYWORDS.items():
        if any(_english_keyword_in_text(text, kw) for kw in english_kws):
            classes.add(cls)
            continue
        if any(kw in text for kw in korean_kws):
            classes.add(cls)
    return classes


def _caption_contradiction_weight(label: str, caption: str | None) -> float:
    """Contradiction signal weight (0.0–1.0).

    label='none' but caption mentions an event → 1.0 (full contradiction).
    label=event class but caption has no matching keywords → 0.5 (weak contradiction).
    """
    own_label = normalize_class(label)
    caption_classes = _caption_keyword_classes(caption)
    if own_label == "none":
        return 1.0 if caption_classes else 0.0
    if own_label in _CLASS_CONTRADICTION_KEYWORDS and own_label not in caption_classes:
        return 0.5
    return 0.0


def _count_frames_for_dq(model_name: str = DEFAULT_MODEL) -> int:
    sql = """
        SELECT COUNT(*)
        FROM image_embeddings e
        JOIN image_metadata im ON im.image_id = e.image_id
        WHERE e.entity_type = 'frame' AND e.model_name = %(model)s
    """
    try:
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, {"model": model_name})
            row = cur.fetchone()
            return int(row[0]) if row else 0
    except Exception as exc:  # noqa: BLE001
        print(f"_count_frames_for_dq: count skipped: {exc}")
        return 0


def _load_frames_for_dq(
    model_name: str = DEFAULT_MODEL, limit: int | None = None, image_ids: list[str] | None = None
) -> list[dict]:
    """image_embeddings(frame) + image_metadata → DQ 계산용 레코드 로드.

    image_ids 지정 시 해당 이미지만 로드(예: 라벨된 FiftyOne 프레임만 → 132K+ 전체 풀로드 방지).
    """
    sql = """
        SELECT e.image_id, e.embedding,
               im.image_bucket, im.image_key
        FROM image_embeddings e
        JOIN image_metadata im ON im.image_id = e.image_id
        WHERE e.entity_type = 'frame' AND e.model_name = %(model)s
    """
    params: dict[str, object] = {"model": model_name}
    if image_ids is not None:
        sql += "\n          AND e.image_id = ANY(%(ids)s)"
        params["ids"] = [str(i) for i in image_ids]
    sql += "\n        ORDER BY e.image_id"
    if limit is not None:
        limit_int = max(0, int(limit))
        if limit_int <= 0:
            return []
        sql += "\n        LIMIT %(limit)s"
        params["limit"] = limit_int
    rows: list[dict] = []
    try:
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            for image_id, emb, bucket, key in cur.fetchall():
                rows.append(
                    {
                        "image_id": str(image_id),
                        "embedding": _parse_vector(emb),
                        "bucket": bucket or "",
                        "key": key or "",
                        "minio_key": f"{bucket}/{key}" if bucket and key else "",
                    }
                )
    except Exception as exc:  # noqa: BLE001
        print(f"_load_frames_for_dq: embedding load skipped: {exc}")
    return rows


def _load_fo_metadata_for_dq(label_field: str = "normalized_class") -> dict[str, dict]:
    """FiftyOne 'frames' 단일 스캔 → image_id: {label, caption}. 두 번 스캔하던 것을 통합."""
    result: dict[str, dict] = {}
    try:
        import fiftyone as fo

        if not fo.dataset_exists("frames"):
            return result
        ds = fo.load_dataset("frames")
        for s in ds:
            iid = _sample_value(s, "image_id")
            if not iid:
                continue
            label = _sample_value(s, label_field)
            if label is None:
                label = normalize_class(_sample_value(s, "detection_class"))
            # stored value accepted as-is (already normalized by attach_labels or prior write)
            caption = _sample_value(s, "caption", "")
            result[str(iid)] = {
                "label": str(label) if label else "none",
                "caption": str(caption) if caption else "",
            }
    except Exception as exc:  # noqa: BLE001
        print(f"_load_fo_metadata_for_dq: metadata load skipped: {exc}")
    return result


def compute_label_suspect(
    model_name: str = DEFAULT_MODEL,
    k: int = 10,
    label_field: str = "normalized_class",
) -> dict:
    """임베딩 kNN 기반 라벨 의심 스코어 계산.

    Returns dict(scores, rows[top200], n, truncated). suspect_score = 0.6*knn_disagree + 0.4*contradiction.
    N <= DQ_FULL_MATRIX_MAX_N 제한 (초과 시 image_id 순 앞부분만, truncated=True).
    """
    result_n = 0
    truncated = False
    try:
        import numpy as np

        total_n = _count_frames_for_dq(model_name)
        result_n = total_n
        if total_n < 2:
            return {"scores": {}, "rows": [], "n": total_n, "truncated": False}

        truncated = total_n > DQ_FULL_MATRIX_MAX_N
        limit = DQ_FULL_MATRIX_MAX_N if truncated else None
        if truncated:
            print(
                f"compute_label_suspect: N={total_n} > DQ_FULL_MATRIX_MAX_N={DQ_FULL_MATRIX_MAX_N}; "
                "returning partial result for first rows ordered by image_id."
            )

        dq_rows = _load_frames_for_dq(model_name, limit=limit)
        n = len(dq_rows)
        if n < 2:
            return {"scores": {}, "rows": [], "n": n, "truncated": False}

        image_ids = [r["image_id"] for r in dq_rows]
        minio_keys = {r["image_id"]: r["minio_key"] for r in dq_rows}

        fo_metadata = _load_fo_metadata_for_dq(label_field)
        labels = [str(fo_metadata.get(iid, {}).get("label") or "none") for iid in image_ids]
        captions = [str(fo_metadata.get(iid, {}).get("caption") or "") for iid in image_ids]

        k_eff = min(max(1, int(k)), n - 1)

        mat = np.array([r["embedding"] for r in dq_rows], dtype="float32")
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1.0, norms)
        mat_norm = mat / norms
        sim_mat = mat_norm @ mat_norm.T

        scores_list = []
        for i in range(n):
            try:
                sims = sim_mat[i].copy()
                sims[i] = -2.0
                top_idx = np.argpartition(sims, -k_eff)[-k_eff:]
                top_idx = top_idx[np.argsort(sims[top_idx])[::-1]]

                neighbor_labels = [labels[j] for j in top_idx]
                own_label = labels[i]
                knn_disagree = float(sum(1 for nl in neighbor_labels if nl != own_label) / k_eff)

                neighbor_counter = Counter(neighbor_labels)
                neighbor_majority = neighbor_counter.most_common(1)[0][0] if neighbor_counter else "none"

                contradiction = _caption_contradiction_weight(own_label, captions[i])
                suspect_score = float(min(1.0, max(0.0, 0.6 * knn_disagree + 0.4 * contradiction)))

                scores_list.append(
                    {
                        "image_id": image_ids[i],
                        "label": own_label,
                        "neighbor_majority": neighbor_majority,
                        "knn_disagree": round(knn_disagree, 4),
                        "contradiction": round(contradiction, 4),
                        "suspect_score": round(suspect_score, 4),
                        "minio_key": minio_keys.get(image_ids[i], ""),
                    }
                )
            except Exception as exc:  # noqa: BLE001 — per-frame fail-forward
                print(f"compute_label_suspect: skipped image_id={image_ids[i]}: {exc}")
                scores_list.append(
                    {
                        "image_id": image_ids[i],
                        "label": labels[i],
                        "neighbor_majority": "?",
                        "knn_disagree": 0.0,
                        "contradiction": 0.0,
                        "suspect_score": 0.0,
                        "minio_key": minio_keys.get(image_ids[i], ""),
                    }
                )

        scores_map = {r["image_id"]: r["suspect_score"] for r in scores_list}
        top_rows = sorted(scores_list, key=lambda x: x["suspect_score"], reverse=True)[:200]
        return {"scores": scores_map, "rows": top_rows, "n": total_n, "truncated": truncated}
    except Exception as exc:  # noqa: BLE001
        print(f"compute_label_suspect: skipped: {exc}")
        return {"scores": {}, "rows": [], "n": result_n, "truncated": truncated}


# ── 클래스 분리도 리포트 (데이터 품질) ──
# 라벨별 임베딩 centroid 간 cosine 유사도 + silhouette + kNN purity.
# 분리도가 높을수록 각 클래스가 임베딩 공간에서 잘 구분됨 → 학습 데이터 품질 지표.

def class_separation_report(
    label_field: str = "normalized_class",
    model_name: str = DEFAULT_MODEL,
    exclude_none: bool = True,
) -> dict:
    """임베딩 공간 기준 클래스 분리도 리포트 (silhouette + centroid cosine + kNN purity). 가드 포함."""
    warnings_list: list[str] = []
    try:
        import numpy as np
    except Exception as exc:  # noqa: BLE001
        print(f"class_separation_report: numpy import failed: {exc}")
        return {}

    try:
        fo_metadata = _load_fo_metadata_for_dq(label_field)
        if not fo_metadata:
            print("class_separation_report: 라벨 없음 (FiftyOne 데이터셋 확인 필요)")
            return {}

        # 라벨된 FiftyOne 프레임(≤2740)만 로드 — 132K 전체 풀로드 방지 (라벨 누락 없이 bounded)
        dq_rows = _load_frames_for_dq(model_name, image_ids=list(fo_metadata.keys()))
        if not dq_rows:
            return {}

        filtered: list[tuple[list[float], str]] = []
        for r in dq_rows:
            lbl = str(fo_metadata.get(r["image_id"], {}).get("label") or "none")
            if exclude_none and lbl == "none":
                continue
            if lbl == "":
                continue
            filtered.append((r["embedding"], lbl))

        if len(filtered) < 4:
            print(f"class_separation_report: 유효 샘플이 {len(filtered)}개로 부족, 스킵.")
            return {}

        embs = np.array([f[0] for f in filtered], dtype="float32")
        label_list = [f[1] for f in filtered]
        counts: dict[str, int] = dict(Counter(label_list))
        unique_labels = sorted(counts.keys())

        small_classes = sorted(lbl for lbl, cnt in counts.items() if cnt == 1)
        if small_classes:
            warnings_list.append(
                f"클래스 {small_classes}는 샘플 수=1; purity가 0으로 나오는 것은 정상 (global kNN 결과)."
            )

        if len(unique_labels) < 2:
            print(f"class_separation_report: 클래스가 {len(unique_labels)}개 (<2), silhouette 불가.")
            silhouette_val = None
        elif len(filtered) > DQ_FULL_MATRIX_MAX_N:
            warnings_list.append(f"silhouette 스킵: 샘플 수 {len(filtered)} > {DQ_FULL_MATRIX_MAX_N}.")
            silhouette_val = None
        else:
            try:
                from sklearn.metrics import silhouette_score

                silhouette_val = float(silhouette_score(embs, label_list, metric="cosine"))
            except Exception as exc:  # noqa: BLE001
                print(f"class_separation_report: silhouette 계산 실패: {exc}")
                silhouette_val = None

        centroids = {}
        for lbl in unique_labels:
            idx = [i for i, lb in enumerate(label_list) if lb == lbl]
            c = embs[idx].mean(axis=0)
            norm = float(np.linalg.norm(c))
            centroids[lbl] = c / norm if norm > 0 else c

        cent_mat = np.array([centroids[lb] for lb in unique_labels], dtype="float32")
        cos_sim = (cent_mat @ cent_mat.T).tolist()

        min_class_size = min(counts.values()) if counts else 0
        k_pur = min(10, min_class_size - 1, len(filtered) - 1)

        purity: dict[str, float] = {}
        purity_skipped_reason: str | None = None

        if len(filtered) > DQ_FULL_MATRIX_MAX_N:
            purity_skipped_reason = "too many samples"
            warnings_list.append(f"purity 스킵: 샘플 수 {len(filtered)} > {DQ_FULL_MATRIX_MAX_N}.")
        elif k_pur <= 0:
            purity_skipped_reason = "small class"
            warnings_list.append("purity 스킵: 샘플 수=1인 클래스 포함, kNN 계산 불가.")
        else:
            norms = np.linalg.norm(embs, axis=1, keepdims=True)
            norms = np.where(norms == 0, 1.0, norms)
            embs_norm = embs / norms
            sim_full = embs_norm @ embs_norm.T
            for lbl in unique_labels:
                purity_vals = []
                for i, lb in enumerate(label_list):
                    if lb != lbl:
                        continue
                    sims = sim_full[i].copy()
                    sims[i] = -2.0
                    top_idx = np.argpartition(sims, -k_pur)[-k_pur:]
                    neighbor_labels = [label_list[j] for j in top_idx]
                    purity_vals.append(sum(1 for nl in neighbor_labels if nl == lbl) / k_pur)
                purity[lbl] = round(float(sum(purity_vals) / len(purity_vals)), 4) if purity_vals else 0.0

        result: dict = {
            "n": len(filtered),
            "labels": unique_labels,
            "counts": counts,
            "silhouette": silhouette_val,
            "centroid_names": unique_labels,
            "centroid_cos": cos_sim,
            "purity": purity,
        }
        if warnings_list:
            result["warnings"] = warnings_list
        if small_classes:
            result["small_classes"] = small_classes
        if purity_skipped_reason:
            result["purity_skipped_reason"] = purity_skipped_reason
        return result
    except Exception as exc:  # noqa: BLE001
        print(f"class_separation_report: skipped: {exc}")
        return {}


def class_separation_figure(label_field: str = "normalized_class"):
    """class_separation_report centroid cosine 유사도 heatmap (plotly). cluster_distance_heatmap 패턴 재사용."""
    import plotly.graph_objects as go

    rep = class_separation_report(label_field=label_field)
    centroid_cos = rep.get("centroid_cos") if rep else None
    if not rep or not rep.get("centroid_names") or not centroid_cos or len(centroid_cos) < 2:
        fig = go.Figure()
        fig.add_annotation(text="분리도 데이터 없음", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    names = rep["centroid_names"]
    counts = rep["counts"]
    labels_annotated = [f"{n} (n={counts.get(n,0)})" for n in names]
    z = rep["centroid_cos"]
    sil = rep.get("silhouette")
    sil_txt = f"{sil:.3f}" if sil is not None else "n/a"

    # z 값 어노테이션 텍스트
    z_text = [[f"{v:.2f}" for v in row] for row in z]

    fig = go.Figure(
        go.Heatmap(
            z=z,
            x=labels_annotated,
            y=labels_annotated,
            colorscale="RdBu",
            zmin=-1,
            zmax=1,
            text=z_text,
            texttemplate="%{text}",
            colorbar={"title": "cosine sim"},
        )
    )
    fig.update_layout(
        title=f"클래스 centroid cosine 유사도 (silhouette={sil_txt})",
        width=720,
        height=640,
        xaxis={"tickangle": -35},
    )
    return fig


def caption_alignment_figure(model_name: str = DEFAULT_MODEL, baseline_n: int = 2000):
    """matched vs mismatched cosine 분포 overlaid histogram. returns (go.Figure, summary dict)."""
    import plotly.graph_objects as go

    a = caption_image_alignment(model_name, baseline_n=baseline_n)
    fig = go.Figure()
    fig.add_trace(
        go.Histogram(
            x=a["matched"], name=f"matched 자기영상 (n={a['n_matched']})",
            opacity=0.7, marker_color="#2ca02c", histnorm="probability density", nbinsx=40,
        )
    )
    fig.add_trace(
        go.Histogram(
            x=a["mismatched"], name=f"mismatched 다른영상 (n={len(a['mismatched'])})",
            opacity=0.55, marker_color="#d62728", histnorm="probability density", nbinsx=40,
        )
    )
    for val, color, label in [
        (a["matched_mean"], "#2ca02c", "matched μ"),
        (a["mismatched_mean"], "#d62728", "mismatch μ"),
    ]:
        if val == val:  # NaN 가드
            fig.add_vline(x=val, line_dash="dash", line_color=color, annotation_text=f"{label}={val:.3f}")
    gap_txt = f"{a['gap']:.3f}" if a["gap"] == a["gap"] else "n/a"
    fig.update_layout(
        barmode="overlay",
        title=f"캡션↔이미지 cosine 정합 (gap={gap_txt})",
        xaxis_title="cosine similarity", yaxis_title="density",
        legend={"orientation": "h"},
    )
    return fig, a


# ── 클러스터 간 거리 (centroid cosine) — FiftyOne 의 UMAP 시각근접과 달리 정량 거리 ──
# 클러스터/구성 시각화는 전체가 아닌 무작위 표본(≤CLUSTER_SAMPLE_MAX)으로 KMeans — 132K+ 풀로드/CPU 폭증 방지.
# 표본 기반이라 names(대표 site)/sizes(개수)는 비율적 대표값 (시각화 용도).
CLUSTER_SAMPLE_MAX = 20000


def _cluster_centroids(entity_type: str = "frame", k: int = 12, model_name: str = DEFAULT_MODEL):
    """entity_type 임베딩 KMeans(k) → (names, centroids, sizes). frame=dominant site, caption=대표 텍스트.

    무작위 표본 ≤CLUSTER_SAMPLE_MAX 으로 계산 (대규모 풀로드/CPU 폭증 방지; 비율 대표값).
    """
    from collections import Counter

    import numpy as np
    from sklearn.cluster import KMeans

    if entity_type == "frame":
        sql = (
            "SELECT split_part(im.image_key,'/',1), e.embedding "
            "FROM image_embeddings e JOIN image_metadata im ON im.image_id=e.image_id "
            "WHERE e.entity_type='frame' AND e.model_name=%(model)s "
            "ORDER BY random() LIMIT %(n)s"
        )
    else:
        sql = (
            "SELECT left(text_content, 20), embedding FROM image_embeddings "
            "WHERE entity_type=%(et)s AND model_name=%(model)s "
            "ORDER BY random() LIMIT %(n)s"
        )
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"model": model_name, "et": entity_type, "n": CLUSTER_SAMPLE_MAX})
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

    # 무작위 표본 ≤CLUSTER_SAMPLE_MAX 으로 구성 계산 (132K+ 풀로드/CPU 폭증 방지; 비율 대표값)
    if entity_type == "frame":
        sql = (
            "SELECT split_part(im.image_key,'/',1), e.embedding FROM image_embeddings e "
            "JOIN image_metadata im ON im.image_id=e.image_id "
            "WHERE e.entity_type='frame' AND e.model_name=%(m)s "
            "ORDER BY random() LIMIT %(n)s"
        )
        path = ["site", "cluster"]
    else:
        sql = (
            "SELECT '(caption)', embedding FROM image_embeddings WHERE entity_type=%(et)s "
            "AND model_name=%(m)s ORDER BY random() LIMIT %(n)s"
        )
        path = ["cluster"]
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, {"m": model_name, "et": entity_type, "n": CLUSTER_SAMPLE_MAX})
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


# ── FiftyOne App 텍스트→이미지 검색 (prompt-capable similarity index) ──
#
# FiftyOne App 의 우상단 유사검색 박스에 자연어를 입력하면 frame 이미지가 검색되게 하는 구성.
# 핵심 제약 (fiftyone 1.17.0 / fiftyone.brain similarity.py 소스 확인):
#   - compute_similarity(model=...) 에 **Model 인스턴스**를 넘기면 SimilarityConfig.__init__ 이
#     model=None / supports_prompts=None 으로 폐기 → DB 직렬화 안 됨 → App·재오픈 시 prompt 영구 비활성.
#   - 반드시 **문자열 zoo 이름**으로 넘겨야 config 에 model="<name>" + supports_prompts=True 가 보존되고,
#     쿼리 시점에 App 이 get_model()→foz.load_zoo_model("<name>")→ModelConfig.from_dict→build() 로
#     우리 Model 을 재구성한 뒤 model.embed_prompts([text]) 를 호출한다.
# 그래서: (1) PromptMixin 을 구현한 custom Model 을 importable 한 이 모듈에 두고,
#         (2) 그 클래스를 가리키는 로컬 zoo manifest 를 등록하여 문자열 이름으로 resolve 가능하게 하고,
#         (3) compute_similarity(model="<name>", embeddings_field="embedding", backend="sklearn") 로 인덱스 빌드.
# 이미지측 임베딩은 precomputed('embedding' 필드)라 인덱스 빌드 때 모델이 전혀 호출되지 않는다.
# 텍스트 쿼리만 우리 Model.embed_prompt → _embed_text → embedding-service /embed_text 로 임베딩되어
# 이미지측 PE-Core 와 동일한 1024-d 공간에 매핑된다 (FiftyOne 기본 CLIP 을 쓰면 공간 불일치로 깨짐).

TEXT_SEARCH_MODEL_NAME = "user/pe-core-text-http"

# zoo manifest 영속 경로 (FiftyOne 영속 볼륨). App/새 프로세스가 이 manifest 를 읽어 모델 이름을 resolve.
_TEXT_SEARCH_MANIFEST_PATH = os.environ.get(
    "PIA_TEXT_SEARCH_MANIFEST_PATH", "/data/fiftyone/user_models/manifest.json"
)


# PECoreTextModel / PECoreTextModelConfig 는 _build_text_search_classes() 가 모듈 로드 끝에서
# 실제 클래스로 globals() 에 바인딩한다 (eta Configurable.parse 가 동일 모듈에서 두 이름을 import 하므로
# fiftyone 의존을 import-time 으로 강제하지 않으려고 lazy 정의).


def _build_text_search_classes():
    """fiftyone.core.models 를 lazy import 하여 Config/Model 클래스를 정의해 모듈 전역에 바인딩.

    import-time 에 fiftyone 을 강제로 끌어오지 않도록 함수로 감싸되, 모듈 로드 끝에서 1회 실행한다.
    클래스명은 반드시 ``<ModelClass>`` + ``<ModelClass>Config`` 쌍이어야 한다
    (eta Configurable.parse 가 동일 모듈에서 두 이름을 import 하므로 — 네이밍 강제).
    """
    import numpy as np
    from eta.core.config import Config
    import fiftyone.core.models as fom

    class _PECoreTextModelConfig(Config):
        """PE-Core 텍스트 prompt embedder 설정 (Model 의 연결된 Config 클래스).

        이것은 **inner** config 다 — 외부 ``fiftyone.core.models.ModelConfig`` ({type, config}) 가
        ``Configurable.parse`` 로 이 클래스를 ``<Model>Config`` 이름으로 찾아 ``config`` dict 로 인스턴스화한다.
        따라서 ``eta.core.config.Config`` 를 직접 상속해야 하며 ``type`` 키를 요구하지 않는다.
        가중치 없음(HTTP 호출만)이라 임베딩 차원과 embedding-service URL override 만 받는다.
        """

        def __init__(self, d):
            self.dim = self.parse_int(d, "dim", default=1024)
            self.api_url = self.parse_string(d, "api_url", default=None)

    class _PECoreTextModel(fom.Model, fom.EmbeddingsMixin, fom.PromptMixin):
        """텍스트 prompt 만 임베딩하는 torch-비의존 Model.

        - 이미지측 임베딩은 pgvector/'embedding' 필드에 precomputed → predict/embed 는 호출되지 않음.
        - 텍스트 prompt 는 embedding-service /embed_text (1024-d, PE-Core 공간) 로 임베딩.
        """

        def __init__(self, config):
            self.config = config
            self._preprocess = False

        # --- Model 추상 멤버 ---
        @property
        def media_type(self):
            return "image"

        @property
        def ragged_batches(self):
            return False

        @property
        def transforms(self):
            return None

        @property
        def preprocess(self):
            return self._preprocess

        @preprocess.setter
        def preprocess(self, value):
            # 내부 코드가 model.preprocess = False 를 대입하므로 setter 가 반드시 필요.
            self._preprocess = value

        def predict(self, arg):
            raise NotImplementedError("image embeddings are precomputed externally (pgvector / 'embedding' field)")

        def predict_all(self, args):
            raise NotImplementedError("image embeddings are precomputed externally (pgvector / 'embedding' field)")

        # --- EmbeddingsMixin ---
        @property
        def has_embeddings(self):
            return True

        def get_embeddings(self):
            raise NotImplementedError("image embeddings are precomputed externally")

        # --- PromptMixin (핵심: App 텍스트박스 활성화) ---
        @property
        def can_embed_prompts(self):
            return True

        def embed_prompt(self, prompt):
            api_url = getattr(self.config, "api_url", None)
            if api_url:
                os.environ.setdefault("EMBEDDING_API_URL", api_url)
            vec = _embed_text(prompt)  # list[float] (1024) via embedding-service /embed_text
            arr = np.asarray(vec, dtype=np.float32)
            # 이미지측 임베딩이 L2-normalized 이므로 prompt 도 방어적으로 정규화 (cosine 공간 정합 보장).
            norm = float(np.linalg.norm(arr))
            if norm > 0:
                arr = arr / norm
            return arr

        def embed_prompts(self, prompts):
            return np.stack([self.embed_prompt(p) for p in prompts]).astype(np.float32)

    # eta Configurable.parse 네이밍 규칙(<Class> + <Class>Config)을 만족하도록 정확한 이름으로 모듈 전역 바인딩
    _PECoreTextModel.__name__ = "PECoreTextModel"
    _PECoreTextModel.__qualname__ = "PECoreTextModel"
    _PECoreTextModelConfig.__name__ = "PECoreTextModelConfig"
    _PECoreTextModelConfig.__qualname__ = "PECoreTextModelConfig"
    globals()["PECoreTextModel"] = _PECoreTextModel
    globals()["PECoreTextModelConfig"] = _PECoreTextModelConfig
    return _PECoreTextModel, _PECoreTextModelConfig


# ── 능동학습 큐 (Active Learning Queue) ──
# 라벨이 없는 프레임(normalized_class='none') 중 희귀 이벤트(fire/smoke/fall) 와 가까운 것을 우선 레이블링하도록 랭킹.
# al_score = 0.45*rare_sim + 0.30*representativeness + 0.25*uniqueness (FiftyOne 필드)
# rare_sim = max cosine similarity to fire/smoke centroids (L2-normalized mean embedding)

def _compute_class_centroids(
    embeddings_by_class: dict[str, list[list[float]]],
) -> dict[str, "np.ndarray"]:  # type: ignore[name-defined]
    """각 클래스의 임베딩 리스트 → L2-정규화된 평균 centroid."""
    import numpy as np

    centroids: dict[str, "np.ndarray"] = {}
    for cls, embs in embeddings_by_class.items():
        if not embs:
            continue
        mat = np.array(embs, dtype="float32")
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1.0, norms)
        normalized = mat / norms
        mean_vec = normalized.mean(axis=0)
        n = float(np.linalg.norm(mean_vec))
        centroids[cls] = mean_vec / n if n > 0 else mean_vec
    return centroids


# 후보 풀 배수: rare_sim top-(n*MULT) 를 인덱스로 받아 al_score 재정렬 + 다양성 floor 적용.
# 메모리는 풀 크기(작은 행: image_id/key/rare_sim, 임베딩 미수신)에 묶여 코퍼스 크기와 무관.
AL_CANDIDATE_POOL_MULT = 5


def _rare_topk(
    centroid: Any, k: int, model_name: str,
    *, exclude_ids: list[str] | None = None, ef_search: int = 200,
) -> list[dict]:
    """centroid 벡터에 cosine 가까운 frame top-k (HNSW 인덱스). 임베딩 미반환 → 메모리 O(k).

    exclude_ids(미라벨 필터)가 있으면 hnsw.iterative_scan 으로 필터 통과분 k 개를 채울 때까지
    인덱스 스캔. 반환 행: image_id, image_bucket, image_key, rare_sim(=1-cosine_dist).
    """
    vec = centroid.tolist() if hasattr(centroid, "tolist") else list(centroid)
    lit = "[" + ",".join(repr(float(x)) for x in vec) + "]"
    excl = [str(i) for i in (exclude_ids or [])]
    params: dict = {"q": lit, "model": model_name, "k": int(k), "ef": ef_search}
    where = ["e.entity_type = 'frame'", "e.model_name = %(model)s"]
    if excl:
        # image_id::text 캐스트로 컬럼 타입(uuid/text) 무관하게 비교 — 벡터 인덱스 정렬엔 영향 없음
        where.append("e.image_id::text <> ALL(%(excl)s)")
        params["excl"] = excl
    where_sql = "\n  AND ".join(where)
    base_sql = f"""
        SELECT e.image_id, im.image_bucket, im.image_key,
               1 - (e.embedding <=> %(q)s::vector) AS rare_sim
        FROM image_embeddings e
        JOIN image_metadata im ON im.image_id = e.image_id
        WHERE {where_sql}
        ORDER BY e.embedding <=> %(q)s::vector
        LIMIT %(k)s
    """
    with _pg_conn() as conn, conn.cursor() as cur:
        if excl:
            cur.execute(
                "SET LOCAL hnsw.iterative_scan = 'relaxed_order'; "
                "SET LOCAL hnsw.ef_search = %(ef)s; "
                "SET LOCAL hnsw.max_scan_tuples = 100000;",
                params,
            )
        cur.execute(base_sql, params)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _fo_scores_for_ids(image_ids: list[str]) -> dict[str, dict]:
    """주어진 image_id 한정으로 FiftyOne uniqueness/representativeness 조회 (bounded).

    필드 미존재(=STEP 0 미실행) 또는 데이터셋 없으면 빈 dict → 호출부에서 0 처리.
    전체 스캔하던 것을 후보 풀(작은 집합)로 한정해 대용량에서도 메모리/시간 안전.
    """
    out: dict[str, dict] = {}
    if not image_ids:
        return out
    try:
        import fiftyone as fo
        from fiftyone import ViewField as F

        if not fo.dataset_exists("frames"):
            return out
        ds = fo.load_dataset("frames")
        schema = ds.get_field_schema()
        if "uniqueness" not in schema and "representativeness" not in schema:
            return out  # 두 필드 다 없으면 전부 0 — 굳이 스캔 안 함
        view = ds.match(F("image_id").is_in([str(i) for i in image_ids]))
        for s in view:
            iid = _sample_value(s, "image_id")
            if iid:
                out[str(iid)] = {
                    "uniqueness": _sample_value(s, "uniqueness"),
                    "representativeness": _sample_value(s, "representativeness"),
                }
    except Exception as exc:  # noqa: BLE001 — FiftyOne optional
        print(f"_fo_scores_for_ids: skipped: {exc}")
    return out


def active_learning_queue(n: int = 200, model_name: str = DEFAULT_MODEL) -> dict:
    """hard-positive mining 큐 (pgvector pushdown): 희귀 클래스 lookalike 우선 + 다양성 보장.

    fire/smoke centroid 를 '라벨된 소수 프레임'으로만 계산한 뒤, 미라벨 프레임 중 centroid 에
    cosine 가까운 top-pool 을 HNSW 인덱스로 받아 al_score 재정렬해 상위 n개 반환.
    임베딩 전량을 메모리에 올리지 않아 코퍼스가 커져도 메모리 O(pool) — OOM/2만 캡 없음(전체 커버).

    al_score = 0.45*rare_sim + 0.30*representativeness + 0.25*uniqueness
      (uniqueness/representativeness 는 FiftyOne 필드; 미계산이면 0 → 사실상 rare_sim 단독.
       필드가 있으면 최종 후보 풀 한정으로 조회해 반영.)
    top-n의 최소 20%는 non-near-rare(representative/edge)로 채워 다양성 보장.
    Returns dict(rows=[...], n_candidates=int, n=int, reason_counts=dict, truncated=False).
    rows 각 항목: image_id, al_score, rare_sim, nearest_rare_class,
                  representativeness, uniqueness, reason, minio_key
    """
    try:
        # 1. FiftyOne 라벨 맵 (작은 dict: label/caption 문자열만 → 메모리 안전, 임베딩 아님)
        fo_metadata = _load_fo_metadata_for_dq("normalized_class")
        labeled_ids_by_class: dict[str, list[str]] = {"fire": [], "smoke": []}
        labeled_non_none: list[str] = []
        for iid, meta in fo_metadata.items():
            lbl = str(meta.get("label") or "none")
            if lbl != "none":
                labeled_non_none.append(iid)
            if lbl in labeled_ids_by_class:
                labeled_ids_by_class[lbl].append(iid)

        # 2. 라벨된 fire/smoke 임베딩만 로드(소수) → centroid
        seed_ids = labeled_ids_by_class["fire"] + labeled_ids_by_class["smoke"]
        if not seed_ids:
            return {"rows": [], "n_candidates": 0, "n": n, "reason_counts": {}, "truncated": False}
        seed_rows = _load_frames_for_dq(model_name, image_ids=seed_ids)
        emb_by_id = {r["image_id"]: r["embedding"] for r in seed_rows}
        embs_by_class = {
            c: [emb_by_id[i] for i in labeled_ids_by_class[c] if i in emb_by_id]
            for c in ("fire", "smoke")
        }
        centroids = _compute_class_centroids(embs_by_class)
        rare_classes = [c for c in ("fire", "smoke") if c in centroids]
        if not rare_classes:
            return {"rows": [], "n_candidates": 0, "n": n, "reason_counts": {}, "truncated": False}

        total_frames = _count_frames_for_dq(model_name)
        n_candidates = max(0, total_frames - len(labeled_non_none))

        # 3. centroid 별 인덱스 top-pool (미라벨만; 임베딩 미수신). image_id 기준 max rare_sim 병합.
        pool_k = max(n * AL_CANDIDATE_POOL_MULT, n + 100)
        best: dict[str, dict] = {}
        for cls in rare_classes:
            for h in _rare_topk(centroids[cls], pool_k, model_name, exclude_ids=labeled_non_none):
                iid = str(h["image_id"])
                rs = float(h["rare_sim"])
                prev = best.get(iid)
                if prev is None or rs > prev["rare_sim"]:
                    bucket, key = h.get("image_bucket"), h.get("image_key")
                    best[iid] = {
                        "rare_sim": rs,
                        "nearest_rare_class": cls,
                        "minio_key": f"{bucket}/{key}" if bucket and key else "",
                    }
        if not best:
            return {"rows": [], "n_candidates": n_candidates, "n": n, "reason_counts": {}, "truncated": False}

        # 4. uniqueness/representativeness 는 후보 풀 한정 조회 (없으면 0)
        fo_scores = _fo_scores_for_ids(list(best.keys()))

        RARE_SIM_THRESHOLD = 0.7  # PE-Core cosine baseline ~0.3-0.8; 0.5는 너무 낮아 거의 전부 near-rare 분류됨
        scored_rows = []
        for iid, b in best.items():
            sc = fo_scores.get(iid, {})
            uniqueness = float(sc.get("uniqueness") or 0.0)
            representativeness = float(sc.get("representativeness") or 0.0)
            rare_sim = max(0.0, min(1.0, b["rare_sim"]))
            al_score = 0.45 * rare_sim + 0.30 * representativeness + 0.25 * uniqueness
            if rare_sim > RARE_SIM_THRESHOLD:
                reason = f"near-{b['nearest_rare_class']}"
            elif uniqueness >= representativeness:
                reason = "edge/unique"
            else:
                reason = "representative"
            scored_rows.append({
                "image_id": iid,
                "al_score": round(al_score, 4),
                "rare_sim": round(rare_sim, 4),
                "nearest_rare_class": b["nearest_rare_class"],
                "representativeness": round(representativeness, 4),
                "uniqueness": round(uniqueness, 4),
                "reason": reason,
                "minio_key": b["minio_key"],
            })

        # 5. 다양성 floor: top-n 의 최소 20%를 non-near-rare 로 (기존 로직 동일)
        sorted_rows = sorted(scored_rows, key=lambda x: x["al_score"], reverse=True)
        near_rare_rows = [r for r in sorted_rows if str(r["reason"]).startswith("near-")]
        non_near_rare_rows = [r for r in sorted_rows if not str(r["reason"]).startswith("near-")]
        non_near_floor = min(n // 5, len(non_near_rare_rows))
        near_limit = min((4 * n) // 5, len(near_rare_rows))
        selected_rows = near_rare_rows[:near_limit] + non_near_rare_rows[:non_near_floor]
        selected_ids = {str(row["image_id"]) for row in selected_rows}
        target_n = min(n, len(sorted_rows))
        for row in sorted_rows:
            if len(selected_rows) >= target_n:
                break
            iid = str(row["image_id"])
            if iid not in selected_ids:
                selected_rows.append(row)
                selected_ids.add(iid)
        top_rows = sorted(selected_rows, key=lambda x: x["al_score"], reverse=True)[:n]
        reason_counts = dict(Counter(str(row["reason"]) for row in top_rows))
        return {
            "rows": top_rows,
            "n_candidates": n_candidates,
            "n": n,
            "reason_counts": reason_counts,
            "truncated": False,
        }

    except Exception as exc:  # noqa: BLE001
        print(f"active_learning_queue: skipped: {exc}")
        return {"rows": [], "n_candidates": 0, "n": n, "reason_counts": {}, "truncated": False}


# ── 캡션 키워드 앵커링 분석 (Caption Keyword Anchoring) ──
# 라벨된 프레임의 자유형 캡션이 클래스 canonical keyword를 직접 포함하는지 검사.
# 낮은 앵커링률은 캡션 품질 점수가 아니며, 맥락적 한국어 캡션에서는 정상.

def caption_quality_report(model_name: str = DEFAULT_MODEL) -> dict:
    """라벨된 프레임(fall/fire/smoke) 캡션의 키워드 앵커링률. (품질 점수 아님)

    Gemini 자유형 한국어 캡션은 canonical keyword 없이도 정확할 수 있다.
    Returns dict(by_class={class:{n,caption_has_matching_kw,caption_missing_kw,keyword_anchor_rate}},
                 overall_keyword_anchor_rate=float, examples_missing=[{image_id,normalized_class,caption}...20])
    """
    try:
        fo_metadata = _load_fo_metadata_for_dq("normalized_class")
        if not fo_metadata:
            return {"by_class": {}, "overall_keyword_anchor_rate": 0.0, "examples_missing": []}
        labeled_classes = frozenset({"fall", "fire", "smoke"})
        by_class: dict[str, dict] = {}
        examples_missing: list[dict] = []
        total_labeled = 0
        total_has_kw = 0
        for iid, meta in fo_metadata.items():
            lbl = str(meta.get("label") or "none")
            if lbl not in labeled_classes:
                continue
            caption = str(meta.get("caption") or "")
            total_labeled += 1
            has_kw = lbl in _caption_keyword_classes(caption)
            if lbl not in by_class:
                by_class[lbl] = {"n": 0, "caption_has_matching_kw": 0, "caption_missing_kw": 0}
            by_class[lbl]["n"] += 1
            if has_kw:
                by_class[lbl]["caption_has_matching_kw"] += 1
                total_has_kw += 1
            else:
                by_class[lbl]["caption_missing_kw"] += 1
                if len(examples_missing) < 20:
                    examples_missing.append(
                        {"image_id": iid, "normalized_class": lbl, "caption": caption[:200]}
                    )
        for stats in by_class.values():
            nn = stats["n"]
            stats["keyword_anchor_rate"] = round(stats["caption_has_matching_kw"] / nn, 4) if nn > 0 else 0.0
        overall = round(total_has_kw / total_labeled, 4) if total_labeled > 0 else 0.0
        return {"by_class": by_class, "overall_keyword_anchor_rate": overall, "examples_missing": examples_missing}
    except Exception as exc:  # noqa: BLE001
        print(f"caption_quality_report: skipped: {exc}")
        return {"by_class": {}, "overall_keyword_anchor_rate": 0.0, "examples_missing": []}


def _text_search_manifest_dict() -> dict:
    return {
        "models": [
            {
                "base_name": TEXT_SEARCH_MODEL_NAME,
                "description": "PE-Core text prompt embedder (matches precomputed PE-Core image embeddings)",
                "tags": ["embeddings", "prompts", "custom", "user"],
                # manager/base_filename 없음 → load_zoo_model 가 다운로드/HasPublishedModel 을 스킵.
                "default_deployment_config_dict": {
                    "type": "fiftyone_pgvector.PECoreTextModel",
                    "config": {"dim": 1024},
                },
            }
        ]
    }


def register_text_search_model(manifest_path: str | None = None) -> str:
    """로컬 zoo manifest 를 (영속 경로에) 쓰고 fo.config.model_zoo_manifest_paths 에 등록.

    멱등. 이 프로세스에서 load_zoo_model(TEXT_SEARCH_MODEL_NAME) 가 resolve 되도록 만든다.
    영속 파일 + (가능하면) FIFTYONE_MODEL_ZOO_MANIFEST_PATHS env 로 새 프로세스에서도 재구성되게 한다.
    """
    import fiftyone as fo

    path = manifest_path or _TEXT_SEARCH_MANIFEST_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(_text_search_manifest_dict(), f)

    paths = list(fo.config.model_zoo_manifest_paths or [])
    if path not in paths:
        paths.append(path)
        fo.config.model_zoo_manifest_paths = paths
    return path


def build_text_search_index(
    ds,
    brain_key: str = "text_search",
    embeddings_field: str = "embedding",
    *,
    overwrite: bool = True,
):
    """``ds`` 에 prompt-capable similarity index 를 생성 (App 텍스트→이미지 검색용).

    - 이미지측: precomputed ``embeddings_field`` ('embedding') 사용 → 인덱스 빌드 시 모델 미호출.
    - 텍스트측: PECoreTextModel.embed_prompt → embedding-service /embed_text (동일 1024-d 공간).
    - model 은 **문자열** zoo 이름으로 넘겨 config 에 보존 → App/재오픈 시 재구성 가능.

    반환: fiftyone.brain SimilarityIndex (results.config.supports_prompts == True 여야 정상).
    """
    import fiftyone.brain as fob
    import fiftyone.zoo as foz

    register_text_search_model()

    # 모델이 prompt-capable 로 resolve 되는지 사전 확인 (이름 resolve + can_embed_prompts).
    _m = foz.load_zoo_model(TEXT_SEARCH_MODEL_NAME)
    if not getattr(_m, "can_embed_prompts", False):
        raise RuntimeError("text-search model resolved but can_embed_prompts is False")

    if overwrite and ds.has_brain_run(brain_key):
        ds.delete_brain_run(brain_key)

    # supports_prompts 는 넘기지 않는다 — compute_similarity 가 model.can_embed_prompts 에서 자동 산출.
    return fob.compute_similarity(
        ds,
        model=TEXT_SEARCH_MODEL_NAME,  # ★ 문자열 → config 직렬화 + supports_prompts 자동 True
        embeddings=embeddings_field,  # ★ precomputed 1024-d 필드 (모델 미호출)
        backend="sklearn",
        metric="cosine",
        brain_key=brain_key,
    )


# ── HDBSCAN 클러스터링 ──

def hdbscan_report(
    entity_type: str = "frame",
    min_cluster_size: int = 15,
    model_name: str = DEFAULT_MODEL,
) -> dict:
    """pgvector 임베딩 기반 HDBSCAN 클러스터링 리포트 (READ-ONLY, FiftyOne 데이터셋 불필요).

    entity_type='frame': _load_frames_for_dq() 재사용.
    entity_type='caption': image_embeddings WHERE entity_type='caption' 직접 조회.
    DQ_FULL_MATRIX_MAX_N 초과 시 {} 반환.
    Returns {"n_clusters": int, "noise": int, "noise_frac": float, "n": int,
             "sizes": {label: count} 내림차순, "min_cluster_size": int}.
    """
    try:
        import numpy as np
        from sklearn.cluster import HDBSCAN

        if entity_type == "frame":
            rows = _load_frames_for_dq(model_name, limit=DQ_FULL_MATRIX_MAX_N)
            n_check = len(rows)
            embeddings_list = [r["embedding"] for r in rows]
        else:
            # caption: image_embeddings WHERE entity_type='caption' 직접 조회
            rows_raw: list[list[float]] = []
            try:
                with _pg_conn() as conn, conn.cursor() as cur:
                    cur.execute(
                        "SELECT embedding FROM image_embeddings "
                        "WHERE entity_type='caption' AND model_name=%(model)s "
                        "ORDER BY entity_id LIMIT %(lim)s",
                        {"model": model_name, "lim": DQ_FULL_MATRIX_MAX_N + 1},
                    )
                    rows_raw = [_parse_vector(r[0]) for r in cur.fetchall()]
            except Exception as exc:  # noqa: BLE001
                print(f"hdbscan_report: caption embedding 로드 실패: {exc}")
                return {}
            n_check = len(rows_raw)
            embeddings_list = rows_raw

        if n_check == 0:
            return {}
        if n_check > DQ_FULL_MATRIX_MAX_N:
            print(
                f"hdbscan_report: N={n_check} > DQ_FULL_MATRIX_MAX_N={DQ_FULL_MATRIX_MAX_N}, 스킵."
            )
            return {}

        mat = np.array(embeddings_list, dtype="float32")
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1.0, norms)
        embs = mat / norms

        labels_arr = HDBSCAN(min_cluster_size=min_cluster_size, copy=True).fit_predict(embs)

        sizes: dict[str, int] = {}
        for lbl in labels_arr:
            key = "noise" if int(lbl) == -1 else f"h{int(lbl):02d}"
            sizes[key] = sizes.get(key, 0) + 1

        n_total = len(labels_arr)
        noise_count = sizes.get("noise", 0)
        n_clusters = len([k for k in sizes if k != "noise"])
        noise_frac = round(noise_count / n_total, 4) if n_total > 0 else 0.0
        sizes_sorted = dict(sorted(sizes.items(), key=lambda x: -x[1]))
        return {
            "n_clusters": n_clusters,
            "noise": noise_count,
            "noise_frac": noise_frac,
            "n": n_total,
            "sizes": sizes_sorted,
            "min_cluster_size": min_cluster_size,
        }
    except Exception as exc:  # noqa: BLE001 — analysis surface fail-forward
        print(f"hdbscan_report: 실패: {exc}")
        return {}


def hdbscan_figure(
    entity_type: str = "frame",
    min_cluster_size: int = 15,
    model_name: str = DEFAULT_MODEL,
    report: dict | None = None,
):
    """hdbscan_report 결과를 plotly 바차트로 반환 (노이즈=회색, 클러스터=색상 팔레트).

    report 를 넘기면 재계산 없이 재사용(HDBSCAN 이중 계산 방지). 데이터 없음 → annotation 빈 Figure.
    """
    import plotly.graph_objects as go

    if report is None:
        report = hdbscan_report(entity_type=entity_type, min_cluster_size=min_cluster_size,
                                model_name=model_name)
    if not report:
        fig = go.Figure()
        fig.add_annotation(
            text="데이터 없음",
            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False, font={"size": 18},
        )
        return fig

    sizes = report.get("sizes", {})
    # noise 를 마지막으로, 나머지는 크기 내림차순 유지
    cluster_keys = [k for k in sizes if k != "noise"]
    if "noise" in sizes:
        cluster_keys.append("noise")

    import plotly.express as px

    palette = px.colors.qualitative.Plotly
    colors = []
    for k in cluster_keys:
        if k == "noise":
            colors.append("lightgray")
        else:
            # cluster 인덱스 번호를 팔레트 색으로
            try:
                idx = int(k[1:])  # 'h00' → 0
            except (ValueError, IndexError):
                idx = 0
            colors.append(palette[idx % len(palette)])

    fig = go.Figure(
        go.Bar(
            x=cluster_keys,
            y=[sizes[k] for k in cluster_keys],
            marker_color=colors,
        )
    )
    fig.update_layout(
        title=(
            f"HDBSCAN 클러스터 분포 ({entity_type}, "
            f"n_clusters={report['n_clusters']}, noise_frac={report['noise_frac']:.1%}, "
            f"min_cluster_size={min_cluster_size})"
        ),
        xaxis_title="cluster",
        yaxis_title="count",
        width=860,
        height=460,
    )
    return fig


# 모듈 로드 시 Model/Config 클래스를 정의하고(앱 프로세스 import 시 FQ 클래스 resolve 보장),
# 로컬 zoo manifest 를 등록한다. fiftyone 미설치 등 환경에서는 조용히 skip (fail-forward).
try:
    _build_text_search_classes()
    register_text_search_model()
except Exception as _exc:  # noqa: BLE001 — analysis surface should import even without fiftyone present
    print(f"fiftyone_pgvector: text-search model registration skipped: {_exc}")