"""Sync Label Studio annotations back to MinIO labels JSON + DuckDB labels.

Source of truth = LS 사람 submit. annotation 결과를 그대로 vlm-labels 버킷과
DuckDB labels 테이블에 반영한다. (upstream Gemini 결과의 유무·내용은 무시)

흐름 (video / TimelineLabels):
  LS task → annotation result(timelinelabels) 추출
    → MinIO vlm-labels/<folder>/<sub>/events/<stem>.json 결정
        · 기존 events JSON 있으면 그 키 재사용
        · 없으면 raw_key 에서 events JSON 키 계산 (신규 생성)
    → MinIO put (이벤트 M개 또는 [] 모두 동일 경로로 저장)
    → DuckDB: DELETE WHERE labels_key=? 후 M개 INSERT (M=0이면 DELETE만)

흐름 (image / RectangleLabels): cdd83c9 의 image 분기 정책 유지 — 빈 검수면
    annotations=[] 로 기존 sam3 JSON 비우고, 기존 JSON 부재 시엔 생성 안 함.

주의:
  - `/sync-approve` 로 finalized 된 후의 재submit 은 본 sync 흐름에서 다루지
    않는다 (processed_clips.source_label_id dangling 위험). 재검수 정책이
    필요하면 별도 설계 필요.

Usage:
    python ls_sync.py --project 3 --api-key "..."
    python ls_sync.py --project 3 --api-key "..." --prefix hyundai_v2/01_27
    python ls_sync.py --project 3 --api-key "..." --dry-run
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures as cf
import json
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse, parse_qs

# 클립 파일명 패턴: {base}_{8자리ms}_{8자리ms}
_CLIP_PATTERN = re.compile(r"^(.+)_(\d{8})_(\d{8})$")

import boto3
import requests
from botocore.config import Config as BotoConfig

DEFAULT_LS_URL = "http://localhost:8080"
DEFAULT_MINIO_ENDPOINT = "172.168.47.36:9000"
DEFAULT_MINIO_ACCESS_KEY = "minioadmin"
DEFAULT_MINIO_SECRET_KEY = "minioadmin"
DEFAULT_BUCKET = "vlm-labels"
DEFAULT_FPS = 24


# ---------------------------------------------------------------------------
# MinIO
# ---------------------------------------------------------------------------

def build_minio_client(endpoint: str, access_key: str, secret_key: str):
    url = endpoint if endpoint.startswith("http") else f"http://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def build_json_key_index(client, bucket: str, prefix: str = "") -> dict[str, str]:
    """stem → MinIO key 인덱스. (*/events/*.json만 대상)"""
    paginator = client.get_paginator("list_objects_v2")
    index: dict[str, str] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            if key.endswith(".json") and len(parts) >= 2 and parts[-2] == "events":
                index[Path(key).stem] = key
    return index


def read_json(client, bucket: str, key: str) -> list | dict:
    resp = client.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))


def write_json(client, bucket: str, key: str, data: list | dict) -> None:
    body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def _extract_raw_key_from_video_url(video_url: str, raw_bucket: str = "vlm-raw") -> str | None:
    """LS task data.video presigned URL → raw_files.raw_key 형태.

    예: 'http://172.168.47.36:9000/vlm-raw/smart-city/normal_case/cam6.mp4?X-Amz-...'
        → 'smart-city/normal_case/cam6.mp4'

    fileuri 쿼리 파라미터(legacy)가 있으면 그걸 우선 사용하고,
    없으면 URL path 에서 첫 segment(bucket) 를 떼어 raw_key 로 본다.
    """
    fileuri = _decode_fileuri(video_url)
    if fileuri:
        if fileuri.startswith(f"{raw_bucket}/"):
            return fileuri[len(raw_bucket) + 1:]
        return fileuri or None
    try:
        path = (urlparse(video_url).path or "").lstrip("/")
        if "/" not in path:
            return None
        bucket, key = path.split("/", 1)
        if bucket != raw_bucket:
            # 다른 버킷이면 안전상 None — caller 에서 ERROR 처리
            return None
        return key or None
    except Exception:
        return None


def _resolve_labels_key_for_video(raw_key: str, stem: str) -> str:
    """raw_key 의 events/<stem>.json 형태로 변환.

    예: 'smart-city/normal_case/camera6_vending.mp4'
        → 'smart-city/normal_case/events/camera6_vending.json'
    """
    p = PurePosixPath(raw_key)
    return str(p.parent / "events" / f"{stem}.json")


# ---------------------------------------------------------------------------
# Label Studio
# ---------------------------------------------------------------------------

def resolve_auth_headers(ls_url: str, token: str) -> dict[str, str]:
    try:
        payload_part = token.split(".")[1]
        payload_part += "=" * (-len(payload_part) % 4)
        payload = json.loads(base64.b64decode(payload_part).decode("utf-8"))
        if payload.get("token_type") == "refresh":
            resp = requests.post(f"{ls_url}/api/token/refresh/", json={"refresh": token})
            resp.raise_for_status()
            return {"Authorization": f"Bearer {resp.json()['access']}"}
    except Exception:
        pass
    return {"Authorization": f"Token {token}"}


class _LSAuth:
    """JWT access token auto-refresh wrapper.

    ls_sync가 수백 개 task를 순차 조회하는 동안 access token(기본 5분 만료)이 끊어지면
    401을 만나 실패했다. 호출 한 번에 만료 감지 → refresh 재시도 한 번으로 끊기지 않게 한다.
    병렬 fetch 시 여러 스레드가 동시에 refresh하지 않도록 Lock으로 보호.
    """

    def __init__(self, ls_url: str, token: str) -> None:
        self.ls_url = ls_url
        self.token = token
        self._lock = threading.Lock()
        self.headers = resolve_auth_headers(ls_url, token)

    def refresh(self) -> None:
        with self._lock:
            self.headers = resolve_auth_headers(self.ls_url, self.token)


def _ls_get(auth: _LSAuth, url: str, params: dict | None = None):
    resp = requests.get(url, headers=auth.headers, params=params)
    if resp.status_code == 401:
        auth.refresh()
        resp = requests.get(url, headers=auth.headers, params=params)
    resp.raise_for_status()
    return resp


def _decode_fileuri(video_url: str) -> str | None:
    try:
        qs = parse_qs(urlparse(video_url).query)
        encoded = qs.get("fileuri", [None])[0]
        if encoded:
            return base64.b64decode(encoded).decode("utf-8")
    except Exception:
        pass
    return None


def fetch_annotated_tasks(ls_url: str, auth: _LSAuth, project_id: int) -> list[dict]:
    """annotation이 1개 이상 있는 task만 반환."""
    annotated = []
    page = 1
    while True:
        resp = _ls_get(
            auth,
            f"{ls_url}/api/tasks/",
            params={"project": project_id, "page": page, "page_size": 500},
        )
        data = resp.json()
        tasks = data if isinstance(data, list) else data.get("tasks", [])
        if not tasks:
            break
        for task in tasks:
            if task.get("total_annotations", 0) > 0:
                annotated.append(task)
        if isinstance(data, list) or not data.get("next"):
            break
        page += 1
    return annotated


def fetch_task_detail(ls_url: str, auth: _LSAuth, task_id: int) -> dict:
    return _ls_get(auth, f"{ls_url}/api/tasks/{task_id}/").json()


# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------

def _connect_duckdb_with_retry(
    db_path: str,
    *,
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    read_only: bool = False,
):
    """DuckDB 연결 시 lock 에러 발생하면 exponential backoff로 재시도."""
    import duckdb
    import time as _time
    import random as _random

    for attempt in range(max_retries + 1):
        try:
            return duckdb.connect(db_path, read_only=read_only)
        except duckdb.IOException as exc:
            if "lock" not in str(exc).lower() or attempt >= max_retries:
                raise
            delay = min(base_delay * (2 ** attempt) + _random.uniform(0, 1), max_delay)
            print(f"[RETRY] DuckDB lock 감지, {delay:.1f}s 후 재시도 ({attempt + 1}/{max_retries})")
            _time.sleep(delay)
    raise RuntimeError("DuckDB 연결 재시도 초과")


def lookup_asset_id_by_raw_key(db_path: str, raw_key: str, conn=None) -> str | None:
    """raw_files.raw_key 단독 조회로 asset_id 반환. 없으면 None.

    note:
      - source_unit_name 은 dispatch 시 입력한 원본 표기(대소문자/특수문자
        보존)이고 raw_key 는 sanitize 된 MinIO key 라 둘이 다를 수 있다.
        조회 키로는 raw_key 단독을 쓴다 (raw_key 는 사실상 unique).
      - conn 이 주어지면 재사용. 없으면 read-only 로 새 연결.
    """
    if not raw_key:
        return None
    if conn is None:
        if not db_path:
            return None
        c = _connect_duckdb_with_retry(db_path, read_only=True)
        try:
            row = c.execute(
                "SELECT asset_id FROM raw_files WHERE raw_key = ? LIMIT 1",
                [raw_key],
            ).fetchone()
        finally:
            c.close()
    else:
        row = conn.execute(
            "SELECT asset_id FROM raw_files WHERE raw_key = ? LIMIT 1",
            [raw_key],
        ).fetchone()
    return row[0] if row else None


def upsert_video_labels(
    db_path: str,
    labels_bucket: str,
    labels_key: str,
    asset_id: str,
    new_events: list[dict],
    conn=None,
) -> tuple[int, int]:
    """labels_key 기준으로 기존 row 전부 DELETE → 사람 submit 결과로 INSERT.

    SOT = 사람 submit. 3-way merge / removed_by_reviewer 상태 없음.
    new_events=[] 이면 DELETE 만 (INSERT 없음).
    DELETE + INSERT 를 한 트랜잭션으로 묶어 원자성 보장.

    반환값: (deleted_count, inserted_count)
    """
    import hashlib
    owns_conn = conn is None
    if owns_conn:
        conn = _connect_duckdb_with_retry(db_path)
    try:
        conn.execute("BEGIN")
        deleted_row = conn.execute(
            "SELECT COUNT(*) FROM labels WHERE labels_key = ?",
            [labels_key],
        ).fetchone()
        deleted = int(deleted_row[0]) if deleted_row else 0

        conn.execute("DELETE FROM labels WHERE labels_key = ?", [labels_key])

        inserted = 0
        event_count = len(new_events)
        for i, ev in enumerate(new_events):
            start_sec = ev["timestamp"][0]
            end_sec = ev["timestamp"][1]
            token = f"{asset_id}|manual_review|{i}|{start_sec}|{end_sec}"
            label_id = hashlib.sha1(token.encode()).hexdigest()
            conn.execute(
                """
                INSERT INTO labels (
                    label_id, asset_id, labels_bucket, labels_key,
                    label_format, label_tool, label_source,
                    review_status, event_index, event_count,
                    timestamp_start_sec, timestamp_end_sec,
                    label_status, created_at
                ) VALUES (?, ?, ?, ?, 'json', 'label_studio', 'manual_review',
                          'reviewed', ?, ?, ?, ?, 'completed', NOW())
                """,
                [
                    label_id, asset_id, labels_bucket, labels_key,
                    i, event_count, start_sec, end_sec,
                ],
            )
            inserted += 1

        conn.execute("COMMIT")
        return deleted, inserted
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        if owns_conn:
            conn.close()


# ---------------------------------------------------------------------------
# 변환 로직
# ---------------------------------------------------------------------------

def annotation_to_events(annotation: dict, fps: int) -> list[dict]:
    """LS annotation result → Gemini 이벤트 포맷 변환."""
    events = []
    for item in annotation.get("result", []):
        if item.get("type") != "timelinelabels":
            continue
        value = item.get("value", {})
        for r in value.get("ranges", []):
            start_sec = r["start"] / fps
            end_sec = r["end"] / fps
            labels = value.get("timelinelabels", [])
            category = labels[0] if labels else "unknown"
            events.append({
                "category": category,
                "duration": round(end_sec - start_sec, 3),
                "timestamp": [round(start_sec, 3), round(end_sec, 3)],
            })
    return events


def find_matching_event_index(json_data: list, clip_start_sec: float, clip_end_sec: float) -> int | None:
    """clip 구간과 겹치는 이벤트 인덱스 반환. 없으면 None."""
    for i, ev in enumerate(json_data):
        ts = ev.get("timestamp")
        if not ts or len(ts) < 2:
            continue
        ev_start, ev_end = float(ts[0]), float(ts[1])
        if ev_end > clip_start_sec and ev_start < clip_end_sec:
            return i
    return None


# ---------------------------------------------------------------------------
# Image mode helpers (RectangleLabels → SAM3 COCO)
# ---------------------------------------------------------------------------

def _parse_image_url_to_key(image_url: str) -> str | None:
    """presigned URL → MinIO object key.

    형태: http://<endpoint>/<bucket>/<key>?X-Amz-...
    bucket 구분 없이 첫 path segment 제거하고 나머지를 key로 사용.
    """
    try:
        parsed = urlparse(image_url)
        path = (parsed.path or "").lstrip("/")
        if "/" not in path:
            return None
        # 첫 segment = bucket, 나머지 = key
        return path.split("/", 1)[1] or None
    except Exception:
        return None


def _sam3_key_from_image_key(image_key: str) -> str:
    """build_sam3_detection_key()와 동일 규약 — cross-package import 회피 위해 인라인."""
    p = PurePosixPath(str(image_key or ""))
    stem = p.stem or "image"
    parent = p.parent
    raw_parent = parent.parent if parent.name == "image" else parent
    if str(raw_parent) and str(raw_parent) != ".":
        return f"{raw_parent}/sam3_segmentations/{stem}.json"
    return f"sam3_segmentations/{stem}.json"


def annotation_to_rectangles(annotation: dict) -> list[dict]:
    """LS RectangleLabels annotation → 절대 픽셀 bbox 리스트.

    반환 항목:
      {"label": str, "bbox": [x, y, w, h], "score": float | None,
       "rotation": float, "image_width": int, "image_height": int}
    """
    rects: list[dict] = []
    for item in annotation.get("result", []):
        if item.get("type") != "rectanglelabels":
            continue
        v = item.get("value", {}) or {}
        W = int(item.get("original_width") or 0)
        H = int(item.get("original_height") or 0)
        if W <= 0 or H <= 0:
            continue
        x = float(v.get("x", 0)) * W / 100.0
        y = float(v.get("y", 0)) * H / 100.0
        w = float(v.get("width", 0)) * W / 100.0
        h = float(v.get("height", 0)) * H / 100.0
        labels = v.get("rectanglelabels") or []
        label_name = str(labels[0]).strip() if labels else "unknown"
        rects.append({
            "label": label_name,
            "bbox": [round(x, 2), round(y, 2), round(w, 2), round(h, 2)],
            "score": item.get("score"),
            "rotation": float(v.get("rotation", 0) or 0),
            "image_width": W,
            "image_height": H,
        })
    return rects


def _merge_categories(existing: list[dict], new_names: list[str]) -> tuple[list[dict], dict[str, int]]:
    """기존 categories 보존 + 신규 label 확장. (updated_list, name_to_id) 반환."""
    name_to_id = {str(c.get("name", "")).strip(): int(c["id"]) for c in existing if "id" in c and "name" in c}
    max_id = max(name_to_id.values(), default=0)
    merged = list(existing)
    for name in new_names:
        name_s = name.strip()
        if not name_s or name_s in name_to_id:
            continue
        max_id += 1
        merged.append({"id": max_id, "name": name_s, "supercategory": "object"})
        name_to_id[name_s] = max_id
    return merged, name_to_id


def build_reviewed_coco_json(
    existing: dict,
    rectangles: list[dict],
    image_key: str,
) -> dict:
    """기존 SAM3 COCO JSON에 검수 결과를 반영해 새 JSON 생성."""
    out = dict(existing) if isinstance(existing, dict) else {}

    # images 보존 (또는 rectangles의 W/H로 보강)
    images = out.get("images") or []
    if not images and rectangles:
        r0 = rectangles[0]
        images = [{
            "id": 1,
            "file_name": image_key,
            "width": r0.get("image_width"),
            "height": r0.get("image_height"),
        }]
    image_id = int(images[0]["id"]) if images else 1

    categories_in = out.get("categories") or []
    label_names = [r["label"] for r in rectangles]
    categories_out, name_to_id = _merge_categories(categories_in, label_names)

    new_annotations: list[dict] = []
    for i, r in enumerate(rectangles, start=1):
        bbox = r["bbox"]
        ann = {
            "id": i,
            "image_id": image_id,
            "category_id": name_to_id[r["label"]],
            "bbox": bbox,
            "area": round(float(bbox[2]) * float(bbox[3]), 2),
            "iscrowd": 0,
            "segmentation": [],
        }
        if r.get("score") is not None:
            ann["score"] = float(r["score"])
        if r.get("rotation"):
            ann["rotation"] = r["rotation"]
        new_annotations.append(ann)

    meta = dict(out.get("meta") or {})
    meta["review_source"] = "manual_review"
    meta["reviewed_at"] = datetime.now(timezone.utc).isoformat()
    meta["reviewed_object_count"] = len(new_annotations)

    out["info"] = out.get("info") or {}
    out["licenses"] = out.get("licenses") or []
    out["images"] = images
    out["annotations"] = new_annotations
    out["categories"] = categories_out
    out["meta"] = meta
    return out


def update_image_labels_in_db(db_path: str, labels_key: str, object_count: int, conn=None) -> int:
    """image_labels 테이블의 review_status/label_source/object_count 전이.
    conn이 주어지면 재사용 (커넥션 open/close 오버헤드 감소).
    """
    if not labels_key or (not db_path and conn is None):
        return 0
    owns_conn = conn is None
    if owns_conn:
        conn = _connect_duckdb_with_retry(db_path)
    try:
        conn.execute(
            """
            UPDATE image_labels
            SET review_status = 'reviewed',
                label_source  = 'manual_review',
                object_count  = ?
            WHERE labels_key = ?
            """,
            [int(object_count), labels_key],
        )
        row = conn.execute(
            "SELECT COUNT(*) FROM image_labels WHERE labels_key = ? AND review_status = 'reviewed'",
            [labels_key],
        ).fetchone()
        return int(row[0]) if row else 0
    finally:
        if owns_conn:
            conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(
    project_id: int,
    ls_url: str,
    api_key: str,
    fps: int,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    bucket: str,
    prefix: str,
    dry_run: bool,
    db_path: str | None = None,
) -> None:
    print(f"[INFO] MinIO: {minio_endpoint}, bucket={bucket}")
    minio = build_minio_client(minio_endpoint, minio_access_key, minio_secret_key)

    print("[INFO] MinIO JSON 인덱스 구성 중...")
    json_index = build_json_key_index(minio, bucket, prefix)
    print(f"[INFO] events/*.json {len(json_index)}개 인덱싱 완료")

    print(f"[INFO] Label Studio annotation 조회 중... (project={project_id})")
    auth = _LSAuth(ls_url, api_key)
    annotated_tasks = fetch_annotated_tasks(ls_url, auth, project_id)
    print(f"[INFO] annotation 있는 task {len(annotated_tasks)}개\n")

    # task detail 병렬 pre-fetch (LS API rate limit 고려, 기본 8 동시). 0이면 순차 fallback.
    fetch_workers = int(os.getenv("LS_SYNC_FETCH_WORKERS", "8"))
    detail_cache: dict[int, dict | None] = {}
    if fetch_workers > 1 and len(annotated_tasks) > 1:
        print(f"[INFO] task detail 병렬 fetch (workers={fetch_workers})")
        with cf.ThreadPoolExecutor(max_workers=fetch_workers) as ex:
            futs = {ex.submit(fetch_task_detail, ls_url, auth, t["id"]): t["id"] for t in annotated_tasks}
            for fut in cf.as_completed(futs):
                tid = futs[fut]
                try:
                    detail_cache[tid] = fut.result()
                except Exception as exc:
                    print(f"[ERROR]    task {tid} detail fetch 실패: {exc}")
                    detail_cache[tid] = None

    updated = empty_save = skipped = no_match = error = 0

    # DuckDB 커넥션을 루프 전체에서 재사용 (매 UPDATE마다 open/close 오버헤드 제거).
    db_conn = None
    if db_path and not dry_run:
        try:
            db_conn = _connect_duckdb_with_retry(db_path)
        except Exception as exc:
            print(f"[WARN] DuckDB 커넥션 재사용 실패 — 기존 방식(매번 open) 사용: {exc}")

    for task in annotated_tasks:
        task_id = task["id"]
        try:
            detail = detail_cache.get(task_id) if detail_cache else None
            if detail is None and task_id not in detail_cache:
                detail = fetch_task_detail(ls_url, auth, task_id)
            if detail is None:
                error += 1
                continue
            annotations = detail.get("annotations", [])
            if not annotations:
                skipped += 1
                continue

            # 가장 최근 annotation 사용
            annotation = sorted(annotations, key=lambda a: a["updated_at"])[-1]
            if annotation.get("was_cancelled"):
                skipped += 1
                continue

            # 모드 판별 — result 아이템 type 기준
            result_types = {str(r.get("type") or "") for r in annotation.get("result", [])}
            data = detail.get("data", {}) or {}

            # ───────── image mode (RectangleLabels → SAM3 COCO JSON) ─────────
            # data.image가 있으면 image 프로젝트로 간주. result가 비어도 "객체 없음 확정" 검수로 처리.
            if "rectanglelabels" in result_types or data.get("image"):
                image_url = data.get("image", "")
                image_key = _parse_image_url_to_key(image_url)
                if not image_key:
                    print(f"[NO MATCH] task {task_id} → image URL 파싱 실패: {image_url[:120]}")
                    no_match += 1
                    continue
                sam3_key = _sam3_key_from_image_key(image_key)

                rectangles = annotation_to_rectangles(annotation)

                try:
                    existing = read_json(minio, bucket, sam3_key)
                    if not isinstance(existing, dict):
                        existing = {}
                except Exception:
                    existing = {}

                reviewed_json = build_reviewed_coco_json(existing, rectangles, image_key)

                if dry_run:
                    tag = "[DRY-RUN]" if rectangles else "[DRY-EMPTY]"
                    print(f"{tag}  task {task_id} → {sam3_key} ({len(rectangles)}개 bbox)")
                else:
                    # 빈 검수는 기존 JSON이 있을 때만 annotations를 비워서 덮어씀 (없으면 생성 안 함)
                    if rectangles or existing:
                        write_json(minio, bucket, sam3_key, reviewed_json)
                    db_updated = update_image_labels_in_db(db_path, sam3_key, len(rectangles), conn=db_conn)
                    tag = "[UPDATED]" if rectangles else "[EMPTY]   "
                    print(f"{tag}  task {task_id} → {sam3_key} (bbox {len(rectangles)}, DB {db_updated}건)")
                updated += 1
                continue

            # ───────── video mode (TimelineLabels → events JSON) ─────────
            # SOT = 사람 submit. events 가 비어있어도(false-positive 확정) 그대로 저장.
            # MinIO events JSON 키:
            #   - 기존 인덱스 hit → 그 키 사용
            #   - miss             → raw_key 에서 <folder>/<sub>/events/<stem>.json 계산
            video_url = data.get("video", "")
            raw_key = _extract_raw_key_from_video_url(video_url)
            if not raw_key:
                print(f"[ERROR]    task {task_id} → video URL 파싱 실패: {video_url[:120]}")
                error += 1
                continue

            stem = PurePosixPath(raw_key).stem
            m = _CLIP_PATTERN.match(stem)
            base = m.group(1) if m else stem

            json_key = json_index.get(base) or _resolve_labels_key_for_video(raw_key, stem)

            asset_id = lookup_asset_id_by_raw_key(db_path, raw_key, conn=db_conn)
            if not asset_id:
                print(f"[ERROR]    task {task_id} → raw_files miss: raw_key={raw_key}")
                error += 1
                continue

            new_events = annotation_to_events(annotation, fps)
            ev_count = len(new_events)

            if dry_run:
                tag = "[DRY-EMPTY]" if ev_count == 0 else "[DRY-RUN]  "
                print(f"{tag}  task {task_id} → {json_key} ({ev_count}건)")
            else:
                write_json(minio, bucket, json_key, new_events)
                deleted, inserted = upsert_video_labels(
                    db_path, bucket, json_key, asset_id, new_events, conn=db_conn,
                )
                print(
                    f"[UPDATED]  task {task_id} → {json_key} "
                    f"(MinIO {ev_count}건, DB del={deleted} ins={inserted})"
                )

            if ev_count == 0:
                empty_save += 1
            else:
                updated += 1

        except Exception as exc:
            print(f"[ERROR]    task {task_id}: {exc}")
            error += 1

    if db_conn is not None:
        try:
            db_conn.close()
        except Exception:
            pass

    suffix = " (dry-run)" if dry_run else ""
    print(
        f"\n[DONE] 업데이트 {updated} / 빈저장 {empty_save} / "
        f"스킵 {skipped} / 미매칭 {no_match} / 오류 {error}{suffix}"
    )


def _parse_clip_times(stem: str) -> tuple[float, float]:
    """stem 끝에서 시작/종료 시간 파싱.
    - 8자리 ms: _00222000_00226000 → (222.0, 226.0)
    - 4자리 MMSS: _0435_0455 → (275.0, 295.0)
    fallback: (0.0, inf)
    """
    import re
    m8 = re.search(r"_(\d{8})_(\d{8})$", stem)
    if m8:
        return int(m8.group(1)) / 1000.0, int(m8.group(2)) / 1000.0

    m4 = re.search(r"_(\d{4})_(\d{4})$", stem)
    if m4:
        def mmss_to_sec(s: str) -> float:
            return int(s[:2]) * 60 + int(s[2:])
        return mmss_to_sec(m4.group(1)), mmss_to_sec(m4.group(2))

    return 0.0, float("inf")


def main() -> int:
    parser = argparse.ArgumentParser(description="LS annotation → MinIO JSON sync")
    parser.add_argument("--project", type=int, required=True)
    parser.add_argument("--ls-url", default=os.environ.get("LS_URL", DEFAULT_LS_URL))
    parser.add_argument("--api-key", default=os.environ.get("LS_API_KEY"))
    parser.add_argument("--fps", type=int, default=DEFAULT_FPS)
    parser.add_argument("--minio-endpoint", default=os.environ.get("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT))
    parser.add_argument("--minio-access-key", default=os.environ.get("MINIO_ACCESS_KEY", DEFAULT_MINIO_ACCESS_KEY))
    parser.add_argument("--minio-secret-key", default=os.environ.get("MINIO_SECRET_KEY", DEFAULT_MINIO_SECRET_KEY))
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default="", help="MinIO key prefix 필터")
    parser.add_argument("--dry-run", action="store_true", help="실제 업로드 없이 변경사항만 출력")
    parser.add_argument("--db-path", default=os.environ.get("DATAOPS_DUCKDB_PATH"), required=not os.environ.get("DATAOPS_DUCKDB_PATH"), help="DuckDB 파일 경로 (DATAOPS_DUCKDB_PATH 환경변수로도 설정 가능)")
    args = parser.parse_args()

    if not args.api_key:
        parser.error("--api-key 또는 LS_API_KEY 환경변수가 필요합니다.")
    if not args.db_path:
        parser.error("--db-path 또는 DATAOPS_DUCKDB_PATH 환경변수가 필요합니다.")

    run(
        project_id=args.project,
        ls_url=args.ls_url.rstrip("/"),
        api_key=args.api_key,
        fps=args.fps,
        minio_endpoint=args.minio_endpoint,
        minio_access_key=args.minio_access_key,
        minio_secret_key=args.minio_secret_key,
        bucket=args.bucket,
        prefix=args.prefix,
        dry_run=args.dry_run,
        db_path=args.db_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
