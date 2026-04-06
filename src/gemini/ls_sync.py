"""Sync Label Studio annotations back to MinIO Gemini JSON.

LS에서 사람이 수정한 annotation이 있으면 해당 클립의 MinIO JSON을
수정된 GT 값으로 업데이트합니다.

흐름:
  LS task (annotation 있는 것만)
    → clip stem 추출 (video URL decode)
    → MinIO vlm-labels/*/events/{stem}.json 찾기
    → annotation frames ÷ FPS → 초 변환
    → JSON의 해당 이벤트 timestamp 수정
    → MinIO 업로드

Usage:
    python ls_sync.py --project 3 --api-key "..."

    # 특정 prefix만
    python ls_sync.py --project 3 --api-key "..." --prefix hyundai_v2/01_27

    # dry-run (실제 업로드 없이 변경사항만 출력)
    python ls_sync.py --project 3 --api-key "..." --dry-run
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
from pathlib import Path
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


def _decode_fileuri(video_url: str) -> str | None:
    try:
        qs = parse_qs(urlparse(video_url).query)
        encoded = qs.get("fileuri", [None])[0]
        if encoded:
            return base64.b64decode(encoded).decode("utf-8")
    except Exception:
        pass
    return None


def fetch_annotated_tasks(ls_url: str, headers: dict, project_id: int) -> list[dict]:
    """annotation이 1개 이상 있는 task만 반환."""
    annotated = []
    page = 1
    while True:
        resp = requests.get(
            f"{ls_url}/api/tasks/",
            headers=headers,
            params={"project": project_id, "page": page, "page_size": 500},
        )
        resp.raise_for_status()
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


def fetch_task_detail(ls_url: str, headers: dict, task_id: int) -> dict:
    resp = requests.get(f"{ls_url}/api/tasks/{task_id}/", headers=headers)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------

def update_labels_in_db(
    db_path: str,
    json_key: str,
    new_events: list[dict],
) -> int:
    """labels 테이블에서 labels_key 기준으로 timestamp 및 review_status 업데이트.
    - 기존 행 수 > new_events: 잉여 행은 review_status='removed_by_reviewer'
    - 기존 행 수 < new_events: 추가된 이벤트는 신규 행 INSERT
    반환값: 업데이트된 행 수 (removed/inserted 포함)
    """
    import hashlib
    import duckdb
    conn = duckdb.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT label_id, event_index,
                   asset_id, labels_bucket, labels_key,
                   label_format, label_tool, label_status
            FROM labels
            WHERE labels_key = ?
            ORDER BY event_index
            """,
            [json_key],
        ).fetchall()

        if not rows:
            return 0

        updated = 0
        matched = min(len(rows), len(new_events))

        # 1) 공통 구간: timestamp 업데이트
        for i in range(matched):
            label_id = rows[i][0]
            ev = new_events[i]
            start_sec = ev["timestamp"][0]
            end_sec = ev["timestamp"][1]
            conn.execute(
                """
                UPDATE labels
                SET timestamp_start_sec = ?,
                    timestamp_end_sec   = ?,
                    review_status       = 'reviewed',
                    label_source        = 'manual_review'
                WHERE label_id = ?
                """,
                [start_sec, end_sec, label_id],
            )
            updated += 1

        # 2) 검수자가 이벤트 삭제한 경우: 잉여 DB 행 마킹
        for i in range(matched, len(rows)):
            label_id = rows[i][0]
            conn.execute(
                "UPDATE labels SET review_status = 'removed_by_reviewer' WHERE label_id = ?",
                [label_id],
            )
            updated += 1

        # 3) 검수자가 이벤트 추가한 경우: 신규 행 INSERT
        if len(new_events) > len(rows):
            ref = rows[0]
            asset_id, labels_bucket, labels_key_val = ref[2], ref[3], ref[4]
            label_format, label_tool, label_status = ref[5], ref[6], ref[7]
            next_index = max(r[1] for r in rows) + 1

            for i in range(len(rows), len(new_events)):
                ev = new_events[i]
                start_sec = ev["timestamp"][0]
                end_sec = ev["timestamp"][1]
                token = f"{asset_id}|manual_review|{next_index}|{start_sec}|{end_sec}"
                new_id = hashlib.sha1(token.encode()).hexdigest()
                conn.execute(
                    """
                    INSERT INTO labels (
                        label_id, asset_id, labels_bucket, labels_key,
                        label_format, label_tool, label_source,
                        review_status, event_index, event_count,
                        timestamp_start_sec, timestamp_end_sec,
                        label_status
                    ) VALUES (?, ?, ?, ?, ?, ?, 'manual_review', 'reviewed',
                               ?, ?, ?, ?, ?)
                    """,
                    [
                        new_id, asset_id, labels_bucket, labels_key_val,
                        label_format, label_tool,
                        next_index, len(new_events),
                        start_sec, end_sec,
                        label_status,
                    ],
                )
                next_index += 1
                updated += 1

        return updated
    finally:
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
    auth_headers = resolve_auth_headers(ls_url, api_key)
    annotated_tasks = fetch_annotated_tasks(ls_url, auth_headers, project_id)
    print(f"[INFO] annotation 있는 task {len(annotated_tasks)}개\n")

    updated = skipped = no_match = error = 0

    for task in annotated_tasks:
        task_id = task["id"]
        try:
            detail = fetch_task_detail(ls_url, auth_headers, task_id)
            annotations = detail.get("annotations", [])
            if not annotations:
                skipped += 1
                continue

            # 가장 최근 annotation 사용
            annotation = sorted(annotations, key=lambda a: a["updated_at"])[-1]
            if annotation.get("was_cancelled"):
                skipped += 1
                continue

            # clip stem 추출 → base 변환 후 JSON 매칭
            video_url = detail.get("data", {}).get("video", "")
            s3_path = _decode_fileuri(video_url)
            stem = Path(s3_path or video_url).stem
            m = _CLIP_PATTERN.match(stem)
            base = m.group(1) if m else stem

            json_key = json_index.get(base)
            if not json_key:
                print(f"[NO MATCH] task {task_id} → stem='{stem}', base='{base}'")
                no_match += 1
                continue

            # annotation → 이벤트 변환
            new_events = annotation_to_events(annotation, fps)
            if not new_events:
                print(f"[SKIP]     task {task_id} → annotation에 timelinelabels 없음")
                skipped += 1
                continue

            # 기존 JSON 읽기
            existing = read_json(minio, bucket, json_key)
            existing_list: list = existing if isinstance(existing, list) else []

            # clip이 1:1 매핑인 경우 (per-clip JSON): 전체 교체
            # clip이 1:N인 경우 (per-raw JSON): 해당 구간 이벤트만 교체
            if len(existing_list) <= 1:
                # per-clip JSON: 전체 교체
                updated_json = new_events
            else:
                # per-raw JSON: 구간 매핑으로 교체
                # clip_start/end는 stem에서 파싱 (8자리ms 또는 4자리 MMSS)
                clip_start_sec, clip_end_sec = _parse_clip_times(stem)
                idx = find_matching_event_index(existing_list, clip_start_sec, clip_end_sec)
                if idx is None:
                    print(f"[NO EVENT] task {task_id} → JSON에서 매칭 이벤트 없음")
                    skipped += 1
                    continue
                updated_json = existing_list[:]
                # 기존 메타 필드 유지 + timestamp/category/duration 갱신
                for ev in new_events:
                    updated_json[idx] = {**updated_json[idx], **ev}

            if dry_run:
                print(f"[DRY-RUN]  task {task_id} → {json_key}")
                print(f"           변경 내용: {new_events}")
            else:
                write_json(minio, bucket, json_key, updated_json)
                db_updated = update_labels_in_db(db_path, json_key, new_events)
                print(f"[UPDATED]  task {task_id} → {json_key} (MinIO {len(new_events)}건, DB {db_updated}건)")
            updated += 1

        except Exception as exc:
            print(f"[ERROR]    task {task_id}: {exc}")
            error += 1

    suffix = " (dry-run)" if dry_run else ""
    print(f"\n[DONE] 업데이트 {updated} / 스킵 {skipped} / 미매칭 {no_match} / 오류 {error}{suffix}")


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
