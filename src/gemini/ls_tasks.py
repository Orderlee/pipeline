"""MinIO clip → Label Studio task 자동 생성 및 presigned URL 갱신.

흐름:
  MinIO vlm-processed/*/clips/*.mp4
    → 폴더명 기준 LS project 찾기 or 생성
    → presigned URL(7일) 생성
    → LS task 생성 (중복 방지)
    → vlm-labels/*/events/ JSON 있으면 prediction 즉시 attach

URL 갱신:
  기존 task의 presigned URL 만료 임박(기본 1일 이내) or 만료 시
    → 새 presigned URL 발급
    → task data 업데이트

Usage:
    # 새 task 생성
    python ls_tasks.py create --prefix hyundai_v2/01_27_collection_data

    # URL 갱신
    python ls_tasks.py renew --project-name 01_27_collection_data

    # 환경변수
    export LS_API_KEY=...
    export MINIO_ENDPOINT=172.168.47.36:9000
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import random
import re
import string
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

STATE_FILE = Path(
    os.environ.get("LS_STATE_FILE", Path(__file__).parent / "ls_review_state.json")
)

import boto3
import requests
from botocore.config import Config as BotoConfig

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_LS_URL = "http://localhost:8080"
DEFAULT_MINIO_ENDPOINT = "172.168.47.36:9000"
DEFAULT_MINIO_ACCESS_KEY = "minioadmin"
DEFAULT_MINIO_SECRET_KEY = "minioadmin"
DEFAULT_RAW_BUCKET = "vlm-processed"
DEFAULT_LABEL_BUCKET = "vlm-labels"
DEFAULT_FPS = 24
DEFAULT_PRESIGN_EXPIRES = 3600 * 24 * 7   # 7일
DEFAULT_RENEW_THRESHOLD = 3600 * 24 * 1   # 만료 1일 이내이면 갱신
FROM_NAME = "videoLabels"
TO_NAME = "video"

VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi", ".webm"}

# 클립 파일명 패턴: {base}_{8자리ms}_{8자리ms}
_CLIP_PATTERN = re.compile(r"^(.+)_(\d{8})_(\d{8})$")


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


def list_clip_keys(client, bucket: str, prefix: str) -> list[str]:
    """vlm-processed 버킷에서 clips 하위 영상 파일 목록."""
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if Path(key).suffix.lower() in VIDEO_EXTENSIONS:
                keys.append(key)
    return keys


def generate_presigned_url(client, bucket: str, key: str, expires: int = DEFAULT_PRESIGN_EXPIRES) -> str:
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires,
    )


def list_event_json_keys(client, bucket: str, prefix: str) -> dict[str, str]:
    """{stem → key} for */events/*.json."""
    paginator = client.get_paginator("list_objects_v2")
    index: dict[str, str] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            if key.endswith(".json") and len(parts) >= 2 and parts[-2] == "events":
                index[Path(key).stem] = key
    return index


def read_json_from_minio(client, bucket: str, key: str) -> list | dict:
    resp = client.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))


# ---------------------------------------------------------------------------
# Presigned URL 만료 파싱
# ---------------------------------------------------------------------------

def get_presigned_expiry(url: str) -> datetime | None:
    """presigned URL에서 만료 시각 파싱. 파싱 불가 시 None."""
    try:
        qs = parse_qs(urlparse(url).query)
        date_str = qs.get("X-Amz-Date", [None])[0]
        expires_str = qs.get("X-Amz-Expires", [None])[0]
        if date_str and expires_str:
            created = datetime.strptime(date_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            return created + timedelta(seconds=int(expires_str))
    except Exception:
        pass
    return None


def is_url_expiring(url: str, threshold_sec: int = DEFAULT_RENEW_THRESHOLD) -> bool:
    """만료까지 threshold_sec 이하이면 True."""
    expiry = get_presigned_expiry(url)
    if expiry is None:
        return False
    remaining = (expiry - datetime.now(timezone.utc)).total_seconds()
    return remaining < threshold_sec


# ---------------------------------------------------------------------------
# Label Studio auth
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


# ---------------------------------------------------------------------------
# Label Studio project
# ---------------------------------------------------------------------------

def find_or_create_project(ls_url: str, headers: dict, project_name: str) -> tuple[int, bool]:
    """project_name으로 프로젝트 조회, 없으면 생성. (project_id, is_new) 반환."""
    resp = requests.get(f"{ls_url}/api/projects/", headers=headers)
    resp.raise_for_status()
    data = resp.json()
    projects = data if isinstance(data, list) else data.get("results", [])
    for p in projects:
        if p["title"] == project_name:
            print(f"[INFO] 기존 project 사용: '{project_name}' (id={p['id']})")
            return p["id"], False

    # 생성
    resp = requests.post(
        f"{ls_url}/api/projects/",
        headers={**headers, "Content-Type": "application/json"},
        json={
            "title": project_name,
            "label_config": _default_label_config(),
        },
    )
    resp.raise_for_status()
    project_id = resp.json()["id"]
    print(f"[INFO] 새 project 생성: '{project_name}' (id={project_id})")
    return project_id, True


def register_webhook(ls_url: str, headers: dict, project_id: int) -> None:
    """신규 project에 webhook 자동 등록."""
    webhook_host = os.environ.get("WEBHOOK_HOST", "localhost")
    webhook_port = os.environ.get("WEBHOOK_PORT", "8001")
    webhook_url = f"http://{webhook_host}:{webhook_port}/webhook"
    resp = requests.post(
        f"{ls_url}/api/webhooks/",
        headers={**headers, "Content-Type": "application/json"},
        json={
            "url": webhook_url,
            "actions": ["ANNOTATION_CREATED", "ANNOTATION_UPDATED"],
            "is_active": True,
            "send_payload": True,
            "project": project_id,
        },
    )
    resp.raise_for_status()
    wh_id = resp.json().get("id")
    print(f"[INFO] webhook 자동 등록 완료: id={wh_id}, url={webhook_url}, project={project_id}")


def _default_label_config() -> str:
    return """<View>
  <TimelineLabels name="videoLabels" toName="video">
    <Label value="fall" background="#e74c3c"/>
    <Label value="fight" background="#e67e22"/>
    <Label value="smoke" background="#95a5a6"/>
    <Label value="fire" background="#e74c3c"/>
    <Label value="unsafe_act" background="#f39c12"/>
  </TimelineLabels>
  <Video name="video" value="$video" timelineHeight="120" />
</View>"""


# ---------------------------------------------------------------------------
# Label Studio tasks
# ---------------------------------------------------------------------------

def fetch_existing_task_stems(ls_url: str, headers: dict, project_id: int) -> dict[str, dict]:
    """{clip_stem → task} 인덱스."""
    index: dict[str, dict] = {}
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
            video_url = task.get("data", {}).get("video", "")
            stem = Path(urlparse(video_url).path).stem
            index[stem] = task
        if isinstance(data, list) or not data.get("next"):
            break
        page += 1
    return index


def create_task(ls_url: str, headers: dict, project_id: int, video_url: str, folder: str) -> dict:
    resp = requests.post(
        f"{ls_url}/api/tasks/",
        headers={**headers, "Content-Type": "application/json"},
        json={
            "project": project_id,
            "data": {"video": video_url, "folder": folder},
        },
    )
    resp.raise_for_status()
    return resp.json()


def update_task_url(ls_url: str, headers: dict, task_id: int, new_url: str, folder: str) -> None:
    resp = requests.patch(
        f"{ls_url}/api/tasks/{task_id}/",
        headers={**headers, "Content-Type": "application/json"},
        json={"data": {"video": new_url, "folder": folder}},
    )
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# Prediction 생성 (ls_import 로직 인라인)
# ---------------------------------------------------------------------------

def _rand_id(n: int = 10) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def gemini_events_to_ls_result(
    events: list[dict], fps: int, clip_start_sec: float = 0.0, clip_end_sec: float = float("inf")
) -> list[dict]:
    """이벤트 목록 → LS result. 클립 구간에 속하는 이벤트만 로컬 타임스탬프로 변환."""
    result = []
    for ev in events:
        ts = ev.get("timestamp")
        if not ts or len(ts) < 2:
            continue
        raw_start, raw_end = float(ts[0]), float(ts[1])
        # 클립 구간과 겹치지 않으면 스킵
        if raw_end <= clip_start_sec or raw_start >= clip_end_sec:
            continue
        local_start = max(raw_start - clip_start_sec, 0.0)
        local_end = raw_end - clip_start_sec
        category = str(ev.get("category", "unknown"))
        result.append({
            "value": {
                "ranges": [{"start": round(local_start * fps), "end": round(local_end * fps)}],
                "timelinelabels": [category],
            },
            "id": _rand_id(),
            "from_name": FROM_NAME,
            "to_name": TO_NAME,
            "type": "timelinelabels",
            "origin": "prediction",
        })
    return result


def create_prediction(ls_url: str, headers: dict, task_id: int, result: list[dict]) -> dict:
    resp = requests.post(
        f"{ls_url}/api/predictions/",
        headers={**headers, "Content-Type": "application/json"},
        json={"task": task_id, "result": result, "score": 0.0},
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# 폴더명 추출
# ---------------------------------------------------------------------------

def extract_folder_name(prefix: str) -> str:
    """prefix에서 project name으로 쓸 폴더명 추출 (마지막 non-empty 컴포넌트)."""
    parts = [p for p in prefix.rstrip("/").split("/") if p]
    return parts[-1] if parts else prefix


# ---------------------------------------------------------------------------
# Review state 관리
# ---------------------------------------------------------------------------

def load_review_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def update_review_state(project_id: int, title: str, label_keys: list[str], task_count: int) -> None:
    """project별 검수 상태를 state 파일에 기록."""
    state = load_review_state()
    pid = str(project_id)
    existing = state.get(pid, {})
    merged_keys = sorted(set(existing.get("label_keys", [])) | set(label_keys))
    state[pid] = {
        "project_id": project_id,
        "title": title,
        "task_count": task_count,
        "label_keys": merged_keys,
        "created_at": existing.get("created_at", datetime.now(timezone.utc).isoformat()),
        "last_sync_at": existing.get("last_sync_at"),
        "status": existing.get("status", "pending_finalize"),
    }
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# create 커맨드
# ---------------------------------------------------------------------------

def cmd_create(args, minio, auth_headers: dict) -> None:
    prefix = args.prefix.rstrip("/")
    folder_name = extract_folder_name(prefix)
    project_id, is_new = find_or_create_project(args.ls_url, auth_headers, folder_name)

    # 신규 project에만 webhook 자동 등록
    if is_new:
        try:
            register_webhook(args.ls_url, auth_headers, project_id)
        except Exception as exc:
            print(f"[WARN] webhook 등록 실패 (수동 등록 필요): {exc}")

    print(f"[INFO] 클립 목록 조회 중... (bucket={args.bucket}, prefix={prefix})")
    clip_keys = list_clip_keys(minio, args.bucket, prefix)
    print(f"[INFO] 클립 {len(clip_keys)}개 발견")

    print("[INFO] 기존 task 인덱스 구성 중...")
    existing = fetch_existing_task_stems(args.ls_url, auth_headers, project_id)
    print(f"[INFO] 기존 task {len(existing)}개\n")

    # Gemini JSON 인덱스: clips 한 단계 위 경로에서 탐색
    prefix_parts = prefix.rstrip("/").split("/")
    label_prefix = "/".join(prefix_parts[:-1]) if len(prefix_parts) > 1 else ""
    json_index = list_event_json_keys(minio, args.label_bucket, label_prefix)

    created = skipped = error = 0
    collected_label_keys: list[str] = []

    for key in clip_keys:
        stem = Path(key).stem
        if stem in existing:
            skipped += 1
            continue

        try:
            video_url = generate_presigned_url(minio, args.bucket, key, DEFAULT_PRESIGN_EXPIRES)
            task = create_task(args.ls_url, auth_headers, project_id, video_url, folder_name)
            task_id = task["id"]

            # prediction 즉시 attach + label_key 수집
            m = _CLIP_PATTERN.match(stem)
            base = m.group(1) if m else stem
            clip_start_sec = int(m.group(2)) / 1000.0 if m else 0.0
            clip_end_sec = int(m.group(3)) / 1000.0 if m else float("inf")

            json_key = json_index.get(base)
            pred_count = 0
            if json_key:
                if json_key not in collected_label_keys:
                    collected_label_keys.append(json_key)
                data = read_json_from_minio(minio, args.label_bucket, json_key)
                events = data if isinstance(data, list) else []
                if events:
                    ls_result = gemini_events_to_ls_result(events, args.fps, clip_start_sec, clip_end_sec)
                    if ls_result:
                        create_prediction(args.ls_url, auth_headers, task_id, ls_result)
                        pred_count = len(ls_result)

            pred_msg = f", prediction {pred_count}건" if pred_count else ""
            print(f"[CREATED]  task {task_id} ← {stem}{pred_msg}")
            created += 1

        except Exception as exc:
            print(f"[ERROR]    {key}: {exc}")
            error += 1

    # Review state 파일 기록
    if collected_label_keys:
        update_review_state(project_id, folder_name, collected_label_keys, created)
        print(f"[INFO] review state 저장 완료 ({STATE_FILE.name})")

    print(f"\n[DONE] 생성 {created} / 스킵(기존) {skipped} / 오류 {error} (총 {len(clip_keys)})")


# ---------------------------------------------------------------------------
# renew 커맨드
# ---------------------------------------------------------------------------

def cmd_renew(args, minio, auth_headers: dict) -> None:
    project_id, _ = find_or_create_project(args.ls_url, auth_headers, args.project_name)

    print("[INFO] 기존 task 전체 조회 중...")
    existing = fetch_existing_task_stems(args.ls_url, auth_headers, project_id)
    print(f"[INFO] task {len(existing)}개 조회됨\n")

    renewed = skipped = error = 0

    for stem, task in existing.items():
        task_id = task["id"]
        video_url = task.get("data", {}).get("video", "")
        folder = task.get("data", {}).get("folder", "")

        if not is_url_expiring(video_url, args.threshold):
            skipped += 1
            continue

        # MinIO key 복원: URL path에서 bucket 이후 경로
        parsed = urlparse(video_url)
        # path 형식: /bucket/key
        path_parts = parsed.path.lstrip("/").split("/", 1)
        if len(path_parts) < 2:
            print(f"[SKIP]     task {task_id} URL 파싱 실패")
            skipped += 1
            continue

        bucket, key = path_parts[0], path_parts[1]
        try:
            new_url = generate_presigned_url(minio, bucket, key, DEFAULT_PRESIGN_EXPIRES)
            update_task_url(args.ls_url, auth_headers, task_id, new_url, folder)
            expiry = get_presigned_expiry(new_url)
            print(f"[RENEWED]  task {task_id} ← {stem} (만료: {expiry.strftime('%Y-%m-%d %H:%M UTC') if expiry else '?'})")
            renewed += 1
        except Exception as exc:
            print(f"[ERROR]    task {task_id}: {exc}")
            error += 1

    print(f"\n[DONE] 갱신 {renewed} / 스킵 {skipped} / 오류 {error} (총 {len(existing)})")


def cmd_renew_all(args, minio, auth_headers: dict) -> None:
    """모든 project를 순회하며 만료 임박 presigned URL 갱신."""
    resp = requests.get(f"{args.ls_url}/api/projects/", headers=auth_headers, params={"page_size": 1000})
    resp.raise_for_status()
    data = resp.json()
    projects = data if isinstance(data, list) else data.get("results", [])

    if not projects:
        print("[INFO] LS에 project가 없습니다.")
        return

    total_renewed = total_skipped = total_error = 0

    for project in projects:
        title = project["title"]
        print(f"\n{'='*60}")
        print(f"[PROJECT] {title} (id={project['id']})")
        print(f"{'='*60}")

        args.project_name = title
        try:
            cmd_renew(args, minio, auth_headers)
        except Exception as exc:
            print(f"[ERROR] project '{title}' 갱신 실패: {exc}")
            total_error += 1

    print(f"\n[ALL DONE] {len(projects)}개 project 처리 완료")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="MinIO clip → Label Studio task 관리")
    parser.add_argument("--ls-url", default=os.environ.get("LS_URL", DEFAULT_LS_URL))
    parser.add_argument("--api-key", default=os.environ.get("LS_API_KEY"))
    parser.add_argument("--minio-endpoint", default=os.environ.get("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT))
    parser.add_argument("--minio-access-key", default=os.environ.get("MINIO_ACCESS_KEY", DEFAULT_MINIO_ACCESS_KEY))
    parser.add_argument("--minio-secret-key", default=os.environ.get("MINIO_SECRET_KEY", DEFAULT_MINIO_SECRET_KEY))
    parser.add_argument("--bucket", default=DEFAULT_RAW_BUCKET)
    parser.add_argument("--label-bucket", default=DEFAULT_LABEL_BUCKET)
    parser.add_argument("--fps", type=int, default=DEFAULT_FPS)

    sub = parser.add_subparsers(dest="command", required=True)

    # create
    p_create = sub.add_parser("create", help="새 task 생성")
    p_create.add_argument("--prefix", required=True, help="MinIO prefix (예: hyundai_v2/01_27_collection_data)")

    # renew
    p_renew = sub.add_parser("renew", help="만료 임박 presigned URL 갱신")
    p_renew_group = p_renew.add_mutually_exclusive_group(required=True)
    p_renew_group.add_argument("--project-name", help="LS project 이름 (폴더명)")
    p_renew_group.add_argument("--all-projects", action="store_true", help="모든 project의 URL 일괄 갱신")
    p_renew.add_argument("--threshold", type=int, default=DEFAULT_RENEW_THRESHOLD, help="갱신 임계값(초), 기본 86400(1일)")

    args = parser.parse_args()

    if not args.api_key:
        parser.error("--api-key 또는 LS_API_KEY 환경변수가 필요합니다.")

    minio = build_minio_client(args.minio_endpoint, args.minio_access_key, args.minio_secret_key)
    auth_headers = resolve_auth_headers(args.ls_url, args.api_key)

    if args.command == "create":
        cmd_create(args, minio, auth_headers)
    elif args.command == "renew":
        if args.all_projects:
            cmd_renew_all(args, minio, auth_headers)
        else:
            cmd_renew(args, minio, auth_headers)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
