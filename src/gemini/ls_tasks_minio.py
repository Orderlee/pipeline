"""MinIO client, presigned URL helpers, LS project/task helpers, and renew CLI commands."""

from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import boto3
import requests
from botocore.config import Config as BotoConfig

try:
    from gemini.ls_tasks_label_config import _default_label_config
except ModuleNotFoundError:
    from ls_tasks_label_config import _default_label_config  # type: ignore[no-redef]

DEFAULT_PRESIGN_EXPIRES = 3600 * 24 * 7  # 7일
DEFAULT_RENEW_THRESHOLD = 3600 * 24 * 1  # 만료 1일 이내이면 갱신
VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi", ".webm"}


# ---------------------------------------------------------------------------
# Label Studio auth
# cmd_renew_all 이 160 project 를 순회하는 동안 access token(JWT refresh→Bearer, ~5분 만료)이
# 끊기지 않도록 project 마다 재발급할 때 사용. ls_tasks.py 의 동일 함수를 복사 — ls_tasks.py 가
# cmd_renew_all 을 이 모듈에서 import 하므로 역 import 시 순환. ls_sync.py 도 자체 사본을 둔다.
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
    """vlm-raw 버킷에서 prefix 하위 영상 파일 목록."""
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


def list_sam3_json_keys(client, bucket: str, prefix: str) -> dict[str, str]:
    """{image_stem → key} for */sam3_segmentations/*.json (COCO per-image)."""
    paginator = client.get_paginator("list_objects_v2")
    index: dict[str, str] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            if key.endswith(".json") and len(parts) >= 2 and parts[-2] == "sam3_segmentations":
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
# LS project helpers
# ---------------------------------------------------------------------------


def find_or_create_project(
    ls_url: str,
    headers: dict,
    project_name: str,
    label_config: str | None = None,
) -> tuple[int, bool]:
    """project_name으로 프로젝트 조회, 없으면 생성. (project_id, is_new) 반환."""
    resp = requests.get(f"{ls_url}/api/projects/", headers=headers)
    resp.raise_for_status()
    data = resp.json()
    projects = data if isinstance(data, list) else data.get("results", [])
    for p in projects:
        if p["title"] == project_name:
            print(f"[INFO] 기존 project 사용: '{project_name}' (id={p['id']})")
            return p["id"], False

    resp = requests.post(
        f"{ls_url}/api/projects/",
        headers={**headers, "Content-Type": "application/json"},
        json={
            "title": project_name,
            "label_config": label_config or _default_label_config(),
        },
    )
    resp.raise_for_status()
    project_id = resp.json()["id"]
    print(f"[INFO] 새 project 생성: '{project_name}' (id={project_id})")
    return project_id, True


def register_webhook(ls_url: str, headers: dict, project_id: int) -> None:
    """신규 project에 webhook 자동 등록."""
    import logging

    _logger = logging.getLogger(__name__)
    webhook_host = os.environ.get("WEBHOOK_HOST", "localhost")
    webhook_port = os.environ.get("WEBHOOK_PORT", "8001")
    webhook_url = f"http://{webhook_host}:{webhook_port}/webhook"
    secret = os.environ.get("LS_WEBHOOK_SECRET", "")
    if not secret:
        _logger.error(
            "LS_WEBHOOK_SECRET 미설정 — webhook 자동 등록 건너뜀 (project=%s). "
            "수동 등록: python ls_webhook.py register --project %s",
            project_id,
            project_id,
        )
        return
    resp = requests.post(
        f"{ls_url}/api/webhooks/",
        headers={**headers, "Content-Type": "application/json"},
        json={
            "url": webhook_url,
            "actions": ["ANNOTATION_CREATED", "ANNOTATION_UPDATED"],
            "is_active": True,
            "send_payload": True,
            "send_for_all_actions": False,
            "project": project_id,
            "headers": {"X-Webhook-Token": secret},
        },
    )
    resp.raise_for_status()
    wh_id = resp.json().get("id")
    print(f"[INFO] webhook 자동 등록 완료: id={wh_id}, url={webhook_url}, project={project_id}")


# ---------------------------------------------------------------------------
# LS task helpers
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


def update_task_url(ls_url: str, headers: dict, task_id: int, new_url: str, folder: str) -> None:
    resp = requests.patch(
        f"{ls_url}/api/tasks/{task_id}/",
        headers={**headers, "Content-Type": "application/json"},
        json={"data": {"video": new_url, "folder": folder}},
    )
    resp.raise_for_status()


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

        parsed = urlparse(video_url)
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
            print(
                f"[RENEWED]  task {task_id} ← {stem} (만료: {expiry.strftime('%Y-%m-%d %H:%M UTC') if expiry else '?'})"
            )
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

    total_error = 0

    for project in projects:
        title = project["title"]
        print(f"\n{'=' * 60}")
        print(f"[PROJECT] {title} (id={project['id']})")
        print(f"{'=' * 60}")

        args.project_name = title
        # access token(JWT refresh→Bearer, ~5분 만료)이 160개 project 순회 동안 끊기지 않도록
        # project 마다 재발급 (static Token 이면 동일 헤더 반환 → no-op). 누적 TTL 만료 방지.
        auth_headers = resolve_auth_headers(args.ls_url, args.api_key)
        try:
            cmd_renew(args, minio, auth_headers)
        except Exception as exc:
            print(f"[ERROR] project '{title}' 갱신 실패: {exc}")
            total_error += 1

    print(f"\n[ALL DONE] {len(projects)}개 project 처리 완료")
