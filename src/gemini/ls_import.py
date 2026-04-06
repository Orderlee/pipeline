"""Import Gemini event JSON from MinIO as Label Studio predictions.

MinIO에서 vlm-labels 버킷의 */events/*.json 파일을 읽어
Label Studio prediction으로 import합니다.

JSON은 raw 영상 기준 하나로 통합 출력되고,
클립은 {base}_{start_ms8}_{end_ms8}.mp4 형식으로 분리되어 LS task로 존재합니다.
각 이벤트를 해당 클립 task에 로컬 타임스탬프로 변환하여 prediction을 생성합니다.

Usage:
    python ls_import.py --project 3

    # 특정 prefix만
    python ls_import.py --project 3 --prefix hyundai_v2/01_27_collection_data

    # 환경변수로 설정
    export LS_API_KEY=...
    export MINIO_ENDPOINT=172.168.47.36:9000
    python ls_import.py --project 3
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import random
import re
import string
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse, parse_qs

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
DEFAULT_BUCKET = "vlm-labels"
DEFAULT_FPS = 24
FROM_NAME = "videoLabels"
TO_NAME = "video"

# 클립 파일명 패턴: {base}_{8자리ms}_{8자리ms}
_CLIP_PATTERN = re.compile(r"^(.+)_(\d{8})_(\d{8})$")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ClipTask:
    task_id: int
    clip_start_sec: float
    clip_end_sec: float


# ---------------------------------------------------------------------------
# MinIO helpers
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


def list_event_json_keys(client, bucket: str, prefix: str = "") -> list[str]:
    """vlm-labels 버킷에서 .../events/*.json 키만 반환."""
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            if key.endswith(".json") and len(parts) >= 2 and parts[-2] == "events":
                keys.append(key)
    return keys


def read_json_from_minio(client, bucket: str, key: str) -> list | dict:
    response = client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


# ---------------------------------------------------------------------------
# Label Studio task index
# ---------------------------------------------------------------------------

def _decode_fileuri(video_url: str) -> str | None:
    """base64 fileuri → S3 경로."""
    try:
        parsed = urlparse(video_url)
        qs = parse_qs(parsed.query)
        encoded = qs.get("fileuri", [None])[0]
        if encoded:
            return base64.b64decode(encoded).decode("utf-8")
    except Exception:
        pass
    return None


def resolve_auth_headers(ls_url: str, token: str) -> dict[str, str]:
    """refresh token이면 access token으로 교환, 아니면 그대로 사용."""
    try:
        payload_part = token.split(".")[1]
        payload_part += "=" * (-len(payload_part) % 4)
        payload = json.loads(base64.b64decode(payload_part).decode("utf-8"))
        if payload.get("token_type") == "refresh":
            resp = requests.post(
                f"{ls_url}/api/token/refresh/",
                json={"refresh": token},
            )
            resp.raise_for_status()
            access_token = resp.json()["access"]
            return {"Authorization": f"Bearer {access_token}"}
    except Exception:
        pass
    return {"Authorization": f"Token {token}"}


def build_clip_task_index(ls_url: str, auth_headers: dict, project_id: int) -> dict[str, list[ClipTask]]:
    """base_name → [ClipTask, ...] 인덱스 구성.

    클립 파일명: {base}_{start_ms8}_{end_ms8}.mp4
    stem을 파싱해서 base_name 기준으로 그룹핑.
    파싱 실패 시 stem 전체를 base_name으로 처리 (1:1 매핑 fallback).
    """
    index: dict[str, list[ClipTask]] = {}
    page = 1

    while True:
        resp = requests.get(
            f"{ls_url}/api/tasks/",
            headers=auth_headers,
            params={"project": project_id, "page": page, "page_size": 500},
        )
        resp.raise_for_status()
        data = resp.json()
        tasks = data if isinstance(data, list) else data.get("tasks", [])
        if not tasks:
            break

        for task in tasks:
            task_id = task["id"]
            video_url = task.get("data", {}).get("video", "")
            s3_path = _decode_fileuri(video_url)
            stem = Path(s3_path or video_url).stem

            m = _CLIP_PATTERN.match(stem)
            if m:
                base = m.group(1)
                clip_start_sec = int(m.group(2)) / 1000.0
                clip_end_sec = int(m.group(3)) / 1000.0
            else:
                # 클립 패턴 없는 경우 (1:1 매핑 fallback)
                base = stem
                clip_start_sec = 0.0
                clip_end_sec = float("inf")

            index.setdefault(base, []).append(
                ClipTask(task_id=task_id, clip_start_sec=clip_start_sec, clip_end_sec=clip_end_sec)
            )

        if isinstance(data, list) or not data.get("next"):
            break
        page += 1

    return index


# ---------------------------------------------------------------------------
# Gemini JSON → Label Studio result
# ---------------------------------------------------------------------------

def _rand_id(n: int = 10) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def event_to_ls_annotation(event: dict, clip_start_sec: float, fps: int) -> dict | None:
    """이벤트 하나를 클립 로컬 타임스탬프 기준 LS annotation으로 변환."""
    ts = event.get("timestamp")
    if not ts or len(ts) < 2:
        return None
    raw_start, raw_end = float(ts[0]), float(ts[1])
    local_start = max(raw_start - clip_start_sec, 0.0)
    local_end = raw_end - clip_start_sec
    if local_end <= 0:
        return None
    category = str(event.get("category", "unknown"))
    return {
        "value": {
            "ranges": [{"start": round(local_start * fps), "end": round(local_end * fps)}],
            "timelinelabels": [category],
        },
        "id": _rand_id(),
        "from_name": FROM_NAME,
        "to_name": TO_NAME,
        "type": "timelinelabels",
        "origin": "prediction",
    }


def match_events_to_clips(
    events: list[dict],
    clips: list[ClipTask],
    fps: int,
) -> list[tuple[ClipTask, list[dict]]]:
    """각 클립에 속하는 이벤트를 매핑. [(ClipTask, [ls_annotation, ...]), ...]"""
    result = []
    for clip in clips:
        ls_annotations = []
        for ev in events:
            ts = ev.get("timestamp")
            if not ts or len(ts) < 2:
                continue
            raw_start, raw_end = float(ts[0]), float(ts[1])
            # 이벤트가 클립 구간과 겹치는지 확인
            if raw_end <= clip.clip_start_sec or raw_start >= clip.clip_end_sec:
                continue
            ann = event_to_ls_annotation(ev, clip.clip_start_sec, fps)
            if ann:
                ls_annotations.append(ann)
        result.append((clip, ls_annotations))
    return result


# ---------------------------------------------------------------------------
# Label Studio API
# ---------------------------------------------------------------------------

def create_prediction(ls_url: str, headers: dict, task_id: int, result: list[dict]) -> dict:
    resp = requests.post(
        f"{ls_url}/api/predictions/",
        headers={**headers, "Content-Type": "application/json"},
        json={"task": task_id, "result": result, "score": 0.0},
    )
    resp.raise_for_status()
    return resp.json()


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
) -> None:
    print(f"[INFO] MinIO: {minio_endpoint}, bucket={bucket}, prefix='{prefix}'")
    minio = build_minio_client(minio_endpoint, minio_access_key, minio_secret_key)

    keys = list_event_json_keys(minio, bucket, prefix)
    print(f"[INFO] events/*.json {len(keys)}개 발견")
    if not keys:
        print("[WARN] 처리할 파일이 없습니다.")
        return

    print(f"[INFO] Label Studio task 인덱스 구성 중... ({ls_url}, project={project_id})")
    auth_headers = resolve_auth_headers(ls_url, api_key)
    clip_index = build_clip_task_index(ls_url, auth_headers, project_id)
    total_tasks = sum(len(v) for v in clip_index.values())
    print(f"[INFO] task {total_tasks}개 로드됨 (base {len(clip_index)}종)\n")

    ok = no_match = empty = error = 0

    for key in keys:
        base_name = Path(key).stem  # raw 영상명 = JSON stem

        clips = clip_index.get(base_name)
        if not clips:
            print(f"[NO MATCH] {key}  (base='{base_name}')")
            no_match += 1
            continue

        try:
            data = read_json_from_minio(minio, bucket, key)
            events: list[dict] = data if isinstance(data, list) else []

            if not events:
                for clip in clips:
                    print(f"[EMPTY]    {key} (task {clip.task_id})")
                empty += 1
                continue

            matched = match_events_to_clips(events, clips, fps)
            for clip, ls_annotations in matched:
                if not ls_annotations:
                    print(f"[NO EVENT] task {clip.task_id} ({clip.clip_start_sec:.1f}s~{clip.clip_end_sec:.1f}s) ← 이벤트 없음")
                    continue
                prediction = create_prediction(ls_url, auth_headers, clip.task_id, ls_annotations)
                print(
                    f"[OK]       task {clip.task_id} "
                    f"({clip.clip_start_sec:.1f}s~{clip.clip_end_sec:.1f}s) "
                    f"← prediction {prediction.get('id')} ({len(ls_annotations)}건)"
                )
                ok += 1

        except Exception as exc:
            print(f"[ERROR]    {key}: {exc}")
            error += 1

    print(f"\n[DONE] 성공 {ok} / 미매칭 {no_match} / 빈파일 {empty} / 오류 {error} (총 {len(keys)})")


def main() -> int:
    parser = argparse.ArgumentParser(description="MinIO Gemini JSON → Label Studio prediction import")
    parser.add_argument("--project", type=int, required=True, help="Label Studio project ID")
    parser.add_argument("--ls-url", default=os.environ.get("LS_URL", DEFAULT_LS_URL))
    parser.add_argument("--api-key", default=os.environ.get("LS_API_KEY"))
    parser.add_argument("--fps", type=int, default=DEFAULT_FPS)
    parser.add_argument("--minio-endpoint", default=os.environ.get("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT))
    parser.add_argument("--minio-access-key", default=os.environ.get("MINIO_ACCESS_KEY", DEFAULT_MINIO_ACCESS_KEY))
    parser.add_argument("--minio-secret-key", default=os.environ.get("MINIO_SECRET_KEY", DEFAULT_MINIO_SECRET_KEY))
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default="", help="MinIO key prefix 필터 (예: hyundai_v2/01_27_collection_data)")
    args = parser.parse_args()

    if not args.api_key:
        parser.error("--api-key 또는 LS_API_KEY 환경변수가 필요합니다.")

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
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
