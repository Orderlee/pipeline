"""NAS 라벨 데이터 → Label Studio task 일괄 import.

기존 ls_tasks.py와 독립된 로컬 전용 스크립트.
MinIO를 경유하지 않고 NAS 경로에서 직접 읽어 LS task를 생성합니다.

JSON 포맷 (SamsungCNT_Smoke 기준):
{
    "video_info": {"fps": 30.0, "total_frame": 286, ...},
    "clips": {
        "video_clip1": {
            "category": "smoke",
            "timestamp": [start_frame, end_frame],   ← 프레임 단위
            ...
        }
    }
}

Usage:
    # MinIO에 업로드 후 presigned URL로 task 생성 (기본)
    python nas_to_ls.py \\
        --dir /Volumes/PoC_benchmark/.../smoke \\
        --project-name SamsungCNT_Smoke \\
        --api-key $LS_API_KEY

    # 카테고리 필터 (smoke만 prediction으로 표시)
    python nas_to_ls.py \\
        --dir /Volumes/PoC_benchmark/.../smoke \\
        --project-name SamsungCNT_Smoke \\
        --skip-categories normal \\
        --api-key $LS_API_KEY

환경변수:
    LS_API_KEY
    LS_URL               (기본: http://localhost:8080)
    MINIO_ENDPOINT       (기본: 172.168.47.36:9000)
    MINIO_ACCESS_KEY     (기본: minioadmin)
    MINIO_SECRET_KEY     (기본: minioadmin)
    MINIO_IMPORT_BUCKET  (기본: vlm-poc-import)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import random
import string
from pathlib import Path

import boto3
import requests
from botocore.config import Config as BotoConfig

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_LS_URL           = "http://localhost:8080"
DEFAULT_MINIO_ENDPOINT   = "172.168.47.36:9000"
DEFAULT_MINIO_ACCESS_KEY = "minioadmin"
DEFAULT_MINIO_SECRET_KEY = "minioadmin"
DEFAULT_IMPORT_BUCKET    = "vlm-poc-import"
DEFAULT_PRESIGN_EXPIRES  = 3600 * 24 * 7   # 7일

VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi", ".webm"}

FROM_NAME = "videoLabels"
TO_NAME   = "video"


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


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)
        print(f"[INFO] bucket 생성: {bucket}")


def upload_video(client, bucket: str, key: str, local_path: Path) -> None:
    client.upload_file(str(local_path), bucket, key)


def generate_presigned_url(client, bucket: str, key: str, expires: int = DEFAULT_PRESIGN_EXPIRES) -> str:
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires,
    )


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

def find_or_create_project(ls_url: str, headers: dict, project_name: str) -> int:
    resp = requests.get(f"{ls_url}/api/projects/", headers=headers)
    resp.raise_for_status()
    data = resp.json()
    projects = data if isinstance(data, list) else data.get("results", [])
    for p in projects:
        if p["title"] == project_name:
            print(f"[INFO] 기존 project 사용: '{project_name}' (id={p['id']})")
            return p["id"]

    resp = requests.post(
        f"{ls_url}/api/projects/",
        headers={**headers, "Content-Type": "application/json"},
        json={"title": project_name, "label_config": _default_label_config()},
    )
    resp.raise_for_status()
    project_id = resp.json()["id"]
    print(f"[INFO] 새 project 생성: '{project_name}' (id={project_id})")
    return project_id


def _default_label_config() -> str:
    return """<View>
  <Header value="$filename" style="font-size:16px; font-weight:bold; margin-bottom:8px;"/>
  <TimelineLabels name="videoLabels" toName="video">
    <Label value="smoke" background="#95a5a6"/>
    <Label value="fire" background="#e74c3c"/>
    <Label value="normal" background="#2ecc71"/>
  </TimelineLabels>
  <Video name="video" value="$video" timelineHeight="120" />
</View>"""


def fetch_existing_task_stems(ls_url: str, headers: dict, project_id: int) -> set[str]:
    """중복 방지: 이미 생성된 task의 stem 집합."""
    stems: set[str] = set()
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
            stems.add(Path(video_url).stem)
        if isinstance(data, list) or not data.get("next"):
            break
        page += 1
    return stems


def create_task(ls_url: str, headers: dict, project_id: int, video_url: str, meta: dict) -> int:
    resp = requests.post(
        f"{ls_url}/api/tasks/",
        headers={**headers, "Content-Type": "application/json"},
        json={"project": project_id, "data": {"video": video_url, **meta}},
    )
    resp.raise_for_status()
    return resp.json()["id"]


def create_prediction(ls_url: str, headers: dict, task_id: int, result: list[dict]) -> None:
    resp = requests.post(
        f"{ls_url}/api/predictions/",
        headers={**headers, "Content-Type": "application/json"},
        json={"task": task_id, "result": result, "score": 0.0},
    )
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# JSON → LS prediction 변환
# ---------------------------------------------------------------------------

def _rand_id(n: int = 10) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def json_to_ls_result(
    clips: dict,
    skip_categories: set[str],
) -> list[dict]:
    """clips dict → LS TimelineLabels result.
    timestamp는 이미 프레임 단위이므로 변환 없이 그대로 사용.
    """
    result = []
    for clip in clips.values():
        category = str(clip.get("category", "unknown"))
        if category in skip_categories:
            continue
        ts = clip.get("timestamp", [])
        if len(ts) < 2:
            continue
        start_frame, end_frame = int(ts[0]), int(ts[1])
        result.append({
            "value": {
                "ranges": [{"start": start_frame, "end": end_frame}],
                "timelinelabels": [category],
            },
            "id": _rand_id(),
            "from_name": FROM_NAME,
            "to_name": TO_NAME,
            "type": "timelinelabels",
            "origin": "prediction",
        })
    return result


# ---------------------------------------------------------------------------
# 메인 import 로직
# ---------------------------------------------------------------------------

def run_import(args) -> None:
    video_dir = Path(args.dir)
    if not video_dir.exists():
        raise SystemExit(f"경로 없음: {video_dir}")

    ls_url     = args.ls_url.rstrip("/")
    headers    = resolve_auth_headers(ls_url, args.api_key)
    project_id = find_or_create_project(ls_url, headers, args.project_name)

    minio = build_minio_client(args.minio_endpoint, args.minio_access_key, args.minio_secret_key)
    ensure_bucket(minio, args.bucket)

    existing_stems = fetch_existing_task_stems(ls_url, headers, project_id)
    print(f"[INFO] 기존 task {len(existing_stems)}개\n")

    skip_categories = set(args.skip_categories) if args.skip_categories else set()

    video_files = sorted(
        f for f in video_dir.iterdir() if f.suffix.lower() in VIDEO_EXTENSIONS
    )
    print(f"[INFO] 영상 {len(video_files)}개 발견")

    created = skipped = error = 0

    for video_path in video_files:
        stem     = video_path.stem
        json_path = video_path.with_suffix(".json")

        if stem in existing_stems:
            skipped += 1
            continue

        if not json_path.exists():
            print(f"[SKIP]    JSON 없음: {json_path.name}")
            skipped += 1
            continue

        try:
            label_data = json.loads(json_path.read_text(encoding="utf-8"))
            clips      = label_data.get("clips", {})
            video_info = label_data.get("video_info", {})
            fps        = video_info.get("fps", 30.0)

            # MinIO 업로드
            minio_key = f"{args.prefix}/{stem}{video_path.suffix}"
            print(f"[UPLOAD]  {video_path.name} → {args.bucket}/{minio_key}")
            upload_video(minio, args.bucket, minio_key, video_path)
            video_url = generate_presigned_url(minio, args.bucket, minio_key)

            # task 생성
            task_id = create_task(
                ls_url, headers, project_id, video_url,
                meta={"filename": stem, "source": video_info.get("source", ""), "fps": fps},
            )

            # prediction attach
            ls_result = json_to_ls_result(clips, skip_categories)
            if ls_result:
                create_prediction(ls_url, headers, task_id, ls_result)

            print(f"[CREATED] task {task_id} ← {stem} (prediction {len(ls_result)}건)")
            created += 1

        except Exception as exc:
            print(f"[ERROR]   {video_path.name}: {exc}")
            error += 1

    print(f"\n[DONE] 생성 {created} / 스킵 {skipped} / 오류 {error} (총 {len(video_files)})")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="NAS 라벨 데이터 → Label Studio import")
    parser.add_argument("--dir",          required=True, help="영상+JSON이 있는 NAS 디렉토리 경로")
    parser.add_argument("--project-name", required=True, help="LS project 이름")
    parser.add_argument("--prefix",       default="poc-import", help="MinIO 저장 prefix (기본: poc-import)")
    parser.add_argument("--skip-categories", nargs="*", default=["normal"],
                        help="prediction에서 제외할 카테고리 (기본: normal)")
    parser.add_argument("--ls-url",       default=os.environ.get("LS_URL", DEFAULT_LS_URL))
    parser.add_argument("--api-key",      default=os.environ.get("LS_API_KEY"))
    parser.add_argument("--minio-endpoint",   default=os.environ.get("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT))
    parser.add_argument("--minio-access-key", default=os.environ.get("MINIO_ACCESS_KEY", DEFAULT_MINIO_ACCESS_KEY))
    parser.add_argument("--minio-secret-key", default=os.environ.get("MINIO_SECRET_KEY", DEFAULT_MINIO_SECRET_KEY))
    parser.add_argument("--bucket",       default=os.environ.get("MINIO_IMPORT_BUCKET", DEFAULT_IMPORT_BUCKET))

    args = parser.parse_args()

    if not args.api_key:
        parser.error("--api-key 또는 LS_API_KEY 환경변수가 필요합니다.")

    run_import(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
