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

def _default_ls_state_path() -> Path:
    # 소스 트리(`/src/vlm/gemini/`)는 read-only bind mount 이므로 기본값은 쓰기 가능 위치로 폴백.
    # 우선순위: LS_STATE_FILE > DAGSTER_HOME > /tmp.
    override = os.environ.get("LS_STATE_FILE")
    if override:
        return Path(override)
    dagster_home = os.environ.get("DAGSTER_HOME")
    if dagster_home:
        return Path(dagster_home) / "ls_review_state.json"
    return Path("/tmp/ls_review_state.json")


STATE_FILE = _default_ls_state_path()

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
# raw bucket: Gemini 초벌이 끝난 원본 영상이 들어있는 곳. LS task는 원본 영상을 가리킴.
# clip 분할은 LS 검수 확정 후 post_review_clip_job에서 vlm-processed/{folder}/clips/*.mp4 로 생성.
DEFAULT_RAW_BUCKET = "vlm-raw"
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

def find_or_create_project(
    ls_url: str,
    headers: dict,
    project_name: str,
    label_config: str | None = None,
) -> tuple[int, bool]:
    """project_name으로 프로젝트 조회, 없으면 생성. (project_id, is_new) 반환.

    label_config: None 이면 _default_label_config() 사용 (video 전용 legacy).
    기존 project 가 이미 있으면 label_config 는 건드리지 않음 (사람이 LS UI 에서 관리).
    """
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
            "label_config": label_config or _default_label_config(),
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


_LABEL_PALETTE = [
    "#e74c3c", "#e67e22", "#f39c12", "#16a085",
    "#2980b9", "#8e44ad", "#7f8c8d", "#27ae60",
]

# dispatch canonical category → 동의어 집합 (lowercase). Gemini/SAM3 의 raw prediction 을
# dispatch 가 요구한 라벨로 정규화한다. 매핑 안 되는 카테고리는 prediction 에서 drop
# (리뷰어에게 노이즈 라벨이 섞이지 않도록).
# 운영 중 새 Gemini/SAM3 동의어가 나오면 여기에 추가.
CATEGORY_SYNONYMS: dict[str, set[str]] = {
    "falldown": {
        "falldown", "fall", "simulated_fall", "fall_simulation",
        "intentional_fall_simulation", "fall_recovery_drill",
        "recovery_from_fall_simulation", "deliberate_fall_from_wheelchair",
        "fall_recovery", "fall_risk", "fall_assistance",
        # VHC 의료진이 의도적으로 연출한 낙상 시나리오 — falldown 데이터로 유효.
        "deliberate_lie_down", "deliberate_recovery",
        # smart-city 에서 바닥에 쓰러진 사람 묘사 — 낙상 의미.
        "person_lying_on_ground",
    },
    "person":   {"person"},
    "fire":     {"fire", "flame", "explosion"},
    "smoke":    {"smoke", "smoking", "cigarette"},
}


def build_label_normalizer(target_cats: list[str]) -> dict[str, str]:
    """target_cats 에 속하는 canonical → synonym 매핑을 뒤집어 {synonym: canonical} 리턴.

    target_cats 에 없는 canonical 은 무시. target_cats 에 있지만 SYNONYMS 테이블에 없는
    canonical 은 자기 자신만 매핑 (identity). 키는 전부 lowercase.
    """
    canonical_set = {c.strip().lower() for c in (target_cats or []) if c}
    normalizer: dict[str, str] = {}
    for canon, synonyms in CATEGORY_SYNONYMS.items():
        if canon not in canonical_set:
            continue
        for s in synonyms:
            normalizer[s.strip().lower()] = canon
    # SYNONYMS 테이블에 없는 canonical 도 자기 자신 매핑.
    for canon in canonical_set:
        normalizer.setdefault(canon, canon)
    return normalizer


def _labels_xml(categories: list[str]) -> str:
    """카테고리 리스트 → `<Label value=.. background=..>` 라인. (dispatch.categories 만 표시, `other` 없음)"""
    cats = [c for c in (categories or []) if c]
    return "\n".join(
        f'    <Label value="{c}" background="{_LABEL_PALETTE[i % len(_LABEL_PALETTE)]}"/>'
        for i, c in enumerate(cats)
    )


def _video_label_config(categories: list[str]) -> str:
    """video project 전용 — TimelineLabels 만."""
    return f"""<View>
  <Video name="video" value="$video" timelineHeight="120" />
  <TimelineLabels name="videoLabels" toName="video">
{_labels_xml(categories)}
  </TimelineLabels>
</View>"""


def _image_label_config(categories: list[str]) -> str:
    """image project 전용 — RectangleLabels 만."""
    return f"""<View>
  <Image name="image" value="$image" />
  <RectangleLabels name="imageLabels" toName="image">
{_labels_xml(categories)}
  </RectangleLabels>
</View>"""


def _parse_csv_or_json_list(raw: str | None) -> list[str]:
    """'a,b,c' 또는 '["a","b"]' → ['a','b','c'] (lowercase, 중복 제거, 순서 유지)."""
    if not raw:
        return []
    rendered = str(raw).strip()
    if not rendered:
        return []
    values: list[str]
    try:
        if rendered.startswith("["):
            parsed = json.loads(rendered)
            values = [str(v) for v in parsed] if isinstance(parsed, list) else []
        else:
            values = rendered.split(",")
    except Exception:
        values = rendered.split(",")
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        s = v.strip().lower()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


# ---------------------------------------------------------------------------
# Image task 생성 (data.image)
# ---------------------------------------------------------------------------

def create_image_task(
    ls_url: str, headers: dict, project_id: int, image_url: str, folder: str
) -> dict:
    resp = requests.post(
        f"{ls_url}/api/tasks/",
        headers={**headers, "Content-Type": "application/json"},
        json={
            "project": project_id,
            "data": {"image": image_url, "folder": folder},
        },
    )
    resp.raise_for_status()
    return resp.json()


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
    events: list[dict],
    fps: int,
    clip_start_sec: float = 0.0,
    clip_end_sec: float = float("inf"),
    normalizer: dict[str, str] | None = None,
) -> list[dict]:
    """이벤트 목록 → LS result. 클립 구간에 속하는 이벤트만 로컬 타임스탬프로 변환.

    normalizer 가 주어지면 각 이벤트의 category 를 dispatch canonical 로 변환.
    매핑 안 되는 카테고리는 drop (리뷰어에게 노이즈 라벨을 노출하지 않기 위함).
    normalizer=None 이면 raw category 그대로 (legacy).
    """
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
        raw_cat = str(ev.get("category", "")).strip().lower()
        if normalizer is not None:
            canonical = normalizer.get(raw_cat)
            if not canonical:
                continue
            category = canonical
        else:
            category = raw_cat or "unknown"
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


def sam3_coco_to_ls_rectangles(
    coco: dict,
    allowed_labels: set[str],
    from_name: str,
    to_name: str,
    label_map: dict[str, str] | None = None,
    normalizer: dict[str, str] | None = None,
) -> list[dict]:
    """SAM3 COCO per-image JSON → LS RectangleLabels result (percentage).

    정규화 우선순위: normalizer(synonym→canonical) > label_map(단순 rename).
    normalizer 가 주어지면 매핑 안 되는 카테고리는 drop (dispatch 범위 밖 탐지 결과는 리뷰에서 제외).
    normalizer=None 이면 allowed_labels 필터만 적용 (legacy).
    이미지 크기는 coco['images'][0]의 width/height 사용.
    """
    images = coco.get("images") or []
    annotations = coco.get("annotations") or []
    categories = coco.get("categories") or []
    if not images or not annotations:
        return []

    img = images[0]
    width = float(img.get("width") or 0)
    height = float(img.get("height") or 0)
    if width <= 0 or height <= 0:
        return []

    cat_by_id = {int(c["id"]): str(c.get("name") or "") for c in categories}

    lmap = label_map or {}
    result: list[dict] = []
    for ann in annotations:
        bbox = ann.get("bbox")
        if not bbox or len(bbox) < 4:
            continue
        raw = cat_by_id.get(int(ann.get("category_id", -1)), "")
        label = lmap.get(raw, raw).strip().lower()
        if normalizer is not None:
            canonical = normalizer.get(label)
            if not canonical:
                continue
            label = canonical
        elif label not in allowed_labels:
            continue
        x, y, w, h = (float(v) for v in bbox[:4])
        score = float(ann.get("score") or 0.0)
        result.append({
            "value": {
                "x": x / width * 100.0,
                "y": y / height * 100.0,
                "width": w / width * 100.0,
                "height": h / height * 100.0,
                "rotation": 0,
                "rectanglelabels": [label],
            },
            "id": _rand_id(),
            "from_name": from_name,
            "to_name": to_name,
            "type": "rectanglelabels",
            "origin": "prediction",
            "original_width": int(width),
            "original_height": int(height),
            "image_rotation": 0,
            "score": score,
        })
    return result


def parse_rectangle_labels_config(label_config: str) -> tuple[str, str, set[str]] | None:
    """label_config XML에서 <RectangleLabels> name/toName과 허용 Label value 목록 추출.

    반환: (from_name, to_name, {label_values}) 또는 RectangleLabels 없으면 None.
    """
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(label_config)
    except ET.ParseError:
        return None
    rect = root.find(".//RectangleLabels")
    if rect is None:
        return None
    from_name = rect.get("name") or ""
    to_name = rect.get("toName") or ""
    values = {el.get("value") for el in rect.findall("Label") if el.get("value")}
    if not from_name or not to_name or not values:
        return None
    return from_name, to_name, values


def fetch_project_label_config(ls_url: str, headers: dict, project_id: int) -> str:
    resp = requests.get(f"{ls_url}/api/projects/{project_id}/", headers=headers)
    resp.raise_for_status()
    return resp.json().get("label_config") or ""


def fetch_existing_task_image_stems(ls_url: str, headers: dict, project_id: int) -> dict[str, dict]:
    """{image_stem → task} 인덱스 — data.image URL 기반."""
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
            image_url = task.get("data", {}).get("image", "")
            if not image_url:
                continue
            stem = Path(urlparse(image_url).path).stem
            if stem:
                index[stem] = task
        if isinstance(data, list) or not data.get("next"):
            break
        page += 1
    return index


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
    """MinIO 데이터 + JSON → LS task + prediction.

    mode='video': vlm-raw/*.mp4 + vlm-labels/*/events/*.json → video task
    mode='image': vlm-labels/*/sam3_segmentations/*.json → image task (프레임 이미지 presigned)

    project 이름 = `<folder>_<mode>_<suffix>` (suffix = sensor 가 dispatch.requested_at 에서 포맷).
    카테고리별 split 하지 않음. label_config 에 dispatch.categories + `other` 가 선언되며,
    prediction 에 없는 카테고리가 나오면 `other` 로 coerce 된다.
    """
    mode = getattr(args, "mode", "video") or "video"
    if mode == "image":
        _create_image(args, minio, auth_headers)
    else:
        _create_video(args, minio, auth_headers)


def _resolve_category_targets(args) -> list[str]:
    """--categories 인자를 파싱. 비어있으면 []."""
    raw = getattr(args, "categories", "") or ""
    return _parse_csv_or_json_list(raw)


def _build_project_title(folder_name: str, mode: str, suffix: str) -> str:
    """`<folder>_<mode>_<suffix>`. suffix 는 dispatch.requested_at 을 YYMMDD_HHMM 으로 포맷한 값."""
    base = f"{folder_name}_{mode}"
    return f"{base}_{suffix}" if suffix else base


def _ensure_dated_project(
    ls_url: str, auth_headers: dict, title: str, label_config: str
) -> int:
    pid, is_new = find_or_create_project(ls_url, auth_headers, title, label_config=label_config)
    if is_new:
        try:
            register_webhook(ls_url, auth_headers, pid)
        except Exception as exc:
            print(f"[WARN] webhook 등록 실패 (수동 등록 필요): {exc}")
    return pid


def _create_video(args, minio, auth_headers: dict) -> None:
    """video mode: vlm-raw/<prefix>/**.mp4 → video task (+ events JSON prediction).

    1 원본 영상 = 1 task. prediction timestamp 는 원본 시간축 그대로(clip_start=0, clip_end=inf).
    project = `<folder>_video_<suffix>`. 카테고리 split 없음 — label_config 에 cats + `other` 가 선언됨.
    """
    prefix = args.prefix.rstrip("/")
    folder_name = extract_folder_name(prefix)
    target_cats = _resolve_category_targets(args)
    suffix = getattr(args, "project_suffix", "") or ""
    title = _build_project_title(folder_name, "video", suffix)

    print(f"[INFO] video mode: prefix={prefix}, project={title}, categories={target_cats or '(none)'}")

    project_id = _ensure_dated_project(
        args.ls_url, auth_headers, title, _video_label_config(target_cats)
    )

    print(f"[INFO] 원본 영상 목록 조회 중... (bucket={args.bucket}, prefix={prefix})")
    video_keys = list_clip_keys(minio, args.bucket, prefix)
    print(f"[INFO] 영상 {len(video_keys)}개 발견")

    existing = fetch_existing_task_stems(args.ls_url, auth_headers, project_id)
    json_index = list_event_json_keys(minio, args.label_bucket, prefix)
    print(f"[INFO] events JSON {len(json_index)}개 인덱싱")

    normalizer = build_label_normalizer(target_cats)
    print(f"[INFO] synonym normalizer: {len(normalizer)}개 매핑 → {sorted(set(normalizer.values()))}\n")

    created = skipped = error = 0
    collected_label_keys: list[str] = []

    for key in video_keys:
        stem = Path(key).stem
        if stem in existing:
            skipped += 1
            continue

        json_key = json_index.get(stem)
        events: list[dict] = []
        if json_key:
            try:
                data = read_json_from_minio(minio, args.label_bucket, json_key)
                events = data if isinstance(data, list) else []
            except Exception as exc:
                print(f"[WARN] events JSON 읽기 실패 {json_key}: {exc}")
                events = []

        try:
            video_url = generate_presigned_url(minio, args.bucket, key, DEFAULT_PRESIGN_EXPIRES)
            rel = key[len(prefix):].lstrip("/") if key.startswith(prefix) else key
            subfolder = rel.split("/", 1)[0] if "/" in rel else ""
            task_folder = subfolder or folder_name
            task = create_task(args.ls_url, auth_headers, project_id, video_url, task_folder)
            task_id = task["id"]

            pred_count = 0
            if events:
                ls_result = gemini_events_to_ls_result(
                    events, args.fps,
                    clip_start_sec=0.0, clip_end_sec=float("inf"),
                    normalizer=normalizer,
                )
                if ls_result:
                    create_prediction(args.ls_url, auth_headers, task_id, ls_result)
                    pred_count = len(ls_result)
                    if json_key and json_key not in collected_label_keys:
                        collected_label_keys.append(json_key)

            pred_msg = f", prediction {pred_count}건" if pred_count else ""
            print(f"[CREATED]  task {task_id} ← {stem}{pred_msg}")
            created += 1

        except Exception as exc:
            print(f"[ERROR]    {key}: {exc}")
            error += 1

    if collected_label_keys:
        update_review_state(project_id, title, collected_label_keys, created)
        print(f"[INFO] review state 저장 완료 ({STATE_FILE.name})")

    print(f"\n[DONE] video mode: 생성 {created} / 스킵(기존) {skipped} / 오류 {error}")


def _create_image(args, minio, auth_headers: dict) -> None:
    """image mode: vlm-labels/<prefix>/*/sam3_segmentations/*.json → image task (+ RectangleLabels prediction).

    각 SAM3 COCO JSON 하나 = 한 프레임. COCO `images[0].file_name` 이 vlm-processed 의 key 를 가리킴.
    project = `<folder>_image_<suffix>`. label_config 에 dispatch.categories + `other` 선언.
    """
    prefix = args.prefix.rstrip("/")
    folder_name = extract_folder_name(prefix)
    target_cats = _resolve_category_targets(args)
    if not target_cats:
        print("[ERROR] image mode 는 --categories 가 필수입니다 (label_config 구성에 필요)")
        return
    suffix = getattr(args, "project_suffix", "") or ""
    title = _build_project_title(folder_name, "image", suffix)

    print(f"[INFO] image mode: prefix={prefix}, project={title}, categories={target_cats}")

    project_id = _ensure_dated_project(
        args.ls_url, auth_headers, title, _image_label_config(target_cats)
    )

    existing = fetch_existing_task_image_stems(args.ls_url, auth_headers, project_id)
    json_index = list_sam3_json_keys(minio, args.label_bucket, prefix)
    print(f"[INFO] sam3_segmentations JSON {len(json_index)}개 인덱싱")

    normalizer = build_label_normalizer(target_cats)
    allowed_labels = set(normalizer.values())
    print(f"[INFO] synonym normalizer: {len(normalizer)}개 매핑 → {sorted(allowed_labels)}\n")

    processed_bucket = getattr(args, "processed_bucket", None) or "vlm-processed"
    created = skipped = error = dropped_no_image = 0

    for stem, json_key in json_index.items():
        if stem in existing:
            skipped += 1
            continue

        try:
            coco = read_json_from_minio(minio, args.label_bucket, json_key)
        except Exception as exc:
            print(f"[ERROR] JSON 읽기 실패 {json_key}: {exc}")
            error += 1
            continue

        images = coco.get("images") or []
        if not images:
            dropped_no_image += 1
            continue
        image_key = str(images[0].get("file_name") or "").strip()
        if not image_key:
            dropped_no_image += 1
            continue

        try:
            image_url = generate_presigned_url(
                minio, processed_bucket, image_key, DEFAULT_PRESIGN_EXPIRES
            )
            rel = image_key[len(prefix):].lstrip("/") if image_key.startswith(prefix) else image_key
            subfolder = rel.split("/", 1)[0] if "/" in rel else ""
            task_folder = subfolder or folder_name
            task = create_image_task(args.ls_url, auth_headers, project_id, image_url, task_folder)
            task_id = task["id"]

            ls_result = sam3_coco_to_ls_rectangles(
                coco, allowed_labels, "imageLabels", "image",
                label_map=None, normalizer=normalizer,
            )
            pred_count = 0
            if ls_result:
                create_prediction(args.ls_url, auth_headers, task_id, ls_result)
                pred_count = len(ls_result)

            print(f"[CREATED]  task {task_id} ← {stem} (bbox {pred_count})")
            created += 1

        except Exception as exc:
            print(f"[ERROR]    {stem}: {exc}")
            error += 1

    # video mode 와 동일하게 review state 에 label_keys 기록.
    # 이전엔 image mode 가 update_review_state 호출을 빠뜨려, /sync-list 에 안 뜨고
    # /sync-approve 가 "label_keys 없음" 으로 거부되던 결함이 있었음.
    # 모든 sam3 JSON 키를 박음 (existing 포함) — update_review_state 가 union merge 라 idempotent.
    # 따라서 같은 명령 재호출만으로 기존 proj 의 backfill 도 자연 처리됨.
    if json_index:
        update_review_state(project_id, title, list(json_index.values()), created + skipped)
        print(f"[INFO] review state 저장 완료 ({STATE_FILE.name})")

    print(
        f"\n[DONE] image mode: 생성 {created} / 스킵(기존) {skipped} / "
        f"이미지경로 누락 {dropped_no_image} / 오류 {error}"
    )


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
# attach-predictions 커맨드 — 기존 task에 Gemini events JSON을 prediction으로 사후 주입
# ---------------------------------------------------------------------------

def _resolve_project_id(ls_url: str, headers: dict, project: str) -> int:
    """project 인자를 id(정수) 또는 title(문자)로 받아 project_id 반환."""
    try:
        return int(project)
    except ValueError:
        pass
    resp = requests.get(f"{ls_url}/api/projects/", headers=headers, params={"page_size": 1000})
    resp.raise_for_status()
    data = resp.json()
    projects = data if isinstance(data, list) else data.get("results", [])
    for p in projects:
        if p["title"] == project:
            return int(p["id"])
    raise RuntimeError(f"LS project를 찾을 수 없음: '{project}'")


def _detect_task_mode(ls_url: str, headers: dict, project_id: int) -> str:
    """첫 task를 조회해 'image' 또는 'video' 모드 자동 감지. 비어있으면 'video' 기본."""
    resp = requests.get(
        f"{ls_url}/api/tasks/",
        headers=headers,
        params={"project": project_id, "page": 1, "page_size": 1},
    )
    resp.raise_for_status()
    data = resp.json()
    tasks = data if isinstance(data, list) else data.get("tasks", [])
    if not tasks:
        return "video"
    d = tasks[0].get("data", {}) or {}
    if d.get("image"):
        return "image"
    return "video"


def _attach_sam3_images(args, minio, auth_headers: dict, project_id: int) -> None:
    prefix = args.prefix.rstrip("/")
    print(f"[INFO] mode=image, project_id={project_id}, label_bucket={args.label_bucket}, prefix={prefix}")

    label_config = fetch_project_label_config(args.ls_url, auth_headers, project_id)
    parsed = parse_rectangle_labels_config(label_config)
    if not parsed:
        print("[ERROR] project label_config에 <RectangleLabels>가 없습니다 — image mode는 bbox 라벨 필수")
        return
    from_name, to_name, allowed = parsed
    label_map: dict[str, str] = {}
    for pair in (args.label_map or "").split(","):
        pair = pair.strip()
        if "=" in pair:
            src, dst = pair.split("=", 1)
            label_map[src.strip()] = dst.strip()
    print(f"[INFO] RectangleLabels: name={from_name}, toName={to_name}, allowed={sorted(allowed)}")
    if label_map:
        print(f"[INFO] label_map: {label_map}")

    existing = fetch_existing_task_image_stems(args.ls_url, auth_headers, project_id)
    print(f"[INFO] 기존 image task {len(existing)}개")

    json_index = list_sam3_json_keys(minio, args.label_bucket, prefix)
    print(f"[INFO] sam3_segmentations JSON {len(json_index)}개 스캔 완료\n")

    attached = skipped_has_pred = skipped_no_json = skipped_empty = error = 0
    for stem, task in existing.items():
        task_id = task["id"]
        if int(task.get("total_predictions") or 0) > 0:
            skipped_has_pred += 1
            continue

        json_key = json_index.get(stem)
        if not json_key:
            skipped_no_json += 1
            continue

        try:
            coco = read_json_from_minio(minio, args.label_bucket, json_key)
            ls_result = sam3_coco_to_ls_rectangles(coco, allowed, from_name, to_name, label_map)
            if not ls_result:
                skipped_empty += 1
                continue
            create_prediction(args.ls_url, auth_headers, task_id, ls_result)
            print(f"[ATTACH]  task {task_id} ← {stem} (bbox {len(ls_result)}건)")
            attached += 1
        except Exception as exc:
            print(f"[ERROR]   task {task_id}: {exc}")
            error += 1

    print(
        f"\n[DONE] 주입 {attached} / 이미있음 {skipped_has_pred} / "
        f"JSON 없음 {skipped_no_json} / 허용라벨 없음 {skipped_empty} / 오류 {error} "
        f"(총 {len(existing)})"
    )


def _attach_video_events(args, minio, auth_headers: dict, project_id: int) -> None:
    prefix = args.prefix.rstrip("/")
    print(f"[INFO] mode=video, project_id={project_id}, label_bucket={args.label_bucket}, prefix={prefix}")

    existing = fetch_existing_task_stems(args.ls_url, auth_headers, project_id)
    print(f"[INFO] 기존 video task {len(existing)}개")

    json_index = list_event_json_keys(minio, args.label_bucket, prefix)
    print(f"[INFO] events JSON {len(json_index)}개 스캔 완료\n")

    attached = skipped_has_pred = skipped_no_json = error = 0
    for stem, task in existing.items():
        task_id = task["id"]
        if int(task.get("total_predictions") or 0) > 0:
            skipped_has_pred += 1
            continue

        m = _CLIP_PATTERN.match(stem)
        base = m.group(1) if m else stem
        clip_start_sec = int(m.group(2)) / 1000.0 if m else 0.0
        clip_end_sec = int(m.group(3)) / 1000.0 if m else float("inf")

        json_key = json_index.get(base)
        if not json_key:
            skipped_no_json += 1
            continue

        try:
            data = read_json_from_minio(minio, args.label_bucket, json_key)
            events = data if isinstance(data, list) else []
            if not events:
                skipped_no_json += 1
                continue
            ls_result = gemini_events_to_ls_result(events, args.fps, clip_start_sec, clip_end_sec)
            if not ls_result:
                skipped_no_json += 1
                continue
            create_prediction(args.ls_url, auth_headers, task_id, ls_result)
            print(f"[ATTACH]  task {task_id} ← {stem} (prediction {len(ls_result)}건)")
            attached += 1
        except Exception as exc:
            print(f"[ERROR]   task {task_id}: {exc}")
            error += 1

    print(
        f"\n[DONE] 주입 {attached} / 이미있음 {skipped_has_pred} / "
        f"JSON 없음 {skipped_no_json} / 오류 {error} (총 {len(existing)})"
    )


def cmd_attach_predictions(args, minio, auth_headers: dict) -> None:
    """기존 task에 MinIO JSON을 prediction으로 사후 주입 (idempotent).

    mode 자동 감지:
      - image 모드: task.data.image 존재 → sam3_segmentations/*.json → RectangleLabels
                    (project label_config의 RectangleLabels 허용 라벨만 필터링)
      - video 모드: task.data.video → events/*.json → TimelineLabels

    --mode 명시 시 자동 감지 무시.
    skip 조건: task.total_predictions > 0 / stem 매칭 JSON 없음 / 주입 result 0건.
    """
    project_id = _resolve_project_id(args.ls_url, auth_headers, args.project)
    mode = args.mode if args.mode != "auto" else _detect_task_mode(
        args.ls_url, auth_headers, project_id
    )
    if mode == "image":
        _attach_sam3_images(args, minio, auth_headers, project_id)
    else:
        _attach_video_events(args, minio, auth_headers, project_id)


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
    p_create.add_argument("--prefix", required=True, help="MinIO prefix (예: vanguardhealthcarevhc)")
    p_create.add_argument(
        "--mode",
        choices=["video", "image"],
        default="video",
        help="video(기본): vlm-raw/*.mp4 + events JSON → video task / "
             "image: vlm-labels/*/sam3_segmentations/*.json → image task (프레임 이미지 presigned)",
    )
    p_create.add_argument(
        "--categories",
        default="",
        help="카테고리 CSV 또는 JSON array — label_config 에 선언될 라벨 목록. "
             "prediction 에 없는 카테고리가 나오면 `other` 로 coerce 됨.",
    )
    p_create.add_argument(
        "--project-suffix",
        default="",
        help="project 이름 접미사 (예: YYMMDD_HHMM). dispatch.requested_at 기준으로 sensor 가 채움. "
             "지정 시 project 이름 = <folder>_<mode>_<suffix> (batch 별 격리).",
    )
    p_create.add_argument(
        "--processed-bucket",
        default="vlm-processed",
        help="image mode 에서 프레임 이미지를 presign 할 버킷 (기본: vlm-processed)",
    )

    # renew
    p_renew = sub.add_parser("renew", help="만료 임박 presigned URL 갱신")
    p_renew_group = p_renew.add_mutually_exclusive_group(required=True)
    p_renew_group.add_argument("--project-name", help="LS project 이름 (폴더명)")
    p_renew_group.add_argument("--all-projects", action="store_true", help="모든 project의 URL 일괄 갱신")
    p_renew.add_argument("--threshold", type=int, default=DEFAULT_RENEW_THRESHOLD, help="갱신 임계값(초), 기본 86400(1일)")

    # attach-predictions — 기존 task에 Gemini events JSON을 prediction으로 사후 주입
    p_attach = sub.add_parser(
        "attach-predictions",
        help="기존 task에 Gemini events JSON을 prediction으로 사후 주입 (idempotent)",
    )
    p_attach.add_argument("--project", required=True, help="LS project id (숫자) 또는 title (문자)")
    p_attach.add_argument("--prefix", required=True, help="vlm-labels 하위 prefix (예: vanguardhealthcarevhc/falldown)")
    p_attach.add_argument(
        "--mode",
        choices=["auto", "image", "video"],
        default="auto",
        help="auto(기본): 첫 task로 판정 / image: SAM3 COCO→RectangleLabels / video: Gemini events→TimelineLabels",
    )
    p_attach.add_argument(
        "--label-map",
        default="",
        help="SAM3→LS 라벨 매핑. 예: 'person=fall,knife=unsafe_act' (image 모드 전용)",
    )

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
    elif args.command == "attach-predictions":
        cmd_attach_predictions(args, minio, auth_headers)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
