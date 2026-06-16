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
    export MINIO_ENDPOINT=10.0.0.51:9000
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_LS_URL = "http://localhost:8080"
DEFAULT_MINIO_ENDPOINT = "10.0.0.51:9000"
DEFAULT_MINIO_ACCESS_KEY = ""
DEFAULT_MINIO_SECRET_KEY = ""
# raw bucket: Gemini 초벌이 끝난 원본 영상이 들어있는 곳. LS task는 원본 영상을 가리킴.
# clip 분할은 LS 검수 확정 후 post_review_clip_job에서 vlm-processed/{folder}/clips/*.mp4 로 생성.
DEFAULT_RAW_BUCKET = "vlm-raw"
DEFAULT_LABEL_BUCKET = "vlm-labels"
DEFAULT_FPS = 24
DEFAULT_PRESIGN_EXPIRES = 3600 * 24 * 7  # 7일
DEFAULT_RENEW_THRESHOLD = 3600 * 24 * 1  # 만료 1일 이내이면 갱신

FROM_NAME = "videoLabels"
TO_NAME = "video"
VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi", ".webm"}

# 클립 파일명 패턴: {base}_{8자리ms}_{8자리ms}
_CLIP_PATTERN = re.compile(r"^(.+)_(\d{8})_(\d{8})$")


# ---------------------------------------------------------------------------
# Category synonyms (canonical source; re-used by ls_tasks_label_config.py)
# ---------------------------------------------------------------------------

# dispatch canonical category → 동의어 집합 (lowercase). Gemini/SAM3 의 raw prediction 을
# dispatch 가 요구한 라벨로 정규화한다. 매핑 안 되는 카테고리는 prediction 에서 drop
# (리뷰어에게 노이즈 라벨이 섞이지 않도록).
# 운영 중 새 Gemini/SAM3 동의어가 나오면 여기에 추가.
CATEGORY_SYNONYMS: dict[str, set[str]] = {
    "falldown": {
        "falldown",
        "fall",
        "simulated_fall",
        "fall_simulation",
        "intentional_fall_simulation",
        "fall_recovery_drill",
        "recovery_from_fall_simulation",
        "deliberate_fall_from_wheelchair",
        "fall_recovery",
        "fall_risk",
        "fall_assistance",
        # VHC 의료진이 의도적으로 연출한 낙상 시나리오 — falldown 데이터로 유효.
        "deliberate_lie_down",
        "deliberate_recovery",
        # smart-city 에서 바닥에 쓰러진 사람 묘사 — 낙상 의미.
        "person_lying_on_ground",
        # 2026-06-04: promote 폼 hybrid preset SAM3 자연어 phrase.
        # promote.html JS 의 PRESETS["falldown"].classes 와 sync 필요.
        # 사용자 e00879ff-b04 batch 에서 SAM3 가 40 boxes 잡았는데 LS prediction
        # 으로 import 안 되던 issue 의 원인 — normalizer drop.
        "fallen person",
        "person lying down",
        "person on the ground",
    },
    "person": {"person"},
    "fire": {
        "fire",
        "flame",
        "explosion",
        # 2026-06-04 hybrid preset SAM3 phrase — PRESETS["fire"].classes sync.
        "open flame",
    },
    "smoke": {
        "smoke",
        "smoking",
        "cigarette",
        # 2026-06-04 hybrid preset SAM3 phrase — PRESETS["smoke"].classes sync.
        "smoke cloud",
        # NOTE: 'flame' 은 fire 의 synonym 으로만 유지 (test_ls_category_synonyms
        # 의 test_existing_fire_smoke_person_mappings_intact 호환). 영상에 fire+smoke
        # 공출현 시 사용자가 두 preset 모두 선택해야 함 (의도된 정책).
    },
    # 2026-05-29: vanguardhealthcarevhc_v2 dispatch 카테고리 매핑 추가.
    # 운영 진단으로 normalizer drop 폭주 발견 (515 events 중 18건만 매핑됨).
    # 보수적으로 의미 직접 일치 케이스만 등록. 추가 동의어는 운영자 검토 후 별도 PR.
    "violence": {
        "violence",
        "fight",
        # 2026-06-04 hybrid preset SAM3 phrase — PRESETS["violence"].classes sync.
        "fighting people",
        "punching person",
        "person hitting person",
    },
    "weapon": {
        # 2026-06-04 hybrid preset 신규 canonical. PRESETS["weapon"].classes sync.
        "weapon",
        "gun",
        "knife",
        "baseball bat",
        "bat",
        "sword",
    },
    "climbing up": {"climbing up", "climbing_up", "unsafe_climbing_activity"},
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


# ---------------------------------------------------------------------------
# State file path
# ---------------------------------------------------------------------------


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
# main
# ---------------------------------------------------------------------------


def main() -> int:
    try:
        from gemini.ls_tasks_minio import (
            build_minio_client,
            cmd_renew,
            cmd_renew_all,
        )
        from gemini.ls_tasks_create import cmd_create, cmd_attach_predictions
    except ModuleNotFoundError:
        from ls_tasks_minio import (  # type: ignore[no-redef]
            build_minio_client,
            cmd_renew,
            cmd_renew_all,
        )
        from ls_tasks_create import cmd_create, cmd_attach_predictions  # type: ignore[no-redef]

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
    p_renew.add_argument(
        "--threshold", type=int, default=DEFAULT_RENEW_THRESHOLD, help="갱신 임계값(초), 기본 86400(1일)"
    )

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

    _MINIO_DEFAULTS = {"minioadmin", "admin", ""}
    # config.py(PipelineConfig) / minio_cross_sync.py 와 동일한 escape hatch.
    # 보안 커밋(SEC-MINIO-FALLBACK)이 이 가드를 추가하면서 다른 두 곳이 존중하는
    # ALLOW_INSECURE_DEFAULT_CREDS 우회를 누락 → prod(기본 자격 + 우회 ON)에서 LS
    # create/renew 만 exit=2 로 실패. 일관성 회복: 우회 ON 이면 기본 자격 허용.
    _insecure_ok = os.environ.get("ALLOW_INSECURE_DEFAULT_CREDS", "").strip().lower() in {"1", "true", "yes"}
    if not _insecure_ok:
        if not args.minio_access_key or args.minio_access_key in _MINIO_DEFAULTS:
            parser.error("--minio-access-key 또는 MINIO_ACCESS_KEY 에 유효한 자격증명이 필요합니다.")
        if not args.minio_secret_key or args.minio_secret_key in _MINIO_DEFAULTS:
            parser.error("--minio-secret-key 또는 MINIO_SECRET_KEY 에 유효한 자격증명이 필요합니다.")
    else:
        # 우회 경로: 빈 값이면 minioadmin 으로 fallback (minio_cross_sync 와 동일 동작)
        if not args.minio_access_key:
            args.minio_access_key = "minioadmin"
        if not args.minio_secret_key:
            args.minio_secret_key = "minioadmin"

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
