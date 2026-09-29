"""Label Studio task creation and prediction attach commands."""

from __future__ import annotations

import random
import re
import string
from pathlib import Path
from urllib.parse import urlparse

import requests

try:
    from gemini.ls_tasks_label_config import (
        _video_label_config,
        _image_label_config,
        _parse_csv_or_json_list,
        build_synthetic_provenance,
        parse_rectangle_labels_config,
        fetch_project_label_config,
        build_label_normalizer,
    )
    from gemini.ls_tasks_minio import (
        DEFAULT_PRESIGN_EXPIRES,
        find_or_create_project,
        register_webhook,
        fetch_existing_task_stems,
        generate_presigned_url,
        list_clip_keys,
        list_event_json_keys,
        list_sam3_json_keys,
        read_json_from_minio,
    )
    from gemini.ls_task_gate import GateConfig, GateStats
    from gemini.ls_task_gate import decide as gate_decide
except ModuleNotFoundError:
    from ls_tasks_label_config import (  # type: ignore[no-redef]
        _video_label_config,
        _image_label_config,
        _parse_csv_or_json_list,
        build_synthetic_provenance,
        parse_rectangle_labels_config,
        fetch_project_label_config,
        build_label_normalizer,
    )
    from ls_tasks_minio import (  # type: ignore[no-redef]
        DEFAULT_PRESIGN_EXPIRES,
        find_or_create_project,
        register_webhook,
        fetch_existing_task_stems,
        generate_presigned_url,
        list_clip_keys,
        list_event_json_keys,
        list_sam3_json_keys,
        read_json_from_minio,
    )
    from ls_task_gate import GateConfig, GateStats  # type: ignore[no-redef]
    from ls_task_gate import decide as gate_decide  # type: ignore[no-redef]


FROM_NAME = "videoLabels"
TO_NAME = "video"
_CLIP_PATTERN = re.compile(r"^(.+)_(\d{8})_(\d{8})$")


# ---------------------------------------------------------------------------
# 공통 헬퍼
# ---------------------------------------------------------------------------


def _rand_id(n: int = 10) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def extract_folder_name(prefix: str) -> str:
    """prefix에서 project name으로 쓸 폴더명 추출 (마지막 non-empty 컴포넌트)."""
    parts = [p for p in prefix.rstrip("/").split("/") if p]
    return parts[-1] if parts else prefix


# ---------------------------------------------------------------------------
# Image task 생성 (data.image)
# ---------------------------------------------------------------------------


def create_image_task(
    ls_url: str,
    headers: dict,
    project_id: int,
    image_url: str,
    folder: str,
    extra: dict | None = None,
) -> dict:
    """extra 는 `data` 에 함께 실을 추가 필드 (합성 배치 provenance 등). 기본 None = 기존 payload 그대로.

    미디어/폴더 키를 나중에 덮어써서 extra 가 `image`/`folder` 를 침범하지 못하게 한다.
    """
    data = dict(extra or {})
    data.update({"image": image_url, "folder": folder})
    resp = requests.post(
        f"{ls_url}/api/tasks/",
        headers={**headers, "Content-Type": "application/json"},
        json={
            "project": project_id,
            "data": data,
        },
    )
    resp.raise_for_status()
    return resp.json()


def _get_review_state_fns():
    try:
        from gemini.ls_tasks import load_review_state, update_review_state
    except ModuleNotFoundError:
        from ls_tasks import load_review_state, update_review_state  # type: ignore[no-redef]
    return load_review_state, update_review_state


# ---------------------------------------------------------------------------
# LS task helpers
# ---------------------------------------------------------------------------


def create_task(
    ls_url: str,
    headers: dict,
    project_id: int,
    video_url: str,
    folder: str,
    extra: dict | None = None,
) -> dict:
    """extra 는 `data` 에 함께 실을 추가 필드 (합성 배치 provenance 등). 기본 None = 기존 payload 그대로."""
    data = dict(extra or {})
    data.update({"video": video_url, "folder": folder})
    resp = requests.post(
        f"{ls_url}/api/tasks/",
        headers={**headers, "Content-Type": "application/json"},
        json={
            "project": project_id,
            "data": data,
        },
    )
    resp.raise_for_status()
    return resp.json()


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
# Prediction 생성
# ---------------------------------------------------------------------------


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
        result.append(
            {
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
        )
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
    class_thresholds: dict[str, float] | None = None,
) -> list[dict]:
    """SAM3 COCO per-image JSON → LS RectangleLabels result (percentage).

    정규화 우선순위: normalizer(synonym→canonical) > label_map(단순 rename).
    normalizer 가 주어지면 매핑 안 되는 카테고리는 drop (dispatch 범위 밖 탐지 결과는 리뷰에서 제외).
    normalizer=None 이면 allowed_labels 필터만 적용 (legacy).
    이미지 크기는 coco['images'][0]의 width/height 사용.

    class_thresholds (2026-06-04): SAM3 raw phrase → 최소 score. SAM3 server-side
    threshold 가 일부 phrase 에만 적용된 경우(미등록 phrase 는 base 0.25 만 적용) 후처리
    에서 한 번 더 filter. attach-predictions 가 기존 COCO 의 노이즈 박스 제거하는 데
    사용. None 이면 score-based filtering 없음.
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
    cls_thr = class_thresholds or {}
    result: list[dict] = []
    for ann in annotations:
        bbox = ann.get("bbox")
        if not bbox or len(bbox) < 4:
            continue
        raw = cat_by_id.get(int(ann.get("category_id", -1)), "")
        label = lmap.get(raw, raw).strip().lower()
        # post-hoc per-phrase threshold filter (raw 기준, normalize 전).
        score = float(ann.get("score") or 0.0)
        if cls_thr:
            phrase_threshold = cls_thr.get(raw.lower(), cls_thr.get("__default__", 0.0))
            if score < phrase_threshold:
                continue
        if normalizer is not None:
            canonical = normalizer.get(label)
            if not canonical:
                continue
            label = canonical
        elif label not in allowed_labels:
            continue
        x, y, w, h = (float(v) for v in bbox[:4])
        result.append(
            {
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
            }
        )
    return result


# ---------------------------------------------------------------------------
# project 헬퍼
# ---------------------------------------------------------------------------


def _resolve_category_targets(args) -> list[str]:
    """--categories 인자를 파싱. 비어있으면 []."""
    raw = getattr(args, "categories", "") or ""
    return _parse_csv_or_json_list(raw)


def _synthetic_provenance(args, target_cats: list[str]) -> dict[str, str]:
    """합성 배치 task 의 `data` 에 실을 출처 필드 — 엔진/batch/의도 이벤트.

    엔진·batch_id 는 sensor 가 `raw_files`(정본) 에서 읽어 CLI 로 넘긴다. 의도 이벤트는
    dispatch 가 요구한 카테고리(`--categories`) 그대로다 — 합성은 그 클래스를 채우려고 만든
    것이므로 "요청한 이벤트" 와 같은 값이다.
    """
    return build_synthetic_provenance(
        getattr(args, "genai_engine", "") or "",
        getattr(args, "genai_batch_id", "") or "",
        target_cats,
    )


def _build_project_title(folder_name: str, mode: str, suffix: str) -> str:
    """`<folder>_<mode>_<suffix>`. suffix 는 dispatch.requested_at 을 YYMMDD_HHMM 으로 포맷한 값."""
    base = f"{folder_name}_{mode}"
    return f"{base}_{suffix}" if suffix else base


def _ensure_dated_project(ls_url: str, auth_headers: dict, title: str, label_config: str) -> int:
    pid, is_new = find_or_create_project(ls_url, auth_headers, title, label_config=label_config)
    if is_new:
        try:
            register_webhook(ls_url, auth_headers, pid)
        except Exception as exc:
            print(f"[WARN] webhook 등록 실패 (수동 등록 필요): {exc}")
    return pid


# ---------------------------------------------------------------------------
# _create_video / _create_image
# ---------------------------------------------------------------------------


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
    # 합성 여부는 label_config(프로젝트 생성 시점에 확정) 과 task data 양쪽에 필요하므로
    # 프로젝트를 만들기 **전에** 읽는다.
    is_synthetic = bool(getattr(args, "synthetic", False))
    provenance = _synthetic_provenance(args, target_cats) if is_synthetic else {}

    print(f"[INFO] video mode: prefix={prefix}, project={title}, categories={target_cats or '(none)'}")
    if provenance:
        print(f"[INFO] 합성 provenance: {provenance['provenance']}")

    project_id = _ensure_dated_project(
        args.ls_url, auth_headers, title, _video_label_config(target_cats, synthetic=is_synthetic)
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

    gate_cfg = GateConfig.from_env()
    gate_stats = GateStats()
    print(f"[INFO] {gate_cfg.describe()}{' — 합성 배치' if is_synthetic else ''}")

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

        # 게이트: 열거 기준이 events JSON 이 아니라 vlm-raw 영상 키라, 이벤트 0건 영상도
        # 여기까지 온다. 실측 81.2%가 그 경우였다.
        decision = gate_decide(len(events), key, gate_cfg, is_synthetic)
        gate_stats.record(decision)
        if not decision.send:
            continue

        try:
            video_url = generate_presigned_url(minio, args.bucket, key, DEFAULT_PRESIGN_EXPIRES)
            rel = key[len(prefix) :].lstrip("/") if key.startswith(prefix) else key
            subfolder = rel.split("/", 1)[0] if "/" in rel else ""
            task_folder = subfolder or folder_name
            task = create_task(args.ls_url, auth_headers, project_id, video_url, task_folder, extra=provenance)
            task_id = task["id"]

            pred_count = 0
            if events:
                ls_result = gemini_events_to_ls_result(
                    events,
                    args.fps,
                    clip_start_sec=0.0,
                    clip_end_sec=float("inf"),
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
        _, _update_review_state = _get_review_state_fns()
        _update_review_state(project_id, title, collected_label_keys, created)
        print(f"[INFO] review state 저장 완료 ({args.ls_url})")

    print(f"\n[DONE] video mode: 생성 {created} / 스킵(기존) {skipped} / 오류 {error}")
    print(f"[GATE] {gate_stats.summary()}")
    # 호출부(defs/ls/sensor.py)가 파싱하는 기계 판독 줄. 이게 없으면 "0건 생성"과 "정상 생성"을
    # 구분할 수 없어 job SUCCESS + ls_task_status='created' + LS task 0건 이 동시에 성립한다
    # (2026-09-21 실측).
    print(f"[RESULT] mode=video created={created} skipped={skipped} " f"error={error} gated_out={gate_stats.gated_out}")


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
    # video mode 와 동일 — 프로젝트 생성 전에 합성 여부를 확정해야 label_config 에 반영된다.
    is_synthetic = bool(getattr(args, "synthetic", False))
    provenance = _synthetic_provenance(args, target_cats) if is_synthetic else {}

    print(f"[INFO] image mode: prefix={prefix}, project={title}, categories={target_cats}")
    if provenance:
        print(f"[INFO] 합성 provenance: {provenance['provenance']}")

    project_id = _ensure_dated_project(
        args.ls_url, auth_headers, title, _image_label_config(target_cats, synthetic=is_synthetic)
    )

    existing = fetch_existing_task_image_stems(args.ls_url, auth_headers, project_id)
    json_index = list_sam3_json_keys(minio, args.label_bucket, prefix)
    print(f"[INFO] sam3_segmentations JSON {len(json_index)}개 인덱싱")

    normalizer = build_label_normalizer(target_cats)
    allowed_labels = set(normalizer.values())
    print(f"[INFO] synonym normalizer: {len(normalizer)}개 매핑 → {sorted(allowed_labels)}\n")

    # 2026-06-04: per-phrase score threshold filter (yolo_thresholds dict 의 값 사용).
    # SAM3 server 가 미등록 phrase 에 base 0.25 만 적용해서 노이즈 박스 통과한 경우
    # client-side 추가 filter. attach-predictions 와 동일 동작.
    try:
        import sys as _sys

        _sys.path.insert(0, "/src")
        from vlm_pipeline.lib.yolo_thresholds import YOLO_CLASS_CONFIDENCE_THRESHOLDS

        class_thresholds = dict(YOLO_CLASS_CONFIDENCE_THRESHOLDS)
        print(f"[INFO] class_thresholds: {len(class_thresholds)} entries (per-phrase score filter)")
    except Exception as exc:
        class_thresholds = None
        print(f"[WARN] class_thresholds 로드 실패 — score filter skip: {exc}")

    processed_bucket = getattr(args, "processed_bucket", None) or "vlm-processed"
    created = skipped = error = dropped_no_image = 0

    gate_cfg = GateConfig.from_env()
    gate_stats = GateStats()
    print(f"[INFO] {gate_cfg.describe()}{' — 합성 배치' if is_synthetic else ''}")

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
            # 게이트: 박스 수를 알아야 판정할 수 있으므로 태스크 생성 **전에** 변환한다.
            # (예전에는 create_image_task 뒤에 있어서, 임계값으로 박스가 전부 떨어져도
            #  빈 태스크가 이미 만들어진 뒤였다 — 실측 54.9%가 그 경우였다.)
            ls_result = sam3_coco_to_ls_rectangles(
                coco,
                allowed_labels,
                "imageLabels",
                "image",
                label_map=None,
                normalizer=normalizer,
                class_thresholds=class_thresholds,
            )
            decision = gate_decide(len(ls_result or []), json_key, gate_cfg, is_synthetic)
            gate_stats.record(decision)
            if not decision.send:
                continue

            image_url = generate_presigned_url(minio, processed_bucket, image_key, DEFAULT_PRESIGN_EXPIRES)
            rel = image_key[len(prefix) :].lstrip("/") if image_key.startswith(prefix) else image_key
            subfolder = rel.split("/", 1)[0] if "/" in rel else ""
            task_folder = subfolder or folder_name
            task = create_image_task(args.ls_url, auth_headers, project_id, image_url, task_folder, extra=provenance)
            task_id = task["id"]

            pred_count = 0
            if ls_result:
                create_prediction(args.ls_url, auth_headers, task_id, ls_result)
                pred_count = len(ls_result)

            why = "" if decision.reason == "has_result" else f" [{decision.reason}]"
            print(f"[CREATED]  task {task_id} ← {stem} (bbox {pred_count}){why}")
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
        _, _update_review_state = _get_review_state_fns()
        _update_review_state(project_id, title, list(json_index.values()), created + skipped)
        print(f"[INFO] review state 저장 완료 ({args.ls_url})")

    print(
        f"\n[DONE] image mode: 생성 {created} / 스킵(기존) {skipped} / "
        f"이미지경로 누락 {dropped_no_image} / 오류 {error}"
    )
    print(f"[GATE] {gate_stats.summary()}")
    print(
        f"[RESULT] mode=image created={created} skipped={skipped} "
        f"error={error + dropped_no_image} gated_out={gate_stats.gated_out}"
    )


# ---------------------------------------------------------------------------
# cmd_create
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

    # 2026-06-04 fix: SAM3 자연어 prompt (예: "fallen person") → canonical label
    # (예: "falldown") 정규화. allowed_labels (LS label_config) 만 필터하면 SAM3 phrase
    # 가 그대로 drop 됨. build_label_normalizer 가 CATEGORY_SYNONYMS dict 로
    # synonym → canonical 매핑 생성 → sam3_coco_to_ls_rectangles 가 매핑 후 박스 keep.
    normalizer = build_label_normalizer(list(allowed))
    print(f"[INFO] normalizer entries: {len(normalizer)}")

    # post-hoc per-phrase threshold filter — SAM3 server 가 미등록 phrase 에 base 0.25
    # 만 적용해서 노이즈 박스가 통과한 경우 attach 시 한 번 더 거름.
    # 사용자 batch e00879ff-b04 케이스: "person on the ground" 30+ boxes (0.5-0.7) → drop.
    try:
        import sys

        sys.path.insert(0, "/src")
        from vlm_pipeline.lib.yolo_thresholds import YOLO_CLASS_CONFIDENCE_THRESHOLDS

        class_thresholds = dict(YOLO_CLASS_CONFIDENCE_THRESHOLDS)
        print(f"[INFO] class_thresholds loaded: {len(class_thresholds)} entries")
    except Exception as exc:
        class_thresholds = None
        print(f"[WARN] class_thresholds import 실패 — score filter skip: {exc}")

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
            ls_result = sam3_coco_to_ls_rectangles(
                coco,
                allowed,
                from_name,
                to_name,
                label_map=label_map,
                normalizer=normalizer,
                class_thresholds=class_thresholds,
            )
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
    mode = args.mode if args.mode != "auto" else _detect_task_mode(args.ls_url, auth_headers, project_id)
    if mode == "image":
        _attach_sam3_images(args, minio, auth_headers, project_id)
    else:
        _attach_video_events(args, minio, auth_headers, project_id)
