"""공통 환경변수·타입 변환 유틸리티 + dispatch outputs 정규화."""

from __future__ import annotations

import functools
import json
import logging
import os
import re
from collections.abc import Iterable, Mapping
from pathlib import Path, PurePosixPath

from vlm_pipeline.lib.sanitizer import sanitize_path_component

logger = logging.getLogger(__name__)


def as_int(value: object, default: int) -> int:
    """값을 int로 변환, 실패 시 default 반환."""
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def int_env(name: str, default: int, minimum: int = 0) -> int:
    """환경변수를 int로 읽되, minimum 이상을 보장."""
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = int(default)
    return max(minimum, value)


def float_env(name: str, default: float, minimum: float = 0.0) -> float:
    """환경변수를 float로 읽되, minimum 이상을 보장."""
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = float(default)
    return max(minimum, value)


def coerce_float(value: object, default: float = 0.0) -> float:
    """값을 float로 변환, 실패 시 default 반환."""
    if value in (None, ""):
        return default
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def bool_env(name: str, default: bool = False) -> bool:
    """환경변수를 bool로 읽는다. 미설정이면 default."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def default_postgres_dsn() -> str | None:
    """PostgreSQL DSN 기본값 (환경변수 우선).

    ``DATAOPS_POSTGRES_DSN`` 미설정이면 None 반환.
    형식: ``postgresql://user:pass@host:port/dbname``
    """
    raw = os.getenv("DATAOPS_POSTGRES_DSN", "").strip()
    return raw or None


IS_STAGING = bool_env("IS_STAGING", False)


def dispatch_folder_for_source_unit(tags: Mapping[str, str] | None) -> str | None:
    """staging dispatch run 태그에서 `raw_files.source_unit_name` 조회용 문자열.

    manifest의 원본 폴더명(`folder_name_original`)이 DB에 그대로 저장되므로,
    sanitize된 `folder_name`보다 이쪽을 우선한다.
    """
    if not tags:
        return None
    s = str(tags.get("folder_name_original") or tags.get("folder_name") or "").strip()
    return s or None


def dispatch_raw_key_prefix_folder(tags: Mapping[str, str] | None) -> str | None:
    """staging dispatch run 태그에서 `raw_key LIKE '<prefix>/%'` 용 첫 경로 세그먼트.

    INGEST의 `raw_key`는 `sanitize_path_component` 기준이므로, 태그의 sanitize된
    `folder_name`을 우선하고 없으면 원본을 동일 규칙으로 정규화한다.
    """
    if not tags:
        return None
    direct = str(tags.get("folder_name") or "").strip()
    if direct:
        return direct
    orig = str(tags.get("folder_name_original") or "").strip()
    if not orig:
        return None
    return sanitize_path_component(orig) or None


def storage_raw_key_prefix_from_source_unit(source_unit_name: str | None) -> str:
    """MinIO raw_key용 source unit prefix를 정규화한다.

    GCP auto-bootstrap source unit은 `gcp/<bucket>/...` 형태로 관리되지만,
    MinIO object key는 `gcp/`를 제거한 `<bucket>/...` 형태를 사용한다.
    나머지 source unit은 기존과 동일하게 전체 경로를 sanitize해서 유지한다.
    """
    raw = str(source_unit_name or "").strip().strip("/")
    if not raw:
        return ""

    parts = [sanitize_path_component(part) for part in PurePosixPath(raw).parts if part not in {"", ".", ".."}]
    if len(parts) >= 2 and parts[0] == "gcp":
        parts = parts[1:]

    return "/".join(part for part in parts if part)


# ── Dispatch outputs 분기 유틸 ──

# run_mode → outputs 자동 변환 매핑
_RUN_MODE_TO_OUTPUTS: dict[str, list[str]] = {
    "gemini": ["timestamp_video", "captioning_video"],
    "yolo": ["bbox"],
    "both": ["timestamp_video", "captioning_video", "bbox"],
}

_OUTPUT_NAME_ALIASES: dict[str, str] = {
    "timestamp": "timestamp_video",
    "timestamp_video": "timestamp_video",
    "captioning": "captioning_video",
    "captioning_video": "captioning_video",
    "captioning_image": "captioning_image",
    "classification": "classification_video",  # 기본은 video — image classification은 명시적으로 지정
    "image_classification": "classification_image",
    "classification_image": "classification_image",
    "video_classification": "classification_video",
    "classification_video": "classification_video",
    "bbox": "bbox",
    "skip": "skip",
}

# 유효한 dispatch runtime output 키 목록
VALID_OUTPUTS = frozenset(
    [
        "timestamp_video",
        "captioning_video",
        "captioning_image",
        "classification_video",
        "classification_image",
        "bbox",
    ]
)

VALID_LABELING_METHODS = frozenset([*VALID_OUTPUTS, "skip"])

YOLO_OUTPUTS = frozenset(
    [
        "bbox",
        "classification_image",
    ]
)

SAM3_OUTPUTS = frozenset(
    [
        "bbox",
        "segmentation",
    ]
)

CAPTIONING_OUTPUTS = frozenset(
    [
        "captioning_video",
        "captioning_image",
    ]
)

TIMESTAMP_OUTPUTS = frozenset(
    [
        "timestamp_video",
        "captioning_video",
        "captioning_image",
    ]
)

# output 간 의존성: key를 요청하면 value도 자동 포함
# (frame_extraction은 bbox 요청 시 내부 stage로만 추가, requested_outputs에는 미포함)
_OUTPUT_DEPENDENCIES: dict[str, list[str]] = {
    "captioning_video": ["timestamp_video"],
    "captioning_image": ["timestamp_video", "captioning_video"],
}

# 라벨 클래스 정본(ontology) — lib/ 기준 ../data/label_ontology.json
_LABEL_ONTOLOGY_PATH = Path(__file__).resolve().parent.parent / "data" / "label_ontology.json"


@functools.lru_cache(maxsize=1)
def load_label_ontology() -> dict[str, dict]:
    """라벨 클래스 정본(ontology)의 ``classes`` dict 반환.

    ``src/vlm_pipeline/data/label_ontology.json`` 을 ``__file__`` 기준 상대경로로 읽는다
    (컨테이너도 src/ 트리 구조가 보존된다). 파일 IO 는 lru_cache 로 1회만.

    ``schema_version`` / ``_comment`` 등 메타 키는 제외하고 클래스 맵만 돌려준다.
    """
    with _LABEL_ONTOLOGY_PATH.open(encoding="utf-8") as fp:
        payload = json.load(fp)
    return payload["classes"]


def _derive_category_to_classes() -> dict[str, list[str]]:
    """ontology → dispatch categories → SAM3 text prompt 매핑."""
    derived: dict[str, list[str]] = {}
    for name, spec in load_label_ontology().items():
        if not spec.get("dispatch_category"):
            continue
        phrases = list(spec.get("detect_phrases") or [])
        if not phrases:
            continue
        derived[name] = phrases
    return derived


# categories → classes 파생 (auto_labeling_unified_spec, staging spec flow)
#
# 2026-06-05: agent / spec 등 외부 dispatch 가 categories 만 보낼 때 SAM3 자연어 prompt 로
# expansion. SAM3.1 은 text-prompted segmentation 이라 합성어 / 추상어
# (예: "person_fallen", "violence") 매칭 못함 → 자연어 명사구 필수.
#
# ⚠ 이 맵을 손으로 고치지 말 것. 정본은 src/vlm_pipeline/data/label_ontology.json 단 하나이고
# 이 변수는 거기서 파생된다 (dispatch_category=true 이면서 detect_phrases 가 비지 않은 클래스).
# 다른 소비처(ls_tasks.CATEGORY_SYNONYMS, genai promote.html PRESETS,
# sql/migrations/postgres/022_label_ontology.sql)와의 일치는 CI parity test 가 강제한다.
CATEGORY_TO_CLASSES: dict[str, list[str]] = _derive_category_to_classes()


@functools.lru_cache(maxsize=1)
def _alias_to_canonical() -> dict[str, str]:
    """alias(lowercase) → canonical 역인덱스.

    ⚠️ canonical 이름 자신도 alias 로 등록된 클래스가 있고(정본 8개), 반대로 canonical 이
    다른 클래스의 alias 인 경우도 있다 — `smoking` 은 독립 canonical 이면서 `smoke` 의 alias 다
    (정본에 미해결 충돌로 기록됨). 그래서 이 맵은 alias 만 담고, 해석은 canonical 을 먼저 본다
    (resolve_to_canonical 참고). 020 계보 뷰 주석의 "canonical-first" 규칙과 동일하다.
    """
    index: dict[str, str] = {}
    for canonical, spec in load_label_ontology().items():
        for alias in spec.get("aliases") or []:
            index.setdefault(str(alias).strip().lower(), canonical)
    return index


def resolve_to_canonical(value: object) -> str | None:
    """임의의 라벨 문자열을 정본 canonical 로 해석한다. 모르면 None.

    None 이 곧 "정본이 모르는 값" 의 정의이고, observed_categories 원장에 기록할 대상이다.
    입력은 소문자·trim 만 하고 그 외 정규화는 하지 않는다 — 원문 보존은 호출부 책임이다.
    """
    rendered = str(value or "").strip().lower()
    if not rendered:
        return None
    if rendered in load_label_ontology():
        return rendered
    return _alias_to_canonical().get(rendered)


def derive_classes_from_categories(categories: list[str] | None) -> list[str]:
    """categories 배열을 받아 classes 배열 반환. 중복 제거, 순서 유지."""
    if not categories:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for cat in categories:
        cat = (cat or "").strip().lower()
        if not cat or cat in seen:
            continue
        classes = CATEGORY_TO_CLASSES.get(cat)
        if classes is None:
            # ontology 에 없는 카테고리는 그대로 SAM3 prompt 으로 나간다(라이브 dispatch 경로라
            # 예외를 던지지 않는다). 한글 카테고리 등이 조용히 통과한 이력이 있어 가시화만 한다.
            logger.warning(
                "derive_classes_from_categories: unknown category %r — label_ontology.json 에 없어 "
                "expansion 없이 그대로 사용함",
                cat,
            )
            classes = [cat]
        for c in classes:
            if c not in seen:
                seen.add(c)
                out.append(c)
    return out


def normalize_output_name(value: object) -> str:
    """출력 키 표기를 내부 canonical snake_case로 정규화."""
    rendered = str(value or "").strip().lower()
    if not rendered:
        return ""
    rendered = rendered.replace("-", "_").replace(" ", "_")
    rendered = re.sub(r"_+", "_", rendered)
    rendered = rendered.strip("_")
    return _OUTPUT_NAME_ALIASES.get(rendered, rendered)


def normalize_output_names(values: Iterable[object] | None) -> list[str]:
    """반복 가능한 출력 값을 canonical 목록으로 정규화."""
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        rendered = normalize_output_name(value)
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return normalized


def parse_outputs_raw(outputs_raw: str | None) -> list[str]:
    """쉼표 구분 outputs 문자열을 정규화된 output 목록으로 파싱."""
    if not outputs_raw:
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in str(outputs_raw).split(","):
        output = normalize_output_name(raw)
        if not output or output in seen:
            continue
        seen.add(output)
        normalized.append(output)
    return normalized


def is_dispatch_yolo_only_requested(tags) -> bool:
    """Staging dispatch run에서 YOLO 계열만 실행해야 하는지 판별."""
    if not tags or str(tags.get("spec_id") or "").strip():
        return False
    outputs_raw = tags.get("requested_outputs") or tags.get("outputs") or tags.get("labeling_method") or ""
    requested = set(parse_outputs_raw(outputs_raw))
    return bool(requested) and requested.issubset(YOLO_OUTPUTS)


def requested_outputs_require_timestamp(requested_outputs: Iterable[object] | None) -> bool:
    normalized = set(normalize_output_names(requested_outputs))
    return bool(normalized & TIMESTAMP_OUTPUTS)


def requested_outputs_require_caption_labels(requested_outputs: Iterable[object] | None) -> bool:
    normalized = set(normalize_output_names(requested_outputs))
    return bool(normalized & CAPTIONING_OUTPUTS)


def requested_outputs_require_frame_image_caption(requested_outputs: Iterable[object] | None) -> bool:
    normalized = set(normalize_output_names(requested_outputs))
    return "captioning_image" in normalized


def requested_outputs_require_raw_video_frames(requested_outputs: Iterable[object] | None) -> bool:
    normalized = set(normalize_output_names(requested_outputs))
    return bool(normalized & YOLO_OUTPUTS) and not bool(normalized & CAPTIONING_OUTPUTS)


def resolve_outputs(run_mode: str | None, outputs_raw: str | None) -> list[str]:
    """run_mode 또는 outputs 문자열에서 실행할 산출물 목록을 반환.

    우선순위: outputs_raw > run_mode (하위 호환)
    의존성 자동 해석: captioning_video → timestamp_video 자동 포함
    """
    if outputs_raw:
        parsed = parse_outputs_raw(outputs_raw)
        valid = [o for o in parsed if o in VALID_OUTPUTS]
        if not valid:
            valid = list(_RUN_MODE_TO_OUTPUTS.get("both", []))
    elif run_mode and run_mode in _RUN_MODE_TO_OUTPUTS:
        valid = list(_RUN_MODE_TO_OUTPUTS[run_mode])
    else:
        # 기본: 전체 실행
        valid = list(_RUN_MODE_TO_OUTPUTS["both"])

    # 의존성 자동 추가 (captioning → timestamp 등)
    resolved = list(valid)
    for output in valid:
        for dep in _OUTPUT_DEPENDENCIES.get(output, []):
            if dep not in resolved:
                resolved.append(dep)

    return resolved


def should_run_output(context, required_output: str) -> bool:
    """현재 run에서 특정 output이 요청되었는지 확인.

    dispatch_stage_job 이외의 일반 job에서 호출 시 항상 True 반환.
    """
    outputs_raw = (
        context.run.tags.get("requested_outputs")
        or context.run.tags.get("outputs")
        or context.run.tags.get("labeling_method")
    )
    run_mode = context.run.tags.get("run_mode")

    # dispatch가 아닌 일반 실행엔 태그가 없으므로 항상 실행
    if not outputs_raw and not run_mode:
        return True

    resolved = resolve_outputs(run_mode, outputs_raw)
    return normalize_output_name(required_output) in resolved


def should_run_any_output(context, required_outputs: set[str] | frozenset[str] | list[str] | tuple[str, ...]) -> bool:
    """현재 run에서 주어진 output 후보 중 하나라도 요청되었는지 확인."""
    outputs_raw = (
        context.run.tags.get("requested_outputs")
        or context.run.tags.get("outputs")
        or context.run.tags.get("labeling_method")
    )
    run_mode = context.run.tags.get("run_mode")

    if not outputs_raw and not run_mode:
        return True

    required = {normalize_output_name(output) for output in required_outputs if normalize_output_name(output)}
    if not required:
        return False

    resolved = set(resolve_outputs(run_mode, outputs_raw))
    return bool(required & resolved)
