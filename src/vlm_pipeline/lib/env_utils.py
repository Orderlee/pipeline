"""공통 환경변수·타입 변환 유틸리티.

여러 모듈에서 중복 정의되던 as_int, int_env, bool_env, is_duckdb_lock_conflict를
한 곳에서 관리한다.
"""

from __future__ import annotations

import os
import re


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


def bool_env(name: str, default: bool = False) -> bool:
    """환경변수를 bool로 읽는다. 미설정이면 default."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def is_duckdb_lock_conflict(exc: BaseException) -> bool:
    """DuckDB 파일 lock 충돌 여부를 판별."""
    message = str(exc).lower()
    return (
        "could not set lock on file" in message
        and "conflicting lock is held" in message
    )


def extract_lock_owner_pid(text: str) -> str | None:
    """DuckDB lock 메시지에서 PID를 추출."""
    match = re.search(r"\(PID\s+(\d+)\)", str(text or ""), flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1)


def default_duckdb_path() -> str:
    """DuckDB 파일 경로 기본값 (환경변수 우선)."""
    return (
        os.getenv("DATAOPS_DUCKDB_PATH")
        or os.getenv("DUCKDB_PATH")
        or "/data/pipeline.duckdb"
    )

IS_STAGING = bool_env("IS_STAGING", False)


# ── Dispatch outputs 분기 유틸 ──

# run_mode → outputs 자동 변환 매핑
_RUN_MODE_TO_OUTPUTS: dict[str, list[str]] = {
    "gemini": ["timestamp", "captioning"],
    "yolo": ["bbox"],
    "both": ["timestamp", "captioning", "bbox"],
}

# 유효한 output 키 목록
VALID_OUTPUTS = frozenset([
    "timestamp",       # Gemini 이벤트 구간 추출
    "captioning",      # Gemini 캡셔닝 + clip 절단 + 프레임 추출
    "bbox",            # YOLO-World bbox detection
    "image_classification",  # (향후) 이미지 분류
    "video_classification",  # (향후) 비디오 분류
])

# output 간 의존성: key를 요청하면 value도 자동 포함
_OUTPUT_DEPENDENCIES: dict[str, list[str]] = {
    "captioning": ["timestamp"],  # captioning은 timestamp(이벤트 구간 추출)에 의존
}


def resolve_outputs(run_mode: str | None, outputs_raw: str | None) -> list[str]:
    """run_mode 또는 outputs 문자열에서 실행할 산출물 목록을 반환.

    우선순위: outputs_raw > run_mode (하위 호환)
    의존성 자동 해석: captioning → timestamp 자동 포함
    """
    if outputs_raw:
        parsed = [o.strip().lower() for o in outputs_raw.split(",") if o.strip()]
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
    outputs_raw = context.run.tags.get("outputs")
    run_mode = context.run.tags.get("run_mode")

    # dispatch가 아닌 일반 실행엔 태그가 없으므로 항상 실행
    if not outputs_raw and not run_mode:
        return True

    resolved = resolve_outputs(run_mode, outputs_raw)
    return required_output in resolved

