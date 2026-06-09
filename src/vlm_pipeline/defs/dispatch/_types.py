"""dispatch 도메인 type 정의 — service.py / builders.py 가 공유.

이 모듈은 service.py 와 builders.py 사이 circular import 회피를 위해 분리됨.
service.py 는 여기 정의된 모든 심볼을 re-export 한다 (호출자 호환).

L4: dagster 의존 (RunRequest). 운영 호스트 컨테이너 안에서만 import 가능.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from dagster import RunRequest

DispatchDuplicatePolicy = Literal["reject", "accept_noop"]
DispatchInFlightPolicy = Literal["reject", "defer"]
DispatchIngressStatus = Literal["run_request", "rejected", "duplicate_noop", "deferred"]


@dataclass(frozen=True)
class DispatchAppliedParams:
    max_frames_per_video: int | None
    jpeg_quality: int | None
    confidence_threshold: float | None
    iou_threshold: float | None


@dataclass(frozen=True)
class PreparedDispatchRequest:
    request_id: str
    folder_name: str
    incoming_folder_path: Path
    run_mode: str
    outputs_str: str
    labeling_method: list[str]
    categories: list[str]
    classes: list[str]
    image_profile: str
    requested_by: str | None
    requested_at: str | None
    archive_only: bool
    storage_outputs: str
    storage_labeling_method: str
    storage_categories: str
    storage_classes: str
    # GenAI promote 경로용 provenance pass-through. None 이면 manifest 에 안 들어가고
    # ops_register 는 DB DEFAULT(source_type='camera', label_policy='required') 적용.
    # ops_register.py:17-19 의 _VALID_* enum 으로 fail-loud validate.
    source_type: str | None = None
    genai_engine: str | None = None
    label_policy: str | None = None
    genai_batch_id: str | None = None
    items: list[dict] | None = None
    # archive 경로 override — archive_dir 기준 상대 경로. 예: "genai/abc123".
    # None 이면 기존 동작(archive_dir / folder_name). GenAI promote 가 평탄한 archive/
    # 루트 대신 archive/genai/<batch_id>/ 형태로 묶으려고 추가됨 (2026-05-29).
    archive_path: str | None = None
    # category → description (자연어 설명) 매핑. Gemini prompt 에 inject 되어 검출 정확도
    # 향상. promote 폼의 hybrid preset 이 자동 채움. 빈 dict / 미입력 = 기존 prompt 동작
    # (regression 0). 2026-06-02 등록.
    gemini_descriptions: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class DispatchIngressRequest:
    payload: Mapping[str, Any]
    fallback_request_id: str
    duplicate_policy: DispatchDuplicatePolicy
    in_flight_policy: DispatchInFlightPolicy


@dataclass(frozen=True)
class DispatchIngressResult:
    status: DispatchIngressStatus
    request_id: str
    reason: str
    prepared: PreparedDispatchRequest | None = None
    run_request: RunRequest | None = None
