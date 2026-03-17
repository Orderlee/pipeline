"""인입 데이터 유효성 검증 — 파일 존재, 크기, 확장자, 읽기 가능.

Layer 1: 순수 Python, Dagster 의존 없음.
"""

import os
from dataclasses import dataclass
from pathlib import Path

ALLOWED_EXTENSIONS: set[str] = {
    # 이미지
    ".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".webp", ".heic",
    # 비디오
    ".mp4", ".avi", ".mov", ".mkv", ".webm", ".mpeg", ".mpg", ".m4v", ".mts", ".m2ts",
}

IMAGE_EXTENSIONS: set[str] = {
    ".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".webp", ".heic",
}

VIDEO_EXTENSIONS: set[str] = {
    ".mp4", ".avi", ".mov", ".mkv", ".webm", ".mpeg", ".mpg", ".m4v", ".mts", ".m2ts",
}


@dataclass
class ValidationResult:
    """검증 결과."""

    ok: bool
    level: str  # PASS | WARN | FAIL
    message: str = ""


def validate_incoming(path: str | Path) -> ValidationResult:
    """인입 파일 유효성 검증.

    검증 항목:
      V1. 파일 존재 여부
      V2. 파일 크기 > 0
      V3. 확장자 허용 목록 (미디어만, 라벨 JSON 제외)
      V4. 파일 읽기 가능 여부
      V5. 파일명 길이 ≤ 255
      V6. 경로 깊이 ≤ 10
    """
    file_path = Path(path)

    # V1: 파일 존재
    if not file_path.exists() or not file_path.is_file():
        return ValidationResult(False, "FAIL", "file_missing")

    # V2: 파일 크기 > 0
    try:
        stat = file_path.stat()
    except OSError:
        return ValidationResult(False, "FAIL", "stat_failed")

    if stat.st_size <= 0:
        return ValidationResult(False, "FAIL", "empty_file")

    # V3: 확장자 허용 목록
    ext = file_path.suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        return ValidationResult(False, "FAIL", f"unsupported_ext:{ext}")

    # V4: 읽기 가능
    if not os.access(str(file_path), os.R_OK):
        return ValidationResult(False, "FAIL", "not_readable")

    # V5: 파일명 길이
    if len(file_path.name) > 255:
        return ValidationResult(True, "WARN", "name_too_long")

    # V6: 경로 깊이
    if len(file_path.parts) > 10:
        return ValidationResult(True, "WARN", "path_too_deep")

    # HEIC 변환 필요 경고
    if ext == ".heic":
        return ValidationResult(True, "WARN", "heic_requires_conversion")

    return ValidationResult(True, "PASS", "")


def detect_media_type(path: str | Path) -> str:
    """확장자 기반 미디어 타입 판별."""
    ext = Path(path).suffix.lower()
    if ext in IMAGE_EXTENSIONS:
        return "image"
    if ext in VIDEO_EXTENSIONS:
        return "video"
    return "unknown"
