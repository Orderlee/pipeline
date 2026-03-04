from dataclasses import dataclass
from pathlib import Path

ALLOWED_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".webp", ".heic",
    ".mp4", ".avi", ".mov", ".mkv", ".webm", ".mpeg", ".mpg", ".m4v", ".mts", ".m2ts",
}


@dataclass
class ValidationResult:
    ok: bool
    level: str  # PASS | WARN | FAIL
    message: str = ""


def validate_incoming(path: str | Path) -> ValidationResult:
    file_path = Path(path)

    if not file_path.exists() or not file_path.is_file():
        return ValidationResult(False, "FAIL", "file_missing")

    try:
        stat = file_path.stat()
    except OSError:
        return ValidationResult(False, "FAIL", "stat_failed")

    if stat.st_size <= 0:
        return ValidationResult(False, "FAIL", "empty_file")

    ext = file_path.suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        return ValidationResult(False, "FAIL", f"unsupported_ext:{ext}")

    if len(file_path.name) > 255:
        return ValidationResult(True, "WARN", "name_too_long")

    if len(file_path.parts) > 10:
        return ValidationResult(True, "WARN", "path_too_deep")

    if ext == ".heic":
        return ValidationResult(True, "WARN", "heic_requires_conversion")

    return ValidationResult(True, "PASS", "")
