"""이미지 1-pass 로딩 — read → checksum + verify + image_metadata 동시 추출.

Layer 1: 순수 Python, Dagster 의존 없음.
★ NFS에서 파일 1회만 읽어 모든 메타 추출 (3회→1회 최적화).
"""

from datetime import datetime
from io import BytesIO
from pathlib import Path

from PIL import Image

from .checksum import sha256_bytes

try:
    from pillow_heif import register_heif_opener

    register_heif_opener()
except Exception:  # noqa: BLE001
    pass


def load_image_once(path: str | Path) -> dict:
    """파일을 1회 읽어서 checksum + verify + image_metadata 모두 추출.

    Returns:
        dict with keys:
          - file_size: int
          - checksum: str (SHA-256)
          - file_bytes: bytes (MinIO 업로드용 재사용)
          - image_metadata: dict (width, height, color_mode, bit_depth, codec, has_alpha, orientation)
    """
    file_path = Path(path)

    # ① NFS에서 1회만 읽기
    file_bytes = file_path.read_bytes()

    # ② checksum (메모리에서 계산)
    checksum = sha256_bytes(file_bytes)

    # ③ 이미지 검증 (메모리에서)
    img = Image.open(BytesIO(file_bytes))
    img.verify()

    # ④ 메타데이터 추출 (verify 후 재오픈 — PIL 제약)
    img = Image.open(BytesIO(file_bytes))
    width, height = img.size
    color_mode = img.mode
    codec = (img.format or file_path.suffix.lstrip(".")).lower()
    has_alpha = color_mode in {"RGBA", "LA"} or ("transparency" in getattr(img, "info", {}))
    bit_depth = 16 if color_mode.endswith("16") else 8

    # ⑤ EXIF Orientation (없으면 1=정상)
    orientation = 1
    try:
        exif = img.getexif()
        if exif:
            orientation = int(exif.get(274, 1))  # 274 = Orientation tag
    except Exception:  # noqa: BLE001
        pass

    return {
        "file_size": len(file_bytes),
        "checksum": checksum,
        "file_bytes": file_bytes,
        "image_metadata": {
            "width": width,
            "height": height,
            "color_mode": color_mode,
            "bit_depth": bit_depth,
            "codec": codec,
            "has_alpha": bool(has_alpha),
            "orientation": orientation,
            "extracted_at": datetime.now(),
        },
    }
