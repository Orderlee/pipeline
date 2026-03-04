from datetime import datetime
from pathlib import Path

from PIL import Image

from .checksum import sha256sum

try:
    from pillow_heif import register_heif_opener

    register_heif_opener()
except Exception:  # noqa: BLE001
    pass


def load_image_once(path: str | Path) -> dict:
    """Read an image once and return checksum + metadata."""
    file_path = Path(path)
    checksum = sha256sum(file_path)
    stat = file_path.stat()

    with Image.open(file_path) as img:
        img.verify()

    with Image.open(file_path) as img:
        width, height = img.size
        mode = img.mode
        codec = (img.format or file_path.suffix.lstrip(".")).lower()
        has_alpha = mode in {"RGBA", "LA"} or ("transparency" in getattr(img, "info", {}))
        orientation = 1
        exif = None
        try:
            exif = img.getexif()
        except Exception:  # noqa: BLE001
            exif = None
        if exif:
            orientation = int(exif.get(274, 1))

    return {
        "file_size": stat.st_size,
        "checksum": checksum,
        "image_metadata": {
            "width": width,
            "height": height,
            "color_mode": mode,
            "bit_depth": 8,
            "codec": codec,
            "has_alpha": bool(has_alpha),
            "orientation": orientation,
            "extracted_at": datetime.now(),
        },
    }
