import hashlib
from pathlib import Path


def sha256sum(file_path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    """Calculate SHA-256 for a file path."""
    path = Path(file_path)
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()
