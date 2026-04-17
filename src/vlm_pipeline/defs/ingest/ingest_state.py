"""RawIngestState — raw_ingest 파이프라인 실행 전반의 mutable state."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class RawIngestState:
    manifest_path: str | None
    request_id: str | None
    folder_name: str | None
    archive_only: bool
    stage: str = "entered"
    manifest: dict | None = None
    transfer_tool: str = ""
    archive_requested: bool = False
    archive_unit_dir_hint: Path | None = None
    archive_prepared_for_upload: bool = False
    ingest_rejections: list[dict] = field(default_factory=list)
    retry_candidates: list[dict] = field(default_factory=list)
    records: list[dict] = field(default_factory=list)
    uploaded: list[dict] = field(default_factory=list)
    archived: list[dict] = field(default_factory=list)
