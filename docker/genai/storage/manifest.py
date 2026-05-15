"""GenAI batch 의 NAS manifest 빌더.

생성되는 _manifest.json 형식 (sensor / ops_register 가 읽음):

  {
    "kind": "originals" | "outputs",
    "batch_id": "<uuid>",
    "engine": "kling" | ...,
    "output_media": "video" | "image",
    "transfer_tool": "genai_ui",
    "source_type": "genai_source" | "genai_output",
    "label_policy": "none",
    "genai_engine": "<engine>",
    "source_unit_name": "genai_<batch_id>",
    "items": [{"seq": int, "filename": str, "provider_job_id"?: str}, ...]
  }

ingest sensor 가 manifest 의 source_type/genai_engine/label_policy 를 raw_files INSERT
시 통과시킨다 (Phase 1 ops_register pass-through 참고).
"""

from __future__ import annotations

from typing import Any


def build_originals_manifest(
    batch_id: str,
    engine: str,
    output_media: str,
    items: list[dict[str, Any]],
) -> dict:
    return {
        "kind": "originals",
        "batch_id": batch_id,
        "engine": engine,
        "output_media": output_media,
        "transfer_tool": "genai_ui",
        "source_type": "genai_source",
        "label_policy": "none",
        "genai_engine": engine,
        "source_unit_name": f"genai_{batch_id}",
        "items": items,
    }


def build_outputs_manifest(
    batch_id: str,
    engine: str,
    output_media: str,
    items: list[dict[str, Any]],
) -> dict:
    return {
        "kind": "outputs",
        "batch_id": batch_id,
        "engine": engine,
        "output_media": output_media,
        "transfer_tool": "genai_ui",
        "source_type": "genai_output",
        "label_policy": "none",
        "genai_engine": engine,
        "source_unit_name": f"genai_{batch_id}",
        "items": items,
    }
