"""프레임 → 임베딩 row 빌드 및 sensor 순수 헬퍼. per-file fail-forward."""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


def build_frame_embedding_rows(
    pending: list[dict[str, Any]],
    *,
    minio,
    client,
    model_name: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    """pending 프레임 목록을 임베딩 row dict 로 변환.

    각 프레임 실패는 failed 리스트에 image_id 추가 후 계속 (per-file fail-forward).
    Returns (rows, failed_image_ids).
    """
    rows: list[dict[str, Any]] = []
    failed: list[str] = []
    for p in pending:
        image_id = p["image_id"]
        try:
            data = minio.download(p["image_bucket"], p["image_key"])
            vec = client.embed(data)
            if len(vec) != 1024:
                raise ValueError(f"unexpected dim {len(vec)}")
            rows.append(
                {
                    "embedding_id": f"frame|{image_id}|{model_name}",
                    "entity_type": "frame",
                    "entity_id": image_id,
                    "image_id": image_id,
                    "model_name": model_name,
                    "dim": len(vec),
                    "embedding": vec,
                    "source_bucket": p["image_bucket"],
                    "source_key": p["image_key"],
                    "bbox": None,
                }
            )
        except Exception as exc:
            log.warning("frame embed failed image_id=%s: %s", image_id, exc)
            failed.append(image_id)
    return rows, failed


def build_caption_embedding_rows(
    pending: list[dict[str, Any]],
    *,
    client,
    model_name: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    """pending caption 목록을 임베딩 row dict 로 변환.

    각 caption 실패는 failed 리스트에 label_id 추가 후 계속 (per-item fail-forward).
    Returns (rows, failed_label_ids).
    """
    rows: list[dict[str, Any]] = []
    failed: list[str] = []
    for p in pending:
        label_id = p["label_id"]
        try:
            vec = client.embed_text(p["caption_text"])
            if len(vec) != 1024:
                raise ValueError(f"unexpected dim {len(vec)}")
            rows.append(
                {
                    "embedding_id": f"caption|{label_id}|{model_name}",
                    "entity_type": "caption",
                    "entity_id": label_id,
                    "image_id": None,
                    "asset_id": p.get("asset_id"),
                    "model_name": model_name,
                    "dim": len(vec),
                    "embedding": vec,
                    "source_bucket": None,
                    "source_key": None,
                    "bbox": None,
                    "text_content": p["caption_text"],
                }
            )
        except Exception as exc:
            log.warning("caption embed failed label_id=%s: %s", label_id, exc)
            failed.append(label_id)
    return rows, failed


def _parse_seq(cursor: str | None) -> int:
    """커서 'count=N|seq=M' 에서 monotonic seq 추출 (없으면 0)."""
    if not cursor:
        return 0
    for part in cursor.split("|"):
        if part.startswith("seq="):
            try:
                return int(part[4:])
            except ValueError:
                return 0
    return 0


def _encode_cursor(count: int, seq: int) -> str:
    return f"count={count}|seq={seq}"


def decide_frame_embedding_run(
    *,
    backlog_count: int,
    prev_cursor: str | None,
    in_flight: bool,
    limit: int,
    model_name: str,
    image_roles: list[str] | None = None,
) -> tuple[dict | None, str, str | None]:
    """프레임 임베딩 sensor 결정 (순수 함수, dagster 비의존 → 단위 테스트 가능).

    Returns (run_config_or_None, new_cursor, run_key_or_None):
      - backlog<=0  → skip (seq 유지)
      - in_flight   → skip (실행 중 → 중복 run 방지, seq 유지)
      - 그 외       → run (seq+1, 고유 run_key)

    핵심: backlog 가 줄지 않아도(이전 run 실패/0건) in_flight 만 아니면 매번 **새 seq → 새 run_key**
    로 재시도한다. count-only 커서가 backlog 불변 시 재시도를 영구 억제하던 버그(Codex HIGH) 해결.
    """
    prev_seq = _parse_seq(prev_cursor)
    if backlog_count <= 0:
        return None, _encode_cursor(0, prev_seq), None
    if in_flight:
        return None, _encode_cursor(backlog_count, prev_seq), None
    seq = prev_seq + 1
    cfg: dict[str, Any] = {"limit": limit, "model_name": model_name}
    if image_roles:
        cfg["image_roles"] = list(image_roles)
    run_config = {"ops": {"frame_embedding": {"config": cfg}}}
    return run_config, _encode_cursor(backlog_count, seq), f"frame-embed-{seq}"
