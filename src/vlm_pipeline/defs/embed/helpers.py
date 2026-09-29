"""프레임/video → 임베딩 row 빌드 및 sensor 순수 헬퍼. per-file fail-forward."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable

from vlm_pipeline.lib.gpu_lease import (
    GPU0_COMFY,
    GpuLease,
    describe_lease,
    is_lease_blocking,
    lease_from_pg_row,
    seconds_until_free,
)

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
    translate: Callable[[list[str]], list[str]] | None = None,
) -> tuple[list[dict[str, Any]], list[str], int, int]:
    """pending caption 목록을 임베딩 row dict 로 변환.

    영문 텍스트로 임베딩하고 text_content 에는 원문(한국어)을 남긴다 — PE-Core-L14-336
    텍스트 인코더가 영어 중심이라 ko 원문은 cross-modal 정렬이 거의 0 이기 때문이다.

    영문의 출처는 두 가지이고 **DB 에 저장된 것을 우선한다**:
      1. `caption_text_en` — Gemini 가 애초에 반환한 영문(migration 025). 재번역하지 않는다.
      2. `translate` — 1번이 없는 구 행에 대한 fallback. batch Gemini 호출이라 비싸므로
         저장된 영문이 없는 행만 모아서 한 번 호출한다.

    translate 는 optional callable list[str]→list[str].
    Per-item fail-forward: a single embed failure adds to failed list and continues.
    Returns (rows, failed_label_ids, translated_count, fallback_count).
    translated_count = 영문으로 임베딩된 caption 수 (저장된 영문 + 번역 성공분).
    fallback_count   = 원문(한국어)으로 임베딩된 caption 수 (영문도 없고 번역도 못 한 경우).
    """
    rows: list[dict[str, Any]] = []
    failed: list[str] = []

    original_texts = [p["caption_text"] for p in pending]
    stored_en = [str(p.get("caption_text_en") or "").strip() for p in pending]

    # 저장된 영문이 있으면 그대로 쓰고, 없는 것만 번역 대상으로 모은다.
    embed_texts = list(original_texts)
    english_flags = [False] * len(pending)
    for i, stored in enumerate(stored_en):
        if stored:
            embed_texts[i] = stored
            english_flags[i] = True

    todo = [i for i, stored in enumerate(stored_en) if not stored]
    if translate is not None and todo:
        translated: list[str] | None
        try:
            translated = translate([original_texts[i] for i in todo])
        except Exception as exc:
            log.warning("caption batch translate failed (%s), falling back to originals", exc)
            translated = None
        if translated is not None and len(translated) != len(todo):
            log.warning(
                "translate returned %d items for %d inputs, falling back to originals",
                len(translated),
                len(todo),
            )
            translated = None
        if translated is not None:
            for i, text in zip(todo, translated):
                embed_texts[i] = text
                english_flags[i] = text != original_texts[i]

    translated_count = 0
    fallback_count = 0

    for p, embed_text, is_english in zip(pending, embed_texts, english_flags):
        label_id = p["label_id"]
        try:
            vec = client.embed_text(embed_text)
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
            if is_english:
                translated_count += 1
            else:
                fallback_count += 1
        except Exception as exc:
            log.warning("caption embed failed label_id=%s: %s", label_id, exc)
            failed.append(label_id)
    return rows, failed, translated_count, fallback_count


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


def decide_video_embedding_run(
    *,
    backlog_count: int,
    prev_cursor: str | None,
    in_flight: bool,
    limit: int,
    video_model_name: str,
    frame_model_name: str,
    video_roles: list[str] | None = None,
) -> tuple[dict | None, str, str | None]:
    """Video 임베딩 sensor 결정 (순수 함수, dagster 비의존 → 단위 테스트 가능).

    Returns (run_config_or_None, new_cursor, run_key_or_None):
      - backlog<=0  → skip (seq 유지)
      - in_flight   → skip (실행 중 → 중복 run 방지, seq 유지)
      - 그 외       → run (seq+1, 고유 run_key)

    decide_frame_embedding_run 과 동일한 monotonic-seq 전략으로 backlog 불변 시에도 재시도 보장.
    """
    prev_seq = _parse_seq(prev_cursor)
    if backlog_count <= 0:
        return None, _encode_cursor(0, prev_seq), None
    if in_flight:
        return None, _encode_cursor(backlog_count, prev_seq), None
    seq = prev_seq + 1
    cfg: dict[str, Any] = {
        "limit": limit,
        "video_model_name": video_model_name,
        "frame_model_name": frame_model_name,
    }
    if video_roles:
        cfg["video_roles"] = list(video_roles)
    run_config = {"ops": {"video_embedding": {"config": cfg}}}
    return run_config, _encode_cursor(backlog_count, seq), f"video-embed-{seq}"


# ----------------------------------------------------------------------
# GPU0 lease gate — comfy(ComfyUI) 생성과 임베딩 서빙의 상호배제 (읽기 전용)
# ----------------------------------------------------------------------
def read_blocking_gpu0_lease(db, *, now_ts: float | None = None) -> GpuLease | None:
    """comfy 가 지금 GPU0 를 들고 있으면 그 lease, 아니면 None.

    **fail-open**: 조회가 실패하면(030 미적용 staging, DB 순단, 권한 등) None 을 돌려
    호출부가 그냥 진행하게 둔다. 이 lease 는 안전장치가 아니라 **진단 보조**다 — 실제
    GPU0 보호는 comfy 쪽 `_prepare_gpu()` 의 VRAM 하한과 embedding-service 의 정비
    게이트가 한다. 여기서 fail-closed 로 잠그면 DB 딸꾹질 한 번에 임베딩이 통째로
    멈추고, 그것이 `lib/gpu_lease.py` 가 막으려는 바로 그 부류의 사고다.
    """
    try:
        row = db.get_generation_gpu_lease(GPU0_COMFY)
    except Exception as exc:  # noqa: BLE001 — 의도적 fail-open (위 docstring)
        # warning 이어야 한다. debug 로 두면 조회가 영구히 깨져도(030 롤백, 스키마 드리프트,
        # 쿼리 오타) 게이트가 **조용히** baseline 동작으로 퇴화하고 아무도 모른다 —
        # 이 레포가 반복해 밟은 "부재에 기댄 안전" 함정이다. asset run 당 1회라 스팸 아님.
        log.warning("gpu0 lease lookup failed (%s) — gate disabled for this run, proceeding", exc)
        return None
    lease = lease_from_pg_row(row, resource=GPU0_COMFY)
    now = time.time() if now_ts is None else now_ts
    return lease if is_lease_blocking(lease, now_ts=now) else None


def await_embedding_gpu(db, client, *, now_ts: float | None = None) -> tuple[bool, str | None]:
    """임베딩 asset 이 지금 진행해도 되는지 판정.

    Returns:
        ``(True, None)``  — 서비스 준비 완료, 진행.
        ``(False, str)``  — comfy 가 GPU0 를 들고 있다. 이번 run 은 **건너뛰고** 다음
                            tick 에 재시도한다 (backlog 센서가 monotonic seq 로 고유
                            run_key 를 만들므로 backlog 가 안 줄어도 재발화된다).
        ``(False, None)`` — lease 와 무관하게 서비스가 안 뜬다 = systemic. 호출부가
                            기존대로 Failure 를 던진다.

    lease 를 `wait_until_ready()` **앞에서** 먼저 본다 — comfy 가 잡고 있는 게 확실한데
    120s 를 헛돌 이유가 없다. 실패 후 **한 번 더** 보는 이유는 대기 중에 comfy 가 lease 를
    새로 잡았을 수 있어서다. 그 경우도 systemic 이 아니라 defer 가 맞다.

    ⚠️ **경계**: 이 판정은 임베딩 루프 **진입 시점**의 스냅샷이다. `limit` 기본 500건을
    도는 도중 comfy 가 lease 를 잡아가면 남은 건들은 503 을 맞는다. 그건 per-row
    fail-forward 가 `failed` 로 흡수하고, 전량 실패 시 호출부의 `inserted == 0` 가드가
    잡는다 — 이 함수가 생기기 전과 **동일한** 동작이라 회귀가 아니다. 루프 안에서 매 건
    lease 를 재조회하는 비용(건당 SELECT)이 comfy job 빈도(≈26건/3일)에 비해 크므로
    의도적으로 닫지 않았다.
    """
    lease = read_blocking_gpu0_lease(db, now_ts=now_ts)
    if lease is not None:
        return False, _lease_defer_reason(lease, now_ts=now_ts)
    if client.wait_until_ready():
        return True, None
    lease = read_blocking_gpu0_lease(db, now_ts=now_ts)
    if lease is not None:
        return False, _lease_defer_reason(lease, now_ts=now_ts)
    return False, None


def _lease_defer_reason(lease: GpuLease, *, now_ts: float | None = None) -> str:
    now = time.time() if now_ts is None else now_ts
    return (
        f"gpu0_comfy lease held ({describe_lease(lease)}) — deferring; "
        f"frees in <= {seconds_until_free(lease, now_ts=now):.0f}s (TTL 상한, 보통 더 빠름)"
    )
