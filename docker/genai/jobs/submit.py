"""POST /genai/batches 처리 — N장 이미지 + 프롬프트 → batch + N jobs + NAS originals.

흐름:
  1) genai_batches + genai_jobs × N INSERT (Postgres TXN, status='pending')
  2) /nas/staging/incoming/genai/<batch>/originals/<seq>.<ext> atomic write
  3) originals/_manifest.json 마지막에 작성 → ingest sensor 픽업 트리거
  4) 어댑터 submit() 비동기 호출 — provider_job_id 받아 update_job_submitted()
     동기 엔진(Phase 4: Nano/GPT) 은 submit 시점에 결과까지 받음 → finalize 도 즉시.

이 함수는 FastAPI handler 의 헬퍼. 응답으로 batch_id 반환.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path, PurePosixPath

from adapters import get_adapter
from db import pg
from storage.manifest import build_originals_manifest
from storage.nas_writer import atomic_write_bytes, atomic_write_json


_NAS_INCOMING = os.getenv("GENAI_NAS_INCOMING", "/nas/staging/incoming")


def submit_batch(
    engine: str,
    prompt: str,
    files: list[tuple[str, bytes]],     # [(filename, bytes), ...]  text-only 모드 시 []
    requested_by: str | None = None,
    options: dict | None = None,
) -> dict:
    if not prompt or not prompt.strip():
        raise ValueError("prompt empty")
    opts = options or {}
    is_text_only = (opts.get("mode") or "").strip().lower() == "txt2video"
    if not files and not is_text_only:
        raise ValueError("files empty")
    if is_text_only and engine != "veo":
        # 현재 txt2video 지원 엔진은 Veo 뿐. 다른 엔진이 mode=txt2video 보내면 명시 거부.
        raise ValueError(f"txt2video 지원 안 함: engine={engine!r} (veo 만 가능)")
    adapter = get_adapter(engine)

    batch_id = str(uuid.uuid4())[:12]
    n = len(files) if files else 1   # text-only: 1 job
    submitted_now = datetime.now()

    # 1) DB INSERT — batch + N jobs (status='pending')
    # input_total_bytes 는 quota 집계용 (limits.check_daily_quota 가 options_json 파싱)
    options_with_bytes = dict(options or {})
    options_with_bytes["input_total_bytes"] = sum(len(b) for _, b in files)
    pg.insert_genai_batch({
        "batch_id": batch_id,
        "engine": engine,
        "output_media": adapter.output_media,
        "prompt": prompt,
        "options_json": _json_or_none(options_with_bytes),
        "requested_by": requested_by,
        "status": "running",       # submit 직후엔 jobs 가 in-flight
        "n_total": n,
    })
    jobs = [
        {"job_id": f"{batch_id}-{seq:03d}", "batch_id": batch_id, "seq_in_batch": seq}
        for seq in range(1, n + 1)
    ]
    pg.insert_genai_jobs_batch(jobs)

    # 2) NAS originals/ atomic write — 날짜별 폴더 안에 batch
    # text-only(txt2video) 는 입력 이미지 없음 → originals 디렉토리·manifest 둘 다 skip.
    date_dir = submitted_now.strftime("%Y-%m-%d")
    batch_root = Path(_NAS_INCOMING) / "genai" / date_dir / batch_id
    originals_dir = batch_root / "originals"
    item_meta: list[dict] = []
    if files:
        for seq, (filename, blob) in enumerate(files, start=1):
            ext = PurePosixPath(filename).suffix or ".png"
            target_name = f"{seq:03d}{ext}"
            target_path = originals_dir / target_name
            atomic_write_bytes(target_path, blob)
            item_meta.append({
                "seq": seq,
                "filename": target_name,
                "original_filename": filename,
                "size": len(blob),
            })

        # 3) originals/_manifest.json 마지막에 작성
        originals_manifest = build_originals_manifest(
            batch_id=batch_id,
            engine=engine,
            output_media=adapter.output_media,
            items=item_meta,
        )
        atomic_write_json(originals_dir / "_manifest.json", originals_manifest)

    # 4) 어댑터 submit (per job) — 비동기/동기 분기
    # text-only: 단일 job, image_bytes=b"" / filename="" 으로 호출.
    iter_items: list[tuple[str, bytes]] = files if files else [("", b"")]
    sync_results: list[dict] = []
    for seq, (filename, blob) in enumerate(iter_items, start=1):
        job_id = f"{batch_id}-{seq:03d}"
        try:
            sub = adapter.submit(blob, filename, prompt, options=options)
            pg.update_job_submitted(job_id, sub.provider_job_id)
            if sub.is_synchronous:
                if sub.immediate_result is None:
                    raise RuntimeError(
                        f"sync engine 의 immediate_result 가 None: engine={engine}"
                    )
                sync_results.append({
                    "job_id": job_id,
                    "seq": seq,
                    "bytes": sub.immediate_result,
                    "ext": sub.immediate_ext or adapter.output_ext,
                    "cost_units": sub.cost_units,
                })
        except Exception as exc:
            pg.update_job_status(job_id, status="failed", error_message=str(exc))

    # 5) 동기 엔진 결과 즉시 finalize (NAS outputs atomic write + status='done')
    if sync_results:
        from .finalize import finalize_sync_results
        finalize_sync_results(
            batch_id=batch_id,
            engine=engine,
            output_media=adapter.output_media,
            results=sync_results,
        )

    return {
        "batch_id": batch_id,
        "engine": engine,
        "n_total": n,
        "submitted_at": submitted_now.isoformat(),
    }


def _json_or_none(d: dict | None) -> str | None:
    if not d:
        return None
    import json as _json
    return _json.dumps(d, ensure_ascii=False)
