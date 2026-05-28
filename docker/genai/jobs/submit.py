"""POST /genai/batches 처리 — N장 이미지 + 프롬프트 → batch + N jobs + NAS originals.

흐름:
  1) genai_batches + genai_jobs × N INSERT (Postgres TXN, status='pending')
  2) <GENAI_NAS_INCOMING>/<YYYY-MM-DD>/<batch>/originals/<seq>.<ext> atomic write
     (기본 /nas/data/genai_studio — auto-bootstrap ingest 격리 경로)
  3) originals/_manifest.json 작성 (provenance bridge 완성 시 ingest 픽업용)
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


_NAS_INCOMING = os.getenv("GENAI_NAS_INCOMING", "/nas/data/genai_studio")


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
    batch_root = Path(_NAS_INCOMING) / date_dir / batch_id
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
    #
    # 동시성 게이트 (Q6 — Kling 1303 회피): engine 동시 한도가 있으면(>0) 현재
    # in-flight 수를 고려해 한도까지만 즉시 submit. 나머지는 처음부터 'pending'
    # 으로 두고 sensor drain 이 슬롯 빌 때 제출. 1303 은 race 안전망으로만 처리.
    from adapters import KlingTransientError, engine_max_concurrent
    max_conc = engine_max_concurrent(engine)
    budget = None
    if max_conc > 0:
        budget = max(0, max_conc - pg.count_inflight_jobs(engine))

    iter_items: list[tuple[str, bytes]] = files if files else [("", b"")]
    sync_results: list[dict] = []
    for seq, (filename, blob) in enumerate(iter_items, start=1):
        job_id = f"{batch_id}-{seq:03d}"
        # 용량 소진 시 즉시 submit 안 하고 deferred(pending) — drain 이 이어받음
        if budget is not None and budget <= 0:
            pg.mark_job_deferred(job_id, f"{engine} 동시 작업 한도 대기 (max={max_conc})")
            continue
        try:
            sub = adapter.submit(blob, filename, prompt, options=options)
            pg.update_job_submitted(job_id, sub.provider_job_id)
            if budget is not None:
                budget -= 1
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
        except KlingTransientError as exc:
            # 1303 등 — race 로 한도 초과. failed 아님, deferred 로 두고 budget 소진
            # 처리해 같은 pass 에서 더 안 쏨 (Codex Q1).
            pg.mark_job_deferred(job_id, str(exc))
            if budget is not None:
                budget = 0
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

    # 6) batch status 재계산 — submit 루프에서 일부/전부 실패했거나(비동기 엔진의
    # submit 실패: Kling 1201/429 등) 동기 finalize 가 없던 경우, 여기서 갱신 안 하면
    # batch 가 'running' 에 영원히 멈춤 (n_failed=0 인 채로). recompute 는 idempotent
    # 라 finalize_sync_results 가 이미 호출했어도 안전.
    #   - 전부 submit 실패 → 'failed'
    #   - 비동기 일부 submit 성공 → 'running' 유지 (sensor 가 이후 finalize)
    pg.recompute_batch_status(batch_id)

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
