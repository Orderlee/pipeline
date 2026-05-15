"""Job finalize — 외부 API 결과 다운로드 + NAS atomic write + DB 상태 갱신.

이 함수는 두 곳에서 호출된다:
  - 비동기 엔진(Kling, Higgsfield): Dagster polling sensor 가 'done' 감지 후 호출
  - 동기 엔진(Nanobanana, GPT Image): submit.py 에서 즉시 호출

NAS 안착 순서:
  1) outputs/<seq>.<ext> atomic write (.partial → rename)
  2) 같은 batch 안의 모든 jobs 가 finalize 끝나면
     outputs/_manifest.json 마지막 작성 → ingest sensor 픽업 트리거.

manifest 시각이 batch 단위라 per-job finalize 가 마지막 job 인지 확인 필요 →
genai_jobs 의 status COUNT 로 검사.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from adapters import get_adapter
from db import pg
from storage.manifest import build_outputs_manifest
from storage.nas_writer import atomic_write_bytes, atomic_write_json


_NAS_INCOMING = os.getenv("GENAI_NAS_INCOMING", "/nas/staging/incoming")


def _batch_outputs_dir(batch_id: str) -> Path:
    """batch.submitted_at 기준 날짜 폴더 안의 outputs 경로."""
    batch = pg.get_batch_with_jobs(batch_id)
    ts = (batch or {}).get("submitted_at") if batch else None
    if not isinstance(ts, datetime):
        ts = datetime.now()
    return Path(_NAS_INCOMING) / "genai" / ts.strftime("%Y-%m-%d") / batch_id / "outputs"


def finalize_job(
    batch_id: str,
    seq_in_batch: int,
    engine: str,
    result_url: str,
    output_ext: str,
    cost_units: float | None = None,
) -> None:
    """단일 job 의 결과를 NAS 에 안착 + status='done'."""
    adapter = get_adapter(engine)
    blob = adapter.download_result(result_url)

    outputs_dir = _batch_outputs_dir(batch_id)
    target_name = f"{seq_in_batch:03d}{output_ext}"
    atomic_write_bytes(outputs_dir / target_name, blob)

    job_id = f"{batch_id}-{seq_in_batch:03d}"
    pg.update_job_status(job_id, status="done", cost_units=cost_units)
    pg.recompute_batch_status(batch_id)

    # 이 batch 의 모든 jobs 가 끝났으면 outputs/_manifest.json 작성
    _maybe_write_outputs_manifest(batch_id, engine, adapter.output_media, outputs_dir)


def finalize_sync_results(
    batch_id: str,
    engine: str,
    output_media: str,
    results: list[dict],
) -> None:
    """동기 엔진(Nanobanana, GPT Image)의 즉시 결과를 NAS 에 안착 + status='done'.

    submit.py 가 batch 안의 모든 동기 결과를 모아서 한 번에 호출. 비동기 엔진의
    finalize_job 과 달리 외부 API 다운로드 없이 in-memory bytes 를 NAS write.
    """
    outputs_dir = _batch_outputs_dir(batch_id)
    for r in results:
        seq = int(r["seq"])
        target_name = f"{seq:03d}{r['ext']}"
        atomic_write_bytes(outputs_dir / target_name, r["bytes"])
        pg.update_job_status(r["job_id"], status="done", cost_units=r.get("cost_units"))
    pg.recompute_batch_status(batch_id)
    # 모든 동기 결과 안착 → manifest 작성
    _maybe_write_outputs_manifest(batch_id, engine, output_media, outputs_dir)


def fail_job(batch_id: str, seq_in_batch: int, error_message: str) -> None:
    job_id = f"{batch_id}-{seq_in_batch:03d}"
    pg.update_job_status(job_id, status="failed", error_message=error_message)
    pg.recompute_batch_status(batch_id)
    # 실패 후에도 batch 종료 여부 확인해 manifest 작성 검토
    batch = pg.get_batch_with_jobs(batch_id)
    if batch is None:
        return
    engine = batch["engine"]
    output_media = batch["output_media"]
    outputs_dir = _batch_outputs_dir(batch_id)
    _maybe_write_outputs_manifest(batch_id, engine, output_media, outputs_dir)


def _maybe_write_outputs_manifest(
    batch_id: str,
    engine: str,
    output_media: str,
    outputs_dir: Path,
) -> None:
    """배치의 모든 job 이 done|failed 면 outputs/_manifest.json 작성.

    Codex Q8 HIGH: 다중 writer race — BackgroundTask finalize / sync finalize / retry
    세 곳에서 동시 호출 가능. PG advisory lock 으로 single writer 직렬화.
    pg_try_advisory_xact_lock 이 false 면 다른 writer 가 처리 중 → no-op.
    """
    # batch_id 의 hashtext → 64-bit advisory lock key
    with pg.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s)::bigint)",
                         (f"genai_manifest:{batch_id}",))
            (got_lock,) = cur.fetchone()
            if not got_lock:
                # 다른 writer 가 이미 처리 중 — silent skip (그 writer 가 마무리)
                return
            # 트랜잭션 안에서 batch + jobs 조회 + manifest 작성. lock 은 commit 시 해제.
            cur.execute(
                """
                SELECT engine, output_media FROM genai_batches WHERE batch_id = %s
                """,
                (batch_id,),
            )
            row = cur.fetchone()
            if row is None:
                return
            engine_db, output_media_db = row
            cur.execute(
                """
                SELECT seq_in_batch, status, provider_job_id, cost_units
                  FROM genai_jobs WHERE batch_id = %s ORDER BY seq_in_batch
                """,
                (batch_id,),
            )
            job_rows = cur.fetchall()
            pending = [r for r in job_rows if r[1] in ("pending", "submitted", "running")]
            if pending:
                return  # 아직 끝나지 않은 job 있음

            items = []
            output_ext = ".mp4" if (output_media_db or output_media) == "video" else ".png"
            for seq, status, provider_id, cost in job_rows:
                if status != "done":
                    continue
                items.append({
                    "seq": int(seq),
                    "filename": f"{int(seq):03d}{output_ext}",
                    "provider_job_id": provider_id,
                    "cost_units": float(cost) if cost is not None else None,
                })

            manifest = build_outputs_manifest(
                batch_id=batch_id,
                engine=engine_db or engine,
                output_media=output_media_db or output_media,
                items=items,
            )
            atomic_write_json(outputs_dir / "_manifest.json", manifest)
