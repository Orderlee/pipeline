"""Job finalize — 외부 API 결과 다운로드 + NAS atomic write + DB 상태 갱신.

이 함수는 두 곳에서 호출된다:
  - 비동기 엔진(Kling, Higgsfield): Dagster polling sensor 가 'done' 감지 후 호출
  - 동기 엔진(Nanobanana, GPT Image): submit.py 에서 즉시 호출

NAS 안착 순서:
  1) outputs/<입력이미지명>.<ext> atomic write (.partial → rename)
     — 파일명 = 입력 이미지 stem + output 확장자 (build_output_namer). 원본 없으면 <seq:03d>.
  2) 같은 batch 안의 모든 jobs 가 finalize 끝나면
     outputs/_manifest.json 마지막 작성 → ingest sensor 픽업 트리거.

manifest 시각이 batch 단위라 per-job finalize 가 마지막 job 인지 확인 필요 →
genai_jobs 의 status COUNT 로 검사.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

from adapters import get_adapter
from db import pg
from storage.manifest import build_outputs_manifest
from storage.nas_writer import atomic_write_bytes, atomic_write_json


_NAS_INCOMING = os.getenv("GENAI_NAS_INCOMING", "/nas/data/genai_studio")


def _batch_outputs_dir(batch_id: str) -> Path:
    """batch.submitted_at 기준 날짜 폴더 안의 outputs 경로.
    구조: <GENAI_NAS_INCOMING>/<YYYY-MM-DD>/<batch_id>/outputs/"""
    batch = pg.get_batch_with_jobs(batch_id)
    ts = (batch or {}).get("submitted_at") if batch else None
    if not isinstance(ts, datetime):
        ts = datetime.now()
    return Path(_NAS_INCOMING) / ts.strftime("%Y-%m-%d") / batch_id / "outputs"


# ----------------------------------------------------------------------
# 출력 파일명 = 입력 이미지명(stem) + output 확장자.
#   예) originals/ 의 혼잡도관리CCTV#12_crowded.png → outputs/ 의 혼잡도관리CCTV#12_crowded.mp4
# 원본명 소스는 originals/_manifest.json 의 original_filename (submit 시 기록).
# ----------------------------------------------------------------------
def _basename_stem(name: str) -> str:
    """경로 구분자 제거 후 확장자 뗀 basename. 빈 문자열이면 그대로 빈 문자열."""
    base = (name or "").replace("\\", "/").rsplit("/", 1)[-1]
    dot = base.rfind(".")
    return base[:dot] if dot > 0 else base


def _read_originals_stems(outputs_dir: Path) -> dict[int, str]:
    """seq -> 입력 이미지 stem. originals/_manifest.json 이 소스. 없으면 {} (txt2video 등)."""
    mpath = outputs_dir.parent / "originals" / "_manifest.json"
    try:
        data = json.loads(mpath.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    stems: dict[int, str] = {}
    for it in data.get("items", []) or []:
        try:
            seq = int(it["seq"])
        except (KeyError, TypeError, ValueError):
            continue
        stem = _basename_stem(it.get("original_filename") or it.get("filename") or "")
        if stem:
            stems[seq] = stem
    return stems


def build_output_namer(outputs_dir: Path) -> Callable[[int, str], str]:
    """seq,ext -> 출력 파일명. 입력 이미지 stem 으로 맞춤.

    batch 내 동일 stem 이 둘 이상이면 덮어쓰기 방지로 `<stem>__<seq:03d>` 접미
    (모든 충돌 seq 에 결정적으로 적용 — write 순서와 무관). 원본 없으면 `<seq:03d>` fallback.
    """
    stems = _read_originals_stems(outputs_dir)
    dup = {s for s, c in Counter(stems.values()).items() if c > 1}

    def namer(seq: int, ext: str) -> str:
        stem = stems.get(int(seq))
        if not stem:
            return f"{int(seq):03d}{ext}"
        return f"{stem}__{int(seq):03d}{ext}" if stem in dup else f"{stem}{ext}"

    return namer


def resolve_output_filenames(
    outputs_dir: Path, output_ext: str, seqs: Iterable[int]
) -> dict[int, str]:
    """seq -> 실제 출력 파일명 (promote / UI 링크용).

    우선순위:
      1) outputs/_manifest.json items[].filename — 완료 batch 의 실제 디스크명.
         legacy 배치(001.mp4)도 정확. finalize 가 파일과 함께 기록한 값.
      2) originals 기반 파생 (in-progress: manifest 미작성 구간) — finalize 가 쓸 이름과 동일.
    """
    try:
        data = json.loads((outputs_dir / "_manifest.json").read_text(encoding="utf-8"))
        recorded = {
            int(it["seq"]): it["filename"]
            for it in (data.get("items", []) or [])
            if it.get("filename")
        }
    except (OSError, ValueError, KeyError, TypeError):
        recorded = {}
    namer = build_output_namer(outputs_dir)
    return {int(s): (recorded.get(int(s)) or namer(int(s), output_ext)) for s in seqs}


def _fallback_cost_units(batch_id: str, engine: str) -> float | None:
    """Provider 응답에 cost 가 없을 때 가격표로 예상 차감 unit 산출 (Kling 전용).

    Kling image2video task-query 응답엔 credit 차감 필드가 없어 poll() 이 cost_units=None
    을 반환한다. batch.options_json 의 (model/mode/duration) 으로 lib.kling_pricing 에서
    환산. mode = 해상도 (std=720P/pro=1080P/4k=4K). 가격표 miss 시 None (그대로 NULL)."""
    if engine != "kling":
        return None
    batch = pg.get_batch_with_jobs(batch_id)
    opts_text = (batch or {}).get("options_json") or ""
    try:
        opts = json.loads(opts_text) if opts_text else {}
    except (TypeError, ValueError):
        return None
    from lib.kling_pricing import estimate_kling_cost
    est = estimate_kling_cost(
        model_name=opts.get("model_name") or "",
        mode=opts.get("mode") or "pro",
        duration=opts.get("duration") or "5",
    )
    return est["cost_units"]


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
    target_name = build_output_namer(outputs_dir)(seq_in_batch, output_ext)
    atomic_write_bytes(outputs_dir / target_name, blob)

    # provider 가 cost 를 안 주는 경우(예: Kling task 응답엔 cost 필드 없음) 가격표로 산출.
    # 안 채우면 cost_units 가 영구 NULL → costs 탭/집계가 $0 으로 보임.
    if cost_units is None:
        cost_units = _fallback_cost_units(batch_id, engine)

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
    namer = build_output_namer(outputs_dir)
    for r in results:
        seq = int(r["seq"])
        target_name = namer(seq, r["ext"])
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
            namer = build_output_namer(outputs_dir)
            for seq, status, provider_id, cost in job_rows:
                if status != "done":
                    continue
                items.append({
                    "seq": int(seq),
                    "filename": namer(int(seq), output_ext),
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
