"""GenAI 출력물 → 일반 라벨링 파이프라인으로 promote.

흐름:
  1) batch.status in {succeeded, partial_success} 확인 + done jobs 추출
  2) pg.claim_batch_for_promote(batch_id) — race-safe boolean flag
     이미 promote 된 batch 면 False → 409 응답
  3) dispatch request JSON 을 <INCOMING_DIR>/.dispatch/pending/<request_id>.json 에
     atomic write (FIRST! auto-bootstrap exclusion 이 file 존재만 보고 동작하므로 파일을
     copy 하기 전에 dispatch request 가 있어야 한다).
  4) 각 done job 출력물 (`<GENAI_NAS_INCOMING>/<date>/<batch>/outputs/<seq>.<ext>`) 을
     `<INCOMING_DIR>/genai_<batch>.tmp/<seq>.<ext>` 로 복사 → 전체 끝나면 `.tmp` →
     `genai_<batch>` 로 rename (atomic on same fs).

엔진별 ext: video(mp4) vs image(png) 는 batch.output_media 기준.

manifest 필드 매핑 (ops_register.py:191-211 에서 사용):
  - source_type=genai_output    → raw_files.source_type
  - genai_engine=<batch.engine> → raw_files.genai_engine
  - label_policy=<user choice>  → raw_files.label_policy ('required' or 'none')
  - batch_id=<batch>            → ops_register 가 items[seq] 와 합쳐 raw_files.genai_batch_id +
                                   genai_jobs 매핑
  - items=[{seq, filename, ...}] → seq→filename 매핑 (asset_id 링크용)
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from db import pg


# manifest_id 와 dispatch sensor 가 받는 enum 의 유효 집합. dispatch service 가
# 자체 validate 안 하므로 여기서 fail-loud (ops_register 의 fail-loud 보다 사용자에게
# 더 가까운 위치에서 reject 가능 = 400 응답 vs 500/run failure).
VALID_LABEL_POLICIES = frozenset({"required", "none"})
VALID_LABELING_METHODS = frozenset({
    "timestamp_video",
    "captioning_video",
    "captioning_image",
    "classification_video",
    "classification_image",
    "bbox",
    "skip",
})
# ops_register._VALID_GENAI_ENGINES 와 동기화 — 003_genai_veo.sql CHECK constraint 와 일치.
# promote 단계에서 fail-loud 해야 사용자가 400 받고 Dagster run 실패 안 봄 (Codex MEDIUM-2).
VALID_GENAI_ENGINES = frozenset({"kling", "higgsfield", "veo", "nanobanana", "gpt_image"})
# batch.status 가 promote 가능한 셋. failed 면 결과물 자체가 없음.
PROMOTABLE_STATUSES = frozenset({"succeeded", "partial_success", "done"})


def _output_ext_for(batch: dict) -> str:
    """batch.output_media 로 확장자 결정. video→.mp4, image→.png (genai 컨벤션)."""
    media = (batch.get("output_media") or "").strip().lower()
    return ".mp4" if media == "video" else ".png"


def _batch_outputs_dir(batch_id: str, submitted_at: datetime) -> Path:
    """<GENAI_NAS_INCOMING>/<YYYY-MM-DD>/<batch>/outputs — finalize.py 와 동일 컨벤션."""
    nas_root = Path(os.getenv("GENAI_NAS_INCOMING", "/nas/data/genai_studio"))
    return nas_root / submitted_at.strftime("%Y-%m-%d") / batch_id / "outputs"


class PromoteValidationError(Exception):
    """입력 검증 실패 — endpoint 가 400 으로 매핑."""


class PromoteConflictError(Exception):
    """이미 promote 된 batch — endpoint 가 409 으로 매핑."""


def promote_batch_to_labeling(
    batch_id: str,
    *,
    labeling_method: list[str],
    label_policy: str,
    categories: list[str],
    classes: list[str],
    image_profile: str = "current",
    run_mode: str = "labeling_only",
    requested_by: str | None = None,
) -> dict[str, Any]:
    """promote 실행. validate → claim → dispatch request 쓰기 → 파일 copy + rename.

    예외:
      - PromoteValidationError: 입력/상태 문제 (batch not found, 잘못된 enum, done 0개 등)
      - PromoteConflictError: 이미 promote 됨 (race-safe via PG UPDATE)
      - 그 외 OSError: NAS 쓰기 실패 — caller (endpoint) 가 500 으로 매핑

    반환: {request_id, folder_name, copied_count, dispatch_path}
    """
    # 1) 입력 validate
    if not labeling_method:
        raise PromoteValidationError("labeling_method 비어있음")
    bad_methods = [m for m in labeling_method if m not in VALID_LABELING_METHODS]
    if bad_methods:
        raise PromoteValidationError(
            f"unknown labeling_method: {bad_methods} (allowed: {sorted(VALID_LABELING_METHODS)})"
        )
    if label_policy not in VALID_LABEL_POLICIES:
        raise PromoteValidationError(
            f"label_policy={label_policy!r} not in {sorted(VALID_LABEL_POLICIES)}"
        )

    # 2) batch 조회 + 상태 확인
    batch = pg.get_batch_with_jobs(batch_id)
    if batch is None:
        raise PromoteValidationError(f"batch not found: {batch_id}")
    status = (batch.get("status") or "").strip().lower()
    if status not in PROMOTABLE_STATUSES:
        raise PromoteValidationError(
            f"batch.status={status!r} not promotable (need one of {sorted(PROMOTABLE_STATUSES)})"
        )

    # done job 만 promote — failed/pending 은 skip. seq 순서 보장.
    done_jobs = sorted(
        [j for j in (batch.get("jobs") or []) if (j.get("status") or "") == "done"],
        key=lambda j: int(j.get("seq_in_batch") or 0),
    )
    if not done_jobs:
        raise PromoteValidationError("no done jobs in batch (모두 실패/대기)")

    engine = (batch.get("engine") or "").strip().lower()
    if not engine:
        raise PromoteValidationError(f"batch.engine missing for {batch_id}")
    # Codex MEDIUM-2: engine 도 promote 단계에서 fail-loud (사용자에게 400 → run 실패 회피).
    if engine not in VALID_GENAI_ENGINES:
        raise PromoteValidationError(
            f"engine={engine!r} not in {sorted(VALID_GENAI_ENGINES)} "
            f"(ops_register 가 CHECK constraint 로 reject 했을 것)"
        )

    output_ext = _output_ext_for(batch)
    # Codex HIGH-2: submitted_at 이 str/None 으로 오면 fallback to datetime.now() 가 잘못된
    # 날짜폴더 탐색 → 모든 src.exists() 실패. psycopg2 default cursor 는 TIMESTAMPTZ → datetime
    # 자동 캐스팅하지만, 방어로 str 도 파싱한다.
    submitted_at = batch.get("submitted_at")
    if isinstance(submitted_at, str) and submitted_at:
        try:
            submitted_at = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))
        except ValueError:
            submitted_at = None
    if not isinstance(submitted_at, datetime):
        raise PromoteValidationError(
            f"batch.submitted_at 가 datetime 이 아님 (got {type(batch.get('submitted_at'))!r}) "
            f"— DB schema 또는 cursor 설정 이상"
        )

    # 3) 경로 + 페이로드 구성 — claim 전에 file 존재까지 검증 (Codex HIGH-1).
    incoming_dir = Path(os.getenv("INCOMING_DIR", "/nas/data/incoming"))
    folder_name = f"genai_{batch_id}"
    target_dir = incoming_dir / folder_name
    tmp_dir = incoming_dir / f"{folder_name}.tmp"
    src_outputs_dir = _batch_outputs_dir(batch_id, submitted_at)

    # items: seq → filename 매핑. ops_register 가 raw_files ↔ genai_jobs 링크용.
    items: list[dict[str, Any]] = []
    src_paths: list[tuple[int, Path]] = []
    for j in done_jobs:
        seq = int(j.get("seq_in_batch") or 0)
        filename = f"{seq:03d}{output_ext}"
        src = src_outputs_dir / filename
        if not src.exists():
            # output 누락 — claim 안 한 상태에서 fail. 다음 재시도 가능.
            raise PromoteValidationError(
                f"output 파일 없음: seq={seq} path={src} "
                f"(NAS 정리됐거나 finalize 미완료)"
            )
        src_paths.append((seq, src))
        items.append({
            "seq": seq,
            "filename": filename,
            "provider_job_id": j.get("provider_job_id"),
        })

    # target_dir 충돌 사전 검사 — claim 전에 fail 해 재시도 가능 (Codex HIGH-1).
    if target_dir.exists():
        raise PromoteValidationError(
            f"target dir 충돌: {target_dir} 이미 존재 "
            f"(이전 promote 잔여물? 수동 정리 후 재시도)"
        )

    # 4) Atomic claim — 모든 사전 검증 통과 후. 두 번째 호출은 False 받아 409.
    # SQL options_json::jsonb 가 malformed JSON 만나면 PG 에러 → 500. caller 가 처리.
    claimed = pg.claim_batch_for_promote(batch_id)
    if not claimed:
        raise PromoteConflictError(f"batch {batch_id} 이미 promote 됨")

    request_id = uuid4().hex
    now_iso = datetime.now().isoformat()
    payload: dict[str, Any] = {
        "request_id": request_id,
        "folder_name": folder_name,
        "run_mode": run_mode,
        "labeling_method": labeling_method,
        "categories": categories,
        "classes": classes,
        "image_profile": image_profile,
        "requested_by": requested_by,
        "requested_at": now_iso,
        "archive_only": False,
        # GenAI provenance — service.prepare_dispatch_request 가 pass-through →
        # write_dispatch_manifest 가 manifest 에 통과 → ops_register 가 raw_files 에 INSERT.
        "source_type": "genai_output",
        "genai_engine": engine,
        "label_policy": label_policy,
        "batch_id": batch_id,
        "items": items,
        "transfer_tool": "genai_promote",
    }

    # 5) 파일 복사 먼저 — tmp dir 에 전부 복사 후 atomic rename.
    # 순서 이유: dispatch sensor 가 JSON 을 보면 즉시 dispatch_stage_job 트리거하므로
    # 파일이 target/ 에 안착한 뒤에 JSON 이 visible 해져야 한다. auto-bootstrap 은
    # default 180s tick + stability window (수 초) 이므로 target/ → JSON 사이 <100ms
    # 윈도우에선 도달 못함. 반대 순서면 dispatch_stage_job 이 빈 폴더에서 ingest 0건.
    #
    # 실패 시 claim 보상: claim 후 execute 가 실패하면 pg.release_batch_promote_claim
    # 으로 unset 해 재시도 가능. (Codex HIGH-1 의 보강 — orphan claim 방지.)
    pending_dir = incoming_dir / ".dispatch" / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    dispatch_target = pending_dir / f"{request_id}.json"

    def _rollback_claim() -> None:
        try:
            pg.release_batch_promote_claim(batch_id)
        except Exception:
            # 보상 실패는 swallow — caller 의 raise 가 우선. 관리자가 수동 unset.
            pass

    try:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
        tmp_dir.mkdir(parents=True, exist_ok=False)
        for seq, src in src_paths:
            dst = tmp_dir / f"{seq:03d}{output_ext}"
            shutil.copy2(str(src), str(dst))
        tmp_dir.rename(target_dir)
    except Exception:
        # 파일 복사/rename 실패 → tmp 청소 + claim 해제 (재시도 가능 상태로 되돌림).
        try:
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        _rollback_claim()
        raise

    # 6) Dispatch request JSON 마지막에 — atomic .tmp → rename.
    # 이 시점 이후로 dispatch_sensor 가 JSON 을 픽 + 30s 안에 dispatch_stage_job 발화.
    dispatch_tmp = pending_dir / f".{request_id}.json.tmp"
    try:
        dispatch_tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        dispatch_tmp.replace(dispatch_target)
    except Exception:
        # JSON write 실패 — target_dir 청소 + claim 해제. auto-bootstrap 이 빈/없는 폴더
        # 픽업해 일반 ingest 로 흘러가는 것 방지 (Codex MEDIUM-1 orphan 방지).
        try:
            dispatch_tmp.unlink(missing_ok=True)
        except Exception:
            pass
        try:
            if target_dir.exists():
                shutil.rmtree(target_dir, ignore_errors=True)
        except Exception:
            pass
        _rollback_claim()
        raise

    return {
        "request_id": request_id,
        "folder_name": folder_name,
        "copied_count": len(src_paths),
        "dispatch_path": str(dispatch_target),
    }
