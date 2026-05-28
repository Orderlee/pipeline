"""INGEST @op — register_incoming: 검증 + 정규화 + raw_files 배치 insert."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from uuid import uuid4

from vlm_pipeline.lib.env_utils import storage_raw_key_prefix_from_source_unit
from vlm_pipeline.lib.sanitizer import make_unique_key, sanitize_filename, sanitize_path_component
from vlm_pipeline.lib.validator import detect_media_type, validate_incoming
from vlm_pipeline.resources.postgres import PostgresResource

from .ops_common import _append_ingest_rejection

# GenAI provenance — manifest 가 명시한 경우만 통과. CHECK constraint 와 일치.
_VALID_SOURCE_TYPES = frozenset({"camera", "nas_upload", "genai_source", "genai_output"})
_VALID_GENAI_ENGINES = frozenset({"kling", "higgsfield", "veo", "nanobanana", "gpt_image"})
_VALID_LABEL_POLICIES = frozenset({"required", "none"})


def register_incoming(
    context,
    db: PostgresResource,
    manifest: dict,
    ingest_rejections: list[dict] | None = None,
) -> list[dict]:
    """검증 → 정규화 → raw_files 배치 INSERT.

    manifest path는 컨테이너 경로 (/nas/incoming/...) 기준.
    라벨 JSON은 INGEST 대상 아님 (LABEL 단계에서 별도 생성).

    MinIO raw_key는 source unit/rel_path 구조를 유지하여 폴더 구조를 보존한다.
    단, GCP auto-bootstrap unit(`gcp/<bucket>/...`)은 `gcp/` prefix를 제거한다.
    """
    results: list[dict] = []
    batch_id = manifest.get("manifest_id", f"batch_{datetime.now():%Y%m%d_%H%M%S}")
    source_unit_name = manifest.get("source_unit_name", "")
    source_unit_type = str(manifest.get("source_unit_type", "")).strip().lower()

    # GenAI manifest 의 items[seq] 매핑 — orchestrator 가 _manifest.json 에 넣은 경우만.
    # 미지정 시 빈 dict (legacy / 일반 NAS source 영향 없음).
    _genai_batch_id = manifest.get("batch_id") if manifest.get("source_type") in (
        "genai_source", "genai_output"
    ) else None
    _genai_filename_to_seq: dict[str, int] = {}
    if _genai_batch_id:
        for it in manifest.get("items", []) or []:
            seq = it.get("seq")
            if seq is None:
                continue
            for k in ("filename", "original_filename"):
                fname = it.get(k)
                if fname:
                    _genai_filename_to_seq[fname] = int(seq)

    def _sanitize_path_parts(raw: str) -> str:
        parts = [p for p in Path(str(raw or "")).parts if p not in ("", ".", "..")]
        return "/".join(sanitize_path_component(p) for p in parts if p)

    # ── 이미 completed인 source_path는 조기 차단 (ffprobe 전에 drop) ──────────
    _manifest_file_entries = [e for e in manifest.get("files", []) if isinstance(e, dict)]
    _candidate_source_paths = [
        str(e.get("path", "")).strip() for e in _manifest_file_entries if str(e.get("path", "")).strip()
    ]
    try:
        _completed_source_set: set[str] = (
            db.find_completed_source_paths(_candidate_source_paths)
            if _candidate_source_paths
            else set()
        )
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"find_completed_source_paths 조회 실패(필터 스킵): {exc}")
        _completed_source_set = set()

    # ── raw_key 충돌 방지: 배치 내 + DB 기존 키 모두 추적 ──────────
    # 1차 패스: 모든 파일의 raw_key 후보를 먼저 계산
    _pre_entries: list[tuple[dict, str, str, str, str, str, object]] = []
    _candidate_keys: list[str] = []

    for entry in manifest.get("files", []):
        filepath = entry.get("path", "")
        rel_path = entry.get("rel_path", "")
        if filepath and str(filepath).strip() in _completed_source_set:
            results.append({
                "path": filepath,
                "status": "already_completed",
                "message": "already_completed_in_prior_run",
            })
            continue
        try:
            vr = validate_incoming(filepath)
            if vr.level == "FAIL":
                media_type = detect_media_type(filepath)
                _append_ingest_rejection(
                    ingest_rejections,
                    source_path=filepath,
                    rel_path=rel_path,
                    media_type=media_type,
                    stage="register",
                    error_message=vr.message,
                    retryable=False,
                    error_code=vr.message,
                )
                context.log.warning(f"register 검증 실패(미삽입): {filepath}: {vr.message}")
                results.append({"path": filepath, "status": "failed", "message": vr.message})
                continue

            original_name = Path(filepath).name
            sanitized_name = sanitize_filename(original_name)
            media_type = detect_media_type(filepath)

            if rel_path:
                sanitized_rel_dir = _sanitize_path_parts(str(Path(rel_path).parent))
                if sanitized_rel_dir:
                    sanitized_rel = f"{sanitized_rel_dir}/{sanitized_name}"
                else:
                    sanitized_rel = sanitized_name
            else:
                sanitized_rel = sanitized_name

            sanitized_source_unit_name = storage_raw_key_prefix_from_source_unit(source_unit_name)
            if sanitized_source_unit_name and source_unit_type != "file":
                candidate_key = f"{sanitized_source_unit_name}/{sanitized_rel}"
            else:
                candidate_key = f"{sanitized_rel}"

            _candidate_keys.append(candidate_key)
            _pre_entries.append((entry, filepath, rel_path, original_name, sanitized_name, media_type, vr))

        except Exception as e:
            context.log.error(f"처리 실패: {filepath}: {e}")
            _append_ingest_rejection(
                ingest_rejections,
                source_path=filepath,
                rel_path=rel_path,
                media_type=detect_media_type(filepath),
                stage="register",
                error_message=str(e),
                retryable=False,
                error_code="register_exception",
            )
            results.append({"path": filepath, "status": "failed", "message": str(e)})

    # DB에서 후보 키들의 기존 존재 여부를 한 번에 조회하여 seen set 초기화
    try:
        _seen_keys: set[str] = db.find_existing_raw_keys(_candidate_keys) if _candidate_keys else set()
    except Exception:  # noqa: BLE001
        _seen_keys = set()

    # 2차 패스: 고유 raw_key 확정 + 레코드 생성
    for (_entry, filepath, rel_path, original_name, sanitized_name, media_type, vr) in _pre_entries:
        sanitized_source_unit_name = storage_raw_key_prefix_from_source_unit(source_unit_name)
        if rel_path:
            sanitized_rel_dir = _sanitize_path_parts(str(Path(rel_path).parent))
            sanitized_rel = f"{sanitized_rel_dir}/{sanitized_name}" if sanitized_rel_dir else sanitized_name
        else:
            sanitized_rel = sanitized_name

        if sanitized_source_unit_name and source_unit_type != "file":
            candidate_key = f"{sanitized_source_unit_name}/{sanitized_rel}"
        else:
            candidate_key = f"{sanitized_rel}"

        raw_key = make_unique_key(candidate_key, _seen_keys)
        if raw_key != candidate_key:
            context.log.warning(
                f"raw_key 충돌 감지, suffix 부여: {candidate_key} -> {raw_key} "
                f"(original={original_name})"
            )

        asset_id = str(uuid4())
        record = {
            "asset_id": asset_id,
            "source_path": filepath,
            "original_name": original_name,
            "media_type": media_type,
            "raw_key": raw_key,
            "ingest_batch_id": batch_id,
            "transfer_tool": manifest.get("transfer_tool", "manual"),
            "ingest_status": "pending",
            "error_message": vr.message if vr.level == "WARN" else None,
        }
        if source_unit_name:
            record["source_unit_name"] = source_unit_name

        # GenAI provenance — orchestrator(Phase 3) 가 manifest 에 명시한 경우만 통과.
        # 미지정 시 DB DEFAULT (source_type='camera', label_policy='required') 적용.
        # 잘못된 값은 PG CHECK 위반으로 INSERT 실패 → 배치 전체가 per-row fallback 으로
        # 떨어져 디버깅이 어려우므로, 여기서 fail-loud 로 거른다.
        genai_source_type = manifest.get("source_type")
        if genai_source_type:
            if genai_source_type not in _VALID_SOURCE_TYPES:
                raise ValueError(
                    f"manifest.source_type={genai_source_type!r} not in {sorted(_VALID_SOURCE_TYPES)}"
                )
            record["source_type"] = genai_source_type
        genai_engine = manifest.get("genai_engine")
        if genai_engine:
            if genai_engine not in _VALID_GENAI_ENGINES:
                raise ValueError(
                    f"manifest.genai_engine={genai_engine!r} not in {sorted(_VALID_GENAI_ENGINES)}"
                )
            record["genai_engine"] = genai_engine
        genai_label_policy = manifest.get("label_policy")
        if genai_label_policy:
            if genai_label_policy not in _VALID_LABEL_POLICIES:
                raise ValueError(
                    f"manifest.label_policy={genai_label_policy!r} not in {sorted(_VALID_LABEL_POLICIES)}"
                )
            record["label_policy"] = genai_label_policy

        result_entry = {
            "asset_id": asset_id,
            "path": filepath,
            "original_name": original_name,
            "sanitized_name": sanitized_name,
            "media_type": media_type,
            "raw_key": raw_key,
            "rel_path": rel_path,
            "status": "registered",
            "record": record,
        }
        # GenAI items 매핑 — ops_normalize 가 _link_genai_asset_if_any 로 사용.
        if _genai_batch_id and _genai_filename_to_seq:
            seq = _genai_filename_to_seq.get(original_name) \
                  or _genai_filename_to_seq.get(sanitized_name)
            if seq is not None:
                result_entry["_genai_batch_id"] = _genai_batch_id
                result_entry["_genai_seq_in_batch"] = int(seq)
            else:
                # batch_id 는 있는데 seq 매칭 실패 — orchestrator 의 manifest 와 실제
                # 안착 파일명이 어긋난 케이스. genai_jobs FK 가 NULL 로 남아 build asset
                # 의 pair query 가 해당 row 를 누락. 디버깅 위해 fail-loud 로 warn.
                context.log.warning(
                    f"genai filename->seq 매칭 실패 batch={_genai_batch_id} "
                    f"original_name={original_name!r} sanitized={sanitized_name!r} "
                    f"items={sorted(_genai_filename_to_seq.keys())[:5]}..."
                )
        results.append(result_entry)

    return results
