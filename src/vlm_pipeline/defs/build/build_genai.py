"""BUILD GenAI paired dataset 빌드 로직.

Layer 3: 순수 Python + MinIOResource/DB 사용. Dagster @asset 없음.
_build_project_genai 와 그 직속 헬퍼 3개를 assets.py 에서 분리.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import PurePosixPath
from uuid import uuid4

from vlm_pipeline.lib.dataset_lineage import make_build_lineage
from vlm_pipeline.resources.minio import MinIOResource

from vlm_pipeline.defs.build.build_helpers import (
    _GENAI_OUTPUT_DIR,
    RAW_BUCKET,
    _copy_if_outdated,
    _dataset_bucket_value,
    _minio_prefix_from_key,
    _write_dataset_artifact,
)


def _genai_pairs_or_empty(db, folder: str, log=None) -> list[dict]:
    """find_project_genai_pairs 가 없는 백엔드(legacy DuckDB)면 빈 리스트.

    AttributeError 만 graceful 흡수 (legacy 백엔드 호환).
    그 외 예외(DB 연결 / SQL 오류)는 surface — silent swallow 하면 GenAI 폴더가
    일반 path 로 fallthrough 해서 빈 dataset 이 'completed' 로 마킹되는 위험.
    """
    fn = getattr(db, "find_project_genai_pairs", None)
    if not callable(fn):
        return []
    try:
        return fn(folder) or []
    except AttributeError:
        # 마이그레이션 미적용 등 메서드 자체 결손
        return []
    except Exception as exc:
        # DB 연결 오류 등은 명시적으로 재발생 — 일반 path 로 가지 않게 한다.
        if log is not None:
            log.error(f"[{folder}] find_project_genai_pairs 실패: {exc}")
        raise


def _genai_manifest_include_prompt() -> bool:
    """GENAI_MANIFEST_INCLUDE_PROMPT — 1 시 manifest 에 prompt 평문 포함.
    기본 0 (PII 회피, hash 만 저장)."""
    return os.getenv("GENAI_MANIFEST_INCLUDE_PROMPT", "0") == "1"


def _prompt_hash(prompt: str | None) -> str | None:
    if not prompt:
        return None
    import hashlib

    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16]


def _build_project_genai(context, db, minio: MinIOResource, folder: str, pairs: list[dict]) -> dict:
    """GenAI fan-out batch 의 paired dataset 빌드 (label-free).

    pairs 의 각 row = 1 batch_id + 1 seq_in_batch + (input image, output {video|image}).
    원본은 vlm-raw 에서 vlm-dataset/<folder_prefix>/images/<seq>.<ext> 로,
    결과는 vlm-dataset/<folder_prefix>/{videos|generated_images}/<seq>.<ext> 로 copy.

    실패 정책:
      - copy 실패 / output_media mismatch → skipped_pairs 누적, manifest 에 기록
      - 최종 성공 pair 0 건 → build_status='failed', dataset_id 반환
      - skipped > 0 & 일부 성공 → build_status='partial'
    """
    log = context.log

    # ---- 1. folder_prefix 추출 + invariant 검사 ----
    sample_input_key = pairs[0]["input_raw_key"]
    folder_prefix = _minio_prefix_from_key(sample_input_key)
    if not folder_prefix:
        log.error(f"[{folder}] genai input_raw_key 에서 folder prefix 추출 실패 — skip")
        return {"folder": folder, "error": "no_folder_prefix"}

    # 모든 pair 가 같은 folder_prefix 인지 확인 (서로 다른 batch 가 같은 source_unit_name 으로
    # 들어왔을 때 cross-contamination 방어)
    foreign = [p["job_id"] for p in pairs if _minio_prefix_from_key(p["input_raw_key"]) != folder_prefix]
    if foreign:
        log.error(
            f"[{folder}] folder_prefix invariant 위반: {len(foreign)} pairs "
            f"prefix != {folder_prefix!r} (e.g. {foreign[:3]}) — skip"
        )
        return {"folder": folder, "error": "folder_prefix_mismatch", "foreign_count": len(foreign)}

    # ---- 2. batch metadata 수집 (config 보강용) ----
    batch_ids = sorted({p["batch_id"] for p in pairs})
    engines = sorted({p.get("engine") for p in pairs if p.get("engine")})
    prompt_hashes = sorted({_prompt_hash(p.get("prompt")) for p in pairs if p.get("prompt")})

    log.info(
        f"[{folder}] GenAI pair build: pairs={len(pairs)} prefix={folder_prefix} batches={batch_ids} engines={engines}"
    )

    # ---- 3. dataset row 등록 (Phase 3-D lineage 메타 포함) ----
    dataset_id = str(uuid4())
    config_dict = {
        "schema": "genai_paired",
        "label_free": True,
        "with_timestamp": False,
        "with_bbox": False,
        "batch_ids": batch_ids,
        "engines": engines,
        "prompt_hashes": prompt_hashes,
        "source_unit_name": folder,
    }
    db.insert_dataset(
        {
            "dataset_id": dataset_id,
            "name": folder,
            "version": "v1",
            "config": json.dumps(config_dict),
            "split_ratio": None,
            "dataset_bucket": _dataset_bucket_value(),
            "dataset_prefix": folder_prefix,
            "build_status": "building",
            **make_build_lineage(config_dict),
        }
    )

    # ---- 4. pair 단위 copy ----
    include_prompt = _genai_manifest_include_prompt()
    pair_entries: list[dict] = []
    skipped_entries: list[dict] = []
    image_copies_new = 0
    output_copies_new = 0
    output_video_count = 0
    output_image_count = 0
    for pair in pairs:
        seq = int(pair["seq_in_batch"])
        input_raw_bucket = pair["input_raw_bucket"] or RAW_BUCKET
        input_raw_key = pair["input_raw_key"]
        output_raw_bucket = pair["output_raw_bucket"] or RAW_BUCKET
        output_raw_key = pair["output_raw_key"]

        in_ext = PurePosixPath(input_raw_key).suffix or ".png"
        out_ext = PurePosixPath(output_raw_key).suffix or ".bin"

        # output_media: batch.output_media (video|image)
        output_media = (pair.get("output_media") or "").lower()
        out_dir = _GENAI_OUTPUT_DIR.get(output_media)
        if not out_dir:
            log.error(f"[{folder}] pair {seq}: unknown output_media={output_media!r} — skip")
            skipped_entries.append(
                {"job_id": pair["job_id"], "seq": seq, "reason": f"unknown_output_media:{output_media}"}
            )
            continue

        # batch.output_media vs raw_files.media_type cross-validation
        actual_media = (pair.get("output_media_type") or "").lower()
        if actual_media and actual_media != output_media:
            log.error(
                f"[{folder}] pair {seq}: output_media mismatch "
                f"batch={output_media!r} vs raw_files.media_type={actual_media!r} — skip"
            )
            skipped_entries.append(
                {
                    "job_id": pair["job_id"],
                    "seq": seq,
                    "reason": f"output_media_mismatch:{output_media}!={actual_media}",
                }
            )
            continue

        seq_str = f"{seq:03d}"
        dataset_image_key = f"{folder_prefix}/images/{seq_str}{in_ext}"
        dataset_output_key = f"{folder_prefix}/{out_dir}/{seq_str}{out_ext}"

        try:
            if _copy_if_outdated(minio, input_raw_bucket, input_raw_key, dataset_image_key, log):
                image_copies_new += 1
            if _copy_if_outdated(minio, output_raw_bucket, output_raw_key, dataset_output_key, log):
                output_copies_new += 1
        except Exception as exc:
            log.error(f"[{folder}] genai pair {seq} copy 실패: {exc}")
            skipped_entries.append(
                {"job_id": pair["job_id"], "seq": seq, "reason": f"copy_failed:{type(exc).__name__}"}
            )
            continue

        if output_media == "video":
            output_video_count += 1
        else:
            output_image_count += 1

        entry = {
            "pair_id": pair["job_id"],
            "batch_id": pair["batch_id"],
            "seq": seq,
            "source_key": dataset_image_key,
            "generated_key": dataset_output_key,
            "engine": pair.get("engine"),
            "prompt_hash": _prompt_hash(pair.get("prompt")),
            "label_free": True,
            "provider_job_id": pair.get("provider_job_id"),
            "cost_units": pair.get("cost_units"),
            "source_raw_bucket": input_raw_bucket,
            "source_raw_key": input_raw_key,
            "output_raw_bucket": output_raw_bucket,
            "output_raw_key": output_raw_key,
        }
        if include_prompt:
            entry["prompt"] = pair.get("prompt")
        pair_entries.append(entry)

    # ---- 5. build_status 결정 ----
    if not pair_entries:
        # 전부 실패 — manifest 작성 후 failed 마킹
        final_status = "failed"
    elif skipped_entries:
        final_status = "partial"
    else:
        final_status = "completed"

    # ---- 6. manifest.json ----
    manifest = {
        "project": folder,
        "folder_prefix": folder_prefix,
        "schema": "genai_paired",
        "schema_version": 1,
        "label_free": True,
        "built_at": datetime.utcnow().isoformat() + "Z",
        "bucket": _dataset_bucket_value(),
        "prefix": folder_prefix,
        "build_status": final_status,
        "counts": {
            "pairs": len(pair_entries),
            "images": len(pair_entries),
            "videos": output_video_count,
            "generated_images": output_image_count,
            "skipped": len(skipped_entries),
        },
        "batch_ids": batch_ids,
        "engines": engines,
        "pairs": pair_entries,
        "skipped_pairs": skipped_entries,
    }
    _write_dataset_artifact(
        minio,
        f"{folder_prefix}/manifest.json",
        json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        "application/json",
    )

    db.update_dataset_status(dataset_id, final_status)

    summary = {
        "folder": folder,
        "folder_prefix": folder_prefix,
        "dataset_id": dataset_id,
        "schema": "genai_paired",
        "build_status": final_status,
        "pairs": len(pair_entries),
        "skipped": len(skipped_entries),
        "videos": output_video_count,
        "generated_images": output_image_count,
        "image_copies_new": image_copies_new,
        "output_copies_new": output_copies_new,
    }
    log.info(f"[{folder}] genai {final_status}: {summary}")
    return summary
