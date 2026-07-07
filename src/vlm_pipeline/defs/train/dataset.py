"""TRAIN snapshot builder @asset — frozen, sealed, immutable train dataset versions.

Layer 4: Dagster @asset. Delegates pure logic to lib/dataset_split + lib/trainset_manifest
(L1-2) and DB access to PostgresTrainMixin (L1 resource). Builds a group-aware,
stratified, deterministic 3-way split frozen to vlm-dataset/_trainsets/<id>/ and
records a train_dataset_versions row. Idempotent on (task, content_checksum).

No model-derived labels are ever used (design §2). AL contribution = AL-queue ∩
image_label_annotations; honestly 0 today.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from datetime import datetime
from uuid import uuid4

from dagster import Field, asset

from vlm_pipeline.lib.checksum import sha256_bytes
from vlm_pipeline.lib.coco_merge import merge_coco
from vlm_pipeline.lib.dataset_split import SPLIT_NAMES, _per_class_floor_ok, _split_groups
from vlm_pipeline.lib.dvc_pull import build_dvc_get_argv
from vlm_pipeline.lib.trainset_manifest import (
    build_manifest,
    content_checksum,
    coco_bbox_to_yolo,
)
from vlm_pipeline.resources.minio import MinIOResource
from vlm_pipeline.resources.postgres import PostgresResource

DATASET_BUCKET = "vlm-dataset"
TRAINSETS_PREFIX = "_trainsets"
_DEFAULT_RATIOS = {"train": 0.8, "val": 0.1, "test": 0.1}


def _yolo_label_text(rows_for_image: list[dict], class_map: dict, img_w: int, img_h: int) -> str:
    lines: list[str] = []
    for r in rows_for_image:
        cls = class_map.get(str(r["category"]))
        if cls is None:
            continue
        yolo = coco_bbox_to_yolo([r["bbox_x"], r["bbox_y"], r["bbox_w"], r["bbox_h"]], img_w, img_h)
        if not yolo:
            continue
        xc, yc, ww, hh = yolo
        lines.append(f"{cls} {xc:.6f} {yc:.6f} {ww:.6f} {hh:.6f}")
    return "\n".join(lines) + "\n"


def _run_build_trainset(
    db,
    minio,
    *,
    task: str,
    folder_name: str | None,
    ratios: dict,
    seed: int,
    group_key_field: str,
    min_per_split: int,
    force_new: bool,
    log,
) -> dict:
    # ---- 1. candidates (LS finalized boxes; AL∩annotations honest count) ----
    candidates = db.find_sam3_finalized_bbox_candidates(folder_name=folder_name)
    ls_count = len(candidates)
    image_ids = sorted({str(c["image_id"]) for c in candidates})
    al_confirmed = db.find_al_confirmed_image_ids(image_ids)
    al_confirmed_count = len(al_confirmed)
    log.info(
        f"[trainset] task={task} folder={folder_name} ls_boxes={ls_count} "
        f"images={len(image_ids)} al_confirmed={al_confirmed_count}"
    )
    if not candidates:
        return {
            "skipped_duplicate": False,
            "empty": True,
            "ls_count": 0,
            "al_confirmed_count": 0,
            "total_count": 0,
            "per_class_counts": {},
            "content_checksum": None,
            "train_dataset_version_id": None,
        }

    # ---- 2. stable pre-sort + class_map (sorted category -> contiguous idx) ----
    candidates = sorted(candidates, key=lambda c: (str(c["image_id"]), int(c.get("box_index", 0))))
    classes = sorted({str(c["category"]) for c in candidates})
    class_map = {name: idx for idx, name in enumerate(classes)}

    # ---- 3. group-aware 3-way split (per image_id-level record, grouped by source) ----
    # collapse boxes to per-image records for splitting, carry category for stratify
    per_image: dict[str, dict] = {}
    for c in candidates:
        rec = per_image.setdefault(
            str(c["image_id"]),
            {"image_id": str(c["image_id"]), "group": str(c[group_key_field]), "categories": set()},
        )
        rec["categories"].add(str(c["category"]))
    image_records = [per_image[i] for i in sorted(per_image.keys())]

    splits = _split_groups(
        image_records, key_fn=lambda r: r["group"], ratios=ratios, seed=seed
    )

    # ---- 4. per-class stratify floor (fail honestly if rare class starved) ----
    # explode by category so a multi-class image counts toward each of its classes
    def _explode(rows):
        out = []
        for r in rows:
            for cat in sorted(r["categories"]):
                out.append({"image_id": r["image_id"], "category": cat})
        return out

    exploded = {name: _explode(rows) for name, rows in splits.items()}
    ok, per_class_counts = _per_class_floor_ok(
        exploded, class_fn=lambda r: r["category"], min_per_split=min_per_split
    )
    if not ok:
        raise ValueError(
            f"stratify floor violated: a class has < {min_per_split} examples in some split. "
            f"per_class_counts={json.dumps(per_class_counts)}"
        )

    # ---- 5. split assignment map (image_id -> split) ----
    split_assignment: dict[str, str] = {}
    for name, rows in splits.items():
        for r in rows:
            split_assignment[r["image_id"]] = name

    # ---- 6. content_checksum over (sorted manifest + class_map + split + seed) ----
    boxes_by_image: dict[str, list[dict]] = {}
    for c in candidates:
        boxes_by_image.setdefault(str(c["image_id"]), []).append(c)
    # manifest = sorted (image_key, sha256(image_key + sorted box payload)) — stable
    objects: list[tuple[str, str]] = []
    for c in candidates:
        payload = json.dumps(
            {
                "k": c["image_key"],
                "cat": c["category"],
                "bi": int(c.get("box_index", 0)),
                "b": [c["bbox_x"], c["bbox_y"], c["bbox_w"], c["bbox_h"]],
            },
            sort_keys=True,
        ).encode("utf-8")
        objects.append((f"{c['image_key']}#{c.get('box_index', 0)}", sha256_bytes(payload)))
    manifest_objs = build_manifest(objects)
    checksum = content_checksum(task, manifest_objs, class_map, split_assignment, seed)

    # ---- 7. idempotency gate ----
    if not force_new and db.train_dataset_version_exists(task, checksum):
        log.info(f"[trainset] duplicate content_checksum={checksum} — no-op")
        return {
            "skipped_duplicate": True,
            "empty": False,
            "ls_count": ls_count,
            "al_confirmed_count": al_confirmed_count,
            "total_count": len(image_records),
            "per_class_counts": per_class_counts,
            "content_checksum": checksum,
            "train_dataset_version_id": None,
        }

    # ---- 8. write-then-seal to vlm-dataset/_trainsets/<id>/ ----
    version_id = str(uuid4())
    root = f"{TRAINSETS_PREFIX}/{version_id}"
    # 8a. YOLO label .txt per image, under labels/<split>/<image_id>.txt
    #     (image WxH not in image_metadata candidate row -> recorded as 0; the
    #      trainer reads actual WxH at load time. Labels emitted only when WxH known.)
    for image_id, split_name in split_assignment.items():
        rows = boxes_by_image.get(image_id, [])
        # WxH unknown here -> emit class-only placeholder label is wrong; instead store
        # raw COCO boxes for the trainer adapter to normalize. Persist per-image JSON.
        coco_payload = {
            "image_id": image_id,
            "image_key": rows[0]["image_key"] if rows else None,
            "boxes": [
                {
                    "category": r["category"],
                    "class_index": class_map[str(r["category"])],
                    "bbox": [r["bbox_x"], r["bbox_y"], r["bbox_w"], r["bbox_h"]],
                }
                for r in rows
            ],
        }
        minio.upload_json(DATASET_BUCKET, f"{root}/labels/{split_name}/{image_id}.json", coco_payload)
    # 8b. split assignment + manifest + class_map
    split_assignment_key = f"{root}/splits/split_assignment.json"
    manifest_key = f"{root}/manifest.json"
    minio.upload_json(DATASET_BUCKET, split_assignment_key, split_assignment)
    minio.upload_json(
        DATASET_BUCKET,
        manifest_key,
        {
            "train_dataset_version_id": version_id,
            "task": task,
            "class_map": class_map,
            "split_ratios": ratios,
            "seed": seed,
            "content_checksum": checksum,
            "objects": manifest_objs["objects"],
            "count": manifest_objs["count"],
        },
    )
    # 8c. SEAL marker LAST (presence == sealed/immutable)
    minio.upload(DATASET_BUCKET, f"{root}/SEALED", b"sealed\n", "text/plain")

    # ---- 9. insert train_dataset_versions row ----
    db.insert_train_dataset_version(
        {
            "train_dataset_version_id": version_id,
            "created_at": datetime.utcnow(),
            "task": task,
            "source_spec": {
                "folder_name": folder_name,
                "ls_finalized": True,
                "al_intersect_annotations": True,
                "ratios": ratios,
            },
            "class_map": class_map,
            "group_key_field": group_key_field,
            "split_assignment_key": split_assignment_key,
            "split_ratios": ratios,
            "manifest_key": manifest_key,
            "content_checksum": checksum,
            "ls_count": ls_count,
            "al_confirmed_count": al_confirmed_count,
            "per_class_counts": per_class_counts,
            "total_count": len(image_records),
            "seed": seed,
            "upstream_dataset_id": None,
        }
    )
    log.info(f"[trainset] sealed version={version_id} checksum={checksum} images={len(image_records)}")
    return {
        "skipped_duplicate": False,
        "empty": False,
        "ls_count": ls_count,
        "al_confirmed_count": al_confirmed_count,
        "total_count": len(image_records),
        "per_class_counts": per_class_counts,
        "content_checksum": checksum,
        "train_dataset_version_id": version_id,
    }


@asset(
    description=(
        "Build a frozen, sealed, immutable train_dataset_versions snapshot from "
        "LS-finalized bbox annotations (+ AL∩annotations, honest 0 today). "
        "Group-aware deterministic 3-way split + per-class stratify floor + "
        "content_checksum idempotency. Writes vlm-dataset/_trainsets/<id>/."
    ),
    group_name="train",
    config_schema={
        "task": Field(str, default_value="sam3_detection", is_required=False),
        "folder": Field(str, is_required=False),
        "seed": Field(int, default_value=42, is_required=False),
        "group_key_field": Field(str, default_value="source_asset_id", is_required=False),
        "min_per_split": Field(int, default_value=1, is_required=False),
        "force_new": Field(bool, default_value=False, is_required=False),
        # Tier 2 (AI 엔지니어): sources 주면 PG-folder 대신 'pinned DVC 데이터셋 N개 조합' 경로.
        # 예: sources=["source-d:current","incheon:current"], class_remap=["flame=fire"].
        "sources": Field([str], default_value=[], is_required=False),
        "class_allowlist": Field([str], default_value=[], is_required=False),
        "class_remap": Field([str], default_value=[], is_required=False),
    },
)
def build_trainset(
    context,
    db: PostgresResource,
    minio: MinIOResource,
) -> dict:
    cfg = context.op_config or {}
    ratios = json.loads(os.getenv("TRAINSET_SPLIT_RATIOS", json.dumps(_DEFAULT_RATIOS)))
    sources_cfg = cfg.get("sources") or []
    if sources_cfg:
        # Tier 2: "task" 또는 "task:alias" → {task, alias}; "old=new" → remap.
        sources = []
        for s in sources_cfg:
            t, _, a = str(s).partition(":")
            sources.append({"task": t, "alias": a or "current"})
        remap = {}
        for r in cfg.get("class_remap") or []:
            old, _, new = str(r).partition("=")
            if old and new:
                remap[old] = new
        summary = _run_build_trainset_from_dvc(
            db, minio,
            sources=sources,
            task=cfg.get("task", "sam3_detection"),
            seed=int(cfg.get("seed", 42)),
            split_ratios=ratios,
            group_key_field=cfg.get("group_key_field") or None,
            class_allowlist=(cfg.get("class_allowlist") or None),
            class_remap=(remap or None),
            force_new=bool(cfg.get("force_new", False)),
            log=context.log,
        )
    else:
        summary = _run_build_trainset(
            db,
            minio,
            task=cfg.get("task", "sam3_detection"),
            folder_name=cfg.get("folder"),
            ratios=ratios,
            seed=int(cfg.get("seed", 42)),
            group_key_field=cfg.get("group_key_field", "source_asset_id"),
            min_per_split=int(cfg.get("min_per_split", 1)),
            force_new=bool(cfg.get("force_new", False)),
            log=context.log,
        )
    # scalar 만 metadata 로 (dict/list 값은 Dagster metadata 타입 에러 회피).
    context.add_output_metadata(
        {k: v for k, v in summary.items() if isinstance(v, (str, int, float, bool))}
    )
    return summary


# ── DVC curation-source hook (Section J — opt-in pinned dataset → frozen snapshot) ──


def _default_dvc_get(argv: list[str]) -> None:
    subprocess.run(argv, check=True)


def _materialize_pinned_dvc_source(db, *, task, alias="current", dest_root, dvc_get=None):
    """If a DVC alias is pinned for `task`, `dvc get` it into dest_root and return its back-link.

    Returns {"dataset_catalog_id", "git_rev", "dvc_out_path", "local_path"} or None when no
    alias is pinned (the builder then keeps its existing LS/AL candidate-query path — DVC
    source is OPT-IN). The pulled bytes feed split/freeze; the returned dataset_catalog_id is
    written to train_dataset_versions.dataset_catalog_id (J1 FK) so the frozen snapshot links
    back to its curation source (spec §7.6 step 4). dvc_get is injected so CI never runs dvc.
    """
    row = db.get_catalog_by_alias(task, alias)
    if row is None:
        return None
    dvc_get = dvc_get or _default_dvc_get
    repo_path = os.environ.get("DVC_DATA_REPO_PATH", "/srv/data-repos/dvc-datasets.git")
    out_path = row["dvc_out_path"]
    local_path = os.path.join(dest_root, out_path)
    argv = build_dvc_get_argv(repo_path, out_path, row["git_rev"], local_path)
    dvc_get(argv)
    return {
        "dataset_catalog_id": row["dataset_catalog_id"],
        "git_rev": row["git_rev"],
        "dvc_out_path": out_path,
        "local_path": local_path,
    }


# ── Tier 2: 여러 프로젝트 DVC COCO 데이터셋 조합 (AI 엔지니어) ──

_COCO_FILENAME = "coco.json"  # ponytail: 데이터 엔지니어 COCO 데이터셋 out 루트 표준 파일명 (override 가능)


def assemble_multi_project_coco(
    db,
    *,
    sources,
    dest_root,
    class_allowlist=None,
    class_remap=None,
    dvc_get=None,
    coco_filename=_COCO_FILENAME,
):
    """여러 프로젝트의 pinned DVC COCO 데이터셋을 조합 (Tier 2, design §8 멀티소스).

    sources=[{"task": <project>, "alias": "current"}, ...]. 각 소스: get_catalog_by_alias 로 pin 해석
    → dataset_pull(dvc get, 주입형) 로 로컬 확보 → <local_path>/coco.json 읽기 → merge_coco 로 합침.
    pin 없는 프로젝트는 skip 하고 source_spec.missing 에 기록(조용한 누락 금지). class_allowlist/remap 은
    union/선택 조합을 위해 merge_coco 로 위임. dvc_get 주입 → CI mock(실 dvc 미실행).

    Returns (merged_coco, source_spec). source_spec = {sources:[{task,catalog_id,git_rev,dvc_out_path,
    gt_source}], missing, provenance, class_allowlist, class_remap} → train_dataset_versions.source_spec
    로 lineage 기록.

    ⚠️ Trust boundary (M-4): every source here is a DVC-committed COCO file — trusted as
    human-curated GT because it only reaches DVC via the human curation/review gate upstream
    (LS finalize → export → dvc add/commit), NOT because this function re-runs the SQL
    model-derived-label exclusion that the single-project path (_run_build_trainset) applies.
    The no-self-training invariant on THIS path is therefore enforced by process (who is
    allowed to `dvc add`+commit into the curated repo), not by a query filter. Each entry is
    tagged gt_source='dvc_curated' so this trust boundary is visible in the recorded lineage,
    not just in a docstring.
    """
    pulled = []
    spec_sources = []
    missing = []
    for s in sources:
        task = s["task"]
        alias = s.get("alias", "current")
        src = _materialize_pinned_dvc_source(db, task=task, alias=alias, dest_root=dest_root, dvc_get=dvc_get)
        if src is None:
            missing.append({"task": task, "alias": alias})
            continue
        with open(os.path.join(src["local_path"], coco_filename), encoding="utf-8") as fh:
            coco = json.load(fh)
        pulled.append((task, coco))
        spec_sources.append({
            "task": task,
            "dataset_catalog_id": src["dataset_catalog_id"],
            "git_rev": src["git_rev"],
            "dvc_out_path": src["dvc_out_path"],
            "gt_source": "dvc_curated",
        })
    merged, provenance = merge_coco(pulled, class_allowlist=class_allowlist, class_remap=class_remap)
    source_spec = {
        "sources": spec_sources,
        "missing": missing,
        "provenance": provenance,
        "class_allowlist": class_allowlist,
        "class_remap": class_remap,
    }
    return merged, source_spec


def freeze_multi_project_trainset(
    db,
    minio,
    *,
    merged_coco,
    source_spec,
    task,
    seed=42,
    split_ratios=None,
    group_key_field=None,
    force_new=False,
    min_per_split=1,
    log=None,
):
    """assemble 한 merged COCO 를 불변 train_dataset_versions 스냅샷으로 동결 (Tier 2 freeze).

    이미지 바이트는 재복사 안 함 — DVC 데이터셋이 이미 MinIO 에 content-addressed 정본
    (source_spec 으로 재현). 여기선 manifest+split+content_checksum+source_spec(멀티프로젝트 lineage)
    만 동결하고, merged COCO json 을 _trainsets/<id>/coco.json 에 올린다. (task, content_checksum) 멱등.

    dataset_catalog_id (H-1): train_dataset_versions 의 FK 컬럼은 정확히 1개 dataset_catalog 행만
    가리킬 수 있다. source_spec["sources"] 가 단일 소스일 때만(unambiguous) 그 소스의
    dataset_catalog_id 를 채운다 — 소스가 여러 개면 FK 는 NULL 로 남기고 source_spec JSON 이
    멀티프로젝트 lineage 의 유일한 source of truth 로 남는다(단일 컬럼으로 N개를 표현 불가).

    per-class stratify floor (L-2): 단일소스 경로(_run_build_trainset)와 달리 Tier-2 는 조합이
    예측 불가(N개 프로젝트 union) 하므로 floor 위반을 하드 fail 하지 않는다 — 합법적인 rare-class
    조합을 조기 차단하지 않기 위해 WARN 로그 + starved_classes 를 결과 dict 에 기록만 한다.
    """
    split_ratios = split_ratios or {"train": 0.8, "val": 0.1, "test": 0.1}
    images = merged_coco.get("images", [])
    # 빈 merge 를 스냅샷으로 봉인하지 않는다 — 모든 source 의 pin 이 없거나 결측이면(source_spec.missing)
    # merged 가 0장이 되는데, 그대로 freeze 하면 total_count=0 인 '정상처럼 보이는' 사용불가 스냅샷이
    # 조용히 생긴다. freeze 는 operator 가 source 리스트로 트리거하는 행위라 loud fail 이 맞다.
    if not images:
        raise ValueError(
            "freeze_multi_project_trainset: merged COCO has 0 images — all sources missing/unpinned? "
            f"missing={source_spec.get('missing')}"
        )
    cats = {c["id"]: c["name"] for c in merged_coco.get("categories", [])}
    class_map = {name: i for i, name in enumerate(sorted(cats.values()))}

    anns_by_img: dict = {}
    per_class_counts: dict = {}
    for a in merged_coco.get("annotations", []):
        anns_by_img.setdefault(a["image_id"], []).append(a)
        cn = cats.get(a["category_id"])
        if cn:
            per_class_counts[cn] = per_class_counts.get(cn, 0) + 1

    def _ukey(im):
        # source-qualified per-image 키 — 프로젝트 간 file_name 충돌 방지 + 결정적 (Codex BUG2).
        return f"{im.get('source', '_')}/{im.get('file_name') or im['id']}"

    def _gkey(im):
        # 그룹 키(leakage 방지): source_asset_id 등 있으면 그걸로 묶고, 없으면 per-image.
        if group_key_field:
            v = im.get(group_key_field)
            if v:
                return str(v)
        return _ukey(im)

    splits = _split_groups(images, _gkey, split_ratios, seed)
    split_assignment: dict = {}
    for sname, rows in splits.items():
        for im in rows:
            split_assignment[_ukey(im)] = sname

    # ---- per-class stratify floor (L-2): WARN + record, never hard-fail (multi-source combos
    # are less predictable than the single-project path's, which does raise on this check). ----
    def _explode_by_category(rows):
        out = []
        for im in rows:
            for a in anns_by_img.get(im["id"], []):
                cn = cats.get(a["category_id"])
                if cn:
                    out.append({"category": cn})
        return out

    exploded_by_split = {sname: _explode_by_category(rows) for sname, rows in splits.items()}
    _floor_ok, _floor_counts = _per_class_floor_ok(
        exploded_by_split, class_fn=lambda r: r["category"], min_per_split=min_per_split
    )
    starved_classes = sorted(
        cls
        for cls, per in _floor_counts.items()
        for sname in SPLIT_NAMES
        if exploded_by_split.get(sname) and per[sname] < min_per_split
    )
    if starved_classes:
        msg = (
            f"[trainset/dvc] per-class stratify floor violated (min_per_split={min_per_split}): "
            f"starved_classes={starved_classes} per_class_counts={json.dumps(_floor_counts)}"
        )
        if log is not None:
            log.warning(msg)
        else:
            print(msg)

    # manifest: per-image (source/file_name, sha256 over sorted box payloads) — stable/deterministic.
    objects = []
    for im in images:
        fk = _ukey(im)
        boxes = sorted(
            json.dumps({"c": cats.get(a["category_id"]), "b": a.get("bbox")}, sort_keys=True)
            for a in anns_by_img.get(im["id"], [])
        )
        objects.append((fk, sha256_bytes("".join(boxes).encode("utf-8"))))
    manifest = build_manifest(objects)
    checksum = content_checksum(task, manifest, class_map, split_assignment, seed)

    if not force_new and db.train_dataset_version_exists(task, checksum):
        return {"skipped_duplicate": True, "content_checksum": checksum, "train_dataset_version_id": None}

    version_id = str(uuid4())
    manifest_key = f"{TRAINSETS_PREFIX}/{version_id}/coco.json"
    minio.upload(DATASET_BUCKET, manifest_key, json.dumps(merged_coco).encode("utf-8"))
    # split_assignment 도 MinIO 에 동결하고 그 '키'를 기록 (Codex BUG1: 리터럴 문자열 금지).
    split_key = f"{TRAINSETS_PREFIX}/{version_id}/split_assignment.json"
    minio.upload(DATASET_BUCKET, split_key, json.dumps(split_assignment, sort_keys=True).encode("utf-8"))
    # H-1: FK is unambiguous only for exactly one source — multi-source lineage stays in
    # source_spec only (a single column can't represent N catalog rows).
    spec_sources = source_spec.get("sources") or []
    dataset_catalog_id = spec_sources[0].get("dataset_catalog_id") if len(spec_sources) == 1 else None
    db.insert_train_dataset_version({
        "train_dataset_version_id": version_id,
        "task": task,
        "source_spec": source_spec,        # 멀티프로젝트 출처 lineage
        "class_map": class_map,
        "group_key_field": group_key_field or "file_name",
        "split_assignment_key": split_key,
        "split_ratios": split_ratios,
        "manifest_key": manifest_key,
        "content_checksum": checksum,
        "ls_count": 0,
        "al_confirmed_count": 0,
        "per_class_counts": per_class_counts,
        "total_count": len(images),
        "seed": seed,
        "dataset_catalog_id": dataset_catalog_id,
    })
    return {
        "skipped_duplicate": False,
        "content_checksum": checksum,
        "train_dataset_version_id": version_id,
        "total_count": len(images),
        "per_class_counts": per_class_counts,
        "starved_classes": starved_classes,
    }


def _run_build_trainset_from_dvc(
    db,
    minio,
    *,
    sources,
    task,
    seed=42,
    split_ratios=None,
    group_key_field=None,
    class_allowlist=None,
    class_remap=None,
    force_new=False,
    dvc_get=None,
    dest_root=None,
    log=None,
):
    """Tier 2 DVC-sources 경로: pinned 프로젝트 데이터셋 N개 → assemble → freeze (불변 학습셋)."""
    dest_root = dest_root or tempfile.mkdtemp(prefix="trainset_assemble_")
    merged, source_spec = assemble_multi_project_coco(
        db, sources=sources, dest_root=dest_root,
        class_allowlist=class_allowlist or None, class_remap=class_remap or None, dvc_get=dvc_get,
    )
    if log:
        log.info(
            "[trainset/dvc] sources=%d missing=%d images=%d",
            len(source_spec["sources"]), len(source_spec["missing"]), len(merged.get("images", [])),
        )
    out = freeze_multi_project_trainset(
        db, minio, merged_coco=merged, source_spec=source_spec, task=task,
        seed=seed, split_ratios=split_ratios, group_key_field=group_key_field, force_new=force_new,
        log=log,
    )
    out["sources"] = len(source_spec["sources"])
    out["missing_sources"] = len(source_spec["missing"])
    return out
