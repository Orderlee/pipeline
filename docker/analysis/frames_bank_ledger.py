#!/usr/bin/env python3
"""FiftyOne `frames` 데이터셋(구 frames_captions, 2026-08-19 개명) → 뱅크 평가 원장 생산자.

분석기(prompt_geometry.py --profile frames)는 이 출력만 소비하고 DB 를 모른다 —
source-h 의 ledger.jsonl/embed.npz 데이터 계약을 그대로 미러 (스펙 §4·§5-2).

GT: image_id → image_labels(review_status='finalized') **좌조인** + annotations.category
    → crosswalk(fail-closed). 무박스 finalized = '__no_box_finalized__' → normal
    (inner join 이 이를 조용히 버리는 기존 QA 쿼리 함정 회피 — codex 지적).
    미등재 category 나 다중 이벤트 클래스 프레임은 gt_class=-1 + 사유 카운트.
SAM3 auto_generated 는 어떤 경우에도 GT 로 쓰지 않는다 (bank_gt 불변식, 스펙 §7).
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import time

import numpy as np
import psycopg2
import yaml

ROOT = "/data/fiftyone/frames_bank"
WORK = f"{ROOT}/work"
DSN = os.environ.get("DATAOPS_POSTGRES_DSN",
                     "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")
MAP_YAML = os.environ.get("BANK_DOMAIN_MAP", "/workspace/bank_domain_map.yaml")
NAME_TO_ID = {"normal": 0, "falldown": 1, "fire": 2, "smoke": 3, "smoking": 4}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_finalized_gt(crosswalk: dict) -> tuple[dict, collections.Counter, int]:
    """image_id(str) → frame class 이름. 좌조인이라 무박스 finalized 도 잡힌다.

    ⚠️ 방어적 이중 확인: WHERE 절이 `review_status = 'finalized'` 하나뿐이라, 누군가 이
    쿼리를 미래에 느슨하게 고치면(예: `IN ('finalized', 'reviewed')`) 이 함수가 조용히
    non-finalized 행을 GT 로 흘려보낼 수 있다. 그래서 SELECT 에 `il.review_status` 를
    중복으로 실어 코드에서 재검증한다 — SQL 필터를 신뢰하지 않고 행 단위로 재확인.
    """
    q = """
    SELECT il.image_id::text, ila.category, il.review_status
    FROM image_labels il
    LEFT JOIN image_label_annotations ila ON ila.image_label_id = il.image_label_id
    WHERE il.review_status = 'finalized'
    """
    cats: dict[str, set] = collections.defaultdict(set)
    n_boxes = 0
    bad_status: collections.Counter = collections.Counter()
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(q)
        for image_id, category, review_status in cur.fetchall():
            if review_status != "finalized":
                bad_status[review_status] += 1
                continue
            cats[image_id].add(category)          # None = 무박스 finalized
            if category is not None:
                n_boxes += 1
    if bad_status:
        # SQL 의 WHERE 절과 행 단위 검증이 어긋났다 — 쿼리가 느슨해졌다는 신호이므로
        # 조용히 걸러내지 않고 fail-closed 한다 (bank_gt 불변식, 스펙 §7).
        raise RuntimeError(
            "fetch_finalized_gt: SQL 이 review_status='finalized' 아닌 행을 반환했다 "
            f"(WHERE 절이 느슨해졌을 가능성) — {dict(bad_status)}"
        )
    gt: dict[str, str] = {}
    excluded: collections.Counter = collections.Counter()
    for image_id, cs in cats.items():
        mapped: set[str] = set()
        bad = False
        for c in cs:
            key = "__no_box_finalized__" if c is None else c
            m = crosswalk.get(key)                # 미등재 = None = fail-closed
            if m is None:
                bad = True
                excluded[key] += 1
            elif m != "normal":
                mapped.add(m)
        if bad:
            continue
        if len(mapped) > 1:                       # 한 프레임 다중 이벤트 — frame 단일클래스 GT 불성립
            excluded["__multi_class__"] += 1
            continue
        gt[image_id] = mapped.pop() if mapped else "normal"
    return gt, excluded, n_boxes


_ALLOWED_GT_SOURCES = {"ls_finalized", None}


def assert_gt_source_pure(rows: list[dict]) -> None:
    """이 원장은 LS finalized GT 전용이다 (bank_gt 불변식, 스펙 §7).

    산업 현장(source-h) 프레임 GT 는 `frames_eval.py` 가 만드는 **별도 원장**
    (gt_source='nas_folder', NAS 폴더명 파생)이고 이 파일과 절대 혼용되면 안 된다.
    실행 시점 self-check 가 유일한 방어선이다 (이 코드는 CI pytest 게이트 밖 — analysis 는
    CI 미대상). 방어 대상은 "미래에 누가 위 SQL/매핑을 느슨하게 고쳐 다른 gt_source 가
    새는 것" 이다.
    """
    bad = collections.Counter(
        r.get("gt_source") for r in rows if r.get("gt_source") not in _ALLOWED_GT_SOURCES
    )
    if bad:
        raise RuntimeError(
            "frames_bank_ledger: gt_source 오염 감지 — LS finalized 전용 원장에 허용되지 "
            f"않는 gt_source 가 섞였다: {dict(bad)} (허용: 'ls_finalized' 또는 None). "
            "nas_folder 계열 GT(frames_eval.py 원장)와 혼용 금지."
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="파일 미기록, 스탬프만 출력")
    args = ap.parse_args()
    import fiftyone as fo
    from fiftyone import ViewField as F

    with open(MAP_YAML, encoding="utf-8") as f:
        m = yaml.safe_load(f) or {}
    crosswalk = m.get("class_crosswalk") or {}
    proj2dom = {p: d for d, cfg in (m.get("domains") or {}).items()
                for p in (cfg.get("projects") or [])}

    gt_by_image, excluded, n_boxes = fetch_finalized_gt(crosswalk)
    observed_at = time.strftime("%Y-%m-%dT%H:%M:%S")

    ds = fo.load_dataset("frames")
    view = ds.match(F("modality") == "frame")     # 캡션 11,978 = 같은 필드의 텍스트 벡터 → 제외 (필수)
    ids = view.values("id")
    image_ids = view.values("image_id")
    projects = view.values("project")
    assets = view.values("asset_id")

    rows = []
    n_gt = 0
    for sid, iid, proj, asset in zip(ids, image_ids, projects, assets):
        g = gt_by_image.get(str(iid)) if iid else None
        gid = NAME_TO_ID.get(g) if g is not None else None
        if g is not None and gid is None:      # crosswalk YAML 오타/신규 클래스 → 이미지 단위 fail-closed
            excluded[f"__unknown_class__:{g}"] += 1
        if gid is not None:
            n_gt += 1
        rows.append({
            "key": sid,
            "image_id": str(iid) if iid else None,
            "project": proj,
            "domain": proj2dom.get(proj),
            "src_video": asset or proj or "unknown",   # 부트스트랩 군집 단위 (iid 아님 방어)
            "gt_class": gid if gid is not None else -1,
            "gt_source": "ls_finalized" if gid is not None else None,
            "gt_observed_at": observed_at if gid is not None else None,
        })

    dom_counts = collections.Counter(r["domain"] for r in rows if r["domain"])
    log(f"[stamp] ledger: frame {len(rows):,} / 매핑 {dict(dom_counts) or '없음(0단계)'} / "
        f"GT 이미지 {n_gt} (box {n_boxes:,}) / crosswalk 제외 {dict(excluded) or '없음'}")

    # 원장 쓰기 직전 self-check — dry-run 에서도 돌려 조기에 잡는다 (CI pytest 게이트 없음).
    assert_gt_source_pure(rows)

    if args.dry_run:
        return

    os.makedirs(WORK, exist_ok=True)
    with open(f"{WORK}/ledger.jsonl", "w", encoding="utf-8") as f:
        for r in rows:                            # 전량 재작성 — 원천이 DB/데이터셋이라 증분 불필요
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    scored = [r["key"] for r in rows if r["domain"]]
    if scored:
        vecs = view.select(scored, ordered=True).values("image_embedding")
        X = np.asarray(vecs, dtype=np.float32)
        np.savez_compressed(f"{WORK}/embed.npz", key=np.array(scored), vec=X)
        log(f"embed.npz: {X.shape}")
    elif os.path.exists(f"{WORK}/embed.npz"):
        os.remove(f"{WORK}/embed.npz")            # 매핑 제거 시 stale 임베딩도 제거

    gt_sha = hashlib.sha256(json.dumps(
        sorted([r["image_id"], r["gt_class"]] for r in rows if r["gt_class"] >= 0)
    ).encode()).hexdigest()[:16]
    with open(f"{WORK}/gt_snapshot.json", "w", encoding="utf-8") as f:
        json.dump({"sha": gt_sha, "n_images": n_gt, "n_boxes": n_boxes,
                   "excluded": dict(excluded),
                   "crosswalk_version": m.get("crosswalk_version"),
                   "gt_observed_at": observed_at}, f, ensure_ascii=False, indent=1)
    log(f"ledger {len(rows):,}행 / gt_snapshot sha={gt_sha}")


if __name__ == "__main__":
    main()
