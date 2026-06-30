"""여러 프로젝트 COCO 데이터셋 병합 (Tier 2: AI 엔지니어가 N개 DVC 데이터셋 조합). L1 순수.

union 기본 + 선택적 class allowlist(쓸 클래스만) / remap(동의어 통합·개명).
이미지/어노테이션 id 는 소스 간 충돌 방지 위해 재번호; 각 이미지에 source 태그 → provenance.
카테고리는 (remap 후) 이름으로 통합 — 두 프로젝트의 같은 클래스가 한 category 로.

⚠️ lib 계층 규칙: dagster/resources/ops import 금지 (순수 stdlib).
# ponytail: dedup 미포함 — 프로젝트 disjoint 가정. 동일 프레임이 여러 프로젝트에 겹치면
#   image file_name 기준 dedup 추가 (충돌 관측 시). 지금은 YAGNI.
"""
from __future__ import annotations

from typing import Any


def merge_coco(
    sources: list[tuple[str, dict[str, Any]]],
    *,
    class_allowlist: list[str] | None = None,
    class_remap: dict[str, str] | None = None,
) -> tuple[dict[str, Any], dict[str, dict[str, int]]]:
    """sources=[(name, coco_dict), ...] → (merged_coco, provenance).

    merged_coco: {images, annotations, categories} — id 재번호, category 는 이름으로 통합.
    provenance: {source_name: {"images": n, "annotations": n_kept}} + 최종 클래스는 categories 로.
    allowlist 주면 그 클래스(이름) 어노테이션만 유지(이미지는 유지=negative 허용). remap 은 allowlist 검사 전 적용.
    """
    remap = class_remap or {}
    allow = set(class_allowlist) if class_allowlist is not None else None
    cat_id_by_name: dict[str, int] = {}
    out_imgs: list[dict] = []
    out_anns: list[dict] = []
    prov: dict[str, dict[str, int]] = {}
    next_img = 1
    next_ann = 1

    for name, coco in sources:
        # 이 소스의 old category_id → (remap 적용) 통합 이름
        name_by_oldcat = {c["id"]: remap.get(c["name"], c["name"]) for c in coco.get("categories", [])}
        img_id_map: dict[Any, int] = {}
        n_img = 0
        n_ann = 0
        n_orphan = 0
        for img in coco.get("images", []):
            nid = next_img
            next_img += 1
            img_id_map[img["id"]] = nid
            out_imgs.append({**img, "id": nid, "source": name})
            n_img += 1
        for ann in coco.get("annotations", []):
            cname = name_by_oldcat.get(ann["category_id"])
            if cname is None:
                continue
            if allow is not None and cname not in allow:
                continue
            new_img_id = img_id_map.get(ann["image_id"])
            if new_img_id is None:
                # orphan: image_id 가 이 소스 images 에 없음(malformed/외부 exporter) → crash 대신 skip+count.
                n_orphan += 1
                continue
            if cname not in cat_id_by_name:
                cat_id_by_name[cname] = len(cat_id_by_name) + 1
            out_anns.append({
                **ann,
                "id": next_ann,
                "image_id": new_img_id,
                "category_id": cat_id_by_name[cname],
            })
            next_ann += 1
            n_ann += 1
        prov[name] = {"images": n_img, "annotations": n_ann, "orphan_annotations": n_orphan}

    categories = [{"id": cid, "name": cname} for cname, cid in sorted(cat_id_by_name.items(), key=lambda kv: kv[1])]
    return {"images": out_imgs, "annotations": out_anns, "categories": categories}, prov
