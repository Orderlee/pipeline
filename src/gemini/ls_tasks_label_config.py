"""Label config builders, category normalization, and LS project config helpers."""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET

import requests

_LABEL_PALETTE = [
    "#e74c3c",
    "#e67e22",
    "#f39c12",
    "#16a085",
    "#2980b9",
    "#8e44ad",
    "#7f8c8d",
    "#27ae60",
]

# dispatch canonical category → 동의어 집합 (lowercase). Gemini/SAM3 의 raw prediction 을
# dispatch 가 요구한 라벨로 정규화한다. 매핑 안 되는 카테고리는 prediction 에서 drop
# (리뷰어에게 노이즈 라벨이 섞이지 않도록).
# 운영 중 새 Gemini/SAM3 동의어가 나오면 여기에 추가.
CATEGORY_SYNONYMS: dict[str, set[str]] = {
    "falldown": {
        "falldown",
        "fall",
        "simulated_fall",
        "fall_simulation",
        "intentional_fall_simulation",
        "fall_recovery_drill",
        "recovery_from_fall_simulation",
        "deliberate_fall_from_wheelchair",
        "fall_recovery",
        "fall_risk",
        "fall_assistance",
        # VHC 의료진이 의도적으로 연출한 낙상 시나리오 — falldown 데이터로 유효.
        "deliberate_lie_down",
        "deliberate_recovery",
        # smart-city 에서 바닥에 쓰러진 사람 묘사 — 낙상 의미.
        "person_lying_on_ground",
        # 2026-06-04: promote 폼 hybrid preset SAM3 자연어 phrase.
        # promote.html JS 의 PRESETS["falldown"].classes 와 sync 필요.
        # 사용자 e00879ff-b04 batch 에서 SAM3 가 40 boxes 잡았는데 LS prediction
        # 으로 import 안 되던 issue 의 원인 — normalizer drop.
        "fallen person",
        "person lying down",
        "person on the ground",
    },
    "person": {"person"},
    "fire": {
        "fire",
        "flame",
        "explosion",
        # 2026-06-04 hybrid preset SAM3 phrase — PRESETS["fire"].classes sync.
        "open flame",
    },
    "smoke": {
        "smoke",
        "smoking",
        "cigarette",
        # 2026-06-04 hybrid preset SAM3 phrase — PRESETS["smoke"].classes sync.
        "smoke cloud",
        # NOTE: 'flame' 은 fire 의 synonym 으로만 유지 (test_ls_category_synonyms
        # 의 test_existing_fire_smoke_person_mappings_intact 호환). 영상에 fire+smoke
        # 공출현 시 사용자가 두 preset 모두 선택해야 함 (의도된 정책).
    },
    # 2026-05-29: vanguardhealthcarevhc_v2 dispatch 카테고리 매핑 추가.
    # 운영 진단으로 normalizer drop 폭주 발견 (515 events 중 18건만 매핑됨).
    # 보수적으로 의미 직접 일치 케이스만 등록. 추가 동의어는 운영자 검토 후 별도 PR.
    "violence": {
        "violence",
        "fight",
        # 2026-06-04 hybrid preset SAM3 phrase — PRESETS["violence"].classes sync.
        "fighting people",
        "punching person",
        "person hitting person",
    },
    "weapon": {
        # 2026-06-04 hybrid preset 신규 canonical. PRESETS["weapon"].classes sync.
        "weapon",
        "gun",
        "knife",
        "baseball bat",
        "bat",
        "sword",
    },
    "climbing up": {"climbing up", "climbing_up", "unsafe_climbing_activity"},
}


def build_label_normalizer(target_cats: list[str]) -> dict[str, str]:
    """target_cats 에 속하는 canonical → synonym 매핑을 뒤집어 {synonym: canonical} 리턴.

    target_cats 에 없는 canonical 은 무시. target_cats 에 있지만 SYNONYMS 테이블에 없는
    canonical 은 자기 자신만 매핑 (identity). 키는 전부 lowercase.
    """
    canonical_set = {c.strip().lower() for c in (target_cats or []) if c}
    normalizer: dict[str, str] = {}
    for canon, synonyms in CATEGORY_SYNONYMS.items():
        if canon not in canonical_set:
            continue
        for s in synonyms:
            normalizer[s.strip().lower()] = canon
    # SYNONYMS 테이블에 없는 canonical 도 자기 자신 매핑.
    for canon in canonical_set:
        normalizer.setdefault(canon, canon)
    return normalizer


def _labels_xml(categories: list[str]) -> str:
    """카테고리 리스트 → `<Label value=.. background=..>` 라인. (dispatch.categories 만 표시, `other` 없음)"""
    cats = [c for c in (categories or []) if c]
    return "\n".join(
        f'    <Label value="{c}" background="{_LABEL_PALETTE[i % len(_LABEL_PALETTE)]}"/>' for i, c in enumerate(cats)
    )


def _default_label_config() -> str:
    return """<View>
  <TimelineLabels name="videoLabels" toName="video">
    <Label value="fall" background="#e74c3c"/>
    <Label value="fight" background="#e67e22"/>
    <Label value="smoke" background="#95a5a6"/>
    <Label value="fire" background="#e74c3c"/>
    <Label value="unsafe_act" background="#f39c12"/>
  </TimelineLabels>
  <Video name="video" value="$video" timelineHeight="120" />
</View>"""


def _video_label_config(categories: list[str]) -> str:
    """video project 전용 — TimelineLabels 만."""
    return f"""<View>
  <Video name="video" value="$video" timelineHeight="120" />
  <TimelineLabels name="videoLabels" toName="video">
{_labels_xml(categories)}
  </TimelineLabels>
</View>"""


def _image_label_config(categories: list[str]) -> str:
    """image project 전용 — RectangleLabels 만."""
    return f"""<View>
  <Image name="image" value="$image" />
  <RectangleLabels name="imageLabels" toName="image">
{_labels_xml(categories)}
  </RectangleLabels>
</View>"""


def _parse_csv_or_json_list(raw: str | None) -> list[str]:
    """'a,b,c' 또는 '["a","b"]' → ['a','b','c'] (lowercase, 중복 제거, 순서 유지)."""
    if not raw:
        return []
    rendered = str(raw).strip()
    if not rendered:
        return []
    values: list[str]
    try:
        if rendered.startswith("["):
            parsed = json.loads(rendered)
            values = [str(v) for v in parsed] if isinstance(parsed, list) else []
        else:
            values = rendered.split(",")
    except Exception:
        values = rendered.split(",")
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        s = v.strip().lower()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def parse_rectangle_labels_config(label_config: str) -> tuple[str, str, set[str]] | None:
    """label_config XML에서 <RectangleLabels> name/toName과 허용 Label value 목록 추출.

    반환: (from_name, to_name, {label_values}) 또는 RectangleLabels 없으면 None.
    """
    try:
        root = ET.fromstring(label_config)
    except ET.ParseError:
        return None
    rect = root.find(".//RectangleLabels")
    if rect is None:
        return None
    from_name = rect.get("name") or ""
    to_name = rect.get("toName") or ""
    values = {el.get("value") for el in rect.findall("Label") if el.get("value")}
    if not from_name or not to_name or not values:
        return None
    return from_name, to_name, values


def fetch_project_label_config(ls_url: str, headers: dict, project_id: int) -> str:
    resp = requests.get(f"{ls_url}/api/projects/{project_id}/", headers=headers)
    resp.raise_for_status()
    return resp.json().get("label_config") or ""
