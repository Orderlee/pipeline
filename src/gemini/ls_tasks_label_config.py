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
    # 2026-05-29: sourcej_v2 dispatch 카테고리 매핑 추가.
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


# ---------------------------------------------------------------------------
# 합성(genai) 배치 provenance — 검수자가 실사 CCTV 와 구분할 수 있게 한다.
#
# 2026-09-21 실측: comfy_local 합성본이 LS 프로젝트 823 까지 도달했지만, `src/gemini/` 와
# `defs/ls/` 어디에도 `synthetic`/`comfy_local` 참조가 없어 **검수자 화면에서 실사 CCTV 와
# 구분할 수단이 전혀 없었다.** 합성본을 실사로 오인한 검수는 학습셋 오염으로 직결된다
# (설계서 Phase D 의 목적 자체가 그 오인 방지다).
#
# 두 갈래로 싣는다:
#   data 필드  — `synthetic`/`genai_engine`/`genai_batch_id`/`intended_event` (기계 판독용.
#                LS export·API 로 그대로 나온다) + `provenance` (사람이 읽는 한 줄)
#   label_config — 위 `provenance` 를 띄우는 배너 + 설계서 Phase D.3 의 4개 판정 체크리스트
#
# **합성 배치에만** 붙는다. `synthetic=False` 가 기본이고 그 경로의 출력은 이 변경 이전과
# 바이트 단위로 같다 — 실사 프로젝트의 label_config 는 건드리지 않는다.
# ---------------------------------------------------------------------------

SYNTHETIC_QC_NAME = "synthetic_qc"

# 설계서 Phase D.3 의 4개 판정. 순서·번호를 바꾸면 이미 쌓인 annotation 과 대조가 안 되므로
# 문구 수정 시 번호는 유지할 것.
SYNTHETIC_QC_CRITERIA: tuple[str, ...] = (
    "1. 요청한 이벤트가 실제로 보인다",
    "2. background·camera geometry 가 보존됐다",
    "3. 마스크 경계·인체·불/연기 artifact 가 허용 가능하다",
    "4. bbox 와 event label 이 실제 생성 결과에 맞는다",
)

_SYNTHETIC_BANNER = "합성 생성본(synthetic) — 실사 CCTV 가 아니다. 아래 출처를 먼저 확인할 것."
_SYNTHETIC_QC_HEADER = "합성 검수 판정 — 통과한 항목만 체크 (미체크 = 불합격)"


def build_synthetic_provenance(
    engine: str | None, batch_id: str | None, categories: list[str] | None
) -> dict[str, str]:
    """합성 배치 task 의 `data` 에 실을 출처 필드.

    값이 비면 `unknown` 으로 채운다 — 키 자체를 빼면 label_config 의 `$provenance` 가 빈 줄로
    렌더되어 "합성인데 표시가 없는 화면"이 되고, 그건 이 기능이 막으려던 상태 그대로다.
    """
    engine_s = (engine or "").strip() or "unknown"
    batch_s = (batch_id or "").strip() or "unknown"
    events = [c.strip() for c in (categories or []) if c and c.strip()]
    event_s = ", ".join(events) if events else "unknown"
    return {
        "synthetic": "true",
        "genai_engine": engine_s,
        "genai_batch_id": batch_s,
        "intended_event": event_s,
        "provenance": f"합성 생성본 · 엔진 {engine_s} · batch {batch_s} · 의도 이벤트 {event_s}",
    }


def _synthetic_banner_xml() -> str:
    return f'  <Header value="{_SYNTHETIC_BANNER}"/>\n' '  <Text name="provenance" value="$provenance"/>\n'


def _synthetic_qc_xml(to_name: str) -> str:
    choices = "\n".join(f'    <Choice value="{c}"/>' for c in SYNTHETIC_QC_CRITERIA)
    return (
        f'  <Header value="{_SYNTHETIC_QC_HEADER}"/>\n'
        f'  <Choices name="{SYNTHETIC_QC_NAME}" toName="{to_name}" choice="multiple" showInLine="false">\n'
        f"{choices}\n"
        "  </Choices>\n"
    )


def _video_label_config(categories: list[str], synthetic: bool = False) -> str:
    """video project 전용 — TimelineLabels 만. synthetic=True 면 출처 배너 + 합성 판정 추가."""
    banner = _synthetic_banner_xml() if synthetic else ""
    qc = _synthetic_qc_xml("video") if synthetic else ""
    return f"""<View>
{banner}  <Video name="video" value="$video" timelineHeight="120" />
  <TimelineLabels name="videoLabels" toName="video">
{_labels_xml(categories)}
  </TimelineLabels>
{qc}</View>"""


def _image_label_config(categories: list[str], synthetic: bool = False) -> str:
    """image project 전용 — RectangleLabels 만. synthetic=True 면 출처 배너 + 합성 판정 추가."""
    banner = _synthetic_banner_xml() if synthetic else ""
    qc = _synthetic_qc_xml("image") if synthetic else ""
    return f"""<View>
{banner}  <Image name="image" value="$image" />
  <RectangleLabels name="imageLabels" toName="image">
{_labels_xml(categories)}
  </RectangleLabels>
{qc}</View>"""


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
