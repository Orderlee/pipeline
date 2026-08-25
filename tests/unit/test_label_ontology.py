"""라벨 클래스 정본(``src/vlm_pipeline/data/label_ontology.json``) parity 회귀 테스트.

이 파일이 유일한 SoT 이고, 아래 4곳(현재는 3곳 — sql/migrations 는 DB 투영이라
pytest 로 검증하지 않는다)이 정본에서 파생된다:

  1) ``vlm_pipeline.lib.env_utils.CATEGORY_TO_CLASSES``  (dispatch categories -> SAM3 prompt)
  2) ``gemini.ls_tasks.CATEGORY_SYNONYMS``                (LS prediction normalizer)
  3) ``docker/genai/templates/promote.html`` 의 ``const PRESETS = {...}``  (GenAI Studio UI)

드리프트가 다시 생기면(과거: smoke preset 이 flame 을 SAM3 로 보냄, weapon 이 3곳마다 다름)
이 테스트가 CI 에서 막는다. stdlib 만 사용 — 새 서드파티 의존성 없음.

``tests/unit/conftest.py`` 가 이미 ``src/`` 를 ``sys.path`` 에 얹으므로
(``vlm_pipeline`` / ``gemini`` 모듈 재import 캐시까지 정리) 여기서 다시 하지 않는다.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from gemini.ls_tasks import CATEGORY_SYNONYMS
from vlm_pipeline.lib.env_utils import CATEGORY_TO_CLASSES
from vlm_pipeline.lib.yolo_thresholds import YOLO_CLASS_CONFIDENCE_THRESHOLDS

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ONTOLOGY_PATH = _REPO_ROOT / "src" / "vlm_pipeline" / "data" / "label_ontology.json"
_PROMOTE_HTML_PATH = _REPO_ROOT / "docker" / "genai" / "templates" / "promote.html"

with _ONTOLOGY_PATH.open(encoding="utf-8") as _fp:
    _ONTOLOGY = json.load(_fp)

CLASSES: dict[str, dict] = _ONTOLOGY["classes"]


# ---------------------------------------------------------------------------
# promote.html 의 `const PRESETS = {...}` JS 객체 리터럴 파서 (JSON 아님).
# 필드 순서에 의존하지 않도록 각 preset 블록을 중괄호 depth 로 분리한 뒤
# 필드별로 개별 정규식을 적용한다.
# ---------------------------------------------------------------------------


def _find_matching_brace(text: str, open_idx: int) -> int:
    """``text[open_idx]`` 가 ``{`` 일 때 이에 대응하는 ``}`` 의 인덱스를 반환."""
    assert text[open_idx] == "{"
    depth = 0
    for i in range(open_idx, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return i
    raise ValueError("matching '}' not found")


def _parse_preset_fields(block: str) -> dict:
    def get_str(name: str) -> str | None:
        m = re.search(rf"{name}\s*:\s*'([^']*)'", block)
        return m.group(1) if m else None

    def get_list(name: str) -> list[str] | None:
        m = re.search(rf"{name}\s*:\s*\[([^\]]*)\]", block)
        if not m:
            return None
        return re.findall(r"'([^']*)'", m.group(1))

    return {
        "label": get_str("label"),
        "categories": get_list("categories"),
        "classes": get_list("classes"),
        "gemini_desc": get_str("gemini_desc"),
    }


def _parse_promote_html_presets(html_text: str) -> dict[str, dict]:
    marker = "const PRESETS = "
    start = html_text.index(marker) + len(marker)
    while html_text[start] != "{":
        start += 1
    end = _find_matching_brace(html_text, start)
    body = html_text[start + 1 : end]

    key_re = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*:\s*\{")
    presets: dict[str, dict] = {}
    pos = 0
    while True:
        m = key_re.search(body, pos)
        if not m:
            break
        key = m.group(1)
        brace_start = m.end() - 1
        brace_end = _find_matching_brace(body, brace_start)
        block = body[brace_start + 1 : brace_end]
        presets[key] = _parse_preset_fields(block)
        pos = brace_end + 1
    return presets


# ---------------------------------------------------------------------------
# (a) 스키마 무결성
# ---------------------------------------------------------------------------


def test_schema_every_class_has_required_fields():
    required = {"description", "dispatch_category", "detect_phrases", "aliases", "ui_preset"}
    for name, spec in CLASSES.items():
        missing = required - spec.keys()
        assert not missing, f"class {name!r} missing fields: {missing}"
        assert isinstance(spec["description"], str), f"class {name!r} description must be str"
        assert isinstance(spec["dispatch_category"], bool), f"class {name!r} dispatch_category must be bool"
        assert isinstance(spec["detect_phrases"], list), f"class {name!r} detect_phrases must be list"
        assert all(
            isinstance(p, str) for p in spec["detect_phrases"]
        ), f"class {name!r} detect_phrases must be list[str]"
        assert isinstance(spec["aliases"], list), f"class {name!r} aliases must be list"
        assert all(isinstance(a, str) for a in spec["aliases"]), f"class {name!r} aliases must be list[str]"
        assert spec["ui_preset"] is None or isinstance(
            spec["ui_preset"], dict
        ), f"class {name!r} ui_preset must be dict or null"


def test_schema_descriptions_non_empty():
    """class_5 같은 익명 클래스(설명 없이 방치) 재발 방지."""
    blank = [name for name, spec in CLASSES.items() if not spec["description"].strip()]
    assert not blank, f"description 이 비어있는 클래스: {blank}"


def test_schema_canonical_names_lowercase_and_unique():
    names = list(CLASSES.keys())
    assert len(names) == len(set(names)), "canonical 이름 중복"
    not_lower = [n for n in names if n != n.lower()]
    assert not not_lower, f"lowercase 아닌 canonical 이름: {not_lower}"


def test_schema_detect_phrases_lowercase_no_dupes():
    for name, spec in CLASSES.items():
        phrases = spec["detect_phrases"]
        not_lower = [p for p in phrases if p != p.lower()]
        assert not not_lower, f"class {name!r} detect_phrases 에 lowercase 아닌 항목: {not_lower}"
        assert len(phrases) == len(set(phrases)), f"class {name!r} detect_phrases 중복: {phrases}"


def test_schema_aliases_lowercase_no_dupes():
    for name, spec in CLASSES.items():
        aliases = spec["aliases"]
        not_lower = [a for a in aliases if a != a.lower()]
        assert not not_lower, f"class {name!r} aliases 에 lowercase 아닌 항목: {not_lower}"
        assert len(aliases) == len(set(aliases)), f"class {name!r} aliases 중복: {aliases}"


def test_schema_dispatch_category_requires_detect_phrases():
    offenders = [name for name, spec in CLASSES.items() if spec["dispatch_category"] and not spec["detect_phrases"]]
    assert not offenders, f"dispatch_category=true 인데 detect_phrases 가 빈 클래스: {offenders}"


# ---------------------------------------------------------------------------
# (b) env_utils.CATEGORY_TO_CLASSES parity
# ---------------------------------------------------------------------------


def test_env_utils_category_to_classes_key_parity():
    expected = {name for name, spec in CLASSES.items() if spec["dispatch_category"] and spec["detect_phrases"]}
    assert set(CATEGORY_TO_CLASSES.keys()) == expected


def test_env_utils_category_to_classes_value_parity_and_order():
    for name in CATEGORY_TO_CLASSES:
        assert CATEGORY_TO_CLASSES[name] == CLASSES[name]["detect_phrases"], (
            f"class {name!r}: CATEGORY_TO_CLASSES={CATEGORY_TO_CLASSES[name]!r} != "
            f"ontology detect_phrases={CLASSES[name]['detect_phrases']!r}"
        )


# ---------------------------------------------------------------------------
# (c) ls_tasks.CATEGORY_SYNONYMS parity
# ---------------------------------------------------------------------------


def test_ls_tasks_category_synonyms_key_parity():
    expected = {name for name, spec in CLASSES.items() if spec["aliases"]}
    assert set(CATEGORY_SYNONYMS.keys()) == expected


def test_ls_tasks_category_synonyms_value_parity():
    for name in CATEGORY_SYNONYMS:
        assert CATEGORY_SYNONYMS[name] == set(CLASSES[name]["aliases"]), (
            f"class {name!r}: CATEGORY_SYNONYMS={CATEGORY_SYNONYMS[name]!r} != "
            f"ontology aliases set={set(CLASSES[name]['aliases'])!r}"
        )


# ---------------------------------------------------------------------------
# (d) docker/genai/templates/promote.html 의 PRESETS parity
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def promote_html_presets() -> dict[str, dict]:
    html_text = _PROMOTE_HTML_PATH.read_text(encoding="utf-8")
    return _parse_promote_html_presets(html_text)


def test_promote_html_presets_key_parity(promote_html_presets):
    expected = {name for name, spec in CLASSES.items() if spec["ui_preset"] is not None}
    assert set(promote_html_presets.keys()) == expected


def test_promote_html_presets_field_parity(promote_html_presets):
    for name, preset in promote_html_presets.items():
        spec = CLASSES[name]
        assert (
            preset["classes"] == spec["detect_phrases"]
        ), f"preset {name!r}: classes={preset['classes']!r} != detect_phrases={spec['detect_phrases']!r}"
        assert (
            preset["label"] == spec["ui_preset"]["label"]
        ), f"preset {name!r}: label={preset['label']!r} != ui_preset.label={spec['ui_preset']['label']!r}"
        assert preset["gemini_desc"] == spec["ui_preset"]["gemini_desc"], (
            f"preset {name!r}: gemini_desc={preset['gemini_desc']!r} != "
            f"ui_preset.gemini_desc={spec['ui_preset']['gemini_desc']!r}"
        )
        assert preset["categories"] == [name], f"preset {name!r}: categories={preset['categories']!r} != [{name!r}]"


# ---------------------------------------------------------------------------
# (e) yolo_thresholds.YOLO_CLASS_CONFIDENCE_THRESHOLDS 커버리지
# ---------------------------------------------------------------------------


def test_yolo_thresholds_cover_all_detect_phrases():
    """정본의 모든 detect_phrases 문구가 threshold dict 에 키로 존재해야 한다.

    ⚠️ 이 테스트는 지금 통과할 수도 실패할 수도 있다. 실패하면 빠진 문구를 메시지에
    담아 실패시키는 것이 의도된 동작이다 — 느슨하게 만들지 말 것.
    """
    all_phrases: set[str] = set()
    for spec in CLASSES.values():
        all_phrases.update(spec["detect_phrases"])
    missing = sorted(all_phrases - set(YOLO_CLASS_CONFIDENCE_THRESHOLDS.keys()))
    assert not missing, f"YOLO_CLASS_CONFIDENCE_THRESHOLDS 에 빠진 문구 {len(missing)}개: {missing}"


# ---------------------------------------------------------------------------
# (f) 회귀 방지 스냅샷 — 확정된 값을 하드코딩으로 못박는다.
# ---------------------------------------------------------------------------


def test_snapshot_smoke_detect_phrases():
    """flame 이 smoke 프리셋에 다시 섞여 들어가면 실패한다."""
    assert CLASSES["smoke"]["detect_phrases"] == ["smoke", "smoke cloud"]


def test_snapshot_weapon_detect_phrases():
    assert CLASSES["weapon"]["detect_phrases"] == ["gun", "knife", "baseball bat", "sword", "bat", "dagger"]


def test_snapshot_falldown_detect_phrases():
    assert CLASSES["falldown"]["detect_phrases"] == [
        "fallen person",
        "person lying down",
        "person on the ground",
    ]


# ---------------------------------------------------------------------------
# (g) migration 022 seed parity — JSON 정본 ↔ DB 투영 드리프트 방지.
#
# 022 는 일회성 시드이므로 정본이 바뀌면 후속 migration(023+)이 필요하다. 그 후속을
# 잊으면 JSON 과 DB 가 조용히 갈리는데, 그것이 바로 이 온톨로지가 없애려던 병이다.
# SQL 파서를 만들지 않고 시드 리터럴 존재 + 개수로만 검증한다.
# ---------------------------------------------------------------------------

_MIGRATION_PATH = _REPO_ROOT / "src" / "vlm_pipeline" / "sql" / "migrations" / "postgres" / "022_label_ontology.sql"


@pytest.fixture(scope="module")
def migration_sql() -> str:
    return _MIGRATION_PATH.read_text(encoding="utf-8")


def test_migration_seeds_every_canonical(migration_sql):
    missing = [name for name in CLASSES if f"'{name}',\n" not in migration_sql]
    assert not missing, f"022 시드에 없는 canonical: {missing}"


def test_migration_seeds_every_alias(migration_sql):
    expected = {(alias, name) for name, spec in CLASSES.items() for alias in spec["aliases"]}
    missing = [pair for pair in sorted(expected) if f"('{pair[0]}', '{pair[1]}')" not in migration_sql]
    assert not missing, f"022 시드에 없는 alias: {missing}"


def test_migration_alias_seed_has_no_extras(migration_sql):
    """SQL 에만 있고 JSON 에 없는 alias 를 잡는다 (존재 검사만으로는 안 걸린다)."""
    block = migration_sql.split("INSERT INTO label_class_aliases", 1)[1]
    seeded = set(re.findall(r"\('([^']+)', '([^']+)'\)", block))
    expected = {(alias, name) for name, spec in CLASSES.items() for alias in spec["aliases"]}
    assert seeded == expected, f"only-SQL={sorted(seeded - expected)} only-JSON={sorted(expected - seeded)}"


def test_migration_class_count_assertion_is_name_scoped(migration_sql):
    """개수 어서션은 이름 목록으로 한정돼야 한다.

    `COUNT(*) = N FROM label_classes` (한정 없음) 형태는 매 부팅마다 검증되므로, 후속
    migration 이 14번째 클래스를 넣는 순간 Dagster 부팅이 막힌다. 022 는 자기가 심은
    13개가 여전히 있는지만 확인해야 한다.
    """
    m = re.search(
        r"SELECT COUNT\(\*\) = (\d+) FROM label_classes WHERE canonical IN \(([^)]*)\)",
        migration_sql,
    )
    assert m, "이름으로 한정된 label_classes 개수 어서션을 찾지 못함 (한정 없는 COUNT 는 부팅 풋건)"
    declared = int(m.group(1))
    names = re.findall(r"'([^']+)'", m.group(2))
    assert declared == len(names), f"어서션 개수 {declared} 와 이름 수 {len(names)} 불일치"
    unknown = [n for n in names if n not in CLASSES]
    assert not unknown, f"정본에 없는 canonical 을 어서션이 요구한다: {unknown}"
