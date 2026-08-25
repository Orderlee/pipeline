"""미상 카테고리 관측(observed_categories) 단위 테스트.

정본 해석기(resolve_to_canonical)와 기록 헬퍼의 fail-soft·필터 동작을 DB 없이 검증한다.
"""

from __future__ import annotations

from vlm_pipeline.defs.label import timestamp as ts
from vlm_pipeline.defs.process import captioning as cap
from vlm_pipeline.lib.env_utils import resolve_to_canonical


class _FakeLog:
    def __init__(self) -> None:
        self.warnings: list[str] = []
        self.infos: list[str] = []

    def warning(self, msg, *args) -> None:
        self.warnings.append(msg % args if args else msg)

    def info(self, msg, *args) -> None:
        self.infos.append(msg % args if args else msg)


class _Ctx:
    def __init__(self) -> None:
        self.log = _FakeLog()


class _DB:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def record_observed_categories(self, source, values, source_unit=None) -> int:
        self.calls.append((source, list(values), source_unit))
        return len(list(values))


class _RaisingDB:
    def record_observed_categories(self, *_a, **_k):
        raise RuntimeError('relation "observed_categories" does not exist')


# ── resolve_to_canonical ──


def test_resolves_canonical_directly():
    assert resolve_to_canonical("fire") == "fire"


def test_resolution_is_case_and_whitespace_insensitive():
    assert resolve_to_canonical("  FIRE  ") == "fire"


def test_resolves_alias_to_canonical():
    assert resolve_to_canonical("fallen person") == "falldown"
    assert resolve_to_canonical("person_lying_on_ground") == "falldown"


def test_canonical_wins_over_alias_for_smoking():
    """`smoking` 은 독립 canonical 이면서 `smoke` 의 alias 다 (정본에 미해결 충돌로 기록).

    canonical-first 규칙이 깨지면 흡연 이벤트가 조용히 연기로 접힌다.
    """
    assert resolve_to_canonical("smoking") == "smoking"


def test_unknown_values_return_none():
    for value in ("etc", "safety_equipment", "쓰러짐(falldown)", "화재(fire)", "존재하지않는값"):
        assert resolve_to_canonical(value) is None, value


def test_blank_and_none_return_none():
    for value in ("", "   ", None, 0):
        assert resolve_to_canonical(value) is None


# ── Gemini 이벤트 기록 헬퍼 ──


def test_event_recorder_only_reports_unknown_categories():
    db = _DB()
    events = [
        {"category": "fire"},
        {"category": "fallen person"},
        {"category": "etc"},
        {"category": "unsafe_climbing_activity"},
        {"category": "새로운이벤트"},
    ]
    cap._record_unknown_event_categories(_Ctx(), db, events, source_unit="source-h")
    assert len(db.calls) == 1
    source, values, unit = db.calls[0]
    assert source == "gemini_event"
    assert values == ["etc", "새로운이벤트"]
    assert unit == "source-h"


def test_event_recorder_preserves_raw_form():
    """원문 보존 — 소문자화·정규화해서 넘기면 무엇이 왔는지 잃는다."""
    db = _DB()
    cap._record_unknown_event_categories(_Ctx(), db, [{"category": "  Weird_Cat  "}], source_unit=None)
    assert db.calls[0][1] == ["Weird_Cat"]


def test_event_recorder_skips_db_when_all_known():
    db = _DB()
    cap._record_unknown_event_categories(_Ctx(), db, [{"category": "fire"}, {"category": "smoke"}], source_unit=None)
    assert db.calls == []


def test_event_recorder_ignores_malformed_events():
    db = _DB()
    cap._record_unknown_event_categories(
        _Ctx(), db, ["not a dict", {"no_category": 1}, {"category": ""}, {"category": None}], source_unit=None
    )
    assert db.calls == []


def test_event_recorder_dedups_within_one_asset():
    db = _DB()
    cap._record_unknown_event_categories(
        _Ctx(), db, [{"category": "etc"}, {"category": "etc"}, {"category": "etc"}], source_unit=None
    )
    assert db.calls[0][1] == ["etc"]


def test_event_recorder_is_fail_soft():
    ctx = _Ctx()
    cap._record_unknown_event_categories(ctx, _RaisingDB(), [{"category": "etc"}], source_unit=None)
    assert len(ctx.log.warnings) == 1


# ── dispatch 카테고리 기록 헬퍼 ──


def test_dispatch_recorder_reports_only_unknown():
    db = _DB()
    ts._record_unknown_dispatch_categories(_Ctx(), db, ["fire", "smoke", "etc", "safety_equipment"], "source-h")
    source, values, unit = db.calls[0]
    assert source == "dispatch_request"
    assert values == ["etc", "safety_equipment"]
    assert unit == "source-h"


def test_dispatch_recorder_catches_korean_categories():
    """실측: 한글 카테고리가 매핑에 없어 빈 classes 로 조용히 통과한 dispatch 가 있었다."""
    db = _DB()
    ts._record_unknown_dispatch_categories(_Ctx(), db, ["연기(smoke)", "화재(fire)", "쓰러짐(falldown)"], None)
    assert db.calls[0][1] == ["쓰러짐(falldown)", "연기(smoke)", "화재(fire)"]


def test_dispatch_recorder_noops_on_empty():
    db = _DB()
    ts._record_unknown_dispatch_categories(_Ctx(), db, [], None)
    ts._record_unknown_dispatch_categories(_Ctx(), db, ["fire"], None)
    assert db.calls == []


def test_dispatch_recorder_is_fail_soft():
    ctx = _Ctx()
    ts._record_unknown_dispatch_categories(ctx, _RaisingDB(), ["etc"], None)
    assert len(ctx.log.warnings) == 1
