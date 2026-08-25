"""generation_prompts write 경로 (migration 018 Phase 1) 단위 테스트.

DB 왕복(dedup ON CONFLICT / 포인터 생존)은 스크래치 PG 로 검증했고, 여기서는 DB 없이
검증 가능한 계약만 본다 — SQL 상수의 불변식과 계보 기록의 fail-soft 동작.
"""

from __future__ import annotations

import hashlib
import re

from vlm_pipeline.defs.label import timestamp as ts
from vlm_pipeline.resources import postgres_labeling as pl


class _FakeLog:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, msg, *args) -> None:
        self.warnings.append(msg % args if args else msg)

    def info(self, msg, *args) -> None:  # pragma: no cover - 사용 안 함
        pass


class _FakeContext:
    def __init__(self) -> None:
        self.log = _FakeLog()
        self.run_id = "run-abc"


class _RaisingDB:
    """018 미적용 환경(테이블 없음)을 흉내낸다."""

    def upsert_generation_prompt(self, **_kwargs):
        raise RuntimeError('relation "generation_prompts" does not exist')

    def set_timestamp_generation_prompt(self, *_args):
        raise RuntimeError('column "timestamp_generation_prompt_id" does not exist')


class _RecordingDB:
    def __init__(self) -> None:
        self.upsert_kwargs: dict | None = None
        self.links: list[tuple[str, str]] = []

    def upsert_generation_prompt(self, **kwargs) -> str:
        self.upsert_kwargs = kwargs
        return "prompt-uuid-1"

    def set_timestamp_generation_prompt(self, asset_id: str, prompt_id: str) -> None:
        self.links.append((asset_id, prompt_id))


class _Analyzer:
    model_name = "gemini-2.5-flash"


# ── content_hash ──


def test_content_hash_is_sha256_of_utf8():
    text = "화염 감지 프롬프트"
    assert pl.generation_prompt_content_hash(text) == hashlib.sha256(text.encode("utf-8")).hexdigest()


def test_content_hash_differs_for_different_prompts():
    assert pl.generation_prompt_content_hash("a") != pl.generation_prompt_content_hash("b")


# ── SQL 계약 ──


def test_upsert_sql_dedups_on_type_model_hash():
    assert "ON CONFLICT (prompt_type, model_name, content_hash) DO NOTHING" in pl._GENERATION_PROMPT_UPSERT_SQL
    assert "RETURNING prompt_id" in pl._GENERATION_PROMPT_UPSERT_SQL


def test_upsert_sql_omits_spec_id():
    """spec_id 는 labeling_specs(0행) 를 참조하는 FK — 넣으면 INSERT 가 FK 위반으로 죽는다."""
    assert "spec_id" not in pl._GENERATION_PROMPT_UPSERT_SQL


def test_pointer_sql_targets_video_metadata_not_labels():
    """018 설계: labels 는 재생성·LS 검수 때 전량 DELETE 되므로 링크를 두면 계보가 사라진다."""
    assert "UPDATE video_metadata" in pl._TIMESTAMP_PROMPT_POINTER_SQL
    assert "timestamp_generation_prompt_id" in pl._TIMESTAMP_PROMPT_POINTER_SQL
    assert "labels" not in pl._TIMESTAMP_PROMPT_POINTER_SQL


# ── fail-soft (라벨링을 멈추지 않는다) ──


def test_record_returns_none_and_warns_when_table_missing():
    ctx = _FakeContext()
    got = ts._record_generation_prompt(ctx, _RaisingDB(), rendered_prompt="p", analyzer=_Analyzer())
    assert got is None
    assert len(ctx.log.warnings) == 1


def test_link_swallows_failure():
    ctx = _FakeContext()
    ts._link_generation_prompt(ctx, _RaisingDB(), "asset-1", "prompt-1")
    assert len(ctx.log.warnings) == 1


def test_link_noops_without_prompt_id():
    ctx = _FakeContext()
    db = _RecordingDB()
    ts._link_generation_prompt(ctx, db, "asset-1", None)
    assert db.links == []
    assert ctx.log.warnings == []


# ── 전달되는 값 ──


def test_record_passes_prompt_type_and_template_and_model():
    db = _RecordingDB()
    got = ts._record_generation_prompt(
        _FakeContext(),
        db,
        rendered_prompt="rendered",
        analyzer=_Analyzer(),
        categories=["fire"],
        descriptions={"fire": "flames"},
    )
    assert got == "prompt-uuid-1"
    kw = db.upsert_kwargs
    assert kw["prompt_type"] == "video_event_timestamp"
    assert kw["template_name"] == "VIDEO_EVENT_PROMPT"
    assert kw["model_name"] == "gemini-2.5-flash"
    assert kw["rendered_prompt"] == "rendered"
    assert kw["categories"] == ["fire"]
    assert kw["category_descriptions"] == {"fire": "flames"}
    assert kw["dagster_run_id"] == "run-abc"


def test_record_falls_back_to_default_model_when_analyzer_lacks_name():
    db = _RecordingDB()
    ts._record_generation_prompt(_FakeContext(), db, rendered_prompt="r", analyzer=object())
    assert db.upsert_kwargs["model_name"] == "gemini-2.5-flash"


def test_record_normalizes_empty_categories_to_none():
    db = _RecordingDB()
    ts._record_generation_prompt(_FakeContext(), db, rendered_prompt="r", analyzer=_Analyzer(), categories=[])
    assert db.upsert_kwargs["categories"] is None
    assert db.upsert_kwargs["category_descriptions"] is None


def test_prompt_type_is_allowed_by_migration_check_constraint():
    """018 의 CHECK 제약이 우리가 쓰는 prompt_type 을 허용해야 INSERT 가 통과한다.

    파일 전체 substring 검색은 안 된다 — 018 헤더 주석에도 'video_event_timestamp' 가 있어서
    CHECK 제약이 깨져도 초록색으로 통과한다. CHECK 블록만 떼어내 검사한다.
    """
    from pathlib import Path

    sql = (
        Path(__file__).resolve().parents[2] / "src/vlm_pipeline/sql/migrations/postgres/018_generation_prompts.sql"
    ).read_text(encoding="utf-8")
    m = re.search(r"generation_prompts_type_check CHECK \((.*?)\n    \)", sql, re.S)
    assert m, "018 의 generation_prompts_type_check CHECK 블록을 찾지 못함"
    check_body = m.group(1)
    assert "'video_event_timestamp'" in check_body
    # 주석이 아니라 실제 제약 안에 있는지 — 주석 줄(--)을 제거하고 재확인
    code_only = "\n".join(ln for ln in check_body.splitlines() if not ln.strip().startswith("--"))
    assert "'video_event_timestamp'" in code_only


# ── upsert 의 두 분기 (RETURNING 성공 / DO NOTHING 후 SELECT) 행동 검증 ──


class _FakeCursor:
    """execute 순서를 기록하고, 지정된 fetchone 결과를 순서대로 돌려준다."""

    def __init__(self, fetch_results: list) -> None:
        self._fetch = list(fetch_results)
        self.executed: list[str] = []

    def execute(self, sql, _params=None) -> None:
        self.executed.append(sql)

    def fetchone(self):
        return self._fetch.pop(0)

    def __enter__(self):
        return self

    def __exit__(self, *_exc) -> None:
        return None


class _FakeConn:
    def __init__(self, cur) -> None:
        self._cur = cur

    def cursor(self):
        return self._cur

    def __enter__(self):
        return self

    def __exit__(self, *_exc) -> None:
        return None


class _FakeMixin(pl.PostgresLabelingMixin):
    def __init__(self, cur) -> None:  # noqa: D107 - 테스트용
        self._cur = cur

    def connect(self):
        return _FakeConn(self._cur)


def test_upsert_returns_inserted_id_without_second_query():
    cur = _FakeCursor([("new-id",)])
    got = _FakeMixin(cur).upsert_generation_prompt(
        prompt_type="video_event_timestamp",
        template_name="VIDEO_EVENT_PROMPT",
        rendered_prompt="p",
        model_name="m",
    )
    assert got == "new-id"
    assert len(cur.executed) == 1, "INSERT 가 행을 돌려주면 추가 SELECT 를 하지 않아야 한다"


def test_upsert_falls_back_to_select_when_conflict_returns_no_row():
    """ON CONFLICT DO NOTHING 은 0행을 돌려준다 → 기존 행의 prompt_id 를 재사용해야 한다."""
    cur = _FakeCursor([None, ("existing-id",)])
    got = _FakeMixin(cur).upsert_generation_prompt(
        prompt_type="video_event_timestamp",
        template_name="VIDEO_EVENT_PROMPT",
        rendered_prompt="p",
        model_name="m",
    )
    assert got == "existing-id"
    assert len(cur.executed) == 2
    assert "INSERT INTO generation_prompts" in cur.executed[0]
    assert "SELECT prompt_id FROM generation_prompts" in cur.executed[1]


def test_upsert_raises_when_dedup_row_vanishes():
    """0행 + 후속 SELECT 도 0행이면 조용히 넘기지 않고 예외를 던진다 (호출부가 fail-soft 처리)."""
    cur = _FakeCursor([None, None])
    try:
        _FakeMixin(cur).upsert_generation_prompt(
            prompt_type="video_event_timestamp",
            template_name="VIDEO_EVENT_PROMPT",
            rendered_prompt="p",
            model_name="m",
        )
    except RuntimeError:
        return
    raise AssertionError("RuntimeError 를 기대했다")


# ── 020 계보 뷰의 조인 grain (codex 리뷰 #1 회귀 방지) ──


def _lineage_sql() -> str:
    from pathlib import Path

    return (
        Path(__file__).resolve().parents[2] / "src/vlm_pipeline/sql/migrations/postgres/020_prompt_lineage_views.sql"
    ).read_text(encoding="utf-8")


def _lineage_join_line() -> str:
    sql = _lineage_sql()
    body = sql[sql.index("CREATE OR REPLACE VIEW v_prompt_lineage") :]
    lines = [ln.strip() for ln in body.splitlines() if ln.strip().startswith("JOIN labels")]
    assert len(lines) == 1, f"v_prompt_lineage 의 labels 조인이 1개가 아니다: {lines}"
    return lines[0]


def test_lineage_view_does_not_join_labels_on_asset_id():
    """asset_id 조인은 classification 라벨까지 timestamp 프롬프트에 오귀속시킨다.

    defs/label/import_support.py 의 insert_label 이 label_format='video_classification_json'
    행을 같은 asset_id 로 labels 에 넣기 때문. 스크래치 PG 실측: 구 조인 5행 중 2행이 오귀속.
    """
    assert "l.asset_id = vm.asset_id" not in _lineage_join_line()


def test_lineage_view_joins_on_recorded_label_key_of_both_paths():
    """routed 는 timestamp_label_key, MVP 는 auto_label_key 에 실제 산출 키를 기록한다."""
    join = _lineage_join_line()
    assert "l.labels_key" in join
    assert "vm.timestamp_label_key" in join
    assert "vm.auto_label_key" in join, "MVP 경로를 빼면 그 경로의 계보가 통째로 사라진다"
