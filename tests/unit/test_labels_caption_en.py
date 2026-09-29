"""labels.caption_text_en (migration 025) — 영문 캡션 적재 회귀 테스트.

Gemini VIDEO_EVENT_SCHEMA 는 ko_caption/en_caption 을 둘 다 required 로 받아왔는데
적재 시점에 `caption_text = ko or en` 으로 접혀 영문이 버려졌다. 이 테스트가 지키는 계약:

① 행 빌더가 영문을 `caption_text_en` 에 **폴백 없이** 담는다 (언어 혼재 금지).
② `caption_text` 는 기존 표시용 폴백 동작(ko 우선)을 유지한다.
③ LS 검수(TimelineLabels)가 labels 를 전량 DELETE 후 재INSERT 할 때, 구간이 그대로인
   이벤트의 캡션은 살아남고 사람이 경계를 옮긴 이벤트는 NULL 로 떨어진다.

③ 이 없으면 검수 한 번에 ko·en 캡션이 통째로 사라진다 (annotation_to_events 는
category/duration/timestamp 만 만들고 캡션 필드가 없다).
"""

from __future__ import annotations

from vlm_pipeline.defs.process.helpers_metadata import _build_gemini_label_rows


def _event(start, end, ko=None, en=None, category="smoke"):
    ev = {"category": category, "duration": round(end - start, 3), "timestamp": [start, end]}
    if ko is not None:
        ev["ko_caption"] = ko
    if en is not None:
        ev["en_caption"] = en
    return ev


class TestGeminiLabelRowBuilder:
    def test_both_languages_are_stored(self):
        rows = _build_gemini_label_rows(
            "asset-1", "src/events/a.json", [_event(1.0, 5.0, ko="연기 발생", en="Smoke emerges")]
        )

        assert len(rows) == 1, rows
        assert rows[0]["caption_text"] == "연기 발생"
        assert rows[0]["caption_text_en"] == "Smoke emerges"

    def test_english_only_does_not_leak_into_en_column_as_fallback(self):
        """ko 부재 시 caption_text 는 영문으로 폴백(기존 동작)하지만 caption_text_en 은 영문 그대로."""
        rows = _build_gemini_label_rows("asset-1", "k", [_event(1.0, 5.0, en="Smoke emerges")])

        assert rows[0]["caption_text"] == "Smoke emerges"
        assert rows[0]["caption_text_en"] == "Smoke emerges"

    def test_korean_only_leaves_en_column_null(self):
        """영문이 없으면 NULL — 한국어를 영문 컬럼에 폴백시키면 안 된다."""
        rows = _build_gemini_label_rows("asset-1", "k", [_event(1.0, 5.0, ko="연기 발생")])

        assert rows[0]["caption_text"] == "연기 발생"
        assert rows[0]["caption_text_en"] is None

    def test_no_caption_at_all(self):
        rows = _build_gemini_label_rows("asset-1", "k", [_event(1.0, 5.0)])

        assert rows[0]["caption_text"] is None
        assert rows[0]["caption_text_en"] is None


class _FakeCursor:
    """upsert_video_labels 가 부르는 순서대로 응답하는 최소 커서.

    호출 순서: finalized COUNT → 전체 COUNT → 캡션 SELECT → DELETE → INSERT×N
    """

    def __init__(self, prior_rows):
        self._prior_rows = prior_rows
        self._pending = None
        self.inserted: list[tuple] = []

    def execute(self, sql, params=None):
        text = " ".join(str(sql).split())
        if "COUNT(*)" in text and "finalized" in text:
            self._pending = [(0,)]
        elif "COUNT(*)" in text:
            self._pending = [(len(self._prior_rows),)]
        elif "SELECT timestamp_start_sec" in text:
            self._pending = list(self._prior_rows)
        elif text.startswith("DELETE"):
            self._pending = []
        elif "INSERT INTO labels" in text:
            self.inserted.append(tuple(params or ()))
            self._pending = []
        else:  # pragma: no cover — 예상 못 한 쿼리는 테스트가 드러내야 한다
            raise AssertionError(f"예상 못 한 쿼리: {text[:120]}")

    def fetchone(self):
        return self._pending[0] if self._pending else None

    def fetchall(self):
        return list(self._pending or [])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        pass


def _run_upsert(prior_rows, new_events, fps=None):
    from gemini.ls_sync_db import upsert_video_labels

    cur = _FakeCursor(prior_rows)
    conn = _FakeConn(cur)
    deleted, inserted = upsert_video_labels(
        "dsn", "vlm-labels", "src/events/a.json", "asset-1", new_events, conn=conn, fps=fps
    )
    return cur.inserted, deleted, inserted


def _ls_round_trip(sec: float, fps: float) -> float:
    """실제 LS 왕복 — ls_tasks_create.py 의 `round(sec*fps)` → ls_sync_converters.py 의 `frame/fps`."""
    return round(sec * fps) / fps


class TestReviewPreservesCaptions:
    def test_unchanged_segment_keeps_both_captions(self):
        prior = [(1.0, 5.0, "연기 발생", "Smoke emerges")]
        rows, _deleted, inserted = _run_upsert(prior, [_event(1.0, 5.0)])

        assert inserted == 1
        # INSERT params 꼬리 4개 = (start, end, caption_text, caption_text_en)
        assert rows[0][-2:] == ("연기 발생", "Smoke emerges"), rows[0]

    def test_moved_boundary_drops_caption(self):
        """사람이 경계를 옮기면 캡션이 그 구간을 설명하지 않으므로 NULL."""
        prior = [(1.0, 5.0, "연기 발생", "Smoke emerges")]
        rows, _deleted, inserted = _run_upsert(prior, [_event(2.5, 7.0)])

        assert inserted == 1
        assert rows[0][-2:] == (None, None), rows[0]

    def test_caption_free_prior_rows_are_ignored(self):
        prior = [(1.0, 5.0, None, None)]
        rows, _deleted, inserted = _run_upsert(prior, [_event(1.0, 5.0)])

        assert inserted == 1
        assert rows[0][-2:] == (None, None), rows[0]

    def test_frame_quantized_round_trip_still_matches(self):
        """LS 왕복(초→프레임→초) 양자화로 값이 달라져도 '손대지 않은' 구간은 캡션을 지켜야 한다.

        정확 일치로 비교하던 초판은 여기서 조용히 실패했다 — 실측 정확일치율이
        fps=29.97 에서 21.3%(끝점당)라 이벤트 단위로는 ~5% 만 살아남는다.
        """
        for fps in (23.976, 24, 25, 29.97, 30, 59.94):
            start_orig, end_orig = 12.34, 16.78
            prior = [(start_orig, end_orig, "연기 발생", "Smoke emerges")]
            reviewed = _event(_ls_round_trip(start_orig, fps), _ls_round_trip(end_orig, fps))

            rows, _deleted, inserted = _run_upsert(prior, [reviewed], fps=fps)

            assert inserted == 1
            assert rows[0][-2:] == ("연기 발생", "Smoke emerges"), f"fps={fps} 에서 캡션 소실: {rows[0]}"

    def test_real_boundary_move_still_drops_caption_under_tolerance(self):
        """허용오차를 넣어도 사람이 실제로 옮긴 구간은 여전히 걸러야 한다(허용오차 ≫ 이동량 금지)."""
        fps = 30
        prior = [(12.34, 16.78, "연기 발생", "Smoke emerges")]
        rows, _deleted, inserted = _run_upsert(prior, [_event(12.5, 16.78)], fps=fps)  # 0.16s = ~5프레임 이동

        assert inserted == 1
        assert rows[0][-2:] == (None, None), rows[0]

    def test_closest_prior_wins_when_several_are_in_range(self):
        fps = 30
        prior = [(1.00, 5.00, "가까움", "near"), (1.03, 5.03, "더 멂", "far")]
        rows, _deleted, inserted = _run_upsert(prior, [_event(1.005, 5.005)], fps=fps)

        assert inserted == 1
        assert rows[0][-2:] == ("가까움", "near"), rows[0]


if __name__ == "__main__":  # pragma: no cover
    import sys

    import pytest

    sys.exit(pytest.main([__file__, "-q"]))
