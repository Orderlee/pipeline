"""LS submit 폴러(ls_webhook_submit_poll) 단위 테스트 — mock 기반, dagster import 없음.

핵심 계약:
- state 에 있고 미확정인 프로젝트의 최종 submit 만 처리한다 (Slack 경로와 동일 대상 조건)
- sync 성공(state='pending_finalize')이 확인될 때만 finalize 한다 (인터록)
- 실패한 프로젝트는 쿨다운 동안 재시도하지 않는다 (알림 스팸 방지)
"""

from unittest.mock import MagicMock, patch

import gemini.ls_webhook_submit_poll as poll_mod
from gemini.ls_webhook_submit_poll import poll_once


def _resp(json_data, status=200):
    m = MagicMock()
    m.status_code = status
    m.json.return_value = json_data
    m.raise_for_status.return_value = None
    return m


def _run(projects, submit_states, state_seq):
    """poll_once 를 mock 환경에서 실행하고 (counts, sync_mock, finalize_mock) 반환.

    submit_states: {pid: bool(is_submitted)}
    state_seq: load_state() 가 순서대로 돌려줄 dict 목록 (부족하면 마지막 값 반복)
    """
    poll_mod._last_attempt_at.clear()
    states = list(state_seq)

    def fake_load_state():
        return states.pop(0) if len(states) > 1 else states[0]

    def fake_get(url, **kwargs):
        if url.endswith("/api/projects/"):
            return _resp({"results": projects})
        pid = int(url.rsplit("/", 2)[-2])
        return _resp({"is_submitted": submit_states.get(pid, False)})

    with (
        patch.object(poll_mod, "requests") as req,
        patch.object(poll_mod, "load_state", side_effect=fake_load_state),
        patch.object(poll_mod, "run_sync_and_notify") as sync,
        patch.object(poll_mod, "finalize_project") as fin,
    ):
        req.get.side_effect = fake_get
        req.RequestException = Exception
        counts = poll_once(lambda: {})
    return counts, sync, fin


PROJ = [{"id": 7, "title": "proj-7"}]


class TestSubmitDetection:
    def test_submitted_and_synced_project_is_finalized(self):
        counts, sync, fin = _run(
            PROJ,
            {7: True},
            # 1) 대상 판정: pending 상태 → 2) sync 후 인터록 확인: pending_finalize
            [{"7": {"status": "pending_finalize"}}, {"7": {"status": "pending_finalize"}}],
        )
        sync.assert_called_once_with(7, "proj-7")
        fin.assert_called_once_with(7)
        assert counts["finalized"] == 1

    def test_not_submitted_project_is_skipped(self):
        counts, sync, fin = _run(PROJ, {7: False}, [{"7": {"status": "pending_finalize"}}])
        sync.assert_not_called()
        fin.assert_not_called()
        assert counts["skipped"] == 1

    def test_already_finalized_project_is_skipped(self):
        counts, sync, fin = _run(PROJ, {7: True}, [{"7": {"status": "finalized"}}])
        sync.assert_not_called()
        fin.assert_not_called()

    def test_project_without_state_is_skipped(self):
        """ls_tasks.py create 미경유(수동 생성 등) 프로젝트는 Slack 경로처럼 대상 제외."""
        counts, sync, fin = _run(PROJ, {7: True}, [{}])
        sync.assert_not_called()
        fin.assert_not_called()


class TestSyncInterlock:
    def test_finalize_not_called_when_sync_did_not_complete(self):
        """sync 실패 시 state 가 pending_finalize 로 바뀌지 않음 → finalize 금지."""
        counts, sync, fin = _run(
            PROJ,
            {7: True},
            [{"7": {"status": "pending"}}, {"7": {"status": "pending"}}],
        )
        sync.assert_called_once()
        fin.assert_not_called()
        assert counts["finalized"] == 0

    def test_failed_project_respects_retry_cooldown(self):
        """같은 프로젝트를 쿨다운 내 재스캔하면 sync 재시도하지 않는다."""
        poll_mod._last_attempt_at.clear()
        state = {"7": {"status": "pending"}}

        def fake_get(url, **kwargs):
            if url.endswith("/api/projects/"):
                return _resp({"results": PROJ})
            return _resp({"is_submitted": True})

        with (
            patch.object(poll_mod, "requests") as req,
            patch.object(poll_mod, "load_state", return_value=state),
            patch.object(poll_mod, "run_sync_and_notify") as sync,
            patch.object(poll_mod, "finalize_project") as fin,
        ):
            req.get.side_effect = fake_get
            req.RequestException = Exception
            poll_once(lambda: {})
            counts2 = poll_once(lambda: {})

        sync.assert_called_once()  # 두 번째 스캔은 쿨다운으로 skip
        fin.assert_not_called()
        assert counts2["cooldown"] == 1
