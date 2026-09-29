"""LS task 생성 결과 판정 — "조용한 0건"을 정상으로 기록하지 않게 하는 테스트.

2026-09-21 실측: comfy_local 합성본 1장을 promote 했을 때 `ls_tasks.py create` 가 exit 0
으로 끝나고 `ls_task_status='created'` 까지 기록됐는데, Label Studio 프로젝트의 task 는
0건이었다. 라벨러 게이트가 유일한 후보를 걷어냈지만 호출부는 그 사실을 알 방법이 없었다.
(같은 형태의 무언 실패가 이 레포에 반복 등장한다 — 산출물 0건일 때 계약을 박는 게 최저비용이다.)

그래서 CLI 가 기계 판독 줄을 내보내고, 호출부가 그것을 읽어 판정한다. 이 테스트는 그
계약의 양쪽을 고정한다.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from vlm_pipeline.defs.ls.sensor import (  # noqa: E402
    _parse_create_result,
    _resolve_ls_task_status,
)


class _Log:
    def __init__(self):
        self.warnings = []

    def warning(self, msg):
        self.warnings.append(msg)


class _Ctx:
    def __init__(self):
        self.log = _Log()


def test_parses_the_machine_readable_line():
    stdout = (
        "[INFO] 게이트 ON\n"
        "[DONE] image mode: 생성 3 / 스킵(기존) 1 / 오류 0\n"
        "[RESULT] mode=image created=3 skipped=1 error=0 gated_out=7\n"
    )
    assert _parse_create_result(stdout) == {
        "created": 3,
        "skipped": 1,
        "error": 0,
        "gated_out": 7,
    }


def test_last_result_line_wins():
    """한 프로세스가 여러 번 출력해도 마지막 것이 최종 상태다."""
    stdout = (
        "[RESULT] mode=image created=0 skipped=0 error=0 gated_out=1\n"
        "[RESULT] mode=image created=5 skipped=0 error=0 gated_out=1\n"
    )
    assert _parse_create_result(stdout)["created"] == 5


def test_missing_line_returns_none():
    assert _parse_create_result("[DONE] image mode: 생성 0") is None
    assert _parse_create_result("") is None


def test_withheld_candidates_are_an_error_not_a_success():
    """게이트가 후보를 전부 걷어냈으면 'created' 로 기록하면 안 된다."""
    ctx = _Ctx()
    outcomes = [("image", {"created": 0, "skipped": 0, "error": 0, "gated_out": 12})]
    with pytest.raises(RuntimeError, match="LS task 0건"):
        _resolve_ls_task_status(ctx, "req-1", ["image"], outcomes)


def test_no_candidates_at_all_is_skipped_not_failed():
    """ "만들 게 없었다"와 "만들 게 있었는데 안 갔다"는 다른 사건이다.

    둘을 failed 하나로 뭉치면 운영자 트리아지 신호가 죽는다. 후보가 0이면 정상 종료다
    (SAM3 결과가 아직 없는 배치 등). ls_task_status 어휘는 스키마 주석이 이미
    `pending | created | skipped` 로 예고하고 있다.
    """
    ctx = _Ctx()
    outcomes = [("image", {"created": 0, "skipped": 0, "error": 0, "gated_out": 0})]
    assert _resolve_ls_task_status(ctx, "req-1", ["image"], outcomes) == "skipped"


def test_no_mode_ran_is_skipped_and_not_misdiagnosed_as_legacy():
    """실행된 모드가 0개면 [RESULT] 가 없는 것이 정상 — 구버전으로 오진하면 안 된다.

    prod 에 `labeling_method='skip'` 행이 실재한다. 그 요청은 video/image 어느 매핑에도
    안 걸려 서브프로세스가 아예 안 돈다. 이걸 "구버전 스크립트"로 읽고 'created' 를
    찍으면 이 가드가 닫겠다던 조용한 0건이 그대로 성립한다.
    """
    ctx = _Ctx()
    assert _resolve_ls_task_status(ctx, "req_97cccc", [], []) == "skipped"
    assert any("실행된 LS 생성 모드 없음" in w for w in ctx.log.warnings)
    assert not any("[RESULT] 줄 없음" in w for w in ctx.log.warnings)


def test_already_existing_tasks_count_as_produced():
    """재실행(idempotent)에서 전부 skip 되는 것은 정상이다 — 실패로 만들면 안 된다."""
    ctx = _Ctx()
    outcomes = [("image", {"created": 0, "skipped": 4, "error": 0, "gated_out": 0})]
    assert _resolve_ls_task_status(ctx, "req-1", ["image"], outcomes) == "created"


def test_one_productive_mode_is_enough():
    """video 0건 + image 생성이면 통과 — 이미지 전용 배치의 정상 형태다."""
    ctx = _Ctx()
    outcomes = [
        ("video", {"created": 0, "skipped": 0, "error": 0, "gated_out": 0}),
        ("image", {"created": 2, "skipped": 0, "error": 0, "gated_out": 0}),
    ]
    assert _resolve_ls_task_status(ctx, "req-1", ["video", "image"], outcomes) == "created"


def test_unparseable_output_warns_but_does_not_block():
    """구버전 스크립트와 섞여 돌 때 배포가 멈추지 않게 — 대신 침묵하지 않는다."""
    ctx = _Ctx()
    assert _resolve_ls_task_status(ctx, "req-1", ["image"], [("image", None)]) == "created"
    assert any("검증 skip" in w for w in ctx.log.warnings)


def test_image_only_methods_still_route_to_the_image_project():
    """`captioning_image` 단독 dispatch 가 조용히 사라지지 않는다.

    2026-09-21 까지 `_IMAGE_METHODS` 에 captioning_image 가 없었다. env_utils 의 의존성
    확장이 timestamp_video 를 항상 끌고 붙여 video 경로로 새던 덕에 가려져 있었을 뿐이다.
    이미지 배치에서 그 확장을 끊으면 video/image 어느 쪽도 안 도는 무동작이 된다.
    """
    from vlm_pipeline.defs.ls.sensor import _IMAGE_METHODS, _VIDEO_METHODS

    methods = {"captioning_image"}
    assert methods & _IMAGE_METHODS, "captioning_image 가 image 경로로 라우팅되어야 한다"
    assert not (methods & _VIDEO_METHODS), "이미지 전용 요청이 video 프로젝트를 만들면 안 된다"

    # comfy_local 의 실제 조합도 video 를 켜지 않는다 (유령 video 프로젝트 방지).
    comfy = {"captioning_image", "bbox"}
    assert comfy & _IMAGE_METHODS
    assert not (comfy & _VIDEO_METHODS)
