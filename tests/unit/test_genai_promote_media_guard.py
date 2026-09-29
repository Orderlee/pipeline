"""GenAI promote 의 media 계약 회귀 테스트 (2026-09-21 유령 video 프로젝트).

배경: comfy_local batch ``b4fe9a6f-339``(이미지 1장) 를 ``[captioning_image, bbox]`` 로
promote 했는데 dispatch 가 ``['bbox','captioning_image','captioning_video','timestamp_video']``
로 확장했고, 이미지뿐인 배치에 빈 video LS 프로젝트(id 821)가 생겼다.

확장 지점은 ``lib/env_utils._OUTPUT_DEPENDENCIES["captioning_image"]`` →
``resolve_outputs()`` → ``lib/dispatch_payload._finalize_outputs()`` 였다. 그 의존성은
"비디오에서 프레임을 뽑으려면 먼저 timestamp 가 필요하다"는 video 전제라 원본이
이미지면 성립하지 않는다.

이 파일은 세 겹을 모두 고정한다:
  1) promote 서버측 거부 (``validate_labeling_method_for_media``)
  2) dispatch JSON 에 ``output_media`` 가 실려 나가는지
  3) 그 JSON 을 파이프라인이 파싱했을 때 video 메서드가 붙지 않는지
  4) promote.html 의 엔진(매체)별 기본 체크 상태
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from vlm_pipeline.lib.dispatch_payload import parse_dispatch_request_payload

ROOT = Path(__file__).resolve().parents[2]
GENAI = ROOT / "docker" / "genai"
if str(GENAI) not in sys.path:
    sys.path.insert(0, str(GENAI))

import jobs.promote as promote  # noqa: E402

TEMPLATES_DIR = GENAI / "templates"


# ---------------------------------------------------------------------------
# (1) 서버측 media 검증
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("method", ["timestamp_video", "captioning_video", "classification_video"])
def test_validate_rejects_video_method_on_image_batch(method: str) -> None:
    with pytest.raises(promote.PromoteValidationError) as exc:
        promote.validate_labeling_method_for_media([method, "bbox"], "image")

    assert method in str(exc.value)


def test_validate_allows_image_methods_on_image_batch() -> None:
    promote.validate_labeling_method_for_media(["captioning_image", "bbox", "classification_image"], "image")


def test_validate_leaves_video_batch_untouched() -> None:
    promote.validate_labeling_method_for_media(["timestamp_video", "captioning_video", "bbox"], "video")


# ---------------------------------------------------------------------------
# (2)+(3) repromote → dispatch JSON → 파이프라인 파싱까지 관통
# ---------------------------------------------------------------------------


class _FakePg:
    """promote 가 쓰는 pg 표면만 흉내낸다 (DB 접속 없음)."""

    def __init__(self, batch: dict) -> None:
        self._batch = batch
        self.claims: list[str] = []
        self.releases: list[str] = []

    def get_batch_with_jobs(self, batch_id: str) -> dict:
        return self._batch

    def claim_batch_for_promote(self, batch_id: str) -> bool:
        self.claims.append(batch_id)
        return True

    def release_batch_promote_claim(self, batch_id: str) -> None:
        self.releases.append(batch_id)


def _image_batch(batch_id: str = "b4fe9a6f-339") -> dict:
    return {
        "batch_id": batch_id,
        "engine": "comfy_local",
        "output_media": "image",
        "status": "succeeded",
        "options_json": json.dumps({"promoted_to_labeling": True}),
        "jobs": [{"seq_in_batch": 1, "status": "done", "provider_job_id": "p1"}],
    }


def _run_repromote(monkeypatch, tmp_path: Path, batch: dict, methods: list[str]) -> dict:
    monkeypatch.setattr(promote, "pg", _FakePg(batch))
    monkeypatch.setenv("INCOMING_DIR", str(tmp_path))
    return promote.repromote_batch_to_labeling(
        batch["batch_id"],
        labeling_method=methods,
        label_policy="required",
        categories=["smoke"],
        classes=["smoke"],
    )


def test_repromote_rejects_video_method_on_image_batch(monkeypatch, tmp_path: Path) -> None:
    with pytest.raises(promote.PromoteValidationError):
        _run_repromote(monkeypatch, tmp_path, _image_batch(), ["captioning_image", "timestamp_video"])


def test_repromote_writes_output_media_and_parses_without_video_methods(monkeypatch, tmp_path: Path) -> None:
    result = _run_repromote(monkeypatch, tmp_path, _image_batch(), ["captioning_image", "bbox"])

    payload = json.loads(Path(result["dispatch_path"]).read_text(encoding="utf-8"))
    assert payload["output_media"] == "image"
    assert payload["labeling_method"] == ["captioning_image", "bbox"]

    # 파이프라인이 이 JSON 을 파싱해도 video 메서드가 붙지 않아야 한다 (유령 project 821).
    parsed = parse_dispatch_request_payload(payload)
    assert parsed["labeling_method"] == ["bbox", "captioning_image"]
    assert "timestamp_video" not in parsed["labeling_method"]
    assert "captioning_video" not in parsed["labeling_method"]


# ---------------------------------------------------------------------------
# (4) promote.html 의 매체별 기본 체크 상태
# ---------------------------------------------------------------------------


def _render_promote_html(batch: dict) -> str:
    jinja2 = pytest.importorskip("jinja2")
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)), autoescape=True)
    return env.get_template("promote.html").render(batch=batch, promoted=False)


def _checked_methods(html: str) -> set[str]:
    """``<input ... name="labeling_method" value="X" ... checked>`` 인 X 들."""
    import re

    found: set[str] = set()
    for tag in re.findall(r"<input[^>]*name=\"labeling_method\"[^>]*>", html):
        value = re.search(r"value=\"([^\"]+)\"", tag)
        if value and "checked" in tag:
            found.add(value.group(1))
    return found


def _disabled_methods(html: str) -> set[str]:
    import re

    found: set[str] = set()
    for tag in re.findall(r"<input[^>]*name=\"labeling_method\"[^>]*>", html):
        value = re.search(r"value=\"([^\"]+)\"", tag)
        if value and "disabled" in tag:
            found.add(value.group(1))
    return found


def test_promote_html_image_batch_defaults_to_image_methods() -> None:
    html = _render_promote_html(
        {"batch_id": "b4fe9a6f-339", "engine": "comfy_local", "output_media": "image", "n_succeeded": 1, "n_total": 1}
    )

    assert _checked_methods(html) == {"captioning_image", "bbox"}
    # video 전용은 아예 못 고르게 — disabled 는 submit 되지 않는다.
    assert _disabled_methods(html) == {"timestamp_video", "captioning_video", "classification_video"}


def test_promote_html_video_batch_keeps_legacy_default() -> None:
    html = _render_promote_html(
        {"batch_id": "vid-1", "engine": "kling", "output_media": "video", "n_succeeded": 2, "n_total": 2}
    )

    assert _checked_methods(html) == {"timestamp_video"}
    assert _disabled_methods(html) == set()
