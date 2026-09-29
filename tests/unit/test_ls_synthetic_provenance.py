"""합성 생성본이 LS 검수 화면에서 실사 CCTV 와 구분되는가 (P1-1).

2026-09-21 실측: comfy_local 합성본이 LS 프로젝트 823 까지 도달했지만 `src/gemini/` 와
`defs/ls/` 전체에 `synthetic`/`comfy_local` 참조가 0건이었다 — 검수자가 합성본을 실사로
오인해도 막을 것이 아무것도 없었다. 오인된 검수는 그대로 학습셋 오염이 된다.

이 테스트가 고정하는 계약은 둘이다.

1. **합성 task 에만 붙는다.** 실사 프로젝트의 label_config 와 task payload 는 이 변경
   이전과 바이트 단위로 같다. 기존 프로젝트 회귀는 이 레포에서 가장 비싼 실수다.
2. **붙을 때는 전부 붙는다.** 출처(합성 여부·엔진·batch·의도 이벤트)와 설계서
   Phase D.3 의 4개 판정이 함께 간다. 하나라도 빠지면 "표시는 있는데 판정은 못 하는"
   절반 상태가 되고, 그건 없느니만 못하다.

(라이브 LS 1.23.0 의 `POST /api/projects/validate/` 로 네 config 모두 204 확인 — 여기서는
프로젝트를 만들지 않으므로 XML well-formed 까지만 본다.)
"""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from gemini.ls_tasks_label_config import (  # noqa: E402
    SYNTHETIC_QC_CRITERIA,
    SYNTHETIC_QC_NAME,
    _image_label_config,
    _video_label_config,
    build_synthetic_provenance,
)

# ---------------------------------------------------------------------------
# 1. 실사(기존) 경로 — 바이트 단위 불변
# ---------------------------------------------------------------------------

_LEGACY_IMAGE = """<View>
  <Image name="image" value="$image" />
  <RectangleLabels name="imageLabels" toName="image">
    <Label value="smoke" background="#e74c3c"/>
    <Label value="fire" background="#e67e22"/>
  </RectangleLabels>
</View>"""

_LEGACY_VIDEO = """<View>
  <Video name="video" value="$video" timelineHeight="120" />
  <TimelineLabels name="videoLabels" toName="video">
    <Label value="smoke" background="#e74c3c"/>
    <Label value="fire" background="#e67e22"/>
  </TimelineLabels>
</View>"""


def test_real_footage_image_config_is_byte_identical():
    """실사 프로젝트 label_config 는 한 글자도 바뀌지 않는다 (기본값 synthetic=False)."""
    assert _image_label_config(["smoke", "fire"]) == _LEGACY_IMAGE


def test_real_footage_video_config_is_byte_identical():
    assert _video_label_config(["smoke", "fire"]) == _LEGACY_VIDEO


def test_real_footage_config_has_no_synthetic_markup():
    """`Header`/`Choices`/`$provenance` 중 하나라도 새면 기존 프로젝트가 영향을 받는다."""
    for cfg in (_image_label_config(["smoke"]), _video_label_config(["smoke"])):
        assert "Choices" not in cfg
        assert "Header" not in cfg
        assert "provenance" not in cfg
        assert SYNTHETIC_QC_NAME not in cfg


# ---------------------------------------------------------------------------
# 2. 합성 경로 — 출처 + 4개 판정
# ---------------------------------------------------------------------------


def test_synthetic_image_config_carries_provenance_and_four_criteria():
    cfg = _image_label_config(["smoke"], synthetic=True)
    root = ET.fromstring(cfg)  # LS 에 보내기 전에 깨진 XML 을 여기서 잡는다

    assert 'value="$provenance"' in cfg, "출처 한 줄이 화면에 안 뜨면 구분 수단이 없다"

    choices = root.find(f".//Choices[@name='{SYNTHETIC_QC_NAME}']")
    assert choices is not None
    assert choices.get("toName") == "image"
    values = [c.get("value") for c in choices.findall("Choice")]
    assert values == list(SYNTHETIC_QC_CRITERIA)
    assert len(values) == 4

    # 기존 라벨링 UI 는 그대로 살아 있어야 한다 — 배지만 붙이고 bbox 를 뺏으면 안 된다.
    rect = root.find(".//RectangleLabels")
    assert rect is not None and rect.get("toName") == "image"
    assert [label.get("value") for label in rect.findall("Label")] == ["smoke"]


def test_synthetic_video_config_binds_criteria_to_the_video_tag():
    """video 배치(kling/veo)도 같은 판정을 받는다. toName 이 틀리면 LS 가 config 를 거부한다."""
    cfg = _video_label_config(["falldown"], synthetic=True)
    root = ET.fromstring(cfg)

    choices = root.find(f".//Choices[@name='{SYNTHETIC_QC_NAME}']")
    assert choices is not None
    assert choices.get("toName") == "video"
    assert len(choices.findall("Choice")) == 4

    timeline = root.find(".//TimelineLabels")
    assert timeline is not None
    assert [label.get("value") for label in timeline.findall("Label")] == ["falldown"]


def test_four_criteria_cover_phase_d3():
    """설계서 Phase D.3 의 네 판정 — 이벤트 실재 / 배경 보존 / artifact 허용 / bbox 정합."""
    joined = " ".join(SYNTHETIC_QC_CRITERIA)
    for token in ("이벤트", "geometry", "artifact", "bbox"):
        assert token in joined


# ---------------------------------------------------------------------------
# 3. data 필드 — 기계 판독용 출처
# ---------------------------------------------------------------------------


def test_provenance_fields_are_machine_readable():
    prov = build_synthetic_provenance("comfy_local", "b4fe9a6f-339", ["smoke"])
    assert prov["synthetic"] == "true"
    assert prov["genai_engine"] == "comfy_local"
    assert prov["genai_batch_id"] == "b4fe9a6f-339"
    assert prov["intended_event"] == "smoke"
    # 사람이 읽는 한 줄에도 셋이 다 들어간다 (label_config 가 이 키 하나만 렌더한다).
    for part in ("comfy_local", "b4fe9a6f-339", "smoke"):
        assert part in prov["provenance"]


def test_missing_provenance_degrades_to_unknown_not_to_silence():
    """값이 없다고 키를 빼면 배지가 빈 줄이 된다 — 그건 표시가 없는 것과 같다."""
    prov = build_synthetic_provenance(None, "", [])
    assert prov["synthetic"] == "true"
    assert prov["genai_engine"] == "unknown"
    assert prov["genai_batch_id"] == "unknown"
    assert prov["intended_event"] == "unknown"


def test_multiple_intended_events_are_listed():
    prov = build_synthetic_provenance("comfy_local", "b1", ["smoke", " fire ", ""])
    assert prov["intended_event"] == "smoke, fire"


# ---------------------------------------------------------------------------
# 4. task payload — LS `data` 에 실제로 실리는가
# ---------------------------------------------------------------------------


class _FakeResp:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return {"id": 1}


def _capture_post(monkeypatch, module):
    """module.requests.post 를 가로채 마지막 payload 를 돌려준다."""
    captured: dict = {}

    def fake_post(url, headers=None, json=None, **kwargs):  # noqa: A002 - requests 시그니처
        captured["url"] = url
        captured["json"] = json
        return _FakeResp(json)

    monkeypatch.setattr(module.requests, "post", fake_post)
    return captured


def test_real_footage_task_payload_is_unchanged(monkeypatch):
    """extra 를 안 주면 payload 는 예전 그대로 — 실사 task 는 필드 하나 안 늘어난다."""
    from gemini import ls_tasks_create

    captured = _capture_post(monkeypatch, ls_tasks_create)
    ls_tasks_create.create_image_task("http://ls", {}, 7, "http://minio/img.png", "folderA")
    assert captured["json"] == {"project": 7, "data": {"image": "http://minio/img.png", "folder": "folderA"}}

    ls_tasks_create.create_task("http://ls", {}, 7, "http://minio/v.mp4", "folderA")
    assert captured["json"] == {"project": 7, "data": {"video": "http://minio/v.mp4", "folder": "folderA"}}


def test_synthetic_task_payload_carries_provenance(monkeypatch):
    from gemini import ls_tasks_create

    captured = _capture_post(monkeypatch, ls_tasks_create)
    prov = build_synthetic_provenance("comfy_local", "b4fe9a6f-339", ["smoke"])
    ls_tasks_create.create_image_task("http://ls", {}, 7, "http://minio/img.png", "folderA", extra=prov)

    data = captured["json"]["data"]
    assert data["image"] == "http://minio/img.png"
    assert data["synthetic"] == "true"
    assert data["genai_engine"] == "comfy_local"
    assert data["genai_batch_id"] == "b4fe9a6f-339"
    assert data["intended_event"] == "smoke"


def test_extra_cannot_clobber_the_media_url(monkeypatch):
    """presigned URL 이 extra 에 덮이면 검수자가 아무것도 못 연다 — 미디어 키가 항상 이긴다."""
    from gemini import ls_tasks_create

    captured = _capture_post(monkeypatch, ls_tasks_create)
    ls_tasks_create.create_image_task(
        "http://ls", {}, 7, "http://minio/real.png", "folderA", extra={"image": "http://evil", "folder": "x"}
    )
    assert captured["json"]["data"]["image"] == "http://minio/real.png"
    assert captured["json"]["data"]["folder"] == "folderA"


# ---------------------------------------------------------------------------
# 5. `_create_image` 배선 — 프로젝트 생성과 task 생성 양쪽에 도달하는가
# ---------------------------------------------------------------------------


class _Args:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _image_args(**overrides):
    base = dict(
        prefix="genai_b4fe9a6f-339",
        categories="smoke",
        project_suffix="260921_1200",
        synthetic=False,
        genai_engine="",
        genai_batch_id="",
        ls_url="http://ls",
        bucket="vlm-raw",
        label_bucket="vlm-labels",
        processed_bucket="vlm-processed",
        fps=24,
    )
    base.update(overrides)
    return _Args(**base)


def _run_create_image(monkeypatch, args) -> tuple[dict, list[dict]]:
    """`_create_image` 를 외부 I/O 없이 돌리고 (프로젝트 생성 인자, task extra 목록) 반환."""
    from gemini import ls_tasks_create

    monkeypatch.setenv("LS_TASK_GATE_ENABLED", "false")

    project_call: dict = {}
    task_extras: list[dict] = []

    def fake_ensure(ls_url, headers, title, label_config):
        project_call.update({"title": title, "label_config": label_config})
        return 999

    def fake_create_image_task(ls_url, headers, project_id, image_url, folder, extra=None):
        task_extras.append(dict(extra or {}))
        return {"id": len(task_extras)}

    monkeypatch.setattr(ls_tasks_create, "_ensure_dated_project", fake_ensure)
    monkeypatch.setattr(ls_tasks_create, "fetch_existing_task_image_stems", lambda *a, **k: {})
    monkeypatch.setattr(
        ls_tasks_create, "list_sam3_json_keys", lambda *a, **k: {"img1": "pfx/sam3_segmentations/img1.json"}
    )
    monkeypatch.setattr(
        ls_tasks_create,
        "read_json_from_minio",
        lambda *a, **k: {
            "images": [{"file_name": "genai_b4fe9a6f-339/img1.png", "width": 100, "height": 100}],
            "annotations": [{"bbox": [1, 2, 3, 4], "category_id": 1, "score": 0.99}],
            "categories": [{"id": 1, "name": "smoke"}],
        },
    )
    monkeypatch.setattr(ls_tasks_create, "generate_presigned_url", lambda *a, **k: "http://minio/img1.png?sig=1")
    monkeypatch.setattr(ls_tasks_create, "create_image_task", fake_create_image_task)
    monkeypatch.setattr(ls_tasks_create, "create_prediction", lambda *a, **k: {"id": 1})
    monkeypatch.setattr(ls_tasks_create, "_get_review_state_fns", lambda: (lambda: {}, lambda *a, **k: None))

    ls_tasks_create._create_image(args, object(), {})
    return project_call, task_extras


def test_create_image_marks_synthetic_batches_end_to_end(monkeypatch):
    """label_config(프로젝트 생성 시점) 와 task data 양쪽에 도달해야 실제로 보인다."""
    project_call, task_extras = _run_create_image(
        monkeypatch,
        _image_args(synthetic=True, genai_engine="comfy_local", genai_batch_id="b4fe9a6f-339"),
    )
    assert SYNTHETIC_QC_NAME in project_call["label_config"]
    assert "$provenance" in project_call["label_config"]
    assert task_extras and task_extras[0]["genai_engine"] == "comfy_local"
    assert task_extras[0]["genai_batch_id"] == "b4fe9a6f-339"
    assert task_extras[0]["synthetic"] == "true"


def test_create_image_leaves_real_batches_alone(monkeypatch):
    """합성이 아니면 label_config 는 기존 그대로이고 task data 도 안 늘어난다."""
    project_call, task_extras = _run_create_image(monkeypatch, _image_args(synthetic=False))
    assert project_call["label_config"] == _image_label_config(["smoke"])
    assert SYNTHETIC_QC_NAME not in project_call["label_config"]
    assert task_extras == [{}]


# ---------------------------------------------------------------------------
# 6. sensor — 출처를 어디서 읽어 어떻게 넘기는가
# ---------------------------------------------------------------------------


def test_batch_id_is_derived_from_the_folder_contract():
    """`source_unit_name = genai_<batch_id>` 가 유일한 복원 경로다 (raw_files 에 batch 컬럼 없음)."""
    from vlm_pipeline.defs.ls.sensor import _genai_batch_id

    assert _genai_batch_id("genai_b4fe9a6f-339") == "b4fe9a6f-339"
    # 규약 밖 폴더는 조용히 빈 값 — 실사 폴더명에서 엉뚱한 batch 를 지어내면 안 된다.
    assert _genai_batch_id("sourcei_v2") == ""
    assert _genai_batch_id("genai_") == ""
    assert _genai_batch_id("") == ""
    assert _genai_batch_id(None) == ""


def test_synthetic_argv_keeps_the_existing_flag_and_adds_provenance():
    """`--synthetic` 는 그대로 나가고(게이트 면제 배선 보존) 출처만 덧붙는다."""
    from vlm_pipeline.defs.ls.sensor import _synthetic_argv

    assert _synthetic_argv("comfy_local", "b4fe9a6f-339") == [
        "--synthetic",
        "--genai-engine",
        "comfy_local",
        "--genai-batch-id",
        "b4fe9a6f-339",
    ]
    # 값이 없으면 빈 문자열을 넘기지 않고 플래그를 뺀다 → CLI 기본값이 'unknown' 으로 표시.
    assert _synthetic_argv("", "") == ["--synthetic"]


def test_create_cli_accepts_the_provenance_flags():
    """sensor 가 넘기는 인자를 CLI 가 실제로 받는가 — 오타 하나면 exit=2 로 전부 실패한다."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("ls_tasks_cli_check", _SRC / "gemini" / "ls_tasks.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    src = (_SRC / "gemini" / "ls_tasks.py").read_text(encoding="utf-8")
    for flag in ("--genai-engine", "--genai-batch-id", "--synthetic"):
        assert f'"{flag}"' in src
