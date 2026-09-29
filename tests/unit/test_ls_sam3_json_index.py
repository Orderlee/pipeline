"""LS image task 의 SAM3 JSON 열거 — pseudo 스냅샷이 태스크가 되면 안 된다.

2026-09-21 실측: comfy_local 배치를 promote 했더니 이미지 1장인데 LS 프로젝트에 task 가
2건 생겼다. 원인은 `Path(key).stem` 이 `<stem>.pseudo.json` 에서 `<stem>.pseudo` 라는
**별개 stem** 을 만들어 같은 이미지가 두 번 색인된 것. vlm-labels 의 SAM3 JSON 39,056건
중 19,528건이 pseudo 라, 이건 합성 데이터만의 문제가 아니라 모든 SAM3 코호트에 해당한다.

중복 검수보다 심각한 건 되쓰기다. pseudo 태스크에 단 주석을 ls_sync 가
`<stem>.pseudo.json` 으로 되돌려 쓰면 "검수 전 모델 출력" 스냅샷이 사람 수정본으로
덮이고, pseudo vs GT 품질평가의 전제가 조용히 사라진다.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from gemini.ls_tasks_minio import list_sam3_json_keys  # noqa: E402


class _FakeClient:
    def __init__(self, keys):
        self._keys = keys

    def get_paginator(self, _name):
        keys = self._keys

        class _P:
            def paginate(self, **_kw):
                yield {"Contents": [{"Key": k} for k in keys]}

        return _P()


def test_pseudo_snapshot_is_not_a_task_source():
    client = _FakeClient(
        [
            "site/sam3_segmentations/frame_001.json",
            "site/sam3_segmentations/frame_001.pseudo.json",
            "site/sam3_segmentations/frame_002.json",
            "site/sam3_segmentations/frame_002.pseudo.json",
        ]
    )
    index = list_sam3_json_keys(client, "vlm-labels", "site/")
    assert set(index) == {"frame_001", "frame_002"}, "이미지 1장당 항목 1개여야 한다"
    assert all(not v.endswith(".pseudo.json") for v in index.values())


def test_non_sam3_and_non_json_keys_are_ignored():
    client = _FakeClient(
        [
            "site/sam3_segmentations/frame_001.json",
            "site/events/frame_001.json",  # 다른 산출물
            "site/sam3_segmentations/frame_001.png",  # json 아님
        ]
    )
    index = list_sam3_json_keys(client, "vlm-labels", "site/")
    assert index == {"frame_001": "site/sam3_segmentations/frame_001.json"}
