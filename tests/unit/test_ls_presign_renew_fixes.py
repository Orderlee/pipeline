"""presign 갱신 버그 3종 회귀 테스트 (2026-07-22 인시던트).

배경: docs/design-docs/gcpls/presign-renew-bugfix.md
① 프로젝트 이름 조회가 기본 페이지(30개)만 봐서 빈 중복 프로젝트를 매일 생성
② 이미지 태스크가 빈 stem 한 키로 붕괴해 renew/dedup 이 1건만 처리
③ task data 통째 교체로 이미지 태스크의 image 키 유실
"""

from unittest.mock import MagicMock, patch

from gemini.ls_tasks_minio import fetch_existing_task_stems, find_or_create_project, update_task_url


def _resp(json_data, status=200):
    m = MagicMock()
    m.status_code = status
    m.json.return_value = json_data
    m.raise_for_status.return_value = None
    return m


class TestFindOrCreateProjectPagination:
    def test_project_lookup_requests_full_page_size(self):
        """① 이름 조회는 전체 프로젝트를 대상으로 해야 함 (기본 30개 페이지 금지)."""
        with patch("gemini.ls_tasks_minio.requests") as req:
            req.get.return_value = _resp({"results": [{"id": 7, "title": "old-project"}]})
            pid, is_new = find_or_create_project("http://ls", {}, "old-project")

        assert (pid, is_new) == (7, False)
        assert req.get.call_args.kwargs.get("params", {}).get("page_size", 0) >= 100
        req.post.assert_not_called()  # 찾았으면 생성(중복) 금지


class TestFetchExistingTaskStems:
    def test_image_tasks_do_not_collapse_into_single_key(self):
        """② 이미지 태스크(video 키 없음)가 각각 별도 키로 인덱싱되어야 함."""
        tasks = [
            {"id": 1, "data": {"image": "http://m/vlm-raw/a/x.jpg", "folder": "a"}},
            {"id": 2, "data": {"image": "http://m/vlm-raw/a/y.jpg", "folder": "a"}},
            {"id": 3, "data": {"folder": "a"}},  # 미디어 없음 — task id 로 격리
        ]
        with patch("gemini.ls_tasks_minio.requests") as req:
            req.get.return_value = _resp({"tasks": tasks})
            index = fetch_existing_task_stems("http://ls", {}, 99)

        assert len(index) == 3
        assert index["x"]["id"] == 1 and index["y"]["id"] == 2
        assert index["__task_3"]["id"] == 3


class TestUpdateTaskUrlPreservesData:
    def test_image_task_keeps_image_key_and_extra_fields(self):
        """③ 미디어 키만 교체 — image 키 유지, 그 외 data 필드 보존, video 키 미생성."""
        task = {"id": 42, "data": {"image": "http://m/vlm-raw/a/old.jpg", "folder": "a", "extra": 1}}
        with patch("gemini.ls_tasks_minio.requests") as req:
            req.patch.return_value = _resp({})
            update_task_url("http://ls", {}, task, "http://m/vlm-raw/a/new.jpg")

        sent = req.patch.call_args.kwargs["json"]["data"]
        assert sent == {"image": "http://m/vlm-raw/a/new.jpg", "folder": "a", "extra": 1}
        assert "video" not in sent
        assert "/api/tasks/42/" in req.patch.call_args.args[0]

    def test_video_task_updates_video_key(self):
        task = {"id": 7, "data": {"video": "http://m/vlm-raw/b/old.mp4", "folder": "b"}}
        with patch("gemini.ls_tasks_minio.requests") as req:
            req.patch.return_value = _resp({})
            update_task_url("http://ls", {}, task, "http://m/vlm-raw/b/new.mp4")

        sent = req.patch.call_args.kwargs["json"]["data"]
        assert sent == {"video": "http://m/vlm-raw/b/new.mp4", "folder": "b"}
