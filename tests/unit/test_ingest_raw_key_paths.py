from __future__ import annotations

from vlm_pipeline.lib.env_utils import storage_raw_key_prefix_from_source_unit
from vlm_pipeline.lib.sanitizer import make_unique_key, sanitize_filename


def test_storage_raw_key_prefix_from_source_unit_strips_gcp_prefix() -> None:
    assert (
        storage_raw_key_prefix_from_source_unit("gcp/source-c-event-bucket/20260330")
        == "source-c-event-bucket/20260330"
    )


def test_storage_raw_key_prefix_from_source_unit_keeps_non_gcp_prefix() -> None:
    assert storage_raw_key_prefix_from_source_unit("tmp_data_2") == "tmp_data_2"


def test_storage_raw_key_prefix_from_source_unit_sanitizes_components() -> None:
    assert storage_raw_key_prefix_from_source_unit("gcp/My Bucket/2026 03 30") == "my_bucket/2026_03_30"


# ── make_unique_key 테스트 ──────────────────────────────────────


def test_make_unique_key_no_collision() -> None:
    seen: set[str] = set()
    assert make_unique_key("folder/file.mp4", seen) == "folder/file.mp4"
    assert "folder/file.mp4" in seen


def test_make_unique_key_batch_collision() -> None:
    """같은 키를 연속 요청하면 _2, _3 suffix가 붙는다."""
    seen: set[str] = set()
    k1 = make_unique_key("folder/video.mp4", seen)
    k2 = make_unique_key("folder/video.mp4", seen)
    k3 = make_unique_key("folder/video.mp4", seen)
    assert k1 == "folder/video.mp4"
    assert k2 == "folder/video_2.mp4"
    assert k3 == "folder/video_3.mp4"


def test_make_unique_key_db_existing() -> None:
    """DB에 이미 존재하는 키는 건너뛴다."""
    seen: set[str] = {"folder/video.mp4", "folder/video_2.mp4"}
    result = make_unique_key("folder/video.mp4", seen)
    assert result == "folder/video_3.mp4"


def test_make_unique_key_preserves_extension() -> None:
    seen: set[str] = {"img.jpg"}
    result = make_unique_key("img.jpg", seen)
    assert result == "img_2.jpg"
    assert result.endswith(".jpg")


def test_make_unique_key_no_extension() -> None:
    seen: set[str] = {"readme"}
    result = make_unique_key("readme", seen)
    assert result == "readme_2"


def test_make_unique_key_nested_path() -> None:
    """경로 구분자가 포함된 키에서도 stem 부분에만 suffix가 붙는다."""
    seen: set[str] = {"a/b/c.mp4"}
    result = make_unique_key("a/b/c.mp4", seen)
    assert result == "a/b/c_2.mp4"


# ── sanitize_filename 충돌 재현 테스트 ──────────────────────────


def test_sanitize_filename_trailing_underscore_collision() -> None:
    """실제 사건: 끝에 _ 차이가 정규화 후 같은 이름이 되는 케이스."""
    name_a = "kling_20260407_作品_shot_1_5s__2656_0.mp4"
    name_b = "kling_20260407_作品_shot_1_5s__2656_0_.mp4"
    sanitized_a = sanitize_filename(name_a)
    sanitized_b = sanitize_filename(name_b)
    assert sanitized_a == sanitized_b, "정규화 후 동일해야 충돌이 재현됨"

    seen: set[str] = set()
    key_a = make_unique_key(f"folder/{sanitized_a}", seen)
    key_b = make_unique_key(f"folder/{sanitized_b}", seen)
    assert key_a != key_b, "make_unique_key가 충돌을 방지해야 함"
    assert key_b == f"folder/{sanitized_a.replace('.mp4', '_2.mp4')}"
