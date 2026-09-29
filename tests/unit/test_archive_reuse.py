"""Archive 디렉토리 재사용 — dispatch 반복 시 기존 archive에 병합되는지 검증.

순수 함수(find_existing_archive_directory, resolve_unique_directory 등)는 직접 테스트하고,
archive_uploaded_assets는 Dagster context/DuckDB를 mock하여 통합 테스트한다.
"""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from unittest import mock


_MISSING = object()


def _stub_modules():
    """Dagster/pydantic 의존 없이 archive.py를 로드하기 위한 stub.

    주의: stub_names 중 일부는 **이미 실제로 import 돼 있을 수 있다**. 그 경우 아래
    setattr 들은 진짜 모듈을 덮어쓰므로, 되돌리지 않으면 같은 pytest 세션의 뒤따르는
    테스트가 오염된다 (실측: env_utils.int_env 가 stub 으로 남아 GEMINI_* 를 읽는
    test_gemini_video_preview 가 ffmpeg 를 아예 호출하지 않게 됨). 덮어쓴 속성을
    patched 로 기록해 _load_archive_module() 의 finally 에서 복원한다.
    """
    stubs: dict[str, types.ModuleType] = {}
    patched: list[tuple[types.ModuleType, str, object]] = []

    def _set(mod: types.ModuleType, attr: str, value: object) -> None:
        patched.append((mod, attr, getattr(mod, attr, _MISSING)))
        setattr(mod, attr, value)

    stub_names = [
        "vlm_pipeline.resources.config",
        "vlm_pipeline.resources",
        "vlm_pipeline.resources.duckdb_base",
        "vlm_pipeline.resources.duckdb_phash",
        "vlm_pipeline.resources.duckdb_migration",
        "vlm_pipeline.resources.minio_resource",
        "vlm_pipeline.resources.duckdb",
        "vlm_pipeline.lib.env_utils",
        "vlm_pipeline.lib.runtime_profile",
        "dagster",
        "pydantic_settings",
    ]
    for mod_name in stub_names:
        if mod_name not in sys.modules:
            stubs[mod_name] = types.ModuleType(mod_name)
            sys.modules[mod_name] = stubs[mod_name]

    _set(sys.modules["vlm_pipeline.lib.env_utils"], "int_env", lambda key, default=0, minimum=0, **kw: default)

    profile_stub = sys.modules["vlm_pipeline.lib.runtime_profile"]
    _set(profile_stub, "RuntimeProfile", type("RuntimeProfile", (), {}))
    _set(profile_stub, "resolve_runtime_profile", lambda *a, **kw: None)

    _set(sys.modules["vlm_pipeline.resources.config"], "PipelineConfig", type("PipelineConfig", (), {}))
    _set(sys.modules["vlm_pipeline.resources.duckdb"], "DuckDBResource", type("DuckDBResource", (), {}))

    return stubs, patched


def _load_archive_module():
    stubs, patched = _stub_modules()
    try:
        if "vlm_pipeline.defs.ingest.archive" in sys.modules:
            del sys.modules["vlm_pipeline.defs.ingest.archive"]
        mod = importlib.import_module("vlm_pipeline.defs.ingest.archive")
    finally:
        for mod_name, stub in stubs.items():
            if sys.modules.get(mod_name) is stub:
                del sys.modules[mod_name]
        # 실제 모듈에 덮어쓴 속성 복원 — 없으면 세션 전체가 오염된다 (위 docstring 참고).
        for target, attr, original in reversed(patched):
            if original is _MISSING:
                delattr(target, attr)
            else:
                setattr(target, attr, original)
    return mod


_archive = _load_archive_module()
find_existing_archive_directory = _archive.find_existing_archive_directory
resolve_unique_directory = _archive.resolve_unique_directory
resolve_unique_file = _archive.resolve_unique_file
archive_uploaded_assets = _archive.archive_uploaded_assets


class TestFindExistingArchiveDirectory:
    def test_returns_base_when_exists(self, tmp_path: Path) -> None:
        d = tmp_path / "MyFolder"
        d.mkdir()
        assert find_existing_archive_directory(d) == d

    def test_returns_suffix_variant(self, tmp_path: Path) -> None:
        base = tmp_path / "MyFolder"
        suffixed = tmp_path / "MyFolder__2"
        suffixed.mkdir()
        assert find_existing_archive_directory(base) == suffixed

    def test_returns_none_when_nothing_exists(self, tmp_path: Path) -> None:
        base = tmp_path / "MyFolder"
        assert find_existing_archive_directory(base) is None


class TestResolveUniqueDirectory:
    def test_returns_base_when_not_exists(self, tmp_path: Path) -> None:
        d = tmp_path / "MyFolder"
        assert resolve_unique_directory(d) == d

    def test_returns_suffix_when_exists(self, tmp_path: Path) -> None:
        d = tmp_path / "MyFolder"
        d.mkdir()
        result = resolve_unique_directory(d)
        assert result == tmp_path / "MyFolder__2"

    def test_increments_suffix(self, tmp_path: Path) -> None:
        d = tmp_path / "MyFolder"
        d.mkdir()
        (tmp_path / "MyFolder__2").mkdir()
        result = resolve_unique_directory(d)
        assert result == tmp_path / "MyFolder__3"


class TestArchiveUploadedAssetsReuse:
    """archive_uploaded_assets가 기존 archive 디렉토리를 재사용하는지 검증."""

    def _make_context(self):
        ctx = mock.MagicMock()
        ctx.log.info = mock.MagicMock()
        ctx.log.warning = mock.MagicMock()
        return ctx

    def _make_db(self):
        db = mock.MagicMock()
        db.update_raw_file_status = mock.MagicMock()
        return db

    def test_second_dispatch_reuses_existing_archive_dir(self, tmp_path: Path) -> None:
        """같은 source_unit_name으로 2회 archive 시 같은 디렉토리에 병합."""
        incoming = tmp_path / "incoming" / "TestFolder"
        archive_root = tmp_path / "archive"
        archive_root.mkdir(parents=True)

        # 1차 dispatch: incoming에 파일 2개
        incoming.mkdir(parents=True)
        (incoming / "a.mp4").write_bytes(b"\x00" * 10)
        (incoming / "b.mp4").write_bytes(b"\x00" * 20)

        manifest1 = {
            "source_unit_type": "directory",
            "source_unit_name": "TestFolder",
            "source_unit_path": str(incoming),
            "source_dir": str(tmp_path / "incoming"),
            "file_count": 2,
            "source_unit_total_file_count": 2,
            "source_unit_chunk_count": 1,
            "files": [
                {"path": str(incoming / "a.mp4"), "rel_path": "a.mp4"},
                {"path": str(incoming / "b.mp4"), "rel_path": "b.mp4"},
            ],
        }
        uploaded1 = [
            {"asset_id": "id1", "source_path": str(incoming / "a.mp4"), "media_type": "video"},
            {"asset_id": "id2", "source_path": str(incoming / "b.mp4"), "media_type": "video"},
        ]

        ctx = self._make_context()
        db = self._make_db()
        archived1, hint1 = archive_uploaded_assets(ctx, db, manifest1, uploaded1, str(archive_root))

        archive_dir_1 = archive_root / "TestFolder"
        assert archive_dir_1.exists()
        assert (archive_dir_1 / "a.mp4").exists()
        assert (archive_dir_1 / "b.mp4").exists()
        assert not (archive_root / "TestFolder__2").exists()

        # 2차 dispatch: incoming에 새 파일 2개
        incoming.mkdir(parents=True, exist_ok=True)
        (incoming / "c.mp4").write_bytes(b"\x00" * 30)
        (incoming / "d.mp4").write_bytes(b"\x00" * 40)

        manifest2 = {
            "source_unit_type": "directory",
            "source_unit_name": "TestFolder",
            "source_unit_path": str(incoming),
            "source_dir": str(tmp_path / "incoming"),
            "file_count": 2,
            "source_unit_total_file_count": 2,
            "source_unit_chunk_count": 1,
            "files": [
                {"path": str(incoming / "c.mp4"), "rel_path": "c.mp4"},
                {"path": str(incoming / "d.mp4"), "rel_path": "d.mp4"},
            ],
        }
        uploaded2 = [
            {"asset_id": "id3", "source_path": str(incoming / "c.mp4"), "media_type": "video"},
            {"asset_id": "id4", "source_path": str(incoming / "d.mp4"), "media_type": "video"},
        ]

        archived2, hint2 = archive_uploaded_assets(ctx, db, manifest2, uploaded2, str(archive_root))

        # __2 폴더가 생기지 않고 기존 디렉토리에 병합
        assert not (archive_root / "TestFolder__2").exists()
        assert archive_dir_1.exists()
        assert (archive_dir_1 / "c.mp4").exists()
        assert (archive_dir_1 / "d.mp4").exists()
        assert hint2 == archive_dir_1

    def test_first_dispatch_creates_new_dir(self, tmp_path: Path) -> None:
        """기존 archive 디렉토리가 없으면 새로 생성."""
        incoming = tmp_path / "incoming" / "NewFolder"
        incoming.mkdir(parents=True)
        (incoming / "x.mp4").write_bytes(b"\x00" * 10)

        archive_root = tmp_path / "archive"
        archive_root.mkdir(parents=True)

        manifest = {
            "source_unit_type": "directory",
            "source_unit_name": "NewFolder",
            "source_unit_path": str(incoming),
            "source_dir": str(tmp_path / "incoming"),
            "file_count": 1,
            "source_unit_total_file_count": 1,
            "source_unit_chunk_count": 1,
            "files": [{"path": str(incoming / "x.mp4"), "rel_path": "x.mp4"}],
        }
        uploaded = [{"asset_id": "id1", "source_path": str(incoming / "x.mp4"), "media_type": "video"}]

        ctx = self._make_context()
        db = self._make_db()
        archived, hint = archive_uploaded_assets(ctx, db, manifest, uploaded, str(archive_root))

        assert (archive_root / "NewFolder").exists()
        assert (archive_root / "NewFolder" / "x.mp4").exists()

    def test_file_name_collision_uses_suffix_on_file(self, tmp_path: Path) -> None:
        """같은 파일명이 이미 archive에 있으면 파일에만 __2 suffix."""
        incoming = tmp_path / "incoming" / "Folder"
        archive_root = tmp_path / "archive"
        archive_dir = archive_root / "Folder"
        archive_dir.mkdir(parents=True)
        (archive_dir / "video.mp4").write_bytes(b"\x00" * 10)

        incoming.mkdir(parents=True)
        (incoming / "video.mp4").write_bytes(b"\xff" * 20)

        manifest = {
            "source_unit_type": "directory",
            "source_unit_name": "Folder",
            "source_unit_path": str(incoming),
            "source_dir": str(tmp_path / "incoming"),
            "file_count": 1,
            "source_unit_total_file_count": 1,
            "source_unit_chunk_count": 1,
            "files": [{"path": str(incoming / "video.mp4"), "rel_path": "video.mp4"}],
        }
        uploaded = [{"asset_id": "id1", "source_path": str(incoming / "video.mp4"), "media_type": "video"}]

        ctx = self._make_context()
        db = self._make_db()
        archived, hint = archive_uploaded_assets(ctx, db, manifest, uploaded, str(archive_root))

        assert not (archive_root / "Folder__2").exists()
        assert (archive_dir / "video.mp4").exists()
        assert (archive_dir / "video__2.mp4").exists()
        assert hint == archive_dir
