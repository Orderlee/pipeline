"""scripts/dataset_pull.py + lib.dvc_pull — resolve via catalog, dvc get mocked, verify md5.

Dry-run default → no dvc invoked, no bytes moved (scaffolding only).
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "src"))

from vlm_pipeline.lib.dvc_pull import build_dvc_get_argv, compute_dvc_md5, verify_pulled_md5  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "dataset_pull", str((pathlib.Path(__file__).resolve().parents[2] / "scripts" / "dataset_pull.py"))
)
dataset_pull = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(dataset_pull)


def test_build_dvc_get_argv():
    argv = build_dvc_get_argv("/srv/data-repos/dvc-datasets.git", "fire_v3", "abc123", "/tmp/out")
    assert argv == ["dvc", "get", "/srv/data-repos/dvc-datasets.git", "fire_v3", "--rev", "abc123", "-o", "/tmp/out"]


def test_verify_pulled_md5():
    assert verify_pulled_md5(None, "anything") is True  # nothing to verify
    assert verify_pulled_md5("deadbeef", "deadbeef") is True
    assert verify_pulled_md5("deadbeef", "cafef00d") is False


def test_compute_dvc_md5_matches_dvc_for_a_file(tmp_path):
    f = tmp_path / "a.txt"
    f.write_bytes(b"hello\n")
    # dvc 3.67.1 이 같은 내용에 대해 내는 값 (컨테이너에서 `dvc add` 로 대조 확인).
    assert compute_dvc_md5(str(f)) == "b1946ac92492d2347c6235b4d2611184"


def test_compute_dvc_md5_matches_dvc_for_a_directory(tmp_path):
    """디렉토리는 DVC 의 dir-hash 규칙(<hash>.dir)을 따라야 한다.

    기대값은 dvc 3.67.1 로 실제 `dvc add data` 를 돌려 얻은 값이다 — 우리 계산이
    DVC 와 어긋나면 실 pull 이 정상 바이트에도 mismatch(exit 3)를 낸다.
    """
    data = tmp_path / "data"
    (data / "sub").mkdir(parents=True)
    (data / "a.txt").write_bytes(b"hello\n")
    (data / "b.bin").write_bytes(b"world world\n")
    (data / "sub" / "c.txt").write_bytes(b"nested\n")
    assert compute_dvc_md5(str(data)) == "846b769158c938838ce462bb0a116d21.dir"


def test_compute_dvc_md5_is_none_for_missing_path(tmp_path):
    assert compute_dvc_md5(str(tmp_path / "nope")) is None


def test_real_pull_does_not_fail_on_matching_bytes(monkeypatch, tmp_path, capsys):
    """회귀 방지: `_computed_md5()` 스텁이 항상 None 이라 실 pull 이 늘 exit 3 였다.

    바이트가 맞는데 실패하는 '가짜 실패' 는 검증이 없는 것보다 나쁘다.
    """
    dest = tmp_path / "pulled"
    dest.mkdir()
    (dest / "a.txt").write_bytes(b"hello\n")
    expected = compute_dvc_md5(str(dest))

    class _DB:
        def get_catalog_by_alias(self, task, alias="current"):
            return {"git_rev": "abc123", "dvc_out_path": "fire_v3", "dvc_md5": expected}

    monkeypatch.setattr(dataset_pull, "_open_db", lambda: _DB())
    monkeypatch.setattr(dataset_pull, "_repo_path", lambda: "/srv/data-repos/dvc-datasets.git")
    monkeypatch.setattr(dataset_pull, "_run_dvc_get", lambda *a, **k: None)  # 바이트는 이미 dest 에 있음

    rc = dataset_pull.main(["--task", "t", "--dest", str(dest), "--no-dry-run"])
    assert rc == 0, capsys.readouterr().err


def test_real_pull_detects_corrupted_bytes(monkeypatch, tmp_path):
    dest = tmp_path / "pulled"
    dest.mkdir()
    (dest / "a.txt").write_bytes(b"tampered\n")

    class _DB:
        def get_catalog_by_alias(self, task, alias="current"):
            return {"git_rev": "abc123", "dvc_out_path": "fire_v3", "dvc_md5": "deadbeef.dir"}

    monkeypatch.setattr(dataset_pull, "_open_db", lambda: _DB())
    monkeypatch.setattr(dataset_pull, "_repo_path", lambda: "/srv/data-repos/dvc-datasets.git")
    monkeypatch.setattr(dataset_pull, "_run_dvc_get", lambda *a, **k: None)

    assert dataset_pull.main(["--task", "t", "--dest", str(dest), "--no-dry-run"]) == 3


def test_dry_run_resolves_but_does_not_invoke_dvc(monkeypatch, capsys):
    invoked = {"dvc": False}

    class _DB:
        def get_catalog_by_alias(self, task, alias="current"):
            assert task == "sam3_detection"
            return {
                "dataset_catalog_id": "cid-1",
                "git_rev": "abc123",
                "dvc_out_path": "fire_v3",
                "dvc_md5": "deadbeef",
                "status": "pinned",
                "commit_subject": "curate: fire v3",
            }

    monkeypatch.setattr(dataset_pull, "_open_db", lambda: _DB())
    monkeypatch.setattr(dataset_pull, "_repo_path", lambda: "/srv/data-repos/dvc-datasets.git")
    monkeypatch.setattr(
        dataset_pull,
        "_run_dvc_get",
        lambda *a, **k: invoked.__setitem__("dvc", True),
    )

    rc = dataset_pull.main(["--task", "sam3_detection", "--alias", "current", "--dest", "/tmp/x"])
    assert rc == 0
    assert invoked["dvc"] is False, "dry-run must NOT invoke dvc get"
    out = capsys.readouterr().out
    assert "abc123" in out and "fire_v3" in out and "DRY-RUN" in out


def test_no_pin_returns_nonzero(monkeypatch):
    class _DB:
        def get_catalog_by_alias(self, task, alias="current"):
            return None

    monkeypatch.setattr(dataset_pull, "_open_db", lambda: _DB())
    monkeypatch.setattr(dataset_pull, "_repo_path", lambda: "/srv/data-repos/dvc-datasets.git")
    rc = dataset_pull.main(["--task", "sam3_detection", "--dest", "/tmp/x"])
    assert rc == 2
