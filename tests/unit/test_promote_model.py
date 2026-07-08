"""scripts/promote_model.py 단위 테스트 (Section E 승격/롤백, built-but-not-executed).

PG cursor / MinIO / subprocess(docker) 모두 스텁. dry-run 기본이므로 prod 무변경.
실 DB·실 docker 미사용 (CI GPU/도커 없음).
"""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_SPEC = importlib.util.spec_from_file_location("promote_model", str(pathlib.Path("scripts/promote_model.py").resolve()))
promote_model = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(promote_model)


class _FakeCursor:
    """rowcount/fetchone/fetchall 를 시퀀스로 반환하는 최소 psycopg2 cursor 스텁."""

    def __init__(self, results):
        self._results = list(results)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._results.pop(0) if self._results else None

    def fetchall(self):
        out = self._results.pop(0) if self._results else []
        return out


def test_select_promotable_returns_the_promotable_row():
    row = {
        "model_version_id": 7,
        "model": "pe_core",
        "version": "ft-2026.06.29-lora-001",
        "status": "promotable",
        "checkpoint_key": "_models/pe_core/ft-2026.06.29-lora-001/merged.pt",
        "artifact_checksum": "abc123",
    }
    cur = _FakeCursor([row])
    got = promote_model.select_promotable_row(cur, model="pe_core", model_version_id=7)
    assert got["model_version_id"] == 7
    # WHERE status='promotable' 가 SQL 에 박혀 있어야 함
    sql = cur.executed[0][0].lower()
    assert "status" in sql and "promotable" in sql


def test_select_promotable_raises_when_none():
    cur = _FakeCursor([None])
    with pytest.raises(promote_model.PromotionError):
        promote_model.select_promotable_row(cur, model="pe_core", model_version_id=99)


class _FakeMinio:
    def __init__(self, payloads):
        # payloads: {object_name: bytes}
        self._payloads = payloads
        self.downloaded = []

    def fget_object(self, bucket, object_name, file_path):
        self.downloaded.append((bucket, object_name, file_path))
        data = self._payloads[object_name]
        with open(file_path, "wb") as fh:
            fh.write(data)


def _sha256(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def test_download_and_verify_ok(tmp_path):
    payload = b"merged-weights"
    key = "_models/pe_core/v1/merged.pt"
    mc = _FakeMinio({key: payload})
    dest = tmp_path / "pe-core" / "merged.pt"
    out = promote_model.download_and_verify(
        mc,
        checkpoint_key=key,
        artifact_checksum=_sha256(payload),
        dest_path=dest,
        dry_run=False,
    )
    assert out == dest
    assert dest.read_bytes() == payload
    assert mc.downloaded[0][0] == "vlm-dataset"  # bucket
    assert mc.downloaded[0][1] == key


def test_download_and_verify_checksum_mismatch_raises_and_cleans(tmp_path):
    payload = b"merged-weights"
    key = "_models/pe_core/v1/merged.pt"
    mc = _FakeMinio({key: payload})
    dest = tmp_path / "pe-core" / "merged.pt"
    with pytest.raises(promote_model.PromotionError):
        promote_model.download_and_verify(
            mc,
            checkpoint_key=key,
            artifact_checksum="deadbeef",  # wrong
            dest_path=dest,
            dry_run=False,
        )
    assert not dest.exists()  # bad file removed → 서빙 절대 손상 안 함


def test_download_and_verify_dry_run_no_download(tmp_path):
    key = "_models/pe_core/v1/merged.pt"
    mc = _FakeMinio({key: b"x"})
    dest = tmp_path / "pe-core" / "merged.pt"
    out = promote_model.download_and_verify(
        mc, checkpoint_key=key, artifact_checksum="abc", dest_path=dest, dry_run=True
    )
    assert out == dest
    assert mc.downloaded == []  # no MinIO call
    assert not dest.exists()


def test_promote_transition_sets_promoted_and_archives_prior():
    cur = _FakeCursor([])
    row = {"model_version_id": 7, "model": "pe_core", "version": "v2"}
    promote_model.promote_transition(cur, row=row, env="prod")
    sqls = " ".join(s.lower() for s, _ in cur.executed)
    # 새 행 promoted + promoted_at/promoted_env
    assert "status = 'promoted'" in sqls
    assert "promoted_at" in sqls and "promoted_env" in sqls
    # 직전 promoted → archived
    assert "status = 'archived'" in sqls


def test_select_rollback_target_picks_prior_promoted():
    prior = {
        "model_version_id": 5,
        "model": "pe_core",
        "version": "v1",
        "status": "archived",
        "checkpoint_key": "_models/pe_core/v1/merged.pt",
        "artifact_checksum": "c5",
    }
    cur = _FakeCursor([prior])
    got = promote_model.select_rollback_target(cur, model="pe_core")
    assert got["model_version_id"] == 5
    sql = cur.executed[0][0].lower()
    assert "promoted_at is not null" in sql  # 이전에 promoted 된 적 있는 행만
    assert "order by promoted_at desc" in sql


def test_select_rollback_target_none_raises():
    cur = _FakeCursor([None])
    with pytest.raises(promote_model.PromotionError):
        promote_model.select_rollback_target(cur, model="pe_core")


def test_rollback_transition_flips_current_and_restores():
    cur = _FakeCursor([])
    restore = {"model_version_id": 5, "model": "pe_core"}
    promote_model.rollback_transition(cur, restore_row=restore, current_promoted_id=7, env="prod")
    sqls = " ".join(s.lower() for s, _ in cur.executed)
    assert "status = 'rolled_back'" in sqls  # 현 promoted → rolled_back
    assert "status = 'promoted'" in sqls  # restore → promoted


def test_docker_recreate_dry_run_no_subprocess(monkeypatch):
    calls = []
    monkeypatch.setattr(promote_model.subprocess, "run", lambda *a, **k: calls.append((a, k)))
    promote_model.docker_recreate("embedding-service", dry_run=True)
    assert calls == []


def test_docker_recreate_apply_invokes_compose(monkeypatch):
    calls = []
    monkeypatch.setattr(promote_model.subprocess, "run", lambda *a, **k: calls.append((a, k)) or _Ok())
    promote_model.docker_recreate("embedding-service", dry_run=False)
    assert calls, "subprocess.run not called on --apply"
    argv = calls[0][0][0]
    assert "up" in argv and "--force-recreate" in argv and "embedding-service" in argv


class _Ok:
    returncode = 0


def test_h5_commits_registry_before_docker_recreate(monkeypatch):
    """H-5: DB commit 이 docker_recreate 前에 일어나야 함.

    예전 순서(recreate→commit)는 그 사이 crash 시 '새 가중치 서빙 중 + registry 는 옛것 promoted'
    라는 조용한 under-report 를 남겼다. commit 이 먼저면 crash 시 loud over-report(복구 가능)로 바뀐다.
    """
    order: list[str] = []
    cur = _FakeCursor([])

    class _Conn:
        def __init__(self):
            self.autocommit = None

        def cursor(self):
            c = cur

            class _Ctx:
                def __enter__(self_inner):
                    return c

                def __exit__(self_inner, *a):
                    return False

            return _Ctx()

        def commit(self):
            order.append("commit")

        def rollback(self):
            order.append("rollback")

        def close(self):
            pass

    monkeypatch.setattr(
        promote_model,
        "select_promotable_row",
        lambda c, *, model, model_version_id: {
            "model_version_id": 1,
            "model": "sam3",
            "version": "v1",
            "checkpoint_key": "k",
            "artifact_checksum": "s",
        },
    )
    monkeypatch.setattr(promote_model, "download_and_verify", lambda *a, **k: None)
    monkeypatch.setattr(promote_model, "_write_env_var", lambda *a, **k: order.append("env_write"))
    monkeypatch.setattr(promote_model, "_make_minio", lambda: object())
    monkeypatch.setattr(promote_model, "docker_recreate", lambda service, *, dry_run: order.append("recreate"))
    import psycopg2

    monkeypatch.setattr(psycopg2, "connect", lambda dsn: _Conn())

    rc = promote_model.main(["--model", "sam3", "--dsn", "postgresql://x", "--apply"])
    assert rc == 0
    # env write must also follow commit: writing docker/.env before a rolled-back commit would
    # leave serving pointing at an unpromoted checkpoint (registry=truth violation via env file).
    assert order == [
        "commit",
        "env_write",
        "recreate",
    ], f"commit must precede env write and recreate (H-5), got {order}"
