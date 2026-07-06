"""sourcea_site_download asset — preflight skip 경로와 happy path."""
from datetime import datetime
from unittest.mock import patch

from dagster import build_asset_context

from vlm_pipeline.defs.ingest import sourcea_download as mod

ENV = {
    "SOURCEA_MINIO_HOST": "10.0.0.21",
    "SOURCEA_DB_HOST": "10.0.0.21",
    "SOURCEA_DB_USER": "u",
    "SOURCEA_DB_PASS": "p",
    "SOURCEA_DB_NAME": "db",
}


def _set_env(monkeypatch, dest):
    for k, v in ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("SOURCEA_DEST", str(dest))


def _meta(result):
    return {k: getattr(v, "value", v) for k, v in result.metadata.items()}


def test_skip_when_env_missing(monkeypatch):
    for k in list(ENV) + ["SOURCEA_DEST"]:
        monkeypatch.delenv(k, raising=False)
    out = mod.sourcea_site_download(build_asset_context())
    assert _meta(out)["skipped"] is True


def test_skip_when_mount_missing(monkeypatch, tmp_path):
    _set_env(monkeypatch, tmp_path / "no_mount" / "sourcea")  # 부모 디렉토리 없음
    out = mod.sourcea_site_download(build_asset_context())
    assert _meta(out)["skipped"] is True


def test_skip_when_minio_unreachable(monkeypatch, tmp_path):
    _set_env(monkeypatch, tmp_path / "sourcea")
    with patch.object(mod.socket, "create_connection", side_effect=OSError("no route")):
        out = mod.sourcea_site_download(build_asset_context())
    assert _meta(out)["skipped"] is True


def test_skip_when_lookback_zero(monkeypatch, tmp_path):
    dest = tmp_path / "sourcea"
    dest.mkdir(parents=True)
    _set_env(monkeypatch, dest)
    monkeypatch.setenv("SOURCEA_LOOKBACK_DAYS", "0")
    with patch.object(mod.socket, "create_connection"):
        out = mod.sourcea_site_download(build_asset_context())
    assert _meta(out)["skipped"] is True


def test_happy_path_downloads_and_classifies(monkeypatch, tmp_path):
    _set_env(monkeypatch, tmp_path / "sourcea")
    dl_result = {"listed": 2, "downloaded": 2, "failed": 0, "deadline_left": 0}
    with (
        patch.object(mod.socket, "create_connection"),
        patch.object(mod.kk, "download_dates", return_value=dl_result) as dl,
        patch.object(mod.kk, "classify", return_value={"copied": 1, "missing": 0}),
    ):
        out = mod.sourcea_site_download(build_asset_context())
    meta = _meta(out)
    assert meta["skipped"] is False and meta["downloaded"] == 2 and meta["classify_copied"] == 1
    assert len(dl.call_args.args[1]) == 14  # 기본 lookback 14일


def test_classify_failure_keeps_download_result(monkeypatch, tmp_path):
    _set_env(monkeypatch, tmp_path / "sourcea")
    dl_result = {"listed": 1, "downloaded": 1, "failed": 0, "deadline_left": 0}
    with (
        patch.object(mod.socket, "create_connection"),
        patch.object(mod.kk, "download_dates", return_value=dl_result),
        patch.object(mod.kk, "classify", side_effect=OSError("db down")),
    ):
        out = mod.sourcea_site_download(build_asset_context())
    meta = _meta(out)
    assert meta["skipped"] is False and meta["downloaded"] == 1 and meta["classify_error"]


class _FixedNowDatetime(datetime):
    """sourcea_download 모듈이 참조하는 `datetime.now(tz)` 를 고정 시각으로 대체."""

    _fixed = datetime(2026, 7, 6, 7, 0)

    @classmethod
    def now(cls, tz=None):
        return cls._fixed.replace(tzinfo=tz) if tz else cls._fixed


def test_is_scheduled_run_true_for_schedule_tag_false_for_ephemeral():
    assert mod._is_scheduled_run(build_asset_context()) is False
    scheduled_ctx = build_asset_context(run_tags={"dagster/schedule_name": "sourcea_download_schedule"})
    assert mod._is_scheduled_run(scheduled_ctx) is True


def test_skip_when_scheduled_run_started_after_deadline(monkeypatch, tmp_path):
    dest = tmp_path / "sourcea"
    dest.mkdir(parents=True)
    _set_env(monkeypatch, dest)
    monkeypatch.setenv("SOURCEA_DEADLINE", "06:55")
    monkeypatch.setattr(mod, "datetime", _FixedNowDatetime)  # 07:00 — 마감(06:55) 이후
    ctx = build_asset_context(run_tags={"dagster/schedule_name": "sourcea_download_schedule"})
    with (
        patch.object(mod.socket, "create_connection"),
        patch.object(mod.kk, "download_dates") as dl,
    ):
        out = mod.sourcea_site_download(ctx)
    assert _meta(out)["skipped"] is True
    dl.assert_not_called()


def test_manual_run_after_deadline_does_not_skip(monkeypatch, tmp_path):
    dest = tmp_path / "sourcea"
    dest.mkdir(parents=True)
    _set_env(monkeypatch, dest)
    monkeypatch.setenv("SOURCEA_DEADLINE", "06:55")
    monkeypatch.setattr(mod, "datetime", _FixedNowDatetime)  # 07:00 — 마감(06:55) 이후
    dl_result = {"listed": 0, "downloaded": 0, "failed": 0, "deadline_left": 0}
    with (
        patch.object(mod.socket, "create_connection"),
        patch.object(mod.kk, "download_dates", return_value=dl_result) as dl,
        patch.object(mod.kk, "classify", return_value={"copied": 0, "missing": 0}),
    ):
        out = mod.sourcea_site_download(build_asset_context())  # 태그 없음 = 수동 run
    assert _meta(out)["skipped"] is False
    dl.assert_called_once()


def test_sourcea_schedule_definition():
    from dagster import DefaultScheduleStatus, define_asset_job

    from vlm_pipeline.definitions_production import build_sourcea_download_schedule

    job = define_asset_job("sourcea_download_job", selection=["pipeline/sourcea_site"])
    s = build_sourcea_download_schedule(job)
    assert s.name == "sourcea_download_schedule"
    assert s.cron_schedule == "0 6 * * *"
    assert s.execution_timezone == "Asia/Seoul"
    assert s.default_status == DefaultScheduleStatus.RUNNING
