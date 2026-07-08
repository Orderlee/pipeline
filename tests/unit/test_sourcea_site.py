"""lib/sourcea_site 순수 로직 테스트 — 네트워크/DB 없이 mock으로만 검증."""

import io
import sys
import time
import types
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from vlm_pipeline.lib import sourcea_site as kk

TZ = ZoneInfo("Asia/Seoul")

ENV = {
    "SOURCEA_MINIO_HOST": "10.0.0.21",
    "SOURCEA_DB_HOST": "10.0.0.21",
    "SOURCEA_DB_USER": "u",
    "SOURCEA_DB_PASS": "p",
    "SOURCEA_DB_NAME": "db",
    "SOURCEA_DEST": "/nas/data/sourcea",
}


def test_from_env_missing_returns_none():
    assert kk.SourceAConfig.from_env(env={}) is None
    partial = {k: v for k, v in ENV.items() if k != "SOURCEA_DB_PASS"}
    assert kk.SourceAConfig.from_env(env=partial) is None


def test_from_env_complete_with_defaults():
    cfg = kk.SourceAConfig.from_env(env=ENV)
    assert cfg.minio_port == 9000 and cfg.db_port == 3306
    assert cfg.dest == Path("/nas/data/sourcea")
    assert cfg.lookback_days == 14 and cfg.deadline == "06:55"
    assert cfg.base == "http://10.0.0.21:9000"
    assert cfg.workers == 16


def test_from_env_workers_override():
    cfg = kk.SourceAConfig.from_env(env={**ENV, "SOURCEA_WORKERS": "4"})
    assert cfg.workers == 4


def test_recent_dates():
    assert kk.recent_dates(3, date(2026, 7, 6)) == ["20260704", "20260705", "20260706"]


def test_compute_deadline_future_and_past():
    now = datetime(2026, 7, 6, 6, 0, tzinfo=TZ)
    d = kk.compute_deadline("06:55", now)
    assert d == datetime(2026, 7, 6, 6, 55, tzinfo=TZ)
    assert kk.compute_deadline("06:55", datetime(2026, 7, 6, 12, 0, tzinfo=TZ)) is None
    assert kk.compute_deadline("", now) is None


def _xml(keys, truncated=False, token=""):
    ns = "http://s3.amazonaws.com/doc/2006-03-01/"
    items = "".join(f"<Contents><Key>{k}</Key><Size>10</Size></Contents>" for k in keys)
    trunc = (
        f"<IsTruncated>true</IsTruncated><NextContinuationToken>{token}</NextContinuationToken>"
        if truncated
        else "<IsTruncated>false</IsTruncated>"
    )
    return f'<ListBucketResult xmlns="{ns}">{items}{trunc}</ListBucketResult>'.encode()


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_list_objects_pagination():
    pages = [_Resp(_xml(["20260706/a.jpg"], truncated=True, token="t1")), _Resp(_xml(["20260706/b.jpg"]))]
    with patch.object(kk.urllib.request, "urlopen", side_effect=pages):
        out = list(kk.list_objects("http://x:9000", "thumbnail", prefix="20260706/"))
    assert out == [("20260706/a.jpg", 10), ("20260706/b.jpg", 10)]


def test_fetch_skips_existing_nonempty(tmp_path):
    dest = tmp_path / "a.jpg"
    dest.write_bytes(b"x")
    with patch.object(kk.urllib.request, "urlopen", side_effect=AssertionError("no network")):
        assert kk.fetch("http://x:9000", "thumbnail", "20260706/a.jpg", dest) == 0


class _ChunkedResp:
    """urlopen 스트리밍 mock — chunk 여러 개를 순서대로 반환 후 빈 바이트로 종료."""

    def __init__(self, chunks):
        self._chunks = list(chunks)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self, _size=-1):
        return self._chunks.pop(0) if self._chunks else b""


def test_fetch_atomic_write(tmp_path):
    dest = tmp_path / "d" / "a.jpg"
    resp = _ChunkedResp([b"da", b"ta"])
    with patch.object(kk.urllib.request, "urlopen", return_value=resp) as uo:
        assert kk.fetch("http://x:9000", "thumbnail", "20260706/a.jpg", dest) == 1
    uo.assert_called_once()
    assert uo.call_args.kwargs["timeout"] == 120
    assert dest.read_bytes() == b"data"
    assert not dest.with_suffix(".jpg.part").exists()


def test_fetch_deadline_already_passed_returns_minus1_no_network(tmp_path):
    dest = tmp_path / "a.jpg"
    deadline = datetime(2026, 7, 6, 6, 55, tzinfo=TZ)
    late = lambda: datetime(2026, 7, 6, 7, 0, tzinfo=TZ)  # noqa: E731
    with patch.object(kk.urllib.request, "urlopen", side_effect=AssertionError("no network")):
        result = kk.fetch("http://x:9000", "thumbnail", "20260706/a.jpg", dest, deadline=deadline, now_fn=late)
    assert result == -1
    assert not dest.exists()


def test_fetch_deadline_passed_between_retries_returns_minus1(tmp_path, monkeypatch):
    dest = tmp_path / "a.jpg"
    deadline = datetime(2026, 7, 6, 6, 55, tzinfo=TZ)
    now_calls = [datetime(2026, 7, 6, 6, 0, tzinfo=TZ), datetime(2026, 7, 6, 7, 0, tzinfo=TZ)]
    now_fn = MagicMock(side_effect=now_calls)
    monkeypatch.setattr(kk.time, "sleep", lambda _s: None)
    with patch.object(kk.urllib.request, "urlopen", side_effect=OSError("boom")) as uo:
        result = kk.fetch(
            "http://x:9000", "thumbnail", "20260706/a.jpg", dest, retries=3, deadline=deadline, now_fn=now_fn
        )
    assert result == -1
    assert uo.call_count == 1  # 두 번째 attempt 는 deadline 에 막혀 네트워크 호출 없음


def _cfg(tmp_path):
    return kk.SourceAConfig.from_env(env={**ENV, "SOURCEA_DEST": str(tmp_path)})


def test_download_dates_deadline_passed_lists_nothing(tmp_path):
    cfg = _cfg(tmp_path)
    deadline = datetime(2026, 7, 6, 6, 55, tzinfo=TZ)
    late = lambda: datetime(2026, 7, 6, 7, 0, tzinfo=TZ)  # noqa: E731
    with patch.object(kk, "list_objects", side_effect=AssertionError("must not enumerate")):
        stats = kk.download_dates(cfg, ["20260706"], deadline=deadline, now_fn=late)
    assert stats["listed"] == 0 and stats["downloaded"] == 0


def test_download_dates_happy_path(tmp_path):
    cfg = _cfg(tmp_path)
    with (
        patch.object(kk, "list_objects", return_value=[("20260706/a.jpg", 10)]) as lo,
        patch.object(kk, "fetch", return_value=1) as f,
    ):
        stats = kk.download_dates(cfg, ["20260706"], deadline=None)
    assert lo.call_count == 2  # thumbnail + video 버킷
    assert f.call_count == 2 and stats["downloaded"] == 2 and stats["failed"] == 0


def test_download_dates_per_file_fail_forward(tmp_path):
    cfg = _cfg(tmp_path)
    with (
        patch.object(kk, "list_objects", return_value=[("20260706/a.jpg", 10)]),
        patch.object(kk, "fetch", side_effect=OSError("boom")),
    ):
        stats = kk.download_dates(cfg, ["20260706"], deadline=None)
    assert stats["failed"] == 2 and stats["downloaded"] == 0


def test_download_dates_enumeration_fail_forward(tmp_path):
    cfg = _cfg(tmp_path)
    import xml.etree.ElementTree as ET

    with patch.object(kk, "list_objects", side_effect=ET.ParseError("bad xml")):
        stats = kk.download_dates(cfg, ["20260706"], deadline=None)
    assert stats["failed"] == 2 and stats["downloaded"] == 0  # 버킷 2개 각각 fail-forward


def test_download_dates_enumeration_stops_mid_pagination_at_deadline(tmp_path):
    cfg = _cfg(tmp_path)
    deadline = datetime(2026, 7, 6, 6, 55, tzinfo=TZ)
    calls = {"n": 0}

    def now_fn():
        calls["n"] += 1
        return datetime(2026, 7, 6, 7, 0, tzinfo=TZ) if calls["n"] > 2 else datetime(2026, 7, 6, 6, 0, tzinfo=TZ)

    keys = [("20260706/a.jpg", 10), ("20260706/b.jpg", 10), ("20260706/c.jpg", 10)]
    with patch.object(kk, "list_objects", return_value=keys), patch.object(kk, "fetch", return_value=1):
        stats = kk.download_dates(cfg, ["20260706"], deadline=deadline, now_fn=now_fn)
    assert stats["listed"] < 6  # 3 keys x 2 버킷 전부 못 모음 — 페이지네이션 도중 마감 반영


def test_download_dates_drain_cancels_pending_without_waiting_for_full_queue(tmp_path):
    cfg = _cfg(tmp_path)
    start = datetime.now(TZ)
    deadline = start + timedelta(seconds=0.07)
    keys = [(f"20260706/{i}.jpg", 10) for i in range(4)]  # x2 버킷 = 8 jobs

    def slow_fetch(base, bucket, key, dest_file, deadline=None, now_fn=None):
        time.sleep(0.03)
        return 1

    t0 = time.monotonic()
    with patch.object(kk, "list_objects", return_value=keys), patch.object(kk, "fetch", side_effect=slow_fetch):
        stats = kk.download_dates(cfg, ["20260706"], deadline=deadline, workers=1, now_fn=lambda: datetime.now(TZ))
    elapsed = time.monotonic() - t0

    assert stats["listed"] == 8
    assert stats["downloaded"] + stats["deadline_left"] + stats["failed"] == 8
    assert stats["deadline_left"] > 0  # 큐 잔여분이 cancel_futures 로 취소됨
    assert stats["downloaded"] < 8  # 전체 큐를 기다리지 않음
    assert elapsed < 0.2  # 순차 완주(8*0.03s=0.24s)보다 훨씬 빨리 끝남


def _fake_pymysql(events):
    cur = MagicMock()
    cur.fetchall.return_value = events
    cur.__enter__ = lambda s: cur
    cur.__exit__ = lambda s, *a: False
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.__enter__ = lambda s: conn
    conn.__exit__ = lambda s, *a: False
    mod = types.ModuleType("pymysql")
    mod.connect = MagicMock(return_value=conn)
    mod.cursors = types.SimpleNamespace(DictCursor=object)
    return mod


def test_classify_sets_connect_read_write_timeout(tmp_path):
    cfg = _cfg(tmp_path)
    mod = _fake_pymysql([])
    with patch.dict(sys.modules, {"pymysql": mod}):
        kk.classify(cfg, None)
    kwargs = mod.connect.call_args.kwargs
    assert kwargs["connect_timeout"] == 10
    assert kwargs["read_timeout"] == 30
    assert kwargs["write_timeout"] == 30


def test_classify_copies_by_category(tmp_path):
    cfg = _cfg(tmp_path)
    (tmp_path / "thumbnail" / "20260706").mkdir(parents=True)
    (tmp_path / "thumbnail" / "20260706" / "e1.jpg").write_bytes(b"x")
    events = [
        {
            "event_name": "fire",
            "thumbnail": "/user-service/minio/thumbnail/20260706/e1.jpg",
            "video_url": "/user-service/minio/video/20260706/e1.mp4",
        },
        {"event_name": "peoplecount", "thumbnail": "/user-service/minio/thumbnail/20260706/e2.jpg", "video_url": None},
    ]
    with patch.dict(sys.modules, {"pymysql": _fake_pymysql(events)}):
        out = kk.classify(cfg, {"20260706"})
    assert (tmp_path / "by_category" / "fire" / "thumbnail" / "20260706" / "e1.jpg").exists()
    assert out["copied"] == 1
    assert out["missing"] == 1  # video 파일은 디스크에 없음
    assert not (tmp_path / "by_category" / "peoplecount").exists()  # 비대상 카테고리 제외


def test_classify_date_filter(tmp_path):
    cfg = _cfg(tmp_path)
    (tmp_path / "thumbnail" / "20260101").mkdir(parents=True)
    (tmp_path / "thumbnail" / "20260101" / "old.jpg").write_bytes(b"x")
    events = [{"event_name": "fire", "thumbnail": "/user-service/minio/thumbnail/20260101/old.jpg", "video_url": None}]
    with patch.dict(sys.modules, {"pymysql": _fake_pymysql(events)}):
        out = kk.classify(cfg, {"20260706"})
    assert out["copied"] == 0 and not (tmp_path / "by_category").exists()


def test_list_objects_skips_malformed_entries():
    ns = "http://s3.amazonaws.com/doc/2006-03-01/"
    xml = (
        f'<ListBucketResult xmlns="{ns}"><Contents><Key>a/b.jpg</Key><Size>10</Size></Contents>'
        f"<Contents></Contents><IsTruncated>false</IsTruncated></ListBucketResult>"
    ).encode()
    with patch.object(kk.urllib.request, "urlopen", return_value=_Resp(xml)):
        assert list(kk.list_objects("http://x:9000", "thumbnail")) == [("a/b.jpg", 10)]
