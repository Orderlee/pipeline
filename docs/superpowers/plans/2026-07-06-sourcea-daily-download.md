# SourceA 일일 수집 Dagster 스케줄 — 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `data_download/download_classify.py`의 SourceA 수집(미러+분류)을 Dagster asset으로 포팅하고 매일 06:00 KST 스케줄 + 06:55 자체 마감으로 운영한다.

**Architecture:** 순수 로직은 `lib/sourcea_site.py`(L1-2, dagster 미의존), asset은 `defs/ingest/sourcea_download.py`(L4)에서 preflight(graceful skip)→다운로드→분류를 orchestrate, 배선은 `definitions_production.py`+`definitions.py`의 `gcs_download` 선례를 그대로 따른다.

**Tech Stack:** Dagster(`@asset`, `ScheduleDefinition`), stdlib `urllib`/`socket`/`concurrent.futures`, `pymysql`(lazy import), pytest + `unittest.mock`.

**Spec:** `docs/superpowers/specs/2026-07-06-sourcea-daily-download-design.md`

## Global Constraints

- cron `0 6 * * *`, `execution_timezone="Asia/Seoul"`, `default_status=RUNNING`
- 자체 마감 기본 `06:55` (env `SOURCEA_DEADLINE`) — run 시작 시점에 이미 지난 마감은 무시(수동 재실행은 끝까지)
- graceful skip 3종: env 미설정 / NAS mount 부재(OSError 포함) / MinIO TCP 실패
- 목적지 `/nas/data/sourcea` (env `SOURCEA_DEST`), lookback 기본 14일
- 날짜 폴더 단위 skip 금지 — 파일 단위 skip-existing(>0 byte)만 사용
- `lib/`는 dagster import 금지 (CI lint `scripts/check_lib_layer_imports.py`), pymysql은 함수 내 lazy import (CI에 pymysql 없어도 테스트 통과해야 함)
- creds(비밀번호)는 어떤 커밋에도 넣지 않는다 — `.env`(호스트 직접 편집)만
- ruff line-length 120, conventional commits, per-file fail-forward
- `.gitignore`는 다른 세션의 미커밋 변경이 있으므로 건드리지 않는다

## 역할 분담 (스펙의 페르소나)

| Task | 담당 |
|---|---|
| Task 1–3 (lib·asset·배선 구현) | 과장 Sonnet (`dagster-impl` 에이전트) |
| Task 4 (폴더 rename·정리) | 대리 Haiku (Agent `model: haiku`) |
| Task 5 (호스트 .env·tailscale cron) | CTO Fable (메인 세션, 호스트 작업) |
| Task 6 (리뷰) | 팀장 Opus (아키텍처) + Codex (네트워크) |
| Task 7 (최종 검증) | CTO Fable |

---

### Task 1: lib 코어 — `sourcea_site.py` [과장 Sonnet]

**Files:**
- Create: `src/vlm_pipeline/lib/sourcea_site.py`
- Test: `tests/unit/test_sourcea_site.py`

**Interfaces:**
- Produces (Task 2가 사용):
  - `SourceAConfig` (frozen dataclass) — 필드: `minio_host:str, minio_port:int, db_host:str, db_port:int, db_user:str, db_pass:str, db_name:str, dest:Path, lookback_days:int=14, deadline:str="06:55"`, property `base -> str`, classmethod `from_env(env=os.environ) -> SourceAConfig | None`
  - `recent_dates(lookback_days:int, today:date) -> list[str]` (YYYYMMDD 오름차순, today 포함)
  - `compute_deadline(deadline_hhmm:str, now:datetime) -> datetime | None`
  - `download_dates(cfg, dates:list[str], deadline:datetime|None=None, workers:int=16, now_fn=None) -> dict` (키: `listed, downloaded, failed, deadline_left`)
  - `classify(cfg, dates:set[str]|None) -> dict` (키: `copied, missing`)
  - `fetch(base:str, bucket:str, key:str, dest_file:Path, retries:int=3) -> int`
  - `list_objects(base:str, bucket:str, prefix:str="") -> Iterator[tuple[str,int]]`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/unit/test_sourcea_site.py`:

```python
"""lib/sourcea_site 순수 로직 테스트 — 네트워크/DB 없이 mock으로만 검증."""
import io
import sys
import types
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

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
    trunc = f"<IsTruncated>true</IsTruncated><NextContinuationToken>{token}</NextContinuationToken>" if truncated else "<IsTruncated>false</IsTruncated>"
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
    with patch.object(kk.urllib.request, "urlretrieve", side_effect=AssertionError("no network")):
        assert kk.fetch("http://x:9000", "thumbnail", "20260706/a.jpg", dest) == 0


def test_fetch_atomic_write(tmp_path):
    dest = tmp_path / "d" / "a.jpg"

    def fake_retrieve(url, tmp):
        Path(tmp).write_bytes(b"data")

    with patch.object(kk.urllib.request, "urlretrieve", side_effect=fake_retrieve):
        assert kk.fetch("http://x:9000", "thumbnail", "20260706/a.jpg", dest) == 1
    assert dest.read_bytes() == b"data"
    assert not dest.with_suffix(".jpg.part").exists()


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
    with patch.object(kk, "list_objects", return_value=[("20260706/a.jpg", 10)]) as lo, \
         patch.object(kk, "fetch", return_value=1) as f:
        stats = kk.download_dates(cfg, ["20260706"], deadline=None)
    assert lo.call_count == 2  # thumbnail + video 버킷
    assert f.call_count == 2 and stats["downloaded"] == 2 and stats["failed"] == 0


def test_download_dates_per_file_fail_forward(tmp_path):
    cfg = _cfg(tmp_path)
    with patch.object(kk, "list_objects", return_value=[("20260706/a.jpg", 10)]), \
         patch.object(kk, "fetch", side_effect=OSError("boom")):
        stats = kk.download_dates(cfg, ["20260706"], deadline=None)
    assert stats["failed"] == 2 and stats["downloaded"] == 0


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


def test_classify_copies_by_category(tmp_path):
    cfg = _cfg(tmp_path)
    (tmp_path / "thumbnail" / "20260706").mkdir(parents=True)
    (tmp_path / "thumbnail" / "20260706" / "e1.jpg").write_bytes(b"x")
    events = [
        {"event_name": "fire", "thumbnail": "/user-service/minio/thumbnail/20260706/e1.jpg",
         "video_url": "/user-service/minio/video/20260706/e1.mp4"},
        {"event_name": "peoplecount", "thumbnail": "/user-service/minio/thumbnail/20260706/e2.jpg",
         "video_url": None},
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
```

- [ ] **Step 2: 실패 확인**

Run: `pytest tests/unit/test_sourcea_site.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'vlm_pipeline.lib.sourcea_site'`

- [ ] **Step 3: 구현**

`src/vlm_pipeline/lib/sourcea_site.py`:

```python
"""SourceA 사이트(Tailscale 경유) MinIO 미러 + DB 분류 — 순수 로직 (L1-2, dagster 미의존).

data_download/download_classify.py 에서 포팅. 원자적 쓰기(.part→rename)와
파일 단위 skip-existing 유지. 날짜 폴더 단위 skip 은 부분 다운로드된 날짜가
영구 누락되는 갭이 있어 채택하지 않는다.
설계: docs/superpowers/specs/2026-07-06-sourcea-daily-download-design.md
"""
from __future__ import annotations

import os
import shutil
import socket
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator
from zoneinfo import ZoneInfo

_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
_TZ = ZoneInfo("Asia/Seoul")
BUCKETS = ("thumbnail", "video")
CATEGORIES = frozenset({"smoke", "fire", "falldown", "violence"})
MEDIA_PREFIX = "/user-service/minio"

_REQUIRED_ENV = (
    "SOURCEA_MINIO_HOST",
    "SOURCEA_DB_HOST",
    "SOURCEA_DB_USER",
    "SOURCEA_DB_PASS",
    "SOURCEA_DB_NAME",
    "SOURCEA_DEST",
)


@dataclass(frozen=True)
class SourceAConfig:
    minio_host: str
    minio_port: int
    db_host: str
    db_port: int
    db_user: str
    db_pass: str
    db_name: str
    dest: Path
    lookback_days: int = 14
    deadline: str = "06:55"

    @property
    def base(self) -> str:
        return f"http://{self.minio_host}:{self.minio_port}"

    @classmethod
    def from_env(cls, env=os.environ) -> "SourceAConfig | None":
        if any(not env.get(k) for k in _REQUIRED_ENV):
            return None
        return cls(
            minio_host=env["SOURCEA_MINIO_HOST"],
            minio_port=int(env.get("SOURCEA_MINIO_PORT", "9000")),
            db_host=env["SOURCEA_DB_HOST"],
            db_port=int(env.get("SOURCEA_DB_PORT", "3306")),
            db_user=env["SOURCEA_DB_USER"],
            db_pass=env["SOURCEA_DB_PASS"],
            db_name=env["SOURCEA_DB_NAME"],
            dest=Path(env["SOURCEA_DEST"]),
            lookback_days=int(env.get("SOURCEA_LOOKBACK_DAYS", "14")),
            deadline=env.get("SOURCEA_DEADLINE", "06:55"),
        )


def recent_dates(lookback_days: int, today: date) -> list[str]:
    """today 포함 최근 N일의 YYYYMMDD 문자열 (오름차순)."""
    return [(today - timedelta(days=i)).strftime("%Y%m%d") for i in range(lookback_days - 1, -1, -1)]


def compute_deadline(deadline_hhmm: str, now: datetime) -> datetime | None:
    """오늘의 HH:MM 마감. 이미 지났으면 None — 수동 재실행은 마감 없이 끝까지 돈다."""
    if not deadline_hhmm:
        return None
    h, m = (int(x) for x in deadline_hhmm.split(":"))
    d = now.replace(hour=h, minute=m, second=0, microsecond=0)
    return d if d > now else None


def list_objects(base: str, bucket: str, prefix: str = "") -> Iterator[tuple[str, int]]:
    """bucket/prefix 아래 전 객체의 (key, size) — S3 ListObjectsV2 pagination."""
    token = None
    while True:
        u = f"{base}/{bucket}?list-type=2&max-keys=1000&prefix={urllib.parse.quote(prefix)}"
        if token:
            u += f"&continuation-token={urllib.parse.quote(token, safe='')}"
        with urllib.request.urlopen(u, timeout=60) as r:
            root = ET.fromstring(r.read())
        for c in root.findall(f"{_NS}Contents"):
            yield c.find(f"{_NS}Key").text, int(c.find(f"{_NS}Size").text)
        if (root.findtext(f"{_NS}IsTruncated") or "false") == "true":
            token = root.findtext(f"{_NS}NextContinuationToken")
        else:
            break


def fetch(base: str, bucket: str, key: str, dest_file: Path, retries: int = 3) -> int:
    """객체 1개 다운로드. 이미 있으면(>0 byte) 0, 새로 받으면 1. 원자적(.part→rename)."""
    if dest_file.exists() and dest_file.stat().st_size > 0:
        return 0
    dest_file.parent.mkdir(parents=True, exist_ok=True)
    url = f"{base}/{bucket}/{urllib.parse.quote(key)}"
    tmp = dest_file.with_suffix(dest_file.suffix + ".part")
    for attempt in range(retries):
        try:
            urllib.request.urlretrieve(url, tmp)
            tmp.replace(dest_file)
            return 1
        except (OSError, socket.timeout):
            if attempt == retries - 1:
                tmp.unlink(missing_ok=True)
                raise
            time.sleep(2 * (attempt + 1))
    return 0  # unreachable — raise 위에서 종료


def download_dates(
    cfg: SourceAConfig,
    dates: list[str],
    deadline: datetime | None = None,
    workers: int = 16,
    now_fn=None,
) -> dict:
    """dates 의 thumbnail/video 전 객체를 파일 단위 incremental 미러.

    deadline 도달 시 신규 작업 제출을 멈추고 진행 중 파일만 마무리한다.
    per-file fail-forward: 개별 실패는 failed 카운트 후 계속.
    """
    now_fn = now_fn or (lambda: datetime.now(_TZ))

    def past() -> bool:
        return deadline is not None and now_fn() >= deadline

    socket.setdefaulttimeout(120)  # stalled read 가 영원히 안 걸리게
    stats = {"listed": 0, "downloaded": 0, "failed": 0, "deadline_left": 0}
    jobs: list[tuple[str, str, Path]] = []
    for bucket in BUCKETS:
        for d in dates:
            if past():
                break
            try:
                for key, _size in list_objects(cfg.base, bucket, prefix=f"{d}/"):
                    jobs.append((bucket, key, cfg.dest / bucket / key))
            except OSError:
                stats["failed"] += 1  # enumeration 실패도 fail-forward
    stats["listed"] = len(jobs)

    futs = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for b, k, p in jobs:
            if past():
                stats["deadline_left"] = len(jobs) - len(futs)
                break
            futs[ex.submit(fetch, cfg.base, b, k, p)] = k
        for f in as_completed(futs):
            try:
                stats["downloaded"] += f.result()
            except Exception:  # noqa: BLE001 — per-file fail-forward
                stats["failed"] += 1
    return stats


def classify(cfg: SourceAConfig, dates: set[str] | None) -> dict:
    """DB camera_events 기반 by_category/<cat>/{thumbnail,video}/<date>/ 복사.

    dates 가 주어지면 해당 날짜만. DB 에 이벤트가 남아 있는 날짜에만 유효.
    """
    import pymysql  # lazy — CI/테스트 환경에 미설치여도 lib import 가능해야 함

    conn = pymysql.connect(
        host=cfg.db_host, port=cfg.db_port, user=cfg.db_user,
        password=cfg.db_pass, database=cfg.db_name,
        connect_timeout=10, cursorclass=pymysql.cursors.DictCursor,
    )
    with conn, conn.cursor() as cur:
        cur.execute("SELECT event_name, thumbnail, video_url FROM camera_events")
        events = cur.fetchall()

    out = {"copied": 0, "missing": 0}
    for e in events:
        cat = e["event_name"]
        if cat not in CATEGORIES:
            continue
        for field, bucket in (("thumbnail", "thumbnail"), ("video_url", "video")):
            rel = (e[field] or "").removeprefix(MEDIA_PREFIX).lstrip("/")
            parts = rel.split("/")
            if len(parts) < 3:
                continue
            d, fname = parts[1], parts[2]
            if dates and d not in dates:
                continue
            src = cfg.dest / bucket / d / fname
            dst = cfg.dest / "by_category" / cat / bucket / d / fname
            if not src.exists():
                out["missing"] += 1
                continue
            if not dst.exists():
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)
            out["copied"] += 1
    return out
```

- [ ] **Step 4: 통과 확인**

Run: `pytest tests/unit/test_sourcea_site.py -q`
Expected: 전부 PASS

- [ ] **Step 5: lib layer lint 확인**

Run: `python scripts/check_lib_layer_imports.py`
Expected: 종료코드 0 (dagster import 없음)

- [ ] **Step 6: 커밋**

```bash
git add src/vlm_pipeline/lib/sourcea_site.py tests/unit/test_sourcea_site.py
git commit -m "feat(sites): SourceA 미러+분류 순수 로직 lib 포팅

data_download/download_classify.py 를 lib/sourcea_site.py 로 포팅.
날짜 폴더 단위 skip 제거(부분 날짜 영구 누락 갭), deadline-aware 제출,
pymysql lazy import (CI 미설치 허용)."
```

---

### Task 2: Dagster asset — `sourcea_download.py` [과장 Sonnet]

**Files:**
- Create: `src/vlm_pipeline/defs/ingest/sourcea_download.py`
- Test: `tests/unit/test_sourcea_download_asset.py`

**Interfaces:**
- Consumes: Task 1의 `sourcea_site` 전체 API (위 시그니처 그대로)
- Produces (Task 3이 사용): asset 함수 `sourcea_site_download` (AssetKey `["pipeline", "sourcea_site"]`)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/unit/test_sourcea_download_asset.py`:

```python
"""sourcea_site_download asset — preflight skip 경로와 happy path."""
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


def test_happy_path_downloads_and_classifies(monkeypatch, tmp_path):
    _set_env(monkeypatch, tmp_path / "sourcea")
    with patch.object(mod.socket, "create_connection"), \
         patch.object(mod.kk, "download_dates", return_value={"listed": 2, "downloaded": 2, "failed": 0, "deadline_left": 0}) as dl, \
         patch.object(mod.kk, "classify", return_value={"copied": 1, "missing": 0}):
        out = mod.sourcea_site_download(build_asset_context())
    meta = _meta(out)
    assert meta["skipped"] is False and meta["downloaded"] == 2 and meta["classify_copied"] == 1
    assert len(dl.call_args.args[1]) == 14  # 기본 lookback 14일


def test_classify_failure_keeps_download_result(monkeypatch, tmp_path):
    _set_env(monkeypatch, tmp_path / "sourcea")
    with patch.object(mod.socket, "create_connection"), \
         patch.object(mod.kk, "download_dates", return_value={"listed": 1, "downloaded": 1, "failed": 0, "deadline_left": 0}), \
         patch.object(mod.kk, "classify", side_effect=OSError("db down")):
        out = mod.sourcea_site_download(build_asset_context())
    meta = _meta(out)
    assert meta["skipped"] is False and meta["downloaded"] == 1 and meta["classify_error"]
```

- [ ] **Step 2: 실패 확인**

Run: `pytest tests/unit/test_sourcea_download_asset.py -q`
Expected: FAIL — `ModuleNotFoundError: ... sourcea_download`

- [ ] **Step 3: 구현**

`src/vlm_pipeline/defs/ingest/sourcea_download.py`:

```python
"""SourceA 사이트 일일 수집 asset (L4).

Tailscale 경유 MinIO(100.x) → /nas/data/sourcea 미러 + DB 기반 by_category 분류.
env·NAS·연결 preflight 실패는 모두 graceful skip — NAS 미복구/tailscale off 상태에서
스케줄이 돌아도 에러가 나지 않는다.
설계: docs/superpowers/specs/2026-07-06-sourcea-daily-download-design.md
"""
from __future__ import annotations

import socket
from datetime import datetime
from zoneinfo import ZoneInfo

from dagster import AssetKey, MaterializeResult, asset

from vlm_pipeline.lib import sourcea_site as kk

_TZ = ZoneInfo("Asia/Seoul")
_CONNECT_TIMEOUT_SEC = 5


def _skip(context, reason: str) -> MaterializeResult:
    context.log.warning(f"sourcea_site_download skip: {reason}")
    return MaterializeResult(metadata={"skipped": True, "reason": reason})


@asset(
    key=AssetKey(["pipeline", "sourcea_site"]),
    description="SourceA MinIO → /nas/data/sourcea 미러 + by_category 분류 (tailscale 필요)",
    group_name="sites",
)
def sourcea_site_download(context) -> MaterializeResult:
    cfg = kk.SourceAConfig.from_env()
    if cfg is None:
        return _skip(context, "SOURCEA_* env 미설정")

    try:
        if not cfg.dest.parent.is_dir():
            return _skip(context, f"NAS mount 없음: {cfg.dest.parent}")
    except OSError as e:  # NFS stale handle 등
        return _skip(context, f"NAS 접근 불가: {e}")

    try:
        socket.create_connection((cfg.minio_host, cfg.minio_port), timeout=_CONNECT_TIMEOUT_SEC).close()
    except OSError as e:
        return _skip(context, f"MinIO {cfg.minio_host}:{cfg.minio_port} 연결 실패(tailscale off?): {e}")

    now = datetime.now(_TZ)
    deadline = kk.compute_deadline(cfg.deadline, now)
    dates = kk.recent_dates(cfg.lookback_days, now.date())
    context.log.info(f"수집 대상 {dates[0]}..{dates[-1]}, deadline={deadline}")

    stats = kk.download_dates(cfg, dates, deadline=deadline)
    context.log.info(f"download: {stats}")

    meta = {"skipped": False, **stats}
    try:
        cls = kk.classify(cfg, set(dates))
        meta.update({f"classify_{k}": v for k, v in cls.items()})
    except Exception as e:  # noqa: BLE001 — DB 순단이어도 다운로드 결과는 보존
        context.log.warning(f"classify 실패(다운로드는 완료): {e}")
        meta["classify_error"] = str(e)
    return MaterializeResult(metadata=meta)
```

- [ ] **Step 4: 통과 확인**

Run: `pytest tests/unit/test_sourcea_download_asset.py tests/unit/test_sourcea_site.py -q`
Expected: 전부 PASS

- [ ] **Step 5: 커밋**

```bash
git add src/vlm_pipeline/defs/ingest/sourcea_download.py tests/unit/test_sourcea_download_asset.py
git commit -m "feat(sites): sourcea_site_download asset — preflight graceful skip + 06:55 자체 마감

env/NAS/tailscale 부재 시 skip, run monitoring 부재로 in-asset deadline 채택.
classify 실패는 fail-forward (다운로드 결과 보존)."
```

---

### Task 3: 배선 + 의존성 [과장 Sonnet]

**Files:**
- Modify: `src/vlm_pipeline/definitions_production.py` (`build_gcs_download_schedule` 아래, ~line 200)
- Modify: `src/vlm_pipeline/definitions.py` (import ~line 18 / job ~line 45 / jobs 리스트 ~line 85 / schedules 리스트 ~line 158 — `gcs_download` 각 지점 바로 옆)
- Modify: `docker/app/requirements.txt` (pymysql 추가)
- Test: `tests/unit/test_sourcea_download_asset.py` (스케줄 테스트 추가)

**Interfaces:**
- Consumes: Task 2의 `sourcea_site_download`
- Produces: `build_sourcea_download_schedule(job) -> ScheduleDefinition`, job 이름 `sourcea_download_job`, 스케줄 이름 `sourcea_download_schedule`

- [ ] **Step 1: 실패하는 스케줄 테스트 추가** (`tests/unit/test_sourcea_download_asset.py` 끝에)

```python
def test_sourcea_schedule_definition():
    from dagster import DefaultScheduleStatus, define_asset_job

    from vlm_pipeline.definitions_production import build_sourcea_download_schedule

    job = define_asset_job("sourcea_download_job", selection=["pipeline/sourcea_site"])
    s = build_sourcea_download_schedule(job)
    assert s.name == "sourcea_download_schedule"
    assert s.cron_schedule == "0 6 * * *"
    assert s.execution_timezone == "Asia/Seoul"
    assert s.default_status == DefaultScheduleStatus.RUNNING
```

- [ ] **Step 2: 실패 확인**

Run: `pytest tests/unit/test_sourcea_download_asset.py::test_sourcea_schedule_definition -q`
Expected: FAIL — `ImportError: cannot import name 'build_sourcea_download_schedule'`

- [ ] **Step 3: 스케줄 빌더 구현** — `definitions_production.py`의 `build_gcs_download_schedule` 함수 정의가 끝나는 지점 바로 아래에 추가:

```python
def build_sourcea_download_schedule(job) -> ScheduleDefinition:
    """매일 06:00 KST SourceA 사이트 수집 (tailscale 창구 06:00-07:00).

    07:00 종료는 asset 내부 자체 마감(기본 06:55, env SOURCEA_DEADLINE)이 보장 —
    이 배포에는 run monitoring 이 없어 dagster/max_runtime 태그가 동작하지 않는다.
    env/NAS/tailscale 부재 시 asset 이 graceful skip 하므로 RUNNING 이 안전하다.
    """
    return ScheduleDefinition(
        name="sourcea_download_schedule",
        job=job,
        cron_schedule="0 6 * * *",
        execution_timezone="Asia/Seoul",
        default_status=DefaultScheduleStatus.RUNNING,
    )
```

- [ ] **Step 4: definitions.py 배선** — `gcs_download` 4개 지점 각각 바로 옆에 같은 모양으로 추가:

```python
# (1) import 블록 — build_gcs_download_schedule 옆
from vlm_pipeline.definitions_production import build_sourcea_download_schedule
# (2) asset import — gcs_download_to_incoming import 옆
from vlm_pipeline.defs.ingest.sourcea_download import sourcea_site_download
# (3) job — _gcs_download_job 정의 옆
_sourcea_download_job = build_asset_job(
    name="sourcea_download_job",
    selection=[sourcea_site_download],
)
# (4) jobs 리스트에 _sourcea_download_job 추가, assets 리스트에 sourcea_site_download 추가
# (5) schedules 리스트에 build_sourcea_download_schedule(_sourcea_download_job) 추가
```

주의: import 는 기존 스타일에 맞춰 파일 상단의 해당 import 문에 이름만 추가한다 (별도 import 문 신설 금지).

- [ ] **Step 5: pymysql 의존성** — `docker/app/requirements.txt`에 한 줄 추가 (알파벳/기존 정렬 위치 준수):

```
pymysql>=1.1
```

`pyproject.toml`이 git tracked인 경우에만(`git ls-files pyproject.toml`로 확인) `dependencies`에도 `"pymysql>=1.1",` 추가. untracked면 건드리지 않는다.

- [ ] **Step 6: 전체 확인**

Run: `pytest tests/unit/test_sourcea_download_asset.py tests/unit/test_sourcea_site.py -q && python -c "import vlm_pipeline.definitions"`
Expected: 테스트 전부 PASS, definitions import 에러 없음 (import 실패 시 env 부재가 아니라 코드 문제 — asset은 env 없이도 정의 가능해야 함)

- [ ] **Step 7: 커밋**

```bash
git add src/vlm_pipeline/definitions_production.py src/vlm_pipeline/definitions.py docker/app/requirements.txt tests/unit/test_sourcea_download_asset.py
git commit -m "feat(sites): sourcea_download_schedule 배선 — 매일 06:00 KST

gcs_download 선례와 동일 패턴 (job + ScheduleDefinition + definitions 배선).
pymysql 추가로 이미지 재빌드 트리거됨."
```

주의: `definitions.py`/`definitions_production.py`에 다른 세션의 미커밋 변경이 있으면(git diff로 확인) 해당 hunk를 커밋에 포함하지 않도록 `git add -p` 사용.

---

### Task 4: 폴더 rename + deprecated 표기 [대리 Haiku]

**Files:**
- Move: `data_download/` → `site_reports/` (untracked — git 커밋 없음)
- Modify: `site_reports/download_classify.py` (docstring 첫머리에 deprecated 안내)
- Create: `site_reports/README.md`

- [ ] **Step 1: rename**

```bash
mv /home/user/work_p/Datapipeline-Data-data_pipeline/data_download /home/user/work_p/Datapipeline-Data-data_pipeline/site_reports
```

- [ ] **Step 2: deprecated 표기** — `site_reports/download_classify.py` docstring 최상단에 추가:

```python
"""[DEPRECATED 2026-07-06] 주기 수집은 Dagster 로 이전됨 —
src/vlm_pipeline/lib/sourcea_site.py + defs/ingest/sourcea_download.py
(sourcea_download_schedule, 매일 06:00 KST). 이 스크립트는 수동 백필용으로만 유지.

Download SourceA event media from MinIO and classify into by_category.
...(기존 docstring 유지)
```

- [ ] **Step 3: README 작성** — `site_reports/README.md`:

```markdown
# site_reports — 외부 사이트 리포트/복구 툴킷

SourceA 등 외부 사이트(Tailscale 경유)의 이벤트 데이터 리포트 생성·복구 도구.
**주기 수집(미러+분류)은 Dagster 로 이전됨** (`sourcea_download_schedule`, 매일 06:00 KST) —
`src/vlm_pipeline/lib/sourcea_site.py` 참고.

| 파일 | 용도 |
|---|---|
| `main.py` | 기간 지정 이벤트 리포트 생성 (`reports/<window>/report.md`) |
| `download_classify.py` | [DEPRECATED] 수동 백필용 미러+분류 |
| `verify_refetch.py` | 스테이징 파일 무결성 검증·재다운로드 |
| `classify_vlm.py` / `frame_match.py` / `assemble.py` / `finalize_unparsed.py` | DB-less 과거 날짜 복구 (일회성) |

- creds 는 `config.py` 평문 — **git 커밋 금지** (폴더 전체 untracked 유지).
- 폴더 rename 으로 `.venv` 의 pip 콘솔 스크립트(절대경로 shebang)는 깨졌지만
  `.venv/bin/python main.py` 실행은 정상. pip 필요 시 venv 재생성.
```

- [ ] **Step 4: 동작 확인**

Run: `cd /home/user/work_p/Datapipeline-Data-data_pipeline/site_reports && .venv/bin/python -c "from config import config; print(config.site_name)"`
Expected: `SourceA Police station Anomaly Detection Control Status` 출력

커밋 없음 (전부 untracked).

---

### Task 5: 호스트 .env + tailscale cron [CTO Fable — 호스트 작업, 커밋 없음]

- [ ] **Step 1: prod `.env`에 키 추가** — `docker/.env` (git 미추적, 호스트 직접 편집):

```
SOURCEA_MINIO_HOST=10.0.0.21
SOURCEA_MINIO_PORT=9000
SOURCEA_DB_HOST=10.0.0.21
SOURCEA_DB_PORT=3306
SOURCEA_DB_USER=<site_reports/config.py 의 db_user 값>
SOURCEA_DB_PASS=<site_reports/config.py 의 db_pass 값>
SOURCEA_DB_NAME=sourcea_db
SOURCEA_DEST=/nas/data/sourcea
```

(실제 값은 `site_reports/config.py`에서 복사. staging `.env.test`에는 추가하지 않음 → staging은 자동 skip.)

- [ ] **Step 2: compose environment passthrough 확인** — dagster 서비스가 `.env`의 SOURCEA_* 를 컨테이너로 전달하는지 확인. `docker/docker-compose.yaml`의 dagster environment 블록에 passthrough가 없으면 추가 필요 (이 경우는 **tracked 파일이므로 커밋 대상** — Task 3 방식으로 커밋):

```yaml
      SOURCEA_MINIO_HOST: ${SOURCEA_MINIO_HOST:-}
      SOURCEA_MINIO_PORT: ${SOURCEA_MINIO_PORT:-9000}
      SOURCEA_DB_HOST: ${SOURCEA_DB_HOST:-}
      SOURCEA_DB_PORT: ${SOURCEA_DB_PORT:-3306}
      SOURCEA_DB_USER: ${SOURCEA_DB_USER:-}
      SOURCEA_DB_PASS: ${SOURCEA_DB_PASS:-}
      SOURCEA_DB_NAME: ${SOURCEA_DB_NAME:-}
      SOURCEA_DEST: ${SOURCEA_DEST:-/nas/data/sourcea}
```

- [ ] **Step 3: tailscale cron 설치** — sudo 가능 여부 확인 후:

```bash
sudo -n true 2>/dev/null && echo "sudo OK" || echo "sudo 불가 — 아래 명령을 사용자에게 전달"
# sudo 가능 시:
(sudo crontab -l 2>/dev/null | grep -v tailscale; echo '55 5 * * * /usr/bin/tailscale up'; echo '5 7 * * * /usr/bin/tailscale down') | sudo crontab -
sudo crontab -l | grep tailscale   # 확인
```

sudo 불가 시 위 두 줄 cron 엔트리를 최종 보고에 포함해 사용자가 root로 설치하도록 안내.

---

### Task 6: 리뷰 [팀장 Opus + 네트워크 전문가 Codex]

- [ ] **Step 1: 팀장(Opus) 아키텍처 리뷰** — Agent(`model: opus`)로 dispatch. 프롬프트에 스펙 경로 + Task 1–3 diff 요약을 주고 점검 요청: 레이어링(lib↔defs), graceful skip 누락 경로, 스케줄/마감 로직의 경계 조건(자정 넘는 run, DST 없음 확인), 기존 컨벤션 위반.
- [ ] **Step 2: 네트워크 전문가(Codex) 리뷰** — `codex` 에이전트로 dispatch. 점검 요청: 컨테이너(bridge)→tailnet 100.x 라우팅 가정의 함정, socket timeout/재시도 적정성, S3 pagination·URL 인코딩, 06:55 마감과 ThreadPoolExecutor 종료 동작.
- [ ] **Step 3: 지적사항 반영** — CONFIRMED 수준 지적만 수정 커밋 (`fix(sites): ...`). 사소한 취향 지적은 기각 사유와 함께 기록.

---

### Task 7: 최종 검증 + 마무리 [CTO Fable]

- [ ] **Step 1: 전체 unit 테스트**

Run: `pytest tests/unit -q`
Expected: 기존 포함 전부 PASS (기존 실패가 있었다면 base와 동일한지 비교)

- [ ] **Step 2: ruff (CI 버전 0.7.4, tracked 파일만)** — memory `project_ruff_lint_ci` 방식:

```bash
python3 -m venv /tmp/claude-1000/-home-user-work-p-Datapipeline-Data-data-pipeline/e1e98942-ea08-4622-bfdf-33400cea8784/scratchpad/ruff074 2>/dev/null || true
/tmp/claude-1000/.../scratchpad/ruff074/bin/pip install -q "ruff==0.7.4"
/tmp/claude-1000/.../scratchpad/ruff074/bin/ruff check src/vlm_pipeline/lib/sourcea_site.py src/vlm_pipeline/defs/ingest/sourcea_download.py tests/unit/test_sourcea_site.py tests/unit/test_sourcea_download_asset.py
```

Expected: no findings

- [ ] **Step 3: lib layer lint**

Run: `python scripts/check_lib_layer_imports.py`
Expected: 종료코드 0

- [ ] **Step 4: 최종 보고** — 사용자에게: 구현 요약, NAS 복구 후 검증 체크리스트(스펙 §배포&검증), sudo 불가 시 tailscale cron 설치 명령, PR(→dev) 생성 여부 확인.

---

## Self-Review 결과

- 스펙 커버리지: asset/스케줄/마감(Task 1–3), env·creds(Task 5), rename(Task 4), tailscale cron(Task 5), 역할 분담(각 Task 태그), 리뷰(Task 6) — 전부 매핑됨. "NAS 복구 후 검증"은 현재 불가하므로 Task 7 Step 4의 인수인계 항목으로 처리.
- 타입 일관성: `download_dates` 반환 키(`listed/downloaded/failed/deadline_left`)와 asset 메타데이터·테스트 assert 일치. `classify` 반환 키(`copied/missing`)와 `classify_*` prefix 일치.
- placeholder 없음 (creds 값 참조는 보안상 의도된 간접 참조).
