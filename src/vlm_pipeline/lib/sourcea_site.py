"""SourceA 사이트(Tailscale 경유) MinIO 미러 + DB 분류 — 순수 로직 (L1-2, dagster 미의존).

data_download/download_classify.py 에서 포팅. 원자적 쓰기(.part→rename)와
파일 단위 skip-existing 유지. 날짜 폴더 단위 skip 은 부분 다운로드된 날짜가
영구 누락되는 갭이 있어 채택하지 않는다.
설계: docs/superpowers/specs/2026-07-06-sourcea-daily-download-design.md
"""
from __future__ import annotations

import os
import shutil
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import CancelledError, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator
from zoneinfo import ZoneInfo

_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
_TZ = ZoneInfo("Asia/Seoul")
_CHUNK_BYTES = 256 * 1024
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
    workers: int = 16

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
            workers=int(env.get("SOURCEA_WORKERS", "16")),
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
            key = c.findtext(f"{_NS}Key")
            size = c.findtext(f"{_NS}Size")
            if key is None or size is None:  # 손상된 listing 항목 — fail-forward skip
                continue
            yield key, int(size)
        if (root.findtext(f"{_NS}IsTruncated") or "false") == "true":
            token = root.findtext(f"{_NS}NextContinuationToken")
        else:
            break


def fetch(
    base: str,
    bucket: str,
    key: str,
    dest_file: Path,
    retries: int = 3,
    deadline: datetime | None = None,
    now_fn=None,
    timeout: int = 120,
) -> int:
    """객체 1개 다운로드.

    이미 있으면(>0 byte) 0, 새로 받으면 1, deadline 경과로 시도를 시작 못하면
    (최초 시도 전 또는 retry 사이) -1. 원자적(.part→rename). 스트리밍을 이미
    시작한 attempt 는 청크 중간에 deadline 을 보지 않고 끝까지 마무리한다 —
    스펙 "진행 중 파일만 마무리".
    """
    if dest_file.exists() and dest_file.stat().st_size > 0:
        return 0
    now_fn = now_fn or (lambda: datetime.now(_TZ))
    url = f"{base}/{bucket}/{urllib.parse.quote(key)}"
    tmp = dest_file.with_suffix(dest_file.suffix + ".part")
    retries = max(1, retries)  # retries=0 이 "skip"(0)으로 오인되지 않도록 최소 1회 시도 보장
    for attempt in range(retries):
        if deadline is not None and now_fn() >= deadline:
            tmp.unlink(missing_ok=True)  # 실패한 이전 attempt 의 stale .part 정리
            return -1
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp, open(tmp, "wb") as out:
                while True:
                    chunk = resp.read(_CHUNK_BYTES)
                    if not chunk:
                        break
                    out.write(chunk)
            tmp.replace(dest_file)
            return 1
        except OSError:  # socket.timeout(py3.10+) == TimeoutError ⊂ OSError, 별도 catch 불필요
            if attempt == retries - 1:
                tmp.unlink(missing_ok=True)
                raise
            time.sleep(2 * (attempt + 1))
    return 0  # unreachable — 루프는 항상 return/raise 로 종료


def download_dates(
    cfg: SourceAConfig,
    dates: list[str],
    deadline: datetime | None = None,
    workers: int | None = None,
    now_fn=None,
) -> dict:
    """dates 의 thumbnail/video 전 객체를 파일 단위 incremental 미러.

    deadline 도달 시 신규 작업 제출을 멈추고(enumeration/submit 모두) 이미 실행 중인
    워커의 in-flight 파일만 마무리한다 — 아직 시작 안 한 큐 잔여분은
    `ThreadPoolExecutor.shutdown(cancel_futures=True)`로 즉시 취소한다(submit 은
    non-blocking 이라 취소 없이는 수천 job 이 그대로 배수된다).
    per-file fail-forward: 개별 실패는 failed 카운트 후 계속.
    """
    now_fn = now_fn or (lambda: datetime.now(_TZ))
    workers = cfg.workers if workers is None else workers

    def past() -> bool:
        return deadline is not None and now_fn() >= deadline

    stats = {"listed": 0, "downloaded": 0, "failed": 0, "deadline_left": 0}
    jobs: list[tuple[str, str, Path]] = []
    for bucket in BUCKETS:
        for d in dates:
            if past():
                break
            try:
                for key, _size in list_objects(cfg.base, bucket, prefix=f"{d}/"):
                    if past():
                        break
                    jobs.append((bucket, key, cfg.dest / bucket / key))
            except Exception:  # noqa: BLE001 — enumeration도 fail-forward (IncompleteRead, XML ParseError 등)
                stats["failed"] += 1
    stats["listed"] = len(jobs)

    futs = {}
    shutdown_triggered = False
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for b, k, p in jobs:
            if past():
                stats["deadline_left"] = len(jobs) - len(futs)
                break
            futs[ex.submit(fetch, cfg.base, b, k, p, deadline=deadline, now_fn=now_fn)] = k
        # as_completed() 는 안 쓴다 — Executor.shutdown(cancel_futures=True) 가 큐에서 직접
        # future.cancel() 하는 경로는 CANCELLED_AND_NOTIFIED 로 전이시키지 않아 as_completed 의
        # waiter 가 영원히 못 깨어난다(cancel() 은 _condition 만 notify, _waiters 는 미통지).
        # future.result() 는 _state 를 직접 보고 CANCELLED 도 즉시 raise 하므로 안전하다.
        for f in futs:
            if not shutdown_triggered and past():
                ex.shutdown(wait=False, cancel_futures=True)  # 큐 잔여분 즉시 취소 — in-flight 만 마무리
                shutdown_triggered = True
            try:
                result = f.result()
            except CancelledError:
                stats["deadline_left"] += 1
                continue
            except Exception:  # noqa: BLE001 — per-file fail-forward
                stats["failed"] += 1
                continue
            if result == -1:
                stats["deadline_left"] += 1
            else:
                stats["downloaded"] += result
    return stats


def classify(cfg: SourceAConfig, dates: set[str] | None) -> dict:
    """DB camera_events 기반 by_category/<cat>/{thumbnail,video}/<date>/ 복사.

    dates 가 주어지면 해당 날짜만. DB 에 이벤트가 남아 있는 날짜에만 유효.
    """
    import pymysql  # lazy — CI/테스트 환경에 미설치여도 lib import 가능해야 함

    conn = pymysql.connect(
        host=cfg.db_host, port=cfg.db_port, user=cfg.db_user,
        password=cfg.db_pass, database=cfg.db_name,
        connect_timeout=10, read_timeout=30, write_timeout=30,
        cursorclass=pymysql.cursors.DictCursor,
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
