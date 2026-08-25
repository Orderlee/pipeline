"""FiftyOne 동기화 HTTP API — `sync_incremental.py` 를 subprocess 로 실행하는 얇은 레이어.

⚠️ 이 프로세스는 FastAPI 만 있으면 되고 **fiftyone 을 import 하지 않는다** — 무거운 작업은
전부 subprocess(`sync_incremental.py`) 안에서 돌고, 그 프로세스가 종료되면 RSS 가 반환된다
(호스트 RAM 62.5G 공유·oom_kill 이력). 동시성은 `threading.Lock` 한 개로 단일 비행만 허용한다
(FiftyOne 이 전 탭에서 세션 하나를 공유하는 것과 같은 이유로, 동기화 job 도 동시에 두 개가
같은 데이터셋을 건드리면 안전하지 않다).

계약 (다른 에이전트가 dagster 쪽에서 이 API 를 소비 — 정확히 이 shape 를 지킬 것):
    GET  /health          → 200 {"ok": true, "busy": <bool>}
    POST /sync/{target}    target ∈ frames|labels|prompts, body(옵션) {"dry_run": true}
                           → 202 {"job_id": "<target>-<n>"} | 409 {"error": "busy", "current": {...}}
    GET  /status           → {"busy": bool,
                                "current": {job_id,target,started_at} | null,
                                "last": {job_id,target,state,returncode,started_at,
                                         finished_at,result,tail} | null}
    GET  /status?job_id=X  → 위 + {"job": <해당 job 레코드> | null}. 폴링 클라이언트는
                             `last` 대신 이걸 봐야 한다 — A 완료 직후 B 가 시작되면
                             `last` 가 B 로 덮여 A 의 종결을 영영 못 보는 경합이 있다.
    인증(옵션): env FIFTYONE_SYNC_TOKEN 설정 시 X-Internal-Token 헤더 요구.
                미설정이면 개방(내부 네트워크 전용 서비스 — 호스트 포트 노출 없음).

`last` 는 job 을 **디스패치하는 순간** state="running" 으로 먼저 채워지고(그래서 프로세스가
아직 끝나지 않았을 때도 /status 로 방금 무엇을 시켰는지 보인다), 완료 시 done/failed 로
덮어써진다. `current` 는 그중 "지금 실행 중인 것만" 을 가리키는 좁은 포인터(409 응답용) —
잡이 끝나면 null 로 돌아간다.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from collections import OrderedDict
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="fiftyone-sync")

TARGETS = ("frames", "labels", "prompts")
SYNC_SCRIPT = os.environ.get("FIFTYONE_SYNC_SCRIPT", "/workspace/sync_incremental.py")
TOKEN = os.environ.get("FIFTYONE_SYNC_TOKEN", "").strip()
TAIL_LINES = 20
# subprocess 절대 상한 — 이게 없으면 hang(PG/MinIO blocking call)이 _busy 를 영구 true 로
# 남겨 컨테이너 재시작 전까지 모든 요청이 409 다. labels 전량 재적재가 수 시간일 수 있어
# 기본을 넉넉히 6h 로 둔다. 만료 시 run() 이 자식을 kill 하고 잡은 failed 로 기록된다.
JOB_TIMEOUT_S = int(os.environ.get("FIFTYONE_SYNC_JOB_TIMEOUT_S", "21600"))

_state_lock = threading.Lock()  # _busy/_current/_last/_history/_job_counter 갱신 보호(요청 스레드 vs 백그라운드 스레드)
_busy = False
_current: dict[str, Any] | None = None
_last: dict[str, Any] | None = None
_history: OrderedDict[str, dict[str, Any]] = OrderedDict()  # job_id → 레코드, 최근 HISTORY_MAX 개
_job_counter = 0
HISTORY_MAX = 50


def _check_token(x_internal_token: str | None) -> None:
    if TOKEN and x_internal_token != TOKEN:
        raise HTTPException(status_code=401, detail="invalid or missing X-Internal-Token")


def _run_job(job_id: str, target: str, dry_run: bool, started_at: float) -> None:
    """subprocess 실행 + 종료 후 상태 갱신. 백그라운드 스레드에서 돈다 — 요청 스레드는 즉시 202."""
    global _busy, _current, _last

    cmd = [sys.executable, SYNC_SCRIPT, target]
    if dry_run:
        cmd.append("--dry-run")

    result: dict | None = None
    tail: list[str] = []
    returncode = -1
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=JOB_TIMEOUT_S)
        returncode = proc.returncode
        out_lines = (proc.stdout or "").splitlines()
        if out_lines:
            try:
                result = json.loads(out_lines[-1])
            except Exception:  # noqa: BLE001 — 마지막 줄이 JSON 아니면 result=None, tail 로 원인 확인
                result = None
        tail = out_lines[-TAIL_LINES:]
        if returncode != 0:
            tail = tail + (proc.stderr or "").splitlines()[-TAIL_LINES:]
    except subprocess.TimeoutExpired:
        tail = [f"job timeout({JOB_TIMEOUT_S}s) — subprocess killed (FIFTYONE_SYNC_JOB_TIMEOUT_S 로 조정)"]
    except Exception as exc:  # noqa: BLE001 — subprocess 실행 자체 실패(스크립트 부재 등)도 잡의 실패로 기록
        tail = [f"{type(exc).__name__}: {exc}"]
    finally:
        finished_at = time.time()
        record = {
            "job_id": job_id,
            "target": target,
            "state": "done" if returncode == 0 else "failed",
            "returncode": returncode,
            "started_at": started_at,
            "finished_at": finished_at,
            "result": result,
            "tail": tail[-TAIL_LINES:],
        }
        with _state_lock:
            _last = record
            _history[job_id] = record
            while len(_history) > HISTORY_MAX:
                _history.popitem(last=False)
            _current = None
            _busy = False


@app.get("/health")
def health() -> dict:
    with _state_lock:
        return {"ok": True, "busy": _busy}


@app.get("/status")
def status(job_id: str | None = None) -> dict:
    with _state_lock:
        payload: dict[str, Any] = {"busy": _busy, "current": _current, "last": _last}
        if job_id is not None:
            payload["job"] = _history.get(job_id)
        return payload


@app.post("/sync/{target}")
async def sync(
    target: str,
    request: Request,
    x_internal_token: str | None = Header(default=None, alias="X-Internal-Token"),
):
    _check_token(x_internal_token)
    if target not in TARGETS:
        raise HTTPException(status_code=404, detail=f"unknown target {target!r} — allowed: {TARGETS}")

    dry_run = False
    body = await request.body()
    if body:
        try:
            payload = json.loads(body)
            if isinstance(payload, dict):
                dry_run = bool(payload.get("dry_run", False))
        except Exception:  # noqa: BLE001 — body 는 계약상 옵션. 못 읽으면 dry_run=False 로 무시
            dry_run = False

    global _busy, _current, _last, _job_counter
    with _state_lock:
        if _busy:
            return JSONResponse(status_code=409, content={"error": "busy", "current": _current})
        _job_counter += 1
        job_id = f"{target}-{_job_counter}"
        started_at = time.time()
        _busy = True
        _current = {"job_id": job_id, "target": target, "started_at": started_at}
        _last = {
            "job_id": job_id,
            "target": target,
            "state": "running",
            "returncode": None,
            "started_at": started_at,
            "finished_at": None,
            "result": None,
            "tail": [],
        }
        _history[job_id] = _last

    thread = threading.Thread(target=_run_job, args=(job_id, target, dry_run, started_at), daemon=True)
    try:
        thread.start()
    except Exception as exc:  # noqa: BLE001 — start() 실패가 _busy 를 영구 true 로 남기면 안 됨
        with _state_lock:
            _busy = False
            _current = None
            _history[job_id] = _last = {**_last, "state": "failed", "tail": [f"thread start 실패: {exc!r}"]}
        raise HTTPException(status_code=500, detail=f"job thread start 실패: {exc!r}") from exc
    return JSONResponse(status_code=202, content={"job_id": job_id})
