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

── 업로드 번들 인제스트 계약 (정본: project_upload/UPLOAD_SPEC.md) ──────────────────────
아래 3개도 fiftyone/numpy 를 이 프로세스에서 import 하지 않는 철학을 그대로 지킨다 — 검증/인제스트는
전부 `project_upload/validate_bundle.py`·`ingest_bundle.py` 를 subprocess 로 호출해서 수행한다
(그 스크립트들만 fiftyone/numpy 를 무겁게 로드하고, 끝나면 프로세스와 함께 RSS 가 반환된다).

    POST /upload/validate  body {"bundle": "<이름 또는 절대경로>"}
                           → 동기 실행(<=180s). 200 { ok, errors, warnings, mode, counts, versions }
                             (validate_bundle.py 는 exit 1 이어도 stdout 에 동일 shape 의 JSON 을
                             내놓는 계약이라 — 그 경우도 200 이고 본문 ok=false 로 표현한다.
                             stdout 이 JSON 이 아니면(스크립트 비정상 종료 등) 502.)
    POST /upload/ingest    body {"bundle": ..., "name"?: str, "overwrite"?: bool,
                                 "skip_viz"?: bool, "attach"?: str}
                           → /sync/{target} 과 **동일한 단일비행 busy 게이트를 공유**한다
                             (busy 면 409 {"error":"busy","current":...}). 202 {"job_id": "upload-<n>"}.
                             job 레코드의 target 은 "upload:<번들이름>" 으로 기록(기존 /status
                             소비자는 자기 job_id 로만 조회하므로 이 표기가 섞여도 안전).
                             완료 후 <번들>/_artifacts/ingest_report.json 이 존재하면 job 레코드에
                             "report_path" 로 첨부한다.
    GET  /upload/bundles   → UPLOAD_ROOT 하위 디렉토리 중 manifest.json 이 있는 것만 나열:
                             [{"bundle","path","has_gt","ingested"}] — 파일시스템만 읽어 락 불필요
                             (플러그인 드롭다운 후보 목록용).
    POST /upload/delete    → {"bundles":[이름,...], "confirm":true} (또는 "dry_run":true 로 계획만).
                             번들 디렉토리 + 그 번들을 가리키는 upload_kit 데이터셋을 **한 쌍으로**
                             지운다 (미디어는 제자리 참조라 번들만 지우면 데이터셋이 깨진다).
                             delete_bundle.py 에 위임 — marker 없는 데이터셋은 절대 안 지운다.
                             busy 중이면 409, 이름 규칙 위반/confirm 누락은 400.

    POST /upload/fetch     → {"url": "http(s)://…/bundle.zip", "name"?: str, "overwrite"?: bool}
                             서버가 직접 내려받아 PUT /upload/archive 와 **같은** 설치·검증 코드를 탄다.
                             202 {"job_id":"fetch-<n>"} → 진행은 GET /upload/job?job_id= 로 폴링
                             (job 레코드에 phase/downloaded/total 추가, state=done 이면 result 가
                              /upload/archive 200 본문과 동일한 dict). `_busy` 를 점유하지 않는다 —
                             다운로드 중에도 /upload/ingest 는 정상 동작한다.
                             주소 정책: http/https + 포트 allowlist + **사내 CIDR allowlist** 만
                             (기본 10.0.0.0/16, 그 밖은 공인 IP 포함 403). 리다이렉트는 홉마다 재검증.

    bundle 경로 해석(공통): "/" 없으면 UPLOAD_ROOT(기본 /data/fiftyone/uploads)/<이름>. 있으면
    그대로 realpath 를 계산해 UPLOAD_ROOT 아래인지 확인한다 — 밖이면 400 (경로 탈출 차단,
    UPLOAD_SPEC.md §1 "반입은 컨테이너 경유"와 동일한 경계를 API 레벨에서도 강제).
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import os
import posixpath
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
import zipfile
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

app = FastAPI(title="fiftyone-sync")

TARGETS = ("frames", "labels", "prompts")
SYNC_SCRIPT = os.environ.get("FIFTYONE_SYNC_SCRIPT", "/workspace/sync_incremental.py")
TOKEN = os.environ.get("FIFTYONE_SYNC_TOKEN", "").strip()
TAIL_LINES = 20
# subprocess 절대 상한 — 이게 없으면 hang(PG/MinIO blocking call)이 _busy 를 영구 true 로
# 남겨 컨테이너 재시작 전까지 모든 요청이 409 다. labels 전량 재적재가 수 시간일 수 있어
# 기본을 넉넉히 6h 로 둔다. 만료 시 run() 이 자식을 kill 하고 잡은 failed 로 기록된다.
# 업로드 인제스트(UMAP 등)도 같은 상한을 공유한다 — 별도로 짧게 둘 근거가 없고, 상한을
# 나눠 관리하면 어느 쪽이 적용됐는지 헷갈리는 비용이 더 크다.
JOB_TIMEOUT_S = int(os.environ.get("FIFTYONE_SYNC_JOB_TIMEOUT_S", "21600"))

# ── 업로드 번들 인제스트 (project_upload/) ───────────────────────────────────
# UPLOAD_ROOT 는 bundle_common.UPLOAD_ROOT 와 반드시 같은 env 키를 읽는다(동일 컨테이너 안에서
# 두 프로세스가 같은 값을 봐야 함). 파일명 상수 4개는 bundle_common 의 리터럴 미러다 — 이 프로세스는
# numpy 를 끌고 오는 bundle_common 을 직접 import 하지 않는다는 위 철학을 지키려 값만 복제한다
# (변경 시 두 파일 동기화 필요. 정본은 항상 bundle_common.py + UPLOAD_SPEC.md).
UPLOAD_ROOT = os.environ.get("UPLOAD_ROOT", "/data/fiftyone/uploads")
VALIDATE_SCRIPT = os.environ.get("UPLOAD_VALIDATE_SCRIPT", "/workspace/project_upload/validate_bundle.py")
INGEST_SCRIPT = os.environ.get("UPLOAD_INGEST_SCRIPT", "/workspace/project_upload/ingest_bundle.py")
DELETE_SCRIPT = os.environ.get("UPLOAD_DELETE_SCRIPT", "/workspace/project_upload/delete_bundle.py")
UPLOAD_VALIDATE_TIMEOUT_S = int(os.environ.get("UPLOAD_VALIDATE_TIMEOUT_S", "180"))
# rmtree 는 수십 GB 번들에서 오래 걸릴 수 있다(NVMe 라 보통 초 단위지만 상한은 넉넉히).
UPLOAD_DELETE_TIMEOUT_S = int(os.environ.get("UPLOAD_DELETE_TIMEOUT_S", "600"))
UPLOAD_MANIFEST_FILE = "manifest.json"
UPLOAD_GT_FILE = "gt.csv"
UPLOAD_ARTIFACTS_DIR = "_artifacts"
UPLOAD_INGEST_REPORT_FILE = "ingest_report.json"
UPLOAD_INGEST_LOG_FILE = "ingest.log"  # ingest_bundle.py subprocess 의 stdout+stderr 전량 영속 기록
# /upload/validate 는 busy 단일비행 게이트 밖(동기 실행)이라 concurrency 를 별도로 제한한다 — 없으면
# 좌석 여러 개가 동시에 눌러 임베딩 npz 전량을 RAM 에 올리는 subprocess 가 무제한 병렬로 뜬다
# (major 리뷰 지적, analysis-sync 에 mem_limit 도 없음). 기본 1 = 사실상 ingest 와 동일한 단일비행.
UPLOAD_VALIDATE_CONCURRENCY = int(os.environ.get("UPLOAD_VALIDATE_CONCURRENCY", "1"))

_state_lock = threading.Lock()  # _busy/_current/_last/_history/_job_counter 갱신 보호(요청 스레드 vs 백그라운드 스레드)
_busy = False
_current: dict[str, Any] | None = None
_last: dict[str, Any] | None = None
_history: OrderedDict[str, dict[str, Any]] = OrderedDict()  # job_id → 레코드, 최근 HISTORY_MAX 개
_job_counter = 0
HISTORY_MAX = 50
_validate_sem = threading.BoundedSemaphore(max(1, UPLOAD_VALIDATE_CONCURRENCY))  # /upload/validate 동시성 상한


def _check_token(x_internal_token: str | None) -> None:
    if TOKEN and x_internal_token != TOKEN:
        raise HTTPException(status_code=401, detail="invalid or missing X-Internal-Token")


async def _read_json_body(request: Request) -> dict:
    """요청 본문을 JSON 객체로 파싱 (빈 본문 → {}). 형식 위반은 400."""
    body = await request.body()
    if not body:
        return {}
    try:
        payload = json.loads(body)
    except Exception as exc:  # noqa: BLE001 — 클라이언트 입력 파싱 실패는 400 대상
        raise HTTPException(status_code=400, detail=f"본문 JSON 파싱 실패: {exc}") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="본문은 JSON 객체여야 함")
    return payload


def _resolve_bundle_dir(bundle: Any) -> str:
    """bundle 이름/경로 → 실제 디렉토리 절대경로 (UPLOAD_ROOT 밖이면 ValueError, 경로 탈출 차단).

    "/" 없는 문자열은 UPLOAD_ROOT 하위 이름으로 해석한다. "/" 가 있으면(상대·절대 불문) 그대로
    realpath 를 계산해 UPLOAD_ROOT 경계 검사만 적용한다 — 번들 실체는 UPLOAD_SPEC.md §1 대로
    항상 UPLOAD_ROOT 아래여야 하므로, 어느 형태로 들어오든 결과 판정은 동일하게 fail-closed 다.
    """
    if not isinstance(bundle, str) or not bundle.strip():
        raise ValueError("bundle 필요 (이름 또는 경로)")
    bundle = bundle.strip()
    path = bundle if "/" in bundle else os.path.join(UPLOAD_ROOT, bundle)
    real = os.path.realpath(path)
    real_root = os.path.realpath(UPLOAD_ROOT)
    try:
        inside = os.path.commonpath([real, real_root]) == real_root
    except ValueError:  # 서로 다른 마운트/드라이브 등 — 비교 불가는 곧 바깥으로 간주
        inside = False
    if not inside:
        raise ValueError(f"bundle 경로가 업로드 루트({UPLOAD_ROOT}) 밖입니다: {real}")
    return real


def _decode_out(v: Any) -> str:
    """subprocess 출력은 str(text=True 정상 종료)·bytes(TimeoutExpired 부분 캡처, cpython 특성상
    text 모드에서도 디코딩 전 원시 bytes 로 온다)·None(캡처 안 됨) 중 하나 — 셋 다 str 로 통일."""
    if v is None:
        return ""
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return v


def _write_full_log(log_path: str, cmd: list[str], returncode: int, out_text: str, err_text: str) -> None:
    """subprocess 전체 stdout+stderr 를 <번들>/_artifacts/ingest.log 에 영속 기록.

    메모리 tail(TAIL_LINES)은 analysis-sync 프로세스가 재시작되면 사라지지만 이 파일은 남는다
    (major 리뷰 지적 — 실패한 인제스트는 증적이 0이었다). 쓰기 실패는 job 실패로 보지 않는다
    (로그 저장 실패가 인제스트 자체 실패를 가려선 안 됨).
    """
    try:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"$ {' '.join(cmd)}\n")
            f.write(f"returncode={returncode}\n")
            f.write("── stdout ──\n")
            f.write(out_text)
            f.write("\n── stderr ──\n")
            f.write(err_text)
            f.write("\n")
    except OSError as exc:  # noqa: BLE001 — 로그 기록 실패는 job 결과에 영향 주지 않는다
        print(f"[sync_api] ingest.log 기록 실패({log_path}): {exc}", file=sys.stderr)


def _run_subprocess(cmd: list[str], timeout: int, log_path: str | None = None) -> tuple[int, dict | None, list[str]]:
    """subprocess 실행 → (returncode, 마지막 stdout 줄의 JSON 파싱 결과 또는 None, tail 로그).

    타임아웃/실행 자체 실패도 예외를 던지지 않고 returncode=-1 + tail 메시지로 흡수한다 —
    백그라운드 스레드에서 돌기 때문에 예외가 새면 잡 상태가 영원히 running 으로 멈춘다.

    log_path 가 주어지면 stdout+stderr **전체**(타임아웃으로 죽어도 그때까지 캡처된 부분 포함
    — Popen.communicate(timeout=) 는 TimeoutExpired.stdout/.stderr 에 부분 출력을 담아 재발생시킨다,
    실측 확인됨)를 그 경로에 영속 기록한다.
    """
    result: dict | None = None
    tail: list[str] = []
    returncode = -1
    out_text = ""
    err_text = ""
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        returncode = proc.returncode
        out_text, err_text = proc.stdout or "", proc.stderr or ""
        out_lines = out_text.splitlines()
        if out_lines:
            try:
                result = json.loads(out_lines[-1])
            except Exception:  # noqa: BLE001 — 마지막 줄이 JSON 아니면 result=None, tail 로 원인 확인
                result = None
        tail = out_lines[-TAIL_LINES:]
        if returncode != 0:
            tail = tail + err_text.splitlines()[-TAIL_LINES:]
    except subprocess.TimeoutExpired as exc:
        out_text, err_text = _decode_out(exc.stdout), _decode_out(exc.stderr)
        tail = [f"job timeout({timeout}s) — subprocess killed (FIFTYONE_SYNC_JOB_TIMEOUT_S 로 조정)"]
        if out_text or err_text:
            tail = tail + (out_text.splitlines() + err_text.splitlines())[-TAIL_LINES:]
    except Exception as exc:  # noqa: BLE001 — subprocess 실행 자체 실패(스크립트 부재 등)도 잡의 실패로 기록
        tail = [f"{type(exc).__name__}: {exc}"]
    if log_path:
        _write_full_log(log_path, cmd, returncode, out_text, err_text)
    return returncode, result, tail


def _finish_job(job_id: str, target: str, started_at: float, returncode: int,
                 result: dict | None, tail: list[str], extra: dict[str, Any] | None = None) -> None:
    """subprocess 종료 후 상태 갱신 (백그라운드 스레드 공유 로직 — _run_job/_run_upload_ingest_job 공통)."""
    global _busy, _current, _last
    finished_at = time.time()
    record: dict[str, Any] = {
        "job_id": job_id,
        "target": target,
        "state": "done" if returncode == 0 else "failed",
        "returncode": returncode,
        "started_at": started_at,
        "finished_at": finished_at,
        "result": result,
        "tail": tail[-TAIL_LINES:],
    }
    if extra:
        record.update(extra)
    with _state_lock:
        _last = record
        _history[job_id] = record
        while len(_history) > HISTORY_MAX:
            _history.popitem(last=False)
        _current = None
        _busy = False


def _run_job(job_id: str, target: str, dry_run: bool, started_at: float) -> None:
    """sync_incremental.py subprocess 실행 + 종료 후 상태 갱신. 백그라운드 스레드에서 돈다."""
    cmd = [sys.executable, SYNC_SCRIPT, target]
    if dry_run:
        cmd.append("--dry-run")
    returncode, result, tail = _run_subprocess(cmd, JOB_TIMEOUT_S)
    _finish_job(job_id, target, started_at, returncode, result, tail)


def _run_upload_ingest_job(job_id: str, target: str, cmd: list[str], bundle_dir: str, started_at: float) -> None:
    """ingest_bundle.py subprocess 실행 + 종료 후 상태 갱신 (+ ingest_report.json 경로 첨부 + ingest.log 영속화).

    try/finally 로 감싼다 — report_path 계산에서 뭔가 새면(권한 오류 등) `_finish_job` 이 영영
    안 불려 `_busy` 가 영구 true 로 잠긴다(minor 리뷰 지적).
    """
    returncode, result, tail = -1, None, []
    extra: dict[str, Any] = {}
    log_path = os.path.join(bundle_dir, UPLOAD_ARTIFACTS_DIR, UPLOAD_INGEST_LOG_FILE)
    try:
        returncode, result, tail = _run_subprocess(cmd, JOB_TIMEOUT_S, log_path=log_path)
        # 이 job 이 실제로 만든 리포트만 첨부한다 — returncode!=0(이번 run 실패)이거나 파일이
        # started_at 이전(이전 성공 run 의 stale 리포트)이면 첨부하지 않는다. 첨부 조건 무시하고
        # 존재만 봤을 때는 실패한 job 옆에 옛 성공 통계가 그대로 붙어 성공처럼 보였다(major 리뷰 지적).
        report_path = os.path.join(bundle_dir, UPLOAD_ARTIFACTS_DIR, UPLOAD_INGEST_REPORT_FILE)
        if returncode == 0 and os.path.isfile(report_path) and os.path.getmtime(report_path) >= started_at:
            extra["report_path"] = report_path
    finally:
        extra["log_path"] = log_path
        _finish_job(job_id, target, started_at, returncode, result, tail, extra=extra)


def _dispatch_job(job_id_prefix: str, record_target: str, run_fn, run_args: tuple = ()) -> JSONResponse:
    """job_id 발급 + busy 단일비행 게이트 + running 레코드 기록 + 백그라운드 스레드 시작.

    /sync/{target} 과 /upload/ingest 가 공유하는 디스패치 로직. run_fn 은
    (job_id, record_target, *run_args, started_at) 시그니처로 호출된다.
    """
    global _busy, _current, _last, _job_counter
    with _state_lock:
        if _busy:
            return JSONResponse(status_code=409, content={"error": "busy", "current": _current})
        _job_counter += 1
        job_id = f"{job_id_prefix}-{_job_counter}"
        started_at = time.time()
        _busy = True
        _current = {"job_id": job_id, "target": record_target, "started_at": started_at}
        _last = {
            "job_id": job_id,
            "target": record_target,
            "state": "running",
            "returncode": None,
            "started_at": started_at,
            "finished_at": None,
            "result": None,
            "tail": [],
        }
        _history[job_id] = _last

    thread = threading.Thread(
        target=run_fn, args=(job_id, record_target, *run_args, started_at), daemon=True,
    )
    try:
        thread.start()
    except Exception as exc:  # noqa: BLE001 — start() 실패가 _busy 를 영구 true 로 남기면 안 됨
        with _state_lock:
            _busy = False
            _current = None
            _history[job_id] = _last = {**_last, "state": "failed", "tail": [f"thread start 실패: {exc!r}"]}
        raise HTTPException(status_code=500, detail=f"job thread start 실패: {exc!r}") from exc
    return JSONResponse(status_code=202, content={"job_id": job_id})


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

    return _dispatch_job(target, target, _run_job, (dry_run,))


@app.post("/upload/validate")
async def upload_validate(
    request: Request,
    x_internal_token: str | None = Header(default=None, alias="X-Internal-Token"),
):
    """번들 읽기 전용 검증 — validate_bundle.py 를 동기 subprocess 로 실행해 그대로 중계한다."""
    _check_token(x_internal_token)
    payload = await _read_json_body(request)
    try:
        bundle_dir = _resolve_bundle_dir(payload.get("bundle"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # /upload/ingest 와 달리 이 핸들러는 busy 단일비행 게이트 밖(동기 실행)이다 — 좌석 여러 개가
    # 동시에 검증을 누르면 임베딩 npz 전량을 RAM 에 올리는 subprocess 가 개수 제한 없이 병렬로
    # 뜬다(major 리뷰 지적, analysis-sync 는 mem_limit 없음). 별도 세마포어로 상한을 건다.
    if not _validate_sem.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail=f"/upload/validate 동시 실행 상한({UPLOAD_VALIDATE_CONCURRENCY}) 초과 — 잠시 후 재시도",
        )
    try:
        cmd = [sys.executable, VALIDATE_SCRIPT, bundle_dir, "--json"]
        try:
            # ⚠️ asyncio.to_thread 필수 — 이 핸들러는 async def(uvicorn 단일 워커 이벤트 루프 위에서
            # 돈다). subprocess.run 을 직접 부르면 최대 UPLOAD_VALIDATE_TIMEOUT_S(180s) 동안 이벤트
            # 루프 자체가 멈춰 같은 프로세스의 /health·/status·/sync/{target} 이 전부 무응답이 된다
            # (codex 리뷰 실증 — /sync·/upload/ingest 는 스레드에 위임해 문제없는데 이 핸들러만 누락).
            proc = await asyncio.to_thread(
                subprocess.run, cmd, capture_output=True, text=True, timeout=UPLOAD_VALIDATE_TIMEOUT_S,
            )
        except subprocess.TimeoutExpired:
            raise HTTPException(
                status_code=502, detail=f"validate_bundle.py 타임아웃({UPLOAD_VALIDATE_TIMEOUT_S}s)"
            ) from None
        except Exception as exc:  # noqa: BLE001 — 스크립트 부재 등 subprocess 실행 자체 실패
            raise HTTPException(status_code=502, detail=f"validate_bundle.py 실행 실패: {exc!r}") from exc

        # validate_bundle.py --json 계약: exit 0(통과)/1(오류)이어도 stdout 마지막은 항상 결과 JSON.
        # exit code 는 무시하고 그 JSON 을 그대로 200 으로 반환 — ok=false 로 실패를 표현한다.
        try:
            result = json.loads(proc.stdout)
        except Exception:  # noqa: BLE001 — 계약 위반(비정상 종료로 JSON 이 아예 안 나온 경우)만 502
            tail = "\n".join(((proc.stdout or "") + (proc.stderr or "")).splitlines()[-TAIL_LINES:])
            raise HTTPException(
                status_code=502,
                detail=f"validate_bundle.py stdout JSON 파싱 실패 (returncode={proc.returncode}): {tail}",
            ) from None
        return result
    finally:
        _validate_sem.release()


@app.post("/upload/ingest")
async def upload_ingest(
    request: Request,
    x_internal_token: str | None = Header(default=None, alias="X-Internal-Token"),
):
    """번들 인제스트 — /sync/{target} 과 동일한 단일비행 busy 게이트를 공유하는 비동기 job."""
    _check_token(x_internal_token)
    payload = await _read_json_body(request)
    try:
        bundle_dir = _resolve_bundle_dir(payload.get("bundle"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    name = payload.get("name")
    if name is not None and not isinstance(name, str):
        raise HTTPException(status_code=400, detail="name 은 문자열이어야 함")
    attach = payload.get("attach")
    if attach is not None and not isinstance(attach, str):
        raise HTTPException(status_code=400, detail="attach 는 문자열이어야 함")
    overwrite = bool(payload.get("overwrite", False))
    skip_viz = bool(payload.get("skip_viz", False))

    cmd = [sys.executable, INGEST_SCRIPT, bundle_dir]
    if name:
        cmd += ["--name", name]
    if overwrite:
        cmd.append("--overwrite")
    if skip_viz:
        cmd.append("--skip-viz")
    if attach:
        cmd += ["--attach", attach]

    bundle_label = os.path.basename(bundle_dir.rstrip(os.sep)) or bundle_dir
    return _dispatch_job("upload", f"upload:{bundle_label}", _run_upload_ingest_job, (cmd, bundle_dir))


@app.post("/upload/delete")
async def upload_delete(
    request: Request,
    x_internal_token: str | None = Header(default=None, alias="X-Internal-Token"),
):
    """번들 디렉토리 + 그 번들로 만든 upload_kit 데이터셋을 **한 쌍으로** 삭제 (delete_bundle.py 위임).

    되돌릴 수 없으므로 게이트를 셋 건다: ① `confirm: true` 명시(다른 엔드포인트엔 없다 — 실수로
    날아가는 호출을 막는다), ② 번들 **이름**만 받는다(경로 금지 → 탈출 불가, 스크립트가 다시
    업로드 루트 직계인지 검사), ③ busy 중이면 409(인제스트가 읽고 있는 번들을 지우지 않는다).
    marker 없는 데이터셋은 스크립트가 절대 건드리지 않는다(sourcei/frames 보호).
    """
    global _busy, _current, _job_counter
    _check_token(x_internal_token)
    payload = await _read_json_body(request)
    bundles = payload.get("bundles")
    if not isinstance(bundles, list) or not bundles or not all(isinstance(b, str) for b in bundles):
        raise HTTPException(status_code=400, detail="bundles 는 비어있지 않은 문자열 배열이어야 함")
    bad = [b for b in bundles if not _BUNDLE_NAME_RE.match(b)]
    if bad:
        raise HTTPException(status_code=400, detail=f"번들 이름 규칙 위반(경로 불가): {bad[:3]}")
    dry_run = bool(payload.get("dry_run", False))
    if not dry_run and payload.get("confirm") is not True:
        raise HTTPException(status_code=400, detail="삭제는 confirm:true 가 필요합니다 (dry_run:true 면 계획만)")
    cmd = [sys.executable, DELETE_SCRIPT, *bundles, "--json"] + ([] if dry_run else ["--apply"])
    with _state_lock:
        if _busy:
            raise HTTPException(status_code=409, detail=f"다른 작업 진행 중이라 삭제할 수 없습니다: {_current}")
        _job_counter += 1
        _busy = True
        _current = {"job_id": f"delete-{_job_counter}", "target": "upload:delete", "started_at": time.time()}
    def _run_delete():
        # busy 해제는 **스레드가** 한다 — 요청이 (몇 번이든) 취소돼도 subprocess 는 계속 돌므로,
        # 이벤트 루프 쪽 finally 에서 풀면 삭제 도중 인제스트가 같은 번들을 집어갈 수 있다.
        global _busy, _current
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=UPLOAD_DELETE_TIMEOUT_S)
        finally:
            with _state_lock:
                _current = None
                _busy = False

    try:
        # asyncio.to_thread 필수 — upload_validate 주석 참고(이벤트 루프 블로킹 방지).
        proc = await asyncio.to_thread(_run_delete)
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=502, detail=f"delete_bundle.py 타임아웃({UPLOAD_DELETE_TIMEOUT_S}s)") from None
    except Exception as exc:  # noqa: BLE001 — 스크립트 부재 등 실행 자체 실패
        raise HTTPException(status_code=502, detail=f"delete_bundle.py 실행 실패: {exc!r}") from exc

    # --json 계약: 사람용 진행 로그 뒤 **마지막 stdout 줄**이 결과 JSON (ingest 의 _run_subprocess 와 동일).
    lines = (proc.stdout or "").splitlines()
    try:
        return json.loads(lines[-1])
    except Exception:  # noqa: BLE001 — 계약 위반(JSON 이 아예 안 나옴)만 502
        tail = "\n".join((lines + (proc.stderr or "").splitlines())[-TAIL_LINES:])
        raise HTTPException(
            status_code=502,
            detail=f"delete_bundle.py stdout JSON 파싱 실패 (returncode={proc.returncode}): {tail}",
        ) from None


@app.get("/upload/bundles")
def upload_bundles(
    x_internal_token: str | None = Header(default=None, alias="X-Internal-Token"),
) -> list[dict]:
    """UPLOAD_ROOT 하위에서 manifest.json 있는 디렉토리 나열 (파일시스템만 읽음, 락 불필요)."""
    _check_token(x_internal_token)
    out: list[dict[str, Any]] = []
    try:
        entries = sorted(os.listdir(UPLOAD_ROOT))
    except OSError as exc:
        # 응답 계약(list[dict])은 유지 — 바꾸면 플러그인/소비자가 dict 를 기대하게 재작업해야
        # 한다. 대신 컨테이너 로그에는 남겨 "번들 0개"와 "마운트 자체가 깨짐"을 구분 가능하게
        # 한다(minor 리뷰 지적 — 이전엔 조용히 빈 목록이라 원인 추적이 안 됐다).
        print(f"[sync_api] /upload/bundles: UPLOAD_ROOT 접근 실패 ({UPLOAD_ROOT}): {exc}", file=sys.stderr)
        return out
    for name in entries:
        bdir = os.path.join(UPLOAD_ROOT, name)
        if not os.path.isdir(bdir) or not os.path.isfile(os.path.join(bdir, UPLOAD_MANIFEST_FILE)):
            continue
        out.append({
            "bundle": name,
            "path": bdir,
            "has_gt": os.path.isfile(os.path.join(bdir, UPLOAD_GT_FILE)),
            "ingested": os.path.isfile(os.path.join(bdir, UPLOAD_ARTIFACTS_DIR, UPLOAD_INGEST_REPORT_FILE)),
        })
    return out


# ── FiftyOne 좌석 자동 배정 (2026-09-03) ─────────────────────────────────────
# nginx-seats.conf 가 쿠키·IP표·?seat 가 전부 없는 접속을 `/__seat_assign` → 여기로 넘긴다.
# 각 좌석 컨테이너의 점유 카운터(fiftyone_relaunch.py, :SEAT_OCCUPANCY_PORT — 그 좌석 :5151 에
# 붙은 브라우저 연결 수)를 병렬로 물어 **빈 좌석**을 고르고 `302 <원래URI>?seat=N` 으로 돌려보낸다.
# 쿠키는 nginx 의 `map $arg_seat` 가 박으므로 여기선 세우지 않는다. 인증 없음 — 브라우저가
# 프록시 경유로 직접 치는 경로라 토큰을 요구할 수 없고, 호스트 포트 노출도 없다.
# 빈 좌석이 없거나 카운터가 전부 죽어 있으면 점유 현황을 보여주는 선택 페이지로 떨어진다
# (nginx 는 이 서비스 자체가 죽었을 때만 자기 정적 폴백 페이지를 쓴다).
SEAT_HOSTS = {
    "1": "analysis-fiftyone", "2": "analysis-fiftyone-2", "3": "analysis-fiftyone-3",
    "4": "analysis-fiftyone-4", "5": "analysis-fiftyone-5",
}
SEAT_OCC_PORT = int(os.environ.get("SEAT_OCCUPANCY_PORT", "5160"))
# 상한 있는 일반 좌석(2~5)을 먼저 채우고 좌석 1(무제한, 무거운 프롬프트 분석용)은 마지막 —
# 무거운 작업을 하는 사람은 `?seat=1` 한 번(쿠키 1년) 또는 nginx IP 표로 자기 좌석을 고정한다.
SEAT_AUTO_ORDER = [s for s in os.environ.get("SEAT_AUTO_ORDER", "2,3,4,5,1").split(",") if s in SEAT_HOSTS]
# 방금 배정한 좌석은 브라우저가 SSE 를 열어 카운터에 잡히기 전까지 '점유'로 간주 — 두 사람이
# 같은 순간 들어와 같은 빈 좌석을 받는 경합 창을 막는다.
SEAT_RECENT_HOLD_S = float(os.environ.get("SEAT_RECENT_HOLD_S", "20"))
_recent_seat_assign: dict[str, float] = {}
_seat_lock = threading.Lock()


def _probe_seat(seat: str) -> dict[str, Any]:
    url = f"http://{SEAT_HOSTS[seat]}:{SEAT_OCC_PORT}/occupancy"
    try:
        with urllib.request.urlopen(url, timeout=1.0) as r:
            data = json.loads(r.read().decode("utf-8"))
        return {"seat": seat, "up": True, "connections": int(data.get("connections", 0))}
    except Exception as exc:  # noqa: BLE001 — 미기동/DNS 실패/카운터 없음 전부 '상태 불명' 한 부류
        return {"seat": seat, "up": False, "connections": None, "error": type(exc).__name__}


_SEAT_POOL = ThreadPoolExecutor(max_workers=len(SEAT_HOSTS), thread_name_prefix="seat-probe")  # 요청마다 풀 생성 금지


def _seat_occupancy_all() -> dict[str, dict[str, Any]]:
    return {r["seat"]: r for r in _SEAT_POOL.map(_probe_seat, SEAT_HOSTS)}


def _with_seat_param(original_uri: str | None, seat: str) -> str:
    """원래 URI 에 seat=N 을 붙인다. 기존 seat 파라미터는 제거 — nginx `$arg_seat` 는 첫 등장값을
    쓰므로 `seat=9&seat=2` 처럼 남기면 무효값이 이겨 302 루프가 된다."""
    uri = original_uri if original_uri and original_uri.startswith("/") else "/"
    parts = urllib.parse.urlsplit(uri)
    # nginx 의 @seat_dead 가 `302 /__seat_assign` 으로 보내거나 누가 그 경로를 직접 치면 원래 URI 가
    # 배정기 자신이다 — 그대로 seat 를 붙여 돌려보내면 다시 배정기로 와 좌석 홀드를 전부 태운다(m5).
    path = parts.path or "/"
    if path.startswith("/__seat_assign"):
        path = "/"
    q = [(k, v) for k, v in urllib.parse.parse_qsl(parts.query, keep_blank_values=True) if k != "seat"]
    q.append(("seat", seat))
    return urllib.parse.urlunsplit(("", "", path, urllib.parse.urlencode(q), ""))


def _seat_pick_html(occ: dict[str, dict[str, Any]], reason: str) -> str:
    rows = []
    for s in sorted(SEAT_HOSTS):
        o = occ.get(s) or {}
        if not o.get("up"):
            rows.append(f'<span class="off">좌석 {s} — 미기동/상태 불명</span>')
            continue
        n = o.get("connections") or 0
        state = f"사용 중 (연결 {n})" if n else "비어 있음"
        label = " — 무거운 프롬프트 분석용(메모리 상한 없음)" if s == "1" else ""
        rows.append(f'<a class="seat" href="/?seat={s}">좌석 {s}{label} · {state}</a>')
    return (
        '<!doctype html><html lang="ko"><head><meta charset="utf-8"><title>FiftyOne 좌석 선택</title>'
        "<style>body{font-family:sans-serif;max-width:640px;margin:48px auto;padding:0 16px;color:#222}"
        "a.seat{display:block;padding:14px 18px;margin:10px 0;border:1px solid #888;border-radius:8px;"
        "text-decoration:none;color:#111;font-size:18px}a.seat:hover{background:#f0f4ff}"
        "span.off{display:block;padding:14px 18px;margin:10px 0;border:1px dashed #bbb;border-radius:8px;"
        "color:#999;font-size:18px}p.note{color:#555;font-size:14px}</style></head><body>"
        f"<h1>FiftyOne 좌석 선택</h1><p>{reason}</p>" + "".join(rows) +
        '<p class="note">FiftyOne 은 한 좌석을 여러 명이 쓰면 데이터셋 전환·패널 설정이 서로 공유됩니다. '
        "선택은 이 브라우저에 1년간 기억되며, 바꾸려면 주소 뒤에 <code>?seat=N</code> 을 붙여 접속하세요.</p>"
        "</body></html>"
    )


@app.get("/seat/occupancy")
def seat_occupancy() -> dict:
    """좌석별 점유 현황 (진단용). 인증 없음 — 읽기 전용 카운트만."""
    return {"seats": _seat_occupancy_all(), "auto_order": SEAT_AUTO_ORDER, "recent_hold_s": SEAT_RECENT_HOLD_S}


@app.get("/seat/assign")
def seat_assign(x_original_uri: str | None = Header(default=None, alias="X-Original-URI")):
    """빈 좌석 자동 배정 → 302 <원래URI>?seat=N. 빈 좌석이 없으면 점유 현황 선택 페이지(200)."""
    occ = _seat_occupancy_all()
    now = time.time()
    chosen: str | None = None
    with _seat_lock:
        for s in SEAT_AUTO_ORDER:
            o = occ.get(s)
            if not o or not o["up"] or o["connections"]:
                continue
            if now - _recent_seat_assign.get(s, 0.0) < SEAT_RECENT_HOLD_S:
                continue
            chosen = s
            _recent_seat_assign[s] = now
            break
    if chosen:
        return RedirectResponse(url=_with_seat_param(x_original_uri, chosen), status_code=302)
    up = [s for s, o in occ.items() if o.get("up")]
    reason = ("빈 좌석이 없습니다 — 다른 사람이 쓰는 좌석을 고르면 화면이 공유됩니다."
              if up else "좌석 점유 카운터에 연결할 수 없어 자동 배정을 못 했습니다 — 직접 고르세요.")
    return HTMLResponse(_seat_pick_html(occ, reason), status_code=200, headers={"Cache-Control": "no-store"})


# ── 브라우저 업로드 (2026-09-03) ─────────────────────────────────────────────
# nginx `/__upload/*` → 여기 `/upload/*`. 번들을 zip 하나로 PUT(raw body 스트리밍)하면 UPLOAD_ROOT 아래로
# 안전하게 해제(경로 탈출·심볼릭링크·크기 상한 검사)하고 validate 결과를 함께 돌려준다. 그 뒤 UI 가
# 기존 /upload/ingest · /upload/job 으로 임포트를 돌린다. docker cp 반입은 그대로 유효(초대형 대안).
# FiftyOne 오퍼레이터 창의 zip 입력(user-embeddings import_project_bundle)도 결국 여기로 PUT 한다 —
# 다만 그 경로는 base64 로 좌석 프로세스를 통과하므로(파일의 ≈3.4배 순간 점유) APP_MODAL_UPLOAD_MAX_MB
# (기본 256MB) 상한이 붙는다. 큰 번들은 이 페이지가 raw 스트리밍으로 직접 받는다.
UPLOAD_MAX_BYTES = int(os.environ.get("UPLOAD_MAX_BYTES", str(20 * 1024 ** 3)))
UPLOAD_UI_FILE = os.environ.get("UPLOAD_UI_FILE", "/workspace/upload_ui.html")
UPLOAD_INCOMING_DIR = ".incoming"  # UPLOAD_ROOT 하위 임시(같은 FS → rename 원자적). 점 접두라 /upload/bundles 가 무시한다
# bundle_common.DATASET_NAME_RE 와 동일 — 이 프로세스는 numpy 를 적재하지 않는다는 파일 철학 때문에 복제
_BUNDLE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def _zip_bundle_root(zf: zipfile.ZipFile) -> str:
    """zip 안 manifest.json 위치로 번들 루트 접두어를 정한다: 루트 직치('') 또는 최상위 폴더 하나('x/')."""
    tail = "manifest.json"
    roots = sorted({
        n[: -len(tail)] for n in zf.namelist()
        if n.endswith(tail) and not n.startswith("__MACOSX/") and n[: -len(tail)].count("/") <= 1
    })
    if len(roots) != 1:
        raise ValueError(
            "zip 루트(또는 최상위 폴더 하나) 에 manifest.json 이 정확히 1개 있어야 함 "
            f"(발견 {len(roots)}개: {roots[:3]})")
    return roots[0]


def _safe_extract(zf: zipfile.ZipFile, root_prefix: str, dest: str) -> tuple[int, int]:
    """zip → dest 로 스트리밍 해제. 경로 탈출·절대경로·백슬래시·심볼릭링크 거부, 해제 총량 상한."""
    total = files = 0
    for info in zf.infolist():
        name = info.filename
        if name.startswith("__MACOSX/") or not name.startswith(root_prefix):
            continue
        rel = name[len(root_prefix):]
        if not rel or rel.endswith("/"):
            continue  # 디렉토리 엔트리
        if "\\" in rel or posixpath.isabs(rel):
            raise ValueError(f"zip 경로 형식 위반: {name!r}")
        norm = posixpath.normpath(rel)
        if norm == ".." or norm.startswith("../"):
            raise ValueError(f"zip 경로 탈출: {name!r}")
        if (info.external_attr >> 16) & 0o170000 == 0o120000:
            raise ValueError(f"zip 심볼릭링크 금지: {name!r}")
        total += info.file_size
        if total > UPLOAD_MAX_BYTES:
            raise ValueError(f"해제 총량이 상한({UPLOAD_MAX_BYTES} bytes)을 초과")
        out = os.path.join(dest, norm)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with zf.open(info) as src, open(out, "wb") as dst:
            shutil.copyfileobj(src, dst, 1024 * 1024)
        files += 1
    if files == 0:
        raise ValueError("zip 에 파일이 없음")
    return files, total


def _validate_dir_sync(bundle_dir: str) -> dict:
    """validate_bundle.py --json 을 동기 subprocess 로 (upload_validate 와 같은 계약, 스레드에서 호출)."""
    cmd = [sys.executable, VALIDATE_SCRIPT, bundle_dir, "--json"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=UPLOAD_VALIDATE_TIMEOUT_S)
        return json.loads(proc.stdout)
    except Exception as exc:  # noqa: BLE001 — 검증기 자체 실패도 업로드 성공은 유지하고 결과에만 표시
        return {"ok": False, "errors": [f"validate_bundle.py 실행/파싱 실패: {type(exc).__name__}: {exc}"],
                "warnings": [], "mode": None, "counts": {}, "versions": []}


def _install_archive(tmp_zip: str, name: str | None, overwrite: bool) -> dict:
    """업로드된 zip → UPLOAD_ROOT/<번들이름>/ 로 설치 + 검증. 스레드에서 실행(해제가 수 분일 수 있음)."""
    if not zipfile.is_zipfile(tmp_zip):
        raise HTTPException(status_code=400, detail="zip 파일이 아닙니다 (번들 디렉토리를 zip 으로 묶어 올리세요)")
    with zipfile.ZipFile(tmp_zip) as zf:
        try:
            root = _zip_bundle_root(zf)
            manifest = json.loads(zf.read(root + "manifest.json").decode("utf-8-sig"))
        except (ValueError, KeyError, UnicodeDecodeError) as exc:
            raise HTTPException(status_code=400, detail=f"번들 zip 구조 오류: {exc}") from exc
        dataset = manifest.get("dataset") if isinstance(manifest, dict) else None
        bundle_name = (name or "").strip() or (dataset if isinstance(dataset, str) else "")
        if not _BUNDLE_NAME_RE.match(bundle_name or "") or bundle_name.endswith("-prompts"):
            raise HTTPException(
                status_code=400,
                detail=f"번들/데이터셋 이름 규칙 위반: {bundle_name!r} (영숫자로 시작, [A-Za-z0-9._-], -prompts 금지)")
        dest = os.path.join(UPLOAD_ROOT, bundle_name)
        if os.path.exists(dest) and not overwrite:
            raise HTTPException(
                status_code=409,
                detail=f"번들 '{bundle_name}' 이 이미 있습니다 — 덮어쓰기를 켜면 디렉토리를 교체합니다 "
                       "(그 번들로 만든 데이터셋은 재임포트 전까지 이미지가 깨짐)")
        stage = os.path.join(UPLOAD_ROOT, UPLOAD_INCOMING_DIR, uuid.uuid4().hex)
        os.makedirs(stage)
        try:
            files, total = _safe_extract(zf, root, stage)
            if os.path.isdir(dest):
                shutil.rmtree(dest)
            os.rename(stage, dest)
        except ValueError as exc:
            shutil.rmtree(stage, ignore_errors=True)
            raise HTTPException(status_code=400, detail=f"zip 해제 거부: {exc}") from exc
        except Exception:
            shutil.rmtree(stage, ignore_errors=True)
            raise
    return {
        "bundle": bundle_name,
        "dataset": (name or "").strip() or (dataset if isinstance(dataset, str) else bundle_name),
        "path": dest, "files": files, "bytes": total,
        "validate": _validate_dir_sync(dest),
    }


@app.put("/upload/archive")
async def upload_archive(request: Request, name: str | None = None, overwrite: bool = False):
    """번들 zip 업로드(raw body 스트리밍) → 해제 → 검증. 인증 없음(프록시 경유 브라우저 경로)."""
    os.makedirs(os.path.join(UPLOAD_ROOT, UPLOAD_INCOMING_DIR), exist_ok=True)
    tmp_zip = os.path.join(UPLOAD_ROOT, UPLOAD_INCOMING_DIR, f"{uuid.uuid4().hex}.zip")
    received = 0
    try:
        with open(tmp_zip, "wb") as f:
            async for chunk in request.stream():
                received += len(chunk)
                if received > UPLOAD_MAX_BYTES:
                    raise HTTPException(status_code=413, detail=f"업로드 상한 {UPLOAD_MAX_BYTES} bytes 초과")
                await asyncio.to_thread(f.write, chunk)
        if received == 0:
            raise HTTPException(status_code=400, detail="빈 업로드")
        return await asyncio.to_thread(_install_archive, tmp_zip, name, overwrite)
    finally:
        try:
            os.remove(tmp_zip)
        except OSError:
            pass


# ── 반입 경로 ③ — URL 다운로드 (`POST /upload/fetch`) ────────────────────────
# 업로드 페이지/오퍼레이터가 URL 을 주면 **서버가 직접 내려받아** 기존 `_install_archive` 에 넘긴다.
# 즉 해제·경로검사·이름규칙·원자설치·검증은 ①(브라우저 스트리밍 업로드)과 **완전히 같은 코드**를 타고,
# 신규 코드는 "URL → tmp zip" 구간뿐이다.
#
# ⚠️ 이 프로세스는 bridge 네트워크(internal=false)에 있어 사내 호스트·인터넷 어디로든 나갈 수 있고,
#    이 엔드포인트는 `/upload/archive` 와 같은 계열이라 **무인증**이다. 그래서 SSRF 를 주소 정책으로 막는다:
#    ① http/https 만 ② 포트 allowlist ③ **CIDR allowlist 에 든 사내 대역만 허용**(공인 IP 포함 그 밖은 전부 거부.
#    루프백·링크로컬 169.254.169.254·도커 브리지 172.x 는 기본값에 안 들어 있으므로 자동 차단)
#    ④ 리다이렉트는 따라가되 **홉마다 재검증** ⑤ DNS 는 getaddrinfo 가 준 **모든** 주소가 허용 대역이어야 통과
#    ⑥ 실패해도 **원격 응답 본문·헤더는 절대 기록하지 않는다**(상태코드·바이트 수만) — 기록하는 순간
#       무인증 사내 GET 프록시가 된다.
# 동시성: `_busy` 단일비행에 **넣지 않는다**. 30분짜리 다운로드가 `/sync/*`·`/upload/ingest` 를 전부 409 로
#    만들면 안 된다(`/upload/validate` 가 전용 세마포어를 쓰는 것과 같은 이유). job 레코드는 `_history` 에만
#    넣으므로 기존 `GET /upload/job` 폴링이 **무변경으로** 동작하고 `/status` 의 last/busy 계약도 안 건드린다.
UPLOAD_FETCH_ENABLED = os.environ.get("UPLOAD_FETCH_ENABLED", "1").strip() not in ("0", "false", "False")
UPLOAD_FETCH_ALLOW_CIDRS = os.environ.get("UPLOAD_FETCH_ALLOW_CIDRS", "10.0.0.0/16")
UPLOAD_FETCH_ALLOW_PORTS = os.environ.get("UPLOAD_FETCH_ALLOW_PORTS", "80,443,9000,9001")
UPLOAD_FETCH_CONNECT_TIMEOUT_S = float(os.environ.get("UPLOAD_FETCH_CONNECT_TIMEOUT_S", "10"))
UPLOAD_FETCH_READ_TIMEOUT_S = float(os.environ.get("UPLOAD_FETCH_READ_TIMEOUT_S", "60"))
# read 타임아웃만으론 "아주 느리게 계속 보내는" 전송을 못 끊는다 — 전체 시한을 따로 둔다.
UPLOAD_FETCH_MAX_SECONDS = int(os.environ.get("UPLOAD_FETCH_MAX_SECONDS", "7200"))
UPLOAD_FETCH_MAX_REDIRECTS = int(os.environ.get("UPLOAD_FETCH_MAX_REDIRECTS", "5"))
# 기본 1 — 20GB 다운로드 N 개가 동시에 .incoming 디스크를 먹는 것을 막는다(해제 전 tmp + 해제본 = 최악 2배).
_fetch_sem = threading.BoundedSemaphore(max(1, int(os.environ.get("UPLOAD_FETCH_CONCURRENCY", "1"))))


def _fetch_allowed_nets() -> list:
    nets = []
    for tok in UPLOAD_FETCH_ALLOW_CIDRS.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            nets.append(ipaddress.ip_network(tok, strict=False))
        except ValueError as exc:  # 설정 오타는 조용히 넘기지 않고 로그 — 다만 기동은 막지 않는다
            print(f"[sync_api] UPLOAD_FETCH_ALLOW_CIDRS 항목 무시({tok!r}): {exc}", file=sys.stderr)
    return nets


def _fetch_check_url(raw: str) -> tuple[str, str]:
    """URL → (정규화 URL, host). 정책 위반은 HTTPException(400 형식 / 403 주소 / 502 DNS).

    리다이렉트 홉마다 다시 부른다 — 첫 URL 만 검사하면 `Location: http://127.0.0.1/...` 로 뚫린다.
    """
    try:
        parts = urllib.parse.urlsplit((raw or "").strip())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"URL 파싱 실패: {exc}") from exc
    if parts.scheme not in ("http", "https"):
        raise HTTPException(status_code=400, detail=f"http/https 만 허용합니다 (받은 스킴: {parts.scheme or '없음'})")
    if parts.username or parts.password:
        raise HTTPException(status_code=400, detail="URL 에 자격증명을 넣을 수 없습니다 (presigned 쿼리 서명을 쓰세요)")
    host = parts.hostname
    if not host:
        raise HTTPException(status_code=400, detail="URL 에 호스트가 없습니다")
    try:
        port = parts.port or (443 if parts.scheme == "https" else 80)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"포트 형식 오류: {exc}") from exc
    allow_ports = {int(p) for p in UPLOAD_FETCH_ALLOW_PORTS.replace(" ", "").split(",") if p}
    if port not in allow_ports:
        raise HTTPException(
            status_code=403,
            detail=f"허용되지 않은 포트 {port} (허용: {sorted(allow_ports)}, env UPLOAD_FETCH_ALLOW_PORTS)")
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise HTTPException(status_code=502, detail=f"DNS 해석 실패: {host} ({exc})") from exc
    nets = _fetch_allowed_nets()
    if not nets:
        raise HTTPException(status_code=403, detail="허용 대역이 비어 있습니다 (env UPLOAD_FETCH_ALLOW_CIDRS)")
    for info in infos:
        addr = ipaddress.ip_address(info[4][0])
        if addr.version == 6 and addr.ipv4_mapped:
            addr = addr.ipv4_mapped
        if not any(addr in net for net in nets):
            raise HTTPException(
                status_code=403,
                detail=f"허용되지 않은 대상 주소 {addr} — 사내 대역만 반입할 수 있습니다 "
                       f"(허용: {UPLOAD_FETCH_ALLOW_CIDRS}). 외부 링크는 파일을 받아 업로드 페이지로 올리세요")
    return urllib.parse.urlunsplit(parts), host


def _fetch_to_tmp(url: str, tmp_zip: str, on_progress) -> int:
    """URL → tmp_zip 스트리밍 저장. 반환 = 수신 바이트. 정책·상한·시한 위반은 ValueError."""
    import requests  # 지연 import — 이미지에 없더라도 API 전체 기동을 깨뜨리지 않는다

    started = time.time()
    current = url
    for _ in range(UPLOAD_FETCH_MAX_REDIRECTS + 1):
        current, _host = _fetch_check_url(current)
        resp = requests.get(
            current, stream=True, allow_redirects=False,
            timeout=(UPLOAD_FETCH_CONNECT_TIMEOUT_S, UPLOAD_FETCH_READ_TIMEOUT_S),
        )
        with resp:
            if resp.status_code in (301, 302, 303, 307, 308):
                location = resp.headers.get("Location")
                if not location:
                    raise ValueError(f"리다이렉트 응답에 Location 이 없습니다 (HTTP {resp.status_code})")
                current = urllib.parse.urljoin(current, location)
                continue
            if resp.status_code != 200:
                raise ValueError(f"원격 응답 HTTP {resp.status_code}")  # 본문은 기록하지 않는다
            declared = int(resp.headers.get("Content-Length") or 0)
            if declared > UPLOAD_MAX_BYTES:
                raise ValueError(f"Content-Length {declared} bytes 가 상한({UPLOAD_MAX_BYTES})을 초과")
            received = 0
            with open(tmp_zip, "wb") as f:
                for chunk in resp.iter_content(1024 * 1024):
                    if not chunk:
                        continue
                    if received == 0 and not chunk.startswith(b"PK\x03\x04"):
                        # 로그인 페이지·에러 HTML 을 20GB 받는 것을 첫 청크에서 끊는다
                        raise ValueError("zip 시그니처(PK)가 아닙니다 — 로그인 페이지나 HTML 응답일 수 있습니다")
                    received += len(chunk)
                    if received > UPLOAD_MAX_BYTES:
                        raise ValueError(f"수신 총량이 상한({UPLOAD_MAX_BYTES} bytes)을 초과")
                    if time.time() - started > UPLOAD_FETCH_MAX_SECONDS:
                        raise ValueError(f"다운로드 전체 시한({UPLOAD_FETCH_MAX_SECONDS}s) 초과")
                    f.write(chunk)
                    on_progress(received, declared or None)
            if received == 0:
                raise ValueError("빈 응답 (0 bytes)")
            return received
    raise ValueError(f"리다이렉트가 {UPLOAD_FETCH_MAX_REDIRECTS}회를 넘었습니다")


def _fetch_patch(job_id: str, patch: dict) -> None:
    """fetch job 레코드 부분 갱신. `_history` 만 건드린다 — `_busy`/`_current`/`_last` 는 불변."""
    with _state_lock:
        record = _history.get(job_id)
        if record is not None:
            record.update(patch)


def _fetch_new_job(target: str) -> tuple[str, float]:
    global _job_counter
    with _state_lock:
        _job_counter += 1
        job_id = f"fetch-{_job_counter}"
        started_at = time.time()
        _history[job_id] = {
            "job_id": job_id, "target": target, "state": "running", "returncode": None,
            "started_at": started_at, "finished_at": None, "result": None, "tail": [],
            "phase": "download", "downloaded": 0, "total": None,
        }
        while len(_history) > HISTORY_MAX:
            _history.popitem(last=False)
    return job_id, started_at


def _run_upload_fetch_job(job_id: str, url: str, name: str | None, overwrite: bool) -> None:
    """다운로드 → `_install_archive` → job 레코드 종결. 백그라운드 스레드.

    finally 에서 tmp 제거 + 세마포어 반납을 **반드시** 한다 — 반납이 새면 이후 모든 fetch 가 영구 409 다
    (`_run_upload_ingest_job` 이 같은 이유로 try/finally 를 쓴다).
    """
    tmp_zip = os.path.join(UPLOAD_ROOT, UPLOAD_INCOMING_DIR, f"{uuid.uuid4().hex}.zip")
    last_report = [0.0]

    def on_progress(done: int, total: int | None) -> None:
        now = time.time()
        if now - last_report[0] < 1.0:      # 20GB 를 청크마다 기록하면 락을 2만 번 잡는다
            return
        last_report[0] = now
        _fetch_patch(job_id, {"downloaded": done, "total": total})

    try:
        os.makedirs(os.path.join(UPLOAD_ROOT, UPLOAD_INCOMING_DIR), exist_ok=True)
        received = _fetch_to_tmp(url, tmp_zip, on_progress)
        _fetch_patch(job_id, {"phase": "install", "downloaded": received})
        result = _install_archive(tmp_zip, name, overwrite)
        _fetch_patch(job_id, {
            "state": "done", "returncode": 0, "finished_at": time.time(), "phase": "done",
            "result": result,
            "tail": [f"{received} bytes 수신 → 번들 '{result['bundle']}' 설치 완료"],
        })
    except HTTPException as exc:     # _install_archive 가 던지는 400/409 등
        _fetch_patch(job_id, {
            "state": "failed", "returncode": exc.status_code, "finished_at": time.time(),
            "phase": "failed", "tail": [str(exc.detail)],
        })
    except Exception as exc:  # noqa: BLE001 — 어떤 실패든 job 레코드에 남겨야 UI 가 이유를 보여준다
        _fetch_patch(job_id, {
            "state": "failed", "returncode": -1, "finished_at": time.time(),
            "phase": "failed", "tail": [f"{type(exc).__name__}: {exc}"],
        })
    finally:
        try:
            os.remove(tmp_zip)
        except OSError:
            pass
        _fetch_sem.release()


@app.post("/upload/fetch")
async def upload_fetch(request: Request):
    """URL 로 번들 zip 반입 (서버가 직접 다운로드). 인증 없음 — 방어는 주소 정책(위 주석).

    202 {"job_id":"fetch-<n>", "url":…, "host":…} → 진행은 기존 `GET /upload/job?job_id=` 로 폴링.
    """
    if not UPLOAD_FETCH_ENABLED:
        raise HTTPException(status_code=503, detail="URL 반입이 꺼져 있습니다 (UPLOAD_FETCH_ENABLED=0)")
    payload = await _read_json_body(request)
    raw_url = payload.get("url")
    if not isinstance(raw_url, str) or not raw_url.strip():
        raise HTTPException(status_code=400, detail="url 이 필요합니다")
    name = payload.get("name")
    if name is not None and not isinstance(name, str):
        raise HTTPException(status_code=400, detail="name 은 문자열이어야 합니다")
    name = (name or "").strip() or None
    if name is not None and (not _BUNDLE_NAME_RE.match(name) or name.endswith("-prompts")):
        raise HTTPException(
            status_code=400,
            detail=f"번들/데이터셋 이름 규칙 위반: {name!r} (영숫자로 시작, [A-Za-z0-9._-], -prompts 금지)")
    overwrite = bool(payload.get("overwrite", False))

    url, host = await asyncio.to_thread(_fetch_check_url, raw_url)  # 400/403/502 를 다운로드 전에 확정
    # 이름 충돌은 **받기 전에** 거른다 — 20GB 를 받고 나서 409 를 주는 건 최악이다.
    if name is not None and os.path.exists(os.path.join(UPLOAD_ROOT, name)) and not overwrite:
        raise HTTPException(
            status_code=409,
            detail=f"번들 '{name}' 이 이미 있습니다 — 덮어쓰기를 켜면 디렉토리를 교체합니다")
    if not _fetch_sem.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="다른 URL 반입이 진행 중입니다 — 끝난 뒤 다시 시도하세요")

    try:
        job_id, _started_at = _fetch_new_job(f"fetch:{name or host}")
        threading.Thread(
            target=_run_upload_fetch_job, args=(job_id, url, name, overwrite), daemon=True,
        ).start()
    except BaseException:
        _fetch_sem.release()
        raise
    return JSONResponse(status_code=202, content={"job_id": job_id, "url": url, "host": host})


@app.get("/upload/job")
def upload_job(job_id: str) -> dict:
    """/status?job_id= 의 /upload 접두 별칭 — 업로드 UI 가 프록시 `/__upload/` 한 경로만 쓰게."""
    return status(job_id)


@app.get("/upload/ui")
def upload_ui():
    """업로드 페이지 (bind mount 된 HTML — 편집 즉시 반영)."""
    try:
        with open(UPLOAD_UI_FILE, encoding="utf-8") as f:
            html = f.read()
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"업로드 UI 파일 없음: {UPLOAD_UI_FILE} ({exc})") from exc
    return HTMLResponse(html, headers={"Cache-Control": "no-store"})
