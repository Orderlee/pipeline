"""LS webhook 수신 서버.

annotation 제출 이벤트를 수신하여:
  1. project 내 전체 task 완료 여부 확인
  2. 완료 시 ls_sync.run() 호출 (MinIO + DuckDB 동기화)
  3. Slack 알림 발송 (첫 완료 vs 재동기화 구분)

Slack slash command:
  /sync-list              확정 대기 중인 project 목록 조회
  /sync-approve {id}      project 최종 확정 → label_status='completed' → downstream 시작

Usage:
    # 수신 서버 실행
    python ls_webhook.py serve

    # LS에 webhook 등록 (project 단위)
    python ls_webhook.py register --project 4

    # 등록된 webhook 목록 확인
    python ls_webhook.py list

환경변수:
    LS_API_KEY           Label Studio API token (필수)
    LS_URL               Label Studio URL (기본: http://localhost:8080)
    WEBHOOK_HOST         이 서버의 호스트명 — LS가 접근할 주소 (기본: localhost)
    WEBHOOK_PORT         이 서버 포트 (기본: 8001)
    SLACK_WEBHOOK_URL    Slack Incoming Webhook URL (미설정 시 알림 생략)
    SLACK_SIGNING_SECRET Slack slash command 검증 시크릿 (미설정 시 검증 생략)
    DATAOPS_DUCKDB_PATH  DuckDB 파일 경로 (필수)
    MINIO_ENDPOINT       (기본: 172.168.47.36:9000)
    MINIO_ACCESS_KEY     (기본: minioadmin)
    MINIO_SECRET_KEY     (기본: minioadmin)
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import uvicorn
from fastapi import BackgroundTasks, FastAPI, Request, Response
from fastapi.responses import PlainTextResponse

# ---------------------------------------------------------------------------
# 환경변수
# ---------------------------------------------------------------------------

DEFAULT_LS_URL           = "http://localhost:8080"
DEFAULT_MINIO_ENDPOINT   = "172.168.47.36:9000"
DEFAULT_MINIO_ACCESS_KEY = "minioadmin"
DEFAULT_MINIO_SECRET_KEY = "minioadmin"
DEFAULT_LABEL_BUCKET     = "vlm-labels"
DEFAULT_FPS              = 24
DEFAULT_WEBHOOK_HOST     = "localhost"
DEFAULT_WEBHOOK_PORT     = 8001
DEFAULT_DAGSTER_GRAPHQL  = "http://docker-dagster-1:3030/graphql"
DEFAULT_POST_REVIEW_JOB  = "post_review_clip_job"

LS_URL            = os.environ.get("LS_URL", DEFAULT_LS_URL).rstrip("/")
API_KEY           = os.environ.get("LS_API_KEY", "")
WEBHOOK_HOST      = os.environ.get("WEBHOOK_HOST", DEFAULT_WEBHOOK_HOST)
WEBHOOK_PORT      = int(os.environ.get("WEBHOOK_PORT", DEFAULT_WEBHOOK_PORT))
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET", "")
DB_PATH           = os.environ.get("DATAOPS_DUCKDB_PATH", "")
MINIO_ENDPOINT    = os.environ.get("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT)
MINIO_ACCESS_KEY  = os.environ.get("MINIO_ACCESS_KEY", DEFAULT_MINIO_ACCESS_KEY)
MINIO_SECRET_KEY  = os.environ.get("MINIO_SECRET_KEY", DEFAULT_MINIO_SECRET_KEY)
DAGSTER_GRAPHQL_URL = os.environ.get("DAGSTER_GRAPHQL_URL", DEFAULT_DAGSTER_GRAPHQL)
POST_REVIEW_JOB_NAME = os.environ.get("POST_REVIEW_JOB_NAME", DEFAULT_POST_REVIEW_JOB)

def _default_ls_state_path() -> Path:
    # 소스 트리(`/src/vlm/gemini/`)는 read-only bind mount 이므로 쓰기 가능 경로로 폴백.
    override = os.environ.get("LS_STATE_FILE")
    if override:
        return Path(override)
    dagster_home = os.environ.get("DAGSTER_HOME")
    if dagster_home:
        return Path(dagster_home) / "ls_review_state.json"
    return Path("/tmp/ls_review_state.json")


STATE_FILE = _default_ls_state_path()


# ---------------------------------------------------------------------------
# Review state 관리 (ls_tasks.py와 공유)
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# Label Studio auth
# ---------------------------------------------------------------------------

def resolve_auth_headers(token: str) -> dict[str, str]:
    try:
        payload_part = token.split(".")[1]
        payload_part += "=" * (-len(payload_part) % 4)
        payload = json.loads(base64.b64decode(payload_part).decode("utf-8"))
        if payload.get("token_type") == "refresh":
            resp = requests.post(f"{LS_URL}/api/token/refresh/", json={"refresh": token})
            resp.raise_for_status()
            return {"Authorization": f"Bearer {resp.json()['access']}"}
    except Exception:
        pass
    return {"Authorization": f"Token {token}"}


# ---------------------------------------------------------------------------
# 완료 여부 확인
# ---------------------------------------------------------------------------

def count_incomplete_tasks(headers: dict, project_id: int) -> int:
    """annotation이 없는 task 수 반환. 0이면 전체 완료."""
    incomplete = 0
    page = 1
    while True:
        resp = requests.get(
            f"{LS_URL}/api/tasks/",
            headers=headers,
            params={"project": project_id, "page": page, "page_size": 500},
        )
        resp.raise_for_status()
        data = resp.json()
        tasks = data if isinstance(data, list) else data.get("tasks", [])
        if not tasks:
            break
        for task in tasks:
            if task.get("total_annotations", 0) == 0:
                incomplete += 1
        if isinstance(data, list) or not data.get("next"):
            break
        page += 1
    return incomplete


# ---------------------------------------------------------------------------
# Slack 알림
# ---------------------------------------------------------------------------

def send_slack(message: str) -> None:
    if not SLACK_WEBHOOK_URL:
        print(f"[SLACK] (URL 미설정) {message}")
        return
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=5)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[SLACK] 발송 실패: {exc}")


def send_slack_response(response_url: str, message: str) -> None:
    """Slack slash command response_url로 후속 메시지 전송."""
    if not response_url:
        return
    try:
        requests.post(response_url, json={"text": message}, timeout=5)
    except Exception as exc:
        print(f"[SLACK] response_url 발송 실패: {exc}")


# ---------------------------------------------------------------------------
# DuckDB finalize
# ---------------------------------------------------------------------------

def finalize_labels_in_db(label_keys: list[str]) -> int:
    """label_status='completed', review_status='finalized' 업데이트.

    labels (video-level) 와 image_labels (image-level) 양쪽에 동일 UPDATE 적용.
    labels_key 스키마가 서로 겹치지 않으므로 각 테이블은 자신의 key 만 매칭됨.
    반환값은 두 테이블의 finalized row 합계.
    """
    import duckdb
    if not label_keys or not DB_PATH:
        return 0
    conn = None
    for attempt in range(5):
        try:
            conn = duckdb.connect(DB_PATH)
            break
        except Exception as e:
            if "locked" in str(e).lower() and attempt < 4:
                time.sleep(2)
                continue
            raise
    if conn is None:
        raise RuntimeError("DuckDB 연결 실패 (5회 재시도 초과)")
    try:
        placeholders = ",".join(["?"] * len(label_keys))
        # Video-level labels (Gemini events)
        conn.execute(
            f"""
            UPDATE labels
            SET label_status  = 'completed',
                review_status = 'finalized'
            WHERE labels_key IN ({placeholders})
              AND label_status = 'pending_review'
            """,
            label_keys,
        )
        # Image-level labels (SAM3 segmentations) — review_status 기준으로 전이
        conn.execute(
            f"""
            UPDATE image_labels
            SET label_status  = 'completed',
                review_status = 'finalized'
            WHERE labels_key IN ({placeholders})
              AND review_status <> 'finalized'
            """,
            label_keys,
        )
        row = conn.execute(
            f"""
            SELECT
              (SELECT COUNT(*) FROM labels
                 WHERE labels_key IN ({placeholders}) AND review_status = 'finalized')
              +
              (SELECT COUNT(*) FROM image_labels
                 WHERE labels_key IN ({placeholders}) AND review_status = 'finalized')
            """,
            label_keys + label_keys,
        ).fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# sync 실행 (background)
# ---------------------------------------------------------------------------

def run_sync_and_notify(project_id: int, project_title: str) -> None:
    """ls_sync.run() 실행 후 Slack 알림 및 state 업데이트."""
    state = load_state()
    pid = str(project_id)
    is_first = state.get(pid, {}).get("last_sync_at") is None

    if not DB_PATH:
        print(f"[ERROR] DATAOPS_DUCKDB_PATH 미설정 — sync 불가")
        send_slack(f"❌ *{project_title}* sync 실패: DuckDB 경로 미설정")
        return

    try:
        import sys as _sys
        _gemini_dir = str(Path(__file__).parent)
        if _gemini_dir not in _sys.path:
            _sys.path.insert(0, _gemini_dir)
        from ls_sync import run as sync_run
        sync_run(
            project_id=project_id,
            ls_url=LS_URL,
            api_key=API_KEY,
            fps=DEFAULT_FPS,
            minio_endpoint=MINIO_ENDPOINT,
            minio_access_key=MINIO_ACCESS_KEY,
            minio_secret_key=MINIO_SECRET_KEY,
            bucket=DEFAULT_LABEL_BUCKET,
            prefix="",
            dry_run=False,
            db_path=DB_PATH,
        )
    except Exception as exc:
        print(f"[ERROR] sync 실패: {exc}")
        send_slack(f"❌ *{project_title}* (id={project_id}) sync 실패: {exc}")
        return

    # state 업데이트
    now = datetime.now(timezone.utc).isoformat()
    entry = state.get(pid, {})
    entry.update({
        "project_id": project_id,
        "title": project_title,
        "last_sync_at": now,
        "status": "pending_finalize",
    })
    state[pid] = entry
    save_state(state)

    if is_first:
        send_slack(
            f"[동기화 완료] *{project_title}* (id={project_id}) — 검수 반영됨\n"
            f"최종 확정이 필요합니다. `/sync-list` 로 확인하세요."
        )
    else:
        send_slack(f"[재동기화] *{project_title}* (id={project_id}) — 수정사항 반영됨, 확정 대기 중")


# ---------------------------------------------------------------------------
# Dagster GraphQL 트리거
# ---------------------------------------------------------------------------

def _discover_repo_selector(graphql_url: str, job_name: str) -> dict | None:
    """repositoryLocations에서 job_name 보유 repo를 찾아 selector dict 반환."""
    query = """
    query { workspaceOrError { __typename ... on Workspace {
      locationEntries { locationOrLoadError { __typename
        ... on RepositoryLocation { name repositories { name pipelines { name } } }
      } }
    } } }
    """
    resp = requests.post(graphql_url, json={"query": query}, timeout=10)
    resp.raise_for_status()
    entries = resp.json().get("data", {}).get("workspaceOrError", {}).get("locationEntries", []) or []
    for entry in entries:
        loc = entry.get("locationOrLoadError") or {}
        if loc.get("__typename") != "RepositoryLocation":
            continue
        for repo in loc.get("repositories", []) or []:
            pipelines = [p.get("name") for p in repo.get("pipelines", []) or []]
            if job_name in pipelines:
                return {
                    "repositoryLocationName": loc.get("name"),
                    "repositoryName": repo.get("name"),
                    "jobName": job_name,
                }
    return None


def trigger_dagster_job(job_name: str, tags: dict[str, str]) -> tuple[bool, str]:
    """Dagster GraphQL launchPipelineExecution 트리거. (성공여부, 메시지) 반환."""
    try:
        selector = _discover_repo_selector(DAGSTER_GRAPHQL_URL, job_name)
    except Exception as exc:
        return False, f"repo discovery 실패: {exc}"
    if not selector:
        return False, f"job '{job_name}'을 포함한 repository를 찾지 못함"

    mutation = """
    mutation Launch($executionParams: ExecutionParams!) {
      launchPipelineExecution(executionParams: $executionParams) {
        __typename
        ... on LaunchRunSuccess { run { runId } }
        ... on PythonError { message }
        ... on PipelineNotFoundError { message }
        ... on RunConfigValidationInvalid { errors { message } }
        ... on InvalidSubsetError { message }
        ... on ConflictingExecutionParamsError { message }
      }
    }
    """
    variables = {
        "executionParams": {
            "selector": selector,
            "runConfigData": "{}",
            "mode": "default",
            "executionMetadata": {
                "tags": [{"key": k, "value": str(v)} for k, v in tags.items()],
            },
        }
    }
    try:
        resp = requests.post(
            DAGSTER_GRAPHQL_URL,
            json={"query": mutation, "variables": variables},
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        return False, f"GraphQL 요청 실패: {exc}"

    result = (payload.get("data") or {}).get("launchPipelineExecution") or {}
    typename = result.get("__typename", "")
    if typename == "LaunchRunSuccess":
        run_id = (result.get("run") or {}).get("runId", "")
        return True, run_id
    return False, f"{typename}: {result.get('message', '') or result.get('errors', '')}"


# ---------------------------------------------------------------------------
# finalize 실행 (background)
# ---------------------------------------------------------------------------

def finalize_project(project_id: int, response_url: str = "") -> None:
    """최종 확정: DuckDB label_status='completed' → downstream 활성화."""
    state = load_state()
    pid = str(project_id)
    info = state.get(pid)

    if not info:
        msg = f"❌ project {project_id} state 정보 없음 (ls_tasks.py create가 먼저 실행되어야 합니다)"
        send_slack(msg)
        send_slack_response(response_url, msg)
        return

    if info.get("status") == "finalized":
        msg = f"ℹ️ *{info['title']}* (id={project_id}) 이미 확정된 project입니다."
        send_slack_response(response_url, msg)
        return

    title = info.get("title", str(project_id))
    label_keys = info.get("label_keys", [])

    if not label_keys:
        msg = f"❌ *{title}* label_keys 없음 — state 파일 확인 필요"
        send_slack(msg)
        send_slack_response(response_url, msg)
        return

    updated = finalize_labels_in_db(label_keys)

    # state 업데이트
    info["status"] = "finalized"
    info["finalized_at"] = datetime.now(timezone.utc).isoformat()
    state[pid] = info
    save_state(state)

    # Dagster post_review_clip_job 트리거 — 검수된 labels.timestamp로 clip 분할
    ok, detail = trigger_dagster_job(
        POST_REVIEW_JOB_NAME,
        tags={
            "trigger": "ls_finalize",
            "project_id": project_id,
            "folder_name": title,
        },
    )
    clip_msg = (
        f"→ clip 생성 job 트리거: run_id={detail}" if ok
        else f"→ ⚠️ clip 생성 트리거 실패 ({detail}) — Dagster UI에서 `{POST_REVIEW_JOB_NAME}` 수동 실행 필요"
    )

    msg = (
        f"✅ *{title}* (id={project_id}) 최종 확정 완료\n"
        f"→ DuckDB {updated}건 반영\n"
        f"{clip_msg}"
    )
    print(f"[FINALIZE] {msg}")
    send_slack(msg)
    send_slack_response(response_url, msg)


# ---------------------------------------------------------------------------
# FastAPI 앱
# ---------------------------------------------------------------------------

app = FastAPI()


@app.post("/webhook")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        payload = await request.json()
    except Exception:
        return Response(content="invalid json", status_code=400)

    action = payload.get("action", "")
    if action not in {"ANNOTATION_CREATED", "ANNOTATION_UPDATED"}:
        return {"status": "ignored", "action": action}

    project_info  = payload.get("project", {})
    project_id    = project_info.get("id")
    project_title = project_info.get("title", str(project_id))

    if not project_id:
        return {"status": "no_project_id"}

    print(f"[RECV] {action} — project '{project_title}' (id={project_id})")

    headers    = resolve_auth_headers(API_KEY)
    incomplete = count_incomplete_tasks(headers, project_id)

    if incomplete > 0:
        print(f"[INFO] 미완료 task {incomplete}개 — 대기 중")
        return {"status": "pending", "incomplete": incomplete}

    print(f"[INFO] project '{project_title}' 전체 완료 → background sync 시작")
    background_tasks.add_task(run_sync_and_notify, project_id, project_title)
    return {"status": "accepted"}


@app.post("/slack/commands")
async def slack_commands(request: Request, background_tasks: BackgroundTasks):
    """Slack slash command 수신: /sync-list, /sync-approve {project_id}"""
    # Slack 서명 검증 (SLACK_SIGNING_SECRET 설정 시)
    if SLACK_SIGNING_SECRET:
        raw_body = await request.body()
        timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
        if not timestamp:
            return Response(content="Missing timestamp", status_code=400)
        try:
            if abs(time.time() - float(timestamp)) > 300:
                return Response(content="Request too old", status_code=400)
        except ValueError:
            return Response(content="Invalid timestamp", status_code=400)
        sig_base = f"v0:{timestamp}:{raw_body.decode()}"
        expected = "v0=" + hmac.new(
            SLACK_SIGNING_SECRET.encode(), sig_base.encode(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, request.headers.get("X-Slack-Signature", "")):
            return Response(content="Invalid signature", status_code=403)

    form         = await request.form()
    command      = str(form.get("command", ""))
    text         = str(form.get("text", "")).strip()
    response_url = str(form.get("response_url", ""))

    if command == "/sync-list":
        return PlainTextResponse(handle_sync_list())

    if command == "/sync-approve":
        if not text.isdigit():
            return PlainTextResponse("사용법: /sync-approve {project_id}")
        project_id = int(text)
        background_tasks.add_task(finalize_project, project_id, response_url)
        return PlainTextResponse(f"project {project_id} 최종 확정 처리 중...")

    return PlainTextResponse(f"알 수 없는 명령어: {command}")


@app.get("/health")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# slash command 핸들러
# ---------------------------------------------------------------------------

def handle_sync_list() -> str:
    state = load_state()
    pending = {k: v for k, v in state.items() if v.get("status") == "pending_finalize"}
    if not pending:
        return "확정 대기 중인 project 없음"
    lines = ["확정 대기 중인 project 목록:"]
    for pid, info in sorted(pending.items(), key=lambda x: x[0]):
        sync_time = (info.get("last_sync_at") or "")[:16].replace("T", " ")
        lines.append(
            f"[{pid}] {info.get('title', '?')} "
            f"— {info.get('task_count', '?')}개, "
            f"마지막 동기화 {sync_time or '없음'}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# webhook 등록 / 목록
# ---------------------------------------------------------------------------

def cmd_register(project_id: int) -> None:
    webhook_url = f"http://{WEBHOOK_HOST}:{WEBHOOK_PORT}/webhook"
    headers = {**resolve_auth_headers(API_KEY), "Content-Type": "application/json"}
    resp = requests.post(
        f"{LS_URL}/api/webhooks/",
        headers=headers,
        json={
            "url": webhook_url,
            "actions": ["ANNOTATION_CREATED", "ANNOTATION_UPDATED"],
            "is_active": True,
            "send_payload": True,
            "project": project_id,
        },
    )
    resp.raise_for_status()
    data = resp.json()
    print(f"[등록 완료] webhook id={data['id']}, url={webhook_url}, project={project_id}")


def cmd_list() -> None:
    headers = resolve_auth_headers(API_KEY)
    resp = requests.get(f"{LS_URL}/api/webhooks/", headers=headers)
    resp.raise_for_status()
    webhooks = resp.json()
    if not webhooks:
        print("등록된 webhook 없음")
        return
    for wh in webhooks:
        print(
            f"id={wh['id']}  url={wh['url']}  "
            f"project={wh.get('project')}  active={wh.get('is_active')}  "
            f"actions={wh.get('actions')}"
        )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="LS webhook 수신 서버 / 등록 관리")
    sub = parser.add_subparsers(dest="command", required=True)

    p_serve = sub.add_parser("serve", help="webhook 수신 서버 실행")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=WEBHOOK_PORT)

    p_reg = sub.add_parser("register", help="LS에 webhook 등록")
    p_reg.add_argument("--project", type=int, required=True)

    sub.add_parser("list", help="등록된 webhook 목록")

    args = parser.parse_args()

    if not API_KEY:
        print("ERROR: LS_API_KEY 환경변수가 필요합니다.")
        return 1

    if args.command == "serve":
        print(f"[INFO] 수신 서버 시작: http://{args.host}:{args.port}/webhook")
        print(f"[INFO] LS 연결: {LS_URL}")
        print(f"[INFO] Slack: {'설정됨' if SLACK_WEBHOOK_URL else '미설정 (로그만 출력)'}")
        print(f"[INFO] DuckDB: {DB_PATH or '미설정'}")
        uvicorn.run(app, host=args.host, port=args.port, log_level="warning")

    elif args.command == "register":
        cmd_register(args.project)

    elif args.command == "list":
        cmd_list()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
