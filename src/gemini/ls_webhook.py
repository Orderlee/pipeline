"""LS webhook 수신 서버.

annotation 제출 이벤트를 수신하여:
  1. project 내 전체 task 완료 여부 확인
  2. 완료 시 ls_sync.run() 호출 (MinIO + PostgreSQL 동기화)
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
    LS_WEBHOOK_SECRET    /webhook 엔드포인트 공유 시크릿 (필수, 미설정 시 503 fail-closed).
                         LS 가 X-Webhook-Token 헤더로 이 값을 전송해야 한다.
                         register / cmd_register 실행 시 자동으로 헤더에 포함됨.
    SLACK_WEBHOOK_URL    Slack Incoming Webhook URL (미설정 시 알림 생략)
    SLACK_SIGNING_SECRET Slack slash command 검증 시크릿 (필수, 미설정 시 503 fail-closed).
    DATAOPS_POSTGRES_DSN PostgreSQL DSN (필수, postgresql://user:pass@host:port/dbname)
    MINIO_ENDPOINT       (기본: 10.0.0.51:9000)
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

import requests
import uvicorn
from fastapi import BackgroundTasks, FastAPI, Request, Response
from fastapi.responses import PlainTextResponse

from vlm_pipeline.lib.slack_notify import send_slack_alert, send_slack_response  # noqa: F401

# ---------------------------------------------------------------------------
# 환경변수 — ls_webhook_env 에서 re-export (하위 호환 + 테스트 monkeypatch 대상).
# ---------------------------------------------------------------------------

from gemini.ls_webhook_env import (  # noqa: E402,F401
    API_KEY,
    DAGSTER_GRAPHQL_URL,
    DEFAULT_FPS,
    DEFAULT_LABEL_BUCKET,
    DEFAULT_WEBHOOK_HOST,
    DEFAULT_WEBHOOK_PORT,
    LS_URL,
    LS_WEBHOOK_SECRET,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    PG_DSN,
    POST_REVIEW_JOB_NAME,
    SLACK_SIGNING_SECRET,
    SLACK_WEBHOOK_URL,
    WEBHOOK_HOST,
    WEBHOOK_PORT,
)

# ---------------------------------------------------------------------------
# State helpers — re-export (ls_tasks.py 등 외부 호환).
# ---------------------------------------------------------------------------

from gemini.ls_webhook_state import (  # noqa: E402,F401
    STATE_FILE,
    _SYNC_DEBOUNCE_LOCK,
    _SYNC_DEBOUNCE_SEC,
    _SYNC_DEBOUNCE_TIMERS,
    _SYNC_GUARD_LOCK,
    _SYNC_IN_FLIGHT,
    load_state,
    save_state,
    schedule_sync_debounced,
)

# ---------------------------------------------------------------------------
# Finalize helpers — re-export.
# ---------------------------------------------------------------------------

from gemini.ls_webhook_finalize import (  # noqa: E402,F401
    _extract_folder_prefixes,
    _run_sync_and_notify_inner,
    finalize_labels_in_db,
    finalize_project,
    run_sync_and_notify,
)

# ---------------------------------------------------------------------------
# Dagster trigger — re-export.
# ---------------------------------------------------------------------------

from gemini.ls_webhook_dagster import (  # noqa: E402,F401
    _discover_repo_selector,
    trigger_dagster_job,
)


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
    """annotation이 없는 task 수 반환. 0이면 전체 완료.
    fast path: /api/projects/{id}/ 1회 호출로 task_number vs num_tasks_with_annotations 비교.
    실패하거나 필드 누락 시 기존 /api/tasks/ 페이지네이션으로 fallback.
    """
    try:
        resp = requests.get(
            f"{LS_URL}/api/projects/{project_id}/",
            headers=headers,
            timeout=5,
        )
        resp.raise_for_status()
        p = resp.json()
        tn = p.get("task_number")
        na = p.get("num_tasks_with_annotations")
        if isinstance(tn, int) and isinstance(na, int) and tn >= 0 and na >= 0:
            return max(tn - na, 0)
    except Exception as exc:
        print(f"[WARN] 완료 판정 fast path 실패, 페이지네이션 fallback: {exc}")

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
# Slack 알림 — lib/slack_notify 위임 (하위 호환 thin wrapper)
# ---------------------------------------------------------------------------


def send_slack(message: str) -> None:
    if not SLACK_WEBHOOK_URL:
        print(f"[SLACK] (URL 미설정) {message}")
        return
    send_slack_alert(message)


# ---------------------------------------------------------------------------
# FastAPI 앱
# ---------------------------------------------------------------------------

app = FastAPI()


@app.post("/webhook")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    if not LS_WEBHOOK_SECRET:
        return Response(content="Webhook secret not configured", status_code=503)

    token = request.headers.get("X-Webhook-Token", "")
    try:
        token_match = hmac.compare_digest(LS_WEBHOOK_SECRET, token)
    except TypeError:
        token_match = False
    if not token_match:
        return Response(content="Forbidden", status_code=403)

    try:
        payload = await request.json()
    except Exception:
        return Response(content="invalid json", status_code=400)

    action = payload.get("action", "")
    if action not in {"ANNOTATION_CREATED", "ANNOTATION_UPDATED"}:
        return {"status": "ignored", "action": action}

    project_info = payload.get("project", {})
    project_id = project_info.get("id")
    project_title = project_info.get("title", str(project_id))

    if not project_id:
        return {"status": "no_project_id"}

    print(f"[RECV] {action} — project '{project_title}' (id={project_id})")

    # 가드: /sync-approve 로 finalized 된 project 의 재 Submit 은 무시.
    # ls_sync.upsert_video_labels / update_image_labels_in_db 가 review_status
    # 를 'reviewed' 로 되돌려 finalize 가 회귀하는 사고 방지. 재검수 정책이
    # 필요하면 별도 운영 절차로 처리해야 한다 (state.json status 수동 리셋 등).
    state = load_state()
    if state.get(str(project_id), {}).get("status") == "finalized":
        msg = (
            f"⚠️ *{project_title}* (id={project_id}) 는 이미 finalized 상태 — "
            f"재 Submit 무시. 재검수가 필요하면 운영자에게 문의 (state.json status "
            f"리셋 후 sync 재실행 필요)."
        )
        print(f"[GUARD] {msg}")
        send_slack(msg)
        return {"status": "ignored_finalized", "project_id": project_id}

    headers = resolve_auth_headers(API_KEY)
    incomplete = count_incomplete_tasks(headers, project_id)

    if incomplete > 0:
        print(f"[INFO] 미완료 task {incomplete}개 — 대기 중")
        return {"status": "pending", "incomplete": incomplete}

    print(f"[INFO] project '{project_title}' 전체 완료 → background sync 시작")
    background_tasks.add_task(schedule_sync_debounced, project_id, project_title)
    return {"status": "accepted"}


@app.post("/slack/commands")
async def slack_commands(request: Request, background_tasks: BackgroundTasks):
    """Slack slash command 수신: /sync-list, /sync-approve {project_id}"""
    if not SLACK_SIGNING_SECRET:
        return Response(content="Slack signing secret not configured", status_code=503)

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
    expected = "v0=" + hmac.new(SLACK_SIGNING_SECRET.encode(), sig_base.encode(), hashlib.sha256).hexdigest()
    try:
        sig_match = hmac.compare_digest(expected, request.headers.get("X-Slack-Signature", ""))
    except TypeError:
        sig_match = False
    if not sig_match:
        return Response(content="Invalid signature", status_code=403)

    form = await request.form()
    command = str(form.get("command", ""))
    text = str(form.get("text", "")).strip()
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
            f"[{pid}] {info.get('title', '?')} — {info.get('task_count', '?')}개, 마지막 동기화 {sync_time or '없음'}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# webhook 등록 / 목록
# ---------------------------------------------------------------------------


def cmd_register(project_id: int) -> None:
    if not LS_WEBHOOK_SECRET:
        print("ERROR: LS_WEBHOOK_SECRET 환경변수가 설정되지 않았습니다. webhook 등록을 거부합니다.")
        raise SystemExit(1)
    webhook_url = f"http://{WEBHOOK_HOST}:{WEBHOOK_PORT}/webhook"
    auth_headers = resolve_auth_headers(API_KEY)
    json_headers = {**auth_headers, "Content-Type": "application/json"}

    existing_resp = requests.get(
        f"{LS_URL}/api/webhooks/",
        headers=auth_headers,
        params={"project": project_id},
    )
    existing_resp.raise_for_status()
    existing = existing_resp.json() if isinstance(existing_resp.json(), list) else []

    for wh in existing:
        if wh.get("url") != webhook_url:
            continue
        wh_headers = wh.get("headers") or {}
        if wh_headers.get("X-Webhook-Token") == LS_WEBHOOK_SECRET:
            print(f"[등록 스킵] webhook id={wh['id']} 이미 동일 URL + 동일 secret 으로 등록됨 (project={project_id})")
            return
        del_resp = requests.delete(f"{LS_URL}/api/webhooks/{wh['id']}/", headers=auth_headers)
        del_resp.raise_for_status()
        print(f"[교체] 구 webhook id={wh['id']} (headerless 또는 다른 secret) 삭제 후 재등록")

    resp = requests.post(
        f"{LS_URL}/api/webhooks/",
        headers=json_headers,
        json={
            "url": webhook_url,
            "actions": ["ANNOTATION_CREATED", "ANNOTATION_UPDATED"],
            "is_active": True,
            "send_payload": True,
            "send_for_all_actions": False,
            "project": project_id,
            "headers": {"X-Webhook-Token": LS_WEBHOOK_SECRET},
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

    p_fin = sub.add_parser("finalize", help="프로젝트 수동 finalize (Slack 슬래시 커맨드 대체)")
    p_fin.add_argument("--project", type=int, required=True)

    args = parser.parse_args()

    if not API_KEY:
        print("ERROR: LS_API_KEY 환경변수가 필요합니다.")
        return 1

    _MINIO_DEFAULTS = {"minioadmin", "admin", ""}
    _insecure_ok = os.environ.get("ALLOW_INSECURE_DEFAULT_CREDS", "").strip().lower() in {"1", "true", "yes"}
    if not MINIO_ACCESS_KEY or MINIO_ACCESS_KEY in _MINIO_DEFAULTS:
        if not _insecure_ok:
            print(
                "ERROR: MINIO_ACCESS_KEY 가 설정되지 않았거나 기본값입니다. 유효한 자격증명을 설정하세요. (임시 우회: ALLOW_INSECURE_DEFAULT_CREDS=1)"
            )
            return 1
    if not MINIO_SECRET_KEY or MINIO_SECRET_KEY in _MINIO_DEFAULTS:
        if not _insecure_ok:
            print(
                "ERROR: MINIO_SECRET_KEY 가 설정되지 않았거나 기본값입니다. 유효한 자격증명을 설정하세요. (임시 우회: ALLOW_INSECURE_DEFAULT_CREDS=1)"
            )
            return 1

    if args.command == "serve":
        print(f"[INFO] 수신 서버 시작: http://{args.host}:{args.port}/webhook")
        print(f"[INFO] LS 연결: {LS_URL}")
        print(f"[INFO] Slack: {'설정됨' if SLACK_WEBHOOK_URL else '미설정 (로그만 출력)'}")
        print(f"[INFO] PostgreSQL: {'설정됨' if PG_DSN else '미설정'}")
        uvicorn.run(app, host=args.host, port=args.port, log_level="warning")

    elif args.command == "register":
        cmd_register(args.project)

    elif args.command == "list":
        cmd_list()

    elif args.command == "finalize":
        finalize_project(args.project)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
