"""Dispatch webhook server.

piaspace-agent가 push 방식으로 dispatch 요청을 보낼 수 있도록
FastAPI 기반 HTTP endpoint를 제공한다.

수신된 요청은 `.dispatch/pending/` 디렉토리에 JSON 파일로 저장되며,
기존 `dispatch_sensor`가 이 파일을 감지하여 `dispatch_stage_job`을 트리거한다.

실행:
    python -m vlm_pipeline.defs.dispatch.webhook_server
    또는 uvicorn vlm_pipeline.defs.dispatch.webhook_server:app --host 0.0.0.0 --port 8090
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

DISPATCH_WEBHOOK_PORT = int(os.getenv("DISPATCH_WEBHOOK_PORT", "8090"))
DISPATCH_WEBHOOK_SECRET = os.getenv("DISPATCH_WEBHOOK_SECRET", "").strip()
INCOMING_DIR = os.getenv("INCOMING_DIR", "/nas/incoming")

DISPATCH_PENDING_DIR = Path(INCOMING_DIR) / ".dispatch" / "pending"

app = FastAPI(title="VLM Pipeline Dispatch Webhook")

_REQUIRED_FIELDS = ("folder_name",)


def _verify_signature(body: bytes, signature: str) -> bool:
    """HMAC-SHA256 서명 검증. secret 미설정 시 항상 통과."""
    if not DISPATCH_WEBHOOK_SECRET:
        return True
    expected = "sha256=" + hmac.new(
        DISPATCH_WEBHOOK_SECRET.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


@app.post("/webhook/dispatch")
async def receive_dispatch(request: Request) -> Response:
    """dispatch 요청 수신 -> .dispatch/pending/ 에 JSON 파일 저장."""
    raw_body = await request.body()

    sig = request.headers.get("X-Webhook-Signature", "")
    if DISPATCH_WEBHOOK_SECRET and not _verify_signature(raw_body, sig):
        return JSONResponse({"error": "invalid_signature"}, status_code=403)

    try:
        payload: dict = json.loads(raw_body)
    except (json.JSONDecodeError, ValueError):
        return JSONResponse({"error": "invalid_json"}, status_code=400)

    if not isinstance(payload, dict):
        return JSONResponse({"error": "payload_must_be_object"}, status_code=400)

    for field in _REQUIRED_FIELDS:
        value = str(payload.get(field) or "").strip()
        if not value:
            return JSONResponse(
                {"error": f"missing_required_field:{field}"},
                status_code=400,
            )

    request_id = str(payload.get("request_id") or "").strip()
    if not request_id:
        request_id = f"webhook_{uuid4().hex[:12]}"
        payload["request_id"] = request_id

    payload.setdefault("received_at", datetime.now().isoformat())
    payload.setdefault("transfer_tool", "webhook")

    DISPATCH_PENDING_DIR.mkdir(parents=True, exist_ok=True)
    file_path = DISPATCH_PENDING_DIR / f"{request_id}.json"

    suffix = 2
    while file_path.exists():
        file_path = DISPATCH_PENDING_DIR / f"{request_id}__{suffix}.json"
        suffix += 1

    file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return JSONResponse(
        {"status": "accepted", "request_id": request_id, "file": file_path.name},
        status_code=202,
    )


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "pending_dir": str(DISPATCH_PENDING_DIR),
        "pending_dir_exists": DISPATCH_PENDING_DIR.exists(),
        "hmac_enabled": bool(DISPATCH_WEBHOOK_SECRET),
    }


def main() -> None:
    import uvicorn

    host = os.getenv("DISPATCH_WEBHOOK_HOST", "0.0.0.0")
    port = DISPATCH_WEBHOOK_PORT
    print(f"[INFO] Dispatch webhook server starting: http://{host}:{port}")
    print(f"[INFO] Pending dir: {DISPATCH_PENDING_DIR}")
    print(f"[INFO] HMAC: {'enabled' if DISPATCH_WEBHOOK_SECRET else 'disabled'}")
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
