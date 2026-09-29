"""GenAI Studio FastAPI app.

엔드포인트:
  GET  /                  — 단일 화면 (탭 2개, 폼)
  POST /genai/batches     — multipart upload + prompt → batch 생성
  GET  /genai/batches     — 최근 batch 목록 (HTMX partial)
  GET  /genai/batches/{batch_id} — 상세 (jobs + 미리보기)
  GET  /healthz

Phase 3 은 Kling 만 (Image→Video 탭 1개). Phase 4 에서 나머지 3엔진 + 두 번째 탭.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import secrets
from pathlib import Path
from pathlib import PurePosixPath
from typing import Annotated

import httpx
import requests
import websockets
from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.websockets import WebSocket, WebSocketDisconnect

from adapters import ENGINE_TAB, all_engine_options, enabled_engines, engines_by_tab
from db import pg
from db.aggregates import cost_summary
from jobs.submit import submit_batch
import limits


_LOG = logging.getLogger("genai.app")
_ROOT = Path(__file__).parent
templates = Jinja2Templates(directory=str(_ROOT / "templates"))
# 헤더 환경 배지. GENAI_ENV_LABEL 로 명시 오버라이드, 미설정 시 staging DB 경로 여부로 유도.
templates.env.globals["env_label"] = os.getenv("GENAI_ENV_LABEL") or (
    "staging · dev" if "staging" in os.getenv("DATAOPS_DUCKDB_PATH", "") else "prod · main"
)
security = HTTPBasic()


# ----- basic auth -------------------------------------------------------
def _check_auth(creds: Annotated[HTTPBasicCredentials, Depends(security)]) -> str:
    # GENAI_BASIC_AUTH_USER/PASS 가 비었더라도 자동 통과는 위험 (사내망 가정 깨질 시
    # 무인증 노출). 명시적 GENAI_AUTH_DISABLED=true 일 때만 우회 허용.
    user = os.getenv("GENAI_BASIC_AUTH_USER", "").strip()
    password = os.getenv("GENAI_BASIC_AUTH_PASS", "").strip()
    auth_disabled = os.getenv("GENAI_AUTH_DISABLED", "").strip().lower() == "true"
    if not user or not password:
        if auth_disabled:
            return creds.username or "anonymous"
        raise HTTPException(
            status_code=503,
            detail="GENAI_BASIC_AUTH_USER/PASS 미설정. "
                   "운영시 채우거나 GENAI_AUTH_DISABLED=true 명시 opt-in 필요.",
        )
    correct_user = secrets.compare_digest(creds.username, user)
    correct_pass = secrets.compare_digest(creds.password, password)
    if not (correct_user and correct_pass):
        raise HTTPException(status_code=401, detail="auth required",
                            headers={"WWW-Authenticate": "Basic"})
    return creds.username


# ----- upload limits ----------------------------------------------------
_MAX_BYTES_PER_FILE = int(os.getenv("GENAI_MAX_BYTES_PER_FILE", str(50 * 1024 * 1024)))
# batch 1개(=단일 /genai/batches 업로드, bulk chunk 단위)당 최대 이미지 수. 50 —
# bulk 상한(_MAX_BULK_JOBS=50)과 정합. async 엔진(kling/veo)은 job 즉시 defer+드레인이라 안전.
_MAX_FILES_PER_BATCH = int(os.getenv("GENAI_MAX_FILES_PER_BATCH", "50"))
_ALLOWED_EXT = {".png", ".jpg", ".jpeg", ".webp"}
_COMFY_NODE_WORKFLOWS = (
    "flux2-klein-4b-edit-v1",
    "sdxl-inpaint-cctv-v1",
)
_COMFY_EDITABLE_BINDINGS = frozenset(
    {
        "source_image",
        "mask_image",
        "prompt",
        "negative_prompt",
        "seed",
        "steps",
        "cfg",
        "denoise",
    }
)

# bulk-submit 묶음 id — 영숫자 / _ / - / . 1~64자. namespacing (team-a.run-001) 용도로 . 허용.
_BULK_GROUP_ID_RE = re.compile(r"[A-Za-z0-9_.-]{1,64}")

# bulk 한 번에 허용하는 최대 jobs (이미지×prompt 조합 총합). default 50 — prod 엔진(kling/veo)은
# ASYNC (job 은 즉시 defer 되고 poll sensor 가 드레인) 이라 브라우저 동기 timeout 무관. 단, sync
# 엔진(nanobanana/gpt_image) 을 활성화하는 경우 ~12s 응답을 브라우저가 기다려야 하니 주의.
# env 로 운영자 override 가능.
_MAX_BULK_JOBS = int(os.getenv("GENAI_MAX_BULK_JOBS", "50"))
_BULK_SLEEP_SECONDS = float(os.getenv("GENAI_BULK_SLEEP_SECONDS", "0.5"))

# 이중 제출 방지 — client 가 보낸 UUID idempotency token 을 TTL 캐시. 브라우저 새로고침
# / double-click 으로 같은 요청이 두 번 들어오면 두 번째는 짧은 미니 response 만 반환.
# ⚠️ in-memory 라 단일 uvicorn worker 가정 (Dockerfile CMD 가 --workers 미지정 → default 1).
# 여러 worker 로 운영하려면 Redis 등 외부 store 로 교체 필요.
_IDEMPOTENCY_TTL = 1800  # 30 분
_idempotency_cache: dict[str, tuple[float, dict]] = {}
_idempotency_lock = __import__("threading").Lock()


def _wants_json(request: Request) -> bool:
    """Accept 헤더 우선순위로 JSON 요청 판정. text/html 보다 application/json 가
    먼저 오면 JSON 응답 (CLI / API 클라이언트). 둘 다 없거나 html 만이면 HTML."""
    accept = (request.headers.get("accept") or "").lower()
    if "application/json" in accept and "text/html" not in accept:
        return True
    # Accept: */*  → 기본 HTML
    if "application/json" in accept and accept.find("application/json") < accept.find("text/html"):
        return True
    return False


def _load_nas_original_blob(batch_id: str, seq: int) -> tuple[bytes | None, str]:
    """genai originals/ 에서 seq 이미지 bytes + filename 읽기.
    경로 후보 (최신 → 구버전 순으로 fallback):
      1. <NAS>/<YYYY-MM-DD>/<batch>/originals/  (현재 단순 구조)
      2. <NAS>/incoming/genai/<YYYY-MM-DD>/<batch>/originals/  (이전: /incoming/genai/ 포함)
      3. <NAS>/genai/<batch>/originals/  (그 이전: 날짜폴더 없던 시절)
    없으면 (None, 탐색경로). retry / drain 양쪽이 공유."""
    from pathlib import Path
    from datetime import datetime
    nas_root = Path(os.getenv("GENAI_NAS_INCOMING", "/nas/data/genai_studio"))
    batch = pg.get_batch_with_jobs(batch_id) or {}
    ts = batch.get("submitted_at")
    if not isinstance(ts, datetime):
        ts = datetime.now()
    date_dir = ts.strftime("%Y-%m-%d")
    candidate_parents = [
        nas_root / date_dir / batch_id / "originals",                    # new
        nas_root / "incoming" / "genai" / date_dir / batch_id / "originals",  # legacy A
        nas_root / "genai" / batch_id / "originals",                      # legacy B
    ]
    for parent in candidate_parents:
        hits = [p for p in parent.glob(f"{int(seq):03d}.*")
                if not p.name.endswith(".partial")]
        if hits:
            orig = hits[0]
            return orig.read_bytes(), orig.name
    return None, str(candidate_parents[0] / f"{int(seq):03d}.*")


def _load_nas_control_blob(batch_id: str, seq: int) -> tuple[bytes | None, str]:
    """Load the quarantined per-job control image (currently an inpaint mask)."""
    from datetime import datetime

    nas_root = Path(os.getenv("GENAI_NAS_INCOMING", "/nas/data/genai_studio"))
    batch = pg.get_batch_with_jobs(batch_id) or {}
    ts = batch.get("submitted_at")
    if not isinstance(ts, datetime):
        ts = datetime.now()
    parent = nas_root / ts.strftime("%Y-%m-%d") / batch_id / "controls"
    hits = [p for p in parent.glob(f"{int(seq):03d}.*") if not p.name.endswith(".partial")]
    if hits:
        return hits[0].read_bytes(), hits[0].name
    return None, str(parent / f"{int(seq):03d}.*")


def _validate_inpaint_pair(source: bytes, mask: bytes) -> None:
    """Require same-size, non-empty raster inputs before they reach ComfyUI."""
    import io

    from PIL import Image, UnidentifiedImageError

    try:
        with Image.open(io.BytesIO(source)) as source_image, Image.open(io.BytesIO(mask)) as mask_image:
            if source_image.size != mask_image.size:
                raise ValueError(
                    f"source/mask dimensions differ: {source_image.size} != {mask_image.size}"
                )
            grayscale = mask_image.convert("L")
            if grayscale.getbbox() is None:
                raise ValueError("mask has no selected pixels")
            colors = grayscale.getcolors(maxcolors=257)
            if colors is None or any(value not in {0, 255} for _count, value in colors):
                raise ValueError("mask must be binary (pixels must be 0 or 255)")
    except UnidentifiedImageError as exc:
        raise ValueError("source or mask is not a decodable image") from exc


def _load_comfy_node_contracts() -> dict[str, dict]:
    """Load only repository-owned workflow templates for the visual node editor.

    The browser receives a visualization contract, but POSTs only allowlisted scalar
    bindings to /genai/batches. It never sends or executes an arbitrary node graph.
    """
    root = Path(os.getenv("COMFYUI_CONTRACT_ROOT", "/app/comfy_contract")) / "workflows"
    configured = {
        item.strip()
        for item in os.getenv("COMFYUI_ALLOWED_WORKFLOWS", ",".join(_COMFY_NODE_WORKFLOWS)).split(",")
        if item.strip()
    }
    contracts: dict[str, dict] = {}
    for workflow_id in _COMFY_NODE_WORKFLOWS:
        if workflow_id not in configured:
            continue
        path = root / f"{workflow_id}.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("workflow_id") != workflow_id:
            raise RuntimeError(f"invalid Comfy workflow contract: {workflow_id}")
        contracts[workflow_id] = payload
    return contracts


def _normalize_comfy_value(value):
    if isinstance(value, list) and len(value) == 2:
        return [str(value[0]), value[1]]
    return value


def _safe_comfy_input_name(value: object) -> str:
    name = str(value or "").strip()
    path = PurePosixPath(name)
    if (
        not name
        or path.is_absolute()
        or ".." in path.parts
        or path.suffix.lower() not in _ALLOWED_EXT
    ):
        raise ValueError("unsafe Comfy input image name")
    return name


def _match_comfy_native_prompt(graph: object) -> tuple[str, dict[str, object]]:
    """Accept scalar edits only when graph structure exactly matches an approved template."""
    if not isinstance(graph, dict):
        raise ValueError("prompt graph must be an object")
    for workflow_id, contract in _load_comfy_node_contracts().items():
        template = contract["prompt"]
        if set(map(str, graph)) != set(map(str, template)):
            continue
        bindings = {
            (str(ref[0]), str(ref[1])): name
            for name, ref in (contract.get("bindings") or {}).items()
        }
        values: dict[str, object] = {}
        matched = True
        for node_id, expected_node in template.items():
            actual_node = graph.get(str(node_id))
            if not isinstance(actual_node, dict) or actual_node.get("class_type") != expected_node.get(
                "class_type"
            ):
                matched = False
                break
            expected_inputs = expected_node.get("inputs") or {}
            actual_inputs = actual_node.get("inputs") or {}
            if set(actual_inputs) != set(expected_inputs):
                matched = False
                break
            for input_name, expected_value in expected_inputs.items():
                actual_value = actual_inputs[input_name]
                binding = bindings.get((str(node_id), str(input_name)))
                if binding in _COMFY_EDITABLE_BINDINGS:
                    values[binding] = actual_value
                    continue
                if _normalize_comfy_value(actual_value) != _normalize_comfy_value(expected_value):
                    matched = False
                    break
            if not matched:
                break
        if not matched:
            continue

        values["source_image"] = _safe_comfy_input_name(values.get("source_image"))
        if "mask_image" in values:
            values["mask_image"] = _safe_comfy_input_name(values["mask_image"])
        prompt = str(values.get("prompt") or "").strip()
        if not prompt or len(prompt) > 10_000:
            raise ValueError("prompt must contain 1..10000 characters")
        values["prompt"] = prompt
        if "negative_prompt" in values:
            negative = str(values.get("negative_prompt") or "").strip()
            if len(negative) > 10_000:
                raise ValueError("negative prompt exceeds 10000 characters")
            values["negative_prompt"] = negative
        seed = int(values.get("seed") or 0)
        steps = int(values.get("steps") or (4 if workflow_id.startswith("flux2-") else 30))
        if seed < 0 or seed >= 2**63:
            raise ValueError("seed must be in [0, 2^63)")
        if workflow_id.startswith("flux2-") and steps != 4:
            raise ValueError("FLUX.2 Klein workflow requires steps=4")
        if not 1 <= steps <= 60:
            raise ValueError("steps must be in [1, 60]")
        values["seed"] = seed
        values["steps"] = steps
        if "cfg" in values:
            cfg = float(values["cfg"])
            if not 0 <= cfg <= 20:
                raise ValueError("cfg must be in [0, 20]")
            values["cfg"] = cfg
        if "denoise" in values:
            denoise = float(values["denoise"])
            if not 0 < denoise <= 1:
                raise ValueError("denoise must be in (0, 1]")
            values["denoise"] = denoise
        return workflow_id, values
    raise ValueError("graph structure does not match an approved Comfy workflow")


def _fetch_comfy_input(name: str) -> bytes:
    path = PurePosixPath(_safe_comfy_input_name(name))
    subfolder = "" if str(path.parent) == "." else str(path.parent)
    response = requests.get(
        f"{os.getenv('COMFYUI_INTERNAL_URL', 'http://comfyui:8188').rstrip('/')}/view",
        params={"filename": path.name, "type": "input", "subfolder": subfolder},
        timeout=60,
    )
    if response.status_code == 404:
        raise ValueError(
            "Load Image node has no available file selected. "
            "For one image, select it inside the Load Image node; "
            "for multiple images, use the top M×1 Batch dialog and its run button."
        )
    response.raise_for_status()
    if len(response.content) > _MAX_BYTES_PER_FILE:
        raise ValueError("Comfy input exceeds GenAI upload limit")
    return response.content


def _basic_auth_valid(authorization: str | None) -> bool:
    auth_disabled = os.getenv("GENAI_AUTH_DISABLED", "").strip().lower() == "true"
    expected_user = os.getenv("GENAI_BASIC_AUTH_USER", "").strip()
    expected_password = os.getenv("GENAI_BASIC_AUTH_PASS", "").strip()
    if auth_disabled and not expected_user and not expected_password:
        return True
    if not authorization or not authorization.lower().startswith("basic "):
        return False
    try:
        decoded = base64.b64decode(authorization.split(None, 1)[1]).decode("utf-8")
        user, password = decoded.split(":", 1)
    except (ValueError, UnicodeDecodeError):
        return False
    return secrets.compare_digest(user, expected_user) and secrets.compare_digest(
        password, expected_password
    )


def _normalize_proxy_path(path: str, *, casefold: bool = True) -> str:
    """Resolve '.'/'..' the way httpx does before the request reaches ComfyUI.

    Matching on the raw path let `./prompt` and `x/../prompt` slip past the guard
    below while httpx still normalized them onto ComfyUI's real /prompt — executing an
    arbitrary node graph with no template match, no GPU lease and no provenance row.

    `casefold=False` keeps the original spelling for the *forwarded* path: the frontend
    bundle is served as case-sensitive hashed files (`assets/index-CENPJz5u.js`), so a
    casefolded path 404s on ComfyUI's static route and the canvas never boots.  The
    allowlist below casefolds its own copy, so folding is not needed for safety.
    """
    parts: list[str] = []
    for part in path.split("/"):
        if not part or part == ".":
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    joined = "/".join(parts)
    return joined.casefold() if casefold else joined


# ----- ComfyUI proxy allowlist ------------------------------------------
# 근거 (추측 금지 — 세 출처의 교집합):
#   1. ComfyUI 고정 커밋 ee71d5c (docker/comfyui/Dockerfile 의 COMFYUI_COMMIT) 의
#      `@routes.*` 선언 전수.  server.py 가 모든 라우트를 `/` 와 `/api` 양쪽에 등록한다.
#   2. comfyui-frontend-package 1.52.7 (requirements.txt 고정) 번들이 실제로 호출하는
#      경로 — `apiURL(e) = api_base + "/api" + e`, `internalURL` = `+ "/internal"`,
#      `fileURL` = api_base 직속(=정적 파일).
#   3. prod `docker-genai-1` 접근로그 실측: ws, api/jobs(폴링), api/object_info,
#      api/system_stats, api/history, 그리고 iframe base `/genai/comfy-native/`.
# 여기에 없는 경로는 403 이다 — custom node 가 라우트를 추가해도 경계가 유지된다.
_COMFY_PROMPT_PATHS = frozenset({"prompt"})
# 실행/자원 제어는 GenAI 가 소유한다.  jobs/cancel 계열은 내부적으로 interrupt+dequeue 라
# blocklist 시절 뚫려 있던 같은 등급의 구멍이었다.
_COMFY_BLOCKED_PATHS = frozenset({"queue", "interrupt", "free", "jobs/cancel"})
_COMFY_API_EXACT = frozenset(
    {
        "embeddings",
        "experiment/models",
        "extensions",
        "features",
        "global_subgraphs",
        "history",
        "i18n",
        "jobs",
        "models",
        "node_replacements",
        "object_info",
        "settings",
        "system_stats",
        "upload/image",
        "upload/mask",
        "userdata",
        "users",
        "v2/userdata",
        "view",
        "workflow_templates",
    }
)
_COMFY_API_PREFIXES = (
    "experiment/models/",
    "global_subgraphs/",
    "history/",
    "jobs/",
    "models/",
    "object_info/",
    "settings/",
    "userdata/",
    "view_metadata/",
    "workflow_templates/",
)
# `/internal/*` 서브앱 — 프론트의 로그 패널과 파일 탐색기가 읽기 전용으로 쓴다.
_COMFY_INTERNAL_EXACT = frozenset({"logs", "logs/raw", "logs/subscribe", "folder_paths"})
_COMFY_INTERNAL_PREFIXES = ("files/",)
# 정적 자원 (web_root = comfyui-frontend-package/static + 서버가 마운트하는 templates/docs).
_COMFY_STATIC_EXACT = frozenset({"", "index.html", "user.css", "materialdesignicons.min.css"})
_COMFY_STATIC_PREFIXES = (
    "assets/",
    "cursor/",
    "docs/",
    "extensions/",
    "fonts/",
    "scripts/",
    "templates/",
)
_COMFY_STATIC_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


def _comfy_proxy_verdict(normalized: str, method: str) -> str:
    """Classify a normalized proxy path: 'prompt' | 'blocked' | 'allow' | 'deny'."""
    key = normalized.casefold()
    if key == "internal" or key.startswith("internal/"):
        rest = key[len("internal/") :] if key.startswith("internal/") else ""
        if rest in _COMFY_INTERNAL_EXACT or any(rest.startswith(p) for p in _COMFY_INTERNAL_PREFIXES):
            return "allow"
        return "deny"
    # ComfyUI mirrors every route under /api, and the frontend always uses that form.
    api = key[len("api/") :] if key.startswith("api/") else key
    if api in _COMFY_PROMPT_PATHS:
        return "prompt"
    if api in _COMFY_BLOCKED_PATHS or (api.startswith("jobs/") and api.endswith("/cancel")):
        return "blocked"
    if api in _COMFY_API_EXACT or any(api.startswith(prefix) for prefix in _COMFY_API_PREFIXES):
        return "allow"
    # Static assets are served from the bare path only (never behind /api).
    if api == key and method.upper() in _COMFY_STATIC_METHODS:
        if key in _COMFY_STATIC_EXACT or any(key.startswith(prefix) for prefix in _COMFY_STATIC_PREFIXES):
            return "allow"
    return "deny"


async def _comfy_http_proxy(request: Request, path: str) -> Response:
    base = os.getenv("COMFYUI_INTERNAL_URL", "http://comfyui:8188").rstrip("/")
    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in {"host", "authorization", "content-length", "connection"}
    }
    try:
        async with httpx.AsyncClient(timeout=120, follow_redirects=False) as client:
            upstream = await client.request(
                request.method,
                f"{base}/{path.lstrip('/')}",
                params=request.query_params,
                content=await request.body(),
                headers=headers,
            )
    except httpx.RequestError as exc:
        return JSONResponse({"error": f"ComfyUI unavailable: {exc}"}, status_code=503)
    response_headers = _safe_comfy_proxy_headers(upstream.headers)
    return Response(content=upstream.content, status_code=upstream.status_code, headers=response_headers)


def _safe_comfy_proxy_headers(headers) -> dict[str, str]:
    """Keep useful headers without letting a Unicode filename turn previews into HTTP 500."""
    allowed = {"content-type", "cache-control", "etag", "last-modified", "content-disposition"}
    safe: dict[str, str] = {}
    for key, value in headers.items():
        if key.lower() not in allowed:
            continue
        try:
            key.encode("latin-1")
            value.encode("latin-1")
        except UnicodeEncodeError:
            continue
        safe[key] = value
    return safe


def create_app() -> FastAPI:
    app = FastAPI(title="GenAI Studio", version="0.1.0")
    app.mount("/static", StaticFiles(directory=str(_ROOT / "static")), name="static")

    @app.get("/healthz")
    def healthz():
        return {"status": "ok", "engines": enabled_engines()}

    @app.get("/", response_class=HTMLResponse)
    def index(
        request: Request,
        engine: str | None = None,
        status: str | None = None,
        user: str = Depends(_check_auth),
    ):
        # 빈 문자열은 None 취급 (chip 의 'all' 링크와 호환)
        f_engine = (engine or "").strip() or None
        f_status = (status or "").strip() or None
        from lib.kling_pricing import pricing_table_json
        # 새 Veo 모델 자동 감지 → 미등록분 배너 알림 (Vertex 조회는 6h 캐시). veo enable 시만.
        new_veo_models: list[str] = []
        veo_env_hint = ""
        if "veo" in enabled_engines():
            try:
                from adapters.veo import VeoAdapter, detect_new_veo_models
                configured = list(VeoAdapter().available_models)
                new_veo_models = detect_new_veo_models(configured)
                if new_veo_models:
                    veo_env_hint = "VEO_AVAILABLE_MODELS=" + ",".join(configured + new_veo_models)
            except Exception:
                new_veo_models = []
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "engines": enabled_engines(),
                "tabs": engines_by_tab(),
                "engine_tab": ENGINE_TAB,
                "engine_options": all_engine_options(),
                "recent_batches": pg.list_batches(
                    engine=f_engine, status=f_status, limit=10,
                ),
                "filter_engine": f_engine,
                "filter_status": f_status,
                "user": user,
                "kling_pricing": pricing_table_json(),
                "max_files_per_batch": _MAX_FILES_PER_BATCH,
                "new_veo_models": new_veo_models,
                "veo_env_hint": veo_env_hint,
            },
        )

    @app.get("/genai/comfy-nodes", response_class=HTMLResponse)
    def comfy_nodes(
        request: Request,
        workflow_id: str | None = None,
        created: str | None = None,
        user: str = Depends(_check_auth),
    ):
        contracts = _load_comfy_node_contracts()
        if not contracts:
            raise HTTPException(status_code=503, detail="no Comfy node workflow is enabled")
        selected = (workflow_id or next(iter(contracts))).strip()
        if selected not in contracts:
            raise HTTPException(status_code=404, detail="workflow_id is not allowlisted")
        return templates.TemplateResponse(
            request=request,
            name="comfy_nodes.html",
            context={
                "user": user,
                "contracts": contracts,
                "selected_workflow": selected,
                "comfy_enabled": "comfy_local" in enabled_engines(),
                "created_batch_id": (created or "").strip() or None,
                "comfy_native_url": "/genai/comfy-native/",
                "max_bulk_jobs": _MAX_BULK_JOBS,
                "max_files_per_batch": _MAX_FILES_PER_BATCH,
            },
        )

    @app.get("/genai/comfy-native")
    def comfy_native_redirect(_user: str = Depends(_check_auth)):
        return RedirectResponse("/genai/comfy-native/", status_code=307)

    @app.post("/genai/comfy-native/api/prompt")
    @app.post("/api/prompt")
    async def comfy_native_prompt(
        request: Request,
        user: str = Depends(_check_auth),
    ):
        if "comfy_local" not in enabled_engines():
            return JSONResponse(
                {
                    "error": {
                        "type": "comfy_local_disabled",
                        "message": "comfy_local engine is disabled until validated models are installed",
                    },
                    "node_errors": {},
                },
                status_code=503,
            )
        try:
            payload = await request.json()
            workflow_id, values = _match_comfy_native_prompt(payload.get("prompt"))
            source_name = str(values["source_image"])
            source = _fetch_comfy_input(source_name)
            controls = None
            if workflow_id == "sdxl-inpaint-cctv-v1":
                mask_name = str(values["mask_image"])
                mask = _fetch_comfy_input(mask_name)
                _validate_inpaint_pair(source, mask)
                controls = [(PurePosixPath(mask_name).name, mask)]
            limits.check_rate_limit(user)
            limits.check_daily_quota(user, len(source) + sum(len(blob) for _, blob in controls or []))
            client_id = str(payload.get("client_id") or "").strip()
            if client_id and not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", client_id):
                raise ValueError("invalid native Comfy client_id")
            options = {
                "workflow_id": workflow_id,
                "seed": values["seed"],
                "steps": values["steps"],
                "native_client_id": client_id,
            }
            for key in ("negative_prompt", "cfg", "denoise"):
                if key in values:
                    options[key] = values[key]
            result = submit_batch(
                engine="comfy_local",
                prompt=str(values["prompt"]),
                files=[(PurePosixPath(source_name).name, source)],
                requested_by=user,
                options=options,
                control_files=controls,
            )
            batch = pg.get_batch_with_jobs(result["batch_id"]) or {}
            jobs = batch.get("jobs") or []
            job = jobs[0] if jobs else {}
            provider_prompt_id = str(job.get("provider_job_id") or "")
            if not provider_prompt_id:
                detail = str(job.get("error_message") or "local generation was deferred")
                status_code = 409 if job.get("status") == "pending" else 422
                return JSONResponse(
                    {
                        "error": {"type": "genai_submit_not_started", "message": detail},
                        "node_errors": {},
                        "genai_batch_id": result["batch_id"],
                    },
                    status_code=status_code,
                )
            return JSONResponse(
                {
                    "prompt_id": provider_prompt_id,
                    "number": 0,
                    "node_errors": {},
                    "genai_batch_id": result["batch_id"],
                }
            )
        except (ValueError, KeyError, TypeError, requests.RequestException) as exc:
            return JSONResponse(
                {
                    "error": {"type": "restricted_workflow_rejected", "message": str(exc)},
                    "node_errors": {},
                },
                status_code=400,
            )
        except limits.LimitExceeded as exc:
            return JSONResponse(
                {"error": {"type": "genai_limit", "message": str(exc)}, "node_errors": {}},
                status_code=429,
            )

    @app.post("/genai/comfy-native/api/interrupt")
    @app.post("/genai/comfy-native/api/free")
    @app.post("/genai/comfy-native/api/queue")
    @app.post("/api/queue")
    @app.post("/api/free")
    @app.post("/api/interrupt")
    def comfy_native_dangerous_control(_user: str = Depends(_check_auth)):
        return JSONResponse(
            {
                "error": {
                    "type": "managed_by_genai",
                    "message": "queue, interrupt and model release are managed by GenAI Studio",
                }
            },
            status_code=403,
        )

    async def _guarded_comfy_proxy(request: Request, path: str, user: str) -> Response:
        """Single allowlist gate for both catch-alls.

        Anything not named in the allowlist above is refused, so a custom node that
        registers a new route does not silently widen the proxy.
        """
        # Decide on the normalized path, forward the same path with its original case.
        normalized = _normalize_proxy_path(path, casefold=False)
        verdict = _comfy_proxy_verdict(normalized, request.method)
        if verdict == "prompt":
            return await comfy_native_prompt(request, user)
        if verdict == "blocked":
            return comfy_native_dangerous_control(user)
        if verdict == "deny":
            # 조용한 실패 금지 — 어떤 경로가 막혔는지 남겨야 UI 회귀를 추적할 수 있다.
            _LOG.warning(
                "comfy proxy denied: user=%s method=%s raw_path=%r normalized=%r",
                user,
                request.method,
                path,
                normalized,
            )
            return JSONResponse(
                {
                    "error": {
                        "type": "comfy_path_not_allowlisted",
                        "message": f"ComfyUI path '{normalized}' is not on the GenAI proxy allowlist",
                    },
                    "node_errors": {},
                },
                status_code=403,
            )
        # Forward the normalized path so ComfyUI receives exactly what was inspected.
        return await _comfy_http_proxy(request, normalized)

    # Keep this catch-all after the restricted execution/control routes.  The
    # native frontend resolves its API relative to the iframe base path.
    @app.api_route(
        "/genai/comfy-native/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    )
    async def comfy_native_proxy(
        request: Request,
        path: str,
        user: str = Depends(_check_auth),
    ):
        return await _guarded_comfy_proxy(request, path, user)

    @app.api_route(
        "/api/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    )
    async def comfy_api_proxy(
        request: Request,
        path: str,
        user: str = Depends(_check_auth),
    ):
        return await _guarded_comfy_proxy(request, f"api/{path}", user)

    @app.websocket("/genai/comfy-native/ws")
    @app.websocket("/ws")
    async def comfy_websocket_proxy(websocket: WebSocket):
        if not _basic_auth_valid(websocket.headers.get("authorization")):
            await websocket.close(code=4401)
            return
        base = os.getenv("COMFYUI_INTERNAL_URL", "http://comfyui:8188").rstrip("/")
        target = re.sub(r"^http", "ws", base) + "/ws"
        if websocket.url.query:
            target += f"?{websocket.url.query}"
        await websocket.accept()
        try:
            async with websockets.connect(target, max_size=None) as upstream:

                async def client_to_upstream():
                    while True:
                        message = await websocket.receive()
                        if message["type"] == "websocket.disconnect":
                            return
                        if message.get("text") is not None:
                            await upstream.send(message["text"])
                        elif message.get("bytes") is not None:
                            await upstream.send(message["bytes"])

                async def upstream_to_client():
                    async for message in upstream:
                        if isinstance(message, str):
                            await websocket.send_text(message)
                        else:
                            await websocket.send_bytes(message)

                tasks = {
                    asyncio.create_task(client_to_upstream()),
                    asyncio.create_task(upstream_to_client()),
                }
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in pending:
                    task.cancel()
                for task in done:
                    task.result()
        except (OSError, websockets.WebSocketException, WebSocketDisconnect):
            try:
                await websocket.close(code=1013)
            except RuntimeError:
                pass

    @app.post("/genai/batches")
    async def submit(
        request: Request,
        engine: Annotated[str, Form()],
        prompt: Annotated[str, Form()],
        files: Annotated[list[UploadFile] | None, File()] = None,
        mask_file: Annotated[UploadFile | None, File()] = None,
        model_name: Annotated[str | None, Form()] = None,
        mode: Annotated[str | None, Form()] = None,
        duration: Annotated[str | None, Form()] = None,
        aspect_ratio: Annotated[str | None, Form()] = None,
        bulk_group_id: Annotated[str | None, Form()] = None,
        workflow_id: Annotated[str | None, Form()] = None,
        negative_prompt: Annotated[str | None, Form()] = None,
        seed: Annotated[str | None, Form()] = None,
        steps: Annotated[str | None, Form()] = None,
        cfg: Annotated[str | None, Form()] = None,
        denoise: Annotated[str | None, Form()] = None,
        return_to: Annotated[str | None, Form()] = None,
        user: str = Depends(_check_auth),
    ):
        if engine not in enabled_engines():
            raise HTTPException(status_code=400, detail=f"engine {engine!r} not enabled")
        # text-only (Veo txt2video) — files 생략 허용. 그 외 엔진/mode 는 files 필수.
        is_text_only = (mode or "").strip().lower() == "txt2video"
        if is_text_only and engine != "veo":
            raise HTTPException(
                status_code=400,
                detail=f"txt2video 지원 안 함: engine={engine!r} (veo 만 가능)",
            )
        # multipart 에서 빈 파일 슬롯(빈 filename) 만 들어온 경우(브라우저가 input[type=file]
        # 을 비워둔 채 submit) → 실제 빈 리스트 취급.
        files = [f for f in (files or []) if (f.filename or "").strip()]
        if not files and not is_text_only:
            raise HTTPException(status_code=400, detail="files required")
        if len(files) > _MAX_FILES_PER_BATCH:
            raise HTTPException(status_code=413,
                                detail=f"too many files: {len(files)} > {_MAX_FILES_PER_BATCH}")
        # rate-limit 검사 (60s sliding window)
        try:
            limits.check_rate_limit(user)
        except limits.LimitExceeded as exc:
            raise HTTPException(status_code=429, detail=str(exc))
        loaded: list[tuple[str, bytes]] = []
        for f in files:
            ext = Path(f.filename or "").suffix.lower()
            if ext not in _ALLOWED_EXT:
                raise HTTPException(status_code=415,
                                    detail=f"unsupported ext: {ext} (allowed: {sorted(_ALLOWED_EXT)})")
            blob = await f.read()
            if len(blob) > _MAX_BYTES_PER_FILE:
                raise HTTPException(status_code=413,
                                    detail=f"file too large: {len(blob)} > {_MAX_BYTES_PER_FILE}")
            loaded.append((f.filename or "image", blob))
        controls: list[tuple[str, bytes]] = []
        if mask_file is not None and (mask_file.filename or "").strip():
            mask_ext = Path(mask_file.filename or "").suffix.lower()
            if mask_ext != ".png":
                raise HTTPException(status_code=415, detail="inpaint mask must be a PNG")
            mask_blob = await mask_file.read()
            if len(mask_blob) > _MAX_BYTES_PER_FILE:
                raise HTTPException(status_code=413, detail="mask file too large")
            controls.append((mask_file.filename or "mask.png", mask_blob))

        if engine == "comfy_local":
            selected_workflow = (workflow_id or "flux2-klein-4b-edit-v1").strip()
            if len(loaded) != 1:
                raise HTTPException(
                    status_code=400, detail="comfy_local requires exactly one source image per batch"
                )
            if selected_workflow == "sdxl-inpaint-cctv-v1":
                if len(controls) != 1:
                    raise HTTPException(status_code=400, detail="SDXL inpaint requires one mask")
                try:
                    _validate_inpaint_pair(loaded[0][1], controls[0][1])
                except ValueError as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
            elif selected_workflow == "flux2-klein-4b-edit-v1":
                if controls:
                    raise HTTPException(status_code=400, detail="FLUX.2 edit does not accept a mask")
            else:
                raise HTTPException(status_code=400, detail="workflow_id is not allowlisted")
        elif controls:
            raise HTTPException(status_code=400, detail="mask_file is only supported by comfy_local")
        # daily quota 검사 (입력 bytes + 일별 batch 수)
        try:
            limits.check_daily_quota(
                user,
                sum(len(b) for _, b in loaded) + sum(len(b) for _, b in controls),
            )
        except limits.LimitExceeded as exc:
            raise HTTPException(status_code=429, detail=str(exc))
        # 엔진별 옵션 (Kling 의 model_name/mode/duration 등) 통과
        options: dict = {}
        if model_name:
            options["model_name"] = model_name
        if mode:
            options["mode"] = mode
        if duration:
            options["duration"] = duration
        if aspect_ratio:
            options["aspect_ratio"] = aspect_ratio
        if engine == "comfy_local":
            options["workflow_id"] = (workflow_id or "flux2-klein-4b-edit-v1").strip()
            if negative_prompt:
                options["negative_prompt"] = negative_prompt.strip()
            for key, raw_value, caster in (
                ("seed", seed, int),
                ("steps", steps, int),
                ("cfg", cfg, float),
                ("denoise", denoise, float),
            ):
                if raw_value is not None and raw_value.strip() != "":
                    try:
                        options[key] = caster(raw_value)
                    except ValueError as exc:
                        raise HTTPException(status_code=400, detail=f"invalid {key}") from exc
        # bulk submit 묶음 id — CLI bulk-submit 가 N batch 를 1 group 으로 묶을 때
        # options_json 에 그대로 저장 (스키마 변경 X). UI 가 그룹 필터에 활용.
        if bulk_group_id and bulk_group_id.strip():
            bgi = bulk_group_id.strip()
            if not _BULK_GROUP_ID_RE.fullmatch(bgi):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "bulk_group_id 형식 오류: 영숫자 / _ / - / . 1~64자만 허용 "
                        f"(got {bgi!r})"
                    ),
                )
            options["bulk_group_id"] = bgi
        result = submit_batch(
            engine=engine,
            prompt=prompt,
            files=loaded,
            requested_by=user,
            options=options or None,
            control_files=controls or None,
        )
        # API 클라이언트가 명시적으로 JSON 요청한 경우만 JSONResponse.
        # 브라우저 (HTML 폼) 는 첫 화면 그대로 머무르도록 303 Redirect → '/?created=<batch_id>'
        # (toast 표시는 index.html 가 URL query 보고 처리).
        accept = (request.headers.get("accept") or "").lower()
        if "application/json" in accept and "text/html" not in accept:
            return JSONResponse(result)
        target = "/genai/comfy-nodes" if return_to == "/genai/comfy-nodes" else "/"
        return RedirectResponse(url=f"{target}?created={result['batch_id']}", status_code=303)

    @app.get("/genai/batches", response_class=HTMLResponse)
    def list_batches(
        request: Request,
        limit: int = 50,
        engine: str | None = None,
        status: str | None = None,
        bulk_group_id: str | None = None,
        user: str = Depends(_check_auth),
    ):
        # 빈 문자열은 None 취급 (HTML form 의 "all" 옵션 호환)
        engine = (engine or "").strip() or None
        status = (status or "").strip() or None
        bulk_group_id = (bulk_group_id or "").strip() or None
        # POST 와 동일한 regex 로 GET query 도 검증 (path injection / 잘못된 입력 거부)
        if bulk_group_id and not _BULK_GROUP_ID_RE.fullmatch(bulk_group_id):
            raise HTTPException(
                status_code=400,
                detail=(
                    "bulk_group_id 형식 오류: 영숫자 / _ / - / . 1~64자만 허용 "
                    f"(got {bulk_group_id!r})"
                ),
            )
        batches = pg.list_batches(
            status=status, engine=engine,
            bulk_group_id=bulk_group_id, limit=int(limit),
        )
        if _wants_json(request):
            return JSONResponse(jsonable_encoder(batches))
        # UI 용 distinct bulk group id 목록 — 최근 500 rows 에서 추출. chip 위젯은 너무
        # 많아지면 깨지므로 top-20 으로 제한. 현재 active filter 가 top-20 밖이면
        # 맨 앞에 끼워넣어 사용자 시야에서 사라지지 않게 한다.
        all_groups = pg.distinct_bulk_groups(limit_rows=500)
        groups = all_groups[:20]
        if bulk_group_id and bulk_group_id not in groups:
            groups = [bulk_group_id] + groups[:19]
        return templates.TemplateResponse(
            request=request,
            name="batches.html",
            context={
                "batches": batches,
                "user": user,
                "engines": enabled_engines(),
                "filter_engine": engine,
                "filter_status": status,
                "filter_bulk_group_id": bulk_group_id,
                "bulk_groups": groups,
            },
        )

    @app.get("/genai/batches/{batch_id}", response_class=HTMLResponse)
    def batch_detail(batch_id: str, request: Request, user: str = Depends(_check_auth)):
        batch = pg.get_batch_with_jobs(batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if _wants_json(request):
            return JSONResponse(jsonable_encoder(batch))
        # template 에서 promote 버튼 표시 여부 결정용 — options_json 안 flag 확인.
        promoted = False
        opt_text = batch.get("options_json") or ""
        if opt_text:
            try:
                import json as _json
                promoted = bool(_json.loads(opt_text).get("promoted_to_labeling"))
            except Exception:
                promoted = False
        # 출력 파일명은 입력 이미지명 기반 (finalize 규칙) — seq→실제 파일명 맵을 넘겨
        # template 이 하드코딩 001.mp4 대신 정확한 링크를 만들도록. legacy 배치도 정확.
        from jobs.finalize import _batch_outputs_dir, resolve_output_filenames
        output_ext = ".mp4" if batch.get("output_media") == "video" else ".png"
        seqs = [int(j.get("seq_in_batch") or 0) for j in (batch.get("jobs") or [])]
        try:
            output_names = resolve_output_filenames(
                _batch_outputs_dir(batch_id), output_ext, seqs
            )
        except Exception:
            output_names = {}
        return templates.TemplateResponse(
            request=request,
            name="batch_detail.html",
            context={
                "batch": batch,
                "user": user,
                "promoted": promoted,
                "output_names": output_names,
            },
        )

    @app.get("/genai/batches/{batch_id}/promote", response_class=HTMLResponse)
    def promote_form(batch_id: str, request: Request, user: str = Depends(_check_auth)):
        """promote-to-labeling 의 입력 폼. labeling_method/label_policy/categories/classes
        를 묶어서 POST /promote-to-labeling 으로 보낸다."""
        batch = pg.get_batch_with_jobs(batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="batch not found")
        from jobs.promote import PROMOTABLE_STATUSES
        status = (batch.get("status") or "").strip().lower()
        if status not in PROMOTABLE_STATUSES:
            raise HTTPException(
                status_code=400,
                detail=f"batch.status={status!r} not promotable",
            )
        # 이미 promote 됐는지 표기
        promoted = False
        opt_text = batch.get("options_json") or ""
        if opt_text:
            try:
                import json as _json
                promoted = bool(_json.loads(opt_text).get("promoted_to_labeling"))
            except Exception:
                promoted = False
        return templates.TemplateResponse(
            request=request,
            name="promote.html",
            context={"batch": batch, "user": user, "promoted": promoted},
        )

    @app.post("/genai/batches/{batch_id}/promote-to-labeling")
    def promote_to_labeling(
        batch_id: str,
        request: Request,
        labeling_method: Annotated[list[str], Form()],
        label_policy: Annotated[str, Form()] = "required",
        categories_text: Annotated[str, Form()] = "",
        classes_text: Annotated[str, Form()] = "",
        image_profile: Annotated[str, Form()] = "current",
        force: Annotated[bool, Form()] = False,
        # 2026-06-02 hybrid preset: { "falldown": "a person falling...", ... } JSON.
        # preset 버튼이 hidden field 에 자동 채움. 사용자 직접 입력 안 함. dispatch
        # service 가 파싱 + validate. 빈 string / 누락 시 기존 동작 (Gemini 자유 카테고리).
        gemini_descriptions_json: Annotated[str, Form()] = "",
        user: str = Depends(_check_auth),
    ):
        """form-encoded multi-select 폼을 받아 promote 실행.
        - labeling_method: 다중 체크박스 (timestamp_video, bbox 등)
        - label_policy: required|none
        - categories_text/classes_text: comma 또는 newline 구분 텍스트 → list
        - gemini_descriptions_json: hybrid preset 의 dict {category: description}
        """
        from jobs.promote import (
            PromoteConflictError,
            PromoteValidationError,
            promote_batch_to_labeling,
            repromote_batch_to_labeling,
        )

        def _split_list(s: str) -> list[str]:
            # 콤마/줄바꿈 둘 다 허용. 빈 항목 제거.
            raw = (s or "").replace(",", "\n")
            return [x.strip() for x in raw.split("\n") if x.strip()]

        labeling_clean = [m.strip() for m in (labeling_method or []) if m.strip()]
        categories = _split_list(categories_text)
        classes = _split_list(classes_text)
        # gemini_descriptions_json: form 에서 빈 string 으로 올 수 있음 — 그대로 전달
        # 하면 service.prepare_dispatch_request 가 None 으로 정규화.
        gemini_desc_raw = (gemini_descriptions_json or "").strip() or None

        try:
            promote_func = repromote_batch_to_labeling if force else promote_batch_to_labeling
            result = promote_func(
                batch_id,
                labeling_method=labeling_clean,
                label_policy=label_policy.strip(),
                categories=categories,
                classes=classes,
                image_profile=(image_profile or "current").strip(),
                gemini_descriptions_json=gemini_desc_raw,
                requested_by=user,
            )
        except PromoteValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except PromoteConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        except Exception as exc:
            # Codex MEDIUM-3: options_json corruption / NAS 일시 장애 등은 명확한 500.
            # FastAPI default 500 보다 detail 명시하면 운영자 디버깅이 빠르다.
            # PG DataError (invalid jsonb cast) / psycopg2 lock timeout / NAS PermissionError
            # 모두 여기로 떨어진다.
            raise HTTPException(
                status_code=500,
                detail=f"promote 실행 실패 ({type(exc).__name__}): {exc}",
            )

        # API 클라이언트는 JSON, 브라우저는 batch 상세로 redirect (toast 는 template 측).
        if _wants_json(request):
            return JSONResponse(result)
        return RedirectResponse(
            url=f"/genai/batches/{batch_id}?promoted=1",
            status_code=303,
        )

    @app.get("/genai/batches/{batch_id}/outputs/{filename}")
    def batch_output_file(
        batch_id: str,
        filename: str,
        request: Request,
        download: bool = False,
        user: str = Depends(_check_auth),
    ):
        """완료된 batch 의 결과 파일(.mp4 / .png) 를 NAS 에서 스트리밍.

        경로 후보 (최신 → 구버전 fallback):
          1. <NAS>/<YYYY-MM-DD>/<batch>/outputs/<file>  (현재 단순 구조)
          2. <NAS>/incoming/genai/<YYYY-MM-DD>/<batch>/outputs/<file>  (legacy A)
          3. <NAS>/genai/<batch>/outputs/<file>  (legacy B, 날짜폴더 이전)
        path traversal 방지: filename 에 / 또는 .. 포함 시 거부.
        """
        if "/" in filename or ".." in filename or filename.startswith("."):
            raise HTTPException(status_code=400, detail="invalid filename")
        batch = pg.get_batch_with_jobs(batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="batch not found")
        ts = batch.get("submitted_at")
        from datetime import datetime as _dt
        if not isinstance(ts, _dt):
            ts = _dt.now()
        date_dir = ts.strftime("%Y-%m-%d")
        nas_root = os.getenv("GENAI_NAS_INCOMING", "/nas/data/genai_studio").rstrip("/")
        candidates = [
            f"{nas_root}/{date_dir}/{batch_id}/outputs/{filename}",
            f"{nas_root}/incoming/genai/{date_dir}/{batch_id}/outputs/{filename}",
            f"{nas_root}/genai/{batch_id}/outputs/{filename}",
        ]
        for path in candidates:
            if os.path.exists(path) and os.path.isfile(path):
                media = "video/mp4" if filename.endswith(".mp4") else (
                    "image/png" if filename.endswith(".png") else "application/octet-stream"
                )
                headers = {}
                if download:
                    # 파일명이 한글/#/공백 포함 가능 → RFC 5987 filename* (UTF-8 pct-encode)
                    # + ASCII fallback. 다운로드 이름 = 출력 파일명(=입력 이미지명) 그대로.
                    from urllib.parse import quote
                    ascii_fallback = filename.encode("ascii", "ignore").decode() or "download"
                    headers["Content-Disposition"] = (
                        f'attachment; filename="{ascii_fallback}"; '
                        f"filename*=UTF-8''{quote(filename)}"
                    )
                return FileResponse(path, media_type=media, headers=headers)
        raise HTTPException(
            status_code=404,
            detail=f"output not found in incoming or archive: {filename}",
        )

    @app.get("/genai/limits")
    def get_limits(user: str = Depends(_check_auth)):
        """현재 설정된 한도 + 호출 사용자의 24h 사용량 + 남은 headroom.

        CLI bulk-submit 가 제출 전 사전 체크용으로 호출. 응답은 항상 JSON
        (Accept 헤더 무시) — UI 노출 페이지 없음.

        구조:
          {
            "limits": {...env 한도...},
            "usage": {
               "rate_limit": {per_min, used_60s, remaining},
               "daily_batches": {limit, used_24h, remaining},
               "daily_bytes": {limit, used_24h, remaining}
            },
            "per_batch": {
               "max_files": int, "max_bytes_per_file": int
            }
          }
        """
        from lib.kling_pricing import pricing_table_json
        return JSONResponse({
            "limits": limits.status(),
            "usage": limits.usage(user),
            "per_batch": {
                "max_files": _MAX_FILES_PER_BATCH,
                "max_bytes_per_file": _MAX_BYTES_PER_FILE,
            },
            "pricing": {
                "kling": pricing_table_json(),
            },
        })

    # ----- UI bulk-submit -------------------------------------------------
    @app.get("/genai/bulk", response_class=HTMLResponse)
    def bulk_form(request: Request, user: str = Depends(_check_auth)):
        """대량 (이미지 × 프롬프트) 제출 폼. 클라이언트 JS 가 plan/cost preview."""
        from lib.kling_pricing import pricing_table_json
        requested_engine = (request.query_params.get("engine") or "").strip()
        bulk_engines = enabled_engines()
        selected_engine = requested_engine if requested_engine in bulk_engines else (
            bulk_engines[0] if bulk_engines else None
        )
        requested_pair_mode = (request.query_params.get("pair_mode") or "").strip()
        selected_pair_mode = (
            requested_pair_mode
            if requested_pair_mode in {"paired", "cartesian"}
            else "cartesian" if selected_engine == "comfy_local" else "paired"
        )
        return templates.TemplateResponse(
            request=request,
            name="bulk.html",
            context={
                "engines": bulk_engines,
                "selected_engine": selected_engine,
                "selected_pair_mode": selected_pair_mode,
                "engine_options": all_engine_options(),
                "engine_tab": ENGINE_TAB,
                "max_bulk_jobs": _MAX_BULK_JOBS,
                "max_files_per_batch": _MAX_FILES_PER_BATCH,
                "user": user,
                "kling_pricing": pricing_table_json(),
            },
        )

    @app.post("/genai/bulk-batches")
    async def bulk_submit(
        request: Request,
        engine: Annotated[str, Form()],
        prompts_text: Annotated[str, Form()],
        idempotency_token: Annotated[str, Form()],
        pair_mode: Annotated[str, Form()] = "paired",
        text_only: Annotated[str, Form()] = "false",
        bulk_group_id: Annotated[str | None, Form()] = None,
        files: Annotated[list[UploadFile] | None, File()] = None,
        mask_files: Annotated[list[UploadFile] | None, File()] = None,
        model_name: Annotated[str | None, Form()] = None,
        mode: Annotated[str | None, Form()] = None,
        duration: Annotated[str | None, Form()] = None,
        aspect_ratio: Annotated[str | None, Form()] = None,
        workflow_id: Annotated[str | None, Form()] = None,
        negative_prompt: Annotated[str | None, Form()] = None,
        seed: Annotated[str | None, Form()] = None,
        steps: Annotated[str | None, Form()] = None,
        cfg: Annotated[str | None, Form()] = None,
        denoise: Annotated[str | None, Form()] = None,
        prompt_graph: Annotated[str | None, Form()] = None,
        user: str = Depends(_check_auth),
    ):
        import asyncio as _asyncio
        import time as _time
        import uuid as _uuid

        # 1) Idempotency 체크 — 같은 token 이 30분 안에 또 들어오면 첫 응답 그대로 반환.
        # 브라우저 새로고침 / double-click 으로 인한 중복 제출 차단.
        if not idempotency_token or len(idempotency_token) < 8 or len(idempotency_token) > 128:
            raise HTTPException(status_code=400, detail="idempotency_token 형식 오류")
        cache_key = f"{user}:{idempotency_token}"
        with _idempotency_lock:
            # TTL purge
            now = _time.time()
            expired = [k for k, (ts, _) in _idempotency_cache.items() if now - ts > _IDEMPOTENCY_TTL]
            for k in expired:
                _idempotency_cache.pop(k, None)
            cached = _idempotency_cache.get(cache_key)
            if cached:
                _, prev_result = cached
                return JSONResponse({
                    "duplicate": True,
                    "message": "동일 idempotency_token 의 이전 요청 결과 재반환.",
                    "result": prev_result,
                })

        # 2) 기본 검증
        if engine not in enabled_engines():
            raise HTTPException(status_code=400, detail=f"engine {engine!r} not enabled")
        is_text_only = (text_only or "").strip().lower() in ("true", "1", "yes")
        if is_text_only and engine != "veo":
            raise HTTPException(status_code=400,
                                detail=f"text_only 는 veo 전용 (got engine={engine!r})")
        if pair_mode not in ("paired", "cartesian"):
            raise HTTPException(status_code=400,
                                detail=f"pair_mode 는 paired|cartesian (got {pair_mode!r})")
        if prompt_graph:
            if engine != "comfy_local":
                raise HTTPException(
                    status_code=400,
                    detail="prompt_graph is only supported by comfy_local",
                )
            if len(prompt_graph) > 1_000_000:
                raise HTTPException(status_code=413, detail="prompt_graph is too large")
            try:
                graph_workflow_id, graph_values = _match_comfy_native_prompt(
                    json.loads(prompt_graph)
                )
            except (json.JSONDecodeError, ValueError, KeyError, TypeError) as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"approved Comfy graph required: {exc}",
                ) from exc
            if workflow_id and workflow_id.strip() != graph_workflow_id:
                raise HTTPException(
                    status_code=400,
                    detail="workflow_id does not match prompt_graph",
                )
            workflow_id = graph_workflow_id
            prompts_text = str(graph_values["prompt"])
            negative_prompt = (
                str(graph_values["negative_prompt"])
                if "negative_prompt" in graph_values
                else None
            )
            seed = str(graph_values["seed"])
            steps = str(graph_values["steps"])
            cfg = str(graph_values["cfg"]) if "cfg" in graph_values else None
            denoise = str(graph_values["denoise"]) if "denoise" in graph_values else None
        if bulk_group_id:
            bgi = bulk_group_id.strip()
            if bgi and not _BULK_GROUP_ID_RE.fullmatch(bgi):
                raise HTTPException(status_code=400,
                                    detail="bulk_group_id 형식 오류 (영숫자/_/-/. 1~64자)")
        else:
            bgi = None

        prompts = [p.strip() for p in (prompts_text or "").splitlines() if p.strip()]
        if not prompts:
            raise HTTPException(status_code=400, detail="prompts_text 비어있음")

        # 3) 파일 검증 (text-only 면 skip)
        files = [f for f in (files or []) if (f.filename or "").strip()]
        loaded: list[tuple[str, bytes]] = []
        if not is_text_only:
            if not files:
                raise HTTPException(status_code=400,
                                    detail="이미지 없음 (text-only 가 아니면 필수)")
            for f in files:
                ext = Path(f.filename or "").suffix.lower()
                if ext not in _ALLOWED_EXT:
                    raise HTTPException(status_code=415,
                                        detail=f"unsupported ext {ext} ({f.filename})")
                blob = await f.read()
                if len(blob) > _MAX_BYTES_PER_FILE:
                    raise HTTPException(status_code=413,
                                        detail=f"파일 너무 큼 ({f.filename}, {len(blob)} > {_MAX_BYTES_PER_FILE})")
                loaded.append((f.filename or "image", blob))

        mask_files = [f for f in (mask_files or []) if (f.filename or "").strip()]
        controls: list[tuple[str, bytes]] = []
        for f in mask_files:
            ext = Path(f.filename or "").suffix.lower()
            if ext != ".png":
                raise HTTPException(status_code=415, detail=f"inpaint mask must be PNG ({f.filename})")
            blob = await f.read()
            if len(blob) > _MAX_BYTES_PER_FILE:
                raise HTTPException(status_code=413, detail=f"mask file too large ({f.filename})")
            controls.append((f.filename or "mask.png", blob))

        selected_workflow: str | None = None
        if engine == "comfy_local":
            selected_workflow = (workflow_id or "flux2-klein-4b-edit-v1").strip()
            configured_workflows = {
                item.strip()
                for item in os.getenv(
                    "COMFYUI_ALLOWED_WORKFLOWS", ",".join(_COMFY_NODE_WORKFLOWS)
                ).split(",")
                if item.strip()
            }
            if (
                selected_workflow not in _COMFY_NODE_WORKFLOWS
                or selected_workflow not in configured_workflows
            ):
                raise HTTPException(status_code=400, detail="workflow_id is not allowlisted")
            if selected_workflow == "sdxl-inpaint-cctv-v1":
                if len(controls) != len(loaded):
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "SDXL inpaint requires one mask per source image in the same order "
                            f"(images={len(loaded)}, masks={len(controls)})"
                        ),
                    )
                for index, (source_item, mask_item) in enumerate(zip(loaded, controls), start=1):
                    try:
                        _validate_inpaint_pair(source_item[1], mask_item[1])
                    except ValueError as exc:
                        raise HTTPException(
                            status_code=400,
                            detail=f"source/mask pair {index} invalid: {exc}",
                        ) from exc
            elif controls:
                raise HTTPException(status_code=400, detail="FLUX.2 edit does not accept masks")
        elif controls:
            raise HTTPException(status_code=400, detail="mask_files are only supported by comfy_local")

        # Comfy bulk convenience: one prompt is a broadcast instruction for every
        # selected source image.  Normalize this server-side as well as in the UI so
        # stale tabs/API clients that still submit pair_mode=paired do not fail M×1.
        if (
            engine == "comfy_local"
            and not is_text_only
            and len(prompts) == 1
            and len(loaded) > 1
        ):
            pair_mode = "cartesian"

        # 4) plan 구성
        plan: list[dict] = []
        if is_text_only:
            plan = [{"prompt": p, "files": [], "controls": []} for p in prompts]
        elif pair_mode == "paired":
            if len(loaded) != len(prompts):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"paired 모드는 N==M 필요 "
                        f"(images={len(loaded)}, prompts={len(prompts)}). "
                        "cartesian 모드로 변경하거나 개수를 맞춰주세요."
                    ),
                )
            for index, (p, item) in enumerate(zip(prompts, loaded)):
                pair_controls = [controls[index]] if controls else []
                plan.append({"prompt": p, "files": [item], "controls": pair_controls})
        else:  # cartesian: 같은 prompt 마다 _MAX_FILES_PER_BATCH 단위 chunk
            for p in prompts:
                for i in range(0, len(loaded), _MAX_FILES_PER_BATCH):
                    plan.append({
                        "prompt": p,
                        "files": loaded[i:i + _MAX_FILES_PER_BATCH],
                        "controls": controls[i:i + _MAX_FILES_PER_BATCH] if controls else [],
                    })

        # 5) hard cap — 총 jobs 수
        total_jobs = sum(max(1, len(b["files"])) for b in plan)
        if total_jobs > _MAX_BULK_JOBS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"bulk 한도 초과: total_jobs={total_jobs} > GENAI_MAX_BULK_JOBS={_MAX_BULK_JOBS}. "
                    "CLI bulk-submit 사용 또는 GENAI_MAX_BULK_JOBS env 상향 검토."
                ),
            )

        # 6) limits 사전 체크 (server-side 정직성 — preview/confirm 분리 안 하므로 1회 검사)
        n_batches = len(plan)
        usage = limits.usage(user)
        rem_b = usage.get("daily_batches", {}).get("remaining")
        rem_bytes = usage.get("daily_bytes", {}).get("remaining")
        # Cartesian mode reuses the same upload for every prompt, but every generated
        # batch persists its own quarantined copy. Account for those actual bytes.
        total_input_bytes = sum(
            len(blob)
            for batch in plan
            for _filename, blob in batch["files"] + batch["controls"]
        )
        if rem_b is not None and n_batches > rem_b:
            raise HTTPException(status_code=429,
                                detail=f"일별 배치 한도 초과: 제출 {n_batches} > 잔여 {rem_b}")
        if rem_bytes is not None and total_input_bytes > rem_bytes:
            raise HTTPException(status_code=429,
                                detail=f"일별 bytes 한도 초과: 제출 {total_input_bytes:,} > 잔여 {rem_bytes:,}")

        # 7) options
        bgi = bgi or f"bgi-{_uuid.uuid4().hex[:12]}"
        common_opts: dict[str, object] = {}
        if model_name:
            common_opts["model_name"] = model_name
        if mode:
            common_opts["mode"] = mode
        if duration:
            common_opts["duration"] = duration
        if aspect_ratio:
            common_opts["aspect_ratio"] = aspect_ratio
        if is_text_only:
            common_opts.setdefault("mode", "txt2video")
        if engine == "comfy_local":
            common_opts["workflow_id"] = selected_workflow or "flux2-klein-4b-edit-v1"
            if negative_prompt:
                common_opts["negative_prompt"] = negative_prompt.strip()
            for key, raw_value, caster in (
                ("seed", seed, int),
                ("steps", steps, int),
                ("cfg", cfg, float),
                ("denoise", denoise, float),
            ):
                if raw_value is not None and raw_value.strip() != "":
                    try:
                        common_opts[key] = caster(raw_value)
                    except ValueError as exc:
                        raise HTTPException(status_code=400, detail=f"invalid {key}") from exc
            parsed_seed = int(common_opts.get("seed", 0))
            is_flux_workflow = bool(selected_workflow and selected_workflow.startswith("flux2-"))
            parsed_steps = int(common_opts.get("steps", 4 if is_flux_workflow else 30))
            parsed_cfg = float(common_opts.get("cfg", 6.0))
            parsed_denoise = float(common_opts.get("denoise", 0.85))
            if parsed_seed < 0 or parsed_seed >= 2**63:
                raise HTTPException(status_code=400, detail="seed must be in [0, 2^63)")
            if is_flux_workflow and parsed_steps != 4:
                raise HTTPException(status_code=400, detail="FLUX.2 Klein workflow requires steps=4")
            if not 1 <= parsed_steps <= 60:
                raise HTTPException(status_code=400, detail="steps must be in [1, 60]")
            if not 0 <= parsed_cfg <= 20:
                raise HTTPException(status_code=400, detail="cfg must be in [0, 20]")
            if not 0 < parsed_denoise <= 1:
                raise HTTPException(status_code=400, detail="denoise must be in (0, 1]")
        common_opts["bulk_group_id"] = bgi

        # 8) 순차 submit (sleep with throttle)
        submitted: list[dict] = []
        failed: list[dict] = []
        for i, b in enumerate(plan, 1):
            try:
                result = submit_batch(
                    engine=engine, prompt=b["prompt"],
                    files=b["files"], requested_by=user,
                    options=dict(common_opts),
                    control_files=b["controls"] or None,
                )
                submitted.append({
                    "i": i,
                    "batch_id": result.get("batch_id"),
                    "n_images": len(b["files"]),
                    "n_controls": len(b["controls"]),
                })
            except Exception as exc:
                failed.append({"i": i, "error": str(exc), "type": type(exc).__name__})
            # async sleep — blocking time.sleep 사용 시 uvicorn event loop 자체가 정지 →
            # 다른 요청 처리 불가. asyncio.sleep 으로 양보.
            if i < len(plan) and _BULK_SLEEP_SECONDS > 0:
                await _asyncio.sleep(_BULK_SLEEP_SECONDS)

        result_payload = {
            "bulk_group_id": bgi,
            "engine": engine,
            "workflow_id": selected_workflow,
            "pair_mode": pair_mode,
            "total_planned": len(plan),
            "total_jobs": total_jobs,
            "submitted": submitted,
            "failed": failed,
        }

        # 9) idempotency cache 저장 (응답 직전, 결과 포함)
        with _idempotency_lock:
            _idempotency_cache[cache_key] = (_time.time(), result_payload)

        # 10) 응답 — JSON 요청이면 JSON, 브라우저 폼이면 결과 페이지 redirect (group 필터)
        accept = (request.headers.get("accept") or "").lower()
        if "application/json" in accept and "text/html" not in accept:
            return JSONResponse(result_payload)
        return RedirectResponse(
            url=f"/genai/batches?bulk_group_id={bgi}&created_bulk={len(submitted)}",
            status_code=303,
        )

    @app.get("/genai/costs", response_class=HTMLResponse)
    def costs(request: Request, user: str = Depends(_check_auth)):
        summary = cost_summary()
        # Kling 계정 리소스팩(요금제) 라이브 조회 — best-effort. 실패해도 탭 나머지는 렌더.
        packs_panel = {"packs": [], "alerts": [], "error": None}
        try:
            import time as _time
            from adapters.kling import (
                fetch_kling_resource_packs, order_packs_for_display,
                summarize_resource_packs, resource_pack_totals)
            all_packs = fetch_kling_resource_packs()
            # ponytail: 팩 누적은 계정당 수십 개 수준이라 상한 없음. 수백 개가 되면 최근 N개로 자를 것.
            packs_panel = summarize_resource_packs(order_packs_for_display(all_packs), _time.time())
            packs_panel["totals"] = resource_pack_totals(all_packs)
            packs_panel["error"] = None
        except Exception as exc:
            packs_panel = {"packs": [], "alerts": [], "error": str(exc)}
        if _wants_json(request):
            return JSONResponse(jsonable_encoder(
                {"summary": summary, "limits": limits.status(), "resource_packs": packs_panel}))
        return templates.TemplateResponse(
            request=request,
            name="costs.html",
            context={
                "summary": summary,
                "packs_panel": packs_panel,
                "user": user,
            },
        )

    @app.post("/genai/jobs/{job_id}/retry")
    def retry_job(job_id: str, user: str = Depends(_check_auth)):
        """job 1건을 재제출. 두 용도 겸용 (동일 기계로직 — 시작 status 만 다름):
          - retry  : status='failed' job 을 복구 재시도 (실패 회복)
          - re-roll : status='done' job 을 폐기 후 재생성 (결과물 불만족 → 새 영상).
            Kling 은 비결정적이라 같은 prompt/원본이라도 다른 결과. finalize 가 seq 별
            결정적 파일명으로 덮어쓰므로 기존 출력은 in-place 교체됨.
        input_asset_id 와 prompt 는 보존, 새 provider_job_id 발급.

        Codex Q3 HIGH: atomic CAS — status(failed|done) → 'submitted' RETURNING. RETURNING
        이 빈 row 면 다른 retry/reroll 가 이미 채갔거나 in-flight → 409. 외부 API 중복 호출 방지.
        """
        with pg.connect() as conn:
            with conn.cursor() as cur:
                # 1) atomic transition (failed=재시도 / done=재생성 만 통과 — in-flight 중복 차단)
                #    cost_units 도 NULL 로 — done→reroll 시 in-flight 동안 stale 비용 표기 방지,
                #    finalize 가 새 cost 로 다시 채움. failed 는 원래 NULL 이라 무영향.
                cur.execute(
                    """
                    UPDATE genai_jobs
                       SET status = 'submitted',
                           error_message = NULL,
                           provider_job_id = NULL,
                           cost_units = NULL,
                           submitted_at = CURRENT_TIMESTAMP,
                           completed_at = NULL
                     WHERE job_id = %s AND status IN ('failed', 'done')
                    RETURNING batch_id, seq_in_batch
                    """,
                    (job_id,),
                )
                cas = cur.fetchone()
                if cas is None:
                    raise HTTPException(
                        status_code=409,
                        detail="job is not in 'failed'/'done' state (retry/reroll race or already in-flight)",
                    )
                # 2) 메타 조회 (engine/prompt/options_json 은 batches 에)
                cur.execute(
                    """
                    SELECT b.engine, b.prompt, b.options_json
                      FROM genai_jobs j
                      JOIN genai_batches b ON b.batch_id = j.batch_id
                     WHERE j.job_id = %s
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
        batch_id, seq = cas
        # batch.status 'running' 복귀는 recompute 경유 — raw UPDATE 는 advisory lock
        # 미참여라 동시 recompute 의 stale terminal 쓰기와 경합 (Codex round-2 HIGH)
        pg.recompute_batch_status(batch_id)
        engine, prompt, options_json_raw = row

        # options_json 에서 mode 회수 — txt2video 면 NAS 원본 읽기 skip
        import json as _json
        try:
            opts_retry = _json.loads(options_json_raw) if options_json_raw else {}
        except Exception:
            opts_retry = {}
        is_text_only_retry = (opts_retry.get("mode") or "").strip().lower() == "txt2video"

        from adapters import AdapterDeferredError, KlingTransientError, get_adapter
        adapter = get_adapter(engine)

        if is_text_only_retry:
            blob = b""
            orig_name = ""
        else:
            # 원본 input image 를 NAS 에서 다시 읽어 어댑터 submit (공유 헬퍼)
            blob, orig_name = _load_nas_original_blob(batch_id, int(seq))
            if blob is None:
                # CAS 한 status 를 'failed' 로 되돌림 (원본 없으면 retry 자체 불가).
                # raw UPDATE 아닌 guarded helper — 동시 finalize 가 먼저 'done' 찍었으면 no-op.
                pg.update_job_status(job_id, status="failed",
                                     error_message="original file gone (archive moved?)")
                pg.recompute_batch_status(batch_id)  # batch 를 'running' 으로 둔 채 이탈 방지
                raise HTTPException(status_code=410, detail=f"original file gone: {orig_name}")

        opts_retry["_job_id"] = job_id
        if engine == "comfy_local" and opts_retry.get("workflow_id") == "sdxl-inpaint-cctv-v1":
            mask_blob, mask_name = _load_nas_control_blob(batch_id, int(seq))
            if mask_blob is None:
                pg.update_job_status(job_id, status="failed", error_message="control mask gone")
                pg.recompute_batch_status(batch_id)
                raise HTTPException(status_code=410, detail=f"control mask gone: {mask_name}")
            opts_retry["_mask_bytes"] = mask_blob
            opts_retry["_mask_filename"] = mask_name

        try:
            sub = adapter.submit(blob, orig_name, prompt, options=opts_retry or None)
        except KlingTransientError as exc:
            # 1303 등 동시 한도 — failed 아님. deferred(pending) 로 두고 sensor drain
            # 이 슬롯 빌 때 재제출. batch 는 running 유지.
            pg.mark_job_deferred(job_id, str(exc))
            pg.recompute_batch_status(batch_id)
            return {"job_id": job_id, "status": "pending", "action": "deferred",
                    "detail": str(exc)}
        except AdapterDeferredError as exc:
            pg.mark_job_deferred(job_id, str(exc))
            pg.recompute_batch_status(batch_id)
            return {
                "job_id": job_id,
                "status": "pending",
                "action": "deferred",
                "detail": str(exc),
            }
        except Exception as exc:
            # 외부 API 실패 시 status='failed' 로 되돌림
            pg.update_job_status(job_id, status="failed",
                                  error_message=f"retry adapter submit failed: {exc}")
            pg.recompute_batch_status(batch_id)  # batch 를 'running' 으로 둔 채 이탈 방지
            raise HTTPException(status_code=502, detail=f"adapter submit failed: {exc}")
        pg.update_job_submitted(job_id, sub.provider_job_id)
        # 동기 엔진은 submit 시점에 결과 → finalize_sync_results 1건으로 호출
        if sub.is_synchronous and sub.immediate_result is not None:
            from jobs.finalize import finalize_sync_results
            finalize_sync_results(
                batch_id=batch_id,
                engine=engine,
                output_media=adapter.output_media,
                results=[{
                    "job_id": job_id,
                    "seq": int(seq),
                    "bytes": sub.immediate_result,
                    "ext": sub.immediate_ext or adapter.output_ext,
                    "cost_units": sub.cost_units,
                }],
            )
            return {"job_id": job_id, "status": "done", "action": "sync_finalized"}
        return {"job_id": job_id, "status": "submitted", "provider_job_id": sub.provider_job_id}

    @app.post("/genai/batches/{batch_id}/cancel")
    def cancel_batch_route(batch_id: str, user: str = Depends(_check_auth)):
        """미완료(pending/submitted/running) job 을 일괄 취소. pending 정지 = drain 이 더 이상
        제출 안 함(과금 차단). 이미 submitted 된 job 은 Kling 에 provider-cancel API 가 없어
        DB 상에서만 abandon (계속 생성될 수 있음)."""
        # A local Comfy job owns GPU0 until its prompt is interrupted and resources are
        # released. Provider APIs without cancellation keep the existing DB-only behavior.
        with pg.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT b.engine, j.provider_job_id
                      FROM genai_jobs j JOIN genai_batches b ON b.batch_id=j.batch_id
                     WHERE j.batch_id=%s AND j.status IN ('submitted','running')
                    """,
                    (batch_id,),
                )
                active_jobs = cur.fetchall()
        for engine, provider_job_id in active_jobs:
            if engine == "comfy_local" and provider_job_id:
                try:
                    from adapters import get_adapter

                    get_adapter(engine).cancel(provider_job_id)
                except Exception:
                    # Cancellation remains fail-forward: DB terminal state is authoritative,
                    # while the lease TTL is the crash-safe cleanup path.
                    pass
        n = pg.cancel_batch(batch_id, f"cancelled by {user} (재작업)")
        return {"batch_id": batch_id, "cancelled_jobs": n}

    # ----- Internal API (Dagster polling sensor 전용) -----------------
    # Phase 3.5: Dagster 이미지에 어댑터 코드를 COPY 하지 않고, HTTP 경유로
    # poll/finalize 를 위임. 인증은 GENAI_INTERNAL_TOKEN env 로 분리 (basic auth 와 별개).
    def _check_internal(request: Request) -> None:
        expected = os.getenv("GENAI_INTERNAL_TOKEN", "").strip()
        if not expected:
            raise HTTPException(
                status_code=503,
                detail="GENAI_INTERNAL_TOKEN 미설정 — internal API 비활성",
            )
        provided = request.headers.get("X-Internal-Token", "")
        if not secrets.compare_digest(provided, expected):
            raise HTTPException(status_code=401, detail="invalid internal token")

    def _async_engines() -> list[str]:
        """is_synchronous=False 인 엔진만 (sync 는 submit 에서 finalize 됨)."""
        from adapters import _ADAPTERS  # type: ignore[attr-defined]
        out: list[str] = []
        for name, cls in _ADAPTERS.items():
            try:
                if not getattr(cls, "is_synchronous", True):
                    out.append(name)
            except Exception:
                pass
        return out

    def _do_finalize(batch_id: str, seq: int, engine: str, result_url: str,
                     output_ext: str, cost_units: float | None) -> None:
        """Background task — sensor timeout 보다 긴 download 도 안전하게 처리."""
        from jobs.finalize import fail_job, finalize_job
        try:
            finalize_job(
                batch_id=batch_id,
                seq_in_batch=int(seq),
                engine=engine,
                result_url=result_url,
                output_ext=output_ext,
                cost_units=cost_units,
            )
        except Exception as exc:
            fail_job(batch_id, int(seq), f"finalize_failed: {exc}")

    @app.post("/internal/jobs/{job_id}/poll")
    def internal_poll_job(
        job_id: str,
        request: Request,
        background: BackgroundTasks,
    ):
        """Dagster sensor 가 호출. 1 job 의 외부 API 상태 확인.

        'done' 일 때 finalize(NAS download 포함) 는 BackgroundTasks 로 분리.
        sensor 의 짧은 timeout 안에 finalize 를 끝낼 수 없을 때 sensor 가 timeout
        retry 하면서 동일 job 을 재호출 → 중복 finalize 위험. 이를 방지하기 위해
        finalize 시작 직전에 status='running' (또는 unchanged) 인 row 만 진입하고,
        atomic 으로 status='running' (이미 그러함) 유지 + BackgroundTasks 로 응답 즉시 반환.
        """
        _check_internal(request)
        with pg.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT j.batch_id, j.seq_in_batch, j.provider_job_id, j.status,
                           b.engine, b.output_media
                      FROM genai_jobs j
                      JOIN genai_batches b ON b.batch_id = j.batch_id
                     WHERE j.job_id = %s
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="job not found")
        batch_id, seq, provider_id, status, engine, output_media = row
        if status not in ("submitted", "running"):
            return {"job_id": job_id, "status": status, "action": "noop"}
        if not provider_id:
            return {"job_id": job_id, "status": status, "action": "no_provider_id"}

        from adapters import get_adapter
        from jobs.finalize import fail_job

        try:
            adapter = get_adapter(engine)
            res = adapter.poll(provider_id)
        except Exception as exc:
            return {"job_id": job_id, "status": "error", "error": str(exc)}

        if res.status == "running":
            return {"job_id": job_id, "status": "running", "action": "wait"}
        if res.status == "failed":
            fail_job(batch_id, int(seq), res.error_message or "provider failed")
            return {"job_id": job_id, "status": "failed", "action": "fail_recorded"}
        if res.status == "done":
            # 중복 finalize 방지 — sensor 가 60s 안에 응답 받게 즉시 반환,
            # 실제 download 는 background. 다음 tick 의 pending list 에는 여전히
            # 'running' 상태로 잡히지만, finalize_job 자체가 idempotent (atomic
            # rename + UPDATE done) 라 outcome 동일. 외부 API 다운로드 비용 중복은
            # 별도 mitigation 필요 시 status='running' → 'finalizing' 같은 추가 enum.
            background.add_task(
                _do_finalize,
                batch_id=batch_id,
                seq=int(seq),
                engine=engine,
                result_url=res.result_url or "",
                output_ext=adapter.output_ext,
                cost_units=res.cost_units,
            )
            return {"job_id": job_id, "status": "done", "action": "finalize_scheduled"}
        return {"job_id": job_id, "status": res.status, "action": "unknown"}

    @app.get("/internal/jobs/pending")
    def internal_list_pending(request: Request, limit: int = 50):
        _check_internal(request)
        # 비동기 엔진만 — 어댑터의 is_synchronous=False 로 동적 결정 (Codex Q4 MED).
        async_engines = _async_engines()
        if not async_engines:
            return {"pending": []}
        # IN 절 — 길이 동적이라 placeholder 직접 생성
        in_placeholders = ", ".join(["%s"] * len(async_engines))
        sql = f"""
            SELECT j.job_id, j.batch_id, j.seq_in_batch, j.provider_job_id,
                   j.status, b.engine, b.output_media
              FROM genai_jobs j
              JOIN genai_batches b ON b.batch_id = j.batch_id
             WHERE j.status IN ('submitted','running')
               AND b.engine IN ({in_placeholders})
             ORDER BY j.submitted_at NULLS LAST
             LIMIT %s
        """
        params = (*async_engines, int(limit))
        with pg.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        cols = ["job_id", "batch_id", "seq_in_batch", "provider_job_id",
                "status", "engine", "output_media"]
        return {"pending": [dict(zip(cols, r)) for r in rows]}

    @app.post("/internal/jobs/submit-pending")
    def internal_submit_pending(request: Request, limit: int = 50):
        """deferred(pending) 비동기 job 을 동시성 한도 안에서 제출 (drain).

        Kling 1303 회피의 핵심: submit_batch 가 한도 초과분을 'pending' 으로 남기면
        sensor 가 매 tick 이 endpoint 호출 → 슬롯 빈 만큼만 제출. 1303 재발 시 다시
        deferred 로 두고 다음 tick 재시도.
        """
        _check_internal(request)
        from adapters import AdapterDeferredError, KlingTransientError, engine_max_concurrent, get_adapter
        import json as _json

        result = {"submitted": 0, "deferred": 0, "failed": 0}
        affected_batches: set[str] = set()
        for engine in _async_engines():
            pend = pg.list_pending_async_jobs(engine, limit=limit)
            if not pend:
                continue
            max_conc = engine_max_concurrent(engine)
            budget = None
            if max_conc > 0:
                budget = max(0, max_conc - pg.count_inflight_jobs(engine))
            adapter = get_adapter(engine)
            for job in pend:
                if budget is not None and budget <= 0:
                    result["deferred"] += 1
                    continue
                job_id = job["job_id"]
                batch_id = job["batch_id"]
                seq = int(job["seq_in_batch"])
                affected_batches.add(batch_id)
                try:
                    opts = _json.loads(job["options_json"]) if job["options_json"] else {}
                except Exception:
                    opts = {}
                is_text_only = (opts.get("mode") or "").strip().lower() == "txt2video"
                if is_text_only:
                    blob, name = b"", ""
                else:
                    blob, name = _load_nas_original_blob(batch_id, seq)
                    if blob is None:
                        pg.update_job_status(job_id, status="failed",
                                             error_message="original file gone (drain)")
                        result["failed"] += 1
                        continue
                opts["_job_id"] = job_id
                if engine == "comfy_local" and opts.get("workflow_id") == "sdxl-inpaint-cctv-v1":
                    mask_blob, mask_name = _load_nas_control_blob(batch_id, seq)
                    if mask_blob is None:
                        pg.update_job_status(
                            job_id, status="failed", error_message="control mask gone (drain)"
                        )
                        result["failed"] += 1
                        continue
                    opts["_mask_bytes"] = mask_blob
                    opts["_mask_filename"] = mask_name
                try:
                    sub = adapter.submit(blob, name, job["prompt"], options=opts or None)
                    pg.update_job_submitted(job_id, sub.provider_job_id)
                    if budget is not None:
                        budget -= 1
                    result["submitted"] += 1
                    if sub.is_synchronous and sub.immediate_result is not None:
                        from jobs.finalize import finalize_sync_results
                        finalize_sync_results(
                            batch_id=batch_id, engine=engine,
                            output_media=adapter.output_media,
                            results=[{
                                "job_id": job_id, "seq": seq,
                                "bytes": sub.immediate_result,
                                "ext": sub.immediate_ext or adapter.output_ext,
                                "cost_units": sub.cost_units,
                            }],
                        )
                except KlingTransientError as exc:
                    # 슬롯 아직 참 — deferred 유지, 같은 engine pass 중단 (Codex Q1)
                    pg.mark_job_deferred(job_id, str(exc))
                    result["deferred"] += 1
                    if budget is not None:
                        budget = 0
                except AdapterDeferredError as exc:
                    pg.mark_job_deferred(job_id, str(exc))
                    result["deferred"] += 1
                    if budget is not None:
                        budget = 0
                except Exception as exc:
                    pg.update_job_status(job_id, status="failed",
                                         error_message=f"drain submit failed: {exc}")
                    result["failed"] += 1
        # batch status 재계산 (finalize_sync_results 가 한 것 외 — submitted/failed 반영)
        for bid in affected_batches:
            pg.recompute_batch_status(bid)
        return result

    @app.post("/internal/batches/reconcile-stale")
    def internal_reconcile_stale(request: Request, limit: int = 200):
        """모든 job 이 terminal 인데 batch.status 가 running/pending 으로 남은 stale
        batch 를 보정. sensor 가 매 tick 호출하는 안전망 (submit 부분실패 crash 등)."""
        _check_internal(request)
        reconciled = pg.reconcile_stale_batches(limit=int(limit))
        return {
            "reconciled": len(reconciled),
            "batches": [{"batch_id": b, "status": s} for b, s in reconciled],
        }

    return app


app = create_app()
