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

import os
import re
import secrets
from pathlib import Path
from typing import Annotated

from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from adapters import ENGINE_TAB, all_engine_options, enabled_engines, engines_by_tab
from db import pg
from db.aggregates import cost_summary
from jobs.submit import submit_batch
import limits


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
            },
        )

    @app.post("/genai/batches")
    async def submit(
        request: Request,
        engine: Annotated[str, Form()],
        prompt: Annotated[str, Form()],
        files: Annotated[list[UploadFile] | None, File()] = None,
        model_name: Annotated[str | None, Form()] = None,
        mode: Annotated[str | None, Form()] = None,
        duration: Annotated[str | None, Form()] = None,
        aspect_ratio: Annotated[str | None, Form()] = None,
        bulk_group_id: Annotated[str | None, Form()] = None,
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
        # daily quota 검사 (입력 bytes + 일별 batch 수)
        try:
            limits.check_daily_quota(user, sum(len(b) for _, b in loaded))
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
        )
        # API 클라이언트가 명시적으로 JSON 요청한 경우만 JSONResponse.
        # 브라우저 (HTML 폼) 는 첫 화면 그대로 머무르도록 303 Redirect → '/?created=<batch_id>'
        # (toast 표시는 index.html 가 URL query 보고 처리).
        accept = (request.headers.get("accept") or "").lower()
        if "application/json" in accept and "text/html" not in accept:
            return JSONResponse(result)
        return RedirectResponse(
            url=f"/?created={result['batch_id']}",
            status_code=303,
        )

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
        return templates.TemplateResponse(
            request=request,
            name="bulk.html",
            context={
                "engines": enabled_engines(),
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
        model_name: Annotated[str | None, Form()] = None,
        mode: Annotated[str | None, Form()] = None,
        duration: Annotated[str | None, Form()] = None,
        aspect_ratio: Annotated[str | None, Form()] = None,
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

        # 4) plan 구성
        plan: list[dict] = []
        if is_text_only:
            plan = [{"prompt": p, "files": []} for p in prompts]
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
            for p, item in zip(prompts, loaded):
                plan.append({"prompt": p, "files": [item]})
        else:  # cartesian: 같은 prompt 마다 _MAX_FILES_PER_BATCH 단위 chunk
            for p in prompts:
                for i in range(0, len(loaded), _MAX_FILES_PER_BATCH):
                    plan.append({"prompt": p, "files": loaded[i:i + _MAX_FILES_PER_BATCH]})

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
        total_input_bytes = sum(len(b) for _, b in loaded)
        if rem_b is not None and n_batches > rem_b:
            raise HTTPException(status_code=429,
                                detail=f"일별 배치 한도 초과: 제출 {n_batches} > 잔여 {rem_b}")
        if rem_bytes is not None and total_input_bytes > rem_bytes:
            raise HTTPException(status_code=429,
                                detail=f"일별 bytes 한도 초과: 제출 {total_input_bytes:,} > 잔여 {rem_bytes:,}")

        # 7) options
        bgi = bgi or f"bgi-{_uuid.uuid4().hex[:12]}"
        common_opts: dict[str, str] = {}
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
                )
                submitted.append({
                    "i": i,
                    "batch_id": result.get("batch_id"),
                    "n_images": len(b["files"]),
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
            "total_planned": len(plan),
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
                fetch_kling_resource_packs, summarize_resource_packs, resource_pack_totals)
            all_packs = fetch_kling_resource_packs()
            # 표는 현재 활성(online) 팩만 노출. 누적 합계는 이전(runOut/expired)+현재 전체 합산.
            online = [p for p in all_packs if p.get("status") == "online"]
            packs_panel = summarize_resource_packs(online, _time.time())
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
        """실패한 job 1건을 재시도. input_asset_id 와 prompt 는 보존, 새 provider_job_id 발급.

        Codex Q3 HIGH: atomic CAS — status='failed' → 'submitted' RETURNING. RETURNING
        이 빈 row 면 다른 retry 가 이미 채갔거나 status 변동 → 409. 외부 API 중복 호출 방지.
        """
        with pg.connect() as conn:
            with conn.cursor() as cur:
                # 1) atomic transition (status='failed' 만 통과)
                cur.execute(
                    """
                    UPDATE genai_jobs
                       SET status = 'submitted',
                           error_message = NULL,
                           provider_job_id = NULL,
                           submitted_at = CURRENT_TIMESTAMP,
                           completed_at = NULL
                     WHERE job_id = %s AND status = 'failed'
                    RETURNING batch_id, seq_in_batch
                    """,
                    (job_id,),
                )
                cas = cur.fetchone()
                if cas is None:
                    raise HTTPException(
                        status_code=409,
                        detail="job is not in 'failed' state (retry race or already in-flight)",
                    )
                # batch.status 도 즉시 'running' 으로 복귀
                cur.execute(
                    "UPDATE genai_batches SET status='running', completed_at=NULL "
                    "WHERE batch_id=%s",
                    (cas[0],),
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
        engine, prompt, options_json_raw = row
        status = "submitted"  # CAS 통과 — 이미 전환됨

        # options_json 에서 mode 회수 — txt2video 면 NAS 원본 읽기 skip
        import json as _json
        try:
            opts_retry = _json.loads(options_json_raw) if options_json_raw else {}
        except Exception:
            opts_retry = {}
        is_text_only_retry = (opts_retry.get("mode") or "").strip().lower() == "txt2video"

        from adapters import KlingTransientError, get_adapter
        adapter = get_adapter(engine)

        if is_text_only_retry:
            blob = b""
            orig_name = ""
        else:
            # 원본 input image 를 NAS 에서 다시 읽어 어댑터 submit (공유 헬퍼)
            blob, orig_name = _load_nas_original_blob(batch_id, int(seq))
            if blob is None:
                # CAS 한 status 를 'failed' 로 되돌림 (원본 없으면 retry 자체 불가)
                with pg.connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE genai_jobs SET status='failed', "
                            "error_message='original file gone (archive moved?)' "
                            "WHERE job_id=%s",
                            (job_id,),
                        )
                raise HTTPException(status_code=410, detail=f"original file gone: {orig_name}")

        try:
            sub = adapter.submit(blob, orig_name, prompt, options=opts_retry or None)
        except KlingTransientError as exc:
            # 1303 등 동시 한도 — failed 아님. deferred(pending) 로 두고 sensor drain
            # 이 슬롯 빌 때 재제출. batch 는 running 유지.
            pg.mark_job_deferred(job_id, str(exc))
            pg.recompute_batch_status(batch_id)
            return {"job_id": job_id, "status": "pending", "action": "deferred",
                    "detail": str(exc)}
        except Exception as exc:
            # 외부 API 실패 시 status='failed' 로 되돌림
            pg.update_job_status(job_id, status="failed",
                                  error_message=f"retry adapter submit failed: {exc}")
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
        from adapters import KlingTransientError, engine_max_concurrent, get_adapter
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
