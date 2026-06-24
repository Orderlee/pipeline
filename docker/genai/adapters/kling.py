"""Kling AI image2video 어댑터.

JWT 인증: AccessKey (iss) + SecretKey (HS256 sign).
공식 엔드포인트: https://api.klingai.com/v1/videos/image2video (image2video 비디오 생성)
                /v1/videos/image2video/{task_id} (status poll)

API 키 (KLING_ACCESS_KEY / KLING_SECRET_KEY) 미설정 시 mocking 모드 동작:
  - submit() → fake provider_job_id 반환
  - poll() → 첫 호출 'running', N초 후 'done' + 작은 더미 mp4 placeholder

이렇게 두면 사용자가 키를 채우기 전에도 e2e 흐름(NAS atomic write → ingest sensor →
build asset) 을 검증 가능.
"""

from __future__ import annotations

import os
import time
from typing import Any

from .base import _FAKE_MP4_PLACEHOLDER, BaseGenAIAdapter, PollResult, SubmitResult


class KlingError(RuntimeError):
    """Kling API 비-2xx 응답 (영구 실패 계열). 구조화 필드 보존."""

    def __init__(self, http_status: int, code, message: str, body: str):
        self.http_status = http_status
        self.provider_code = code
        self.provider_message = message
        self.body = body
        super().__init__(
            f"kling HTTP {http_status} code={code}: {message}" if message
            else f"kling HTTP {http_status}: {body[:300]}"
        )


class KlingTransientError(KlingError):
    """일시적/재시도 가능 에러 — 특히 1303 'parallel task over resource pack limit'.
    동시 작업 슬롯이 차서 발생 → 슬롯이 비면 재시도하면 통과. 영구 실패 아님."""


# Kling 에러 코드 → transient 여부 (admission control 계열)
_KLING_TRANSIENT_CODES = {1303}  # parallel task over resource pack limit


class KlingAdapter(BaseGenAIAdapter):
    engine = "kling"
    output_media = "video"
    is_synchronous = False
    output_ext = ".mp4"

    # Kling /v1/videos/image2video 의 공식 model_name enum (app.klingai.com 2026-05 확인).
    # ⚠️ "kling-video-o1" / "kling-video-3" 같은 UI/마케팅 이름은 API 가 거부
    #    (code 1201 "model is not supported"). API 식별자는 kling-vN-... 형식만 허용.
    # mode/duration 지원은 model 버전별 상이 — 공식 Capability Map 참조.
    # env KLING_AVAILABLE_MODELS 로 override (CSV).
    DEFAULT_MODELS: tuple[str, ...] = (
        "kling-v3",            # V3 — multi-shot 등 신기능 + per-second 과금 (3~15s 전 구간 유일 지원)
        "kling-v2-6",          # 최신 세대 — 공식 primary 예제 모델 (5/10s)
        "kling-v2-5-turbo",    # V2.5 turbo
        "kling-v2-1",          # V2.1
        "kling-v2-1-master",   # V2.1 Master — pro 전용
        "kling-v2-master",     # V2 Master
        "kling-v1-6",          # V1.6
        "kling-v1-5",          # V1.5
        "kling-v1",            # V1 (field 생략 시 API default)
    )

    # 공식 image2video API duration enum (kling.ai API Reference, 2026-06 확인): "3".."15".
    # ⚠️ 실지원은 model/mode 별 상이 (Capability Map). tier-priced 모델
    #    (kling-v2-6 default, v2-5-turbo, v2-1, v1.x, *-master) 은 5/10 만 —
    #    그 외 길이 submit 시 API 거부 가능 (code 1201 류). kling-v3 만 per-second 과금으로
    #    3~15 전 구간 동작 (multi-shot 합계 15s 포함).
    # env KLING_AVAILABLE_DURATIONS 로 override (CSV). lib/kling_pricing.SUPPORTED_DURATIONS 와 동기 유지.
    DEFAULT_DURATIONS: tuple[str, ...] = (
        "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
    )

    def __init__(self) -> None:
        self.access_key = os.getenv("KLING_ACCESS_KEY", "").strip()
        self.secret_key = os.getenv("KLING_SECRET_KEY", "").strip()
        self.api_base = os.getenv("KLING_API_BASE", "https://api.klingai.com").rstrip("/")
        self.model_name = os.getenv("KLING_MODEL_NAME", "kling-v3").strip()
        self.mode = os.getenv("KLING_MODE", "pro").strip()           # std | pro (Pro 요금제 → pro 권장)
        self.duration = os.getenv("KLING_DURATION", "5").strip()      # 3..15 (전 모델 호환 default 5)
        models_csv = os.getenv("KLING_AVAILABLE_MODELS", "").strip()
        self.available_models: tuple[str, ...] = (
            tuple(m.strip() for m in models_csv.split(",") if m.strip())
            if models_csv else self.DEFAULT_MODELS
        )
        durations_csv = os.getenv("KLING_AVAILABLE_DURATIONS", "").strip()
        self.available_durations: tuple[str, ...] = (
            tuple(d.strip() for d in durations_csv.split(",") if d.strip())
            if durations_csv else self.DEFAULT_DURATIONS
        )
        self.submit_timeout = int(os.getenv("KLING_SUBMIT_TIMEOUT", "60"))
        self.poll_timeout = int(os.getenv("KLING_POLL_TIMEOUT", "30"))
        self.download_timeout = int(os.getenv("KLING_DOWNLOAD_TIMEOUT", "300"))
        self.is_mock = not (self.access_key and self.secret_key)
        if self.is_mock:
            # 모킹 모드 추적용 in-memory store: provider_job_id -> {submitted_at, ...}
            self._mock_jobs: dict[str, dict[str, Any]] = {}

    # --------------------------------------------------------------
    # JWT 서명 (Kling 공식 가이드)
    # --------------------------------------------------------------
    def _build_jwt(self) -> str:
        import jwt  # PyJWT
        now = int(time.time())
        payload = {
            "iss": self.access_key,
            "exp": now + 1800,        # 30 minutes
            "nbf": now - 5,
        }
        return jwt.encode(payload, self.secret_key, algorithm="HS256",
                          headers={"alg": "HS256", "typ": "JWT"})

    # --------------------------------------------------------------
    # public API
    # --------------------------------------------------------------
    def submit(
        self,
        image_bytes: bytes,
        image_filename: str,
        prompt: str,
        options: dict | None = None,
    ) -> SubmitResult:
        if self.is_mock:
            import uuid
            provider_id = f"kling-mock-{uuid.uuid4().hex[:12]}"
            self._mock_jobs[provider_id] = {
                "submitted_at": time.time(),
                "prompt": prompt,
            }
            return SubmitResult(
                provider_job_id=provider_id,
                is_synchronous=False,
            )

        # 실제 호출: image bytes → base64 → JSON body. requests 사용.
        import base64
        import requests
        opts = options or {}
        body = {
            "model_name": opts.get("model_name", self.model_name),
            "mode": opts.get("mode", self.mode),
            "image": base64.b64encode(image_bytes).decode("ascii"),
            "prompt": prompt,
            "duration": str(opts.get("duration", self.duration)),
            "aspect_ratio": opts.get("aspect_ratio", "16:9"),
        }
        # negative prompt / cfg_scale / camera control 등 향후 옵션은 opts 통해 그대로 통과
        for extra in ("negative_prompt", "cfg_scale", "camera_control", "static_mask",
                      "dynamic_masks", "image_tail", "callback_url"):
            if extra in opts and opts[extra] is not None:
                body[extra] = opts[extra]
        headers = {
            "Authorization": f"Bearer {self._build_jwt()}",
            "Content-Type": "application/json",
        }
        r = requests.post(
            f"{self.api_base}/v1/videos/image2video",
            json=body,
            headers=headers,
            timeout=self.submit_timeout,
        )
        if not r.ok:
            # 4xx/5xx body 의 code/message 를 구조화. 1303(parallel limit) 등 admission
            # control 은 KlingTransientError 로 분류 → 호출자가 재시도/deferred 처리.
            raw = r.text[:600]
            code = None
            msg = ""
            try:
                jb = r.json()
                code = jb.get("code")
                msg = jb.get("message") or ""
            except Exception:
                pass
            ctx = (f"(model={body['model_name']!r} mode={body.get('mode')!r} "
                   f"duration={body.get('duration')!r}) ")
            # code 는 API 버전에 따라 int 1303 또는 str "1303" 로 올 수 있어 str 비교.
            code_norm = str(code) if code is not None else ""
            is_transient = (
                code_norm in {str(c) for c in _KLING_TRANSIENT_CODES}
                or "parallel task" in msg.lower()
                or "resource pack limit" in msg.lower()
            )
            full_msg = f"{ctx}{msg}".strip()
            if is_transient:
                raise KlingTransientError(r.status_code, code,
                                          f"Kling 동시 작업 한도 초과 — {full_msg}", raw)
            raise KlingError(r.status_code, code, full_msg or raw, raw)
        data = r.json().get("data", {}) or {}
        task_id = data.get("task_id")
        if not task_id:
            raise RuntimeError(f"kling submit 응답에 task_id 없음: {r.text[:500]}")
        return SubmitResult(provider_job_id=str(task_id), is_synchronous=False)

    def poll(self, provider_job_id: str) -> PollResult:
        if self.is_mock:
            job = self._mock_jobs.get(provider_job_id)
            if job is None:
                # 컨테이너 재시작으로 in-memory mock store 가 초기화된 상태.
                # 'failed' 가 아니라 'done' + placeholder 로 처리해 finalize 가 진행되도록
                # — mocking 모드는 e2e 흐름 검증용이라 stale id 도 정상 종료가 안전.
                return PollResult(
                    status="done",
                    result_url=f"mock://kling/{provider_job_id}.mp4",
                    cost_units=0.0,
                )
            elapsed = time.time() - job["submitted_at"]
            if elapsed < 2:
                return PollResult(status="running")
            return PollResult(
                status="done",
                result_url=f"mock://kling/{provider_job_id}.mp4",
                cost_units=0.0,
            )

        import requests
        headers = {"Authorization": f"Bearer {self._build_jwt()}"}
        r = requests.get(
            f"{self.api_base}/v1/videos/image2video/{provider_job_id}",
            headers=headers,
            timeout=self.poll_timeout,
        )
        if not r.ok:
            return PollResult(
                status="failed",
                error_message=f"kling poll HTTP {r.status_code}: {r.text[:300]}",
            )
        data = r.json().get("data", {}) or {}
        task_status = (data.get("task_status") or "").lower()
        if task_status in ("submitted", "processing", "running"):
            return PollResult(status="running")
        if task_status == "succeed":
            videos = (data.get("task_result") or {}).get("videos") or []
            if not videos:
                return PollResult(status="failed", error_message="empty videos[]")
            url = videos[0].get("url")
            # cost 추출 — Kling 응답 구조에서 credit 차감량 (필드명은 model/version 별로 다양:
            # 'cost' / 'price' / 'consume_credits' 등). 가장 흔한 후보들 시도.
            cost = (data.get("cost") or data.get("price")
                    or data.get("consume_credits") or data.get("credits_used"))
            try:
                cost_units = float(cost) if cost is not None else None
            except (TypeError, ValueError):
                cost_units = None
            return PollResult(status="done", result_url=url, cost_units=cost_units)
        if task_status == "failed":
            return PollResult(status="failed",
                              error_message=data.get("task_status_msg") or "kling failed")
        return PollResult(status="running")

    def download_result(self, result_url: str) -> bytes:
        if self.is_mock or result_url.startswith("mock://"):
            return _FAKE_MP4_PLACEHOLDER

        import requests
        r = requests.get(result_url, timeout=self.download_timeout, stream=True)
        r.raise_for_status()
        return r.content


# ------------------------------------------------------------------
# 계정 리소스팩(요금제) 조회 — Kling Open API GET /account/costs
# 무료, QPS<=1, remaining 은 12h 지연 집계. 같은 AccessKey/SecretKey JWT 사용.
# 응답: data.resource_pack_subscribe_infos[] (name/type/total/remaining/effective/invalid/status)
# ------------------------------------------------------------------
_RESOURCE_PACK_CACHE: dict[str, Any] = {"ts": 0.0, "packs": None}
# ponytail: 5min 캐시 — QPS<=1 보호 + remaining 12h 지연이라 자주 칠 이유 없음.
_RESOURCE_PACK_TTL = 300


def fetch_kling_resource_packs(force: bool = False) -> list[dict]:
    """계정 리소스팩 목록+잔여 반환 (실패 시 raise). 키 미설정(mock)이면 []."""
    now = time.time()
    cached = _RESOURCE_PACK_CACHE["packs"]
    if not force and cached is not None and now - _RESOURCE_PACK_CACHE["ts"] < _RESOURCE_PACK_TTL:
        return cached
    ad = KlingAdapter()
    if ad.is_mock:
        return []
    import requests
    now_ms = int(now * 1000)
    r = requests.get(
        f"{ad.api_base}/account/costs",
        params={"start_time": now_ms - 365 * 24 * 3600 * 1000, "end_time": now_ms},
        headers={"Authorization": f"Bearer {ad._build_jwt()}",
                 "Content-Type": "application/json"},
        timeout=ad.poll_timeout,
    )
    r.raise_for_status()
    body = r.json()
    if body.get("code") not in (0, None):
        raise RuntimeError(f"account/costs code={body.get('code')}: {body.get('message')}")
    packs = ((body.get("data") or {}).get("resource_pack_subscribe_infos")) or []
    _RESOURCE_PACK_CACHE.update(ts=now, packs=packs)
    return packs


def summarize_resource_packs(packs: list[dict], now_ts: float,
                             expiry_warn_days: float = 7.0,
                             low_pct_warn: float = 15.0) -> dict:
    """리소스팩을 표시용으로 보강(days_left/pct/USD/날짜) + 알림 산출. now 주입(테스트용).

    알림은 status='online' 팩만 — 만료 임박(expiry_warn_days 이내) 또는 잔여 낮음(low_pct_warn% 이하).
    runOut/expired 는 이미 소진/만료라 actionable 아님 → 알림 제외."""
    out: list[dict] = []
    alerts: list[str] = []
    for p in packs:
        inv = p.get("invalid_time")
        tot = p.get("total_quantity") or 0
        rem = p.get("remaining_quantity")
        days = round((inv / 1000 - now_ts) / 86400, 1) if inv else None
        pct = round(rem / tot * 100, 1) if (tot and rem is not None) else None
        out.append({
            **p,
            "days_left": days,
            "pct_remaining": pct,
            "remaining_usd": round(rem * 0.14, 2) if rem is not None else None,
            "invalid_date": time.strftime("%Y-%m-%d", time.localtime(inv / 1000)) if inv else "-",
        })
        if p.get("status") == "online":
            name = p.get("resource_pack_name", "?")
            if days is not None and days <= expiry_warn_days:
                alerts.append(f"⚠️ '{name}' 만료 임박 — {days}일 남음")
            if pct is not None and pct <= low_pct_warn:
                alerts.append(f"⚠️ '{name}' 잔여 부족 — {pct}% ({rem:.0f}u)")
    return {"packs": out, "alerts": alerts}


def resource_pack_totals(packs: list[dict], usd_per_unit: float = 0.14) -> dict:
    """전체 요금제(팩) 누적 합산 — 이전(runOut/expired)+현재(online) 모두 포함.

    used = Σ(total - remaining), 즉 모든 팩에서 실제 차감된 unit 합 = Kling 누적 사용량.
    remaining 미보고(None) 팩은 used 합산에서 제외(구매량 purchased 에는 포함)."""
    used = purchased = remaining = 0.0
    for p in packs:
        tot = p.get("total_quantity") or 0
        rem = p.get("remaining_quantity")
        purchased += tot
        if rem is not None:
            remaining += rem
            used += tot - rem
    return {
        "n_packs": len(packs),
        "used": round(used, 2),
        "purchased": round(purchased, 2),
        "remaining": round(remaining, 2),
        "used_usd": round(used * usd_per_unit, 2),
        "remaining_usd": round(remaining * usd_per_unit, 2),
    }
