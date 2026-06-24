"""Kling Video 가격 lookup — image2video / text2video 기본 모드만.

기준: https://kling.ai/document-api/productBilling/prePaidResourcePackage (2026-06-01 확인).
환산: 1 credit unit = $0.14 USD (모든 행에서 일관).

Trial package 도 동일 단가 — trial 은 무료 credit 한도만 다름. 가격표 데이터는 paid/trial
구분 없음. 사용자에게 "예상 차감 credit + USD" 만 보여주면 충분.

미지원/누락 케이스:
- v3 는 초당 가격 (3~15s 입력 받아 곱셈) — _KLING_V3_PER_SECOND_CREDITS 사용
- tier-priced 모델(v2-6 등)은 5/10 만 가격표 보유. UI 가 3~15 전 구간을 노출하지만
  5/10 외 길이는 cost_units=None('가격표 누락')로 표기 + submit 시 API 가 거부할 수 있음
  (해당 모델 미지원 길이). 실제 >10s 영상은 kling-v3 사용 권장.
- v2-6 의 sound options (sound=on/voice) 는 현재 어댑터가 항상 off 송신이므로 lookup
  키에 'sound' 차원 안 둠. 향후 sound 노출시 확장 필요.
- Motion Control / Multi-elements / Reference-to-Video / Lip Sync / Effect / Extend Video
  같은 advanced 모드는 별도 가격표 — Studio UI 가 일반 image2video/text2video 만
  지원하므로 미반영.
"""

from __future__ import annotations

from typing import TypedDict

# 1 credit unit = $0.14 USD — 가격표의 "Deduct N units" + "$X" 컬럼에서 역산.
USD_PER_UNIT = 0.14

# Kling video 의 mode = 출력 해상도 (공식 docs): std=720P, pro=1080P, 4k=4K.
# 4K 는 v3 등 일부 모델만 지원 (Capability Map) — 비지원 모델+4k 는 가격표 miss → cost None.
SUPPORTED_MODES = ("std", "pro", "4k")

# 공식 image2video API duration enum ("3".."15"). adapters.kling.KlingAdapter.DEFAULT_DURATIONS
# 와 동기 유지. ⚠️ 아래 _KLING_VIDEO_CREDITS 가격표는 tier-priced 모델의 5/10 만 보유 —
# 그 외 길이는 estimate_kling_cost() 가 cost_units=None('가격표 누락') 으로 graceful 처리.
# kling-v3 만 per-second 라 3~15 전 구간 정확한 단가 산출.
SUPPORTED_DURATIONS = ("3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15")


class CostEstimate(TypedDict):
    """예상 차감/금액. lookup miss 시 cost_units=None."""
    cost_units: float | None
    cost_usd: float | None
    note: str  # 사용자 노출용 hint (e.g. "v3 per-second pricing")


# Lookup key: (model_name, mode, duration_seconds_str)
# 값: credit units. USD 는 USD_PER_UNIT 곱셈으로 도출.
_KLING_VIDEO_CREDITS: dict[tuple[str, str, str], float] = {
    # V1.0
    ("kling-v1", "std", "5"):  1.0,
    ("kling-v1", "std", "10"): 2.0,
    ("kling-v1", "pro", "5"):  3.5,
    ("kling-v1", "pro", "10"): 7.0,
    # V1.5
    ("kling-v1-5", "std", "5"):  2.0,
    ("kling-v1-5", "std", "10"): 4.0,
    ("kling-v1-5", "pro", "5"):  3.5,
    ("kling-v1-5", "pro", "10"): 7.0,
    # V1.6 — V1.5 와 동일 단가
    ("kling-v1-6", "std", "5"):  2.0,
    ("kling-v1-6", "std", "10"): 4.0,
    ("kling-v1-6", "pro", "5"):  3.5,
    ("kling-v1-6", "pro", "10"): 7.0,
    # V2 Master (mode 무관, flat)
    ("kling-v2-master", "std", "5"):  10.0,
    ("kling-v2-master", "std", "10"): 20.0,
    ("kling-v2-master", "pro", "5"):  10.0,
    ("kling-v2-master", "pro", "10"): 20.0,
    # V2.1 (V1.5/1.6 와 동일)
    ("kling-v2-1", "std", "5"):  2.0,
    ("kling-v2-1", "std", "10"): 4.0,
    ("kling-v2-1", "pro", "5"):  3.5,
    ("kling-v2-1", "pro", "10"): 7.0,
    # V2.1 Master (V2 Master 와 동일)
    ("kling-v2-1-master", "std", "5"):  10.0,
    ("kling-v2-1-master", "std", "10"): 20.0,
    ("kling-v2-1-master", "pro", "5"):  10.0,
    ("kling-v2-1-master", "pro", "10"): 20.0,
    # V2.5 Turbo
    ("kling-v2-5-turbo", "std", "5"):  1.5,
    ("kling-v2-5-turbo", "std", "10"): 3.0,
    ("kling-v2-5-turbo", "pro", "5"):  2.5,
    ("kling-v2-5-turbo", "pro", "10"): 5.0,
    # V2.6 (sound=off 기본 — 어댑터가 항상 off 송신)
    ("kling-v2-6", "std", "5"):  1.5,
    ("kling-v2-6", "std", "10"): 3.0,
    ("kling-v2-6", "pro", "5"):  2.5,
    ("kling-v2-6", "pro", "10"): 5.0,
}

# V3 는 초당 단가 — duration 직접 곱셈. mode = 해상도 (std=720P / pro=1080P / 4k=4K).
# 출처: https://kling.ai/dev/pricing (kling-v3 "No Native Audio" 행, 2026-06-24 확인).
_KLING_V3_PER_SECOND_CREDITS: dict[str, float] = {
    "std": 0.6,  # 720P, sound off
    "pro": 0.8,  # 1080P, sound off
    "4k":  3.0,  # 4K,    sound off
}


def estimate_kling_cost(
    model_name: str,
    mode: str = "pro",
    duration: str | int = 5,
) -> CostEstimate:
    """단일 Kling video job 의 예상 차감 + USD 반환.

    매핑 누락 시 cost_units=None + note 에 사유.
    """
    m = (model_name or "").strip().lower()
    md = (mode or "").strip().lower()
    dur = str(duration or "").strip()
    if dur == "":
        dur = "5"

    # V3 per-second
    if m == "kling-v3":
        rate = _KLING_V3_PER_SECOND_CREDITS.get(md)
        if rate is None:
            return CostEstimate(cost_units=None, cost_usd=None,
                                note=f"v3 mode={md!r} 지원 안 함 (std/pro 만)")
        try:
            sec = int(dur)
        except ValueError:
            return CostEstimate(cost_units=None, cost_usd=None,
                                note=f"v3 duration={dur!r} 정수 필요")
        if sec <= 0:
            return CostEstimate(cost_units=None, cost_usd=None,
                                note="duration <= 0")
        units = round(rate * sec, 4)
        return CostEstimate(
            cost_units=units,
            cost_usd=round(units * USD_PER_UNIT, 4),
            note=f"v3 per-second ({rate}/s × {sec}s, sound=off)",
        )

    key = (m, md, dur)
    units = _KLING_VIDEO_CREDITS.get(key)
    if units is None and md in ("std", "pro"):
        # master 모델은 std/pro 동일 단가 → 서로 fallback. (4k 는 별도 해상도 tier 라
        # fallback 금지 — flat 표에 4k 없는 모델은 그대로 None = '가격표 누락'.)
        alt = "pro" if md == "std" else "std"
        units = _KLING_VIDEO_CREDITS.get((m, alt, dur))
    if units is None:
        return CostEstimate(
            cost_units=None, cost_usd=None,
            note=f"가격표 누락: model={m!r} mode={md!r} duration={dur!r}s",
        )
    return CostEstimate(
        cost_units=units,
        cost_usd=round(units * USD_PER_UNIT, 4),
        note="image2video / text2video 기본 (sound=off)",
    )


def pricing_table_json() -> dict:
    """/genai/limits API 노출용 JSON. UI JS 도 fetch 해서 클라이언트 lookup 가능.

    구조:
      {
        "engine": "kling",
        "usd_per_unit": 0.14,
        "video": {
          "kling-v2-6": {"std": {"5": 1.5, "10": 3.0}, "pro": {"5": 2.5, "10": 5.0}},
          ...
        },
        "video_per_second": {  # v3 만
          "kling-v3": {"std": 0.6, "pro": 0.8}
        }
      }
    """
    video: dict[str, dict[str, dict[str, float]]] = {}
    for (model, mode, dur), units in _KLING_VIDEO_CREDITS.items():
        video.setdefault(model, {}).setdefault(mode, {})[dur] = units
    return {
        "engine": "kling",
        "usd_per_unit": USD_PER_UNIT,
        "supported_modes": list(SUPPORTED_MODES),
        "supported_durations": list(SUPPORTED_DURATIONS),
        "video": video,
        "video_per_second": {
            "kling-v3": dict(_KLING_V3_PER_SECOND_CREDITS),
        },
        "source": "https://kling.ai/document-api/productBilling/prePaidResourcePackage",
        "captured_at": "2026-06-01",
    }
