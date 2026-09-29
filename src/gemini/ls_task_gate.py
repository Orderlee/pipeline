"""라벨러에게 보낼지 말지를 정하는 게이트 — video/image 두 진입점이 공유한다.

왜 필요한가 (2026-09-10 DB 실측):

    Track A (timestamp) : auto_label 완료 22,639 영상 중 **18,378(81.2%)** 가 labels 행 없음
    Track B (bbox)      : 자동 bbox 454,726 프레임 중 **249,686(54.9%)** 가 박스 0개

그런데 두 진입점 모두 **열거한 것을 전부 태스크로 만든다.** `_create_video` 는 events JSON 이
아니라 vlm-raw 영상 키를 열거하고, `_create_image` 는 임계값을 적용하기 **전에**
`create_image_task` 를 부른다. 결과적으로 라벨러가 여는 화면의 절반 이상이 비어 있다.

이 모듈은 그 판정을 한 곳에 모은다. 규칙은 트랙 공통이다 — "자동 라벨링 결과가 0건이면
기본 제외, 단 일부는 통과시킨다".

**0건을 전부 버리면 안 되는 이유**: 모델이 놓친 것을 사람이 영원히 못 보게 되고, 학습셋이
"모델이 이미 찾는 것"으로만 채워져 사각지대가 고착된다. 그래서 두 갈래를 남긴다 —

    BYPASS : 무작위 표본. 게이트의 **오탈락률을 추정하는 유일한 수단**이다.
             이게 없으면 게이트 이후 생기는 모든 사람 라벨이 selection-biased 가 되어
             편향 없는 평가셋을 영구히 못 만든다.
    (AL)   : 능동학습이 고른 소수. 별도 배선이며 이 모듈은 통과 여부만 받는다.

표집 방식 — 시드 없는 **결정적 해시**를 쓴다. 같은 키는 언제 돌려도 같은 판정이라
재실행·재개가 안전하고, 키에 asset 이 들어가므로 asset 마다 독립적으로 ~ratio 가 뽑혀
**비례 층화**가 자동으로 된다(프레임 단위 전역 무작위는 한 영상에 몰릴 수 있다).

ponytail: "배치당 최소 200장" 은 스트리밍으로 보장할 수 없어(모집단을 미리 모르므로)
    구현하지 않았다. 대신 실제 통과 수를 리포트하니, 작은 배치에서는 운영자가 ratio 를
    올린다. 정확한 최소값이 필요해지면 그때 2-pass 로 바꾼다.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass

_BUCKETS = 10_000


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        print(f"[WARN] {name}={raw!r} 파싱 실패 — 기본값 {default} 사용")
        return default


@dataclass(frozen=True)
class GateConfig:
    """게이트 설정. **기본은 off** — 켜는 것은 명시적 결정이어야 한다."""

    enabled: bool = False
    bypass_ratio: float = 0.02
    # 합성본(source_type='genai_output')은 기본 전량 통과한다. 게이트의 전제는 "자동 라벨이
    # 되는 데이터"인데 합성 생성의 목적은 정반대 — 자동 검출이 약한 클래스(연기·화재·쓰러짐)를
    # 사람 검수로 채우는 것이다. 게이트를 그대로 두면 SAM3 가 이미 잘 잡는 것만 사람에게 가고
    # 합성으로 메우려던 결손은 영원히 안 메워진다.
    #
    # ⚠️ 이 면제의 최초 근거였던 "SAM3 가 합성 연기에서 0건을 검출했다" 는 **틀렸다**(2026-09-21
    # 정정). 그때 LS task 가 0건이던 진짜 이유는 배관이었다 — find_pending_images 의 image_role
    # 필터가 정지 이미지를 후보에서 뺐고, SAM3 는 그 이미지를 본 적조차 없었다. 배관을 고친 뒤
    # 실제로 돌리니 categories=['smoke'], score 0.6953 으로 **잡았다**.
    #
    # 그래도 기본 1.0 을 유지하는 근거는 구조적인 것이다: 게이트는 result_count==0 일 때만
    # 개입하는데, 0건인 합성본이란 곧 "SAM3 가 못 본 이벤트" 이고 그게 ComfyUI 를 돌린 이유
    # 자체다. 실측도 이를 지지한다 — 사람 GT 가 있는 fire_smoke_gt 코호트에서 0건 비율은
    # normal 4.8% ≪ fire 20.7% < smoke 28.6% 로, 이벤트 클래스가 normal 보다 3~6배 자주
    # 빈손이다. 합성 표본은 아직 n=1 이라 검출률 자체는 측정 불가(95% CI [0.025, 1.0]).
    synthetic_bypass_ratio: float = 1.0

    @classmethod
    def from_env(cls) -> GateConfig:
        return cls(
            enabled=_env_bool("LS_TASK_GATE_ENABLED", False),
            bypass_ratio=_env_float("LS_TASK_GATE_BYPASS_RATIO", 0.02),
            synthetic_bypass_ratio=_env_float("LS_TASK_GATE_SYNTHETIC_BYPASS_RATIO", 1.0),
        )

    def ratio_for(self, is_synthetic: bool) -> float:
        return self.synthetic_bypass_ratio if is_synthetic else self.bypass_ratio

    def describe(self) -> str:
        if not self.enabled:
            return "게이트 OFF — 열거된 전부를 태스크로 만든다 (기존 동작)"
        return (
            f"게이트 ON — 자동 결과 0건 제외, 그중 {self.bypass_ratio:.1%} 는 무작위 통과 "
            f"(합성본은 {self.synthetic_bypass_ratio:.1%})"
        )


def _bypass(key: str, ratio: float) -> bool:
    """결정적 해시 표집. 같은 키 → 항상 같은 판정."""
    if ratio <= 0:
        return False
    if ratio >= 1:
        return True
    digest = hashlib.sha1(key.encode("utf-8")).digest()
    bucket = int.from_bytes(digest[:4], "big") % _BUCKETS
    return bucket < int(ratio * _BUCKETS)


@dataclass(frozen=True)
class Decision:
    send: bool
    reason: str  # "has_result" | "bypass_sample" | "synthetic_bypass" | "empty_result"


def decide(result_count: int, key: str, cfg: GateConfig, is_synthetic: bool = False) -> Decision:
    """이 후보를 라벨러에게 보낼 것인가.

    Args:
        result_count: 자동 라벨링 결과 수. image=임계값 적용 후 박스 수, video=이벤트 수.
        key: 안정적인 식별자. asset/경로를 포함해야 층화가 된다 (MinIO 키를 그대로 쓰면 됨).
        cfg: GateConfig.
        is_synthetic: 합성 생성본(genai_output)인가. 별도 bypass 비율이 적용된다 —
            이유는 GateConfig.synthetic_bypass_ratio 주석 참고.
    """
    if not cfg.enabled:
        return Decision(True, "has_result" if result_count else "empty_result")
    if result_count > 0:
        return Decision(True, "has_result")
    if _bypass(key, cfg.ratio_for(is_synthetic)):
        # 합성 통과는 무작위 표본이 아니다(확률 1). 같은 reason 으로 세면 BYPASS 표본이
        # 오염되어 게이트의 오탈락률 추정이 깨진다 — 이 모듈 docstring 이 그 표본을
        # "오탈락률을 추정하는 유일한 수단"이라고 못 박고 있다.
        return Decision(True, "synthetic_bypass" if is_synthetic else "bypass_sample")
    return Decision(False, "empty_result")


class GateStats:
    """진입점이 공유하는 집계 — 게이트를 채점하려면 이 수치가 남아야 한다."""

    def __init__(self) -> None:
        self.sent_with_result = 0
        self.sent_bypass = 0
        self.sent_synthetic = 0
        self.gated_out = 0

    def record(self, d: Decision) -> None:
        if not d.send:
            self.gated_out += 1
        elif d.reason == "synthetic_bypass":
            self.sent_synthetic += 1
        elif d.reason == "bypass_sample":
            self.sent_bypass += 1
        else:
            self.sent_with_result += 1

    def summary(self) -> str:
        total = self.sent_with_result + self.sent_bypass + self.sent_synthetic + self.gated_out
        if not total:
            return "게이트: 후보 0건"
        cut = 100.0 * self.gated_out / total
        sent = self.sent_with_result + self.sent_bypass + self.sent_synthetic
        return (
            f"게이트: 후보 {total} → 전달 {sent} "
            f"(결과보유 {self.sent_with_result} + 무작위통과 {self.sent_bypass} "
            f"+ 합성통과 {self.sent_synthetic}) "
            f"/ 제외 {self.gated_out} ({cut:.1f}%)"
        )
