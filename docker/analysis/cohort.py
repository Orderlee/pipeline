#!/usr/bin/env python3
"""코호트 레지스트리 — 새 현장을 **설정 1줄**로 편입한다.

왜: `docker/analysis/` 파이썬 176개 중 **92개**가 `"sourcei"` 또는 `sourcei_gt` 경로를
하드코딩하고 있고, 코호트를 인자로 받는 것은 **11개**뿐이다(2026-09-16 실측). 새 현장이
생길 때마다 스크립트를 새로 쓰는 구조를 여기서 끊는다.

⚠️ **군집키(`group`)를 자동 유도하지 않는다.** `sitej_subway` 는 `camera` 가 58대라
자동 유도하면 그걸 고르는데, 그 코퍼스는 연출 동시녹화라 카메라 홀드아웃에 누수가 있고
올바른 군집키는 `session` 이다. 군집키를 잘못 잡으면 신뢰구간이 좁아져 **"유의하다"는
거짓 결론**이 나온다 — 이 계층이 막으려는 사고 그 자체다. 그래서 미등록 코호트는
`camera` 로 조용히 폴백하지 않고 **거부**한다.

설계: `docs/superpowers/specs/2026-09-16-analysis-standardization-design.md`
"""

from __future__ import annotations

import time

#: 새 현장 편입은 여기 한 줄. 스크립트를 새로 쓰지 않는다.
#: 모든 키는 **명시 필수** — 빠진 값을 추론하지 않는다.
#: `target_classes` 는 GT 라벨 문자열과 **글자 단위로** 같아야 한다(데이터의 오타까지 포함).
COHORTS = {
    "sourcei": dict(
        prompts="sourcei-prompts",
        gt_field="ground_truth",
        group="camera",
        negative_class="normal",
        target_classes=["falldown", "fire", "smoke"],
    ),
    "sitej_subway": dict(
        prompts="sitej_subway-prompts",
        gt_field="ground_truth",
        group="session",  # ⚠️ camera(58대) 아님 — 위 모듈 주석 참고
        negative_class="normal",
        # 'intrustion' 은 데이터의 실제 라벨 철자다(오타이지만 정본) — 고치면 조인이 깨진다.
        target_classes=["falldown", "fire", "smoke", "intrustion"],
    ),
}

#: 등록하지 않기로 **판정한** 코호트와 그 사유. 침묵보다 명시가 낫다.
REFUSED = {
    "frames": (
        "G0_INSUFFICIENT_GT",
        "bank_gt 가 203,869 중 40장뿐이고 군집 필드가 없다 — 표본이 생기면 cohort.py 에 등록할 것",
    ),
}


class CohortRefused(Exception):
    """이 코호트로는 산출하지 않는다 — 예외가 아니라 **판정**이다."""

    def __init__(self, cohort, reason_code, detail):
        super().__init__(f"{cohort}: {reason_code} — {detail}")
        self.cohort = cohort
        self.reason_code = reason_code
        self.detail = detail


def resolve(name):
    """코호트 설정 **복사본** 반환. 미등록·거부 대상이면 `CohortRefused`.

    복사본인 이유: 호출부가 받은 dict 를 고쳐도 레지스트리가 오염되지 않아야 한다
    (한 프로세스에서 여러 코호트를 연속 처리할 때 조용히 섞인다).
    """
    if name in REFUSED:
        raise CohortRefused(name, *REFUSED[name])
    if name not in COHORTS:
        raise CohortRefused(
            name,
            "G0_COHORT_NOT_REGISTERED",
            "COHORTS 에 없다. group 은 자동 유도하지 않으므로 cohort.py 에 한 줄 추가할 것 "
            "(prompts/gt_field/group/negative_class/target_classes 전부 명시)",
        )
    return dict(COHORTS[name])


def load_cohort(name, stages=("S0", "S4")):
    """라이브 FiftyOne 데이터셋에서 `analysis_standard.run()` 의 `D` 를 만든다.

    ⚠️ **`preds.npz` 를 읽지 않는다.** 그 파일은 7,498장 구코호트이고 라이브 `sourcei` 는
    6,032장이다. 구 `load_sourcei()` 는 `assert hid == list(d["ids"])` 에서 **터져서 아예
    돌지 않는 상태**였다(2026-09-16 실측) — 이 승격의 직접적 이유다.

    `stages` 로 **선택 적재**한다. 단일 비대 D 를 만들면 S0 만 필요한 호출이 S1~S5 의
    입력(문장 벡터·점수 행렬)까지 읽어 느려지고 예외 시점도 흐려진다.
    """
    import fiftyone as fo
    import numpy as np

    cfg = resolve(name)  # 미등록·거부 대상이면 여기서 CohortRefused
    ds = fo.load_dataset(name)
    sch = ds.get_field_schema()
    if cfg["group"] not in sch:
        raise CohortRefused(
            name, "G0_GROUP_FIELD_MISSING",
            f"레지스트리가 지정한 군집키 '{cfg['group']}' 가 데이터셋에 없다 — "
            f"다른 필드로 대체하지 않는다(거짓 유의의 원인)")
    if cfg["gt_field"] not in sch:
        raise CohortRefused(name, "G0_GT_FIELD_MISSING", f"GT 필드 '{cfg['gt_field']}' 가 없다")

    classes = [cfg["negative_class"]] + list(cfg["target_classes"])
    gt_lab, grp = ds.values([f"{cfg['gt_field']}.label", cfg["group"]])
    unknown = sorted({x for x in gt_lab if x not in classes})
    if unknown:
        raise CohortRefused(
            name, "G0_UNKNOWN_GT_CLASS",
            f"레지스트리에 없는 GT 클래스 {unknown} — target_classes 를 고치거나 데이터를 확인할 것")
    if any(g is None for g in grp):
        n = sum(1 for g in grp if g is None)
        raise CohortRefused(name, "G0_GROUP_VALUE_NULL",
                            f"군집키 '{cfg['group']}' 가 {n}행에서 비어 있다 — 군집 부트스트랩이 성립하지 않는다")

    gt = np.array([classes.index(x) for x in gt_lab])
    return dict(name=name, gt=gt, group=np.array(grp), classes=classes,
                events=list(cfg["target_classes"]), cohort_cfg=cfg,
                requested_stages=list(stages))


def refusal_artifact(name, reason_code, detail):
    """거부도 **아티팩트로 발행한다.**

    생략하면 이전 성공 아티팩트가 그대로 남아 소비자가 낡은 표를 계속 그린다
    (이 저장소의 반복 버그 형태 "부재에 기댄 안전"). 거부는 이전 성공본을 **덮어쓴다**.
    """
    return dict(
        cohort=name,
        status="refused",
        reason_code=reason_code,
        detail=detail,
        display_allowed=False,
        complete=True,
        generated_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    )
