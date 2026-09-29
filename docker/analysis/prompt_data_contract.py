#!/usr/bin/env python3
"""프롬프트 뱅크 데이터 해석 계약 — 생산자(뱅크 빌더)와 소비자(패널·분석기)가 공유한다.

왜 이 모듈이 있나: 버전 정규화·local gidx·중복 판정이 4,216줄짜리 FiftyOne 패널
(`plugins/user-prompt-compare/__init__.py`) 안에만 있었다. 분석기가 그걸 쓰려면 패널 전체
(`fiftyone.operators` 포함)를 import 해야 하는데 분석기는 App 밖에서 돌기 때문에 성립하지
않는다. `prompt_standard.py` 가 프롬프트 **생성** 규칙에 한 일을 데이터 **해석** 규칙에 한다.

**순수 함수만 둔다** — fiftyone·psycopg2·numpy 를 import 하지 않는다. 그래야 CI(셋 다 없음)
에서 돌고, 계약이 런타임 환경에 의존하지 않는다.
"""

from __future__ import annotations

import hashlib

#: FiftyOne 전역 gidx 에 얹히는 세대 블록 크기.
#: ⚠️ 런타임 env 에서 읽지 않는다 — 세대마다 값이 흔들리면 프레임↔문장 조인이 조용히 어긋난다.
GIDX_OFFSET = 100_000

#: 텍스트가 없는 뱅크(`prompt_banks.sentence_storage='external_only'`)가 쓰는 자리표시자 접두.
#: 실측: 55뱅크 중 17개가 external_only 이고 sourcei-prompts 기준 8개 버전이 텍스트 100% 자리표시자다.
PLACEHOLDER_PREFIX = "(텍스트 없음 #"


def norm_version(v):
    """`V1.0.10.3` / `v1.0.10.3` / `1.0.10.3` → `1.0.10.3`.

    `prompt_banks.version_tag` 는 대소문자·`v` 접두가 흔들린다(실측 55행에 `V1.0.10.3` 과
    `1.0.13.0` 이 함께 있다) — 정규화 없이 등식 조인하면 **예외 없이 0건**이 된다.
    """
    return str(v if v is not None else "").strip().lstrip("vV")


def local_gidx(g):
    """전역 gidx → 뱅크-로컬 행 번호."""
    return None if g is None else int(g) % GIDX_OFFSET


def build_gidx_class_map(versions, gidxs, categories):
    """(버전, gidx, 클래스) 3열 → `{(norm_version, local_gidx): class}`.

    버전·gidx 중 하나라도 None 인 행은 **버린다**. 이 맵의 결손은 예측 복원 실패로
    이어지므로 호출부가 결손 건수를 반드시 보고해야 한다.
    (실측: sourcei 31버전 전부 unmapped 0건으로 복원된다.)
    """
    out = {}
    for v, g, c in zip(versions, gidxs, categories):
        if v is None or g is None:
            continue
        out[(norm_version(v), local_gidx(g))] = c
    return out


def text_set_hash(texts):
    """버전의 문장 집합 지문 — 순서 비의존, 앞뒤 공백 무시."""
    h = hashlib.sha256()
    for t in sorted((s or "").strip() for s in texts):
        h.update(t.encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()


def exact_duplicate_groups(preds_by_version):
    """예측벡터가 **바이트 동일**한 버전들을 묶는다. 1개짜리 묶음은 반환하지 않는다.

    실측(sourcei 6,032프레임): 31버전 → 고유 24개, 4묶음(최대 5버전). 이걸 접지 않으면
    다중비교 family 가 부풀고(C(31,2)=465 vs C(24,2)=276) 표에 같은 내용이 여러 줄로 나온다.
    """
    by = {}
    for ver, vec in preds_by_version.items():
        by.setdefault(vec, []).append(ver)
    return sorted([sorted(v) for v in by.values() if len(v) > 1])


def misattribution_suspects(dup_groups, text_hash_by_version):
    """벡터 귀속 오류 의심 묶음 — **예측은 같은데 문장 집합이 다른** 경우.

    근거(실측): `v1.0.2.0` 은 공급자 JSON 이 `v1.0.2.1` 과 바이트 동일이라 뷰의 점들이
    `v1.0.2.1` 의 벡터다. 텍스트는 자리표시자라 눈에는 다르게 보이지만 예측은 같다.
    이 기준을 sourcei 4묶음에 적용하면 `v1.0.2.0/v1.0.2.1`(기존 확인)과
    `V1.0.11.1`(신규 의심) 둘만 걸린다.

    ⚠️ **자리표시자 비율을 기준으로 삼으면 안 된다** — external_only 뱅크 8종이 텍스트
    100% 자리표시자인데 `category.label` 은 멀쩡하고 벡터도 유효하다(corrupt 아님).
    """
    out = []
    for grp in dup_groups:
        if len({text_hash_by_version.get(v) for v in grp}) > 1:
            out.append(sorted(grp))
    return out
