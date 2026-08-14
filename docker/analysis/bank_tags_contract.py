#!/usr/bin/env python3
"""뱅크 버전 태그 해석 계약 검사 — 생산자 ↔ 소비자 드리프트 탐지기.

배경: 버전 접미사가 세 세대로 갈려 있다.
    v1_0_8_0  vt   — `pred_`/`vote_`/`wave_pred_` 계열
    v1080     vtag — `winner_gidx_`/`wave_iou_`/`margin_` 계열 (현행 생산자 정본)
    v080      구   — 2026-08-11 이전 잔존. 폴백 전용

소비자들이 각자 부분 구현을 갖고 있고 **공유 모듈을 둘 수 없다** — 컨테이너의 PYTHONPATH 가
미설정이라 `/workspace` 가 플러그인 프로세스의 sys.path 에 없다 (실측). 그래서 공유 *코드*
대신 공유 *계약* 을 둔다. 방향이 `테스트 → 소비자` 라 import 문제가 없다.

이 검사가 존재하는 이유(실제 사고, 2026-08-14):
    생산자가 2026-08-11 에 `vtag()` 를 전 파트 조인으로 바꿨는데 `prompt_scores_export.
    suffixes()` 가 따라가지 않아, `winner_gidx_{v}` 가 **전 버전에서 해석 실패**했다.
    → 정본 3층 export 의 귀속 층이 전 행 null. `validate` 는 7,498건으로 잡을 수 있었지만
    아무도 돌리지 않아 몇 달을 갔다. 이 파일은 그 층보다 **앞단(리졸버)** 을 지킨다.

실행:
    docker exec docker-analysis-1 python /workspace/bank_tags_contract.py
    docker exec docker-analysis-1 python /workspace/bank_tags_contract.py --datasets sourcei
    python3 bank_tags_contract.py --pure-only        # fiftyone 없이 (호스트/CI)

정본: docker/analysis/bank_tags_contract.py
설계 근거: docs/superpowers/specs/2026-08-14-fiftyone-bank-filter-schema-design.md §3 D7
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import re
import sys

WORKSPACE = os.path.dirname(os.path.abspath(__file__))
# 배포본 우선 — App 이 실제로 실행하는 코드를 검사해야 의미가 있다. 없으면 저장소 사본.
PROBE_PATHS = [
    "/data/fiftyone/datasets/__plugins__/user-prompt-probe/__init__.py",
    os.path.join(WORKSPACE, "plugins", "user-prompt-probe", "__init__.py"),
]

# 경계 케이스 — 라이브 버전 목록과 무관하게 항상 검사한다.
#   v1.0.5.0 / v2.0.5.0  뒤 3파트만 쓰면 둘 다 v050 으로 붕괴 (vtag 주석 :147-150 의 사고)
#   v1.0.8               3파트라 vtag 와 구 표기가 v108 로 같아짐 → 중복 접기 필요
#   v1.0.8.4-prune205    큐레이션 버전명. digits-only 로 만들면 v1084205 로 갈라진다
EDGE_VERSIONS = ["v1.0.8.0", "v1.0.8.4", "v1.0.13.2", "v1.0.5.0", "v2.0.5.0",
                 "v1.0.8", "v1.0.8.4-prune205"]

# 소비자가 실제로 조회하는 필드 템플릿. 플레이스홀더 표기가 서로 다르다.
#   prompt_scores_export.resolve(schema, "winner_gidx_{v}", bank)
#   user-prompt-probe._pick_field(schema, "winner_gidx_{tag}", version)
PSE_TEMPLATES = ["pred_{v}", "pred_margin_{v}", "winner_gidx_{v}",
                 "vote_{v}", "vote_margin_{v}", "wave_pred_{v}"]
PROBE_TEMPLATES = ["winner_gidx_{tag}", "wave_iou_falldown_{tag}",
                   "wave_iou_fire_{tag}", "wave_iou_smoke_{tag}"]

FAILURES: list[str] = []
CHECKS = 0


def fail(msg: str) -> None:
    FAILURES.append(msg)
    print(f"  ❌ {msg}")


def ok(msg: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"  ✅ {msg}")


# ────────────────────── 소비자 로딩 ──────────────────────
def load_probe():
    """플러그인은 디렉토리명에 하이픈이 있어 일반 import 가 안 된다 — 파일 경로로 로드.

    호스트에는 numpy/fiftyone 이 없어 exec_module 이 죽는다. 검사기가 **환경 때문에**
    실패하면 안 되므로 fail-soft 로 넘기고, 건너뛴 사실을 출력에 남긴다
    (컨테이너에서 돌리면 정상 로드된다 — 거기가 실제 실행 환경이다).
    """
    for path in PROBE_PATHS:
        if not os.path.exists(path):
            continue
        spec = importlib.util.spec_from_file_location("_probe_under_test", path)
        mod = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(mod)
        except Exception as exc:  # noqa: BLE001
            return None, f"{path} (로드 실패: {type(exc).__name__}: {exc})"
        return mod, path
    return None, None


def load_pse():
    if WORKSPACE not in sys.path:
        sys.path.insert(0, WORKSPACE)
    import prompt_scores_export as pse
    return pse


def load_vtag():
    """생산자 정본. numpy 등 무거운 import 가 딸려오므로 실패해도 계속 진행한다."""
    if WORKSPACE not in sys.path:
        sys.path.insert(0, WORKSPACE)
    try:
        import prompt_geometry as pg
        return pg.vtag
    except Exception as exc:  # noqa: BLE001
        print(f"  ⚠️  prompt_geometry.vtag 로드 실패 ({type(exc).__name__}) — "
              f"내장 정의로 대체한다. 생산자 정본과의 일치는 이번 실행에서 검사되지 않는다.")
        return None


def vtag_reference(version: str) -> str:
    """prompt_geometry.vtag 와 문자 단위로 같아야 하는 참조 구현 (C0 이 이를 고정한다)."""
    return "v" + "".join(version.lstrip("vV").split("."))


# ────────────────────── 계약 ──────────────────────
def c0_producer_canon(vtag, versions) -> None:
    """C0: 참조 구현이 생산자 정본과 일치하는가. 여기가 어긋나면 나머지 검사가 무의미하다."""
    print("\n[C0] 생산자 정본 일치")
    if vtag is None:
        print("  ⏭  prompt_geometry 미로드 — 건너뜀")
        return
    bad = [v for v in versions if vtag(v) != vtag_reference(v)]
    if bad:
        fail(f"vtag() 와 참조 구현 불일치: {bad[:5]} "
             f"(예: vtag={vtag(bad[0])!r} vs ref={vtag_reference(bad[0])!r}) "
             f"— 생산자가 규칙을 바꿨다. 이 파일의 vtag_reference 를 먼저 맞출 것")
    else:
        ok(f"vtag() == vtag_reference, {len(versions)}개 버전")


def c1_no_collision(versions) -> None:
    """C1: 서로 다른 버전이 같은 태그로 붕괴하지 않는가 (vtag 주석 :147-150 사고의 회귀 검사)."""
    print("\n[C1] 태그 충돌 없음")
    seen: dict[str, str] = {}
    clash = []
    for v in versions:
        t = vtag_reference(v)
        if t in seen and seen[t] != v:
            clash.append((seen[t], v, t))
        seen[t] = v
    if clash:
        fail(f"서로 다른 버전이 같은 vtag 로 붕괴: {clash[:5]}")
    else:
        ok(f"{len(versions)}개 버전 → 고유 태그 {len(seen)}개")


def c2_producer_reachable(pse, probe, versions) -> None:
    """C2: 각 소비자의 후보 목록이 **생산자 현행 태그**를 포함하는가.

    M6 이 고친 것이 정확히 이 계약이다 — `suffixes()` 에 vtag 후보가 없어
    `winner_gidx_v1080` 을 영원히 못 찾았다.
    """
    print("\n[C2] 소비자가 생산자 현행 태그에 도달 가능")
    for name, fn in (("prompt_scores_export.suffixes", lambda v: pse.suffixes(v)),
                     ("user-prompt-probe._ver_tags", (lambda v: probe._ver_tags(v)) if probe else None)):
        if fn is None:
            print(f"  ⏭  {name} — 소비자 미로드, 건너뜀")
            continue
        missing = [v for v in versions if vtag_reference(v) not in fn(v)]
        if missing:
            fail(f"{name}: 생산자 태그 미포함 {len(missing)}건 — "
                 f"예 {missing[0]!r} → 후보 {fn(missing[0])}, 필요 {vtag_reference(missing[0])!r}")
        else:
            ok(f"{name}: {len(versions)}개 버전 전부 vtag 후보 보유")


def c3_fallback(pse, probe) -> None:
    """C3: 구 슬러그만 존재하는 데이터셋에서도 여전히 해석되는가 (합성 스키마)."""
    print("\n[C3] 구 슬러그 폴백 유효")
    cases = [
        ("신 슬러그만", {"winner_gidx_v1080"}, "winner_gidx_v1080"),
        ("구 슬러그만", {"winner_gidx_v080"}, "winner_gidx_v080"),
        ("둘 다 → 신 우선", {"winner_gidx_v1080", "winner_gidx_v080"}, "winner_gidx_v1080"),
    ]
    for label, schema, want in cases:
        got = pse.resolve(schema, "winner_gidx_{v}", "v1.0.8.0")
        if got != want:
            fail(f"pse.resolve [{label}]: {got!r} != {want!r}")
        else:
            ok(f"pse.resolve [{label}] → {got}")
        if probe:
            got = probe._pick_field(schema, "winner_gidx_{tag}", "v1.0.8.0")
            if got != want:
                fail(f"probe._pick_field [{label}]: {got!r} != {want!r}")
            else:
                ok(f"probe._pick_field [{label}] → {got}")


def c4_live_resolution(pse, probe, dataset_names) -> None:
    """C4 (핵심): 스키마에 필드가 **실재하면** 소비자가 반드시 찾아야 한다.

    어느 세대 표기로 쓰였든 상관없다 — 있으면 찾아야 한다. M6 버그를 잡았을 검사이며,
    합성 케이스가 아니라 실제 데이터셋을 본다는 점에서 C2/C3 보다 강하다.
    """
    print("\n[C4] 라이브 스키마 도달성")
    try:
        import fiftyone as fo
    except Exception as exc:  # noqa: BLE001
        print(f"  ⏭  fiftyone 미가용 ({type(exc).__name__}) — 건너뜀 (--pure-only 와 동일)")
        return

    # 필드명에서 (계열, 태그) 를 뽑는다. 태그 → 버전 역변환은 모호하므로 하지 않고,
    # "이 계열에 존재하는 태그 집합" 과 "소비자가 만들어낸 후보" 의 교집합으로 판정한다.
    tag_re = re.compile(r"^(?P<fam>.+?)_(?P<tag>v[\d_]+(?:-[\w]+)?)$")

    for name in dataset_names:
        # ⚠️ 요청받은 데이터셋을 못 보면 **실패**다. 조용히 건너뛰고 초록으로 끝나면
        #    경계 케이스 7개만 보고 "통과" 를 보고하게 되는데, 그게 바로 이 파일이
        #    막으려는 실패 유형이다 (약한 검사로의 무증상 후퇴).
        try:
            ds = fo.load_dataset(name)
        except Exception as exc:  # noqa: BLE001
            fail(f"{name}: 로드 실패 ({type(exc).__name__}: {exc}) — 검사하지 못했다")
            continue
        schema = set(ds.get_field_schema())
        versions = live_versions(name)
        if not versions:
            fail(f"{name}: 버전 목록이 비었다 ({name}-prompts 의 bank_version) — 검사하지 못했다")
            continue

        fam_tags: dict[str, set[str]] = {}
        for f in schema:
            m = tag_re.match(f)
            if m:
                fam_tags.setdefault(m.group("fam"), set()).add(m.group("tag"))

        checked = unreachable = 0
        for tmpl, resolver in ([(t, lambda tm, v: pse.resolve(schema, tm, v)) for t in PSE_TEMPLATES]
                               + ([(t, lambda tm, v: probe._pick_field(schema, tm, v))
                                   for t in PROBE_TEMPLATES] if probe else [])):
            fam = tmpl.rsplit("_", 1)[0]          # "winner_gidx_{v}" → "winner_gidx"
            have = fam_tags.get(fam, set())
            if not have:
                continue                           # 이 계열은 이 데이터셋에 아예 없다 — 계약 무관
            for v in versions:
                cand = set(pse.suffixes(v)) | (set(probe._ver_tags(v)) if probe else set())
                if not (cand & have):
                    continue                       # 이 버전은 이 계열에 필드가 없다 — 정상
                checked += 1
                if resolver(tmpl, v) is None:
                    unreachable += 1
                    if unreachable <= 3:
                        fail(f"{name}: {fam}_<{sorted(cand & have)[0]}> 는 실재하는데 "
                             f"{tmpl!r} 이 {v} 를 해석하지 못한다")
        if unreachable:
            fail(f"{name}: 도달 불가 {unreachable}/{checked}건")
        else:
            ok(f"{name}: 실재 필드 {checked}건 전부 도달 가능")


def live_versions(dataset_name: str) -> list[str]:
    """`<dataset>-prompts` 의 distinct bank_version.

    `ds.distinct()` 는 이 크기(60만 문장)에서 Mongo 의 100MB $addToSet 한도를 넘긴다 —
    실측 확인. 디스크 스필을 허용하는 $group 집계로 우회한다.
    """
    import fiftyone as fo
    try:
        p = fo.load_dataset(f"{dataset_name}-prompts")
    except Exception:  # noqa: BLE001
        return []
    cur = p._sample_collection.aggregate(
        [{"$group": {"_id": "$bank_version.label"}}], allowDiskUse=True)
    return sorted(r["_id"] for r in cur if r["_id"])


# ────────────────────── main ──────────────────────
def main() -> int:
    ap = argparse.ArgumentParser(description="뱅크 버전 태그 해석 계약 검사")
    ap.add_argument("--datasets", nargs="*", default=["sourcei", "source-h"])
    ap.add_argument("--pure-only", action="store_true",
                    help="라이브 스키마 검사(C4) 생략 — fiftyone 없는 호스트/CI 용")
    args = ap.parse_args()

    pse = load_pse()
    probe, probe_path = load_probe()
    print(f"소비자: prompt_scores_export ✓ / user-prompt-probe "
          f"{'✓ ' + probe_path if probe else '✗ ' + str(probe_path) + ' — 해당 검사 생략'}")
    vtag = load_vtag()

    versions = EDGE_VERSIONS[:]
    if not args.pure_only:
        try:
            for name in args.datasets:
                versions += live_versions(name)
        except Exception as exc:  # noqa: BLE001
            print(f"  ⚠️  라이브 버전 목록 수집 실패 ({type(exc).__name__}) — 경계 케이스만 검사")
    versions = sorted(set(versions))
    print(f"검사 버전 {len(versions)}개 (경계 {len(EDGE_VERSIONS)} + 라이브)")

    c0_producer_canon(vtag, versions)
    c1_no_collision(versions)
    c2_producer_reachable(pse, probe, versions)
    c3_fallback(pse, probe)
    if not args.pure_only:
        c4_live_resolution(pse, probe, args.datasets)

    print()
    if FAILURES:
        print(f"❌ 계약 위반 {len(FAILURES)}건 / 통과 {CHECKS}건")
        for m in FAILURES:
            print(f"   · {m}")
        return 1
    print(f"✅ 계약 통과 {CHECKS}건 — 버전 {len(versions)}개")
    return 0


if __name__ == "__main__":
    sys.exit(main())
