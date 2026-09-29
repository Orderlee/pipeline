"""BANK_LIST 순서 = **gidx 블록 배정**. 정렬이 아니라 **이미 박힌 배정**에서 만든다.

왜 이 모듈이 따로 있나 (2026-09-09)
------------------------------------
`BANK_LIST` 의 **위치**가 곧 gidx 블록이다 (`prompt_geometry`: `gidx = BANKS.index(v) *
GIDX_OFFSET + local`, OFFSET=100,000). 그런데 네 곳이 각자 `glob → int() semantic sort` 로
그 순서를 **매번 다시 계산**하고 있었다. 정렬로 만든 순서는 입력이 바뀌면 같이 바뀌므로,
**이미 저장된 gidx 를 소급해서 다른 문장에 가리키게 만든다** — 2026-08-20 에 실제로 났던
사고다(`v1.0.8.0` 이 한 실행에선 블록 0, `frames-prompts` 에선 18 → 프레임↔문장 조인이
`v1.0.1.0` 문장에 조용히 붙음. 개수는 맞고 정체가 틀림).

그래서 이 모듈의 계약은 하나다: **이미 배정된 버전은 절대 움직이지 않는다.**
새 버전만 뒤에 붙인다(append-only). 근거는 정렬표가 아니라 `<ds>-prompts` 에 **실제로 박혀
있는 블록**이다 — `prompt_geometry.gidx_offset_for()` 가 이미 같은 원칙을 쓴다("문장 쪽이
정본").

⚠️ 위치 리스트로는 **빈 블록을 표현할 수 없다.** 실측(2026-09-09):
  frames-prompts       29버전 블록 0..28, 갭 없음          → 리스트로 표현 가능 ✅
  sourcei-prompts      31버전, vOPT=29, **블록 30 비어 있음**, vGEN=31  → 표현 불가 ❌
  sourcei-OPT-prompts  vOPT=9, 블록 0..8 비어 있음                      → 표현 불가 ❌
sourcei 쪽 갭은 `sourcei_optbank_register.py`(GIDX0=2,900,000)와
`genfull_register.py`(GIDX0=3,100,000)가 블록을 손으로 박아 생긴 것이다. 그런 데이터셋에
정렬로 만든 리스트를 들이대면 vGEN 이 31→30 으로 **압축**되어 저장된 31개
`winner_gidx_*` 가 전부 어긋난다. 그래서 갭이 있으면 **조용히 다른 순서를 주지 않고 죽는다.**

`prompt_geometry` 를 import 하지 않는다 — 그 모듈은 import 시점에 env 의존 전역
(`VERSIONS`/`BANKS`)을 굳히고 무거운 분석 의존성을 끌어온다. 여기서 부르면 순환이 된다.
"""
from __future__ import annotations

import glob
import os

GIDX_OFFSET = 100_000     # prompt_geometry.GIDX_OFFSET 미러 (사본 동기화)


def strip_v(version: str) -> str:
    """선행 `v` **한 글자만** 떼어낸다.

    ⚠️ `lstrip("v")` 를 쓰면 안 된다 (codex 리뷰 2026-09-08): `lstrip` 은 **문자 집합을
    반복 제거**한다 — `"vv1.2".lstrip("v") == "1.2"` 라 `v1.2` 와 구분이 사라지고,
    본문이 `v` 로 시작하는 이름(`"vvery.1"` → `"ery.1"`)은 글자가 잘려나간다.
    """
    return version[1:] if version.startswith("v") else version


def natural_key(version: str):
    """숫자 버전 먼저(숫자 순), 그 외는 뒤에(이름 순). **새 버전 정렬에만** 쓴다.

    `v1.0.8.4` < `v1.0.10.3` 처럼 숫자 순서를 지키고(문자열 정렬이면 10 < 8 로 뒤집힌다),
    `vGEN.2026.08.28` 같은 불투명 이름은 int 파서에 밀어 넣지 않고 뒤로 보낸다 —
    현행 `int(x)` 는 여기서 `ValueError: invalid literal for int() with base 10: 'GEN'`
    로 죽어 `sync_prompts` 가 매 tick 실패했다(2026-09-09 실측).

    ⚠️ **이미 배정된 버전의 순서를 이 키로 다시 매기면 안 된다.** 그건 이 모듈이 막으려는
    바로 그 사고다. 오직 `bank_list()` 가 미배정 신규분을 뒤에 붙일 때만 쓴다.
    """
    parts = strip_v(version).split(".")
    try:
        return (0, tuple(int(x) for x in parts), ())
    except ValueError:
        return (1, (), tuple(parts))


def versions_on_disk(npz_dir: str) -> list[str]:
    """`<npz_dir>/v*.npz` 의 버전 이름들 (순서 미정 — 순서는 `bank_list()` 가 정한다)."""
    return [os.path.basename(p)[:-4] for p in glob.glob(os.path.join(npz_dir, "v*.npz"))]


def assigned_blocks(prompts_dataset: str) -> dict[str, int]:
    """`<ds>-prompts` 에 **실제로 박혀 있는** {버전: 블록}. 없으면 빈 dict.

    한 버전이 여러 블록에 걸쳐 있으면 (손으로 섞였거나 실패한 리빌드 잔재) 그 자체가
    사고 신호라 예외를 낸다 — 조용히 하나를 고르면 나머지 절반이 미아가 된다.
    """
    import fiftyone as fo

    if not fo.dataset_exists(prompts_dataset):
        return {}
    p = fo.load_dataset(prompts_dataset)
    sch = p.get_field_schema()
    if "gidx" not in sch or "bank_version" not in sch:
        return {}
    out: dict[str, int] = {}
    for v in sorted(x for x in p.distinct("bank_version.label") if x):
        lo, hi = p.match(fo.ViewField("bank_version.label") == v).bounds("gidx")
        if lo is None or hi is None:
            continue
        b_lo, b_hi = int(lo) // GIDX_OFFSET, int(hi) // GIDX_OFFSET
        if b_lo != b_hi:
            raise SystemExit(
                f"{prompts_dataset}: {v} 가 블록 {b_lo}~{b_hi} 에 걸쳐 있다 — gidx 배정이 "
                f"이미 깨졌다. BANK_LIST 를 새로 만들면 그 위에 덮어쓰게 되므로 중단한다.")
        out[v] = b_lo
    return out


def bank_list(npz_dir: str, prompts_dataset: str) -> list[str]:
    """BANK_LIST 로 쓸 순서. **기존 배정 보존 + 신규만 뒤에 append.**

    반환 리스트의 인덱스가 곧 gidx 블록이므로, 기존 버전은 반드시 자기 블록 자리에 온다.
    갭이 있어 위치 리스트로 표현할 수 없으면 **죽는다**(모듈 docstring 의 sourcei 사례).
    """
    disk = versions_on_disk(npz_dir)
    if not disk:
        return []
    assigned = assigned_blocks(prompts_dataset)

    if not assigned:            # 부트스트랩 — 아직 아무것도 안 박혔다. 정렬로 정해도 안전.
        return sorted(disk, key=natural_key)

    gaps = sorted(set(range(max(assigned.values()) + 1)) - set(assigned.values()))
    if gaps:
        raise SystemExit(
            f"{prompts_dataset}: 블록 {gaps} 가 비어 있어 위치 기반 BANK_LIST 로 기존 배정을 "
            f"재현할 수 없다 (현재 배정: {sorted(assigned.items(), key=lambda kv: kv[1])}). "
            f"정렬로 만든 리스트를 쓰면 뒤쪽 버전이 앞으로 **압축**되어 이미 저장된 "
            f"winner_gidx_* 가 전부 다른 문장을 가리킨다. 손으로 블록을 박은 스크립트"
            f"(sourcei_optbank_register.py / genfull_register.py)가 만든 갭이라면, 이 "
            f"데이터셋은 전량 리빌드 대신 그 스크립트의 배정을 유지해야 한다.")

    ordered = [None] * (max(assigned.values()) + 1)
    for v, b in assigned.items():
        ordered[b] = v
    missing = [v for v in ordered if v is not None and v not in disk]
    if missing:
        raise SystemExit(
            f"{prompts_dataset}: 이미 배정된 {missing} 의 npz 가 {npz_dir} 에 없다 — 그대로 "
            f"리빌드하면 그 버전 문장이 삭제되고 블록이 앞으로 당겨진다. npz 를 복구하거나 "
            f"대상 데이터셋을 다시 정할 것.")
    fresh = sorted(set(disk) - set(assigned), key=natural_key)
    return [v for v in ordered if v is not None] + fresh


def assert_blocks_preserved(order: list[str], prompts_dataset: str) -> None:
    """리빌드 **전에** 부르는 안전핀: 이 순서가 기존 배정을 한 칸도 안 옮기는지 확인.

    `stage_promptmap` 은 `overwrite=True` 로 데이터셋을 통째로 다시 만든다 — 잘못된 순서로
    들어가면 되돌릴 수 없다. 그래서 파괴 직전에 한 번 더 못을 박는다.
    """
    assigned = assigned_blocks(prompts_dataset)
    moved = {v: (b, order.index(v) if v in order else None)
             for v, b in assigned.items() if order[b:b + 1] != [v]}
    if moved:
        raise SystemExit(
            f"{prompts_dataset}: 이 BANK_LIST 는 기존 gidx 블록을 옮긴다 "
            f"{{버전: (기존, 새것)}} = {moved}. 저장된 winner_gidx_* 가 다른 문장을 가리키게 "
            f"되므로 중단한다.")


def selftest() -> None:
    """오프라인 계약 검증 (FiftyOne 불필요). `python3 bank_blocks.py` 로 실행."""
    assert strip_v("v1.0.8.4") == "1.0.8.4"
    assert strip_v("vv1.2") == "v1.2", "회귀: lstrip 처럼 v 를 여러 개 떼면 버전이 아려진다"
    assert strip_v("GEN.1") == "GEN.1"

    # 숫자 순서가 문자열 순서를 이겨야 한다 (8 < 10)
    nums = ["v1.0.10.3", "v1.0.8.4", "v1.0.2.0"]
    assert sorted(nums, key=natural_key) == ["v1.0.2.0", "v1.0.8.4", "v1.0.10.3"]
    # 불투명 이름은 int 파서에 안 들어가고 뒤로 (현행 코드가 죽던 지점)
    mixed = ["vGEN.2026.08.28", "v1.0.8.4", "vOPT.2026.08.28", "v1.0.10.3"]
    assert sorted(mixed, key=natural_key) == [
        "v1.0.8.4", "v1.0.10.3", "vGEN.2026.08.28", "vOPT.2026.08.28"]

    # append-only 계약: 기존 배정은 자리를 지키고 신규만 뒤에 붙는다
    import unittest.mock as m
    live = {"v1.0.2.0": 0, "v1.0.8.4": 1, "v1.0.10.3": 2}
    disk = ["v1.0.10.3", "vGEN.2026.08.28", "v1.0.2.0", "v1.0.8.4", "vOPT.2026.08.28"]
    with m.patch(f"{__name__}.versions_on_disk", return_value=disk), \
            m.patch(f"{__name__}.assigned_blocks", return_value=live):
        got = bank_list("/x", "d-prompts")
    assert got == ["v1.0.2.0", "v1.0.8.4", "v1.0.10.3",
                   "vGEN.2026.08.28", "vOPT.2026.08.28"], got

    # ⛔ 갭이 있으면 조용히 압축하지 말고 죽어야 한다 (sourcei vOPT=29 / 갭 30 / vGEN=31)
    gapped = {"v1.0.2.0": 0, "vOPT.2026.08.28": 1, "vGEN.2026.08.28": 3}
    with m.patch(f"{__name__}.versions_on_disk", return_value=list(gapped)), \
            m.patch(f"{__name__}.assigned_blocks", return_value=gapped):
        try:
            bank_list("/x", "d-prompts")
        except SystemExit as e:
            assert "비어 있어" in str(e), e
        else:
            raise AssertionError("회귀: 갭이 있는데 리스트를 만들어 줬다 — 블록이 압축된다")

    # 배정된 버전의 npz 가 사라지면 죽어야 한다 (남은 버전이 앞으로 당겨지므로)
    with m.patch(f"{__name__}.versions_on_disk", return_value=["v1.0.2.0"]), \
            m.patch(f"{__name__}.assigned_blocks", return_value={"v1.0.2.0": 0, "v1.0.8.4": 1}):
        try:
            bank_list("/x", "d-prompts")
        except SystemExit as e:
            assert "npz 가" in str(e), e
        else:
            raise AssertionError("회귀: 배정된 버전의 npz 가 없는데 통과시켰다")

    # 부트스트랩(아직 아무것도 안 박힘)은 정렬로 정해도 안전
    with m.patch(f"{__name__}.versions_on_disk", return_value=mixed), \
            m.patch(f"{__name__}.assigned_blocks", return_value={}):
        assert bank_list("/x", "none-prompts") == sorted(mixed, key=natural_key)

    print("bank_blocks selftest OK")


if __name__ == "__main__":
    selftest()
