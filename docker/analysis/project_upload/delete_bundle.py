"""업로드 번들 디렉토리 + 그 번들로 만든 FiftyOne 데이터셋을 **한 쌍으로** 삭제한다.

왜 한 쌍인가: FiftyOne 은 이미지를 복사 없이 **제자리 참조**한다(UPLOAD_SPEC.md §1) —
번들만 지우면 데이터셋은 남지만 모든 썸네일이 깨진 좀비가 된다. 그래서 이 스크립트는
"번들 지우기" 를 **번들 + 그 번들을 가리키는 upload_kit 데이터셋 전부** 로 정의한다.

대상 판정은 **이름이 아니라 marker** 다 — `ingest_bundle.py --name` 으로 데이터셋 이름을
바꿔 넣었으면 이름이 안 맞는다. `ds.info["upload_kit"]["bundle"]` 의 realpath 가 이 번들과
같은 데이터셋만 지운다. marker 가 없는 데이터셋(sourcei·frames 등)은 이름이 우연히
같아도 **절대** 건드리지 않는다 (ingest_bundle._check_conflict 와 같은 보호 원칙).

지우지 않는 것: Postgres 에 등록된 프롬프트 뱅크(`register_bank_db.py` 로 넣은 것). 뱅크는
전역 레지스트리라 다른 데이터셋도 참조하며, `origin_uri` 가 사라진 번들을 가리키게 될 뿐
동작에는 영향이 없다.

    python3 delete_bundle.py source-n dtro2            # 계획만 (기본 dry-run)
    python3 delete_bundle.py source-n --apply
    python3 delete_bundle.py source-n --apply --json   # sync_api 가 쓰는 형식
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys

import bundle_common as bc


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("bundles", nargs="*", help="번들 이름 (UPLOAD_ROOT 바로 아래 디렉토리명)")
    p.add_argument("--apply", action="store_true", help="실제 삭제 (기본 dry-run)")
    p.add_argument("--json", action="store_true", help="마지막 줄에 JSON 결과 (API 소비용)")
    p.add_argument("--selftest", action="store_true")
    return p.parse_args()


def resolve_bundle(name: str) -> str:
    """번들 이름 → 절대경로. **UPLOAD_ROOT 의 직계 자식**만 허용 (경로 탈출·루트 자체 삭제 차단).

    `_resolve_bundle_dir`(sync_api) 는 하위 경로도 허용하지만, 삭제는 되돌릴 수 없으므로
    여기서는 더 좁게 잠근다 — `.incoming` 같은 점 접두 내부 디렉토리도 거부한다.
    """
    root = os.path.realpath(bc.UPLOAD_ROOT)
    real = os.path.realpath(os.path.join(bc.UPLOAD_ROOT, name))
    if os.path.dirname(real) != root or os.path.basename(real).startswith("."):
        raise bc.BundleError(f"번들 이름이 아님(업로드 루트 직계만 허용): {name!r} → {real}")
    if not os.path.isdir(real):
        raise bc.BundleError(f"번들 디렉토리가 없음: {real}")
    return real


def match_datasets(bundle_real: str, markers: dict[str, dict | None]) -> tuple[list[str], list[str]]:
    """(지울 데이터셋, 보호된 데이터셋). **FiftyOne 비의존 순수함수** (selftest 대상).

    markers: 데이터셋 이름 → `ds.info.get("upload_kit")` (marker 없으면 None).
    보호 = 이름은 번들과 같은데 marker 가 없는 것 — 다른 사람이 만든 동명 자산일 수 있다.
    """
    base = os.path.basename(bundle_real)
    victims, protected = [], []
    for name, marker in sorted(markers.items()):
        if not marker:
            if name == base or name == base + bc.PROMPTS_SUFFIX:
                protected.append(name)
            continue
        if os.path.realpath(str(marker.get("bundle") or "")) == bundle_real:
            victims.append(name)
    return victims, protected


def run(names: list[str], apply_: bool) -> dict:
    import fiftyone as fo

    markers = {n: (fo.load_dataset(n).info or {}).get("upload_kit") for n in fo.list_datasets()}
    results = []
    for name in names:
        # per-bundle fail-forward — 다중 선택 삭제에서 하나가 없다고 나머지를 취소하지 않는다
        # (repo 공통 정책. 옛 버전은 첫 실패에서 전체가 중단됐다).
        try:
            real = resolve_bundle(name)
        except bc.BundleError as exc:
            print(f"[건너뜀] {name}: {exc}")
            results.append({"bundle": name, "path": None, "datasets": [], "protected": [],
                            "bytes": 0, "deleted": False, "error": str(exc)})
            continue
        victims, protected = match_datasets(real, markers)
        size = sum(os.path.getsize(os.path.join(d, f))
                   for d, _, fs in os.walk(real) for f in fs
                   if os.path.exists(os.path.join(d, f)))
        item = {"bundle": name, "path": real, "datasets": victims,
                "protected": protected, "bytes": size, "deleted": False}
        print(f"[계획] {name}: 데이터셋 {victims or '없음'} · {size / 1048576:.1f}MB · {real}")
        for p in protected:
            print(f"    ⚠️ 보호(marker 없음, 건드리지 않음): {p}")
        if apply_:
            try:
                # 데이터셋 먼저, 디렉토리 나중 — 반대로 하면 디렉토리 삭제가 실패했을 때
                # 데이터셋만 사라져 재인제스트도 못 하는 상태가 된다.
                for ds_name in victims:
                    fo.delete_dataset(ds_name)
                    print(f"[삭제] 데이터셋 {ds_name}")
                shutil.rmtree(real)
                print(f"[삭제] 디렉토리 {real}")
                item["deleted"] = True
            except Exception as exc:  # noqa: BLE001 — 한 번들의 실패가 나머지를 막지 않게
                item["error"] = f"{type(exc).__name__}: {exc}"
                print(f"[실패] {name}: {item['error']}")
        results.append(item)
    errs = [i for i in results if i.get("error")]
    return {"ok": not errs, "applied": apply_, "items": results,
            "bytes": sum(i["bytes"] for i in results),
            "datasets": [d for i in results for d in i["datasets"] if i["deleted"]],
            "errors": [f"{i['bundle']}: {i['error']}" for i in errs]}


def selftest() -> int:
    B = "/data/fiftyone/uploads/dtro2"
    markers = {
        "dtro2": {"bundle": B}, "dtro2-prompts": {"bundle": B},
        "renamed": {"bundle": B},                       # --name 으로 이름만 바뀐 경우도 잡힌다
        "other": {"bundle": "/data/fiftyone/uploads/other"},
        "sourcei": None, "frames": None,
    }
    victims, protected = match_datasets(B, markers)
    assert victims == ["dtro2", "dtro2-prompts", "renamed"], victims
    assert protected == [], protected

    # marker 없는 동명 자산은 지우지 않고 '보호'로 보고한다
    v2, p2 = match_datasets(B, {"dtro2": None, "dtro2-prompts": None, "x": {"bundle": B}})
    assert v2 == ["x"] and p2 == ["dtro2", "dtro2-prompts"], (v2, p2)

    # marker 의 bundle 이 다른 번들이면 대상 아님
    assert match_datasets("/data/fiftyone/uploads/other", markers)[0] == ["other"]

    root = os.path.realpath(bc.UPLOAD_ROOT)
    for bad in ("..", "../etc", ".incoming", "a/b", "", "."):
        try:
            resolve_bundle(bad)
        except bc.BundleError:
            continue
        except Exception as exc:  # noqa: BLE001
            raise AssertionError(f"{bad!r}: BundleError 가 아닌 {type(exc).__name__}") from exc
        raise AssertionError(f"{bad!r} 이 거부되지 않았다 (root={root})")
    print("selftest OK")
    return 0


def main() -> int:
    args = parse_args()
    if args.selftest:
        return selftest()
    if not args.bundles:
        raise SystemExit("[실패] 번들 이름이 필요합니다")
    try:
        out = run(args.bundles, args.apply)
    except Exception as exc:  # noqa: BLE001 — FiftyOne/Mongo 장애도 JSON 계약을 지켜 API 가 읽게
        print(f"[실패] {type(exc).__name__}: {exc}")
        if args.json:
            print(json.dumps({"ok": False, "errors": [f"{type(exc).__name__}: {exc}"]}, ensure_ascii=False))
        return 1
    if not args.apply:
        print("\nDRY-RUN — --apply 로 실제 삭제")
    if args.json:
        print(json.dumps(out, ensure_ascii=False))
    return 0 if out["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
