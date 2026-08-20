#!/usr/bin/env python3
"""FiftyOne App 번들의 modalSelector 가드 패치 (재적용 가능).

증상: 뒤로/앞으로·URL 전환·모달 닫기 직후
    TypeError: Cannot read properties of undefined (reading 'id')
        at variables (.../assets/index-<hash>.js:699:118171)

원인: Relay 쿼리의 `variables` 가 modalSelector 를 **엄격 비교로만** 막는다.

    variables:({get:Ot})=>{
      const mt=Ot(modalSelector);
      if(mt===null)return null;              // ← null 만 걸러낸다
      ...
      return {..., filter:{id:mt.id, ...}}   // ← mt 가 undefined 면 여기서 throw
    }

모달이 비는 순간 이 selector 는 `undefined` 를 돌려줄 수 있는데 가드는 `null` 만 본다.
`==` (느슨한 비교)로 바꾸면 null·undefined 를 함께 막는다 — 반환값은 원래대로 `null`
이라 Relay 는 "쿼리 스킵"으로 읽고, 동작 변화는 크래시가 안 나는 것뿐이다.

⚠️ **전량 치환 절대 금지.** `if(<x>===null)return null` 꼴은 번들 곳곳에 있다 —
   실측(2026-08-20, fiftyone 1.19.0)으로 확인한 것만:
     · three.js  `Math.log2(mt)`        → mt 는 숫자
     · React 리컨실러 `finishedWork`     → 건드리면 렌더러가 깨진다
     · headlessui `headlessui-portal-root` → Ot 는 document
   "가드 뒤에 역참조가 보이면 대상" 같은 휴리스틱도 위 셋을 다 잡아버린다(7곳 오탐).
   그래서 **값의 출처가 `modalSelector` 인 자리만** 고친다 — 바로 앞에
   `const <x>=<get>(modalSelector);` 가 붙어 있는 형태. 이 조건으로 정확히 2곳이다.

⚠️ **이 패치는 컨테이너 레이어에 남는다 — 이미지 재빌드/force-recreate 하면 사라진다.**
   `restart` 로는 살아남는다. 사라지면 크래시가 다시 나므로 그때 다시 돌린다.
   (2026-08-20 실측: 예전에 적용했다는 패치가 남아 있지 않았다.)

사용:
    python3 patch_fiftyone_bundle.py                      # dry-run (기본)
    python3 patch_fiftyone_bundle.py --apply
    python3 patch_fiftyone_bundle.py selftest

정본: docker/analysis/patch_fiftyone_bundle.py
"""
from __future__ import annotations

import argparse
import glob
import re
import sys

ASSETS = ("/usr/local/lib/python3.11/site-packages/fiftyone/server/static/assets")
# `const <x> = <get>(modalSelector);if(<x>===null)return null`
# 식별자 이름은 minify 라 빌드마다 바뀐다 — 역참조(\2)로 같은 이름임을 강제한다.
GUARD = re.compile(r"(const (\w+)=\w+\(modalSelector\);)if\(\2===null\)return null")
# 이미 패치된 자리 (멱등성 판정용)
DONE = re.compile(r"const (\w+)=\w+\(modalSelector\);if\(\1==null\)return null")


def patch_text(src: str) -> "tuple[str, int, int]":
    """(패치된 텍스트, 고친 수, 이미 돼 있던 수)."""
    already = len(DONE.findall(src))
    new, fixed = GUARD.subn(lambda m: f"{m.group(1)}if({m.group(2)}==null)return null", src)
    return new, fixed, already


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--assets", default=ASSETS)
    ap.add_argument("--apply", action="store_true", help="실제 쓰기 (기본 dry-run)")
    args = ap.parse_args(argv)

    files = sorted(glob.glob(f"{args.assets}/index-*.js"))
    if not files:
        print(f"❌ 번들 없음: {args.assets}/index-*.js")
        return 2
    total_fixed = 0
    for path in files:
        with open(path, encoding="utf-8", errors="surrogateescape") as fh:
            src = fh.read()
        new, fixed, already = patch_text(src)
        if not fixed:
            if already:
                print(f"  이미 패치됨: {path.rsplit('/', 1)[-1]}  {already}곳")
            continue
        total_fixed += fixed
        mark = "적용" if args.apply else "대상(dry-run)"
        print(f"  {mark}: {path.rsplit('/', 1)[-1]}  고침 {fixed} (기적용 {already})")
        if args.apply:
            with open(path, "w", encoding="utf-8", errors="surrogateescape") as fh:
                fh.write(new)
    if not total_fixed:
        print("✅ 이미 패치돼 있음 (고칠 자리 없음)")
        return 0
    if not args.apply:
        print(f"→ 총 {total_fixed}곳. 실제로 고치려면 --apply. 이후 브라우저 하드 새로고침.")
        return 0
    print(f"✅ {total_fixed}곳 패치 완료 — 브라우저 하드 새로고침(Ctrl+Shift+R) 필요")
    return 0


def selftest() -> int:
    # ① modalSelector 에서 온 값은 고친다
    a = 'variables:({get:Ot})=>{const mt=Ot(modalSelector);if(mt===null)return null;' \
        'return{filter:{id:mt.id}}}'
    got, fixed, already = patch_text(a)
    assert (fixed, already) == (1, 0), (fixed, already)
    assert "const mt=Ot(modalSelector);if(mt==null)return null" in got, got

    # ② 남의 코드는 절대 안 건드린다 — 실측으로 오탐했던 세 종류를 그대로 박아 둔다.
    #    (느슨한 휴리스틱이면 여기서 전부 깨진다 = 앱 전체 장애)
    for name, other in (
        ("three.js", 'function x(mt){if(mt===null)return null;const Lt=Math.log2(mt)-2}'),
        ("React", 'var bx=il.finishedWork;if(bx===null)return null;if(bx===il.current)throw 1'),
        ("headlessui", 'let Ot=q?.getElementById("headlessui-portal-root");'
                       'if(Ot===null)return null;Ot.body.appendChild(sr)'),
    ):
        got_o, fixed_o, _ = patch_text(other)
        assert (fixed_o, got_o) == (0, other), name

    # ③ 멱등 — 이미 패치된 텍스트는 더 안 고치고, 기적용으로 센다
    once = patch_text(a)[0]
    got2, fixed2, already2 = patch_text(once)
    assert (fixed2, already2, got2) == (0, 1, once), (fixed2, already2)

    # ④ minify 식별자가 바뀌어도 따라간다 (번들 재빌드마다 이름이 변한다)
    c = 'const q7=zz(modalSelector);if(q7===null)return null;return{id:q7.id}'
    got, fixed, _ = patch_text(c)
    assert fixed == 1 and "if(q7==null)return null" in got, got

    # ⑤ 이름이 어긋나면(다른 변수의 가드) 고치지 않는다
    d = 'const mt=Ot(modalSelector);if(zz===null)return null;return{id:mt.id}'
    assert patch_text(d)[1] == 0, d
    print("selftest OK")
    return 0


if __name__ == "__main__":
    sys.exit(selftest() if sys.argv[1:2] == ["selftest"] else main())
