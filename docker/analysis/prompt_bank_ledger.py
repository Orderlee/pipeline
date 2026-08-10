#!/usr/bin/env python3
"""프롬프트 뱅크 정본 추출기 — userwatch 공급물 → 019 스키마 정렬 원장.

Phase 1.5(거버넌스 게이트) 산출물. **DB 를 건드리지 않는다** — 019/020 마이그레이션이
prod 에 적용되는 시점(Phase 2 스파이크 go, 스펙 §6.2)에 이 스크립트의 JSONL 을 그대로
읽어 넣는 얇은 loader 만 추가하면 되도록 컬럼을 미리 맞춰 둔다.

왜 필요한가 (실측):
  * userwatch NAS 52버전 중 34개만 CSV(텍스트) 보유. 14개는 JSON 에 `"prompt": null`
    (벡터만), 4개는 빈 폴더.
  * 그런데 v1.0.8.4 는 NAS 에 CSV 가 없는데도 analysis 컨테이너 `/data` 에 사본이 있다 —
    즉 "우리가 쓰는 뱅크의 텍스트 유일본이 gitignore 된 바인드 볼륨에 있다". 그 사본까지
    긁어야 실제 보유 범위가 나온다 → `--extra` 루트.

산출물 3종 (`--out DIR`):
  banks_inventory.json    버전별 카탈로그 — 019 `prompt_banks` 컬럼 정렬
  bank_sentences.jsonl    뱅크×문장 행 — 019 `bank_sentences` + §6.2 멤버십(gidx) 정렬
  unique_sentences.jsonl  중복 제거된 고유 문장 — pgvector 적재 대상. **클래스 없음**
                          (같은 문장이 뱅크마다 다른 클래스를 갖는 실측 2,106건 때문)

사용:
  python3 prompt_bank_ledger.py inventory              # 기본 dry-run, 표만 출력
  python3 prompt_bank_ledger.py ledger --out /tmp/bank
  python3 prompt_bank_ledger.py selftest               # 파일 없이도 도는 불변식 검사

정본: docker/analysis/prompt_bank_ledger.py (컨테이너 /workspace 는 수동 사본 — README 참조)
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
from collections import Counter, defaultdict

# userwatch 공급 원본 (읽기 전용 SoT — 절대 쓰지 않는다)
NAS_ROOT = os.environ.get("BANK_NAS_ROOT", "/home/user/mou/userwatch/prompts")
# NAS 에 없는 버전의 로컬 사본이 사는 곳. 컨테이너 안에서는 /data/fiftyone/sourceh/prompts
EXTRA_ROOTS = [
    r for r in os.environ.get(
        "BANK_EXTRA_ROOTS",
        "/data/fiftyone/sourceh/prompts",
    ).split(",") if r
]

# prompt_geometry.py 의 CLASS_NAMES 와 동일. 5~7 은 userwatch 미문서 클래스 —
# 이름을 발명하지 않고 class_<n> 으로 보존한다 (019 "규칙을 발명하지 않는다" 원칙).
CLASS_NAMES = {0: "normal", 1: "falldown", 2: "fire", 3: "smoke", 4: "smoking"}

VERSION_RE = re.compile(r"text_features[_-](.+)\.(csv|json)$", re.IGNORECASE)


def norm_text(s: str) -> str:
    """019 주석의 '공백정규화+소문자화'. sentences.jsonl 원장과 동일 알고리즘."""
    return re.sub(r"\s+", " ", s.strip().lower())


def content_hash(text: str) -> str:
    """sha256(정규화 텍스트)[:16].

    ⚠️ 알려진 결함 승계: class 가 해시에 미포함이라 같은 text·다른 class 는 충돌한다.
    019 가 이 알고리즘을 고치지 않기로 했고, 실측상 **뱅크 내부 충돌은 0건**이라
    UNIQUE(bank_id, content_hash) 를 위반하지 않는다 (전 버전 검사). 뱅크 간에는
    2천여 건이 겹치지만 그건 멤버십으로 표현되므로 문제되지 않는다.
    """
    return hashlib.sha256(norm_text(text).encode("utf-8")).hexdigest()[:16]


def class_label(raw: str) -> str:
    try:
        return CLASS_NAMES.get(int(raw), f"class_{int(raw)}")
    except (TypeError, ValueError):
        return "class_unknown"


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _json_has_text(path: str) -> bool:
    """28GB JSON 을 파싱하지 않고 앞머리만 보고 prompt 필드 유무를 판정."""
    with open(path, "rb") as fh:
        head = fh.read(4096).decode("utf-8", "replace")
    m = re.search(r'"prompt"\s*:\s*(null|")', head)
    return bool(m) and m.group(1) != "null"


def scan_roots(nas_root: str, extra_roots: list[str]) -> dict[str, dict]:
    """버전 태그 → {csv, json, roots} 매핑. 나중 루트(extra)가 CSV 를 보충한다."""
    found: dict[str, dict] = defaultdict(lambda: {"csv": None, "json": None, "roots": []})
    for root, kind in [(nas_root, "nas")] + [(r, "local") for r in extra_roots]:
        if not os.path.isdir(root):
            continue
        for dirpath, _dirnames, filenames in os.walk(root):
            for fn in filenames:
                m = VERSION_RE.match(fn)
                if not m:
                    continue
                tag, ext = m.group(1), m.group(2).lower()
                entry = found[tag]
                if entry[ext] is None:          # 먼저 발견한 루트를 정본으로
                    entry[ext] = os.path.join(dirpath, fn)
                    entry["roots"].append(kind)
        # 파일이 하나도 없는 버전 폴더도 카탈로그에 남긴다 (빈 폴더 = 사실)
        if root == nas_root:
            for d in sorted(os.listdir(root)):
                if os.path.isdir(os.path.join(root, d)) and d not in found:
                    found[d]  # defaultdict 로 빈 엔트리 생성
    return dict(found)


def read_csv_rows(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        return [r for r in csv.DictReader(fh) if (r.get("prompt") or "").strip()]


def build_inventory(nas_root: str, extra_roots: list[str], *, with_checksum: bool) -> list[dict]:
    banks = []
    for tag, e in sorted(scan_roots(nas_root, extra_roots).items()):
        rec = {
            "version_tag": tag,                       # 019: 원문 그대로 보존
            "source": "userwatch",
            "origin_uri": e["csv"] or e["json"] or os.path.join(nas_root, tag),
            "embedding_npz_key": None,
            "model_name": "PE-Core-L14-336",
            "sentence_count": None,
            "class_counts": None,
            "checksum": None,
            "sentence_storage": "external_only",      # 텍스트 있으면 아래에서 승격
            "notes": None,
            "_text_source": None,                     # 019 컬럼 아님 — 운영 판단용
        }
        if e["csv"]:
            rows = read_csv_rows(e["csv"])
            rec["sentence_count"] = len(rows)
            rec["class_counts"] = dict(Counter(class_label(r.get("class")) for r in rows))
            rec["sentence_storage"] = "db_backed"
            rec["_text_source"] = "csv:" + ("nas" if e["csv"].startswith(nas_root) else "local")
            if with_checksum:
                rec["checksum"] = sha256_file(e["csv"])
        elif e["json"]:
            rec["_text_source"] = "json_text" if _json_has_text(e["json"]) else "vector_only"
            rec["notes"] = ("JSON 에 prompt 텍스트 없음 (벡터 전용) — userwatch 에 CSV 요청 필요"
                            if rec["_text_source"] == "vector_only" else None)
        else:
            rec["_text_source"] = "empty"
            rec["notes"] = "빈 폴더 — 파일 0개"
        banks.append(rec)
    return banks


def build_ledger(nas_root: str, extra_roots: list[str]):
    """뱅크별 문장행 + 고유 문장 원장. 반환: (bank_rows, unique_rows, collisions)

    ⚠️ `class_label` 은 **문장의 속성이 아니라 멤버십의 속성**이다 — 실측상 2,106개 문장이
    뱅크에 따라 다른 클래스를 갖는다(대부분 normal↔smoking, 즉 smoking 클래스는 기존 normal
    문장의 재라벨로 도입됐다). 따라서 고유 문장 행에는 클래스를 달지 않는다. 벡터도 텍스트만의
    함수(PE-Core)라 클래스와 무관하므로, 임베딩 적재 대상은 고유 문장 쪽이 맞다.
    """
    bank_rows: list[dict] = []                  # 019 bank_sentences + §6.2 멤버십(gidx)
    unique: dict[str, dict] = {}                # 벡터 적재 대상 — 클래스 없음
    collisions: list[dict] = []
    for tag, e in sorted(scan_roots(nas_root, extra_roots).items()):
        if not e["csv"]:
            continue
        seen_in_bank: dict[str, str] = {}
        for gidx, r in enumerate(read_csv_rows(e["csv"])):
            text, cls = r["prompt"].strip(), class_label(r.get("class"))
            h = content_hash(text)
            prev = seen_in_bank.get(h)
            if prev is not None and prev != cls:
                # 뱅크 **내부** 충돌만 UNIQUE(bank_id, content_hash) 위반이다. 실측 0건이지만
                # 새 뱅크가 들어오면 깨질 수 있으므로 조용히 넘기지 않는다.
                collisions.append({"version_tag": tag, "content_hash": h,
                                   "class_a": prev, "class_b": cls, "text": text})
            seen_in_bank[h] = cls
            # gidx = 뱅크 안에서의 **행 위치**. 프레임의 winner_gidx 와 JSON feature 배열
            # 인덱스가 가리키는 값이 이것이다. CSV 의 `ID` 컬럼은 gidx 가 아니라 레거시
            # 변형 표시다 (실측: v1.0.8.0 은 12,480행에 ID 고유값 2,405개·0 이 9,978행) —
            # ID 를 gidx 로 쓰면 (bank_id, gidx) 가 충돌한다. 빈 prompt 행은 전 뱅크
            # 통틀어 0건이라 필터링이 인덱스를 밀지 않는다 (실측).
            bank_rows.append({"version_tag": tag, "content_hash": h, "text": text,
                              "class_label": cls, "origin": "userwatch", "adopted": False,
                              "gidx": gidx,
                              "legacy_id": int(r["ID"]) if (r.get("ID") or "").isdigit() else None})
            u = unique.setdefault(h, {"content_hash": h, "text": text,
                                      "n_versions": 0, "class_labels": []})
            u["n_versions"] += 1
            if cls not in u["class_labels"]:
                u["class_labels"].append(cls)
    return bank_rows, list(unique.values()), collisions


def cmd_inventory(args) -> int:
    banks = build_inventory(args.nas_root, args.extra, with_checksum=args.checksum)
    by_state = Counter(b["_text_source"] for b in banks)
    print(f"{'version_tag':16s} {'텍스트':12s} {'문장수':>8s}  notes")
    print("-" * 78)
    for b in banks:
        print(f"{b['version_tag']:16s} {b['_text_source'] or '-':12s} "
              f"{b['sentence_count'] or 0:8,d}  {b['notes'] or ''}")
    total = sum(b["sentence_count"] or 0 for b in banks)
    print("-" * 78)
    print(f"버전 {len(banks)}개 / 상태 {dict(by_state)} / 텍스트 보유 문장 총 {total:,}행")
    missing = [b["version_tag"] for b in banks if b["_text_source"] in ("vector_only", "empty")]
    if missing:
        print(f"\n⚠️ userwatch 에 CSV 요청할 버전 {len(missing)}개: {', '.join(missing)}")
    if args.out:
        os.makedirs(args.out, exist_ok=True)
        p = os.path.join(args.out, "banks_inventory.json")
        with open(p, "w", encoding="utf-8") as fh:
            json.dump(banks, fh, ensure_ascii=False, indent=2)
        print(f"→ {p}")
    return 0


def cmd_ledger(args) -> int:
    bank_rows, uniq, coll = build_ledger(args.nas_root, args.extra)
    versions = len({r["version_tag"] for r in bank_rows})
    print(f"버전 {versions}개 / 뱅크 문장행 {len(bank_rows):,} / 고유 문장 {len(uniq):,}개")
    print(f"클래스 분포(뱅크 행 기준): {dict(Counter(r['class_label'] for r in bank_rows))}")
    multi = [u for u in uniq if len(u["class_labels"]) > 1]
    print(f"뱅크 간 클래스 상충 문장 {len(multi):,}개 — 클래스는 멤버십 속성으로만 보존")
    if multi:
        print(f"  상충 조합: {Counter(tuple(sorted(u['class_labels'])) for u in multi).most_common(5)}")
    approx = sum(len(u["text"]) for u in uniq)
    print(f"텍스트 총량 {approx / 1e6:.1f} MB / 벡터 환산 "
          f"{len(uniq) * 1024 * 4 / 1e9:.2f} GB / 재임베딩 ≈ {len(uniq) * 7.5 / 6e4:.0f}분")
    # 같은 뱅크에 같은 문장이 두 번 들어간 경우 — 019 의 UNIQUE 가 gidx 기준이라 허용된다.
    # (원안의 content_hash UNIQUE 였다면 이만큼이 조용히 사라졌을 자리다)
    per_bank = Counter((r["version_tag"], r["content_hash"]) for r in bank_rows)
    dup_text = sum(v - 1 for v in per_bank.values() if v > 1)
    print(f"뱅크 내 같은 문장 반복 {dup_text}건 — gidx 로 구분되어 보존된다")
    if coll:
        print(f"\n❌ 뱅크 내부 같은 문장에 다른 클래스 {len(coll)}건 — 클래스 원장 모순, 적재 전 확인 필요")
        for c in coll[:5]:
            print(f"   {c['version_tag']} {c['content_hash']} {c['class_a']}≠{c['class_b']}")
    else:
        print("✅ 뱅크 내부 클래스 모순 0건")
    if args.out:
        os.makedirs(args.out, exist_ok=True)
        for name, rows in (("bank_sentences.jsonl", bank_rows),
                           ("unique_sentences.jsonl", uniq)):
            p = os.path.join(args.out, name)
            with open(p, "w", encoding="utf-8") as fh:
                for r in rows:
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n")
            print(f"→ {p} ({len(rows):,}행)")
    else:
        print("\n(dry-run — 파일을 쓰려면 --out DIR)")
    return 1 if coll else 0


def cmd_selftest(_args) -> int:
    # 1) 해시 알고리즘이 기존 sentences.jsonl 원장과 같은가 (실제 원장 행에서 역산한 벡터)
    assert content_hash(
        "A CCTV view of a quiet waste storage yard at night. "
        "A streetlight pole stands in the center."
    ) == "6d65f1dca5f8d142", "content_hash 가 sentences.jsonl 원장과 불일치"
    # 2) 정규화가 대소문자·공백에 불변
    assert content_hash("  A  Fire  ") == content_hash("a fire")
    # 3) class 는 해시에 안 들어간다 (019 가 승계하기로 한 결함 — 고쳤는지 감지)
    assert content_hash("a fire") == content_hash("a fire")
    # 4) 클래스 라벨 매핑이 prompt_geometry 와 일치 + 미문서 클래스 보존
    assert class_label("2") == "fire" and class_label("7") == "class_7"
    assert class_label("") == "class_unknown"
    # 5) 버전 태그 추출이 userwatch 의 비일관 표기 4종을 모두 잡는가
    for fn, want in [("text_features_v1.0.8.0.csv", "v1.0.8.0"),
                     ("text_features_1.0.13.2.json", "1.0.13.2"),
                     ("text_features_V1.0.11.0.csv", "V1.0.11.0"),
                     ("text_features_v1.0.8.0+night5.csv", "v1.0.8.0+night5")]:
        m = VERSION_RE.match(fn)
        assert m and m.group(1) == want, f"{fn} → {m and m.group(1)} (기대 {want})"
    # 6) 클래스는 멤버십 속성 — 고유 문장 행에 class_label 이 새어 들어가면 실패
    #    (같은 텍스트가 normal/smoking 두 클래스로 존재하는 실측 2,106건을 뭉개는 회귀 가드)
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        os.makedirs(f"{td}/vX", exist_ok=True)
        with open(f"{td}/vX/text_features_vX.csv", "w", encoding="utf-8") as fh:
            fh.write("ID,class,prompt\n0,0,a man smokes\n")
        os.makedirs(f"{td}/vY", exist_ok=True)
        with open(f"{td}/vY/text_features_vY.csv", "w", encoding="utf-8") as fh:
            fh.write("ID,class,prompt\n0,4,A Man  Smokes\n")
        rows, uniq, coll = build_ledger(td, [])
        assert len(rows) == 2 and len(uniq) == 1, (len(rows), len(uniq))
        # gidx 는 행 위치지 CSV 의 ID 컬럼이 아니다 (둘 다 0 인 케이스라 위치로 검증)
        assert [r["gidx"] for r in rows] == [0, 0], [r["gidx"] for r in rows]
        assert "class_label" not in uniq[0], "고유 문장에 class_label 이 있으면 상충을 뭉갠다"
        assert sorted(uniq[0]["class_labels"]) == ["normal", "smoking"]
        assert coll == [], "서로 다른 뱅크의 클래스 차이는 UNIQUE 위반이 아니다"
    print("✅ selftest 통과 (6종)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("command", choices=["inventory", "ledger", "selftest"])
    ap.add_argument("--nas-root", default=NAS_ROOT)
    ap.add_argument("--extra", action="append", default=None,
                    help=f"NAS 에 없는 버전의 로컬 사본 루트 (기본: {EXTRA_ROOTS})")
    ap.add_argument("--out", help="산출물 디렉토리 (미지정 시 dry-run)")
    ap.add_argument("--checksum", action="store_true",
                    help="원본 CSV sha256 계산 (019 checksum 컬럼용, 느림)")
    args = ap.parse_args()
    if args.extra is None:
        args.extra = EXTRA_ROOTS
    return {"inventory": cmd_inventory, "ledger": cmd_ledger,
            "selftest": cmd_selftest}[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
