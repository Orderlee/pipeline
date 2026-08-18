#!/usr/bin/env python3
"""뱅크 문장의 **정본 해석 계층** + 자리표시자 복구 CLI.

이 모듈이 "이 뱅크 버전의 문장은 무엇인가" 에 답하는 유일한 곳이다. 소비자 셋
(`prompt_geometry.load_all` / `frames_eval` / `prompt_eval`)이 각자 npz 를 열어
`z["prompt"]` 를 읽던 것을 `load_bank()` 하나로 모은다 — **벡터는 npz, 문장은 DB**.
그래야 npz 를 다시 만들어도 문장이 딸려 날아가지 않는다(2026-08-11 사고의 재발 조건 제거).

## 무엇이 깨졌나 (2026-08-18 실측)

userwatch 공급물은 벡터(`text_features_<v>.json`)와 문장(`text_features_<v>.csv`)이 **따로**
온다. JSON 의 `"prompt"` 는 전 버전 `null` 이다. 2026-08-11 일괄 npz 빌드가 JSON 만 읽고
옆의 CSV 를 보지 않아 npz 의 `prompt` 배열이 `(텍스트 없음 #N)` 으로 채워졌고,
`stage_promptmap` 이 그걸 그대로 `-prompts` 데이터셋 `text` 로 옮겼다.

  · npz 29버전 중 **27개가 자리표시자**, 그중 **19개는 CSV 로 복구 가능**
  · 나머지 8개(v1.0.5.4/5.5/5.6/5.7, v1.0.13.0/13.1/13.2, v1.0.2.0)는 원본에 문장이 없거나
    CSV 행수가 npz 와 달라 **복구 불가** — 자리표시자가 사실이다
  · `#N` 의 N 은 공급자 `ID` 컬럼이지 행 번호가 **아니다** (v1.0.8.0 은 12,480행에 ID 2,405종,
    v1.0.6.2 는 16,125행 전부 ID=0). 자리표시자로 문장을 역추적할 수 없다.

## 문장 소스 2종 — DB 우선

  · **DB**(`bank_sentences` ⨝ `prompt_banks`, 기본): `text NOT NULL` + `gidx` 보유 →
    `ORDER BY gidx` 가 곧 npz 행 순서다. 8/11 재빌드에 **무손상 생존**한 유일한 사본이고
    "문장 없음"을 자리표시자가 아니라 `sentence_storage='external_only'` 라는 뱅크 상태로
    들고 있다. NAS 마운트도 CSV 사본도 필요 없다.
  · **CSV**(`text_features_<v>.csv`, 폴백): DB 미적재 버전용. 행 순서가 유일한 대응이라
    아래 정렬 게이트가 필수다.

두 소스는 `--compare` 로 전 버전 원소단위 대조가 가능하다 (실측 29/29 동일).

## 정렬 근거 (CSV 행 i ↔ npz 행 i)

CSV 의 `ID` 컬럼은 행 번호가 아니므로 **행 순서**가 유일한 대응이다. 그래서 fail-closed 로
세 가지를 전부 통과할 때만 쓴다: 행수 일치 · `class` 배열 원소단위 일치 · 빈/자리표시자 문장 없음.
독립 검증: v1.0.8.0 은 프레임 6,484건의 `winner_peak_piece` 가 `winner_gidx_v1080 %
GIDX_OFFSET` 행 문장의 부분문자열로 **6,484/6,484** 일치했다.

## 사용

    python3 repair_bank_prompts.py                 # 감사(기본 dry-run) — 표만 출력
    python3 repair_bank_prompts.py --apply         # npz 복구 (원본 prompt 는 write-once 백업)
    python3 repair_bank_prompts.py --sync          # 백필 미리보기 (쓰지 않는다)
    python3 repair_bank_prompts.py --apply --sync  # npz 복구 + 백필 실제 반영
    python3 repair_bank_prompts.py --compare       # DB ↔ CSV 소스 동등성 대조
    python3 repair_bank_prompts.py --source csv    # DB 무시하고 CSV 만 (auto|db|csv)
    python3 repair_bank_prompts.py --selftest      # 파일 없이 도는 불변식 검사

`--sync` 는 `stage_promptmap` 재빌드가 **아니다**. 벡터·cls 가 그대로라 gidx·wins·purity·
wave_gain·UMAP 좌표가 전부 불변이고 바뀌는 건 `text` 뿐이라, 데이터셋을 날리는 재빌드
(`fo.Dataset(overwrite=True)` — 태그·워크스페이스 소실) 대신 필드 하나만 갱신한다.

정본: docker/analysis/repair_bank_prompts.py
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import sys

import numpy as np

PROMPT_DIR = os.environ.get("BANK_PROMPT_DIR", "/data/fiftyone/sourceh/prompts")
# 앞 루트가 우선. `_csv/` 는 NAS(`/home/user/mou/userwatch/prompts`) 사본 — 컨테이너에는 NAS
# 마운트가 없어서 한 번 복사해 둔다. 호스트에서 돌리면 NAS 를 직접 읽는다.
CSV_ROOTS = [r for r in os.environ.get(
    "BANK_CSV_ROOTS",
    f"{PROMPT_DIR},{PROMPT_DIR}/_csv,/home/user/mou/userwatch/prompts").split(",") if r]
BACKUP_DIR = f"{PROMPT_DIR}/_prompt_backup"
PLACEHOLDER_PREFIX = "(텍스트 없음"      # prompt_geometry.PLACEHOLDER_PREFIX 와 동일 문자열
GIDX_OFFSET = 100_000                    # prompt_geometry.GIDX_OFFSET 와 동일 값
CLASS_NAMES = {0: "normal", 1: "falldown", 2: "fire", 3: "smoke", 4: "smoking"}
VERSION_RE = re.compile(r"text_features[_-](.+)\.csv$", re.IGNORECASE)
# analysis 컨테이너에 실제로 꽂혀 있는 이름이 첫 번째다 (`DATAOPS_POSTGRES_DSN`).
DSN_ENV = ("BANK_DB_DSN", "DATAOPS_POSTGRES_DSN", "POSTGRES_DSN", "DATABASE_URL")


def log(m: str) -> None:
    print(m, flush=True)


def is_placeholder(s) -> bool:
    return str(s or "").lstrip().startswith(PLACEHOLDER_PREFIX)


def norm_ver(v: str) -> str:
    """`V1.0.10.3` / `v1.0.10.3` / `1.0.13.0` → `1.0.10.3` — 공급자 표기 흔들림 흡수."""
    return v.strip().lstrip("vV")


def class_to_int(label) -> int:
    """`fire` → 2, `class_7` → 7. 원장(`prompt_bank_ledger.class_label`)의 역함수."""
    rev = {v: k for k, v in CLASS_NAMES.items()}
    t = str(label).strip()
    if t in rev:
        return rev[t]
    m = re.fullmatch(r"class_(\d+)", t)
    if m:
        return int(m.group(1))
    raise ValueError(f"모르는 클래스 라벨 {label!r}")


def read_csv_texts(path: str) -> tuple[list[str], np.ndarray]:
    rows = list(csv.DictReader(open(path, encoding="utf-8")))
    if not rows or "prompt" not in rows[0] or "class" not in rows[0]:
        raise ValueError(f"헤더에 prompt/class 없음: {os.path.basename(path)}")
    return ([r["prompt"] for r in rows],
            np.array([int(r["class"]) for r in rows], dtype=np.int64))


def csv_candidates() -> dict[str, tuple]:
    """norm_ver → (texts, cls, 사유). 읽기 실패는 그 버전만 후보에서 빠진다."""
    out: dict[str, tuple] = {}
    for root in CSV_ROOTS:
        for p in sorted(glob.glob(f"{root}/**/text_features_*.csv", recursive=True)):
            m = VERSION_RE.search(os.path.basename(p))
            if not m or norm_ver(m.group(1)) in out:      # 앞 루트 우선
                continue
            try:
                texts, ccls = read_csv_texts(p)
            except (OSError, ValueError):
                continue
            out[norm_ver(m.group(1))] = (texts, ccls, f"CSV {os.path.basename(p)}")
    return out


def rows_to_candidates(rows) -> dict[str, tuple]:
    """DB 행 [(version_tag, gidx, class_label, text)] → norm_ver → (texts, cls, 사유).

    `gidx` 가 0..n-1 을 정확히 한 번씩 덮을 때만 후보로 낸다 — 그래야 `ORDER BY gidx` 가
    npz 행 순서와 같다고 말할 수 있다 (CSV 경로에는 없는 추가 게이트).
    """
    by: dict[str, list] = {}
    for ver, gidx, lab, text in rows:
        by.setdefault(norm_ver(ver), []).append((gidx, lab, text))
    out: dict[str, tuple] = {}
    for ver, items in by.items():
        items.sort(key=lambda x: (x[0] is None, x[0]))
        gs = [g for g, _, _ in items]
        if None in gs or gs != list(range(len(gs))):
            out[ver] = None                               # 게이트 실패를 사유로 남긴다
            continue
        try:
            cls = np.array([class_to_int(lab) for _, lab, _ in items], dtype=np.int64)
        except ValueError:
            out[ver] = None
            continue
        out[ver] = ([t for _, _, t in items], cls, "DB bank_sentences")
    return out


def db_candidates(versions: list[str] | None = None) -> dict[str, tuple]:
    """DSN/psycopg2 가 없으면 조용히 빈 dict — 폴백(CSV·npz)이 있으므로 여기서 죽지 않는다.

    versions 를 주면 그 버전만 조회한다. 전량은 506,247행이라 2뱅크만 쓰는 스테이지가
    매번 다 끌어오면 낭비다. 대소문자·접두 흔들림(`V1.0.10.3` vs `v1.0.10.3`)이 있어
    `version_tag` 직접 비교 대신 정규화 후 필터한다.
    """
    dsn = next((os.environ[k] for k in DSN_ENV if os.environ.get(k)), None)
    if not dsn:
        return {}
    try:
        import psycopg2
    except ImportError:
        return {}
    sql = """SELECT b.version_tag, s.gidx, s.class_label, s.text
             FROM bank_sentences s JOIN prompt_banks b USING (bank_id)"""
    try:
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            if versions is None:
                cur.execute(sql)
            else:
                cur.execute(sql + " WHERE lower(ltrim(b.version_tag, 'vV')) = ANY(%s)",
                            ([norm_ver(v) for v in versions],))
            rows = cur.fetchall()
    except Exception as exc:                              # noqa: BLE001 — 폴백이 있다
        log(f"⚠️ DB 조회 실패 — 파일 소스로 폴백: {exc}")
        return {}
    return {k: v for k, v in rows_to_candidates(rows).items() if v}


# 한 프로세스 안에서 같은 버전을 두 번 조회하지 않는다 (스테이지가 load_all 을 여러 번 부른다).
_DB_CACHE: dict[str, tuple] = {}
_DB_MISS: set[str] = set()


def _db_for(versions: list[str]) -> dict[str, tuple]:
    want = {norm_ver(v) for v in versions} - set(_DB_CACHE) - _DB_MISS
    if want:
        got = db_candidates(sorted(want))
        _DB_CACHE.update(got)
        _DB_MISS.update(want - set(got))
    return {k: _DB_CACHE[k] for k in (norm_ver(v) for v in versions) if k in _DB_CACHE}


def load_bank(version: str, prompt_dir: str | None = None) -> dict:
    """뱅크 1개 → `{vec, cls, prompt}`. **벡터·클래스는 npz, 문장은 DB 정본.**

    DB 가 없거나(`external_only` 버전) 정합 게이트를 통과하지 못하면 **npz 문장을 그대로
    쓴다** — 조용히 틀린 문장을 넣느니 옛 값을 유지한다. 거부 사유는 로그로 남긴다.
    `BANK_TEXT_SOURCE=npz` 로 DB 경로를 끌 수 있다 (DB 장애 시 탈출구).
    """
    d = prompt_dir or PROMPT_DIR
    z = np.load(f"{d}/{version}.npz", allow_pickle=True)
    bank = {"vec": z["vec"].astype(np.float32), "cls": z["cls"].astype(np.int64),
            "prompt": [str(p) for p in z["prompt"]] if "prompt" in z else []}
    if os.environ.get("BANK_TEXT_SOURCE", "db").lower() != "db":
        return bank
    cand = _db_for([version]).get(norm_ver(version))
    if cand is None:
        return bank
    state, texts, why = check_candidate(bank["cls"], cand)
    if state != "recoverable":
        log(f"⚠️ {version}: DB 정본 거부 ({why}) — npz 문장 유지")
        return bank
    if list(texts) != bank["prompt"]:
        n_d = len(texts) if not bank["prompt"] else \
            sum(1 for a, b in zip(texts, bank["prompt"]) if a != b)
        log(f"{version}: 문장을 DB 정본으로 교체 ({n_d:,}행 다름 — npz 는 파생물이다)")
    bank["prompt"] = list(texts)
    return bank


def check_candidate(cls: np.ndarray, cand) -> tuple[str, list[str] | None, str]:
    """(상태, 복구 문장, 사유). fail-closed — 하나라도 어긋나면 복구하지 않는다."""
    if not cand:
        return "unrecoverable", None, "문장 소스 없음 (벡터 전용이 사실)"
    texts, ccls, why = cand
    if len(texts) != len(cls):
        return "unrecoverable", None, f"행수 불일치 src={len(texts)} npz={len(cls)} ({why})"
    if not np.array_equal(ccls, cls):
        return "unrecoverable", None, f"class 배열 불일치 {int((ccls != cls).sum())}행 ({why})"
    if any(not (t or "").strip() for t in texts):
        return "unrecoverable", None, f"빈 문장 포함 ({why})"
    if any(is_placeholder(t) for t in texts):
        return "unrecoverable", None, f"소스 자체가 자리표시자 ({why})"
    return "recoverable", texts, why


def resolve_sources(source: str) -> dict[str, tuple]:
    """source = auto|db|csv. auto 는 **DB 우선** — 유일하게 무손상 생존한 사본이다."""
    db = db_candidates() if source in ("auto", "db") else {}
    cs = csv_candidates() if source in ("auto", "csv") else {}
    return {**cs, **db} if source == "auto" else (db or {}) if source == "db" else cs


def npz_versions() -> list[str]:
    def key(p):
        return tuple(int(x) for x in os.path.basename(p)[1:-4].split("."))
    return [os.path.basename(p)[:-4]
            for p in sorted(glob.glob(f"{PROMPT_DIR}/v*.npz"), key=key)]


def audit(source: str = "auto") -> list[dict]:
    cands = resolve_sources(source)
    out = []
    for v in npz_versions():
        path = f"{PROMPT_DIR}/{v}.npz"
        z = np.load(path, allow_pickle=True)
        cls = z["cls"]
        pr = [str(x) for x in z["prompt"].tolist()] if "prompt" in z else []
        n_ph = sum(1 for t in pr if is_placeholder(t))
        if not pr:
            state, texts, why = "unrecoverable", None, "npz 에 prompt 배열 없음"
        else:
            state, texts, why = check_candidate(cls, cands.get(norm_ver(v)))
            if n_ph == 0:
                # 자리표시자가 없어도 **정본과 다르면 고친다** — 이게 "DB 가 정본" 의 실제 의미다.
                # (실측: CSV 로 복구한 5버전이 후행 공백 1,875행만큼 DB 와 달랐다.)
                if state != "recoverable":
                    state, texts, why = "clean", None, "문장 보유 (대조할 정본 없음)"
                elif list(texts) == pr:
                    state, texts, why = "clean", None, f"문장 보유 · 정본 일치 ({why})"
                else:
                    n_d = sum(1 for a, b in zip(texts, pr) if a != b)
                    state, why = "drift", f"정본과 {n_d}행 다름 ({why})"
        out.append({"version": v, "path": path, "n": len(cls), "n_ph": n_ph,
                    "state": state, "texts": texts, "why": why})
        del z
    return out


def compare_sources() -> int:
    """DB ↔ CSV 원소단위 대조. 두 정본이 같다고 **말하는 대신 재는** 단계."""
    db, cs = db_candidates(), csv_candidates()
    log(f"DB 후보 {len(db)}버전 · CSV 후보 {len(cs)}버전")
    log(f"{'version':<12} {'n':>7}  대조")
    diff = 0
    for v in npz_versions():
        k = norm_ver(v)
        d, c = db.get(k), cs.get(k)
        n = len(np.load(f"{PROMPT_DIR}/{v}.npz", allow_pickle=True)["cls"])
        if d and c:
            if len(d[0]) != len(c[0]):
                verdict_s = f"⚠️ 행수 다름 db={len(d[0])} csv={len(c[0])}"
                diff += 1
            else:
                # 공백 차이와 문장 차이를 **구분**한다. 실측 1,875행 전부 전자였고
                # (공급자 CSV 의 후행 공백을 원장 적재가 strip), 이걸 뭉뚱그려 "다름" 으로
                # 내면 진짜 문장 차이가 났을 때 신호가 묻힌다.
                ws = real = 0
                first = None
                for i, (x, y) in enumerate(zip(d[0], c[0])):
                    if x == y:
                        continue
                    if x == y.strip():
                        ws += 1
                    else:
                        real += 1
                        first = i if first is None else first
                if real:
                    verdict_s = f"⚠️ 문장 {real}행 다름 (첫 행 {first})"
                    diff += 1
                elif ws:
                    verdict_s = f"동일 — 공백만 {ws}행 (DB=strip)"
                else:
                    verdict_s = f"동일 ({len(d[0]):,}행)"
        elif d:
            verdict_s = f"DB 만 ({len(d[0]):,}행)"
        elif c:
            verdict_s = f"CSV 만 ({len(c[0]):,}행)"
        else:
            verdict_s = "양쪽 없음 (벡터 전용)"
        log(f"{v:<12} {n:>7,}  {verdict_s}")
    log(f"\n문장 불일치 {diff}버전 (공백 차이는 불일치로 세지 않는다)")
    return 1 if diff else 0


def repair_npz(row: dict) -> None:
    """prompt 배열만 교체. vec/cls 는 바이트 동일해야 하고, 검증 통과 후에만 원자 교체."""
    path, texts = row["path"], row["texts"]
    z = np.load(path, allow_pickle=True)
    arrays = {k: z[k] for k in z.files}
    old_prompt = arrays["prompt"]
    new_prompt = np.array(texts, dtype=object)

    os.makedirs(BACKUP_DIR, exist_ok=True)
    bak = f"{BACKUP_DIR}/{row['version']}.prompt.npz"
    if not os.path.exists(bak):                       # write-once
        np.savez_compressed(bak, prompt=old_prompt)
    arrays["prompt"] = new_prompt

    # ⚠️ 확장자는 `.npz` 여야 한다 — savez_compressed 는 그렇지 않으면 `.npz` 를 **덧붙여**
    #    다른 파일에 쓴다 (selftest 가 잡은 실제 버그).
    tmp = f"{path}.tmp.npz"
    np.savez_compressed(tmp, **arrays)
    chk = np.load(tmp, allow_pickle=True)
    assert set(chk.files) == set(arrays), f"{row['version']}: 키 집합이 바뀌었다"
    assert np.array_equal(chk["vec"], arrays["vec"]), f"{row['version']}: vec 변형됨"
    assert np.array_equal(chk["cls"], arrays["cls"]), f"{row['version']}: cls 변형됨"
    assert [str(x) for x in chk["prompt"].tolist()] == list(texts), \
        f"{row['version']}: prompt 왕복 불일치"
    del z, chk
    os.replace(tmp, path)


def sync_datasets(dry: bool) -> None:
    """`<ds>-prompts` 의 `text` 를 복구된 npz 로 백필. gidx → 로컬행은 `% GIDX_OFFSET`."""
    import fiftyone as fo

    banks = {}
    for v in npz_versions():
        z = np.load(f"{PROMPT_DIR}/{v}.npz", allow_pickle=True)
        pr = [str(x) for x in z["prompt"].tolist()] if "prompt" in z else []
        if pr and not any(is_placeholder(t) for t in pr):
            banks[v] = (pr, z["cls"])
        del z
    log(f"문장 보유 뱅크 {len(banks)}종")

    for name in [n for n in fo.list_datasets() if n.endswith("-prompts")]:
        ds = fo.load_dataset(name)
        gidx, bver, cat, sid, cur = (ds.values(f) for f in
                                     ("gidx", "bank_version.label", "category.label",
                                      "id", "text"))
        upd, skipped, bad = {}, [], []
        for v in sorted(set(bver)):
            if v not in banks:
                skipped.append(v)
                continue
            pr, cls = banks[v]
            rows = [i for i, b in enumerate(bver) if b == v]
            # 무결성: gidx 로컬행의 class 가 데이터셋 category 와 같아야 정렬이 맞는 것이다.
            # 하나라도 어긋나면 그 버전은 통째로 건너뛴다 (조용한 오정렬 > 미복구).
            mism = [i for i in rows
                    if CLASS_NAMES.get(int(cls[gidx[i] % GIDX_OFFSET])) != cat[i]]
            if mism:
                bad.append(f"{v}({len(mism)}/{len(rows)}행 category 불일치)")
                continue
            for i in rows:
                t = pr[gidx[i] % GIDX_OFFSET]
                if cur[i] != t:
                    upd[sid[i]] = t
        log(f"{name}: 갱신 {len(upd):,}행 / 전체 {len(gidx):,} "
            f"· 문장없는 버전 {len(skipped)}종 {sorted(skipped)}")
        if bad:
            log(f"  ⚠️ 정렬 검증 실패로 건너뜀: {bad}")
        if upd and not dry:
            ds.set_values("text", upd, key_field="id")
            ds.save()
            log("  → 반영 완료")
        elif upd:
            log("  (dry-run — `--apply` 없이는 쓰지 않음)")


def selftest() -> None:
    import tempfile
    assert is_placeholder("(텍스트 없음 #0)") and is_placeholder("  (텍스트 없음 #12)")
    assert not is_placeholder("A small fire.") and not is_placeholder("") and not is_placeholder(None)
    assert norm_ver("V1.0.10.3") == norm_ver("v1.0.10.3") == norm_ver("1.0.10.3") == "1.0.10.3"

    cls = np.array([0, 0, 1, 2], dtype=np.int64)
    with tempfile.TemporaryDirectory() as d:
        def mkcsv(name, rows):
            p = f"{d}/{name}"
            with open(p, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=["ID", "class", "prompt"])
                w.writeheader()
                for c, t in rows:
                    w.writerow({"ID": 0, "class": c, "prompt": t})
            return p
        def cand(path):
            t, c = read_csv_texts(path)
            return (t, c, os.path.basename(path))
        good = mkcsv("text_features_v9.9.9.9.csv",
                     [(0, "A."), (0, "B."), (1, "C."), (2, "D.")])
        assert check_candidate(cls, cand(good))[0] == "recoverable"
        assert check_candidate(cls, None)[0] == "unrecoverable"
        assert check_candidate(cls, cand(mkcsv("a.csv", [(0, "A.")])))[0] == "unrecoverable"   # 행수
        assert check_candidate(cls, cand(mkcsv("b.csv", [(0, "A."), (1, "B."), (1, "C."), (2, "D.")]))
                               )[0] == "unrecoverable"                                         # class
        assert check_candidate(cls, cand(mkcsv("c.csv", [(0, "A."), (0, " "), (1, "C."), (2, "D.")]))
                               )[0] == "unrecoverable"                                         # 빈 문장
        assert check_candidate(cls, cand(mkcsv("d.csv", [(0, "A."), (0, "(텍스트 없음 #0)"),
                                                         (1, "C."), (2, "D.")]))
                               )[0] == "unrecoverable"                                         # 자리표시자

        # ── DB 경로: 순수부(rows → 후보)만 오프라인 검사 ──
        assert class_to_int("fire") == 2 and class_to_int("class_7") == 7
        try:
            class_to_int("모르는라벨")
            raise AssertionError("모르는 라벨은 예외여야 한다")
        except ValueError:
            pass
        rows = [("V9.9.9.9", 1, "normal", "B."), ("v9.9.9.9", 0, "normal", "A."),
                ("v9.9.9.9", 3, "fire", "D."), ("v9.9.9.9", 2, "falldown", "C.")]
        got = rows_to_candidates(rows)["9.9.9.9"]          # 대소문자·접두 흔들림 흡수 + gidx 정렬
        assert got[0] == ["A.", "B.", "C.", "D."] and np.array_equal(got[1], cls)
        assert check_candidate(cls, got)[0] == "recoverable"
        # gidx 가 0..n-1 을 못 덮으면 후보에서 탈락 (CSV 경로에 없는 추가 게이트)
        assert rows_to_candidates([("v9.9.9.9", 0, "normal", "A."),
                                   ("v9.9.9.9", 5, "fire", "D.")])["9.9.9.9"] is None
        assert rows_to_candidates([("v9.9.9.9", 0, "normal", "A."),
                                   ("v9.9.9.9", 0, "fire", "D.")])["9.9.9.9"] is None
        assert rows_to_candidates([("v9.9.9.9", None, "normal", "A.")])["9.9.9.9"] is None

        # npz 왕복 — vec/cls 불변 + prompt 교체 + write-once 백업
        global PROMPT_DIR, BACKUP_DIR
        PROMPT_DIR, BACKUP_DIR = d, f"{d}/_prompt_backup"
        vec = np.random.RandomState(0).rand(4, 8).astype(np.float32)
        np.savez_compressed(f"{d}/v9.9.9.9.npz", vec=vec, cls=cls,
                            prompt=np.array(["(텍스트 없음 #0)"] * 4, dtype=object))
        row = {"version": "v9.9.9.9", "path": f"{d}/v9.9.9.9.npz",
               "texts": ["A.", "B.", "C.", "D."]}
        repair_npz(row)
        z = np.load(f"{d}/v9.9.9.9.npz", allow_pickle=True)
        assert np.array_equal(z["vec"], vec) and np.array_equal(z["cls"], cls)
        assert [str(x) for x in z["prompt"].tolist()] == ["A.", "B.", "C.", "D."]
        b = np.load(f"{BACKUP_DIR}/v9.9.9.9.prompt.npz", allow_pickle=True)
        assert is_placeholder(str(b["prompt"][0])), "백업이 원본 자리표시자여야 한다"
        # audit: 자리표시자 없어도 정본과 다르면 drift 로 잡아야 한다
        global CSV_ROOTS
        CSV_ROOTS = [d]
        assert audit("csv")[0]["state"] == "clean", "정본과 같으면 clean"
        with open(f"{d}/text_features_v9.9.9.9.csv", "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["ID", "class", "prompt"])
            w.writeheader()
            for c, t in [(0, "A. "), (0, "B."), (1, "C."), (2, "D.")]:
                w.writerow({"ID": 0, "class": c, "prompt": t})
        drifted = audit("csv")[0]
        assert drifted["state"] == "drift" and "1행 다름" in drifted["why"], drifted
        assert drifted["texts"][0] == "A. "

        # load_bank: DB 정본 우선 / 거부 시 npz 유지 / kill-switch
        _DB_CACHE.clear()
        _DB_MISS.clear()
        NPZ_T, DB_T = ["A.", "B.", "C.", "D."], ["Z.", "B.", "C.", "D."]
        b0 = load_bank("v9.9.9.9", d)                      # DB 후보 없음 → npz 문장
        assert b0["prompt"] == NPZ_T and np.array_equal(b0["vec"], vec)
        _DB_CACHE["9.9.9.9"] = (DB_T, cls, "DB stub")
        assert load_bank("v9.9.9.9", d)["prompt"] == DB_T                    # DB 가 이긴다
        os.environ["BANK_TEXT_SOURCE"] = "npz"
        assert load_bank("v9.9.9.9", d)["prompt"] == NPZ_T                   # kill-switch
        del os.environ["BANK_TEXT_SOURCE"]
        _DB_CACHE["9.9.9.9"] = (["X."], np.array([0]), "DB stub")            # 행수 불일치
        assert load_bank("v9.9.9.9", d)["prompt"] == NPZ_T                   # 거부 → npz 유지
        _DB_CACHE.clear()
        _DB_MISS.clear()

        repair_npz(row)                                   # 2회차: 백업을 덮지 않는다
        b2 = np.load(f"{BACKUP_DIR}/v9.9.9.9.prompt.npz", allow_pickle=True)
        assert is_placeholder(str(b2["prompt"][0])), "백업 write-once 위반"
        assert not glob.glob(f"{d}/*.tmp*"), "tmp 잔여물"
    log("selftest OK")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--apply", action="store_true", help="npz 를 실제로 고친다")
    ap.add_argument("--sync", action="store_true", help="-prompts 데이터셋 text 백필")
    ap.add_argument("--source", default="auto", choices=("auto", "db", "csv"),
                    help="문장 소스. auto=DB 우선 후 CSV 폴백 (기본)")
    ap.add_argument("--compare", action="store_true", help="DB ↔ CSV 동등성만 대조하고 끝")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        selftest()
        return 0
    if a.compare:
        return compare_sources()

    rows = audit(a.source)
    log(f"{'bank':>3} {'version':<12} {'n':>7} {'자리표시자':>10}  상태")
    for i, r in enumerate(rows):
        mark = {"clean": "정상", "recoverable": "✅복구가능", "drift": "↻정본과 drift",
                "unrecoverable": "—복구불가"}[r["state"]]
        log(f"{i:>3} {r['version']:<12} {r['n']:>7,} {r['n_ph']:>10,}  {mark}  {r['why']}")
    rec = [r for r in rows if r["state"] in ("recoverable", "drift")]
    log(f"\n정상 {sum(1 for r in rows if r['state']=='clean')} · "
        f"복구가능 {sum(1 for r in rows if r['state']=='recoverable')} · "
        f"drift {sum(1 for r in rows if r['state']=='drift')} · "
        f"복구불가 {sum(1 for r in rows if r['state']=='unrecoverable')}")

    if rec and a.apply:
        for r in rec:
            repair_npz(r)
            verb = "복구" if r["state"] == "recoverable" else "정본정렬"
            log(f"{verb} {r['version']}: {r['n']:,}문장 ← {r['why']}")
    elif rec:
        log("(dry-run — npz 를 고치려면 `--apply`)")

    if a.sync:
        log("")
        sync_datasets(dry=not a.apply)
    return 0


if __name__ == "__main__":
    sys.exit(main())
