#!/usr/bin/env python3
"""업로드 킷 번들의 프롬프트 뱅크를 **Postgres 정본**(019 스키마)에 등록한다.

왜 필요한가: compare 패널의 `cos` 열은 PG `prompt_banks ⨝ bank_sentences ⨝
image_embeddings(entity_type='prompt')` top-k 로만 채워진다 (`user-prompt-compare`
의 `pdb_frame_topk`). 업로드 킷(`project_upload/ingest_bundle.py`)은 문장 벡터를
FiftyOne `sentence_embedding` 필드에만 넣으므로, 킷으로 들어온 뱅크는 등록 전까지
표의 cos 가 **전부 `-`** 다 (2026-09-04 `vGEN.2026.09.04` 리포트).

정본은 **번들**(prompts.csv + prompt_embeddings.npz)이지 FiftyOne 사본이 아니다.
데이터셋 marker 의 지문으로 번들이 인제스트 당시와 같은지 먼저 대조하고(score_bundle
과 같은 계약), 그 다음 FiftyOne 쪽 `gidx`↔텍스트 정렬까지 실제로 확인한다 — 패널이
`전역 gidx % 100000` 로 조인하므로 이 정렬이 어긋나면 조용히 남의 문장 점수가 붙는다.

⚠️ `prompt_banks` 는 **전역 레지스트리**다. 등록하면 그 뱅크가 모든 데이터셋의 top-k
   표에 버전 한 줄로 늘어난다(2026-09-04 기준 37 → 38). 프로젝트 전용 뱅크를 무심코
   넣지 말 것. 그래서 킷 인제스트가 이걸 자동으로 부르지 않는다 — 수동 결정이다.

    docker exec docker-analysis-1 python3 /workspace/register_bank_db.py dtro2
    docker exec docker-analysis-1 python3 /workspace/register_bank_db.py \\
        dtro2 --version vGEN.2026.09.04 --apply

기본 DRY-RUN. `--apply` 로 실제 쓰기.
"""
from __future__ import annotations

import argparse
import os
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "project_upload"))

# `content_hash`/`norm_text` 는 019 규약의 정본 구현을 그대로 쓴다 (재구현 금지 — 해시가
# 1비트만 달라도 기존 121,614 prompt 벡터와 조인이 통째로 끊긴다).
from prompt_bank_ledger import content_hash, norm_text  # noqa: E402

PANEL_MODEL = os.environ.get("BANK_EMBED_MODEL", "facebook/PE-Core-L14-336")
DSN_ENV = ("BANK_DB_DSN", "DATAOPS_POSTGRES_DSN", "POSTGRES_DSN", "DATABASE_URL")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("dataset", nargs="?", help="업로드 킷이 만든 이미지 데이터셋 이름 (예: dtro2)")
    p.add_argument("--version", action="append", dest="versions",
                   help="등록할 뱅크 버전 (반복 가능). 생략하면 번들의 전 버전")
    p.add_argument("--source", default="internal", choices=("userwatch", "internal", "hybrid"),
                   help="prompt_banks.source — CHECK 제약 3값만 허용 (기본 internal)")
    p.add_argument("--apply", action="store_true", help="실제 등록 (기본 dry-run)")
    p.add_argument("--selftest", action="store_true", help="순수함수 자체검증만 하고 종료")
    return p.parse_args()


def build_plan(rows: list[dict], version: str, have_hashes: set[str],
               db_text: dict[str, str]) -> dict:
    """번들 문장행 → 그 버전의 등록 계획. **FiftyOne·DB 접근 없는 순수함수** (selftest 대상).

    gidx 는 **뱅크-로컬 행 번호**(0..n-1, CSV 등장 순)다. 전역 gidx 를 넣으면 패널이
    `전역 % 100000` 로 조인하다 0행이 나와 조용히 `external_only` 폴백으로 떨어진다
    (optbank_db_register.py 의 2026-08-28 실측 주석과 같은 함정).
    """
    sel = [r for r in rows if r["version"] == version]
    if not sel:
        raise SystemExit(f"[실패] 번들에 버전 {version!r} 이 없음")
    items = [{"gidx": i, "text": r["text"], "class": r["class"],
              "hash": content_hash(r["text"]), "row": r["_row"]}
             for i, r in enumerate(sel)]
    hs = [it["hash"] for it in items]
    # 같은 해시 다른 텍스트 = 019 가 고치지 않기로 한 결함(class 미포함)의 발현. 조용히
    # 남의 문장 벡터를 물려받게 되므로 등록 전에 멈춘다.
    collisions = [it for it in items
                  if it["hash"] in db_text and norm_text(db_text[it["hash"]]) != norm_text(it["text"])]
    seen: set[str] = set()
    need_vec = [it for it in items
                if it["hash"] not in have_hashes and not (it["hash"] in seen or seen.add(it["hash"]))]
    return {"version": version, "items": items, "need_vec": need_vec,
            "dup_in_bank": len(hs) - len(set(hs)), "collisions": collisions}


def check_alignment(prompt_ds, version: str, items: list[dict], offset: int) -> None:
    """FiftyOne `<X>-prompts` 의 `gidx % offset` ↔ 번들 로컬 gidx 텍스트 일치 검사.

    패널의 조인 키가 정확히 이 값이므로, 여기서 어긋나면 등록은 성공하지만 표에는
    **다른 문장의 cos** 가 붙는다 (크래시 없는 오답). 전량 대조한다 — 수천 행이라 싸다.
    """
    import fiftyone as fo

    sub = prompt_ds.match(fo.ViewField("bank_version.label") == version)
    n = sub.count()
    if n != len(items):
        raise SystemExit(f"[실패] {prompt_ds.name} 의 {version} 문장 {n}행 ≠ 번들 {len(items)}행")
    by_gidx = {it["gidx"]: it["text"] for it in items}
    bad = 0
    for g, t in zip(*sub.values(["gidx", "text"])):
        local = int(g) % offset
        if local not in by_gidx or str(t).strip() != by_gidx[local].strip():
            bad += 1
    if bad:
        raise SystemExit(f"[실패] gidx↔텍스트 정렬 불일치 {bad}/{n}행 — 등록하면 남의 문장 점수가 붙는다")
    print(f"[정렬] {version}: gidx↔텍스트 {n:,}행 전량 일치")


def main() -> int:
    args = parse_args()
    if args.selftest:
        return selftest()
    if not args.dataset:
        raise SystemExit("[실패] dataset 인자가 필요합니다 (--selftest 제외)")

    import fiftyone as fo
    import numpy as np
    import psycopg2
    from psycopg2.extras import execute_batch

    import bundle_common as bc

    ds = fo.load_dataset(args.dataset)
    marker = (ds.info or {}).get("upload_kit")
    if not marker:
        raise SystemExit(f"[실패] '{args.dataset}' 은 업로드 킷 데이터셋이 아님 (upload_kit marker 없음)")
    bundle_dir = marker["bundle"]
    prompt_ds = fo.load_dataset(args.dataset + bc.PROMPTS_SUFFIX)

    manifest = bc.load_manifest(bundle_dir)
    model = str(manifest.get("model_name") or "")
    if model != PANEL_MODEL:
        raise SystemExit(
            f"[실패] 번들 인코더 {model!r} ≠ 패널이 조회하는 모델 {PANEL_MODEL!r} — 등록해도 cos 는 "
            f"안 뜬다(패널 질의가 model_name 으로 거른다). 같은 인코더로 재임베딩하거나 "
            f"BANK_EMBED_MODEL 을 맞출 것")

    dim = int(manifest["embedding_dim"])
    keys, img_vec = bc.load_image_npz(bundle_dir, dim)
    rows, sent_vec, versions = bc.load_prompts(bundle_dir, dim)
    for i, r in enumerate(rows):
        r["_row"] = i                      # 번들 npz 행 번호 (버전 필터 후에도 벡터를 찾기 위해)
    fp = bc.bundle_fingerprint(keys, img_vec, rows, sent_vec)
    if marker.get("fingerprint") != fp:
        raise SystemExit(
            f"[실패] 번들 지문 불일치 (marker {marker.get('fingerprint')} ≠ 현재 {fp}) — "
            f"인제스트 이후 prompts.csv/npz 가 바뀌었다. --overwrite 재인제스트 후 다시 실행")
    print(f"[번들] {bundle_dir} · 지문 {fp} · 문장 {len(rows):,} · 버전 {versions}")

    dsn = next((os.environ[k] for k in DSN_ENV if os.environ.get(k)), None)
    if not dsn:
        raise SystemExit("[실패] DSN 미설정 (" + "/".join(DSN_ENV) + ")")
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SELECT entity_id FROM image_embeddings WHERE entity_type='prompt' AND model_name=%s",
                (PANEL_MODEL,))
    have = {r[0] for r in cur}
    cur.execute("SELECT content_hash, MIN(text) FROM bank_sentences GROUP BY content_hash")
    db_text = dict(cur.fetchall())

    targets = args.versions or versions
    plans = []
    for v in targets:
        plan = build_plan(rows, v, have, db_text)
        check_alignment(prompt_ds, v, plan["items"], bc.GIDX_OFFSET)
        cur.execute("SELECT bank_id, source, sentence_count FROM prompt_banks WHERE version_tag=%s", (v,))
        plan["existing"] = cur.fetchone()
        plans.append(plan)
        ex = f"기존 뱅크 재등록({plan['existing'][1]})" if plan["existing"] else "신규 뱅크"
        print(f"[계획] {v}: 문장 {len(plan['items']):,} · 벡터 신규 {len(plan['need_vec']):,} "
              f"· 뱅크내 해시중복 {plan['dup_in_bank']} · 해시충돌 {len(plan['collisions'])} · {ex}")
        for it in plan["collisions"][:3]:
            print(f"    충돌 {it['hash']}: 번들 {it['text'][:50]!r} vs DB {db_text[it['hash']][:50]!r}")

    if any(p["collisions"] for p in plans):
        raise SystemExit("[실패] 해시 충돌(같은 해시·다른 텍스트) — 등록하면 남의 벡터를 물려받는다")
    if not args.apply:
        print("\nDRY-RUN — --apply 로 실제 등록")
        return 0

    for plan in plans:
        v = plan["version"]
        if plan["need_vec"]:
            execute_batch(cur, """
                INSERT INTO image_embeddings (embedding_id, entity_type, entity_id, model_name, dim, embedding)
                VALUES (%s, 'prompt', %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                [(f"prompt:{it['hash']}:{PANEL_MODEL}", it["hash"], PANEL_MODEL, dim,
                  "[" + ",".join(f"{float(x):.6f}" for x in sent_vec[it["row"]]) + "]")
                 for it in plan["need_vec"]], page_size=200)
            print(f"[적용] {v}: image_embeddings {len(plan['need_vec']):,} 삽입")
        if plan["existing"]:
            bank_id = plan["existing"][0]
            cur.execute("DELETE FROM bank_sentences WHERE bank_id=%s", (bank_id,))
        else:
            bank_id = str(uuid.uuid4())
            cur.execute("""INSERT INTO prompt_banks
                (bank_id, version_tag, source, sentence_storage, origin_uri, model_name,
                 sentence_count, ingested_by, notes)
                VALUES (%s,%s,%s,'db_backed',%s,%s,%s,'register_bank_db.py',%s)""",
                (bank_id, v, args.source, bundle_dir, PANEL_MODEL, len(plan["items"]),
                 f"업로드 킷 번들 등록 ({args.dataset}, 지문 {fp})"))
        execute_batch(cur, """
            INSERT INTO bank_sentences (sentence_id, bank_id, content_hash, class_label, text, gidx, origin, adopted)
            VALUES (%s,%s,%s,%s,%s,%s,'upload-kit',TRUE) ON CONFLICT DO NOTHING""",
            [(str(uuid.uuid4()), bank_id, it["hash"], it["class"], it["text"], it["gidx"])
             for it in plan["items"]], page_size=500)
        cur.execute("UPDATE prompt_banks SET sentence_count=%s, sentence_storage='db_backed' WHERE bank_id=%s",
                    (len(plan["items"]), bank_id))
        print(f"[적용] {v}: bank_sentences {len(plan['items']):,} 삽입 (bank_id={bank_id})")
    conn.commit()

    # ── 검증: 패널이 실제로 쓰는 질의로 이 뱅크가 나오는지 (등록 성공 ≠ cos 표시) ──
    smp = ds.first()
    emb = smp["embedding"] if "embedding" in ds.get_field_schema() else None
    for plan in plans:
        v = plan["version"]
        cur.execute("""SELECT count(*), count(DISTINCT s.content_hash), count(e.entity_id)
                       FROM bank_sentences s JOIN prompt_banks b USING(bank_id)
                       LEFT JOIN image_embeddings e ON e.entity_type='prompt'
                            AND e.entity_id=s.content_hash AND e.model_name=%s
                       WHERE b.version_tag=%s""", (PANEL_MODEL, v))
        n, nh, nv = cur.fetchone()
        print(f"[검증] {v}: 문장 {n:,} · 고유해시 {nh:,} · 벡터 연결 {nv:,} ({nv / max(n, 1):.1%})")
        if emb is not None:
            vec = "[" + ",".join(f"{float(x):.6f}" for x in emb) + "]"
            cur.execute("""SELECT s.gidx, s.text, 1-(e.embedding <=> %s::vector) AS cos
                             FROM bank_sentences s JOIN prompt_banks b USING (bank_id)
                             JOIN image_embeddings e ON e.entity_type='prompt'
                              AND e.entity_id=s.content_hash AND e.model_name=%s
                            WHERE b.version_tag=%s
                            ORDER BY e.embedding <=> %s::vector LIMIT 1""",
                        (vec, PANEL_MODEL, v, vec))
            top = cur.fetchone()
            print(f"[검증] {v}: 첫 프레임 top-1 cos={top[2]:.4f} gidx={top[0]} {top[1][:60]!r}")
    conn.close()
    print("\n완료 — App 에서 프레임 1장을 선택하면 표의 cos 가 채워집니다 "
          "(패널 프로세스 캐시 때문에 이미 연 표는 프레임을 다시 골라야 갱신됨)")
    return 0


def selftest() -> int:
    rows = [
        {"version": "vA", "class": "fire", "text": "a burning car", "_row": 0},
        {"version": "vB", "class": "fire", "text": "a burning car", "_row": 1},
        {"version": "vA", "class": "smoke", "text": "  Thick   SMOKE  ", "_row": 2},
    ]
    plan = build_plan(rows, "vA", have_hashes=set(), db_text={})
    assert [it["gidx"] for it in plan["items"]] == [0, 1], plan["items"]
    assert plan["items"][1]["row"] == 2, "버전 필터 후에도 npz 행 번호가 보존돼야 한다"
    assert len(plan["need_vec"]) == 2 and plan["dup_in_bank"] == 0

    # 이미 DB 에 있는 해시는 벡터 삽입 대상이 아니다
    h0 = content_hash("a burning car")
    assert len(build_plan(rows, "vA", {h0}, {})["need_vec"]) == 1

    # 공백/대소문자만 다른 텍스트는 같은 해시 → 충돌 아님
    same = build_plan(rows, "vA", set(), {content_hash("Thick smoke"): "thick    smoke"})
    assert not same["collisions"], same["collisions"]

    # 진짜 다른 텍스트가 같은 해시로 오면 충돌로 잡힌다
    coll = build_plan(rows, "vA", set(), {h0: "a totally different sentence"})
    assert len(coll["collisions"]) == 1 and coll["collisions"][0]["hash"] == h0

    dupes = [{"version": "vA", "class": "fire", "text": "x", "_row": 0},
             {"version": "vA", "class": "smoke", "text": "X ", "_row": 1}]
    d = build_plan(dupes, "vA", set(), {})
    assert d["dup_in_bank"] == 1 and len(d["need_vec"]) == 1, d
    print("selftest OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
