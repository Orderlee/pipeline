#!/usr/bin/env python3
"""뱅크 원장 JSONL → Postgres 적재 (019) + 문장 벡터 흡수 (image_embeddings).

[[prompt_bank_ledger.py]] 가 만든 산출물을 019 스키마에 넣는 얇은 loader. 계산은 하지
않는다 — 정규화·해시·검증은 전부 ledger 쪽에서 끝났고 여기는 INSERT 만 한다.

멱등성: bank_id = uuid5("userwatch:<version_tag>"), sentence_id = uuid5("<bank_id>:<gidx>").
같은 원장을 몇 번 돌려도 같은 행을 갱신한다 (ON CONFLICT DO UPDATE).

기본은 dry-run — 실제 쓰기는 `--apply` 를 줘야 한다 (promote_model.py 관례).

사용:
    python3 prompt_bank_load.py load  <ledger_dir> --apply
    python3 prompt_bank_load.py embed <ledger_dir> --apply     # 재개 가능(기적재분 skip)
    python3 prompt_bank_load.py verify           # 적재 정합 + 벡터 귀속 감사

정본: docker/analysis/prompt_bank_load.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid

import psycopg2
from psycopg2.extras import execute_values

DSN = os.environ.get(
    "BANK_LOAD_DSN", "postgresql://airflow:airflow@localhost:15433/vlm_pipeline")
EMBED_URL = os.environ.get("EMBEDDING_API_URL", "http://localhost:8004")
MODEL_NAME = os.environ.get("BANK_EMBED_MODEL", "facebook/PE-Core-L14-336")
NS = uuid.NAMESPACE_URL


def bank_uuid(version_tag: str) -> str:
    return str(uuid.uuid5(NS, f"userwatch:{version_tag}"))


def sentence_uuid(bank_id: str, gidx) -> str:
    return str(uuid.uuid5(NS, f"{bank_id}:{gidx}"))


def log(m: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def read_jsonl(path: str):
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


# ────────────────────── load ──────────────────────
def cmd_load(args) -> int:
    inv = json.load(open(os.path.join(args.dir, "banks_inventory.json"), encoding="utf-8"))
    rows = list(read_jsonl(os.path.join(args.dir, "bank_sentences.jsonl")))

    banks = [(
        bank_uuid(b["version_tag"]), b["version_tag"], "userwatch",
        b["sentence_storage"], b["origin_uri"], b.get("model_name"),
        b.get("sentence_count"), json.dumps(b.get("class_counts")) if b.get("class_counts") else None,
        b.get("checksum"), "prompt_bank_ledger.py",
        b.get("notes") or (f"텍스트 없음({b.get('_text_source')})"
                           if b["sentence_storage"] == "external_only" else None),
    ) for b in inv]

    sents = []
    for r in rows:
        bid = bank_uuid(r["version_tag"])
        sents.append((sentence_uuid(bid, r["gidx"]), bid, r["content_hash"], r["text"],
                      r["class_label"], r["gidx"], "userwatch", bool(r.get("adopted"))))

    db_backed = sum(1 for b in inv if b["sentence_storage"] == "db_backed")
    log(f"뱅크 {len(banks)}개 (db_backed {db_backed} / external_only {len(banks) - db_backed}) "
        f"/ 문장 {len(sents):,}행")
    if not args.apply:
        log("dry-run — 쓰지 않았다. 실제 적재는 --apply")
        return 0

    with psycopg2.connect(args.dsn) as conn, conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO prompt_banks
              (bank_id, version_tag, source, sentence_storage, origin_uri, model_name,
               sentence_count, class_counts, checksum, ingested_by, notes)
            VALUES %s
            ON CONFLICT (source, version_tag) DO UPDATE SET
              sentence_storage = EXCLUDED.sentence_storage,
              origin_uri       = EXCLUDED.origin_uri,
              sentence_count   = EXCLUDED.sentence_count,
              class_counts     = EXCLUDED.class_counts,
              checksum         = EXCLUDED.checksum,
              notes            = EXCLUDED.notes
        """, banks)
        log(f"prompt_banks {cur.rowcount}행")
        execute_values(cur, """
            INSERT INTO bank_sentences
              (sentence_id, bank_id, content_hash, text, class_label, gidx, origin, adopted)
            VALUES %s
            ON CONFLICT (bank_id, gidx) DO UPDATE SET
              content_hash = EXCLUDED.content_hash,
              text         = EXCLUDED.text,
              class_label  = EXCLUDED.class_label
        """, sents, page_size=5000)
        log(f"bank_sentences {len(sents):,}행 적재")
    return cmd_verify(args)


# ────────────────────── embed ──────────────────────
def cmd_embed(args) -> int:
    import requests

    uniq = list(read_jsonl(os.path.join(args.dir, "unique_sentences.jsonl")))
    with psycopg2.connect(args.dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT entity_id FROM image_embeddings "
                    "WHERE entity_type = 'prompt' AND model_name = %s", (MODEL_NAME,))
        done = {r[0] for r in cur.fetchall()}
    todo = [u for u in uniq if u["content_hash"] not in done]
    log(f"고유 문장 {len(uniq):,} / 기적재 {len(done):,} / 남은 {len(todo):,} "
        f"(≈{len(todo) * 7.5 / 6e4:.0f}분)")
    if not args.apply:
        log("dry-run — 쓰지 않았다. 실제 임베딩은 --apply")
        return 0
    if not todo:
        return 0

    sess, buf, t0, n = requests.Session(), [], time.time(), 0
    with psycopg2.connect(args.dsn) as conn:
        for i, u in enumerate(todo):
            r = sess.post(f"{EMBED_URL}/embed_text", data={"text": u["text"]}, timeout=180)
            r.raise_for_status()
            vec = r.json()["vector"]
            buf.append((str(uuid.uuid5(NS, f"prompt:{u['content_hash']}:{MODEL_NAME}")),
                        "prompt", u["content_hash"], MODEL_NAME, len(vec),
                        "[" + ",".join(f"{x:.6f}" for x in vec) + "]", u["text"]))
            if len(buf) >= 500 or i == len(todo) - 1:
                with conn.cursor() as cur:
                    execute_values(cur, """
                        INSERT INTO image_embeddings
                          (embedding_id, entity_type, entity_id, model_name, dim,
                           embedding, text_content)
                        VALUES %s
                        ON CONFLICT (entity_type, entity_id, model_name) DO NOTHING
                    """, buf)
                conn.commit()
                n += len(buf)
                buf = []
                el = time.time() - t0
                log(f"{n:,}/{len(todo):,} ({el:.0f}s, {n / max(el, 1):.0f}/s, "
                    f"ETA {(len(todo) - n) / max(n / max(el, 1), 1e-9) / 60:.0f}분)")
    return 0


# ────────────────────── verify ──────────────────────
NPZ_DIR = os.environ.get("BANK_NPZ_DIR", "/data/fiftyone/sourceh/prompts")


def vector_hash(path: str) -> "tuple[str, tuple]":
    """뱅크 npz 의 벡터 배열 지문. 문장 텍스트와 무관하게 **좌표만** 본다."""
    import hashlib

    import numpy as np
    v = np.ascontiguousarray(np.load(path, allow_pickle=True)["vec"])
    return hashlib.sha256(v.tobytes()).hexdigest()[:16], tuple(v.shape)


def audit_vector_attribution(cur, npz_dir: str = NPZ_DIR) -> "list[str]":
    """문장 지문 ↔ 벡터 해시가 **1:1** 인지 검사. 어긋나면 벡터 귀속이 틀린 것이다.

    ⚠️ "다른 version_tag 가 같은 벡터" 를 곧바로 오류로 보면 **안 된다** — 실측 29버전 중
       4건이 벡터를 공유하는데 그 중 3건은 문장이 진짜 같아서 벡터도 같은 정상이다:
         0e17ad9c  V1.0.11.0·v1.0.6.1·v1.0.7.0·v1.0.8.0  12,480행 · 문장지문 동일 ✅
         66e6e132  v1.0.5.1·v1.0.6.0                     12,381행 · 동일        ✅
         f31e23a4  v1.0.1.0·v1.0.5.0                     12,568행 · 동일        ✅
         c1a2b4db  v1.0.2.0(12,568/869f3371) ≠ v1.0.2.1(14,600/df5a733f)       ❌
       마지막만 손상이다: v1.0.2.0 은 문장이 v1.0.1.0 과 같은데 벡터는 v1.0.2.1 것이다
       (공급자 JSON 두 개가 md5 동일 — 2026-08-19 확인). 그래서 규칙은 개수도 이름도
       아닌 **대응 관계**다. 양방향 다 본다: 한 벡터가 두 문장집합에 붙어도, 한 문장집합이
       두 벡터에 붙어도 위반이다.
    """
    import collections
    import glob

    cur.execute("""
        SELECT b.version_tag,
               md5(string_agg(s.content_hash, ',' ORDER BY s.gidx)) AS text_fp
        FROM prompt_banks b JOIN bank_sentences s USING (bank_id)
        GROUP BY b.version_tag
    """)
    text_fp = {v: fp for v, fp in cur.fetchall()}

    vec_fp, shapes = {}, {}
    for f in sorted(glob.glob(f"{npz_dir}/*.npz")):
        ver = f.rsplit("/", 1)[-1][:-4]
        vec_fp[ver], shapes[ver] = vector_hash(f)

    both = [v for v in vec_fp if v in text_fp]
    if not both:
        return [f"{npz_dir} 의 npz 버전과 DB 문장 버전이 하나도 안 겹친다 — 경로/표기 확인"]

    by_vec, by_text = collections.defaultdict(set), collections.defaultdict(set)
    for v in both:
        by_vec[vec_fp[v]].add(text_fp[v])
        by_text[text_fp[v]].add(vec_fp[v])

    bad = []
    for h, fps in by_vec.items():
        if len(fps) > 1:
            vers = sorted(v for v in both if vec_fp[v] == h)
            bad.append(f"벡터 {h} 가 문장집합 {len(fps)}개에 붙어 있다: "
                       + ", ".join(f"{v}(문장 {text_fp[v][:8]}, {shapes[v][0]:,}행)"
                                   for v in vers))
    for fp, hs in by_text.items():
        if len(hs) > 1:
            vers = sorted(v for v in both if text_fp[v] == fp)
            bad.append(f"문장집합 {fp[:8]} 이 벡터 {len(hs)}종에 붙어 있다: "
                       + ", ".join(f"{v}(벡터 {vec_fp[v]})" for v in vers))
    return bad


def cmd_verify(args) -> int:
    q = {
        "prompt_banks": "SELECT count(*) FROM prompt_banks",
        "  db_backed": "SELECT count(*) FROM prompt_banks WHERE sentence_storage='db_backed'",
        "bank_sentences": "SELECT count(*) FROM bank_sentences",
        "  고유 content_hash": "SELECT count(DISTINCT content_hash) FROM bank_sentences",
        "prompt 벡터": "SELECT count(*) FROM image_embeddings WHERE entity_type='prompt'",
    }
    bad = []
    with psycopg2.connect(args.dsn) as conn, conn.cursor() as cur:
        for k, sql in q.items():
            cur.execute(sql)
            print(f"  {k:22s} {cur.fetchone()[0]:,}")
        # 조인 폐쇄: FiftyOne 의 winner_gidx 가 가리키는 gidx 가 DB 에 다 있는가
        cur.execute("""
            SELECT count(*) FROM prompt_banks b JOIN bank_sentences s USING (bank_id)
            WHERE b.version_tag = %s AND s.gidx IS NOT NULL
        """, (args.join_bank,))
        n = cur.fetchone()[0]
        if n == 0:
            bad.append(f"{args.join_bank}: gidx 있는 문장 0행 — 조인 불가")
        else:
            print(f"  {args.join_bank} gidx 문장    {n:,}")
        # 벡터가 문장 원장 밖을 가리키지 않는가
        cur.execute("""
            SELECT count(*) FROM image_embeddings e
            WHERE e.entity_type='prompt'
              AND NOT EXISTS (SELECT 1 FROM bank_sentences s WHERE s.content_hash = e.entity_id)
        """)
        orphan = cur.fetchone()[0]
        if orphan:
            bad.append(f"prompt 벡터 {orphan:,}개가 bank_sentences 에 없는 문장을 가리킨다")
        # 벡터 귀속 계약 — 문장 지문 ↔ 벡터 해시 1:1 (위 함수 주석에 근거)
        if args.skip_vector_audit:
            print("  벡터 귀속 감사        건너뜀 (--skip-vector-audit)")
        else:
            try:
                viol = audit_vector_attribution(cur, args.npz_dir)
            except Exception as e:              # noqa: BLE001 — 감사 실패가 verify 를 못 죽인다
                print(f"  벡터 귀속 감사        ⚠️ 불가: {type(e).__name__}: {e}")
            else:
                print(f"  벡터 귀속 감사        위반 {len(viol)}건")
                bad.extend(viol)
    if bad:
        print("❌ " + "\n❌ ".join(bad))
        return 1
    print("✅ 적재 정합 OK")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="command", required=True)
    for name, fn, needs_dir in (("load", cmd_load, True), ("embed", cmd_embed, True),
                                ("verify", cmd_verify, False)):
        p = sub.add_parser(name)
        if needs_dir:
            p.add_argument("dir", help="prompt_bank_ledger.py --out 디렉토리")
            p.add_argument("--apply", action="store_true", help="실제 쓰기 (기본 dry-run)")
        p.add_argument("--dsn", default=DSN)
        p.add_argument("--join-bank", default="v1.0.8.0")
        p.add_argument("--npz-dir", default=NPZ_DIR,
                       help="뱅크 벡터 npz 디렉토리 (벡터 귀속 감사용)")
        p.add_argument("--skip-vector-audit", action="store_true",
                       help="벡터 귀속 감사 생략 (npz 가 없는 환경)")
        p.set_defaults(func=fn)
    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
