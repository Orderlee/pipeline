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
    python3 prompt_bank_load.py verify

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
        p.set_defaults(func=fn)
    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
