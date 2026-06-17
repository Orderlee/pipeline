#!/usr/bin/env python3
"""pgvector HNSW recall/latency 벤치 — entity_type 별, ef_search 스윕.

prod 규모에서 재실행해 ef_search/빌드파라미터를 튜닝하는 용도. read-only.
사용: DATAOPS_POSTGRES_DSN=... python scripts/bench_pgvector_hnsw.py [--entity frame] [--k 10] [--samples 100]
HNSW(근사) vs seqscan(exact) top-k 겹침으로 recall@k 측정 + p50/p95 latency.
"""

from __future__ import annotations

import argparse
import os
import statistics
import time

import psycopg2

MODEL = "facebook/PE-Core-L14-336"
EF_GRID = [10, 40, 100, 200, 400]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entity", default="frame")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--samples", type=int, default=100)
    ap.add_argument("--model", default=MODEL)
    args = ap.parse_args()

    conn = psycopg2.connect(os.environ["DATAOPS_POSTGRES_DSN"])
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        "SELECT count(*) FROM image_embeddings WHERE entity_type=%s AND model_name=%s",
        (args.entity, args.model),
    )
    n = cur.fetchone()[0]
    cur.execute(
        "SELECT entity_id, embedding FROM image_embeddings WHERE entity_type=%s AND model_name=%s "
        "ORDER BY random() LIMIT %s",
        (args.entity, args.model, args.samples),
    )
    samples = cur.fetchall()
    k = args.k

    def topk(emb, ef=None, exact=False):
        cur.execute("SET enable_indexscan=%s" % ("off" if exact else "on"))
        cur.execute("SET enable_bitmapscan=%s" % ("off" if exact else "on"))
        if ef:
            cur.execute("SET hnsw.ef_search=%d" % ef)
        t = time.perf_counter()
        cur.execute(
            "SELECT entity_id FROM image_embeddings WHERE entity_type=%s "
            "ORDER BY embedding <=> %s::vector LIMIT %s",
            (args.entity, emb, k),
        )
        return [r[0] for r in cur.fetchall()], (time.perf_counter() - t) * 1000

    truth = {eid: set(topk(emb, exact=True)[0]) for eid, emb in samples}
    print(f"N={n} entity={args.entity} queries={len(samples)} K={k} (HNSW build=default m=16/ef_construction=64)")
    for ef in EF_GRID:
        recs, lats = [], []
        for eid, emb in samples:
            rows, lat = topk(emb, ef=ef)
            recs.append(len(truth[eid] & set(rows)) / k)
            lats.append(lat)
        lats.sort()
        p95 = lats[min(len(lats) - 1, int(len(lats) * 0.95))]
        print(f"ef_search={ef:4d}  recall@{k}={statistics.mean(recs):.3f}  p50={statistics.median(lats):.2f}ms  p95={p95:.2f}ms")


if __name__ == "__main__":
    main()
