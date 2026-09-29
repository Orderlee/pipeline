#!/usr/bin/env python3
"""archive_sourcep 프레임의 `ambiguous` 를 events JSON 구간 중첩으로 다시 계산한다.

왜: sitej_certbody 는 적재 때 '한 시각을 두 클래스 구간이 덮으면 ambiguous' 규칙을 적용했는데
archive_sourcep 는 빠뜨렸다. 그 결과 fire·smoke 가 실제로 동시에 일어나는 sourcep 에서
같은 순간이 fire 프레임과 smoke 프레임으로 각각 들어가 **시각적으로 동일한데 라벨이 다른**
표본이 1,380장 생겼다. 전이 행렬에서 sourcep 행·열이 전부 무너진 원인이다
(자기 대각선 0.370 — 다른 현장 어디서 학습해도 0.275~0.403).

규칙은 sitej 와 동일: 프레임 시각 t 를 덮는 이벤트 카테고리가 2개 이상이면 ambiguous.
경계 ±0.25s 는 boundary. **행을 지우지 않는다** — 플래그만 세워 채점에서 빠지게 한다.
"""
import json
import os
from collections import defaultdict

import boto3
import psycopg2
from botocore.client import Config
from psycopg2.extras import execute_values

PG = dict(host="docker-postgres-1", port=5432, user="airflow",
          password=os.environ.get("POSTGRES_PASSWORD", "airflow"), dbname="vlm_pipeline")
COHORT = "archive_sourcep"
MARGIN = 0.25


def main():
    s3 = boto3.client("s3", endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://10.0.0.51:9000"),
                      aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
                      aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
                      config=Config(signature_version="s3v4"))
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute("SELECT frame_key, t_sec, cls, extra->>'labels_key' FROM al_frames WHERE cohort=%s", (COHORT,))
    rows = cur.fetchall()

    ev = {}
    for _, _, _, lkey in rows:
        if lkey and lkey not in ev:
            try:
                d = json.loads(s3.get_object(Bucket="vlm-labels", Key=lkey)["Body"].read())
                ev[lkey] = [(e.get("category"), float(e["timestamp"][0]), float(e["timestamp"][1]))
                            for e in d if e.get("timestamp")]
            except Exception:
                ev[lkey] = []
    print(f"events JSON {len(ev)}개")

    upd, stats = [], defaultdict(int)
    for fk, t, cls, lkey in rows:
        evs = ev.get(lkey, [])
        if t is None or not evs:
            continue
        cover = {c for c, a, b in evs if a <= t <= b}
        bnd = any(abs(t - a) <= MARGIN or abs(t - b) <= MARGIN for _, a, b in evs)
        amb = len(cover) > 1
        upd.append((amb, bnd, sorted(cover), COHORT, fk))
        stats["ambiguous"] += amb; stats["boundary"] += bnd
    execute_values(cur,
        "UPDATE al_frames SET ambiguous=v.amb, boundary=v.bnd, "
        "extra = al_frames.extra || jsonb_build_object('covering', to_jsonb(v.cov)) "
        "FROM (VALUES %s) AS v(amb,bnd,cov,co,fk) "
        "WHERE al_frames.cohort=v.co AND al_frames.frame_key=v.fk",
        upd, template="(%s::boolean,%s::boolean,%s::text[],%s,%s)", page_size=500)
    conn.commit()
    print(f"갱신 {len(upd)} · ambiguous {stats['ambiguous']} · boundary {stats['boundary']}")
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
