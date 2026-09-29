#!/usr/bin/env python3
"""아카이브 사람확정 프레임 3,897장 → `al_frames` 편입.

클래스 정본은 **MinIO events JSON 의 `events[i].category`** 다.
폴더 경로(`sourcep/organized_videos/fire/...`)로 쓰면 안 된다 — `fire/` 안에 smoke 86건이
섞여 있어 773 중 112건(14.5%)이 오라벨된다([[project-archive-poc-export]]).

label_source='human': 이 라벨은 `labels.label_source='manual_review'` +
`review_status='finalized'` 다(708행 전량). 지금까지 코호트 중 유일하게 사람 검수 확정이다.
group_key: sourcep 는 카메라 식별자가 어디에도 없다 → **부모 asset(영상)** 을 그룹으로 쓴다.
같은 영상 프레임끼리는 독립이 아니므로 최소한 그 단위는 묶어야 한다.

임베딩은 이미 `image_embeddings`(entity_type='frame') 에 있으므로 **SQL 로 복사**한다
(Python 왕복 없음). 같은 벡터가 두 entity_type 으로 존재하지만 조인 패턴이 하나로 통일된다.
"""
import json
import os

import boto3
import psycopg2
from botocore.client import Config
from psycopg2.extras import execute_values

PG = dict(host="docker-postgres-1", port=5432, user="airflow",
          password=os.environ.get("POSTGRES_PASSWORD", "airflow"), dbname="vlm_pipeline")
COHORT = "archive_sourcep"

SQL_FRAMES = """
SELECT e.entity_id AS image_id, im.source_asset_id, im.image_bucket, im.image_key,
       l.labels_key, l.event_index, im.frame_sec, r.raw_key
FROM image_embeddings e
JOIN image_metadata im  ON im.image_id = e.entity_id AND e.entity_type = 'frame'
JOIN processed_clips pc ON pc.clip_id  = im.source_clip_id
JOIN labels l           ON l.label_id  = pc.source_label_id
JOIN raw_files r        ON r.asset_id  = im.source_asset_id
WHERE im.image_role = 'processed_clip_frame'
"""

COLS = ["cohort", "frame_key", "media_uri", "site", "cls", "label_rule", "label_source",
        "group_key", "camera", "session", "video_stem", "t_sec", "ambiguous", "boundary",
        "n_passes", "asset_id", "extra"]


def main():
    s3 = boto3.client("s3", endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://10.0.0.51:9000"),
                      aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
                      aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
                      config=Config(signature_version="s3v4"))
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute(SQL_FRAMES)
    rows = cur.fetchall()
    print(f"아카이브 프레임 {len(rows):,}")

    cache, miss = {}, 0
    def category(labels_key, idx):
        nonlocal miss
        if labels_key not in cache:
            try:
                cache[labels_key] = json.loads(s3.get_object(Bucket="vlm-labels", Key=labels_key)["Body"].read())
            except Exception:
                cache[labels_key] = None
        ev = cache[labels_key]
        if not isinstance(ev, list) or idx is None or idx >= len(ev):
            miss += 1
            return None
        return ev[idx].get("category")

    meta = []
    for image_id, asset_id, bucket, key, lkey, eidx, fsec, raw_key in rows:
        cls = category(lkey, eidx)
        meta.append((COHORT, image_id, f"minio://{bucket}/{key}", "sourcep", cls,
                     "archive_events_json", "human",
                     asset_id,                      # group_key = 부모 영상(카메라 식별자 없음)
                     None, None, raw_key, fsec, False, False, None, asset_id,
                     json.dumps({"labels_key": lkey, "event_index": eidx}, ensure_ascii=False)))
    print(f"events JSON {len(cache)}개 조회 · 카테고리 미해소 {miss}")

    execute_values(cur, f"INSERT INTO al_frames ({','.join(COLS)}) VALUES %s "
                        "ON CONFLICT (cohort, frame_key) DO UPDATE SET "
                        + ",".join(f"{c}=EXCLUDED.{c}" for c in COLS if c not in ("cohort", "frame_key")),
                   meta, page_size=500)
    # 임베딩은 기존 'frame' 행을 그대로 복제 — Python 왕복 불필요
    cur.execute("""
        INSERT INTO image_embeddings (embedding_id, entity_type, entity_id, model_name, dim, embedding)
        SELECT 'al_frame:' || %s || '/' || f.frame_key, 'al_frame', %s || '/' || f.frame_key,
               e.model_name, e.dim, e.embedding
        FROM al_frames f
        JOIN image_embeddings e ON e.entity_type='frame' AND e.entity_id = f.frame_key
        WHERE f.cohort = %s
        ON CONFLICT (entity_type, entity_id, model_name) DO UPDATE SET embedding = EXCLUDED.embedding
    """, (COHORT, COHORT, COHORT))
    print(f"임베딩 복제 {cur.rowcount}")
    conn.commit(); cur.close(); conn.close()


if __name__ == "__main__":
    main()
