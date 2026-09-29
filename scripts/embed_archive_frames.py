#!/usr/bin/env python3
"""아카이브 코호트 프레임을 PE-Core 로 임베딩하고 이벤트(video) 벡터까지 만든다.

  docker exec -i docker-dagster-daemon-1 python3 - < scripts/embed_archive_frames.py           # dry-run
  docker exec -i docker-dagster-daemon-1 python3 - -- --apply < scripts/embed_archive_frames.py

**왜 `frame_embedding` asset 을 안 쓰나** — `_PENDING_FRAMES_SQL` 이
`ORDER BY im.image_id LIMIT n` 인데 **코호트 필터가 없다**. 지금 임베딩 대기가
raw_video_frame 377,218 + processed_clip_frame 13,682 인데, 그중 우리 것 3,897 을 뺀
**나머지는 전부 MinIO 객체가 소실**된 행이다(2026-08-31 NAS 재구축). 그냥 돌리면 죽은 행부터
집어서 실패하고, `inserted == 0` 이면 asset 이 `Failure` 를 던진다.
그래서 이 스크립트는 **`data_source='archive_import'` 로 좁혀** 직접 넣는다.

두 단계:
  1) frame  — 프레임 JPEG → PE-Core → `image_embeddings(entity_type='frame')`
  2) video  — 클립(=이벤트)별 프레임 벡터의 **L2 정규화 평균** →
              `image_embeddings(entity_type='video')`
              `video_embedding` asset 의 frame_pool 방식과 동일한 정의(SQL 집계).
              **이게 타임스탬프 트랙 AL 의 후보 좌표**다 — 라벨링 단위가 영상/이벤트이므로
              프레임이 아니라 이벤트 벡터로 골라야 한다.

⚠️ 서비스가 lazy-load 라 첫 호출 전에 warmup 이 필요하다(`model_loaded:false` 로 시작).
"""

from __future__ import annotations

import argparse
import os
import sys
from uuid import uuid4

sys.path.insert(0, "/src/vlm")

from vlm_pipeline.lib.embedding import get_embedding_client  # noqa: E402

MODEL = os.environ.get("EMBED_MODEL", "facebook/PE-Core-L14-336")
VIDEO_MODEL = f"{MODEL}/framepool"

_PENDING_SQL = """
SELECT im.image_id, im.image_bucket, im.image_key
FROM image_metadata im
JOIN processed_clips pc ON pc.clip_id = im.source_clip_id
WHERE pc.data_source = 'archive_import'
  AND im.image_bucket IS NOT NULL AND im.image_key IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM image_embeddings e
      WHERE e.entity_type = 'frame' AND e.entity_id = im.image_id AND e.model_name = %(model)s
  )
ORDER BY im.image_id
"""

# 클립(=이벤트) 단위 프레임 평균. pgvector 에서 바로 집계해 서비스 호출이 없다.
_VIDEO_POOL_SQL = """
INSERT INTO image_embeddings (embedding_id, entity_type, entity_id, image_id, model_name, dim, embedding, asset_id, created_at)
SELECT gen_random_uuid()::text, 'video', pc.clip_id, NULL, %(vmodel)s, 1024,
       (avg(e.embedding))::vector(1024), pc.source_asset_id, now()
FROM processed_clips pc
JOIN image_metadata im ON im.source_clip_id = pc.clip_id
JOIN image_embeddings e ON e.entity_type = 'frame' AND e.entity_id = im.image_id
                       AND e.model_name = %(model)s
WHERE pc.data_source = 'archive_import'
GROUP BY pc.clip_id, pc.source_asset_id
ON CONFLICT (entity_type, entity_id, model_name) DO NOTHING
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    import psycopg2
    from vlm_pipeline.resources.minio import MinIOResource

    conn = psycopg2.connect(os.environ["DATAOPS_POSTGRES_DSN"])
    cur = conn.cursor()
    minio = MinIOResource(
        endpoint=os.environ.get("MINIO_ENDPOINT", "http://10.0.0.51:9000"),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )

    cur.execute(_PENDING_SQL, {"model": MODEL})
    rows = cur.fetchall()
    if args.limit:
        rows = rows[: args.limit]
    print(f"[INFO] 임베딩 대기 프레임 {len(rows)} (아카이브 코호트 한정)")

    if not args.apply:
        print("\n[DRY-RUN] --apply 를 주면 실제로 임베딩한다.")
        return

    client = get_embedding_client()
    print("[INFO] 모델 warmup … (lazy-load 라 첫 호출 전에 필요)")
    if not client.wait_until_ready(max_wait_sec=300.0):
        raise SystemExit("임베딩 서비스가 준비되지 않았다")
    print("[OK] 준비 완료")

    ok = fail = 0
    for i, (image_id, bucket, key) in enumerate(rows, start=1):
        try:
            blob = minio.download(bucket, key)
            vec = client.embed(blob)
            cur.execute(
                """INSERT INTO image_embeddings
                   (embedding_id, entity_type, entity_id, image_id, model_name, dim, embedding,
                    source_bucket, source_key, created_at)
                   VALUES (%s,'frame',%s,%s,%s,%s,%s,%s,%s,now())
                   ON CONFLICT (entity_type, entity_id, model_name) DO NOTHING""",
                (str(uuid4()), image_id, image_id, MODEL, len(vec), str(vec), bucket, key),
            )
            ok += 1
        except Exception as exc:  # noqa: BLE001 — per-frame fail-forward
            fail += 1
            if fail <= 5:
                print(f"[WARN] {key[:60]}: {str(exc)[:90]}")
        if i % 200 == 0:
            conn.commit()
            print(f"  … {i}/{len(rows)} · ok {ok} fail {fail}", flush=True)
    conn.commit()
    print(f"\n[FRAME] ok {ok} · fail {fail}")

    # --- 이벤트(video) 벡터 = 클립별 프레임 평균 ---
    cur.execute(_VIDEO_POOL_SQL, {"model": MODEL, "vmodel": VIDEO_MODEL})
    conn.commit()
    cur.execute(
        "SELECT count(*) FROM image_embeddings WHERE entity_type='video' AND model_name=%s",
        (VIDEO_MODEL,),
    )
    print(f"[VIDEO] 이벤트 벡터 {cur.fetchone()[0]} (model={VIDEO_MODEL})")


if __name__ == "__main__":
    main()
