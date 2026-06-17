"""prod 임베딩 backfill (standalone, dagster 비의존) — 무중단 적용용.

analysis 컨테이너 안에서 실행. prod PG(DATAOPS_POSTGRES_DSN) + prod MinIO(MINIO_*) +
embedding-service(EMBEDDING_API_URL) 를 사용. image_embeddings 의 upsert 규칙·entity_id·
model_name 을 dagster 헬퍼(defs/embed/helpers.py, resources/postgres_embedding.py)와
**동일하게** 맞춰, 추후 dagster 라이브 backfill 이 중복 생성하지 않도록 한다.

  python backfill_prod_embeddings.py captions [limit]
  python backfill_prod_embeddings.py frames   [limit]   # limit = 샘플 크기

ON CONFLICT(entity_type,entity_id,model_name) upsert → 재실행 안전. per-item fail-forward.
주의: embedding-service /health 의 model_name 이 MODEL 과 일치해야 함(불일치 시 중단).
Codex 리뷰 반영: read tx 즉시 commit(idle-in-tx 방지), flush rollback, 누락 cred 하드페일,
silent-zero 하드페일, limit 도달 경고, non-finite 벡터 거부.
"""

from __future__ import annotations

import logging
import math
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import psycopg2

from embedding_client import EmbeddingClient  # = lib/embedding.py (컨테이너에 cp)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill")

MODEL = "facebook/PE-Core-L14-336"  # = defs/embed/assets.DEFAULT_MODEL
FRAME_ROLES = ["processed_clip_frame", "raw_video_frame", "source_image"]
WORKERS = int(os.environ.get("BACKFILL_WORKERS", "6"))
CHUNK = 200

# --- postgres_embedding.py 에서 verbatim 복사 (divergence 방지) ---
PENDING_CAPTIONS_SQL = """
SELECT labels.label_id, labels.asset_id, labels.caption_text
FROM labels
WHERE labels.caption_text IS NOT NULL AND labels.caption_text <> ''
AND NOT EXISTS (
    SELECT 1 FROM image_embeddings e
    WHERE e.entity_type = 'caption' AND e.entity_id = labels.label_id AND e.model_name = %(model_name)s
)
ORDER BY labels.label_id
LIMIT %(limit)s
"""

PENDING_FRAMES_SQL = """
SELECT im.image_id, im.image_bucket, im.image_key, im.image_role
FROM image_metadata im
WHERE im.image_bucket IS NOT NULL AND im.image_key IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM image_embeddings e
    WHERE e.entity_type = 'frame' AND e.entity_id = im.image_id AND e.model_name = %(model_name)s
)
AND im.image_role = ANY(%(roles)s)
ORDER BY im.image_id
LIMIT %(limit)s
"""

INSERT_SQL = """
INSERT INTO image_embeddings
  (embedding_id, entity_type, entity_id, image_id, model_name, dim, embedding,
   source_bucket, source_key, bbox, asset_id, text_content)
VALUES (%(embedding_id)s, %(entity_type)s, %(entity_id)s, %(image_id)s, %(model_name)s, %(dim)s,
        %(embedding)s::vector, %(source_bucket)s, %(source_key)s, %(bbox)s,
        %(asset_id)s, %(text_content)s)
ON CONFLICT (entity_type, entity_id, model_name) DO UPDATE SET
  embedding = EXCLUDED.embedding, dim = EXCLUDED.dim, image_id = EXCLUDED.image_id,
  source_bucket = EXCLUDED.source_bucket, source_key = EXCLUDED.source_key,
  bbox = EXCLUDED.bbox, asset_id = EXCLUDED.asset_id, text_content = EXCLUDED.text_content,
  created_at = now()
"""


def require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"required env var missing or empty: {name}")
    return v


def vec_lit(values) -> str:
    floats = [float(x) for x in values]
    if not all(math.isfinite(x) for x in floats):
        raise ValueError("non-finite value in embedding vector")
    return "[" + ",".join(repr(x) for x in floats) + "]"


_tl = threading.local()
_minio_tl = threading.local()


def _client() -> EmbeddingClient:
    c = getattr(_tl, "c", None)
    if c is None:
        c = EmbeddingClient()
        _tl.c = c
    return c


def _minio():
    c = getattr(_minio_tl, "c", None)
    if c is None:
        c = boto3.client(
            "s3",
            endpoint_url=require_env("MINIO_ENDPOINT"),
            aws_access_key_id=require_env("MINIO_ACCESS_KEY"),
            aws_secret_access_key=require_env("MINIO_SECRET_KEY"),
        )
        _minio_tl.c = c
    return c


def caption_row(rec):
    label_id, asset_id, caption_text = rec
    vec = _client().embed_text(caption_text)
    if len(vec) != 1024:
        raise ValueError(f"dim {len(vec)}")
    return {
        "embedding_id": f"caption|{label_id}|{MODEL}",
        "entity_type": "caption",
        "entity_id": label_id,
        "image_id": None,
        "asset_id": asset_id,
        "model_name": MODEL,
        "dim": len(vec),
        "embedding": vec_lit(vec),
        "source_bucket": None,
        "source_key": None,
        "bbox": None,
        "text_content": caption_text,
    }


def frame_row(rec):
    image_id, bucket, key, _role = rec
    obj = _minio().get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    vec = _client().embed(data)
    if len(vec) != 1024:
        raise ValueError(f"dim {len(vec)}")
    return {
        "embedding_id": f"frame|{image_id}|{MODEL}",
        "entity_type": "frame",
        "entity_id": image_id,
        "image_id": image_id,
        "asset_id": None,
        "model_name": MODEL,
        "dim": len(vec),
        "embedding": vec_lit(vec),
        "source_bucket": bucket,
        "source_key": key,
        "bbox": None,
        "text_content": None,
    }


def _flush(conn, rows, kind, done, failed) -> int:
    """rows 배치 upsert. 실패 시 rollback 후 raise. 적재 건수 반환."""
    if not rows:
        return 0
    try:
        with conn.cursor() as cur:
            cur.executemany(INSERT_SQL, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("%s: insert chunk failed; rolled back %d rows", kind, len(rows))
        raise
    log.info("%s: upserted %d rows (done=%d failed=%d)", kind, len(rows), done, failed)
    return len(rows)


def run(kind: str, limit: int):
    conn = psycopg2.connect(require_env("DATAOPS_POSTGRES_DSN"))
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            if kind == "captions":
                cur.execute(PENDING_CAPTIONS_SQL, {"model_name": MODEL, "limit": limit})
                builder = caption_row
            else:
                cur.execute(PENDING_FRAMES_SQL, {"model_name": MODEL, "roles": FRAME_ROLES, "limit": limit})
                builder = frame_row
            pending = cur.fetchall()
        conn.commit()  # MF-1: read tx 즉시 종료 (idle-in-tx + autovacuum 차단 방지)

        log.info("%s: pending=%d (limit=%d) model=%s workers=%d", kind, len(pending), limit, MODEL, WORKERS)
        if len(pending) == limit:
            log.warning(
                "%s: 결과가 limit(%d)에 도달 — 전체 backlog 가 더 클 수 있음. 더 큰 limit 로 재실행.", kind, limit
            )
        if not pending:
            log.info("%s: nothing to do", kind)
            return

        rows, failed, done, inserted = [], 0, 0, 0
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(builder, rec): rec for rec in pending}
            for fut in as_completed(futs):
                done += 1
                try:
                    rows.append(fut.result())
                except Exception as exc:  # noqa: BLE001 per-item fail-forward
                    failed += 1
                    log.warning("%s embed failed: %s", kind, exc)
                if len(rows) >= CHUNK:
                    inserted += _flush(conn, rows, kind, done, failed)
                    rows = []
        inserted += _flush(conn, rows, kind, done, failed)

        log.info("%s: DONE done=%d failed=%d inserted=%d", kind, done, failed, inserted)
        if pending and inserted == 0:  # MF-4: silent-zero 하드페일
            raise RuntimeError(
                f"{kind}: pending={len(pending)} 인데 0건 적재 — embedding-service / MinIO 자격증명 확인"
            )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    kind = sys.argv[1] if len(sys.argv) > 1 else "captions"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else (100000 if kind == "captions" else 3000)
    assert kind in ("captions", "frames"), kind

    client = EmbeddingClient()
    if not client.wait_until_ready(max_wait_sec=60):
        log.error("embedding-service not ready at %s", client.base_url)
        sys.exit(1)
    # N-2: 서비스 모델 신원 확인 (다른 1024-d 모델이면 MODEL 키로 잘못 적재됨)
    info = client._session.get(f"{client.base_url}/health", timeout=5).json()
    if info.get("model_name") != MODEL:
        log.error("service model mismatch: expected %s got %s", MODEL, info.get("model_name"))
        sys.exit(1)
    run(kind, limit)
