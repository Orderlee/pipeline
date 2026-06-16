# pgvector 임베딩 파이프라인 (P0+P1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dagster 에 프레임 임베딩(PE-Core-L14-336 → pgvector) 파이프라인을 추가한다 — 라이브 sensor 자동 + 수동 트리거, staging 에서 검증 가능.

**Architecture:** 신규 `embedding-service` 컨테이너(FastAPI, open_clip 백엔드, GPU 0)가 이미지/텍스트 임베딩을 서빙. Dagster `defs/embed/` 의 backlog sensor 가 `image_metadata` 에서 미임베딩 프레임을 감지 → `frame_embedding_asset` 가 MinIO 이미지를 HTTP 로 임베딩 → `image_embeddings`(pgvector 1024-d) 에 upsert. SAM3 검출 패턴([defs/sam/](../../../src/vlm_pipeline/defs/sam/))을 1:1 미러.

**Tech Stack:** Dagster, Postgres 15 + pgvector 0.8.2, open_clip (`timm/PE-Core-L-14-336`), FastAPI, psycopg2, moto[s3], pytest.

**범위:** 이 plan 은 **P0(인프라) + P1(프레임 경로)** 만. 검출 bbox(P2)·검색 helper(P3)·prod 적용(P4)은 별도 plan.

**검증 환경:** staging (`pipeline-test-postgres-1`/`vlm_pipeline_staging`, 이미 pgvector 활성, 프레임 2,416개). prod 미적용.

**Spec:** [docs/design-docs/2026-06-15-dagster-pgvector-embeddings-design.md](../../design-docs/2026-06-15-dagster-pgvector-embeddings-design.md)

---

## File Structure

| 파일 | 책임 | Create/Modify |
|---|---|---|
| `src/vlm_pipeline/sql/migrations/postgres/006_image_embeddings.sql` | pgvector extension + `image_embeddings` 테이블 + 인덱스 | Create |
| `src/vlm_pipeline/resources/postgres_embedding.py` | `PostgresEmbeddingMixin`: 미임베딩 프레임 조회 + 배치 upsert | Create |
| `src/vlm_pipeline/resources/postgres.py` | 상속에 mixin 추가 | Modify |
| `tests/conftest.py` | 테스트 PG resource MRO 에 mixin 추가 | Modify |
| `src/vlm_pipeline/lib/embedding.py` | `EmbeddingClient` HTTP 클라이언트 | Create |
| `docker/embedding/app.py` `backends/{base,open_clip_be}.py` `requirements.txt` `Dockerfile` | 임베딩 서비스 | Create |
| `docker/docker-compose.yaml` | `embedding` 서비스 + `EMBEDDING_API_URL` env | Modify |
| `src/vlm_pipeline/defs/embed/{__init__,helpers,assets,sensor}.py` | 프레임 임베딩 도메인 | Create |
| `src/vlm_pipeline/definitions_production.py` (+ staging) | 등록 | Modify |
| `tests/unit/test_*` | 각 단위 테스트 | Create |

---

## P0 — 인프라

### Task 1: `006_image_embeddings.sql` 마이그레이션

**Files:**
- Create: `src/vlm_pipeline/sql/migrations/postgres/006_image_embeddings.sql`
- Modify: `tests/conftest.py` (MRO 는 Task 2 에서 함께)
- Test: `tests/unit/test_pgvector_migration.py`

- [ ] **Step 1: 마이그레이션 SQL 작성**

```sql
-- 006_image_embeddings.sql — pgvector 임베딩 저장 (frame + detection)
BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS image_embeddings (
  embedding_id  TEXT PRIMARY KEY,
  entity_type   TEXT NOT NULL,
  entity_id     TEXT NOT NULL,
  image_id      TEXT NOT NULL,
  model_name    TEXT NOT NULL,
  dim           INTEGER NOT NULL,
  embedding     vector(1024) NOT NULL,
  source_bucket TEXT,
  source_key    TEXT,
  bbox          JSONB,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (entity_type, entity_id, model_name)
);

CREATE INDEX IF NOT EXISTS image_embeddings_entity_idx ON image_embeddings (entity_type, model_name);
CREATE INDEX IF NOT EXISTS image_embeddings_image_idx  ON image_embeddings (image_id);
CREATE INDEX IF NOT EXISTS image_embeddings_hnsw ON image_embeddings USING hnsw (embedding vector_cosine_ops);

COMMIT;
-- @ASSERT_AFTER: SELECT 1 FROM pg_extension WHERE extname='vector'
-- @ASSERT_AFTER: SELECT 1 FROM information_schema.tables WHERE table_name='image_embeddings'
```

> ⚠️ `_REQUIRED_MIGRATIONS`([postgres_migration.py:80-88](../../../src/vlm_pipeline/resources/postgres_migration.py#L80))에는 **추가하지 않는다** — pgvector 미적용 환경(prod 현재) 부팅 실패 방지.

- [ ] **Step 2: 테스트 작성 (pgvector 없으면 skip)**

```python
# tests/unit/test_pgvector_migration.py
import pytest

def _has_pgvector(db):
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_available_extensions WHERE name='vector'")
        return cur.fetchone() is not None

def test_image_embeddings_table_created(postgres_resource):
    db = postgres_resource
    if not _has_pgvector(db):
        pytest.skip("test PG has no pgvector extension available")
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name='image_embeddings'")
        assert cur.fetchone() is not None
        cur.execute("SELECT 1 FROM pg_extension WHERE extname='vector'")
        assert cur.fetchone() is not None
```

> `postgres_resource` fixture 는 per-test DB 생성 후 `ensure_schema()`(모든 마이그레이션 적용)를 호출한다 — [conftest.py](../../../tests/conftest.py) 참조. 따라서 006 도 자동 적용된다.

- [ ] **Step 3: 테스트 실행 (PG fixture 가용 시)**

Run: `DATAOPS_TEST_POSTGRES_DSN=postgresql://postgres:test@localhost:5432/postgres pytest tests/unit/test_pgvector_migration.py -v`
Expected: PASS (또는 pgvector 없는 PG 면 SKIP)

- [ ] **Step 4: Commit**

```bash
git add src/vlm_pipeline/sql/migrations/postgres/006_image_embeddings.sql tests/unit/test_pgvector_migration.py
git commit -m "feat(embed): add 006 image_embeddings pgvector migration"
```

---

### Task 2: `PostgresEmbeddingMixin` resource

**Files:**
- Create: `src/vlm_pipeline/resources/postgres_embedding.py`
- Modify: `src/vlm_pipeline/resources/postgres.py:30` (상속 추가), `tests/conftest.py` (MRO 추가)
- Test: `tests/unit/test_postgres_embedding.py`

- [ ] **Step 1: 실패 테스트 작성**

```python
# tests/unit/test_postgres_embedding.py
import pytest

def _has_pgvector(db):
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_available_extensions WHERE name='vector'")
        return cur.fetchone() is not None

MODEL = "facebook/PE-Core-L14-336"

def _seed_frame(db, image_id, role="raw_video_frame"):
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO image_metadata (image_id, source_asset_id, image_bucket, image_key, image_role) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (image_id) DO NOTHING",
            (image_id, "asset1", "vlm-processed", f"u/image/{image_id}.jpg", role),
        )

def test_find_pending_frame_embeddings_returns_unembedded(postgres_resource):
    db = postgres_resource
    if not _has_pgvector(db):
        pytest.skip("no pgvector")
    _seed_frame(db, "f1"); _seed_frame(db, "f2")
    pending = db.find_pending_frame_embeddings(model_name=MODEL, limit=10)
    ids = {p["image_id"] for p in pending}
    assert {"f1", "f2"} <= ids

def test_batch_insert_then_pending_excludes(postgres_resource):
    db = postgres_resource
    if not _has_pgvector(db):
        pytest.skip("no pgvector")
    _seed_frame(db, "f3")
    row = {
        "embedding_id": "frame|f3|" + MODEL, "entity_type": "frame", "entity_id": "f3",
        "image_id": "f3", "model_name": MODEL, "dim": 1024, "embedding": [0.1] * 1024,
        "source_bucket": "vlm-processed", "source_key": "u/image/f3.jpg", "bbox": None,
    }
    assert db.batch_insert_embeddings([row]) == 1
    pending = db.find_pending_frame_embeddings(model_name=MODEL, limit=10)
    assert "f3" not in {p["image_id"] for p in pending}

def test_batch_insert_idempotent(postgres_resource):
    db = postgres_resource
    if not _has_pgvector(db):
        pytest.skip("no pgvector")
    _seed_frame(db, "f4")
    row = {
        "embedding_id": "frame|f4|" + MODEL, "entity_type": "frame", "entity_id": "f4",
        "image_id": "f4", "model_name": MODEL, "dim": 1024, "embedding": [0.2] * 1024,
        "source_bucket": "vlm-processed", "source_key": "u/image/f4.jpg", "bbox": None,
    }
    db.batch_insert_embeddings([row])
    db.batch_insert_embeddings([row])  # 재실행 안전
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM image_embeddings WHERE image_id='f4'")
        assert cur.fetchone()[0] == 1
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `pytest tests/unit/test_postgres_embedding.py -v`
Expected: FAIL (`AttributeError: ... find_pending_frame_embeddings`)

- [ ] **Step 3: Mixin 구현**

```python
# src/vlm_pipeline/resources/postgres_embedding.py
"""image_embeddings (pgvector) 조회/적재 mixin. postgres_detection.py 패턴 미러."""
from __future__ import annotations

import json
from typing import Any

_INSERT_SQL = """
INSERT INTO image_embeddings
  (embedding_id, entity_type, entity_id, image_id, model_name, dim, embedding, source_bucket, source_key, bbox)
VALUES (%(embedding_id)s, %(entity_type)s, %(entity_id)s, %(image_id)s, %(model_name)s, %(dim)s,
        %(embedding)s, %(source_bucket)s, %(source_key)s, %(bbox)s)
ON CONFLICT (entity_type, entity_id, model_name) DO UPDATE SET
  embedding = EXCLUDED.embedding, dim = EXCLUDED.dim,
  source_bucket = EXCLUDED.source_bucket, source_key = EXCLUDED.source_key,
  bbox = EXCLUDED.bbox, created_at = now()
"""

_PENDING_FRAMES_SQL = """
SELECT im.image_id, im.image_bucket, im.image_key, im.image_role
FROM image_metadata im
WHERE NOT EXISTS (
    SELECT 1 FROM image_embeddings e
    WHERE e.entity_type='frame' AND e.entity_id=im.image_id AND e.model_name=%(model_name)s
)
{role_filter}
ORDER BY im.image_id
LIMIT %(limit)s
"""


class PostgresEmbeddingMixin:
    def find_pending_frame_embeddings(
        self, model_name: str, limit: int = 500, image_roles: list[str] | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"model_name": model_name, "limit": limit}
        role_filter = ""
        if image_roles:
            role_filter = "AND im.image_role = ANY(%(roles)s)"
            params["roles"] = list(image_roles)
        sql = _PENDING_FRAMES_SQL.format(role_filter=role_filter)
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c.name for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def batch_insert_embeddings(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        payload = []
        for r in rows:
            rr = dict(r)
            emb = rr["embedding"]
            # pgvector 텍스트 리터럴 '[...]' 로 전달 (psycopg2 어댑터 불요)
            rr["embedding"] = "[" + ",".join(repr(float(x)) for x in emb) + "]"
            rr["bbox"] = json.dumps(rr["bbox"]) if rr.get("bbox") is not None else None
            payload.append(rr)
        with self.connect() as conn, conn.cursor() as cur:
            cur.executemany(_INSERT_SQL, payload)
        return len(payload)

    def count_frame_backlog(self, model_name: str, image_roles: list[str] | None = None) -> int:
        params: dict[str, Any] = {"model_name": model_name}
        role_filter = ""
        if image_roles:
            role_filter = "AND im.image_role = ANY(%(roles)s)"
            params["roles"] = list(image_roles)
        sql = (
            "SELECT count(*) FROM image_metadata im WHERE NOT EXISTS ("
            "SELECT 1 FROM image_embeddings e WHERE e.entity_type='frame' "
            "AND e.entity_id=im.image_id AND e.model_name=%(model_name)s) " + role_filter
        )
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return int(cur.fetchone()[0])
```

> `vector` 컬럼 INSERT 는 `'[0.1,0.2,...]'` 텍스트 리터럴이 가장 안전 (psycopg2 vector 어댑터 불필요). cosine 쿼리도 동일 리터럴로 파라미터 바인딩.

- [ ] **Step 4: `PostgresResource` 상속에 추가**

[postgres.py:30](../../../src/vlm_pipeline/resources/postgres.py#L30) 클래스 정의의 mixin 목록에 `PostgresEmbeddingMixin` 추가 (import 포함):

```python
from vlm_pipeline.resources.postgres_embedding import PostgresEmbeddingMixin
# class PostgresResource(..., PostgresEmbeddingMixin, PostgresBaseMixin):  ← 기존 목록 끝, BaseMixin 앞에 삽입
```

- [ ] **Step 5: 테스트 PG MRO 에 추가**

[tests/conftest.py](../../../tests/conftest.py) 의 `_PostgresTestResource` mixin 목록(`PostgresProcessMixin` 등과 같은 줄)에 `PostgresEmbeddingMixin` 추가 + import.

- [ ] **Step 6: 테스트 실행 → 통과**

Run: `pytest tests/unit/test_postgres_embedding.py -v`
Expected: PASS (pgvector 가용 시) / SKIP

- [ ] **Step 7: Commit**

```bash
git add src/vlm_pipeline/resources/postgres_embedding.py src/vlm_pipeline/resources/postgres.py tests/conftest.py tests/unit/test_postgres_embedding.py
git commit -m "feat(embed): add PostgresEmbeddingMixin (pending frames + batch upsert)"
```

---

### Task 3: `EmbeddingClient` HTTP 라이브러리

**Files:**
- Create: `src/vlm_pipeline/lib/embedding.py`
- Test: `tests/unit/test_embedding_client.py`

미러: [lib/sam3.py:18-105](../../../src/vlm_pipeline/lib/sam3.py#L18).

- [ ] **Step 1: 실패 테스트 작성**

```python
# tests/unit/test_embedding_client.py
from unittest.mock import MagicMock, patch
from vlm_pipeline.lib.embedding import EmbeddingClient

def test_embed_posts_image_and_returns_vector():
    client = EmbeddingClient(base_url="http://embed:8003")
    fake = MagicMock()
    fake.json.return_value = {"vector": [0.5] * 1024, "dim": 1024, "model_name": "facebook/PE-Core-L14-336"}
    fake.raise_for_status.return_value = None
    with patch.object(client, "_session") as sess:
        sess.post.return_value = fake
        vec = client.embed(b"\xff\xd8jpgbytes")
    assert len(vec) == 1024
    args, kwargs = sess.post.call_args
    assert args[0].endswith("/embed")
    assert "files" in kwargs

def test_embed_text_posts_text():
    client = EmbeddingClient(base_url="http://embed:8003")
    fake = MagicMock()
    fake.json.return_value = {"vector": [0.1] * 1024, "dim": 1024}
    fake.raise_for_status.return_value = None
    with patch.object(client, "_session") as sess:
        sess.post.return_value = fake
        vec = client.embed_text("a fire scene")
    assert len(vec) == 1024
    args, kwargs = sess.post.call_args
    assert args[0].endswith("/embed_text")
```

- [ ] **Step 2: 실행 → 실패**

Run: `pytest tests/unit/test_embedding_client.py -v`
Expected: FAIL (module 없음)

- [ ] **Step 3: 구현**

```python
# src/vlm_pipeline/lib/embedding.py
"""PE-Core 임베딩 서비스 HTTP 클라이언트. lib/sam3.py 패턴 미러."""
from __future__ import annotations

import os
import time
from functools import lru_cache

import requests

DEFAULT_URL = "http://embedding-service:8003"


class EmbeddingClient:
    def __init__(self, base_url: str | None = None, timeout: float = 60.0) -> None:
        self.base_url = (base_url or os.environ.get("EMBEDDING_API_URL", DEFAULT_URL)).rstrip("/")
        self.timeout = timeout
        self.__session: requests.Session | None = None

    @property
    def _session(self) -> requests.Session:
        if self.__session is None:
            self.__session = requests.Session()
        return self.__session

    def wait_until_ready(self, max_wait_sec: float = 120.0) -> bool:
        deadline = time.monotonic() + max_wait_sec
        while time.monotonic() < deadline:
            try:
                r = self._session.get(f"{self.base_url}/health", timeout=5)
                if r.ok and r.json().get("model_loaded"):
                    return True
            except requests.RequestException:
                pass
            time.sleep(3)
        return False

    def embed(self, image_bytes: bytes) -> list[float]:
        r = self._session.post(
            f"{self.base_url}/embed",
            files={"file": ("image.jpg", image_bytes, "application/octet-stream")},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()["vector"]

    def embed_text(self, text: str) -> list[float]:
        r = self._session.post(
            f"{self.base_url}/embed_text", data={"text": text}, timeout=self.timeout
        )
        r.raise_for_status()
        return r.json()["vector"]


@lru_cache(maxsize=1)
def get_embedding_client() -> EmbeddingClient:
    return EmbeddingClient()
```

- [ ] **Step 4: 실행 → 통과**

Run: `pytest tests/unit/test_embedding_client.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/vlm_pipeline/lib/embedding.py tests/unit/test_embedding_client.py
git commit -m "feat(embed): add EmbeddingClient HTTP lib"
```

---

### Task 4: 임베딩 서비스 컨테이너

**Files:**
- Create: `docker/embedding/app.py`, `docker/embedding/backends/base.py`, `docker/embedding/backends/open_clip_be.py`, `docker/embedding/backends/__init__.py`, `docker/embedding/requirements.txt`, `docker/embedding/Dockerfile`
- Test: `tests/unit/test_embedding_service_contract.py` (mock 백엔드)

- [ ] **Step 1: 백엔드 ABC + open_clip 구현**

```python
# docker/embedding/backends/base.py
from __future__ import annotations
from abc import ABC, abstractmethod

class EmbeddingBackend(ABC):
    name: str
    dim: int
    @abstractmethod
    def load(self) -> None: ...
    @abstractmethod
    def embed_image(self, image_bytes: bytes) -> list[float]: ...
    @abstractmethod
    def embed_text(self, text: str) -> list[float]: ...
```

```python
# docker/embedding/backends/open_clip_be.py
from __future__ import annotations
import io
from .base import EmbeddingBackend

class OpenClipBackend(EmbeddingBackend):
    name = "facebook/PE-Core-L14-336"
    dim = 1024
    def __init__(self, device: str = "cuda:0") -> None:
        self.device = device
        self._model = None
        self._preprocess = None
        self._tokenizer = None
    def load(self) -> None:
        import open_clip, torch
        self._torch = torch
        model, _, preprocess = open_clip.create_model_and_transforms("hf-hub:timm/PE-Core-L-14-336")
        self._tokenizer = open_clip.get_tokenizer("hf-hub:timm/PE-Core-L-14-336")
        self._model = model.to(self.device).eval()
        self._preprocess = preprocess
    def embed_image(self, image_bytes: bytes) -> list[float]:
        from PIL import Image
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        x = self._preprocess(img).unsqueeze(0).to(self.device)
        with self._torch.no_grad():
            f = self._model.encode_image(x, normalize=True)
        return f[0].cpu().float().tolist()
    def embed_text(self, text: str) -> list[float]:
        toks = self._tokenizer([text]).to(self.device)
        with self._torch.no_grad():
            f = self._model.encode_text(toks, normalize=True)
        return f[0].cpu().float().tolist()
```

- [ ] **Step 2: FastAPI app**

```python
# docker/embedding/app.py
import os
from fastapi import FastAPI, File, Form, UploadFile

def _make_backend():
    name = os.environ.get("EMBEDDING_BACKEND", "open_clip")
    device = os.environ.get("EMBEDDING_DEVICE", "cuda:0")
    if name == "open_clip":
        from backends.open_clip_be import OpenClipBackend
        return OpenClipBackend(device=device)
    if name == "perception_models":
        raise NotImplementedError("perception_models backend: 추후 구현 (P? 별도)")
    raise ValueError(f"unknown EMBEDDING_BACKEND={name}")

app = FastAPI()
_backend = _make_backend()
_ready = {"v": False}

@app.on_event("startup")
def _startup():
    _backend.load()
    _ready["v"] = True

@app.get("/health")
def health():
    return {"model_loaded": _ready["v"], "model_name": _backend.name, "dim": _backend.dim}

@app.post("/embed")
async def embed(file: UploadFile = File(...)):
    data = await file.read()
    return {"vector": _backend.embed_image(data), "dim": _backend.dim, "model_name": _backend.name}

@app.post("/embed_text")
def embed_text(text: str = Form(...)):
    return {"vector": _backend.embed_text(text), "dim": _backend.dim, "model_name": _backend.name}
```

- [ ] **Step 3: requirements + Dockerfile**

```
# docker/embedding/requirements.txt
fastapi
uvicorn[standard]
open_clip_torch>=2.24
pillow
python-multipart
```

```dockerfile
# docker/embedding/Dockerfile
FROM pytorch/pytorch:2.4.0-cuda12.4-cudnn9-runtime
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
ENV EMBEDDING_BACKEND=open_clip EMBEDDING_DEVICE=cuda:0 HF_HOME=/data/models/pe-core
EXPOSE 8003
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8003"]
```

- [ ] **Step 4: 계약 테스트 (mock 백엔드 — 모델 다운로드 없이)**

```python
# tests/unit/test_embedding_service_contract.py
import sys, importlib, pathlib
import pytest
from fastapi.testclient import TestClient

@pytest.fixture
def client(monkeypatch):
    svc_dir = pathlib.Path("docker/embedding")
    sys.path.insert(0, str(svc_dir))
    monkeypatch.setenv("EMBEDDING_BACKEND", "open_clip")
    import backends.open_clip_be as be
    monkeypatch.setattr(be.OpenClipBackend, "load", lambda self: None)
    monkeypatch.setattr(be.OpenClipBackend, "embed_image", lambda self, b: [0.1] * 1024)
    monkeypatch.setattr(be.OpenClipBackend, "embed_text", lambda self, t: [0.2] * 1024)
    app_mod = importlib.import_module("app")
    with TestClient(app_mod.app) as c:
        yield c
    sys.path.remove(str(svc_dir))

def test_health_and_embed(client):
    h = client.get("/health").json()
    assert h["dim"] == 1024 and h["model_loaded"] is True
    v = client.post("/embed", files={"file": ("i.jpg", b"x", "application/octet-stream")}).json()
    assert len(v["vector"]) == 1024
    t = client.post("/embed_text", data={"text": "fire"}).json()
    assert len(t["vector"]) == 1024
```

- [ ] **Step 5: 실행 → 통과**

Run: `pip install fastapi 'uvicorn[standard]' python-multipart httpx; pytest tests/unit/test_embedding_service_contract.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add docker/embedding tests/unit/test_embedding_service_contract.py
git commit -m "feat(embed): add embedding-service container (open_clip PE-Core backend)"
```

---

### Task 5: compose `embedding` 서비스 등록

**Files:**
- Modify: `docker/docker-compose.yaml`

- [ ] **Step 1: 서비스 블록 추가** (sam3 블록을 미러, GPU 0)

```yaml
  embedding-service:
    build:
      context: ./embedding
    image: datapipeline-embedding:gpu-cu124
    environment:
      EMBEDDING_BACKEND: ${EMBEDDING_BACKEND:-open_clip}
      EMBEDDING_DEVICE: cuda:0
      NVIDIA_VISIBLE_DEVICES: "0"
      CUDA_VISIBLE_DEVICES: "0"
      HF_HOME: /data/models/pe-core
    volumes:
      - ./data/models:/data/models
    deploy:
      resources:
        reservations:
          devices: [{driver: nvidia, device_ids: ["0"], capabilities: [gpu]}]
    networks: [pipeline-network]
    restart: unless-stopped
```

- [ ] **Step 2: dagster 계열 서비스에 `EMBEDDING_API_URL` env 추가**

dagster / dagster-daemon / dagster-code-server 의 `environment:` 에:
```yaml
      EMBEDDING_API_URL: http://embedding-service:8003
```

- [ ] **Step 3: compose 문법 검증**

Run: `cd docker && docker compose -f docker-compose.yaml config >/dev/null && echo OK`
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add docker/docker-compose.yaml
git commit -m "feat(embed): add embedding-service to compose + EMBEDDING_API_URL"
```

---

## P1 — 프레임 임베딩 경로

### Task 6: `defs/embed/helpers.py` — 프레임 → row 빌드

**Files:**
- Create: `src/vlm_pipeline/defs/embed/__init__.py` (빈 파일), `src/vlm_pipeline/defs/embed/helpers.py`
- Test: `tests/unit/test_embed_helpers.py`

- [ ] **Step 1: 실패 테스트 작성** (mock_minio + mock client)

```python
# tests/unit/test_embed_helpers.py
from unittest.mock import MagicMock
from vlm_pipeline.defs.embed.helpers import build_frame_embedding_rows

MODEL = "facebook/PE-Core-L14-336"

def test_build_rows_downloads_embeds_and_shapes(mock_minio):
    mock_minio.upload_bytes("vlm-processed", "u/image/f1.jpg", b"jpgbytes")  # moto helper or put
    client = MagicMock()
    client.embed.return_value = [0.3] * 1024
    pending = [{"image_id": "f1", "image_bucket": "vlm-processed", "image_key": "u/image/f1.jpg", "image_role": "raw_video_frame"}]
    rows, failed = build_frame_embedding_rows(pending, minio=mock_minio, client=client, model_name=MODEL)
    assert failed == []
    assert rows[0]["entity_type"] == "frame"
    assert rows[0]["entity_id"] == "f1"
    assert rows[0]["dim"] == 1024 and len(rows[0]["embedding"]) == 1024
    assert rows[0]["embedding_id"] == f"frame|f1|{MODEL}"

def test_build_rows_fail_forward(mock_minio):
    client = MagicMock()
    client.embed.return_value = [0.3] * 1024
    pending = [{"image_id": "missing", "image_bucket": "vlm-processed", "image_key": "nope.jpg", "image_role": "raw_video_frame"}]
    rows, failed = build_frame_embedding_rows(pending, minio=mock_minio, client=client, model_name=MODEL)
    assert rows == [] and failed == ["missing"]
```

> `mock_minio` 의 객체 업로드 API 는 [resources/minio.py](../../../src/vlm_pipeline/resources/minio.py) 의 upload 메서드명을 확인해 맞춘다 (`upload_bytes`/`put_object`).

- [ ] **Step 2: 실행 → 실패**

Run: `pytest tests/unit/test_embed_helpers.py -v`
Expected: FAIL

- [ ] **Step 3: 구현**

```python
# src/vlm_pipeline/defs/embed/helpers.py
"""프레임 → 임베딩 row 빌드. per-file fail-forward."""
from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


def build_frame_embedding_rows(pending, *, minio, client, model_name: str) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    failed: list[str] = []
    for p in pending:
        image_id = p["image_id"]
        try:
            data = minio.download(p["image_bucket"], p["image_key"])
            vec = client.embed(data)
            if len(vec) != 1024:
                raise ValueError(f"unexpected dim {len(vec)}")
            rows.append({
                "embedding_id": f"frame|{image_id}|{model_name}",
                "entity_type": "frame", "entity_id": image_id, "image_id": image_id,
                "model_name": model_name, "dim": len(vec), "embedding": vec,
                "source_bucket": p["image_bucket"], "source_key": p["image_key"], "bbox": None,
            })
        except Exception as exc:  # fail-forward
            log.warning("frame embed failed image_id=%s: %s", image_id, exc)
            failed.append(image_id)
    return rows, failed
```

- [ ] **Step 4: 실행 → 통과** — `pytest tests/unit/test_embed_helpers.py -v` → PASS

- [ ] **Step 5: Commit**

```bash
git add src/vlm_pipeline/defs/embed/__init__.py src/vlm_pipeline/defs/embed/helpers.py tests/unit/test_embed_helpers.py
git commit -m "feat(embed): frame embedding row builder (fail-forward)"
```

---

### Task 7: `frame_embedding_asset`

**Files:**
- Create: `src/vlm_pipeline/defs/embed/assets.py`
- Test: `tests/unit/test_frame_embedding_asset.py`

미러: [sam/detection_assets.py:98-229](../../../src/vlm_pipeline/defs/sam/detection_assets.py#L98).

- [ ] **Step 1: 실패 테스트 작성** (Dagster `build_op_context` + mock resources)

```python
# tests/unit/test_frame_embedding_asset.py
from unittest.mock import MagicMock
from dagster import build_asset_context
from vlm_pipeline.defs.embed.assets import frame_embedding_asset

MODEL = "facebook/PE-Core-L14-336"

def test_asset_embeds_pending_and_inserts():
    db = MagicMock()
    db.find_pending_frame_embeddings.return_value = [
        {"image_id": "f1", "image_bucket": "vlm-processed", "image_key": "u/image/f1.jpg", "image_role": "raw_video_frame"}
    ]
    db.batch_insert_embeddings.return_value = 1
    minio = MagicMock(); minio.download.return_value = b"jpg"
    client = MagicMock(); client.embed.return_value = [0.1] * 1024; client.wait_until_ready.return_value = True
    ctx = build_asset_context(resources={"db": db, "minio": minio})
    out = frame_embedding_asset(ctx, db, minio, _client=client, _config={"limit": 10, "model_name": MODEL})
    assert out["embedded"] == 1
    db.batch_insert_embeddings.assert_called_once()
```

> 실제 asset 시그니처는 `config_schema` + resource 주입. 테스트 편의를 위해 `_client`/`_config` 주입 hook 을 두거나, `build_asset_context(... config=...)` 로 config 전달 + `get_embedding_client` patch. 구현 시 둘 중 한 패턴으로 통일.

- [ ] **Step 2: 실행 → 실패**

- [ ] **Step 3: 구현**

```python
# src/vlm_pipeline/defs/embed/assets.py
from __future__ import annotations

from dagster import asset, Config

from vlm_pipeline.lib.embedding import get_embedding_client
from vlm_pipeline.defs.embed.helpers import build_frame_embedding_rows

DEFAULT_MODEL = "facebook/PE-Core-L14-336"


class FrameEmbeddingConfig(Config):
    limit: int = 500
    model_name: str = DEFAULT_MODEL
    image_roles: list[str] = []  # 빈 리스트 = 전부


@asset(name="frame_embedding", group_name="embed")
def frame_embedding_asset(context, db, minio, config: FrameEmbeddingConfig) -> dict:
    client = get_embedding_client()
    if not client.wait_until_ready():
        context.log.error("embedding-service not ready")
        return {"embedded": 0, "failed": 0, "skipped": "service_not_ready"}
    roles = config.image_roles or None
    pending = db.find_pending_frame_embeddings(model_name=config.model_name, limit=config.limit, image_roles=roles)
    rows, failed = build_frame_embedding_rows(pending, minio=minio, client=client, model_name=config.model_name)
    inserted = db.batch_insert_embeddings(rows)
    context.log.info("frame_embedding: pending=%d inserted=%d failed=%d", len(pending), inserted, len(failed))
    return {"embedded": inserted, "failed": len(failed), "pending": len(pending)}
```

> 테스트는 `build_asset_context(resources=..., config=...)` 로 호출하고 `get_embedding_client` 를 monkeypatch. Step 1 테스트의 `_client/_config` hook 대신 이 패턴으로 맞춘다 (Dagster 표준).

- [ ] **Step 4: 실행 → 통과**

- [ ] **Step 5: Commit**

```bash
git add src/vlm_pipeline/defs/embed/assets.py tests/unit/test_frame_embedding_asset.py
git commit -m "feat(embed): frame_embedding asset (live + manual)"
```

---

### Task 8: 프레임 backlog sensor

**Files:**
- Create: `src/vlm_pipeline/defs/embed/sensor.py`
- Test: `tests/unit/test_embed_frame_sensor.py`

미러: [shared/detection_sensor_factory.py:24-129](../../../src/vlm_pipeline/defs/shared/detection_sensor_factory.py#L24) 의 backlog/커서 패턴.

- [ ] **Step 1: 실패 테스트 작성**

```python
# tests/unit/test_embed_frame_sensor.py
from unittest.mock import MagicMock
from vlm_pipeline.defs.embed.sensor import compute_frame_backlog_request

def test_request_when_backlog_grows():
    db = MagicMock(); db.count_frame_backlog.return_value = 5
    req, cursor = compute_frame_backlog_request(db, model_name="m", prev_cursor=None, limit=100)
    assert req is not None and cursor == "count=5"

def test_no_request_when_unchanged():
    db = MagicMock(); db.count_frame_backlog.return_value = 5
    req, cursor = compute_frame_backlog_request(db, model_name="m", prev_cursor="count=5", limit=100)
    assert req is None

def test_no_request_when_zero():
    db = MagicMock(); db.count_frame_backlog.return_value = 0
    req, cursor = compute_frame_backlog_request(db, model_name="m", prev_cursor=None, limit=100)
    assert req is None
```

- [ ] **Step 2: 실행 → 실패**

- [ ] **Step 3: 구현** (순수 함수 + sensor 래퍼)

```python
# src/vlm_pipeline/defs/embed/sensor.py
from __future__ import annotations

from dagster import RunRequest, SensorResult, sensor, DefaultSensorStatus

from vlm_pipeline.defs.embed.assets import DEFAULT_MODEL

_SENSOR_LIMIT = 200


def compute_frame_backlog_request(db, *, model_name: str, prev_cursor: str | None, limit: int):
    count = db.count_frame_backlog(model_name=model_name)
    token = f"count={count}"
    if count <= 0 or token == prev_cursor:
        return None, prev_cursor or token
    cfg = {"ops": {"frame_embedding": {"config": {"limit": limit, "model_name": model_name}}}}
    return RunRequest(run_key=token, run_config=cfg), token


@sensor(target="frame_embedding", default_status=DefaultSensorStatus.STOPPED, minimum_interval_seconds=120)
def frame_embedding_backlog_sensor(context):
    from vlm_pipeline.resources.postgres import PostgresResource  # lazy (L4→L3 허용 아님: context.resources 사용)
    db = context.resources.db
    req, cursor = compute_frame_backlog_request(
        db, model_name=DEFAULT_MODEL, prev_cursor=context.cursor, limit=_SENSOR_LIMIT
    )
    return SensorResult(run_requests=[req] if req else [], cursor=cursor)
```

> sensor 의 `target=`/job 연결과 `required_resource_keys`/`RunRequest` 형식은 [defs/sam/](../../../src/vlm_pipeline/defs/sam/) 등록부와 동일하게 맞춘다. 위 import 주석은 제거하고 `context.resources.db` 사용.

- [ ] **Step 4: 실행 → 통과** (순수 함수 단위테스트만; sensor 통합은 definitions 등록 후 staging 에서)

- [ ] **Step 5: Commit**

```bash
git add src/vlm_pipeline/defs/embed/sensor.py tests/unit/test_embed_frame_sensor.py
git commit -m "feat(embed): frame backlog sensor (cursor-based)"
```

---

### Task 9: definitions 등록 + 수동 job

**Files:**
- Modify: `src/vlm_pipeline/definitions_production.py` (+ staging 프로파일이 별도면 해당 파일)
- Test: `tests/unit/test_definitions_profiles_catalog.py` (기존 카탈로그 테스트에 embed asset/sensor 등장 확인 추가)

- [ ] **Step 1: 등록**
  - `frame_embedding` asset 을 Definitions 의 assets 에 추가
  - `frame_embedding_backlog_sensor` 를 sensors 에 추가
  - `db`(PostgresResource), `minio`(MinIOResource) resource 키 연결 (기존과 동일)
  - 수동 실행용 job: `define_asset_job("frame_embedding_job", selection=["frame_embedding"])`

- [ ] **Step 2: import-time 로딩 검증**

Run: `python -c "import vlm_pipeline.definitions_production as d; print('frame_embedding' in [a.key.to_user_string() for a in d.defs.get_asset_graph().all_asset_keys] if hasattr(d,'defs') else 'check')"`
Expected: 로딩 에러 없음 + asset 등장

- [ ] **Step 3: lib layer lint + ruff**

Run: `python scripts/check_lib_layer_imports.py && ruff check src/vlm_pipeline/defs/embed src/vlm_pipeline/lib/embedding.py src/vlm_pipeline/resources/postgres_embedding.py`
Expected: PASS (lib/embedding.py 는 dagster/defs/resources import 없음 — requests 만)

- [ ] **Step 4: 전체 단위테스트**

Run: `pytest tests/unit -q -k "embed or embedding or pgvector"`
Expected: PASS/SKIP, no FAIL

- [ ] **Step 5: Commit**

```bash
git add src/vlm_pipeline/definitions_production.py tests/unit/test_definitions_profiles_catalog.py
git commit -m "feat(embed): register frame_embedding asset/sensor/job in definitions"
```

---

### Task 10: staging E2E 검증 (수동 ops 체크리스트)

> 코드 아님 — staging 배포 후 사람이 확인. dev 브랜치 머지 → staging 자동배포 후.

- [ ] **Step 1: 이미지 빌드 (staging)**

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline_test
./scripts/compose-staging.sh build embedding-service
```

- [ ] **Step 2: 서비스 기동 + health**

```bash
./scripts/compose-staging.sh up -d embedding-service
docker exec pipeline-test-dagster-1 sh -lc 'curl -s http://embedding-service:8003/health'
# expect {"model_loaded": true, "model_name": "facebook/PE-Core-L14-336", "dim": 1024}
```

- [ ] **Step 3: 수동 backfill (2,416 프레임)** — Dagster UI(:3031)에서 `frame_embedding_job` 실행 (config limit 크게) 또는 반복 tick.

- [ ] **Step 4: 적재 확인**

```bash
docker exec -i pipeline-test-postgres-1 psql -U airflow -d vlm_pipeline_staging -c \
"SELECT count(*), count(DISTINCT image_id) FROM image_embeddings WHERE entity_type='frame';"
```

- [ ] **Step 5: 유사도 + 텍스트 검색 smoke**

```sql
-- 임의 프레임의 top-5 유사 프레임
SELECT entity_id, embedding <=> (SELECT embedding FROM image_embeddings LIMIT 1) AS d
FROM image_embeddings ORDER BY d LIMIT 5;
```
텍스트 검색은 `embed_text("...")` 결과 벡터로 동일 쿼리(노트북/임시 스크립트).

- [ ] **Step 6: 결과 기록** — 적재 수/검색 동작/소요시간을 PR 코멘트에 남김.

---

## Self-Review (작성자 체크)

- **Spec coverage**: 모델 PE-Core(Task4)·1024 스키마(Task1)·frame 전부+role config(Task2,6,7)·live sensor(Task8)+manual job(Task9)·open_clip+백엔드추상화(Task4)·GPU0(Task5)·fail-forward(Task6)·staging-first(Task10) 모두 task 존재. 검출/검색/prod 는 명시적으로 별도 plan(범위 밖).
- **Placeholder scan**: 모든 코드 step 실제 코드 포함. Task7 의 테스트/asset hook 패턴 불일치 가능성 → "build_asset_context+monkeypatch 로 통일" 명시. Task8 sensor 의 import 주석 → "제거하고 context.resources.db 사용" 명시.
- **Type consistency**: row dict 키(`embedding_id/entity_type/entity_id/image_id/model_name/dim/embedding/source_bucket/source_key/bbox`)가 Task2 INSERT·Task6 빌더·Task1 스키마 전반 일치. `find_pending_frame_embeddings(model_name, limit, image_roles)` / `count_frame_backlog(model_name, image_roles)` / `batch_insert_embeddings(rows)` 시그니처가 resource·asset·sensor 에서 동일.
- **알려진 확인필요(실행 중 검증)**: ① `minio` 업로드/다운로드 메서드명(`download` 확인됨, 업로드명은 minio.py 확인), ② `postgres.py`/`conftest.py` mixin MRO 정확한 삽입 위치, ③ definitions 등록부의 resource 키명(`db`/`minio`), ④ Dockerfile base 의 cu124/torch 호환.
