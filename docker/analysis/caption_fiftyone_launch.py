"""FiftyOne 'captions' 데이터셋 빌드 — 캡션 임베딩 UMAP 탐색용.

각 캡션 = 대표 키프레임(영상) + 캡션 텍스트 + 캡션 임베딩 + 클러스터(capNN), UMAP(emb_viz).
별도 앱 불필요 — 기존 FiftyOne 앱(:5153)의 데이터셋 셀렉터에서 'captions' 로 전환해 본다.
컨테이너에서 실행: python caption_fiftyone_launch.py
"""

import os

import fiftyone as fo

import fiftyone_pgvector as fp

limit_env = os.environ.get("CAPTION_VIZ_LIMIT", "").strip()
limit = int(limit_env) if limit_env else None
print(f"loading caption embeddings (limit={limit})...", flush=True)
rows = fp.load_caption_embeddings(limit=limit)
print(f"caption rows={len(rows)}", flush=True)
fp.build_caption_fiftyone_dataset("captions", rows, umap=True)
ds = fo.load_dataset("captions")
print(f"BUILT captions={ds.count()} brain={ds.list_brain_runs()}", flush=True)
