"""prod FiftyOne 기동 — frame 임베딩으로 'frames' 데이터셋 빌드 후 app 띄우고 keep-alive.

analysis 컨테이너 안에서 detached 실행. 미디어는 MinIO 에서 media_dir 로 내려받아 로컬 경로 사용.
"""

import time

import fiftyone as fo

import fiftyone_pgvector as fp

print("loading frame embeddings...", flush=True)
rows = fp.load_frame_embeddings(limit=5000)
print(f"rows={len(rows)}", flush=True)
fp.build_fiftyone_dataset("frames", rows, umap=True, labels=True, caption_clusters=True)
ds = fo.load_dataset("frames")
# App 텍스트→이미지 검색(prompt similarity index). 이미지 임베딩은 precomputed → 모델 미호출,
# 텍스트 쿼리만 embedding-service /embed_text 로 임베딩. fail-forward(검색은 optional).
try:
    fp.build_text_search_index(ds, brain_key="text_search")
    print("text_search index built (App 텍스트→이미지 검색 활성)", flush=True)
except Exception as exc:  # noqa: BLE001 — 검색 인덱스 optional
    print(f"text_search index skipped: {exc}", flush=True)
print("dataset built; launching app on :5151 (0.0.0.0)", flush=True)
fo.launch_app(ds, address="0.0.0.0", port=5151)
print("APP_LAUNCHED", flush=True)
time.sleep(10**9)
