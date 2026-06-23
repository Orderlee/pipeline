"""prod FiftyOne 기동 — frame 임베딩으로 'frames' 데이터셋 빌드 후 app 띄우고 keep-alive.

analysis 컨테이너 안에서 detached 실행. 미디어는 MinIO 에서 media_dir 로 내려받아 로컬 경로 사용.
"""

import os
import time

import fiftyone as fo

import fiftyone_pgvector as fp

# 'frames' 데이터셋에 적재할 프레임 수 조절 — FIFTYONE_FRAMES_LIMIT (정수 / 0 / all / none).
#   미설정·0·all·none → 전체(LIMIT 없음). 기본 5000. 큰 값일수록 미디어 다운로드(~127KB/장)+UMAP 부하 증가.
_lim = os.getenv("FIFTYONE_FRAMES_LIMIT", "5000").strip().lower()
limit = None if _lim in ("0", "all", "none", "") else int(_lim)
print(f"loading frame embeddings (limit={'ALL' if limit is None else limit})...", flush=True)
rows = fp.load_frame_embeddings(limit=limit)
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
