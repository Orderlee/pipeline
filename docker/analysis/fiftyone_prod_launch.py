"""prod FiftyOne 기동 — frame 임베딩으로 레거시 프레임 데이터셋 빌드 후 app 띄우고 keep-alive.

analysis 컨테이너 안에서 detached 실행. 미디어는 MinIO 에서 media_dir 로 내려받아 로컬 경로 사용.

⚠️ **2026-08-19 로 superseded.** 이 스크립트는 구 'frames' 데이터셋(프레임만, 5,000장 기본)을
빌드하던 것인데, 같은 날 FiftyOne 데이터셋 개명으로 **'frames' 가 정본 이름**이 됐다
(구 frames_captions — 프레임 187,994 + 캡션 11,978 = 199,972, 뱅크평가·프롬프트 짝
'frames-prompts'·2h cron 이 전부 이 데이터셋에 걸려 있다). `build_fiftyone_dataset(...,
overwrite=True)` 는 동명 데이터셋을 지우고 다시 만들기 때문에, 이 파일이 옛날처럼 실행되면
정본을 통째로 날린다. 그래서 아래에서 즉시 중단한다.

앱만 다시 띄우려면 `fiftyone_relaunch.py` (빌드 없음, `FO_DATASET` 기본 'frames').
정본 재빌드는 `fiftyone_full_build.py` → `merge_frames_captions.py` → `enrich_frames_captions.py`.
"""

import os
import time

import fiftyone as fo

import fiftyone_pgvector as fp

# ── superseded 가드 (가장 먼저 — DB/MinIO 를 건드리기 전에 끊는다) ──────────────
# 탈출구를 남기되, 통과해도 **정본 이름으로는 절대 쓰지 않는다**: 대상이 'frames_legacy' 로
# 강제된다. env 만 켜면 옛 동작이 그대로 돌아오는 형태였으면 가드의 의미가 없다.
LEGACY_DATASET = "frames_legacy"
if os.getenv("FIFTYONE_LEGACY_BUILD", "").strip() not in ("1", "true", "yes"):
    raise SystemExit(
        "superseded: 'frames' 는 이제 정본 데이터셋(구 frames_captions). "
        "이 빌더가 다시 필요하면 FIFTYONE_LEGACY_BUILD=1 로 강제 + 대상 이름을 바꿔라 "
        f"(강제 시 자동으로 '{LEGACY_DATASET}' 에 빌드된다)"
    )

# 데이터셋에 적재할 프레임 수 조절 — FIFTYONE_FRAMES_LIMIT (정수 / 0 / all / none).
#   미설정·0·all·none → 전체(LIMIT 없음). 기본 5000. 큰 값일수록 미디어 다운로드(~127KB/장)+UMAP 부하 증가.
_lim = os.getenv("FIFTYONE_FRAMES_LIMIT", "5000").strip().lower()
limit = None if _lim in ("0", "all", "none", "") else int(_lim)
print(f"loading frame embeddings (limit={'ALL' if limit is None else limit})...", flush=True)
rows = fp.load_frame_embeddings(limit=limit)
print(f"rows={len(rows)}", flush=True)
fp.build_fiftyone_dataset(LEGACY_DATASET, rows, umap=True, labels=True, caption_clusters=True)
ds = fo.load_dataset(LEGACY_DATASET)
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
