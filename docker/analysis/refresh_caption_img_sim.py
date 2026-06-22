"""captions 데이터셋에 caption_img_sim 필드 in-place 추가/갱신.

caption↔자기영상 best-frame cosine 유사도 (caption+frame 둘 다 임베딩된 캡션만, 나머지 None).
임베딩/UMAP/미디어/클러스터는 불변 → FiftyOne 앱 무중단(set_values 벌크 갱신). **소스(image_embeddings)는 read-only**.

캡션이 영어로 재임베딩되거나 frame 임베딩 커버리지가 늘면 다시 실행해 값 갱신:
    docker exec docker-analysis-1 python /workspace/refresh_caption_img_sim.py
"""

import fiftyone as fo

import fiftyone_pgvector as fp

ds = fo.load_dataset("captions")
sim_map = fp.fetch_caption_image_sim()  # entity_id -> best-frame cosine
ids = ds.values("entity_id")
vals = [float(sim_map[str(e)]) if (e is not None and str(e) in sim_map) else None for e in ids]
ds.set_values("caption_img_sim", vals)
n = sum(1 for v in vals if v is not None)
print(f"caption_img_sim set on {ds.count()} samples; {n} with value (rest None)", flush=True)
