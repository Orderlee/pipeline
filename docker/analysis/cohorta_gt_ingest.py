#!/usr/bin/env python3
"""cohort-a processed_data 의 **사람 bbox GT** → al_frames. dagster 컨테이너 실행.

왜 중요한가: `processed_data/<class>/<video>/bboxes/*_boundingbox.json` 에 COCO 형식
사람 어노테이션(`person` / `falldown_person`)이 12클래스 2,842영상분 있는데
**DB 에 하나도 없다**(파이프라인 전체 `image_labels.review_status='finalized'` 288건).
쓰러짐만 3,000 박스 — 현재 falldown 사람확정 625장의 몇 배다.

프레임 라벨 규칙: `falldown_person` 박스가 하나라도 있으면 `falldown`, 사람만 있으면
`normal`(넘어지지 않은 사람), 박스 0개면 `normal`. **사람이 그린 박스에서 직접 유도**하므로
label_source='human' 이다(자기학습 금지 불변식 통과).

⚠️ 이 프레임들은 파이프라인이 뽑은 프레임(`<stem>_00000001.jpg`, 영상당 10장)과 **다른 집합**이다
(`<video>_frameNN.jpg`, 영상당 ~30장). 매핑하지 않고 별도 코호트로 둔다 — 억지로 시각을
맞추면 조용한 오배정이 난다.
⚠️ JPEG 은 nas_secondary ro 마운트에 있다. MinIO 업로드는 하지 않는다 — 학습·채점용이지
라벨러가 열 대상이 아니다.
"""
import glob
import json
import os
import sys
import time

sys.path.insert(0, "/src/vlm")
import numpy as np  # noqa: E402
import psycopg2  # noqa: E402
from psycopg2.extras import execute_values  # noqa: E402

from vlm_pipeline.lib.embedding import EmbeddingClient  # noqa: E402

PG = dict(host="docker-postgres-1", port=5432, user="airflow",
          password=os.environ.get("POSTGRES_PASSWORD", "airflow"), dbname="vlm_pipeline")
ROOT = "/nas/datasets/projects/cohort-a/processed_data"
FOLDERS = os.environ.get("VN_GT_FOLDERS", "outdoor_fall_2,escalator_fall").split(",")
COHORT = "cohorta_falldown_gt"
MODEL = "facebook/PE-Core-L14-336"
COLS = ["cohort", "frame_key", "media_uri", "site", "cls", "label_rule", "label_source",
        "group_key", "camera", "session", "video_stem", "t_sec", "ambiguous", "boundary",
        "n_passes", "asset_id", "extra"]


def collect():
    out = []
    for fold in FOLDERS:
        for p in sorted(glob.glob(f"{ROOT}/{fold}/*/bboxes/*.json")):
            vid = os.path.basename(os.path.dirname(os.path.dirname(p)))
            try:
                d = json.load(open(p, encoding="utf-8"))
            except Exception:
                continue
            cats = {c["id"]: c["name"] for c in d.get("categories", [])}
            by_img = {}
            for a in d.get("annotations", []):
                by_img.setdefault(a["image_id"], []).append(cats.get(a["category_id"], "?"))
            for im in d.get("images", []):
                names = by_img.get(im["id"], [])
                fp = f"{ROOT}/{fold}/{vid}/{im['file_name']}"
                if not os.path.isfile(fp):
                    continue
                cls = "falldown" if "falldown_person" in names else "normal"
                out.append(dict(
                    key=f"{fold}/{vid}/{im['file_name']}", path=fp, cls=cls, folder=fold,
                    video=vid, n_fall=names.count("falldown_person"), n_person=names.count("person"),
                    # 파일명 <date>_<AM|PM>_<..>_<..> 에서 날짜를 세션으로
                    session=vid.split("_")[0] if "_" in vid else vid))
    return out


def main():
    rows = collect()
    print(f"GT 프레임 {len(rows):,} · 영상 {len({r['video'] for r in rows})}", flush=True)
    from collections import Counter
    print("  클래스:", dict(Counter(r["cls"] for r in rows)),
          "· 폴더:", dict(Counter(r["folder"] for r in rows)), flush=True)

    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute("SELECT entity_id FROM image_embeddings WHERE entity_type='al_frame' AND entity_id LIKE %s",
                (f"{COHORT}/%",))
    have = {r[0].split("/", 1)[1] for r in cur.fetchall()}
    todo = [r for r in rows if r["key"] not in have]
    print(f"  기존 {len(have):,} · 신규 {len(todo):,}", flush=True)

    client = EmbeddingClient(); client.wait_until_ready(600)
    meta, vecs, fail, t0 = [], [], 0, time.time()
    for n, r in enumerate(todo, 1):
        try:
            with open(r["path"], "rb") as fh:
                v = np.asarray(client.embed(fh.read()), dtype=np.float32)
        except Exception:
            fail += 1; continue
        meta.append((COHORT, r["key"], r["path"], "cohorta", r["cls"],
                     "coco_bbox_person_falldown", "human",
                     r["video"],                      # group_key = 영상
                     None, r["session"], r["video"], None, False, False, None, None,
                     json.dumps({"folder": r["folder"], "n_falldown_box": r["n_fall"],
                                 "n_person_box": r["n_person"]}, ensure_ascii=False)))
        vecs.append((f"al_frame:{COHORT}/{r['key']}", "al_frame", f"{COHORT}/{r['key']}",
                     MODEL, len(v), "[" + ",".join(f"{x:.6f}" for x in v) + "]"))
        if n % 500 == 0:
            el = time.time() - t0
            print(f"  {n}/{len(todo)} · {el:.0f}s · {el/n:.2f}s/장 · 실패 {fail}", flush=True)

    if meta:
        execute_values(cur, f"INSERT INTO al_frames ({','.join(COLS)}) VALUES %s "
                            "ON CONFLICT (cohort, frame_key) DO UPDATE SET "
                            + ",".join(f"{c}=EXCLUDED.{c}" for c in COLS if c not in ("cohort", "frame_key")),
                       meta, page_size=500)
        execute_values(cur, "INSERT INTO image_embeddings (embedding_id, entity_type, entity_id, model_name, dim, embedding) "
                            "VALUES %s ON CONFLICT (entity_type, entity_id, model_name) DO UPDATE SET embedding=EXCLUDED.embedding",
                       vecs, template="(%s,%s,%s,%s,%s,%s::vector)", page_size=200)
        conn.commit()
    print(f"\n적재 {len(meta):,} · 실패 {fail}")
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
