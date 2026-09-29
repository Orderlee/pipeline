#!/usr/bin/env python3
"""AL 코호트 npz → Postgres `al_frames` + `image_embeddings`(entity_type='al_frame').

흩어진 산출물(npz·FiftyOne·NAS 폴더)을 파이프라인 DB 한 곳으로 모은다.
임베딩이 pgvector 에 들어가면 `fiftyone_sync_sensor` 가 스냅샷 diff 로 변화를 보게 되므로
FiftyOne 반영도 그때부터 자동 경로를 탄다(UMAP refit 은 여전히 수동 — RAM 때문).

⚠️ `label_source` 를 코호트마다 명시한다 — 자기학습 금지 불변식의 판정 근거다.
   sitej_certbody      = derived (사람 GT 구간을 기계 규칙으로 프레임에 투영)
   sourcea_thumb = model   (현장 탐지기 알람 카테고리. 학습 금지)
   sourcei        = 행별로 ledger 의 gt_source 를 따른다(caption 파생 69% 가 섞여 있다)

⚠️ `group_key` 도 코호트마다 다르다 — certbody 는 session(연출 동시녹화), 나머지는 camera.
"""
import json
import os
import sys
import unicodedata as u

import numpy as np
import psycopg2
from psycopg2.extras import execute_values

PG = dict(host="docker-postgres-1", port=5432, user="airflow", password=os.environ.get("POSTGRES_PASSWORD", "airflow"), dbname="vlm_pipeline")
MODEL = "facebook/PE-Core-L14-336"
R = "/data/fiftyone/frames_bank/report"
N = lambda s: u.normalize("NFC", str(s))  # noqa: E731


def rows_sitej_certbody():
    d = np.load(f"{R}/sourcea/sitej_certbody.npz", allow_pickle=True)
    man = {N(json.loads(l)["key"]): json.loads(l)
           for l in open("/data/fiftyone/uploads/sitej_certbody/manifest.jsonl", encoding="utf-8")}
    for i, v in zip(d["ids"], d["vectors"]):
        k = N(i); m = man.get(k, {})
        yield dict(cohort="sitej_certbody", frame_key=k,
                   media_uri=f"/data/fiftyone/uploads/sitej_certbody/images/{k}",
                   site="sitej_subway", cls=m.get("cls"), label_rule="interval_projection_v1",
                   label_source="derived", group_key=m.get("session"),
                   camera=N(m.get("camera", "")), session=m.get("session"),
                   video_stem=N(m.get("video_stem", "")), t_sec=m.get("t_sec"),
                   ambiguous=bool(m.get("ambiguous")), boundary=bool(m.get("boundary")),
                   n_passes=m.get("n_passes"), asset_id=None,
                   extra=json.dumps({"classes": m.get("classes", [])}, ensure_ascii=False)), v


def rows_sourcea():
    d = np.load(f"{R}/sourcea/thumb_embeddings.npz", allow_pickle=True)
    for i, v, c, cam, dt in zip(d["ids"], d["vectors"], d["cls"], d["camera"], d["date"]):
        k = N(i)
        yield dict(cohort="sourcea_thumb", frame_key=k,
                   media_uri=f"/nas/data/sourcea/by_category/{k}",
                   site="sourcea", cls=str(c), label_rule="alarm_category",
                   label_source="model", group_key=str(cam), camera=str(cam),
                   session=str(dt), video_stem=None, t_sec=None,
                   ambiguous=False, boundary=False, n_passes=None, asset_id=None,
                   extra=json.dumps({"date": str(dt)})), v


def rows_sourcei():
    import fiftyone as fo
    ds = fo.load_dataset("sourcei")
    ids, fps, gt, cam, emb = ds.values(["id", "filepath", "ground_truth.label", "camera", "embedding"])
    src = {}
    for line in open("/data/fiftyone/sourcei/work/ledger.jsonl", encoding="utf-8"):
        r = json.loads(line); src[N(os.path.basename(r.get("key", "")))] = r.get("gt_source")
    for i, f, g, c, e in zip(ids, fps, gt, cam, emb):
        if e is None or len(e) == 0:
            continue
        base = N(os.path.basename(f))
        gs = src.get(base)
        yield dict(cohort="sourcei", frame_key=base, media_uri=f, site="sourcei_site-g",
                   cls=g, label_rule=f"ledger:{gs or 'unknown'}",
                   # caption 파생은 Gemini 출력에서 유도한 라벨이라 model 이다
                   label_source={"caption": "model", "filename": "human", "folder": "human"}.get(gs, "unknown"),
                   group_key=c, camera=c, session=None, video_stem=None, t_sec=None,
                   ambiguous=False, boundary=False, n_passes=None, asset_id=None,
                   extra=json.dumps({"gt_source": gs})), np.asarray(e, dtype=np.float32)


COHORTS = {"sitej_certbody": rows_sitej_certbody, "sourcea_thumb": rows_sourcea, "sourcei": rows_sourcei}
COLS = ["cohort", "frame_key", "media_uri", "site", "cls", "label_rule", "label_source",
        "group_key", "camera", "session", "video_stem", "t_sec", "ambiguous", "boundary",
        "n_passes", "asset_id", "extra"]


def main():
    want = sys.argv[1:] or list(COHORTS)
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    for name in want:
        meta, vecs = [], []
        for m, v in COHORTS[name]():
            meta.append(tuple(m[c] for c in COLS))
            vecs.append((f"al_frame:{name}/{m['frame_key']}", "al_frame",
                         f"{name}/{m['frame_key']}", MODEL, len(v),
                         "[" + ",".join(f"{x:.6f}" for x in np.asarray(v, dtype=np.float32)) + "]"))
        execute_values(cur, f"INSERT INTO al_frames ({','.join(COLS)}) VALUES %s "
                            "ON CONFLICT (cohort, frame_key) DO UPDATE SET "
                            + ",".join(f"{c}=EXCLUDED.{c}" for c in COLS if c not in ("cohort", "frame_key")),
                       meta, page_size=500)
        execute_values(cur, "INSERT INTO image_embeddings "
                            "(embedding_id, entity_type, entity_id, model_name, dim, embedding) VALUES %s "
                            "ON CONFLICT (entity_type, entity_id, model_name) DO UPDATE SET embedding=EXCLUDED.embedding",
                       vecs, template="(%s,%s,%s,%s,%s,%s::vector)", page_size=200)
        conn.commit()
        print(f"{name}: al_frames {len(meta):,} · embeddings {len(vecs):,}", flush=True)
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
