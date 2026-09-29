#!/usr/bin/env python3
"""fire/smoke 외부 GT 이벤트 JSON → al_frames. **dagster 컨테이너에서 실행.**

sourcep 제외(사용자 지시 + 자기 라벨이 자기를 못 맞히는 코호트라 이미 노이즈로 판정됨).

⚠️ **소스마다 timestamp 단위가 다르다. 섞으면 조용히 틀린다.**
  fire_smoke        `[{category, duration, timestamp:[a,b], en/ko_caption}]`  → **초**
                    (정수 초만 관측: 1,096개 값 전부 소수 0자리)
  loc-c_raw/fire `{video_info:{fps,...}, clips:{video_clipN:{category,duration,timestamp:[a,b]}}}`
                    → **프레임**. 실측 근거: [875,920] & duration 1.55 & fps 29 → (920-875)/29 = 1.55 ✓

⚠️ 한글 파일명은 인제스트 때 로마자화된다(`0821실내_2_smoke` → `0821silnae_2_smoke`, 320 중 16건).
   매칭은 반드시 **파이프라인 이미지 안의 살균 함수**로 한다.

라벨 투영은 certbody sitej 와 같은 규칙: 프레임 시각 t 를 덮는 카테고리가 0개면 normal,
1개면 그 클래스, 2개 이상이면 ambiguous(단일라벨 채점에서 제외). 경계 ±0.25s 는 boundary.

label_source='human': 이 JSON 들은 **파이프라인이 만든 게 아니다**(해당 asset 의 `labels` 행 0).
외부에서 납품된 어노테이션이다. 우리 모델 산출물이 아니므로 자기학습 금지에 걸리지 않는다.
"""
import glob
import json
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, "/src/vlm")
import psycopg2  # noqa: E402
from psycopg2.extras import execute_values  # noqa: E402

from vlm_pipeline.lib.sanitizer import sanitize_filename, sanitize_path_component  # noqa: E402

PG = dict(host="docker-postgres-1", port=5432, user="airflow",
          password=os.environ.get("POSTGRES_PASSWORD", "airflow"), dbname="vlm_pipeline")
MARGIN = 0.25
PROJ = "/nas/datasets/projects"

SOURCES = [
    # (cohort, site, source_unit_name, json_glob, 단위)
    ("fire_smoke_gt", "fire_smoke", "fire_smoke", f"{PROJ}/fire_smoke/*.json", "sec"),
    ("loc-c_fire_gt", "loc-c", "loc-c_raw",
     f"{PROJ}/loc-c_raw/organized_videos/fire/*.json", "frame"),
]
COLS = ["cohort", "frame_key", "media_uri", "site", "cls", "label_rule", "label_source",
        "group_key", "camera", "session", "video_stem", "t_sec", "ambiguous", "boundary",
        "n_passes", "asset_id", "extra"]


def parse_events(path, unit):
    """→ [(category, start_sec, end_sec)] · 단위 변환은 여기 한 곳에서만."""
    d = json.load(open(path, encoding="utf-8"))
    out = []
    if isinstance(d, list):                                   # fire_smoke 형식
        for e in d:
            ts = e.get("timestamp") or []
            if len(ts) == 2 and e.get("category"):
                out.append((e["category"], float(ts[0]), float(ts[1])))
    elif isinstance(d, dict):                                 # loc-c/songpa 형식
        fps = float((d.get("video_info") or {}).get("fps") or 0) or None
        for _, c in (d.get("clips") or {}).items():
            ts = c.get("timestamp") or []
            if len(ts) != 2 or not c.get("category"):
                continue
            a, b = float(ts[0]), float(ts[1])
            if unit == "frame":
                if not fps:
                    continue                                   # fps 없으면 변환 불가 — 버린다
                a, b = a / fps, b / fps
            out.append((c["category"], a, b))
    return out


def label_at(t, events):
    cover = {c for c, a, b in events if a <= t <= b}
    bnd = any(abs(t - a) <= MARGIN or abs(t - b) <= MARGIN for _, a, b in events)
    return cover, bnd


def main():
    assert sanitize_path_component("맞은편") == "majeunpyeon", "살균 폴백 환경 — 중단"
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    grand = Counter()

    for cohort, site, unit_name, pattern, unit in SOURCES:
        js = sorted(glob.glob(pattern))
        # 살균 basename → asset
        cur.execute("""SELECT r.asset_id, r.raw_key FROM raw_files r WHERE r.source_unit_name=%s""", (unit_name,))
        by_stem = {os.path.splitext(os.path.basename(rk))[0]: aid for aid, rk in cur.fetchall()}
        # 프레임: asset → [(image_id, key, sec)]
        cur.execute("""SELECT im.source_asset_id, im.image_id, im.image_bucket, im.image_key, im.frame_sec
                       FROM image_metadata im JOIN raw_files r ON r.asset_id=im.source_asset_id
                       JOIN image_embeddings e ON e.entity_type='frame' AND e.entity_id=im.image_id
                       WHERE r.source_unit_name=%s AND im.image_role='raw_video_frame'""", (unit_name,))
        frames = defaultdict(list)
        for aid, iid, bkt, key, sec in cur.fetchall():
            frames[aid].append((iid, bkt, key, sec))

        meta, stats = [], Counter()
        for p in js:
            stem = os.path.splitext(os.path.basename(p))[0]
            aid = by_stem.get(stem) or by_stem.get(os.path.splitext(sanitize_filename(stem + ".mp4"))[0])
            if not aid:
                stats["asset미해소"] += 1; continue
            ev = parse_events(p, unit)
            if not ev:
                stats["이벤트0"] += 1; continue
            fr = frames.get(aid, [])
            if not fr:
                stats["프레임없음"] += 1; continue
            for iid, bkt, key, sec in fr:
                if sec is None:
                    continue
                cover, bnd = label_at(float(sec), ev)
                amb = len(cover) > 1
                cls = "normal" if not cover else sorted(cover)[0]
                meta.append((cohort, iid, f"minio://{bkt}/{key}", site, cls,
                             f"external_event_json:{unit_name}", "human",
                             aid, None, None, stem, float(sec), amb, bnd, None, aid,
                             json.dumps({"covering": sorted(cover), "ts_unit": unit,
                                         "n_events": len(ev)}, ensure_ascii=False)))
                stats[cls] += 1; stats["__amb"] += amb
        print(f"[{cohort}] JSON {len(js)} · 프레임 {len(meta):,} · {dict(stats)}", flush=True)
        if not meta:
            continue
        execute_values(cur, f"INSERT INTO al_frames ({','.join(COLS)}) VALUES %s "
                            "ON CONFLICT (cohort, frame_key) DO UPDATE SET "
                            + ",".join(f"{c}=EXCLUDED.{c}" for c in COLS if c not in ("cohort", "frame_key")),
                       meta, page_size=500)
        cur.execute("""
            INSERT INTO image_embeddings (embedding_id, entity_type, entity_id, model_name, dim, embedding)
            SELECT 'al_frame:' || %s || '/' || f.frame_key, 'al_frame', %s || '/' || f.frame_key,
                   e.model_name, e.dim, e.embedding
            FROM al_frames f JOIN image_embeddings e ON e.entity_type='frame' AND e.entity_id=f.frame_key
            WHERE f.cohort=%s
            ON CONFLICT (entity_type, entity_id, model_name) DO UPDATE SET embedding=EXCLUDED.embedding
        """, (cohort, cohort, cohort))
        print(f"          임베딩 복제 {cur.rowcount}", flush=True)
        conn.commit()
        grand.update(stats)
    print("\n합계:", dict(grand))
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
