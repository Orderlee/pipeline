#!/usr/bin/env python3
"""승인된 AL 선별 목록 → Label Studio 이미지 태스크. **dagster 컨테이너에서 실행.**

    docker exec docker-dagster-daemon-1 python3 /tmp/al_to_ls.py \
      --approved /data/fiftyone/frames_bank/report/al_pick/<pool>__margin.approved.csv \
      --categories person,falldown --project-suffix al01 --apply

⚠️ **게이트를 적용하지 않는다.** 게이트(G1)는 "자동 라벨링 결과 0건이면 제외"인데,
AL 선별분은 게이트 설계가 처음부터 열어둔 **AL 분기**(0건 중에서 되살릴 것을 고르는 자리)다.
박스 0개라서 값진 표본일 수 있으므로 여기서 다시 거르면 선별을 무효화한다.
그래서 태스크 meta 에 `source='al_select'` 와 margin 을 박아 **나중에 채점 가능**하게 남긴다.

⚠️ 기본은 dry-run. `--apply` 없이는 LS 에 아무것도 만들지 않는다.
⚠️ SAM3 자동 박스는 예측(prediction)으로 미리 붙인다 — 사람이 0에서 그리지 않게.
   단 그 박스는 `auto_generated` 이지 GT 가 아니다.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys

import psycopg2

sys.path.insert(0, "/src")
sys.path.insert(0, "/src/vlm")

from gemini.ls_tasks_create import (  # noqa: E402
    _ensure_dated_project,
    _image_label_config,
    create_image_task,
    create_prediction,
    sam3_coco_to_ls_rectangles,
)
from gemini.ls_tasks_minio import (  # noqa: E402
    DEFAULT_PRESIGN_EXPIRES,
    build_minio_client,
    generate_presigned_url,
    read_json_from_minio,
    resolve_auth_headers,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--approved", required=True)
    ap.add_argument("--categories", default="person,falldown")
    ap.add_argument("--project-suffix", default="al")
    ap.add_argument("--project-title", default="")
    ap.add_argument("--ls-url", default=os.environ.get("LS_URL", "http://labelstudio:8080"))
    ap.add_argument("--label-bucket", default="vlm-labels")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    rows = list(csv.DictReader(open(args.approved, encoding="utf-8")))
    if args.limit:
        rows = rows[: args.limit]
    cats = [c.strip() for c in args.categories.split(",") if c.strip()]
    pool = os.path.basename(args.approved).split("__")[0]
    title = args.project_title or f"{pool}_{args.project_suffix}"
    print(f"승인 목록 {len(rows)}건 · 프로젝트 '{title}' · 카테고리 {cats}")
    print(f"모드: {'APPLY (실제 생성)' if args.apply else 'DRY-RUN (아무것도 안 만듦)'}")

    minio = build_minio_client(
        os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        os.environ.get("MINIO_ACCESS_KEY", ""),
        os.environ.get("MINIO_SECRET_KEY", ""),
    )
    if not args.apply:
        for r in rows[:5]:
            uri = r["media_uri"]
            b, _, k = uri[len("minio://"):].partition("/")
            try:
                minio.head_object(Bucket=b, Key=k)
                ok = "객체 OK"
            except Exception as exc:  # noqa: BLE001
                ok = f"객체 없음 {type(exc).__name__}"
            print(f"  #{r['rank']:>4} {r['pred']:<9} margin {float(r['margin']):.3f} · {ok} · {k[-60:]}")
        miss = 0
        for r in rows:
            b, _, k = r["media_uri"][len("minio://"):].partition("/")
            try:
                minio.head_object(Bucket=b, Key=k)
            except Exception:  # noqa: BLE001
                miss += 1
        print(f"\n[DRY-RUN] MinIO 객체 누락 {miss}/{len(rows)} · --apply 로 실제 생성")
        return

    hdrs = resolve_auth_headers(args.ls_url, os.environ.get("LS_API_KEY", ""))
    pid = _ensure_dated_project(args.ls_url, hdrs, title, _image_label_config(cats))
    print(f"project_id={pid}")

    # 라운드 추적: CSV 의 round_id 로 al_rounds 를 'sent' 로 올리고 태스크 id 를 되돌려 심는다.
    # 이게 없으면 LS 확정 결과를 어느 선별에서 왔는지 되짚을 수 없다(= 루프가 안 닫힌다).
    rid = (rows[0].get("round_id") or "").strip() if rows else ""
    pg = psycopg2.connect(host="docker-postgres-1", port=5432, user="airflow",
                          password=os.environ.get("POSTGRES_PASSWORD", "airflow"),
                          dbname="vlm_pipeline") if rid else None
    if rid:
        with pg.cursor() as c:
            c.execute("UPDATE al_rounds SET status='sent', ls_project_id=%s WHERE round_id=%s", (pid, rid))
        pg.commit()
        print(f"round_id={rid} → status=sent")
    else:
        print("[WARN] CSV 에 round_id 가 없다 — 추적 없이 진행(구버전 산출물)")

    created = err = nobox = 0
    for r in rows:
        b, _, key = r["media_uri"][len("minio://"):].partition("/")
        try:
            url = generate_presigned_url(minio, b, key, DEFAULT_PRESIGN_EXPIRES)
            task = create_image_task(args.ls_url, hdrs, pid, url, pool)
            tid = task["id"]
            # SAM3 자동 박스가 있으면 예측으로 미리 붙인다(사람이 0에서 그리지 않게)
            npred = 0
            stem = os.path.splitext(os.path.basename(key))[0]
            jkey = f"{os.path.dirname(os.path.dirname(key))}/sam3_segmentations/{stem}.json"
            try:
                coco = read_json_from_minio(minio, args.label_bucket, jkey)
                res = sam3_coco_to_ls_rectangles(coco, set(cats), "imageLabels", "image")
                if res:
                    create_prediction(args.ls_url, hdrs, tid, res)
                    npred = len(res)
            except Exception:  # noqa: BLE001 — 예측 부재는 태스크를 무효화하지 않는다
                nobox += 1
            if rid:
                with pg.cursor() as c:
                    c.execute("UPDATE al_selections SET ls_task_id=%s, approved=TRUE "
                              "WHERE round_id=%s AND frame_key=%s", (tid, rid, r["frame_key"]))
            created += 1
            if created <= 5 or created % 50 == 0:
                print(f"[CREATED] task {tid} ← #{r['rank']} {r['pred']} margin {float(r['margin']):.3f} (pred {npred})")
        except Exception as exc:  # noqa: BLE001 — per-file fail-forward
            err += 1
            print(f"[ERROR] #{r.get('rank')}: {type(exc).__name__} {exc}")
    if rid:
        pg.commit(); pg.close()
    print(f"\n[DONE] 생성 {created} / 오류 {err} / 예측없음 {nobox}")
    print(f"  LS: http://10.0.0.10:8084/projects/{pid}/data")


if __name__ == "__main__":
    main()
