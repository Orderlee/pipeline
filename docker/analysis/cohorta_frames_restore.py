#!/usr/bin/env python3
"""cohort-a outdoor_fall 프레임 JPEG 복원 → MinIO. **dagster 컨테이너에서 실행.**

왜 새로 만들지 않고 '복원'인가: `image_metadata` 에 프레임 행 3,052개가 이미 있고
`image_key`·`frame_sec`(0,2,5,7,9,12…)·`frame_index` 가 정확히 기록돼 있다. 객체만
NAS 재구축 때 날아갔다(표본 12/12 소실 실측). 같은 키에 같은 시각의 프레임을 다시 올리면
DB·라벨(`image_labels` 3,052행, SAM3 박스 4,308개)이 그대로 다시 유효해진다.
새 행을 만들면 그 라벨들과 연결이 끊긴다.

원본은 `archive_path` 로 찾는다 — `/nas/data/archive/cohort-a/...` 는 사라졌고
`/nas/datasets/projects/cohort-a/...`(ro 마운트)가 살아 있어 이번에 갱신해 뒀다.
"""
import io
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, "/src/vlm")
import boto3  # noqa: E402
import psycopg2  # noqa: E402
from botocore.client import Config  # noqa: E402

PG = dict(host="docker-postgres-1", port=5432, user="airflow",
          password=os.environ.get("POSTGRES_PASSWORD", "airflow"), dbname="vlm_pipeline")
FOLDER = os.environ.get("VN_FOLDER", "outdoor_fall")
LIMIT = int(os.environ.get("LIMIT", "0"))

SQL = """
SELECT r.asset_id, r.archive_path, im.image_id, im.image_bucket, im.image_key, im.frame_sec
FROM image_metadata im
JOIN raw_files r ON r.asset_id = im.source_asset_id
WHERE r.source_unit_name = 'cohort-a'
  AND r.raw_key LIKE %s
  AND im.image_role = 'raw_video_frame'
ORDER BY r.asset_id, im.frame_index
"""


def main():
    s3 = boto3.client("s3", endpoint_url=os.environ["MINIO_ENDPOINT"],
                      aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
                      aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
                      config=Config(signature_version="s3v4"))
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute(SQL, (f"%organized_videos/{FOLDER}/%",))
    rows = cur.fetchall()
    cur.close(); conn.close()

    by_asset = {}
    for aid, ap, iid, bkt, key, sec in rows:
        by_asset.setdefault((aid, ap), []).append((iid, bkt, key, sec))
    items = list(by_asset.items())
    if LIMIT:
        items = items[:LIMIT]
    print(f"[{FOLDER}] 영상 {len(items)} · 프레임 {sum(len(v) for _, v in items)}", flush=True)

    up = skip = fail = nosrc = 0
    with tempfile.TemporaryDirectory() as td:
        for n, ((aid, ap), frames) in enumerate(items, 1):
            if not ap or not os.path.isfile(ap):
                nosrc += len(frames); continue
            for iid, bkt, key, sec in frames:
                try:
                    s3.head_object(Bucket=bkt, Key=key)
                    skip += 1; continue          # 이미 있으면 건드리지 않는다(재실행 안전)
                except Exception:
                    pass
                out = f"{td}/f.jpg"
                subprocess.run(["ffmpeg", "-y", "-v", "error", "-ss", f"{float(sec):.3f}",
                                "-i", ap, "-frames:v", "1", "-q:v", "2", out],
                               capture_output=True, timeout=180)
                if not (os.path.isfile(out) and os.path.getsize(out)):
                    fail += 1; continue
                with open(out, "rb") as fh:
                    s3.put_object(Bucket=bkt, Key=key, Body=fh.read(), ContentType="image/jpeg")
                os.remove(out); up += 1
            if n % 25 == 0:
                print(f"  {n}/{len(items)} · 업로드 {up} · 기존 {skip} · 실패 {fail}", flush=True)
    print(f"\n완료 · 업로드 {up} · 기존보유 {skip} · 추출실패 {fail} · 원본없음 {nosrc}")


if __name__ == "__main__":
    main()
