#!/usr/bin/env python3
"""certbody sitej 영상 176개를 raw_files/video_metadata 에 등록하고 al_frames 와 잇는다.

**dagster 컨테이너에서 실행** (/nas/data + 살균 함수 + ffprobe 보유).

왜 정식 인제스트(incoming→auto_bootstrap) 를 안 쓰나: 그 경로는 dedup·dispatch·프레임추출을
전부 깨우고 NAS 쿼터를 탄다. 여기 목적은 **프레임이 원본 영상을 가리키게** 하는 것뿐이라
archive 에 파일을 두고 행만 등록한다. `spec_id` NULL 이라 아무 단계도 돌지 않는다
(sourcea 1,751행이 몇 달째 pending 인 것과 같은 상태 — 의도된 것이다).

⚠️ 클래스는 raw_key 에 넣지 않는다. 한 영상이 두 클래스 폴더에 있는 경우가 24건이라
폴더=클래스로 쓰면 오라벨된다(아카이브 코호트가 이미 당한 함정). 클래스는 al_frames 에만 둔다.
"""
import hashlib
import json
import os
import subprocess
import sys
import uuid

sys.path.insert(0, "/src/vlm")
import psycopg2  # noqa: E402
from psycopg2.extras import execute_values  # noqa: E402

from vlm_pipeline.lib.sanitizer import sanitize_path_component  # noqa: E402

PG = dict(host="docker-postgres-1", port=5432, user="airflow",
          password=os.environ.get("POSTGRES_PASSWORD", "airflow"), dbname="vlm_pipeline")
UNIT = "sitej_subway_certbody"
DST = f"/nas/data/archive/{UNIT}"


def sha256(p, chunk=1 << 20):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while (b := f.read(chunk)):
            h.update(b)
    return h.hexdigest()


def probe(p):
    o = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0",
                        "-show_entries", "stream=width,height,r_frame_rate,codec_name,nb_frames:format=duration,bit_rate",
                        "-of", "json", p], capture_output=True, text=True, timeout=120)
    try:
        d = json.loads(o.stdout); st = (d.get("streams") or [{}])[0]; fm = d.get("format", {})
        num, _, den = (st.get("r_frame_rate") or "0/1").partition("/")
        fps = float(num) / float(den or 1) if float(den or 1) else None
        return dict(width=st.get("width"), height=st.get("height"), codec=st.get("codec_name"),
                    fps=fps, duration_sec=float(fm["duration"]) if fm.get("duration") else None,
                    bitrate=int(fm["bit_rate"]) if fm.get("bit_rate") else None,
                    frame_count=int(st["nb_frames"]) if st.get("nb_frames") else None)
    except Exception:
        return {}


def main():
    assert sanitize_path_component("맞은편") == "majeunpyeon", "살균 폴백 환경 — 중단"
    files = sorted(f for f in os.listdir(DST) if f.endswith(".mp4"))
    print(f"{DST}: {len(files)} 파일")

    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute("SELECT checksum, asset_id FROM raw_files WHERE checksum IS NOT NULL")
    known = dict(cur.fetchall())

    raws, vms, dup = [], [], 0
    for f in files:
        p = f"{DST}/{f}"
        ck = sha256(p)
        if ck in known:          # checksum UNIQUE — 이미 있는 자산이면 그 asset 을 재사용
            dup += 1
            continue
        aid = str(uuid.uuid4())
        known[ck] = aid
        raws.append((aid, p, f, "video", os.path.getsize(p), ck, p, "vlm-raw",
                     f"{UNIT}/{f}", UNIT, "archived", "manual_certbody_register", "camera"))
        m = probe(p)
        vms.append((aid, m.get("width"), m.get("height"), m.get("duration_sec"), m.get("fps"),
                    m.get("codec"), m.get("bitrate"), m.get("frame_count")))
    print(f"신규 {len(raws)} · checksum 중복(기존 자산 재사용) {dup}")

    if raws:
        execute_values(cur, """INSERT INTO raw_files
            (asset_id, source_path, original_name, media_type, file_size, checksum, archive_path,
             raw_bucket, raw_key, source_unit_name, ingest_status, transfer_tool, source_type)
            VALUES %s ON CONFLICT (checksum) DO NOTHING""", raws, page_size=200)
        execute_values(cur, """INSERT INTO video_metadata
            (asset_id, width, height, duration_sec, fps, codec, bitrate, frame_count)
            VALUES %s ON CONFLICT (asset_id) DO NOTHING""", vms, page_size=200)
    conn.commit()

    # al_frames.asset_id 연결: video_stem(NFC 한글) → 살균 basename → raw_key
    cur.execute("SELECT raw_key, asset_id FROM raw_files WHERE source_unit_name=%s", (UNIT,))
    by_san = {os.path.basename(rk): aid for rk, aid in cur.fetchall()}
    cur.execute("SELECT frame_key, video_stem FROM al_frames WHERE cohort='sitej_certbody'")
    from vlm_pipeline.lib.sanitizer import sanitize_filename
    upd, nohit = [], 0
    for fk, stem in cur.fetchall():
        aid = by_san.get(sanitize_filename(f"{stem}.mp4")) if stem else None
        if aid:
            upd.append((aid, "sitej_certbody", fk))
        else:
            nohit += 1
    execute_values(cur, "UPDATE al_frames SET asset_id=v.aid FROM (VALUES %s) AS v(aid,co,fk) "
                        "WHERE al_frames.cohort=v.co AND al_frames.frame_key=v.fk", upd, page_size=1000)
    conn.commit()
    print(f"al_frames.asset_id 백필 {len(upd)} · 미해소 {nohit}")
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
