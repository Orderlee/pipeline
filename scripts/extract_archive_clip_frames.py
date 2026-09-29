#!/usr/bin/env python3
"""아카이브 클립에서 프레임을 추출해 MinIO + image_metadata 에 적재한다.

  docker exec -i docker-dagster-daemon-1 python3 - < scripts/extract_archive_clip_frames.py           # dry-run
  docker exec -i docker-dagster-daemon-1 python3 - -- --apply < scripts/extract_archive_clip_frames.py
  docker exec -i docker-dagster-daemon-1 python3 - -- --apply --interval 2.0 --max-per-clip 8 < ...

**왜 프레임이 필요한가 — 트랙마다 이유가 다르다.**

    타임스탬프만 하는 프로젝트 : 라벨링에는 프레임이 안 쓰인다. 그런데도 필요한 이유는
        `video_embedding` 이 **frame_pool**(프레임 임베딩의 L2 정규화 평균)로만 영상 벡터를
        만들기 때문이다(video_model 은 미구현). 프레임이 0장이면 그 영상은 AL 후보 좌표에
        아예 올라가지 못한다. → **적게라도 있어야 한다**
    bbox 하는 프로젝트         : 프레임이 **라벨링 단위 그 자체**다. → 조밀해야 한다
    동시 진행 프로젝트         : bbox 기준이 상위집합이므로 조밀 기준을 따른다

실사용 `dispatch_requests.labeling_method` 실측: bbox 25 / timestamp_video 11 /
timestamp_video+bbox 1. **세 조합이 전부 존재**하므로 밀도는 프로젝트 단위로 갈려야 한다.
이 스크립트는 아카이브 코호트를 **투트랙 모두에 먹이기 위해 조밀 기준**으로 뽑는다.

기본 정책 = 2초 간격 · 클립당 최대 8장 (708 클립 → 약 3,900 프레임). 근거:
  - 클립 실측: 중앙값 10.8s · p90 84.5s · 최대 900.3s. 긴 꼬리가 총량을 지배한다
  - 무제한이면 14,699장인데 상한 8장이면 3,897장. **이벤트당 중앙값은 6장으로 동일** —
    자르는 건 오직 긴 꼬리(900s smoke 450장 → 8장)뿐이다
  - 그 꼬리는 버려도 손실이 적다. 연속 프레임 코사인 실측상 2초 간격에서도 **55.3%가
    cos>0.95** 로 거의 중복이다

원본은 **NAS 아카이브 파일에서 직접** 읽는다(MinIO 다운로드 없음). `extract_frame_jpeg_bytes`
는 timeout + 재시도를 갖고 있어 CIFS 지연에 안전하다(클립 절단 경로와 달리).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, "/src/vlm")

from vlm_pipeline.lib.key_builders import build_processed_clip_image_key  # noqa: E402
from vlm_pipeline.lib.sanitizer import sanitize_filename  # noqa: E402
from vlm_pipeline.lib.video_frames import (  # noqa: E402
    describe_frame_bytes,
    extract_frame_jpeg_bytes,
    plan_frame_timestamps,
)

ARCHIVE_ROOT = Path(os.environ.get("ARCHIVE_ROOT", "/nas/data/archive"))
PROCESSED_BUCKET = "vlm-processed"
# DB 쪽: <살균 stem>_<startms:08d>_<endms:08d>.mp4
CLIP_RE = re.compile(r"^(?P<stem>.+)_(?P<s>\d{8})_(?P<e>\d{8})\.mp4$")
# 디스크 쪽: <원본 stem>_ev<NN>_<cat>_<start>-<end>.mp4
DISK_CLIP_RE = re.compile(r"^(?P<stem>.+)_ev(?P<idx>\d+)_(?P<cat>[a-z_]+)_(?P<s>[\d.]+)-(?P<e>[\d.]+)\.mp4$")


def build_clip_disk_index() -> dict[tuple[str, int, int], Path]:
    """(살균 stem, 시작 ms, 종료 ms) → 디스크 클립 경로.

    DB `clip_key` 는 **살균된 raw_key** 기반(`20150102_013401a_...`)이고 디스크는 **원본
    파일명**(`20150102_013401A_ev01_fire_6.900-22.233.mp4`)이라 이름이 그대로는 안 맞는다.
    그래서 살균 stem + 창(시작·종료 ms)으로 색인한다.

    ⚠️ **시작시각만으로는 안 된다** — 한 영상에 `start=0` 인 이벤트가 여러 개인 경우가
    실측 5그룹 10클립 있다(예: `20150102_030636a` 의 0-19.033s 와 0-63.333s).
    시작만 키로 쓰면 63초 클립의 62초 지점을 19초 파일에서 뽑으려다 실패한다(실제 발생).
    종료시각까지 넣어야 유일해진다.

    클립은 **부모 영상 폴더가 아니라 이벤트 클래스 폴더**에 있으므로(songpa/fire/clips 안에
    falldown 클립) 전역 인덱스가 필요하다.
    """
    idx: dict[tuple[str, int, int], Path] = {}
    for p in ARCHIVE_ROOT.glob("*/*/clips/*.mp4"):
        m = DISK_CLIP_RE.match(p.name)
        if not m:
            continue
        stem = sanitize_filename(m["stem"] + ".mp4")[: -len(".mp4")]
        key = (stem, int(round(float(m["s"]) * 1000)), int(round(float(m["e"]) * 1000)))
        idx.setdefault(key, p)
    return idx


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--interval", type=float, default=2.0, help="추출 간격(초)")
    ap.add_argument("--max-per-clip", type=int, default=8, help="클립당 최대 프레임")
    ap.add_argument("--limit", type=int, default=0, help="처리할 클립 수 상한 (0=전부)")
    args = ap.parse_args()

    import psycopg2
    from vlm_pipeline.resources.minio import MinIOResource

    conn = psycopg2.connect(os.environ["DATAOPS_POSTGRES_DSN"])
    cur = conn.cursor()
    minio = MinIOResource(
        endpoint=os.environ.get("MINIO_ENDPOINT", "http://10.0.0.51:9000"),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )

    cur.execute(
        """SELECT clip_id, source_asset_id, clip_key, clip_start_sec, clip_end_sec
           FROM processed_clips
           WHERE data_source = 'archive_import'
             AND COALESCE(image_extract_status, 'pending') <> 'completed'
           ORDER BY clip_key"""
    )
    clips = cur.fetchall()
    if args.limit:
        clips = clips[: args.limit]
    disk = build_clip_disk_index()
    print(f"[INFO] 대상 클립 {len(clips)} · 디스크 클립 인덱스 {len(disk)}")
    print(f"[INFO] 정책: {args.interval}s 간격 · 클립당 최대 {args.max_per_clip}장")

    n_frames = n_clips = n_missing = n_failed = 0

    for clip_id, asset_id, clip_key, start, end in clips:
        # DB clip_key(살균 stem + ms) ↔ 디스크(원본 stem + 초) 를 (살균stem, 시작ms) 로 맞춘다
        m = CLIP_RE.match(Path(clip_key).name)
        src = disk.get((m["stem"], int(m["s"]), int(m["e"]))) if m else None
        if src is None or not src.exists():
            n_missing += 1
            if n_missing <= 3:
                print(f"[MISS] 디스크 원본 없음: {Path(clip_key).name}")
            continue

        duration = float(end) - float(start)
        secs = plan_frame_timestamps(
            duration_sec=duration, fps=None, frame_count=None,
            max_frames_per_video=args.max_per_clip, frame_interval_sec=args.interval,
        )

        made = 0
        for i, sec in enumerate(secs, start=1):
            try:
                jpeg = extract_frame_jpeg_bytes(src, sec)
            except Exception as exc:  # noqa: BLE001 — per-frame fail-forward
                n_failed += 1
                print(f"[WARN] 프레임 추출 실패 {src.name}@{sec}s: {str(exc)[:80]}")
                continue
            image_key = build_processed_clip_image_key(clip_key, i)
            if args.apply:
                meta = describe_frame_bytes(jpeg)
                minio.upload(PROCESSED_BUCKET, image_key, jpeg, content_type="image/jpeg")
                cur.execute(
                    """INSERT INTO image_metadata
                       (image_id, source_asset_id, source_clip_id, image_bucket, image_key,
                        image_role, frame_index, frame_sec, file_size, width, height,
                        color_mode, bit_depth, has_alpha, orientation)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (image_bucket, image_key) DO NOTHING""",
                    (str(uuid4()), asset_id, clip_id, PROCESSED_BUCKET, image_key,
                     "processed_clip_frame", i, float(sec), meta["file_size"],
                     meta["width"], meta["height"], meta["color_mode"],
                     meta["bit_depth"], meta["has_alpha"], meta["orientation"]),
                )
            made += 1
            n_frames += 1

        if args.apply and made:
            cur.execute(
                """UPDATE processed_clips
                   SET image_extract_status='completed', image_extract_count=%s,
                       image_extracted_at=now()
                   WHERE clip_id=%s""",
                (made, clip_id),
            )
            conn.commit()
        n_clips += 1
        if n_clips % 50 == 0:
            print(f"  … {n_clips}/{len(clips)} 클립 · 프레임 {n_frames}", flush=True)

    print(f"\n=== {'적용' if args.apply else 'DRY-RUN'} ===")
    print(f"  처리 클립   {n_clips}")
    print(f"  프레임      {n_frames}")
    print(f"  원본 없음   {n_missing}")
    print(f"  추출 실패   {n_failed}")
    if not args.apply:
        print("\n--apply 를 주면 실제로 쓴다.")


if __name__ == "__main__":
    main()
