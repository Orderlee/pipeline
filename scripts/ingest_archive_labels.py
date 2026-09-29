#!/usr/bin/env python3
"""아카이브의 확정 타임스탬프 라벨을 MinIO + `labels` 로 1회 적재한다.

  # 컨테이너 안에서 (src/ 는 런타임 컨테이너에 마운트되지 않으므로 stdin 으로 준다)
  docker exec -i docker-dagster-daemon-1 python3 - < scripts/ingest_archive_labels.py            # dry-run
  docker exec -i docker-dagster-daemon-1 python3 - -- --apply < scripts/ingest_archive_labels.py
  docker exec -i docker-dagster-daemon-1 python3 - -- --clips --apply < scripts/ingest_archive_labels.py

무엇을 만드나:
  vlm-labels/<raw_parent>/events/<stem>.json   ← 아카이브 events 리스트 **그대로**
  labels 행 (이벤트당 1행)                      ← timestamp_start/end_sec, review_status='finalized'
  --clips 를 주면 추가로:
  vlm-processed/<raw_parent>/clips/<stem>_<startms>_<endms>.mp4  +  processed_clips 행

왜 변환이 없나 — MinIO events JSON 의 소비자(`gemini_events_to_ls_result`)가 읽는 스키마가
`{"timestamp": [start, end], "category": ...}` 이고 **아카이브 JSON 의 events 원소와 정확히
같다**. 그대로 올리면 LS prediction 도 그대로 붙는다.

`build_dataset` 은 DB 가 아니라 **MinIO 를 직접 list** 해서
(`label_keys = set(minio.list_keys(LABELS_BUCKET, f"{folder_prefix}/"))`)
`build_gemini_label_key(raw_key)` = `<parent>/events/<stem>.json` 이 있는 영상만 집어간다.
그래서 키 규약이 틀리면 **조용히 건너뛴다** — 이 스크립트가 그 규약을 쓰는 이유다.

⚠️ 별칭(alias) 처리 — 아카이브에는 **동일 콘텐츠가 다른 이름으로 두 벌** 있고
   `raw_files.checksum` 이 UNIQUE 라 DB 에는 한 행뿐이다(실측 17쌍). 두 벌 다 적재하면
   한 asset 에 이벤트가 겹쳐 쓰인다. **DB raw_key 와 이름이 맞는 쪽만 채택**하고 나머지는
   버리며 그 사실을 보고한다. 버려진 쪽은 §2.2 의 라벨 노이즈 측정용으로만 쓴다.

⚠️ `labels` 에 category 컬럼이 없다(prod 실측). 클래스는 **MinIO events JSON 에만** 남는다 —
   기존 Gemini 경로와 동일하다. DB 에서 클래스로 질의하려면 migration 026 이 필요하고,
   그건 이미지 리빌드가 선행돼야 한다.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, "/src/vlm")

from vlm_pipeline.lib.key_builders import build_gemini_label_key, build_processed_clip_key  # noqa: E402
from vlm_pipeline.lib.sanitizer import sanitize_filename, sanitize_path_component  # noqa: E402

ARCHIVE_ROOT = Path(os.environ.get("ARCHIVE_ROOT", "/nas/data/archive"))
LABELS_BUCKET = "vlm-labels"
PROCESSED_BUCKET = "vlm-processed"
VIDEO_EXT = {".mp4", ".mov"}
CLIP_RE = re.compile(r"^(?P<stem>.+)_ev(?P<idx>\d+)_(?P<cat>[a-z_]+)_(?P<s>[\d.]+)-(?P<e>[\d.]+)\.mp4$")

# prod labels 실제 컬럼만. caption_text_en / category 는 아직 없다 (migration 025/026 미적용).
_INSERT_SQL = """
INSERT INTO labels (
    label_id, asset_id, labels_bucket, labels_key,
    label_format, label_tool, label_source, review_status,
    event_index, event_count, timestamp_start_sec, timestamp_end_sec,
    caption_text, object_count, label_status, created_at
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (labels_key, event_index) DO UPDATE SET
    asset_id = EXCLUDED.asset_id,
    timestamp_start_sec = EXCLUDED.timestamp_start_sec,
    timestamp_end_sec = EXCLUDED.timestamp_end_sec,
    event_count = EXCLUDED.event_count,
    review_status = EXCLUDED.review_status,
    label_status = EXCLUDED.label_status
"""


def assert_romanizer() -> None:
    got = sanitize_path_component("맞은편")
    if got != "majeunpyeon":
        raise SystemExit(f"sanitizer 불일치({got!r}) — 파이프라인 이미지 안에서 실행하라")


def resolve_assets(cur) -> tuple[dict, dict]:
    cur.execute(
        "SELECT asset_id, raw_key, file_size FROM raw_files WHERE source_unit_name = ANY(%s)",
        (["sourcep", "songpa"],),
    )
    by_name, by_size = {}, defaultdict(list)
    for asset_id, raw_key, size in cur.fetchall():
        rec = {"asset_id": asset_id, "raw_key": raw_key, "file_size": size}
        by_name[raw_key.rsplit("/", 1)[-1]] = rec
        by_size[size].append(rec)
    return by_name, by_size


def collect() -> list[dict]:
    """아카이브 (영상, events) 를 모으고 별칭을 해소한다."""
    rows = []
    for video in sorted(p for p in ARCHIVE_ROOT.glob("*/*/*") if p.suffix.lower() in VIDEO_EXT):
        sidecar = video.with_suffix(".json")
        if not sidecar.exists():
            print(f"[WARN] sidecar 없음: {video.name}")
            continue
        try:
            payload = json.loads(sidecar.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001 — per-file fail-forward
            print(f"[ERROR] JSON 파싱 실패 {sidecar.name}: {exc}")
            continue
        rows.append({
            "video": video,
            "events": payload.get("events") or [],
            "sanitized": sanitize_filename(video.name),
            "size": video.stat().st_size,
        })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="실제로 쓴다 (기본 dry-run)")
    ap.add_argument("--clips", action="store_true", help="클립도 vlm-processed 에 올리고 processed_clips 등록")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    assert_romanizer()
    print("[OK] sanitizer assert 통과")

    import psycopg2
    from vlm_pipeline.resources.minio import MinIOResource

    conn = psycopg2.connect(os.environ["DATAOPS_POSTGRES_DSN"])
    cur = conn.cursor()
    by_name, by_size = resolve_assets(cur)
    entries = collect()
    print(f"[INFO] 아카이브 {len(entries)}건 · raw_files 색인 {len(by_name)}")

    minio = MinIOResource(
        endpoint=os.environ.get("MINIO_ENDPOINT", "http://10.0.0.51:9000"),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )

    # --- 별칭 해소: 이름으로 잡힌 쪽을 채택 ---
    claimed: dict[str, dict] = {}
    dropped: list[str] = []
    for e in entries:
        rec = by_name.get(e["sanitized"])
        how = "name"
        if rec is None:
            cands = by_size.get(e["size"], [])
            rec = cands[0] if len(cands) == 1 else None
            how = "size"
        if rec is None:
            print(f"[MISS] 해소 실패: {e['video'].name}")
            continue
        aid = rec["asset_id"]
        prev = claimed.get(aid)
        if prev is None or (prev["how"] == "size" and how == "name"):
            if prev is not None:
                dropped.append(prev["entry"]["video"].name)
            claimed[aid] = {"entry": e, "rec": rec, "how": how}
        else:
            dropped.append(e["video"].name)

    print(f"[INFO] 채택 {len(claimed)} asset · 별칭으로 버린 파일 {len(dropped)}")
    if dropped and args.verbose:
        for d in dropped[:10]:
            print(f"    drop: {d}")

    n_json = n_rows = n_clip = 0
    now = datetime.now()

    for aid, c in claimed.items():
        e, rec = c["entry"], c["rec"]
        raw_key = rec["raw_key"]
        events = e["events"]
        labels_key = build_gemini_label_key(raw_key)

        if args.apply:
            minio.upload_json(LABELS_BUCKET, labels_key, events)
        n_json += 1

        if events:
            for idx, ev in enumerate(events):
                ts = ev.get("timestamp") or [None, None]
                if args.apply:
                    cur.execute(_INSERT_SQL, (
                        str(uuid4()), aid, LABELS_BUCKET, labels_key,
                        "auto_event_json", "archive_import", "manual_review", "finalized",
                        idx, len(events), ts[0], ts[1],
                        None, 0, "completed", now,
                    ))
                n_rows += 1
        else:
            # 사람이 확인한 negative — NULL 타임스탬프 행이 find_processable 에 잡혀
            # clip_to_frame 이 video_clip_range_missing 으로 영구 실패하는 것을 막는다.
            if args.apply:
                cur.execute(_INSERT_SQL, (
                    str(uuid4()), aid, LABELS_BUCKET, labels_key,
                    "auto_event_json", "archive_import", "manual_review", "finalized",
                    0, 0, None, None, None, 0, "no_events", now,
                ))
            n_rows += 1

        if args.clips:
            n_clip += _ingest_clips(cur, minio, e, raw_key, aid, now, args.apply)

    if args.apply:
        conn.commit()

    print(f"\n=== {'적용' if args.apply else 'DRY-RUN'} ===")
    print(f"  events JSON  {n_json}")
    print(f"  labels 행    {n_rows}")
    if args.clips:
        print(f"  클립         {n_clip}")
    if not args.apply:
        print("\n--apply 를 주면 실제로 쓴다.")


def _ingest_clips(cur, minio, entry, raw_key, asset_id, now, apply) -> int:
    """아카이브 클립 → vlm-processed + processed_clips. 파일명이 창을 담고 있다."""
    stem = entry["video"].stem
    count = 0
    # 클립은 부모 영상 폴더가 아니라 **이벤트 클래스 폴더**에 있으므로 전역으로 찾는다
    for clip in sorted(ARCHIVE_ROOT.glob(f"*/*/clips/{stem}_ev*.mp4")):
        m = CLIP_RE.match(clip.name)
        if not m:
            continue
        start, end = float(m["s"]), float(m["e"])
        clip_key = build_processed_clip_key(
            raw_key, event_index=int(m["idx"]) - 1,
            clip_start_sec=start, clip_end_sec=end, media_type="video",
        )
        if apply:
            with clip.open("rb") as fh:
                minio.upload_fileobj(PROCESSED_BUCKET, clip_key, fh)
            cur.execute(
                """INSERT INTO processed_clips
                   (clip_id, source_asset_id, event_index, clip_start_sec, clip_end_sec,
                    file_size, processed_bucket, clip_key, data_source, process_status, created_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (clip_id) DO NOTHING""",
                (str(uuid4()), asset_id, int(m["idx"]) - 1, start, end,
                 clip.stat().st_size, PROCESSED_BUCKET, clip_key, "archive_import", "completed", now),
            )
        count += 1
    return count


if __name__ == "__main__":
    main()
