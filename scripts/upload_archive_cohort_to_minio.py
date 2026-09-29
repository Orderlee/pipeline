#!/usr/bin/env python3
"""아카이브 코호트 영상을 MinIO 에 올리고 카테고리별 트리를 만든다.

  docker exec -i docker-dagster-daemon-1 python3 - < scripts/upload_archive_cohort_to_minio.py            # dry-run
  docker exec -i docker-dagster-daemon-1 python3 - -- --selftest < scripts/upload_archive_cohort_to_minio.py
  docker exec -i docker-dagster-daemon-1 python3 - -- --apply < scripts/upload_archive_cohort_to_minio.py

**왜 필요한가.** 2026-08-31 NAS_primary 재구축으로 `vlm-raw` 가 통째로 비었다(객체 0 /
raw_files 129,970 행). 기존 `reupload_minio_from_archive.py` 는 `raw_files.archive_path`
를 소스로 쓰는데 그 경로도 같이 날아갔다(무작위 300개 표본 **300개 전부 부재**). 지금 NAS 에
남아 있는 바이트는 `/nas/data/archive` 의 PoC export 뿐이라, 복구 가능한 코호트는 여기뿐이다.

**두 갈래를 한 번의 업로드로 처리한다.**

    vlm-raw/<canonical raw_key>                          ← 업로드 (326.7GB)
    vlm-classification/<unit>/video/<category>/<rel>.<ext>  ← 서버사이드 복사 (네트워크 0)

`vlm-raw` 키는 **반드시 DB 의 raw_key** 다. `raw_key = <source_unit_name>/<rel_path>` 가
전역 불변식이고, 아카이브의 카테고리 폴더는 raw_key 에 존재하지 않는다 — 그건 eng-a 의
`build_poc_export.py` 가 `dump7.json` 에서 나중에 만든 **파생 그룹핑**이다. 카테고리 폴더를
raw_key 로 쓰면 DB 129,970 행이 전부 어긋난다. 그래서 카테고리 분할은 `vlm-classification`
에서만 한다 (`defs/build/classification.py` 가 쓰는 것과 **동일한 키 규약**이라 나중에 그
asset 을 돌려도 같은 자리에 떨어진다).

**카테고리는 폴더명이 아니라 이벤트 JSON 의 `category` 를 쓴다.** 폴더 클래스는 정본이
아니다 — `sourcep/fire/` 안에 smoke 이벤트가 86건 있어서, 폴더로 라벨하면 773 이벤트 중
112건이 오라벨된다. 영상 하나가 여러 클래스를 담으면 각 클래스 폴더에 모두 걸린다(검색
관점에서 이게 맞다). 폴더명과 어긋난 건수는 마지막에 보고한다.

**단 하나의 예외가 `normal` 이다.** 정본 category 값은 fire·smoke·intrusion·no_harness·
falldown 5개뿐이고 `normal` 은 **없다** — normal 은 "이벤트 0개"로만 표현된다. 이벤트를
그대로 따르면 normal 영상 32개가 카테고리 트리에서 통째로 사라진다. 이벤트 0개인 JSON 은
실측 **32개 전부가 `*/normal/` 폴더**이고 그 반대(normal 폴더인데 이벤트 있음)도 0건이라,
"이벤트 0개 = normal" 은 추측이 아니라 데이터가 만장일치로 말하는 사실이다. 그래도 폴더가
normal 이 아닌데 이벤트가 0개인 영상은 카테고리를 붙이지 않고 **이상치로 보고**한다 —
지금은 0건이지만 아카이브가 갱신되면 조용히 오분류될 자리다.

**함정 2개 (실측으로 확인함)**

  1. songpa `*_merged.mp4` 10개가 20~23GB 다. S3 `CopyObject` 는 **단일 파트 5GB 상한**이
     있어 `MinIOResource.copy()`(= `copy_object`) 가 이것들에서 실패한다. 그래서 카테고리
     복사는 멀티파트를 처리하는 관리형 `client.copy()` 를 쓴다.
  2. `build_helpers._copy_if_outdated` 는 재사용할 수 없다. `DATASET_STORAGE` 기본값이
     `fs` 라 `dst_bucket` 인자를 무시하고 NFS 로 써버린다.

**멱등**. 목적지에 같은 크기의 객체가 이미 있으면 건너뛴다. 중단 후 재실행해도 안전하다.
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sys
from collections import defaultdict
from pathlib import Path, PurePosixPath

sys.path.insert(0, "/src/vlm")

from vlm_pipeline.lib.env_utils import default_postgres_dsn  # noqa: E402
from vlm_pipeline.lib.sanitizer import sanitize_filename, sanitize_path_component  # noqa: E402

ARCHIVE_ROOT = Path(os.environ.get("ARCHIVE_ROOT", "/nas/data/archive"))
RAW_BUCKET = "vlm-raw"
CLASSIFICATION_BUCKET = "vlm-classification"
VIDEO_SUFFIXES = {".mp4", ".mov", ".avi", ".mkv", ".ts", ".m4v"}


# ---------------------------------------------------------------------------
# 경로 규약 — defs/build/classification.py 와 동일해야 한다
# ---------------------------------------------------------------------------


def minio_prefix_from_key(key: str) -> str:
    """raw_key 의 첫 세그먼트 = MinIO folder prefix."""
    parts = PurePosixPath(str(key or "").strip()).parts
    return parts[0] if parts else ""


def classification_key(raw_key: str, category: str) -> str | None:
    """vlm-classification/<prefix>/video/<safe_cat>/<basename>.

    `defs/build/classification.py` 는 `_rel_stem_path` 로 원본 하위 경로를 보존해서
    `sourcep/video/intrusion/organized_videos/unauthorized_intrusion/x.mp4` 를 만든다.
    카테고리 폴더 밑에 원본 폴더명이 또 나와 카테고리 트리로서 읽히지 않으므로 여기서는
    **basename 만** 붙여 평탄화한다. 코호트 236개 전부에 대해 (unit, category, basename)
    충돌이 **0건**임을 실측으로 확인했다. 충돌이 생기면 호출자가 감지해 보고한다.
    """
    safe_cat = sanitize_path_component(category)
    if not safe_cat or safe_cat == "unnamed":
        return None
    prefix = minio_prefix_from_key(raw_key)
    return f"{prefix}/video/{safe_cat}/{PurePosixPath(raw_key).name}"


# ---------------------------------------------------------------------------
# 인덱스
# ---------------------------------------------------------------------------


def load_raw_key_index(dsn: str) -> dict[str, list[str]]:
    """살균 basename → raw_key 목록.

    LIKE 로 매칭하지 않는다. raw_key 에는 `_` 가 흔한데 LIKE 에서 `_` 는 임의 1자
    와일드카드라 엉뚱한 행을 집는다. Python dict 로 정확 일치시킨다.
    """
    import psycopg2

    idx: dict[str, list[str]] = defaultdict(list)
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT raw_key FROM raw_files
                WHERE media_type = 'video'
                  AND raw_key IS NOT NULL AND length(trim(raw_key)) > 0
                """
            )
            for (raw_key,) in cur:
                idx[PurePosixPath(raw_key).name].append(raw_key)
    finally:
        conn.close()
    return idx


def archive_videos() -> list[Path]:
    """`<unit>/<category>/<video>` — clips/ 산출물은 제외한다."""
    out = [
        p
        for p in ARCHIVE_ROOT.glob("*/*/*")
        if p.is_file() and p.suffix.lower() in VIDEO_SUFFIXES
    ]
    return sorted(out)


NEGATIVE_FOLDER = "normal"


def event_categories(video: Path) -> tuple[set[str], bool]:
    """옆에 놓인 `<stem>.json` 에서 category 집합. (카테고리, JSON 존재 여부).

    이벤트 0개면 사람이 확인한 negative 다. 정본에 `normal` 카테고리 값이 없으므로
    폴더가 `normal` 일 때만 `normal` 로 준다. 폴더가 다른데 0개면 빈 집합을 돌려
    호출자가 이상치로 보고하게 한다 (조용히 사라지지 않게).
    """
    sidecar = video.with_suffix(".json")
    if not sidecar.exists():
        return set(), False
    try:
        payload = json.loads(sidecar.read_text(encoding="utf-8"))
    except Exception:
        return set(), False
    events = payload.get("events") if isinstance(payload, dict) else payload
    if not isinstance(events, list):
        return set(), True
    cats = {str(e.get("category") or "").strip() for e in events if isinstance(e, dict)}
    cats = {c for c in cats if c}
    if not cats and video.parent.name.lower() == NEGATIVE_FOLDER:
        return {NEGATIVE_FOLDER}, True
    return cats, True


# ---------------------------------------------------------------------------
# 전송
# ---------------------------------------------------------------------------


def head_size(minio, bucket: str, key: str) -> int | None:
    head = minio.head(bucket, key)
    if head is None:
        return None
    return int(head.get("size") or head.get("ContentLength") or -1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="처리할 영상 수 상한 (0=전부)")
    ap.add_argument("--max-gb", type=float, default=0.0, help="이 크기 넘는 영상은 건너뜀 (0=제한없음)")
    ap.add_argument("--skip-raw", action="store_true", help="vlm-raw 업로드 생략, 카테고리 복사만")
    ap.add_argument("--selftest", action="store_true", help="경로 규약 자체검증 후 종료")
    args = ap.parse_args()

    if args.selftest:
        selftest()
        return

    from vlm_pipeline.resources.minio import MinIOResource

    minio = MinIOResource(
        endpoint=os.environ.get("MINIO_ENDPOINT", "http://10.0.0.51:9000"),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )
    if args.apply:
        minio.ensure_bucket(RAW_BUCKET)
        minio.ensure_bucket(CLASSIFICATION_BUCKET)

    idx = load_raw_key_index(default_postgres_dsn())
    videos = archive_videos()
    print(f"아카이브 영상 {len(videos)}개 / raw_key 인덱스 {len(idx)}개\n")

    unmatched: list[Path] = []
    ambiguous: list[Path] = []
    no_json: list[Path] = []
    uncategorized: list[Path] = []
    folder_mismatch: list[tuple[str, str, str]] = []
    dst_owner: dict[str, str] = {}
    collisions: list[tuple[str, str, str]] = []
    n_uploaded = n_raw_skip = n_copied = n_copy_skip = 0
    bytes_uploaded = 0
    processed = 0

    for video in videos:
        if args.limit and processed >= args.limit:
            break
        base = sanitize_filename(video.name)
        keys = idx.get(base) or []
        if not keys:
            unmatched.append(video)
            continue
        if len(set(keys)) > 1:
            ambiguous.append(video)
            continue
        raw_key = keys[0]
        size = video.stat().st_size
        if args.max_gb and size > args.max_gb * 1024**3:
            continue
        processed += 1

        # ---- 1. vlm-raw ----
        if not args.skip_raw:
            if head_size(minio, RAW_BUCKET, raw_key) == size:
                n_raw_skip += 1
            else:
                print(f"  UP  {size/1024**3:7.2f}GB  {raw_key}")
                if args.apply:
                    ctype = mimetypes.guess_type(video.name)[0] or "video/mp4"
                    minio.upload_file(RAW_BUCKET, raw_key, video, content_type=ctype)
                n_uploaded += 1
                bytes_uploaded += size

        # ---- 2. vlm-classification (서버사이드) ----
        cats, has_json = event_categories(video)
        if not has_json:
            no_json.append(video)
            continue
        if not cats:
            uncategorized.append(video)
            continue
        folder_cat = video.parent.name
        for cat in sorted(cats):
            if sanitize_path_component(cat) != sanitize_path_component(folder_cat):
                folder_mismatch.append((str(video.relative_to(ARCHIVE_ROOT)), folder_cat, cat))
            dst = classification_key(raw_key, cat)
            if dst is None:
                continue
            # 평탄화가 서로 다른 원본을 한 자리에 겹치면 조용히 덮어쓴다. 막고 보고한다.
            prev = dst_owner.get(dst)
            if prev is not None and prev != raw_key:
                collisions.append((dst, prev, raw_key))
                continue
            dst_owner[dst] = raw_key
            if head_size(minio, CLASSIFICATION_BUCKET, dst) == size:
                n_copy_skip += 1
                continue
            print(f"  CP  {cat:<12} {dst}")
            if args.apply:
                # copy_object 가 아니라 관리형 copy — 5GB 넘는 songpa merged 때문.
                minio.client.copy(
                    CopySource={"Bucket": RAW_BUCKET, "Key": raw_key},
                    Bucket=CLASSIFICATION_BUCKET,
                    Key=dst,
                    Config=minio.transfer_config,
                )
            n_copied += 1

    mode = "적용" if args.apply else "DRY-RUN"
    print(f"\n=== {mode} ===")
    print(f"  처리 영상        {processed}")
    print(f"  vlm-raw 업로드   {n_uploaded}  ({bytes_uploaded/1024**3:.1f} GB)   이미있음 {n_raw_skip}")
    print(f"  분류 복사        {n_copied}   이미있음 {n_copy_skip}")
    print(f"  raw_key 미매칭   {len(unmatched)}")
    print(f"  basename 모호    {len(ambiguous)}")
    print(f"  이벤트 JSON 없음 {len(no_json)}")
    print(f"  카테고리 없음(이상치) {len(uncategorized)}")
    print(f"  폴더≠정본 카테고리 {len(folder_mismatch)}")
    print(f"  목적지 키 충돌   {len(collisions)}")
    for p in unmatched:
        print(f"    미매칭: {p.relative_to(ARCHIVE_ROOT)}")
    for p in ambiguous:
        print(f"    모호  : {p.relative_to(ARCHIVE_ROOT)}")
    for p in uncategorized:
        print(f"    무카테고리: {p.relative_to(ARCHIVE_ROOT)}")
    for dst, a, b in collisions:
        print(f"    충돌  : {dst}\n            {a}\n            {b}")
    if not args.apply:
        print("\n--apply 를 주면 실제로 쓴다.")


def selftest() -> None:
    """경로 규약이 defs/build/classification.py 와 같은지 고정한다."""
    rk = "sourcep/organized_videos/unauthorized_intrusion/20150104_213324a_-_1of10.mp4"
    assert minio_prefix_from_key(rk) == "sourcep"
    # 카테고리 밑은 평탄하다 — 원본 폴더명이 다시 나오면 안 된다
    assert classification_key(rk, "intrusion") == (
        "sourcep/video/intrusion/20150104_213324a_-_1of10.mp4"
    )
    assert "organized_videos" not in classification_key(rk, "intrusion")
    assert "unauthorized_intrusion" not in classification_key(rk, "intrusion").split("/video/")[1]
    # 확장자는 raw_key 것을 따른다 (아카이브의 .MOV 대소문자에 끌려가지 않게)
    mov = "sourcep/organized_videos/fire/file210101-000858f.mov"
    assert classification_key(mov, "smoke").endswith("file210101-000858f.mov")
    # 한글/공백 카테고리도 세그먼트로 안전해야 한다
    assert classification_key(rk, "쓰러짐") is not None
    assert "/video/" in (classification_key(rk, "no harness") or "")
    assert classification_key(rk, "no harness").split("/video/")[1].startswith("no_harness/")
    # 빈 카테고리는 폴더를 만들지 않는다
    assert classification_key(rk, "   ") is None
    # raw_key 에 카테고리 폴더가 섞여 들어가면 안 된다 (불변식)
    assert "/fire/" not in rk

    # 이벤트 0개 처리: normal 폴더면 normal, 아니면 빈 집합(이상치로 보고)
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        for folder, expect in (("normal", {"normal"}), ("fire", set())):
            d = Path(td) / "unit" / folder
            d.mkdir(parents=True)
            vid = d / "a.mp4"
            vid.touch()
            (d / "a.json").write_text(json.dumps({"events": []}), encoding="utf-8")
            assert event_categories(vid) == (expect, True), folder
        # 이벤트가 있으면 폴더와 무관하게 정본을 따른다
        d = Path(td) / "unit" / "fire"
        vid2 = d / "b.mp4"
        vid2.touch()
        (d / "b.json").write_text(
            json.dumps({"events": [{"category": "smoke"}, {"category": "fire"}]}), encoding="utf-8"
        )
        assert event_categories(vid2) == ({"smoke", "fire"}, True)
        # JSON 자체가 없으면 has_json=False (normal 로 넘어가지 않는다)
        d3 = Path(td) / "unit" / "normal"
        vid3 = d3 / "c.mp4"
        vid3.touch()
        assert event_categories(vid3) == (set(), False)
    print("selftest OK")


if __name__ == "__main__":
    main()
