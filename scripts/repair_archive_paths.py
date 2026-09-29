#!/usr/bin/env python3
"""PoC export 가 아카이브 트리를 재편하면서 끊긴 `raw_files.archive_path` 를 복구한다.

  # 컨테이너 안에서 돌린다 (아래 "왜 컨테이너인가" 참조)
  docker exec -i docker-dagster-daemon-1 python3 - < scripts/repair_archive_paths.py            # dry-run
  docker exec -i docker-dagster-daemon-1 python3 - < scripts/repair_archive_paths.py -- --verbose
  docker exec -i docker-dagster-daemon-1 python3 - < scripts/repair_archive_paths.py -- --apply

배경 (2026-09-10 실측):
  `materialize_video_path`(lib/media_utils.py)는 **archive_path 를 먼저 보고 존재하면 그대로
  쓴다**. MinIO 는 fallback 이다. 그래서 MinIO 5버킷이 비어 있어도 이 코호트의 원본 영상은
  블로커가 아니다 — 단 archive_path 가 살아 있을 때만.

  그런데 PoC export 가 아카이브를 `<site>/<class>/` 로 재편하면서 DB 가 가리키던 경로가 사라졌다:

    DB archive_path : /nas/data/archive/songpa/organized_videos/24hour_video/merged_videos/3611_....mp4
    실제            : No such file or directory
    현재 위치       : /nas/data/archive/songpa/smoke/3611_... 가락본동118(팔각정어린이공원)_merged.mp4

  이 상태로 clip_to_frame 을 돌리면 archive 분기를 놓치고 빈 MinIO 로 떨어져 **파일마다 실패**한다.

해소 사다리 (계획 A2):
  1) 살균 basename **완전 일치** — source_unit_name 으로 스코프해서 형제 파일 오매칭을 막는다.
     `find_by_raw_key_stem` 은 쓰지 않는다: `LIKE %s || '.%'` 인데 살균 스템은 `_` 가 빽빽하고
     `_` 는 LIKE 의 단일문자 와일드카드다. ORDER BY 없는 LIMIT 1 이라 형제가 잡힌다.
  2) file_size 완전 일치 — 아카이브에는 **동일 콘텐츠가 다른 파일명으로 두 벌** 들어 있고
     (`-0176`↔`-0177`, `FILE...F`↔`FILE...F-008`), raw_files.checksum 이 UNIQUE 라 인제스트는
     한쪽만 남겼다. 아카이브가 **버려진 쪽 이름**에 라벨을 달아서 17건이 이름으로 안 잡힌다.
     file_size 는 DB 에 100% 채워져 있고 파일을 읽지 않아 공짜다.
  3) `--verify-checksum` 을 주면 sha256 까지 대조한다. 327GB CIFS 라 기본값은 off.

왜 컨테이너인가 — `lib/sanitizer.py` 의 한글 로마자화가 **환경 의존적**이다.
  `_romanize_korean` 은 optional `korean_romanizer` 패키지를 우선 쓰고 없으면 내부 자모 표로
  조용히 폴백한다. 패키지 없는 호스트에서 "맞은편" → `mateunpyeon`, prod 키는 `majeunpyeon`.
  호스트에서 계산하면 한글 이름이 "DB 에 없음"으로 오판된다. 그래서 부팅 assert 로 고정한다.

이 스크립트는 `archive_path` 컬럼만 UPDATE 한다. 파일을 옮기거나 지우지 않는다.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__) or ".", "..", "src"))
sys.path.insert(0, "/src/vlm")  # 컨테이너에서 stdin 실행 시 __file__ 이 없다

from vlm_pipeline.lib.sanitizer import sanitize_filename, sanitize_path_component  # noqa: E402

ARCHIVE_ROOT = Path(os.environ.get("ARCHIVE_ROOT", "/nas/data/archive"))
# JSON 의 origin 은 호스트 경로. 컨테이너 마운트로 옮긴다.
ORIGIN_HOST_PREFIX = "/home/user/mou/nas_secondary/datasets/projects"
ORIGIN_CONTAINER_PREFIX = "/nas/datasets/projects"
VIDEO_EXT = {".mp4", ".mov", ".MOV", ".MP4"}


def assert_romanizer() -> None:
    """살균 결과가 prod 키와 같은 규칙인지 고정. 다르면 조인 결과 전부 무효다."""
    got = sanitize_path_component("맞은편")
    if got != "majeunpyeon":
        raise SystemExit(
            f"sanitizer 불일치: sanitize_path_component('맞은편') = {got!r}, 기대 'majeunpyeon'.\n"
            "korean_romanizer 패키지가 없는 환경이다. 파이프라인 이미지 안에서 실행하라."
        )


def sanitized_key(object_name: str) -> str:
    """경로 → INGEST 정본 raw_key (repair_unsanitized_raw_keys.py 와 동일 규칙)."""
    parts = [p for p in str(object_name).split("/") if p]
    if not parts:
        return ""
    *dirs, filename = parts
    return "/".join([sanitize_path_component(d) for d in dirs] + [sanitize_filename(filename)])


def build_master_index(sites: set[str]) -> dict[str, Path]:
    """마스터 데이터셋(nas_secondary)의 basename → 경로 색인. 한 번만 훑는다.

    파일당 glob 을 돌리면 CIFS 에서 236회 왕복이라 2분을 넘긴다. 단일 walk 로 끝낸다.
    """
    index: dict[str, Path] = {}
    for site in sorted(sites):
        base = Path(ORIGIN_CONTAINER_PREFIX) / site / "organized_videos"
        if not base.is_dir():
            print(f"[WARN] 마스터 없음: {base}")
            continue
        for root, _dirs, files in os.walk(base):
            for fn in files:
                if Path(fn).suffix.lower() in {".mp4", ".mov"}:
                    index.setdefault(fn, Path(root) / fn)
    print(f"[INFO] 마스터 색인: {len(index)}개 영상 ({ORIGIN_CONTAINER_PREFIX})")
    return index


def resolve_new_archive_path(video: Path, origin: str, master: dict[str, Path]) -> Path | None:
    """복구할 archive_path 를 정한다. 없으면 None.

    **마스터(nas_secondary)를 우선한다.** 아카이브는 PoC export 의 작업영역이라 `--clean-clips`
    로 재생성되지만 마스터는 안정적이고, 컨테이너에 **read-only** 로 붙어 있어 파이프라인이
    훼손할 수 없다. 실측(2026-09-10): 아카이브 236건 전부 마스터에 같은 이름·같은 크기로 있다.

    origin 필드는 신뢰하지 않는다 — export 완료 후 235/236 이 아카이브 자신을 가리키도록
    재작성됐다(측정치). 이름+크기 대조가 더 견고하다.
    """
    cand = master.get(video.name)
    if cand is not None and cand.exists() and cand.stat().st_size == video.stat().st_size:
        return cand
    if origin.startswith(ORIGIN_HOST_PREFIX):
        c2 = Path(origin.replace(ORIGIN_HOST_PREFIX, ORIGIN_CONTAINER_PREFIX, 1))
        if c2.exists():
            return c2
    return video if video.exists() else None


def scan_archive() -> list[dict]:
    """아카이브의 (영상, sidecar JSON) 쌍을 모은다."""
    rows: list[dict] = []
    for video in sorted(p for p in ARCHIVE_ROOT.glob("*/*/*") if p.suffix in VIDEO_EXT):
        sidecar = video.with_suffix(".json")
        origin = ""
        if sidecar.exists():
            try:
                origin = str(json.loads(sidecar.read_text(encoding="utf-8")).get("origin") or "")
            except Exception as exc:  # noqa: BLE001 — per-file fail-forward
                print(f"[WARN] JSON 읽기 실패 {sidecar.name}: {exc}")
        rows.append({
            "video": video,
            "site": video.relative_to(ARCHIVE_ROOT).parts[0],
            "origin": origin,
            "sanitized_basename": sanitize_filename(video.name),
            "size": video.stat().st_size,
        })
    return rows


def load_db_index(db, sites: set[str]) -> tuple[dict, dict]:
    """source_unit_name 이 sites 인 raw_files 를 basename / file_size 로 색인."""
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT asset_id, raw_key, file_size, checksum, archive_path "
            "FROM raw_files WHERE source_unit_name = ANY(%s)",
            (sorted(sites),),
        )
        rows = cur.fetchall()
    by_name: dict[str, list[dict]] = defaultdict(list)
    by_size: dict[int, list[dict]] = defaultdict(list)
    for asset_id, raw_key, file_size, checksum, archive_path in rows:
        rec = {"asset_id": asset_id, "raw_key": raw_key, "file_size": file_size,
               "checksum": checksum, "archive_path": archive_path}
        by_name[str(raw_key).rsplit("/", 1)[-1]].append(rec)
        by_size[file_size].append(rec)
    print(f"[INFO] raw_files 색인: {len(rows)}행 (source_unit_name in {sorted(sites)})")
    return by_name, by_size


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="실제로 UPDATE 한다 (기본은 dry-run)")
    ap.add_argument("--verbose", action="store_true", help="건별로 출력")
    ap.add_argument("--verify-checksum", action="store_true", help="sha256 까지 대조 (느림)")
    args = ap.parse_args()

    assert_romanizer()
    print("[OK] sanitizer assert 통과 — ' 맞은편' → majeunpyeon")

    from vlm_pipeline.resources.postgres import PostgresResource

    dsn = os.environ.get("DATAOPS_POSTGRES_DSN")
    if not dsn:
        raise SystemExit("DATAOPS_POSTGRES_DSN not set")
    db = PostgresResource(dsn=dsn)

    entries = scan_archive()
    sites = {e["site"] for e in entries}
    print(f"[INFO] 아카이브 영상 {len(entries)}건, 사이트 {sorted(sites)}")
    by_name, by_size = load_db_index(db, sites)
    master = build_master_index(sites)

    updates: list[tuple[str, str]] = []  # (asset_id, new_archive_path)
    stat = defaultdict(int)
    size_mismatch: list[str] = []
    # 여러 아카이브 파일이 같은 asset 으로 해소되는 경우를 반드시 잡는다.
    # 아카이브에는 **동일 콘텐츠가 다른 이름으로 두 벌** 있고 raw_files.checksum 이 UNIQUE 라
    # DB 에는 한 행뿐이다. archive_path 는 어느 쪽이 이겨도 내용이 같아 무해하지만,
    # **labels 부착(A2)에서는 두 JSON 이 한 asset 에 이벤트를 겹쳐 쓰게 되므로 치명적**이다.
    alias: dict[str, list[str]] = defaultdict(list)

    for e in entries:
        cands = by_name.get(e["sanitized_basename"], [])
        how = "name"
        if len(cands) != 1:
            cands = by_size.get(e["size"], [])
            how = "size"
        if len(cands) != 1:
            stat["unresolved" if not cands else "ambiguous"] += 1
            if args.verbose:
                print(f"[MISS] {how}={len(cands)}  {e['video'].name}")
            continue

        rec = cands[0]
        stat[f"resolved_by_{how}"] += 1
        alias[rec["asset_id"]].append(e["video"].name)

        # 이름으로 잡혔더라도 크기를 반드시 교차확인한다 — 살균은 many-to-one 이다
        if how == "name" and rec["file_size"] != e["size"]:
            size_mismatch.append(f"{e['video'].name}: db={rec['file_size']} fs={e['size']}")

        if args.verify_checksum:
            from vlm_pipeline.lib.checksum import sha256sum
            if sha256sum(e["video"]) != rec["checksum"]:
                stat["checksum_mismatch"] += 1
                print(f"[CHECKSUM] 불일치 {e['video'].name}")
                continue

        new_path = resolve_new_archive_path(e["video"], e["origin"], master)
        if new_path is None:
            stat["no_existing_file"] += 1
            continue
        if str(new_path) == str(rec["archive_path"]):
            stat["already_ok"] += 1
            continue
        updates.append((rec["asset_id"], str(new_path)))
        if args.verbose:
            print(f"[FIX ] {rec['raw_key']}\n       {rec['archive_path']}\n    -> {new_path}")

    aliased = {a: names for a, names in alias.items() if len(names) > 1}

    print("\n=== 요약 ===")
    for k in sorted(stat):
        print(f"  {k:24s} {stat[k]}")
    print(f"  {'distinct_assets':24s} {len(alias)}")
    print(f"  {'needs_update':24s} {len(updates)}")
    if aliased:
        dup_files = sum(len(v) - 1 for v in aliased.values())
        print(
            f"\n[ALIAS] 아카이브 파일 여러 개가 같은 asset 으로 해소된다: "
            f"asset {len(aliased)}개 / 잉여 파일 {dup_files}개"
        )
        print("  archive_path 는 내용이 같아 무해하지만, **labels 부착(A2)에서는**")
        print("  두 JSON 이 한 asset 에 이벤트를 겹쳐 쓰므로 반드시 한쪽을 골라야 한다.")
        for a, names in list(aliased.items())[:5]:
            print(f"    {a[:8]}… ← {names}")
    if size_mismatch:
        print(f"\n[WARN] 이름은 맞았는데 크기가 다른 {len(size_mismatch)}건 — 오매칭 의심:")
        for m in size_mismatch[:10]:
            print("   ", m)

    if not args.apply:
        print("\n[DRY-RUN] --apply 를 주면 위 needs_update 건을 UPDATE 한다.")
        return

    with db.connect() as conn, conn.cursor() as cur:
        for asset_id, new_path in updates:
            cur.execute("UPDATE raw_files SET archive_path = %s WHERE asset_id = %s", (new_path, asset_id))
        conn.commit()
    print(f"\n[APPLIED] {len(updates)}행 UPDATE 완료")


if __name__ == "__main__":
    main()
