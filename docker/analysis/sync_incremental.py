#!/usr/bin/env python3
"""FiftyOne `frames` / `frames-prompts` 동기화 CLI — analysis-sync API 가 subprocess 로 부른다.

호출부(sync_api.py)가 이 스크립트를 프로세스로 띄우는 이유는 **종료 시 RSS 반환**이다
(호스트 RAM 62.5G 공유·oom_kill 이력 — project_host_memory_profile 참고). 컨테이너 안에서
직접 실행도 가능:
    python3 sync_incremental.py <frames|labels|prompts> [--dry-run]

stdout **마지막 줄**에 결과 JSON 1줄 + `/data/fiftyone/_sync/last_<target>.json` 파일에도
기록한다(계약: sync_api.py 가 이 마지막 줄을 파싱해 `result` 로 보관). 그 전의 모든 로그는
`[sync +NNs] ...` 접두라 JSON 파서가 마지막 줄만 보면 된다.

결과 JSON shape (전 target 공통):
    {"target": str, "dry_run": bool, "added": int, "refreshed": int,
     "remaining": int, "warnings": [str, ...]}

## target 별 동작

frames  — PG `image_embeddings(entity_type='frame')` 신규 `entity_id` 만 자기치유형
          set-diff 로 add 한다. 커서를 두지 않고 매 호출마다 기존 `entity_id` 전체와
          비교한다 — 멱등이고, 어떤 이유로든 누락된 행이 있으면 다음 호출이 스스로
          채운다("자기치유"). 신규 표본에만 `attach_project`/`attach_labels_batched` 를
          적용한다(전체 200K 를 매 호출마다 재적재하지 않음).
labels  — 기존 `frames` 프레임 표본에 SAM3 라벨/캡션/환경을 배치로 재적재한다. 죽어있던
          2h cron(`refresh_frames_labels.py`)의 배치+병렬 버전 대체.
prompts — `frames-prompts` 를 `prompt_geometry.py promptmap --profile frames` 로 전량
          재빌드한다(설계상 이 데이터셋은 "증분"이 아니라 "정본이 재작성"되는 산출물이다 —
          `stage_promptmap()` 자체가 `fo.Dataset(name, overwrite=True, ...)` 를 쓴다).

## 공통 안전장치
  - 메모리 가드: `fiftyone_full_build.py:118-143` 의 idiom(MemAvailable 하한, 대기 후
    바닥나면 `MemoryFloor`)을 재사용. frames/labels 는 이 예외를 **잡아서** 부분 진행을
    정상 결과로 보고한다(호출자가 재호출하면 이어진다) — 프로세스를 죽이지 않는다.
  - `list(ds)` / `ds.values("embedding")` 전량 로드 금지(행당 ~32KB). 배치·keyset
    페이징만 사용.
  - `fo.delete_dataset` / `Dataset(overwrite=True)` 를 `frames` 에 쓰지 않는다
    (`prompts` 타깃의 `promptmap` 서브프로세스 내부 동작은 그 데이터셋 자체가 재작성
    산출물이라 예외).
  - gidx 는 등식 조인에 쓰지 않는다(`% GIDX_OFFSET` 계약, 이 스크립트는 gidx 를 다루지
    않지만 prompts 서브프로세스가 그 계약을 따르는 코드를 실행한다).

env:
  DATAOPS_POSTGRES_DSN / MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY
                              — fiftyone_pgvector 공용 연결 정보
  SYNC_MIN_AVAIL_MB           메모리 하한(MB)                          기본 2000
  FIFTYONE_SYNC_MAX_ADD       frames 1회 호출당 최대 add 행 수          기본 50000
  SYNC_PAGE_SIZE              frames PG keyset 페이지 크기             기본 500
  RFL_CHUNK                   labels id 배치 크기(refresh_frames_labels.py 와 동일 이름) 기본 5000
  SYNC_PROMPT_NPZ_DIR         prompts BANK_LIST 를 구성할 뱅크 npz 폴더 기본 /data/fiftyone/sourceh/prompts
  SYNC_PROMPTS_TIMEOUT_S      prompts promptmap subprocess 타임아웃(초) 기본 7200
"""

from __future__ import annotations

import argparse
import gc
import glob
import json
import os
import subprocess
import sys
import time

SYNC_STATE_DIR = "/data/fiftyone/_sync"
MIN_AVAIL_MB = int(os.environ.get("SYNC_MIN_AVAIL_MB", "2000"))
MAX_ADD = int(os.environ.get("FIFTYONE_SYNC_MAX_ADD", "50000"))
PAGE_SIZE = int(os.environ.get("SYNC_PAGE_SIZE", "500"))
LABELS_CHUNK = int(os.environ.get("RFL_CHUNK", "5000"))
PROMPT_NPZ_DIR = os.environ.get("SYNC_PROMPT_NPZ_DIR", "/data/fiftyone/sourceh/prompts")
PROMPTS_TIMEOUT_S = int(os.environ.get("SYNC_PROMPTS_TIMEOUT_S", "7200"))
TAIL_LINES = 20

T0 = time.time()


def log(msg: str) -> None:
    print(f"[sync +{time.time() - T0:6.0f}s] {msg}", flush=True)


# ── 메모리 가드 (fiftyone_full_build.py:118-143 idiom 재사용) ─────────────────
class MemoryFloor(RuntimeError):
    """가용 메모리가 하한 밑에 머물러 안전하게 중단 — 부분 진행은 정상(재호출로 이어감)."""


def mem_avail_mb() -> int:
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) // 1024
    except Exception:  # noqa: BLE001 — 못 읽으면 가드 비활성(무한대 취급)
        pass
    return 1 << 30


def wait_for_memory(tries: int = 20, sleep_s: int = 15) -> None:
    for i in range(tries):
        avail = mem_avail_mb()
        if avail >= MIN_AVAIL_MB:
            return
        gc.collect()
        log(f"  ⏸ MemAvailable={avail}MB < {MIN_AVAIL_MB}MB — 대기 {i + 1}/{tries}")
        time.sleep(sleep_s)
    raise MemoryFloor(f"MemAvailable 이 {MIN_AVAIL_MB}MB 밑에 머묾 — 중단(재호출로 이어감)")


def _write_result(target: str, result: dict) -> None:
    os.makedirs(SYNC_STATE_DIR, exist_ok=True)
    path = os.path.join(SYNC_STATE_DIR, f"last_{target}.json")
    try:
        with open(path, "w") as fh:
            json.dump(result, fh)
    except Exception as exc:  # noqa: BLE001 — 상태 파일 기록 실패가 sync 결과를 무효화하면 안 됨
        log(f"상태 파일 기록 실패({path}): {exc!r}")


def _emit(result: dict) -> None:
    """stdout 마지막 줄 1줄 JSON — sync_api.py 계약."""
    print(json.dumps(result), flush=True)


# ────────────────────── frames ──────────────────────
# INNER JOIN: image_metadata 없는 frame 임베딩 행은 이 스크립트에 아예 안 보인다. 임베딩
# 파이프라인이 image_metadata 를 소스로 임베딩을 만들므로 고아 행은 구조상 없다(2026-08-21
# 실측 조인율 188,190/188,190 = 100%). 이 불변식이 깨지면 해당 행은 remaining 에도 안 잡힌다.
_PAGE_SQL = """
    SELECT e.entity_id, e.image_id, im.image_bucket, im.image_key, im.source_asset_id, e.embedding
    FROM image_embeddings e
    JOIN image_metadata im ON im.image_id = e.image_id
    WHERE e.entity_type = 'frame' AND e.model_name = %(model)s
      AND e.entity_id > %(after)s
    ORDER BY e.entity_id
    LIMIT %(size)s
"""


def _load_page(fp, after: str, size: int, model_name: str) -> list[dict]:
    """fiftyone_full_build.py:152-178 의 keyset 페이징 이식(OFFSET 금지 — 200K 에서 O(n²) 회피)."""
    out: list[dict] = []
    with fp._pg_conn() as conn, conn.cursor() as cur:
        cur.execute(_PAGE_SQL, {"model": model_name, "after": after, "size": size})
        for entity_id, image_id, bucket, key, asset_id, emb in cur.fetchall():
            out.append(
                {
                    "entity_id": entity_id,
                    "image_id": image_id,
                    "bucket": bucket,
                    "key": key,
                    "asset_id": asset_id,
                    "embedding": fp._parse_vector(emb),
                }
            )
    return out


def _require_minio(fp):
    """MinIO 도달성 사전 점검 — 전면 불통이면 크게 실패한다.

    불통인 채 labels 재적재를 돌리면 attach 가 per-file fail-forward 로 detections 를 전부
    못 읽어 detection_class 를 일괄 "none" 으로 덮어쓴다(조용한 wipe — 2026-08-21 NAS 박스
    다운 중 실측으로 확인한 시나리오). frames add 도 전 건 다운로드 실패라 의미가 없다.
    """
    mc = fp._minio_client()
    try:
        mc.head_bucket(Bucket="vlm-labels")
    except Exception as exc:  # noqa: BLE001 — 원인 불문 전면 불통으로 취급
        raise RuntimeError(f"MinIO 불통({os.environ.get('MINIO_ENDPOINT')}) — sync 중단: {exc!r}") from exc
    return mc


def _count_caption_embeddings(fp, model_name: str) -> int | None:
    try:
        with fp._pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM image_embeddings WHERE entity_type = 'caption' AND model_name = %(model)s",
                {"model": model_name},
            )
            row = cur.fetchone()
        return int(row[0]) if row else None
    except Exception as exc:  # noqa: BLE001 — 캡션 델타 감지는 best-effort
        log(f"caption count query skipped: {exc!r}")
        return None


def _attach_new_frame_metadata(fp, ds, new_ids: list[str]) -> None:
    """신규 프레임 id 부분집합에 `project` 필드를 채우고, saved view/tag 는 전체 ds 기준으로 재동기화.

    ⚠️ `fiftyone_pgvector.attach_project()` 를 신규 id 만 담은 view 로 바로 호출하지
    않는다 — 그 함수 끝의 `make_project_saved_views()` 는 **인자로 받은 ds 스코프
    안에서만** `proj: <name>` saved view 를 재계산해 저장한다. 신규 id 몇 개만 담긴
    view 를 넘기면 그 작은 부분집합으로 기존(전체 데이터셋 크기) saved view 를 덮어써,
    그 뷰를 쓰는 Embeddings 패널 project subset 이 조용히 망가진다(2026-08 gotchas
    참고). 그래서 `project` 필드 계산만 신규 id 로 좁히고, saved view 재계산은 전체
    `ds` 로 넘긴다(문자열 집계라 가볍다).
    """
    view = ds.select(new_ids)
    sids = view.values("id")
    image_ids = view.values("image_id")
    minio_keys = view.values("minio_key")
    image_keys = fp._fetch_image_keys([i for i in image_ids if i])
    proj_by_sid: dict[str, str] = {}
    for sid, iid, mk in zip(sids, image_ids, minio_keys):
        proj = fp._project_of(image_keys.get(str(iid))) if iid else "none"
        if proj == "none" and mk:
            parts = str(mk).split("/")
            if len(parts) >= 2 and parts[1]:
                proj = parts[1]
        proj_by_sid[sid] = proj
    ds.set_values("project", proj_by_sid, key_field="id")
    fp.make_project_saved_views(ds)


def sync_frames(dry_run: bool) -> dict:
    import fiftyone as fo
    import fiftyone_pgvector as fp
    from concurrent.futures import ThreadPoolExecutor

    warnings: list[str] = []
    ds = fo.load_dataset("frames")
    existing = {str(e) for e in ds.values("entity_id") if e}
    model_name = fp._active_model_name()
    log(
        f"frames: 기존 entity_id={len(existing):,} model={model_name} "
        f"dry_run={dry_run} max_add={MAX_ADD} page={PAGE_SIZE}"
    )

    mc = _require_minio(fp) if not dry_run else None
    media_dir = fp.MEDIA_DIR
    if not dry_run:
        os.makedirs(media_dir, exist_ok=True)

    def fetch_media(r):
        ext = os.path.splitext(r["key"])[1] or ".jpg"
        lp = os.path.join(media_dir, f"{r['image_id']}{ext}")
        if not os.path.exists(lp):
            try:
                mc.download_file(r["bucket"], r["key"], lp)
            except Exception:  # noqa: BLE001 — 객체 누락 등은 skip (per-file fail-forward)
                return None
        return lp

    after = ""
    added = 0
    fresh_total = 0
    total_scanned = 0
    missing = 0
    new_ids: list[str] = []
    floor_hit = False
    while True:
        try:
            wait_for_memory()
        except MemoryFloor as exc:
            log(f"⚠️ {exc} — frames 스캔 중단(부분 진행 유지)")
            warnings.append(f"메모리 하한 도달로 조기 중단 — {exc}")
            floor_hit = True
            break
        page = _load_page(fp, after, PAGE_SIZE, model_name)
        if not page:
            break
        after = page[-1]["entity_id"]
        total_scanned += len(page)
        fresh = [r for r in page if str(r["entity_id"]) not in existing]
        fresh_total += len(fresh)
        del page

        if not dry_run and fresh and added < MAX_ADD:
            take = fresh[: MAX_ADD - added]
            with ThreadPoolExecutor(max_workers=6) as ex:
                paths = list(ex.map(fetch_media, take))
            samples = []
            for r, lp in zip(take, paths):
                if not lp:
                    missing += 1
                    continue
                s = fo.Sample(filepath=lp)
                s["image_id"] = r["image_id"]
                s["entity_id"] = r["entity_id"]
                s["embedding"] = r["embedding"]
                s["minio_key"] = f"{r['bucket']}/{r['key']}"
                s["modality"] = "frame"  # frames 정본은 혼합 모달리티(프레임+캡션)
                aid = r.get("asset_id")
                if aid:
                    s["asset_id"] = str(aid)
                samples.append(s)
                existing.add(str(r["entity_id"]))
            if samples:
                new_ids.extend(str(i) for i in ds.add_samples(samples, progress=False))
                added += len(samples)
            del samples, paths
        del fresh
        gc.collect()
        if total_scanned % (PAGE_SIZE * 20) < PAGE_SIZE:
            log(f"  scanned total={total_scanned:,} fresh={fresh_total:,} added={added:,} avail={mem_avail_mb()}MB")

    # remaining = "재호출이 진전시킬 수 있는 양" — 영구 누락 미디어(missing)를 포함하면
    # 호출자(dagster op)가 remaining>0 을 보고 무한 재-POST 한다. missing 은 warnings 로만.
    remaining = max(0, fresh_total - added - missing) if not dry_run else max(0, fresh_total - added)
    if floor_hit:
        # 스캔이 중간에 끊겼으면 못 본 꼬리 크기를 모른다 — remaining=0 으로 "완료" 위장 금지.
        remaining = max(remaining, 1)
    log(f"frames: scan 완료 fresh={fresh_total:,} added={added:,} missing={missing} remaining={remaining}")

    if new_ids:
        try:
            _attach_new_frame_metadata(fp, ds, new_ids)
            log(f"attach_project(신규 {len(new_ids)}건) 완료")
        except Exception as exc:  # noqa: BLE001 — 표시층 실패가 add 결과를 무효화하면 안 됨
            log(f"attach_project(신규분) skipped: {exc!r}")
        try:
            fp.attach_labels_batched(ds.select(new_ids), mc, chunk=min(LABELS_CHUNK, len(new_ids)), log=log)
        except Exception as exc:  # noqa: BLE001
            log(f"attach_labels_batched(신규분) skipped: {exc!r}")

    if added > 0:
        warnings.append(
            "신규 샘플 emb_viz(UMAP) 좌표 없음 — recompute_viz.py 재실행 필요"
            " (RV_DATASET=frames python3 recompute_viz.py)"
        )
    if dry_run:
        if fresh_total > 0:
            warnings.append(f"dry-run: fresh {fresh_total:,}건 감지(add 는 실행하지 않음)")
    elif fresh_total > MAX_ADD:
        warnings.append(f"FIFTYONE_SYNC_MAX_ADD={MAX_ADD} 초과 — {remaining:,}건 남음(다음 호출에서 이어서 처리)")
    elif not floor_hit and remaining > 0:
        warnings.append(f"{remaining:,}건 미처리 — 다음 호출에서 자기치유(재스캔이라 커서 불필요)")
    if missing:
        warnings.append(f"미디어 다운로드 실패 {missing}건 — 이번 호출에서 미add, 다음 호출에서 재시도")

    # caption 델타 감지 (item 7) — 구현은 범위 밖, 조용히 넘기지 않고 경고만 남긴다.
    try:
        cap_db = _count_caption_embeddings(fp, model_name)
        sch = ds.get_field_schema()
        cap_ds = None
        if "modality" in sch:
            from fiftyone import ViewField as F

            cap_ds = ds.match(F("modality") == "caption").count()
        if cap_db is not None and cap_ds is not None and cap_db > cap_ds:
            warnings.append(
                f"caption 증분 add 미구현 — PG caption 임베딩 {cap_db:,} > " f"frames modality=caption {cap_ds:,}"
            )
    except Exception as exc:  # noqa: BLE001 — 델타 감지 실패가 sync 결과를 깨뜨리면 안 됨
        log(f"caption 델타 감지 skipped: {exc!r}")

    return {
        "target": "frames",
        "dry_run": bool(dry_run),
        "added": added,
        "refreshed": 0,
        "remaining": remaining,
        "warnings": warnings,
    }


# ────────────────────── labels ──────────────────────
def sync_labels(dry_run: bool) -> dict:
    import fiftyone as fo
    from fiftyone import ViewField as F
    import fiftyone_pgvector as fp

    warnings: list[str] = []
    ds = fo.load_dataset("frames")
    if "modality" in ds.get_field_schema():
        target = ds.match(F("modality") == "frame")
    else:
        target = ds
    ids = target.values("id")
    log(f"labels: target frames={len(ids):,} / dataset={ds.count():,} chunk={LABELS_CHUNK} dry_run={dry_run}")

    if dry_run:
        return {
            "target": "labels",
            "dry_run": True,
            "added": 0,
            "refreshed": 0,
            "remaining": len(ids),
            "warnings": warnings,
        }

    mc = _require_minio(fp)
    done = 0
    for i in range(0, len(ids), LABELS_CHUNK):
        try:
            wait_for_memory()
        except MemoryFloor as exc:
            log(f"⚠️ {exc} — labels 재적재 중단(부분 진행 유지)")
            warnings.append(f"메모리 하한 도달로 조기 중단 — {exc}")
            break
        batch = ids[i : i + LABELS_CHUNK]
        fp.attach_labels_batched(ds.select(batch), mc, chunk=LABELS_CHUNK, log=log)
        done += len(batch)
        log(f"  labels {done:,}/{len(ids):,}")

    remaining = max(0, len(ids) - done)
    if remaining:
        warnings.append(f"{remaining}건 미처리 — 다음 호출에서 이어서 처리(커서 없이 전량 순회라 안전)")
    return {
        "target": "labels",
        "dry_run": False,
        "added": 0,
        "refreshed": done,
        "remaining": remaining,
        "warnings": warnings,
    }


# ────────────────────── prompts ──────────────────────
def _bank_list_from_npz(npz_dir: str) -> list[str]:
    """rebuild_banks_all.py:20-23 과 동일한 구성 — npz glob → semantic sort."""
    npzs = glob.glob(os.path.join(npz_dir, "v*.npz"))
    versions = [os.path.basename(p)[:-4] for p in npzs]
    versions.sort(key=lambda v: tuple(int(x) for x in v.lstrip("v").split(".")))
    return versions


def sync_prompts(dry_run: bool) -> dict:
    warnings: list[str] = []
    versions = _bank_list_from_npz(PROMPT_NPZ_DIR)
    bank_list = ",".join(versions)
    cmd = [sys.executable, "/workspace/prompt_geometry.py", "promptmap", "--profile", "frames"]
    log(f"prompts: {len(versions)} bank npz under {PROMPT_NPZ_DIR}")

    if not versions:
        warnings.append(f"뱅크 npz 를 찾지 못함: {PROMPT_NPZ_DIR}/v*.npz — promptmap 실행 안 함")

    if dry_run or not versions:
        warnings.append(f"BANK_LIST={bank_list or '(empty)'}")
        warnings.append(f"cmd={' '.join(cmd)}")
        return {
            "target": "prompts",
            "dry_run": bool(dry_run),
            "added": 0,
            "refreshed": 0,
            "remaining": len(versions),
            "warnings": warnings,
        }

    env = dict(os.environ)
    env["BANK_LIST"] = bank_list
    log(f"prompts: 실행 {' '.join(cmd)} (BANK_LIST={len(versions)}버전, timeout={PROMPTS_TIMEOUT_S}s)")
    try:
        proc = subprocess.run(
            cmd,
            env=env,
            cwd="/workspace",
            capture_output=True,
            text=True,
            timeout=PROMPTS_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as exc:
        # 실패를 remaining>0 인 "성공" 으로 보고하면 호출자(dagster op)가 2시간짜리 실패
        # subprocess 를 무한 재시도한다 — 예외로 올려 CLI exit 1 → state=failed 로 만든다.
        log(f"prompts: timeout: {exc!r}")
        raise RuntimeError(f"promptmap subprocess timeout({PROMPTS_TIMEOUT_S}s)") from exc

    if proc.returncode != 0:
        tail = (proc.stdout or "").splitlines()[-TAIL_LINES:]
        err_tail = (proc.stderr or "").splitlines()[-TAIL_LINES:]
        log(f"prompts: 실패 rc={proc.returncode}")
        raise RuntimeError(f"promptmap subprocess 실패 rc={proc.returncode} stdout_tail={tail} stderr_tail={err_tail}")

    log("prompts: promptmap 완료")
    return {
        "target": "prompts",
        "dry_run": False,
        "added": 0,
        "refreshed": len(versions),
        "remaining": 0,
        "warnings": warnings,
    }


# ────────────────────── main ──────────────────────
_DISPATCH = {"frames": sync_frames, "labels": sync_labels, "prompts": sync_prompts}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("target", choices=sorted(_DISPATCH))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    try:
        result = _DISPATCH[args.target](args.dry_run)
    except Exception as exc:  # noqa: BLE001 — 예외도 계약된 JSON 한 줄로 낸다(호출자 파싱 보장)
        log(f"sync 실패: {exc!r}")
        result = {
            "target": args.target,
            "dry_run": bool(args.dry_run),
            "added": 0,
            "refreshed": 0,
            "remaining": -1,
            "warnings": [f"{type(exc).__name__}: {exc}"],
        }
        _write_result(args.target, result)
        _emit(result)
        return 1

    _write_result(args.target, result)
    _emit(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
