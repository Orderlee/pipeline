"""`captions` 데이터셋의 회색 플레이스홀더를 실제 키프레임으로 교체 — 배치·저부하·재개 가능.

## 왜 필요한가 (2026-07-28 실측)

`captions` 11,978건 중 **11,535건(96.3%)** 이 320×240 짙은 회색 플레이스홀더
(`fiftyone_pgvector.py:576` — 키프레임 없어도 UMAP 점은 찍히게 하려는 의도).
실제 사진이 있는 건 443건이고 그마저 **원본 영상 11개**에서 나온 것이다.

원인: 캡션 샘플의 이미지는 `image_metadata`(추출된 프레임)에서 온다. 그런데 프레임 추출
대상(102,074 asset)과 Gemini 캡션 대상(4,235 asset)이 거의 겹치지 않는다(교집합 481).
`frames_full` 을 빌드해도 이 조회는 `image_metadata` 를 직접 읽으므로 해결되지 않는다.

이건 그림 보기 문제만이 아니다. `caption_img_sim`(캡션↔이미지 유사도)이 330건만 채워져
있는데, 이게 **caption 임베딩이 유의미한지 보는 가장 직접적인 지표**다. 실제 이미지가
없으면 측정 자체가 불가능하다.

## 방법

`raw_files` 의 원본 영상(4,235 asset 전부 `raw_key` 보유)에서 프레임 1장씩 뽑는다.
`/nas` 가 이 컨테이너에 마운트돼 있지 않으므로 **MinIO presigned URL 을 ffmpeg 에 직접**
물린다 — HTTP range 로 앞부분만 읽어 영상 전체를 내려받지 않는다 (실측 0.3s / 123KB).

## 자원 예의 (호스트는 prod 파이프라인·타 사용자와 공유)

  - asset 배치 단위 처리, 배치마다 `MemAvailable` 확인 → 하한 밑이면 대기, 계속 낮으면 중단
  - `CKF_WORKERS` 기본 3 — MinIO 는 prod ingest 와 같은 NAS 박스 (2026-07-02 IO 포화 이력)
  - ffmpeg `-threads 1`, `os.nice` 로 우선순위 양보
  - 추출 결과를 asset 별로 캐시 → 중단 후 재실행 시 이미 뽑은 건 건너뜀

env:
  CKF_BATCH        asset 배치 크기      기본 200
  CKF_WORKERS      ffmpeg 병렬          기본 3
  CKF_LIMIT        처리할 asset 수 상한 기본 0(=전체)
  CKF_SEEK         seek 위치(초)        기본 1
  CKF_MIN_AVAIL_MB 메모리 하한(MB)      기본 3000
  CKF_NICE         nice 값              기본 10
  CKF_DATASET      기본 'captions'
  CKF_DRY_RUN      1=추출만 하고 DB 미변경
"""

import os
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fiftyone as fo

import fiftyone_pgvector as fp

BATCH = int(os.getenv("CKF_BATCH", "200"))
WORKERS = int(os.getenv("CKF_WORKERS", "3"))
LIMIT = int(os.getenv("CKF_LIMIT", "0"))
SEEK = os.getenv("CKF_SEEK", "1")
MIN_AVAIL_MB = int(os.getenv("CKF_MIN_AVAIL_MB", "3000"))
NICE = int(os.getenv("CKF_NICE", "10"))
DATASET = os.getenv("CKF_DATASET", "captions")
DRY_RUN = os.getenv("CKF_DRY_RUN", "0").strip() in ("1", "true", "yes")

T0 = time.time()


def log(msg):
    print(f"[ckf +{time.time() - T0:6.0f}s] {msg}", flush=True)


try:
    os.nice(NICE)
except Exception as exc:  # noqa: BLE001
    log(f"nice({NICE}) 실패: {exc!r}")


def mem_avail_mb() -> int:
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) // 1024
    except Exception:  # noqa: BLE001 — 못 읽으면 가드 비활성
        pass
    return 1 << 30


def wait_for_memory(tries: int = 20, sleep_s: int = 15):
    for i in range(tries):
        if mem_avail_mb() >= MIN_AVAIL_MB:
            return
        log(f"  ⏸ MemAvailable={mem_avail_mb()}MB < {MIN_AVAIL_MB}MB — 대기 {i + 1}/{tries}")
        time.sleep(sleep_s)
    raise RuntimeError(f"MemAvailable 이 {MIN_AVAIL_MB}MB 밑에 머묾 — 중단 (재실행하면 이어서 진행)")


def batches(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


ds = fo.load_dataset(DATASET)
todo = ds.match({"has_keyframe": False})
sids, asset_ids, filepaths = todo.values(["id", "asset_id", "filepath"])
log(f"{DATASET}: 플레이스홀더 {len(sids)}건")

# asset → [(sample_id, filepath), ...] — 한 영상에서 1프레임만 뽑아 여러 캡션에 재사용
by_asset: dict[str, list[tuple[str, str]]] = defaultdict(list)
for sid, aid, fpth in zip(sids, asset_ids, filepaths):
    if aid and fpth:
        by_asset[str(aid)].append((sid, fpth))

assets = sorted(by_asset)
if LIMIT:
    assets = assets[:LIMIT]
log(f"대상 asset {len(assets)}개 (캡션 {sum(len(by_asset[a]) for a in assets)}건)")

# 추출 캐시 — 재실행 시 건너뛰기용
CACHE = os.path.join(fp.MEDIA_DIR, "captions", "_keyframes")
os.makedirs(CACHE, exist_ok=True)
mc = fp._minio_client()


def fetch_sources(asset_batch: list[str]) -> dict[str, tuple[str, str, str]]:
    """asset_id → (bucket, key, media_type)"""
    out: dict[str, tuple[str, str, str]] = {}
    with fp._pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT asset_id, raw_bucket, raw_key, media_type FROM raw_files "
            "WHERE asset_id = ANY(%(ids)s)",
            {"ids": asset_batch},
        )
        for aid, bucket, key, mtype in cur.fetchall():
            if bucket and key:
                out[str(aid)] = (bucket, key, mtype or "video")
    return out


def extract(args) -> tuple[str, str | None]:
    """asset 하나에서 프레임 1장. 이미 캐시에 있으면 재사용."""
    aid, (bucket, key, mtype) = args
    dst = os.path.join(CACHE, f"{aid}.jpg")
    if os.path.exists(dst) and os.path.getsize(dst) > 0:
        return aid, dst
    try:
        if mtype == "image":  # 이미지 asset 이면 그냥 내려받는다
            mc.download_file(bucket, key, dst)
            return aid, dst
        url = fp.presigned_url(bucket, key, expires=1800)
        # -ss 를 -i 앞에 두면 input seek (빠름). HTTP range 로 앞부분만 읽는다.
        r = subprocess.run(
            ["ffmpeg", "-nostdin", "-loglevel", "error", "-threads", "1",
             "-ss", SEEK, "-i", url, "-frames:v", "1", "-q:v", "3", "-y", dst],
            capture_output=True,
            timeout=120,
        )
        if r.returncode != 0 or not os.path.exists(dst) or os.path.getsize(dst) == 0:
            # 영상이 SEEK 보다 짧을 수 있다 → 0초에서 재시도
            r = subprocess.run(
                ["ffmpeg", "-nostdin", "-loglevel", "error", "-threads", "1",
                 "-i", url, "-frames:v", "1", "-q:v", "3", "-y", dst],
                capture_output=True,
                timeout=120,
            )
        if r.returncode == 0 and os.path.exists(dst) and os.path.getsize(dst) > 0:
            return aid, dst
        return aid, None
    except Exception:  # noqa: BLE001 — per-asset fail-forward
        return aid, None


ok_assets = failed_assets = 0
updated_samples = 0
try:
    for bi, asset_batch in enumerate(batches(assets, BATCH), 1):
        wait_for_memory()
        sources = fetch_sources(asset_batch)
        work = [(a, sources[a]) for a in asset_batch if a in sources]
        missing_src = len(asset_batch) - len(work)

        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            results = list(ex.map(extract, work))

        # 새 filepath 로 복사 — 같은 경로에 덮어쓰면 브라우저/앱이 옛 플레이스홀더를
        # 캐시해 그대로 보일 수 있다. 경로를 바꾸면 그 문제가 없다.
        fp_updates: dict[str, str] = {}
        hk_updates: dict[str, bool] = {}
        for aid, src in results:
            if not src:
                failed_assets += 1
                continue
            ok_assets += 1
            for sid, old in by_asset[aid]:
                stem, ext = os.path.splitext(old)
                new = f"{stem}_kf{ext or '.jpg'}"
                try:
                    if not os.path.exists(new) or os.path.getsize(new) == 0:
                        shutil.copyfile(src, new)
                    fp_updates[sid] = new
                    hk_updates[sid] = True
                except Exception as exc:  # noqa: BLE001 — per-sample fail-forward
                    log(f"  copy 실패 sid={sid}: {exc!r}")

        if fp_updates and not DRY_RUN:
            ds.set_values("filepath", fp_updates, key_field="id")
            ds.set_values("has_keyframe", hk_updates, key_field="id")
            try:  # 플레이스홀더는 320×240 이었으므로 metadata 재계산 필요
                # num_workers 를 명시하지 않으면 cpu_count 기준으로 워커를 띄운다
                # (실측 56 스레드). IO 대기라 CPU 는 0.1코어였지만 코어 많은 호스트에서
                # 과다 생성되지 않게 고정한다.
                ds.select(list(fp_updates), ordered=False).compute_metadata(
                    overwrite=True, progress=False, num_workers=WORKERS
                )
            except Exception as exc:  # noqa: BLE001 — metadata 실패해도 이미지는 보인다
                log(f"  metadata 재계산 skip: {exc!r}")
            updated_samples += len(fp_updates)

        log(
            f"batch {bi}/{(len(assets) + BATCH - 1) // BATCH} "
            f"asset ok={ok_assets} fail={failed_assets} src없음={missing_src} "
            f"샘플갱신={updated_samples} avail={mem_avail_mb()}MB"
        )
except RuntimeError as exc:
    log(f"⚠️ {exc}")
    raise SystemExit(2) from exc

log(f"DONE asset ok={ok_assets} fail={failed_assets} 샘플갱신={updated_samples} dry_run={DRY_RUN}")
if not DRY_RUN:
    left = ds.match({"has_keyframe": False}).count()
    log(f"남은 플레이스홀더: {left} / 전체 {ds.count()}")
