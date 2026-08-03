"""FiftyOne 'frames' 전체(188K) 배치 빌드 — 메모리/CPU 상한 준수 + 중단 후 재개 가능.

## 왜 배치인가 (2026-07-28 실측)

이전 버전은 `load_frame_embeddings(limit=None)` 로 188K 행을 **한 번에** 올렸다.
임베딩이 `list[float]` 1024-d 라 행당 ~32KB → **12GB**. 호스트(62GB)에 이미 타 프로세스가
46GB 를 쓰고 있어 가용이 1GB 밑으로 떨어졌고, add 단계 속도가 404→0.1 samples/s 로
붕괴했다(스왑 경합). 5단계의 `ds.values("embedding")` 가 또 12GB 를 요구해 완주 불가.

이 버전은 **어느 단계에서도 CHUNK 개 이상의 임베딩을 메모리에 두지 않는다**:
  - DB 읽기: `entity_id` keyset 페이징 (OFFSET 아님 — 188K 에서 O(n²) 회피)
  - 라벨: id 배치별로 조회·MinIO 읽기·set_values
  - UMAP: FIT 샘플만 fit → 배치 transform
  - PCA: IncrementalPCA partial_fit (전체 로드 불가)

## 자원 예의
  - BLAS/OpenMP 스레드 캡 (numpy import 전 설정 — 대시보드와 동일 패턴, commit eb19746)
  - `os.nice` 로 우선순위 양보 → 다른 작업이 CPU 를 먼저 가져간다
  - 배치마다 MemAvailable 확인. 하한 밑이면 **대기**, 계속 낮으면 깨끗하게 중단(재개 가능)
  - 미디어/MinIO 병렬도를 낮게 (NAS 는 prod ingest 와 공유 — 2026-07-02 IO 포화 이력)

## 재개
`FFB_RESUME=1`(기본) 이면 기존 데이터셋을 지우지 않고 **없는 entity_id 만** 추가한다.
중단돼도 다시 실행하면 이어서 진행된다. 처음부터 다시 하려면 `FFB_RESUME=0`.

env:
  FFB_LIMIT        정수 / all|0|none      기본 all
  FFB_DATASET      기본 'frames'
  FFB_CHUNK        배치 크기              기본 2000
  FFB_WORKERS      미디어/JSON 병렬       기본 6
  FFB_FIT          UMAP fit 샘플 수       기본 30000
  FFB_TBATCH       UMAP transform 배치    기본 10000
  FFB_MIN_AVAIL_MB 메모리 하한(MB)        기본 4000
  FFB_MAX_THREADS  BLAS 스레드 캡         기본 코어/4
  FFB_NICE         nice 값                기본 10
  FFB_RESUME       1=이어서, 0=처음부터   기본 1
  FFB_LAUNCH       1=앱 기동+keepalive    기본 0 (빌드만 — 기존 앱 건드리지 않음)
  FFB_TEXT_SEARCH  1=텍스트검색 인덱스    기본 0 (무거움, 별도 실행 권장)
"""

import os

# ── 스레드 캡: numpy/BLAS 로드 **전에** 설정해야 유효 ──────────────────────────
_MAX_THREADS = int(os.environ.get("FFB_MAX_THREADS", str(max(1, (os.cpu_count() or 4) // 4))))
for _v in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "NUMBA_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
):
    os.environ.setdefault(_v, str(_MAX_THREADS))

import gc
import random
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import numpy as np

import fiftyone as fo
import fiftyone.brain as fob

import fiftyone_pgvector as fp

try:  # 이미 로드된 BLAS 풀에도 적용. 참조 유지 필수 — GC 되면 캡 해제됨.
    import threadpoolctl

    _tp_limiter = threadpoolctl.threadpool_limits(_MAX_THREADS)
except Exception:  # noqa: BLE001 — threadpoolctl 없어도 env 캡은 유효
    _tp_limiter = None

T0 = time.time()


def log(msg):
    print(f"[ffb +{time.time() - T0:6.0f}s] {msg}", flush=True)


_lim = os.getenv("FFB_LIMIT", "all").strip().lower()
LIMIT = None if _lim in ("0", "all", "none", "") else int(_lim)
DATASET = os.getenv("FFB_DATASET", "frames")
CHUNK = int(os.getenv("FFB_CHUNK", "2000"))
WORKERS = int(os.getenv("FFB_WORKERS", "6"))
FIT = int(os.getenv("FFB_FIT", "30000"))
TBATCH = int(os.getenv("FFB_TBATCH", "10000"))
MIN_AVAIL_MB = int(os.getenv("FFB_MIN_AVAIL_MB", "4000"))
NICE = int(os.getenv("FFB_NICE", "10"))
RESUME = os.getenv("FFB_RESUME", "1").strip() not in ("0", "false", "no")
LAUNCH = os.getenv("FFB_LAUNCH", "0").strip() in ("1", "true", "yes")
TEXT_SEARCH = os.getenv("FFB_TEXT_SEARCH", "0").strip() in ("1", "true", "yes")

try:
    os.nice(NICE)
except Exception as exc:  # noqa: BLE001 — nice 실패해도 진행
    log(f"nice({NICE}) 실패: {exc!r}")

log(
    f"start LIMIT={'ALL' if LIMIT is None else LIMIT} DATASET={DATASET} CHUNK={CHUNK} "
    f"WORKERS={WORKERS} FIT={FIT} TBATCH={TBATCH} MIN_AVAIL={MIN_AVAIL_MB}MB "
    f"THREADS={_MAX_THREADS} NICE={NICE} RESUME={RESUME}"
)


# ── 메모리 가드 ────────────────────────────────────────────────────────────────
class MemoryFloor(RuntimeError):
    """가용 메모리가 하한 밑에 머물러 안전하게 중단."""


def mem_avail_mb() -> int:
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) // 1024
    except Exception:  # noqa: BLE001 — 못 읽으면 가드 비활성(무한대 취급)
        pass
    return 1 << 30


def wait_for_memory(tries: int = 20, sleep_s: int = 15):
    """하한 밑이면 기다린다. 계속 낮으면 MemoryFloor — 재개 가능한 상태로 중단."""
    for i in range(tries):
        avail = mem_avail_mb()
        if avail >= MIN_AVAIL_MB:
            return
        gc.collect()
        log(f"  ⏸ MemAvailable={avail}MB < {MIN_AVAIL_MB}MB — 대기 {i + 1}/{tries}")
        time.sleep(sleep_s)
    raise MemoryFloor(f"MemAvailable 이 {MIN_AVAIL_MB}MB 밑에 머묾 — 중단 (FFB_RESUME=1 로 재개)")


def batches(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# ── 1. DB keyset 페이징 (OFFSET 금지 — 188K 에서 O(n²)) ────────────────────────
_PAGE_SQL = """
    SELECT e.entity_id, e.image_id, im.image_bucket, im.image_key, im.source_asset_id, e.embedding
    FROM image_embeddings e
    JOIN image_metadata im ON im.image_id = e.image_id
    WHERE e.entity_type = 'frame' AND e.model_name = %(model)s
      AND e.entity_id > %(after)s
    ORDER BY e.entity_id
    LIMIT %(size)s
"""


def load_page(after: str, size: int, model_name: str = fp.DEFAULT_MODEL) -> list[dict]:
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


# ── 2. 데이터셋 준비 (재개 지원) ───────────────────────────────────────────────
existing: set[str] = set()
if fo.dataset_exists(DATASET):
    if RESUME:
        ds = fo.load_dataset(DATASET)
        existing = {str(e) for e in ds.values("entity_id") if e}
        log(f"resume: 기존 {DATASET} n={ds.count()} (entity_id {len(existing)}개 확보)")
    else:
        log(f"RESUME=0 → 기존 {DATASET} 삭제")
        fo.delete_dataset(DATASET)
        ds = fo.Dataset(DATASET, persistent=True)
else:
    ds = fo.Dataset(DATASET, persistent=True)

mc = fp._minio_client()
MEDIA = fp.MEDIA_DIR
os.makedirs(MEDIA, exist_ok=True)


def fetch_media(r):
    ext = os.path.splitext(r["key"])[1] or ".jpg"
    lp = os.path.join(MEDIA, f"{r['image_id']}{ext}")
    if not os.path.exists(lp):
        try:
            mc.download_file(r["bucket"], r["key"], lp)
        except Exception:  # noqa: BLE001 — 누락/오류 skip (per-file fail-forward)
            return None
    return lp


# ── 3. 페이지 루프: 읽기 → 미디어 → add_samples → 즉시 해제 ────────────────────
after = ""
added = skipped = missing = 0
try:
    while True:
        if LIMIT is not None and added + skipped >= LIMIT:
            break
        wait_for_memory()
        size = CHUNK if LIMIT is None else min(CHUNK, LIMIT - added - skipped)
        page = load_page(after, size)
        if not page:
            break
        after = page[-1]["entity_id"]

        fresh = [r for r in page if str(r["entity_id"]) not in existing]
        skipped += len(page) - len(fresh)
        del page

        if fresh:
            with ThreadPoolExecutor(max_workers=WORKERS) as ex:
                paths = list(ex.map(fetch_media, fresh))
            samples = []
            for r, lp in zip(fresh, paths):
                if not lp:
                    missing += 1
                    continue
                s = fo.Sample(filepath=lp)
                s["image_id"] = r["image_id"]
                s["entity_id"] = r["entity_id"]
                s["embedding"] = r["embedding"]
                s["minio_key"] = f"{r['bucket']}/{r['key']}"
                aid = r.get("asset_id")
                if aid:
                    s["asset_id"] = str(aid)
                samples.append(s)
                existing.add(str(r["entity_id"]))
            if samples:
                ds.add_samples(samples, progress=False)
                added += len(samples)
            del samples, paths
        del fresh
        gc.collect()

        if (added + skipped) % (CHUNK * 10) < CHUNK:
            log(f"  add {added} (skip={skipped} missing={missing}) avail={mem_avail_mb()}MB")
    log(f"add 완료 added={added} skipped={skipped} missing={missing} total={ds.count()}")
except MemoryFloor as exc:
    log(f"⚠️ {exc} — add 단계에서 중단 (n={ds.count()})")
    raise SystemExit(2) from exc


# ── 4. 라벨: id 배치별 조회 + MinIO JSON 병렬 읽기 + 배치 set_values ───────────
def attach_labels_batched():
    all_ids = ds.values("id")
    log(f"labels: {len(all_ids)} samples, 배치 {CHUNK}")
    done = 0
    det_frames = 0
    for id_batch in batches(all_ids, CHUNK):
        wait_for_memory()
        view = ds.select(id_batch, ordered=True)
        sids, image_ids, asset_ids, filepaths = view.values(
            ["id", "image_id", "asset_id", "filepath"]
        )
        iids = [str(i) if i else "" for i in image_ids]

        frame_assets = fp._fetch_frame_asset_ids([i for i in iids if i])
        aids = [
            str(a) if a else str(frame_assets.get(i, "") or "")
            for a, i in zip(asset_ids, iids)
        ]
        caps = fp._fetch_asset_captions([a for a in aids if a])
        envs = fp._fetch_video_env([a for a in aids if a])
        refs = fp._fetch_sam3_label_refs([i for i in iids if i])

        def read_dets(args):
            iid, fpth = args
            dets = []
            for bucket, key in refs.get(iid, []):
                try:
                    payload = fp._read_minio_json(bucket, key, mc=mc)
                    if isinstance(payload, dict):
                        dets.extend(fp._detections_from_coco(payload, fpth))
                except Exception:  # noqa: BLE001 — per-file fail-forward
                    continue
            return dets

        # IO-bound — 낮은 병렬도로 NAS 부담을 줄이면서 순차보다 빠르게
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            det_lists = list(ex.map(read_dets, zip(iids, filepaths)))

        cap_d, dn_d, env_d, dc_d, nm_d, det_d = {}, {}, {}, {}, {}, {}
        for sid, aid, dets in zip(sids, aids, det_lists):
            cap_d[sid] = caps.get(aid, "") if aid else ""
            dn, env = envs.get(aid, (None, None)) if aid else (None, None)
            dn_d[sid] = dn or "none"
            env_d[sid] = env or "none"
            if dets:
                det_d[sid] = fo.Detections(detections=dets)
                dc = Counter(d.label for d in dets).most_common(1)[0][0]
                det_frames += 1
            else:
                dc = "none"
            dc_d[sid] = dc
            nm_d[sid] = fp.normalize_class(dc)

        ds.set_values("caption", cap_d, key_field="id")
        ds.set_values("daynight", dn_d, key_field="id")
        ds.set_values("environment", env_d, key_field="id")
        ds.set_values("detection_class", dc_d, key_field="id")
        ds.set_values("normalized_class", nm_d, key_field="id")
        if det_d:
            ds.set_values("detections", det_d, key_field="id")

        done += len(id_batch)
        del view, det_lists, cap_d, dn_d, env_d, dc_d, nm_d, det_d, refs, caps, envs
        gc.collect()
        if done % (CHUNK * 10) < CHUNK:
            log(f"  labels {done}/{len(all_ids)} (det={det_frames}) avail={mem_avail_mb()}MB")
    log(f"labels 완료 (detections on {det_frames} frames)")


try:
    attach_labels_batched()
except MemoryFloor as exc:
    log(f"⚠️ {exc} — labels 단계에서 중단")
    raise SystemExit(2) from exc
except Exception as exc:  # noqa: BLE001 — 라벨 실패해도 빌드 유지
    log(f"labels skipped: {exc!r}")

try:
    fp.attach_project(ds)  # id/image_id/minio_key 만 로드 — 문자열이라 가볍다
    log("project + saved views 완료")
except Exception as exc:  # noqa: BLE001
    log(f"project skipped: {exc!r}")


# ── 5. UMAP: FIT 샘플만 fit → 배치 transform (전체 로드 금지) ──────────────────
def embeddings_of(id_batch) -> np.ndarray:
    return np.asarray(
        ds.select(id_batch, ordered=True).values("embedding"), dtype="float32"
    )


def build_umap(all_ids) -> np.ndarray | None:
    try:
        import umap
    except Exception as exc:  # noqa: BLE001
        log(f"UMAP skipped (umap-learn 없음): {exc!r}")
        return None

    n = len(all_ids)
    reducer = umap.UMAP(n_components=2, metric="cosine", low_memory=True, verbose=False)
    if n > FIT:
        random.seed(42)
        fit_ids = [all_ids[i] for i in sorted(random.sample(range(n), FIT))]
        wait_for_memory()
        Xf = embeddings_of(fit_ids)
        log(f"UMAP fit on {Xf.shape}")
        reducer.fit(Xf)
        del Xf, fit_ids
        gc.collect()
        pts = np.empty((n, 2), dtype="float32")
        off = 0
        for id_batch in batches(all_ids, TBATCH):
            wait_for_memory()
            Xb = embeddings_of(id_batch)
            pts[off : off + len(id_batch)] = reducer.transform(Xb)
            off += len(id_batch)
            del Xb
            gc.collect()
            log(f"  UMAP transform {off}/{n} avail={mem_avail_mb()}MB")
        return pts
    wait_for_memory()
    X = embeddings_of(all_ids)
    pts = reducer.fit_transform(X).astype("float32")
    del X
    gc.collect()
    return pts


def build_pca(all_ids) -> np.ndarray | None:
    """IncrementalPCA — 전체를 메모리에 올리지 않고 partial_fit."""
    try:
        from sklearn.decomposition import IncrementalPCA
    except Exception as exc:  # noqa: BLE001
        log(f"PCA skipped: {exc!r}")
        return None
    n = len(all_ids)
    ipca = IncrementalPCA(n_components=2)
    for id_batch in batches(all_ids, TBATCH):
        wait_for_memory()
        Xb = embeddings_of(id_batch)
        if len(Xb) >= 2:  # partial_fit 은 n_samples >= n_components 필요
            ipca.partial_fit(Xb)
        del Xb
        gc.collect()
    pts = np.empty((n, 2), dtype="float32")
    off = 0
    for id_batch in batches(all_ids, TBATCH):
        wait_for_memory()
        Xb = embeddings_of(id_batch)
        pts[off : off + len(id_batch)] = ipca.transform(Xb)
        off += len(id_batch)
        del Xb
        gc.collect()
    return pts


try:
    # points= 는 samples 기본 순서에 정렬돼야 한다 (sample_ids 인자 미지원).
    # 그래서 같은 ds.values("id") 순서로 배치를 만들어 그 순서대로 채운다.
    ordered_ids = ds.values("id")
    log(f"UMAP/PCA 대상 n={len(ordered_ids)}")

    upts = build_umap(ordered_ids)
    if upts is not None:
        fob.compute_visualization(ds, points=upts, brain_key="emb_viz")
        log("emb_viz 등록 (points=)")
        del upts
        gc.collect()

    ppts = build_pca(ordered_ids)
    if ppts is not None:
        fob.compute_visualization(ds, points=ppts, brain_key="emb_viz_pca")
        log("emb_viz_pca 등록 (IncrementalPCA)")
        del ppts
        gc.collect()
except MemoryFloor as exc:
    log(f"⚠️ {exc} — 투영 단계에서 중단 (샘플/라벨은 보존됨)")
    raise SystemExit(2) from exc
except Exception as exc:  # noqa: BLE001 — 투영 실패해도 데이터셋은 유지
    log(f"projection skipped: {exc!r}")

log(f"BUILD DONE dataset={DATASET} samples={ds.count()} avail={mem_avail_mb()}MB")

if TEXT_SEARCH:
    try:
        wait_for_memory()
        fp.build_text_search_index(ds, brain_key="text_search")
        log("text_search 인덱스 완료")
    except Exception as exc:  # noqa: BLE001
        log(f"text_search skipped: {exc!r}")

# ── 표시층 자동 정리 ──────────────────────────────────────────────────────
# 빌드하면 필드가 평평하게 40~70개 쏟아져 필터 사이드바에서 분석이 불가능하다.
# fiftyone_presentation 이 필드별 카디널리티를 실측해 역할을 자동 판정하고
#   ① 사이드바 그룹(순서·접힘)  ② 노이즈 제외 저장뷰 `00_analysis`  ③ 워크스페이스
# 를 만든다. 멱등이라 매 빌드마다 다시 불러도 안전하다.
# ⚠️ 사이드바 그룹만으로는 필드가 숨겨지지 않는다(FiftyOne 이 미배정 필드를 자동
#    PRIMITIVES 그룹으로 되살린다) — 분석은 `00_analysis` 뷰를 선택해서 시작할 것.
try:
    import fiftyone_presentation as fpres

    fpres.apply(
        ds,
        dry_run=False,
        workspaces=[
            ("explore", "emb_viz", "project"),
            ("explore-class", "emb_viz", "normalized_class"),
        ],
    )
    log("presentation(사이드바 그룹 + 00_analysis 뷰 + 워크스페이스) 적용 완료")
except Exception as exc:  # noqa: BLE001 — 표시층 실패가 빌드를 깨뜨리지 않게
    log(f"presentation skipped: {exc!r}")

if LAUNCH:
    log("launching app on :5151")
    fo.launch_app(ds, address="0.0.0.0", port=5151)
    log("APP_LAUNCHED")
    time.sleep(10**9)
