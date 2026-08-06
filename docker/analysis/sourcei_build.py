#!/usr/bin/env python3
"""source-i 이벤트 구간 → SAM3.1 검출 → 임베딩 → FiftyOne `sourcei` 데이터셋.

요청(2026-08-05): sourcei_* 로 진행 / `/datasets/sourcei` / **SAM3.1 로 이벤트 구간만 이미지
추출** / 프롬프트 뱅크는 v1.0.8.0 그대로.

**이 데이터셋의 성격 — recall 벤치마크가 아니라 오탐(FP) 스트레스 테스트다.**
DB 실측(810 이벤트/109편): 4클래스 GT 는 falldown 63 / fire 5 / smoke 5 이벤트뿐이고
(fire 는 총 10초) 나머지는 near_miss 509 · other/drop/violence 125 · v3 캡션 NULL 102 다.
즉 이벤트 구간 대부분이 **4클래스 어디에도 해당하지 않는 실내 장면**이라, 뱅크가 여기서
fire/smoke/falldown 을 부르면 그게 곧 오탐이다. 실내 데이터로 옮기자는 회의 §1 의 목적
(오탐 발생 위치 진단)에 정확히 맞는 쓰임이고, recall 을 인용해서는 안 된다.

⚠️ **"넘어질 뻔함"(near_miss) 509건은 falldown 이 아니다** — 기본 GT 를 normal 로 둔다.
   falldown 으로 세면 없는 FN 을 만들어낸다. 판단을 코드에 묻지 않고 `event_kind` 필드로
   남기므로 App 에서 뒤집어 볼 수 있다.

⚠️ 영상을 **내려받지 않는다** — presigned URL 에 ffmpeg `-ss/-to` 로 Range 요청만 보낸다
   (실측: 3,600초 원격 mp4 에서 5초 구간 추출 0.4초). 호스트 루트 디스크가 98%(여유 21GB)
   라 8.4GB 영상 사본을 만들 여유가 없다. 프레임만 남긴다 (~1GB).

스테이지:
    segments  DB(labels+raw_files) → segments.jsonl (구간 + GT + event_kind)
    frames    presigned URL + ffmpeg fps=2 → frames/<cls>/*.jpg + ledger.jsonl
    sam3      SAM3.1 /segment (4클래스 개념 프롬프트) → sam3.jsonl (+ sam3_hit)
    embed     embedding-service /embed → embed.npz (prompt_geometry.load_all 포맷)
    build     FiftyOne `sourcei` + UMAP + 필드
    all       위 전부 순차

이후 뱅크 분석은 prompt_geometry.py 를 `--profile sourcei` 로 재사용한다:
    BANK_A=v1.0.8.0 BANK_B=v1.0.8.0 python prompt_geometry.py attach --profile sourcei
    ... wave / promptmap / attrs 동일
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import subprocess
import sys
import time

import numpy as np

ROOT = os.environ.get("HY_ROOT", "/data/fiftyone/sourcei")
WORK = f"{ROOT}/work"
FRAMES = f"{ROOT}/frames"
DATASET = os.environ.get("HY_DATASET", "sourcei")
FPS = float(os.environ.get("HY_FPS", "2"))          # 제품 pe_inference `--model_input_fps 2` 와 동일
PAD_SEC = float(os.environ.get("HY_PAD_SEC", "0.5"))  # 구간 경계 여유 (Gemini 타임스탬프 오차)
MIN_SEG_SEC = float(os.environ.get("HY_MIN_SEG_SEC", "0.5"))
SAM3_URL = os.environ.get("SAM3_API_URL", "http://docker-sam3-1:8002")
EMBED_URL = os.environ.get("EMBEDDING_API_URL", "http://embedding-service:8003")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://10.0.0.51:9000")
RAW_BUCKET = os.environ.get("HY_RAW_BUCKET", "vlm-raw")
PG_DSN = os.environ.get("DATAOPS_POSTGRES_DSN",
                        "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")

CLASS_NAMES = {0: "normal", 1: "falldown", 2: "fire", 3: "smoke"}
# SAM3.1 개념 프롬프트 — COCO 카테고리 통합(2026-06-29) 이후의 정본 이름을 쓴다
SAM3_PROMPTS = ["person", "fallen person", "fire", "smoke"]
# GT 규칙 우선순위: 폴더(v2 는 클래스 폴더가 있다) → 캡션 → 없음.
# `뻔` 을 falldown 보다 **먼저** 본다 — "넘어질 뻔함" 이 `넘어지` 에 먼저 걸리면 안 된다.
CAPTION_RULES = (
    ("near_miss", re.compile(r"뻔")),
    ("falldown", re.compile(r"넘어지|쓰러|주저앉|눕")),
    ("fire", re.compile(r"화재|화염|불꽃")),
    ("smoke", re.compile(r"연기")),
    ("drop", re.compile(r"떨어|낙하|유실")),
    ("violence", re.compile(r"발로|폭행|싸움|밀치")),
)
# event_kind → 4클래스 GT. near_miss/drop/violence/other/unknown 은 normal 이다
# (뱅크에 해당 클래스가 없다 → 뱅크가 이벤트를 부르면 오탐).
KIND_TO_CLASS = {"falldown": 1, "fire": 2, "smoke": 3}


def person_bin(n: int) -> str:
    """연속 int 는 App 에서 색이 안 나온다 → 구간화 필드가 따로 필요하다."""
    return ("0" if n == 0 else "1" if n == 1 else "2-3" if n <= 3
            else "4-6" if n <= 6 else "7-10" if n <= 10 else "11+")


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def psql(sql: str) -> list[tuple]:
    import psycopg2
    with psycopg2.connect(PG_DSN) as con, con.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def s3_client():
    import boto3
    import botocore
    key = os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
    sec = os.environ.get("MINIO_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not (key and sec):
        raise SystemExit("MinIO 자격 없음 — MINIO_ACCESS_KEY/MINIO_SECRET_KEY 를 넘길 것")
    return boto3.client("s3", endpoint_url=MINIO_ENDPOINT, aws_access_key_id=key,
                        aws_secret_access_key=sec,
                        config=botocore.client.Config(signature_version="s3v4"))


def camera_of(su: str, raw_key: str) -> str:
    """카메라 = 고정 설치 지점. cheonho 는 파일 stem 이 곧 설치 위치(9대), v2 는 stem 에서
    클래스 접미사를 떼면 위치가 남는다. v3 는 파일명이 uuid 라 알 수 없다 → unknown."""
    stem = os.path.splitext(os.path.basename(raw_key))[0]
    if su == "sourcei_v3":
        return "v3_unknown"
    if su == "sourcei_v2":
        stem = re.sub(r"_(crowd_)?(esfalldown|falldown|fire|smoke|normal)"
                      r"(_\d{4}_\d{4})?$", "", stem)
    return stem or "unknown"


def kind_of(raw_key: str, caption: str | None) -> tuple[str, str]:
    """(event_kind, gt_source). 폴더 GT 가 캡션보다 강하다 (사람이 만든 디렉토리 구조)."""
    m = re.search(r"/(esfalldown|falldown|fire|smoke|normal)/", raw_key)
    if m:
        k = m.group(1)
        return ("falldown" if k == "esfalldown" else k), "folder"
    # 파일명 stem 에도 클래스가 박혀 있다 (`33_smoke_20260619_012648`, `14_falldown_...`).
    # 2026-08-06: 이걸 안 봐서 캡션 NULL 인 1,806장이 전부 unknown→normal 로 떨어졌고,
    # SAM3 가 그중 33% 에서 fallen person 을 찾아 반증했다. 사람이 붙인 이름이므로
    # Gemini 캡션보다 강한 근거로 취급한다 (폴더 다음 순위).
    m = re.search(r"(?:^|[_/])(esfalldown|falldown|fire|smoke|normal)(?=[_.]|$)",
                  os.path.basename(raw_key))
    if m:
        k = m.group(1)
        return ("falldown" if k == "esfalldown" else k), "filename"
    if not caption:
        return "unknown", "none"
    for kind, rx in CAPTION_RULES:
        if rx.search(caption):
            return kind, "caption"
    return "other", "caption"


# ────────────────────── segments ──────────────────────
def stage_segments() -> None:
    rows = psql("""
        SELECT r.source_unit_name, r.raw_key, l.asset_id, l.event_index,
               l.timestamp_start_sec, l.timestamp_end_sec, COALESCE(l.caption_text,''),
               COALESCE(vm.duration_sec,0)
        FROM labels l JOIN raw_files r ON r.asset_id=l.asset_id
        LEFT JOIN video_metadata vm ON vm.asset_id=l.asset_id
        WHERE r.source_unit_name LIKE 'sourcei%'
          AND l.timestamp_start_sec IS NOT NULL AND l.timestamp_end_sec IS NOT NULL
        ORDER BY r.raw_key, l.event_index
    """)
    os.makedirs(WORK, exist_ok=True)
    out, skipped, kinds = [], 0, {}
    for su, raw_key, asset_id, ev, s0, s1, cap, dur in rows:
        a, b = float(s0), float(s1)
        if b <= a:
            b = a + MIN_SEG_SEC
        a = max(0.0, a - PAD_SEC)
        b = b + PAD_SEC
        if float(dur) > 0:
            b = min(b, float(dur))
        if b - a < MIN_SEG_SEC:
            skipped += 1
            continue
        kind, gsrc = kind_of(raw_key, cap or None)
        kinds[kind] = kinds.get(kind, 0) + 1
        out.append({"su": su, "raw_key": raw_key, "asset_id": asset_id,
                    "event_index": int(ev), "start": round(a, 3), "end": round(b, 3),
                    "caption": cap, "event_kind": kind, "gt_source": gsrc,
                    "gt_class": KIND_TO_CLASS.get(kind, 0),
                    "camera": camera_of(su, raw_key)})
    p = f"{WORK}/segments.jsonl"
    with open(p, "w", encoding="utf-8") as f:
        for r in out:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    secs = sum(r["end"] - r["start"] for r in out)
    log(f"segments: {len(out):,}건 (스킵 {skipped}) · 총 {secs:,.0f}초 → 예상 프레임 "
        f"{int(secs * FPS):,} (fps={FPS}) → {p}")
    log("segments: event_kind " + " ".join(f"{k}={v}" for k, v in
                                          sorted(kinds.items(), key=lambda x: -x[1])))
    gtd = {}
    for r in out:
        gtd[CLASS_NAMES[r["gt_class"]]] = gtd.get(CLASS_NAMES[r["gt_class"]], 0) + 1
    log(f"segments: GT 분포 {gtd} — near_miss/drop/other 는 normal 이다 (뱅크에 그 클래스가 없다)")


# ────────────────────── frames ──────────────────────
def stage_frames() -> None:
    segs = [json.loads(ln) for ln in open(f"{WORK}/segments.jsonl", encoding="utf-8")]
    s3 = s3_client()
    presigned: dict[str, tuple[float, str]] = {}

    def url_for(key: str) -> str:
        now = time.time()
        u = presigned.get(key)
        if u and u[0] > now + 60:
            return u[1]
        exp = 3600
        link = s3.generate_presigned_url("get_object",
                                         Params={"Bucket": RAW_BUCKET, "Key": key},
                                         ExpiresIn=exp)
        presigned[key] = (now + exp, link)
        return link

    ledger, fails, t0 = [], [], time.time()
    for i, sg in enumerate(segs):
        cls = CLASS_NAMES[sg["gt_class"]]
        d = f"{FRAMES}/{cls}"
        os.makedirs(d, exist_ok=True)
        stem = re.sub(r"[^0-9A-Za-z._-]", "_",
                      os.path.splitext(os.path.basename(sg["raw_key"]))[0])[:70]
        pat = f"{d}/{stem}__e{sg['event_index']:03d}_%03d.jpg"
        r = subprocess.run(["ffmpeg", "-y", "-loglevel", "error",
                            "-ss", f"{sg['start']:.3f}", "-to", f"{sg['end']:.3f}",
                            "-i", url_for(sg["raw_key"]),
                            "-vf", f"fps={FPS}", "-q:v", "3", pat],
                           capture_output=True, text=True)
        if r.returncode != 0:
            fails.append({"raw_key": sg["raw_key"], "event_index": sg["event_index"],
                          "err": r.stderr[-300:]})
            continue
        n = 0
        # ⚠️ 상한을 두면 초과분이 **디스크에는 있고 ledger 에는 없는 유령 프레임**이 된다.
        #    2026-08-06: `range(1,1000)` 이 600초짜리 pseudo-event(Gemini 가 영상 전체를 한
        #    이벤트로 라벨) 에서 202장을 조용히 버렸다. 상한 없이 끝까지 세고, 한 세그먼트가
        #    비정상적으로 크면 경고한다 (한 세그먼트가 데이터셋을 지배하는 신호다).
        j = 0
        while True:
            j += 1
            fp = pat % j
            if not os.path.exists(fp):
                break
            n += 1
            ledger.append({
                "key": f"{cls}/{os.path.basename(fp)}", "filepath": fp,
                "gt_class": sg["gt_class"], "event_kind": sg["event_kind"],
                "gt_source": sg["gt_source"], "camera": sg["camera"], "su": sg["su"],
                # src_video = 영상 단위 집계 키 (prompt_geometry 가 이 이름을 쓴다)
                "src_video": os.path.splitext(os.path.basename(sg["raw_key"]))[0],
                "event_index": sg["event_index"], "frame_in_event": j,
                "t_sec": round(sg["start"] + (j - 1) / FPS, 3),
                "caption": sg["caption"], "raw_key": sg["raw_key"],
            })
        if n == 0:
            fails.append({"raw_key": sg["raw_key"], "event_index": sg["event_index"],
                          "err": "frames=0"})
        elif n > 300:
            log(f"frames: ⚠️ 한 세그먼트에서 {n:,}장 — {sg['raw_key']} e{sg['event_index']:03d} "
                f"({sg['end'] - sg['start']:.0f}초). Gemini 가 영상 전체를 한 이벤트로 라벨한 "
                "경우다. 이 세그먼트가 데이터셋을 지배하는지 확인할 것")
        if (i + 1) % 100 == 0:
            log(f"frames: {i + 1}/{len(segs)} 구간 · 프레임 {len(ledger):,} "
                f"· {time.time() - t0:.0f}s")
    with open(f"{WORK}/ledger.jsonl", "w", encoding="utf-8") as f:
        for r in ledger:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    if fails:
        with open(f"{WORK}/frames_failed.jsonl", "w", encoding="utf-8") as f:
            for r in fails:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    byc = {}
    for r in ledger:
        byc[CLASS_NAMES[r["gt_class"]]] = byc.get(CLASS_NAMES[r["gt_class"]], 0) + 1
    log(f"frames: {len(ledger):,}장 (실패 구간 {len(fails)}) · GT {byc} "
        f"· {time.time() - t0:.0f}s → {WORK}/ledger.jsonl")


# ────────────────────── sam3 ──────────────────────
def stage_sam3() -> None:
    """SAM3.1 검출. **프레임을 지우지 않고 `sam3_hit` 플래그만 남긴다** — 미검출 프레임도
    오탐 분석의 모수라서 버리면 되돌릴 수 없다. '이벤트 구간만' 을 더 좁히고 싶으면
    App 에서 `sam3_hit=True` 로 필터하면 된다.

    ⚠️ 응답 스키마 (2026-08-05 실측): 라벨은 `prompt_class`, 박스는 `mask_bbox`(xyxy),
       크기는 `image_size=[w,h]`. `prompt`/`label`/`width`/`height` 는 **없다**.

    ⚠️ `docker-sam3-1` 은 prod·staging 공유이고 워커 3개가 각 5.2GB 를 상주해
       15.6/15.72 GB 를 쓴다 (여유 70MB). 1920×1080 원본을 그대로 보내면 CUDA OOM 500 이
       난다 — **공유 컨테이너 설정을 건드리지 않고** 클라이언트에서만 완화한다:
       긴 변 SAM3_MAX_SIDE 로 축소 + max_masks 축소 + 백오프 재시도.
       (CLAUDE.md 의 "workers 3 ≈ 11.1GB" 는 stale — 7일 기동 후 실측 15.6GB.)
    """
    import requests
    from PIL import Image
    # 1280 은 실측 실패율 17%(3회 재시도 후에도 OOM) + 1.36s/frame 이었다 → 1024.
    # ⚠️ 축소 배율을 바꾸면 **검출 민감도가 바뀐다**. 데이터셋 안에서 섞이면 조용한 교란이
    #    되므로 값을 바꿀 때는 sam3.jsonl 을 지우고 전량 재처리할 것 (부분 재개 금지).
    #    감사 가능하게 레코드마다 `max_side` 를 같이 적는다.
    max_side = int(os.environ.get("SAM3_MAX_SIDE", "1024"))
    max_masks = int(os.environ.get("SAM3_MAX_MASKS", "10"))
    retries = int(os.environ.get("SAM3_RETRIES", "4"))
    # ⚠️ **근본 원인은 축소 배율이 아니었다.** 장기 실행 중 워커 3개의 PyTorch 캐시가
    #    누적돼 16.85/16.88 GB 까지 차면 **모든** 요청이 OOM 500 이 된다 (아까 성공했던
    #    프레임도 전부 실패했다 = 프레임/배율 무관). `/unload` 6회로 6.35GB 까지 회수되고
    #    다음 `/segment` 가 lazy reload 한다 — prod 컨테이너 재시작 없이 복구된다.
    #    워커가 3개라 unload 1회는 한 워커만 비우므로 여러 번 때린다.
    unload_every = int(os.environ.get("SAM3_UNLOAD_EVERY", "500"))
    led = [json.loads(ln) for ln in open(f"{WORK}/ledger.jsonl", encoding="utf-8")]
    sess = requests.Session()
    try:
        sess.post(f"{SAM3_URL}/warmup", timeout=600)
    except Exception as exc:                                        # noqa: BLE001
        log(f"sam3: warmup 실패(무시) {exc!r}")
    h = sess.get(f"{SAM3_URL}/health", timeout=30).json()
    log(f"sam3: health model_loaded={h.get('model_loaded')} gpu={h.get('gpu_memory')}")

    done, out, t0 = set(), [], time.time()
    p = f"{WORK}/sam3.jsonl"
    if os.path.exists(p):                                            # 멱등 재개
        for ln in open(p, encoding="utf-8"):
            try:
                out.append(json.loads(ln))
                done.add(out[-1]["key"])
            except json.JSONDecodeError:
                pass
        log(f"sam3: 기존 {len(done):,}건 재사용")
    fails = 0
    with open(p, "a", encoding="utf-8") as fh:
        for i, r in enumerate(led):
            if r["key"] in done:
                continue
            # 축소는 클라이언트에서 — 공유 SAM3 의 VRAM 여유가 70MB 뿐이다.
            # bbox 는 축소 배율로 되돌리지 않고 **축소 좌표계 크기와 함께** 저장하고,
            # build 에서 그 크기로 정규화한다 (원본 좌표 복원이 불필요해진다).
            try:
                with Image.open(r["filepath"]) as pim:
                    pim = pim.convert("RGB")
                    if max(pim.size) > max_side:
                        sc = max_side / max(pim.size)
                        pim = pim.resize((max(1, round(pim.width * sc)),
                                          max(1, round(pim.height * sc))), Image.BILINEAR)
                    buf = io.BytesIO()
                    pim.save(buf, format="JPEG", quality=90)
                    blob = buf.getvalue()
            except Exception as exc:                                 # noqa: BLE001
                fails += 1
                if fails <= 5:
                    log(f"sam3: 이미지 로드 실패 {r['key']} {exc!r}")
                continue
            j = None
            for attempt in range(retries):
                try:
                    resp = sess.post(f"{SAM3_URL}/segment",
                                     files={"file": (os.path.basename(r["filepath"]),
                                                     io.BytesIO(blob), "image/jpeg")},
                                     data={"prompts_json": json.dumps(SAM3_PROMPTS,
                                                                     ensure_ascii=False),
                                           "score_threshold": "0.0",
                                           "max_masks_per_prompt": str(max_masks)},
                                     timeout=300)
                    resp.raise_for_status()
                    j = resp.json()
                    break
                except Exception as exc:                             # noqa: BLE001
                    if attempt == retries - 1:
                        fails += 1
                        if fails <= 5:
                            log(f"sam3: 실패 {r['key']} {exc!r}")
                    else:
                        time.sleep(0.4 * (attempt + 1))              # OOM 은 대개 일시적
            if j is None:
                continue
            dets = []
            for d in (j.get("detections") or []):
                box = d.get("mask_bbox") or d.get("model_box")
                if not box:
                    continue
                dets.append({"label": str(d.get("prompt_class") or "?"),
                             "bbox": [float(x) for x in box],
                             "score": float(d.get("score") or 0.0)})
            wh = j.get("image_size") or [0, 0]                        # [w, h] — 축소 좌표계
            rec = {"key": r["key"], "n": len(dets), "dets": dets,
                   "width": int(wh[0]), "height": int(wh[1]), "max_side": max_side}
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out.append(rec)
            if (i + 1) % 200 == 0:
                fh.flush()
                try:
                    g = sess.get(f"{SAM3_URL}/health", timeout=30).json().get("gpu_memory", {})
                except Exception:                                    # noqa: BLE001
                    g = {}
                log(f"sam3: {i + 1}/{len(led)} · {time.time() - t0:.0f}s "
                    f"· 검출있음 {sum(1 for x in out if x['n']):,} · 실패 {fails} "
                    f"· gpu_free={g.get('free_gb')}")
            if unload_every and (i + 1) % unload_every == 0:
                for _ in range(4):                                   # 워커 3개 커버
                    try:
                        sess.post(f"{SAM3_URL}/unload", timeout=120)
                    except Exception:                                # noqa: BLE001
                        pass
                log(f"sam3: {i + 1} — VRAM 캐시 회수(/unload ×4). 다음 요청이 lazy reload")
    hit = sum(1 for x in out if x["n"])
    bylab = {}
    for x in out:
        for d in x["dets"]:
            bylab[d["label"]] = bylab.get(d["label"], 0) + 1
    log(f"sam3: {len(out):,}/{len(led):,}프레임 · **실패 {fails} ({fails / max(len(led), 1):.1%})** "
        f"· 검출있음 {hit:,} ({hit / max(len(out), 1):.1%}) · max_side={max_side} "
        f"· 라벨별 {bylab} → {p}")
    if fails / max(len(led), 1) > 0.05:
        log("sam3: ⚠️ 실패율 5% 초과 — 실패 프레임은 sam3_hit 이 비어 근거가 없다. "
            "SAM3_MAX_SIDE 를 더 낮추고 sam3.jsonl 을 지우고 전량 재처리할 것")


# ────────────────────── embed ──────────────────────
def stage_embed() -> None:
    import requests
    led = [json.loads(ln) for ln in open(f"{WORK}/ledger.jsonl", encoding="utf-8")]
    sess = requests.Session()
    keys, vecs, fails, t0 = [], [], 0, time.time()
    for i, r in enumerate(led):
        try:
            with open(r["filepath"], "rb") as im:
                resp = sess.post(f"{EMBED_URL}/embed",
                                 files={"file": (os.path.basename(r["filepath"]),
                                                 im, "image/jpeg")}, timeout=180)
            resp.raise_for_status()
            v = np.asarray(resp.json()["vector"], dtype=np.float32)
        except Exception as exc:                                     # noqa: BLE001
            fails += 1
            if fails <= 5:
                log(f"embed: 실패 {r['key']} {exc!r}")
            continue
        keys.append(r["key"])
        vecs.append(v / np.linalg.norm(v))
        if (i + 1) % 500 == 0:
            log(f"embed: {i + 1}/{len(led)} · {time.time() - t0:.0f}s")
    X = np.stack(vecs).astype(np.float32)
    np.savez_compressed(f"{WORK}/embed.npz", key=np.array(keys), vec=X)
    log(f"embed: {len(keys):,}장 dim={X.shape[1]} (실패 {fails}) · {time.time() - t0:.0f}s "
        f"→ {WORK}/embed.npz")


# ────────────────────── build ──────────────────────
def stage_build() -> None:
    import fiftyone as fo
    import fiftyone.brain as fob

    led = {r["key"]: r for r in
           (json.loads(ln) for ln in open(f"{WORK}/ledger.jsonl", encoding="utf-8"))}
    d = np.load(f"{WORK}/embed.npz", allow_pickle=True)
    emb = {str(k): v for k, v in zip(d["key"], d["vec"])}
    sam = {}
    if os.path.exists(f"{WORK}/sam3.jsonl"):
        for ln in open(f"{WORK}/sam3.jsonl", encoding="utf-8"):
            try:
                r = json.loads(ln)
                sam[r["key"]] = r
            except json.JSONDecodeError:
                pass

    ds = fo.Dataset(DATASET, overwrite=True, persistent=True)
    batch, n_emb = [], 0
    for key, r in led.items():
        if not os.path.exists(r["filepath"]):
            continue
        s = fo.Sample(filepath=r["filepath"])
        s["ground_truth"] = fo.Classification(label=CLASS_NAMES[r["gt_class"]])
        s["event_kind"] = fo.Classification(label=r["event_kind"])
        s["gt_source"] = fo.Classification(label=r["gt_source"])
        s["source_unit"] = fo.Classification(label=r["su"])
        s["camera"] = r["camera"]
        s["src_video"] = r["src_video"]
        s["event_index"] = r["event_index"]
        s["frame_in_event"] = r["frame_in_event"]
        s["t_sec"] = r["t_sec"]
        s["caption"] = r["caption"]
        sm = sam.get(key)
        if sm is not None:
            s["sam3_hit"] = fo.Classification(label="hit" if sm["n"] else "miss")
            s["sam3_n"] = sm["n"]
            # 인원수 — zero-shot 프로브는 유/무만 낼 수 있고 셀 수는 없다. SAM3 박스가 정본.
            # `fallen person` 은 따로 센다 (겹쳐서 더하면 한 사람이 두 번 세어진다).
            s["person_count"] = sum(1 for d in sm["dets"] if d["label"] == "person")
            s["fallen_person_count"] = sum(1 for d in sm["dets"]
                                           if d["label"] == "fallen person")
            s["person_count_bin"] = fo.Classification(label=person_bin(s["person_count"]))
            w, h = sm.get("width") or 0, sm.get("height") or 0
            if sm["dets"] and w and h:
                s["sam3"] = fo.Detections(detections=[
                    fo.Detection(label=str(dt["label"]), confidence=dt["score"],
                                 bounding_box=[dt["bbox"][0] / w, dt["bbox"][1] / h,
                                               (dt["bbox"][2] - dt["bbox"][0]) / w,
                                               (dt["bbox"][3] - dt["bbox"][1]) / h])
                    for dt in sm["dets"]])
                s["sam3_labels"] = fo.Classifications(classifications=[
                    fo.Classification(label=str(dt["label"]), confidence=dt["score"])
                    for dt in sm["dets"]])
        v = emb.get(key)
        if v is not None:
            s["embedding"] = np.asarray(v, dtype=np.float32).tolist()
            n_emb += 1
        batch.append(s)
        if len(batch) >= 2000:
            ds.add_samples(batch)
            batch = []
    if batch:
        ds.add_samples(batch)
    log(f"build: 샘플 {ds.count():,} (임베딩 {n_emb:,})")

    for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
               "NUMBA_NUM_THREADS"):
        os.environ.setdefault(_v, str(max(1, (os.cpu_count() or 4) // 4)))
    import umap
    ids = ds.match({"embedding": {"$ne": None}}).values("id")
    E = np.asarray(ds.select(ids, ordered=True).values("embedding"), dtype=np.float32)
    log(f"build: UMAP fit {E.shape}")
    pts = umap.UMAP(n_components=2, metric="cosine", low_memory=True,
                    random_state=42).fit_transform(E)
    fob.compute_visualization(ds.select(ids, ordered=True), points=pts, brain_key="emb_viz")

    for name, color in (("explore", "ground_truth.label"), ("kind", "event_kind.label"),
                        ("sam3", "sam3_hit.label"), ("site", "source_unit.label")):
        if color.split(".")[0] not in ds.get_field_schema():
            continue
        space = fo.Space(children=[
            fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
            fo.Space(children=[fo.Panel(type="Embeddings",
                                        state={"brainResult": "emb_viz",
                                               "colorByField": color})]),
        ], orientation="horizontal")
        ds.save_workspace(name, space, description=f"emb_viz (색: {color})")
    ds.save()
    log(f"build: {DATASET} 완료 · brain {ds.list_brain_runs()} · ws {ds.list_workspaces()}")
    log("build: GT " + str(ds.count_values("ground_truth.label"))
        + " · kind " + str(ds.count_values("event_kind.label")))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=["segments", "frames", "sam3", "embed", "build", "all"])
    a = ap.parse_args()
    os.makedirs(WORK, exist_ok=True)
    table = {"segments": stage_segments, "frames": stage_frames, "sam3": stage_sam3,
             "embed": stage_embed, "build": stage_build}
    for st in (["segments", "frames", "sam3", "embed", "build"] if a.stage == "all"
               else [a.stage]):
        log(f"───── stage: {st} ─────")
        table[st]()
    log("완료")


if __name__ == "__main__":
    sys.exit(main())
