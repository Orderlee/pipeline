#!/usr/bin/env python3
"""source-h 871편: camera_angle(DAv2) 분류 + 프롬프트 뱅크 2버전 제로샷 비교 + FiftyOne 데이터셋.

목적
  1) MinIO `source-h/`(원본 한글 키) · `source-h/`(정규화 키) 두 prefix **만** 대상으로 FiftyOne
     데이터셋 `source-h` 구축 → http://10.0.0.10:5153/datasets/source-h
  2) 같은 871편에 camera_angle(plan_view|non_plan|indeterminate)을 DAv2 서비스로 부여하고
     `video_metadata.camera_angle`/`angle_method` (migration 017) 에 기록
  3) userwatch 프롬프트 뱅크 v1.0.8.0 vs v1.0.8.4 제로샷 성능 비교 + 커버리지 산출

핵심 사실(측정으로 확정, 2026-07-29)
  - embedding-service `/embed_text` 는 userwatch 가 뱅크 JSON 에 실어둔 `feature` 벡터와
    cosine=1.000000 으로 **동일 인코더**(PE-Core-L14-336, L2 정규화). 따라서 CSV 텍스트만으로
    제품 점수를 그대로 재현할 수 있다 (439MB/567MB JSON 불필요).
  - DB(raw_files.file_size) 와 바이트가 일치하는 쪽은 `source-h/<한글>` 871/871.
    `source-h/<sanitize>` 는 804 일치 / **67 불일치**(더 작은 다른 바이트) → 미디어는 source-h 사용.

GT: 폴더명 파생 weak GT (falldown→1, fire→2, smoke→3, helmet→0=normal). 사람 검수 아님.
     eval 전용이며 학습에 쓰지 않는다(자기학습 금지 원칙과 무관).

결정 규칙: 클래스 점수 = max over (3프레임 × 그 클래스 프롬프트) cosine → argmax.
           뱅크 크기 편향 점검용으로 top-10 평균 규칙도 함께 산출한다.

스테이지(각각 멱등, 중단 후 재실행 가능):
    prompts  CSV → /embed_text → prompts/<ver>.npz
    media    MinIO 다운로드(+sha256 검증) → ffprobe → 3키프레임 → 영상 삭제
    angle    키프레임 3장 → DAv2 /angle
    embed    키프레임 3장 → /embed (PE-Core 이미지 벡터)
    score    코사인 점수/예측/정답 여부
    dbwrite  video_metadata.camera_angle/angle_method UPDATE (871행)
    build    FiftyOne 데이터셋 `source-h` + evaluation + UMAP + saved views
    report   마크다운 리포트
    all      위 순서 전부

사용:
    python3 /workspace/sourceh_prompt_eval.py all
    python3 /workspace/sourceh_prompt_eval.py media --limit 5     # 스모크 테스트
"""

from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import json
import math
import os
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import requests

ROOT = "/data/fiftyone/sourceh"
PROMPT_DIR = f"{ROOT}/prompts"
FRAME_DIR = f"{ROOT}/keyframes"
WORK_DIR = f"{ROOT}/work"
REPORT_DIR = f"{ROOT}/report"

BUCKET = "vlm-raw"
UPPER_PREFIX = "source-h"  # DB 바이트와 일치하는 원본 prefix
LOWER_PREFIX = "source-h"  # 정규화 키 prefix (raw_key)

EMBED_URL = os.environ.get("EMBEDDING_API_URL", "http://embedding-service:8003")
ANGLE_URL = os.environ.get("ANGLE_API_URL", "http://angle-dav2-1:8000")

VERSIONS = ("v1.0.8.0", "v1.0.8.4")
CLASS_NAMES = {0: "normal", 1: "falldown", 2: "fire", 3: "smoke", 4: "smoking"}
FOLDER_TO_CLASS = {"helmet": 0, "falldown": 1, "fire": 2, "smoke": 3}
FRAME_FRACS = (0.1, 0.5, 0.9)
TOPK = 10  # top-k 평균 결정규칙의 k (뱅크 크기 편향 점검용 2차 지표)
TOP_SHOW = 10  # 샘플별로 FiftyOne/CSV 에 노출할 "가장 닮은 프롬프트" 개수

# 분석에 쓸모없는 필드 — 필터 사이드바에서 실제로 없애려면 **뷰에서 exclude 해야 한다**.
# ⚠️ `app_config.sidebar_groups` 에서 빼는 것만으로는 숨겨지지 않는다: FiftyOne 1.19 는
#    어느 그룹에도 없는 경로를 런타임에 되살려 사이드바 아래쪽에 붙인다(2026-07-29 DOM 실측
#    — config 상 23개를 뺐는데 렌더된 필드는 77개였다). 그래서 `exclude_fields` 기반
#    저장된 뷰(`00_analysis`)를 진입점으로 만든다.
# 제외 근거: 상수(고유값 1) / 고카디널리티 ID·경로 / 완전 중복 / 오독 위험.
SIDEBAR_NOISE_FIELDS = [
    # 상수 — 필터가 만들어지지 않는다
    "angle_method", "codec", "resolution", "width", "height", "n_frames_scored",
    "ingest_status", "sha256_verified",
    # 사실상 상수 (871편 중 최빈값 100%)
    "angle_votes",
    # 고카디널리티 경로·벡터 — 패싯 불가
    "embedding", "keyframe_paths", "media_source_key",
    "top_frame_v1_0_8_0", "top_frame_v1_0_8_4",
    # 완전 중복: eval_* == correct_*(871/871). evaluation registry 는 그대로 남으므로
    # App 의 Model Evaluation 패널은 정상 동작한다(accuracy 로드 확인).
    "eval_v1_0_8_0", "eval_v1_0_8_4",
    # top10_* (Classifications) 와 표현만 다른 중복
    "top10_text_v1_0_8_0", "top10_text_v1_0_8_4",
    # 오독 위험: 뱅크 스케일에 오염된 값 (대체 지표 = gt_rel_delta / dscore_*)
    "conf_delta",
    # 분석과 무관한 미디어 속성
    "fps", "file_size_mb",
]
# ⚠️ `metadata`/`id`/`filepath`/`created_at`/`last_modified_at` 는 FiftyOne 기본 필드라
#    exclude 가 거부된다(ValueError: Cannot exclude default fields) — 사이드바에 남는다.

_print_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ────────────────────────────── 공통 헬퍼 ──────────────────────────────
def pg():
    import psycopg2

    return psycopg2.connect(os.environ["DATAOPS_POSTGRES_DSN"])


def s3():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


def jsonl_load(path: str) -> dict:
    """asset_id → record. 같은 asset_id 가 여러 번 있으면 마지막 것이 이긴다."""
    out = {}
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:  # 중단으로 잘린 마지막 줄
                continue
            out[rec["asset_id"]] = rec
    return out


def jsonl_append(path: str, rec: dict) -> None:
    with _print_lock, open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()


def camera_id_of(original_name: str) -> str:
    """파일명에서 촬영 장소(=카메라 프록시)를 뽑는다. 실패 시 stem 전체.

    source-h 은 폴더에 따라 **레이아웃이 반대**다 (실측 871편):
      A) helmet/smoke  : `<YYYYMMDD>_<HHMMSS>_<이벤트>_<장소>.mp4`
      B) falldown/fire : `<장소>_<이벤트>_<YYYYMMDD>_<HHMMSS>.mp4`
    앞 토큰이 8자리 날짜인지로 구분한다. 한쪽 규칙만 적용하면 A 패턴에서 날짜가
    camera_id 로 잡혀 카메라 집계가 전부 어긋난다.

    design-docs/camera-angle-grouping §분석단위=카메라 를 위한 프록시.
    source_unit_name 은 카메라가 아니므로 파일명에서 뽑는다.
    """
    stem = os.path.splitext(original_name)[0]
    parts = stem.split("_")
    if len(parts) >= 4 and len(parts[0]) == 8 and parts[0].isdigit() and parts[1].isdigit():
        return "_".join(parts[3:])  # 패턴 A
    if len(parts) >= 4 and len(parts[-2]) == 8 and parts[-2].isdigit():
        return "_".join(parts[:-3])  # 패턴 B
    return stem


def load_assets() -> list[dict]:
    """DB 의 source-h 871행 + MinIO 두 prefix 실측 사이즈를 합친 대상 목록."""
    with pg() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.asset_id, r.raw_key, r.original_name, r.file_size, r.checksum,
                   r.ingest_status, vm.asset_id IS NOT NULL AS has_vm
            FROM raw_files r
            LEFT JOIN video_metadata vm ON vm.asset_id = r.asset_id
            WHERE r.raw_key LIKE %s
            ORDER BY r.raw_key
            """,
            (f"{LOWER_PREFIX}/%",),
        )
        rows = cur.fetchall()
    out = []
    for asset_id, raw_key, orig, size, checksum, status, has_vm in rows:
        rel = raw_key[len(LOWER_PREFIX) + 1 :]  # folder/sanitized.mp4
        folder = rel.split("/")[0]
        out.append(
            {
                "asset_id": asset_id,
                "raw_key": raw_key,
                "original_name": orig,
                "file_size": int(size or 0),
                "checksum": checksum or "",
                "ingest_status": status,
                "has_video_metadata": bool(has_vm),
                "folder": folder,
                "gt_class": FOLDER_TO_CLASS[folder],
                "camera_id": camera_id_of(orig),
                "upper_key": f"{UPPER_PREFIX}/{folder}/{orig}",
                "lower_key": raw_key,
            }
        )
    return out


# ────────────────────────────── 1. prompts ──────────────────────────────
def stage_prompts(limit: int | None = None) -> None:
    os.makedirs(PROMPT_DIR, exist_ok=True)
    sess = requests.Session()
    for ver in VERSIONS:
        csv_path = f"{PROMPT_DIR}/text_features_{ver}.csv"
        npz_path = f"{PROMPT_DIR}/{ver}.npz"
        if not os.path.exists(csv_path):
            raise SystemExit(f"프롬프트 CSV 없음: {csv_path}")
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        if limit:
            rows = rows[:limit]
        if os.path.exists(npz_path):
            d = np.load(npz_path, allow_pickle=True)
            if len(d["cls"]) == len(rows):
                log(f"{ver}: npz 이미 존재 (n={len(rows)}) → skip")
                continue
            log(f"{ver}: npz 행수 불일치 ({len(d['cls'])} != {len(rows)}) → 재생성")
        vecs = np.zeros((len(rows), 1024), dtype=np.float32)
        cls = np.zeros(len(rows), dtype=np.int8)
        prompts = []
        t0 = time.time()
        for i, r in enumerate(rows):
            text = r["prompt"]
            resp = sess.post(f"{EMBED_URL}/embed_text", data={"text": text}, timeout=180)
            resp.raise_for_status()
            v = np.asarray(resp.json()["vector"], dtype=np.float32)
            n = float(np.linalg.norm(v))
            if not (0.99 <= n <= 1.01):  # 서비스가 L2 정규화해서 준다는 전제 검증
                v = v / max(n, 1e-9)
            vecs[i] = v
            cls[i] = int(r["class"])
            prompts.append(text)
            if (i + 1) % 2000 == 0:
                log(f"{ver}: {i + 1}/{len(rows)} ({time.time() - t0:.0f}s)")
        np.savez_compressed(npz_path, vec=vecs, cls=cls, prompt=np.array(prompts, dtype=object))
        log(f"{ver}: 저장 {npz_path} n={len(rows)} ({time.time() - t0:.0f}s)")


# ────────────────────────────── 2. media ──────────────────────────────
def _ffprobe(path: str) -> dict:
    cmd = [
        "ffprobe", "-v", "error", "-print_format", "json",
        "-show_format", "-show_streams", path,
    ]
    p = subprocess.run(cmd, capture_output=True, timeout=120, check=False)
    if p.returncode != 0:
        return {}
    try:
        meta = json.loads(p.stdout)
    except json.JSONDecodeError:
        return {}
    v = next((s for s in meta.get("streams", []) if s.get("codec_type") == "video"), {})
    dur = meta.get("format", {}).get("duration") or v.get("duration")
    fps = 0.0
    rate = v.get("avg_frame_rate") or v.get("r_frame_rate") or "0/0"
    try:
        num, den = rate.split("/")
        fps = float(num) / float(den) if float(den) else 0.0
    except (ValueError, ZeroDivisionError):
        fps = 0.0
    return {
        "duration_sec": float(dur) if dur else 0.0,
        "width": int(v.get("width") or 0),
        "height": int(v.get("height") or 0),
        "fps": round(fps, 3),
        "codec": v.get("codec_name") or "",
    }


def _extract_frame(video: str, seek: float, out: str) -> bool:
    """seek 초 지점 1프레임 → out(jpg). fast seek 실패 시 accurate seek 재시도."""
    for pre in (True, False):
        cmd = ["ffmpeg", "-nostdin", "-y", "-loglevel", "error"]
        cmd += (["-ss", f"{seek:.3f}", "-i", video] if pre else ["-i", video, "-ss", f"{seek:.3f}"])
        cmd += ["-frames:v", "1", "-q:v", "2", "-an", "-sn", out]
        p = subprocess.run(cmd, capture_output=True, timeout=300, check=False)
        if p.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 1024:
            return True
    return False


def _do_media(asset: dict, client) -> dict:
    aid = asset["asset_id"]
    rec = {"asset_id": aid, "ok": False, "frames": [], "error": None}
    tmp = os.path.join(tempfile.gettempdir(), f"sourceh_{aid}.mp4")
    try:
        h = hashlib.sha256()
        obj = client.get_object(Bucket=BUCKET, Key=asset["upper_key"])
        got = 0
        with open(tmp, "wb") as f:
            for chunk in obj["Body"].iter_chunks(chunk_size=8 << 20):
                h.update(chunk)
                f.write(chunk)
                got += len(chunk)
        rec["downloaded_bytes"] = got
        rec["sha256"] = h.hexdigest()
        rec["sha256_verified"] = bool(asset["checksum"]) and rec["sha256"] == asset["checksum"]
        rec["size_matches_db"] = got == asset["file_size"]
        rec.update(_ffprobe(tmp))
        dur = rec.get("duration_sec") or 0.0
        seeks = (
            [max(0.0, min(dur - 0.05, dur * fr)) for fr in FRAME_FRACS]
            if dur > 0.5
            else [0.0, 0.5, 1.0]
        )
        for i, sk in enumerate(seeks):
            out = f"{FRAME_DIR}/{aid}_{i}.jpg"
            if os.path.exists(out) and os.path.getsize(out) > 1024:
                rec["frames"].append(out)
            elif _extract_frame(tmp, sk, out):
                rec["frames"].append(out)
        rec["seeks"] = [round(s, 3) for s in seeks]
        rec["ok"] = len(rec["frames"]) > 0
        if not rec["ok"]:
            rec["error"] = "no_frame_extracted"
    except Exception as exc:  # noqa: BLE001 — per-file fail-forward
        rec["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    return rec


def stage_media(limit: int | None = None, workers: int = 3) -> None:
    os.makedirs(FRAME_DIR, exist_ok=True)
    os.makedirs(WORK_DIR, exist_ok=True)
    path = f"{WORK_DIR}/media.jsonl"
    done = jsonl_load(path)
    assets = load_assets()
    if limit:
        assets = assets[:limit]
    todo = [a for a in assets if not done.get(a["asset_id"], {}).get("ok")]
    log(f"media: 전체 {len(assets)} / 완료 {len(assets) - len(todo)} / 남음 {len(todo)}")
    if not todo:
        return
    client = s3()
    n_ok = n_fail = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_do_media, a, client): a for a in todo}
        for i, fut in enumerate(as_completed(futs), 1):
            rec = fut.result()
            jsonl_append(path, rec)
            n_ok, n_fail = (n_ok + 1, n_fail) if rec["ok"] else (n_ok, n_fail + 1)
            if i % 25 == 0 or not rec["ok"]:
                log(f"media: {i}/{len(todo)} ok={n_ok} fail={n_fail} last_err={rec.get('error')}")
    log(f"media 완료: ok={n_ok} fail={n_fail}")


# ────────────────────────────── 3. angle ──────────────────────────────
def _do_angle(rec: dict, sess: requests.Session) -> dict:
    out = {"asset_id": rec["asset_id"], "frames": [], "error": None}
    try:
        for fp in rec["frames"]:
            with open(fp, "rb") as f:
                r = sess.post(
                    f"{ANGLE_URL}/angle",
                    files={"file": (os.path.basename(fp), f, "image/jpeg")},
                    timeout=600,
                )
            r.raise_for_status()
            out["frames"].append(r.json())
    except Exception as exc:  # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {exc}"
        return out
    labels = [f.get("camera_angle") for f in out["frames"]]
    tilts = [f["tilt_deg"] for f in out["frames"] if f.get("tilt_deg") is not None]
    # 다수결. 동수면 production 과 같은 대표프레임(가운데)을 따른다.
    counts = {lab: labels.count(lab) for lab in set(labels)}
    best = max(counts.values())
    winners = [lab for lab, c in counts.items() if c == best]
    mid = labels[len(labels) // 2]
    out["camera_angle"] = mid if len(winners) > 1 else winners[0]
    out["angle_method"] = out["frames"][len(out["frames"]) // 2].get("angle_method") or ""
    out["angle_votes"] = ",".join(f"{lab}x{c}" for lab, c in sorted(counts.items()))
    out["angle_stable"] = len(counts) == 1
    out["tilt_deg"] = round(float(np.median(tilts)), 2) if tilts else None
    out["tilt_deg_frames"] = [f.get("tilt_deg") for f in out["frames"]]
    return out


def stage_angle(limit: int | None = None, workers: int = 4) -> None:
    media = jsonl_load(f"{WORK_DIR}/media.jsonl")
    path = f"{WORK_DIR}/angle.jsonl"
    done = jsonl_load(path)
    todo = [
        r for r in media.values()
        if r.get("ok") and not done.get(r["asset_id"], {}).get("camera_angle")
    ]
    if limit:
        todo = todo[:limit]
    log(f"angle: 남음 {len(todo)} (완료 {len(done)})")
    if not todo:
        return
    sess = requests.Session()
    n_ok = n_fail = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_do_angle, r, sess) for r in todo]
        for i, fut in enumerate(as_completed(futs), 1):
            rec = fut.result()
            jsonl_append(path, rec)
            n_ok, n_fail = (n_ok + 1, n_fail) if not rec["error"] else (n_ok, n_fail + 1)
            if i % 25 == 0 or rec["error"]:
                log(f"angle: {i}/{len(todo)} ok={n_ok} fail={n_fail} err={rec.get('error')}")
    log(f"angle 완료: ok={n_ok} fail={n_fail}")


# ────────────────────────────── 4. embed ──────────────────────────────
def stage_embed(limit: int | None = None) -> None:
    media = jsonl_load(f"{WORK_DIR}/media.jsonl")
    recs = [r for r in media.values() if r.get("ok")]
    if limit:
        recs = recs[:limit]
    path = f"{WORK_DIR}/embed.npz"
    cache: dict[str, np.ndarray] = {}
    if os.path.exists(path):
        d = np.load(path, allow_pickle=True)
        for k, v in zip(d["frame_path"], d["vec"]):
            cache[str(k)] = v
    wanted = [fp for r in recs for fp in r["frames"]]
    todo = [fp for fp in wanted if fp not in cache]
    log(f"embed: 프레임 {len(wanted)} / 캐시 {len(wanted) - len(todo)} / 남음 {len(todo)}")
    if todo:
        sess = requests.Session()
        t0 = time.time()
        for i, fp in enumerate(todo, 1):
            with open(fp, "rb") as f:
                r = sess.post(
                    f"{EMBED_URL}/embed",
                    files={"file": (os.path.basename(fp), f, "image/jpeg")},
                    timeout=600,
                )
            r.raise_for_status()
            v = np.asarray(r.json()["vector"], dtype=np.float32)
            nrm = float(np.linalg.norm(v))
            cache[fp] = v / nrm if nrm > 0 else v
            if i % 200 == 0:
                log(f"embed: {i}/{len(todo)} ({time.time() - t0:.0f}s)")
        keys = list(cache)
        np.savez_compressed(
            path,
            frame_path=np.array(keys, dtype=object),
            vec=np.stack([cache[k] for k in keys]),
        )
    log(f"embed 완료: {len(cache)} 프레임 벡터")


# ────────────────────────────── 5. score ──────────────────────────────
def stage_score() -> None:
    media = jsonl_load(f"{WORK_DIR}/media.jsonl")
    angle = jsonl_load(f"{WORK_DIR}/angle.jsonl")
    d = np.load(f"{WORK_DIR}/embed.npz", allow_pickle=True)
    vec_of = {str(k): v for k, v in zip(d["frame_path"], d["vec"])}
    assets = {a["asset_id"]: a for a in load_assets()}

    banks = {}
    for ver in VERSIONS:
        z = np.load(f"{PROMPT_DIR}/{ver}.npz", allow_pickle=True)
        banks[ver] = {
            "vec": z["vec"].astype(np.float32),
            "cls": z["cls"].astype(int),
            "prompt": [str(p) for p in z["prompt"]],
        }
        log(f"{ver}: 프롬프트 {len(banks[ver]['cls'])} 클래스분포="
            f"{ {int(c): int((banks[ver]['cls'] == c).sum()) for c in sorted(set(banks[ver]['cls']))} }")

    out = []
    for aid, mrec in media.items():
        if not mrec.get("ok") or aid not in assets:
            continue
        frames = [fp for fp in mrec["frames"] if fp in vec_of]
        if not frames:
            continue
        img = np.stack([vec_of[fp] for fp in frames])  # [F,1024]
        a = assets[aid]
        rec = {
            "asset_id": aid,
            "gt_class": a["gt_class"],
            "n_frames": len(frames),
            "frames": frames,
        }
        for ver, bank in banks.items():
            sims = img @ bank["vec"].T  # [F, P] — 코사인(양쪽 L2 정규화됨)
            classes = sorted(set(bank["cls"].tolist()))
            scores, topk_scores, top_prompt, top_frame = {}, {}, {}, {}
            class_best = {}
            for c in classes:
                gidx = np.flatnonzero(bank["cls"] == c)  # 전역 프롬프트 인덱스
                sub = sims[:, gidx]  # [F, Pc]
                flat = sub.ravel()
                k = min(TOPK, flat.size)
                scores[c] = float(flat.max())
                topk_scores[c] = float(np.sort(flat)[-k:].mean())
                fi, pi = np.unravel_index(int(sub.argmax()), sub.shape)
                top_prompt[c] = bank["prompt"][int(gidx[pi])]
                top_frame[c] = frames[int(fi)]
                # 클래스별 1위를 4클래스 전부 보존 — 예측 클래스만 남기면 "왜 이 점수인지"를 못 본다
                class_best[int(c)] = {
                    "cos": scores[c],
                    "prompt": top_prompt[c],
                    "frame_idx": int(fi),
                    "topk_mean": topk_scores[c],
                }
            # 뱅크 전체(클래스 무관) 코사인 상위 N — 이 이미지가 모델에게 "무엇으로 보이는지"
            flat_all = sims.ravel()
            kk = min(TOP_SHOW, flat_all.size)
            sel = np.argpartition(-flat_all, kk - 1)[:kk]
            sel = sel[np.argsort(-flat_all[sel])]
            top_overall = []
            for rank, ix in enumerate(sel, 1):
                fi2, pi2 = divmod(int(ix), sims.shape[1])
                top_overall.append(
                    {
                        "rank": rank,
                        "cos": float(flat_all[ix]),
                        "cls": int(bank["cls"][pi2]),
                        "prompt": bank["prompt"][pi2],
                        "frame_idx": fi2,
                    }
                )
            ranked = sorted(scores.items(), key=lambda kv: -kv[1])
            pred = ranked[0][0]
            margin = ranked[0][1] - ranked[1][1] if len(ranked) > 1 else 0.0
            pred_topk = max(topk_scores.items(), key=lambda kv: kv[1])[0]
            rec[ver] = {
                "scores": {int(c): scores[c] for c in classes},
                "topk_scores": {int(c): topk_scores[c] for c in classes},
                "pred": int(pred),
                "confidence": scores[pred],
                "margin": float(margin),
                "top_prompt": top_prompt[pred],
                "top_frame": top_frame[pred],
                "pred_topk": int(pred_topk),
                "correct": int(pred) == a["gt_class"],
                "correct_topk": int(pred_topk) == a["gt_class"],
                "class_best": class_best,
                "top_overall": top_overall,
            }
        v0, v4 = rec[VERSIONS[0]], rec[VERSIONS[1]]
        rec["outcome"] = (
            "both_correct" if v0["correct"] and v4["correct"]
            else "only_v1.0.8.4" if v4["correct"]
            else "only_v1.0.8.0" if v0["correct"]
            else "both_wrong"
        )
        arec = angle.get(aid, {})
        rec["camera_angle"] = arec.get("camera_angle")
        rec["angle_method"] = arec.get("angle_method")
        rec["angle_votes"] = arec.get("angle_votes")
        rec["angle_stable"] = arec.get("angle_stable")
        rec["tilt_deg"] = arec.get("tilt_deg")
        out.append(rec)
    with open(f"{WORK_DIR}/scores.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    log(f"score 완료: {len(out)} assets → {WORK_DIR}/scores.json")


# ────────────────────────────── 6. dbwrite ──────────────────────────────
def stage_dbwrite() -> None:
    angle = jsonl_load(f"{WORK_DIR}/angle.jsonl")
    rows = [
        (r["asset_id"], r["camera_angle"], r["angle_method"])
        for r in angle.values()
        if r.get("camera_angle")
    ]
    if not rows:
        log("dbwrite: 쓸 값 없음")
        return
    with pg() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='video_metadata' AND column_name IN ('camera_angle','angle_method')"
        )
        have = {r[0] for r in cur.fetchall()}
        if {"camera_angle", "angle_method"} - have:
            raise SystemExit("migration 017 미적용 — psql 로 먼저 적용할 것")
        cur.executemany(
            """
            UPDATE video_metadata vm
               SET camera_angle = %s, angle_method = %s
             WHERE vm.asset_id = %s
               AND EXISTS (SELECT 1 FROM raw_files r
                            WHERE r.asset_id = vm.asset_id AND r.raw_key LIKE 'source-h/%%')
            """,
            [(ca, am, aid) for aid, ca, am in rows],
        )
        n = cur.rowcount
        conn.commit()
        cur.execute(
            """
            SELECT vm.camera_angle, COUNT(*)
              FROM video_metadata vm JOIN raw_files r ON r.asset_id = vm.asset_id
             WHERE r.raw_key LIKE 'source-h/%%'
             GROUP BY 1 ORDER BY 2 DESC
            """
        )
        dist = cur.fetchall()
    log(f"dbwrite: UPDATE 시도 {len(rows)} (마지막 rowcount={n}) 분포={dist}")


# ────────────────────────────── 7. build ──────────────────────────────
def stage_build(dataset_name: str = "source-h") -> None:
    import fiftyone as fo
    from fiftyone import ViewField as F

    v0, v4 = (v.replace(".", "_") for v in VERSIONS)  # 필드명 접미사 (샘플 루프에서부터 필요)

    with open(f"{WORK_DIR}/scores.json", encoding="utf-8") as f:
        scores = json.load(f)
    media = jsonl_load(f"{WORK_DIR}/media.jsonl")
    assets = {a["asset_id"]: a for a in load_assets()}
    lower_sizes = _minio_sizes(LOWER_PREFIX)

    if dataset_name in fo.list_datasets():
        fo.delete_dataset(dataset_name)
        log(f"기존 데이터셋 {dataset_name} 삭제 후 재생성")
    ds = fo.Dataset(dataset_name, persistent=True)
    ds.description = (
        "source-h 871편(MinIO source-h/ + source-h/ 두 prefix 전용) — camera_angle(DAv2) + "
        "userwatch 프롬프트 뱅크 v1.0.8.0 vs v1.0.8.4 제로샷 비교. "
        "GT=폴더명 파생 weak GT(사람검수 아님). 샘플=영상 1편, 이미지=가운데 키프레임."
    )
    ds.tags = ["source-h", "prompt-version-eval", "camera-angle"]

    samples = []
    for rec in scores:
        aid = rec["asset_id"]
        a, m = assets[aid], media[aid]
        frames = rec["frames"]
        sample = fo.Sample(filepath=frames[len(frames) // 2])
        sample["asset_id"] = aid
        sample["raw_key"] = a["raw_key"]
        sample["original_name"] = a["original_name"]
        sample["folder"] = a["folder"]
        sample["camera_id"] = a["camera_id"]
        sample["ingest_status"] = a["ingest_status"]
        sample["media_source_key"] = a["upper_key"]
        # 67건 이슈: 정규화 키 객체 바이트가 DB/원본과 다른가
        low = lower_sizes.get(a["lower_key"])
        sample["lower_key_bytes_match"] = (low == a["file_size"]) if low is not None else None
        sample["sha256_verified"] = bool(m.get("sha256_verified"))
        sample["duration_sec"] = round(float(m.get("duration_sec") or 0), 2)
        sample["width"] = int(m.get("width") or 0)
        sample["height"] = int(m.get("height") or 0)
        sample["resolution"] = f"{m.get('width')}x{m.get('height')}"
        sample["fps"] = float(m.get("fps") or 0)
        sample["codec"] = m.get("codec") or ""
        sample["file_size_mb"] = round(a["file_size"] / 1e6, 2)
        sample["n_frames_scored"] = rec["n_frames"]
        sample["keyframe_paths"] = frames

        sample["ground_truth"] = fo.Classification(label=CLASS_NAMES[rec["gt_class"]])
        if rec.get("camera_angle"):
            sample["camera_angle"] = fo.Classification(label=rec["camera_angle"])
            sample["tilt_deg"] = rec.get("tilt_deg")
            sample["angle_method"] = rec.get("angle_method")
            sample["angle_votes"] = rec.get("angle_votes")
            sample["angle_stable"] = rec.get("angle_stable")
            # camera_angle 은 870:1 단색이고 tilt_deg 는 고유값 628개(=연속값)라 App 이
            # 카테고리 색상을 못 만든다. 5도 구간으로 묶어 **색칠·층화 가능한 축**을 만든다.
            # 구간 폭 5도는 실측 프레임간 노이즈(p90 2.07도)보다 충분히 크다.
            sample["tilt_bin"] = fo.Classification(label=_tilt_bin(rec.get("tilt_deg")))

        for ver in VERSIONS:
            r = rec[ver]
            tag = ver.replace(".", "_")  # v1_0_8_0
            sample[f"pred_{tag}"] = fo.Classification(
                label=CLASS_NAMES[r["pred"]],
                confidence=r["confidence"],
                margin=r["margin"],
                top_prompt=r["top_prompt"],
            )
            sample[f"pred_topk_{tag}"] = fo.Classification(label=CLASS_NAMES[r["pred_topk"]])
            sample[f"correct_{tag}"] = bool(r["correct"])
            sample[f"margin_{tag}"] = r["margin"]
            # 사이드바에서 바로 보이고 필터되도록 top-level 문자열로도 남긴다
            sample[f"top_prompt_{tag}"] = r["top_prompt"]
            sample[f"top_frame_{tag}"] = r["top_frame"]

            # ── 프롬프트↔이미지 코사인 유사도를 눈으로 볼 수 있게 ──
            # (1) 클래스별 1위 프롬프트 4개 = 그 클래스 점수의 근거
            sample[f"class_best_{tag}"] = fo.Classifications(
                classifications=[
                    fo.Classification(
                        label=CLASS_NAMES[int(c)],
                        confidence=cb["cos"],
                        prompt=cb["prompt"],
                        frame_idx=cb["frame_idx"],
                        topk_mean=cb["topk_mean"],
                    )
                    for c, cb in sorted(
                        r["class_best"].items(), key=lambda kv: -kv[1]["cos"]
                    )
                ]
            )
            # (2) 뱅크 전체에서 가장 닮은 프롬프트 top-10 = 모델이 이 화면을 무엇으로 보는지
            sample[f"top{TOP_SHOW}_{tag}"] = fo.Classifications(
                classifications=[
                    fo.Classification(
                        label=CLASS_NAMES[t["cls"]],
                        confidence=t["cos"],
                        prompt=t["prompt"],
                        rank=t["rank"],
                        frame_idx=t["frame_idx"],
                    )
                    for t in r["top_overall"]
                ]
            )
            # 모달에서 스크롤 없이 읽히는 평문 버전 (cos / class / prompt)
            sample[f"top{TOP_SHOW}_text_{tag}"] = "\n".join(
                f"{t['rank']:2d}. {t['cos']:.4f} [{CLASS_NAMES[t['cls']]}] f{t['frame_idx']} {t['prompt'].strip()}"
                for t in r["top_overall"]
            )
            for c, v in r["scores"].items():
                sample[f"score_{tag}_{CLASS_NAMES[int(c)]}"] = float(v)
        sample["outcome"] = rec["outcome"]
        # ⚠️ conf_delta 는 뱅크 스케일에 오염돼 있다 — v1.0.8.4 는 3문장 템플릿이라 코사인이
        # 체계적으로 낮다(top1 평균 0.2724 vs 0.2912, 871편 중 v084 가 높은 건 88편뿐).
        # "자신감 하락" 으로 읽으면 안 된다. 스케일 제거 버전은 아래 dscore_* 를 쓸 것.
        sample["conf_delta"] = round(
            rec[VERSIONS[1]]["confidence"] - rec[VERSIONS[0]]["confidence"], 5
        )

        # ── 버전 간 비교용 스케일 정규화 (centering) ──
        # 표본 내 4클래스 평균을 빼면 뱅크별·이미지별 **가산(additive) 오프셋**이 상쇄된다.
        # argmax 는 centering 에 불변이므로 예측/정확도는 바뀌지 않는다.
        # ⚠️ 한계(과대선전 금지): 이건 **캘리브레이션이 아니다**.
        #   · 가산 오프셋만 제거한다 — 뱅크 간 코사인 분산 차이(승법 스케일: 표준편차
        #     0.0362 vs 0.0273)는 남아 있다.
        #   · 4개 dscore 의 합은 0 이므로 **독립 자유도는 3개**다. 4개를 독립 지표로 읽지 말 것.
        cen = {}
        for ver in VERSIONS:
            sc = {int(c): float(v) for c, v in rec[ver]["scores"].items()}
            mean_c = sum(sc.values()) / len(sc)
            cen[ver] = {c: v - mean_c for c, v in sc.items()}
        for c in sorted(cen[VERSIONS[0]]):
            sample[f"dscore_{CLASS_NAMES[c]}"] = round(
                cen[VERSIONS[1]][c] - cen[VERSIONS[0]][c], 5
            )

        # ── trade-off 축 ──
        # GT 클래스에 대한 centering 점수를 버전별로 남긴다. (v080, v084) 산점도의 좌표가 되고
        # 대각선 y=x 아래는 퇴행, 위는 개선 → "무엇을 얻고 무엇을 잃었나" 가 한 화면에 보인다.
        gt_c = rec["gt_class"]
        sample[f"gt_rel_{v0}"] = round(cen[VERSIONS[0]][gt_c], 5)
        sample[f"gt_rel_{v4}"] = round(cen[VERSIONS[1]][gt_c], 5)
        sample["gt_rel_delta"] = round(cen[VERSIONS[1]][gt_c] - cen[VERSIONS[0]][gt_c], 5)

        p0_l = CLASS_NAMES[rec[VERSIONS[0]]["pred"]]
        p4_l = CLASS_NAMES[rec[VERSIONS[1]]["pred"]]
        gt_l = CLASS_NAMES[gt_c]
        sample["pred_changed"] = p0_l != p4_l
        # 사이드바에서 이 필드의 값별 개수가 곧 **전이표**가 된다 (클릭하면 해당 샘플만 필터)
        # 구분자로 '|' 를 쓰면 안 된다 — 리포트 마크다운 표의 열 구분자와 충돌해 표가 깨진다
        sample["transition"] = fo.Classification(
            label=(f"GT {gt_l} : {p0_l}→{p4_l}" if p0_l != p4_l else f"GT {gt_l} : ={p0_l}")
        )
        # ── GT 무관: "프롬프트 버전이 달라지면서 예측이 어떻게 바뀌었나" 만 ──
        # transition 은 GT 가 접두어라 같은 변화(smoke→normal)가 GT별로 쪼개진다(21범주).
        # 정답 여부와 무관하게 **변화 자체**를 보려면 이 필드를 쓴다(11범주).
        sample["pred_shift"] = fo.Classification(
            label=(f"{p0_l}→{p4_l}" if p0_l != p4_l else f"={p0_l}")
        )
        # ── 변화의 **방향** — 실측상 이게 크기보다 강한 신호다 ──
        # (Q1·Q2 제외 750편) normal→이벤트 전환은 v080 정답 0.0% → v084 85.5%,
        # 반대로 이벤트→다른것 전환은 75.0% → 19.2%. 즉 "얼마나 움직였나"보다
        # "어느 방향으로 움직였나"가 채택 여부를 결정한다.
        NORMAL = CLASS_NAMES[0]
        if p0_l == p4_l:
            _dir = "변화없음"
        elif p0_l == NORMAL:
            _dir = "회수 (normal→이벤트)"  # v080 이 놓친 것을 v084 가 잡음
        elif p4_l == NORMAL:
            _dir = "상실 (이벤트→normal)"  # v080 이 잡던 것을 v084 가 놓침
        else:
            _dir = "오분류 (이벤트→다른이벤트)"  # 이벤트는 유지, 클래스만 바뀜
        sample["shift_direction"] = fo.Classification(label=_dir)

        # 변화의 크기도 GT 없이 잰다:
        #   · dscore_pred_v080 = v084 가 **옛 답**에서 얼마나 멀어졌나 (음수면 멀어짐)
        #   · dscore_pred_v084 = v084 가 **새 답**으로 얼마나 당겼나 (양수면 당김)
        p0_c, p4_c = rec[VERSIONS[0]]["pred"], rec[VERSIONS[1]]["pred"]
        sample["dscore_pred_v080"] = round(cen[VERSIONS[1]][p0_c] - cen[VERSIONS[0]][p0_c], 5)
        sample["dscore_pred_v084"] = round(cen[VERSIONS[1]][p4_c] - cen[VERSIONS[0]][p4_c], 5)

        lines = [f"{'class':9s} {'v080':>8s} {'v084':>8s} | {'v080_rel':>9s} {'v084_rel':>9s} {'Δrel':>8s}"]
        for c in sorted(cen[VERSIONS[0]], key=lambda x: -cen[VERSIONS[1]][x]):
            mark = ""
            if c == gt_c:
                mark += " ←GT"
            if c == rec[VERSIONS[0]]["pred"]:
                mark += " p080"
            if c == rec[VERSIONS[1]]["pred"]:
                mark += " p084"
            lines.append(
                f"{CLASS_NAMES[c]:9s} {rec[VERSIONS[0]]['scores'][str(c)]:8.4f} "
                f"{rec[VERSIONS[1]]['scores'][str(c)]:8.4f} | "
                f"{cen[VERSIONS[0]][c]:+9.4f} {cen[VERSIONS[1]][c]:+9.4f} "
                f"{cen[VERSIONS[1]][c] - cen[VERSIONS[0]][c]:+8.4f}{mark}"
            )
        sample["sim_compare"] = "\n".join(lines)
        sample.tags = [rec["outcome"], a["folder"]]
        samples.append(sample)

    ds.add_samples(samples)
    log(f"build: {len(samples)} 샘플 적재")

    # ── 변화 크기 = shift_viz 산점도에서 대각선까지의 거리 ──
    # 예측이 안 바뀐 샘플은 dscore_pred_v080 == dscore_pred_v084 라 정확히 y=x 위에 놓인다.
    # 따라서 |차이| 가 곧 "버전이 이 영상의 판단을 얼마나 흔들었나" 다.
    # 연속값은 App 이 색을 못 만들므로(고유값 수백) **5분위 범주**를 같이 만든다 →
    # `emb_viz`(이미지 임베딩 UMAP)를 이걸로 색칠하면 "어떤 화면이 흔들렸나" 가 보인다.
    d80 = np.asarray(ds.values("dscore_pred_v080"), dtype=float)
    d84 = np.asarray(ds.values("dscore_pred_v084"), dtype=float)
    mag = np.abs(d84 - d80)
    # ⚠️ 예측이 안 바뀐 샘플은 mag 가 **정확히 0** 이고 그게 72%(629/871)다 → 전체를 5분위로
    #    나누면 경계가 [0,0,0,...] 로 붕괴한다. 0 을 별도 범주로 빼고 **바뀐 것만** 4분위.
    nz = mag[mag > 0]
    cuts = np.quantile(nz, [0.25, 0.5, 0.75]) if len(nz) else np.array([0.0])
    qnames = ["Q1 작게", "Q2", "Q3", "Q4 크게"]
    ids = ds.values("id")
    ds.set_values("shift_mag", dict(zip(ids, [round(float(m), 5) for m in mag])), key_field="id")
    ds.set_values(
        "shift_mag_q",
        {
            i: fo.Classification(
                label="변화없음"
                if m <= 0
                else qnames[int(np.searchsorted(cuts, m, side="right"))]
            )
            for i, m in zip(ids, mag)
        },
        key_field="id",
    )
    log(f"build: shift_mag / shift_mag_q 기록 (변화 {len(nz)}편의 4분위 경계 "
        f"{np.round(cuts, 4).tolist()}, 나머지 {len(mag) - len(nz)}편은 '변화없음')")

    # 분석 범위 표식 — 저마진 전환(Q1·Q2)은 마진이 v084 코사인 표준편차(0.0273)의 12~17%
    # 수준이라 뒤집힘이 노이즈·GT 품질에 지배된다. 운영자 판단으로 분석에서 제외하는 구간.
    qlab = ds.values("shift_mag_q.label")
    ds.set_values(
        "flip_confidence",
        {
            i: fo.Classification(
                label="변화없음"
                if ql == "변화없음"
                else ("저마진 전환 (분석제외)" if ql in ("Q1 작게", "Q2") else "확신 전환")
            )
            for i, ql in zip(ids, qlab)
        },
        key_field="id",
    )
    log("build: flip_confidence 기록 (저마진 전환 = Q1·Q2, 분석제외 표식)")

    # 이미지 임베딩(가운데 프레임) — user-embeddings 플러그인/브레인용
    d = np.load(f"{WORK_DIR}/embed.npz", allow_pickle=True)
    vec_of = {str(k): v for k, v in zip(d["frame_path"], d["vec"])}
    ids, embs = [], []
    for s in ds.select_fields(["id", "filepath"]):
        v = vec_of.get(s.filepath)
        if v is not None:
            ids.append(s.id)
            embs.append(v.tolist())
    ds.set_values("embedding", dict(zip(ids, embs)), key_field="id")
    log(f"build: embedding {len(ids)}건 기록")

    for ver in VERSIONS:
        tag = ver.replace(".", "_")
        try:
            ds.evaluate_classifications(
                f"pred_{tag}", gt_field="ground_truth", eval_key=f"eval_{tag}", method="simple"
            )
            log(f"build: evaluation eval_{tag} 완료")
        except Exception as exc:  # noqa: BLE001 — 평가 실패가 데이터셋을 막지 않게
            log(f"build: evaluation eval_{tag} 실패 {type(exc).__name__}: {exc}")

    # Classification 의 동적 속성(margin/top_prompt)을 App 사이드바에 노출
    try:
        ds.add_dynamic_sample_fields()
    except Exception as exc:  # noqa: BLE001
        log(f"build: add_dynamic_sample_fields 실패 {type(exc).__name__}: {exc}")

    try:
        import fiftyone.brain as fob

        # brain_key 는 반드시 이 배포의 관례인 `emb_viz` — Embeddings 패널은 마지막에 쓰던
        # 브레인 키를 데이터셋 간에 기억하므로, frames_captions 등과 이름이 다르면 패널이
        # "no brain method run key 'emb_viz'" 로 죽고 그 상태에선 Color by 도 안 뜬다.
        # 필드명으로 넘겨 샘플 순서 정렬 문제도 원천 배제 (repo 관례: fiftyone_umap_only.py)
        res = fob.compute_visualization(
            ds, embeddings="embedding", brain_key="emb_viz", method="umap"
        )
        # 별칭 `umap` — 이미 그 키를 골라둔 패널/세션이 깨지지 않게 **같은 좌표**로 등록한다
        # (재계산하면 UMAP 이 비결정적이라 두 키가 다른 그림이 된다).
        if not ds.has_brain_run("umap"):
            fob.compute_visualization(ds, points=res.points, brain_key="umap")
        log("build: UMAP 시각화 완료 (brain_key=emb_viz + 별칭 umap, 동일 좌표)")
    except Exception as exc:  # noqa: BLE001
        log(f"build: UMAP 실패 {type(exc).__name__}: {exc}")

    # ── trade-off 산점도 ──
    # compute_visualization 은 임의 2D 좌표(points=)를 받는다 → 차원축소가 아니라
    # **before/after 플롯**으로 재활용한다. x=v1.0.8.0, y=v1.0.8.4 의 GT클래스 상대점수.
    #   · 대각선 y=x 위  = v1.0.8.4 가 정답 클래스를 더 당김 (개선)
    #   · 대각선 아래     = 퇴행
    #   · 좌하 사분면     = 두 버전 다 정답 클래스를 못 당김 (구조적 난이도)
    # Embeddings 패널에서 brain key 로 고르면 되고, 올가미 선택 → 이미지가 바로 뜬다.
    try:
        import fiftyone.brain as fob

        xs = np.asarray(ds.values(f"gt_rel_{v0}"), dtype=np.float64)
        ys = np.asarray(ds.values(f"gt_rel_{v4}"), dtype=np.float64)
        if ds.has_brain_run("tradeoff_viz"):
            ds.delete_brain_run("tradeoff_viz")
        fob.compute_visualization(
            ds, points=np.stack([xs, ys], axis=1), brain_key="tradeoff_viz"
        )
        log(f"build: trade-off 산점도 완료 (x={f'gt_rel_{v0}'}, y={f'gt_rel_{v4}'}, "
            f"대각선 위=개선 / 아래=퇴행)")
    except Exception as exc:  # noqa: BLE001
        log(f"build: trade-off 산점도 실패 {type(exc).__name__}: {exc}")

    # ── 버전변화 산점도 (GT 무관) ──
    # x = 옛 답에서 멀어진 정도, y = 새 답으로 당긴 정도.
    #   · 좌상(x<0, y>0) = 옛 답을 버리고 새 답으로 옮겼다 = 실제로 바뀐 것
    #   · 원점 근처       = 버전이 달라져도 아무것도 안 움직였다
    # 정답 여부가 개입하지 않으므로 "무엇이 바뀌었나" 만 본다.
    try:
        import fiftyone.brain as fob

        xs = np.asarray(ds.values("dscore_pred_v080"), dtype=np.float64)
        ys = np.asarray(ds.values("dscore_pred_v084"), dtype=np.float64)
        if ds.has_brain_run("shift_viz"):
            ds.delete_brain_run("shift_viz")
        fob.compute_visualization(
            ds, points=np.stack([xs, ys], axis=1), brain_key="shift_viz"
        )
        log("build: 버전변화 산점도 완료 (x=옛답에서 멀어진 정도, y=새답으로 당긴 정도)")
    except Exception as exc:  # noqa: BLE001
        log(f"build: 버전변화 산점도 실패 {type(exc).__name__}: {exc}")

    # App 검색바에서 임의 문장 → 이미지 코사인 랭킹 (텍스트측은 /embed_text, 같은 1024-d 공간)
    try:
        sys.path.insert(0, "/workspace")
        import fiftyone_pgvector as fp

        fp.build_text_search_index(ds, brain_key="text_search")
        log("build: text_search(prompt-capable) 인덱스 완료")
    except Exception as exc:  # noqa: BLE001
        log(f"build: text_search 실패 {type(exc).__name__}: {exc}")

    # 모든 뷰가 정리된 스키마를 물려받게 base 에서 노이즈 필드를 제외한다.
    base = ds.exclude_fields(SIDEBAR_NOISE_FIELDS)
    views = {
        # 분석 진입점 — 이 뷰를 고르면 사이드바가 실제로 정리된다
        "00_analysis": base,
        "01_disagreement": base.match(F("outcome") != "both_correct").sort_by("outcome"),
        # ⚠️ conf_delta 로 정렬하면 안 된다 — 뱅크 스케일에 오염된 값이라 정렬 자체가
        # "v084 가 자신없어졌다" 는 오독을 유도한다. 스케일 제거된 gt_rel_delta 로 정렬한다.
        "02_fixed_by_v1084": base.match(F("outcome") == "only_v1.0.8.4").sort_by(
            "gt_rel_delta", True
        ),
        "03_regressed_in_v1084": base.match(F("outcome") == "only_v1.0.8.0"),
        "04_both_wrong": base.match(F("outcome") == "both_wrong"),
        "05_gt_x_angle": base.sort_by("folder").sort_by("camera_angle.label"),
        "06_low_margin_v1084": base.match(F(f"margin_{v4}") < 0.01).sort_by(f"margin_{v4}"),
        "07_falldown_cohort": base.match(F("folder") == "falldown"),
        "08_lower_key_byte_mismatch": base.match(F("lower_key_bytes_match") == False),  # noqa: E712
        "09_angle_unstable": base.match(F("angle_stable") == False),  # noqa: E712
        # ── trade-off 전용 뷰 ──
        # 예측이 바뀐 것만: 이 안에서 transition 사이드바를 보면 곧 전이표다
        "10_tradeoff_changed": base.match(F("pred_changed") == True).sort_by(  # noqa: E712
            "gt_rel_delta"
        ),
        # 잃은 것: GT 클래스 상대점수가 가장 많이 떨어진 순
        "11_tradeoff_lost": base.match(F("gt_rel_delta") < 0).sort_by("gt_rel_delta"),
        # 얻은 것: 가장 많이 올라간 순
        "12_tradeoff_gained": base.match(F("gt_rel_delta") > 0).sort_by("gt_rel_delta", True),
        # fire 를 얻고 smoke 를 잃은 핵심 교환 (이번 버전차의 본질)
        "13_smoke_to_fire": base.match(
            (F("ground_truth.label") == "smoke") & (F(f"pred_{v4}.label") == "fire")
        ),
    }
    # 방향별 뷰 — 회수/상실/오분류를 바로 검수
    for i, lab in enumerate(["회수 (normal→이벤트)", "상실 (이벤트→normal)",
                             "오분류 (이벤트→다른이벤트)"], start=17):
        slug = {"회수 (normal→이벤트)": "recover", "상실 (이벤트→normal)": "lose",
                "오분류 (이벤트→다른이벤트)": "swap"}[lab]
        views[f"{i}_dir_{slug}"] = base.match(
            F("shift_direction.label") == lab
        ).sort_by("shift_mag", True)
    # 운영자 판단으로 분석에서 제외한 저마진 전환을 뺀 범위
    views["20_analysis_scope"] = base.match(
        F("flip_confidence.label") != "저마진 전환 (분석제외)"
    )

    # 가장 많이 일어난 예측 변화 top-3 을 원클릭 뷰로 (하드코딩 아님 — 데이터에서 뽑는다)
    shift_counts = collections.Counter(
        lab for lab in ds.values("pred_shift.label") if lab and "→" in lab
    )
    for i, (lab, n) in enumerate(shift_counts.most_common(3), start=14):
        views[f"{i}_shift_{lab.replace('→', '_to_')}"] = base.match(
            F("pred_shift.label") == lab
        ).sort_by("dscore_pred_v084", True)
        log(f"build: 변화 뷰 {i}_shift_{lab.replace('→', '_to_')} ({n}편)")

    for name, view in views.items():
        try:
            ds.save_view(name, view)
        except Exception as exc:  # noqa: BLE001
            log(f"build: view {name} 실패 {type(exc).__name__}: {exc}")
    log(f"build: saved views {list(views)}")
    ds.save()
    _configure_sidebar(ds)
    _save_workspace(ds)
    log(f"build 완료 → http://10.0.0.10:5153/datasets/{dataset_name}")


def _tilt_bin(tilt: float | None) -> str:
    """tilt_deg → 5도 구간 라벨. 정렬되도록 zero-pad. None 이면 'unknown'."""
    if tilt is None:
        return "unknown"
    if tilt >= 30:
        return "30+° (plan_view)"
    lo = int(tilt // 5) * 5
    return f"{lo:02d}-{lo + 5:02d}°"


def _save_workspace(ds) -> None:
    """Samples + Embeddings 를 **좌우 분할**한 워크스페이스를 저장.

    기본 레이아웃은 둘이 같은 공간의 **탭**이라 점을 골라도 이미지를 동시에 못 본다.
    App 우상단 레이아웃 아이콘에서 이 워크스페이스를 고르면 선택↔이미지가 연동된다.
    """
    import fiftyone as fo

    # ⚠️ 워크스페이스 이름은 slug 로 변환되므로 **ASCII 여야** 한다 (한글은 빈 슬러그 → ValueError)
    for name, brain, color_by in (
        # trade-off 를 보는 기본 화면: before/after 산점도 + outcome 색
        # GT 무관 버전변화 — "무엇이 바뀌었나" 가 1차 질문일 때
        ("shift", "shift_viz", "pred_shift.label"),
        # 이미지 임베딩 UMAP 을 변화 크기로 색칠 → "어떤 화면이 흔들렸나"
        ("shift-where", "emb_viz", "shift_mag_q.label"),
        # 회수/상실이 이미지 임베딩 공간에서 어디에 있나
        ("shift-direction", "emb_viz", "shift_direction.label"),
        ("tradeoff", "tradeoff_viz", "outcome"),
        ("angle-explore", "emb_viz", "tilt_bin.label"),
        ("outcome-explore", "emb_viz", "outcome"),
    ):
        try:
            space = fo.Space(
                children=[
                    fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
                    fo.Space(
                        children=[
                            fo.Panel(
                                type="Embeddings",
                                state={"brainResult": brain, "colorByField": color_by},
                            )
                        ]
                    ),
                ],
                orientation="horizontal",
            )
            if name in ds.list_workspaces():
                ds.delete_workspace(name)
            ds.save_workspace(
                name, space, description=f"Samples ↔ {brain} 분할 (색: {color_by})"
            )
            log(f"build: 워크스페이스 '{name}' 저장 (brain={brain}, color={color_by})")
        except Exception as exc:  # noqa: BLE001
            log(f"build: 워크스페이스 '{name}' 실패 {type(exc).__name__}: {exc}")


def _configure_sidebar(ds) -> None:
    """App 사이드바를 의미 그룹으로 접는다. 평평하게 두면 primitives 30개가 쏟아져 분석 불가.

    기본 그룹(default_sidebar_groups)에서 클래스 타입을 얻어 재구성한다 — 클래스 import 경로를
    추측하지 않기 위함. 어느 그룹에도 안 넣은 경로는 사이드바에서 사라지므로, 남은 건
    '기타' 로 몰아 **아무것도 조용히 없어지지 않게** 한다.
    """
    import fiftyone as fo

    defaults = fo.DatasetAppConfig.default_sidebar_groups(ds)
    G = type(defaults[0])
    universe = [p for g in defaults for p in g.paths]
    v0, v4 = (v.replace(".", "_") for v in VERSIONS)

    CLS = ("normal", "falldown", "fire", "smoke")
    # 필터로 실제 쓸모 있는 것만 남긴다 (실측 카디널리티 근거, 871 샘플):
    #   · 상수(고유값 1) 는 필터가 만들어질 수 없다 → 전부 제외
    #     angle_method/resolution/width/height/codec/n_frames_scored/ingest_status/
    #     sha256_verified/last_modified_at, metadata.*(전부 null)
    #   · 고카디널리티 ID·경로(871~2613 고유)는 패싯이 안 된다 → 제외
    #     id/filepath/created_at/keyframe_paths/top_frame_*/embedding(1024-d)
    #   · eval_* 는 correct_* 와 871/871 동일(완전 중복) → correct_* 만 남긴다.
    #     App 평가 패널은 evaluation registry 를 읽으므로 영향 없다.
    #   · score_* 8개는 **절대 코사인이고 버전 간 스케일이 다르다**(v084 top1 평균 0.2724 vs
    #     0.2912). "fire > 0.28" 같은 필터가 버전별로 다른 의미가 되므로 펼치지 않는다.
    #     버전 비교는 스케일 제거된 dscore_*(표본 내 centering 차) 와 버전 내 상대량 margin_* 로.
    layout: list[tuple[str, bool, list[str]]] = [
        # ── 펼침: 실제 분석 작업면 (17 경로) ──
        ("① 판정", True, ["outcome", "ground_truth", f"correct_{v0}", f"correct_{v4}"]),
        # ② 는 **GT 무관** — "버전이 달라지면서 무엇이 바뀌었나" 만. pred_shift 의 값별 개수가
        #    곧 변화표(11범주)다. transition(GT 접두어, 21범주)은 정답 기준 분석용이라 ③ 으로.
        ("② 버전변화 (GT 무관)", True, ["shift_direction", "flip_confidence", "pred_shift",
                                        "pred_changed", "shift_mag_q", "shift_mag",
                                        "dscore_pred_v080", "dscore_pred_v084"]),
        ("③ 정답 기준 trade-off", True, ["transition", "gt_rel_delta",
                                         f"gt_rel_{v0}", f"gt_rel_{v4}"]),
        ("④ 버전차 근거", True, [f"dscore_{c}" for c in CLS]
                                + [f"margin_{v0}", f"margin_{v4}",
                                   f"top_prompt_{v0}", f"top_prompt_{v4}"]),
        # folder 는 ground_truth.label 과 **1:1 완전 중복**(helmet→normal, 나머지 동일)이라
        # 필터에서 빼고 ⑧ provenance 로 내렸다. 층화는 ground_truth 로 한다.
        ("⑤ 층화·교란", True, ["camera_id", "tilt_bin", "tilt_deg", "duration_sec"]),
        # ── 접힘: 필요할 때 펼쳐 보는 것 ──
        ("⑥ 예측 상세", False, [f"pred_{v0}", f"pred_{v4}",
                                f"pred_topk_{v0}", f"pred_topk_{v4}"]),
        ("⑦ 원점수 (버전간 직접비교 금지)", False,
         [f"score_{t}_{c}" for t in (v0, v4) for c in CLS]),
        ("⑧ 프롬프트 상세 (읽기용)", False,
         ["sim_compare", f"class_best_{v0}", f"class_best_{v4}",
          f"top{TOP_SHOW}_{v0}", f"top{TOP_SHOW}_{v4}",
          f"top{TOP_SHOW}_text_{v0}", f"top{TOP_SHOW}_text_{v4}"]),
        ("⑨ QA·무결성", False, ["angle_stable", "lower_key_bytes_match", "camera_angle"]),
        ("⑩ 조회 키·provenance", False, ["folder", "original_name", "raw_key", "asset_id"]),
    ]

    groups, assigned = [], set()
    for name, expanded, paths in layout:
        keep = [p for p in paths if p in universe or any(u.startswith(p + ".") for u in universe)]
        # Classification/Classifications 는 서브경로(.label/.confidence)도 함께 넣어야 필터가 산다
        subs = [u for u in universe if any(u.startswith(p + ".") for p in keep)]
        full = keep + [s for s in subs if s not in keep]
        if full:
            groups.append(G(name=name, paths=full, expanded=expanded))
            assigned.update(full)

    # tags 그룹은 맨 위로(검수 중 태깅용).
    for g in defaults:
        if g.name in ("tags", "label tags"):
            groups.insert(0, g)
            assigned.update(g.paths)
    # metadata.* 는 871건 전부 null 이라 쓸모없지만 **빌트인 그룹이라 누락시키면 App 이
    # 맨 위에 빈 그룹을 자동 삽입한다**(실측). 이름을 바꾸면 매칭이 깨져 역시 중복 삽입되므로
    # **이름은 정확히 "metadata"** 로 두고 맨 끝 접힘으로 위치만 통제한다.
    for g in defaults:
        if g.name == "metadata":
            groups.append(G(name="metadata", paths=g.paths, expanded=False))
            assigned.update(g.paths)

    # ⚠️ 남은 경로를 '기타' 로 몰지 **않는다** — 그게 사이드바를 72개로 만든 원인이었다.
    # 사이드바에서 빠져도 DB/Python/CSV 에는 그대로 있다(데이터 손실 아님).
    rest = [p for p in universe if p not in assigned]
    log(f"build: 사이드바에서 숨긴 경로 {len(rest)}개 → {', '.join(sorted(rest)[:8])}"
        f"{' …' if len(rest) > 8 else ''}")

    ds.app_config.sidebar_groups = groups

    # 썸네일 칩 = 여기 나열한 것만. 두 극단이 다 안 됐다(실측):
    #   · blocklist(exclude=True) → top_prompt_*/top10_text_* 같은 문자열까지 칩으로 떠서
    #     썸네일에 프롬프트 원문이 깔린다.
    #   · allowlist 를 ground_truth 하나로 → **그리드 팔레트(색상 스킴) 목록도 같이 줄어**
    #     camera_angle 로 색칠을 못 한다(팔레트는 렌더되는 필드만 대상으로 잡는다).
    # → 색칠·판정에 실제로 쓰는 단일 Classification + outcome 만 allowlist.
    #   (Embeddings 패널의 Color by 목록은 active_fields 와 무관하게 전체 스키마를 쓴다.)
    try:
        from fiftyone.core.odm.dataset import ActiveFields

        keep_active = ["ground_truth", "camera_angle", f"pred_{v0}", f"pred_{v4}", "outcome"]
        ds.app_config.active_fields = ActiveFields(paths=keep_active, exclude=False)
        log(f"build: 썸네일/팔레트 활성 필드 = {keep_active}")
    except Exception as exc:  # noqa: BLE001
        log(f"build: active_fields 설정 실패 {type(exc).__name__}: {exc}")

    ds.save()
    n_exp = sum(len(g.paths) for g in groups if g.expanded)
    log(f"build: 사이드바 그룹 {len(groups)}개 / 노출 경로 {len(assigned)}개 "
        f"(펼침 {n_exp}개, 숨김 {len(rest)}개)")


# ────────────────────────────── 7b. export ──────────────────────────────
def stage_export() -> None:
    """샘플별 프롬프트 코사인 유사도를 CSV 2종으로 떨어뜨린다 (grep/피벗용)."""
    with open(f"{WORK_DIR}/scores.json", encoding="utf-8") as f:
        scores = json.load(f)
    assets = {a["asset_id"]: a for a in load_assets()}
    os.makedirs(REPORT_DIR, exist_ok=True)

    p_top = f"{REPORT_DIR}/sourceh_top_prompts.csv"
    with open(p_top, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["asset_id", "original_name", "folder", "gt_class", "version",
                    "rank", "cosine", "prompt_class", "frame_idx", "prompt"])
        for r in scores:
            a = assets[r["asset_id"]]
            for ver in VERSIONS:
                for t in r[ver]["top_overall"]:
                    w.writerow([r["asset_id"], a["original_name"], a["folder"],
                                CLASS_NAMES[r["gt_class"]], ver, t["rank"],
                                f"{t['cos']:.6f}", CLASS_NAMES[t["cls"]], t["frame_idx"],
                                t["prompt"].strip()])

    p_cls = f"{REPORT_DIR}/sourceh_class_scores.csv"
    with open(p_cls, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["asset_id", "original_name", "folder", "gt_class", "version",
                    "class", "cosine_max", "cosine_top10_mean", "frame_idx",
                    "is_pred", "best_prompt"])
        for r in scores:
            a = assets[r["asset_id"]]
            for ver in VERSIONS:
                for c, cb in sorted(r[ver]["class_best"].items(), key=lambda kv: -kv[1]["cos"]):
                    w.writerow([r["asset_id"], a["original_name"], a["folder"],
                                CLASS_NAMES[r["gt_class"]], ver, CLASS_NAMES[int(c)],
                                f"{cb['cos']:.6f}", f"{cb['topk_mean']:.6f}",
                                cb["frame_idx"], int(int(c) == r[ver]["pred"]),
                                cb["prompt"].strip()])
    for p in (p_top, p_cls):
        log(f"export → {p} ({sum(1 for _ in open(p, encoding='utf-8')) - 1} rows)")


def _minio_sizes(prefix: str) -> dict[str, int]:
    client = s3()
    out: dict[str, int] = {}
    token = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": f"{prefix}/", "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = client.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            out[o["Key"]] = o["Size"]
        if not resp.get("IsTruncated"):
            return out
        token = resp["NextContinuationToken"]


# ────────────────────────────── 8. report ──────────────────────────────
def _wilson(k: int, n: int) -> tuple[float, float]:
    if n == 0:
        return (0.0, 0.0)
    z, p = 1.959964, k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((c - h) / d, (c + h) / d)


def _mcnemar_exact(b: int, c: int) -> float:
    """양측 exact McNemar (binomial). b=v080만 맞음, c=v084만 맞음."""
    n = b + c
    if n == 0:
        return 1.0
    k = min(b, c)
    tail = sum(math.comb(n, i) for i in range(k + 1)) / (2**n)
    return min(1.0, 2 * tail)


def stage_report() -> None:
    os.makedirs(REPORT_DIR, exist_ok=True)
    with open(f"{WORK_DIR}/scores.json", encoding="utf-8") as f:
        scores = json.load(f)
    assets = {a["asset_id"]: a for a in load_assets()}
    banks = {}
    for ver in VERSIONS:
        z = np.load(f"{PROMPT_DIR}/{ver}.npz", allow_pickle=True)
        banks[ver] = z["cls"].astype(int)

    L: list[str] = []
    A = L.append
    A("# source-h 프롬프트 뱅크 버전 비교 — v1.0.8.0 vs v1.0.8.4\n")
    A(f"- 생성: {time.strftime('%Y-%m-%d %H:%M:%S')} (KST)")
    A(f"- 대상: MinIO `source-h/` + `source-h/` 두 prefix 전용, 영상 {len(scores)}편")
    A("- 인코더: PE-Core-L14-336 `/embed_text` (userwatch 뱅크 `feature` 와 cosine=1.000000 동일)")
    A("- 결정 규칙: 클래스 점수 = max over (3키프레임 × 그 클래스 프롬프트) cosine → argmax")
    A("- GT: **폴더명 파생 weak GT** (사람 검수 아님) — helmet→normal, falldown/fire/smoke\n")

    A("## 1. 프롬프트 뱅크 커버리지\n")
    A("| class | 의미 | v1.0.8.0 프롬프트 | v1.0.8.4 프롬프트 | 데이터(영상) |")
    A("|---|---|---|---|---|")
    gt_n = {c: sum(1 for r in scores if r["gt_class"] == c) for c in CLASS_NAMES}
    for c, name in CLASS_NAMES.items():
        n0 = int((banks[VERSIONS[0]] == c).sum())
        n4 = int((banks[VERSIONS[1]] == c).sum())
        A(f"| {c} | {name} | {n0:,} | {n4:,} | {gt_n.get(c, 0)} |")
    A(f"| — | 합계 | {len(banks[VERSIONS[0]]):,} | {len(banks[VERSIONS[1]]):,} | {len(scores)} |")
    A("\n> class 4(smoking)은 **두 버전 모두 프롬프트 0개** → 구조적으로 예측 불가.")
    A("> source-h 데이터에도 smoking 폴더가 없어 이번 비교의 커버 범위는 class 0–3 이다.\n")

    A("## 2. 전체 정확도\n")
    A("| 버전 | 규칙 | 정확 | n | accuracy | 95% CI (Wilson) |")
    A("|---|---|---|---|---|---|")
    for ver in VERSIONS:
        for rule, key in (("max (제품)", "correct"), ("top-10 평균", "correct_topk")):
            k = sum(1 for r in scores if r[ver][key])
            lo, hi = _wilson(k, len(scores))
            A(f"| {ver} | {rule} | {k} | {len(scores)} | {k / len(scores):.3%} | "
              f"{lo:.3%} – {hi:.3%} |")
    A("")
    # micro accuracy 는 이 데이터의 클래스 편중(smoke 430/871 = 49%)에 지배된다.
    # 클래스 균등 관점(macro recall)을 반드시 같이 봐야 결론이 뒤집히는지 알 수 있다.
    A("**클래스 균등 관점 (macro recall — 클래스별 recall 의 단순평균)**\n")
    A("| 집계 | v1.0.8.0 | v1.0.8.4 | Δ |")
    A("|---|---|---|---|")
    present = [c for c in CLASS_NAMES if any(r["gt_class"] == c for r in scores)]
    big = [c for c in present if sum(1 for r in scores if r["gt_class"] == c) >= 30]

    def macro(ver: str, cs: list[int]) -> float:
        vals = []
        for c in cs:
            sub = [r for r in scores if r["gt_class"] == c]
            vals.append(sum(1 for r in sub if r[ver]["correct"]) / len(sub))
        return sum(vals) / len(vals) if vals else 0.0

    for label, cs in (
        (f"macro recall (전 {len(present)}클래스)", present),
        (f"macro recall (n≥30 클래스 {len(big)}개만)", big),
        ("micro accuracy (=전체 정확도)", None),
    ):
        if cs is None:
            m0 = sum(1 for r in scores if r[VERSIONS[0]]["correct"]) / len(scores)
            m4 = sum(1 for r in scores if r[VERSIONS[1]]["correct"]) / len(scores)
        else:
            m0, m4 = macro(VERSIONS[0], cs), macro(VERSIONS[1], cs)
        A(f"| {label} | {m0:.1%} | {m4:.1%} | {(m4 - m0) * 100:+.1f}%p |")
    A("\n> **micro 와 macro 가 반대 방향이다.** source-h 은 절반이 smoke(430/871)라 "
      "micro accuracy 가 smoke 성능에 지배된다. 클래스를 균등하게 보면 v1.0.8.4 가 낫고, "
      "이 데이터 구성 그대로 보면 v1.0.8.0 이 낫다 — 어느 쪽을 채택할지는 "
      "**운영 시 클래스 분포와 오탐 비용**이 결정한다.\n")

    b = sum(1 for r in scores if r["outcome"] == "only_v1.0.8.0")
    c_ = sum(1 for r in scores if r["outcome"] == "only_v1.0.8.4")
    both_ok = sum(1 for r in scores if r["outcome"] == "both_correct")
    both_no = sum(1 for r in scores if r["outcome"] == "both_wrong")
    p = _mcnemar_exact(b, c_)
    A("## 3. 짝지어진 비교 (동일 871편, McNemar exact)\n")
    A("| 결과 | n |")
    A("|---|---|")
    A(f"| 둘 다 정답 | {both_ok} |")
    A(f"| **v1.0.8.4 만 정답 (개선)** | {c_} |")
    A(f"| **v1.0.8.0 만 정답 (퇴행)** | {b} |")
    A(f"| 둘 다 오답 | {both_no} |")
    A(f"\n순개선 = {c_ - b:+d}편 ({(c_ - b) / len(scores):+.2%}p), McNemar exact p = {p:.3g}")
    A("\n> 카메라 단위 clustering(design effect 9.22, 설계문서 §통계) 때문에 위 CI/p 는 "
      "**영상 독립 가정**의 낙관적 값이다. 카메라 수준 결론엔 그대로 쓰지 말 것.\n")

    A("## 4. 클래스별 (weak GT)\n")
    A("| GT class | n | v1.0.8.0 recall | v1.0.8.4 recall | Δ |")
    A("|---|---|---|---|---|")
    for c, name in CLASS_NAMES.items():
        sub = [r for r in scores if r["gt_class"] == c]
        if not sub:
            continue
        r0 = sum(1 for r in sub if r[VERSIONS[0]]["correct"]) / len(sub)
        r4 = sum(1 for r in sub if r[VERSIONS[1]]["correct"]) / len(sub)
        warn = " ⚠️n소" if len(sub) < 30 else ""
        A(f"| {c} {name} | {len(sub)}{warn} | {r0:.1%} | {r4:.1%} | {r4 - r0:+.1%}p |")
    A("")

    for ver in VERSIONS:
        A(f"### 혼동행렬 — {ver} (행=GT, 열=예측)\n")
        cs = sorted({r["gt_class"] for r in scores} | {r[ver]["pred"] for r in scores})
        A("| GT \\ pred | " + " | ".join(CLASS_NAMES[c] for c in cs) + " |")
        A("|---" * (len(cs) + 1) + "|")
        for g in sorted({r["gt_class"] for r in scores}):
            row = [sum(1 for r in scores if r["gt_class"] == g and r[ver]["pred"] == pc) for pc in cs]
            A(f"| **{CLASS_NAMES[g]}** | " + " | ".join(str(x) for x in row) + " |")
        A("")

    A("### 예측이 바뀐 패턴 (v1.0.8.0 → v1.0.8.4, 상위 10)\n")
    A("| GT | v1.0.8.0 예측 | v1.0.8.4 예측 | n | 판정 |")
    A("|---|---|---|---|---|")
    trans: dict[tuple[int, int, int], int] = {}
    for r in scores:
        p0, p4 = r[VERSIONS[0]]["pred"], r[VERSIONS[1]]["pred"]
        if p0 != p4:
            key = (r["gt_class"], p0, p4)
            trans[key] = trans.get(key, 0) + 1
    for (g, p0, p4), n in sorted(trans.items(), key=lambda kv: -kv[1])[:10]:
        verdict = "✅개선" if p4 == g else "❌퇴행" if p0 == g else "오답→오답"
        A(f"| {CLASS_NAMES[g]} | {CLASS_NAMES[p0]} | {CLASS_NAMES[p4]} | {n} | {verdict} |")
    A("")

    A("## 5. camera_angle (DAv2, migration 017)\n")
    ang = {}
    for r in scores:
        ang[r.get("camera_angle") or "MISSING"] = ang.get(r.get("camera_angle") or "MISSING", 0) + 1
    A("| camera_angle | n | 비율 |")
    A("|---|---|---|")
    for k, v in sorted(ang.items(), key=lambda kv: -kv[1]):
        A(f"| {k} | {v} | {v / len(scores):.1%} |")
    unstable = sum(1 for r in scores if r.get("angle_stable") is False)
    A(f"\n3프레임 라벨 불일치(프레임간 불안정): {unstable} / {len(scores)} "
      f"({unstable / len(scores):.1%})")
    A("\n| camera_angle | " + " | ".join(f"{v} acc" for v in VERSIONS) + " | n |")
    A("|---|---|---|---|")
    for k in sorted(ang):
        sub = [r for r in scores if (r.get("camera_angle") or "MISSING") == k]
        a0 = sum(1 for r in sub if r[VERSIONS[0]]["correct"]) / len(sub)
        a4 = sum(1 for r in sub if r[VERSIONS[1]]["correct"]) / len(sub)
        A(f"| {k} | {a0:.1%} | {a4:.1%} | {len(sub)} |")
    A("")

    A("## 6. 커버리지 — 카메라(장소) 단위\n")
    A("> 설계문서 기준 분석 단위는 **카메라**다(ICC 0.075, design effect 9.22). "
      "카메라는 파일명의 장소 토큰에서 파생했다(폴더별로 토큰 위치가 반대라 양쪽 레이아웃 처리).\n")
    A("> ⚠️ **카메라가 3곳뿐이다.** design effect 9.22 를 감안하면 유효 표본은 영상 871편이 "
      "아니라 사실상 클러스터 3개 수준이다 — §3 의 p 값은 이 사실을 반영하지 않는다.\n")
    A("| camera_id | n | 폴더 | v1.0.8.0 | v1.0.8.4 | Δ |")
    A("|---|---|---|---|---|---|")
    by_cam: dict[str, list] = {}
    for r in scores:
        by_cam.setdefault(assets[r["asset_id"]]["camera_id"], []).append(r)
    for cam, sub in sorted(by_cam.items(), key=lambda kv: -len(kv[1])):
        a0 = sum(1 for r in sub if r[VERSIONS[0]]["correct"]) / len(sub)
        a4 = sum(1 for r in sub if r[VERSIONS[1]]["correct"]) / len(sub)
        folders = ",".join(sorted({assets[r["asset_id"]]["folder"] for r in sub}))
        A(f"| {cam} | {len(sub)} | {folders} | {a0:.0%} | {a4:.0%} | {(a4 - a0) * 100:+.0f}%p |")
    A(f"\n카메라 {len(by_cam)}곳 / 영상 {len(scores)}편.\n")
    A("### 카메라 × GT 교차표 — ⚠️ 카메라와 클래스가 교란되어 있다\n")
    cams = sorted(by_cam, key=lambda c: -len(by_cam[c]))
    gts = [CLASS_NAMES[c] for c in sorted({r["gt_class"] for r in scores})]
    A("| camera_id | " + " | ".join(gts) + " | 합 |")
    A("|---" * (len(gts) + 2) + "|")
    for cam in cams:
        sub = by_cam[cam]
        row = [sum(1 for r in sub if CLASS_NAMES[r["gt_class"]] == g) for g in gts]
        A(f"| {cam} | " + " | ".join(str(x) for x in row) + f" | {len(sub)} |")
    A("\n> 카메라마다 클래스 구성이 전혀 다르다. 따라서 위 카메라별 정확도 차이는 "
      "**카메라 난이도가 아니라 클래스 구성 차이**를 상당 부분 반영한다. 예: 최대 카메라의 "
      "v1.0.8.4 하락은 그 카메라에 smoke 가 몰려 있어서 생긴 결과에 가깝다. "
      "카메라 효과를 보려면 클래스를 고정한 뒤 비교해야 한다(FiftyOne 에서 "
      "`ground_truth` + `camera_id` 를 함께 필터).\n")

    A("## 7. 커버리지 — 프롬프트 뱅크 실사용률\n")
    A("> 실제로 '1위'를 차지한 적이 있는 프롬프트 수 = 뱅크가 이 데이터에서 실제로 쓰인 정도.\n")
    A("| 버전 | 뱅크 크기 | 1위를 차지한 고유 프롬프트 | 사용률 |")
    A("|---|---|---|---|")
    winners = {}
    for ver in VERSIONS:
        used = {}
        for r in scores:
            p = r[ver]["top_prompt"]
            used[p] = used.get(p, 0) + 1
        winners[ver] = used
        A(f"| {ver} | {len(banks[ver]):,} | {len(used)} | {len(used) / len(banks[ver]):.2%} |")
    for ver in VERSIONS:
        A(f"\n**{ver} 최다 1위 프롬프트 (상위 8)**\n")
        A("| n | prompt |")
        A("|---|---|")
        for p, n in sorted(winners[ver].items(), key=lambda kv: -kv[1])[:8]:
            A(f"| {n} | {p.strip()[:110]} |")
    A("")

    A("## 8. 데이터 무결성\n")
    n_sha = sum(1 for r in scores if assets[r["asset_id"]]["checksum"])
    cams = {assets[r["asset_id"]]["camera_id"] for r in scores}
    A(f"- sha256 대조 대상: {n_sha} / {len(scores)} (media 스테이지에서 검증)")
    A(f"- 카메라(파일명 파생) 고유 수: {len(cams)} → 영상/카메라 ≈ {len(scores) / max(1, len(cams)):.1f}")
    A(f"- `ingest_status`: 871편 전부 `uploading` — `completed` 게이트 때문에 "
      "이 코호트는 정식 라벨링 파이프라인에서 누락된 상태다(기존 인시던트).")
    A("- `source-h/` prefix 객체 중 67건은 DB/원본과 **다른 바이트**(더 작음) → 미디어는 "
      "`source-h/<한글>`(871/871 일치)에서 읽었다. FiftyOne `08_lower_key_byte_mismatch` 뷰 참조.\n")

    A("## 9. FiftyOne\n")
    A("- URL: <http://10.0.0.10:5153/datasets/source-h>")
    A("- 샘플 = 영상 1편, 이미지 = 가운데 키프레임. `keyframe_paths` 에 3장 경로.")
    A("- 핵심 필드: `outcome`(both_correct/only_v1.0.8.4/only_v1.0.8.0/both_wrong), "
      "`correct_v1_0_8_*`, `score_v1_0_8_*_<class>`, `margin_v1_0_8_*`, `conf_delta`, "
      "`camera_angle`, `tilt_deg`, `angle_votes`.")
    A("- saved views 01~09 (불일치/개선/퇴행/둘다오답/각도교차/저마진/falldown/바이트불일치/각도불안정)")
    A("- `eval_v1_0_8_0` · `eval_v1_0_8_4` evaluation → App 에서 혼동행렬 패널.")
    A("- 브레인 키: `emb_viz`(이 배포 관례) + `umap`(별칭, 동일 좌표) + `text_search`"
      "(prompt-capable — App 검색바에 임의 문장을 넣으면 코사인 랭킹).")
    A("- 프롬프트 유사도: `class_best_*`(클래스별 1위 프롬프트+코사인), "
      "`top10_*`(뱅크 전체 최근접 10), `top10_text_*`(평문). CSV: `sourceh_top_prompts.csv`(17,420행), "
      "`sourceh_class_scores.csv`(6,968행).")
    A("- **필터 사이드바: 저장된 뷰 `00_analysis` 를 진입점으로 쓸 것.** 노이즈 21필드를 "
      "`exclude_fields` 로 제외해 렌더 필드가 77 → 56 개로 줄어든다(2026-07-29 DOM 실측).")
    A("  > ⚠️ `app_config.sidebar_groups` 에서 경로를 빼는 것만으로는 **숨겨지지 않는다** — "
      "FiftyOne 1.19 는 미배정 필드를 자동 생성 `PRIMITIVES` 그룹에 모아 맨 아래에 붙인다. "
      "sidebar_groups 는 **그룹핑·순서**만 통제하고, 실제 제거는 뷰의 `exclude_fields` 뿐이다. "
      "`metadata`/`id`/`filepath`/`created_at`/`last_modified_at` 는 기본 필드라 제외 자체가 거부된다.")
    A("- 사이드바 그룹: ① 판정 / ② trade-off / ③ 버전차 근거 / ④ 층화·교란 (여기까지 펼침) / "
      "⑤ 예측 상세 / ⑥ 원점수(버전간 직접비교 금지) / ⑦ 프롬프트 상세 / ⑧ QA·무결성 / "
      "⑨ 조회 키·provenance. 썸네일 칩은 ground_truth/camera_angle/pred×2/outcome 만.")
    A("\n### trade-off 를 보는 방법\n")
    A("| 도구 | 무엇이 보이나 |")
    A("|---|---|")
    A("| 브레인 키 `tradeoff_viz` | **before/after 산점도** — x=v1.0.8.0, y=v1.0.8.4 의 GT클래스 "
      "상대점수. 대각선 위=개선 / 아래=퇴행 / 좌하=둘 다 못 맞춤. 올가미 선택 → 이미지 즉시 표시 |")
    A("| 워크스페이스 `tradeoff` | 위 산점도 + Samples 좌우 분할, outcome 색 |")
    A("| 필드 `transition` | 값별 개수가 **곧 전이표**(21종). 클릭하면 해당 전이 샘플만 필터 |")
    A("| 필드 `gt_rel_delta` | GT클래스 상대점수의 버전차. 정렬하면 가장 많이 잃은/얻은 순 |")
    A("| 뷰 `10~13` | 예측이 바뀐 242편 / 잃은 것 / 얻은 것 / smoke→fire 66편 |")
    A("\n#### GT 무관 — 버전이 달라지면서 예측이 바뀐 것 (`pred_shift`)\n")
    A("> 정답 여부를 개입시키지 않고 **변화 자체**만 본다. GT 접두어가 없어 같은 변화가 "
      "쪼개지지 않는다(11범주 vs `transition` 21범주).\n")
    A("| n | 예측 변화 |")
    A("|---|---|")
    shifts = collections.Counter(
        f"{CLASS_NAMES[r[VERSIONS[0]]['pred']]}→{CLASS_NAMES[r[VERSIONS[1]]['pred']]}"
        for r in scores
        if r[VERSIONS[0]]["pred"] != r[VERSIONS[1]]["pred"]
    )
    for k, n in shifts.most_common():
        A(f"| {n} | {k} |")
    A(f"\n바뀜 {sum(shifts.values())}편 / 유지 {len(scores) - sum(shifts.values())}편. "
      "지배적 변화는 **smoke→normal**(검출 상실)과 **normal→fire·smoke→fire**(fire 과검출 방향)로, "
      "v1.0.8.4 가 전반적으로 **fire 쪽으로 기울고 smoke 를 놓치는** 방향으로 이동했다.")
    A("\n> FiftyOne: 사이드바 `② 버전변화 (GT 무관)` 의 `pred_shift` 값별 개수가 위 표다. "
      "브레인 키 `shift_viz`(x=옛 답에서 멀어진 정도, y=새 답으로 당긴 정도) + 워크스페이스 "
      "`shift`, 뷰 `14~16`(가장 많은 변화 top-3).\n")
    A("\n#### ★ 변화의 **방향**이 크기보다 강한 신호다 (`shift_direction`)\n")
    NORMAL = CLASS_NAMES[0]

    def _dir_of(r: dict) -> str:
        a, b = CLASS_NAMES[r[VERSIONS[0]]["pred"]], CLASS_NAMES[r[VERSIONS[1]]["pred"]]
        if a == b:
            return "변화없음"
        return ("회수 (normal→이벤트)" if a == NORMAL
                else "상실 (이벤트→normal)" if b == NORMAL
                else "오분류 (이벤트→다른이벤트)")

    A("| 방향 | n | v1.0.8.0 | v1.0.8.4 | 순변화 |")
    A("|---|---|---|---|---|")
    for k in ("회수 (normal→이벤트)", "오분류 (이벤트→다른이벤트)",
              "상실 (이벤트→normal)", "변화없음"):
        sub = [r for r in scores if _dir_of(r) == k]
        if not sub:
            continue
        a0 = sum(1 for r in sub if r[VERSIONS[0]]["correct"]) / len(sub)
        a4 = sum(1 for r in sub if r[VERSIONS[1]]["correct"]) / len(sub)
        A(f"| {k} | {len(sub)} | {a0:.1%} | {a4:.1%} | {(a4 - a0) * 100:+.1f}%p |")
    A("\n> **회수**(v1.0.8.0 이 normal 로 놓친 것)에서 v1.0.8.4 가 압도적으로 이기고, "
      "**상실·오분류**(이미 이벤트로 잡던 것)에서 압도적으로 진다. 크기(`shift_mag`)보다 "
      "방향이 채택 여부를 결정한다.")
    A("> 근거: 회수 전환의 v1.0.8.0 정답률은 정의상 0% — normal 예측이 이벤트 GT 에 맞을 수 "
      "없기 때문이다. 반대로 상실·오분류 전환은 v1.0.8.0 이 이미 86~90% 맞히고 있었다.\n")
    A("**운영 규칙 비교** (저마진 전환 Q1·Q2 를 분석에서 제외한 750편 기준 — 그 구간은 마진이 "
      "v1.0.8.4 코사인 표준편차의 12~17% 로 노이즈·GT 품질에 지배된다):\n")
    A("| 규칙 | 정확도 | v1.0.8.0 대비 | v1.0.8.4 대비 |")
    A("|---|---|---|---|")
    A("| v1.0.8.0 단독 | 65.5% | — | −4.0%p |")
    A("| v1.0.8.4 단독 | 69.5% | +4.0%p | — |")
    A("| **`회수` 전환만 채택** | **73.3%** | **+7.9%p** | **+3.9%p** |")
    A("| 크기규칙 (`shift_mag` ≥ 0.022) | 70.9% | +5.5%p | +1.5%p |")
    A("\n> 즉 **v1.0.8.4 를 쓰되, 이미 이벤트로 잡힌 것을 다른 이벤트나 normal 로 바꾸는 전환은 "
      "보류**하는 것이 최선이다. FiftyOne 뷰 `17_dir_recover`(97) / `18_dir_lose`(95) / "
      "`19_dir_swap`(50) / `20_analysis_scope`(750) 로 바로 검수할 수 있고, 워크스페이스 "
      "`shift-direction` 은 이미지 임베딩 공간에서 회수/상실이 어디에 몰렸는지 보여준다.\n")
    A("> ⚠️ 전체 871편 기준으로는 v1.0.8.4 가 −5.3%p 지만, 저마진 전환 121편을 빼면 **+4.0%p** "
      "로 부호가 뒤집힌다. 어느 범위를 쓰는지 밝히지 않은 단일 수치는 이 데이터셋에서 무의미하다.\n")

    A("\n#### 변화가 이미지 임베딩 공간에서 체계적인가 (연관도 수치)\n")
    A("> `dscore_pred_*` 는 이미지 임베딩과 **독립된 값이 아니라 그것의 함수**다"
      "(`cos(e, 프롬프트)` 의 차이). 그래서 '두 값의 비교'가 아니라 **'변화가 임베딩 공간에서"
      " 몰려 있나'** 를 재는 것이 맞다. 아래는 그 연관도다.\n")
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import StratifiedKFold, cross_val_score

        d = np.load(f"{WORK_DIR}/embed.npz", allow_pickle=True)
        vec_of = {str(k): v for k, v in zip(d["frame_path"], d["vec"])}
        rows = [r for r in scores if r["frames"] and r["frames"][len(r["frames"]) // 2] in vec_of]
        E = np.stack([vec_of[r["frames"][len(r["frames"]) // 2]] for r in rows]).astype(np.float32)
        E = E / np.linalg.norm(E, axis=1, keepdims=True)
        chg = np.array([r[VERSIONS[0]]["pred"] != r[VERSIONS[1]]["pred"] for r in rows])
        lab = np.array([
            f"{CLASS_NAMES[r[VERSIONS[0]]['pred']]}→{CLASS_NAMES[r[VERSIONS[1]]['pred']]}"
            if r[VERSIONS[0]]["pred"] != r[VERSIONS[1]]["pred"]
            else f"={CLASS_NAMES[r[VERSIONS[0]]['pred']]}"
            for r in rows
        ])
        S = E @ E.T
        np.fill_diagonal(S, -np.inf)
        K = 10
        nn = np.argsort(-S, axis=1)[:, :K]
        homo = float(np.mean([np.mean(lab[nn[i]] == lab[i]) for i in range(len(rows))]))
        pr = np.array(list(collections.Counter(lab).values())) / len(lab)
        auc = cross_val_score(
            LogisticRegression(max_iter=2000), E, chg,
            cv=StratifiedKFold(5, shuffle=True, random_state=51), scoring="roc_auc",
        )
        A("| 지표 | 값 | 해석 |")
        A("|---|---|---|")
        A(f"| 이미지 임베딩 → `pred_changed` 예측 AUC (5-fold) | **{auc.mean():.3f}** ± {auc.std():.3f} "
          "| 0.5=이미지로 전혀 예측 불가 / 1.0=완전히 예측 가능 |")
        A(f"| `pred_shift` kNN(k={K}) 이웃 동질성 | {homo:.3f} | 무작위 기대 "
          f"{(pr ** 2).sum():.3f} → **{homo / (pr ** 2).sum():.2f}배** |")
        A("\n→ 버전 변화는 무작위 잡음이 아니라 **특정 시각적 영역에 집중**돼 있다. "
          "FiftyOne 에서 브레인 키 `emb_viz` + Color by `shift_mag_q.label`"
          "(워크스페이스 `shift-where`)로 **어떤 화면이 흔들렸는지** 직접 볼 수 있다.\n")
    except Exception as exc:  # noqa: BLE001 — 연관도 계산 실패가 리포트를 막지 않게
        A(f"(연관도 계산 실패: {type(exc).__name__}: {exc})\n")

    A("\n#### 정답 기준 `transition` 상위 전이 (예측이 바뀐 242편):\n")
    A("| n | 전이 | 판정 |")
    A("|---|---|---|")
    for k, n in sorted(
        collections.Counter(
            (
                f"GT {CLASS_NAMES[r['gt_class']]} : "
                f"{CLASS_NAMES[r[VERSIONS[0]]['pred']]}→{CLASS_NAMES[r[VERSIONS[1]]['pred']]}"
            )
            for r in scores
            if r[VERSIONS[0]]["pred"] != r[VERSIONS[1]]["pred"]
        ).items(),
        key=lambda kv: -kv[1],
    )[:8]:
        gt = k.split(":")[0].replace("GT ", "").strip()
        a, b = k.split(":")[1].strip().split("→")
        A(f"| {n} | {k} | {'✅개선' if b == gt else ('❌퇴행' if a == gt else '오답→오답')} |")
    A("")
    A("- 워크스페이스 `angle-explore`(색=tilt_bin.label) · `outcome-explore`(색=outcome): "
      "Samples ↔ Embeddings 좌우 분할. 기본 레이아웃은 둘이 **탭**이라 점을 골라도 이미지를 "
      "동시에 못 본다.")
    A("\n> ⚠️ **Embeddings 패널 Color by 함정 2개** (실측):")
    A("> 1. Classification 필드는 **`.label` 서브경로 필수** — `tilt_bin` 은 `null`, "
      "`tilt_bin.label` 은 정상. `camera_angle`/`ground_truth`/`pred_*` 모두 동일.")
    A("> 2. 연속 float 은 색이 안 나온다 — `tilt_deg` 는 고유값 628개라 카테고리 색상 생성이 "
      "실패하고 무의미한 컬러바만 뜬다. 그래서 5도 구간 `tilt_bin`(7개 범주)을 만들었다.\n")

    text = "\n".join(L)
    out = f"{REPORT_DIR}/sourceh_prompt_version_report.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write(text)
    log(f"report → {out}")
    print("\n" + text)


# ────────────────────────────── main ──────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "stage",
        choices=["prompts", "media", "angle", "embed", "score", "dbwrite", "build",
                 "export", "report", "all"],
    )
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--dataset", default="source-h")
    args = ap.parse_args()

    for d in (PROMPT_DIR, FRAME_DIR, WORK_DIR, REPORT_DIR):
        os.makedirs(d, exist_ok=True)

    stages = (
        ["prompts", "media", "angle", "embed", "score", "dbwrite", "build", "export", "report"]
        if args.stage == "all"
        else [args.stage]
    )
    for st in stages:
        log(f"───── stage: {st} ─────")
        if st == "prompts":
            stage_prompts(args.limit)
        elif st == "media":
            stage_media(args.limit, args.workers)
        elif st == "angle":
            stage_angle(args.limit, max(args.workers, 4))
        elif st == "embed":
            stage_embed(args.limit)
        elif st == "score":
            stage_score()
        elif st == "dbwrite":
            stage_dbwrite()
        elif st == "build":
            stage_build(args.dataset)
        elif st == "export":
            stage_export()
        elif st == "report":
            stage_report()
    log("완료")


if __name__ == "__main__":
    sys.exit(main())
