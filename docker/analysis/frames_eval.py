#!/usr/bin/env python3
"""source-h **프레임 단위 재라벨링** 데이터셋 → 임베딩 + 프롬프트 버전 비교 + FiftyOne.

기존 `prompt_eval.py`(영상 단위, MinIO 소스)의 후속. 근본적으로 다른 데이터다:

| | 구 버전 | 이 스크립트 |
|---|---|---|
| 소스 | MinIO `vlm-raw/source-h/` 영상 871편 | NAS `/home/user/mou/nas_primary/source-h/` **JPG 프레임** |
| 샘플 단위 | 영상 1편 (키프레임 3장 추출) | **프레임 1장** (추출 불필요) |
| GT | 영상 폴더명 = weak GT | **프레임 폴더명 = 사람이 프레임별로 재라벨** |
| 클래스 | helmet→normal 매핑(내 가정) | `normal` 이 **명시적 폴더**로 존재 |

**재라벨링의 의미 (2026-07-30 실측)**: 영상 단위로 fire/smoke 였던 것에서 뽑은 프레임 다수가
`normal` 로 재라벨됐다 (normal←fire 3,149프레임/7영상, normal←smoke 969프레임/219영상).
구 분석의 "v1.0.8.4 가 smoke 를 잃었다"는 결론은 상당 부분 **영상 단위 GT 잡음**이었을 수
있다 — 이 데이터셋이 그 검증 수단이다.

## 운영 제약 (설계에 반영됨)

1. **업로드 진행 중** → 파일 집합이 계속 늘어난다. 모든 단계가 `(folder, name)` 키로 멱등이며
   재실행 시 **새로 올라온 것만** 처리한다. `--limit` 으로 한 번에 처리할 양을 제한한다.
2. **analysis 컨테이너에 NAS 마운트가 없다** (`/data/fiftyone` 만 rw) → FiftyOne 이 이미지를
   서빙하려면 `/data/fiftyone/sourceh_v2/frames/` 로 **복사**해야 한다. compose 에 NAS 마운트를
   추가하면 컨테이너 recreate = 실행 중인 FiftyOne 앱 중단이라 택하지 않았다.
3. **파일명이 NFD(맥 분해형)** 다 (`.DS_Store` 동반). 한글 매칭 전 반드시 `NFC` 정규화 —
   안 하면 `'화재' in name` 이 조용히 False 가 된다(실측).
4. 루트 디스크가 97% 다 → 복사량을 `du` 로 로깅하고, 원본(NAS)은 **절대 건드리지 않는다**.

## 스테이지 (각각 멱등, `--limit` 지원)

    scan     NAS 목록 → 원장(ledger.jsonl). 파일 크기·mtime 으로 변경 감지
    copy     NAS → /data/fiftyone/sourceh_v2/frames/<folder>/ 증분 복사
    embed    복사된 프레임 → /embed (PE-Core) → embed.npz 누적
    score    프롬프트 뱅크 2버전 코사인 → scores.json  (프롬프트 npz 는 구 스크립트 산출물 재사용)
    build    FiftyOne 데이터셋 `source-h-frames`
    report   마크다운 리포트

사용:
    python3 /workspace/frames_eval.py scan
    python3 /workspace/frames_eval.py copy --limit 1500
    python3 /workspace/frames_eval.py embed --limit 2000
    python3 /workspace/frames_eval.py all --limit 2000     # 업로드 끝나면 --limit 없이
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import shutil
import sys
import time
import unicodedata as ud


# ⚠️ numpy/requests 는 **지연 import** 한다. scan/copy 스테이지는 NAS 를 읽어야 해서
#    NAS 가 마운트된 일회성 컨테이너나 호스트에서 돌 수 있는데, 그 환경에 numpy 가 없을 수 있다.
#    (analysis 컨테이너엔 NAS 마운트가 없어 scan/copy 를 거기서 못 돌린다 — 실측)
#
# 실행 예 (NAS + fiftyone 볼륨 + 네트워크를 다 붙인 일회성 컨테이너):
#   docker run --rm --network pipeline-network \
#     -v /home/user/mou/nas_primary/source-h:/nas/source-h:ro \
#     -v /home/user/work_p/<repo>/docker/data/fiftyone:/data/fiftyone \
#     -v /home/user/work_p/<repo>/docker/analysis:/ws:ro \
#     -e SOURCEH_NAS_ROOT=/nas/source-h \
#     datapipeline-analysis:latest python3 /ws/frames_eval.py copy --limit 1500
# NAS 는 **:ro** 로 붙여 원본을 물리적으로 못 건드리게 한다.

NAS_ROOT = os.environ.get("SOURCEH_NAS_ROOT", "/home/user/mou/nas_primary/source-h")
ROOT = "/data/fiftyone/sourceh_v2"
FRAME_DIR = f"{ROOT}/frames"
WORK_DIR = f"{ROOT}/work"
REPORT_DIR = f"{ROOT}/report"
# 프롬프트 임베딩은 구 스크립트가 만든 것을 그대로 재사용한다 (버전 키가 같으면 동일 벡터)
PROMPT_DIR = "/data/fiftyone/sourceh/prompts"

EMBED_URL = os.environ.get("EMBEDDING_API_URL", "http://embedding-service:8003")
VERSIONS = ("v1.0.8.0", "v1.0.8.4")
CLASS_NAMES = {0: "normal", 1: "falldown", 2: "fire", 3: "smoke", 4: "smoking"}
# 폴더명 = 사람이 재라벨한 GT. helmet 은 5-클래스 택소노미에 없어 normal 로 본다(구 버전과 동일 근거).
FOLDER_TO_CLASS = {"normal": 0, "falldown": 1, "fire": 2, "smoke": 3, "helmet": 0}
EVENT_TOKENS = {"쓰러짐": "falldown", "화재": "fire", "연기": "smoke", "헬멧": "helmet"}
TOPK = 10
TOP_SHOW = 10


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def nfc(s: str) -> str:
    """맥 업로드 파일명은 NFD 다 — 한글 비교 전 반드시 정규화."""
    return ud.normalize("NFC", s)


def src_of(stem: str) -> tuple[str, int]:
    """`<원본영상명>_<프레임번호>` → (원본영상명, 프레임번호). 번호 없으면 (stem, -1)."""
    head, _, tail = stem.rpartition("_")
    if head and tail.isdigit():
        return head, int(tail)
    return stem, -1


def original_event(src: str) -> str:
    """원본 영상명에 박힌 **원래** 이벤트 라벨 (재라벨 여부 비교용)."""
    s = nfc(src)
    for tok, lab in EVENT_TOKENS.items():
        if tok in s:
            return lab
    return "unknown"


def camera_of(src: str) -> str:
    """장소(=카메라 프록시). source-h 은 폴더별로 파일명 레이아웃이 반대다(구 스크립트 §camera_id_of)."""
    p = nfc(src).split("_")
    if len(p) >= 4 and len(p[0]) == 8 and p[0].isdigit() and p[1].isdigit():
        return "_".join(p[3:])
    if len(p) >= 4 and len(p[-2]) == 8 and p[-2].isdigit():
        return "_".join(p[:-3])
    return p[0] if p else src


def jsonl_load(path: str, key: str = "key") -> dict:
    out = {}
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:  # 중단으로 잘린 마지막 줄
                continue
            out[r[key]] = r
    return out


def jsonl_append(path: str, recs: list[dict]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        for r in recs:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


# ────────────────────────── 1. scan ──────────────────────────
def stage_scan() -> None:
    """NAS 를 한 번만 훑어 원장을 갱신. 업로드 중이므로 매 실행마다 새 파일이 늘어난다."""
    os.makedirs(WORK_DIR, exist_ok=True)
    path = f"{WORK_DIR}/ledger.jsonl"
    known = jsonl_load(path)
    if not os.path.isdir(NAS_ROOT):
        raise SystemExit(f"NAS 경로 없음: {NAS_ROOT}")
    fresh = []
    for folder in sorted(os.listdir(NAS_ROOT)):
        d = os.path.join(NAS_ROOT, folder)
        if not os.path.isdir(d) or folder.startswith("."):
            continue
        if folder not in FOLDER_TO_CLASS:
            log(f"scan: 알 수 없는 폴더 '{folder}' 건너뜀 (FOLDER_TO_CLASS 미등록)")
            continue
        with os.scandir(d) as it:
            for e in it:
                if not e.name.lower().endswith((".jpg", ".jpeg", ".png")):
                    continue
                key = f"{folder}/{e.name}"
                st = e.stat()
                prev = known.get(key)
                if prev and prev.get("size") == st.st_size:
                    continue  # 이미 알고 있고 크기도 동일 → 변화 없음
                stem = os.path.splitext(e.name)[0]
                src, fr = src_of(stem)
                fresh.append({
                    "key": key, "folder": folder, "name": e.name,
                    "size": st.st_size, "mtime": round(st.st_mtime, 3),
                    "src_video": nfc(src), "frame_index": fr,
                    "gt_class": FOLDER_TO_CLASS[folder],
                    "original_event": original_event(src),
                    "camera": camera_of(src),
                })
    if fresh:
        jsonl_append(path, fresh)
    total = len(jsonl_load(path))
    log(f"scan: 신규/변경 {len(fresh)} → 원장 총 {total}개")
    dist = collections.Counter(r["folder"] for r in jsonl_load(path).values())
    log(f"scan: 폴더별 {dict(sorted(dist.items()))}")


# ────────────────────────── 2. copy ──────────────────────────
def stage_copy(limit: int | None) -> None:
    """NAS → 컨테이너가 볼 수 있는 /data/fiftyone 으로 증분 복사. 원본은 읽기만."""
    led = jsonl_load(f"{WORK_DIR}/ledger.jsonl")
    todo = []
    for k, r in led.items():
        dst = os.path.join(FRAME_DIR, r["folder"], r["name"])
        if os.path.exists(dst) and os.path.getsize(dst) == r["size"]:
            continue
        todo.append((r, dst))
    if limit:
        todo = todo[:limit]
    log(f"copy: 남은 {len(todo)} (전체 원장 {len(led)})")
    n = err = 0
    for r, dst in todo:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        src = os.path.join(NAS_ROOT, r["folder"], r["name"])
        try:
            shutil.copy2(src, dst)
            # 업로드 중 잘린 파일 방어: 크기가 원장과 다르면 버린다(다음 실행에서 재시도)
            if os.path.getsize(dst) != r["size"]:
                os.remove(dst)
                err += 1
                continue
            n += 1
        except Exception as exc:  # noqa: BLE001 — per-file fail-forward
            err += 1
            if err <= 3:
                log(f"copy 실패 {r['key']}: {type(exc).__name__}: {exc}")
        if n and n % 500 == 0:
            log(f"copy: {n}/{len(todo)}")
    log(f"copy 완료: 복사 {n} 실패/불일치 {err}")


# ────────────────────────── 3. embed ──────────────────────────
def stage_embed(limit: int | None) -> None:
    """복사된 프레임 → PE-Core /embed. npz 에 누적(멱등)."""
    import numpy as np
    import requests
    led = jsonl_load(f"{WORK_DIR}/ledger.jsonl")
    path = f"{WORK_DIR}/embed.npz"
    cache: dict[str, np.ndarray] = {}
    if os.path.exists(path):
        d = np.load(path, allow_pickle=True)
        cache = {str(k): v for k, v in zip(d["key"], d["vec"])}
    todo = [
        r for k, r in led.items()
        if k not in cache and os.path.exists(os.path.join(FRAME_DIR, r["folder"], r["name"]))
    ]
    if limit:
        todo = todo[:limit]
    log(f"embed: 남은 {len(todo)} (캐시 {len(cache)})")
    if not todo:
        return
    sess = requests.Session()
    t0 = time.time()
    ok = err = 0
    for i, r in enumerate(todo, 1):
        fp = os.path.join(FRAME_DIR, r["folder"], r["name"])
        try:
            with open(fp, "rb") as f:
                resp = sess.post(f"{EMBED_URL}/embed",
                                 files={"file": (r["name"], f, "image/jpeg")}, timeout=600)
            resp.raise_for_status()
            v = np.asarray(resp.json()["vector"], dtype=np.float32)
            nrm = float(np.linalg.norm(v))
            cache[r["key"]] = v / nrm if nrm > 0 else v
            ok += 1
        except Exception as exc:  # noqa: BLE001
            err += 1
            if err <= 3:
                log(f"embed 실패 {r['key']}: {type(exc).__name__}: {exc}")
        if i % 500 == 0:
            log(f"embed: {i}/{len(todo)} ({time.time() - t0:.0f}s)")
    keys = list(cache)
    np.savez_compressed(path, key=np.array(keys, dtype=object),
                        vec=np.stack([cache[k] for k in keys]))
    log(f"embed 완료: ok={ok} err={err} → 총 {len(cache)} 벡터")


# ────────────────────────── 3b. angle ──────────────────────────
def stage_angle(limit: int | None, workers: int = 6, per_frame: bool = False) -> None:
    """camera_angle(DAv2) — 기본은 원본 영상 단위, `--per-frame` 이면 전 프레임.

    ⚠️ **DAv2 는 이 배포에서 GPU 로 돈다** — `angle-dav2-1` 의 `ANGLE_DEVICE=cuda`,
    `CUDA_VISIBLE_DEVICES=0` (embedding-service 와 GPU0 공유). `docker/angle/app.py` 의
    기본값은 `cpu`(0.78s/프레임)지만 실배포는 cuda 라 **~29건/초**다. 그래서 프레임 13,144장
    전량도 수 분이면 끝난다 — CPU 기준으로 비용을 추정하면 15배 과대평가하게 된다.

    영상 단위(기본): 각도는 카메라/장면 속성이라 영상 안에서 사실상 불변이다
      (구 데이터셋 실측 프레임간 tilt 산포 p50 0.71°). 869회로 끝난다.
    프레임 단위(--per-frame): GPU 라 저렴하고, 프레임별 실측값 + **영상 내 안정성**
      (angle_stable) 까지 얻는다. PTZ 로 화각이 변한 영상도 잡힌다.

    출력: work/angle.jsonl(key=src_video) 또는 work/angle_frames.jsonl(key=frame key).
    """
    import requests

    led = jsonl_load(f"{WORK_DIR}/ledger.jsonl")
    path = f"{WORK_DIR}/angle_frames.jsonl" if per_frame else f"{WORK_DIR}/angle.jsonl"
    dkey = "key" if per_frame else "src_video"
    done = jsonl_load(path, key=dkey)
    # 영상별 대표 프레임 = 복사된 것 중 frame_index 중앙값에 가까운 것
    by_src: dict[str, list] = collections.defaultdict(list)
    for r in led.values():
        fp = os.path.join(FRAME_DIR, r["folder"], r["name"])
        if os.path.exists(fp):
            by_src[r["src_video"]].append((r["frame_index"], fp))
    todo = []
    if per_frame:
        for k, r in led.items():
            fp = os.path.join(FRAME_DIR, r["folder"], r["name"])
            if k in done and done[k].get("camera_angle"):
                continue
            if os.path.exists(fp):
                todo.append((k, fp))
        log(f"angle(프레임단위): 남은 {len(todo)} / 전체 {len(led)}")
    else:
        for src, items in by_src.items():
            if src in done and done[src].get("camera_angle"):
                continue
            items.sort()
            todo.append((src, items[len(items) // 2][1]))
        log(f"angle(영상단위): 영상 {len(by_src)} 중 남은 {len(todo)} (프레임 {len(led)}장은 전파)")
    if limit:
        todo = todo[:limit]
    if not todo:
        return
    url = os.environ.get("ANGLE_API_URL", "http://angle-dav2-1:8000")
    sess = requests.Session()
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def one(src: str, fp: str) -> dict:
        try:
            with open(fp, "rb") as f:
                resp = sess.post(f"{url}/angle",
                                 files={"file": (os.path.basename(fp), f, "image/jpeg")},
                                 timeout=600)
            resp.raise_for_status()
            j = resp.json()
            return {dkey: src, "rep_frame": fp, "camera_angle": j.get("camera_angle"),
                    "tilt_deg": j.get("tilt_deg"), "angle_method": j.get("angle_method")}
        except Exception as exc:  # noqa: BLE001 — per-item fail-forward
            return {dkey: src, "rep_frame": fp, "camera_angle": None,
                    "error": f"{type(exc).__name__}: {exc}"}

    ok = err = 0
    buf = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(one, s, f) for s, f in todo]
        for i, fut in enumerate(as_completed(futs), 1):
            rec = fut.result()
            buf.append(rec)
            ok, err = (ok + 1, err) if rec.get("camera_angle") else (ok, err + 1)
            if len(buf) >= 50:
                jsonl_append(path, buf)
                buf = []
            if i % 100 == 0:
                log(f"angle: {i}/{len(todo)} ok={ok} err={err}")
    if buf:
        jsonl_append(path, buf)
    log(f"angle 완료: ok={ok} err={err}")


# ────────────────────────── 4. score ──────────────────────────
def stage_score() -> None:
    import numpy as np

    led = jsonl_load(f"{WORK_DIR}/ledger.jsonl")
    d = np.load(f"{WORK_DIR}/embed.npz", allow_pickle=True)
    vec_of = {str(k): v for k, v in zip(d["key"], d["vec"])}
    banks = {}
    for ver in VERSIONS:
        p = f"{PROMPT_DIR}/{ver}.npz"
        if not os.path.exists(p):
            raise SystemExit(f"프롬프트 npz 없음: {p} — prompt_eval.py prompts 를 먼저 실행")
        z = np.load(p, allow_pickle=True)
        banks[ver] = {"vec": z["vec"].astype(np.float32), "cls": z["cls"].astype(int),
                      "prompt": [str(x) for x in z["prompt"]]}
    keys = [k for k in led if k in vec_of]
    if not keys:
        raise SystemExit("점수 낼 임베딩이 없다 — embed 스테이지를 먼저 실행")
    X = np.stack([vec_of[k] for k in keys])  # [N,1024]
    log(f"score: {len(keys)} 프레임 × 뱅크 {[len(b['cls']) for b in banks.values()]}")
    out = []
    CHUNK = 2000  # 20만 프레임까지도 메모리 안전
    per_ver: dict[str, list] = {v: [] for v in VERSIONS}
    for ver, bank in banks.items():
        classes = sorted(set(bank["cls"].tolist()))
        gidx = {c: np.flatnonzero(bank["cls"] == c) for c in classes}
        for s in range(0, len(keys), CHUNK):
            sims = X[s:s + CHUNK] @ bank["vec"].T
            for row in range(sims.shape[0]):
                scores, topk, tp = {}, {}, {}
                for c in classes:
                    sub = sims[row, gidx[c]]
                    scores[c] = float(sub.max())
                    k = min(TOPK, sub.size)
                    topk[c] = float(np.sort(sub)[-k:].mean())
                    tp[c] = bank["prompt"][int(gidx[c][int(sub.argmax())])]
                ranked = sorted(scores.items(), key=lambda kv: -kv[1])
                per_ver[ver].append({
                    "scores": {int(c): scores[c] for c in classes},
                    "pred": int(ranked[0][0]),
                    "confidence": ranked[0][1],
                    "margin": float(ranked[0][1] - ranked[1][1]) if len(ranked) > 1 else 0.0,
                    "top_prompt": tp[ranked[0][0]],
                    "class_best": {int(c): {"cos": scores[c], "prompt": tp[c],
                                            "topk_mean": topk[c]} for c in classes},
                    "pred_topk": int(max(topk.items(), key=lambda kv: kv[1])[0]),
                })
        log(f"score: {ver} 완료")
    for i, k in enumerate(keys):
        r = dict(led[k])
        for ver in VERSIONS:
            v = per_ver[ver][i]
            v["correct"] = v["pred"] == r["gt_class"]
            r[ver] = v
        p0, p4 = r[VERSIONS[0]]["pred"], r[VERSIONS[1]]["pred"]
        r["outcome"] = ("both_correct" if r[VERSIONS[0]]["correct"] and r[VERSIONS[1]]["correct"]
                        else f"only_{VERSIONS[1]}" if r[VERSIONS[1]]["correct"]
                        else f"only_{VERSIONS[0]}" if r[VERSIONS[0]]["correct"] else "both_wrong")
        # ── 버전 간 비교용 centering ──
        # 뱅크마다 코사인 절대 스케일이 다르므로(v1.0.8.4 는 3문장 템플릿이라 체계적으로 낮음)
        # 표본 내 클래스 평균을 빼서 **가산 오프셋**을 제거한 뒤 비교한다.
        # argmax 불변이므로 예측/정확도는 바뀌지 않는다. (승법 스케일 차이는 남는다 — 캘리브레이션 아님)
        cen = {}
        for ver in VERSIONS:
            sc = {int(c): float(v) for c, v in r[ver]["scores"].items()}
            m = sum(sc.values()) / len(sc)
            cen[ver] = {c: v - m for c, v in sc.items()}
        for c in cen[VERSIONS[0]]:
            r[f"dscore_{CLASS_NAMES[c]}"] = round(cen[VERSIONS[1]][c] - cen[VERSIONS[0]][c], 5)
        # GT 무관 축: 옛 답에서 멀어진 정도 / 새 답으로 당긴 정도 → shift_viz 산점도 좌표
        r["dscore_pred_v080"] = round(cen[VERSIONS[1]][p0] - cen[VERSIONS[0]][p0], 5)
        r["dscore_pred_v084"] = round(cen[VERSIONS[1]][p4] - cen[VERSIONS[0]][p4], 5)
        r["shift_mag"] = round(abs(r["dscore_pred_v084"] - r["dscore_pred_v080"]), 5)
        # 정답 기준 축: GT 클래스 상대점수의 before/after → tradeoff_viz 좌표 (대각선 위=개선)
        g = r["gt_class"]
        r["gt_rel_v080"] = round(cen[VERSIONS[0]][g], 5)
        r["gt_rel_v084"] = round(cen[VERSIONS[1]][g], 5)
        r["gt_rel_delta"] = round(cen[VERSIONS[1]][g] - cen[VERSIONS[0]][g], 5)

        a, b = CLASS_NAMES[p0], CLASS_NAMES[p4]
        r["pred_shift"] = f"{a}→{b}" if a != b else f"={a}"
        NRM = CLASS_NAMES[0]
        r["shift_direction"] = ("변화없음" if a == b else
                                "회수 (normal→이벤트)" if a == NRM else
                                "상실 (이벤트→normal)" if b == NRM else
                                "오분류 (이벤트→다른이벤트)")
        out.append(r)
    with open(f"{WORK_DIR}/scores.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    log(f"score 완료: {len(out)} → {WORK_DIR}/scores.json")


# ────────────────────────── 5. build ──────────────────────────
def stage_build(dataset_name: str = "source-h-frames") -> None:
    import fiftyone as fo
    import numpy as np
    from fiftyone import ViewField as F

    with open(f"{WORK_DIR}/scores.json", encoding="utf-8") as f:
        scores = json.load(f)
    # 프레임별 값이 있으면 그것을 쓰고, 없으면 영상 단위 값으로 폴백한다.
    angle_by_src = jsonl_load(f"{WORK_DIR}/angle.jsonl", key="src_video")
    angle_by_frame = jsonl_load(f"{WORK_DIR}/angle_frames.jsonl", key="key")
    log(f"build: camera_angle — 프레임별 {len(angle_by_frame)}건 / 영상단위 {len(angle_by_src)}건")
    # 영상 내 각도 라벨 일치 여부(angle_stable): 프레임별 값이 있을 때만 계산 가능
    _labs = collections.defaultdict(set)
    _tilts = collections.defaultdict(list)
    led_for_angle = jsonl_load(f"{WORK_DIR}/ledger.jsonl")
    for k, v in angle_by_frame.items():
        if not v.get("camera_angle") or k not in led_for_angle:
            continue
        sv = led_for_angle[k]["src_video"]
        _labs[sv].add(v["camera_angle"])
        if v.get("tilt_deg") is not None:
            _tilts[sv].append(float(v["tilt_deg"]))
    stable_by_src = {sv: (len(l) == 1) for sv, l in _labs.items()}
    spread_by_src = {sv: (max(t) - min(t)) for sv, t in _tilts.items() if t}
    if stable_by_src:
        _uns = sum(1 for v in stable_by_src.values() if not v)
        log(f"build: 영상내 각도 불일치 {_uns}/{len(stable_by_src)}편")
    if dataset_name in fo.list_datasets():
        fo.delete_dataset(dataset_name)
        log(f"기존 {dataset_name} 삭제 후 재생성")
    ds = fo.Dataset(dataset_name, persistent=True)
    ds.description = (
        "source-h 프레임 단위 재라벨 데이터셋 (NAS /source-h). GT=폴더명(사람이 프레임별로 재라벨). "
        "프롬프트 뱅크 v1.0.8.0 vs v1.0.8.4 제로샷 비교."
    )
    ds.tags = ["source-h", "frame-level", "relabeled"]

    samples = []
    for r in scores:
        fp = os.path.join(FRAME_DIR, r["folder"], r["name"])
        if not os.path.exists(fp):
            continue
        s = fo.Sample(filepath=fp)
        s["ground_truth"] = fo.Classification(label=CLASS_NAMES[r["gt_class"]])
        s["folder"] = r["folder"]
        s["src_video"] = r["src_video"]
        s["frame_index"] = r["frame_index"]
        s["camera"] = r["camera"]
        s["original_event"] = r["original_event"]
        # 재라벨 여부 — 이 데이터셋의 존재 이유
        s["relabeled"] = r["original_event"] not in ("unknown", CLASS_NAMES[r["gt_class"]])
        s["relabel_transition"] = fo.Classification(
            label=f"{r['original_event']}→{CLASS_NAMES[r['gt_class']]}"
        )
        for ver in VERSIONS:
            tag = ver.replace(".", "_")
            v = r[ver]
            s[f"pred_{tag}"] = fo.Classification(label=CLASS_NAMES[v["pred"]],
                                                 confidence=v["confidence"])
            s[f"correct_{tag}"] = bool(v["correct"])
            s[f"margin_{tag}"] = v["margin"]
            s[f"top_prompt_{tag}"] = v["top_prompt"]
            s[f"class_best_{tag}"] = fo.Classifications(classifications=[
                fo.Classification(label=CLASS_NAMES[int(c)], confidence=cb["cos"],
                                  prompt=cb["prompt"])
                for c, cb in sorted(v["class_best"].items(), key=lambda kv: -kv[1]["cos"])
            ])
        # camera_angle 은 원본 영상 단위로 계산해 프레임에 전파 (각도는 카메라 속성)
        ang = angle_by_frame.get(r["key"]) or angle_by_src.get(r["src_video"])
        if ang and ang.get("camera_angle"):
            s["camera_angle"] = fo.Classification(label=ang["camera_angle"])
            s["angle_method"] = ang.get("angle_method")
            t = ang.get("tilt_deg")
            s["tilt_deg"] = t
            s["tilt_bin"] = fo.Classification(
                label="unknown" if t is None else
                ("30+° (plan_view)" if t >= 30 else f"{int(t // 5) * 5:02d}-{int(t // 5) * 5 + 5:02d}°")
            )
            if r["src_video"] in stable_by_src:
                s["angle_stable"] = bool(stable_by_src[r["src_video"]])
                s["angle_tilt_spread"] = round(spread_by_src.get(r["src_video"], 0.0), 2)
        s["outcome"] = r["outcome"]
        s["pred_shift"] = fo.Classification(label=r["pred_shift"])
        s["shift_direction"] = fo.Classification(label=r["shift_direction"])
        for k in ("dscore_pred_v080", "dscore_pred_v084", "shift_mag",
                  "gt_rel_v080", "gt_rel_v084", "gt_rel_delta"):
            s[k] = r[k]
        for c in CLASS_NAMES.values():
            if f"dscore_{c}" in r:
                s[f"dscore_{c}"] = r[f"dscore_{c}"]
        s.tags = [r["folder"], r["outcome"]]
        samples.append(s)
    ds.add_samples(samples)
    log(f"build: {len(samples)} 샘플 적재")

    d = np.load(f"{WORK_DIR}/embed.npz", allow_pickle=True)
    vec_of = {str(k): v for k, v in zip(d["key"], d["vec"])}
    upd = {}
    for s in ds.select_fields(["id", "folder", "filepath"]):
        key = f"{s.folder}/{os.path.basename(s.filepath)}"
        if key in vec_of:
            upd[s.id] = vec_of[key].tolist()
    ds.set_values("embedding", upd, key_field="id")
    log(f"build: embedding {len(upd)}건")

    try:
        ds.add_dynamic_sample_fields()
    except Exception as exc:  # noqa: BLE001
        log(f"build: add_dynamic_sample_fields 실패 {exc!r}")
    try:
        import fiftyone.brain as fob
        fob.compute_visualization(ds, embeddings="embedding", brain_key="emb_viz", method="umap")
        log("build: emb_viz 완료")
    except Exception as exc:  # noqa: BLE001
        log(f"build: emb_viz 실패 {exc!r}")

    # ── 임의 2D 좌표를 brain key 로 등록 (차원축소가 아니라 before/after 산점도로 재활용) ──
    # ⚠️ Embeddings 패널은 **마지막에 쓰던 brain key 를 데이터셋 간에 기억**한다.
    #    구 source-h(영상 단위)에서 shift_viz/tradeoff_viz 를 쓰다가 이 데이터셋으로 오면
    #    "Dataset has no brain method run key 'shift_viz'" 로 패널이 죽는다(실측) →
    #    같은 이름으로 같은 의미의 축을 만들어 둔다.
    for bkey, xf, yf, desc in (
        ("shift_viz", "dscore_pred_v080", "dscore_pred_v084",
         "GT무관: x=옛답에서 멀어진 정도, y=새답으로 당긴 정도 (대각선=변화없음)"),
        ("tradeoff_viz", "gt_rel_v080", "gt_rel_v084",
         "정답기준: x=v1.0.8.0, y=v1.0.8.4 의 GT클래스 상대점수 (대각선 위=개선)"),
    ):
        try:
            import fiftyone.brain as fob
            pts = np.stack([np.asarray(ds.values(xf), dtype=np.float64),
                            np.asarray(ds.values(yf), dtype=np.float64)], axis=1)
            if ds.has_brain_run(bkey):
                ds.delete_brain_run(bkey)
            fob.compute_visualization(ds, points=pts, brain_key=bkey)
            log(f"build: {bkey} 완료 — {desc}")
        except Exception as exc:  # noqa: BLE001
            log(f"build: {bkey} 실패 {exc!r}")

    # 연속 float 은 App 이 카테고리 색을 못 만든다 → shift_mag 구간화 (변화없음 별도)
    try:
        mag = np.asarray(ds.values("shift_mag"), dtype=float)
        nz = mag[mag > 0]
        cuts = np.quantile(nz, [0.25, 0.5, 0.75]) if len(nz) else np.array([0.0])
        qn = ["Q1 작게", "Q2", "Q3", "Q4 크게"]
        ds.set_values("shift_mag_q", {
            i: fo.Classification(label="변화없음" if m <= 0
                                 else qn[int(np.searchsorted(cuts, m, side="right"))])
            for i, m in zip(ds.values("id"), mag)
        }, key_field="id")
        log(f"build: shift_mag_q (변화 {len(nz)}건 4분위 {np.round(cuts, 4).tolist()})")
    except Exception as exc:  # noqa: BLE001
        log(f"build: shift_mag_q 실패 {exc!r}")
    # embedding 을 "쓰는" 항목: 프롬프트 검색바 + 이미지 유사 정렬 (precomputed 필드라 모델 미호출)
    try:
        sys.path.insert(0, "/workspace")
        import fiftyone_pgvector as fpv
        fpv.build_text_search_index(ds, brain_key="text_search")
        log("build: text_search(prompt-capable) 인덱스 완료")
    except Exception as exc:  # noqa: BLE001
        log(f"build: text_search 실패 {exc!r}")
    try:
        sys.path.insert(0, "/workspace")
        import fiftyone_presentation as fpres
        fpres.apply(ds, dry_run=False,
                    workspaces=[("explore", "emb_viz", "ground_truth.label"),
                                ("relabel", "emb_viz", "relabel_transition.label"),
                                ("shift", "shift_viz", "pred_shift.label"),
                                ("tradeoff", "tradeoff_viz", "outcome"),
                                ("shift-where", "emb_viz", "shift_direction.label")])
    except Exception as exc:  # noqa: BLE001
        log(f"build: presentation 실패 {exc!r}")
    for nm, view in {
        "00_relabeled": ds.match(F("relabeled") == True),  # noqa: E712
        "01_disagreement": ds.match(F("outcome") != "both_correct"),
        "02_recover": ds.match(F("shift_direction.label") == "회수 (normal→이벤트)"),
        "03_lose": ds.match(F("shift_direction.label") == "상실 (이벤트→normal)"),
    }.items():
        try:
            ds.save_view(nm, view)
        except Exception as exc:  # noqa: BLE001
            log(f"build: view {nm} 실패 {exc!r}")
    log(f"build 완료 → http://10.0.0.10:5153/datasets/{dataset_name}")


# ────────────────────────── 6. report ──────────────────────────
def stage_report() -> None:
    os.makedirs(REPORT_DIR, exist_ok=True)
    with open(f"{WORK_DIR}/scores.json", encoding="utf-8") as f:
        scores = json.load(f)
    L: list[str] = []
    A = L.append
    A("# source-h 프레임 단위 재라벨 데이터셋 — 프롬프트 버전 비교\n")
    A(f"- 생성: {time.strftime('%Y-%m-%d %H:%M:%S')} | 소스: `{NAS_ROOT}` (업로드 진행 중일 수 있음)")
    A(f"- 프레임 {len(scores):,}개 / 원본영상 {len({r['src_video'] for r in scores}):,}개 "
      f"/ 카메라 {len({r['camera'] for r in scores})}곳")
    A("- GT = **폴더명(사람이 프레임별 재라벨)**. 구 버전의 영상단위 weak GT 와 다르다.\n")
    A("## 클래스 분포\n")
    A("| GT | 프레임 | 원본영상 |")
    A("|---|---|---|")
    for c, n in sorted(collections.Counter(r["gt_class"] for r in scores).items()):
        vids = len({r["src_video"] for r in scores if r["gt_class"] == c})
        A(f"| {CLASS_NAMES[c]} | {n:,} | {vids} |")
    A("\n## 재라벨 현황 (원래 영상 라벨 → 새 프레임 GT)\n")
    A("| 전이 | 프레임 | 판정 |")
    A("|---|---|---|")
    for (oe, gc), n in sorted(
        collections.Counter((r["original_event"], r["gt_class"]) for r in scores).items(),
        key=lambda kv: -kv[1],
    ):
        same = oe == CLASS_NAMES[gc]
        A(f"| {oe} → {CLASS_NAMES[gc]} | {n:,} | {'동일' if same else '**재라벨됨**'} |")
    A("\n## 정확도\n")
    A("| 버전 | 정확 | n | accuracy |")
    A("|---|---|---|---|")
    for ver in VERSIONS:
        k = sum(1 for r in scores if r[ver]["correct"])
        A(f"| {ver} | {k:,} | {len(scores):,} | {k / len(scores):.2%} |")
    A("\n### 클래스별 recall\n")
    A("| GT | n | " + " | ".join(VERSIONS) + " | Δ |")
    A("|---|---|---|---|---|")
    for c in sorted({r["gt_class"] for r in scores}):
        sub = [r for r in scores if r["gt_class"] == c]
        a0 = sum(1 for r in sub if r[VERSIONS[0]]["correct"]) / len(sub)
        a4 = sum(1 for r in sub if r[VERSIONS[1]]["correct"]) / len(sub)
        A(f"| {CLASS_NAMES[c]} | {len(sub):,} | {a0:.1%} | {a4:.1%} | {(a4 - a0) * 100:+.1f}%p |")
    A("\n### 변화 방향 (GT 무관)\n")
    A("| 방향 | n | " + " | ".join(VERSIONS) + " |")
    A("|---|---|---|---|")
    for k, n in sorted(collections.Counter(r["shift_direction"] for r in scores).items(),
                       key=lambda kv: -kv[1]):
        sub = [r for r in scores if r["shift_direction"] == k]
        a0 = sum(1 for r in sub if r[VERSIONS[0]]["correct"]) / len(sub)
        a4 = sum(1 for r in sub if r[VERSIONS[1]]["correct"]) / len(sub)
        A(f"| {k} | {n:,} | {a0:.1%} | {a4:.1%} |")
    A("")
    text = "\n".join(L)
    out = f"{REPORT_DIR}/sourceh_frames_report.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write(text)
    log(f"report → {out}")
    print("\n" + text)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=["scan", "copy", "embed", "angle", "score", "build", "report", "all"])
    ap.add_argument("--limit", type=int, default=None, help="한 번에 처리할 최대 개수 (업로드 중 배치용)")
    ap.add_argument("--dataset", default="source-h-frames")
    ap.add_argument("--per-frame", action="store_true",
                    help="angle 을 프레임 단위로 (GPU 라 저렴 + 영상내 안정성 산출)")
    args = ap.parse_args()
    for d in (FRAME_DIR, WORK_DIR, REPORT_DIR):
        os.makedirs(d, exist_ok=True)
    stages = (["scan", "copy", "embed", "angle", "score", "build", "report"]
              if args.stage == "all" else [args.stage])
    for st in stages:
        log(f"───── stage: {st} ─────")
        if st == "scan":
            stage_scan()
        elif st == "copy":
            stage_copy(args.limit)
        elif st == "embed":
            stage_embed(args.limit)
        elif st == "angle":
            stage_angle(args.limit, per_frame=args.per_frame)
        elif st == "score":
            stage_score()
        elif st == "build":
            stage_build(args.dataset)
        elif st == "report":
            stage_report()
    log("완료")


if __name__ == "__main__":
    sys.exit(main())
