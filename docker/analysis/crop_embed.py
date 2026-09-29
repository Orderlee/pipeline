#!/usr/bin/env python3
"""SAM3 객체 박스로 크롭 → PE-Core 임베딩 → 프레임당 평균풀 벡터.

가설: 전체프레임 벡터 1024-d 의 대부분이 배경(=카메라 정체성)이라 이벤트 신호가 묻힌다.
객체 박스만 잘라 임베딩하면 배경이 물리적으로 빠진다.

**프롬프트는 모든 이미지에 동일하게 넣는다.** 표본의 GT 로 프롬프트를 고르면 누수다.
고정 어휘는 배포 시스템이 실제로 하는 일이므로 누수가 아니다 — 다만 "크롭이 좋아진 게
배경 제거 덕인가 SAM3 탐지력 덕인가"가 섞이므로, 프롬프트별 박스수·최고점수를
`det_features` 로 따로 저장해 **탐지신호만으로 채점하는 대조군**을 가능하게 한다.

산출: crop_embeddings.npz  {ids, vectors(1024), n_boxes, det_features, det_cols}
재개 가능 — 체크포인트를 읽어 남은 것만 처리한다.
"""

import io
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import requests
from PIL import Image

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CKPT = f"{OUT}/crop_embeddings.npz"
SAM3 = os.environ.get("SAM3_API_URL", "http://docker-sam3-1:8002")
EMB = os.environ.get("EMBEDDING_API_URL", "http://embedding-service:8003")

PROMPTS = ["person", "fire", "smoke", "person lying on the ground"]
SCORE_THR = 0.3
MAX_MASKS = 10
PAD = 0.12  # 타이트한 박스는 맥락을 다 버린다 — 12% 여유
MIN_SIDE = 32  # 336 으로 늘릴 때 의미 없는 티끌 제외
# SAM3 워커 3개가 각 5.25GB 로 15.72GiB 를 이미 채운다 — 동시 요청 여유가 0 이라
# WORKERS=4 면 즉시 CUDA OOM(500) 이 난다(2026-09-14 실측). 직렬로 간다.
WORKERS = 1
SEG_MAX_SIDE = int(os.environ.get("SEG_MAX_SIDE", "1024"))  # 원본 그대로면 OOM. 축소본으로 탐지, 박스는 원본 좌표로 복원
RETRIES = 5
# GPU1 은 SAM3 워커 3개가 각 5.28GB 로 15.85GB 를 전부 쥐고 있고 PyTorch 캐싱 할당자가
# high-water mark 를 안 놓는다 → 직렬화만으로는 OOM 이 반복된다. 500 이 나면 /unload 로
# 캐시를 비우고 재시도한다(모델은 다음 요청에 lazy 재적재, ~8s).
UNLOAD_ROUNDS = 1  # 4 는 3워커를 전부 내려 재적재 폭풍을 만든다(실측 5분간 unload 100회)

_lock = threading.Lock()
_unload_lock = threading.Lock()
_done: dict[str, tuple] = {}
_t0 = time.time()


def _downscale(img):
    """탐지용 축소본 + 원본 복원 배율. 크롭은 원본에서 하므로 해상도 손실이 없다."""
    m = max(img.width, img.height)
    if m <= SEG_MAX_SIDE:
        return img, 1.0
    s = SEG_MAX_SIDE / m
    return img.resize((max(1, int(img.width * s)), max(1, int(img.height * s)))), s


def sam3_boxes(img, sess):
    small, scale = _downscale(img)
    buf = io.BytesIO()
    small.convert("RGB").save(buf, format="JPEG", quality=92)
    payload = buf.getvalue()
    for attempt in range(RETRIES):
        try:
            r = sess.post(
                f"{SAM3}/segment",
                files={"file": ("i.jpg", payload, "image/jpeg")},
                data={
                    "prompts_json": json.dumps(PROMPTS),
                    "score_threshold": str(SCORE_THR),
                    "max_masks_per_prompt": str(MAX_MASKS),
                },
                timeout=180,
            )
            r.raise_for_status()
            break
        except Exception:  # OOM/503 은 일시적 — 캐시를 비우고 재시도
            if attempt == RETRIES - 1:
                raise
            with _unload_lock:
                for _ in range(UNLOAD_ROUNDS):
                    try:
                        sess.post(f"{SAM3}/unload", timeout=90)
                    except Exception:
                        pass
            time.sleep(3.0 * (attempt + 1))
    inv = 1.0 / scale
    # mask_rle 은 크다 — bbox/score/class 만 들고 나머지는 버린다
    return [
        ([c * inv for c in d["mask_bbox"]], float(d["score"]), d["prompt_class"])
        for d in r.json().get("detections", [])
        if d.get("mask_bbox")
    ]


def crop(img, box):
    x1, y1, x2, y2 = box
    w, h = x2 - x1, y2 - y1
    if w < MIN_SIDE or h < MIN_SIDE:
        return None
    px, py = w * PAD, h * PAD
    x1, y1 = max(0, x1 - px), max(0, y1 - py)
    x2, y2 = min(img.width, x2 + px), min(img.height, y2 + py)
    if x2 - x1 < MIN_SIDE or y2 - y1 < MIN_SIDE:
        return None
    return img.crop((int(x1), int(y1), int(x2), int(y2)))


def embed(pil, sess):
    buf = io.BytesIO()
    pil.convert("RGB").save(buf, format="JPEG", quality=90)
    r = sess.post(f"{EMB}/embed", files={"file": ("c.jpg", buf.getvalue(), "image/jpeg")}, timeout=120)
    r.raise_for_status()
    return r.json()["vector"]


def one(args):
    sid, path = args
    sess = requests.Session()
    try:
        img = Image.open(path)
        img.load()
        boxes = sam3_boxes(img, sess)
        det = []
        for p in PROMPTS:
            hits = [s for _, s, c in boxes if c == p]
            det.extend([len(hits), max(hits) if hits else 0.0])
        vecs = []
        if boxes:
            for b, _, _ in boxes:
                c = crop(img, b)
                if c is not None:
                    vecs.append(embed(c, sess))
        v = np.mean(np.asarray(vecs, dtype=np.float32), axis=0) if vecs else np.zeros(1024, dtype=np.float32)
        with _lock:
            _done[sid] = (v, len(vecs), np.asarray(det, dtype=np.float32))
            n = len(_done)
            if n % 200 == 0:
                el = time.time() - _t0
                print(f"  {n} 완료 · {el / 60:.1f}분 · {el / n:.2f}s/장 · 남은 ~{(TOTAL - n) * el / n / 60:.0f}분", flush=True)
                save()
    except Exception as exc:  # noqa: BLE001 — per-file fail-forward
        with _lock:
            _done[sid] = (np.zeros(1024, dtype=np.float32), -1, np.zeros(len(PROMPTS) * 2, dtype=np.float32))
        print(f"  실패 {os.path.basename(path)}: {type(exc).__name__} {exc}", flush=True)
    finally:
        sess.close()


def save():
    ids = list(_done)
    np.savez_compressed(
        CKPT,
        ids=np.array(ids),
        vectors=np.stack([_done[i][0] for i in ids]),
        n_boxes=np.array([_done[i][1] for i in ids]),
        det_features=np.stack([_done[i][2] for i in ids]),
        det_cols=np.array([f"{p}__{k}" for p in PROMPTS for k in ("n", "maxscore")]),
        prompts=np.array(PROMPTS),
    )


def selftest():
    """크롭 기하만 검증 — 좌표가 이미지 밖으로 나가거나 뒤집히면 조용히 쓰레기가 나온다."""
    img = Image.new("RGB", (200, 100))
    assert crop(img, [0, 0, 10, 10]) is None, "MIN_SIDE 미만이 통과됨"
    c = crop(img, [50, 20, 150, 80])
    assert c is not None and c.width <= 200 and c.height <= 100, "패딩이 이미지 밖으로 나감"
    assert crop(img, [0, 0, 200, 100]).size == (200, 100), "전체 박스 패딩이 클램프 안 됨"
    big = Image.new("RGB", (SEG_MAX_SIDE * 2, SEG_MAX_SIDE))
    small, sc = _downscale(big)
    assert max(small.size) == SEG_MAX_SIDE, "축소 상한 미적용"
    # 축소본 박스를 원본 좌표로 되돌리면 원래 값이어야 한다 — 틀리면 엉뚱한 데를 자른다.
    # SEG_MAX_SIDE 에 독립이어야 한다(상수를 바꿔도 성립해야 진짜 검사다).
    assert abs(small.width / 2 / sc - big.width / 2) < 1.0, "박스 역스케일 오류"
    tiny = Image.new("RGB", (SEG_MAX_SIDE // 2, SEG_MAX_SIDE // 4))
    assert _downscale(tiny)[1] == 1.0, "상한 이하인데 축소함"
    print("selftest 통과 (크롭 기하 + 축소/역스케일)")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
        raise SystemExit

    import fiftyone as fo

    ds = fo.load_dataset("sourcei")
    ids, paths, gts = ds.values(["id", "filepath", "ground_truth.label"])
    # 희귀 이벤트 클래스부터. 중간에 멈춰도 신호가 있는 쪽은 확보된다.
    PRIO = {"fire": 0, "falldown": 1, "smoke": 2}  # normal 은 9 → 맨 뒤
    order = sorted(range(len(ids)), key=lambda i: (PRIO.get(gts[i], 9), i))
    todo = [(ids[i], paths[i]) for i in order]
    import collections as _c
    print("처리 순서:", list(_c.Counter(gts[i] for i in order).keys()), flush=True)

    if os.path.exists(CKPT):
        d = np.load(CKPT, allow_pickle=True)
        for i, v, nb, df in zip(d["ids"], d["vectors"], d["n_boxes"], d["det_features"]):
            if int(nb) >= 0:  # 실패분은 재시도
                _done[str(i)] = (v, int(nb), df)
        todo = [(i, p) for i, p in todo if i not in _done]
        print(f"체크포인트 {len(_done):,} 적재 → 남은 {len(todo):,}")

    lim = int(os.environ.get("LIMIT", "0"))
    if lim:
        todo = todo[:lim]
        print(f"LIMIT={lim} — 처리량 시험 모드")
    TOTAL = len(_done) + len(todo)
    print(f"총 {TOTAL:,} 프레임 / 워커 {WORKERS} / 프롬프트 {PROMPTS}", flush=True)
    selftest()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(one, todo))
    save()
    nb = np.array([_done[i][1] for i in _done])
    print(f"\n완료 {len(_done):,} · 박스 0개 {int((nb == 0).sum()):,} · 실패 {int((nb < 0).sum()):,}")
    print(f"프레임당 크롭 중앙 {np.median(nb[nb > 0]):.0f} · 저장 {CKPT}")
