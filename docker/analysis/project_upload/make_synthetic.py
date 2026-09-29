#!/usr/bin/env python3
"""업로드 킷(project_upload) 검증/e2e 용 합성 번들 생성기.

UPLOAD_SPEC.md 계약을 그대로 만족하는 최소 번들을 만든다 — manifest.json / images/ /
image_embeddings.npz / prompts.csv / prompt_embeddings.npz (+ gt.csv, gt 모드만).
포맷·상수·정규식은 전부 bundle_common 에서만 가져오고 여기서 복제하지 않는다.

클래스 3개(fire/smoke/normal) × 버전 2개("v1.0","v2.0.beta") 고정 — 두 버전명은
vtag()="v10"/"v20beta", vt()="v1_0"/"v2_0_beta" 로 서로 다른 sanitize 경로(점·영문 혼합)를
동시에 태운다. v2.0.beta 의 "fire" 문장 중 마지막 2개는 벡터를 일부러 smoke centroid 로
만들어(class 라벨은 fire 그대로) purity<1 채점 경로를 유도한다.

사용 예:
    python3 make_synthetic.py /data/fiftyone/uploads/_upe2e_gt --mode gt
    python3 make_synthetic.py /data/fiftyone/uploads/_upe2e_nogt --mode nogt
    python3 make_synthetic.py /data/fiftyone/uploads/_upe2e_fold --mode folders
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re

import numpy as np
from PIL import Image

import bundle_common as bc

CLASSES = ("fire", "smoke", "normal")
VERSIONS = ("v1.0", "v2.0.beta")  # vtag/vt sanitize 경로(점·영문 혼합)를 태우는 이름 (하이픈은 VERSION_RE 금지)
SENT_PER_CLASS = 5
CAMERAS = ("cam1", "cam2")
IMG_SIZE = 48
IMG_NOISE_SCALE = 0.3
SENT_NOISE_SCALE = 0.15
GT_DROP_EVERY = 20  # idx % 20 == 0 인 이미지는 GT 행을 의도적으로 누락 (5% ≈ partial-GT 경고)
WRONG_LABEL_CLASS = "fire"
WRONG_LABEL_TARGET = "smoke"  # v2.0.beta 오답 라벨 문장이 실제로 가리키는 클래스
WRONG_LABEL_COUNT = 2

CLASS_COLORS = {
    "fire": (215, 60, 40),
    "smoke": (120, 120, 120),
    "normal": (60, 160, 95),
}


def _dataset_name_from_out_dir(out_dir: str) -> str:
    """out_dir basename → manifest 'dataset' 이름.

    run_e2e.sh 는 안전 규약상 "_upe2e_*" 처럼 언더스코어로 시작하는 디렉토리명을 쓰지만,
    bundle_common.DATASET_NAME_RE 는 첫 글자가 영숫자여야 한다(외부 번들 계약이라 완화 불가 —
    ingest_bundle.py 의 `--name` CLI 오버라이드도 같은 정규식을 그대로 적용받는다). 그래서
    basename 앞의 비영숫자 문자만 벗겨 정본 규칙을 충족시킨다 — run_e2e.sh 는 실제 FiftyOne
    데이터셋 이름으로 이 값과 동일하게 언더스코어를 뗀 이름을 `--name` 에 넘긴다(언더스코어를
    다시 붙이지 않는다).
    """
    base = os.path.basename(os.path.normpath(out_dir))
    name = re.sub(r"^[^A-Za-z0-9]+", "", base) or "bundle"
    if not bc.DATASET_NAME_RE.match(name):
        name = re.sub(r"[^A-Za-z0-9._-]", "_", name)[:64]
        if not name or not name[0].isalnum():
            name = "b" + name
    return name[:64]


def _make_class_centroids(dim: int, rng: np.random.Generator) -> dict[str, np.ndarray]:
    """클래스별 centroid — QR 분해로 완전직교(요구한 "직교성 높음"의 상한)를 만든다."""
    n = len(CLASSES)
    if dim < n:
        raise SystemExit(f"[make_synthetic] --dim({dim}) 이 클래스 수({n}) 미만 — 직교 centroid 생성 불가")
    mat = rng.normal(size=(dim, dim))
    q, _ = np.linalg.qr(mat)
    return {c: q[i].astype(np.float32) for i, c in enumerate(CLASSES)}


def _save_image(path: str, cls: str, rng: np.random.Generator) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    base = np.array(CLASS_COLORS[cls], dtype=np.int16)
    noise = rng.integers(-25, 26, size=(IMG_SIZE, IMG_SIZE, 3), dtype=np.int16)
    arr = np.clip(base[None, None, :] + noise, 0, 255).astype(np.uint8)
    Image.fromarray(arr, mode="RGB").save(path, format="JPEG", quality=85)


def _build_images(
    bundle_dir: str,
    mode: str,
    n_per_class: int,
    dim: int,
    centroids: dict[str, np.ndarray],
    rng: np.random.Generator,
) -> tuple[list[str], np.ndarray, list[dict]]:
    images_dir = os.path.join(bundle_dir, bc.IMAGES_DIR)
    keys: list[str] = []
    raw_vecs: list[np.ndarray] = []
    gt_kept: list[dict] = []
    gt_dropped = 0
    idx = 0
    for cls in CLASSES:
        centroid = centroids[cls]
        for i in range(n_per_class):
            idx += 1
            camera = CAMERAS[idx % 2]
            if mode == "folders":
                rel = f"{cls}/{i + 1:04d}.jpg"
            else:
                rel = f"{camera}_{idx:04d}.jpg"
            _save_image(os.path.join(images_dir, rel), cls, rng)
            noise = rng.normal(size=dim).astype(np.float32)
            raw_vecs.append(centroid + noise * IMG_NOISE_SCALE)
            keys.append(rel)
            if mode == "gt":
                if idx % GT_DROP_EVERY == 0:
                    gt_dropped += 1
                else:
                    gt_kept.append({"key": rel, "class": cls, "camera": camera})
    vec = bc.l2_normalize(np.stack(raw_vecs))
    print(f"[make_synthetic] images: {len(keys)}장 (클래스 {len(CLASSES)}×{n_per_class}), mode={mode}")
    if mode == "gt":
        print(f"[make_synthetic] gt.csv: {len(gt_kept)}행 유지 / {gt_dropped}행 의도적 누락 (partial-GT 경고 유도)")
    return keys, vec, gt_kept


def _build_prompts(dim: int, centroids: dict[str, np.ndarray], rng: np.random.Generator):
    rows: list[dict] = []
    raw_vecs: list[np.ndarray] = []
    wrong_used = 0
    for version in VERSIONS:
        for cls in CLASSES:
            for s in range(SENT_PER_CLASS):
                text = f"{version} 참조문장 — {cls} 장면 #{s + 1}"
                use_wrong = (
                    version == "v2.0.beta"
                    and cls == WRONG_LABEL_CLASS
                    and s >= SENT_PER_CLASS - WRONG_LABEL_COUNT
                )
                base = centroids[WRONG_LABEL_TARGET] if use_wrong else centroids[cls]
                noise = rng.normal(size=dim).astype(np.float32)
                raw_vecs.append(base + noise * SENT_NOISE_SCALE)
                rows.append({"version": version, "class": cls, "text": text})
                if use_wrong:
                    wrong_used += 1
    vec = bc.l2_normalize(np.stack(raw_vecs))
    print(
        f"[make_synthetic] prompts: {len(rows)}행 "
        f"({len(VERSIONS)}버전×{len(CLASSES)}클래스×{SENT_PER_CLASS}문장), "
        f"오답라벨 {wrong_used}개(class={WRONG_LABEL_CLASS!r} 인데 벡터는 {WRONG_LABEL_TARGET!r} centroid)"
    )
    return rows, vec


def build_bundle(out_dir: str, mode: str, dim: int, n_per_class: int, seed: int) -> None:
    if mode not in ("gt", "nogt", "folders"):
        raise SystemExit(f"[make_synthetic] --mode 값 위반: {mode!r} (gt|nogt|folders)")
    if n_per_class < 1:
        raise SystemExit(f"[make_synthetic] --n-per-class 는 1 이상이어야 함: {n_per_class}")

    rng = np.random.default_rng(seed)
    os.makedirs(out_dir, exist_ok=True)
    dataset_name = _dataset_name_from_out_dir(out_dir)

    centroids = _make_class_centroids(dim, rng)
    keys, img_vec, gt_kept = _build_images(out_dir, mode, n_per_class, dim, centroids, rng)
    prompt_rows, prompt_vec = _build_prompts(dim, centroids, rng)

    np.savez(os.path.join(out_dir, bc.IMG_NPZ), key=np.array(keys), vec=img_vec)
    np.savez(os.path.join(out_dir, bc.PROMPT_NPZ), vec=prompt_vec)

    with open(os.path.join(out_dir, bc.PROMPTS_CSV), "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["version", "class", "text"])
        w.writeheader()
        w.writerows(prompt_rows)

    if mode == "gt":
        with open(os.path.join(out_dir, bc.GT_CSV), "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["key", "class", "camera"])
            w.writeheader()
            w.writerows(gt_kept)

    manifest = {
        "format_version": bc.FORMAT_VERSION,
        "dataset": dataset_name,
        "model_name": "synthetic-testenc",
        "embedding_dim": dim,
        "gt_mode": "folders" if mode == "folders" else "auto",
    }
    with open(os.path.join(out_dir, bc.MANIFEST), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"[make_synthetic] manifest: dataset={dataset_name!r} gt_mode={manifest['gt_mode']!r} dim={dim}")
    print(f"[make_synthetic] 완료: {out_dir}")


def main() -> None:
    ap = argparse.ArgumentParser(description="업로드 킷 검증/e2e 용 합성 번들 생성기")
    ap.add_argument("out_dir", help="번들을 생성할 디렉토리 (basename → manifest dataset 이름)")
    ap.add_argument("--mode", required=True, choices=("gt", "nogt", "folders"))
    ap.add_argument("--dim", type=int, default=64)
    ap.add_argument("--n-per-class", type=int, default=30)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()
    build_bundle(args.out_dir, args.mode, args.dim, args.n_per_class, args.seed)


if __name__ == "__main__":
    main()
