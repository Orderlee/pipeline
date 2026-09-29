"""업로드 번들 킷 공용 계약 — 상수·로더·sanitize. 정본 스펙: UPLOAD_SPEC.md.

정본 참조 (규칙을 여기서 바꾸면 안 됨):
- 필드 접미사: prompt_geometry.vtag / vt(=version.replace('.','_'))
- 뱅크 gidx 블록: prompt_geometry.GIDX_OFFSET=100_000
- 청크: prompt_geometry.bank_top2_stream (frame 1024 × sentence 2048)
- brain key: 플러그인 3종 하드코딩 'emb_viz'
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import posixpath
import re

import numpy as np

FORMAT_VERSION = 1
UPLOAD_ROOT = os.environ.get("UPLOAD_ROOT", "/data/fiftyone/uploads")
BRAIN_KEY = "emb_viz"
GIDX_OFFSET = 100_000
EMBED_DIM_DEFAULT = 1024
FRAME_BATCH = 1024
SENT_BLOCK = 2048
PROMPTS_SUFFIX = "-prompts"
UMAP_SEED = 42

MANIFEST = "manifest.json"
IMAGES_DIR = "images"
IMG_NPZ = "image_embeddings.npz"
PROMPTS_CSV = "prompts.csv"
PROMPT_NPZ = "prompt_embeddings.npz"
GT_CSV = "gt.csv"
ARTIFACTS_DIR = "_artifacts"

DATASET_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
# version 은 영숫자+점만 — 하이픈/언더스코어를 허용하면 vtag()/vt() 의 sanitize 가 정본
# (prompt_geometry.vtag, 패널 version_to_winner_field 의 필드 역해석)과 갈라져 패널 조인이
# 조용히 깨진다 (codex 리뷰 실증: "v2.0-beta" → 킷 v20_beta vs 정본 v20-beta). 이 도메인에선
# sanitize 가 항등이라 정본과 100% 동일해진다.
VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.]{0,31}$")
# class 는 영숫자+언더스코어만 — 점/하이픈을 허용하면 cos_best_<class_suffix> 가 서로 다른
# 클래스("a.b"/"a-b")에서 같은 필드명으로 붕괴해 조용히 덮어쓴다. 이 도메인에선 suffix 가 항등.
CLASS_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_]{0,31}$")
IMG_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}

PURITY_EDGES = ((0.25, "0-25%"), (0.50, "25-50%"), (0.75, "50-75%"), (0.90, "75-90%"))


class BundleError(ValueError):
    """번들 계약 위반 (fail-closed 대상)."""


# ── sanitize (정본 미러) ────────────────────────────────────────────────────

def vtag(version: str) -> str:
    """v1.0.8.4 → v1084 — margin_*/winner_* 접미사 (prompt_geometry.vtag 복제)."""
    t = "v" + "".join(version.lstrip("vV").split("."))
    return re.sub(r"[^0-9A-Za-z_]", "_", t)


def vt(version: str) -> str:
    """v1.0.8.4 → v1_0_8_4 — pred_*/top_prompt_* 접미사 (정본 vt 복제 + 필드 안전화)."""
    return re.sub(r"[^0-9A-Za-z_]", "_", version.replace(".", "_"))


def class_suffix(cls: str) -> str:
    """cos_best_<class> 접미사용 sanitize."""
    return re.sub(r"[^0-9A-Za-z_]", "_", cls)


# ── 수치 헬퍼 ───────────────────────────────────────────────────────────────

def l2_normalize(a: np.ndarray) -> np.ndarray:
    """행 단위 L2 정규화 (float32). 영벡터는 그대로 0 유지 — 검증 단계에서 이미 거른다."""
    a = np.asarray(a, dtype=np.float32)
    n = np.linalg.norm(a, axis=1, keepdims=True)
    return a / np.maximum(n, 1e-9)


def check_vectors(vec: np.ndarray, dim: int, what: str) -> list[str]:
    """NaN/Inf/영벡터/차원 검사 → 오류 문자열 목록 (비면 통과)."""
    errs: list[str] = []
    if vec.ndim != 2:
        return [f"{what}: vec 은 2차원이어야 함 (현재 {vec.ndim}차원)"]
    if vec.shape[1] != dim:
        errs.append(f"{what}: 차원 {vec.shape[1]} ≠ manifest embedding_dim {dim}")
    if not np.isfinite(vec).all():
        errs.append(f"{what}: NaN/Inf 포함")
    else:
        zero = int((np.linalg.norm(vec, axis=1) < 1e-9).sum())
        if zero:
            errs.append(f"{what}: 영벡터 {zero}개")
    return errs


# ── 로더 (계약 강제) ────────────────────────────────────────────────────────

def load_manifest(bundle_dir: str) -> dict:
    p = os.path.join(bundle_dir, MANIFEST)
    if not os.path.isfile(p):
        raise BundleError(f"{MANIFEST} 없음: {p}")
    try:
        # utf-8-sig: prompts.csv/gt.csv 와 동일하게 BOM 허용(Windows 툴 산출물 대비) + with 로 fd 누수 방지
        with open(p, encoding="utf-8-sig") as f:
            m = json.load(f)
    except json.JSONDecodeError as e:
        raise BundleError(f"{MANIFEST} JSON 파싱 실패: {e}") from e
    # 최상위가 dict 가 아니면(배열 등) 아래 m.get() 이 AttributeError 로 새나간다 — validate_bundle.py
    # 는 BundleError 만 잡으므로 여기서 미리 걸러야 (raw traceback 대신) 한국어 오류로 fail-closed.
    if not isinstance(m, dict):
        raise BundleError(f"{MANIFEST}: 최상위 값이 JSON 객체가 아님 (현재 타입 {type(m).__name__})")
    if m.get("format_version") != FORMAT_VERSION:
        raise BundleError(f"format_version {m.get('format_version')!r} ≠ {FORMAT_VERSION}")
    name = m.get("dataset", "")
    if not isinstance(name, str) or not DATASET_NAME_RE.match(name):
        raise BundleError(f"dataset 이름 규칙 위반: {name!r} (^[A-Za-z0-9][A-Za-z0-9._-]{{0,63}}$)")
    if name.endswith(PROMPTS_SUFFIX):
        raise BundleError(f"dataset 이름은 {PROMPTS_SUFFIX!r} 로 끝날 수 없음 (예약): {name!r}")
    if isinstance(m.get("format_version"), bool):  # JSON true 가 int 1 로 통과하는 것 차단
        raise BundleError(f"format_version 은 정수여야 함: {m['format_version']!r}")
    m.setdefault("model_name", "unknown")
    m.setdefault("embedding_dim", EMBED_DIM_DEFAULT)
    m.setdefault("gt_mode", "auto")
    d = m["embedding_dim"]
    if isinstance(d, bool) or not isinstance(d, int) or d < 2:
        raise BundleError(f"embedding_dim 은 2 이상의 정수여야 함: {d!r}")
    if m["gt_mode"] not in ("auto", "csv", "folders", "none"):
        raise BundleError(f"gt_mode 값 위반: {m['gt_mode']!r}")
    return m


def _safe_rel_key(key: str) -> bool:
    """images/ 상대경로 key 안전성: 절대경로·상위 탈출·백슬래시 금지."""
    if not key or key.startswith("/") or "\\" in key:
        return False
    norm = posixpath.normpath(key)
    return norm == key and not norm.startswith("..")


def load_image_npz(bundle_dir: str, dim: int) -> tuple[list[str], np.ndarray]:
    """→ (keys, vec float32 [N,dim] 재정규화 완료). 계약 위반 시 BundleError."""
    p = os.path.join(bundle_dir, IMG_NPZ)
    if not os.path.isfile(p):
        raise BundleError(f"{IMG_NPZ} 없음")
    # allow_pickle=False(기본값) — 외부 작업자 업로드물이라 object-array pickle 임의코드실행을
    # 막는다. 계약(str key + float32 vec)엔 pickle 이 애초에 필요 없다.
    with np.load(p) as z:
        if "key" not in z or "vec" not in z:
            raise BundleError(f"{IMG_NPZ}: 'key'/'vec' 키 필요 (현재 {sorted(z.files)})")
        keys = [str(k) for k in z["key"]]
        vec = np.asarray(z["vec"], dtype=np.float32)
    if not keys:
        raise BundleError(f"{IMG_NPZ}: 이미지 0장")
    if len(keys) != len(vec):
        raise BundleError(f"{IMG_NPZ}: key {len(keys)}개 ≠ vec {len(vec)}행")
    if len(keys) != len(set(keys)):
        raise BundleError(f"{IMG_NPZ}: key 중복 존재")
    bad = [k for k in keys if not _safe_rel_key(k)]
    if bad:
        raise BundleError(f"{IMG_NPZ}: 비정상 key {len(bad)}개 (예: {bad[:3]})")
    errs = check_vectors(vec, dim, IMG_NPZ)
    if errs:
        raise BundleError("; ".join(errs))
    return keys, l2_normalize(vec)


def load_prompts(bundle_dir: str, dim: int) -> tuple[list[dict], np.ndarray, list[str]]:
    """→ (rows[{version,class,text}], vec float32 [M,dim] 재정규화, versions CSV 등장순).

    강제: 헤더 version,class,text · 버전 연속 그룹핑 · 버전당 < GIDX_OFFSET ·
    npz 행수 == CSV 행수 · vtag/vt 충돌 없음.
    """
    pc = os.path.join(bundle_dir, PROMPTS_CSV)
    pn = os.path.join(bundle_dir, PROMPT_NPZ)
    if not os.path.isfile(pc):
        raise BundleError(f"{PROMPTS_CSV} 없음")
    if not os.path.isfile(pn):
        raise BundleError(f"{PROMPT_NPZ} 없음")
    with open(pc, encoding="utf-8-sig", newline="") as f:
        rd = csv.DictReader(f)
        need = {"version", "class", "text"}
        if rd.fieldnames is None or not need.issubset(set(rd.fieldnames)):
            raise BundleError(f"{PROMPTS_CSV}: 헤더에 version,class,text 필요 (현재 {rd.fieldnames})")
        if len(rd.fieldnames) != len(set(rd.fieldnames)):
            raise BundleError(f"{PROMPTS_CSV}: 중복 헤더 존재 {rd.fieldnames}")
        rows = []
        for i, r in enumerate(rd, start=2):
            if r.get(None):  # 헤더보다 열이 많은 행 — DictReader 가 잉여분을 None 키에 숨긴다
                raise BundleError(f"{PROMPTS_CSV}:{i}: 헤더({len(rd.fieldnames)}열)보다 열이 많음 (미인용 콤마?)")
            v, c, t = (r.get("version") or "").strip(), (r.get("class") or "").strip(), (r.get("text") or "").strip()
            if not VERSION_RE.match(v):
                raise BundleError(f"{PROMPTS_CSV}:{i}: version 규칙 위반 {v!r}")
            if not CLASS_RE.match(c):
                raise BundleError(f"{PROMPTS_CSV}:{i}: class 규칙 위반 {c!r}")
            if not t:
                raise BundleError(f"{PROMPTS_CSV}:{i}: text 빈 문자열")
            rows.append({"version": v, "class": c, "text": t})
    if not rows:
        raise BundleError(f"{PROMPTS_CSV}: 행 0개")
    versions: list[str] = []
    for r in rows:
        if r["version"] not in versions:
            versions.append(r["version"])
    # 연속 그룹핑 검사
    seen_done: set[str] = set()
    prev = None
    for r in rows:
        v = r["version"]
        if v != prev:
            if v in seen_done:
                raise BundleError(f"{PROMPTS_CSV}: version {v!r} 행이 비연속 (버전별 그룹핑 필요)")
            if prev is not None:
                seen_done.add(prev)
            prev = v
    per = {v: sum(1 for r in rows if r["version"] == v) for v in versions}
    over = {v: n for v, n in per.items() if n >= GIDX_OFFSET}
    if over:
        raise BundleError(f"버전당 문장수 한계({GIDX_OFFSET}) 초과: {over}")
    # 버전별 최종 필드명 5종의 전역 서로소 검사 — 같은 접미사 네임스페이스 충돌뿐 아니라
    # 교차 충돌(예: version='margin_v10' 의 pred_ 필드 == 다른 버전의 pred_margin_ 필드)까지 잡는다
    owner: dict[str, str] = {}
    for v in versions:
        for fname in (f"pred_{vt(v)}", f"top_prompt_{vt(v)}", f"pred_margin_{vtag(v)}",
                      f"winner_gidx_{vtag(v)}", f"pred_correct_{vtag(v)}"):
            if fname in owner and owner[fname] != v:
                raise BundleError(f"버전 필드명 충돌: {owner[fname]!r} vs {v!r} → 같은 필드 {fname!r}")
            owner[fname] = v
    with np.load(pn) as z:  # allow_pickle=False(기본값) — load_image_npz 와 동일 사유
        if "vec" not in z:
            raise BundleError(f"{PROMPT_NPZ}: 'vec' 키 필요 (현재 {sorted(z.files)})")
        vec = np.asarray(z["vec"], dtype=np.float32)
    if len(vec) != len(rows):
        raise BundleError(f"{PROMPT_NPZ} 행수 {len(vec)} ≠ {PROMPTS_CSV} 행수 {len(rows)}")
    errs = check_vectors(vec, dim, PROMPT_NPZ)
    if errs:
        raise BundleError("; ".join(errs))
    return rows, l2_normalize(vec), versions


def resolve_gt_mode(bundle_dir: str, manifest: dict) -> str:
    """→ 'csv' | 'folders' | 'none' (auto 해석 포함)."""
    mode = manifest.get("gt_mode", "auto")
    has_csv = os.path.isfile(os.path.join(bundle_dir, GT_CSV))
    if mode == "auto":
        return "csv" if has_csv else "none"
    if mode == "csv" and not has_csv:
        raise BundleError(f"gt_mode=csv 인데 {GT_CSV} 없음")
    return mode


def load_gt(bundle_dir: str, image_keys: list[str], mode: str) -> tuple[dict, bool]:
    """→ (key → {'class': str, 'camera': str|None}, has_camera).

    mode='csv': gt.csv 파싱 (key 는 image_keys 부분집합이어야 함 — 초과 key 는 오류).
    mode='folders': images/<class>/... 첫 폴더명이 class (하위폴더 더 깊으면 첫 세그먼트).
    mode='none': ({}, False).
    """
    if mode == "none":
        return {}, False
    if mode == "folders":
        gt = {}
        bad: list[str] = []
        for k in image_keys:
            seg = k.split("/", 1)
            if len(seg) == 2 and CLASS_RE.match(seg[0]):
                gt[k] = {"class": seg[0], "camera": None}
            else:
                # fail-closed: 폴더명 오탈자(클래스 규칙 위반)나 루트 직치 이미지를 조용히
                # "GT 없음"으로 강등하면 체계적 오탈자가 결손으로 위장된다 (codex 리뷰 지적)
                bad.append(k)
        if bad:
            raise BundleError(
                f"gt_mode=folders: images/<class>/ 규칙 위반 key {len(bad)}개 (예: {bad[:3]}) — "
                f"클래스 폴더명은 {CLASS_RE.pattern}")
        if not gt:
            raise BundleError("gt_mode=folders 인데 images/<class>/ 구조에서 클래스를 못 얻음")
        return gt, False
    p = os.path.join(bundle_dir, GT_CSV)
    keyset = set(image_keys)
    gt = {}
    has_camera = False
    with open(p, encoding="utf-8-sig", newline="") as f:
        rd = csv.DictReader(f)
        if rd.fieldnames is None or not {"key", "class"}.issubset(set(rd.fieldnames)):
            raise BundleError(f"{GT_CSV}: 헤더에 key,class 필요 (현재 {rd.fieldnames})")
        if len(rd.fieldnames) != len(set(rd.fieldnames)):
            raise BundleError(f"{GT_CSV}: 중복 헤더 존재 {rd.fieldnames}")
        has_camera = "camera" in rd.fieldnames
        for i, r in enumerate(rd, start=2):
            if r.get(None):
                raise BundleError(f"{GT_CSV}:{i}: 헤더({len(rd.fieldnames)}열)보다 열이 많음 (미인용 콤마?)")
            k, c = (r.get("key") or "").strip(), (r.get("class") or "").strip()
            cam = (r.get("camera") or "").strip() if has_camera else ""
            if k not in keyset:
                raise BundleError(f"{GT_CSV}:{i}: key {k!r} 가 {IMG_NPZ} key 집합 밖")
            if not CLASS_RE.match(c):
                raise BundleError(f"{GT_CSV}:{i}: class 규칙 위반 {c!r}")
            if k in gt:
                raise BundleError(f"{GT_CSV}:{i}: key 중복 {k!r}")
            gt[k] = {"class": c, "camera": cam or None}
    if not gt:
        raise BundleError(f"{GT_CSV}: 행 0개")
    if has_camera and not any(v["camera"] for v in gt.values()):
        # camera 헤더는 있으나 전 행 공란 — FiftyOne 이 all-None 값으로는 신규 필드 타입을
        # 추론 못 해 set_values 에서 ValueError 로 죽는다(score_bundle.py). 헤더만 있고 값이
        # 없으면 "camera 없음"과 동일하게 취급해 그 크래시를 fail-closed 검증 이전에 방지한다.
        has_camera = False
    return gt, has_camera


def purity_bin(p: float) -> str:
    for edge, label in PURITY_EDGES:
        if p < edge:
            return label
    return "90-100%"


def bundle_fingerprint(keys: list[str], img_vec: np.ndarray, rows: list[dict], sent_vec: np.ndarray) -> str:
    """이미지 key/벡터 + 문장 행/벡터의 결정론적 지문 (gt.csv 는 제외 — GT 나중 추가 허용).

    ingest 가 marker 에 박고 재채점(score_bundle)이 대조한다 — 같은 행수의 몰래 교체/재정렬
    (기존 불변식들이 전부 통과하는 조용한 오귀속 경로)을 차단한다. 벡터는 로더가 재정규화한
    float32 를 그대로 해시하므로 동일 입력 → 동일 지문.
    """
    h = hashlib.sha256()
    h.update("\x00".join(keys).encode("utf-8"))
    h.update(np.ascontiguousarray(img_vec, dtype=np.float32).tobytes())
    h.update("\x00".join(f"{r['version']}\x01{r['class']}\x01{r['text']}" for r in rows).encode("utf-8"))
    h.update(np.ascontiguousarray(sent_vec, dtype=np.float32).tobytes())
    return h.hexdigest()[:16]


def dataset_marker(bundle_dir: str, gt_mode: str, fingerprint: str | None = None) -> dict:
    m = {"format_version": FORMAT_VERSION, "bundle": os.path.abspath(bundle_dir), "gt_mode": gt_mode}
    if fingerprint:
        m["fingerprint"] = fingerprint
    return m


def write_report(bundle_dir: str, name: str, payload: dict) -> str:
    d = os.path.join(bundle_dir, ARTIFACTS_DIR)
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, name)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    return p


# ── 청크 코사인 코어 (정본 bank_top2_stream 의 일반화) ──────────────────────

def chunked_group_max(Q: np.ndarray, R: np.ndarray, groups: np.ndarray, n_groups: int,
                      q_batch: int = FRAME_BATCH, r_block: int = SENT_BLOCK,
                      top2: bool = False):
    """쿼리별·그룹별 최고 내적과 그 전역 ref 인덱스 (유사도 행렬 비상주).

    Q [Nq,D]·R [Nr,D] 는 L2 정규화 가정(내적=코사인). groups [Nr] = 각 ref 의 그룹 id
    (0..n_groups-1). 반환 (best [Nq,G] float32, argbest [Nq,G] int64 — R 의 전역 인덱스,
    빈 그룹은 -inf/-1). top2=True 면 (best, argbest, second [Nq,G]) — second 는 그룹 내
    2위 내적 (그룹 원소 1개면 -inf).
    """
    Nq = len(Q)
    best = np.full((Nq, n_groups), -np.inf, dtype=np.float32)
    second = np.full((Nq, n_groups), -np.inf, dtype=np.float32) if top2 else None
    argbest = np.full((Nq, n_groups), -1, dtype=np.int64)
    groups = np.asarray(groups, dtype=np.int64)
    for g in range(n_groups):
        ridx = np.nonzero(groups == g)[0]
        if len(ridx) == 0:
            continue
        for r0 in range(0, len(ridx), r_block):
            rb = ridx[r0:r0 + r_block]
            Vb = R[rb]
            for q0 in range(0, Nq, q_batch):
                S = Q[q0:q0 + q_batch] @ Vb.T  # [b, len(rb)]
                m1 = S.max(axis=1)
                a1 = S.argmax(axis=1)
                sl = slice(q0, q0 + len(m1))
                if top2:
                    # 블록 2위: 1위 마스킹 후 최댓값 (블록 1열이면 -inf)
                    if S.shape[1] > 1:
                        S2 = S.copy()
                        S2[np.arange(len(m1)), a1] = -np.inf
                        b2 = S2.max(axis=1)
                    else:
                        b2 = np.full(len(m1), -np.inf, dtype=np.float32)
                    old1 = np.array(best[sl, g], copy=True)
                    old2 = np.array(second[sl, g], copy=True)
                    # 토너먼트 병합: 기존 상위2 원소값(old1≥old2)과 블록 상위2(m1≥b2)의
                    # 합집합 상위2 = 네 값 중 큰 순서 1·2위
                    cand = np.stack([old1, old2, m1, b2], axis=1)
                    cand.sort(axis=1)
                    second[sl, g] = cand[:, -2]
                    upd = m1 > old1
                    best[sl, g] = cand[:, -1]
                    ab = argbest[sl, g]
                    ab[upd] = rb[a1[upd]]
                    argbest[sl, g] = ab
                else:
                    upd = m1 > best[sl, g]
                    bcol = best[sl, g]
                    bcol[upd] = m1[upd]
                    best[sl, g] = bcol
                    ab = argbest[sl, g]
                    ab[upd] = rb[a1[upd]]
                    argbest[sl, g] = ab
    if top2:
        return best, argbest, second
    return best, argbest


# ── 셀프테스트 ──────────────────────────────────────────────────────────────

def _selftest() -> None:
    assert vtag("v1.0.8.4") == "v1084" and vtag("baseline") == "vbaseline"
    assert vt("v1.0.8.4") == "v1_0_8_4" and vt("v2-final") == "v2_final"  # 도메인 밖 방어 동작
    assert class_suffix("fire") == "fire" and class_suffix("no-fall") == "no_fall"
    # 유효 도메인: VERSION_RE 안에서는 vtag/vt 가 정본(prompt_geometry)과 문자 단위로 동일해야 함
    for v_ in ("v1.0", "v2.0.beta", "baseline2"):
        assert VERSION_RE.match(v_)
        assert vtag(v_) == "v" + "".join(v_.lstrip("vV").split("."))  # 정본 식 그대로
        assert vt(v_) == v_.replace(".", "_")
    assert not VERSION_RE.match("v2.0-beta") and not VERSION_RE.match("v1_0")  # 하이픈/언더스코어 금지
    assert CLASS_RE.match("no_fall") and not CLASS_RE.match("no-fall") and not CLASS_RE.match("a.b")
    # 번들 지문: 결정론 + 민감성 (문장 텍스트 1자 변경/벡터 1개 변경 → 다른 지문)
    _k = ["a.jpg", "b.jpg"]
    _iv = l2_normalize(np.arange(8, dtype=np.float32).reshape(2, 4) + 1)
    _rows = [{"version": "v1.0", "class": "fire", "text": "불"}]
    _sv = l2_normalize(np.ones((1, 4), np.float32))
    fp1 = bundle_fingerprint(_k, _iv, _rows, _sv)
    assert fp1 == bundle_fingerprint(list(_k), _iv.copy(), [dict(_rows[0])], _sv.copy())
    assert fp1 != bundle_fingerprint(_k, _iv, [{**_rows[0], "text": "물"}], _sv)
    assert fp1 != bundle_fingerprint(_k[::-1], _iv, _rows, _sv)
    a = l2_normalize(np.array([[3.0, 4.0]], dtype=np.float64))
    assert a.dtype == np.float32 and abs(float(np.linalg.norm(a[0])) - 1.0) < 1e-6
    assert check_vectors(np.ones((2, 4), np.float32), 4, "x") == []
    assert check_vectors(np.ones((2, 3), np.float32), 4, "x")
    assert not _safe_rel_key("../evil.jpg") and not _safe_rel_key("/abs.jpg") and _safe_rel_key("a/b.jpg")
    assert purity_bin(0.1) == "0-25%" and purity_bin(0.95) == "90-100%"
    # manifest 최상위가 dict 아니면(배열 등) AttributeError 대신 BundleError (리뷰 지적 재현 방지)
    import tempfile as _tempfile
    with _tempfile.TemporaryDirectory() as _root:
        with open(os.path.join(_root, MANIFEST), "w", encoding="utf-8") as _f:
            json.dump([1, 2, 3], _f)
        try:
            load_manifest(_root)
            raise AssertionError("배열 manifest 인데 load_manifest 가 통과")
        except BundleError as _e:
            assert "JSON 객체가 아님" in str(_e), _e
    # prompts.csv 파서 왕복
    buf = io.StringIO("version,class,text\r\nv1,fire,a\r\nv1,smoke,b\r\nv2,fire,c\r\n")
    rd = csv.DictReader(buf)
    assert {"version", "class", "text"}.issubset(set(rd.fieldnames or []))
    # chunked_group_max ↔ 브루트포스 대조 (청크 경계 강제: q_batch=3, r_block=2)
    rng = np.random.default_rng(0)
    Q = l2_normalize(rng.normal(size=(11, 8)))
    R = l2_normalize(rng.normal(size=(13, 8)))
    grp = rng.integers(0, 4, size=13)
    grp[grp == 3] = 2  # 그룹 3 = 빈 그룹
    S = Q @ R.T
    best, arg, sec = chunked_group_max(Q, R, grp, 4, q_batch=3, r_block=2, top2=True)
    for g in range(4):
        idx = np.nonzero(grp == g)[0]
        if len(idx) == 0:
            assert np.all(np.isneginf(best[:, g])) and np.all(arg[:, g] == -1)
            continue
        Sg = S[:, idx]
        assert np.allclose(best[:, g], Sg.max(axis=1), atol=1e-6)
        assert np.all(arg[:, g] == idx[Sg.argmax(axis=1)])
        if len(idx) > 1:
            srt = np.sort(Sg, axis=1)
            assert np.allclose(sec[:, g], srt[:, -2], atol=1e-6)
        else:
            assert np.all(np.isneginf(sec[:, g]))
    b2_, a2_ = chunked_group_max(Q, R, np.zeros(13, np.int64), 1, q_batch=4, r_block=3)
    assert np.allclose(b2_[:, 0], S.max(axis=1), atol=1e-6) and np.all(a2_[:, 0] == S.argmax(axis=1))
    print("bundle_common selftest OK")


if __name__ == "__main__":
    _selftest()
