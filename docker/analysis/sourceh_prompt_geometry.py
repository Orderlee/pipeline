#!/usr/bin/env python3
"""프롬프트 뱅크 기하 분석 — v1.0.8.0 → v1.0.8.4 향상이 '개수'인가 '위치'인가.

가설 (사용자 제기):
  H1(개수): 뱅크가 커져서 max-cosine 통계가 올라갔다 (order statistics).
  H2(기하): 문장들이 이미지 임베딩 매니폴드의 **특정 영역에 접근**하도록 재작성됐다
            ("It is a {장면}. {상태}. {이벤트}." 템플릿 효과).

데이터: source-h 프레임 13,144장 임베딩(sourceh_v2/work/embed.npz) + 뱅크 2벌(sourceh/prompts/*.npz).
전부 같은 인코더(PE-Core-L14-336, L2 정규화 — cosine=내적).

스테이지 (analyze 가 캐시를 만들고 나머지가 소비):
    analyze  유사도 행렬(청크 fp32) → 동일예산 재표집 / matched-min / 한계곡선 /
             per-prompt 승수·근접도 → geometry.json + cache.npz
    ablate   승자 프롬프트 절제(장면 접두 탈착) → /embed_text 라이브 → would-win rate
    gap      v084 미검출 프레임 군집 → 공백 지도 + 후보 문장 프로브 + FiftyOne 필드
    report   markdown 종합

⚠️ fp16 금지 — 승리 margin 중앙값 ~0.01, fp16 분해능이 이를 먹는다. fp32 유사도 행렬
   1.5GB 는 in-RAM (실행 전 가용 메모리 확인 — 2026-07-30 호스트 스왑 소진 사건 참조).
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import sys
import time

import numpy as np

PROFILES = {
    "sourceh": {
        "root": "/data/fiftyone/sourceh_v2",
        "dataset": "source-h",
        "prompt_dir": "/data/fiftyone/sourceh/prompts",
        "class_names": {0: "normal", 1: "falldown", 2: "fire", 3: "smoke"},
        "map_yaml": None,
    },
    "frames": {
        "root": "/data/fiftyone/frames_bank",
        "dataset": "frames_captions",
        "prompt_dir": "/data/fiftyone/sourceh/prompts",   # 뱅크 npz 는 버전 전역 자원 — 공유
        "class_names": {0: "normal", 1: "falldown", 2: "fire", 3: "smoke", 4: "smoking"},
        "map_yaml": os.environ.get("BANK_DOMAIN_MAP", "/workspace/bank_domain_map.yaml"),
    },
}
PROFILE = "sourceh"
ROOT = PROFILES["sourceh"]["root"]
WORK = f"{ROOT}/work"
GEO = f"{WORK}/geometry"
REPORT_DIR = f"{ROOT}/report"
PROMPT_DIR = PROFILES["sourceh"]["prompt_dir"]
EMBED_URL = os.environ.get("EMBEDDING_API_URL", "http://embedding-service:8003")


def set_profile(name: str) -> None:
    """모듈 전역 경로/클래스를 프로필로 전환 — 기존 900줄 수학은 전역만 보므로 무수정 재사용."""
    global PROFILE, ROOT, WORK, GEO, REPORT_DIR, PROMPT_DIR, CLASS_NAMES
    p = PROFILES[name]
    PROFILE = name
    ROOT = p["root"]
    WORK = f"{ROOT}/work"
    GEO = f"{WORK}/geometry"
    REPORT_DIR = f"{ROOT}/report"
    PROMPT_DIR = p["prompt_dir"]
    CLASS_NAMES = p["class_names"]


def assert_mem_budget(budget_gb: float) -> None:
    """공유 호스트 보호 — 2026-07 스왑 쓰래싱 사건 재발 방지. 부족하면 시작 자체를 거부."""
    avail_kb = 0
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("MemAvailable:"):
                avail_kb = int(line.split()[1])
                break
    avail_gb = avail_kb / 1024 / 1024
    if avail_gb < 2 * budget_gb:
        raise SystemExit(f"메모리 부족: available {avail_gb:.1f}G < 2×budget {budget_gb:.0f}G — 시작 거부")


# 비교 대상 뱅크 버전 — env 로 파라미터화 (새 버전이 나오면 BANK_A/BANK_B 만 바꿔 재실행)
VERSIONS = (os.environ.get("BANK_A", "v1.0.8.0"), os.environ.get("BANK_B", "v1.0.8.4"))
V0, V4 = VERSIONS
CLASS_NAMES = {0: "normal", 1: "falldown", 2: "fire", 3: "smoke"}
EVENT_CLASSES = (1, 2, 3)
SEEDS = 10


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def vtag(version: str) -> str:
    """v1.0.8.4 → v084 — `margin_*`/`winner_*` 필드 접미사.

    기존 코드가 ("v080","v084") 를 하드코딩하고 있었다. BANK_A/BANK_B 를 바꿔 재실행하면
    새 버전 값이 옛 이름 필드에 덮여 조용히 거짓말을 한다 → 버전에서 파생한다.
    """
    parts = version.lstrip("v").split(".")
    return "v" + "".join(parts[-3:] if len(parts) >= 3 else parts)


def jsonl_load(path: str, key: str = "key") -> dict:
    out = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            out[r[key]] = r
    return out


def load_all():
    led = jsonl_load(f"{WORK}/ledger.jsonl")
    d = np.load(f"{WORK}/embed.npz", allow_pickle=True)
    keys = [str(k) for k in d["key"]]
    mask = [k in led for k in keys]
    keys = [k for k, m in zip(keys, mask) if m]
    X = d["vec"][np.array(mask)].astype(np.float32)
    X /= np.linalg.norm(X, axis=1, keepdims=True)
    gt = np.array([led[k]["gt_class"] for k in keys], dtype=np.int64)
    src = np.array([led[k]["src_video"] for k in keys])
    banks = {}
    for v in VERSIONS:
        z = np.load(f"{PROMPT_DIR}/{v}.npz", allow_pickle=True)
        banks[v] = {
            "vec": z["vec"].astype(np.float32),
            "cls": z["cls"].astype(np.int64),
            "prompt": [str(p) for p in z["prompt"]],
        }
    return keys, X, gt, src, banks


def class_sims(X: np.ndarray, bank: dict) -> dict[int, np.ndarray]:
    """클래스별 유사도 행렬 [N, n_c] fp32. 2048행 청크로 계산해 피크 메모리 억제."""
    out = {}
    for c in sorted(set(bank["cls"].tolist())):
        idx = np.flatnonzero(bank["cls"] == c)
        V = bank["vec"][idx]
        S = np.empty((X.shape[0], len(idx)), dtype=np.float32)
        for s in range(0, X.shape[0], 2048):
            S[s:s + 2048] = X[s:s + 2048] @ V.T
        out[c] = S
    return out


def bank_best_stream(X: np.ndarray, bank: dict, batch: int = 1024,
                     block: int = 2048) -> tuple[dict, dict]:
    """클래스별 per-frame best cosine + argmax(뱅크 전역 인덱스) — 유사도 행렬 미상주.

    class_sims 는 [N, n_c] 전체를 할당한다(sourceh 13k 에선 OK, frames 200k 에선 12GB → 스왑
    쓰래싱). 여기선 [batch, block] 타일(8MB)만 만들고 즉시 running max 로 접는다. fp32 필수.
    """
    classes = sorted(set(bank["cls"].tolist()))
    n = X.shape[0]
    best = {c: np.full(n, -2.0, dtype=np.float32) for c in classes}
    arg = {c: np.zeros(n, dtype=np.int64) for c in classes}
    for c in classes:
        gidx = np.flatnonzero(bank["cls"] == c)
        V = bank["vec"][gidx]
        for q in range(0, V.shape[0], block):
            Vb = V[q:q + block]
            for s in range(0, n, batch):
                S = X[s:s + batch] @ Vb.T
                m = S.max(axis=1)
                a = S.argmax(axis=1)
                seg_best = best[c][s:s + batch]          # view — 제자리 갱신
                seg_arg = arg[c][s:s + batch]
                upd = m > seg_best
                seg_best[upd] = m[upd]
                seg_arg[upd] = gidx[q + a[upd]]
        best[c] = np.ascontiguousarray(best[c])
    return best, arg


def bank_top2_stream(X: np.ndarray, bank: dict, drop: np.ndarray | None = None,
                     batch: int = 1024, block: int = 2048) -> tuple[dict, dict, dict]:
    """클래스별 per-frame 1·2위 cosine + 1위의 **클래스-로컬** 인덱스.

    LOO(문장 하나 제거) counterfactual 에 필요한 건 "그 문장을 지웠을 때의 클래스 점수"
    = 그 클래스 내 2위다. `bank_best_stream` 은 1위만 접어 보관해서 이걸 못 준다.
    `drop`(뱅크 전역 bool 마스크)을 주면 해당 문장을 아예 뺀 상태로 계산한다 — 탐욕 그룹
    제거가 라운드마다 재적합할 때 쓴다. 반환 인덱스는 cache.npz 의 `arg_*` 와 같은
    **원본 뱅크 기준 클래스-로컬** 번호라 drop 이후에도 문장 정체성이 유지된다.
    """
    classes = sorted(set(bank["cls"].tolist()))
    n = X.shape[0]
    b1 = {c: np.full(n, -2.0, dtype=np.float32) for c in classes}
    b2 = {c: np.full(n, -2.0, dtype=np.float32) for c in classes}
    a1 = {c: np.full(n, -1, dtype=np.int64) for c in classes}
    for c in classes:
        gidx = np.flatnonzero(bank["cls"] == c)
        local = np.arange(len(gidx)) if drop is None else np.flatnonzero(~drop[gidx])
        if len(local) == 0:
            continue                       # 클래스가 통째로 비면 점수 −2 유지 = 절대 안 이김
        V = bank["vec"][gidx[local]]
        for q in range(0, V.shape[0], block):
            Vb = V[q:q + block]
            for s in range(0, n, batch):
                S = X[s:s + batch] @ Vb.T
                m1 = S.max(axis=1)
                a = S.argmax(axis=1)
                if S.shape[1] > 1:
                    S[np.arange(S.shape[0]), a] = -np.inf
                    m2 = S.max(axis=1)
                else:
                    m2 = np.full(S.shape[0], -2.0, dtype=np.float32)
                r1, r2, ra = b1[c][s:s + batch], b2[c][s:s + batch], a1[c][s:s + batch]
                win = m1 > r1
                # 새 1위가 나오면 **옛 1위가 2위 후보로 내려간다** — r1 갱신 전에 계산해야 한다
                new2 = np.where(win, np.maximum(r1, m2), np.maximum(r2, m1))
                ra[win] = local[q + a[win]]
                r1[win] = m1[win]
                r2[:] = new2
    return b1, b2, a1


def crosswalk_class(cw: dict, category: str) -> str | None:
    """box category → frame class. 미등재 = None = 그 이미지 GT 제외 (fail-closed)."""
    return cw.get(category)


def minn_tier(n: int) -> str:
    """min-n 게이트 (스펙 §7): 0=no_gt(0% 표시 금지) / <30=counts_only / <100=exploratory."""
    if n == 0:
        return "no_gt"
    if n < 30:
        return "counts_only"
    if n < 100:
        return "exploratory"
    return "reportable"


NAME_TO_ID = {"normal": 0, "falldown": 1, "fire": 2, "smoke": 3, "smoking": 4}


def load_domain_map() -> dict:
    import yaml

    path = PROFILES[PROFILE]["map_yaml"]
    with open(path, encoding="utf-8") as f:
        m = yaml.safe_load(f) or {}
    if not m.get("domains"):            # 미기재·null 모두 빈 dict 로 (0단계)
        m["domains"] = {}
    m.setdefault("class_crosswalk", {})
    m.setdefault("unsupported_classes", [])
    m["project_to_domain"] = {p: d for d, cfg in m["domains"].items()
                              for p in (cfg.get("projects") or [])}
    for d, cfg in m["domains"].items():
        for k in ("bank_a", "bank_b"):
            if not cfg.get(k):
                raise SystemExit(f"bank_domain_map.yaml: domains.{d}.{k} 누락 (fail-closed)")
    return m


def predict(best: dict[int, np.ndarray]) -> np.ndarray:
    """클래스별 per-frame best cosine → argmax 예측."""
    classes = sorted(best)
    M = np.stack([best[c] for c in classes], axis=1)
    return np.array(classes)[M.argmax(axis=1)]


def recalls(pred: np.ndarray, gt: np.ndarray) -> dict:
    out = {"micro": float((pred == gt).mean())}
    per = {}
    for c in sorted(set(gt.tolist())):
        m = gt == c
        per[CLASS_NAMES[c]] = float((pred[m] == c).mean())
    out["per_class"] = per
    out["macro"] = float(np.mean(list(per.values())))
    return out


# ────────────────────── bank ──────────────────────
def stage_bank(csv_path: str, version: str) -> None:
    """새 뱅크 CSV(ID,class,prompt) → /embed_text → PROMPT_DIR/<version>.npz.

    userwatch JSON 의 feature 와 /embed_text 가 cosine=1.000000 동일 인코더임이 검증돼 있어
    CSV 텍스트만으로 제품 벡터를 재현한다 (7.5ms/건 → 1.6만 문장 ≈ 2분).
    """
    import csv as _csv
    import requests

    rows = list(_csv.DictReader(open(csv_path, newline="", encoding="utf-8")))
    out = f"{PROMPT_DIR}/{version}.npz"
    if os.path.exists(out):
        z = np.load(out, allow_pickle=True)
        if len(z["cls"]) == len(rows):
            log(f"bank {version}: 이미 존재 (n={len(rows)}) → skip")
            return
    sess = requests.Session()
    vecs = np.zeros((len(rows), 1024), dtype=np.float32)
    cls = np.zeros(len(rows), dtype=np.int64)
    t0 = time.time()
    for i, r in enumerate(rows):
        vecs[i] = _embed_text(sess, r["prompt"])
        cls[i] = int(r["class"])
        if (i + 1) % 2000 == 0:
            log(f"bank {version}: {i + 1}/{len(rows)} ({time.time() - t0:.0f}s)")
    np.savez_compressed(out, vec=vecs, cls=cls,
                        prompt=np.array([r["prompt"] for r in rows], dtype=object))
    log(f"bank {version}: 저장 {out} (n={len(rows)})")


# ────────────────────── analyze ──────────────────────
def stage_analyze() -> None:
    os.makedirs(GEO, exist_ok=True)
    keys, X, gt, src, banks = load_all()
    log(f"프레임 {len(keys)} / 뱅크 {[len(b['cls']) for b in banks.values()]}")
    sims = {v: class_sims(X, banks[v]) for v in VERSIONS}
    log("유사도 행렬 완료 (fp32)")

    results: dict = {"n_frames": len(keys)}

    # 0) 풀 뱅크 기준 (sanity + 캐시)
    full_best = {v: {c: sims[v][c].max(axis=1) for c in sims[v]} for v in VERSIONS}
    full_arg = {v: {c: sims[v][c].argmax(axis=1) for c in sims[v]} for v in VERSIONS}
    results["full"] = {v: recalls(predict(full_best[v]), gt) for v in VERSIONS}
    log(f"full: {V0} micro={results['full'][V0]['micro']:.4f} / "
        f"{V4} micro={results['full'][V4]['micro']:.4f}")

    # 1) 동일 예산 재표집: v084 를 총 12,480(=v080 전체)으로 층화 축소
    target_total = len(banks[V0]["cls"])
    cls4 = banks[V4]["cls"]
    props = {c: (cls4 == c).sum() / len(cls4) for c in sims[V4]}
    eq_runs = []
    for seed in range(SEEDS):
        rng = np.random.default_rng(seed)
        best = {}
        for c in sims[V4]:
            n_c = max(1, round(props[c] * target_total))
            take = rng.choice(sims[V4][c].shape[1], size=min(n_c, sims[V4][c].shape[1]),
                              replace=False)
            best[c] = sims[V4][c][:, take].max(axis=1)
        eq_runs.append(recalls(predict(best), gt))
    results["equal_budget_v084_at_12480"] = {
        "micro_mean": float(np.mean([r["micro"] for r in eq_runs])),
        "micro_std": float(np.std([r["micro"] for r in eq_runs])),
        "per_class_mean": {k: float(np.mean([r["per_class"][k] for r in eq_runs]))
                           for k in eq_runs[0]["per_class"]},
    }
    log(f"동일예산 v084@{target_total}: micro {results['equal_budget_v084_at_12480']['micro_mean']:.4f}"
        f"±{results['equal_budget_v084_at_12480']['micro_std']:.4f}")

    # 2) matched-min: 두 뱅크 다 클래스별 min 크기로
    min_sizes = {c: min(sims[V0][c].shape[1], sims[V4][c].shape[1]) for c in sims[V0]}
    results["matched_min_sizes"] = {CLASS_NAMES[c]: int(n) for c, n in min_sizes.items()}
    mm = {}
    for v in VERSIONS:
        runs = []
        for seed in range(SEEDS):
            rng = np.random.default_rng(1000 + seed)
            best = {}
            for c in sims[v]:
                take = rng.choice(sims[v][c].shape[1], size=min_sizes[c], replace=False)
                best[c] = sims[v][c][:, take].max(axis=1)
            runs.append(recalls(predict(best), gt))
        mm[v] = {
            "micro_mean": float(np.mean([r["micro"] for r in runs])),
            "micro_std": float(np.std([r["micro"] for r in runs])),
            "per_class_mean": {k: float(np.mean([r["per_class"][k] for r in runs]))
                               for k in runs[0]["per_class"]},
        }
    results["matched_min"] = mm
    log(f"matched-min: {V0} micro {mm[V0]['micro_mean']:.4f} / {V4} micro {mm[V4]['micro_mean']:.4f}")

    # 3) 클래스별 한계곡선: 이벤트 클래스 c 만 grid, 나머지 풀 고정
    curves = {}
    for c in EVENT_CLASSES:
        curves[CLASS_NAMES[c]] = {}
        for v in VERSIONS:
            n_c = sims[v][c].shape[1]
            grid = sorted({g for g in (25, 50, 100, 200, 400, 800, 1600, 3000) if g < n_c} | {n_c})
            pts = []
            for size in grid:
                rec_c = []
                for seed in range(SEEDS):
                    rng = np.random.default_rng(2000 + seed)
                    best = {cc: full_best[v][cc] for cc in sims[v]}
                    take = rng.choice(n_c, size=size, replace=False)
                    best[c] = sims[v][c][:, take].max(axis=1)
                    pred = predict(best)
                    m = gt == c
                    rec_c.append(float((pred[m] == c).mean()))
                pts.append({"size": int(size), "recall_mean": float(np.mean(rec_c)),
                            "recall_std": float(np.std(rec_c))})
            curves[CLASS_NAMES[c]][v] = pts
        log(f"한계곡선 {CLASS_NAMES[c]} 완료")
    results["marginal_curves"] = curves

    # 4) per-prompt 통계: 승수(그 프롬프트가 per-frame class-best 인 횟수) / 매니폴드 근접도
    cent = {c: (lambda m: m / np.linalg.norm(m))(X[gt == c].mean(axis=0)) for c in sims[V0]}
    prompt_stats = {}
    for v in VERSIONS:
        rows = []
        for c in sims[v]:
            idx = np.flatnonzero(banks[v]["cls"] == c)
            wins = np.bincount(full_arg[v][c], minlength=len(idx))
            prox = sims[v][c].max(axis=0)  # 각 프롬프트의 최고 프레임 cosine
            cc = banks[v]["vec"][idx] @ cent[c]
            for j in range(len(idx)):
                rows.append((c, int(wins[j]), float(prox[j]), float(cc[j])))
        w = np.array([r[1] for r in rows], dtype=float)
        p = np.array([r[2] for r in rows])
        ccs = np.array([r[3] for r in rows])
        winner = w > 0
        prompt_stats[v] = {
            "n_prompts": len(rows),
            "n_winners": int(winner.sum()),
            "utilization": float(winner.mean()),
            "spearman_wins_vs_proximity": float(_spearman(w, p)),
            "winner_proximity_mean": float(p[winner].mean()),
            "loser_proximity_mean": float(p[~winner].mean()),
            "winner_centroid_cos_mean": float(ccs[winner].mean()),
            "loser_centroid_cos_mean": float(ccs[~winner].mean()),
        }
    results["prompt_stats"] = prompt_stats

    # 캐시: ablate/gap 이 소비할 per-frame 축약값
    np.savez_compressed(
        f"{GEO}/cache.npz",
        keys=np.array(keys, dtype=object), gt=gt, src=np.array(src, dtype=object),
        **{f"best_{v.replace('.', '_')}_{c}": full_best[v][c] for v in VERSIONS for c in full_best[v]},
        **{f"arg_{v.replace('.', '_')}_{c}": full_arg[v][c] for v in VERSIONS for c in full_arg[v]},
    )
    with open(f"{GEO}/geometry.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=1)
    log(f"analyze 완료 → {GEO}/geometry.json")


def _spearman(a: np.ndarray, b: np.ndarray) -> float:
    ra = np.argsort(np.argsort(a)).astype(float)
    rb = np.argsort(np.argsort(b)).astype(float)
    ra -= ra.mean(); rb -= rb.mean()
    d = np.sqrt((ra ** 2).sum() * (rb ** 2).sum())
    return float((ra * rb).sum() / d) if d > 0 else 0.0


# ────────────────────── ablate ──────────────────────
def _embed_text(sess, text: str) -> np.ndarray:
    r = sess.post(f"{EMBED_URL}/embed_text", data={"text": text}, timeout=180)
    r.raise_for_status()
    v = np.asarray(r.json()["vector"], dtype=np.float32)
    return v / np.linalg.norm(v)


def _variants_v084(text: str) -> dict[str, str]:
    """v084 템플릿 'It is a {장면}. {상태}. {이벤트}.' 절제 변형."""
    sents = [s.strip() for s in text.strip().rstrip(".").split(". ")]
    sents = [s if s.endswith(".") else s + "." for s in sents]
    out = {"full": text.strip()}
    if len(sents) >= 2:
        out["event_only"] = sents[-1]                      # 이벤트 문장만
        out["scene_only"] = " ".join(sents[:-1])           # 장면·상태만 (이벤트 제거)
        out["no_scene"] = " ".join(sents[1:])              # 장면 접두만 제거
    return out


def stage_ablate(top_k: int = 5) -> None:
    import requests

    keys, X, gt, src, banks = load_all()
    cache = np.load(f"{GEO}/cache.npz", allow_pickle=True)
    tag4 = V4.replace(".", "_")
    # 경쟁선: v084 풀 뱅크에서 "자기 클래스를 제외한" per-frame 최고 cosine
    best4 = {c: cache[f"best_{tag4}_{c}"] for c in CLASS_NAMES}
    sess = requests.Session()
    sims_full = {v: class_sims(X, banks[v]) for v in VERSIONS}  # 승수 재산출용

    report = {}
    for c in EVENT_CLASSES:
        cname = CLASS_NAMES[c]
        others = np.max(np.stack([best4[o] for o in CLASS_NAMES if o != c]), axis=0)
        frames_c = gt == c
        entry = {"n_frames": int(frames_c.sum()), "prompts": []}
        for v in VERSIONS:
            idx = np.flatnonzero(banks[v]["cls"] == c)
            wins = np.bincount(sims_full[v][c].argmax(axis=1), minlength=len(idx))
            top = np.argsort(-wins)[:top_k]
            for j in top:
                if wins[j] == 0:
                    continue
                text = banks[v]["prompt"][idx[j]]
                variants = (_variants_v084(text) if v == V4
                            else {"full": text.strip(),
                                  "scene_prefixed": "It is a warehouse. " + text.strip()})
                var_out = {}
                for vn, vt in variants.items():
                    e = _embed_text(sess, vt)
                    cos = X[frames_c] @ e
                    would_win = float((cos > others[frames_c]).mean())
                    var_out[vn] = {"mean_cos": float(cos.mean()),
                                   "would_win_rate": would_win, "text": vt}
                entry["prompts"].append({"bank": v, "wins": int(wins[j]), "variants": var_out})
        report[cname] = entry
        log(f"ablate {cname}: {len(entry['prompts'])}개 프롬프트 절제 완료")
    with open(f"{GEO}/ablation.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=1)
    log(f"ablate 완료 → {GEO}/ablation.json")


# ────────────────────── gap ──────────────────────
# 공백 프로브 후보 — 군집의 '현재 승자(normal) 서술' 과 이벤트를 융합해 특정 영역을 겨냥한
# 수작업 문장. 방법론 시연용이며, 실제 뱅크 갱신 문장은 이 절차로 검증 후 채택하면 된다.
PROBE_CANDIDATES = {
    "smoke": [
        "It is a warehouse. The camera lens is slightly dirty. Thin white smoke is drifting upward.",
        "It is an industrial storage yard at night. Faint gray smoke is spreading under bright floodlights.",
        "A security camera view of stacked containers. A thin haze of smoke is rising in the distance.",
        "It is a warehouse. Vehicle headlights are shining. White smoke is billowing near the vehicles.",
    ],
    "fire": [
        "It is an industrial yard at night. A small orange flame flickers between stacked drums.",
        "A CCTV view of a storage area. A bright fire is burning with visible flames.",
        "It is a warehouse. The area is mostly empty. A fire glows behind the fence at night.",
    ],
    "falldown": [
        "It is a warehouse. A worker is lying flat on the ground near the containers.",
        "A CCTV view of an industrial site. Only the lower body of a person lying on the floor is visible.",
        "It is a storage yard. Someone has collapsed on the concrete and is not moving.",
    ],
}


def stage_gap() -> None:
    import requests
    from sklearn.cluster import KMeans

    keys, X, gt, src, banks = load_all()
    cache = np.load(f"{GEO}/cache.npz", allow_pickle=True)
    tag4 = V4.replace(".", "_")
    best4 = {c: cache[f"best_{tag4}_{c}"] for c in CLASS_NAMES}
    arg4 = {c: cache[f"arg_{tag4}_{c}"] for c in CLASS_NAMES}
    pred4 = predict(best4)
    sess = requests.Session()

    gap_out = {}
    fo_fields: dict[str, dict] = {"cluster": {}, "deficit": {}}
    for c in EVENT_CLASSES:
        cname = CLASS_NAMES[c]
        miss = np.flatnonzero((gt == c) & (pred4 != c))
        if len(miss) < 20:
            gap_out[cname] = {"n_missed": int(len(miss)), "note": "군집화 생략(표본 부족)"}
            continue
        k = max(2, min(4, len(miss) // 60))
        km = KMeans(n_clusters=k, n_init=5, random_state=51).fit(X[miss])
        others = np.max(np.stack([best4[o] for o in CLASS_NAMES if o != c]), axis=0)
        clusters = []
        for ci in range(k):
            members = miss[km.labels_ == ci]
            # 이 군집을 실제로 잡아먹는 승자 프롬프트 (예측 클래스의 best)
            winner_texts = collections.Counter()
            for i in members:
                pc = int(pred4[i])
                pidx = np.flatnonzero(banks[V4]["cls"] == pc)[arg4[pc][i]]
                winner_texts[banks[V4]["prompt"][pidx]] += 1
            deficit = float((others[members] - best4[c][members]).mean())
            # 프로브: 후보 문장을 라이브 임베딩해 이 군집에서 would-win 측정
            probes = []
            for cand in PROBE_CANDIDATES.get(cname, []):
                e = _embed_text(sess, cand)
                cos = X[members] @ e
                probes.append({"text": cand,
                               "would_win_rate": float((cos > others[members]).mean()),
                               "mean_cos": float(cos.mean())})
            probes.sort(key=lambda p: -p["would_win_rate"])
            clusters.append({
                "cluster": f"{cname}_miss_{ci}", "n": int(len(members)),
                "mean_deficit": deficit,
                "top_winner_prompts": [{"n": n, "text": t[:110]}
                                       for t, n in winner_texts.most_common(3)],
                "probes": probes,
            })
            for i in members:
                fo_fields["cluster"][keys[i]] = f"{cname}_miss_{ci}"
                fo_fields["deficit"][keys[i]] = float(others[i] - best4[c][i])
        gap_out[cname] = {"n_missed": int(len(miss)), "clusters": clusters}
        log(f"gap {cname}: 미검출 {len(miss)} → {k}군집")

    with open(f"{GEO}/gap.json", "w", encoding="utf-8") as f:
        json.dump(gap_out, f, ensure_ascii=False, indent=1)

    # FiftyOne 반영 (재빌드 없이 set_values)
    try:
        import fiftyone as fo
        from fiftyone import ViewField as F

        ds = fo.load_dataset("source-h")
        key_to_id = {}
        for s in ds.select_fields(["id", "filepath"]):
            # ⚠️ folder "필드" 를 조인 키로 쓰면 slim 이후(필드 삭제) 재실행이 깨진다 —
            #    filepath 경로(/frames/<folder>/<name>)에서 파생한다 (codex 리뷰 반영)
            key_to_id[f"{os.path.basename(os.path.dirname(s.filepath))}/"
                      f"{os.path.basename(s.filepath)}"] = s.id
        # `v084_missed` 는 쓰지 않는다: `gap_cluster is not None` 과 정확히 동치인 중복이고,
        # 이름에 v084 가 박혀 있어 BANK_B 를 바꾸면 조용히 거짓말을 한다 (codex 지적).
        ds.set_values("gap_cluster", {key_to_id[k]: fo.Classification(label=v)
                                      for k, v in fo_fields["cluster"].items()
                                      if k in key_to_id}, key_field="id")
        ds.set_values("gap_deficit", {key_to_id[k]: v for k, v in fo_fields["deficit"].items()
                                      if k in key_to_id}, key_field="id")
        for c in EVENT_CLASSES:
            cname = CLASS_NAMES[c]
            nm = f"0{4 + c}_gap_{cname}"
            view = ds.match(F("gap_cluster.label") != None).match(  # noqa: E711
                F("gap_cluster.label").starts_with(f"{cname}_miss")).sort_by("gap_deficit", True)
            if nm in ds.list_saved_views():
                ds.delete_saved_view(nm)
            ds.save_view(nm, view)
        log("gap: FiftyOne 필드(gap_cluster/gap_deficit) + 뷰 저장")
    except Exception as exc:  # noqa: BLE001 — FiftyOne 반영 실패가 분석을 막지 않게
        log(f"gap: FiftyOne 반영 실패 {exc!r}")
    log(f"gap 완료 → {GEO}/gap.json")


# ────────────────────── prune ──────────────────────
# `guide` 는 문장 **추가**의 counterfactual(FN 구조율/유발 FP)을 잰다. **삭제**의
# counterfactual 이 없었는데, 이번 뱅크 교체 이득의 98.6% 가 "경쟁 문장 소거"였다 —
# 즉 실제 레버는 삭제 쪽이다. 여기서 그 레버를 값으로 만든다.
#
# ⚠️ 개별 LOO 합 ≠ 통째 제거의 실측 이득. 두 방향 모두 가능하다 —
#    과대평가(근사 중복 문장이 서로 백업) 또는 **과소평가**(나쁜 문장 뒤에 또 나쁜 문장이
#    있어 같이 지워야 드러남). source-h 실측은 후자였다(v080 R1: 개별합 +292 vs 실측 +364).
#    그래서 라운드마다 통째로 지워보고 실측 이득을 곡선으로 남긴다.
#
# ⚠️⚠️ 탐욕 제거는 **평가셋에 그대로 적합**된다 → 그 이득을 그대로 믿으면 과적합이다.
#    영상 단위(src_video)로 2폴드를 갈라 A 에서 고른 삭제셋을 B 에서 재본다. 프레임이 아니라
#    영상으로 가르는 이유: 같은 영상의 프레임은 강하게 상관돼 프레임 분할은 누수다.
PRUNE_ROUNDS = int(os.environ.get("PRUNE_ROUNDS", "12"))
PURITY_EDGES = ((0.25, "0-25%"), (0.50, "25-50%"), (0.75, "50-75%"), (0.90, "75-90%"))


def purity_bin(p: float) -> str:
    for hi, lab in PURITY_EDGES:
        if p < hi:
            return lab
    return "90-100%"


def loo_bin(g: int) -> str:
    """제거이득 = 이 문장을 지웠을 때 늘어나는 정답 프레임 수 (양수면 그 문장이 유해)."""
    return ("유해 +10↑" if g >= 10 else "유해 +1~9" if g >= 1
            else "중립 0" if g == 0 else "유익 (지우면 손해)")


class _Pruner:
    """한 뱅크에 대한 채점·LOO·탐욕 제거. 프레임 부분집합으로도 그대로 돌아간다(홀드아웃용)."""

    def __init__(self, X, gt, bank):
        self.X, self.gt, self.bank = X, gt, bank
        self.classes = sorted(set(bank["cls"].tolist()))
        self.cls_arr = np.array(self.classes)
        self.gidx = {c: np.flatnonzero(bank["cls"] == c) for c in self.classes}

    def score(self, mask):
        b1, b2, a1 = bank_top2_stream(self.X, self.bank, drop=mask)
        M = np.stack([b1[c] for c in self.classes], axis=1)
        return b2, a1, M, self.cls_arr[M.argmax(axis=1)]

    def hits(self, mask):
        return int((self.score(mask)[3] == self.gt).sum())

    def loo_gains(self, b2, a1, M, pred):
        """문장별 제거이득. 그 문장이 자기 클래스 1위였던 프레임만 재판정하면 충분하다
        (클래스 점수는 내려가기만 하므로 다른 프레임의 argmax 는 바뀔 수 없다)."""
        out = {}
        for ci, c in enumerate(self.classes):
            for p in np.unique(a1[c]):
                if p < 0:
                    continue
                fr = np.flatnonzero(a1[c] == p)
                sub = M[fr].copy()
                sub[:, ci] = b2[c][fr]
                new = self.cls_arr[sub.argmax(axis=1)]
                out[(c, int(p))] = int((new == self.gt[fr]).sum() - (pred[fr] == self.gt[fr]).sum())
        return out

    def greedy(self, tag=""):
        """LOO-양수 집합을 라운드마다 통째로 제거 → (drop 마스크, 곡선, base, final)."""
        drop = np.zeros(len(self.bank["cls"]), dtype=bool)
        b2, a1, M, pred = self.score(drop)
        base = hits = int((pred == self.gt).sum())
        curve, converged = [], False
        for rnd in range(PRUNE_ROUNDS):
            gains = self.loo_gains(b2, a1, M, pred)
            cand = [k for k, g in gains.items() if g > 0]
            if not cand:
                converged = True
                break
            trial = drop.copy()
            for c, p in cand:
                trial[self.gidx[c][p]] = True
            tb2, ta1, tM, tpred = self.score(trial)
            th = int((tpred == self.gt).sum())
            curve.append({"round": rnd + 1, "dropped_this_round": len(cand),
                          "naive_loo_sum": sum(gains[k] for k in cand),
                          "actual_gain": th - hits, "cum_dropped": int(trial.sum()), "hits": th})
            if tag:
                log(f"prune {tag}: R{rnd + 1} {len(cand)}문장 제거 → 실측 {th - hits:+d} "
                    f"(개별합 {sum(gains[k] for k in cand):+d}) 누적 {int(trial.sum())}문장 "
                    f"/ 정답 {th:,}")
            if th <= hits:
                if tag:
                    log(f"prune {tag}: R{rnd + 1} 배치 제거가 이득 없음 → 되돌리고 중단")
                converged = True
                break
            drop, b2, a1, M, pred, hits = trial, tb2, ta1, tM, tpred, th
        if not converged and tag:   # 상한 절단은 반드시 드러낸다 (조용한 truncation 금지)
            log(f"prune {tag}: ⚠️ PRUNE_ROUNDS={PRUNE_ROUNDS} 상한에서 중단 — 아직 수렴 안 함. "
                "더 보려면 PRUNE_ROUNDS 를 올려라")
        return drop, curve, base, hits, converged


def _prune_bank(X: np.ndarray, gt: np.ndarray, src: np.ndarray,
                bank: dict, version: str) -> dict:
    pr = _Pruner(X, gt, bank)
    b2, a1, M, pred = pr.score(None)
    base_hits = int((pred == gt).sum())
    log(f"prune {version}: 기준 정답 {base_hits:,}/{len(gt):,} ({base_hits / len(gt):.2%})")
    gains0 = pr.loo_gains(b2, a1, M, pred)

    # 문장별 통계 (전체 뱅크 기준) — 승수는 **전역 승자**일 때만 센다
    sents = []
    for c in pr.classes:
        won = (a1[c] >= 0) & (pred == c)
        for p in (np.unique(a1[c][won]) if won.any() else []):
            fr = np.flatnonzero(won & (a1[c] == p))
            sents.append({
                "gidx": int(pr.gidx[c][p]), "cls": int(c), "cls_name": CLASS_NAMES[int(c)],
                "wins": int(len(fr)),
                # 선언클래스 순도 — 다수결 순도가 아니다. 전부 normal 프레임을 가져간 smoke
                # 문장은 다수결 1.00 / 선언 0.00 이고, 후자가 맞는 판정이다.
                "purity": float((gt[fr] == c).mean()),
                "loo_gain": int(gains0.get((c, int(p)), 0)),
                "text": bank["prompt"][int(pr.gidx[c][p])],
            })
    sents.sort(key=lambda r: (-r["loo_gain"], r["purity"]))
    n_harm = sum(1 for r in sents if r["loo_gain"] > 0)
    log(f"prune {version}: 승자 {len(sents)}개 중 순유해 {n_harm}개 "
        f"(개별 LOO 합 +{sum(max(0, r['loo_gain']) for r in sents)})")

    drop, curve, _, final_hits, converged = pr.greedy(tag=version)
    for r in sents:
        r["dropped"] = bool(drop[r["gidx"]])

    # ── 홀드아웃: 영상 2폴드. A 에서 고른 삭제셋을 B 에서 재본다 ──
    vids = sorted(set(src.tolist()))
    fold_b = {v for i, v in enumerate(vids) if i % 2}         # 결정적 분할 (seed 불필요)
    mb = np.array([s in fold_b for s in src])
    hold = {"n_videos": len(vids), "n_a": int((~mb).sum()), "n_b": int(mb.sum())}
    if mb.any() and (~mb).any():
        pa = _Pruner(X[~mb], gt[~mb], bank)
        pb = _Pruner(X[mb], gt[mb], bank)
        drop_a, _, _, _, _ = pa.greedy()
        b_before, b_after = pb.hits(None), pb.hits(drop_a)
        hold.update({
            "n_dropped_on_a": int(drop_a.sum()),
            "b_before": b_before, "b_after": b_after, "b_gain": b_after - b_before,
            "b_gain_pp": 100.0 * (b_after - b_before) / max(1, int(mb.sum())),
            "insample_gain_pp": 100.0 * (final_hits - base_hits) / len(gt),
        })
        log(f"prune {version}: 홀드아웃(영상 {len(vids)}개 → A {hold['n_a']:,}/B {hold['n_b']:,}프레임) "
            f"A에서 고른 {hold['n_dropped_on_a']}문장 → B 정답 {b_before:,}→{b_after:,} "
            f"({hold['b_gain_pp']:+.2f}pp) vs 인샘플 {hold['insample_gain_pp']:+.2f}pp")
    else:
        hold["note"] = "영상이 1개뿐 — 홀드아웃 불가"
        log(f"prune {version}: ⚠️ 홀드아웃 불가 (영상 {len(vids)}개) — 인샘플 이득은 과적합 상한이다")

    winner_g = np.array([pr.gidx[int(c)][a1[int(c)][i]] for i, c in enumerate(pred)])
    return {"version": version, "n_frames": int(len(gt)), "base_hits": base_hits,
            "final_hits": final_hits, "n_dropped": int(drop.sum()),
            "total_gain": final_hits - base_hits, "converged": converged,
            "n_winners": len(sents), "n_harmful": n_harm,
            "curve": curve, "holdout": hold, "sentences": sents,
            "_winner_gidx": winner_g, "_by_gidx": {r["gidx"]: r for r in sents}}


def stage_prune() -> None:
    """문장별 (승수 · 선언클래스 순도 · LOO 제거이득) + 탐욕 그룹 제거 곡선 → 삭제 랭킹,
    그리고 그 셋을 **프레임 단위 Color-by 필드**로 내린다.

    왜 문장 정체성이 아니라 품질로 칠하나: 두 뱅크는 공통 문장이 0개라 문장 이름으로는
    색 범례를 공유할 수 없다(토글 비교 불가). 품질 스케일은 공유된다. 게다가 실측상
    "나쁜 문장 = 넓고 흩어진 영토" 는 거짓이고(UMAP 분산 ↔ 제거이득 spearman +0.13/−0.10),
    실제로 유해한 문장은 국소적으로 잘못 조준돼 **조밀**하다 — 공간 분산은 신호가 아니다.
    """
    import fiftyone as fo

    keys, X, gt, src, banks = load_all()
    res = {}
    for v in VERSIONS:
        res[v] = _prune_bank(X, gt, src, banks[v], v)

    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    key_to_id = {}
    for s in ds.select_fields(["id", "filepath"]):
        key_to_id[f"{os.path.basename(os.path.dirname(s.filepath))}/"
                  f"{os.path.basename(s.filepath)}"] = s.id
    ids = [key_to_id.get(k) for k in keys]
    ok = [i for i, x in enumerate(ids) if x]
    if len(ok) < len(ids):
        log(f"prune: FiftyOne 매칭 {len(ok)}/{len(ids)} (나머지는 필드 미설정)")

    for v in VERSIONS:
        tag = vtag(v)
        r = res[v]
        wg, byg = r["_winner_gidx"], r["_by_gidx"]
        ds.set_values(f"winner_purity_{tag}",
                      {ids[i]: fo.Classification(label=purity_bin(byg[int(wg[i])]["purity"]))
                       for i in ok if int(wg[i]) in byg}, key_field="id")
        ds.set_values(f"winner_loo_{tag}",
                      {ids[i]: fo.Classification(label=loo_bin(byg[int(wg[i])]["loo_gain"]))
                       for i in ok if int(wg[i]) in byg}, key_field="id")
        log(f"prune: 필드 winner_purity_{tag}/winner_loo_{tag} 기록")

    # cos(v080 승자, v084 승자) — 같은 자리를 고쳐 쓴 건가, 딴 문장이 영토를 뺏은 건가.
    # 절대 임계는 인코더마다 다르므로 **분위 경계를 라벨에 박아** 자기설명하게 만든다
    # (한 번의 비교 런 안에서만 의미 있는 진단 축 — 런 간 비교용 아님).
    w0 = banks[V0]["vec"][res[V0]["_winner_gidx"]]
    w4 = banks[V4]["vec"][res[V4]["_winner_gidx"]]
    pair = np.einsum("ij,ij->i", w0, w4)
    edges = np.quantile(pair, [0.2, 0.4, 0.6, 0.8])

    def pair_label(x):
        i = int(np.searchsorted(edges, x))
        if i == 0:
            return f"Q1 ≤{edges[0]:.2f}"
        if i == len(edges):
            return f"Q{i + 1} >{edges[-1]:.2f}"
        return f"Q{i + 1} {edges[i - 1]:.2f}-{edges[i]:.2f}"

    ds.set_values("winner_pair_cos",
                  {ids[i]: fo.Classification(label=pair_label(float(pair[i]))) for i in ok},
                  key_field="id")
    log(f"prune: winner_pair_cos 분위 {np.round(edges, 3).tolist()} "
        f"(min {pair.min():.3f} / 중앙 {np.median(pair):.3f} / max {pair.max():.3f})")

    out = {v: {k: r for k, r in res[v].items() if not k.startswith("_")} for v in VERSIONS}
    with open(f"{GEO}/prune.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    os.makedirs(REPORT_DIR, exist_ok=True)
    import csv as _csv
    for v in VERSIONS:
        p = f"{REPORT_DIR}/prune_{v}.csv"
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = _csv.DictWriter(f, fieldnames=["gidx", "cls", "cls_name", "wins", "purity",
                                               "loo_gain", "dropped", "text"])
            w.writeheader()
            w.writerows(res[v]["sentences"])
        log(f"prune: 삭제 랭킹 CSV → {p} ({len(res[v]['sentences'])}문장)")
    log(f"prune 완료 → {GEO}/prune.json")


# ────────────────────── viz ──────────────────────
def stage_viz() -> None:
    """기하 분석을 FiftyOne 에서 눈으로 비교하게 만든다.

      · margin_viz — x=구버전 마진(자기클래스 best − 타클래스 best), y=신버전 마진.
                     뱅크 내부 차이라 스케일이 상쇄된다. margin>0 ⟺ 그 뱅크가 정답을 맞춤.
                     **사분면이 곧 결론**: 우하=구버전만 정답, 좌상=신버전만 정답.
    필드: margin_<vtag> 2개. 색은 `flip.label` 이 담당한다 (같은 4분할을 이미 인코딩).

    ⚠️ 여기서 계산하던 `gt_cos_*` / `cover_viz` / `margin_quadrant` / `margin_*_bin` 은
       전부 slim 이 곧바로 지우던 것들이라 계산·쓰기 자체를 제거했다. cover_viz(절대 코사인
       산점도)는 뱅크 간 가산 오프셋 때문에 애초에 공정 비교가 아니었고 margin_viz 가 대체한다.
    """
    import fiftyone as fo

    keys, X, gt, src, banks = load_all()
    cache = np.load(f"{GEO}/cache.npz", allow_pickle=True)
    best = {v: {c: cache[f"best_{v.replace('.', '_')}_{c}"] for c in CLASS_NAMES} for v in VERSIONS}

    n = len(keys)
    margin = {}
    for v in VERSIONS:
        own = np.array([best[v][int(g)][i] for i, g in enumerate(gt)], dtype=np.float32)
        other = np.empty(n, dtype=np.float32)
        for i, g in enumerate(gt):
            other[i] = max(best[v][o][i] for o in CLASS_NAMES if o != int(g))
        margin[v] = own - other

    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    key_to_id = {}
    for s in ds.select_fields(["id", "filepath"]):
        key_to_id[f"{os.path.basename(os.path.dirname(s.filepath))}/"
                  f"{os.path.basename(s.filepath)}"] = s.id
    ids = [key_to_id.get(k) for k in keys]
    ok = [i for i, x in enumerate(ids) if x]

    for v in VERSIONS:
        ds.set_values(f"margin_{vtag(v)}",
                      {ids[i]: float(margin[v][i]) for i in ok}, key_field="id")

    import fiftyone.brain as fob
    bkey = "margin_viz"
    if ds.has_brain_run(bkey):
        ds.delete_brain_run(bkey)
    pts = np.stack([margin[V0], margin[V4]], axis=1).astype(np.float64)
    sel = np.array([i in set(ok) for i in range(n)])
    fob.compute_visualization(ds.select([ids[i] for i in ok]) if len(ok) != n else ds,
                              points=pts[sel] if len(ok) != n else pts, brain_key=bkey)
    log(f"viz: {bkey} 등록")

    # 워크스페이스 (Samples ↔ 산점도 분할). slim 이 최종 세트로 다시 정의한다.
    for name, brain, color in (("margin", "margin_viz", "flip.label"),
                               ("gap", "emb_viz", "gap_cluster.label")):
        try:
            space = fo.Space(children=[
                fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
                fo.Space(children=[fo.Panel(type="Embeddings",
                                            state={"brainResult": brain, "colorByField": color})]),
            ], orientation="horizontal")
            if name in ds.list_workspaces():
                ds.delete_workspace(name)
            ds.save_workspace(name, space, description=f"{brain} (색: {color})")
        except Exception as exc:  # noqa: BLE001
            log(f"viz: 워크스페이스 {name} 실패 {exc!r}")
    log(f"viz: 워크스페이스 {ds.list_workspaces()}")
    # 사이드바/00_analysis 는 slim 이 소유한다 — 여기서 fiftyone_presentation 을 돌려봐야
    # 곧바로 덮어써지는 중복 작업이었고, 이미 삭제 예정인 필드를 "색칠 불가" 경고로 흘렸다.
    log("viz 완료 (사이드바 구성은 slim 담당)")


# ────────────────────── flips ──────────────────────
def stage_flips() -> None:
    """요구 #1·#2: 버전 전환으로 오탐→정탐(또는 반대)이 된 **프레임 각각**에 대해
    무엇이 왜 바뀌었는지를 FiftyOne 필드로 만든다.

    이유 분해는 centered rel 점수(프레임 내 클래스 평균 제거 — 뱅크 간 가산 오프셋 상쇄):
      · 자기 문장 접근  — GT 클래스 rel 점수가 올랐다 (새 뱅크 문장이 이 이미지에 더 접근)
      · 경쟁 문장 소거  — 구 버전에서 이기던 오답 클래스의 rel 점수가 내렸다
      · 복합/재배열    — 둘 다이거나 어느 쪽도 명확치 않음
    `why_text` 에 전·후 승자 문장과 코사인을 그대로 적는다 (사람이 읽는 근거).
    """
    import fiftyone as fo
    from fiftyone import ViewField as F

    keys, X, gt, src, banks = load_all()
    cache = np.load(f"{GEO}/cache.npz", allow_pickle=True)
    tagged = {v: v.replace(".", "_") for v in VERSIONS}
    best = {v: {c: cache[f"best_{tagged[v]}_{c}"] for c in CLASS_NAMES} for v in VERSIONS}
    arg = {v: {c: cache[f"arg_{tagged[v]}_{c}"] for c in CLASS_NAMES} for v in VERSIONS}
    classes = sorted(CLASS_NAMES)
    stacked = {v: np.stack([best[v][c] for c in classes], axis=1) for v in VERSIONS}
    rel = {v: stacked[v] - stacked[v].mean(axis=1, keepdims=True) for v in VERSIONS}
    pred = {v: np.array(classes)[stacked[v].argmax(axis=1)] for v in VERSIONS}
    cidx = {c: i for i, c in enumerate(classes)}
    pidx = {v: {c: np.flatnonzero(banks[v]["cls"] == c) for c in classes} for v in VERSIONS}

    def sentence(v, c, i):
        return banks[v]["prompt"][pidx[v][c][arg[v][c][i]]]

    EPS = 0.005
    n = len(keys)
    flip = np.empty(n, dtype=object)
    reason = np.empty(n, dtype=object)
    why = np.empty(n, dtype=object)
    counts = collections.Counter()
    for i in range(n):
        g = int(gt[i])
        ok0, ok4 = pred[VERSIONS[0]][i] == g, pred[VERSIONS[1]][i] == g
        flip[i] = ("오탐→정탐" if not ok0 and ok4 else "정탐→오탐" if ok0 and not ok4
                   else "계속 정탐" if ok0 else "계속 오탐")
        counts[flip[i]] += 1
        if flip[i] in ("계속 정탐", "계속 오탐"):
            reason[i] = flip[i]
            why[i] = ""
            continue
        va, vb = (VERSIONS[0], VERSIONS[1])
        wrong_v, right_v = (va, vb) if flip[i] == "오탐→정탐" else (vb, va)
        r_wrong = int(pred[wrong_v][i])            # 오답이던 클래스
        own_d = rel[vb][i, cidx[g]] - rel[va][i, cidx[g]]
        rival_d = rel[vb][i, cidx[r_wrong]] - rel[va][i, cidx[r_wrong]]
        if flip[i] == "정탐→오탐":                  # 방향 반전해 같은 의미로 읽는다
            own_d, rival_d = -own_d, -rival_d
        up, down = own_d > EPS, rival_d < -EPS
        # ⚠️ 방향별로 라벨이 달라야 한다 — 정탐→오탐은 부호를 뒤집어 계산하므로
        #    up 은 "자기문장이 (v084 에서) 약해짐", down 은 "경쟁문장이 새로 접근함"을 뜻한다.
        if flip[i] == "오탐→정탐":
            reason[i] = ("자기접근+경쟁소거" if up and down else
                         "자기문장 접근" if up else "경쟁문장 소거" if down else "재배열(미세)")
        else:
            reason[i] = ("자기약화+경쟁등장" if up and down else
                         "자기문장 약화" if up else "경쟁문장 등장" if down else "재배열(미세)")
        w_sent = sentence(wrong_v, r_wrong, i)
        r_sent = sentence(right_v, g, i)
        why[i] = (f"[{wrong_v}] 오답 {CLASS_NAMES[r_wrong]} «{w_sent[:80]}» "
                  f"cos {best[wrong_v][r_wrong][i]:.3f} > {CLASS_NAMES[g]} {best[wrong_v][g][i]:.3f}\n"
                  f"[{right_v}] 정답 {CLASS_NAMES[g]} «{r_sent[:80]}» "
                  f"cos {best[right_v][g][i]:.3f} ≥ 경쟁 {best[right_v][r_wrong][i]:.3f}\n"
                  f"원인: {reason[i]} (자기Δrel {own_d:+.4f} / 경쟁Δrel {rival_d:+.4f})")
    log(f"flips: {dict(counts)}")
    rc = collections.Counter(reason[flip == "오탐→정탐"])
    log(f"flips: 오탐→정탐 이유 분해 {dict(rc)}")

    ds = fo.load_dataset("source-h")
    key_to_id = {}
    for smp in ds.select_fields(["id", "filepath"]):
        key_to_id[f"{os.path.basename(os.path.dirname(smp.filepath))}/"
                  f"{os.path.basename(smp.filepath)}"] = smp.id
    ids = {k: key_to_id.get(k) for k in keys}
    ds.set_values("flip", {ids[k]: fo.Classification(label=flip[i])
                           for i, k in enumerate(keys) if ids[k]}, key_field="id")
    # ⚠️ 하단 칩은 ~1줄 폭에서 잘린다 — 긴 문자열은 (a) 줄 단위 필드로 분리하고
    #    (b) Classification 속성으로도 넣는다 (모달에서 칩 호버 → 속성 툴팁에 전문 표시).
    #    확실한 전문 열람은 모달 우상단 JSON 토글(중괄호 아이콘).
    why_a = np.empty(n, dtype=object)
    why_b = np.empty(n, dtype=object)
    for i in range(n):
        if why[i]:
            parts = str(why[i]).split("\n")
            why_a[i] = parts[0] if parts else ""
            why_b[i] = parts[1] if len(parts) > 1 else ""
        else:
            why_a[i] = why_b[i] = ""
    # 표현은 하나만: 전문은 why_before/after 문자열 필드가 담당 (속성 중복 제거 — codex)
    ds.set_values("flip_reason", {ids[k]: fo.Classification(label=str(reason[i]))
                                  for i, k in enumerate(keys) if ids[k]}, key_field="id")
    # margin_delta = GT클래스 마진(자기−타클래스)의 버전차 — 뷰 30/31 의 심각도 정렬 키
    md = {}
    for i, k in enumerate(keys):
        if not ids[k]:
            continue
        g = int(gt[i])
        m0 = best[VERSIONS[0]][g][i] - max(best[VERSIONS[0]][o][i] for o in CLASS_NAMES if o != g)
        m1 = best[VERSIONS[1]][g][i] - max(best[VERSIONS[1]][o][i] for o in CLASS_NAMES if o != g)
        md[ids[k]] = round(float(m1 - m0), 5)
    ds.set_values("margin_delta", md, key_field="id")
    ds.set_values("why_before", {ids[k]: str(why_a[i])
                                 for i, k in enumerate(keys) if ids[k] and why_a[i]}, key_field="id")
    ds.set_values("why_after", {ids[k]: str(why_b[i])
                                for i, k in enumerate(keys) if ids[k] and why_b[i]}, key_field="id")
    # why_text(전문 1필드)는 why_before/after 로 분해돼 완전 중복이라 더 이상 쓰지 않는다.
    # slim 이 지우는 필드를 여기서 매번 되살리던 순환이었다 (artifact 소유권 버그).
    try:
        ds.add_dynamic_sample_fields()   # flip_reason.before/after 를 스키마에 노출
    except Exception as exc:  # noqa: BLE001
        log(f"flips: dynamic fields 실패 {exc!r}")
    # 정렬은 margin_delta(= GT클래스 마진의 버전차) — gt_rel_delta 는 fixed 1,541 중 354건이
    # 역부호(경쟁이 더 빨리 하락)라 심각도 정렬로 부적합 (codex 리뷰).
    for nm, lab, desc in (("30_fixed_오탐to정탐", "오탐→정탐", True),
                          ("31_broken_정탐to오탐", "정탐→오탐", False)):
        if nm in ds.list_saved_views():
            ds.delete_saved_view(nm)
        ds.save_view(nm, ds.match(F("flip.label") == lab).sort_by("margin_delta", desc))
    try:
        space = fo.Space(children=[
            fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
            fo.Space(children=[fo.Panel(type="Embeddings",
                                        state={"brainResult": "emb_viz",
                                               "colorByField": "flip.label"})]),
        ], orientation="horizontal")
        if "flips" in ds.list_workspaces():
            ds.delete_workspace("flips")
        ds.save_workspace("flips", space, description="emb_viz (색: flip.label)")
    except Exception as exc:  # noqa: BLE001
        log(f"flips: 워크스페이스 실패 {exc!r}")
    # broken_reasons 도 덤프한다 — guide 의 서사(③ "지운 자석이 사실 일도 하고 있었다")가
    # 이 분해를 인용하는데 지금까지 하드코딩이었다.
    bc = collections.Counter(reason[flip == "정탐→오탐"])
    by_cls = {d: dict(collections.Counter(CLASS_NAMES[int(gt[i])]
                                          for i in np.flatnonzero(flip == d)))
              for d in ("오탐→정탐", "정탐→오탐")}
    json.dump({"counts": dict(counts), "fixed_reasons": dict(rc), "broken_reasons": dict(bc),
               "by_class": by_cls, "banks": list(VERSIONS)},
              open(f"{GEO}/flips.json", "w"), ensure_ascii=False)
    log(f"flips: 정탐→오탐 이유 분해 {dict(bc)}")
    log("flips 완료 → 필드 flip/flip_reason/why_before/after, 뷰 30/31, 워크스페이스 flips")


# ────────────────────── guide ──────────────────────
# 요구 #3: 프롬프트를 "어떻게 만들어야 하는지"를 값으로. 장면어 × 이벤트절 조합을 라이브
# 임베딩해 FN 구조율·유발 FP·선택도를 측정한 랭킹을 자동 생성한다.
SCENE_WORDS = ["warehouse", "construction site", "parking lot", "rooftop", "storage yard",
               "industrial yard", "loading dock", "factory floor", "gas station"]
STATE_SENT = "Daily routines are unfolding."   # 절제 실험에서 +17%p 기여가 실측된 상태 문장


def _read_json(path: str):
    return json.load(open(path, encoding="utf-8")) if os.path.exists(path) else None


def _magnet_narrative() -> list[str]:
    """"문장은 자석이다" 도입부 — **모든 숫자를 flips.json / prune.json 에서 읽는다.**

    이전에는 문자열 리터럴(1,541 / 16 / 1,520 / 458 / 444 / 13)이었다. 라이브 값과 이미
    어긋나 있었고, 무엇보다 BANK_A/B 를 바꿔 재실행하면 **"기준 뱅크: <신버전>" 헤더 밑에
    옛 버전 숫자가 그대로** 찍혔다. 표준 절차 스크립트에서 이건 조용한 거짓말이다.
    """
    fl = _read_json(f"{GEO}/flips.json")
    if fl is None:
        raise SystemExit("guide: flips.json 없음 — `flips` 스테이지를 먼저 실행하라 "
                         "(도입부 숫자를 여기서 읽는다)")
    va, vb = VERSIONS
    cnt, fr = fl["counts"], fl["fixed_reasons"]
    br = fl.get("broken_reasons", {})
    n_fix, n_brk = cnt.get("오탐→정탐", 0), cnt.get("정탐→오탐", 0)
    n_own = fr.get("자기문장 접근", 0)                                   # ① 좋은 자석 신설
    n_rival = fr.get("경쟁문장 소거", 0) + fr.get("자기접근+경쟁소거", 0)  # ② 나쁜 자석 제거
    n_tie = fr.get("재배열(미세)", 0)                                    # ④ 동점 뒤집힘
    n_lost = br.get("자기문장 약화", 0) + br.get("자기약화+경쟁등장", 0)   # ③ 지운 자석의 부작용
    brk_cls = fl.get("by_class", {}).get("정탐→오탐", {})
    top_brk = max(brk_cls, key=brk_cls.get) if brk_cls else None

    # ② 의 사례 문장은 일화가 아니라 prune 이 실측한 **구 뱅크 최악 문장**을 쓴다
    pr = _read_json(f"{GEO}/prune.json")
    worst = None
    if pr and va in pr and pr[va]["sentences"]:
        s0 = pr[va]["sentences"][0]
        if s0["loo_gain"] > 0:
            worst = s0

    L = ["## 작성 전에 꼭 알아야 할 것 — 문장은 자석이다\n",
         "모델은 사진을 보고 **가장 비슷한 문장 하나**를 찾아 그 문장의 클래스로 답한다. "
         "즉 문장 하나하나가 사진을 끌어당기는 **자석**이다. 좋은 자석은 자기 클래스 사진만 "
         "당기고, 나쁜 자석은 아무 사진이나 다 당긴다(= 만능 자석). "
         f"참고로 {va}→{vb} 는 문장을 추가한 게 아니라 **전부 갈아엎은 것**이다(두 버전에 "
         f"공통 문장 0개). 그 전면 교체가 승패를 어떻게 바꿨는지 전부 추적해 보니, "
         "네 가지 경우뿐이었다:\n",
         f"**① 좋은 자석이 새로 생겨서 맞췄다** (개선 {n_fix:,}장 중 {n_own:,}장)",
         "> 예전엔 자동차 헤드라이트 반사 사진에 어울리는 문장이 없어서 모델이 '불'이라고 "
         "답했다. 새 버전에 \"카메라 렌즈에 빛이 반사된다\"는 문장이 생기자 정답(normal)을 찾았다.",
         "> → **교훈: 모델이 틀리는 진짜 이유(반사, 헤드라이트, 렌즈 얼룩)를 그대로 문장으로 "
         "쓰면, 그 사진들을 정확히 데려올 수 있다.**\n",
         f"**② 나쁜 자석이 없어져서 맞췄다** (개선 {n_fix:,}장 중 {n_rival:,}장에 관여"
         f"{' — 대부분!' if n_fix and n_rival / n_fix > 0.7 else ''})"]
    if worst:
        L.append(f"> 실측 최악의 만능 자석은 {va} 의 [{worst['cls_name']}] "
                 f"«{worst['text'][:90]}» — 이 문장 **하나만 지워도 {worst['loo_gain']:,}장**이 "
                 f"저절로 정답이 된다 (이 문장이 가져간 {worst['wins']:,}장 중 선언클래스가 "
                 f"실제 정답인 비율은 {worst['purity']:.0%}).")
    else:
        L.append("> (구 뱅크 최악 문장은 `prune` 스테이지 실행 후 여기에 자동 인용된다)")
    L += ["> → **교훈: 좋은 문장을 새로 쓰는 것만큼, 아무 데나 붙는 나쁜 문장을 지우는 게 "
          "중요하다. 나쁜 자석이 되기 쉬운 문장: 특정 물건 언급(빨간 가방/통), 위치·시간 수식"
          "(오른쪽 위에/저녁에), 두루뭉술한 장면 묘사(a clear view of...).**\n",
          f"**③ 지운 자석이 사실 일도 하고 있었다** (손상 {n_brk:,}장 중 {n_lost:,}장"
          f"{f' — {top_brk} 가 대부분' if top_brk else ''})",
          "> 위의 만능 자석을 지웠더니, 그 자석이 잡아주던 **진짜 사진들**이 갈 곳을 잃고 "
          "틀리기 시작했다.",
          "> → **교훈: 나쁜 문장을 지울 때는, 그 문장이 맞추던 진짜 사진들을 대신 데려올 "
          "좋은 문장을 반드시 같이 넣어라.**\n",
          f"**④ 동점 승부가 우연히 뒤집혔다** ({n_tie:,}장)",
          "> 두 문장의 점수가 거의 같아서(0.005 이내) 순위만 살짝 바뀐 것. 운이다.",
          "> → **교훈: 이 사진들은 문장 설계의 근거로 쓰지 말 것. 오히려 정답 라벨이 맞는지 "
          "다시 볼 후보다.**\n",
          "정리: 아래 표의 후보 문장들은 위 교훈에 따라 ①처럼 데려오는 힘(FN 구조율)이 크고 "
          "②의 만능 자석이 아닌 것(유발 FP 낮음)만 골라 채택한다. "
          f"삭제 쪽 랭킹은 `{REPORT_DIR}/prune_<version>.csv` 를 보라.\n"]
    return L


def stage_guide() -> None:
    import requests

    keys, X, gt, src, banks = load_all()
    cache = np.load(f"{GEO}/cache.npz", allow_pickle=True)
    tag_b = VERSIONS[1].replace(".", "_")
    best_b = {c: cache[f"best_{tag_b}_{c}"] for c in CLASS_NAMES}
    classes = sorted(CLASS_NAMES)
    pred_b = np.array(classes)[np.stack([best_b[c] for c in classes], axis=1).argmax(axis=1)]
    sess = requests.Session()

    L = [f"# 프롬프트 작성 가이드 (자동 생성, 기준 뱅크: {VERSIONS[1]})",
         f"\n- 생성: {time.strftime('%Y-%m-%d %H:%M')} | 프레임 {len(keys):,}장",
         "- **채택 기준(권고)**: 유발 FP ≤ 0.10% 인 후보 중 FN 구조율 최대. "
         "구조율이 비슷하면 선택도(구조율÷FP) 높은 쪽.",
         "- **FN 구조율** = 지금 놓치고 있는 사진 중에서, 이 문장을 넣으면 새로 맞추게 되는 비율 (높을수록 좋음)",
         "- **유발 FP** = 이 문장이 엉뚱한 다른 종류의 사진까지 가져가 버리는 비율 (낮을수록 좋음)\n",
         *_magnet_narrative()]
    guide_json = {}
    for c in EVENT_CLASSES:
        cname = CLASS_NAMES[c]
        fn = np.flatnonzero((gt == c) & (pred_b != c))       # 현재 놓치는 프레임
        oth = np.flatnonzero(gt != c)
        others_best = np.max(np.stack([best_b[o] for o in CLASS_NAMES if o != c]), axis=0)
        own_best_oth = np.array([best_b[int(gt[i])][i] for i in oth])
        # 이벤트절: 현재 뱅크에서 FN 구조율이 가장 높은 문장의 마지막 절을 자동 추출
        idx = np.flatnonzero(banks[VERSIONS[1]]["cls"] == c)
        S_fn = X[fn] @ banks[VERSIONS[1]]["vec"][idx].T
        rescue_per = (S_fn > others_best[fn][:, None]).mean(axis=0)
        base_sent = banks[VERSIONS[1]]["prompt"][idx[int(np.argmax(rescue_per))]]
        event_clause = base_sent.strip().rstrip(".").split(". ")[-1] + "."
        rows = []
        for scene in SCENE_WORDS:
            for tpl, text in (("scene+event", f"It is a {scene}. {event_clause}"),
                              ("scene+state+event", f"It is a {scene}. {STATE_SENT} {event_clause}")):
                e = _embed_text(sess, text)
                rescue = float((X[fn] @ e > others_best[fn]).mean()) if len(fn) else 0.0
                fp = float((X[oth] @ e > own_best_oth).mean())
                rows.append({"scene": scene, "template": tpl, "text": text,
                             "fn_rescue": rescue, "induced_fp": fp,
                             "selectivity": rescue / max(fp, 1e-4)})
        rows.sort(key=lambda r: (-(r["induced_fp"] <= 0.001), -r["fn_rescue"]))
        guide_json[cname] = {"event_clause": event_clause, "n_fn": int(len(fn)),
                             "candidates": rows}
        L.append(f"## {cname} — 미검출 {len(fn):,}프레임, 이벤트절(자동): “{event_clause}”\n")
        L.append("| 장면어 | 템플릿 | FN 구조율 | 유발 FP | 선택도 | 판정 |")
        L.append("|---|---|---|---|---|---|")
        for r in rows[:10]:
            ok = "✅" if r["induced_fp"] <= 0.001 and r["fn_rescue"] > 0.05 else \
                 ("⚠️ FP" if r["induced_fp"] > 0.001 else "낮음")
            L.append(f"| {r['scene']} | {r['template']} | {r['fn_rescue']:.1%} | "
                     f"{r['induced_fp']:.2%} | {r['selectivity']:.0f}x | {ok} |")
        L.append("")
        log(f"guide {cname}: {len(rows)}후보 측정 (이벤트절: {event_clause[:40]})")
    json.dump(guide_json, open(f"{GEO}/guide.json", "w"), ensure_ascii=False)
    out = f"{REPORT_DIR}/prompt_authoring_guide.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    log(f"guide 완료 → {out}")


# ────────────────────── slim ──────────────────────
# 분석 표면 큐레이션 — 워크플로 5개 기준으로 정리한다.
#   W1 플립 검수(flip/why) · W2 사분면 판정(margin_viz) · W3 프롬프트 품질(winner_*) ·
#   W4 다음 타깃(gap) · W5 자유 탐색(text_search)
# 모든 삭제 항목은 스테이지 재실행으로 복원 가능 (cache.npz/scores.json 이 원본).
#
# ⚠️ 이 리스트는 **다른 스테이지가 쓰지 않는 필드만** 담아야 한다. 쓰고→지우고→다시 쓰는
#    순환은 artifact 소유권 버그다 (`stage_selftest` 가 소스를 검사해 강제한다).
#    여기 남은 이름들은 과거 런이 남긴 잔재를 청소하는 **tombstone** 이다.
SLIM_DROP_FIELDS = [
    # 동일 4분할 3중 인코딩 → flip 만 유지
    "outcome", "margin_quadrant", "correct_v1_0_8_0", "correct_v1_0_8_4", "v084_missed",
    # GT/재라벨 중복 → ground_truth / relabel_transition 만 유지
    "folder", "relabeled", "original_event",
    # 정답기준 수치축 → margin_<vtag> 2개만 유지
    "gt_cos_v080", "gt_cos_v084", "gt_rel_v080", "gt_rel_v084",
    "margin_v1_0_8_0", "margin_v1_0_8_4",           # 옛 정의(top1−top2)
    # gt_rel_delta: 코드 주석대로 fixed 중 354건이 역부호라 심각도 정렬 부적합 → margin_delta 가 대체
    "gt_rel_delta",
    # 변화축 → shift_direction 만 유지.
    #   shift_mag_q: 13,144 중 10,880(82.8%)이 "변화없음" 한 통. 존재 이유였던 flip_confidence 는
    #                871영상 시절 필드로 이 데이터셋엔 없다. 심각도 정렬은 margin_delta 담당
    #   dscore_pred_*: 유일 소비자가 shift_viz(아래에서 삭제). 자기/경쟁 분해는
    #                  flip_reason + why_before/after 가 담는다
    "pred_shift", "shift_mag", "shift_mag_q", "dscore_pred_v080", "dscore_pred_v084",
    "dscore_normal", "dscore_falldown", "dscore_fire", "dscore_smoke",
    # 각도: 고정 카메라 3대라 tilt_bin 도 사실상 카메라 프록시(두 bin 에 9,758장)이고,
    # 뱅크 A/B 는 동일 프레임 대응비교라 층화 교란이 원리적으로 불가능하다
    "camera_angle", "angle_method", "tilt_deg", "angle_tilt_spread", "angle_stable", "tilt_bin",
    # 구버전 class_best 는 관성 유지였음 (codex): 어느 워크플로에도 안 쓰임
    "class_best_v1_0_8_0",
    # why 중복 → why_before/after + flip_reason 속성만 유지
    "why_text", "margin_v084_bin",
]
# shift_viz: 축이 dscore_pred 2개 = GT-free 좌표. 전 프레임에 GT 가 있는 데이터셋에서
#            GT-free 축은 margin_viz 에 엄격히 열등하다.
SLIM_DROP_BRAINS = ["cover_viz", "tradeoff_viz", "shift_viz"]
SLIM_DROP_WORKSPACES = ["relabel", "shift", "shift-where", "tradeoff", "coverage"]
SLIM_DROP_VIEWS = ["00_relabeled", "01_disagreement", "02_recover", "03_lose"]
SLIM_NOISE = ["embedding"]   # 00_analysis 제외는 embedding 만 (src_video 는 코호트 키로 노출)


def sidebar_subpaths(keep: list[str], universe: list[str]) -> list[str]:
    """사이드바 그룹에 넣을 서브경로 — **1단만**.

    ⚠️ FiftyOne 1.19 App 의 `pullSidebarValue` 는 doc-list 분기에서 `keys[0]`/`keys[1]` 만
    본다 (`sample[keys[0]].map(x => x[keys[1]])`). 부모가 ListField(EmbeddedDocument) 인
    3단 경로(`class_best_v1_0_8_4.classifications.label` 등)를 sidebar_groups 에 넣으면
    모달을 열 때 `sample["class_best_v1_0_8_4"]` 가 dict 라 `.map is not a function` 으로
    App 전체가 죽는다. FiftyOne 기본 그룹도 1단까지만 넣는다 — 그걸 따른다.
    빠진 서브경로는 App 이 라벨 엔트리 안에서 알아서 렌더하므로 손실이 아니다.
    """
    return [u for u in universe
            if any(u.startswith(p + ".") and u.count(".") == p.count(".") + 1 for p in keep)]


def stage_slim() -> None:
    if PROFILES[PROFILE]["dataset"] != "source-h":
        raise SystemExit("slim 은 source-h 전용 — SLIM_DROP_* 하드코딩 리스트가 다른 데이터셋의 "
                         "필드/brain/뷰를 파괴한다 (스펙 §5-1). frames 프로필에서 영구 금지")
    import fiftyone as fo

    ds = fo.load_dataset("source-h")
    sch = ds.get_field_schema()
    drop = [f for f in SLIM_DROP_FIELDS if f in sch]
    if drop:
        ds.delete_sample_fields(drop)
    for b in SLIM_DROP_BRAINS:
        if ds.has_brain_run(b):
            ds.delete_brain_run(b)
    for w in SLIM_DROP_WORKSPACES:
        if w in ds.list_workspaces():
            ds.delete_workspace(w)
    for v in SLIM_DROP_VIEWS:
        if v in ds.list_saved_views():
            ds.delete_saved_view(v)
    log(f"slim: 필드 −{len(drop)} → {len(ds.get_field_schema())}개 / "
        f"brain {ds.list_brain_runs()} / ws {ds.list_workspaces()}")

    # 워크스페이스 5개 재정의. `prompt` 는 이번에 추가한 프롬프트-품질 색칠이다.
    # ⚠️ `prompt` 를 볼 때는 **먼저 Color by 를 `camera` 로 바꿔 널 모델을 확인**하라.
    #    승자문장→카메라 예측력이 82~87% 라, 그림이 카메라 지도와 닮으면 그 그림은
    #    프롬프트에 대해 아무것도 말하지 않는다.
    a_tag, vb_tag = (vtag(v) for v in VERSIONS)
    workspaces = (("flips", "emb_viz", "flip.label"),
                  ("margin", "margin_viz", "flip.label"),
                  ("prompt", "emb_viz", f"winner_purity_{vb_tag}.label"),
                  ("gap", "emb_viz", "gap_cluster.label"),
                  ("explore", "emb_viz", "ground_truth.label"))
    for name, brain, color in workspaces:
        try:
            space = fo.Space(children=[
                fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
                fo.Space(children=[fo.Panel(type="Embeddings",
                                            state={"brainResult": brain, "colorByField": color})]),
            ], orientation="horizontal")
            if name in ds.list_workspaces():
                ds.delete_workspace(name)
            ds.save_workspace(name, space, description=f"{brain} (색: {color})")
        except Exception as exc:  # noqa: BLE001
            log(f"slim: 워크스페이스 {name} 실패 {exc!r}")

    # 사이드바: 워크플로 이름의 그룹 6개 (자동판정 대신 도메인 구성)
    defaults = fo.DatasetAppConfig.default_sidebar_groups(ds)
    G = type(defaults[0])
    v0t, v4t = (v.replace(".", "_") for v in VERSIONS)
    a, b = (vtag(v) for v in VERSIONS)
    layout = [
        ("① 판정", True, ["flip", "flip_reason", "ground_truth",
                          f"pred_{v4t}", f"pred_{v0t}"]),
        ("② 근거", False, ["why_before", "why_after", f"top_prompt_{v4t}", f"top_prompt_{v0t}",
                           "shift_direction"]),
        # ③ 은 Embeddings 패널 Color by 전용 축 — 승자 문장의 품질을 프레임에 내린 것
        ("③ 프롬프트 품질", True, [f"winner_purity_{b}", f"winner_purity_{a}",
                                  f"winner_loo_{b}", f"winner_loo_{a}", "winner_pair_cos"]),
        ("④ 다음 타깃", False, ["gap_cluster", "gap_deficit"]),
        ("⑤ 층화", False, ["camera", "relabel_transition", "src_video", "frame_index"]),
        ("⑥ 상세", False, [f"class_best_{v4t}", f"margin_{a}", f"margin_{b}", "margin_delta"]),
    ]
    universe = list(ds.get_field_schema(flat=True))
    groups, assigned = [], set()
    for g in defaults:
        if g.name in ("tags", "label tags"):
            groups.append(g)
            assigned.update(g.paths)
    for name, exp, paths in layout:
        keep = [p for p in paths if p in universe]
        subs = sidebar_subpaths(keep, universe)
        groups.append(G(name=name, paths=keep + [s2 for s2 in subs if s2 not in keep],
                        expanded=exp))
        assigned.update(keep + subs)
    for g in defaults:                                # metadata 는 이름 유지 + 맨 끝
        if g.name == "metadata":
            groups.append(G(name="metadata", paths=g.paths, expanded=False))
            assigned.update(g.paths)
    ds.app_config.sidebar_groups = groups
    from fiftyone.core.odm.dataset import ActiveFields

    # ⚠️ **active_fields 는 allowlist 이고, 여기 없는 필드로 Color by 를 걸면 App 이 죽는다**
    #    ("TypeError: Cannot read properties of undefined (reading 'id')" → 에러 화면).
    #    2026-07-31 실측: 워크스페이스 flips/margin(색 flip.label, active) = 정상,
    #    gap(색 gap_cluster.label, non-active) = 크래시. `gap` 은 이 커밋 이전부터 깨져 있었다.
    #    그래서 목록을 손으로 적지 않고 **워크스페이스 색 필드에서 파생**한다 —
    #    워크스페이스를 늘려도 자동으로 따라온다.
    color_roots = [c.split(".")[0] for _, _, c in workspaces]
    active = ["ground_truth", "flip"] + color_roots + [
        f"winner_purity_{a_tag}", f"winner_loo_{vb_tag}", f"winner_loo_{a_tag}",
        "winner_pair_cos", "camera",   # 사용자가 Color by 로 토글할 축들 (+ camera=널 모델)
    ]
    paths = list(dict.fromkeys(p for p in active if p in ds.get_field_schema()))
    ds.app_config.active_fields = ActiveFields(paths=paths, exclude=False)
    log(f"slim: active_fields(색칠 허용) {paths}")
    ds.save()

    # 00_analysis 재저장 (남은 필드 기준 노이즈 제외)
    excl = [f for f in SLIM_NOISE if f in ds.get_field_schema()]
    if "00_analysis" in ds.list_saved_views():
        ds.delete_saved_view("00_analysis")
    ds.save_view("00_analysis", ds.exclude_fields(excl))
    log(f"slim 완료: 필드 {len(ds.get_field_schema())} / brain {len(ds.list_brain_runs())} / "
        f"ws {len(ds.list_workspaces())} / views {len(ds.list_saved_views())}")


# ────────────────────── report ──────────────────────
def stage_report() -> None:
    g = json.load(open(f"{GEO}/geometry.json", encoding="utf-8"))
    ab = json.load(open(f"{GEO}/ablation.json", encoding="utf-8")) if os.path.exists(f"{GEO}/ablation.json") else None
    gp = json.load(open(f"{GEO}/gap.json", encoding="utf-8")) if os.path.exists(f"{GEO}/gap.json") else None

    L: list[str] = []
    A = L.append
    A("# 프롬프트 뱅크 기하 분석 — 개수가 아니라 위치인가\n")
    A(f"- 생성: {time.strftime('%Y-%m-%d %H:%M')} | 프레임 {g['n_frames']:,}장 (사람 재라벨 GT)")
    A("- 가설 H1=뱅크 크기(개수) / H2=문장의 임베딩 공간 배치(기하)\n")

    A("## 1. 동일 예산 검정 (H1 vs H2 의 1차 판정)\n")
    A("| 조건 | micro accuracy |")
    A("|---|---|")
    A(f"| v1.0.8.0 전체 ({12480:,}개) | {g['full'][V0]['micro']:.2%} |")
    eq = g["equal_budget_v084_at_12480"]
    A(f"| **v1.0.8.4 를 12,480개로 축소** (층화 ×{SEEDS} seeds) | **{eq['micro_mean']:.2%} ± {eq['micro_std']:.2%}** |")
    A(f"| v1.0.8.4 전체 ({16125:,}개) | {g['full'][V4]['micro']:.2%} |")
    delta_geo = eq["micro_mean"] - g["full"][V0]["micro"]
    delta_cnt = g["full"][V4]["micro"] - eq["micro_mean"]
    A(f"\n→ 같은 개수에서의 차이(**기하 효과**) = {delta_geo * 100:+.1f}%p, "
      f"개수를 16,125로 늘린 추가분(**개수 효과**) = {delta_cnt * 100:+.1f}%p\n")

    A("## 2. matched-min (클래스별 동수)\n")
    mm = g["matched_min"]
    A(f"클래스별 n = {g['matched_min_sizes']} 로 양쪽 통일 (falldown 은 v084 가 3,000→160 으로 깎임)\n")
    A("| 뱅크 | micro | " + " | ".join(CLASS_NAMES[c] for c in sorted(CLASS_NAMES)) + " |")
    A("|---|---|" + "---|" * len(CLASS_NAMES))
    for v in VERSIONS:
        pc = mm[v]["per_class_mean"]
        A(f"| {v} | {mm[v]['micro_mean']:.2%}±{mm[v]['micro_std']:.2%} | "
          + " | ".join(f"{pc[CLASS_NAMES[c]]:.1%}" for c in sorted(CLASS_NAMES)) + " |")
    A("")

    A("## 3. 클래스별 한계곡선 (개수의 한계효용)\n")
    for cname, by_v in g["marginal_curves"].items():
        A(f"### {cname}\n")
        A("| 프롬프트 수 | " + " | ".join(VERSIONS) + " |")
        A("|---|---|---|")
        sizes = sorted({p["size"] for v in VERSIONS for p in by_v[v]})
        for s in sizes:
            row = []
            for v in VERSIONS:
                m = next((p for p in by_v[v] if p["size"] == s), None)
                row.append(f"{m['recall_mean']:.1%}±{m['recall_std']:.1%}" if m else "—")
            A(f"| {s:,} | " + " | ".join(row) + " |")
        A("")

    A("## 4. per-prompt 기하 통계\n")
    A("| 뱅크 | 프롬프트 | 승자 | 사용률 | 승수↔근접도 Spearman | 승자 근접도 | 비승자 근접도 |")
    A("|---|---|---|---|---|---|---|")
    for v in VERSIONS:
        s = g["prompt_stats"][v]
        A(f"| {v} | {s['n_prompts']:,} | {s['n_winners']:,} | {s['utilization']:.2%} | "
          f"{s['spearman_wins_vs_proximity']:.3f} | {s['winner_proximity_mean']:.4f} | "
          f"{s['loser_proximity_mean']:.4f} |")
    A("")

    if ab:
        A("## 5. 절제 실험 — 장면 접두가 벡터를 이미지 영역으로 옮기는가\n")
        for cname, entry in ab.items():
            A(f"### {cname} (GT {entry['n_frames']:,}프레임)\n")
            A("| 뱅크 | 승수 | 변형 | would-win | 평균 cos | 문장 |")
            A("|---|---|---|---|---|---|")
            for p in entry["prompts"]:
                for vn, vo in p["variants"].items():
                    A(f"| {p['bank']} | {p['wins']} | {vn} | {vo['would_win_rate']:.1%} | "
                      f"{vo['mean_cos']:.4f} | {vo['text'][:70]} |")
            A("")

    if gp:
        A("## 6. 커버리지 공백 지도 + 문장 프로브\n")
        for cname, entry in gp.items():
            A(f"### {cname} — 미검출 {entry['n_missed']:,}프레임\n")
            for cl in entry.get("clusters", []):
                A(f"**{cl['cluster']}** (n={cl['n']}, 평균 부족분 {cl['mean_deficit']:.4f})")
                A(f"- 현재 이 군집을 잡아먹는 프롬프트: "
                  + " / ".join(f"[{w['n']}] {w['text']}" for w in cl["top_winner_prompts"][:2]))
                if cl["probes"]:
                    b = cl["probes"][0]
                    A(f"- 최고 프로브: would-win {b['would_win_rate']:.1%} — “{b['text']}”")
                A("")
    text = "\n".join(L)
    os.makedirs(REPORT_DIR, exist_ok=True)
    out = f"{REPORT_DIR}/sourceh_prompt_geometry.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write(text)
    log(f"report → {out}")
    print("\n" + text)


def _load_frames_ledger() -> list[dict]:
    return list(jsonl_load(f"{WORK}/ledger.jsonl").values())


def _append_run(run_id: str, domain: str, **kw) -> None:
    import resource

    os.makedirs(GEO, exist_ok=True)
    rec = {"run_id": run_id, "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
           "profile": PROFILE, "domain": domain,
           "mem_peak_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024, **kw}
    with open(f"{GEO}/runs.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


BANK_FIELDS = ("bank_domain", "bank_pred", "bank_decision_margin",
               "bank_shift", "bank_gap", "bank_gt")
# weak(SAM3 normalized_class) → 뱅크 클래스. none/person 은 어느 쪽으로도 주장 불가 → 미등재.
WEAK_TO_BANK = {"fall": "falldown", "fire": "fire", "smoke": "smoke"}


def stage_score() -> None:
    """frames: 도메인 샤드 GT-free 채점 → 필드 publish (clear-then-set) + 런 원장."""
    m = load_domain_map()
    rows = _load_frames_ledger()
    total = len(rows)
    by_dom: dict[str, list] = collections.defaultdict(list)
    for r in rows:
        if r.get("domain"):
            by_dom[r["domain"]].append(r)
    n_gt = sum(1 for r in rows if r.get("gt_class", -1) >= 0)
    log(f"[stamp] score: 전체 {total:,} / 매핑 {sum(map(len, by_dom.values())):,}"
        f" ({len(by_dom)}개 도메인) / GT {n_gt}")

    import fiftyone as fo

    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    run_id = f"score-{time.strftime('%Y%m%d-%H%M%S')}"

    # clear-then-set 은 hard-skip 판정보다 먼저 — 매핑이 비워진(축소된) 경우에도 이전 런의
    # stale 필드를 반드시 제거해야 한다 (스펙 §8: stale 값이 가장 악질적인 분석 거짓말)
    sch = ds.get_field_schema()
    for fld in BANK_FIELDS:
        if fld in sch:
            ds.clear_sample_field(fld)

    if not by_dom:
        ds.info["bank_run"] = {"run_id": run_id, "profile": PROFILE, "domains": {}, "errors": {},
                               "n_gt": n_gt, "total": total,
                               "ts": time.strftime("%Y-%m-%d %H:%M")}
        ds.save()
        log("score: 매핑된 도메인 없음 → hard-skip (0단계). bank_domain_map.yaml 의 "
            "domains 를 노션 버전관리 페이지 기준으로 시드하면 열린다")
        return

    z = np.load(f"{WORK}/embed.npz", allow_pickle=True)
    key2i = {str(k): i for i, k in enumerate(z["key"])}
    Xall = z["vec"].astype(np.float32)
    Xall /= np.linalg.norm(Xall, axis=1, keepdims=True)

    ok_doms: list[str] = []
    errors: dict[str, str] = {}
    for dom in sorted(by_dom):
        try:
            _score_domain(ds, m, dom, by_dom[dom], key2i, Xall, run_id)
            ok_doms.append(dom)
        except Exception as exc:  # noqa: BLE001 — per-domain fail-forward (파이프라인 관례)
            log(f"score {dom}: 실패 {exc!r} — 다음 도메인 진행")
            _append_run(run_id, dom, error=repr(exc))
            errors[dom] = repr(exc)
            # 실패한 도메인의 이전 런 산출물이 남아있으면 gap/gtsync/report 가 이번 런의
            # 결과인 것처럼 오소비한다 (스펙 §8) — 반드시 같이 제거한다.
            for stale in (f"{GEO}/{dom}_score.npz", f"{GEO}/{dom}_queue.json"):
                if os.path.exists(stale):
                    os.remove(stale)
                    log(f"score {dom}: stale 캐시 삭제 {stale}")
    ds.info["bank_run"] = {
        "run_id": run_id, "profile": PROFILE,
        "domains": {d: {"a": m["domains"][d]["bank_a"], "b": m["domains"][d]["bank_b"],
                        "n": len(by_dom[d])} for d in ok_doms},
        "errors": errors,
        "n_gt": n_gt, "total": total, "ts": time.strftime("%Y-%m-%d %H:%M"),
    }
    ds.save()
    log(f"score 완료: run={run_id}")


def _score_domain(ds, m: dict, dom: str, drows: list, key2i: dict,
                  Xall: np.ndarray, run_id: str) -> None:
    import fiftyone as fo

    cfg = m["domains"][dom]
    va, vb = cfg["bank_a"], cfg["bank_b"]
    banks = {}
    for v in (va, vb):
        path = f"{PROMPT_DIR}/{v}.npz"
        if not os.path.exists(path):
            raise FileNotFoundError(f"뱅크 npz 없음: {path} — 먼저 bank 스테이지로 생성")
        zb = np.load(path, allow_pickle=True)
        banks[v] = {"vec": zb["vec"].astype(np.float32), "cls": zb["cls"].astype(np.int64),
                    "prompt": [str(p) for p in zb["prompt"]]}
    keys = [r["key"] for r in drows if r["key"] in key2i]
    if not keys:
        log(f"[stamp] score {dom}: embed 교집합 0 → skip (ledger 재실행 필요?)")
        return
    X = Xall[[key2i[k] for k in keys]]

    best_a, _ = bank_best_stream(X, banks[va])
    best_b, arg_b = bank_best_stream(X, banks[vb])
    pred_a, pred_b = predict(best_a), predict(best_b)

    def dmargin(best: dict) -> np.ndarray:
        M = np.stack([best[c] for c in sorted(best)], axis=1)
        M.sort(axis=1)
        return (M[:, -1] - M[:, -2]).astype(np.float32)   # decision margin = top1−top2 (GT-free)

    margin_a, margin_b = dmargin(best_a), dmargin(best_b)

    ds.set_values("bank_domain", {k: dom for k in keys}, key_field="id")
    ds.set_values("bank_pred", {k: fo.Classification(label=CLASS_NAMES[int(p)])
                                for k, p in zip(keys, pred_b)}, key_field="id")
    ds.set_values("bank_decision_margin",
                  {k: float(v) for k, v in zip(keys, margin_b)}, key_field="id")
    ds.set_values("bank_shift", {
        k: fo.Classification(label=(f"{CLASS_NAMES[int(a)]}→{CLASS_NAMES[int(b)]}"
                                    if a != b else "unchanged"))
        for k, a, b in zip(keys, pred_a, pred_b)}, key_field="id")

    # weak concordance (참고 신호 — recall 아님, 스펙 §7)
    weak = ds.select(keys, ordered=True).values("normalized_class")
    wmask = [i for i, w in enumerate(weak) if WEAK_TO_BANK.get(w or "")]
    concordance = (float(np.mean([CLASS_NAMES[int(pred_b[i])] == WEAK_TO_BANK[weak[i]]
                                  for i in wmask])) if wmask else None)

    np.savez_compressed(f"{GEO}/{dom}_score.npz",
                        key=np.array(keys), pred_a=pred_a, pred_b=pred_b,
                        margin=margin_b, margin_a=margin_a,
                        **{f"best_b_{c}": best_b[c] for c in best_b},
                        **{f"arg_b_{c}": arg_b[c] for c in arg_b})
    n_shift = int((pred_a != pred_b).sum())
    log(f"score {dom}: n={len(keys):,} {va}→{vb} 예측변화 {n_shift:,} "
        f"({n_shift / len(keys):.1%}) / margin 중앙값 {np.median(margin_b):.4f}"
        f"{f' / concordance(weak,참고) {concordance:.1%} n={len(wmask)}' if concordance is not None else ''}")
    _append_run(run_id, dom, bank_a=va, bank_b=vb, n_scored=len(keys), n_shift=n_shift,
                margin_median=float(np.median(margin_b)),
                concordance_weak=concordance, n_weak=len(wmask))


def stage_gap_frames() -> None:
    """도메인별 저확신 꼬리(margin 하위 10%) 군집 + 리뷰 큐(weak 불일치 × 저확신) 뷰."""
    from sklearn.cluster import KMeans

    import fiftyone as fo

    m = load_domain_map()
    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    if not os.path.exists(f"{WORK}/embed.npz"):
        log("[stamp] gap: embed.npz 없음(매핑 0) → hard-skip")
        return
    z = np.load(f"{WORK}/embed.npz", allow_pickle=True)
    key2i = {str(k): i for i, k in enumerate(z["key"])}
    Xall = z["vec"].astype(np.float32)
    Xall /= np.linalg.norm(Xall, axis=1, keepdims=True)

    run_id = f"gap-{time.strftime('%Y%m%d-%H%M%S')}"
    for dom in sorted(m["domains"]):
        sp = f"{GEO}/{dom}_score.npz"
        if not os.path.exists(sp):
            log(f"[stamp] gap {dom}: score 캐시 없음 → skip")
            continue
        try:
            sc = np.load(sp, allow_pickle=True)
            keys = [str(k) for k in sc["key"]]
            margin = sc["margin"]
            pred_b = sc["pred_b"]
            tail = np.flatnonzero(margin <= np.quantile(margin, 0.10))
            log(f"[stamp] gap {dom}: n={len(keys):,} / 저확신 꼬리 {len(tail)}")
            # tail 을 한 번만 필터 — 이후 fit/set_values 전부 이 tail_f 기준으로 통일
            # (필터 전 tail 과 필터 후 KMeans 라벨을 zip 하면 인덱스가 밀려 엉뚱한
            #  샘플에 군집이 배정된다 — embed.npz 가 score 이후 축소/재생성된 경우 실제로 발생)
            tail_f = [i for i in tail if keys[i] in key2i]
            if len(tail_f) < len(tail):
                log(f"gap {dom}: embed 교집합 누락 {len(tail) - len(tail_f)}건"
                    " (embed.npz 축소/재생성 가능성) → 해당 건 군집 제외")
            if len(tail_f) >= 40:
                k = max(2, min(6, len(tail_f) // 60))
                emb_idx = [key2i[keys[i]] for i in tail_f]
                km = KMeans(n_clusters=k, n_init=5, random_state=51).fit(Xall[emb_idx])
                ds.set_values("bank_gap",
                              {keys[i]: int(lab) for i, lab in zip(tail_f, km.labels_)},
                              key_field="id")
                log(f"gap {dom}: {k}군집 → bank_gap")
            # 리뷰 큐: 필드 추가 없이 ordered select 뷰 (스펙 §7 — LS 태스크 생성은 범위 밖)
            weak = ds.select(keys, ordered=True).values("normalized_class")

            def qkey(i: int) -> tuple:
                w = WEAK_TO_BANK.get(weak[i] or "")
                disagree = 1 if (w and CLASS_NAMES[int(pred_b[i])] != w) else 0
                return (-disagree, float(margin[i]))          # 불일치 우선, 저확신 오름차순

            order = sorted(range(len(keys)), key=qkey)[:500]
            qname = f"bank: {dom} review-queue"
            if qname in ds.list_saved_views():
                ds.delete_saved_view(qname)
            ds.save_view(qname, ds.select([keys[i] for i in order], ordered=True),
                         description="사람 검수 후보 — weak 불일치 × 저확신 (GT 축적 경로)")
            # report 상위 N 목록 + (선택) LS 반입용 원본 — 스펙 §7
            fps = ds.select([keys[i] for i in order], ordered=True).values("filepath")
            with open(f"{GEO}/{dom}_queue.json", "w", encoding="utf-8") as f:
                json.dump([{"key": keys[i], "filepath": fp, "margin": float(margin[i]),
                            "weak": weak[i], "pred": CLASS_NAMES[int(pred_b[i])]}
                           for i, fp in zip(order, fps)], f, ensure_ascii=False, indent=1)
            log(f"gap {dom}: 리뷰 큐 {len(order)} → 뷰 '{qname}' + {dom}_queue.json")
        except Exception as exc:  # noqa: BLE001 — per-domain fail-forward (파이프라인 관례)
            log(f"gap {dom}: 실패 {exc!r} — 다음 도메인 진행")
            _append_run(run_id, dom, error=repr(exc))


def _sidebar_bank_group(ds) -> None:
    """기존 그룹 보존 + '⑥ 프롬프트뱅크' 그룹 append (멱등)."""
    import fiftyone as fo

    cur = ds.app_config.sidebar_groups
    if cur is None:
        cur = fo.DatasetAppConfig.default_sidebar_groups(ds)
    G = type(cur[0])
    universe = list(ds.get_field_schema(flat=True))
    paths = [p for p in BANK_FIELDS if p in universe]
    paths += [u for u in universe
              if any(u.startswith(p + ".") for p in paths) and u not in paths]
    groups = [g for g in cur if g.name != "⑥ 프롬프트뱅크"]
    for g in groups:
        g.paths = [p for p in g.paths if p not in paths]
    groups.append(G(name="⑥ 프롬프트뱅크", paths=paths, expanded=False))
    ds.app_config.sidebar_groups = groups
    ds.save()


def stage_viz_frames() -> None:
    """x=A margin, y=B margin 산점도(확신도 비교 — GT 아님) + 뷰/워크스페이스/사이드바."""
    import fiftyone as fo
    import fiftyone.brain as fob
    from fiftyone import ViewField as F

    m = load_domain_map()
    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    scored = []
    for dom in sorted(m["domains"]):
        sp = f"{GEO}/{dom}_score.npz"
        if os.path.exists(sp):
            scored.append((dom, np.load(sp, allow_pickle=True)))
    if not scored:
        log("[stamp] viz: 채점 캐시 없음 → hard-skip")
        return
    keys = [str(k) for _, sc in scored for k in sc["key"]]
    ma = np.concatenate([sc["margin_a"] for _, sc in scored]).astype(np.float64)
    mb = np.concatenate([sc["margin"] for _, sc in scored]).astype(np.float64)

    bkey = "bank_margin_viz"
    if ds.has_brain_run(bkey):
        ds.delete_brain_run(bkey)
    fob.compute_visualization(ds.select(keys, ordered=True),   # ordered 필수 — points 정렬 일치
                              points=np.stack([ma, mb], axis=1), brain_key=bkey)

    run_id = f"viz-{time.strftime('%Y%m%d-%H%M%S')}"
    for dom, _ in scored:
        try:
            for nm, view in ((f"bank: {dom} scored", ds.match(F("bank_domain") == dom)),
                             (f"bank: {dom} shifted",
                              ds.match(F("bank_domain") == dom)
                                .match(F("bank_shift.label") != "unchanged")
                                .sort_by("bank_decision_margin"))):
                if nm in ds.list_saved_views():
                    ds.delete_saved_view(nm)
                ds.save_view(nm, view)
        except Exception as exc:  # noqa: BLE001 — per-domain fail-forward (파이프라인 관례)
            log(f"viz {dom}: 실패 {exc!r} — 다음 도메인 진행")
            _append_run(run_id, dom, error=repr(exc))

    ws = "bank-eval"                                # 워크스페이스명 ASCII (App slug 함정)
    space = fo.Space(children=[
        fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
        fo.Space(children=[fo.Panel(type="Embeddings",
                                    state={"brainResult": bkey,
                                           "colorByField": "bank_shift.label"})]),
    ], orientation="horizontal")
    if ws in ds.list_workspaces():
        ds.delete_workspace(ws)
    ds.save_workspace(ws, space,
                      description="x=A margin, y=B margin — 확신도 비교 (GT 정오 아님)")
    _sidebar_bank_group(ds)
    log(f"viz: brain {bkey} / 뷰 {2 * len(scored)}개 / 워크스페이스 {ws} / 사이드바 ⑥")


def _append_gt_eval_keys(run_id: str, domain: str, keys: list) -> None:
    with open(f"{GEO}/gt_eval_keys.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps({"run_id": run_id, "domain": domain, "keys": keys}) + "\n")


def _last_gt_eval_keys() -> set:
    """직전 gtsync 가 평가에 쓴 GT 키 (도메인별 마지막 기록) — 교집합 델타의 기준."""
    path = f"{GEO}/gt_eval_keys.jsonl"
    if not os.path.exists(path):
        return set()
    last: dict[str, list] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            last[r["domain"]] = r["keys"]
    return {k for ks in last.values() for k in ks}


def stage_gtsync() -> None:
    """GT 오버레이 — 재채점 없이 캐시+원장으로 bank_gt/지표 갱신 (score_run 과 분리, 스펙 §8)."""
    import fiftyone as fo

    m = load_domain_map()
    rows = _load_frames_ledger()
    gt_by_key = {r["key"]: r["gt_class"] for r in rows if r.get("gt_class", -1) >= 0}
    src_by_key = {r["key"]: r.get("src_video") for r in rows if r.get("gt_class", -1) >= 0}
    snap = {}
    if os.path.exists(f"{WORK}/gt_snapshot.json"):
        snap = json.load(open(f"{WORK}/gt_snapshot.json", encoding="utf-8"))
    log(f"[stamp] gtsync: GT {len(gt_by_key)} (snapshot {snap.get('sha')}) / "
        f"crosswalk v{snap.get('crosswalk_version')}")
    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    sch = ds.get_field_schema()
    if "bank_gt" in sch:
        ds.clear_sample_field("bank_gt")
    if gt_by_key:
        ds.set_values("bank_gt", {k: fo.Classification(label=CLASS_NAMES[c])
                                  for k, c in gt_by_key.items()}, key_field="id")

    run_id = f"gtsync-{time.strftime('%Y%m%d-%H%M%S')}"
    prev_keys = _last_gt_eval_keys()
    for dom in sorted(m["domains"]):
        sp = f"{GEO}/{dom}_score.npz"
        if not os.path.exists(sp):
            log(f"[stamp] gtsync {dom}: score 캐시 없음 → skip")
            continue
        try:
            sc = np.load(sp, allow_pickle=True)
            keys = [str(k) for k in sc["key"]]
            idx = [i for i, k in enumerate(keys) if k in gt_by_key]
            tier = minn_tier(len(idx))
            n_src = len({src_by_key.get(keys[i]) for i in idx}) if idx else 0
            if tier == "reportable" and n_src < 30:  # 스펙 §7: reportable ≥100 이미지 +소스영상 ≥30
                log(f"[stamp] gtsync {dom}: reportable→exploratory 캡: 소스영상 {n_src} < 30")
                tier = "exploratory"
            log(f"[stamp] gtsync {dom}: GT {len(idx)} / {len(keys):,} → tier={tier} (소스영상 {n_src})")
            rec: dict = {"n_gt": len(idx), "tier": tier, "n_src": n_src, "gt_snapshot": snap.get("sha")}
            if idx:
                gt = np.array([gt_by_key[keys[i]] for i in idx])
                if tier in ("exploratory", "reportable"):
                    rec["recall_a"] = recalls(sc["pred_a"][idx], gt)
                    rec["recall_b"] = recalls(sc["pred_b"][idx], gt)
                    inter = [i for i in idx if keys[i] in prev_keys]
                    if inter:                          # GT 성장 착시 차단 — 교집합 두 벌 보고
                        gti = np.array([gt_by_key[keys[i]] for i in inter])
                        rec["intersection_prev"] = {
                            "n": len(inter),
                            "micro_a": float((sc["pred_a"][inter] == gti).mean()),
                            "micro_b": float((sc["pred_b"][inter] == gti).mean()),
                        }
                else:                                  # counts_only — 백분율 표시 금지
                    rec["counts"] = {"n": len(idx),
                                     "correct_b": int((sc["pred_b"][idx] == gt).sum())}
            _append_run(run_id, dom, **rec)
            _append_gt_eval_keys(run_id, dom, [keys[i] for i in idx])
        except Exception as exc:  # noqa: BLE001 — per-domain fail-forward (파이프라인 관례)
            log(f"gtsync {dom}: 실패 {exc!r} — 다음 도메인 진행")
            _append_run(run_id, dom, error=repr(exc))
    log(f"gtsync 완료: run={run_id}")


def stage_report_frames() -> None:
    rows = _load_frames_ledger()
    total = len(rows)
    by_dom = collections.Counter(r["domain"] for r in rows if r.get("domain"))
    n_gt = sum(1 for r in rows if r.get("gt_class", -1) >= 0)
    runs = []
    if os.path.exists(f"{GEO}/runs.jsonl"):
        with open(f"{GEO}/runs.jsonl", encoding="utf-8") as f:
            runs = [json.loads(x) for x in f if x.strip()]
    latest: dict[tuple, dict] = {}
    for r in runs:                                    # (종류, 도메인) 별 마지막 기록
        latest[(r["run_id"].split("-")[0], r["domain"])] = r

    L: list[str] = []
    A = L.append
    A("# frames_captions 프롬프트 뱅크 평가 리포트\n")
    A(f"- 생성: {time.strftime('%Y-%m-%d %H:%M')} | frame {total:,} (캡션 모달리티 제외)")
    A(f"- 커버리지: 뱅크 매핑 {sum(by_dom.values()):,}"
      f" ({dict(by_dom) if by_dom else '없음 — 0단계: bank_domain_map.yaml 시드 대기'})"
      f" / GT {n_gt} / 전체 {total:,}")
    A("- ⚠️ GT-free 축(pred/shift/margin)은 **확신도·변화**이지 정오가 아니다. "
      "recall 은 min-n tier(≥30) 통과 도메인만. concordance 는 SAM3 참고 신호(정확도 아님).\n")
    A("| 도메인 | 뱅크 A→B | 채점 n | 예측변화 | GT n | tier | recall B (micro) | 교집합 델타 |")
    A("|---|---|---|---|---|---|---|---|")
    for dom in sorted(by_dom):
        s = latest.get(("score", dom), {})
        g = latest.get(("gtsync", dom), {})
        rb = g.get("recall_b", {}).get("micro")
        counts = g.get("counts")
        ip = g.get("intersection_prev")
        if rb is not None:
            rb_txt = f"{rb:.1%}"
        elif counts:                       # counts_only — 건수만, 백분율 환산 금지
            rb_txt = f"{counts['correct_b']}/{counts['n']}건"
        else:
            rb_txt = "NA"
        ip_txt = f"n={ip['n']} B {ip['micro_b']:.1%}" if ip else "—"
        A(f"| {dom} | {s.get('bank_a', '?')}→{s.get('bank_b', '?')} "
          f"| {s.get('n_scored', 0):,} | {s.get('n_shift', 0):,} "
          f"| {g.get('n_gt', 0)} | {g.get('tier', 'no_gt')} | {rb_txt} | {ip_txt} |")
    if not by_dom:
        A("| — | — | 0 | — | 0 | no_gt | NA | — |")
    A("\n## 리뷰 큐 상위 (사람 검수 → GT 축적 경로)\n")
    for dom in sorted(by_dom):
        qp = f"{GEO}/{dom}_queue.json"
        if not os.path.exists(qp):
            continue
        q = json.load(open(qp, encoding="utf-8"))
        A(f"### {dom} (총 {len(q)}건 — 뷰 `bank: {dom} review-queue`)\n")
        for r in q[:5]:
            A(f"- `{os.path.basename(r['filepath'])}` margin={r['margin']:.4f} "
              f"weak={r['weak']} pred={r['pred']}")
        A("")
    A("\n## FiftyOne\n")
    A("- 워크스페이스 `bank-eval` (x=A margin, y=B margin — 확신도 비교)")
    A("- 뷰 `bank: <도메인> scored / shifted / review-queue` — 리뷰 큐가 GT 축적 경로다")
    A("- 사이드바 그룹 `⑥ 프롬프트뱅크`: " + ", ".join(BANK_FIELDS))
    os.makedirs(REPORT_DIR, exist_ok=True)
    out = f"{REPORT_DIR}/bank_eval_report.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    log(f"report → {out}")


def stage_selftest() -> None:
    """데이터 불필요 자가검증 — 스트리밍 리덕션 == 순진 행렬곱, crosswalk fail-closed, min-n."""
    rng = np.random.default_rng(0)
    X = rng.normal(size=(500, 64)).astype(np.float32)
    X /= np.linalg.norm(X, axis=1, keepdims=True)
    V = rng.normal(size=(300, 64)).astype(np.float32)
    V /= np.linalg.norm(V, axis=1, keepdims=True)
    bank = {"vec": V, "cls": rng.integers(0, 4, 300).astype(np.int64),
            "prompt": [f"p{i}" for i in range(300)]}
    best, arg = bank_best_stream(X, bank, batch=64, block=32)   # 일부러 작은 배치로 경계 검증
    for c in sorted(set(bank["cls"].tolist())):
        idx = np.flatnonzero(bank["cls"] == c)
        S = X @ V[idx].T
        assert np.allclose(best[c], S.max(axis=1), atol=1e-6), f"best mismatch c={c}"
        # arg 는 뱅크 전역 인덱스 — 그 프롬프트와의 코사인이 곧 best 여야 한다
        recomputed = np.einsum("ij,ij->i", X, V[arg[c]])
        assert np.allclose(best[c], recomputed, atol=1e-6), f"arg 가 best 를 가리키지 않음 c={c}"
        assert np.isin(arg[c], idx).all(), f"arg 가 타 클래스 프롬프트를 가리킴 c={c}"
    # top-2 스트리밍 == 순진 행렬곱의 1·2위 (LOO counterfactual 이 2위에 전적으로 의존)
    b1, b2, a1 = bank_top2_stream(X, bank, batch=64, block=32)
    for c in sorted(set(bank["cls"].tolist())):
        idx = np.flatnonzero(bank["cls"] == c)
        S = np.sort(X @ V[idx].T, axis=1)
        assert np.allclose(b1[c], S[:, -1], atol=1e-6), f"top1 mismatch c={c}"
        assert np.allclose(b2[c], S[:, -2], atol=1e-6), f"top2 mismatch c={c}"
        assert np.allclose(b1[c], np.einsum("ij,ij->i", X, V[idx][a1[c]]), atol=1e-6), \
            f"a1 이 top1 을 가리키지 않음 c={c}"
    # drop 마스크: 각 클래스 1위를 지우면 새 1위가 옛 2위여야 한다
    drop = np.zeros(len(bank["cls"]), dtype=bool)
    c0 = sorted(set(bank["cls"].tolist()))[0]
    g0 = np.flatnonzero(bank["cls"] == c0)
    drop[g0[a1[c0][0]]] = True
    d1, _, _ = bank_top2_stream(X, bank, drop=drop, batch=64, block=32)
    assert abs(float(d1[c0][0]) - float(b2[c0][0])) < 1e-6, "drop 후 1위가 옛 2위가 아님"

    # artifact 소유권 불변식 — 어떤 스테이지도 slim 이 지우는 필드를 쓰면 안 된다.
    # 수동 매니페스트가 아니라 **자기 소스**를 검사하므로 드리프트하지 않는다.
    # (한계: f-string 으로 조립하는 필드명은 못 잡는다 — 리터럴만 검사)
    import re as _re
    src_txt = open(os.path.abspath(__file__), encoding="utf-8").read()
    written = set(_re.findall(r'set_values\(\s*"([A-Za-z0-9_]+)"', src_txt))
    clash = sorted(written & set(SLIM_DROP_FIELDS))
    assert not clash, (f"스테이지가 slim 삭제 대상을 쓴다 (쓰고→지우고→다시 쓰는 순환): {clash}. "
                       "해당 스테이지의 쓰기를 없애거나 SLIM_DROP_FIELDS 에서 빼라")

    cw = {"fire": "fire", "__no_box_finalized__": "normal"}
    assert crosswalk_class(cw, "fire") == "fire"
    assert crosswalk_class(cw, "patient") is None, "미등재 category 는 None(fail-closed)이어야 한다"
    assert crosswalk_class(cw, "__no_box_finalized__") == "normal"
    assert minn_tier(0) == "no_gt" and minn_tier(5) == "counts_only"
    assert minn_tier(30) == "exploratory" and minn_tier(99) == "exploratory"
    assert minn_tier(100) == "reportable"
    # 사이드바 서브경로는 1단까지만 — 3단이 새면 App 모달이 TypeError 로 죽는다
    uni = ["class_best_v1", "class_best_v1.classifications",
           "class_best_v1.classifications.label", "flip_reason", "flip_reason.before"]
    assert sidebar_subpaths(["class_best_v1", "flip_reason"], uni) == [
        "class_best_v1.classifications", "flip_reason.before"]
    assert vtag("v1.0.8.0") == "v080" and vtag("v1.0.8.4") == "v084"
    assert vtag("v1.0.9.0") == "v090", "새 버전 값이 옛 이름 필드에 덮이면 안 된다"
    assert purity_bin(0.0) == "0-25%" and purity_bin(0.5) == "50-75%" and purity_bin(1.0) == "90-100%"
    assert loo_bin(12) == "유해 +10↑" and loo_bin(0) == "중립 0" and loo_bin(-3).startswith("유익")
    log("selftest OK")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=["bank", "analyze", "ablate", "gap", "prune", "viz", "flips",
                                      "guide", "slim", "report", "all", "selftest",
                                      "score", "gtsync"])
    ap.add_argument("--profile", choices=list(PROFILES),
                    default=os.environ.get("BANK_PROFILE", "sourceh"))
    ap.add_argument("--csv", help="bank 스테이지: 프롬프트 CSV 경로")
    ap.add_argument("--version", help="bank 스테이지: 버전 이름 (npz 파일명)")
    ap.add_argument("--mem-budget-gb", type=float, default=4.0)
    args = ap.parse_args()
    set_profile(args.profile)
    assert_mem_budget(args.mem_budget_gb)
    os.makedirs(GEO, exist_ok=True)

    if PROFILE == "frames":
        sourceh_only = {"analyze", "ablate", "flips", "guide", "slim", "prune"}
        table = {"score": stage_score, "gap": stage_gap_frames, "viz": stage_viz_frames,
                 "gtsync": stage_gtsync, "report": stage_report_frames,
                 "selftest": stage_selftest}
        stages = ["score", "gap", "viz", "gtsync", "report"] if args.stage == "all" else [args.stage]
        for st in stages:
            log(f"───── stage: {st} (profile=frames) ─────")
            if st in sourceh_only:
                raise SystemExit(f"{st} 는 sourceh 프로필 전용 — frames 자격 미달 "
                                 "(팩토리얼=동일도메인 뱅크 2벌, guide/flips=GT 분모 필요. 스펙 §1)")
            if st == "bank":
                if not (args.csv and args.version):
                    raise SystemExit("bank 스테이지는 --csv 와 --version 이 필요하다")
                stage_bank(args.csv, args.version)
                continue
            table[st]()
        log("완료")
        return

    # ⚠️ 순서 고정: flips → prune → guide. guide 의 도입부가 flips.json/prune.json 을 읽고,
    #    slim 은 새 winner_* 필드를 사이드바에 편입해야 하므로 prune 뒤여야 한다.
    stages = ["analyze", "ablate", "gap", "flips", "prune", "viz", "guide", "slim", "report"] \
        if args.stage == "all" else [args.stage]
    for st in stages:
        log(f"───── stage: {st} ─────")
        if st == "selftest":
            stage_selftest()
            continue
        if st in ("score", "gtsync"):
            raise SystemExit(f"{st} 는 frames 프로필 전용")
        if st == "bank":
            if not (args.csv and args.version):
                raise SystemExit("bank 스테이지는 --csv 와 --version 이 필요하다")
            stage_bank(args.csv, args.version)
            continue
        {"analyze": stage_analyze, "ablate": stage_ablate, "gap": stage_gap, "viz": stage_viz,
         "prune": stage_prune, "flips": stage_flips, "guide": stage_guide, "slim": stage_slim,
         "report": stage_report}[st]()
    log("완료")


if __name__ == "__main__":
    sys.exit(main())
