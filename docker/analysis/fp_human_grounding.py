#!/usr/bin/env python3
"""오탐(FP) 예산을 **사람 근거** 위로 옮기기 위한 오염률 측정.

문제 (2026-08-29 실측, `filter_ab/gt_provenance.json`):
    이벤트 macro-F1 분모(3,175 프레임)는 사람 근거 93.9% 인데,
    **normal 오탐 분모(4,323 프레임)는 사람 근거 0.0%** 다 — 4,162 장이 Gemini 캡션 파생이고
    159 장은 근거가 아예 없다(캡션 NULL → 기본값 normal). 즉 G4 게이트
    ("정상 프레임 오탐 > 5% 면 배치 불가") 를 **모델 라벨 위에서** 재고 있다.
    Gemini 가 실제 이벤트를 normal 로 적었다면 정탐을 오탐으로 센다.

이 스크립트가 재는 것 — 오염률 p = P(실제로는 이벤트 | GT=normal):

  S1 provenance   normal 4,323 의 gt_source × event_kind × 카메라 분해 (ledger.jsonl)
  S2 path         경로·파일명에 사람 근거가 남아 있는가 (접근2). 남아 있으면 직접 셀 수 있다
  S3 caption      **결정론적 캡션 감사** — normal 캡션 79종 전수. `sourcei_build.kind_of()` 의
                  CAPTION_RULES 가 놓친/거꾸로 잡은 건을 어휘로 정확히 센다. 양방향이다:
                    (a) normal→event  규칙 미스 (`넘어짐`/`넘어진` 이 `넘어지` 에 안 걸림)
                    (b) event→normal  부정문 오탐 (`넘어지지 않았습니다` 가 `넘어지` 에 걸림)
                  이 층은 **모델 재판단이 아니라 파서 감사**라 정확하다(exact).
  S4 sam3         SAM3 `fallen person`/`fire`/`smoke` 불일치율. 이미 계산돼 있다
                  (`work/sam3.jsonl`, 2026-08-06 build) → **GPU 요청 0건**.
                  ⚠️ SAM3 도 모델이다. GT 가 아니라 **불일치 탐지기**로만 쓴다. 얻는 것은
                  상한뿐: 관측 히트 g = p·sens + (1-p)·fpr, fpr≥0 → **p ≤ g / sens**.
  S5 banks        preds.npz 의 35뱅크 × 3규칙 per-frame 예측으로 후보 오염군 vs 청정 normal
                  의 발화율 대비 → 보정식의 r_e(오염 프레임에서의 발화율)를 실측으로 묶는다
  S6 correct      보정식 + `msmax` 탈락 판정(0.0551 vs 0.05) 뒤집힘 임계 p*
  S7 power        사람 검수 표본설계 — 군집단위는 프레임이 아니라 **세그먼트**(한 이벤트의
                  모든 프레임이 캡션 하나를 공유하므로 세그먼트 내 ICC=1). 카메라 ICC 는 실측.

DB 쓰기 없음. SAM3 API 호출 없음(기존 산출물 재사용).
실행: docker exec docker-analysis-1 sh -c "cd /workspace && COS_THREADS=2 nice -n 19 python3 fp_human_grounding.py"
"""

from __future__ import annotations

import collections
import json
import math
import os
import re

import numpy as np

HY = os.environ.get("HY_ROOT", "/data/fiftyone/sourcei")
LEDGER = f"{HY}/work/ledger.jsonl"
SAM3 = f"{HY}/work/sam3.jsonl"
REPORT = os.environ.get("HY_REPORT", "/data/fiftyone/frames_bank/report/sourcei_gt")
PREDS = f"{REPORT}/preds.npz"
OUT_DIR = f"{REPORT}/filter_ab"
OUT = f"{OUT_DIR}/fp_grounding.json"

CLASS_NAMES = {0: "normal", 1: "falldown", 2: "fire", 3: "smoke"}
# filter_ab.json §2026-08-28 — 이 두 값이 판정의 대상이다
FP_BUDGET = 0.05
OBSERVED = {"base": 0.0490, "msmax": 0.0551, "contain0.8": 0.0463,
            "contain0.6": 0.0470, "and_polar": 0.0490, "msmax+contain": 0.0495}


def log(m: str) -> None:
    print(m, flush=True)


# ══════════════════════════ S3 캡션 어휘 (결정론적) ══════════════════════════
# sourcei_build.CAPTION_RULES 의 실패 모드를 정확히 겨냥한다.
#   원 규칙: ("near_miss", r"뻔") → ("falldown", r"넘어지|쓰러|주저앉|눕") → ...
#   한국어는 음절 조합이라 어간 `넘어지` 는 **넘어지다/넘어지고/넘어지면/넘어지지** 에만
#   문자 그대로 나타난다. 활용형 넘어짐/넘어진/넘어졌/넘어져 는 전부 미스한다.
#   반대로 **부정문** `넘어지지 않았다` 는 `넘어지` 에 걸려 falldown 으로 잡힌다.
FALL_TOKEN = re.compile(r"넘어짐|넘어진|넘어졌|넘어져|넘어지|쓰러|주저앉|자빠|눕")
# 부정: "넘어지지 않-" / "넘어지지는 않-"
NEGATED = re.compile(r"넘어지지\s*(는\s*)?않|쓰러지지\s*(는\s*)?않")
# 미수(near-miss) 표지 — 사람이 실제로 넘어지지 않았음을 캡션이 명시
NEARMISS = re.compile(r"뻔|헛디뎠지만|휘청거렸지만|회복했|균형을\s*되찾|붙잡아|잡아주어")
# 넘어진 주체가 사람인가 물건인가
PERSON = r"(사람들|사람|남성|여성|아이들|아이|어린이|직원|승객|성인|작업자)"
OBJECT = r"(짐|가방|상자|물건|봉투|쇼핑백|잡지|책|종이가방|카트)"
PERSON_FALL = re.compile(PERSON + r"[^.]{0,12}?" + FALL_TOKEN.pattern)
OBJECT_FALL = re.compile(OBJECT + r"[^.]{0,6}?" + FALL_TOKEN.pattern)
FIRE = re.compile(r"화재|화염|불꽃|불길|발화|연소")
SMOKE = re.compile(r"연기|매연")
# 소화기 재배치 등 — 소방설비 언급은 화재가 아니다
FIRE_FALSE = re.compile(r"소화기|소화전|화재경보|소방")
# 자세 불안정만 (넘어지진 않음) — 판단 유예 대상
WOBBLE = re.compile(r"비틀거|휘청|균형을\s*잃")


def caption_verdict(text: str | None) -> tuple[str, str]:
    """(판정, 근거). 판정 ∈ fall_person / fire / smoke / near_miss / fall_object /
    wobble / non_event / no_caption. 순서가 곧 규칙이다 — JSON 에 전수 덤프해 사람이 검증한다."""
    t = (text or "").strip()
    if not t:
        return "no_caption", "캡션 없음"
    if FIRE.search(t) and not FIRE_FALSE.search(t):
        return "fire", "화재 어휘"
    if SMOKE.search(t):
        return "smoke", "연기 어휘"
    if NEGATED.search(t):
        return "near_miss", "부정문(넘어지지 않-)"
    if NEARMISS.search(t) and not re.search(r"뻔[^.]{0,20}넘어짐", t):
        return "near_miss", "미수 표지"
    if PERSON_FALL.search(t):
        return "fall_person", "사람 + 전도 어휘"
    if FALL_TOKEN.search(t):
        if OBJECT_FALL.search(t):
            return "fall_object", "물건만 전도"
        return "wobble", "전도 어휘 있으나 주체 불명"
    if WOBBLE.search(t):
        return "wobble", "자세 불안정만"
    return "non_event", "이벤트 어휘 없음"


VERDICT_CLASS = {"fall_person": 1, "fire": 2, "smoke": 3}


# ══════════════════════════ 로드 ══════════════════════════
def load_ledger() -> list[dict]:
    rows = [json.loads(ln) for ln in open(LEDGER, encoding="utf-8")]
    for r in rows:
        r["seg"] = (r["raw_key"], r["event_index"])
    return rows


def load_sam3() -> dict[str, dict[str, float]]:
    """key → {label: max_score}."""
    out: dict[str, dict[str, float]] = {}
    for ln in open(SAM3, encoding="utf-8"):
        r = json.loads(ln)
        m: dict[str, float] = {}
        for d in r["dets"]:
            lb = d["label"]
            if d["score"] > m.get(lb, 0.0):
                m[lb] = float(d["score"])
        out[r["key"]] = m
    return out


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (round(max(0.0, c - h), 4), round(min(1.0, c + h), 4))


def main() -> None:
    res: dict = {"_meta": {
        "question": "GT=normal 4,323 중 실제로는 이벤트인 비율 p",
        "inputs": {"ledger": LEDGER, "sam3": SAM3, "preds": PREDS},
        "sam3_api_calls": 0,
        "sam3_note": "2026-08-06 build 산출물 재사용 — GPU 요청 0건",
    }}
    led = load_ledger()
    assert len(led) == 7498, len(led)
    norm = [r for r in led if r["gt_class"] == 0]
    ev = [r for r in led if r["gt_class"] != 0]

    # ── S1 provenance ────────────────────────────────────────────────
    by = collections.Counter((r["gt_source"], r["event_kind"]) for r in norm)
    cam_norm = collections.Counter(r["camera"] for r in norm)
    cam_all = collections.Counter(r["camera"] for r in led)
    s1 = {
        "n_normal": len(norm), "n_event": len(ev),
        "normal_by_source_kind": {f"{s}/{k}": v for (s, k), v in by.most_common()},
        "normal_human_grounded": sum(v for (s, _), v in by.items()
                                     if s in ("folder", "filename")),
        "normal_camera_dist": [
            {"camera": c, "n_normal": n, "n_all": cam_all[c],
             "share_of_normal": round(n / len(norm), 4),
             "normal_share_within_camera": round(n / cam_all[c], 4)}
            for c, n in cam_norm.most_common()],
        "n_cameras_with_normal": len(cam_norm),
    }
    res["S1_provenance"] = s1
    log(f"[S1] normal {len(norm):,} · 사람근거 {s1['normal_human_grounded']} "
        f"· 카메라 {len(cam_norm)}/{len(cam_all)}")
    for d in s1["normal_camera_dist"][:6]:
        log(f"     {d['camera'][:44]:44s} {d['n_normal']:5,} "
            f"({d['share_of_normal']*100:5.1f}% of normal)")

    # ── S2 경로·파일명 근거 (접근 2) ──────────────────────────────────
    # kind_of() 와 동일한 정규식으로 normal 프레임의 raw_key 를 다시 훑는다.
    fold_rx = re.compile(r"/(esfalldown|falldown|fire|smoke|normal)/")
    file_rx = re.compile(r"(?:^|[_/])(esfalldown|falldown|fire|smoke|normal)(?=[_.]|$)")
    hits = collections.Counter()
    for r in norm:
        rk = r["raw_key"]
        if fold_rx.search(rk):
            hits["folder"] += 1
        elif file_rx.search(os.path.basename(rk)):
            hits["filename"] += 1
        else:
            hits["none"] += 1
    vids = collections.Counter(r["raw_key"] for r in norm)
    res["S2_path_evidence"] = {
        "normal_frames_with_folder_token": hits["folder"],
        "normal_frames_with_filename_token": hits["filename"],
        "normal_frames_with_no_path_token": hits["none"],
        "distinct_source_videos": len(vids),
        "prefixes": dict(collections.Counter(r["raw_key"].split("/")[0] for r in norm)),
        "verdict": ("경로에 사람 근거가 남아 있지 않다 — 접근2 로는 오염률을 셀 수 없다"
                    if hits["folder"] + hits["filename"] <= 2 else
                    "경로 근거가 존재 — 직접 계수 가능"),
    }
    log(f"[S2] 경로 토큰: folder {hits['folder']} / filename {hits['filename']} / "
        f"없음 {hits['none']}  → {res['S2_path_evidence']['verdict']}")

    # ── S2b 로마자 한글 클래스 토큰 (kind_of() 어휘 밖) ────────────────
    # kind_of() 의 파일명 어휘는 영어 5종(esfalldown|falldown|fire|smoke|normal)뿐이다.
    # 실제 파일명에는 **로마자 표기 한글**도 섞여 있다 — 이건 사람이 붙인 이름이므로
    # folder/filename 과 동급의 사람 근거다. 이 어휘를 넣으면 근거 0 블록이 사라진다.
    ROMAN = {
        "sseureojim": ("falldown", "쓰러짐"), "sseureojin": ("falldown", "쓰러진"),
        "neomeojim": ("falldown", "넘어짐"), "neomeojin": ("falldown", "넘어진"),
        "hwajae": ("fire", "화재"), "yeongi": ("smoke", "연기"),
        "jeongsang": ("normal", "정상"),
    }
    # 판정 표기(정탐/오탐) — 클래스가 아니라 "그 클래스가 맞다/아니다" 를 사람이 확인한 표시
    MARK = {"jeongtam": "정탐(true positive — 사람이 확인)",
            "otam": "오탐(false positive — 사람이 부정)"}
    n_none_ = sum(1 for r in norm if r["gt_source"] == "none")
    rom_hits: dict = {}
    for r in norm:
        base = os.path.basename(r["raw_key"]).lower()
        found = [(t, ROMAN[t]) for t in ROMAN if t in base]
        marks = [MARK[t] for t in MARK if t in base]
        if found or marks:
            k2 = r["raw_key"]
            e = rom_hits.setdefault(k2, {"raw_key": k2, "n_frames": 0,
                                         "gt_source_now": r["gt_source"],
                                         "tokens": [f"{t}={v[1]}→{v[0]}" for t, v in found],
                                         "marks": marks,
                                         "implied_class": found[0][1][0] if found else None})
            e["n_frames"] += 1
    n_rom = sum(e["n_frames"] for e in rom_hits.values()
                if e["implied_class"] and e["implied_class"] != "normal")
    res["S2b_romanized_korean_evidence"] = {
        "why": "kind_of() 파일명 어휘가 영어 5종뿐이라 로마자 한글 표기를 통째로 놓친다. "
               "이건 모델 추론이 아니라 **사람이 붙인 파일명** — folder/filename 과 동급 근거다",
        "lexicon": {k: v[1] for k, v in ROMAN.items()},
        "verdict_markers": MARK,
        "normal_frames_with_romanized_event_token": n_rom,
        "pct_of_normal": round(100 * n_rom / len(norm), 3),
        "pct_of_zero_evidence_block": round(100 * n_rom / max(n_none_, 1), 1),
        "files": sorted(rom_hits.values(), key=lambda x: -x["n_frames"]),
    }
    log(f"[S2b] 로마자 한글 근거: normal {n_rom} 프레임이 사람이 붙인 이벤트 이름을 갖고 있다 "
        f"({100*n_rom/len(norm):.2f}% of normal)")
    for e in sorted(rom_hits.values(), key=lambda x: -x["n_frames"]):
        log(f"      {e['n_frames']:4d}f  src={e['gt_source_now']:8s} {e['raw_key']}  "
            f"{e['tokens']} {e['marks']}")

    # ── S3 캡션 감사 (결정론적, 양방향) ────────────────────────────────
    cap_n: dict[str, dict] = {}
    for r in led:
        t = r["caption"] or ""
        e = cap_n.setdefault(t, {"caption": t, "n_frames": 0, "segs": set(),
                                 "gt": collections.Counter(), "src": collections.Counter()})
        e["n_frames"] += 1
        e["segs"].add(r["seg"])
        e["gt"][CLASS_NAMES[r["gt_class"]]] += 1
        e["src"][r["gt_source"]] += 1
    audit = []
    for t, e in cap_n.items():
        v, why = caption_verdict(t)
        audit.append({"caption": t, "n_frames": e["n_frames"], "n_segments": len(e["segs"]),
                      "assigned_gt": dict(e["gt"]), "gt_source": dict(e["src"]),
                      "verdict": v, "why": why})
    audit.sort(key=lambda x: -x["n_frames"])

    # (a) normal → event : 캡션은 이벤트인데 GT 가 normal (규칙 미스)
    # (b) event → normal : 캡션은 미수/부정인데 GT 가 이벤트 (부정문 오탐)
    # 캡션 근거가 있는 프레임에만 적용한다 (folder/filename 은 사람 근거라 건드리지 않는다).
    miss_a, miss_b, amb = [], [], []
    for a in audit:
        cap_only = a["gt_source"].get("caption", 0)
        if not cap_only:
            continue
        v = a["verdict"]
        norm_cnt = a["assigned_gt"].get("normal", 0)
        evt_cnt = sum(n for c, n in a["assigned_gt"].items() if c != "normal")
        if v in VERDICT_CLASS and norm_cnt:
            miss_a.append({**a, "n_affected": norm_cnt, "should_be": CLASS_NAMES[VERDICT_CLASS[v]]})
        if v in ("near_miss", "fall_object", "non_event") and evt_cnt:
            miss_b.append({**a, "n_affected": evt_cnt, "should_be": "normal"})
        if v == "wobble":
            amb.append(a)
    n_a = sum(x["n_affected"] for x in miss_a)
    n_b = sum(x["n_affected"] for x in miss_b)
    n_amb = sum(x["n_frames"] for x in amb)
    n_cap_norm = sum(1 for r in norm if r["gt_source"] == "caption")
    n_none = sum(1 for r in norm if r["gt_source"] == "none")
    res["S3_caption_audit"] = {
        "n_distinct_captions": len(audit),
        "n_distinct_captions_on_normal": sum(1 for a in audit
                                             if a["assigned_gt"].get("normal")),
        "normal_to_event_frames": n_a,
        "normal_to_event_pct_of_normal": round(100 * n_a / len(norm), 3),
        "normal_to_event_pct_of_caption_normal": round(100 * n_a / max(n_cap_norm, 1), 3),
        "event_to_normal_frames": n_b,
        "event_to_normal_pct_of_event": round(100 * n_b / len(ev), 3),
        "ambiguous_wobble_frames": n_amb,
        "net_normal_denominator_delta": n_b - n_a,
        "normal_to_event_items": [{k: x[k] for k in
                                   ("caption", "n_frames", "n_segments", "n_affected",
                                    "should_be", "verdict", "why")} for x in miss_a],
        "event_to_normal_items": [{k: x[k] for k in
                                   ("caption", "n_frames", "n_segments", "n_affected",
                                    "assigned_gt", "verdict", "why")} for x in miss_b],
        "ambiguous_items": [{k: x[k] for k in ("caption", "n_frames", "assigned_gt",
                                               "why")} for x in amb],
        "full_caption_table": audit,
    }
    log(f"[S3] 캡션 {len(audit)}종 전수 · normal→event(규칙미스) {n_a} 프레임 "
        f"({100*n_a/len(norm):.2f}% of normal) · event→normal(부정문) {n_b} 프레임 "
        f"({100*n_b/len(ev):.2f}% of event) · 판단유예 {n_amb}")
    for x in miss_a:
        log(f"     [a] {x['n_affected']:4d}f  {x['caption'][:56]}")
    for x in miss_b:
        log(f"     [b] {x['n_affected']:4d}f  {x['caption'][:56]}")

    # ── S4 SAM3 불일치 (기존 산출물) ──────────────────────────────────
    sam = load_sam3()
    THR = [0.5, 0.6, 0.7]
    EVMAP = {1: "fallen person", 2: "fire", 3: "smoke"}
    s4: dict = {"note": "SAM3 는 모델이다 — GT 아님. 불일치 탐지기로만 사용. "
                        "p ≤ hit_normal / sens 상한만 유효하다.",
                "n_frames_with_sam3": len(sam), "thresholds": {}}
    for thr in THR:
        def hit(r: dict, lbl: str) -> bool:
            return sam.get(r["key"], {}).get(lbl, 0.0) >= thr

        blk: dict = {"per_class_sensitivity_on_human_grounded": {}, "normal": {}}
        sens = {}
        for cls, lbl in EVMAP.items():
            sub = [r for r in ev if r["gt_class"] == cls
                   and r["gt_source"] in ("folder", "filename")]
            k = sum(hit(r, lbl) for r in sub)
            sens[cls] = k / len(sub) if sub else 0.0
            blk["per_class_sensitivity_on_human_grounded"][CLASS_NAMES[cls]] = {
                "n": len(sub), "hits": k, "sens": round(sens[cls], 4),
                "ci95": wilson(k, len(sub))}
        anyhit = [r for r in norm if any(hit(r, lb) for lb in EVMAP.values())]
        g = len(anyhit) / len(norm)
        # 클래스별 상한 (fallen person 만으로도 계산)
        per = {}
        for cls, lbl in EVMAP.items():
            k = sum(hit(r, lbl) for r in norm)
            gc = k / len(norm)
            per[CLASS_NAMES[cls]] = {
                "hits_on_normal": k, "rate": round(gc, 4), "ci95": wilson(k, len(norm)),
                "sens": round(sens[cls], 4),
                "p_upper_bound": round(min(1.0, gc / sens[cls]), 4) if sens[cls] > 0 else None}
        # 가장 타이트한(=가장 작은) 상한이 곧 유효 상한이 아니다 — 클래스별로 독립 상한이라
        # 전체 오염 상한은 합(union) 을 sens 로 나눈 값의 min 이 아니라 클래스별 합.
        sens_mean = np.mean([sens[c] for c in EVMAP if sens[c] > 0]) if sens else 0.0
        blk["normal"] = {
            "any_event_hit": len(anyhit), "rate": round(g, 4), "ci95": wilson(len(anyhit), len(norm)),
            "per_class": per,
            "p_upper_bound_union": round(min(1.0, g / sens_mean), 4) if sens_mean > 0 else None,
            "by_event_kind": {},
            "by_gt_source": {},
            "by_camera": {},
        }
        for key, getter in (("by_event_kind", lambda r: r["event_kind"]),
                            ("by_gt_source", lambda r: r["gt_source"]),
                            ("by_camera", lambda r: r["camera"])):
            agg: dict = {}
            for r in norm:
                k2 = getter(r)
                a2 = agg.setdefault(k2, [0, 0])
                a2[1] += 1
                if any(hit(r, lb) for lb in EVMAP.values()):
                    a2[0] += 1
            blk["normal"][key] = {k2: {"hits": v[0], "n": v[1],
                                       "rate": round(v[0] / v[1], 4), "ci95": wilson(v[0], v[1])}
                                  for k2, v in sorted(agg.items(), key=lambda x: -x[1][1])}
        s4["thresholds"][str(thr)] = blk
    res["S4_sam3_disagreement"] = s4
    b = s4["thresholds"]["0.5"]
    log(f"[S4] SAM3 sens(사람근거): " + " ".join(
        f"{c}={v['sens']:.3f}(n={v['n']})"
        for c, v in b["per_class_sensitivity_on_human_grounded"].items()))
    log(f"[S4] normal 히트 {b['normal']['any_event_hit']}/{len(norm)} "
        f"= {b['normal']['rate']:.4f} → 오염 상한 p ≤ {b['normal']['p_upper_bound_union']}")
    for k2, v in list(b["normal"]["by_event_kind"].items()):
        log(f"     kind {k2:10s} {v['hits']:4d}/{v['n']:5d} = {v['rate']:.4f}")

    # ── S5 뱅크 발화율 대비 (r_e 실측 묶기) ────────────────────────────
    d = np.load(PREDS, allow_pickle=True)
    ids = list(d["ids"])
    # ledger 순서와 preds.npz 순서 정합: preds 는 FiftyOne 순서 = ledger 순서(build 시 동일)
    ok_order = (len(ids) == len(led))
    banks = [str(b) for b in d["banks"]]
    gt_np = d["gt"]
    src_np = d["gt_source"]
    led_gt = np.array([r["gt_class"] for r in led], np.int8)
    aligned = bool((led_gt == gt_np).all()) and ok_order
    # 후보 오염군 마스크 (S3 규칙미스 + SAM3 히트)
    a_caps = {x["caption"] for x in miss_a}
    m_norm = gt_np == 0
    m_rulemiss = np.array([(r["gt_class"] == 0 and (r["caption"] or "") in a_caps)
                           for r in led])
    m_sam = np.array([(r["gt_class"] == 0 and
                       any(sam.get(r["key"], {}).get(lb, 0.0) >= 0.5 for lb in EVMAP.values()))
                      for r in led])
    m_cand = m_rulemiss | m_sam
    m_clean = m_norm & ~m_cand
    m_human_ev = (gt_np != 0) & np.isin(src_np, ["folder", "filename"])
    # S3(b) 부정문 오탐 프레임 — 지금은 event 쪽에 있으나 normal 로 와야 한다
    b_caps = {x["caption"] for x in miss_b}
    m_negation = np.array([(r["gt_class"] != 0 and r["gt_source"] == "caption"
                            and (r["caption"] or "") in b_caps) for r in led])
    # 근거 0 블록 (캡션 NULL → 기본값 normal)
    m_noev = np.array([(r["gt_class"] == 0 and r["gt_source"] == "none") for r in led])
    BLOCKS = {
        "human_event(folder/filename)": m_human_ev,
        "normal/clean": m_clean,
        "normal/rule_miss(25)": m_rulemiss,
        "normal/no_evidence(159)": m_noev,
        "normal/near_miss": np.array([r["gt_class"] == 0 and r["event_kind"] == "near_miss"
                                      for r in led]),
        "normal/other": np.array([r["gt_class"] == 0 and r["event_kind"] == "other"
                                  for r in led]),
        "normal/drop": np.array([r["gt_class"] == 0 and r["event_kind"] == "drop"
                                 for r in led]),
        "event/negation(94)": m_negation,
        "normal/sam3_cand": m_sam,
        "normal/candidate(union)": m_cand,
    }
    s5: dict = {"aligned_with_ledger": aligned, "n_banks": len(banks),
                "n_rulemiss": int(m_rulemiss.sum()), "n_sam3_cand": int(m_sam.sum()),
                "n_candidate": int(m_cand.sum()), "n_clean_normal": int(m_clean.sum()),
                "n_negation": int(m_negation.sum()), "n_no_evidence": int(m_noev.sum()),
                "block_fire_rate_topk": {}, "rules": {}}
    if aligned:
        # 블록별 발화율 — 35뱅크 topk 의 중앙값/사분위. 오염 후보군이 정말 "이벤트처럼"
        # 반응하는지 본다. 청정 normal 과 자릿수가 같으면 오염 근거가 약한 것이다.
        for bname, mk in BLOCKS.items():
            if not mk.any():
                continue
            v = np.array([float((d[f"topk__{bk}"][mk] > 0).mean()) for bk in banks
                          if f"topk__{bk}" in d.files])
            s5["block_fire_rate_topk"][bname] = {
                "n_frames": int(mk.sum()), "median": round(float(np.median(v)), 4),
                "q25": round(float(np.percentile(v, 25)), 4),
                "q75": round(float(np.percentile(v, 75)), 4),
                "max": round(float(v.max()), 4)}
        for bname, st in s5["block_fire_rate_topk"].items():
            log(f"     block {bname:30s} n={st['n_frames']:5d} "
                f"발화율 med={st['median']:.4f} [q25 {st['q25']:.4f}, q75 {st['q75']:.4f}]")
        for rule in ("topk", "argmax", "wave"):
            rows = []
            for bk in banks:
                key = f"{rule}__{bk}"
                if key not in d.files:
                    continue
                p = d[key]
                rows.append({
                    "bank": bk,
                    "fire_on_human_event": round(float((p[m_human_ev] > 0).mean()), 4),
                    "fire_on_clean_normal": round(float((p[m_clean] > 0).mean()), 4),
                    "fire_on_candidate": round(float((p[m_cand] > 0).mean()), 4)
                    if m_cand.any() else None,
                    "fire_on_rulemiss": round(float((p[m_rulemiss] > 0).mean()), 4)
                    if m_rulemiss.any() else None,
                })
            arr = lambda k: np.array([r[k] for r in rows if r[k] is not None])  # noqa: E731
            s5["rules"][rule] = {
                "per_bank": rows,
                "median_fire_on_human_event": round(float(np.median(arr("fire_on_human_event"))), 4),
                "median_fire_on_clean_normal": round(float(np.median(arr("fire_on_clean_normal"))), 4),
                "median_fire_on_candidate": round(float(np.median(arr("fire_on_candidate"))), 4),
                "median_fire_on_rulemiss": round(float(np.median(arr("fire_on_rulemiss"))), 4),
            }
        t5 = s5["rules"]["topk"]
        log(f"[S5] topk 발화율 중앙값 — 사람근거 이벤트 {t5['median_fire_on_human_event']:.4f} "
            f"· 청정 normal {t5['median_fire_on_clean_normal']:.4f} "
            f"· 후보오염 {t5['median_fire_on_candidate']:.4f} "
            f"· 규칙미스 {t5['median_fire_on_rulemiss']:.4f}")
    else:
        log("[S5] ⚠️ preds.npz 와 ledger 정렬 실패 — 뱅크 대비 생략")
    res["S5_bank_firing"] = s5

    # ── S6 보정식 + msmax 뒤집힘 임계 ─────────────────────────────────
    # 관측:  f_obs = (1-p)·f_true + p·r_e      (p = 오염률, r_e = 오염 프레임 발화율)
    # 보정:  f_true = (f_obs - p·r_e) / (1 - p)
    # 방향:  df_true/dp |_{p=0} = f_obs - r_e  → r_e > f_obs 일 때만 보정이 오탐을 낮춘다
    # 뒤집힘: f_true ≤ B  ⇔  p ≥ (f_obs - B) / (r_e - B)      (r_e > B 일 때만 해 존재)
    r_e_emp = (s5["rules"]["topk"]["median_fire_on_human_event"]
               if aligned else None)
    # r_e 의 **해석적 하한** — filter_ab 변형은 35뱅크와 다른 뱅크라 위 실측을 그대로 못 쓴다.
    # 대신 공개된 per-class F1 로부터 recall 하한이 나온다:
    #   F1 = 2PR/(P+R), P ≤ 1 → F1 ≤ 2R/(1+R) → R ≥ F1/(2-F1)
    # 오염 프레임에서의 발화율 r_e 는 그 클래스 recall 과 같은 규모라고 본다.
    F1 = {"base": {"falldown": 0.5280, "fire": 0.6410, "smoke": 0.4755},
          "msmax": {"falldown": 0.5240, "fire": 0.6331, "smoke": 0.4881}}
    r_lb = {v: {c: round(f / (2 - f), 4) for c, f in d2.items()} for v, d2 in F1.items()}
    r_grid = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.80, 1.00]
    if r_e_emp:
        r_grid = sorted(set(r_grid + [round(r_e_emp, 4)]))
    r_grid = sorted(set(r_grid + [min(r_lb["msmax"].values()),
                                  r_lb["msmax"]["falldown"]]))

    def f_true(f_obs: float, p: float, r_e: float) -> float:
        return (f_obs - p * r_e) / (1 - p)

    def p_flip(f_obs: float, r_e: float, B: float = FP_BUDGET) -> float | None:
        if f_obs <= B:
            return 0.0
        if r_e <= B:
            return None  # 해 없음 — 오염을 보정해도 예산 아래로 못 내려간다
        return (f_obs - B) / (r_e - B)

    s6: dict = {
        "formula": "f_true = (f_obs - p*r_e) / (1 - p)",
        "flip_formula": "p* = (f_obs - B) / (r_e - B),  B=0.05,  r_e>B 일 때만 해 존재",
        "sign_rule": "r_e > f_obs 이면 보정이 오탐을 낮추고, r_e < f_obs 이면 오히려 높인다",
        "r_e_empirical_topk_median": r_e_emp,
        "r_e_analytic_lower_bound": {
            "derivation": "F1 = 2PR/(P+R), P≤1 ⇒ R ≥ F1/(2-F1). filter_ab.json 의 per-class F1 사용",
            "per_variant": r_lb,
            "msmax_min_over_classes": min(r_lb["msmax"].values()),
            "msmax_falldown": r_lb["msmax"]["falldown"],
            "note": "오염은 falldown 계열에 몰려 있다(S3) → falldown 하한이 더 적절한 앵커"},
        "budget": FP_BUDGET,
        "variants": {},
        "msmax_flip_threshold_by_r_e": {},
        "corrected_at_measured_p": {},
    }
    for name, f_obs in OBSERVED.items():
        s6["variants"][name] = {
            "f_obs": f_obs,
            "p_flip_by_r_e": {str(r): (None if p_flip(f_obs, r) is None
                                       else round(p_flip(f_obs, r), 4)) for r in r_grid},
        }
    for r in r_grid:
        pf = p_flip(OBSERVED["msmax"], r)
        s6["msmax_flip_threshold_by_r_e"][str(r)] = None if pf is None else round(pf, 4)

    # 실측 p 후보 3종으로 보정값 산출
    p_rule = n_a / len(norm)                       # 하한 (파서 감사, exact)
    p_sam_ub = b["normal"]["p_upper_bound_union"]  # 상한 (SAM3, 모델)
    p_cands = {"rule_miss_exact_lower_bound": p_rule}
    if p_sam_ub:
        p_cands["sam3_upper_bound"] = float(p_sam_ub)
    for pname, p in p_cands.items():
        entry = {}
        for name, f_obs in OBSERVED.items():
            entry[name] = {str(r): round(f_true(f_obs, p, r), 4) for r in r_grid}
        s6["corrected_at_measured_p"][f"p={p:.4f} ({pname})"] = entry
    # ── S6b 분모 수리(denominator repair) ─────────────────────────────
    # 오염률을 "가정"하는 대신, S3 로 **정확히 식별된** 프레임을 분모에서 빼고 더한다.
    #   제거: 규칙미스 n_a (실제 이벤트) + 근거0 n_none (판단 불가 → 게이트 대상 아님)
    #   추가: 부정문 n_b (실제 normal 인데 이벤트로 잘못 들어가 있음)
    # 변형별 그 블록 발화율 x(제거), y(추가) 는 filter_ab 가 저장하지 않아 미지수다 →
    # 전 구간 그리드로 스윙 폭을 보인다. 스윙이 예산을 가로지르면 판정은 견고하지 않다.
    N = len(norm)
    rm = n_a + n_none      # 분모에서 뺄 프레임
    add = n_b              # 분모에 더할 프레임
    s6b: dict = {
        "formula": "f_rep = (f_obs*N - x*rm + y*add) / (N - rm + add)",
        "N": N, "remove": {"rule_miss": n_a, "no_evidence": n_none, "total": rm},
        "add_from_event_side": add,
        "x_note": "x = 제거 블록에서 해당 변형이 발화한 비율 (filter_ab 미저장 → 미지수)",
        "y_note": "y = 추가 블록(부정문 94장)에서의 발화율 (미지수)",
        "grid": {},
    }
    for name, f_obs in (("base", OBSERVED["base"]), ("msmax", OBSERVED["msmax"])):
        g: dict = {}
        for x in (0.0, 0.25, 0.5, 0.75, 1.0):
            for y in (0.0, 0.25, 0.5):
                g[f"x={x},y={y}"] = round((f_obs * N - x * rm + y * add) / (N - rm + add), 4)
        vals = list(g.values())
        s6b["grid"][name] = {"cells": g, "min": min(vals), "max": max(vals),
                             "crosses_budget": bool(min(vals) < FP_BUDGET < max(vals))}
    res["S6b_denominator_repair"] = s6b
    log(f"[S6b] 분모 수리 — 제거 {rm}(규칙미스 {n_a} + 근거0 {n_none}) / 추가 {add}. "
        f"msmax 스윙 {s6b['grid']['msmax']['min']}~{s6b['grid']['msmax']['max']} "
        f"(예산 가로지름={s6b['grid']['msmax']['crosses_budget']})")

    res["S6_correction"] = s6
    log(f"[S6] msmax 뒤집힘 임계 p* — " + " ".join(
        f"r_e={k}:{'해없음' if v is None else f'{v*100:.2f}%'}"
        for k, v in s6["msmax_flip_threshold_by_r_e"].items()))

    # ── S7 사람 검수 표본설계 ─────────────────────────────────────────
    # 군집 단위: 세그먼트(= 한 Gemini 이벤트). 한 세그먼트의 모든 프레임이 캡션 하나를
    # 공유하므로 세그먼트 내 오염 상태는 상수 → 세그먼트 내 ICC = 1 (프레임을 더 봐도
    # 정보가 늘지 않는다). 따라서 유효 표본 = **검수한 세그먼트 수**다.
    seg_norm: dict = {}
    for r in norm:
        e = seg_norm.setdefault(r["seg"], {"n": 0, "camera": r["camera"],
                                           "kind": r["event_kind"], "hits": 0})
        e["n"] += 1
        if any(sam.get(r["key"], {}).get(lb, 0.0) >= 0.5 for lb in EVMAP.values()):
            e["hits"] += 1
    segs = list(seg_norm.values())
    # 카메라 간 ICC — 세그먼트 수준 오염 대리지표(SAM3 히트율 이진화)의 ANOVA ICC(1)
    bycam: dict = collections.defaultdict(list)
    for s in segs:
        bycam[s["camera"]].append(1.0 if s["hits"] > 0 else 0.0)
    groups = [v for v in bycam.values() if len(v) >= 2]
    icc_cam = None
    if len(groups) >= 2:
        allv = np.concatenate([np.array(g) for g in groups])
        gm = allv.mean()
        k_list = [len(g) for g in groups]
        k0 = (sum(k_list) - sum(k * k for k in k_list) / sum(k_list)) / (len(groups) - 1)
        msb = sum(len(g) * (np.mean(g) - gm) ** 2 for g in groups) / (len(groups) - 1)
        msw = sum(((np.array(g) - np.mean(g)) ** 2).sum() for g in groups) / \
            (sum(k_list) - len(groups))
        icc_cam = float((msb - msw) / (msb + (k0 - 1) * msw)) if (msb + (k0 - 1) * msw) > 0 else 0.0
        icc_cam = max(0.0, min(1.0, icc_cam))

    def n_segments_needed(p: float, e: float, m_cam: int, icc: float, z: float = 1.96) -> dict:
        n0 = z * z * p * (1 - p) / (e * e)
        if icc <= 0:
            return {"n_segments": math.ceil(n0), "deff": 1.0, "feasible": True}
        # deff = 1 + (n/m - 1)·icc  (카메라당 세그먼트 수 n/m)
        # n_eff = n/deff ≥ n0  →  n ≥ ...  ;  n→∞ 이면 n_eff → m/icc (상한)
        cap = m_cam / icc
        if n0 >= cap:
            return {"n_segments": None, "deff": None, "feasible": False,
                    "n_eff_ceiling": round(cap, 1),
                    "why": f"카메라 {m_cam}대 · ICC {icc:.3f} 에서 유효표본 상한 {cap:.1f} < 필요 {n0:.1f}"}
        # n0 = n / (1 + (n/m - 1)icc)  →  n0(1 - icc) + n0·icc·n/m = n
        n = n0 * (1 - icc) / (1 - n0 * icc / m_cam)
        return {"n_segments": math.ceil(n), "deff": round(n / n0, 2), "feasible": True}

    m_cam = len(bycam)
    s7 = {
        "sampling_unit": "세그먼트(Gemini 이벤트 1건). 세그먼트 내 프레임은 캡션을 공유하므로 "
                         "세그먼트 내 ICC=1 — 프레임을 더 봐도 유효표본이 늘지 않는다",
        "n_normal_segments": len(segs),
        "n_normal_frames": len(norm),
        "frames_per_segment": {"mean": round(len(norm) / len(segs), 2),
                               "median": int(np.median([s["n"] for s in segs])),
                               "max": max(s["n"] for s in segs)},
        "n_cameras": m_cam,
        "icc_camera_measured": None if icc_cam is None else round(icc_cam, 4),
        "icc_note": "세그먼트 수준 SAM3-히트 이진지표의 ANOVA ICC(1). 오염 자체가 아니라 "
                    "대리지표라 참고값. 프로젝트 기억의 deff 232/ICC 0.51 은 프레임·뱅크점수 "
                    "기준이라 이 설계에 그대로 쓰면 안 된다",
        "designs": {},
        "weighting": "프레임 가중(fp_normal 이 프레임 평균이므로) — 세그먼트를 프레임수 비례"
                     "(PPS)로 뽑으면 세그먼트 단순평균이 프레임가중 추정치가 된다",
    }
    for p_assume in (0.01, 0.03, 0.05, 0.10):
        for e in (0.01, 0.02, 0.03, 0.05):
            for icc_use in ([0.0] + ([round(icc_cam, 4)] if icc_cam else [])):
                s7["designs"][f"p={p_assume}, ±{e}, icc={icc_use}"] = \
                    n_segments_needed(p_assume, e, m_cam, icc_use)
    # 표본추출보다 **전수(census)** 가 싸다 — 모집단이 캡션 129종/세그먼트 619건뿐이고
    # 캡션 층은 이미 S3 에서 결정론적으로 끝났다. 남은 미지는 캡션이 없는 블록뿐이다.
    noev_segs = {r["seg"] for r in norm if r["gt_source"] == "none"}
    amb_segs = {r["seg"] for r in norm
                if caption_verdict(r["caption"])[0] == "wobble"}
    s7["census_alternative"] = {
        "why": "모집단이 작고(캡션 129종·세그먼트 619) 층 내부가 균질하다. 확률표본으로 "
               "±p 를 추정하는 것보다 남은 미지 블록을 전수 검수하는 편이 싸고 정확하다",
        "already_resolved_by_parser_audit": {"n_frames": len(norm) - n_none - n_amb,
                                             "how": "S3 캡션 어휘 감사(결정론적)"},
        "unresolved_blocks": [
            {"block": "캡션 없음(gt_source=none)", "n_frames": n_none,
             "n_segments": len(noev_segs), "camera": "v3_unknown",
             "sam3_hit_rate": b["normal"]["by_gt_source"].get("none", {}).get("rate"),
             "action": "이 세그먼트들만 사람이 보면 오염률이 **정확히** 결정된다"},
            {"block": "판단유예(자세 불안정)", "n_frames": n_amb,
             "n_segments": len(amb_segs),
             "action": "falldown 정의(넘어짐 vs 비틀거림) 를 정하면 결정된다 — 정책 문제"},
        ],
        "review_cost_frames": n_none + n_amb,
        "review_cost_segments": len(noev_segs) + len(amb_segs),
    }
    res["S7_review_design"] = s7
    log(f"[S7] normal 세그먼트 {len(segs)} · 카메라 {m_cam} · 카메라 ICC(실측 대리) "
        f"{icc_cam if icc_cam is None else round(icc_cam,4)}")
    for k2 in (f"p=0.03, ±0.01, icc=0.0", f"p=0.03, ±0.02, icc=0.0"):
        log(f"     {k2} → {s7['designs'].get(k2)}")

    # ── S8 실측 재채점 결과 해석 (rerun 산출물이 있으면) ───────────────
    rr_path = f"{OUT_DIR}/fp_grounding_rerun.json"
    if os.path.exists(rr_path):
        rr = json.load(open(rr_path, encoding="utf-8"))["scenarios"]
        s8: dict = {"source": rr_path, "scenarios": {}}
        n_norm_R = {sc: rr[sc].get("_n_normal_denominator") for sc in rr
                    if "error" not in rr[sc]}
        for sc in ("R0", "R1", "R2", "R3"):
            if sc not in rr or "error" in rr[sc]:
                continue
            s8["scenarios"][sc] = {
                "n_normal_denominator": n_norm_R[sc],
                **{v: {"fp_normal": rr[sc][v]["fp_normal"], "mf1": rr[sc][v]["mf1"],
                       "fires": round(rr[sc][v]["fp_normal"] * n_norm_R[sc], 1),
                       "passes_G4": rr[sc][v]["fp_normal"] <= FP_BUDGET}
                   for v in ("base", "msmax") if v in rr[sc]},
                "n_scored": rr[sc].get("_n_scored")}
        if "R1" in s8["scenarios"] and "R2" in s8["scenarios"]:
            blk = {}
            for v in ("base", "msmax"):
                f1_ = s8["scenarios"]["R1"][v]["fires"]
                f2_ = s8["scenarios"]["R2"][v]["fires"]
                fires159 = f1_ - f2_
                rate159 = fires159 / max(n_none, 1)
                # 159 중 실제 이벤트 비율 q 가 얼마면 G4 를 통과하는가:
                #  (F - q·n·rate) / (N - q·n) ≤ B   (발화가 이벤트 프레임에 균등 분포 가정)
                F, N2, n_ = f1_, n_norm_R["R1"], n_none
                den = n_ * rate159 - FP_BUDGET * n_
                q = (F - FP_BUDGET * N2) / den if den > 0 else None
                blk[v] = {"fires_from_159": round(fires159, 1),
                          "fire_rate_on_159": round(rate159, 4),
                          "fire_rate_elsewhere": round(f2_ / n_norm_R["R2"], 4),
                          "share_of_all_normal_fires": round(fires159 / max(f1_, 1e-9), 4),
                          "q_star_event_share_of_159_to_pass_G4":
                              None if q is None else round(max(0.0, min(1.0, q)), 4)}
            s8["zero_evidence_block"] = {
                "n_frames": n_none, "camera": "v3_unknown (sourcei_v3, 캡션 NULL)",
                "sam3_event_hit_rate": b["normal"]["by_gt_source"].get("none", {}).get("rate"),
                "per_variant": blk,
                "reading": "이 159장은 folder·filename·caption 어느 근거도 없다. normal 은 "
                           "판정이 아니라 **폴백**이다. 오염 여부와 무관하게 G4 분모에서 빠져야 "
                           "한다 — '모름'은 '정상'이 아니다."}
            log(f"[S8] 근거0 159장이 normal 발화의 "
                f"{blk['msmax']['share_of_all_normal_fires']*100:.1f}% (msmax) 를 차지 · "
                f"그 안 발화율 {blk['msmax']['fire_rate_on_159']:.3f} vs 나머지 "
                f"{blk['msmax']['fire_rate_elsewhere']:.4f} · G4 통과에 필요한 실제이벤트 비율 "
                f"q* = {blk['msmax']['q_star_event_share_of_159_to_pass_G4']}")
        res["S8_measured_rescore"] = s8

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=1)
    log(f"→ {OUT}")


# ══════════════════════ rerun: 수리된 GT 로 filter_ab 재채점 ══════════════════════
# 위 S6/S6b 는 **대수**다 (r_e·x·y 가 미지수). filter_ab.py 는 결정론적이므로, GT 만 바꿔
# base/msmax 를 다시 채점하면 뒤집힘 여부를 **실측**할 수 있다.
# filter_ab.py 를 디스크에서 고치지 않는다 — 소스를 읽어 메모리에서 치환하고 exec 한다.
# 치환은 전부 assert 로 검증한다 (조용한 무동작 방지).
PATCH_ABDIR = ('ABDIR = f"{OUT}/filter_ab"', 'ABDIR = os.environ["AB_ABDIR"]')
PATCH_GT = ('gt, cam, ids = d["gt"], d["camera"], list(d["ids"])',
            'gt, cam, ids = d["gt"], d["camera"], list(d["ids"])\n'
            '_ov = np.load(os.environ["AB_GT_OVERRIDE"]); gt = _ov["gt"].astype(gt.dtype)')
PATCH_ALL = ('ALL = np.arange(len(gt))',
             'ALL = _ov["rows"].astype(np.int64)')
PATCH_BOOT = ('    lo, hi = boot_ci(Sm, lab[cols])',
              '    lo, hi = (0.0, 0.0) if NBOOT == 0 else boot_ci(Sm, lab[cols])')
PATCH_VARS = ('ck = f"{ABDIR}/checkpoint.jsonl"',
              'VARIANTS = [v for v in VARIANTS if v["name"] in '
              'os.environ.get("AB_ONLY", "base,msmax").split(",")]\n'
              'ck = f"{ABDIR}/checkpoint.jsonl"')


def rerun() -> None:
    """수리 시나리오별 filter_ab 재채점. scenario ∈ R0(무수정 sanity)/R1(파서수리)/R2(+근거0 제외)."""
    import subprocess
    import sys as _sys
    led = load_ledger()
    gt0 = np.array([r["gt_class"] for r in led], np.int8)
    a_caps, b_caps = set(), set()
    for r in led:  # S3 규칙을 그대로 재적용 (단일 진리)
        v, _ = caption_verdict(r["caption"])
        if r["gt_source"] == "caption":
            if r["gt_class"] == 0 and v in VERDICT_CLASS:
                a_caps.add(r["caption"])
            if r["gt_class"] != 0 and v in ("near_miss", "fall_object", "non_event"):
                b_caps.add(r["caption"])
    # S2b 로마자 근거: 사람이 붙인 파일명이 클래스를 말한다 → 제외가 아니라 **재라벨**이 정답
    ROM = {"sseureojim": 1, "neomeojim": 1, "hwajae": 2, "yeongi": 3}
    rom_cls = []
    for r in led:
        base = os.path.basename(r["raw_key"]).lower()
        rom_cls.append(next((c for t, c in ROM.items() if t in base), 0))
    rom_cls = np.array(rom_cls, np.int8)

    scen = {}
    for name in ("R0", "R1", "R2", "R3"):
        gt = gt0.copy()
        rows = np.arange(len(led))
        if name != "R0":
            for i, r in enumerate(led):
                if r["gt_source"] != "caption":
                    continue
                v, _ = caption_verdict(r["caption"])
                if r["gt_class"] == 0 and r["caption"] in a_caps:
                    gt[i] = VERDICT_CLASS[v]
                elif r["gt_class"] != 0 and r["caption"] in b_caps:
                    gt[i] = 0
        if name == "R2":
            rows = np.array([i for i, r in enumerate(led) if r["gt_source"] != "none"],
                            np.int64)
        if name == "R3":
            # 로마자 근거가 있는 프레임은 그 클래스로 재라벨, 근거가 정말 없는 것만 제외
            m = (rom_cls > 0) & np.array([r["gt_source"] == "none" for r in led])
            gt[m] = rom_cls[m]
            rows = np.array([i for i, r in enumerate(led)
                             if not (r["gt_source"] == "none" and rom_cls[i] == 0)], np.int64)
        scen[name] = (gt, rows)
        log(f"[rerun] {name}: 변경 {int((gt != gt0).sum())} 프레임 · 채점 {len(rows)} 프레임")

    src = open("/workspace/filter_ab.py", encoding="utf-8").read()
    for old, new in (PATCH_ABDIR, PATCH_GT, PATCH_ALL, PATCH_BOOT, PATCH_VARS):
        assert src.count(old) == 1, f"치환 대상 불일치(무동작 위험): {old[:50]}"
        src = src.replace(old, new)
    tmp = "/tmp/_filter_ab_gtrepair.py"
    open(tmp, "w", encoding="utf-8").write(src)

    out: dict = {}
    for name, (gt, rows) in scen.items():
        d = f"{OUT_DIR}/gt_repair/{name}"
        os.makedirs(d, exist_ok=True)
        ov = f"{d}/override.npz"
        np.savez(ov, gt=gt, rows=rows)
        for f in ("checkpoint.jsonl",):  # 재실행 시 stale 체크포인트 제거
            if os.path.exists(f"{d}/{f}"):
                os.remove(f"{d}/{f}")
        env = {**os.environ, "AB_ABDIR": d, "AB_GT_OVERRIDE": ov, "AB_NBOOT": "0",
               "AB_ONLY": "base,msmax", "COS_THREADS": os.environ.get("COS_THREADS", "2")}
        log(f"[rerun] {name} 실행 …")
        p = subprocess.run([_sys.executable, tmp], env=env, cwd="/workspace",
                           capture_output=True, text=True)
        log(p.stdout[-1500:] if p.stdout else "")
        if p.returncode != 0:
            log(f"[rerun] {name} 실패 rc={p.returncode}\n{p.stderr[-2000:]}")
            out[name] = {"error": p.stderr[-2000:]}
            continue
        out[name] = {r["name"]: {k: r[k] for k in ("mf1", "fp_normal", "per_class",
                                                   "n_sentences")}
                     for r in (json.loads(l) for l in open(f"{d}/checkpoint.jsonl"))}
        out[name]["_n_scored"] = int(len(rows))
        out[name]["_n_normal_denominator"] = int((gt[rows] == 0).sum())
        out[name]["_n_gt_changed"] = int((gt != gt0).sum())
    path = f"{OUT_DIR}/fp_grounding_rerun.json"
    json.dump({"scenarios": out, "budget": FP_BUDGET,
               "R0": "무수정 — filter_ab.json 재현 sanity",
               "R1": "파서 수리(규칙미스 25 → 이벤트, 부정문 94 → normal)",
               "R2": "R1 + 근거0 159 프레임 채점 제외",
               "R3": "R1 + 로마자 한글 파일명(사람 근거)로 152장 재라벨, 근거 진짜 0인 7장만 제외"},
              open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    log(f"→ {path}")
    for k, v in out.items():
        if "error" in v:
            continue
        log(f"  {k}: " + " ".join(f"{n}.fp={v[n]['fp_normal']}" for n in ("base", "msmax")
                                  if n in v))


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "rerun":
        rerun()
    else:
        main()
