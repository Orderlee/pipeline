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
import re
import sys
import time

import numpy as np

# ── 프로필 공용 축 3종 (2026-08-18, Phase 1) ────────────────────────────────
# 사이트 층화(화면4·LOPO)를 두 데이터셋에서 같은 코드로 돌리기 위한 프로필 파라미터.
#   group_field   원장 행에서 그룹 축을 읽을 키 (sourceh=카메라 / frames=project)
#   group_plural  CSV 컬럼 접두 — sourceh 은 기존 prune CSV 의 `n_cams_*` 와 **문자 동일**해야
#                 하고, frames 는 계획서(§3)의 `n_projects_win` 과 문자 동일해야 한다
#   group_unit    라벨의 단위 세는 말 ("3대" vs "3곳") — SCOPE 라벨에 박힌다
#   key_join      원장 `key` → FiftyOne 문서 조인 방식.
#                 filepath_tail = `<부모폴더>/<파일명>` (frames_eval·sourcei_build 원장)
#                 sample_id     = key 자체가 FiftyOne sample id (frames_bank_ledger 원장)
#   embed_field   sample_id 프로필에서 임베딩을 스트리밍할 필드 (embed.npz 부재 시 폴백)
PROFILES = {
    "sourceh": {
        "root": "/data/fiftyone/sourceh_v2",
        "dataset": "source-h",
        "prompt_dir": "/data/fiftyone/sourceh/prompts",
        "class_names": {0: "normal", 1: "falldown", 2: "fire", 3: "smoke"},
        "map_yaml": None,
        # 원장 = frames_eval.py (NAS 폴더명 파생 = 사람 재라벨). 그 파일의 GT_SOURCE 와 동일 문자열.
        "expected_gt_source": frozenset({"nas_folder"}),
        "group_field": "camera", "group_plural": "cams", "group_unit": "대",
        "key_join": "filepath_tail", "embed_field": None,
    },
    "frames": {
        "root": "/data/fiftyone/frames_bank",
        # 2026-08-19 개명: FiftyOne 데이터셋 `frames_captions` → `frames` (짝 데이터셋도
        # `frames_captions-prompts` → `frames-prompts`). 프로필 키와 우연히 같아진 것이지
        # 자동 파생이 아니다 — `root`(뱅크 산출물 경로 `frames_bank`)는 개명 대상이 아니다.
        "dataset": "frames",
        "prompt_dir": "/data/fiftyone/sourceh/prompts",   # 뱅크 npz 는 버전 전역 자원 — 공유
        "class_names": {0: "normal", 1: "falldown", 2: "fire", 3: "smoke", 4: "smoking"},
        "map_yaml": os.environ.get("BANK_DOMAIN_MAP", "/workspace/bank_domain_map.yaml"),
        # 원장 = frames_bank_ledger.py (Label Studio finalized = 사람 검수).
        "expected_gt_source": frozenset({"ls_finalized"}),
        # 원장 키 = FiftyOne **샘플 id**(24-hex, 실물 대조 2026-08-18 — image_id(UUID)가
        # 아니다). 미디어가 평면(media/<uuid>.jpg)이라 basename 조인도 불가 — promptmap 등
        # 프레임 조인은 샘플 id 로 한다 (sourceh/sourcei 는 경로 파생 키라 불필요).
        "frame_key_field": "id",
        "group_field": "project", "group_plural": "projects", "group_unit": "곳",
        "key_join": "sample_id", "embed_field": "image_embedding",
    },
    # source-i — `sourcei_build.py` 가 만든 실내 이벤트구간 프레임.
    # ⚠️ recall 벤치마크 아님. 4클래스 GT 가 falldown 57 / fire 5 / smoke 6 구간뿐이고
    #    normal 721 구간(near_miss 509 포함)이 모수다 → **오탐(FP) 스트레스 테스트**로만 읽는다.
    "sourcei": {
        "root": "/data/fiftyone/sourcei",
        "dataset": "sourcei",
        "prompt_dir": "/data/fiftyone/sourceh/prompts",   # 뱅크 npz 공유
        "class_names": {0: "normal", 1: "falldown", 2: "fire", 3: "smoke"},
        "map_yaml": None,
        # 원장 = sourcei_build.py `kind_of()` 가 내는 4값. 위 프로필과 달리 **단일값이 아니다**
        # — 근거 강도 순서(folder > filename > caption)를 값으로 남기는 설계라 한 원장 안에
        # 정상적으로 섞여 있다. 'none' 은 문자열이지 None 이 아니다(캡션 없음 = 근거 없음).
        # ⚠️ 'caption' 은 Gemini 파생 = **모델 라벨**이다. 여기서 통과시키는 건 그게 이 원장의
        #    설계값이라서일 뿐, 학습/eval GT 로 승격해도 된다는 뜻이 아니다 (위 FP 스트레스
        #    테스트 주석 + CLAUDE.md "자기학습 금지" 참조).
        # ⚠️ 비대칭 주의: sourcei_build.py 는 (frames_eval.py / frames_bank_ledger.py 와 달리)
        #    쓰기 쪽 assert_gt_source_pure 가드가 **없다** — 이 읽기 쪽 체크가 유일한 방어선이다.
        "expected_gt_source": frozenset({"folder", "filename", "caption", "none"}),
        "group_field": "camera", "group_plural": "cams", "group_unit": "대",
        "key_join": "filepath_tail", "embed_field": None,
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


# 비교 대상 뱅크 버전 — env 로 파라미터화 (새 버전이 나오면 BANK_A/BANK_B 만 바꿔 재실행).
# 다중 버전(2026-08-11, 사용자 요청 "userwatch 전 버전"): BANK_LIST="v1,v2,..." 가 있으면
# 그 전체가 VERSIONS 가 된다 — wave/attach/promptmap 은 원래 BANKS 루프라 그대로 동작.
# ⚠️ 순서가 gidx 오프셋(GIDX_OFFSET × 순번)을 정하므로 재실행 간에 순서를 바꾸면 안 된다.
if os.environ.get("BANK_LIST"):
    VERSIONS = tuple(v.strip() for v in os.environ["BANK_LIST"].split(",") if v.strip())
else:
    VERSIONS = (os.environ.get("BANK_A", "v1.0.8.0"), os.environ.get("BANK_B", "v1.0.8.4"))
V0, V4 = VERSIONS[0], VERSIONS[-1]
# 단일뱅크 모드 — `BANK_A == BANK_B` 로 돌리면(예: sourcei 는 v1.0.8.0 만) 뱅크를 훑는
# 스테이지가 같은 일을 두 번 하고 promptmap 은 문장이 두 배로 들어간다. 여기서 한 번만 접는다.
BANKS = tuple(dict.fromkeys(VERSIONS))
CLASS_NAMES = {0: "normal", 1: "falldown", 2: "fire", 3: "smoke"}

# 다중 뱅크를 한 `<dataset>-prompts` 에 실을 때의 gidx 전역 유일성 (2026-08-11):
#   FiftyOne gidx = BANKS.index(version) * GIDX_OFFSET + 뱅크-로컬 인덱스.
# user-prompt-compare 패널이 gidx 를 전역 유일 id 로 쓰고(클릭 ids·row_of 딕셔너리·selftest
# 불변식) 프레임 필드 `winner_gidx_<tag>` 와 등식 조인하므로, promptmap 과 attach 가 같은
# 오프셋을 써야 한다. 첫 버전(BANK_A)은 오프셋 0 — 기존 v080 산출물과 바이트 동일.
# 뱅크-로컬 인덱스(prune CSV·geometry 내부)와의 환산은 gidx % GIDX_OFFSET.
GIDX_OFFSET = 100_000

# ── 판정규칙 ────────────────────────────────────────────────────────────────
# 제품 APO 는 **전역 top-K 문장의 클래스 다수결**로 판정한다 (`bank_vote_stream` 참고).
# argmax 는 그 규칙의 K=1 특수해다 — 예전 스테이지들이 전부 K=1 을 쓰고 있었다.
# RULE=argmax 로 두면 옛 동작 그대로라 회귀 비교가 가능하다.
RULE = os.environ.get("RULE", "topk").lower()
RULE_K = 1 if RULE == "argmax" else int(os.environ.get("RULE_K", "10"))
EVENT_CLASSES = (1, 2, 3)
SEEDS = 10

# ── 판정규칙 2: 분포 IoU (wave) ─────────────────────────────────────────────
# 제품 `pe_inference/01_TuningFree_v2.py` 가 실제로 쓰는 규칙. top-k 와 근본적으로 다르다:
# 문장 하나를 고르는 게 아니라 **클래스별 코사인 분포의 모양**을 normal 분포와 비교한다.
#   프레임마다 cos(이미지, 전체 문장) → 클래스별 히스토그램(전 클래스 공통 edges, 비율 정규화)
#   → IoU(normal, event) = Σmin/Σmax → IoU < WAVE_THR 이면 그 이벤트 후보.
# 기본값은 pe_inference README 의 권장 실행값(`--iou_mode hist --iou_hist_bins 80
# --iou_threshold 0.15`) 을 그대로 따른다.
WAVE_BINS = int(os.environ.get("WAVE_BINS", "80"))
WAVE_THR = float(os.environ.get("WAVE_THR", "0.15"))


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def vtag(version: str) -> str:
    """v1.0.8.4 → v1084 — `margin_*`/`winner_*` 필드 접미사.

    기존 코드가 ("v080","v084") 를 하드코딩하고 있었다. BANK_A/BANK_B 를 바꿔 재실행하면
    새 버전 값이 옛 이름 필드에 덮여 조용히 거짓말을 한다 → 버전에서 파생한다.

    2026-08-11 전 파트 조인으로 변경: 마지막 3파트만 쓰면 v1.0.5.0/v2.0.5.0 이 같은
    "v050", v5.0.15.0/v6.0.15.0/v8.0.15.0 이 같은 "v0150" 으로 붕괴한다 (userwatch 52버전
    적재에서 실충돌). 전 파트 조인은 54버전 전수 유일 확인. 기존 v080/v084 필드는
    v1080/v1084 로 재백필된다 (패널 version_to_winner_field 와 동기)."""
    return "v" + "".join(version.lstrip("vV").split("."))


# ── top-K 순위 사다리 필드 (2026-08-18) ─────────────────────────────────────
# 1위는 기존 `winner_gidx_<vtag>` / `top_prompt_<vt>` 가 그대로 담당한다(계약 불변).
# 여기서 추가하는 건 **2·3위뿐**이다 — 프레임당 K=10 ListField 전개는 기각됐다
# (스펙 §1-2: flat 스키마가 곧 App 필터. 프레임당 10칸이면 필터가 뱅크 수 ×10 으로 는다).
#
# 명명은 형제 필드의 접미사 세대를 **그대로 승계**한다 (스펙 §1-4 명명 부채를 늘리지 않는다):
#   gidx 계열 → vtag   `winner_gidx_r2_v1084`
#   문장 계열 → vt     `top_prompt_r2_v1_0_8_4`
# D7 리졸버 정규식 `^(?P<fam>.+?)_(?P<tag>v[\d_]+...)$` 로 파싱하면 fam 이
# `winner_gidx_r2` / `top_prompt_r2` 라는 **별 계열**로 떨어져 기존 계열과 안 섞인다
# (`winner_gidx_v1084_r2` 처럼 태그 뒤에 붙이면 fam 파싱이 깨진다 — 그래서 앞에 붙인다).
RANK_EXTRA = (2, 3)                       # 노출할 하위 순위. 1위는 기존 필드 담당
RANK_FIELD_RE = re.compile(r"^(?:winner_gidx|top_prompt)_r\d+_v[\d_]+(?:-[\w]+)?$")


def rank_gidx_field(tag: str, rank: int) -> str:
    """r위 문장의 gidx 필드명 (tag = `vtag(version)`). rank 는 1-기반."""
    return f"winner_gidx_r{rank}_{tag}"


def rank_prompt_field(vt: str, rank: int) -> str:
    """r위 문장의 원문 필드명 (vt = `version.replace('.', '_')`). rank 는 1-기반."""
    return f"top_prompt_r{rank}_{vt}"


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


def assert_gt_source_pure(rows, context: str) -> None:
    """이 프로필 원장의 `gt_source` 순도 — 다른 계열 원장이 섞이면 fail-closed.

    원장 3벌은 GT 출처가 서로 다르고, 각 프로필은 자기 것 하나만 읽어야 한다:
        sourceh    ← frames_eval.py         `nas_folder`   (NAS 폴더명 = 사람 재라벨)
        frames  ← frames_bank_ledger.py  `ls_finalized` (Label Studio 사람 검수)
        sourcei ← sourcei_build.py       `folder|filename|caption|none` (근거 강도 순)
    계열이 섞이면 뱅크 버전 비교·recall 이 통째로 무의미해지는데 **숫자는 멀쩡히 나온다** —
    조용한 오염이라 산출물만 봐서는 못 잡는다. analysis 는 CI pytest 밖이라 실행 시점
    self-check 가 유일한 방어선이다 (`frames_eval.py` / `frames_bank_ledger.py` 의 동명
    함수와 같은 계약 — 세 파일이 같은 모양으로 읽히게 맞춰 뒀다).

    `None` 은 이 필드가 생기기 전 구 원장 행이라 **항상** 허용한다(하위호환). 반면 sourcei 의
    `'none'` 은 문자열 값이고 그 원장의 정상 값이다 — 둘을 헷갈리지 말 것.
    """
    allowed = PROFILES[PROFILE]["expected_gt_source"]
    bad = collections.Counter(
        r.get("gt_source") for r in rows
        if r.get("gt_source") is not None and r.get("gt_source") not in allowed
    )
    if bad:
        raise RuntimeError(
            f"{context}: gt_source 혼재 감지 — 프로필 '{PROFILE}' 원장({WORK}/ledger.jsonl)에 "
            f"{sorted(allowed)} / None 이외의 값이 섞였다: {dict(bad)}. "
            "원장 계열 혼용 금지 — frames_eval.py=nas_folder / "
            "frames_bank_ledger.py=ls_finalized / sourcei_build.py=folder·filename·caption·none."
        )


def load_all():
    led = jsonl_load(f"{WORK}/ledger.jsonl")
    # GT 깔때기 — 이 파일의 모든 스테이지가 여기서 gt 를 받는다. 조립 **전에** 순도를 막는다.
    assert_gt_source_pure(led.values(), context=f"load_all[{PROFILE}]")
    d = np.load(f"{WORK}/embed.npz", allow_pickle=True)
    keys = [str(k) for k in d["key"]]
    mask = [k in led for k in keys]
    keys = [k for k, m in zip(keys, mask) if m]
    X = d["vec"][np.array(mask)].astype(np.float32)
    X /= np.linalg.norm(X, axis=1, keepdims=True)
    gt = np.array([led[k]["gt_class"] for k in keys], dtype=np.int64)
    src = np.array([led[k]["src_video"] for k in keys])
    banks = {v: load_bank(v) for v in VERSIONS}
    for v, b in banks.items():
        # gidx 블록 = 뱅크당 GIDX_OFFSET. 넘치면 크래시가 아니라 **다른 버전 문장으로의
        # 조용한 오귀속**이다 (gidx % GIDX_OFFSET 이 이웃 블록을 가리킴). 2026-08-19 실측
        # 최대 뱅크 79,842/100,000 = 80% 소진, 추세 가파름 — 넘는 날 여기서 시끄럽게 멈춘다.
        # 근본 해결(GIDX_OFFSET 증설 + 전량 재백필)은 별건.
        if len(b["cls"]) > GIDX_OFFSET:
            raise SystemExit(f"뱅크 {v} 문장 {len(b['cls']):,} > GIDX_OFFSET {GIDX_OFFSET:,} — "
                             "gidx 블록 충돌(조용한 버전 오귀속). GIDX_OFFSET 증설+재백필 필요")
    return keys, X, gt, src, banks


def load_bank(version: str) -> dict:
    """뱅크 1개 → `{vec, cls, prompt}`. **벡터·클래스는 npz, 문장은 DB 정본.**

    npz 는 벡터의 정본이지 문장의 정본이 아니다 — 2026-08-11 재빌드가 27버전의 문장을
    자리표시자로 덮은 사고가 그 구분이 없어서 났다. 판정 규칙과 폴백은 형제 모듈
    `repair_bank_prompts.load_bank` 한 곳에만 있다 (소비자 셋이 같은 규칙을 쓰도록).
    """
    import repair_bank_prompts as _bank          # 같은 디렉토리 형제 모듈
    return _bank.load_bank(version, PROMPT_DIR)


def load_groups(keys: list[str]) -> np.ndarray:
    """프레임별 **사이트 그룹** (sourceh=camera / frames=project). 뱅크 지표를 사이트 층화로
    재려면 필수다 — pooled 값은 프레임이 가장 많은 그룹이 지배한다(실측: area-a 7,979
    vs ODC 1,144, pooled 게이트를 통과한 후보의 held 카메라 FN 구조율이 6케이스 전부 0.0%.
    frames 는 편중이 더 크다: cohort-b 73,390 vs violence 144).

    축 이름을 프로필이 정하므로 source-h 의 카메라 코드와 frames 의 project 코드가 **한 벌**이다.
    """
    fld = PROFILES[PROFILE]["group_field"]
    led = jsonl_load(f"{WORK}/ledger.jsonl")
    return np.array([led[k].get(fld) or "unknown" for k in keys])


def load_cameras(keys: list[str]) -> np.ndarray:
    """`load_groups` 의 구 이름 — sourceh/sourcei 스테이지가 부른다 (그 프로필의 축이 camera)."""
    return load_groups(keys)


FRAMES_EMBED_CHUNK = int(os.environ.get("FRAMES_EMBED_CHUNK", "2000"))


def _stream_frames_embeddings(keys: list[str]) -> tuple[list[str], np.ndarray]:
    """FiftyOne `image_embedding` 을 청크로 읽어 [N, d] fp32 로 채운다.

    ⚠️ `view.values("image_embedding")` 을 20만 건에 한 번에 걸면 안 된다 — ListField 라
       원소마다 파이썬 float 객체가 나서 청크 없이는 수 GB 가 순간에 뜬다(호스트 가용 11Gi
       공유, OOM 이력). 청크 2,000 이면 파이썬 측 상주가 ~65MB 로 눌린다.
    반환 배열은 **정규화 전**이다 (호출부가 `load_all()` 과 같은 자리에서 정규화한다).
    """
    import fiftyone as fo

    fld = PROFILES[PROFILE]["embed_field"]
    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    live = set(ds.values("id"))
    n_req = len(keys)
    keys = [k for k in keys if k in live]         # 원장에만 있고 데이터셋에서 지워진 행 방어
    if n_req and not keys:
        # 요청 키가 전부 데이터셋에 없다 = 원장이 앞서 있는 상태. 캐시 병합 경로에서 정상 가능하므로
        # 여기서 죽이지 않고 빈 결과를 돌려준다 (호출부가 개수로 판단한다).
        log(f"embed 스트리밍: 요청 {n_req:,}건이 데이터셋에 없다 → 0건 반환 (원장 재생성 필요?)")
        return [], np.zeros((0, 0), dtype=np.float32)
    X = None
    kept: list[str] = []
    n = 0
    t0 = time.time()
    for s in range(0, len(keys), FRAMES_EMBED_CHUNK):
        chunk = keys[s:s + FRAMES_EMBED_CHUNK]
        vecs = ds.select(chunk, ordered=True).values(fld)
        for k, v in zip(chunk, vecs):
            if v is None or len(v) == 0:
                continue                          # 임베딩 결손 — 조용히 0벡터로 채우면 안 된다
            if X is None:
                X = np.empty((len(keys), len(v)), dtype=np.float32)
            X[n] = v
            kept.append(k)
            n += 1
        if (s // FRAMES_EMBED_CHUNK) % 20 == 0:
            log(f"embed 스트리밍 {min(s + FRAMES_EMBED_CHUNK, len(keys)):,}/{len(keys):,} "
                f"({time.time() - t0:.0f}s)")
    if X is None:
        raise SystemExit(f"{PROFILES[PROFILE]['dataset']}: {fld} 가 전부 비었다 — "
                         "임베딩 파이프라인 먼저 확인")
    if n < len(keys):
        log(f"embed 스트리밍: {fld} 결손 {len(keys) - n:,}건 제외")
        X = np.ascontiguousarray(X[:n])
    log(f"embed 스트리밍 완료: {X.shape} ({time.time() - t0:.0f}s)")
    return kept, X


def _load_frames_matrix() -> tuple[list[str], np.ndarray, np.ndarray, np.ndarray]:
    """frames 프로필의 (keys, X, gt, src) — `load_all()` 의 frames 판.

    ⚠️ **`load_all()` 을 그대로 쓸 수 없는 이유**: frames 의 `work/embed.npz` 는
       `frames_bank_ledger.py` 가 **도메인 매핑된 프레임만** 담는 산출물이고
       (`scored = [r for r in rows if r["domain"]]`), `bank_domain_map.yaml` 의 domains 가
       비어 있는 0단계에서는 **파일 자체가 없다** (2026-08-18 실측: work/ 에 ledger.jsonl 과
       gt_snapshot.json 만 존재). attach/site 는 도메인 매핑과 무관하게 전 프레임에 걸려야
       하므로, npz 가 있으면 쓰고 없으면 데이터셋에서 스트리밍한다.
       또 `load_all()` 은 VERSIONS 의 뱅크를 **전부** 올리는데(BANK_LIST 면 52벌),
       attach/site 는 뱅크 1벌만 쓴다 — 여기서는 뱅크를 아예 안 읽는다.

    `FRAMES_EMBED_CACHE=<path>` 를 주면 그 경로를 캐시로 쓴다(있으면 읽고, 없으면 스트리밍 후
    기록). 기본값은 캐시 없음 — 원장이 소유한 `work/embed.npz` 는 **절대 덮어쓰지 않는다.**

    ⚠️ **캐시는 원장의 부분집합일 수 있다 — 그 차이를 조용히 삼키면 안 된다.**
       (a) `work/embed.npz` 는 애초에 도메인 매핑분만 담는 설계이고,
       (b) 캐시를 만든 뒤 프레임이 새로 들어오면(원장 20만 / 캐시 18만) 교집합만 취하는 구현은
           **2만 장을 검색조차 안 하고 누락**시킨다. 로그도 "교집합 18만/18만" 이라 정상처럼 보인다.
       그래서 로드 직후 원장과 대조해 **미캐시 키만 추가 스트리밍**해 병합하고, 캐시 경로가
       주어졌으면 전량으로 재작성한다. 캐시 npz 에 `n_ledger`/`created_at` 메타를 같이 실어
       stale 여부를 파일만 보고도 알 수 있게 한다.
    """
    rows = _load_frames_ledger()                  # gt_source 순도 게이트 (load_all 과 동일 계약)
    led = {r["key"]: r for r in rows}
    order = [r["key"] for r in rows]              # 원장 순서 = 정본 커버리지 목록
    cache = os.environ.get("FRAMES_EMBED_CACHE")
    src_path = cache if (cache and os.path.exists(cache)) else f"{WORK}/embed.npz"
    if os.path.exists(src_path):
        d = np.load(src_path, allow_pickle=True)
        ks = [str(k) for k in d["key"]]
        m = np.array([k in led for k in ks], dtype=bool)
        keys = [k for k, x in zip(ks, m) if x]
        X = d["vec"][m].astype(np.float32)
        meta = (f" [캐시메타 n_ledger={int(d['n_ledger'])} @{str(d['created_at'])}]"
                if "n_ledger" in d.files else " [캐시메타 없음 — 구 캐시]")
        stale_drop = len(ks) - len(keys)
        log(f"frames: 임베딩 {src_path} → {X.shape} "
            f"(원장 교집합 {len(keys):,}/{len(ks):,}, 원장 {len(order):,}){meta}")
        if stale_drop:
            log(f"frames: 캐시에만 있고 원장에 없는 키 {stale_drop:,}건 제외 (원장이 축소됐다)")
        have = set(keys)
        missing = [k for k in order if k not in have]
        if missing:
            # 조용히 진행 금지 — 누락분을 실제로 채운다. 못 채우면 그것도 로그로 드러난다.
            log(f"frames: ⚠️ 캐시 미포함 {len(missing):,}건 발견 ({len(missing) / len(order):.1%}) "
                "— 그만큼만 추가 스트리밍해 병합한다 (교집합만 쓰면 무음 누락이 된다)")
            add_keys, add_X = _stream_frames_embeddings(missing)
            if add_X.shape[0]:
                if add_X.shape[1] != X.shape[1]:
                    raise SystemExit(f"frames: 캐시 차원 {X.shape[1]} != 데이터셋 차원 "
                                     f"{add_X.shape[1]} — 인코더가 바뀌었다. 캐시를 지우고 재실행")
                keys = keys + add_keys
                X = np.concatenate([X, add_X], axis=0)
                if cache:
                    np.savez(cache, key=np.array(keys), vec=X, n_ledger=len(order),
                             created_at=time.strftime("%Y-%m-%dT%H:%M:%S"))
                    log(f"frames: 캐시 재작성 → {cache} ({len(keys):,}장)")
            gap = len(order) - len(keys)
            log(f"frames: 병합 후 {len(keys):,}장 / 원장 {len(order):,} "
                f"({len(keys) / len(order):.1%})"
                + (f" — 여전히 {gap:,}건 미확보(데이터셋 부재 또는 임베딩 결손)" if gap else " — 전량 확보"))
    else:
        keys, X = _stream_frames_embeddings(order)
        if cache:
            np.savez(cache, key=np.array(keys), vec=X,
                     n_ledger=len(order), created_at=time.strftime("%Y-%m-%dT%H:%M:%S"))
            log(f"frames: 임베딩 캐시 기록 → {cache}")
    nrm = np.linalg.norm(X, axis=1, keepdims=True)
    bad = (nrm[:, 0] <= 0)
    if bad.any():                                 # 0벡터를 나누면 NaN 이 조용히 전 지표에 번진다
        log(f"frames: ⚠️ 0-노름 임베딩 {int(bad.sum()):,}건 제외")
        keep = ~bad
        X, nrm = X[keep], nrm[keep]
        keys = [k for k, x in zip(keys, keep) if x]
    X = X / nrm
    gt = np.array([led[k].get("gt_class", -1) for k in keys], dtype=np.int64)
    src = np.array([led[k].get("src_video") or "unknown" for k in keys])
    return keys, X, gt, src


def load_matched() -> tuple[list[str], np.ndarray, np.ndarray, np.ndarray]:
    """프로필 무관 (keys, X, gt, src) 입구 — 뱅크 1벌만 쓰는 스테이지(attach/site)용.

    sourceh/sourcei 는 `load_all()` 그대로(뱅크 로드 포함, 기존 동작 보존), frames 만 위 전용
    로더를 탄다. 두 경로 모두 원장 순도 게이트를 지난다.
    """
    if PROFILE == "frames":
        return _load_frames_matrix()
    keys, X, gt, src, _ = load_all()
    return keys, X, gt, src


SET_VALUES_BATCH = int(os.environ.get("SET_VALUES_BATCH", "20000"))
# `active_fields` 는 **데이터셋 전역 App 설정**이라 다른 사람 화면까지 바꾼다. `frames`
# (구 frames_captions) 는 2026-08-18 실측에서 `active_fields=None`(제한 없음) 이었다.
KEEP_ACTIVE_FIELDS = os.environ.get("KEEP_ACTIVE_FIELDS", "").lower() in ("1", "true", "yes")
SET_ACTIVE_FIELDS = os.environ.get("SET_ACTIVE_FIELDS", "").lower() in ("1", "true", "yes")


def set_active_fields(ds, active_fields_cls, want: list) -> list:
    """`app_config.active_fields` 갱신 — **넓히기만 하고, 없던 제한을 새로 만들지 않는다.**

    세 갈래다:
      · 기존이 리스트(제한 있음)  → `want` 를 얹은 **합집합**. 남이 보던 필드가 안 사라진다.
      · 기존이 None(제한 없음)    → **그대로 둔다** (기본). `SET_ACTIVE_FIELDS=1` 일 때만 신설.
      · `KEEP_ACTIVE_FIELDS=1`    → 어느 쪽이든 손대지 않는다.

    ⚠️ **왜 None 을 기본으로 보존하나** — "allowlist 밖 필드로 Color by 하면 App 이 TypeError 로
       죽는다"는 실측(계획서 §2)은 **allowlist 가 존재할 때만** 성립하는 문제다. `active_fields`
       가 None 이면 allowlist 라는 개념 자체가 없어 어떤 필드로 칠해도 크래시 위험이 없다.
       반대로 None 인 데이터셋에 allowlist 를 처음 세우면 그 순간 **그동안 보이던 모든 필드가
       그리드에서 사라지고**(`frames` 는 공유 데이터셋이다) 목록 밖 필드는 Color by 가
       크래시하게 된다 — 즉 기본 동작이 위험을 *만들어내는* 쪽이 된다. 위험한 쪽을 기본값으로
       두지 않는다.
    """
    cur_paths = getattr(ds.app_config.active_fields, "paths", None)
    cur = list(cur_paths or [])
    merged = [f for f in dict.fromkeys(cur + want) if f in ds.get_field_schema()]
    if KEEP_ACTIVE_FIELDS:
        log(f"active_fields: KEEP_ACTIVE_FIELDS=1 — 갱신 생략 "
            f"(현재 {cur_paths if cur_paths else 'None(제한 없음)'} / 제안 {merged})")
        return cur
    if not cur_paths:                            # 제한 없음(None/빈 목록) = 신설 여부는 opt-in
        if not SET_ACTIVE_FIELDS:
            log("active_fields: 현재 제한 없음(None) — **그대로 둔다**. 무제한이면 Color by "
                f"크래시 위험도 없다. allowlist 를 세우려면 SET_ACTIVE_FIELDS=1 (제안 {merged})")
            return cur
        log(f"active_fields: SET_ACTIVE_FIELDS=1 — 무제한 → allowlist 신설 {merged} "
            "(목록 밖 필드는 그리드에서 사라지고 Color by 대상에서 제외된다)")
    ds.app_config.active_fields = active_fields_cls(paths=merged, exclude=False)
    return merged


def set_values_batched(ds, field: str, pairs: list, make, batch: int | None = None) -> None:
    """`{sample_id: 값}` 을 통째로 만들지 않고 배치로 나눠 쓴다.

    `pairs` = [(sample_id, 프레임인덱스), ...], `make(프레임인덱스)` = 그 자리의 값.
    source-h 13k 에서는 한 번에 만들어도 됐지만 frames 199,972 × `fo.Classification` 은 dict
    하나가 수백 MB 로 뜬다 (호스트 가용 11Gi 공유 + OOM 이력 — 계획서 §4 운영 제약).
    값이 dict 안에서 **동시에 살아 있는 수**를 배치 크기로 묶는 게 요점이다.
    """
    b = batch or SET_VALUES_BATCH
    for s in range(0, len(pairs), b):
        ds.set_values(field, {sid: make(i) for sid, i in pairs[s:s + b]}, key_field="id")


def key_to_ids(ds, keys: list[str]) -> list:
    """원장 key → FiftyOne sample id. 조인 방식은 프로필이 정한다 (`key_join`).

    ⚠️ frames 원장의 key 는 **이미 sample id** 다 (frames_bank_ledger.py 가 `"key": sid`).
       파일명 조인을 그대로 쓰면 전건 미매칭(필드 0개 기록)인데 로그만 보면 정상처럼 보인다.
    """
    if PROFILES[PROFILE]["key_join"] == "sample_id":
        live = set(ds.values("id"))
        return [k if k in live else None for k in keys]
    k2i = {}
    for s in ds.select_fields(["id", "filepath"]):
        k2i[f"{os.path.basename(os.path.dirname(s.filepath))}/"
            f"{os.path.basename(s.filepath)}"] = s.id
    return [k2i.get(k) for k in keys]


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


def bank_topk_stream(X: np.ndarray, bank: dict, k: int = None, drop: np.ndarray | None = None,
                     batch: int = 1024, block: int = 2048) -> tuple[dict, dict]:
    """클래스별 per-frame 상위 (k+1) cosine + 그 문장들의 **클래스-로컬** 인덱스.

    `bank_top2_stream` 의 일반화다. k=1 이면 (1위,2위) 라 옛 함수와 같은 정보를 준다.
    **왜 k 가 아니라 k+1 인가** — LOO 는 "top-k 안의 문장 하나를 지우면 무엇이 들어오나"를
    물으므로 항상 한 칸 여유가 필요하다. 그 한 칸이 없으면 제거 후 표를 못 센다.

    `drop`(뱅크 전역 bool)을 주면 그 문장을 뺀 상태로 계산한다 — 탐욕 그룹제거의 라운드 재적합용.
    인덱스는 drop 이후에도 **원본 뱅크 기준 클래스-로컬** 번호라 문장 정체성이 유지된다.
    메모리: 클래스당 [N, k+1] 두 벌뿐 (N=13k·k=10 이면 4.6MB) — 유사도 행렬 미상주.
    """
    k = RULE_K if k is None else k
    w = k + 1
    classes = sorted(set(bank["cls"].tolist()))
    n = X.shape[0]
    vals = {c: np.full((n, w), -2.0, dtype=np.float32) for c in classes}
    idxs = {c: np.full((n, w), -1, dtype=np.int64) for c in classes}
    for c in classes:
        gidx = np.flatnonzero(bank["cls"] == c)
        local = np.arange(len(gidx)) if drop is None else np.flatnonzero(~drop[gidx])
        if len(local) == 0:
            continue                       # 클래스가 통째로 비면 −2 유지 = 절대 안 이김
        V = bank["vec"][gidx[local]]
        for q in range(0, V.shape[0], block):
            Vb = V[q:q + block]
            for s in range(0, n, batch):
                S = X[s:s + batch] @ Vb.T
                lab = local[q:q + Vb.shape[0]]
                # 기존 상위 w 와 이번 타일을 합쳐 다시 상위 w — 병합이 exact 인 표준 논증
                cand_v = np.concatenate([vals[c][s:s + batch], S], axis=1)
                cand_i = np.concatenate(
                    [idxs[c][s:s + batch], np.broadcast_to(lab, S.shape)], axis=1)
                if cand_v.shape[1] > w:
                    part = np.argpartition(-cand_v, w - 1, axis=1)[:, :w]
                    cand_v = np.take_along_axis(cand_v, part, 1)
                    cand_i = np.take_along_axis(cand_i, part, 1)
                # kind="stable" — float32 코사인 동점에서 문장 정체성이 실행마다 흔들리면
                # 순위 사다리(winner_gidx_r2/r3)가 재현되지 않는다. 같은 파일 stage_attach 가
                # "argsort 는 unstable quicksort 라 동점에서 [:,1] 이 승자와 같아질 수 있다"고
                # 적어둔 것과 같은 함정이다.
                # **pred 는 안 바뀐다**: 여기 argsort 는 집합이 아니라 순서만 정한다(집합은 위
                # argpartition, 혹은 폭이 w 이하면 전량 유지). 남는 `vals[c]` 값의 다중집합이
                # 같으므로 vote_topk 의 `allv`·클래스 라벨이 동일하고, votes(합)·topc(최댓값)도
                # 동일하다. 동점에서 `idxs[c]` 의 정체성만 결정적으로 고정된다.
                # ⚠️ 남는 비결정성 1건: 위 `argpartition` 은 w 경계 동점 중 **누구를 남길지**를
                #    보장하지 않는다(값은 같고 정체성만 흔들린다). 전면 안정화는 타일마다 full
                #    sort 라 200K 프레임에서 감당이 안 돼 받아들인다 — 그 경계는 클래스 안
                #    w번째(기본 11위)라 노출 대상인 1~3위에 닿지 않는다.
                o = np.argsort(-cand_v, axis=1, kind="stable")
                vals[c][s:s + batch] = np.take_along_axis(cand_v, o, 1)[:, :w]
                idxs[c][s:s + batch] = np.take_along_axis(cand_i, o, 1)[:, :w]
    return vals, idxs


def vote_topk(vals: dict, idxs: dict, k: int = None,
              exclude: set | None = None) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """전역 top-k 다수결 — `bank_vote_stream` 과 동일 규칙 (동표는 클래스 최고 코사인).

    `exclude` 에 (class, local_idx) 를 넣으면 그 문장이 없는 것처럼 계산한다 — LOO 용.
    반환 (pred, votes[N,C], sel_idx[N,k]) — sel_idx 는 표를 만든 문장의 클래스-로컬 번호
    (-1 은 빈자리). 이게 argmax 시절의 `arg`(승자 1개)를 대체한다.

    ── 동점 처리 2단 (2026-08-18) ─────────────────────────────────────────
    ① **선택 집합**을 정하는 `o` 는 기본 kind 그대로 둔다. `kind="stable"` 로 바꾸면 동점이
       top-k 경계에 걸린 프레임에서 **집합이 달라져 pred 가 바뀐다** (실측: 동점 주입
       200프레임 중 28프레임에서 pred·votes 불일치). 판정 재현은 이 개작의 범위 밖이므로
       기존 동작을 유지한다.
    ② **사다리 순서**만 `lexsort` 로 다시 세운다 — 1차 −코사인, 2차 원래 열번호
       (= 클래스 오름차순 → 그 클래스 안 상위 슬롯). 집합이 그대로이고 `votes`(합)·
       `topc`(최댓값)는 열 순서에 불변이므로 **pred 는 바이트 단위로 안 바뀐다.**
       얻는 것: 동점에서 `winner_gidx_r2/r3_*` 가 quicksort 의 임의 순서에 안 흔들린다.
       동점 규칙 방향은 아래 `argmax` 타이브레이크(낮은 클래스 id 우선)와 같다.
    """
    k = RULE_K if k is None else k
    cs = sorted(vals)
    keep_v, keep_i, keep_c = [], [], []
    for c in cs:
        v, i = vals[c].copy(), idxs[c]
        if exclude:
            for (ec, ei) in exclude:
                if ec == c:
                    v = np.where(i == ei, -2.0, v)
        keep_v.append(v); keep_i.append(i)
        keep_c.append(np.full(v.shape[1], c))
    allv = np.concatenate(keep_v, 1)
    alli = np.concatenate(keep_i, 1)
    lab = np.concatenate(keep_c)
    kg = min(k, allv.shape[1])
    o = np.argsort(-allv, axis=1)[:, :kg]                  # ① 집합 — 기존 동작 유지
    ro = np.lexsort((o, -np.take_along_axis(allv, o, 1)), axis=1)   # ② 사다리 순서만 결정적으로
    o = np.take_along_axis(o, ro, 1)
    sel_c = lab[o]
    sel_v = np.take_along_axis(allv, o, 1)
    sel_i = np.take_along_axis(alli, o, 1)
    live = sel_v > -1.9                                    # 빈자리 제외
    votes = np.stack([((sel_c == c) & live).sum(1) for c in cs], 1)
    topc = np.stack([np.where((sel_c == c) & live, sel_v, -2.0).max(1) for c in cs], 1)
    pred = np.array(cs)[(votes + (topc + 2.0) / 10.0).argmax(1)]
    sel_i = np.where(live, sel_i, -1)
    return pred, votes, np.stack([sel_c, sel_i], -1)


def predict_rule(X: np.ndarray, bank: dict, drop: np.ndarray | None = None) -> np.ndarray:
    """현재 판정규칙(RULE/RULE_K)으로 프레임 예측. 스테이지가 규칙을 몰라도 되게 감싼다."""
    vals, idxs = bank_topk_stream(X, bank, drop=drop)
    return vote_topk(vals, idxs)[0]


def bank_reach_stream(X: np.ndarray, bank: dict, best: dict[int, np.ndarray],
                      groups: np.ndarray | None = None,
                      batch: int = 2048, block: int = 2048) -> tuple[np.ndarray, dict]:
    """문장별 **reach** = maxᵢ( cos(p,i) − others_best(i) ) — "이 문장이 어디서든 이길 수 있나".

    승수(wins)는 실제로 1위를 한 횟수라 뱅크의 97~98% 가 0이 되어 순위가 죽는다. reach 는
    "얼마나 모자랐나"까지 연속으로 재므로 비승자 사이의 서열이 살아난다. 실측(v1.0.8.4):
    실제 승자 319개인데 reach>0 은 4,312개(26.7%), 완전 불활성(reach<−0.10)은 0개 —
    비승자는 중복도 죽은 문장도 아니고 **같은 클래스 팀메이트에게 지는 예비군**이다.
    (사이트 전이 실측: 못 본 카메라에서 승자의 66% 가 학습 카메라 비승자였다.)

    `others_best(i)` = 그 프레임에서 **자기 클래스를 뺀** 최고 점수라 뱅크 간 가산 오프셋이
    상쇄된다 — 절대 코사인으로 재면 안 되는 이유와 같다(§13 cover_viz 폐기 참조).
    `groups`(프레임별 카메라 등)를 주면 그룹별 reach 도 같이 낸다 — 사이트 특이 문장 판정용.
    """
    classes = sorted(set(bank["cls"].tolist()))
    n = X.shape[0]
    keys = sorted(set(groups.tolist())) if groups is not None else []
    gmask = {k: (groups == k) for k in keys}
    reach = np.full(len(bank["cls"]), -np.inf, dtype=np.float32)
    reach_g = {k: np.full(len(bank["cls"]), -np.inf, dtype=np.float32) for k in keys}
    for c in classes:
        others = np.max(np.stack([best[o] for o in classes if o != c]), axis=0) \
            if len(classes) > 1 else np.zeros(n, dtype=np.float32)
        gidx = np.flatnonzero(bank["cls"] == c)
        V = bank["vec"][gidx]
        for q in range(0, V.shape[0], block):
            Vb = V[q:q + block]
            acc = np.full(Vb.shape[0], -np.inf, dtype=np.float32)
            accg = {k: np.full(Vb.shape[0], -np.inf, dtype=np.float32) for k in keys}
            for s in range(0, n, batch):
                D = X[s:s + batch] @ Vb.T - others[s:s + batch, None]   # [batch, block]
                np.maximum(acc, D.max(axis=0), out=acc)
                for k in keys:
                    m = gmask[k][s:s + batch]
                    if m.any():
                        np.maximum(accg[k], D[m].max(axis=0), out=accg[k])
            reach[gidx[q:q + block]] = acc
            for k in keys:
                reach_g[k][gidx[q:q + block]] = accg[k]
    return reach, reach_g


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


def event_tier(n: int) -> str:
    """이벤트 표본 수 → 주장 가능 tier. 경계는 `minn_tier` 와 같지만 분모가 **GT 가 아니라
    규칙 예측**이라 0 칸 이름만 바꾼다 — `no_gt` 라고 부르면 GT 얘기로 읽힌다.

    LOPO(사이트 전이)에서 "이벤트가 거의 없는 project 는 제외하거나 별도 tier"(계획서 §4)를
    **하드코딩 목록 없이** 처리하는 자리다. loc-c 계열이 그 후보로 지목돼 있지만 이름을
    박지 않는다 — 뱅크·규칙이 바뀌면 어느 project 가 비는지도 바뀐다.
    """
    return "no_event" if n == 0 else minn_tier(n)


def gt_tier(gt: np.ndarray | None) -> str:
    """`load_all()` 의 gt 배열 → min-n tier. GT 미조인/전무면 `"no_gt"`.

    ⚠️ **배열이 있다 ≠ GT 가 있다.** `load_all()` 은 원장 `gt_class` 를 필터 없이 싣는데
    (미검수 프레임은 −1), 나머지 코드는 `gt_class >= 0` 만 GT 로 친다(`stage_gtsync` 참조).
    전부 −1 이면 `pred == gt` 가 항상 거짓이라 정답수·순도·LOO 이득이 **전부 0 으로 조용히
    수렴**하는데, reach 랭킹(선택 산출물)은 GT 없이도 그럴듯하게 나온다. 그 조합이
    "선택 결과"를 "품질 판정"으로 오독하게 만드는 지점이라 tier 를 산출물 최상단에 박는다.
    """
    if gt is None:
        return "no_gt"
    arr = np.asarray(gt)
    if arr.size == 0:
        return "no_gt"
    return minn_tier(int((arr >= 0).sum()))


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
    # 자리표시자를 임베딩하면 그 문자열 자체가 자석이 된다 (§is_placeholder 주석)
    n_ph = sum(1 for r in rows if is_placeholder(r.get("prompt")))
    if n_ph:
        raise SystemExit(f"bank {version}: CSV 에 문장 없는 행이 {n_ph}/{len(rows)}개 "
                         f"({PLACEHOLDER_PREFIX}...) — 벡터전용 뱅크의 자리표시자다. "
                         "텍스트 원본 CSV 를 쓰라")
    out = f"{PROMPT_DIR}/{version}.npz"
    if os.path.exists(out):
        z = np.load(out, allow_pickle=True)
        # ⚠️ 행수만 비교하면 안 된다 — `bankfrom` 큐레이션은 **같은 개수·다른 문장** 버전을
        #    대량 생산하므로, 같은 버전명으로 재실행하면 새 문장을 임베딩하지 않고 옛 npz 를
        #    조용히 재사용한다 (2026-08-11 감사 지적). 문장 내용으로 비교한다.
        same = (list(z["prompt"]) == [r["prompt"] for r in rows]) if "prompt" in z \
            else (len(z["cls"]) == len(rows))
        if same:
            log(f"bank {version}: 이미 존재 (n={len(rows)}, 문장 동일) → skip")
            return
        log(f"bank {version}: 기존 npz 와 문장이 다르다 "
            f"(기존 {len(z['cls'])} / 신규 {len(rows)}) → 재임베딩")
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


# ────────────────────── bankfrom (큐레이션) ──────────────────────
def _norm_text(s: str) -> str:
    """원장(`prompt_bank_ledger.py:56`)과 **같은** 정규화 — 공백 접기 + 소문자."""
    return " ".join((s or "").split()).lower()


# 벡터전용 뱅크(텍스트 미보유)를 npz 로 흡수할 때 `prompt` 배열에 채워지는 자리표시자.
# 이걸 문장으로 착각하면 자리표시자 문자열을 임베딩한 **가짜 자석**이 뱅크에 들어가고, 점수는
# 그럴싸하게 나온다 — 그래서 문장을 쓰는 모든 입구에서 막는다.
#
# 2026-08-11 일괄 재빌드가 userwatch **JSON**(`"prompt": null` — 전 버전)만 읽고 옆의 CSV 를
# 보지 않아 npz 29버전 중 27개의 `prompt` 가 이걸로 덮였고 문장 데이터셋까지 전파됐다
# (벡터·cls 는 온전). 2026-08-18 `repair_bank_prompts.py --apply --sync` 로 **CSV 가 있는
# 19버전을 복구**했다. 남은 8버전(v1.0.2.0·v1.0.5.4~5.7·v1.0.13.x)은 원본에 문장이 없거나
# CSV 행수가 npz 와 달라 자리표시자가 **사실**이다.
# ⚠️ `#N` 의 N 은 공급자 `ID` 컬럼이지 행 번호가 아니다 (v1.0.8.0 은 12,480행에 ID 2,405종,
#    v1.0.6.2 는 16,125행 전부 ID=0 → 전부 `#0`). 자리표시자로 행을 역추적할 수 없다.
PLACEHOLDER_PREFIX = "(텍스트 없음"


def is_placeholder(s: str) -> bool:
    return str(s or "").lstrip().startswith(PLACEHOLDER_PREFIX)


def _bank_rows(texts: list, labels: list, cls_map: dict) -> tuple[list[tuple[int, str]], int]:
    """(class int, prompt) 행 + 중복탈락수. **fail-closed** — 모르는 라벨/빈 문장은 예외.

    조용한 기본값(`.get(label, 0)`)을 두지 않는 이유: normal(0) 로 흘러들어간 fire 문장은
    뱅크를 조용히 망치고 점수에는 그럴싸하게 나타난다.
    중복 판정은 (정규화 텍스트, class) — 텍스트만으로 접으면 같은 문장을 다른 클래스로
    쓰는 정당한 경우까지 삼킨다 (원장 content_hash 의 알려진 결함과 같은 함정).
    """
    rev = {v: k for k, v in cls_map.items()}
    rows, seen, dropped = [], set(), 0
    n_ph = sum(1 for t in texts if is_placeholder(t))
    if n_ph:
        raise SystemExit(
            f"bankfrom: 선택에 문장이 없는 표본이 {n_ph}/{len(texts)}개 있다 "
            f"({PLACEHOLDER_PREFIX}...). 그 뱅크 버전은 벡터만 있고 텍스트가 없다 — "
            "CSV 로 뽑으면 자리표시자를 임베딩한 가짜 자석이 만들어진다.\n"
            "  · 사이드바 `bank_version` 으로 텍스트가 있는 버전만 남기고 다시 고르거나,\n"
            "  · 원본 CSV 로 npz 를 복원한 뒤 `promptmap` 을 다시 돌려라 (예: "
            f"bank --csv {PROMPT_DIR}/text_features_<버전>.csv --version <버전>)")
    for t, lab in zip(texts, labels):
        if not (t or "").strip():
            raise SystemExit("bankfrom: 빈 text 표본이 선택에 포함됐다 — 태그를 확인하라")
        if lab not in rev:
            raise SystemExit(f"bankfrom: 모르는 클래스 라벨 {lab!r} — 허용: {sorted(rev)}")
        k = (_norm_text(t), rev[lab])
        if k in seen:
            dropped += 1
            continue
        seen.add(k)
        rows.append((rev[lab], t))
    return rows, dropped


def stage_bankfrom(tag: str, version: str, notes: str | None = None) -> None:
    """App 에서 **태그한 문장** → 뱅크 CSV + provenance + 원장 1행 (큐레이션 버전 확정).

    ## 왜 태그인가 (플러그인 operator 를 만들지 않은 이유)

    네이티브 Embeddings 라쏘는 `ctx.selected` 에 **들어오지 않는다** — `ctx.extended_selection`
    으로만 오고, 그걸 받아 뷰를 건드리면 선택이 스스로 사라진다 (prompt-compare 패널의
    `on_change_extended_selection` 실측 주석). 반면 라쏘는 그리드를 이미 좁히므로
    **그리드 전체선택 → 네이티브 tag 버튼**이면 선택 UI 코드가 0줄이고, 태그는 어느 선택
    경로에서 왔든 살아남는다. 게다가 클래스 이름↔int 사상이 이 파일의 `CLASS_NAMES` 하나로
    끝나 App 플러그인에 사상을 **두 번째로 하드코딩**하는 drift 위험이 없다.

    ## 흐름

        App: 사이드바 필터 or 라쏘 → 그리드 전체선택 → tag `bank:<version>`
        python prompt_geometry.py bankfrom --tag bank:<version> --version <version>
        python prompt_geometry.py bank --csv <위 CSV> --version <version>   # → npz

    ## 산출물

    · `PROMPT_DIR/authored_<version>.csv` — 정본 헤더 `ID,class,prompt` (`stage_bank` 입력).
      ⚠️ 파일명이 `authored_` 인 것은 필수다. `text_features_*` 로 쓰면
      `prompt_bank_ledger.py scan_roots()` 의 `VERSION_RE` 에 걸려 우리 뱅크가
      **외부 공급 뱅크로 위장 등록**된다.
    · `PROMPT_DIR/authored_<version>.json` — provenance. 부모 버전·태그·분모·미평가 표시.
      이게 없으면 "v080 에서 205개 삭제"와 "전면 재작성"이 사후 구분되지 않는다 (실측상
      이 둘은 가치 부호가 반대다).
    · `prompt_banks`(source='internal') 1행 + `bank_sentences` N행 — DSN 도달 시. 실패해도
      CSV/JSON 은 남긴다 (fail-soft: 작업을 잃지 않는다).

    ## 읽는 사람에게

    `eval` 은 항상 `null` 로 시작한다 — **이 버전은 아직 미평가다.** 선택에 쓰인 점수
    (`wins`/`purity` = top-k 축, `wave_gain` = 분포 IoU 축)는 GT 파생이라 그대로는 train
    점수다. 같은 뱅크 교체가 top-k 로는 +19pp, 제품 규칙으로는 +2.9pp 였다.
    ⚠️ `wave_gain` **원값 정렬 금지** — normal 클래스는 부호가 반대다
    (`stage_promptmap` 의 `signed = where(cls==0, -wgain, wgain)`). 정렬은 `wave_role` 로.
    """
    import csv as _csv
    import datetime as _dt

    import fiftyone as fo

    name = f"{PROFILES[PROFILE]['dataset']}-prompts"
    if name not in fo.list_datasets():
        raise SystemExit(f"bankfrom: 데이터셋 {name} 없음 — `promptmap` 스테이지를 먼저 돌려라")
    ds = fo.load_dataset(name)
    view = ds.match_tags(tag)
    if not view.count():
        raise SystemExit(f"bankfrom: 태그 {tag!r} 인 문장이 0개 "
                         f"(현재 태그: {ds.count_sample_tags()})")

    rows, dropped = _bank_rows(view.values("text"), view.values("category.label"), CLASS_NAMES)
    parents = collections.Counter(x for x in view.values("bank_version.label") if x)
    counts = collections.Counter(CLASS_NAMES[c] for c, _ in rows)

    out = f"{PROMPT_DIR}/authored_{version}.csv"
    try:                                    # x = 배타 생성. 같은 버전명 재사용을 조용히 덮지 않는다
        f = open(out, "x", newline="", encoding="utf-8")
    except FileExistsError:
        # 컨테이너가 root 로 쓰므로 호스트 일반 유저로는 지울 수 없다 — 실제 되는 명령을 준다.
        raise SystemExit(
            f"bankfrom: {out} 이 이미 있다 — 버전명을 바꾸거나 다음으로 치워라\n"
            f"  docker exec docker-analysis-1 rm {out} {out[:-4]}.json") from None
    with f:
        w = _csv.writer(f)
        w.writerow(["ID", "class", "prompt"])
        for i, (c, t) in enumerate(rows):
            w.writerow([i, c, t])           # ID 는 stage_bank 가 무시한다 (행 순번으로 보존)

    prov = {
        "version": version, "source": "internal", "origin": "curated",
        "dataset": name, "tag": tag, "profile": PROFILE,
        "n_selected": view.count(), "n_rows": len(rows), "n_dropped_dup": dropped,
        "class_counts": dict(counts), "parents": dict(parents),
        "model_name": "PE-Core-L14-336",
        "created_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "notes": notes,
        # 미평가 표시 — 홀드아웃 재채점 전에는 어떤 점수도 이 버전에 붙지 않는다.
        "eval": None,
        "eval_protocol": "leave-one-camera-out, 제품 규칙(분포 IoU). 프레임 이항 CI 금지 "
                         "(설계효과 9.22 → 소스영상 클러스터 부트스트랩). 음성대조군="
                         "같은 클래스 구성 무작위 동수 선택.",
    }
    with open(f"{PROMPT_DIR}/authored_{version}.json", "w", encoding="utf-8") as jf:
        json.dump(prov, jf, ensure_ascii=False, indent=2)

    log(f"bankfrom {version}: 태그 {tag} → 문장 {len(rows):,} "
        f"(선택 {view.count():,} / 중복탈락 {dropped}) → {out}")
    log(f"bankfrom {version}: 클래스 {dict(counts)} · 부모 {dict(parents)}")
    if len(parents) == 1:
        (pv, pn), = parents.items()
        total = ds.match({"bank_version.label": pv}).count()
        log(f"bankfrom {version}: {pv} 기준 유지 {pn:,}/{total:,} "
            f"(삭제 {total - pn:,}) — counterfactual 재현 가능")
    _bankfrom_ledger(version, rows, prov)
    log(f"bankfrom {version}: 다음 → python {os.path.basename(__file__)} bank "
        f"--csv {out} --version {version}")
    log(f"bankfrom {version}: ⚠️ 미평가 버전이다. 홀드아웃 재채점 전에는 점수를 인용하지 말 것")


def _bankfrom_ledger(version: str, rows: list, prov: dict) -> None:
    """019 원장에 `source='internal'` 뱅크 1행 + 문장 N행. **fail-soft** — 못 붙어도 CSV 는 산다.

    · `bank_id` 는 uuid5 로 결정적 — 재실행이 행을 복제하지 않는다 (ON CONFLICT DO NOTHING).
    · `parent_bank_id` 는 UUID 를 **계산하지 않고 조회**한다. 기존 외부공급 행의 UUID 생성
      규칙을 우리가 재현한다고 가정하면 FK 가 조용히 깨진다.
    · `gidx` = CSV 행 순번. `stage_bank` 가 만드는 npz 의 인덱스와 같은 정의여야
      프레임 `winner_gidx_*` 조인이 성립한다.
    """
    import hashlib
    import uuid as _uuid

    dsn = os.environ.get("DATAOPS_POSTGRES_DSN") or os.environ.get("POSTGRES_DSN")
    if not dsn:
        log("bankfrom: DATAOPS_POSTGRES_DSN 없음 → 원장 생략 (CSV/JSON 만)")
        return
    try:
        import psycopg2
        from psycopg2.extras import execute_values

        bank_id = str(_uuid.uuid5(_uuid.NAMESPACE_URL, f"internal:{version}"))
        parent_tag = next(iter(prov["parents"])) if len(prov["parents"]) == 1 else None
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            parent_id = None
            if parent_tag:
                cur.execute("SELECT bank_id FROM prompt_banks WHERE version_tag = %s "
                            "ORDER BY created_at LIMIT 1", (parent_tag,))
                got = cur.fetchone()
                parent_id = got[0] if got else None
                if not got:
                    log(f"bankfrom: 부모 {parent_tag} 원장 미등록 → parent_bank_id NULL")
            cur.execute(
                "INSERT INTO prompt_banks (bank_id, version_tag, source, sentence_storage, "
                "  origin_uri, model_name, sentence_count, class_counts, parent_bank_id, "
                "  ingested_by, notes) "
                "VALUES (%s,%s,'internal','db_backed',%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (bank_id, version, f"{PROMPT_DIR}/authored_{version}.csv",
                 prov["model_name"], len(rows), json.dumps(prov["class_counts"]), parent_id,
                 "prompt_geometry.bankfrom", json.dumps(prov, ensure_ascii=False)))
            execute_values(
                cur,
                "INSERT INTO bank_sentences (sentence_id, bank_id, content_hash, text, "
                "  class_label, gidx, origin, adopted) VALUES %s ON CONFLICT DO NOTHING",
                [(str(_uuid.uuid5(_uuid.NAMESPACE_URL, f"{version}:{i}")), bank_id,
                  hashlib.sha256(_norm_text(t).encode()).hexdigest()[:16], t,
                  CLASS_NAMES[c], i, "curated", False)
                 for i, (c, t) in enumerate(rows)])
        log(f"bankfrom {version}: 원장 등록 bank_id={bank_id} (문장 {len(rows):,})")
    except Exception as e:                  # noqa: BLE001 — 원장은 부가물, CSV 를 잃지 않는다
        log(f"bankfrom: 원장 등록 실패 ({type(e).__name__}: {e}) — CSV/JSON 은 정상 저장됨")


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


def load_probe_candidates() -> dict[str, list[str]]:
    """프로브 후보 문장. `PROBE_CANDIDATES_CSV` 가 있으면 CSV 가 위 dict 를 **통째로 대체**한다.

    위 하드코딩은 source-h(산업 현장) 문장 고정이라 다른 현장·도메인의 후보를 프로브할 방법이
    없었다. 미지정이면 옛 dict 그대로 — 기존 실행은 바이트 단위로 동일하다.

    CSV: 헤더 `class,prompt` 2컬럼. class 는 **이름**(`CLASS_NAMES` 의 값)이지 int 가 아니다.
    인코딩은 utf-8-sig — 엑셀이 저장한 BOM 이 첫 헤더명을 `﻿class` 로 만들어 조용히
    KeyError 를 내는 걸 막는다.

    ⚠️ **이벤트 클래스만 유효하다** (`EVENT_CLASSES`). 유일한 소비자인 `stage_gap()` 이
    `for c in EVENT_CLASSES` 로만 순회하므로, `normal` 처럼 그 밖의 클래스만 담긴 CSV 는
    검증을 통과해도 프로브가 **조용히 0건**이 된다. 그래서 어휘를 이벤트 클래스로 좁혀
    "통과했는데 아무 일도 안 일어남"을 막는다.

    ⚠️ 적용 범위: `stage_gap()` 은 `main()` 라우팅상 **sourceh 프로필에서만** 돈다
    (frames 는 `gap`→`stage_gap_frames` 로 가고 그쪽은 이 후보를 쓰지 않으며, sourcei 는
    `gap` 자체가 거부된다). 즉 이 로더는 현재 sourceh 경로 전용이다.

    지연 로드인 이유(모듈 상수 아님): `CLASS_NAMES` 는 `set_profile()` 이 런타임에 갈아끼우는
    전역이라 import 시점에 읽으면 sourceh 사전에 영구 고정된다. 지금은 세 프로필 모두 1~3 이
    falldown/fire/smoke 로 같아 결과가 우연히 일치하지만, 그 우연에 기대지 않는다.
    """
    path = os.environ.get("PROBE_CANDIDATES_CSV", "").strip()
    if not path:
        return PROBE_CANDIDATES

    import csv as _csv

    vocab = {CLASS_NAMES[c] for c in EVENT_CLASSES}
    out: dict[str, list[str]] = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        rd = _csv.DictReader(f)
        cols = {(c or "").strip() for c in (rd.fieldnames or [])}
        missing = {"class", "prompt"} - cols
        if missing:
            raise SystemExit(f"PROBE_CANDIDATES_CSV={path}: 컬럼 누락 {sorted(missing)} "
                             f"(헤더 필요: class,prompt / 실제: {rd.fieldnames})")
        for ln, row in enumerate(rd, start=2):
            cname = (row.get("class") or "").strip()
            text = (row.get("prompt") or "").strip()
            if not cname and not text:
                continue                                   # 빈 줄은 흘린다
            if not text:
                raise SystemExit(f"PROBE_CANDIDATES_CSV={path}:{ln}: prompt 가 비어 있다 "
                                 f"(class={cname!r})")
            # fail-fast: 어휘 밖 class 를 흘려보내면 그 행은 아무 스테이지도 소비하지 않아
            # "프로브가 조용히 0건"이 된다. 오타 하나가 침묵으로 끝나는 걸 막는다.
            if cname not in vocab:
                raise SystemExit(
                    f"PROBE_CANDIDATES_CSV={path}:{ln}: 사용할 수 없는 class {cname!r} — "
                    f"stage_gap 은 이벤트 클래스만 프로브한다 (프로필 {PROFILE} 기준 "
                    f"허용: {sorted(vocab)}). 'normal' 등 비이벤트 클래스는 순회 대상이 "
                    "아니라 통과시켜도 프로브 0건으로 조용히 끝난다.")
            out.setdefault(cname, []).append(text)
    if not out:
        raise SystemExit(f"PROBE_CANDIDATES_CSV={path}: 유효한 행이 0개 — "
                         "프로브 없이 도는 걸 막으려고 여기서 멈춘다")
    log(f"gap: 프로브 후보를 CSV 로 대체 → {path} "
        f"({ {k: len(v) for k, v in sorted(out.items())} })")
    return out


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
    probe_cands = load_probe_candidates()   # 한 번만 — 군집 루프 안에서 CSV 재읽기 금지

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
        # 뱅크 전체 스캔(12,480~16,125 문장)을 member 마다 반복하지 않는다 — 예측 클래스는
        # 3~5종뿐이라 클래스당 한 번이면 충분하다 (_Pruner.__init__ 의 self.gidx 와 같은 패턴).
        gidx_of = {pc: np.flatnonzero(banks[V4]["cls"] == pc)
                   for pc in {int(x) for x in pred4[miss]}}
        for ci in range(k):
            members = miss[km.labels_ == ci]
            # 이 군집을 실제로 잡아먹는 승자 프롬프트 (예측 클래스의 best)
            winner_texts = collections.Counter()
            for i in members:
                pc = int(pred4[i])
                pidx = gidx_of[pc][arg4[pc][i]]
                winner_texts[banks[V4]["prompt"][pidx]] += 1
            deficit = float((others[members] - best4[c][members]).mean())
            # 프로브: 후보 문장을 라이브 임베딩해 이 군집에서 would-win 측정
            probes = []
            for cand in probe_cands.get(cname, []):
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
# 상한이지 목표가 아니다 — 루프는 이득이 마르면 스스로 멈춘다. 12 로 뒀더니 v1.0.8.0 이
# 상한에 걸려 조용히 잘렸고(R12 시점에도 +6 씩 벌던 중), 풀어보니 R16 에 수렴해 +12 를 더
# 벌었다. v1.0.8.4 는 R10 수렴. 라운드당 ≈1.5s 라 여유를 크게 두는 편이 싸다.
PRUNE_ROUNDS = int(os.environ.get("PRUNE_ROUNDS", "30"))
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
        """현재 판정규칙으로 채점. 반환 state 는 규칙마다 내용이 다르니 통째로 넘긴다.

        argmax(K=1): (b2, a1, M, pred) — 옛 형식 그대로
        top-K:       {"vals","idxs","pred"} — 클래스별 상위 K+1 과 그 문장 인덱스
        """
        if RULE == "argmax":
            b1, b2, a1 = bank_top2_stream(self.X, self.bank, drop=mask)
            M = np.stack([b1[c] for c in self.classes], axis=1)
            return b2, a1, M, self.cls_arr[M.argmax(axis=1)]
        vals, idxs = bank_topk_stream(self.X, self.bank, drop=mask)
        pred, _, sel = vote_topk(vals, idxs)
        return {"vals": vals, "idxs": idxs, "sel": sel, "pred": pred}

    @staticmethod
    def _pred_of(state):
        return state[3] if isinstance(state, tuple) else state["pred"]

    def best_of(self, state):
        """클래스별 per-frame 최고 코사인 — reach 계산용(판정규칙과 무관한 잠재력 지표)."""
        if isinstance(state, tuple):
            return {c: state[2][:, i] for i, c in enumerate(self.classes)}
        return {c: state["vals"][c][:, 0] for c in self.classes}

    def class_best_local(self, state):
        """클래스별 per-frame 최고 코사인 문장의 **클래스-로컬** 인덱스 (cache.npz `arg_*` 와 동일 키).

        `best_of` 의 쌍이다 — 값이 아니라 그 값을 낸 문장을 가리킨다. 두 규칙 모두에서
        정의된다(top-K 는 클래스별 상위 사다리의 0번 칸이 곧 그 클래스의 1위).
        """
        if isinstance(state, tuple):
            return dict(state[1])
        return {c: state["idxs"][c][:, 0] for c in self.classes}

    def top1_gidx(self, state):
        """프레임별 **최고 코사인 기여 문장**의 뱅크 전역 인덱스. K=1 이면 argmax 승자와 같다.
        top-K 에서 프레임당 기여 문장이 여럿이라도, 사이트범위 같은 단일값 필드는 대표 1개가 필요하다."""
        if isinstance(state, tuple):
            _, a1, _, pred = state
            return np.array([self.gidx[int(c)][a1[int(c)][i]] for i, c in enumerate(pred)])
        sel = state["sel"]                       # 이미 코사인 내림차순
        c0, i0 = sel[:, 0, 0], sel[:, 0, 1]
        return np.array([self.gidx[int(c)][int(i)] if i >= 0 else 0 for c, i in zip(c0, i0)])

    def rank_gidx(self, state, r: int):
        """프레임별 **r위 기여 문장**의 뱅크 전역 인덱스 (r=0 이 1위). 없으면 원소 −1.

        ── "순위"의 정의 (사용자 Q1 확정 스키마의 근거) ─────────────────────────────
        top-K 다수결에서 **득표순은 문장 단위로 정의되지 않는다**. `vote_topk` 을 보면
        선택된 K 문장은 각각 정확히 1표씩 넣으므로(`votes = (sel_c == c).sum(1)`) 문장
        사이에 표 차이가 없다 — 득표는 *클래스*의 속성이지 문장의 속성이 아니다.
        state 안에서 문장 사이에 실재하는 유일한 서열은 **코사인 내림차순**이고,
        `vote_topk` 이 이미 그 순서로 `sel` 을 돌려준다 (`o = np.argsort(-allv, 1)[:, :k]`
        → `sel_i = take_along_axis(alli, o, 1)`). 따라서:

            rank r = 그 프레임의 **전역 top-K 풀 안에서 코사인 r번째** 문장
                     (클래스 무관 — 2위가 1위와 다른 클래스일 수 있다)

        r=0 은 `top1_gidx()` 와 같은 문장이고, 그건 곧 argmax 규칙의 승자와도 같다
        (max_c max_p cos = max_p cos — `_selftest_topk_ranks` 가 이 등식을 고정한다).
        그래서 `winner_gidx_<tag>` 를 top1 대표로 유지해도 규칙 전환으로 값이 안 바뀐다.

        ⚠️ RULE=argmax(K=1) state 에는 **2위 문장의 인덱스가 없다** — `bank_top2_stream`
           은 2위의 *값*(b2)만 접어 보관하고 인덱스는 1위(a1)만 준다. 클래스 사다리
           (2위 클래스의 1위 문장)로 대신 채우면 이름은 같은데 뜻이 다른 값이 되므로
           **채우지 않고 None 을 돌려준다** (조용한 거짓말 금지). 호출부는 필드를 아예
           안 쓴다.
        """
        if isinstance(state, tuple):
            return self.top1_gidx(state) if r == 0 else None
        sel = state["sel"]
        if r >= sel.shape[1]:
            return None
        c, i = sel[:, r, 0], sel[:, r, 1]
        return np.array([self.gidx[int(cc)][int(ii)] if ii >= 0 else -1
                         for cc, ii in zip(c, i)], dtype=np.int64)

    def rank_cos(self, state, r: int):
        """`rank_gidx` 와 **같은 문장**의 코사인. 열 자체가 없으면 None, 빈 슬롯은 NaN.

        ⚠️ 정렬을 다시 하지 않는다. `sel` 이 이미 지목한 (클래스, 클래스-로컬 인덱스)로
           `vals` 를 되짚는다 — 그래야 `rank_gidx` 와 **구조적으로** 어긋날 수 없다.
           예전 구현은 `allv` 를 독립적으로 재정렬했는데, 그러면 동점에서 정렬 kind 가
           조금만 달라져도 값과 문장이 다른 자리를 가리킨다 (두 곳의 kind 를 영원히
           맞춰야 하는 숨은 결합).
        ⚠️ 빈 슬롯은 **NaN**. `bank_topk_stream` 은 못 채운 자리에 센티널 −2.0 을 남기는데
           그대로 흘리면 "코사인 −2.0" 이라는 실재하지 않는 값이 새어나가 (rank_gidx 는
           같은 자리에 −1 을 내는데) 중앙값·정렬이 조용히 오염된다.
        """
        if isinstance(state, tuple) or r >= state["sel"].shape[1]:
            return None
        sc, si = state["sel"][:, r, 0], state["sel"][:, r, 1]
        out = np.full(len(si), np.nan, dtype=np.float32)
        for cl in self.classes:
            m = (sc == cl) & (si >= 0)
            if not m.any():
                continue
            # idxs[cl] 의 한 행 안에서 같은 문장은 한 번만 나온다 (타일마다 문장이 유일) →
            # argmax 로 찾은 첫 일치가 유일 일치다
            col = (state["idxs"][cl][m] == si[m][:, None]).argmax(1)
            out[m] = np.take_along_axis(state["vals"][cl][m], col[:, None], 1)[:, 0]
        return out

    def touched_by(self, state, drop):
        """그 프레임의 판정에 기여한 문장 중 하나라도 삭제됐나 = 판정이 바뀔 수 있는 프레임."""
        if isinstance(state, tuple):
            return drop[self.top1_gidx(state)]
        sel = state["sel"]
        out = np.zeros(sel.shape[0], dtype=bool)
        for j in range(sel.shape[1]):
            c, i = sel[:, j, 0], sel[:, j, 1]
            live = i >= 0
            g = np.zeros(len(out), dtype=np.int64)
            if live.any():
                g[live] = [self.gidx[int(cc)][int(ii)] for cc, ii in zip(c[live], i[live])]
            out |= live & drop[g]
        return out

    def contrib_frames(self, state):
        """문장(뱅크 전역 gidx) → 그 문장이 **판정에 기여한** 프레임 인덱스.

        argmax: 그 문장이 1위였고 그 클래스로 예측된 프레임 (= 옛 '승자')
        top-K : 그 문장이 전역 top-K 에 표를 넣은 프레임. 문장 하나가 여러 프레임에,
                한 프레임에 여러 문장이 걸린다 — K=1 이면 옛 정의와 일치한다.
        """
        out = {}
        if isinstance(state, tuple):
            _, a1, _, pred = state
            for c in self.classes:
                won = (a1[c] >= 0) & (pred == c)
                if not won.any():
                    continue
                for pi in np.unique(a1[c][won]):
                    out[int(self.gidx[c][pi])] = np.flatnonzero(won & (a1[c] == pi))
            return out
        sel = state["sel"]
        sc, si = sel[:, :, 0], sel[:, :, 1]
        rows = np.repeat(np.arange(sc.shape[0]), sc.shape[1])
        acc = {}
        for cc, ii, rr in zip(sc.ravel(), si.ravel(), rows):
            if ii >= 0:
                acc.setdefault(int(self.gidx[int(cc)][int(ii)]), []).append(rr)
        return {g: np.asarray(v) for g, v in acc.items()}

    def hits(self, mask):
        return int((self._pred_of(self.score(mask)) == self.gt).sum())

    def loo_gains(self, *state):
        """문장별 제거이득 (양수 = 지우면 정답이 는다).

        argmax: 그 문장이 자기 클래스 1위였던 프레임만 재판정하면 충분하다
                (클래스 점수는 내려가기만 하므로 다른 프레임의 argmax 는 안 바뀐다).
        top-K : **그 문장이 top-K 에 표를 넣은 프레임**만 재판정한다. 같은 논리인데
                영향 프레임이 K 배로 늘어난다 — 문장 하나가 여러 프레임의 표에 낀다.
        """
        if RULE == "argmax":
            b2, a1, M, pred = state
            out = {}
            for ci, c in enumerate(self.classes):
                for p in np.unique(a1[c]):
                    if p < 0:
                        continue
                    fr = np.flatnonzero(a1[c] == p)
                    sub = M[fr].copy()
                    sub[:, ci] = b2[c][fr]
                    new = self.cls_arr[sub.argmax(axis=1)]
                    out[(c, int(p))] = int((new == self.gt[fr]).sum()
                                           - (pred[fr] == self.gt[fr]).sum())
            return out

        st = state[0]
        vals, idxs, sel, pred = st["vals"], st["idxs"], st["sel"], st["pred"]
        # sel[:, :, 0]=클래스, [:, :, 1]=클래스-로컬 인덱스. 문장별 기여 프레임을 모은다
        contrib = {}
        sc, si = sel[:, :, 0], sel[:, :, 1]
        rows = np.repeat(np.arange(len(pred)), sc.shape[1])
        for cc, ii, rr in zip(sc.ravel(), si.ravel(), rows):
            if ii < 0:
                continue
            contrib.setdefault((int(cc), int(ii)), []).append(rr)
        out = {}
        for (c, pidx), fr in contrib.items():
            fr = np.asarray(fr)
            sub_v = {cl: vals[cl][fr] for cl in self.classes}
            sub_i = {cl: idxs[cl][fr] for cl in self.classes}
            new = vote_topk(sub_v, sub_i, exclude={(c, pidx)})[0]
            out[(c, pidx)] = int((new == self.gt[fr]).sum() - (pred[fr] == self.gt[fr]).sum())
        return out

    def greedy(self, tag=""):
        """LOO-양수 집합을 라운드마다 통째로 제거 → (drop 마스크, 곡선, base, final)."""
        drop = np.zeros(len(self.bank["cls"]), dtype=bool)
        state = self.score(drop)
        pred = self._pred_of(state)
        base = hits = int((pred == self.gt).sum())
        curve, converged = [], False
        for rnd in range(PRUNE_ROUNDS):
            gains = self.loo_gains(*state) if isinstance(state, tuple) else self.loo_gains(state)
            cand = [k for k, g in gains.items() if g > 0]
            if not cand:
                converged = True
                break
            trial = drop.copy()
            for c, p in cand:
                trial[self.gidx[c][p]] = True
            tstate = self.score(trial)
            tpred = self._pred_of(tstate)
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
            drop, state, pred, hits = trial, tstate, tpred, th
        if not converged and tag:   # 상한 절단은 반드시 드러낸다 (조용한 truncation 금지)
            log(f"prune {tag}: ⚠️ PRUNE_ROUNDS={PRUNE_ROUNDS} 상한에서 중단 — 아직 수렴 안 함. "
                "더 보려면 PRUNE_ROUNDS 를 올려라")
        return drop, curve, base, hits, converged


def contrib_pairs(pr: _Pruner, state) -> tuple[np.ndarray, np.ndarray]:
    """`_Pruner.contrib_frames` 의 **벡터화 쌍둥이** — (문장 gidx, 프레임 idx) 평행 배열.

    집합은 같다(`_selftest_site_scope` 가 두 경로의 동치를 고정한다). 따로 두는 이유는 규모다:
    contrib_frames 는 프레임당 K개(199,972×10 = 200만)를 파이썬 루프 + dict/list 로 쌓아
    frames 프로필에서 수백 MB·수십 초를 쓴다. 사이트 집계에 필요한 건 "문장×그룹 유일쌍"
    뿐이라 정수 배열 두 개로 끝난다 (200만 × 8B × 2 = 32MB).

    argmax 규칙: 그 문장이 자기 클래스 1위였고 그 클래스로 예측된 프레임 (= 옛 '승자')
    top-K 규칙: 그 문장이 전역 top-K 에 표를 넣은 프레임
    """
    if isinstance(state, tuple):
        _, a1, _, pred = state
        gs, fs = [], []
        for c in pr.classes:
            m = (a1[c] >= 0) & (pred == c)
            if not m.any():
                continue
            fr = np.flatnonzero(m)
            gs.append(pr.gidx[c][a1[c][fr]])
            fs.append(fr)
        if not gs:
            return np.zeros(0, dtype=np.int64), np.zeros(0, dtype=np.int64)
        return np.concatenate(gs), np.concatenate(fs)
    sel = state["sel"]
    n, w = sel.shape[0], sel.shape[1]
    sc = sel[:, :, 0].ravel()
    si = sel[:, :, 1].ravel()
    rows = np.repeat(np.arange(n, dtype=np.int64), w)
    live = si >= 0
    sc, si, rows = sc[live], si[live], rows[live]
    g = np.zeros(len(si), dtype=np.int64)
    for c in pr.classes:
        m = sc == c
        if m.any():
            g[m] = pr.gidx[c][si[m]]
    return g, rows


def group_win_matrix(sent_idx: np.ndarray, frame_idx: np.ndarray, gcode: np.ndarray,
                     n_bank: int, n_groups: int) -> np.ndarray:
    """[문장, 그룹] 승수 행렬. `n_groups_win = (W > 0).sum(1)`.

    dense 로 두는 게 맞다 — 12,480문장 × 21 project = 262K 칸(2MB). 널모델이 이 계산을
    수십 번 반복하므로 bincount 한 방으로 끝나는 형태가 필요하다.
    """
    flat = sent_idx * n_groups + gcode[frame_idx]
    return np.bincount(flat, minlength=n_bank * n_groups).reshape(n_bank, n_groups)


def _cramers_v(a: np.ndarray, b: np.ndarray, na: int, nb: int) -> float:
    """두 범주 축의 연관 강도 (0=무관, 1=완전 재인코딩). 널모델 게이트 ②의 수치."""
    if na < 2 or nb < 2 or len(a) == 0:
        return 0.0
    T = np.bincount(a * nb + b, minlength=na * nb).reshape(na, nb).astype(np.float64)
    n = T.sum()
    E = T.sum(1, keepdims=True) @ T.sum(0, keepdims=True) / n
    chi2 = float((((T - E) ** 2) / np.where(E > 0, E, 1.0)).sum())
    return float(np.sqrt(chi2 / (n * (min(na, nb) - 1))))


def _predict_acc(a: np.ndarray, b: np.ndarray, na: int, nb: int) -> tuple[float, float]:
    """a(예: site_scope) 로 b(예: project) 를 맞히는 다수결 정확도 + 최빈 b 기저율.

    Cramér's V 는 크기가 커도 해석이 어렵다. "이 축을 알면 project 를 얼마나 맞히나"는
    기저율과 나란히 놓으면 재인코딩 여부를 눈으로 판정할 수 있다.
    """
    if len(a) == 0:
        return 0.0, 0.0
    T = np.bincount(a * nb + b, minlength=na * nb).reshape(na, nb)
    return float(T.max(1).sum() / T.sum()), float(T.sum(0).max() / T.sum())


def _prune_bank(X: np.ndarray, gt: np.ndarray, src: np.ndarray, cam: np.ndarray,
                bank: dict, version: str) -> dict:
    # tier 를 **가장 먼저** 찍는다. 이 스테이지의 산출물(reach 랭킹·삭제셋)은 GT 가 없어도
    # 끝까지 계산되므로, 로그 첫 줄에 no_gt 가 없으면 아래 정답/순도 숫자를 그대로 믿게 된다.
    tier = gt_tier(gt)
    if tier == "no_gt":
        log(f"prune {version}: tier=no_gt — GT 0건. 아래 정답·순도·LOO 이득은 전부 무의미하고 "
            "reach/wins 랭킹만 **선택(selection) 신호**로 읽어라 (품질 판정 아님)")
    else:
        log(f"prune {version}: tier={tier} (GT {int((np.asarray(gt) >= 0).sum()):,}건)")

    pr = _Pruner(X, gt, bank)
    state = pr.score(None)
    pred = pr._pred_of(state)
    base_hits = int((pred == gt).sum())
    log(f"prune {version}: 규칙 {RULE}(k={RULE_K}) / 기준 정답 {base_hits:,}/{len(gt):,} "
        f"({base_hits / len(gt):.2%})")
    gains0 = pr.loo_gains(*state) if isinstance(state, tuple) else pr.loo_gains(state)

    # reach — 전 문장(비승자 포함). "승수"는 98%가 0이라 순위가 죽는다
    best = pr.best_of(state)
    reach, reach_cam = bank_reach_stream(X, bank, best, groups=cam)
    cams = sorted(set(cam.tolist()))
    n_pos = int((reach > 0).sum())
    n_dead = int((reach < -0.10).sum())
    log(f"prune {version}: reach>0 {n_pos:,}/{len(reach):,} ({n_pos / len(reach):.1%}) "
        f"— 실제 승자보다 훨씬 많다(예비군) / 완전 불활성(reach<−0.10) {n_dead:,}")

    # 문장 행: **전 뱅크**를 낸다. 승자만 담으면 라운드2+ 에서 승격→삭제된 문장이
    # 삭제 랭킹에서 통째로 빠진다 (v080 실측: 삭제 201 중 CSV 에 92개만 있었다).
    # argmax 면 '승자 프레임', top-K 면 '표를 넣은 프레임'. 정의는 contrib_frames 참고
    win_frames = pr.contrib_frames(state)

    sents = []
    for g in range(len(bank["cls"])):
        c = int(bank["cls"][g])
        fr = win_frames.get(g)
        row = {
            # 행마다 같은 값이지만 CSV 첫 칸에 있어야 한다 — 이 파일만 떼어 열었을 때
            # (엑셀/DictReader) tier 를 못 보면 선택 랭킹을 성적표로 읽게 된다.
            "tier": tier,
            "gidx": g, "cls": c, "cls_name": CLASS_NAMES[c],
            "wins": int(len(fr)) if fr is not None else 0,
            # 선언클래스 순도 — 다수결 순도가 아니다. 전부 normal 프레임을 가져간 smoke
            # 문장은 다수결 1.00 / 선언 0.00 이고, 후자가 맞는 판정이다.
            "purity": float((gt[fr] == c).mean()) if fr is not None else None,
            # cross-class 분해: 훔친 프레임의 실제 GT 분포. normal 오탈취(임계치 문제)와
            # fire↔smoke 오탈취(개념 경계 붕괴)는 처방이 다른데 순도 한 숫자는 둘 다 뭉갠다.
            # `.get(...,"no_gt")` 필수 — 원장의 미검수 프레임은 gt_class=−1 인데 CLASS_NAMES
            # 에 −1 이 없어 `KeyError: -1` 로 죽었다. 그러면 GT 없는 뱅크는 리포트 자체가
            # 안 나와 tier=no_gt 표기가 **도달 불가**가 된다 (이 스테이지는 GT 없이도
            # reach 랭킹을 내는 게 목적이다). −1 은 오탈취가 아니라 '모름'이라 별도 이름을 준다.
            "stolen": ("|".join(f"{CLASS_NAMES.get(int(k), 'no_gt')}:{v}" for k, v in
                                sorted(collections.Counter(gt[fr][gt[fr] != c].tolist()).items()))
                       if fr is not None else ""),
            "loo_gain": 0,                       # 아래에서 gains0 로 덮어쓴다
            "reach": round(float(reach[g]), 5),
            # ⚠️ 이 둘은 다른 값이다. 예전엔 n_cams_win 을 reach>0 으로 셌는데,
            # 승자 201개 중 74.6% 가 실제 승수와 불일치했고 **전부 한 방향**이었다
            # (reach 가 "공통"으로 과대포장, 119건 vs 역방향 0건). reach>0 은
            # "그 카메라 어딘가에서 1등이 될 수 있었다"는 잠재력이고, 실제로 이겼는지가 아니다.
            "n_cams_win": (len({cam[i] for i in fr}) if fr is not None else 0),
            "n_cams_reach": int(sum(reach_cam[k][g] > 0 for k in cams)),
            "text": bank["prompt"][g],
        }
        for k in cams:
            row[f"reach_{k}"] = round(float(reach_cam[k][g]), 5)
        sents.append(row)
    # loo_gain 은 (클래스, 클래스-로컬 인덱스) 키라 전역 인덱스로 되돌려 채운다
    for (c, p), gval in gains0.items():
        sents[int(pr.gidx[c][p])]["loo_gain"] = int(gval)

    n_win = sum(1 for r in sents if r["wins"] > 0)
    n_harm = sum(1 for r in sents if r["loo_gain"] > 0)
    log(f"prune {version}: 승자 {n_win}개 중 순유해 {n_harm}개 "
        f"(개별 LOO 합 +{sum(max(0, r['loo_gain']) for r in sents)})")

    drop, curve, _, final_hits, converged = pr.greedy(tag=version)
    for r in sents:
        r["dropped"] = bool(drop[r["gidx"]])
    n_drop_nonwinner = sum(1 for r in sents if r["dropped"] and r["wins"] == 0)
    log(f"prune {version}: 삭제 {int(drop.sum())}문장 중 {n_drop_nonwinner}개는 라운드0 비승자 "
        "— 앞 라운드 삭제로 승격됐다가 지워진 것(예비군 승격의 직접 증거)")

    # ── 홀드아웃: 영상 2폴드. A 에서 고른 삭제셋을 A 자신과 B 에 각각 적용 ──
    # ⚠️ 대조군은 반드시 **같은 삭제셋의 A 이득**이다. 전체 13,144 에 적합한 greedy 의
    #    이득(insample_full)과 B 이득을 나란히 놓으면 분모·적합대상이 달라 비교 불가다
    #    (2026-08-03 수정 — 그 전까지 문서·로그가 그 잘못된 짝을 인용하고 있었다).
    vids = sorted(set(src.tolist()))
    fold_b = {v for i, v in enumerate(vids) if i % 2}         # 결정적 분할 (seed 불필요)
    mb = np.array([s in fold_b for s in src])
    hold = {"n_videos": len(vids), "n_a": int((~mb).sum()), "n_b": int(mb.sum())}
    if mb.any() and (~mb).any():
        pa = _Pruner(X[~mb], gt[~mb], bank)
        pb = _Pruner(X[mb], gt[mb], bank)
        drop_a, _, a_base, a_final, _ = pa.greedy()
        b_before, b_after = pb.hits(None), pb.hits(drop_a)
        na, nb = hold["n_a"], hold["n_b"]
        a_gain_pp = 100.0 * (a_final - a_base) / max(1, na)
        b_gain_pp = 100.0 * (b_after - b_before) / max(1, nb)
        hold.update({
            "n_dropped_on_a": int(drop_a.sum()),
            "a_base": a_base, "a_after": a_final, "a_gain_pp": a_gain_pp,
            "b_before": b_before, "b_after": b_after, "b_gain_pp": b_gain_pp,
            "transfer_ratio": (b_gain_pp / a_gain_pp) if a_gain_pp else None,
            "a_baseline": a_base / max(1, na), "b_baseline": b_before / max(1, nb),
            # 참고용 — A 이득과 비교 금지 (적합 대상이 전체 프레임이다)
            "insample_full_gain_pp": 100.0 * (final_hits - base_hits) / len(gt),
        })
        log(f"prune {version}: 홀드아웃(영상 {len(vids)} → A {na:,}/B {nb:,}프레임) "
            f"A에서 고른 {int(drop_a.sum())}문장 → "
            f"A(적합) {a_gain_pp:+.2f}pp / B(홀드아웃) {b_gain_pp:+.2f}pp "
            f"= 전이율 {hold['transfer_ratio']:.0%}" if a_gain_pp else "")
        log(f"prune {version}: 폴드 난이도 A {hold['a_baseline']:.2%} vs B {hold['b_baseline']:.2%} "
            "(B 가 낮으면 개선 여지가 커서 전이율이 100% 를 넘을 수 있다)")
    else:
        hold["note"] = "영상이 1개뿐 — 홀드아웃 불가"
        log(f"prune {version}: ⚠️ 홀드아웃 불가 (영상 {len(vids)}개) — 인샘플 이득은 과적합 상한이다")

    # 같은 뿌리의 두 번째 사고: 여기도 argmax 시절 state 내부값(`a1`)을 호출자 스코프에서
    # 그대로 쓰고 있었다. `a1` 은 `_Pruner.score()` 의 argmax 분기 지역변수라 이 스코프에
    # 존재한 적이 없다 → RULE 과 무관하게 NameError. 규칙 무관 대표 문장은 top1_gidx 가 낸다.
    winner_g = pr.top1_gidx(state)
    by_g = {r["gidx"]: r for r in sents}
    ranked = sorted(sents, key=lambda r: (-r["loo_gain"], r["purity"] if r["purity"] is not None else 9))
    # JSON 에는 **결정 대상**(이겼거나 삭제됐거나)만 넣는다 — 전 뱅크 16k행을 넣으면
    # prune.json 이 6MB 가 되어 사람이 못 읽는다. 전량은 CSV 가 받는다.
    decision = [r for r in ranked if r["wins"] > 0 or r["dropped"]]
    # ⚠️ `tier` 는 **버전 객체의 첫 키**다 (prune.json 의 진짜 최상단이 아니라).
    #    prune.json 의 최상단 키 공간은 버전 이름 전용이고, report_charts.py 의
    #    `versions = [...] or list(pr)` 폴백이 그걸 그대로 버전 목록으로 쓴다 —
    #    거기에 "tier" 를 끼우면 `prune_tier.csv` 를 열려다 죽는다. 하나 위로 올리지 말 것.
    return {"tier": tier,
            "n_gt": int((np.asarray(gt) >= 0).sum()),
            "version": version, "n_frames": int(len(gt)), "base_hits": base_hits,
            "final_hits": final_hits, "n_dropped": int(drop.sum()),
            "n_dropped_nonwinner": n_drop_nonwinner,
            "total_gain": final_hits - base_hits, "converged": converged,
            "n_prompts": len(sents), "n_winners": n_win, "n_harmful": n_harm,
            "reach_positive": n_pos, "reach_dead": n_dead, "cameras": cams,
            "curve": curve, "holdout": hold, "sentences": decision,
            "_winner_gidx": winner_g, "_by_gidx": by_g, "_all_rows": ranked}


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
    cam = load_cameras(keys)
    res = {}
    for v in VERSIONS:
        res[v] = _prune_bank(X, gt, src, cam, banks[v], v)

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
        rows = res[v]["_all_rows"]
        p = f"{REPORT_DIR}/prune_{v}.csv"
        # ⚠️ **전 뱅크**를 쓴다. 예전엔 라운드0 승자만 써서 삭제 랭킹이 실제 삭제셋의 절반도
        #    못 담았다 (v080: 삭제 201 중 92행만). 라운드2+ 에서 승격→삭제된 문장이 빠졌었다.
        cols = ["tier",
                "gidx", "cls", "cls_name", "wins", "purity", "stolen", "loo_gain", "reach",
                *[f"reach_{k}" for k in res[v]["cameras"]],
                "n_cams_win", "n_cams_reach", "dropped", "text"]
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = _csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        nd = sum(1 for r in rows if r["dropped"])
        log(f"prune: 랭킹 CSV → {p} [tier={res[v]['tier']}] "
            f"(전 뱅크 {len(rows):,}행 / 삭제 {nd} 전수 포함)")
    tiers = ", ".join(f"{v}={res[v]['tier']}" for v in VERSIONS)
    log(f"prune 완료 → {GEO}/prune.json (tier: {tiers})")


# ────────────────────── attach ──────────────────────
def stage_attach() -> None:
    """뱅크 **1벌**을 프레임에 붙인다 — 이미지마다 "가장 맞는 문장"과 그 예측 클래스.

    비교 분석(flips/prune)과 달리 뱅크 하나만 필요할 때 쓴다. 어느 버전을 붙일지는
    env `BANK_ATTACH`(기본 `BANK_A`) 로 고른다.

    붙는 필드 (버전 태그는 필드명에 박힌다 — 다른 버전을 붙여도 서로 안 덮어쓴다):
      · `pred_<v_underscored>`        Classification. label=예측 클래스,
                                      confidence=그 클래스의 best cosine. Color by 는 `.label`
      · `top_prompt_<v_underscored>`  실제로 이긴 문장 원문 (StringField)
      · `pred_margin_<vtag>`          top1 − top2 클래스 점수차. 확신도 정렬용
        (⚠️ 연속 float 은 App 에서 색이 안 나온다 — 색칠하려면 구간화 필드가 따로 필요)
      · `winner_gidx_r{2,3}_<vtag>`   2·3위 문장의 gidx (IntField, top-K 규칙에서만)
      · `top_prompt_r{2,3}_<v_underscored>` 그 문장 원문 (StringField)

    ── 규칙 개작 (2026-08-18) ──────────────────────────────────────────────
    계산 자체는 **top-K state 위에서** 한다 (`_Pruner.score`). 예전엔 `bank_top2_stream`
    으로 클래스별 1·2위만 접어서 순위 사다리를 만들 수 없었다.

    ⚠️ 그렇다고 `pred_<vt>` 를 다수결로 바꾸지는 **않는다.** 이 필드군은 정본 3층
       익스포트의 `argmax_k1` 슬롯이고(`prompt_scores_export.RULE_FIELDS`), 다수결 슬롯은
       이미 `vote_<vt>` / `vote_margin_<vtag>`(stage_vote) 가 따로 갖고 있다. 여기를
       다수결로 덮으면 두 규칙이 같은 숫자가 되어 스펙 §M9 의 "규칙별 예측 슬롯" 비교가
       통째로 무의미해지고, P3(같은 개념 두 이름 금지)도 깨진다.
       → `pred_/pred_margin_/runner_up/close_call/cos_best_*` = argmax_k1 슬롯 유지.
         단 값은 같은 state 의 열0(클래스별 최고 코사인)에서 뽑으므로 옛 경로와 동일하다.
    ⚠️ `winner_gidx_<vtag>` 는 규칙과 무관하게 **값이 안 변한다** — argmax 승자 =
       max_c max_p cos = max_p cos = 전역 코사인 1위 = top-K 사다리의 1위이기 때문이다
       (`_selftest_topk_ranks` 가 이 등식을 고정한다). 그래서 top1 대표를 유지해도
       `@user/prompt-compare` 의 등식 조인과 저장뷰가 그대로 산다.

    ⚠️ 뱅크 npz 는 **읽기 전용**이다. 여기서 하는 일은 프레임 → 문장 매칭 결과를
       FiftyOne 문서에 쓰는 것뿐이고 프롬프트 자체는 아무것도 변하지 않는다.

    ── frames 프로필 개방 (2026-08-18, Phase 1-1) ───────────────────────────
    GT 불필요 스테이지라 `frames`(구 frames_captions) 에서도 성립한다 (계획서 §3 이식 판정 "그대로").
    ⚠️ 대상은 **modality=frame 187,994장**이다 — 데이터셋 문서 수 199,972 는 캡션 11,978
       (같은 필드에 든 **텍스트** 벡터)을 포함한 값이라, 그걸 프레임 수로 읽고 이미지 지표를
       계산하면 모달리티가 섞인다. 원장(`frames_bank_ledger.py`)이 이미 frame 만 담으므로
       여기서는 원장 키를 그대로 따르면 된다 (2026-08-18 라이브 실측: 199,972 / 187,994 / 21 project).
    세 지점만 프로필화했다:
      · 입구 — `load_matched()` (frames 는 embed.npz 부재 시 데이터셋 스트리밍)
      · 조인 — `key_to_ids()` (frames 원장 key = sample id)
      · GT 의존 산출 — 정확도는 **GT 있는 프레임만** 분모로 쓰고 tier 를 같이 찍는다
        (frames 는 ls_finalized 40장뿐이라 전 프레임 분모로 재면 0.02% 상한의 가짜 성적표가
         나온다). tier=no_gt 면 줄 자체를 안 찍는다.
    """
    import fiftyone as fo

    version = os.environ.get("BANK_ATTACH", VERSIONS[0])
    path = f"{PROMPT_DIR}/{version}.npz"
    if not os.path.exists(path):
        raise SystemExit(f"뱅크 npz 없음: {path} — 먼저 `bank --csv <csv> --version {version}`")

    keys, X, gt, src = load_matched()
    bank = load_bank(version)                      # 문장은 DB 정본 (load_bank 주석)
    classes = sorted(set(bank["cls"].tolist()))
    log(f"attach {version}: 문장 {len(bank['cls']):,} / 프레임 {len(keys):,} / "
        f"규칙 {RULE}(k={RULE_K}) / GT tier={gt_tier(gt)} — 매칭 계산")

    pr = _Pruner(X, gt, bank)
    state = pr.score(None)                      # RULE=topk → 사다리 포함 / argmax → 옛 4-tuple
    b1 = pr.best_of(state)                      # {c: [N]} 클래스별 최고 코사인 (두 규칙 공통)
    M = np.stack([b1[c] for c in classes], axis=1)
    order = np.sort(M, axis=1)
    pred_margin = (order[:, -1] - order[:, -2]).astype(np.float32)   # top1−top2 (GT 불필요)
    winner_col = M.argmax(axis=1)
    pred = np.array(classes)[winner_col]        # argmax_k1 슬롯 — 위 docstring 참조
    # 2위 클래스 — **승자 열을 마스킹하고 argmax** 한다. `np.argsort` 는 unstable quicksort 라
    # 동점(float32 코사인에서 실제 발생 가능)일 때 [:,1] 이 승자와 같아질 수 있다.
    # `argmax` 는 첫 인덱스를 보장하므로 pred 의 타이브레이크 규칙과도 일치한다.
    # (bank_top2_stream 이 m2 를 구할 때 쓰는 것과 같은 기법)
    _mask = M.copy()
    _mask[np.arange(M.shape[0]), winner_col] = -np.inf
    second_col = _mask.argmax(axis=1)
    win_g = pr.top1_gidx(state)                 # 전역 코사인 1위 = argmax 승자 (등식은 selftest)
    best = M.max(axis=1)
    # 순위 사다리 2·3위 — `rank_gidx` 의 docstring 이 "순위" 정의와 argmax 미지원 근거를 갖는다
    rank_g = {r: pr.rank_gidx(state, r - 1) for r in RANK_EXTRA}
    rank_c = {r: pr.rank_cos(state, r - 1) for r in RANK_EXTRA}
    if all(rank_g[r] is None for r in RANK_EXTRA):
        log(f"attach {version}: ⚠️ 규칙 {RULE}(k={RULE_K}) 에는 2·3위 문장 **인덱스가 없다** "
            "— 순위 필드를 만들지 않는다 (클래스 사다리로 대신 채우면 이름만 같은 딴 값). "
            "RULE=topk 로 다시 돌리면 생긴다")
    # 규칙 축 자기보고 — 이 스테이지가 쓰는 argmax 슬롯과 제품 규칙이 어디서 갈리는지
    rule_pred = pr._pred_of(state)
    n_split = int((rule_pred != pred).sum())
    log(f"attach {version}: argmax_k1 ↔ {RULE}(k={RULE_K}) 판정 불일치 {n_split:,}/{len(pred):,} "
        f"({n_split / max(1, len(pred)):.1%}) — 다수결 예측은 `vote_<vt>`(stage_vote) 소관이다")

    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    ids = key_to_ids(ds, keys)
    ok = [i for i, x in enumerate(ids) if x]
    if len(ok) < len(ids):
        log(f"attach: FiftyOne 매칭 {len(ok)}/{len(ids)} — 나머지 프레임은 필드 미설정")
    if not ok:
        raise SystemExit(f"attach: 원장 key 가 데이터셋과 하나도 안 붙는다 "
                         f"(key_join={PROFILES[PROFILE]['key_join']}) — 조인 방식 확인")

    vt = version.replace(".", "_")
    if not vt.startswith("v"):
        vt = "v" + vt
    tag = vtag(version)
    # clear-then-set: 이전 런의 stale 값이 남으면 가장 악질적인 거짓말이 된다
    # ── 신규 6+1 필드는 **버전 중립** 이름을 쓴다 (codex 지적) ──
    # 버전을 필드명에 박으면 48버전 환경에서 스키마가 뱅크 수만큼 늘어난다(형제 stage `score`
    # 가 같은 이유로 폐기한 패턴). 대신 **어느 뱅크가 썼는지를 `attached_bank` 로 기록**한다.
    # ⚠️ 그래서 다른 버전을 attach 하면 이 6개는 **덮어써진다**. 기존 태그 필드
    #    (`pred_<vt>`/`pred_margin_<tag>`/`winner_gidx_<tag>`/`top_prompt_<vt>`)는 flips·prune·
    #    guide 가 2뱅크 대응비교로 읽으므로 그대로 태그를 유지한다.
    cos_flds = [f"cos_best_{CLASS_NAMES[c]}" for c in classes]
    prev = ds.get_field_schema()
    old_bank = None
    if "attached_bank" in prev:
        vals = [v for v in set(ds.values("attached_bank.label")) if v]
        old_bank = vals[0] if len(vals) == 1 else (vals or None)
    if old_bank and old_bank != version:
        log(f"attach: ⚠️ 버전 중립 필드 6개를 {old_bank} → {version} 로 **덮어쓴다** "
            f"(cos_best_*/runner_up/close_call). 두 뱅크를 나란히 보려면 태그 필드를 쓸 것")
    rank_flds = [f for r in RANK_EXTRA
                 for f in (rank_gidx_field(tag, r), rank_prompt_field(vt, r))]
    for fld in (f"pred_{vt}", f"top_prompt_{vt}", f"pred_margin_{tag}",
                f"winner_gidx_{tag}", "runner_up", "close_call", "attached_bank",
                *cos_flds, *rank_flds):
        if fld in prev:
            ds.clear_sample_field(fld)   # 규칙을 바꿔 재실행하면 순위 필드도 stale 이 된다
    pairs = [(ids[i], i) for i in ok]            # 배치 쓰기 단위 (set_values_batched 참조)
    set_values_batched(ds, f"pred_{vt}", pairs,
                       lambda i: fo.Classification(label=CLASS_NAMES[int(pred[i])],
                                                   confidence=float(best[i])))
    set_values_batched(ds, f"top_prompt_{vt}", pairs, lambda i: bank["prompt"][int(win_g[i])])
    # ⚠️ 문장을 **숫자 ID** 로도 내린다 — 이게 화면2(문장→프레임 역방향 조회)의 유일한 실용 경로다.
    # 이 App 은 Query Performance 모드(enable_query_performance=True)라 String/Classification
    # 필드가 타입·카디널리티 불문 전부 **자유텍스트 substring 검색**으로만 렌더된다
    # (체크박스·카운트 없음 — camera 3종·ground_truth 4종도 동일, 실측 확인).
    # 서술문에서 "smoke" 하나로 여러 문장이 동시에 잡혀 문장 특정이 불가능하다.
    # IntField 는 min/max 정확값 필터라 충돌이 없고, 값이 prune CSV 의 `gidx` 와 같은 키다.
    # gidx 전역 오프셋 (GIDX_OFFSET 주석): -prompts 데이터셋의 gidx 와 등식 조인이 되도록
    # 버전 순번 오프셋을 더한다. BANK_A(=BANKS[0])는 0 이라 기존 v080 값과 동일.
    goff = (BANKS.index(version) if version in BANKS else 0) * GIDX_OFFSET
    set_values_batched(ds, f"winner_gidx_{tag}", pairs, lambda i: int(win_g[i]) + goff)
    set_values_batched(ds, f"pred_margin_{tag}", pairs, lambda i: float(pred_margin[i]))

    # ── 순위 사다리 2·3위 (사용자 Q1: "선택된 것 말고 나머지 순위도 보여줘") ──
    # 1위와 **같은 goff/같은 키공간**을 쓴다 — prune CSV·`-prompts` gidx 와 그대로 조인된다.
    # gidx<0(빈 자리, 문장 3개 미만인 초소형 뱅크)은 **쓰지 않는다** — goff 를 더하면
    # 엉뚱한 문장을 가리키게 되므로 null 로 남기는 게 맞다.
    for r in RANK_EXTRA:
        g = rank_g[r]
        if g is None:
            continue
        live = [i for i in ok if g[i] >= 0]
        lpairs = [(ids[i], i) for i in live]
        set_values_batched(ds, rank_gidx_field(tag, r), lpairs, lambda i: int(g[i]) + goff)
        set_values_batched(ds, rank_prompt_field(vt, r), lpairs,
                           lambda i: bank["prompt"][int(g[i])])
        cos = rank_c[r]
        n_cross = int((bank["cls"][g[live]] != bank["cls"][win_g[live]]).sum()) if live else 0
        log(f"attach {version}: {r}위 필드 {rank_gidx_field(tag, r)} / "
            f"{rank_prompt_field(vt, r)} ({len(live):,}장, 고유문장 "
            f"{len(set(g[live].tolist())):,}, 1위와 다른 클래스 {n_cross:,}장"
            + (f", cos 중앙 {float(np.median(cos[live])):.3f}" if cos is not None and live else "")
            + ")")

    # 긴 문장 원문이 썸네일 칩으로 깔리면 이미지가 안 보인다 → 그리드에 띄울 필드만 allowlist.
    # ⚠️ Color by 대상은 반드시 여기 있어야 한다 (없으면 App 이 TypeError 로 죽는다).
    from fiftyone.core.odm.dataset import ActiveFields
    # 클래스별 best 코사인 — M 이 이미 [N, C] 라 추가 계산이 없다.
    # ⚠️ **뱅크 내부 비교만 유효**하다. 뱅크 간 절대 코사인 비교는 가산 오프셋 때문에 불공정
    #    (이 파일 cover_viz 폐기 이력과 같은 이유).
    # ⚠️ codex 지적: 버전명을 필드에 박는 패턴은 48버전 환경에서 스키마 누수다. 여기서는
    #    stage_attach 가 원래 버전 태그 설계(다른 버전을 붙여도 안 덮어씀)라 따르되,
    #    은퇴한 태그는 clear 가 아니라 `delete_sample_fields` 로 지워야 스키마가 준다.
    for j, c in enumerate(classes):
        set_values_batched(ds, f"cos_best_{CLASS_NAMES[c]}", pairs, lambda i, j=j: float(M[i, j]))
    set_values_batched(ds, "runner_up", pairs,
                       lambda i: fo.Classification(
                           label=CLASS_NAMES[classes[int(second_col[i])]]))
    # 이 6개가 어느 뱅크 산출인지 — 버전을 필드명에서 뺀 대가로 반드시 있어야 한다
    set_values_batched(ds, "attached_bank", pairs,
                       lambda i: fo.Classification(label=version))
    # 아슬아슬함 — 연속 float 은 App 에서 색이 안 나온다.
    # ⚠️ 경계를 **절대값으로 박지 않는다**: 이 파일이 이미 절대컷 0.005 를 기각했다
    #    ("절대컷은 뱅크의 성질이지 프레임의 성질" — v080 11.5%ile vs v084 8.1%ile).
    #    뱅크별 백분위에서 뽑아 **뱅크 상대** 라벨로 만든다 → 버전 간 비교는 여전히 금물.
    pm_ok = pred_margin[ok]
    qs = np.percentile(pm_ok, [10, 25, 50, 75]) if len(pm_ok) else np.zeros(4)
    q_lab = [f"하위10%(≤{qs[0]:.4f})", f"10-25%(≤{qs[1]:.4f})", f"25-50%(≤{qs[2]:.4f})",
             f"50-75%(≤{qs[3]:.4f})", f"상위25%(>{qs[3]:.4f})"]

    def _cc(m: float) -> str:
        if not np.isfinite(m):                    # NaN 이 ">최대" 로 새면 가장 확신한 것처럼 보인다
            return "미정의"
        return q_lab[int(np.searchsorted(qs, m, side="left"))]

    set_values_batched(ds, "close_call", pairs,
                       lambda i: fo.Classification(label=_cc(float(pred_margin[i]))))
    log(f"attach {version}: 버전중립 필드 {len(classes) + 3}개 "
        f"(cos_best_*·runner_up·close_call·attached_bank) "
        f"· 마진 백분위 경계 {np.round(qs, 4).tolist()}")

    # relabel_transition 은 뺐다 — 재라벨 이력은 이 분석의 축이 아니고 소유자는 frames_eval.py 다
    # codex 지적: Color by 대상은 반드시 active_fields 에 있어야 한다 (없으면 App 크래시).
    # ⚠️ 순위 필드(winner_gidx_r*/top_prompt_r*)는 **일부러 뺀다** — gidx 는 사람이 쓰는
    #    필터가 아니라 패널 조인 키이고(스펙 §4-5), 문장 원문은 고카디널리티라 필터
    #    부적합이다(P4/§4-4). 둘 다 모달에서 읽는 값이고, 여기 넣으면 그리드 칩이
    #    이미지를 덮는다. G1(분석가가 보는 필터 증가율 0)도 이 원칙에서 나온다.
    active = [f for f in ("ground_truth", f"pred_{vt}", "runner_up", "close_call",
                          "attached_bank", "environment",
                          PROFILES[PROFILE]["group_field"])   # sourceh=camera / frames=project
              if f in ds.get_field_schema()]
    active = set_active_fields(ds, ActiveFields, active)
    # 규칙 출처 — 어느 규칙의 사다리인지 필드명에는 안 들어간다. probecache 의
    # `probe_k_<tag>` 와 같은 자리(ds.info)에 남긴다. 스키마 비용 0.
    made = [r for r in RANK_EXTRA if rank_g[r] is not None]   # 사다리 폭이 모자라면 일부만 난다
    ds.info = {**(ds.info or {}),
               f"attach_rule_{tag}": {"rule": RULE, "k": RULE_K, "bank": version,
                                      "ranks": made, "gt_tier": gt_tier(gt)}}
    ds.save()

    dist = collections.Counter(CLASS_NAMES[int(p)] for p in pred)
    n_used = len(set(win_g.tolist()))
    log(f"attach {version}: 필드 pred_{vt} / top_prompt_{vt} / pred_margin_{tag} / "
        f"winner_gidx_{tag} 기록 ({len(ok):,}장)")
    # ⚠️ tier 를 정확도와 **같은 줄**에 찍는다 — no_gt 인데 "정확도 0.00%" 만 보이면
    #    성적표로 읽힌다 (prune 이 같은 이유로 tier 를 먼저 찍는다).
    # ⚠️ 분모는 **GT 있는 프레임**뿐이다. 전 프레임을 분모로 쓰면 미검수(gt=−1)가 전부 오답으로
    #    계산돼, frames(ls_finalized 40장/199,972) 에서 "정확도 0.02%" 라는 가짜 성적표가 난다.
    #    sourceh 은 전 프레임이 검수돼 있어 값이 예전과 동일하다.
    tier = gt_tier(gt)
    m_gt = np.asarray(gt) >= 0
    if tier == "no_gt":
        acc_txt = f"GT[tier={tier}] — 정확도 생략 (검수 프레임 0장)"
    else:
        acc = float((pred[m_gt] == gt[m_gt]).mean())
        acc_txt = (f"GT[tier={tier}, n={int(m_gt.sum()):,}] 대비 argmax_k1 정확도 {acc:.2%}"
                   + ("" if m_gt.all() else f" (검수분만 — 전체 {len(pred):,}장 중)"))
    log(f"attach {version}: 예측 분포 {dict(dist)} / {acc_txt} / "
        f"실제로 쓰인 1위 문장 {n_used:,}개 ({n_used / len(bank['cls']):.2%})")
    log(f"attach {version}: 그리드 표시 필드(active_fields) {active or 'None(제한 없음, 미변경)'} "
        "— 문장 원문·순위 사다리는 모달에서 본다")
    log("attach 완료")


# ────────────────────── site (화면4 — 사이트 범위 + LOPO 전이 검정) ──────────
# 계획: docs/apo-fiftyone-plan-2026-08-03.md §4 Phase 1 (1-2·1-3·1-4) + §1 D3.
# 전부 **GT-free** 다 — frames 의 GT 는 ls_finalized 40장(0.02%)뿐이고, 이 스테이지의 어떤
# 숫자도 gt 를 안 읽는다. 그래도 산출물 최상단에 `gt_tier` 를 박는다: 옆 스테이지 산출물과
# 섞여 읽힐 때 "GT 로 검증된 값"으로 오독되는 것을 막는 게 이 파일의 관례다(prune/attach 동일).
SITE_NULL_PERM = int(os.environ.get("SITE_NULL_PERM", "50"))     # 널모델 순열 횟수
SITE_SCREEN_CAP = int(os.environ.get("SITE_SCREEN_CAP", "5000"))  # Embeddings 패널 상한
SITE_BRAIN = os.environ.get("SITE_BRAIN", "emb_viz")


def _pick_brain(ds) -> str | None:
    """화면4 가 쓸 시각화 brain run. 없는 키를 박으면 패널이 빈 채로 뜨거나 죽는다."""
    try:
        runs = list(ds.list_brain_runs())
    except Exception:  # noqa: BLE001 — brain 미설치/미등록은 치명이 아니다
        return None
    if SITE_BRAIN in runs:
        return SITE_BRAIN
    viz = [r for r in runs if "viz" in r or "vis" in r]
    return viz[0] if viz else None


def _stratified_ids(ids: list, idx: list, strata: np.ndarray, cap: int) -> list:
    """층 라운드로빈 표본 — 큰 project 가 5,000 슬롯을 통째로 먹지 않게.

    FiftyOne Embeddings 패널은 **5,000점 상한**이라 199,972 전체를 못 그린다(계획서 §4).
    무작위 take 로 자르면 cohort-b(73,390) 가 37% 를 가져가 사이트 비교가 성립하지 않는다.
    결정적이다 — 원장 순서가 고정이므로 재실행 간 같은 표본이 나온다.
    """
    by_s: dict = {}
    for i in idx:
        by_s.setdefault(strata[i], []).append(i)
    out: list = []
    pos = 0
    while len(out) < cap:
        added = False
        for s in sorted(by_s):
            lst = by_s[s]
            if pos < len(lst):
                out.append(ids[lst[pos]])
                added = True
                if len(out) >= cap:
                    break
        if not added:
            break
        pos += 1
    return out


def stage_site() -> None:
    """화면4 — 승자 문장의 **사이트 범위**(공통/사이트특이) + 널모델 게이트 + LOPO 전이 검정.

    ── 정의 (수식) ───────────────────────────────────────────────────────────
    그룹 축 g ∈ G = 프로필의 `group_field` (sourceh=카메라 / frames=project, 실측 21개).
    문장 p 의 **승수 기반** 사이트 수 (top1(i) = 프레임 i 의 전역 코사인 1위 문장):
        W(p, g) = |{프레임 i : g(i)=g 이고 top1(i)=p}|
        n_win(p) = |{g : W(p,g) > 0}|
    프레임 i 의 사이트 범위:
        scope(i) = SCOPE(min(3, n_win(top1(i))))

    ⚠️ **"이겼다" = top-1** 로 잡는다. source-h `stage_screens` 는 코드상 `contrib_frames`
       (top-K 규칙에서는 "표를 넣은 문장")를 쓰는데, 그 정의를 frames 에 그대로 옮기면
       **축이 죽는다**: K=10 이면 프레임당 10문장이 기여로 잡혀 문장 하나가 온 project 에
       닿는다 (합성 검증에서 전 문장이 '공통 (3곳+)' 로 포화 — 계획서 §0-4 게이트 ③
       "한 범주가 90% 넘지 않는가" 위반). 그리고 source-h 의 74.6% 불일치 실측은 **argmax 시절**
       측정이라 그때의 `contrib_frames` == top-1 승수였다. 즉 top-1 기준이 "측정된 정의"를
       보존하는 쪽이고, 규칙(topk/argmax)이 바뀌어도 값이 안 변한다는 이점까지 딸려온다
       (`top1_gidx` 의 규칙 불변성은 `_selftest_topk_ranks` ②가 고정).
       top-K 기여 기준은 버리지 않고 `n_<plural>_contrib` 컬럼 + 로그로 남긴다.
    ⚠️ **`reach_g > 0` 기준을 쓰면 안 된다** — source-h 실측에서 실제 승수 기준과 승자 201개 중
       74.6% 가 불일치했고 **전부 한 방향**이었다(reach 가 "공통"으로 과대포장, 119건 vs 0건).
       reach 는 "그 사이트 어딘가에서 1등이 될 수 있었다"는 잠재력이라 별 컬럼
       (`n_<plural>_reach`)으로 같이 낸다 — 계획서 §3 이 지목한 `bank_reach_stream(groups=)`
       산출이 그것이고, 두 기준의 불일치율을 로그에 남겨 frames 판으로 재측정한다.

    ── 널모델 (게이트 ②, 계획서 §0-4 "화면4가 project 색칠과 닮으면 폐기") ───
    ① 순열: 프레임→그룹 사상만 무작위로 섞고(문장×프레임 기여쌍은 그대로) n_win 을 다시 센다.
       그룹 크기 분포는 보존되므로, 관측 공통비율이 순열과 같으면 site_scope 는
       **"큰 project 가 많은 문장을 먹는다"의 재인코딩**이다 → 화면4 폐기.
       판정: z = (obs − mean_null)/sd_null 과 단측 경험 p. **obs 가 null 보다 낮아야**
       (공통이 덜 나와야) 사이트 정보가 있다.
    ② 재인코딩: 프레임 단위 (scope, group) 의 Cramér's V + scope→group 다수결 정확도 vs
       최빈 group 기저율. V 가 1 에 가깝거나 정확도가 기저율을 크게 웃돌면 project 색칠이다.

    ── LOPO (1-4, D3 의 frames 판) ──────────────────────────────────────────
    각 그룹 q 에 대해 W(q) = {p : W(p,q)>0}, W_out(q) = ∪_{q'≠q} W(q') 로 두고
        recall_out    = |W(q) ∩ W_out(q)| / |W(q)|      다른 곳 승자가 q 승자를 덮는 비율
        jaccard       = |∩| / |∪|
        topN_recall   = |W(q) ∩ TopN| / |W(q)|,  N=|W(q)|, TopN = s_out 상위 N
                        (s_out(p) = p 가 이긴 **다른** 그룹 수, 동점은 out reach 최대값)
                        기대값(무작위 N개) = N/M 을 나란히 찍는다 — 이게 없으면 큰 수가
                        전이 성공처럼 보인다
        frame_cover   = q 의 프레임 중 top1 승자가 W_out(q) 에 있던 비율
                        (= "새 현장의 승자가 이미 다른 현장 승자였나". D3 의 예비군 66% 대응)
        spearman      = ρ(max_{q'≠q} reach_{q'}, reach_q)  — 잠재력 랭킹의 전이
    이벤트가 거의 없는 그룹은 제외가 아니라 **tier 표기**다: `minn_tier(이벤트 예측 프레임 수)`
    (GT 가 아니라 규칙 예측이라 `event_tier` 라는 별 이름을 준다). pooled 요약은
    `reportable` 만 쓰고 나머지도 표에는 남긴다 — 하드코딩 제외 목록을 만들지 않는다.
    """
    import csv as _csv

    import fiftyone as fo

    if PROFILE == "sourceh":
        raise SystemExit("site 는 frames 프로필용이다 — sourceh 은 `screens` 가 같은 일을 "
                         "카메라 축으로 이미 한다 (winner_site_scope_<tag>)")
    version = os.environ.get("BANK_ATTACH", VERSIONS[0])
    path = f"{PROMPT_DIR}/{version}.npz"
    if not os.path.exists(path):
        raise SystemExit(f"뱅크 npz 없음: {path} — 먼저 `bank --csv <csv> --version {version}`")
    gfield = PROFILES[PROFILE]["group_field"]
    plural = PROFILES[PROFILE]["group_plural"]
    SL = scope_labels(PROFILES[PROFILE]["group_unit"])
    tag = vtag(version)

    keys, X, gt, src = load_matched()
    if not keys:
        raise SystemExit("site: 프레임 0장 — 원장/임베딩 먼저 확인")
    groups = load_groups(keys)
    tier = gt_tier(gt)
    log(f"site {version}: tier={tier} (이 스테이지 전 지표는 GT-free — tier 는 오독 방지 표기) "
        f"/ 규칙 {RULE}(k={RULE_K}) / 그룹축 {gfield}")

    # 그룹별로 프레임을 **먼저 정렬**한다. reach 의 그룹 누적이 배치마다 21개 boolean 마스크로
    # 타일을 복사하는데, 정렬해 두면 한 배치에 그룹이 1~2개만 걸려 그 복사가 사라진다
    # (값은 그룹 내 max 라 순서 불변 — 재정렬은 결과를 바꾸지 않는다).
    gnames = sorted(set(groups.tolist()))
    gcode_of = {g: i for i, g in enumerate(gnames)}
    gcode = np.array([gcode_of[g] for g in groups], dtype=np.int64)
    order = np.argsort(gcode, kind="stable")
    keys = [keys[i] for i in order]
    X, gt, src, groups, gcode = X[order], gt[order], src[order], groups[order], gcode[order]
    NG, N = len(gnames), len(keys)
    sizes = np.bincount(gcode, minlength=NG)
    log(f"site {version}: 프레임 {N:,} / {gfield} {NG}개 "
        f"(최대 {gnames[int(sizes.argmax())]} {int(sizes.max()):,} / 최소 "
        f"{gnames[int(sizes.argmin())]} {int(sizes.min()):,})")

    bank = load_bank(version)                      # 문장은 DB 정본 (load_bank 주석)
    M = len(bank["cls"])
    pr = _Pruner(X, gt, bank)
    state = pr.score(None)
    pred = pr._pred_of(state)
    win_g = pr.top1_gidx(state)
    frames_all = np.arange(N, dtype=np.int64)
    W = group_win_matrix(win_g, frames_all, gcode, M, NG)      # 정본: top-1 승수
    n_win = (W > 0).sum(1)
    is_winner = W.sum(1) > 0
    sent_idx, frame_idx = contrib_pairs(pr, state)             # 진단: top-K 기여
    Wc = group_win_matrix(sent_idx, frame_idx, gcode, M, NG)
    n_contrib = (Wc > 0).sum(1)
    log(f"site {version}: 문장 {M:,} 중 승자(top1) {int(is_winner.sum()):,} / "
        f"기여(top-K) {int((Wc.sum(1) > 0).sum()):,} / 기여쌍 {len(sent_idx):,}")
    sat = float((n_contrib[Wc.sum(1) > 0] >= min(3, NG)).mean()) if (Wc.sum(1) > 0).any() else 0.0
    log(f"site {version}: [정의 대조] 기여기준으로 재면 '공통(3{PROFILES[PROFILE]['group_unit']}+)' 이 "
        f"{sat:.1%} — 90% 를 넘으면 그 기준은 축이 죽은 것이다(게이트 ③). scope 는 top-1 기준")

    # reach (잠재력) — 계획서 §3 의 `bank_reach_stream(groups=project)`
    best = pr.best_of(state)
    reach, reach_g = bank_reach_stream(X, bank, best, groups=groups)
    R = np.stack([reach_g[g] for g in gnames], axis=1)
    n_reach = (R > 0).sum(1)
    dis = int((n_win[is_winner] != n_reach[is_winner]).sum())
    over = int((n_reach[is_winner] > n_win[is_winner]).sum())
    log(f"site {version}: 승수기준 vs reach>0 기준 불일치 {dis:,}/{int(is_winner.sum()):,} "
        f"({dis / max(1, int(is_winner.sum())):.1%}) — 그중 reach 과대 {over:,} / 과소 {dis - over:,}"
        " (source-h 실측은 74.6% 불일치·전부 과대. scope 는 승수기준을 쓴다)")

    scope_code = np.minimum(3, np.maximum(1, n_win[win_g]))
    scope = np.array([SL[int(c)] for c in scope_code], dtype=object)
    fr_dist = collections.Counter(scope.tolist())
    st_dist = collections.Counter(int(min(3, v)) for v in n_win[is_winner])
    log(f"site {version}: scope(프레임) {dict(fr_dist)}")
    log(f"site {version}: scope(문장, 승자분) {dict(sorted(st_dist.items()))} / "
        f"공통(≥2{PROFILES[PROFILE]['group_unit']}) 문장 "
        f"{int((n_win[is_winner] >= 2).sum()):,} ({(n_win[is_winner] >= 2).mean():.1%})")

    # ── 널모델 ① 순열 ──
    # 문장→프레임 승수 관계는 **그대로 두고** 프레임→그룹 사상만 섞는다. 그룹 크기 분포가
    # 보존되므로, 관측이 순열과 같으면 "많이 이긴 문장이 자동으로 여러 곳에 걸린다"는
    # 크기 효과만 본 것이다.
    rng = np.random.default_rng(51)
    obs_share = float((n_win[is_winner] >= 2).mean())
    obs_mean = float(n_win[is_winner].mean())
    obs_fr = float((scope_code == 1).mean())
    null_share, null_mean, null_fr = [], [], []
    for _ in range(SITE_NULL_PERM):
        gp = gcode[rng.permutation(N)]
        Wp = group_win_matrix(win_g, frames_all, gp, M, NG)
        nwp = (Wp > 0).sum(1)
        null_share.append(float((nwp[is_winner] >= 2).mean()))
        null_mean.append(float(nwp[is_winner].mean()))
        null_fr.append(float((np.minimum(3, np.maximum(1, nwp[win_g])) == 1).mean()))

    def _z(obs: float, arr: list, direction: str) -> dict:
        """관측 vs 순열분포. `direction` = 사이트 정보가 있을 때 관측이 향하는 쪽.

        `p_lower` = P(null ≤ obs) 의 경험값(+1 보정). lower 방향 지표에서는 작을수록,
        higher 방향 지표에서는 클수록 사이트 정보가 있다는 뜻이다 — 부호를 헷갈리기 쉬워
        방향을 값 옆에 같이 싣는다.
        """
        a = np.asarray(arr, dtype=np.float64)
        sd = float(a.std(ddof=1)) if len(a) > 1 else 0.0
        return {"obs": obs, "null_mean": float(a.mean()), "null_sd": sd,
                "z": (obs - float(a.mean())) / sd if sd > 0 else None,
                "p_lower": float((a <= obs).sum() + 1) / (len(a) + 1),
                "direction": direction}

    null = {"n_perm": SITE_NULL_PERM,
            "share_common_sentences": _z(obs_share, null_share, "lower_is_site_specific"),
            "mean_n_groups_win": _z(obs_mean, null_mean, "lower_is_site_specific"),
            "share_site_specific_frames": _z(obs_fr, null_fr, "higher_is_site_specific")}
    s = null["share_common_sentences"]
    log(f"site {version}: [널모델①] 공통문장 비율 관측 {s['obs']:.3f} vs 순열 "
        f"{s['null_mean']:.3f}±{s['null_sd']:.3f} (z={s['z'] if s['z'] is None else round(s['z'], 2)}, "
        f"p_lower={s['p_lower']:.3f}, perm={SITE_NULL_PERM})")

    # ── 널모델 ② 재인코딩 (scope ↔ group) ──
    sc_idx = (scope_code - 1).astype(np.int64)
    v_cram = _cramers_v(sc_idx, gcode, 3, NG)
    acc, base = _predict_acc(sc_idx, gcode, 3, NG)
    null["reencoding"] = {"cramers_v": v_cram, "scope_to_group_acc": acc,
                          "majority_group_baseline": base, "lift": acc - base}
    log(f"site {version}: [널모델②] scope↔{gfield} Cramér's V {v_cram:.3f} / "
        f"scope→{gfield} 다수결 정확도 {acc:.1%} vs 최빈 기저율 {base:.1%} "
        f"(lift {acc - base:+.1%}) — 1.0/큰 lift 면 {gfield} 색칠의 재인코딩이다")

    # ── 게이트 판정 (계획서 §0-4) — 해석을 사람에게 미루지 않는다 ──
    # 경계값의 출처:
    #   ① z ≤ −2 / p_lower ≤ 0.05 — 순열 50회에서 얻을 수 있는 최소 p 가 1/51≈0.020 이라
    #      0.05 는 이 표본에서 의미가 있다. 방향은 "관측이 더 사이트특이" 쪽.
    #   ② acc ≥ 0.84 — 이 파일이 이미 기각한 축들의 실측 대역이다 (err_cluster→camera
    #      0.84~0.91, emb_viz 좌표→camera 0.998). 같은 자로 재야 일관된다.
    #   ③ 한 범주 ≥ 90% — 계획서 §0-4 게이트 ③ 문구 그대로.
    n_fr = sum(fr_dist.values())
    top_share = max(fr_dist.values()) / n_fr if n_fr else 1.0
    verdict = {
        "perm_pass": bool(s["z"] is not None and s["z"] <= -2 and s["p_lower"] <= 0.05),
        "not_reencoding_pass": bool(acc < 0.84 and v_cram < 0.8),
        "category_balance_pass": bool(top_share < 0.90),
        "max_scope_share": top_share,
        "thresholds": {"z": -2, "p_lower": 0.05, "scope_to_group_acc": 0.84,
                       "cramers_v": 0.8, "max_category_share": 0.90},
    }
    verdict["adopt"] = bool(verdict["perm_pass"] and verdict["not_reencoding_pass"]
                            and verdict["category_balance_pass"])
    null["verdict"] = verdict
    log(f"site {version}: [게이트] 순열 {'통과' if verdict['perm_pass'] else '실패'} / "
        f"비재인코딩 {'통과' if verdict['not_reencoding_pass'] else '실패'} / "
        f"범주균형 {'통과' if verdict['category_balance_pass'] else '실패'}"
        f"(최대범주 {top_share:.1%}) → 화면4 "
        f"{'채택' if verdict['adopt'] else '**폐기 권고** (계획서 §0-4)'}")

    # ── LOPO ──
    # 이벤트 클래스는 **뱅크에서** 읽는다 — 모듈 상수 `EVENT_CLASSES`(1,2,3) 는 source-h 4클래스
    # 시절 값이고 frames 프로필엔 smoking(4) 이 있다. tier 분모를 상수로 박으면 smoking 뱅크에서
    # 이벤트 프레임이 통째로 안 세어진다.
    ev_cls = [c for c in sorted(set(bank["cls"].tolist())) if c != 0]
    ev = np.isin(pred, ev_cls)
    log(f"site {version}: 이벤트 클래스 {[CLASS_NAMES.get(c, str(c)) for c in ev_cls]} / "
        f"이벤트 예측 프레임 {int(ev.sum()):,} ({ev.mean():.1%}) — event_tier 의 분모")
    rows_lopo = []
    for qi, q in enumerate(gnames):
        inq = gcode == qi
        win_q = W[:, qi] > 0
        others = [j for j in range(NG) if j != qi]
        win_out = (W[:, others] > 0).any(1) if others else np.zeros(M, dtype=bool)
        s_out = (W[:, others] > 0).sum(1) if others else np.zeros(M, dtype=np.int64)
        r_out = R[:, others].max(1) if others else np.full(M, -np.inf, dtype=np.float32)
        n_q = int(win_q.sum())
        inter = int((win_q & win_out).sum())
        union = int((win_q | win_out).sum())
        n_ev = int(ev[inq].sum())
        # 동점(같은 s_out) 은 out reach 로 가른다 — 랜덤 순서면 topN 이 실행마다 흔들린다
        rank = np.lexsort((-r_out, -s_out))[:n_q]
        top_mask = np.zeros(M, dtype=bool)
        top_mask[rank] = True
        rows_lopo.append({
            "tier": tier, "rule": RULE, gfield: q,
            "n_frames": int(inq.sum()),
            # ⚠️ 크기 교락을 읽을 수 있게 **비중을 같은 행에 싣는다**. 초대형 그룹을 홀드아웃하면
            #    남는 풀이 그만큼 작아져 `frame_coverage_out`·`recall_out` 이 구조적으로 낮게
            #    나온다 (frames 실측: cohort-b 가 전체의 39%). 크기 축 없이 이 수치들을
            #    나란히 읽으면 "이 사이트가 특이하다"로 오독한다.
            "frame_share": round(float(inq.mean()), 5),
            "n_frames_out": int((~inq).sum()),
            "n_event_pred": n_ev,
            "event_rate": round(float(ev[inq].mean()) if inq.any() else 0.0, 5),
            "event_tier": event_tier(n_ev),
            "n_winners": n_q,
            "n_winners_out": int(win_out.sum()),
            "recall_out": round(inter / n_q, 5) if n_q else None,
            "precision_out": round(inter / max(1, int(win_out.sum())), 5),
            "jaccard": round(inter / union, 5) if union else None,
            "topN_recall": round(int((win_q & top_mask).sum()) / n_q, 5) if n_q else None,
            "topN_recall_random": round(n_q / M, 5) if n_q else None,
            "frame_coverage_out": round(float(win_out[win_g[inq]].mean()), 5) if inq.any() else None,
            "spearman_reach_out": round(_spearman(r_out.astype(np.float64),
                                                  R[:, qi].astype(np.float64)), 4),
        })
    rep = [r for r in rows_lopo if r["event_tier"] == "reportable"]
    pool = rep or rows_lopo

    def _avg(k: str) -> float | None:
        vals = [r[k] for r in pool if r[k] is not None]
        return round(float(np.mean(vals)), 4) if vals else None

    summary = {"n_groups": NG, "n_reportable": len(rep),
               "pool": "reportable" if rep else "all(reportable 0)",
               # 크기 교락 경고를 요약에도 남긴다 — 요약만 인용될 때가 가장 위험하다
               "max_frame_share": round(float(sizes.max() / N), 5),
               "max_group": gnames[int(sizes.argmax())],
               "size_confound_note": ("frame_coverage_out·recall_out 은 홀드아웃 그룹이 클수록 "
                                      "잔여 풀이 줄어 낮아진다 — 행별 frame_share 와 같이 읽을 것"),
               **{k: _avg(k) for k in ("recall_out", "jaccard", "topN_recall",
                                       "topN_recall_random", "frame_coverage_out",
                                       "spearman_reach_out")}}
    log(f"site {version}: [LOPO] {gfield} {NG}개 중 event_tier=reportable {len(rep)}개 "
        f"({summary['pool']} 기준 평균) recall_out {summary['recall_out']} / "
        f"topN {summary['topN_recall']} vs 무작위 {summary['topN_recall_random']} / "
        f"frame_cover {summary['frame_coverage_out']} / ρ(reach) {summary['spearman_reach_out']}")
    for r in sorted(rows_lopo, key=lambda r: -r["n_frames"])[:5]:
        log(f"site {version}: [LOPO] {r[gfield]} n={r['n_frames']:,}({r['frame_share']:.1%}) "
            f"tier={r['event_tier']}(ev {r['n_event_pred']:,}) 승자 {r['n_winners']:,} "
            f"recall_out {r['recall_out']} / frame_cover {r['frame_coverage_out']}")

    # ── FiftyOne 반영 ──
    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    ids = key_to_ids(ds, keys)
    ok = [i for i, x in enumerate(ids) if x]
    if not ok:
        raise SystemExit(f"site: 원장 key 가 데이터셋과 하나도 안 붙는다 "
                         f"(key_join={PROFILES[PROFILE]['key_join']})")
    if len(ok) < len(ids):
        log(f"site: FiftyOne 매칭 {len(ok):,}/{len(ids):,} — 나머지는 필드 미설정")
    sch = ds.get_field_schema()
    for fld in ("winner_site_scope", "winner_n_sites"):
        if fld in sch:
            ds.clear_sample_field(fld)             # clear-then-set (stale 값 금지)
    pairs = [(ids[i], i) for i in ok]
    set_values_batched(ds, "winner_site_scope", pairs,
                       lambda i: fo.Classification(label=scope[i]))
    # 숫자 축은 Int 로도 내린다 — App 이 Query Performance 모드라 String/Classification 은
    # 자유텍스트 검색으로만 뜨고 "≥2곳" 같은 필터가 안 된다 (winner_gidx 와 같은 이유).
    set_values_batched(ds, "winner_n_sites", pairs, lambda i: int(n_win[win_g[i]]))

    from fiftyone.core.odm.dataset import ActiveFields
    vt = version.replace(".", "_")
    if not vt.startswith("v"):
        vt = "v" + vt
    # ⚠️ Color by 대상(`winner_site_scope`)은 반드시 active_fields 에 있어야 한다.
    #    `winner_n_sites` 는 **일부러 뺀다** — 필터용 Int 이고 그리드 칩으로 뜨면 자리만 먹는다.
    active = set_active_fields(ds, ActiveFields,
                               ["winner_site_scope", f"pred_{vt}", "attached_bank", gfield])

    ds.info = {**(ds.info or {}),
               f"site_run_{tag}": {"rule": RULE, "k": RULE_K, "bank": version,
                                   "gt_tier": tier, "group_field": gfield, "n_groups": NG,
                                   "null": null, "lopo": summary}}
    ds.save()

    # ── 산출물 ──
    os.makedirs(GEO, exist_ok=True)
    os.makedirs(REPORT_DIR, exist_ok=True)
    with open(f"{GEO}/site.json", "w", encoding="utf-8") as f:
        json.dump({"gt_tier": tier, "gt_free": True, "version": version,
                   "rule": RULE, "k": RULE_K, "group_field": gfield,
                   "scope_basis": "top1_win",   # ≠ top-K 기여. docstring 의 정의 주석 참조
                   "n_frames": N, "n_prompts": M, "groups": gnames,
                   "n_winners": int(is_winner.sum()),
                   "contrib_basis_saturation": sat,
                   "group_sizes": {g: int(sizes[i]) for i, g in enumerate(gnames)},
                   "scope_frames": {k: int(v) for k, v in fr_dist.items()},
                   "scope_sentences": {str(k): int(v) for k, v in sorted(st_dist.items())},
                   "win_vs_reach_disagree": {"n": dis, "n_winners": int(is_winner.sum()),
                                             "reach_over": over},
                   "null_model": null, "lopo_summary": summary, "lopo": rows_lopo},
                  f, ensure_ascii=False, indent=1)
    lp = f"{REPORT_DIR}/site_lopo_{version}.csv"
    with open(lp, "w", newline="", encoding="utf-8") as f:
        w = _csv.DictWriter(f, fieldnames=list(rows_lopo[0]), extrasaction="ignore")
        w.writeheader()
        w.writerows(rows_lopo)
    sp = f"{REPORT_DIR}/site_prompts_{version}.csv"
    cols = ["tier", "gidx", "cls", "cls_name", "wins", "contribs",
            f"n_{plural}_win", f"n_{plural}_contrib", f"n_{plural}_reach",
            "site_scope", "reach", *[f"reach_{g}" for g in gnames],
            *[f"wins_{g}" for g in gnames], "text"]
    with open(sp, "w", newline="", encoding="utf-8") as f:
        w = _csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for g in range(M):
            c = int(bank["cls"][g])
            row = {"tier": tier, "gidx": g, "cls": c, "cls_name": CLASS_NAMES.get(c, str(c)),
                   "wins": int(W[g].sum()), "contribs": int(Wc[g].sum()),
                   f"n_{plural}_win": int(n_win[g]),
                   f"n_{plural}_contrib": int(n_contrib[g]),
                   f"n_{plural}_reach": int(n_reach[g]),
                   "site_scope": SL[int(min(3, max(1, n_win[g])))] if W[g].sum() else "미승리",
                   "reach": round(float(reach[g]), 5), "text": bank["prompt"][g]}
            for j, nm in enumerate(gnames):
                row[f"reach_{nm}"] = round(float(R[g, j]), 5)
                row[f"wins_{nm}"] = int(W[g, j])
            w.writerow(row)
    # 저장뷰/워크스페이스는 **산출물 기록 뒤**에 만든다 — App UI 층 실패(slug 충돌 등)가
    # 45초짜리 분석 결과(site.json/CSV)까지 날린 실사고(2026-08-18)의 재발 방지.
    # 저장뷰 — Embeddings 패널 5,000점 상한이라 **뷰로 좁힌다** (계획서 §4)
    spec_idx = [i for i in ok if scope_code[i] == 1]
    v_spec = _stratified_ids(ids, spec_idx, groups, SITE_SCREEN_CAP)
    mixed_strata = np.array([f"{groups[i]}|{scope_code[i]}" for i in range(len(keys))])
    v_mix = _stratified_ids(ids, ok, mixed_strata, SITE_SCREEN_CAP)
    # ⚠️ FiftyOne 저장뷰 slug 는 ASCII 만 남긴다 — 한글만 다른 두 이름이 같은 slug
    #    ('04-projects')로 접혀 ValueError 가 난 실사고. 유일성은 ASCII 부분(04/04b)이 담당.
    for nm, sel_ids, desc in (
        (f"04_사이트특이_{plural}", v_spec,
         f"{version} 승자가 1{PROFILES[PROFILE]['group_unit']}에서만 이기는 프레임 "
         f"({len(spec_idx):,}장 중 {gfield} 층화 {len(v_spec):,}장 — Embeddings 5,000 상한)"),
        (f"04b_사이트범위_표본_{plural}", v_mix,
         f"{gfield}×scope 층화 표본 {len(v_mix):,}장 — 화면4 Color by winner_site_scope"),
    ):
        if nm in ds.list_saved_views():
            ds.delete_saved_view(nm)
        if not sel_ids:                            # 빈 뷰를 저장하면 화면4 가 조용히 백지가 된다
            log(f"site: ⚠️ 뷰 {nm} 대상 0장 — 저장하지 않는다")
            continue
        ds.save_view(nm, ds.select(sel_ids, ordered=True), description=desc)

    brain = _pick_brain(ds)
    if brain is None:
        log("site: ⚠️ 시각화 brain run 이 없다 — 화면4 를 Samples 단독으로 만든다 "
            "(Embeddings 패널은 emb_viz 등록 후 다시 site 를 돌리면 붙는다)")
        space = fo.Space(children=[fo.Panel(type="Samples", pinned=True)])
    else:
        space = fo.Space(children=[
            fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
            fo.Space(children=[fo.Panel(type="Embeddings",
                                        state={"brainResult": brain,
                                               "colorByField": "winner_site_scope.label"})]),
        ], orientation="horizontal")
    ws = "4-site"                                  # 워크스페이스명 ASCII (App slug 함정)
    if ws in ds.list_workspaces():
        ds.delete_workspace(ws)
    ds.save_workspace(ws, space,
                      description=f"{brain or 'Samples'} (색: winner_site_scope) — {version}")
    log(f"site: 필드 winner_site_scope/winner_n_sites ({len(ok):,}장) / "
        f"뷰 04_사이트특이_{plural}·04b_사이트범위_표본_{plural} / 워크스페이스 {ws}")
    log(f"site: active_fields {active or 'None(제한 없음, 미변경)'} "
        "— Color by winner_site_scope 는 제한 없음 상태에서도 안전하다")
    log(f"site 완료 → {GEO}/site.json · {lp} · {sp} [tier={tier}, GT-free]")


# ────────────────────── vote (Top-K APO 판정 규칙) ──────────────────────
def bank_vote_stream(X: np.ndarray, bank: dict, k: int,
                     batch: int = 512) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """**전역 top-k 문장의 class 다수결** — APO(Top-K) 의 실제 판정 규칙.

    출처: Notion「스프린트 3-1. gen/prune/추론 루프 설계」§2 —
    *"Top-K APO는 top-k 프롬프트의 class 다수결로 카테고리를 정한다"*, threshold 없음.

    우리 `predict()`(클래스별 best → argmax)는 이 규칙의 **k=1 특수해**다. 전역 1위 문장의
    클래스 = 클래스별 max 의 argmax 이므로 정확히 같다. 즉 기존 보고서 전체가 k=1 코너였다.
    k 를 올리면 뱅크의 **클래스별 문장 수 비율**이 사전확률로 직접 들어온다
    (v080 normal 85.8%/falldown 1.3% vs v084 53.5%/18.6%) — 규칙 전환이 결론을 바꿀 수 있다.

    ⚠️ 동표 처리는 문서에 규정이 없다. 여기서는 **그 클래스가 top-k 안에서 낸 최고 코사인**
    으로 깬다. `(topc+2)/10 ∈ [0, 0.3] < 1표` 라 코사인이 표차를 뒤집는 일은 없다.

    반환: (pred, votes[N,C], margin). margin = (1위표 − 2위표)/k — GT 불필요한 확신도.
    """
    V, cls = bank["vec"], bank["cls"]
    classes = sorted(set(cls.tolist()))
    n, C = X.shape[0], len(classes)
    k = min(k, V.shape[0])
    votes = np.zeros((n, C), dtype=np.int32)
    topc = np.full((n, C), -2.0, dtype=np.float32)
    for s in range(0, n, batch):
        S = X[s:s + batch] @ V.T                              # [b, M] 타일만 상주
        idx = np.argpartition(-S, k - 1, axis=1)[:, :k]        # top-k (내부 순서 무관)
        sc = np.take_along_axis(S, idx, axis=1)
        cs = cls[idx]
        for j, c in enumerate(classes):
            m = cs == c
            votes[s:s + batch, j] = m.sum(axis=1)
            topc[s:s + batch, j] = np.where(m, sc, -2.0).max(axis=1)
    o = np.sort(votes, axis=1)
    margin = ((o[:, -1] - o[:, -2]) / k).astype(np.float32)
    key = votes + (topc + 2.0) / 10.0
    return np.array(classes)[key.argmax(axis=1)], votes, margin


def prf(pred: np.ndarray, gt: np.ndarray) -> dict:
    """클래스별 P/R/F1 + macro-F1 + micro.

    ⚠️ `recalls()` 의 macro 는 **평균 recall** 이라 희소 클래스를 과다예측하면 precision 을
    버리고 올라간다 (실측: v080 k=1→50 에서 smoke 예측 1,991→2,705, GT 1,163). 회사 리포트는
    전부 F1 이므로 규칙을 맞춰도 지표가 다르면 여전히 비교 불가다 → 여기서 F1 을 같이 낸다.
    """
    out = {}
    for c in sorted(set(gt.tolist()) | set(pred.tolist())):
        tp = int(((pred == c) & (gt == c)).sum())
        p = tp / max(1, int((pred == c).sum()))
        r = tp / max(1, int((gt == c).sum()))
        out[CLASS_NAMES.get(int(c), str(c))] = {
            "P": p, "R": r, "F1": 0.0 if p + r == 0 else 2 * p * r / (p + r)}
    return {"micro": float((pred == gt).mean()),
            "macro_f1": float(np.mean([v["F1"] for v in out.values()])),
            "macro_recall": float(np.mean([v["R"] for v in out.values()])),
            "per_class": out}


def stage_vote() -> None:
    """APO(Top-K 다수결) 규칙으로 두 뱅크를 재채점 + FiftyOne 반영.

    `attach`/`flips`/`prune` 이 쓰는 argmax 는 이 규칙의 k=1 이다. 여기서는 k 를 훑어
    **규칙 축에서 결론이 유지되는지**를 본다. k 는 문서에 값이 없어 스윕이 유일한 정직한 방법.

    붙는 필드: `vote_<vt>`(Classification, confidence=득표율) · `vote_margin_<tag>` ·
    `rule_flip_<tag>`(k=1 ↔ k=K 판정이 갈린 프레임의 "argmax→vote" 레이블)
    """
    import fiftyone as fo
    from fiftyone import ViewField as F

    ks = [int(x) for x in os.environ.get("VOTE_KS", "1,3,5,10,20,50").split(",")]
    kmain = int(os.environ.get("VOTE_K", "10"))
    if kmain not in ks:
        ks.append(kmain)
    ks = sorted(set(ks))

    keys, X, gt, src, banks = load_all()
    log(f"vote: 프레임 {len(keys):,} / k 스윕 {ks} / 주 k={kmain}")
    out, cache = {}, {}
    for v in VERSIONS:
        bank = banks[v]
        ratio = {CLASS_NAMES[c]: float((bank["cls"] == c).mean())
                 for c in sorted(set(bank["cls"].tolist()))}
        log(f"vote {v}: 문장 {len(bank['cls']):,} / 클래스 비율 "
            + " ".join(f"{n}={r:.1%}" for n, r in ratio.items()))
        rows = {}
        for k in ks:
            pred, votes, margin = bank_vote_stream(X, bank, k)
            m = prf(pred, gt)
            rows[k] = dict(m, pred_dist={CLASS_NAMES[c]: int((pred == c).sum())
                                         for c in sorted(CLASS_NAMES)})
            if k in (1, kmain):
                cache[(v, k)] = (pred, margin)
            log(f"vote {v} k={k:<3} micro={m['micro']:.2%} macroF1={m['macro_f1']:.2%} "
                f"macroR={m['macro_recall']:.2%}  "
                + " ".join(f"{n}=F1 {d['F1']:.2f}(P{d['P']:.2f}/R{d['R']:.2f})"
                           for n, d in m["per_class"].items()))
        out[v] = {"class_ratio": ratio, "n_prompts": int(len(bank["cls"])), "by_k": rows}

        # 불변식: k=1 다수결 ≡ argmax(클래스별 best) — 규칙 재현이 맞는지의 유일한 자기검증
        b, _ = bank_best_stream(X, bank)
        assert (cache[(v, 1)][0] == predict(b)).all(), f"{v}: k=1 vote ≠ argmax — 규칙 재현 오류"
        log(f"vote {v}: 불변식 OK (k=1 다수결 ≡ argmax)")

    with open(f"{GEO}/vote.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)

    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    key_to_id = {}
    for s in ds.select_fields(["id", "filepath"]):
        key_to_id[f"{os.path.basename(os.path.dirname(s.filepath))}/"
                  f"{os.path.basename(s.filepath)}"] = s.id
    ids = [key_to_id.get(k) for k in keys]
    ok = [i for i, x in enumerate(ids) if x]
    if len(ok) < len(ids):
        log(f"vote: FiftyOne 매칭 {len(ok)}/{len(ids)}")

    active = ["ground_truth"]
    for v in VERSIONS:
        vt = v.replace(".", "_")
        vt = vt if vt.startswith("v") else "v" + vt
        tag = vtag(v)
        pred1, _ = cache[(v, 1)]
        predk, mk = cache[(v, kmain)]
        for fld in (f"vote_{vt}", f"vote_margin_{tag}", f"rule_flip_{tag}"):
            if fld in ds.get_field_schema():
                ds.clear_sample_field(fld)
        ds.set_values(f"vote_{vt}",
                      {ids[i]: fo.Classification(label=CLASS_NAMES[int(predk[i])],
                                                 confidence=float(mk[i])) for i in ok},
                      key_field="id")
        ds.set_values(f"vote_margin_{tag}",
                      {ids[i]: float(mk[i]) for i in ok}, key_field="id")
        # 규칙 전환으로 판정이 갈린 프레임 — "다시 보여줘"의 핵심 화면
        flip = {ids[i]: fo.Classification(
            label=f"{CLASS_NAMES[int(pred1[i])]}→{CLASS_NAMES[int(predk[i])]}")
            for i in ok if pred1[i] != predk[i]}
        ds.set_values(f"rule_flip_{tag}", flip, key_field="id")
        nm = f"1{VERSIONS.index(v)}_rule_flip_{tag}"
        if nm in ds.list_saved_views():
            ds.delete_saved_view(nm)
        ds.save_view(nm, ds.match(F(f"rule_flip_{tag}.label") != None)   # noqa: E711
                     .sort_by(f"vote_margin_{tag}"),
                     description=f"{v}: argmax(k=1) ↔ 다수결(k={kmain}) 판정 불일치")
        log(f"vote {v}: vote_{vt} / vote_margin_{tag} / rule_flip_{tag} "
            f"({len(flip):,}장 불일치 = {len(flip) / max(1, len(ok)):.1%}) + 뷰 '{nm}'")
        active += [f"vote_{vt}", f"rule_flip_{tag}"]

    from fiftyone.core.odm.dataset import ActiveFields
    keep = [f for f in dict.fromkeys(active + ["camera"]) if f in ds.get_field_schema()]
    ds.app_config.active_fields = ActiveFields(paths=keep, exclude=False)
    ds.save()
    log(f"vote: active_fields {keep}")
    log(f"vote 완료 → {GEO}/vote.json")


# ────────────────────── screens ──────────────────────
# APO 개선용 FiftyOne 화면의 프레임 필드. 계획: docs/apo-fiftyone-plan-2026-08-03.md
#
# ⚠️ 색칠 후보 중 **기각된 것** (2026-08-03 적대적 검증, 되살리지 말 것):
#   · error_type — 생성후보 뷰 안에서 GT 의 완전한 재인코딩 (FP ≡ pool∧gt=normal, 845=845 항등식)
#   · err_cluster — 임베딩 KMeans 가 카메라 재인코딩 (cluster→camera acc 0.84~0.91, AMI 0.48~0.50).
#                   공짜인 3×2 camera×{FP,FN} 교차표와 같은 정보
#   · margin_bin("라벨 의심") — 절대컷 0.005 는 뱅크의 성질이지 프레임의 성질이 아님
#                   (v080 11.5%분위 vs v084 8.1%, 같은 프레임 Jaccard 0.177, 정규화 4종 모두 무효).
#                   게다가 오류 신호 방향이 클래스마다 뒤집힌다 — AUC GT=normal 0.886 vs GT=fire 0.009.
#                   "라벨 의심"이라는 이름도 근거 없음: relabel 뒤집힘 lift 0.92배, AUC 0.409
#   · emb_viz 위 어떤 색칠이든 — 좌표→카메라 kNN 예측력 0.998. 보이는 덩어리는 1순위 카메라다
def scope_labels(unit: str) -> dict[int, str]:
    """사이트 범위 라벨 — 단위 세는 말만 프로필이 갈아 끼운다 (카메라 "대" / project "곳").

    ⚠️ sourceh 문자열은 **바꾸지 않는다**: `stage_screens` 의 저장뷰 `04_사이트특이_<tag>` 가
       `SCOPE_LABELS[1]` 과 문자 등식으로 매칭한다 (라벨을 고치면 뷰가 조용히 0건이 된다).
    """
    return {1: f"사이트특이 (1{unit})", 2: f"공통 (2{unit})", 3: f"공통 (3{unit}+)"}


SCOPE_LABELS = scope_labels("대")          # sourceh(카메라) 정본


def score_p(win_frames: dict, cls_of, gt: np.ndarray, normal_cls: int = 0) -> dict:
    """APO 의 프롬프트 개별점수. **높을수록 삭제 1순위** (Notion [APO 스프린트] 1-2).

        Score_p = FP_p/N_normal   − TP_p/N_abnormal    (abnormal 프롬프트)
                = FN_p/N_abnormal − TN_p/N_normal      (normal 프롬프트)

    APO 는 이 점수 상위 K=40 을 G_del 로 잡아 유전 알고리즘의 탐색 시작점으로 쓴다.
    여기서는 그 G_del 을 그대로 재현해 "APO 가 지우려는 집합이 프레임에 어떤 영향을 주나"를 본다.
    (우리 `loo_gain` 과는 다른 지표다 — Score_p 는 비율 차, LOO 는 실제 제거 counterfactual.)
    """
    n_norm = int((gt == normal_cls).sum())
    n_abn = int((gt != normal_cls).sum())
    out = {}
    for g, fr in win_frames.items():
        is_norm_prompt = cls_of(g) == normal_cls
        gt_norm = int((gt[fr] == normal_cls).sum())
        gt_abn = len(fr) - gt_norm
        if is_norm_prompt:                      # normal 프롬프트: 이벤트를 normal 로 삼키면 FN
            out[g] = gt_abn / max(1, n_abn) - gt_norm / max(1, n_norm)
        else:                                   # abnormal 프롬프트: normal 을 가져가면 FP
            out[g] = gt_norm / max(1, n_norm) - gt_abn / max(1, n_abn)
    return out


def stage_probecache() -> None:
    """프롬프트 프로브가 App 안에서 즉답하도록 **top-K 판정 상태를 프레임 필드로 캐시**한다.

    App 오퍼레이터는 뱅크(수만 문장 × 1024-d)를 못 올린다. 대신 프레임마다 네 값만 있으면
    "이 후보 문장을 넣으면 판정이 어떻게 바뀌나"를 정확히 계산할 수 있다:

      probe_bar_<tag>   : top-K 의 **마지막 코사인** = 진입 기준선. 후보가 이걸 넘어야 표가 된다
      probe_votes_<tag> : 클래스별 현재 득표 [normal, falldown, fire, smoke]
      probe_topc_<tag>  : 클래스별 top-K 내 최고 코사인 (동표 해소용 — 규칙과 동일)
      probe_out_<tag>   : 진입 시 **밀려나는** 문장의 클래스 (= K위 문장의 클래스)

    후보 코사인 c 가 bar 를 넘으면 votes[cand]+1 / votes[out]−1 로 갱신하고 같은 tie-break
    (votes + (topc+2)/10) 를 적용하면 **실제 재채점과 정확히 같은 예측**이 나온다.
    """
    import fiftyone as fo

    version = os.environ.get("BANK_ATTACH", VERSIONS[0])
    keys, X, gt, src, banks = load_all()
    if version.lower() in ("all", "__all__", "전체"):
        # 「전체」 = 뱅크 **합집합** 위의 top-K. App 드롭다운에서 prompt-compare 의 '전체' 와
        # 같은 모수를 프로브·문장생성에도 주기 위한 것이다 (뱅크가 2벌인데 프로브는 1벌만
        # 보이던 문제, 2026-08-12 사용자 지적).
        # ⚠️ 합집합은 **별개 뱅크**다. 순진 병합이 단일 뱅크보다 나빴던 실측이 있으므로
        #    후보 문장을 넓은 모수에서 보려는 용도로만 쓰고, 제품 성능으로 인용하지 말 것.
        tag = "all"
        version = "+".join(BANKS)
        bank = {"vec": np.concatenate([banks[v]["vec"] for v in BANKS], axis=0),
                "cls": np.concatenate([banks[v]["cls"] for v in BANKS], axis=0),
                "prompt": [p for v in BANKS for p in banks[v]["prompt"]]}
        log(f"probecache: 합집합 뱅크 {version} → 문장 {len(bank['cls']):,}")
    else:
        tag = vtag(version)
        bank = banks[version]
    vals, idxs = bank_topk_stream(X, bank)
    cs = sorted(vals)
    pred, votes, sel = vote_topk(vals, idxs)
    log(f"probecache {version}: 규칙 {RULE}(k={RULE_K}) / 정답 "
        f"{int((pred == gt).sum()):,}/{len(gt):,} ({(pred == gt).mean():.2%})")

    # sel 은 코사인 내림차순 → 마지막 칸이 진입 기준선이자 밀려날 자리
    kk = sel.shape[1]
    bar = np.full(len(gt), -2.0, dtype=np.float32)
    out_c = np.full(len(gt), -1, dtype=np.int64)
    allv = np.concatenate([vals[c] for c in cs], 1)
    lab = np.concatenate([np.full(vals[c].shape[1], c) for c in cs])
    order = np.argsort(-allv, axis=1)[:, :kk]
    ord_v = np.take_along_axis(allv, order, 1)
    ord_c = lab[order]
    bar[:] = ord_v[:, -1]
    out_c[:] = ord_c[:, -1]
    # ⚠️ 짝은 **이 블록 안에서만** 맞춘다 — 예전엔 `vote_topk` 이 만든 `sel_c` 와 여기서
    #    독립적으로 만든 `order` 를 같은 자리로 짝지어 읽었다. 두 정렬이 동점에서 조금만
    #    갈리면 topc(동표 해소값)가 조용히 틀리는 숨은 결합이었다. `ord_c`/`ord_v` 는 같은
    #    `order` 에서 나오므로 정렬 kind 와 무관하게 항상 짝이 맞고, `max` 는 순서 불변이라
    #    값도 기존과 동일하다.
    topc = np.stack([np.where(ord_c == c, ord_v, -2.0).max(1) for c in cs], 1)
    vlist = [[int(v) for v in row] for row in votes]
    tlist = [[float(v) for v in row] for row in topc]

    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    key_to_id = {}
    for smp in ds.select_fields(["id", "filepath"]):
        key_to_id[f"{os.path.basename(os.path.dirname(smp.filepath))}/"
                  f"{os.path.basename(smp.filepath)}"] = smp.id
    ids = [key_to_id.get(k) for k in keys]
    ok = [i for i, x in enumerate(ids) if x]
    log(f"probecache: FiftyOne 매칭 {len(ok):,}/{len(keys):,}")

    for fld, arr in ((f"probe_bar_{tag}", [float(bar[i]) for i in ok]),
                     (f"probe_out_{tag}", [int(out_c[i]) for i in ok]),
                     (f"probe_votes_{tag}", [vlist[i] for i in ok]),
                     (f"probe_topc_{tag}", [tlist[i] for i in ok])):
        ds.set_values(fld, dict(zip([ids[i] for i in ok], arr)), key_field="id")
    # 클래스 순서를 App 이 알아야 한다 — 데이터셋 info 에 남긴다
    ds.info = {**(ds.info or {}),
               f"probe_classes_{tag}": [CLASS_NAMES[c] for c in cs],
               f"probe_k_{tag}": RULE_K, f"probe_bank_{tag}": version}
    ds.save()
    log(f"probecache: 필드 4종 기록 (probe_*_{tag}) / 클래스 순서 "
        f"{[CLASS_NAMES[c] for c in cs]} / k={RULE_K}")


def stage_gen() -> None:
    """화면1 — APO [Generate] 의 **입력 선정**을 보고 고치는 화면.

    APO 는 에폭마다 오답 이미지에서 **무작위 50장**을 뽑아 PE-lang 에 넣는다. 그 무작위가
    두 가지를 놓친다:
      · 소수 클래스가 통째로 빠진다 (falldown 오답은 전체의 2% 미만 → 50장 중 기대 1장)
      · 같은 장면 연속 프레임이 중복으로 뽑힌다 (프레임 추출이라 이웃끼리 cos>0.99)
    둘 다 "생성된 문장이 특정 조건에 쏠린다"로 이어진다 — 그게 나쁜 자석의 출처다.

    ⚠️ 색칠 후보로 검토했다가 **기각**한 것들 (되살리지 말 것, 근거는 SCOPE_LABELS 위 주석):
    error_type(GT 재인코딩) · err_cluster(카메라 재인코딩) · margin_bin(뱅크의 성질).
    대신 **샘플링 단위 그 자체**(`gen_stratum`)와 **선정 결과**(`gen_pick`)를 칠한다 —
    이 둘은 화면에서 고칠 수 있는 유일한 손잡이다.

    ⚠️ 후보 풀은 이진 FP/FN 이 아니라 **오답 전체**다. fire 를 smoke 로 부른 프레임은
    FP 도 FN 도 아니라 예전 풀에서 빠졌는데, 개념 경계 붕괴라 생성이 가장 필요한 축이다.

    ── 규칙 개작 전수 확인 (2026-08-18) ────────────────────────────────────
    오답 풀은 오늘 `_pred_of` 로 규칙 중립이 됐고, 이 스테이지에 **남은 argmax 계산은
    없다** — 중복제거(코사인)·층화(카메라×GT)·쿼터(층 크기)는 전부 판정규칙과 무관하다.
    남아 있던 건 계산이 아니라 **출처 표기**였다:
      · 어느 규칙으로 만든 오답 풀인지 로그·화면 어디에도 없었다 (규칙을 바꾸면 풀이
        통째로 달라지는데 산출 필드명은 그대로다) → 로그·뷰 설명·`ds.info` 에 박는다.
      · GT 미검수(−1) 행이 `pred != gt` 로 **전부 오답으로 계산**됐다. 부분검수 원장에서는
        생성 후보가 "라벨이 없는 프레임"으로 뒤덮인다 — tier 표기가 그걸 드러낸다.
    """
    import fiftyone as fo

    version = os.environ.get("BANK_ATTACH", VERSIONS[0])
    n_epoch = int(os.environ.get("GEN_EPOCH", "50"))
    dup_thr = float(os.environ.get("GEN_DUP_COS", "0.95"))
    tag = vtag(version)
    keys, X, gt, src, banks = load_all()
    cam = load_cameras(keys)
    bank = load_bank(version)                      # 문장은 DB 정본 (load_bank 주석)

    tier = gt_tier(gt)
    if tier == "no_gt":
        raise SystemExit("gen: tier=no_gt — '오답 풀'은 GT 없이 정의되지 않는다 "
                         "(전 프레임이 오답으로 잡혀 무작위 표집과 다를 게 없어진다). "
                         "원장(gt_class)을 먼저 채울 것")
    pr = _Pruner(X, gt, bank)
    # ⚠️ `score()` 반환은 규칙마다 다르다 (argmax=4-tuple / top-K=dict). 여기서 4-tuple 로
    #    풀면 기본값 RULE=topk 에서 통째로 죽는다 — 규칙을 모르는 채로 pred 만 꺼낸다.
    pred = pr._pred_of(pr.score(None))
    # ⚠️ 미검수(−1)는 오답이 아니라 **모름**이다. 빼지 않으면 풀이 라벨 공백으로 오염된다.
    labeled = gt >= 0
    err = np.flatnonzero(labeled & (pred != gt))
    fp = int(((gt[err] == 0) & (pred[err] != 0)).sum())
    fn = int(((gt[err] != 0) & (pred[err] == 0)).sum())
    log(f"gen {version} [규칙 {RULE}(k={RULE_K}) · GT tier={tier}]: 검수 {int(labeled.sum()):,}"
        f"/{len(gt):,} (미검수 {int((~labeled).sum()):,}장은 풀에서 제외)")
    log(f"gen {version}: 오답 풀 {len(err):,} = FP {fp:,} + FN {fn:,} + "
        f"오분류(이상↔이상) {len(err)-fp-fn:,}  ← 마지막 항이 이진 FP/FN 풀에서 빠져 있던 것")

    # ── 중복 제거: 이웃 프레임이 사실상 같은 그림이라 생성 입력이 낭비된다 ──
    keep, dup = [], set()
    Xe = X[err]
    for a in range(len(err)):
        if a in dup:
            continue
        keep.append(a)
        sim = Xe[a] @ Xe.T
        for b in np.flatnonzero(sim > dup_thr):
            if b != a:
                dup.add(int(b))
    log(f"gen {version}: cos>{dup_thr} 중복 제거 {len(dup):,} → 유효 후보 {len(keep):,}")

    # ── 층화: 카메라 × GT클래스. 무작위가 놓치는 건 '희귀 조합'이지 '희귀 클래스'만이 아니다 ──
    strat = np.array([f"{cam[i]}|{CLASS_NAMES[int(gt[i])]}" for i in err], dtype=object)
    kept = np.array(keep)
    by_s = collections.defaultdict(list)
    for a in kept:
        by_s[strat[a]].append(int(a))
    log(f"gen {version}: 층 {len(by_s)}개 "
        f"{ {k: len(v) for k, v in sorted(by_s.items(), key=lambda kv: -len(kv[1]))} }")

    # 쿼터: ① 비어있지 않은 층마다 최소 1 (층 커버리지) ② 소수 클래스 바닥 ③ 나머지 비례
    rng = np.random.default_rng(0)
    quota = {s: 1 for s in by_s}
    rare = [s for s in by_s if s.endswith(f"|{CLASS_NAMES[1]}")]      # falldown
    floor_rare = int(os.environ.get("GEN_RARE_FLOOR", "5"))
    while rare and sum(quota[s] for s in rare) < floor_rare:
        s = min(rare, key=lambda s: quota[s] / max(1, len(by_s[s])))
        if quota[s] >= len(by_s[s]):
            rare.remove(s)
            continue
        quota[s] += 1
    left = n_epoch - sum(quota.values())
    if left > 0:                                       # 비례 배분 (층 크기 상한)
        tot = sum(len(v) for v in by_s.values())
        for s in sorted(by_s, key=lambda s: -len(by_s[s])):
            add = min(len(by_s[s]) - quota[s], int(round(left * len(by_s[s]) / tot)))
            quota[s] += max(0, add)
        while sum(quota.values()) < n_epoch:
            s = max(by_s, key=lambda s: len(by_s[s]) - quota[s])
            if len(by_s[s]) <= quota[s]:
                break
            quota[s] += 1
        # 비례 배분의 반올림이 합을 넘길 수 있다 → 큰 층부터 깎되 바닥(1)은 지킨다
        while sum(quota.values()) > n_epoch:
            s = max((s for s in by_s if quota[s] > 1), key=lambda s: quota[s], default=None)
            if s is None:
                break
            quota[s] -= 1

    picked = set()
    for s, idxs in by_s.items():
        picked |= set(rng.choice(idxs, min(quota[s], len(idxs)), replace=False).tolist())
    log(f"gen {version}: 쿼터 {dict(sorted(quota.items()))} → 선정 {len(picked)}장")
    got = collections.Counter(CLASS_NAMES[int(gt[err[a]])] for a in picked)
    # ⚠️ CLASS_NAMES 는 dict 라 enumerate 하면 키(0..3)가 나온다 — items() 를 써야 이름이 붙는다
    exp = {nm: round(n_epoch * float((gt[err] == i).mean()), 1) for i, nm in CLASS_NAMES.items()}
    log(f"gen {version}: 클래스 분포 층화 {dict(got)} vs 무작위 기대 {exp}")

    pick = np.empty(len(err), dtype=object)
    pick[:] = "미선정 (풀에만)"
    pick[list(dup)] = "중복제외 (cos>%.2f)" % dup_thr
    pick[list(picked)] = "선정 (이번 에폭)"

    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    key_to_id = {}
    for s in ds.select_fields(["id", "filepath"]):
        key_to_id[f"{os.path.basename(os.path.dirname(s.filepath))}/"
                  f"{os.path.basename(s.filepath)}"] = s.id
    f_st, f_pk = f"gen_stratum_{tag}", f"gen_pick_{tag}"
    for fld in (f_st, f_pk):
        if fld in ds.get_field_schema():
            ds.clear_sample_field(fld)
    vs, vp = {}, {}
    for a, i in enumerate(err):
        sid = key_to_id.get(keys[i])
        if sid:
            vs[sid] = fo.Classification(label=str(strat[a]))
            vp[sid] = fo.Classification(label=str(pick[a]))
    ds.set_values(f_st, vs, key_field="id")
    ds.set_values(f_pk, vp, key_field="id")

    from fiftyone import ViewField as F
    space = fo.Space(children=[
        fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
        fo.Space(children=[fo.Panel(type="Embeddings",
                                    state={"brainResult": "emb_viz",
                                           "colorByField": f"{f_pk}.label"})]),
    ], orientation="horizontal")
    if "1-generate" in ds.list_workspaces():
        ds.delete_workspace("1-generate")
    # 규칙·tier 를 화면 설명에 박는다 — 이 풀은 규칙을 바꾸면 통째로 달라지는데 필드명은 같다
    prov = f"규칙 {RULE}(k={RULE_K}) · GT tier={tier} · 뱅크 {version}"
    ds.save_workspace("1-generate", space,
                      description=f"emb_viz (색: {f_pk}.label) · {prov}")
    for nm, view, desc in (
            (f"01_생성후보_{tag}", ds.match(F(f"{f_pk}.label") != None),          # noqa: E711
             f"오답 {len(err):,}장(중복포함) · {prov}"),
            (f"02_이번에폭_{tag}", ds.match(F(f"{f_pk}.label") == "선정 (이번 에폭)"),
             f"층화 선정 {len(picked)}장 / 목표 {n_epoch} · {prov}")):
        if nm in ds.list_saved_views():
            ds.delete_saved_view(nm)
        ds.save_view(nm, view, description=desc)

    from fiftyone.core.odm.dataset import ActiveFields
    cur = list(ds.app_config.active_fields.paths) if ds.app_config.active_fields else []
    for f in (f_pk, f_st):
        if f not in cur:
            cur.insert(0, f)
    ds.app_config.active_fields = ActiveFields(paths=cur, exclude=False)
    ds.info = {**(ds.info or {}),
               f"gen_run_{tag}": {"rule": RULE, "k": RULE_K, "bank": version, "gt_tier": tier,
                                  "n_labeled": int(labeled.sum()), "n_err": int(len(err)),
                                  "n_picked": int(len(picked)), "dup_cos": dup_thr}}
    ds.save()
    log(f"gen: 워크스페이스 1-generate / 뷰 01_생성후보_{tag}·02_이번에폭_{tag} [{prov}]")
    log(f"gen: active_fields {cur}")
    log("gen 완료")


def stage_screens() -> None:
    """화면3·4 의 프레임 필드 + 워크스페이스/저장뷰. 뱅크 1벌(BANK_ATTACH) 기준.

    · `winner_del_effect_<tag>` — **G_del(Score_p 상위 K) 를 통째로 지웠을 때** 이 프레임이
      어떻게 되나. 5값. 3값(개선/손해/무관)은 "무관"이 96.9% 라 죽는다 —
      "무관"을 (승자가 삭제대상 아님 / 삭제됐지만 정답유지 / 삭제됐는데 여전히 오답) 으로 쪼갠다.
      ⚠️ **개별삭제(프레임 자기 승자 1개만)로 재면 안 된다** — 순증감이 −42장(손해 391 > 개선 349)
      이라 "지우면 손해"라는 반대 결론이 나오고, 그 신호의 88% 가 저margin 동전던지기에 몰린다.
      배치 Δ+404 vs 개별합 +249 — 시너지가 프레임 단위에서도 157장 재현된다.
    · `winner_site_scope_<tag>` — 이 프레임을 이긴 문장이 몇 대의 카메라에서 실제로 이기나.
      ⚠️ **`reach_<cam> > 0` 기준을 쓰면 안 된다** — 실제 승수 기준과 승자 201개 중 74.6% 가
      불일치하고 전부 한 방향(reach 가 "공통"으로 과대포장, 119건 vs 역방향 0건).
    """
    import fiftyone as fo

    version = os.environ.get("BANK_ATTACH", VERSIONS[0])
    K = int(os.environ.get("GDEL_K", "40"))       # APO 기본값
    tag = vtag(version)
    keys, X, gt, src, banks = load_all()
    cam = load_cameras(keys)
    bank = load_bank(version)                      # 문장은 DB 정본 (load_bank 주석)

    pr = _Pruner(X, gt, bank)
    state = pr.score(None)
    pred = pr._pred_of(state)
    corr0 = pred == gt
    win_g = pr.top1_gidx(state)                  # 사이트범위처럼 단일값이 필요한 곳의 대표 문장
    win_frames = pr.contrib_frames(state)        # argmax=승자 / top-K=표를 넣은 문장
    log(f"screens {version}: 규칙 {RULE}(k={RULE_K}) / 기준 정답 {int(corr0.sum()):,}/{len(gt):,} "
        f"({corr0.mean():.2%}) / 기여 {len(win_frames)}문장")

    # ── G_del = APO 의 Score_p 상위 K ──
    sp = score_p(win_frames, lambda g: int(bank["cls"][g]), gt)
    g_del = [g for g, _ in sorted(sp.items(), key=lambda kv: -kv[1])[:K]]
    drop = np.zeros(len(bank["cls"]), dtype=bool)
    drop[g_del] = True
    pred_b = pr._pred_of(pr.score(drop))
    corr_b = pred_b == gt
    touched = pr.touched_by(state, drop)         # top-K 는 기여 문장 중 하나만 지워져도 영향
    log(f"screens {version}: G_del K={K} (Score_p 상위) → 영향 프레임 "
        f"{int(touched.sum()):,} ({touched.mean():.1%}) / 전체 정답 "
        f"{int(corr0.sum()):,}→{int(corr_b.sum()):,} ({int(corr_b.sum() - corr0.sum()):+,})")

    eff = np.empty(len(gt), dtype=object)
    eff[:] = "미영향 (판정 유지)"
    eff[touched & ~corr0 & corr_b] = "개선 (오답→정답)"
    eff[touched & corr0 & ~corr_b] = "손해 (정답→오답)"
    eff[touched & corr0 & corr_b] = "안전교체 (정답 유지)"
    eff[touched & ~corr0 & ~corr_b] = "잔존오답 (여전히 틀림)"
    log(f"screens {version}: del_effect {dict(collections.Counter(eff.tolist()))}")

    # ── site scope: **실제 승수** 기준 카메라 수 ──
    cams = sorted(set(cam.tolist()))
    n_cams = {g: len({cam[i] for i in fr}) for g, fr in win_frames.items()}
    scope = np.array([SCOPE_LABELS[min(3, n_cams.get(int(g), 1))] for g in win_g], dtype=object)
    log(f"screens {version}: site_scope(프레임) {dict(collections.Counter(scope.tolist()))}")
    log(f"screens {version}: site_scope(문장) "
        f"{dict(collections.Counter(min(3, v) for v in n_cams.values()))} / 카메라 {cams}")

    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    key_to_id = {}
    for s in ds.select_fields(["id", "filepath"]):
        key_to_id[f"{os.path.basename(os.path.dirname(s.filepath))}/"
                  f"{os.path.basename(s.filepath)}"] = s.id
    ids = [key_to_id.get(k) for k in keys]
    ok = [i for i, x in enumerate(ids) if x]
    f_eff, f_scope = f"winner_del_effect_{tag}", f"winner_site_scope_{tag}"
    for fld in (f_eff, f_scope):
        if fld in ds.get_field_schema():
            ds.clear_sample_field(fld)
    ds.set_values(f_eff, {ids[i]: fo.Classification(label=eff[i]) for i in ok}, key_field="id")
    ds.set_values(f_scope, {ids[i]: fo.Classification(label=scope[i]) for i in ok}, key_field="id")

    # ── 워크스페이스 3 + 저장뷰 ──
    from fiftyone import ViewField as F
    vt = version.replace(".", "_")
    if not vt.startswith("v"):
        vt = "v" + vt
    spaces = (("2-audit", None, None),                       # Samples 단독 (문장 필터 진입)
              ("3-prune", "emb_viz", f"{f_eff}.label"),
              ("4-site", "emb_viz", f"{f_scope}.label"))
    for name, brain, color in spaces:
        try:
            if brain is None:
                space = fo.Space(children=[fo.Panel(type="Samples", pinned=True)])
            else:
                space = fo.Space(children=[
                    fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
                    fo.Space(children=[fo.Panel(type="Embeddings",
                                                state={"brainResult": brain,
                                                       "colorByField": color})]),
                ], orientation="horizontal")
            if name in ds.list_workspaces():
                ds.delete_workspace(name)
            ds.save_workspace(name, space,
                              description=(f"{brain} (색: {color})" if brain else "문장→프레임 조회"))
        except Exception as exc:  # noqa: BLE001
            log(f"screens: 워크스페이스 {name} 실패 {exc!r}")

    for nm, view in (
        # 8ea5f0d 라벨 개명("승자 유지"→"판정 유지") 때 이 비교식만 누락 — 문자열이 영원히
        # 불일치라 "삭제영향" 뷰가 전건(~13,100)을 담았다 (실제 영향분은 ~3%). 생산자
        # 상수(L3241)와 같은 문자열을 쓴다.
        (f"03_삭제영향_{tag}", ds.match(F(f"{f_eff}.label") != "미영향 (판정 유지)")),
        (f"04_사이트특이_{tag}", ds.match(F(f"{f_scope}.label") == SCOPE_LABELS[1])),
        (f"05_오답_{tag}", ds.match(F(f"pred_{vt}.label") != F("ground_truth.label"))),
    ):
        if nm in ds.list_saved_views():
            ds.delete_saved_view(nm)
        ds.save_view(nm, view)

    # active_fields 는 allowlist — 여기 없는 필드로 Color by 하면 App 이 TypeError 로 죽는다.
    # winner_gidx 는 **넣지 않는다**: Color-by 용이 아니라 필터 키이고, 칩으로 뜨면 자리만 먹는다.
    from fiftyone.core.odm.dataset import ActiveFields
    active = [f for f in ("ground_truth", f"pred_{vt}", f_eff, f_scope,
                          "environment", "camera") if f in ds.get_field_schema()]
    ds.app_config.active_fields = ActiveFields(paths=active, exclude=False)
    ds.save()
    log(f"screens: 워크스페이스 {ds.list_workspaces()} / 뷰 {ds.list_saved_views()}")
    log(f"screens: active_fields {active}")
    log("screens 완료")


# ────────────────────── attrs (이미지별 속성) ──────────────────────
# 축 정의 — 라벨마다 프롬프트 앙상블(평균 후 재정규화). 축을 늘리려면 여기 항목만 추가한다.
# 라벨당 문장 수를 **같게** 유지할 것 — 뱅크 판정에서 클래스별 문장 수가 사전확률로 새는
# 것과 같은 문제가 여기서도 생긴다.
ATTR_AXES = {
    "environment": {
        "indoor": ["an indoor scene", "It is an indoor scene.",
                   "a photo taken inside a building",
                   "an indoor industrial facility with a ceiling and walls"],
        "outdoor": ["an outdoor scene", "It is an outdoor scene.",
                    "a photo taken outdoors under the open sky",
                    "an outdoor industrial yard with open sky"],
    },
    "daynight": {
        "day": ["a scene in daylight", "It is daytime.",
                "a photo taken during the day", "a bright scene lit by the sun"],
        "night": ["a scene at night", "It is nighttime.",
                  "a photo taken at night", "a dark scene lit by artificial lights"],
    },
    # ponytail: 사람 유/무만은 zero-shot 이 가장 약한 축이다 — CCTV 의 작고 먼 인물은
    # 전역 임베딩에 거의 안 남는다. GT falldown 프레임(정의상 사람 있음)에서의 검출률로
    # 신뢰도를 재고, 부족하면 SAM3 `/segment`(person) 로 갈아탄다.
    "person": {
        "yes": ["a scene with a person in it", "There is a person in the scene.",
                "a photo showing people", "a worker is present in the scene"],
        "no": ["an empty scene with no people", "There is nobody in the scene.",
               "a photo with no people", "a deserted scene with no workers"],
    },
    # 실내 프레임에서는 정의되지 않는다 — environment=outdoor 로 필터해서 읽을 것.
    "weather": {
        "clear": ["a scene on a clear sunny day", "The sky is clear and sunny.",
                  "a photo taken in bright sunshine", "sharp sunlit shadows on the ground"],
        "overcast": ["a scene on an overcast cloudy day", "The sky is grey and overcast.",
                     "a photo taken under flat cloudy light", "a dull scene with no shadows"],
        "rain": ["a scene in the rain", "It is raining.",
                 "a photo taken during rainfall", "wet ground reflecting in the rain"],
        "snow": ["a snowy scene", "It is snowing.",
                 "a photo taken in the snow", "snow covering the ground"],
    },
}
# 축별 자기검증 축 — GT 없이 쓸 수 있는 것만. 파일명 시각(`_YYYYMMDD_HHMMSS`)은
# daynight 의 **사실상 GT** 라 유일하게 축 전용 검증을 붙인다.
ATTR_CLOCK_DAY = (7, 18)          # [start, end) 시. 3~6월 한국 일출~일몰 근사

# 축이 **정의되지 않는 구간**에는 라벨을 주지 않는다 (`undetermined`).
# 2026-08-05 실측: 게이트 없이 돌리면 weather 가 날씨가 아니라 밝기를 읽는다 —
# `clear` 1,911장이 전부 day 였고(야간 clear 0장) 야간 5,579장은 rain/overcast 로 임의
# 분할됐다. 그 결과 rain 48% 가 GT normal(=야간 프레임)과 거의 겹쳐 조용한 교란이 된다.
# 게이트 축은 자기보다 **먼저** 계산돼야 한다 (ATTR_AXES 는 dict 삽입순 = 계산순).
ATTR_GATES = {"weather": {"daynight": ("day",), "environment": ("outdoor",)}}
ATTR_UNDET = "undetermined"


def _scene_from_db(src_videos: list[str]) -> dict[str, dict]:
    """`video_metadata` 의 Gemini 씬 축을 src_video(=파일 stem) 로 매핑.

    조인 키가 stem 인 이유: 프레임 원장은 raw_key 를 안 들고 있고 stem 은 유지된다.
    stem 이 중복되면(다른 소스에 같은 파일명) 값을 버린다 — 틀린 조인보다 없는 게 낫다.
    DB 도달 불가/스키마 부재는 조용히 빈 dict (분석은 프로브 축으로 계속된다).
    """
    dsn = os.environ.get("DATAOPS_POSTGRES_DSN")
    if not dsn or not src_videos:
        return {}
    try:
        import psycopg2
        with psycopg2.connect(dsn) as con, con.cursor() as cur:
            cur.execute("""
                SELECT split_part(r.raw_key,'/',-1), vm.environment_type, vm.daynight_type,
                       vm.weather, vm.env_method
                FROM raw_files r JOIN video_metadata vm ON vm.asset_id=r.asset_id
                WHERE COALESCE(vm.environment_type, vm.daynight_type, vm.weather) IS NOT NULL
            """)
            rows = cur.fetchall()
    except Exception as exc:                                          # noqa: BLE001
        log(f"attrs: DB 씬 조회 생략 {exc!r}")
        return {}
    want, out, dup = set(src_videos), {}, set()
    for fn, env, dn, wx, meth in rows:
        stem = os.path.splitext(fn or "")[0]
        if stem not in want:
            continue
        if stem in out:
            dup.add(stem)
            continue
        out[stem] = {"environment_type": env, "daynight_type": dn, "weather": wx,
                     "env_method": meth}
    for s in dup:
        out.pop(s, None)
    if dup:
        log(f"attrs: stem 중복 {len(dup)}건은 조인 불가로 제외")
    return out


def stage_attrs() -> None:
    """프레임 임베딩에 텍스트 프로브를 걸어 **이미지별 속성**을 만든다 (현재 1축: 실내/실외).

    왜 DB 가 아니라 여기인가: 파이프라인에 `video_metadata.environment_type` 슬롯이 이미
    있지만 source-h 871편은 **전부 `env_method='deferred'`/NULL** 이다 (2026-08-05 확인 —
    Places365 정지 + Gemini 씬 백필 미실행). 게다가 그건 영상 단위 값이고 요청은 이미지
    단위다. 이미 있는 프레임 임베딩 + `/embed_text` 면 새 모델도 GPU 도 필요 없다.

    붙는 필드(축마다 2개): `<축>`(Classification, confidence=margin) · `<축>_margin`
    (1위−2위 코사인. 낮은 순으로 정렬하면 애매한 프레임이 위로 온다).

    축은 노션 「데이터 임베딩 회의 내용 정리」 §3 세분화 기준: 실내·실외 / 사람 유무 /
    주간·야간 / 날씨. 이상상황 카테고리는 `ground_truth` 가 이미 담당한다.
    끝에 `_attrs_cross()` 가 **조건별 오탐·미탐 크로스탭**(회의 요구의 본체)을 낸다.

    ⚠️ GT 가 없다 — 자기검증 3종을 모든 축에 같은 방식으로 낸다: ① 카메라 내 일관성
       ② 영상 내 일관성 ③ GT 클래스별 분포. daynight 만 **파일명 시각이 사실상 GT** 라
       추가 대조가 붙는다.
    ⚠️ source-h 에서 environment 의 정보량은 사실상 0 이다 — 카메라 3대뿐이라 실내/실외는
       `camera` 의 함수다 (slim 이 `camera_angle`/`tilt_bin` 을 지운 것과 같은 이유).
       여러 도메인이 섞인 `--profile frames` 에서 의미가 생긴다.
    ⚠️ weather 는 실내 프레임에서 정의되지 않는다 — `environment=outdoor` 로 필터해 읽을 것.
    """
    import fiftyone as fo
    import requests

    keys, X, gt, src, banks = load_all()
    cam = load_cameras(keys)
    sess = requests.Session()
    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    ids = key_to_ids(ds, keys)
    ok = [i for i, x in enumerate(ids) if x]
    if len(ok) < len(ids):
        log(f"attrs: FiftyOne 매칭 {len(ok)}/{len(ids)}")
    if not ok:
        raise SystemExit(f"attrs: 원장 key 가 데이터셋과 하나도 안 붙는다 "
                         f"(key_join={PROFILES[PROFILE]['key_join']}) — 조인 방식 확인")

    out, preds = {}, {}
    # ── DB 정본 축 (파이프라인 편입 완료 시 자동으로 채워진다) ──
    # `video_metadata` 의 Gemini 씬 6축 중 3개가 여기 축과 스키마가 겹친다. 값이 들어오면
    # `db_*` 필드로 그대로 노출하고 **프로브 축은 건드리지 않는다** — 정본을 덮어쓰는 대신
    # 나란히 두고 비교하게 한다 (덮어쓰면 어느 쪽 숫자인지 사후에 알 수 없다).
    # 2026-08-06 현재 source-h 871 / sourcei 157 / frames 187,994 **전부 NULL(env_method=deferred)**
    # → 아래는 no-op 이고 커버리지만 로그로 남는다.
    db_map = _scene_from_db(sorted(set(src.tolist())))
    if db_map:
        for axis, col in (("environment", "environment_type"), ("daynight", "daynight_type"),
                          ("weather", "weather")):
            vals = {ids[i]: fo.Classification(label=str(db_map[src[i]][col]))
                    for i in ok if db_map.get(src[i], {}).get(col)}
            if vals:
                ds.set_values(f"db_{axis}", vals, key_field="id")
                log(f"attrs: db_{axis} 기록 {len(vals):,}/{len(ok):,}장 (DB 정본)")
        out["_db_coverage"] = {a: sum(1 for v in db_map.values() if v.get(c))
                               for a, c in (("environment", "environment_type"),
                                            ("daynight", "daynight_type"),
                                            ("weather", "weather"))}
        log(f"attrs: DB 씬값 커버리지(영상 기준) {out['_db_coverage']} / 영상 {len(db_map):,}")
    else:
        log("attrs: DB 씬값 없음 — env_method='deferred' (Places365 정지 + Gemini 씬 백필 미실행). "
            "프로브 축만 기록한다. 백필이 돌면 db_* 필드가 자동으로 생긴다")

    for axis, labels in ATTR_AXES.items():
        nper = {lab: len(t) for lab, t in labels.items()}
        if len(set(nper.values())) != 1:
            raise SystemExit(f"attrs {axis}: 라벨당 문장 수가 다르다 {nper} — 사전확률이 샌다")
        names = sorted(labels)
        C = np.empty((len(names), X.shape[1]), dtype=np.float32)
        for i, lab in enumerate(names):
            E = np.stack([_embed_text(sess, t) for t in labels[lab]])
            m = E.mean(axis=0)
            C[i] = m / np.linalg.norm(m)
        S = X @ C.T
        pick = S.argmax(axis=1)
        srt = np.sort(S, axis=1)
        margin = (srt[:, -1] - srt[:, -2]).astype(np.float32)
        pred = np.array(names)[pick].astype(object)      # object — 라벨 길이 잘림 방지

        gate = ATTR_GATES.get(axis, {})
        if gate:
            missing = [g for g in gate if g not in preds]
            if missing:
                raise SystemExit(f"attrs {axis}: 게이트 축 {missing} 이 먼저 계산되지 않았다 "
                                 "— ATTR_AXES 삽입 순서를 확인할 것")
            off = np.zeros(len(pred), dtype=bool)
            for g, allowed in gate.items():
                off |= ~np.isin(np.asarray(preds[g], dtype=object), list(allowed))
            pred[off] = ATTR_UNDET
            margin[off] = 0.0
            log(f"attrs {axis}: 게이트 {gate} → {int(off.sum()):,}/{len(pred):,} "
                f"({off.mean():.1%}) 를 {ATTR_UNDET} 처리")

        # ── 자기검증 3종 (GT 불필요, 모든 축에 같은 방식) ──
        #   ① 카메라 내 일관성 — 카메라 고정 축(environment/weather)이라면 갈리면 잡음
        #   ② 영상 내 일관성   — 한 영상 안에서 바뀔 수 없는 축(daynight/weather)의 하한
        #   ③ GT 클래스별 분포 — person 은 falldown 에서 100% 여야 한다(정의상 사람이 있다)
        def _group_consistency(g):
            per = {}
            for k in sorted(set(g.tolist())):
                m = g == k
                dist = collections.Counter(pred[m].tolist())
                top, ntop = dist.most_common(1)[0]
                per[str(k)] = {"n": int(m.sum()), "label": top,
                               "consistency": round(ntop / int(m.sum()), 4),
                               "dist": {a: int(b) for a, b in dist.items()},
                               "margin_median": round(float(np.median(margin[m])), 5)}
            return per

        per_cam = _group_consistency(cam)
        per_vid = _group_consistency(src)
        by_gt = {CLASS_NAMES[c]: {a: int(b) for a, b in
                                 collections.Counter(pred[gt == c].tolist()).items()}
                 for c in sorted(set(gt.tolist()))}
        vid_mean = float(np.mean([d["consistency"] for d in per_vid.values()]))
        out[axis] = {
            "labels": names,
            "dist": {a: int(b) for a, b in collections.Counter(pred.tolist()).items()},
            "margin_p10": round(float(np.percentile(margin, 10)), 5),
            "per_camera": per_cam, "by_gt": by_gt,
            "video_consistency_mean": round(vid_mean, 4),
            "video_consistency_min": round(min(d["consistency"] for d in per_vid.values()), 4),
        }
        worst = min(per_cam.values(), key=lambda d: d["consistency"])
        log(f"attrs {axis}: " + " ".join(f"{a}={b:,}" for a, b in out[axis]["dist"].items())
            + f" | 카메라내 최저 {worst['consistency']:.1%}"
            + f" | 영상내 평균 {vid_mean:.1%}"
            + f" | margin 하위10% ≤ {out[axis]['margin_p10']:.4f}")
        for k, d in per_cam.items():
            log(f"attrs {axis}   {k}: {d['label']} {d['consistency']:.1%} "
                f"(n={d['n']:,}, margin중앙 {d['margin_median']:+.4f})")
        log(f"attrs {axis}   GT별: " + " · ".join(
            f"{c}={dict(sorted(d.items(), key=lambda x: -x[1]))}" for c, d in by_gt.items()))

        # ④ daynight 전용 — 파일명 시각(`_YYYYMMDD_HHMMSS`)이 사실상 GT 다
        #    ⚠️ `daynight` 은 **시각적** 주야(밝은가/어두운가)다. 실내에서는 시계 시각과
        #    분리된다 — 실측: source-h(옥외) 일치 98.6% vs sourcei(실내) 18.0%. 새벽 1시
        #    백화점이 조명 때문에 "day" 로 보이는 것은 분류 실패가 아니다. 시계 시각이
        #    필요하면 아래 `clock_daynight`/`clock_hour` 를 쓸 것 (파일명에서 파싱).
        if axis == "daynight":
            import re as _re
            hh = np.full(len(keys), -1, dtype=np.int32)
            for i, s_ in enumerate(src):
                mo = _re.search(r"(20\d{6})_([01]\d|2[0-3])([0-5]\d)([0-5]\d)", str(s_))
                if mo:
                    hh[i] = int(mo.group(2))
            has = hh >= 0
            if has.any():
                cd = np.where((hh >= ATTR_CLOCK_DAY[0]) & (hh < ATTR_CLOCK_DAY[1]),
                              "day", "night")
                ds.set_values("clock_daynight",
                              {ids[i]: fo.Classification(label=str(cd[i]))
                               for i in ok if has[i]}, key_field="id")
                ds.set_values("clock_hour",
                              {ids[i]: int(hh[i]) for i in ok if has[i]}, key_field="id")
                log(f"attrs daynight   clock_daynight/clock_hour 기록 {int(has.sum()):,}장 "
                    "(파일명 시각 — 시각적 daynight 과 별개 축)")
            if has.any():
                ref = np.where((hh >= ATTR_CLOCK_DAY[0]) & (hh < ATTR_CLOCK_DAY[1]),
                               "day", "night")
                acc = float((pred[has] == ref[has]).mean())
                out[axis]["clock_agreement"] = round(acc, 4)
                out[axis]["clock_ref_dist"] = {a: int(b) for a, b in
                                               collections.Counter(ref[has].tolist()).items()}
                log(f"attrs daynight   파일명 시각 대조: 일치 {acc:.1%} "
                    f"(n={int(has.sum()):,}, 기준 {ATTR_CLOCK_DAY[0]}~{ATTR_CLOCK_DAY[1]}시=day, "
                    f"참조분포 {out[axis]['clock_ref_dist']})")
            else:
                log("attrs daynight   파일명에서 시각 파싱 실패 — 대조 생략")

        # ⑤ weather 전용 — 같은 **날짜**면 날씨는 같아야 한다. 파일명 YYYYMMDD 가 참조축.
        #    날짜 내 일관성이 낮으면 그 축은 날씨가 아니라 프레임별 밝기/색을 읽고 있다.
        if axis == "weather":
            import re as _re
            dt = np.array([(_re.search(r"(\d{8})_\d{6}", str(s_)).group(1)
                            if _re.search(r"(\d{8})_\d{6}", str(s_)) else "?") for s_ in src])
            det = pred != ATTR_UNDET
            per_date, rows_ = {}, []
            for d in sorted(set(dt[det].tolist())):
                m = det & (dt == d)
                if not m.any():
                    continue
                c = collections.Counter(pred[m].tolist())
                top, ntop = c.most_common(1)[0]
                per_date[d] = {"n": int(m.sum()), "label": top,
                               "consistency": round(ntop / int(m.sum()), 4),
                               "dist": {a: int(b) for a, b in c.items()}}
                rows_.append(per_date[d]["consistency"])
            out[axis]["per_date"] = per_date
            out[axis]["date_consistency_mean"] = round(float(np.mean(rows_)), 4) if rows_ else None
            log(f"attrs weather   날짜 내 일관성 평균 {np.mean(rows_):.1%} "
                f"(날짜 {len(per_date)}개) — 낮으면 이 축은 날씨가 아니라 밝기/색을 읽는 것이다")
            for d, v_ in sorted(per_date.items())[:6]:
                log(f"attrs weather     {d}: {v_['label']} {v_['consistency']:.1%} "
                    f"(n={v_['n']:,}, {v_['dist']})")

        preds[axis] = pred
        ds.set_values(axis, {ids[i]: fo.Classification(label=str(pred[i]),
                                                       confidence=float(margin[i]))
                             for i in ok}, key_field="id")
        ds.set_values(f"{axis}_margin", {ids[i]: float(margin[i]) for i in ok}, key_field="id")

    _attrs_cross(preds, gt, cam, out)
    with open(f"{GEO}/attrs.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    ds.save()
    log(f"attrs 완료 → {GEO}/attrs.json · 필드 {sorted(ATTR_AXES)}")


def _attrs_cross(preds: dict, gt: np.ndarray, cam: np.ndarray, out: dict) -> None:
    """회의 요구의 본체 — **세부 조건별 오탐·미탐 발생 위치**를 두 규칙으로 나란히 낸다.

    조건 축 = attrs 축 + camera + GT 클래스. 규칙 축 = top-k 다수결 / 분포 IoU(wave).
    wave 예측은 `wave` 스테이지가 남긴 npz 에서 읽는다 (없으면 top-k 만).

    FiftyOne 에서 필터로 같은 걸 볼 수 있지만, 회의에 붙일 **한 장의 표**는 앱이 못 만든다.
    → `report/attrs_cross.md`.
    """
    rules = {}
    for v in BANKS:
        p = f"{GEO}/wave_{vtag(v)}.npz"
        if os.path.exists(p):
            rules[f"wave {v}"] = np.load(p)["pred"]
    if not rules:
        log("attrs cross: wave npz 없음 — `wave` 스테이지 먼저 (크로스탭 생략)")
        return
    keys_, X_, gt_, src_, banks = load_all()
    for v in BANKS:
        rules[f"topk {v}"] = bank_vote_stream(X_, banks[v], RULE_K)[0]

    axes = dict(preds)
    axes["camera"] = cam
    L = ["# 세부 조건별 오탐·미탐 — 판정규칙 2벌 비교", "",
         f"생성 {time.strftime('%Y-%m-%d %H:%M')} · 프레임 {len(gt):,} · "
         f"top-k k={RULE_K} · wave bins={WAVE_BINS} thr={WAVE_THR}", "",
         "`FN` = GT 이벤트인데 다른 클래스로 판정 · `FP` = GT normal 인데 이벤트로 판정", "",
         "⚠️ **acc 를 슬라이스끼리 비교하지 말 것** — 슬라이스마다 GT 이벤트/정상 구성이 다르다",
         "(예: weather=clear 는 이벤트가 몰린 구간이라 acc 가 낮은 게 당연하다). 비교 가능한 건",
         "이벤트 대비 `FN%` 와 정상 대비 `FP%` 다.", ""]
    rows = {}
    for aname, av in axes.items():
        L += [f"## {aname}", "",
              "| 조건 | n | 이벤트/정상 | "
              + " | ".join(f"{r}<br>acc / FN / FP" for r in rules) + " |",
              "|---|---:|---:|" + "---|" * len(rules)]
        for val in sorted(set(av.tolist())):
            m = av == val
            ev, nm = m & (gt != 0), m & (gt == 0)
            cells, rec = [], {}
            for rname, rp in rules.items():
                acc = float((rp[m] == gt[m]).mean())
                fn = int((ev & (rp != gt)).sum())
                fp = int((nm & (rp != 0)).sum())
                cells.append(f"{acc:.1%} / {fn:,}({fn / max(int(ev.sum()), 1):.0%}) "
                             f"/ {fp:,}({fp / max(int(nm.sum()), 1):.0%})")
                rec[rname] = {"acc": round(acc, 4), "fn": fn, "fp": fp,
                              "n_event": int(ev.sum()), "n_normal": int(nm.sum())}
            L.append(f"| {val} | {int(m.sum()):,} | {int(ev.sum()):,}/{int(nm.sum()):,} | "
                     + " | ".join(cells) + " |")
            rows[f"{aname}={val}"] = rec
        L.append("")
    os.makedirs(REPORT_DIR, exist_ok=True)
    p = f"{REPORT_DIR}/attrs_cross.md"
    with open(p, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    out["_cross"] = rows
    log(f"attrs cross: 조건 {len(rows)}개 × 규칙 {len(rules)}개 → {p}")


# ────────────────────── wave (분포 IoU) ──────────────────────
def hist_iou(h_a: np.ndarray, h_b: np.ndarray) -> np.ndarray:
    """면적 IoU = Σmin/Σmax, 마지막 축 기준. `compute_hist_iou()` 의 벡터화 판."""
    return (np.minimum(h_a, h_b).sum(-1)
            / np.maximum(np.maximum(h_a, h_b).sum(-1), 1e-12))


def wave_stream(X: np.ndarray, bank: dict, gt: np.ndarray, bins: int = WAVE_BINS,
                chunk: int = 256) -> tuple[np.ndarray, tuple, np.ndarray, np.ndarray]:
    """제품 분포-IoU 를 프레임 전량에 재현 + **문장별 기여도**를 같이 낸다.

    문장별 기여도 = LOO ΔIoU = (그 문장을 뺐을 때의 IoU) − (있을 때의 IoU), 자기 클래스가
    GT 인 프레임들에서의 평균. 부호 해석이 역할에 따라 뒤집힌다 —
      · 이벤트 문장: 이벤트 프레임에서 IoU 는 **낮아야** 탐지된다 → ΔIoU>0 = 그 문장이
        분리를 만들고 있었다 = 유익.
      · normal 문장: normal 프레임에서 IoU 는 **높아야** 조용하다 → ΔIoU>0 = 그 문장이
        IoU 를 끌어내리고 있었다 = 오탐 유발. (이벤트 클래스별 ΔIoU 의 평균으로 잰다.)
    raw 값은 그대로 저장하고 해석은 `wave_role` 라벨이 담당한다.

    ⚠️ 왜 12,480회 LOO 가 현실적인가: IoU 는 히스토그램만 보므로 **같은 bin 에 떨어진
       문장들의 ΔIoU 는 프레임마다 동일하다**. 그래서 프레임×클래스×bin (80개) 만 계산하고
       문장은 자기 bin 값을 집어가면 된다 — 문장 수와 무관해진다.

    ⚠️ 재현하지 않는 것: 디바운스(최근 5프레임 중 3↑). source-h 은 이미 추출된 키프레임
       집합이라 시간 이웃이 없다. 여기 IoU 는 디바운스 **이전** 신호다 (디바운스는 고립
       발화만 지우므로 프레임 단위 비교에서는 wave 에 유리하게도 불리하게도 안 쓰인다).
    ⚠️ `iou_mode='std'` 는 미구현 — README 권장 실행값이 hist 다. 필요해지면 여기에 추가.
    """
    V, cls = bank["vec"], bank["cls"]
    classes = sorted(set(cls.tolist()))
    events = tuple(c for c in classes if c != 0)
    if 0 not in classes:
        raise SystemExit("wave 는 normal(0) 클래스 문장이 있어야 성립한다 (기준 분포)")
    mem = {c: np.flatnonzero(cls == c) for c in classes}
    ncls = {c: len(mem[c]) for c in classes}
    n = X.shape[0]
    iou = np.empty((n, len(events)), dtype=np.float32)
    gain = np.zeros(len(cls), dtype=np.float64)
    gain_n = np.zeros(len(cls), dtype=np.int64)
    EYE = np.eye(bins, dtype=np.float32)

    for a in range(0, n, chunk):
        S = X[a:a + chunk] @ V.T                                  # [f, M]
        f = S.shape[0]
        lo, hi = S.min(axis=1), S.max(axis=1)
        w = np.maximum(hi - lo, 1e-6)
        # np.histogram(linspace(lo,hi,bins+1)) 과 동일한 배정 (마지막 edge 는 포함 → clip)
        Bi = np.clip(((S - lo[:, None]) / w[:, None] * bins).astype(np.int32), 0, bins - 1)
        del S
        fi = np.arange(f)
        cnt, h = {}, {}
        for c in classes:
            flat = (fi[:, None] * bins + Bi[:, mem[c]]).ravel()
            cnt[c] = np.bincount(flat, minlength=f * bins).reshape(f, bins).astype(np.float32)
            h[c] = cnt[c] / ncls[c]
        base = {e: hist_iou(h[0], h[e]) for e in events}           # [f]
        for j, e in enumerate(events):
            iou[a:a + f, j] = base[e]

        # bin 별 LOO ΔIoU
        delta = {}
        for e in events:
            Hp = (cnt[e][:, None, :] - EYE[None]) / max(ncls[e] - 1, 1)
            delta[e] = hist_iou(h[0][:, None, :], Hp) - base[e][:, None]
            del Hp
        H0p = (cnt[0][:, None, :] - EYE[None]) / max(ncls[0] - 1, 1)
        d0 = np.zeros((f, bins), dtype=np.float32)
        for e in events:
            d0 += hist_iou(H0p, h[e][:, None, :]) - base[e][:, None]
        delta[0] = d0 / len(events)
        del H0p

        # 자기 클래스가 GT 인 프레임에서만 누적
        for c in classes:
            rows = np.flatnonzero(gt[a:a + f] == c)
            if len(rows) == 0:
                continue
            picked = np.take_along_axis(delta[c][rows], Bi[np.ix_(rows, mem[c])], axis=1)
            gain[mem[c]] += picked.sum(axis=0)
            gain_n[mem[c]] += len(rows)
    return iou, events, gain / np.maximum(gain_n, 1), gain_n


def stage_wave() -> None:
    """제품 분포-IoU 규칙으로 프레임을 재채점 + 문장별 기여도 캐시 (`promptmap` 이 소비).

    붙는 프레임 필드: `wave_pred_<vt>`(Classification, confidence=1−최저IoU) ·
    `wave_iou_<cls>_<tag>`(클래스별 IoU, 정렬용 float) · `wave_vs_topk_<tag>`
    (top-k 다수결과 판정이 갈린 프레임의 "topk→wave" 라벨).

    단일라벨 축소: 이벤트 중 IoU 최저가 임계 미만이면 그 클래스, 아니면 normal. 제품은
    다중라벨(임계 미만 전부 발화)이지만 GT 가 4-클래스 단일라벨이라 비교를 위해 축소한다 —
    다중발화 프레임 수를 로그로 같이 낸다 (축소로 숨는 양).
    """
    import fiftyone as fo

    keys, X, gt, src, banks = load_all()
    ds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    ids = key_to_ids(ds, keys)
    ok = [i for i, x in enumerate(ids) if x]
    if len(ok) < len(ids):
        log(f"wave: FiftyOne 매칭 {len(ok)}/{len(ids)}")
    if not ok:
        raise SystemExit(f"wave: 원장 key 가 데이터셋과 하나도 안 붙는다 "
                         f"(key_join={PROFILES[PROFILE]['key_join']}) — 조인 방식 확인")

    summary = {}
    for v in BANKS:
        bank = banks[v]
        tag, vt = vtag(v), v.replace(".", "_")
        log(f"wave {v}: 문장 {len(bank['cls']):,} / bins={WAVE_BINS} thr={WAVE_THR} — 계산")
        iou, events, wgain, gn = wave_stream(X, bank, gt)
        fired = iou < WAVE_THR
        pred = np.where(fired.any(axis=1), np.array(events)[iou.argmin(axis=1)], 0)
        m = prf(pred, gt)
        multi = int((fired.sum(axis=1) > 1).sum())
        pk, _, _ = bank_vote_stream(X, bank, RULE_K)
        mk = prf(pk, gt)
        log(f"wave {v}: micro={m['micro']:.2%} macroF1={m['macro_f1']:.2%}  "
            + " ".join(f"{n}=F1 {d['F1']:.2f}(P{d['P']:.2f}/R{d['R']:.2f})"
                       for n, d in m["per_class"].items()))
        log(f"wave {v}: top-k(k={RULE_K}) micro={mk['micro']:.2%} macroF1={mk['macro_f1']:.2%} "
            f"| 두 규칙 판정 일치 {float((pred == pk).mean()):.1%} "
            f"| 다중발화(단일라벨 축소로 숨는 프레임) {multi:,}")
        np.savez_compressed(f"{GEO}/wave_{tag}.npz", iou=iou, events=np.array(events),
                            gain=wgain, gain_n=gn, pred=pred)
        summary[v] = {"wave": m, "topk": mk, "agree": float((pred == pk).mean()),
                      "multi_fire": multi, "bins": WAVE_BINS, "thr": WAVE_THR,
                      "rule_k": RULE_K}

        # WAVE_WRITE_FIELDS=0 — npz(문장 기여도, promptmap/-prompts 가 소비)만 내고 프레임
        # 필드는 생략한다. 29버전 × 필드 6종 ≈ 170 필드가 flat 스키마에 얹히는 게 스펙
        # §1-4 명명 부채의 본체라, -prompts wave 축 목적이면 npz 로 충분하다 (2026-08-19).
        if os.environ.get("WAVE_WRITE_FIELDS", "1") != "0":
            for j, e in enumerate(events):
                ds.set_values(f"wave_iou_{CLASS_NAMES[e]}_{tag}",
                              {ids[i]: float(iou[i, j]) for i in ok}, key_field="id")
            conf = 1.0 - iou.min(axis=1)
            ds.set_values(f"wave_pred_{vt}",
                          {ids[i]: fo.Classification(label=CLASS_NAMES[int(pred[i])],
                                                     confidence=float(conf[i])) for i in ok},
                          key_field="id")
            ds.set_values(f"wave_vs_topk_{tag}",
                          {ids[i]: fo.Classification(
                              label=f"{CLASS_NAMES[int(pk[i])]}→{CLASS_NAMES[int(pred[i])]}")
                              for i in ok if pk[i] != pred[i]}, key_field="id")
        else:
            log(f"wave {v}: WAVE_WRITE_FIELDS=0 — 프레임 필드 생략 (npz 만)")

    with open(f"{GEO}/wave.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=1)
    ds.save()
    log(f"wave 완료 → {GEO}/wave.json + wave_<tag>.npz (promptmap 이 문장 기여도를 읽는다)")


# ────────────────────── promptmap ──────────────────────
def nearest_frame_stream(X: np.ndarray, P: np.ndarray,
                         chunk: int = 2048) -> tuple[np.ndarray, np.ndarray]:
    """문장별 최근접 프레임 (cos, 프레임 인덱스). 문장 축으로 잘라 피크 메모리를 억제한다 —
    13,144×16,125 fp32 를 한 번에 만들면 848MB 이고 이 호스트의 병목은 RAM 이다."""
    best = np.empty(len(P), dtype=np.float32)
    idx = np.empty(len(P), dtype=np.int64)
    for s in range(0, len(P), chunk):
        S = X @ P[s:s + chunk].T                       # [N, chunk]
        idx[s:s + chunk] = S.argmax(axis=0)
        best[s:s + chunk] = S.max(axis=0)
        del S
    return best, idx


def stage_promptmap() -> None:
    """축을 뒤집어 **문장 하나 = 표본 하나** 인 데이터셋 `<dataset>-prompts` 를 만든다.

    기존 스테이지는 전부 프레임 관점이었다 (프레임마다 이긴 문장 = `top_prompt_*`).
    프롬프트를 카테고리별로 보고 싶으면 표본이 문장이어야 한다.

    · UMAP 은 **문장 벡터만** 으로 만든다. 문장과 이미지를 한 UMAP 에 올리는 길은
      atlas 도크스트링의 실측으로 이미 기각됐다 (text↔image cos 중앙 0.147 vs
      text↔text 0.631 vs image↔image 0.756 → modality 두 덩이가 되고 최근접 질의가
      엔티티 타입 분류기가 된다). 여기 좌표는 **문장끼리의 기하만** 뜻한다.
    · 이미지 연결은 좌표가 아니라 **표본 속성**으로 준다:
      `filepath` = 그 문장의 최근접 프레임 → 썸네일이 곧 "이 문장이 뭘 잡는지" 다.
      `match` = 그 프레임의 GT 가 문장 클래스와 같은가 (hit/miss).
      `wins`/`purity` = 실제로 가져간 프레임 수와 그 중 정답 비율.
    · 두 판정규칙을 **같은 점 위에 나란히** 올린다:
      - top-k 축: `wins`/`purity`/`adopted` — 클래스별 best 의 전역 argmax(atlas 와 같은
        정의라 `prompt_frames_*.csv` 와 숫자가 맞는다).
      - wave 축: `wave_gain`/`wave_role` — 제품 분포-IoU 에서의 LOO 기여도.
        `wave` 스테이지를 먼저 돌려야 붙는다 (없으면 top-k 축만 붙고 경고).
      두 축은 **모수가 다르다**: top-k 는 이긴 문장만 값이 있고(채택 1.6%), wave 는 분포
      전체가 판정에 들어가므로 모든 문장에 값이 있다. "실사용률" 결론은 규칙에 종속이다.
    · brain_key 는 `emb_viz` 로 고정한다. Embeddings 패널이 키를 기억해서 다른 이름이면
      Color by 까지 죽는 App 함정이 있다.
    """
    import fiftyone as fo

    keys, X, gt, src, banks = load_all()
    cam = load_cameras(keys)

    # 썸네일 경로 — 소스 프레임 데이터셋에서 key → filepath.
    # ⚠️ 원장 키 형식은 프로필마다 다르다: sourceh/sourcei 는 `<folder>/<name>`(경로 파생)이라
    #    basename 조인이 성립하지만, frames 는 키가 **image_id(hex)** 이고 미디어가 평면
    #    (`media/<uuid>.jpg`)이라 basename 조인이 전량 미스한다 (2026-08-18 실사고 —
    #    "최근접 프레임 스킵 12480" → UMAP 입력 0행). PROFILES 의 frame_key_field 가 있으면
    #    그 샘플 필드로 조인한다.
    sds = fo.load_dataset(PROFILES[PROFILE]["dataset"])
    _jf = PROFILES[PROFILE].get("frame_key_field")
    _fps = sds.values("filepath")
    if _jf:
        _fkeys = [str(k) if k else None for k in sds.values(_jf)]
    else:
        _fkeys = [f"{os.path.basename(os.path.dirname(fp))}/{os.path.basename(fp)}"
                  for fp in _fps]
    key2fp = {k: fp for k, fp in zip(_fkeys, _fps) if k}
    if not (set(keys[:64]) & set(key2fp)):
        raise SystemExit(
            f"promptmap: 원장 키와 {PROFILES[PROFILE]['dataset']} 조인 키가 전혀 겹치지 않는다 "
            f"(원장 예: {keys[0]!r} / 조인 예: {next(iter(key2fp), None)!r}) — "
            "frame_key_field 설정을 확인하라. 조용히 전량 스킵하는 것보다 여기서 죽는 게 낫다")
    # 최근접 프레임의 씬 조건 — "이 문장은 어떤 상황의 이미지에 붙나". attrs 가 먼저 돌아야
    # 채워진다 (없으면 그냥 생략). db_* 가 있으면 그걸 우선한다 (정본).
    sch0 = sds.get_field_schema()
    key2attr: dict[str, dict] = {}
    for ax in ("environment", "daynight", "person"):
        fld = f"db_{ax}" if f"db_{ax}" in sch0 else (ax if ax in sch0 else None)
        if not fld:
            continue
        for k, lab in zip(_fkeys, sds.values(f"{fld}.label")):
            if lab and k:
                key2attr.setdefault(k, {})[ax] = lab
    if key2attr:
        log(f"promptmap: 최근접 프레임 씬 조건 {len(key2attr):,}장분 사용")

    name = f"{PROFILES[PROFILE]['dataset']}-prompts"
    ds = fo.Dataset(name, overwrite=True, persistent=True)

    # UMAP 입력을 루프에서 numpy 로 직접 수집 — ds.values("embedding") 는 전 행을
    # Python list-of-lists 로 올려서 54버전 ~78만 행이면 20GB+ (가용 8GB 호스트에서 OOM).
    # 여기서 add_samples 와 같은 순서·같은 스킵으로 모으므로 대응이 어긋나지 않는다.
    E_parts: list = []
    inserted_ids: list = []   # ID-keyed points 용 — 순서 결합 제거 (codex 3A)

    for vi, v in enumerate(BANKS):
        goff = vi * GIDX_OFFSET          # gidx 전역 유일성 — GIDX_OFFSET 주석 참고
        bank = banks[v]
        P, cls = bank["vec"], bank["cls"]
        classes = sorted(set(cls.tolist()))
        gidx = {c: np.flatnonzero(cls == c) for c in classes}

        # wave 축 — 부호를 역할에 맞춰 뒤집어 "양수=유익" 한 축으로 만들고, 클래스 내
        # 백분위로 층화한다. 클래스마다 문장 수가 달라(normal 10,703 vs falldown 160)
        # ΔIoU 절대크기가 자동으로 달라지므로 전역 임계는 클래스를 오분류한다.
        wpath = f"{GEO}/wave_{vtag(v)}.npz"
        wgain = wrole = None
        if os.path.exists(wpath):
            wgain = np.load(wpath)["gain"]
            signed = np.where(cls == 0, -wgain, wgain)
            wrole = np.full(len(cls), "중간", dtype=object)
            for c in classes:
                g = gidx[c]
                lo_q, hi_q = np.percentile(signed[g], [10, 90])
                # 부호 실재 조건 — gain=0(무기여) 동점 덩어리가 백분위에 걸리면 "하위10%"
                # 라벨이 40%+ 를 담는 거짓이 된다 (frames 실측 43.6%, 정상 위주 데이터셋의
                # 퇴화 — sourcei 는 이벤트 밀도 덕에 우연히 무증상). 유해=음의 기여 실재,
                # 유익=양의 기여 실재일 때만.
                wrole[g[(signed[g] >= hi_q) & (signed[g] > 0)]] = "유익 상위10%"
                wrole[g[(signed[g] <= lo_q) & (signed[g] < 0)]] = "유해 하위10%"
        else:
            log(f"promptmap {v}: {wpath} 없음 — wave 축 생략 (`wave` 스테이지 먼저)")

        b1, _, a1 = bank_top2_stream(X, bank)
        M = np.stack([b1[c] for c in classes], axis=1)
        pred = np.array(classes)[M.argmax(axis=1)]
        win_g = np.array([gidx[int(c)][a1[int(c)][i]] for i, c in enumerate(pred)])
        won = collections.defaultdict(list)
        for i, g in enumerate(win_g.tolist()):
            won[g].append(i)

        ncos, nidx = nearest_frame_stream(X, P)
        log(f"promptmap {v}: 문장 {len(P):,} — 채택 {len(won):,} / 최근접 계산 완료")

        batch, missing, kept_g = [], 0, []
        for g in range(len(P)):
            fp = key2fp.get(keys[int(nidx[g])])
            if fp is None:                             # 소스 데이터셋에 없는 프레임
                missing += 1
                continue
            kept_g.append(g)
            c = int(cls[g])
            fr = won.get(g, [])
            ngt = int(gt[int(nidx[g])])
            s = fo.Sample(filepath=fp)
            s["text"] = bank["prompt"][g]
            s["category"] = fo.Classification(label=CLASS_NAMES[c])
            s["bank_version"] = fo.Classification(label=v)
            # gt=-1(미검수)은 CLASS_NAMES 에 없다 — frames 도메인은 GT 0 이 정상이라
            # 여기서 KeyError 로 죽으면 -prompts 빌드 자체가 불가능해진다 (_prune_bank 의
            # stolen no_gt 가드와 같은 계열). match 도 hit/miss 어느 쪽도 아니다.
            s["nearest_gt"] = fo.Classification(label=CLASS_NAMES.get(ngt, "no_gt"),
                                                confidence=float(ncos[g]))
            s["match"] = fo.Classification(
                label=("no_gt" if ngt < 0 else ("hit" if ngt == c else "miss")))
            s["nearest_key"] = keys[int(nidx[g])]
            for ax, lab in (key2attr.get(keys[int(nidx[g])]) or {}).items():
                s[f"nearest_{ax}"] = fo.Classification(label=str(lab))
            s["adopted"] = fo.Classification(label="채택" if fr else "미채택")
            s["wins"] = len(fr)
            s["gidx"] = goff + g
            if fr:
                p = float((gt[fr] == c).mean())
                s["purity"] = round(p, 4)
                s["purity_tier"] = fo.Classification(label=purity_bin(p))
                s["n_cameras"] = len(set(cam[fr].tolist()))
            if wgain is not None:
                s["wave_gain"] = float(wgain[g])
                s["wave_role"] = fo.Classification(label=str(wrole[g]))
            # embedding(1024-d 리스트)은 몽고에 넣지 않는다 — 문서 부피의 94%였고,
            # 29버전 60만 행에서 WiredTiger 캐시를 부풀려 mongod 딥스톨(ping 9분43초,
            # _OperationCancelled 연쇄)을 일으켰다 (2026-08-11 실측 2회). 벡터 정본은
            # PROMPT_DIR/<ver>.npz — gidx%GIDX_OFFSET 로 조회. UMAP 은 위 E_parts 경로.
            batch.append(s)
            if len(batch) >= 2000:                     # 2000 씩 흘려 피크 RAM 억제
                inserted_ids.extend(map(str, ds.add_samples(batch)))
                batch = []
        if batch:
            inserted_ids.extend(map(str, ds.add_samples(batch)))
        if missing:
            log(f"promptmap {v}: 최근접 프레임이 소스에 없어 스킵 {missing}")
        E_parts.append(P[np.asarray(kept_g, dtype=np.int64)].astype(np.float32))

    # UMAP — E_parts 는 add_samples 와 같은 순서/스킵으로 모였으므로 대응이 어긋나지 않는다
    for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
               "NUMBA_NUM_THREADS"):
        os.environ.setdefault(_v, str(max(1, (os.cpu_count() or 4) // 4)))
    import fiftyone.brain as fob
    import umap

    E = np.concatenate(E_parts, axis=0)
    del E_parts
    assert len(E) == ds.count(), f"UMAP 입력 {len(E)} ≠ 데이터셋 {ds.count()} — 스킵 대응 붕괴"
    umap_init = "spectral"
    if len(E) > 100_000:
        # 1024-d 코사인 UMAP 은 29버전 60만 행에서 메모리가 폭발한다 (RLIMIT 16GB 에서
        # MemoryError 실측, 2026-08-12) — PCA 64-d 사전축소 (UMAP 문서 권장 관행).
        # 주의: PCA 는 평균 중심화를 하므로 축소 공간의 cosine 은 원 벡터의 cosine 과
        # 수학적으로 동일하지 않다 (codex 3B) — 시각화 배치 용도의 근사로 수용.
        # init 도 random 으로 — spectral 은 큰 connected component(실측 23,929노드)에서
        # dense n×n(4.3GB) 을 만들다 죽는다.
        from sklearn.decomposition import PCA
        E = PCA(n_components=64, svd_solver="randomized",
                random_state=42).fit_transform(E).astype(np.float32)
        umap_init = "random"
        log(f"promptmap: PCA 사전축소 → {E.shape}")
    log(f"promptmap: UMAP fit {E.shape} (init={umap_init})")
    pts = umap.UMAP(n_components=2, metric="cosine", low_memory=True,
                    init=umap_init, random_state=42).fit_transform(E)
    # ID-keyed dict (codex 3A): raw ndarray 는 compute_visualization 이 개수만 검증하고
    # 순서는 ds.values("id") 와의 암묵 결합 — 중간에 재정렬이 끼면 조용히 뒤섞인다.
    assert len(inserted_ids) == len(pts)
    fob.compute_visualization(ds, points={i: p for i, p in zip(inserted_ids, pts)},
                              brain_key="emb_viz")

    sch = ds.get_field_schema()
    for wsname, color in (("prompts", "category.label"),
                          ("topk", "adopted.label"),
                          ("wave", "wave_role.label")):
        if color.split(".")[0] not in sch:
            continue
        space = fo.Space(children=[
            fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
            fo.Space(children=[fo.Panel(type="Embeddings",
                                        state={"brainResult": "emb_viz",
                                               "colorByField": color})]),
        ], orientation="horizontal")
        ds.save_workspace(wsname, space, description=f"문장 UMAP (색: {color})")
    ds.save()

    hit = ds.match({"match.label": "hit"}).count()
    log(f"promptmap: {name} 문장 {ds.count():,} · 최근접 프레임 GT 일치 {hit / ds.count():.1%} "
        f"· 워크스페이스 {ds.list_workspaces()}")
    log("promptmap 완료")


# ────────────────────── atlas ──────────────────────
def stage_atlas() -> None:
    """채택 문장 ↔ 이미지 ↔ 영상 을 **연결**해 두 방향으로 낸다 (요청: 세 임베딩 비교).

    ⚠️ 하지 **않는** 것과 그 이유 (전부 라이브 실측으로 기각됨):
      · 세 종류를 한 벡터 컬럼/한 UMAP 에 올리기 — text↔image cos 중앙 0.147 vs
        text↔text 0.631 vs image↔image 0.756 으로 분포가 겹치지 않는다. 최근접 질의가
        엔티티 타입 분류기가 되고 UMAP 은 modality 로 두 덩이가 된다.
      · 영상 임베딩(프레임 센트로이드) — 프레임 중앙값 커버리지와 spearman 0.993 인
        재인코딩이고, 회수가능 오답 지목력이 gap_cluster 62.3% 대비 36.3% 로 열등하다.
        여기선 영상을 **벡터가 아니라 집계 키**로만 쓴다 (길이 편향 자체가 안 생긴다).
      · 뱅크 간 절대 코사인 비교 — 가산 오프셋 때문에 불공정 (§13 cover_viz 폐기와 동일).

    대신 **뱅크 내부 상대량**(reach·margin·승수)과 **소속 관계**만 낸다:
      prompt_frames_<ver>.csv  — 문장 관점: 이 문장이 어떤 영상/프레임을 가져갔나 + 최근접
      video_prompts_<ver>.csv  — 영상 관점: 이 영상을 어떤 문장이 점유하나 + 커버리지 하한
    """
    import csv as _csv

    keys, X, gt, src, banks = load_all()
    cam = load_cameras(keys)
    os.makedirs(REPORT_DIR, exist_ok=True)

    for v in VERSIONS:
        bank = banks[v]
        classes = sorted(set(bank["cls"].tolist()))
        gidx = {c: np.flatnonzero(bank["cls"] == c) for c in classes}
        b1, _, a1 = bank_top2_stream(X, bank)
        M = np.stack([b1[c] for c in classes], axis=1)
        pred = np.array(classes)[M.argmax(axis=1)]
        cidx = {c: i for i, c in enumerate(classes)}
        win_g = np.array([gidx[int(c)][a1[int(c)][i]] for i, c in enumerate(pred)])
        own = np.array([M[i, cidx[int(g)]] for i, g in enumerate(gt)], dtype=np.float32)
        other = np.array([max(M[i, cidx[o]] for o in classes if o != int(g))
                          for i, g in enumerate(gt)], dtype=np.float32)
        margin = own - other

        # ── 문장 관점 ──
        adopted = sorted(set(win_g.tolist()))
        rows = []
        for g in adopted:
            fr = np.flatnonzero(win_g == g)
            c = int(bank["cls"][g])
            vids = collections.Counter(src[fr].tolist())
            cams = collections.Counter(cam[fr].tolist())
            # 최근접 프레임 — 이겼든 아니든. "이 문장이 어디를 겨냥하고 있나"
            cs = X @ bank["vec"][g]
            top = np.argsort(-cs)[:5]
            rows.append({
                "gidx": g, "cls_name": CLASS_NAMES[c], "wins": int(len(fr)),
                "purity": round(float((gt[fr] == c).mean()), 4),
                "n_videos_won": len(vids), "n_cameras_won": len(cams),
                "top_videos": "|".join(f"{k}:{n}" for k, n in vids.most_common(3)),
                "cameras": "|".join(f"{k}:{n}" for k, n in cams.most_common()),
                # 최근접 5프레임: key:cos:GT:이겼나 — 절대 cos 는 뱅크 내 참고용이다
                "nearest_frames": "|".join(
                    f"{keys[i]}:{cs[i]:.3f}:{CLASS_NAMES[int(gt[i])]}:{'W' if win_g[i] == g else '-'}"
                    for i in top),
                "text": bank["prompt"][g],
            })
        rows.sort(key=lambda r: -r["wins"])
        p1 = f"{REPORT_DIR}/prompt_frames_{v}.csv"
        with open(p1, "w", newline="", encoding="utf-8") as f:
            w = _csv.DictWriter(f, fieldnames=list(rows[0]), extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)

        # ── 영상 관점 (영상 = 집계 키, 벡터 아님) ──
        vrows = []
        for vid in sorted(set(src.tolist())):
            fr = np.flatnonzero(src == vid)
            gts = collections.Counter(gt[fr].tolist())
            owners = collections.Counter(win_g[fr].tolist())
            vrows.append({
                "src_video": vid, "camera": cam[fr][0], "n_frames": int(len(fr)),
                "gt": "|".join(f"{CLASS_NAMES[int(k)]}:{n}" for k, n in gts.most_common()),
                "accuracy": round(float((pred[fr] == gt[fr]).mean()), 4),
                "n_distinct_winners": len(owners),
                "top_prompts": "|".join(f"{g}:{n}" for g, n in owners.most_common(3)),
                # 커버리지 하한 — 영상 안에서 GT 클래스가 가장 약하게 잡힌 프레임.
                # 센트로이드보다 영상 오답률 예측이 강하다(spearman −0.733 vs −0.654).
                "margin_min": round(float(margin[fr].min()), 5),
                "margin_median": round(float(np.median(margin[fr])), 5),
            })
        vrows.sort(key=lambda r: r["margin_min"])
        p2 = f"{REPORT_DIR}/video_prompts_{v}.csv"
        with open(p2, "w", newline="", encoding="utf-8") as f:
            w = _csv.DictWriter(f, fieldnames=list(vrows[0]))
            w.writeheader()
            w.writerows(vrows)

        conc = sum(r["wins"] for r in rows[:3]) / max(1, len(keys))
        multi = sum(1 for r in rows if r["n_cameras_won"] > 1)
        log(f"atlas {v}: 채택 {len(rows)}문장 → {p1} / 영상 {len(vrows)} → {p2} "
            f"| top3 문장이 프레임의 {conc:.1%} 점유 · {multi}문장이 2대 이상 카메라에서 승리")
        log(f"atlas {v}: 커버리지 최악 영상 " + ", ".join(
            f"{r['src_video'][:26]}({r['margin_min']:+.3f}, acc {r['accuracy']:.0%})"
            for r in vrows[:3]))
    log("atlas 완료")


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

    ── 규칙 개작 (2026-08-18) ──────────────────────────────────────────────
    판정(`flip`)과 이유 분해가 둘 다 **현재 판정규칙**을 따른다. 예전엔 `cache.npz` 의
    클래스별 최고 코사인을 argmax 해서 전이를 셌다 — 제품이 top-K 다수결로 넘어간 뒤에도
    "argmax 시절의 전이"를 그리고 있었다는 뜻이다. 이제 두 뱅크를 각각 `_Pruner.score()`
    로 채점하고, 규칙에 맞는 양으로 분해한다:

      RULE=topk   자기Δ = GT 클래스 **득표** 변화 / 경쟁Δ = 오답 클래스 득표 변화 (정수 표)
      RULE=argmax 자기Δ·경쟁Δ = centered rel 코사인 변화 (옛 정의 그대로 — 회귀 비교용)

    분해 라벨 어휘(자기문장 접근 / 경쟁문장 소거 / …)는 **바꾸지 않는다** — `guide` 서사와
    `report_charts.c3_flip_reasons` 가 이 키를 읽는다.

    ⚠️ `rule_flip_<vtag>`(stage_vote)과 역할이 겹치지 않는다. 축이 직교한다:
         `flip`           = **같은 규칙**으로 뱅크 A ↔ B 를 비교 (버전 축)
         `rule_flip_<tag>` = **같은 뱅크**로 k=1 ↔ k=K 를 비교 (규칙 축)
       그래서 통합·위임 대상이 아니고, 여기서 `rule_flip_*` 를 다시 쓰지도 않는다.

    ⚠️ `margin_delta` 는 규칙을 안 따른다 — 정의가 "GT 클래스 코사인 마진의 버전차"라
       판정규칙과 무관한 **기하량**이고, 뷰 30/31 의 심각도 정렬 키로 그 성질이 필요하다
       (규칙 확신도는 `vote_margin_<tag>` 가 따로 있다 — P3 중복 금지).

    `why_before`/`why_after` 에 전·후 대표 문장·코사인·(top-K 면) 득표와 **2·3위 사다리**를
    적는다. 문장 전문은 attach 의 `top_prompt_r{2,3}_<vt>` 로 같은 프레임에서 열린다.
    """
    import fiftyone as fo
    from fiftyone import ViewField as F

    keys, X, gt, src, banks = load_all()
    classes = sorted(CLASS_NAMES)
    cidx = {c: i for i, c in enumerate(classes)}
    tier = gt_tier(gt)
    if tier == "no_gt":
        raise SystemExit("flips: tier=no_gt — 정탐/오탐 전이는 GT 없이 정의되지 않는다. "
                         "원장(gt_class)을 먼저 채울 것")
    log(f"flips: 규칙 {RULE}(k={RULE_K}) / GT tier={tier} / 프레임 {len(keys):,} "
        f"— 뱅크 2벌 재채점")

    prn, state = {}, {}
    for v in VERSIONS:
        prn[v] = _Pruner(X, gt, banks[v])
        missing = [CLASS_NAMES[c] for c in classes if c not in prn[v].classes]
        if missing:
            raise SystemExit(f"flips: {v} 뱅크에 클래스 {missing} 문장이 0개 — 전이 분해 불가")
        state[v] = prn[v].score(None)
    best = {v: prn[v].best_of(state[v]) for v in VERSIONS}
    arg = {v: prn[v].class_best_local(state[v]) for v in VERSIONS}
    stacked = {v: np.stack([best[v][c] for c in classes], axis=1) for v in VERSIONS}
    rel = {v: stacked[v] - stacked[v].mean(axis=1, keepdims=True) for v in VERSIONS}
    pred = {v: prn[v]._pred_of(state[v]) for v in VERSIONS}
    pidx = {v: {c: np.flatnonzero(banks[v]["cls"] == c) for c in classes} for v in VERSIONS}
    # 규칙별 분해 재료. topk 는 표(정수), argmax 는 rel 코사인 — 단위가 달라 라벨 문구도 다르다.
    by_votes = RULE != "argmax"
    votes = ({v: vote_topk(state[v]["vals"], state[v]["idxs"])[1] for v in VERSIONS}
             if by_votes else None)
    vcls = {v: sorted(state[v]["vals"]) for v in VERSIONS} if by_votes else None
    # 사다리 2·3위 (top-K 에서만). None 이면 문구에서 통째로 빠진다 — 없는 걸 지어내지 않는다.
    ladder = {v: [(r, prn[v].rank_gidx(state[v], r - 1), prn[v].rank_cos(state[v], r - 1))
                  for r in RANK_EXTRA] for v in VERSIONS}

    def sentence(v, c, i):
        return banks[v]["prompt"][pidx[v][c][arg[v][c][i]]]

    def vote_of(v, i, c):
        """프레임 i 의 클래스 c 득표. 뱅크에 그 클래스가 없으면 0 (vote_topk 열 순서 주의)."""
        cs = vcls[v]
        return int(votes[v][i, cs.index(c)]) if c in cs else 0

    def ladder_txt(v, i):
        parts = [f"{r}위 {CLASS_NAMES[int(banks[v]['cls'][int(g[i])])]} {float(cc[i]):.3f}"
                 for r, g, cc in ladder[v] if g is not None and cc is not None and g[i] >= 0]
        return (" · 사다리 " + " / ".join(parts)) if parts else ""

    EPS = 0.005                     # argmax 경로 전용 (코사인 단위). 표는 정수라 0 이 경계다
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
        if by_votes:
            own_d = float(vote_of(vb, i, g) - vote_of(va, i, g))
            rival_d = float(vote_of(vb, i, r_wrong) - vote_of(va, i, r_wrong))
            eps, unit = 0.0, "표"
        else:
            own_d = float(rel[vb][i, cidx[g]] - rel[va][i, cidx[g]])
            rival_d = float(rel[vb][i, cidx[r_wrong]] - rel[va][i, cidx[r_wrong]])
            eps, unit = EPS, "rel"
        if flip[i] == "정탐→오탐":                  # 방향 반전해 같은 의미로 읽는다
            own_d, rival_d = -own_d, -rival_d
        up, down = own_d > eps, rival_d < -eps
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
        w_vote = (f" | 표 {CLASS_NAMES[r_wrong]} {vote_of(wrong_v, i, r_wrong)}"
                  f" vs {CLASS_NAMES[g]} {vote_of(wrong_v, i, g)}") if by_votes else ""
        r_vote = (f" | 표 {CLASS_NAMES[g]} {vote_of(right_v, i, g)}"
                  f" vs {CLASS_NAMES[r_wrong]} {vote_of(right_v, i, r_wrong)}") if by_votes else ""
        why[i] = (f"[{wrong_v}] 오답 {CLASS_NAMES[r_wrong]} «{w_sent[:80]}» "
                  f"cos {best[wrong_v][r_wrong][i]:.3f} > {CLASS_NAMES[g]} "
                  f"{best[wrong_v][g][i]:.3f}{w_vote}{ladder_txt(wrong_v, i)}\n"
                  f"[{right_v}] 정답 {CLASS_NAMES[g]} «{r_sent[:80]}» "
                  f"cos {best[right_v][g][i]:.3f} ≥ 경쟁 "
                  f"{best[right_v][r_wrong][i]:.3f}{r_vote}{ladder_txt(right_v, i)}\n"
                  f"원인: {reason[i]} (자기Δ{unit} {own_d:+.4f} / 경쟁Δ{unit} {rival_d:+.4f})")
    for v in VERSIONS:
        log(f"flips: {v} {RULE} 정답 {int((pred[v] == gt).sum()):,}/{n:,} "
            f"({(pred[v] == gt).mean():.2%})")
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
    # margin_delta = GT클래스 마진(자기−타클래스)의 버전차 — 뷰 30/31 의 심각도 정렬 키.
    # 판정규칙과 무관한 **기하량**이라 topk 개작에서도 정의를 유지한다 (docstring 참고).
    md = {}
    for i, k in enumerate(keys):
        if not ids[k]:
            continue
        g = int(gt[i])
        if g < 0:            # 미검수 행 — GT 기준 마진이 없다 (있는 척하면 정렬이 거짓말한다)
            continue
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
        # 뷰 설명에 규칙·tier 를 박는다 — 화면만 보고 "어느 규칙의 전이인가"를 알 수 있어야 한다
        ds.save_view(nm, ds.match(F("flip.label") == lab).sort_by("margin_delta", desc),
                     description=f"{VERSIONS[0]}→{VERSIONS[1]} {lab} · 규칙 {RULE}(k={RULE_K}) "
                                 f"· GT tier={tier} · 정렬 margin_delta")
    try:
        space = fo.Space(children=[
            fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
            fo.Space(children=[fo.Panel(type="Embeddings",
                                        state={"brainResult": "emb_viz",
                                               "colorByField": "flip.label"})]),
        ], orientation="horizontal")
        if "flips" in ds.list_workspaces():
            ds.delete_workspace("flips")
        ds.save_workspace("flips", space,
                          description=f"emb_viz (색: flip.label) · 규칙 {RULE}(k={RULE_K})")
    except Exception as exc:  # noqa: BLE001
        log(f"flips: 워크스페이스 실패 {exc!r}")
    # broken_reasons 도 덤프한다 — guide 의 서사(③ "지운 자석이 사실 일도 하고 있었다")가
    # 이 분해를 인용하는데 지금까지 하드코딩이었다.
    bc = collections.Counter(reason[flip == "정탐→오탐"])
    by_cls = {d: dict(collections.Counter(CLASS_NAMES[int(gt[i])]
                                          for i in np.flatnonzero(flip == d)))
              for d in ("오탐→정탐", "정탐→오탐")}
    # rule/tier 를 산출물에 박는다 — guide 가 이 JSON 을 인용하는데, 규칙이 다른 두 런의
    # 숫자가 같은 문장 틀에 들어가면 구분이 안 된다 (tier 표기 관례와 같은 이유).
    json.dump({"counts": dict(counts), "fixed_reasons": dict(rc), "broken_reasons": dict(bc),
               "by_class": by_cls, "banks": list(VERSIONS),
               "rule": RULE, "rule_k": RULE_K, "gt_tier": tier,
               "reason_unit": "votes" if by_votes else "rel_cosine"},
              open(f"{GEO}/flips.json", "w"), ensure_ascii=False)
    log(f"flips: 정탐→오탐 이유 분해 {dict(bc)}")
    log(f"flips 완료 [규칙 {RULE}(k={RULE_K}) · tier={tier}] → 필드 flip/flip_reason/"
        "why_before/after, 뷰 30/31, 워크스페이스 flips")


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
    cam = load_cameras(keys)
    cams = sorted(set(cam.tolist()))
    cache = np.load(f"{GEO}/cache.npz", allow_pickle=True)
    tag_b = VERSIONS[1].replace(".", "_")
    best_b = {c: cache[f"best_{tag_b}_{c}"] for c in CLASS_NAMES}
    classes = sorted(CLASS_NAMES)
    pred_b = np.array(classes)[np.stack([best_b[c] for c in classes], axis=1).argmax(axis=1)]
    sess = requests.Session()

    L = [f"# 프롬프트 작성 가이드 (자동 생성, 기준 뱅크: {VERSIONS[1]})",
         f"\n- 생성: {time.strftime('%Y-%m-%d %H:%M')} | 프레임 {len(keys):,}장 | "
         f"카메라 {len(cams)}대 ({', '.join(cams)})",
         "- **채택 기준**: **모든 카메라에서** 유발 FP ≤ 0.10% 이고 FN 구조율 > 5%. "
         "즉 `max_카메라(FP) ≤ 0.1%` AND `min_카메라(구조율) > 5%`.",
         "  > ⚠️ pooled(전체 합산) 기준은 쓰지 않는다 — 프레임이 가장 많은 카메라가 값을 "
         "지배해서 다른 현장으로 전이되지 않는다 (실측: pooled 1위 후보의 미검증 카메라 "
         "FN 구조율이 6케이스 전부 **0.0%**). 표에 pooled 도 같이 싣되 판정은 층화 기준이다.",
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
        if not len(fn):
            # 미검출 0 → 구조율 분모가 0이라 argmax 가 nan 위에서 임의값을 고른다.
            # 조용히 엉뚱한 이벤트절을 뽑느니 섹션을 비운다 (GT 정정으로 실제 발생: falldown).
            guide_json[cname] = {"event_clause": None, "n_fn": 0, "candidates": []}
            L.append(f"## {cname} — **미검출 0프레임**. 이 클래스는 추가할 문장이 없다 "
                     "(회수할 대상이 없으므로 구조율이 정의되지 않는다).\n")
            log(f"guide {cname}: 미검출 0 → 후보 생성 생략")
            continue
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
                win_fn = (X[fn] @ e > others_best[fn]) if len(fn) else np.zeros(0, bool)
                win_fp = X[oth] @ e > own_best_oth
                # 카메라 층화 — pooled 값은 프레임이 가장 많은 카메라가 지배한다.
                # 실측: pooled 1위 후보의 held 카메라 FN 구조율이 6케이스 전부 0.0% 였다.
                per = {}
                for k in cams:
                    fk = fn[cam[fn] == k] if len(fn) else np.array([], dtype=int)
                    ok_ = cam[oth] == k
                    per[k] = {
                        "n_fn": int(len(fk)),
                        "fn_rescue": float(win_fn[cam[fn] == k].mean()) if len(fk) else None,
                        "induced_fp": float(win_fp[ok_].mean()) if ok_.any() else None,
                    }
                rs = [d["fn_rescue"] for d in per.values() if d["fn_rescue"] is not None]
                fs = [d["induced_fp"] for d in per.values() if d["induced_fp"] is not None]
                rescue = float(win_fn.mean()) if len(fn) else 0.0
                fp = float(win_fp.mean())
                rows.append({"scene": scene, "template": tpl, "text": text,
                             "fn_rescue": rescue, "induced_fp": fp,
                             "selectivity": rescue / max(fp, 1e-4),
                             "per_camera": per,
                             "fn_rescue_min": min(rs) if rs else 0.0,
                             "induced_fp_max": max(fs) if fs else 1.0})
        # 정렬·판정 모두 **층화 기준**. pooled 는 참고 컬럼으로만 남긴다.
        rows.sort(key=lambda r: (-(r["induced_fp_max"] <= 0.001), -r["fn_rescue_min"]))
        guide_json[cname] = {"event_clause": event_clause, "n_fn": int(len(fn)),
                             "n_fn_by_camera": {k: int((cam[fn] == k).sum()) for k in cams}
                             if len(fn) else {},
                             "candidates": rows}
        L.append(f"## {cname} — 미검출 {len(fn):,}프레임, 이벤트절(자동): “{event_clause}”\n")
        if len(fn):
            L.append("카메라별 미검출: "
                     + " · ".join(f"{k} {int((cam[fn] == k).sum()):,}" for k in cams) + "\n")
        L.append("| 장면어 | 템플릿 | FN 구조율(최악카메라) | 유발 FP(최악카메라) | "
                 "pooled 구조율 | pooled FP | 판정 |")
        L.append("|---|---|---|---|---|---|---|")
        for r in rows[:10]:
            ok = "✅" if r["induced_fp_max"] <= 0.001 and r["fn_rescue_min"] > 0.05 else \
                 ("⚠️ FP" if r["induced_fp_max"] > 0.001 else "낮음")
            L.append(f"| {r['scene']} | {r['template']} | {r['fn_rescue_min']:.1%} | "
                     f"{r['induced_fp_max']:.2%} | {r['fn_rescue']:.1%} | "
                     f"{r['induced_fp']:.2%} | {ok} |")
        L.append("")
        n_pool_ok = sum(1 for r in rows if r["induced_fp"] <= 0.001 and r["fn_rescue"] > 0.05)
        n_strat_ok = sum(1 for r in rows if r["induced_fp_max"] <= 0.001 and r["fn_rescue_min"] > 0.05)
        log(f"guide {cname}: {len(rows)}후보 — pooled 통과 {n_pool_ok} → 카메라 층화 통과 "
            f"{n_strat_ok} (이벤트절: {event_clause[:40]})")
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
                  # 규칙 축 — 제품 분포-IoU 가 top-k 와 갈린 프레임 (wave 스테이지 산출)
                  ("wave", "emb_viz", f"wave_vs_topk_{vb_tag}.label"),
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
        # relabel_transition 제외 — 재라벨 이력은 층화 축이 아니다 (필드는 frames_eval.py
        # 소유라 남겨두고 노출만 뺀다. 정말 지우려면 SLIM_DROP_FIELDS 에 넣을 것)
        ("⑤ 층화", False, ["camera", "environment", "src_video", "frame_index"]),
        ("⑥ 상세", False, [f"class_best_{v4t}", f"margin_{a}", f"margin_{b}", "margin_delta"]),
        # ⑦ 판정규칙 2 — 제품 분포-IoU. top-k 필드와 섞으면 어느 규칙의 숫자인지 헷갈린다
        ("⑦ 분포IoU(wave)", False, [f"wave_pred_{v4t}", f"wave_pred_{v0t}",
                                    f"wave_vs_topk_{b}", f"wave_vs_topk_{a}"]
         + [f"wave_iou_{CLASS_NAMES[c]}_{b}" for c in sorted(CLASS_NAMES) if c != 0]),
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
    # 순위 사다리(r2/r3)는 분석 뷰에서 뺀다 — gidx 는 패널 조인 키(스펙 §4-5), 문장 원문은
    # 고카디널리티라 필터 부적합(P4/§4-4). 둘 다 모달에서 읽는 값이고, 뷰에 두면 뱅크가
    # 늘 때마다 필터가 +4 씩 는다 (G1: 분석가가 보는 필터 증가율 0).
    excl += [f for f in ds.get_field_schema() if RANK_FIELD_RE.match(f)]
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
    out = f"{REPORT_DIR}/prompt_geometry.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write(text)
    log(f"report → {out}")
    print("\n" + text)


def _load_frames_ledger() -> list[dict]:
    """frames 프로필의 **두 번째 GT 입구**. `load_all()` 과 같은 ledger.jsonl 을 읽는다.

    ⚠️ 실제 GT 소비 스테이지(`stage_score`/`stage_gtsync`/`stage_report_frames`)는
    `load_all()` 을 안 거치고 여기로 들어온다 — 순도 체크를 `load_all()` 에만 걸면 frames
    경로가 통째로 우회된다. 두 입구 모두에서 같은 계약으로 막는다.
    """
    rows = list(jsonl_load(f"{WORK}/ledger.jsonl").values())
    assert_gt_source_pure(rows, context=f"_load_frames_ledger[{PROFILE}]")
    return rows


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
    # copy=False — npz 의 vec 은 이미 float32 라 기본 astype 은 **쓸데없이 한 벌 더** 뜬다
    # (frames 187,994x1024 = 770 MB). NpzFile 은 접근마다 새로 풀어주므로 in-place 나눗셈이
    # 다른 참조를 건드릴 위험도 없다. 이 호스트는 스왑이 말라 있어 순간 할당이 곧 비용이다.
    Xall = z["vec"].astype(np.float32, copy=False)
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
    # ⚠️ `ds.select(ordered=True)` 는 데이터셋에 없는 id 를 **조용히 빼고** 반환한다
    # (실측: 요청 4 → 반환 3). 그대로 위치로 zip 하면 첫 결측 이후 weak[i] 가 pred_b[i] 와
    # 다른 프레임을 가리켜 concordance 가 무의미한 수가 된다. key 로 되짚어 정렬을 강제한다
    # (_stream_frames_embeddings 의 live 필터와 같은 방어).
    _wid, _wcls = ds.select(keys, ordered=True).values(["id", "normalized_class"])
    _wmap = dict(zip(_wid, _wcls))
    weak = [_wmap.get(k) for k in keys]
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
    # copy=False — npz 의 vec 은 이미 float32 라 기본 astype 은 **쓸데없이 한 벌 더** 뜬다
    # (frames 187,994x1024 = 770 MB). NpzFile 은 접근마다 새로 풀어주므로 in-place 나눗셈이
    # 다른 참조를 건드릴 위험도 없다. 이 호스트는 스왑이 말라 있어 순간 할당이 곧 비용이다.
    Xall = z["vec"].astype(np.float32, copy=False)
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
    A("# frames 프롬프트 뱅크 평가 리포트\n")
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


def _selftest_bankfrom() -> None:
    """`bankfrom` 행 빌더 — 클래스 이름→int 역사상, 중복 접기, fail-closed 4종.

    ⚠️ `stage_selftest` **맨 앞**에서 부른다. 뒤쪽 `vtag` assert 가 29버전 재빌드로 stale 해져
    실패하는 상태라(그건 별 트랙), 뒤에 두면 이 검증에 영원히 도달하지 못한다.
    """
    cm = {0: "normal", 2: "fire"}
    rows, dropped = _bank_rows(["A fire.", "A  fire. ", "Nothing."], ["fire", "fire", "normal"], cm)
    assert rows == [(2, "A fire."), (0, "Nothing.")], rows      # 공백만 다른 중복은 접힌다
    assert dropped == 1
    # 같은 텍스트라도 클래스가 다르면 별 행 — 텍스트만으로 접으면 안 된다
    assert len(_bank_rows(["x", "x"], ["fire", "normal"], cm)[0]) == 2
    for bad in ((["x"], ["smoke"]), ([" "], ["fire"]),         # 모르는 라벨 / 빈 문장
                (["(텍스트 없음 #3)"], ["fire"]),               # 벡터전용 뱅크 자리표시자
                (["A fire.", "(텍스트 없음 #9)"], ["fire", "fire"])):   # 일부만 섞여도 거부
        try:
            _bank_rows(list(bad[0]), list(bad[1]), cm)
            raise AssertionError(f"fail-closed 안 됨: {bad}")
        except SystemExit:
            pass
    assert is_placeholder("(텍스트 없음 #0)") and is_placeholder("  (텍스트 없음 #12)")
    assert not is_placeholder("A small fire blazes in the center in daylight.")
    assert not is_placeholder("") and not is_placeholder(None)
    log("selftest: bankfrom OK")


def _with_rule(rule: str, k: int):
    """RULE/RULE_K 를 임시 교체하는 컨텍스트 — 두 규칙을 한 프로세스에서 태우기 위한 것.

    두 값은 import 시점에 env 로 굳는 모듈 전역이고 함수들이 호출 시점에 읽는다. 테스트가
    양쪽 분기를 다 타려면 여기서 갈아끼우는 수밖에 없다 (프로덕션 경로는 안 건드린다).
    """
    import contextlib

    @contextlib.contextmanager
    def _cm():
        g = globals()
        old = (g["RULE"], g["RULE_K"])
        g["RULE"], g["RULE_K"] = rule, k
        try:
            yield
        finally:
            g["RULE"], g["RULE_K"] = old
    return _cm()


def _selftest_topk_ranks() -> None:
    """top-K 순위 사다리 — 순위의 정의·필드 명명·r2/r3 산출 정확성.

    ⚠️ `stage_selftest` **앞쪽**에서 부른다. 뒤쪽 `vtag` assert 가 29버전 재빌드로 stale 해져
    실패하는 상태라(별 트랙), 뒤에 두면 이 검증에 영원히 도달하지 못한다 —
    `_selftest_bankfrom` 이 같은 이유로 맨 앞에 있다.

    고정하는 것 7가지:
      ① sel 이 **전역 코사인 내림차순** (= rank 의 정의. 득표순이 아님)
      ② rank 0 == `top1_gidx` == **argmax 규칙의 승자** (winner_gidx 가 규칙 전환에도 불변)
      ③ rank r == 순진 계산의 전역 r위 문장
      ④ RULE=argmax state 에서는 r≥1 이 None (클래스 사다리로 위조하지 않는다)
      ⑤ 필드명이 D7 리졸버 정규식에서 **별 계열**로 파싱된다 (기존 계열 오염 금지)
      ⑥ **under-fill 경계** — 뱅크 문장이 K 보다 적어 슬롯이 비면 `rank_gidx`=−1 인 그 자리에
         `rank_cos` 는 **NaN**. 센티널 −2.0 이 새면 "코사인 −2.0" 이라는 없는 값이 필드로 나간다
      ⑦ **동점 재현성** — 동일 벡터가 여러 클래스에 있어도 사다리 순서·값이 매 실행 동일하고,
         `rank_cos` 가 `rank_gidx` 가 가리킨 **그 문장**의 코사인과 일치한다
    """
    rng = np.random.default_rng(7)
    n, d, m = 40, 12, 37
    X = rng.normal(size=(n, d)).astype(np.float32)
    X /= np.linalg.norm(X, axis=1, keepdims=True)
    V = rng.normal(size=(m, d)).astype(np.float32)
    V /= np.linalg.norm(V, axis=1, keepdims=True)
    cls = np.array([0] * 14 + [1] * 8 + [2] * 8 + [3] * 7, dtype=np.int64)
    bank = {"vec": V, "cls": cls, "prompt": [f"s{i}" for i in range(m)]}
    gt = rng.integers(0, 4, n).astype(np.int64)
    S = X @ V.T                                        # 순진 전역 코사인 [n, m]
    K = 5

    with _with_rule("topk", K):
        pr = _Pruner(X, gt, bank)
        st = pr.score(None)
        assert isinstance(st, dict), "RULE=topk 인데 state 가 dict 가 아니다"
        # ① sel 은 코사인 내림차순 — vote_topk 의 argsort 계약
        gid = {c: np.flatnonzero(cls == c) for c in sorted(set(cls.tolist()))}
        sel = st["sel"]
        for i in range(n):
            cos = [float(S[i, gid[int(c)][int(j)]]) for c, j in sel[i] if j >= 0]
            assert all(cos[t] >= cos[t + 1] - 1e-6 for t in range(len(cos) - 1)), \
                f"sel 이 코사인 내림차순이 아니다 (프레임 {i}): {cos}"
        # ②③ rank r == 순진 전역 r위. r=0 은 argmax 승자와 같은 문장이어야 한다
        naive = np.argsort(-S, axis=1)                 # [n, m] 전역 내림차순
        for r in range(3):
            g = pr.rank_gidx(st, r)
            assert g is not None and g.shape == (n,), f"rank {r} 미산출"
            assert (g == naive[:, r]).all(), \
                f"rank {r} 가 전역 {r + 1}위가 아니다: {g[:5]} vs {naive[:5, r]}"
            cc = pr.rank_cos(st, r)
            assert np.allclose(cc, S[np.arange(n), naive[:, r]], atol=1e-6), \
                f"rank_cos {r} 가 그 문장의 코사인이 아니다"
        assert (pr.top1_gidx(st) == naive[:, 0]).all(), "top1_gidx != 전역 1위"
        assert pr.rank_gidx(st, K) is None, "사다리 폭(K) 밖인데 None 이 아니다"
        # best_of/class_best_local 이 클래스별 1위를 정확히 가리킨다 (attach 의 argmax 슬롯 근거)
        b, a = pr.best_of(st), pr.class_best_local(st)
        for c in gid:
            assert np.allclose(b[c], S[:, gid[c]].max(1), atol=1e-6), f"best_of c={c}"
            assert np.allclose(S[np.arange(n), gid[c][a[c]]], b[c], atol=1e-6), \
                f"class_best_local 이 클래스 1위를 안 가리킨다 c={c}"

    with _with_rule("argmax", 1):
        pr_a = _Pruner(X, gt, bank)
        st_a = pr_a.score(None)
        assert isinstance(st_a, tuple), "RULE=argmax 인데 state 가 4-tuple 이 아니다"
        # ② 규칙이 달라도 **1위 문장은 같다** — max_c max_p cos == max_p cos.
        #    `winner_gidx_<tag>` 를 top1 대표로 유지해도 값이 안 바뀐다는 근거가 이것이다.
        assert (pr_a.top1_gidx(st_a) == naive[:, 0]).all(), "argmax 승자 != 전역 1위"
        assert (pr_a.rank_gidx(st_a, 0) == naive[:, 0]).all()
        # ④ 2·3위는 argmax state 에 **없다** — 클래스 사다리로 위조하지 않는다
        for r in (1, 2):
            assert pr_a.rank_gidx(st_a, r) is None, f"argmax 인데 rank {r} 를 지어냈다"
            assert pr_a.rank_cos(st_a, r) is None

    # ⑥ under-fill 경계 — 뱅크 문장 2개 < K 라 sel 슬롯 2·3·4번이 빈다.
    #    (클래스가 적은 것만으로는 안 된다 — 다른 클래스가 사다리를 메운다. 뱅크 **전체**가
    #     K 보다 작아야 빈 슬롯이 생긴다.)
    with _with_rule("topk", K):
        Vs = V[:2].copy()
        bs = {"vec": Vs, "cls": np.array([0, 2], dtype=np.int64), "prompt": ["a", "b"]}
        prs = _Pruner(X, gt, bs)
        sts = prs.score(None)
        Ss = X @ Vs.T
        assert sts["sel"].shape[1] >= 3, "이 경계 검증은 사다리 폭 3 이상을 전제한다"
        g0s, c0s = prs.rank_gidx(sts, 0), prs.rank_cos(sts, 0)
        assert (g0s == Ss.argmax(1)).all(), "under-fill 뱅크에서 1위가 틀렸다"
        assert np.isfinite(c0s).all() and np.allclose(c0s, Ss.max(1), atol=1e-6), \
            "살아있는 슬롯의 rank_cos 가 코사인과 다르다"
        g2s, c2s = prs.rank_gidx(sts, 2), prs.rank_cos(sts, 2)
        assert g2s is not None and (g2s == -1).all(), "빈 슬롯인데 gidx 가 −1 이 아니다"
        assert np.isnan(c2s).all(), "빈 슬롯 rank_cos 가 NaN 이 아니다"
        assert not (c2s == -2.0).any(), \
            "bank_topk_stream 의 채움 센티널 −2.0 이 rank_cos 로 누출됐다 (없는 값이 필드로 나간다)"

    # ⑦ 동점 재현성 — 같은 벡터를 여러 클래스에 심어 정확한 동점을 만든다
    with _with_rule("topk", K):
        Vt = V.copy()
        for pos in (0, 14, 22, 30):                 # 각 클래스 첫 문장을 동일 벡터로
            Vt[pos] = V[0]
        for pos in (1, 15, 23, 31):                 # 두 번째 동점 그룹
            Vt[pos] = V[15]
        bt = {"vec": Vt, "cls": cls, "prompt": bank["prompt"]}
        prt = _Pruner(X, gt, bt)
        St = X @ Vt.T
        st1 = prt.score(None)
        st2 = prt.score(None)
        assert (st1["sel"] == st2["sel"]).all(), "동점에서 사다리가 실행마다 달라진다"
        for r in range(3):
            g, cc = prt.rank_gidx(st1, r), prt.rank_cos(st1, r)
            assert (g == prt.rank_gidx(st2, r)).all(), f"rank {r} 재현 실패"
            live = g >= 0
            # 핵심: 값이 **그 문장**의 코사인이어야 한다 (독립 재정렬이면 동점에서 어긋난다)
            assert np.allclose(cc[live], St[np.flatnonzero(live), g[live]], atol=1e-6), \
                f"rank_cos 가 rank_gidx 가 가리킨 문장의 코사인이 아니다 (r={r}, 동점)"
        c0t, c1t, c2t = (prt.rank_cos(st1, r) for r in range(3))
        assert (c0t >= c1t - 1e-6).all() and (c1t >= c2t - 1e-6).all(), "사다리가 내림차순이 아니다"

    # ⑤ 필드 명명 — 형제 필드의 접미사 세대 승계 + D7 계열 파싱
    d7 = re.compile(r"^(?P<fam>.+?)_(?P<tag>v[\d_]+(?:-[\w]+)?)$")
    assert rank_gidx_field("v1084", 2) == "winner_gidx_r2_v1084"
    assert rank_prompt_field("v1_0_8_4", 3) == "top_prompt_r3_v1_0_8_4"
    for f, want_fam in ((rank_gidx_field(vtag("v1.0.8.4"), 2), "winner_gidx_r2"),
                        (rank_gidx_field(vtag("v1.0.13.2"), 3), "winner_gidx_r3"),
                        (rank_prompt_field("v1_0_8_4", 2), "top_prompt_r2"),
                        (rank_prompt_field("v1_0_8_4-prune205", 2), "top_prompt_r2")):
        mm = d7.match(f)
        assert mm and mm.group("fam") == want_fam, \
            f"{f} → fam {mm and mm.group('fam')!r} (기대 {want_fam!r}) — 기존 계열을 오염시킨다"
        assert RANK_FIELD_RE.match(f), f"{f} 가 RANK_FIELD_RE 에 안 걸린다 (slim 이 못 가린다)"
    # 1위 필드는 순위 정규식에 **걸리면 안 된다** (걸리면 slim 이 조인 키를 뷰에서 지운다)
    for f in ("winner_gidx_v1084", "top_prompt_v1_0_8_4", "pred_margin_v1084"):
        assert not RANK_FIELD_RE.match(f), f"{f} 가 순위 필드로 오인된다"
    log("selftest: topk 순위 사다리 OK (정의=전역 코사인 내림차순 / r1==argmax 승자 / "
        "argmax 는 r2·r3 미정의 / under-fill 빈슬롯 cos=NaN / 동점 재현성·값-문장 정합)")


def _selftest_site_scope() -> None:
    """사이트 범위(화면4) 재료 — 벡터화 기여쌍·그룹 승수·널모델 통계량·라벨 계약.

    ⚠️ `stage_selftest` **앞쪽**에서 부른다 (`_selftest_bankfrom`·`_selftest_topk_ranks` 와
       같은 이유 — 뒤쪽 `vtag` assert 가 stale 해 도달을 막는다).

    고정하는 것 6가지:
      ① `contrib_pairs` == `_Pruner.contrib_frames` (두 규칙 모두, 집합 단위)
      ② `group_win_matrix` == 순진 이중루프 카운트
      ③ scope 라벨: sourceh 문자열 불변 + 단위만 프로필로 갈린다
      ④ 널모델 ②(재인코딩) 통계량의 양 극단 — 완전 재인코딩 V=1 / 독립축 V≈0
      ⑤ 순열 널모델이 **사이트특이를 실제로 구별한다** (구성상 특이 vs 섞은 것)
      ⑥ `n_win` 은 그룹 축 순서·프레임 정렬에 불변 (stage_site 가 그룹 정렬을 하므로)
    """
    rng = np.random.default_rng(11)
    n, d, m = 60, 10, 30
    X = rng.normal(size=(n, d)).astype(np.float32)
    X /= np.linalg.norm(X, axis=1, keepdims=True)
    V = rng.normal(size=(m, d)).astype(np.float32)
    V /= np.linalg.norm(V, axis=1, keepdims=True)
    cls = np.array([0] * 12 + [1] * 6 + [2] * 6 + [3] * 6, dtype=np.int64)
    bank = {"vec": V, "cls": cls, "prompt": [f"s{i}" for i in range(m)]}
    gt = rng.integers(0, 4, n).astype(np.int64)
    gcode = np.array([i % 4 for i in range(n)], dtype=np.int64)

    for rule, k in (("topk", 5), ("argmax", 1)):
        with _with_rule(rule, k):
            pr = _Pruner(X, gt, bank)
            st = pr.score(None)
            want = pr.contrib_frames(st)
            gi, fi = contrib_pairs(pr, st)
            got: dict[int, set] = {}
            for g, f in zip(gi.tolist(), fi.tolist()):
                got.setdefault(g, set()).add(f)
            assert set(got) == set(want), f"[{rule}] 기여 문장 집합 불일치"
            for g in want:                                  # ①
                assert got[g] == set(want[g].tolist()), f"[{rule}] 문장 {g} 기여 프레임 불일치"
            # ② 그룹 승수 == 순진 카운트
            W = group_win_matrix(gi, fi, gcode, m, 4)
            naive = np.zeros((m, 4), dtype=np.int64)
            for g, f in zip(gi.tolist(), fi.tolist()):
                naive[g, gcode[f]] += 1
            assert (W == naive).all(), f"[{rule}] group_win_matrix 불일치"
            # ⑥ 프레임을 재정렬해도 문장별 그룹 수는 같다
            perm = rng.permutation(n)
            pr2 = _Pruner(X[perm], gt[perm], bank)
            st2 = pr2.score(None)
            gi2, fi2 = contrib_pairs(pr2, st2)
            W2 = group_win_matrix(gi2, fi2, gcode[perm], m, 4)
            assert ((W > 0).sum(1) == (W2 > 0).sum(1)).all(), f"[{rule}] n_win 이 정렬에 의존한다"

    # ③ 라벨 계약 — sourceh 문자열은 저장뷰가 문자 등식으로 쓴다
    assert scope_labels("대") == SCOPE_LABELS == {1: "사이트특이 (1대)", 2: "공통 (2대)",
                                                  3: "공통 (3대+)"}
    assert scope_labels("곳")[1] == "사이트특이 (1곳)" and scope_labels("곳")[3] == "공통 (3곳+)"

    # ④ 재인코딩 통계량의 양 극단
    a = np.array([i % 3 for i in range(300)])
    assert abs(_cramers_v(a, a * 1, 3, 3) - 1.0) < 1e-9, "완전 재인코딩인데 V != 1"
    acc, base = _predict_acc(a, a, 3, 3)
    assert acc == 1.0 and abs(base - 1 / 3) < 0.01
    b_ind = np.array([(i // 3) % 3 for i in range(300)])     # a 와 독립
    assert _cramers_v(a, b_ind, 3, 3) < 0.1, "독립축인데 V 가 크다"
    # event_tier: 경계는 minn_tier 와 같고 0 칸 이름만 다르다 (GT 얘기로 오독 금지)
    assert [event_tier(k) for k in (0, 1, 29, 30, 99, 100)] == \
        ["no_event", "counts_only", "counts_only", "exploratory", "exploratory", "reportable"]
    assert minn_tier(0) == "no_gt" and event_tier(0) == "no_event"

    # ⑤ 순열 널모델이 진짜 사이트특이를 구별하나 — 문장 0..3 은 각자 한 그룹에서만 이기고,
    #    문장 4 는 전 그룹에서 이긴다. 섞으면 큰 문장은 그대로 공통, 특이 문장도 공통이 된다.
    si = np.array([0, 1, 2, 3] + [4] * 40, dtype=np.int64)
    fi = np.array([0, 1, 2, 3] + list(range(40)), dtype=np.int64)
    gc = np.array([i % 4 for i in range(40)], dtype=np.int64)
    nw = (group_win_matrix(si, fi, gc, 5, 4) > 0).sum(1)
    assert nw.tolist() == [1, 1, 1, 1, 4], nw.tolist()
    obs = float((nw[nw > 0] >= 2).mean())
    r2 = np.random.default_rng(3)
    nulls = []
    for _ in range(40):
        gp = gc[r2.permutation(40)]
        nulls.append(float(((group_win_matrix(si, fi, gp, 5, 4) > 0).sum(1)[nw > 0] >= 2).mean()))
    assert obs < float(np.mean(nulls)) + 1e-12, "널모델이 사이트특이를 못 구별한다"
    log("selftest: site scope OK (기여쌍 벡터화 동치 / 그룹 승수 / 라벨 계약 / "
        "재인코딩 V 양극단 / 순열 널모델 방향)")


def stage_selftest() -> None:
    """데이터 불필요 자가검증 — 스트리밍 리덕션 == 순진 행렬곱, crosswalk fail-closed, min-n."""
    _selftest_bankfrom()
    _selftest_topk_ranks()
    _selftest_site_scope()
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

    # reach == 순진 계산 (자기 클래스를 뺀 최고점 대비 최대 여유)
    cls_arr = sorted(set(bank["cls"].tolist()))
    best_naive = {c: (X @ V[np.flatnonzero(bank["cls"] == c)].T).max(axis=1) for c in cls_arr}
    grp = np.array(["a"] * 250 + ["b"] * 250)
    rc, rcg = bank_reach_stream(X, bank, best_naive, groups=grp, batch=64, block=32)
    for c in cls_arr:
        oth = np.max(np.stack([best_naive[o] for o in cls_arr if o != c]), axis=0)
        for g in np.flatnonzero(bank["cls"] == c)[:5]:
            assert abs(float(rc[g]) - float((X @ V[g] - oth).max())) < 1e-5, f"reach 불일치 g={g}"
            assert abs(float(rcg["a"][g]) - float((X[:250] @ V[g] - oth[:250]).max())) < 1e-5, \
                f"그룹 reach 불일치 g={g}"
    # 승자는 정의상 reach>0 (전역 1위였다면 자기 클래스 밖 최고점을 이겼다는 뜻)
    M = np.stack([best_naive[c] for c in cls_arr], axis=1)
    predn = np.array(cls_arr)[M.argmax(axis=1)]
    for c in cls_arr:
        idx = np.flatnonzero(bank["cls"] == c)
        w = (X[predn == c] @ V[idx].T).argmax(axis=1) if (predn == c).any() else []
        for j in np.unique(w):
            assert rc[idx[j]] > 0, "전역 승자인데 reach<=0"

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
    # gt_tier: "배열이 있다 ≠ GT 가 있다". 전부 −1 인 원장을 no_gt 로 안 부르면 선택 산출물이
    # 성적표로 읽힌다 — prune 리포트 최상단 표기가 이 함수 하나에 걸려 있다.
    assert gt_tier(None) == "no_gt"
    assert gt_tier(np.array([], dtype=np.int64)) == "no_gt"
    assert gt_tier(np.full(500, -1, dtype=np.int64)) == "no_gt", "전부 미검수(−1)면 no_gt"
    assert gt_tier(np.array([-1] * 495 + [0] * 5)) == "counts_only"
    assert gt_tier(np.zeros(120, dtype=np.int64)) == "reportable"

    # gt_source 순도 — 프로필마다 기대값이 다르고, None(구 행)은 어디서나 통과해야 한다.
    _prof = PROFILE
    try:
        set_profile("sourceh")
        assert_gt_source_pure([{"gt_source": "nas_folder"}, {"gt_source": None}, {}],
                              context="selftest")
        for foreign in ("ls_finalized", "folder", "caption"):
            try:
                assert_gt_source_pure([{"gt_source": "nas_folder"}, {"gt_source": foreign}],
                                      context="selftest")
            except RuntimeError:
                pass
            else:
                raise AssertionError(f"sourceh 원장에 {foreign} 가 섞였는데 통과했다")
        set_profile("frames")
        assert_gt_source_pure([{"gt_source": "ls_finalized"}, {"gt_source": None}], context="selftest")
        try:
            assert_gt_source_pure([{"gt_source": "nas_folder"}], context="selftest")
        except RuntimeError:
            pass
        else:
            raise AssertionError("frames 원장에 nas_folder 가 섞였는데 통과했다")
        # sourcei 는 4값이 정상 공존 — 'none'(문자열)과 None(구 행)이 둘 다 통과해야 한다
        set_profile("sourcei")
        assert_gt_source_pure([{"gt_source": s} for s in
                               ("folder", "filename", "caption", "none")] + [{"gt_source": None}],
                              context="selftest")
        try:
            assert_gt_source_pure([{"gt_source": "ls_finalized"}], context="selftest")
        except RuntimeError:
            pass
        else:
            raise AssertionError("sourcei 원장에 ls_finalized 가 섞였는데 통과했다")
    finally:
        set_profile(_prof)

    # 프로브 후보 CSV 로더 — 미지정이면 하드코딩과 **동일 객체**(하위호환), 지정하면 대체.
    import tempfile as _tf
    _saved = os.environ.pop("PROBE_CANDIDATES_CSV", None)
    try:
        assert load_probe_candidates() is PROBE_CANDIDATES, "미지정인데 기존 dict 가 아니다"
        with _tf.TemporaryDirectory() as td:
            good = f"{td}/probe.csv"
            # BOM + 공백 패딩 — 엑셀이 실제로 내놓는 형태
            with open(good, "w", encoding="utf-8-sig") as f:
                f.write("class,prompt\nfire, A bright fire is burning. \n\nfire,Flames spread.\n"
                        "smoke,Thin white smoke drifts upward.\n")
            os.environ["PROBE_CANDIDATES_CSV"] = good
            got = load_probe_candidates()
            assert got == {"fire": ["A bright fire is burning.", "Flames spread."],
                           "smoke": ["Thin white smoke drifts upward."]}, got
            assert "falldown" not in got, "CSV 는 병합이 아니라 **대체**여야 한다"

            bad = f"{td}/bad.csv"
            with open(bad, "w", encoding="utf-8") as f:
                f.write("class,prompt\nvehicle,A car is parked.\n")
            os.environ["PROBE_CANDIDATES_CSV"] = bad
            try:
                load_probe_candidates()
            except SystemExit:
                pass
            else:
                raise AssertionError("어휘 밖 class 인데 통과했다 (fail-fast 아님)")

            nocol = f"{td}/nocol.csv"
            with open(nocol, "w", encoding="utf-8") as f:
                f.write("klass,text\nfire,x\n")
            os.environ["PROBE_CANDIDATES_CSV"] = nocol
            try:
                load_probe_candidates()
            except SystemExit:
                pass
            else:
                raise AssertionError("컬럼 누락인데 통과했다")

            # 비이벤트 클래스만 담긴 CSV — 이게 통과하면 stage_gap 이 순회를 안 해서
            # "프로브 0건"이 조용히 된다. normal 은 어느 프로필에서도 거부돼야 한다.
            for cls_only in ("normal", "smoking"):
                only = f"{td}/only_{cls_only}.csv"
                with open(only, "w", encoding="utf-8") as f:
                    f.write(f"class,prompt\n{cls_only},Nothing is happening.\n")
                os.environ["PROBE_CANDIDATES_CSV"] = only
                for prof in ("sourceh", "frames", "sourcei"):
                    set_profile(prof)
                    try:
                        load_probe_candidates()
                    except SystemExit:
                        pass
                    else:
                        raise AssertionError(
                            f"{prof}: 비이벤트 클래스 {cls_only!r} 만 있는 CSV 가 통과했다 "
                            "— 프로브 0건이 조용히 된다")
            set_profile(_prof)
    finally:
        set_profile(_prof)
        os.environ.pop("PROBE_CANDIDATES_CSV", None)
        if _saved is not None:
            os.environ["PROBE_CANDIDATES_CSV"] = _saved
    # 사이드바 서브경로는 1단까지만 — 3단이 새면 App 모달이 TypeError 로 죽는다
    uni = ["class_best_v1", "class_best_v1.classifications",
           "class_best_v1.classifications.label", "flip_reason", "flip_reason.before"]
    assert sidebar_subpaths(["class_best_v1", "flip_reason"], uni) == [
        "class_best_v1.classifications", "flip_reason.before"]
    # 2026-08-11 전 파트 조인 계약 (vtag docstring). 옛 "v080" 기대값이 여기 남아
    # selftest 를 죽이는 바람에 frames_bank_eval.sh (1단계=selftest, set -e) 가
    # 통째로 막혔었다 (2026-08-18) — 기대값은 코드가 아니라 계약을 따라간다.
    assert vtag("v1.0.8.0") == "v1080" and vtag("v1.0.8.4") == "v1084"
    assert vtag("v1.0.9.0") == "v1090", "새 버전 값이 옛 이름 필드에 덮이면 안 된다"
    assert vtag("v1.0.5.0") != vtag("v2.0.5.0"), "마지막 3파트 조인이면 붕괴하는 실충돌 쌍"
    # wave: 제품 compute_hist_iou 재현 + bin 별 LOO 지름길 == 브루트포스 LOO.
    # 지름길("같은 bin 이면 ΔIoU 가 같다")이 틀리면 문장 기여도 전체가 조용히 거짓이 된다.
    ha, hb = np.array([0.5, 0.3, 0.2]), np.array([0.2, 0.3, 0.5])
    assert abs(float(hist_iou(ha, hb))
               - float(np.minimum(ha, hb).sum() / np.maximum(ha, hb).sum())) < 1e-12
    wb = 8
    Xw = rng.normal(size=(6, 16)).astype(np.float32)
    Xw /= np.linalg.norm(Xw, axis=1, keepdims=True)
    Vw = rng.normal(size=(40, 16)).astype(np.float32)
    Vw /= np.linalg.norm(Vw, axis=1, keepdims=True)
    clsw = np.array([0] * 20 + [1] * 10 + [2] * 10)
    gtw = np.array([0, 0, 1, 1, 2, 2])
    bw = {"vec": Vw, "cls": clsw, "prompt": [f"p{i}" for i in range(40)]}
    iouw, evw, gw, _ = wave_stream(Xw, bw, gtw, bins=wb, chunk=4)   # chunk 경계도 태운다
    Sw = Xw @ Vw.T

    def _hists(i, edges, keep=None):
        k = np.ones(len(clsw), bool) if keep is None else keep
        out = {}
        for c in (0, 1, 2):
            m = (clsw == c) & k
            out[c] = np.histogram(Sw[i][m], bins=edges)[0] / max(int(m.sum()), 1)
        return out

    for i in range(len(Xw)):
        ed = np.linspace(Sw[i].min(), Sw[i].max(), wb + 1)
        h = _hists(i, ed)
        for j, e in enumerate(evw):
            assert abs(float(iouw[i, j]) - float(hist_iou(h[0], h[e]))) < 1e-6, \
                "wave IoU 가 np.histogram 재현과 불일치 (bin 배정 규칙 확인)"
    for p in (0, 7, 19, 20, 25, 35):
        c = int(clsw[p])
        rows = np.flatnonzero(gtw == c)
        tot = 0.0
        for i in rows:
            # ⚠️ edges 는 **원래 뱅크 기준으로 고정**한다. 제거된 문장이 그 프레임의 전역
            #    min/max 였다면 진짜 LOO 는 격자까지 움직이지만(문장 2/M개), 측정 격자를
            #    고정하는 쪽이 counterfactual 로 더 옳고 wave_stream 도 그렇게 계산한다.
            ed = np.linspace(Sw[i].min(), Sw[i].max(), wb + 1)
            h = _hists(i, ed)
            keep = np.ones(len(clsw), bool)
            keep[p] = False
            h2 = _hists(i, ed, keep)
            if c == 0:
                tot += float(np.mean([hist_iou(h2[0], h[e]) - hist_iou(h[0], h[e])
                                      for e in evw]))
            else:
                tot += float(hist_iou(h[0], h2[c]) - hist_iou(h[0], h[c]))
        assert abs(float(gw[p]) - tot / len(rows)) < 1e-6, \
            f"bin 별 LOO 지름길 != 브루트포스 LOO (문장 {p}, 클래스 {c})"

    # 문장별 최근접 프레임: 청크 리덕션 == 순진 행렬곱 (축이 프레임이 아니라 문장이다)
    nb, ni = nearest_frame_stream(X, V, chunk=32)
    S_full = X @ V.T
    assert np.allclose(nb, S_full.max(axis=0), atol=1e-6), "nearest cos 불일치"
    assert np.allclose(nb, np.einsum("ij,ij->i", X[ni], V), atol=1e-6), \
        "nearest idx 가 그 cos 를 가리키지 않음"
    assert purity_bin(0.0) == "0-25%" and purity_bin(0.5) == "50-75%" and purity_bin(1.0) == "90-100%"
    assert loo_bin(12) == "유해 +10↑" and loo_bin(0) == "중립 0" and loo_bin(-3).startswith("유익")

    log("selftest OK")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=["bank", "bankfrom", "analyze", "ablate", "attach", "gap", "prune", "atlas",
                                      "wave", "promptmap", "attrs", "viz", "flips",
                                      "guide", "slim", "report", "gen", "screens", "vote", "probecache", "all", "selftest",
                                      "score", "gtsync", "site"])
    ap.add_argument("--profile", choices=list(PROFILES),
                    default=os.environ.get("BANK_PROFILE", "sourceh"))
    ap.add_argument("--csv", help="bank 스테이지: 프롬프트 CSV 경로")
    ap.add_argument("--version", help="bank/bankfrom 스테이지: 버전 이름 (npz·CSV 파일명)")
    ap.add_argument("--tag", help="bankfrom 스테이지: App 에서 붙인 표본 태그")
    ap.add_argument("--notes", help="bankfrom 스테이지: 이 버전을 왜 만들었는지 (provenance 에 저장)")
    ap.add_argument("--mem-budget-gb", type=float, default=4.0)
    args = ap.parse_args()
    set_profile(args.profile)

    # 프로필 분기보다 앞 — bankfrom 은 프롬프트 데이터셋의 문자열 몇천 개만 읽으므로 세 프로필
    # 모두에서 성립하고, **메모리 가드 대상이 아니다** (그 가드는 유사도 행렬 1.5GB 스테이지용).
    # 아래 분기의 허용목록 3벌도 건드리지 않는다.
    if args.stage == "bankfrom":
        if not (args.tag and args.version):
            raise SystemExit("bankfrom 스테이지는 --tag 와 --version 이 필요하다")
        stage_bankfrom(args.tag, args.version, args.notes)
        return

    assert_mem_budget(args.mem_budget_gb)
    os.makedirs(GEO, exist_ok=True)

    if PROFILE == "sourcei":
        # 허용 스테이지만 — 팩토리얼/절제/플립은 **동일도메인 뱅크 2벌**이 있어야 성립하고
        # sourcei 는 v1.0.8.0 단일 뱅크다. guide 는 클래스별 GT 분모가 필요한데 fire 5구간
        # 이라 성립하지 않는다. slim 은 source-h 하드코딩 리스트라 다른 데이터셋을 파괴한다.
        # probecache 는 단일 뱅크로도 성립한다 (프레임별 클래스 점수 캐시 → App 프롬프트 프로브).
        table = {"attach": stage_attach, "vote": stage_vote, "wave": stage_wave,
                 "promptmap": stage_promptmap, "attrs": stage_attrs,
                 "probecache": stage_probecache, "selftest": stage_selftest}
        stages = ["attach", "wave", "promptmap", "attrs"] if args.stage == "all" else [args.stage]
        for st in stages:
            log(f"───── stage: {st} (profile=sourcei) ─────")
            if st not in table:
                raise SystemExit(
                    f"{st} 는 sourcei 프로필에서 성립하지 않는다 — 허용: {sorted(table)}. "
                    "(뱅크 2벌 비교/GT 분모/slim 하드코딩 사유)")
            table[st]()
        log("완료")
        return

    if PROFILE == "frames":
        # ⚠️ `attach` 는 2026-08-18 에 이 목록에서 **빠졌다** — GT 불필요 스테이지라
        #    frames 에서 성립한다는 판정이 계획서 §3(이식 판정 "그대로")에 있고, 코드도
        #    프로필 3지점(입구/조인/GT 의존 산출)만 갈아 끼우면 그대로 돈다.
        #    나머지(팩토리얼=동일도메인 뱅크 2벌, guide/flips/prune=GT 분모)는 여전히 sourceh 전용.
        sourceh_only = {"analyze", "ablate", "flips", "guide", "slim", "prune", "atlas", "vote"}
        table = {"score": stage_score, "gap": stage_gap_frames, "viz": stage_viz_frames,
                 "gtsync": stage_gtsync, "report": stage_report_frames,
                 "wave": stage_wave, "promptmap": stage_promptmap, "attach": stage_attach,
                 "site": stage_site, "attrs": stage_attrs, "selftest": stage_selftest}
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
    stages = ["analyze", "ablate", "gap", "flips", "prune", "atlas", "viz", "guide", "slim", "report"] \
        if args.stage == "all" else [args.stage]
    for st in stages:
        log(f"───── stage: {st} ─────")
        if st == "selftest":
            stage_selftest()
            continue
        if st in ("score", "gtsync"):
            raise SystemExit(f"{st} 는 frames 프로필 전용")
        if st == "site":
            raise SystemExit("site 는 frames 프로필 전용 — sourceh 의 같은 축은 `screens` 가 "
                             "카메라 기준으로 이미 낸다 (winner_site_scope_<tag>)")
        if st == "bank":
            if not (args.csv and args.version):
                raise SystemExit("bank 스테이지는 --csv 와 --version 이 필요하다")
            stage_bank(args.csv, args.version)
            continue
        {"analyze": stage_analyze, "ablate": stage_ablate, "gap": stage_gap, "viz": stage_viz,
         "prune": stage_prune, "atlas": stage_atlas, "attach": stage_attach,
         "wave": stage_wave, "promptmap": stage_promptmap, "attrs": stage_attrs,
         "flips": stage_flips, "guide": stage_guide, "slim": stage_slim,
         "gen": stage_gen, "screens": stage_screens, "vote": stage_vote,
         "probecache": stage_probecache,
         "report": stage_report}[st]()
    log("완료")


if __name__ == "__main__":
    sys.exit(main())
