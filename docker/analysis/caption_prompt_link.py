#!/usr/bin/env python3
"""캡션 11,978건 ↔ 프롬프트 뱅크 **양방향** 연동 (2026-08-19).

`frames` 데이터셋은 혼합 모달리티다 — frame 187,994 + caption 11,978 이 **같은 필드**에
들어 있다(`merge_frames_captions.py`). 지금까지 뱅크 분석은 전부 frame 쪽만 봤고
(`prompt_geometry.stage_attach` 는 원장이 frame 만 담아 자동으로 걸러진다), 캡션은
"임베딩은 있는데 뱅크와 아무 관계도 없는" 상태로 남아 있었다. 이 모듈이 그 둘을 잇는다.

두 방향을 **한 파일 두 서브커맨드**로 낸다:

    link            캡션 → 뱅크.  캡션마다 최근접 문장 top1~3 을 `frames` 의 **캡션 문서에만** 기록
    enrich-prompts  뱅크 → 캡션.  `frames-prompts`(문장 1개 = 표본 1개) 에 캡션을 별 모달리티로 편입

둘 다 기본 **dry-run**(계획 표만 출력), `--apply` 로 실제 쓰기 (`scripts/promote_model.py` 관례).

────────────────────────────────────────────────────────────────────────────
## 왜 이게 기하적으로 성립하나 (프레임 쪽과 다른 점)

`stage_atlas` 도크스트링의 라이브 실측: **text↔image cos 중앙 0.147** vs
**text↔text 0.631** vs image↔image 0.756. 문장과 이미지를 한 공간에서 최근접 질의하면
그건 "엔티티 타입 분류기"가 된다 — 그래서 promptmap 은 문장 UMAP 을 문장 벡터만으로 만든다.

캡션은 **텍스트**다. 캡션↔문장은 text↔text 대역(0.631 근방)이라 최근접 질의가 의미를 갖는다.
이 모듈이 프레임 쪽 `top_prompt_*` 와 같은 계산을 캡션에 대해 하면서도 "모달리티 혼합"
경고에 걸리지 않는 이유가 이것이다. 실행 후 r1 코사인 중앙값을 위 두 상수와 **같은 줄에**
찍어서(아래 `_cos_band`) 벡터 소스를 잘못 골랐을 때 바로 드러나게 한다.

────────────────────────────────────────────────────────────────────────────
## 벡터 정본: 데이터셋 `caption_embedding` (pgvector 아님)

같은 캡션의 벡터가 두 곳에 있고 **내용이 다르다**:

  · pgvector `image_embeddings(entity_type='caption')` 11,978행 — **한국어 원문** 기준.
    `reembed_captions_en.py` 는 이 테이블을 **읽기만** 하고(그 스크립트 L252, 보존용
    `caption_embedding_ko` 의 소스) 갱신하지 않는다. 즉 DB 쪽은 재임베딩 이전 상태다.
  · FiftyOne `frames.caption_embedding` — `reembed_captions_en.py` 가 **영어 번역문**으로
    다시 만든 벡터 (그 스크립트 L295). `embedding` 필드도 같이 영어로 덮었다(L310).

한국어 캡션 벡터는 **붕괴돼 있다**: effective rank 1.5/1024, 상위 1방향이 분산 94.6%,
무관한 캡션끼리 pairwise cos 0.951 (`enrich_frames_captions.py` 도크스트링 실측).
판별격차는 한국어 +0.0073(≈무작위) vs 영어 +0.0837(11.5배). 뱅크 문장은 영어다.
→ 한국어 벡터로 최근접 문장을 뽑으면 **전 캡션이 같은 문장 한두 개로 붙는다.**

따라서 정본은 **데이터셋 필드**다. `--vector-source pgvector` 는 탈출구로만 남기고,
그 경로를 타면 경고 + `vector_source` 를 산출물에 박는다. `auto`(기본)는
`caption_embedding` 이 있으면 그걸 쓰고, 없을 때만 pgvector 로 내려간다.
`caption_en` 필드 존재 여부로 "영어 기준인가"를 자기보고한다 (재임베딩의 지문).

────────────────────────────────────────────────────────────────────────────
## 역방향 오염 방지 (프레임 불가침)

`fiftyone_pgvector.attach_labels` 는 프레임 전용 필드가 캡션에 새지 않도록
`modality == 'caption' or not image_id` 면 skip 한다. 이 모듈은 **그 계약의 반대편**이라
가드도 반대로 건다 — 다만 여집합이 아니라 **더 좁게(AND)** 잡는다:

    쓰기 대상 = (modality == 'caption')  AND  (image_id 없음)

여집합(OR)로 잡으면 "image_id 가 빠진 프레임"에 캡션 필드가 붙는다. 프레임의 image_id
커버리지는 실측 100% 라 지금은 같은 집합이지만, 스키마가 흔들렸을 때 **틀리는 쪽이
프레임 오염**인 설계는 쓰지 않는다. 애매한 문서(둘 중 하나만 만족)는 쓰지 않고 **센다**
(`ambiguous` 카운터) — 조용히 넘기면 다음 사람이 커버리지를 성공으로 읽는다.

`enrich-prompts` 는 `frames` 를 **읽기만** 한다 (쓰기 대상은 `frames-prompts`).

────────────────────────────────────────────────────────────────────────────
## GT 없음 (tier 표기)

이 모듈의 어떤 숫자도 GT 를 읽지 않는다 — 최근접 문장은 코사인만으로 정해진다.
그래도 산출물 최상단에 `gt_tier="no_gt"` / `gt_free=True` 를 박는다 (prompt_geometry 의
site·attrs 스테이지와 같은 관례). 옆 스테이지 산출물과 나란히 놓였을 때 "정확도 0%" 로
오독되는 사고를 막는 표식이다.

## env

    CPL_DATASET            기본 `frames`
    CPL_PROMPTS_DATASET    기본 `<CPL_DATASET>-prompts`
    BANK_ATTACH            link 대상 뱅크. 기본 `BANK_A`(=v1.0.8.0) — prompt_geometry 관례
    BANK_A / BANK_B / BANK_LIST   gidx 전역 오프셋 순서를 정한다 (아래 BANKS 주석)
    BANK_PROMPT_DIR        뱅크 npz 디렉토리. 기본 /data/fiftyone/sourceh/prompts
    BANK_TEXT_SOURCE       `db`(기본)/`npz` — 문장 정본 (repair_bank_prompts.load_bank)
    DATAOPS_POSTGRES_DSN   pgvector 폴백 + 뱅크 문장 DB
    CPL_SET_VALUES_BATCH   기본 2000
    CPL_MEM_BUDGET_GB      기본 4.0 (재-UMAP 진입 전 가드)

정본: docker/analysis/caption_prompt_link.py
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import numpy as np

# 형제 모듈(`repair_bank_prompts`)을 같은 디렉토리에서 찾는다 — prompt_geometry 와 같은 관례.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DATASET = os.environ.get("CPL_DATASET", "frames")
PROMPT_DIR = os.environ.get("BANK_PROMPT_DIR", "/data/fiftyone/sourceh/prompts")

# ⚠️ 아래 3개는 prompt_geometry 와 **같은 값**이어야 한다. import 대신 복제하는 이유는
# repair_bank_prompts.py 가 GIDX_OFFSET 을 복제한 것과 같다 — 348KB 모듈을 끌어오면
# 이 CLI 가 env(BANK_LIST 등) 파싱과 numpy 상수를 통째로 상속하고, 합성 harness 도
# 그 전부를 흉내내야 한다. 값이 바뀌면 세 파일을 같이 고친다.
GIDX_OFFSET = 100_000                     # prompt_geometry.GIDX_OFFSET 와 동일 값
CLASS_NAMES = {0: "normal", 1: "falldown", 2: "fire", 3: "smoke", 4: "smoking"}

# 뱅크 순서 = gidx 전역 오프셋 순서. prompt_geometry 의 VERSIONS/BANKS 유도를 **문자 그대로**
# 복제한다 — 여기서 순서가 어긋나면 `cap_prompt_gidx_r1` 이 `-prompts` 의 gidx 와 다른
# 문장을 가리킨다 (조용히 틀리는 종류의 버그).
if os.environ.get("BANK_LIST"):
    VERSIONS = tuple(v.strip() for v in os.environ["BANK_LIST"].split(",") if v.strip())
else:
    VERSIONS = (os.environ.get("BANK_A", "v1.0.8.0"), os.environ.get("BANK_B", "v1.0.8.4"))
BANKS = tuple(dict.fromkeys(VERSIONS))
DEFAULT_BANK = os.environ.get("BANK_ATTACH", VERSIONS[0])

SET_VALUES_BATCH = int(os.environ.get("CPL_SET_VALUES_BATCH", "2000"))
VEC_CHUNK = int(os.environ.get("CPL_VEC_CHUNK", "2000"))      # ListField 스트리밍 청크
COS_CHUNK = int(os.environ.get("CPL_COS_CHUNK", "2048"))      # 유사도 행렬 청크 (문장 축)
MEM_BUDGET_GB = float(os.environ.get("CPL_MEM_BUDGET_GB", "4.0"))
# 이 행수를 넘으면 UMAP 전에 PCA 64-d 로 사전축소한다. 기본값은 promptmap 과 같은 100,000 —
# 1024-d 코사인 UMAP 이 60만 행에서 RLIMIT 16GB MemoryError 로 죽은 실측(2026-08-12)에서 나온 값.
# 호스트 여유가 다르면 낮춰 잡을 수 있다 (낮출수록 안전하고, 대신 좌표가 근사가 된다).
PCA_MIN_ROWS = int(os.environ.get("CPL_PCA_MIN_ROWS", "100000"))

# `stage_atlas` 도크스트링의 라이브 실측 — 판독 기준선. 상수로 박아 로그에 같이 찍는다.
COS_TEXT_TEXT = 0.631
COS_TEXT_IMAGE = 0.147

RANKS = (1, 2, 3)
# 캡션 전용임이 이름에서 드러나야 한다(`cap_` 접두). 계열 자체는 프레임의
# `top_prompt_r{2,3}_<vt>` / `winner_gidx_r{2,3}_<tag>` 와 같은 "순위 사다리" 관례를 따른다.
# ⚠️ 다만 **버전 태그를 이름에 박지 않는다** — stage_attach 의 버전중립 6필드(`cos_best_*`
#    /`runner_up`/`attached_bank`)와 같은 판단이다: 54버전 환경에서 태그를 박으면 스키마가
#    뱅크 수만큼 늘어난다. 대신 어느 뱅크 산출인지를 `cap_prompt_bank` 로 남긴다
#    (= `attached_bank` 와 같은 자리·같은 이유). 다른 뱅크로 재실행하면 **덮어써진다.**
F_BANK = "cap_prompt_bank"
F_CLS_R1 = "cap_prompt_cls_r1"
F_GIDX_R1 = "cap_prompt_gidx_r1"


def f_prompt(rank: int) -> str:
    """r위 문장 원문 필드 (StringField)."""
    return f"cap_prompt_r{rank}"


def f_cos(rank: int) -> str:
    """r위 코사인 필드 (FloatField)."""
    return f"cap_prompt_cos_r{rank}"


LINK_FIELDS = tuple(
    [f_prompt(r) for r in RANKS] + [f_cos(r) for r in RANKS] + [F_CLS_R1, F_GIDX_R1, F_BANK]
)

T0 = time.time()


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')} +{time.time() - T0:5.0f}s] {msg}", flush=True)


# ────────────────────── 자원 (공유 호스트) ──────────────────────
def mem_avail_gb() -> float:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / 1024 / 1024
    except OSError:                                   # /proc 없는 환경(합성 harness)
        pass
    return float("inf")


def peak_rss_gb() -> float:
    """이 프로세스의 피크 RSS. 재-UMAP 은 60만 행이라 **끝나고 나서 얼마 썼는지**를 남긴다."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024 / 1024
    except OSError:
        pass
    return float("nan")


def assert_mem_budget(budget_gb: float) -> None:
    """prompt_geometry.assert_mem_budget 와 같은 계약 — 부족하면 **시작 자체를 거부**한다.
    2026-07 스왑 쓰래싱 사건(load 165, SSH 끊김) 재발 방지."""
    avail = mem_avail_gb()
    if avail < 2 * budget_gb:
        raise SystemExit(f"메모리 부족: available {avail:.1f}G < 2×budget {budget_gb:.0f}G — 시작 거부")


def cap_blas_threads() -> None:
    for v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
              "NUMEXPR_NUM_THREADS", "NUMBA_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
        os.environ.setdefault(v, str(max(1, (os.cpu_count() or 4) // 4)))


# ────────────────────── 공용 헬퍼 ──────────────────────
def load_bank(version: str) -> dict:
    """뱅크 1개 → `{vec, cls, prompt}`. **벡터는 npz, 문장은 DB 정본.**

    판정 규칙과 폴백은 형제 모듈 `repair_bank_prompts.load_bank` 한 곳에만 있다 —
    소비자(prompt_geometry / frames_eval / prompt_eval / 여기)가 같은 규칙을 쓰도록.
    """
    import repair_bank_prompts as _bank                # 같은 디렉토리 형제 모듈

    b = _bank.load_bank(version, PROMPT_DIR)
    # prompt_geometry.load_all 과 동일 가드 — 이 파일은 GIDX_OFFSET 을 복제했으므로 검사도 복제.
    if len(b["cls"]) > GIDX_OFFSET:
        raise SystemExit(f"뱅크 {version} 문장 {len(b['cls']):,} > GIDX_OFFSET {GIDX_OFFSET:,} — "
                         "gidx 블록 충돌(조용한 버전 오귀속). GIDX_OFFSET 증설+재백필 필요")
    return b


def l2norm(a: np.ndarray, what: str) -> np.ndarray:
    """cosine = 내적이 되도록 정규화. **이미 정규화돼 있으면 무연산**이지만, 그렇지 않은
    입력이 조용히 섞이면 코사인이 아니라 스케일 비교가 되므로 편차를 로그로 남긴다."""
    a = np.asarray(a, dtype=np.float32)
    n = np.linalg.norm(a, axis=1, keepdims=True)
    bad = float(np.max(np.abs(n - 1.0))) if len(n) else 0.0
    if bad > 1e-3:
        log(f"⚠️ {what}: 저장 벡터가 정규화 상태가 아니다 (|‖v‖−1| 최대 {bad:.4f}) — 여기서 정규화한다")
    n[n == 0] = 1.0
    return (a / n).astype(np.float32)


def set_values_batched(ds, field: str, pairs: list, make, batch: int | None = None) -> None:
    """`{sample_id: 값}` 을 통째로 만들지 않고 배치로 나눠 쓴다 (prompt_geometry 동명 함수와
    같은 계약). `pairs` = [(sample_id, 행인덱스), ...], `make(행인덱스)` = 그 자리의 값.

    캡션은 11,978 뿐이라 한 번에 써도 되지만, 배치 2,000 은 이 데이터셋의 **다른 쓰기와
    같은 단위**여서 동시에 도는 스테이지와 메모리 피크가 겹치지 않는다.
    """
    b = batch or SET_VALUES_BATCH
    for s in range(0, len(pairs), b):
        ds.set_values(field, {sid: make(i) for sid, i in pairs[s:s + b]}, key_field="id")


def is_caption_row(modality, image_id) -> bool:
    """**쓰기 대상 판정 — 캡션만.** `fiftyone_pgvector.attach_labels` 가드의 대칭형.

    저쪽: `modality == 'caption' or not image_id` → skip (프레임 전용 필드를 캡션에 안 씀).
    여기: 그 여집합(OR)이 아니라 **AND** 로 더 좁게 잡는다 — 여집합을 쓰면 "image_id 가
    빠진 프레임"이 캡션 취급을 받는다. 틀렸을 때 프레임이 오염되는 쪽을 기본값으로 두지 않는다.
    둘 중 하나만 만족하는 문서는 쓰지도 세지도 않고 `ambiguous` 로 따로 센다.
    """
    return str(modality or "") == "caption" and not str(image_id or "")


def partition_modality(ids: list, mods: list, image_ids: list) -> tuple[list, list, list]:
    """(캡션 id, 프레임 id, 애매한 id). 세 번째가 0 이 아니면 스키마가 흔들린 것이다."""
    cap, frame, ambi = [], [], []
    for sid, m, iid in zip(ids, mods, image_ids):
        is_cap_decl = str(m or "") == "caption"
        has_iid = bool(str(iid or ""))
        if is_cap_decl and not has_iid:
            cap.append(sid)
        elif not is_cap_decl and has_iid:
            frame.append(sid)
        else:
            ambi.append(sid)
    return cap, frame, ambi


def topk_over_banks(C: np.ndarray, bank_rows: list, k: int = 3) -> tuple[np.ndarray, np.ndarray]:
    """캡션 [n, d] × 여러 뱅크 → 전역 top-k (코사인 내림차순).

    `bank_rows` = [(goff, P[m, d]), ...]. 반환 `(cos[n, k], gidx[n, k])` — gidx 는
    `goff + 뱅크-로컬 인덱스` 로 이미 전역화돼 있다 (`-prompts` 의 gidx 와 등식 조인 가능).

    뱅크를 **한 벌씩** 훑으며 러닝 top-k 를 갱신한다. 전 뱅크(54벌 60만 문장)를 한 행렬로
    쌓으면 2.5GB 가 상주하는데, 이 호스트의 병목은 RAM 이다 (promptmap 의 E_parts 주석과
    같은 이유). 뱅크 1벌이면 최대 16,125 문장 = 66MB.

    ⚠️ 동점 타이브레이크는 **작은 gidx 우선**으로 못 박는다. `np.argsort` 는 unstable
       quicksort 라 fp32 동점(실제 발생)에서 순서가 실행마다 흔들린다 — `stage_attach` 가
       2위 클래스를 argmax 마스킹으로 구한 것과 같은 계열의 방어다.
    """
    n = len(C)
    best_c = np.full((n, k), -np.inf, dtype=np.float32)
    best_g = np.full((n, k), -1, dtype=np.int64)
    for goff, P in bank_rows:
        P = np.asarray(P, dtype=np.float32)
        if not len(P):
            continue
        for s in range(0, len(P), COS_CHUNK):
            block = P[s:s + COS_CHUNK]
            S = C @ block.T                                    # [n, chunk]
            kk = min(k, S.shape[1])
            part = np.argpartition(-S, kk - 1, axis=1)[:, :kk]  # 후보만 추려 정렬비용 절감
            cand_c = np.take_along_axis(S, part, axis=1)
            cand_g = (part + goff + s).astype(np.int64)
            best_c, best_g = _merge_topk(best_c, best_g, cand_c, cand_g, k)
            del S
    return best_c, best_g


def _merge_topk(ac: np.ndarray, ag: np.ndarray, bc: np.ndarray, bg: np.ndarray,
                k: int) -> tuple[np.ndarray, np.ndarray]:
    """두 후보 집합을 합쳐 상위 k. 정렬 키 = (−cos, gidx) — 결정론적."""
    cc = np.concatenate([ac, bc], axis=1)
    gg = np.concatenate([ag, bg], axis=1)
    # lexsort 는 **마지막 키가 1순위**다. gidx 오름차순을 먼저 깔고 cos 내림차순을 얹으면
    # "cos 같으면 작은 gidx" 가 된다. (안정성에 기대지 않고 키로 못 박는 형태)
    order = np.lexsort((gg, -cc), axis=1)[:, :k]
    return (np.take_along_axis(cc, order, axis=1).astype(np.float32),
            np.take_along_axis(gg, order, axis=1).astype(np.int64))


def _cos_band(med: float) -> str:
    """관측 중앙값을 실측 기준선 옆에 세워 벡터 소스 오선택을 즉시 드러낸다."""
    if med >= 0.45:
        return f"text↔text 대역 (기준 {COS_TEXT_TEXT})"
    if med <= 0.25:
        return (f"⚠️ text↔image 대역 (기준 {COS_TEXT_IMAGE}) — 캡션 벡터가 **이미지**이거나 "
                "언어가 어긋났을 가능성. --vector-source / caption_en 유무 확인")
    return f"중간대 — 기준 text↔text {COS_TEXT_TEXT} / text↔image {COS_TEXT_IMAGE} 사이"


def _dwidth(s: str) -> int:
    """터미널 표시폭. 한글은 2칸이라 `len()` 으로 맞추면 표가 어긋난다."""
    import unicodedata

    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in s)


def plan_table(title: str, rows: list) -> None:
    """dry-run 계획 표 (promote_model.py 의 dry-run 출력과 같은 성격)."""
    w = max((_dwidth(str(a)) for a, _ in rows), default=0)
    print(f"\n── {title} " + "─" * 40)
    for a, b in rows:
        print(f"  {a}{' ' * (w - _dwidth(str(a)))}  {b}")
    print()


# ────────────────────── 캡션 벡터 로딩 ──────────────────────
def _stream_field(ds, ids: list, field: str, chunk: int = VEC_CHUNK) -> list:
    """`ds.values(<ListField>)` 를 통째로 걸지 않는다 — 1024-d ListField 는 원소마다 파이썬
    float 객체가 나서 수만 건이면 GB 단위로 뜬다 (prompt_geometry._stream_frames_embeddings
    주석과 같은 함정). 청크 2,000 이면 파이썬 측 상주가 ~65MB."""
    out = []
    for s in range(0, len(ids), chunk):
        out.extend(ds.select(ids[s:s + chunk], ordered=True).values(field))
    return out


def load_caption_vectors(ds, cap_ids: list, source: str) -> tuple[list, np.ndarray, dict]:
    """캡션 벡터 → (기여한 id, [n, d] fp32, meta). 정본 판단 근거는 모듈 도크스트링 참조.

    `source`:
      `auto`     — `caption_embedding` 이 스키마에 있으면 그것(영어 재임베딩본), 없으면 pgvector
      `dataset`  — `caption_embedding` 강제 (없으면 죽는다)
      `pgvector` — image_embeddings(entity_type='caption') 강제. **한국어 벡터**라 경고를 낸다
    """
    schema = ds.get_field_schema()
    has_field = "caption_embedding" in schema
    lang = "en(재임베딩됨)" if "caption_en" in schema else "ko?(caption_en 필드 없음 — 미재임베딩 의심)"

    use_ds = has_field if source == "auto" else (source == "dataset")
    if source == "dataset" and not has_field:
        raise SystemExit("--vector-source dataset 인데 `caption_embedding` 필드가 없다 — "
                         "`enrich_frames_captions.py` / `reembed_captions_en.py` 를 먼저 돌려라")
    if use_ds:
        vals = _stream_field(ds, cap_ids, "caption_embedding")
        kept, vecs = [], []
        for sid, v in zip(cap_ids, vals):
            if v is None or not len(v):
                continue
            kept.append(sid)
            vecs.append(v)
        meta = {"vector_source": "dataset.caption_embedding", "text_lang_basis": lang,
                "n_null": len(cap_ids) - len(kept)}
        if not kept:
            raise SystemExit("caption_embedding 이 전건 비어 있다 — 재임베딩을 먼저 돌려라")
        log(f"캡션 벡터: dataset.caption_embedding {len(kept):,}/{len(cap_ids):,} (언어기준 {lang})")
        return kept, l2norm(np.asarray(vecs, dtype=np.float32), "caption_embedding"), meta

    # ── pgvector 폴백 (탈출구) ──
    log("⚠️ pgvector(image_embeddings, entity_type='caption') 경로 — 이 벡터는 **한국어 원문** "
        "기준이다 (reembed_captions_en.py 는 이 테이블을 갱신하지 않는다). effective rank "
        "1.5/1024·무관 캡션 pairwise cos 0.951 — 최근접 문장이 사실상 상수가 된다. "
        "값을 순위/임계 판정에 쓰지 말 것.")
    import fiftyone_pgvector as fp                            # 형제 모듈 (DSN·파싱 재사용)

    rows = fp._load_caption_embeddings()
    by_entity = {str(r["entity_id"]): r["embedding"] for r in rows if r.get("embedding")}
    ent = _stream_field(ds, cap_ids, "entity_id", chunk=20000)
    kept, vecs, miss = [], [], 0
    for sid, eid in zip(cap_ids, ent):
        v = by_entity.get(str(eid or ""))
        if v is None:
            miss += 1
            continue
        kept.append(sid)
        vecs.append(v)
    if not kept:
        raise SystemExit("pgvector 캡션 임베딩이 데이터셋 entity_id 와 하나도 안 붙는다 — 조인 확인")
    log(f"캡션 벡터: pgvector {len(kept):,}/{len(cap_ids):,} (미매칭 {miss:,})")
    return kept, l2norm(np.asarray(vecs, dtype=np.float32), "pgvector caption"), {
        "vector_source": "pgvector.image_embeddings(caption)", "text_lang_basis": "ko",
        "n_null": miss, "warning": "korean_collapsed_vectors"}


# ────────────────────── stage: link ──────────────────────
def stage_link(args) -> None:
    """캡션마다 최근접 뱅크 문장 top1~3 → `frames` 의 **캡션 문서에만** 기록.

    붙는 필드 (전부 `cap_` 접두 — 프레임의 `top_prompt_*` 계열과 이름공간이 안 겹친다):
      · `cap_prompt_r{1,2,3}`      그 순위 문장 **원문** (StringField)
      · `cap_prompt_cos_r{1,2,3}`  그 순위 코사인 (FloatField)
      · `cap_prompt_cls_r1`        1위 문장의 **선언 클래스** (Classification — Color by 대상)
      · `cap_prompt_gidx_r1`       1위 문장의 전역 gidx (IntField)
      · `cap_prompt_bank`          어느 뱅크 산출인가 (Classification)

    ⚠️ `cap_prompt_cls_r1` 은 **문장이 스스로 선언한 클래스**지 캡션의 예측 라벨이 아니다.
       캡션에는 GT 가 없고(`gt_tier=no_gt`) 이 모듈은 정확도를 계산하지 않는다.
    ⚠️ `cap_prompt_gidx_r1` 은 사람이 쓰는 필터가 아니라 `-prompts` 데이터셋과의 **조인 키**다
       (`stage_attach` 의 `winner_gidx_*` 와 같은 자리·같은 이유: 이 App 은 Query Performance
       모드라 String 필드가 자유텍스트 substring 검색으로만 렌더돼 문장 특정이 불가능하다).
       그래서 active_fields 에는 넣지 않는다 — 그리드 칩이 썸네일을 덮는다.
    """
    import fiftyone as fo

    ds = fo.load_dataset(args.dataset)
    schema = ds.get_field_schema()
    if "modality" not in schema:
        raise SystemExit(f"{args.dataset}: `modality` 필드가 없다 — 혼합 모달리티 데이터셋이 아니다. "
                         "merge_frames_captions.py 산출물인지 확인하라")
    ids = ds.values("id")
    mods = ds.values("modality")
    image_ids = ds.values("image_id") if "image_id" in schema else [None] * len(ids)
    cap_ids, frame_ids, ambi = partition_modality(ids, mods, image_ids)
    log(f"{args.dataset}: 전체 {len(ids):,} = 캡션 {len(cap_ids):,} + 프레임 {len(frame_ids):,}"
        + (f" + **애매 {len(ambi):,}**" if ambi else ""))
    if ambi:
        log(f"⚠️ 애매한 문서 {len(ambi):,}건 (modality 와 image_id 유무가 어긋남) — "
            "**쓰지 않는다**. 예: " + ", ".join(str(a) for a in ambi[:3]))
    if not cap_ids:
        raise SystemExit("캡션 문서가 0건 — modality=='caption' AND image_id 부재 조건을 확인하라")

    version = args.bank
    path = f"{PROMPT_DIR}/{version}.npz"
    if not os.path.exists(path):
        raise SystemExit(f"뱅크 npz 없음: {path}")
    bank = load_bank(version)                              # 문장은 DB 정본 (load_bank 주석)
    P = l2norm(bank["vec"], f"bank {version}")
    if len(bank["prompt"]) != len(P):
        # npz 에 `prompt` 가 아예 없거나(external_only 뱅크) 행수가 어긋난 상태.
        # 문장 원문 없이 코사인만 쓰는 건 이 기능의 목적(캡션 옆에 문장을 보여주기)을 잃는다.
        raise SystemExit(f"{version}: 문장 {len(bank['prompt']):,} ≠ 벡터 {len(P):,} — "
                         "`repair_bank_prompts.py --audit` 로 문장 소스를 먼저 확인하라")
    goff = (BANKS.index(version) if version in BANKS else 0) * GIDX_OFFSET
    if version not in BANKS:
        log(f"⚠️ {version} 이 BANKS{list(BANKS)} 에 없다 — gidx 오프셋 0 을 쓴다. "
            "`-prompts` 와 조인하려면 BANK_A/BANK_B/BANK_LIST 를 그때와 같게 두고 재실행하라")

    kept_ids, C, vmeta = load_caption_vectors(ds, cap_ids, args.vector_source)
    if C.shape[1] != P.shape[1]:
        raise SystemExit(f"차원 불일치: 캡션 {C.shape[1]}-d vs 뱅크 {P.shape[1]}-d — "
                         "같은 인코더(PE-Core-L14-336, 1024-d)가 아니다")

    log(f"link {version}: 문장 {len(P):,} × 캡션 {len(C):,} — top-3 계산 (청크 {COS_CHUNK})")
    cos, gidx = topk_over_banks(C, [(goff, P)], k=3)
    local = (gidx - goff).astype(np.int64)
    med = float(np.median(cos[:, 0]))
    log(f"link {version}: r1 코사인 중앙 {med:.3f} · p10 {float(np.percentile(cos[:, 0], 10)):.3f} "
        f"· p90 {float(np.percentile(cos[:, 0], 90)):.3f} → {_cos_band(med)}")
    n_uniq = int(len(set(gidx[:, 0].tolist())))
    log(f"link {version}: 1위로 쓰인 고유 문장 {n_uniq:,}개 ({n_uniq / max(1, len(P)):.2%}) "
        f"— 1에 가까울수록 벡터 붕괴 신호")

    made = {}
    for r in RANKS:
        made[f_prompt(r)] = f"문장 원문 (r{r})"
        made[f_cos(r)] = f"코사인 (r{r})"
    made[F_CLS_R1] = "1위 문장의 선언 클래스"
    made[F_GIDX_R1] = f"1위 gidx (오프셋 {goff:,})"
    made[F_BANK] = version

    if not args.apply:
        plan_table("DRY-RUN 계획 — link (아무것도 쓰지 않았다)", [
            ("데이터셋", args.dataset),
            ("뱅크", f"{version} (문장 {len(P):,} / gidx 오프셋 {goff:,})"),
            ("벡터 소스", f"{vmeta['vector_source']} · 언어기준 {vmeta.get('text_lang_basis')}"),
            ("쓰기 대상", f"캡션 {len(kept_ids):,} 문서 (modality=='caption' AND image_id 부재)"),
            ("불가침", f"프레임 {len(frame_ids):,} · 애매 {len(ambi):,} — 0 필드 기록"),
            ("생성 필드", ", ".join(LINK_FIELDS)),
            ("r1 코사인", f"중앙 {med:.3f} — {_cos_band(med)}"),
            ("배치", f"set_values {SET_VALUES_BATCH} 건씩"),
            ("gt_tier", "no_gt (GT-free — 정확도 계산 없음)"),
            ("실행", "`--apply` 를 붙이면 위 계획대로 쓴다"),
        ])
        return

    # clear-then-set — 이전 런의 stale 값이 남으면 가장 악질적인 거짓말이 된다 (stage_attach 관례).
    # ⚠️ clear_sample_field 는 **전 문서**를 지운다. 이 필드군은 애초에 캡션 전용이라
    #    프레임에는 값이 없고, 지우는 행위 자체가 프레임 문서에 필드를 만들지 않는다.
    for fld in LINK_FIELDS:
        if fld in ds.get_field_schema():
            ds.clear_sample_field(fld)

    pairs = [(sid, i) for i, sid in enumerate(kept_ids)]
    for j, r in enumerate(RANKS):
        set_values_batched(ds, f_prompt(r), pairs, lambda i, j=j: bank["prompt"][int(local[i, j])])
        set_values_batched(ds, f_cos(r), pairs, lambda i, j=j: float(cos[i, j]))
    set_values_batched(ds, F_CLS_R1, pairs,
                       lambda i: fo.Classification(
                           label=CLASS_NAMES.get(int(bank["cls"][int(local[i, 0])]),
                                                 f"class_{int(bank['cls'][int(local[i, 0])])}")))
    set_values_batched(ds, F_GIDX_R1, pairs, lambda i: int(gidx[i, 0]))
    set_values_batched(ds, F_BANK, pairs, lambda i: fo.Classification(label=version))

    ds.info = {**(ds.info or {}), "caption_prompt_link": {
        "bank": version, "gidx_offset": goff, "n_captions": len(kept_ids),
        "n_frames_untouched": len(frame_ids), "n_ambiguous": len(ambi),
        "cos_r1_median": round(med, 4), "unique_top1": n_uniq,
        "gt_tier": "no_gt", "gt_free": True, **vmeta}}
    ds.save()
    log(f"link {version}: 필드 {len(LINK_FIELDS)}개 × 캡션 {len(kept_ids):,} 기록 완료 "
        f"(프레임 {len(frame_ids):,} 불가침) · 피크 RSS {peak_rss_gb():.2f}G")


# ────────────────────── stage: enrich-prompts ──────────────────────
def _resolve_filepaths(ds, cap_ids: list, mode: str) -> tuple[dict, dict]:
    """캡션 id → `-prompts` 표본으로 쓸 filepath. (매핑, 소스별 카운트).

    우선순위 (`auto`):
      1. `asset`   — `frames` 안에서 **같은 asset_id 를 가진 프레임 표본**의 filepath.
                     과제가 지정한 1순위다.
      2. `caption` — 캡션 표본 자신의 filepath. 이것도 결국 **그 영상의 대표 프레임**이다
                     (`fiftyone_pgvector._fetch_asset_keyframe` 이 asset 당 1장을 골라
                     내려받은 것을 `build_caption_fiftyone_dataset` 이 심볼릭링크로 걸고,
                     `backfill_caption_keyframes.py` 가 남은 것을 실제 키프레임으로 채웠다).
      3. skip + 카운트.

    ⚠️ 1번만 쓰면 대부분 스킵된다: 프레임 추출 대상과 Gemini 캡션 대상이 거의 안 겹쳐
       **asset 교집합이 481**뿐이다 (프레임 187,994 중 캡션 보유 264 = 0.1%,
       `merge_frames_captions.py` 도크스트링 실측). 그래서 기본은 `auto` 다.
       `--filepath-source asset` 로 1번만 강제할 수 있고, 그때의 스킵 수를 그대로 보고한다.
    """
    schema = ds.get_field_schema()
    out: dict = {}
    cnt = {"asset": 0, "caption": 0, "skipped": 0}
    if "asset_id" not in schema:
        log("⚠️ `asset_id` 필드가 없다 — asset 조인 경로를 건너뛴다")
        mode = "caption" if mode in ("auto", "asset") else mode

    cap_assets = _stream_field(ds, cap_ids, "asset_id", chunk=20000) if "asset_id" in schema \
        else [None] * len(cap_ids)
    cap_fps = _stream_field(ds, cap_ids, "filepath", chunk=20000)
    cap_hk = _stream_field(ds, cap_ids, "has_keyframe", chunk=20000) \
        if "has_keyframe" in schema else [None] * len(cap_ids)

    asset2fp: dict = {}
    if mode in ("auto", "asset") and "asset_id" in schema:
        # 프레임 표본에서 asset → filepath 1장. image_id 보유 = 프레임 (is_caption_row 의 반대편).
        aids, fps, iids = ds.values("asset_id"), ds.values("filepath"), \
            (ds.values("image_id") if "image_id" in schema else [None] * ds.count())
        for a, fpth, iid in zip(aids, fps, iids):
            if a and fpth and str(iid or "") and str(a) not in asset2fp:
                asset2fp[str(a)] = fpth
        log(f"asset→프레임 filepath 사전 {len(asset2fp):,}건")

    for sid, a, fpth, hk in zip(cap_ids, cap_assets, cap_fps, cap_hk):
        got = asset2fp.get(str(a or "")) if mode in ("auto", "asset") else None
        if got:
            out[sid] = got
            cnt["asset"] += 1
            continue
        if mode in ("auto", "caption") and fpth and (hk is None or bool(hk)):
            out[sid] = fpth
            cnt["caption"] += 1
            continue
        cnt["skipped"] += 1
    return out, cnt


def _prompt_row_vectors(ids: list, gidxs: list, versions: list):
    """`-prompts` 문장행 → (id 리스트, 정규화 벡터) 를 **뱅크 단위로** 흘리는 제너레이터.

    ⚠️ `-prompts` 는 `embedding` 을 **일부러 저장하지 않는다** — 29버전 60만 행에서 문서
       부피의 94% 였고 WiredTiger 캐시를 부풀려 mongod 딥스톨을 냈다 (promptmap 주석,
       2026-08-11 실측 2회). 벡터 정본은 `PROMPT_DIR/<ver>.npz`, 조회 키는
       `gidx % GIDX_OFFSET`. 이 함수가 그 계약의 유일한 독자다.

    제너레이터인 이유: 60만 행 전체(2.5GB)를 동시에 들지 않기 위해서다. 대신 두 번 순회하는
    호출부(IncrementalPCA fit → transform)는 npz 를 두 번 읽는다 — RAM 을 디스크로 바꾼
    의도된 교환이다 (호스트 병목은 RAM, npz 는 로컬 SSD).
    """
    by_ver: dict = {}
    for sid, g, v in zip(ids, gidxs, versions):
        if g is None or not v:
            continue
        by_ver.setdefault(str(v), []).append((sid, int(g)))
    for ver, items in by_ver.items():
        try:
            vec = np.load(f"{PROMPT_DIR}/{ver}.npz", allow_pickle=True)["vec"].astype(np.float32)
        except (OSError, KeyError) as exc:
            log(f"⚠️ 뱅크 npz 로드 실패 {ver}: {exc} — 그 버전 문장 {len(items):,}행은 좌표에서 빠진다")
            continue
        loc = np.array([g % GIDX_OFFSET for _, g in items], dtype=np.int64)
        bad = int((loc >= len(vec)).sum())
        if bad:
            log(f"⚠️ {ver}: gidx 지역 인덱스가 npz 행수({len(vec):,})를 넘는 행 {bad:,}건 — 제외")
        ok = loc < len(vec)
        yield ([sid for (sid, _), m in zip(items, ok) if m],
               l2norm(vec[loc[ok]], f"bank {ver}"))


def _knn_place(cap_vecs: np.ndarray, gidx2pt: dict, bank_rows: list, k: int) -> np.ndarray:
    """기존 `emb_viz` 좌표를 **재계산하지 않고** 캡션만 근사 배치 (out-of-sample 확장).

    캡션의 최근접 문장 k개를 1024-d 원공간에서 찾고, 그 문장들의 **기존 2-d 좌표**를
    코사인 가중 평균한다 (landmark/Nyström 계열 보간).

    ⚠️ 해석 한계 — 이 배치는 이웃 문장들의 **볼록포 안**에만 떨어진다. "캡션이 문장 매니폴드
       바깥에 산다" 는 신호는 원리적으로 보이지 않는다. modality gap 을 보고 싶으면
       `--umap full` 이어야 한다. 싼 대신 결론의 종류가 제한된다.
    """
    cos, gid = topk_over_banks(cap_vecs, bank_rows, k=k)
    pts = np.zeros((len(cap_vecs), 2), dtype=np.float32)
    lost = 0
    for i in range(len(cap_vecs)):
        nb = [(gidx2pt[int(g)], float(c)) for g, c in zip(gid[i], cos[i]) if int(g) in gidx2pt]
        if not nb:
            lost += 1
            continue
        base = min(c for _, c in nb)
        w = np.array([max(c - base, 0.0) + 1e-6 for _, c in nb], dtype=np.float32)
        pts[i] = (np.array([p for p, _ in nb], dtype=np.float32) * w[:, None]).sum(0) / w.sum()
    if lost:
        log(f"⚠️ knn 배치: 이웃 좌표를 하나도 못 찾은 캡션 {lost:,}건 → 원점 (0,0)")
    return pts


def stage_enrich_prompts(args) -> None:
    """`frames-prompts` 에 캡션 11,978 을 **별 모달리티 표본**으로 편입.

    `-prompts` 는 "문장 1개 = 표본 1개" 인 데이터셋이다 (`stage_promptmap` 산출).
    거기에 캡션을 넣으면 같은 화면에서 "이 뱅크 문장들이 실제 현장 캡션과 같은 동네에
    있는가"를 볼 수 있다 — 뱅크 큐레이션의 직접 근거다.

    쓰는 필드:
      · `text`          캡션 원문 (기존 문장행과 **같은 필드** — 그래야 한 축에서 읽힌다)
      · `entity`        Classification. 캡션=`caption`, 기존 문장행=`prompt` **백필**
                        → 구분축이 없으면 화면에서 두 종류를 못 가른다
      · `caption_id`    출처 `frames` 표본 id (역추적)
      · `asset_id`      출처 영상
      · `bank_version`  **세팅하지 않는다** — 캡션은 어느 뱅크 소속도 아니다 (None)
      · `gidx`          **세팅하지 않는다** — gidx 는 뱅크 문장의 전역 유일 id 이고
                        `@user/prompt-compare` 패널이 등식 조인 키로 쓴다. 캡션에 값을 주면
                        그 조인이 캡션을 맞힌다. 캡션의 정체성은 `caption_id` 가 갖는다

    좌표 (`--umap`):
      · `full`(기본) 문장+캡션 전체 재-UMAP → **새 brain key `emb_viz_cap`**.
        기존 `emb_viz` 는 **건드리지 않는다** (Embeddings 패널이 마지막 키를 기억하는
        App 함정 때문에 워크스페이스에 `brainResult` 를 못 박아 둘 다 열 수 있게 한다).
      · `knn`  기존 emb_viz 좌표 위에 캡션만 근사 배치 (싸다, 해석 한계는 `_knn_place` 참조)
      · `skip` 좌표 없이 행만 넣는다

    멱등: 재실행하면 `entity.label == 'caption'` 행을 **먼저 지우고** 다시 넣는다.
    """
    import fiftyone as fo

    ds = fo.load_dataset(args.dataset)
    tgt = args.target or os.environ.get("CPL_PROMPTS_DATASET") or f"{args.dataset}-prompts"
    if not fo.dataset_exists(tgt):
        raise SystemExit(f"{tgt} 없음 — `prompt_geometry.py promptmap --profile frames` 를 먼저 돌려라")
    tds = fo.load_dataset(tgt)
    tsch = tds.get_field_schema()

    # ── 인벤토리 ──
    t_ids = tds.values("id")
    t_ent = tds.values("entity.label") if "entity" in tsch else [None] * len(t_ids)
    old_cap = [sid for sid, e in zip(t_ids, t_ent) if e == "caption"]
    need_prompt_backfill = [sid for sid, e in zip(t_ids, t_ent) if e != "caption" and not e]
    log(f"{tgt}: 표본 {len(t_ids):,} (기존 caption 행 {len(old_cap):,} / entity 미설정 "
        f"{len(need_prompt_backfill):,})")

    # ── 캡션 수집 ──
    sch = ds.get_field_schema()
    ids = ds.values("id")
    mods = ds.values("modality") if "modality" in sch else [None] * len(ids)
    image_ids = ds.values("image_id") if "image_id" in sch else [None] * len(ids)
    cap_ids, frame_ids, ambi = partition_modality(ids, mods, image_ids)
    log(f"{args.dataset}: 캡션 {len(cap_ids):,} / 프레임 {len(frame_ids):,}"
        + (f" / 애매 {len(ambi):,}" if ambi else ""))
    if not cap_ids:
        raise SystemExit("캡션 문서가 0건")

    fp_map, fp_cnt = _resolve_filepaths(ds, cap_ids, args.filepath_source)
    use_ids = [s for s in cap_ids if s in fp_map]
    if "caption" not in sch:
        raise SystemExit(f"{args.dataset}: `caption` 필드가 없다 — 캡션 원문을 옮길 수 없다")
    # 표시는 한국어 원문(`caption`)이다. `caption_en`(번역문)은 임베딩 기준일 뿐 표시 정본이
    # 아니다 — reembed_captions_en.py 도크스트링의 "표시는 한국어 그대로" 결정을 따른다.
    texts = dict(zip(cap_ids, _stream_field(ds, cap_ids, "caption", chunk=20000)))
    assets = dict(zip(cap_ids, _stream_field(ds, cap_ids, "asset_id", chunk=20000))) \
        if "asset_id" in sch else {}
    log(f"filepath 해결: asset 조인 {fp_cnt['asset']:,} / 캡션 자체 키프레임 {fp_cnt['caption']:,} "
        f"/ **스킵 {fp_cnt['skipped']:,}** → 편입 {len(use_ids):,}")

    if not args.apply:
        plan_table("DRY-RUN 계획 — enrich-prompts (아무것도 쓰지 않았다)", [
            ("소스", f"{args.dataset} 캡션 {len(cap_ids):,}"),
            ("대상", f"{tgt} (현재 {len(t_ids):,} 표본)"),
            ("삭제", f"기존 entity=='caption' 행 {len(old_cap):,} (멱등성)"),
            ("백필", f"entity='prompt' 를 {len(need_prompt_backfill):,} 문장행에"),
            ("삽입", f"caption 행 {len(use_ids):,} (filepath: asset {fp_cnt['asset']:,} / "
                     f"자체 {fp_cnt['caption']:,} / 스킵 {fp_cnt['skipped']:,})"),
            ("좌표", {"full": f"전체 재-UMAP → brain_key `emb_viz_cap` "
                              f"(≈{len(t_ids) - len(old_cap) + len(use_ids):,}행, PCA64 경유)",
                      "knn": "기존 emb_viz 위에 캡션만 근사 배치 → `emb_viz_cap`",
                      "skip": "좌표 없음"}[args.umap]),
            ("보존", "기존 `emb_viz` 는 덮지 않는다"),
            ("미설정", "bank_version / gidx (캡션은 뱅크 소속이 아니다)"),
            ("gt_tier", "no_gt (GT-free)"),
            ("실행", "`--apply` 를 붙이면 위 계획대로 쓴다"),
        ])
        return

    # ⚠️ 메모리 가드는 **삽입 전**에 건다. 삽입 후에 죽으면 좌표 없는 캡션 행만 남은
    #    반쯤 갱신된 데이터셋이 된다 (재실행하면 지우고 다시 넣지만, 그 사이 화면이 거짓말한다).
    if args.umap == "full":
        assert_mem_budget(MEM_BUDGET_GB)

    # ── 1. 멱등: 기존 caption 행 삭제 ──
    if old_cap:
        tds.delete_samples(old_cap)
        log(f"기존 caption 행 {len(old_cap):,} 삭제 (재삽입 전)")

    # ── 2. entity='prompt' 백필 (남은 행 = 전부 문장) ──
    live = tds.values("id")
    live_ent = tds.values("entity.label") if "entity" in tds.get_field_schema() else [None] * len(live)
    todo = [sid for sid, e in zip(live, live_ent) if not e]
    if todo:
        set_values_batched(tds, "entity", [(s, 0) for s in todo],
                           lambda _i: fo.Classification(label="prompt"))
        log(f"entity='prompt' 백필 {len(todo):,}행")

    # ── 3. caption 행 삽입 ──
    batch, inserted = [], []
    for sid in use_ids:
        s = fo.Sample(filepath=fp_map[sid])
        s["text"] = texts.get(sid) or ""
        s["entity"] = fo.Classification(label="caption")
        s["caption_id"] = str(sid)
        if assets.get(sid):
            s["asset_id"] = str(assets[sid])
        batch.append(s)
        if len(batch) >= SET_VALUES_BATCH:
            inserted.extend(map(str, tds.add_samples(batch)))
            batch = []
    if batch:
        inserted.extend(map(str, tds.add_samples(batch)))
    log(f"caption 행 {len(inserted):,} 삽입 → {tgt} 총 {tds.count():,}")

    # ── 4. 좌표 ──
    if args.umap != "skip":
        _build_cap_viz(tds, ds, inserted, use_ids, args)

    # ── 5. 워크스페이스 — brainResult 를 못 박아 emb_viz 와 골라 열 수 있게 ──
    if args.umap != "skip":
        _save_ws(tds, "caption", "emb_viz_cap", "entity.label")
    tds.info = {**(tds.info or {}), "caption_enrich": {
        "source_dataset": args.dataset, "n_captions": len(inserted),
        "filepath_source": args.filepath_source, "filepath_counts": fp_cnt,
        "umap": args.umap, "brain_key": "emb_viz_cap" if args.umap != "skip" else None,
        "gt_tier": "no_gt", "gt_free": True}}
    tds.save()
    log(f"enrich-prompts 완료 · 워크스페이스 {tds.list_workspaces()} · 피크 RSS {peak_rss_gb():.2f}G")


def _save_ws(tds, name: str, bkey: str, color: str) -> None:
    """Samples ↔ Embeddings 분할. `brainResult` 를 못 박는 이유: 패널이 **마지막 키를
    기억해서** 데이터셋에 brain key 가 여럿이면 엉뚱한 투영이 열린다 (Color by 까지 죽는
    App 함정). `promptviz.workspace` 와 같은 계약."""
    import fiftyone as fo

    space = fo.Space(children=[
        fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
        fo.Space(children=[fo.Panel(type="Embeddings",
                                    state={"brainResult": bkey, "colorByField": color})]),
    ], orientation="horizontal")
    if name in tds.list_workspaces():
        tds.delete_workspace(name)
    tds.save_workspace(name, space, description=f"{bkey} (색: {color})")


def _build_cap_viz(tds, ds, cap_sample_ids: list, cap_src_ids: list, args) -> None:
    """`emb_viz_cap` 등록. 기존 `emb_viz` 는 읽기만 하고 **덮지 않는다**."""
    import fiftyone.brain as fob

    # 재실행 멱등 — 기존 emb_viz_cap 을 먼저 지운다 (promptviz.register 관례). 없으면
    # compute_visualization 이 중복 brain key 로 죽는다. 합성 harness 의 가짜 fiftyone 은
    # 키 유일성을 강제하지 않아 이 구멍을 못 잡았다 (2026-08-19 직독 리뷰에서 발견).
    if tds.has_brain_run("emb_viz_cap"):
        tds.delete_brain_run("emb_viz_cap")

    cap_kept, C, _vmeta = load_caption_vectors(ds, cap_src_ids, args.vector_source)
    pos = {s: i for i, s in enumerate(cap_kept)}
    # 삽입 순서 = use_ids 순서. 벡터가 없어 빠진 캡션은 좌표에서도 빠진다.
    pairs = [(new_id, pos[src]) for new_id, src in zip(cap_sample_ids, cap_src_ids) if src in pos]
    if not pairs:
        log("⚠️ 캡션 벡터가 하나도 없어 좌표를 만들지 않는다")
        return
    Ccap = C[[p for _, p in pairs]]

    tsch = tds.get_field_schema()
    p_ids = tds.values("id")
    p_ent = tds.values("entity.label") if "entity" in tsch else [None] * len(p_ids)
    p_gidx = tds.values("gidx") if "gidx" in tsch else [None] * len(p_ids)
    p_ver = tds.values("bank_version.label") if "bank_version" in tsch else [None] * len(p_ids)
    s_ids, s_gidx, s_ver = [], [], []
    for sid, e, g, v in zip(p_ids, p_ent, p_gidx, p_ver):
        if e == "caption":
            continue
        s_ids.append(sid)
        s_gidx.append(g)
        s_ver.append(v)
    log(f"emb_viz_cap: 문장 {len(s_ids):,} + 캡션 {len(Ccap):,}")
    if not any(g is not None for g in s_gidx) or not any(s_ver):
        raise SystemExit("문장행에 gidx/bank_version 이 없다 — 벡터를 npz 에서 되찾을 수 없다. "
                         "`prompt_geometry.py promptmap` 산출물인지 확인하라")

    if args.umap == "knn":
        try:
            prev = tds.load_brain_results(args.base_brain)
        except Exception as exc:  # noqa: BLE001 — 키 이름/버전에 따라 예외형이 다르다
            raise SystemExit(f"기존 brain key `{args.base_brain}` 를 못 읽는다 ({exc}) — "
                             "`--umap full` 로 처음부터 만들어라") from exc
        pv_ids = [str(i) for i in (getattr(prev, "sample_ids", None) or [])]
        pts_prev = np.asarray(getattr(prev, "points", []), dtype=np.float32)
        if len(pv_ids) != len(pts_prev):
            raise SystemExit(f"{args.base_brain}: sample_ids {len(pv_ids)} ≠ points {len(pts_prev)} "
                             "— 좌표-표본 대응이 깨졌다. --umap full 로 다시 만들어라")
        id2pt = dict(zip(pv_ids, pts_prev))
        gidx2pt = {int(g): id2pt[sid] for sid, g in zip(s_ids, s_gidx)
                   if g is not None and sid in id2pt}
        if not gidx2pt:
            raise SystemExit(f"`{args.base_brain}` 좌표와 문장행 gidx 가 하나도 안 붙는다 — "
                             "--umap full 로 다시 만들어라")
        # gidx→좌표 사전을 쓰므로 이웃 후보도 **전역 gidx** 로 나와야 한다 → 버전별 npz 를
        # 데이터에 박힌 오프셋과 함께 묶는다 (_bank_rows_for).
        cap_pts = _knn_place(Ccap, gidx2pt, _bank_rows_for(s_gidx, s_ver), k=args.knn)
        points = {sid: id2pt[sid] for sid in s_ids if sid in id2pt}     # 기존 좌표 그대로 이관
        points.update({pairs[i][0]: cap_pts[i] for i in range(len(pairs))})
        fob.compute_visualization(tds, points=points, brain_key="emb_viz_cap")
        log(f"emb_viz_cap 등록 (knn 근사 k={args.knn}, 문장 {len(points) - len(pairs):,} 이관 "
            f"+ 캡션 {len(pairs):,} 보간) — 기존 {args.base_brain} 보존 "
            f"· 피크 RSS {peak_rss_gb():.2f}G")
        return

    # ── full: PCA64 → UMAP. promptmap 의 60만 행 경로를 그대로 재사용 ──
    cap_blas_threads()
    import umap as _umap

    n_total = len(s_ids) + len(Ccap)
    use_pca = n_total > PCA_MIN_ROWS
    order_ids: list = []
    parts: list = []
    if use_pca:
        from sklearn.decomposition import IncrementalPCA   # 큰 경로에서만 필요한 의존
        # 1024-d 코사인 UMAP 은 60만 행에서 메모리가 폭발한다 (RLIMIT 16GB MemoryError 실측,
        # 2026-08-12) → PCA 64-d 사전축소. **IncrementalPCA** 를 쓰는 이유는 전 행렬(60만×1024
        # = 2.5GB)을 상주시키지 않기 위해서다 — 뱅크 npz 를 한 벌씩만 연다.
        # ⚠️ PCA 는 평균 중심화를 하므로 축소 공간의 cosine 은 원 벡터의 cosine 과 수학적으로
        #    동일하지 않다 (promptmap 의 codex 3B 주석과 같은 유보) — 배치 용도의 근사로 수용.
        ipca, n_fit = IncrementalPCA(n_components=64), 0
        for _sids, V in _prompt_row_vectors(s_ids, s_gidx, s_ver):
            for s in range(0, len(V), 20000):
                if len(V[s:s + 20000]) >= 64:
                    ipca.partial_fit(V[s:s + 20000])
                    n_fit += 1
        for s in range(0, len(Ccap), 20000):
            if len(Ccap[s:s + 20000]) >= 64:
                ipca.partial_fit(Ccap[s:s + 20000])
                n_fit += 1
        if not n_fit:
            raise SystemExit("IncrementalPCA 에 넣을 블록(≥64행)이 하나도 없다 — 데이터 확인")
        for sids, V in _prompt_row_vectors(s_ids, s_gidx, s_ver):   # npz 2회 읽기(도크스트링)
            order_ids.extend(sids)
            parts.append(ipca.transform(V).astype(np.float32))
        order_ids.extend(nid for nid, _ in pairs)
        parts.append(ipca.transform(Ccap).astype(np.float32))
        init = "random"      # spectral 은 큰 connected component 에서 dense n×n 을 만들다 죽는다
        log(f"emb_viz_cap: IncrementalPCA 사전축소 → 64-d ({n_fit} 블록 fit)")
    else:
        for sids, V in _prompt_row_vectors(s_ids, s_gidx, s_ver):
            order_ids.extend(sids)
            parts.append(V)
        order_ids.extend(nid for nid, _ in pairs)
        parts.append(Ccap)
        init = "spectral"
    E = np.concatenate(parts, axis=0)
    del parts

    log(f"emb_viz_cap: UMAP fit {E.shape} (init={init}) · MemAvailable {mem_avail_gb():.1f}G")
    pts = _umap.UMAP(n_components=2, metric="cosine", low_memory=True,
                     init=init, random_state=42).fit_transform(E)
    assert len(order_ids) == len(pts), f"좌표 {len(pts)} ≠ 표본 {len(order_ids)} — 대응 붕괴"
    # ID-keyed dict — raw ndarray 는 개수만 검증되고 순서가 `ds.values('id')` 와 암묵 결합된다
    # (promptmap 의 codex 3A). 여기선 순서가 뱅크별로 재편되므로 특히 위험하다.
    fob.compute_visualization(tds, points={i: p for i, p in zip(order_ids, pts)},
                              brain_key="emb_viz_cap")
    log(f"emb_viz_cap 등록 (전체 재-UMAP {len(pts):,}행) — 기존 emb_viz 보존 "
        f"· 피크 RSS {peak_rss_gb():.2f}G")


def _bank_rows_for(gidxs: list, versions: list) -> list:
    """knn 용 `[(goff, P), ...]` — goff 는 그 버전 행들의 실제 gidx 상위자리에서 되읽는다.
    (BANKS env 가 promptmap 때와 달라져도 데이터에 박힌 오프셋을 따르게 하는 안전장치)"""
    by_ver: dict = {}
    for g, v in zip(gidxs, versions):
        if g is None or not v:
            continue
        by_ver.setdefault(str(v), set()).add(int(g) // GIDX_OFFSET)
    rows = []
    for ver, offs in by_ver.items():
        if len(offs) != 1:
            log(f"⚠️ {ver}: gidx 오프셋이 여러 개다 {sorted(offs)} — 첫 값을 쓴다")
        goff = sorted(offs)[0] * GIDX_OFFSET
        try:
            vec = np.load(f"{PROMPT_DIR}/{ver}.npz", allow_pickle=True)["vec"].astype(np.float32)
        except (OSError, KeyError) as exc:
            log(f"⚠️ 뱅크 npz 로드 실패 {ver}: {exc} — knn 이웃 후보에서 제외")
            continue
        rows.append((goff, l2norm(vec, f"bank {ver}")))
    return rows


# ────────────────────── selftest (파일·DB·FiftyOne 없이) ──────────────────────
def stage_selftest() -> None:
    """순수 함수 불변식. 실데이터 harness 는 scratchpad 의 `caption_link_harness.py` 가 담당."""
    # 1) 모달리티 가드 진리표 — 프레임 불가침의 핵심
    assert is_caption_row("caption", None) is True
    assert is_caption_row("caption", "") is True
    assert is_caption_row("frame", "img-1") is False
    assert is_caption_row("caption", "img-1") is False, "캡션 선언이어도 image_id 가 있으면 안 쓴다"
    assert is_caption_row(None, None) is False, "modality 미설정은 캡션으로 보지 않는다"
    assert is_caption_row("frame", None) is False
    cap, frm, ambi = partition_modality(
        ["a", "b", "c", "d"], ["caption", "frame", "caption", None], [None, "i1", "i2", None])
    assert (cap, frm, ambi) == (["a"], ["b"], ["c", "d"]), (cap, frm, ambi)

    # 2) top-k 정확성 — 순진 계산과 원소단위 일치 (단일 뱅크 / 다중 뱅크 둘 다)
    rng = np.random.default_rng(7)
    C = rng.normal(size=(23, 16)).astype(np.float32)
    C /= np.linalg.norm(C, axis=1, keepdims=True)
    P1 = rng.normal(size=(31, 16)).astype(np.float32)
    P1 /= np.linalg.norm(P1, axis=1, keepdims=True)
    P2 = rng.normal(size=(17, 16)).astype(np.float32)
    P2 /= np.linalg.norm(P2, axis=1, keepdims=True)
    cos, gid = topk_over_banks(C, [(0, P1)], k=3)
    S = C @ P1.T
    naive = np.argsort(-S, axis=1, kind="stable")[:, :3]
    assert np.allclose(cos, np.take_along_axis(S, naive, axis=1), atol=1e-6), "단일 뱅크 cos 불일치"
    assert (gid == naive).all(), "단일 뱅크 gidx 불일치"
    cos2, gid2 = topk_over_banks(C, [(0, P1), (GIDX_OFFSET, P2)], k=3)
    Sall = np.concatenate([C @ P1.T, C @ P2.T], axis=1)
    gall = np.concatenate([np.arange(31), GIDX_OFFSET + np.arange(17)])
    nv = np.argsort(-Sall, axis=1, kind="stable")[:, :3]
    assert np.allclose(cos2, np.take_along_axis(Sall, nv, axis=1), atol=1e-6), "다중 뱅크 cos 불일치"
    assert (gid2 == gall[nv]).all(), "다중 뱅크 gidx 불일치 (오프셋 붕괴)"
    # 코사인이 실제로 그 문장을 가리키는가 (인덱스↔값 대응)
    for i in range(len(C)):
        for j in range(3):
            g = int(gid2[i, j])
            v = P1[g] if g < GIDX_OFFSET else P2[g - GIDX_OFFSET]
            assert abs(float(C[i] @ v) - float(cos2[i, j])) < 1e-6, "gidx 가 그 cos 를 안 가리킴"

    # 3) 동점 타이브레이크 = 작은 gidx (unstable quicksort 에 안 흔들린다)
    D = np.zeros((1, 4), dtype=np.float32)
    D[0, 0] = 1.0
    Pt = np.tile(D, (5, 1))                       # 5문장 전부 동일 → cos 전부 동점
    _c, g = topk_over_banks(D, [(500, Pt)], k=3)
    assert g[0].tolist() == [500, 501, 502], g[0].tolist()

    # 4) gidx 전역화 round-trip
    for vi in range(3):
        for loc in (0, 1, 12_479):
            g = vi * GIDX_OFFSET + loc
            assert g // GIDX_OFFSET == vi and g % GIDX_OFFSET == loc

    # 5) 필드명 관례 — cap_ 접두 + 순위 계열
    assert f_prompt(1) == "cap_prompt_r1" and f_cos(3) == "cap_prompt_cos_r3"
    assert all(f.startswith("cap_") for f in LINK_FIELDS), "캡션 전용임이 이름에 드러나야 한다"
    assert len(set(LINK_FIELDS)) == len(LINK_FIELDS) == 9

    # 6) 코사인 대역 판독 — 벡터 소스 오선택 탐지기
    assert "text↔text" in _cos_band(0.63) and "⚠️" not in _cos_band(0.63)
    assert "⚠️" in _cos_band(0.147), "text↔image 대역은 경고여야 한다"

    # 7) knn 배치 = 이웃 좌표의 볼록결합 (한 이웃만 압도적이면 그 점에 수렴)
    pt = {0: np.array([0.0, 0.0], np.float32), 1: np.array([10.0, 0.0], np.float32),
          2: np.array([0.0, 10.0], np.float32)}
    Pk = np.eye(3, dtype=np.float32)[:, :3]
    q = np.array([[1.0, 0.0, 0.0]], dtype=np.float32)
    out = _knn_place(q, pt, [(0, Pk)], k=3)
    assert np.allclose(out[0], [0.0, 0.0], atol=1e-3), out[0]
    q2 = np.array([[0.0, 1.0, 0.0]], dtype=np.float32)
    assert np.allclose(_knn_place(q2, pt, [(0, Pk)], k=3)[0], [10.0, 0.0], atol=1e-3)

    # 8) l2norm 은 이미 정규화된 입력에 무연산
    V = rng.normal(size=(9, 8)).astype(np.float32)
    V /= np.linalg.norm(V, axis=1, keepdims=True)
    assert np.allclose(l2norm(V, "t"), V, atol=1e-6)
    assert np.allclose(np.linalg.norm(l2norm(rng.normal(size=(9, 8)) * 7, "t"), axis=1), 1.0,
                       atol=1e-5)

    log("selftest OK")


# ────────────────────── CLI ──────────────────────
def main() -> int:
    ap = argparse.ArgumentParser(
        description="캡션 ↔ 프롬프트 뱅크 양방향 연동 (기본 dry-run, --apply 로 실행)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("link", help="캡션에 최근접 프롬프트 top1~3 필드를 붙인다")
    p1.add_argument("--dataset", default=DATASET)
    p1.add_argument("--bank", default=DEFAULT_BANK, help=f"기본 BANK_ATTACH (={DEFAULT_BANK})")
    p1.add_argument("--vector-source", choices=["auto", "dataset", "pgvector"], default="auto",
                    help="캡션 벡터 정본. auto=dataset.caption_embedding(영어 재임베딩본) 우선")
    p1.add_argument("--apply", action="store_true")

    p2 = sub.add_parser("enrich-prompts", help="frames-prompts 에 캡션을 별 모달리티로 편입")
    p2.add_argument("--dataset", default=DATASET, help="캡션 소스 (혼합 모달리티 데이터셋)")
    p2.add_argument("--target", default=None, help="기본 <dataset>-prompts")
    p2.add_argument("--vector-source", choices=["auto", "dataset", "pgvector"], default="auto")
    p2.add_argument("--filepath-source", choices=["auto", "asset", "caption"], default="auto",
                    help="asset=frames 프레임 조인만 / caption=캡션 자체 키프레임 / auto=1→2")
    p2.add_argument("--umap", choices=["full", "knn", "skip"], default="full",
                    help="full=전체 재-UMAP(정공법) / knn=기존 emb_viz 위 근사 배치 / skip")
    p2.add_argument("--base-brain", default="emb_viz", help="knn 모드가 읽을 기존 brain key")
    p2.add_argument("--knn", type=int, default=15, help="knn 모드 이웃 수")
    p2.add_argument("--apply", action="store_true")

    sub.add_parser("selftest", help="파일·DB·FiftyOne 없이 도는 불변식 검사")

    args = ap.parse_args()
    if args.cmd == "selftest":
        stage_selftest()
        return 0
    if args.cmd == "link":
        stage_link(args)
    else:
        stage_enrich_prompts(args)
    if not args.apply:
        log("dry-run — 아무것도 쓰지 않았다. 실행하려면 `--apply`")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
