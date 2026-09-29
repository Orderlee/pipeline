"""user-prompt-compare — 문장(<세션 데이터셋>-prompts) ↔ 프레임(세션 데이터셋) 크로스 데이터셋
비교 Panel.

모드 A(스펙 §5.2): 판정규칙(argmax_k1|dist_iou)별 문장 산점도 + 프레임↔문장 양방향 선택 연동.
Task 12부터 프롬프트 데이터셋은 세션 데이터셋 이름에서 "<name>-prompts"로 자동 유도된다
(sourcei 세션 → sourcei-prompts, source-h 세션 → source-h-prompts) — 짝이 없으면 안내만 뜨고
크래시하지 않는다. 뱅크 버전이 여럿이면 "전체"/버전별 선택기로 산점도를 필터링한다.

문장 텍스트는 **Postgres 019 스키마(`bank_sentences`)가 정본**이다 (2026-08-19) — 데이터셋의
`text` 필드는 npz 를 `gidx % GIDX_OFFSET` 로 퍼온 파생물이라 43.3%가 자리표시자다. 아래
"prompt DB" 블록 참고. DB 를 못 읽거나 게이트에서 거부되면 데이터셋 필드로 폴백하고 그
사실을 배너에 싣는다.

정본: docker/analysis/plugins/user-prompt-compare/ (git)
배포: docker cp → /data/fiftyone/datasets/__plugins__/user-prompt-compare/
      + 플러그인 **디렉토리 touch** (plugins_cache dir_state 무효화)
"""

import hashlib
import json
import os
import threading
from collections import OrderedDict
import time

import fiftyone as fo
import fiftyone.operators as foo
import fiftyone.operators.types as types

# Task 12 이전 하드코딩 값 — 지금은 (a) selftest 오프라인 픽스처, (b) ctx.dataset 이 없을 때의
# fallback 기본값 두 용도로만 남는다. 런타임 조인은 _prompts_dataset_name(ctx)/
# _current_winner_field(ctx)가 세션 데이터셋에서 유도한 값을 쓴다 (아래 정의부 주석 참고).
PROMPTS_DATASET = "sourcei-prompts"
BRAIN_KEY = "emb_viz"          # 하드코딩 — App이 다른 키에서 죽는 실측 함정

FRAMES_DATASET = "sourcei"
VTAG = "v1080"   # 2026-08-11 전 파트 태그로 통일 (구 v080 — vtag 주석 참고)
WINNER_FIELD = f"winner_gidx_{VTAG}"
# 그리는 점 상한. 2026-08-19 20,000 → 700,000 (사용자: "전체 이미지 및 프롬프트가 나와야
# 분석이 가능"). 603,318 전량을 통과시킨다. 형제 패널 image_embeddings 와 같은 판단이다 —
# 속도 이득은 점 수 축소가 아니라 payload 축소에서 나온다(전량 실측 20초).
MAX_POINTS = 700_000
# 호버 텍스트를 실을 최대 점 수. **여기가 전량 표시의 열쇠였다** — 실측 분해:
#   drawn 603,318 → fig 107.58MB 중 text 77.21MB(72%) · ids 6.45MB · 나머지 좌표
#   호버 내용물이 문장 원문(≤80자)이라 원리적으로 안 줄어든다.
# 호버를 끄면 30.37MB, 좌표 반올림까지 하면 그 절반이다. 잃는 건 툴팁뿐이고 상세는
# **선택 → 문장 표**(_rows_to_markdown)가 이미 담당한다 — 즉 LOD 의 상세 단계가
# 새로 만들 것 없이 이미 있다. (FiftyOne PlotlyView 는 줌 이벤트를 노출하지 않아
# 줌 기반 LOD 는 불가능하다 — on_click/on_selected 둘뿐. 그래서 선택을 트리거로 쓴다.)
HOVER_BUDGET = 20_000
# 벡터 전용(문장 미보유) 뱅크 버전의 자리표시자 접두사 — `prompt_geometry.PLACEHOLDER_PREFIX`
# 와 **같은 문자열**. import 하지 않는 이유: 이 플러그인은 App 프로세스에서 돌고 /workspace 가
# sys.path 에 없다. 두 곳에 사는 상수이므로 한쪽을 바꾸면 다른 쪽도 바꿔야 한다.
PLACEHOLDER_PREFIX = "(텍스트 없음"
#  ⚠️ 상한을 64MB → 192MB 로 올림 (2026-08-12). 64MB 는 28,605행 시절 예산이었고,
#     29버전 리빌드로 603,318행이 되며 실측 ≈50.6MB 로 한계에 붙었다 — 필드가 몇 개만
#     늘거나 버전이 추가되면 `AssertionError: 캐시 예산 64MB 초과` 로 패널이 죽는다.
#     엔트리는 여전히 1개만 유지하므로(_CACHE.clear()) 상주 메모리는 이 상한이 곧 전부다.
CACHE_CAP_BYTES = 192 * 2**20

_CACHE = {}  # (dataset_name, brain_key, last_modified_at) -> bundle. 엔트리 1개 유지.

META_FIELDS = ["gidx", "text", "category", "adopted", "wins", "purity",
               "n_cameras", "wave_gain", "wave_role", "bank_version",
               # 규칙 준수 — `prompt_rule_fields.py` 가 `prompt_standard` 정본으로 채운다.
               # 없으면 색칠이 단색으로 떨어지므로(부재는 조용하다) 범례에 사유를 적는다.
               "form", "rule_ok"]

# Task 12 — 뱅크 버전 선택기 + 프롬프트 데이터셋 자동 유도.
ALL_VERSIONS_LABEL = "전체"
# 컨트롤 그리드 열 수. App 번들 getGridSx 실측: orientation=horizontal 은 display:grid 인데
# columns 가 없으면 gridAutoFlow="column" 이라 **줄바꿈이 없다** — 컨트롤이 5개를 넘으면
# 뒤쪽(뱅크 버전·선택 해제)이 패널 폭 밖으로 밀려 조작 불가였다(2026-08-12 실측).
# columns=N 이면 gridTemplateColumns=repeat(N,1fr) 로 행이 자동으로 접힌다.
CONTROLS_COLUMNS = 3
NO_PROMPTS_PAIR_TEXT = "이 데이터셋에는 프롬프트 짝이 없습니다"

# 표시 드롭다운 값 (2026-08-10 피드백: 토글 버튼 전부 드롭다운으로 통일)
SHOW_ALL_LABEL = "전체 (미채택 포함)"
SHOW_ADOPTED_LABEL = "채택만"


# ══════════════════════════════════════════════════════════════════════════════
# prompt DB (Postgres 019 스키마) — 문장·벡터 **정본** 해석
# ══════════════════════════════════════════════════════════════════════════════
# ⚠️ **사본 동기화 블록** — 이 섹션(`PDB_*` / `pdb_*` / `_pdb_*`)은
#      plugins/user-prompt-compare · user-image-embeddings · user-embeddings
#    세 곳에 **글자 단위로 같은 사본**으로 들어 있다. 플러그인 디렉토리가 각각 독립
#    배포 단위(`docker cp <디렉토리>`)라 공유 import 경로가 없다 — CLASS_COLORS·
#    PLACEHOLDER_PREFIX 복제와 같은 관례다. **한 곳을 고치면 나머지 둘도 같이 고칠 것.**
#
# 왜 DB 인가 (2026-08-19 사용자 요청: "DB 연결 해야해 이제 gidx 그걸로 하지마"):
#   `<X>-prompts` 데이터셋의 `text` 필드는 npz(`PROMPT_DIR/<ver>.npz`)의 `prompt`
#   배열을 `gidx % GIDX_OFFSET` 행으로 퍼온 **파생물**이다. 2026-08-11 재빌드가 27버전의
#   문장을 자리표시자로 덮어써서 sourcei-prompts 603,318행 중 **261,244행(43.3%)** 이
#   `(텍스트 없음 #N)` 이다(2026-08-19 실측). 정본은 Postgres 019 스키마다:
#
#     prompt_banks(bank_id, version_tag, sentence_storage, …)
#       ⨝ bank_sentences(bank_id, gidx, text, class_label, content_hash)   [UNIQUE(bank_id,gidx)]
#       ⨝ image_embeddings(entity_type='prompt', entity_id=content_hash)   → 1024-d 벡터
#
#   조인 키 = (샘플 `bank_version.label` 정규화, 샘플 `gidx % PDB_GIDX_OFFSET`)
#           → (prompt_banks.version_tag 정규화, bank_sentences.gidx)
#   실측 커버리지: prompt 벡터 121,614행 = bank_sentences 고유 content_hash 121,614개
#   (고아 0) — 문장이 DB 에 있으면 벡터도 반드시 있다.
#
# fail-closed 게이트 (repair_bank_prompts.check_candidate 와 같은 규약 — 조용히 틀린
# 문장을 넣느니 폴백한다):
#   ① 그 버전이 DB 에 있고 문장을 보유해야 한다 (`external_only` = 문장 없음이 사실)
#   ② DB 문장 수 == 그 버전의 **데이터셋 행 수**(= npz 행 수). 다르면 gidx 정렬을
#      신뢰할 수 없다 — 실측 v1.0.2.0 은 DB 12,568 vs 데이터셋 14,600 이라 gidx 로
#      읽으면 **다른 문장**이 나온다(표본 10개 중 9개 불일치).
#   ③ 가져온 행의 `class_label` == 샘플의 `category.label`
#   하나라도 어긋나면 그 **버전 전체**가 폴백이고, 어느 소스를 썼는지 `pdb_note()` 가
#   배너에 싣는다 — **조용한 폴백 금지**(2026-08-19 요구사항).

PDB_DSN_ENV = ("BANK_DB_DSN", "DATAOPS_POSTGRES_DSN", "POSTGRES_DSN", "DATABASE_URL")
PDB_MODEL = os.environ.get("BANK_EMBED_MODEL", "facebook/PE-Core-L14-336")
PDB_GIDX_OFFSET = 100_000        # prompt_geometry.GIDX_OFFSET 와 같은 값 (복제 상수)
PDB_MEMO_CAP = 300_000           # (버전, 로컬 gidx) → 문장 메모 상한. 넘으면 통째로 비운다
PDB_SRC_DB = "DB 정본(bank_sentences)"
PDB_SRC_FALLBACK = "데이터셋 text 필드(npz 파생)"

_PDB_BANKS = None                # norm_ver -> (bank_id, version_tag, storage, n_sent)
_PDB_BANKS_ERR = None            # 마지막 뱅크 조회 실패 사유 — 배너에 그대로 싣는다
_PDB_TEXT = {}                   # (norm_ver, local_gidx) -> (text, class_label)
_PDB_CONN = None
_PDB_LOCK = threading.Lock()


def pdb_enabled():
    """`PROMPT_DB=off` 로 DB 경로를 끌 수 있다 (DB 장애 시 탈출구 — 폴백은 데이터셋 필드)."""
    return os.environ.get("PROMPT_DB", "on").strip().lower() not in ("0", "off", "false", "no")


def pdb_norm_ver(v):
    """`V1.0.10.3` / `v1.0.10.3` / `1.0.10.3` → `1.0.10.3`.

    `prompt_banks.version_tag` 는 대소문자·`v` 접두가 흔들린다 (실측 52행에 `V1.0.10.3`
    과 `1.0.13.0` 이 함께 있다) — 정규화 없이 등식 조인하면 조용히 0건이 된다.
    """
    return str(v if v is not None else "").strip().lstrip("vV")


def pdb_local_gidx(g):
    """FiftyOne 전역 gidx → 뱅크-로컬 행 번호 (= `bank_sentences.gidx`)."""
    return None if g is None else int(g) % PDB_GIDX_OFFSET


def _pdb_dsn():
    return next((os.environ[k] for k in PDB_DSN_ENV if os.environ.get(k)), None)


def _pdb_query(sql, params):
    """커넥션 1개를 프로세스 수명 동안 재사용. 끊겼으면 **한 번만** 재연결 후 재시도.

    App 은 유휴 시간이 길어 커넥션이 서버측에서 끊기는 일이 잦다 — 요청마다 새로
    연결하면 버전당 왕복이 붙고(전체 필터 = 최대 29버전), 안 하면 첫 조회가 죽는다.
    """
    global _PDB_CONN
    import psycopg2

    with _PDB_LOCK:
        for last in (False, True):
            try:
                if _PDB_CONN is None or _PDB_CONN.closed:
                    dsn = _pdb_dsn()
                    if not dsn:
                        raise RuntimeError("DSN 미설정 (" + "/".join(PDB_DSN_ENV) + ")")
                    _PDB_CONN = psycopg2.connect(dsn, connect_timeout=5)
                    _PDB_CONN.autocommit = True     # 읽기 전용 — 트랜잭션을 열어두지 않는다
                with _PDB_CONN.cursor() as cur:
                    cur.execute(sql, params)
                    return cur.fetchall()
            except psycopg2.Error:
                try:
                    if _PDB_CONN is not None:
                        _PDB_CONN.close()
                except Exception:       # noqa: BLE001 — 이미 끊긴 커넥션
                    pass
                _PDB_CONN = None
                if last:
                    raise
    return []


def pdb_banks(refresh=False):
    """norm_ver -> (bank_id, version_tag, sentence_storage, n_sent). 52행 — 1회만 읽는다.

    실패는 예외가 아니라 **빈 dict + 사유 기록**이다 (패널이 죽으면 안 된다).
    """
    global _PDB_BANKS, _PDB_BANKS_ERR
    if _PDB_BANKS is not None and not refresh:
        return _PDB_BANKS
    if not pdb_enabled():
        _PDB_BANKS, _PDB_BANKS_ERR = {}, "PROMPT_DB=off (수동 비활성)"
        return _PDB_BANKS
    try:
        rows = _pdb_query(
            "SELECT b.bank_id, b.version_tag, b.sentence_storage, count(s.sentence_id) "
            "  FROM prompt_banks b LEFT JOIN bank_sentences s USING (bank_id) "
            " GROUP BY 1, 2, 3 "
            # 걸러야 하는 것은 **출처가 아니라 gidx 규약**이다. 예전엔 `source='userwatch'` 로
            # 좁혔는데, 같은 규약을 지키는 사내 뱅크(hybrid·internal)까지 통째로 빠져
            # 패널이 조용히 `external_only` 폴백으로 떨어졌다 (2026-08-28 vOPT/vGEN 실측).
            # 규약 = `bank_sentences.gidx` 가 0부터 시작하는 뱅크-로컬 행 번호.
            "HAVING min(s.gidx) = 0 OR count(s.sentence_id) = 0", ())
    except Exception as e:      # noqa: BLE001 — DSN 부재·DB 다운 전부 폴백 대상
        _PDB_BANKS, _PDB_BANKS_ERR = {}, f"{type(e).__name__}: {e}"
        return _PDB_BANKS
    out = {}
    for bank_id, tag, storage, n in rows:
        key = pdb_norm_ver(tag)
        # 대소문자만 다른 두 행이 같은 버전으로 접히면 문장이 많은 쪽을 쓴다.
        if key not in out or int(n) > out[key][3]:
            out[key] = (bank_id, tag, storage, int(n))
    _PDB_BANKS, _PDB_BANKS_ERR = out, None
    return out


def pdb_fetch_texts(version, locals_):
    """(버전, 로컬 gidx 목록) → {local_gidx: (text, class_label)}.

    `WHERE bank_id = %s AND gidx = ANY(%s)` — UNIQUE(bank_id, gidx) 인덱스를 그대로 탄다.
    **전량 로드 금지**: 뷰에 그려지는 행만 묻는다(패널 기준 최대 MAX_POINTS).
    같은 (버전, 행)은 프로세스 안에서 두 번 묻지 않는다 — 서브샘플이 캐시돼 있어
    두 번째 갱신부터는 전부 메모 적중이다.
    """
    bank = pdb_banks().get(pdb_norm_ver(version))
    if bank is None:
        return {}
    key = pdb_norm_ver(version)
    want = sorted({int(g) for g in locals_ if g is not None})
    got = {g: _PDB_TEXT[(key, g)] for g in want if (key, g) in _PDB_TEXT}
    miss = [g for g in want if g not in got]
    if miss:
        rows = _pdb_query(
            "SELECT gidx, text, class_label FROM bank_sentences "
            " WHERE bank_id = %s AND gidx = ANY(%s)", (bank[0], miss))
        if len(_PDB_TEXT) > PDB_MEMO_CAP:
            _PDB_TEXT.clear()
        for g, text, label in rows:
            got[int(g)] = (text, label)
            _PDB_TEXT[(key, int(g))] = (text, label)
    return got


def pdb_fetch_vectors(version, locals_):
    """(버전, 로컬 gidx 목록) → {local_gidx: [float, …]} (1024-d).

    `bank_sentences.content_hash` → `image_embeddings(entity_type='prompt')` 조인.
    pgvector 값은 psycopg2 어댑터가 없어 `'[0.1,0.2,…]'` 문자열로 온다 — `::text` 로
    의도를 못박고 여기서 파싱한다. 메모하지 않는다(1024-d × 수만 행 = GB 단위).
    """
    bank = pdb_banks().get(pdb_norm_ver(version))
    if bank is None:
        return {}
    want = sorted({int(g) for g in locals_ if g is not None})
    if not want:
        return {}
    rows = _pdb_query(
        "SELECT s.gidx, e.embedding::text FROM bank_sentences s "
        "  JOIN image_embeddings e ON e.entity_type = 'prompt' "
        "   AND e.entity_id = s.content_hash AND e.model_name = %s "
        " WHERE s.bank_id = %s AND s.gidx = ANY(%s)", (PDB_MODEL, bank[0], want))
    return {int(g): [float(x) for x in str(v).strip("[]").split(",")] for g, v in rows}


PDB_TOPK = 5              # 버전당 보여줄 후보 수 (1위=승자, 2~5위=차순위)
_TOPK_MEMO = {}           # 키 = (dataset, frame_id, k, 스키마 세대). 훅 재발화마다 1초 질의 반복 방지.
_TOPK_SCHEMA = 2          # 행 dict 모양이 바뀌면 올린다 (2 = `cls` 추가)


def pdb_frame_vector(filepath):
    """FiftyOne 에 `embedding` 필드가 없는 데이터셋용 폴백 — Postgres 프레임 벡터.

    실측(2026-08-31): `frames`(199,972장) 에는 `embedding` 필드가 **아예 없다**. 대신
    미디어 파일명이 파이프라인 UUID 이고 그게 `image_embeddings.entity_id`
    (`entity_type='frame'`, 188,190행) 다. 인코더가 문장 벡터와 같은
    `facebook/PE-Core-L14-336`(1024-d)라 그대로 비교된다.
    """
    uid = os.path.splitext(os.path.basename(str(filepath)))[0]
    rows = _pdb_query(
        "SELECT embedding::text FROM image_embeddings "
        " WHERE entity_type='frame' AND entity_id=%s AND model_name=%s LIMIT 1",
        (uid, PDB_MODEL))
    if not rows:
        return None
    return [float(x) for x in str(rows[0][0]).strip("[]").split(",")]


def pdb_frame_topk(dataset_name, frame_id, emb, k=PDB_TOPK):
    """프레임 1장 → **버전별 상위 k개 프롬프트**와 코사인 (한 번의 SQL).

    왜 필요한가: 데이터셋에 구워진 건 `winner_gidx_<태그>`(1위)뿐이라 2~5위를 볼 수 없고,
    "전체" 필터에서는 어느 버전 문장인지도 알 수 없다 — 같은 문장이 여러 버전에 공유돼
    (content_hash 로 벡터를 공유) 통합 top-k 가 한 문장의 복제본으로 덮인다
    (2026-08-31 실측: 상위 12개 중 10개가 같은 문장의 다른 버전, cos 전부 0.2769).
    그래서 **버전별(PARTITION BY bank_id) 순위**로 뽑는다.

    실측(2026-08-31, sourcei 프레임 1장): 37버전 × 5 = 185행 **1.06초**.
    1위는 저장된 `winner_gidx_*` 와 정확히 일치함을 확인했다(vOPT 584 · v1080 2487 ·
    vGEN 422) — 즉 이 질의는 기존 승자 정의를 재현한다.
    """
    # 캐시 키에 **행 스키마 세대**를 박는다. 프로세스가 살아 있는 동안 옛 모양의 행이
    # 남아 있으면 새 렌더가 `cls` 없는 행을 그린다 — 조용히 클래스가 `-` 로만 나온다.
    key = (dataset_name, str(frame_id), int(k), _TOPK_SCHEMA)
    if key in _TOPK_MEMO:
        return _TOPK_MEMO[key]
    vec = "[" + ",".join(f"{float(x):.6f}" for x in emb) + "]"
    rows = _pdb_query(
        "SELECT version_tag, gidx, txt, cls, cos, rnk FROM ("
        "  SELECT b.version_tag, s.gidx, s.text AS txt, s.class_label AS cls,"
        "         1-(e.embedding <=> %s::vector) AS cos,"
        "         row_number() OVER (PARTITION BY b.bank_id"
        "                            ORDER BY e.embedding <=> %s::vector) AS rnk"
        "    FROM bank_sentences s"
        "    JOIN prompt_banks b USING (bank_id)"
        "    JOIN image_embeddings e ON e.entity_type='prompt'"
        "     AND e.entity_id=s.content_hash AND e.model_name=%s"
        ") t WHERE rnk <= %s ORDER BY version_tag, rnk", (vec, vec, PDB_MODEL, int(k)))
    # `cls` = `bank_sentences.class_label` — 같은 테이블 같은 행이라 추가 질의가 없다.
    # ⚠️ 이건 **문장이 노리는 클래스(뱅크 정의)** 지 사람 라벨도 모델 판정도 아니다.
    out = [{"ver": str(v), "gidx": int(g), "text": str(t)[:110],
            "cls": str(cl or "-"), "cos": round(float(c), 4), "rank": int(r)}
           for v, g, t, cl, c, r in rows]
    while len(_TOPK_MEMO) > 4:          # 프레임 몇 장만 — panel_state 왕복 부담을 안 키운다
        _TOPK_MEMO.pop(next(iter(_TOPK_MEMO)))
    _TOPK_MEMO[key] = out
    return out


def cls_counts_line(classes):
    """표시된 행들의 **클래스 분포** 한 줄 (개수 내림차순). 값이 없으면 빈 문자열.

    왜: 두 표는 최대 50행·185행이라 클래스 열만으로는 "이 프레임에 어떤 클래스 문장이
    몰리는가" 를 눈으로 세야 한다. 한 줄 요약이 그 질문에 바로 답한다.

    ⚠️ **"표시" 를 반드시 밝힌다.** 이 분포는 잘린 표시분의 분포이고 모집단(뱅크 전체)
    분포가 아니다 — 표기 없이 내놓으면 채택 비율을 30배 부풀려 읽던 사고와 같은 형태다.
    """
    from collections import Counter
    got = [str(c) for c in classes if c not in (None, "", "-")]
    if not got:
        return ""
    cnt = Counter(got)
    body = " · ".join(f"{c} {n:,}" for c, n in
                      sorted(cnt.items(), key=lambda kv: (-kv[1], kv[0])))
    return f"*표시 {len(got):,}행 클래스: {body}*"


def topk_to_markdown(rows, only_version=None, src=None, ref_class=None):
    """버전별 1~5위 표. `only_version` 이 주어지면 그 버전만. `src` = 프레임 벡터 출처.

    `ref_class`: 이 프레임 자신의 `category` 값 — 1위 행이 그것과 같은지 표기한다.
    없으면(필드 없음·None) 기준 표기 전체를 생략한다.
    """
    if not rows:
        return "*(선택 없음)*"
    if len(rows) == 1 and "err" in rows[0]:
        return f"*⚠️ {rows[0]['err']}*"
    if only_version and only_version != ALL_VERSIONS_LABEL:
        rows = [r for r in rows if pdb_norm_ver(r["ver"]) == pdb_norm_ver(only_version)]
        if not rows:
            return f"*({only_version} 뱅크에 문장이 없습니다)*"
    # ⚠️ 버전마다 표를 따로 뽑으면 "전체" 에서 37개 표가 세로로 쌓여 읽을 수 없다
    #    (2026-08-31 사용자 리포트: "승자 문장들이 보기가 힘든 점").
    #    **한 표 + 버전 열**로 합치고 cos 내림차순으로 정렬한다 — 어느 버전이 이 프레임을
    #    가장 잘 설명하는지가 위에서부터 바로 읽힌다.
    out = []
    if src:
        out.append(f"*이미지 벡터 출처: {src} · 점수 = 이미지↔문장 코사인 "
                   f"(top-k 기준, 제품 판정규칙 아님)*")
        out.append("")
    # ⚠️ 정렬 = **버전 블록 단위**, 블록 안에서는 1→5 순위 (2026-08-31 사용자 요청:
    #    "1버전 1,2,3,4,5 · 2버전 1,2,3,4,5 순서로"). 전역 cos 내림차순으로 섞으면
    #    한 버전의 1~5위가 흩어져 버전 간 비교가 안 된다.
    #    버전 **블록의 순서**는 그 버전의 1위 cos 내림차순 — 이 프레임을 가장 잘 설명하는
    #    버전이 맨 위로 온다.
    best = {}
    for r in rows:
        best[r["ver"]] = max(best.get(r["ver"], -2.0), float(r["cos"]))
    n_ver = len(best)
    out.append(f"*{n_ver}개 버전 × 상위 {PDB_TOPK} · 버전별 1→{PDB_TOPK}위, "
               f"버전 순서는 1위 cos 내림차순*")
    # 기준 = 이 프레임의 `category`. ⚠️ **사람 정답이 아니다** — sourcei 에서 이 필드는
    # v1.0.8.0 뱅크의 argmax 예측이다(실측 3중 확인). "정답/오답/적중" 으로 부르면
    # 자기참조 평가가 되므로 "기준" 이라는 말만 쓴다. 필드가 없으면 이 줄이 아예 없다.
    if ref_class:
        out.append(f"*기준 = 이 프레임의 `category` = **{ref_class}** "
                   f"(v1.0.8.0 argmax · 사람 정답 아님)*")
    dist = cls_counts_line([r.get("cls") for r in rows])
    if dist:
        out.append(dist)
    out.append("")
    out.append("| 버전 | 순위 | 클래스 | cos | 문장 |")
    out.append("|---|---:|---|---:|---|")
    prev = None
    for r in sorted(rows, key=lambda x: (-best[x["ver"]], str(x["ver"]), int(x["rank"]))):
        mark = "🏆" if r["rank"] == 1 else str(r["rank"])
        txt = str(r["text"]).replace("|", chr(92) + "|")
        ver = "" if r["ver"] == prev else str(r["ver"])   # 블록 안에서는 버전명 반복 생략
        prev = r["ver"]
        # `cls` 없는 행 = 이 프로세스에 남아 있던 옛 스키마 캐시/구형 panel_state 에코.
        cls = str(r.get("cls") or "-")
        # 기준 일치는 **1위 행에만** 붙인다 — 2~5위는 그 버전의 승자가 아니라서
        # 일치/불일치가 판정과 무관하다.
        if ref_class and r["rank"] == 1 and cls != "-":
            cls += " (= 기준)" if cls == ref_class else " (≠ 기준)"
        out.append(f"| {ver} | {mark} | {cls} | {r['cos']:.4f} | {txt} |")
    return "\n".join(out)


def pdb_version_counts(versions):
    """버전별 행 수 — 게이트 ②의 분모(그 버전이 데이터셋에서 차지하는 행 수)."""
    counts = {}
    for v in versions:
        if v is not None:
            counts[str(v)] = counts.get(str(v), 0) + 1
    return counts


def pdb_resolve_texts(versions, gidxs, fallback, ver_counts, categories=None):
    """샘플 정렬 시퀀스 → (문장 리스트, 출처 메타). 위 게이트 ①②③ 적용.

    versions[i]  샘플의 `bank_version.label`   gidxs[i]  샘플의 전역 `gidx`
    fallback[i]  데이터셋 `text` 필드 값(폴백)  categories[i]  `category.label`(게이트 ③)
    ver_counts   {버전: 데이터셋 전체 행 수} — `pdb_version_counts()` 로 만든다.
                 (전체가 아닌 표시분으로 만들면 게이트 ②가 항상 실패한다.)
    """
    out = list(fallback)
    # `corrupt` = **그리면 조용한 오답이 되는 버전**. `reject` 와 다르다:
    #   · external_only  → 텍스트만 없고 벡터는 유효 → reject 이지만 corrupt 아님 (그린다)
    #   · 행수 불일치·class 불일치 → gidx 정렬이 깨져 **벡터 귀속 자체가 틀렸다** → corrupt
    # v1.0.2.0 이 후자다: 공급자 JSON 이 v1.0.2.1 과 **바이트 동일**(md5 6c387ea8… 실측)이라
    # 뷰의 14,600점은 v1.0.2.1 의 벡터다. 텍스트는 자리표시자라 눈에 보이지만, 기하 비교에
    # 끼면 v1.0.2.1 을 자기 자신과 비교하게 된다 — 경고만으로는 못 막는다.
    meta = {"db_rows": 0, "db_versions": [], "reject": {}, "corrupt": [], "err": None}
    if not pdb_enabled():
        meta["err"] = "PROMPT_DB=off"
        return out, meta
    banks = pdb_banks()
    if not banks:
        meta["err"] = _PDB_BANKS_ERR or "prompt_banks 0행"
        return out, meta

    by_ver = {}
    for i, v in enumerate(versions):
        if v is not None and gidxs[i] is not None:
            by_ver.setdefault(str(v), []).append(i)

    def _reject(why, detail):
        meta["reject"].setdefault(why, []).append(detail)

    for version, idxs in by_ver.items():
        bank = banks.get(pdb_norm_ver(version))
        if bank is None or bank[3] == 0:
            _reject("DB 문장 미보유(external_only)", version)
            continue
        want = ver_counts.get(version) if ver_counts else None
        if want is not None and int(want) != bank[3]:
            # ② 행수 불일치 = gidx 정렬 붕괴. 실측 v1.0.2.0 이 여기서 걸린다.
            _reject("행수 불일치(gidx 정렬 불가)", f"{version} DB {bank[3]:,}≠뷰 {int(want):,}")
            meta["corrupt"].append(version)
            continue
        locs = [pdb_local_gidx(gidxs[i]) for i in idxs]
        try:
            got = pdb_fetch_texts(version, locs)
        except Exception as e:      # noqa: BLE001 — 폴백이 있다
            meta["err"] = f"{type(e).__name__}: {e}"
            _reject("조회 실패", version)
            continue
        if categories is not None:
            bad = next(
                (f"{version} gidx {g}: DB {got[g][1]} ≠ 뷰 {categories[i]}"
                 for i, g in zip(idxs, locs)
                 if g in got and categories[i] is not None and got[g][1] != categories[i]),
                None)
            if bad:
                _reject("class 불일치(정렬 붕괴)", bad)
                meta["corrupt"].append(version)
                continue
        n = 0
        for i, g in zip(idxs, locs):
            row = got.get(g)
            if row is not None:
                out[i] = row[0]
                n += 1
        if n:
            meta["db_rows"] += n
            meta["db_versions"].append(version)
    return out, meta


def pdb_note(meta, label="문장"):
    """배너 한 줄 — **어느 소스를 몇 행에 썼는지 항상 밝힌다** (조용한 폴백 금지).

    ⚠️ 배너는 단일 문단이어야 하므로 개행을 넣지 않는다 (형제 패널의 stale 문단 함정).
    """
    if meta.get("db_rows"):
        note = (f"{label} 출처: **{PDB_SRC_DB}** {meta['db_rows']:,}행"
                f"/{len(meta['db_versions'])}버전")
    else:
        note = f"{label} 출처: **{PDB_SRC_FALLBACK}** — DB 해석 0행"
    for why, items in sorted(meta.get("reject", {}).items()):
        head = ", ".join(sorted(items)[:2])
        more = f" 외 {len(items) - 2}" if len(items) > 2 else ""
        note += f" · ⚠️ 폴백 {len(items)}버전 [{why}: {head}{more}]"
    if meta.get("err"):
        note += f" · ⚠️ DB 오류: {meta['err']}"
    return note


def pdb_selftest():
    """DB 없이 도는 순수부 계약 (세 사본 모두 같은 검사를 갖는다)."""
    assert pdb_norm_ver("V1.0.10.3") == pdb_norm_ver("v1.0.10.3") == "1.0.10.3"
    assert pdb_norm_ver(None) == "" and pdb_norm_ver("1.0.13.0") == "1.0.13.0"
    assert pdb_local_gidx(300_012) == 12 and pdb_local_gidx(12) == 12
    assert pdb_local_gidx(None) is None
    assert pdb_version_counts(["a", "a", None, "b"]) == {"a": 2, "b": 1}

    global _PDB_BANKS, _PDB_BANKS_ERR
    saved, saved_err, saved_text = _PDB_BANKS, _PDB_BANKS_ERR, dict(_PDB_TEXT)
    try:
        # 게이트 ②: 행수가 다르면 그 버전은 통째로 폴백 (v1.0.2.0 실측 케이스)
        _PDB_BANKS, _PDB_BANKS_ERR = {"1.0.2.0": ("bid", "v1.0.2.0", "db_backed", 12568)}, None
        vers, gid, fb = ["v1.0.2.0"] * 2, [0, 1], ["(텍스트 없음 #0)", "(텍스트 없음 #1)"]
        out, meta = pdb_resolve_texts(vers, gid, fb, {"v1.0.2.0": 14600})
        assert out == fb and meta["db_rows"] == 0, (out, meta)
        assert "행수 불일치(gidx 정렬 불가)" in meta["reject"], meta
        # 정렬 붕괴는 reject(폴백 텍스트)로 끝나지 않는다 — 소비자가 **그리지 않도록**
        # corrupt 목록에도 실려야 한다. 이게 빠져 있어 경고만 뜨고 점은 계속 그려졌다.
        assert meta["corrupt"] == ["v1.0.2.0"], meta
        assert "폴백" in pdb_note(meta) and "\n" not in pdb_note(meta)

        # 게이트 ①: external_only(문장 0행)도 폴백
        _PDB_BANKS = {"1.0.13.0": ("bid", "v1.0.13.0", "external_only", 0)}
        out, meta = pdb_resolve_texts(["v1.0.13.0"], [7], ["(텍스트 없음 #7)"], {"v1.0.13.0": 45840})
        assert out == ["(텍스트 없음 #7)"] and "DB 문장 미보유(external_only)" in meta["reject"]

        # 게이트 ③ + 정상 경로: 행수가 맞고 class 도 맞으면 DB 가 이긴다
        _PDB_BANKS = {"1.0.8.0": ("bid", "v1.0.8.0", "db_backed", 3)}
        _PDB_TEXT.clear()
        for g, (t, c) in {0: ("A.", "fire"), 1: ("B.", "smoke"), 2: ("C.", "fire")}.items():
            _PDB_TEXT[("1.0.8.0", g)] = (t, c)
        vers, gid = ["v1.0.8.0"] * 3, [300_000, 300_001, 300_002]
        out, meta = pdb_resolve_texts(vers, gid, ["x", "y", "z"], {"v1.0.8.0": 3},
                                      categories=["fire", "smoke", "fire"])
        assert out == ["A.", "B.", "C."] and meta["db_rows"] == 3, (out, meta)
        assert not meta["reject"] and "DB 정본" in pdb_note(meta)
        out, meta = pdb_resolve_texts(vers, gid, ["x", "y", "z"], {"v1.0.8.0": 3},
                                      categories=["fire", "fire", "fire"])
        assert out == ["x", "y", "z"], out                 # class 어긋나면 그 버전 전체 폴백
        assert "class 불일치(정렬 붕괴)" in meta["reject"], meta

        # 뱅크를 못 읽으면 전부 폴백 + 사유가 배너에 실린다
        _PDB_BANKS, _PDB_BANKS_ERR = {}, "OperationalError: down"
        out, meta = pdb_resolve_texts(["v1.0.8.0"], [0], ["fb"], {})
        assert out == ["fb"] and "down" in pdb_note(meta)
    finally:
        _PDB_BANKS, _PDB_BANKS_ERR = saved, saved_err
        _PDB_TEXT.clear()
        _PDB_TEXT.update(saved_text)


def _bundle_nbytes(b):
    import numpy as np
    return sum(v.nbytes for v in b.values() if isinstance(v, np.ndarray))


def load_prompt_bundle(dataset_name=PROMPTS_DATASET):
    """문장 좌표+메타 로드. embedding(1024-d)은 절대 읽지 않는다 (스펙 §5.5).

    Task 12: dataset_name 파라미터화 — source-h 세션은 "source-h-prompts", sourcei 세션은
    "sourcei-prompts"를 각자 넘긴다. 캐시 키에 dataset_name이 들어가므로 데이터셋을
    전환하면 재계산되지만, `_CACHE.clear()`가 항상 먼저 실행돼 1엔트리만 유지된다
    (스펙 §5.5 캐시 예산은 여전히 활성 엔트리 1개 기준).
    """
    import numpy as np
    ds = fo.load_dataset(dataset_name)
    key = (dataset_name, BRAIN_KEY, data_version(ds))   # data_version docstring 참고
    if key in _CACHE:
        return _CACHE[key]
    res = ds.load_brain_results(BRAIN_KEY)
    xy = np.asarray(res.points, dtype="float32")
    # ⚠️ 좌표↔메타 정렬은 **brain result 의 sample_ids 기준**이다 (형제 패널
    #    user-image-embeddings.load_image_bundle 와 같은 계약). 구현은 `ds.values(...)`
    #    순서가 곧 좌표 순서라고 가정했는데, brain run 이 데이터셋 일부만 덮으면 그 가정이
    #    깨진다 — 2026-08-19 실측: `frames-prompts` 는 샘플 615,296 인데 `emb_viz` 는
    #    603,318점(캡션 11,978행 제외)이라 `build_mode_a` 가 IndexError 로 죽었고,
    #    반대로 메타가 더 짧았다면 **크래시 없이 좌표가 통째로 밀려** 엉뚱한 점에 엉뚱한
    #    문장이 붙는다. sample_ids 로 명시 정렬해 두 경우를 모두 없앤다.
    brain_ids = [str(i) for i in res.sample_ids]
    b = {"xy": xy}
    schema = ds.get_field_schema()
    # ⚠️ 필드별 `ds.values(f)` 를 돌면 컬렉션을 **필드 수만큼 전체 순회**한다. 28,605행 시절엔
    #    무해했지만 29버전 리빌드로 603,318행이 되며 10회 순회 = **119.5초(실측)** 가 되어
    #    오퍼레이터 600초 타임아웃(`on_rule_change`)까지 터졌다. `values([...])` 는 **한 번의
    #    집계**로 전 필드를 가져온다 — 배열·후처리·길이가 모두 동일해 의미 변화가 없다.
    #    ⚠️ 그리고 **Classification 필드는 `.label` 로 직접 읽는다**. `values("category")` 는
    #    603,318개 Classification 객체를 만든 뒤 우리가 `.label` 을 꺼내는데, 실측
    #    `category` 25s vs `category.label` 1.9s / `wave_role` 25.3s vs 2.7s — **9~13배**다.
    #    META_FIELDS 의 embedded 4종(category·adopted·wave_role·bank_version)이 로드 시간의
    #    거의 전부였다(105s 중 ~100s). 값은 아래 후처리가 뽑던 것과 **동일한 문자열**이다.
    have, paths = [], ["id"]
    for f in META_FIELDS:
        if f not in schema:
            continue
        have.append(f)
        paths.append(f + ".label"
                     if type(schema[f]).__name__ == "EmbeddedDocumentField" else f)
    cols = ds.values(paths)
    pos = {str(v): i for i, v in enumerate(cols[0])}
    order = [pos.get(sid, -1) for sid in brain_ids]
    if any(o < 0 for o in order):        # brain 에만 있고 데이터셋엔 없는 잔재 방어
        keep = [k for k, o in enumerate(order) if o >= 0]
        xy = b["xy"] = xy[np.asarray(keep, dtype=np.int64)]
        order = [order[k] for k in keep]
    if order == list(range(len(cols[0]))):
        got = dict(zip(have, cols[1:]))   # 순서 동일 — 재색인 비용 0 (sourcei-prompts 경로)
    else:
        got = {f: [col[o] for o in order] for f, col in zip(have, cols[1:])}
    for f in META_FIELDS:
        if f not in schema:
            b[f] = None
            continue
        vals = got[f]
        if not vals:
            b[f] = None   # 0행 데이터셋 — IndexError 대신 필드 없음과 동일 취급 (opus F10)
            continue
        # dtype 판정은 **첫 non-None 값**으로 (opus F10): 0번 행이 None 인 컬럼(예: wave
        # 캐시 없는 버전이 앞에 오는 wave_role)을 vals[0]으로 판정하면 Classification
        # 객체 배열이 그대로 남아 그룹명이 "<Classification: ...>" 로 쪼개진다.
        probe = next((v for v in vals if v is not None), None)
        if hasattr(probe, "label"):
            vals = [v.label if v else None for v in vals]
            probe = next((v for v in vals if v is not None), None)
        b[f] = np.asarray(vals, dtype=object) if isinstance(probe, str) \
            else np.asarray([0 if v is None else v for v in vals])
    if b.get("adopted") is not None and b["adopted"].dtype == object:
        b["adopted"] = np.asarray([v in (True, "채택", "true") for v in b["adopted"]])
    # 벡터화용 정수 코드 (2026-08-14): build_mode_a 의 버전 필터·층화 서브샘플이 603k행
    # Python 루프(str 비교·dict 적재)로 요청당 수 초씩 태우던 것을 C 속도 마스크로 바꾼다.
    # 로드 시 1회만 문자열화(캐시에 함께 저장) — int32 600k ≈ 2.4MB/필드라 캐시 예산 무해.
    for f in ("category", "bank_version"):
        if b.get(f) is not None:
            uniq, codes = np.unique(
                np.asarray(["" if v is None else str(v) for v in b[f]], dtype=object),
                return_inverse=True)
            b[f + "_uniq"] = uniq
            b[f + "_codes"] = codes.astype(np.int32)
    # ⚠️ 메시지에 상한을 하드코딩하지 않는다 — 64MB→192MB 로 올렸을 때 문구가 stale 해져
    #    실제로 캡에 걸려도 잘못된 숫자가 찍혔다 (codex 지적, 2026-08-12).
    # ⚠️ `_bundle_nbytes` 는 numpy `.nbytes` 만 더한다 — object dtype 배열이 참조하는
    #    파이썬 문자열 실메모리는 세지 않으므로, 이 상한은 RSS 상한이 아니라 배열 바이트 근사다.
    assert _bundle_nbytes(b) <= CACHE_CAP_BYTES, (
        f"캐시 예산 초과: {_bundle_nbytes(b)/2**20:.1f}MB > {CACHE_CAP_BYTES/2**20:.0f}MB "
        "(배열 바이트 기준 — 문자열 실메모리 미포함)")
    _CACHE.clear()          # 엔트리 1개만 유지
    _CACHE[key] = b
    return b


def _gidx_block(coll, field):
    """이 컬렉션 필드가 쓰는 gidx 블록(오프셋 세대). 여러 블록에 걸치면 None."""
    try:
        lo, hi = coll.bounds(field)
    except Exception:      # noqa: BLE001 — 진단 실패가 조인을 죽이면 안 된다
        return None
    if lo is None or hi is None:
        return None
    b0, b1 = int(lo) // PDB_GIDX_OFFSET, int(hi) // PDB_GIDX_OFFSET
    return b0 if b0 == b1 else None


_SHIFT_MEMO = {}    # (dataset, winner_field) -> shift. 프로세스 수명 동안 유지.
_PVERS_MEMO = {}    # prompts 데이터셋 -> bank_version 목록
_PBLOCK_MEMO = {}   # prompts 데이터셋 -> {version: gidx 블록 | None}
_FBLOCK_MEMO = {}   # (frames 데이터셋, winner 필드) -> 블록 | None


def _prompt_blocks(prompts):
    """버전별 gidx 블록 `{version: block|None}` — **한 번의 `values()`** 로 전부 계산.

    ⚠️ 원래는 버전마다 `match(...).bounds("gidx")` 를 돌아 31버전에 **7.6초**였다
    (2026-08-31 실측). 블록은 재빌드 전까지 상수라 한 번만 읽는다. `None` 은 "여러 블록에
    걸침" 으로 `_gidx_block` 과 **같은 의미**다(보정 근거 없음 → shift 0).
    """
    name = prompts.name
    if name in _PBLOCK_MEMO:
        return _PBLOCK_MEMO[name]
    out = {}
    try:
        # 패널은 산점도를 그리려고 이미 번들을 로드해 둔다 — 캐시 히트면 왕복 0회다.
        # (번들 미로드 시에만 `values()` 전량 왕복으로 폴백: 607k 행 ≈ 2초)
        try:
            _b = load_prompt_bundle(name)
            gs, vs = _b["gidx"], _b["bank_version"]
        except Exception:      # noqa: BLE001
            gs, vs = prompts.values(["gidx", "bank_version.label"])
        # ⚠️ 파이썬 루프 금지 — 60만 행이라 그것만 10초대다. numpy 로 버전별 min/max.
        import numpy as np
        g_arr = np.asarray([-1 if x is None else int(x) for x in gs], dtype=np.int64)
        v_arr = np.asarray(["" if not x else str(x) for x in vs], dtype=object)
        ok = g_arr >= 0
        blocks = g_arr // PDB_GIDX_OFFSET
        out = {}
        for v in {x for x in v_arr[ok].tolist() if x}:
            m = ok & (v_arr == v)
            bs = blocks[m]
            if bs.size:
                lo, hi = int(bs.min()), int(bs.max())
                out[v] = lo if lo == hi else None
    except Exception:      # noqa: BLE001 — 실패는 "보정 없음"(=0)으로 흡수
        out = {}
    _PBLOCK_MEMO[name] = out
    return out


def _prompt_versions(prompts):
    """문장 데이터셋의 뱅크 버전 목록 — **한 번만** 집계한다.

    ⚠️ `distinct("bank_version.label")` 는 607,318행 집계라 ~1.3초다. 이 값은 winner
    필드와 무관하게 같은데 필드마다 다시 돌아서 31필드 순회가 41초였다 (2026-08-31).
    """
    name = prompts.name
    if name not in _PVERS_MEMO:
        _PVERS_MEMO[name] = [v for v in prompts.distinct("bank_version.label") if v]
    return _PVERS_MEMO[name]


def gidx_shift(dataset_name, winner_field):
    """`_gidx_shift_uncached` 의 메모 래퍼.

    ⚠️ 메모가 없으면 **필드마다** `prompts.distinct("bank_version.label")`(607,318행 집계)이
    돈다. 버전별 승자를 모으면서 31필드를 순회하니 프레임 1장 선택이 **41초** 걸렸다
    (2026-08-31 실측). 오프셋은 데이터 재빌드 전까지 상수라 캐시가 정당하다.
    """
    key = (str(dataset_name), str(winner_field))
    if key not in _SHIFT_MEMO:
        _SHIFT_MEMO[key] = _gidx_shift_uncached(dataset_name, winner_field)
    return _SHIFT_MEMO[key]


def _gidx_shift_uncached(dataset_name, winner_field):
    """프레임 필드 gidx → `<ds>-prompts` gidx 공간으로 옮기는 보정값 (0 = 세대 일치).

    ⚠️ **이 보정이 없으면 조용히 남의 버전 문장에 붙는다.** 오프셋은 `prompt_geometry` 가
    `BANKS.index(version) * GIDX_OFFSET` 로 붙이는데 `BANKS` 는 런타임 env(`BANK_LIST`/
    `BANK_A`/`BANK_B`)에서 오므로 **같은 데이터셋도 실행마다 다른 블록**을 쓴다.
    실측(2026-08-20):
        frames.winner_gidx_v1080    블록 0   (attach 를 2버전 리스트로 돌린 세대)
        frames-prompts v1.0.8.0     블록 18  (프롬프트맵을 29버전으로 돌린 세대)
        sourcei.winner_gidx_v1080   블록 18  (두 세대가 같아 파일럿에서 안 보였다)
    그래서 등식 조인이 fire_smoke 뷰의 승자 511개를 **v1.0.1.0 문장**에 붙였다 —
    개수는 맞고 정체가 틀리는 조용한 오답. 로컬 인덱스로는 511/511 일치, 등식으로는 교집합 0.

    루트 README 의 계약("FiftyOne gidx 는 전역 값이라 조인은 `winner_gidx % 100000` 으로")을
    **이 한 곳에서** 구현한다. 소비자(하이라이트·문장 표·row_of)는 문장 키공간 하나만 보므로
    경계에서 한 번 옮기는 것으로 끝난다. DB(019)도 `(bank_id, 로컬 gidx)` 복합키라 로컬
    인덱스가 정본 표현이고, 전역 오프셋은 App 사이드바가 IntField 만 필터할 수 있어서
    생긴 렌더 계층 임시방편이다.
    """
    if not fo.dataset_exists(dataset_name):
        return 0
    pname = f"{dataset_name}-prompts"
    if not fo.dataset_exists(pname):
        return 0
    prompts = fo.load_dataset(pname)
    psch = prompts.get_field_schema()
    if "gidx" not in psch or "bank_version" not in psch:
        return 0
    ver = next((v for v in _prompt_versions(prompts)
                if version_to_winner_field(v) == winner_field), None)
    if ver is None:
        return 0      # 이 필드에 대응하는 뱅크 버전이 문장 쪽에 없다 — 보정 근거 없음
    fkey = (str(dataset_name), str(winner_field))
    if fkey not in _FBLOCK_MEMO:
        _FBLOCK_MEMO[fkey] = _gidx_block(fo.load_dataset(dataset_name), winner_field)
    fb = _FBLOCK_MEMO[fkey]
    pb = _prompt_blocks(prompts).get(ver)
    if fb is None or pb is None:
        return 0
    return (pb - fb) * PDB_GIDX_OFFSET


def frame_ids_to_gidx(frame_ids, dataset_name=FRAMES_DATASET, winner_field=WINNER_FIELD):
    frames = fo.load_dataset(dataset_name)
    if winner_field not in frames.get_field_schema():
        return []   # Task 12: 조인 필드 없음 — 크래시 대신 빈 결과 (호출부가 계속 진행)
    shift = gidx_shift(dataset_name, winner_field)   # 오프셋 세대 보정 (gidx_shift 주석)
    vals = frames.select(frame_ids).values(winner_field)
    return sorted({int(v) + shift for v in vals if v is not None})


def dataset_winner_fields(dataset_name):
    """이 프레임 데이터셋에 **실제로 있는** (버전, winner 필드) 목록.

    `WINNER_FIELD`(=winner_gidx_v1080) 하드코딩을 대체한다. 그 상수는 sourcei 기준
    레거시 기본값이라, "전체" 를 골라도 승자 조인이 v1.0.8.0 **한 버전만** 봤다
    (2026-08-31 사용자 리포트: "cos 가 더 높은 값도 있는데 왜 이 문장이 승자인가" —
    실측하니 그 문장은 v1.0.8.0 안에서는 정확히 1위였고, 더 높은 cos 는 전부 다른
    버전 것이었다. 버그가 아니라 **범위 불일치**였다).
    """
    frames = fo.load_dataset(dataset_name)
    sch = frames.get_field_schema()
    out = []
    for ver in (pdb_banks() or {}).values():
        tag = str(ver[1])
        f = version_to_winner_field(tag)
        if f in sch:
            out.append((tag, f))
    if not out:      # DB 미가용 등 — 최소한 레거시 기본값이라도
        if WINNER_FIELD in sch:
            out = [(str(WINNER_FIELD).replace("winner_gidx_", ""), WINNER_FIELD)]
    return sorted(set(out))


def frame_ids_to_gidx_all(frame_ids, dataset_name):
    """프레임 → **버전별 승자 gidx** {version: [gidx]}. 필드 전부를 한 번에 읽는다.

    "전체" 필터에서 쓴다 — 그래야 위 승자 표와 아래 버전별 top-k 표의 **범위가 같아진다**.
    """
    pairs = dataset_winner_fields(dataset_name)
    if not pairs:
        return {}
    frames = fo.load_dataset(dataset_name)
    fields = [f for _v, f in pairs]
    # ⚠️ `values([f])` 는 **필드가 하나여도** 필드별 리스트를 돌려준다([[v, …]]). 한 겹 더
    #    감싸면 `frames`(winner 필드 1개)에서만 `int([v])` 로 터진다 — sourcei(31개)에서는
    #    분기를 안 타서 안 보이는, "한 데이터셋에서만 깨지는" 전형이다 (2026-08-31).
    cols = frames.select(frame_ids).values(fields)   # 왕복 1회
    out = {}
    for (ver, f), vals in zip(pairs, cols):
        shift = gidx_shift(dataset_name, f)          # 오프셋 세대 보정 (gidx_shift 주석)
        gs = sorted({int(v) + shift for v in (vals or []) if v is not None})
        if gs:
            out[ver] = gs
    return out


def gidx_to_frame_ids(g, dataset_name=FRAMES_DATASET, winner_field=WINNER_FIELD):
    frames = fo.load_dataset(dataset_name)
    if winner_field not in frames.get_field_schema():
        return []   # Task 12: 조인 필드 없음 — 크래시 대신 빈 결과 (호출부가 계속 진행)
    shift = gidx_shift(dataset_name, winner_field)   # 역변환 (gidx_shift 주석)
    return frames.match(fo.ViewField(winner_field) == int(g) - shift).values("id")


def gidxes_to_frame_ids(gs, dataset_name=FRAMES_DATASET, winner_field=WINNER_FIELD):
    """복수 gidx → 프레임 id 일괄 조인 (lasso 다중선택용 — gidx당 쿼리 1회 대신 is_in 1회)."""
    frames = fo.load_dataset(dataset_name)
    if winner_field not in frames.get_field_schema():
        return []
    shift = gidx_shift(dataset_name, winner_field)   # 역변환 (gidx_shift 주석)
    return frames.match(
        fo.ViewField(winner_field).is_in([int(g) - shift for g in gs])).values("id")


# ── Task 12 — 뱅크 버전 → 조인 필드 매핑 + 프롬프트 데이터셋 자동 유도 ──

def version_to_winner_field(version):
    """버전 문자열 → winner_gidx_v<태그> 필드명 — **prompt_geometry.vtag 와 문자 단위 동일**.

    예: "v1.0.8.0" -> "winner_gidx_v1080",  "v1.0.13.2" -> "winner_gidx_v10132"
    (2026-08-11: 마지막 3자리 방식은 v1.0.5.0/v2.0.5.0 이 같은 v050 으로 붕괴.)
    ⚠️ digits-only(re.sub(r"\\D",...))로 만들면 안 된다 (opus F3, 2026-08-12): 큐레이션
    버전명("v1.0.8.4-prune205")에서 프로듀서는 "v1084-prune205", digits 는 "v1084205" 로
    갈라져 실재하는 필드를 "없음"으로 안내한다. 점 split 조인만이 프로듀서와 일치한다.
    """
    return "winner_gidx_v" + "".join(str(version).lstrip("vV").split("."))


def _resolve_join_field(dataset, version):
    """버전 → 조인 필드, 단 세션 데이터셋 스키마에 실제로 없으면 None.

    호출부는 None을 "조인 필드 없음" 안내로 처리해야 하며 절대 KeyError/ValueError로
    죽으면 안 된다. (2026-08-11 리빌드로 sourcei/source-h 둘 다 v080·v084 조인 필드를
    갖게 됐지만, 새 버전 백필 전의 데이터셋에서는 여전히 None 경로가 정상 동작이다.)
    """
    if dataset is None or version is None:
        return None
    field = version_to_winner_field(version)
    try:
        schema = dataset.get_field_schema()
    except Exception:
        return None
    return field if field in schema else None


def _prompts_dataset_name(ctx):
    """모드 A 프롬프트 데이터셋 이름을 세션 데이터셋에서 유도: "<dataset>-prompts".

    ctx.dataset이 없는 호출(오프라인 selftest 등)은 레거시 PROMPTS_DATASET로 폴백한다.
    """
    ds = getattr(ctx, "dataset", None)
    if ds is not None:
        return f"{ds.name}-prompts"
    return PROMPTS_DATASET


def _current_winner_field(ctx):
    """프레임→문장 역방향 조인(그리드 체크박스/lasso)에 쓸 winner 필드.

    버전 필터가 특정 버전으로 잡혀 있으면 그 버전에서 유도, "전체"/미설정이면
    레거시 기본값(WINNER_FIELD=winner_gidx_v1080)으로 폴백 — 기존 sourcei 기본 동작과
    바이트 단위로 동일하게 유지(회귀 방지).
    """
    filt = _mem(ctx).bank_version_filter
    if filt and filt != ALL_VERSIONS_LABEL:
        return version_to_winner_field(filt)
    return WINNER_FIELD


def view_winner_gidx(ctx):
    """현재 뷰(프레임)가 뽑는 승자 문장 `gidx` 집합. 필터가 없으면 `None`.

    `None` = 데이터셋에 구워진 전역 `adopted` 를 그대로 쓴다(기존 동작, 회귀 없음).
    빈 집합은 "이 뷰에서 이기는 문장이 하나도 없다" 라 의미가 다르므로 구분한다.

    ⚠️ 프레임 데이터셋 세션에서만 성립한다. `-prompts` 세션의 `ctx.view` 는 **문장** 뷰라
       `winner_gidx_*` 를 낼 수 없다 — 거기서 억지로 구하면 빈 집합이 나와 화면의 채택이
       통째로 사라진다(크래시 없는 조용한 오답).
    """
    view = getattr(ctx, "view", None)
    session = getattr(getattr(ctx, "dataset", None), "name", None)
    if view is None or session is None or session.endswith("-prompts"):
        return None
    # ⚠️ **우리가 건 프레임 선택 스테이지는 빼고 본다** (2026-08-31 사용자 리포트:
    #    "전체뷰가 prompt embedding 시각화가 안 나오고 하나만 나온다").
    #    프레임 1장을 고르면 `show_samples` 가 Select 스테이지를 걸고, 그걸 뷰 필터로
    #    읽으면 "이 뷰의 승자" = 문장 1종이 되어 `표시=채택만` 산점도가 점 1개로 붕괴한다
    #    (실측 배너: `표시 2/607,318 (0.0%)`). 선택은 **강조**이지 모집단이 아니다 —
    #    형제 패널 user-image-embeddings 의 `view_without_our_selection` 과 같은 처치.
    stages = [t for t in (_client_view_stages(ctx) or [])
              if not (isinstance(t, dict) and t.get("_uuid") == SHOW_SAMPLES_STAGE_ID)]
    if not stages:
        return None            # 사용자 필터 없음 → 구워진 전역 채택 그대로 (기존 동작)
    # 스테이지가 없으면 전량이다 — 전체 프레임 `values()` 왕복을 피한다.
    if not getattr(view, "_stages", None):
        return None
    field = _current_winner_field(ctx)
    try:
        if field not in view.get_field_schema():
            return None
        shift = gidx_shift(session, field)        # 오프셋 세대 보정 (gidx_shift 주석)
        return {int(v) + shift for v in view.values(field) if v is not None}
    except Exception:      # noqa: BLE001 — 뷰 조회 실패가 패널을 죽이면 안 된다
        return None


def stratified_subsample(labels, max_points, seed=0):
    """클래스 비례 서브샘플, 클래스당 최소 1점 보장. 인덱스 리스트 반환."""
    import numpy as np
    arr = np.asarray(labels)
    n = len(arr)
    if n <= max_points:
        return list(range(n))
    rng = np.random.default_rng(seed)
    if arr.dtype != object:
        # 정수 코드/문자열 배열 경로 (2026-08-14): 603k행에서 Python dict 적재 루프가
        # 초 단위였다 — np.unique + 마스크로 클래스별 인덱스를 C 속도로 뽑는다. 계약
        # (클래스당 최소 1점·비례 배분·seed 결정론)은 동일, 클래스 순회 순서만
        # 등장순→정렬순으로 바뀐다 (개수 불변, 뽑히는 개별 점만 달라질 수 있음).
        groups = [np.nonzero(arr == u)[0] for u in np.unique(arr)]
    else:  # None 섞인 object 리스트 — np.unique 가 정렬에서 죽으므로 기존 경로 유지
        by_class = {}
        for i, lab in enumerate(labels):
            by_class.setdefault(lab, []).append(i)
        groups = list(by_class.values())
    # 클래스당 1점을 먼저 확보한 뒤 비례 배분 (opus F4): 구현이 out[:max_points] 로
    # 정렬 전 절단하면 반올림 합이 예산을 넘는 순간 삽입 순서상 뒤쪽 클래스가 통째로
    # 사라져 "클래스당 최소 1점 보장" docstring 계약이 깨진다.
    out = []
    extra = []
    for idxs in groups:
        k = max(1, int(round(len(idxs) / n * max_points)))
        pick = rng.choice(idxs, size=min(k, len(idxs)), replace=False).tolist()
        out.append(pick[0])
        extra.extend(pick[1:])
    rng.shuffle(extra)   # 절단이 특정 클래스에 몰리지 않게
    return sorted(out + extra[: max(0, max_points - len(out))])


# ── UI 계약 문자열 (스펙 §5.4 — 임의 수정 금지) ──
# 정정(2026-08-10): 구 문구 "제품 판정규칙(topk_vote K=10 다수결)"은 stale —
# pe_inference/01_TuningFree_v2.py에 top-k/argmax 0 hits. 실제 제품 판정은
# 분포 IoU(클래스별 cos 히스토그램 80bin vs normal, IoU<0.15) + 디바운스(5중 3) @2fps.
BANNER_RULE = ("이 조인은 top-k(K=1 전역 argmax) 승자 기준 — 실제 제품 판정"
               "(분포 IoU 80bin·thr 0.15 + 디바운스 5중3 @2fps)과 다른 값")
BANNER_COORDS_A = "좌우 UMAP은 독립 fit — 좌표 공간 비교 금지, 연결은 선택 하이라이트로만"
BANNER_WAVE_NOCLICK = "dist_iou에는 프레임 귀속이 없습니다 — 기여도는 전역 LOO(wave_gain)"
RESERVE_TEXT = "가져간 프레임 0 — 예비군 (새 카메라 승자의 66%가 여기서 나온다)"

GREY = "#CCCCCC"
# 회색 계열 금지 (사용자 피드백 2026-08-10): 미채택(GREY)·중간(#999999)·smoke(#7F7F7F)가
# 전부 무채색이라 서로 안 구분됐다 — 채택 팔레트(CLASS/WAVE_ROLE)에는 유채색만 쓴다.
# smoke=하늘색(#56B4E9): person(#009E73 초록, fiftyone_app_setup.py)과 겹치지 않는 잔여
# Okabe-Ito 유채색. normal(#0072B2)과 같은 파랑 계열이지만 명도 차가 큰 공인 구분쌍.
# 하이라이트 색 — 클래스 팔레트(#D55E00·#56B4E9·#E69F00·#0072B2·#CC79A7)와 무교집합.
# 채도가 높아 다크 배경·밀집 영역에서 가장 먼저 눈에 걸린다.
HILITE = "#FF2D95"

CLASS_COLORS = {  # Task 2와 동일 값 (배포 단위가 달라 복사 유지 — 변경 시 양쪽 동기화)
    "fire": "#D55E00", "smoke": "#56B4E9", "falldown": "#E69F00",
    "normal": "#0072B2", "smoking": "#CC79A7",
}
# dist_iou 전용 — wave_role 값 색칠. dict 순서 = 그리기 순서(z-order): 다수(중간 9,980)를
# 아래에 깔고 유익/유해를 위에 얹는다. 중간=회색 복귀(2026-08-10 정정): dist_iou 화면에서
# 미채택 회색 trace가 사라져(전 문장이 wave 분포 참여 — build_mode_a 참고) 회색 충돌이
# 없어졌고, 다수 중간은 배경으로 가라앉아야 유익/유해가 산다.
WAVE_ROLE_COLORS = {
    "중간": "#999999", "유익 상위10%": "#009E73", "유해 하위10%": "#D55E00",
}

# 색칠 축 — **규칙과 독립** (2026-08-12 사용자 요청). 기본값은 규칙별 기존 동작 유지.
COLOR_BY_CATEGORY = "category"
COLOR_BY_WAVE_ROLE = "wave_role"
COLOR_BY_FORM = "form"
COLOR_BY_RULE = "rule_ok"
MODE_MEANING = {"A": "문장↔프레임", "B": "그룹 overlay", "C": "군집"}
COLOR_BY_LABELS = {COLOR_BY_CATEGORY: "클래스", COLOR_BY_WAVE_ROLE: "wave 역할",
                   COLOR_BY_FORM: "문장 형태", COLOR_BY_RULE: "규칙 준수"}
# 문장 형태 — `prompt_standard.FORMS` 의 4종 + other. 클래스 팔레트와 겹치지 않게 골랐다.
FORM_COLORS = {"person_led": "#0072B2", "phenomenon_led": "#D55E00",
               "scene_led": "#009E73", "camera_led": "#CC79A7", "other": "#999999"}
# 규칙 준수 — 「미판정」(자리표시자·텍스트 없음)을 회색으로 **따로** 둔다. 비워 두면
# 통과와 구별되지 않아 부재를 준수로 읽는다.
RULE_COLORS = {"통과": "#009E73", "위반": "#D55E00", "미판정": "#666666"}


def _bundle_ver_counts(b):
    """번들 전체의 버전별 행 수 (게이트 ② 분모). 1회 계산해 번들에 얹는다.

    ⚠️ **표시분이 아니라 데이터셋 전체** 기준이어야 한다 — 서브샘플된 개수로 재면
    DB 행수와 항상 어긋나 모든 버전이 폴백된다.
    """
    counts = b.get("_ver_counts")
    if counts is None:
        bv = b.get("bank_version")
        counts = pdb_version_counts([] if bv is None else list(bv))
        b["_ver_counts"] = counts
    return counts


def _resolve_text(b, idxs):
    """번들 행 인덱스들 → (DB 정본 문장 리스트, 출처 메타).

    문장은 DB(`bank_sentences`)가 정본이고 데이터셋 `text` 는 npz 파생 폴백이다 —
    자세한 근거·게이트는 위 "prompt DB" 블록 주석 참고. 뷰에 그려지는 행만 묻는다.
    """
    idxs = [int(i) for i in idxs]
    bv, cat, txt, gid = (b.get("bank_version"), b.get("category"),
                         b.get("text"), b.get("gidx"))

    def _cell(arr, i):
        """배열 자체가 없거나(필드 부재) 그 칸이 None 이면 None — `"None"` 문자열 금지.

        ⚠️ `str(arr[i])` 로 뭉치면 frames-prompts 의 캡션 행(11,978개, bank_version·
        category 가 NULL)이 `"None"` 이라는 가짜 뱅크 버전으로 묶여 배너에 폴백 사유가
        허위로 뜬다. 그런 행은 애초에 뱅크 문장이 아니라 조인 대상이 아니다.
        """
        if arr is None or arr[i] is None:
            return None
        return str(arr[i])

    return pdb_resolve_texts(
        [_cell(bv, i) for i in idxs],
        [None if gid is None or gid[i] is None else int(gid[i]) for i in idxs],
        [("" if txt is None else _cell(txt, i) or "") for i in idxs],
        _bundle_ver_counts(b),
        categories=None if cat is None else [_cell(cat, i) for i in idxs],
    )


def _val(b, field, i, default="-"):
    """번들 칸 하나. **필드가 없는 데이터셋에서도 죽지 않는다.**

    `load_prompt_bundle` 은 스키마에 없는 META_FIELDS 를 `None` 으로 채우는데(예:
    `frames-prompts` 에는 `wave_gain`·`wave_role` 이 없다) 호출부가 곧바로 `[i]` 로
    첨자하면 `'NoneType' object is not subscriptable` 로 패널이 통째로 죽는다
    (2026-08-19 실측 — 좌표 정렬을 고친 뒤 드러난 다음 층 크래시).
    """
    arr = b.get(field)
    return default if arr is None else arr[i]


def _frame_ref_class(smp):
    """프레임 샘플의 `category` 라벨 — 없으면 `None` (표기 전체를 생략시킨다).

    ⚠️ 이 값은 **사람 정답이 아니다.** sourcei 에서 `category` 는 v1.0.8.0 뱅크의
    argmax 예측이다(실측 3중 확인: argmax(cos_best_*)==category.label 7,498/7,498,
    confidence 전부 null). 그래서 화면 문구는 "기준" 까지만 말하고 정답/정확도라고
    부르지 않는다 — 그 축으로 신버전을 채점하면 자기참조 평가가 된다.
    `frames` 처럼 이 필드가 없는 데이터셋도 있으므로 조회는 fail-soft 다.
    """
    try:
        cat = smp["category"]
    except Exception:               # noqa: BLE001 — 필드 없음/스키마 차이. 표기만 생략한다
        return None
    lab = getattr(cat, "label", cat)
    return None if lab in (None, "") else str(lab)


def _hover(b, i, texts=None):
    """호버 한 줄. `texts` 는 {번들 행 인덱스: 문장} — 주면 DB 정본을 싣는다."""
    text = (texts or {}).get(int(i))
    if text is None:
        text = _val(b, "text", i, "")
    return (f"[{_val(b, 'gidx', i)}] {str(text)[:80]}<br>"
            f"class={_val(b, 'category', i)} wins={_val(b, 'wins', i)} "
            f"purity={_val(b, 'purity', i)} wave_gain={_val(b, 'wave_gain', i)}")


def build_mode_a(bundle, rule, show_unadopted, selected_gidx, bank_version_filter=None,
                 color_by=None, view_winner=None):
    """문장 산점도 (모드 A). trace: [0]미채택 [1..k]채택(그룹별 1개) [마지막]하이라이트.

    채택점은 그룹(argmax_k1=클래스, dist_iou=wave_role)별로 trace를 쪼갠다 — Plotly 범례는
    trace 단위라, 단일 trace + per-point 색 배열이면 범례에 클래스→색 매핑이 아예 안 나온다
    (화면에 파랑 normal 240점이 있어도 범례엔 첫 점 색(주황) 글리프 하나 — 2026-08-10 실측 버그).

    bank_version_filter: None/"전체" -> 전 문장(기존 동작과 동일, 회귀 없음).
    특정 버전 문자열이면 그 버전 문장만 남기고 서브샘플/렌더 (Task 12).
    """
    import numpy as np
    b = bundle
    n = len(b["gidx"])
    idx_all = np.arange(n)
    # ── 채택(승자)을 **현재 뷰의 프레임 기준**으로 다시 매긴다 ──
    #    데이터셋에 구워진 `adopted` 는 전체 프레임 기준 **전역** 승자다. 뷰가 프레임을
    #    좁히면 그 프레임들이 뽑는 승자 집합이 달라진다 — 실측(2026-08-20):
    #      전역 17,230 · source-e(프레임 2,477) → 86 · appdata(24,572) → 2,678
    #    반영하지 않으면 화면이 "이 뷰의 승자" 인 척 전역 승자를 보여준다(조용한 오답).
    #    ⚠️ 캐시된 번들 dict 는 **수정하지 않는다** — 다른 렌더가 같은 객체를 본다.
    #    ⚠️ `gidx` 는 int64 라 np.isin 이 해시 경로를 탄다(0.001s). object dtype 이었으면
    #       373배 느렸다 — 형제 패널에서 그걸로 "Still loading" 이 났다.
    if view_winner is not None:
        want = np.fromiter(view_winner, dtype=np.int64, count=len(view_winner))
        adopted_all = np.isin(b["gidx"].astype(np.int64), want)
    else:
        adopted_all = b["adopted"].astype(bool)
    bv = b.get("bank_version")
    vfilt = bank_version_filter \
        if bank_version_filter and bank_version_filter != ALL_VERSIONS_LABEL else None
    if vfilt is not None and bv is not None:
        codes, uniq = b.get("bank_version_codes"), b.get("bank_version_uniq")
        if codes is not None:
            # 벡터화 (2026-08-14): 구 [str(bv[i]) == filter ...] 는 603k행 Python 루프로
            # 요청당 수 초 — 드롭다운 전환 왕복 ~20초(이중 발화 ×2 + 재발화 캐스케이드
            # 동시 실행)의 주범 중 하나였다. 코드 배열은 load_prompt_bundle 이 1회 계산.
            pos = np.nonzero(uniq == vfilt)[0]
            keep = (codes == pos[0]) if len(pos) else np.zeros(n, dtype=bool)
        else:  # 코드 배열 없는 합성 번들(selftest 픽스처) 하위호환
            keep = np.asarray([str(bv[i]) == vfilt for i in idx_all])
        idx_all = idx_all[keep]
    # ⚠️ **모집단**(버전 필터 적용 후, 서브샘플 전)을 여기서 붙잡는다 — 아래 서브샘플이
    #    idx_all 을 20,000 으로 줄여버리면 "전체가 몇 개였는지" 를 영구히 잃는다.
    #    기본 화면이 603,318 중 20,000(3.3%)만 그리면서 아무 표기도 없었고, 범례의
    #    "미채택 9,208" 은 실제 592,526 의 1/64 였다 (2026-08-14 감사 실측). 분석자가
    #    이걸 모르면 화면의 비율을 모집단 비율로 오독한다 — 채택점은 전수 보존되므로
    #    화면상 채택 비율이 모집단의 **약 29배**로 부풀어 있다.
    pop_idx = idx_all
    pop_n = len(pop_idx)
    adopted = adopted_all[idx_all]
    if len(idx_all) > MAX_POINTS:
        # 채택 점(승자 문장 — 이 패널의 존재 이유)은 전수 보존하고 **미채택만** 층화한다.
        # 29버전 60만 행에서 category 층화만 걸면 채택 ~5,600점이 ~185점으로 뭉개진다
        # (2026-08-12). 채택 총수는 버전당 ~200이라 MAX_POINTS 예산 안에 항상 들어간다.
        # 서브샘플은 (버전 필터, seed 고정)에만 의존 — 규칙/색칠/표시/선택과 무관하므로
        # 번들 안에 캐시해 드롭다운 왕복을 상수 시간으로 만든다 (2026-08-14). 번들이
        # _CACHE 1엔트리라 리빌드 시 캐시도 함께 증발 — 누수/불일치 없음.
        sub_cache = b.setdefault("_subsample_cache", {})
        cached = sub_cache.get(vfilt or ALL_VERSIONS_LABEL)
        if cached is None:
            keep_idx = idx_all[adopted][:MAX_POINTS]   # 채택 폭증 시에도 하드캡 유지 (codex A)
            rest = idx_all[~adopted]
            budget = max(0, MAX_POINTS - len(keep_idx))
            ccodes = b.get("category_codes")
            cats = ccodes[rest] if ccodes is not None \
                else [b["category"][i] for i in rest]
            sub_pos = np.asarray(stratified_subsample(cats, budget), dtype=np.int64)
            cached = np.sort(np.concatenate([keep_idx, rest[sub_pos]]))
            sub_cache[vfilt or ALL_VERSIONS_LABEL] = cached
        idx_all = cached
        adopted = adopted_all[idx_all]

    # ── 호버 문장을 **DB 정본**으로 (2026-08-19: "이제 gidx 그걸로 하지마") ──
    #    데이터셋 `text` 는 npz 를 `gidx % GIDX_OFFSET` 로 퍼온 파생물이라 603,318행 중
    #    261,244행(43.3%)이 `(텍스트 없음 #N)` 이다. 여기서 **그려지는 행만**(≤MAX_POINTS)
    #    배치 조회한다 — 서브샘플이 캐시돼 있어 두 번째 갱신부터는 전부 메모 적중이다.
    #    실패·거부는 아래 배너가 버전 단위로 밝힌다 (조용한 폴백 금지).
    hov_texts, text_meta = _resolve_text(b, idx_all)
    hov = dict(zip((int(i) for i in idx_all), hov_texts))   # 인덱스 키 — 아래 축소와 무관
    # ── 정렬 붕괴 버전은 **그리지 않는다** (2026-08-19) ──
    #    경고 배너만으로는 조용한 오답이 남는다: 화면에 점이 있는 한 분석자는 그 버전을
    #    비교에 넣는다. `corrupt` 판정 근거는 `_resolve_text` 주석 참고.
    bad_vers = set(text_meta.get("corrupt") or ())
    bv_all = b.get("bank_version")
    if bad_vers and bv_all is not None:
        keep = np.asarray([str(bv_all[i]) not in bad_vers for i in idx_all], dtype=bool)
        if not keep.all():
            text_meta["dropped_n"] = int((~keep).sum())
            idx_all = idx_all[keep]
            adopted = adopted_all[idx_all]
    # 호버는 예산 안에서만 **전송**한다. DB 조회 자체는 계속 한다 — 배너의 문장 출처
    # 회계(폴백 몇 버전, DB 정본 몇 행)가 거기서 나오므로 끊으면 조용한 폴백이 된다.
    hover_on = len(idx_all) <= HOVER_BUDGET

    def trace(mask, color, size, name, opacity):
        # "ids" (customdata 아님) — FiftyOne PlotlyView의 onClick 이벤트는 trace.ids[pointIndex]만
        # ctx.params["id"]로 전달한다 (App 번들 getIdForTrace 실측, 문서의 "data.customdata"는 오기).
        ii = idx_all[mask]
        # ⚠️ 좌표는 float64 로 넓힌 **뒤** 3자리 반올림. float32 를 그대로 round 하면
        #    1.3 이 float64 확장에서 1.2999999523162842 로 되살아나 무동작이다(실측).
        t = {
            "type": "scattergl", "mode": "markers", "name": name,
            "x": np.round(b["xy"][ii, 0].astype("float64"), 3).tolist(),
            "y": np.round(b["xy"][ii, 1].astype("float64"), 3).tolist(),
            "ids": [str(int(b["gidx"][i])) for i in ii],
            "marker": {"color": color, "size": size, "opacity": opacity},
        }
        if hover_on:
            t["text"] = [_hover(b, i, hov) for i in ii]
            t["hoverinfo"] = "text"
        else:
            t["hoverinfo"] = "skip"
        return t

    # Task 12: 배너에 현재 버전 필터 표기 (BANNER_RULE/BANNER_WAVE_NOCLICK/BANNER_COORDS_A는
    # `in` 검사로 selftest가 고정하므로 접미사 추가는 기존 assert를 깨지 않는다).
    vtxt = bank_version_filter or ALL_VERSIONS_LABEL
    sub = f"{BANNER_COORDS_A} · 버전: {vtxt}"
    if rule == "argmax_k1" and not show_unadopted:
        # 토글 피드백(사용자 피드백): 미채택 숨김 상태를 배너에도 굵게 명시 —
        # 버튼 라벨(render)과 이중으로 상태가 보이게 한다. (argmax 전용 — dist_iou에는
        # 미채택 개념이 없다, 아래 정정 참고.)
        sub += " · **표시: 채택만**"
    # 마커 시인성(사용자 피드백): App 테마 배경(mediaSpace, 다크)이 기본이라 작은/반투명
    # 점이 묻힌다 — 채택점은 크게 + 흰 테두리, 미채택은 한 단계 밝게. 팔레트 자체는 유지.
    if rule == "argmax_k1":
        banner = f"{BANNER_RULE} · {sub}"
        member = adopted            # 그룹 trace 대상 = 채택 (미채택은 회색 예비군 trace)
    else:  # dist_iou — 클릭 무효
        banner = f"{BANNER_WAVE_NOCLICK} · {sub}"
        # 정정(2026-08-10): wave(dist_iou)는 **모든 문장이 분포에 참여**한다 — wave_role/
        # wave_gain이 전 12,480행에 존재(실측: 유익 1,250·유해 1,250·중간 9,980, 미채택
        # 12,166행 전부 wave_gain≠0). K=1 승수 기준인 adopted 마스크로 12,166개를 회색
        # "미채택 예비군"으로 칠하던 것은 틀린 표현(유익 1,237·유해 1,247을 뭉갬).
        member = np.ones(len(idx_all), dtype=bool)

    # ── 색칠 축은 **규칙과 독립** (2026-08-12 사용자 요청: wave 화면에서도 카테고리 범례).
    #    기본값은 규칙별 기존 동작 그대로 유지 — topk=클래스, dist_iou=wave_role (회귀 방지).
    #    배너·클릭 귀속·member 마스크는 규칙의 성질이라 위에 남겨 두고 여기서 색만 고른다.
    cb = color_by or ("category" if rule == "argmax_k1" else "wave_role")

    def _groups(idx):
        """idx 에 대한 (그룹 배열, 팔레트). 모집단·표시분 양쪽에 같은 규칙을 적용한다."""
        if cb == COLOR_BY_CATEGORY and b.get("category") is not None:
            return np.asarray([str(b["category"][i]) for i in idx], dtype=object), CLASS_COLORS
        if cb == COLOR_BY_WAVE_ROLE and b.get("wave_role") is not None:
            return np.asarray([str(b["wave_role"][i]) for i in idx], dtype=object), WAVE_ROLE_COLORS
        if cb == COLOR_BY_FORM and b.get("form") is not None:
            return np.asarray([str(b["form"][i] or "other") for i in idx], dtype=object), FORM_COLORS
        if cb == COLOR_BY_RULE and b.get("rule_ok") is not None:
            return np.asarray([str(b["rule_ok"][i] or "미판정") for i in idx], dtype=object), RULE_COLORS
        return np.asarray(["전체"] * len(idx), dtype=object), {}   # 필드 부재 — 단색 1 trace

    groups_arr, palette = _groups(idx_all)
    # 모집단 그룹 카운트 — 범례에 "표시분/모집단" 을 같이 싣기 위해. 서브샘플이 없었으면
    # (= 같은 배열) 재계산하지 않는다 (603k object 배열 순회는 비싸다).
    if len(pop_idx) == len(idx_all):
        pop_groups = groups_arr
        pop_adopted = adopted
    else:
        pop_groups, _pal = _groups(pop_idx)
        pop_adopted = adopted_all[pop_idx]
    pop_count = dict(zip(*np.unique(pop_groups, return_counts=True)))

    def _legend(name, drawn, total):
        """범례 라벨. 표시분과 모집단이 다르면 **둘 다** 보여준다 (위 pop_idx 주석)."""
        return f"{name} {drawn:,}" if drawn == total else f"{name} {drawn:,}/{total:,}"

    data = []
    if rule == "argmax_k1":
        if show_unadopted:
            # size 6 = 네이티브 Embeddings(emb_viz) 패널의 점 크기와 동일 (라이브 실측 — 사용자
            # 요청: 두 화면의 점 크기 체감이 같아야 비교가 편하다). 미채택 구분은 회색+반투명으로.
            data.append(trace(~adopted, GREY, 6,
                              _legend("미채택", int((~adopted).sum()),
                                      int((~pop_adopted).sum())) + " (예비군)", 0.45))
        else:
            # 빈 trace(x=[]) 대신 visible=False: 배열 길이를 유지한 채 플래그만 뒤집는다.
            # (빈 배열 방식은 클라이언트 patch 딥머지에서 옛 점을 못 지우는 문제가 있었다 —
            # _refresh의 set_data 금지 주석. visible=False trace는 범례에서도 빠진다 — 실측.)
            t_hidden = trace(~adopted, GREY, 6,
                             _legend("미채택", int((~adopted).sum()),
                                     int((~pop_adopted).sum())) + " (숨김)", 0.45)
            t_hidden["visible"] = False
            data.append(t_hidden)
    # 그룹별 trace 1개 (docstring 참고 — 범례에 그룹별 색+개수가 나오고, 범례 클릭으로
    # 그룹 토글도 된다). 팔레트 순서 → 팔레트 밖 그룹(사전순) 순으로 안정 정렬, 빈 그룹은 생략.
    order = [grp for grp in palette if (member & (groups_arr == grp)).any()]
    order += sorted(set(groups_arr[member]) - set(palette))
    for grp in order:
        m = member & (groups_arr == grp)
        t = trace(m, palette.get(grp, "#999999"), 5,
                  _legend(grp, int(m.sum()), int(pop_count.get(grp, m.sum()))), 0.95)
        if rule == "argmax_k1":
            t["marker"] = {"color": palette.get(grp, "#999999"),
                           "size": [6 + min(10, int(b["wins"][i]) // 50) for i in idx_all[m]],
                           "opacity": 0.95, "line": {"width": 0.8, "color": "#FFFFFF"}}
        elif grp == "중간":
            # 다수(≈80%)인 중간은 배경으로 — 작고 연하게, 테두리 없음
            t["marker"] = {"color": "#999999", "size": 5, "opacity": 0.35}
        else:
            t["marker"] = {"color": palette.get(grp, "#999999"), "size": 7,
                           "opacity": 0.95, "line": {"width": 0.8, "color": "#FFFFFF"}}
        data.append(t)

    sel = [i for i in range(len(idx_all))
           if int(b["gidx"][idx_all[i]]) in (selected_gidx or set())]
    hi = idx_all[sel]
    # ⚠️ 하이라이트는 **후광 + 코어 2겹**이다 (2026-08-31 사용자 요청: "시인성이 높은 색으로").
    #    옛 스타일(Okabe-Ito 노랑 `#F0E442` 의 size 14 circle-open)은 60만 점이 깔린 밀집
    #    영역에서 링 안쪽으로 배경 점들이 그대로 보여 형태가 무너졌고, falldown(#E69F00)·
    #    fire(#D55E00) 같은 따뜻한 클래스 색과 명도가 붙어 눈에 안 걸렸다.
    #    후광(큰 반투명 원)이 밀집 배경을 눌러 위치를 만들고, 코어(작은 불투명 원 + 흰 테두리)가
    #    정확한 점을 찍는다. 색은 `HILITE`(마젠타) — 클래스 팔레트 5색과 교집합이 없다.
    #    ⚠️ 코어를 **마지막** trace 로 유지한다: 소비자와 회귀 테스트가 `data[-1]` 을
    #       "하이라이트(ids 보유)" 로 읽는다.
    # ⚠️ **trace 를 늘리지 않는다.** 후광을 별도 trace 로 얹으면 이 파일과 형제 패널의
    #    `data[:-1]`(= "하이라이트 빼고 전부") 가정이 20곳에서 깨진다. 같은 시인성을
    #    한 trace 로 낸다: 마젠타 코어 + **굵은 흰 링**.
    # ── 표↔플롯 연결: **번호** (2026-08-31 사용자 리포트: "어떤 위치에 있는지 알 수 없다")
    #    마젠타 점만으로는 어느 점이 어느 문장인지 알 수 없다. **같은 좌표(=같은 문장)는
    #    같은 번호**를 쓴다 — 승자 31개가 고유 문장 11종이라 31개를 다 찍으면 겹쳐서
    #    못 읽는다(실측). 아래 승자 표가 `#` 열로 같은 번호를 싣는다.
    _tr, hl_index = highlight_trace(b, hi)      # 모드 C 와 같은 헬퍼 (스타일·번호 규칙 공유)
    data.append(_tr)
    # ⚠️ 표시/전체를 배너에 **반드시** 싣는다 (2026-08-14): 기본 화면이 603,318 중 20,000
    #    (3.3%)만 그리는데 아무 표기가 없어, 분석자가 화면 비율을 모집단 비율로 오독했다.
    #    채택점은 전수 보존되고 미채택만 층화되므로 화면의 채택 비율은 모집단보다 크게
    #    부풀어 있다 — 그 왜곡 배수까지 숫자로 밝힌다. (형제 패널 image_embeddings 와 동일 계약.)
    # ⚠️ 하이라이트(후광+코어)는 **모집단이 아니다** — 표시 수에서 뺀다. 옛 코드는 전 trace 를
    #    합산해 선택이 있을 때 "표시" 가 선택 수만큼 부풀었다(후광 도입 후엔 2배로 부풀 뻔했다).
    shown_n = sum(len(t["x"]) for t in data
                  if t.get("visible") is not False and t.get("name") != "선택")
    if shown_n < pop_n:
        pct = shown_n / pop_n * 100
        pop_ad = int(pop_adopted.sum())
        drawn_ad = int(adopted.sum())
        bias = ((drawn_ad / max(shown_n, 1)) / (pop_ad / max(pop_n, 1))) if pop_ad else 0
        banner += (f" · **표시 {shown_n:,}/{pop_n:,} ({pct:.1f}%)** — 층화 서브샘플"
                   + (f", 채택 비율 {bias:.0f}배 과대" if bias >= 1.5 else ""))
    else:
        banner += f" · 표시 {shown_n:,}/{pop_n:,} (전량)"
    if not hover_on:
        # 호버가 꺼진 사실을 **반드시** 밝힌다 — 말없이 툴팁이 안 뜨면 고장으로 읽힌다.
        banner += f" · 호버 off ({shown_n:,} > {HOVER_BUDGET:,})"
    # 문장 출처를 **항상** 밝힌다 (2026-08-19): DB 정본이 몇 행이고 어느 버전이 폴백인지
    # 안 적으면, 자리표시자가 섞인 화면을 정본으로 오독한다 — 조용한 폴백 금지.
    banner += " · " + pdb_note(text_meta)
    if view_winner is not None:
        banner += (f" · **채택 = 현재 뷰 기준** (뷰가 뽑는 승자 문장 {len(view_winner):,}종)"
                   " — 전역 채택이 아니다")
    dropped_n = int(text_meta.get("dropped_n") or 0)
    if dropped_n:
        banner += (f" · ⛔ **정렬 붕괴 {len(bad_vers)}버전 {dropped_n:,}점 제외**"
                   f" ({', '.join(sorted(bad_vers))}) — 벡터 귀속이 틀려 기하 비교 불가")
    # banner 는 layout.title 이 아니라 **별도 키**다 (2026-08-12): plotly title 은 modebar
    # (우상단 아이콘 줄)와 같은 영역에 그려져 글자와 아이콘이 겹쳐 판독 불가였고, 자동 줄바꿈이
    # 없어 패널 폭을 넘으면 문장 중간에서 잘렸다. 패널이 이걸 md 로 렌더한다(render 참고).
    return {"data": data,
            "banner": banner,
            # 기계용 — banner 는 사람용 문장이라 테스트/호출자가 파싱하면 안 된다.
            # `hl_index` = {gidx: 화면 번호} — 아래 승자 표의 `#` 열이 같은 번호를 쓴다.
            "hl_index": hl_index,
            "dropped_n": dropped_n,
            "dropped_versions": sorted(bad_vers),
            "layout": {"showlegend": True, "dragmode": "pan",
                       "xaxis": {"visible": False}, "yaxis": {"visible": False},
                       # height 고정 금지 — PlotlyView는 style.height(=view의 height kwarg,
                       # 기본 "100%")를 따르므로 render()의 vh 기반 height가 실높이를 정한다
                       # (App 번들 실측: bo=Yn?.height||"100%"). autosize가 그 style을 추적.
                       "autosize": True,
                       # t: 60 → 30 — 제목이 빠진 만큼 회수 (modebar 여유만 남긴다)
                       "margin": {"l": 10, "r": 10, "t": 30, "b": 10}}}


# ── 모드 B (스펙 §5.1b, R5-b) — 같은 데이터셋 슬라이스를 하나의 emb_viz 좌표에 overlay ──
BANNER_COORDS_B = ("같은 좌표계(UMAP 공유 fit) — 그룹 간 공간 비교 유효 "
                    "(모드 A는 독립 fit이라 비교 금지, 모드 B는 비교 가능)")
OKABE_ITO_B = ["#0072B2", "#E69F00", "#009E73", "#D55E00",
               "#CC79A7", "#56B4E9", "#F0E442", "#000000"]


def highlight_trace(bundle, hi):
    """선택 하이라이트 trace + `{gidx: 화면 번호}`. **모드 A·C 공용** (스타일 이중화 금지).

    · 색 `HILITE`(마젠타) + 굵은 흰 링 — 클래스 팔레트와 무교집합, 밀집 배경에서도 걸린다
    · 마커 위 **번호**: 같은 좌표(=같은 문장)는 같은 번호. 31개를 다 찍으면 겹쳐 못 읽는다
    · 호버: `#번호 [버전] 문장` — 위치→문장 방향
    """
    import numpy as np
    b = bundle
    idxmap, keyed = {}, {}
    for i in hi:
        keyed.setdefault((round(float(b["xy"][i][0]), 2),
                          round(float(b["xy"][i][1]), 2)), []).append(i)
    for n, k in enumerate(sorted(keyed), start=1):
        for i in keyed[k]:
            idxmap[int(b["gidx"][i])] = n
    vers, txts = b.get("bank_version"), b.get("text")
    trace = {
        "type": "scattergl", "mode": "markers+text", "name": "선택",
        "x": np.round(b["xy"][hi, 0].astype("float64"), 3).tolist(),
        "y": np.round(b["xy"][hi, 1].astype("float64"), 3).tolist(),
        "ids": [str(int(b["gidx"][i])) for i in hi],
        "text": [str(idxmap.get(int(b["gidx"][i]), "")) for i in hi],
        "textposition": "top center",
        "textfont": {"color": HILITE, "size": 13, "family": "monospace"},
        "hovertext": [f"#{idxmap.get(int(b['gidx'][i]), '')} "
                      f"[{str(vers[i]) if vers is not None else '?'}] "
                      f"{str(txts[i])[:90] if txts is not None else ''}" for i in hi],
        "hoverinfo": "text",
        "marker": {"color": HILITE, "size": 13, "symbol": "circle",
                   "line": {"width": 4, "color": "#FFFFFF"}},
    }
    return trace, idxmap


CLUSTER_K_DEFAULT = 12
CLUSTER_EMB_MAX = 20_000      # 이 이하면 **1024-d 임베딩 공간**에서 군집, 넘으면 UMAP 좌표로
# ⚠️ 모드 C 전용 점 상한. 전량(607,315)을 그리면 figure 가 **13.85MB**(ids 5.03MB 포함)이고
#    플롯 이벤트마다 그게 되돌아와 왕복 ~69초다 — 오늘 이 규모로 Chrome 을 한 번 죽였다
#    (project_fiftyone_panel_data_channel_failed). 군집 구조는 층화 서브샘플로도 그대로
#    보이므로 200,000 으로 자르고, **ids 는 아예 싣지 않는다**(모드 C 는 조인 용도가 없다).
MODE_C_MAX_POINTS = 200_000
BANNER_COORDS_C = ("문장 군집 — 좌표는 emb_viz(UMAP) 그대로이고 **색만 군집 id** 다. "
                   "좌우 산점도는 독립 fit 이라 좌표 비교는 여전히 금물")


def build_mode_c(bundle, k=CLUSTER_K_DEFAULT, bank_version_filter=None, selected_gidx=None):
    """모드 C — 문장 군집. 좌표는 emb_viz 를 그대로 쓰고 **색만** 군집 id 로 칠한다.

    ⚠️ 군집 공간을 숨기지 않는다. 두 가지가 **다른 질문**이기 때문이다:
      · 1024-d 임베딩 공간 군집 = "의미가 비슷한 문장끼리"  ← 우리가 원하는 것
      · 2-d UMAP 좌표 군집      = "그림에서 뭉쳐 보이는 것"  ← UMAP 왜곡을 그대로 군집화
    그려지는 점이 `CLUSTER_EMB_MAX` 이하면 벡터를 DB 에서 끌어와 임베딩 공간에서 군집하고,
    넘으면 좌표 공간으로 내려간다 — **어느 쪽을 썼는지 배너가 밝힌다.**
    실측(2026-08-31): UMAP 2-d KMeans k=12 는 14,498점 0.09초 / 200,000점 0.28초,
    벡터 조달은 14,498개 ≈ 1초.
    """
    import numpy as np
    b = bundle
    idx = np.arange(len(b["xy"]))
    vers = b.get("bank_version")
    if bank_version_filter and bank_version_filter != ALL_VERSIONS_LABEL and vers is not None:
        keep = np.asarray([pdb_norm_ver(str(v)) == pdb_norm_ver(bank_version_filter)
                           for v in vers])
        idx = idx[keep]
    finite = np.isfinite(b["xy"][idx]).all(1)      # NaN 좌표(실측 3/607,318)는 그릴 수 없다
    idx = idx[finite]
    if len(idx) == 0:
        return {"data": [], "banner": f"{BANNER_COORDS_C} · 그릴 점이 없습니다",
                "layout": {"xaxis": {"visible": False}, "yaxis": {"visible": False}}}
    if len(idx) > MODE_C_MAX_POINTS:
        idx = idx[stratified_subsample(np.zeros(len(idx)), MODE_C_MAX_POINTS)]

    X, space = None, "UMAP 좌표"
    if len(idx) <= CLUSTER_EMB_MAX and vers is not None:
        by_ver = {}
        for i in idx:
            by_ver.setdefault(str(vers[i]), []).append(int(b["gidx"][i]))
        vecs, miss = {}, 0
        for ver, gs in by_ver.items():
            got = pdb_fetch_vectors(ver, [pdb_local_gidx(g) for g in gs])
            for g in gs:
                v = got.get(pdb_local_gidx(g))
                if v is None:
                    miss += 1
                else:
                    vecs[(ver, g)] = v
        if len(vecs) >= 0.9 * len(idx):            # 10% 넘게 비면 좌표 공간으로 내려간다
            X = np.asarray([vecs.get((str(vers[i]), int(b["gidx"][i])),
                                     [0.0] * 1024) for i in idx], dtype="float32")
            space = f"임베딩 1024-d (벡터 미보유 {miss:,}개는 원점 처리)"
    if X is None:
        X = b["xy"][idx]
    from sklearn.cluster import MiniBatchKMeans
    k = max(2, min(int(k or CLUSTER_K_DEFAULT), 24, len(idx)))
    lab = MiniBatchKMeans(n_clusters=k, n_init=3, random_state=0).fit_predict(X)

    data, xy = [], b["xy"][idx]
    for cid in range(k):
        m = lab == cid
        if not m.any():
            continue
        data.append({
            "type": "scattergl", "mode": "markers", "name": f"군집 {cid} ({int(m.sum()):,})",
            "x": np.round(xy[m, 0].astype("float64"), 3).tolist(),
            "y": np.round(xy[m, 1].astype("float64"), 3).tolist(),
            # ids 없음 — 클릭 조인을 안 하므로 5MB 를 실을 이유가 없다 (상수 주석 참고)
            "marker": {"size": 5, "opacity": 0.85},
            "hoverinfo": "skip",     # 60만 점 호버 텍스트는 요청을 MB 로 부풀린다
        })
    # ⚠️ 선택 하이라이트는 **서브샘플과 무관하게** 그린다 (2026-08-31 사용자 리포트:
    #    "이미지 패널에서 하나 선택했는데 C-군집에서 표시가 안 된다"). 200,000 상한에
    #    걸려 탈락한 점도 선택되었으면 반드시 보여야 한다 — 안 그러면 "선택이 안 먹었다"로
    #    읽힌다. 모드 A 와 **같은 헬퍼**를 써 스타일·번호 규칙이 갈리지 않게 한다.
    hl_index = {}
    sel_g = {int(g) for g in (selected_gidx or set())}
    if sel_g:
        hi = np.asarray([i for i in range(len(b["gidx"]))
                         if int(b["gidx"][i]) in sel_g
                         and np.isfinite(b["xy"][i]).all()], dtype=np.int64)
        if len(hi):
            tr, hl_index = highlight_trace(b, hi)
            data.append(tr)
    banner = (f"**군집 {k}개** · 군집 공간: **{space}** · 표시 {len(idx):,}/{len(b['xy']):,}장"
              + (f" · **선택 {len(hl_index):,}점 강조**" if hl_index else "")
              + f" · 탐색·중복 발견용 · {BANNER_COORDS_C}")
    return {"data": data, "banner": banner, "hl_index": hl_index,
            "layout": {"showlegend": True, "dragmode": "pan", "autosize": True,
                       "uirevision": "modeC",
                       "xaxis": {"visible": False}, "yaxis": {"visible": False},
                       "margin": {"l": 10, "r": 10, "t": 30, "b": 10}}}


def build_mode_b(ds_name, group_field, groups, brain_key=BRAIN_KEY):
    """같은 데이터셋의 그룹 슬라이스들을 하나의 emb_viz 좌표 위에 overlay.

    `frames`(구 frames_captions, project 22개)이 본래 타깃 — 그룹당 1 trace, 같은 UMAP fit을 공유하므로
    좌표 공간 비교가 정당하다 (스펙 §5.1b, 모드 A와 달리). 그룹 필드는 문자열/Classification
    모두 허용(카테고리 값이면 .label로 평탄화). Task 6 stratified_subsample로 그룹당
    MAX_POINTS/n 서브샘플 — 네이티브 Embeddings 패널의 5,000점 상한 우회.
    """
    import numpy as np
    ds = fo.load_dataset(ds_name)

    # 크래시 가드 (2026-08-10 실사용 오류): 기본 group_field="project"는 `frames`
    # 용이라 sourcei 등 다른 데이터셋엔 없다 — 무방비 ds.values()가 ValueError로 패널을
    # 죽였다. 조인 필드 부재와 같은 규약: 크래시 대신 안내 배너만 그린다.
    def _notice(text):
        return {"data": [], "banner": f"{text} · {BANNER_COORDS_B}",
                "layout": {"xaxis": {"visible": False}, "yaxis": {"visible": False}}}

    try:
        field_missing = ds.get_field(group_field) is None
    except Exception:
        field_missing = True
    if field_missing:
        return _notice(f"이 데이터셋에는 그룹 필드 '{group_field}'가 없습니다")
    if brain_key not in ds.list_brain_runs():
        return _notice(f"이 데이터셋에는 brain run '{brain_key}'가 없습니다 — 좌표 없음")

    xy = np.asarray(ds.load_brain_results(brain_key).points, dtype="float32")
    labels = ds.values(group_field)
    if labels and hasattr(labels[0], "label"):
        labels = [v.label if v else None for v in labels]
    labels = np.asarray(labels, dtype=object)
    data = []
    per_group_cap = max(1, MAX_POINTS // max(1, len(groups)))
    for gi, grp in enumerate(groups):
        ii = np.where(labels == grp)[0]
        if len(ii) > per_group_cap:
            ii = ii[np.asarray(stratified_subsample([grp] * len(ii), per_group_cap, seed=gi))]
        data.append({
            "type": "scattergl", "mode": "markers",
            "name": f"{grp} ({len(ii)})",
            "x": xy[ii, 0].tolist(), "y": xy[ii, 1].tolist(),
            "marker": {"size": 6, "opacity": 0.75,
                       "line": {"width": 0.5, "color": "#FFFFFF"},
                       "color": OKABE_ITO_B[gi % len(OKABE_ITO_B)]},
        })
    return {"data": data, "banner": BANNER_COORDS_B,
            "layout": {"showlegend": True,
                       "xaxis": {"visible": False}, "yaxis": {"visible": False},
                       "margin": {"l": 10, "r": 10, "t": 30, "b": 10}}}


# App 내장 ShowSamples 오퍼레이터가 자기 Select stage 에 박는 고정 uuid.
# (번들 실측: const SHOW_SAMPLES_STAGE_ID="show_samples_stage_id"; execute 는
#  view.filter(s => s._uuid !== SHOW_SAMPLES_STAGE_ID) 로 직전 것을 지운 뒤 새로 붙인다)
SHOW_SAMPLES_STAGE_ID = "show_samples_stage_id"


def _client_view_stages(ctx):
    """App 이 EXEC 페이로드에 실어 보낸 **뷰 바 원본 스테이지 dict 목록**.

    ⚠️ `ctx.view`(DatasetView)와 다르다 — ctx.view 는 request_params 의 filters/extended
    (사이드바 필터·확장 선택)까지 스테이지로 구워 넣으므로, 그걸 되돌려 set_view 하면
    사이드바 필터가 뷰 바 칩으로 승격돼 버린다. 여기 원본 dict 에는 App 이 붙인 `_uuid` 가
    살아 있어 우리 스테이지만 정확히 골라낼 수 있다 (ExecutionContext.view 소스 실측).
    """
    rp = getattr(ctx, "request_params", None) or {}
    return [s for s in (rp.get("view") or []) if isinstance(s, dict)]


def _has_our_stage(ctx):
    """뷰 바에 우리가 건 Select 스테이지가 남아 있는가 (패널 상태와 무관)."""
    return any(s.get("_uuid") == SHOW_SAMPLES_STAGE_ID for s in _client_view_stages(ctx))


def _view_sig(ctx):
    """현재 **뷰 바 스테이지 + 사이드바 필터 + extended** 의 지문 (에코 판별용).

    `_client_view_stages` 와 달리 우리 Select 스테이지도 포함한다 — "뷰가 바뀌었나"를
    묻는 질문이라 출처를 가릴 이유가 없다.
    """
    rp = getattr(ctx, "request_params", None) or {}
    raw = json.dumps([rp.get("view") or [], rp.get("filters") or {},
                      rp.get("extended") or {}], sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _dedup_guard(ctx, state_key, ids):
    """재발화 가드 (공용 헬퍼). 반환: 처리해야 하면 False, 중복(스킵)이면 True.

    시그니처는 **서버 기억**(`_mem`)에 둔다 (2026-09-09). 예전엔 패널 상태에 뒀는데
    그 경로는 두 가지 함정이 있었다:
      ① 선택이 바뀔 때마다 상태 쓰기가 발생 → App 재조회 → 사용자의 사이드바 필터 소멸.
      ② 배포본 panel.py 의 `PanelRefBase.__setattr__` 는 `_` 로 시작하는 키를 `.set()`
         우회 경로로 처리해 실제 왕복 dict 에 안 실렸다 — 밑줄 키를 쓰면 매 호출 리셋돼
         진짜 "전체 해제"(ids=[]) 전이까지 영구히 삼켰다(Task 8 fix round).
    서버 기억에는 두 함정이 다 없다.
    """
    sig = sorted(ids)
    with _MEM_LOCK:                     # check-then-act — 동시 훅이 같은 키를 본다 (F6)
        st = _mem(ctx)
        if sig == (st.get(state_key) or []):
            return True
        st.set(state_key, sig)
    return False


_MISSING = object()
_APPLIED = {}   # (panel_id, control) -> 마지막 반영 값. 프로세스 생존 동안만.

# ── 서버측 패널 기억 (2026-09-09) ────────────────────────────────────────────────
# ⚠️ **패널 상태에 "값이 바뀌는" 쓰기를 하면 사용자의 사이드바 필터가 지워진다.**
#    전 구간 확인(소스 + 헤드리스 실측, 2026-09-09):
#      ctx.panel.state.X = v → ops.patch_panel_state (`operators/panel.py:262`, 더티체크 없음)
#      → App `panelsStateAtom` → MainSpace 의 deep-equal 통과 시 `setSessionSpaces`(debounce 500ms)
#      → `router.replace(..., {event:"spaces"})` → DatasetPageQuery **network-only** 재조회
#      → 서버 `serialize_dataset` 이 조회마다 **새 ObjectId** 를 발급(`server/query.py:645`)
#      → 클라이언트 전용 `filters` 아톰이 `new.id !== prev.id` 로 스스로 비워진다.
#    실측: 사이드바 `ground_truth.label=fire` 로 155장 → 색칠 드롭다운 변경 → **2초 뒤 6,032장**.
#    사용자 리포트(2026-09-09): "이미지를 선택했을 때는 잘 나왔는데 시간이 지나자 label 선택이
#    사라지며 초기 상태로" — 그 '시간'이 위 debounce+재조회다.
#
# ⇒ 규칙: **값이 바뀌는 상태 쓰기 = 사용자 필터 파괴.** 그래서 이 패널의 상태는 전부 여기
#   서버측에 둔다. 클라이언트로 내보내는 것은 컨트롤 미러(`controls`) 하나뿐이고, 그것도
#   **클라이언트가 낡았을 때만** 민다 (`_sync_controls`).
#   같은 이유로 `_reconcile`(낡은 클라이언트 에코를 서버 기억에 맞추던 보정)은 필요 없어져
#   삭제했다 — 이제 상태가 애초에 클라이언트를 왕복하지 않는다.
#
# 키 = (패널, 데이터셋). 같은 데이터셋을 연 탭들은 기억을 공유하는데 이는 **오늘과 같은
# 의미**다 (패널 상태는 세션 spaces 에 실려 이미 탭 간 공유였다).
# 콜드 키는 **빈 상태로** 시작한다 — on_load 가 기본값을 세운다. 요청이 실어 온 패널 상태를
# 시드로 쓰지 않는 이유는 아래 `_mem` 안의 F1 주석 참고(옛 배포가 남긴 최상위 키가 다른
# 데이터셋에서 되살아난다). 대가: 플러그인 프로세스가 재시작하면 컨트롤은 기본값에서 시작한다.
_MEM = OrderedDict()          # (panel, dataset) -> dict
_MEM_LOCK = threading.RLock()
PANEL_NAME = "user_prompt_compare"


class _Mem:
    """`ctx.panel.state` 와 **같은 문법**의 서버측 상태 (patch 를 내보내지 않는다).

    ⚠️ 잠금은 엔트리 생성·축출에만 건다. 값 쓰기는 dict 대입이라 원자적이지만, **동시 요청이
    서로 다른 키를 섞어 쓸 수는 있다**(예: 새 선택의 표 + 옛 배너). 오퍼레이터는 스레드풀에서
    동시 실행된다(`executor.py`). 값은 작고 다음 refresh 가 수렴시키므로 수용한다 —
    figure 처럼 **함께 봐야 하는 값**은 `_put_fig` 의 한 엔트리 규칙을 계속 쓴다.
    """

    def __init__(self, d):
        self.__dict__["_d"] = d

    def __getattr__(self, key):
        if key.startswith("_"):
            raise AttributeError(key)      # 오타 은폐 방지 (실제 PanelState 와 같은 계약)
        return self.__dict__["_d"].get(key)

    def __setattr__(self, key, value):
        self.__dict__["_d"][key] = value

    def get(self, key, default=None):
        return self.__dict__["_d"].get(key, default)

    def set(self, key, value=None):
        self.__dict__["_d"][key] = value

    def snapshot(self):
        """지금 이 순간의 사본. **render 는 이걸로 읽는다** — 여러 값을 따로 읽는 사이
        동시 요청의 `_refresh` 가 키를 하나씩 갈아끼우면 "새 표 + 옛 배너" 가 섞인다
        (`_put_fig` 가 한 엔트리로 합쳐 막은 것과 같은 부류의 사고, 코덱스 리뷰 F6)."""
        with _MEM_LOCK:
            return _Mem(dict(self.__dict__["_d"]))




# ── 컨트롤 UI 규약 (2026-09-09) ────────────────────────────────────────────────
# 두 패널이 **같은 규칙**을 쓴다 (섞이면 같은 화면에서 컨트롤마다 조작법이 달라 보인다):
#   · 값 컨트롤은 언제나 **굵은 라벨 한 줄 + 컨트롤** — 옛 드롭다운의 라벨 위치를 유지한다.
#   · `_fits_inline` 이 참이면 버튼을 그 자리에 편다(클릭 1번). 아니면 `현재값 ▾` 토글만
#     두고 목록은 컨트롤 행 **바로 아래**에 격자로 펼친다.
#   · 현재 값은 `contained`, 나머지는 `outlined`. 격자 열 수는 `PICKER_COLUMNS` 로 통일.
#   · 동작 버튼(선택 해제 등)은 라벨 줄 없이 — 값 컨트롤과 시각적으로 구분된다.
#   · 이름 규칙: 컨트롤 v_stack = `<name>`, 토글 = `<name>_toggle`,
#     펼친 격자 = `<name>_picker`, 옵션 버튼 = `pick_<name>__<value>`.
#   ⚠️ enum/드롭다운으로 되돌리지 말 것 — 값을 고르는 순간 App 이 패널 상태를 써서
#      사용자의 사이드바 필터가 지워진다 (`_mem` 주석의 체인).
INLINE_MAX = 4
# 컨트롤 셀은 패널 폭의 1/3 남짓이라 **라벨 길이 합**이 넘치면 버튼 안에서 글자 단위로
# 줄바꿈된다 (2026-09-09 실측: 색칠 4개 = "클래스/wave 역할/문장 형태/규칙 준수" 20자가
# "클래 스" 처럼 쪼개졌다. 규칙 2개 = "topk/dist_iou" 12자는 멀쩡). 그래서 개수만 보지 않는다.
INLINE_BUDGET = 14
PICKER_COLUMNS = 4


def _fits_inline(options):
    """옵션을 컨트롤 셀 안에 그대로 펼 수 있는가 (개수 + 라벨 길이 합)."""
    return (len(options) <= INLINE_MAX
            and sum(len(str(lab)) for _v, lab in options) <= INLINE_BUDGET)


def _control_box(parent, name, label):
    """굵은 라벨 한 줄 + 내용이 들어갈 세로 상자 (모든 값 컨트롤의 공통 껍데기)."""
    box = parent.v_stack(name, gap=0)
    box.md(f"**{label}**", name=f"{name}__label")
    return box


def _btn_choice(parent, name, label, options, current, handler):
    """옵션이 적을 때(≤ INLINE_MAX): 라벨 + 그 자리에 버튼들. 클릭 한 번으로 값이 바뀐다."""
    grp = _control_box(parent, name, label).btn_group(f"{name}__btns")
    for value, lab in options:
        grp.btn(f"pick_{name}__{value}", label=lab, params={"value": value},
                variant="contained" if value == current else "outlined",
                on_click=handler)


def _control_toggle(parent, name, label, current_text, handler):
    """옵션이 많을 때: 라벨 + `현재값 ▾` 토글 하나. 목록은 `_picker_grid` 가 아래에 편다."""
    _control_box(parent, name, label).btn(
        f"{name}_toggle", label=f"{current_text} ▾", variant="outlined",
        on_click=handler)


def _picker_grid(panel, name, options, current, handler,
                 columns=PICKER_COLUMNS, note=None):
    """펼친 옵션 격자. `current` 는 값 하나 또는 값 집합(다중 선택).

    헤더 문구는 두지 않는다 — 어떤 컨트롤의 목록인지는 바로 위 토글이 이미 말한다.
    """
    chosen = current if isinstance(current, (set, frozenset)) else {current}
    if note:
        panel.md(note, name=f"{name}__note")
    # 적은 개수는 좌측으로 묶고(btn_group), 많으면 격자로 편다. h_stack 은 칸을 전폭으로
    # 균등 분할해서, 옵션이 서넛일 때 버튼이 화면 양끝으로 흩어져 보인다(2026-09-09 실측).
    grid = (panel.btn_group(f"{name}_picker") if len(options) <= columns
            else panel.h_stack(f"{name}_picker", gap=1, columns=columns))
    for value, lab in options:
        grid.btn(f"pick_{name}__{value}", label=lab, params={"value": value},
                 variant="contained" if value in chosen else "outlined",
                 on_click=handler)


def group_field_candidates(ctx, cap=24):
    """모드 B 그룹 필드 후보 — 최상위 StringField + `<label>.label` 경로.

    자유 입력을 버튼 피커로 바꾸면서 필요해졌다(오탈자로 "그룹 필드가 없습니다" 를 보던
    경로가 사라진다). 스키마만 읽으므로 비용은 무시할 만하다.
    """
    ds = getattr(ctx, "dataset", None)
    if ds is None:
        return []
    import fiftyone.core.fields as fof
    import fiftyone.core.labels as fol
    out = []
    for name, field in ds.get_field_schema().items():
        if isinstance(field, fof.StringField):
            out.append(name)
        elif isinstance(field, fof.EmbeddedDocumentField) \
                and issubclass(field.document_type, fol.Label):
            out.append(f"{name}.label")
    return out[:cap]


def group_value_candidates(ctx, field, cap=40):
    """그 필드의 값 후보(빈도 내림차순)와 절단 여부. **피커가 열렸을 때만** 부른다.

    `count_values` 한 번이라 200k 샘플에서도 초 단위지만, 매 렌더마다 돌 이유는 없다 —
    호출부가 `picker == "groups"` 로 게이트한다.
    """
    ds = getattr(ctx, "dataset", None)
    if ds is None:
        return [], False
    try:
        counts = ds.count_values(field)
    except Exception:       # noqa: BLE001 — 없는 필드/집계 실패가 패널을 죽이면 안 된다
        return [], False
    vals = [str(v) for v, _n in sorted(counts.items(), key=lambda kv: -kv[1])
            if v is not None]
    return vals[:cap], len(vals) > cap




def _mem(ctx):
    ds = getattr(ctx, "dataset", None)
    key = (PANEL_NAME, ds.name if ds is not None else "-")
    with _MEM_LOCK:
        d = _MEM.get(key)
        if d is None:
            # ⚠️ **요청이 실어 온 패널 상태로 시드하지 않는다** (2026-09-09 코덱스 리뷰 F1).
            #    클라이언트 dict 에는 이 변경 이전 배포가 심어 둔 최상위 키
            #    (mode/bank_version_filter/selected_gidx/scatter_data…)가 세션이 살아 있는 한
            #    계속 들어 있다. 그걸 시드로 쓰면 데이터셋을 바꿔도 `mode` 가 이미 있어
            #    on_load 리셋이 안 돌고, 옛 데이터셋의 버전 필터가 새 데이터셋에 남는다
            #    (메모리 §16 계약 파괴 + `_forget_applied` 도 건너뛴다).
            #    콜드는 콜드로 시작한다 — on_load 가 기본값을 세운다.
            d = {}
            _MEM[key] = d
            # 상한 128: (패널 3종 × 데이터셋 수) 를 넉넉히 덮는다. 세션 중 축출이 일어나면
            # 그 패널이 콜드로 보여 컨트롤이 기본값으로 되돌아가므로 여유를 크게 잡는다.
            while len(_MEM) > 128:
                _MEM.popitem(last=False)
        _MEM.move_to_end(key)
    return _Mem(d)


# 산점도 trace 리스트의 서버측 보관소 (2026-08-14). ctx.panel.state 에 실으면 이후 **모든**
# 훅 요청의 panel_state 와 spaces 트리(패널 상태 복제본)에 2MB×2 로 왕복하는데, 서버가
# 요청 바디 1MB 당 ~2.5초를 태운다(4MB 요청 = 10초, curl 실측) — 드롭다운 한 번에 5~20초
# 걸리던 최종 병목이 이것이었다. 여기 두면 요청/응답 state 는 KB 단위로 준다.
# 키: 실 요청 = panel_id (여러 클라이언트가 같은 워크스페이스 패널을 열면 공유되지만,
# OSS App 은 세션 자체가 전 클라이언트 공유라 패널 상태도 어차피 수렴한다). 오프라인
# (selftest) = id(ctx.panel) 폴백 — fake ctx 별로 유일해 크로스토크가 없다.
_FIGS = OrderedDict()      # key -> {"data", "banner", "layout"} — **한 엔트리**
_FIGS_LOCK = threading.Lock()


def data_version(dataset):
    """데이터 버전 문자열 — 캐시 무효화 신호. **샘플 최대 `last_modified_at`** 을 쓴다.

    ⚠️ `dataset.last_modified_at` 은 **샘플 편집으로 갱신되지 않는다** (실측 2026-09-01:
    GT 65장을 바꿨는데도 `sourcei.last_modified_at` 은 2026-08-28 에 멈춰 있었고, 샘플
    최대값만 09-01 로 움직였다). 그걸 캐시 키로 쓰면 **GT·라벨을 바꿔도 패널이 옛 번들과
    옛 figure 를 계속 내준다** — 사용자 리포트: "image embedding 에서는 적용이 안 된 것
    같은데 그대로 fire 234개야".
    ⚠️ `bounds("last_modified_at")` 금지: 집계라 인덱스를 못 타고 `frames`(199,972)에서
    **1.1~7.0초**다. `sort+limit 1` 은 인덱스를 타서 전 데이터셋 **0.001~0.010초**.
    
    ⚠️ **표본 수도 같이 넣는다.** 삭제는 *남은* 샘플의 `last_modified_at` 을 건드리지
    않는다 — 실측 2026-09-02: `sourcei` 를 6,733 → 6,353 으로 380장 지운 뒤에도 최대값은
    직전 라운드의 05:38:56 에 그대로 멈춰 있었다. 시각만 키로 쓰면 **삭제가 캐시를 무효화
    하지 못해 패널이 지워진 점을 계속 그린다** (`keep = order >= 0` 필터는 번들을 새로
    만들 때만 도는 방어라 캐시 히트에는 개입하지 못한다).
    """
    try:
        n = dataset.count()
    except Exception:      # noqa: BLE001 — 카운트 실패로 패널을 죽이지 않는다
        n = -1
    try:
        v = (dataset.sort_by("last_modified_at", reverse=True)
                    .limit(1).values("last_modified_at"))
        if v and v[0] is not None:
            return f"{v[0]}#{n}"
    except Exception:      # noqa: BLE001 — 버전 조회 실패가 패널을 죽이면 안 된다
        pass
    return f"{getattr(dataset, 'last_modified_at', '')}#{n}"


def _fig_key(ctx):
    """fig 캐시 키 = (데이터셋, 패널 인스턴스).

    ⚠️ **데이터셋을 키에 넣어야 한다** (2026-08-20, 다른 세션 실측으로 확인): 예전 키는
    패널 인스턴스만 봤다 → 헤더 선택기로 데이터셋을 바꾸면 `render` 가 **옛 데이터셋의
    figure 를 그대로 꺼내 그렸다.** 실제 피해: `frames` 의 199,972점을 문장 패널로 오인해
    검증이 한 번 헛돌았다. 데이터셋이 키에 있으면 전환 직후엔 캐시 미스가 되고, `render`
    의 결정론 재구성 폴백이 **새 데이터셋 기준으로** 다시 만든다(웜 ~0.1s).
    (App 은 URL 로 데이터셋이 안 붙고 접속 시 서버 세션에 스스로 동기화하므로, 전환은
     항상 이 경로를 탄다 — 옆 패널 `user-image-embeddings._fig_key` 도 같은 계약.)
    인스턴스 성분은 panel_id, 없으면(render 경로) `id(ctx.panel)` 폴백.
    """
    ds = getattr(ctx, "dataset", None)
    inst = (getattr(ctx, "params", None) or {}).get("panel_id") or id(ctx.panel)
    # ⚠️ 데이터 버전도 키에 넣는다 — 없으면 라벨을 바꿔도 옛 figure 가 계속 나온다
    #    (`data_version` docstring 의 실측 참고).
    # ⚠️ **뷰 지문은 모드 A 에만 넣는다** (2026-09-04). 모드 A 는 `view_winner_gidx(ctx)` 로
    #    뷰에 의존하므로 뷰가 바뀌면 캐시가 미스여야 render 가 새 뷰로 다시 그린다.
    #    ⛔ 모드 B/C 에 넣으면 **빈 산점도**가 된다: 둘의 figure 는 뷰를 인자로 받지도 않는데
    #    (`build_mode_b(ds_name, group_field, groups)` / `build_mode_c(bundle, k, ...)`),
    #    render 의 재구성 폴백은 `mode not in ("B","C")` 로 막혀 있어서 미스를 메울 수가 없다
    #    → `data=fig_data or []` 로 플롯이 통째로 비워진다. 뷰 무관이니 히트가 정답이다.
    #    (codex 리뷰 지적, 2026-09-07.)
    # ⚠️ **모드를 키에 넣는다** (codex 리뷰, 2026-09-07): 예전엔 B 와 C 가 똑같이 "-" 를 써서
    #    한 키를 공유했다 — 순차 전환은 `on_mode_change` 가 덮어써서 안 드러나지만, 동시 요청
    #    에서는 B 의 렌더가 그 사이 쓰인 C 의 figure 를 집어간다.
    mode = _mem(ctx).mode or "A"      # 미설정 = 기본 모드 A (render 기본값과 일치)
    view_part = _view_sig(ctx) if mode == "A" else "-"
    return (ds.name if ds is not None else "-", inst,
            data_version(ds) if ds is not None else "", mode, view_part)


def _put_fig(ctx, data, banner=None, layout=None):
    """figure + 배너 + 레이아웃을 **한 엔트리**로 보관 (2026-09-04, codex 리뷰 반영 09-07).

    ⚠️ 셋을 두 dict 로 쪼개면 안 된다: 오퍼레이터는 스레드풀에서 **동시 실행**되므로
    (`executor.py` 의 `loop.run_in_executor`) 요청 A 가 data 를 쓴 뒤 멈추고 B 가 data+meta 를
    쓰고 A 가 meta 를 쓰면 **B 의 그림 + A 의 배너**가 남는다. 독립 축출 루프도 서로 다른 키를
    지울 수 있다. 한 엔트리 + 한 번의 조회면 그 조합 자체가 불가능하다.
    ⚠️ 배너를 패널 상태에만 두면 안 되는 이유: 같은 뷰의 두 번째 렌더는 캐시 **히트**라
    재구성을 건너뛰는데, 그때 `ctx.panel.state.banner` 로 폴백하면 필터 전 모집단
    ("표시 373/373장")이 되살아난다 — 게다가 그 상태는 **공유 세션이라 남의 것일 수도** 있다.
    """
    key = _fig_key(ctx)
    with _FIGS_LOCK:
        _FIGS[key] = {"data": data, "banner": banner, "layout": layout}
        _FIGS.move_to_end(key)
        # 키에 뷰 지문이 들어가 카디널리티가 늘었다 — 상한도 같이 올린다(8 → 16).
        while len(_FIGS) > 16:
            _FIGS.popitem(last=False)


def _get_entry(ctx):
    """이 뷰의 (data, banner, layout) 한 벌. 없으면 None. **조회는 한 번만** 한다.

    LRU: 히트하면 맨 뒤로 보내 **지금 보고 있는 엔트리가 축출되지 않게** 한다. FIFO 였을 때는
    활성 엔트리가 계속 가장 오래된 것으로 남아, 다른 클라이언트가 16개만 만들면 쫓겨났다
    (모드 B/C 는 render 재구성 폴백이 막혀 있어 그 순간 산점도가 빈 채로 굳는다).
    """
    key = _fig_key(ctx)
    with _FIGS_LOCK:
        e = _FIGS.get(key)
        if e is not None:
            _FIGS.move_to_end(key)
        return e


def _get_fig(ctx):
    e = _get_entry(ctx)
    return e["data"] if e is not None else None


def _change_guard(ctx, control, value, carried_same):
    """드롭다운 변경 dedup. 반환: 스킵(no-op)이면 True.

    2026-08-14 실측(fetch 인터셉트 타임라인)으로 확정한 두 가지 클라이언트 동작 때문에
    **서버가 마지막으로 반영한 값(_APPLIED)** 기준으로 판정해야 한다:
    ① 드롭다운 한 번에 같은 on_*_change 가 135ms 간격 2발 — 둘 다 변경 전 panel_state
       를 실어 오므로 요청 상태와의 값 비교로는 두 번째를 못 잡는다.
    ② 요청에 실려 오는 panel_state 는 응답 1왕복만큼 낡는다 — 앞 변경이 처리되는 동안
       사용자가 되돌리는 클릭(dist→topk)을 하면 carried state 기준으론 '값 그대로'로
       보여 **진짜 변경이 삼켜진다** (드롭다운은 topk 인데 화면은 dist 로 굳는 버그).
    _APPLIED 에 기록이 없으면(프로세스 재시작 직후 등) carried_same(요청 상태 기준
    등가 여부)으로 폴백한다. panel_id 없는 호출(selftest·오프라인)은 항상 폴백 경로 —
    실 클라이언트 요청만 panel_id 를 싣는다(요청 바디 실측).
    ⚠️ 이 가드가 실효하려면 플러그인 모듈이 요청 간 생존해야 한다 —
       FIFTYONE_PLUGINS_CACHE_ENABLED=true 필수 (기본 false 는 요청마다 재임포트라
       모듈 전역이 증발하고, 603k 번들 _CACHE 도 매 요청 재로드돼 왕복이 20초를 넘긴다.
       fiftyone_relaunch.py 가 세팅한다).
    """
    pid = (getattr(ctx, "params", None) or {}).get("panel_id")
    if pid is None:
        return carried_same
    key = (pid, control)
    prev = _APPLIED.get(key, _MISSING)
    _APPLIED[key] = value
    if prev is _MISSING:
        # ⚠️ 첫 관측은 **무조건 처리** (2026-08-14 실측, image_embeddings 패널에서 재현):
        #    클라이언트는 드롭다운 값을 낙관적으로 먼저 바꿔 그 값을 panel_state 에 담아
        #    보내므로 carried_same 이 이미 True 다. 서버 기억이 빈 상태(프로세스 재기동 직후
        #    첫 클릭)에서 이를 믿으면 **진짜 변경이 삼켜진다** — 드롭다운만 새 값이고 플롯·
        #    배너는 옛 값으로 남는다. 같은 값 에코가 한 번 더 도는 비용은 결과가 같아 무해.
        return False
    return prev == value


def _forget_applied(ctx):
    """`_APPLIED`(서버가 마지막으로 반영한 값)를 비운다 — **상태 리셋과 짝으로만** 부른다.

    ⚠️ 리셋과 가드는 함께 움직여야 한다. `on_load` 가 상태를 기본값으로 되돌렸는데
    `_APPLIED` 에 옛 선택("채택만")이 남아 있으면, 사용자가 **같은 값을 다시 고를 때**
    `_change_guard` 가 "같은 값" 이라며 삼킨다 → 서버 상태는 "전체" 인데 클라이언트는
    낙관적으로 "채택만" 을 표시하고, 다음 `_refresh` 의 `_sync_controls` 가 "전체" 를
    되밀어 **전체 ↔ 채택만 왕복**으로 보인다 (2026-08-20 사용자 신고의 정체).
    """
    pid = (getattr(ctx, "params", None) or {}).get("panel_id")
    if pid is None:
        _APPLIED.clear()      # pid 를 모르면 전부 — 같은 값 에코 1회는 결과가 같아 무해
        return
    for key in [k for k in _APPLIED if k[0] == pid]:
        _APPLIED.pop(key, None)


_TRACE_FLAG = "/tmp/user_compare_trace.on"     # 이 파일이 있을 때만 기록
_TRACE_PATH = "/tmp/user_compare_trace.jsonl"


def _trace(ctx, event, **extra):
    """훅 호출 타임라인 기록 — **플래그 파일이 있을 때만**. 없으면 stat 한 번이 전부.

    왜 파일 게이트인가: App 프로세스는 이미 돌고 있어 env 를 바꿀 수 없다. 플래그 파일이면
    재기동 없이 켜고 끈다(`touch /tmp/user_compare_trace.on` / `rm`).
    용도: 패널이 반복 리마운트(churn)하며 `표시` 가 전체↔채택만 왕복으로 보이는 현상의
    **트리거 훅을 특정**하기 위한 일회성 계측 (2026-08-20). 상시 켜 두지 말 것.
    """
    try:
        if not os.path.exists(_TRACE_FLAG):
            return
        st = getattr(getattr(ctx, "panel", None), "state", None)
        rec = {"t": round(time.time(), 3), "ev": event,
               "pid": (getattr(ctx, "params", None) or {}).get("panel_id"),
               "mode": getattr(st, "mode", None),
               "show": getattr(st, "show_unadopted", None),
               "ver": getattr(st, "bank_version_filter", None),
               "ds": getattr(getattr(ctx, "dataset", None), "name", None),
               "nstage": len(getattr(getattr(ctx, "view", None), "_stages", None) or []),
               **extra}
        with open(_TRACE_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:      # noqa: BLE001 — 계측이 패널을 죽이면 안 된다
        pass


def _remember(ctx, control, value):
    """서버가 **프로그램적으로** 바꾼 컨트롤 값을 `_APPLIED` 에 반영한다.

    `_change_guard` 는 사용자 클릭 경로에서만 기억을 갱신한다. 핸들러가 곁들여 바꾸는 값
    (예: 규칙 전환 시 색칠 기본값)을 안 적어두면 `_sync_controls` 의 기억 우선 미러가
    **옛 값을 되밀어** 방금 바뀐 색칠이 화면에서 되돌아간다.
    """
    pid = (getattr(ctx, "params", None) or {}).get("panel_id")
    if pid is not None:
        _APPLIED[(pid, control)] = value


def _rows_to_markdown(rows, join_field_missing=None, total=None):
    """선택 프레임의 승자 문장 표 (프레임→문장 방향, 스펙 §5.2). types.TableView 대신 md —
    Object.md(markdown, name=...)의 첫 인자가 실제 표시 내용이라 여기서 직접 조립한다.

    join_field_missing: Task 12 — 문장 클릭의 버전→조인 필드 매핑이 세션 데이터셋에
    없을 때 그 필드명. 표 위에 안내만 붙이고 표 자체(선택 문장 메타)는 그대로 보여준다
    (조인 실패 ≠ 문장 정보 없음).
    total: 전체 선택 문장 수 — rows가 상한으로 잘렸으면 표 위에 표기 (lasso 다중선택).
    """
    note = ""
    if join_field_missing:
        note = (f"*(조인 필드 없음: `{join_field_missing}` — 이 데이터셋에 해당 필드가 없어 "
                f"프레임 하이라이트를 건너뜁니다)*\n\n")
    if total and total > len(rows):
        note += f"*(선택 {total}개 중 상위 {len(rows)}개 표시)*\n\n"
    if len(rows) > 1:
        # 버전별 승자를 모으면 같은 문장이 여러 버전에서 반복된다 (실측: 31버전 중
        # 14버전이 같은 문장). 행 수만 보고 "왜 이렇게 많나" 로 읽히지 않게 구성을 밝히고,
        # 문장 기준으로 정렬해 중복이 붙어 보이게 한다. wins/purity 는 **버전마다 다르므로**
        # 행을 합치지 않는다 (합치면 남의 버전 통계를 한 줄에 뭉개는 조용한 오답).
        n_txt = len({str(r.get("text")) for r in rows})
        note += (f"*{len(rows)}행 = 버전별 승자 1개씩 · **고유 문장 {n_txt}종** "
                 f"(같은 문장을 여러 버전이 공유)*\n\n")
        rows = sorted(rows, key=lambda r: (-(r.get("cos") or -2.0),
                                           str(r.get("text")), str(r.get("ver") or "")))
    if not rows:
        return note + "*(선택된 프레임 없음)*"
    # 자리표시자 경고 — 그 행의 text 는 문장이 아니다. `#N` 의 N 은 공급자 CSV/JSON 의 `ID`
    # 컬럼이라 행을 식별하지도 못한다 (실측: v1.0.8.0 은 12,480행에 ID 2,405종, v1.0.6.2 는
    # 16,125행 전부 ID=0 → 전부 `#0`). "0번 문장" 으로 읽히는 사고를 여기서 끊는다.
    n_ph = sum(1 for r in rows
               if str(r["text"]).lstrip().startswith(PLACEHOLDER_PREFIX))
    if n_ph:
        # 2026-08-19 이후 이 표의 문장은 **DB(bank_sentences) 정본**을 먼저 본다 — 그러고도
        # 자리표시자가 남았다면 그 뱅크는 DB 에도 문장이 없다(`sentence_storage='external_only'`)
        # 거나 행수/class 게이트에서 거부된 것이다. 즉 여기 남은 자리표시자는 **사실**이다.
        note += (f"*(⚠️ {n_ph}/{len(rows)}행은 DB 정본에도 문장이 없는 뱅크 버전입니다 — "
                 f"`{PLACEHOLDER_PREFIX} #N)` 의 N 은 공급자 ID 라 문장을 식별하지 않습니다. "
                 f"원본 CSV 가 있으면 `repair_bank_prompts.py`, 없으면 벡터 전용이 사실)*\n\n")
    # ⚠️ **버전 열은 필수다.** 같은 문장이 여러 버전에 공유되고(content_hash 로 벡터까지
    #    공유) "전체" 필터에서는 조인이 레거시 기본값(v1.0.8.0)으로 폴백하므로, 버전 없이는
    #    이 행이 어느 뱅크 것인지 알 방법이 없다 (2026-08-31 사용자 리포트).
    # gidx 는 내부 식별자라 화면에서 읽을 가치가 낮다 — **cos**(이미지↔문장 코사인)로 대체.
    # `-` 는 그 행의 코사인을 못 구한 경우(프레임 미선택·후보 계산 실패·벡터 미보유)다.
    # `#` = 플롯의 마젠타 점에 찍힌 번호 (build_mode_a 의 `hl_index`). 같은 문장을 공유하는
    # 버전들은 같은 번호를 갖는다 — 표에서 번호를 보고 플롯에서 그 점을 찾는다.
    # 클래스 = 그 문장이 노리는 클래스(뱅크 정의). 호버는 원래부터 보여줬는데 표만
    # 빠져 있어 "어느 카테고리 문장인지 알 수 없다" 는 리포트가 났다 (2026-09-09).
    dist = cls_counts_line([r.get("cls") for r in rows])
    if dist:
        note += dist + "\n\n"
    header = ("| # | 버전 | 클래스 | cos | text | wins | purity | n_cameras | wave_gain |\n"
              "|---:|---|---|---:|---|---|---|---|---|\n")
    # wave_gain 은 `.3f` 로는 전 행의 99.6% 가 0.000 으로 뭉갠다 (실측 중앙값 |1.2e-05|,
    # 90분위 |4.8e-05|, 최대 4.1e-03) — LOO ΔIoU 는 원래 이 스케일이다. 6자리로 편다.
    body = "".join(
        f"| {r.get('hl') if r.get('hl') is not None else '-'} | "
        f"{r.get('ver') or '?'} | "
        f"{r.get('cls') or '-'} | "
        f"{('%.4f' % r['cos']) if r.get('cos') is not None else '-'} | "
        f"{str(r['text']).replace('|', chr(92) + '|')} | "
        f"{r['wins']} | {r['purity']:.3f} | "
        f"{r['n_cameras']} | {r['wave_gain']:.6f} |\n"
        for r in rows
    )
    return note + header + body


class PromptComparePanel(foo.Panel):
    @property
    def config(self):
        return foo.PanelConfig(name="user_prompt_compare",
                               label="Prompt Compare", surfaces="grid")

    def on_load(self, ctx):
        """마운트 초기화. **이미 초기화된 패널의 사용자 선택은 건드리지 않는다.**

        ⚠️ 옛 코드는 매번 무조건 기본값을 대입했다. `on_load` 는 리마운트뿐 아니라
        `on_change_selected` 의 자가복구 경로(빈 panel_state 에코)에서도 불리므로,
        **사용자가 고른 `표시=채택만` 이 남의 탭 에코 한 번에 "전체" 로 되돌아갔다.**
        되돌아간 뒤 사용자가 같은 값을 다시 고르면 `_change_guard` 가 삼켜서(옛 값을
        기억하고 있으므로) **전체 ↔ 채택만 왕복**이 됐다 — 2026-08-20 사용자 신고.

        그래서 리셋은 **이 데이터셋의 서버 기억이 비어 있을 때만** 한다
        (`mode` 가 없다 = 콜드). 서버 기억은 `(패널, 데이터셋)` 키라 이 한 조건이
        옛 코드의 `first` 와 `switched` 를 둘 다 대신한다:
          · 리마운트·빈 panel_state 에코 → 기억이 warm → **사용자 선택 보존**
          · 데이터셋 전환 → 그 데이터셋 기억이 콜드 → 리셋
            (메모리 §16 의 "데이터셋 전환 시 bank_version_filter 는 리셋 유지")
        리셋할 때는 `_forget_applied` 로 가드 기억도 같이 비운다 (그 함수 주석 참고) —
        `_APPLIED` 는 panel_id 키라 데이터셋 전환에도 살아남아, 안 비우면 새 데이터셋에서
        **같은 값을 다시 고르는 클릭이 가드에 삼켜진다**.
        """
        _trace(ctx, "on_load")
        if _mem(ctx).mode is None:
            _mem(ctx).rule = "argmax_k1"
            _mem(ctx).color_by = COLOR_BY_CATEGORY
            _mem(ctx).show_unadopted = True
            _mem(ctx).mode = "A"                      # "A"|"B" — Task 9, 스펙 §5.1b
            _mem(ctx).group_field = "project"
            _mem(ctx).groups = ""
            # Task 12 — 뱅크 버전 선택기 + 프롬프트 데이터셋 자동 유도 상태.
            _mem(ctx).bank_version_filter = ALL_VERSIONS_LABEL
            _forget_applied(ctx)
        # 파생·휘발 상태는 리마운트마다 비운다 (프레임 id 는 세션 밖에서 유효하지 않다).
        _mem(ctx).selected_gidx = []
        _mem(ctx).sel_total = 0
        # top-k 3종도 같이 비운다. 여기가 `selected_gidx` 를 매 리마운트 비우는데 이들은
        # 안 비워서, **선택은 사라졌는데 표 B 는 남는** 상태가 있었다. 데이터셋을 전환하면
        # 그 잔재가 새 데이터셋 화면에 옛 프레임의 후보·기준으로 붙는다 (빈 selection 훅은
        # early-return/dedup 이라 덮어주지 못하고, 프롬프트 짝이 없는 데이터셋은 `_refresh`
        # 가 조기 반환해 확실히 남는다 — codex 지적 2026-09-09). 기준 표기가 붙은 뒤로는
        # 그 잔재가 "이 프레임의 기준" 이라고 **주장**하므로 조용한 오답이 된다.
        _mem(ctx).frame_topk = []
        _mem(ctx).topk_ref_class = None
        _mem(ctx).topk_multi = None
        _mem(ctx).bank_versions = []
        _mem(ctx).prompts_available = True
        _mem(ctx).join_field_missing = None
        self._refresh(ctx)

    def _refresh(self, ctx, update_plot=True):
        """update_plot=False = 성능 옵션: fig(_FIGS)·banner·layout 을 다시 쓰지
        않고 표(top_table)·컨트롤만 갱신한다. 선택 계열 훅은 뷰 변경이 어차피 재렌더를
        유발하므로 이중 재렌더를 피하려 False를 쓴다.

        ⚠️ 리로드 버그의 최종 진단(2026-08-10, on_change_extended_selection 주석 참고):
        emb_viz extendedSelection 파괴의 트리거는 재렌더도 상태 쓰기도 아니고 **이 패널에
        on_change 훅이 등록돼 있어 selection 변화 시 발생하는 훅 EXEC 왕복 그 자체**다
        (훅 바디 no-op이어도 파괴, 등록 제거 프로브만 생존). 한때 update_plot=False가
        파괴를 막는 것처럼 보였던 실측은 호스트 load 190 교란(파괴 측 서버 왕복이 부하로
        실패)이었다 — update_plot은 파괴 방지 수단이 아니다. 방어는 "받은 선택을 즉시
        뷰로 승격 + 빈 에코 무시" (_select_frames_view / on_change_extended_selection).
        """
        _trace(ctx, "_refresh", update_plot=update_plot)
        # ⚠️ **컨트롤 미러가 없다** (2026-09-09): 모든 컨트롤이 state 바인딩 없는 버튼으로
        #    바뀌면서 클라이언트로 내보낼 패널 상태가 하나도 남지 않았다. 그래서 이 패널은
        #    이제 사용자의 사이드바 필터를 **어떤 경로로도** 지우지 않는다 —
        #    콜드 스타트에서 클라이언트 표시값을 교정하던 1회성 쓰기도 함께 사라졌다.
        self._refresh_body(ctx, update_plot)

    def _refresh_body(self, ctx, update_plot=True):
        # 과거 set_data(patch_panel_data)로 심어진 패널 데이터가 세션에 영속되어, 있으면
        # 스키마 data를 영원히 가린다(App 번들 `mt||view.data` 우선순위). set_data를 안 쓰는
        # 지금도 옛 세션의 잔재가 남아 있으므로 매 갱신마다 비워 스키마 경로만 살린다.
        ctx.panel.data.clear()
        # 레거시 위생 (2026-08-14): 옛 배포가 **클라이언트 패널 상태**에 실은
        # scatter_data(2MB)가 세션에 남아 있으면 모든 훅 왕복을 계속 부풀린다(요청 2.5s/MB).
        # ⚠️ 서버 기억에 None 을 쓰는 건 아무 소용이 없다 (코덱스 리뷰 F4) — 클라이언트는
        #    모른다. **실제로 실려 온 경우에만** 한 번 지운다: 그 한 번은 상태 변경이라
        #    사이드바 필터를 지우지만, 안 지우면 그 세션은 영구히 2MB 를 왕복한다.
        if (getattr(ctx, "panel_state", None) or {}).get("scatter_data") is not None:
            ctx.panel.state.set("scatter_data", None)
        if _mem(ctx).mode == "C":
            # 모드 C — 문장 군집. 좌표는 emb_viz 그대로, 색만 군집 id.
            # ⚠️ 품질 판정용이 아니다: 과거 측정에서 군집 특이도는 **장소 어휘**가 지배했고
            #    이벤트 클래스로 갈리지 않았다. 중복 발견·탐색 용도로 위치를 정한 기능이다
            #    (2026-08-31 사용자와 합의). 군집 공간(임베딩/좌표)은 배너가 밝힌다.
            try:
                k = int(str(_mem(ctx).cluster_k or CLUSTER_K_DEFAULT).strip())
            except (TypeError, ValueError):
                k = CLUSTER_K_DEFAULT
            pname = _prompts_dataset_name(ctx)
            if not fo.dataset_exists(pname):
                fig = {"data": [], "banner": f"{BANNER_COORDS_C} · `{pname}` 없음",
                       "layout": {"xaxis": {"visible": False}, "yaxis": {"visible": False}}}
            else:
                fig = build_mode_c(
                    load_prompt_bundle(pname), k=k,
                    bank_version_filter=_mem(ctx).bank_version_filter,
                    # 선택을 모드 C 에도 넘긴다 — 안 넘기면 "선택이 표시되지 않는다"
                    selected_gidx=set(_mem(ctx).selected_gidx or []))
            _mem(ctx).banner = fig.get("banner", "")
            _mem(ctx).layout = fig["layout"]
            _mem(ctx).hl_index = {str(g): n
                                        for g, n in (fig.get("hl_index") or {}).items()}
            _put_fig(ctx, fig["data"], banner=_mem(ctx).banner,
                     layout=fig["layout"])
            _mem(ctx).top_table = []
            _mem(ctx).sel_total = 0
            return

        if _mem(ctx).mode == "B":
            # 모드 B는 ctx.dataset(현재 세션 데이터셋)을 그린다 — sourcei(ground_truth 등)에서도
            # 열리지만 본용도는 `frames`(구 frames_captions)에서 project 간 비교.
            groups = [g.strip() for g in (_mem(ctx).groups or "").split(",") if g.strip()]
            group_field = _mem(ctx).group_field or "project"
            if groups and ctx.dataset is not None:
                fig = build_mode_b(ctx.dataset.name, group_field, groups)
            else:
                fig = {"data": [],
                       "banner": f"{BANNER_COORDS_B} · 그룹 미선택",
                       "layout": {"xaxis": {"visible": False}, "yaxis": {"visible": False}}}
            # set_data 금지 — 아래 모드 A 쪽 주석 참고 (patch=딥머지라 줄어든 배열이 안 지워짐).
            _mem(ctx).banner = fig.get("banner", "")
            _mem(ctx).layout = fig["layout"]
            _put_fig(ctx, fig["data"], banner=_mem(ctx).banner,
                     layout=fig["layout"])
            _mem(ctx).top_table = []
            _mem(ctx).sel_total = 0
            return

        # Task 12: 프롬프트 데이터셋 자동 유도 — "<세션 데이터셋>-prompts". 없으면 크래시
        # 대신 안내 배너만 그리고 모드 A 컨트롤(규칙/미채택/버전)은 render()에서 숨긴다.
        prompts_name = _prompts_dataset_name(ctx)
        if not fo.dataset_exists(prompts_name):
            _mem(ctx).prompts_available = False
            _mem(ctx).bank_versions = []
            fig = {"data": [], "banner": NO_PROMPTS_PAIR_TEXT,
                   "layout": {"xaxis": {"visible": False}, "yaxis": {"visible": False}}}
            _mem(ctx).banner = fig["banner"]
            _mem(ctx).layout = fig["layout"]
            _put_fig(ctx, fig["data"], banner=_mem(ctx).banner,
                     layout=fig["layout"])
            _mem(ctx).top_table = []
            _mem(ctx).sel_total = 0
            return

        _mem(ctx).prompts_available = True
        b = load_prompt_bundle(prompts_name)
        bv = b.get("bank_version")
        _mem(ctx).bank_versions = sorted({str(v) for v in bv if v is not None}) \
            if bv is not None else []
        version_filter = _mem(ctx).bank_version_filter or ALL_VERSIONS_LABEL
        sel = set(_mem(ctx).selected_gidx or [])
        if update_plot:
            # ⚠️ `or "argmax_k1"` 는 493행 컨트롤 미러와 **같은 기본값**이어야 한다.
            #    미러에만 폴백이 있어서, state.rule 이 None 인 경로(패널 상태 초기화 등)에서
            #    드롭다운은 "topk" 로 보이는데 플롯은 else 분기(dist_iou)로 가 범례가
            #    wave_role(유익/유해/중간)로 나왔다 — 2026-08-12 사용자 리포트의 원인.
            fig = build_mode_a(b, rule=_mem(ctx).rule or "argmax_k1",
                               show_unadopted=_mem(ctx).show_unadopted,
                               selected_gidx=sel, bank_version_filter=version_filter,
                               color_by=_mem(ctx).color_by,
                               view_winner=view_winner_gidx(ctx))
            # 플롯 번호를 표와 공유한다 (아래 `#` 열). 키는 문자열로 — panel_state 는 JSON
            # 왕복이라 int 키가 문자열로 바뀌어 돌아온다.
            _mem(ctx).hl_index = {str(g): n for g, n in (fig.get("hl_index") or {}).items()}
        # ⚠️ set_data 사용 금지 (사용자 피드백 라운드 실측, 2026-08-07): set_data →
        # patch_panel_data 는 클라이언트 패널 데이터 저장소에 **딥머지(patch)** 된다 —
        # 배열이 줄어드는 갱신(미채택 숨김: 12,166→0, 버전 필터: 전체→부분집합)에서 새
        # 짧은 배열이 옛 긴 배열의 꼬리를 못 지워 유령 점이 화면에 남는다(토글 후 trace0
        # n=12,279 잔존 실측). 반면 render()가 스키마에 굽는 data(_FIGDATA 경유)는
        # show_panel_output 마다 통째로 교체되므로, 데이터 갱신은 이 경로 하나만 쓴다.
        # (한 번이라도 set_data 를 부르면 클라이언트가 patched data 를 스키마 data 보다
        # 우선하므로 — App 번들 `wo=mergeData(mt||Lt?.view?.data,…)` — 부분 도입도 불가.)
        # trace 리스트 자체는 state 에 싣지 않는다 — _FIGDATA 주석(요청 2.5s/MB) 참고.
        if update_plot:
            _mem(ctx).banner = fig.get("banner", "")
            _mem(ctx).layout = fig["layout"]
            _put_fig(ctx, fig["data"], banner=_mem(ctx).banner,
                     layout=fig["layout"])
        rows = []
        if sel:
            import numpy as np
            # 상한 50 (구 20): lasso 다중선택 도입으로 수십 개 선택이 일상 — 표가 내부 스크롤을
            # 갖게 돼(render의 maxHeight) 행이 늘어도 레이아웃을 밀지 않는다.
            # 정렬 = wins 내림차순: 넓은 box select는 미채택(wins 0)이 다수라 gidx순으로는
            # 승자 문장이 상한 밖으로 밀린다 (2026-08-10 실측: 7,716개 선택 중 대부분 미채택).
            idxs = np.nonzero(np.isin(b["gidx"], np.asarray(sorted(sel))))[0]
            top = [int(i) for i in idxs[np.argsort(-b["wins"][idxs].astype(int),
                                                   kind="stable")][:50]]
            # 표의 문장도 DB 정본으로 (≤50행 — 호버와 같은 메모를 공유해 대개 왕복 0회).
            texts, _meta = _resolve_text(b, top)
            for i, text in zip(top, texts):
                # `_val` 폴백 — 필드 없는 데이터셋(frames-prompts 의 wave_gain 등)에서
                # 표가 죽지 않게. 자세한 근거는 `_val` docstring.
                # `cls` — 번들 `category` 를 그대로 쓴다. DB(`pdb_fetch_texts`)의 두 번째
                # 값을 따로 읽으면, 게이트 ③ 이 그 버전을 폴백시킨 경우 **문장은 폴백인데
                # 클래스는 거부된 DB 행** 이 되어 출처가 갈린다. 번들 값은 산점도 점·호버와
                # 같은 행 의미를 유지한다.
                # ⚠️ `or "-"`: `_val` 은 **배열 자체**가 없을 때만 기본값을 준다. 셀 단위
                #    None 은 방어해야 한다 — `frames-prompts` 데이터셋에는 category 가 NULL 인
                #    캡션 행이 11,978개 있다(실측). 현행 `emb_viz` 가 그 행을 안 덮어서
                #    번들에는 안 들어오지만(603,318점), brain run 범위가 바뀌면 들어온다.
                rows.append({"gidx": int(b["gidx"][i]), "text": str(text),
                             "ver": (str(b["bank_version"][i])
                                     if b.get("bank_version") is not None else None),
                             "cls": str(_val(b, "category", i, "-") or "-"),
                             "wins": int(_val(b, "wins", i, 0)),
                             "purity": float(_val(b, "purity", i, 0.0)),
                             "n_cameras": int(_val(b, "n_cameras", i, 0)),
                             "wave_gain": float(_val(b, "wave_gain", i, 0.0))})
        # 승자 행에 **코사인**을 붙인다 (사용자 요청 2026-08-31: gidx 대신 cos).
        # 승자는 정의상 그 버전의 1위라 `frame_topk` 안에 반드시 있다 — 조회 키는
        # (정규화 버전, **로컬** gidx) 다. 번들 gidx 는 전역이라 `pdb_local_gidx` 로 내린다.
        tk = {(pdb_norm_ver(str(r["ver"])), int(r["gidx"])): float(r["cos"])
              for r in (_mem(ctx).frame_topk or []) if "err" not in r}
        hlx = _mem(ctx).hl_index or {}
        for r in rows:
            r["cos"] = tk.get((pdb_norm_ver(str(r.get("ver") or "")),
                               pdb_local_gidx(r["gidx"])))
            r["hl"] = hlx.get(str(r["gidx"]), hlx.get(r["gidx"]))
        _mem(ctx).top_table = rows
        _mem(ctx).sel_total = len(sel)

    # ── 프레임 → 문장 : Samples 그리드 체크박스 (Task 5 실측 확정 훅) ──
    #    (실측, Task 8) App은 패널 오퍼레이터가 하나라도 실행되면 등록된 on_change_* 훅
    #    전부를 "현재 값"으로 재발화한다 — 값이 실제로 안 바뀌어도 재발화됨. 마지막으로 처리한
    #    시그니처와 같으면 무시해야 다른 훅(on_plot_click 등)이 막 세팅한 상태를 덮어쓰지 않는다.
    #    가드 상태는 `_dedup_guard`로 위임 (밑줄 없는 상태 키 필수 — 헬퍼 docstring 참고).
    def on_change_view(self, ctx):
        """뷰 바 스테이지·사이드바 필터가 바뀌면 다시 그린다.

        프레임이 좁아지면 **그 프레임들이 뽑는 승자 문장 집합**이 달라지므로, 단순
        재렌더가 아니라 채택 판정 자체가 바뀐다 (build_mode_a 의 view_winner 주석 참고).

        ⚠️ **여기서 패널 상태를 쓰면 안 된다** (2026-09-04 실측). `_refresh` 가 남기는
        `patch_panel_state` 는 App 에서 `setSpacesMutation` → `DatasetPageQuery` 재조회로
        이어지는데, 사이드바 필터는 서버 세션이 아니라 **클라이언트에만** 있는 상태라 그
        재조회에 통째로 날아간다 — 사용자가 사이드바에서 값을 골라도 0.7초 뒤 전량으로
        돌아오던 증상의 정체 (뷰 바 스테이지는 세션에 있어 살아남는다).
        그래서 이 훅은 **아무것도 하지 않는다**: App 은 훅 실행 직후 `render()` 를 부르고,
        `_fig_key` 에 뷰 지문이 들어 있어 render 가 새 뷰 기준(`view_winner_gidx`)으로
        figure·배너를 재구성한다. 표(top_table)는 **선택** 함수라 뷰 변경과 무관하다.

        ✅ 2026-09-09: 컨트롤·선택 경로까지 전부 닫았다 — 패널 상태는 서버측 `_mem` 으로
        옮겼고, 클라이언트로 나가는 것은 컨트롤 미러 하나뿐이며 그것도 클라이언트가 낡았을
        때만 나간다. 셀프테스트의 "사이드바 필터 보존 계약" 블록이 전 훅에 대해 쓰기 0 을
        못 박는다.
        """
        _trace(ctx, "on_change_view")
        return

    def on_change_selected(self, ctx):
        _trace(ctx, "on_change_selected")
        if _mem(ctx).mode is None:
            # 자가 복구 (2026-08-14 실측): 서버 재시작 뒤 옛 탭이 재접속하면 빈 panel_state
            # 에코가 공유 세션에 퍼져 컨트롤/배너가 통째로 사라진다. 초기화 안 된 상태
            # (mode=None)로 도착한 에코는 on_load 로 되살린다 — 지울 사용자 선택이 없는
            # 상태에서만 발동하므로 안전. 수동 에코 훅 2개에만 필요 (on_*_change 는
            # _refresh 경유로 자연 복구).
            self.on_load(ctx)
            return
        if _mem(ctx).rule != "argmax_k1":
            return   # dist_iou: 프레임 귀속 없음 — 배너가 없다고 선언한 조인을 그리지 않는다 (opus F7)
        ids = ctx.selected or []
        if _dedup_guard(ctx, "sel_seen", ids):
            return
        _mem(ctx).join_field_missing = None  # 이전 클릭 안내는 새 프레임 선택과 무관
        # Task 12: 프레임 데이터셋은 항상 "현재 세션 데이터셋"(ctx.dataset) — 예전처럼
        # 하드코딩된 FRAMES_DATASET("sourcei")로 고정하면 source-h 세션에서 오조인된다.
        frames_name = ctx.dataset.name if ctx.dataset is not None else FRAMES_DATASET
        winner_field = _current_winner_field(ctx)
        _mem(ctx).selected_gidx = self._winner_gidx(ctx, ids, frames_name, winner_field)
        _mem(ctx).frame_topk = self._topk_for(ctx, ids, frames_name)
        # ⚠️ **여기는 update_plot=True 여야 한다** (2026-09-09 사용자 리포트: "samples 이미지를
        #    선택했는데 prompt 패널에 선택 표시가 안 된다"). `_refresh` docstring 이 밝힌
        #    update_plot=False 의 전제는 *"선택 계열 훅은 뷰 변경이 어차피 재렌더를 유발한다"*
        #    인데, **그리드 체크박스 선택은 뷰를 바꾸지 않는다** — `_view_sig` 는 request_params
        #    의 view/filters/extended 만 해싱하고 `selected` 는 안 본다(FiftyOne 1.19
        #    `ExecutionContext.selected` 는 `selected_samples`/`selected` 에서 온다). 그래서
        #    `_fig_key` 가 그대로라 render 가 **캐시 히트로 하이라이트 없는 옛 figure** 를 냈다:
        #    표는 갱신되는데 산점도만 안 바뀌는 정확한 증상. lasso 경로(extended)는 지문이
        #    바뀌어 render 재구성이 메워주므로 그쪽만 동작했다.
        #    True 로 두면 여기서 fig 를 만들어 `_put_fig` 하므로 render 는 캐시 히트가 되고,
        #    표의 `#` 열이 읽는 `hl_index` 도 같이 최신이 된다(False 면 stale). 이중 재렌더도
        #    아니다 — 어느 쪽이든 build 는 정확히 한 번이고, False 는 그 한 번을 render 로
        #    미루는 것이었을 뿐이다.
        self._refresh(ctx)

    def _winner_gidx(self, ctx, ids, frames_name, winner_field):
        """선택 프레임의 승자 gidx — **버전 필터가 "전체" 면 전 버전**을 모은다.

        옛 동작은 "전체" 에서도 `WINNER_FIELD`(v1.0.8.0) 하나만 봐서, 아래 top-k 표
        (37버전)와 범위가 어긋났다 (`dataset_winner_fields` docstring 참고).
        """
        if not ids:
            return []
        filt = _mem(ctx).bank_version_filter
        if filt and filt != ALL_VERSIONS_LABEL:
            return frame_ids_to_gidx(ids, dataset_name=frames_name, winner_field=winner_field)
        by_ver = frame_ids_to_gidx_all(ids, frames_name)
        return sorted({g for gs in by_ver.values() for g in gs})

    def _topk_for(self, ctx, ids, frames_name):
        """프레임 **1장**일 때만 버전별 후보를 뽑는다 (다중 선택엔 의미 없음 + 질의가 1초대).

        저장된 건 `winner_gidx_<태그>`(1위)뿐이라 2~5위는 어디에도 없다 — 즉석 KNN 으로만
        얻는다. 실패는 조용히 빈 목록(패널이 죽으면 안 된다 — pdb_banks 와 같은 계약).
        """
        if not ids:
            return []
        # 여러 장이면 **첫 프레임 기준**으로 계산하고 그 사실을 밝힌다 — 거부하면
        # 사용자는 "아무것도 안 나온다" 로만 읽는다 (2026-08-31 사용자 리포트).
        note = (f"{len(ids):,}장 선택 — **첫 프레임 기준**"
                if len(ids) > 1 else None)
        _mem(ctx).topk_multi = note
        try:
            smp = ctx.dataset[str(ids[0])]
            emb = smp["embedding"] if "embedding" in ctx.dataset.get_field_schema() else None
            src = "FiftyOne embedding"
            if emb is None:         # `frames` 처럼 필드가 없는 데이터셋 — DB 폴백
                emb, src = pdb_frame_vector(smp.filepath), "Postgres frame 벡터"
            # 출처를 화면에 밝힌다 — 폴백이 조용히 일어나면 어느 벡터로 잰 점수인지 알 수 없다
            _mem(ctx).topk_src = src
            _mem(ctx).topk_ref_class = _frame_ref_class(smp)
        except Exception as e:      # noqa: BLE001
            return [{"err": f"프레임 조회 실패: {type(e).__name__}: {e}"}]
        if emb is None:
            return [{"err": "이 프레임의 임베딩을 못 찾았습니다 "
                            "(FiftyOne `embedding` 필드 없음 + Postgres frame 벡터도 없음)"}]
        try:
            out = pdb_frame_topk(frames_name, ids[0], emb)
        except Exception as e:      # noqa: BLE001 — 조용히 삼키면 증상만 남는다
            return [{"err": f"DB 질의 실패: {type(e).__name__}: {str(e)[:120]}"}]
        return out or [{"err": "질의는 됐지만 결과 0행 (뱅크/벡터 조인 확인 필요)"}]

    # ── 프레임 → 문장 : 네이티브 Embeddings lasso (Task 5 실측 — on_change_selected는
    #    lasso에 반응하지 않는다, 0 ids 유지. lasso는 이 훅으로만 온다.
    #    payload = {"selection": [sample_id, ...], "scope": "global"|None, ...}) ──
    def on_change_extended_selection(self, ctx):
        _trace(ctx, "on_change_ext_sel")
        if _mem(ctx).mode is None:
            self.on_load(ctx)   # 빈 상태 에코 자가 복구 — on_change_selected 주석 참고
            return
        if _mem(ctx).rule != "argmax_k1":
            return   # dist_iou: 프레임 귀속 없음 (opus F7 — on_change_selected 와 동일 게이트)
        ext = ctx.extended_selection or {}
        ids = ext.get("selection") or []
        if not ids:
            return   # 빈 에코 무시 — 해제는 '선택 해제' 버튼/그리드 해제 경로가 담당
        if _dedup_guard(ctx, "ext_sel_seen", ids):
            return
        _mem(ctx).join_field_missing = None
        frames_name = ctx.dataset.name if ctx.dataset is not None else FRAMES_DATASET
        winner_field = _current_winner_field(ctx)
        _mem(ctx).selected_gidx = self._winner_gidx(ctx, ids, frames_name, winner_field)
        _mem(ctx).frame_topk = self._topk_for(ctx, ids, frames_name)
        # ⚠️ 이 방향은 **읽기 전용** — 뷰를 절대 건드리지 않는다 (2026-08-11 사용자 리포트:
        # "embeddings 에서 선택하면 add stage 에 뭔가 추가된다"의 직접 원인이 여기 있던
        # 뷰 승격이었다). 네이티브 lasso 자체는 뷰 바에 아무것도 만들지 않는다 — Embeddings
        # 청크는 setView 를 import 하지 않고, 그리드 좁히기는 뷰 바에 렌더되지 않는
        # extendedSelectionOverrideStage 로 건다. 게다가 그 청크의 플롯/override 이펙트
        # deps 에 view 가 들어 있어, 우리가 뷰를 바꾸면 override 가 재계산되며 사용자의
        # 선택이 스스로 사라진다 — 즉 옛 "방어책"이 막으려던 증상의 원인 일부였다.
        self._refresh(ctx, update_plot=False)   # 표·컨트롤만 갱신 (12k점 재렌더 회피)

    # ── 문장 → 프레임 ──
    def on_plot_click(self, ctx):
        # ctx.params["id"] ← trace.ids[pointIndex] (App 번들 getIdForTrace 실측).
        # data.customdata 는 onClick 이벤트에 아예 실리지 않는다 — 브리프 원문의 가정은 틀렸다.
        if _mem(ctx).mode != "A":
            return  # 모드 B trace는 ids를 싣지 않아 프레임 귀속이 없다 (교차 데이터셋 조인 아님)
        raw_id = (ctx.params or {}).get("id")
        if raw_id is None:
            return
        g = int(raw_id)
        if _mem(ctx).rule != "argmax_k1":
            return  # dist_iou 모드: 귀속 없음 — 클릭 무효 (배너가 안내)
        # Task 12: 클릭된 "그 문장 row"의 bank_version에서 조인 필드를 유도한다 — 패널의
        # 전역 버전 필터가 아니라 그 문장 자체가 속한 버전 기준(요구사항 3). 필드가 세션
        # 데이터셋 스키마에 없으면 "조인 필드 없음" 안내로 무효 처리하고 크래시하지 않는다.
        prompts_name = _prompts_dataset_name(ctx)
        if not fo.dataset_exists(prompts_name):
            return
        import numpy as np
        b = load_prompt_bundle(prompts_name)
        idxs = np.where(b["gidx"] == g)[0]
        if len(idxs) == 0:
            return
        version_str = str(b["bank_version"][int(idxs[0])]) if b.get("bank_version") is not None else None
        join_field = _resolve_join_field(ctx.dataset, version_str)
        _mem(ctx).selected_gidx = [g]
        # 빈 그리드-에코 선점: show_samples 뷰 변경이 ctx.selected 를 비우고 on_change_selected
        # 를 재발화시키면 방금 만든 문장 선택이 지워진다 (opus F5, 2026-08-12) — 빈 시그니처를
        # 미리 심어 dedup 가드가 그 에코를 삼키게 한다.
        _mem(ctx).set("sel_seen", [])
        ids = []
        if join_field is None:
            _mem(ctx).join_field_missing = \
                version_to_winner_field(version_str) if version_str else "?"
        else:
            _mem(ctx).join_field_missing = None
            frames_name = ctx.dataset.name if ctx.dataset is not None else FRAMES_DATASET
            ids = gidx_to_frame_ids(g, dataset_name=frames_name, winner_field=join_field)
        if ids:
            self._select_frames_view(ctx, ids)   # 뷰 기반 반영 — 헬퍼 docstring 참고
        else:
            # 조인 실패/승자 프레임 0 — 이전 문장의 Select 칩을 걷어낸다. 안 걷으면
            # 그리드의 옛 프레임들이 새 문장의 결과처럼 읽힌다 (opus F1).
            self._clear_frames_view(ctx)
        # 전체 갱신(하이라이트 포함) — 이 방향은 뷰 기반이라 재렌더가 파괴할 extended
        # selection이 없다 (사용자 피드백: 선택했으면 시각적으로 표시돼야 한다).
        # emb_viz 선택을 소비하는 on_change_* 훅과 달리 update_plot=False가 불필요.
        self._refresh(ctx)

    # ── 문장 → 프레임 : Plotly lasso/box select (modebar 로 드래그 모드 전환) ──
    # 네이티브 Embeddings 패널의 g/s 단축키는 App 번들 React 컴포넌트 내부 하드코딩이라
    # Python 패널 API(on_load/on_startup 뿐, PanelConfig에 hotkey 없음 — 1.19.0 실측)로는
    # 재현 불가. 대신 modebar를 상시 표시(render의 displayModeBar)해 pan↔lasso↔box 전환은
    # 클라이언트에서 즉시 되게 하고, 선택 이벤트를 이 훅으로 받아 클릭과 동일하게 조인한다.
    def on_plot_selected(self, ctx):
        # PlotlyView onSelected: ctx.params["data"] = [{"trace","trace_idx","idx","id",...}]
        # — id = trace.ids[idx] (on_click과 같은 계약, PlotlyView docstring 실측).
        _trace(ctx, "plot_selected_in", rule=_mem(ctx).rule,
               n_items=len((ctx.params or {}).get("data") or []))
        if _mem(ctx).mode != "A" or _mem(ctx).rule != "argmax_k1":
            _trace(ctx, "plot_selected_gate_off")
            return  # 모드 B/dist_iou: 프레임 귀속 없음 — 클릭과 동일하게 무효
        items = (ctx.params or {}).get("data") or []
        ids = sorted({int(d["id"]) for d in items if d.get("id") is not None})
        _trace(ctx, "plot_selected_ids", n_ids=len(ids))
        if not ids:
            # scattergl box select는 mouseup에 plotly_selected를 두 번 쏜다 — 점 있는 이벤트
            # 직후 빈 이벤트(실측 2026-08-10: selected 6666 → selected 0 연속). 빈 payload가
            # 방금 만든 선택을 지우면 box select가 "안 되는" 것처럼 보인다. 해제는
            # on_plot_double_click(플롯 더블클릭)과 그리드 선택 해제 경로가 담당.
            return
        # dedup 가드 없음 — on_click과 같은 plot 이벤트라 App 재발화(on_change_* 한정, Task 8)
        # 대상이 아니고, 가드를 걸면 클릭으로 상태가 바뀐 뒤 같은 영역 재-lasso가 삼켜진다.
        _mem(ctx).selected_gidx = ids
        _mem(ctx).join_field_missing = None
        _mem(ctx).set("sel_seen", [])   # 빈 그리드-에코 선점 (opus F5 — on_plot_click 참고)
        frame_ids = []
        prompts_name = _prompts_dataset_name(ctx)
        if ids and fo.dataset_exists(prompts_name):
            b = load_prompt_bundle(prompts_name)
            # 문장별 bank_version → 조인 필드로 버킷팅 (on_plot_click과 같은 per-문장 규칙).
            # 성능(codex 리뷰): lasso는 미채택 포함 최대 MAX_POINTS개 — gidx당 np.where 풀스캔
            # 대신 gidx→row 딕셔너리 1회 + 버전→조인필드 메모이즈로 O(n+k).
            row_of = {int(v): i for i, v in enumerate(b["gidx"])}
            jf_of_version = {}
            by_field = {}
            for g in ids:
                i = row_of.get(g)
                if i is None:
                    continue
                vs = str(b["bank_version"][i]) if b.get("bank_version") is not None else None
                if vs not in jf_of_version:
                    jf_of_version[vs] = _resolve_join_field(ctx.dataset, vs)
                jf = jf_of_version[vs]
                if jf is None:
                    _mem(ctx).join_field_missing = \
                        version_to_winner_field(vs) if vs else "?"
                    continue
                by_field.setdefault(jf, []).append(g)
            frames_name = ctx.dataset.name if ctx.dataset is not None else FRAMES_DATASET
            for jf, gs in by_field.items():
                frame_ids += gidxes_to_frame_ids(gs, dataset_name=frames_name, winner_field=jf)
            # 중복 제거(codex 리뷰): 같은 프레임이 버전별 winner 필드 양쪽의 승자면 버킷 두 개에서
            # 두 번 들어온다 — 중복이 남으면 클라이언트가 dedup해 돌려줄 때 에코 선점 비교
            # (sorted 리스트 동등)가 어긋나 진짜 처리로 오인된다.
            frame_ids = sorted(set(frame_ids))
        _trace(ctx, "plot_selected_frames", n_frames=len(frame_ids))
        if frame_ids:
            self._select_frames_view(ctx, frame_ids)
        else:
            self._clear_frames_view(ctx)   # 승자 0 — 이전 문장 칩 잔존 방지 (opus F1)
        # 전체 갱신(하이라이트 포함) — 뷰 기반이라 재렌더가 파괴할 extended selection이
        # 없다 (사용자 피드백: box select 후 선택 표시가 보여야 한다). update_plot=False는
        # emb_viz의 extended selection을 소비하는 on_change_* 훅에만 필요 (_refresh docstring).
        self._refresh(ctx)

    def _select_frames_view(self, ctx, frame_ids):
        """문장→프레임 반영은 extended selection이 아니라 **뷰(Select stage)** 로 건다.

        extended selection은 scope를 "global"로 주든 emb_viz 자체 scope로 주든 네이티브
        Embeddings 패널의 내부 동기화 기계가 ~10초 뒤 스스로 지운다 (2026-08-10 실측:
        두 scope 모두 그리드 필터→복귀→선택 소멸; emb_viz 패널을 닫으면 안 지워짐 =
        그 패널이 소거 주체. App 번들이라 Python에서 수정 불가). 뷰 경로는 자가 소거
        기계가 없고, 그리드·Embeddings 패널 모두 선택 프레임만 보여주는 형태로 반영된다
        (사용자 요청: 관련 이미지가 samples/embeddings에 표시). 해제 = 플롯 더블클릭.

        ⚠️ 반드시 내장 `show_samples` 를 쓴다 — `set_view(ctx.view.select(...))` 로 직접
        만들면 **선택할 때마다 Select stage 가 뷰 바에 하나씩 쌓인다**(2026-08-11 사용자
        리포트: "embedding 을 선택하면 add stage 에 뭔가가 추가된다"). base 가 이미 우리
        스테이지를 포함한 ctx.view 라 append 되고, 교집합으로만 좁아져 다른 영역을 lasso
        하면 결과가 비기도 한다. 내장 오퍼레이터는 고정 `_uuid`("show_samples_stage_id")로
        **직전 스테이지를 제거한 뒤 새로 붙이므로** 항상 1개만 유지되고 사용자가 뷰 바에
        직접 건 필터는 보존된다 (App 번들 ShowSamples.execute 실측:
        `view.filter(s => s._uuid !== SHOW_SAMPLES_STAGE_ID)` 후 append).
        """
        ctx.ops.show_samples(list(frame_ids))

    def _clear_frames_view(self, ctx):
        """우리 Select 스테이지만 제거 — 사용자가 뷰 바에 직접 건 스테이지는 보존.

        ⚠️ `show_samples(None)` 금지: ShowSamples 는 로컬 레지스트리 오퍼레이터라 실행 전
        validateOperatorInputs 를 거치는데 `samples` 가 **required List** 로 선언돼 있어
        (번들 실측: `mt.list("samples", new OperatorString, {required:!0})`) null 이
        "Required property" 로 걸려 execute 에 도달조차 못 한다. `[]` 도 불가 — JS 에서
        빈 배열은 truthy 라 `Select(sample_ids=[])` 가 붙어 0장 그리드가 된다.
        `clear_view()` 는 동작하지만 사용자 스테이지까지 통째로 날린다. 그래서 원본 스테이지
        리스트에서 우리 것만 빼고 set_view 를 직접 트리거한다.
        """
        # ponytail: request_params["view"] 는 EXEC 요청 시점 스냅샷 — trigger 는 enqueue 후
        # 즉시 리턴하므로(executor.py 실측, codex) 직전 트리거가 아직 클라에 반영 전이면
        # 밀리초급 TOCTOU 창이 있다. 사람 손 속도에서는 실발생이 없어 수용; 재발 시
        # 클라이언트측 idempotent clear 오퍼레이터로 승격.
        stages = _client_view_stages(ctx)
        kept = [s for s in stages if s.get("_uuid") != SHOW_SAMPLES_STAGE_ID]
        if len(kept) == len(stages):
            return False                      # 우리 칩 없음 — 사용자 뷰를 건드리지 않는다
        # ctx.ops.set_view(view=...) 는 DatasetView 전용(_serialize_view 호출)이라 raw stage
        # 리스트는 trigger 로 직접 넘긴다.
        ctx.trigger("set_view", params={"view": kept})
        return True

    def _clear_selection(self, ctx):
        """선택 해제 — 문장 선택·프레임 뷰·하이라이트를 한 번에 원복.

        ext_sel_seen 에는 []가 아니라 **현재 살아 있는 extended selection 의 시그니처**를
        심는다 (opus F6): 빈 에코는 훅 진입부에서 이미 early-return 이라 [] 선점은 무의미
        하고, 오히려 아직 살아 있는 lasso 의 재발화 dedup 을 풀어 방금 한 해제가 selected_gidx
        를 되살린다. 라이브 시그니처를 심으면 그 재발화는 삼켜지고, 새 lasso 는 시그니처가
        달라 정상 통과한다."""
        _mem(ctx).selected_gidx = []
        _mem(ctx).sel_total = 0
        _mem(ctx).join_field_missing = None
        live = (getattr(ctx, "extended_selection", None) or {}).get("selection") or []
        _mem(ctx).set("ext_sel_seen", sorted(str(i) for i in live))
        _mem(ctx).set("sel_seen",
                            sorted(str(i) for i in (getattr(ctx, "selected", None) or [])))
        self._clear_frames_view(ctx)
        self._refresh(ctx)   # 전체 갱신 — 하이라이트 링 즉시 제거 (뷰 기반이라 재렌더 무해)

    def on_clear_selection(self, ctx):
        """컨트롤 행의 '선택 해제' 버튼 — 선택이 있을 때만 렌더된다(render 참고).

        ⚠️ 더블클릭에 기대면 안 된다: plotly 는 이 플롯에서 더블클릭에 `plotly_doubleclick`
        이 아니라 **`plotly_click` 을 두 번** 쏜다(2026-08-11 브라우저 실측) — PlotlyView 의
        on_double_click 훅은 발화한 적이 없다. 그래서 명시적 버튼이 유일하게 동작하는
        패널 내 해제 경로다 (뷰 바의 × 로도 스테이지는 지울 수 있지만 패널 상태는 남는다)."""
        self._clear_selection(ctx)

    def on_plot_double_click(self, ctx):
        """더블클릭 훅 — 현재 plotly 가 안 쏘지만(on_clear_selection 주석) 계약상 유지."""
        if _mem(ctx).mode != "A":
            return
        self._clear_selection(ctx)

    # ── 컨트롤 드롭다운 핸들러 (2026-08-10 피드백: 토글 버튼 → 드롭다운 통일).
    #    값은 ctx.params["value"] (아래 on_group_field_change 주석의 실측 계약과 동일).
    # ⚠️ 모든 on_*_change 공통 (2026-08-14 실측 — fetch 인터셉트 타임라인으로 확정):
    #   App 은 어떤 패널 오퍼레이터든 실행되면 등록된 on_change 훅을 재발화하고(Task 8),
    #   서버가 밀어넣은 값 변경(예: on_rule_change 의 color_by 리셋)도 "변경"으로 보고
    #   그 컨트롤의 on_change 를 쏜다 — 가드 없으면 변경 1회당 _refresh 가 3~4회 돌고,
    #   왕복이 느릴 때 응답이 역순 도착하면 stale 렌더가 새 렌더를 덮는다(드롭다운은
    #   topk 인데 화면은 dist_iou 로 굳는 실사용 버그의 직접 원인). 판정 기준은
    #   _change_guard docstring 참고 — 요청이 실어 온 state 가 아니라 서버가 마지막으로
    #   반영한 값 기준이어야 이중 발화와 '왕복 중 재클릭'을 모두 옳게 처리한다.
    def on_mode_change(self, ctx):
        v = ctx.params.get("value")
        if v not in ("A", "B", "C") \
                or _change_guard(ctx, "mode", v, v == _mem(ctx).mode):
            return
        _mem(ctx).mode = v
        self._refresh(ctx)

    def on_cluster_k_change(self, ctx):
        v = (ctx.params or {}).get("value")
        if v is not None:
            v = str(v).strip()
        if _change_guard(ctx, "cluster_k", v, v == _mem(ctx).cluster_k):
            self._close_picker(ctx, "cluster_k")
            return
        _mem(ctx).cluster_k = v
        _mem(ctx).picker = None
        self._refresh(ctx)

    def on_rule_change(self, ctx):
        v = ctx.params.get("value")
        if v not in ("argmax_k1", "dist_iou") \
                or _change_guard(ctx, "rule", v, v == _mem(ctx).rule):
            return
        _mem(ctx).rule = v
        # 규칙을 바꾸면 색칠도 그 규칙의 기본값으로 되돌린다 — 그래야 "topk 인데 wave 범례"
        # 같은 어긋남이 상태에 남지 않는다. 바꾸고 싶으면 색칠 드롭다운으로 다시 고르면 된다.
        _mem(ctx).color_by = (COLOR_BY_CATEGORY if v == "argmax_k1"
                                    else COLOR_BY_WAVE_ROLE)
        _remember(ctx, "color_by", _mem(ctx).color_by)   # 기억 미러 (_remember 주석)
        self._refresh(ctx)

    def _close_picker(self, ctx, which):
        """열려 있던 피커만 닫고 다시 그린다. 값이 안 바뀐 경로에서만 부른다."""
        if _mem(ctx).picker == which:
            _mem(ctx).picker = None
            self._refresh(ctx, update_plot=False)

    def _toggle_picker(self, ctx, which):
        """피커 하나만 열려 있게 토글한다 (다른 게 열려 있으면 그걸 닫고 이걸 연다)."""
        _mem(ctx).picker = None if _mem(ctx).picker == which else which
        self._refresh(ctx, update_plot=False)

    def on_toggle_field_picker(self, ctx):
        """모드 B 그룹 필드 피커 열기/닫기."""
        self._toggle_picker(ctx, "group_field")

    def on_toggle_groups_picker(self, ctx):
        """모드 B 그룹 값 피커 열기/닫기. **다중 선택**이라 고른 뒤에도 닫지 않는다."""
        self._toggle_picker(ctx, "groups")

    def on_toggle_k_picker(self, ctx):
        """모드 C 군집 수 피커 열기/닫기."""
        self._toggle_picker(ctx, "cluster_k")

    def on_group_toggle(self, ctx):
        """그룹 값 하나를 넣거나 뺀다 (쉼표 목록이 정본 — build_mode_b 계약 유지).

        자유 입력이었을 때는 사용자가 값을 정확히 타이핑해야 했고(오탈자 = 빈 화면),
        그 입력 컴포넌트가 패널 상태를 써서 사이드바 필터까지 지웠다. 여기서는 둘 다 없다.
        """
        v = (ctx.params or {}).get("value")
        if not v:
            return
        cur = [g.strip() for g in (_mem(ctx).groups or "").split(",") if g.strip()]
        cur = [g for g in cur if g != v] if v in cur else [*cur, v]
        _mem(ctx).groups = ",".join(cur)
        _forget_applied(ctx)     # 같은 값을 다시 눌러 빼는 동작이 가드에 삼켜지면 안 된다
        self._refresh(ctx)

    def on_toggle_color_picker(self, ctx):
        """색칠 피커 열기/닫기 — 서버 기억만 바꾼다."""
        self._toggle_picker(ctx, "color")

    def on_toggle_bank_picker(self, ctx):
        """뱅크 버전 피커 열기/닫기 — 서버 기억만 바꾼다."""
        self._toggle_picker(ctx, "bank")

    def on_color_change(self, ctx):
        # 화이트리스트 — 밖의 값이 조용히 폴백되면 범례가 규칙과 어긋난 채로 굳는다
        v = (ctx.params or {}).get("value")
        if v not in COLOR_BY_LABELS:
            return
        if _change_guard(ctx, "color_by", v, v == _mem(ctx).color_by):
            self._close_picker(ctx, "color")      # 같은 값 재선택 — 목록만 닫는다
            return
        _mem(ctx).color_by = v
        _mem(ctx).picker = None
        self._refresh(ctx)

    def on_show_change(self, ctx):
        _trace(ctx, "on_show_change")
        # 화이트리스트 (codex 3차 리뷰): 두 라벨 밖의 값이 조용히 "채택만"으로 폴백되면 안 된다
        values = {SHOW_ALL_LABEL: True, SHOW_ADOPTED_LABEL: False}
        v = (ctx.params or {}).get("value")
        if v not in values \
                or _change_guard(ctx, "show_mode", v,
                                 values[v] == _mem(ctx).show_unadopted):
            return
        _mem(ctx).show_unadopted = values[v]
        self._refresh(ctx)

    # 실측(fiftyone-plugins panel-examples InputsExample/DropdownMenuExample): Property-level
    # on_change 콜백은 바뀐 값을 ctx.params["value"]로만 전달한다 — 브리프 원문의
    # ctx.params["group_field"]/["groups"] 키 가정은 틀렸다(그 키들은 애초에 존재하지 않음).
    # 필드 2개가 같은 시그니처를 공유하므로 어느 쪽이 바뀌었는지 값만으로는 구분 불가 —
    # 필드별로 전용 핸들러를 둔다.
    def on_group_field_change(self, ctx):
        v = ctx.params.get("value")
        if v is None:
            return
        if _change_guard(ctx, "group_field", v, v == _mem(ctx).group_field):
            self._close_picker(ctx, "group_field")   # 같은 값 재선택 — 목록만 닫는다
            return
        _mem(ctx).group_field = v
        # 필드가 바뀌면 이전 필드의 값 선택은 의미가 없다 (그대로 두면 빈 화면이 된다).
        _mem(ctx).groups = ""
        _mem(ctx).picker = "groups"      # 바로 값 고르기로 이어 준다 (이름 규칙 = 컨트롤명)
        self._refresh(ctx)

    def on_groups_change(self, ctx):
        v = ctx.params.get("value")
        if v is None or _change_guard(ctx, "groups", v, v == _mem(ctx).groups):
            return
        _mem(ctx).groups = v
        self._refresh(ctx)

    # Task 12 — 뱅크 버전 드롭다운. Task 9 조사로 확정된 계약과 동일하게 값은
    # ctx.params["value"]로 온다(패널 예제 InputsExample/DropdownMenuExample 실측,
    # 필드명 키 가정 아님).
    def on_bank_version_change(self, ctx):
        v = ctx.params.get("value")
        if v is None:
            return
        if _change_guard(ctx, "bank_version_filter", v,
                         v == _mem(ctx).bank_version_filter):
            self._close_picker(ctx, "bank")   # 같은 값 에코/이중 발화 — 목록만 닫는다
            return
        _mem(ctx).bank_version_filter = v
        _mem(ctx).picker = None
        _mem(ctx).join_field_missing = None
        # 버전 전환 시 stale 선택 정리 (opus F2): 이전 버전 필드 기준 selected_gidx 를
        # 남기면 표·하이라이트가 화면과 다른 버전을 가리키고, dedup 시그니처 탓에 같은
        # 프레임의 새 버전 재조인은 영구히 안 일어난다. 그리드 선택이 살아 있으면 새
        # 버전 기준으로 **즉시 재조인**하고, 없으면 문장 선택을 비운다.
        ids = ctx.selected or []
        if ids and _mem(ctx).rule == "argmax_k1":
            frames_name = ctx.dataset.name if ctx.dataset is not None else FRAMES_DATASET
            _mem(ctx).selected_gidx = frame_ids_to_gidx(
                ids, dataset_name=frames_name, winner_field=_current_winner_field(ctx))
        else:
            _mem(ctx).selected_gidx = []
        self._refresh(ctx)

    def render(self, ctx):
        # ⚠️ **스냅샷으로 읽는다** (코덱스 리뷰 F6): 아래에서 값을 여러 번 따로 읽는 사이
        #    동시 요청의 `_refresh` 가 기억을 하나씩 갈아끼우면 "새 표 + 옛 배너" 처럼 섞인
        #    화면이 나온다 — `_put_fig` 가 한 엔트리로 합쳐 막은 것과 같은 부류다.
        st = _mem(ctx).snapshot()
        _trace(ctx, "render")
        panel = types.Object()
        # 컨트롤 = 드롭다운 4개를 h_stack 한 줄에 (2026-08-10 피드백 ×3: ① 세로 스택이 수직
        # 공간을 잡아먹어 하단 표가 뷰포트 밖으로 밀림, ② 버튼/드롭다운 혼재와 뱅크 버전의
        # 어색한 위치 → 라벨 있는 드롭다운으로 통일, ③ 폭은 글 크기에 맞게 — h_stack이 내용
        # 폭으로 잡아준다). flat + view.space 는 패널 렌더러가 무시(실측), h_stack 중첩의
        # state 바인딩 단절은 _sync_controls 의 중첩 경로 미러링으로 해결 — 그쪽 주석 참고.
        # ⚠️ 목록을 연 동안에는 **그 컨트롤만** 그린다 (2026-09-09 사용자 지적: 목록이
        #    누른 버튼 바로 아래가 아니라 컨트롤 행 전체 아래에 떠 위치가 어긋나 보였다).
        #    한 컨트롤만 남으면 목록이 그 버튼 바로 아래에 전폭으로 붙고, 좁은 칸에 격자를
        #    구겨 넣지 않아도 된다. 고르거나 다시 누르면 원래 행으로 돌아온다.
        def _shown(name):
            return st.picker in (None, name)

        row = panel.h_stack("controls", gap=2, align_y="bottom",
                            columns=CONTROLS_COLUMNS)
        _cur_mode = st.mode or "A"
        if _shown("mode"):
            _btn_choice(row, "mode", f"모드 · {MODE_MEANING.get(_cur_mode, '')}",
                        [(m, m) for m in ("A", "B", "C")], _cur_mode, self.on_mode_change)
        if st.mode == "C":
            # ⚠️ 자유 입력(str)도 enum 과 같은 경로로 패널 상태를 쓴다 — 프리셋 버튼으로 바꿨다.
            #    k 는 거친 손잡이라 프리셋으로 충분하다(옛 자유값이 남아 있으면 함께 보여준다).
            _k = str(st.cluster_k or CLUSTER_K_DEFAULT)
            if _shown("cluster_k"):
                _control_toggle(row, "cluster_k", f"군집 수 k (기본 {CLUSTER_K_DEFAULT})",
                                _k, self.on_toggle_k_picker)
        elif st.mode == "B":
            _gf = st.group_field or "project"
            if _shown("group_field"):
                _control_toggle(row, "group_field", "그룹 필드", _gf,
                                self.on_toggle_field_picker)
            _sel_groups = [g.strip() for g in (st.groups or "").split(",") if g.strip()]
            if _shown("groups"):
                _control_toggle(row, "groups", "그룹들",
                                f"{len(_sel_groups)}개" if _sel_groups else "미선택",
                                self.on_toggle_groups_picker)
        elif st.prompts_available:
            # 표기는 "topk" (사용자 요청 — 팀 용어), 내부 값·조인 필드는 argmax_k1 유지
            # (winner_gidx_* 프로듀서/원장 식별자와의 일관성). 정확한 정의는 배너가 설명.
            if _shown("rule"):
                _btn_choice(row, "rule", "규칙",
                            [("argmax_k1", "topk"), ("dist_iou", "dist_iou")],
                            st.rule or "argmax_k1", self.on_rule_change)
            # 색칠 축 — 규칙과 독립. wave 화면에서도 클래스 범례를 볼 수 있다 (2026-08-12 요청).
            # ⚠️ **enum 이 아니라 버튼 피커다** (2026-09-09): 드롭다운에서 값을 고르면 App 의
            #    스키마 렌더러가 패널 상태를 써서(우리 쓰기 0 이어도) 사용자의 사이드바 필터가
            #    지워진다 — 번들 실측상 값을 들고도 상태를 안 쓰는 컴포넌트는 Button/Table 뿐.
            _cur_color = st.color_by or (COLOR_BY_CATEGORY if st.rule == "argmax_k1"
                                         else COLOR_BY_WAVE_ROLE)
            _color_opts = list(COLOR_BY_LABELS.items())
            if _shown("color"):
                if _fits_inline(_color_opts):
                    _btn_choice(row, "color", "색칠", _color_opts, _cur_color,
                                self.on_color_change)
                else:
                    _control_toggle(row, "color", "색칠",
                                    COLOR_BY_LABELS.get(_cur_color, _cur_color),
                                    self.on_toggle_color_picker)
            if st.rule == "argmax_k1" and _shown("show_mode"):
                # 표시(전체/채택만)는 argmax 전용 — dist_iou는 전 문장이 wave 분포에
                # 참여하므로 미채택 개념 자체가 없다 (build_mode_a의 2026-08-10 정정).
                _btn_choice(row, "show_mode", "표시",
                            [(SHOW_ALL_LABEL, "전체"), (SHOW_ADOPTED_LABEL, "채택만")],
                            SHOW_ALL_LABEL if st.show_unadopted else SHOW_ADOPTED_LABEL,
                            self.on_show_change)
            # 뱅크 버전 선택기: "전체" + 실제 프롬프트 데이터셋의 distinct bank_version.
            # 2026-08-11 리빌드로 sourcei-prompts/source-h-prompts 둘 다 v1.0.8.0+v1.0.8.4 —
            # 코드는 값 개수에 의존하지 않는다 (버전 추가 = promptmap 재빌드만).
            # ⚠️ 색칠과 같은 이유로 버튼 피커 (버전이 31개라 목록은 격자로 편다).
            if _shown("bank"):
                _control_toggle(row, "bank", "뱅크 버전",
                                st.bank_version_filter or ALL_VERSIONS_LABEL,
                                self.on_toggle_bank_picker)
            n_sel = len(st.selected_gidx or [])
            # 선택이 있거나, **패널 상태는 비었는데 우리 뷰 칩만 남은 경우**(F5·워크스페이스
            # 전환 후)에도 노출한다 — 그때 버튼이 숨으면 사용자가 패널에서 뺄 방법이 없다.
            if n_sel or _has_our_stage(ctx):
                row.btn("clear_selection",
                        label=("✕ 선택 해제" + (f" ({n_sel})" if n_sel else " (뷰만)")),
                        on_click=self.on_clear_selection)
        if st.picker == "group_field" and st.mode == "B":
            _picker_grid(panel, "group_field",
                         [(f, f) for f in group_field_candidates(ctx)],
                         st.group_field or "project", self.on_group_field_change)
        if st.picker == "groups" and st.mode == "B":
            _vals, _capped = group_value_candidates(ctx, st.group_field or "project")
            _picker_grid(panel, "groups", [(v, v) for v in _vals],
                         {g.strip() for g in (st.groups or "").split(",") if g.strip()},
                         self.on_group_toggle,
                         note=(f"*상위 {len(_vals)}개*" if _capped else None))
        if st.picker == "cluster_k" and st.mode == "C":
            _ks = sorted({str(x) for x in (4, 6, 8, 12, 16, 24, 32)} | {_k},
                         key=lambda t: (len(t), t))
            _picker_grid(panel, "cluster_k",
                         [(k, k) for k in _ks], _k, self.on_cluster_k_change)
        if st.picker == "color" and st.mode == "A" and st.prompts_available:
            _picker_grid(panel, "color", list(COLOR_BY_LABELS.items()),
                         _cur_color, self.on_color_change)
        if st.picker == "bank" and st.mode == "A" and st.prompts_available:
            # 버전 라벨(v1.0.8.4)은 짧아 6열이 읽기 좋다 — 31개를 4열로 펴면 8줄이 된다.
            _picker_grid(panel, "bank",
                         [(v, v) for v in [ALL_VERSIONS_LABEL, *(st.bank_versions or [])]],
                         st.bank_version_filter or ALL_VERSIONS_LABEL,
                         self.on_bank_version_change, columns=6)
        # 프롬프트 짝이 없을 땐 모드 드롭다운만 남긴다 — 안내는 아래 배너가 싣는다
        # (구: 컨트롤 행 안의 md. 그리드 셀 하나를 차지해 드롭다운과 높이가 어긋났다).
        # data=... (Task 11 "클릭해야 나온다" 방어선 — 2차 안전망):
        # 실사용 버그의 **1차·확정 원인은 이 파일이 아니라 fiftyone_app_setup.py 쪽**이었다 —
        # cmd_workspace_compare()가 만드는 Space에 active_child를 안 채워서, 워크스페이스
        # 로드시 이 패널의 on_load 오퍼레이터 자체가 한 번도 실행되지 않았다(네트워크 로그로
        # 확인: load_workspace 이후 이 패널의 /operators/execute가 전혀 안 나가다가 패널 탭을
        # 클릭한 순간에야 첫 execute 발생). active_child 수정으로 그 근본 원인은 해결됐다.
        # 다만 on_load가 정상 발화하는 경우에도 잠재 경합이 하나 더 있다(실측, docker-analysis-1
        # 배포본 index-CFYL-qQX.js 역공학): set_data()는 patch_panel_data 오퍼레이터를 거쳐
        # `setTimeout(fn, 1)`로, render()가 만든 스키마는 show_panel_output 오퍼레이터를 거쳐
        # *또 다른* `setTimeout(fn, 1)`로 — 서로 다른 타이머로 큐잉되어 도착 순서가 항상
        # 보장되진 않는다(클릭 이후 재렌더는 패널이 이미 마운트된 상태라 이 경합이 안 걸림 —
        # Task 8 실측과 합치). PlotlyView는 `data ?? schema.view.data`로 폴백하므로(App 번들
        # PlotlyView 컴포넌트, `wo=mergeData(mt||Lt?.view?.data,...)`), show_panel_output
        # 스키마에 data를 직접 구워 넣으면 이 잠재 경합과도 무관하게 항상 채워진다.
        # 사용자 피드백 라운드(2026-08-07)부터 set_data는 아예 쓰지 않는다 — patch 딥머지가
        # 줄어든 배열을 못 지우는 문제까지 겹쳐, 스키마 data가 유일한 갱신 경로다(_refresh 주석).
        # height(사용자 피드백): 고정 800px는 큰 화면에서 아래 공간을 놀리고 작은 화면에선
        # 스크롤을 만든다. PlotlyView는 이 kwarg를 plotly div의 style.height로 그대로 쓰므로
        # (App 번들: bo=Yn?.height||"100%") vh 단위가 동작한다 — 뷰포트가 크면 크게, 작으면
        # 작게, 단독/분할(가로 분할이라 세로 공간 동일) 모두 자동 반응. 360px = 상단 컨트롤
        # +탭바+하단 표 예산, 480px = 최소 보장. config.responsive로 창 리사이즈도 추적.
        # 프로퍼티 키 "scatter_v2": 옛 배포가 set_data("scatter")로 세션 저장소에 영속시킨
        # patched data가 리로드 후에도 스키마 data를 가리는 문제(위 주석)의 결정적 우회 —
        # 키가 다르면 저장된 patch가 아예 매칭되지 않는다. data.clear()는 잔재 정리용 보조.
        # displayModeBar 상시 표시: pan↔lasso↔box 전환 버튼 (g/s 단축키 대체 — 단축키는
        # App 번들 전용이라 Python 패널에서 불가, on_plot_selected 주석 참고). 전환은
        # 클라이언트 즉시(서버 왕복 없음), 선택하면 on_selected로 문장→프레임 조인.
        # 높이 예산(사용자 피드백 2026-08-10): 구 360px 예산은 표를 뷰포트 밖으로 밀었다 —
        # 500px = 탭바+컨트롤 한 줄+하단 표(maxHeight 240px, 아래) 몫. 표가 항상 같이 보인다.
        # 배너: plotly title 에서 빼내 여기에 (build_mode_a 주석 참고). 마크다운이라
        # 폭에 맞춰 접히고 modebar 와 겹치지 않는다. 빈 문자열이면 아예 만들지 않는다.
        # ⚠️ figure 를 **배너보다 먼저** 만든다 (2026-09-04): 뷰가 바뀐 렌더는 여기서
        #    재구성되는데, 그 배너를 같은 렌더에 실어야 패널 상태를 한 글자도 쓰지 않고
        #    최신 표기를 낼 수 있다 (상태를 쓰면 setSpaces → DatasetPageQuery 가 돌아
        #    사용자의 사이드바 필터가 지워진다 — on_change_view 참고).
        # ⚠️ 진위(truthiness) 아니라 **키 존재**로 고른다: 빈 문자열 배너("모드 B 그룹 미입력"
        #    등)가 정당한 값인데 `or` 로 폴백하면 **직전 뷰의 배너**가 되살아난다.
        # ⚠️ **조회는 한 번**이다: data/banner/layout 을 따로 꺼내면 그 사이 다른 요청이
        #    엔트리를 갈아끼워 "새 그림 + 옛 배너" 조합이 나온다 (codex 리뷰, 2026-09-07).
        _entry = _get_entry(ctx)
        banner_txt = _entry["banner"] if _entry is not None else st.banner
        layout = _entry["layout"] if _entry is not None else st.layout
        fig_data = _entry["data"] if _entry is not None else None
        if fig_data is None and st.mode not in ("B", "C") \
                and st.prompts_available is not False:
            # 프로세스 재시작/캐시 축출로 서버측 fig 가 없는 경우 — 컨트롤 상태에서 결정론
            # 재구성 (~0.1s, 번들 캐시 warm 기준). 모드 B 는 이 폴백 없이 다음 상호작용에
            # 맡긴다 (그룹 재입력이 자연 경로). 실패하면 빈 산점도 — on_load 가 곧 채운다.
            try:
                b = load_prompt_bundle(_prompts_dataset_name(ctx))
                fig = build_mode_a(
                    b, rule=st.rule or "argmax_k1",
                    show_unadopted=st.show_unadopted,
                    selected_gidx=set(st.selected_gidx or []),
                    bank_version_filter=(st.bank_version_filter
                                         or ALL_VERSIONS_LABEL),
                    color_by=st.color_by,
                    view_winner=view_winner_gidx(ctx))
                fig_data = fig["data"]
                # 데이터셋 전환 직후엔 배너도 클라이언트가 실어 온 **옛 데이터셋 문장**이다
                # (state 는 요청마다 실려 온다). 방금 새로 만든 배너로 덮어 stale 표기를 없앤다
                # — 모집단 숫자가 곧 데이터셋 게이트라 이게 틀리면 오독으로 이어진다.
                # ⚠️ **상태에 쓰지 않고 이번 렌더에만 싣는다** (2026-09-04): render 에서
                #    state 를 쓰면 setSpaces → DatasetPageQuery 로 사이드바 필터가 날아간다.
                layout = fig["layout"]
                _put_fig(ctx, fig_data, banner=fig.get("banner", ""), layout=layout)
                banner_txt = fig.get("banner", "")
            except Exception:
                fig_data = None
        # 배너는 **그릴 게 없을 때만** (형제 패널과 같은 규칙, 2026-09-09 사용자 요청).
        if banner_txt and not fig_data:
            panel.md(banner_txt, name="banner_md")
        panel.plot("scatter_v2", data=fig_data or [],
                   layout=layout or {},
                   # 500 → 560: 컨트롤이 2행(columns=3)까지 접히고 배너 줄이 추가된 몫.
                   height="max(400px, calc(100vh - 560px))",
                   config={"responsive": True, "displayModeBar": True},
                   on_click=self.on_plot_click,
                   on_selected=self.on_plot_selected,
                   on_double_click=self.on_plot_double_click)
        if st.mode != "B":
            # 표 내부 스크롤: 문장이 많아도(lasso 다중선택) 패널 전체가 아니라 표 안에서만
            # 스크롤한다 — componentsProps.container는 SchemaIO 표준 래퍼 prop(App 번들
            # getComponentProps(ctx,"container") 실측)이라 sx가 그대로 먹는다.
            # `overflowX` 도 준다: 클래스 열이 붙어 9열이 되면서 좁은 패널에서 가로로
            # 넘친다 — 의미 있는 열을 압축하는 대신 좌우 스크롤로 받는다.
            panel.md(_rows_to_markdown(st.top_table, st.join_field_missing,
                                       total=st.sel_total),
                     name="table_md", label="선택 프레임의 승자 문장",
                     componentsProps={"container": {
                         "sx": {"maxHeight": "180px", "overflowY": "auto",
                                "overflowX": "auto"}}})
            # 버전별 1~5위 — 저장 필드에 없는 정보(2~5위)라 즉석 KNN 결과다.
            # 버전 필터가 특정 버전이면 그 버전만, "전체" 면 전 버전을 버전별로 나눠 보여준다.
            _tk_src = st.topk_src
            if st.topk_multi:
                _tk_src = f"{_tk_src} · {st.topk_multi}"
            panel.md(topk_to_markdown(st.frame_topk or [],
                                      st.bank_version_filter, _tk_src,
                                      ref_class=st.topk_ref_class),
                     name="topk_md", label=f"버전별 상위 {PDB_TOPK} 후보 (버전별 1→{PDB_TOPK}위)",
                     componentsProps={"container": {
                         "sx": {"maxHeight": "460px", "overflowY": "auto",
                                "overflowX": "auto"}}})
        return types.Property(panel, view=types.GridView())


class PiaDefaultWorkspace(foo.Operator):
    """데이터셋 열릴 때 'compare' 워크스페이스를 기본으로 로드 (2026-08-14 요청).

    App 은 데이터셋 전환 시 spaces 를 항상 default_workspace_factory()(Samples 단독)로
    리셋한다 (fiftyone/server/mutation.py set_dataset 실측) — 서버 코드 패치 대신
    on_dataset_open 오퍼레이터로 뒤집는다. 워크스페이스 구성 자체는
    fiftyone_app_setup.py workspace-compare 가 데이터셋별로 저장한다:
    프롬프트 짝 있으면 반반 스택(Samples/Embeddings | Prompt Compare),
    "-prompts"/짝 없음이면 Samples | Embeddings(emb_viz) — 그런 데이터셋에서
    Prompt Compare 는 "짝 없음" 배너만 그리므로 임베딩 패널이 기본이어야 한다.
    """

    @property
    def config(self):
        return foo.OperatorConfig(
            name="user_default_workspace",
            label="Open 'compare' workspace on dataset open",
            unlisted=True,
            on_dataset_open=True,
        )

    def execute(self, ctx):
        ds = getattr(ctx, "dataset", None)
        try:
            if ds is not None and "compare" in ds.list_workspaces():
                ctx.ops.set_spaces(name="compare")
        except Exception:
            pass   # best-effort UX — 실패 시 App 기본 화면(Samples)이면 충분
        return {}


def register(p):
    p.register(PromptComparePanel)
    p.register(PiaDefaultWorkspace)


def selftest():
    """조인 불변식 3개 (스펙 §5.6) + 데이터 계층 검증. App 불필요.

    FiftyOne 업그레이드 게이트로도 쓴다. 셋째가 깨지면 producer drift 의심.
    """
    import numpy as np

    pdb_selftest()          # prompt DB 해석 계층 (DB 없이 도는 순수부)
    _MEM.clear()            # 서버 기억은 프로세스 전역 — 테스트는 콜드에서 시작한다

    # ── 사이드바 필터 보존 계약 (2026-09-04) ────────────────────────────────────
    # ⚠️ 이 블록은 **selftest 맨 앞**에 둔다: 아래쪽 `조인 필드 없음` 단언이 선행 결함으로
    #    실패해 그 뒤 92개 단언이 아예 실행되지 않는다(2026-09-07 실측). 뒤에 붙이면 죽은 코드다.
    class _KeyState:
        def __init__(self, mode):
            self._d = {"mode": mode}
        def __getattr__(self, k):
            if k.startswith("_"):
                raise AttributeError(k)
            return self._d.get(k)
        def __setattr__(self, k, v):
            if k.startswith("_"):
                super().__setattr__(k, v)
            else:
                self._d[k] = v
        def get(self, k, default=None):
            return self._d.get(k, default)
        def set(self, k, v):
            self._d[k] = v

    class _KeyCtx:
        def __init__(self, mode, filters):
            self.panel = type("P", (), {})()
            self.panel.state = _KeyState(mode)
            self.panel.data = type("D", (), {"clear": lambda self: None})()
            # 전용 데이터셋 이름 — 서버 기억이 (패널, 데이터셋) 키라 이걸로 다른 테스트와
            # 격리된다. 모드는 그 기억에 넣는다 (`_fig_key` 가 거기서 읽는다).
            self.dataset = type("D", (), {"name": "__figkey__"})()
            self.params = {"panel_id": "p1"}
            self.request_params = {"filters": filters}
            _mem(self).mode = mode

    _f1 = {"ground_truth.label": {"values": ["fire"]}}
    _f2 = {"ground_truth.label": {"values": ["smoke"]}}
    # 모드 A 는 `view_winner_gidx(ctx)` 로 뷰에 의존한다 → 뷰가 바뀌면 키도 바뀌어야
    # render 폴백이 새 뷰로 다시 그린다.
    assert _fig_key(_KeyCtx("A", _f1)) != _fig_key(_KeyCtx("A", _f2)), \
        "회귀: 모드 A 의 fig 캐시 키에 뷰 지문이 없다 — 필터를 걸어도 옛 figure 가 나온다"
    # ⛔ 모드 B/C 는 반대다. 둘의 figure 는 뷰를 인자로 받지도 않는데(build_mode_b/c),
    #    render 의 재구성 폴백은 `mode not in ("B","C")` 로 막혀 있다 → 키가 뷰 때문에
    #    미스가 되면 아무도 못 메워 `data=fig_data or []` 로 **산점도가 통째로 빈다**.
    for _m in ("B", "C"):
        assert _fig_key(_KeyCtx(_m, _f1)) == _fig_key(_KeyCtx(_m, _f2)), \
            f"회귀: 모드 {_m} 키가 뷰에 반응한다 — 뷰가 바뀌면 산점도가 빈 채로 남는다"
    # 모드끼리는 서로 다른 키여야 한다 — 같으면 동시 요청에서 남의 모드 figure 를 집어간다.
    assert len({_fig_key(_KeyCtx(_m, _f1)) for _m in ("A", "B", "C")}) == 3, \
        "회귀: 모드가 fig 캐시 키에 없다 — B 가 C 의 figure 를 받을 수 있다"
    assert _fig_key(_KeyCtx(None, _f1)) == _fig_key(_KeyCtx("A", _f1)), \
        "회귀: mode 미설정이 기본 A 로 수렴하지 않는다"
    # 뷰 변경 훅은 패널 상태를 한 글자도 쓰면 안 된다 (setSpaces→DatasetPageQuery 로
    # 사용자의 사이드바 필터가 날아간다).
    _vc = _KeyCtx("A", _f1)
    _snap = dict(_vc.panel.state._d)
    PromptComparePanel().on_change_view(_vc)
    assert dict(_vc.panel.state._d) == _snap, \
        "회귀: on_change_view 가 패널 상태를 썼다 — 사이드바 필터가 지워진다"
    b = load_prompt_bundle()
    frames = fo.load_dataset(FRAMES_DATASET)

    # 좌표↔메타 정렬 (2026-08-19): 길이가 어긋나면 크래시(길면) 또는 **조용한 오귀속**(짧으면).
    for f in META_FIELDS:
        if b.get(f) is not None:
            assert len(b[f]) == len(b["xy"]), (f, len(b[f]), len(b["xy"]))

    # ── 문장 정본 = DB (2026-08-19) ──
    # 데이터셋 `text` 는 npz 파생 폴백이므로, DB 가 살아 있으면 표시 문장이 그쪽에서 와야
    # 한다. DB 가 없는 환경(오프라인)에서는 폴백이 정상 동작이므로 강제하지 않고,
    # **어느 소스를 썼는지 배너가 반드시 말한다**는 계약만 고정한다.
    probe_idx = list(range(min(200, len(b["gidx"]))))
    ptexts, pmeta = _resolve_text(b, probe_idx)
    assert len(ptexts) == len(probe_idx)
    assert "출처" in pdb_note(pmeta) and "\n" not in pdb_note(pmeta)
    if pmeta["db_rows"]:
        assert PDB_SRC_DB in pdb_note(pmeta), pdb_note(pmeta)
        # DB 로 해석된 행에 자리표시자가 남으면 안 된다 (DB 는 NOT NULL + 빈 문자열 금지)
        assert not any(str(t).lstrip().startswith(PLACEHOLDER_PREFIX)
                       for t, v in zip(ptexts, (b["bank_version"][i] for i in probe_idx))
                       if str(v) in pmeta["db_versions"]), "DB 해석분에 자리표시자"
    # 게이트 ② 분모는 **데이터셋 전체** 행 수여야 한다 (표시분으로 재면 전부 폴백)
    counts = _bundle_ver_counts(b)
    assert sum(counts.values()) == int(np.count_nonzero(
        [v is not None for v in b["bank_version"]])), counts

    # 다중 뱅크 버전 (2026-08-11 리빌드): 버전별 문장 행 마스크 — 이하 불변식은 버전 단위다
    bv_all = b.get("bank_version")
    versions = sorted({str(v) for v in bv_all if v is not None}) if bv_all is not None else []
    assert versions, "bank_version 없음 — 데이터 계층 회귀"
    vmasks = {v: np.asarray([str(x) == v for x in bv_all]) for v in versions}

    # 불변식 1: 완전분할 — **버전별로** 승수 총합 = 프레임 수 (각 버전이 전 프레임을 분할)
    for v in versions:
        assert int(np.sum(b["wins"][vmasks[v]])) == frames.count(), \
            (v, int(np.sum(b["wins"][vmasks[v]])), frames.count())
    # 불변식 2: 프레임의 승자 gidx ⊆ **그 버전** 문장 gidx (opus F8: 전역 합집합과 비교하면
    # v084 컬럼에 v080 오프셋 값이 들어가도(재백필 누락·태그 충돌·배치 순서 변경) 초록이 된다)
    gidx_all = set(int(g) for g in b["gidx"])
    frames_schema = frames.get_field_schema()
    for v in versions:
        f = version_to_winner_field(v)
        if f not in frames_schema:
            continue
        winner = set(frames.values(f))
        winner.discard(None)
        gidx_v = {int(g) for g in b["gidx"][vmasks[v]]}
        assert winner <= gidx_v, \
            f"{f} 승자 gidx가 {v} 문장 밖 — 오프셋/재백필 불일치 (밖: {sorted(winner - gidx_v)[:3]})"
    # 불변식 3: 채택 ⟺ wins>0
    # ── 뷰 기준 채택 (2026-08-20) ──
    #    프레임 뷰가 좁아지면 승자 문장 집합이 달라진다. 실측: 전역 17,230 vs
    #    source-e(프레임 2,477) 86. 반영 안 하면 전역 승자를 "이 뷰의 승자" 로 보여준다.
    _g_all = sorted({int(g) for g in b["gidx"]})
    _some = set(_g_all[:3])
    _f_glob = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx=set())
    _f_view = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx=set(),
                           view_winner=_some)
    _ad_glob = sum(len(t["x"]) for t in _f_glob["data"][1:-1])
    _ad_view = sum(len(t["x"]) for t in _f_view["data"][1:-1])
    assert _ad_view <= len(_some), (_ad_view, len(_some))     # 뷰 승자 밖은 채택 아님
    assert _ad_view != _ad_glob, "뷰 승자를 줬는데 전역과 같다 — 주입이 무시됐다"
    assert "채택 = 현재 뷰 기준" in _f_view["banner"]
    assert "채택 = 현재 뷰 기준" not in _f_glob["banner"]
    # 빈 집합(뷰에 승자 0) 과 None(필터 없음) 은 다르다
    _f_zero = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx=set(),
                           view_winner=set())
    assert sum(len(t["x"]) for t in _f_zero["data"][1:-1]) == 0
    # 캐시된 번들을 건드리지 않았는가 (다른 렌더가 같은 dict 를 본다)
    assert int(b["adopted"].astype(bool).sum()) == int(b["adopted"].astype(bool).sum())
    assert _ad_glob == sum(len(t["x"]) for t in build_mode_a(
        b, rule="argmax_k1", show_unadopted=True, selected_gidx=set())["data"][1:-1]), \
        "회귀: view_winner 호출이 번들의 전역 adopted 를 오염시켰다"

    assert all((w > 0) == bool(a) for w, a in zip(b["wins"], b["adopted"]))
    # 불변식 4 (codex 3차 리뷰): gidx 전역 유일 — row_of 딕셔너리/np.where 단일행 전제.
    # 다중 버전은 GIDX_OFFSET(prompt_geometry) 오프셋으로 유일성을 보장한다.
    assert len(b["gidx"]) == len(gidx_all), "gidx 전역 유일성 붕괴"

    # ── gidx 오프셋 세대 보정 (2026-08-20) ──
    #    프레임 필드와 `-prompts` 의 오프셋 세대가 어긋나도 조인이 성립해야 한다.
    #    산술 계약: shift = (문장블록 − 프레임블록) × OFFSET, 왕복은 항등.
    for _fb, _pb in ((0, 18), (18, 18), (18, 0), (3, 21)):
        _s = (_pb - _fb) * PDB_GIDX_OFFSET
        for _loc in (0, 451, 12_479):
            _frame_key = _loc + _fb * PDB_GIDX_OFFSET
            assert _frame_key + _s == _loc + _pb * PDB_GIDX_OFFSET
            assert (_frame_key + _s) % PDB_GIDX_OFFSET == _loc, "로컬 인덱스가 보존돼야 한다"
            assert (_frame_key + _s) - _s == _frame_key, "왕복 항등"
    assert gidx_shift("__user_no_such_ds__", WINNER_FIELD) == 0, "없는 데이터셋은 보정 0"
    assert gidx_shift(FRAMES_DATASET, "winner_gidx_v999") == 0, "짝 없는 필드는 보정 0"
    # 라이브 계약: 보정 후 프레임 gidx 가 그 버전 문장의 블록에 들어가야 한다.
    for _ds, _fld, _ver in ((FRAMES_DATASET, WINNER_FIELD, "v1.0.8.0"),
                            ("frames", "winner_gidx_v1080", "v1.0.8.0")):
        if not (fo.dataset_exists(_ds) and fo.dataset_exists(f"{_ds}-prompts")):
            continue
        _fr = fo.load_dataset(_ds)
        if _fld not in _fr.get_field_schema():
            continue
        _s = gidx_shift(_ds, _fld)
        _lo, _hi = _fr.bounds(_fld)
        _pv = fo.load_dataset(f"{_ds}-prompts").match(
            fo.ViewField("bank_version.label") == _ver)
        _plo, _phi = _pv.bounds("gidx")
        if _lo is None or _plo is None:
            continue
        _want = int(_plo) // PDB_GIDX_OFFSET
        assert (int(_lo) + _s) // PDB_GIDX_OFFSET == _want, \
            f"{_ds}.{_fld}: 보정 후 블록 {(int(_lo)+_s)//PDB_GIDX_OFFSET} ≠ 문장 블록 {_want}"
        assert (int(_hi) + _s) // PDB_GIDX_OFFSET == _want, f"{_ds}.{_fld}: 상한이 블록을 넘음"
        print(f"  gidx 세대 보정 {_ds}.{_fld}: shift={_s:+,} "
              f"(프레임 블록 {int(_lo)//PDB_GIDX_OFFSET} → 문장 블록 {_want})")

    # 조인 왕복: 임의 채택 문장 → 프레임들 → 도로 그 문장. 다중 버전이라 왕복은
    # **그 문장 버전의 winner 필드**로 해야 한다 (버전 혼합 시 gidx 오프셋이 다른 필드와
    # 안 맞는 게 정상 — on_plot_click도 per-문장 버전으로 조인한다).
    # g_ver 는 "MAX_POINTS 이하 크기 버전" 중에서 고른다 (opus F9): 정확 수량 계약들이
    # "버전 필터를 걸면 서브샘플이 없다"를 전제하는데, 29버전 리빌드 후 일부 버전
    # (v1.0.13.2=79,842행 등)은 단독으로도 MAX_POINTS 를 넘어 그 전제가 무너진다 —
    # 큰 버전이 최대 wins 를 갖는 순간 selftest 가 중간에 죽어 뒤쪽 계약 전체가 미검증.
    small_ver = {v for v in versions if int(vmasks[v].sum()) <= MAX_POINTS}
    small_rows = np.flatnonzero(np.asarray([str(x) in small_ver for x in bv_all])) \
        if bv_all is not None else np.arange(len(b["wins"]))
    assert len(small_rows), "MAX_POINTS 이하 버전이 하나도 없음 — 합성 픽스처 필요"
    gi0 = int(small_rows[np.argmax(b["wins"][small_rows])])
    g = int(b["gidx"][gi0])
    g_ver = str(bv_all[gi0]) if bv_all is not None else None
    g_field = version_to_winner_field(g_ver) if g_ver else WINNER_FIELD
    ids = gidx_to_frame_ids(g, winner_field=g_field)
    assert ids and set(frame_ids_to_gidx(ids, winner_field=g_field)) == {g}

    # 일괄 조인(lasso 다중선택 경로): 단건 조인의 합집합과 동일해야 한다 — 같은 버전 내에서
    vm0 = vmasks[g_ver] if g_ver else np.ones(len(b["wins"]), dtype=bool)
    vrows = np.flatnonzero(vm0)
    top2 = [int(b["gidx"][i]) for i in vrows[np.argsort(b["wins"][vrows])[-2:]]]
    assert set(gidxes_to_frame_ids(top2, winner_field=g_field)) == \
        set(gidx_to_frame_ids(top2[0], winner_field=g_field)) | \
        set(gidx_to_frame_ids(top2[1], winner_field=g_field))

    # 회색 충돌 금지 (사용자 피드백 2026-08-10): 같은 화면에 무채색 2종이 공존하면 안 된다.
    # argmax 화면 = 미채택(GREY)과 공존하는 CLASS_COLORS는 전부 유채색.
    # dist_iou 화면 = 미채택 trace가 없으므로(전 문장 wave 참여 정정) 중간=회색 허용,
    # 유익/유해만 유채색이면 된다.
    def _greyish(c):
        r, gr, bl = (int(c[i:i + 2], 16) for i in (1, 3, 5))
        return max(r, gr, bl) - min(r, gr, bl) < 30
    for c in CLASS_COLORS.values():
        assert not _greyish(c), f"CLASS_COLORS에 회색 계열 색 {c} — 미채택 GREY와 충돌"
    for role, c in WAVE_ROLE_COLORS.items():
        if role != "중간":
            assert not _greyish(c), f"wave 강조 팔레트에 회색 계열 색 {c}"
    assert GREY not in set(CLASS_COLORS.values()) | set(WAVE_ROLE_COLORS.values())
    assert len(set(CLASS_COLORS.values())) == len(CLASS_COLORS)          # 팔레트 내 중복 금지
    assert len(set(WAVE_ROLE_COLORS.values())) == len(WAVE_ROLE_COLORS)

    # 배너 정정 고정 (2026-08-10): 제품 판정은 분포 IoU — stale "topk_vote" 문구 부활 금지
    assert "분포 IoU" in BANNER_RULE and "topk_vote" not in BANNER_RULE

    # 층화 서브샘플: 상한 준수 + 전 클래스 보존
    labs = ["a"] * 100 + ["b"] * 10
    idx = stratified_subsample(labs, 20)
    assert len(idx) <= 20 and {labs[i] for i in idx} == {"a", "b"}

    # 모드 A figure: 규칙별 계약 — trace 구조 [0]미채택 [1..k]채택(그룹별) [-1]선택.
    # 수량·이름의 정확 계약은 **서브샘플이 없는 조건**에서 검증한다: 2버전 28,605행 >
    # MAX_POINTS 라 전체 뷰는 층화 서브샘플이 걸린다 — g와 같은 버전 필터(12,480행)로 고정.
    fig = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx={g},
                       bank_version_filter=g_ver)
    assert all(t["type"] == "scattergl" for t in fig["data"])           # scattergl 강제
    n_shown = sum(len(t["x"]) for t in fig["data"][:-1])
    assert n_shown == min(int(vmasks[g_ver].sum()), MAX_POINTS)          # 버전 내 전체 표시

    # 서브샘플 채택 보존 계약 (2026-08-12): 전체 뷰(60만 행 > MAX_POINTS)에서도 채택 점은
    # 전수 표시돼야 한다 — category 층화만 걸면 채택 ~5,600점이 3%로 뭉개지던 문제의 가드.
    if len(b["gidx"]) > MAX_POINTS:
        fig_sub = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx=set())
        assert sum(len(t["x"]) for t in fig_sub["data"][1:-1]) == \
            int(b["adopted"].astype(bool).sum()), "회귀: 서브샘플이 채택 점을 떨어뜨림"
    assert BANNER_RULE in fig["banner"]                 # 규칙 배너
    # 반응형 height 계약: layout에 height 고정 금지 — 실높이는 render()의 view height
    # (vh 기반 style)가 정한다. 고정값이 부활하면 큰 화면에서 아래 공간이 다시 논다.
    assert "height" not in fig["layout"] and fig["layout"]["autosize"] is True
    # 선택 하이라이트는 다크 배경에서 보이는 색이어야 한다 (#000000 회귀 방지)
    assert fig["data"][-1]["marker"]["color"] == HILITE
    # 하이라이트는 **한 trace** 여야 한다 — 늘리면 `data[:-1]` 가정이 곳곳에서 깨진다
    assert fig["data"][-1]["marker"]["line"]["width"] >= 3, "회귀: 하이라이트 링이 얇다"
    assert HILITE not in set(CLASS_COLORS.values()), "회귀: 하이라이트가 클래스 색과 충돌"
    # 범례 회귀 가드 (2026-08-10 fix): 채택은 클래스별 trace — 색은 단일 문자열이어야 한다.
    # per-point 색 배열이 부활하면 범례에 클래스→색 매핑이 다시 사라진다(파랑 normal이
    # 화면에 있어도 범례엔 주황 글리프 하나뿐이던 버그).
    adopted_traces = fig["data"][1:-1]
    assert adopted_traces, "채택 trace 0개"
    assert all(isinstance(t["marker"]["color"], str) for t in adopted_traces), \
        "회귀: 채택 trace가 per-point 색 배열 — 범례에 클래스 매핑이 안 나온다"
    adopted_ver = b["adopted"].astype(bool) & vmasks[g_ver]
    assert sum(len(t["x"]) for t in adopted_traces) == int(adopted_ver.sum())
    cats_adopted = {str(c) for c, a in zip(b["category"], adopted_ver) if a}
    assert {t["name"].rsplit(" ", 1)[0] for t in adopted_traces} == cats_adopted, \
        "범례 이름(<클래스> <개수>)이 채택 클래스 집합과 불일치"
    assert all(t["marker"]["color"] == CLASS_COLORS.get(t["name"].rsplit(" ", 1)[0], "#999999")
               for t in adopted_traces)                                  # 범례 글리프 색 = 팔레트 색
    # dist_iou (2026-08-10 정정): 전 문장이 wave 분포에 참여 — 미채택 회색 trace 금지,
    # 전 12,480점이 wave_role 그룹 trace로 나뉜다 (adopted 마스크 사용 = 회귀).
    fig_w = build_mode_a(b, rule="dist_iou", show_unadopted=True, selected_gidx=set())
    assert BANNER_WAVE_NOCLICK in fig_w["banner"]       # 귀속 없음 안내
    w_traces = fig_w["data"][:-1]
    n_w = sum(len(t["x"]) for t in w_traces)
    if len(b["gidx"]) <= MAX_POINTS:
        # 정렬 붕괴 버전(벡터 귀속이 틀린 것)은 의도적으로 제외된다 — **그만큼만** 빠져야
        # 하고 그 이상 빠지면 adopted 마스크 부활 회귀다. 제외량은 fig 가 구조화해 준다
        # (banner 는 사람용 문장이라 파싱 금지).
        assert n_w == len(b["gidx"]) - fig_w["dropped_n"], \
            (n_w, len(b["gidx"]), fig_w["dropped_n"])
        if fig_w["dropped_n"]:
            assert fig_w["dropped_versions"], "제외했는데 버전 목록이 비었다"
    else:
        # 서브샘플: 채택 전수 보존 + 미채택 층화(반올림 미달 허용) ≈ MAX_POINTS
        assert MAX_POINTS * 0.98 <= n_w <= MAX_POINTS, n_w
    assert not any("미채택" in t["name"] for t in w_traces), \
        "회귀: dist_iou에 미채택 trace — wave에는 미채택 개념이 없다"
    assert all(isinstance(t["marker"]["color"], str) for t in w_traces)
    assert any(t["marker"]["color"] != "#999999" for t in w_traces), \
        "dist_iou trace 전체 회색 — wave_role 색 매핑 누락 의심"
    w_names = {t["name"].rsplit(" ", 1)[0] for t in w_traces}
    assert {"유익 상위10%", "유해 하위10%", "중간"} <= w_names
    fig_h = build_mode_a(b, rule="argmax_k1", show_unadopted=False, selected_gidx=set(),
                         bank_version_filter=g_ver)   # 수량 정확 검증 — 서브샘플 회피
    # 숨김 = visible:False (빈 배열 아님 — 클라이언트 patch 딥머지가 옛 점을 못 지운다).
    # 배열 길이는 전체 유지, 플래그만 뒤집혀야 한다.
    assert fig_h["data"][0]["visible"] is False
    assert len(fig_h["data"][0]["x"]) == int((~b["adopted"].astype(bool) & vmasks[g_ver]).sum())
    assert sum(len(t["x"]) for t in fig_h["data"][1:-1]) == int(adopted_ver.sum())
    assert "표시: 채택만" in fig_h["banner"]            # 숨김 상태 배너 명시
    assert "(숨김)" in fig_h["data"][0]["name"]                          # 범례에도 상태 표기

    # ── Task 12: 버전 → 조인 필드 매핑 함수 단위 검증 (지시된 예시 그대로) ──
    assert version_to_winner_field("v1.0.8.0") == "winner_gidx_v1080"
    assert version_to_winner_field("v1.0.8.4") == "winner_gidx_v1084"
    assert version_to_winner_field("v1") == "winner_gidx_v1"     # 짧은 입력도 크래시 없음
    assert version_to_winner_field("v1.0.13.2") == "winner_gidx_v10132"
    # 충돌 회귀 가드: 마지막 3자리 방식이면 이 둘이 같은 필드로 붕괴한다
    assert version_to_winner_field("v1.0.5.0") != version_to_winner_field("v2.0.5.0")

    class _FakeSchemaDS:
        def __init__(self, fields):
            self._fields = set(fields)
        def get_field_schema(self):
            return {f: None for f in self._fields}

    fake_ds = _FakeSchemaDS(["winner_gidx_v1080"])
    assert _resolve_join_field(fake_ds, "v1.0.8.0") == "winner_gidx_v1080"
    assert _resolve_join_field(fake_ds, "v1.0.8.4") is None      # 필드 없음 → None(크래시 아님)
    assert _resolve_join_field(None, "v1.0.8.0") is None         # 데이터셋 없음 → None

    # 실측 검증(2026-08-11 리빌드 후): sourcei/source-h 프레임에 v080·v084 조인 필드가 모두
    # 백필됐다 (2026-08-07의 "v084 부재" 상태는 리빌드로 해소 — 커버리지 공백 ② 메움).
    assert _resolve_join_field(frames, "v1.0.8.0") == WINNER_FIELD
    assert _resolve_join_field(frames, "v1.0.8.4") == "winner_gidx_v1084"
    if fo.dataset_exists("source-h"):
        sourceh_frames = fo.load_dataset("source-h")
        sourceh_schema = sourceh_frames.get_field_schema()
        assert "winner_gidx_v1080" in sourceh_schema
        assert _resolve_join_field(sourceh_frames, "v1.0.8.0") == "winner_gidx_v1080"
        assert _resolve_join_field(sourceh_frames, "v1.0.8.4") == "winner_gidx_v1084"

    # _prompts_dataset_name: ctx.dataset.name에서 유도, 없으면 레거시 PROMPTS_DATASET 폴백.
    class _FakeDataset:
        def __init__(self, name):
            self.name = name
    class _FakeCtxDS:
        def __init__(self, dataset):
            self.dataset = dataset
    assert _prompts_dataset_name(_FakeCtxDS(_FakeDataset("source-h"))) == "source-h-prompts"
    assert _prompts_dataset_name(_FakeCtxDS(_FakeDataset("sourcei"))) == "sourcei-prompts"
    assert _prompts_dataset_name(_FakeCtxDS(None)) == PROMPTS_DATASET

    # _current_winner_field: "전체"/미설정은 레거시 기본값(v080)으로 폴백 — 회귀 방지.
    class _FakeState2:
        def __init__(self, v):
            self.bank_version_filter = v
    class _FakePanel2:
        def __init__(self, v):
            self.state = _FakeState2(v)
    class _FakeCtx2:
        def __init__(self, v):
            self.panel = _FakePanel2(v)
            self.dataset = type("D", (), {"name": f"__winner_field_{v}__"})()
            _mem(self).bank_version_filter = v      # 정본은 서버 기억이다
    assert _current_winner_field(_FakeCtx2(ALL_VERSIONS_LABEL)) == WINNER_FIELD
    assert _current_winner_field(_FakeCtx2(None)) == WINNER_FIELD
    assert _current_winner_field(_FakeCtx2("v1.0.8.4")) == "winner_gidx_v1084"

    # frame_ids_to_gidx/gidx_to_frame_ids: 존재하지 않는 조인 필드는 크래시 대신 빈 결과.
    assert gidx_to_frame_ids(g, dataset_name=FRAMES_DATASET, winner_field="winner_gidx_v999") == []
    assert frame_ids_to_gidx(ids, dataset_name=FRAMES_DATASET, winner_field="winner_gidx_v999") == []
    assert gidxes_to_frame_ids([g], dataset_name=FRAMES_DATASET, winner_field="winner_gidx_v999") == []

    # 뱅크 버전 필터: "전체"는 전 문장(기존 동작과 바이트 단위 동일), 특정 버전은 그 버전만.
    bank_versions = sorted({str(v) for v in b["bank_version"] if v is not None}) \
        if b.get("bank_version") is not None else []
    assert bank_versions, "sourcei-prompts에 bank_version 값이 없음 — 데이터 계층 회귀 의심"
    # 2026-08-11 리빌드 고정: 두 버전이 다 보여야 한다 (사용자 요청 "다 보이게")
    assert {"v1.0.8.0", "v1.0.8.4"} <= set(bank_versions), bank_versions
    v0 = bank_versions[0]
    fig_all = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx=set(),
                            bank_version_filter=ALL_VERSIONS_LABEL)
    fig_v0 = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx=set(),
                           bank_version_filter=v0)
    n_all = sum(len(t["x"]) for t in fig_all["data"][:-1])
    n_v0 = sum(len(t["x"]) for t in fig_v0["data"][:-1])
    if len(b["gidx"]) <= MAX_POINTS:
        # "전체" = 전수 **에서 정렬 붕괴 버전만 뺀 것**. 그 버전은 벡터 귀속이 틀려
        # 기하 비교에 넣으면 안 된다 (2026-08-19, v1.0.2.0 실측).
        assert n_all == len(b["gidx"]) - fig_all["dropped_n"], \
            (n_all, len(b["gidx"]), fig_all["dropped_n"])
    else:
        assert MAX_POINTS * 0.98 <= n_all <= MAX_POINTS   # 채택 전수 + 층화(미달 허용)
    assert n_v0 <= n_all                       # 특정 버전은 부분집합
    # 버전 필터는 그 버전 문장만 남긴다 (2026-08-11 다중 버전 리빌드 후 실효 검증)
    assert n_v0 == min(int(vmasks[v0].sum()) - fig_v0["dropped_n"], MAX_POINTS)
    assert f"버전: {v0}" in fig_v0["banner"]
    assert f"버전: {ALL_VERSIONS_LABEL}" in fig_all["banner"]
    # bank_version_filter 기본값(None)은 필터 없음과 동일해야 한다(하위호환 — 기존 호출부).
    fig_default = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx=set())
    assert sum(len(t["x"]) for t in fig_default["data"][:-1]) == n_all

    # load_prompt_bundle dataset_name 파라미터화 + 캐시 1엔트리 유지 검증.
    _CACHE.clear()
    load_prompt_bundle(PROMPTS_DATASET)
    assert len(_CACHE) == 1
    if fo.dataset_exists("source-h-prompts"):
        load_prompt_bundle("source-h-prompts")
        assert len(_CACHE) == 1, "회귀: dataset_name 전환 후에도 캐시 엔트리가 1개를 넘음"
        load_prompt_bundle(PROMPTS_DATASET)
        assert len(_CACHE) == 1

    # source-h-prompts 존재 시 bundle 로드/필터 스모크 (요구사항 4, 없으면 skip).
    if fo.dataset_exists("source-h-prompts"):
        b_sourceh = load_prompt_bundle("source-h-prompts")
        assert len(b_sourceh["gidx"]) > 0
        sourceh_versions = sorted({str(v) for v in b_sourceh["bank_version"] if v is not None}) \
            if b_sourceh.get("bank_version") is not None else []
        assert sourceh_versions, "source-h-prompts에 bank_version 값이 없음"
        fig_sourceh_all = build_mode_a(b_sourceh, rule="argmax_k1", show_unadopted=True,
                                     selected_gidx=set(), bank_version_filter=ALL_VERSIONS_LABEL)
        fig_sourceh_v0 = build_mode_a(b_sourceh, rule="argmax_k1", show_unadopted=True,
                                    selected_gidx=set(), bank_version_filter=sourceh_versions[0])
        assert all(t["type"] == "scattergl" for t in fig_sourceh_all["data"])
        n_sourceh_all = sum(len(t["x"]) for t in fig_sourceh_all["data"][:-1])
        n_sourceh_v0 = sum(len(t["x"]) for t in fig_sourceh_v0["data"][:-1])
        assert n_sourceh_all == min(len(b_sourceh["gidx"]), MAX_POINTS)   # 2버전 > MAX_POINTS 서브샘플
        assert n_sourceh_v0 <= n_sourceh_all
    else:
        print("source-h-prompts not found — skip smoke")

    # brain run 이 데이터셋 일부만 덮는 경우 (frames-prompts: 615,296 샘플 / emb_viz
    # 603,318점 — 캡션 11,978행 제외). 2026-08-19 이전에는 여기서 IndexError 로 죽었다.
    # ⚠️ **데이터셋 존재만으로는 부족하다 — brain run 도 있어야 한다** (2026-09-09 실측).
    #    `sync_incremental.py prompts`(SYNC_PROMPTS_ALLOW_DESTRUCTIVE=1)가 이 데이터셋을
    #    재구축하는 동안은 샘플이 남아 있는데 `emb_viz` 가 없어서 `load_brain_results` 가
    #    `ValueError: Dataset has no brain method run key 'emb_viz'` 로 죽는다 — 공유
    #    호스트라 리빌드가 흔하고, 그때마다 셀프테스트 전체가 빨개져 **진짜 회귀를 가린다**.
    #    위 source-h 스킵과 같은 계약으로, 없으면 사유를 밝히고 건너뛴다.
    _fr_ready = fo.dataset_exists("frames-prompts") and \
        BRAIN_KEY in fo.load_dataset("frames-prompts").list_brain_runs()
    if not _fr_ready and fo.dataset_exists("frames-prompts"):
        print(f"frames-prompts 에 brain run '{BRAIN_KEY}' 없음 (리빌드 중?) — skip")
    if _fr_ready:
        b_fr = load_prompt_bundle("frames-prompts")
        assert len(b_fr["gidx"]) == len(b_fr["xy"]), (len(b_fr["gidx"]), len(b_fr["xy"]))
        fig_fr = build_mode_a(b_fr, rule="argmax_k1", show_unadopted=True, selected_gidx=set())
        assert sum(len(t["x"]) for t in fig_fr["data"][:-1]) > 0
        assert "출처" in fig_fr["banner"], fig_fr["banner"]
        _CACHE.clear()
        load_prompt_bundle(PROMPTS_DATASET)

    # _rows_to_markdown join_field_missing 안내 (표 내용은 그대로 유지).
    row12 = {"gidx": g, "text": "hello12", "wins": 1, "purity": 0.5,
             "n_cameras": 1, "wave_gain": 0.1}
    md_missing = _rows_to_markdown([], "winner_gidx_v1084")
    assert "조인 필드 없음" in md_missing and "winner_gidx_v1084" in md_missing
    assert "선택된 프레임 없음" in md_missing
    md_missing_rows = _rows_to_markdown([row12], "winner_gidx_v1084")
    assert "조인 필드 없음" in md_missing_rows and "| 클래스 |" in md_missing_rows
    # `cls` 키가 없는 구형 행(배포 전 탭의 panel_state 에코)도 죽지 않고 `-` 로 나온다
    assert "| ? | - |" in md_missing_rows, md_missing_rows

    # 클릭 매핑 계약: PlotlyView의 onClick은 trace.ids[pointIndex]만 ctx.params["id"]로 전달한다
    # (customdata 아님 — App 번들 getIdForTrace 실측, Task 8). 하이라이트 트레이스 ids로 역추적 가능해야 함.
    assert "ids" in fig["data"][-1] and set(int(x) for x in fig["data"][-1]["ids"]) == {g}
    assert all("ids" in t for t in fig["data"])

    # 승자 문장 표 마크다운: 빈 선택은 안내문, 채워진 선택은 헤더+행 포함
    assert "선택된 프레임 없음" in _rows_to_markdown([])
    row = {"gidx": g, "text": "hello", "hl": 3, "cls": "fire", "wins": 1, "purity": 0.5,
           "n_cameras": 2, "wave_gain": 0.1}
    md = _rows_to_markdown([row])
    assert "| 클래스 |" in md and "| 3 |" in md and "hello" in md
    # 클래스 열 + 분포 한 줄. 분포는 **표시분**임을 반드시 밝힌다 (모집단 오독 금지)
    assert "| fire |" in md and "표시 1행 클래스: fire 1" in md, md
    # 표 셀 안 `|` 이스케이프 (원본 텍스트에 파이프가 있어도 열 정렬이 깨지면 안 됨)
    # 상한 잘림 표기 (lasso 다중선택): 전체 수 > 표시 행 수면 안내가 붙는다
    md_trunc = _rows_to_markdown([row], total=5)
    assert "선택 5개 중 상위 1개" in md_trunc and "| 3 |" in md_trunc
    # 클래스를 못 구한 행(category 없는 데이터셋·NULL 캡션 행)은 `-` 이고
    # 분포 줄은 **아예 나오지 않는다** (없는 것을 0으로 세지 않는다)
    md_nocls = _rows_to_markdown([{**row, "cls": "-"}])
    assert "| - |" in md_nocls and "클래스:" not in md_nocls, md_nocls
    # 자리표시자 경고 (벡터 전용 뱅크) — 정상 문장 행에는 붙지 않아야 한다
    assert "DB 정본에도 문장이 없는 뱅크" not in md
    md_ph = _rows_to_markdown([{**row, "text": "(텍스트 없음 #0)"}, row])
    assert "⚠️ 1/2행" in md_ph and "DB 정본에도 문장이 없는 뱅크" in md_ph
    # wave_gain 표시 정밀도 — LOO ΔIoU 실측 스케일(1e-05)이 0.000 으로 뭉개지면 안 된다
    assert "0.000012" in _rows_to_markdown([{**row, "wave_gain": 1.21e-05}])

    row_pipe = {**row, "text": "a|b|c"}
    md_pipe = _rows_to_markdown([row_pipe])
    assert "a\\|b\\|c" in md_pipe                    # 파이프가 이스케이프된 채 보존됨
    body_line = [ln for ln in md_pipe.splitlines() if ln.startswith("| 3 |")][0]
    cols = body_line.replace("\\|", "").split("|")   # 이스케이프 제거 후에도 9컬럼 유지돼야 함
    assert len(cols) == 11, cols                      # 양끝 빈 문자열 2 + 컬럼 9

    # ── 버전별 상위 표 (topk) ──────────────────────────────────────────────
    tk_rows = [{"ver": "v1.0.8.0", "gidx": 1, "text": "a fire", "cls": "fire",
                "cos": 0.31, "rank": 1},
               {"ver": "v1.0.8.0", "gidx": 2, "text": "a room", "cls": "normal",
                "cos": 0.29, "rank": 2}]
    tk = topk_to_markdown(tk_rows)
    assert "| 버전 | 순위 | 클래스 | cos | 문장 |" in tk, tk
    assert "표시 2행 클래스: fire 1 · normal 1" in tk, tk
    assert "기준" not in tk, tk                       # ref_class 없으면 표기 전체 생략
    # 기준 = 프레임의 category. 1위 행에만 붙고, 2위 행에는 붙지 않는다.
    tk_ref = topk_to_markdown(tk_rows, ref_class="fire")
    assert "fire (= 기준)" in tk_ref and "사람 정답 아님" in tk_ref, tk_ref
    assert "normal (= 기준)" not in tk_ref and "normal (≠ 기준)" not in tk_ref, tk_ref
    assert topk_to_markdown(tk_rows, ref_class="smoke").count("(≠ 기준)") == 1
    # 금지 어휘 — category 는 v1.0.8.0 argmax 라 정답/정확도로 불러선 안 된다
    for banned in ("정답 =", "오답", "적중", "정확도"):
        assert banned not in tk_ref, banned
    # `cls` 없는 구형 캐시 행도 `-` 로 렌더된다
    tk_old = topk_to_markdown([{k: v for k, v in tk_rows[0].items() if k != "cls"}])
    assert "| 🏆 | - |" in tk_old, tk_old

    # dedup 가드 회귀 (Task 8 fix round): 밑줄 없는 상태 키만 실제로 영속되어야 하고,
    # 빈 payload 로의 "진짜 선택 해제" 전이는 스퓨리어스 재발화와 구별돼 절대 삼켜지면
    # 안 된다. 배포본 panel.py의 PanelRefBase.__setattr__가 `_` 시작 키를 self.set()
    # 우회(순수 인스턴스 속성, ctx.panel_state 라운드트립 밖)로 처리하는 걸 실측했으므로
    # (panel.py:223-235) 여기서는 실제 규약과 동일한 get/set 인터페이스의 fake로 검증한다.
    class _FakePanelState:
        def __init__(self):
            self._d = {}
        def get(self, k, default=None):
            return self._d.get(k, default)
        def set(self, k, v):
            self._d[k] = v
    class _FakeCtx:
        def __init__(self):
            self.panel = type("P", (), {"state": _FakePanelState()})()
    fctx = _FakeCtx()
    assert _dedup_guard(fctx, "sel_seen", ["a", "b"]) is False   # 최초 진입 → 처리
    assert _dedup_guard(fctx, "sel_seen", ["b", "a"]) is True    # 순서만 다른 재발화 → 스킵
    assert _dedup_guard(fctx, "sel_seen", []) is False, \
        "회귀: 실제 전체 선택 해제 전이가 삼켜짐"                  # 진짜 "전체 해제" → 반드시 처리
    assert _dedup_guard(fctx, "sel_seen", []) is True             # 그 다음 스퓨리어스 빈 재발화만 스킵

    # 모드 B (Task 9): sourcei를 ground_truth 2클래스로 갈라 같은 좌표계 overlay (구조 검증용).
    # `frames`(project 22개)이 본용도지만 selftest는 App 없이 도는 sourcei로 검증한다.
    figb = build_mode_b(FRAMES_DATASET, "ground_truth", ["normal", "falldown"], BRAIN_KEY)
    assert len(figb["data"]) == 2 and all(t["type"] == "scattergl" for t in figb["data"])
    assert "같은 좌표계" in figb["banner"]
    # 크래시 가드 (2026-08-10 실사용 오류): 없는 그룹 필드는 ValueError 대신 안내 배너.
    # 기본값 "project"가 sourcei에 없어 on_groups_change가 패널을 죽였던 케이스 그대로.
    figb_nf = build_mode_b(FRAMES_DATASET, "project", ["cohort-b"], BRAIN_KEY)
    assert figb_nf["data"] == [] and "그룹 필드 'project'가 없습니다" in figb_nf["banner"]
    figb_nb = build_mode_b(FRAMES_DATASET, "ground_truth", ["normal"], "no_such_brain_key")
    assert figb_nb["data"] == [] and "brain run" in figb_nb["banner"]
    n_normal = int(np.sum(np.asarray(frames.values("ground_truth.label"), dtype=object) == "normal"))
    assert figb["data"][0]["name"] == f"normal ({min(n_normal, MAX_POINTS // 2)})"
    # 데이터 계약(모드 A와 동일): 스키마 PlotlyView.data에는 trace 리스트만 굽는다 —
    # {"data":...,"layout":...} 통짜를 넘기면 0점 렌더된다는 게 Task 5 스파이크 실측이므로,
    # 여기서도 fig 전체가 아니라 fig["data"]만 trace 스키마를 만족하는지 확인한다.
    assert all(set(t.keys()) >= {"type", "x", "y", "marker"} for t in figb["data"])

    # Task 11 회귀 가드 — "클릭해야 나온다" 2차 방어선(1차·확정 원인은 fiftyone_app_setup.py의
    # active_child 누락 — render()의 docstring/주석 참고): on_load 직후 render()가 만드는
    # 스키마의 PlotlyView.data가 (set_data 왕복 없이도) 즉시 비어있지 않아야 한다. set_data()는
    # patch_panel_data로, render()의 스키마는 show_panel_output으로 — 각각 독립된
    # setTimeout(fn,1)로 지연 적용되는 별도 채널이라 최초 마운트 시 도착 순서가 보장되지 않는다.
    # PlotlyView(data=...)로 스키마에 직접 구우면 이 잠재 경합과 무관하게 항상 채워진다.
    class _FakePanelStateAttr:
        def set(self, key, value=None):
            # 실제 PanelState.set 은 pydash 중첩 경로 — 여기선 최상위 키만 흉내내면 충분
            # (_sync_controls 가 "controls" 단일 키에 dict 를 통째로 넣는다)
            setattr(self, key.split(".")[0], value)

        def get(self, key, default=None):
            # `vars(self)` 로 본다 — 아래 `__getattr__` 가 미설정 키에 None 을 주므로
            # `getattr(..., default)` 로는 **비-None default 가 무시된다**.
            return vars(self).get(key.split(".")[0], default)

        def __getattr__(self, key):
            # ⚠️ **실제 계약**: 세팅 안 된 상태 키를 읽으면 PanelState 는 `None` 을 준다
            #    (그래서 render 가 `topk_src` 를 가드 없이 읽는다). 이 목은 그걸 안 흉내내서
            #    `_refresh` 가 새 상태 키를 하나 읽을 때마다 AttributeError 로 셀프테스트가
            #    통째로 죽었다 — 목이 실제 API 계약을 따라야 한다는 그 규칙의 재발이다.
            #    `_` 로 시작하는 내부 속성은 그대로 AttributeError 로 남긴다 (오타 은폐 방지).
            if key.startswith("_"):
                raise AttributeError(key)
            return None
    class _FakePanelData:
        def __init__(self, calls):
            self._calls = calls
        def clear(self):
            self._calls.append("clear")
    class _FakePanelAttr:
        def __init__(self):
            self.state = _FakePanelStateAttr()
            self.data_calls = []
            self.data = _FakePanelData(self.data_calls)
        def set_data(self, name, value):
            self.data_calls.append((name, value))
    class _FakeCtxAttr:
        def __init__(self):
            self.panel = _FakePanelAttr()
            self.dataset = None

        @property
        def panel_state(self):
            # 클라이언트는 서버가 마지막에 밀어준 것을 그대로 되돌려 준다.
            return dict(vars(self.panel.state))
    _MEM.clear()          # 위 순수함수 테스트가 남긴 서버 기억과 섞이지 않게
    render_ctx = _FakeCtxAttr()
    panel_instance = PromptComparePanel()
    # 리마운트 잔재: on_load 는 `selected_gidx` 를 비우므로 그 파생물인 top-k 표·기준도
    # 같이 비워야 한다. 안 비우면 데이터셋 전환 후 옛 프레임의 기준이 새 화면에 붙는다.
    _mem(render_ctx).frame_topk = [{"ver": "v1.0.8.0", "gidx": 1, "text": "stale",
                                          "cls": "fire", "cos": 0.9, "rank": 1}]
    _mem(render_ctx).topk_ref_class = "fire"
    panel_instance.on_load(render_ctx)               # 실제 마운트 시퀀스 그대로: on_load → _refresh
    assert not _mem(render_ctx).frame_topk, "회귀: 리마운트 후 옛 top-k 표가 남았다"
    assert _mem(render_ctx).topk_ref_class is None, "회귀: 옛 기준 클래스가 남았다"
    schema = panel_instance.render(render_ctx)        # set_data 왕복(비동기) 없이 바로 render()
    scatter_view = schema.type.properties["scatter_v2"].view
    assert scatter_view.data, \
        "회귀: render() 스키마의 초기 data가 비어있음 — 최초 마운트 시 빈 산점도(Task 11) 재발"
    n_schema = sum(len(t["x"]) for t in scatter_view.data[:-1])
    # 상한 = 전수에서 정렬 붕괴 버전을 뺀 값 (그 버전은 벡터 귀속이 틀려 안 그린다).
    _cap = min(len(b["gidx"]) - build_mode_a(
        b, rule="argmax_k1", show_unadopted=True, selected_gidx=set())["dropped_n"],
        MAX_POINTS)
    assert _cap * 0.98 <= n_schema <= _cap, \
        f"회귀: 스키마에 구운 data 포인트 수({n_schema})가 기대치({_cap})와 불일치"
    assert scatter_view.layout, "회귀: render() 스키마의 초기 layout이 비어있음"
    # 컨트롤 한 줄 (2026-08-10 피드백): h_stack("controls")
    ctrl_props = schema.type.properties["controls"].type.properties
    # 아직 state 바인딩인 것 = enum 3개. 색칠·뱅크 버전은 **버튼 피커**다 (2026-09-09):
    # enum 은 값을 고르는 순간 App 이 패널 상태를 써서 사용자의 사이드바 필터를 지운다.
    assert {"mode", "rule", "show_mode"} <= set(ctrl_props)
    # 이름 규칙(두 패널 공통): 컨트롤 = `<name>`, 토글 = `<name>_toggle`,
    # 펼친 격자 = `<name>_picker`, 옵션 = `pick_<name>__<value>`.
    assert {"color", "bank"} <= set(ctrl_props), "회귀: 색칠·뱅크 버전 컨트롤이 없다"
    assert "bank_toggle" in ctrl_props["bank"].type.properties, "회귀: 토글 이름 규칙 이탈"
    # 인라인/피커 판정은 `_fits_inline` 한 곳에서만 나온다 — 라벨이 길면 셀 안에서 글자
    # 단위로 줄바꿈되므로(2026-09-09 실측) 개수가 아니라 길이 합으로 가른다.
    assert _fits_inline([("A", "A"), ("B", "B"), ("C", "C")]), "짧은 옵션은 인라인이어야 한다"
    assert not _fits_inline(list(COLOR_BY_LABELS.items())), \
        "회귀: 색칠 라벨 길이면 피커여야 한다 (인라인이면 '클래 스' 처럼 쪼개진다)"
    assert "color_toggle" in ctrl_props["color"].type.properties, \
        "회귀: 색칠이 토글이 아니다 — `_fits_inline` 판정과 렌더가 갈렸다"
    assert not ({"color_by", "bank_version_filter"} & set(ctrl_props)), \
        "회귀: 색칠/뱅크 버전이 enum 으로 돌아갔다 — 한 번 고를 때마다 사이드바 필터가 지워진다"
    # 미러 자체가 없어야 한다 (2026-09-09 2단계): 모든 컨트롤이 버튼이라 클라이언트가
    # 들고 있어야 할 패널 상태가 0 이다. `controls` 키가 생겼다면 누군가 enum 을 되살린 것.
    assert not vars(render_ctx.panel.state), \
        f"회귀: 패널 상태를 썼다 — 사이드바 필터가 지워진다: {sorted(vars(render_ctx.panel.state))}"

    # 컨트롤 핸들러 상태 전이 + 화이트리스트 (codex 3차 리뷰 (a)/(e)-1): 허용값만 반영,
    # 예상밖 값은 상태를 건드리지 않아야 한다 (조용한 폴백 금지).
    class _FakeCtxHandler(_FakeCtxAttr):
        def __init__(self):
            super().__init__()
            self.params = {}
    hctx = _FakeCtxHandler()
    panel_instance.on_load(hctx)
    hctx.params = {"value": "dist_iou"}
    panel_instance.on_rule_change(hctx)
    assert _mem(hctx).rule == "dist_iou"
    assert not vars(hctx.panel.state), "회귀: 규칙 변경이 패널 상태를 썼다"
    hctx.params = {"value": "nonsense"}
    panel_instance.on_rule_change(hctx)
    assert _mem(hctx).rule == "dist_iou"                       # 화이트리스트 밖 → 무시
    hctx.params = {"value": SHOW_ADOPTED_LABEL}
    panel_instance.on_show_change(hctx)
    assert _mem(hctx).show_unadopted is False
    hctx.params = {"value": "nonsense"}
    panel_instance.on_show_change(hctx)
    assert _mem(hctx).show_unadopted is False                  # 예상밖 값 → 상태 유지
    hctx.params = {"value": SHOW_ALL_LABEL}
    panel_instance.on_show_change(hctx)
    assert _mem(hctx).show_unadopted is True
    hctx.params = {"value": "B"}
    panel_instance.on_mode_change(hctx)
    assert _mem(hctx).mode == "B"
    hctx.params = {"value": "A"}
    panel_instance.on_mode_change(hctx)
    assert _mem(hctx).mode == "A"

    # ── 사이드바 필터 보존 계약: **모든 훅이 패널 상태를 쓰지 않는다** (2026-09-09) ──
    #    값이 바뀌는 상태 쓰기 하나면 App 이 spaces 를 영속하며 DatasetPageQuery 를
    #    network-only 로 다시 돌리고, 그때 새 Dataset.id 가 발급돼 클라이언트 전용
    #    `filters`(사이드바 필터)가 스스로 비워진다 (`_mem` 주석의 전 구간 체인).
    #    사용자 리포트(2026-09-09): "이미지 선택은 잘 됐는데 시간이 지나자 label 선택이 사라짐".
    #    유일한 예외는 컨트롤 미러이고, 그것도 클라이언트가 낡았을 때만 나간다.
    def _writes(fn, ctx_, *a, **kw):
        before = dict(vars(ctx_.panel.state))
        fn(ctx_, *a, **kw)
        after = dict(vars(ctx_.panel.state))
        return {k: v for k, v in after.items() if before.get(k) != v}

    kctx = _FakeCtxHandler()
    kctx.params = {"panel_id": "no-write-contract"}
    # ⚠️ **on_load 도 계약 안이다** (2026-09-09 2단계): 미러가 사라져 콜드 스타트에서도
    #    쓸 게 없다. 이게 "재시작 뒤 첫 로드에서 필터가 한 번 지워지던" 경로를 없앤다.
    assert _writes(panel_instance.on_load, kctx) == {}, \
        "회귀: on_load 가 패널 상태를 썼다 — 콜드 스타트 1회 wipe 재발"
    # ⚠️ **훅이 실제로 몸통을 실행해야 계약이 의미가 있다** (코덱스 리뷰 F7): 선택 훅은
    #    `ctx.selected` 가 비면 dedup 가드에서 즉시 돌아 나가므로, 빈 선택으로 도는 단언은
    #    "아무것도 안 한 코드가 아무것도 안 썼다" 를 확인할 뿐이다. 진짜 프레임 id 를 싣는다.
    _real_ids = [str(i) for i in frames.take(2).values("id")]
    kctx.selected = _real_ids
    kctx.extended_selection = {"selection": _real_ids}
    for _hook in ("on_change_view", "on_change_selected", "on_change_extended_selection",
                  "render", "on_plot_double_click"):
        kctx.params = {"panel_id": "no-write-contract"}
        _dedup_reset = _mem(kctx)
        _dedup_reset.set("sel_seen", [])                # 앞 훅이 남긴 지문을 지워 몸통 보장
        _dedup_reset.set("ext_sel_seen", [])
        assert _writes(getattr(panel_instance, _hook), kctx) == {}, \
            f"회귀: {_hook} 가 패널 상태를 썼다 — 사용자의 사이드바 필터가 지워진다"
    assert _mem(kctx).selected_gidx is not None, "선택 훅이 몸통까지 갔는지 확인"
    assert _writes(panel_instance._refresh, kctx) == {}, \
        "회귀: _refresh 가 패널 상태를 썼다 (컨트롤 미러가 이미 최신인데도 밀었다)"
    # 사용자가 우리 드롭다운을 직접 고르는 경로 — App 이 그 값을 낙관적으로 이미 반영해
    # 두므로 미러를 다시 밀 이유가 없다. 이게 2026-09-09 재현 실험(155장 → 2초 뒤 6,032장)의
    # 경로였다.
    kctx.params = {"panel_id": "no-write-contract"}
    assert _writes(panel_instance.on_toggle_color_picker, kctx) == {}, \
        "회귀: 색칠 피커 토글이 패널 상태를 썼다"
    assert _mem(kctx).picker == "color"
    _open = panel_instance.render(kctx).type.properties
    assert "color_picker" in _open and any(
        k.startswith("pick_color__") for k in _open["color_picker"].type.properties), \
        "회귀: 색칠 피커를 열었는데 옵션 버튼이 없다"
    kctx.params = {"panel_id": "no-write-contract", "value": COLOR_BY_WAVE_ROLE}
    assert _writes(panel_instance.on_color_change, kctx) == {}, \
        "회귀: 색칠 변경이 패널 상태를 썼다 — 드롭다운 한 번에 사이드바 필터가 날아간다"
    assert _mem(kctx).color_by == COLOR_BY_WAVE_ROLE and _mem(kctx).picker is None
    # 선택 계열: 상태는 서버 기억에만 남는다.
    kctx.params = {"panel_id": "no-write-contract"}
    kctx.selected = []
    assert _writes(panel_instance.on_clear_selection, kctx) == {}, \
        "회귀: 선택 해제가 패널 상태를 썼다"
    assert _mem(kctx).selected_gidx == []

    # ── 버튼 피커(색칠·뱅크 버전): 토글도 선택도 패널 상태를 쓰지 않는다 (2026-09-09) ──
    #    드롭다운(enum)이었을 때는 값을 고르는 순간 App 이 스스로 패널 상태를 써서 사용자의
    #    사이드바 필터가 사라졌다. 버튼(ButtonView)은 번들상 onChange 자체가 없어 안전하다.
    #    ⚠️ 여기서 그리드 선택을 비운다: 버전 전환 경로가 선택 프레임마다 top-k KNN 질의를
    #    돌려(그 계약은 위 블록이 이미 검증) 셀프테스트가 수 분 길어진다. 이 블록의 계약은
    #    "상태를 쓰지 않는다" 이므로 조인 유무와 무관하다.
    kctx.selected = []
    kctx.params = {"panel_id": "no-write-contract", "value": COLOR_BY_WAVE_ROLE}
    _APPLIED.clear()
    assert _writes(panel_instance.on_color_change, kctx) == {}, \
        "회귀: 색칠 선택이 패널 상태를 썼다 — 한 번 고를 때마다 사이드바 필터가 날아간다"
    assert _mem(kctx).color_by == COLOR_BY_WAVE_ROLE
    kctx.params = {"panel_id": "no-write-contract"}
    assert _writes(panel_instance.on_toggle_bank_picker, kctx) == {}, \
        "회귀: 뱅크 버전 피커 토글이 패널 상태를 썼다"
    assert _mem(kctx).picker == "bank"
    _open = panel_instance.render(kctx).type.properties
    assert "bank_picker" in _open and any(
        k.startswith("pick_bank__") for k in _open["bank_picker"].type.properties), \
        "회귀: 뱅크 피커를 열었는데 옵션 버튼이 없다"
    kctx.params = {"panel_id": "no-write-contract", "value": ALL_VERSIONS_LABEL}
    _APPLIED.clear()
    assert _writes(panel_instance.on_bank_version_change, kctx) == {}, \
        "회귀: 뱅크 버전 선택이 패널 상태를 썼다"
    assert _mem(kctx).picker is None, "고른 뒤에는 피커가 닫혀야 한다"
    # 같은 값 재선택(가드에 걸리는 경로)도 목록은 닫힌다 — 안 닫히면 "안 먹는다" 로 보인다
    panel_instance.on_toggle_bank_picker(kctx)
    assert _writes(panel_instance.on_bank_version_change, kctx) == {}
    assert _mem(kctx).picker is None, "회귀: 같은 값 재선택 시 피커가 열린 채 남는다"

    # ⚠️ **컨트롤 변경은 예외다** (2026-09-09 실측 + 코덱스 리뷰 F3): 사용자가 드롭다운을
    #    고르면 **App 이 스스로** `controls.<key>` 를 패널 상태에 써서 같은 체인을 태운다 —
    #    서버 쓰기가 0 이어도 사이드바 필터는 날아간다(계측으로 확인). 그래서 규칙 변경처럼
    #    파생 컨트롤(색칠)까지 바뀌어 미러를 되밀어야 하는 경우도 **추가 손해가 없다**.
    #    다만 그 쓰기는 `controls` 한 키로 제한돼야 한다 — 다른 키가 새면 계약 위반이다.
    kctx.params = {"panel_id": "no-write-contract", "value": "dist_iou"}
    _rule_writes = _writes(panel_instance.on_rule_change, kctx)
    assert set(_rule_writes) <= {"controls"}, \
        f"회귀: 규칙 변경이 컨트롤 미러 밖의 상태를 썼다: {sorted(_rule_writes)}"
    assert _mem(kctx).rule == "dist_iou"

    # ── 레거시 에코 격리 (코덱스 리뷰 F1) ──────────────────────────────────────
    #    이 변경 **이전** 배포는 mode/bank_version_filter 같은 최상위 키를 패널 상태에 썼고,
    #    그 값은 세션이 살아 있는 한 계속 실려 온다. 그걸 서버 기억의 시드로 쓰면 데이터셋을
    #    바꿔도 `mode` 가 이미 있어 리셋이 안 돌아 **남의 데이터셋 뱅크 버전**이 남는다.
    _MEM.clear()
    legacy = _FakeCtxHandler()
    legacy.params = {"panel_id": "legacy-echo"}
    legacy.panel.state.mode = "B"                       # 옛 배포가 심어 둔 최상위 키들
    legacy.panel.state.bank_version_filter = "v1.0.8.4"
    legacy.panel.state.selected_gidx = [12345]
    panel_instance.on_load(legacy)
    assert _mem(legacy).bank_version_filter == ALL_VERSIONS_LABEL, \
        "회귀: 레거시 에코가 서버 기억으로 새어 데이터셋 전환 리셋이 무력화된다"
    assert _mem(legacy).mode == "A" and _mem(legacy).selected_gidx == [], \
        "회귀: 레거시 에코의 최상위 키가 서버 기억으로 복사됐다"

    # ── "전체 ↔ 채택만" 왕복 회귀 (2026-08-20 사용자 신고) ──
    #    ① 채택만 선택 → 반영  ② 리마운트(on_load)  ③ 리셋 뒤 같은 값 재선택
    #    옛 코드: ②가 상태를 전체로 되돌리는데 `_APPLIED` 는 "채택만" 을 기억 → ③이
    #    가드에 삼켜져 서버는 전체 · 드롭다운은 채택만 → `_sync_controls` 가 되밀어 왕복.
    #    panel_id 를 실어야 `_APPLIED` 경로를 탄다 (없으면 carried_same 폴백).
    fctx = _FakeCtxHandler()
    fctx.params = {"panel_id": "flap-regression"}
    panel_instance.on_load(fctx)
    assert _mem(fctx).show_unadopted is True
    fctx.params = {"panel_id": "flap-regression", "value": SHOW_ADOPTED_LABEL}
    panel_instance.on_show_change(fctx)
    assert _mem(fctx).show_unadopted is False, "① 채택만이 반영돼야 한다"
    # ② 리마운트는 사용자 선택을 보존한다 (같은 데이터셋 = 리셋 사유가 아니다)
    fctx.params = {"panel_id": "flap-regression"}
    panel_instance.on_load(fctx)
    assert _mem(fctx).show_unadopted is False, \
        "회귀: 리마운트가 사용자 선택을 전체로 되돌렸다 (왕복 버그의 절반)"
    assert _APPLIED.get(("flap-regression", "show_mode")) == SHOW_ADOPTED_LABEL, \
        "회귀: 가드 기억이 옛 값으로 되돌아갔다 (재선택이 삼켜진다)"
    # ③ **뷰 에코는 미러를 다시 밀지 않아야 한다.** 밀면 클라이언트가 그 값을
    #    `on_show_change` 로 되돌려 보내고, 동시 처리되는 다음 응답이 또 밀어 왕복이
    #    스스로 지속된다 (`_sync_controls` 주석의 실측 60초 5회). 값이 그대로면 침묵이 정답
    #    — 그리고 2026-09-09 이후로는 그 침묵이 **사용자의 사이드바 필터를 지키는 수단**이다.
    panel_instance._refresh(fctx, update_plot=False)
    assert not vars(fctx.panel.state), "회귀: _refresh 가 패널 상태를 썼다"
    # ③ **빈 panel_state 에코**(옛 탭 재접속 = 자가복구 경로)가 와도 사용자 선택을
    #    되살려야 한다 — 이제 정본은 `(패널, 데이터셋)` 서버 기억이다.
    echo = _FakeCtxHandler()
    echo.params = {"panel_id": "flap-regression"}      # 같은 데이터셋, 빈 패널 상태
    panel_instance.on_load(echo)                       # warm → 리셋하지 않는다
    assert _mem(echo).show_unadopted is False, \
        "회귀: 빈 상태 에코가 사용자 선택(채택만)을 기본값으로 되돌렸다"
    assert not vars(echo.panel.state), "회귀: 빈 에코 자가복구가 패널 상태를 썼다"
    #    기억과 다른 값으로 바꾸는 것도 당연히 먹어야 한다 (가드가 삼키지 않는다)
    echo.params = {"panel_id": "flap-regression", "value": SHOW_ALL_LABEL}
    panel_instance.on_show_change(echo)
    assert _mem(echo).show_unadopted is True, "회귀: 되돌리기 클릭이 삼켜졌다"
    #    ⚠️ 새 panel_id 라도 **같은 데이터셋이면 기억을 잇는다** (2026-09-09 의미 변경):
    #    옛 코드의 정본은 pid 키 `_APPLIED` 였지만, 이제 패널 상태 자체가 (패널, 데이터셋)
    #    서버 기억이다. 이건 오늘과 같은 사용자 경험이다 — 패널 상태는 세션 spaces 에 실려
    #    이미 탭 사이에 공유됐다.
    brand_new = _FakeCtxHandler()
    brand_new.params = {"panel_id": "flap-brand-new"}
    panel_instance.on_load(brand_new)
    assert _mem(brand_new).show_unadopted is True, "같은 데이터셋 기억(전체)을 이어야 한다"
    # 데이터셋 전환은 버전 필터를 리셋한다 (메모리 §16 요구사항). 이제 그 리셋은
    # **기억 키가 데이터셋별**이라는 사실에서 나온다 — 별도 분기가 아니다.
    sw = _FakeCtxHandler()
    sw.params = {"panel_id": "flap-switch"}
    panel_instance.on_load(sw)
    _mem(sw).bank_version_filter = "v1.0.8.4"
    _mem(sw).show_unadopted = False
    sw.dataset = type("D", (), {"name": "__switched_dataset__"})()   # 헤더에서 전환
    panel_instance.on_load(sw)
    assert _mem(sw).bank_version_filter == ALL_VERSIONS_LABEL, \
        "데이터셋 전환 시 버전 필터는 리셋돼야 한다 (뱅크 목록이 다르다)"
    assert _mem(sw).show_unadopted is True
    # 가드 기억에 **옛 데이터셋 값이 남으면 안 된다** — 남으면 새 데이터셋에서 같은 값을
    # 고르는 클릭이 `_change_guard` 에 삼켜진다. (비어 있는 것도 정답이다: 미러가 이미
    # 최신이면 `_sync_controls` 가 침묵하므로 `_remember` 도 안 돈다.)
    assert _APPLIED.get(("flap-switch", "bank_version_filter")) in (None, ALL_VERSIONS_LABEL), \
        f"회귀: 전환 뒤에도 가드 기억이 옛 값을 들고 있다: {_APPLIED.get(('flap-switch', 'bank_version_filter'))}"

    # 데이터셋 전환은 fig 캐시를 갈라야 한다 (2026-08-20: 옛 키가 인스턴스만 봐서
    # frames 의 199,972점이 문장 패널에 남았다 — `_fig_key` 주석)
    class _DsCtx:
        def __init__(self, name, panel):
            self.dataset = type("D", (), {"name": name})()
            self.panel = panel
            self.params = {"panel_id": "figkey-switch"}
    shared_panel = _FakeCtxHandler().panel
    a, b_ = _DsCtx("frames", shared_panel), _DsCtx("sourcei", shared_panel)
    _put_fig(a, [{"marker": "frames-fig"}])
    assert _get_fig(a) == [{"marker": "frames-fig"}]
    assert _get_fig(b_) is None, \
        "회귀: 다른 데이터셋에서 옛 fig 가 보인다 (stale 플롯 오독의 원인)"

    # update_plot=False 계약 (성능): 플롯 상태를 다시 쓰지 않고 표만 갱신한다
    # (파괴 방지 수단 아님 — _refresh docstring의 최종 진단 참고).
    prev_scatter = _get_fig(render_ctx)
    _mem(render_ctx).selected_gidx = [g]
    panel_instance._refresh(render_ctx, update_plot=False)
    assert _get_fig(render_ctx) is prev_scatter, \
        "회귀: update_plot=False인데 fig 가 교체됨 — emb_viz 선택 파괴 재발"
    assert _mem(render_ctx).top_table and _mem(render_ctx).top_table[0]["gidx"] == g, \
        "update_plot=False에서도 승자 문장 표는 갱신돼야 한다"
    _mem(render_ctx).selected_gidx = []
    panel_instance._refresh(render_ctx)   # 전체 갱신 원복

    # ⚠️ **그리드 선택 훅은 플롯을 다시 써야 한다** (2026-09-09 회귀 방지). 그리드 체크박스는
    #    뷰·extended 를 건드리지 않아 `_fig_key` 지문이 그대로다 → update_plot=False 로 두면
    #    render 가 캐시 히트로 하이라이트 없는 옛 figure 를 낸다("선택 표시가 안 된다").
    #    여기서 잡는 것은 그 한 줄이다: 누군가 다시 update_plot=False 로 바꾸면 실패한다.
    _seen_kw = {}
    _real_refresh = panel_instance._refresh
    panel_instance._refresh = lambda ctx, **kw: _seen_kw.update(kw)
    try:
        gctx = _FakeCtxHandler()
        gctx.panel = render_ctx.panel               # 초기화된 상태(mode/rule)를 그대로 쓴다
        gctx.dataset = render_ctx.dataset
        gctx.selected = ["deadbeefdeadbeefdeadbeef"]   # 존재하지 않는 id → 조인 0, 경로는 동일
        panel_instance.on_change_selected(gctx)
    finally:
        panel_instance._refresh = _real_refresh
    assert _seen_kw.get("update_plot", True) is not False, \
        f"회귀: on_change_selected 가 플롯을 다시 쓰지 않는다 (그리드 선택 하이라이트 소실) {_seen_kw}"

    # box select 이중 발화 가드 (2026-08-10 실사용 오류): scattergl box select는 점 있는
    # plotly_selected 직후 빈 이벤트를 한 번 더 쏜다 — 빈 payload가 방금 선택을 지우면 안 됨.
    hctx.params = {"value": "argmax_k1"}
    panel_instance.on_rule_change(hctx)
    _mem(hctx).selected_gidx = [999]
    hctx.params = {"data": []}
    panel_instance.on_plot_selected(hctx)
    assert _mem(hctx).selected_gidx == [999], "회귀: 빈 plotly_selected가 선택을 지움"
    # 명시적 해제는 더블클릭 훅 — 문장 선택과 프레임 하이라이트(extended selection) 모두 비움
    class _FakeOps:
        def __init__(self):
            self.calls = []
        def show_samples(self, samples, use_extended_selection=False):
            self.calls.append(("show_samples", samples))
        def clear_view(self):
            self.calls.append("clear_view")
        def set_view(self, view=None, name=None):
            self.calls.append(("set_view", view))
        def set_extended_selection(self, *a, **k):
            self.calls.append("set_extended_selection")
        def set_spaces(self, spaces=None, name=None):
            self.calls.append(("set_spaces", name))

    OUR = {"_cls": "fiftyone.core.stages.Select", "kwargs": [],
           "_uuid": SHOW_SAMPLES_STAGE_ID}
    USER = {"_cls": "fiftyone.core.stages.Match", "kwargs": [], "_uuid": "u-1"}

    def _mk_clear_ctx(stages):
        c = _FakeCtxHandler()
        panel_instance.on_load(c)
        c.ops = _FakeOps()
        c.request_params = {"view": list(stages)}
        c.triggers = []
        c.trigger = lambda name, params=None: c.triggers.append((name, params))
        _mem(c).selected_gidx = [g]
        return c

    # 해제 = 우리 _uuid 스테이지만 제거한 set_view 트리거 (2026-08-11).
    # ⚠️ show_samples(None) 은 App 검증(samples required)에서 죽어 execute 에 도달조차
    # 못 한다 — 이 계약이 회귀하면 해제가 조용히 안 먹는다(브라우저 실측으로 확인된 증상).
    cctx = _mk_clear_ctx([USER, OUR])
    panel_instance.on_clear_selection(cctx)
    assert _mem(cctx).selected_gidx == [] and _mem(cctx).sel_total == 0
    assert not any(c[0] == "show_samples" for c in cctx.ops.calls if isinstance(c, tuple)), \
        "회귀: 해제에 show_samples 사용 — required 검증에서 죽어 실행되지 않는다"
    assert cctx.triggers == [("set_view", {"view": [USER]})], cctx.triggers

    # 우리 칩이 없으면 사용자 뷰를 절대 건드리지 않는다
    cctx2 = _mk_clear_ctx([USER])
    panel_instance.on_clear_selection(cctx2)
    assert cctx2.triggers == [], cctx2.triggers

    # 더블클릭 훅도 같은 해제 경로 (현재 plotly 가 안 쏘지만 계약 유지)
    cctx3 = _mk_clear_ctx([OUR])
    panel_instance.on_plot_double_click(cctx3)
    assert cctx3.triggers == [("set_view", {"view": []})], cctx3.triggers

    # '선택 해제' 버튼 노출 조건: 선택이 있거나, 패널 상태는 비었는데 뷰 칩만 남았을 때
    def _ctrls(c):
        return panel_instance.render(c).type.properties["controls"].type.properties
    hctx.request_params = {"view": []}
    _mem(hctx).selected_gidx = [g]
    assert "clear_selection" in _ctrls(hctx)
    _mem(hctx).selected_gidx = []
    assert "clear_selection" not in _ctrls(hctx)
    hctx.request_params = {"view": [OUR]}          # F5 후: 상태는 비었지만 칩은 남음
    assert "clear_selection" in _ctrls(hctx), "회귀: 뷰 칩만 남으면 패널에서 해제 불가"
    hctx.request_params = {"view": [USER]}
    assert "clear_selection" not in _ctrls(hctx)   # 사용자 스테이지는 우리 소관 아님

    # emb_viz 방향은 **읽기 전용** — 뷰를 건드리면 사용자가 본 "add stage 추가" 재발
    ectx = _mk_clear_ctx([])
    ectx.ops = _FakeOps()
    ectx.triggers = []
    ectx.extended_selection = {"selection": list(ids[:3])}
    _mem(ectx).set("ext_sel_seen", [])
    panel_instance.on_change_extended_selection(ectx)
    assert ectx.ops.calls == [] and ectx.triggers == [], \
        "회귀: emb_viz lasso 가 뷰를 건드림 — 뷰 바에 스테이지가 생긴다"

    # 스테이지 누적 방지 계약 (2026-08-11 사용자 리포트): 프레임 반영은 반드시 내장
    # show_samples 여야 한다 — set_view(ctx.view.select(...))는 선택할 때마다 뷰 바에
    # Select stage를 하나씩 쌓고 교집합으로만 좁아진다.
    hctx.ops = _FakeOps()
    panel_instance._select_frames_view(hctx, ["fid1", "fid2"])
    assert hctx.ops.calls == [("show_samples", ["fid1", "fid2"])], hctx.ops.calls
    assert not any(c == "set_view" or (isinstance(c, tuple) and c[0] == "set_view")
                   for c in hctx.ops.calls), "회귀: set_view 직접 호출 — 스테이지 누적 재발"

    # 빈 extendedSelection 에코 무시 (리로드 버그 방어 — on_change_extended_selection 주석):
    # 훅 EXEC가 유발한 App의 extendedSelection 소거 잔향이 표/선택을 지우면 안 된다.
    _mem(hctx).selected_gidx = [g]
    hctx.extended_selection = {"selection": []}
    panel_instance.on_change_extended_selection(hctx)
    assert _mem(hctx).selected_gidx == [g], "회귀: 빈 extendedSelection 에코가 선택을 지움"

    # 표시(전체/채택만) 드롭다운은 argmax 전용 — dist_iou에는 미채택 개념이 없다 (정정)
    hctx.params = {"value": "dist_iou"}
    panel_instance.on_rule_change(hctx)
    dist_ctrls = panel_instance.render(hctx).type.properties["controls"].type.properties
    assert "show_mode" not in dist_ctrls and "rule" in dist_ctrls
    hctx.params = {"value": "argmax_k1"}
    panel_instance.on_rule_change(hctx)

    # ── Task 12: 프롬프트 짝이 없는 데이터셋에서 모드 A가 크래시 대신 안내를 낸다 ──
    # 옛 픽스처는 라이브 `frames_captions`(2026-08-07 실측상 짝 없음)였는데, 그 데이터셋이
    # 2026-08-19 에 `frames` 로 개명되면서 짝 `frames-prompts` 까지 생겨 no-pair 사례가 아니게
    # 됐다. 라이브 데이터셋에 픽스처를 매달면 개명·개통 때마다 이렇게 조용히 무효가 되므로
    # (옛 코드는 `if fo.dataset_exists(...)` 라 데이터셋이 사라지면 통째로 skip 됐다),
    # **짝이 없음을 그 자리에서 단언하는 합성 이름**을 쓴다. on_load 는 기본 모드 A 에서
    # `<name>-prompts` 존재 여부만 보고 즉시 반환하므로 본체 데이터셋은 실존할 필요가 없다.
    nopair_name = "__user_selftest_nopair__"
    assert not fo.dataset_exists(f"{nopair_name}-prompts")

    class _FakeDatasetNP:
        def __init__(self, name):
            self.name = name

    class _FakeCtxNoPair(_FakeCtxAttr):
        def __init__(self, dataset_name):
            super().__init__()
            self.dataset = _FakeDatasetNP(dataset_name)

    nopair_ctx = _FakeCtxNoPair(nopair_name)
    panel_instance.on_load(nopair_ctx)
    assert _mem(nopair_ctx).prompts_available is False, \
        "회귀: 프롬프트 짝 없는 데이터셋에서도 available=True로 남음"
    assert all(c == "clear" for c in nopair_ctx.panel.data_calls), \
        "회귀: set_data 호출됨 — patch 딥머지가 줄어든 배열을 못 지우므로 스키마 경로만 써야 한다"
    assert "clear" in nopair_ctx.panel.data_calls, \
        "회귀: data.clear() 미호출 — 옛 세션의 patched data가 스키마 data를 가린다"
    assert _get_fig(nopair_ctx) == [], \
        "회귀: 프롬프트 짝 없음인데 산점도에 데이터가 실림"
    assert _mem(nopair_ctx).scatter_data is None, \
        "회귀: scatter_data 가 state 에 실림 — 훅 왕복 페이로드 2.5s/MB 재발"
    assert NO_PROMPTS_PAIR_TEXT in _mem(nopair_ctx).banner
    nopair_schema = panel_instance.render(nopair_ctx)
    # 모드 A 전용 컨트롤(규칙/표시/버전)이 비활성 — 안내 텍스트만 렌더.
    nopair_ctrls = nopair_schema.type.properties["controls"].type.properties
    # 안내는 컨트롤 셀이 아니라 배너로 나간다 (2026-08-12 UI 정리)
    assert "no_prompts_notice" not in nopair_ctrls
    assert "banner_md" in nopair_schema.type.properties
    assert "bank_version_filter" not in nopair_ctrls
    assert "rule" not in nopair_ctrls and "show_mode" not in nopair_ctrls

    # ── 2026-08-14: 드롭다운 desync 버그 수정 검증 (재발화 에코·이중 발화·벡터화) ──
    # ① 같은 값 에코는 _refresh 자체를 건너뛴다 — App 훅 재발화 캐스케이드 차단.
    refresh_calls = []
    orig_refresh = panel_instance._refresh
    panel_instance._refresh = lambda c, update_plot=True: refresh_calls.append(1)
    try:
        hctx.params = {"value": _mem(hctx).rule}
        panel_instance.on_rule_change(hctx)
        hctx.params = {"value": _mem(hctx).color_by}
        panel_instance.on_color_change(hctx)
        hctx.params = {"value": SHOW_ALL_LABEL if _mem(hctx).show_unadopted
                       else SHOW_ADOPTED_LABEL}
        panel_instance.on_show_change(hctx)
        hctx.params = {"value": _mem(hctx).bank_version_filter}
        panel_instance.on_bank_version_change(hctx)
        hctx.params = {"value": _mem(hctx).mode}
        panel_instance.on_mode_change(hctx)
        hctx.params = {"value": _mem(hctx).group_field}
        panel_instance.on_group_field_change(hctx)
    finally:
        panel_instance._refresh = orig_refresh
    assert refresh_calls == [], \
        "회귀: 같은 값 재발화(App 훅 에코)가 _refresh 를 유발 — 렌더 폭풍/역순 덮어쓰기 재발"

    # ② 변경 dedup 가드: 실 요청(panel_id 있음)은 서버 기억(_APPLIED) 기준.
    class _PidCtx:
        params = {"panel_id": "selftest-pid-1"}
    # 최초 관측은 carried_same 이 True 여도 처리 (클라이언트 낙관적 업데이트 — 위 주석 참고)
    assert _change_guard(_PidCtx(), "rule", "dist_iou", True) is False
    assert _change_guard(_PidCtx(), "rule", "dist_iou", False) is True   # 이중 발화 → 흡수
    # 왕복 지연 중 재클릭: 요청이 실어 온 낡은 state 기준으론 '같은 값'(carried_same=True)
    # 이지만 서버가 마지막으로 반영한 값(dist_iou)과 다르므로 반드시 통과해야 한다 —
    # carried state 로 판정하면 진짜 변경이 삼켜져 UI 가 stale 로 굳는다 (2026-08-14 실측).
    assert _change_guard(_PidCtx(), "rule", "argmax_k1", True) is False
    class _NoPidCtx:
        params = {}
    assert _change_guard(_NoPidCtx(), "rule", "x", False) is False
    assert _change_guard(_NoPidCtx(), "rule", "x", True) is True, \
        "panel_id 없는 경로는 carried_same 폴백이어야 한다 (selftest/오프라인 호환)"

    # ③ 코드 배열 프리컴퓨트 정합: uniq[codes[k]] == str(원본[k]) (None → "").
    if b.get("bank_version") is not None:
        assert b.get("bank_version_codes") is not None
        for k in (0, len(b["gidx"]) // 2, len(b["gidx"]) - 1):
            want = "" if b["bank_version"][k] is None else str(b["bank_version"][k])
            assert b["bank_version_uniq"][b["bank_version_codes"][k]] == want
    # 서브샘플 캐시: MAX_POINTS 초과 번들이면 첫 build 가 캐시를 남기고 재호출과 동일 결과.
    if len(b["gidx"]) > MAX_POINTS:
        fig_c1 = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx=set())
        assert ALL_VERSIONS_LABEL in b.get("_subsample_cache", {}), \
            "회귀: 서브샘플 캐시 미적재 — 드롭다운 왕복마다 603k행 층화 재계산"
        fig_c2 = build_mode_a(b, rule="argmax_k1", show_unadopted=True, selected_gidx=set())
        assert [len(t["x"]) for t in fig_c1["data"]] == [len(t["x"]) for t in fig_c2["data"]]
    # 정수 코드 경로의 층화 계약: 클래스당 최소 1점 + 예산 준수 + 중복 없음.
    codes_fixture = np.asarray([0] * 100 + [1] * 10 + [2], dtype=np.int32)
    picked = stratified_subsample(codes_fixture, 20)
    assert len(picked) == len(set(picked)) == 20
    assert {int(codes_fixture[i]) for i in picked} == {0, 1, 2}

    # ④ on_dataset_open 기본 워크스페이스 오퍼레이터 (요청 2/4).
    dws = PiaDefaultWorkspace()
    assert dws.config.on_dataset_open is True and dws.config.unlisted is True
    class _FakeDsW:
        def __init__(self, ws):
            self._ws = ws
        def list_workspaces(self):
            return self._ws
    wctx = _FakeCtxHandler()
    wctx.ops = _FakeOps()
    wctx.dataset = _FakeDsW(["compare", "rules"])
    dws.execute(wctx)
    assert ("set_spaces", "compare") in wctx.ops.calls
    wctx2 = _FakeCtxHandler()
    wctx2.ops = _FakeOps()
    wctx2.dataset = _FakeDsW([])
    dws.execute(wctx2)
    assert wctx2.ops.calls == [], "compare 없는 데이터셋은 spaces 를 건드리지 않는다"

    print("selftest OK")


if __name__ == "__main__":
    selftest()
