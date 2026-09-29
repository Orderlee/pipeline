"""user-image-embeddings — 이미지(프레임) 임베딩 산점도 Panel.

왜 네이티브 Embeddings 패널이 아니라 자체 패널인가 (2026-08-14 사용자 요청):
  ① `<X>-prompts` 데이터셋의 `emb_viz` 는 **문장 임베딩**이다 (실측: gidx 603,318 개가
     전부 고유, 같은 이미지를 공유하는 22,578 샘플의 좌표 std 9.17 — 이미지 기준이면 0).
     그 화면에서 "이미지 임베딩" 을 보려면 **프레임 데이터셋(`<X>`)의 좌표**를 그려야 하는데,
     네이티브 패널은 현재 데이터셋의 brain run 만 그린다 — 크로스 데이터셋이 불가능.
  ② 네이티브 패널은 **마지막에 쓰던 brain key 를 데이터셋 간에 기억**해서, 한 데이터셋에만
     새 키를 만들면 다른 데이터셋에서 `Failed to load results for brain run` 으로 죽는다
     (reference_fiftyone_app_gotchas §1).
  ③ 네이티브 패널은 뷰의 전 샘플을 그린다 — 603,318 문장 샘플이 고작 2,528 개 이미지 위치에
     겹쳐 찍히며 렌더에 **110초**가 걸렸다(실측). 이미지 단위로 그리면 같은 그림을 2,528 점
     으로 낸다.

문장 패널(`sentence_embeddings`)의 **호버 문장은 Postgres 019 스키마가 정본**이다
(2026-08-19 "DB 연결 해야해 이제 gidx 그걸로 하지마"). 데이터셋 `text` 필드는 npz 를
`gidx % GIDX_OFFSET` 로 퍼온 파생물이고 43.3%가 자리표시자라 아예 로드하지 않는다 —
아래 "prompt DB" 블록 참고.

정본: docker/analysis/plugins/user-image-embeddings/ (git)
배포: docker cp → /data/fiftyone/datasets/__plugins__/user-image-embeddings/
      + 플러그인 **디렉토리 touch** (plugins_cache dir_state 무효화)
"""
import hashlib
import json
import os
import threading
from collections import OrderedDict

import fiftyone as fo
import fiftyone.operators as foo
import fiftyone.operators.types as types

BRAIN_KEY = "emb_viz"          # 하드코딩 — App/스크립트 전반의 고정 키 (gotchas §1)
PROMPTS_SUFFIX = "-prompts"
# 그리는 점 상한. 2026-08-19 20,000 → 200,000 상향 (사용자: "전체 이미지 및 프롬프트가
# 나와야 분석이 가능"). 실측으로 정당화된 값이지 넉넉히 잡은 값이 아니다:
#   frames        199,972점 → figJSON 26.60MB → 브라우저 완전 로드 20초 (캡처 실측)
#   frames-prompts 전량 603,318점 → 90.20MB  ← 못 쓴다. 200,000 에서 29.90MB 로 비슷해진다
# 200,000 은 이미지(199,972)를 **전량** 통과시키면서 문장 60만은 막는 경계다.
# ⚠️ 문장까지 전량(603,318)을 그리려면 점당 바이트를 줄여야 한다 — 실측 분해:
#   text(호버) 49.43MB(54.8%) · ids 16.89MB(18.7%) · x+y 23.88MB(26.4%)
#   호버는 문장 원문(≤110자)이라 원리적으로 안 줄어든다. 전량을 원하면 호버를 포기하고
#   클릭→패널 표시로 바꿔야 한다(26.09MB). 그건 UX 트레이드오프라 사용자 결정 사항.
MAX_POINTS = 200_000
# 이미지 단위라 문장 번들(192MB)보다 훨씬 작다. 다만 `<X>-prompts` 세션에서 문장 패널이
# 쓰는 번들은 60만 행 × 축 10개라 2026-08-19 실측 59.8MB 로 옛 64MB 예산에 붙어 있었다
# (DB 조인 키인 `gidx` 열 하나만 더해도 터진다) → 96MB. 엔트리는 여전히 1개만 유지한다.
CACHE_CAP_BYTES = 96 * 2**20

_CACHE = {}   # (dataset_name, brain_key, last_modified_at) -> bundle. 엔트리 1개 유지.

# 색칠 후보 — 프레임 데이터셋에 **실제로 있는 것만** 드롭다운에 뜬다 (sourcei/source-h 스키마가
# 다르다: sourcei 는 event_kind·category 보유, source-h 은 없음).
#
# ⚠️ 라벨에 **단위(영상/프레임)와 출처(사람/모델)를 반드시 박는다** (2026-08-14 사용자 요청:
#    "분석하는데 기준을 둬, 조금이라도 다르면 이용자가 차이를 알아야 해"). ground_truth 와
#    category 는 값 집합이 같아서(fire/smoke/falldown/normal) 이름만으로는 구분이 안 되는데
#    실측상 **일치율 69.4%** 인 서로 다른 축이다:
#      ground_truth — 영상 109개 중 105개(96%)에서 영상 내 상수 = **영상/이벤트 단위 사람 라벨**
#      category     — **사람 라벨이 아니라 v1.0.8.0 모델의 argmax 예측** (아래 항목 주석 참고)
#    ⚠️ 2026-08-14 정정: 한때 category 를 "프레임 단위 정답" 으로 적어 두었는데 **틀렸다**.
#    이 축을 정답으로 읽으면 구버전 예측으로 신버전을 채점하는 자기참조 평가가 된다.
# (라벨, 설명 — 설명은 배너에 그대로 실린다)
COLOR_CANDIDATES = [
    ("ground_truth", "정답 (영상 단위·사람)", "그 프레임이 속한 영상/이벤트의 클래스"),
    # ⚠️⚠️ `category` 는 **사람 라벨이 아니라 v1.0.8.0 모델의 argmax 예측**이다
    #    (2026-08-14 실측으로 확정, 세 갈래가 모두 같은 결론):
    #      · argmax(cos_best_fire|smoke|falldown|normal) == category.label → 7,498/7,498
    #      · category.confidence 가 7,498/7,498 전부 null (사람 라벨엔 confidence 가 없다)
    #      · pred_v1_0_8_0.label == category.label → 7,498/7,498
    #    사람이 모델의 오답까지 프레임 단위로 똑같이 재현할 수는 없다. 따라서 기준(ground_truth)
    #    과의 2,293장 차이는 "영상 단위 vs 프레임 단위" 가 아니라 **그 모델의 오류율(30.6%)** 이다.
    #    이 축을 정답으로 착각하고 신버전을 채점하면 v1.0.8.0 의 예측을 기준으로 삼는
    #    자기참조 평가가 된다 — source-h 에서 부호가 뒤집힌 사고(-5.3pp ↔ +8.2pp)와 같은 종류.
    ("category", "v1.0.8.0 예측 (모델)", "⚠️ 사람 정답 아님 — cos_best_* argmax 와 7,498/7,498 동일"),
    ("event_kind", "이벤트 종류 (영상 단위·사람)", "영상 분류 — near_miss·other 등 세부 종류 포함"),
    ("relabel_transition", "재라벨 전이 (프레임 단위)", "영상 라벨→프레임 실제 (예: falldown→normal)"),
    # ── `frames` 계열 축 (sourcei 에는 없다 — 스키마에 있는 것만 자동 노출된다) ──
    # frames 는 위 sourcei 필드명을 하나도 안 갖고 있어 색칠 축이 environment/daynight
    # 둘뿐이었는데, 그 둘은 199,972행이 전부 'none'/'(없음)' 이라 사실상 색칠이 없었다
    # (2026-08-19 사용자 지적: "카테고리 색칠이 안되고 있다"). 실제 클래스 축을 싣는다.
    # 채움 수는 전부 187,994 (= modality:frame 전량, 캡션 11,978 은 제외).
    # 목록·distinct 수는 fiftyone_app_setup.FRAMES_FILTER_GROUPS 의 §4-4 실측(2026-08-18)과
    # 맞춘다 — 그쪽이 이 데이터셋 축 선정의 정본이다(플러그인은 독립 배포 단위라 import 는
    # 안 하고 값만 맞춘다, 아래 axes_for 주석).
    #
    # ⚠️ 2026-08-19 사용자 정정: "뱅크 판정 말고 원래 이미지에 대한 라벨링되어있는 데이터가
    #    나와야지" — `normalized_class`(attach_labels 가 image_labels 에서 투영한 실제 검출
    #    클래스)가 `bank_pred`(프롬프트뱅크 채점을 한 번 더 거친 모델 판정)보다 이미지에 더
    #    가깝다. 기본 색칠축은 아래 DEFAULT_COLOR_BY 가 `normalized_class` 로 고정한다 —
    #    bank_pred 는 드롭다운 선택지로는 그대로 남는다(제거 아님).
    ("normalized_class", "검출 라벨 (이미지 자동 라벨링)",
     "그 프레임에 실제로 표시된 검출 클래스 — attach_labels 가 image_labels(SAM3)에서 투영. "
     "⚠️ 사람 GT 아님(이 데이터셋은 GT 0장) — 그래도 뱅크 판정보다 이미지에 더 가깝다"),
    ("detections", "박스 라벨 (이미지 자동 라벨링)",
     "그 프레임의 검출 박스 라벨들 — 라벨이 섞이면 '다중', 박스가 없으면 'none' "
     "(⚠️ 앵커 아님 — normalized_class 와 같은 SAM3 출처)"),
    ("bank_pred", "뱅크 판정 (모델)", "뱅크 채점 argmax — normal/falldown/fire/smoke"),
    ("pred_v1_0_8_0", "v1.0.8.0 예측 (모델)", "⚠️ 사람 정답 아님 — v1.0.8.0 뱅크 argmax"),
    ("bank_shift", "판정 전이 (A→B)", "뱅크 버전 간 판정 변화 (예: normal→falldown)"),
    ("runner_up", "2위 클래스 (모델)", "argmax 다음 순위 — 혼동 구조를 본다"),
    ("close_call", "판정 근접도 (모델)", "1·2위 근접도 구간 — 좁을수록(하위10%) 애매한 판정"),
    ("winner_site_scope", "판정 근거 사이트 폭 (모델)",
     "그 판정의 근거 문장이 몇 사이트에서 공통으로 나오는가 — 사이트 특이/공통"),
    ("project", "프로젝트 (수집 단위)", "수집 단위(사이트/카메라 묶음)"),
    ("modality", "행 종류 (frame/caption)", "frame 187,994 · caption 11,978"),
    ("environment", "실내/실외 (모델 추론)", "⚠️ 검증 정확도 54% — 참고용"),
    ("daynight", "주야 (모델 추론)", "검증 정확도 98.6%"),
    ("person", "사람 유무 (모델 추론)", "검증 정확도 100%"),
    ("weather", "날씨 (모델 추론)", "⚠️ 신뢰 불가 — 밝기를 읽는 것으로 확인됨"),
    ("camera", "카메라", "촬영 카메라(설치 위치) 단위"),
]

# 문장(`<X>-prompts`) 데이터셋용 축. **좌하 패널이 이걸 쓴다** — 그 데이터셋의 emb_viz 는
# 문장 좌표(603,318)라, 네이티브 Embeddings 패널로는 (a) brain key 를 매번 손으로 골라야
# 하고 (b) 고르면 60만 점을 그려 110초 + Chrome 크래시가 났다. 자체 패널이 층화 서브샘플로
# 2만 점만 그리면 **선택 단계가 사라지고** 6.4초에 뜬다 (2026-08-14 실측).
SENTENCE_CANDIDATES = [
    ("category", "문장 클래스 (뱅크)", "그 문장이 노리는 클래스 — 사람/모델 라벨이 아니라 뱅크 정의"),
    ("adopted", "채택 여부", "K=1 승자로 뽑혔는가 (미채택도 wave 분포엔 전부 참여)"),
    ("wave_role", "wave 역할", "분포 IoU 기여도 — 유익 상위10% / 유해 하위10% / 중간"),
    ("match", "최근접 적중", "이 문장의 최근접 이미지 정답이 문장 클래스와 같은가 (hit/miss)"),
    ("nearest_gt", "최근접 이미지 정답", "가장 가까운 이미지의 사람 라벨 — 문장 클래스와 다르면 miss"),
    ("purity_tier", "순도 구간", "승자 문장의 클래스 순도 (미채택은 None)"),
    ("bank_version", "뱅크 버전", "29개 버전 — 버전별 문장 집합 비교용"),
    ("nearest_daynight", "최근접 주야", "최근접 이미지의 주야 추론값"),
    ("nearest_person", "최근접 사람 유무", "최근접 이미지의 사람 유무 추론값"),
    ("nearest_environment", "최근접 실내/실외", "⚠️ 최근접 이미지의 실내외 추론값 (정확도 54%)"),
]


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
            # ⚠️ 2026-08-29: 걸러야 하는 것은 **출처가 아니라 gidx 규약**이다. `source='userwatch'`
            #    로 좁히면 같은 규약을 지키는 사내 뱅크(hybrid·internal)가 통째로 빠져
            #    조용히 `external_only` 폴백으로 떨어진다. `user-prompt-compare` 는 2026-08-28 에
            #    이미 고쳤는데 이 두 플러그인이 사본으로 남아 드리프트했다(3중 사본 패턴).
            #    규약 = `bank_sentences.gidx` 가 0 부터 시작하는 뱅크-로컬 행 번호.
            " GROUP BY 1, 2, 3 "
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
    # 2026-08-19 에 user-prompt-compare 가 먼저 고쳤는데 이 사본에는 반영되지 않아,
    # 여기서는 배너 경고만 뜨고 정렬 붕괴 버전이 계속 그려지고 있었다(2026-09-15 수정).
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


def axes_for(dataset_name):
    """대상 데이터셋에 맞는 축 목록. 문장 데이터셋과 이미지 데이터셋은 축이 완전히 다르다."""
    return SENTENCE_CANDIDATES if dataset_name.endswith(PROMPTS_SUFFIX) else COLOR_CANDIDATES


# 데이터셋별 권장 기본 색칠 축. 여기 없는 데이터셋(sourcei 포함)은 기존 그대로 axes_for() 순서의
# 첫 유효 축이 기본이다(sourcei 회귀 0 — 아래 default_color_by() 폴백 경로).
DEFAULT_COLOR_BY = {
    # 2026-08-19 사용자 정정: "뱅크 판정 말고 원래 이미지에 대한 라벨링되어있는 데이터가
    # 나와야지" — bank_pred(뱅크 채점을 거친 모델 판정)가 아니라 normalized_class(이미지에
    # 직접 붙은 검출 라벨)를 기본으로 삼는다. bank_pred 는 드롭다운에서 여전히 고를 수 있다.
    "frames": "normalized_class",
}


def default_color_by(dataset_name, fields):
    """데이터셋별 권장 기본 색칠 축 — `fields`(그 데이터셋의 실제 유효 축) 안에 있을 때만 쓴다.

    권장 축이 스키마 변경 등으로 사라져도 하드코딩된 이름에 팬닉하지 않고 항상 axes_for()
    순서의 첫 유효 축으로 안전하게 폴백한다(기존 동작과 동일한 경로 — sourcei 회귀 0).
    """
    want = DEFAULT_COLOR_BY.get(dataset_name)
    if want and want in fields:
        return want
    return fields[0] if fields else "전체"


# 기준 축 — 다른 클래스 축을 고르면 이것과 몇 장 어긋나는지 배너에 싣는다.
REF_AXIS = "ground_truth"
# 같은 값 집합(fire/smoke/falldown/normal 계열)을 써서 서로 비교가 의미 있는 축들.
# 여기 밖 축(주야·카메라 등)은 값 자체가 달라 불일치 수치가 무의미하므로 비교하지 않는다.
CLASS_AXES = ("ground_truth", "category", "event_kind")

# 클래스 고정색 — fiftyone_app_setup.CLASS_COLORS 와 동일 값 (배포 단위가 달라 복사 유지).
# ⚠️ 원본은 "unknown"/"none" 도 회색(#BBBBBB) 고정인데, 여기 사본엔 "none" 만 더한다 —
#    "unknown" 은 이미 sourcei event_kind 159행이 실사용 중이라(OKABE_ITO 순환색을 이미
#    받고 있다) 지금 추가하면 그 축의 기존 색이 바뀌는 회귀가 된다. "none" 은 2026-08-19
#    현재 어떤 기존 축에도 없는 값이라(신규 normalized_class/detections 전용) 안전하다 —
#    이 값이 frames 의 새 기본축(normalized_class)에서 지배 범주(59.9%)라 회색이 맞다.
CLASS_COLORS = {
    "fire": "#D55E00", "smoke": "#56B4E9", "falldown": "#E69F00",
    "normal": "#0072B2", "smoking": "#CC79A7", "person": "#009E73",
    "none": "#BBBBBB",
}
# 하이라이트 색 — ⚠️ 옛 값 `#F0E442`(노랑)는 **OKABE_ITO 팔레트에 들어 있어** 클래스
# 색과 충돌했다(그 색으로 칠린 그룹과 하이라이트를 구분할 수 없었다). 형제 패널
# user-prompt-compare 와 같은 마젠타로 통일한다 — 양쪽 팔레트 모두와 무교집합.
HILITE = "#FF2D95"

OKABE_ITO = ["#0072B2", "#E69F00", "#009E73", "#D55E00",
             "#CC79A7", "#56B4E9", "#F0E442", "#7F7F7F"]
# ⚠️ 8색을 넘는 그룹은 **색이 재사용된다** — event_kind(9종)에서 smoke(1,542)와
#    near_miss(1,503) 가 똑같은 #56B4E9 로 찍혀 분석자가 두 클래스를 구분할 수 없었다
#    (2026-08-14 실측). 색맹 안전 팔레트를 늘리는 대신 **마커 모양**을 바꿔 구분한다 —
#    9번째 그룹부터 diamond, 17번째부터 square. 색+모양 조합으로 24그룹까지 유일하다.
MARKER_SYMBOLS = ["circle", "diamond", "square"]

BANNER_CROSS = ("이미지 임베딩 — 점 1개 = 프레임 이미지 1장 "
                "(문장 산점도와 좌표계가 다르다: 독립 fit, 위치 비교 금지)")
NO_IMAGES_TEXT = "이미지 임베딩을 찾을 수 없습니다"
# 호버 문장/파일명을 **점마다** 실어 보낼 상한. 넘으면 호버는 클래스만(trace 단위, 점당
# 0 바이트) 남기고 개별 문장은 **클릭 표시**로 넘긴다. 2026-08-31 실측 근거:
#   호버 텍스트는 figure JSON 의 62%이고, 그 바이트는 플롯 이벤트마다 브라우저→서버로
#   되돌아온다(서버 2.5s/MB). 200,000점 문장 패널에서 클릭 반영이 165초였다.
#   호버를 온디맨드로 넘기면 18.66MB → 4.60MB, 왕복 93초 → 23초.
# 20,000 은 "작은 데이터셋의 호버 탐색은 손해 볼 이유가 없다"는 선(사용자 결정 (b)안):
#   sourcei 7,498 · sourcei-OPT 7,498 · sourcei-OPT-prompts 2,000 은 그대로 호버 유지,
#   sourcei-prompts 200,000 · frames 199,972 · frames-prompts 만 클릭 표시로 전환된다.
HOVER_TEXT_MAX_POINTS = 20_000
SHOW_SAMPLES_CAP = 500   # 그리드 반영 상한 — 요청 폭증 방지 (아래 on_plot_selected 주석)
SHOW_SAMPLES_STAGE_ID = "show_samples_stage_id"   # App 번들 ShowSamples 의 고정 _uuid


def view_without_our_selection(ctx):
    """우리가 건 **선택**을 뺀 뷰. 사용자 필터가 없으면 `None`(= 전량).

    선택은 "무엇을 강조할지"이지 "무엇을 그릴지"가 아니다. 그런데 `ctx.view` 는 클라이언트
    view 스테이지 + filters + **extended**(= extended selection) 를 전부 합친 결과라
    (`fiftyone/operators/executor.py` ExecutionContext.view 실측), 선택 직후 그대로 쓰면
    산점도가 선택한 점만 남기고 접힌다 — 2026-08-31 실측: 263점 박스 선택 직후 배너가
    "표시 263/263장" 으로 바뀌었다. 사용자의 실제 필터(뷰 바 스테이지·사이드바)만 남긴다.
    """
    rp = getattr(ctx, "request_params", None) or {}
    stages = [t for t in (rp.get("view") or [])
              if not (isinstance(t, dict) and t.get("_uuid") == SHOW_SAMPLES_STAGE_ID)]
    extended = {k: v for k, v in (rp.get("extended") or {}).items()
                if k != "fiftyone.core.stages.Select"}
    filters = rp.get("filters") or None
    if not stages and not filters and not extended:
        return None
    from fiftyone.server.view import get_view      # 지연 임포트 — 순수 함수 경로를 무겁게 안 한다
    return get_view(ctx.dataset, stages=stages, filters=filters,
                    extended_stages=extended or None)

def _has_our_stage(ctx):
    """뷰 바에 내장 show_samples 가 박은 우리 Select 스테이지가 있는가 (기억과 무관).

    서버 기억은 프로세스 재시작·플러그인 재임포트로 사라지지만 **뷰 바의 칩은 남는다**.
    그때 해제 버튼을 기억만 보고 숨기면 그리드가 좁혀진 채 풀 방법이 없어진다
    (코덱스 리뷰 F5 — 형제 패널 `user-prompt-compare._has_our_stage` 와 같은 계약).
    """
    rp = getattr(ctx, "request_params", None) or {}
    return any(isinstance(t, dict) and t.get("_uuid") == SHOW_SAMPLES_STAGE_ID
               for t in (rp.get("view") or []))


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


def _clear_our_stage(ctx):
    """내장 show_samples 가 박은 고정 `_uuid` 스테이지만 제거한다. 반환: 제거했는가.

    ⚠️ `ctx.ops.clear_view()` 를 쓰면 안 된다 (2026-09-09): App 의 ClearView 는
    `reset(view)` 라 **사용자가 뷰 바에 직접 건 스테이지까지** 날린다(matchtags 등).
    ⚠️ `show_samples(None)`/`show_samples([])` 도 불가 — 전자는 required 검증에 걸려
    execute 에 도달조차 못 하고, 후자는 JS 에서 빈 배열이 truthy 라 `Select([])` 가 붙어
    0장 그리드가 된다 (형제 패널 `user-prompt-compare._clear_frames_view` 실측).
    그래서 클라이언트가 실어 온 **원본 스테이지 목록**에서 우리 것만 빼고 되돌린다.
    """
    rp = getattr(ctx, "request_params", None) or {}
    stages = [t for t in (rp.get("view") or []) if isinstance(t, dict)]
    kept = [t for t in stages if t.get("_uuid") != SHOW_SAMPLES_STAGE_ID]
    if len(kept) == len(stages):
        return False                  # 우리 칩 없음 — 사용자 뷰를 건드리지 않는다
    # ctx.ops.set_view(view=...) 는 DatasetView 전용이라 raw stage 목록은 trigger 로 넘긴다.
    ctx.trigger("set_view", params={"view": kept})
    return True


# 산점도 trace 리스트의 서버측 보관소. panel state 에 실으면 이후 모든 훅 요청의
# panel_state + spaces 트리에 2벌로 왕복하는데, 서버가 요청 바디 **1MB 당 ~2.5초**를
# 태운다 (2026-08-14 curl 실측: 4MB POST = 10초). user-prompt-compare 와 같은 처치.
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


def _fig_key(ctx, panel):
    """fig 캐시 키 = (**패널**, 데이터셋, 색칠축).

    ⚠️ **패널 이름이 키에 있어야 한다** (2026-09-09): `-prompts` 계열 `compare` 워크스페이스는
    `sentence_embeddings` 와 `image_embeddings` 를 **동시에** 띄운다(실측:
    sourcei-prompts/frames-prompts/sourcei-OPT-prompts). 둘은 같은 세션 데이터셋을 보고
    색칠축도 같은 이름을 쓸 수 있어, 패널 성분이 없으면 한 패널이 **다른 패널의 figure** 를
    캐시 히트로 집어간다(문장 좌표 자리에 프레임 좌표).

    ⚠️ panel_id 로 키를 잡으면 안 된다 (2026-08-14 실측): panel_id 는 **훅 요청에만**
    실려 오고 render() 경로에는 없을 수 있어, _refresh 가 넣은 fig 를 render 가 못 찾아
    패널이 통째로 빈 채 그려졌다(배너·드롭다운·산점도 동시 소실). 데이터셋+색칠축은 양쪽
    경로에서 모두 접근 가능하고, 이 두 값이 같으면 fig 도 같다(선택 하이라이트는 아래
    build_figure 가 selected_ids 로 매번 다시 얹으므로 키에 넣지 않는다 — 선택이 바뀌면
    _refresh 가 어차피 새로 put 한다).
    """
    ds = getattr(ctx, "dataset", None)
    # ⚠️ **데이터 버전을 키에 넣는다** — 없으면 GT 를 바꿔도 옛 figure 가 계속 나온다
    #    (실측: fire 234 → 166 으로 바뀐 뒤에도 화면은 234 를 유지했다).
    # ⚠️ **뷰 지문도 넣는다** (2026-09-04): figure 는 `view_ids` 로 뷰에 의존하는데 키가
    #    뷰를 몰라서, `on_change_view` 가 상태를 안 쓰는 경로(아래 그 훅 참고)로 바뀐 뒤
    #    render 가 옛 뷰의 figure 를 캐시 히트로 계속 내줄 수 있었다.
    return (panel, ds.name if ds is not None else "-", _mem(ctx, panel).color_by or "",
            data_version(ds) if ds is not None else "", view_sig(ctx))


def view_sig(ctx):
    """현재 **뷰 바 스테이지 + 사이드바 필터 + extended + 그리드 선택** 의 지문
    (fig 캐시 키의 일부).

    `ctx.view` 대신 `request_params` 원본을 쓴다 — 뷰 재구성 비용 없이 "클라이언트가 지금
    무슨 뷰를 보고 있나"만 알면 되고, 같은 뷰의 훅 재발화 에코는 같은 지문이라 캐시 히트로
    공짜가 된다. `request_params` 가 없는 호출(selftest·오프라인)은 빈 뷰 지문으로 수렴한다.
    """
    rp = getattr(ctx, "request_params", None) or {}
    # ⚠️ **그리드 선택(`selected`)도 지문에 넣는다** (2026-09-09 사용자 리포트: "-prompts
    #    에서 samples 이미지를 선택해도 아무 선택 표시가 안 된다"). 체크박스 선택은 뷰
    #    스테이지도 extended 도 만들지 않으므로, 이 셋만 해싱하면 지문이 그대로다 →
    #    `_fig_key` 히트 → render 가 하이라이트 없는 옛 figure 를 그대로 낸다.
    #    이 패널의 하이라이트는 `_build_fig` 가 render 안에서 얹으므로(상태 쓰기 금지 —
    #    `on_change_view` 주석) **키가 미스가 되는 것만이 유일한 재구성 트리거**다.
    #    id 목록을 그대로 넣지 않고 정렬 후 해싱한다 — 순서만 다른 에코는 같은 지문.
    sel = sorted(str(i) for i in (getattr(ctx, "selected", None) or []))
    raw = json.dumps([rp.get("view") or [], rp.get("filters") or {},
                      rp.get("extended") or {}, sel], sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _put_fig(ctx, panel, data, banner=None, layout=None):
    """figure + 배너 + 레이아웃을 **한 엔트리**로 보관 (2026-09-04, codex 리뷰 반영 09-07).

    ⚠️ 셋을 두 dict 로 쪼개면 안 된다: 오퍼레이터는 스레드풀에서 **동시 실행**되므로
    (`executor.py` 의 `loop.run_in_executor`) 요청 A 가 data 를 쓴 뒤 멈추고 B 가 data+meta 를
    쓰고 A 가 meta 를 쓰면 **B 의 그림 + A 의 배너**가 남는다. 독립 축출 루프도 서로 다른 키를
    지울 수 있다. 한 엔트리 + 한 번의 조회면 그 조합 자체가 불가능하다.
    ⚠️ 배너를 여기 함께 두는 이유: 같은 뷰의 두 번째 렌더는 캐시 **히트**라 재구성을
    건너뛰는데, 그때 배너를 딴 데서 가져오면(옛 코드는 패널 상태) 필터 전 모집단
    ("표시 373/373장")이 되살아난다. 뷰 지문이 키에 있으니 여기 값은 항상 그 뷰의 것이다.
    """
    key = _fig_key(ctx, panel)
    with _FIGS_LOCK:
        _FIGS[key] = {"data": data, "banner": banner, "layout": layout}
        _FIGS.move_to_end(key)
        # 키에 뷰 지문이 들어가 카디널리티가 늘었다 — 상한도 같이 올린다(8 → 16).
        while len(_FIGS) > 16:
            _FIGS.popitem(last=False)


def _get_entry(ctx, panel):
    """이 뷰의 (data, banner, layout) 한 벌. 없으면 None. **조회는 한 번만** 한다.

    LRU: 히트하면 맨 뒤로 보내 **지금 보고 있는 엔트리가 축출되지 않게** 한다. FIFO 였을 때는
    활성 엔트리가 계속 가장 오래된 것으로 남아, 다른 클라이언트가 16개만 만들면 쫓겨났다
    (모드 B/C 는 render 재구성 폴백이 막혀 있어 그 순간 산점도가 빈 채로 굳는다).
    """
    key = _fig_key(ctx, panel)
    with _FIGS_LOCK:
        e = _FIGS.get(key)
        if e is not None:
            _FIGS.move_to_end(key)
        return e


def _get_fig(ctx, panel):
    e = _get_entry(ctx, panel)
    return e["data"] if e is not None else None


# ── 서버측 패널 기억 (2026-09-09) ────────────────────────────────────────────────
# ⚠️ **패널 상태에 "값이 바뀌는" 쓰기를 하면 사용자의 사이드바 필터가 지워진다.**
#    전 구간 확인(소스 + 헤드리스 실측, 2026-09-09):
#      ctx.panel.state.X = v → ops.patch_panel_state (`operators/panel.py:262`, 더티체크 없음)
#      → App `panelsStateAtom` → MainSpace 의 deep-equal 통과 시 `setSessionSpaces`(debounce 500ms)
#      → `router.replace(..., {event:"spaces"})` → DatasetPageQuery **network-only** 재조회
#      → 서버 `serialize_dataset` 이 조회마다 **새 ObjectId** 를 발급(`server/query.py:645`)
#      → 클라이언트 전용 `filters` 아톰이 `new.id !== prev.id` 로 스스로 비워진다.
#    실측: 사이드바 `ground_truth.label=fire` 로 155장 → 우리 색칠 드롭다운 변경 → **2초 뒤 6,032장**.
#    (같은 재조회가 `extendedSelection`·`similarityParameters` 도 함께 날린다.)
#
# ⇒ 규칙: **값이 바뀌는 상태 쓰기 = 사용자 필터 파괴.** (같은 값 재쓰기는 App 의 deep-equal
#   가드가 막아 무해하지만 왕복 낭비다.) 그래서 선택·배너·표·축 목록처럼 매번 바뀌는 값은
#   전부 이 서버측 기억에 둔다. **이 패널이 클라이언트로 내보내는 패널 상태는 0 이다** —
#   유일한 컨트롤(색칠)이 state 바인딩 없는 버튼 피커라 미러가 필요 없다(2026-09-09).
#
# 키 = (패널, 데이터셋). panel_id 를 쓰지 않는 이유는 `_fig_key` 주석과 같다(render 경로에
# 없을 수 있다는 2026-08-14 실측). 그 결과 같은 데이터셋을 연 탭들은 기억을 공유하는데,
# 이는 **오늘과 같은 의미**다 — 패널 상태는 세션(spaces)에 실려 이미 탭 간 공유였다.
# 콜드 키는 **빈 상태로** 시작한다 — on_load 가 기본값을 세운다. 요청이 실어 온 패널 상태를
# 시드로 쓰지 않는 이유는 아래 `_mem` 안의 F1 주석 참고(옛 배포가 남긴 최상위 키가 다른
# 데이터셋에서 되살아난다). 대가: 플러그인 프로세스가 재시작하면 컨트롤은 기본값에서 시작한다.
_MEM = OrderedDict()          # (panel, dataset) -> dict
_MEM_LOCK = threading.RLock()


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
        """지금 이 순간의 사본. **render 는 이걸로 읽는다** — 8개 값을 따로 읽는 사이
        동시 요청의 `_refresh` 가 키를 하나씩 갈아끼우면 "새 표 + 옛 배너" 가 섞인다
        (`_put_fig` 가 한 엔트리로 합쳐 막은 것과 같은 부류의 사고, 코덱스 리뷰 F6)."""
        with _MEM_LOCK:
            return _Mem(dict(self.__dict__["_d"]))




def _mem(ctx, panel):
    ds = getattr(ctx, "dataset", None)
    key = (panel, ds.name if ds is not None else "-")
    with _MEM_LOCK:
        d = _MEM.get(key)
        if d is None:
            # ⚠️ **요청이 실어 온 패널 상태로 시드하지 않는다** (2026-09-09 코덱스 리뷰 F1).
            #    클라이언트가 실어 오는 dict 에는 이 변경 이전 배포가 심어 둔 최상위 키
            #    (mode/fields/selected_ids/bank_version_filter/scatter_data…)가 **세션이 살아
            #    있는 한 계속** 들어 있다. 그걸 시드로 쓰면
            #      · 데이터셋을 바꿔도 `mode` 가 이미 있어 on_load 리셋이 안 돌고
            #        (메모리 §16 의 "전환 시 버전 필터 리셋" 계약 파괴),
            #      · 옛 데이터셋의 `fields`/`selected_ids` 가 그대로 살아나 유령 드롭다운·
            #        유령 "선택 해제 (1)" 이 뜬다 (2026-08-14 에 한 번 고쳤던 그 증상).
            #    콜드는 콜드로 시작한다 — on_load 가 기본값을 세운다.
            d = {}
            _MEM[key] = d
            # 상한 128: (패널 3종 × 데이터셋 수) 를 넉넉히 덮는다. 세션 중 축출이 일어나면
            # 그 패널이 콜드로 보여 컨트롤이 기본값으로 되돌아가므로 여유를 크게 잡는다.
            while len(_MEM) > 128:
                _MEM.popitem(last=False)
        _MEM.move_to_end(key)
    return _Mem(d)


_MISSING = object()
_APPLIED = {}   # (panel_id, control) -> 마지막 반영 값


def _change_guard(ctx, control, value, carried_same):
    """드롭다운 변경 dedup — user-prompt-compare 와 동일 계약.

    App 은 패널 오퍼레이터가 하나라도 실행되면 등록된 on_change 훅을 재발화하고, 드롭다운
    한 번에 같은 훅이 135ms 간격 2발 온다. 요청이 실어 오는 panel_state 는 1왕복 낡아서
    값 비교만으로는 '왕복 중 재클릭'을 삼킨다 → 서버가 마지막으로 반영한 값 기준으로 판정.
    panel_id 없는 호출(selftest·오프라인)은 carried_same 폴백.
    """
    pid = (getattr(ctx, "params", None) or {}).get("panel_id")
    if pid is None:
        return carried_same
    key = (pid, control)
    prev = _APPLIED.get(key, _MISSING)
    _APPLIED[key] = value
    if prev is _MISSING:
        # ⚠️ 첫 관측은 **무조건 처리**한다 (2026-08-14 실측 버그): 클라이언트는 드롭다운 값을
        #    낙관적으로 먼저 바꿔 그 값을 panel_state 에 담아 보낸다 → carried_same 이 이미
        #    True 라서, 서버 기억이 빈 상태(프로세스 재기동 직후 첫 클릭)에서 carried_same 을
        #    믿으면 **진짜 변경이 삼켜진다**. 증상: 드롭다운만 새 값이고 플롯·배너는 옛 축.
        #    같은 값 에코가 한 번 더 도는 비용(_refresh 1회)은 결과가 같아 무해하다.
        return False
    return prev == value


CROSS_HIGHLIGHT_MAX = 500
# 크로스 하이라이트를 계산할 세션 뷰 크기 상한. 뷰가 문장 수만 개로 좁혀진 경우 그 전부를
# 프레임으로 옮기면 하이라이트가 산점도를 통째로 덮어 의미가 없고 조회도 비싸다.


def sentence_ids_to_frame_ids(prompts_name, sent_ids, frames_name):
    """문장 샘플 id → 그 문장이 붙어 있는 **프레임 id** (filepath 경유).

    `-prompts` 세션에서 이 패널은 프레임 좌표를 그리므로 문장 샘플 id 를 그대로 비교하면
    교집합이 0 이다 — id 공간이 다르다 (2026-08-31 사용자 리포트: 뷰가 문장 1개로 좁혀졌고
    문장 패널은 노란 원으로 반영했는데 이미지 패널 범례에는 `선택` trace 가 없었다).

    ⚠️ 조인 축은 **filepath** 다. 승자 조인(`gidx % 100000` ↔ `winner_gidx_<ver>`)을 먼저
    구현했는데 그건 "이 문장이 이 프레임에서 **이겼나**" 라는 다른 질문이라 고른 문장 대부분이
    "대응 프레임 없음" 으로 나온다 — v1.0.12.0 실측: 7,498 프레임의 고유 승자 문장 545개뿐.
    사용자가 기대하는 관계는 "이 문장이 달린 이미지" 이고 filepath 가 그걸 답한다:
    커버리지 **100%** (문장 표본 3,000/3,000 실측 2026-08-31).
    """
    try:
        ps = fo.load_dataset(prompts_name)
        frames = fo.load_dataset(frames_name)
        fps = sorted({str(x) for x in ps.select(sent_ids).values("filepath") if x})
        if not fps:
            return []
        return sorted({str(i) for i in
                       frames.match(fo.ViewField("filepath").is_in(fps)).values("id")})
    except Exception:      # noqa: BLE001 — 매핑 실패가 패널을 죽이면 안 된다
        return []


_REFCACHE = {}   # (session, frames, last_modified_at) -> frozenset(frame id). 엔트리 2개 유지.


def session_referenced_frame_ids(session_name, frames_name):
    """이 세션(문장 데이터셋)이 **실제로 참조하는** 프레임 id 집합 (filepath 경유).

    크로스에서 프레임 전량을 그리면 대부분이 죽은 점이 된다 — 2026-08-31 실측:
    `sourcei-prompts` 는 `sourcei` 프레임 7,498개 중 **2,595개(34.6%)** 만 참조하고,
    나머지 4,903개는 이 세션에 문장이 없어 클릭해도 그리드에 보여줄 게 없다. 사용자는
    "선택이 안 된다" 로 읽는다(리포트 2건). 사용자 결정 (B)안: **참조하는 것만 그린다.**

    ⚠️ 기준은 **데이터셋**이지 현재 뷰가 아니다. 뷰로 좁히면 문장 1개 선택 직후 산점도가
    점 1개로 접힌다 — 강조와 필터를 뭉개는 그 실패는 이미 겪었다(`view_without_our_selection`).
    """
    ps = fo.load_dataset(session_name)
    key = (session_name, frames_name, str(ps.last_modified_at))
    if key in _REFCACHE:
        return _REFCACHE[key]
    frames = fo.load_dataset(frames_name)
    try:
        fps = [x for x in ps.distinct("filepath") if x]
        out = frozenset(str(i) for i in
                        frames.match(fo.ViewField("filepath").is_in(fps)).values("id"))
    except Exception:      # noqa: BLE001 — 실패 시 전량 그리기로 폴백(기능 정지보다 낫다)
        return None
    while len(_REFCACHE) >= 2:
        _REFCACHE.pop(next(iter(_REFCACHE)))
    _REFCACHE[key] = out
    return out


def frame_ids_to_sentence_ids(frames_name, frame_ids, prompts_name, cap):
    """프레임 id → 그 프레임에 달린 **문장 샘플 id** (filepath 경유) + 전체 개수.

    `sentence_ids_to_frame_ids` 의 역방향. 그리드를 좁히는 데 쓴다.

    ⚠️ 팬아웃이 심하게 치우쳐 있다 — 2026-08-31 실측(sourcei-prompts 607,318 / sourcei 7,498):
        평균 81 · **최대 22,578** · 문장이 0개인 프레임 4,903 / 7,498 · 고유 filepath 2,595
    옛 코드는 이 최대값을 근거로 크로스 그리드 반영을 **통째로 막았는데**, 그래서 문장 1개짜리
    프레임을 골라도 그리드가 안 움직였다 (2026-08-31 사용자 리포트: "image embedding 패널에서
    선택했는데 select 가 발동 안 했어요"). 무조건 차단이 아니라 **상한**으로 다룬다.
    """
    try:
        frames = fo.load_dataset(frames_name)
        ps = fo.load_dataset(prompts_name)
        fps = sorted({str(x) for x in frames.select(frame_ids).values("filepath") if x})
        if not fps:
            return [], 0
        v = ps.match(fo.ViewField("filepath").is_in(fps))
        total = v.count()
        return [str(i) for i in v.limit(cap).values("id")], total
    except Exception:      # noqa: BLE001 — 매핑 실패가 선택 자체를 막으면 안 된다
        return [], 0


def frames_dataset_name(session_name):
    """세션 데이터셋 이름 → 이미지(프레임) 데이터셋 이름.

    "sourcei-prompts" -> "sourcei",  "sourcei" -> "sourcei"
    """
    if session_name.endswith(PROMPTS_SUFFIX):
        return session_name[: -len(PROMPTS_SUFFIX)]
    return session_name


def _bundle_nbytes(b):
    import numpy as np
    return sum(v.nbytes for v in b.values() if isinstance(v, np.ndarray))


def _reduce_list_labels(v):
    """`Detections` 류 ListField 값(샘플당 label 리스트) → 색칠 가능한 스칼라 카테고리.

    ⚠️ 함정(2026-08-19 실사용 지적): `Detections` 문서는 `Classification` 과 달리 `.label`
    이 없다 — 그대로 `<field>.label` 로 읽으면 조용히 전부 `None` 만 돌아온다(예외가 아니라
    "silent wrong"). 정본 경로는 `<field>.detections.label` 인데, 이번엔 **샘플당 리스트**가
    나온다. 리스트를 그대로 `str()` 하면 라벨 조합마다 별개 카테고리가 돼 카디널리티가
    폭발하므로(수만 개 조합), 여기서 한 칸으로 접는다.

    FiftyOne 은 "그 필드 자체가 없음"과 "리스트가 비어 있음"을 둘 다 `None` 으로 접어(실측:
    캡션 11,978행 + 검출 0개인 프레임 112,543행이 합쳐져 `None` 124,521개) 구분이 안 되는데,
    구분할 필요가 없다 — 둘 다 "이 행엔 박스 라벨이 없다"는 같은 의미다.
    """
    if not v:
        return "none"
    uniq = sorted(set(v))
    return uniq[0] if len(uniq) == 1 else "다중"


def load_image_bundle(dataset_name):
    """이미지 좌표 + 색칠 메타 로드. 1024-d embedding 은 절대 읽지 않는다.

    ⚠️ 좌표↔샘플 매핑은 **brain result 의 sample_ids 기준**이다. `ds.values(...)` 순서와
    우연히 같더라도(실측 일치) 그 가정에 기대지 않는다 — 뷰/재정렬이 끼면 조용히 어긋나
    "클래스 색이 엉뚱한 점에 칠해지는" 최악의 오답이 된다.
    """
    import numpy as np
    ds = fo.load_dataset(dataset_name)
    key = (dataset_name, BRAIN_KEY, data_version(ds))   # data_version docstring 참고
    if key in _CACHE:
        return _CACHE[key]

    res = ds.load_brain_results(BRAIN_KEY)
    xy = np.asarray(res.points, dtype="float32")
    brain_ids = [str(i) for i in res.sample_ids]

    schema = ds.get_field_schema()
    axes = axes_for(dataset_name)
    have = [f for f, _lab, _desc in axes if f in schema]
    # Classification 은 `.label` 로 직접 읽는다 — `values(f)` 는 행마다 파이썬 객체를 만들어
    # 9~13배 느리다 (gotchas §13, 603k 행 실측 25.0s → 1.9s).
    # ⚠️ `Detections` 문서는 모양이 다르다 — `.label` 이 아니라 `.detections.label` 이고, 그
    # 경로는 **샘플당 리스트**를 돌려준다 (`_reduce_list_labels` 주석 참고). `list_fields` 에
    # 표시해 뒀다가 아래에서 리스트를 스칼라로 접는다.
    paths = ["id", "filepath"]
    list_fields = set()
    for f in have:
        fld = schema[f]
        if type(fld).__name__ != "EmbeddedDocumentField":
            paths.append(f)
            continue
        doc_name = getattr(getattr(fld, "document_type", None), "__name__", "")
        if doc_name == "Detections":
            paths.append(f + ".detections.label")
            list_fields.add(f)
        else:
            paths.append(f + ".label")
    # 문장 데이터셋은 DB 조인 키(`gidx`)를 함께 싣는다. **`text` 는 싣지 않는다** —
    # 그 필드는 npz 파생물이라 43.3%가 자리표시자이고, 60만 행 문자열이라 번들 예산도
    # 크게 먹는다. 호버 문장은 그려지는 점에 대해서만 DB 에서 배치 조회한다 (build_figure).
    sentence = dataset_name.endswith(PROMPTS_SUFFIX) and "gidx" in schema
    if sentence:
        paths.append("gidx")
    cols = ds.values(paths)
    ids, filepaths = [str(v) for v in cols[0]], cols[1]

    pos = {sid: i for i, sid in enumerate(ids)}
    order = np.asarray([pos.get(sid, -1) for sid in brain_ids])
    keep = order >= 0                      # brain index 에만 있고 데이터셋엔 없는 잔재 방어
    xy, order = xy[keep], order[keep]

    b = {"xy": xy,
         "id": np.asarray([brain_ids[i] for i in np.nonzero(keep)[0]], dtype=object),
         "filepath": np.asarray([filepaths[i] for i in order], dtype=object)}
    for f, col in zip(have, cols[2:]):
        vals = [col[i] for i in order]
        if f in list_fields:
            # `Detections.detections.label` 은 샘플당 리스트 — 스칼라로 접은 뒤에는 "(없음)"
            # 센티널이 필요 없다(빈 리스트도 "none" 이라는 실제 값으로 나온다).
            b[f] = np.asarray([_reduce_list_labels(v) for v in vals], dtype=object)
        else:
            b[f] = np.asarray(["(없음)" if v is None else str(v) for v in vals], dtype=object)
    if sentence:
        # int32 + `-1` 센티널: frames-prompts 는 캡션 행 11,978개의 gidx 가 None 이다
        # (뱅크 문장이 아니라 영상 캡션 — DB 조인 대상이 아니다). float NaN 을 쓰면
        # `% GIDX_OFFSET` 에서 조용히 이상한 값이 나오므로 정수 센티널로 못박는다.
        gcol = cols[-1]
        b["_gidx"] = np.asarray(
            [-1 if gcol[i] is None else int(gcol[i]) for i in order], dtype=np.int32)
    b["_sentence"] = bool(sentence)
    b["_name"] = dataset_name
    # ⚠️ **값이 한 종류뿐인 축은 색칠로 내놓지 않는다.** frames 의 environment·daynight 이
    #    199,972행 전부 'none' 인데도 드롭다운에 떠서, 고르면 전 점이 한 색이 되고 배너는
    #    "두 축이 100% 동일" 이라고만 말했다 — 크래시가 아니라 조용한 무의미다
    #    (2026-08-19 사용자 지적). `(없음)` 센티널을 뺀 실값이 2종 미만이면 제외한다.
    useful = []
    for f in have:
        real = {v for v in b[f].tolist() if v != "(없음)"}
        if len(real) >= 2:
            useful.append(f)
    b["_fields"] = useful
    b["_degenerate"] = [f for f in have if f not in useful]
    # 축 메타를 번들에 실어 둔다 — 이후 배너/드롭다운/경고가 **대상 데이터셋의 축 정의**를
    # 따라간다 (이미지 패널과 문장 패널이 같은 함수를 공유하는 방법).
    b["_axes"] = list(axes)

    assert _bundle_nbytes(b) <= CACHE_CAP_BYTES, (
        f"캐시 예산 초과: {_bundle_nbytes(b)/2**20:.1f}MB "
        f"> {CACHE_CAP_BYTES/2**20:.0f}MB (배열 바이트 기준)")
    _CACHE.clear()
    _CACHE[key] = b
    return b


def stratified_subsample(labels, max_points, seed=0):
    """클래스 비례 서브샘플, 클래스당 최소 1점 보장. 인덱스 리스트 반환."""
    import numpy as np
    arr = np.asarray(labels)
    n = len(arr)
    if n <= max_points:
        return list(range(n))
    rng = np.random.default_rng(seed)
    out, extra = [], []
    for u in np.unique(arr):
        idxs = np.nonzero(arr == u)[0]
        k = max(1, int(round(len(idxs) / n * max_points)))
        pick = rng.choice(idxs, size=min(k, len(idxs)), replace=False).tolist()
        out.append(pick[0])
        extra.extend(pick[1:])
    rng.shuffle(extra)
    return sorted(out + extra[: max(0, max_points - len(out))])


def _color_for(group, i):
    return CLASS_COLORS.get(group, OKABE_ITO[i % len(OKABE_ITO)])


def _symbol_for(group, i):
    """8색 순환을 넘어가는 그룹은 마커 모양으로 구분 (위 MARKER_SYMBOLS 주석)."""
    if group in CLASS_COLORS:
        return "circle"          # 고정색 클래스는 항상 원 — 팀 공통 표기 유지
    return MARKER_SYMBOLS[(i // len(OKABE_ITO)) % len(MARKER_SYMBOLS)]


def identical_axes(bundle, axis):
    """선택 축과 **값이 100% 동일한** 다른 축들. 자기참조 평가 함정 자동 노출용.

    2026-08-14: sourcei 의 `category` 가 `pred_v1_0_8_0` 와 7,498/7,498 동일한데 이름만
    보고 "정답" 으로 읽던 사고가 있었다. 두 축이 완전히 같으면 그건 **독립 정보가 아니다** —
    한쪽으로 다른 쪽을 채점하면 안 된다. 배너가 이걸 스스로 말하게 한다.
    """
    import numpy as np
    a = bundle.get(axis)
    if a is None:
        return []
    same = []
    for f in bundle.get("_fields", []):
        if f == axis:
            continue
        o = bundle.get(f)
        if o is not None and len(o) == len(a) and not np.any(a != o):
            same.append(f)
    return same


def axis_note(bundle, axis, ref=REF_AXIS):
    """색칠 축 설명 + **기준 축과의 불일치**를 한 줄로. (2026-08-14 사용자 요청)

    "조금이라도 다르면 이용자가 차이를 알아야 한다" — ground_truth 와 category 처럼 값
    집합이 같은 축은 이름만 보면 같은 것처럼 읽히므로, 고른 축이 기준(영상 단위 정답)과
    **몇 장 어긋나는지** 숫자로 박아 둔다. 값 집합이 다른 축(주야·카메라 등)은 불일치
    수치가 무의미하므로 설명만 싣는다.
    """
    import numpy as np
    axes = bundle.get("_axes") or COLOR_CANDIDATES
    desc = {f: d for f, _l, d in axes}.get(axis, "")
    parts = [desc] if desc else []
    if axis == ref:
        parts.append("**기준 축**")
    a, r = bundle.get(axis), bundle.get(ref)
    if axis != ref and axis in CLASS_AXES and a is not None and r is not None:
        n = len(a)
        diff = a != r
        # ⚠️ "다름" 을 한 숫자로 뭉치면 오해를 부른다 (2026-08-14): event_kind 는 기준(4클래스)
        #    밖 값(near_miss·other·drop…)을 갖기 때문에 4,321장이 달라지는데, 그건 상충이
        #    아니라 **더 세분화된 분류**다. 반대로 category 의 2,293장은 양쪽 다 같은 4클래스
        #    안에서 값이 갈리는 **진짜 상충**이다. 두 종류를 나눠 표기한다.
        in_ref = np.isin(a, np.unique(r))
        conflict = int(np.count_nonzero(diff & in_ref))
        finer = int(np.count_nonzero(diff & ~in_ref))
        ref_lab = {f: l for f, l, _d in axes}.get(ref, ref)
        if conflict:
            parts.append(f"⚠️ 기준({ref_lab})과 **{conflict:,}장 상충** "
                         f"({conflict / n * 100:.1f}%)")
        if finer:
            parts.append(f"기준에 없는 세분값 {finer:,}장")
        if not conflict and not finer:
            parts.append(f"기준({ref_lab})과 100% 일치")
    # 값이 완전히 같은 축이 있으면 **독립 정보가 아니라는 사실**을 알린다 (identical_axes 주석)
    same = identical_axes(bundle, axis)
    if same:
        labs = {f: l for f, l, _d in axes}
        parts.append("⚠️ " + "·".join(labs.get(f, f) for f in same) + "와 **값이 100% 동일** "
                     "(같은 정보 — 서로 채점 금지)")
    return " · ".join(parts)


SENTENCE_UNRESOLVED = "(DB 미보유 문장)"


def _sentence_texts(bundle, idx):
    """문장 번들의 그려질 행들 → ({행: 문장}, 출처 메타). 이미지 번들이면 (None, None).

    문장 정본은 DB(`bank_sentences`) 하나뿐이다 — 이 패널은 데이터셋 `text` 를 아예
    로드하지 않으므로 **폴백이 없다**. DB 가 안 되면 호버가 `(DB 미보유 문장)` 이 되고
    배너가 그 사실을 밝힌다 (조용히 파일명을 보여주던 옛 동작보다 정직하다).
    """
    b = bundle
    if not b.get("_sentence") or b.get("_gidx") is None:
        return None, None
    rows = [int(i) for i in idx]
    gid = [int(b["_gidx"][i]) for i in rows]
    gid = [None if g < 0 else g for g in gid]           # 캡션 행(-1 센티널)은 조인 대상 아님
    bv, cat = b.get("bank_version"), b.get("category")
    counts = b.get("_ver_counts")
    if counts is None:
        counts = pdb_version_counts([] if bv is None else list(bv))
        b["_ver_counts"] = counts                       # 게이트 ② 분모 = 데이터셋 전체 행 수
    texts, meta = pdb_resolve_texts(
        [None if bv is None else str(bv[i]) for i in rows],
        gid,
        [SENTENCE_UNRESOLVED] * len(rows),
        counts,
        categories=None if cat is None else [str(cat[i]) for i in rows],
    )
    return dict(zip(rows, texts)), meta


def _point_label(bundle, k, hov):
    """호버 첫 줄. 문장 패널이면 DB 문장, 이미지 패널이면 파일명."""
    if hov is not None:
        return str(hov.get(int(k), SENTENCE_UNRESOLVED))[:110]
    return str(bundle["filepath"][k]).rsplit("/", 1)[-1]


def point_label_for_id(bundle, rid):
    """샘플 id 하나 → 호버에 쓰던 그 라벨 (없으면 None).

    호버 상한을 넘긴 큰 플롯에서 per-point text 를 안 보내는 대신, 클릭한 **한 점만**
    이 경로로 되찾는다 (문장이면 DB 1행 조회). 전량 전송의 대안이라 비용이 O(1) 이다.
    """
    import numpy as np
    hit = np.nonzero(bundle["id"] == str(rid))[0]
    if len(hit) == 0:
        return None                     # 세대 불일치·뷰 밖 — 오답 대신 무표시
    k = int(hit[0])
    hov, _meta = _sentence_texts(bundle, np.asarray([k]))
    return _point_label(bundle, k, hov)


def _none_group_label(bundle, row_idx):
    """`(없음)` 그룹의 범례 표기 — 전부 캡션 행이면 정직하게 `(캡션)` 으로 밝힌다.

    frames 의 Classification 류 축(bank_pred 등)은 캡션 11,978행에 **구조적으로** 값이
    없다(그 행은 프레임이 아니라 영상 캡션이라 애초에 이 필드가 없다) — "라벨링 결측" 으로
    오독하지 않도록 범례에 정체를 그대로 밝힌다(2026-08-19 사용자 지적). `modality` 축이
    없는 데이터셋(sourcei 등)이거나 결측이 캡션만으로 안 설명되면 기존 표기를 그대로 쓴다
    — 저장된 그룹핑 값(`bundle[color_by]`)은 안 건드리고 **표시 문자열만** 바꾼다.

    ⚠️ **전건-캡션 조건은 부재 등식이었다** (2026-08-19): 결측이 전부 캡션일 때만 정직해지고
    프레임 하나만 섞이면 구조적 부재(캡션)와 실제 미라벨 프레임이 한 범례로 뭉갰다. 이제
    modality 로 갈라 둘 다 적는다 — 프레임 결측이 0 이면 뒷부분은 생략해 기존 표기를 보존한다.
    반환값은 `(범례 문자열[개수 포함], 호버 표기[개수 없음])` 2-튜플이다 — 호버는 값을
    보여주는 자리라 개수가 들어가면 안 된다 (호출부가 개수를 덧붙이지 않는다).
    """
    n = len(row_idx)
    modality = bundle.get("modality")
    if modality is None or n == 0:
        return f"(없음) {n}", "(없음)"
    sub = modality[row_idx]
    n_cap = int((sub == "caption").sum())
    n_oth = n - n_cap
    if n_cap == 0:
        return f"(없음) {n}", "(없음)"          # 캡션이 없으면 전부 실제 결측
    if n_oth == 0:
        return f"(캡션) {n}", "(캡션)"          # 전건 캡션 — b05e4ba 동작 보존
    return f"(캡션) {n_cap} · (라벨없음·프레임) {n_oth}", "(캡션/라벨없음)"


def _id_mask(ids, wanted):
    """`ids`(object dtype 배열)의 각 원소가 `wanted`(set) 에 있는지 → bool 마스크.

    ⚠️ `np.isin` 을 쓰면 안 된다. object dtype 은 해시 경로를 못 타서 정렬·비교로
       떨어진다 — 실측(2026-08-20, 199,972 × 2,477): **np.isin 3.73s vs set 0.01s (373배)**.
       렌더마다 도는 자리라 뷰 필터를 넣자마자 패널이 "Still loading" 으로 굳었다.
    """
    import numpy as np
    w = wanted if isinstance(wanted, (set, frozenset)) else set(wanted)
    return np.fromiter((x in w for x in ids), dtype=bool, count=len(ids))


def build_figure(bundle, color_by, selected_ids=None, cross_note="",
                 banner_text=BANNER_CROSS, view_ids=None, limit_ids=None):
    """이미지 산점도. trace = 색칠 그룹별 1개 + 마지막 하이라이트.

    그룹별 trace 로 쪼개는 이유: Plotly 범례는 trace 단위라, 단일 trace + per-point 색
    배열이면 범례에 클래스→색 매핑이 아예 안 나온다.
    """
    import numpy as np
    b = bundle
    n_all = len(b["xy"])
    idx = np.arange(n_all)
    groups = b.get(color_by)
    if groups is None:
        groups = np.asarray(["전체"] * n_all, dtype=object)
    sel = set(selected_ids or ())
    # ── 크로스 모집단 제한: 이 세션이 참조하는 프레임만 (사용자 결정 (B)안) ──
    #    뷰 필터와 **별개**다: 이쪽은 데이터셋 기준 모집단이고, 아래 view_ids 는 사용자 필터다.
    limit_note = ""
    if limit_ids is not None:
        idx = idx[_id_mask(b["id"], limit_ids)]
        sel &= set(limit_ids)
        limit_note = f" · 세션 참조 {len(idx):,}/{n_all:,}장"
    # ── 현재 뷰로 좁히기 (뷰 바 matchtags·사이드바 필터 등) ──
    #    `view_ids is None` = 필터 없음(전량). **빈 집합과 다르다** — 빈 집합은
    #    "뷰가 0건" 이라 정말로 아무것도 안 그리는 게 맞다.
    view_note = ""
    if view_ids is not None:
        idx = idx[_id_mask(b["id"], view_ids)]
        sel &= set(view_ids)        # 뷰 밖 선택은 되살리지 않는다 (아래 keep_sel 과 짝)
        view_note = f" · 뷰 {len(idx):,}/{n_all:,}장"
    n = len(idx)                    # 배너 분모 = **뷰 모집단** (전체가 아니다)
    n_all_note = limit_note
    if n > MAX_POINTS:
        base = idx
        sub = np.asarray(stratified_subsample(groups[base], MAX_POINTS), dtype=np.int64)
        idx = base[sub]             # 층화가 돌려준 위치를 원본 인덱스로 되돌린다
        # 선택된 점은 서브샘플에서 탈락해도 **반드시 남긴다** (코드리뷰 지적, 2026-08-14):
        # 층화는 현재 색칠축 기준이라 축을 바꾸면 살아남는 표본이 달라져, 방금 고른 점이
        # 하이라이트만이 아니라 산점도에서 통째로 사라진다 — 조용해서 더 나쁘다.
        # (현 데이터 7,498·13,144 는 MAX_POINTS 아래라 아직 미도달 경로.)
        if sel:
            # 선택 복원도 **뷰 안에서만** — 뷰 밖 점을 되살리면 필터가 거짓말이 된다.
            keep_sel = base[_id_mask(b["id"][base], sel)]
            idx = np.union1d(idx, keep_sel)
    # 문장 패널은 호버에 **DB 정본 문장**을 싣는다 (2026-08-19). 예전에는 이 자리에
    # `filepath` 파일명이 떴는데, 문장 샘플의 filepath 는 "가장 가까운 이미지"라 문장을
    # 식별하지 못했다. 그려지는 점만(≤MAX_POINTS) 배치 조회한다 — 전량 로드 금지.
    # 그려지는 점이 상한을 넘으면 per-point 호버를 아예 만들지 않는다 — DB 배치 조회까지
    # 건너뛰므로 서버 쪽 비용도 함께 사라진다 (HOVER_TEXT_MAX_POINTS 주석 실측 참고).
    hover_off = len(idx) > HOVER_TEXT_MAX_POINTS
    hov, text_meta = (None, None) if hover_off else _sentence_texts(b, idx)
    # ── 정렬 붕괴 버전은 **그리지 않는다** (2026-09-15, user-prompt-compare 2026-08-19 의 이식) ──
    #    배너 경고만으로는 조용한 오답이 남는다: 화면에 점이 있는 한 분석자는 그 버전을
    #    비교에 넣는다. 그런데 corrupt 버전은 gidx 정렬이 깨져 **벡터 귀속 자체가 틀렸다** —
    #    v1.0.2.0 을 그리면 사실상 v1.0.2.1 을 자기 자신과 비교하게 된다.
    #    판정 근거는 pdb_resolve_texts 의 meta["corrupt"] 주석 참고.
    #    ⚠️ hover_off(점이 상한 초과)면 DB 조회를 아예 안 해 corrupt 를 알 수 없다 —
    #       그 경우는 드롭되지 않는다. 조회 비용을 되살리는 건 별도 판단 사항.
    bad_vers = set((text_meta or {}).get("corrupt") or ())
    bv_all = b.get("bank_version")
    if bad_vers and bv_all is not None:
        keep = np.asarray([str(bv_all[i]) not in bad_vers for i in idx], dtype=bool)
        if not keep.all():
            text_meta["dropped_n"] = int((~keep).sum())
            idx = idx[keep]
            if hov is not None:
                alive = {int(i) for i in idx}
                hov = {k: v for k, v in hov.items() if k in alive}
    g_sub = groups[idx]
    data = []
    # ⚠️ 그리기 순서 = z-order (plotly 는 뒤 trace 를 위에 그린다). 구현이 CLASS_COLORS
    #    **dict 순서**를 따라서, 희소 클래스가 먼저(아래) 깔리고 다수 클래스가 그 위를 덮었다
    #    — event_kind 실측: fire(229) 가 index 0 이라 near_miss(1,503)·other(2,459) 에
    #    가려 화면에서 사라졌다. **개수 내림차순**으로 깔면 희소 클래스가 항상 위에 온다.
    #    (색은 여전히 CLASS_COLORS 고정 — 순서와 색은 별개다.)
    present = [(int((g_sub == u).sum()), u) for u in set(g_sub.tolist())]
    order = [u for _n, u in sorted(present, key=lambda t: (-t[0], str(t[1])))]
    # 색·모양 인덱스는 **CLASS_COLORS 우선순위 기준**으로 고정한다 — 그리기 순서가 바뀌어도
    # 같은 그룹이 항상 같은 색/모양을 받아야 화면 간 비교가 된다.
    keyed = list(CLASS_COLORS) + sorted(set(g_sub.tolist()) - set(CLASS_COLORS))
    cidx = {u: i for i, u in enumerate(keyed)}
    for grp in order:
        i = cidx.get(grp, 0)
        m = g_sub == grp
        ii = idx[m]
        # 범례 표시 문자열 — 그룹핑 키(`grp`)는 그대로 두고 **문자열만** 정직하게 바꾼다.
        # `(없음)` 은 개수까지 포함한 전체 문자열을 돌려받는다 (캡션/프레임 분리 집계).
        name, hov_label = (_none_group_label(b, ii) if grp == "(없음)"
                           else (f"{grp} {int(m.sum())}", grp))
        trace = {
            "type": "scattergl", "mode": "markers",
            "name": name,
            # ⚠️ float64 로 넓힌 **뒤** 반올림한다. xy 는 float32 라 그대로 round 하면
            #    1.3 이 float64 확장에서 1.2999999523162842 로 되살아나 오히려 안 줄어든다
            #    (2026-08-19 실측: 26.60MB → 26.57MB = 사실상 무동작).
            "x": np.round(b["xy"][ii, 0].astype("float64"), 3).tolist(),
            "y": np.round(b["xy"][ii, 1].astype("float64"), 3).tolist(),
            "ids": [str(b["id"][k]) for k in ii],
            # ⚠️ 접미사 `<br>{축}={그룹}` 은 **이 trace 안에서 전 점이 같은 값**이라 per-point
            #    text 에 넣으면 순수 중복이다 — 2026-08-31 실측: 200,000점에서 3.64MB
            #    (figure JSON 의 16.3%), 7,498점에서 0.17MB(20.3%). 그 바이트는 플롯
            #    이벤트마다 브라우저→서버로 되돌아오므로(서버 2.5s/MB) 그대로 지연이 된다.
            #    hovertemplate 은 trace 당 1벌만 실려 가고 **화면 표기는 완전히 동일하다.**
            "marker": {"color": _color_for(grp, i), "size": 6, "opacity": 0.9,
                       "symbol": _symbol_for(grp, i),
                       "line": {"width": 0.5, "color": "#FFFFFF"}},
        }
        if hover_off:
            # 클래스만 trace 단위로 (점당 0 바이트). 개별 문장/파일명은 클릭 → 패널 표시.
            trace["hovertemplate"] = f"{color_by}={hov_label}<extra></extra>"
        else:
            trace["text"] = [_point_label(b, k, hov) for k in ii]
            trace["hovertemplate"] = f"%{{text}}<br>{color_by}={hov_label}<extra></extra>"
            trace["hoverinfo"] = "text"    # hovertemplate 미지원 경로 폴백
        if grp == "none":
            # 지배 카테고리(예: normalized_class 의 '검출 없음' 59.9%)를 범례에서 기본
            # 흐리게/치워 둔다 — Plotly 는 legendonly trace 도 범례 클릭 한 번으로 다시
            # 켜지는 **네이티브 토글**이라 "지운다" 가 아니라 "치운다" 다(2026-08-19 사용자
            # 요청: "'none'이 지배 범주라 범례에서 흐리게/토글 가능하게"). 데이터·표시 카운트
            # (banner의 "표시 X/Y장")는 그대로 유지 — 안 보이는 건 렌더 상태일 뿐이다.
            trace["visible"] = "legendonly"
        data.append(trace)
    hi = idx[_id_mask(b["id"][idx], sel)] if sel else idx[:0]
    # 하이라이트는 **한 trace**로 유지한다 (형제 패널과 동일 계약): 별도 후광 trace 를
    # 얹으면 이 파일의 `data[:-1]`(하이라이트 빼고 전부) 가정이 여러 곳에서 깨진다.
    data.append({
        "type": "scattergl", "mode": "markers", "name": "선택",
        "x": np.round(b["xy"][hi, 0].astype("float64"), 3).tolist(),
        "y": np.round(b["xy"][hi, 1].astype("float64"), 3).tolist(),
        "ids": [str(b["id"][k]) for k in hi],
        # 옛 값 `#F0E442`(노랑)는 **OKABE_ITO 팔레트에 들어 있어** 그 색으로 칠린 그룹과
        # 하이라이트를 구분할 수 없었다. 마젠타 + 굵은 흰 링으로 통일 (2026-08-31).
        "marker": {"color": HILITE, "size": 13, "symbol": "circle",
                   "line": {"width": 4, "color": "#FFFFFF"}},
    })
    shown = sum(len(t["x"]) for t in data[:-1])
    lab = {f: l for f, l, _d in (b.get("_axes") or COLOR_CANDIDATES)}.get(
        color_by, color_by)
    # ⚠️ 배너는 **한 줄(단일 문단)** 로 유지한다 (2026-08-14 실측): `\n\n` 으로 문단을 나누면
    #    md 가 <p> 여러 개로 렌더되고, 축을 바꿨을 때 **첫 문단만 매칭돼 뒷 문단이 옛 텍스트로
    #    남는다** — 플롯은 새 축인데 배너는 이전 축을 가리키는 최악의 어긋남이 났다.
    #    (user-prompt-compare 의 배너도 단일 줄이라 정상 갱신된다.) 길어도 md 가 폭에 맞춰 접는다.
    # 색칠 정보를 **맨 앞**에 둔다 — 사용자가 축 차이를 먼저 봐야 한다는 요구(2026-08-14).
    note = axis_note(b, color_by)
    banner = f"**색칠: {lab}**" + (f" — {note}" if note else "")
    banner += f" · 표시 {shown:,}/{n:,}장{n_all_note}{view_note} · {banner_text}"
    if hover_off:
        # 기능을 조용히 뺐다고 오인하지 않게 **이유와 대체 경로**를 배너에 밝힌다.
        banner += (f" · 호버는 클래스만 (점 {shown:,} > {HOVER_TEXT_MAX_POINTS:,} — "
                   f"문장/파일명 전량 전송이 클릭 반영을 수십 초 지연시킨다). "
                   f"**개별 값은 점을 클릭**하면 아래에 뜬다")
    if cross_note:
        banner += f" · {cross_note}"
    if text_meta is not None:
        # 조용한 폴백 금지 — 호버 문장을 DB 에서 몇 행 가져왔고 어느 버전이 빠졌는지 밝힌다.
        banner += " · " + pdb_note(text_meta)
    return {"data": data, "banner": banner,
            # height 고정 금지 — 실높이는 render() 의 vh 기반 view height 가 정한다.
            # title 금지 — plotly title 은 modebar 와 같은 영역이라 글자가 아이콘에 겹친다.
            # ⚠️ `uirevision` 필수: 이 값이 그대로면 Plotly 가 **사용자 조작**(dragmode·줌·
            # 범례 토글)을 layout 재전송보다 우선한다. 없으면 refresh 마다 dragmode 가
            # "pan" 으로 되돌아가, 모드바로 Box Select 를 켜 둔 사이에 패널이 한 번만
            # 갱신돼도 선택 도구가 조용히 꺼진다 (2026-08-31 실측). 데이터셋이 바뀌면
            # 좌표계가 달라지므로 그때만 리셋한다.
            "layout": {"showlegend": True, "dragmode": "pan", "autosize": True,
                       "uirevision": str(bundle.get("_name") or "user"),
                       "xaxis": {"visible": False}, "yaxis": {"visible": False},
                       "margin": {"l": 10, "r": 10, "t": 30, "b": 10}}}


class ImageEmbeddingsPanel(foo.Panel):
    # 서브클래스(문장 패널)가 갈아끼우는 지점 — 나머지 로직은 전부 공유한다.
    # NAME 은 오퍼레이터 이름이자 **서버측 기억·fig 캐시의 키 성분**이다 (`_mem`/`_fig_key`):
    # 두 패널이 한 워크스페이스에 함께 뜨므로 이게 없으면 서로의 상태를 집어간다.
    NAME = "image_embeddings"
    BANNER = BANNER_CROSS
    NOT_FOUND = NO_IMAGES_TEXT
    # ⚠️ 플롯 높이는 **그 패널이 놓인 칸 크기**에 맞춰야 한다 (2026-08-14 사용자 지적):
    #    이미지 패널은 워크스페이스 우측 = 화면 **전체 높이**를 쓰므로 100vh 기준이 맞지만,
    #    문장 패널은 좌측 스택의 아래 칸 = **화면 절반**(1080 뷰포트에서 약 495px)이다.
    #    같은 100vh 예산(560px)을 쓰면 산점도 아래쪽이 칸 밖으로 잘려 나간다(실측).
    PLOT_HEIGHT = "max(400px, calc(100vh - 520px))"
    # 같은 패널이 **두 자리**에 놓인다. 위 값은 전체높이 칸 기준이다.
    #   · `<X>-prompts` compare 우측 = 전체높이 칸        → PLOT_HEIGHT
    #   · 프레임 데이터셋 compare 좌하단 = 세로 스택 아래 칸 → HALF_PANE_PLOT_HEIGHT
    # 후자에 전체높이 예산을 쓰면 칸을 넘쳐 잘린다 — DOM 실측(2026-08-19, 1800×1000):
    # 플롯 top 591 · height 480 · bottom 1071 로 뷰포트 1000 을 **71px 초과**했다.
    #
    # ⚠️ `calc(100vh - Npx)` 형태로는 **어떤 N 을 넣어도 한 화면 크기에서만** 맞는다.
    #    플롯 상단이 뷰포트에 비례해 내려가기 때문이다 — 4개 뷰포트 실측:
    #        vh  800 1000 1200 1440
    #      top   507  591  675  776      → 기울기 84/200 = 0.42
    #    이 0.42 는 우연이 아니라 compare 워크스페이스 세로 분할 `sizes=[0.42, 0.58]`
    #    의 그 값이다 (위 칸 Samples 가 42%). 즉 top = 0.42·vh + 171px 이고
    #    남는 높이 = 0.58·vh - 171px 다. 상수항 171 = 앱 헤더 + 색칠 컨트롤 + 배너.
    #    처음에 `calc(100vh - 600px)` 로 고쳤다가 vh=1000 에서만 맞고 1200/1440 에서
    #    75px/176px 벗어나는 걸 사용자가 잡았다. **한 지점만 재고 고쳤다 하지 말 것.**
    #    여백 10px 을 둬서 58vh - 181px.
    #    ⚠️ 워크스페이스 분할을 바꾸면 이 58 도 같이 바꿔야 한다 (fiftyone_app_setup
    #       `_compare_space` 의 left_stack sizes).
    # ⚠️ 워크스페이스 패널 state(`pane="half"`)로 알려주는 방식을 먼저 시도했다가 버렸다 —
    #    워크스페이스 문서에는 저장되는데(확인함) 커스텀 패널의 `ctx.panel.state` 로는
    #    전달되지 않아 조용히 기본값을 탔다. 대신 **세션 데이터셋 이름**으로 가른다:
    #    이 판별은 같은 클래스의 `target_dataset()` 이 이미 쓰는 것과 동일한 검사다.
    HALF_PANE_PLOT_HEIGHT = "max(180px, calc(58vh - 181px))"

    def plot_height(self, ctx):
        """놓인 칸에 맞는 플롯 높이.

        프레임 데이터셋 세션 = compare 좌하단(절반 칸), `-prompts` 세션 = 우측(전체 칸).
        """
        ds = getattr(getattr(ctx, "dataset", None), "name", None)
        if ds and frames_dataset_name(ds) == ds:
            return self.HALF_PANE_PLOT_HEIGHT
        return self.PLOT_HEIGHT

    def target_dataset(self, session):
        """그릴 좌표의 출처 데이터셋. 이미지 패널은 프레임 데이터셋(크로스 가능)."""
        return frames_dataset_name(session)

    def view_ids(self, ctx):
        """현재 뷰가 **이 패널이 그리는 데이터셋**을 좁히고 있으면 그 id 집합, 아니면 None.

        `None` = 필터 없음(전량 그린다). 빈 집합과 구분해야 한다 — 빈 집합은 "뷰가 0건"
        이므로 정말로 아무것도 안 그리는 게 맞다.

        ⚠️ **크로스 데이터셋에서는 절대 걸면 안 된다.** `-prompts` 세션에서 이 패널은
           `frames` 좌표를 그리는데, 그 세션의 뷰 id 로 거르면 교집합이 0 이라 화면이
           통째로 빈다 (크래시가 아니라 조용한 빈 화면).
        """
        session = getattr(getattr(ctx, "dataset", None), "name", None)
        if session is None or self.target_dataset(session) != session:
            return None
        # request_params 가 있을 때만 재구성한다 — 없으면(테스트 스텁 등) ctx.view 가 진실.
        view = getattr(ctx, "view", None)
        if getattr(ctx, "request_params", None):
            try:
                view = view_without_our_selection(ctx)
            except Exception:   # noqa: BLE001 — 뷰 재구성 실패가 패널을 죽이면 안 된다
                view = getattr(ctx, "view", None)
        # 스테이지가 없으면 전량이다 — 199,972건 `values("id")` 왕복(실측 0.97s)을 피한다.
        # (필터된 뷰는 2,477건 0.18s 로 싸다.)
        if view is None or not getattr(view, "_stages", None):
            return None
        try:
            return {str(i) for i in view.values("id")}
        except Exception:       # noqa: BLE001 — 뷰 조회 실패가 패널을 죽이면 안 된다
            return None

    def _cross_highlight(self, ctx, session, frames_name):
        """`-prompts` 세션에서 **현재 뷰의 문장들에 대응하는 프레임**을 하이라이트 대상으로.

        돌려주는 값: `(프레임 id 리스트 또는 None, 배너에 붙일 안내 또는 None)`.
        `None` = 하이라이트를 건드리지 않는다(패널 자기 선택을 그대로 둔다).
        """
        view = getattr(ctx, "view", None)
        if view is None or not getattr(view, "_stages", None):
            return None, None          # 뷰가 전량 — 세션 선택으로 볼 게 없다
        try:
            n = view.count()
        except Exception:              # noqa: BLE001
            return None, None
        if n == 0:
            return [], None
        if n > CROSS_HIGHLIGHT_MAX:
            # 조용히 포기하지 않는다 — 왜 하이라이트가 없는지 배너가 밝힌다.
            return None, (f"세션 뷰 {n:,}개는 하이라이트 상한 {CROSS_HIGHLIGHT_MAX:,} 초과 "
                          f"— 프레임 강조 생략")
        try:
            sent_ids = [str(i) for i in view.values("id")]
        except Exception:              # noqa: BLE001
            return None, None
        fids = sentence_ids_to_frame_ids(session, sent_ids, frames_name)
        if not fids:
            return [], (f"세션 뷰의 문장 {n:,}개에 대응하는 프레임이 없다")
        return fids, f"세션 뷰 문장 {n:,}개 → 프레임 {len(fids):,}개 강조"

    def _grid_highlight(self, ctx, session, frames_name, cross):
        """Samples 그리드에서 체크한 샘플 → 하이라이트 대상 id.

        돌려주는 값: `(id 리스트 또는 None, 배너 안내 또는 None)`.
        `None` = 그리드 선택이 없다(하이라이트를 건드리지 않는다).

        크로스(`-prompts` 세션에서 프레임 좌표를 그릴 때)면 고른 것은 **문장** 샘플이라
        id 공간이 달라 그대로 비교하면 교집합 0 이다 — `sentence_ids_to_frame_ids` 로
        filepath 경유 변환한다(그 함수 주석: 커버리지 100%).
        ⚠️ 프레임→문장 팬아웃은 평균 81 · **최대 22,578** 로 극단적으로 치우쳐 있으므로
        (반대 방향도 같은 표가 근거다) 변환 **입력**에 상한을 건다. 조용히 포기하지 않고
        배너가 사유를 밝힌다 — `_cross_highlight` 와 같은 계약.
        """
        ids = [str(i) for i in (getattr(ctx, "selected", None) or [])]
        if not ids:
            return None, None
        if not cross:
            return ids, f"그리드 선택 {len(ids):,}개 강조"
        if len(ids) > CROSS_HIGHLIGHT_MAX:
            return None, (f"그리드 선택 {len(ids):,}개는 상한 {CROSS_HIGHLIGHT_MAX:,} 초과 "
                          f"— 프레임 강조 생략")
        fids = sentence_ids_to_frame_ids(session, ids, frames_name)
        if not fids:
            # ⚠️ `[]` 가 아니라 `[""]`… 도 아니고 **빈 리스트를 그대로** 돌려주되, 호출부가
            #    `is not None` 으로 받으므로 세션 뷰 폴백으로 새지 않는다 (코덱스 nit):
            #    빈 리스트는 "강조할 게 없다" 이지 "그리드 선택이 없다" 가 아니다.
            return [], f"그리드 선택 문장 {len(ids):,}개에 대응하는 프레임이 없다"
        return fids, f"그리드 선택 문장 {len(ids):,}개 → 프레임 {len(fids):,}개 강조"

    def on_change_view(self, ctx):
        """뷰 바 스테이지(matchtags 등)·사이드바 필터 변경 → 다시 그린다.

        이 훅이 없으면 패널은 전체 데이터셋만 읽고 뷰 변화에 **아무 반응이 없다**
        (2026-08-20 사용자 지적: matchtags 에 source-e 을 넣어도 무반응).
        FiftyOne 은 패널이 이 이름의 메서드를 정의한 경우에만 이벤트를 보낸다
        (`fiftyone/operators/panel.py` 의 ctx_change_events).

        ⚠️ **여기서 패널 상태를 쓰면 안 된다** (2026-09-04 실측). `_refresh` 가 남기는
        `patch_panel_state` 는 App 에서 `setSpacesMutation` → `DatasetPageQuery` 재조회로
        이어지는데, 사이드바 필터는 서버 세션이 아니라 **클라이언트에만** 있는 상태라 그
        재조회에 통째로 날아간다 — 사용자가 `pred_correct_*` 를 골라도 0.7초 뒤 전량으로
        돌아오던 증상의 정체 (뷰 바 스테이지는 세션에 있어 살아남는다).
        그래서 이 훅은 **아무것도 하지 않는다**: App 은 훅 실행 직후 `render()` 를 부르고,
        `_fig_key` 에 뷰 지문이 들어 있어 render 가 새 뷰의 figure·배너를 재구성한다
        (같은 뷰의 재발화 에코는 캐시 히트라 공짜).

        ✅ 2026-09-09: 남아 있던 컨트롤·선택 경로까지 전부 닫았다 — 패널의 모든 상태는
        서버측 `_mem` 에 있고 클라이언트로 나가는 것은 컨트롤 미러 하나뿐이며 그것도
        클라이언트가 낡았을 때만 나간다. 셀프테스트의 "사이드바 필터 보존 계약" 블록이
        전 훅(`on_change_view`/`on_change_selected`/`render`/`_refresh`/색칠 변경/선택·해제)에
        대해 상태 쓰기 0 을 못 박는다.
        """
        return

    def on_change_selected(self, ctx):
        """Samples 그리드 체크박스 선택 → 다시 그린다. **바디는 의도적으로 비어 있다.**

        이 훅이 없으면 FiftyOne 은 이 패널에 selection 이벤트를 **아예 보내지 않는다**
        (`fiftyone/operators/panel.py` 의 `ctx_change_events` 는 패널이 그 이름의 메서드를
        정의한 경우에만 등록한다) → `Panel.execute` 가 안 불리고 `render` 도 안 불려서,
        그리드에서 이미지를 골라도 산점도가 **아무 반응이 없다** (2026-09-09 사용자 리포트:
        "-prompts 에서 samples 이미지를 선택해도 아무 선택 표시가 안 된다"). 하이라이트를
        얹는 코드는 이미 있었고(`_build_fig`), 그걸 깨울 이벤트가 없던 것이다.

        ⚠️ **과거에 이 훅을 만들었다가 원복한 이력이 있다** (2026-08-31: "훅 재발화 +
        stale panel_state 에코와 dedup 시그니처가 핑퐁해 패널이 로딩에서 못 나왔다").
        재도입이 안전하다고 판단한 근거 3개 — 논거가 아니라 실측이다:
          ① **바디가 아무 상태도 쓰지 않는다.** 에코 핑퐁은 훅이 `ctx.panel.state` 를 쓰고
             dedup 가드로 판정할 때 생긴다. 여기엔 쓰기도 가드도 없다. 같은 파일의
             `on_change_view` 가 정확히 같은 패턴(바디 `return`)으로 이미 검증돼 있다.
          ② 형제 패널 `_refresh` docstring 의 *"훅 바디 no-op이어도 (네이티브 emb_viz
             extendedSelection) 파괴"* 실측은 **우리 패널이 네이티브 Embeddings 패널과 함께
             마운트된 경우**의 이야기다. 2026-09-09 전 데이터셋 워크스페이스 실측:
             `compare`(= `user_default_workspace` 가 여는 기본값)에는 네이티브 `Embeddings`
             가 **없고**(Samples + 우리 패널만), 네이티브를 쓰는 워크스페이스(`frames` 의
             `explore`·`explore-class`·`4-site`·`bank-eval`)에는 **우리 패널이 없다**.
             마운트되지 않은 패널에는 이벤트가 가지 않으므로 그 조합이 성립하지 않는다.
             ⚠️ 사용자가 `+` 로 네이티브 Embeddings 를 우리 패널 옆에 직접 붙이면 그 전제가
             깨진다 — 그때 lasso 선택이 스스로 사라지면 여기를 먼저 의심할 것.
          ③ lasso 는 이 훅으로 오지 않는다 (형제 패널 Task 5 실측: "on_change_selected 는
             lasso 에 반응하지 않는다, 0 ids 유지"). 즉 이 훅은 그리드 체크박스 전용이다.
        `execute()` 가 훅 직후 **같은 ctx** 로 `render()` 를 부르고, `_fig_key` 에 그리드
        선택 지문이 들어 있어 render 가 새 선택 기준으로 figure 를 재구성한다.

        ⚠️ 피드백 경로 (형제 패널 opus F5 실측): `_apply_selection` 이 부르는
        `ctx.ops.show_samples()` 의 뷰 변경은 `ctx.selected` 를 **비우고** 이 훅을 빈 값으로
        재발화시킨다. 그래도 이 패널의 자기 선택은 안 지워진다 — `_build_fig` 가
        서버 기억의 `selected_ids` 를 그리드 선택(ctx)보다 **먼저** 보기 때문이다.
        그 우선순위가 이 에코에 대한 방어이기도 하다.
        """
        return

    @property
    def config(self):
        # ⚠️ 패널 이름을 바꾸면 **저장된 워크스페이스가 옛 이름을 가리켜** App 이
        #    `Panel "<옛이름>" no longer exists!` 를 띄운다 — 이름 변경 시 반드시
        #    `fiftyone_app_setup.py workspace-compare` 재실행으로 워크스페이스를 다시 저장할 것.
        #    (2026-08-14: user_image_embeddings → image_embeddings 로 변경, 등록된 91개
        #    오퍼레이터에 image_embeddings 는 없어 충돌 없음 — 네이티브 Embeddings 패널은
        #    App 내장이라 오퍼레이터 레지스트리에 아예 없다.)
        return foo.PanelConfig(name=self.NAME,
                               label="Image Embeddings", surfaces="grid")

    # ── 상태 ──
    def mem(self, ctx):
        """이 패널·이 데이터셋의 서버측 상태 (`_mem` 주석 — 상태 쓰기가 필터를 지운다)."""
        return _mem(ctx, self.NAME)

    def on_load(self, ctx):
        # ⚠️ **이미 있는 값을 덮지 않는다** (2026-08-14 실사용 버그): on_load 는 패널
        #    리마운트·빈 상태 에코·render 자가복구로 **반복 호출**된다. 여기서 color_by 를
        #    무조건 None 으로 밀면 사용자가 고른 색칠 축이 매번 기본값으로 되돌아가
        #    "드롭다운을 바꿔도 그림이 안 바뀐다" 로 보인다 (gotchas §16 과 같은 증상 —
        #    compare 패널의 알려진 미수정 버그를 여기서 되풀이하지 않는다).
        #    데이터셋이 바뀌어 그 축이 없어진 경우는 _refresh 가 첫 필드로 교체해 준다.
        st = self.mem(ctx)
        st.color_by = st.color_by or None
        st.selected_ids = st.selected_ids or []
        st.fields = []
        st.available = True
        self._refresh(ctx)

    def _build_fig(self, ctx, b, color_by, session, frames_name, cross):
        """현재 ctx 기준 figure 1개. **`_refresh` 와 render 폴백이 공유한다.**

        ⚠️ 공유하는 이유가 실제 버그였다 (2026-09-04): `on_change_view` 가 상태를 안 쓰는
        경로로 바뀌면서 뷰 변경 렌더는 render 폴백만 타는데, 그 폴백이 `_refresh` 와 달리
        **크로스 하이라이트(`_cross_highlight`)와 그 note 를 빼먹고** 있었다 — `-prompts`
        세션에서 뷰를 건드리면 강조가 조용히 사라졌다. 두 경로가 같은 함수를 부르게 해
        divergence 자체를 없앤다. 상태를 쓰지 않는 순수 조립부만 여기 둔다.
        """
        note = f"출처: `{frames_name}` (세션은 `{session}`)" if cross else ""
        sel_ids = self.mem(ctx).selected_ids
        # ⚠️ **직접 한 선택이 항상 이긴다.** 이 가드가 없으면 뷰 바에 Select 스테이지가
        #    남아 있는 동안 매 refresh 가 세션 뷰 강조로 되돌려, 이 패널에서 박스 선택을
        #    해도 "선택이 안 되는" 것처럼 보인다 (2026-08-31 사용자 리포트: "반대로
        #    image embedding 패널에서 box select 했는데 선택이 안 돼 / select 를 닫고
        #    해야 하는거야?" — 닫아야 되는 게 맞았고 그게 버그였다).
        #    세션 뷰 강조는 **자기 선택이 없을 때만** 쓰는 폴백이다.
        if not sel_ids:
            # ── 그리드 체크박스 선택 (2026-09-09) ──────────────────────────────
            #    `ctx.selected` 는 훅·render 양쪽 ctx 에 실려 온다(FiftyOne 1.19
            #    `ExecutionContext.selected`). 크로스면 문장 id 라 프레임 id 로 옮긴다.
            gs, gnote = self._grid_highlight(ctx, session, frames_name, cross)
            if gnote:
                note = f"{note} · {gnote}" if note else gnote
            if gs is not None:
                sel_ids = gs
                if not gs:
                    # 그리드 선택은 있는데 강조할 대상이 0 — 여기서 끝낸다. 아래 세션 뷰
                    # 폴백으로 흘러가면 배너가 "대응 프레임 없다 + 뷰 강조 N개" 로 모순된다.
                    return build_figure(
                        b, color_by, selected_ids=[], cross_note=note,
                        banner_text=self.BANNER, view_ids=self.view_ids(ctx),
                        limit_ids=(session_referenced_frame_ids(session, frames_name)
                                   if cross else None))
        if cross and not sel_ids:
            # 세션 **뷰**(문장 필터) 기반 폴백 — 그리드 선택도 자기 선택도 없을 때만.
            #    ⚠️ 예전 주석은 "새 `on_change_selected` 훅을 만들지 않는다" 였다. 그 판단은
            #    *뷰가 이미 바뀌는* 경로(형제 패널이 선택을 뷰로 승격)에서만 옳았고, 뷰를
            #    바꾸지 않는 **그리드 체크박스**는 그래서 영영 도달하지 못했다 — 위 훅의
            #    docstring 참고 (상태를 안 쓰는 no-op 훅으로 되돌렸다).
            xs, xnote = self._cross_highlight(ctx, session, frames_name)
            if xs is not None:
                sel_ids = xs
            if xnote:
                note = f"{note} · {xnote}" if note else xnote
        return build_figure(
            b, color_by, selected_ids=sel_ids, cross_note=note,
            banner_text=self.BANNER, view_ids=self.view_ids(ctx),
            # 복구 렌더도 **같은 모집단**을 써야 한다 — 아니면 그 경로에서만 죽은 점
            # 4,903 개가 되살아나 사용자가 다시 못 누르는 점을 만난다.
            limit_ids=session_referenced_frame_ids(session, frames_name) if cross else None)

    def _refresh(self, ctx, update_plot=True):
        # ⚠️ **컨트롤 미러가 없다** (2026-09-09): 이 패널의 유일한 컨트롤(색칠)이 state 바인딩
        #    없는 버튼 피커로 바뀌면서, 클라이언트로 내보낼 패널 상태가 하나도 남지 않았다.
        #    = 이 패널은 이제 사용자의 사이드바 필터를 **어떤 경로로도** 지우지 않는다.
        self._refresh_body(ctx, update_plot)

    def _refresh_body(self, ctx, update_plot=True):
        st = self.mem(ctx)
        # 옛 set_data 잔재가 있으면 스키마 data 를 영원히 가린다 (App `mt||view.data` 우선순위)
        ctx.panel.data.clear()

        session = ctx.dataset.name if ctx.dataset is not None else None
        if session is None:
            return

        def _unavailable(msg):
            # ⚠️ 컨트롤 상태도 **반드시** 함께 비운다: fields 를 남기면 데이터셋을 전환했을 때
            #    배너는 "찾을 수 없습니다" 인데 색칠 드롭다운에는 이전 데이터셋의 필드가
            #    그대로 남아 고를 수 있는 유령 컨트롤이 된다 (코드리뷰 지적, 2026-08-14).
            st.available = False
            st.fields = []
            st.axes = []
            st.selected_ids = []
            st.banner = msg
            _unavail_layout = {"xaxis": {"visible": False},
                               "yaxis": {"visible": False}}
            st.layout = _unavail_layout
            _put_fig(ctx, self.NAME, [], banner=msg, layout=_unavail_layout)

        frames_name = self.target_dataset(session)
        cross = frames_name != session
        if not fo.dataset_exists(frames_name):
            _unavailable(f"{self.NOT_FOUND} — `{frames_name}` 데이터셋이 없습니다")
            return
        try:
            b = load_image_bundle(frames_name)
        except Exception as e:      # brain run 부재/재빌드 중 — 크래시 대신 안내 (gotchas §12)
            _unavailable(f"{self.NOT_FOUND} — {type(e).__name__}: {e}")
            return

        st.available = True
        st.fields = list(b["_fields"])
        st.axes = list(b["_axes"])
        color_by = st.color_by
        if color_by not in b["_fields"]:
            # frames_name 기준(세션이 아니라 **좌표 출처 데이터셋** 기준) — -prompts 세션에서
            # 크로스로 그릴 때도 이미지 데이터셋의 권장 축을 그대로 따라간다.
            color_by = default_color_by(frames_name, b["_fields"])
            st.color_by = color_by
        if update_plot:
            fig = self._build_fig(ctx, b, color_by, session, frames_name, cross)
            st.banner = fig["banner"]
            st.layout = fig["layout"]
            _put_fig(ctx, self.NAME, fig["data"],
                     banner=fig["banner"], layout=fig["layout"])

    # ── 컨트롤 ──
    def on_toggle_color_picker(self, ctx):
        """색칠 축 피커 열기/닫기. 상태는 서버 기억에만 — 패널 상태를 쓰지 않는다."""
        st = self.mem(ctx)
        st.picker = None if st.picker == "color" else "color"
        self._refresh(ctx, update_plot=False)

    def on_color_change(self, ctx):
        v = (ctx.params or {}).get("value")
        st = self.mem(ctx)
        if not v:
            return
        if _change_guard(ctx, "color_by", v, v == st.color_by):
            # 같은 값 재선택·에코 — 값은 그대로 두되 **피커는 닫는다** (안 닫으면 같은 축을
            # 다시 눌렀을 때 목록이 열린 채 남아 "안 먹는다" 로 보인다).
            if st.picker == "color":
                st.picker = None
                self._refresh(ctx, update_plot=False)
            return
        st.color_by = v
        st.picker = None
        self._refresh(ctx)

    # ── 선택 ──
    #   크로스(-prompts 세션의 이미지 패널)에서는 **그리드를 건드리지 않는다**: 이미지 1장에
    #   문장 샘플이 최대 22,578개 달려 있어 show_samples 로 넘기면 요청이 MB 단위로 부풀고
    #   (서버 2.5s/MB) 그리드도 의미를 잃는다. 그리는 데이터셋 == 세션일 때만 좁힌다.
    #   ⚠️ 판정은 `self.target_dataset(session)` 이다 — `frames_dataset_name(session)` 으로
    #      하드코딩하면 **문장 패널(SentenceEmbeddingsPanel)에서 틀린다**: 그 패널은 자기
    #      데이터셋(`<X>-prompts`)의 문장을 그리므로 크로스가 아닌데도 세션 이름이 -prompts
    #      라는 이유만으로 그리드 반영이 통째로 막혔다 (2026-08-31 발견). `view_ids` 는
    #      처음부터 target_dataset 로 판정하고 있어 두 판정이 서로 어긋나 있었다.
    def _apply_selection(self, ctx, ids):
        ids = [str(i) for i in ids]
        st = self.mem(ctx)
        st.sel_total = len(ids)                       # 절단 전 진짜 개수 (아래 render 표기)
        # ⚠️ 개별 값 표시는 **여기 한 곳에서만** 정한다 (2026-08-31 실측 버그):
        #    처음엔 on_plot_click 이 세팅하고 on_plot_selected 가 지우게 나눠 놨는데,
        #    scattergl 은 클릭에도 `plotly_selected` 를 함께 쏘고 App 은 패널 오퍼레이터가
        #    하나 돌면 훅을 재발화한다 → 클릭이 방금 세팅한 값을 선택 핸들러가 덮어
        #    **클릭 표시가 영영 안 뜬다**(라이브에서만 재현, 오프라인 단위 호출은 통과).
        #    두 경로가 모두 이 함수를 지나므로 개수로 판정하면 순서 경합이 사라진다.
        st.point_info = self._point_info(ctx, ids)
        st.selected_ids = ids[:SHOW_SAMPLES_CAP]
        session = ctx.dataset.name if ctx.dataset is not None else None
        if session and self.target_dataset(session) == session and ids:
            # ⚠️ `use_extended_selection=True` 필수. 기본값(False)은 뷰 스테이지를 갈아끼울
            #    뿐이라 형제 패널 user-prompt-compare 의 `on_change_extended_selection` 훅이
            #    영영 안 울린다 — compare 워크스페이스에서 좌하 산점도로 프레임을 골라도
            #    우측 문장 표가 반응하지 않는 증상. 2026-08-19 이 패널이 네이티브
            #    Embeddings 를 대체하면서 그 훅의 유일한 발신자가 됐다.
            # ⚠️ **두 번 보낸다 — 둘이 하는 일이 다르다.**
            #   · extended=True  → App 번들 ShowSamples 는 `extendedSelection` 아톰만
            #     세팅하고 즉시 return(뷰 미변경). 형제 패널 user-prompt-compare 의
            #     `on_change_extended_selection` 훅은 **이것만** 듣는다.
            #   · extended=False → 고정 `_uuid` 의 Select 스테이지를 뷰에 건다.
            #     **그리드가 실제로 좁아지는 건 이쪽뿐이다.** extended 만 보내던 시절엔
            #     세팅 0.1초 뒤 App 이 되돌려 순효과가 0 이었다 (2026-08-31 실측:
            #     GraphQL extendedStages 가 {Select:500개} → null → null 로 즉시 원복,
            #     그리드는 7,498 고정. 사용자 리포트 "samples 에서는 변동이 없어").
            #     스테이지는 고정 _uuid 라 누적되지 않고(직전 것을 지우고 붙는다),
            #     `view_without_our_selection` 이 산점도 쪽에서 이 스테이지를 무시한다.
            ctx.ops.show_samples(ids[:SHOW_SAMPLES_CAP], use_extended_selection=True)
            ctx.ops.show_samples(ids[:SHOW_SAMPLES_CAP])
            st.cross_grid_note = None
        elif session and ids:
            # 크로스(`-prompts` 세션의 이미지 패널): 고른 **프레임**에 달린 문장 샘플로 옮겨
            # 그리드를 좁힌다 (frame_ids_to_sentence_ids docstring 의 팬아웃 실측 참고).
            sids, total = frame_ids_to_sentence_ids(
                self.target_dataset(session), ids, session, SHOW_SAMPLES_CAP)
            if sids:
                ctx.ops.show_samples(sids)      # 뷰 경로 — 그리드가 실제로 좁아진다
                st.cross_grid_note = (
                    f"프레임 {len(ids):,}개 → 문장 {total:,}개"
                    + (f" (그리드는 {len(sids):,}개만 반영)" if total > len(sids) else ""))
            else:
                st.cross_grid_note = (
                    f"고른 프레임 {len(ids):,}개에 달린 문장 샘플이 없다 — 그리드 변화 없음 "
                    f"(이 데이터셋은 프레임 7,498개 중 4,903개가 문장 0개다)")
        self._refresh(ctx)

    def _point_info(self, ctx, ids):
        """선택이 **점 1개**일 때만 그 값을 되찾는다 (다중 선택엔 단일 값이 무의미).

        호버 상한을 넘긴 큰 플롯에서 개별 문장/파일명을 볼 수 있는 유일한 경로다.
        """
        if len(ids) != 1:
            return None
        session = ctx.dataset.name if ctx.dataset is not None else None
        if not session:
            return None
        try:
            b = load_image_bundle(self.target_dataset(session))
            return point_label_for_id(b, ids[0]) or f"(값 없음: id={ids[0]})"
        except Exception as e:      # noqa: BLE001 — 조회 실패가 선택 자체를 막으면 안 된다
            # ⚠️ 조용히 삼키지 않는다: `except: pass` 였을 때 "클릭 표시가 안 뜬다"는
            #    증상만 남고 원인이 사라져 진단이 한 바퀴 늦었다 (2026-08-31).
            return f"(조회 실패: {type(e).__name__}: {e})"

    def on_plot_click(self, ctx):
        rid = (ctx.params or {}).get("id")
        if rid is None:
            return
        self._apply_selection(ctx, [str(rid)])

    def on_plot_selected(self, ctx):
        # PlotlyView onSelected: params["data"] = [{"id", "trace", "idx", ...}]
        rows = (ctx.params or {}).get("data") or []
        ids = [str(r.get("id")) for r in rows if r.get("id") is not None]
        if not ids:
            return   # 빈 에코 무시 — scattergl box select 는 mouseup 에 빈 이벤트를 더 쏜다
        self._apply_selection(ctx, ids)

    def on_clear_selection(self, ctx):
        st = self.mem(ctx)
        st.selected_ids = []
        st.sel_total = 0
        st.point_info = None
        st.cross_grid_note = None
        session = ctx.dataset.name if ctx.dataset is not None else None
        if session:
            # 우리가 뷰를 걸었으므로 해제도 해야 한다 — 안 하면 그리드가 좁혀진 채 남아
            # "해제가 안 되는" 것처럼 보인다.
            # ⚠️ `ctx.ops.clear_view()` 금지 (2026-09-09): App 의 ClearView 는 `reset(view)`
            #    라 **사용자가 뷰 바에 직접 건 스테이지까지 통째로** 날린다(matchtags 등).
            #    형제 패널 `user-prompt-compare._clear_frames_view` 와 같은 방식으로,
            #    내장 show_samples 가 박은 고정 `_uuid` 스테이지만 빼고 되돌린다.
            _clear_our_stage(ctx)
        self._refresh(ctx)

    def render(self, ctx):
        panel = types.Object()
        # ⚠️ **스냅샷으로 읽는다** (코덱스 리뷰 F6): 아래에서 값을 여러 번 따로 읽는 사이
        #    동시 요청의 `_refresh` 가 기억을 하나씩 갈아끼우면 섞인 화면이 나온다.
        st = self.mem(ctx).snapshot()
        # 자가 복구 (2026-08-14 실측): 패널 리마운트·프로세스 재시작으로 상태가 통째로 비는
        # 일이 있다. 그대로 그리면 배너·드롭다운·산점도가 동시에 사라진 빈 패널이 된다.
        # ⚠️ 판정 출처가 **서버 기억**으로 바뀌었다 (2026-09-09): 예전엔 클라이언트가 실어 온
        #    빈 panel_state 에코에도 발동해, 옛 탭의 에코 하나가 on_load → 상태 11회 쓰기 →
        #    사용자의 사이드바 필터 소멸로 이어졌다(`_mem` 주석의 체인). 서버 기억은 에코로
        #    비지 않으므로 이제 진짜 콜드 스타트에서만 발동한다.
        if not st.fields and st.available is not False \
                and getattr(ctx, "dataset", None) is not None:
            self.on_load(ctx)
            st = self.mem(ctx).snapshot()      # on_load 가 채운 값을 이번 렌더에 반영
        # ⚠️ align_y="bottom" (실측, 2026-08-19): 헤드리스 캡처로 "✕ 선택 해제" 버튼이
        #    드롭다운과 어긋나 뜨는 원인을 픽셀 단위로 확인했다 — enum("색칠")은 라벨(21px)+
        #    간격(8px)+입력 박스(32.7px) = 62px 칸을 쓰는데, btn 은 라벨 줄이 없어 32.7px뿐이다.
        #    align_y="center" 는 그 62px **칸 전체**를 기준으로 버튼을 가운데 두므로 버튼이
        #    드롭다운 박스보다 15px 위(칸의 라벨 쪽)에 뜬다 — "정렬이 깨져 플롯 쪽으로 떠
        #    보인다"는 사용자 지적과 정확히 일치. 드롭다운은 칸 높이=자기 높이라 "bottom" 으로
        #    바꿔도 안 움직이고, 버튼만 칸 바닥(=드롭다운 박스와 같은 y)으로 내려온다 —
        #    user-prompt-compare 의 같은 버튼도 같은 원인으로 어긋나 있으나(실측 확인) 그 파일은
        #    이 세션의 수정 대상이 아니다.
        row = panel.h_stack("controls", gap=2, align_y="bottom", columns=3)
        fields = st.fields or []
        meta = {f: (lab, desc) for f, lab, desc in (st.axes or COLOR_CANDIDATES)}
        if fields:
            # ⚠️ **드롭다운(enum)을 쓰지 않는다** (2026-09-09): 사용자가 값을 고르면 App 의
            #    스키마 렌더러가 재구성한 패널 데이터를 통째로 `panelsStateAtom` 에 머지하고
            #    (동등성 가드 없음), 그 쓰기가 spaces 영속 → DatasetPageQuery 재조회 →
            #    새 Dataset.id → **사용자의 사이드바 필터가 지워진다**. 서버 쓰기가 0 이어도
            #    일어난다(계측 확인). 번들 실측상 값을 들고도 상태를 안 쓰는 컴포넌트는
            #    ButtonView/TableView 계열뿐이라 **버튼 피커**로 바꿨다.
            #    (RadioGroup·Dropdown·Choices·Autocomplete 는 전부 쓴다 — 대체제가 아니다.)
            cur_lab = meta.get(st.color_by or "", (st.color_by or "-", ""))[0]
            _opts = [(f, meta.get(f, (f, ""))[0]) for f in fields]
            if _fits_inline(_opts):     # 축이 적은 데이터셋이면 클릭 한 번으로 (규칙은 한 곳)
                _btn_choice(row, "color", "색칠", _opts, st.color_by, self.on_color_change)
            else:
                _control_toggle(row, "color", "색칠", cur_lab, self.on_toggle_color_picker)
        if st.selected_ids or _has_our_stage(ctx):
            # 절단을 숨기지 않는다: 라쏘 3,000점을 "(500)" 으로만 보여주면 사용자가
            # 500장이 선택됐다고 믿는다 (형제 패널 user-prompt-compare 와 같은 처치).
            n_shown = len(st.selected_ids or [])
            n_total = st.sel_total or n_shown
            label = ("✕ 선택 해제" if not n_shown          # 기억은 비었는데 칩만 남은 경우
                     else f"✕ 선택 해제 ({n_shown:,})" if n_total <= n_shown
                     else f"✕ 선택 해제 ({n_total:,} 중 {n_shown:,} 반영)")
            row.btn("clear_selection", label=label, on_click=self.on_clear_selection)
        if st.picker == "color" and fields:
            # 열려 있을 때만, **컨트롤 행 바로 아래** 전폭으로 (형제 패널과 같은 규칙).
            _picker_grid(panel, "color",
                         [(f, meta.get(f, (f, ""))[0]) for f in fields],
                         st.color_by, self.on_color_change)
        # ⚠️ figure 를 **배너보다 먼저** 만든다 (2026-09-04): 뷰가 바뀐 렌더는 여기서
        #    재구성되는데, 그 결과의 배너("표시 112/373장")를 같은 렌더에 실어야 상태를
        #    한 글자도 쓰지 않고 최신 숫자를 보여줄 수 있다. 상태를 쓰면 setSpaces →
        #    DatasetPageQuery 가 돌아 사용자의 사이드바 필터가 지워진다 (on_change_view 참고).
        # ⚠️ 진위(truthiness) 아니라 **키 존재**로 고른다: 빈 문자열 배너("모드 B 그룹 미입력"
        #    등)가 정당한 값인데 `or` 로 폴백하면 **직전 뷰의 배너**가 되살아난다.
        # ⚠️ **조회는 한 번**이다: data/banner/layout 을 따로 꺼내면 그 사이 다른 요청이
        #    엔트리를 갈아끼워 "새 그림 + 옛 배너" 조합이 나온다 (codex 리뷰, 2026-09-07).
        _entry = _get_entry(ctx, self.NAME)
        banner_txt = _entry["banner"] if _entry is not None else st.banner
        layout = _entry["layout"] if _entry is not None else st.layout
        fig_data = _entry["data"] if _entry is not None else None
        if fig_data is None and st.available is not False:
            # 프로세스 재시작/캐시 축출 후 결정론 재구성 (번들 warm 이면 ~0.1s)
            try:
                session = ctx.dataset.name if ctx.dataset is not None else None
                frames_name = self.target_dataset(session)
                b = load_image_bundle(frames_name)
                fig = self._build_fig(
                    ctx, b,
                    st.color_by or default_color_by(frames_name, b["_fields"]),
                    session, frames_name, frames_name != session)
                fig_data = fig["data"]
                banner_txt = fig["banner"]
                layout = fig["layout"]
                _put_fig(ctx, self.NAME, fig_data, banner=banner_txt, layout=layout)
            except Exception:
                fig_data = None
        # 배너는 **그릴 게 없을 때만** 뜬다 (2026-09-09 사용자 요청): 축 설명·좌표계 주의·
        # 모집단 표기는 매 화면에 붙어 읽히지 않는다. 데이터셋/브레인런이 없어 산점도가
        # 비는 경우에만 사유를 남긴다 — 그때는 배너가 유일한 설명이다.
        if banner_txt and (st.available is False or not fig_data):
            panel.md(banner_txt, name="banner_md")
        if st.cross_grid_note:
            panel.md(st.cross_grid_note, name="cross_grid_md")
        if st.point_info:
            panel.md(f"선택한 점: `{st.point_info}`", name="point_md")
        panel.plot("img_scatter", data=fig_data or [],
                   layout=layout or {},
                   height=self.plot_height(ctx),
                   config={"responsive": True, "displayModeBar": True},
                   on_click=self.on_plot_click,
                   on_selected=self.on_plot_selected)
        return types.Property(panel, view=types.GridView())


BANNER_SENTENCE = ("문장 임베딩 — 점 1개 = 프롬프트 문장 1개 "
                   "(이미지 산점도와 좌표계가 다르다: 독립 fit, 위치 비교 금지)")
NO_SENTENCES_TEXT = "문장 임베딩을 찾을 수 없습니다"


class SentenceEmbeddingsPanel(ImageEmbeddingsPanel):
    """`<X>-prompts` 세션의 **자기 데이터셋** 문장 좌표를 그린다.

    존재 이유 (2026-08-14 사용자 요청: "sourcei-prompt 에서 기본이 emb_viz 로 안 되어
    있어 일일이 선택해야 해"): 그 자리의 네이티브 Embeddings 패널은
      · brainResult 를 비워두면 → 매번 손으로 brain key 를 골라야 하고,
      · emb_viz 를 박아두면 → 603,318 점을 그려 110초 + Chrome 크래시("Error code: 5").
    자체 패널은 층화 서브샘플로 20,000 점만 그려 **6.4초에 자동으로** 뜨고(실측), 배너가
    표시/전체 비율까지 밝힌다. 즉 선택 단계 자체가 없어진다.
    """

    NAME = "sentence_embeddings"
    BANNER = BANNER_SENTENCE
    NOT_FOUND = NO_SENTENCES_TEXT
    # 좌측 스택 아래 칸(= 화면 높이의 절반) 기준. 50vh 에서 컨트롤+배너 2줄+여유 140px 를 뺀다
    # — 1080 뷰포트에서 400px, 900 뷰포트에서 310px. 칸을 넘지 않으면서 산점도가 판독 가능한 값.
    PLOT_HEIGHT = "max(240px, calc(50vh - 140px))"

    def target_dataset(self, session):
        return session          # 크로스 조인 없음 — 자기 데이터셋의 문장 좌표

    @property
    def config(self):
        return foo.PanelConfig(name=self.NAME,
                               label="Sentence Embeddings", surfaces="grid")


def register(p):
    p.register(ImageEmbeddingsPanel)
    p.register(SentenceEmbeddingsPanel)


# ── selftest (컨테이너에서 `python __init__.py`) ──
def selftest():
    global MAX_POINTS        # ④ 서브샘플 경로를 상한 축소로 강제하기 위해 (아래 finally 복원)
    global SHOW_SAMPLES_CAP  # 절단 표기 검증도 같은 방식(상한 축소 → finally 복원)
    global HOVER_TEXT_MAX_POINTS   # 호버 두 모드를 한 데이터로 다 검증하려면 상한을 흔든다
    import numpy as np

    pdb_selftest()           # prompt DB 해석 계층 (DB 없이 도는 순수부)

    # 이름 유도 (요구사항의 핵심 계약)
    assert frames_dataset_name("sourcei-prompts") == "sourcei"
    assert frames_dataset_name("source-h-prompts") == "source-h"
    assert frames_dataset_name("sourcei") == "sourcei"
    assert frames_dataset_name("frames") == "frames"

    # 층화 서브샘플 계약: 클래스당 최소 1점 + 예산 준수 + 중복 없음
    fixture = np.asarray(["a"] * 100 + ["b"] * 10 + ["c"])
    picked = stratified_subsample(fixture, 20)
    assert len(picked) == len(set(picked)) == 20
    assert set(fixture[picked].tolist()) == {"a", "b", "c"}

    ds_name = "sourcei"
    if not fo.dataset_exists(ds_name):
        print("selftest OK (데이터셋 없음 — 오프라인 부분만 검증)")
        return

    b = load_image_bundle(ds_name)
    n = len(b["xy"])
    ds = fo.load_dataset(ds_name)
    assert n == ds.count(), f"좌표 수 {n} != 샘플 수 {ds.count()}"
    assert "ground_truth" in b["_fields"]

    # ⚠️ 매핑 정합 — 이 패널의 유일한 '조용한 오답' 경로다. brain sample_ids 순서로 실은
    #    filepath/클래스가 실제 그 샘플의 값과 같아야 한다. 무작위 표본으로 전수 대신 검증.
    rng = np.random.default_rng(0)
    for k in rng.choice(n, size=min(25, n), replace=False):
        k = int(k)
        s = ds[str(b["id"][k])]
        assert s.filepath == b["filepath"][k], f"filepath 불일치 @{k}"
        gt = s.ground_truth.label if s.ground_truth is not None else None
        assert b["ground_truth"][k] == ("(없음)" if gt is None else gt), f"클래스 불일치 @{k}"

    # ── 축 구분 계약 (2026-08-14 사용자 요청: "조금이라도 다르면 이용자가 차이를 알아야") ──
    # ground_truth 와 category 는 값 집합이 같아 이름만으로는 구분 불가 → 라벨에 단위 명시 +
    # 배너에 기준 대비 불일치 장수. 실측 일치율 69.4% (다름 2,293/7,498).
    labs = {f: l for f, l, _d in COLOR_CANDIDATES}
    descs = {f: d for f, _l, d in COLOR_CANDIDATES}
    assert "영상 단위" in labs["ground_truth"], labs
    # ⚠️ category 는 사람 정답이 아니라 v1.0.8.0 모델 예측이다 (2026-08-14 실측 3중 확인).
    #    라벨이 "정답" 으로 되돌아가면 자기참조 평가(구 모델 예측으로 신 모델 채점) 재발.
    assert "정답" not in labs["category"], labs["category"]
    assert "예측" in labs["category"] and "모델" in labs["category"], labs["category"]
    assert "사람 정답 아님" in descs["category"], descs["category"]
    for f, l, d in COLOR_CANDIDATES:        # 전 축이 단위/출처를 라벨에 명시해야 한다
        assert "(" in l and ")" in l or f == "camera", (f, l)
        assert d, f"{f}: 설명 없음 — 배너가 축의 의미를 못 싣는다"
    # ── z-order: 개수 내림차순으로 깔려야 희소 클래스가 다수 클래스에 안 가린다 ──
    if "event_kind" in b["_fields"]:
        fig_ek = build_figure(b, "event_kind")
        counts = [int(t["name"].rsplit(" ", 1)[-1]) for t in fig_ek["data"][:-1]]
        assert counts == sorted(counts, reverse=True), \
            f"회귀: z-order 가 개수 내림차순이 아니다 — 희소 클래스가 덮인다: {counts}"
        # 8색을 넘는 그룹은 모양으로 구분돼야 한다 (색 재사용만으로는 구별 불가)
        seen = {}
        for t in fig_ek["data"][:-1]:
            k = (t["marker"]["color"], t["marker"].get("symbol", "circle"))
            assert k not in seen, f"회귀: 색+모양이 겹치는 그룹 — {t['name']} vs {seen[k]}"
            seen[k] = t["name"]
    # ── 값이 100% 동일한 축은 배너가 스스로 밝혀야 한다 (자기참조 평가 방지) ──
    if "category" in b["_fields"]:
        note = axis_note(b, "category")
        # category 의 차이는 **전부 같은 4클래스 안에서 갈리는 진짜 상충**이라 '상충' 으로 표기
        n_diff = int((b["category"] != b["ground_truth"]).sum())
        assert "상충" in note and f"{n_diff:,}장" in note, (n_diff, note)
        assert n_diff > 0, "회귀: 두 축이 완전히 같다면 색칠 축을 둘 다 둘 이유가 없다"
        assert "세분값" not in note, f"category 는 기준 밖 값이 없어야 한다: {note}"
        assert axis_note(b, "ground_truth").startswith("그 프레임이 속한")   # 기준 축 설명
        assert "기준 축" in axis_note(b, "ground_truth")
    if "event_kind" in b["_fields"]:
        # event_kind 는 기준(4클래스) 밖 값(near_miss·other…)을 가지므로 '세분값' 으로 구분돼야
        # 한다 — 이걸 '상충' 으로 뭉치면 정상 세분화를 오류로 읽는다 (2026-08-14).
        note_ek = axis_note(b, "event_kind")
        assert "세분값" in note_ek, note_ek
    # 값 집합이 다른 축(주야·카메라 등)은 불일치 수치를 아예 싣지 않는다 (무의미)
    if "daynight" in b["_fields"]:
        nd = axis_note(b, "daynight")
        assert "상충" not in nd and "세분값" not in nd, nd

    fig = build_figure(b, "ground_truth")
    assert all(t["type"] == "scattergl" for t in fig["data"])          # scattergl 강제
    # 배너에 축 라벨(단위 포함)이 실린다 — 필드명만 뜨면 사용자가 축을 구분 못 한다
    assert labs["ground_truth"] in fig["banner"], fig["banner"]
    # 배너는 단일 문단이어야 한다 — 문단을 나누면 축 전환 시 뒷 문단이 stale 로 남는다
    assert "\n\n" not in fig["banner"], "회귀: 배너가 여러 문단 — 축 바꿔도 뒷줄이 안 바뀐다"
    assert fig["banner"].startswith("**색칠:"), fig["banner"]
    if "category" in b["_fields"]:
        fig_cat = build_figure(b, "category")
        assert labs["category"] in fig_cat["banner"]
        assert "상충" in fig_cat["banner"], fig_cat["banner"]
    assert "height" not in fig["layout"] and fig["layout"]["autosize"] is True
    assert sum(len(t["x"]) for t in fig["data"][:-1]) == min(n, MAX_POINTS)
    names = [t["name"] for t in fig["data"][:-1]]
    assert any(x.startswith("fire") for x in names), names            # 클래스별 trace 분리
    assert fig["data"][0]["ids"], "ids 미탑재 — 클릭/lasso 조인이 죽는다"
    # 하이라이트 trace 는 선택 id 만
    some = [str(b["id"][0]), str(b["id"][5])]
    fig_sel = build_figure(b, "ground_truth", selected_ids=some)
    assert len(fig_sel["data"][-1]["x"]) == 2 and fig_sel["data"][-1]["name"] == "선택"
    # 색칠 축 전환이 그룹 구성을 실제로 바꾼다
    if "environment" in b["_fields"]:
        fig_env = build_figure(b, "environment")
        assert [t["name"] for t in fig_env["data"][:-1]] != names

    # 캐시: 같은 데이터셋 재로드는 동일 객체 (요청마다 재계산 금지)
    assert load_image_bundle(ds_name) is b

    # 변경 dedup 가드
    class _Pid:
        params = {"panel_id": "selftest-pid"}
    # 첫 관측은 carried_same 이 True 여도 처리해야 한다 — 클라이언트가 값을 낙관적으로 먼저
    # 바꿔 보내므로, 재기동 직후 첫 클릭에서 carried_same 을 믿으면 변경이 삼켜진다 (실측).
    assert _change_guard(_Pid(), "color_by", "ground_truth", True) is False
    assert _change_guard(_Pid(), "color_by", "ground_truth", False) is True   # 이중 발화 흡수
    assert _change_guard(_Pid(), "color_by", "environment", True) is False    # 왕복중 재클릭 통과

    # 패널 마운트 시퀀스 (on_load → render) — 스키마에 data 가 구워져야 첫 화면이 안 빈다
    class _State:
        def __init__(self):
            self._d = {}
        def __getattr__(self, k):
            if k.startswith("_"):
                raise AttributeError(k)
            return self._d.get(k)
        def __setattr__(self, k, v):
            if k.startswith("_"):
                super().__setattr__(k, v)
            else:
                self._d[k] = v
        def set(self, k, v):
            self._d[k] = v
        def get(self, k, default=None):
            return self._d.get(k, default)

    class _Data:
        def clear(self):
            pass

    class _Panel:
        def __init__(self):
            self.state = _State()
            self.data = _Data()

    class _Ops:
        def __init__(self):
            self.calls = []
        def show_samples(self, ids, **kw):
            self.calls.append(("show_samples", list(ids),
                               bool(kw.get("use_extended_selection"))))
        def clear_view(self):
            self.calls.append("clear_view")

    class _Ctx:
        def __init__(self, name):
            self.panel = _Panel()
            self.dataset = fo.load_dataset(name)
            self.params = {}
            self.ops = _Ops()
            self.triggers = []

        def trigger(self, name, params=None):
            self.triggers.append((name, params))

        @property
        def panel_state(self):
            # 클라이언트는 서버가 마지막에 밀어준 것을 그대로 되돌려 준다.
            return self.panel.state._d

    panel = ImageEmbeddingsPanel()
    _MEM.clear()          # 서버 기억은 프로세스 전역 — 테스트는 콜드에서 시작한다
    c = _Ctx(ds_name)
    panel.on_load(c)
    schema = panel.render(c)
    view = schema.type.properties["img_scatter"].view
    assert view.data, "회귀: 최초 마운트 스키마에 data 없음 — 빈 산점도"
    assert panel.mem(c).color_by == "ground_truth"
    # 색칠은 드롭다운이 아니라 **버튼 피커**다 (enum 은 App 이 패널 상태를 써 필터를 지운다).
    # 이름 규칙(두 패널 공통): 컨트롤 = `<name>`, 토글 = `<name>_toggle`,
    # 펼친 격자 = `<name>_picker`, 옵션 = `pick_<name>__<value>`.
    _ctl = schema.type.properties["controls"].type.properties
    assert "color" in _ctl, "회귀: 색칠 컨트롤이 없다"
    assert "color_toggle" in _ctl["color"].type.properties, "회귀: 토글 이름 규칙 이탈"
    assert "color_by" not in schema.type.properties["controls"].type.properties, \
        "회귀: 색칠이 enum 으로 돌아갔다 — 한 번 고를 때마다 사이드바 필터가 지워진다"
    assert panel.mem(c).banner and "이미지 임베딩" in panel.mem(c).banner
    # 큰 데이터는 state 에 실리지 않는다 (요청 2.5s/MB 재발 방지)
    assert c.panel.state.get("scatter_data") is None
    assert c.panel.state.get("img_scatter") is None
    # 패널 이름 계약: 워크스페이스(fiftyone_app_setup._compare_space)가 이 이름으로 패널을
    # 참조한다 — 어긋나면 App 이 `Panel "<name>" no longer exists!` 만 띄운다 (2026-08-14).
    assert panel.config.name == "image_embeddings", panel.config.name

    # ── 사이드바 필터 보존 계약 (2026-09-04, 2026-09-09 전 훅으로 확장) ──────────
    # 패널 상태에 **값이 바뀌는** 쓰기를 하면 App 이 spaces 를 영속하며
    # DatasetPageQuery 를 network-only 로 다시 돌리고, 그때 서버가 새 Dataset.id 를
    # 발급해 클라이언트 전용 `filters` 아톰이 스스로 비워진다 (`_mem` 주석의 전 구간 체인).
    # 실측(2026-09-09 헤드리스): 사이드바 fire 로 155장 → 색칠 드롭다운 변경 → 2초 뒤 6,032장.
    # ⇒ 훅은 패널 상태를 쓰면 안 된다. 유일한 예외가 컨트롤 미러이고, 그것도
    #   **클라이언트가 낡았을 때만** 쓴다. 아래가 그 계약을 못 박는다.
    def _writes(fn, ctx_, *a, **kw):
        """fn 실행이 패널 상태에 남긴 변화 {키: 새 값}."""
        before = dict(ctx_.panel.state._d)
        fn(ctx_, *a, **kw)
        after = dict(ctx_.panel.state._d)
        return {k: v for k, v in after.items() if before.get(k) != v}

    assert _writes(panel.on_load, c) == {}, \
        "회귀: on_load 가 패널 상태를 썼다 — 콜드 스타트 1회 wipe 재발"
    assert _writes(panel.on_change_view, c) == {}, \
        "회귀: on_change_view 가 패널 상태를 썼다 — 사용자의 사이드바 필터가 지워진다"
    assert c.ops.calls == [], f"회귀: on_change_view 가 ops 를 호출했다: {c.ops.calls}"
    assert _writes(panel.on_change_selected, c) == {}, \
        "회귀: 그리드 선택 훅이 패널 상태를 썼다 — 사용자의 사이드바 필터가 지워진다"
    assert _writes(panel.render, c) == {}, \
        "회귀: render 가 패널 상태를 썼다 — 매 렌더마다 사이드바 필터가 지워진다"
    assert _writes(panel._refresh, c) == {}, \
        "회귀: _refresh 가 패널 상태를 썼다 (컨트롤 미러가 이미 최신인데도 밀었다)"
    # 색칠 = **버튼 피커**. 드롭다운(enum)이었을 때는 사용자가 값을 고르는 순간 App 이 스스로
    # 패널 상태를 써서(우리 쓰기 0 이어도) 사이드바 필터가 날아갔다 — 2026-09-09 실측.
    # 여기서 못 박는 것: 토글도 선택도 패널 상태를 한 글자도 쓰지 않는다.
    if "category" in load_image_bundle(ds_name)["_fields"]:
        _cc = _Ctx(ds_name)
        panel.on_load(_cc)
        assert _writes(panel.on_toggle_color_picker, _cc) == {}, \
            "회귀: 색칠 피커 토글이 패널 상태를 썼다"
        assert panel.mem(_cc).picker == "color"
        _open = panel.render(_cc).type.properties
        assert "color_picker" in _open, "회귀: 피커를 열었는데 목록이 안 나온다"
        assert any(k.startswith("pick_color__")
                   for k in _open["color_picker"].type.properties), "옵션 버튼이 없다"
        _cc.params = {"value": "category"}
        _APPLIED.clear()
        assert _writes(panel.on_color_change, _cc) == {}, \
            "회귀: 색칠 선택이 패널 상태를 썼다 — 한 번 고를 때마다 사이드바 필터가 날아간다"
        assert panel.mem(_cc).color_by == "category", "색칠 변경이 서버 기억에 반영돼야 한다"
        assert panel.mem(_cc).picker is None, "고른 뒤에는 피커가 닫혀야 한다"
        # 같은 값을 다시 골라도(가드에 걸려도) 피커는 닫힌다 — 안 닫히면 "안 먹는다" 로 보인다
        panel.on_toggle_color_picker(_cc)
        assert _writes(panel.on_color_change, _cc) == {}
        assert panel.mem(_cc).picker is None, "회귀: 같은 값 재선택 시 피커가 열린 채 남는다"
    # 선택 계열도 마찬가지 — 상태는 서버에만 남는다.
    _sc = _Ctx(ds_name)
    panel.on_load(_sc)
    _ids = [str(i) for i in load_image_bundle(ds_name)["id"][:3]]
    assert _writes(panel._apply_selection, _sc, _ids) == {}, \
        "회귀: 산점도 선택이 패널 상태를 썼다 — 사이드바 필터가 지워진다"
    assert panel.mem(_sc).selected_ids == _ids
    assert _writes(panel.on_clear_selection, _sc) == {}, \
        "회귀: 선택 해제가 패널 상태를 썼다"
    assert panel.mem(_sc).selected_ids == []

    # ── 레거시 에코 격리 (코덱스 리뷰 F1) ──────────────────────────────────────
    #    이 변경 **이전** 배포는 fields/selected_ids 같은 최상위 키를 패널 상태에 썼고 그
    #    값은 세션이 살아 있는 한 계속 실려 온다. 서버 기억의 시드로 쓰면 **다른 데이터셋의**
    #    색칠 필드가 유령 드롭다운으로, 남의 선택이 유령 "선택 해제 (1)" 로 되살아난다
    #    (2026-08-14 에 이미 한 번 고쳤던 증상).
    _MEM.clear()
    _lg = _Ctx(ds_name)
    _lg.panel.state._d.update({"fields": ["__ghost_a__", "__ghost_b__"],
                               "selected_ids": ["deadbeefdeadbeefdeadbeef"],
                               "available": True})
    panel.render(_lg)
    assert "__ghost_a__" not in (panel.mem(_lg).fields or []), \
        "회귀: 레거시 에코의 필드 목록이 서버 기억으로 새어 유령 드롭다운이 뜬다"
    assert panel.mem(_lg).selected_ids == [], \
        "회귀: 레거시 에코의 선택이 살아나 유령 '선택 해제' 가 뜬다"

    # ── 뷰 바에 우리 칩만 남은 경우에도 해제 버튼은 떠야 한다 (코덱스 리뷰 F5) ──
    #    서버 기억은 프로세스 재시작으로 비지만 칩은 남는다 — 버튼이 없으면 좁혀진 그리드를
    #    풀 방법이 사라진다.
    _MEM.clear()
    _chip = _Ctx(ds_name)
    _chip.request_params = {"view": [{"_cls": "fiftyone.core.stages.Select",
                                      "_uuid": SHOW_SAMPLES_STAGE_ID, "kwargs": []}]}
    assert _has_our_stage(_chip) and not panel.mem(_chip).selected_ids
    _sch_chip = panel.render(_chip)
    assert "clear_selection" in _sch_chip.type.properties["controls"].type.properties, \
        "회귀: 뷰 바에 우리 칩이 남았는데 해제 버튼이 없다 — 그리드를 풀 수 없다"

    # 뷰가 바뀌면 fig 캐시 키도 바뀌어야 render 의 재구성 폴백이 **새 뷰로** 다시 그린다.
    # 키가 뷰를 모르면 옛 뷰 figure 를 캐시 히트로 계속 내준다 = 필터를 걸어도 산점도가
    # 안 좁혀진다 (상태를 안 쓰게 된 뒤로는 이 키가 유일한 갱신 신호다).
    c_filt = _Ctx(ds_name)
    c_filt.panel.state._d.update(c.panel.state._d)
    c_filt.request_params = {"filters": {"ground_truth.label": {"values": ["fire"]}}}
    assert _fig_key(c_filt, panel.NAME) != _fig_key(c, panel.NAME), "회귀: fig 캐시 키에 뷰 지문이 없다"

    # 캐시 **히트** 시 배너는 그 뷰의 _FIGMETA 에서 와야 한다 — panel.state.banner 로
    # 폴백하면 필터 전 모집단("표시 373/373장")이 되살아나는 조용한 오답이 된다.
    _put_fig(c_filt, panel.NAME, ["D"], banner="BANNER_FILTERED", layout={"x": 1})
    _e = _get_entry(c_filt, panel.NAME)
    assert _e is not None and _e["banner"] == "BANNER_FILTERED" and _e["data"] == ["D"], _e
    assert (_get_entry(c, panel.NAME) or {}).get("banner") != "BANNER_FILTERED", \
        "회귀: 뷰가 달라도 배너 캐시가 공유된다 — 필터 전 숫자가 남는다"
    # data/banner/layout 은 **한 엔트리**여야 한다 — 따로 꺼내면 동시 요청에서
    # "새 그림 + 옛 배너" 조합이 나온다 (codex 리뷰 2026-09-07).
    assert set(_e) == {"data", "banner", "layout"}, f"회귀: 캐시 엔트리가 쪼개졌다: {set(_e)}"
    # 빈 배너는 정당한 값이다(모드 B 등) — truthiness 로 고르면 직전 뷰 배너가 되살아난다.
    _put_fig(c_filt, panel.NAME, [], banner="", layout={"x": 1})
    assert _get_entry(c_filt, panel.NAME)["banner"] == "", "회귀: 빈 배너가 저장되지 않는다"

    # LRU: 히트한 엔트리는 축출되지 않아야 한다. FIFO 였을 때는 지금 보고 있는 엔트리가
    # 계속 '가장 오래된 것' 으로 남아, 다른 클라이언트가 상한만큼 만들면 쫓겨났다
    # (모드 B/C 는 render 재구성 폴백이 막혀 있어 그 순간 산점도가 빈 채로 굳는다).
    _put_fig(c_filt, panel.NAME, ["KEEP"], banner="keep", layout={})
    for _i in range(20):
        _ctmp = _Ctx(ds_name)
        _ctmp.panel.state._d.update(c.panel.state._d)
        _ctmp.request_params = {"filters": {"f": {"values": [f"v{_i}"]}}}
        _put_fig(_ctmp, panel.NAME, [_i], banner=f"b{_i}", layout={})
        assert _get_entry(c_filt, panel.NAME) is not None, \
            f"회귀: 활성 엔트리가 {_i + 1}개 삽입 만에 축출됐다 — LRU 가 아니다"
    assert len(_FIGS) <= 16, f"회귀: 캐시 상한이 안 걸린다: {len(_FIGS)}"

    # ── 2026-08-14 실사용 회귀 3종 (패널이 빈 채로 그려지던 원인) ──
    # ① fig 캐시 키는 panel_id 에 의존하면 안 된다: 훅 요청(panel_id 있음)이 넣은 fig 를
    #    render(panel_id 없음)가 반드시 찾아야 한다. 못 찾으면 패널이 통째로 빈다.
    c2 = _Ctx(ds_name)
    c2.params = {"panel_id": "hook-req-1"}      # 훅 요청처럼 panel_id 를 싣고 갱신
    panel.on_load(c2)
    c2.params = {}                              # render 는 panel_id 없이 온다
    assert _get_fig(c2, panel.NAME), "회귀: panel_id 유무로 fig 캐시 키가 갈려 render 가 fig 를 잃는다"
    sch2 = panel.render(c2)
    assert sch2.type.properties["img_scatter"].view.data, "회귀: render 가 빈 산점도를 냈다"

    # ② 콜드 스타트(서버 기억 없음) → render 가 on_load 로 자가 복구해야 한다
    _MEM.clear()
    c3 = _Ctx(ds_name)                          # on_load 없이 곧바로 render
    sch3 = panel.render(c3)
    assert panel.mem(c3).fields, "회귀: 콜드 스타트에서 자가 복구 실패 — 빈 패널로 굳는다"
    assert sch3.type.properties["img_scatter"].view.data
    assert "color" in sch3.type.properties["controls"].type.properties
    # ②-b **빈 panel_state 에코는 자가복구를 발동시키면 안 된다** (2026-09-09): 옛 탭의
    #      빈 에코 하나가 on_load 를 돌려 상태를 11번 쓰면 그때마다 사용자의 사이드바
    #      필터가 사라진다. 서버 기억이 살아 있으면 에코는 무시돼야 한다.
    c3b = _Ctx(ds_name)                         # 클라이언트 상태는 비었지만 기억은 warm
    assert _writes(panel.render, c3b) == {}, \
        "회귀: 빈 상태 에코가 자가복구를 발동시켰다 — 사이드바 필터가 지워진다"
    assert c3b.ops.calls == []

    # ②-b ⚠️ 자가 복구(on_load 재실행)가 **사용자 선택을 덮으면 안 된다** — 이걸 놓치면
    #      드롭다운을 바꿔도 매번 기본 축으로 되돌아간다 (2026-08-14 실사용 버그).
    if "category" in b["_fields"]:
        c5 = _Ctx(ds_name)
        panel.on_load(c5)
        c5.params = {"value": "category"}
        panel.on_color_change(c5)
        assert panel.mem(c5).color_by == "category"
        panel.on_load(c5)                       # 리마운트/빈 에코로 재호출
        assert panel.mem(c5).color_by == "category", \
            "회귀: on_load 가 사용자가 고른 색칠 축을 기본값으로 되돌린다"
        panel.render(c5)                        # 자가복구 경로도 같이
        assert panel.mem(c5).color_by == "category"
        assert labs["category"] in panel.mem(c5).banner, panel.mem(c5).banner

    # ③ 이미지가 없는 세션은 컨트롤도 함께 비워야 한다 (유령 드롭다운 방지)
    c4 = _Ctx(ds_name)
    panel.on_load(c4)
    assert panel.mem(c4).fields                # 정상 데이터셋에서 채워둔 뒤
    class _Missing:
        name = "__no_such_dataset__-prompts"
    c4.dataset = _Missing()
    panel._refresh(c4)
    assert panel.mem(c4).available is False
    assert panel.mem(c4).fields == [], "회귀: 없는 데이터셋인데 이전 색칠 필드가 남았다"
    assert panel.mem(c4).selected_ids == []
    assert NO_IMAGES_TEXT in panel.mem(c4).banner
    # 그 상태의 render 는 색칠 컨트롤을 만들지 않는다 (자가복구도 발동하면 안 됨)
    sch4 = panel.render(c4)
    assert "color" not in sch4.type.properties["controls"].type.properties

    # ④ 선택은 서브샘플 탈락에도 살아남는다 (MAX_POINTS 초과 경로 — 축소 상한으로 강제)
    _orig_cap = MAX_POINTS
    try:
        MAX_POINTS = 50
        keep_ids = [str(b["id"][k]) for k in (0, 7, 123)]
        fig_cap = build_figure(b, "ground_truth", selected_ids=keep_ids)
        drawn = set()
        for t in fig_cap["data"][:-1]:
            drawn.update(t["ids"])
        assert set(keep_ids) <= drawn, "회귀: 선택한 점이 서브샘플에서 탈락해 사라졌다"
        assert len(fig_cap["data"][-1]["x"]) == 3, "회귀: 하이라이트가 선택 수와 다르다"
    finally:
        MAX_POINTS = _orig_cap

    # 같은 데이터셋: 클릭이 그리드를 좁힌다
    c.ops.calls.clear()
    c.params = {"id": str(b["id"][3])}
    panel.on_plot_click(c)
    assert c.ops.calls and c.ops.calls[-1][0] == "show_samples"
    assert panel.mem(c).selected_ids == [str(b["id"][3])]
    # ⚠️ 회귀 가드 (2026-08-31): **뷰 경로(extended=False)가 반드시 함께 나가야** 그리드가
    #    좁아진다. extended 만 보내면 App 이 0.1초 뒤 되돌려 순효과가 0 이 된다.
    assert [k[2] for k in c.ops.calls] == [True, False], c.ops.calls
    assert all(k[1] == [str(b["id"][3])] for k in c.ops.calls), c.ops.calls

    # ⚠️ 클릭 표시 회귀 가드 (2026-08-31 라이브 버그): 클릭 1점 → 값 표시,
    #    다중 선택 → 표시 없음. 두 판정이 서로 다른 핸들러로 흩어지면 scattergl 의
    #    click+selected 동시 발화 + App 훅 재발화가 방금 세팅한 값을 덮는다.
    c.ops.calls.clear()
    c.params = {"id": str(b["id"][3])}
    panel.on_plot_click(c)
    assert panel.mem(c).point_info and "(값 없음" not in panel.mem(c).point_info, \
        panel.mem(c).point_info
    c.params = {"data": [{"id": str(x)} for x in b["id"][:4]]}
    panel.on_plot_selected(c)
    assert panel.mem(c).point_info is None, "회귀: 다중 선택인데 단일 점 값이 남았다"
    c.params = {"data": [{"id": str(b["id"][7])}]}
    panel.on_plot_selected(c)
    assert panel.mem(c).point_info, "회귀: 1점 박스선택인데 값이 안 뜬다 (핸들러별 분기 재발)"

    # 절단 표기: 상한을 넘으면 "N 중 M 반영" 으로 정직하게 (조용한 500 표기 금지)
    _cap = SHOW_SAMPLES_CAP
    try:
        SHOW_SAMPLES_CAP = 3
        c.params = {"data": [{"id": str(x)} for x in b["id"][:9]]}
        panel.on_plot_selected(c)
        assert panel.mem(c).sel_total == 9 and len(panel.mem(c).selected_ids) == 3
        lbl = panel.render(c).type.properties["controls"].type.properties
        assert "9 중 3 반영" in lbl["clear_selection"].view.label, lbl["clear_selection"].view.label
    finally:
        SHOW_SAMPLES_CAP = _cap
    panel.on_clear_selection(c)
    assert panel.mem(c).sel_total == 0

    # 산점도는 **우리 선택 스테이지**를 필터로 보지 않는다 (선택 = 강조, 필터 아님)
    class _RP:
        def __init__(self, rp):
            self.request_params = rp
            self.dataset = c.dataset
    ours = {"_cls": "fiftyone.core.stages.Select", "_uuid": SHOW_SAMPLES_STAGE_ID,
            "kwargs": [["sample_ids", [str(b["id"][0])]], ["ordered", False]]}
    assert view_without_our_selection(_RP({"view": [ours]})) is None, \
        "회귀: 우리 Select 스테이지를 사용자 필터로 오인 — 박스 선택이 산점도를 접는다"
    assert view_without_our_selection(
        _RP({"extended": {"fiftyone.core.stages.Select": {"sample_ids": ["x"]}}})) is None
    assert view_without_our_selection(_RP({})) is None

    # 크로스(-prompts) 세션: 그리드는 **절대** 건드리지 않는다
    if fo.dataset_exists(ds_name + PROMPTS_SUFFIX):
        cp = _Ctx(ds_name + PROMPTS_SUFFIX)
        panel.on_load(cp)
        assert panel.mem(cp).available is True
        assert _get_fig(cp, panel.NAME), "회귀: -prompts 세션에서 이미지 산점도가 비었다"
        assert "출처:" in panel.mem(cp).banner
        # ⚠️ 계약 변경 (2026-08-31, 사용자 결정 (B)안): 크로스 모집단은 프레임 **전량이 아니라
        #    이 세션이 참조하는 분량**이다. 전량을 그리면 65.4% 가 눌러도 그리드에 보여줄 게
        #    없는 죽은 점이 되어 "선택이 안 된다" 로 읽혔다(리포트 2건).
        _ref = session_referenced_frame_ids(ds_name + PROMPTS_SUFFIX, ds_name)
        n_pts = sum(len(t["x"]) for t in _get_fig(cp, panel.NAME)[:-1])
        assert _ref is not None and 0 < len(_ref) <= n, len(_ref or ())
        assert n_pts == min(len(_ref), MAX_POINTS), (n_pts, len(_ref), n)
        assert "세션 참조" in panel.mem(cp).banner, panel.mem(cp).banner
        # 그려진 점은 **전부** 그리드를 움직일 수 있어야 한다 (죽은 점 0)
        _drawn = {i for t in _get_fig(cp, panel.NAME)[:-1] for i in t["ids"]}
        assert _drawn <= set(_ref), "회귀: 세션이 참조하지 않는 프레임이 그려졌다"
        # ── 크로스 하이라이트 (2026-08-31 사용자 리포트 회귀 가드) ──
        #    뷰가 문장으로 좁혀지면 그 문장이 달린 **프레임**이 강조돼야 한다. id 공간이
        #    달라 그대로 비교하면 교집합 0 → filepath 로 옮긴다. 모집단은 그대로 유지.
        _ps = fo.load_dataset(ds_name + PROMPTS_SUFFIX)
        _sid = str(_ps.first().id)
        cx = _Ctx(ds_name + PROMPTS_SUFFIX)
        cx.view = _ps.select([_sid])
        panel.on_load(cx)
        _cf = _get_fig(cx, panel.NAME)
        assert _cf and len(_cf[-1]["x"]) >= 1, \
            "회귀: 세션 뷰가 문장 1개로 좁혀졌는데 프레임 하이라이트가 없다"
        assert set(_cf[-1]["ids"]) <= {str(x) for x in b["id"]}, "회귀: 하이라이트가 id 공간 밖"
        # 강조는 필터가 아니다 — 모집단(= 세션 참조분)이 그대로 유지돼야 한다
        assert sum(len(t["x"]) for t in _cf[:-1]) == min(len(_ref), MAX_POINTS), \
            "회귀: 크로스 하이라이트가 산점도를 좁혔다 (강조는 필터가 아니다)"
        assert "강조" in (panel.mem(cx).banner or ""), panel.mem(cx).banner
        assert cx.ops.calls == [], f"회귀: 크로스 하이라이트가 뷰/그리드를 건드렸다 {cx.ops.calls}"
        # ⚠️ 우선순위: **자기 박스 선택이 세션 뷰 강조를 이긴다** (2026-08-31 회귀 가드)
        #    뒤집히면 뷰 바에 Select 가 있는 동안 이 패널의 선택이 매 refresh 로 사라진다.
        # ⚠️ 선택 대상은 **그려진 모집단 안에서** 골라야 한다 (모집단 밖 선택은 설계상
        #    하이라이트되지 않는다 — view_ids 와 같은 계약: "밖의 선택은 되살리지 않는다").
        _two = sorted(_ref)[:2]
        cx.params = {"data": [{"id": _two[0]}, {"id": _two[1]}]}
        panel.on_plot_selected(cx)
        _cf2 = _get_fig(cx, panel.NAME)
        assert sorted(_cf2[-1]["ids"]) == sorted(_two), \
            f"회귀: 박스 선택이 세션 뷰 강조에 덮였다 {_cf2[-1]['ids']}"
        panel.on_clear_selection(cx)          # 해제하면 세션 뷰 강조로 복귀해야 한다
        _cf3 = _get_fig(cx, panel.NAME)
        assert _cf3 and len(_cf3[-1]["x"]) >= 1, "회귀: 선택 해제 후 세션 뷰 강조가 안 돌아왔다"

        # 상한 초과는 조용히 포기하지 않는다
        cx2 = _Ctx(ds_name + PROMPTS_SUFFIX)
        cx2.view = _ps.limit(CROSS_HIGHLIGHT_MAX + 1)
        panel.on_load(cx2)
        assert "상한" in (panel.mem(cx2).banner or ""), panel.mem(cx2).banner

        # ⚠️ 계약 변경 (2026-08-31): 크로스는 그리드를 **상한 내에서 좁힌다.**
        #    옛 계약은 "절대 안 좁힌다" 였는데(최대 팬아웃 22,578 근거) 그 탓에 문장 1개짜리
        #    프레임을 골라도 그리드가 안 움직였다 — 사용자 리포트 "select 가 발동 안 했어요".
        #    이제 상한 SHOW_SAMPLES_CAP 까지만 보내고, 절단·0건을 배너가 밝힌다.
        cp.ops.calls.clear()
        cp.params = {"data": [{"id": str(b["id"][1])}, {"id": str(b["id"][2])}]}
        panel.on_plot_selected(cp)
        assert len(panel.mem(cp).selected_ids) == 2
        assert len(cp.ops.calls) <= 1, f"회귀: 크로스가 extended/뷰 2벌을 보냈다 {cp.ops.calls}"
        assert panel.mem(cp).cross_grid_note, "회귀: 팬아웃(또는 0건)을 배너에 밝히지 않았다"
        if cp.ops.calls:
            _sent = cp.ops.calls[0][1]
            assert len(_sent) <= SHOW_SAMPLES_CAP, f"회귀: 그리드 상한 초과 {len(_sent)}"
            assert cp.ops.calls[0][2] is False, "회귀: 크로스가 extended selection 을 썼다"
        # 빈 에코가 선택을 지우면 안 된다
        cp.params = {"data": []}
        panel.on_plot_selected(cp)
        assert len(panel.mem(cp).selected_ids) == 2

    # ── 문장 패널 (2026-08-14: emb_viz 를 손으로 고르는 단계를 없애는 좌하 패널) ──
    sp = SentenceEmbeddingsPanel()
    assert sp.config.name == "sentence_embeddings"
    # 그리드 반영 판정은 **패널마다** 다르다 (2026-08-31 회귀 가드):
    #   이미지 패널 @ frames 세션      → 자기 것   → 좁힌다
    #   이미지 패널 @ -prompts 세션    → 크로스    → 절대 안 좁힌다 (문장 22,578배 팬아웃)
    #   문장   패널 @ -prompts 세션    → 자기 것   → 좁힌다
    _ip = ImageEmbeddingsPanel.__new__(ImageEmbeddingsPanel)
    _spx = SentenceEmbeddingsPanel.__new__(SentenceEmbeddingsPanel)
    assert _ip.target_dataset("sourcei") == "sourcei"
    assert _ip.target_dataset("sourcei-prompts") == "sourcei"        # 크로스
    assert _spx.target_dataset("sourcei-prompts") == "sourcei-prompts"
    # ⚠️ 높이 예산은 **놓인 칸 크기**를 따라야 한다: 우측(전체 높이)은 100vh 기준, 좌하(절반)는
    #    50vh 기준. 문장 패널이 100vh 예산을 쓰면 산점도 아래가 칸 밖으로 잘린다 (실측).
    # ── 뷰 필터: 크로스 데이터셋에서는 절대 걸면 안 된다 ──
    #    -prompts 세션에서 이 패널은 frames 좌표를 그린다. 그 세션 뷰 id 로 거르면
    #    교집합 0 → 화면이 통째로 빈다(크래시 없는 조용한 실패). None 이어야 한다.
    class _ViewStub:
        _stages = [{"_cls": "match_tags"}]

        @staticmethod
        def values(_f):
            return ["deadbeef"]

    def _vctx(ds_name, has_view=True):
        return type("C", (), {
            "dataset": type("D", (), {"name": ds_name})(),
            "view": _ViewStub() if has_view else None})()
    _vp = ImageEmbeddingsPanel.__new__(ImageEmbeddingsPanel)
    assert _vp.view_ids(_vctx("frames")) == {"deadbeef"}          # 자기 데이터셋 → 건다
    assert _vp.view_ids(_vctx("frames-prompts")) is None          # 크로스 → 절대 안 건다
    assert _vp.view_ids(_vctx("frames", has_view=False)) is None  # 뷰 없음
    _sp_v = SentenceEmbeddingsPanel.__new__(SentenceEmbeddingsPanel)
    assert _sp_v.view_ids(_vctx("frames-prompts")) == {"deadbeef"}  # 문장 패널은 자기 것

    class _NoStage:
        _stages = []
    assert _vp.view_ids(type("C", (), {
        "dataset": type("D", (), {"name": "frames"})(), "view": _NoStage()})()) is None, \
        "스테이지 없는 뷰까지 values() 를 돌면 전량 199,972건 왕복(0.97s)을 매 렌더마다 낸다"

    # None(필터 없음) 과 set()(뷰 0건) 은 의미가 다르다 — 뭉개면 빈 뷰가 전량으로 보인다
    import numpy as _np
    _b = {"xy": _np.zeros((3, 2), dtype="float32"),
          "id": _np.asarray(["a", "b", "c"], dtype=object),
          "filepath": _np.asarray(["/x/a.jpg", "/x/b.jpg", "/x/c.jpg"], dtype=object)}
    # ⚠️ 호버 접미사 회귀 가드 (2026-08-31): 접미사가 per-point text 로 돌아가면
    #    200,000점에서 3.64MB 가 플롯 이벤트마다 되돌아온다 (build_figure 주석 실측).
    _bt = build_figure(b, "ground_truth")["data"][:-1]
    assert _bt, "색칠 trace 가 없다"
    for _t in _bt:
        assert "hovertemplate" in _t, "회귀: 접미사가 trace 단위로 안 실렸다"
        assert "ground_truth=" in _t["hovertemplate"]
        assert not any("ground_truth=" in _s for _s in _t["text"]), \
            "회귀: 반복 접미사가 per-point text 로 되돌아갔다"
    assert sum(len(t.get("x", [])) for t in build_figure(_b, None)["data"]) == 3
    assert sum(len(t.get("x", [])) for t in build_figure(_b, None, view_ids=set())["data"]) == 0
    assert sum(len(t.get("x", [])) for t in
               build_figure(_b, None, view_ids={"b"})["data"]) == 1

    assert "100vh" in ImageEmbeddingsPanel.PLOT_HEIGHT
    # 절반 칸 예산은 전체 칸보다 **작아야** 한다 (안 그러면 잘림이 그대로 남는다)
    assert ImageEmbeddingsPanel.HALF_PANE_PLOT_HEIGHT != ImageEmbeddingsPanel.PLOT_HEIGHT
    # 절반 칸 예산은 **vh 비례항이 100 미만**이어야 한다. 100vh 기준 상수 빼기는
    # 한 화면 크기에서만 맞는다 (2026-08-19 실측: vh=1200 에서 75px 초과).
    _h = ImageEmbeddingsPanel.HALF_PANE_PLOT_HEIGHT
    assert "58vh" in _h and "100vh" not in _h, _h
    # 분할이 [0.42, 0.58] 인 한 58 이어야 한다 — top = 0.42·vh + 171 실측에서 유도
    for _vh, _top in ((800, 507), (1000, 591), (1200, 675), (1440, 776)):
        _avail = 0.58 * _vh - 181
        assert _top + _avail <= _vh, (_vh, _top, _avail)

    def _pane_ctx(ds_name):
        return type("C", (), {"dataset": type("D", (), {"name": ds_name})()})()
    _ip = ImageEmbeddingsPanel.__new__(ImageEmbeddingsPanel)
    # 프레임 세션 = 좌하단 절반 칸 · -prompts 세션 = 우측 전체 칸
    assert _ip.plot_height(_pane_ctx("frames")) == \
        ImageEmbeddingsPanel.HALF_PANE_PLOT_HEIGHT
    assert _ip.plot_height(_pane_ctx("sourcei")) == \
        ImageEmbeddingsPanel.HALF_PANE_PLOT_HEIGHT
    assert _ip.plot_height(_pane_ctx("frames-prompts")) == \
        ImageEmbeddingsPanel.PLOT_HEIGHT
    assert "50vh" in sp.PLOT_HEIGHT and "100vh" not in sp.PLOT_HEIGHT, sp.PLOT_HEIGHT
    assert sp.PLOT_HEIGHT != ImageEmbeddingsPanel.PLOT_HEIGHT
    assert sp.target_dataset("sourcei-prompts") == "sourcei-prompts", "문장 패널은 크로스 조인 금지"
    assert panel.target_dataset("sourcei-prompts") == "sourcei", "이미지 패널은 프레임셋을 본다"
    pname = ds_name + PROMPTS_SUFFIX
    if fo.dataset_exists(pname):
        sb = load_image_bundle(pname)
        assert len(sb["xy"]) == fo.load_dataset(pname).count()
        # 문장 데이터셋엔 **문장 축**이 적용돼야 한다 (이미지 축을 쓰면 대부분 없어 1개만 뜬다)
        assert any(f == "adopted" for f, _l, _d in sb["_axes"]), "회귀: 이미지 축 목록이 적용됐다"
        assert "adopted" in sb["_fields"] and "match" in sb["_fields"], sb["_fields"]
        # DB 조인 키만 싣고 `text` 는 **안 싣는다** (npz 파생 자리표시자 43.3%)
        assert sb.get("_sentence") is True and sb.get("_gidx") is not None
        assert "text" not in sb, "회귀: 문장 번들이 npz 파생 text 를 다시 실었다"
        sfig = build_figure(sb, "adopted", banner_text=BANNER_SENTENCE)
        drawn = sum(len(t["x"]) for t in sfig["data"][:-1])
        assert drawn <= MAX_POINTS, drawn      # 60만 전량 렌더 금지 (110초 + Chrome 크래시)
        assert "문장 임베딩" in sfig["banner"] and f"/{len(sb['xy']):,}장" in sfig["banner"], \
            sfig["banner"]
        # ── 호버 상한 초과 모드 (현 데이터 200,000 > 20,000): per-point text 금지 ──
        if drawn > HOVER_TEXT_MAX_POINTS:
            assert "호버는 클래스만" in sfig["banner"], sfig["banner"]
            assert all("text" not in t for t in sfig["data"][:-1]), \
                "회귀: 호버 상한을 넘었는데 per-point text 를 실었다 — 플롯 이벤트가 다시 무거워진다"
            assert all("adopted=" in t.get("hovertemplate", "") for t in sfig["data"][:-1]), \
                "회귀: 호버를 뺀 자리에 클래스 표기(trace 단위, 점당 0바이트)도 없다"
            # ⚠️ 호버를 뺀 대가가 **정보 소실**이 되면 안 된다 — 클릭 표시가 그 값을 되찾아야 한다
            assert point_label_for_id(sb, str(sb["id"][0])), \
                "회귀: 클릭 표시가 문장을 못 되찾는다 (호버 제거가 정보 소실로 전락)"
        # ── 상한 아래 모드: 옛 계약 그대로 (문장 호버 + DB 출처 표기, 조용한 폴백 금지) ──
        _oh = HOVER_TEXT_MAX_POINTS
        try:
            HOVER_TEXT_MAX_POINTS = 10 ** 9
            sfig_h = build_figure(sb, "adopted", banner_text=BANNER_SENTENCE)
            assert "출처" in sfig_h["banner"], sfig_h["banner"]
            hov, hmeta = _sentence_texts(sb, list(range(min(200, len(sb["xy"])))))
            assert hov is not None and hmeta is not None
            if hmeta["db_rows"]:
                assert PDB_SRC_DB in sfig_h["banner"], sfig_h["banner"]
                hover0 = sfig_h["data"][0]["text"][0]
                assert not hover0.startswith("(DB"), hover0
                assert ".jpg" not in hover0, hover0        # 파일명 회귀 금지
        finally:
            HOVER_TEXT_MAX_POINTS = _oh
        # 이미지 패널(비-문장 번들)은 예전 그대로 파일명 호버 + DB 표기 없음
        assert _sentence_texts(b, [0]) == (None, None)
        assert "출처: **DB" not in build_figure(b, "ground_truth")["banner"]
        # 마운트 즉시 축이 자동 선택되고 산점도가 채워져야 한다 — 그래야 '손으로 고르기' 가 없다
        cs = _Ctx(pname)
        sp.on_load(cs)
        assert sp.mem(cs).color_by in [f for f, _l, _d in SENTENCE_CANDIDATES], \
            sp.mem(cs).color_by
        assert _get_fig(cs, sp.NAME), "회귀: 문장 패널이 빈 산점도 — 손으로 고를 단계가 다시 생긴다"
        assert "문장 임베딩" in sp.mem(cs).banner, sp.mem(cs).banner
        assert sp.render(cs).type.properties["img_scatter"].view.data
        # 두 패널이 같은 세션에서 서로의 fig 를 덮지 않아야 한다 (캐시 키에 데이터셋+축)
        ci = _Ctx(pname)
        panel.on_load(ci)                      # 같은 -prompts 세션의 이미지 패널
        assert "이미지 임베딩" in panel.mem(ci).banner
        assert "문장 임베딩" in sp.mem(cs).banner, "회귀: 이미지 패널이 문장 배너를 덮었다"

    # ── 그리드 체크박스 선택 (2026-09-09 회귀 방지) ────────────────────────────
    # ① 이벤트가 도달해야 한다: FiftyOne 은 패널이 이 메서드를 정의한 경우에만
    #    selection 이벤트를 보낸다 — 없으면 render 조차 안 불려 "아무 반응이 없다".
    assert callable(getattr(ImageEmbeddingsPanel, "on_change_selected", None)), \
        "회귀: on_change_selected 가 없다 — 그리드 선택 이벤트가 패널에 도달하지 않는다"
    # ② 캐시 키가 선택을 봐야 한다: 그리드 선택은 뷰·extended 를 안 바꾸므로 지문이
    #    그대로면 render 가 하이라이트 없는 옛 figure 를 캐시 히트로 낸다.
    class _SelCtx(_Ctx):
        def __init__(self, name, sel):
            super().__init__(name)
            self.selected = list(sel)
            self.request_params = {}
    k_none = view_sig(_SelCtx(ds_name, []))
    k_one = view_sig(_SelCtx(ds_name, ["a" * 24]))
    assert k_none != k_one, "회귀: 그리드 선택이 fig 캐시 지문에 반영되지 않는다"
    assert view_sig(_SelCtx(ds_name, ["b" * 24, "a" * 24])) == \
        view_sig(_SelCtx(ds_name, ["a" * 24, "b" * 24])), "순서만 다른 에코는 같은 지문이어야 한다"
    # ③ 비-크로스는 고른 id 를 그대로 강조하고, 선택이 없으면 손대지 않는다(None).
    gp = ImageEmbeddingsPanel()
    assert gp._grid_highlight(_SelCtx(ds_name, []), ds_name, ds_name, False) == (None, None)
    ids3, note3 = gp._grid_highlight(_SelCtx(ds_name, ["x" * 24]), ds_name, ds_name, False)
    assert ids3 == ["x" * 24] and "그리드 선택" in note3, (ids3, note3)
    # ④ 크로스는 상한을 넘으면 **조용히 포기하지 않고** 사유를 배너에 밝힌다.
    over = [f"{i:024d}" for i in range(CROSS_HIGHLIGHT_MAX + 1)]
    ids4, note4 = gp._grid_highlight(_SelCtx(ds_name, over), "x-prompts", ds_name, True)
    assert ids4 is None and "상한" in note4, (ids4, note4)

    print("selftest OK")


if __name__ == "__main__":
    selftest()
