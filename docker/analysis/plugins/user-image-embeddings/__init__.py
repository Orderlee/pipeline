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
import os
import threading

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
            " WHERE b.source = 'userwatch' "          # gidx 규약(userwatch:<tag>)을 쓰는 원장만
            " GROUP BY 1, 2, 3", ())
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
    meta = {"db_rows": 0, "db_versions": [], "reject": {}, "err": None}
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
SHOW_SAMPLES_CAP = 500   # 그리드 반영 상한 — 요청 폭증 방지 (아래 on_plot_selected 주석)

# 산점도 trace 리스트의 서버측 보관소. panel state 에 실으면 이후 모든 훅 요청의
# panel_state + spaces 트리에 2벌로 왕복하는데, 서버가 요청 바디 **1MB 당 ~2.5초**를
# 태운다 (2026-08-14 curl 실측: 4MB POST = 10초). user-prompt-compare 와 같은 처치.
_FIGDATA = {}


def _fig_key(ctx):
    """fig 캐시 키 = (데이터셋, 색칠축).

    ⚠️ panel_id 로 키를 잡으면 안 된다 (2026-08-14 실측): panel_id 는 **훅 요청에만**
    실려 오고 render() 경로에는 없을 수 있어, _refresh 가 넣은 fig 를 render 가 못 찾아
    패널이 통째로 빈 채 그려졌다(배너·드롭다운·산점도 동시 소실). 데이터셋+색칠축은 양쪽
    경로에서 모두 접근 가능하고, 이 두 값이 같으면 fig 도 같다(선택 하이라이트는 아래
    build_figure 가 selected_ids 로 매번 다시 얹으므로 키에 넣지 않는다 — 선택이 바뀌면
    _refresh 가 어차피 새로 put 한다).
    """
    ds = getattr(ctx, "dataset", None)
    return (ds.name if ds is not None else "-", ctx.panel.state.color_by or "")


def _put_fig(ctx, data):
    _FIGDATA[_fig_key(ctx)] = data
    while len(_FIGDATA) > 8:
        _FIGDATA.pop(next(iter(_FIGDATA)))


def _get_fig(ctx):
    return _FIGDATA.get(_fig_key(ctx))


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
    key = (dataset_name, BRAIN_KEY, str(ds.last_modified_at))
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


def build_figure(bundle, color_by, selected_ids=None, cross_note="",
                 banner_text=BANNER_CROSS):
    """이미지 산점도. trace = 색칠 그룹별 1개 + 마지막 하이라이트.

    그룹별 trace 로 쪼개는 이유: Plotly 범례는 trace 단위라, 단일 trace + per-point 색
    배열이면 범례에 클래스→색 매핑이 아예 안 나온다.
    """
    import numpy as np
    b = bundle
    n = len(b["xy"])
    idx = np.arange(n)
    groups = b.get(color_by)
    if groups is None:
        groups = np.asarray(["전체"] * n, dtype=object)
    sel = set(selected_ids or ())
    if n > MAX_POINTS:
        idx = np.asarray(stratified_subsample(groups, MAX_POINTS), dtype=np.int64)
        # 선택된 점은 서브샘플에서 탈락해도 **반드시 남긴다** (코드리뷰 지적, 2026-08-14):
        # 층화는 현재 색칠축 기준이라 축을 바꾸면 살아남는 표본이 달라져, 방금 고른 점이
        # 하이라이트만이 아니라 산점도에서 통째로 사라진다 — 조용해서 더 나쁘다.
        # (현 데이터 7,498·13,144 는 MAX_POINTS 아래라 아직 미도달 경로.)
        if sel:
            keep_sel = np.nonzero(np.isin(b["id"], np.asarray(sorted(sel), dtype=object)))[0]
            idx = np.union1d(idx, keep_sel)
    g_sub = groups[idx]
    # 문장 패널은 호버에 **DB 정본 문장**을 싣는다 (2026-08-19). 예전에는 이 자리에
    # `filepath` 파일명이 떴는데, 문장 샘플의 filepath 는 "가장 가까운 이미지"라 문장을
    # 식별하지 못했다. 그려지는 점만(≤MAX_POINTS) 배치 조회한다 — 전량 로드 금지.
    hov, text_meta = _sentence_texts(b, idx)
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
            "text": [f"{_point_label(b, k, hov)}<br>{color_by}={hov_label}" for k in ii],
            "hoverinfo": "text",
            "marker": {"color": _color_for(grp, i), "size": 6, "opacity": 0.9,
                       "symbol": _symbol_for(grp, i),
                       "line": {"width": 0.5, "color": "#FFFFFF"}},
        }
        if grp == "none":
            # 지배 카테고리(예: normalized_class 의 '검출 없음' 59.9%)를 범례에서 기본
            # 흐리게/치워 둔다 — Plotly 는 legendonly trace 도 범례 클릭 한 번으로 다시
            # 켜지는 **네이티브 토글**이라 "지운다" 가 아니라 "치운다" 다(2026-08-19 사용자
            # 요청: "'none'이 지배 범주라 범례에서 흐리게/토글 가능하게"). 데이터·표시 카운트
            # (banner의 "표시 X/Y장")는 그대로 유지 — 안 보이는 건 렌더 상태일 뿐이다.
            trace["visible"] = "legendonly"
        data.append(trace)
    hi = idx[np.isin(b["id"][idx], np.asarray(sorted(sel), dtype=object))] if sel else idx[:0]
    data.append({
        "type": "scattergl", "mode": "markers", "name": "선택",
        "x": np.round(b["xy"][hi, 0].astype("float64"), 3).tolist(),
        "y": np.round(b["xy"][hi, 1].astype("float64"), 3).tolist(),
        "ids": [str(b["id"][k]) for k in hi],
        # 다크 배경에서 검은 링은 안 보인다 — Okabe-Ito 노랑(클래스 색과 무교집합)
        "marker": {"color": "#F0E442", "size": 14, "symbol": "circle-open",
                   "line": {"width": 3}},
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
    banner += f" · 표시 {shown:,}/{n:,}장 · {banner_text}"
    if cross_note:
        banner += f" · {cross_note}"
    if text_meta is not None:
        # 조용한 폴백 금지 — 호버 문장을 DB 에서 몇 행 가져왔고 어느 버전이 빠졌는지 밝힌다.
        banner += " · " + pdb_note(text_meta)
    return {"data": data, "banner": banner,
            # height 고정 금지 — 실높이는 render() 의 vh 기반 view height 가 정한다.
            # title 금지 — plotly title 은 modebar 와 같은 영역이라 글자가 아이콘에 겹친다.
            "layout": {"showlegend": True, "dragmode": "pan", "autosize": True,
                       "xaxis": {"visible": False}, "yaxis": {"visible": False},
                       "margin": {"l": 10, "r": 10, "t": 30, "b": 10}}}


class ImageEmbeddingsPanel(foo.Panel):
    # 서브클래스(문장 패널)가 갈아끼우는 지점 — 나머지 로직은 전부 공유한다.
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

    @property
    def config(self):
        # ⚠️ 패널 이름을 바꾸면 **저장된 워크스페이스가 옛 이름을 가리켜** App 이
        #    `Panel "<옛이름>" no longer exists!` 를 띄운다 — 이름 변경 시 반드시
        #    `fiftyone_app_setup.py workspace-compare` 재실행으로 워크스페이스를 다시 저장할 것.
        #    (2026-08-14: user_image_embeddings → image_embeddings 로 변경, 등록된 91개
        #    오퍼레이터에 image_embeddings 는 없어 충돌 없음 — 네이티브 Embeddings 패널은
        #    App 내장이라 오퍼레이터 레지스트리에 아예 없다.)
        return foo.PanelConfig(name="image_embeddings",
                               label="Image Embeddings", surfaces="grid")

    # ── 상태 ──
    def on_load(self, ctx):
        # ⚠️ **이미 있는 값을 덮지 않는다** (2026-08-14 실사용 버그): on_load 는 패널
        #    리마운트·빈 상태 에코·render 자가복구로 **반복 호출**된다. 여기서 color_by 를
        #    무조건 None 으로 밀면 사용자가 고른 색칠 축이 매번 기본값으로 되돌아가
        #    "드롭다운을 바꿔도 그림이 안 바뀐다" 로 보인다 (gotchas §16 과 같은 증상 —
        #    compare 패널의 알려진 미수정 버그를 여기서 되풀이하지 않는다).
        #    데이터셋이 바뀌어 그 축이 없어진 경우는 _refresh 가 첫 필드로 교체해 준다.
        ctx.panel.state.color_by = ctx.panel.state.color_by or None
        ctx.panel.state.selected_ids = ctx.panel.state.selected_ids or []
        ctx.panel.state.fields = []
        ctx.panel.state.available = True
        self._refresh(ctx)

    def _sync_controls(self, ctx):
        ctx.panel.state.set("controls", {"color_by": ctx.panel.state.color_by or ""})

    def _refresh(self, ctx, update_plot=True):
        self._sync_controls(ctx)
        # 옛 set_data 잔재가 있으면 스키마 data 를 영원히 가린다 (App `mt||view.data` 우선순위)
        ctx.panel.data.clear()

        session = ctx.dataset.name if ctx.dataset is not None else None
        if session is None:
            return

        def _unavailable(msg):
            # ⚠️ 컨트롤 상태도 **반드시** 함께 비운다: fields 를 남기면 데이터셋을 전환했을 때
            #    배너는 "찾을 수 없습니다" 인데 색칠 드롭다운에는 이전 데이터셋의 필드가
            #    그대로 남아 고를 수 있는 유령 컨트롤이 된다 (코드리뷰 지적, 2026-08-14).
            ctx.panel.state.available = False
            ctx.panel.state.fields = []
            ctx.panel.state.axes = []
            ctx.panel.state.selected_ids = []
            ctx.panel.state.banner = msg
            ctx.panel.state.layout = {"xaxis": {"visible": False},
                                      "yaxis": {"visible": False}}
            _put_fig(ctx, [])

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

        ctx.panel.state.available = True
        ctx.panel.state.fields = list(b["_fields"])
        ctx.panel.state.axes = list(b["_axes"])
        color_by = ctx.panel.state.color_by
        if color_by not in b["_fields"]:
            # frames_name 기준(세션이 아니라 **좌표 출처 데이터셋** 기준) — -prompts 세션에서
            # 크로스로 그릴 때도 이미지 데이터셋의 권장 축을 그대로 따라간다.
            color_by = default_color_by(frames_name, b["_fields"])
            ctx.panel.state.color_by = color_by
        if update_plot:
            note = f"출처: `{frames_name}` (세션은 `{session}`)" if cross else ""
            fig = build_figure(b, color_by,
                               selected_ids=ctx.panel.state.selected_ids, cross_note=note,
                               banner_text=self.BANNER)
            ctx.panel.state.banner = fig["banner"]
            ctx.panel.state.layout = fig["layout"]
            _put_fig(ctx, fig["data"])

    # ── 컨트롤 ──
    def on_color_change(self, ctx):
        v = (ctx.params or {}).get("value")
        if not v or _change_guard(ctx, "color_by", v, v == ctx.panel.state.color_by):
            return
        ctx.panel.state.color_by = v
        self._refresh(ctx)

    # ── 선택 ──
    #   크로스(-prompts 세션)에서는 **그리드를 건드리지 않는다**: 이미지 1장에 문장 샘플이
    #   최대 22,578개 달려 있어 show_samples 로 넘기면 요청이 MB 단위로 부풀고(서버 2.5s/MB)
    #   그리드도 의미를 잃는다. 같은 데이터셋일 때만 그리드를 좁힌다.
    def _apply_selection(self, ctx, ids):
        ctx.panel.state.selected_ids = list(ids)[:SHOW_SAMPLES_CAP]
        session = ctx.dataset.name if ctx.dataset is not None else None
        if session and frames_dataset_name(session) == session and ids:
            # ⚠️ `use_extended_selection=True` 필수. 기본값(False)은 뷰 스테이지를 갈아끼울
            #    뿐이라 형제 패널 user-prompt-compare 의 `on_change_extended_selection` 훅이
            #    영영 안 울린다 — compare 워크스페이스에서 좌하 산점도로 프레임을 골라도
            #    우측 문장 표가 반응하지 않는 증상. 2026-08-19 이 패널이 네이티브
            #    Embeddings 를 대체하면서 그 훅의 유일한 발신자가 됐다.
            ctx.ops.show_samples(list(ids)[:SHOW_SAMPLES_CAP],
                                 use_extended_selection=True)
        self._refresh(ctx)

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
        ctx.panel.state.selected_ids = []
        session = ctx.dataset.name if ctx.dataset is not None else None
        if session and frames_dataset_name(session) == session:
            ctx.ops.clear_view()
        self._refresh(ctx)

    def render(self, ctx):
        panel = types.Object()
        # 자가 복구 (2026-08-14 실측): 공유 세션이라 옛 탭의 **빈 panel_state 에코**나 패널
        # 리마운트로 상태가 통째로 비는 일이 있다. 그대로 그리면 배너·드롭다운·산점도가
        # 동시에 사라진 빈 패널이 된다 — 지울 사용자 선택이 없는 상태에서만 발동하므로
        # on_load 로 되살리는 편이 항상 이득. (user-prompt-compare 도 같은 처치.)
        if not ctx.panel.state.fields and ctx.panel.state.available is not False \
                and getattr(ctx, "dataset", None) is not None:
            self.on_load(ctx)
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
        fields = ctx.panel.state.fields or []
        if fields:
            # 드롭다운은 **고르기 전에도** 축을 구분할 수 있어야 한다 — 라벨에 단위(영상/
            # 프레임)를, description 에 의미를 싣는다 (App 이 옵션 부제로 렌더).
            meta = {f: (lab, desc)
                    for f, lab, desc in (ctx.panel.state.axes or COLOR_CANDIDATES)}
            choices = types.Choices()
            for f in fields:
                lab, desc = meta.get(f, (f, ""))
                choices.add_choice(f, label=lab, description=desc)
            row.enum("color_by", choices.values(), label="색칠", view=choices,
                     on_change=self.on_color_change)
        if ctx.panel.state.selected_ids:
            row.btn("clear_selection",
                    label=f"✕ 선택 해제 ({len(ctx.panel.state.selected_ids)})",
                    on_click=self.on_clear_selection)
        if ctx.panel.state.banner:
            panel.md(ctx.panel.state.banner, name="banner_md")
        fig_data = _get_fig(ctx)
        if fig_data is None and ctx.panel.state.available is not False:
            # 프로세스 재시작/캐시 축출 후 결정론 재구성 (번들 warm 이면 ~0.1s)
            try:
                session = ctx.dataset.name if ctx.dataset is not None else None
                frames_name = self.target_dataset(session)
                b = load_image_bundle(frames_name)
                fig = build_figure(
                    b, ctx.panel.state.color_by or default_color_by(frames_name, b["_fields"]),
                    selected_ids=ctx.panel.state.selected_ids,
                    banner_text=self.BANNER)
                fig_data = fig["data"]
                _put_fig(ctx, fig_data)
            except Exception:
                fig_data = None
        panel.plot("img_scatter", data=fig_data or [],
                   layout=ctx.panel.state.layout or {},
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

    BANNER = BANNER_SENTENCE
    NOT_FOUND = NO_SENTENCES_TEXT
    # 좌측 스택 아래 칸(= 화면 높이의 절반) 기준. 50vh 에서 컨트롤+배너 2줄+여유 140px 를 뺀다
    # — 1080 뷰포트에서 400px, 900 뷰포트에서 310px. 칸을 넘지 않으면서 산점도가 판독 가능한 값.
    PLOT_HEIGHT = "max(240px, calc(50vh - 140px))"

    def target_dataset(self, session):
        return session          # 크로스 조인 없음 — 자기 데이터셋의 문장 좌표

    @property
    def config(self):
        return foo.PanelConfig(name="sentence_embeddings",
                               label="Sentence Embeddings", surfaces="grid")


def register(p):
    p.register(ImageEmbeddingsPanel)
    p.register(SentenceEmbeddingsPanel)


# ── selftest (컨테이너에서 `python __init__.py`) ──
def selftest():
    global MAX_POINTS        # ④ 서브샘플 경로를 상한 축소로 강제하기 위해 (아래 finally 복원)
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
            self.calls.append(("show_samples", list(ids)))
        def clear_view(self):
            self.calls.append("clear_view")

    class _Ctx:
        def __init__(self, name):
            self.panel = _Panel()
            self.dataset = fo.load_dataset(name)
            self.params = {}
            self.ops = _Ops()

    panel = ImageEmbeddingsPanel()
    c = _Ctx(ds_name)
    panel.on_load(c)
    schema = panel.render(c)
    view = schema.type.properties["img_scatter"].view
    assert view.data, "회귀: 최초 마운트 스키마에 data 없음 — 빈 산점도"
    assert c.panel.state.color_by == "ground_truth"
    assert "color_by" in schema.type.properties["controls"].type.properties
    assert c.panel.state.banner and "이미지 임베딩" in c.panel.state.banner
    # 큰 데이터는 state 에 실리지 않는다 (요청 2.5s/MB 재발 방지)
    assert c.panel.state.get("scatter_data") is None
    assert c.panel.state.get("img_scatter") is None
    # 패널 이름 계약: 워크스페이스(fiftyone_app_setup._compare_space)가 이 이름으로 패널을
    # 참조한다 — 어긋나면 App 이 `Panel "<name>" no longer exists!` 만 띄운다 (2026-08-14).
    assert panel.config.name == "image_embeddings", panel.config.name

    # ── 2026-08-14 실사용 회귀 3종 (패널이 빈 채로 그려지던 원인) ──
    # ① fig 캐시 키는 panel_id 에 의존하면 안 된다: 훅 요청(panel_id 있음)이 넣은 fig 를
    #    render(panel_id 없음)가 반드시 찾아야 한다. 못 찾으면 패널이 통째로 빈다.
    c2 = _Ctx(ds_name)
    c2.params = {"panel_id": "hook-req-1"}      # 훅 요청처럼 panel_id 를 싣고 갱신
    panel.on_load(c2)
    c2.params = {}                              # render 는 panel_id 없이 온다
    assert _get_fig(c2), "회귀: panel_id 유무로 fig 캐시 키가 갈려 render 가 fig 를 잃는다"
    sch2 = panel.render(c2)
    assert sch2.type.properties["img_scatter"].view.data, "회귀: render 가 빈 산점도를 냈다"

    # ② 상태가 통째로 빈 에코 → render 가 on_load 로 자가 복구해야 한다
    c3 = _Ctx(ds_name)                          # on_load 없이 곧바로 render (빈 state)
    sch3 = panel.render(c3)
    assert c3.panel.state.fields, "회귀: 빈 상태 에코에서 자가 복구 실패 — 빈 패널로 굳는다"
    assert sch3.type.properties["img_scatter"].view.data
    assert "color_by" in sch3.type.properties["controls"].type.properties

    # ②-b ⚠️ 자가 복구(on_load 재실행)가 **사용자 선택을 덮으면 안 된다** — 이걸 놓치면
    #      드롭다운을 바꿔도 매번 기본 축으로 되돌아간다 (2026-08-14 실사용 버그).
    if "category" in b["_fields"]:
        c5 = _Ctx(ds_name)
        panel.on_load(c5)
        c5.params = {"value": "category"}
        panel.on_color_change(c5)
        assert c5.panel.state.color_by == "category"
        panel.on_load(c5)                       # 리마운트/빈 에코로 재호출
        assert c5.panel.state.color_by == "category", \
            "회귀: on_load 가 사용자가 고른 색칠 축을 기본값으로 되돌린다"
        panel.render(c5)                        # 자가복구 경로도 같이
        assert c5.panel.state.color_by == "category"
        assert labs["category"] in c5.panel.state.banner, c5.panel.state.banner

    # ③ 이미지가 없는 세션은 컨트롤도 함께 비워야 한다 (유령 드롭다운 방지)
    c4 = _Ctx(ds_name)
    panel.on_load(c4)
    assert c4.panel.state.fields                # 정상 데이터셋에서 채워둔 뒤
    class _Missing:
        name = "__no_such_dataset__-prompts"
    c4.dataset = _Missing()
    panel._refresh(c4)
    assert c4.panel.state.available is False
    assert c4.panel.state.fields == [], "회귀: 없는 데이터셋인데 이전 색칠 필드가 남았다"
    assert c4.panel.state.selected_ids == []
    assert NO_IMAGES_TEXT in c4.panel.state.banner
    # 그 상태의 render 는 색칠 드롭다운을 만들지 않는다 (자가복구도 발동하면 안 됨)
    sch4 = panel.render(c4)
    assert "color_by" not in sch4.type.properties["controls"].type.properties

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
    c.params = {"id": str(b["id"][3])}
    panel.on_plot_click(c)
    assert c.ops.calls and c.ops.calls[-1][0] == "show_samples"
    assert c.panel.state.selected_ids == [str(b["id"][3])]

    # 크로스(-prompts) 세션: 그리드는 **절대** 건드리지 않는다
    if fo.dataset_exists(ds_name + PROMPTS_SUFFIX):
        cp = _Ctx(ds_name + PROMPTS_SUFFIX)
        panel.on_load(cp)
        assert cp.panel.state.available is True
        assert _get_fig(cp), "회귀: -prompts 세션에서 이미지 산점도가 비었다"
        assert "출처:" in cp.panel.state.banner
        n_pts = sum(len(t["x"]) for t in _get_fig(cp)[:-1])
        assert n_pts == min(n, MAX_POINTS), (n_pts, n)
        cp.params = {"data": [{"id": str(b["id"][1])}, {"id": str(b["id"][2])}]}
        panel.on_plot_selected(cp)
        assert cp.ops.calls == [], "회귀: 크로스 세션이 그리드를 좁혔다 (60만 샘플 폭발)"
        assert len(cp.panel.state.selected_ids) == 2
        # 빈 에코가 선택을 지우면 안 된다
        cp.params = {"data": []}
        panel.on_plot_selected(cp)
        assert len(cp.panel.state.selected_ids) == 2

    # ── 문장 패널 (2026-08-14: emb_viz 를 손으로 고르는 단계를 없애는 좌하 패널) ──
    sp = SentenceEmbeddingsPanel()
    assert sp.config.name == "sentence_embeddings"
    # ⚠️ 높이 예산은 **놓인 칸 크기**를 따라야 한다: 우측(전체 높이)은 100vh 기준, 좌하(절반)는
    #    50vh 기준. 문장 패널이 100vh 예산을 쓰면 산점도 아래가 칸 밖으로 잘린다 (실측).
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
        # 호버 = **문장**이어야 한다 (옛 동작: 최근접 이미지 파일명 — 문장을 식별 못 했다).
        # 출처 표기는 항상 붙는다 (조용한 폴백 금지).
        assert "출처" in sfig["banner"], sfig["banner"]
        hov, hmeta = _sentence_texts(sb, list(range(min(200, len(sb["xy"])))))
        assert hov is not None and hmeta is not None
        if hmeta["db_rows"]:
            assert PDB_SRC_DB in sfig["banner"], sfig["banner"]
            hover0 = sfig["data"][0]["text"][0]
            assert not hover0.startswith("(DB"), hover0
            assert ".jpg" not in hover0.split("<br>")[0], hover0   # 파일명 회귀 금지
        # 이미지 패널(비-문장 번들)은 예전 그대로 파일명 호버 + DB 표기 없음
        assert _sentence_texts(b, [0]) == (None, None)
        assert "출처: **DB" not in build_figure(b, "ground_truth")["banner"]
        # 마운트 즉시 축이 자동 선택되고 산점도가 채워져야 한다 — 그래야 '손으로 고르기' 가 없다
        cs = _Ctx(pname)
        sp.on_load(cs)
        assert cs.panel.state.color_by in [f for f, _l, _d in SENTENCE_CANDIDATES], \
            cs.panel.state.color_by
        assert _get_fig(cs), "회귀: 문장 패널이 빈 산점도 — 손으로 고를 단계가 다시 생긴다"
        assert "문장 임베딩" in cs.panel.state.banner, cs.panel.state.banner
        assert sp.render(cs).type.properties["img_scatter"].view.data
        # 두 패널이 같은 세션에서 서로의 fig 를 덮지 않아야 한다 (캐시 키에 데이터셋+축)
        ci = _Ctx(pname)
        panel.on_load(ci)                      # 같은 -prompts 세션의 이미지 패널
        assert "이미지 임베딩" in ci.panel.state.banner
        assert "문장 임베딩" in cs.panel.state.banner, "회귀: 이미지 패널이 문장 배너를 덮었다"

    print("selftest OK")


if __name__ == "__main__":
    selftest()
