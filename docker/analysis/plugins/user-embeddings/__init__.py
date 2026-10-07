"""Embeddings 패널 보강 — OSS 에서 막힌 두 가지를 버튼으로 되살린다.

1. `compute_visualization` — OSS 는 패널의 `+` 를 Enterprise CTA 로 하드코딩해뒀다
   (`APP_MODE="fiftyone"` 빌드타임 상수 → minifier 가 실제 호출 분기를 지워버려
   번들 패치 없이는 못 되살림). 네이티브 placement API 로 같은 자리에 버튼을 더 단다.
   `@voxel51/brain` 원본 프롬프트는 입력이 12개인데다 embeddings 를 비우면 zoo
   모델을 받으러 가서 **빈 brain key 만 남기고 실패**한다 — 이 데이터셋에 맞춰
   임베딩 필드를 자동 선택하는 4-입력 폼으로 감쌌다.

2. `combine_color_fields` — Color by 는 필드 하나만 받는다. 두 필드를 합친
   파생 StringField 를 만들어 그걸 고르게 한다 (조합별 색 = 사실상 2-필드 색칠).

3. `save_visualization_coords` — 패널에는 축 눈금/라벨 토글이 없다 (UMAP/t-SNE 축값은
   재실행마다 바뀌어 해석 불가라 의도된 설계). 좌표가 필요하면 brain 결과의 points 를
   `<key>_x`/`<key>_y` FloatField 로 꺼낸다 — 사이드바 슬라이더 필터·Color by 그라디언트로
   쓸 수 있다.

4. `move_media` / `delete_media` — App 은 **디스크 파일을 건드리는 버튼이 없다.**
   기본 `delete_selected_samples` 는 DB 샘플만 지우고 파일은 남기고, 이동은 아예 없다.
   선택한 샘플(또는 현재 뷰)의 미디어를 실제로 옮기거나 지운다. 이동은 `filepath` 까지
   갱신해 데이터셋이 안 깨지게 한다.

5. `compute_visualization` 의 **prompt DB 임베딩 소스** (2026-08-19 "DB 연결 해야해
   이제 gidx 그걸로 하지마") — `<X>-prompts` 데이터셋은 문서 부피 때문에 1024-d 벡터를
   샘플에 저장하지 않는다. 그래서 이 오퍼레이터는 그 데이터셋에서 "임베딩 필드가 없습니다"
   로 막혀 있었다. 이제 (bank_version, gidx) → Postgres `bank_sentences` ⨝
   `image_embeddings(entity_type='prompt')` 로 벡터를 끌어와 계산한다 — npz 경유 아님.

로직 자체 검증: 컨테이너에서 `python __init__.py` (파일 이동/충돌 처리 assert).
"""

import base64
import binascii
import contextlib
import gc
import json
import os
import random
import shutil
import sys
import threading
import time

import numpy as np
import requests

import fiftyone as fo
import fiftyone.brain as fob
import fiftyone.operators as foo
import fiftyone.operators.types as types

COMBO_SEPARATOR = " | "

# 이 오퍼레이터들은 **FiftyOne App 프로세스 안에서** 실행된다. 188K 데이터셋의
# 임베딩을 한 번에 올리면 12GB(list[float] 1024-d)라 앱과 호스트가 같이 죽는다
# (2026-07-28 실측: 가용 15GB→1GB, add 속도 404→0.1/s 붕괴). 그래서 전부 배치.
FIT_MAX = 30_000  # 이 이하면 통짜 계산, 초과하면 샘플-fit → 배치 transform
TBATCH = 10_000  # 임베딩 로드/변환 배치
SET_BATCH = 20_000  # set_values 배치


def _batches(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _thread_cap(max_threads=4):
    """계산 중에만 BLAS/OpenMP 스레드를 묶어 호스트 CPU 독점을 막는다."""
    try:
        import threadpoolctl

        return threadpoolctl.threadpool_limits(max_threads)
    except Exception:  # noqa: BLE001 — 없으면 캡 없이 진행
        return contextlib.nullcontext()


def _embeddings_of(dataset, ids, field):
    return np.asarray(
        dataset.select(ids, ordered=True).values(field), dtype="float32"
    )


def _set_values_batched(dataset, field, mapping_items):
    """{sample_id: value} 를 배치로 나눠 쓴다 — 188K 단일 bulk write 회피."""
    for chunk in _batches(mapping_items, SET_BATCH):
        dataset.set_values(field, dict(chunk), key_field="id")
        gc.collect()

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
_PDB_BANKS_AT = 0.0              # 실패 캐시 시각 — 60초 뒤 재시도
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
    global _PDB_BANKS, _PDB_BANKS_ERR, _PDB_BANKS_AT
    if _PDB_BANKS is not None and not refresh and not (_PDB_BANKS_ERR and time.time() - _PDB_BANKS_AT > 60):
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
        _PDB_BANKS_AT = time.time()
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

    global _PDB_BANKS, _PDB_BANKS_ERR, _PDB_BANKS_AT
    saved, saved_err, saved_text = _PDB_BANKS, _PDB_BANKS_ERR, dict(_PDB_TEXT)
    saved_at = _PDB_BANKS_AT
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
        _PDB_BANKS_AT = time.time()      # 60초 재시도 창 안 — 실제 DB 를 두드리지 않는다
        out, meta = pdb_resolve_texts(["v1.0.8.0"], [0], ["fb"], {})
        assert out == ["fb"] and "down" in pdb_note(meta)
    finally:
        _PDB_BANKS, _PDB_BANKS_ERR, _PDB_BANKS_AT = saved, saved_err, saved_at
        _PDB_TEXT.clear()
        _PDB_TEXT.update(saved_text)


METHODS = (
    ("umap", "UMAP", "비선형 — 국소 군집이 잘 갈린다 (기본)"),
    ("tsne", "t-SNE", "비선형 — 느리지만 촘촘한 군집에 강함"),
    ("pca", "PCA", "선형 — 즉시 계산, 전역 구조 보존"),
)


def _vector_fields(dataset):
    """임베딩으로 쓸 수 있는 필드. tags 같은 문자열 리스트는 제외."""
    numeric = (fo.FloatField, fo.IntField)
    out = []
    for name, field in dataset.get_field_schema().items():
        if isinstance(field, fo.VectorField):
            out.append(name)
        elif isinstance(field, fo.ListField) and isinstance(field.field, numeric):
            out.append(name)
    return out


# ── prompt DB 임베딩 소스 ────────────────────────────────────────────────────
# `<X>-prompts` 데이터셋은 문서 부피(1024-d × 60만) 때문에 벡터를 샘플에 저장하지
# 않는다 → `_vector_fields()` 가 빈 리스트라 이 오퍼레이터가 통째로 막혀 있었다.
# 정본 벡터는 Postgres 에 있다 (`image_embeddings`, entity_type='prompt', 121,614행,
# `bank_sentences.content_hash` 로 조인). 드롭다운에 이 소스를 하나 더 단다.
PDB_EMBED_CHOICE = "__prompt_db__"
PDB_EMBED_LABEL = "prompt DB (Postgres) — 문장 벡터"
# 1024-d float 을 파이썬으로 끌어오는 경로다 (1만행 ≈ 40MB + 파싱). App 프로세스 안에서
# 동기로 도는 오퍼레이터라 상한을 두고 뷰를 좁히게 만든다 — MAX_FILE_OPS 와 같은 규약.
PDB_MAX_VECTORS = 60_000
PDB_FETCH_BATCH = 5_000


def _pdb_source_available(dataset):
    """이 데이터셋에서 prompt DB 소스를 제시해도 되는가 (스키마만 본다 — 쿼리 없음)."""
    try:
        schema = dataset.get_field_schema()
    except Exception:       # noqa: BLE001 — 드롭다운 구성 실패로 폼이 죽으면 안 된다
        return False
    return "gidx" in schema and "bank_version" in schema


def _pdb_embeddings(target):
    """뷰의 샘플 순서대로 (N, 1024) 배열. `compute_visualization(points=…)` 규약과 동일.

    ⚠️ 반환 배열은 `target.values("id")` 순서다 — `fob.compute_visualization` 이
    embeddings 를 그 순서로 해석한다. 벡터가 없는 샘플이 하나라도 있으면 **조용히 건너뛰지
    않고 거부**한다: 행이 빠지면 좌표↔샘플 대응이 통째로 밀려 "엉뚱한 점" 이 된다.
    """
    n = target.count()
    if n > PDB_MAX_VECTORS:
        raise ValueError(
            f"{n:,}개는 한 번에 너무 많습니다 (상한 {PDB_MAX_VECTORS:,}) — 뷰를 좁히세요. "
            "prompt DB 벡터는 1024-d 라 전량 로드가 App 프로세스를 멈춥니다.")
    gidx, bver = target.values(["gidx", "bank_version.label"])
    counts = pdb_version_counts(bver)
    banks = pdb_banks()
    if not banks:
        raise ValueError(f"prompt DB 를 읽을 수 없습니다: {_PDB_BANKS_ERR}")

    out = np.zeros((n, 0), dtype="float32")
    missing, rejected = [], []
    by_ver = {}
    for i, v in enumerate(bver):
        if v is not None and gidx[i] is not None:
            by_ver.setdefault(str(v), []).append(i)
    for version, idxs in by_ver.items():
        bank = banks.get(pdb_norm_ver(version))
        # 게이트 ①② — 문장 해석과 **같은 규약**. 행수가 다르면 gidx 정렬을 못 믿는다.
        if bank is None or bank[3] == 0:
            rejected.append(f"{version}(DB 문장 미보유)")
            continue
        if int(counts.get(version, 0)) != bank[3]:
            rejected.append(f"{version}(행수 DB {bank[3]:,}≠뷰 {counts.get(version, 0):,})")
            continue
        locs = [pdb_local_gidx(gidx[i]) for i in idxs]
        got = {}
        for chunk in _batches(locs, PDB_FETCH_BATCH):
            got.update(pdb_fetch_vectors(version, chunk))
        for i, g in zip(idxs, locs):
            vec = got.get(g)
            if vec is None:
                missing.append(i)
                continue
            if out.shape[1] == 0:
                out = np.zeros((n, len(vec)), dtype="float32")
            out[i] = vec
    holes = sorted(set(missing) | (set(range(n)) - {i for v in by_ver.values() for i in v}))
    if rejected or holes:
        raise ValueError(
            f"prompt DB 벡터가 {len(holes):,}/{n:,}행에서 비었습니다 — 좌표가 밀리므로 "
            f"계산하지 않습니다. 거부된 뱅크: {', '.join(rejected[:5]) or '없음'}"
            f"{f' 외 {len(rejected) - 5}' if len(rejected) > 5 else ''}. "
            "그 버전을 빼고 뷰를 좁히세요 (사이드바 bank_version 필터).")
    return out


class ComputeVisualization(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="compute_visualization",
            label="Compute visualization (OSS)",
            dynamic=True,
        )

    def resolve_placement(self, ctx):
        # ⚠️ EMBEDDINGS_ACTIONS 가 아니라 그리드 툴바에 둔다. 시각화 brain key 가
        # 0개인 데이터셋(예: 갓 빌드한 frames_full)에서 Embeddings 패널은 툴바 없이
        # Enterprise CTA 만 렌더한다 → EMBEDDINGS_ACTIONS 버튼이 **정작 필요한 순간에
        # 닿지 않는다.** 그리드 툴바는 항상 보이므로 두 상태 모두에서 사용 가능.
        return types.Placement(
            types.Places.SAMPLES_GRID_ACTIONS,
            types.Button(
                label="Compute visualization (OSS)", icon="add_chart", prompt=True
            ),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        fields = _vector_fields(ctx.dataset)
        # `<X>-prompts` 는 벡터를 샘플에 저장하지 않는다 — 정본은 Postgres 다 (위 주석).
        sources = list(fields)
        if _pdb_source_available(ctx.dataset):
            sources.append(PDB_EMBED_CHOICE)

        if not sources:
            inputs.view(
                "none",
                types.Error(label="임베딩 필드가 없습니다 (숫자 ListField/VectorField)"),
            )
            return types.Property(inputs)

        inputs.str(
            "brain_key",
            required=True,
            label="Brain key",
            description="Embeddings 패널 왼쪽 드롭다운에 나타날 이름",
        )

        # ponytail: 임베딩 필드가 하나면 고를 게 없다 — 기본값으로 박고 폼에서 감춘다.
        embeddings_choices = types.DropdownView()
        for name in sources:
            if name == PDB_EMBED_CHOICE:
                embeddings_choices.add_choice(
                    name, label=PDB_EMBED_LABEL,
                    description="bank_sentences ⨝ image_embeddings(entity_type='prompt') "
                                "— (bank_version, gidx) 조인, npz 경유 아님")
            else:
                embeddings_choices.add_choice(name, label=name)
        inputs.enum(
            "embeddings",
            sources,
            # 저장된 벡터 필드가 없는 데이터셋에서는 DB 소스가 유일한 선택지가 된다.
            default=sources[0],
            required=True,
            label="Embeddings",
            description="이미 계산된 임베딩 필드 또는 prompt DB",
            view=embeddings_choices,
        )
        if ctx.params.get("embeddings") == PDB_EMBED_CHOICE:
            inputs.view(
                "pdb",
                types.Notice(label=(
                    f"prompt DB 는 1024-d 를 파이썬으로 끌어옵니다 — 상한 "
                    f"{PDB_MAX_VECTORS:,}행. 뱅크 버전 하나로 뷰를 좁혀 쓰세요 "
                    "(행수/클래스 게이트에 걸리는 버전이 섞이면 계산을 거부합니다)")),
            )

        method_choices = types.DropdownView()
        for value, label, desc in METHODS:
            method_choices.add_choice(value, label=label, description=desc)
        inputs.enum(
            "method",
            [m[0] for m in METHODS],
            default="umap",
            required=True,
            label="Method",
            view=method_choices,
        )

        target_choices = types.RadioGroup()
        target_choices.add_choice("DATASET", label="전체 데이터셋")
        target_choices.add_choice("CURRENT_VIEW", label="현재 뷰(필터 적용분)")
        inputs.enum(
            "target",
            target_choices.values(),
            default="DATASET",
            required=True,
            label="대상",
            view=target_choices,
        )

        brain_key = ctx.params.get("brain_key")
        if brain_key and brain_key in ctx.dataset.list_brain_runs():
            inputs.view(
                "dup",
                types.Warning(label=f"'{brain_key}' 는 이미 있습니다 — 덮어씁니다"),
            )

        return types.Property(
            inputs, view=types.View(label="Compute visualization (OSS)")
        )

    def execute(self, ctx):
        brain_key = ctx.params["brain_key"]
        embeddings = ctx.params["embeddings"]
        method = ctx.params.get("method", "umap")

        target = ctx.dataset
        if ctx.params.get("target") == "CURRENT_VIEW" and ctx.view is not None:
            target = ctx.view

        if brain_key in ctx.dataset.list_brain_runs():
            ctx.dataset.delete_brain_run(brain_key)

        n = target.count()
        source = "필드"
        if embeddings == PDB_EMBED_CHOICE:
            # DB 벡터는 필드가 아니라 배열로 넘긴다. `_big_projection` 은 필드명을 받아
            # 배치로 다시 읽는 구조라 이 경로에는 쓰지 않는다 — 대신 PDB_MAX_VECTORS 가
            # 상한을 지키므로 통짜 fit 이 성립한다.
            embeddings = _pdb_embeddings(target)
            source = PDB_EMBED_LABEL
            with _thread_cap():
                fob.compute_visualization(
                    target, embeddings=embeddings, method=method,
                    brain_key=brain_key, num_dims=2,
                )
            return {"brain_key": brain_key, "count": n, "method": method,
                    "source": source}

        with _thread_cap():
            if n <= FIT_MAX:
                fob.compute_visualization(
                    target,
                    embeddings=embeddings,
                    method=method,
                    brain_key=brain_key,
                    num_dims=2,
                )
            else:
                points = _big_projection(target, embeddings, method, n)
                fob.compute_visualization(target, points=points, brain_key=brain_key)

        return {"brain_key": brain_key, "count": n, "method": method, "source": source}

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("brain_key", label="생성된 brain key")
        outputs.str("method", label="method")
        outputs.int("count", label="샘플 수")
        outputs.str("source", label="임베딩 출처")
        outputs.view(
            "hint", types.Notice(label="F5 로 새로고침한 뒤 왼쪽 드롭다운에서 선택하세요")
        )
        return types.Property(outputs, view=types.View(label="완료"))


def _big_projection(target, field, method, n):
    """FIT_MAX 초과 데이터셋용 배치 투영.

    `points=` 는 samples 기본 순서에 정렬돼야 하므로(sample_ids 인자 없음)
    `values("id")` 순서로 배치를 만들어 같은 순서로 채운다.
    """
    ids = target.values("id")
    dataset = target if isinstance(target, fo.Dataset) else target._dataset

    if method == "tsne":
        # sklearn TSNE 는 out-of-sample transform 이 없어 전량 fit 뿐인데
        # 188K 는 메모리·시간 모두 불가. 조용히 다른 결과를 내지 말고 거부한다.
        raise ValueError(
            f"t-SNE 는 {FIT_MAX:,}개 초과에서 지원하지 않습니다 (out-of-sample 변환 불가). "
            f"현재 {n:,}개 — UMAP/PCA 를 쓰거나 뷰를 좁혀 주세요."
        )

    pts = np.empty((n, 2), dtype="float32")

    if method == "pca":
        from sklearn.decomposition import IncrementalPCA

        ipca = IncrementalPCA(n_components=2)
        for batch in _batches(ids, TBATCH):
            X = _embeddings_of(dataset, batch, field)
            if len(X) >= 2:
                ipca.partial_fit(X)
            del X
            gc.collect()
        off = 0
        for batch in _batches(ids, TBATCH):
            X = _embeddings_of(dataset, batch, field)
            pts[off : off + len(batch)] = ipca.transform(X)
            off += len(batch)
            del X
            gc.collect()
        return pts

    import umap

    reducer = umap.UMAP(n_components=2, metric="cosine", low_memory=True, verbose=False)
    random.seed(42)
    fit_ids = [ids[i] for i in sorted(random.sample(range(n), min(FIT_MAX, n)))]
    Xf = _embeddings_of(dataset, fit_ids, field)
    reducer.fit(Xf)
    del Xf, fit_ids
    gc.collect()
    off = 0
    for batch in _batches(ids, TBATCH):
        X = _embeddings_of(dataset, batch, field)
        pts[off : off + len(batch)] = reducer.transform(X)
        off += len(batch)
        del X
        gc.collect()
    return pts


def _scalar_fields(dataset):
    """Color by 로 쓸 만한 스칼라 필드만. id/filepath 류 고유값은 색칠해도 의미 없다."""
    skip = {"id", "filepath", "minio_key", "image_id", "entity_id", "asset_id", "caption"}
    types_ok = (fo.StringField, fo.BooleanField, fo.IntField)
    return [
        name
        for name, field in dataset.get_field_schema().items()
        if isinstance(field, types_ok) and name not in skip
    ]


class CombineColorFields(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="combine_color_fields",
            label="Color by 2 fields",
            dynamic=True,
        )

    def resolve_placement(self, ctx):
        return types.Placement(
            types.Places.EMBEDDINGS_ACTIONS,
            types.Button(label="Color by 2 fields", icon="palette", prompt=True),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        choices = _scalar_fields(ctx.dataset)

        for key, label in (("field1", "First field"), ("field2", "Second field")):
            dropdown = types.DropdownView()
            for name in choices:
                dropdown.add_choice(name, label=name)
            inputs.enum(key, choices, required=True, label=label, view=dropdown)

        f1 = ctx.params.get("field1")
        f2 = ctx.params.get("field2")
        if f1 and f2:
            if f1 == f2:
                inputs.view("warn", types.Warning(label="서로 다른 두 필드를 고르세요"))
            else:
                inputs.view(
                    "preview",
                    types.Notice(label=f"생성될 필드: {_combo_name(f1, f2)}"),
                )

        return types.Property(inputs, view=types.View(label="Color by 2 fields"))

    def execute(self, ctx):
        f1 = ctx.params["field1"]
        f2 = ctx.params["field2"]
        if f1 == f2:
            raise ValueError("서로 다른 두 필드를 골라야 합니다")

        # ponytail: 뷰가 아니라 항상 데이터셋 전체에 쓴다. 필터된 뷰에만 쓰면
        # 나머지 샘플이 None 이 돼서 Color by 범례에 'none' 덩어리가 생긴다.
        dataset = ctx.dataset
        target = _combo_name(f1, f2)
        ids = dataset.values("id")
        v1 = dataset.values(f1)
        v2 = dataset.values(f2)
        items = [
            (sid, _fmt(a) + COMBO_SEPARATOR + _fmt(b))
            for sid, a, b in zip(ids, v1, v2)
        ]
        del v1, v2
        gc.collect()
        _set_values_batched(dataset, target, items)

        return {"field": target, "count": len(items)}

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("field", label="생성된 필드")
        outputs.int("count", label="적용된 샘플 수")
        outputs.view(
            "hint",
            types.Notice(label="F5 로 새로고침한 뒤 Color by 에서 선택하세요"),
        )
        return types.Property(outputs, view=types.View(label="완료"))


def _combo_name(f1, f2):
    return f"{f1}__x__{f2}"


def _fmt(value):
    return "none" if value is None else str(value)


def _visualization_keys(dataset):
    """points 를 가진 시각화 brain run 만. similarity 인덱스(text_search)는 제외."""
    keys = []
    for key in dataset.list_brain_runs():
        try:
            cls = dataset.get_brain_info(key).config.cls or ""
        except Exception:  # noqa: BLE001 — 손상된 run 은 조용히 건너뛴다
            continue
        if "visualization" in cls.lower():
            keys.append(key)
    return keys


class SaveVisualizationCoords(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="save_visualization_coords",
            label="좌표를 필드로 저장",
            dynamic=True,
        )

    def resolve_placement(self, ctx):
        return types.Placement(
            types.Places.EMBEDDINGS_ACTIONS,
            types.Button(label="좌표를 필드로 저장", icon="straighten", prompt=True),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        keys = _visualization_keys(ctx.dataset)

        if not keys:
            inputs.view("none", types.Error(label="시각화 brain key 가 없습니다"))
            return types.Property(inputs)

        dropdown = types.DropdownView()
        for key in keys:
            dropdown.add_choice(key, label=key)
        inputs.enum(
            "brain_key",
            keys,
            default=keys[0],
            required=True,
            label="Brain key",
            description="이 시각화의 2D 좌표를 필드로 꺼냅니다",
            view=dropdown,
        )

        brain_key = ctx.params.get("brain_key")
        if brain_key:
            inputs.view(
                "preview",
                types.Notice(
                    label=f"생성될 필드: {brain_key}_x, {brain_key}_y"
                ),
            )

        return types.Property(inputs, view=types.View(label="좌표를 필드로 저장"))

    def execute(self, ctx):
        brain_key = ctx.params["brain_key"]
        results = ctx.dataset.load_brain_results(brain_key)
        if results is None:
            raise ValueError(f"'{brain_key}' 에 결과가 없습니다 (실패한 run)")

        points = results.points
        if points.shape[1] < 2:
            raise ValueError(f"2D 이상이어야 합니다 (num_dims={points.shape[1]})")

        # patches 기반 시각화면 sample_ids 가 없고 label_ids 를 쓴다.
        ids = getattr(results, "sample_ids", None)
        if ids is None:
            raise ValueError("patches 기반 시각화는 지원하지 않습니다")

        # 188K 단일 bulk write 를 피해 배치로 쓴다 (id 키라 순서 의존 없음).
        sids = [str(i) for i in ids]
        _set_values_batched(
            ctx.dataset, f"{brain_key}_x", list(zip(sids, points[:, 0].tolist()))
        )
        _set_values_batched(
            ctx.dataset, f"{brain_key}_y", list(zip(sids, points[:, 1].tolist()))
        )

        return {
            "fields": f"{brain_key}_x, {brain_key}_y",
            "count": len(ids),
        }

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("fields", label="생성된 필드")
        outputs.int("count", label="샘플 수")
        outputs.view(
            "hint",
            types.Notice(
                label="F5 후 사이드바에서 슬라이더 필터로, Color by 에서 그라디언트로 쓸 수 있습니다"
            ),
        )
        return types.Property(outputs, view=types.View(label="완료"))


# ── 미디어 파일 이동/삭제 ────────────────────────────────────────────────────
# App 오퍼레이터는 App 프로세스 안에서 **동기로** 돈다. 20만 장 파일 I/O 를 걸면
# 앱이 그대로 멈춘다 → 상한을 두고 뷰를 좁히게 만든다.
# ponytail: 상한 초과를 나누어 처리하고 싶으면 delegated execution
# (`fiftyone delegated launch` 별도 프로세스) 으로 올릴 것.
MAX_FILE_OPS = 20_000

DIR_PROBE = 200  # 이동 후보 디렉토리를 찾을 때 훑는 샘플 수


def _target_view(ctx):
    if ctx.params.get("target") != "CURRENT_VIEW" and ctx.selected:
        return ctx.dataset.select(ctx.selected)
    return ctx.view if ctx.view is not None else ctx.dataset.view()


def _target_input(ctx, inputs):
    """선택이 있을 때만 '선택 vs 현재 뷰' 를 묻는다 (없으면 현재 뷰 뿐)."""
    n = len(ctx.selected)
    if not n:
        return
    radio = types.RadioGroup()
    radio.add_choice("SELECTED", label=f"선택한 {n}장")
    radio.add_choice("CURRENT_VIEW", label="현재 뷰 전체")
    inputs.enum(
        "target", radio.values(), default="SELECTED", required=True, view=radio
    )


def _media_dirs(view):
    """이동 후보 = 대상 파일이 실제 들어있는 디렉토리 + 그 형제 디렉토리.

    임의 경로 입력을 막는 게 목적이다 — filepath 로 보이는 미디어 트리 안에서만
    옮긴다 (예: `frames/falldown` → `frames/normal` 오분류 정정).
    후보 수집은 앞 DIR_PROBE 장만 훑는다 (dynamic 폼이 매 입력마다 재계산되므로).
    """
    here = {os.path.dirname(p) for p in view.limit(DIR_PROBE).values("filepath")}
    out = set(here)
    for d in here:
        with contextlib.suppress(OSError):
            out.update(e.path for e in os.scandir(os.path.dirname(d)) if e.is_dir())
    return sorted(out), sorted(here)


def _move_files(samples, dst):
    """대상에 같은 이름이 있으면 덮어쓰지 않고 건너뛴다."""
    moved = skipped = 0
    for s in samples:
        new = os.path.join(dst, os.path.basename(s.filepath))
        if new == s.filepath or os.path.exists(new):
            skipped += 1
            continue
        shutil.move(s.filepath, new)
        s.filepath = new  # 안 하면 데이터셋이 깨진 경로를 가리킨다
        moved += 1
    return moved, skipped


def _check_count(view):
    n = len(view)
    if n > MAX_FILE_OPS:
        raise ValueError(
            f"{n:,}장은 한 번에 너무 많습니다 (상한 {MAX_FILE_OPS:,}) — 뷰를 좁히세요"
        )
    return n


class MoveMedia(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="move_media", label="미디어 파일 이동", dynamic=True
        )

    def resolve_placement(self, ctx):
        return types.Placement(
            types.Places.SAMPLES_GRID_SECONDARY_ACTIONS,
            types.Button(
                label="미디어 파일 이동", icon="drive_file_move", prompt=True
            ),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        _target_input(ctx, inputs)
        view = _target_view(ctx)
        choices, here = _media_dirs(view)
        if not choices:
            inputs.view("none", types.Error(label="대상 샘플이 없습니다"))
            return types.Property(inputs)

        dropdown = types.DropdownView()
        for d in choices:
            dropdown.add_choice(d, label=d)
        inputs.enum(
            "dst",
            choices,
            required=True,
            label="대상 디렉토리",
            description="현재 위치: " + ", ".join(here),
            view=dropdown,
        )

        n = len(view)
        if n > MAX_FILE_OPS:
            inputs.view(
                "cap",
                types.Warning(label=f"{n:,}장 — 상한 {MAX_FILE_OPS:,} 초과, 뷰를 좁히세요"),
            )
        else:
            inputs.view(
                "info",
                types.Notice(label=f"{n:,}장 이동 + filepath 갱신 (데이터셋 유지)"),
            )
        return types.Property(inputs, view=types.View(label="미디어 파일 이동"))

    def execute(self, ctx):
        view = _target_view(ctx)
        dst = ctx.params["dst"]
        allowed, _ = _media_dirs(view)
        if dst not in allowed:  # 폼 밖에서 들어온 임의 경로 차단
            raise ValueError(f"허용되지 않은 대상입니다: {dst}")
        _check_count(view)

        moved, skipped = _move_files(view.iter_samples(autosave=True), dst)
        ctx.trigger("reload_samples")
        return {"dst": dst, "moved": moved, "skipped": skipped}

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("dst", label="대상 디렉토리")
        outputs.int("moved", label="이동한 파일")
        outputs.int("skipped", label="건너뜀 (같은 이름 존재)")
        return types.Property(outputs, view=types.View(label="이동 완료"))


def _prune_brain_results(dataset, quiet=False):
    """지워진 표본의 좌표를 brain result 에서 **함께** 잘라낸다.

    `delete_samples` 는 샘플 문서와 `embedding` 필드를 지우지만 시각화 run 에 구워진
    `points`/`sample_ids` 배열은 건드리지 않는다. 그래서 삭제 후에도 좌표가 그대로 남고,
    **FiftyOne 네이티브 Embeddings 패널은 그 배열을 그대로 그린다** — 사용자에게는 "지운
    이미지가 임베딩 패널에 계속 나타난다" 로 보인다 (실측 2026-09-02: `sourcei` 표본
    6,353 인데 `emb_viz` 좌표 7,498, 죽은 점 1,145). 자체 패널 두 개는 로드할 때
    `keep = order >= 0` 로 걸러내므로 증상이 안 보여서 진단이 늦어졌다.

    sourcei 계열은 임베딩 정본이 FiftyOne `embedding` 필드라 pgvector 는 건드리지 않는다
    (실측: sourcei 파일 stem 400개 중 `image_embeddings` 매칭 0건 — 그 테이블의
    `entity_type='frame'` 188,190행은 `frames` 도메인 것이다). 남의 행을 지우지 않는다.
    """
    pruned = []
    for key in dataset.list_brain_runs():
        try:
            res = dataset.load_brain_results(key)
        except Exception:      # noqa: BLE001 — 실패한 run 이 삭제를 막으면 안 된다
            continue
        # ⚠️ `points` 는 **getattr 로** 꺼낸다. brain run 에는 시각화만 있는 게 아니라
        # 유사도 인덱스도 섞여 있고(`SklearnSimilarityIndex`) 그쪽엔 속성이 아예 없어서
        # `res.points` 가 None 이 아니라 AttributeError 를 던진다 (실측 2026-09-02: 전
        # 데이터셋 훑기가 여기서 죽었다). 삭제 오퍼레이터 안에서 터지면 삭제가 실패한다.
        sids = getattr(res, "sample_ids", None)
        pts = getattr(res, "points", None)
        if sids is None or pts is None:
            continue            # 유사도 인덱스 · patches 기반(label_ids) 은 대상 아님
        live = set(dataset.values("id"))
        mask = np.array([str(i) in live for i in sids])
        if mask.all():
            continue
        res.points = pts[mask]
        res.sample_ids = sids[mask]
        res.save()
        pruned.append((key, int((~mask).sum())))
        if not quiet:
            print(f"[prune] {key}: 죽은 점 {int((~mask).sum()):,} 제거 → {int(mask.sum()):,}")
    return pruned


class DeleteMedia(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="delete_media", label="미디어 파일 삭제", dynamic=True
        )

    def resolve_placement(self, ctx):
        return types.Placement(
            types.Places.SAMPLES_GRID_SECONDARY_ACTIONS,
            types.Button(
                label="미디어 파일 삭제", icon="delete_forever", prompt=True
            ),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        _target_input(ctx, inputs)
        n = len(_target_view(ctx))
        inputs.view(
            "warn",
            types.Warning(
                label=f"{n:,}장 — 샘플과 디스크 파일이 함께 영구 삭제됩니다 (복구 불가)"
            ),
        )
        inputs.bool(
            "confirm",
            default=False,
            label="삭제를 확인합니다",
            view=types.CheckboxView(),
        )
        return types.Property(inputs, view=types.View(label="미디어 파일 삭제"))

    def execute(self, ctx):
        if not ctx.params.get("confirm"):
            raise ValueError("확인 체크박스를 켜야 삭제합니다")
        view = _target_view(ctx)
        _check_count(view)

        paths = view.values("filepath")
        ctx.dataset.delete_samples(view)
        removed = 0
        for p in paths:
            try:
                os.remove(p)
                removed += 1
            except OSError:  # 이미 없거나 권한 없음 — 샘플은 이미 지워졌다
                pass

        # 임베딩 좌표도 함께 — 안 하면 지운 이미지가 네이티브 Embeddings 패널에 남는다
        pruned = sum(n for _k, n in _prune_brain_results(ctx.dataset, quiet=True))

        ctx.trigger("clear_selected_samples")
        ctx.trigger("reload_dataset")
        return {"samples": len(paths), "removed": removed, "pruned": pruned}

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.int("samples", label="삭제한 샘플")
        outputs.int("removed", label="삭제한 파일")
        outputs.int("pruned", label="함께 지운 임베딩 좌표")
        return types.Property(outputs, view=types.View(label="삭제 완료"))


# ══════════════════════════════════════════════════════════════════════════════
# 프로젝트 번들 업로드 — analysis-sync(:8010) 의 /upload/* 를 오퍼레이터로 노출
# ══════════════════════════════════════════════════════════════════════════════
# UPLOAD_SPEC.md 의 인제스트(ingest_bundle.py)는 임베딩 전체를 메모리에 올리는 무거운
# 배치라 App 프로세스(좌석 mem_limit 3g) 안에서 돌리면 안 된다. 이 오퍼레이터는 계산을
# 절대 자기 프로세스에서 하지 않고 analysis-sync 컨테이너의 서브프로세스에 위임만 한다
# (sync_api.py 의 POST /sync/{target} 와 같은 위임 패턴 — job_id 를 받아 폴링으로만 본다).
#
# 실측 계약(sync_api.py 직접 대조 확인, 2026-09-03 코드리뷰로 최초 버전의 추측 오류 3건 수정):
#      GET  /upload/bundles → [{"bundle","path","has_gt","ingested"}] — 원소는 항상 dict,
#           키는 "bundle"(이름 아님). GT 유무·인제스트 여부는 서버 필드를 안 믿고 로컬
#           (UPLOAD_ROOT 마운트 + fo.load_dataset 의 upload_kit marker)에서 직접 판정한다.
#      POST /upload/validate · /upload/ingest 바디 = resolve_input 파라미터 그대로
#           {"bundle","name","overwrite","skip_viz","attach"}.
#           validate 응답 = validate_bundle.validate() 그대로(ok/errors/warnings/mode/counts/versions).
#           ingest 202 = {"job_id": "upload-<n>"} — "upload:<bundle>" 은 job_id 가 아니라
#           job 레코드의 **target** 필드 값이다(job_id 와 target 을 혼동하지 말 것).
#           409 = {"error":"busy","current":{...}}.
#      GET  /status 의 job 레코드는 report_path 를 **최상위**에 붙인다(result 안이 아님) —
#           ingest_bundle.py 의 마지막 stdout 줄이 JSON 이 아니라 result 파싱은 보통 None.

SYNC_API_DEFAULT = "http://analysis-sync:8010"
UPLOAD_DOCS_HINT = (
    "브라우저 업로드 페이지 = 이 FiftyOne 주소 뒤에 /__upload/ui "
    "(예: http://10.0.0.10:5153/__upload/ui — zip 을 끌어다 놓으면 해제·검증·임포트까지). "
    "대용량은 네트워크 드라이브 \\\\10.0.0.10\\user\\work_p\\Datapipeline-Data-data_pipeline\\docker\\data\\fiftyone\\uploads\\<이름> "
    "에 번들 폴더를 복사(user 계정, 호스트 경로 docker/data/fiftyone/uploads/) — 목록에 바로 뜹니다 (UPLOAD_SPEC.md §1 ②)"
)
UPLOAD_INGEST_LOG_FILE = "ingest.log"  # sync_api.UPLOAD_INGEST_LOG_FILE 리터럴 미러
UPLOAD_STATUS_LOG_TAIL_LINES = 20  # sync_api.TAIL_LINES 리터럴 미러 — 파일 폴백도 같은 분량만
# 서버 UPLOAD_VALIDATE_TIMEOUT_S(sync_api.py, 기본 180s)보다 클라이언트 HTTP 타임아웃이
# 짧으면, 정상 진행 중인 validate 를 클라이언트가 먼저 포기해 "연결 실패"로 오표시하면서도
# 서버는 계속 돈다(codex 리뷰 실증). 같은 env 를 읽어 항상 서버보다 여유 있게 유지한다.
# try/except 필수 — 이 대입은 모듈 최상위(import 시점)에서 평가된다. env 가 "180s"/빈 문자열
# 등 정수로 못 바꾸는 값이면 ValueError 가 이 파일 **전체**의 import 를 실패시켜, 이 오퍼레이터와
# 무관한 기존 5개 오퍼레이터(compute_visualization 등)까지 App 에서 통째로 사라진다
# (FIFTYONE_PLUGINS_CACHE_ENABLED=true 라 재시작 전까지 복구도 안 됨 — minor 리뷰 지적).
try:
    _VALIDATE_HTTP_TIMEOUT = int(os.environ.get("UPLOAD_VALIDATE_TIMEOUT_S", "180")) + 20
except ValueError:
    _VALIDATE_HTTP_TIMEOUT = 200

# 모달 안에서 직접 올릴 수 있는 zip 상한. FiftyOne 파일 입력은 base64 로 좌석 프로세스를 통과하므로
# 파일 크기의 ≈3.7배가 순간 점유된다 (요청 바디 1.34배 + json 파싱본 1.34배 + 디코드 bytes 1배).
# 512MB → ≈1.9GiB 순간 점유. 좌석 mem_limit 6g, 유휴 FiftyOne 이 0.6GiB(2026-09-03 실측)이므로
# sourcei 급 패널(≈3.4GiB)을 띄운 좌석에서도 아슬하게 버틴다. **1GB 로 올리면 ≈3.7GiB 라 그 조합에서
# OOM 이다** — 그보다 큰 번들은 좌석을 아예 거치지 않는 /__upload/ui(스트리밍, UPLOAD_MAX_BYTES 기본
# 20GB)로 보낸다. 좌석 메모리를 더 주는 것은 답이 아니다(호스트 62.5GB 공유, oom_kill 이력).
# nginx 좌석 경로 client_max_body_size 는 이 값 × 1.34 이상이어야 한다(현재 1g).
try:
    _MODAL_UPLOAD_MAX_MB = int(os.environ.get("APP_MODAL_UPLOAD_MAX_MB", "512"))
except ValueError:
    _MODAL_UPLOAD_MAX_MB = 512
_UPLOAD_HTTP_TIMEOUT = 1800  # 해제(수만 파일)가 같은 요청 안에서 끝난다


def _sync_api_url():
    return os.environ.get("FIFTYONE_SYNC_API_URL", SYNC_API_DEFAULT).rstrip("/")


def _sync_headers():
    token = os.environ.get("FIFTYONE_SYNC_TOKEN", "").strip()
    return {"X-Internal-Token": token} if token else {}


def _bundle_common():
    """UPLOAD_SPEC.md 정본 상수/로더 지연 임포트. 실패하면 None(호출부가 기본값으로 대체).

    `/workspace/project_upload` 는 이 플러그인과 별개 배포 단위가 아니라 **같은 컨테이너**
    (analysis-fiftyone) 안의 bind mount 라 user-bank-slots 와 같은 방식으로 import 할 수 있다
    (사본 금지 — UPLOAD_SPEC.md §5 "공용 상수/로더는 bundle_common.py 만 사용").
    """
    if "/workspace/project_upload" not in sys.path:
        sys.path.insert(0, "/workspace/project_upload")
    try:
        import bundle_common as bc
        return bc
    except Exception:  # noqa: BLE001 — 못 불러와도 로컬 스캔 폴백은 하드코딩 기본값으로 계속 동작
        return None


def _scan_upload_dirs():
    """analysis-sync 무응답 시 폴백: UPLOAD_ROOT 아래 manifest.json 있는 디렉토리만."""
    bc = _bundle_common()
    root = bc.UPLOAD_ROOT if bc else "/data/fiftyone/uploads"
    manifest = bc.MANIFEST if bc else "manifest.json"
    try:
        return sorted(
            e.name for e in os.scandir(root)
            if e.is_dir() and os.path.isfile(os.path.join(e.path, manifest))
        )
    except OSError:
        return []


def _list_bundle_names():
    """(번들 이름 정렬 목록, source). source ∈ 'api' | 'scan'(API 실패 시 로컬 스캔 폴백)."""
    try:
        resp = requests.get(f"{_sync_api_url()}/upload/bundles", headers=_sync_headers(), timeout=5)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("bundles", data) if isinstance(data, dict) else data
        # 서버(sync_api.upload_bundles) 는 원소마다 {"bundle","path","has_gt","ingested"} dict 를
        # 준다 — 키는 "bundle" (예전에 "name"/"dataset" 로 잘못 가정해 매번 None → 빈 목록이었다).
        names = {
            it if isinstance(it, str) else (it or {}).get("bundle") or (it or {}).get("name") or (it or {}).get("dataset")
            for it in (items or [])
        }
        names.discard(None)
        return sorted(names), "api"
    except Exception:  # noqa: BLE001 — 네트워크/파싱 실패 전부 로컬 스캔 폴백 대상
        return _scan_upload_dirs(), "scan"


def _bundle_has_gt(name):
    """gt.csv 존재로 GT 유무 추정. gt_mode=folders(이미지 폴더명이 GT)는 놓칠 수 있는
    최선노력 표시일 뿐이다 — 드롭다운마다 재계산되는 dynamic 폼이라 manifest 전체 해석(gt_mode
    분기)은 생략하고 파일 존재만 본다."""
    bc = _bundle_common()
    root = bc.UPLOAD_ROOT if bc else "/data/fiftyone/uploads"
    gt_csv = bc.GT_CSV if bc else "gt.csv"
    return os.path.isfile(os.path.join(root, name, gt_csv))


def _manifest_dataset_name(bundle_name):
    """번들 **디렉토리명** → manifest.json 의 실제 FiftyOne 데이터셋 이름 (읽기 실패 시 디렉토리명 폴백).

    둘은 다를 수 있다 — 예: e2e 세이프티 컨벤션 "_upe2e_*"(DATASET_NAME_RE 가 선행 언더스코어를
    거부해 manifest.dataset 은 "upe2e_*" 로 언더스코어를 뗀다). 이 해석 없이 디렉토리명으로 바로
    fo.dataset_exists 를 부르면, 실제로 이미 인제스트된 번들도 '인제스트됨' 표시가 절대 안 붙는다
    (E2E 실측 확인된 버그)."""
    bc = _bundle_common()
    root = bc.UPLOAD_ROOT if bc else "/data/fiftyone/uploads"
    manifest_file = bc.MANIFEST if bc else "manifest.json"
    try:
        with open(os.path.join(root, bundle_name, manifest_file), encoding="utf-8-sig") as f:
            ds_name = json.load(f).get("dataset")
        if isinstance(ds_name, str) and ds_name:
            return ds_name
    except Exception:  # noqa: BLE001 — 못 읽으면 디렉토리명 그대로 폴백(이전 동작과 동일)
        pass
    return bundle_name


def _dataset_has_upload_marker(dataset_name):
    """FiftyOne 에 이 **리터럴 데이터셋 이름**으로 + upload_kit marker 가 이미 있는가

    (validate_bundle._check_fiftyone_collision 과 같은 판정 — marker 없는 동명 자산은
    ingest_bundle 이 --overwrite 여도 거부하므로 '인제스트됨'이 아니라 '이름 충돌'이다)."""
    try:
        if not fo.dataset_exists(dataset_name):
            return False
        return bool((fo.load_dataset(dataset_name).info or {}).get("upload_kit"))
    except Exception:  # noqa: BLE001 — 판정 실패는 '모른다'가 아니라 '아니다'로 — 과신 라벨 방지
        return False


def _bundle_ingested(bundle_name):
    """번들 **디렉토리명**을 manifest 로 실제 데이터셋 이름으로 해석한 뒤 인제스트 여부 판정."""
    return _dataset_has_upload_marker(_manifest_dataset_name(bundle_name))


def _bundle_label(name):
    tags = [t for t, hit in (("GT ✓", _bundle_has_gt(name)), ("인제스트됨", _bundle_ingested(name))) if hit]
    return f"{name} ({', '.join(tags)})" if tags else name


def _fetch_bundle_url(url, headers, source_url, name, overwrite):
    """URL 반입 — analysis-sync `POST /upload/fetch` 시작 후 `/upload/job` 을 폴링해 결과를 돌려준다.

    반환 shape 는 `_upload_bundle_zip` 과 **동일**(bundle/dataset/files/bytes/validate) — 그래야
    호출부 하류(_guard_name_clash → validate 확인 → _start_ingest)가 그대로 재사용된다.
    실행 중 블로킹 폴링을 하지만, zip 업로드 경로도 이미 같은 요청 안에서 최대 30분을 블로킹한다.
    """
    body = {"url": source_url, "overwrite": overwrite}
    if name:
        body["name"] = name
    try:
        resp = requests.post(f"{url}/upload/fetch", json=body, headers=headers, timeout=(10, 60))
    except requests.RequestException as exc:
        raise ValueError(f"analysis-sync 연결 실패({url}/upload/fetch): {exc}") from exc
    if resp.status_code == 404:
        raise ValueError("analysis-sync 에 /upload/fetch 가 없습니다 — 컨테이너 재시작이 필요할 수 있습니다")
    if resp.status_code != 202:
        try:
            detail = (resp.json() or {}).get("detail") or resp.text[:300]
        except ValueError:
            detail = resp.text[:300]
        raise ValueError(f"URL 반입 거부(HTTP {resp.status_code}): {detail}")
    job_id = (resp.json() or {}).get("job_id", "")
    if not job_id:
        raise ValueError("URL 반입 job_id 를 받지 못했습니다")

    deadline = time.time() + _UPLOAD_HTTP_TIMEOUT
    while time.time() < deadline:
        time.sleep(2)
        try:
            jresp = requests.get(f"{url}/upload/job", params={"job_id": job_id}, headers=headers, timeout=15)
            job = (jresp.json() or {}).get("job") or {}
        except (requests.RequestException, ValueError):
            continue                      # 폴링 한 번 실패는 치명적이지 않다 — 다음 주기에 다시
        state = job.get("state")
        if state == "done":
            result = job.get("result") or {}
            if not result.get("bundle"):
                raise ValueError("URL 반입은 끝났는데 결과에 번들 이름이 없습니다")
            return result
        if state == "failed":
            tail = job.get("tail") or ["사유 없음"]
            raise ValueError(f"URL 반입 실패: {tail[-1]}")
    raise ValueError(
        f"URL 반입이 {_UPLOAD_HTTP_TIMEOUT}s 안에 끝나지 않았습니다 — 업로드 페이지(/__upload/ui)에서 "
        f"job {job_id} 의 진행을 확인하세요(작업은 서버에서 계속 진행됩니다)")


def _guard_name_clash(target_name, overwrite):
    """이름 충돌은 서버 /upload/ingest 가 게이트하지 않는다(단일비행 busy 만) — 그대로 보내면
    job 이 4/9 단계에서 실패해 사용자는 '시작됨' 을 본 뒤 상태 오퍼레이터에서야 실패를 안다.
    여기서 먼저 막아 즉시 안내한다 (ingest_bundle._check_conflict 와 같은 판정)."""
    if overwrite:
        return
    clash = [n for n in (target_name, str(target_name) + "-prompts") if fo.dataset_exists(n)]
    if not clash:
        return
    protected = [n for n in clash if not _dataset_has_upload_marker(n)]
    if protected:
        raise ValueError(
            f"데이터셋 {protected} 은 upload_kit marker 가 없는 기존 자산 — 이 이름으로는 "
            f"인제스트할 수 없습니다(덮어쓰기로도 삭제 불가). '데이터셋 이름' 을 다르게 지정하세요.")
    raise ValueError(
        f"데이터셋 {clash} 이미 존재 — 재생성하려면 '덮어쓰기' 를 켜세요 "
        f"(저장된 뷰/워크스페이스는 함께 삭제됩니다).")


def _upload_bundle_zip(url, headers, arch, name, overwrite):
    """모달에서 고른 zip(base64) → analysis-sync `PUT /upload/archive` → 해제·검증 결과.

    바이트가 좌석 프로세스를 한 번 거치지만(FiftyOne 파일 입력이 base64 params 라 불가피),
    해제·검증·설치는 전부 analysis-sync 가 한다 — /__upload/ui 와 같은 코드 경로다.
    """
    content = arch.get("content") or ""
    if "base64," in content[:128]:      # data:application/zip;base64,... 형태도 허용
        content = content.split("base64,", 1)[1]
    try:
        raw = base64.b64decode(content)
    except (ValueError, binascii.Error) as exc:
        raise ValueError(f"업로드 파일을 읽지 못했습니다(base64 디코드 실패): {exc}") from exc
    if not raw:
        raise ValueError("업로드 파일이 비어 있습니다")

    params = {"overwrite": "true" if overwrite else "false"}
    if name:
        params["name"] = name
    hdrs = dict(headers)
    hdrs["Content-Type"] = "application/zip"
    try:
        resp = requests.put(f"{url}/upload/archive", params=params, data=raw, headers=hdrs,
                            timeout=(10, _UPLOAD_HTTP_TIMEOUT))
    except requests.RequestException as exc:
        raise ValueError(f"analysis-sync 연결 실패({url}/upload/archive): {exc}") from exc
    if resp.status_code >= 400:
        try:
            detail = (resp.json() or {}).get("detail") or resp.text[:300]
        except ValueError:
            detail = resp.text[:300]
        raise ValueError(f"업로드 실패(HTTP {resp.status_code}): {detail}")
    try:
        return resp.json()
    except ValueError as exc:
        raise ValueError(f"/upload/archive 응답이 JSON 이 아닙니다: {resp.text[:300]}") from exc


def _resolve_report_path(job, bundle):
    """job(/status 레코드) 또는 선택한 bundle 이름으로 ingest_report.json 절대경로 역산.

    서버(sync_api._run_upload_ingest_job)는 report_path 를 job 레코드 **최상위**에 붙인다
    (result 안이 아님 — ingest_bundle.py 마지막 stdout 줄은 사람용 텍스트라 result 파싱은
    거의 항상 None). result 안도 하위호환으로 한 번 더 본다.
    """
    bc = _bundle_common()
    root = bc.UPLOAD_ROOT if bc else "/data/fiftyone/uploads"
    artifacts = bc.ARTIFACTS_DIR if bc else "_artifacts"
    job = job or {}
    res = job.get("result") or {}
    for key in ("report_path", "ingest_report_path", "report"):
        v = job.get(key)
        if isinstance(v, str) and v:
            return v
        v = res.get(key)
        if isinstance(v, str) and v:
            return v
    ref = res.get("bundle_dir") or res.get("name") or bundle
    if not ref:
        return None
    base = ref if os.path.isabs(str(ref)) else os.path.join(root, os.path.basename(str(ref)))
    return os.path.join(base, artifacts, "ingest_report.json")


class ImportProjectBundle(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="import_project_bundle", label="프로젝트 번들 임포트", dynamic=True,
        )

    def resolve_placement(self, ctx):
        # ComputeVisualization 과 같은 자리 — 데이터셋 상태와 무관하게 항상 보이는 그리드 툴바
        # (전례: 이 파일의 ComputeVisualization.resolve_placement, 같은 SAMPLES_GRID_ACTIONS).
        return types.Placement(
            types.Places.SAMPLES_GRID_ACTIONS,
            types.Button(label="프로젝트 번들 임포트", icon="cloud_upload", prompt=True),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        names, source = _list_bundle_names()

        # ① 이 창에서 바로 zip 올리기. lite=True 라 파일 내용은 **실행 시에만** 전송된다 —
        #    dynamic=True 폼은 값이 바뀔 때마다 params 를 서버로 왕복시키므로, lite 가 없으면
        #    체크박스 하나 누를 때마다 수백 MB base64 가 다시 올라간다.
        arch = ctx.params.get("archive") or {}
        has_archive = bool(arch.get("name"))
        inputs.define_property(
            "archive", types.UploadedFile(),
            label=f"zip 업로드 (≤{_MODAL_UPLOAD_MAX_MB}MB)",
            description=f"번들 폴더를 zip 으로 묶어 여기 놓으면 반입·검증·임포트가 이 창에서 끝납니다. 더 크면 {UPLOAD_DOCS_HINT}",
            view=types.FileView(
                types=".zip,application/zip",
                max_size=_MODAL_UPLOAD_MAX_MB * 1024 * 1024,
                max_size_error_message=(
                    f"{_MODAL_UPLOAD_MAX_MB}MB 초과 — 이 창은 파일을 base64 로 앱 프로세스에 태우기 때문에 "
                    "상한이 있습니다. 큰 번들은 업로드 페이지(/__upload/ui, 스트리밍 20GB)를 쓰세요"),
                lite=True,
            ),
        )
        if has_archive:
            nbytes = arch.get("size") or 0
            size = f"{nbytes / 1048576:.1f}MB" if nbytes >= 1048576 else f"{nbytes / 1024:.0f}KB"
            inputs.view("arch_note", types.Notice(
                label=f"업로드 예정: {arch.get('name')} ({size}) — 실행하면 서버 반입 후 검증·임포트까지 이어집니다"))

        # ①-B 또는 URL 로 가져오기 — 바이트가 좌석을 안 거치므로 크기 제한이 없다(서버가 직접 받는다).
        #     사내 주소만 허용된다(analysis-sync 의 CIDR allowlist).
        source_url = (ctx.params.get("source_url") or "").strip()
        inputs.str(
            "source_url", label="또는 zip URL 로 가져오기",
            description="사내 http/https 직링크(파일서버·MinIO presigned). 서버가 직접 내려받아 반입합니다 — 크기 제한 없음",
        )
        if has_archive and source_url:
            inputs.view("multi_src", types.Warning(
                label="zip 업로드와 URL 이 둘 다 채워졌습니다 — zip 업로드가 우선 적용됩니다"))

        # ② 또는 이미 서버에 있는 번들 선택 (docker cp / 이전 업로드분)
        if names:
            dropdown = types.DropdownView()
            for n in names:
                dropdown.add_choice(n, label=_bundle_label(n))
            inputs.enum(
                "bundle", names, required=not has_archive and not source_url,
                label="또는 서버에 있는 번들" if (has_archive or source_url) else "번들",
                description="/data/fiftyone/uploads/<이름> (manifest.json 보유)",
                view=dropdown,
            )
            if source == "scan":
                inputs.view("scan_note", types.Warning(
                    label="analysis-sync API 무응답 — 로컬 디렉토리 스캔 결과입니다"))
        elif not has_archive:
            lead = "서버에 번들이 없습니다" if source == "api" else "analysis-sync 응답 실패 (로컬 스캔도 0건)"
            inputs.view("none", types.Notice(label=f"{lead} — 위에 zip 을 올리면 됩니다"))

        inputs.str("name", label="데이터셋 이름 (선택)",
                   description="비우면 manifest.json 의 dataset 사용")
        inputs.bool("overwrite", default=False, label="덮어쓰기",
                    description="upload_kit marker 있는 기존 쌍만 재생성 가능")
        inputs.bool("skip_viz", default=False, label="emb_viz(UMAP) 생략",
                    description="compare 워크스페이스도 저장하지 않음 — 데이터셋이 Samples 단독으로 열림")
        inputs.str("attach", label="attach 버전 (선택)",
                   description="cos_best_<class>/attached_bank 계산 기준 버전")

        # "name" 오버라이드가 있으면 그 값 자체가 최종 FiftyOne 데이터셋 이름(ingest_bundle.py
        # --name 과 동일 의미) 이라 manifest 조회가 필요 없다. 없으면 "bundle"은 디렉토리명이라
        # manifest.json 의 dataset 필드로 실제 이름을 해석해야 한다 — 디렉토리명을 그대로 쓰면
        # (예: e2e "_upe2e_*" 처럼 디렉토리명과 manifest.dataset 이 다른 경우) 이미 인제스트된
        # 번들도 이 경고가 절대 뜨지 않는다 (E2E 실측 확인된 버그, 이전 코드는 여기서
        # _bundle_ingested(디렉토리명) 을 직접 호출했다).
        name_override = (ctx.params.get("name") or "").strip()
        # zip 업로드 경로에서는 아직 서버에 번들이 없어 manifest 를 못 읽는다 — 이름을 직접 적었을
        # 때만 미리 경고할 수 있고, 나머지는 실행 중 업로드 직후 같은 판정(_guard_name_clash)을 탄다.
        bundle_sel = None if (has_archive or source_url) else ctx.params.get("bundle")
        target = name_override or (_manifest_dataset_name(bundle_sel) if bundle_sel else None)
        if target and _dataset_has_upload_marker(target):
            inputs.view("dup", types.Warning(
                label=f"'{target}' 은 이미 인제스트됨 — 덮어쓰기를 켜야 재생성됩니다"))

        return types.Property(inputs, view=types.View(label="프로젝트 번들 임포트"))

    def execute(self, ctx):
        url, headers = _sync_api_url(), _sync_headers()
        name_override = (ctx.params.get("name") or "").strip() or None
        overwrite = bool(ctx.params.get("overwrite", False))
        arch = ctx.params.get("archive") or {}
        uploaded = None

        source_url = (ctx.params.get("source_url") or "").strip()
        if arch.get("content"):
            # 이 창에 놓은 zip → 서버 반입(해제·검증까지 analysis-sync 가 한다). 여기서 이미
            # validate 결과를 받으므로 아래 /upload/validate 는 건너뛴다 (검증기 중복 실행 방지).
            uploaded = _upload_bundle_zip(url, headers, arch, name_override, overwrite)
            bundle = uploaded["bundle"]
        elif source_url:
            # URL 반입 — 서버가 직접 내려받아 같은 설치·검증 코드를 탄다. 반환 shape 가 위와
            # 동일해서 하류(_guard_name_clash 이후)는 한 줄도 바뀌지 않는다.
            uploaded = _fetch_bundle_url(url, headers, source_url, name_override, overwrite)
            bundle = uploaded["bundle"]
        else:
            bundle = ctx.params.get("bundle")
            if not bundle:
                raise ValueError("zip 을 올리거나 URL 을 넣거나 서버에 있는 번들을 선택하세요")

        body = {
            "bundle": bundle,
            "name": name_override,
            "overwrite": overwrite,
            "skip_viz": bool(ctx.params.get("skip_viz", False)),
            "attach": ctx.params.get("attach") or None,
        }
        # 실제 데이터셋 이름은 --name 아니면 manifest.json 의 dataset — 번들 디렉토리명이 아니다
        # (2f 실패 케이스: 디렉토리 '_upe2e_op' vs 데이터셋 'upe2e_op' 가 갈려 충돌을 못 봤다).
        target_name = body["name"] or _manifest_dataset_name(bundle)
        _guard_name_clash(target_name, overwrite)

        if uploaded is not None:
            vresult = uploaded.get("validate") or {}
            if not vresult.get("ok", False):
                errs = vresult.get("errors") or ["validate 결과 없음"]
                raise ValueError(
                    f"업로드는 됐지만(번들 '{bundle}' 서버에 남아 있음) 검증 실패:\n"
                    + "\n".join(f"- {e}" for e in errs))
            warn_msgs = vresult.get("warnings") or []
            return self._start_ingest(url, headers, body, target_name, warn_msgs)

        try:
            vresp = requests.post(f"{url}/upload/validate", json=body, headers=headers, timeout=_VALIDATE_HTTP_TIMEOUT)
        except requests.RequestException as e:
            raise ValueError(f"analysis-sync 연결 실패({url}/upload/validate): {e}") from e
        if vresp.status_code == 404:
            raise ValueError("analysis-sync 에 /upload/validate 가 아직 없습니다 (서버 구현 대기 중)")
        try:
            vresult = vresp.json()
        except ValueError as e:
            raise ValueError(
                f"/upload/validate 응답이 JSON 이 아닙니다(HTTP {vresp.status_code}): {vresp.text[:300]}"
            ) from e
        if vresp.status_code >= 400 or not vresult.get("ok", False):
            # validate_bundle.py 가 준 errors 를 우선, 없으면(=sync_api 자체가 에러 낸 경우)
            # FastAPI HTTPException 본문의 "detail"(400 경로 탈출/401 토큰/502 파싱실패 등,
            # 전부 한국어 사유가 실려온다)을 본다 — 이전엔 둘 다 없을 때만 쓸 "HTTP {code}"
            # 문자열을 항상 써서 서버가 알려준 실제 원인이 매번 버려졌다 (major 리뷰 지적).
            detail = vresult.get("detail")
            errs = vresult.get("errors") or (
                [detail] if isinstance(detail, str) else [f"HTTP {vresp.status_code}: {vresp.text[:300]}"]
            )
            raise ValueError("검증 실패:\n" + "\n".join(f"- {e}" for e in errs))
        return self._start_ingest(url, headers, body, target_name, vresult.get("warnings") or [])

    def _start_ingest(self, url, headers, body, target_name, warn_msgs):
        try:
            iresp = requests.post(f"{url}/upload/ingest", json=body, headers=headers, timeout=30)
        except requests.RequestException as e:
            raise ValueError(f"analysis-sync 연결 실패({url}/upload/ingest): {e}") from e

        if iresp.status_code == 409:
            try:
                detail = iresp.json()
            except ValueError:
                detail = iresp.text[:300]
            return {
                "started": False, "busy": True, "job_id": "", "target_name": target_name,
                "warnings": "\n".join(f"- {w}" for w in warn_msgs),
                "hint": f"analysis-sync 가 다른 작업 중입니다(409) — 잠시 후 재시도: {detail}",
            }
        if iresp.status_code not in (200, 202):
            raise ValueError(f"/upload/ingest 실패(HTTP {iresp.status_code}): {iresp.text[:300]}")
        try:
            job_id = (iresp.json() or {}).get("job_id", "")
        except ValueError:
            job_id = ""

        return {
            "started": True, "busy": False, "job_id": job_id, "target_name": target_name,
            "warnings": "\n".join(f"- {w}" for w in warn_msgs),
            "hint": (f"상태는 '프로젝트 임포트 상태' 오퍼레이터로 확인하세요. 완료 후 헤더 "
                     f"선택기에서 '{target_name}' 선택(새로고침 필요할 수 있음)."),
        }

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.bool("started", label="시작됨")
        outputs.bool("busy", label="busy(409)")
        outputs.str("job_id", label="job_id")
        outputs.str("target_name", label="데이터셋 이름(예정)")
        outputs.str("warnings", label="검증 경고")
        outputs.str("hint", label="안내")
        return types.Property(outputs, view=types.View(label="프로젝트 번들 임포트"))


class ImportBundleStatus(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="import_bundle_status", label="프로젝트 임포트 상태", dynamic=True,
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        inputs.str(
            "job_id", label="job_id (선택)",
            description="비우면 analysis-sync 의 current/last 중 target 이 'upload:' 로 "
                        "시작하는 최근 작업을 찾습니다",
        )
        names, _source = _list_bundle_names()
        if names:
            dropdown = types.DropdownView()
            for n in names:
                dropdown.add_choice(n, label=_bundle_label(n))
            inputs.enum(
                "bundle", names, label="번들 (선택 — 완료된 리포트 직접 읽기)",
                description="job 이력이 없어도 _artifacts/ingest_report.json 을 마운트에서 바로 읽습니다",
                view=dropdown,
            )
        return types.Property(inputs, view=types.View(label="프로젝트 임포트 상태"))

    def execute(self, ctx):
        job_id = (ctx.params.get("job_id") or "").strip()
        bundle = ctx.params.get("bundle") or None
        url, headers = _sync_api_url(), _sync_headers()

        try:
            resp = requests.get(
                f"{url}/status", params={"job_id": job_id} if job_id else None,
                headers=headers, timeout=10,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:  # noqa: BLE001 — API 무응답이어도 로컬 리포트 읽기는 계속 시도
            payload = {"_api_error": f"{type(e).__name__}: {e}"}

        job = None
        note = ""
        if job_id:
            job = payload.get("job")
            if job is None and "_api_error" not in payload:
                note = f"job_id {job_id!r} 를 history 에서 찾을 수 없습니다(만료 또는 오타)"
        else:
            # "upload:<bundle>" 은 job_id 가 아니라 job 레코드의 target 필드 값이다
            # (job_id 는 항상 "upload-<n>" — sync_api._dispatch_job). job_id 로 걸러 매칭이
            # 영구히 안 되던 버그였다.
            for cand in (payload.get("current"), payload.get("last")):
                if cand and str(cand.get("target", "")).startswith("upload:"):
                    job = cand
                    break
            if job is None and "_api_error" not in payload:
                note = "최근 upload 작업이 없습니다 (current/last 모두 target='upload:...' 아님)"

        result = {
            "api_error": payload.get("_api_error", ""),
            "note": note,
            "job_id": (job or {}).get("job_id", job_id),
            "state": (job or {}).get("state", ""),
            "returncode": str((job or {}).get("returncode", "")),
            "tail": "\n".join((job or {}).get("tail") or []),
            "report_summary": "",
            "report_path": "",
        }

        report_path = _resolve_report_path(job, bundle)
        if report_path and os.path.isfile(report_path):
            try:
                with open(report_path, encoding="utf-8") as f:
                    report = json.load(f)
                c = report.get("counts", {})
                result["report_summary"] = (
                    f"이미지 {c.get('images')}장(GT {c.get('images_with_gt')}) · "
                    f"문장 {c.get('prompts')}개(버전 {len(c.get('versions') or [])}종) · "
                    f"viz {c.get('image_viz_points')}/{c.get('prompt_viz_points')}"
                )
                result["report_path"] = report_path
            except Exception as e:  # noqa: BLE001 — 리포트가 있어도 파싱 실패가 상태 표시를 막지 않음
                result["report_summary"] = f"리포트 파싱 실패: {e}"

        # returncode -9/137(SIGKILL) 힌트 — OOM 등으로 강제종료되면 job 레코드 자체는 정상
        # 갱신되지만(state=failed) 사용자는 원인을 알 방법이 없었다 (minor 리뷰 지적).
        rc = (job or {}).get("returncode")
        if rc in (-9, 137):
            result["note"] = (
                f"{result['note']} returncode {rc} — 메모리 부족(OOM) 등으로 강제 종료된 것으로 "
                "추정됩니다. 데이터셋이 부분 생성됐을 수 있음 — --overwrite 재실행을 고려하세요."
            ).strip()

        # job 기록이 없을 때(analysis-sync 재시작으로 in-memory history 소실 등) 폴백 — 마운트의
        # _artifacts/ingest.log·ingest_report.json 존재만으로 최선노력 상태를 보여준다. report_path
        # 는 위에서 이미 job=None 이어도(_resolve_report_path 가 bundle 이름만으로) 시도했으므로
        # 여기서는 tail/state 만 채운다 (필수 보강 — 이전엔 job 이력이 없으면 tail/state 가 항상
        # 빈 문자열이라 재시작 여부·부분 실패 여부를 마운트를 열어보지 않고는 알 수 없었다).
        if job is None and bundle:
            bc = _bundle_common()
            root = bc.UPLOAD_ROOT if bc else "/data/fiftyone/uploads"
            artifacts = bc.ARTIFACTS_DIR if bc else "_artifacts"
            log_path = os.path.join(root, bundle, artifacts, UPLOAD_INGEST_LOG_FILE)
            has_log = os.path.isfile(log_path)
            if not result["tail"] and has_log:
                try:
                    with open(log_path, encoding="utf-8", errors="replace") as f:
                        lines = f.readlines()
                    result["tail"] = "".join(lines[-UPLOAD_STATUS_LOG_TAIL_LINES:]).rstrip("\n")
                except OSError as e:  # noqa: BLE001 — 로그 읽기 실패가 상태 표시 전체를 막지 않음
                    result["tail"] = f"ingest.log 읽기 실패: {e}"
            if not result["state"]:
                if result["report_path"]:
                    result["state"] = "done(추정 — job 기록 없음, ingest_report.json 존재)"
                elif has_log:
                    result["state"] = "unknown(추정 — job 기록 없음, ingest.log 만 존재 — 실패/진행중일 수 있음)"
            if has_log or result["report_path"]:
                result["note"] = (
                    f"{result['note']} job 기록 없음(analysis-sync 재시작 등) — "
                    "ingest.log/ingest_report.json 파일 기준 최선노력 표시입니다."
                ).strip()

        return result

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("job_id", label="job_id")
        outputs.str("state", label="state")
        outputs.str("returncode", label="returncode")
        outputs.str("tail", label="최근 로그(tail)")
        outputs.str("report_summary", label="리포트 요약")
        outputs.str("report_path", label="리포트 경로")
        outputs.str("note", label="안내")
        outputs.str("api_error", label="API 오류(있으면)")
        return types.Property(outputs, view=types.View(label="프로젝트 임포트 상태"))


def _bundle_upload_selftest():
    """번들 업로드 오퍼레이터의 순수 로직만 검증 (네트워크·mongo 없이)."""
    import tempfile

    bc = _bundle_common()
    assert bc is not None, "/workspace/project_upload/bundle_common import 실패"

    with tempfile.TemporaryDirectory() as tmp:
        os.makedirs(os.path.join(tmp, "demo"))
        open(os.path.join(tmp, "demo", bc.MANIFEST), "w").close()
        open(os.path.join(tmp, "demo", bc.GT_CSV), "w").close()
        os.makedirs(os.path.join(tmp, "no_manifest"))

        saved_root = bc.UPLOAD_ROOT
        try:
            bc.UPLOAD_ROOT = tmp
            assert _scan_upload_dirs() == ["demo"]
            assert _bundle_has_gt("demo") is True
            assert _bundle_has_gt("no_manifest") is False

            expect = os.path.join(tmp, "demo", bc.ARTIFACTS_DIR, "ingest_report.json")
            assert _resolve_report_path({"result": {"bundle_dir": os.path.join(tmp, "demo")}}, None) == expect
            assert _resolve_report_path(None, "demo") == expect
            assert _resolve_report_path({"result": {}}, None) is None
            # 서버 실제 계약: report_path 는 job 레코드 최상위(result 안 아님) — 회귀 방지
            assert _resolve_report_path({"report_path": "/x/y.json"}, None) == "/x/y.json"
            # 서버 원소 키는 "bundle" — "name"/"dataset" 로 잘못 읽던 회귀 방지
            items = [{"bundle": "demo", "path": "x", "has_gt": True, "ingested": False}]
            names = {it.get("bundle") or it.get("name") or it.get("dataset") for it in items}
            names.discard(None)
            assert names == {"demo"}

            # 번들 디렉토리명 ≠ manifest.dataset 인 경우 실제 이름으로 해석하는지 (E2E 로 확인된
            # 버그: "_upe2e_*" 처럼 선행 언더스코어가 있으면 manifest 쪽은 그걸 뗀 이름을 쓴다).
            os.makedirs(os.path.join(tmp, "_upe2e_demo"))
            with open(os.path.join(tmp, "_upe2e_demo", bc.MANIFEST), "w", encoding="utf-8") as f:
                json.dump({"format_version": 1, "dataset": "upe2e_demo", "embedding_dim": 8}, f)
            assert _manifest_dataset_name("_upe2e_demo") == "upe2e_demo", _manifest_dataset_name("_upe2e_demo")
            assert _manifest_dataset_name("no_manifest") == "no_manifest"  # 못 읽으면 디렉토리명 폴백
            assert _manifest_dataset_name("does_not_exist_at_all") == "does_not_exist_at_all"
        finally:
            bc.UPLOAD_ROOT = saved_root

    print("bundle upload self-check OK")


def register(p):
    p.register(ComputeVisualization)
    p.register(CombineColorFields)
    p.register(SaveVisualizationCoords)
    p.register(MoveMedia)
    p.register(DeleteMedia)
    p.register(ImportProjectBundle)
    p.register(ImportBundleStatus)


def _self_check():
    """파일 이동/후보 디렉토리 로직만 검증 (App·mongo 없이)."""
    import tempfile

    class FakeSample:
        def __init__(self, path):
            self.filepath = path

    class FakeView:
        def __init__(self, paths):
            self._paths = paths

        def limit(self, n):
            return FakeView(self._paths[:n])

        def values(self, _field):
            return self._paths

    with tempfile.TemporaryDirectory() as tmp:
        fall = os.path.join(tmp, "frames", "falldown")
        normal = os.path.join(tmp, "frames", "normal")
        os.makedirs(fall)
        os.makedirs(normal)
        for name in ("a.jpg", "b.jpg"):
            open(os.path.join(fall, name), "w").close()
        open(os.path.join(normal, "b.jpg"), "w").close()  # 이름 충돌 유발

        paths = [os.path.join(fall, n) for n in ("a.jpg", "b.jpg")]
        choices, here = _media_dirs(FakeView(paths))
        assert here == [fall], here
        assert choices == sorted([fall, normal]), choices  # 형제 디렉토리가 후보에 들어온다

        samples = [FakeSample(p) for p in paths]
        assert _move_files(samples, normal) == (1, 1)
        assert samples[0].filepath == os.path.join(normal, "a.jpg")
        assert not os.path.exists(os.path.join(fall, "a.jpg"))
        assert os.path.exists(os.path.join(fall, "b.jpg"))  # 충돌 건은 그대로

    pdb_selftest()          # prompt DB 해석 계층 (DB 없이 도는 순수부)
    _bundle_upload_selftest()   # 번들 업로드 오퍼레이터 순수 로직 (네트워크·mongo 없이)

    # prompt DB 소스 제시 조건 = 조인 키 두 개가 스키마에 있을 때만 (쿼리 없이 판정)
    class _Schema:
        def __init__(self, keys):
            self._k = keys

        def get_field_schema(self):
            return dict.fromkeys(self._k, object())

    assert _pdb_source_available(_Schema(["gidx", "bank_version", "text"]))
    assert not _pdb_source_available(_Schema(["gidx"]))
    assert not _pdb_source_available(_Schema(["filepath"]))

    # 상한 초과는 **조용히 자르지 않고** 거부한다 (좌표↔샘플 대응이 밀리면 최악의 오답)
    class _Big:
        def count(self):
            return PDB_MAX_VECTORS + 1

    try:
        _pdb_embeddings(_Big())
        raise AssertionError("상한 초과인데 통과했다")
    except ValueError as e:
        assert "상한" in str(e), e

    print("self-check OK")


if __name__ == "__main__":
    _self_check()
