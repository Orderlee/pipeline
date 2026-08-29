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

import contextlib
import gc
import os
import random
import shutil
import threading

import numpy as np

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

        ctx.trigger("clear_selected_samples")
        ctx.trigger("reload_dataset")
        return {"samples": len(paths), "removed": removed}

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.int("samples", label="삭제한 샘플")
        outputs.int("removed", label="삭제한 파일")
        return types.Property(outputs, view=types.View(label="삭제 완료"))


def register(p):
    p.register(ComputeVisualization)
    p.register(CombineColorFields)
    p.register(SaveVisualizationCoords)
    p.register(MoveMedia)
    p.register(DeleteMedia)


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
