#!/usr/bin/env python3
"""프롬프트 뱅크 × 프레임 임베딩 코사인 분석 — 전 버전, Postgres 단독.

CSV·npz 를 데이터 저장소로 쓰지 않는다. 입력은 pgvector(`image_embeddings`) +
`prompt_banks`/`bank_sentences`, 출력은 `analysis.*` 테이블. 리포트(HTML/노션)는
그 테이블만 읽어 만든다 — 중간 파일이 진실을 갖는 구간을 없앤 것이 이 파일의 요점이다.

왜 SQL 이 아니라 numpy 인가 (실측):
  · SQL CROSS JOIN 은 102k 페어/s. 전 뱅크 = 188,190 프레임 × 506,247 문장행
    = 95.3G 페어 → 10.8일. 공용 prod DB 를 열흘 점유할 수 없다.
  · 문장 **벡터는 121,614개뿐**이다(뱅크 간 텍스트 공유). 프레임 × 고유문장
    = 22.9G 페어 = 47 TFLOP → sgemm 으로 20분. 뱅크별 점수는 그 위의
    열 인덱스 집합에 대한 max 라 재계산이 아니다.
  → 즉 DB 는 저장소로 쓰고 커널만 BLAS 로 내린다. GPU 는 안 쓴다: SAM3/
    embedding-service 가 상주하는 공용 자원이고 이 작업은 CPU 20분으로 끝난다.

계약 (어기면 조용한 오답이 된다):
  · project = split_part(image_embeddings.source_key, '/', 1)
  · 문장 벡터 조인 = bank_sentences.content_hash → image_embeddings.entity_id
    (entity_type='prompt'). 커버리지 100% 실측 — 미달 뱅크는 skip + 사유 기록,
    조용히 일부만 채점하지 않는다.
  · class_label 은 **뱅크 속성**이다. 같은 문장이 뱅크마다 다른 클래스일 수
    있어(실측 2,106건) 클래스 인덱스는 뱅크별로 따로 만든다.
  · gidx 는 (bank_version, gidx) 쌍으로만 의미가 있다. 뱅크를 걸치는 gidx
    등식 조인 금지.

사용법:
    python3 prompt_cos_db.py plan                  # 규모·비용만, 쓰기 없음
    python3 prompt_cos_db.py score                 # 전 뱅크 채점 → analysis.*
    python3 prompt_cos_db.py score --banks v1.0.8.0,v1.0.8.4
    python3 prompt_cos_db.py report                # → HTML
    python3 prompt_cos_db.py notion                # → 노션 작업 트래커
    python3 prompt_cos_db.py selftest              # DB 없이 도는 불변식 검사
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time

# BLAS 스레드 캡은 numpy import 보다 **앞**이어야 먹는다. 호스트는 16코어이고
# load 가 이미 7 대라 절반만 쓴다 (라벨링·FiftyOne 과 공유).
_THREADS = os.environ.get("COS_THREADS", "6")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ.setdefault(_v, _THREADS)

import numpy as np  # noqa: E402
import psycopg2  # noqa: E402
import psycopg2.extras  # noqa: E402

DSN = os.environ.get("DATAOPS_POSTGRES_DSN",
                     "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")
REPORT_DIR = os.environ.get("COS_REPORT_DIR", "/data/fiftyone/frames_bank/report")
# 프레임 청크: 청크 × 121,614 × 4B 가 점수 행렬이다. 1000 → 486MB.
# 호스트 가용 RAM 이 14GB 대이고 oom_kill 이력이 있어 올리지 않는다.
CHUNK = int(os.environ.get("COS_CHUNK", "1000"))
# 열 블록: fancy indexing 이 gather 사본을 만드므로 이걸로 피크를 묶는다.
COL_BLOCK = int(os.environ.get("COS_COL_BLOCK", "8192"))
# 자리표시자 클래스 — 뱅크에 실재하나 의미가 없다. 채점은 하되 리포트에서 접는다.
PLACEHOLDER_CLASSES = ("class_5", "class_6", "class_7")
# 정수 클래스 → 이름. `prompt_geometry.py` 와 **같은 규약**이어야 두 경로의 결과를
# 나란히 놓을 수 있다. 벡터 전용 뱅크(JSON)는 문자열 라벨이 없고 정수 `class` 만
# 있으므로 이 표로 사상한다. 실측 확인: 1.0.13.0 은 0~3, v0.0.0.0 은 0~4(smoking).
# 표에 없는 정수는 `class_N` 으로 두고 자리표시자 취급한다(리포트에서 접힌다).
CLASS_NAMES = {0: "normal", 1: "falldown", 2: "fire", 3: "smoke", 4: "smoking"}
# 제품 분포-IoU 규칙 상수 — `prompt_geometry.py` 의 WAVE_BINS/WAVE_THR 과 **같은 값**이어야
# 두 경로의 수치를 비교할 수 있다. 바꾸려면 양쪽을 같이 바꿀 것.
WAVE_BINS = int(os.environ.get("WAVE_BINS", "80"))
WAVE_THR = float(os.environ.get("WAVE_THR", "0.15"))
# 제품 APO top-K 다수결의 K — prompt_geometry.RULE_K 와 같은 값이어야 비교가 성립.
RULE_K = int(os.environ.get("RULE_K", "10"))
# 문장 affinity 히스토그램 — 코사인 실측 범위가 0.10~0.35 대라 여유를 둔 고정 구간.
# 구간 밖 관측은 카운트해서 로그로 경고한다 (조용히 절단하지 않는다).
AFF_BINS = int(os.environ.get("COS_AFF_BINS", "40"))
AFF_LO = float(os.environ.get("COS_AFF_LO", "-0.10"))
AFF_HI = float(os.environ.get("COS_AFF_HI", "0.60"))
NOTION_DB = "1de6a557-fb8e-808c-9df3-fccd33c8a6c7"  # 작업들 > 이영우 > … > Data 팀
# 분석 범위에서 제외하는 프로젝트 (사용자 지정 2026-08-21). **프레임 질의에서 자른다** —
# 보고 단계에서만 걸면 계산이 낭비되고(이 3개가 전 프레임의 52퍼센트) 무엇보다 필터를
# 빼먹은 질의가 조용히 전량을 보고한다. 실제로 그 실수를 했다.
EXCLUDE_PROJECTS = tuple(
    x.strip() for x in os.environ.get(
        "COS_EXCLUDE_PROJECTS", "cohort-b,appdata,violence").split(",") if x.strip())


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def connect():
    return psycopg2.connect(DSN)


# ─────────────────────────────── 스키마 ───────────────────────────────

DDL = """
CREATE SCHEMA IF NOT EXISTS analysis;

CREATE TABLE IF NOT EXISTS analysis.bank_run (
  bank_version text PRIMARY KEY,
  status       text NOT NULL,
  n_frames     integer,
  n_sentences  integer,
  n_vectors    integer,
  n_dup_text   integer,
  classes      jsonb,
  seconds      real,
  err          text,
  run_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analysis.bank_project_class_stats (
  bank_version text    NOT NULL,
  project      text    NOT NULL,
  class_label  text    NOT NULL,
  n_frames     integer NOT NULL,
  avg_cos      real    NOT NULL,
  p50_cos      real    NOT NULL,
  p90_cos      real    NOT NULL,
  max_cos      real    NOT NULL,
  n_pred       integer NOT NULL,   -- 이 클래스가 argmax 였던 프레임 수
  avg_margin   real,               -- 예측=이 클래스인 프레임의 (1등-2등) 평균
  PRIMARY KEY (bank_version, project, class_label)
);

CREATE TABLE IF NOT EXISTS analysis.bank_sentence_wins (
  bank_version text    NOT NULL,
  project      text    NOT NULL,
  class_label  text    NOT NULL,
  gidx         integer NOT NULL,
  wins         integer NOT NULL,   -- 이 문장이 예측클래스 승자였던 프레임 수
  avg_cos      real    NOT NULL,
  avg_margin   real    NOT NULL,
  PRIMARY KEY (bank_version, project, gidx)
);
CREATE INDEX IF NOT EXISTS bank_sentence_wins_lookup
  ON analysis.bank_sentence_wins (bank_version, project, wins DESC);

-- 제품 분포-IoU 규칙(wave). argmax 와는 다른 채점기이므로 테이블을 분리한다 —
-- 같은 표에 섞으면 규칙을 명시하지 않은 비교가 반드시 생긴다.
CREATE TABLE IF NOT EXISTS analysis.bank_wave_stats (
  bank_version text    NOT NULL,
  project      text    NOT NULL,
  class_label  text    NOT NULL,
  n_frames     integer NOT NULL,
  avg_iou      real,               -- normal 대비 면적 IoU 평균 (낮을수록 분리됨)
  p10_iou      real,
  p50_iou      real,
  n_fired      integer NOT NULL,   -- iou < WAVE_THR 인 프레임 수 (다중발화 허용)
  n_pred       integer NOT NULL,   -- 단일라벨 축소 후 이 클래스로 판정된 프레임 수
  PRIMARY KEY (bank_version, project, class_label)
);

-- 규칙 간 일치도 — "argmax 와 분포 IoU 가 얼마나 다른 답을 내는가"의 정본.
-- 문장 × 그룹 평균 코사인. **뱅크 독립**이다 — 벡터는 문장만의 함수이므로 한 번 계산하면
-- 모든 뱅크가 재사용한다. group_kind='project' | 'cluster'(무감독 KMeans).
CREATE TABLE IF NOT EXISTS analysis.sentence_affinity (
  content_hash text    NOT NULL,
  group_kind   text    NOT NULL,
  group_key    text    NOT NULL,
  n_frames     integer NOT NULL,
  mean_cos     real    NOT NULL,
  p90_cos      real    NOT NULL,
  max_cos      real    NOT NULL,
  PRIMARY KEY (content_hash, group_kind, group_key)
);
CREATE INDEX IF NOT EXISTS sentence_affinity_grp
  ON analysis.sentence_affinity (group_kind, group_key, mean_cos DESC);

-- 무감독 이미지 클러스터 — GT 없이 프레임 임베딩만으로 만든 잠재 클래스.
-- 구문별 Ridge 계수 — **전 뱅크**. 클래스가 뱅크 속성이므로 뱅크마다 층이 달라지고
-- 계수도 달라진다. 전 버전에 걸쳐 강건한 구문을 찾는 것이 이 테이블의 목적이다.
-- 배치 원장. cron 이 부를 때마다 pending 스텝 하나만 처리한다 — 디스크 99% 환경에서
-- 한 번에 전량을 돌리면 중간에 ENOSPC 로 PG 가 멈춘다(실측: cluster affinity 가 3GB→6GB).
CREATE TABLE IF NOT EXISTS analysis.batch_step (
  step_id  serial PRIMARY KEY,
  kind     text NOT NULL,
  arg      text NOT NULL DEFAULT '',
  status   text NOT NULL DEFAULT 'pending',   -- pending | running | done | failed | skipped
  ord      integer NOT NULL DEFAULT 100,
  run_at   timestamptz,
  seconds  real,
  note     text,
  UNIQUE (kind, arg)
);

CREATE TABLE IF NOT EXISTS analysis.phrase_beta (
  bank_version text    NOT NULL,
  group_kind   text    NOT NULL,
  group_key    text    NOT NULL,
  class_label  text    NOT NULL,
  phrase       text    NOT NULL,
  n_with       integer NOT NULL,
  n_stratum    integer NOT NULL,
  delta        real    NOT NULL,
  beta         real    NOT NULL,
  PRIMARY KEY (bank_version, group_kind, group_key, class_label, phrase)
);
CREATE INDEX IF NOT EXISTS phrase_beta_ph ON analysis.phrase_beta (phrase, group_kind);

CREATE TABLE IF NOT EXISTS analysis.frame_cluster (
  entity_id  text    NOT NULL,
  method     text    NOT NULL,
  cluster_id integer NOT NULL,
  project    text    NOT NULL,
  PRIMARY KEY (entity_id, method)
);
CREATE INDEX IF NOT EXISTS frame_cluster_grp ON analysis.frame_cluster (method, cluster_id);

CREATE TABLE IF NOT EXISTS analysis.bank_rule_agreement (
  bank_version   text    NOT NULL,
  project        text    NOT NULL,
  n_frames       integer NOT NULL,
  n_agree        integer NOT NULL,
  argmax_events  integer NOT NULL,
  wave_events    integer NOT NULL,
  multi_fire     integer NOT NULL,  -- 임계 미만 클래스가 2개 이상 = 축소로 숨는 양
  PRIMARY KEY (bank_version, project)
);

-- top-K(제품 APO 다수결, K=10) 채점 — argmax/wave 와 규칙이 다르므로 표를 분리한다.
CREATE TABLE IF NOT EXISTS analysis.bank_topk_stats (
  bank_version text    NOT NULL,
  project      text    NOT NULL,
  class_label  text    NOT NULL,
  n_frames     integer NOT NULL,
  n_pred       integer NOT NULL,
  PRIMARY KEY (bank_version, project, class_label)
);

-- 3규칙(topk/wave/argmax) 동시 채점의 그룹별 비교 — "어떤 환경에서 어떤 규칙이
-- 갈리는가"의 정본. group_kind='project' | 'cluster'(kmeans64 환경 군집).
CREATE TABLE IF NOT EXISTS analysis.bank_rule_env (
  bank_version  text    NOT NULL,
  group_kind    text    NOT NULL,
  group_key     text    NOT NULL,
  n_frames      integer NOT NULL,
  topk_events   integer NOT NULL,
  wave_events   integer NOT NULL,
  argmax_events integer NOT NULL,
  agree_tw      integer NOT NULL,   -- topk == wave 프레임 수
  agree_ta      integer NOT NULL,   -- topk == argmax
  agree_wa      integer NOT NULL,   -- wave == argmax
  PRIMARY KEY (bank_version, group_kind, group_key)
);
"""


def ensure_schema(cur) -> None:
    cur.execute(DDL)


# ─────────────────────────────── 적재 ───────────────────────────────

def load_banks(cur, only: list[str] | None) -> list[dict]:
    """db_backed 뱅크 목록 + 문장(content_hash, class_label, gidx)."""
    q = """
      SELECT b.version_tag, s.content_hash, s.class_label, MIN(s.gidx) AS gidx,
             COUNT(*) AS n_same_text
      FROM prompt_banks b JOIN bank_sentences s USING(bank_id)
      WHERE b.sentence_storage = 'db_backed'
      GROUP BY 1,2,3
      ORDER BY 1
    """
    cur.execute(q)
    by_ver: dict[str, dict] = {}
    for ver, chash, cls, gidx, n_same in cur:
        if only and ver not in only:
            continue
        b = by_ver.setdefault(ver, {"version": ver, "rows": [], "dup_text": 0})
        b["rows"].append((chash, cls, gidx))
        b["dup_text"] += n_same - 1
    return list(by_ver.values())


def load_sentence_vectors(cur) -> tuple[dict[str, int], np.ndarray]:
    """고유 문장 벡터를 한 번만 올린다. 반환: content_hash→열, L2정규화 행렬.

    ⚠️ **행 순서는 보장되지 않는다** (`ORDER BY` 없음 → Postgres 실행계획 의존).
       2026-08-28 에 문장 2,500개를 등록하자 순서가 바뀌어, 행 인덱스로 저장돼 있던
       `m_s_bg90k.npy`·`Ak_kmeans64.npy` 가 조용히 무효가 됐다(근사 m_s 와 피어슨 0.33).
       → **행 인덱스로 캐시를 저장하지 말고 `content_hash` 로 키를 잡아라.**
          정렬본이 필요하면 `sent_stats_byhash.npz`(rebuild_sent_stats.py) 를 쓴다.
       여기에 ORDER BY 를 넣는 것은 다른 인덱스 기반 캐시(`percls_*.npy`,
       `cluster_specificity_z.npy`)를 한꺼번에 흔들므로 별건으로 판단한다.
    """
    cur.execute("""
      SELECT entity_id, embedding::text FROM image_embeddings
      WHERE entity_type='prompt'
    """)
    ids: list[str] = []
    vecs: list[np.ndarray] = []
    for eid, vtxt in cur:
        ids.append(eid)
        vecs.append(np.fromstring(vtxt.strip("[]"), sep=",", dtype=np.float32))
    M = np.vstack(vecs)
    M /= np.linalg.norm(M, axis=1, keepdims=True)
    return {h: i for i, h in enumerate(ids)}, M


def frame_batches(conn, chunk: int, limit: int | None = None):
    """프레임을 서버사이드 커서로 흘린다 (188,190 × 1024 를 한 번에 안 올린다).

    limit 은 **검증용**이다 — ORDER BY entity_id 라 앞쪽 프로젝트에 편향되므로
    limit 을 걸고 나온 수치를 전량 결과로 읽으면 안 된다.
    """
    with conn.cursor(name="frames_cur") as cur:
        cur.itersize = chunk
        cur.execute("""
          SELECT entity_id, split_part(source_key,'/',1) AS project, embedding::text
          FROM image_embeddings WHERE entity_type='frame' AND source_key IS NOT NULL
            AND split_part(source_key,'/',1) <> ALL(%s)
          ORDER BY entity_id
        """ + (f" LIMIT {int(limit)}" if limit else ""),
            (list(EXCLUDE_PROJECTS) or [""],))
        buf_p: list[str] = []
        buf_v: list[np.ndarray] = []
        for _eid, proj, vtxt in cur:
            buf_p.append(proj)
            buf_v.append(np.fromstring(vtxt.strip("[]"), sep=",", dtype=np.float32))
            if len(buf_p) >= chunk:
                yield buf_p, _norm(np.vstack(buf_v))
                buf_p, buf_v = [], []
        if buf_p:
            yield buf_p, _norm(np.vstack(buf_v))


def _norm(M: np.ndarray) -> np.ndarray:
    M /= np.linalg.norm(M, axis=1, keepdims=True)
    return M


# ─────────────────────────────── 커널 ───────────────────────────────

def max_argmax(S: np.ndarray, cols: np.ndarray, block: int = COL_BLOCK):
    """S 의 cols 열들에 대한 행별 max 와 그 열 인덱스. 블록 처리로 피크 RSS 를 묶는다.

    fancy indexing 은 gather 사본을 만든다 — cols 전체를 한 번에 넘기면
    (행 × 506,247 × 4B) 가 순간 할당돼 호스트를 흔든다. 블록이 그 상한이다.
    """
    n = S.shape[0]
    best = np.full(n, -2.0, dtype=np.float32)
    arg = np.zeros(n, dtype=np.int64)
    rows = np.arange(n)
    for s in range(0, len(cols), block):
        sub = cols[s:s + block]
        blk = S[:, sub]
        j = blk.argmax(axis=1)
        v = blk[rows, j]
        upd = v > best
        best[upd] = v[upd]
        arg[upd] = sub[j[upd]]
    return best, arg


def top2_margin(per_class: np.ndarray) -> np.ndarray:
    """(행, 클래스) 점수에서 1등−2등. 클래스가 1개면 마진이 정의되지 않아 NaN."""
    if per_class.shape[1] < 2:
        return np.full(per_class.shape[0], np.nan, dtype=np.float32)
    part = np.partition(per_class, -2, axis=1)
    return (part[:, -1] - part[:, -2]).astype(np.float32)


# ─────────────────────────────── 스테이지 ───────────────────────────────

def stage_plan(args) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""SELECT COUNT(*) FROM image_embeddings
                       WHERE entity_type='frame' AND source_key IS NOT NULL
                         AND split_part(source_key,'/',1) <> ALL(%s)""",
                    (list(EXCLUDE_PROJECTS) or [""],))
        n_frames = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM image_embeddings WHERE entity_type='prompt'")
        n_vec = cur.fetchone()[0]
        cur.execute("""SELECT COUNT(*) FROM bank_sentences s JOIN prompt_banks b USING(bank_id)
                       WHERE b.sentence_storage='db_backed'""")
        n_rows = cur.fetchone()[0]
        banks = load_banks(cur, _split(args.banks))
    pairs_sql = n_frames * n_rows
    pairs_np = n_frames * n_vec
    log(f"프레임 {n_frames:,} / 고유 문장벡터 {n_vec:,} / 뱅크 문장행 {n_rows:,}"
        f" / 대상 뱅크 {len(banks)}")
    log(f"SQL CROSS JOIN 이면 {pairs_sql/1e9:.1f}G 페어 "
        f"→ {pairs_sql/102_000/86400:.1f}일 (102k 페어/s 실측)")
    log(f"numpy 는 고유문장만 {pairs_np/1e9:.1f}G 페어 = "
        f"{pairs_np*1024*2/1e12:.1f} TFLOP → 수십 분")
    log(f"청크 {CHUNK} → 점수행렬 {CHUNK*n_vec*4/1e6:.0f} MB, "
        f"문장행렬 {n_vec*1024*4/1e6:.0f} MB, BLAS 스레드 {_THREADS}")


def stage_score(args) -> None:
    only = _split(args.banks)
    t0 = time.time()
    with connect() as conn:
        with conn.cursor() as cur:
            ensure_schema(cur)
            conn.commit()
            log("문장 벡터 적재…")
            h2c, SENT = load_sentence_vectors(cur)
            log(f"  고유 문장 {SENT.shape[0]:,} × {SENT.shape[1]}")
            banks = load_banks(cur, only)
            log(f"대상 뱅크 {len(banks)}")

        # 뱅크별 열 인덱스 — 클래스는 뱅크 속성이라 여기서 뱅크별로 만든다.
        prepared, skipped = [], []
        for b in banks:
            missing = [h for h, _c, _g in b["rows"] if h not in h2c]
            if missing:
                skipped.append((b["version"], f"벡터 없는 문장 {len(missing)}건"))
                continue
            cls_cols: dict[str, np.ndarray] = {}
            cls_gidx: dict[str, dict[int, int]] = {}
            for chash, cls, gidx in b["rows"]:
                col = h2c[chash]
                cls_cols.setdefault(cls, []).append(col)
                cls_gidx.setdefault(cls, {})[col] = gidx
            prepared.append({
                "version": b["version"], "dup_text": b["dup_text"],
                "classes": sorted(cls_cols),
                "cols": {c: np.asarray(v, dtype=np.int64) for c, v in cls_cols.items()},
                "col2gidx": cls_gidx,
                "n_sent": len(b["rows"]),
            })
        for ver, why in skipped:
            log(f"  skip {ver}: {why}")

        # 누적기 — 뱅크 × 클래스 × 프레임. 35뱅크 × 5클래스 × 188k × 8B ≈ 263MB.
        acc = {p["version"]: {"cos": [], "gidx": []} for p in prepared}
        projects: list[str] = []
        n_seen = 0
        for pj, F in frame_batches(conn, CHUNK, args.limit):
            S = F @ SENT.T                      # (chunk, 121614) — 여기가 비용 전부
            projects.extend(pj)
            for p in prepared:
                cos_c, gid_c = [], []
                for cls in p["classes"]:
                    best, arg = max_argmax(S, p["cols"][cls])
                    m = p["col2gidx"][cls]
                    cos_c.append(best)
                    gid_c.append(np.fromiter((m[int(a)] for a in arg),
                                             dtype=np.int32, count=len(arg)))
                acc[p["version"]]["cos"].append(np.stack(cos_c, axis=1))
                acc[p["version"]]["gidx"].append(np.stack(gid_c, axis=1))
            n_seen += len(pj)
            del S
            if n_seen % (CHUNK * 20) == 0:
                log(f"  프레임 {n_seen:,} 처리 ({time.time()-t0:.0f}s)")
        log(f"채점 끝 — 프레임 {n_seen:,}, {time.time()-t0:.0f}s. 적재 시작")

        proj_arr = np.asarray(projects)
        for p in prepared:
            _write_bank(conn, p, np.vstack(acc[p["version"]]["cos"]),
                        np.vstack(acc[p["version"]]["gidx"]), proj_arr, n_seen)
        with conn.cursor() as cur:
            for ver, why in skipped:
                cur.execute("""INSERT INTO analysis.bank_run
                    (bank_version,status,err) VALUES (%s,'skipped',%s)
                    ON CONFLICT (bank_version) DO UPDATE SET
                      status='skipped', err=EXCLUDED.err, run_at=now()""", (ver, why))
        conn.commit()
    log(f"완료 — 총 {time.time()-t0:.0f}s")


def _write_bank(conn, p: dict, cos: np.ndarray, gidx: np.ndarray,
                proj: np.ndarray, n_frames: int) -> None:
    """한 뱅크의 집계 2벌을 delete-then-insert. 프레임 단위 원본은 저장하지 않는다
    (188k × 35뱅크 × 5클래스 ≈ 27M행 = 2.4GB, 루트 디스크가 98% 라 못 쓴다)."""
    ver, classes = p["version"], p["classes"]
    pred = cos.argmax(axis=1)
    margin = top2_margin(cos)
    rows_stats, rows_wins = [], []
    for prj in np.unique(proj):
        pm = proj == prj
        for ci, cls in enumerate(classes):
            v = cos[pm, ci]
            is_pred = pm & (pred == ci)
            n_pred = int(is_pred.sum())
            mg = margin[is_pred]
            rows_stats.append((
                ver, str(prj), cls, int(v.size),
                float(v.mean()), float(np.percentile(v, 50)),
                float(np.percentile(v, 90)), float(v.max()),
                n_pred,
                float(np.nanmean(mg)) if n_pred else None,
            ))
            if not n_pred:
                continue
            g = gidx[is_pred, ci]
            c = cos[is_pred, ci]
            for gv in np.unique(g):
                sel = g == gv
                rows_wins.append((ver, str(prj), cls, int(gv), int(sel.sum()),
                                  float(c[sel].mean()), float(np.nanmean(mg[sel]))))
    with conn.cursor() as cur:
        cur.execute("DELETE FROM analysis.bank_project_class_stats WHERE bank_version=%s", (ver,))
        cur.execute("DELETE FROM analysis.bank_sentence_wins WHERE bank_version=%s", (ver,))
        psycopg2.extras.execute_values(cur, """
            INSERT INTO analysis.bank_project_class_stats
            (bank_version,project,class_label,n_frames,avg_cos,p50_cos,p90_cos,
             max_cos,n_pred,avg_margin) VALUES %s""", rows_stats, page_size=500)
        psycopg2.extras.execute_values(cur, """
            INSERT INTO analysis.bank_sentence_wins
            (bank_version,project,class_label,gidx,wins,avg_cos,avg_margin)
            VALUES %s""", rows_wins, page_size=1000)
        cur.execute("""INSERT INTO analysis.bank_run
            (bank_version,status,n_frames,n_sentences,n_vectors,n_dup_text,classes)
            VALUES (%s,'ok',%s,%s,%s,%s,%s)
            ON CONFLICT (bank_version) DO UPDATE SET
              status='ok', n_frames=EXCLUDED.n_frames, n_sentences=EXCLUDED.n_sentences,
              n_vectors=EXCLUDED.n_vectors, n_dup_text=EXCLUDED.n_dup_text,
              classes=EXCLUDED.classes, err=NULL, run_at=now()""",
            (ver, n_frames, p["n_sent"], int(sum(len(v) for v in p["cols"].values())),
             p["dup_text"], json.dumps(classes)))
    conn.commit()
    log(f"  {ver}: stats {len(rows_stats):,} / wins {len(rows_wins):,}")


def _split(s: str | None) -> list[str] | None:
    return [x.strip() for x in s.split(",") if x.strip()] if s else None




def hist_percentile(hist: np.ndarray, q: float) -> np.ndarray:
    """행별 히스토그램에서 q 백분위수의 bin 중심값. hist: [rows, AFF_BINS]."""
    cum = hist.cumsum(axis=1)
    tot = cum[:, -1:].astype(np.float64)
    target = tot * (q / 100.0)
    idx = (cum < target).sum(axis=1)
    idx = np.clip(idx, 0, AFF_BINS - 1)
    w = (AFF_HI - AFF_LO) / AFF_BINS
    return (AFF_LO + (idx + 0.5) * w).astype(np.float32)


def hist_iou(h_a: np.ndarray, h_b: np.ndarray) -> np.ndarray:
    """면적 IoU = Σmin/Σmax (마지막 축). `prompt_geometry.hist_iou` 와 동일 정의."""
    return (np.minimum(h_a, h_b).sum(-1)
            / np.maximum(np.maximum(h_a, h_b).sum(-1), 1e-12))


def wave_iou(Sb: np.ndarray, members: dict, bins: int = WAVE_BINS):
    """제품 분포-IoU: 프레임별로 뱅크 전 문장 코사인을 적응적 binning 하고,
    각 이벤트 클래스 히스토그램과 normal 히스토그램의 면적 IoU 를 낸다.

    ⚠️ binning 범위는 **프레임별** min~max 다 (전역 고정 구간이 아니다). 고차원 코사인은
       프레임마다 절대 수준이 달라서, 고정 구간을 쓰면 밝은/어두운 프레임이 서로 다른
       bin 해상도를 갖게 되고 IoU 가 프레임 밝기를 읽는다.

    반환: {class_label: iou[f]} (normal 제외)
    """
    lo = Sb.min(axis=1)
    hi = Sb.max(axis=1)
    w = np.maximum(hi - lo, 1e-6)
    Bi = np.clip(((Sb - lo[:, None]) / w[:, None] * bins).astype(np.int32), 0, bins - 1)
    f = Sb.shape[0]
    fi = np.arange(f)
    h = {}
    for c, idx in members.items():
        flat = (fi[:, None] * bins + Bi[:, idx]).ravel()
        cnt = np.bincount(flat, minlength=f * bins).reshape(f, bins)
        h[c] = cnt.astype(np.float32) / len(idx)
    return {c: hist_iou(h["normal"], h[c]) for c in members if c != "normal"}


def stage_wave(args) -> None:
    """전 뱅크를 **제품 분포-IoU 규칙**으로 재채점 + argmax 와의 일치도 산출.

    argmax 결과(`bank_project_class_stats`)를 대체하지 않는다 — 별 테이블에 쓴다.
    두 규칙은 서로 다른 질문에 답하므로 섞으면 안 된다.
    """
    only = _split(args.banks)
    t0 = time.time()
    with connect() as conn:
        with conn.cursor() as cur:
            ensure_schema(cur)
            conn.commit()
            log("문장 벡터 적재…")
            h2c, SENT = load_sentence_vectors(cur)
            banks = load_banks(cur, only)

        prepared, skipped = [], []
        for b in banks:
            if any(h not in h2c for h, _c, _g in b["rows"]):
                skipped.append((b["version"], "벡터 없는 문장 존재"))
                continue
            cls_local: dict[str, list[int]] = {}
            gcols: list[int] = []
            for chash, cls, _g in b["rows"]:
                cls_local.setdefault(cls, []).append(len(gcols))
                gcols.append(h2c[chash])
            if "normal" not in cls_local:
                # normal 이 기준 분포다 — 없으면 IoU 가 정의되지 않는다. 0 으로 채우지 않는다.
                skipped.append((b["version"], "normal 클래스 문장 없음 — IoU 기준 분포 부재"))
                continue
            if len(cls_local) < 2:
                skipped.append((b["version"], "이벤트 클래스 없음"))
                continue
            prepared.append({
                "version": b["version"],
                "gcols": np.asarray(gcols, dtype=np.int64),
                "members": {c: np.asarray(v, dtype=np.int64) for c, v in cls_local.items()},
                "events": sorted(c for c in cls_local if c != "normal"),
            })
        for ver, why in skipped:
            log(f"  skip {ver}: {why}")
        log(f"대상 뱅크 {len(prepared)} (bins={WAVE_BINS} thr={WAVE_THR})")

        acc = {p["version"]: {"iou": [], "wpred": [], "apred": [], "multi": []}
               for p in prepared}
        projects: list[str] = []
        n_seen = 0
        for pj, F in frame_batches(conn, CHUNK, args.limit):
            S = F @ SENT.T
            projects.extend(pj)
            for p in prepared:
                Sb = S[:, p["gcols"]]                       # 이 뱅크의 문장만
                iou = wave_iou(Sb, p["members"])
                ev = p["events"]
                I = np.stack([iou[c] for c in ev], axis=1)   # [f, n_ev]
                fired = I < WAVE_THR
                # 단일라벨 축소: 발화한 것 중 IoU 최저. 아무것도 안 발화하면 normal(-1).
                wpred = np.where(fired.any(axis=1), I.argmin(axis=1), -1)
                # 같은 gather 로 argmax 규칙도 같이 낸다 — 일치도를 정확히 재려면 동일 프레임
                # 동일 뱅크에서 두 규칙을 동시에 계산해야 한다.
                per_cls = np.stack([Sb[:, p["members"][c]].max(axis=1)
                                    for c in ["normal"] + ev], axis=1)
                a = per_cls.argmax(axis=1)
                apred = np.where(a == 0, -1, a - 1)          # -1 = normal
                acc[p["version"]]["iou"].append(I)
                acc[p["version"]]["wpred"].append(wpred)
                acc[p["version"]]["apred"].append(apred)
                acc[p["version"]]["multi"].append(fired.sum(axis=1) > 1)
                del Sb, per_cls
            n_seen += len(pj)
            del S
            if n_seen % (CHUNK * 20) == 0:
                log(f"  프레임 {n_seen:,} 처리 ({time.time()-t0:.0f}s)")
        log(f"wave 채점 끝 — 프레임 {n_seen:,}, {time.time()-t0:.0f}s. 적재 시작")

        proj = np.asarray(projects)
        for p in prepared:
            a = acc[p["version"]]
            _write_wave(conn, p, np.vstack(a["iou"]), np.concatenate(a["wpred"]),
                        np.concatenate(a["apred"]), np.concatenate(a["multi"]), proj)
    log(f"wave 완료 — 총 {time.time()-t0:.0f}s")


def _write_wave(conn, p: dict, I: np.ndarray, wpred: np.ndarray, apred: np.ndarray,
                multi: np.ndarray, proj: np.ndarray) -> None:
    ver, ev = p["version"], p["events"]
    rows_stats, rows_agree = [], []
    for prj in np.unique(proj):
        pm = proj == prj
        n = int(pm.sum())
        for j, cls in enumerate(ev):
            v = I[pm, j]
            rows_stats.append((
                ver, str(prj), cls, n,
                float(v.mean()), float(np.percentile(v, 10)), float(np.percentile(v, 50)),
                int((v < WAVE_THR).sum()), int((wpred[pm] == j).sum())))
        rows_stats.append((ver, str(prj), "normal", n, None, None, None,
                           int((wpred[pm] == -1).sum()), int((wpred[pm] == -1).sum())))
        rows_agree.append((ver, str(prj), n,
                           int((wpred[pm] == apred[pm]).sum()),
                           int((apred[pm] >= 0).sum()),
                           int((wpred[pm] >= 0).sum()),
                           int(multi[pm].sum())))
    with conn.cursor() as cur:
        cur.execute("DELETE FROM analysis.bank_wave_stats WHERE bank_version=%s", (ver,))
        cur.execute("DELETE FROM analysis.bank_rule_agreement WHERE bank_version=%s", (ver,))
        psycopg2.extras.execute_values(cur, """
            INSERT INTO analysis.bank_wave_stats (bank_version,project,class_label,
              n_frames,avg_iou,p10_iou,p50_iou,n_fired,n_pred) VALUES %s""",
            rows_stats, page_size=500)
        psycopg2.extras.execute_values(cur, """
            INSERT INTO analysis.bank_rule_agreement (bank_version,project,n_frames,
              n_agree,argmax_events,wave_events,multi_fire) VALUES %s""",
            rows_agree, page_size=500)
    conn.commit()
    tot = sum(r[2] for r in rows_agree)
    ag = sum(r[3] for r in rows_agree)
    log(f"  {ver}: wave {len(rows_stats):,}행 / 규칙 일치 {ag:,}/{tot:,} ({100*ag/max(tot,1):.1f}%)")




# ─────────────────── top-K vs 분포-IoU — 3규칙 동시 채점 ───────────────────

def topk_vote(Sb: np.ndarray, lab: np.ndarray, n_cls: int, k: int | None = None) -> np.ndarray:
    """전역 top-K 다수결 — `prompt_geometry.vote_topk` 와 동일 판정
    (동표는 클래스 최고 코사인: votes + (topc+2)/10 의 argmax).
    lab[j] = 뱅크-로컬 열 j 의 클래스 인덱스. 반환 pred[f] (클래스 인덱스)."""
    k = RULE_K if k is None else k
    kg = min(k, Sb.shape[1])
    part = np.argpartition(-Sb, kg - 1, axis=1)[:, :kg]
    sel_v = np.take_along_axis(Sb, part, 1)
    sel_c = lab[part]
    votes = np.stack([(sel_c == ci).sum(1) for ci in range(n_cls)], 1)
    topc = np.stack([np.where(sel_c == ci, sel_v, -2.0).max(1) for ci in range(n_cls)], 1)
    return (votes + (topc + 2.0) / 10.0).argmax(1)


def _topk_selfcheck() -> None:
    """topk_vote 를 브루트포스와 대조 — 판정 로직이 깨지면 여기서 죽는다."""
    rng = np.random.default_rng(0)
    Sb = rng.standard_normal((50, 40)).astype(np.float32)
    lab = rng.integers(0, 3, 40).astype(np.int32)
    pred = topk_vote(Sb, lab, 3, k=10)
    for f in range(50):
        o = np.argsort(-Sb[f])[:10]
        votes = np.bincount(lab[o], minlength=3).astype(np.float64)
        topc = np.full(3, -2.0)
        for c in range(3):
            m = lab[o] == c
            if m.any():
                topc[c] = float(Sb[f][o][m].max())
        assert int(pred[f]) == int((votes + (topc + 2.0) / 10.0).argmax()), f"frame {f}"


def _acc_rules(pjA, kA, tpred, wpred, apred, ev, env, clsd, nd) -> None:
    """한 청크의 3규칙 예측을 (project|cluster) 그룹 카운터에 누적한다.
    카운터 = [n, topk_ev, wave_ev, argmax_ev, agree_tw, agree_ta, agree_wa]."""
    for kind, keys in (("project", pjA), ("cluster", kA)):
        for key in np.unique(keys):
            m = keys == key
            row = env.setdefault((kind, str(key)), np.zeros(7, dtype=np.int64))
            row += np.array([m.sum(), (tpred[m] >= 0).sum(), (wpred[m] >= 0).sum(),
                             (apred[m] >= 0).sum(), (tpred[m] == wpred[m]).sum(),
                             (tpred[m] == apred[m]).sum(), (wpred[m] == apred[m]).sum()],
                            dtype=np.int64)
    for prj in np.unique(pjA):
        m = pjA == prj
        nd[str(prj)] = nd.get(str(prj), 0) + int(m.sum())
        tsub = tpred[m]
        for j, c in enumerate(ev):
            clsd[(str(prj), c)] = clsd.get((str(prj), c), 0) + int((tsub == j).sum())
        clsd[(str(prj), "normal")] = clsd.get((str(prj), "normal"), 0) + int((tsub == -1).sum())


def _write_rulecmp(conn, ver: str, env: dict, clsd: dict, nd: dict) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM analysis.bank_topk_stats WHERE bank_version=%s", (ver,))
        cur.execute("DELETE FROM analysis.bank_rule_env WHERE bank_version=%s", (ver,))
        psycopg2.extras.execute_values(cur, """
            INSERT INTO analysis.bank_topk_stats
            (bank_version,project,class_label,n_frames,n_pred) VALUES %s""",
            [(ver, p, c, nd[p], n) for (p, c), n in clsd.items()], page_size=1000)
        psycopg2.extras.execute_values(cur, """
            INSERT INTO analysis.bank_rule_env
            (bank_version,group_kind,group_key,n_frames,topk_events,wave_events,
             argmax_events,agree_tw,agree_ta,agree_wa) VALUES %s""",
            [(ver, k, g) + tuple(int(x) for x in row)
             for (k, g), row in env.items()], page_size=1000)
    conn.commit()
    pr = [r for (k, _g), r in env.items() if k == "project"]
    n = sum(int(r[0]) for r in pr)
    tw = sum(int(r[4]) for r in pr)
    log(f"  {ver}: topk_ev {sum(int(r[1]) for r in pr):,} / wave_ev "
        f"{sum(int(r[2]) for r in pr):,} / topk==wave {100*tw/max(n,1):.1f}%")


def stage_topk(args) -> None:
    """전 텍스트 뱅크를 **top-K(K=10)·분포-IoU·argmax 세 규칙으로 동시에** 채점하고
    프로젝트/환경(kmeans64)별 이벤트 수·일치도를 집계한다.

    일치도는 같은 프레임·같은 뱅크·같은 cos 행렬에서 세 규칙을 함께 내야 정확하다
    (stage_wave 의 원칙). 프레임 단위 원본은 저장하지 않는다 (디스크 99%).
    벡터 전용 뱅크는 batch_step kind='topk-ext' 로 (JSON 로딩이 스텝 하나 값이라).
    """
    _topk_selfcheck()
    only = _split(args.banks)
    t0 = time.time()
    with connect() as conn:
        with conn.cursor() as cur:
            ensure_schema(cur)
            conn.commit()
            log("문장 벡터 적재…")
            h2c, SENT = load_sentence_vectors(cur)
            banks = load_banks(cur, only)
            cur.execute("SELECT entity_id, cluster_id FROM analysis.frame_cluster"
                        " WHERE method=%s", (args.method,))
            e2k = dict(cur.fetchall())
        log(f"군집 매핑 {len(e2k):,} ({args.method})")

        prepared, skipped = [], []
        for b in banks:
            if any(h not in h2c for h, _c, _g in b["rows"]):
                skipped.append((b["version"], "벡터 없는 문장 존재"))
                continue
            cls_local: dict[str, list[int]] = {}
            gcols: list[int] = []
            for chash, cls, _g in b["rows"]:
                cls_local.setdefault(cls, []).append(len(gcols))
                gcols.append(h2c[chash])
            if "normal" not in cls_local or len(cls_local) < 2:
                skipped.append((b["version"], "normal/이벤트 클래스 요건 미달"))
                continue
            cs = sorted(cls_local)
            lab = np.empty(len(gcols), dtype=np.int32)
            for ci, c in enumerate(cs):
                lab[np.asarray(cls_local[c], dtype=np.int64)] = ci
            ev = [c for c in cs if c != "normal"]
            prepared.append({
                "version": b["version"], "classes": cs, "events": ev, "lab": lab,
                "gcols": np.asarray(gcols, dtype=np.int64),
                "members": {c: np.asarray(v, dtype=np.int64) for c, v in cls_local.items()},
                "tmap": np.asarray([ev.index(c) if c != "normal" else -1 for c in cs],
                                   dtype=np.int32),
            })
        for ver, why in skipped:
            log(f"  skip {ver}: {why}")
        log(f"대상 뱅크 {len(prepared)} (K={RULE_K}, bins={WAVE_BINS} thr={WAVE_THR})")

        acc = {p["version"]: ({}, {}, {}) for p in prepared}   # env, clsd, nd
        n_seen = 0
        for pj, F, eids in frame_batches_ids(conn, CHUNK, args.limit):
            S = F @ SENT.T
            pjA = np.asarray(pj)
            kA = np.asarray([e2k.get(e, -1) for e in eids], dtype=np.int32)
            for p in prepared:
                Sb = S[:, p["gcols"]]
                ev = p["events"]
                tpred = p["tmap"][topk_vote(Sb, p["lab"], len(p["classes"]))]
                iou = wave_iou(Sb, p["members"])
                I = np.stack([iou[c] for c in ev], axis=1)
                wpred = np.where((I < WAVE_THR).any(axis=1), I.argmin(axis=1), -1)
                per_cls = np.stack([Sb[:, p["members"][c]].max(axis=1)
                                    for c in ["normal"] + ev], axis=1)
                a = per_cls.argmax(axis=1)
                apred = np.where(a == 0, -1, a - 1)
                env, clsd, nd = acc[p["version"]]
                _acc_rules(pjA, kA, tpred, wpred, apred, ev, env, clsd, nd)
                del Sb, per_cls
            n_seen += len(pj)
            del S
            if n_seen % (CHUNK * 20) == 0:
                log(f"  프레임 {n_seen:,} 처리 ({time.time()-t0:.0f}s)")
        log(f"topk 채점 끝 — 프레임 {n_seen:,}, {time.time()-t0:.0f}s. 적재 시작")
        for p in prepared:
            env, clsd, nd = acc[p["version"]]
            _write_rulecmp(conn, p["version"], env, clsd, nd)
    log(f"topk 완료 — 총 {time.time()-t0:.0f}s")


def _run_topk_ext(version: str) -> str:
    """벡터 전용 뱅크의 3규칙 동시 채점 — _run_score_ext 와 같은 이유로 뱅크당 스텝 1개."""
    _topk_selfcheck()
    bank = _load_ext_bank(version)
    if bank is None:
        return "skip: JSON 없음 또는 빈 파일"
    V, C, _I = bank
    classes = sorted(set(C.tolist()))
    if 0 not in classes:
        return f"skip: normal(0) 클래스 없음 (classes={classes})"
    if len(classes) < 2:
        return "skip: 이벤트 클래스 없음"
    names = {c: CLASS_NAMES.get(c, f"class_{c}") for c in classes}
    members = {names[c]: np.flatnonzero(C == c) for c in classes}
    cs = sorted(names[c] for c in classes)
    ev = [c for c in cs if c != "normal"]
    lab = np.empty(V.shape[0], dtype=np.int32)
    for ci, cname in enumerate(cs):
        lab[members[cname]] = ci
    tmap = np.asarray([ev.index(c) if c != "normal" else -1 for c in cs], dtype=np.int32)
    log(f"  뱅크 {version}: 벡터 {V.shape[0]:,} 클래스 {cs} (3규칙 동시)")

    env, clsd, nd = {}, {}, {}
    n_seen = 0
    with connect() as conn:
        with conn.cursor() as cur:
            ensure_schema(cur)
            conn.commit()
            cur.execute("SELECT entity_id, cluster_id FROM analysis.frame_cluster"
                        " WHERE method='kmeans64'")
            e2k = dict(cur.fetchall())
        for pj, F, eids in frame_batches_ids(conn, CHUNK, None):
            Sb = F @ V.T
            pjA = np.asarray(pj)
            kA = np.asarray([e2k.get(e, -1) for e in eids], dtype=np.int32)
            tpred = tmap[topk_vote(Sb, lab, len(cs))]
            iou = wave_iou(Sb, members)
            I = np.stack([iou[c] for c in ev], axis=1)
            wpred = np.where((I < WAVE_THR).any(axis=1), I.argmin(axis=1), -1)
            per_cls = np.stack([Sb[:, members[c]].max(axis=1) for c in ["normal"] + ev],
                               axis=1)
            a = per_cls.argmax(axis=1)
            apred = np.where(a == 0, -1, a - 1)
            _acc_rules(pjA, kA, tpred, wpred, apred, ev, env, clsd, nd)
            n_seen += len(pj)
            del Sb, per_cls
        _write_rulecmp(conn, version, env, clsd, nd)
    return f"ok: 프레임 {n_seen:,}"


def stage_affinity(args) -> None:
    """문장 × 그룹 평균 코사인 — "어떤 문장이 어디에 붙는가"의 정본.

    뱅크 독립이라 121,614 문장 × 그룹 한 벌만 만들면 35개 뱅크 전부가 재사용한다.
    그룹은 project(현장) 기본이고, `--groups cluster` 면 무감독 KMeans 클러스터를 쓴다
    (GT 없이 이미지 임베딩만으로 만든 잠재 클래스).

    ⚠️ 메모리: 그룹별 누적기는 **고정 크기**여야 한다. 초안에서 그룹별로 S 조각을 리스트에
       모았다가 cohort-b(73,390 프레임) 하나가 35GB 로 계산돼 폐기했다. 지금은
       합/최대/히스토그램(40 bin) 만 들고 있어 그룹당 20MB 로 상한이 잡힌다.
    """
    kind = args.groups
    t0 = time.time()
    with connect() as conn:
        with conn.cursor() as cur:
            ensure_schema(cur)
            conn.commit()
            log("문장 벡터 적재…")
            h2c, SENT = load_sentence_vectors(cur)
            hashes = [None] * len(h2c)
            for h, i in h2c.items():
                hashes[i] = h
            gmap = None
            if kind == "cluster":
                cur.execute("""SELECT entity_id, cluster_id, project
                               FROM analysis.frame_cluster WHERE method=%s""", (args.method,))
                # within-project 방법(wp*)은 cluster_id 가 프로젝트 안에서만 유일하다.
                # 그래서 그룹키에 프로젝트를 붙여야 한다 — 안 붙이면 서로 다른 현장의
                # 0번 군집이 한 그룹으로 합쳐져 현장 통제가 무의미해진다.
                wp = args.method.startswith("wp")
                gmap = {e: (f"{prj}#{c}" if wp else str(c)) for e, c, prj in cur}
                if not gmap:
                    raise SystemExit(f"클러스터 없음 (method={args.method}) — 먼저 "
                                     f"`cluster` 스테이지를 돌릴 것")
                log(f"클러스터 적재 {len(gmap):,} 프레임 / {len(set(gmap.values()))} 군집")

        M = SENT.shape[0]
        acc: dict[str, dict] = {}
        n_seen = 0
        for pj, F, eids in frame_batches_ids(conn, CHUNK, args.limit):
            S = F @ SENT.T                                  # [f, M]
            keys = ([gmap.get(e) for e in eids] if kind == "cluster" else pj)
            karr = np.asarray([k if k is not None else "\0" for k in keys])
            for g in np.unique(karr):
                if g == "\0":
                    continue
                Sg = S[karr == g]
                a = acc.setdefault(str(g), {
                    "sum": np.zeros(M, np.float64), "n": 0,
                    "max": np.full(M, -2.0, np.float32),
                    "hist": np.zeros((M, AFF_BINS), np.int32), "oob": 0})
                a["sum"] += Sg.sum(axis=0)
                a["n"] += Sg.shape[0]
                np.maximum(a["max"], Sg.max(axis=0), out=a["max"])
                # 고정 구간 히스토그램 — 백분위수를 나중에 뽑기 위한 유일한 고정메모리 수단
                bi = np.clip(((Sg - AFF_LO) / (AFF_HI - AFF_LO) * AFF_BINS).astype(np.int32),
                             0, AFF_BINS - 1)
                a["oob"] += int(((Sg < AFF_LO) | (Sg > AFF_HI)).sum())
                for r in range(bi.shape[0]):
                    np.add.at(a["hist"], (np.arange(M), bi[r]), 1)
                del Sg, bi
            n_seen += len(pj)
            del S
            if n_seen % (CHUNK * 10) == 0:
                log(f"  프레임 {n_seen:,} ({time.time()-t0:.0f}s) / 그룹 {len(acc)}")
        log(f"누적 끝 — 프레임 {n_seen:,}, {time.time()-t0:.0f}s")

        with conn.cursor() as cur:
            cur.execute("DELETE FROM analysis.sentence_affinity WHERE group_kind=%s", (kind,))
            for g, a in sorted(acc.items()):
                mean = (a["sum"] / a["n"]).astype(np.float32)
                p90 = hist_percentile(a["hist"], 90.0)
                if a["oob"]:
                    log(f"  ⚠️ {kind}={g}: 히스토그램 구간 밖 {a['oob']:,} 관측 "
                        f"— p90 이 절단됐을 수 있다 (AFF_LO/AFF_HI 확인)")
                rows = [(hashes[j], kind, g, a["n"], float(mean[j]), float(p90[j]),
                         float(a["max"][j])) for j in range(M)]
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO analysis.sentence_affinity
                    (content_hash,group_kind,group_key,n_frames,mean_cos,p90_cos,max_cos)
                    VALUES %s""", rows, page_size=2000)
                conn.commit()
                log(f"  {kind}={g}: 문장 {len(rows):,} 적재 (프레임 {a['n']:,})")
    log(f"affinity 완료 — 총 {time.time()-t0:.0f}s")


def frame_batches_ids(conn, chunk: int, limit: int | None = None):
    """frame_batches 와 같지만 entity_id 도 돌려준다 (클러스터 조인용)."""
    with conn.cursor(name="frames_cur_ids") as cur:
        cur.itersize = chunk
        cur.execute("""
          SELECT entity_id, split_part(source_key,'/',1) AS project, embedding::text
          FROM image_embeddings WHERE entity_type='frame' AND source_key IS NOT NULL
            AND split_part(source_key,'/',1) <> ALL(%s)
          ORDER BY entity_id
        """ + (f" LIMIT {int(limit)}" if limit else ""),
            (list(EXCLUDE_PROJECTS) or [""],))
        bp, bv, be = [], [], []
        for eid, proj, vtxt in cur:
            be.append(eid)
            bp.append(proj)
            bv.append(np.fromstring(vtxt.strip("[]"), sep=",", dtype=np.float32))
            if len(bp) >= chunk:
                yield bp, _norm(np.vstack(bv)), be
                bp, bv, be = [], [], []
        if bp:
            yield bp, _norm(np.vstack(bv)), be




def _wp_k(n: int, kmax: int) -> int:
    """프로젝트 크기에 맞춘 군집 수. 작은 현장에 큰 k 를 주면 군집당 표본이 무너진다.

    규칙: k ≈ sqrt(n/40) 를 [2, kmax] 로 클립. n=6,140(source-f) → 12,
    n=288(sourcej) → 2. 근거는 "군집당 최소 수십 프레임" 이라는 실무 하한이고
    이론적 최적이 아니다 — 안정성(E2)으로 사후 검증할 것.
    """
    import math
    return max(2, min(kmax, int(round(math.sqrt(n / 40.0)))))


def stage_cluster(args) -> None:
    """GT 없이 프레임 임베딩만으로 잠재 클래스를 만든다 (MiniBatchKMeans).

    왜 성립하나: 대조학습 임베딩은 의미가 방향에 실려 있어서 정규화 후 코사인 기준으로
    뭉치면 라벨 없이도 장면 유형이 갈린다. 그 군집이 "GT 없이 얻은 클래스 후보"이고,
    프롬프트가 어느 군집에 붙는지가 "그 프롬프트가 실제로 무엇을 읽는가"다.

    군집은 GT 가 아니다. 카메라/현장/조명이 의미보다 강한 신호일 수 있다 (우리 실측:
    UMAP 영토가 사실상 카메라 86퍼센트였다). 그래서 군집별 project 순도를 같이 낸다 —
    순도가 높으면 그 군집은 장면 유형이 아니라 카메라다.
    """
    from sklearn.cluster import MiniBatchKMeans

    k = args.k
    method = f"kmeans{k}"
    t0 = time.time()
    with connect() as conn:
        with conn.cursor() as cur:
            ensure_schema(cur)
            conn.commit()
        ids, projs, chunks = [], [], []
        for pj, F, eids in frame_batches_ids(conn, CHUNK, args.limit):
            ids.extend(eids)
            projs.extend(pj)
            chunks.append(F)
        X = np.vstack(chunks)
        del chunks

        if args.within_project:
            # 현장을 통제하고 그 안에서 군집화한다. 전역 군집이 project 와 AMI 0.584 /
            # 순도 84.1% 였으므로(E3 실측) 전역 군집은 "의미"가 아니라 "현장"이다.
            method = f"wp{k}"
            parr = np.asarray(projs)
            rows = []
            for prj in sorted(set(projs)):
                m = parr == prj
                n = int(m.sum())
                kk = _wp_k(n, k)
                km = MiniBatchKMeans(n_clusters=kk, batch_size=2048, n_init=5,
                                     max_iter=200, random_state=0)
                lab = km.fit_predict(X[m])
                idx = np.flatnonzero(m)
                rows.extend((ids[int(i)], method, int(lab[j]), prj)
                            for j, i in enumerate(idx))
                sz = np.bincount(lab, minlength=kk)
                log(f"  {prj:34s} n={n:6,} k={kk:2d} 군집크기 {sz.min():,}~{sz.max():,}")
            with conn.cursor() as cur:
                cur.execute("DELETE FROM analysis.frame_cluster WHERE method=%s", (method,))
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO analysis.frame_cluster (entity_id,method,cluster_id,project)
                    VALUES %s""", rows, page_size=5000)
            conn.commit()
            log(f"cluster 완료 - method={method}, 행 {len(rows):,}, 총 {time.time()-t0:.0f}s")
            return

        log(f"프레임 {X.shape[0]:,} x {X.shape[1]} 적재 ({time.time()-t0:.0f}s) - KMeans k={k}")
        km = MiniBatchKMeans(n_clusters=k, batch_size=4096, n_init=5,
                             max_iter=200, random_state=0)
        lab = km.fit_predict(X)
        # 정규화 벡터의 유클리드 KMeans = 코사인 기준 구면 KMeans 와 단조 동치
        sizes = np.bincount(lab, minlength=k)
        log(f"KMeans 완료 ({time.time()-t0:.0f}s) inertia={km.inertia_:.1f} "
            f"군집 크기 min={sizes.min():,} max={sizes.max():,} 빈군집={int((sizes==0).sum())}")
        with conn.cursor() as cur:
            cur.execute("DELETE FROM analysis.frame_cluster WHERE method=%s", (method,))
            psycopg2.extras.execute_values(cur, """
                INSERT INTO analysis.frame_cluster (entity_id,method,cluster_id,project)
                VALUES %s""",
                [(ids[i], method, int(lab[i]), projs[i]) for i in range(len(ids))],
                page_size=5000)
        conn.commit()
    log(f"cluster 완료 - method={method}, 총 {time.time()-t0:.0f}s")


# --------------------- 문장 형식(구문) 분석 ---------------------

# 이 뱅크 문장군은 전치사구로 슬롯이 열린다. 정규식으로 템플릿을 추측하지 않고
# 전치사 + 최대 4단어를 후보로 잡아 빈도로 발견한다.
_PREP = ("at", "in", "on", "of", "near", "inside", "from", "across", "beside",
         "around", "with", "through", "over", "under", "behind", "next")
_STOP = {"a", "an", "the", "and", "or", "is", "are", "was", "were", "be"}


def phrases_of(text: str, max_words: int = 4) -> set:
    """문장에서 전치사구 후보 + 선행 수식어를 뽑는다 (소문자 정규화).

    ⚠️ **접두사 중첩 제거가 필수다.** 순진하게 n-gram 을 다 담으면 `on a metal` 과
    `on a metal shelf` 가 둘 다 들어가 Jaccard 1.00 인 쌍이 대량 생긴다. 그건 진짜
    동반 출현(교락)이 아니라 추출 산물인데, 그대로 두면 delta 상위표가 같은 슬롯값의
    접두사들로 도배되고 Ridge 도 완전 공선인 열들을 받는다.
    (실측: 이 제거 전에는 J=1.00 쌍이 9개나 상위에 올라왔다.)

    남는 동반 출현 — 예: `in the center of` 와 `of the room` (J=0.89) — 은 서로 다른
    구문이 실제로 함께 나타나는 것이므로 제거하지 않고 Ridge 가 통제한다.
    """
    t = re.sub(r"[^a-zA-Z0-9\s-]", " ", (text or "").lower())
    w = [x for x in t.split() if x]
    span = set()
    for i, tok in enumerate(w):
        if tok in _PREP:
            for n in range(2, max_words + 1):
                if i + n <= len(w) and w[i + n - 1] not in _STOP:
                    span.add(" ".join(w[i:i + n]))
    # 다른 구문의 진부분 접두사인 것을 버린다 (최장 구문만 남긴다)
    out = {a for a in span
           if not any(b != a and b.startswith(a + " ") for b in span)}
    for tok in w[:3]:
        if tok not in _STOP and len(tok) > 2:
            out.add(tok)
    return out


def stage_phrase(args) -> None:
    """어떤 형식의 문장이 잘 붙는가 - 구문 단위 평균 코사인.

    `sentence_affinity`(문장 x 그룹 평균)를 구문으로 접는다. 개별 문장이 아니라 구문
    단위로 봐야 "형식"에 대한 답이 나온다. 같은 클래스 안에서 그 구문을 포함하는 문장
    평균 vs 포함하지 않는 문장 평균의 차(delta)가 그 구문의 기여다.

    클래스 안에서 비교한다 - 클래스를 섞으면 normal 이 코사인 스케일이 높아서(실측 0.26
    vs fire 0.17) 구문 효과가 아니라 클래스 효과를 읽는다.
    delta 는 상관이지 인과가 아니다 - 구문끼리 동반 출현하면 교락된다.
    """
    kind = args.groups
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
          -- ⚠️ 클래스 라벨은 **뱅크별 멤버십 속성**이다. 전 뱅크를 DISTINCT 로 묶으면
          -- (a) 어느 뱅크 기준인지 알 수 없고 (b) 라벨이 상충하는 문장(실측 2,106건)이
          -- 여러 층에 중복 계상되고 (c) 분석 대상 뱅크에 없는 클래스가 등장한다.
          -- 실제로 v1.0.8.0(smoking 문장 0개)에서 `smoking` 층이 나왔다 — 이 버그다.
          SELECT a.group_key, s.class_label, s.content_hash, s.text, a.mean_cos
          FROM analysis.sentence_affinity a
          JOIN bank_sentences s ON s.content_hash = a.content_hash
          JOIN prompt_banks b ON b.bank_id = s.bank_id AND b.version_tag = %s
          WHERE a.group_kind = %s AND (%s = '' OR a.group_key = %s)""",
          (args.bank, kind, args.group or "", args.group or ""))
        rows = cur.fetchall()
    if not rows:
        raise SystemExit(f"sentence_affinity 비어 있음 (group_kind={kind}, bank={args.bank}) - "
                         f"먼저 `affinity --groups {kind}` 를 돌릴 것")
    log(f"affinity {len(rows):,}행 (뱅크 {args.bank} 클래스 기준) - 구문 접기")

    base, agg, cache = {}, {}, {}
    for g, cls, chash, text, mc in rows:
        b = base.setdefault((g, cls), [0.0, 0])
        b[0] += mc
        b[1] += 1
        phs = cache.get(chash)
        if phs is None:
            phs = phrases_of(text)
            cache[chash] = phs
        for ph in phs:
            a = agg.setdefault((g, cls, ph), [0.0, 0])
            a[0] += mc
            a[1] += 1

    minn = args.min_sentences
    out = []
    for (g, cls, ph), (ssum, sn) in agg.items():
        bsum, bn = base[(g, cls)]
        if sn < minn or bn - sn < minn:   # 여집합이 작으면 delta 가 의미 없다
            continue
        wm = ssum / sn
        wo = (bsum - ssum) / (bn - sn)
        out.append((g, cls, ph, sn, bn, wm, wo, wm - wo))
    out.sort(key=lambda r: -abs(r[7]))

    print(f"\n## 구문별 평균 코사인 기여 (상위 {args.top}, |delta| 내림차순)\n")
    print("| 그룹 | 클래스 | 구문 | 문장수 | 포함 평균 | 미포함 평균 | delta |")
    print("|---|---|---|---|---|---|---|")
    for g, cls, ph, sn, _bn, wm, wo, d in out[:args.top]:
        print(f"| {g} | {cls} | `{ph}` | {sn:,} | {wm:.4f} | {wo:.4f} | "
              f"{'+' if d >= 0 else ''}{d:.4f} |")

    os.makedirs(REPORT_DIR, exist_ok=True)
    path = os.path.join(REPORT_DIR, f"phrase_{kind}.tsv")
    with open(path, "w", encoding="utf-8") as f:
        f.write("group\tclass\tphrase\tn_with\tn_class\tmean_with\tmean_without\tdelta\n")
        for r in out:
            f.write("\t".join(str(x) for x in r) + "\n")
    log(f"전체 {len(out):,}행 -> {path}")




def _phrase_matrix(rows: list, minn: int):
    """(group, class) 층별로 문장 x 구문 지시행렬을 만든다.

    반환: {(group, class): (X_csr, y, phrase_list, texts)}
    구문은 그 층에서 minn 이상 나타난 것만 — 소표본 구문은 Ridge 계수가 폭주한다.
    """
    from scipy import sparse

    strata: dict = {}
    cache: dict = {}
    for g, cls, chash, text, mc in rows:
        st = strata.setdefault((g, cls), {"y": [], "ph": [], "tx": []})
        phs = cache.get(chash)
        if phs is None:
            phs = phrases_of(text)
            cache[chash] = phs
        st["y"].append(mc)
        st["ph"].append(phs)
        st["tx"].append(text)

    out = {}
    for key, st in strata.items():
        cnt: dict = {}
        for phs in st["ph"]:
            for ph in phs:
                cnt[ph] = cnt.get(ph, 0) + 1
        n = len(st["y"])
        keep = [ph for ph, c in cnt.items() if c >= minn and n - c >= minn]
        if len(keep) < 2 or n < 4 * minn:
            continue
        col = {ph: j for j, ph in enumerate(keep)}
        ri, ci = [], []
        for i, phs in enumerate(st["ph"]):
            for ph in phs:
                j = col.get(ph)
                if j is not None:
                    ri.append(i)
                    ci.append(j)
        X = sparse.csr_matrix((np.ones(len(ri), dtype=np.float32), (ri, ci)),
                              shape=(n, len(keep)))
        out[key] = (X, np.asarray(st["y"], dtype=np.float64), keep, st["tx"])
    return out


def stage_ridge(args) -> None:
    """구문 delta 를 Ridge 로 통제 + 동반 출현 진단 + 홀드아웃 재현.

    단순 delta(포함 평균 - 미포함 평균)는 구문이 함께 다니면 서로의 효과를 훔친다.
    실측: `escalator` 계열 6개 구문이 상위 40 에 몰려 있었다 = 하나의 슬롯값이 여섯 번
    세어진 것. 지시변수 회귀로 다른 구문을 통제한 뒤의 순수 기여를 낸다.

    구문 수가 많고 상관이 크므로 반드시 L2(Ridge) 다 — OLS 는 다중공선성으로 계수가
    폭주한다. alpha 는 층 크기에 비례해 스케일한다(층마다 문장 수가 10배 이상 다르다).
    """
    from sklearn.linear_model import Ridge

    kind = args.groups
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
          -- ⚠️ 클래스 라벨은 **뱅크별 멤버십 속성**이다. 전 뱅크를 DISTINCT 로 묶으면
          -- (a) 어느 뱅크 기준인지 알 수 없고 (b) 라벨이 상충하는 문장(실측 2,106건)이
          -- 여러 층에 중복 계상되고 (c) 분석 대상 뱅크에 없는 클래스가 등장한다.
          -- 실제로 v1.0.8.0(smoking 문장 0개)에서 `smoking` 층이 나왔다 — 이 버그다.
          SELECT a.group_key, s.class_label, s.content_hash, s.text, a.mean_cos
          FROM analysis.sentence_affinity a
          JOIN bank_sentences s ON s.content_hash = a.content_hash
          JOIN prompt_banks b ON b.bank_id = s.bank_id AND b.version_tag = %s
          WHERE a.group_kind = %s AND (%s = '' OR a.group_key = %s)""",
          (args.bank, kind, args.group or "", args.group or ""))
        rows = cur.fetchall()
    if not rows:
        raise SystemExit(f"sentence_affinity 비어 있음 (group_kind={kind}, bank={args.bank})")
    log(f"affinity {len(rows):,}행 (뱅크 {args.bank} 클래스 기준) → 층별 지시행렬")

    strata = _phrase_matrix(rows, args.min_sentences)
    log(f"층 {len(strata)}개 (문장 {4*args.min_sentences} 이상, 구문 2개 이상)")

    recs = []
    for (g, cls), (X, y, phrases, _tx) in sorted(strata.items()):
        n, m = X.shape
        # alpha 를 층 크기에 비례 — 고정 alpha 는 작은 층을 과도하게 눌러 계수를 0 으로 만든다
        alpha = args.alpha * max(n / 1000.0, 0.1)
        mdl = Ridge(alpha=alpha, fit_intercept=True)
        mdl.fit(X, y)
        # 같은 데이터로 단순 delta 도 계산해 나란히 비교
        Xd = X.toarray().astype(bool)
        for j, ph in enumerate(phrases):
            sel = Xd[:, j]
            k = int(sel.sum())
            d = float(y[sel].mean() - y[~sel].mean())
            recs.append({"group": g, "cls": cls, "phrase": ph, "n_with": k,
                         "n_stratum": n, "delta": d, "beta": float(mdl.coef_[j]),
                         "shrink": (float(mdl.coef_[j]) / d) if abs(d) > 1e-9 else None})

    if not recs:
        raise SystemExit("집계 가능한 층이 없다 — min-sentences 를 낮출 것")

    # ── 동반 출현 진단: delta 상위 구문끼리 Jaccard ──
    top = sorted(recs, key=lambda r: -abs(r["delta"]))[:args.top]
    log("")
    print(f"## Ridge 통제 전후 (상위 {args.top}, |delta| 내림차순)\n")
    print("| 그룹 | 클래스 | 구문 | 문장수 | delta | beta(통제후) | 잔존율 |")
    print("|---|---|---|---|---|---|---|")
    for r in top:
        sh = f"{r['shrink']:.2f}" if r["shrink"] is not None else "—"
        print(f"| {r['group']} | {r['cls']} | `{r['phrase']}` | {r['n_with']:,} | "
              f"{r['delta']:+.4f} | {r['beta']:+.4f} | {sh} |")

    # ── 홀드아웃 재현: 그룹을 반으로 나눠 beta 순위 상관 ──
    if kind == "project" and not args.group:
        from scipy.stats import spearmanr
        import collections
        by_ph = collections.defaultdict(dict)
        for r in recs:
            if r["cls"] != "normal":
                continue
            by_ph[r["phrase"]][r["group"]] = r["beta"]
        groups = sorted({r["group"] for r in recs})
        half = len(groups) // 2
        A, B = set(groups[:half]), set(groups[half:])
        pa, pb = [], []
        for ph, d in by_ph.items():
            va = [v for g, v in d.items() if g in A]
            vb = [v for g, v in d.items() if g in B]
            if len(va) >= 3 and len(vb) >= 3:
                pa.append(float(np.mean(va)))
                pb.append(float(np.mean(vb)))
        if len(pa) >= 10:
            rho, pv = spearmanr(pa, pb)
            log("")
            log(f"홀드아웃 재현 (normal 클래스, 구문 {len(pa)}개): "
                f"프로젝트 절반 A vs B 의 beta 순위상관 rho={rho:.3f} (p={pv:.1e})")
            log("  rho 가 낮으면 그 구문 효과는 현장 특이적이고 일반 규칙이 아니다")

    path = os.path.join(REPORT_DIR, f"ridge_{kind}.tsv")
    os.makedirs(REPORT_DIR, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("group\tclass\tphrase\tn_with\tn_stratum\tdelta\tbeta\tshrink\n")
        for r in sorted(recs, key=lambda r: -abs(r["beta"])):
            sh = "" if r["shrink"] is None else f"{r['shrink']:.4f}"
            f.write(f"{r['group']}\t{r['cls']}\t{r['phrase']}\t{r['n_with']}\t"
                    f"{r['n_stratum']}\t{r['delta']:.6f}\t{r['beta']:.6f}\t{sh}\n")
    log(f"전체 {len(recs):,}행 → {path}")


def stage_cooc(args) -> None:
    """동반 출현 진단 — delta 상위 구문끼리 Jaccard. 하나의 슬롯값이 여러 구문으로
    세어지는지 본다 (Ridge 를 돌리기 전에 먼저 봐야 하는 것)."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
          SELECT DISTINCT s.content_hash, s.class_label, s.text
          FROM bank_sentences s JOIN prompt_banks b USING(bank_id)
          WHERE b.version_tag = %s""", (args.bank,))
        rows = cur.fetchall()
    if not rows:
        raise SystemExit(f"뱅크 {args.bank} 문장 없음")
    by_ph: dict = {}
    for _h, cls, text in rows:
        for ph in phrases_of(text):
            by_ph.setdefault((cls, ph), set()).add(text)
    cand = [(k, v) for k, v in by_ph.items() if len(v) >= args.min_sentences]
    cand.sort(key=lambda kv: -len(kv[1]))
    cand = cand[:args.top]
    print(f"\n## 동반 출현 (Jaccard ≥ {args.jaccard}, 뱅크 {args.bank})\n")
    print("| 클래스 | 구문 A | 구문 B | J | 해석 |")
    print("|---|---|---|---|---|")
    shown = 0
    for i in range(len(cand)):
        (c1, p1), s1 = cand[i]
        for j in range(i + 1, len(cand)):
            (c2, p2), s2 = cand[j]
            if c1 != c2:
                continue
            jac = len(s1 & s2) / max(len(s1 | s2), 1)
            if jac >= args.jaccard:
                tag = "사실상 동일 슬롯" if jac >= 0.9 else "강한 교락"
                print(f"| {c1} | `{p1}` | `{p2}` | {jac:.2f} | {tag} |")
                shown += 1
    if not shown:
        print(f"| — | — | — | — | Jaccard {args.jaccard} 이상 쌍 없음 |")




def _load_affinity(cur, kind: str):
    """affinity 를 한 번만 올린다. (content_hash, group) → mean_cos.

    전 뱅크 루프에서 매 뱅크마다 2.6M 행을 다시 읽으면 35배 낭비다. 벡터/affinity 는
    **뱅크 독립**이므로 한 번 올려 재사용한다 (클래스 라벨만 뱅크별로 갈아 끼운다).
    """
    cur.execute("""SELECT content_hash, group_key, mean_cos
                   FROM analysis.sentence_affinity WHERE group_kind=%s""", (kind,))
    h2i, g2i, trip = {}, {}, []
    for h, g, mc in cur:
        hi = h2i.setdefault(h, len(h2i))
        gi = g2i.setdefault(g, len(g2i))
        trip.append((hi, gi, mc))
    A = np.full((len(h2i), len(g2i)), np.nan, dtype=np.float32)
    for hi, gi, mc in trip:
        A[hi, gi] = mc
    return h2i, g2i, A


def stage_ridge_all(args) -> None:
    """전 뱅크 × 전 그룹의 구문 Ridge 계수를 적재하고 버전 간 강건성을 낸다.

    affinity 는 뱅크 독립이라 한 번만 읽고, 뱅크별로 (문장집합 + 클래스 라벨) 만
    갈아 끼운다. 그래서 35뱅크가 단일 뱅크의 35배가 아니라 거의 같은 시간에 끝난다.
    """
    from sklearn.linear_model import Ridge
    from scipy import sparse

    kind = args.groups
    t0 = time.time()
    with connect() as conn:
        with conn.cursor() as cur:
            ensure_schema(cur)
            conn.commit()
            log("affinity 적재 (1회)…")
            h2i, g2i, A = _load_affinity(cur, kind)
            log(f"  문장 {len(h2i):,} × 그룹 {len(g2i)}")
            cur.execute("""SELECT b.version_tag, s.content_hash, s.class_label, s.text
                           FROM prompt_banks b JOIN bank_sentences s USING(bank_id)
                           WHERE b.sentence_storage='db_backed' ORDER BY b.version_tag""")
            banks: dict = {}
            for ver, ch, cls, text in cur:
                banks.setdefault(ver, []).append((ch, cls, text))
        log(f"뱅크 {len(banks)}개")

        cache: dict = {}
        minn = args.min_sentences
        gkeys = sorted(g2i, key=lambda g: g2i[g])
        total = 0
        with conn.cursor() as cur:
            cur.execute("DELETE FROM analysis.phrase_beta WHERE group_kind=%s", (kind,))
        for ver, rows in banks.items():
            # 뱅크 안에서 (클래스) 별로 문장 인덱스를 모은다
            by_cls: dict = {}
            for ch, cls, text in rows:
                hi = h2i.get(ch)
                if hi is None:
                    continue
                phs = cache.get(ch)
                if phs is None:
                    phs = phrases_of(text)
                    cache[ch] = phs
                by_cls.setdefault(cls, []).append((hi, phs))
            out = []
            for cls, items in by_cls.items():
                if len(items) < 4 * minn:
                    continue
                cnt: dict = {}
                for _hi, phs in items:
                    for ph in phs:
                        cnt[ph] = cnt.get(ph, 0) + 1
                n = len(items)
                keep = [ph for ph, c in cnt.items() if c >= minn and n - c >= minn]
                if len(keep) < 2:
                    continue
                col = {ph: j for j, ph in enumerate(keep)}
                ri, ci = [], []
                for i, (_hi, phs) in enumerate(items):
                    for ph in phs:
                        j = col.get(ph)
                        if j is not None:
                            ri.append(i)
                            ci.append(j)
                X = sparse.csr_matrix((np.ones(len(ri), np.float32), (ri, ci)),
                                      shape=(n, len(keep)))
                Xd = X.toarray().astype(bool)
                hidx = np.fromiter((hi for hi, _ in items), dtype=np.int64, count=n)
                for g in gkeys:
                    y = A[hidx, g2i[g]]
                    ok = ~np.isnan(y)
                    if int(ok.sum()) < 4 * minn:
                        continue
                    yv = y[ok].astype(np.float64)
                    Xo = X[ok]
                    Xdo = Xd[ok]
                    alpha = args.alpha * max(Xo.shape[0] / 1000.0, 0.1)
                    mdl = Ridge(alpha=alpha, fit_intercept=True).fit(Xo, yv)
                    for j, ph in enumerate(keep):
                        sel = Xdo[:, j]
                        k = int(sel.sum())
                        if k < minn or Xo.shape[0] - k < minn:
                            continue
                        d = float(yv[sel].mean() - yv[~sel].mean())
                        out.append((ver, kind, g, cls, ph, k, int(Xo.shape[0]),
                                    d, float(mdl.coef_[j])))
            if out:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, """
                        INSERT INTO analysis.phrase_beta (bank_version,group_kind,group_key,
                          class_label,phrase,n_with,n_stratum,delta,beta) VALUES %s
                        ON CONFLICT DO NOTHING""", out, page_size=2000)
                conn.commit()
            total += len(out)
            log(f"  {ver:12s} 층 {len(by_cls)} → {len(out):,}행 (누적 {total:,})")
    log(f"ridge-all 완료 — {total:,}행, {time.time()-t0:.0f}s")


def stage_robust(args) -> None:
    """전 버전에 걸쳐 강건한 구문 — 뱅크·그룹을 가로질러 부호가 일관되는 것."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
          SELECT phrase,
                 COUNT(DISTINCT bank_version)                     AS banks,
                 COUNT(DISTINCT group_key)                        AS groups,
                 COUNT(*)                                         AS cells,
                 AVG(beta)                                        AS beta_avg,
                 AVG(delta)                                       AS delta_avg,
                 SUM(CASE WHEN beta > 0 THEN 1 ELSE 0 END)::float / COUNT(*) AS pos_frac,
                 AVG(beta) / NULLIF(AVG(delta),0)                 AS shrink
          FROM analysis.phrase_beta
          WHERE group_kind=%s AND class_label = %s
          GROUP BY 1
          HAVING COUNT(DISTINCT bank_version) >= %s AND COUNT(*) >= %s
          ORDER BY ABS(AVG(beta)) DESC LIMIT %s""",
          (args.groups, args.cls, args.min_banks, args.min_cells, args.top))
        rows = cur.fetchall()
    if not rows:
        raise SystemExit("phrase_beta 가 비어 있거나 조건이 너무 엄격하다 — "
                         "`ridge-all` 을 먼저 돌리고 --min-banks 를 낮출 것")
    print(f"\n## 전 버전 강건 구문 (class={args.cls}, 뱅크 {args.min_banks}개 이상, "
          f"셀 {args.min_cells}개 이상)\n")
    print("| 구문 | 뱅크 | 그룹 | 셀 | 평균 beta | 평균 delta | 잔존율 | 부호일관 |")
    print("|---|---|---|---|---|---|---|---|")
    for ph, nb, ng, nc, ba, da, pf, sh in rows:
        cons = max(pf, 1 - pf)
        shs = f"{sh:.2f}" if sh is not None else "—"
        print(f"| `{ph}` | {nb} | {ng} | {nc:,} | {ba:+.4f} | {da:+.4f} | {shs} | "
              f"{cons:.0%} |")
    log("")
    log("부호일관 100% + 잔존율 0.6 이상이면 버전·현장을 가로질러 유효한 구문이다")




# ────────────────────── 배치 (cron 이 스텝 하나씩 처리) ──────────────────────

# 텍스트가 없는(벡터 전용) 뱅크. class + feature 는 있어서 채점은 되고 구문 분석만 불가.
EXT_BANK_DIR = os.environ.get("EXT_BANK_DIR", "/nas_userwatch/prompts")


def stage_batch_plan(_args) -> None:
    """원장에 스텝을 시드한다. 이미 있는 스텝은 건드리지 않는다(재실행 안전)."""
    with connect() as conn, conn.cursor() as cur:
        ensure_schema(cur)
        cur.execute("""SELECT version_tag FROM prompt_banks
                       WHERE sentence_storage='external_only' ORDER BY version_tag""")
        ext = [r[0] for r in cur]
        cur.execute("""SELECT DISTINCT project FROM analysis.frame_cluster
                       WHERE method='wp16' ORDER BY project""")
        projs = [r[0] for r in cur]
        rows = [("score-ext", v, 10) for v in ext] + \
               [("cluster-phrase", p, 20) for p in projs] + \
               [("report", "", 90)]
        psycopg2.extras.execute_values(cur, """
            INSERT INTO analysis.batch_step (kind, arg, ord) VALUES %s
            ON CONFLICT (kind, arg) DO NOTHING""", rows)
        conn.commit()
        cur.execute("""SELECT status, COUNT(*) FROM analysis.batch_step
                       GROUP BY 1 ORDER BY 1""")
        for st, n in cur:
            log(f"  {st:9s} {n}")
    log(f"batch-plan 완료 — 외부뱅크 {len(ext)} / 프로젝트 {len(projs)} / report 1")


def stage_batch_peek(_args) -> None:
    """다음 pending 스텝의 종류만 출력한다 (상태 변경 없음).

    실행 환경을 스텝별로 나눠야 해서 필요하다 — `score-ext` 는 원본 JSON 이 호스트에만
    있고(컨테이너 미마운트), `cluster-phrase` 는 sklearn 이 필요한데 호스트 anaconda 의
    sklearn 이 numpy ABI 불일치로 깨져 있다(sklearn 1.5.1 vs numpy 2.1.3).
    그래서 러너가 이걸 먼저 물어보고 실행기를 고른다.
    """
    with connect() as conn, conn.cursor() as cur:
        ensure_schema(cur)
        conn.commit()
        cur.execute("""SELECT kind, arg FROM analysis.batch_step
                       WHERE status='pending' ORDER BY ord, step_id LIMIT 1""")
        row = cur.fetchone()
    print(f"{row[0]}\t{row[1]}" if row else "none\t")


def stage_batch_next(args) -> None:
    """pending 스텝 하나를 처리한다. cron 이 반복 호출하면 전체가 전진한다."""
    with connect() as conn, conn.cursor() as cur:
        ensure_schema(cur)
        conn.commit()
        cur.execute("""SELECT step_id, kind, arg FROM analysis.batch_step
                       WHERE status='pending' ORDER BY ord, step_id LIMIT 1
                       FOR UPDATE SKIP LOCKED""")
        row = cur.fetchone()
        if not row:
            log("pending 스텝 없음 — 배치 완료")
            return
        sid, kind, arg = row
        cur.execute("""UPDATE analysis.batch_step SET status='running', run_at=now()
                       WHERE step_id=%s""", (sid,))
        conn.commit()
    log(f"스텝 {sid}: {kind} {arg}")
    t0 = time.time()
    try:
        if kind == "score-ext":
            note = _run_score_ext(arg)
        elif kind == "topk-ext":
            note = _run_topk_ext(arg)
        elif kind == "cluster-phrase":
            note = _run_cluster_phrase(arg, args)
        elif kind == "report":
            stage_report(args)
            note = "ok"
        else:
            raise SystemExit(f"알 수 없는 스텝 종류: {kind}")
        st, err = ("skipped", note) if note.startswith("skip:") else ("done", note)
    except Exception as exc:  # noqa: BLE001 — 스텝 단위 fail-forward, 원장에 남긴다
        st, err = "failed", repr(exc)[:400]
        log(f"  실패: {err}")
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""UPDATE analysis.batch_step SET status=%s, seconds=%s, note=%s
                       WHERE step_id=%s""", (st, time.time() - t0, err, sid))
        conn.commit()
    log(f"스텝 {sid} {st} ({time.time()-t0:.0f}s) — {err}")


def _load_ext_bank(version: str):
    """벡터 전용 뱅크 JSON 을 읽는다. 반환 (vec[n,1024] 정규화, cls[n], ids[n]) 또는 None.

    파일이 0.5~1.6 GB 라 이 함수 하나가 스텝 하나를 차지할 만큼 비싸다 — 그래서 배치다.
    """
    import json as _json

    path = f"{EXT_BANK_DIR}/{version}/text_features_{version}.json"
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        data = _json.load(f)
    if not data:
        return None
    V = np.asarray([r["feature"] for r in data], dtype=np.float32)
    C = np.asarray([r.get("class", 0) for r in data], dtype=np.int32)
    # ⚠️ JSON 의 `ID` 필드를 식별자로 쓰면 안 된다 — 1.0.13.0 실측에서 45,840개 레코드의
    #    ID 가 **전부 0** 이었다. 그걸 gidx 로 쓰면 모든 승자가 gidx=0 이 되고,
    #    bank_sentence_wins 의 PK (bank_version, project, gidx) 충돌로 normal 뒤의
    #    이벤트 클래스 행이 전부 드롭된다 (실제로 그렇게 났다: 이벤트 16,425건 예측인데
    #    승자 행은 normal 21개뿐).
    #    그래서 **배열 위치**를 gidx 로 쓴다 — 전역 유일하고 JSON 인덱스로 역추적된다.
    I = np.arange(len(data), dtype=np.int64)
    ids = {r.get("ID") for r in data}
    if len(ids) <= 1 and len(data) > 1:
        log(f"    (ID 필드가 축퇴됨: 고유값 {len(ids)}개 / {len(data):,} 레코드 "
            f"→ gidx = JSON 배열 위치)")
    V /= np.linalg.norm(V, axis=1, keepdims=True)
    return V, C, I


def _run_score_ext(version: str) -> str:
    """벡터 전용 뱅크를 argmax + 분포 IoU 로 채점한다 (텍스트 없어 구문 분석 제외).

    클래스 라벨은 JSON 의 정수 `class` 다. 0=normal 이라는 관례는 wave 가 이미 쓰는 것과
    같다 — 다르면 IoU 기준 분포가 뒤집히므로 0 이 없으면 skip 한다.
    """
    bank = _load_ext_bank(version)
    if bank is None:
        return "skip: JSON 없음 또는 빈 파일"
    V, C, I = bank
    classes = sorted(set(C.tolist()))
    if 0 not in classes:
        return f"skip: normal(0) 클래스 없음 (classes={classes})"
    events = [c for c in classes if c != 0]
    if not events:
        return f"skip: 이벤트 클래스 없음 (classes={classes})"
    names = {c: CLASS_NAMES.get(c, f"class_{c}") for c in classes}
    members = {names[c]: np.flatnonzero(C == c) for c in classes}
    log(f"  뱅크 {version}: 벡터 {V.shape[0]:,} 클래스 {[names[c] for c in classes]}")

    ev_names = [names[c] for c in events]
    acc = {"iou": [], "wp": [], "ap": [], "multi": [], "cos": [], "gidx": []}
    projects: list[str] = []
    with connect() as conn:
        for pj, F in frame_batches(conn, CHUNK, None):
            Sb = F @ V.T
            projects.extend(pj)
            iou = wave_iou(Sb, members)
            Iar = np.stack([iou[c] for c in ev_names], axis=1)
            fired = Iar < WAVE_THR
            acc["wp"].append(np.where(fired.any(1), Iar.argmin(1), -1))
            acc["multi"].append(fired.sum(1) > 1)
            per = np.stack([Sb[:, members[names[c]]].max(1) for c in [0] + events], axis=1)
            gid = np.stack([I[members[names[c]]][Sb[:, members[names[c]]].argmax(1)]
                            for c in [0] + events], axis=1)
            a = per.argmax(1)
            acc["ap"].append(np.where(a == 0, -1, a - 1))
            acc["cos"].append(per)
            acc["gidx"].append(gid)
            acc["iou"].append(Iar)
            del Sb, per, gid
        proj = np.asarray(projects)
        p = {"version": version, "events": ev_names,
             "classes": [names[c] for c in [0] + events]}
        _write_ext(conn, p, np.vstack(acc["cos"]), np.vstack(acc["gidx"]),
                   np.vstack(acc["iou"]), np.concatenate(acc["wp"]),
                   np.concatenate(acc["ap"]), np.concatenate(acc["multi"]), proj)
    return f"ok: 프레임 {len(projects):,} 클래스 {len(classes)}"


def _write_ext(conn, p, cos, gidx, I, wpred, apred, multi, proj) -> None:
    """외부 뱅크 결과를 기존 테이블과 같은 스키마로 적재 (argmax + wave + 일치도)."""
    ver, ev, allc = p["version"], p["events"], p["classes"]
    pred = cos.argmax(axis=1)
    margin = top2_margin(cos)
    rs, rw, rg, ra = [], [], [], []
    for prj in np.unique(proj):
        pm = proj == prj
        n = int(pm.sum())
        for ci, cls in enumerate(allc):
            v = cos[pm, ci]
            isp = pm & (pred == ci)
            npd = int(isp.sum())
            mg = margin[isp]
            rs.append((ver, str(prj), cls, int(v.size), float(v.mean()),
                       float(np.percentile(v, 50)), float(np.percentile(v, 90)),
                       float(v.max()), npd,
                       float(np.nanmean(mg)) if npd else None))
            if npd:
                g, c = gidx[isp, ci], cos[isp, ci]
                for gv in np.unique(g):
                    sel = g == gv
                    rw.append((ver, str(prj), cls, int(gv), int(sel.sum()),
                               float(c[sel].mean()), float(np.nanmean(mg[sel]))))
        for j, cls in enumerate(ev):
            v = I[pm, j]
            rg.append((ver, str(prj), cls, n, float(v.mean()),
                       float(np.percentile(v, 10)), float(np.percentile(v, 50)),
                       int((v < WAVE_THR).sum()), int((wpred[pm] == j).sum())))
        rg.append((ver, str(prj), "normal", n, None, None, None,
                   int((wpred[pm] == -1).sum()), int((wpred[pm] == -1).sum())))
        ra.append((ver, str(prj), n, int((wpred[pm] == apred[pm]).sum()),
                   int((apred[pm] >= 0).sum()), int((wpred[pm] >= 0).sum()),
                   int(multi[pm].sum())))
    with conn.cursor() as cur:
        for t in ("bank_project_class_stats", "bank_sentence_wins", "bank_wave_stats",
                  "bank_rule_agreement"):
            cur.execute(f"DELETE FROM analysis.{t} WHERE bank_version=%s", (ver,))
        psycopg2.extras.execute_values(cur, """INSERT INTO analysis.bank_project_class_stats
            (bank_version,project,class_label,n_frames,avg_cos,p50_cos,p90_cos,max_cos,
             n_pred,avg_margin) VALUES %s""", rs, page_size=500)
        psycopg2.extras.execute_values(cur, """INSERT INTO analysis.bank_sentence_wins
            (bank_version,project,class_label,gidx,wins,avg_cos,avg_margin)
            VALUES %s ON CONFLICT DO NOTHING""", rw, page_size=1000)
        psycopg2.extras.execute_values(cur, """INSERT INTO analysis.bank_wave_stats
            (bank_version,project,class_label,n_frames,avg_iou,p10_iou,p50_iou,
             n_fired,n_pred) VALUES %s""", rg, page_size=500)
        psycopg2.extras.execute_values(cur, """INSERT INTO analysis.bank_rule_agreement
            (bank_version,project,n_frames,n_agree,argmax_events,wave_events,multi_fire)
            VALUES %s""", ra, page_size=500)
        cur.execute("""INSERT INTO analysis.bank_run
            (bank_version,status,n_frames,n_sentences,n_vectors,n_dup_text,classes)
            VALUES (%s,'ok',%s,0,%s,0,%s)
            ON CONFLICT (bank_version) DO UPDATE SET status='ok',
              n_frames=EXCLUDED.n_frames, n_vectors=EXCLUDED.n_vectors,
              classes=EXCLUDED.classes, err='벡터 전용 — 구문 분석 불가', run_at=now()""",
            (ver, int(proj.size), int(cos.shape[0] and gidx.max() + 1),
             json.dumps(allc)))
    conn.commit()
    log(f"  {ver}: stats {len(rs)} / wins {len(rw):,} / wave {len(rg)} / agree {len(ra)}")


def _run_cluster_phrase(project: str, args) -> str:
    """한 프로젝트의 내부 군집에 대해 구문 Ridge 계수를 낸다.

    **affinity 를 DB 에 적재하지 않는다** — 157 군집 × 121,614 문장 = 1,900만 행이
    3GB 넘게 먹어 디스크 99% 환경에서 PG 를 멈출 뻔했다. 문장 평균은 메모리에만 두고
    `phrase_beta` 만 쓴다. 셀당 상위 N개로 잘라 저장량을 묶는다.
    """
    from sklearn.linear_model import Ridge
    from scipy import sparse

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT entity_id, cluster_id FROM analysis.frame_cluster
                           WHERE method='wp16' AND project=%s""", (project,))
            cmap = {e: int(c) for e, c in cur}
            if not cmap:
                return "skip: 군집 없음"
            log("  문장 벡터 적재…")
            h2c, SENT = load_sentence_vectors(cur)
            cur.execute("""SELECT b.version_tag, s.content_hash, s.class_label, s.text
                           FROM prompt_banks b JOIN bank_sentences s USING(bank_id)
                           WHERE b.sentence_storage='db_backed'""")
            banks: dict = {}
            for ver, ch, cls, text in cur:
                banks.setdefault(ver, []).append((ch, cls, text))

        cids = sorted(set(cmap.values()))
        M = SENT.shape[0]
        ssum = {c: np.zeros(M, np.float64) for c in cids}
        scnt = {c: 0 for c in cids}
        n_seen = 0
        for _pj, F, eids in frame_batches_ids(conn, CHUNK, None):
            keep = [i for i, e in enumerate(eids) if e in cmap]
            if not keep:
                continue
            S = F[keep] @ SENT.T
            ks = np.asarray([cmap[eids[i]] for i in keep])
            for c in np.unique(ks):
                m = ks == c
                ssum[int(c)] += S[m].sum(axis=0)
                scnt[int(c)] += int(m.sum())
            n_seen += len(keep)
            del S
        if not n_seen:
            return "skip: 프레임 0"
        mean = {c: (ssum[c] / scnt[c]).astype(np.float32) for c in cids if scnt[c]}
        log(f"  프레임 {n_seen:,} / 군집 {len(mean)}")

        cache: dict = {}
        minn = args.min_sentences
        out = []
        for ver, rows in banks.items():
            by_cls: dict = {}
            for ch, cls, text in rows:
                ci = h2c.get(ch)
                if ci is None:
                    continue
                phs = cache.get(ch)
                if phs is None:
                    phs = phrases_of(text)
                    cache[ch] = phs
                by_cls.setdefault(cls, []).append((ci, phs))
            for cls, items in by_cls.items():
                if len(items) < 4 * minn:
                    continue
                cnt: dict = {}
                for _ci, phs in items:
                    for ph in phs:
                        cnt[ph] = cnt.get(ph, 0) + 1
                n = len(items)
                keep2 = [ph for ph, c in cnt.items() if c >= minn and n - c >= minn]
                if len(keep2) < 2:
                    continue
                col = {ph: j for j, ph in enumerate(keep2)}
                ri, cci = [], []
                for i, (_ci, phs) in enumerate(items):
                    for ph in phs:
                        j = col.get(ph)
                        if j is not None:
                            ri.append(i)
                            cci.append(j)
                X = sparse.csr_matrix((np.ones(len(ri), np.float32), (ri, cci)),
                                      shape=(n, len(keep2)))
                Xd = X.toarray().astype(bool)
                sidx = np.fromiter((ci for ci, _ in items), dtype=np.int64, count=n)
                for c in sorted(mean):
                    y = mean[c][sidx].astype(np.float64)
                    alpha = args.alpha * max(n / 1000.0, 0.1)
                    mdl = Ridge(alpha=alpha, fit_intercept=True).fit(X, y)
                    cell = []
                    for j, ph in enumerate(keep2):
                        sel = Xd[:, j]
                        k = int(sel.sum())
                        d = float(y[sel].mean() - y[~sel].mean())
                        cell.append((ver, "cluster", f"{project}#{c}", cls, ph, k, n,
                                     d, float(mdl.coef_[j])))
                    cell.sort(key=lambda r: -abs(r[8]))
                    out.extend(cell[:args.top_per_cell])
        with conn.cursor() as cur:
            cur.execute("""DELETE FROM analysis.phrase_beta
                           WHERE group_kind='cluster' AND group_key LIKE %s""",
                        (project + "#%",))
            if out:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO analysis.phrase_beta (bank_version,group_kind,group_key,
                      class_label,phrase,n_with,n_stratum,delta,beta) VALUES %s
                    ON CONFLICT DO NOTHING""", out, page_size=2000)
        conn.commit()
    return f"ok: 군집 {len(mean)} 뱅크 {len(banks)} → {len(out):,}행"


# ─────────────────────────────── 리포트 ───────────────────────────────

def vkey(tag: str) -> tuple:
    """버전 자연정렬. 사전순이면 v1.0.10.3 이 v1.0.2.0 앞에 오고 대문자 V 가 따로 묶인다."""
    nums = re.findall(r"\d+", tag or "")
    return (tuple(int(n) for n in nums), (tag or "").lower())


def fetch_dupe_banks(cur) -> list[tuple]:
    """문장집합(content_hash+class_label)이 완전히 같은 뱅크 군집.

    "전 버전 비교" 표에서 이걸 모르면 **거짓 다양성**을 본다 — 실측 35버전 중
    7개가 다른 버전의 정확한 중복이고 기준선 v1.0.8.0 도 그 중 하나다.
    """
    cur.execute("""
      WITH fp AS (
        SELECT b.version_tag AS ver, COUNT(*) AS n,
               md5(string_agg(s.content_hash || ':' || s.class_label, ','
                   ORDER BY s.content_hash, s.class_label)) AS finger
        FROM prompt_banks b JOIN bank_sentences s USING(bank_id)
        WHERE b.sentence_storage='db_backed' GROUP BY 1
      )
      SELECT n, COUNT(*), array_agg(ver ORDER BY ver)
      FROM fp GROUP BY finger, n HAVING COUNT(*) > 1
      ORDER BY 2 DESC, 1 DESC""")
    return cur.fetchall()



# 리포트도 같은 상수를 쓴다 — 제외 목록이 두 곳에 있으면 반드시 갈라진다.
EXCLUDE_DEFAULT = EXCLUDE_PROJECTS


def fetch_matrix(cur, exclude: tuple = ()) -> tuple[list, list, dict]:
    """프로젝트 × 뱅크 이벤트율 행렬 — 전량. 슬라이스 두 개만 보면 놓치는 축이다."""
    cur.execute("""
      SELECT project, bank_version,
             MAX(n_frames) FILTER (WHERE class_label='normal') AS frames,
             SUM(n_pred)   FILTER (WHERE class_label<>'normal') AS ev
      FROM analysis.bank_project_class_stats
      WHERE class_label NOT IN %s
        AND (%s = 0 OR project <> ALL(%s))
      GROUP BY 1,2""",
        (PLACEHOLDER_CLASSES, len(exclude), list(exclude) or [""]))
    cell: dict = {}
    frames: dict = {}
    for prj, ver, n, ev in cur:
        if n:
            frames[prj] = n
            # ev 가 NULL = 이벤트 클래스가 구조적으로 없는 뱅크(자리표시자만). 0% 와 다르다.
            cell[(prj, ver)] = None if ev is None else 100.0 * ev / n
    projects = sorted(frames, key=lambda p: -frames[p])
    banks = sorted({v for _p, v in cell}, key=vkey)
    return projects, banks, {"cell": cell, "frames": frames}


def fetch_concentration(cur, exclude: tuple = ()) -> list[tuple]:
    """문장 집중도 — 한 문장이 프로젝트 이벤트의 몇 %를 가져가는가.

    `max-over-sentences` 규칙에서 이게 큰데 마진이 작으면 그 프로젝트의 판정은
    문장 하나에 걸린 동전 던지기다 (source-f 실측: 1문장이 16.5%, 마진 0.0041).
    """
    cur.execute("""
      WITH ev AS (
        -- 자리표시자 제외가 필수다: 이벤트 클래스가 1개뿐인 뱅크(v2.0.5.x = class_5 단독)는
        -- 문장 하나가 자동으로 전량을 먹어 집중도 표를 통째로 오염시킨다 (구조적 산물이지
        -- 발견이 아니다). 실제로 첫 렌더에서 상위 12개가 전부 class_5/class_6 였다.
        -- ⚠️ 이 주석에 리터럴 퍼센트 기호를 쓰지 말 것 — psycopg2 가 포맷 자리로 읽어
        --    IndexError 가 난다 (실제로 났다).
        SELECT bank_version, project, gidx, class_label, wins, avg_margin
        FROM analysis.bank_sentence_wins
        WHERE class_label <> 'normal' AND class_label NOT IN %s
      ), tot AS (
        SELECT bank_version, project, SUM(wins) AS ev_wins FROM ev GROUP BY 1,2
      ), top AS (
        SELECT DISTINCT ON (e.bank_version, e.project)
               e.bank_version, e.project, e.gidx, e.class_label, e.wins,
               e.avg_margin, t.ev_wins
        FROM ev e JOIN tot t USING (bank_version, project)
        ORDER BY e.bank_version, e.project, e.wins DESC
      )
      SELECT top.project, top.bank_version, top.class_label, top.wins, top.ev_wins,
             100.0*top.wins/NULLIF(top.ev_wins,0) AS share, top.avg_margin, s.text
      FROM top
      JOIN prompt_banks b ON b.version_tag = top.bank_version
      JOIN bank_sentences s ON s.bank_id = b.bank_id AND s.gidx = top.gidx
      WHERE top.ev_wins >= 30
        AND (%s = 0 OR top.project <> ALL(%s))
      ORDER BY share DESC NULLS LAST LIMIT 25""",
        (PLACEHOLDER_CLASSES, len(exclude), list(exclude) or [""]))
    return cur.fetchall()


def fetch_project_winners(cur, bank: str, exclude: tuple = ()) -> dict:
    """프로젝트별 이벤트 승자 문장 상위 5 — 기준선 뱅크 하나에 대해 전 프로젝트."""
    cur.execute("""
      SELECT project, class_label, wins, avg_cos, avg_margin, gidx, text FROM (
        SELECT w.project, w.class_label, w.wins, w.avg_cos, w.avg_margin, w.gidx, s.text,
               ROW_NUMBER() OVER (PARTITION BY w.project ORDER BY w.wins DESC) AS rn
        FROM analysis.bank_sentence_wins w
        JOIN prompt_banks b ON b.version_tag = w.bank_version
        JOIN bank_sentences s ON s.bank_id = b.bank_id AND s.gidx = w.gidx
        WHERE w.bank_version = %s AND w.class_label <> 'normal'
          AND w.class_label NOT IN %s
          AND (%s = 0 OR w.project <> ALL(%s))
      ) t WHERE rn <= 5 ORDER BY project, wins DESC""",
        (bank, PLACEHOLDER_CLASSES, len(exclude), list(exclude) or [""]))
    out: dict = {}
    for prj, cls, wins, cos, mg, gidx, text in cur:
        out.setdefault(prj, []).append((cls, wins, cos, mg, gidx, text))
    return out


def fetch_report_data(cur) -> dict:
    cur.execute("""SELECT bank_version,status,n_frames,n_sentences,n_vectors,
                          n_dup_text,classes,err,run_at
                   FROM analysis.bank_run ORDER BY bank_version""")
    runs = cur.fetchall()
    cur.execute("""
      SELECT bank_version,
             SUM(n_frames) FILTER (WHERE class_label='normal') AS n,
             SUM(n_pred) FILTER (WHERE class_label<>'normal') AS n_event,
             AVG(avg_margin) FILTER (WHERE class_label<>'normal') AS ev_margin,
             AVG(avg_cos) FILTER (WHERE class_label='normal') AS normal_cos
      FROM analysis.bank_project_class_stats
      WHERE class_label NOT IN %s
      GROUP BY 1 ORDER BY 1""", (PLACEHOLDER_CLASSES,))
    overview = cur.fetchall()
    cur.execute("""
      SELECT project,
             SUM(n_frames) FILTER (WHERE class_label='normal') AS n,
             SUM(n_pred) FILTER (WHERE class_label<>'normal') AS n_event
      FROM analysis.bank_project_class_stats
      WHERE class_label NOT IN %s
      GROUP BY 1 ORDER BY 2 DESC NULLS LAST""", (PLACEHOLDER_CLASSES,))
    projects = cur.fetchall()
    cur.execute("""
      SELECT bank_version, class_label, SUM(n_pred) AS n_pred,
             AVG(avg_cos) AS avg_cos
      FROM analysis.bank_project_class_stats
      WHERE class_label NOT IN %s
      GROUP BY 1,2 ORDER BY 1,2""", (PLACEHOLDER_CLASSES,))
    percls = cur.fetchall()
    dupes = fetch_dupe_banks(cur)
    ex = EXCLUDE_DEFAULT
    mprojects, mbanks, mdata = fetch_matrix(cur, ex)
    conc = fetch_concentration(cur, ex)
    base = os.environ.get("COS_BASELINE_BANK", "v1.0.8.0")
    winners = fetch_project_winners(cur, base, ex)
    runs = sorted(runs, key=lambda r: vkey(r[0]))
    overview = sorted(overview, key=lambda r: vkey(r[0]))
    percls = sorted(percls, key=lambda r: (vkey(r[0]), r[1]))
    return {"runs": runs, "overview": overview, "projects": projects,
            "percls": percls, "dupes": dupes,
            "matrix": (mprojects, mbanks, mdata), "conc": conc,
            "winners": winners, "baseline": base, "exclude": ex}


def stage_report(_args) -> None:
    os.makedirs(REPORT_DIR, exist_ok=True)
    with connect() as conn, conn.cursor() as cur:
        d = fetch_report_data(cur)
    path = os.path.join(REPORT_DIR, "prompt_cos_db.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(render_html(d))
    log(f"HTML → {path}")
    print(render_markdown(d))


def _tbl(headers: list[str], rows: list[tuple]) -> str:
    h = "".join(f"<th>{c}</th>" for c in headers)
    body = "".join("<tr>" + "".join(f"<td>{_fmt(c)}</td>" for c in r) + "</tr>"
                   for r in rows)
    return f"<div class=wrap><table><thead><tr>{h}</tr></thead><tbody>{body}</tbody></table></div>"


def _fmt(v) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.4f}"
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)



def _matrix_html(d: dict) -> str:
    projects, banks, md = d["matrix"]
    cell, frames = md["cell"], md["frames"]
    head = "<th>프로젝트</th><th>프레임</th>" + "".join(f"<th>{b}</th>" for b in banks)
    rows = []
    for prj in projects:
        tds = [f"<td>{prj}</td>", f"<td>{frames[prj]:,}</td>"]
        for b in banks:
            v = cell.get((prj, b), "MISSING")
            if v == "MISSING":
                tds.append("<td class=na>·</td>")          # 그 뱅크에 이 프로젝트 행이 없음
            elif v is None:
                tds.append("<td class=na>n/a</td>")        # 이벤트 클래스 구조적 부재
            else:
                cls = "hi" if v >= 20 else ("mid" if v >= 10 else "")
                tds.append(f'<td class="{cls}">{v:.1f}</td>')
        rows.append("<tr>" + "".join(tds) + "</tr>")
    return (f"<div class=wrap><table><thead><tr>{head}</tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></div>")


def _conc_html(d: dict) -> str:
    rows = [(r[0], r[1], r[2], r[3], r[4],
             f"{r[5]:.1f}%" if r[5] is not None else "—",
             f"{r[6]:.4f}" if r[6] is not None else "—",
             (r[7] or "")[:90])
            for r in d["conc"]]
    return _tbl(["프로젝트", "뱅크", "클래스", "1위 승수", "이벤트 총승수",
                 "점유율", "마진", "문장"], rows)


def _winners_html(d: dict) -> str:
    out = []
    for prj, rows in d["winners"].items():
        body = "".join(
            f"<tr><td>{c}</td><td>{w:,}</td><td>{cos:.4f}</td>"
            f"<td>{mg:.4f}</td><td>{g}</td><td>{(t or '')[:88]}</td></tr>"
            for c, w, cos, mg, g, t in rows)
        out.append(
            f"<h3>{prj}</h3><div class=wrap><table><thead><tr>"
            f"<th>클래스</th><th>승수</th><th>평균 cos</th><th>마진</th>"
            f"<th>gidx</th><th>문장</th></tr></thead><tbody>{body}</tbody></table></div>")
    return "".join(out) or "<p>승자 없음.</p>"


def render_html(d: dict) -> str:
    ok = [r for r in d["runs"] if r[1] == "ok"]
    skip = [r for r in d["runs"] if r[1] != "ok"]
    return f"""<title>프롬프트 뱅크 코사인 분석</title>
<style>
:root{{--bg:#fff;--fg:#1a1a1a;--mut:#666;--line:#e3e3e3;--hd:#f6f6f6;--acc:#b34700}}
@media (prefers-color-scheme:dark){{:root:not([data-theme=light]){{
  --bg:#16181c;--fg:#e8e8e8;--mut:#9aa0a6;--line:#2c2f36;--hd:#1e2126;--acc:#ff9f5a}}}}
:root[data-theme=dark]{{--bg:#16181c;--fg:#e8e8e8;--mut:#9aa0a6;--line:#2c2f36;--hd:#1e2126;--acc:#ff9f5a}}
body{{background:var(--bg);color:var(--fg);font:15px/1.65 -apple-system,"Segoe UI",
 "Noto Sans KR",sans-serif;max-width:1080px;margin:0 auto;padding:2rem 1.2rem}}
h1{{font-size:1.6rem;margin:0 0 .3rem}} h2{{font-size:1.15rem;margin:2.2rem 0 .6rem;
 border-bottom:2px solid var(--line);padding-bottom:.3rem}}
.meta{{color:var(--mut);font-size:.88rem;margin-bottom:1.5rem}}
.wrap{{overflow-x:auto;margin:.8rem 0}}
table{{border-collapse:collapse;font-size:.86rem;min-width:100%}}
th,td{{border:1px solid var(--line);padding:.35rem .6rem;text-align:right;white-space:nowrap}}
th{{background:var(--hd);font-weight:600;text-align:right}}
td:first-child,th:first-child,td:nth-child(2),th:nth-child(2){{text-align:left}}
.na{{color:var(--mut);font-style:italic}}
.mid{{background:color-mix(in srgb,var(--acc) 12%,transparent)}}
.hi{{background:color-mix(in srgb,var(--acc) 26%,transparent);font-weight:600}}
h3{{font-size:.98rem;margin:1.4rem 0 .2rem;color:var(--acc)}}
.warn{{border-left:3px solid var(--acc);background:color-mix(in srgb,var(--acc) 8%,transparent);
 padding:.7rem 1rem;margin:1rem 0;font-size:.9rem}}
code{{background:var(--hd);padding:.1rem .35rem;border-radius:3px;font-size:.85em}}
</style>
<h1>프롬프트 뱅크 코사인 분석 — 전 버전</h1>
<div class=meta>생성 {time.strftime('%Y-%m-%d %H:%M')} · 채점 뱅크 {len(ok)}개
 (skip {len(skip)}) · 출처 = Postgres <code>analysis.*</code> · CSV/npz 미사용</div>

<div class=warn><b>읽기 전 주의</b><br>
· 여기 수치는 <b>코사인 top-k</b> 기준이다. 제품 판정규칙은 분포 IoU 이고 두 순위는
  우리 실측에서 무상관(ρ≈−0.2)이었다 — 뱅크 채택 근거로 쓰기 전 <code>wave</code> 로 재확인.<br>
· <b>GT 는 40장</b>(sourcej, normal 단일)뿐이다. 아래 어떤 값도 정오가 아니라
  확신도·분포다.<br>
· 자리표시자 클래스({', '.join(PLACEHOLDER_CLASSES)})는 집계에서 제외했다.</div>

<h2>뱅크 실행 원장</h2>
{_tbl(["뱅크","상태","프레임","문장행","벡터열","중복문장","클래스","오류"],
      [(r[0], r[1], r[2], r[3], r[4], r[5],
        ",".join(r[6]) if r[6] else "—", r[7] or "—") for r in d["runs"]])}

<h2>동일 문장집합 뱅크 — 거짓 다양성 주의</h2>
<div class=meta>content_hash+class_label 집합이 완전히 같은 버전들. 아래 표에서 이들은
 항상 같은 수치가 나오며, 별개 버전으로 세면 비교 표본을 부풀린다.</div>
{_tbl(["문장 수","버전 수","동일 문장집합 버전"],
      [(r[0], r[1], ", ".join(r[2])) for r in d["dupes"]]) if d["dupes"]
 else "<p>중복 없음.</p>"}

<h2>버전별 개요 — 이벤트 예측률</h2>
{_tbl(["뱅크","프레임","이벤트 예측","이벤트율","이벤트 마진","normal 평균 cos"],
      [(r[0], r[1], r[2],
        (r[2]/r[1]) if r[1] and r[2] is not None else None, r[3], r[4])
       for r in d["overview"]])}

<h2>버전 × 클래스 — 예측 수와 평균 코사인</h2>
{_tbl(["뱅크","클래스","예측 프레임","평균 cos"], d["percls"])}

<h2>프로젝트별 이벤트 예측 (전 뱅크 합산)</h2>
{_tbl(["프로젝트","프레임×뱅크","이벤트 예측"], d["projects"])}

<h2>프로젝트 × 뱅크 이벤트율 행렬 (%, argmax 기준)</h2>
<div class=meta>전 {len(d["matrix"][1])} 뱅크 × {len(d["matrix"][0])} 프로젝트. 음영 = 10% 이상 / 20% 이상.
 <span class=na>n/a</span> = 그 뱅크에 이벤트 클래스가 **구조적으로 없음**(자리표시자만) — 0% 아님.
 제외 프로젝트: {", ".join(d["exclude"]) or "없음"}</div>
{_matrix_html(d)}

<h2>문장 집중도 — 한 문장이 프로젝트 이벤트를 얼마나 지배하나</h2>
<div class=meta>`max-over-sentences` 규칙에서 점유율이 높은데 마진이 작으면 그 프로젝트 판정은
 문장 하나에 걸린 동전 던지기다. 이벤트 총승수 30 이상만, 점유율 내림차순 상위 25.</div>
{_conc_html(d)}

<h2>프로젝트별 이벤트 승자 문장 (기준선 {d["baseline"]}, 상위 5)</h2>
{_winners_html(d)}

<h2>다음 질의</h2>
<div class=wrap><pre><code>-- 프로젝트별 승자 문장 상위 20 (뱅크 지정)
SELECT w.gidx, w.class_label, w.wins, ROUND(w.avg_cos::numeric,4) cos,
       ROUND(w.avg_margin::numeric,4) margin, s.text
FROM analysis.bank_sentence_wins w
JOIN prompt_banks b ON b.version_tag = w.bank_version
JOIN bank_sentences s ON s.bank_id = b.bank_id AND s.gidx = w.gidx
WHERE w.bank_version = 'v1.0.8.0' AND w.project = 'source-f'
  AND w.class_label &lt;&gt; 'normal'
ORDER BY w.wins DESC LIMIT 20;</code></pre></div>
"""


def render_markdown(d: dict) -> str:
    ok = [r for r in d["runs"] if r[1] == "ok"]
    out = [f"## 프롬프트 뱅크 코사인 분석 — 전 버전 ({len(ok)}개 채점)",
           "",
           f"생성 {time.strftime('%Y-%m-%d %H:%M')} · 출처 = Postgres `analysis.*` (CSV/npz 미사용)",
           "",
           "> **top-k 기준**이다. 제품 판정규칙(분포 IoU)과 무상관(ρ≈−0.2). "
           "GT 는 40장(normal 단일)뿐이라 정오가 아니라 확신도·분포다.",
           "",
           "### 버전별 이벤트 예측률", "",
           "| 뱅크 | 프레임 | 이벤트 예측 | 이벤트율 | normal 평균 cos |",
           "|---|---|---|---|---|"]
    for ver, n, ne, _mg, ncos in d["overview"]:
        rate = f"{100*ne/n:.2f}%" if n and ne is not None else "—"
        out.append(f"| {ver} | {n:,} | {ne or 0:,} | {rate} | "
                   f"{ncos:.4f} |" if ncos is not None else
                   f"| {ver} | {n:,} | {ne or 0:,} | {rate} | — |")
    if d["dupes"]:
        n_red = sum(r[1] - 1 for r in d["dupes"])
        out += ["", f"### ⚠️ 동일 문장집합 뱅크 — 중복 {n_red}개", "",
                "| 문장 수 | 버전 수 | 동일 문장집합 버전 |", "|---|---|---|"]
        for n, k, vers in d["dupes"]:
            out.append(f"| {n:,} | {k} | {', '.join(vers)} |")
        out += ["", "이들은 항상 같은 수치가 나온다. 별개 버전으로 세면 비교 표본이 부풀려진다.", ""]
    if d.get("conc"):
        out += ["", "### 문장 집중도 — 한 문장이 프로젝트 이벤트를 지배하는 곳 (상위 12)", "",
                "| 프로젝트 | 뱅크 | 클래스 | 1위 승수 | 점유율 | 마진 |", "|---|---|---|---|---|---|"]
        for r in d["conc"][:12]:
            share = f"{r[5]:.1f}%" if r[5] is not None else "—"
            mg = f"{r[6]:.4f}" if r[6] is not None else "—"
            out.append(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]:,} | {share} | {mg} |")
        out += ["", "점유율이 높고 마진이 0.02 미만이면 그 판정은 신뢰할 수 없다.", ""]
    out += ["", "### 프로젝트별 이벤트 예측 (전 뱅크 합산)", "",
            "| 프로젝트 | 프레임×뱅크 | 이벤트 예측 |", "|---|---|---|"]
    for prj, n, ne in d["projects"]:
        out.append(f"| {prj} | {n or 0:,} | {ne or 0:,} |")
    return "\n".join(out)


def stage_notion(_args) -> None:
    """노션 등록은 MCP 경유라 이 스크립트가 직접 하지 않는다 — 본문만 내놓는다."""
    with connect() as conn, conn.cursor() as cur:
        d = fetch_report_data(cur)
    path = os.path.join(REPORT_DIR, "prompt_cos_db.notion.md")
    os.makedirs(REPORT_DIR, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(render_markdown(d))
    log(f"노션 본문 → {path}  (DB {NOTION_DB} 에 붙일 마크다운)")


# ─────────────────────────────── 자기검사 ───────────────────────────────

def stage_selftest(_args) -> None:
    """DB·파일 없이 도는 불변식 검사 — 커널이 조용히 틀리는 경우를 잡는다."""
    rng = np.random.default_rng(0)
    S = rng.random((7, 50), dtype=np.float32)

    cols = np.array([3, 41, 7, 22], dtype=np.int64)
    best, arg = max_argmax(S, cols, block=2)          # 블록 경계를 일부러 쪼갠다
    exp_i = cols[S[:, cols].argmax(axis=1)]
    assert np.allclose(best, S[:, cols].max(axis=1)), "max 불일치"
    assert (arg == exp_i).all(), "argmax 가 원래 열 인덱스를 안 돌려줌"

    # 블록 크기가 결과를 바꾸면 안 된다 (running-max 갱신 조건 버그의 전형)
    for blk in (1, 3, 4, 100):
        b2, a2 = max_argmax(S, cols, block=blk)
        assert np.allclose(b2, best) and (a2 == arg).all(), f"블록 {blk} 에서 결과 변동"

    # 단일 열이어도 죽지 않아야 한다
    b1, a1 = max_argmax(S, cols[:1], block=8)
    assert np.allclose(b1, S[:, cols[0]]) and (a1 == cols[0]).all(), "단일 열 실패"

    m = top2_margin(np.array([[0.9, 0.4, 0.1], [0.5, 0.5, 0.2]], dtype=np.float32))
    assert np.allclose(m, [0.5, 0.0]), f"마진 계산 오류: {m}"
    assert np.isnan(top2_margin(np.array([[0.3]], dtype=np.float32))).all(), \
        "클래스 1개면 마진은 NaN 이어야 한다"

    # ── 분포-IoU 커널 ──
    a = np.array([[1.0, 0.0, 0.0]], dtype=np.float32)
    b = np.array([[0.0, 0.0, 1.0]], dtype=np.float32)
    assert np.allclose(hist_iou(a, a), 1.0), "동일 히스토그램 IoU 는 1"
    assert np.allclose(hist_iou(a, b), 0.0), "교집합 없는 히스토그램 IoU 는 0"
    half = np.array([[0.5, 0.5, 0.0]], dtype=np.float32)
    assert np.allclose(hist_iou(a, half), 1 / 3), f"부분겹침 오류: {hist_iou(a, half)}"

    # 완전 분리 케이스: normal 문장은 프레임과 직교, 이벤트 문장은 프레임과 정렬
    # → 두 히스토그램이 다른 bin 에 떨어져 IoU 가 0 에 가까워야 한다.
    Sb = np.zeros((3, 6), dtype=np.float32)
    Sb[:, :3] = 0.10          # normal 문장 3개 (낮은 코사인)
    Sb[:, 3:] = 0.90          # event 문장 3개 (높은 코사인)
    mem = {"normal": np.array([0, 1, 2]), "fire": np.array([3, 4, 5])}
    got = wave_iou(Sb, mem, bins=8)["fire"]
    assert (got < 0.01).all(), f"완전 분리인데 IoU 가 높다: {got}"

    # 완전 중첩: 두 클래스가 같은 값 → IoU 1
    Sb2 = np.full((2, 4), 0.3, dtype=np.float32)
    mem2 = {"normal": np.array([0, 1]), "fire": np.array([2, 3])}
    got2 = wave_iou(Sb2, mem2, bins=8)["fire"]
    assert np.allclose(got2, 1.0), f"완전 중첩인데 IoU 가 1 이 아니다: {got2}"

    # bins 를 바꿔도 분리/중첩의 방향은 유지돼야 한다 (bin 수에 결과가 뒤집히면 버그)
    for bn in (4, 16, 80):
        assert (wave_iou(Sb, mem, bins=bn)["fire"] < 0.05).all(), f"bins={bn} 에서 분리 실패"

    # ── 정수 클래스 규약: prompt_geometry.py 와 같아야 한다 ──
    assert CLASS_NAMES[0] == "normal", "0 은 normal 이어야 한다 (wave 의 기준 분포)"
    assert CLASS_NAMES[4] == "smoking", "4 는 smoking (v0.0.0.0 실측)"
    assert CLASS_NAMES.get(9, f"class_9") == "class_9" or 9 not in CLASS_NAMES

    # ── 구문 추출: 접두사 중첩이 남으면 안 된다 ──
    ph = phrases_of("Light smoke at the bottom drifts across the warehouse at night.")
    for a in ph:
        for b in ph:
            assert not (a != b and b.startswith(a + " ")), f"접두사 중첩 잔존: {a!r} ⊂ {b!r}"
    assert "at night" in ph, f"시간 구문 누락: {sorted(ph)}"
    assert "light" in ph and "smoke" in ph, f"선행 수식어 누락: {sorted(ph)}"
    # 최장 구문이 남아야 한다 (짧은 접두사가 아니라)
    ph2 = phrases_of("a gray umbrella is on a metal shelf.")
    assert "on a metal shelf" in ph2, f"최장 구문 누락: {sorted(ph2)}"
    assert "on a metal" not in ph2, f"접두사가 남았다: {sorted(ph2)}"

    # 정규화 후 자기 코사인 = 1
    V = _norm(rng.random((5, 16), dtype=np.float32))
    assert np.allclose(np.einsum("ij,ij->i", V, V), 1.0, atol=1e-5), "정규화 실패"
    print("selftest OK")


STAGES = {"plan": stage_plan, "score": stage_score, "wave": stage_wave,
          "topk": stage_topk,
          "affinity": stage_affinity, "cluster": stage_cluster,
          "phrase": stage_phrase, "ridge": stage_ridge, "cooc": stage_cooc,
          "ridge-all": stage_ridge_all, "robust": stage_robust,
          "batch-plan": stage_batch_plan, "batch-next": stage_batch_next,
          "batch-peek": stage_batch_peek,
          "report": stage_report,
          "notion": stage_notion, "selftest": stage_selftest}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("stage", choices=sorted(STAGES))
    ap.add_argument("--banks", help="쉼표 구분 version_tag (기본: db_backed 전부)")
    ap.add_argument("--groups", choices=["project", "cluster"], default="project",
                    help="affinity 그룹 축 — project(현장) 또는 cluster(무감독 KMeans)")
    ap.add_argument("--method", default="kmeans64",
                    help="cluster 그룹 사용 시 frame_cluster.method 이름")
    ap.add_argument("--k", type=int, default=64, help="cluster: KMeans 군집 수 (within-project 면 상한)")
    ap.add_argument("--within-project", action="store_true", dest="within_project",
                    help="cluster: 프로젝트 내부에서만 군집화 (현장 통제). method=wp<k>")
    ap.add_argument("--group", help="phrase: 특정 그룹만 (기본 전체)")
    ap.add_argument("--min-sentences", type=int, default=30, dest="min_sentences",
                    help="phrase: 구문이 이만큼의 문장에 나와야 집계 (소표본 잡음 차단)")
    ap.add_argument("--top", type=int, default=40, help="phrase/ridge/cooc: 출력 상위 개수")
    ap.add_argument("--alpha", type=float, default=1.0,
                    help="ridge: L2 계수 (층 크기에 비례해 스케일된다)")
    ap.add_argument("--bank", default="v1.0.8.0",
                    help="phrase/ridge/cooc: 클래스 라벨의 기준 뱅크. 클래스는 뱅크별 "
                         "멤버십 속성이라 반드시 하나를 고정해야 한다")
    ap.add_argument("--cls", default="normal", help="robust: 대상 클래스")
    ap.add_argument("--min-banks", type=int, default=10, dest="min_banks",
                    help="robust: 이만큼의 뱅크에 나타난 구문만")
    ap.add_argument("--min-cells", type=int, default=50, dest="min_cells",
                    help="robust: (뱅크×그룹) 셀 최소 개수")
    ap.add_argument("--top-per-cell", type=int, default=50, dest="top_per_cell",
                    help="cluster-phrase: (뱅크,군집,클래스) 셀당 저장할 구문 수 상한. "
                         "저장량을 묶는 유일한 수단이다")
    ap.add_argument("--jaccard", type=float, default=0.5,
                    help="cooc: 이 값 이상인 구문 쌍만 표시")
    ap.add_argument("--limit", type=int, default=None,
                    help="프레임 상한 — **검증용만**. entity_id 순이라 프로젝트 편향된다")
    args = ap.parse_args()
    STAGES[args.stage](args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
