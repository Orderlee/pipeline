"""AL 선별 결과 검토 화면 — 라벨러에게 보내기 **전에** 눈으로 확인하는 자리.

왜 필요한가: 선택기는 margin 숫자만 보고 고른다. 그 숫자가 맞는 것을 골랐는지는
사람이 이미지를 봐야 안다. 실제로 이 화면이 없었다면 `(-margin).argsort()` 방향 버그
(가장 확신하는 것을 고르고 있었다)를 수치로만 잡아야 했다.

산출물 `al_pick/<pool>__<strategy>.csv` 를 읽어 그리드로 보여주고, 제외한 것을 뺀
**승인 목록**을 `*.approved.csv` 로 저장한다. 그 파일이 Label Studio 태스크 생성의 입력이다.

embedding_dashboard.py 를 더 키우지 않으려고 분리했다 — 훅은 탭 한 줄이다.
"""

from __future__ import annotations

import csv
import os

import streamlit as st

import fiftyone_pgvector as fp

PICK_DIR = "/data/fiftyone/frames_bank/report/al_pick"
MEDIA_DIR = os.path.join(fp.MEDIA_DIR, "al_pick")


def _pick_files(strategy: str = "") -> list[str]:
    """`<pool>__<strategy>.csv` 중 해당 전략만. approved 산출물은 목록에서 뺀다."""
    if not os.path.isdir(PICK_DIR):
        return []
    fs = [f for f in os.listdir(PICK_DIR)
          if f.endswith(".csv") and not f.endswith(".approved.csv")
          and (not strategy or f.endswith(f"__{strategy}.csv"))]
    return sorted((os.path.join(PICK_DIR, f) for f in fs), key=os.path.getmtime, reverse=True)


@st.cache_data(show_spinner=False, ttl=300)
def _cohorts() -> tuple[list[str], list[str]]:
    """al_frames 에서 (GT 코호트, 미라벨 풀 코호트). 명령어에 실제 이름을 채워 넣기 위함 —
    `<gt_cohort>` 같은 플레이스홀더는 복붙이 안 돼 쓸모가 없다."""
    with fp._pg_conn() as c, c.cursor() as cur:
        cur.execute("""SELECT cohort, label_source, COUNT(*) FROM al_frames GROUP BY 1,2""")
        gt, pool = {}, {}
        for coh, src, n in cur.fetchall():
            (pool if src == "unlabeled" else gt if src in ("human", "derived") else {})[coh] = n
    fmt = lambda d: [f"{k}  ({v:,})" for k, v in sorted(d.items(), key=lambda kv: -kv[1])]  # noqa: E731
    return fmt(gt), fmt(pool)


@st.cache_data(show_spinner=False)
def _load(path: str, mtime: float) -> list[dict]:
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _fetch(rows: list[dict]) -> None:
    """media_uri(minio://bucket/key) → 로컬 캐시. 실패는 None 으로 두고 계속(부분 표시 허용)."""
    os.makedirs(MEDIA_DIR, exist_ok=True)
    mc = fp._minio_client()
    for r in rows:
        if r.get("_local") is not None:
            continue
        uri = r.get("media_uri", "")
        r["_local"] = None
        if not uri.startswith("minio://"):
            continue
        bucket, _, key = uri[len("minio://"):].partition("/")
        lp = os.path.join(MEDIA_DIR, f"{r['frame_key']}{os.path.splitext(key)[1] or '.jpg'}")
        if not os.path.exists(lp):
            try:
                mc.download_file(bucket, key, lp)
            except Exception:  # noqa: BLE001 — 객체 누락은 건너뛰고 나머지를 보여준다
                continue
        r["_local"] = lp


def render(uniform_thumb, strategy: str = "margin") -> None:
    """learner 계열 선택기의 산출물을 검토하고 승인 목록을 만든다.

    **점수 계산은 여기서 하지 않는다.** `al_select.py` 가 GT 로 프로브를 학습하고 풀을 채점하는데,
    그건 수 분짜리 작업이라 Streamlit 요청 안에서 돌리면 UI 가 멈춘다. 여기서는 산출된 CSV 만 읽고,
    없으면 만들 명령을 그대로 보여준다(복붙 실행).
    """
    st.caption(
        {"margin": "학습된 프로브가 **가장 헷갈린** 표본부터. 사람 GT 위 실측에서 25/25 전승·라벨 69% 절감.",
         "coverage": "이미 라벨된 것에서 **가장 먼** 표본부터. margin 보다 이득이 작고 균형 코호트에선 역효과."}
        .get(strategy, ""))
    files = _pick_files(strategy)
    if not files:
        st.info(f"`{strategy}` 산출물이 없습니다. 아래 명령을 그대로 복사해 실행하세요.")
        try:
            gts, pools = _cohorts()
        except Exception as exc:  # noqa: BLE001 — DB 미도달이어도 안내는 보여준다
            gts, pools = [], []
            st.warning(f"코호트 목록 조회 실패: {exc}")
        if gts and pools:
            g1, g2, g3 = st.columns([2, 2, 1])
            gt = g1.selectbox("학습에 쓸 GT 코호트", gts, key=f"gen_gt_{strategy}").split("  ")[0]
            pl = g2.selectbox("점수 매길 미라벨 풀", pools, key=f"gen_pool_{strategy}").split("  ")[0]
            tn = int(g3.number_input("TOPN", 50, 2000, 300, 50, key=f"gen_n_{strategy}"))
            st.code(
                f"docker exec -e GT_COHORTS={gt} -e POOL_COHORT={pl} "
                f"-e STRATEGY={strategy} -e TOPN={tn} \\\n"
                "  docker-analysis-1 python3 /workspace/al_select.py", language="bash")
            st.caption(f"산출: `{PICK_DIR}/{pl}__{strategy}.csv` · 수 분 걸립니다(프로브 학습 포함)")
        else:
            st.caption("al_frames 에 GT 또는 미라벨 풀 코호트가 없습니다.")
        return

    c1, c2, c3 = st.columns([3, 1, 1])
    path = c1.selectbox("선별 결과", files, format_func=os.path.basename)
    rows = [dict(r) for r in _load(path, os.path.getmtime(path))]
    show_n = c2.number_input("표시 수", 8, 400, 40, step=8)
    ncol = int(c3.number_input("열", 2, 6, 4))

    preds = sorted({r.get("pred", "") for r in rows if r.get("pred")})
    sel = st.multiselect("예측 클래스 필터", preds, default=preds)
    view = [r for r in rows if r.get("pred") in sel][: int(show_n)]

    # ── 요약: 이 목록이 '헷갈리는 것'을 골랐는지 한눈에 ──────────────────────
    m = [float(r["margin"]) for r in rows if r.get("margin")]
    a, b, c, d = st.columns(4)
    a.metric("전체", len(rows))
    b.metric("margin 중앙", f"{sorted(m)[len(m)//2]:.3f}" if m else "—")
    c.metric("영상 수", len({r.get("video") for r in rows}))
    from collections import Counter
    c4 = Counter(r.get("pred") for r in rows)
    d.metric("예측 분포", " / ".join(f"{k} {v}" for k, v in c4.most_common()))
    nvid = len({r.get("video") for r in rows})
    st.caption(
        "margin 이 **낮을수록** 모델이 헷갈린 표본입니다 — 목록 중앙값이 풀 전체 중앙값보다 "
        "확실히 낮아야 선택기가 제대로 동작한 것입니다. "
        f"**영상 집중도**: {len(rows)}장이 {nvid}개 영상에서 나왔습니다 "
        f"(장당 {len(rows)/max(nvid,1):.1f}). 소수 영상에 몰리면 유효표본이 줄어듭니다 — "
        "coverage 전략에서 특히 나타납니다(실측: margin 178영상 vs coverage 72영상)."
    )

    key_ex = f"al_excl::{os.path.basename(path)}"
    excl: set = st.session_state.setdefault(key_ex, set())

    _fetch(view)
    cols = st.columns(ncol)
    for i, r in enumerate(view):
        with cols[i % ncol]:
            if r.get("_local"):
                st.image(uniform_thumb(r["_local"]), use_container_width=True)
            else:
                st.warning("이미지 없음(MinIO 객체 누락)")
            st.caption(
                f"#{r.get('rank')} · **{r.get('pred')}** · margin {float(r.get('margin', 0)):.3f}\n\n"
                f"`{(r.get('video') or '')[:38]}` · SAM3 박스 {r.get('sam3_boxes')}"
            )
            fk = r["frame_key"]
            out = st.checkbox("제외", value=fk in excl, key=f"ex_{os.path.basename(path)}_{fk}")
            if out:
                excl.add(fk)
            else:
                excl.discard(fk)

    st.divider()
    keep = [r for r in rows if r["frame_key"] not in excl]
    st.write(f"승인 **{len(keep)}** / 제외 {len(excl)}")
    if st.button("✅ 승인 목록 저장", type="primary"):
        ap = path[:-4] + ".approved.csv"
        with open(ap, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=[k for k in rows[0] if not k.startswith("_")])
            w.writeheader()
            for r in keep:
                w.writerow({k: v for k, v in r.items() if not k.startswith("_")})
        st.success(f"{len(keep)}건 저장 → {ap}")
        st.code(f"docker exec docker-dagster-daemon-1 python3 /tmp/al_to_ls.py --approved {ap}",
                language="bash")
