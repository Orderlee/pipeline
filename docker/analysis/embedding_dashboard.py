"""Embedding 시각화 대시보드 (Streamlit) — image_embeddings(pgvector) 한 페이지 + 데이터 증가 시 자동 반영.

실행(컨테이너): streamlit run embedding_dashboard.py --server.port 8501 --server.address 0.0.0.0
모든 그림은 fiftyone_pgvector 헬퍼 재사용. 무거운 계산은 임베딩 row 수(n)를 캐시 키로 둬서
데이터가 늘면(=n 변경) 자동 재계산. 추가로 auto-refresh 로 주기적 폴링.
"""

from __future__ import annotations

import math
import os

import streamlit as st

import fiftyone_pgvector as fp

st.set_page_config(page_title="Embedding Dashboard", layout="wide")
REFRESH_SEC = int(os.environ.get("DASHBOARD_REFRESH_SEC", "60"))
try:
    from streamlit_autorefresh import st_autorefresh

    st_autorefresh(interval=REFRESH_SEC * 1000, key="auto")
except Exception:  # noqa: BLE001 — autorefresh 없으면 수동 새로고침
    pass


def _counts() -> dict:
    with fp._pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT entity_type, count(*) FROM image_embeddings GROUP BY 1")
        return {str(t): int(c) for t, c in cur.fetchall()}


# 캐시 키에 n(row 수) 포함 → 데이터 증가 시 캐시 미스 → 자동 재계산
@st.cache_data(show_spinner="계산 중...")
def _fig(kind: str, et: str, k: int, n: int, th: float = 0.25):
    if kind == "sunburst":
        return fp.cluster_composition(et, k, kind="sunburst")
    if kind == "treemap":
        return fp.cluster_composition(et, k, kind="treemap")
    if kind == "proj":
        return fp.projection_comparison(et, k)
    if kind == "heatmap":
        return fp.cluster_distance_heatmap(et, k)
    if kind == "mds":
        return fp.cluster_mds_scatter(et, k)
    if kind == "dendro":
        return fp.cluster_dendrogram(et, k)
    if kind == "network":
        return fp.cluster_network(et, k, threshold=th)
    raise ValueError(kind)


# 캐시 키 = (caption 수, frame 수) → 둘 중 하나라도 늘면 자동 재계산
@st.cache_data(show_spinner="정합 계산 중...")
def _alignment(cap_n: int, frm_n: int):
    return fp.caption_alignment_figure(baseline_n=2000)


# 텍스트→이미지/캡션 검색 (embedding-service /embed_text → pgvector cosine). 결과 이미지는 서버측 다운로드 캐시.
# 캐시 키에 필터/모드 포함 — 필터 변경 시 자동 캐시 미스
@st.cache_data(show_spinner="검색 중...")
def _search(
    query: str,
    et: str,
    k: int,
    source: str,
    image_role: str,
    daynight: str,
    environment: str,
    caption_mode: str,
    semantic_query: str,
):
    # None 으로 정규화 (빈 문자열 → 필터 없음 처리)
    src = source.strip() or None
    role = image_role or None
    dn = daynight or None
    env = environment or None

    if et == "caption":
        # caption 대상: caption_mode 에 따라 keyword/semantic/hybrid 라우팅
        return fp.search_captions(
            query, k=k,
            mode=caption_mode,
            semantic_query=semantic_query.strip() or None,
        )
    import os

    hits = fp.search_by_text(
        query, k=k,
        source=src, image_role=role,
        daynight_type=dn, environment_type=env,
    )
    mc = fp._minio_client()
    media = fp.MEDIA_DIR
    os.makedirs(media, exist_ok=True)
    for h in hits:
        bucket, key = h.get("image_bucket"), h.get("image_key")
        h["local"] = None
        if not bucket or not key:
            continue
        ext = os.path.splitext(key)[1] or ".jpg"
        lp = os.path.join(media, f"{h['image_id']}{ext}")
        if not os.path.exists(lp):
            try:
                mc.download_file(bucket, key, lp)
            except Exception:  # noqa: BLE001 — 객체 누락 skip
                lp = None
        h["local"] = lp
    return hits


DQ_CACHE_TTL_SEC = int(os.environ.get("DQ_CACHE_TTL_SEC", "300"))
AL_CACHE_TTL_SEC = int(os.environ.get("AL_CACHE_TTL_SEC", "600"))
HDBSCAN_CACHE_TTL_SEC = int(os.environ.get("HDBSCAN_CACHE_TTL_SEC", "300"))


@st.cache_data(show_spinner=False, ttl=DQ_CACHE_TTL_SEC)
def _dq_uniqueness(frm_n: int):
    try:
        import fiftyone as fo

        if not fo.dataset_exists("frames"):
            return []
        ds = fo.load_dataset("frames")
        vals = ds.values("uniqueness")
        return [v for v in vals if v is not None]
    except Exception as exc:  # noqa: BLE001
        print(f"_dq_uniqueness: {exc}")
        return []


@st.cache_data(show_spinner="클래스 분리도 계산 중...", ttl=DQ_CACHE_TTL_SEC)
def _dq_separation(frm_n: int):
    return fp.class_separation_report(), fp.class_separation_figure()


@st.cache_data(show_spinner="라벨 의심 스코어 계산 중...", ttl=DQ_CACHE_TTL_SEC)
def _dq_suspect(frm_n: int):
    return fp.compute_label_suspect()


def _clear_dq_caches() -> None:
    _dq_uniqueness.clear()
    _dq_separation.clear()
    _dq_suspect.clear()


def _class_separability_reason() -> str:
    """클래스 분리도가 비어있을 때의 '정확한' 사유를 FiftyOne 'frames' 에서 진단.

    class_separation_report 는 여러 사유로 {} 를 반환 → 사용자에겐 '필드 필요'로만 보여 오해 유발.
    여기서 필드 부재 / 라벨 부재 / 단일클래스(none) 를 구분해 안내한다.
    """
    try:
        from collections import Counter

        import fiftyone as fo

        if not fo.dataset_exists("frames"):
            return "FiftyOne 'frames' 데이터셋이 없습니다."
        ds = fo.load_dataset("frames")
        if "normalized_class" not in ds.get_field_schema():
            return (
                "이 데이터셋에는 클래스 라벨(normalized_class)이 아직 없습니다 — 검출/라벨 데이터가 적재돼야 "
                "attach_labels 가 채웁니다. (예: staging 은 image_labels=0 → 라벨 부재. prod 는 정상)"
            )
        cnt = Counter(v for v in ds.values("normalized_class") if v)
        labeled = {k: c for k, c in cnt.items() if k != "none"}
        if len(labeled) < 2:
            return (
                f"라벨된 클래스가 {len(labeled)}개뿐입니다 (분포 {dict(cnt)}). 분리도는 'none' 제외 "
                "≥2 클래스에서만 측정됩니다 — 검출/라벨 데이터가 더 필요합니다."
            )
        return "유효 라벨 샘플이 부족합니다(클래스당 표본 부족)."
    except Exception as exc:  # noqa: BLE001 — 사유 진단도 실패하면 일반 안내
        return f"사유 진단 실패: {exc}"


@st.cache_data(show_spinner="능동학습 큐 계산 중...", ttl=AL_CACHE_TTL_SEC)
def _al_queue(frm_n: int, n: int = 200, model_name: str = fp.DEFAULT_MODEL):
    return fp.active_learning_queue(n=n, model_name=model_name)


@st.cache_data(show_spinner="캡션 키워드 앵커링 분석 중...", ttl=AL_CACHE_TTL_SEC)
def _caption_quality(frm_n: int, model_name: str = fp.DEFAULT_MODEL):
    return fp.caption_quality_report(model_name=model_name)


def _clear_al_caches() -> None:
    _al_queue.clear()
    _caption_quality.clear()


@st.cache_data(show_spinner="HDBSCAN 클러스터링 중...", ttl=HDBSCAN_CACHE_TTL_SEC)
def _hdbscan(et: str, n: int, mcs: int):
    # HDBSCAN 1회만 계산: report 를 figure 에 재사용 (이중 계산 + autorefresh마다 uncached 재계산 방지)
    rep = fp.hdbscan_report(entity_type=et, min_cluster_size=mcs)
    fig = fp.hdbscan_figure(entity_type=et, min_cluster_size=mcs, report=rep)
    return rep, fig


@st.cache_data(show_spinner="이미지 검색 중...", ttl=DQ_CACHE_TTL_SEC)
def _search_image(image_id: str, k: int, source: str, image_role: str, daynight: str, environment: str):
    src = source.strip() or None
    role = image_role or None
    dn = daynight or None
    env = environment or None
    hits = fp.search_by_image(
        image_id, k=k,
        source=src, image_role=role,
        daynight_type=dn, environment_type=env,
    )
    mc = fp._minio_client()
    media = fp.MEDIA_DIR
    os.makedirs(media, exist_ok=True)
    for h in hits:
        bucket, key = h.get("image_bucket"), h.get("image_key")
        h["local"] = None
        if not bucket or not key:
            continue
        ext = os.path.splitext(key)[1] or ".jpg"
        lp = os.path.join(media, f"{h['image_id']}{ext}")
        if not os.path.exists(lp):
            try:
                mc.download_file(bucket, key, lp)
            except Exception:  # noqa: BLE001 — 객체 누락 skip
                lp = None
        h["local"] = lp
    return hits


@st.cache_data(show_spinner="업로드 이미지 검색 중...", ttl=DQ_CACHE_TTL_SEC)
def _search_upload(file_bytes: bytes, k: int, source: str, image_role: str, daynight: str, environment: str):
    """업로드 이미지 바이트 → fp.search_by_uploaded_image → 결과 썸네일 다운로드 캐시.

    캐시 키: (file_bytes, k, source, image_role, daynight, environment) — 필터 포함.
    반환 shape: _search_image 와 동일 (h["local"] 포함).
    """
    src = source.strip() or None
    role = image_role or None
    dn = daynight or None
    env = environment or None
    hits = fp.search_by_uploaded_image(
        file_bytes, k=k,
        source=src, image_role=role,
        daynight_type=dn, environment_type=env,
    )
    mc = fp._minio_client()
    media = fp.MEDIA_DIR
    os.makedirs(media, exist_ok=True)
    for h in hits:
        bucket, key = h.get("image_bucket"), h.get("image_key")
        h["local"] = None
        if not bucket or not key:
            continue
        ext = os.path.splitext(key)[1] or ".jpg"
        lp = os.path.join(media, f"{h['image_id']}{ext}")
        if not os.path.exists(lp):
            try:
                mc.download_file(bucket, key, lp)
            except Exception:  # noqa: BLE001 — 객체 누락 skip
                lp = None
        h["local"] = lp
    return hits


def _pivot_to_image(image_id: str) -> None:
    """pivot 콜백 — 이 이미지로 유사검색 전환.

    on_click 콜백은 rerun 시작 시(위젯 인스턴스화 전) 실행되므로 위젯-key session_state 설정 가능.
    if st.button(): 블록에서 직접 설정하면 이미 인스턴스화된 위젯이라 StreamlitAPIException 발생 → 콜백 사용.
    """
    st.session_state["search_mode"] = "이미지 ID"
    st.session_state["img_search_id"] = image_id


# 검색 결과 썸네일 고정 캔버스 — 원본 가로세로비가 제각각이면 그리드가 들쑥날쑥 →
# 모든 썸네일을 동일한 16:9 캔버스에 레터박스(비율유지 축소 + 중앙 배치)해 셀 높이를 균일화.
THUMB_W, THUMB_H = 480, 270  # 16:9 (THUMB_AR 변경 시 함께 수정)
THUMB_BG = (17, 17, 17)  # 레터박스 여백 색(다크). 원본은 잘리지 않음.


def _uniform_thumb(path: str) -> str:
    """이미지를 고정 16:9 캔버스에 레터박스한 썸네일 경로 반환 (그리드 셀 균일화).

    비율 유지 축소 후 중앙 배치 → 모든 결과가 동일 크기 = 4열 그리드 정렬.
    원본 내용 잘림 없음(여백만 추가). PIL 미설치/실패 시 원본 경로로 graceful fallback.
    썸네일은 원본 옆에 캐시되어 재계산 안 함.
    """
    try:
        from PIL import Image

        tp = f"{os.path.splitext(path)[0]}.grid_{THUMB_W}x{THUMB_H}.jpg"
        if os.path.exists(tp) and os.path.getmtime(tp) >= os.path.getmtime(path):
            return tp
        im = Image.open(path).convert("RGB")
        im.thumbnail((THUMB_W, THUMB_H), Image.LANCZOS)  # 비율 유지 축소
        canvas = Image.new("RGB", (THUMB_W, THUMB_H), THUMB_BG)
        canvas.paste(im, ((THUMB_W - im.width) // 2, (THUMB_H - im.height) // 2))
        canvas.save(tp, "JPEG", quality=85)
        return tp
    except Exception:  # noqa: BLE001 — PIL 미설치/손상 이미지 등 → 원본 표시
        return path


def _render_frame_hits(hits: list, mode_tag: str) -> None:
    """프레임 검색 결과를 4열 썸네일 그리드로 렌더링.

    각 썸네일 아래 "🔍 이걸로 유사검색" 버튼 제공(pivot):
      클릭 → search_mode="이미지 ID", img_search_id=<image_id> 로 설정 후 st.rerun().
    mode_tag: 버튼 key 충돌 방지용 접두사 (예: "txt", "imgid", "upload").
    썸네일은 _uniform_thumb 로 고정 캔버스화 → 가로세로비 차이로 인한 레이아웃 들쑥날쑥 방지.
    """
    ncol = 4
    cols = st.columns(ncol)
    for i, h in enumerate(hits):
        sim = 1 - float(h.get("cosine_dist", 1))
        image_id = h.get("image_id", "")
        with cols[i % ncol]:
            if h.get("local"):
                st.image(_uniform_thumb(h["local"]), caption=f"sim={sim:.3f}", use_container_width=True)
            else:
                st.write(f"sim={sim:.3f} · {(h.get('image_key') or '')[:50]}")
            # pivot 버튼: 이 이미지로 유사검색 전환 (on_click 콜백 = 위젯 생성 전 실행 → 안전)
            btn_key = f"pivot_{mode_tag}_{i}_{image_id}"
            st.button(
                "🔍 이걸로 유사검색", key=btn_key, use_container_width=True,
                on_click=_pivot_to_image, args=(image_id,),
            )


@st.cache_data(show_spinner=False)
def _nearest_df(et: str, k: int, n: int):
    import pandas as pd

    names, dmat, sizes = fp.cluster_distance_matrix(et, k)
    # 이웃은 최대 3개, 단 다른 클러스터 수(k-1)를 넘지 않음 → k<4 일 때 IndexError 방지
    top = min(3, max(0, k - 1))
    recs = []
    for i in range(k):
        nn = sorted((float(dmat[i][j]), j) for j in range(k) if j != i)[:top]
        rec = {"cluster": names[i], "n": sizes[i]}
        for r in range(top):
            rec[f"nearest-{r + 1}"] = f"{names[nn[r][1]]} ({nn[r][0]:.3f})"
        recs.append(rec)
    return pd.DataFrame(recs)


st.title("🔎 Embedding 시각화 대시보드")
counts = _counts()
c1, c2, c3 = st.columns(3)
c1.metric("frame embeddings", counts.get("frame", 0))
c2.metric("caption embeddings", counts.get("caption", 0))
c3.metric("total", sum(counts.values()))

with st.sidebar:
    st.header("설정")
    et = st.selectbox("entity_type", ["frame", "caption"], index=0)
    n = counts.get(et, 0)
    kmax = max(2, min(20, n))
    k = st.slider("클러스터 수 (k)", 2, max(3, kmax), min(12, kmax))
    th = st.slider("network edge threshold", 0.05, 0.6, 0.25, 0.05)
    st.caption(f"{et}: {n} embeddings · auto-refresh {REFRESH_SEC}s")
    st.caption("데이터가 늘면(n 변경) 자동 재계산됩니다.")

if n < k:
    st.warning(f"{et} 임베딩이 {n}개로 k={k} 보다 적습니다. k 를 줄이거나 데이터 적재를 기다리세요.")
    st.stop()

# 탭 콘텐츠 lazy 렌더 — st.tabs 는 매 rerun '모든' 탭(무거운 UMAP/거리/품질/능동학습/HDBSCAN 포함)을
# 계산하므로 대용량(예: prod 188K)에선 cache-miss 시 첫 렌더가 매우 느림. 선택자로 '선택된 탭만' 계산.
TAB_NAMES = [
    "📊 구성/비율", "🗺️ 투영 (UMAP/PCA/MDS)", "📐 클러스터 거리",
    "📋 최근접 표", "🔗 캡션↔이미지 정합", "🔎 텍스트 검색", "🩺 데이터 품질",
    "🎯 능동학습", "🧩 HDBSCAN",
]
_active = st.segmented_control(
    "보기", TAB_NAMES, default=TAB_NAMES[0],
    selection_mode="single", key="active_tab", label_visibility="collapsed",
)
if not _active:  # 단일 모드에서 활성 항목 재클릭 시 None → 첫 탭 유지
    _active = TAB_NAMES[0]
if _active == "📊 구성/비율":
    a, b = st.columns(2)
    a.plotly_chart(_fig("sunburst", et, k, n), use_container_width=True)
    b.plotly_chart(_fig("treemap", et, k, n), use_container_width=True)
if _active == "🗺️ 투영 (UMAP/PCA/MDS)":
    st.plotly_chart(_fig("proj", et, k, n), use_container_width=True)
    st.caption("UMAP=국소 분리 / PCA=선형·전역(explained variance 표기) / MDS=쌍거리 충실")
if _active == "📐 클러스터 거리":
    a, b = st.columns(2)
    a.plotly_chart(_fig("heatmap", et, k, n), use_container_width=True)
    b.plotly_chart(_fig("mds", et, k, n), use_container_width=True)
    c, d = st.columns(2)
    c.plotly_chart(_fig("dendro", et, k, n), use_container_width=True)
    d.plotly_chart(_fig("network", et, k, n, th), use_container_width=True)
if _active == "📋 최근접 표":
    st.dataframe(_nearest_df(et, k, n), use_container_width=True, hide_index=True)
if _active == "🔗 캡션↔이미지 정합":
    cap_n, frm_n = counts.get("caption", 0), counts.get("frame", 0)
    if cap_n == 0 or frm_n == 0:
        st.info("caption 과 frame 임베딩이 둘 다 있어야 정합을 측정할 수 있습니다.")
    else:
        fig, summ = _alignment(cap_n, frm_n)
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("matched μ (자기영상)", f"{summ['matched_mean']:.3f}")
        m2.metric("mismatched μ (다른영상)", f"{summ['mismatched_mean']:.3f}")
        _gap = summ.get("gap")
        m3.metric("gap", f"{_gap:.3f}" if isinstance(_gap, float) and math.isfinite(_gap) else "n/a")
        m4.metric("측정 캡션 / 영상 수", f"{summ['n_matched']} / {summ['n_assets']}")
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            "matched=캡션↔자기 영상 프레임 cosine, mismatched=캡션↔무작위 다른 영상. "
            "gap 이 클수록(matched 가 오른쪽으로 분리될수록) 캡션 임베딩이 자기 이미지에 잘 정렬됨. "
            "⚠️ caption+frame 둘 다 임베딩된 영상만 측정 가능 — 현재 prod 는 겹치는 영상이 적어 표본이 작음. "
            "또한 캡션이 한국어로 임베딩된 동안은 텍스트 인코더 degenerate 로 gap≈0 → 영어 재임베딩 후 분리됨."
        )
if _active == "🔎 텍스트 검색":
    search_mode = st.radio(
        "검색 방식",
        ["텍스트", "이미지 ID", "이미지 업로드"],
        horizontal=True,
        key="search_mode",
    )

    # ── 공통 메타데이터 필터 expander (frame 검색 모드 및 caption 검색에 모두 적용) ──
    with st.expander("필터 설정 (선택)", expanded=False):
        f1, f2, f3, f4 = st.columns(4)
        flt_source = f1.text_input(
            "source (prefix)", key="flt_source", placeholder="seah",
            help="image_key LIKE 'prefix%' 필터. 비워두면 전체.",
        )
        flt_role = f2.selectbox(
            "image_role", ["", "raw_video_frame", "processed_clip_frame", "source_image"],
            key="flt_role", help="image_metadata.image_role 필터.",
        )
        flt_dn = f3.selectbox(
            "daynight_type", ["", "day", "night", "indoor"],
            key="flt_daynight", help="video_metadata.daynight_type 필터 (prod 적용, staging에서는 None).",
        )
        flt_env = f4.selectbox(
            "environment_type", ["", "outdoor", "indoor", "mixed"],
            key="flt_env", help="video_metadata.environment_type 필터 (prod 적용, staging에서는 None).",
        )

    if search_mode == "텍스트":
        st.caption(
            "텍스트 → 이미지/캡션 검색 (embedding-service /embed_text → pgvector cosine). "
            "**영어 쿼리 권장** (PE-Core 텍스트 인코더). caption 대상은 한국어 keyword 검색 지원."
        )
        cq1, cq2, cq3 = st.columns([3, 1, 1])
        q = cq1.text_input("검색어", key="search_q", placeholder="a person falling on the floor")
        et2 = cq2.radio("대상", ["frame", "caption"], key="search_et")
        topk = cq3.slider("결과 수", 4, 40, 12, key="search_k")

        # caption 대상일 때 검색 모드 + semantic_query 추가 입력
        cap_mode = "keyword"
        cap_sem_q = ""
        if et2 == "caption":
            st.caption(
                "caption 검색 모드: **keyword** = 한국어 ILIKE+trigram (기본), "
                "**semantic** = 벡터 kNN (영어 권장), "
                "**hybrid** = RRF 융합."
            )
            cap_mode = st.radio(
                "caption 검색 모드", ["keyword", "semantic", "hybrid"],
                horizontal=True, key="cap_search_mode",
            )
            if cap_mode in ("semantic", "hybrid"):
                cap_sem_q = st.text_input(
                    "영어 semantic 쿼리 (선택, 비우면 위 검색어 사용)",
                    key="cap_sem_q",
                    placeholder="person falling down (영어 권장)",
                )

        if q and q.strip():
            try:
                hits = _search(
                    q.strip(), et2, topk,
                    flt_source, flt_role, flt_dn, flt_env,
                    cap_mode, cap_sem_q,
                )
            except Exception as exc:  # noqa: BLE001 — 검색 실패 표시
                st.error(f"검색 실패: {exc}")
                hits = []
            if not hits:
                st.info("결과 없음.")
            elif et2 == "frame":
                _render_frame_hits(hits, "txt")
            else:
                # caption 결과 렌더링: mode에 따라 표시 필드 다름
                for h in hits:
                    rrf = h.get("rrf_score")
                    cosine_d = h.get("cosine_dist")
                    lex = h.get("lexical_score")
                    sub = h.get("substring_hit", False)
                    score_parts = []
                    if rrf is not None:
                        score_parts.append(f"rrf={rrf:.4f}")
                    if cosine_d is not None:
                        score_parts.append(f"sem_sim={1 - float(cosine_d):.3f}")
                    if lex:
                        score_parts.append(f"lex={float(lex):.3f}")
                    if sub:
                        score_parts.append("substring✓")
                    score_str = " · ".join(score_parts) if score_parts else ""
                    text = h.get("caption_text") or h.get("text_content") or ""
                    st.markdown(f"**{score_str}** — {text}")
        else:
            st.info(
                "검색어를 입력하세요 (frame: 영어 권장, caption keyword: 한국어 가능). "
                "예: 'a fire on the street', '넘어진 사람'."
            )
    elif search_mode == "이미지 ID":
        st.caption("이미지 ID → 가장 유사한 프레임 검색 (pgvector cosine, precomputed embedding).")
        img_id_input = st.text_input("image_id", key="img_search_id", placeholder="uuid")
        img_k = st.slider("결과 수 (k)", 1, 20, 5, key="img_search_k")
        if img_id_input and img_id_input.strip():
            try:
                img_hits = _search_image(
                    img_id_input.strip(), img_k,
                    flt_source, flt_role, flt_dn, flt_env,
                )
            except Exception as exc:  # noqa: BLE001
                err_msg = str(exc)
                if "no embedding" in err_msg.lower():
                    st.info("해당 이미지 ID를 찾을 수 없습니다.")
                else:
                    st.error(f"이미지 검색 실패: {exc}")
                img_hits = []
            if img_hits:
                _render_frame_hits(img_hits, "imgid")
            elif img_id_input.strip():
                st.info("결과 없음.")
        else:
            st.info("image_id 를 입력하세요.")
    else:
        # ── 이미지 업로드 검색 ──
        st.caption(
            "이미지 파일 업로드 → embedding-service /embed → pgvector cosine top-k. "
            "PE-Core 이미지 인코더 사용 (이미지→이미지 cross-instance 유사검색)."
        )
        uploaded = st.file_uploader(
            "이미지 업로드", type=["jpg", "jpeg", "png"], key="img_upload"
        )
        up_k = st.slider("결과 수 (k)", 1, 40, 12, key="upload_search_k")
        if uploaded is not None:
            file_bytes = uploaded.read()
            # 쿼리 이미지 미리보기
            st.image(file_bytes, caption=f"쿼리 이미지: {uploaded.name}", width=240)
            try:
                up_hits = _search_upload(file_bytes, up_k, flt_source, flt_role, flt_dn, flt_env)
            except Exception as exc:  # noqa: BLE001 — embedding-service 다운 등 fail-forward
                st.error(f"업로드 이미지 임베딩/검색 실패: {exc}")
                up_hits = []
            if up_hits:
                st.caption(f"유사 프레임 {len(up_hits)}개")
                _render_frame_hits(up_hits, "upload")
            else:
                st.info("결과 없음.")
        else:
            st.info("JPG/PNG 이미지를 업로드하세요.")
if _active == "🩺 데이터 품질":
    if st.button("🔄 재계산", key="dq_recompute"):
        _clear_dq_caches()
        st.rerun()
    frm_n_dq = counts.get("frame", 0)
    st.caption(
        "데이터 중심 AI 품질 신호 — 임베딩 공간 기반 라벨 검수·클래스 분리도 측정. "
        "normalized_class = taxonomy 정규화 (fall/fire/smoke). "
        "이 지표들을 활용해 모델 학습 전 데이터셋 품질을 높이세요."
    )

    # ── uniqueness (중복/유사 프레임) ──
    st.subheader("uniqueness (중복/유사 프레임)")
    uniqueness_vals = _dq_uniqueness(frm_n_dq)
    if uniqueness_vals:
        import plotly.graph_objects as go

        near_dup_count = sum(1 for v in uniqueness_vals if v < 0.15)
        u1, u2 = st.columns(2)
        u1.metric("전체 프레임", len(uniqueness_vals))
        u2.metric("유사 중복 (uniqueness < 0.15)", near_dup_count)
        fig_u = go.Figure(go.Histogram(x=uniqueness_vals, nbinsx=40, marker_color="#1f77b4"))
        fig_u.update_layout(
            title="프레임 uniqueness 분포",
            xaxis_title="uniqueness",
            yaxis_title="count",
            width=780,
            height=360,
        )
        st.plotly_chart(fig_u, use_container_width=True)
        st.caption(
            "uniqueness=1: 임베딩 공간에서 고립 (독특). uniqueness≈0: 거의 동일한 프레임 다수 존재. "
            "0.15 미만은 사실상 중복 — 학습 데이터 과대표 위험."
        )
    else:
        st.info("uniqueness 값 없음. FiftyOne 'frames' 데이터셋에서 fob.compute_uniqueness(ds) 를 먼저 실행하세요.")

    st.divider()

    # ── 클래스 분리도 ──
    st.subheader("클래스 분리도 (class separability)")
    if frm_n_dq == 0:
        st.info("frame 임베딩이 없습니다.")
    else:
        try:
            sep_rep, sep_fig = _dq_separation(frm_n_dq)
            if sep_rep:
                sil_val = sep_rep.get("silhouette")
                sil_str = f"{sil_val:.3f}" if sil_val is not None else "n/a"
                sc1, sc2 = st.columns(2)
                sc1.metric("silhouette score (cosine)", sil_str, help="1에 가까울수록 클래스 분리 잘됨 (0.2↑ 양호)")
                sc2.metric("분석 프레임 수", sep_rep.get("n", 0))
                st.plotly_chart(sep_fig, use_container_width=True)
                purity = sep_rep.get("purity", {})
                if purity:
                    import pandas as pd

                    purity_df = pd.DataFrame(
                        [{"class": c, "purity (kNN=10)": v, "count": sep_rep["counts"].get(c, 0)}
                         for c, v in sorted(purity.items(), key=lambda x: -x[1])]
                    )
                    st.dataframe(purity_df, use_container_width=True, hide_index=True)
                    st.caption("purity=kNN-10 이웃 중 같은 클래스 비율. 낮을수록 다른 클래스와 혼동 가능성.")
            else:
                st.info("클래스 분리도 계산 불가 — " + _class_separability_reason())
                st.caption(
                    "클래스 분리도는 라벨된 클래스(fall/fire/smoke 등) 간 임베딩 분리를 측정합니다. "
                    "라벨이 있는 prod 'frames'(fall/fire/smoke/none)에서 동작합니다."
                )
        except Exception as exc:  # noqa: BLE001 — 분리도 계산 실패 시 표시
            st.warning(f"클래스 분리도 계산 실패: {exc}")

    st.divider()

    # ── 라벨 의심 (label suspect) ──
    st.subheader("라벨 의심 (label suspect)")
    if frm_n_dq == 0:
        st.info("frame 임베딩이 없습니다.")
    else:
        try:
            suspect_result = _dq_suspect(frm_n_dq)
            if suspect_result.get("truncated"):
                st.warning(
                    f"⚠️ 전체 {suspect_result.get('n','?')}개 중 DQ_FULL_MATRIX_MAX_N="
                    f"{fp.DQ_FULL_MATRIX_MAX_N}개만 분석된 부분 결과입니다."
                )
            suspect_rows = suspect_result.get("rows", [])
            if suspect_rows:
                import pandas as pd

                sus_df = pd.DataFrame(suspect_rows)[
                    ["image_id", "label", "neighbor_majority", "knn_disagree", "suspect_score"]
                ]
                st.dataframe(sus_df, use_container_width=True, hide_index=True)
                st.caption(
                    "kNN 이웃 다수가 다른 클래스 = 라벨오류/혼동 후보 (검수 우선순위). "
                    "suspect_score = 0.6×knn_disagree + 0.4×caption_contradiction (0~1)."
                )
            else:
                st.info(
                    "라벨 의심 결과 없음 (FiftyOne 'frames' 에 normalized_class 또는 detection_class 필드 필요)."
                )
        except Exception as exc:  # noqa: BLE001 — 의심 스코어 계산 실패 시 표시
            st.warning(f"라벨 의심 스코어 계산 실패: {exc}")

if _active == "🎯 능동학습":
    if st.button("🔄 재계산", key="al_recompute"):
        _clear_al_caches()
        st.rerun()
    frm_n_al = counts.get("frame", 0)
    st.caption(
        "능동학습(Active Learning) 및 캡션 품질 교차검증 — 라벨 없는 프레임 중 희귀 이벤트와 가까운 것을 우선 레이블링."
    )

    # ── 능동학습 큐 ──
    st.subheader("🎯 능동학습 큐 (Active Learning Queue)")
    al_n = st.slider("큐 크기 (n)", 50, 500, 200, 50, key="al_n")
    if frm_n_al == 0:
        st.info("frame 임베딩이 없습니다.")
    else:
        try:
            al_result = _al_queue(frm_n_al, al_n, fp.DEFAULT_MODEL)
            al_rows = al_result.get("rows", [])
            n_candidates = al_result.get("n_candidates", 0)
            al1, al2 = st.columns(2)
            al1.metric("후보(none) 프레임 수", n_candidates)
            al2.metric(f"큐 상위 {al_n}개 추출됨", len(al_rows))
            if al_rows:
                import pandas as pd

                al_df = pd.DataFrame(al_rows)[
                    ["image_id", "al_score", "reason", "nearest_rare_class",
                     "rare_sim", "representativeness", "uniqueness", "minio_key"]
                ]
                st.dataframe(al_df, use_container_width=True, hide_index=True)
                st.caption(
                    "al_score = 0.45×rare_sim + 0.30×representativeness + 0.25×uniqueness. "
                    "reason: near-fire/smoke=희귀이벤트 근접, edge/unique=고유 프레임, representative=대표 프레임. "
                    "다양성 보장: top-n의 최소 20%는 non-near-rare로 채워짐."
                )
                reason_counts = al_result.get("reason_counts", {})
                if reason_counts:
                    st.json(reason_counts)
                truncated_flag = al_result.get("truncated", False)
                if truncated_flag:
                    st.warning(
                        f"⚠️ 전체 프레임 수 > DQ_FULL_MATRIX_MAX_N={fp.DQ_FULL_MATRIX_MAX_N}; "
                        "부분 결과입니다 (image_id 순 앞부분)."
                    )
                # CSV 내보내기 버튼
                import csv
                import io

                csv_buf = io.StringIO()
                writer = csv.DictWriter(
                    csv_buf,
                    fieldnames=["image_id", "minio_key", "al_score", "reason", "nearest_rare_class"],
                    extrasaction="ignore",
                )
                writer.writeheader()
                writer.writerows(al_rows)
                st.download_button(
                    label="📥 CSV 다운로드",
                    data=csv_buf.getvalue().encode("utf-8"),
                    file_name="labeling_queue.csv",
                    mime="text/csv",
                    key=f"al_csv_download_{al_n}",
                )
            else:
                st.info(
                    "능동학습 큐 없음. FiftyOne 'frames' 에 normalized_class, uniqueness, representativeness 필드 필요."
                )
        except Exception as exc:  # noqa: BLE001 — AL 계산 실패 표시
            st.warning(f"능동학습 큐 계산 실패: {exc}")

    st.divider()

    # ── 캡션 키워드 앵커링 분석 ──
    st.subheader("💬 캡션 키워드 앵커링 분석")
    if frm_n_al == 0:
        st.info("frame 임베딩이 없습니다.")
    else:
        try:
            cq_result = _caption_quality(frm_n_al, fp.DEFAULT_MODEL)
            overall_rate = cq_result.get("overall_keyword_anchor_rate", 0.0)
            by_class = cq_result.get("by_class", {})
            examples_missing = cq_result.get("examples_missing", [])

            cq1, cq2 = st.columns(2)
            cq1.metric(
                "키워드 앵커링률",
                f"{overall_rate:.1%}",
                help="캡션 품질 점수가 아닙니다. canonical 키워드가 직접 등장한 비율입니다.",
            )
            total_labeled_cnt = sum(v["n"] for v in by_class.values()) if by_class else 0
            cq2.metric("분석 라벨 프레임 수", total_labeled_cnt)

            if by_class:
                import pandas as pd

                cq_df = pd.DataFrame([
                    {
                        "class": cls,
                        "n": stats["n"],
                        "캡션 키워드 있음": stats["caption_has_matching_kw"],
                        "캡션 키워드 없음": stats["caption_missing_kw"],
                        "keyword_anchor_rate": f"{stats['keyword_anchor_rate']:.1%}",
                    }
                    for cls, stats in sorted(by_class.items())
                ])
                st.dataframe(cq_df, use_container_width=True, hide_index=True)
                st.caption(
                    "keyword_anchor_rate = 캡션에 canonical 키워드가 직접 등장한 비율 (품질 점수 아님). "
                    "Gemini 자유형 한국어 캡션은 canonical 키워드 없이도 정확할 수 있어 낮은 값은 정상. "
                    "키워드 매핑: fall→fall/넘어/쓰러/낙상, fire→fire/flame/화재, smoke→smoke/연기."
                )
            else:
                st.info(
                    "캡션 키워드 앵커링 결과 없음. FiftyOne 'frames' 에 normalized_class + caption 필드 필요."
                )

            if examples_missing:
                with st.expander(f"키워드 없이 맥락 표현만 사용된 예시 ({len(examples_missing)}개) (정상일 수 있음)"):
                    import pandas as pd

                    ex_df = pd.DataFrame(examples_missing)[["image_id", "normalized_class", "caption"]]
                    st.dataframe(ex_df, use_container_width=True, hide_index=True)
                    st.caption(
                        "이 프레임들은 라벨은 있지만 캡션에 canonical 키워드가 없음. "
                        "맥락 표현(예: '쓰러진 사람' 대신 '바닥에 누운 사람')이 사용된 경우 정상."
                    )

        except Exception as exc:  # noqa: BLE001 — 캡션 앵커링 계산 실패 표시
            st.warning(f"캡션 키워드 앵커링 분석 실패: {exc}")

if _active == "🧩 HDBSCAN":
    if st.button("🔄 재계산", key="hdbscan_recompute"):
        _hdbscan.clear()
        st.rerun()
    mcs = st.slider("min_cluster_size", 5, 50, 15, key="hdbscan_mcs")
    hdb_n = counts.get(et, 0)
    if hdb_n == 0:
        st.info(f"{et} 임베딩이 없습니다.")
    else:
        try:
            hdb_rep, hdb_fig = _hdbscan(et, hdb_n, mcs)
            if hdb_rep:
                hm1, hm2 = st.columns(2)
                hm1.metric("클러스터 수", hdb_rep.get("n_clusters", 0))
                hm2.metric("noise 비율", f"{hdb_rep.get('noise_frac', 0):.1%}")
            st.plotly_chart(hdb_fig, use_container_width=True)
        except Exception as exc:  # noqa: BLE001 — HDBSCAN 계산 실패 표시
            st.warning(f"HDBSCAN 계산 실패: {exc}")
    st.caption(
        "밀도 기반 클러스터링. noise=저밀도 미할당 포인트. "
        "KMeans와 달리 클러스터 수 자동+noise 분리."
    )
