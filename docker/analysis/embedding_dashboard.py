"""Embedding 시각화 대시보드 (Streamlit) — image_embeddings(pgvector) 한 페이지 + 데이터 증가 시 자동 반영.

실행(컨테이너): streamlit run embedding_dashboard.py --server.port 8501 --server.address 0.0.0.0
모든 그림은 fiftyone_pgvector 헬퍼 재사용. 무거운 계산은 임베딩 row 수(n)를 캐시 키로 둬서
데이터가 늘면(=n 변경) 자동 재계산. 추가로 auto-refresh 로 주기적 폴링.
"""

from __future__ import annotations

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


@st.cache_data(show_spinner=False)
def _nearest_df(et: str, k: int, n: int):
    import pandas as pd

    names, dmat, sizes = fp.cluster_distance_matrix(et, k)
    recs = []
    for i in range(k):
        nn = sorted((float(dmat[i][j]), j) for j in range(k) if j != i)[:3]
        recs.append(
            {
                "cluster": names[i],
                "n": sizes[i],
                "nearest-1": f"{names[nn[0][1]]} ({nn[0][0]:.3f})",
                "nearest-2": f"{names[nn[1][1]]} ({nn[1][0]:.3f})",
                "nearest-3": f"{names[nn[2][1]]} ({nn[2][0]:.3f})",
            }
        )
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

tab_comp, tab_proj, tab_dist, tab_near = st.tabs(
    ["📊 구성/비율", "🗺️ 투영 (UMAP/PCA/MDS)", "📐 클러스터 거리", "📋 최근접 표"]
)
with tab_comp:
    a, b = st.columns(2)
    a.plotly_chart(_fig("sunburst", et, k, n), use_container_width=True)
    b.plotly_chart(_fig("treemap", et, k, n), use_container_width=True)
with tab_proj:
    st.plotly_chart(_fig("proj", et, k, n), use_container_width=True)
    st.caption("UMAP=국소 분리 / PCA=선형·전역(explained variance 표기) / MDS=쌍거리 충실")
with tab_dist:
    a, b = st.columns(2)
    a.plotly_chart(_fig("heatmap", et, k, n), use_container_width=True)
    b.plotly_chart(_fig("mds", et, k, n), use_container_width=True)
    c, d = st.columns(2)
    c.plotly_chart(_fig("dendro", et, k, n), use_container_width=True)
    d.plotly_chart(_fig("network", et, k, n, th), use_container_width=True)
with tab_near:
    st.dataframe(_nearest_df(et, k, n), use_container_width=True, hide_index=True)
