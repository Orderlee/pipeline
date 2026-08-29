#!/usr/bin/env python3
"""A3 산출 CSV(23·23c·24·24b·24c) → 보고서 규약 차트 3장 (한글·버전순·상태색). 재계산 없음."""
import csv, glob, json
import numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt, matplotlib.font_manager as fm
OUT="/data/fiftyone/frames_bank/report/sourcei_gt"; FIG=f"{OUT}/fig"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family":"Noto Sans CJK JP","font.size":11,"axes.spines.top":False,"axes.spines.right":False,"axes.grid":True,
  "grid.color":"#e6e5e1","grid.linewidth":0.6,"axes.edgecolor":"#c3c2b7","figure.facecolor":"#fcfcfb","axes.facecolor":"#fcfcfb",
  "text.color":"#0b0b0b","axes.labelcolor":"#52514e","xtick.color":"#52514e","ytick.color":"#52514e","axes.unicode_minus":False})
CC={"normal":"#8a887f","falldown":"#eda100","fire":"#e34948","smoke":"#4a3aa7"}
rd=lambda n: list(csv.DictReader(open(f"{OUT}/csv/{n}",encoding="utf-8-sig")))
S=json.load(open(f"{OUT}/hubness_mi_summary.json")); H=S["hubness"]["global"]; M=S["mutual_information"]
skew=H["skewness_Nk"]; top1=H["top_1pct_slot_share"]; nk0=H["share_Nk_zero"]; top100=H["top_100_slot_share"]
# ── f26 허브니스 ───────────────────────────────────────────────────────
hist=rd("23b_hubness_dist.csv"); top=rd("23_hubness.csv")[:25]
fig,axes=plt.subplots(1,2,figsize=(17,8),gridspec_kw={"width_ratios":[1,1.5]})
ax=axes[0]; lo=np.array([float(r["bin_lo"]) for r in hist]); n=np.array([float(r["n_sentences"]) for r in hist]); hi=np.array([float(r["bin_hi"]) for r in hist])
ax.bar(range(len(lo)), n, color="#2a78d6"); ax.set_yscale("log")
ax.set_xticks(range(len(lo))); ax.set_xticklabels([f"{int(a)}~{int(b)-1}" if b-a>1 else f"{int(a)}" for a,b in zip(lo,hi)],rotation=45,ha="right",fontsize=8.5)
ax.set_xlabel("k-occurrence Nk = 배경 프레임 20,000 중 top-10 에 든 횟수"); ax.set_ylabel("문장 수 (log)")
ax.set_title(f"Nk 분포 — 문장 {nk0*100:.0f}% 는 top-10 에 0회\n상위 1%(1,216문장)가 슬롯 {top1*100:.1f}%, 상위 100문장이 {top100*100:.1f}%", loc="left", fontsize=10.5)
ax=axes[1]; cls_key=[k for k in top[0] if k.startswith("class")][0]; nk_key=[k for k in top[0] if k.startswith("Nk")][0]
y=np.arange(len(top)); ax.barh(y,[int(r[nk_key]) for r in top],color=[CC.get(r[cls_key],"#c3c2b7") for r in top])
for i,r in enumerate(top): ax.text(5,i,(r["text"][:78]+"…") if len(r["text"])>78 else r["text"],va="center",fontsize=8,color="#0b0b0b",bbox=dict(facecolor="#fcfcfb",alpha=.75,edgecolor="none",pad=1))
ax.set_yticks([]); ax.invert_yaxis(); ax.set_xlabel("Nk"); ax.set_title("허브 문장 상위 25 — 전부 normal 의 장소·카메라 서술\n(색 = 다수결 클래스)", loc="left", fontsize=10.5)
fig.suptitle(f"프롬프트 문장의 허브니스 — Nk 왜도 {skew:.1f}. 상위 100 허브 중 normal 98 / falldown 2. 'the alley from the cctv camera' 가 959 프레임의 top-10 에 등장\n"
             "허브 = 어떤 프레임이든 상위에 끼어드는 문장 = 문장 주효과의 실체. max 계열 규칙은 이 문장들에 판정을 넘긴다", x=0.01, ha="left", fontsize=12)
fig.tight_layout(); fig.savefig(f"{FIG}/f26_hubness.png",dpi=160); plt.close(fig)
# ── f26b 뱅크별 ────────────────────────────────────────────────────────
b=rd("23c_hubness_by_bank.csv"); vkey=lambda s: tuple(int(x) for x in s.lstrip("vV").split(".")); b.sort(key=lambda r: vkey(r["bank"]))
fig,axes=plt.subplots(1,2,figsize=(14,10),sharey=True)
y=np.arange(len(b)); es=np.array([float(r["effective_share"]) for r in b]); t1=np.array([float(r["top1pct_slot_share"]) for r in b]); z0=np.array([float(r["share_nk0"]) for r in b])
med=np.median(es)
axes[0].barh(y,es*100,color=["#e34948" if v<np.percentile(es,25) else ("#1baf7a" if v>np.percentile(es,75) else "#8a887f") for v in es])
axes[0].axvline(med*100,color="#52514e",ls=":",lw=1); axes[0].text(med*100,-0.8,f"중앙값 {med*100:.1f}%",fontsize=8.5,color="#52514e",ha="center")
axes[0].set_yticks(y); axes[0].set_yticklabels([r["bank"] for r in b],fontsize=8.5); axes[0].invert_yaxis(); axes[0].set_xlabel("유효 문장 비율 % = (1/Σp²) / 문장수  — 낮을수록 소수 허브가 뱅크를 지배")
axes[0].set_title("뱅크별 유효 문장 비율 (빨강 = 하위 25% 허브 지배, 초록 = 상위 25% 분산)", loc="left", fontsize=10.5)
for i,r in enumerate(b): axes[0].text(es[i]*100+0.1,i,f"{float(r['effective_sentences']):.0f}/{int(r['n_sentences']):,}",va="center",fontsize=7.5,color="#52514e")
axes[1].barh(y-0.2,t1*100,0.4,color="#2a78d6",label="상위 1% 문장이 차지한 슬롯 %"); axes[1].barh(y+0.2,z0*100,0.4,color="#c3c2b7",label="한 번도 안 뽑힌 문장 %")
axes[1].set_xlabel("%"); axes[1].legend(frameon=False,fontsize=9,loc="lower right"); axes[1].set_title("슬롯 집중도 / 미사용 문장", loc="left", fontsize=10.5)
fig.suptitle(f"전 뱅크 31종 허브니스 — 유효 문장은 뱅크의 {es.min()*100:.1f}~{es.max()*100:.1f}% (중앙값 {med*100:.1f}%). 가장 허브 지배적 {b[int(es.argmin())]['bank']}, 가장 분산 {b[int(es.argmax())]['bank']}\n"
             "배경 20,000 프레임 · 뱅크별 자기 문장 안에서 top-10. 노션 원 보고서의 '유효 문장 1.2~1.3%'(역-허핀달, 승자 기준)와 같은 방향", x=0.01, ha="left", fontsize=12)
fig.tight_layout(); fig.savefig(f"{FIG}/f26b_hubness_by_bank.png",dpi=160); plt.close(fig)
# ── f27 MI 히트맵 ──────────────────────────────────────────────────────
L=rd("24c_lift_matrix.csv"); fc=rd("24_frame_sentence_cluster_mi.csv"); scl=rd("24b_sentence_clusters.csv")
keys=[k for k in L[0] if k!="frame_cluster" and not k.startswith("n_")]; Lm=np.array([[float(r[k]) if r[k] not in ("","nan") else 0 for k in keys] for r in L])
ent={r["frame_cluster"]:float(r["entropy_bits"]) for r in fc}; nfr={r["frame_cluster"]:int(r["n_frames"]) for r in fc}
order=np.argsort([ent.get(r["frame_cluster"],9) for r in L])
words={r["cluster"]:r["top_words"] for r in scl}
xl=[" ".join(words.get(k.replace("sent_cluster_",""),k).split()[:3]) for k in keys]
fig,ax=plt.subplots(figsize=(15,13))
im=ax.imshow(np.log2(np.clip(Lm[order],1e-3,None)).clip(-2,2),cmap="RdBu_r",vmin=-2,vmax=2,aspect="auto")
ax.set_yticks(range(len(L))); ax.set_yticklabels([f"FC{L[i]['frame_cluster']} (H={ent.get(L[i]['frame_cluster'],0):.2f}, n={nfr.get(L[i]['frame_cluster'],0)})" for i in order],fontsize=7)
ax.set_xticks(range(len(keys))); ax.set_xticklabels(xl,rotation=45,ha="right",fontsize=8.5); ax.grid(False)
cb=fig.colorbar(im,ax=ax,shrink=.6); cb.set_label("log2(lift) = 관측/독립기대 (±2 클립, 짙은 파랑 = 관측 0)")
ax.set_xlabel("문장 군집 24 (MiniBatchKMeans, 상위 단어 3개)"); ax.set_ylabel("프레임 군집 64 (kmeans64, 엔트로피 낮은 순)")
ax.set_title(f"프레임 군집 × 문장 군집 — 각 배경 프레임의 top-1 문장이 속한 군집. MI {M['mi_observed_bits']:.2f} bits (H_frame 5.75 / H_sent 3.08), NMI {M['nmi_formula']:.2f} (sklearn {M['nmi_sklearn']:.2f}, AMI {M['ami_sklearn']:.2f})\n"
             f"순열 귀무 평균 {M['permutation_null_mean']:.2f} bits(유한표본 편향 ≈ (63·23)/(2N ln2) 와 일치) · z = {M['permutation_z_score']:.0f}. 프레임 {M['n_frames_with_cluster']:,}개(kmeans64 배정분). "
             "엔트로피 낮은 군집 = 문장 군집 하나에 잠김 (장소 어휘)", loc="left", fontsize=10.5)
fig.tight_layout(); fig.savefig(f"{FIG}/f27_frame_sentence_mi.png",dpi=160); plt.close(fig)
print("charts ok", skew, top1, nk0)
