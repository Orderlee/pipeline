#!/usr/bin/env python3
"""B2 보정 — 클래스별 PCA 스펙트럼을 전 성분(1..100)으로 다시 계산해 규약대로 그리고, 25_class_pca.csv 의 n_for_90pct 버그(미도달→1) 를 '>100' 으로 고친다."""
import os, sys, csv, json, glob, collections
os.environ.setdefault("COS_THREADS","2"); sys.path.insert(0,"/workspace")
from prompt_cos_db import load_sentence_vectors
import numpy as np, psycopg2, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from sklearn.decomposition import PCA
OUT="/data/fiftyone/frames_bank/report/sourcei_gt"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family":"Noto Sans CJK JP","font.size":11,"axes.spines.top":False,"axes.spines.right":False,"axes.grid":True,"grid.color":"#e6e5e1",
 "grid.linewidth":0.6,"axes.edgecolor":"#c3c2b7","figure.facecolor":"#fcfcfb","axes.facecolor":"#fcfcfb","text.color":"#0b0b0b","axes.labelcolor":"#52514e","xtick.color":"#52514e","ytick.color":"#52514e","axes.unicode_minus":False})
CC={"normal":"#8a887f","falldown":"#eda100","fire":"#e34948","smoke":"#4a3aa7"}; CLASSES=["normal","falldown","fire","smoke"]
cur=psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
h2c,SENT=load_sentence_vectors(cur)
cur.execute("SELECT content_hash, class_label, count(*) FROM bank_sentences GROUP BY 1,2")
votes=collections.defaultdict(dict)
for h,c,n in cur: votes[h][c]=n
cls={h:max(v,key=lambda k:(v[k],-ord(k[0]))) for h,v in votes.items()}
idx={c:np.array([h2c[h] for h,k in cls.items() if k==c and h in h2c]) for c in CLASSES}
spec={}; rows=[]
for c in CLASSES:
    X=SENT[idx[c]]; m=X.mean(0); coh=float(np.linalg.norm(m))
    p=PCA(n_components=100,svd_solver="randomized",random_state=0).fit(X-m)
    ev=p.explained_variance_ratio_; cum=np.cumsum(ev); lam=p.explained_variance_
    pr=float(lam.sum()**2/(lam**2).sum())
    n50=int(np.argmax(cum>=0.5)+1) if cum[-1]>=0.5 else ">100"; n90=int(np.argmax(cum>=0.9)+1) if cum[-1]>=0.9 else ">100"
    spec[c]=cum; rows.append(dict(cls=c,n=len(idx[c]),coherence=round(coh,4),var_pc1=round(float(cum[0]),4),var_pc1_5=round(float(cum[4]),4),var_pc1_20=round(float(cum[19]),4),var_pc1_100=round(float(cum[99]),4),participation_ratio=round(pr,2),n_for_50pct=n50,n_for_90pct=n90))
    print(c,len(idx[c]),f"coh {coh:.3f} PR {pr:.1f} cum100 {cum[99]:.3f} n50 {n50} n90 {n90}")
with open(f"{OUT}/csv/25_class_pca.csv","w",newline="",encoding="utf-8-sig") as f:
    w=csv.DictWriter(f,fieldnames=["class(클래스)","n(문장수)","coherence(정합도=평균벡터노름)","var_pc1","var_pc1_5","var_pc1_20","var_pc1_100","participation_ratio(참여율=유효차원수)","n_for_50pct(50%도달성분수)","n_for_90pct(90%도달성분수)"]); w.writeheader()
    for r in rows: w.writerow(dict(zip(w.fieldnames,r.values())))
ang=list(csv.DictReader(open(f"{OUT}/csv/25c_class_subspace_angles.csv",encoding="utf-8-sig"))); auc=list(csv.DictReader(open(f"{OUT}/csv/25d_direction_auc.csv",encoding="utf-8-sig")))
# f28
fig,axes=plt.subplots(1,2,figsize=(14,5.5),gridspec_kw={"width_ratios":[1.4,1]})
ax=axes[0]
for c in CLASSES: ax.plot(range(1,101),spec[c],color=CC[c],lw=2,label=f"{c} (n={len(idx[c]):,}, PR {[r for r in rows if r['cls']==c][0]['participation_ratio']})")
ax.axhline(0.9,color="#c3c2b7",ls=":"); ax.text(1.05,0.905,"90%",fontsize=8.5,color="#52514e"); ax.set_xscale("log"); ax.set_xlabel("주성분 개수 (log)"); ax.set_ylabel("누적 설명 분산 비율"); ax.legend(frameon=False,fontsize=9,loc="lower right")
ax.set_title("클래스별 문장 임베딩 PCA 누적 분산 (중심화 후, 100성분)", loc="left", fontsize=11)
ax=axes[1]; prs=[[r for r in rows if r["cls"]==c][0]["participation_ratio"] for c in CLASSES]; cohs=[[r for r in rows if r["cls"]==c][0]["coherence"] for c in CLASSES]
b=ax.bar(CLASSES,prs,color=[CC[c] for c in CLASSES])
for bx,p,ch in zip(b,prs,cohs): ax.text(bx.get_x()+bx.get_width()/2,p+0.5,f"PR {p}\n정합도 {ch:.2f}",ha="center",fontsize=9)
ax.set_ylabel("참여율 (유효 차원 수)"); ax.set_ylim(0,max(prs)*1.25); ax.set_title("클래스별 유효 차원 수 — 클수록 문장군이 여러 방향으로 퍼져 있음", loc="left", fontsize=11)
fig.suptitle(f"클래스별 문장 PCA — normal 이 가장 분산(PR {prs[0]}, 100성분으로 {spec['normal'][99]*100:.0f}% 만 설명, 90% 미도달), falldown 이 가장 응집(PR {prs[1]}). "
             "어느 클래스도 한 방향(PC1 6.8~13.6%)이 아니다 → 단일 중심벡터·단일 문장으로 대표 불가", x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f28_class_pca_spectrum.png",dpi=160); plt.close(fig)
# f29
fig,axes=plt.subplots(1,2,figsize=(14,5.8),gridspec_kw={"width_ratios":[1,1.3]})
ax=axes[0]; M=np.zeros((4,4))
for r in ang:
    a,b_=CLASSES.index(r["class_a"]),CLASSES.index(r["class_b"]); M[a,b_]=M[b_,a]=float(r["mean_angle_deg"])
im=ax.imshow(M,cmap="Blues",vmin=30,vmax=50)
for i in range(4):
    for j in range(4):
        if i!=j: ax.text(j,i,f"{M[i,j]:.0f}°",ha="center",va="center",fontsize=10,color="white" if M[i,j]>44 else "#0b0b0b")
ax.set_xticks(range(4)); ax.set_xticklabels(CLASSES); ax.set_yticks(range(4)); ax.set_yticklabels(CLASSES); ax.grid(False)
ax.set_title("클래스 부공간(상위 10 PC) 간 평균 주각 — 작을수록 겹침. smoke↔fire 37° 최소", loc="left", fontsize=10.5)
ax=axes[1]; dirs=["d_e","m_e","pc1"]; DN={"d_e":"차 방향 μ_e−μ_normal","m_e":"클래스 평균 μ_e","pc1":"PC1"}; DCOL={"d_e":"#2a78d6","m_e":"#eb6834","pc1":"#c3c2b7"}
groups=[("sourcei","falldown"),("sourcei","fire"),("sourcei","smoke"),("frames_sample","fire"),("frames_sample","smoke")]
w=0.26
for k,dn in enumerate(dirs):
    vals=[]
    for ds_,c in groups:
        v=[float(r["auc"]) for r in auc if r["dataset"].startswith(ds_.split("_")[0]) and r["class"]==c and r["direction"]==dn]; vals.append(v[0] if v else np.nan)
    bars=ax.bar(np.arange(len(groups))+(k-1)*w,vals,w*0.92,color=DCOL[dn],label=DN[dn],hatch=None)
    for bx,v in zip(bars,vals):
        if not np.isnan(v): ax.text(bx.get_x()+bx.get_width()/2,v+0.01,f"{v:.2f}",ha="center",fontsize=8)
ax.set_xticks(range(len(groups))); ax.set_xticklabels([f"{c}\n({'sourcei GT' if d=='sourcei' else 'frames·SAM3'})" for d,c in groups],fontsize=9); ax.set_ylim(0.4,1.08); ax.set_ylabel("AUC (이벤트 vs normal)"); ax.legend(frameon=False,fontsize=9,loc="upper right",ncol=3)
ax.set_title("프레임을 문장 방향에 투영한 AUC — 차 방향이 전 클래스·전 데이터에서 최고", loc="left", fontsize=10.5)
fig.suptitle("클래스 기하 — 클래스 평균은 서로 코사인 0.78~0.91 로 붙어 있고(장소 공통성분), 판별력은 normal 을 뺀 차 방향에만 있다 (fire AUC 0.95/0.92 vs 평균 0.90/0.81 vs PC1 0.59/0.70)", x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f29_class_angles_auc.png",dpi=160); plt.close(fig); print("charts ok")
