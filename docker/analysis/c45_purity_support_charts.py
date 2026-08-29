#!/usr/bin/env python3
"""C4/C5 보정 — 순도·NMI 를 kmeans64·wp16 둘 다 직접 재계산(SAM3 클래스·프로젝트 대상), CSV 28/29 로 그림 f32/f33 을 규약대로 재작성, 요약 JSON 기록."""
import csv, json, glob, collections
import numpy as np, psycopg2, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from sklearn.metrics import normalized_mutual_info_score as nmi, adjusted_mutual_info_score as ami
import fiftyone as fo
from fiftyone import ViewField as F
OUT="/data/fiftyone/frames_bank/report/sourcei_gt"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family":"Noto Sans CJK JP","font.size":11,"axes.spines.top":False,"axes.spines.right":False,"axes.grid":True,"grid.color":"#e6e5e1",
 "grid.linewidth":0.6,"axes.edgecolor":"#c3c2b7","figure.facecolor":"#fcfcfb","axes.facecolor":"#fcfcfb","text.color":"#0b0b0b","axes.labelcolor":"#52514e","xtick.color":"#52514e","ytick.color":"#52514e","axes.unicode_minus":False})
CC={"none":"#8a887f","person":"#2a78d6","fire":"#e34948","smoke":"#4a3aa7","other":"#eda100"}
# ── 순도/NMI 재계산 (kmeans64 + wp16) ──
cur=psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
cur.execute("SELECT entity_id, method, cluster_id, project FROM analysis.frame_cluster"); fc=collections.defaultdict(dict)
for e,m,c,p in cur: fc[m][e]=(c,p)
ds=fo.load_dataset("frames"); ent,ncls=ds.match(F("modality")=="frame").values(["entity_id","normalized_class"]); sam={e:(x or "none") for e,x in zip(ent,ncls)}
res={}
for m in ["kmeans64","wp16"]:
    keys=[e for e in fc[m] if e in sam]; lab=[fc[m][e][0] if m=="kmeans64" else f"{fc[m][e][1]}#{fc[m][e][0]}" for e in keys]
    cls=[sam[e] for e in keys]; proj=[fc[m][e][1] for e in keys]
    ct=collections.Counter(zip(lab,cls)); byk=collections.defaultdict(int)
    for (k,c),n in ct.items(): byk[k]=max(byk[k],n)
    pur=sum(byk.values())/len(keys)
    res[m]=dict(n=len(keys),n_groups=len(set(lab)),purity=round(pur,4),nmi_class=round(nmi(lab,cls),4),ami_class=round(ami(lab,cls),4),nmi_project=round(nmi(lab,proj),4),ami_project=round(ami(lab,proj),4))
    print(m,res[m])
lab_p=[fc["kmeans64"][e][1] for e in fc["kmeans64"] if e in sam]; cls_p=[sam[e] for e in fc["kmeans64"] if e in sam]
res["reference_project_vs_class"]=dict(nmi=round(nmi(lab_p,cls_p),4),ami=round(ami(lab_p,cls_p),4)); print("project vs class", res["reference_project_vs_class"])
print("SAM3 클래스 분포:", collections.Counter(cls_p).most_common())
with open(f"{OUT}/csv/28b_cluster_nmi_summary.csv","w",newline="",encoding="utf-8-sig") as f:
    w=csv.writer(f); w.writerow(["clustering(군집법)","target(대상)","n_frames","n_groups","purity(순도)","NMI","AMI"])
    for m in ["kmeans64","wp16"]:
        w.writerow([m,"SAM3_class",res[m]["n"],res[m]["n_groups"],res[m]["purity"],res[m]["nmi_class"],res[m]["ami_class"]]); w.writerow([m,"project",res[m]["n"],res[m]["n_groups"],"",res[m]["nmi_project"],res[m]["ami_project"]])
    w.writerow(["reference","project_vs_class",len(lab_p),len(set(lab_p)),"",res["reference_project_vs_class"]["nmi"],res["reference_project_vs_class"]["ami"]])
# ── 클러스터별 전체 클래스 구성 직접 계산 (CSV 에는 fire/smoke 열만 있어 person/none 이 비었다) ──
comp=collections.defaultdict(collections.Counter)
for e in fc["kmeans64"]:
    if e in sam: comp[fc["kmeans64"][e][0]][sam[e]] += 1
CLS_ORDER=["fire","smoke","fall","patient","person","none"]
CC2={"fire":"#e34948","smoke":"#4a3aa7","fall":"#eda100","patient":"#e87ba4","person":"#2a78d6","none":"#8a887f"}
# ── f32 ──
r=list(csv.DictReader(open(f"{OUT}/csv/28_cluster_purity.csv",encoding="utf-8-sig")))
def g(x,p):
    k=[kk for kk in x if kk.startswith(p)][0]; return x[k]
r.sort(key=lambda x: -float(g(x,"event_share")))
fig,axes=plt.subplots(1,2,figsize=(17,12),gridspec_kw={"width_ratios":[1.6,1]})
ax=axes[0]; y=np.arange(len(r)); left=np.zeros(len(r))
cids=[int(float(g(x,"cluster"))) for x in r]
tot=np.array([sum(comp[c].values()) for c in cids],dtype=float)
for c in CLS_ORDER:
    v=np.array([comp[cid].get(c,0) for cid in cids],dtype=float)/np.maximum(tot,1)
    ax.barh(y,v,left=left,color=CC2[c],label=c); left+=v
ax.set_yticks(y); ax.set_yticklabels([f"C{g(x,'cluster')}  n={int(float(g(x,'n_frames'))):,}  {g(x,'dominant_project')[:18]}" for x in r],fontsize=7.5); ax.invert_yaxis()
ax.set_xlabel("클러스터 내 SAM3 클래스 구성 비율 (합 1.0)"); ax.set_xlim(0,1.0); ax.legend(frameon=False,ncol=6,loc="lower right",fontsize=8.5)
ev=np.array([float(g(x,"event_share")) for x in r]); ax.set_title(f"kmeans64 군집 64개의 SAM3 클래스 구성 (이벤트 비율 내림차순) — 이벤트 비율 >50% 군집 {int((ev>0.5).sum())}개, >20% {int((ev>0.2).sum())}개, 전부 fire_smoke 현장", loc="left", fontsize=10.5)
ax=axes[1]; names=["kmeans64\n↔ SAM3 클래스","kmeans64\n↔ 프로젝트","wp16\n↔ SAM3 클래스","wp16\n↔ 프로젝트","프로젝트\n↔ SAM3 클래스"]
vals=[res["kmeans64"]["nmi_class"],res["kmeans64"]["nmi_project"],res["wp16"]["nmi_class"],res["wp16"]["nmi_project"],res["reference_project_vs_class"]["nmi"]]
cols=["#e34948","#2a78d6","#e34948","#2a78d6","#8a887f"]; b=ax.bar(range(5),vals,color=cols)
for bx,v in zip(b,vals): ax.text(bx.get_x()+bx.get_width()/2,v+0.01,f"{v:.3f}",ha="center",fontsize=10)
ax.set_xticks(range(5)); ax.set_xticklabels(names,fontsize=9); ax.set_ylabel("NMI"); ax.set_ylim(0,max(vals)*1.25)
ax.set_title(f"군집이 담는 정보 — 프로젝트(장소) NMI {res['kmeans64']['nmi_project']:.2f} vs 이벤트 클래스 {res['kmeans64']['nmi_class']:.2f}\n순도 kmeans64 {res['kmeans64']['purity']:.3f} · wp16 {res['wp16']['purity']:.3f} (wp16 은 프로젝트 안 군집이라 프로젝트 NMI 가 더 높음)", loc="left", fontsize=10)
fig.suptitle("C4 군집 순도/NMI — 이미지 군집은 장소를 4배 더 담고, 이벤트는 fire_smoke 현장의 군집 2~3개에만 몰려 있다 (순열 귀무 NMI ≈ 0.001)", x=0.01, ha="left", fontsize=12)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f32_cluster_purity.png",dpi=160); plt.close(fig)
# ── f33 ──
a=list(csv.DictReader(open(f"{OUT}/csv/29_support_audit.csv",encoding="utf-8-sig"))); b=list(csv.DictReader(open(f"{OUT}/csv/29b_support_by_project.csv",encoding="utf-8-sig")))
dm=np.array([float(g(x,"deploy")) for x in a]); lm=np.array([float(g(x,"labeled")) for x in a]); ncam=np.array([float(g(x,"n_sourcei_cam")) if [k for k in x if k.startswith("n_sourcei_cam")] else float(g(x,"n_cam")) for x in a])
zero=lm==0; mass0=dm[zero].sum(); mass1=dm[ncam<=1].sum()
fig,axes=plt.subplots(1,2,figsize=(16,8),gridspec_kw={"width_ratios":[1,1.1]})
ax=axes[0]; ax.scatter(dm[~zero]*100,lm[~zero]*100,s=30+ncam[~zero]*40,color="#2a78d6",alpha=.75,edgecolor="#fcfcfb",label="sourcei 프레임 있는 군집 (크기=카메라 수)")
ax.scatter(dm[zero]*100,np.full(zero.sum(),0.003),marker="|",s=200,color="#e34948",label=f"sourcei 0 프레임 군집 {int(zero.sum())}개 (배치 질량 {mass0*100:.1f}%)")
lim=[0.002,30]; ax.plot(lim,lim,ls="--",color="#c3c2b7"); ax.set_xscale("log"); ax.set_yscale("log"); ax.set_xlim(0.05,30); ax.set_ylim(0.002,40)
ax.set_xlabel("배치 질량 % (전 프레임 188,190 중 군집 비율)"); ax.set_ylabel("라벨 질량 % (sourcei 7,498 중 군집 비율)"); ax.legend(frameon=False,fontsize=9,loc="upper left")
ax.set_title(f"군집별 배치 vs 라벨 질량 — 배치의 {mass0*100:.1f}% 는 라벨 0, {mass1*100:.1f}% 는 카메라 ≤1", loc="left", fontsize=10.5)
ax=axes[1]; js=[k for k in b[0] if k.lower().startswith("js")][0]; pk=[k for k in b[0] if k.startswith("project")][0]
b.sort(key=lambda x: float(x[js])); yv=np.arange(len(b)); v=[float(x[js]) for x in b]
ax.barh(yv,v,color=["#1baf7a" if i<3 else ("#e34948" if i>=len(b)-3 else "#8a887f") for i in range(len(b))]); ax.set_yticks(yv); ax.set_yticklabels([x[pk] for x in b],fontsize=8.5); ax.invert_yaxis()
for i,x in enumerate(v): ax.text(x+0.005,i,f"{x:.3f}",va="center",fontsize=8)
ax.set_xlabel("JS 발산 (bits) — 프로젝트 군집분포 vs sourcei 군집분포, 낮을수록 유사"); ax.set_title("프로젝트별 대표성 — 초록 가장 유사 3 / 빨강 가장 다른 3", loc="left", fontsize=10.5)
fig.suptitle(f"C5 배치 지원 감사 — sourcei GT 는 배치 프레임 분포의 {100-mass0*100:.0f}% 영역만 덮는다 (64군집 중 {int(zero.sum())}개가 라벨 0). "
             f"라벨을 늘릴 곳은 빈 군집(빨간 눈금)이고, 카메라 1대짜리 군집이 배치 질량의 {mass1*100:.0f}%", x=0.01, ha="left", fontsize=12)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f33_support_audit.png",dpi=160); plt.close(fig)
json.dump(dict(purity_nmi=res,sam3_class_dist=dict(collections.Counter(cls_p)),support=dict(zero_sourcei_clusters=int(zero.sum()),deploy_mass_zero=float(mass0),deploy_mass_le1cam=float(mass1),
          js_most_similar=[(x[pk],float(x[js])) for x in b[:3]],js_least_similar=[(x[pk],float(x[js])) for x in b[-3:]])),open(f"{OUT}/cluster_purity_support_summary.json","w"),ensure_ascii=False,indent=1)
print("zero clusters",int(zero.sum()),"mass0",round(mass0,4),"mass<=1cam",round(mass1,4)); print("charts ok")
