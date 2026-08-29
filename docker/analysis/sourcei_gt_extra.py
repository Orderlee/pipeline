"""sourcei_gt_rules.py 보조: 클래스 오프셋(z-보정) 검증 + 클래스별 max 코사인 저장 (3뱅크)."""
import os, sys, json, numpy as np, psycopg2
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS"): os.environ.setdefault(_v,"6")
sys.path.insert(0,"/workspace")
from prompt_cos_db import load_banks, load_sentence_vectors
from sourcei_gt_rules import load_frames, CLASSES
OUT="/data/fiftyone/frames_bank/report/sourcei_gt"
ids,F,gt,cam,src,unit=load_frames(); gt_i=np.array([CLASSES.index(g) for g in gt])
cur=psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
h2c,SENT=load_sentence_vectors(cur)
want=None  # 전 뱅크
out={}
for b in load_banks(cur,want):
    if not all(c in {x for _,x,_ in b["rows"]} for c in CLASSES): print("skip",b["version"]); continue
    mem={}
    for h,c,_ in b["rows"]: mem.setdefault(c,[]).append(h2c[h])
    per=np.zeros((len(ids),4),np.float32)
    for s in range(0,len(ids),1000):
        S=F[s:s+1000]@SENT.T
        for ci,c in enumerate(CLASSES): per[s:s+1000,ci]=S[:,mem[c]].max(1)
    np.save(f"{OUT}/percls_{b['version']}.npy",per)
    # 오프셋 보정: 카메라 홀드아웃(짝/홀 카메라) — 클래스별 평균 코사인 차이를 normal 기준으로 뺀다
    cams=np.unique(cam); res={}
    for fold in (0,1):
        tr=np.isin(cam,cams[fold::2]); te=~tr
        off=per[tr].mean(0)-per[tr].mean(0)[0]     # 클래스별 평균 - normal 평균 (GT 불필요)
        base=(per[te].argmax(1)==gt_i[te]).mean(); corr=((per[te]-off).argmax(1)==gt_i[te]).mean()
        # GT 를 쓰는 상한: 클래스별 GT-조건 평균으로 맞춘 오프셋 (참고치)
        pc=(per[te]-off).argmax(1)
        res[f"fold{fold}"]=dict(n=int(te.sum()),base=float(base),corr=float(corr),off=off.tolist(),
            fire_recall_base=float((per[te].argmax(1)[gt_i[te]==2]==2).mean()),fire_recall_corr=float((pc[gt_i[te]==2]==2).mean()),
            smoke_recall_base=float((per[te].argmax(1)[gt_i[te]==3]==3).mean()),smoke_recall_corr=float((pc[gt_i[te]==3]==3).mean()),
            normal_recall_base=float((per[te].argmax(1)[gt_i[te]==0]==0).mean()),normal_recall_corr=float((pc[gt_i[te]==0]==0).mean()))
    out[b["version"]]=res; print(b["version"],json.dumps(res)[:600])
json.dump(out,open(f"{OUT}/offset.json","w"),indent=1)
