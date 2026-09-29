#!/usr/bin/env python3
"""오답 예측 과제가 사실은 '정답 클래스 예측'과 같은 과제인지 확인하고, 실질 비교로 대체한다."""
import sys, numpy as np
sys.path.insert(0, "/workspace")
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import accuracy_score, f1_score, recall_score
from sklearn.model_selection import GroupKFold
import xai_features as XF
OUT="/workspace/_xai"; d=np.load(f"{OUT}/features.npz", allow_pickle=True)
F={k:d[k] for k in d.files if not k.startswith("meta_")}
gt=np.asarray([str(x) for x in d["meta_gt"]]); pred=np.asarray([str(x) for x in d["meta_pred"]])
cam=np.asarray([str(x) for x in d["meta_cam"]]); ev=np.asarray([str(x) for x in d["meta_ev"]])
nl=np.asarray(F["normal_lead"],dtype=float); y=(pred!=gt).astype(int)
CL=("normal","falldown","smoke","fire")

print("=== 구조 확인: normal_lead 의 부호가 예측과 같은가 ===")
same=( (nl>0) == (pred=="normal") )
print(f"  sign(normal_lead)>0  ==  pred=='normal' 일치율: {same.mean()*100:.2f}%  ({int((~same).sum())}건 불일치)")
print("  → 부호가 곧 '정상으로 예측'이라는 뜻이고, 오답 여부는 (그 부호, 정답)의 함수다.")
print(f"  검증: y_wrong == ((nl>0) != (gt=='normal')) 일치율 "
      f"{(y == (((nl>0)!=(gt=='normal'))).astype(int)).mean()*100:.2f}%")

G={f['name']:f['group'] for f in XF.FEATURES}
ALL=[f["name"] for f in XF.FEATURES if f["leak"]=="free"]
NONPROMPT=[x for x in ALL if not G[x].startswith("A_")]
IDENT=["pc1","pc2","pc3","pc4","clip_size","event_size","camera_dev","knn_same_camera","cos_to_camera","cos_to_global"]
NP_TRANS=[x for x in NONPROMPT if x not in IDENT]

def cv_multi(sub, groups):
    M,_=XF.design_matrix(F,sub); gkf=GroupKFold(n_splits=5); oof=np.empty(len(gt),dtype=object)
    for tr,te in gkf.split(M,gt,groups):
        m=HistGradientBoostingClassifier(max_depth=3,max_iter=250,learning_rate=.06,
                                         min_samples_leaf=40,random_state=0)
        m.fit(M[tr],gt[tr]); oof[te]=m.predict(M[te])
    return np.asarray([str(x) for x in oof])

print(f"\n=== 실질 비교: 정답 클래스 맞히기 (프롬프트 규칙 vs 라벨-자유 피처) ===")
print(f"{'방식':34s} {'정확도':>7s} {'macroF1':>8s} " + " ".join(f"{c[:6]:>7s}" for c in CL))
def rep(nm, p):
    rec=recall_score(gt,p,labels=list(CL),average=None,zero_division=0)
    print(f"  {nm:32s} {accuracy_score(gt,p)*100:>6.1f}% {f1_score(gt,p,average='macro')*100:>7.1f}% "
          + " ".join(f"{r*100:>6.1f}%" for r in rec))
rep("프롬프트 규칙 v1.0.8.0", pred)
for gname,groups in (("이벤트",ev),("카메라",cam)):
    rep(f"피처 전체 32 ({gname} 분할)", cv_multi(ALL,groups))
    rep(f"프롬프트 제외 21 ({gname} 분할)", cv_multi(NONPROMPT,groups))
    rep(f"프롬프트·정체성 제외 11 ({gname} 분할)", cv_multi(NP_TRANS,groups))
print(f"\n  프롬프트 제외 집합 = {sorted(NONPROMPT)}")
print(f"  정체성까지 제외 = {sorted(NP_TRANS)}")
