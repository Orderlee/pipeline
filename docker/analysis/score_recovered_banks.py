#!/usr/bin/env python3
"""§26 에서 회수한 벡터전용 뱅크 7벌을 sourcei GT 로 **처음** 채점한다.

왜 필요한가 — §26 은 벡터를 회수해 놓고 **좌표 재투영에만 썼다**. 채점 경로
(`analysis_standard.load_sourcei`)는 DB `bank_sentences` 를 읽는데, 이 7벌은 텍스트가
`(텍스트 없음 #N)` 자리표시자라 DB 에 행이 없다. 그래서 31종 표에도, `preds.npz`
35종에도 한 번도 들어간 적이 없다 — 문장 246,644개가 미채점 상태다(분석에 쓴 고유
문장 121,614개의 2배).

클래스 매핑은 추측하지 않았다. 회수 npz 와 DB 양쪽에 있는 v1.0.2.0 으로 대조 확인:
  npz 1:160 / 2:761 / 3:1,049  ==  DB falldown 160 / fire 761 / smoke 1,049
따라서 cls 0/1/2/3 = normal/falldown/fire/smoke = `CLASSES_DEFAULT` 순서 그대로다.

규칙·지표는 `analysis_standard.s3_scoring` 을 그대로 호출한다 — 기존 31종 수치와
같은 정의여야 한 표에 놓을 수 있다.

⚠️ **뱅크 하나당 프로세스 하나.** 79,842문장 뱅크는 점수행렬만 2.4GB 이고
   `_wave_iou` 내부 int64 인덱스까지 합치면 피크가 ~8GB 다. 한 프로세스에 여러 뱅크를
   들면 공유 호스트에서 OOM 이 난다(가드레일 G8 — 뱅크 크기 편차 4배↑).

⚠️ **중복 제거를 DB 경로와 맞춘다.** DB 는 `content_hash` 로 중복을 지우고 담았다
   (v1.0.2.0: npz 14,600 → DB 12,568). top-K 다수결은 문장 개수를 세므로, 중복을
   그대로 두면 기존 31종 수치와 비교가 성립하지 않는다. 텍스트가 없으므로 벡터
   바이트 해시로 같은 일을 한다.

실행 (컨테이너 안):
    docker exec docker-analysis-1 sh -c 'cd /workspace && \
      COS_THREADS=4 nice -n 10 python3 score_recovered_banks.py --bank v1.0.5.7'
"""
import os, sys, json, time, hashlib, argparse
import numpy as np

BASE = "/data/fiftyone/frames_bank/report/sourcei_gt"
VEC = f"{BASE}/vecbanks"
OUT = f"{BASE}/recovered"
T0 = time.time()


def log(m):
    print(f"[{time.time() - T0:6.0f}s] {m}", flush=True)


def load_frames():
    """sourcei 7,498 프레임 임베딩 + GT + 카메라. load_sourcei 와 같은 방식."""
    import fiftyone as fo
    d = np.load(f"{BASE}/preds.npz", allow_pickle=True)
    ds = fo.load_dataset("sourcei")
    hid, hemb = ds.values(["id", "embedding"])
    assert hid == list(d["ids"]), "FiftyOne 샘플 순서가 preds.npz 와 다르다"
    F = np.asarray(hemb, dtype=np.float32)
    F /= np.maximum(np.linalg.norm(F, axis=1, keepdims=True), 1e-9)
    return F, d["gt"], d["camera"]


def load_bank(version):
    """회수 npz → (라벨, 벡터). 중복은 벡터 바이트 해시로 제거(DB content_hash 대응)."""
    z = np.load(f"{VEC}/{version}.npz")
    lab_raw = z["cls"].astype(np.int32)
    V_raw = z["vecs"].astype(np.float32)
    V_raw /= np.maximum(np.linalg.norm(V_raw, axis=1, keepdims=True), 1e-9)

    seen, keep = set(), []
    for i in range(V_raw.shape[0]):
        h = hashlib.blake2b(V_raw[i].tobytes(), digest_size=16).digest()
        if h not in seen:
            seen.add(h)
            keep.append(i)
    keep = np.asarray(keep, dtype=np.int64)
    log(f"  {version}: 원본 {V_raw.shape[0]:,} → 중복제거 {len(keep):,} "
        f"({len(keep) / V_raw.shape[0]:.1%})")
    return lab_raw[keep], V_raw[keep]


def score(version):
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import analysis_standard as A

    F, gt, cam = load_frames()
    log(f"프레임 {F.shape[0]:,} × {F.shape[1]}  GT 분포 {np.bincount(gt).tolist()}")

    lab, V = load_bank(version)
    cls_n = np.bincount(lab, minlength=4).tolist()
    log(f"  클래스 {dict(zip(A.CLASSES_DEFAULT, cls_n))}")

    # 점수행렬: 행 청크로 만들어 임시 피크를 누른다(결과 자체는 통짜여야 s3 가 받는다)
    S = np.empty((F.shape[0], V.shape[0]), dtype=np.float32)
    step = 1500
    for i in range(0, F.shape[0], step):
        S[i:i + step] = F[i:i + step] @ V.T
    log(f"  점수행렬 {S.shape} = {S.nbytes / 1e9:.2f} GB")
    del V

    D = dict(name="sourcei", gt=gt, group=cam, classes=A.CLASSES_DEFAULT,
             events=["falldown", "fire", "smoke"], frames=F, sent=None,
             bank_scores={version: (lab, S)}, bank_vecs={}, bank_counts={},
             cluster=None, ref_bank=version, fp_budget=0.05, all_bank_preds=[])
    R = {"guardrails": []}
    A.s3_scoring(D, R)

    for r in R["S3"]:
        r["n_sentences"] = int(len(lab))
        r["n_raw"] = int(np.load(f"{VEC}/{version}.npz")["cls"].shape[0])
        r["class_counts"] = json.dumps(dict(zip(A.CLASSES_DEFAULT, cls_n)), ensure_ascii=False)

    os.makedirs(OUT, exist_ok=True)
    with open(f"{OUT}/{version}.json", "w") as f:
        json.dump({"bank": version, "rows": R["S3"],
                   "guardrails": [g for g in R["guardrails"] if g]}, f,
                  ensure_ascii=False, indent=2)
    # 예측을 남긴다 — 카메라 군집 쌍대 부트스트랩(G1)은 점수가 아니라 예측이 있어야 돈다
    np.savez_compressed(f"{OUT}/{version}_preds.npz",
                        **{k: v.astype(np.int8) for k, v in R["_preds"][version].items()})
    log(f"저장 {OUT}/{version}.json + _preds.npz")
    return R["S3"]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bank", required=True, help="회수 뱅크 버전 (예: v1.0.5.7)")
    a = ap.parse_args()
    rows = score(a.bank)
    print()
    for r in rows:
        print(f"{r['bank']:<12} {r['rule']:<7} 문장 {r['n_sentences']:>7,}  "
              f"acc {r['acc']:.4f}  mF1 {r['macro_f1']:.4f}  "
              f"PR-AUC {r['prauc']:.4f}  오탐 {r['fp_normal']:.4f}")


if __name__ == "__main__":
    main()
