#!/usr/bin/env python3
"""회수 뱅크 7벌 확장분석(§27) 집계 — 공급 31종과 대조해 표를 찍는다."""
import json, glob, os, csv
import numpy as np

R = "/home/user/work_p/Datapipeline-Data-data_pipeline"
D = R + "/docker/data/fiftyone/frames_bank/report/sourcei_gt/recovered"
CSV = R + "/sourcei_gt_csv"

J = []
for p in sorted(glob.glob(D + "/*_ext.json")):
    J.append(json.load(open(p)))
J.sort(key=lambda x: -x["topk"]["mf1"])
print(f"완료 {len(J)}/7 벌\n")

print("■ §1 프로토타입(클래스 중심벡터) vs top-K")
print(f"{'뱅크':<12}{'topK mF1':>9}{'proto mF1':>10}{'Δ':>9}{'95% CI':>20}{'topK acc':>9}{'proto acc':>10}")
for j in J:
    d, lo, hi, pg = j["proto"]["delta_vs_topk"]
    print(f"{j['bank']:<12}{j['topk']['mf1']:>9.4f}{j['proto']['mf1']:>10.4f}{d:>+9.4f}"
          f"{f'[{lo:+.3f}, {hi:+.3f}]':>20}{j['topk']['acc']:>9.4f}{j['proto']['acc']:>10.4f}")
sig = [j for j in J if j["proto"]["delta_vs_topk"][1] > 0]
print(f"  → CI 하한>0 인 뱅크 {len(sig)}/{len(J)}   (공급 31종은 12/31, 평균 Δ+0.087)")
acc_up = [j for j in J if j["proto"]["acc"] > j["topk"]["acc"]]
print(f"  → 정확도까지 오른 뱅크 {len(acc_up)}/{len(J)}   (공급 31종은 0.685→0.601 로 하락)\n")

print("■ §13 임계 무관 랭킹 PR-AUC (점수함수 3종)")
print(f"{'뱅크':<12}" + "".join(f"{c[:4]+'/'+k[:4]:>12}" for c in ["falldown", "fire", "smoke"]
                                for k in ["maxcos", "diff", "iou"]))
best = {"maxcos": 0, "diff": 0, "iou": 0}
for j in J:
    row = f"{j['bank']:<12}"
    for c in ["falldown", "fire", "smoke"]:
        for k in ["maxcos", "diff", "iou"]:
            v = j["prauc"].get(c, {}).get(k)
            row += f"{v:>12.4f}" if v is not None else f"{'—':>12}"
    print(row)
for j in J:
    for c in ["falldown", "fire", "smoke"]:
        d = j["prauc"].get(c, {})
        if not d or d.get("iou") is None: continue
        best[max(("maxcos", "diff", "iou"), key=lambda k: d[k])] += 1
print(f"  → 점수함수별 1위 횟수 {best}   (공급 31종은 분포-IoU 가 3클래스 전부 최고)\n")

print("■ §3 허브니스")
print(f"{'뱅크':<12}{'미선택%':>9}{'상위1%슬롯':>11}{'유효문장%':>10}{'왜도':>8}  상위100 구성")
for j in J:
    h = j["hubness"]
    mix = " ".join(f"{k[:4]}{v}" for k, v in h["top100_class_mix"].items() if v)
    print(f"{j['bank']:<12}{h['never_selected_pct']:>9.1f}{h['top1pct_slot_share']:>11.1f}"
          f"{h['effective_sentence_pct']:>10.2f}{h['skew']:>8.2f}  {mix}")
print("  → 공급 31종: 미선택 79% · 상위1% 슬롯 23~44% · 유효문장 2.8~10.3%(중앙값 4.0)\n")

print("■ §15 프루닝 — 비열등(CI 하한 > −0.02) 판정")
cuts = ["spec25", "spec10", "main25", "main10", "dup95"]
print(f"{'뱅크':<12}" + "".join(f"{c:>22}" for c in cuts))
for j in J:
    row = f"{j['bank']:<12}"
    for c in cuts:
        p = j["pruning"][c]
        mark = "✅" if p["noninferior"] else "❌"
        row += f"{f'{p[chr(107)+chr(101)+chr(101)+chr(112)+chr(95)+chr(112)+chr(99)+chr(116)]:.0f}% {p[chr(100)+chr(101)+chr(108)+chr(116)+chr(97)]:+.3f} {mark}':>22}"
    print(row)
print()
for c in cuts:
    ni = sum(1 for j in J if j["pruning"][c]["noninferior"])
    md = float(np.median([j["pruning"][c]["delta"] for j in J]))
    print(f"  {c:<8} 비열등 {ni}/{len(J)}  Δ중앙값 {md:+.4f}")
print("  → 공급 31종: spec25 비열등 30/31(Δ+0.0013) · main25 비열등 5/31(Δ−0.0205) · dup 2/31(Δ−0.0864)\n")

print("■ 중복컷 클래스별 유지율 (공급 31종은 falldown 16.9% 가 최저)")
print(f"{'뱅크':<12}" + "".join(f"{c:>11}" for c in ["normal", "falldown", "fire", "smoke"]))
for j in J:
    k = j["pruning"]["dup95"]["keep_by_class"]
    print(f"{j['bank']:<12}" + "".join(f"{k.get(c, float('nan')):>11.1f}" for c in
                                       ["normal", "falldown", "fire", "smoke"]))
