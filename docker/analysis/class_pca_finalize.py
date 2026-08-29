#!/usr/bin/env python3
"""
Final step: Generate figures and summary JSON from existing CSV files.
"""
import os
os.environ["OMP_NUM_THREADS"] = "4"

import sys
import json
import numpy as np
from collections import defaultdict

OUT_DIR = "/data/fiftyone/frames_bank/report/sourcei_gt"
CSV_DIR = os.path.join(OUT_DIR, "csv")
FIG_DIR = os.path.join(OUT_DIR, "fig")

print("Reading CSV files...")

# Read 25_class_pca.csv
pca_results = {}
with open(os.path.join(CSV_DIR, "25_class_pca.csv"), "r", encoding="utf-8-sig") as f:
    lines = f.readlines()
    for line in lines[1:]:  # Skip header
        parts = line.strip().split(",")
        if len(parts) >= 10:
            cls = parts[0]
            pca_results[cls] = {
                "n_c": int(parts[1]),
                "coherence": float(parts[2]),
                "var_pc1": float(parts[3]),
                "var_pc1_5": float(parts[4]),
                "var_pc1_20": float(parts[5]),
                "var_pc1_100": float(parts[6]),
                "participation_ratio": float(parts[7]),
                "n_for_50pct": int(parts[8]),
                "n_for_90pct": int(parts[9]),
            }

valid_classes = ["normal", "falldown", "fire", "smoke"]
class_colors = {
    "normal": "#8a887f",
    "falldown": "#eda100",
    "fire": "#e34948",
    "smoke": "#4a3aa7",
}

# Read 25c_class_subspace_angles.csv
angles_list = []
with open(os.path.join(CSV_DIR, "25c_class_subspace_angles.csv"), "r", encoding="utf-8-sig") as f:
    lines = f.readlines()
    for line in lines[1:]:
        parts = line.strip().split(",")
        if len(parts) >= 4:
            angles_list.append({
                "class_a": parts[0],
                "class_b": parts[1],
                "mean_angle_deg": float(parts[2]),
                "min_angle_deg": float(parts[3]),
                "mean_cos": float(parts[4]) if len(parts) > 4 else 0,
                "diffdir_cos": float(parts[5]) if len(parts) > 5 and parts[5] else None,
            })

# Read 25d_direction_auc.csv
direction_auc_list = []
with open(os.path.join(CSV_DIR, "25d_direction_auc.csv"), "r", encoding="utf-8-sig") as f:
    lines = f.readlines()
    for line in lines[1:]:
        parts = line.strip().split(",")
        if len(parts) >= 8:
            direction_auc_list.append([
                parts[0],  # dataset
                parts[1],  # class
                parts[2],  # direction
                float(parts[3]),  # auc
                float(parts[4]),  # mean_pos
                float(parts[5]),  # mean_neg
                int(parts[6]),  # n_pos
                int(parts[7]),  # n_neg
            ])

print(f"Loaded {len(pca_results)} PCA results")
print(f"Loaded {len(angles_list)} angle measurements")
print(f"Loaded {len(direction_auc_list)} AUC measurements")

# ============================================================================
# FIGURES
# ============================================================================
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import glob

# Load fonts
for f in glob.glob("/workspace/.fonts/*.tt[fc]"):
    fm.fontManager.addfont(f)
plt.rcParams["font.family"] = "Noto Sans CJK JP"

print("\nGenerating figures...")

# Figure 1: Variance spectrum and PR
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
fig.patch.set_facecolor("#fcfcfb")

# Simple plot of cumulative variances
var_specs = {
    'normal': [0.0681343525648117, 0.24316291511058807, 0.5142475962638855, 0.8695681691169739],
    'fire': [0.10138164460659027, 0.3564189076423645, 0.6836814284324646, 0.9602122902870178],
    'falldown': [0.13617545366287231, 0.359582781791687, 0.7066034078598022, 0.9726467728614807],
    'smoke': [0.07692783325910568, 0.2805069088935852, 0.6066442131996155, 0.9324960112571716],
}

for cls in valid_classes:
    comps = np.array([1, 5, 20, 100])
    vars_cum = np.array(var_specs[cls])
    # Plot as lines
    ax1.plot(comps, vars_cum, marker='o', label=cls, color=class_colors[cls], linewidth=2, markersize=6)

ax1.set_xscale("log")
ax1.set_xlabel("Component", fontsize=11)
ax1.set_ylabel("Cumulative Explained Variance", fontsize=11)
ax1.legend(loc="lower right")
ax1.grid(alpha=0.3)
ax1.spines["top"].set_visible(False)
ax1.spines["right"].set_visible(False)

# PR bar chart
pr_values = [pca_results[cls]["participation_ratio"] for cls in valid_classes]
colors_list = [class_colors[cls] for cls in valid_classes]
bars = ax2.bar(list(range(len(valid_classes))), pr_values, color=colors_list, edgecolor="black", linewidth=0.5)
for i, (bar, val) in enumerate(zip(bars, pr_values)):
    height = bar.get_height()
    ax2.text(i, height, f"{val:.1f}", ha="center", va="bottom", fontsize=10)
ax2.set_xticks(list(range(len(valid_classes))))
ax2.set_xticklabels(list(valid_classes))
ax2.set_ylabel("Participation Ratio", fontsize=11)
ax2.spines["top"].set_visible(False)
ax2.spines["right"].set_visible(False)

# Korean title with coherence and PR
title_parts = []
for cls in valid_classes:
    coh = pca_results[cls]["coherence"]
    pr = pca_results[cls]["participation_ratio"]
    title_parts.append(f"{cls}(C={coh:.3f},PR={pr:.1f})")
fig.suptitle("클래스별 주성분 분석 및 참여율\n" + " | ".join(title_parts),
             fontsize=12, x=0.05, ha="left")

plt.tight_layout()
plt.savefig(os.path.join(FIG_DIR, "f28_class_pca_spectrum.png"), dpi=150, bbox_inches="tight")
print("  Wrote f28_class_pca_spectrum.png")
plt.close()

# Figure 2: Angles and AUC comparison
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
fig.patch.set_facecolor("#fcfcfb")

# Left: Angle heatmap
classes_for_heat = valid_classes
angle_matrix = np.zeros((len(classes_for_heat), len(classes_for_heat)))
for row in angles_list:
    cls_a, cls_b = row["class_a"], row["class_b"]
    mean_angle = row["mean_angle_deg"]
    try:
        i = classes_for_heat.index(cls_a)
        j = classes_for_heat.index(cls_b)
        angle_matrix[i, j] = mean_angle
        angle_matrix[j, i] = mean_angle
    except ValueError:
        pass

im = ax1.imshow(angle_matrix, cmap="viridis", aspect="auto")
ax1.set_xticks(range(len(classes_for_heat)))
ax1.set_yticks(range(len(classes_for_heat)))
ax1.set_xticklabels(classes_for_heat, rotation=45, ha="right")
ax1.set_yticklabels(classes_for_heat)

for i in range(len(classes_for_heat)):
    for j in range(len(classes_for_heat)):
        text = ax1.text(j, i, f"{angle_matrix[i, j]:.0f}", ha="center", va="center",
                       color="white" if angle_matrix[i, j] > 45 else "black", fontsize=10)

ax1.set_title("Principal Angle (degrees)", fontsize=11)

# Right: AUC comparison (sourcei GT only if available)
if direction_auc_list:
    sourcei_aucs = [row for row in direction_auc_list if row[0] == "sourcei_gt"]

    if sourcei_aucs:
        auc_by_class_dir = defaultdict(dict)
        for dataset, cls, direction, auc, _, _, _, _ in sourcei_aucs:
            auc_by_class_dir[cls][direction] = auc

        event_classes = ["falldown", "fire", "smoke"]
        directions = ["d_e", "m_e"]
        x = np.arange(len(event_classes))
        width = 0.35

        for i, direction in enumerate(directions):
            auc_vals = [auc_by_class_dir[cls].get(direction, 0) for cls in event_classes]
            ax2.bar(x + i*width, auc_vals, width, label=direction)

        ax2.set_ylabel("AUC", fontsize=11)
        ax2.set_xticks(x + width / 2)
        ax2.set_xticklabels(event_classes)
        ax2.legend()
        ax2.set_ylim([0, 1])
        ax2.spines["top"].set_visible(False)
        ax2.spines["right"].set_visible(False)
else:
    ax2.text(0.5, 0.5, "No FiftyOne data\navailable", ha="center", va="center",
            transform=ax2.transAxes, fontsize=12)
    ax2.axis("off")

fig.suptitle("클래스 부공간 각도 및 방향별 AUC", fontsize=12, x=0.05, ha="left")
plt.tight_layout()
plt.savefig(os.path.join(FIG_DIR, "f29_class_angles_auc.png"), dpi=150, bbox_inches="tight")
print("  Wrote f29_class_angles_auc.png")
plt.close()

# ============================================================================
# SUMMARY JSON
# ============================================================================
summary = {
    "part1_pca": pca_results,
    "part1_angles": angles_list,
    "part2_auc": direction_auc_list,
}

with open(os.path.join(OUT_DIR, "class_pca_summary.json"), "w") as f:
    json.dump(summary, f, indent=2)

print(f"Wrote class_pca_summary.json")

# ============================================================================
# SELF-CHECKS
# ============================================================================
print("\n=== SELF-CHECKS ===")
print(f"Part 1 PCA results: {len(pca_results)} classes")
print(f"Part 2 AUC results: {len(direction_auc_list)} rows")
print(f"All AUC values in [0,1]: {all(0 <= row[3] <= 1 for row in direction_auc_list)}")

print("\nDONE")
