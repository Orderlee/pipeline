#!/usr/bin/env python3
"""
Part 2 of Analysis B2 — complete the AUC analysis, figures, and summary JSON.
This runs separately after Part 1 to handle FiftyOne interactions independently.
"""
import os
os.environ["OMP_NUM_THREADS"] = "4"

import sys
sys.path.insert(0, "/workspace")

import numpy as np
import psycopg2
import json
import csv
from collections import defaultdict
from sklearn.metrics import roc_auc_score
import warnings
warnings.filterwarnings("ignore")

# ============================================================================
# SETUP
# ============================================================================
OUT_DIR = "/data/fiftyone/frames_bank/report/sourcei_gt"
CSV_DIR = os.path.join(OUT_DIR, "csv")
FIG_DIR = os.path.join(OUT_DIR, "fig")
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(FIG_DIR, exist_ok=True)

# Read existing CSVs from Part 1
print("Loading Part 1 results...")

# Load class PCA results
pca_results = {}
with open(os.path.join(CSV_DIR, "25_class_pca.csv")) as f:
    reader = csv.DictReader(f)
    for row in reader:
        cls = row["class"]
        pca_results[cls] = {
            "n_c": int(row["n"]),
            "coherence": float(row["coherence"]),
            "var_pc1": float(row["var_pc1"]),
            "var_pc1_5": float(row["var_pc1_5"]),
            "var_pc1_20": float(row["var_pc1_20"]),
            "var_pc1_100": float(row["var_pc1_100"]),
            "participation_ratio": float(row["participation_ratio"]),
            "n_for_50pct": int(row["n_for_50pct"]),
            "n_for_90pct": int(row["n_for_90pct"]),
        }

valid_classes = ["normal", "falldown", "fire", "smoke"]
class_colors = {
    "normal": "#8a887f",
    "falldown": "#eda100",
    "fire": "#e34948",
    "smoke": "#4a3aa7",
}

# Load angles from Part 1
angles_list = []
with open(os.path.join(CSV_DIR, "25c_class_subspace_angles.csv")) as f:
    reader = csv.DictReader(f)
    for row in reader:
        angles_list.append(row)

print("Part 1 data loaded")

# ============================================================================
# PART 2: Try FiftyOne loading with graceful fallback
# ============================================================================
print("Attempting FiftyOne dataset loading...")

ncls = None
emb_arr = None
gth = None
embh_arr = None

try:
    import fiftyone as fo
    from fiftyone import ViewField as F

    # Load frames dataset
    print("  Loading frames...")
    fr_ds = fo.load_dataset("frames")
    fr = fr_ds.match(F("modality") == "frame")
    ncls_raw = np.array(fr.values("normalized_class"))
    ncls = np.array([x or "none" for x in ncls_raw])
    emb = np.array(fr.values("image_embedding"), dtype=np.float32)
    emb_arr = emb / np.linalg.norm(emb, axis=1, keepdims=True)
    print(f"    Loaded {len(emb_arr)} frames")

    # Load sourcei GT dataset
    print("  Loading sourcei GT...")
    dh = fo.load_dataset("sourcei")
    ids, embh, gth_raw, camh = dh.values(["id", "embedding", "ground_truth.label", "camera"])
    gth = np.array([x or "none" for x in gth_raw])
    embh_arr = np.array(embh, dtype=np.float32)
    embh_arr = embh_arr / np.linalg.norm(embh_arr, axis=1, keepdims=True)
    print(f"    Loaded {len(embh_arr)} sourcei items")

except Exception as e:
    print(f"  WARNING: Could not load FiftyOne datasets: {e}")
    print("  Will skip Part 2 (frames and sourcei AUC analysis)")

# ============================================================================
# Compute AUCs if FiftyOne data available
# ============================================================================
direction_auc_list = []

if ncls is not None and emb_arr is not None:
    print("\nComputing AUCs for frames...")

    # Re-compute difference directions using class means from PCA results
    # We need the class mean vectors - let me load them from DB or use a proxy
    DSN = "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    from prompt_cos_db import load_sentence_vectors
    h2c, SENT = load_sentence_vectors(cur)

    # Get class masks
    hashes = [None] * len(h2c)
    for h, i in h2c.items():
        hashes[i] = h

    cur.execute("""
        SELECT content_hash, class_label, COUNT(*) as cnt
        FROM bank_sentences
        GROUP BY 1, 2
    """)
    class_by_hash_all = defaultdict(lambda: defaultdict(int))
    for h, c, cnt in cur.fetchall():
        class_by_hash_all[h][c] += cnt

    class_by_hash = {}
    for h, class_counts in class_by_hash_all.items():
        filtered = {c: v for c, v in class_counts.items() if c in valid_classes}
        if filtered:
            majority = max(filtered.items(), key=lambda x: (x[1], x[0]))[0]
            class_by_hash[h] = majority

    sent_classes = np.array([class_by_hash.get(hashes[i], None) for i in range(len(SENT))])
    mask_valid = sent_classes != None
    SENT_valid = SENT[mask_valid]
    sent_classes_valid = sent_classes[mask_valid]

    # Compute class means
    class_means = {}
    for cls in valid_classes:
        mask = sent_classes_valid == cls
        class_means[cls] = SENT_valid[mask].mean(axis=0)

    # Difference directions
    diff_dirs = {}
    for cls in ["fire", "smoke", "falldown"]:
        d = class_means[cls] - class_means["normal"]
        d = d / np.linalg.norm(d)
        diff_dirs[cls] = d

    # Frames sample
    fire_idx = np.where(ncls == "fire")[0]
    smoke_idx = np.where(ncls == "smoke")[0]
    neg_idx = np.where(np.isin(ncls, ["none", "person"]))[0]

    RNG = np.random.default_rng(0)
    neg_sample = RNG.choice(neg_idx, min(20000, len(neg_idx)), replace=False)
    sub = np.concatenate([fire_idx, smoke_idx, neg_sample])

    X = np.asarray([emb_arr[i] for i in sub], dtype=np.float32)
    X /= np.linalg.norm(X, axis=1, keepdims=True)

    y_ref = np.zeros(len(sub), dtype=int)
    y_ref[:len(fire_idx)] = 2
    y_ref[len(fire_idx):len(fire_idx) + len(smoke_idx)] = 3

    print(f"  Frames sample: {len(X)} total, fire={len(fire_idx)}, smoke={len(smoke_idx)}")

    # Compute projections - FOR FIRE AND SMOKE ONLY from frames
    for event_cls in ["fire", "smoke"]:
        d_e = diff_dirs.get(event_cls)
        if d_e is not None:
            proj_de = X @ d_e
            y_binary = (y_ref == (2 if event_cls == "fire" else 3)).astype(int)
            if y_binary.sum() > 0 and (1 - y_binary).sum() > 0:
                auc_de = float(roc_auc_score(y_binary, proj_de))
                mean_pos = float(proj_de[y_binary == 1].mean())
                mean_neg = float(proj_de[y_binary == 0].mean())
                n_pos = int(y_binary.sum())
                n_neg = int((1 - y_binary).sum())
                direction_auc_list.append(["frames_sample", event_cls, "d_e", auc_de, mean_pos, mean_neg, n_pos, n_neg])

        m_e = class_means[event_cls]
        proj_me = X @ m_e
        y_binary = (y_ref == (2 if event_cls == "fire" else 3)).astype(int)
        if y_binary.sum() > 0 and (1 - y_binary).sum() > 0:
            auc_me = float(roc_auc_score(y_binary, proj_me))
            mean_pos = float(proj_me[y_binary == 1].mean())
            mean_neg = float(proj_me[y_binary == 0].mean())
            n_pos = int(y_binary.sum())
            n_neg = int((1 - y_binary).sum())
            direction_auc_list.append(["frames_sample", event_cls, "m_e", auc_me, mean_pos, mean_neg, n_pos, n_neg])

if gth is not None and embh_arr is not None:
    print("Computing AUCs for sourcei GT...")

    # sourcei GT analysis
    for event_cls in ["falldown", "fire", "smoke"]:
        d_e = diff_dirs.get(event_cls)
        if d_e is not None:
            proj_de = embh_arr @ d_e
            y_binary = (gth == event_cls).astype(int)
            if y_binary.sum() > 0 and (1 - y_binary).sum() > 0:
                auc_de = float(roc_auc_score(y_binary, proj_de))
                mean_pos = float(proj_de[y_binary == 1].mean())
                mean_neg = float(proj_de[y_binary == 0].mean())
                n_pos = int(y_binary.sum())
                n_neg = int((1 - y_binary).sum())
                direction_auc_list.append(["sourcei_gt", event_cls, "d_e", auc_de, mean_pos, mean_neg, n_pos, n_neg])

        m_e = class_means[event_cls]
        proj_me = embh_arr @ m_e
        y_binary = (gth == event_cls).astype(int)
        if y_binary.sum() > 0 and (1 - y_binary).sum() > 0:
            auc_me = float(roc_auc_score(y_binary, proj_me))
            mean_pos = float(proj_me[y_binary == 1].mean())
            mean_neg = float(proj_me[y_binary == 0].mean())
            n_pos = int(y_binary.sum())
            n_neg = int((1 - y_binary).sum())
            direction_auc_list.append(["sourcei_gt", event_cls, "m_e", auc_me, mean_pos, mean_neg, n_pos, n_neg])

# Write direction AUC CSV
if direction_auc_list:
    with open(os.path.join(CSV_DIR, "25d_direction_auc.csv"), "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["dataset", "class", "direction", "auc", "mean_pos", "mean_neg", "n_pos", "n_neg"])
        for row in direction_auc_list:
            w.writerow(row)
    print(f"Wrote {os.path.join(CSV_DIR, '25d_direction_auc.csv')} ({len(direction_auc_list)} rows)")
else:
    print("No AUC results computed (FiftyOne not available)")

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

print("Generating figures...")

# Figure 1: Variance spectrum and PR
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
fig.patch.set_facecolor("#fcfcfb")

# We need to re-read the PCA to get variance explained - approximation
var_specs = {
    'normal': [0.0681343525648117, 0.24316291511058807, 0.5142475962638855, 0.8695681691169739],
    'fire': [0.10138164460659027, 0.3564189076423645, 0.6836814284324646, 0.9602122902870178],
    'falldown': [0.13617545366287231, 0.359582781791687, 0.7066034078598022, 0.9726467728614807],
    'smoke': [0.07692783325910568, 0.2805069088935852, 0.6066442131996155, 0.9324960112571716],
}

# Interpolate cumulative variance curves
for cls in valid_classes:
    comps = np.array([1, 5, 20, 100])
    vars_cum = np.array(var_specs[cls])
    # Interpolate
    comps_full = np.linspace(1, 100, 100)
    from scipy.interpolate import interp1d
    interp = interp1d(comps, vars_cum, kind='linear', fill_value='extrapolate')
    vars_interp = interp(comps_full)
    ax1.plot(comps_full, vars_interp, label=cls, color=class_colors[cls], linewidth=2)

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
bars = ax2.bar(valid_classes, pr_values, color=colors_list, edgecolor="black", linewidth=0.5)
for bar, val in zip(bars, pr_values):
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2, height, f"{val:.1f}",
             ha="center", va="bottom", fontsize=10)
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
             fontsize=12, loc="left")

plt.tight_layout()
plt.savefig(os.path.join(FIG_DIR, "f28_class_pca_spectrum.png"), dpi=150, bbox_inches="tight")
print(f"  Wrote f28_class_pca_spectrum.png")
plt.close()

# Figure 2: Angles and AUC comparison
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
fig.patch.set_facecolor("#fcfcfb")

# Left: Angle heatmap
classes_for_heat = valid_classes
angle_matrix = np.zeros((len(classes_for_heat), len(classes_for_heat)))
for row in angles_list:
    cls_a, cls_b = row["class_a"], row["class_b"]
    mean_angle = float(row["mean_angle_deg"])
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

fig.suptitle("클래스 부공간 각도 및 방향별 AUC", fontsize=12, loc="left")
plt.tight_layout()
plt.savefig(os.path.join(FIG_DIR, "f29_class_angles_auc.png"), dpi=150, bbox_inches="tight")
print(f"  Wrote f29_class_angles_auc.png")
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

print(f"Wrote {os.path.join(OUT_DIR, 'class_pca_summary.json')}")

# ============================================================================
# SELF-CHECKS
# ============================================================================
print("\n=== SELF-CHECKS ===")
print(f"Part 1 PCA results: {len(pca_results)} classes")
print(f"Part 2 AUC results: {len(direction_auc_list)} rows")
print(f"All AUC values in [0,1]: {all(0 <= row[3] <= 1 for row in direction_auc_list)}")

print("\nDONE")
