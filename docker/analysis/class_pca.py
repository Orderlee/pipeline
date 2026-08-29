#!/usr/bin/env python3
"""
Analysis B2: Class-wise PCA of prompt-sentence embeddings and frame projections.
"""
import os
os.environ["OMP_NUM_THREADS"] = "4"
os.environ["MKL_NUM_THREADS"] = "4"

import sys
sys.path.insert(0, "/workspace")

import numpy as np
import psycopg2
import json
import csv
import glob
from collections import defaultdict
from sklearn.decomposition import PCA
from sklearn.metrics import roc_auc_score
import warnings
warnings.filterwarnings("ignore")

# Load prompt_cos_db
from prompt_cos_db import load_sentence_vectors

# ============================================================================
# SETUP
# ============================================================================
OUT_DIR = "/data/fiftyone/frames_bank/report/sourcei_gt"
CSV_DIR = os.path.join(OUT_DIR, "csv")
FIG_DIR = os.path.join(OUT_DIR, "fig")
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(FIG_DIR, exist_ok=True)

# DSN
DSN = "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"
conn = psycopg2.connect(DSN)
cur = conn.cursor()

# Load sentence vectors
h2c, SENT = load_sentence_vectors(cur)
hashes = [None] * len(h2c)
for h, i in h2c.items():
    hashes[i] = h

print(f"SENT shape: {SENT.shape}")
assert SENT.shape[0] == 121614, f"Expected 121614 sentences, got {SENT.shape[0]}"
assert SENT.shape[1] == 1024, f"Expected 1024 dimensions, got {SENT.shape[1]}"

# Load sentence class labels (majority class per content_hash)
cur.execute("""
    SELECT content_hash, class_label, COUNT(*) as cnt
    FROM bank_sentences
    GROUP BY 1, 2
""")
class_by_hash_all = defaultdict(lambda: defaultdict(int))
for h, c, cnt in cur.fetchall():
    class_by_hash_all[h][c] += cnt

# Determine majority class per hash, restricted to [normal, falldown, fire, smoke]
valid_classes = {"normal", "falldown", "fire", "smoke"}
class_by_hash = {}
for h, class_counts in class_by_hash_all.items():
    # Filter to valid classes
    filtered = {c: v for c, v in class_counts.items() if c in valid_classes}
    if filtered:
        majority = max(filtered.items(), key=lambda x: (x[1], x[0]))[0]  # ties: alphabetical
        class_by_hash[h] = majority

# Load sentence text
cur.execute("""
    SELECT content_hash, MIN(text) FROM bank_sentences GROUP BY 1
""")
text_by_hash = {h: txt for h, txt in cur.fetchall()}

# Map sentence embeddings to classes
sent_classes = np.array([class_by_hash.get(hashes[i], None) for i in range(len(SENT))])
mask_valid = sent_classes != None
SENT_valid = SENT[mask_valid]
sent_classes_valid = sent_classes[mask_valid]
hashes_valid = np.array(hashes)[mask_valid]

print(f"Valid sentences with classes: {len(SENT_valid)}")
print(f"Class distribution: {dict(zip(*np.unique(sent_classes_valid, return_counts=True)))}")

class_sizes = {c: (sent_classes_valid == c).sum() for c in valid_classes}
total_valid = sum(class_sizes.values())
assert total_valid >= 119000, f"Expected ≥119000 valid sentences, got {total_valid}"
print(f"Per-class sizes: {class_sizes}")

# ============================================================================
# PART 1: PER-CLASS PCA
# ============================================================================
pca_results = {}
extremes_list = []

for cls in valid_classes:
    mask = sent_classes_valid == cls
    S_c = SENT_valid[mask]  # n_c x 1024
    n_c = len(S_c)

    # Center
    m_c = S_c.mean(axis=0)
    S_c_centered = S_c - m_c

    # PCA
    pca = PCA(n_components=100, svd_solver="randomized", random_state=0)
    pca.fit(S_c_centered)

    # Variance explained
    var_exp_cumsum = np.cumsum(pca.explained_variance_ratio_)
    var_pc1 = pca.explained_variance_ratio_[0]
    var_pc1_5 = var_exp_cumsum[4]  # PC1-5
    var_pc1_20 = var_exp_cumsum[19]  # PC1-20
    var_pc1_100 = var_exp_cumsum[99]  # PC1-100

    # Participation ratio
    lambdas = pca.explained_variance_
    pr = (lambdas.sum() ** 2) / (lambdas ** 2).sum()

    # Number of components for 50% and 90%
    n_for_50pct = np.argmax(var_exp_cumsum >= 0.5) + 1
    n_for_90pct = np.argmax(var_exp_cumsum >= 0.9) + 1

    # Coherence: ||m_c|| (already L2-normalized, so this is the "mean resultant length")
    coherence = np.linalg.norm(m_c)

    # PC1 projections
    proj_pc1 = S_c_centered @ pca.components_[0]

    # Top-8 positive and negative
    top_pos_idx = np.argsort(proj_pc1)[-8:][::-1]
    top_neg_idx = np.argsort(proj_pc1)[:8]

    for rank, idx in enumerate(top_pos_idx, 1):
        h = hashes_valid[mask][idx]
        txt = text_by_hash.get(h, "N/A")
        extremes_list.append([cls, "+", rank, float(proj_pc1[idx]), txt])

    for rank, idx in enumerate(top_neg_idx, 1):
        h = hashes_valid[mask][idx]
        txt = text_by_hash.get(h, "N/A")
        extremes_list.append([cls, "-", rank, float(proj_pc1[idx]), txt])

    pca_results[cls] = {
        "n_c": n_c,
        "coherence": float(coherence),
        "var_pc1": float(var_pc1),
        "var_pc1_5": float(var_pc1_5),
        "var_pc1_20": float(var_pc1_20),
        "var_pc1_100": float(var_pc1_100),
        "participation_ratio": float(pr),
        "n_for_50pct": int(n_for_50pct),
        "n_for_90pct": int(n_for_90pct),
        "pca": pca,
        "m_c": m_c,
        "S_c": S_c,
        "S_c_centered": S_c_centered,
    }

# Write Part 1 CSV
with open(os.path.join(CSV_DIR, "25_class_pca.csv"), "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["class", "n", "coherence", "var_pc1", "var_pc1_5", "var_pc1_20", "var_pc1_100",
                "participation_ratio", "n_for_50pct", "n_for_90pct"])
    for cls in valid_classes:
        r = pca_results[cls]
        w.writerow([cls, r["n_c"], r["coherence"], r["var_pc1"], r["var_pc1_5"],
                   r["var_pc1_20"], r["var_pc1_100"], r["participation_ratio"],
                   r["n_for_50pct"], r["n_for_90pct"]])

print(f"Wrote {os.path.join(CSV_DIR, '25_class_pca.csv')}")

# Write extremes CSV
with open(os.path.join(CSV_DIR, "25b_class_pc1_extremes.csv"), "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["class", "sign", "rank", "projection", "text"])
    for row in extremes_list:
        w.writerow(row)

print(f"Wrote {os.path.join(CSV_DIR, '25b_class_pc1_extremes.csv')} ({len(extremes_list)} rows)")

# ============================================================================
# PRINCIPAL ANGLES & DIFFERENCE DIRECTIONS
# ============================================================================
class_list = list(valid_classes)
angles_list = []

# Difference directions d_e = normalize(m_e - m_normal)
diff_dirs = {}
for cls in valid_classes:
    if cls != "normal":
        d = pca_results[cls]["m_c"] - pca_results["normal"]["m_c"]
        d = d / np.linalg.norm(d)
        diff_dirs[cls] = d

# Principal angles between all pairs
for i, cls_a in enumerate(class_list):
    for j, cls_b in enumerate(class_list):
        if i < j:
            # Top-10 PCs
            U_a = pca_results[cls_a]["pca"].components_[:10].T  # 1024 x 10
            U_b = pca_results[cls_b]["pca"].components_[:10].T  # 1024 x 10

            # SVD of U_a^T @ U_b
            _, sigma, _ = np.linalg.svd(U_a.T @ U_b)
            angles = np.arccos(np.clip(sigma, 0, 1)) * 180 / np.pi
            mean_angle = float(angles.mean())
            min_angle = float(angles.min())

            # Cosine between class means
            m_a = pca_results[cls_a]["m_c"]
            m_b = pca_results[cls_b]["m_c"]
            mean_cos = float((m_a @ m_b) / (np.linalg.norm(m_a) * np.linalg.norm(m_b)))

            # Cosine between difference directions (if both event classes)
            if cls_a != "normal" and cls_b != "normal":
                diffdir_cos = float(diff_dirs[cls_a] @ diff_dirs[cls_b])
            else:
                diffdir_cos = None

            angles_list.append([cls_a, cls_b, mean_angle, min_angle, mean_cos, diffdir_cos])

# Write angles CSV
with open(os.path.join(CSV_DIR, "25c_class_subspace_angles.csv"), "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["class_a", "class_b", "mean_angle_deg", "min_angle_deg", "mean_cos", "diffdir_cos"])
    for row in angles_list:
        w.writerow(row)

print(f"Wrote {os.path.join(CSV_DIR, '25c_class_subspace_angles.csv')}")

# ============================================================================
# PART 2: FRAMES PROJECTED ONTO CLASS DIRECTIONS
# ============================================================================
import fiftyone as fo
from fiftyone import ViewField as F

# Load frames dataset
try:
    fr_ds = fo.load_dataset("frames")
    fr = fr_ds.match(F("modality") == "frame")
    ncls_raw, emb = fr.values(["normalized_class", "image_embedding"])
    ncls = np.array([x or "none" for x in ncls_raw])
    emb_arr = np.array(emb, dtype=np.float32)
    # L2 normalize
    emb_arr /= np.linalg.norm(emb_arr, axis=1, keepdims=True)
    print(f"Frames dataset: {len(emb_arr)} frames, class distribution: {dict(zip(*np.unique(ncls, return_counts=True)))}")
except Exception as e:
    print(f"Warning: Could not load frames dataset: {e}")
    ncls = None
    emb_arr = None

# Load sourcei GT dataset
try:
    dh = fo.load_dataset("sourcei")
    ids, embh, gth, camh = dh.values(["id", "embedding", "ground_truth.label", "camera"])
    gth = np.array([x or "none" for x in gth])
    embh_arr = np.array(embh, dtype=np.float32)
    # L2 normalize
    embh_arr /= np.linalg.norm(embh_arr, axis=1, keepdims=True)
    print(f"sourcei GT: {len(embh_arr)} frames, class distribution: {dict(zip(*np.unique(gth, return_counts=True)))}")
except Exception as e:
    print(f"Warning: Could not load sourcei GT dataset: {e}")
    gth = None
    embh_arr = None

direction_auc_list = []

# Frames sample (if available)
if ncls is not None and emb_arr is not None:
    fire_idx = np.where(ncls == "fire")[0]
    smoke_idx = np.where(ncls == "smoke")[0]
    neg_idx = np.where(np.isin(ncls, ["none", "person"]))[0]

    RNG = np.random.default_rng(0)
    neg_sample = RNG.choice(neg_idx, min(20000, len(neg_idx)), replace=False)
    sub = np.concatenate([fire_idx, smoke_idx, neg_sample])

    X = np.asarray([emb_arr[i] for i in sub], dtype=np.float32)
    X /= np.linalg.norm(X, axis=1, keepdims=True)

    # Labels: 2=fire, 3=smoke, 0=other
    y_ref = np.zeros(len(sub), dtype=int)
    y_ref[:len(fire_idx)] = 2
    y_ref[len(fire_idx):len(fire_idx) + len(smoke_idx)] = 3

    print(f"Frames sample: {len(X)} total, fire={len(fire_idx)}, smoke={len(smoke_idx)}, neg={len(neg_sample)}")

    # Compute projections
    for event_cls in ["fire", "smoke"]:
        # d_e direction
        d_e = diff_dirs.get(event_cls)
        if d_e is not None:
            proj_de = X @ d_e
            y_binary = (y_ref == (2 if event_cls == "fire" else 3)).astype(int)
            if y_binary.sum() > 0 and (1 - y_binary).sum() > 0:
                auc_de = roc_auc_score(y_binary, proj_de)
                mean_pos = float(proj_de[y_binary == 1].mean())
                mean_neg = float(proj_de[y_binary == 0].mean())
                n_pos = int(y_binary.sum())
                n_neg = int((1 - y_binary).sum())
                direction_auc_list.append(["frames_sample", event_cls, "d_e", auc_de, mean_pos, mean_neg, n_pos, n_neg])

        # m_e direction
        m_e = pca_results[event_cls]["m_c"]
        proj_me = X @ m_e
        y_binary = (y_ref == (2 if event_cls == "fire" else 3)).astype(int)
        if y_binary.sum() > 0 and (1 - y_binary).sum() > 0:
            auc_me = roc_auc_score(y_binary, proj_me)
            mean_pos = float(proj_me[y_binary == 1].mean())
            mean_neg = float(proj_me[y_binary == 0].mean())
            n_pos = int(y_binary.sum())
            n_neg = int((1 - y_binary).sum())
            direction_auc_list.append(["frames_sample", event_cls, "m_e", auc_me, mean_pos, mean_neg, n_pos, n_neg])

        # PC1 direction
        pc1 = pca_results[event_cls]["pca"].components_[0]
        proj_pc1 = X @ pc1
        y_binary = (y_ref == (2 if event_cls == "fire" else 3)).astype(int)
        if y_binary.sum() > 0 and (1 - y_binary).sum() > 0:
            # Try both directions
            auc_pc1_pos = roc_auc_score(y_binary, proj_pc1)
            auc_pc1_neg = roc_auc_score(y_binary, -proj_pc1)
            auc_pc1 = max(auc_pc1_pos, auc_pc1_neg)
            # Use the direction with higher AUC
            if auc_pc1_pos >= auc_pc1_neg:
                proj_use = proj_pc1
            else:
                proj_use = -proj_pc1
            mean_pos = float(proj_use[y_binary == 1].mean())
            mean_neg = float(proj_use[y_binary == 0].mean())
            n_pos = int(y_binary.sum())
            n_neg = int((1 - y_binary).sum())
            direction_auc_list.append(["frames_sample", event_cls, "pc1", auc_pc1, mean_pos, mean_neg, n_pos, n_neg])

# sourcei GT (if available)
if gth is not None and embh_arr is not None:
    print(f"Computing AUC on sourcei GT...")
    for event_cls in ["falldown", "fire", "smoke"]:
        # d_e direction
        d_e = diff_dirs.get(event_cls)
        if d_e is not None:
            proj_de = embh_arr @ d_e
            y_binary = (gth == event_cls).astype(int)
            if y_binary.sum() > 0 and (1 - y_binary).sum() > 0:
                auc_de = roc_auc_score(y_binary, proj_de)
                mean_pos = float(proj_de[y_binary == 1].mean())
                mean_neg = float(proj_de[y_binary == 0].mean())
                n_pos = int(y_binary.sum())
                n_neg = int((1 - y_binary).sum())
                direction_auc_list.append(["sourcei_gt", event_cls, "d_e", auc_de, mean_pos, mean_neg, n_pos, n_neg])

        # m_e direction
        m_e = pca_results[event_cls]["m_c"]
        proj_me = embh_arr @ m_e
        y_binary = (gth == event_cls).astype(int)
        if y_binary.sum() > 0 and (1 - y_binary).sum() > 0:
            auc_me = roc_auc_score(y_binary, proj_me)
            mean_pos = float(proj_me[y_binary == 1].mean())
            mean_neg = float(proj_me[y_binary == 0].mean())
            n_pos = int(y_binary.sum())
            n_neg = int((1 - y_binary).sum())
            direction_auc_list.append(["sourcei_gt", event_cls, "m_e", auc_me, mean_pos, mean_neg, n_pos, n_neg])

        # PC1 direction
        pc1 = pca_results[event_cls]["pca"].components_[0]
        proj_pc1 = embh_arr @ pc1
        y_binary = (gth == event_cls).astype(int)
        if y_binary.sum() > 0 and (1 - y_binary).sum() > 0:
            auc_pc1_pos = roc_auc_score(y_binary, proj_pc1)
            auc_pc1_neg = roc_auc_score(y_binary, -proj_pc1)
            auc_pc1 = max(auc_pc1_pos, auc_pc1_neg)
            if auc_pc1_pos >= auc_pc1_neg:
                proj_use = proj_pc1
            else:
                proj_use = -proj_pc1
            mean_pos = float(proj_use[y_binary == 1].mean())
            mean_neg = float(proj_use[y_binary == 0].mean())
            n_pos = int(y_binary.sum())
            n_neg = int((1 - y_binary).sum())
            direction_auc_list.append(["sourcei_gt", event_cls, "pc1", auc_pc1, mean_pos, mean_neg, n_pos, n_neg])

# Write direction AUC CSV
with open(os.path.join(CSV_DIR, "25d_direction_auc.csv"), "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["dataset", "class", "direction", "auc", "mean_pos", "mean_neg", "n_pos", "n_neg"])
    for row in direction_auc_list:
        w.writerow(row)

print(f"Wrote {os.path.join(CSV_DIR, '25d_direction_auc.csv')} ({len(direction_auc_list)} rows)")

# ============================================================================
# FIGURES
# ============================================================================
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

# Load fonts
for f in glob.glob("/workspace/.fonts/*.tt[fc]"):
    fm.fontManager.addfont(f)
plt.rcParams["font.family"] = "Noto Sans CJK JP"

class_colors = {
    "normal": "#8a887f",
    "falldown": "#eda100",
    "fire": "#e34948",
    "smoke": "#4a3aa7",
}

# Figure 1: Variance spectrum and PR
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
fig.patch.set_facecolor("#fcfcfb")

for cls in valid_classes:
    r = pca_results[cls]
    pca = r["pca"]
    var_exp_cumsum = np.cumsum(pca.explained_variance_ratio_)
    comps = np.arange(1, 101)
    ax1.plot(comps, var_exp_cumsum, label=cls, color=class_colors[cls], linewidth=2)

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
print(f"Wrote {os.path.join(FIG_DIR, 'f28_class_pca_spectrum.png')}")
plt.close()

# Figure 2: Angles and AUC comparison
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
fig.patch.set_facecolor("#fcfcfb")

# Left: Angle heatmap
classes_for_heat = list(valid_classes)
angle_matrix = np.zeros((len(classes_for_heat), len(classes_for_heat)))
for cls_a, cls_b, mean_angle, _, _, _ in angles_list:
    i = classes_for_heat.index(cls_a)
    j = classes_for_heat.index(cls_b)
    angle_matrix[i, j] = mean_angle
    angle_matrix[j, i] = mean_angle

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

# Right: AUC comparison (sourcei GT only)
sourcei_aucs = [row for row in direction_auc_list if row[0] == "sourcei_gt"]

if sourcei_aucs:
    # Group by class and direction
    auc_by_class_dir = defaultdict(dict)
    for dataset, cls, direction, auc, _, _, _, _ in sourcei_aucs:
        auc_by_class_dir[cls][direction] = auc

    event_classes = ["falldown", "fire", "smoke"]
    directions = ["d_e", "m_e", "pc1"]
    x = np.arange(len(event_classes))
    width = 0.25

    for i, direction in enumerate(directions):
        auc_vals = [auc_by_class_dir[cls].get(direction, 0) for cls in event_classes]
        ax2.bar(x + i*width, auc_vals, width, label=direction)

    ax2.set_ylabel("AUC", fontsize=11)
    ax2.set_xticks(x + width)
    ax2.set_xticklabels(event_classes)
    ax2.legend()
    ax2.set_ylim([0, 1])
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

fig.suptitle("클래스 부공간 각도 및 방향별 AUC", fontsize=12, loc="left")
plt.tight_layout()
plt.savefig(os.path.join(FIG_DIR, "f29_class_angles_auc.png"), dpi=150, bbox_inches="tight")
print(f"Wrote {os.path.join(FIG_DIR, 'f29_class_angles_auc.png')}")
plt.close()

# ============================================================================
# SUMMARY JSON
# ============================================================================
summary = {
    "part1_pca": {cls: {k: v for k, v in pca_results[cls].items() if k != "pca" and k not in ["m_c", "S_c", "S_c_centered"]}
                  for cls in valid_classes},
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
print(f"SENT.shape == (121614, 1024): {SENT.shape == (121614, 1024)}")
print(f"Sum of class sizes: {total_valid} (≥119000: {total_valid >= 119000})")

for cls in valid_classes:
    r = pca_results[cls]
    var_exp = np.cumsum(r["pca"].explained_variance_ratio_)
    is_nondecreasing = np.all(np.diff(var_exp) >= -1e-6)
    is_leq_1 = np.all(var_exp <= 1.0001)
    print(f"{cls}: var non-decreasing={is_nondecreasing}, ≤1={is_leq_1}")

if ncls is not None and emb_arr is not None:
    print(f"Frames sample: {len(X)} total, fire={len(fire_idx)}, smoke={len(smoke_idx)}")

if gth is not None and embh_arr is not None:
    print(f"sourcei GT: {len(embh_arr)} frames")

for row in direction_auc_list[:5]:
    auc_val = row[3]
    print(f"AUC {row[0]} {row[1]} {row[2]}: {auc_val} (in [0,1]: {0 <= auc_val <= 1})")

print("\nDONE")
