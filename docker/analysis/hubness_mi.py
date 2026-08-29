#!/usr/bin/env python3
"""
Analysis A3: Hubness of prompt sentences + mutual information
between frame clusters and sentence clusters.
"""
import os, sys
sys.path.insert(0, "/workspace")
from prompt_cos_db import load_sentence_vectors

import numpy as np
import psycopg2
import json
import csv
import collections
import glob
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from scipy.stats import spearmanr
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import normalized_mutual_info_score, adjusted_mutual_info_score
import fiftyone as fo
from fiftyone import ViewField as F

# Setup fonts
[fm.fontManager.addfont(f) for f in glob.glob("/workspace/.fonts/*.tt[fc]")]
plt.rcParams["font.family"] = "Noto Sans CJK JP"

# Output directories
out_dir = "/data/fiftyone/frames_bank/report/sourcei_gt"
csv_dir = f"{out_dir}/csv"
fig_dir = f"{out_dir}/fig"
os.makedirs(csv_dir, exist_ok=True)
os.makedirs(fig_dir, exist_ok=True)

print("=" * 80)
print("Analysis A3: Hubness + Mutual Information")
print("=" * 80)

# ============================================================================
# LOAD SENTENCE VECTORS AND METADATA
# ============================================================================
print("\n[1] Loading sentence vectors and metadata...")
conn = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")
cur = conn.cursor()

# Load embeddings
h2c, SENT = load_sentence_vectors(cur)
print(f"  Loaded {len(h2c)} sentences, embedding shape {SENT.shape}")
assert SENT.shape == (121614, 1024), f"Expected (121614, 1024), got {SENT.shape}"

# Build inverse hash list
hashes = [None] * len(h2c)
for h, i in h2c.items():
    hashes[i] = h

# Load sentence text and majority class
cur.execute("""
    SELECT content_hash, class_label, count(*) as cnt
    FROM bank_sentences
    GROUP BY content_hash, class_label
    ORDER BY content_hash, cnt DESC
""")
rows = cur.fetchall()
sent_class = {}  # content_hash -> majority class
sent_text = {}   # content_hash -> text
for h, cls, cnt in rows:
    if h not in sent_class:
        sent_class[h] = cls

cur.execute("""
    SELECT content_hash, MIN(text) FROM bank_sentences
    GROUP BY content_hash
""")
for h, txt in cur.fetchall():
    sent_text[h] = txt

print(f"  Loaded text and class labels for {len(sent_class)} sentences")

# Load all prompt banks (skip versions starting with "v2.")
print("\n[1b] Loading prompt banks...")
from prompt_cos_db import load_banks
banks = load_banks(cur, None)
banks_to_process = [b for b in banks if not b["version"].startswith("v2.")]
print(f"  Loaded {len(banks_to_process)} banks (skipped v2.* versions)")
for b in banks_to_process:
    print(f"    - {b['version']}: {len(b['rows'])} sentences")

# ============================================================================
# LOAD FRAMES AND EMBEDDINGS
# ============================================================================
print("\n[2] Loading frame embeddings from FiftyOne...")
ds = fo.load_dataset("frames")
fr = ds.match(F("modality") == "frame")
ncls_raw, emb, ent = fr.values(["normalized_class", "image_embedding", "entity_id"])
ncls = np.array([x or "none" for x in ncls_raw])
print(f"  Loaded {len(ent)} frames")

# Background sample
RNG = np.random.default_rng(0)
neg_idx = np.where(np.isin(ncls, ["none", "person"]))[0]
print(f"  Available background frames (none/person): {len(neg_idx)}")
bg = RNG.choice(neg_idx, size=min(20000, len(neg_idx)), replace=False)
print(f"  Selected {len(bg)} background frames")

BG = np.asarray([emb[i] for i in bg], dtype=np.float32)
BG /= np.linalg.norm(BG, axis=1, keepdims=True)
assert BG.shape == (len(bg), 1024), f"Expected ({len(bg)}, 1024), got {BG.shape}"
print(f"  BG shape: {BG.shape}")

# ============================================================================
# PART 1: HUBNESS ANALYSIS
# ============================================================================
print("\n[3] Computing hubness (k-occurrence in top-10)...")

# Initialize global and per-bank Nk counters
Nk = np.zeros(len(h2c), dtype=np.int32)  # global occurrence in top-10
rowsum = np.zeros(len(h2c), dtype=np.float32)  # global sum of cosines

# Per-bank Nk storage
bank_Nk = {}
for bank in banks_to_process:
    bank_Nk[bank["version"]] = np.zeros(len(h2c), dtype=np.int32)

chunk_size = 1000
for s in range(0, len(bg), chunk_size):
    e = min(s + chunk_size, len(bg))
    S = BG[s:e] @ SENT.T  # shape (e-s, 121614) — computed ONCE per chunk

    # Global top-10 sentences per frame
    if S.shape[1] >= 10:
        top = np.argpartition(-S, 9, axis=1)[:, :10]
    else:
        top = np.arange(S.shape[1])  # fallback

    Nk += np.bincount(top.ravel(), minlength=len(h2c))
    rowsum += S.sum(0)

    # Per-bank analysis: for each bank, find top-10 within that bank's sentences only
    for bank in banks_to_process:
        rows = bank["rows"]
        cols = [h2c[h] for h, _, _ in rows if h in h2c]
        if len(cols) > 0:
            Sb = S[:, cols]  # restrict to this bank's sentences
            if len(cols) >= 10:
                top_b = np.argpartition(-Sb, 9, axis=1)[:, :10]
            else:
                top_b = np.argsort(-Sb, axis=1)[:, :len(cols)]
            # Map back to global indices
            top_b_global = np.array(cols)[top_b.ravel()]
            bank_Nk[bank["version"]] += np.bincount(top_b_global, minlength=len(h2c))

    if (s // chunk_size) % 10 == 0:
        print(f"  Processed {e}/{len(bg)} frames")

assert Nk.sum() == 200000, f"Expected Nk.sum() == 200000, got {Nk.sum()}"
m = rowsum / len(bg)  # main effect
print(f"  Nk.sum() = {Nk.sum()}")

# Hubness statistics
nk_mean = Nk.mean()
nk_std = Nk.std()
nk_skew = ((Nk - nk_mean) ** 3).mean() / (nk_std ** 3) if nk_std > 0 else 0
share_zero = (Nk == 0).sum() / len(Nk)
top1pct = int(0.01 * len(Nk))
top1pct_slots = Nk[np.argsort(-Nk)[:top1pct]].sum() / 200000
top100_slots = Nk[np.argsort(-Nk)[:100]].sum() / 200000

# Spearman correlation
rho, pval = spearmanr(Nk, m)

print(f"  Skewness of Nk: {nk_skew:.4f}")
print(f"  Share Nk==0: {share_zero:.4f}")
print(f"  Top 1% (1216) sentences: {top1pct_slots:.4f} of slots")
print(f"  Top 100 sentences: {top100_slots:.4f} of slots")
print(f"  Spearman(Nk, m): ρ={rho:.4f}, p={pval:.4e}")

# Class distribution of top-100 hubs vs all
top100_idx = np.argsort(-Nk)[:100]
top100_classes = collections.Counter([sent_class.get(hashes[i], "unknown") for i in top100_idx])
all_classes = collections.Counter([sent_class.get(h, "unknown") for h in hashes])
print(f"  Top-100 hub classes: {dict(top100_classes)}")
print(f"  All sentences classes: {dict(all_classes)}")

# ============================================================================
# PER-BANK HUBNESS ANALYSIS
# ============================================================================
print("\n[3b] Computing per-bank hubness statistics...")
bank_stats = []
for bank in banks_to_process:
    version = bank["version"]
    rows = bank["rows"]
    nk_b = bank_Nk[version]

    # Restrict to sentences in this bank
    bank_hashes = {h for h, _, _ in rows}
    bank_idx = [i for i in range(len(hashes)) if hashes[i] in bank_hashes]
    nk_b_restricted = nk_b[bank_idx]

    # Statistics
    n_sent_b = len(bank_idx)
    nk_mean_b = nk_b_restricted.mean()
    nk_std_b = nk_b_restricted.std()
    skew_b = ((nk_b_restricted - nk_mean_b) ** 3).mean() / (nk_std_b ** 3) if nk_std_b > 0 else 0
    share_zero_b = (nk_b_restricted == 0).sum() / len(nk_b_restricted) if len(nk_b_restricted) > 0 else 0

    # Top-1% slot share for this bank
    top1pct_b = int(0.01 * len(nk_b_restricted))
    top1pct_slots_b = nk_b_restricted[np.argsort(-nk_b_restricted)[:max(1, top1pct_b)]].sum() / 200000

    # Effective number of sentences (inverse Herfindahl)
    p_j = nk_b_restricted / 200000
    herfindahl = np.sum(p_j ** 2)
    effective_sentences = 1.0 / herfindahl if herfindahl > 0 else 0
    effective_share = effective_sentences / n_sent_b if n_sent_b > 0 else 0

    bank_stats.append({
        "version": version,
        "n_sentences": n_sent_b,
        "skewness": skew_b,
        "share_nk0": share_zero_b,
        "top1pct_slot_share": top1pct_slots_b,
        "effective_sentences": effective_sentences,
        "effective_share": effective_share
    })

    print(f"  {version}: n={n_sent_b}, skew={skew_b:.3f}, share_nk0={share_zero_b:.3f}, eff_share={effective_share:.3f}")

# Sort by version for output
bank_stats_sorted = sorted(bank_stats, key=lambda x: x["version"])

# Write hubness CSV
with open(f"{csv_dir}/23_hubness.csv", "w", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(["rank", "content_hash", "class", "Nk", "share", "main_effect_m", "text"])
    for rank, idx in enumerate(np.argsort(-Nk)[:300], 1):
        h = hashes[idx]
        cls = sent_class.get(h, "unknown")
        txt = sent_text.get(h, "")[:100]  # truncate
        share = Nk[idx] / 200000
        writer.writerow([rank, h, cls, Nk[idx], f"{share:.6f}", f"{m[idx]:.6f}", txt])

# Write hubness distribution
bins_lo = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]
bins_hi = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 10000]
with open(f"{csv_dir}/23b_hubness_dist.csv", "w", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(["bin_lo", "bin_hi", "n_sentences"])
    for lo, hi in zip(bins_lo, bins_hi):
        count = ((Nk >= lo) & (Nk < hi)).sum()
        writer.writerow([lo, hi, count])

# Write per-bank hubness CSV
with open(f"{csv_dir}/23c_hubness_by_bank.csv", "w", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(["bank", "n_sentences", "skewness", "share_nk0", "top1pct_slot_share", "effective_sentences", "effective_share"])
    for stats in bank_stats_sorted:
        writer.writerow([
            stats["version"],
            stats["n_sentences"],
            f"{stats['skewness']:.6f}",
            f"{stats['share_nk0']:.6f}",
            f"{stats['top1pct_slot_share']:.6f}",
            f"{stats['effective_sentences']:.2f}",
            f"{stats['effective_share']:.6f}"
        ])

# Figure: hubness
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
fig.patch.set_facecolor("#fcfcfb")

# Left: Nk distribution (log-log)
ax1.hist(Nk[Nk > 0], bins=np.logspace(0, np.log2(Nk.max()), 30, base=2), color="#8a887f", alpha=0.7)
ax1.set_xscale("log")
ax1.set_yscale("log")
ax1.set_xlabel("k-occurrence (Nk)", fontsize=11)
ax1.set_ylabel("count (log scale)", fontsize=11)
ax1.spines["top"].set_visible(False)
ax1.spines["right"].set_visible(False)
ax1.set_title(f"Hubness Distribution (skew={nk_skew:.2f}, top-1%={top1pct_slots:.2%})",
              loc="left", fontsize=11, fontweight="bold")

# Right: Top-25 hubs
top25_idx = np.argsort(-Nk)[:25]
y_pos = np.arange(len(top25_idx))
colors = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
bar_colors = [colors.get(sent_class.get(hashes[i], "unknown"), "#2a78d6") for i in top25_idx]
labels = [sent_text.get(hashes[i], "")[:70] for i in top25_idx]
ax2.barh(y_pos, Nk[top25_idx], color=bar_colors, alpha=0.8)
ax2.set_yticks(y_pos)
ax2.set_yticklabels(labels, fontsize=9)
ax2.set_xlabel("k-occurrence (Nk)", fontsize=11)
ax2.spines["top"].set_visible(False)
ax2.spines["right"].set_visible(False)
ax2.set_title("Top-25 Hub Sentences", loc="left", fontsize=11, fontweight="bold")
ax2.invert_yaxis()

plt.tight_layout()
plt.savefig(f"{fig_dir}/f26_hubness.png", dpi=100, bbox_inches="tight")
plt.close()
print(f"  Wrote fig/f26_hubness.png")

# Figure: per-bank hubness
fig, ax = plt.subplots(figsize=(10, 6))
fig.patch.set_facecolor("#fcfcfb")

# Sort banks by version for y-axis
banks_sorted = sorted(bank_stats, key=lambda x: x["version"])
versions = [b["version"] for b in banks_sorted]
eff_shares = [b["effective_share"] for b in banks_sorted]

# Plot
y_pos = np.arange(len(versions))
ax.barh(y_pos, eff_shares, color="#8a887f", alpha=0.7)
ax.set_yticks(y_pos)
ax.set_yticklabels(versions, fontsize=10)
ax.set_xscale("log")
ax.set_xlabel("Effective Share (log scale)", fontsize=11)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_title("Hubness Concentration by Prompt Bank (effective_sentences / n_sentences)",
             loc="left", fontsize=11, fontweight="bold")
plt.tight_layout()
plt.savefig(f"{fig_dir}/f26b_hubness_by_bank.png", dpi=100, bbox_inches="tight")
plt.close()
print(f"  Wrote fig/f26b_hubness_by_bank.png")

# Report per-bank stats
eff_shares_arr = np.array(eff_shares)
print(f"  Per-bank effective_share: min={eff_shares_arr.min():.4f}, median={np.median(eff_shares_arr):.4f}, max={eff_shares_arr.max():.4f}")
most_hub = sorted(bank_stats, key=lambda x: x["effective_share"], reverse=True)[:3]
least_hub = sorted(bank_stats, key=lambda x: x["effective_share"])[:3]
print(f"  Most hub-dominated (high eff_share): {[b['version'] for b in most_hub]}")
print(f"  Least hub-dominated (low eff_share): {[b['version'] for b in least_hub]}")

# ============================================================================
# PART 2: MUTUAL INFORMATION
# ============================================================================
print("\n[4] Loading frame clusters...")
cur.execute("""
    SELECT entity_id, cluster_id FROM analysis.frame_cluster
    WHERE method = 'kmeans64'
""")
frame_cluster_map = {}
for ent_id, cid in cur.fetchall():
    frame_cluster_map[ent_id] = cid

print(f"  Loaded clusters for {len(frame_cluster_map)} frames")

# Build mapping from background frame indices to entity_ids
bg_ents = [ent[i] for i in bg]
print(f"  Selected {len(bg_ents)} background frame entity_ids")

# Recompute top-1 sentences and assign to clusters
print("\n[5] Computing frame-sentence cluster associations...")
top1_sent_idx = []  # top-1 sentence index for each background frame
for s in range(0, len(bg), chunk_size):
    e = min(s + chunk_size, len(bg))
    S = BG[s:e] @ SENT.T
    top1_sent_idx.extend(S.argmax(1))
    if (s // chunk_size) % 10 == 0:
        print(f"  Processed {e}/{len(bg)} frames")

assert len(top1_sent_idx) == len(bg)
top1_sent_idx = np.array(top1_sent_idx)

# Sentence clustering
print("\n[6] Clustering sentences (MiniBatchKMeans, k=24)...")
km = MiniBatchKMeans(n_clusters=24, random_state=0, batch_size=8192, n_init=3)
sc = km.fit_predict(SENT)  # sentence cluster labels
print(f"  Sentence cluster sizes: {np.bincount(sc)}")

# Build contingency table: frame cluster x sentence cluster
frame_labels = []
sent_labels = []
used_frames = 0
for i, ent_id in enumerate(bg_ents):
    if ent_id in frame_cluster_map:
        frame_labels.append(frame_cluster_map[ent_id])
        sent_labels.append(sc[top1_sent_idx[i]])
        used_frames += 1

frame_labels = np.array(frame_labels)
sent_labels = np.array(sent_labels)
print(f"  Using {used_frames} frames with both frame cluster and top-1 sentence")

# Contingency table
C = np.zeros((64, 24), dtype=np.int32)
for f, s in zip(frame_labels, sent_labels):
    C[f, s] += 1
print(f"  Contingency table shape: {C.shape}, sum: {C.sum()}")
assert C.sum() == used_frames, f"Expected sum {used_frames}, got {C.sum()}"

# Mutual information
p_joint = C / C.sum()
p_frame = C.sum(axis=1) / C.sum()
p_sent = C.sum(axis=0) / C.sum()

# MI in bits
H_frame = -np.sum(p_frame[p_frame > 0] * np.log2(p_frame[p_frame > 0]))
H_sent = -np.sum(p_sent[p_sent > 0] * np.log2(p_sent[p_sent > 0]))

mi_observed = 0.0
for i in range(64):
    for j in range(24):
        if p_joint[i, j] > 0:
            mi_observed += p_joint[i, j] * np.log2(p_joint[i, j] / (p_frame[i] * p_sent[j]))

nmi_formula1 = mi_observed / np.sqrt(H_frame * H_sent) if H_frame > 0 and H_sent > 0 else 0
nmi_sklearn = normalized_mutual_info_score(frame_labels, sent_labels)
ami_sklearn = adjusted_mutual_info_score(frame_labels, sent_labels)

print(f"  MI (observed): {mi_observed:.4f} bits")
print(f"  H(frame): {H_frame:.4f}, H(sent): {H_sent:.4f}")
print(f"  NMI (formula): {nmi_formula1:.4f}")
print(f"  NMI (sklearn): {nmi_sklearn:.4f}")
print(f"  AMI (sklearn): {ami_sklearn:.4f}")

# Permutation null
print("\n[7] Computing permutation null (200 permutations)...")
mi_null = []
for perm_idx in range(200):
    sent_labels_perm = RNG.permutation(sent_labels)
    mi_perm = 0.0
    for i in range(64):
        for j in range(24):
            mask = (frame_labels == i) & (sent_labels_perm == j)
            p_ij = mask.sum() / len(frame_labels)
            if p_ij > 0:
                mi_perm += p_ij * np.log2(p_ij / (p_frame[i] * p_sent[j]))
    mi_null.append(mi_perm)
    if (perm_idx + 1) % 50 == 0:
        print(f"  Permutation {perm_idx + 1}/200")

mi_null = np.array(mi_null)
mi_null_mean = mi_null.mean()
mi_null_975 = np.percentile(mi_null, 97.5)
mi_z = (mi_observed - mi_null_mean) / (mi_null.std() + 1e-10)

print(f"  MI null mean: {mi_null_mean:.4f}, 97.5%: {mi_null_975:.4f}")
print(f"  Z-score: {mi_z:.4f}")

# Lift matrix
L = p_joint / (p_frame[:, None] * p_sent[None, :])
L = np.nan_to_num(L, 0)

# Per-frame-cluster entropy and top sentence cluster
frame_entropy = []
frame_top_sent = []
frame_top_lift = []
for i in range(64):
    p_i = C[i, :] / C[i, :].sum()
    h_i = -np.sum(p_i[p_i > 0] * np.log2(p_i[p_i > 0]))
    frame_entropy.append(h_i)
    top_j = np.argmax(C[i, :])
    frame_top_sent.append(top_j)
    frame_top_lift.append(L[i, top_j])

frame_entropy = np.array(frame_entropy)
frame_top_sent = np.array(frame_top_sent)
frame_top_lift = np.array(frame_top_lift)

# Sentence cluster info
sent_cluster_info = {}
for j in range(24):
    # Most frequent words in this cluster
    words_freq = collections.Counter()
    stopwords = {
        "a", "an", "the", "of", "in", "on", "at", "to", "with", "and", "or",
        "is", "are", "by", "for", "from", "into", "near", "under", "over",
        "as", "its", "their", "his", "her", "this", "that", "there", "it"
    }
    for idx in np.where(sc == j)[0]:
        txt = sent_text.get(hashes[idx], "")
        for word in txt.lower().split():
            word = word.strip(".,!?;:\"'()-")
            if word and word not in stopwords and len(word) > 2:
                words_freq[word] += 1
    top_words = [w for w, _ in words_freq.most_common(8)]

    # Class distribution
    class_freq = collections.Counter()
    for idx in np.where(sc == j)[0]:
        cls = sent_class.get(hashes[idx], "unknown")
        class_freq[cls] += 1

    sent_cluster_info[j] = {
        "n": (sc == j).sum(),
        "top_words": top_words,
        "class_freq": dict(class_freq)
    }

# Write sentence cluster info
with open(f"{csv_dir}/24b_sentence_clusters.csv", "w", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(["cluster", "n_sentences", "top_words", "class_normal", "class_falldown", "class_fire", "class_smoke"])
    for j in range(24):
        info = sent_cluster_info[j]
        top_words = " ".join(info["top_words"])
        cf = info["class_freq"]
        writer.writerow([
            j,
            info["n"],
            top_words,
            cf.get("normal", 0),
            cf.get("falldown", 0),
            cf.get("fire", 0),
            cf.get("smoke", 0)
        ])

# Write frame-sentence MI CSV
frame_cluster_sizes = C.sum(axis=1)
with open(f"{csv_dir}/24_frame_sentence_cluster_mi.csv", "w", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(["frame_cluster", "n_frames", "entropy_bits", "top_sentence_cluster", "top_lift", "top_words"])
    for i in range(64):
        j = frame_top_sent[i]
        info = sent_cluster_info[j]
        top_words = " ".join(info["top_words"][:3])
        writer.writerow([i, frame_cluster_sizes[i], f"{frame_entropy[i]:.4f}", j, f"{frame_top_lift[i]:.4f}", top_words])

# Write lift matrix
with open(f"{csv_dir}/24c_lift_matrix.csv", "w", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(["frame_cluster"] + [f"sent_cluster_{j}" for j in range(24)])
    for i in range(64):
        writer.writerow([i] + [f"{L[i, j]:.4f}" for j in range(24)])

# Figure: MI heatmap
fig, ax = plt.subplots(figsize=(12, 10))
fig.patch.set_facecolor("#fcfcfb")

# Sort frame clusters by entropy (low first)
sort_idx = np.argsort(frame_entropy)
L_sorted = L[sort_idx, :]
entropy_sorted = frame_entropy[sort_idx]

# Plot heatmap
im = ax.imshow(np.log2(L_sorted + 1e-10), cmap="RdBu_r", vmin=-2, vmax=2, aspect="auto")

# Label y-axis with frame cluster + entropy
y_labels = [f"FC{sort_idx[i]} (H={entropy_sorted[i]:.2f})" for i in range(64)]
ax.set_yticks(range(64))
ax.set_yticklabels(y_labels, fontsize=8)

# Label x-axis with sentence cluster top words
x_labels = [" ".join(sent_cluster_info[j]["top_words"][:3]) for j in range(24)]
ax.set_xticks(range(24))
ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=9)

ax.set_ylabel("Frame Cluster (sorted by entropy)", fontsize=11)
ax.set_xlabel("Sentence Cluster", fontsize=11)
ax.set_title(f"Frame-Sentence Cluster MI (MI={mi_observed:.3f} bits, NMI={nmi_sklearn:.3f}, z={mi_z:.2f})",
             loc="left", fontsize=11, fontweight="bold")

cbar = plt.colorbar(im, ax=ax, label="log2(Lift)")
plt.tight_layout()
plt.savefig(f"{fig_dir}/f27_frame_sentence_mi.png", dpi=100, bbox_inches="tight")
plt.close()
print(f"  Wrote fig/f27_frame_sentence_mi.png")

# ============================================================================
# SUMMARY JSON
# ============================================================================
print("\n[8] Writing summary JSON...")

# Prepare per-bank summary
bank_summary = []
for stats in bank_stats_sorted:
    bank_summary.append({
        "version": stats["version"],
        "n_sentences": stats["n_sentences"],
        "skewness": float(stats["skewness"]),
        "share_nk0": float(stats["share_nk0"]),
        "top1pct_slot_share": float(stats["top1pct_slot_share"]),
        "effective_sentences": float(stats["effective_sentences"]),
        "effective_share": float(stats["effective_share"])
    })

summary = {
    "hubness": {
        "global": {
            "skewness_Nk": float(nk_skew),
            "share_Nk_zero": float(share_zero),
            "top_1pct_slot_share": float(top1pct_slots),
            "top_100_slot_share": float(top100_slots),
            "spearman_Nk_m_rho": float(rho),
            "spearman_Nk_m_pval": float(pval),
            "top_100_hub_classes": dict(top100_classes),
            "all_sentences_classes": dict(all_classes)
        },
        "per_bank": bank_summary
    },
    "mutual_information": {
        "mi_observed_bits": float(mi_observed),
        "H_frame_bits": float(H_frame),
        "H_sent_bits": float(H_sent),
        "nmi_formula": float(nmi_formula1),
        "nmi_sklearn": float(nmi_sklearn),
        "ami_sklearn": float(ami_sklearn),
        "permutation_null_mean": float(mi_null_mean),
        "permutation_null_975": float(mi_null_975),
        "permutation_z_score": float(mi_z),
        "n_frames_with_cluster": int(used_frames)
    },
    "frame_clusters": {
        "lowest_entropy": [
            {
                "frame_cluster": int(sort_idx[i]),
                "entropy_bits": float(frame_entropy[sort_idx[i]]),
                "top_sentence_cluster": int(frame_top_sent[sort_idx[i]]),
                "top_lift": float(frame_top_lift[sort_idx[i]]),
                "top_words": sent_cluster_info[int(frame_top_sent[sort_idx[i]])]["top_words"]
            }
            for i in range(5)
        ]
    }
}

with open(f"{out_dir}/hubness_mi_summary.json", "w") as f:
    json.dump(summary, f, indent=2)
print(f"  Wrote hubness_mi_summary.json")

print("\n" + "=" * 80)
print("DONE")
