#!/usr/bin/env python3
"""
C4: Cluster purity / NMI analysis
C5: Deployment-support audit

Run in container: docker exec docker-analysis-1 sh -c 'cd /workspace && python3 cluster_purity_support.py'
"""

import os
import sys
import json
import glob
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.patches import Rectangle
from scipy.spatial.distance import cosine, jensenshannon
from scipy.stats import entropy
from sklearn.metrics import normalized_mutual_info_score, adjusted_mutual_info_score
from sklearn.preprocessing import normalize

import fiftyone as fo

# ============================================================================
# CONFIG
# ============================================================================
PG_DSN = "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"
OUT_ROOT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CSV_DIR = os.path.join(OUT_ROOT, "csv")
FIG_DIR = os.path.join(OUT_ROOT, "fig")
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(FIG_DIR, exist_ok=True)

# Setup fonts
for font_path in glob.glob("/workspace/.fonts/*.tt[fc]"):
    fm.fontManager.addfont(font_path)
plt.rcParams["font.family"] = "Noto Sans CJK JP"

# Class colors
CLASS_COLORS = {
    "none": "#8a887f",
    "person": "#2a78d6",
    "fire": "#e34948",
    "smoke": "#4a3aa7",
}

print("[INFO] Starting cluster_purity_support.py", flush=True)

# ============================================================================
# LOAD FRAMES AND CLUSTERS
# ============================================================================
print("[STEP 1] Loading frame embeddings and cluster assignments...", flush=True)

conn = psycopg2.connect(PG_DSN)
cur = conn.cursor(name="embeddings")
cur.itersize = 5000

frame_embeddings = {}  # entity_id -> (embedding, project, source_key)
frame_count = 0

cur.execute("SELECT entity_id, embedding::text, source_key FROM image_embeddings WHERE entity_type='frame'")
for row in cur:
    entity_id, emb_str, source_key = row
    try:
        emb = np.fromstring(emb_str.strip("[]"), sep=",", dtype=np.float32)
        project = source_key.split("/")[0] if source_key else None
        frame_embeddings[entity_id] = (emb, project, source_key)
        frame_count += 1
    except Exception as e:
        print(f"[WARN] Failed to parse embedding for {entity_id}: {e}", flush=True)

print(f"[OK] Loaded {frame_count} frame embeddings", flush=True)
assert frame_count == 188190, f"Expected 188190 frame embeddings, got {frame_count}"

# Load cluster assignments
cur2 = conn.cursor(name="clusters", cursor_factory=RealDictCursor)
cur2.itersize = 5000

frame_clusters_kmeans = {}  # entity_id -> cluster_id (kmeans64)
frame_clusters_wp = {}      # entity_id -> (project, cluster_id) (wp16)
cluster_count_kmeans = 0
cluster_count_wp = 0

cur2.execute("SELECT entity_id, method, cluster_id, project FROM analysis.frame_cluster")
for row in cur2:
    entity_id = row["entity_id"]
    method = row["method"]
    cluster_id = row["cluster_id"]
    project = row["project"]

    if method == "kmeans64":
        frame_clusters_kmeans[entity_id] = cluster_id
        cluster_count_kmeans += 1
    elif method == "wp16":
        frame_clusters_wp[entity_id] = (project, cluster_id)
        cluster_count_wp += 1

print(f"[OK] Loaded {cluster_count_kmeans} kmeans64 and {cluster_count_wp} wp16 cluster assignments", flush=True)
assert cluster_count_kmeans == 90084, f"Expected 90084 kmeans64, got {cluster_count_kmeans}"
assert cluster_count_wp == 90084, f"Expected 90084 wp16, got {cluster_count_wp}"

conn.close()

# ============================================================================
# LOAD FIFTYONE FRAMES (SAM3 CLASSES)
# ============================================================================
print("[STEP 2] Loading FiftyOne frames dataset (SAM3 classes)...", flush=True)

frames_ds = fo.load_dataset("frames")
from fiftyone import ViewField as F
fr = frames_ds.match(F("modality") == "frame")

ent, ncls_raw, proj = fr.values(["entity_id", "normalized_class", "project"])
sam3_classes = {}  # entity_id -> class
distinct_classes = set()

for e, nc, p in zip(ent, ncls_raw, proj):
    cls = nc if nc else "none"
    sam3_classes[e] = cls
    distinct_classes.add(cls)

print(f"[OK] Loaded {len(sam3_classes)} frames with SAM3 classes", flush=True)
print(f"[OK] Distinct SAM3 classes: {sorted(distinct_classes)}", flush=True)

# ============================================================================
# LOAD sourcei GT
# ============================================================================
print("[STEP 3] Loading sourcei GT dataset...", flush=True)

dh = fo.load_dataset("sourcei")
ids, emb_list, gt_list, cam_list = dh.values(["id", "embedding", "ground_truth.label", "camera"])

sourcei_embeddings = {}  # id -> embedding
sourcei_gt_class = {}    # id -> class
sourcei_cameras = {}     # id -> camera

for fid, emb, gt, cam in zip(ids, emb_list, gt_list, cam_list):
    if emb is not None:
        sourcei_embeddings[fid] = np.array(emb, dtype=np.float32)
    sourcei_gt_class[fid] = gt if gt else "none"
    sourcei_cameras[fid] = cam

print(f"[OK] Loaded {len(sourcei_embeddings)} sourcei embeddings and {len(sourcei_gt_class)} GT labels", flush=True)
assert len(sourcei_embeddings) == 7498, f"Expected 7498 sourcei frames, got {len(sourcei_embeddings)}"

# ============================================================================
# C4: CLUSTER PURITY AND NMI
# ============================================================================
print("[STEP 4] Computing C4 - Cluster purity and NMI...", flush=True)

# Prepare data: join clusters with SAM3 classes
frame_with_class_list = []
for entity_id, cluster_id in frame_clusters_kmeans.items():
    if entity_id in sam3_classes:
        cls = sam3_classes[entity_id]
        project = None
        if entity_id in frame_embeddings:
            _, project, _ = frame_embeddings[entity_id]
        frame_with_class_list.append({
            "entity_id": entity_id,
            "cluster": cluster_id,
            "class": cls,
            "project": project,
        })

df_clustered = pd.DataFrame(frame_with_class_list)
n_dropped = cluster_count_kmeans - len(df_clustered)
print(f"[OK] {len(df_clustered)} frames have both cluster and SAM3 class; dropped {n_dropped}", flush=True)

# Build contingency tables
contingency_cluster_class = pd.crosstab(
    df_clustered["cluster"], df_clustered["class"], margins=False
)
contingency_cluster_project = pd.crosstab(
    df_clustered["cluster"], df_clustered["project"], margins=False
)

# Overall purity: max class per cluster / total
purity_kmeans = (contingency_cluster_class.max(axis=1).sum()) / len(df_clustered)

# NMI and AMI
nmi_cluster_class = normalized_mutual_info_score(
    df_clustered["cluster"], df_clustered["class"]
)
ami_cluster_class = adjusted_mutual_info_score(
    df_clustered["cluster"], df_clustered["class"]
)

nmi_cluster_project = normalized_mutual_info_score(
    df_clustered["cluster"], df_clustered["project"]
)
ami_cluster_project = adjusted_mutual_info_score(
    df_clustered["cluster"], df_clustered["project"]
)

# NMI(project, class) reference
nmi_project_class = normalized_mutual_info_score(
    df_clustered["project"], df_clustered["class"]
)
ami_project_class = adjusted_mutual_info_score(
    df_clustered["project"], df_clustered["class"]
)

print(f"[OK] Purity(kmeans64, class): {purity_kmeans:.4f}", flush=True)
print(f"[OK] NMI(cluster, class): {nmi_cluster_class:.4f}, AMI: {ami_cluster_class:.4f}", flush=True)
print(f"[OK] NMI(cluster, project): {nmi_cluster_project:.4f}, AMI: {ami_cluster_project:.4f}", flush=True)
print(f"[OK] NMI(project, class): {nmi_project_class:.4f}, AMI: {ami_project_class:.4f}", flush=True)

# Permutation null: shuffle class labels 100 times
null_nmi_values = []
for _ in range(100):
    class_shuffled = np.random.permutation(df_clustered["class"].values)
    nmi_null = normalized_mutual_info_score(
        df_clustered["cluster"].values, class_shuffled
    )
    null_nmi_values.append(nmi_null)

null_nmi_mean = np.mean(null_nmi_values)
null_nmi_p975 = np.percentile(null_nmi_values, 97.5)
print(f"[OK] Permutation null NMI: mean={null_nmi_mean:.4f}, p97.5={null_nmi_p975:.4f}", flush=True)

# Per-cluster statistics
cluster_stats = []
for cluster_id in sorted(contingency_cluster_class.index):
    row = contingency_cluster_class.loc[cluster_id]
    n_frames = row.sum()

    # Dominant class
    dominant_class = row.idxmax()
    dominant_class_share = row.max() / n_frames

    # Fire and smoke shares
    fire_share = (row.get("fire", 0)) / n_frames
    smoke_share = (row.get("smoke", 0)) / n_frames
    event_share = fire_share + smoke_share

    # Project info
    proj_row = contingency_cluster_project.loc[cluster_id]
    dominant_project = proj_row.idxmax()
    dominant_project_share = proj_row.max() / n_frames
    n_projects = (proj_row > 0).sum()

    # Entropy of class distribution
    class_probs = row / n_frames
    class_entropy = entropy(class_probs, base=2)

    cluster_stats.append({
        "cluster_id": cluster_id,
        "n_frames": n_frames,
        "dominant_class": dominant_class,
        "dominant_class_share": dominant_class_share,
        "fire_share": fire_share,
        "smoke_share": smoke_share,
        "event_share": event_share,
        "dominant_project": dominant_project,
        "dominant_project_share": dominant_project_share,
        "n_projects": n_projects,
        "class_entropy": class_entropy,
    })

df_cluster_stats = pd.DataFrame(cluster_stats)
df_cluster_stats = df_cluster_stats.sort_values("event_share", ascending=False)

print(f"[OK] Computed per-cluster statistics for {len(df_cluster_stats)} clusters", flush=True)

# Write C4 CSV
df_cluster_stats.to_csv(
    os.path.join(CSV_DIR, "28_cluster_purity.csv"),
    index=False,
    encoding="utf-8-sig"
)
print(f"[OK] Wrote 28_cluster_purity.csv", flush=True)

# C4 summary CSV
summary_rows = [
    {
        "clustering": "kmeans64",
        "target": "SAM3_class",
        "purity": purity_kmeans,
        "NMI": nmi_cluster_class,
        "AMI": ami_cluster_class,
        "null_mean": null_nmi_mean,
        "null_p97.5": null_nmi_p975,
    },
    {
        "clustering": "kmeans64",
        "target": "project",
        "purity": None,
        "NMI": nmi_cluster_project,
        "AMI": ami_cluster_project,
        "null_mean": None,
        "null_p97.5": None,
    },
    {
        "clustering": "reference",
        "target": "project_vs_class",
        "purity": None,
        "NMI": nmi_project_class,
        "AMI": ami_project_class,
        "null_mean": None,
        "null_p97.5": None,
    },
]

df_summary = pd.DataFrame(summary_rows)
df_summary.to_csv(
    os.path.join(CSV_DIR, "28b_cluster_nmi_summary.csv"),
    index=False,
    encoding="utf-8-sig"
)
print(f"[OK] Wrote 28b_cluster_nmi_summary.csv", flush=True)

# Top 5 clusters by event share
top_5 = df_cluster_stats.head(5)
print("[RESULT] Top 5 clusters by event share:", flush=True)
for idx, row in top_5.iterrows():
    print(f"  Cluster {row['cluster_id']}: n={row['n_frames']}, fire%={row['fire_share']*100:.1f}, "
          f"smoke%={row['smoke_share']*100:.1f}, project={row['dominant_project']}", flush=True)

n_high_event = (df_cluster_stats["event_share"] > 0.5).sum()
print(f"[RESULT] Clusters with event_share > 50%: {n_high_event}", flush=True)

# ============================================================================
# C5: DEPLOYMENT SUPPORT AUDIT
# ============================================================================
print("[STEP 5] Computing C5 - Deployment support audit...", flush=True)

# L2 normalize all frame embeddings
print("[STEP 5.1] L2-normalizing embeddings...", flush=True)
frame_embs_list = []
frame_ids_list = []
frame_projects_list = []

for entity_id, (emb, project, _) in frame_embeddings.items():
    emb_normalized = emb / (np.linalg.norm(emb) + 1e-8)
    frame_embs_list.append(emb_normalized)
    frame_ids_list.append(entity_id)
    frame_projects_list.append(project)

frame_embs_matrix = np.array(frame_embs_list, dtype=np.float32)  # (188190, 1024)
print(f"[OK] Frame embeddings matrix shape: {frame_embs_matrix.shape}", flush=True)

# Compute kmeans64 centroids from the 90084 labeled frames
print("[STEP 5.2] Computing kmeans64 centroids...", flush=True)
centroid_sums = {}
centroid_counts = {}

for entity_id, cluster_id in frame_clusters_kmeans.items():
    if entity_id in frame_embeddings:
        emb, _, _ = frame_embeddings[entity_id]
        emb_norm = emb / (np.linalg.norm(emb) + 1e-8)

        if cluster_id not in centroid_sums:
            centroid_sums[cluster_id] = np.zeros(1024, dtype=np.float32)
            centroid_counts[cluster_id] = 0

        centroid_sums[cluster_id] += emb_norm
        centroid_counts[cluster_id] += 1

centroids = {}
for cluster_id in sorted(centroid_sums.keys()):
    centroid = centroid_sums[cluster_id] / centroid_counts[cluster_id]
    centroid = centroid / (np.linalg.norm(centroid) + 1e-8)
    centroids[cluster_id] = centroid

print(f"[OK] Computed {len(centroids)} centroids", flush=True)

# Assign all 188190 frames to nearest centroid by cosine
print("[STEP 5.3] Assigning all frames to nearest centroid...", flush=True)
frame_assignments = {}  # entity_id -> cluster_id
for idx, entity_id in enumerate(frame_ids_list):
    emb = frame_embs_matrix[idx]

    best_cluster = -1
    best_sim = -np.inf
    for cluster_id, centroid in centroids.items():
        sim = np.dot(emb, centroid)  # cosine similarity
        if sim > best_sim:
            best_sim = sim
            best_cluster = cluster_id

    frame_assignments[entity_id] = best_cluster

print(f"[OK] Assigned {len(frame_assignments)} frames to clusters", flush=True)

# Check agreement on the 90084 labeled frames
agreement_count = 0
for entity_id, original_cluster in frame_clusters_kmeans.items():
    if frame_assignments[entity_id] == original_cluster:
        agreement_count += 1

agreement_rate = agreement_count / len(frame_clusters_kmeans)
print(f"[RESULT] Nearest-centroid agreement on labeled frames: {agreement_rate:.4f}", flush=True)

# Assign sourcei GT frames to nearest centroid
print("[STEP 5.4] Assigning sourcei frames to nearest centroid...", flush=True)
sourcei_assignments = {}  # sourcei_id -> cluster_id

for sourcei_id, emb in sourcei_embeddings.items():
    emb_norm = emb / (np.linalg.norm(emb) + 1e-8)

    best_cluster = -1
    best_sim = -np.inf
    for cluster_id, centroid in centroids.items():
        sim = np.dot(emb_norm, centroid)
        if sim > best_sim:
            best_sim = sim
            best_cluster = cluster_id

    sourcei_assignments[sourcei_id] = best_cluster

print(f"[OK] Assigned {len(sourcei_assignments)} sourcei frames to clusters", flush=True)

# Per-cluster deployment and labeled mass
print("[STEP 5.5] Computing per-cluster support statistics...", flush=True)

# Pre-build entity_id -> project mapping for fast lookup
entity_to_project = {}
for idx, entity_id in enumerate(frame_ids_list):
    entity_to_project[entity_id] = frame_projects_list[idx]

support_stats = []

for cluster_id in sorted(centroids.keys()):
    # Deployment mass
    deployment_frames = [
        entity_id for entity_id, cid in frame_assignments.items() if cid == cluster_id
    ]
    deployment_mass = len(deployment_frames) / len(frame_assignments)

    # Labeled mass
    sourcei_frames = [
        hid for hid, cid in sourcei_assignments.items() if cid == cluster_id
    ]
    labeled_mass = len(sourcei_frames) / len(sourcei_assignments)

    ratio = labeled_mass / deployment_mass if deployment_mass > 0 else 0

    # sourcei cameras in this cluster
    cameras_in_cluster = set()
    for hid in sourcei_frames:
        if hid in sourcei_cameras:
            cameras_in_cluster.add(sourcei_cameras[hid])
    n_cameras = len(cameras_in_cluster)

    # Deployment projects in this cluster
    projects_in_cluster = set()
    for entity_id in deployment_frames:
        project = entity_to_project.get(entity_id)
        if project:
            projects_in_cluster.add(project)
    n_projects = len(projects_in_cluster)

    # Count frames per project for dominant project
    project_counts = {}
    for entity_id in deployment_frames:
        project = entity_to_project.get(entity_id)
        if project:
            project_counts[project] = project_counts.get(project, 0) + 1

    dominant_project = max(project_counts, key=project_counts.get) if project_counts else None

    # sourcei GT class composition
    class_counts = {}
    for hid in sourcei_frames:
        cls = sourcei_gt_class.get(hid, "none")
        class_counts[cls] = class_counts.get(cls, 0) + 1

    support_stats.append({
        "cluster_id": cluster_id,
        "deployment_mass": deployment_mass,
        "labeled_mass": labeled_mass,
        "ratio_labeled_deployment": ratio,
        "n_sourcei_cameras": n_cameras,
        "n_deployment_projects": n_projects,
        "dominant_deployment_project": dominant_project,
        "sourcei_class_composition": json.dumps(class_counts),
        "n_sourcei_frames": len(sourcei_frames),
        "n_deployment_frames": len(deployment_frames),
    })

df_support = pd.DataFrame(support_stats)

# Deployment mass in zero-sourcei clusters
zero_sourcei_mass = df_support[df_support["n_sourcei_frames"] == 0]["deployment_mass"].sum()
print(f"[RESULT] Deployment mass in zero-sourcei clusters: {zero_sourcei_mass:.4f}", flush=True)

# Deployment mass in <= 1 camera clusters
one_camera_mass = df_support[df_support["n_sourcei_cameras"] <= 1]["deployment_mass"].sum()
print(f"[RESULT] Deployment mass in <=1-camera clusters: {one_camera_mass:.4f}", flush=True)

# JS divergence overall
deployment_dist = df_support["deployment_mass"].values
labeled_dist = df_support["labeled_mass"].values
js_div = jensenshannon(deployment_dist, labeled_dist)
print(f"[RESULT] JS divergence (deployment vs labeled): {js_div:.4f} bits", flush=True)

# Write C5 CSV
df_support.to_csv(
    os.path.join(CSV_DIR, "29_support_audit.csv"),
    index=False,
    encoding="utf-8-sig"
)
print(f"[OK] Wrote 29_support_audit.csv", flush=True)

# Per-project divergence
print("[STEP 5.6] Computing per-project divergence...", flush=True)

# Pre-build cluster_id -> n_sourcei_frames mapping
cluster_to_sourcei_count = {}
for _, row in df_support.iterrows():
    cluster_to_sourcei_count[row["cluster_id"]] = row["n_sourcei_frames"]

project_divergences = []

all_projects = set(frame_projects_list) - {None}
for project in sorted(all_projects):
    # Get cluster distribution for this project
    project_frames = [
        entity_id for entity_id, proj in zip(frame_ids_list, frame_projects_list)
        if proj == project
    ]
    project_cluster_counts = {}
    for entity_id in project_frames:
        cid = frame_assignments[entity_id]
        project_cluster_counts[cid] = project_cluster_counts.get(cid, 0) + 1

    # Build distribution
    project_dist = np.zeros(len(centroids))
    for cluster_id in sorted(centroids.keys()):
        project_dist[cluster_id] = project_cluster_counts.get(cluster_id, 0) / len(project_frames)

    # JS divergence to sourcei distribution
    js_div_project = jensenshannon(project_dist, labeled_dist)

    # Share of frames in zero-sourcei clusters
    zero_sourcei_share = 0
    for entity_id in project_frames:
        cid = frame_assignments[entity_id]
        if cluster_to_sourcei_count.get(cid, 0) == 0:
            zero_sourcei_share += 1
    zero_sourcei_share /= len(project_frames)

    project_divergences.append({
        "project": project,
        "n_frames": len(project_frames),
        "js_divergence_to_sourcei": js_div_project,
        "share_in_zero_sourcei_clusters": zero_sourcei_share,
    })

df_project_div = pd.DataFrame(project_divergences)
df_project_div = df_project_div.sort_values("js_divergence_to_sourcei")

# Write project divergence CSV
df_project_div.to_csv(
    os.path.join(CSV_DIR, "29b_support_by_project.csv"),
    index=False,
    encoding="utf-8-sig"
)
print(f"[OK] Wrote 29b_support_by_project.csv", flush=True)

# Most and least representative projects
most_rep = df_project_div.head(3)
least_rep = df_project_div.tail(3)

print("[RESULT] Most representative projects (lowest JS divergence):", flush=True)
for _, row in most_rep.iterrows():
    print(f"  {row['project']}: JS={row['js_divergence_to_sourcei']:.4f}", flush=True)

print("[RESULT] Least representative projects (highest JS divergence):", flush=True)
for _, row in least_rep.iterrows():
    print(f"  {row['project']}: JS={row['js_divergence_to_sourcei']:.4f}", flush=True)

# ============================================================================
# FIGURE 1: C4 CLUSTER PURITY
# ============================================================================
print("[STEP 6] Creating figure f32_cluster_purity.png...", flush=True)

fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(16, 10))
fig.patch.set_facecolor("#fcfcfb")
ax_left.set_facecolor("#fcfcfb")
ax_right.set_facecolor("#fcfcfb")

# Left: stacked bar chart of class composition
df_for_fig = df_cluster_stats.sort_values("event_share", ascending=False)
class_order = ["fire", "smoke", "person", "none"]

y_pos = np.arange(len(df_for_fig))
left_vals = np.zeros(len(df_for_fig))

for cls in class_order:
    cls_shares = []
    for _, row in df_for_fig.iterrows():
        cluster_id = int(row["cluster_id"])
        if cluster_id in contingency_cluster_class.index:
            row_data = contingency_cluster_class.loc[cluster_id]
            share = row_data.get(cls, 0) / row_data.sum() if row_data.sum() > 0 else 0
        else:
            share = 0
        cls_shares.append(share)

    cls_shares = np.array(cls_shares)
    color = CLASS_COLORS.get(cls, "#cccccc")
    ax_left.barh(y_pos, cls_shares, left=left_vals, label=cls, color=color, edgecolor="none", height=0.8)
    left_vals += cls_shares

# Add n on the right
for i, (_, row) in enumerate(df_for_fig.iterrows()):
    n = int(row["n_frames"])
    ax_left.text(1.02, i, f"n={n}", va="center", ha="left", fontsize=7)

ax_left.set_yticks(y_pos)
ax_left.set_yticklabels([f"C{int(row['cluster_id'])}" for _, row in df_for_fig.iterrows()], fontsize=8)
ax_left.set_xlabel("Fraction", fontsize=10)
ax_left.set_title("64 Clusters by Event Share (fire+smoke)", fontsize=11, loc="left", fontweight="bold")
ax_left.legend(loc="lower right", fontsize=9)
ax_left.spines["top"].set_visible(False)
ax_left.spines["right"].set_visible(False)
ax_left.set_xlim(0, 1.15)

# Right: NMI/AMI comparison
nmi_labels = ["NMI\n(cluster,class)", "NMI\n(cluster,project)", "NMI\n(project,class)"]
nmi_values = [nmi_cluster_class, nmi_cluster_project, nmi_project_class]
ami_values = [ami_cluster_class, ami_cluster_project, ami_project_class]

x_pos_nmi = np.arange(len(nmi_labels))
width = 0.35

bars1 = ax_right.bar(x_pos_nmi - width/2, nmi_values, width, label="NMI", color="#4a90e2", edgecolor="black", linewidth=1)
bars2 = ax_right.bar(x_pos_nmi + width/2, ami_values, width, label="AMI", color="#f5a623", edgecolor="black", linewidth=1)

# Add permutation null line
ax_right.axhline(y=null_nmi_mean, color="red", linestyle="--", linewidth=2, label=f"Null mean={null_nmi_mean:.3f}")
ax_right.axhline(y=null_nmi_p975, color="red", linestyle=":", linewidth=1, label=f"Null p97.5={null_nmi_p975:.3f}")

# Add value labels on bars
for bar in bars1:
    height = bar.get_height()
    ax_right.text(bar.get_x() + bar.get_width()/2, height + 0.02, f"{height:.3f}",
                  ha="center", va="bottom", fontsize=9)
for bar in bars2:
    height = bar.get_height()
    ax_right.text(bar.get_x() + bar.get_width()/2, height + 0.02, f"{height:.3f}",
                  ha="center", va="bottom", fontsize=9)

ax_right.set_ylabel("Score", fontsize=10)
ax_right.set_title(f"NMI/AMI: Clusters encode place, not events\n(Purity={purity_kmeans:.3f})",
                   fontsize=11, loc="left", fontweight="bold")
ax_right.set_xticks(x_pos_nmi)
ax_right.set_xticklabels(nmi_labels, fontsize=9)
ax_right.legend(loc="upper left", fontsize=8)
ax_right.spines["top"].set_visible(False)
ax_right.spines["right"].set_visible(False)
ax_right.set_ylim(0, max(nmi_values + ami_values + [null_nmi_p975]) * 1.15)

plt.tight_layout()
plt.savefig(os.path.join(FIG_DIR, "f32_cluster_purity.png"), dpi=150, bbox_inches="tight", facecolor="#fcfcfb")
plt.close()
print(f"[OK] Saved f32_cluster_purity.png", flush=True)

# ============================================================================
# FIGURE 2: C5 SUPPORT AUDIT
# ============================================================================
print("[STEP 7] Creating figure f33_support_audit.png...", flush=True)

fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(16, 10))
fig.patch.set_facecolor("#fcfcfb")
ax_left.set_facecolor("#fcfcfb")
ax_right.set_facecolor("#fcfcfb")

# Left: log-log scatter of deployment vs labeled mass
deployment_mass_vals = df_support["deployment_mass"].values
labeled_mass_vals = df_support["labeled_mass"].values
n_cameras_vals = df_support["n_sourcei_cameras"].values

# Handle zero values for log scale
eps = 1e-5
deployment_log = np.log10(np.maximum(deployment_mass_vals, eps))
labeled_log = np.log10(np.maximum(labeled_mass_vals, eps))

scatter = ax_left.scatter(
    deployment_log, labeled_log,
    s=50 + 100 * n_cameras_vals,  # size by n_cameras
    alpha=0.6,
    c="steelblue",
    edgecolors="black",
    linewidth=0.5
)

# y=x line
min_log = min(deployment_log.min(), labeled_log.min())
max_log = max(deployment_log.max(), labeled_log.max())
ax_left.plot([min_log, max_log], [min_log, max_log], "k--", linewidth=1.5, alpha=0.5, label="y=x")

# Annotate zero-labeled clusters as rug
zero_labeled_indices = np.where(labeled_mass_vals == 0)[0]
zero_labeled_deployment = deployment_log[zero_labeled_indices]
ax_left.scatter(zero_labeled_deployment, np.full_like(zero_labeled_deployment, min_log - 0.3),
                marker="|", s=200, c="red", alpha=0.7, linewidth=2)

zero_mass = df_support[df_support["labeled_mass"] == 0]["deployment_mass"].sum()

ax_left.set_xlabel("Log10(Deployment Mass)", fontsize=10)
ax_left.set_ylabel("Log10(Labeled Mass)", fontsize=10)
ax_left.set_title(f"Coverage gap: {zero_mass:.1%} deployment in zero-sourcei clusters",
                  fontsize=11, loc="left", fontweight="bold")
ax_left.legend(fontsize=9)
ax_left.spines["top"].set_visible(False)
ax_left.spines["right"].set_visible(False)
ax_left.grid(True, alpha=0.2)

# Right: JS divergence by project
df_proj_sorted = df_project_div.sort_values("js_divergence_to_sourcei", ascending=True)

y_pos_proj = np.arange(len(df_proj_sorted))
colors_proj = ["#e34948" if "sourcei" in p.lower() else "#4a3aa7"
               for p in df_proj_sorted["project"]]

ax_right.barh(y_pos_proj, df_proj_sorted["js_divergence_to_sourcei"], color=colors_proj, edgecolor="black", linewidth=0.5)

ax_right.set_yticks(y_pos_proj)
ax_right.set_yticklabels(df_proj_sorted["project"], fontsize=8)
ax_right.set_xlabel("JS Divergence to sourcei", fontsize=10)
ax_right.set_title("Project representativeness\n(lower JS = more similar to labeled distribution)",
                   fontsize=11, loc="left", fontweight="bold")
ax_right.spines["top"].set_visible(False)
ax_right.spines["right"].set_visible(False)

# Add value labels
for i, (_, row) in enumerate(df_proj_sorted.iterrows()):
    ax_right.text(row["js_divergence_to_sourcei"] + 0.01, i, f"{row['js_divergence_to_sourcei']:.3f}",
                  va="center", ha="left", fontsize=7)

plt.tight_layout()
plt.savefig(os.path.join(FIG_DIR, "f33_support_audit.png"), dpi=150, bbox_inches="tight", facecolor="#fcfcfb")
plt.close()
print(f"[OK] Saved f33_support_audit.png", flush=True)

# ============================================================================
# SUMMARY JSON
# ============================================================================
print("[STEP 8] Writing summary JSON...", flush=True)

summary_json = {
    "C4_cluster_purity": {
        "purity_kmeans64": float(purity_kmeans),
        "nmi_cluster_class": float(nmi_cluster_class),
        "ami_cluster_class": float(ami_cluster_class),
        "nmi_cluster_project": float(nmi_cluster_project),
        "ami_cluster_project": float(ami_cluster_project),
        "nmi_project_class": float(nmi_project_class),
        "ami_project_class": float(ami_project_class),
        "permutation_null_mean": float(null_nmi_mean),
        "permutation_null_p97.5": float(null_nmi_p975),
        "top_5_clusters_by_event_share": [
            {
                "cluster_id": int(row["cluster_id"]),
                "n_frames": int(row["n_frames"]),
                "fire_share": float(row["fire_share"]),
                "smoke_share": float(row["smoke_share"]),
                "dominant_project": str(row["dominant_project"]),
            }
            for _, row in top_5.iterrows()
        ],
        "n_clusters_with_event_share_gt_50pct": int(n_high_event),
    },
    "C5_deployment_support": {
        "nearest_centroid_agreement": float(agreement_rate),
        "deployment_mass_in_zero_sourcei_clusters": float(zero_sourcei_mass),
        "deployment_mass_in_le1_camera_clusters": float(one_camera_mass),
        "js_divergence_overall": float(js_div),
        "most_representative_projects": [
            {
                "project": row["project"],
                "js_divergence": float(row["js_divergence_to_sourcei"]),
            }
            for _, row in most_rep.iterrows()
        ],
        "least_representative_projects": [
            {
                "project": row["project"],
                "js_divergence": float(row["js_divergence_to_sourcei"]),
            }
            for _, row in least_rep.iterrows()
        ],
    },
    "data_info": {
        "n_frame_embeddings": int(frame_count),
        "n_sourcei_embeddings": int(len(sourcei_embeddings)),
        "n_clustered_frames": int(len(df_clustered)),
        "n_dropped_frames_no_class": int(n_dropped),
        "distinct_sam3_classes": sorted(list(distinct_classes)),
    },
}

with open(os.path.join(CSV_DIR, "cluster_purity_support_summary.json"), "w") as f:
    json.dump(summary_json, f, indent=2)
print(f"[OK] Wrote cluster_purity_support_summary.json", flush=True)

print("[DONE]", flush=True)
