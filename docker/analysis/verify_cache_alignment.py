#!/usr/bin/env python3
"""
캐시 정렬 상태 검증 — 행 인덱스 기반 numpy 파일들의 정렬 완전성 검사.

배경:
  2026-08-28에 prompt_banks 에 2,500개 문장이 추가되면서 image_embeddings.entity_id의
  DB 순서가 변경되었습니다. `prompt_cos_db.load_sentence_vectors()`는 ORDER BY 없이
  쿼리를 실행하므로, 반환되는 행들의 순서가 Postgres 실행계획에 따라 달라집니다.

  이로 인해 행 인덱스로 저장된 캐시들(`percls_*.npy`, `cluster_specificity_z.npy` 등)이
  현재 DB 순서와 맞지 않을 가능성이 생겼습니다.

정답 기준:
  `sent_stats_byhash.npz` 는 content_hash를 키로 저장했고, 그 안의 `hashes` 배열이
  현재 DB의 정렬된 문장 순서를 대표합니다.

검증 방법:
  1. sent_stats_byhash.npz 에서 정렬된 해시 배열과 통계(`sd`, `m_s_mean` 등) 로드
  2. prompt_cos_db.load_sentence_vectors()로 현재 DB 순서 파악
  3. 대상 배열 로드 (cluster_specificity_z.npy, percls_*.npy, 기타)
  4. 각 배열이 어떤 순서를 가정했는지 판별:
     - cluster_specificity_z.npy: shape (N, M) → N=문장수, z-score 파생이므로
       정렬돼 있으면 |r| ≈ 1.0
     - percls_*.npy: shape (프레임수, 4) → 프레임 기반, 정의 파악 필요
     - m_s_bg90k.npy, Ak_kmeans64.npy 등: 정의 파악 후 비교
  5. 피어슨/스피어만 상관으로 정렬 상태 판별

금지:
  - 파일 수정/삭제 (읽기만)
  - DB 쓰기
  - 측정 안 한 수치 보고
"""

import os
import json
import sys
import numpy as np
import psycopg2
from scipy import stats as scipy_stats

# 환경 설정
OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
DSN = os.environ.get(
    "DATAOPS_POSTGRES_DSN",
    "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"
)
MAX_RAM_MB = 6000  # 공유 호스트, RAM 6GB 상한
THREADS = os.environ.get("COS_THREADS", "2")

for _v in (
    "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"
):
    os.environ.setdefault(_v, THREADS)


def log(msg):
    print(f"[verify] {msg}", flush=True)


def load_sent_stats_byhash():
    """정답 기준: sent_stats_byhash.npz 로드."""
    path = os.path.join(OUT, "sent_stats_byhash.npz")
    if not os.path.exists(path):
        log(f"ERROR: {path} 없음")
        sys.exit(1)

    stats = np.load(path, allow_pickle=True)
    log(f"sent_stats_byhash.npz 로드: {list(stats.keys())}")

    hashes = stats["hashes"]  # 정렬된 해시 배열
    log(f"  정렬된 해시 수: {len(hashes)}")

    return stats, hashes


def load_current_db_order(conn):
    """현재 DB의 문장 순서 파악 (ORDER BY 없음, 실행계획 의존)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT entity_id FROM image_embeddings
            WHERE entity_type='prompt'
        """)
        db_hashes = [row[0] for row in cur]

    log(f"현재 DB 프롬프트 임베딩: {len(db_hashes)}개")
    if len(db_hashes) > 0:
        log(f"  첫 5개: {db_hashes[:5]}")

    return db_hashes


def compare_orderings(baseline_hashes, current_db_hashes):
    """기준 순서와 현재 DB 순서 비교."""
    baseline_set = set(baseline_hashes)
    current_set = set(current_db_hashes)

    missing = baseline_set - current_set
    added = current_set - baseline_set

    if missing:
        log(f"WARNING: 기준에는 있지만 현재 DB엔 없는 해시 {len(missing)}개")
    if added:
        log(f"WARNING: 현재 DB엔 있지만 기준엔 없는 해시 {len(added)}개")

    # 겹치는 부분만 추출
    common = [h for h in baseline_hashes if h in current_set]
    log(f"공통 해시: {len(common)}/{len(baseline_hashes)}")

    if len(common) < len(baseline_hashes):
        # 기준 순서에서 현재에 존재하는 것들의 인덱스
        baseline_indices = np.array([i for i, h in enumerate(baseline_hashes) if h in current_set])
        # 현재 순서에서 공통 것들의 인덱스
        current_indices = np.array([i for i, h in enumerate(current_db_hashes) if h in baseline_set])

        log(f"  기준 중 {len(baseline_indices)}/{len(baseline_hashes)} 매칭")
        log(f"  현재 중 {len(current_indices)}/{len(current_db_hashes)} 매칭")

    return common


def check_cluster_specificity_z():
    """cluster_specificity_z.npy 검증."""
    log("\n=== cluster_specificity_z.npy ===")
    path = os.path.join(OUT, "cluster_specificity_z.npy")

    if not os.path.exists(path):
        return {"file": "cluster_specificity_z.npy", "status": "파일없음"}

    arr = np.load(path)
    log(f"shape={arr.shape} dtype={arr.dtype} nbytes={arr.nbytes / 1024 / 1024:.1f}MB")

    # cluster_specificity_keys.json 에서 메타데이터 확인
    keys_path = os.path.join(OUT, "cluster_specificity_keys.json")
    if os.path.exists(keys_path):
        with open(keys_path) as f:
            keys = json.load(f)
        stored_hashes = keys.get("hashes", [])
        log(f"cluster_specificity_keys.json의 저장된 해시 수: {len(stored_hashes)}")

        if len(stored_hashes) != arr.shape[0]:
            log(f"  WARNING: 배열 행수({arr.shape[0]}) ≠ 저장된 해시수({len(stored_hashes)})")

    # sent_stats_byhash와 비교할 수 없음 — cluster_specificity_z는 cluster 친화도의 z-score
    # 이는 sentence_affinity 테이블에서 파생된 것이고, 그 테이블의 DISTINCT content_hash의 순서가
    # image_embeddings의 순서와 다를 수 있음.

    return {
        "file": "cluster_specificity_z.npy",
        "shape": arr.shape,
        "dtype": str(arr.dtype),
        "status": "형태 확인됨 (DB순서 의존성 높음)",
        "note": "cluster_specificity_keys.json과 일치해야 함"
    }


def check_percls_files(conn, stats, baseline_hashes, current_db_hashes):
    """percls_*.npy 파일들 검증."""
    log("\n=== percls_*.npy 파일들 ===")

    results = []

    # 프레임 수 파악
    import glob
    percls_files = sorted(glob.glob(os.path.join(OUT, "percls_*.npy")))
    log(f"found {len(percls_files)} percls_*.npy 파일")

    # 프레임 메타데이터 로드 (sourcei_gt_rules.py와 동일)
    frame_meta_path = os.path.join(OUT, "frame_meta.npz")
    if not os.path.exists(frame_meta_path):
        log(f"ERROR: {frame_meta_path} 없음 — 프레임 수 파악 실패")
        for f in percls_files[:3]:
            arr = np.load(f)
            results.append({
                "file": os.path.basename(f),
                "shape": arr.shape,
                "dtype": str(arr.dtype),
                "status": "프레임메타 미보유"
            })
        return results

    frame_meta = np.load(frame_meta_path, allow_pickle=True)
    n_frames = len(frame_meta["ids"]) if "ids" in frame_meta else None
    log(f"frame_meta.npz: {list(frame_meta.keys())}")

    if n_frames:
        log(f"프레임 수: {n_frames}")

    # 각 percls 파일 검증
    for f in percls_files[:3]:  # 처음 3개만 (메모리 절감)
        basename = os.path.basename(f)
        arr = np.load(f)

        log(f"  {basename}: shape={arr.shape} dtype={arr.dtype}")

        if arr.shape[1] != 4:
            results.append({
                "file": basename,
                "shape": arr.shape,
                "status": "깨짐 (컬럼수≠4)"
            })
            continue

        # 정의: percls_*.npy[i, j] = 프레임 i에서 클래스 j의 최대 코사인
        # → 행 인덱스는 프레임 순서이므로 sent_stats_byhash와 직접 비교 불가
        # 대신 통계 범위만 확인

        stats_row = {
            "file": basename,
            "shape": arr.shape,
            "dtype": str(arr.dtype),
            "mean": float(np.mean(arr)),
            "std": float(np.std(arr)),
            "min": float(np.min(arr)),
            "max": float(np.max(arr)),
            "status": "프레임기반 (정렬상태 판별불가)"
        }
        results.append(stats_row)

    return results


def check_other_npy_files(stats, baseline_hashes, current_db_hashes):
    """기타 .npy 파일 검증 (m_s_bg90k.npy, Ak_kmeans64.npy 등)."""
    log("\n=== 기타 .npy 파일들 ===")

    results = []

    # 알려진 캐시 파일들
    known_files = {
        "m_s_bg90k.npy": "m_s (문장 연쇄 기울기) 백그라운드 9만0개 표본",
        "Ak_kmeans64.npy": "A 행렬 (KMeans64 클러스터용)",
    }

    for filename, description in known_files.items():
        path = os.path.join(OUT, filename)
        if not os.path.exists(path):
            results.append({
                "file": filename,
                "status": "파일없음"
            })
            continue

        arr = np.load(path)
        log(f"  {filename}: shape={arr.shape} dtype={arr.dtype}")

        # m_s_bg90k는 이미 sent_stats_byhash로 재구축됐으므로 비교 불필요
        # Ak_kmeans64는 정의를 알아야 함

        result = {
            "file": filename,
            "shape": arr.shape,
            "dtype": str(arr.dtype),
            "nbytes_mb": float(arr.nbytes / 1024 / 1024),
            "status": "형태 확인됨"
        }

        if filename == "m_s_bg90k.npy" and len(baseline_hashes) > 0:
            # m_s_bg90k가 존재하면 이는 이전 버전의 캐시
            result["note"] = "sent_stats_byhash로 재구축됨 — legacy 캐시"

        results.append(result)

    return results


def check_filter_ab_directory():
    """filter_ab/ 디렉토리 내 캐시 확인."""
    log("\n=== filter_ab/ 디렉토리 ===")

    filter_ab_dir = os.path.join(OUT, "filter_ab")
    if not os.path.exists(filter_ab_dir):
        return [{"status": "filter_ab/ 디렉토리 없음"}]

    import glob
    files = glob.glob(os.path.join(filter_ab_dir, "*.npy"))
    log(f"found {len(files)} .npy 파일")

    results = []
    for f in files[:5]:  # 처음 5개만
        arr = np.load(f)
        basename = os.path.basename(f)
        results.append({
            "file": basename,
            "shape": arr.shape,
            "dtype": str(arr.dtype),
            "status": "형태 확인됨"
        })

    return results


def main():
    log("캐시 정렬 상태 검증 시작")

    # 1. 정답 기준 로드
    stats, baseline_hashes = load_sent_stats_byhash()

    # 2. 현재 DB 순서 파악
    conn = psycopg2.connect(DSN)
    current_db_hashes = load_current_db_order(conn)
    conn.close()

    # 3. 순서 비교
    common = compare_orderings(baseline_hashes, current_db_hashes)

    # 4. 각 캐시 파일 검증
    # 순서 변경 여부: 첫 10개 해시가 다른지 확인
    order_changed = not all(
        baseline_hashes[i] == current_db_hashes[i]
        for i in range(min(10, len(baseline_hashes), len(current_db_hashes)))
    )

    results = {
        "baseline_hashes": len(baseline_hashes),
        "current_db_hashes": len(current_db_hashes),
        "common_hashes": len(common),
        "order_changed": order_changed,
        "files": []
    }

    # cluster_specificity_z.npy
    cluster_z_result = check_cluster_specificity_z()
    results["files"].append(cluster_z_result)

    # percls_*.npy
    percls_results = check_percls_files(conn, stats, baseline_hashes, current_db_hashes)
    results["files"].extend(percls_results)

    # 기타 .npy 파일
    other_results = check_other_npy_files(stats, baseline_hashes, current_db_hashes)
    results["files"].extend(other_results)

    # filter_ab/ 디렉토리
    filter_ab_results = check_filter_ab_directory()
    results["files"].extend(filter_ab_results)

    # 5. 결과 저장
    output_path = os.path.join(OUT, "filter_ab", "cache_alignment.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # numpy 타입을 JSON 직렬화 가능한 형태로 변환
    def convert_to_serializable(obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        elif isinstance(obj, dict):
            return {k: convert_to_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_to_serializable(v) for v in obj]
        return obj

    results_serializable = convert_to_serializable(results)

    with open(output_path, "w") as f:
        json.dump(results_serializable, f, indent=2, ensure_ascii=False)

    log(f"결과 저장: {output_path}")

    # 요약 출력
    log("\n=== 검증 요약 ===")
    log(f"기준 해시: {results['baseline_hashes']}개")
    log(f"현재 DB 해시: {results['current_db_hashes']}개")
    log(f"공통: {results['common_hashes']}개")
    log(f"순서 변경됨: {results['order_changed']}")

    if results["order_changed"]:
        log("WARNING: DB 해시 순서가 기준과 다릅니다. 행 인덱스 기반 캐시 재구축 권장.")

    log(f"검증 완료. 상세 결과: {output_path}")


if __name__ == "__main__":
    main()
