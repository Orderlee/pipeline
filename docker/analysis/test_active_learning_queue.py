"""active_learning_queue (pgvector pushdown) 오케스트레이션 로직 단위검증.

DB/FiftyOne 없이 — 헬퍼를 가짜로 갈아끼워 병합(max rare_sim)·다양성 floor·al_score 가중을 확인.
실행: python3 test_active_learning_queue.py
"""
import fiftyone_pgvector as fp


def _install_fakes(*, rare_topk, fo_scores, labels=None, total=1000):
    labels = labels or {"f1": {"label": "fire"}, "f2": {"label": "fire"}, "s1": {"label": "smoke"}}
    fp._load_fo_metadata_for_dq = lambda *a, **k: labels
    fp._load_frames_for_dq = lambda model_name=None, limit=None, image_ids=None: [
        {"image_id": i, "embedding": [0.1, 0.2]} for i in (image_ids or [])
    ]
    fp._compute_class_centroids = lambda ebc: {"fire": "FIRE", "smoke": "SMOKE"}
    fp._count_frames_for_dq = lambda *a, **k: total
    fp._rare_topk = rare_topk
    fp._fo_scores_for_ids = fo_scores


def test_merge_diversity_and_score():
    # fire: 6 near(>0.7) + 2 non-near.  smoke: n1 의 더 높은 rare_sim → 병합 max + class 전환 검증.
    def rare_topk(centroid, k, model_name, *, exclude_ids=None, ef_search=200):
        if centroid == "FIRE":
            near = [{"image_id": f"n{i}", "image_bucket": "b", "image_key": f"k{i}",
                     "rare_sim": s} for i, s in enumerate([0.90, 0.89, 0.88, 0.87, 0.86, 0.85])]
            non = [{"image_id": "x1", "image_bucket": "b", "image_key": "kx1", "rare_sim": 0.50},
                   {"image_id": "x2", "image_bucket": "b", "image_key": "kx2", "rare_sim": 0.45}]
            return near + non
        return [{"image_id": "n0", "image_bucket": "b", "image_key": "k0", "rare_sim": 0.95}]  # SMOKE > fire's n0

    _install_fakes(rare_topk=rare_topk, fo_scores=lambda ids: {})
    out = fp.active_learning_queue(n=5)
    rows = out["rows"]

    assert out["n_candidates"] == 997, out["n_candidates"]          # 1000 - 3 labeled
    assert out["truncated"] is False
    assert len(rows) == 5, len(rows)
    # 병합 max + class 전환: n0 은 smoke 0.95 가 fire 0.90 을 이김
    n0 = next(r for r in rows if r["image_id"] == "n0")
    assert n0["rare_sim"] == 0.95 and n0["nearest_rare_class"] == "smoke", n0
    # al_score = 0.45*rare_sim (scores 비어있음 → uniq/rep 0)
    assert n0["al_score"] == round(0.45 * 0.95, 4), n0["al_score"]
    # 다양성 floor: top-5 에 non-near(edge/unique) 최소 1개 강제 (floor 없으면 전부 near)
    rc = out["reason_counts"]
    assert rc.get("edge/unique", 0) >= 1, rc
    assert sum(v for k, v in rc.items() if k.startswith("near-")) == 4, rc  # near_limit=4*5//5
    # 내림차순 정렬
    scores = [r["al_score"] for r in rows]
    assert scores == sorted(scores, reverse=True), scores


def test_uniqueness_representativeness_reorders():
    # rep/uniq 가 채워지면(STEP 0 실행 후) al_score 가중이 랭킹을 바꾼다.
    def rare_topk(centroid, k, model_name, *, exclude_ids=None, ef_search=200):
        if centroid == "FIRE":
            return [{"image_id": "near1", "image_bucket": "b", "image_key": "k", "rare_sim": 0.80},
                    {"image_id": "rep1", "image_bucket": "b", "image_key": "k", "rare_sim": 0.50}]
        return []

    # rep1: rare 낮지만 representativeness 높음 → al_score 가 near1 을 역전, reason=representative
    scores = {"rep1": {"uniqueness": 0.2, "representativeness": 0.9}}
    _install_fakes(rare_topk=rare_topk, fo_scores=lambda ids: scores)
    out = fp.active_learning_queue(n=5)
    rows = {r["image_id"]: r for r in out["rows"]}

    exp_rep1 = round(0.45 * 0.50 + 0.30 * 0.9 + 0.25 * 0.2, 4)   # 0.545
    exp_near1 = round(0.45 * 0.80, 4)                             # 0.360
    assert rows["rep1"]["al_score"] == exp_rep1, rows["rep1"]
    assert rows["near1"]["al_score"] == exp_near1, rows["near1"]
    assert rows["rep1"]["al_score"] > rows["near1"]["al_score"]  # 역전
    assert rows["rep1"]["reason"] == "representative", rows["rep1"]  # rare<0.7, uniq<rep
    assert rows["near1"]["reason"] == "near-fire", rows["near1"]


def test_no_seed_returns_empty():
    # fire/smoke 라벨 0개 → centroid 없음 → 빈 큐 (검색으로 씨앗 먼저 만들라는 정상 상태)
    _install_fakes(rare_topk=lambda *a, **k: [], fo_scores=lambda ids: {},
                   labels={"p1": {"label": "none"}, "p2": {"label": "none"}})
    out = fp.active_learning_queue(n=5)
    assert out["rows"] == [] and out["n_candidates"] == 0, out


if __name__ == "__main__":
    test_merge_diversity_and_score()
    test_uniqueness_representativeness_reorders()
    test_no_seed_returns_empty()
    print("OK — all active_learning_queue logic checks passed")
