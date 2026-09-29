"""자기학습 금지 게이트(LABEL_SOURCES) 정적 검증 — DB 없이 모듈 임포트 + SQL 문자열만 본다.

깨지면 실패해야 하는 것 (실측 사고: sourcei label_source='model' 4,162행이 al_score_round.py 의
learner_value_probe.py SELECT 에는 있었지만 WHERE 엔 없어 GT 로 조용히 섞였다):
  1. LABEL_SOURCES 기본값이 'model'/'unknown' 을 배제한다 (human,derived 만)
  2. al_select.py — GT 쿼리(Q_GT)에는 label_source 게이트가 있고, 풀 쿼리(Q_POOL)에는 없다
  3. al_select.py 의 풀 쿼리(Q_POOL)에 eval_holdout 제외가 있다
  4. al_simulation.py / al_score_round.py / learner_value_probe.py — 유일한(=GT) 쿼리에
     label_source 게이트가 있다

실행: python3 /workspace/test_al_label_source_gate.py
"""
import al_select
import al_simulation
import al_score_round
import learner_value_probe


def test_default_label_sources_exclude_model_and_unknown():
    for mod in (al_select, al_simulation, al_score_round, learner_value_probe):
        assert mod.LABEL_SOURCES == ["human", "derived"], (mod.__name__, mod.LABEL_SOURCES)
        assert "model" not in mod.LABEL_SOURCES and "unknown" not in mod.LABEL_SOURCES


def test_al_select_gt_gated_pool_not():
    assert "label_source" in al_select.Q_GT, "GT 쿼리에 게이트가 없다"
    assert "label_source" not in al_select.Q_POOL, "풀 쿼리에 게이트가 걸리면 unlabeled 풀이 0건이 된다"


def test_al_select_pool_excludes_eval_holdout():
    assert "eval_holdout" in al_select.Q_POOL, "풀 쿼리가 봉인된 평가 홀드아웃을 선택할 수 있다"
    assert "eval_holdout" not in al_select.Q_GT, "GT 쿼리는 eval_holdout 컬럼과 무관해야 한다"


def test_gt_only_scripts_gated():
    assert "label_source" in al_simulation.SQL
    assert "label_source" in al_score_round.Q
    assert "label_source" in learner_value_probe.SQL


if __name__ == "__main__":
    test_default_label_sources_exclude_model_and_unknown()
    test_al_select_gt_gated_pool_not()
    test_al_select_pool_excludes_eval_holdout()
    test_gt_only_scripts_gated()
    print("OK — all label_source gate checks passed")
