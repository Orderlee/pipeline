"""LS 태스크 게이트 판정 — 되돌리기 어려운 실수 두 가지를 막는 테스트.

1. 게이트가 기본 on 이 되면 아무도 모르게 라벨러 물량이 반토막 난다 → 기본 off 를 못 박는다.
2. 무작위 우회 표본이 사라지면 게이트의 오탈락률을 영원히 추정할 수 없고, 그 시점 이후의
   모든 사람 라벨이 selection-biased 가 된다 → 표본이 실제로 뽑히는지, 그리고 asset 마다
   비례해서 뽑히는지(한 영상에 몰리지 않는지) 확인한다.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from gemini.ls_task_gate import GateConfig, GateStats, decide  # noqa: E402


def test_disabled_by_default_sends_everything():
    """기본 off — 켜는 것은 명시적 결정이어야 한다."""
    cfg = GateConfig()
    assert cfg.enabled is False
    assert decide(0, "any/key.json", cfg).send is True
    assert decide(3, "any/key.json", cfg).send is True


def test_enabled_passes_results_and_cuts_empty():
    cfg = GateConfig(enabled=True, bypass_ratio=0.0)
    assert decide(1, "a/b.json", cfg).send is True
    assert decide(9, "a/b.json", cfg).reason == "has_result"
    d = decide(0, "a/b.json", cfg)
    assert d.send is False and d.reason == "empty_result"


def test_bypass_sample_is_drawn_at_about_the_configured_ratio():
    cfg = GateConfig(enabled=True, bypass_ratio=0.02)
    keys = [f"unit/asset{i // 50:03d}/frame{i:05d}.json" for i in range(20_000)]
    passed = [k for k in keys if decide(0, k, cfg).send]
    ratio = len(passed) / len(keys)
    assert 0.015 < ratio < 0.025, f"우회 비율 {ratio:.4f} 가 0.02 에서 너무 벗어남"


def test_bypass_is_proportional_across_assets_not_clustered():
    """asset 단위 층화 — 표본이 한 영상에 몰리면 유효표본이 줄어 추정이 망가진다."""
    cfg = GateConfig(enabled=True, bypass_ratio=0.10)
    assets = [f"unit/asset{a:03d}" for a in range(40)]
    hit_assets = 0
    for a in assets:
        keys = [f"{a}/frame{i:05d}.json" for i in range(200)]
        if any(decide(0, k, cfg).send for k in keys):
            hit_assets += 1
    # 10% × 200프레임이면 asset 당 기대 20건 — 사실상 모든 asset 이 표본을 내야 한다
    assert hit_assets >= 38, f"표본이 {hit_assets}/40 asset 에만 걸림 — 군집 표집 의심"


def test_bypass_is_deterministic_across_runs():
    """재실행·재개가 같은 판정을 내야 한다 (시드 상태 없음)."""
    cfg = GateConfig(enabled=True, bypass_ratio=0.05)
    keys = [f"k/{i}.json" for i in range(500)]
    first = [decide(0, k, cfg).send for k in keys]
    second = [decide(0, k, cfg).send for k in keys]
    assert first == second


def test_ratio_bounds():
    keys = [f"k/{i}.json" for i in range(200)]
    none_cfg = GateConfig(enabled=True, bypass_ratio=0.0)
    all_cfg = GateConfig(enabled=True, bypass_ratio=1.0)
    assert not any(decide(0, k, none_cfg).send for k in keys)
    assert all(decide(0, k, all_cfg).send for k in keys)


def test_from_env_reads_flags(monkeypatch):
    monkeypatch.setenv("LS_TASK_GATE_ENABLED", "true")
    monkeypatch.setenv("LS_TASK_GATE_BYPASS_RATIO", "0.05")
    cfg = GateConfig.from_env()
    assert cfg.enabled is True and cfg.bypass_ratio == 0.05


def test_from_env_bad_ratio_falls_back_without_crashing(monkeypatch):
    monkeypatch.setenv("LS_TASK_GATE_ENABLED", "1")
    monkeypatch.setenv("LS_TASK_GATE_BYPASS_RATIO", "not-a-number")
    assert GateConfig.from_env().bypass_ratio == 0.02


def test_stats_counts_three_outcomes():
    cfg = GateConfig(enabled=True, bypass_ratio=1.0)
    stats = GateStats()
    stats.record(decide(2, "a.json", cfg))  # has_result
    stats.record(decide(0, "b.json", cfg))  # bypass_sample
    stats.record(decide(0, "c.json", GateConfig(enabled=True, bypass_ratio=0.0)))  # gated
    assert (stats.sent_with_result, stats.sent_bypass, stats.gated_out) == (1, 1, 1)
    assert "제외 1" in stats.summary()


def test_synthetic_batches_are_not_cut_by_default():
    """합성본은 자동 검출 0건이어도 사람에게 간다.

    게이트는 result_count==0 일 때만 개입한다. 0건인 합성본이란 곧 "SAM3 가 못 본
    이벤트" 이고, 그게 ComfyUI 를 돌린 이유 자체다. 이 기본값이 뒤집히면 SAM3 가 이미 잘
    잡는 것만 검수자에게 가고 합성으로 메우려던 결손은 영원히 안 메워진다.

    (최초 근거였던 "SAM3 가 합성 연기에서 0건" 은 2026-09-21 에 반증됐다 — 그건 배관
    문제였고 실제로는 score 0.6953 으로 잡았다. 유지 근거는 위 구조적 논거와, 사람 GT
    코호트에서 이벤트 클래스가 normal 보다 3~6배 자주 빈손이라는 실측이다.)
    """
    cfg = GateConfig(enabled=True, bypass_ratio=0.0)
    assert cfg.synthetic_bypass_ratio == 1.0
    # 실사: 0건이면 잘린다.
    assert decide(0, "real/frame.json", cfg, is_synthetic=False).send is False
    # 합성: 같은 0건이어도 통과한다.
    d = decide(0, "genai_batch/frame.json", cfg, is_synthetic=True)
    assert d.send is True and d.reason == "synthetic_bypass"


def test_synthetic_pass_does_not_pollute_the_random_bypass_sample():
    """합성 통과(확률 1)를 무작위 표본과 같이 세면 오탈락률 추정이 깨진다.

    이 모듈 docstring 이 BYPASS 를 "게이트의 오탈락률을 추정하는 유일한 수단"이라고
    못 박고 있다. 확률 1로 통과한 건을 그 표본에 섞으면 합성 배치의 게이트 손실률을
    영원히 0% 로 읽게 된다.
    """
    cfg = GateConfig(enabled=True, bypass_ratio=0.0)
    stats = GateStats()
    stats.record(decide(0, "genai/a.json", cfg, is_synthetic=True))
    stats.record(decide(0, "real/b.json", cfg, is_synthetic=False))
    stats.record(decide(5, "real/c.json", cfg, is_synthetic=False))
    assert stats.sent_synthetic == 1
    assert stats.sent_bypass == 0, "합성 통과가 무작위 표본에 섞이면 안 된다"
    assert stats.sent_with_result == 1
    assert stats.gated_out == 1
    assert "합성통과 1" in stats.summary()


def test_synthetic_ratio_is_tunable_and_env_backed(monkeypatch):
    """검수 부하가 문제가 되면 비율로 조인다 — 코드 수정 없이."""
    monkeypatch.setenv("LS_TASK_GATE_ENABLED", "true")
    monkeypatch.setenv("LS_TASK_GATE_SYNTHETIC_BYPASS_RATIO", "0.0")
    cfg = GateConfig.from_env()
    assert cfg.synthetic_bypass_ratio == 0.0
    assert decide(0, "genai_batch/frame.json", cfg, is_synthetic=True).send is False
    # 결과가 있으면 비율과 무관하게 항상 통과.
    assert decide(2, "genai_batch/frame.json", cfg, is_synthetic=True).send is True


def test_ratio_for_picks_the_right_knob():
    cfg = GateConfig(enabled=True, bypass_ratio=0.02, synthetic_bypass_ratio=0.5)
    assert cfg.ratio_for(False) == 0.02
    assert cfg.ratio_for(True) == 0.5
