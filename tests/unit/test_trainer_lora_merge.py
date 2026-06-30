"""trainer_lib LoRA->merge export + trainable-param assert, on a tiny dummy nn.Module.

Verifies: (1) assert_trainable_params raises when an adapter targeted nothing,
(2) merge_lora_to_full returns a base-shaped state_dict (no lora_/base_model
key prefixes) that load_state_dict can consume into the original module.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

torch = pytest.importorskip("torch")
peft = pytest.importorskip("peft")

_TRAINER = Path(__file__).resolve().parents[2] / "docker" / "trainer" / "trainer_lib.py"
_spec = importlib.util.spec_from_file_location("trainer_lib", _TRAINER)
trainer_lib = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(trainer_lib)


class _Tiny(torch.nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.proj = torch.nn.Linear(8, 8)
        self.head = torch.nn.Linear(8, 2)

    def forward(self, x):
        return self.head(self.proj(x))


def _lora_wrap(target_modules):
    cfg = peft.LoraConfig(r=4, lora_alpha=8, target_modules=target_modules)
    return peft.get_peft_model(_Tiny(), cfg)


def test_trainable_params_positive_when_target_matches() -> None:
    m = _lora_wrap(["proj"])
    n = trainer_lib.assert_trainable_params(m)
    assert n > 0


def test_trainable_params_raises_when_zero_trainable() -> None:
    # all params frozen -> assert_trainable_params must fail loudly (the guard's own job).
    # (peft >=0.14 itself rejects a no-match target_modules at get_peft_model time, so we
    #  test our guard directly on a 0-trainable module rather than via a bad adapter.)
    m = _Tiny()
    for p in m.parameters():
        p.requires_grad_(False)
    with pytest.raises(ValueError, match="trainable"):
        trainer_lib.assert_trainable_params(m)


def test_merge_produces_base_shaped_state_dict() -> None:
    m = _lora_wrap(["proj"])
    merged = trainer_lib.merge_lora_to_full(m)
    keys = list(merged.keys())
    assert any(k.endswith("proj.weight") for k in keys)
    assert all("lora_" not in k for k in keys)
    assert all("base_model" not in k for k in keys)
    # the merged dict loads into a fresh stock module (serving-shaped)
    fresh = _Tiny()
    fresh.load_state_dict(merged)
