"""open_clip 백엔드 checkpoint-override 분기 테스트 (Section E serving load).

torch/open_clip 실모델 없이 monkeypatch 로 load_state_dict 분기만 검증.
env 미설정 = 현행 stock 동작 보존(회귀 가드), env 설정 = local state_dict 로드 + 버전 model_name.
"""

from __future__ import annotations

import pathlib
import sys

import pytest

_SVC_DIR = str(pathlib.Path("docker/embedding").resolve())


@pytest.fixture
def be_module(monkeypatch):
    if _SVC_DIR not in sys.path:
        sys.path.insert(0, _SVC_DIR)
    import backends.open_clip_be as be

    return be


class _FakeModel:
    def __init__(self):
        self.loaded_state = None
        self.loaded_strict = None

    def to(self, device):
        return self

    def eval(self):
        return self

    def load_state_dict(self, state, strict=True):
        self.loaded_state = state
        self.loaded_strict = strict


def _install_fakes(monkeypatch, be, *, captured_path):
    fake_model = _FakeModel()

    fake_open_clip = type(
        "oc",
        (),
        {
            "create_model_and_transforms": staticmethod(lambda ref: (fake_model, None, (lambda x: x))),
            "get_tokenizer": staticmethod(lambda ref: (lambda xs: xs)),
        },
    )

    def _fake_torch_load(path, map_location=None):
        captured_path.append(path)
        return {"w": "merged-state-dict"}

    fake_torch = type(
        "torch",
        (),
        {
            "load": staticmethod(_fake_torch_load),
            "cuda": type("c", (), {"is_available": staticmethod(lambda: False)})(),
        },
    )
    monkeypatch.setitem(sys.modules, "open_clip", fake_open_clip)
    monkeypatch.setitem(sys.modules, "torch", fake_torch)
    return fake_model


def test_stock_behaviour_when_env_unset(be_module, monkeypatch):
    monkeypatch.delenv("EMBEDDING_CHECKPOINT_PATH", raising=False)
    monkeypatch.delenv("EMBEDDING_MODEL_VERSION", raising=False)
    captured: list = []
    fake_model = _install_fakes(monkeypatch, be_module, captured_path=captured)

    backend = be_module.OpenClipBackend(device="cpu")
    backend.load()

    assert backend.is_loaded() is True
    assert backend.name == "facebook/PE-Core-L14-336"  # unchanged stock model_name
    assert fake_model.loaded_state is None  # no load_state_dict call
    assert captured == []  # torch.load never called


def test_loads_local_state_dict_when_env_set(be_module, monkeypatch, tmp_path):
    ckpt = tmp_path / "merged.pt"
    ckpt.write_bytes(b"x")
    monkeypatch.setenv("EMBEDDING_CHECKPOINT_PATH", str(ckpt))
    monkeypatch.setenv("EMBEDDING_MODEL_VERSION", "ft-2026.06.29-lora-001")
    captured: list = []
    fake_model = _install_fakes(monkeypatch, be_module, captured_path=captured)

    backend = be_module.OpenClipBackend(device="cpu")
    backend.load()

    assert backend.is_loaded() is True
    assert fake_model.loaded_state == {"w": "merged-state-dict"}
    assert fake_model.loaded_strict is True
    assert captured == [str(ckpt)]
    # §5.3 model_name 버전 인코딩
    assert backend.name == "facebook/PE-Core-L14-336@ft-2026.06.29-lora-001"


def test_missing_checkpoint_file_raises(be_module, monkeypatch):
    monkeypatch.setenv("EMBEDDING_CHECKPOINT_PATH", "/nonexistent/merged.pt")
    monkeypatch.delenv("EMBEDDING_MODEL_VERSION", raising=False)
    captured: list = []
    _install_fakes(monkeypatch, be_module, captured_path=captured)

    backend = be_module.OpenClipBackend(device="cpu")
    with pytest.raises(FileNotFoundError):
        backend.load()
