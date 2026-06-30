"""docker/analysis/fiftyone_pgvector.py 는 serving 코드라 vlm_pipeline.lib 를 import 못 함 →
DEFAULT_MODEL 상수 + _active_model_name() 폴백이 lib 의 STOCK_PE_CORE_MODEL_NAME 과
같은 값을 쓰는지 pin (drift 가드).
"""

from __future__ import annotations

import pathlib
import sys

from vlm_pipeline.lib.embedding_model_name import STOCK_PE_CORE_MODEL_NAME


def _load_fp(monkeypatch):
    d = str(pathlib.Path("docker/analysis").resolve())
    if d not in sys.path:
        sys.path.insert(0, d)
    import fiftyone_pgvector as fp

    return fp


def test_default_model_constant_matches_lib(monkeypatch) -> None:
    fp = _load_fp(monkeypatch)
    assert fp.DEFAULT_MODEL == STOCK_PE_CORE_MODEL_NAME


def test_active_model_name_falls_back_to_constant_on_db_error(monkeypatch) -> None:
    fp = _load_fp(monkeypatch)

    def _boom():
        raise RuntimeError("no pg")

    monkeypatch.setattr(fp, "_pg_conn", _boom)
    # _active_model_name must swallow errors and return the stock constant (search must never
    # break just because the pointer table is missing/unreachable).
    assert fp._active_model_name() == fp.DEFAULT_MODEL
