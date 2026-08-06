"""repair_unsanitized_raw_keys.sanitized_key — 비정규 MinIO 객체 키 → 정본 raw_key.

기대값은 prod 실데이터에서 확인한 쌍이다 (2026-07-29 dry-run: 871 객체 중
크기 불일치 0 / DB 행 없음 0 으로 전량 매칭). 이 매핑이 어긋나면 복구 스크립트가
엉뚱한 키로 복사하거나 아무것도 못 찾는다.

lib.sanitizer 만 의존 — dagster import 없음.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "src"))

_SPEC = importlib.util.spec_from_file_location(
    "repair_unsanitized_raw_keys",
    str((pathlib.Path(__file__).resolve().parents[2] / "scripts" / "repair_unsanitized_raw_keys.py")),
)
repair = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(repair)


def test_maps_real_prod_objects_to_their_db_raw_key():
    assert (
        repair.sanitized_key("source-h/fire/폐기물보관장_화재_20260320_025730.mp4")
        == "source-h/fire/pyegimulbogwanjang_hwajae_20260320_025730.mp4"
    )
    assert (
        repair.sanitized_key("source-h/helmet/20260401_153736_헬멧_폐기물보관장.mp4")
        == "source-h/helmet/20260401_153736_helmet_pyegimulbogwanjang.mp4"
    )


def test_directory_components_are_sanitized_as_paths_not_filenames():
    """디렉토리에 점이 있어도 확장자로 오인해 자르면 안 된다."""
    out = repair.sanitized_key("A.B/sub/파일.mp4")
    assert out.count("/") == 2
    assert out.endswith(".mp4")
    assert out.split("/")[0] == "a.b" or "." in out.split("/")[0]


def test_already_canonical_key_is_idempotent():
    key = "source-h/fire/pyegimulbogwanjang_hwajae_20260320_025730.mp4"
    assert repair.sanitized_key(key) == key


def test_empty_and_degenerate_inputs():
    assert repair.sanitized_key("") == ""
    assert repair.sanitized_key("///") == ""
    assert repair.sanitized_key("파일.mp4") == "pail.mp4" or repair.sanitized_key("파일.mp4").endswith(".mp4")
