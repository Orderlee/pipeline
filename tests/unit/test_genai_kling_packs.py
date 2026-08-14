"""docker/genai/adapters/kling.py 리소스팩 표시 로직 회귀 테스트.

genai 컨테이너 코드는 설치되는 패키지가 아니므로 sys.path 에 docker/genai 를 얹어 import
한다 (test_genai_pg_recompute.py 와 동일 패턴 — 거기선 단일 파일이라 importlib 로 직접
로드했지만, kling.py 는 `from .base import ...` 상대 import 가 있어 패키지로 올려야 한다).

지키려는 불변식은 하나다: **요금제가 전량 만료돼도 표가 비지 않는다.**
online 만 남기는 필터가 있었을 때 2026-08-14 에 실제로 표가 통째로 사라졌고,
남은 문구가 "리소스팩 없음 (또는 KLING 키 미설정)" 이라 키가 날아간 것으로 오진됐다.
"""

import sys
from pathlib import Path

import pytest

_GENAI_ROOT = Path(__file__).resolve().parents[2] / "docker" / "genai"
if str(_GENAI_ROOT) not in sys.path:
    sys.path.insert(0, str(_GENAI_ROOT))

kling = pytest.importorskip("adapters.kling")


def _pack(name, status, total, remaining, effective, invalid):
    return {
        "resource_pack_name": name,
        "status": status,
        "total_quantity": total,
        "remaining_quantity": remaining,
        "effective_time": effective,
        "invalid_time": invalid,
    }


DAY = 86_400_000  # ms

# 2026-08-14 계정 실제 형태: online 0개, runOut 3 + expired 1.
ALL_EXPIRED = [
    _pack("Trial-Video-100Units-5Con-1Months", "runOut", 100, 0, 100 * DAY, 130 * DAY),
    _pack("Trial-Video-1000Units-5Con-1Months", "runOut", 1000, 0, 127 * DAY, 157 * DAY),
    _pack("Trial-Video-1000Units-5Con-1Months", "expired", 1000, 2, 150 * DAY, 180 * DAY),
]


def test_all_packs_expired_still_renders_rows():
    """핵심 회귀: online 이 0개여도 표에 넣을 행이 남아야 한다."""
    ordered = kling.order_packs_for_display(ALL_EXPIRED)
    assert len(ordered) == len(ALL_EXPIRED), "만료 팩을 걸러내면 표가 비어 오진을 부른다"

    panel = kling.summarize_resource_packs(ordered, now_ts=200 * DAY / 1000)
    assert panel["packs"], "패널이 비면 템플릿이 '리소스팩 없음' 로 떨어진다"
    # 요금제 이름이 실제로 노출 가능한 상태인지 (표 첫 컬럼의 소스)
    assert all(p["resource_pack_name"] for p in panel["packs"])
    # 만료 팩은 actionable 하지 않으므로 알림은 없어야 한다
    assert panel["alerts"] == []


def test_online_pack_sorts_first_then_most_recent():
    live = _pack("Trial-Video-1000Units-5Con-1Months", "online", 1000, 900, 120 * DAY, 210 * DAY)
    ordered = kling.order_packs_for_display([*ALL_EXPIRED, live])

    assert ordered[0]["status"] == "online", "활성 요금제가 맨 위여야 한다"
    # 나머지는 최근 결제(effective_time) 내림차순
    rest = [p["effective_time"] for p in ordered[1:]]
    assert rest == sorted(rest, reverse=True)


def test_totals_count_every_pack_regardless_of_status():
    """누적 합계는 표시 필터와 무관하게 전 팩 기준 — 표시 로직 변경에 흔들리면 안 된다."""
    totals = kling.resource_pack_totals(ALL_EXPIRED)
    assert totals["n_packs"] == 3
    assert totals["purchased"] == 2100
    assert totals["remaining"] == 2
    assert totals["used"] == 2098
