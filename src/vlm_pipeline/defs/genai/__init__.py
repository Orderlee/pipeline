"""Dagster GenAI 도메인 모듈.

비동기 GenAI jobs (Kling, Higgsfield) 의 polling sensor + 상태 갱신 ops.
동기 엔진(Nanobanana, GPT Image) 은 docker/genai 컨테이너 안에서 submit 시점에 결과
처리하므로 Dagster polling 불필요.

Phase F (synthetic coverage 제어평면) 도 여기 산다 — planner/dispatch 둘 다 GenAI 의
internal endpoint 를 경유하지 자체 생성 경로를 갖지 않기 때문이다. 둘 다 **기본 꺼짐**:
`synthetic_coverage_planner_schedule` = STOPPED, `synthetic_campaign_dispatch_sensor` = STOPPED.
"""

from .campaign_dispatch import synthetic_campaign_dispatch_sensor
from .coverage_planner import (
    synthetic_coverage_plan,
    synthetic_coverage_planner_job,
    synthetic_coverage_planner_schedule,
)
from .sensor import genai_poll_sensor

__all__ = [
    "genai_poll_sensor",
    "synthetic_campaign_dispatch_sensor",
    "synthetic_coverage_plan",
    "synthetic_coverage_planner_job",
    "synthetic_coverage_planner_schedule",
]
