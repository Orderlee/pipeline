"""카메라 씬 분류 백필 job — Gemini 5축 + DAv2 camera_angle 독립 라벨러.

design: docs/design-docs/camera-angle-grouping-2026-07-29.md §3.2, §7.

ingest 당시 angle_method='deferred' 이거나 env_method='deferred' 로 남은 비디오에 대해
scene_backfill_helpers.process_one_video 를 재실행하여 video_metadata (camera_angle/
subject_scale/occlusion_state/environment_type/daynight_type/weather/env_method/
angle_method) 를 채운다. camera_angle 은 사람 GT 98편 실측(plan-vs-rest AUC 0.947, 오검출
0/94)으로 lib/video_angle_dav2.py(DAv2 서비스)가 전담하므로, 이 job 은 한 번의 Gemini
호출(나머지 5축)과 한 번의 DAv2 HTTP 호출(camera_angle)을 영상당 각각 독립적으로
수행한다 — 한쪽이 실패해도 다른 쪽 결과는 그대로 기록된다. env_backfill.py 구조를 그대로
복제.

env_backfill.py(Places365)는 그대로 남겨둔다 — 이 job 이 env_method='gemini-2.5-flash' 를
쓰면 그 행은 이후 env_backfill 의 env_method='deferred' 선택 조건에서 자동으로 빠지므로
두 백필이 같은 행을 두 번 처리하며 충돌하지 않는다.

Layer 3/4: @op + @job 정의.
"""

from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

from dagster import Field, job, op

from vlm_pipeline.defs.ingest.scene_backfill_helpers import (
    OUTCOME_FAILED,
    OUTCOME_SKIP,
    process_one_video,
)
from vlm_pipeline.lib.env_utils import int_env
from vlm_pipeline.resources.postgres import PostgresResource

_ARCHIVE_MOUNT_PROBE = "/nas/data/archive"

# 실측(2026-07-29, dagster 컨테이너에서 실제 Vertex/DAv2 호출): Vertex gemini-2.5-flash
# 4동시 1.67s/건 → 8동시 0.67s/건(최적) → 16동시 1.10s/건(악화, 429 는 0건). DAv2 서비스도
# 순차 0.156s/건 → 8동시 0.072s/건. 두 지표 모두 8에서 최적이라 기본값으로 채택.
_DEFAULT_CONCURRENCY = 8
# 상한 16: 위 실측에서 16동시부터 Vertex 처리량이 악화되고, PG ThreadedConnectionPool 도
# pool_max=10(resources/postgres_base.py) 이라 16동시 DB write 는 이론상 PoolError 를 유발할
# 수 있다 — 다만 PoolError("connection pool exhausted")는 이미 재시도 대상(_is_transient_pg_error)
# 이라 상한을 16까지만 두고 그 초과분 흡수는 기존 재시도 메커니즘에 맡긴다.
_MAX_CONCURRENCY = 16


@dataclass(frozen=True)
class _SceneWorkerResult:
    """스레드 워커 반환값 — 카운터 증가/로그 출력은 메인 스레드가 전담한다."""

    asset_id: str
    outcome: str
    scene_result: dict[str, Any] | None
    err_msg: str | None
    db_attempted: bool
    db_ok: bool
    db_err_msg: str | None


def _process_and_write_one_video(video: dict[str, Any], db: PostgresResource) -> _SceneWorkerResult:
    """process_one_video 실행 + (기록할 결과가 있으면) DB write 까지 한 워커 단위로 수행.

    per-file fail-forward 스레드 경계 보증: 이 함수는 절대 예외를 밖으로 던지지 않는다 —
    future.result() 에서 예외가 터지면 나머지 결과 집계를 방해하므로, process_one_video
    계약 위반이든 예상 밖 버그든 전부 _SceneWorkerResult 로 흡수해 반환한다.
    DB write(PostgresResource.connect())는 커넥션 풀 기반이라 여러 스레드에서 동시에 호출해도
    안전하다 — 각 호출이 풀에서 별도 커넥션을 빌려 쓰고 반납한다.
    """
    asset_id = "unknown"
    try:
        asset_id = video.get("asset_id")
        outcome, scene_result, err_msg = process_one_video(video)

        if outcome == OUTCOME_FAILED or scene_result is None:
            return _SceneWorkerResult(asset_id, outcome, scene_result, err_msg, False, False, None)

        try:
            db.update_video_scene(asset_id, **scene_result)
        except Exception as db_exc:
            return _SceneWorkerResult(asset_id, outcome, scene_result, err_msg, True, False, str(db_exc))
        return _SceneWorkerResult(asset_id, outcome, scene_result, err_msg, True, True, None)
    except Exception as exc:
        return _SceneWorkerResult(asset_id, OUTCOME_FAILED, None, f"unexpected worker error: {exc}", False, False, None)


@op(
    name="video_scene_backfill",
    description="카메라 씬 백필(Gemini 5축 + DAv2 camera_angle 독립 호출) — "
    "angle_method/env_method='deferred' 비디오를 배치 처리",
    config_schema={
        "limit": Field(int, default_value=1000, description="처리할 최대 비디오 수"),
        "concurrency": Field(
            int,
            default_value=int_env("SCENE_BACKFILL_CONCURRENCY", _DEFAULT_CONCURRENCY, minimum=1),
            description="영상 병렬 처리 스레드 수. 1이면 스레드풀 없이 기존 순차 경로(디버깅/롤백 안전판). "
            f"실측 기반 기본값 {_DEFAULT_CONCURRENCY}, 상한 {_MAX_CONCURRENCY}(초과분은 자동 clamp).",
        ),
    },
)
def video_scene_backfill(context, db: PostgresResource) -> dict:
    # env_backfill FIX 3 과 동일 근거(archive mount preflight) — NAS 마운트 장애 중
    # archive_path 미확인을 'deferred_missing_archive' 터미널 마커로 대량 오분류하는 것을 방지.
    # 스레드풀 생성보다 항상 먼저 확인한다(위치 고정).
    if not os.path.isdir(_ARCHIVE_MOUNT_PROBE):
        raise RuntimeError(
            f"archive mount not available ({_ARCHIVE_MOUNT_PROBE}): "
            "aborting scene_backfill to avoid mis-marking rows as deferred_missing_archive"
        )

    limit = int(context.op_config.get("limit", 1000))
    concurrency = max(1, min(int(context.op_config.get("concurrency", _DEFAULT_CONCURRENCY)), _MAX_CONCURRENCY))
    candidates = db.find_deferred_scene_videos(limit=limit)

    total = len(candidates)
    context.log.info(f"scene_backfill 시작: 대상 {total}건 (limit={limit}, concurrency={concurrency})")

    done = 0
    partial = 0
    failed = 0
    skipped = 0
    skipped_terminal = 0

    start = time.monotonic()

    if concurrency == 1:
        # 기존 순차 경로 — 그대로 유지(디버깅/롤백 안전판). 스레드풀 생성 없음.
        for video in candidates:
            asset_id = video["asset_id"]

            outcome, scene_result, err_msg = process_one_video(video)

            if outcome == OUTCOME_SKIP:
                context.log.warning(f"skip: asset_id={asset_id} — {err_msg}")
                skipped += 1
                # 터미널 마커 기록 — 안 쓰면 이 행이 'deferred' selection bucket 에 남아 매 tick 재시도된다.
                if scene_result is not None:
                    try:
                        db.update_video_scene(asset_id, **scene_result)
                        skipped_terminal += 1
                    except Exception as exc:
                        context.log.error(f"terminal marker update 실패: asset_id={asset_id} — {exc}")
                continue

            if outcome == OUTCOME_FAILED:
                context.log.error(f"classify 실패: asset_id={asset_id} — {err_msg}")
                failed += 1
                continue

            assert scene_result is not None
            # err_msg 가 있는 DONE 은 두 라벨러 중 하나만 성공한 부분 성공이다(실패 독립성) —
            # 성공한 쪽은 기록하되, 실패한 쪽의 사유를 조용히 삼키지 않고 남긴다.
            if err_msg:
                partial += 1
                context.log.warning(f"partial: asset_id={asset_id} — {err_msg}")
            try:
                db.update_video_scene(asset_id, **scene_result)
                done += 1
            except Exception as exc:
                context.log.error(f"update 실패: asset_id={asset_id} — {exc}")
                failed += 1
    else:
        # 스레드풀 병렬 처리 — 판정 로직/DB write 는 _process_and_write_one_video 안에서
        # 순차 경로와 동일하게 수행되고, 카운터 증가와 로그 출력만 여기 메인 스레드로 옮겨졌다
        # (worker 안에서 context.log 호출 시 로그 순서가 뒤섞여 장애 분석이 어려워지므로).
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(_process_and_write_one_video, video, db) for video in candidates]
            for future in as_completed(futures):
                # _process_and_write_one_video 는 절대 예외를 던지지 않으므로 result() 는 안전하다.
                result = future.result()
                asset_id = result.asset_id

                if result.outcome == OUTCOME_SKIP:
                    context.log.warning(f"skip: asset_id={asset_id} — {result.err_msg}")
                    skipped += 1
                    if result.db_attempted:
                        if result.db_ok:
                            skipped_terminal += 1
                        else:
                            context.log.error(f"terminal marker update 실패: asset_id={asset_id} — {result.db_err_msg}")
                    continue

                if result.outcome == OUTCOME_FAILED:
                    context.log.error(f"classify 실패: asset_id={asset_id} — {result.err_msg}")
                    failed += 1
                    continue

                if result.err_msg:
                    partial += 1
                    context.log.warning(f"partial: asset_id={asset_id} — {result.err_msg}")
                if result.db_ok:
                    done += 1
                else:
                    context.log.error(f"update 실패: asset_id={asset_id} — {result.db_err_msg}")
                    failed += 1

    elapsed = time.monotonic() - start
    sec_per_video = (elapsed / total) if total else 0.0

    # Gemini 429/5xx(is_vertex_rate_limit_error/is_vertex_server_error, lib/gemini.py)는
    # 이미 그 호출 wrapper 안에서 자체 재시도 예산을 소진한 뒤에만 예외로 올라온다.
    # process_one_video 는 그 예외를 잡아 outcome=FAILED(또는 partial DONE)로 흡수하고,
    # FAILED 행은 DB write 자체가 없어 angle_method/env_method='deferred' 그대로 남는다 —
    # 다음 스케줄 tick 이 자동 재시도하므로 이 op 레벨에 별도 재시도 루프를 두지 않는다.
    context.log.info(
        f"scene_backfill 완료: done={done} (partial={partial}) failed={failed} "
        f"skipped={skipped} (terminal={skipped_terminal}) / total={total} "
        f"concurrency={concurrency} elapsed={elapsed:.1f}s sec/video={sec_per_video:.3f}"
    )
    return {
        "done": done,
        "partial": partial,
        "failed": failed,
        "skipped": skipped,
        "skipped_terminal": skipped_terminal,
        "total": total,
        "concurrency": concurrency,
        "elapsed_sec": round(elapsed, 3),
        "sec_per_video": round(sec_per_video, 3),
    }


@job(
    name="video_scene_backfill_job",
    description="[백필] deferred 비디오에 카메라 씬 분류(Gemini 5축 + DAv2 camera_angle) 재실행 "
    "(평일 20:00 KST 스케줄)",
)
def video_scene_backfill_job():
    video_scene_backfill()
