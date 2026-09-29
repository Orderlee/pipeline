"""LS task 자동 생성 sensor + presigned URL 갱신 schedule.

dispatch_stage_job이 완료된 후 (dispatch_requests.status='completed'),
아직 LS task가 생성되지 않은 요청을 감지하여 ls_task_create_job을 트리거합니다.

처리 흐름:
  dispatch_requests
      WHERE status='completed'
        AND COALESCE(ls_task_status, 'pending') = 'pending'
    → labeling_method 읽어서 video/image mode 분기:
        timestamp_video/timestamp/video  → ls_tasks.py create --mode video --categories ...
        bbox/segmentation/image          → ls_tasks.py create --mode image --categories ...
      둘 다 있으면 두 번 호출. categories 가 있으면 project title = `<folder>_<category>`.
    → ls_task_status = 'created' 업데이트

presigned URL 갱신:
  매일 05:00 KST — ls_tasks.py renew --all-projects 실행
  만료 1일 이내 URL을 자동 갱신하여 라벨링 작업 중단 방지
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    job,
    op,
    sensor,
)
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

from vlm_pipeline.lib.detection_common import parse_tag_list
from vlm_pipeline.lib.env_utils import (
    default_postgres_dsn,
    int_env,
)
from vlm_pipeline.lib.sensor_db import open_sensor_read_connection
from vlm_pipeline.lib.minio_cross_sync import (
    is_cross_sync_needed,
    ls_minio_endpoint,
    sync_folder_for_ls,
)
from vlm_pipeline.lib.sanitizer import sanitize_path_component

# labeling_method 값 → ls_tasks.py create --mode 매핑
_VIDEO_METHODS = {"timestamp_video", "timestamp", "video"}
# captioning_image 는 2026-09-21 까지 빠져 있었다. 그때까지는 env_utils._OUTPUT_DEPENDENCIES 가
# captioning_image 에 timestamp_video 를 항상 끌고 붙여서 video 경로로 새던 덕에 드러나지
# 않았는데, 이미지 배치에서 그 의존성 확장을 끊고 나면 captioning_image 단독 dispatch 가
# video/image 어느 프로젝트도 못 만들고 조용히 사라진다.
_IMAGE_METHODS = {"bbox", "segmentation", "image", "captioning_image"}

LS_TASKS_SCRIPT = Path(os.environ.get("LS_TASKS_SCRIPT", Path(__file__).parents[3] / "gemini" / "ls_tasks.py"))


# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------


def _fetch_pending_dispatch_requests() -> list[dict]:
    """ls_task_status='pending'이고 dispatch가 완료된 요청 목록.

    labeling_method / categories / requested_at (fallback: completed_at, created_at) 도 함께 읽는다.
    project 이름 = `<folder>_<mode>_<YYMMDD>_<HHMM>` (batch 별 격리) 생성에 사용.
    """
    conn = open_sensor_read_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT request_id, folder_name, labeling_method, categories, classes,
                       COALESCE(requested_at, completed_at, created_at) AS batch_ts,
                       -- dispatch_requests 에는 source_type 컬럼이 없어 합성 여부를 알 수 없다.
                       -- 정본인 raw_files 로 folder 역조회한다 (dispatch 가 완료된 요청만
                       -- 대상이므로 이 시점엔 raw_files 행이 이미 존재한다).
                       EXISTS (
                           SELECT 1 FROM raw_files rf
                            WHERE rf.source_unit_name = dispatch_requests.folder_name
                              AND rf.source_type = 'genai_output'
                       ) AS is_synthetic,
                       -- 검수자 화면의 출처 배지용. dispatch_requests 에는 genai_engine 도 없어
                       -- 같은 정본에서 읽는다. genai_engine 이 NULL 이 아닌 행만 보므로
                       -- idx_raw_files_genai_engine 으로 좁혀진다 (합성 코호트는 소수).
                       (
                           SELECT rf2.genai_engine
                             FROM raw_files rf2
                            WHERE rf2.source_unit_name = dispatch_requests.folder_name
                              AND rf2.source_type = 'genai_output'
                              AND rf2.genai_engine IS NOT NULL
                            LIMIT 1
                       ) AS genai_engine
                FROM dispatch_requests
                WHERE status = 'completed'
                  AND COALESCE(ls_task_status, 'pending') = 'pending'
                ORDER BY completed_at
                """
            )
            rows = cur.fetchall()
        return [
            {
                "request_id": r[0],
                "folder_name": r[1],
                "labeling_method": r[2],
                "categories": r[3],
                # 2026-06-01: image mode (bbox) 에서 categories 비어있으면 classes 로
                # fallback. UI 사용자가 둘 중 어느 쪽에 넣어도 LS task 생성되게.
                "classes": r[4],
                "batch_ts": r[5],
                "is_synthetic": bool(r[6]),
                # 합성 배치의 생성 엔진 (comfy_local/kling/veo/...). 합성이 아니면 NULL.
                "genai_engine": r[7] or "",
            }
            for r in rows
            if r[1]
        ]
    finally:
        conn.close()


_RESULT_RE = re.compile(
    r"\[RESULT\]\s+mode=(?P<mode>\w+)\s+created=(?P<created>\d+)\s+skipped=(?P<skipped>\d+)"
    r"\s+error=(?P<error>\d+)\s+gated_out=(?P<gated_out>\d+)"
)


def _parse_create_result(stdout: str) -> dict | None:
    """`ls_tasks.py create` 의 기계 판독 줄을 파싱. 없으면 None (구버전 호환)."""
    match = None
    for match in _RESULT_RE.finditer(stdout or ""):
        pass
    if match is None:
        return None
    return {k: int(v) for k, v in match.groupdict().items() if k != "mode"}


def _resolve_ls_task_status(context, request_id: str, ran_modes: list, outcomes: list) -> str:
    """기록할 터미널 상태를 정한다 — 'created' 또는 'skipped'. 부당한 0건이면 raise.

    2026-09-21 실측: exit 0 + `ls_task_status='created'` + LS task 0건이 동시에 성립했다.
    라벨러 게이트가 후보를 전부 걷어냈는데 호출부는 그 사실을 몰랐다.

    "만들 게 없었다"와 "만들 게 있었는데 아무것도 안 갔다"를 섞으면 안 된다. 전자는 정상
    종료(`skipped`)이고 후자는 조사 대상이다. 둘을 `failed` 하나로 뭉치면 운영자의 트리아지
    신호가 죽고, 반대로 둘 다 `created` 로 두면 이번 사건이 그대로 재현된다.
    """
    if not ran_modes:
        # labeling_method 가 video/image 어느 매핑에도 안 걸린 요청 (prod 에 'skip' 행 실재).
        # 서브프로세스가 애초에 안 돌았으므로 [RESULT] 가 없는 것이 정상이다 — 구버전
        # 스크립트로 오진하고 'created' 를 찍으면 안 된다.
        context.log.warning(f"실행된 LS 생성 모드 없음 — 'skipped' 로 기록: request_id={request_id}")
        return "skipped"

    parsed = [(mode, r) for mode, r in outcomes if r is not None]
    if not parsed:
        # 모드는 돌았는데 [RESULT] 가 없다 = 구버전 스크립트. 판정 근거가 없으니 기존 동작 유지.
        context.log.warning(f"[RESULT] 줄 없음 — 생성 건수 검증 skip: request_id={request_id}")
        return "created"

    produced = sum(r["created"] + r["skipped"] for _, r in parsed)
    if produced > 0:
        return "created"

    detail = ", ".join(
        f"{mode}(created={r['created']}, gated_out={r['gated_out']}, error={r['error']})" for mode, r in parsed
    )
    withheld = sum(r["gated_out"] + r["error"] for _, r in parsed)
    if withheld == 0:
        # 후보 자체가 0건 — 아직 SAM3 결과가 없거나 원래 만들 게 없는 배치다. 실패가 아니다.
        context.log.warning(f"LS task 후보 0건 — 'skipped' 로 기록: request_id={request_id}, {detail}")
        return "skipped"
    raise RuntimeError(f"LS task 0건 — 검수자에게 아무것도 가지 않았다: {detail}")


def _genai_batch_id(folder_name: str) -> str:
    """`genai_<batch_id>` 폴더명에서 batch_id 를 떼어낸다. 규약과 다르면 빈 문자열.

    이 규약의 정본은 `docker/genai/storage/manifest.py` 의 `source_unit_name = f"genai_{batch_id}"`
    다 — dispatch·raw_files 양쪽에 같은 이름이 들어간다. raw_files 에는 batch_id 컬럼이 없고
    `genai_jobs.output_asset_id` 도 comfy_local 경로에서는 채워지지 않아(2026-09-21 실측 전량
    NULL) DB 조인으로는 복원할 수 없다.
    """
    name = (folder_name or "").strip()
    prefix = "genai_"
    if not name.startswith(prefix) or len(name) <= len(prefix):
        return ""
    return name[len(prefix) :]


def _synthetic_argv(engine: str, batch_id: str) -> list[str]:
    """합성 배치에 붙일 `ls_tasks.py create` 인자.

    값이 비면 해당 플래그를 빼고 CLI 기본값(빈 문자열 → 화면에는 'unknown')에 맡긴다 —
    빈 값을 넘겨 배지에 공백이 뜨는 것보다 'unknown' 이 낫다.
    """
    argv = ["--synthetic"]
    if engine:
        argv += ["--genai-engine", engine]
    if batch_id:
        argv += ["--genai-batch-id", batch_id]
    return argv


def _format_batch_suffix(ts) -> str:
    """TIMESTAMP → `YYMMDD_HHMM`. None/falsey 이면 빈 문자열(접미사 없음)."""
    if not ts:
        return ""
    try:
        return ts.strftime("%y%m%d_%H%M")
    except Exception:
        return ""


def _update_ls_task_status(
    request_id: str,
    status: str,
    *,
    error_message: str | None = None,
) -> None:
    """dispatch_requests.ls_task_status update — Postgres direct.

    error_message 도 함께 적재 (실패 시 운영자 디버깅 용). status=failed 인데
    error_message 비어있으면 운영자가 root cause 추적 불가 — 2026-05-20 part1
    `Command timed out after 600s` 케이스 발견.
    """
    import psycopg2  # noqa: PLC0415 - lazy import

    dsn = default_postgres_dsn()
    if not dsn:
        raise RuntimeError("DATAOPS_POSTGRES_DSN 미설정 — ls_task_status update 불가")
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            if error_message is not None:
                cur.execute(
                    "UPDATE dispatch_requests SET ls_task_status = %s, error_message = %s WHERE request_id = %s",
                    (status, error_message[:500], request_id),
                )
            else:
                cur.execute(
                    "UPDATE dispatch_requests SET ls_task_status = %s WHERE request_id = %s",
                    (status, request_id),
                )


# ---------------------------------------------------------------------------
# Op
# ---------------------------------------------------------------------------


@op
def create_ls_tasks(context) -> None:
    """dispatch request 별로 ls_tasks.py create 실행."""
    requests = _fetch_pending_dispatch_requests()

    if not requests:
        context.log.info("ls task 생성 대상 없음")
        return

    api_key = os.environ.get("LS_API_KEY", "")
    if not api_key:
        raise RuntimeError("LS_API_KEY 환경변수가 필요합니다.")

    need_sync = is_cross_sync_needed()
    target_ep = ls_minio_endpoint()

    for req in requests:
        request_id = req["request_id"]
        raw_folder = req["folder_name"].rstrip("/")
        # dispatch가 MinIO에 쓰는 key는 sanitize_path_component로 정규화된 폴더명을 사용하므로
        # LS 자동 생성 경로도 동일 규칙으로 prefix를 만들어야 events/원본 영상 매칭이 일치한다.
        folder_name = sanitize_path_component(raw_folder)
        raw_prefix = folder_name

        methods = set(parse_tag_list(req.get("labeling_method")))
        categories = parse_tag_list(req.get("categories"))
        # 2026-06-01: image mode (bbox) 의 LS label config 는 categories 가 필수.
        # categories 비어있으면 classes 로 fallback — 사용자가 promote 폼에서 어느 쪽에
        # 입력해도 LS task 생성. 두 필드 의미상 같은 "라벨링 클래스" 라 union OK.
        classes_list = parse_tag_list(req.get("classes"))
        cat_csv = ",".join(categories)
        # image label set: categories 우선, 비면 classes
        image_label_set = categories if categories else classes_list
        image_label_csv = ",".join(image_label_set)
        batch_suffix = _format_batch_suffix(req.get("batch_ts"))
        is_synthetic = bool(req.get("is_synthetic"))
        # 검수자가 실사 CCTV 와 구분할 수 있도록 LS task 에 실어 보낼 출처 정보.
        genai_engine = str(req.get("genai_engine") or "")
        genai_batch_id = _genai_batch_id(raw_folder)
        run_video = bool(methods & _VIDEO_METHODS)
        run_image = bool(methods & _IMAGE_METHODS)
        # labeling_method 미지정 dispatch (legacy) → video 만 실행
        if not methods:
            run_video = True

        context.log.info(
            f"ls task 생성 시작: request_id={request_id}, folder={raw_folder}, "
            f"prefix={raw_prefix}, methods={sorted(methods)}, "
            f"categories={categories}, classes={classes_list}, "
            f"image_label_set={image_label_set}, "
            f"batch_suffix={batch_suffix or '(none)'}, video={run_video}, image={run_image}, "
            f"synthetic={is_synthetic}, engine={genai_engine or '(none)'}, "
            f"batch_id={genai_batch_id or '(none)'}"
        )

        try:
            # (A/C) staging이면 클립·라벨을 production MinIO로 복사
            if need_sync:
                n = sync_folder_for_ls(
                    folder_name,
                    target_endpoint=target_ep,
                    log_fn=lambda msg: context.log.info(msg),
                )
                context.log.info(f"staging→production 동기화: {n}건 복사")

            statuses: list[tuple[str, int, str]] = []  # (mode, returncode, tail)
            outcomes: list[tuple[str, dict | None]] = []  # (mode, parsed [RESULT])

            def _run_create(mode: str) -> None:
                # --api-key / --minio-endpoint는 ls_tasks.py top-level argparse 옵션이므로
                # 반드시 subcommand(create) 앞에 배치해야 한다.
                argv = [
                    sys.executable,
                    str(LS_TASKS_SCRIPT),
                    "--minio-endpoint",
                    target_ep,
                    "--api-key",
                    api_key,
                    "create",
                    "--mode",
                    mode,
                    "--prefix",
                    raw_prefix,
                ]
                # 2026-06-01: image mode 에선 image_label_csv (= categories or classes fallback),
                # video mode 는 cat_csv (Gemini event 카테고리) 사용.
                effective_csv = image_label_csv if mode == "image" else cat_csv
                if effective_csv:
                    argv += ["--categories", effective_csv]
                if batch_suffix:
                    argv += ["--project-suffix", batch_suffix]
                if is_synthetic:
                    # 합성본은 자동 검출 0건이어도 사람에게 보낸다 — ls_task_gate 참고.
                    # 엔진/batch_id 는 검수 화면의 출처 배지로 간다 (ls_tasks_label_config).
                    argv += _synthetic_argv(genai_engine, genai_batch_id)
                # 2026-05-20 finding: part1 bbox 1249 → 12,532 image LS 등록 시 600s 부족 → 5번 retry 모두 timeout.
                # image mode 일 때 timeout 충분히 크게 (default 1800s = 30분). env var 로 override 가능.
                timeout_sec = int_env("LS_TASKS_CREATE_TIMEOUT_SEC", 1800)
                try:
                    result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout_sec)
                except subprocess.TimeoutExpired as exc:
                    # subprocess.TimeoutExpired 의 default __str__ 가 argv 전체 (API key 포함) 노출 →
                    # 마스킹된 메시지로 재발생.
                    raise RuntimeError(
                        f"ls_tasks.py create --mode {mode} timed out after {timeout_sec}s (prefix={raw_prefix})"
                    ) from exc
                tail = (result.stderr or result.stdout or "").strip().splitlines()[-20:]
                context.log.info(f"[ls_tasks create --mode {mode}] stdout:\n{result.stdout}")
                if result.returncode != 0:
                    context.log.error(f"[ls_tasks create --mode {mode}] stderr:\n{result.stderr}")
                statuses.append((mode, result.returncode, "\n".join(tail)))
                outcomes.append((mode, _parse_create_result(result.stdout)))

            if run_video:
                _run_create("video")
            if run_image:
                # 2026-06-01: categories 가 비어도 classes 로 fallback (image_label_set).
                # 둘 다 비어있을 때만 skip. fallback 사실은 INFO 로그로 표시.
                if not image_label_set:
                    context.log.warning(
                        f"image mode 요청됐지만 categories/classes 둘 다 비어있음 — skip: " f"request_id={request_id}"
                    )
                else:
                    if not categories and classes_list:
                        context.log.info(
                            f"image mode label_config: categories 비어있음 → classes 로 fallback "
                            f"({classes_list}). request_id={request_id}"
                        )
                    _run_create("image")

            failed_modes = [m for m, rc, _ in statuses if rc != 0]
            if failed_modes:
                raise RuntimeError(f"ls_tasks.py create 실패: modes={failed_modes}")

            ls_status = _resolve_ls_task_status(context, request_id, [m for m, _, _ in statuses], outcomes)
            _update_ls_task_status(request_id, ls_status)
            context.log.info(f"ls_task_status={ls_status!r} 업데이트: request_id={request_id}")

        except Exception as exc:
            context.log.error(f"ls task 생성 실패: request_id={request_id} — {exc}")
            _update_ls_task_status(request_id, "failed", error_message=str(exc))


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


@job(name="ls_task_create_job", description="dispatch 완료 후 LS task 자동 생성")
def ls_task_create_job():
    create_ls_tasks()


# ---------------------------------------------------------------------------
# Sensor
# ---------------------------------------------------------------------------


@sensor(
    job=ls_task_create_job,
    name="ls_task_create_sensor",
    minimum_interval_seconds=int_env("LS_TASK_SENSOR_INTERVAL_SEC", 60, 30),
    default_status=DefaultSensorStatus.RUNNING,
    description="dispatch 완료 후 LS task 미생성 요청 감지 → ls_task_create_job 트리거",
)
def ls_task_create_sensor(context):
    # 2026-05-22 fix: in-flight check 추가. 이전엔 60s 간격 fire 시 첫 job 이 status='created'
    # 로 update 하기 전까지 같은 pending request 를 다시 picking up → 누적 job (2026-05-21 QA
    # 에서 4 job 동시 진행). LS API 가 idempotent 라 결과는 안전했지만 운영 noise + 자원 낭비.
    try:
        in_flight_runs = context.instance.get_runs(
            filters=RunsFilter(statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED]),
            limit=200,
        )
    except Exception as exc:
        yield SkipReason(f"ls_task in-flight run 조회 실패: {exc}")
        return

    active_jobs = sorted(
        {str(run.job_name) for run in in_flight_runs if str(getattr(run, "job_name", "") or "") == "ls_task_create_job"}
    )
    if active_jobs:
        yield SkipReason(f"ls_task_create_job already running: {', '.join(active_jobs)}")
        return

    try:
        pending = _fetch_pending_dispatch_requests()
    except Exception as exc:
        yield SkipReason(f"DB 조회 실패: {exc}")
        return

    if not pending:
        yield SkipReason("ls task 생성 대기 중인 dispatch 없음")
        return

    request_ids = [r["request_id"] for r in pending]
    context.log.info(f"ls task 생성 대상 {len(pending)}건: {request_ids}")

    yield RunRequest(
        run_key=f"ls-task-create-{int(time.time())}",
        tags={"trigger": "ls_task_create_sensor", "pending_count": str(len(pending))},
    )


# ---------------------------------------------------------------------------
# Presigned URL 갱신 Job + Schedule
# ---------------------------------------------------------------------------


@op
def renew_ls_presigned_urls(context) -> None:
    """모든 LS project의 만료 임박 presigned URL 갱신."""
    api_key = os.environ.get("LS_API_KEY", "")
    if not api_key:
        context.log.warning("LS_API_KEY 미설정 — presigned URL 갱신 건너뜀")
        return

    target_ep = ls_minio_endpoint()
    # --api-key / --minio-endpoint는 ls_tasks.py top-level argparse 옵션이므로
    # 반드시 subcommand(renew) 앞에 배치해야 한다 (subparser는 모름 → exit=2).
    result = subprocess.run(
        [
            sys.executable,
            str(LS_TASKS_SCRIPT),
            "--minio-endpoint",
            target_ep,
            "--api-key",
            api_key,
            "renew",
            "--all-projects",
        ],
        capture_output=True,
        text=True,
        timeout=600,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(f"presigned URL 갱신 실패:\n{result.stderr}")
        raise RuntimeError(f"ls_tasks.py renew 실패 (exit={result.returncode})")


@job(name="ls_presign_renew_job", description="LS presigned URL 만료 임박 자동 갱신")
def ls_presign_renew_job():
    renew_ls_presigned_urls()


ls_presign_renew_schedule = ScheduleDefinition(
    name="ls_presign_renew_schedule",
    job=ls_presign_renew_job,
    cron_schedule="0 5 * * *",
    execution_timezone="Asia/Seoul",
)
