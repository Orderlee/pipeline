"""ls_webhook PostgreSQL finalize + sync-and-notify runner."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

from vlm_pipeline.lib.slack_notify import send_slack_alert, send_slack_response

from gemini.ls_webhook_env import (
    API_KEY,
    DEFAULT_FPS,
    DEFAULT_LABEL_BUCKET,
    LS_URL,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    PG_DSN,
    POST_REVIEW_JOB_NAME,
)
from gemini.ls_webhook_state import (
    _SYNC_GUARD_LOCK,
    _SYNC_IN_FLIGHT,
    load_state,
    save_state,
)


def _extract_folder_prefixes(label_keys: list[str]) -> list[str]:
    """label_keys 의 첫 path component 집합을 반환 (folder_name).

    label_keys 는 보통 `<source_unit_name>/...json` 형식이라 첫 "/" 까지가 folder
    prefix 가 된다. 같은 LS project 의 키는 일반적으로 하나의 folder 에 속하지만,
    안전을 위해 set 으로 모은다.
    """
    prefixes: set[str] = set()
    for k in label_keys or []:
        if not k or "/" not in k:
            continue
        prefixes.add(k.split("/", 1)[0])
    return sorted(prefixes)


def _project_bbox_after_finalize(image_label_ids: list[str]) -> int:
    """이번 finalize 에서 확정된 bbox image_labels(COCO)의 박스를 image_label_annotations 로 투영.

    finalize 직후 호출되는 best-effort 후속 단계. MinIO COCO JSON 이 SOT 이고 이
    테이블은 재생성 가능한 파생 인덱스이므로 실패해도 finalize 결과를 되돌리지 않는다
    (호출자가 예외를 삼킨다). per-row fail-forward — 한 라벨이 깨져도 나머지는 진행.

    대상은 finalize_labels_in_db 가 ``RETURNING`` 으로 모은, **이번 호출에서 실제로
    'finalized' 로 전이된** image_label_id 집합으로 한정한다. 같은 folder 의 기존 확정
    행을 prefix LIKE 로 재투영하는 over-match 를 피한다 (불필요한 MinIO 재읽기 방지).
    label_format='coco' 인 행만 처리 (video 이벤트 라벨은 labels 테이블 소관).

    반환: 투영된(insert 된) 박스 총수.
    """
    import psycopg2
    import psycopg2.errors

    from gemini.ls_sync_db import replace_image_label_annotations
    from gemini.ls_tasks_minio import build_minio_client, read_json_from_minio
    from vlm_pipeline.lib.detection_coco import parse_coco_annotation_boxes

    if not image_label_ids or not PG_DSN:
        return 0

    minio = build_minio_client(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    conn = psycopg2.connect(PG_DSN)
    total = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT image_label_id, image_id, COALESCE(labels_bucket, %s), labels_key
                FROM image_labels
                WHERE review_status = 'finalized'
                  AND label_format = 'coco'
                  AND image_label_id = ANY(%s)
                """,
                [DEFAULT_LABEL_BUCKET, list(image_label_ids)],
            )
            rows = cur.fetchall()
        for image_label_id, image_id, bucket, key in rows:
            try:
                payload = read_json_from_minio(minio, bucket, key)
                boxes = parse_coco_annotation_boxes(payload)
                _, inserted = replace_image_label_annotations(conn, image_label_id, image_id, boxes)
                conn.commit()
                total += inserted
            except psycopg2.errors.UniqueViolation as exc:
                # 동시 finalize 가 같은 image_label_id 를 투영 중 — annotation_id 가 결정적이라
                # 최종 상태는 동일. 데이터 손상 아님, 이 행만 skip.
                conn.rollback()
                print(
                    f"[FINALIZE/bbox-projection] concurrent projection skip (data ok) image_label_id={image_label_id}: {exc}"
                )
            except Exception as exc:  # noqa: BLE001 — per-row fail-forward
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"[FINALIZE/bbox-projection] skip image_label_id={image_label_id} key={key}: {exc}")
        if rows:
            print(f"[FINALIZE/bbox-projection] {len(rows)} image_labels → {total} boxes projected")
        return total
    finally:
        conn.close()


def finalize_labels_in_db(label_keys: list[str]) -> dict:
    """label_status='completed', review_status='finalized' 업데이트.

    labels (video-level) 와 image_labels (image-level) 양쪽에 동일 UPDATE 적용.
    labels_key 스키마가 서로 겹치지 않으므로 각 테이블은 자신의 key 만 매칭됨.

    잔존 row 자동 보정:
      1) state.label_keys IN ({...}) 매칭 1차 UPDATE (review_status<>'finalized')
      2) label_keys 의 folder prefix LIKE 매칭으로 review_status='reviewed' 잔존
         row 도 자동 finalize.
         SOT: 사람 LS submit 결과는 ls_sync.upsert_video_labels /
         update_image_labels_in_db 가 review_status='reviewed' 로 박는다 → 같은
         folder 의 'reviewed' 는 사람 검수 완료 의미라 finalize 안전.
         'pending_review'/'in_review' 등은 prefix 매칭에서 제외하여 미검수 row 보호.

    반환값: {"primary": int, "auto_recovered": int, "bbox_annotations": int}
      - primary: state.label_keys IN 매칭으로 finalize 된 row (양 테이블 합)
      - auto_recovered: prefix LIKE 매칭으로 추가 finalize 된 row (양 테이블 합)
      - bbox_annotations: finalize 직후 image_label_annotations 로 투영된 박스 수 (best-effort)
    """
    import psycopg2

    if not label_keys or not PG_DSN:
        return {"primary": 0, "auto_recovered": 0, "bbox_annotations": 0}
    conn = None
    for attempt in range(5):
        try:
            conn = psycopg2.connect(PG_DSN)
            break
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            if attempt < 4:
                time.sleep(2)
                continue
            raise
    if conn is None:
        raise RuntimeError("PostgreSQL 연결 실패 (5회 재시도 초과)")
    # 모든 UPDATE/COUNT 를 단일 트랜잭션으로 묶고 마지막에 commit (psycopg2 autocommit=False).
    # DuckDB 는 statement 마다 auto-commit 이었으나 PG 는 명시 commit 필요.
    try:
        # 이번 호출에서 실제로 'finalized' 로 전이된 image_labels.image_label_id 만 모은다.
        # bbox projection 을 정확히 이 집합으로 한정 → prefix LIKE over-match / 재투영 방지 (codex MEDIUM-1).
        finalized_image_label_ids: set[str] = set()
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(label_keys))
            # Video-level labels (Gemini events) — image_labels 와 동일 정책 (review_status 기준)
            # 기존엔 label_status='pending_review' 조건이었으나, captioning.py 등 upstream 이
            # INSERT 시 label_status='completed' 로 박아 prod 의 모든 video labels 가 'completed'
            # 상태였기에 UPDATE 가 0건 영향 → review_status 가 'finalized' 로 못 가서
            # build_dataset_on_finalize_sensor 가 video 검수 프로젝트를 영구히 못 잡았던 버그.
            cur.execute(
                f"""
                UPDATE labels
                SET label_status  = 'completed',
                    review_status = 'finalized'
                WHERE labels_key IN ({placeholders})
                  AND review_status <> 'finalized'
                """,
                label_keys,
            )
            # Image-level labels (SAM3 segmentations) — review_status 기준으로 전이
            cur.execute(
                f"""
                UPDATE image_labels
                SET label_status  = 'completed',
                    review_status = 'finalized'
                WHERE labels_key IN ({placeholders})
                  AND review_status <> 'finalized'
                RETURNING image_label_id
                """,
                label_keys,
            )
            finalized_image_label_ids.update(r[0] for r in cur.fetchall() if r[0])
            cur.execute(
                f"""
                SELECT
                  (SELECT COUNT(*) FROM labels
                     WHERE labels_key IN ({placeholders}) AND review_status = 'finalized')
                  +
                  (SELECT COUNT(*) FROM image_labels
                     WHERE labels_key IN ({placeholders}) AND review_status = 'finalized')
                """,
                label_keys + label_keys,
            )
            primary_row = cur.fetchone()
            primary = int(primary_row[0]) if primary_row else 0

            # 2차: folder prefix 기반 잔존 자동 보정.
            # state.label_keys 에 빠진 키가 있어도 같은 folder 의 사람 검수 완료 row 는
            # 함께 finalize. 'reviewed' 만 매칭하여 미검수 row 는 안전하게 제외.
            auto_recovered = 0
            prefixes = _extract_folder_prefixes(label_keys)
            if prefixes:
                patterns = [p + "/%" for p in prefixes]
                or_clauses = " OR ".join(["labels_key LIKE %s"] * len(prefixes))
                # pre-count: 매칭 row 가 곧 UPDATE 영향 row (단일 트랜잭션 + 동시 INSERT 없음 가정).
                cur.execute(
                    f"SELECT COUNT(*) FROM labels WHERE ({or_clauses}) AND review_status = 'reviewed'",
                    patterns,
                )
                pre_v = cur.fetchone()
                cur.execute(
                    f"SELECT COUNT(*) FROM image_labels WHERE ({or_clauses}) AND review_status = 'reviewed'",
                    patterns,
                )
                pre_i = cur.fetchone()
                pending_v = int(pre_v[0]) if pre_v else 0
                pending_i = int(pre_i[0]) if pre_i else 0
                if pending_v:
                    cur.execute(
                        f"""
                        UPDATE labels
                        SET label_status  = 'completed',
                            review_status = 'finalized'
                        WHERE ({or_clauses}) AND review_status = 'reviewed'
                        """,
                        patterns,
                    )
                if pending_i:
                    cur.execute(
                        f"""
                        UPDATE image_labels
                        SET label_status  = 'completed',
                            review_status = 'finalized'
                        WHERE ({or_clauses}) AND review_status = 'reviewed'
                        RETURNING image_label_id
                        """,
                        patterns,
                    )
                    finalized_image_label_ids.update(r[0] for r in cur.fetchall() if r[0])
                auto_recovered = pending_v + pending_i
                if auto_recovered:
                    print(
                        f"[FINALIZE] folder prefix 잔존 자동 보정: labels +{pending_v}, "
                        f"image_labels +{pending_i} (prefixes={prefixes})"
                    )
        conn.commit()
        # finalize 커밋 후 best-effort bbox projection (derived/재생성 가능 — 실패해도 finalize 유지).
        # 별도 커넥션을 쓰므로 위 트랜잭션과 무관. 예외는 삼켜 finalize 결과 반환을 보장.
        bbox_annotations = 0
        try:
            bbox_annotations = _project_bbox_after_finalize(sorted(finalized_image_label_ids))
        except Exception as exc:  # noqa: BLE001 — projection 은 재생성 가능, finalize 를 막지 않음
            print(f"[FINALIZE] bbox annotation projection failed (non-fatal, rebuildable): {exc}")
        return {
            "primary": primary,
            "auto_recovered": auto_recovered,
            "bbox_annotations": bbox_annotations,
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def run_sync_and_notify(project_id: int, project_title: str) -> None:
    """ls_sync.run() 실행 후 Slack 알림 및 state 업데이트."""
    # debounce: 같은 project의 sync가 이미 돌고 있으면 drop (웹훅 중복 수신 대응).
    with _SYNC_GUARD_LOCK:
        if project_id in _SYNC_IN_FLIGHT:
            print(f"[DEDUP] project {project_id} sync 이미 진행 중 — skip")
            return
        _SYNC_IN_FLIGHT.add(project_id)

    try:
        _run_sync_and_notify_inner(project_id, project_title)
    finally:
        with _SYNC_GUARD_LOCK:
            _SYNC_IN_FLIGHT.discard(project_id)


def _run_sync_and_notify_inner(project_id: int, project_title: str) -> None:
    state = load_state()
    pid = str(project_id)
    is_first = state.get(pid, {}).get("last_sync_at") is None

    if not PG_DSN:
        print("[ERROR] DATAOPS_POSTGRES_DSN 미설정 — sync 불가")
        send_slack_alert(f"❌ *{project_title}* sync 실패: PostgreSQL DSN 미설정")
        return

    sync_result: dict = {}
    try:
        import sys as _sys

        _gemini_dir = str(Path(__file__).parent)
        if _gemini_dir not in _sys.path:
            _sys.path.insert(0, _gemini_dir)
        from ls_sync import run as sync_run

        sync_result = (
            sync_run(
                project_id=project_id,
                ls_url=LS_URL,
                api_key=API_KEY,
                fps=DEFAULT_FPS,
                minio_endpoint=MINIO_ENDPOINT,
                minio_access_key=MINIO_ACCESS_KEY,
                minio_secret_key=MINIO_SECRET_KEY,
                bucket=DEFAULT_LABEL_BUCKET,
                prefix="",
                dry_run=False,
                dsn=PG_DSN,
            )
            or {}
        )
    except Exception as exc:
        print(f"[ERROR] sync 실패: {exc}")
        send_slack_alert(f"❌ *{project_title}* (id={project_id}) sync 실패: {exc}")
        return

    # state 업데이트.
    # ls_tasks.py create 시점에 박힌 label_keys 외에, ls_sync 가 신규 생성한 events
    # / sam3 JSON (예: Gemini auto 누락이었던 영상에 사람이 라벨 작성한 케이스) 도
    # union merge 해서 finalize 누락 방지.
    now = datetime.now(timezone.utc).isoformat()
    entry = state.get(pid, {})
    existing_keys = set(entry.get("label_keys", []))
    processed_keys = set(sync_result.get("processed_label_keys", []))
    merged_keys = sorted(existing_keys | processed_keys)
    entry.update(
        {
            "project_id": project_id,
            "title": project_title,
            "last_sync_at": now,
            "status": "pending_finalize",
            "label_keys": merged_keys,
        }
    )
    state[pid] = entry
    save_state(state)
    if processed_keys - existing_keys:
        added = sorted(processed_keys - existing_keys)
        print(f"[INFO] state.json label_keys 에 {len(added)}건 신규 추가: {added}")

    # FinalizedLabelsSkip / FinalizedImageSkip 으로 인해 skip 된 항목 — 운영자 인지.
    skipped_finalized = int(sync_result.get("skipped_finalized", 0) or 0)
    skip_suffix = f"  (finalized skip {skipped_finalized}건)" if skipped_finalized else ""
    if skipped_finalized:
        print(f"[INFO] sync 중 finalized 보호 가드로 {skipped_finalized}건 skip (MinIO/PG 보존)")

    if is_first:
        send_slack_alert(
            f"[동기화 완료] *{project_title}* (id={project_id}) — 검수 반영됨{skip_suffix}\n"
            f"최종 확정이 필요합니다. `/sync-list` 로 확인하세요."
        )
    else:
        send_slack_alert(f"[재동기화] *{project_title}* (id={project_id}) — 수정사항 반영됨, 확정 대기 중{skip_suffix}")


def finalize_project(project_id: int, response_url: str = "") -> None:
    """최종 확정: PostgreSQL label_status='completed' → downstream 활성화."""
    from gemini.ls_webhook_dagster import trigger_dagster_job

    state = load_state()
    pid = str(project_id)
    info = state.get(pid)

    if not info:
        msg = f"❌ project {project_id} state 정보 없음 (ls_tasks.py create가 먼저 실행되어야 합니다)"
        send_slack_alert(msg)
        send_slack_response(response_url, msg)
        return

    if info.get("status") == "finalized":
        msg = f"ℹ️ *{info['title']}* (id={project_id}) 이미 확정된 project입니다."
        send_slack_response(response_url, msg)
        return

    title = info.get("title", str(project_id))
    label_keys = info.get("label_keys", [])

    if not label_keys:
        msg = f"❌ *{title}* label_keys 없음 — state 파일 확인 필요"
        send_slack_alert(msg)
        send_slack_response(response_url, msg)
        return

    result = finalize_labels_in_db(label_keys)
    primary = int(result.get("primary", 0))
    auto_recovered = int(result.get("auto_recovered", 0))
    total_finalized = primary + auto_recovered

    # state 업데이트
    info["status"] = "finalized"
    info["finalized_at"] = datetime.now(timezone.utc).isoformat()
    state[pid] = info
    save_state(state)

    # Dagster post_review_clip_job 트리거 — 검수된 labels.timestamp로 clip 분할
    ok, detail = trigger_dagster_job(
        POST_REVIEW_JOB_NAME,
        tags={
            "trigger": "ls_finalize",
            "project_id": project_id,
            "folder_name": title,
        },
    )
    clip_msg = (
        f"→ clip 생성 job 트리거: run_id={detail}"
        if ok
        else (
            f"→ ⚠️ clip 생성 트리거 실패 ({detail})\n"
            f"   수동 재실행: `python /src/vlm/gemini/ls_webhook.py finalize --project {project_id}`\n"
            f"   또는 Dagster UI에서 `{POST_REVIEW_JOB_NAME}` 수동 실행"
        )
    )

    if auto_recovered:
        db_msg = (
            f"→ PostgreSQL {total_finalized}건 반영 (state 매칭 {primary} + folder prefix 자동 보정 {auto_recovered})"
        )
    else:
        db_msg = f"→ PostgreSQL {total_finalized}건 반영"

    msg = f"✅ *{title}* (id={project_id}) 최종 확정 완료\n{db_msg}\n{clip_msg}"
    print(f"[FINALIZE] {msg}")
    send_slack_alert(msg)
    send_slack_response(response_url, msg)
