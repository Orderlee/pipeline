"""Sync Label Studio annotations back to MinIO labels JSON + PostgreSQL labels.

Source of truth = LS 사람 submit. annotation 결과를 그대로 vlm-labels 버킷과
PostgreSQL labels 테이블에 반영한다. (upstream Gemini 결과의 유무·내용은 무시)

흐름 (video / TimelineLabels):
  LS task → annotation result(timelinelabels) 추출
    → MinIO vlm-labels/<folder>/<sub>/events/<stem>.json 결정
        · 기존 events JSON 있으면 그 키 재사용
        · 없으면 raw_key 에서 events JSON 키 계산 (신규 생성)
    → MinIO put (이벤트 M개 또는 [] 모두 동일 경로로 저장)
    → PostgreSQL: DELETE WHERE labels_key=%s 후 M개 INSERT (M=0이면 DELETE만)

흐름 (image / RectangleLabels): cdd83c9 의 image 분기 정책 유지 — 빈 검수면
    annotations=[] 로 기존 sam3 JSON 비우고, 기존 JSON 부재 시엔 생성 안 함.

주의:
  - `/sync-approve` 로 finalized 된 후의 재submit 은 본 sync 흐름에서 다루지
    않는다 (processed_clips.source_label_id dangling 위험). 재검수 정책이
    필요하면 별도 설계 필요.

Usage:
    python ls_sync.py --project 3 --api-key "..."
    python ls_sync.py --project 3 --api-key "..." --prefix hyundai_v2/01_27
    python ls_sync.py --project 3 --api-key "..." --dry-run
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures as cf
import json
import os
import threading
from pathlib import Path, PurePosixPath

import requests  # noqa: E402

try:
    from gemini.ls_tasks_minio import build_minio_client
    from gemini.ls_sync_db import (
        _connect_postgres_with_retry,
        lookup_asset_id_by_raw_key,
        upsert_video_labels,
        update_image_labels_in_db,
        FinalizedLabelsSkip,
        FinalizedImageSkip,
        ConcurrentLabelsRace,
    )
    from gemini.ls_sync_converters import (
        _CLIP_PATTERN,
        _extract_raw_key_from_video_url,
        _resolve_labels_key_for_video,
        annotation_to_events,
        _parse_image_url_to_key,
        _sam3_key_from_image_key,
        annotation_to_rectangles,
        build_reviewed_coco_json,
    )
except ModuleNotFoundError:
    from ls_tasks_minio import build_minio_client  # type: ignore[no-redef]
    from ls_sync_db import (  # type: ignore[no-redef]
        _connect_postgres_with_retry,
        lookup_asset_id_by_raw_key,
        upsert_video_labels,
        update_image_labels_in_db,
        FinalizedLabelsSkip,
        FinalizedImageSkip,
        ConcurrentLabelsRace,
    )
    from ls_sync_converters import (  # type: ignore[no-redef]
        _CLIP_PATTERN,
        _extract_raw_key_from_video_url,
        _resolve_labels_key_for_video,
        annotation_to_events,
        _parse_image_url_to_key,
        _sam3_key_from_image_key,
        annotation_to_rectangles,
        build_reviewed_coco_json,
    )

DEFAULT_LS_URL = "http://localhost:8080"
DEFAULT_MINIO_ENDPOINT = "10.0.0.51:9000"
DEFAULT_MINIO_ACCESS_KEY = ""
DEFAULT_MINIO_SECRET_KEY = ""
DEFAULT_BUCKET = "vlm-labels"
DEFAULT_FPS = 24


# ---------------------------------------------------------------------------
# MinIO
# ---------------------------------------------------------------------------


def build_json_key_index(client, bucket: str, prefix: str = "") -> dict[str, str]:
    """stem → MinIO key 인덱스. (*/events/*.json만 대상)"""
    paginator = client.get_paginator("list_objects_v2")
    index: dict[str, str] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            if key.endswith(".json") and len(parts) >= 2 and parts[-2] == "events":
                index[Path(key).stem] = key
    return index


def read_json(client, bucket: str, key: str) -> list | dict:
    resp = client.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))


def write_json(client, bucket: str, key: str, data: list | dict) -> None:
    body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


# ---------------------------------------------------------------------------
# Label Studio
# ---------------------------------------------------------------------------


def resolve_auth_headers(ls_url: str, token: str) -> dict[str, str]:
    try:
        payload_part = token.split(".")[1]
        payload_part += "=" * (-len(payload_part) % 4)
        payload = json.loads(base64.b64decode(payload_part).decode("utf-8"))
        if payload.get("token_type") == "refresh":
            resp = requests.post(f"{ls_url}/api/token/refresh/", json={"refresh": token})
            resp.raise_for_status()
            return {"Authorization": f"Bearer {resp.json()['access']}"}
    except Exception:
        pass
    return {"Authorization": f"Token {token}"}


class _LSAuth:
    """JWT access token auto-refresh wrapper.

    ls_sync가 수백 개 task를 순차 조회하는 동안 access token(기본 5분 만료)이 끊어지면
    401을 만나 실패했다. 호출 한 번에 만료 감지 → refresh 재시도 한 번으로 끊기지 않게 한다.
    병렬 fetch 시 여러 스레드가 동시에 refresh하지 않도록 Lock으로 보호.
    """

    def __init__(self, ls_url: str, token: str) -> None:
        self.ls_url = ls_url
        self.token = token
        self._lock = threading.Lock()
        self.headers = resolve_auth_headers(ls_url, token)

    def refresh(self) -> None:
        with self._lock:
            self.headers = resolve_auth_headers(self.ls_url, self.token)


def _ls_get(auth: _LSAuth, url: str, params: dict | None = None):
    resp = requests.get(url, headers=auth.headers, params=params)
    if resp.status_code == 401:
        auth.refresh()
        resp = requests.get(url, headers=auth.headers, params=params)
    resp.raise_for_status()
    return resp


def fetch_annotated_tasks(ls_url: str, auth: _LSAuth, project_id: int) -> list[dict]:
    """annotation이 1개 이상 있는 task만 반환."""
    annotated = []
    page = 1
    while True:
        resp = _ls_get(
            auth,
            f"{ls_url}/api/tasks/",
            params={"project": project_id, "page": page, "page_size": 500},
        )
        data = resp.json()
        tasks = data if isinstance(data, list) else data.get("tasks", [])
        if not tasks:
            break
        for task in tasks:
            if task.get("total_annotations", 0) > 0:
                annotated.append(task)
        if isinstance(data, list) or not data.get("next"):
            break
        page += 1
    return annotated


def fetch_task_detail(ls_url: str, auth: _LSAuth, task_id: int) -> dict:
    return _ls_get(auth, f"{ls_url}/api/tasks/{task_id}/").json()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run(
    project_id: int,
    ls_url: str,
    api_key: str,
    fps: int,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    bucket: str,
    prefix: str,
    dry_run: bool,
    dsn: str | None = None,
) -> dict:
    """LS annotation → MinIO labels JSON + PostgreSQL labels 동기화.

    반환값:
        {
            "processed_label_keys": list[str],   # 이번 sync 가 실제로 쓴 labels_key
                                                  # (video events JSON + image sam3 JSON 모두)
            "updated":   int,
            "empty_save": int,
            "skipped":   int,
            "no_match":  int,
            "error":     int,
        }
        호출자(ls_webhook._run_sync_and_notify_inner)가 processed_label_keys 를
        받아 ls_review_state.json 의 label_keys 에 union merge 한다.

        이전엔 ls_tasks.py create 시점에 박힌 label_keys 만 state.json 에 있고,
        ls_sync 가 신규 events JSON 을 생성해도 (Scenario A: Gemini auto 누락 +
        사람이 라벨 작성) state 에 추가되지 않아 finalize 시 누락되는 결함이
        있었음. 본 함수의 반환값으로 호출자가 동기화한다.
    """
    print(f"[INFO] MinIO: {minio_endpoint}, bucket={bucket}")
    minio = build_minio_client(minio_endpoint, minio_access_key, minio_secret_key)

    print("[INFO] MinIO JSON 인덱스 구성 중...")
    json_index = build_json_key_index(minio, bucket, prefix)
    print(f"[INFO] events/*.json {len(json_index)}개 인덱싱 완료")

    print(f"[INFO] Label Studio annotation 조회 중... (project={project_id})")
    auth = _LSAuth(ls_url, api_key)
    annotated_tasks = fetch_annotated_tasks(ls_url, auth, project_id)
    print(f"[INFO] annotation 있는 task {len(annotated_tasks)}개\n")

    # task detail 병렬 pre-fetch (LS API rate limit 고려, 기본 8 동시). 0이면 순차 fallback.
    fetch_workers = int(os.getenv("LS_SYNC_FETCH_WORKERS", "8"))
    detail_cache: dict[int, dict | None] = {}
    if fetch_workers > 1 and len(annotated_tasks) > 1:
        print(f"[INFO] task detail 병렬 fetch (workers={fetch_workers})")
        with cf.ThreadPoolExecutor(max_workers=fetch_workers) as ex:
            futs = {ex.submit(fetch_task_detail, ls_url, auth, t["id"]): t["id"] for t in annotated_tasks}
            for fut in cf.as_completed(futs):
                tid = futs[fut]
                try:
                    detail_cache[tid] = fut.result()
                except Exception as exc:
                    print(f"[ERROR]    task {tid} detail fetch 실패: {exc}")
                    detail_cache[tid] = None

    updated = empty_save = skipped = no_match = error = skipped_finalized = 0
    # 이번 sync 에서 처리한 labels_key 들 (video events + image sam3).
    # 호출자(ls_webhook)가 받아 ls_review_state.json 의 label_keys 에 union merge.
    processed_label_keys: set[str] = set()

    # PostgreSQL 커넥션을 루프 전체에서 재사용 (매 UPDATE마다 open/close 오버헤드 제거).
    # autocommit=False — 각 task 의 upsert/update 가 자기 작업을 commit() 하므로,
    # 뒤 task 가 실패해 rollback 돼도 앞서 commit 된 task 결과는 보존된다.
    db_conn = None
    if dsn and not dry_run:
        try:
            db_conn = _connect_postgres_with_retry(dsn)
        except Exception as exc:
            print(f"[WARN] PostgreSQL 커넥션 재사용 실패 — 기존 방식(매번 open) 사용: {exc}")

    for task in annotated_tasks:
        task_id = task["id"]
        try:
            detail = detail_cache.get(task_id) if detail_cache else None
            if detail is None and task_id not in detail_cache:
                detail = fetch_task_detail(ls_url, auth, task_id)
            if detail is None:
                error += 1
                continue
            annotations = detail.get("annotations", [])
            if not annotations:
                skipped += 1
                continue

            # 가장 최근 annotation 사용
            annotation = sorted(annotations, key=lambda a: a["updated_at"])[-1]
            if annotation.get("was_cancelled"):
                skipped += 1
                continue

            # 모드 판별 — result 아이템 type 기준
            result_types = {str(r.get("type") or "") for r in annotation.get("result", [])}
            data = detail.get("data", {}) or {}

            # ───────── image mode (RectangleLabels → SAM3 COCO JSON) ─────────
            # data.image가 있으면 image 프로젝트로 간주. result가 비어도 "객체 없음 확정" 검수로 처리.
            if "rectanglelabels" in result_types or data.get("image"):
                image_url = data.get("image", "")
                image_key = _parse_image_url_to_key(image_url)
                if not image_key:
                    print(f"[NO MATCH] task {task_id} → image URL 파싱 실패: {image_url[:120]}")
                    no_match += 1
                    continue
                sam3_key = _sam3_key_from_image_key(image_key)

                rectangles = annotation_to_rectangles(annotation)

                try:
                    existing = read_json(minio, bucket, sam3_key)
                    if not isinstance(existing, dict):
                        existing = {}
                except Exception:
                    existing = {}

                reviewed_json = build_reviewed_coco_json(existing, rectangles, image_key)

                if dry_run:
                    tag = "[DRY-RUN]" if rectangles else "[DRY-EMPTY]"
                    print(f"{tag}  task {task_id} → {sam3_key} ({len(rectangles)}개 bbox)")
                    processed_label_keys.add(sam3_key)
                else:
                    try:
                        db_updated = update_image_labels_in_db(dsn, sam3_key, len(rectangles), conn=db_conn)
                    except FinalizedImageSkip:
                        print(f"[SKIP-FIN] task {task_id} → {sam3_key} finalized, MinIO overwrite 방지")
                        skipped_finalized += 1
                        continue
                    # 빈 검수는 기존 JSON이 있을 때만 annotations를 비워서 덮어씀 (없으면 생성 안 함)
                    if rectangles or existing:
                        write_json(minio, bucket, sam3_key, reviewed_json)
                    tag = "[UPDATED]" if rectangles else "[EMPTY]   "
                    print(f"{tag}  task {task_id} → {sam3_key} (bbox {len(rectangles)}, DB {db_updated}건)")
                    processed_label_keys.add(sam3_key)
                updated += 1
                continue

            # ───────── video mode (TimelineLabels → events JSON) ─────────
            # SOT = 사람 submit. events 가 비어있어도(false-positive 확정) 그대로 저장.
            # MinIO events JSON 키:
            #   - 기존 인덱스 hit → 그 키 사용
            #   - miss             → raw_key 에서 <folder>/<sub>/events/<stem>.json 계산
            video_url = data.get("video", "")
            raw_key = _extract_raw_key_from_video_url(video_url)
            if not raw_key:
                print(f"[ERROR]    task {task_id} → video URL 파싱 실패: {video_url[:120]}")
                error += 1
                continue

            stem = PurePosixPath(raw_key).stem
            m = _CLIP_PATTERN.match(stem)
            base = m.group(1) if m else stem

            json_key = json_index.get(base) or _resolve_labels_key_for_video(raw_key, stem)

            asset_id = lookup_asset_id_by_raw_key(dsn, raw_key, conn=db_conn)
            if not asset_id:
                print(f"[ERROR]    task {task_id} → raw_files miss: raw_key={raw_key}")
                error += 1
                continue

            new_events = annotation_to_events(annotation, fps)
            ev_count = len(new_events)

            if dry_run:
                tag = "[DRY-EMPTY]" if ev_count == 0 else "[DRY-RUN]  "
                print(f"{tag}  task {task_id} → {json_key} ({ev_count}건)")
                processed_label_keys.add(json_key)
                if ev_count == 0:
                    empty_save += 1
                else:
                    updated += 1
            else:
                try:
                    deleted, inserted = upsert_video_labels(
                        dsn,
                        bucket,
                        json_key,
                        asset_id,
                        new_events,
                        conn=db_conn,
                    )
                except FinalizedLabelsSkip:
                    print(f"[SKIP-FIN] task {task_id} → {json_key} finalized, MinIO overwrite 방지")
                    skipped_finalized += 1
                    continue
                except ConcurrentLabelsRace:
                    # (labels_key, event_index) UNIQUE 위반 — 동시 webhook 또는 retry.
                    # log + skip — 다음 sync tick 이 재처리. MinIO 도 안 건드림.
                    print(f"[SKIP-RACE] task {task_id} → {json_key} concurrent race, 다음 tick 재처리")
                    error += 1
                    continue
                write_json(minio, bucket, json_key, new_events)
                print(f"[UPDATED]  task {task_id} → {json_key} (MinIO {ev_count}건, DB del={deleted} ins={inserted})")
                processed_label_keys.add(json_key)
                if ev_count == 0:
                    empty_save += 1
                else:
                    updated += 1

        except Exception as exc:
            print(f"[ERROR]    task {task_id}: {exc}")
            error += 1

    if db_conn is not None:
        try:
            db_conn.close()
        except Exception:
            pass

    suffix = " (dry-run)" if dry_run else ""
    print(
        f"\n[DONE] 업데이트 {updated} / 빈저장 {empty_save} / 스킵 {skipped} / "
        f"finalized_skip {skipped_finalized} / 미매칭 {no_match} / 오류 {error}{suffix}"
    )
    return {
        "processed_label_keys": sorted(processed_label_keys),
        "updated": updated,
        "empty_save": empty_save,
        "skipped": skipped,
        "skipped_finalized": skipped_finalized,
        "no_match": no_match,
        "error": error,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="LS annotation → MinIO JSON sync")
    parser.add_argument("--project", type=int, required=True)
    parser.add_argument("--ls-url", default=os.environ.get("LS_URL", DEFAULT_LS_URL))
    parser.add_argument("--api-key", default=os.environ.get("LS_API_KEY"))
    parser.add_argument("--fps", type=int, default=DEFAULT_FPS)
    parser.add_argument("--minio-endpoint", default=os.environ.get("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT))
    parser.add_argument("--minio-access-key", default=os.environ.get("MINIO_ACCESS_KEY", DEFAULT_MINIO_ACCESS_KEY))
    parser.add_argument("--minio-secret-key", default=os.environ.get("MINIO_SECRET_KEY", DEFAULT_MINIO_SECRET_KEY))
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default="", help="MinIO key prefix 필터")
    parser.add_argument("--dry-run", action="store_true", help="실제 업로드 없이 변경사항만 출력")
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATAOPS_POSTGRES_DSN"),
        required=not os.environ.get("DATAOPS_POSTGRES_DSN"),
        help="PostgreSQL DSN (DATAOPS_POSTGRES_DSN 환경변수로도 설정 가능)",
    )
    args = parser.parse_args()

    if not args.api_key:
        parser.error("--api-key 또는 LS_API_KEY 환경변수가 필요합니다.")
    if not args.dsn:
        parser.error("--dsn 또는 DATAOPS_POSTGRES_DSN 환경변수가 필요합니다.")

    _MINIO_DEFAULTS = {"minioadmin", "admin", ""}
    # ls_webhook.py / config.py / minio_cross_sync.py 와 동일 escape-hatch: 자격 회전 전까지
    # ALLOW_INSECURE_DEFAULT_CREDS=1 면 fail-fast 대신 경고로 강등 (1f3309f 가 이 연결을 누락).
    _insecure_ok = os.environ.get("ALLOW_INSECURE_DEFAULT_CREDS", "").strip().lower() in {"1", "true", "yes"}
    if not args.minio_access_key or args.minio_access_key in _MINIO_DEFAULTS:
        if not _insecure_ok:
            parser.error(
                "--minio-access-key 또는 MINIO_ACCESS_KEY 에 유효한 자격증명이 필요합니다. (임시 우회: ALLOW_INSECURE_DEFAULT_CREDS=1)"
            )
        print("WARNING: 기본 MinIO 자격으로 진행 (ALLOW_INSECURE_DEFAULT_CREDS=1) — 자격 회전 후 해제 권장.")
    if not args.minio_secret_key or args.minio_secret_key in _MINIO_DEFAULTS:
        if not _insecure_ok:
            parser.error(
                "--minio-secret-key 또는 MINIO_SECRET_KEY 에 유효한 자격증명이 필요합니다. (임시 우회: ALLOW_INSECURE_DEFAULT_CREDS=1)"
            )
        print("WARNING: 기본 MinIO 자격으로 진행 (ALLOW_INSECURE_DEFAULT_CREDS=1) — 자격 회전 후 해제 권장.")

    run(
        project_id=args.project,
        ls_url=args.ls_url.rstrip("/"),
        api_key=args.api_key,
        fps=args.fps,
        minio_endpoint=args.minio_endpoint,
        minio_access_key=args.minio_access_key,
        minio_secret_key=args.minio_secret_key,
        bucket=args.bucket,
        prefix=args.prefix,
        dry_run=args.dry_run,
        dsn=args.dsn,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
