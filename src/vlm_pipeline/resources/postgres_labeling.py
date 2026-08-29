"""PG LABELING 도메인 — auto-label, clip image extract.

DuckDBLabelingMixin 1:1 포팅. 핵심 변환:
  - 윈도우 함수 (``ROW_NUMBER() OVER`` 등) PG 호환
  - 트랜잭션 명시 호출 → ``connect()`` ctxmgr 자동 관리

Detection 도메인 (image_labels CRUD + detection 대상 조회) 은
``postgres_detection.py`` 의 ``PostgresDetectionMixin`` 으로 분리.
"""

from __future__ import annotations

import json
import logging

from datetime import datetime
from collections.abc import Iterable
from typing import Any
from uuid import uuid4

from ..lib.checksum import sha256_bytes
from .postgres_detection import PostgresDetectionMixin

logger = logging.getLogger(__name__)


# ── generation_prompts (migration 018) ──
#
# dedup 키가 (prompt_type, model_name, content_hash) 인 이유: 한 run 의 N개 영상이 동일한
# rendered_prompt 를 공유하므로 dedup 없으면 행이 영상 수만큼 폭증한다 (018 파일 주석).
#
# ⚠️ spec_id 는 의도적으로 넣지 않는다. 018 의 spec_id 는 labeling_specs(spec_id) 를 참조하는데
#    그 테이블은 현재 0행이다 (실측) — dispatch tag 의 spec_id 를 그대로 넣으면 FK 위반으로
#    INSERT 가 실패한다. labeling_specs 에 생산자가 배선되면 그때 함께 채운다.
_GENERATION_PROMPT_UPSERT_SQL = """
    INSERT INTO generation_prompts (
        prompt_id, prompt_type, template_name, template_version, model_name,
        categories, category_descriptions, rendered_prompt, content_hash, dagster_run_id
    ) VALUES (
        %(prompt_id)s, %(prompt_type)s, %(template_name)s, %(template_version)s, %(model_name)s,
        %(categories)s::jsonb, %(category_descriptions)s::jsonb, %(rendered_prompt)s,
        %(content_hash)s, %(dagster_run_id)s
    )
    ON CONFLICT (prompt_type, model_name, content_hash) DO NOTHING
    RETURNING prompt_id
"""

_GENERATION_PROMPT_SELECT_SQL = """
    SELECT prompt_id FROM generation_prompts
    WHERE prompt_type = %(prompt_type)s
      AND model_name = %(model_name)s
      AND content_hash = %(content_hash)s
"""

_TIMESTAMP_PROMPT_POINTER_SQL = """
    UPDATE video_metadata SET timestamp_generation_prompt_id = %(prompt_id)s
    WHERE asset_id = %(asset_id)s
"""


def generation_prompt_content_hash(rendered_prompt: str) -> str:
    """dedup 키로 쓰는 sha256(rendered_prompt) hex."""
    return sha256_bytes(rendered_prompt.encode("utf-8"))


# ── observed_categories (migration 023) ──
#
# 정본 밖 카테고리의 판단 유예 원장. UPSERT 는 023 파일 주석의 형태를 그대로 쓴다 —
# **사람이 관리하는 status / mapped_to / notes 는 절대 갱신하지 않는다.** 자동 관측이 사람의
# 승격·거절 결정을 덮으면 원장의 의미가 없어진다.
#
# source_units 는 승격 게이트("서로 다른 unit 2곳 이상")의 분모다. 32개 상한에 닿아도
# observation_count 는 계속 증가한다.
_OBSERVED_CATEGORY_MAX_LENGTH = 200

_OBSERVED_CATEGORY_UPSERT_SQL = """
    INSERT INTO observed_categories AS oc (
        source, raw_value, observation_count, source_units, first_seen, last_seen
    ) VALUES (
        %(source)s,
        %(raw_value)s,
        1,
        CASE
            WHEN %(source_unit)s::TEXT ~ '[^[:space:]]'
            THEN ARRAY[%(source_unit)s]::TEXT[]
            ELSE '{}'::TEXT[]
        END,
        statement_timestamp(),
        statement_timestamp()
    )
    ON CONFLICT (source, raw_value) DO UPDATE
    SET observation_count = oc.observation_count + 1,
        source_units = CASE
            WHEN cardinality(EXCLUDED.source_units) = 0
              OR array_position(oc.source_units, (EXCLUDED.source_units)[1]) IS NOT NULL
              OR cardinality(oc.source_units) >= 32
            THEN oc.source_units
            ELSE array_append(oc.source_units, (EXCLUDED.source_units)[1])
        END,
        last_seen = GREATEST(oc.last_seen, EXCLUDED.last_seen)
    RETURNING oc.source, oc.raw_value
"""


def sanitize_observed_value(value: object) -> str | None:
    """관측 원장에 넣을 수 있는 형태로 검증한다. 부적격이면 None.

    저장 계약: **바깥 공백만 제거하고 그 외는 원문 그대로.** 대소문자·내부 공백·유니코드를
    건드리지 않는다 — 무엇이 들어왔는지가 이 원장의 존재 이유다.
    200자 초과는 truncate 하지 않고 제외한다. truncate 는 서로 다른 malformed 출력을 같은
    prefix 로 접어버려 원장의 목적을 정면으로 깨뜨린다 (카테고리 라벨 실측 최장 31바이트).
    """
    rendered = str(value or "").strip()
    if not rendered or len(rendered) > _OBSERVED_CATEGORY_MAX_LENGTH:
        return None
    return rendered


class PostgresLabelingMixin(PostgresDetectionMixin):
    """Gemini auto-labeling / clip image extraction / image_labels 관련 메서드."""

    # ── auto-label (video_metadata) ──

    def find_auto_label_pending_videos(self, limit: int = 50, folder_name: str | None = None) -> list[dict]:
        with self.connect() as conn:
            query_cond, folder_params = self._folder_filter(folder_name)
            params: list[Any] = folder_params + [max(1, int(limit))]
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        r.asset_id,
                        r.raw_bucket,
                        r.raw_key,
                        r.archive_path,
                        r.source_path,
                        vm.duration_sec,
                        vm.fps,
                        vm.frame_count
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND COALESCE(vm.auto_label_status, 'pending') = 'pending'
                      -- routed/dispatch 흐름이 이미 처리(또는 실패)한 비디오는 제외.
                      -- routed 는 timestamp_status 만 세팅하고 auto_label_status 는
                      -- NULL 로 두므로, 이 가드가 없으면 MVP backstop 이 dispatch 로
                      -- 라벨된 비디오를 Gemini 재호출하여 카테고리필터 라벨을 clobber 한다.
                      -- (auto_labeling_sensor gemini_pending CTE 와 반드시 동일 조건 유지)
                      AND COALESCE(vm.timestamp_status, 'pending') = 'pending'
                      {query_cond}
                    ORDER BY r.created_at
                    LIMIT %s
                    """,
                    params,
                )
                rows = cur.fetchall()
            columns = [
                "asset_id",
                "raw_bucket",
                "raw_key",
                "archive_path",
                "source_path",
                "duration_sec",
                "fps",
                "frame_count",
            ]
            return self._rows_to_dicts(rows, columns)

    _VALID_STAGES = frozenset({"timestamp", "caption", "frame", "bbox", "auto_label"})

    def _update_video_metadata_stage_status(
        self,
        asset_id: str,
        *,
        stage: str,
        status: str,
        error: str | None = None,
        label_key: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        if stage not in self._VALID_STAGES:
            raise ValueError(f"Invalid stage {stage!r}; must be one of {sorted(self._VALID_STAGES)}")
        # auto_label 은 legacy 컬럼명 ({stage}_label_key/{stage}_completed_at 패턴 미준수) 사용.
        if stage == "auto_label":
            label_key_col, completed_at_col = "auto_label_key", "auto_labeled_at"
        else:
            label_key_col, completed_at_col = f"{stage}_label_key", f"{stage}_completed_at"
        label_key_clause = f", {label_key_col} = COALESCE(%s, {label_key_col})" if label_key is not None else ""
        sql = (
            f"UPDATE video_metadata SET {stage}_status = %s, {stage}_error = %s"
            f"{label_key_clause}"
            f", {completed_at_col} = COALESCE(%s, {completed_at_col}) WHERE asset_id = %s"
        )
        params = [status, error]
        if label_key is not None:
            params.append(label_key)
        params.extend([completed_at, asset_id])
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

    def update_auto_label_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        label_key: str | None = None,
        labeled_at: datetime | None = None,
    ) -> None:
        self._update_video_metadata_stage_status(
            asset_id,
            stage="auto_label",
            status=status,
            error=error,
            label_key=label_key,
            completed_at=labeled_at,
        )

    def update_timestamp_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        label_key: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        self._update_video_metadata_stage_status(
            asset_id, stage="timestamp", status=status, error=error, label_key=label_key, completed_at=completed_at
        )

    # ── generation_prompts write 경로 (migration 018 Phase 1) ──

    def upsert_generation_prompt(
        self,
        *,
        prompt_type: str,
        template_name: str,
        rendered_prompt: str,
        model_name: str,
        template_version: str | None = None,
        categories: list[str] | None = None,
        category_descriptions: dict[str, str] | None = None,
        dagster_run_id: str | None = None,
    ) -> str:
        """rendered_prompt 를 dedup 저장하고 prompt_id 를 반환한다.

        같은 (prompt_type, model_name, content_hash) 가 이미 있으면 기존 prompt_id 를 재사용한다.
        예외는 삼키지 않는다 — 호출부(asset)가 라벨링 중단 여부를 결정한다.
        """
        params = {
            "prompt_id": str(uuid4()),
            "prompt_type": prompt_type,
            "template_name": template_name,
            "template_version": template_version,
            "model_name": model_name,
            "categories": json.dumps(categories, ensure_ascii=False) if categories else None,
            "category_descriptions": (
                json.dumps(category_descriptions, ensure_ascii=False) if category_descriptions else None
            ),
            "rendered_prompt": rendered_prompt,
            "content_hash": generation_prompt_content_hash(rendered_prompt),
            "dagster_run_id": dagster_run_id,
        }
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_GENERATION_PROMPT_UPSERT_SQL, params)
                row = cur.fetchone()
                if row is not None:
                    return str(row[0])
                # DO NOTHING → 이미 존재. 기존 행의 prompt_id 를 읽어 재사용한다.
                cur.execute(_GENERATION_PROMPT_SELECT_SQL, params)
                existing = cur.fetchone()
                if existing is None:  # pragma: no cover - dedup 키가 있는데 조회 실패는 불가
                    raise RuntimeError("generation_prompts upsert 후 dedup 행을 찾지 못했습니다")
                return str(existing[0])

    def record_observed_categories(
        self,
        source: str,
        values: Iterable[object],
        source_unit: str | None = None,
    ) -> int:
        """정본 밖 카테고리를 관측 원장에 upsert 한다. 기록된 값의 개수를 반환.

        부적격 값(공백만 / 200자 초과)은 WARNING 후 skip 한다 — 관측은 부수 경로이므로 한 값이
        이상해도 나머지는 남겨야 한다. 예외는 삼키지 않는다(호출부가 fail-soft 를 결정).
        """
        recorded = 0
        pending: list[str] = []
        for value in values:
            sanitized = sanitize_observed_value(value)
            if sanitized is None:
                logger.warning(
                    "observed_categories skip: source=%s length=%d preview=%r",
                    source,
                    len(str(value or "").strip()),
                    str(value or "")[:60],
                )
                continue
            pending.append(sanitized)
        if not pending:
            return 0
        with self.connect() as conn:
            with conn.cursor() as cur:
                for raw_value in pending:
                    cur.execute(
                        _OBSERVED_CATEGORY_UPSERT_SQL,
                        {"source": source, "raw_value": raw_value, "source_unit": source_unit},
                    )
                    recorded += 1
        return recorded

    def set_timestamp_generation_prompt(self, asset_id: str, prompt_id: str) -> None:
        """video_metadata 에 '이 asset 의 라벨을 만든 프롬프트' 포인터를 남긴다.

        labels 가 아니라 video_metadata 에 두는 이유(018 설계): labels 는 재생성/LS 검수 시
        labels_key 기준으로 전량 DELETE 후 재INSERT 되므로, labels 에 링크를 두면 사람이
        수정할 때마다 계보가 함께 지워진다.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_TIMESTAMP_PROMPT_POINTER_SQL, {"prompt_id": prompt_id, "asset_id": asset_id})

    def update_caption_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        self._update_video_metadata_stage_status(
            asset_id, stage="caption", status=status, error=error, completed_at=completed_at
        )

    def update_frame_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        self._update_video_metadata_stage_status(
            asset_id, stage="frame", status=status, error=error, completed_at=completed_at
        )

    def update_bbox_status(
        self,
        asset_id: str,
        status: str,
        *,
        error: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        self._update_video_metadata_stage_status(
            asset_id, stage="bbox", status=status, error=error, completed_at=completed_at
        )

    def find_ready_for_labeling_timestamp_backlog(self, spec_id: str, limit: int = 50) -> list[dict]:
        """Staging spec flow: ready_for_labeling + spec_id, timestamp 미완료 비디오."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.asset_id, r.raw_bucket, r.raw_key, r.archive_path, r.source_path,
                        vm.duration_sec, vm.fps, vm.frame_count
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'ready_for_labeling'
                      AND r.spec_id = %s
                      -- 'failed' 포함 → 이전 spec run 에서 Gemini 실패(transient
                      -- rate-limit/5xx)한 비디오를 재-spec-run 시 재시도. 무한루프
                      -- 아님: 이 쿼리는 clip_timestamp_routed_impl(dispatch/spec run)
                      -- 에서만 호출되고 항상-on 센서가 자동 재발화하지 않는다.
                      AND COALESCE(vm.timestamp_status, 'pending') IN ('pending', 'failed')
                    ORDER BY r.created_at
                    LIMIT %s
                    """,
                    (spec_id, max(1, int(limit))),
                )
                rows = cur.fetchall()
            columns = [
                "asset_id",
                "raw_bucket",
                "raw_key",
                "archive_path",
                "source_path",
                "duration_sec",
                "fps",
                "frame_count",
            ]
            return self._rows_to_dicts(rows, columns)

    def find_timestamp_pending_by_folder(self, folder_name: str, limit: int = 50) -> list[dict]:
        """Dispatch flow: folder_name(source_unit_name) 기준 timestamp 미완료 비디오."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.asset_id, r.raw_bucket, r.raw_key, r.archive_path, r.source_path,
                        vm.duration_sec, vm.fps, vm.frame_count
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND r.source_unit_name = %s
                      -- 'failed' 포함 → 이전 dispatch 에서 실패한 비디오를 폴더
                      -- 재-dispatch 시 재시도 (operator 액션으로 bound, 자동루프 아님).
                      AND COALESCE(vm.timestamp_status, 'pending') IN ('pending', 'failed')
                    ORDER BY r.created_at
                    LIMIT %s
                    """,
                    (folder_name, max(1, int(limit))),
                )
                rows = cur.fetchall()
            columns = [
                "asset_id",
                "raw_bucket",
                "raw_key",
                "archive_path",
                "source_path",
                "duration_sec",
                "fps",
                "frame_count",
            ]
            return self._rows_to_dicts(rows, columns)

    def find_ready_for_labeling_caption_backlog(self, spec_id: str, limit: int = 100) -> list[dict]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.asset_id,
                        r.raw_bucket,
                        r.raw_key,
                        vm.timestamp_label_key,
                        vm.duration_sec
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'ready_for_labeling'
                      AND r.spec_id = %s
                      AND COALESCE(vm.timestamp_status, 'pending') = 'completed'
                      AND COALESCE(vm.caption_status, 'pending') = 'pending'
                      AND COALESCE(vm.timestamp_label_key, '') <> ''
                    ORDER BY COALESCE(vm.timestamp_completed_at, r.created_at), r.asset_id
                    LIMIT %s
                    """,
                    (spec_id, max(1, int(limit))),
                )
                rows = cur.fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "timestamp_label_key", "duration_sec"]
            return self._rows_to_dicts(rows, columns)

    def find_caption_pending_by_folder(self, folder_name: str, limit: int = 100) -> list[dict]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.asset_id,
                        r.raw_bucket,
                        r.raw_key,
                        vm.timestamp_label_key,
                        vm.duration_sec
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND r.source_unit_name = %s
                      AND COALESCE(vm.timestamp_status, 'pending') = 'completed'
                      AND COALESCE(vm.caption_status, 'pending') = 'pending'
                      AND COALESCE(vm.timestamp_label_key, '') <> ''
                    ORDER BY COALESCE(vm.timestamp_completed_at, r.created_at), r.asset_id
                    LIMIT %s
                    """,
                    (folder_name, max(1, int(limit))),
                )
                rows = cur.fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "timestamp_label_key", "duration_sec"]
            return self._rows_to_dicts(rows, columns)

    def find_captioning_pending_videos(self, limit: int = 100, folder_name: str | None = None) -> list[dict]:
        """Gemini JSON 생성 완료(generated) 후 아직 DB 정규화가 안 된 video."""
        with self.connect() as conn:
            query_cond, folder_params = self._folder_filter(folder_name)
            params: list[Any] = folder_params + [max(1, int(limit))]

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        r.asset_id,
                        r.raw_bucket,
                        r.raw_key,
                        vm.auto_label_key,
                        vm.duration_sec
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND vm.auto_label_status = 'generated'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM labels l
                          WHERE l.asset_id = r.asset_id
                            AND l.label_tool = 'gemini'
                            AND l.label_source = 'auto'
                            AND COALESCE(l.labels_key, '') = COALESCE(vm.auto_label_key, '')
                      )
                      {query_cond}
                    ORDER BY vm.auto_labeled_at
                    LIMIT %s
                    """,
                    params,
                )
                rows = cur.fetchall()
            columns = ["asset_id", "raw_bucket", "raw_key", "auto_label_key", "duration_sec"]
            return self._rows_to_dicts(rows, columns)

    def replace_gemini_labels(
        self,
        asset_id: str,
        labels_key: str,
        rows: list[dict[str, Any]],
    ) -> int:
        normalized_asset_id = self._norm_str(asset_id)
        normalized_labels_key = self._norm_str(labels_key)
        if not normalized_asset_id or not normalized_labels_key:
            return 0

        payload_rows: list[tuple] = []
        for row in rows:
            payload_rows.append(
                (
                    row.get("label_id") or str(uuid4()),
                    normalized_asset_id,
                    row.get("labels_bucket", "vlm-labels"),
                    normalized_labels_key,
                    row.get("label_format", "gemini_event_json"),
                    row.get("label_tool", "gemini"),
                    row.get("label_source", "auto"),
                    row.get("review_status", "auto_generated"),
                    row.get("event_index", 0),
                    row.get("event_count"),
                    row.get("timestamp_start_sec"),
                    row.get("timestamp_end_sec"),
                    row.get("caption_text"),
                    row.get("object_count", 0),
                    row.get("label_status", "completed"),
                    row.get("created_at", datetime.now()),
                )
            )

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM labels
                    WHERE asset_id = %s
                      AND label_tool = 'gemini'
                      AND label_source = 'auto'
                      AND labels_key = %s
                    """,
                    (normalized_asset_id, normalized_labels_key),
                )
                if payload_rows:
                    cur.executemany(
                        """
                        INSERT INTO labels (
                            label_id, asset_id, labels_bucket, labels_key,
                            label_format, label_tool, label_source, review_status,
                            event_index, event_count, timestamp_start_sec, timestamp_end_sec,
                            caption_text, object_count, label_status, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        payload_rows,
                    )
        return len(payload_rows)

    # ── clip media meta (processed_clips) ──

    def update_processed_clip_media_meta(
        self,
        clip_id: str,
        duration_sec: float | None,
        fps: float | None,
        frame_count: int | None,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE processed_clips
                    SET duration_sec = %s,
                        fps = %s,
                        frame_count = %s
                    WHERE clip_id = %s
                    """,
                    (duration_sec, fps, frame_count, clip_id),
                )

    def update_processed_clip_status(self, clip_id: str, status: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE processed_clips
                    SET process_status = %s
                    WHERE clip_id = %s
                    """,
                    (status, clip_id),
                )

    def update_clip_image_extract_status(
        self,
        clip_id: str,
        status: str,
        *,
        count: int = 0,
        error: str | None = None,
        extracted_at: datetime | None = None,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE processed_clips
                    SET image_extract_status = %s,
                        image_extract_count = %s,
                        image_extract_error = %s,
                        image_extracted_at = %s
                    WHERE clip_id = %s
                    """,
                    (status, count, error, extracted_at, clip_id),
                )
