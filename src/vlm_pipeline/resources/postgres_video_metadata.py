"""PG video_metadata CRUD 및 프레임 추출 쿼리 mixin (DuckDBVideoMetadataMixin 1:1 포팅).

변환 규칙:
  - placeholder ``?`` → ``%s``
  - ``conn.execute(sql, [...]).fetchall()`` → cursor 패턴
  - DuckDB BOOLEAN 비교는 그대로 (PG strict bool — Python ``True/False`` 그대로 전달)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


class PostgresVideoMetadataMixin:
    """video_metadata insert/update 및 프레임 후보 쿼리."""

    def insert_video_metadata(self, asset_id: str, meta: dict) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO video_metadata (
                        asset_id, width, height, duration_sec, fps,
                        codec, bitrate, frame_count, has_audio,
                        environment_type, daynight_type, outdoor_score,
                        avg_brightness, env_method,
                        camera_angle, subject_scale, occlusion_state, weather, angle_method,
                        extracted_at,
                        frame_extract_status, frame_extract_count,
                        frame_extract_error, frame_extracted_at,
                        original_codec, original_profile, original_has_b_frames,
                        original_level_int, reencode_required, reencode_reason,
                        reencode_applied, reencode_preset
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        asset_id,
                        meta.get("width"),
                        meta.get("height"),
                        meta.get("duration_sec"),
                        meta.get("fps"),
                        meta.get("codec"),
                        meta.get("bitrate"),
                        meta.get("frame_count"),
                        meta.get("has_audio", False),
                        meta.get("environment_type"),
                        meta.get("daynight_type"),
                        meta.get("outdoor_score"),
                        meta.get("avg_brightness"),
                        meta.get("env_method"),
                        meta.get("camera_angle"),
                        meta.get("subject_scale"),
                        meta.get("occlusion_state"),
                        meta.get("weather"),
                        # ingest 시 기본 'deferred' — video_scene_backfill 큐 등록 메커니즘
                        # (env_method 와 동일 규약, design §3.2).
                        meta.get("angle_method", "deferred"),
                        meta.get("extracted_at", datetime.now()),
                        meta.get("frame_extract_status", "pending"),
                        meta.get("frame_extract_count", 0),
                        meta.get("frame_extract_error"),
                        meta.get("frame_extracted_at"),
                        meta.get("original_codec"),
                        meta.get("original_profile"),
                        meta.get("original_has_b_frames", False),
                        meta.get("original_level_int"),
                        meta.get("reencode_required", False),
                        meta.get("reencode_reason"),
                        meta.get("reencode_applied", False),
                        meta.get("reencode_preset"),
                    ),
                )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE video_metadata SET
                        original_codec        = %s,
                        original_profile      = %s,
                        original_has_b_frames = %s,
                        original_level_int    = %s,
                        reencode_required     = %s,
                        reencode_reason       = %s
                    WHERE asset_id = %s
                    """,
                    (
                        meta.get("original_codec"),
                        meta.get("original_profile"),
                        meta.get("original_has_b_frames", False),
                        meta.get("original_level_int"),
                        meta.get("reencode_required", False),
                        meta.get("reencode_reason"),
                        asset_id,
                    ),
                )

    def update_video_reencode_applied(
        self,
        asset_id: str,
        *,
        codec: str = "h264",
        reencode_preset: str = "standard",
    ) -> None:
        normalized_id = self._norm_str(asset_id)
        if not normalized_id:
            return
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE video_metadata
                    SET codec            = %s,
                        reencode_applied = TRUE,
                        reencode_preset  = %s
                    WHERE asset_id = %s
                    """,
                    (codec, reencode_preset, normalized_id),
                )

    def update_video_reencode_reason(self, asset_id: str, reason: str) -> None:
        """reencode_reason 컬럼만 갱신 (fallback 기록용)."""
        normalized_id = self._norm_str(asset_id)
        if not normalized_id:
            return
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE video_metadata SET reencode_reason = %s WHERE asset_id = %s",
                    (str(reason)[:200], normalized_id),
                )

    def update_video_frame_extract_status(
        self,
        asset_id: str,
        status: str,
        *,
        frame_count: int | None = None,
        error_message: str | None = None,
        extracted_at: datetime | None = None,
    ) -> None:
        normalized_id = self._norm_str(asset_id)
        if not normalized_id:
            return

        frame_count_value = frame_count if frame_count is not None else 0
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE video_metadata
                    SET frame_extract_status = %s,
                        frame_extract_count = %s,
                        frame_extract_error = %s,
                        frame_extracted_at = %s,
                        extracted_at = COALESCE(extracted_at, %s)
                    WHERE asset_id = %s
                    """,
                    (
                        status,
                        frame_count_value,
                        error_message,
                        extracted_at,
                        datetime.now(),
                        normalized_id,
                    ),
                )

    def find_deferred_env_videos(self, limit: int = 1000) -> list[dict[str, Any]]:
        """env_method='deferred' 이고 frame_extract_count>0 인 video 목록 반환.

        archive_path IS NOT NULL 조건으로 파일 위치를 확인할 수 있는 레코드만 포함.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT vm.asset_id, rf.archive_path, vm.duration_sec
                    FROM video_metadata vm
                    JOIN raw_files rf ON rf.asset_id = vm.asset_id
                    WHERE vm.env_method = 'deferred'
                      AND COALESCE(vm.frame_extract_count, 0) > 0
                      AND rf.archive_path IS NOT NULL
                    ORDER BY vm.asset_id
                    LIMIT %(limit)s
                    """,
                    {"limit": max(1, int(limit))},
                )
                rows = cur.fetchall()
        return self._rows_to_dicts(rows, ["asset_id", "archive_path", "duration_sec"])

    def update_video_env(
        self,
        asset_id: str,
        *,
        environment_type: str | None,
        daynight_type: str | None,
        outdoor_score: float | None,
        avg_brightness: float | None,
        env_method: str | None,
    ) -> None:
        """video_metadata 의 환경 분류 컬럼만 갱신."""
        normalized_id = self._norm_str(asset_id)
        if not normalized_id:
            return
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE video_metadata
                    SET environment_type = %s,
                        daynight_type    = %s,
                        outdoor_score    = %s,
                        avg_brightness   = %s,
                        env_method       = %s
                    WHERE asset_id = %s
                    """,
                    (environment_type, daynight_type, outdoor_score, avg_brightness, env_method, normalized_id),
                )

    def find_deferred_scene_videos(self, limit: int = 1000) -> list[dict[str, Any]]:
        """angle_method='deferred' 이거나 env_method='deferred' 이고 archive_path 를 아는
        video 목록 반환.

        video_scene_backfill 은 camera_angle 계열과 environment_type 계열을 한 번의
        Qwen 호출로 함께 채우므로(design §3.2), 둘 중 하나만 미완료여도 대상에 포함한다.
        find_deferred_env_videos 와 달리 frame_extract_count>0 조건이 없다 — 씬 분류는
        (프레임 추출 stage 산출물이 아니라) 원본에서 ffmpeg 로 직접 프레임을 뽑으므로
        frame_extract stage 완료 여부에 의존하지 않는다.

        camera_angle/subject_scale/occlusion_state/environment_type/daynight_type/weather/
        env_method 의 **현재 값**도 함께 반환한다 — 둘 중 하나만 'deferred' 인 행(예:
        레거시 env_method='places365_cuda' 행은 이미 채워져 있고 angle_method 만
        'deferred')이 archive 유실 등으로 SKIP 될 때, 이미 채워져 있던 쪽을 None 으로
        되돌리지 않고 그대로 보존하기 위해 호출자(scene_backfill_helpers)가 필요로 한다.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT vm.asset_id, rf.archive_path, vm.duration_sec,
                           vm.camera_angle, vm.subject_scale, vm.occlusion_state,
                           vm.environment_type, vm.daynight_type, vm.weather,
                           vm.env_method, vm.angle_method
                    FROM video_metadata vm
                    JOIN raw_files rf ON rf.asset_id = vm.asset_id
                    WHERE (vm.angle_method = 'deferred' OR vm.env_method = 'deferred')
                      AND rf.archive_path IS NOT NULL
                    ORDER BY vm.asset_id
                    LIMIT %(limit)s
                    """,
                    {"limit": max(1, int(limit))},
                )
                rows = cur.fetchall()
        return self._rows_to_dicts(
            rows,
            [
                "asset_id",
                "archive_path",
                "duration_sec",
                "camera_angle",
                "subject_scale",
                "occlusion_state",
                "environment_type",
                "daynight_type",
                "weather",
                "env_method",
                "angle_method",
            ],
        )

    def count_deferred_scene_videos(self) -> int:
        """find_deferred_scene_videos 와 동일 조건의 레코드 수 반환."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM video_metadata vm
                    JOIN raw_files rf ON rf.asset_id = vm.asset_id
                    WHERE (vm.angle_method = 'deferred' OR vm.env_method = 'deferred')
                      AND rf.archive_path IS NOT NULL
                    """
                )
                row = cur.fetchone()
        return int(row[0]) if row else 0

    def update_video_scene(
        self,
        asset_id: str,
        *,
        camera_angle: str | None,
        subject_scale: str | None,
        occlusion_state: str | None,
        environment_type: str | None,
        daynight_type: str | None,
        weather: str | None,
        env_method: str | None,
        angle_method: str | None,
    ) -> None:
        """video_metadata 의 씬 6축 분류 컬럼 + provenance(env_method/angle_method) 를 한 번에 갱신.

        Places365 전용 update_video_env 와 별도로 유지한다 — Places365 는 삭제가 아니라
        일시정지 상태이므로 env_backfill 이 계속 그 메서드를 쓴다(design §7).
        """
        normalized_id = self._norm_str(asset_id)
        if not normalized_id:
            return
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE video_metadata
                    SET camera_angle     = %s,
                        subject_scale    = %s,
                        occlusion_state  = %s,
                        environment_type = %s,
                        daynight_type    = %s,
                        weather          = %s,
                        env_method       = %s,
                        angle_method     = %s
                    WHERE asset_id = %s
                    """,
                    (
                        camera_angle,
                        subject_scale,
                        occlusion_state,
                        environment_type,
                        daynight_type,
                        weather,
                        env_method,
                        angle_method,
                        normalized_id,
                    ),
                )

    def find_raw_video_extract_pending(self, limit: int = 500, folder_name: str | None = None) -> list[dict[str, Any]]:
        """라벨(event) 없이 처리할 raw video 후보를 반환."""
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
                        vm.duration_sec,
                        vm.fps,
                        vm.frame_count
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND COALESCE(vm.frame_extract_status, 'pending') = 'pending'
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
                "duration_sec",
                "fps",
                "frame_count",
            ]
            return self._rows_to_dicts(rows, columns)
