"""Phase 4-B (#12) — raw_files ingest_status lifecycle integration test.

`raw_files` row 가 ``pending → completed`` 로 흐르는 핵심 경로를 검증.
PostgresIngestRawMixin 의 insert + update + lookup 조합이 실제 PG 상에서
의도된 결과를 만드는지 (in-memory mock 으로는 안 보이는 SQL 의미론
회귀를 catch).

DuckDB→Postgres cutover (2026-05-19) 이후 마이그레이션 회귀를 CI 가 자동으로
잡지 못하던 문제를 해결하는 첫 데이터-경로 회귀 테스트.
"""

from __future__ import annotations

import uuid
from datetime import datetime


def _insert_pending_video(pg_resource, *, source_unit: str = "lifecycle_unit") -> str:
    asset_id = f"asset-{uuid.uuid4().hex[:8]}"
    pg_resource.insert_raw_files_batch(
        [
            {
                "asset_id": asset_id,
                "source_path": f"/incoming/{source_unit}/{asset_id}.mp4",
                "original_name": f"{asset_id}.mp4",
                "checksum": f"chk-{asset_id}",
                "media_type": "video",
                "ingest_status": "pending",
                "raw_key": f"{source_unit}/{asset_id}.mp4",
                "source_unit_name": source_unit,
            }
        ]
    )
    return asset_id


def _ingest_status_for(pg_resource, asset_id: str) -> tuple[str, str | None]:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ingest_status, archive_path FROM raw_files WHERE asset_id = %s",
                (asset_id,),
            )
            row = cur.fetchone()
    assert row is not None, f"raw_files row not found for {asset_id}"
    return row[0], row[1]


def test_pending_row_lookup_skips_completed_only(pg_resource) -> None:
    """pending 상태 row 는 find_by_checksum(completed_only=True) 결과에서 제외."""
    asset_id = _insert_pending_video(pg_resource)

    hit_any = pg_resource.find_any_by_checksum(f"chk-{asset_id}")
    hit_completed = pg_resource.find_by_checksum(f"chk-{asset_id}", completed_only=True)

    assert hit_any is not None and hit_any["asset_id"] == asset_id
    assert hit_completed is None, "pending row 가 completed_only 검색에서 잡힘"


def test_update_to_completed_with_archive_path(pg_resource) -> None:
    """update_raw_file_status('completed', archive_path=...) 후 lookup 동작."""
    asset_id = _insert_pending_video(pg_resource)

    archive_path = f"/archive/lifecycle_unit/{asset_id}.mp4"
    pg_resource.update_raw_file_status(
        asset_id,
        "completed",
        error_message=None,
        archive_path=archive_path,
    )

    status, archive = _ingest_status_for(pg_resource, asset_id)
    assert status == "completed"
    assert archive == archive_path

    # completed_only=True 로 검색하면 이제 잡힌다.
    hit = pg_resource.find_by_checksum(f"chk-{asset_id}", completed_only=True)
    assert hit is not None
    assert hit["asset_id"] == asset_id
    assert hit["archive_path"] == archive_path


def test_count_helpers_consistent_with_lifecycle(pg_resource) -> None:
    """Phase 3-B/C 도입한 count helper 들이 lifecycle 결과와 일치하는지.

    - count_completed_raw_files_without_archive: completed 인데 archive_path NULL
    - count_cross_table_inconsistencies['ingest_completed_no_metadata']:
        video 가 completed 인데 video_metadata 없음
    """
    asset_id = _insert_pending_video(pg_resource)
    # 1) completed + archive_path NULL → count_completed_raw_files_without_archive 1
    pg_resource.update_raw_file_status(asset_id, "completed", archive_path=None)
    no_archive = pg_resource.count_completed_raw_files_without_archive()
    assert no_archive == 1

    # 2) archive_path 채워진 뒤엔 카운트 0
    pg_resource.update_raw_file_status(asset_id, "completed", archive_path=f"/archive/x/{asset_id}.mp4")
    no_archive_after = pg_resource.count_completed_raw_files_without_archive()
    assert no_archive_after == 0

    # 3) video 가 completed 인데 video_metadata 없음 → cross-table inconsistency 1
    cross_counts = pg_resource.count_cross_table_inconsistencies()
    assert cross_counts["ingest_completed_no_metadata"] == 1
    assert cross_counts["timestamp_completed_no_label_key"] == 0


def test_insert_dataset_with_lineage_columns(pg_resource) -> None:
    """Phase 3-D 도입한 spec_hash/git_sha/build_started_at 채워서 INSERT 동작 확인."""
    dataset_id = f"ds-{uuid.uuid4().hex[:8]}"
    now = datetime.now()
    pg_resource.insert_dataset(
        {
            "dataset_id": dataset_id,
            "name": "integration_test",
            "version": "v1",
            "config": '{"k":"v"}',
            "dataset_bucket": "vlm-dataset",
            "dataset_prefix": "integration_test",
            "build_status": "building",
            "spec_hash": "0123456789abcdef",
            "git_sha": "abcd1234",
            "build_started_at": now,
        }
    )

    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT spec_hash, git_sha, build_started_at FROM datasets WHERE dataset_id = %s",
                (dataset_id,),
            )
            row = cur.fetchone()

    assert row is not None
    assert row[0] == "0123456789abcdef"
    assert row[1] == "abcd1234"
    # PG 가 microsecond 일부 round 할 수 있으니 second 단위 비교.
    assert row[2].replace(microsecond=0) == now.replace(microsecond=0)
