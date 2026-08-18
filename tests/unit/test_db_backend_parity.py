"""DuckDB ↔ PostgreSQL backend parity smoke.

``db_resource`` parametrized fixture 로 동일 시나리오를 양 백엔드에서 돌린다.
PG 백엔드가 환경에서 미설정/unreachable 이면 그 param 만 skip 되고 DuckDB 만 진행.

이 파일은 마이그레이션 작업 중 양쪽 결과가 어긋나면 즉시 잡아내는 canary 역할.
운영 시나리오 전체를 커버하지는 않고, mixin 별 핵심 round-trip 만 확인.

알려진 DuckDB era 한계 (xfail 처리):
  - ``test_image_metadata_and_label_upsert``: DuckDB ``INSERT OR REPLACE`` 가
    image_metadata 의 multi-UNIQUE 환경에서 conflict target 명시를 요구하며 실패.
  - ``test_delete_asset_for_reingest_cascade``: DuckDB 트랜잭션 내 FK check 가
    PG 와 다르게 동작 — 같은 트랜잭션 안에서 child 삭제 후에도 parent 삭제 시 위반.
PG 마이그레이션의 가치 중 하나가 이 두 케이스를 명세대로 처리한다는 것.
"""

from __future__ import annotations

import uuid

import pytest


def test_raw_files_round_trip(db_resource):
    """insert → find_by_checksum → update_phash → update_dup_group → count."""
    asset_id = f"asset-{uuid.uuid4().hex[:8]}"

    n = db_resource.insert_raw_files_batch(
        [
            {
                "asset_id": asset_id,
                "source_path": "/tmp/a.jpg",
                "checksum": "checksum-a",
                "media_type": "image",
                "ingest_status": "completed",
                "raw_key": "unit/a.jpg",
                "source_unit_name": "unit",
            }
        ]
    )
    assert n == 1

    hit = db_resource.find_by_checksum("checksum-a", completed_only=True)
    assert hit is not None
    assert hit["asset_id"] == asset_id

    db_resource.update_phash(asset_id, "deadbeef")
    db_resource.update_dup_group(asset_id, "group-x")

    assert db_resource.count_raw_files_for_source_unit_name("unit") == 1


def test_label_upsert_idempotent(db_resource):
    """insert_label 을 같은 label_id 로 두 번 호출해도 ON CONFLICT 처리.

    DuckDB era 의 ``INSERT OR REPLACE`` 와 동일한 의미를 PG era 의
    ``ON CONFLICT (label_id) DO UPDATE`` 가 보존하는지 확인.
    """
    asset_id = f"asset-{uuid.uuid4().hex[:8]}"
    db_resource.insert_raw_files_batch(
        [
            {
                "asset_id": asset_id,
                "source_path": "/tmp/b.mp4",
                "media_type": "video",
                "ingest_status": "completed",
            }
        ]
    )

    label_id = f"label-{uuid.uuid4().hex[:8]}"
    db_resource.insert_label(
        {
            "label_id": label_id,
            "asset_id": asset_id,
            "labels_key": "v1.json",
            "label_format": "gemini_event_json",
            "label_status": "completed",
        }
    )
    db_resource.insert_label(
        {
            "label_id": label_id,
            "asset_id": asset_id,
            "labels_key": "v2.json",  # 같은 PK, 새 값
            "label_format": "gemini_event_json",
            "label_status": "completed",
        }
    )


def _is_duckdb_resource(db_resource) -> bool:
    return type(db_resource).__name__ == "_DuckDBTestResource"


def test_image_metadata_and_label_upsert(db_resource):
    """upsert_image_metadata_rows 와 insert_image_label 의 ON CONFLICT 동작.

    DuckDB era 는 image_metadata 의 multi-UNIQUE 환경에서 ``INSERT OR REPLACE`` 가
    conflict target 명시 없이는 실패한다 — DuckDB native 한계.
    """
    if _is_duckdb_resource(db_resource):
        pytest.xfail("DuckDB INSERT OR REPLACE on image_metadata requires explicit conflict target")

    asset_id = f"asset-{uuid.uuid4().hex[:8]}"
    db_resource.insert_raw_files_batch(
        [
            {
                "asset_id": asset_id,
                "source_path": "/tmp/c.jpg",
                "media_type": "image",
                "ingest_status": "completed",
            }
        ]
    )

    image_id = f"img-{uuid.uuid4().hex[:8]}"
    n = db_resource.upsert_image_metadata_rows(
        [
            {
                "image_id": image_id,
                "source_asset_id": asset_id,
                "image_key": "unit/c.jpg",
                "image_role": "source_image",
                "image_bucket": "vlm-raw",
            }
        ]
    )
    assert n == 1

    # 같은 image_id 로 한 번 더 — ON CONFLICT (image_id) DO UPDATE
    n2 = db_resource.upsert_image_metadata_rows(
        [
            {
                "image_id": image_id,
                "source_asset_id": asset_id,
                "image_key": "unit/c.jpg",
                "image_role": "source_image",
                "image_bucket": "vlm-raw",
                "width": 640,
                "height": 480,
            }
        ]
    )
    assert n2 == 1

    image_label_id = f"il-{uuid.uuid4().hex[:8]}"
    db_resource.insert_image_label(
        {
            "image_label_id": image_label_id,
            "image_id": image_id,
            "labels_key": "v1.bbox.json",
            "label_format": "yolo_bbox",
            "label_tool": "yolo-world",
        }
    )
    db_resource.insert_image_label(
        {
            "image_label_id": image_label_id,
            "image_id": image_id,
            "labels_key": "v2.bbox.json",
            "label_format": "yolo_bbox",
            "label_tool": "yolo-world",
            "object_count": 5,
        }
    )


def test_delete_asset_for_reingest_cascade(db_resource):
    """raw_files 삭제 시 FK-ordered cascade 가 양 백엔드에서 동작.

    DuckDB 는 트랜잭션 안에서 child 를 먼저 삭제했더라도 parent 삭제 시
    FK 를 위반으로 본다 (DuckDB FK 한계). PG 는 명세대로 같은 트랜잭션 내 view 를
    반영해 정상 처리.
    """
    if _is_duckdb_resource(db_resource):
        pytest.xfail("DuckDB FK check inside transaction does not honor in-tx deletes")

    asset_id = f"asset-{uuid.uuid4().hex[:8]}"
    db_resource.insert_raw_files_batch(
        [
            {
                "asset_id": asset_id,
                "source_path": "/tmp/d.jpg",
                "checksum": "csum-d",
                "media_type": "image",
                "ingest_status": "completed",
            }
        ]
    )
    image_id = f"img-{uuid.uuid4().hex[:8]}"
    db_resource.insert_image_metadata(
        asset_id,
        {
            "image_id": image_id,
            "image_key": "unit/d.jpg",
            "image_role": "source_image",
        },
    )

    db_resource.delete_asset_for_reingest(asset_id)

    assert db_resource.find_by_checksum("csum-d", completed_only=False) is None
    assert db_resource.find_image_metadata_by_image_id(image_id) is None


def test_spec_mixin_empty_state(db_resource):
    """spec mixin — labeling_configs/labeling_specs/requester_config_map 가 비어 있어도
    안전하게 None/빈 리스트 반환."""
    assert db_resource.get_labeling_config("nonexistent") is None
    assert db_resource.get_labeling_spec_by_id("nonexistent") is None
    cid, scope = db_resource.resolve_config_for_requester("user-1", "team-1")
    assert cid is None and scope is None
