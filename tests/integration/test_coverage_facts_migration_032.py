"""032_coverage_facts — 사실·reference 계층의 스키마 계약을 실제 PostgreSQL 에서 검증한다.

설계 정본: docs/exec-plans/active/comfyui-local-genai-pipeline-plan.md §6.1 / §6.4.

``DATAOPS_TEST_POSTGRES_DSN`` 미설정/unreachable 이면 파일 전체 skip —
tests/integration/conftest.py 가 테스트마다 임시 DB 를 만들고 ``ensure_schema()`` 로
001~032 를 적용한 뒤 DROP 한다. 즉 아래 테스트는 fresh apply 경로를 매번 다시 탄다.

여기서 검증하는 것은 "테이블이 있다" 가 아니라 **행동 계약**이다:
  * 모델 파생 라벨이 coverage 사실로 들어올 수 없다 (자기학습 금지의 스키마 강제)
  * 정본 행 삭제가 인제스트를 깨지 않고 사실만 함께 정리된다 (CASCADE)
  * subject 당 context 가 정확히 1행이다 (NULL-safe partial UNIQUE)
  * reference 기본 상태가 "사용 불가" 다 (fail-closed)
  * 뷰가 context 미검증 행을 **숨기지 않는다** ("비율 0" 과 "관측 불가" 의 구분)
"""

from __future__ import annotations

import psycopg2
import pytest


# ─── fixtures ────────────────────────────────────────────────────────────────


def _seed_asset(cur, asset_id: str, *, source_type: str = "camera") -> None:
    cur.execute(
        """
        INSERT INTO raw_files (asset_id, source_path, media_type, source_type, checksum)
        VALUES (%s, %s, 'image', %s, %s)
        """,
        (asset_id, f"/nas/data/incoming/{asset_id}.jpg", source_type, f"sha-{asset_id}"),
    )


def _seed_image(cur, image_id: str, asset_id: str, frame_index: int = 0) -> None:
    cur.execute(
        """
        INSERT INTO image_metadata (image_id, source_asset_id, image_bucket, image_key, frame_index)
        VALUES (%s, %s, 'vlm-raw', %s, %s)
        """,
        (image_id, asset_id, f"unit/{image_id}.jpg", frame_index),
    )


@pytest.fixture
def seeded(pg_resource):
    """raw_files 1건 + image_metadata 2건. 정본 테이블 쪽 최소 fixture."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            _seed_asset(cur, "asset-1")
            _seed_image(cur, "img-1", "asset-1", 0)
            _seed_image(cur, "img-2", "asset-1", 1)
    return pg_resource


def _finalized_fact(cur, image_id: str = "img-1", *, canonical_class: str = "falldown", **over) -> None:
    payload = {
        "image_id": image_id,
        "canonical_class": canonical_class,
        "fact_source": "ls_bbox",
        "asset_id": "asset-1",
        "origin_kind": "real",
        "review_status": "finalized",
        "finalized_at": "2026-09-21 00:00:00+00",
    }
    payload.update(over)
    cols = ", ".join(payload)
    marks = ", ".join(["%s"] * len(payload))
    cur.execute(f"INSERT INTO coverage_unit_facts ({cols}) VALUES ({marks})", tuple(payload.values()))


def _context_fact(cur, **over) -> str:
    payload = {
        "subject_type": "asset",
        "asset_id": "asset-1",
        "environment_type": "outdoor",
        "daynight_type": "day",
        "context_source": "places365_cuda",
        "verification_status": "verified",
        "verified_axes": ["environment_type", "daynight_type"],
        "verified_at": "2026-09-21 00:00:00+00",
    }
    payload.update(over)
    cols = ", ".join(payload)
    marks = ", ".join(["%s"] * len(payload))
    cur.execute(
        f"INSERT INTO coverage_context_facts ({cols}) VALUES ({marks}) RETURNING context_fact_id",
        tuple(payload.values()),
    )
    return cur.fetchone()[0]


def _reference(cur, **over) -> str:
    payload = {
        "image_id": "img-2",
        "asset_id": "asset-1",
        "reference_key": "unit/img-2.jpg",
        "allowed_workflows": ["sdxl-inpaint-cctv-v1"],
        "safe_region_json": '{"polygon": [[0, 0], [1, 0], [1, 1]]}',
        "holdout_excluded": False,
        "status": "approved",
        "approved_by": "operator",
        "approved_at": "2026-09-21 00:00:00+00",
    }
    payload.update(over)
    cols = ", ".join(payload)
    marks = ", ".join(["%s"] * len(payload))
    cur.execute(
        f"INSERT INTO generation_reference_pool ({cols}) VALUES ({marks}) RETURNING reference_id",
        tuple(payload.values()),
    )
    return cur.fetchone()[0]


# ─── 1. 적용 자체 ─────────────────────────────────────────────────────────────


def test_032_objects_exist_after_ensure_schema(pg_resource):
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM _pg_migrations WHERE name = '032_coverage_facts.sql'")
            assert cur.fetchone() is not None, "032 가 _pg_migrations 에 기록되지 않았다"
            cur.execute(
                """
                SELECT to_regclass('public.coverage_unit_facts'),
                       to_regclass('public.coverage_context_facts'),
                       to_regclass('public.camera_registry'),
                       to_regclass('public.asset_camera_map'),
                       to_regclass('public.generation_reference_pool'),
                       to_regclass('public.v_eligible_coverage_units'),
                       to_regclass('public.v_generation_reference_candidates')
                """
            )
            assert all(cur.fetchone()), "032 가 만든 객체 중 누락이 있다"


def test_032_is_idempotent(pg_resource):
    """ensure_schema() 재호출이 행을 추가하거나 @ASSERT_AFTER 를 깨뜨리지 않는다."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM _pg_migrations")
            before = cur.fetchone()[0]

    pg_resource.ensure_schema()  # 부팅 시 assertion 재실행 경로

    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM _pg_migrations")
            assert cur.fetchone()[0] == before


def test_032_cascade_fks_reach_the_canonical_tables(pg_resource):
    """정본을 가리키는 FK 는 전부 ON DELETE CASCADE 여야 한다.

    RESTRICT/NO ACTION 이면 coverage 사실이 쌓인 순간부터 재적재 경로
    (``postgres_ingest_raw.py:270-274`` 의 image_metadata→video_metadata→raw_files DELETE)가
    FK 위반으로 깨진다. 파일의 @ASSERT_AFTER 와 같은 불변식을 테스트에서도 고정한다.
    """
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT conname, confdeltype
                  FROM pg_constraint
                 WHERE contype = 'f'
                   AND conrelid IN ('coverage_unit_facts'::regclass,
                                    'coverage_context_facts'::regclass,
                                    'generation_reference_pool'::regclass,
                                    'asset_camera_map'::regclass)
                   AND confrelid IN ('image_metadata'::regclass, 'raw_files'::regclass)
                """
            )
            rows = cur.fetchall()
    assert len(rows) >= 6, rows
    assert [r for r in rows if r[1] != "c"] == [], f"CASCADE 가 아닌 FK: {rows}"


# ─── 2. coverage_unit_facts 계약 ──────────────────────────────────────────────


def test_auto_generated_labels_cannot_become_coverage_facts(seeded):
    """모델 파생 라벨은 이 테이블에 **들어올 수 없다**.

    소비자 쿼리가 ``WHERE review_status='finalized'`` 를 빠뜨려도 오염되지 않게,
    설계서의 "auto_generated 를 부족분 계산에 쓰지 않는다" 를 CHECK 로 강제한다.
    """
    with pytest.raises(psycopg2.errors.CheckViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                _finalized_fact(cur, review_status="auto_generated", finalized_at=None)


def test_finalized_fact_requires_a_finalized_timestamp(seeded):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                _finalized_fact(cur, finalized_at=None)


def test_unknown_canonical_class_is_refused(seeded):
    """canonical_class 는 label_classes 정본 투영(022/026)에만 있는 값이어야 한다."""
    with pytest.raises(psycopg2.errors.ForeignKeyViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                _finalized_fact(cur, canonical_class="not_a_real_class")


def test_grain_is_image_class_factsource(seeded):
    """같은 이미지·같은 클래스라도 fact_source 가 다르면 별개 행, 같으면 중복이다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _finalized_fact(cur, fact_source="ls_bbox")
            _finalized_fact(cur, fact_source="ls_image_event")
            cur.execute("SELECT count(*) FROM coverage_unit_facts")
            assert cur.fetchone()[0] == 2

    with pytest.raises(psycopg2.errors.UniqueViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                _finalized_fact(cur, fact_source="ls_bbox")


def test_deleting_the_image_cascades_the_fact(seeded):
    """정본이 사라지면 그 이미지에 대한 coverage 사실도 함께 사라진다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _finalized_fact(cur)
            cur.execute("DELETE FROM image_metadata WHERE image_id = 'img-1'")
            cur.execute("SELECT count(*) FROM coverage_unit_facts")
            assert cur.fetchone()[0] == 0


def test_the_real_reingest_delete_order_still_works(seeded):
    """``postgres_ingest_raw.py`` 재적재 경로를 그대로 재현한다 — 여기서 막히면 인제스트가 죽는다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _finalized_fact(cur)
            _context_fact(cur)
            _reference(cur)
            cur.execute("DELETE FROM image_metadata WHERE source_asset_id = 'asset-1'")
            cur.execute("DELETE FROM video_metadata WHERE asset_id = 'asset-1'")
            cur.execute("DELETE FROM labels WHERE asset_id = 'asset-1'")
            cur.execute("DELETE FROM raw_files WHERE asset_id = 'asset-1'")
            cur.execute(
                """
                SELECT (SELECT count(*) FROM coverage_unit_facts),
                       (SELECT count(*) FROM coverage_context_facts),
                       (SELECT count(*) FROM generation_reference_pool)
                """
            )
            assert cur.fetchone() == (0, 0, 0)


def test_asset_id_matches_the_canonical_parent(seeded):
    """스키마가 아니라 **이 테스트가** 지키는 불변식.

    ``coverage_unit_facts.asset_id`` 는 ``image_metadata.source_asset_id`` 와 같아야 한다.
    복합 FK 로 강제하려면 598k 행 정본 테이블에 UNIQUE 인덱스를 추가해야 해서 스키마로는
    막지 않았다 — 그래서 여기서 드리프트를 잡는다. 투영 job 이 생기면 같은 쿼리를 재사용할 것.
    """
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _seed_asset(cur, "asset-2")
            _finalized_fact(cur, asset_id="asset-2")  # 일부러 부모와 다른 asset
            cur.execute(
                """
                SELECT count(*)
                  FROM coverage_unit_facts f
                  JOIN image_metadata im ON im.image_id = f.image_id
                 WHERE im.source_asset_id IS DISTINCT FROM f.asset_id
                """
            )
            assert cur.fetchone()[0] == 1, "정합성 쿼리 자체가 드리프트를 못 잡는다"


# ─── 3. coverage_context_facts 계약 ───────────────────────────────────────────


def test_one_context_version_per_subject(seeded):
    """subject 당 1행. NULL 이 섞인 단일 UNIQUE 로는 못 막는 부분을 partial UNIQUE 가 막는다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _context_fact(cur, subject_type="asset", asset_id="asset-1")
            # 같은 asset 을 가리키는 image 단위 행은 **허용**된다 (더 정밀한 subject).
            _context_fact(cur, subject_type="image", asset_id=None, image_id="img-1")
            cur.execute("SELECT count(*) FROM coverage_context_facts")
            assert cur.fetchone()[0] == 2

    with pytest.raises(psycopg2.errors.UniqueViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                _context_fact(cur, subject_type="asset", asset_id="asset-1")


def test_subject_binding_is_exclusive(seeded):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                _context_fact(cur, subject_type="image", image_id="img-1", asset_id="asset-1")


@pytest.mark.parametrize("sentinel", ["deferred", "unknown", "indeterminate"])
def test_axis_sentinels_are_not_context_values(seeded, sentinel):
    """미분류 마커가 축 값 자리로 새면 planner 가 'deferred 라는 환경' 을 셀 이 된다."""
    with pytest.raises(psycopg2.errors.CheckViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                _context_fact(cur, weather=sentinel)


def test_not_applicable_weather_is_a_real_observation(seeded):
    """실내 장면의 weather='not_applicable' 은 관측된 비해당이지 미관측이 아니다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _context_fact(cur, environment_type="indoor", weather="not_applicable")
            cur.execute("SELECT weather FROM coverage_context_facts")
            assert cur.fetchone()[0] == "not_applicable"


def test_verified_must_name_the_axes_it_covers(seeded):
    """축을 하나도 말하지 않는 'verified' 는 아무것도 보장하지 않는다."""
    with pytest.raises(psycopg2.errors.CheckViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                _context_fact(cur, verified_axes=[])


def test_verified_axes_must_be_real_axis_names(seeded):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                _context_fact(cur, verified_axes=["environment_type", "vibes"])


def test_deferred_context_source_is_refused(seeded):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                _context_fact(cur, context_source="deferred")


# ─── 4. camera_registry / asset_camera_map — 만들되 비활성 ────────────────────


def test_camera_tables_ship_empty(pg_resource):
    """설계서 §6.1 "첫 pilot 에서 불필요하면 비활성" — 채우는 경로가 없어야 한다."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT (SELECT count(*) FROM camera_registry), (SELECT count(*) FROM asset_camera_map)")
            assert cur.fetchone() == (0, 0)


def test_source_unit_name_cannot_be_a_camera_assignment_source(seeded):
    """``source_unit_name`` 은 처리 단계/복수 카메라가 섞인 값이라 카메라 키가 될 수 없다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO camera_registry (camera_id) VALUES ('cam-1')")

    with pytest.raises(psycopg2.errors.CheckViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO asset_camera_map (asset_id, camera_id, assignment_source)
                    VALUES ('asset-1', 'cam-1', 'source_unit_name')
                    """
                )


# ─── 5. generation_reference_pool — fail-closed 기본값 ────────────────────────


def test_reference_defaults_are_unusable(seeded):
    """기본 status='draft' + holdout_excluded=true. 승인 없이는 후보 뷰에 뜨지 않는다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO generation_reference_pool
                       (image_id, asset_id, reference_key, allowed_workflows, safe_region_json)
                VALUES ('img-2', 'asset-1', 'unit/img-2.jpg', ARRAY['flux2-klein-4b-edit-v1'], '{}'::jsonb)
                """
            )
            cur.execute("SELECT status, holdout_excluded, requires_safe_region FROM generation_reference_pool")
            assert cur.fetchone() == ("draft", True, True)
            cur.execute("SELECT count(*) FROM v_generation_reference_candidates")
            assert cur.fetchone()[0] == 0


def test_safe_region_is_required_by_default(seeded):
    """inpaint 를 마스크 없이 자동 실행하는 경로는 스키마 단계에서 막힌다."""
    with pytest.raises(psycopg2.errors.CheckViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                _reference(cur, safe_region_json=None)


def test_safe_region_may_be_waived_only_explicitly(seeded):
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _reference(cur, safe_region_json=None, requires_safe_region=False)
            cur.execute("SELECT count(*) FROM v_generation_reference_candidates")
            assert cur.fetchone()[0] == 1


def test_approved_reference_needs_an_approver(seeded):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                _reference(cur, approved_by=None, approved_at=None)


def test_holdout_and_expiry_remove_a_reference_from_candidates(seeded):
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            ref = _reference(cur)
            cur.execute("SELECT count(*) FROM v_generation_reference_candidates")
            assert cur.fetchone()[0] == 1

            cur.execute("UPDATE generation_reference_pool SET holdout_excluded = TRUE WHERE reference_id = %s", (ref,))
            cur.execute("SELECT count(*) FROM v_generation_reference_candidates")
            assert cur.fetchone()[0] == 0

            cur.execute(
                """
                UPDATE generation_reference_pool
                   SET holdout_excluded = FALSE, valid_until = now() - interval '1 day'
                 WHERE reference_id = %s
                """,
                (ref,),
            )
            cur.execute("SELECT count(*) FROM v_generation_reference_candidates")
            assert cur.fetchone()[0] == 0


def test_reference_survives_context_deletion_but_loses_eligibility(seeded):
    """context 가 사라져도 인제스트를 막지 않고(SET NULL), 후보로는 남되 미검증으로 보인다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            ctx = _context_fact(cur)
            _reference(cur, context_fact_id=ctx)
            cur.execute("SELECT context_verified FROM v_generation_reference_candidates")
            assert cur.fetchone()[0] is True

            cur.execute("DELETE FROM coverage_context_facts WHERE context_fact_id = %s", (ctx,))
            cur.execute("SELECT context_fact_id, context_verified FROM v_generation_reference_candidates")
            assert cur.fetchone() == (None, False)


def test_one_reference_row_per_image(seeded):
    with pytest.raises(psycopg2.errors.UniqueViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                _reference(cur)
                _reference(cur)


# ─── 6. §6.4 뷰 계약 ─────────────────────────────────────────────────────────


def test_eligible_view_shows_uncontexted_units_instead_of_hiding_them(seeded):
    """ "비율 0" 과 "context 관측 불가" 를 한 뷰에서 따로 셀 수 있어야 한다.

    context 미검증 행을 뷰가 걸러냈다면 둘 다 count 0 으로 보였을 것이다 — 이 레포가 반복해
    겪은 "부재에 기댄 안전" 패턴. 그래서 뷰는 거르지 않고 컬럼으로 드러낸다.
    """
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _finalized_fact(cur)
            cur.execute(
                """
                SELECT context_fact_id, context_verified, context_verified_axes, environment_type
                  FROM v_eligible_coverage_units
                """
            )
            row = cur.fetchone()
            assert row == (None, False, [], None), row

            cur.execute("SELECT count(*) FILTER (WHERE context_verified) FROM v_eligible_coverage_units")
            assert cur.fetchone()[0] == 0  # eligible cell count
            cur.execute("SELECT count(*) FILTER (WHERE NOT context_verified) FROM v_eligible_coverage_units")
            assert cur.fetchone()[0] == 1  # observed-but-uncontexted


def test_eligible_view_only_counts_finalized_units(seeded):
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _finalized_fact(cur, image_id="img-1", review_status="reviewed", finalized_at=None)
            _finalized_fact(cur, image_id="img-2")
            cur.execute("SELECT image_id FROM v_eligible_coverage_units")
            assert [r[0] for r in cur.fetchall()] == ["img-2"]


def test_asset_level_context_reaches_image_grained_facts(seeded):
    """실측상 context 는 전부 video_metadata(asset 단위)에서 온다 — image 단위만 봤다면 뷰가
    영원히 context 없는 행만 냈을 것이다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _finalized_fact(cur)
            _context_fact(cur, subject_type="asset", asset_id="asset-1")
            cur.execute(
                "SELECT context_subject_type, environment_type, daynight_type, context_verified "
                "FROM v_eligible_coverage_units"
            )
            assert cur.fetchone() == ("asset", "outdoor", "day", True)


def test_image_level_context_wins_over_the_asset_fallback(seeded):
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _finalized_fact(cur)
            _context_fact(cur, subject_type="asset", asset_id="asset-1", environment_type="outdoor")
            _context_fact(
                cur,
                subject_type="image",
                asset_id=None,
                image_id="img-1",
                environment_type="indoor",
                daynight_type="night",
            )
            cur.execute("SELECT context_subject_type, environment_type, daynight_type FROM v_eligible_coverage_units")
            assert cur.fetchone() == ("image", "indoor", "night")


def test_synthetic_origin_stays_separable_from_real(seeded):
    """설계서: synthetic 을 real 과 합쳐서만 보는 집계는 금지. 뷰가 origin_kind 를 그대로 노출한다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _seed_asset(cur, "asset-gen", source_type="genai_output")
            _seed_image(cur, "img-gen", "asset-gen")
            _finalized_fact(cur)
            _finalized_fact(cur, image_id="img-gen", asset_id="asset-gen", origin_kind="synthetic")
            cur.execute("SELECT origin_kind, count(*) FROM v_eligible_coverage_units GROUP BY 1 ORDER BY 1")
            assert cur.fetchall() == [("real", 1), ("synthetic", 1)]
