"""PostgreSQL CRUD helpers for Label Studio sync.

label_studio → PostgreSQL labels / image_labels 테이블 읽기·쓰기.
ls_sync.py 의 run() 에서 사용; 직접 호출도 가능.
"""

from __future__ import annotations


class FinalizedLabelsSkip(RuntimeError):
    """finalized guard 가 활성화돼 upsert_video_labels 를 skip 할 때 발생.

    MinIO write 이전에 PG guard 를 확인해 불일치를 방지한다.
    labels_key 속성으로 어느 키가 guard 에 걸렸는지 호출자가 알 수 있다.
    """

    def __init__(self, labels_key: str) -> None:
        super().__init__(f"finalized guard: {labels_key}")
        self.labels_key = labels_key


class FinalizedImageSkip(RuntimeError):
    """image_labels finalized guard — update_image_labels_in_db 에서 발생.

    UPDATE WHERE review_status <> 'finalized' 로 PG 는 자체 보호되지만,
    호출자가 MinIO write 를 skip 하기 위해 이 예외를 사용한다.
    """

    def __init__(self, labels_key: str) -> None:
        super().__init__(f"image finalized guard: {labels_key}")
        self.labels_key = labels_key


class ConcurrentLabelsRace(RuntimeError):
    """(labels_key, event_index) UNIQUE constraint (005 migration) violation.

    동시 webhook 또는 retry 로 같은 key 에 두 번 INSERT 시도 시 발생.
    silent duplicate 삽입 대신 explicit error 로 호출자에게 전달 — 호출자가
    log + skip 결정. ls_sync.run() 안에서 catch 하여 error counter 증가시킴.
    """

    def __init__(self, labels_key: str) -> None:
        super().__init__(f"concurrent race on labels: {labels_key}")
        self.labels_key = labels_key


def _connect_postgres_with_retry(
    dsn: str,
    *,
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    readonly: bool = False,
):
    """PostgreSQL 연결. transient 오류 시 exponential backoff로 재시도.

    DuckDB 의 단일 파일 write-lock 재시도와 달리, PG 는 lock 경합이 아니라
    일시적 연결 오류(서버 재기동/네트워크)에 대해서만 재시도한다
    (psycopg2.OperationalError / InterfaceError).
    """
    import psycopg2
    import time as _time
    import random as _random

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            conn = psycopg2.connect(dsn)
            if readonly:
                # read-only path 는 트랜잭션 잔존 없이 즉시 자동 커밋.
                conn.set_session(readonly=True, autocommit=True)
            return conn
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            last_exc = exc
            if attempt >= max_retries:
                raise
            delay = min(base_delay * (2**attempt) + _random.uniform(0, 1), max_delay)
            print(f"[RETRY] PostgreSQL 연결 실패, {delay:.1f}s 후 재시도 ({attempt + 1}/{max_retries}): {exc}")
            _time.sleep(delay)
    raise RuntimeError(f"PostgreSQL 연결 재시도 초과: {last_exc}")


def lookup_asset_id_by_raw_key(dsn: str, raw_key: str, conn=None) -> str | None:
    """raw_files.raw_key 단독 조회로 asset_id 반환. 없으면 None.

    note:
      - source_unit_name 은 dispatch 시 입력한 원본 표기(대소문자/특수문자
        보존)이고 raw_key 는 sanitize 된 MinIO key 라 둘이 다를 수 있다.
        조회 키로는 raw_key 단독을 쓴다 (raw_key 는 사실상 unique).
      - conn 이 주어지면 재사용. 없으면 read-only 로 새 연결.
    """
    if not raw_key:
        return None
    if conn is None:
        if not dsn:
            return None
        c = _connect_postgres_with_retry(dsn, readonly=True)
        try:
            with c.cursor() as cur:
                cur.execute(
                    "SELECT asset_id FROM raw_files WHERE raw_key = %s LIMIT 1",
                    (raw_key,),
                )
                row = cur.fetchone()
        finally:
            c.close()
    else:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT asset_id FROM raw_files WHERE raw_key = %s LIMIT 1",
                (raw_key,),
            )
            row = cur.fetchone()
    return row[0] if row else None


def upsert_video_labels(
    dsn: str,
    labels_bucket: str,
    labels_key: str,
    asset_id: str,
    new_events: list[dict],
    conn=None,
) -> tuple[int, int]:
    """labels_key 기준으로 기존 row 전부 DELETE → 사람 submit 결과로 INSERT.

    SOT = 사람 submit. 3-way merge / removed_by_reviewer 상태 없음.
    new_events=[] 이면 DELETE 만 (INSERT 없음).
    guard SELECT + DELETE + INSERT 를 한 트랜잭션으로 묶어 원자성 보장
    (psycopg2 autocommit=False → 마지막에 conn.commit()).

    finalized 보호 가드: 이미 review_status='finalized' 인 row 가 하나라도 있으면
    upsert 자체를 skip. /sync-approve 로 확정된 라벨이 사람 재 Submit 또는
    debounce 잔존 webhook 에 의해 'reviewed' 로 회귀하던 사고 (운영 1회 발생) 방지.
    재검수가 필요하면 state.json status 를 운영자가 명시 리셋 후 sync 재실행해야 한다.

    반환값: (deleted_count, inserted_count)
    """
    import hashlib

    owns_conn = conn is None
    if owns_conn:
        conn = _connect_postgres_with_retry(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM labels WHERE labels_key = %s AND review_status = 'finalized'",
                (labels_key,),
            )
            finalized_row = cur.fetchone()
            if finalized_row and int(finalized_row[0]) > 0:
                print(
                    f"[GUARD] {labels_key} 는 이미 finalized 라벨 보유 → upsert_video_labels skip "
                    f"(SOT 회귀 방지). 재검수 필요 시 state.json status 리셋 후 재실행."
                )
                # guard SELECT 가 연 read 트랜잭션 정리 (공유 conn idle-in-transaction 방지).
                conn.rollback()
                raise FinalizedLabelsSkip(labels_key)

            cur.execute(
                "SELECT COUNT(*) FROM labels WHERE labels_key = %s",
                (labels_key,),
            )
            deleted_row = cur.fetchone()
            deleted = int(deleted_row[0]) if deleted_row else 0

            cur.execute("DELETE FROM labels WHERE labels_key = %s", (labels_key,))

            inserted = 0
            event_count = len(new_events)
            for i, ev in enumerate(new_events):
                start_sec = ev["timestamp"][0]
                end_sec = ev["timestamp"][1]
                token = f"{asset_id}|manual_review|{i}|{start_sec}|{end_sec}"
                label_id = hashlib.sha1(token.encode()).hexdigest()
                cur.execute(
                    """
                    INSERT INTO labels (
                        label_id, asset_id, labels_bucket, labels_key,
                        label_format, label_tool, label_source,
                        review_status, event_index, event_count,
                        timestamp_start_sec, timestamp_end_sec,
                        label_status, created_at
                    ) VALUES (%s, %s, %s, %s, 'json', 'label_studio', 'manual_review',
                              'reviewed', %s, %s, %s, %s, 'completed', NOW())
                    """,
                    (
                        label_id,
                        asset_id,
                        labels_bucket,
                        labels_key,
                        i,
                        event_count,
                        start_sec,
                        end_sec,
                    ),
                )
                inserted += 1

        conn.commit()
        return deleted, inserted
    except FinalizedLabelsSkip:
        # guard 자체 path — 이미 rollback 호출됨, 그대로 전파.
        raise
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        # 005 migration UNIQUE constraint (labels_key, event_index) 위반 시 explicit error.
        # psycopg2.errors.UniqueViolation 도 동일 catch.
        try:
            import psycopg2.errors  # local import — psycopg2 가 없는 환경 (test mock) 대응

            if isinstance(exc, psycopg2.errors.UniqueViolation):
                raise ConcurrentLabelsRace(labels_key) from exc
        except ImportError:
            pass
        raise
    finally:
        if owns_conn:
            conn.close()


def update_image_labels_in_db(dsn: str, labels_key: str, object_count: int, conn=None) -> int:
    """image_labels 테이블의 review_status/label_source/object_count 전이.
    conn이 주어지면 재사용 (커넥션 open/close 오버헤드 감소).

    finalized 보호 가드: WHERE 절에 review_status<>'finalized' 추가. /sync-approve 로
    확정된 image_labels 가 사람 재 Submit 또는 debounce 잔존 webhook 에 의해 'reviewed'
    로 회귀하던 사고 방지. 재검수는 state.json status 리셋 후 재 sync 가 정식 절차.

    psycopg2 autocommit=False → UPDATE 후 conn.commit() (공유 conn 이라도 자기 변경만
    커밋). 예외 시 rollback 으로 in-flight 변경만 폐기.
    """
    if not labels_key or (not dsn and conn is None):
        return 0
    owns_conn = conn is None
    if owns_conn:
        conn = _connect_postgres_with_retry(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM image_labels WHERE labels_key = %s AND review_status = 'finalized'",
                (labels_key,),
            )
            fin_row = cur.fetchone()
            if fin_row and int(fin_row[0]) > 0:
                print(
                    f"[GUARD] {labels_key} 는 이미 finalized image_labels 보유 → update_image_labels_in_db skip "
                    f"(SOT 회귀 방지). 재검수 필요 시 state.json status 리셋 후 재실행."
                )
                conn.rollback()
                raise FinalizedImageSkip(labels_key)

            cur.execute(
                """
                UPDATE image_labels
                SET review_status = 'reviewed',
                    label_source  = 'manual_review',
                    object_count  = %s
                WHERE labels_key = %s
                  AND review_status <> 'finalized'
                """,
                (int(object_count), labels_key),
            )
            cur.execute(
                "SELECT COUNT(*) FROM image_labels WHERE labels_key = %s AND review_status IN ('reviewed','finalized')",
                (labels_key,),
            )
            row = cur.fetchone()
        conn.commit()
        return int(row[0]) if row else 0
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        if owns_conn:
            conn.close()


def replace_image_label_annotations(
    conn,
    image_label_id: str,
    image_id: str | None,
    boxes: list[dict],
) -> tuple[int, int]:
    """확정 image_labels 의 COCO 박스를 image_label_annotations 로 전량 재투영.

    image_label_id 기준 기존 행 전량 DELETE → box 별 INSERT (derived 재생성형 인덱스).
    MinIO COCO JSON 이 SOT 이고 이 테이블은 조회용 사본이므로, 같은 image_label_id 를
    여러 번 투영해도 안전해야 한다 — 멱등성: annotation_id = sha1(f"{image_label_id}|{box_index}").

    호출자가 트랜잭션을 소유(commit/rollback)한다 — image_label_id 단위 원자성. 한 행이
    실패해도 다른 행 projection 을 막지 않도록 호출자가 per-row try/except + rollback 한다.

    boxes 원소는 detection_coco.parse_coco_annotation_boxes() 출력
    ({box_index, category, bbox_x, bbox_y, bbox_w, bbox_h, score}).

    반환: (deleted, inserted).
    """
    import hashlib

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM image_label_annotations WHERE image_label_id = %s",
            (image_label_id,),
        )
        row = cur.fetchone()
        deleted = int(row[0]) if row else 0

        cur.execute(
            "DELETE FROM image_label_annotations WHERE image_label_id = %s",
            (image_label_id,),
        )

        inserted = 0
        for b in boxes:
            annotation_id = hashlib.sha1(f"{image_label_id}|{b['box_index']}".encode()).hexdigest()
            cur.execute(
                """
                INSERT INTO image_label_annotations (
                    annotation_id, image_label_id, image_id, box_index, category,
                    bbox_x, bbox_y, bbox_w, bbox_h, score
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    annotation_id,
                    image_label_id,
                    image_id,
                    int(b["box_index"]),
                    b["category"],
                    b["bbox_x"],
                    b["bbox_y"],
                    b["bbox_w"],
                    b["bbox_h"],
                    b["score"],
                ),
            )
            inserted += 1
    return deleted, inserted
