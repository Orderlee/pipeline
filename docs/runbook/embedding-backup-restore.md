# Embedding Backup & Restore — image_embeddings (pgvector)

## 백업 포함 여부 결론

**image_embeddings 는 기존 restic pg-backup 에 이미 포함된다.**

`docker/pg-backup/backup.sh` 는 `pg_dump -F c -Z 9 -d "${PGDATABASE:-vlm_pipeline}"` 를 전체 DB 단위로 실행한다.
image_embeddings 테이블은 `vlm_pipeline` DB 에 속하므로 별도 설정 없이 매일 02:00 KST 스냅샷에 포함된다.

별도 테이블 단위 백업은 불필요. 단, 아래 조건이 맞지 않으면 **복구 시 실패**한다 → 아래 복구 전제조건 섹션 필독.

---

## 백업 아키텍처 (요약)

| 항목 | 값 |
|------|-----|
| 백업 컨테이너 | `docker-pg-backup-1` (alpine + postgresql15-client + restic) |
| 스케줄 | 매일 02:00 KST (`docker/pg-backup/crontab`) |
| 덤프 대상 DB | `vlm_pipeline` (env `PG_BACKUP_DATABASE`, default `vlm_pipeline`) |
| 덤프 형식 | `pg_dump -F c -Z 9` (custom format, 최대 압축) |
| restic 백엔드 | NAS 10.0.0.36 CIFS — `BACKUP_REPO_HOST_PATH` (default `/home/user/mou/nas_192tb/backup/postgres-restic`) |
| Retention | daily=7, weekly=4, monthly=3 (`BACKUP_RETENTION_*` env) |

---

## 복구 전제조건 — pgvector 필수

### 왜 중요한가

`image_embeddings` 테이블은 `vector(1024)` 컬럼과 HNSW 인덱스를 포함한다.
`pg_restore` 는 dump 안의 `CREATE EXTENSION vector` 구문을 실행하려 시도하는데,
복구 대상 PG 이미지에 **pgvector 확장이 설치되어 있지 않으면** 이 구문이 실패하고
`image_embeddings` 테이블 생성 및 데이터 적재 전체가 skip된다.

### 올바른 복구 대상 이미지

```
# prod 에서 사용 중인 pgvector 지원 이미지 (POSTGRES_IMAGE env 로 override)
datapipeline-pg-pgvector   # 내부 빌드 이미지 (pgvector + pg_duckdb 포함)
```

vanilla `postgres:15` 이미지로 복구하면 `vector` 확장 설치에 실패한다.
복구 전 대상 PG 컨테이너 이미지를 반드시 확인할 것:

```bash
docker inspect docker-postgres-1 --format '{{.Config.Image}}'
```

`pg_duckdb` 확장도 동일: pgduckdb 포함 이미지가 아니면 해당 extension CREATE 가 실패한다.

---

## HNSW 인덱스 복구 주의사항

`pg_dump` custom format은 인덱스 정의를 DDL로 포함한다.
`pg_restore` 시 인덱스는 **데이터 삽입 후 재생성**된다.

현재 HNSW 인덱스:

```sql
CREATE INDEX image_embeddings_hnsw
  ON image_embeddings USING hnsw (embedding vector_cosine_ops);
```

대량 행(수십만 건 이상)이 있으면 HNSW 빌드에 수분~수십분이 소요될 수 있다.
복구 후 인덱스가 정상 존재하는지 확인:

```bash
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
  SELECT indexname, indexdef
  FROM pg_indexes
  WHERE tablename = 'image_embeddings';
"
```

출력에 `image_embeddings_hnsw` 가 없으면 수동으로 재생성:

```sql
CREATE INDEX IF NOT EXISTS image_embeddings_hnsw
  ON image_embeddings USING hnsw (embedding vector_cosine_ops);
```

---

## 백업 전/후 상태 대조

`scripts/verify_embedding_backup.sh` 를 **백업 전**과 **복구 후** 각각 실행해 row count 와 인덱스 존재를 대조한다.

```bash
# 백업 전 (운영 DB)
PGDSN="postgresql://airflow:airflow@localhost:15433/vlm_pipeline" \
  ./scripts/verify_embedding_backup.sh > /tmp/embedding_before.txt

# 복구 후 (복구 대상 DB)
PGDSN="postgresql://airflow:airflow@localhost:15433/vlm_pipeline_restore_drill" \
  ./scripts/verify_embedding_backup.sh > /tmp/embedding_after.txt

diff /tmp/embedding_before.txt /tmp/embedding_after.txt
```

---

## 표준 복구 (분기 드릴)

기존 `scripts/restic-restore.sh` 드릴 절차를 그대로 사용한다.
restic restore drill 에 대한 전체 절차는 `docs/runbook/pg-restore-drill.md` 참조.

드릴 완료 후 image_embeddings 까지 복원됐는지 추가 확인:

```bash
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline_restore_drill -c "
  SELECT entity_type, COUNT(*) FROM image_embeddings GROUP BY entity_type;
"
```

---

## 부분 백업 — 테이블 단위 (선택적)

전체 DB dump 와 별개로 image_embeddings 만 추출할 경우:

```bash
# 호스트에서 컨테이너 경유 dump
docker exec docker-postgres-1 \
  pg_dump -U airflow -d vlm_pipeline \
  -F c -Z 9 -t image_embeddings \
  > /tmp/image_embeddings_$(date '+%Y%m%d_%H%M%S').dump
```

복구:

```bash
# 운영 DB 에 직접 복구 (기존 행은 UNIQUE 제약으로 conflict skip)
docker cp /tmp/image_embeddings_<TS>.dump docker-postgres-1:/tmp/ie_restore.dump
docker exec docker-postgres-1 \
  pg_restore -U airflow -d vlm_pipeline \
  --no-owner --no-privileges \
  --table=image_embeddings \
  /tmp/ie_restore.dump
```

주의: 부분 restore 는 `CREATE EXTENSION vector` 구문이 포함되지 않을 수 있다.
대상 DB 에 vector 확장이 없으면 `CREATE TABLE` 도 실패한다.
사전에 확인:

```sql
SELECT extname, extversion FROM pg_extension WHERE extname = 'vector';
```

---

## 비상 복구 (전체 운영 DB 손실 시)

image_embeddings 단독 손실이 아닌 DB 전체 손실 시 → `docs/runbook/pg-restore-drill.md` 의 "비상 복구" 섹션을 따른다.
복구 후 image_embeddings HNSW 인덱스 존재 확인을 추가 step 으로 수행할 것.

---

## 변경 이력

- 2026-06-16: 초안 (feature/embed-prod-captions)
