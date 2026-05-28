# PG Restore Drill — 분기 1회 검증 절차

> 백업이 *실행*되는 것과 *복원 가능*한 것은 다른 문제다. 이 드릴을 통과하지 못한 백업 = theater.

## TL;DR

```bash
# DRY RUN (snapshot 파싱만, DB 안 건드림)
sudo ./scripts/restic-restore.sh --dry-run

# 풀 드릴 (임시 DB 생성 → row count 검증 → drop)
sudo ./scripts/restic-restore.sh
```

PASS 조건: 스크립트가 exit 0 + `raw_files` row count ≥ 직전 운영 DB 측정치의 95%.

## 백업 아키텍처 (요약)

| 컴포넌트 | 위치 | 비고 |
|---------|------|------|
| `pg-backup` sidecar | `docker-pg-backup-1` (alpine + restic) | compose profile `backup` 로 opt-in |
| 스케줄 | 매일 02:00 KST | `docker/pg-backup/crontab` |
| 덤프 형식 | `pg_dump -F c -Z 9` | custom format, 최대 압축 |
| restic backend | NAS 10.0.0.36 CIFS — `/home/user/mou/nas_192tb/backup/postgres-restic` | 운영 NAS 10.0.0.51 과 *물리적으로* 분리 (failure isolation) |
| Retention | daily=7, weekly=4, monthly=3 | `BACKUP_RETENTION_*` env |

## 분기 드릴 절차

### 1) 사전 점검 (5분)

```bash
# (a) 컨테이너 살아있나
docker ps --filter name=docker-pg-backup-1 --format '{{.Status}}'

# (b) 최근 백업 로그
docker exec docker-pg-backup-1 tail -50 /var/log/backup.log

# (c) restic snapshot 목록
docker exec docker-pg-backup-1 restic snapshots --compact --last 7

# (d) repo health (검증 + 무결성 체크)
docker exec docker-pg-backup-1 restic check --read-data-subset=5%
```

기대값:
- `restic snapshots` 출력에 **오늘 또는 어제 날짜** snapshot 존재
- `restic check` exit 0 (corrupt pack 없음)
- 백업 로그 마지막 라인 `backup done`

⛔ Fail 시 — drill 진행 의미 없음. backup 컨테이너 로그 분석 우선.

### 2) DRY RUN (3분)

```bash
sudo ./scripts/restic-restore.sh --dry-run
```

- restic 에서 stdin dump 를 `/tmp/restic-restore-vlm_pipeline_restore_drill.dump` 로 추출
- `file` 명령으로 PostgreSQL custom dump 시그니처 확인 (`PostgreSQL custom database dump`)
- DB 변경 없음

### 3) 풀 드릴 (10~15분)

```bash
sudo ./scripts/restic-restore.sh
```

스크립트가 하는 일 — 각 단계 실패 시 자동 exit:

1. **snapshot 목록 출력** (최근 3개)
2. `restic dump latest "" > /tmp/restic-restore-*.dump` — stdin 덤프 추출
3. `DROP DATABASE IF EXISTS vlm_pipeline_restore_drill; CREATE DATABASE …` — **임시 DB** 만 사용. 운영 `vlm_pipeline` 은 절대 안 건드림 (`target-db == vlm_pipeline` 강제 차단)
4. `pg_restore --no-owner --no-privileges` 로 임시 DB 에 적재
5. **검증 쿼리** 실행:
   ```sql
   SELECT 'raw_files'        , COUNT(*) FROM raw_files
   UNION ALL SELECT 'video_metadata'   , COUNT(*) FROM video_metadata
   UNION ALL SELECT 'image_labels'     , COUNT(*) FROM image_labels
   UNION ALL SELECT 'labels'           , COUNT(*) FROM labels
   UNION ALL SELECT 'dispatch_requests', COUNT(*) FROM dispatch_requests;
   ```
6. **임시 DB drop + /tmp 정리**
7. `✅ restore drill 통과 — 백업 무결성 확인됨`

### 4) PASS 판정 (수동)

운영 DB 의 같은 시점 row count 와 비교:

```bash
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
  SELECT 'raw_files', COUNT(*) FROM raw_files
  UNION ALL SELECT 'video_metadata', COUNT(*) FROM video_metadata
  UNION ALL SELECT 'labels', COUNT(*) FROM labels;
"
```

기준:
- `raw_files`, `video_metadata`, `labels` 의 복원 row count ≥ 운영 row count × 0.95
  (백업 시점 ↔ 측정 시점 시차로 5% 미만 drift 허용)
- 모든 테이블 row count > 0 (empty DB 복원은 backup 실패)

### 5) 기록

분기 드릴 결과를 `docs/exec-plans/active/<active-plan>.md` 또는 `docs/playbook/` 의 운영 로그 섹션에 한 줄 기록:

```
2026-08-15 — PG restore drill PASS — restored raw_files=12,543 (prod=12,547, drift 0.03%)
                                       runtime 9m12s, snapshot=abc1234
```

## 트러블슈팅

| 증상 | 원인 / 대응 |
|------|-----------|
| `restic snapshots` empty | sidecar 가 한 번도 안 돌았거나 RESTIC_REPOSITORY mount 실패. `docker logs docker-pg-backup-1` 확인. |
| `restic check` corrupt | repo 손상. `restic rebuild-index` 시도 후 그래도 안 되면 **백업 신뢰 상실** — incident 처리. |
| `pg_restore` 경고 (owner/privilege) | `--no-owner --no-privileges` 사용 중이라 경고만 — exit 0 이면 무시 가능. |
| `target-db 가 운영 DB 와 같음` 에러 | 의도된 안전장치. `--target-db` 인자로 다른 이름 명시. |
| 임시 DB drop 실패 | 다른 세션 connection 점유. `pg_terminate_backend` 로 정리 후 재시도. |
| 복원 row count 가 운영 대비 50% 이하 | 백업 시점과 측정 시점 사이 대량 ingest 발생 가능 — `pg_dump` 시간 + 측정 시간 비교. 그래도 의심되면 **백업 1일치 이전 snapshot** 으로 재드릴. |

## 비상 복구 (실제 운영 DB 손실 시)

> 이건 드릴이 아니라 incident response. 위 스크립트는 *못* 씀 (target-db 가드 때문).

```bash
# 1) 운영 컨테이너 중지 (sensor/scheduler 모두)
docker compose stop dagster dagster-daemon dagster-code-server

# 2) 손상된 DB 백업 (rename 으로 보존)
docker exec docker-postgres-1 psql -U airflow -d postgres \
  -c "ALTER DATABASE vlm_pipeline RENAME TO vlm_pipeline_broken_$(date +%Y%m%d);"
docker exec docker-postgres-1 psql -U airflow -d postgres \
  -c "CREATE DATABASE vlm_pipeline;"

# 3) restic 에서 최신 dump 추출 (또는 --snapshot <id> 로 특정 시점)
docker exec docker-pg-backup-1 restic dump latest "" > /tmp/emergency-restore.dump

# 4) 운영 DB 에 직접 복원
docker cp /tmp/emergency-restore.dump docker-postgres-1:/tmp/restore.dump
docker exec docker-postgres-1 pg_restore -U airflow -d vlm_pipeline \
  --no-owner --no-privileges /tmp/restore.dump

# 5) row count 확인
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "SELECT COUNT(*) FROM raw_files;"

# 6) 서비스 재기동 + sensor 1 tick 모니터링
docker compose start dagster dagster-daemon dagster-code-server
```

복구 후:
- Dagster UI 에서 **모든 sensor 가 새 cursor 로 정상 tick 하는지** 확인
- ingest_status 에 stuck row 없는지 점검 (`processing` 상태 1시간 이상 = 의심)
- staging 환경에서 직전 24h 분 ingest 재시도 (idempotent 설계라 안전)

## 변경 이력

- 2026-05-28: 초안 작성, 분기 드릴 절차 정의 (PR #?)
