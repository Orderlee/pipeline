# dagster-staging 장애 복구 (`daemon_heartbeats` / 빈 `runs.db`)

## 증상

- 로그: `sqlite3.OperationalError: no such table: daemon_heartbeats`
- `docker/data/dagster_home_staging/storage/runs.db` 가 **0바이트**이거나 깨짐
- 센서/데몬이 동작하지 않음

## 원인

`staging_reset.sh` 등으로 **`storage/` 안 파일만 부분 삭제**되거나, 빈 DB가 남은 경우 Dagster SQLite 스키마가 불완전함.

## 복구 (권장)

1. **컨테이너 중지**

   ```bash
   docker compose -f docker/docker-compose.yaml --profile staging stop dagster-staging
   ```

2. **staging 스토리지 전체 비우기** (호스트에서 root 권한 필요 시 아래처럼 Alpine 사용)

   ```bash
   docker run --rm --user root \
     -v /절대경로/Datapipeline-Data-data_pipeline/docker/data:/data \
     alpine:latest sh -c "rm -rf /data/dagster_home_staging/storage/* /data/dagster_home_staging/storage/.[!.]* 2>/dev/null; mkdir -p /data/dagster_home_staging/storage"
   ```

3. **재기동**

   ```bash
   docker compose -f docker/docker-compose.yaml --profile staging up -d dagster-staging
   ```

4. **확인**: `runs.db`·`schedules.db`·`index.db` 가 수십~수백 KiB 이상이고, 로그에 SensorDaemon tick 이 반복되면 정상.

## QA와의 관계

- `scripts/cleanup_staging_test.sh` 는 **DuckDB·MinIO·NAS** 만 건드림 → Dagster storage 와 무관.
- **`staging_reset.sh --all`** 은 **Dagster staging storage 를 지움** → 위 복구 절차 후 **반드시 재기동** 필요.
