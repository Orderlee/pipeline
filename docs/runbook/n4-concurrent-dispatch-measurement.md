# N4 — 동시 dispatch 처리량 측정 (운영자 가이드)

> stack-candidates §13 N4. 1 source single dispatch (S24v4 = 5h29m) 외에 동시 dispatch 의 sensor 동작 / GPU contention / DB lock 영향이 측정 안 된 상태. 본 절차는 캠페인 시간이 잡혔을 때 운영자가 그대로 실행할 수 있는 가이드.

## 준비

```bash
# host 에서 (10.0.0.10)
cd /home/user/work_p/Datapipeline-Data-data_pipeline
ls scripts/collect_concurrent_dispatch_metrics.sh
```

## 측정 시나리오 안

### 시나리오 A — 2 source 동시 dispatch (DB lock 영향)

작은 video 100개씩 2 source. `duckdb_writer` tag 가 concurrency=1 이라 라벨링 단계가 직렬화되는지 확인.

| 단계 | 명령 |
|------|------|
| 1) 측정 시작 | `./scripts/collect_concurrent_dispatch_metrics.sh --output /tmp/n4_A --interval 5 --duration 7200 &` |
| 2) dispatch 트리거 | `cp /tmp/dispatch_A.json /home/user/mou/nas_200tb/incoming/.dispatch/pending/` (2개 동시) |
| 3) Dagster UI | `:3030/runs` 에서 동시 run 진행 확인 |
| 4) 측정 종료 | 자동 (duration 만료) 또는 `kill %1` |
| 5) 결과 | `/tmp/n4_A/<timestamp>/SUMMARY.txt` |

### 시나리오 B — 3 source 동시 (image + video 혼합, GPU contention)

| 단계 | 명령 |
|------|------|
| 1) 측정 시작 | `./scripts/collect_concurrent_dispatch_metrics.sh --output /tmp/n4_B --interval 5 --duration 7200 &` |
| 2) dispatch 트리거 | image 1개 + video 2개 dispatch JSON 동시 투입 |
| 3) GPU 모니터 | `watch nvidia-smi` 별도 터미널에서 |
| 4) 결과 | `/tmp/n4_B/<timestamp>/` 의 `gpu/*.csv` peak util |

### 시나리오 C — 5 source 점진 (saturation point)

| 단계 | 명령 |
|------|------|
| 1) 측정 시작 | `./scripts/collect_concurrent_dispatch_metrics.sh --output /tmp/n4_C --interval 3 --duration 14400 &` |
| 2) 첫 dispatch | t=0 에 source 1 트리거 |
| 3) 점진 추가 | t=15min 마다 source 1개씩 추가 (5개까지) |
| 4) 모니터링 | `cross_table_consistency_sensor` Dagster UI 로그 — 잠금 대기 알림 |
| 5) saturation | docker stats CPU > 80% 또는 GPU mem > 14GB 발생 시 — saturation 지점 |

## 측정 metrics

`SUMMARY.txt` 에 자동 집계:

- **GPU peak util** — `nvidia-smi --query-gpu` 결과
  - utilization.gpu / .memory
  - memory.used / memory.total
  - encoder session count (NVENC 동시 사용)
- **docker stats CPU peak** — 컨테이너별 CPU%, Mem, Net IO, Block IO
- **PG connection peak** — `pg_stat_activity` 의 active / lock_wait / idle_in_tx
- **Dagster active runs** — GraphQL `runsOrError(filter:STARTED|QUEUED)` 카운트

원본 snapshot 은 `<output>/<timestamp>/{gpu,docker,pg,dagster}/*.csv|tsv|json` 으로 저장 — 사후 분석 가능.

## 비교 baseline (S24v4 single dispatch)

| metric | S24v4 (single) | N4 측정 후 기록 |
|--------|----------------|-----------------|
| 총 처리시간 | 5h 29m 13s | (시나리오 별 기록) |
| dagster CPU peak | (미측정) | |
| sam3 GPU mem peak | ~7.4GB (workers=2 시) | |
| MinIO upload throughput | (미측정) | |
| PG connection peak | (미측정) | |

## 측정 후 후속

- saturation point 가 발견되면 `cross_table_consistency_sensor` (Phase 3-B) WARNING 빈도 점검
- GPU OOM 위험 시 `SAM3_WORKERS` 환경변수 조정 (workers=3 → 2 fallback 검토)
- PG lock 우선 대기 시 `duckdb_writer` tag concurrency 정책 재검토
- 결과는 `docs/exec-plans/active/qa-scenarios-playbook.md` 에 시나리오 25 추가 형태로 기록

## 변경 이력

- 2026-05-29: 초안 작성, 시나리오 A/B/C 정의, baseline 항목 정의.
