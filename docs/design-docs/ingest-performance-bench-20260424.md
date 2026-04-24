# Ingest 파이프라인 성능 벤치마크 (2026-04-24)

> 세션: staging 파일럿 마이그 8.1GB / 2731 files 에서 8h 41m 소요 → 병목 원인 분석

## 1. 문제 제기

staging 파이프라인으로 8.1GB/28 프로젝트(2731 파일) 파일럿 ingest를 돌린 결과:

```
ingest_job:        53 runs (manifest chunked)
wall time:         8h 41m  (15:59:56 → 00:41:11)
per-run 중앙값:     10m 18s
누적 run time:      8h 31m  (wall ≈ sum — 100% 순차 실행)
실패 run:           0
```

평균 처리량 = **8.1GB / 8.7h ≈ 270 KB/s**. 10GB 규모가 하루 전체를 먹는 건 scale-out 전제 하에서는 운영 불가 수준.

root cause 후보: ① `duckdb_writer` tag concurrency=1 강제, ② 각 run 내부 per-file 순차 처리, ③ SMB 마운트 튜닝.

## 2. 벤치 설계

단계별 isolation 측정 — 코드 수정 없이 각 bottleneck 후보의 thread/process 수 스케일링을 독립적으로 측정.

- **샘플**: staging archive 의 파일럿 50건 (2.08 GB, median 7.0 MB, img 17 / vid 33)
- **NAS**: `/home/pia/mou/staging/archive` (SMB `vers=3.0`, `rsize/wsize=4M`)
- **스크립트**: `scripts/migrate_legacy/bench_ingest_stages.py`

## 3. 측정 결과

### 3.1 읽기 · 체크섬 · phash · MinIO PUT (thread scaling)

| 단계 | 1 th | 2 th | 4 th | 8 th | 4→8 스케일 |
|---|---:|---:|---:|---:|---|
| NAS read (SMB) | 6.7 MB/s | 9.8 | 14.7 | 16.6 | 포화 |
| SHA256 checksum | 5.6 | 10.4 | 15.1 | 18.4 | 여전히↑ |
| phash (imgs/s) | 0.2 | 0.3 | 0.6 | 0.9 | 선형 |
| MinIO PUT | 2.4 | 4.4 | 6.8 | 9.4 | 여전히↑ |

- 1 thread 기준 **per-file 5~18초** 수준의 거대한 오버헤드 (7MB median 파일인데도).
  - SMB open/stat per-file overhead + MinIO PUT 연결 수립이 누적.
- **4~8 thread에서 선형에 가까운 스케일링**. 8 thread에서도 MinIO/SHA256은 포화 전. 16 thread까지도 여력 있을 것으로 추정.

### 3.2 DuckDB 동시 write (process concurrency)

`/tmp/*.duckdb` 임시 파일에 N 프로세스가 200건씩 INSERT. 실패 시 최대 5회 retry (50ms × 시도 횟수).

| procs | elapsed | ok/요청 | lock retry | inserts/s |
|---:|---:|---|---:|---:|
| 1 | 6.24s | 200/200 (100%) | 0 | 32.1 |
| 2 | 15.20s | 381/400 (**95%**) | 119 | 25.1 |
| 4 | 36.11s | 673/800 (**84%**) | 821 | 18.6 |
| 6 | 58.12s | 782/1200 (**65%**) | 2504 | 13.5 |

- **procs=2**: retry 119회로 흡수 가능 (95% 성공).
- **procs=4**: retry 821회, 실패율 16%.
- **procs=6**: retry 2504회, 실패율 35% — 사실상 thrashing.

주의: 벤치는 **1 row per transaction** (pessimistic). 실제 파이프라인은 batch 트랜잭션을 쓰므로 lock 보유 시간이 더 길고 빈도가 낮아 contention 패턴이 다를 수 있다. 실제 tag=2 시도 전에 batch write 단위를 확인해야 함.

## 4. 해석

### 4.1 하드웨어 컨텍스트 (중요)

| 구성 | 스펙 | 역할 |
|---|---|---|
| NAS (Synology DS2422+) | AMD Ryzen V1500B (4C/8T) · **RAM 4GB ECC** · DDR4 2400 MT/s · 1 slot 빈 (최대 64GB) · 기본 4×1GbE · **PCIe Gen3 x8(x4 link) 확장 슬롯 1개** | **SMB + MinIO** 동시 호스팅 |
| Workstation | Intel Xeon w3-2435 (8C/**16T**) · AVX-512/AMX · 16 logical CPUs | Dagster · DuckDB · ingest worker |

NAS 는 풀사이즈 PCIe 확장 슬롯을 보유. 공식 호환 카드:
- **E25G30-F2** (25GbE SFP28 ×2) — 오버스펙
- **E10G30-F2** (10GbE SFP+ ×2) / **E10G30-T2** (10GbE RJ-45 ×2) / **E10G18-T1** (10GbE RJ-45 ×1)
- ⭐ **E10M20-T1**: 10GbE RJ-45 + M.2 NVMe SSD 2슬롯 **콤보 카드** — MinIO HDD write 병목에 직접 효과.

**핵심 제약 = NAS 4GB RAM**. DiskStation OS(~500MB~1GB) + SMB 서버 버퍼 + MinIO 데몬이 **4GB 를 공유**. MinIO 단독도 통상 최소 4GB 권장인데, SMB와 OS까지 얹혀있다 보니 각 서비스가 가용 RAM에 굶주린 상태. 

증상:
- **OS 페이지 캐시가 거의 자라지 못함** → 매 SMB 파일 read 가 물리 디스크 IO → bench 의 per-file 5~18초 오버헤드와 일관.
- **MinIO 가 write 버퍼/캐시를 확보하지 못함** → PUT 2.4 MB/s (1 thread) 는 MinIO 표준 처리량(수백 MB/s)의 1% 수준.

### 4.2 최대 병목 = NAS RAM × per-file 순차 처리 복합

1. NAS 가 small-file metadata 처리에 굶주려 응답 지연 (per-file 5~18s)
2. ingest_job 내부는 파일 1개씩 read → checksum → phash → PUT → mv 로 순차 처리 (threading 없음)
3. DuckDB tag=1 강제로 53 run 전체가 직렬

이 세 가지가 곱셈으로 작용 → 8h 41m.

### 4.3 워크스테이션 쪽은 과잉

Xeon 16 thread 에서 thread=8 까지 아직 여력. **NAS 가 먼저 터짐** — 4~8 thread 이상 밀어도 NAS 응답이 선형 가속 못 따라옴. bench 에서 NAS read 8 thread 포화(16.6 MB/s)가 NAS 자원 경쟁의 신호.

### 4.4 DuckDB 동시성은 제한적 이득

`duckdb_writer` tag 1→2 로 완화해도 lock retry 로 **1.5× 정도**만 기대. tag≥4 는 vanilla DuckDB 로 실용 아님. 큰 이득을 원하면 Postgres 이전이 필요.

### 4.5 네트워크는 아직 여유

DS2422+ 기본 4×1GbE (125 MB/s 이론). bench 최고치 18 MB/s 는 대역폭의 **15%** — 네트워크 아님, NAS CPU/RAM 이 병목. 10GbE 확장카드를 깔아도 RAM 먼저 확장 안 하면 이득 없음.

### 4.6 SMB 는 현 튜닝 양호

mount options (`rsize=4M`, `wsize=4M`, `cache=strict`, `vers=3.0`) 이미 합리적. `vers=3.1.1` 업그레이드 마이너 여지는 있으나 우선순위 낮음.

## 5. 예측 모델 (10GB / 2731 files 기준)

이론 wall time = `max(run_count / tag_concurrency × median_per_run, critical_path)`. 현재 bench는 NAS RAM 4GB 상태에서 측정됨 — NAS 업그레이드는 아래 bench 데이터로는 추산 불가(재측정 필요)하므로 "예상" 라벨로 표기.

| 조합 | 예상 wall time | 개선율 | 비용/난이도 |
|---|---:|---:|---|
| **현재 (tag=1, threads=1, NAS 4GB)** | **8h 41m (실측)** | 1× | — |
| tag=2, threads=1, NAS 4GB | ~5h 30m | 1.6× | 작음 |
| tag=1, threads=4, NAS 4GB | ~2h 10m | 4× | 코드 중간 (NAS 포화로 감쇠 예상) |
| tag=1, threads=8, NAS 4GB | ~1h 30m | 5.8× | 코드 중간 (NAS 더 포화) |
| **NAS 32GB 업그레이드 단독** | **~2h** (예상) | **~4×** | **~20만원, 재부팅** |
| NAS 32GB + E10M20-T1 (NVMe 캐시+10GbE) | ~1h (예상) | ~9× | + ~70~80만원 |
| NAS 32GB + threads=4 | ~45m (예상) | ~12× | 상기 + 코드 중간 |
| NAS 32GB + E10M20-T1 + threads=8 + tag=2 | ~15m (예상) | ~35× | 하드 + 코드 |
| NAS 64GB + MinIO 분리 + Postgres + 전병렬 | ~10m (예상) | ~50× | 큼 |

## 6. 권장 작업 순서 (ROI 순)

### Phase 1 — **NAS RAM 업그레이드** (최우선, 비용 최소)

- 현재 **4GB ECC → 32GB 또는 64GB** (빈 슬롯 1개 존재, 최대 64GB 지원)
- 예상 효과: **3~5× 단축**. MinIO 가 제대로 된 캐시 확보 + OS 페이지 캐시 확장으로 SMB read 대폭 개선.
- 비용: Synology 호환 32GB DDR4 ECC SODIMM ≒ 15~30만원 선. DS2422+는 제3자 ECC 호환 비교적 관대하지만 Synology 공식 호환 리스트 확인 권장.
- 리스크: NAS 재부팅 필요 (다운타임 ~10분). 제3자 메모리 사용 시 Synology 경고 표시(기능 영향 없음).
- 슬롯 전략:
  - (a) 기존 4GB 유지 + 빈 슬롯에 32GB 추가 → 36GB 비대칭 (asymmetric dual-channel, 5~10% 대역폭 손실, 기능 영향 없음)
  - (b) 기존 4GB 제거 + 1×32GB → 32GB 단일채널 (미래 확장 여지)
  - (c) 2×32GB 매칭 → 64GB 완전 dual-channel (최대 성능)
  - MinIO 워크로드는 메모리 용량이 대역폭보다 중요 → (a) 가 가장 실용적.

### Phase 1.5 — **E10M20-T1 콤보 카드 장착** (Phase 1 완료 후)

- **10GbE RJ-45 + M.2 NVMe SSD 2슬롯** 한 장으로 제공.
- NVMe 2슬롯을 MinIO 데이터 볼륨 또는 HDD 앞단 SSD 캐시로 활용 → MinIO PUT 병목(현재 9.4 MB/s) 의 HDD write latency 직접 해소.
- 10GbE 는 네트워크 ceiling 10× 확장 — 현재 대역폭의 15% 만 쓰고 있지만 Phase 1+내부 threading 후엔 1GbE 로 부족해짐.
- 비용: 카드 ~50만원 + NVMe SSD 2EA (1TB 기준 EA당 10~15만원)
- ⚠️ **Phase 1 선행 필수**. 4GB RAM 상태에선 NVMe 속도를 다 못 씀.

### Phase 2 — **run 내부 threading 도입**

- 대상: `defs/ingest/ops_register.py`, `defs/ingest/duplicate.py` (checksum + phash 계산 루프), MinIO 업로드 스텝.
- 구현: `concurrent.futures.ThreadPoolExecutor(max_workers=N)` 로 per-file 처리 map. IO-bound 라 GIL 문제 없음.
- 환경변수 `INGEST_WORKER_THREADS` 로 조정 가능하게 (기본 4).
- 예상 효과: **2~3× 추가 단축** (Phase 1 후). NAS 업그레이드 없이 단독 적용 시 효과 감쇠 — NAS 가 먼저 터져서 thread>4 에서 수익 감소.

### Phase 3 — **tag concurrency 1→2 실험**

- `definitions.py` 의 `run_coordinator` 에서 `duckdb_writer` tag 값 2 로 변경.
- 실측 lock retry 발생률 모니터링 — 실제 batch write 사이즈 기준으로 contention 이 벤치 수치보다 낮을 것으로 기대.
- 예상 추가 이득: **1.3~1.5×**.

### Phase 4 — **MinIO 를 별도 호스트로 분리** (Phase 1.5 로 대부분 해결되면 생략 가능)

- NAS 는 파일 스토리지만, MinIO 는 워크스테이션 혹은 별도 박스로 이전.
- Phase 1.5 (NVMe SSD 캐시) 로 HDD IO 병목이 풀리면 이 Phase 는 생략 가능.
- 필요한 경우: 예상 추가 이득 **2×**. 재배포·네트워크 경로 변경 큼.

### Phase 5 — **필요 시 Postgres 이전**

- Phase 1~4 후에도 ≥30분 이상 걸리는 수집이 정기적으로 발생 시 검토.
- 스키마 마이그, motherduck 동기화 경로 재설계, 일부 DuckDB-specific SQL 포팅 필요.

### Phase 6 — **마이너 튜닝**

- SMB `vers=3.1.1` 업그레이드, 10GbE 확장카드 장착 (Phase 1 선행 필수 — RAM 없이는 무효).
- `INCOMING_SENSOR_MAX_NEW_RUN_REQUESTS_PER_TICK` 상향 (sensor tick 당 run 생성 수).

## 7. 재현 방법

```bash
# 전체 벤치 (약 40~60분)
python3 -u scripts/migrate_legacy/bench_ingest_stages.py \
    --sample 50 --threads 1,2,4,8 --procs 1,2,4,6 --duckdb-rows 200

# 일부 스테이지만
python3 -u scripts/migrate_legacy/bench_ingest_stages.py \
    --sample 50 --only nas,checksum --threads 1,4,8

# 다른 샘플 루트
BENCH_SAMPLE_ROOT=/home/pia/mou/staging/incoming/gcp \
    python3 -u scripts/migrate_legacy/bench_ingest_stages.py ...
```

샘플은 `BENCH_SAMPLE_ROOT/<project>/<batch_or_root>/` 구조를 os.walk 로 탐색해 프로젝트별 cap 만큼 수집 후 round-robin 으로 N개 선택. MinIO 테스트는 staging MinIO(`:9002`) 에 임시 버킷 `vlm-bench-tmp` 를 만들고 테스트 종료 시 비운다.

## 8. 참고

- 관련 doc: [duckdb-lock-contention-analysis.md](duckdb-lock-contention-analysis.md) (센서의 DuckDB write lock 장기 점유 이슈)
- 원본 실측 run: 2026-04-23 15:59 ~ 2026-04-24 00:41 staging 파이프라인 ingest_job 53회
