# docker prune cron — 디스크 96% 해소 (GHCR 대안)

> stack-candidates §7 GHCR 도입 비권장 결론 후 대안. self-hosted runner 가 build cache 누적시켜 호스트 디스크 96% 도달 — GHCR 도입 대신 cache 정리로 해소.

## 즉시 1회 실행 (긴급)

```bash
# 호스트 (10.0.0.10)
cd /home/user/work_p/Datapipeline-Data-data_pipeline

# 안전 확인 (dry-run)
./scripts/docker_prune.sh --dry-run

# 실제 정리
./scripts/docker_prune.sh
```

**기대 효과** (2026-05-29 측정 시점):
- before: 96% (39G avail)
- builder cache 31.19GB (가장 큰 source) → 20GB 까지만 보존 → ~11GB 회수
- image 14.98GB reclaimable
- 합계 ~26GB 회수 → after ~93% (66GB avail) 정도

추가 정리 필요 시:
- `docker volume ls` 검토 후 unused 만 제거 (운영 데이터 손실 위험 — 수동)
- `du -sh /home/user/work_p/Datapipeline-Data-data_pipeline/docker/data/` (DuckDB 등)
- `du -sh /home/user/.docker/buildx/`

## 주기적 cron 등록

호스트 운영자(`user`) crontab:

```bash
crontab -e
# 추가:
0 3 * * 0 /home/user/work_p/Datapipeline-Data-data_pipeline/scripts/docker_prune.sh >> /var/log/docker_prune.log 2>&1
```

- **매주 일요일 03:00 KST** — pg-backup (02:00) 와 1시간 간격
- 로그: `/var/log/docker_prune.log`
- user 유저가 docker group 소속이라 sudo 불필요

## 안전 검증 (수동)

cron 등록 후 다음 일요일 03:01 에 로그 확인:

```bash
tail -50 /var/log/docker_prune.log
docker system df
docker ps -a --filter status=running | wc -l   # 정상 컨테이너 수 변화 없어야 함
```

## 무엇을 정리하나 / 안 하나

| 대상 | 정리 여부 | 이유 |
|------|----------|------|
| dangling images (tagless) | ✅ | 위험 0 |
| age > 168h unreferenced images | ✅ | 운영 image 재사용 흔적 없음 |
| builder cache > 20GB (`--keep-storage 20GB`) | ✅ | layer cache 효과 보존 + 큰 회수 |
| **volumes** | ❌ | 실수 시 DB / 마이그레이션 상태 손실 |
| **stopped containers** | ❌ | 운영자가 디버깅용 stop 한 케이스 보호 |

## 운영 영향

- 다음 deploy 시 빌드 시간 약간 ↑ 가능 (cache 일부 회수) — but 20GB 보존으로 핵심 layer 는 유지
- prod 컨테이너 0 영향 (running 컨테이너의 image 는 prune 대상 외)
- pg-backup snapshot 0 영향 (volume mount)

## 변경 이력

- 2026-05-29: GHCR 도입 비권장 결론 후 대안으로 도입
