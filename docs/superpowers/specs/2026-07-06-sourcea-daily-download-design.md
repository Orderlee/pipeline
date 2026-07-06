# SourceA 일일 수집 Dagster 스케줄 — 설계

**날짜**: 2026-07-06
**상태**: 승인됨 (설계 리뷰 대기)
**요청**: `data_download` 폴더의 SourceA 수집 작업을 Dagster로 매일 06:00에 시작해 07:00 전에 끝나게 한다. 이 작업은 Tailscale이 켜져 있어야 한다. NAS 200TB는 현재 다운 상태(2026-07-06 네트워크 개편)이므로 "동작한다는 가정" 하에 코드를 완성하고, NAS 복구 후 검증한다. 구현은 haiku/sonnet/opus/fable/codex에 페르소나(CTO·팀장·과장·대리·네트워크 전문가)를 부여해 분담한다. `data_download` 폴더는 적절한 이름으로 변경한다.

## 배경

`data_download/`는 SourceA 경찰서 사이트(Tailscale 경유 `macs-sourcea`)의 이벤트 미디어 수집 툴킷이다.

| 파일 | 성격 |
|---|---|
| `download_classify.py` | **주기 작업 대상.** MinIO(thumbnail/video 버킷) → 로컬 미러 + DB `camera_events` 기반 by_category 분류. 파일 단위 skip-existing, 원자적 쓰기(`.part`→rename) |
| `main.py` | 윈도우 지정 리포트 생성 (주 단위, 수동 유지) |
| `classify_vlm.py` `frame_match.py` `assemble.py` `finalize_unparsed.py` `verify_refetch.py` | DB-less 과거 날짜 복구용 일회성 (제외) |

기존 수동 미러는 `~/sourcea_mirror.sh` (mcli, 호스트)로 수행 중이며 목적지는 `/home/user/mou/nas_200tb/sourcea/`.

## 결정 사항 (Q&A)

1. **작업 범위**: 다운로드 + DB 분류 (`download_classify` 포팅). 리포트는 수동 유지.
2. **Tailscale 관리**: 호스트 cron으로 05:55 `tailscale up` / 07:05 `tailscale down` 구성 (이번 작업에 포함).
3. **폴더명**: `data_download` → `site_reports` (멀티사이트 리포트/복구 툴킷 성격, untracked 유지).

## 아키텍처

`gcs_download_schedule`(매일 04:00, 외부 수집) 선례를 그대로 따른다:
asset → `define_asset_job` → `ScheduleDefinition` → `definitions_production.py` 배선.

### 1. Dagster asset — `src/vlm_pipeline/defs/ingest/sourcea_download.py` (신규)

단일 asset `sourcea_site_download`:

1. **Preflight (graceful skip)** — 다음 중 하나라도 실패하면 에러 없이 skip 후 로그만 남긴다 (NFS 장애 대응 규칙과 동일 패턴):
   - MinIO `SOURCEA_MINIO_HOST:9000` TCP 연결 (timeout 5s) — Tailscale off/사이트 다운 감지
   - 목적지 `/nas/data/sourcea` 부모(`/nas/data`) 마운트 존재 — NAS 미복구 감지
2. **다운로드** — 최근 `SOURCEA_LOOKBACK_DAYS`(기본 14)일의 날짜 폴더를 MinIO에서 열거, thumbnail/video 전 객체를 파일 단위 incremental로 미러. 기존 원자적 쓰기(`.part`→rename)와 skip-existing(>0 byte) 유지 → 마감 강제 종료에도 안전하고 다음날 이어받기 가능. 날짜 폴더 단위 skip(기존 `existing_dates`)은 **사용하지 않는다** — 부분 다운로드된 날짜가 영구 누락되는 갭이 있기 때문.
3. **자체 마감(deadline)** — `SOURCEA_DEADLINE`(기본 `06:55`, Asia/Seoul) 도달 시 신규 다운로드 제출을 중단하고 진행 중 파일만 마무리 후 정상 종료. `dagster/max_runtime` 태그는 이 배포에 run monitoring이 없어 동작하지 않으므로 in-asset 방식이 유일하게 확실하다.
4. **분류** — DB `camera_events`의 `event_name` 기반으로 `by_category/<cat>/{thumbnail,video}/<date>/` 복사 (기존 로직 그대로). DB에 이벤트가 남아 있는 날짜에만 유효(기존 동작과 동일).

메타데이터로 다운로드/분류/skip 카운트를 남긴다. DuckDB/Postgres 미접근 → `duckdb_writer` 태그 불필요.

### 2. 스케줄

```python
ScheduleDefinition(
    name="sourcea_download_schedule",
    cron_schedule="0 6 * * *",
    execution_timezone="Asia/Seoul",
    default_status=DefaultScheduleStatus.RUNNING,  # graceful skip 덕에 NAS 복구 전에도 안전
)
```

### 3. 설정 / 시크릿 (`.env`, git 미추적)

| 키 | 값 (prod) | 비고 |
|---|---|---|
| `SOURCEA_MINIO_HOST` | `10.0.0.21` | tailnet IP. 컨테이너 안에서 MagicDNS(`*.internal`) 미해석 → IP 필수 |
| `SOURCEA_MINIO_PORT` | `9000` | |
| `SOURCEA_DB_HOST` / `SOURCEA_DB_PORT` | `10.0.0.21` / `3306` | 동일 호스트 |
| `SOURCEA_DB_USER` / `SOURCEA_DB_PASS` / `SOURCEA_DB_NAME` | (기존 config.py 값) / `source-c2stage` | 평문 creds는 repo에 커밋 금지 |
| `SOURCEA_DEST` | `/nas/data/sourcea` | 컨테이너 경로 = 호스트 `/home/user/mou/nas_200tb/sourcea`. staging은 `NAS_DATA_ROOT`가 `staging/` 하위라 자동 격리 |
| `SOURCEA_LOOKBACK_DAYS` | `14` | |
| `SOURCEA_DEADLINE` | `06:55` | |

env 미설정(예: staging `.env.test`에 키 없음) 시 asset은 graceful skip.

### 4. 의존성

`pymysql`을 `docker/app/requirements.txt`에 추가 → CI가 이미지 재빌드를 트리거한다. 다운로드는 stdlib(`urllib`)만 사용.

### 5. 호스트 Tailscale cron (repo 밖, 호스트 작업)

```cron
55 5 * * * /usr/bin/tailscale up
5  7 * * * /usr/bin/tailscale down
```

root crontab에 설치 (tailscale은 root 권한 필요). user의 sudo 권한 확인 후 설치하며, 불가 시 사용자에게 설치 명령을 전달한다.

### 6. 폴더 rename — `data_download` → `site_reports`

- `mv data_download site_reports`. untracked 유지 (config.py에 평문 creds → 커밋 금지).
- 포팅되는 `download_classify.py`는 폴더 안에 deprecated 주석으로 표기(포팅 위치 안내).
- 폴더 내 `.venv`는 rename 후 pip 콘솔 스크립트(절대경로 shebang)만 깨짐 — `.venv/bin/python main.py` 실행은 정상. 필요 시 venv 재생성 안내만.

### 7. 테스트 — `tests/unit/test_sourcea_download.py`

mocked MinIO XML 응답 + mocked pymysql (repo 표준: `unittest.mock`):

- lookback 날짜 선택 로직 (오늘 기준 N일)
- 마감시간 도달 시 신규 제출 중단·정상 종료
- preflight 실패(TCP/마운트/env 부재) 시 graceful skip
- 분류 매핑 (event_name → by_category 경로)
- 부분 파일(`.part`, 0-byte) 재다운로드 대상 판정

## 에러 처리

- per-file fail-forward: 개별 객체 실패는 로그 후 계속 (repo 정책).
- MinIO/DB 순단: 다운로드는 파일 단위 재시도(기존 3회), DB 연결 실패 시 분류만 skip하고 다운로드 결과는 보존.
- 07:00 하드킬(외부 요인)에도 원자적 쓰기라 손상 파일 없음.

## 배포 & 검증 계획

1. `feature/sourcea-daily-download` → PR → `dev` (staging 자동 배포; staging은 env 키 부재로 skip만 tick 확인)
2. `dev` → `main` (prod 배포는 dagster 재가동을 유발하므로 라벨링 유휴 시간대 권장)
3. **NAS 복구 후 검증** (현재 불가한 가정들):
   - tailscale up 상태에서 dagster 컨테이너(bridge) → `10.0.0.21:9000` 도달 확인 (`docker exec … python -c "socket.create_connection(...)"`)
   - 06:00 tick 후 `/nas/data/sourcea/{thumbnail,video}/<date>/` 산출물 및 run 메타데이터 확인
   - 07:00 전 종료 확인 (run 소요시간)

## 역할 분담 (구현 단계)

| 페르소나 | 모델/에이전트 | 담당 |
|---|---|---|
| CTO | Fable (메인 세션) | 설계 결정, 작업 분배, 통합, 최종 검증·커밋 |
| 팀장 · 백엔드 아키텍트 | Opus | 구현 계획 리뷰, 아키텍처 리스크 점검 |
| 과장 · Dagster/Python 전문가 | Sonnet (`dagster-impl`) | asset/schedule/테스트 구현 |
| 대리 · Python 주니어 | Haiku | 폴더 rename 후속 정리, deprecated 표기, env 문서화 |
| 네트워크 전문가 · 외부 자문 | Codex (`codex` 에이전트) | tailscale 라우팅·타임아웃·재시도 코드 리뷰 |

## 기각한 대안

- **호스트 cron으로 기존 스크립트 직접 실행** — 가장 단순하나 Dagster 가시성(run 이력, 메타데이터, 재실행) 요구에 반함.
- **Dagster가 호스트 미러 스크립트 호출** — 컨테이너에 docker.sock 미마운트로 불가.
- **`dagster/max_runtime` 태그로 07:00 종료** — dagster.yaml에 run monitoring 미설정으로 무효.
