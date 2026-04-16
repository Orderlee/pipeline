# Label Studio 운영 가이드

> Production MinIO → Label Studio 연동 운영 절차

---

## 아키텍처 요약

```
MinIO (vlm-processed | vlm-raw)
    ↓ --auto-detect (비디오 우선, 이미지 fallback)
    ↓ presigned URL
Label Studio (media_type별 label_config로 project/task 생성)
    ↓ annotation 완료
ls-webhook (annotation 수신)
    ↓
MinIO (vlm-labels/*/events/*.json 업데이트)
DuckDB (labels 테이블 업데이트)
```

**지원 버킷 & 미디어 타입:**

| 버킷 | 용도 | prefix 규칙 | 미디어 |
|------|------|-------------|--------|
| `vlm-processed` | Gemini 라벨링 후 클립/프레임 | `{folder}/clips` | 비디오, 이미지 |
| `vlm-raw` | 급한 원본 영상/이미지 | `{folder}` 그대로 | 비디오, 이미지 |

**자동화 경로:**
1. dispatch 완료 → `dispatch_requests.status='completed'`, `bucket` 컬럼에 대상 버킷 기록
2. `ls_task_create_sensor` 감지 → `ls_tasks.py create --bucket <bucket> --auto-detect` 실행
3. 미디어 타입 자동 감지 → media_type별 label_config로 LS project/task 생성
4. 비디오이면 Gemini prediction 자동 첨부 (vlm-labels JSON 참조)
5. 라벨러가 annotation 완료 → webhook으로 `ls_sync` 호출
6. MinIO JSON + DuckDB labels 동기화

---

## 기동 절차

### 1. 사전 조건

- 메인 Docker Compose가 실행 중이어야 합니다 (postgres, pipeline-network)
- `docker/.env`에 LS 관련 환경변수가 설정되어 있어야 합니다

### 2. 환경변수 설정

`docker/.env`에 아래 값을 추가합니다:

```bash
# === Label Studio ===
LS_API_KEY=<LS 계정 Settings → Access Token>
LS_PORT=8084
WEBHOOK_HOST=ls-webhook
LS_WEBHOOK_PORT=8003
LS_TASK_SENSOR_INTERVAL_SEC=60

# (선택) Slack 알림
SLACK_WEBHOOK_URL=
SLACK_SIGNING_SECRET=
```

> `LS_API_KEY`는 LS 첫 기동 후 계정 생성 → 토큰 발급 → 다시 세팅하는 순서입니다.

### 3. Compose 기동

```bash
cd docker

# postgres + 네트워크 먼저
docker compose up -d postgres

# LS + webhook 기동 (메인 이미지 datapipeline:gpu-cu124 가 로컬에 있어야 함)
docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml up -d

# webhook 없이 LS·DB만 먼저 띄우기 (이미지 pull 실패 시)
docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml up -d postgres labelstudio
```

`ls-webhook`은 `image: datapipeline:gpu-cu124`(로컬 빌드)를 씁니다. Docker Hub에 없어 `pull access denied`가 나면 저장소 루트에서 `docker compose -f docker/docker-compose.yaml build` 등으로 이미지를 만든 뒤 다시 `up` 하세요.

### 4. LS 초기 설정

1. `http://<HOST>:8084` 접속
2. 계정 생성 (signup)
3. 우측 상단 → Account & Settings → **Access Token** 복사
4. `docker/.env`에 `LS_API_KEY=<토큰>` 세팅
5. `ls-webhook` 컨테이너 재시작:
   ```bash
   docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml restart ls-webhook
   ```

### 5. Dagster sensor 활성화

Dagster UI (`http://<HOST>:3030`) → Sensors → `ls_task_create_sensor` → **ON**

---

## 운영 시나리오

### A. 파이프라인 자동 흐름 (vlm-processed) — 수동 작업 없음

```
영상 수집 → Gemini 라벨링 → 클립 절단 → vlm-processed 저장
    → dispatch_requests (status='completed', bucket='vlm-processed')
    → ls_task_create_sensor 자동 감지
    → ls_tasks.py --bucket vlm-processed create --prefix {folder}/clips --auto-detect
    → LS 프로젝트 + task 자동 생성 + Gemini prediction 첨부
```

### B. 급한 원본 영상 (vlm-raw) — dispatch 수동 등록

파이프라인을 거치지 않는 raw 영상은 dispatch request를 수동 등록하면 sensor가 자동 처리합니다.

```bash
# DuckDB에 dispatch request 삽입
python3 scripts/query_local_duckdb.py --sql "
  INSERT INTO dispatch_requests (request_id, folder_name, bucket, status, ls_task_status)
  VALUES ('manual-$(date +%s)', 'S-OIL/falldown', 'vlm-raw', 'completed', 'pending')
"
```

또는 sensor 없이 직접 실행:

```bash
python src/gemini/ls_tasks.py \
  --bucket vlm-raw \
  create --prefix S-OIL/falldown --auto-detect
```

### C. 이미지 데이터

비디오가 없는 prefix에서는 `--auto-detect`가 이미지로 자동 fallback합니다.
이미지용 label config (`<Image>` + `<Choices>` + `<RectangleLabels>`)가 적용됩니다.

```bash
python src/gemini/ls_tasks.py \
  --bucket vlm-processed \
  create --prefix vanguardhealthcarevhc/falldown/image --auto-detect
```

---

## 프로젝트 명명 규칙

`--auto-detect` 모드에서는 `build_project_name()`이 prefix로부터 **고객사명 포함** 프로젝트명을 생성합니다.

| prefix | 생성되는 프로젝트명 |
|--------|---------------------|
| `vanguardhealthcarevhc/falldown/clips` | `vanguardhealthcarevhc__falldown` |
| `hyundai_v2/01_27_collection_data` | `hyundai_v2__01_27_collection_data` |
| `S-OIL/smoke` | `S-OIL__smoke` |

`clips`, `image`, `images`, `frames`, `video`, `videos` 같은 구조 디렉토리명은 자동 건너뜁니다.

---

## Presigned URL 갱신

MinIO presigned URL의 기본 유효 기간은 **7일**입니다.

### 자동 갱신 (schedule)

`ls_presign_renew_schedule`이 **매일 05:00 KST**에 실행되어
만료 1일 이내인 URL을 자동 갱신합니다.

Dagster UI → Schedules에서 확인 가능합니다.

### 수동 갱신

```bash
# 특정 프로젝트
python src/gemini/ls_tasks.py renew --project-name <프로젝트명>

# 모든 프로젝트
python src/gemini/ls_tasks.py renew --all-projects
```

---

## Webhook 관리

### 자동 등록

`ls_tasks.py create`로 **새 프로젝트 생성 시** webhook이 자동 등록됩니다.
`WEBHOOK_HOST`가 올바르게 설정되어 있어야 합니다.

### 수동 등록/확인

```bash
# 특정 프로젝트에 webhook 등록
python src/gemini/ls_webhook.py register --project <project_id>

# 등록된 webhook 목록
python src/gemini/ls_webhook.py list
```

---

## 라벨 동기화 (ls_sync)

annotation 완료 시 webhook이 자동으로 `ls_sync`를 호출합니다.

### 수동 동기화

```bash
python src/gemini/ls_sync.py --project <project_id> --api-key <token>

# dry-run (변경사항만 확인)
python src/gemini/ls_sync.py --project <project_id> --api-key <token> --dry-run
```

### DuckDB lock 에러 대응

`ls_sync`는 DuckDB write 시 exponential backoff로 자동 재시도합니다 (최대 5회).
Dagster run과 동시 실행되어도 대부분 자동 복구됩니다.

---

## Slack 연동 (선택)

`SLACK_WEBHOOK_URL`과 `SLACK_SIGNING_SECRET` 설정 시:

- annotation 완료 시 Slack 알림 발송
- Slash command 사용 가능:
  - `/sync-list` — 확정 대기 중인 project 목록
  - `/sync-approve <project_id>` — label_status='completed' 확정

---

## Labeling Interface

`--auto-detect` 모드에서는 감지된 media_type에 따라 label config가 자동 선택됩니다.

### 비디오 (media_type="video")

```xml
<View>
  <TimelineLabels name="videoLabels" toName="video">
    <Label value="fall" background="#e74c3c"/>
    <Label value="fight" background="#e67e22"/>
    <Label value="smoke" background="#95a5a6"/>
    <Label value="fire" background="#e74c3c"/>
    <Label value="unsafe_act" background="#f39c12"/>
  </TimelineLabels>
  <Video name="video" value="$video" timelineHeight="120" />
</View>
```

### 이미지 (media_type="image")

```xml
<View>
  <Image name="image" value="$image" />
  <Choices name="imageClass" toName="image" choice="multiple">
    <Choice value="fall" />
    <Choice value="fight" />
    <Choice value="smoke" />
    <Choice value="fire" />
    <Choice value="unsafe_act" />
  </Choices>
  <RectangleLabels name="bbox" toName="image">
    <Label value="fall" background="#e74c3c"/>
    <Label value="fight" background="#e67e22"/>
    <Label value="smoke" background="#95a5a6"/>
    <Label value="fire" background="#e74c3c"/>
    <Label value="unsafe_act" background="#f39c12"/>
  </RectangleLabels>
</View>
```

라벨 종류 변경: `src/gemini/ls_tasks.py`의 `_label_config_for_media()` 수정
또는 LS UI → Project Settings → Labeling Interface에서 직접 수정

---

## 트러블슈팅

| 증상 | 원인 | 해결 |
|------|------|------|
| `pull access denied for datapipeline` | `ls-webhook`이 로컬 전용 이미지 사용 | `docker compose -f docker/docker-compose.yaml build` 후 `up`, 또는 `up -d postgres labelstudio`만 실행 |
| `pipeline-labelstudio-1` 없음 / 접속 불가 | 파이프라인 스택이 내려가 있음 | `docker compose … up -d postgres labelstudio` 또는 전체 스택 기동 |
| `OperationalError: [Errno -3] Try again` (Postgres) | LS가 Postgres와 다른 Docker 네트워크에만 붙음 | `docker-compose.labelstudio.yaml`에서 `labelstudio`가 `default`+`pipeline-network` 모두 사용하는지 확인, `POSTGRE_HOST=postgres` |
| LS에서 영상/이미지가 안 보임 | presigned URL 만료 | `ls_tasks.py renew --project-name <name>` |
| webhook annotation 미수신 | webhook 미등록 또는 WEBHOOK_HOST 오류 | `ls_webhook.py list`로 확인, 필요시 `register` |
| "database is locked" 에러 | Dagster와 동시 DuckDB write | 자동 재시도 대기 (최대 30초). 지속 시 Dagster run 확인 |
| sensor가 LS task를 안 만듦 | sensor STOPPED 상태 | Dagster UI에서 `ls_task_create_sensor` ON |
| LS_API_KEY 오류 | 토큰 만료 또는 미설정 | LS UI → Settings → 새 토큰 발급 |
| `--auto-detect`인데 미디어 없음 | prefix 경로 오류 또는 파일 미업로드 | MinIO 콘솔에서 버킷/prefix 확인 |
| vlm-raw task가 자동 생성 안 됨 | dispatch_requests에 수동 등록 안 함 | 위 **시나리오 B** 참고하여 DB insert |

---

## 중복 방지

`ls_tasks.py create`는 기존 task의 media stem을 인덱싱하여 **이미 등록된 파일은 자동 skip** 합니다.
같은 prefix로 여러 번 실행해도 중복 task가 생기지 않으므로 안전합니다.

---

## CLI 레퍼런스

```bash
# vlm-processed 클립 (파이프라인 자동 흐름과 동일)
python src/gemini/ls_tasks.py create --prefix hyundai_v2/01_27_collection_data

# vlm-raw 원본 영상 (auto-detect)
python src/gemini/ls_tasks.py --bucket vlm-raw create --prefix S-OIL/falldown --auto-detect

# vlm-processed 이미지 (auto-detect)
python src/gemini/ls_tasks.py create --prefix vanguardhealthcarevhc/falldown/image --auto-detect

# 특정 프로젝트 URL 갱신
python src/gemini/ls_tasks.py renew --project-name S-OIL__smoke

# 전체 프로젝트 URL 갱신
python src/gemini/ls_tasks.py renew --all-projects
```

---

## 관련 파일

| 파일 | 역할 |
|------|------|
| `src/gemini/ls_tasks.py` | task 생성, presigned URL 갱신, `--auto-detect` 미디어 감지 |
| `src/gemini/ls_webhook.py` | webhook 수신 서버, Slack 연동 |
| `src/gemini/ls_sync.py` | annotation → MinIO/DuckDB 동기화 |
| `src/gemini/ls_import.py` | prediction 수동 import |
| `src/vlm_pipeline/defs/ls/sensor.py` | Dagster sensor/schedule, `--bucket` 동적 분기 |
| `src/vlm_pipeline/resources/duckdb_ingest_dispatch.py` | dispatch_requests 테이블 (bucket 컬럼 포함) |
| `docker/docker-compose.labelstudio.yaml` | LS + webhook Docker 설정 |
