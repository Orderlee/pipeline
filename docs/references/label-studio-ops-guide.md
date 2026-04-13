# Label Studio 운영 가이드

> Production MinIO → Label Studio 연동 운영 절차

---

## 아키텍처 요약

```
MinIO (vlm-processed/*/clips/*.mp4)
    ↓ presigned URL
Label Studio (task 생성)
    ↓ annotation 완료
ls-webhook (annotation 수신)
    ↓
MinIO (vlm-labels/*/events/*.json 업데이트)
DuckDB (labels 테이블 업데이트)
```

**자동화 경로:**
1. `dispatch_stage_job` 완료 → `staging_dispatch_requests.status='completed'`
2. `ls_task_create_sensor` 감지 → `ls_tasks.py create` 실행
3. LS presigned URL로 task 생성 + Gemini prediction 자동 첨부
4. 라벨러가 annotation 완료 → webhook으로 `ls_sync` 호출
5. MinIO JSON + DuckDB labels 동기화

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

# LS + webhook 기동
docker compose -f docker-compose.yaml -f docker-compose.labelstudio.yaml up -d
```

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

프로젝트 자동 생성 시 아래 template이 적용됩니다:

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

라벨 종류 변경: `src/gemini/ls_tasks.py`의 `_default_label_config()` 수정
또는 LS UI → Project Settings → Labeling Interface에서 직접 수정

---

## 트러블슈팅

| 증상 | 원인 | 해결 |
|------|------|------|
| LS에서 영상이 안 보임 | presigned URL 만료 | `ls_tasks.py renew --project-name <name>` |
| webhook annotation 미수신 | webhook 미등록 또는 WEBHOOK_HOST 오류 | `ls_webhook.py list`로 확인, 필요시 `register` |
| "database is locked" 에러 | Dagster와 동시 DuckDB write | 자동 재시도 대기 (최대 30초). 지속 시 Dagster run 확인 |
| sensor가 LS task를 안 만듦 | sensor STOPPED 상태 | Dagster UI에서 `ls_task_create_sensor` ON |
| LS_API_KEY 오류 | 토큰 만료 또는 미설정 | LS UI → Settings → 새 토큰 발급 |

---

## 관련 파일

| 파일 | 역할 |
|------|------|
| `src/gemini/ls_tasks.py` | task 생성, presigned URL 갱신 |
| `src/gemini/ls_webhook.py` | webhook 수신 서버, Slack 연동 |
| `src/gemini/ls_sync.py` | annotation → MinIO/DuckDB 동기화 |
| `src/gemini/ls_import.py` | prediction 수동 import |
| `src/vlm_pipeline/defs/ls/sensor.py` | Dagster sensor/schedule |
| `docker/docker-compose.labelstudio.yaml` | LS + webhook Docker 설정 |
