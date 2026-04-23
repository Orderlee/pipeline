# Label Studio 연동 — 로직·코드·운영

> VLM 파이프라인에서 Label Studio(LS)를 **검수 UI**로 쓰기 위한 스크립트·Webhook·Dagster 연동을 한곳에 정리합니다.  
> 소스 상세는 각 파일 상단 docstring과 병행해 참고하세요.

---

## 1. 목적과 위치

- **목적**: MinIO에 있는 클립·Gemini 이벤트 JSON을 LS 태스크로 올려 사람이 구간을 수정하고, 수정 결과를 다시 **`vlm-labels` JSON**과 **DuckDB `labels`**에 반영합니다.
- **코드 루트**: `src/gemini/` (LS API·MinIO 직접 호출). Dagster는 **태스크 생성·URL 갱신**만 `src/vlm_pipeline/defs/ls/sensor.py`에서 subprocess로 연결합니다.

---

## 2. 엔드투엔드 흐름

```
[파이프라인] clip → vlm-processed, events → vlm-labels
        │
        ▼
ls_tasks.py create  (또는 Dagster ls_task_create_job)
  · LS project (폴더명 기준) 생성/조회
  · 클립별 task + presigned URL
  · events JSON 있으면 prediction 즉시 attach
  · 신규 project면 Webhook 자동 등록 시도
        │
        ▼
[검수자] LS UI에서 annotation 작성·Submit
        │
        ▼ (매 Submit마다 POST)
ls_webhook.py POST /webhook
  · action ∈ {ANNOTATION_CREATED, ANNOTATION_UPDATED} 만 처리
  · LS API로 미완료 task 수 조회 → 0이면 ls_sync.run() 백그라운드
        │
        ▼
ls_sync.py
  · annotation → MinIO events/*.json 갱신 + DuckDB labels 갱신
        │
        ▼
Slack: /sync-list → /sync-approve {project_id}
  · finalize_labels_in_db → label_status 등 확정
```

**보조 경로**

- **prediction만 나중에 붙이기**: `ls_import.py` (기존 task에 MinIO JSON 기반 prediction 일괄 생성).
- **NAS/PoC 일괄 적재**: `nas_to_ls.py` (MinIO `vlm-processed` 경로 없이 import 버킷에 올린 뒤 LS task 생성).

---

## 3. 파일별 역할

| 파일 | 역할 |
|------|------|
| `src/gemini/ls_tasks.py` | `vlm-processed` 클립 목록 → LS task 생성, presigned URL(기본 7일), `vlm-labels` 이벤트 JSON이 있으면 **prediction 동시 생성**. `renew`로 URL 갱신. **신규 LS project 시 Webhook 등록 시도**. `ls_review_state.json`에 `label_keys` 등 기록. |
| `src/gemini/ls_import.py` | `vlm-labels/*/events/*.json`만 읽어 **이미 있는** 클립 task에 prediction 일괄 부착 (create와 별도 배치용). |
| `src/gemini/ls_sync.py` | LS 프로젝트의 **최신 annotation**을 읽어 해당 클립 stem에 맞는 **MinIO JSON**을 갱신하고 **`labels` 테이블** 업데이트. |
| `src/gemini/ls_webhook.py` | FastAPI: Webhook 수신, 전체 완료 여부 판단, **`ls_sync.run()`** 트리거, Slack 알림·`/sync-approve`로 DuckDB 최종 확정. |
| `src/gemini/nas_to_ls.py` | NAS 로컬 영상·삼성 CNT 형식 JSON → MinIO import 버킷 업로드 → LS task (파이프라인 메인 경로와 독립). |
| `src/vlm_pipeline/defs/ls/sensor.py` | Dagster: `staging_dispatch_requests`에서 dispatch 완료·LS 미생성 행 → **`ls_tasks.py create`**. 일정 **`renew --all-projects`**. |

---

## 4. Webhook 동작 (부하 관점 포함)

- LS에 등록하는 액션: **`ANNOTATION_CREATED`**, **`ANNOTATION_UPDATED`** (`ls_webhook.py`의 `register` JSON).
- **Submit마다** Webhook 서버로 POST가 옵니다.
- 서버는 매번 LS **`GET /api/tasks/?project=...`** 로 페이지네이션하며, `total_annotations == 0`인 task 수를 셉니다. **0이면** “프로젝트 전체에 annotation이 하나 이상 있다”고 보고 `ls_sync`를 실행합니다.
- 즉 “전체 완료” 전용 이벤트가 아니라 **매 제출마다 전체 스캔**으로 완료를 추정하는 구조입니다. 태스크 수가 매우 많으면 LS API 부하가 커질 수 있어, 필요 시 디바운스·집계 API 검토 등이 후속 과제입니다.

---

## 5. 상태 파일 `ls_review_state.json`

- **경로**: 기본 `src/gemini/ls_review_state.json` (환경변수 `LS_STATE_FILE`로 변경 가능).
- **쓰는 쪽**: `ls_tasks.py`의 `update_review_state` (create 시 `label_keys`, `task_count`, `title` 등).
- **읽는 쪽**: `ls_webhook.py`의 `finalize_project` — Slack `/sync-approve` 시 어떤 `labels_key`를 DuckDB에서 `completed` / `finalized`로 올릴지 결정.

Webhook 서버와 태스크 생성 스크립트가 **같은 파일**을 공유하므로, 배포 시 두 프로세스가 동일 경로를 보도록 맞춰야 합니다.

---

## 6. Dagster 연동

- **Job**: `ls_task_create_job` → op `create_ls_tasks`가 `ls_tasks.py`를 subprocess 실행.
- **Sensor**: `ls_task_create_sensor` — DB에서 `status='completed'` AND `ls_task_status`가 pending인 `staging_dispatch_requests`를 찾아 run 요청. **기본 STOPPED**일 수 있으므로 UI에서 ON이 필요할 수 있음.
- **스크립트 경로**: 환경변수 `LS_TASKS_SCRIPT`로 오버라이드 가능 (기본: repo의 `src/gemini/ls_tasks.py`).

---

## 7. 환경 변수·기동 (요약)

| 변수 | 용도 |
|------|------|
| `LS_API_KEY` | LS API 토큰 (필수) |
| `LS_URL` | LS 베이스 URL |
| `WEBHOOK_HOST` | LS가 Webhook POST를 보낼 수 있는 호스트 (컨테이너/호스트 네트워크 고려) |
| `DATAOPS_DUCKDB_PATH` | `ls_sync` / finalize 시 DuckDB 경로 |
| MinIO 관련 | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` 등 (스크립트 기본값은 문서 시점의 개발 기본; 운영은 compose env 따름) |
| Slack | `SLACK_WEBHOOK_URL`, `SLACK_SIGNING_SECRET` (`ls_webhook.py`의 slash command 검증) |

Compose 오버레이·포트(예: `LS_PORT=8084`)는 루트 `CLAUDE.md`의 **Label Studio 연동** 절을 참고합니다.

---

## 8. CLI 빠른 참조

```bash
# Webhook 수신 서버
python src/gemini/ls_webhook.py serve

# 프로젝트별 Webhook 등록 (LS가 이 서버에 POST)
python src/gemini/ls_webhook.py register --project <id>

# 클립 prefix 기준 task 생성 (MinIO vlm-processed + vlm-labels)
python src/gemini/ls_tasks.py create --prefix <prefix>

# presigned URL 갱신
python src/gemini/ls_tasks.py renew --project-name <title>
python src/gemini/ls_tasks.py renew --all-projects

# MinIO JSON → 기존 task에 prediction만
python src/gemini/ls_import.py --project <id> [--prefix ...]

# LS → MinIO + DuckDB 동기화 (수동 실행 시)
python src/gemini/ls_sync.py --project <id> [--prefix ...] [--dry-run]
```

---

## 9. 관련 문서·코드 위치

| 구분 | 경로 |
|------|------|
| 운영 한줄 체크리스트 | 저장소 루트 `CLAUDE.md` → `## Label Studio 연동` |
| Compose | `docker/docker-compose.labelstudio.yaml` |
| DB (staging dispatch) | `staging_dispatch_requests` — `defs/ls/sensor.py` 주석 및 쿼리 |
| 이 문서 | `docs/references/label-studio-integration.md` |

---

## 10. 알려진 설계 메모

- **완료 정의**: 현재는 “모든 task에 `total_annotations >= 1`”에 가깝게 동작합니다. 스킵·리뷰 워크플로우와 맞지 않으면 정책 조정이 필요할 수 있습니다.
- **동시 마지막 제출**: Webhook이 거의 동시에 두 번 오면 `run_sync_and_notify`가 중복 실행될 수 있으므로, idempotent 설계·락은 운영에서 검토합니다.
