# GenAI Studio — 작동 원리 & 사용 가이드

> 이미지/텍스트로 영상·이미지를 생성하는 멀티엔진 스튜디오. 결과물은 NAS 의 **격리
> 경로**(`<GENAI_NAS_INCOMING>/genai/...`, 기본 `/nas/data/genai_studio/incoming`)에
> 저장되고 **Studio UI 다운로드**로 수령한다.
>
> ⚠️ **현재 학습 파이프라인(MinIO `vlm-raw` + `raw_files`) 자동 적재는 비활성**이다.
> genai provenance manifest(`source_type=genai_output` / `label_policy=none`)가 ingest
> sensor 에 아직 연결되지 않아, auto-bootstrap 이 결과를 일반 camera 미디어로 잘못 줍어
> 학습셋을 오염시키는 것을 막기 위해 출력 경로를 auto-bootstrap 스캔 밖으로 격리했다.
> (자세한 흐름 §부록 C, provenance bridge 는 follow-up.)
>
> 이 문서는 **Part A(사용자)** + **Part B(운영자)** + **부록(레퍼런스)** 로 구성.
> 코드 기준 (`docker/genai/`, `src/vlm_pipeline/defs/genai/`, `scripts/genai_cli/`).

---

## ⚠️ 먼저 읽을 주의사항

| 레벨 | 항목 |
|------|------|
| 🔴 DANGER | Kling `model_name` 은 **API 식별자만** 허용 (`kling-v2-6` 등). `kling-video-o1` 같은 UI/마케팅 이름은 **즉시 1201 거부**. |
| 🔴 DANGER | `GENAI_AUTH_DISABLED=true` 는 **무인증 노출** — 개발 전용, 프로덕션 금지. |
| 🟠 WARNING | 비동기 엔진(Kling/Veo/Higgsfield) 결과는 **Dagster `genai_poll_sensor` + `GENAI_INTERNAL_TOKEN`** 가 있어야 완료됨. 토큰 미설정 시 영구 `running` stuck. |
| 🟠 WARNING | **Veo 는 기본 `GENAI_ENGINES_ENABLED` 에 없음** — 쓰려면 env 에 명시 추가. |
| 🟠 WARNING | **자격증명 미설정 = mocking 모드(무음)** — submit 은 성공하고 placeholder(40byte mp4/빈 png) 를 반환. `/healthz` 로는 감지 불가. |
| 🟠 WARNING | Veo 컨테이너 재시작 시 in-flight 작업의 in-memory tracker 가 사라짐 — REST fallback 으로 대부분 복구되나 보장은 아님. |

---

# Part A — 사용자

## 1. 개요

이미지 1장(또는 텍스트)을 입력하면 5개 엔진 중 하나로 영상/이미지를 생성한다.

### 엔진 매트릭스

| 엔진 | 출력 | 모드 | 입력→출력 | 자격증명 | 비고 |
|------|------|------|-----------|----------|------|
| **kling** | video(.mp4) | 비동기 | image→video | `KLING_ACCESS_KEY` / `KLING_SECRET_KEY` | 플랜별 동시 한도(현재 5) |
| **veo** | video(.mp4) | 비동기 | image→video, **text→video** | `GEMINI_GOOGLE_APPLICATION_CREDENTIALS` | 기본 enable 목록에 없음 |
| **higgsfield** | video(.mp4) | 비동기 | image→video | `FAL_KEY` | fal.ai queue |
| **nanobanana** | image(.png) | **동기** | image→image | `GEMINI_GOOGLE_APPLICATION_CREDENTIALS` | `gemini-2.5-flash-image` |
| **gpt_image** | image(.png) | **동기** | image→image | `OPENAI_API_KEY` | `gpt-image-1` |

- **동기(sync)**: submit 시점에 결과 즉시 반환 → 바로 finalize.
- **비동기(async)**: submit 후 provider_job_id 만 받고, sensor 가 폴링해 완료 처리.

## 2. 빠른 시작

```
URL : http://10.0.0.10:8088/   (staging)
인증: Basic Auth — GENAI_BASIC_AUTH_USER / GENAI_BASIC_AUTH_PASS
```

1. 접속 → **Submit** 탭
2. 엔진 선택 (예: kling)
3. 이미지 1장 업로드 + 프롬프트 입력
4. **생성 시작**
5. **Batches** 탭에서 결과 확인 (1–2분 후 done)

## 3. UI 사용법

상단 네비: **Submit · Bulk · Batches · Costs**

### Submit (`/`) — 단건/소량
- 탭: Image→Video / Text→Video(veo 전용) / Image→Image
- 엔진 카드 + 엔진별 옵션(model/mode/duration/aspect)
- 이미지 dropzone (드래그·여러 장 누적, 최대 20장)
- **1 프롬프트 + N 이미지 = 1 batch · N jobs** (모든 이미지 같은 프롬프트)

### Bulk (`/genai/bulk`) — 대량 N×M
- prompts textarea(한 줄 1개) + 이미지 + pair mode
  - **paired** (1:1, 이미지수==프롬프트수)
  - **cartesian** (N×M 전조합)
- 우측 실시간 preview: batches/jobs/비용(Veo만 추정)/한도
- 한도 **`GENAI_MAX_BULK_JOBS`=25** 초과 시 차단 → CLI 사용
- 같은 제출은 `bulk_group_id` 로 묶임

### Batches (`/genai/batches`) — 목록·필터·결과
- chip 필터: engine / status / group(bulk_group_id)
- 상세 페이지: job별 status + **영상 인라인 재생 + 다운로드** + 실패 job **retry** 버튼

### Costs (`/genai/costs`) — 비용/활동 집계

## 4. CLI 사용법

CLI 진입점은 `scripts/genai-cli.sh` (thin HTTP 클라이언트).

```bash
# 자격증명: env 또는 config
export GENAI_CLI_USER=user GENAI_CLI_PASS='...' GENAI_CLI_BASE=http://10.0.0.10:8088
# 또는: ./scripts/genai-cli.sh config init

./scripts/genai-cli.sh engines                 # 활성 엔진 / health
./scripts/genai-cli.sh submit kling --image cat.png --prompt "slow pan"
./scripts/genai-cli.sh submit veo --text-only --prompt "drone over city" --duration 8 --wait
./scripts/genai-cli.sh submit nanobanana --images-from-dir ./imgs/ --prompt "$(cat p.txt)"
./scripts/genai-cli.sh batches list --engine kling --status failed
./scripts/genai-cli.sh batches show <batch_id>
./scripts/genai-cli.sh wait <batch_id> --timeout 900
./scripts/genai-cli.sh jobs retry <job_id>
./scripts/genai-cli.sh costs --range week
```

서브커맨드: `submit · bulk-submit · batches · wait · jobs · costs · engines · config`
종료 코드: `0`=성공/dry-run, `1`=사용자오류, `2`=실패, `3`=partial, `4`=timeout

### 셸 래퍼 (옵션 미리 적어두고 실행)
```bash
# 단건 — 파일 안의 ENGINE/IMAGE/PROMPT 등 수정 후 실행
vi scripts/run-batch.sh && ./scripts/run-batch.sh
#   IMAGE="" + ENGINE=veo → txt2video 자동
#   IMAGE=디렉토리 → 그 안 전체 이미지

# bulk — dry-run 먼저, 확인 후 CONFIRM=true
vi scripts/run-bulk.sh
./scripts/run-bulk.sh                  # dry-run (plan/비용 미리보기)
CONFIRM=true ./scripts/run-bulk.sh     # 실제 제출
```

### bulk-submit (CLI)
```bash
# manifest 또는 편의 입력. 항상 dry-run 먼저.
./scripts/genai-cli.sh bulk-submit --prompts-file p.json --images-from-dir ./imgs \
    --engine kling --pair-mode paired --dry-run
#   확인 후 --confirm
```
manifest 형식:
```json
{ "engine": "veo", "options": {"duration":"8","aspect_ratio":"9:16"},
  "pairs": [ {"image":"a.png","prompt":"..."}, {"image":null,"prompt":"txt2video"} ] }
```

## 5. 시나리오

| 시나리오 | 도구 | 방법 |
|----------|------|------|
| 1 프롬프트 × N 이미지 | Submit | 이미지 N장 + 프롬프트 1개 |
| N 프롬프트 × N 이미지 (1:1) | Bulk | paired, 개수 일치 |
| N 프롬프트 × M 이미지 (전조합) | Bulk | cartesian |
| 텍스트만 → 영상 | Submit/Bulk | **veo + Text→Video / `--text-only`** |

> txt2video 는 **Veo 전용**. 다른 엔진에 `--text-only` 주면 거부됨.

## 6. 사용자가 자주 만나는 오류

| 메시지 | 원인 | 조치 |
|--------|------|------|
| `1201 model is not supported` | 잘못된 Kling model_name (UI 이름) | `kling-v2-6` 등 API 식별자 사용 |
| `1303 parallel task over resource pack limit` | Kling 동시 한도 초과 | **자동 deferred → sensor 재제출**. 그냥 기다리면 됨 |
| `429 Account balance not enough` | Kling API credit 소진 | API 요금제/크레딧 충전 |
| Veo `safety filter 차단 (RAI)` | 폭력/무기/화재 등 정책 | 프롬프트/이미지 완화 (비용 청구 X) |
| 결과가 안 나옴 (running 유지) | sensor OFF 또는 토큰 미설정 | 운영자에게 (Part B §센서) |
| 결과 mp4 가 40byte | 자격증명 없어 mocking | 운영자에게 (자격증명 확인) |

---

# Part B — 운영자

## 7. 배포 & 컨테이너

- genai 컨테이너는 compose **profile `genai`** — **자동 시작 안 됨**, deploy-stack.sh 도 안 건드림(dagster/postgres만).
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline_test/docker
docker compose --env-file .env.test -p pipeline-test --profile genai up -d genai
# 코드 변경 반영: 위에 --build 추가
```
- 포트 `:8088`, 이미지 `datapipeline-genai:0.1`
- 볼륨: `/home/user/mou/staging:/nas/staging` (결과물 NAS)
- 헬스: `GET /healthz` → `{status:ok, engines:[...]}`

## 8. 인증 & 보안

| 용도 | 변수 | 비고 |
|------|------|------|
| UI Basic Auth | `GENAI_BASIC_AUTH_USER` / `GENAI_BASIC_AUTH_PASS` | 미설정 + `GENAI_AUTH_DISABLED`≠true → 503 |
| 인증 우회 | `GENAI_AUTH_DISABLED=true` | 🔴 개발 전용 |
| Dagster↔genai 내부 API | `GENAI_INTERNAL_TOKEN` | `/internal/*` 호출 토큰. 미설정 시 sensor가 SkipReason만 |

비번 변경: `.env.test` (git 미추적) 의 `GENAI_BASIC_AUTH_PASS` 수정 → `up -d genai`.

## 9. 센서 · 동시성 · 복구

### genai_poll_sensor (Dagster, default RUNNING, 30s)
매 tick 3가지 수행:
1. **submit-pending** — deferred(pending) 작업을 동시성 한도 안에서 제출(drain)
2. **reconcile-stale** — 모든 job 끝났는데 batch가 running 인 것 보정
3. **poll pending** — 비동기 job 상태 확인 → done 시 finalize

```bash
# 상태 확인 (RUNNING 이어야)
curl -s -G http://10.0.0.10:3031/graphql --data-urlencode \
 'query={repositoriesOrError{... on RepositoryConnection{nodes{sensors{name sensorState{status}}}}}}'
# 수동 ON (GraphQL startSensor) — 보통 default RUNNING 이라 불필요
```

### Kling 동시성 게이트
- `engine_max_concurrent`: kling 기본 **5** (`KLING_MAX_CONCURRENT`, 플랜 한도). 동기 엔진 강제 0, 그 외 0(무제한).
- 제출 시 `budget = 한도 − 현재 in-flight` 선계산 → 한도까지만 즉시 submit, 초과분은 `pending`(deferred).
- `1303` 은 race 안전망 — 발생해도 failed 아님, 다음 tick drain.
- **플랜이 바뀌면 `KLING_MAX_CONCURRENT` 만 조정** (compose default 5 / .env.test).

### 수동 복구
```bash
TOKEN=<GENAI_INTERNAL_TOKEN>
# stale batch 보정
curl -s -X POST -H "X-Internal-Token: $TOKEN" :8088/internal/batches/reconcile-stale
# deferred drain
curl -s -X POST -H "X-Internal-Token: $TOKEN" :8088/internal/jobs/submit-pending
# 특정 job 폴링
curl -s -X POST -H "X-Internal-Token: $TOKEN" :8088/internal/jobs/<job_id>/poll
```

## 10. 한도 · 쿼터

| 항목 | env | 기본 | 의미 |
|------|-----|------|------|
| 파일 크기 | `GENAI_MAX_BYTES_PER_FILE` | 50MB | 이미지 1장 상한 |
| batch당 파일 | `GENAI_MAX_FILES_PER_BATCH` | 20 | |
| bulk 1회 jobs | `GENAI_MAX_BULK_JOBS` | 25 | UI bulk 상한 (초과 시 CLI) |
| Kling 동시 | `KLING_MAX_CONCURRENT` | 5 | 플랜 한도 |
| rate limit | `GENAI_RATE_LIMIT_PER_MIN` | 0(off) | 60s sliding |
| 일별 batch | `GENAI_DAILY_BATCH_LIMIT` | 0(off) | per-user |
| 일별 bytes | `GENAI_DAILY_BYTES_LIMIT` | 0(off) | per-user |

## 11. 운영자 트러블슈팅

| 증상 | 원인 | 조치 |
|------|------|------|
| `/healthz` timeout | genai 컨테이너 미시작 | `--profile genai up -d genai` |
| Veo가 UI에 없음 | 기본 enable 목록 제외 | `GENAI_ENGINES_ENABLED` 에 `veo` 추가 |
| 비동기 job 영구 running | `GENAI_INTERNAL_TOKEN` 미설정 / sensor OFF | 토큰 설정 + sensor RUNNING 확인 |
| 결과 placeholder(40byte) | 자격증명 미설정 → mocking | 해당 엔진 API key/credential 확인 |
| retry "original file gone" | ingest가 originals를 archive 이동 | 재현 불가 — 새 submit 권장 |
| Veo 재시작 후 stuck | in-memory tracker 소실 | reconcile/poll 수동 호출 (REST fallback 작동) |

---

# 부록

## A. HTTP API 레퍼런스

| Method · Path | 인증 | 용도 |
|---------------|------|------|
| GET `/healthz` | none | 헬스 + 엔진 목록 |
| GET `/` | basic | Submit 페이지 |
| POST `/genai/batches` | basic | 단건/멀티 batch 제출 (multipart) |
| GET `/genai/batches` | basic | 목록 (engine/status/bulk_group_id 필터) |
| GET `/genai/batches/{id}` | basic | 상세 (JSON or HTML) |
| GET `/genai/batches/{id}/outputs/{file}` | basic | 결과 스트림 (`?download=true`) |
| GET `/genai/limits` | basic | 한도 + 사용량(headroom) JSON |
| GET `/genai/bulk` | basic | Bulk 폼 |
| POST `/genai/bulk-batches` | basic | Bulk 제출 (idempotency_token) |
| GET `/genai/costs` | basic | 비용 집계 |
| POST `/genai/jobs/{id}/retry` | basic | 실패 job 재시도 (1303→deferred) |
| POST `/internal/jobs/{id}/poll` | internal token | 1 job 폴링→finalize |
| GET `/internal/jobs/pending` | internal token | 비동기 pending 목록 |
| POST `/internal/jobs/submit-pending` | internal token | deferred drain |
| POST `/internal/batches/reconcile-stale` | internal token | stale batch 보정 |

## B. 데이터 위치

```
PG (staging: vlm_pipeline_staging / prod: 별도 DB):
  genai_batches  — batch 단위 (status, n_total/succeeded/failed, options_json, prompt)
  genai_jobs     — job 단위 (status, provider_job_id, error_message, seq_in_batch)

NAS (컨테이너 mount: ${NAS_DATA_ROOT} → /nas/data, dagster 와 동일 루트):
  격리 경로 (auto-bootstrap ingest 스캔 밖):
  /nas/data/genai_studio/incoming/genai/<YYYY-MM-DD>/<batch_id>/originals/<seq>.<ext>
                                                              .../outputs/<seq>.<ext>
  + 각 디렉토리에 _manifest.json (provenance bridge 완성 시 ingest 픽업용 — 현재 미사용)

  ※ 호스트 실제 경로:
     staging : /home/user/mou/nas_200tb/staging/genai_studio/incoming/genai/...
     prod    : /home/user/mou/nas_200tb/genai_studio/incoming/genai/...

결과 수령: Studio UI 다운로드 (GET /genai/batches/{id}/outputs/{file})
           — GENAI_NAS_INCOMING 기준 incoming/archive 에서 read.

⚠️ 현재 MinIO vlm-raw / raw_files 자동 적재는 비활성 (provenance bridge follow-up).
```

## C. 데이터 흐름 (1건 기준)

```
[사용자] UI/CLI submit
   │
   ▼
POST /genai/batches  (genai 컨테이너)
   ├─ PG INSERT: genai_batches(status=running) + genai_jobs(status=pending)
   ├─ NAS atomic write (격리): genai_studio/incoming/.../originals/ + _manifest.json
   │                            (txt2video는 originals skip)
   └─ adapter.submit()
        ├─ 동기(nanobanana/gpt_image): 결과 즉시 → finalize → outputs/ write
        └─ 비동기(kling/veo/higgsfield): provider_job_id 저장, status=submitted
                                          (동시 한도 초과분은 pending=deferred)
   │
   ▼  (비동기) Dagster genai_poll_sensor — 30s tick (순서대로)
   ├─ 1. submit-pending: deferred 작업 drain (동시 한도 내)
   ├─ 2. reconcile-stale: 종료됐는데 running 인 batch 보정
   └─ 3. poll: provider 상태 확인 → done 시 finalize → outputs/<seq>.<ext> + _manifest.json
   │
   ▼
[결과 수령] Studio UI 에서 인라인 재생 + 다운로드 (격리 경로에서 직접 stream)

   ✗ (미연결) 학습 파이프라인 자동 적재(MinIO vlm-raw + raw_files):
     genai provenance manifest 가 ingest sensor 에 wired 되어야 활성화. follow-up.
     그 전까지 출력은 격리 경로에만 존재하고 auto-bootstrap 이 건드리지 않음.
```

## D. 환경변수 전체

```
# 경로/DB
GENAI_NAS_INCOMING=/nas/staging/incoming
DATAOPS_POSTGRES_DSN=postgresql://...

# 인증
GENAI_BASIC_AUTH_USER / GENAI_BASIC_AUTH_PASS
GENAI_AUTH_DISABLED            # true 면 무인증 (개발 전용)
GENAI_INTERNAL_TOKEN           # Dagster↔genai 내부 API

# 엔진 게이팅
GENAI_ENGINES_ENABLED          # compose default: kling,higgsfield,nanobanana,gpt_image (veo 제외!)
                               # ※ env 완전 미설정(=compose 미경유) 시 코드 fallback 은 5엔진 전체.
                               #   실제 운영은 compose 경유라 veo 제외가 유효값.

# Kling
KLING_ACCESS_KEY / KLING_SECRET_KEY
KLING_MODEL_NAME=kling-v2-6     # API 식별자만!
KLING_AVAILABLE_MODELS          # CSV override
KLING_MODE=pro / KLING_DURATION=5
KLING_MAX_CONCURRENT=5          # 플랜 동시 한도

# Veo (Vertex)
GEMINI_GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_APPLICATION_CREDENTIALS
GEMINI_PROJECT / GEMINI_LOCATION
VEO_MODEL_NAME=veo-3.1-generate-001
VEO_ASPECT_RATIO=16:9
VEO_OUTPUT_GCS_URI              # 권장(미설정 시 inline bytes fallback)

# 기타 엔진
FAL_KEY                         # Higgsfield
OPENAI_API_KEY                  # GPT Image
NANOBANANA_MODEL=gemini-2.5-flash-image

# 한도
GENAI_MAX_BYTES_PER_FILE=52428800 / GENAI_MAX_FILES_PER_BATCH=20
GENAI_MAX_BULK_JOBS=25 / GENAI_BULK_SLEEP_SECONDS=0.5
GENAI_RATE_LIMIT_PER_MIN=0 / GENAI_DAILY_BATCH_LIMIT=0 / GENAI_DAILY_BYTES_LIMIT=0

# sensor (Dagster 측)
GENAI_INTERNAL_BASE=http://genai:8088
GENAI_POLL_INTERVAL_SECONDS=30 / GENAI_POLL_BATCH=20
```

## E. 관련 문서
- `docs/genai_rollout/plan.md` — 통합 설계 v3
- `docs/genai_rollout/bulk_plan.md` — bulk 설계
- `docs/genai_rollout/cli_plan.md` — CLI 설계
- `docs/genai_rollout/operator_guide.md` — Phase 5 운영 강화
