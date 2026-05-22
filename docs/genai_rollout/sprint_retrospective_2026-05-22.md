# GenAI Studio — 스프린트 회고 (2026-05-22)

> **독자**: AI팀 본부장 및 팀장급
> **범위**: 2026-05-08 ~ 2026-05-22 (약 2주)
> **상태**: dev/staging 안정화 완료 → main 승격 대기

---

## 0. 한 줄 요약

CCTV 영상 라벨링 파이프라인 옆에, **"이미지 + 프롬프트로 5개 외부 생성 모델을 호출해 → 결과물을 자동으로 학습 데이터셋으로 흡수"** 하는 GenAI Studio 서브시스템을 신규 구축. 사내망 웹 UI와 CLI 두 진입점을 모두 제공.

---

## 1. 왜 했나 (Why)

| 문제 | 해결 방향 |
|------|----------|
| 학습용 영상/이미지 데이터가 한정 — CCTV 수집만으로는 희귀 이벤트 커버 어려움 | 외부 생성 모델로 합성 데이터 생산 |
| 모델·키·SDK가 각자 달라서 일회성 스크립트로는 운영 불가 | 통합 컨테이너 + 어댑터 패턴으로 추상화 |
| 생성 결과물이 기존 파이프라인(라벨링·데이터셋 빌드)과 단절 | NAS → ingest sensor → build asset 경로에 자연스럽게 합류시킴 |

핵심 가치: **"누가 키만 채우면 어떤 외부 생성 모델이든 그날부터 학습 데이터로 들어온다"** 는 일관 흐름.

---

## 2. 타임라인 (Phase 1~5 + 운영 강화)

| Phase | 일자 | 무엇 |
|-------|------|------|
| 0 — 사전 결정 | 05-08 이전 | 모델/접근 경로/인증 정책 확정 |
| 1 — DB 스키마 | 05-08 | Postgres `genai_batches` / `genai_jobs` 테이블, 멱등 마이그레이션 |
| 2 — Build 분기 | 05-08 | dataset 빌드 시 GenAI pair 별도 경로 + 부분 실패 표기 |
| 3 — 핵심 컨테이너 | 05-09 | FastAPI `genai` 컨테이너 + Kling 어댑터 + Dagster polling sensor |
| 3.5 — 분리 | 05-10 | sensor ↔ 어댑터 직접 import 제거, HTTP 경유로 결합 해제 |
| 4 — 멀티 엔진 | 05-10 | Nanobanana / GPT Image / Higgsfield 3개 추가 (총 4종) |
| 5 — 운영 강화 | 05-10 | 분당/일별 quota, 비용 대시보드, 부분 실패 재시도, advisory lock |
| 5+ (후속) | 05-11 ~ 05-22 | **Veo 추가 (5번째)**, **Text→Video**, **Bulk N×M 제출**, UI 전면 개편, CLI 추가 |

> 후속 작업 commit 30건+ — 본 회고의 4·5·6장에 반영.

---

## 3. 아키텍처 한눈에 보기

```
   ┌─────────────────┐     ┌──────────────────────────┐     ┌──────────────────┐
   │   사용자(사내망)│     │   GenAI Studio 컨테이너  │     │   외부 생성 모델 │
   │  웹 UI / CLI    │────▶│   (FastAPI, :8088)       │────▶│  Kling / Veo /   │
   └─────────────────┘     │                          │     │  Higgsfield /    │
                           │  ┌────────────────────┐  │     │  Nanobanana /    │
                           │  │ 어댑터 5종         │  │     │  GPT Image       │
                           │  │ (engine-agnostic)  │  │     └──────────────────┘
                           │  └────────────────────┘  │
                           │  ┌────────────────────┐  │
                           │  │ rate-limit / quota │  │
                           │  │ cost / retry       │  │
                           │  └────────────────────┘  │
                           └────┬────────────┬────────┘
                                │ status     │ atomic write
                                ▼            ▼
                       ┌──────────────┐  ┌───────────────────────┐
                       │  Postgres    │  │  NAS (날짜 폴더)       │
                       │ batches/jobs │  │  originals/ outputs/  │
                       └──────┬───────┘  └──────────┬────────────┘
                              │                     │
                              │ poll               │
                              ▼                     ▼
                       ┌──────────────────────────────────────┐
                       │  Dagster (기존 파이프라인)            │
                       │  ┌──────────────────────────────┐    │
                       │  │ genai_poll_sensor            │    │
                       │  │ (HTTP poll only, no import)  │    │
                       │  └──────────────────────────────┘    │
                       │  ┌──────────────────────────────┐    │
                       │  │ INGEST sensor                │    │
                       │  │ NAS → MinIO → raw_files      │    │
                       │  └──────────────────────────────┘    │
                       │  ┌──────────────────────────────┐    │
                       │  │ build asset (GenAI 전용 분기) │    │
                       │  │ paired manifest 생성          │    │
                       │  └──────────────────────────────┘    │
                       └──────────────────────────────────────┘
```

### 핵심 설계 결정 3가지

1. **별도 컨테이너 + HTTP 경계** — 외부 SDK(google-genai / openai / fal-client / requests) 의존성을 메인 Dagster 이미지와 격리. SDK 충돌·재빌드 비용 차단.
2. **NAS만 사용 (MinIO 직접 업로드 안 함)** — 결과물을 NAS에 떨어뜨리면 기존 INGEST 흐름이 알아서 흡수. "생성 데이터 전용 경로"를 따로 만들지 않음.
3. **mocking 모드 자동 감지** — 키 미설정 시 placeholder 파일 반환. e2e 검증을 비용 0으로 가능.

---

## 4. 지원 엔진 5종

| 엔진 | 출력 | 동기/비동기 | 주 용도 | 인증 |
|------|------|------------|---------|------|
| **Kling** | 영상 (mp4) | 비동기 (5~10분) | 일반 image→video, 7개 모델 + 모드/duration 선택 | Kling JWT (Access/Secret) |
| **Veo** (Vertex AI) | 영상 (mp4) | 비동기 | 고품질 image→video + **text→video** 둘 다 | Vertex SA (your-gcp-project) |
| **Higgsfield** (fal.ai) | 영상 (mp4) | 비동기 | image→video, "soul" 모델 | fal.ai 키 |
| **Nanobanana** (Vertex AI) | 이미지 (png) | 동기 (즉시) | image→image 변형 (gemini-2.5-flash-image) | Vertex SA |
| **GPT Image** (OpenAI) | 이미지 (png) | 동기 (즉시) | image→image 편집 (gpt-image-1) | OpenAI 키 |

### 엔진 추상화

각 어댑터는 동일한 인터페이스(`submit / poll / download_result`)를 구현하므로, 새 엔진 추가는 **~100~300줄 어댑터 1개 + registry 등록 + 환경변수 추가** 로 끝납니다. (실제 Veo 추가 시 350줄, Higgsfield 추가 시 136줄)

---

## 5. 기능 인벤토리

### 사용자가 직접 만지는 기능

| 기능 | UI | CLI | API |
|------|-----|------|-----|
| 단건 batch 제출 (이미지 N장 + 프롬프트 1개) | ✅ `/genai/` | ✅ `genai-cli submit` | ✅ POST `/genai/batches` |
| **Bulk N×M** (이미지 N장 × 프롬프트 M개 = N·M jobs) | ✅ `/genai/bulk` | ✅ `genai-cli bulk-submit` | ✅ |
| **Text→Video** (Veo, 이미지 없이 텍스트만) | ✅ Text→Video 탭 | ✅ `--text-only` | ✅ |
| 배치 목록 + engine/status 필터 | ✅ `/genai/batches` | ✅ `genai-cli batches` | ✅ |
| 배치 상세 + 영상/이미지 미리보기 + 다운로드 | ✅ `/genai/batches/<id>` | ✅ `genai-cli jobs` | ✅ |
| 실패 job 재시도 | ✅ 상세 페이지 retry 버튼 | ✅ `genai-cli retry` | ✅ POST `/genai/jobs/<id>/retry` |
| 비용 대시보드 (day/week/month) | ✅ `/genai/costs` | — | ✅ |

### 운영자만 만지는 기능

- `GENAI_RATE_LIMIT_PER_MIN` — 사용자별 분당 batch 한도
- `GENAI_DAILY_BATCH_LIMIT` / `GENAI_DAILY_BYTES_LIMIT` — 일별 batch / 업로드 총량
- `GENAI_ENGINES_ENABLED` — 엔진 화이트리스트 (예: 키 없는 엔진을 UI에서 숨김)
- 기본값 모두 0 = off. 사내망 신뢰 가정.

---

## 6. UI 화면 인벤토리

웹 UI: `http://10.0.0.10:8088/` (사내망, basic auth)

| 경로 | 화면 | 비고 |
|------|------|------|
| `/genai/` | **메인 제출 화면** | 엔진 카드 5종 + 3탭(Image→Video / Image→Image / Text→Video) + 동적 옵션 |
| `/genai/bulk` | **Bulk 제출 화면** | 디렉토리 단위 이미지 N장 × 프롬프트 줄단위 M개 |
| `/genai/batches` | 배치 목록 | engine/status 필터, 그룹 필터 |
| `/genai/batches/<id>` | 배치 상세 | per-job 상태 + 미리보기 + 다운로드 + retry |
| `/genai/costs` | 비용 대시보드 | range(day/week/month) 탭, 엔진별 done/failed/cost_units |

UI 디자인은 스프린트 중반 (5월 13~14) 에 **Workspace / Playground 두 컨셉 시안 → Playground 채택** 한 뒤 약 15회의 fix commit으로 다듬어졌습니다.

---

## 7. 운영 안전장치 (Phase 5)

| 장치 | 무엇 | 트리거 |
|------|------|--------|
| **Rate-limit** | 분당 batch 제한 (sliding window) | 사용자별, 초과 시 429 |
| **Daily quota** | 일별 batch + 총 바이트 제한 | 사용자별, 초과 시 429 |
| **Cost tracking** | 엔진별 cost_units 집계 | UI/API 로 day/week/month 조회 |
| **Atomic retry** | 실패 job 재시도 | UPDATE...RETURNING CAS 로 race-free |
| **Advisory lock** | manifest 동시 쓰기 방지 | PG `pg_try_advisory_xact_lock` |
| **Background finalize** | 외부 다운로드(최대 5분) 가 sensor timeout(60s) 막지 않도록 | FastAPI BackgroundTasks |

> 모두 Codex 코드리뷰 (HIGH 등급 5건 / MED 7건) 반영 결과.

---

## 8. 기존 파이프라인과의 연결점

GenAI 결과물이 **자동으로** 학습 데이터로 들어오는 흐름:

1. 어댑터가 NAS의 `incoming/genai/<batch_id>/outputs/<seq>.mp4` 에 atomic write
2. 모든 job done/failed 시 outputs 폴더에 `_manifest.json` 생성 (advisory lock)
3. 기존 **INGEST sensor** 가 manifest 발견 → MinIO 업로드 + `raw_files` 행 추가
4. **Build asset** 이 `source_type='genai'` 분기로 이동 → input/output 짝을 묶어 paired manifest 생성
5. `vlm-dataset/genai_<batch_id>/` 에 학습 가능한 형태로 적재

→ **GenAI 전용 파이프라인을 따로 만들지 않았다**는 게 핵심. 기존 sensor·asset에 작은 분기만 추가.

---

## 9. 잔여 과제 (다음 스프린트 후보)

### 운영자 작업
- [ ] **5개 엔진 실 키 입력 후 e2e 1회씩 성공 확인** (현재 mocking 으로만 검증)
- [ ] Higgsfield 모델 ID (`fal-ai/higgsfield/i2v-soul`) fal.ai 대시보드에서 확정
- [ ] staging 24h stable 후 main PR → 프로덕션 배포

### 개선 follow-up
- [ ] **분산 rate-limit**: 현재 in-memory(single-container 전제). 워커 증설 시 Redis/PG로 이전
- [ ] **컨테이너 재시작 finalize 누락**: BackgroundTasks 중 컨테이너 죽으면 다운로드 유실 → sensor가 fallback finalize 가능하게
- [ ] **Archive 이후 retry 지원**: 원본이 archive로 이동되면 retry 시 410 → archive에서 원복하는 path 필요
- [ ] Vertex SA의 prod 권한 분리 (현 staging/prod 동일 `your-gcp-project`)
- [ ] Cost 정밀도 — 어댑터별 cost_units 추출 보강

### 신규 검토 가능 항목
- [ ] **6번째 엔진**: 시장 신규 모델 (Sora2 등) 검토. 어댑터 1개 추가 비용 ≈ 1~2일.
- [ ] **프롬프트 라이브러리**: 자주 쓰는 프롬프트 저장 + 변형. 데이터셋 다양성 확보.
- [ ] **결과 자동 라벨링 연계**: 생성 영상이 INGEST된 후 자동으로 Gemini 라벨링 큐에 진입하도록 정책 추가.

---

## 10. 메트릭 (코드/문서 규모)

| 항목 | 수치 |
|------|------|
| 신규 Python 코드 (`docker/genai/`) | ~2,900 줄 |
| CLI 코드 (`scripts/genai_cli/`) | ~600 줄 |
| 어댑터 5개 평균 | ~180 줄 |
| HTML 템플릿 | 8개 |
| GenAI 관련 commit | 69건 (이 기간) |
| 머지된 PR | 20+ |
| Phase 문서 | 6개 (`docs/genai_rollout/`) |

---

## 11. 참고 문서

이 회고가 요약본. 깊게 보려면:

- [`docs/genai_rollout/plan.md`](./plan.md) — 전체 설계 (28KB)
- [`docs/genai_rollout/phase_status.md`](./phase_status.md) — Phase별 완료 항목 + Codex 리뷰 반영 내역
- [`docs/genai_rollout/operator_guide.md`](./operator_guide.md) — 운영자 체크리스트 (키 입력 / e2e 검증 / 트러블슈팅)
- [`docs/genai_rollout/cli_plan.md`](./cli_plan.md) — CLI 설계
- [`docs/genai_rollout/bulk_plan.md`](./bulk_plan.md) — Bulk N×M 설계

코드 진입점:
- 컨테이너: `docker/genai/app.py`
- 어댑터: `docker/genai/adapters/{kling,veo,higgsfield,nanobanana,gpt_image}.py`
- Dagster sensor: `src/vlm_pipeline/defs/genai/sensor.py`
- CLI: `scripts/genai_cli/`
