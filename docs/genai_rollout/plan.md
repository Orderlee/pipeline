# GenAI Studio 통합 계획서 (final · v3)

**대상 환경**: staging (dev 브랜치) — `/home/user/work_p/Datapipeline-Data-data_pipeline_test`
**DB 정책**: Postgres 단일 진리 (DuckDB 는 `postgres_scanner` extension 으로 read-only 조회만)
**작성 기준**: 2026-05-08 / Opus + Codex 합의안 (v3 — fan-out batch + paired dataset)

---

## 1. 핵심 로직

```
사용자 1회 submit
  · 이미지 N장 업로드
  · 프롬프트 1개
  · 엔진 1개
        │
        ▼
  ┌─────────── batch_id 1개 ───────────┐
  │  job_1 → 이미지[1] → 결과[1]       │
  │  job_2 → 이미지[2] → 결과[2]       │
  │   …                                │
  │  job_N → 이미지[N] → 결과[N]       │
  └────────────────────────────────────┘

원본 N장 + 생성물 N개 모두 Postgres 에 기록 + dataset 에 paired 로 적재
```

- **fan-out 단위**: 1 batch = N jobs (1:N)
- **paired 보존**: 각 job 의 (input image, output {video|image}) 가 dataset 안에서 짝으로 추적
- **출력 타입**: 엔진별 자동 결정 — Kling/Higgsfield = MP4, Nanobanana/GPT Image = PNG

## 2. UI — 2 탭 구조

```
┌────────────────────────────────────────────────────────┐
│  GenAI Studio                                          │
│  ┌──────────────┬──────────────┐                       │
│  │ Image→Video  │ Image→Image  │                       │
│  └──────┬───────┴──────────────┘                       │
│         │                                              │
│  엔진 ▼ ◯ Kling  ◯ Higgsfield                          │
│  이미지 드롭존 [N장 업로드]                            │
│  프롬프트 [______________________________]             │
│  옵션 (해상도/길이 등 엔진별)                          │
│  [Submit] → batch_id 발급                              │
│                                                        │
│  Batch 목록 / 상태 ── pending|running|succeeded|       │
│                       partial_success|failed           │
│  └ Job 별 진행 (N개 row, seq_in_batch, status)         │
└────────────────────────────────────────────────────────┘

Tab "Image→Video":  Kling, Higgsfield      → 출력 MP4
Tab "Image→Image":  Nanobanana, GPT Image  → 출력 PNG
```

## 3. 4-엔진 매트릭스

| 엔진 | 입력 | 출력 | 인증 | 동기/비동기 | 신규 secret |
|------|------|------|------|-------------|--------------|
| **Kling** | image + prompt | MP4 | JWT (AK+SK) | 비동기 (submit→poll) | `KLING_ACCESS_KEY`, `KLING_SECRET_KEY` |
| **Higgsfield** | image + prompt | MP4 | fal.ai or replicate | 비동기 | `FAL_KEY` 또는 `REPLICATE_API_TOKEN` |
| **Nanobanana** (Gemini 2.5 Flash Image) | image + prompt | PNG/JPG | Vertex SA | 동기 | **0** — 기존 `your-gcp-project` 재사용 |
| **GPT Image** (`gpt-image-1`) | image (선택) + prompt | PNG | OpenAI API key | 동기 | `OPENAI_API_KEY` |

> 4 엔진 모두 입력 = **이미지(들) + 프롬프트** 로 통일. video 입력 옵션 제거.
> reference-image 시나리오: Nanobanana 또는 GPT Image 로 일원화. (Imagen 라인은 도입하지 않음 — 2026-06-30 discontinuation 회피)

## 4. 데이터 흐름 (전체)

```
   사내망 (10.0.0.10)
   ┌──────────────────────────────────────────────────────────────┐
   │  GenAI Studio UI    POST /genai/batches  (multipart, N files)│
   │           │                                                  │
   │           ▼  (1) Postgres TXN                                │
   │   ┌──────────────────────────────────────┐                   │
   │   │ INSERT genai_batches (batch_id, ...) │                   │
   │   │ INSERT genai_jobs × N                │                   │
   │   └──────────────────────────────────────┘                   │
   │           │                                                  │
   │           ▼  (2) 원본 N장을 NAS 에 atomic write              │
   │   /nas/staging/incoming/genai/<batch_id>/originals/          │
   │     · 001.png  002.png  …  N.png                             │
   │     · _manifest.json   {kind:"originals", batch_id, jobs:[]} │
   │           │                                                  │
   │           ▼  (3) 어댑터 호출                                 │
   │   ┌────────────────────────────────────────┐                 │
   │   │ for seq in 1..N:                       │                 │
   │   │   adapter.submit(image[seq], prompt)   │                 │
   │   │   → genai_jobs.provider_job_id 저장    │                 │
   │   └────────────────────────────────────────┘                 │
   └────────────────┬─────────────────────────────────────────────┘
                    │
                    │ (4) Dagster sensor 가 polling (Postgres)
                    │     비동기 엔진(Kling, Higgs) 만 polling.
                    │     동기 엔진(Nano, GPT) 은 submit 시점에 결과 확보.
                    ▼
   ┌──────────────────────────────────────────────────────────────┐
   │  외부 API:  Kling · fal.ai · Vertex · OpenAI                 │
   │  결과 다운로드 (CDN/blob URL → bytes, 백엔드가 직접)          │
   └──────┬───────────────────────────────────────────────────────┘
          │ (5) 결과 N개를 NAS atomic write
          ▼
   /nas/staging/incoming/genai/<batch_id>/outputs/
     · 001.{mp4|png}  002.{mp4|png}  …
     · _manifest.json  {kind:"outputs", batch_id,
                        items:[{seq, provider_job_id, ...}]}
                       │
                       ▼  [기존 NAS sensor 자동 픽업 — 원본/결과 양쪽]
   ┌──────────────────────────────────────────────────────────────┐
   │  INGEST sensor (수정 ~25줄)                                  │
   │   _manifest.json 의 kind 별 분기:                            │
   │                                                              │
   │   originals/ → raw_files INSERT:                             │
   │     source_type   = 'genai_source'           ★신규 컬럼      │
   │     genai_engine  = batch.engine             ★신규 컬럼      │
   │     label_policy  = 'none'                   ★신규 컬럼      │
   │     media_type    = 'image'                                  │
   │     source_unit_name = 'genai_<batch_id>'                    │
   │   → UPDATE genai_jobs SET input_asset_id = asset_id          │
   │                       WHERE batch_id=? AND seq=?             │
   │                                                              │
   │   outputs/ → raw_files INSERT:                               │
   │     source_type   = 'genai_output'                           │
   │     genai_engine  = batch.engine                             │
   │     label_policy  = 'none'                                   │
   │     media_type    = 'video' | 'image' (확장자 판별)          │
   │     source_unit_name = 'genai_<batch_id>'                    │
   │   → UPDATE genai_jobs SET output_asset_id = asset_id,        │
   │                       status = 'done'                        │
   │                       WHERE batch_id=? AND seq=?             │
   │                                                              │
   │   batch 행 다 채워지면                                       │
   │     UPDATE genai_batches SET status = derived_status         │
   │           (succeeded|partial_success|failed)                 │
   └────────────────┬─────────────────────────────────────────────┘
                    ▼
   ┌──────────────────────────────────────────────────────────────┐
   │  build/assets.py _build_project (CASE 분기 2갈래)            │
   │                                                              │
   │  CASE 1 — 일반 (camera|nas_upload, label_policy='required'): │
   │     기존 로직 그대로                                         │
   │                                                              │
   │  CASE 2 — GenAI (source_type IN                              │
   │           ('genai_source','genai_output'),                   │
   │           label_policy='none'):                              │
   │     SELECT j.batch_id, j.seq_in_batch,                       │
   │            src.raw_key, src.media_type,                      │
   │            out.raw_key, out.media_type,                      │
   │            b.engine, b.prompt                                │
   │       FROM genai_jobs j                                      │
   │       JOIN raw_files src ON src.asset_id=j.input_asset_id    │
   │       JOIN raw_files out ON out.asset_id=j.output_asset_id   │
   │       JOIN genai_batches b ON b.batch_id=j.batch_id          │
   │      WHERE src.source_unit_name = <folder>                   │
   │        AND j.status = 'done'                                 │
   │                                                              │
   │     for each pair:                                           │
   │       copy src → vlm-dataset/<folder>/images/<seq>.png       │
   │       copy out → vlm-dataset/<folder>/                       │
   │              {videos|generated_images}/<seq>.{mp4|png}       │
   │     manifest entry per pair:                                 │
   │       {pair_id, batch_id, seq, source_key, generated_key,    │
   │        engine, prompt, label_free:true,                      │
   │        provider_job_id}                                      │
   └──────────────────────────────────────────────────────────────┘
```

## 5. DB 스키마 변경 — Postgres 전용

**대상**: `src/vlm_pipeline/sql/migrations/postgres/002_genai.sql` (forward-only, idempotent, Phase 1 에서 적용 완료)

```sql
BEGIN;

-- raw_files 보강
ALTER TABLE raw_files ADD COLUMN IF NOT EXISTS source_type   TEXT DEFAULT 'camera';
ALTER TABLE raw_files ADD COLUMN IF NOT EXISTS genai_engine  TEXT;
ALTER TABLE raw_files ADD COLUMN IF NOT EXISTS label_policy  TEXT DEFAULT 'required';
-- + CHECK constraints (PG 15 호환 DO $$ pattern, idempotent)
-- + 인덱스 (idx_raw_files_source_type, idx_raw_files_label_policy, idx_raw_files_genai_engine)

-- batch (1회 submit 단위)
CREATE TABLE IF NOT EXISTS genai_batches (
    batch_id      TEXT PRIMARY KEY,
    engine        TEXT NOT NULL CHECK (engine IN ('kling','higgsfield','nanobanana','gpt_image')),
    output_media  TEXT NOT NULL CHECK (output_media IN ('video','image')),
    prompt        TEXT NOT NULL,
    options_json  TEXT,
    requested_by  TEXT,
    status        TEXT DEFAULT 'pending'
                    CHECK (status IN ('pending','running','succeeded','partial_success','failed','cancelled')),
    n_total       INTEGER NOT NULL CHECK (n_total >= 1),
    n_succeeded   INTEGER DEFAULT 0,
    n_failed      INTEGER DEFAULT 0,
    submitted_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at  TIMESTAMP
);

-- 개별 job (이미지 1장 → 결과 1개)
CREATE TABLE IF NOT EXISTS genai_jobs (
    job_id            TEXT PRIMARY KEY,
    batch_id          TEXT NOT NULL REFERENCES genai_batches(batch_id) ON DELETE CASCADE,
    seq_in_batch      INTEGER NOT NULL CHECK (seq_in_batch >= 1),
    input_asset_id    TEXT REFERENCES raw_files(asset_id),
    output_asset_id   TEXT REFERENCES raw_files(asset_id),
    provider_job_id   TEXT,
    status            TEXT DEFAULT 'pending'
                        CHECK (status IN ('pending','submitted','running','done','failed')),
    error_message     TEXT,
    cost_units        DOUBLE PRECISION,
    submitted_at      TIMESTAMP,
    completed_at      TIMESTAMP,
    UNIQUE(batch_id, seq_in_batch)
);

-- 인덱스: idx_genai_jobs_batch, idx_genai_jobs_status, idx_genai_batches_status
COMMIT;
```

> 별도 `genai_pairs` 테이블 **불필요** — 1 job = 1 input + 1 output 이라 `genai_jobs` FK 2개로 충분.

## 6. 폴더 트리

### 6-1. 코드 (repo)

```
docker/
  genai/                                     ★ 신규 컨테이너 (FastAPI + Jinja2 + HTMX)
    Dockerfile
    requirements.txt
    app.py                  엔트리, 라우트
    templates/
      base.html
      index.html            (탭 2개 + 드롭존 + 프롬프트)
      batches.html          (batch 목록 + status)
      batch_detail.html     (batch 안 N개 job + pair 미리보기)
    static/
      htmx.min.js
      style.css
    adapters/
      __init__.py
      base.py               BaseGenAIAdapter Protocol
                            (submit / poll / download_result)
      kling.py              JWT (AK+SK) 서명 + 비동기
      higgsfield.py         fal.ai 또는 replicate
      nanobanana.py         Vertex (gemini-2.5-flash-image), 동기
      gpt_image.py          OpenAI (gpt-image-1), 동기
    jobs/
      submit.py             POST /genai/batches 처리
                            (Postgres TXN + NAS originals + 어댑터 호출)
      finalize.py           결과 NAS 안착 + manifest 작성
    storage/
      nas_writer.py         atomic write (.partial → rename)
      manifest.py           _manifest.json 빌더
    db/
      pg.py                 Postgres connection (PIPELINE_DB_DSN 재사용)

src/vlm_pipeline/
  defs/
    ingest/sensor.py        (수정 ~25줄: _manifest.json originals/outputs 분기)
    build/assets.py         (수정 ~50줄: CASE 분기 regular vs genai pairs)
    genai/                  ★ 신규 Dagster 모듈
      __init__.py
      sensor.py             genai_poll_sensor (Kling/Higgs polling)
      ops.py                job 상태 → genai_jobs/genai_batches UPDATE
  resources/
    postgres_dedup.py       (수정: candidate SELECT + find_project_genai_pairs)  [Phase 1 적용]
    postgres_genai.py       ★ 신규 mixin (write-side 일체)                        [Phase 1 적용]
    postgres.py             (수정: PostgresGenAIMixin 등록)                        [Phase 1 적용]
  sql/migrations/postgres/
    002_genai.sql           ★ 신규 마이그레이션                                    [Phase 1 적용]
tests/
  unit/genai/               ★ 신규
    test_kling_adapter.py
    test_submit_endpoint.py
    test_build_genai_pairs.py
  integration/test_genai_e2e.py  ★ 신규
```

### 6-2. NAS 런타임 (staging)

```
/home/user/mou/staging/                         (호스트)
└── incoming/                                  ─→ 컨테이너에서 /nas/staging/incoming
    │
    ├── <기존 NAS source 폴더들>               [기존 — 영향 없음]
    │
    └── genai/                                 ★ GenAI 전용 루트
        │
        ├── <batch_id_1>/                      (예: kling_b2f4-1a3c-...)
        │   ├── originals/                     ── 사용자가 업로드한 원본 N장
        │   │   ├── 001.png
        │   │   ├── 002.png
        │   │   ├── 003.png
        │   │   └── _manifest.json             {kind: "originals", batch_id, ...}
        │   │
        │   ├── outputs/                       ── 외부 API 다운로드한 결과 N개
        │   │   ├── 001.mp4
        │   │   ├── 002.mp4
        │   │   ├── 003.mp4
        │   │   └── _manifest.json             {kind: "outputs", batch_id, ...}
        │   │
        │   └── failed/                        (선택) JSONL 실패 로그
        │       └── jobs.jsonl
        │
        ├── <batch_id_2>/                      (다른 batch — 독립)
        │   ├── originals/
        │   ├── outputs/
        │   └── …

/home/user/mou/staging/archive/                 (NAS sensor 처리 후 이동)
└── genai/
    └── <batch_id_1>/{originals,outputs}/
```

**파일 안착 순서 (한 batch 안에서)**:

```
T+0   /genai/<batch>/originals/001.png.partial    ← 백엔드가 쓰기 시작
T+1   /genai/<batch>/originals/001.png            ← atomic rename
T+2   … 002.png, 003.png 동일
T+3   /genai/<batch>/originals/_manifest.json     ← 마지막에 작성 (sensor 픽업 트리거)
T+10s ↓ NAS sensor 가 originals 인식 → raw_files INSERT
T+30s ↓ 백엔드가 외부 API 결과 다운로드 시작
T+5m  /genai/<batch>/outputs/001.mp4.partial
T+5m  /genai/<batch>/outputs/001.mp4
T+15m … 002.mp4, 003.mp4
T+15m /genai/<batch>/outputs/_manifest.json      ← outputs 안착 신호
T+15m ↓ NAS sensor 가 outputs 인식 → raw_files INSERT + genai_jobs FK 업데이트
```

### 6-3. MinIO 영속 (5 버킷 고정)

```
MinIO (staging :9002)
│
├── vlm-raw/                                   ── INGEST sensor 가 업로드
│   └── genai_<batch_id>/                      (source_unit_name = sanitized folder)
│       ├── originals/{001.png, 002.png, 003.png}
│       └── outputs/{001.mp4, 002.mp4, 003.mp4}
│
├── vlm-labels/                                [기존 — GenAI 데이터 진입 안 함, label_policy='none']
├── vlm-processed/                             [기존 — 영향 없음]
│
├── vlm-dataset/                               ── build asset 이 적재
│   └── genai_<batch_id>/                      (project / source_unit_name)
│       ├── images/                            ── 원본 이미지 N장
│       │   └── {001.png, 002.png, 003.png}
│       ├── videos/                            ── Image→Video 결과 (Kling/Higgs)
│       │   └── {001.mp4, 002.mp4, 003.mp4}
│       ├── generated_images/                  ── Image→Image 결과 (Nano/GPT)
│       └── manifest.json                      {project, pairs:[…], counts:{pairs, images, videos, generated_images}}
│
└── vlm-classification/                        [기존 — 영향 없음]
```

### 6-4. Postgres — 행 단위 관계

```
genai_batches  (batch_id PK)
   │ 1
   │
   │ N
genai_jobs     (job_id PK, batch_id FK ON DELETE CASCADE)
   │            ─┬─ input_asset_id  ──→ raw_files.asset_id (source_type='genai_source')
   │             └─ output_asset_id ──→ raw_files.asset_id (source_type='genai_output')
   │
raw_files
   ├ source_type   = 'genai_source' | 'genai_output' | 'camera' | 'nas_upload'
   ├ genai_engine  = 'kling' | 'higgsfield' | 'nanobanana' | 'gpt_image' | NULL
   ├ label_policy  = 'none' | 'required'
   └ source_unit_name = 'genai_<batch_id>'    ← build asset 의 folder 키
```

## 7. 결과 저장 = 자동 로직 (사용자 다운로드 X)

```
[A] 자동 로직 저장  ← v3 채택
┌─────────────┐    ┌──────────────┐   서버 간   ┌──────────┐
│ 사용자      │    │ FastAPI      │   HTTP      │ Kling/   │
│ 브라우저    │───▶│ 백엔드       │────────────▶│ Higgs/   │
│             │    │ (genai 컨테이너)│           │ Vertex/  │
│ submit 클릭 │    │              │             │ OpenAI   │
└─────────────┘    └──────┬───────┘             └────┬─────┘
                          │                          │
                          │  ◀ 결과 bytes 다운로드 ─┘
                          │     (백엔드가 직접)
                          ▼
                   /nas/staging/incoming/genai/<batch>/outputs/
                          │
                          ▼  [NAS sensor 픽업]
                   raw_files + vlm-raw + vlm-dataset
                          │
                          ▼
                   사용자 브라우저는 "완료" 상태만 표시
                   (필요 시 미리보기 링크 = MinIO presigned URL)
```

- 사용자 PC 디스크에는 아무것도 저장 안 됨
- 동기 엔진(Nano/GPT): submit 응답에 base64 PNG 포함 → 디코드 후 NAS 저장
- 비동기 엔진(Kling/Higgs): submit → poll → 완료 응답에 CDN URL → bytes 다운로드 후 NAS 저장
- NAS 경로: `.partial` 으로 쓰고 atomic rename, `_manifest.json` 마지막에

## 8. 기존 모듈 수정점

| 파일 | 변경 | Phase |
|------|------|-------|
| `sql/migrations/postgres/002_genai.sql` (신규) | raw_files 보강 + genai_batches + genai_jobs | 1 ✓ |
| `resources/postgres_genai.py` (신규 mixin) | write-side 일체 | 1 ✓ |
| `resources/postgres_dedup.py` | candidate SELECT + `find_project_genai_pairs` | 1 ✓ |
| `resources/postgres_ingest_raw.py` | conditional 신규 컬럼 | 1 ✓ |
| `resources/postgres.py` | mixin 등록 | 1 ✓ |
| `defs/ingest/ops_register.py` | manifest pass-through + fail-loud | 1 ✓ |
| `defs/build/assets.py` `_build_project` | CASE 분기 (regular vs genai) | 2 |
| `defs/ingest/sensor.py` | `_manifest.json` originals/outputs 인식 | 3 |
| `defs/genai/{sensor,ops}.py` (신규) | Dagster polling sensor | 3 |
| `docker/genai/` (신규 컨테이너) | FastAPI UI + 어댑터 4종 | 3-4 |
| `docker/docker-compose.yaml` | genai 서비스 추가 | 3 |
| `docker/.env.test` (git 미추적) | secret 4종 + GENAI_* | 3 |

> `resources/duckdb_dedup.py`, `resources/duckdb_migration.py` **수정 없음** — DuckDB 는 `postgres_scanner` 로 자동 reflect.

## 9. 보안 / 운영

- **사내망(10.0.0.10) 한정** — compose 의 genai 서비스 포트 binding 을 host LAN IP 로 제한
- **Basic Auth** — FastAPI Depends 또는 nginx 사이드카, 자격 `.env.test`
- **업로드 제한** — 파일 ≤ 50MB/장, batch 당 ≤ 20장, 확장자 화이트리스트(png/jpg/webp), 일별 quota
- **provenance** — `genai_batches.prompt` + `genai_jobs.provider_job_id` 영속, dataset manifest 에 echo
- **partial failure 정책** — N 중 일부 실패 시 batch=`partial_success`, 성공한 pair 만 dataset 진입. UI 에서 실패 job 만 재시도

## 10. 단계별 롤아웃

### Phase 0 — 사전 결정 (코드 0)
- [x] API 키 빈칸으로 코드 진행 (사용자 후입력)
- [x] Higgsfield 접근 경로: fal.ai 채택
- [x] basic auth: 단순 user/pass env-based
- [x] 4h 텀 정책: 옵션 A (야간 무관)
- [x] 휴식 의미: 부하 분산/토큰 절약

### Phase 1 — Postgres 마이그레이션 + ingest 골격 (완료, commit 51433041)
- [x] `002_genai.sql` 작성 (Codex T1 패턴)
- [x] `postgres_dedup.py` candidate SELECT + `find_project_genai_pairs` 신규
- [x] `postgres_ingest_raw.py` conditional 컬럼
- [x] `postgres_genai.py` mixin (Phase 2/3 의존 메서드 일체)
- [x] `ops_register.py` manifest pass-through + fail-loud pre-validation
- [x] PG smoke 검증 (멱등 / GenAI pair JOIN / CHECK / FK CASCADE)

### Phase 2 — build asset CASE 분기
- [ ] `_build_project` 에 `case_genai_pairs()` 분기
- [ ] dataset manifest 의 entry 스키마 정착
- [ ] split_dataset 다운스트림 호환 확인

### Phase 3 — 첫 어댑터(Kling) + FastAPI 컨테이너 골격
- [ ] `docker/genai/` (FastAPI + Jinja2 + HTMX, 1 탭 1 엔진부터)
- [ ] `BaseGenAIAdapter` Protocol
- [ ] **Kling 어댑터** + Image→Video 탭만
- [ ] Dagster `genai_poll_sensor`
- [ ] e2e: UI submit (3장) → batch 1 + jobs 3 → 어댑터 → NAS → ingest → build → vlm-dataset 안에서 3 pair 확인

### Phase 4 — 나머지 3엔진
- [ ] Nanobanana (Vertex SA 재사용 → 빠름) — Image→Image 탭
- [ ] GPT Image (OpenAI key) — Image→Image 탭
- [ ] Higgsfield (fal.ai or replicate) — Image→Video 탭
- [ ] UI 2 탭 완성

### Phase 5 — 운영 강화
- [ ] quota / rate-limit
- [ ] 비용 모니터링 (`genai_batches.cost_units` 집계)
- [ ] 부분 실패 재시도 UI
- [ ] staging 안정화 → main PR

## 11. 위험 & 완화

| # | 위험 | 완화 |
|---|------|------|
| R1 | NAS partial-write race | `.partial → rename` atomic + `_manifest.json` 마지막 작성 |
| R2 | Higgsfield 직접 API 미보장 | fal.ai 또는 replicate 경유 확정 후 어댑터로 흡수 |
| R3 | 외부 API 비용 폭주 | 업로드 크기/장수 제한 + 일별 quota + `cost_units` 추적 |
| R4 | Kling 비동기 polling 장기 (>30분) | sensor timeout + `genai_jobs.status='failed'` + UI 재시도 |
| R5 | 일반 dataset 빌드와 GenAI 데이터 혼입 | `source_type` 필터로 명시 분리 (CASE 분기, Phase 2) |
| R6 | partial failure 시 dataset 일관성 | 성공한 pair 만 manifest 에 포함 |
| R7 | DuckDB read 가 Postgres 신규 컬럼/테이블 못 읽음 | `postgres_scanner` 자동 reflect, Phase 1 끝에 SELECT 검증 1회 |
| R8 | 같은 원본 이미지가 여러 batch 입력으로 재사용 | checksum dedup 자동 1행, `genai_jobs.input_asset_id` 다중 참조 |
| R9 | API 키 미설정 (Phase 3+ e2e 미가능) | mocking/스킵 모드, 사용자 키 입력 후 verify 명령 1회 |

## 12. 검증 체크리스트 (staging)

- [x] Postgres 마이그레이션 idempotent 재적용 OK (Phase 1)
- [ ] DuckDB `postgres_scanner` 가 `genai_batches`/`genai_jobs` SELECT
- [ ] N=3 batch e2e: 원본 3장 + 결과 3개 모두 raw_files + genai_jobs 정상 연결
- [ ] partial failure (N=3 중 1개 강제 실패) 시 batch=`partial_success`, 성공 2 pair 만 dataset 진입
- [ ] 4 엔진 각 탭에서 1회 e2e 성공
- [ ] dataset manifest 에 pair_id 단위 entry, `engine`/`prompt`/`provider_job_id` echo
- [ ] basic auth 차단
- [ ] quota 초과 시 422
- [ ] sensor polling 중 다른 Dagster job 과 Postgres 충돌 없음
- [ ] split_dataset 다운스트림 호환

---

## 부록: v1 → v3 진화 요약

| 항목 | v1 | v2 | v3 |
|------|----|----|----|
| DB 마이그레이션 | DuckDB + Postgres 양쪽 | Postgres 만 | Postgres 만 |
| submit 단위 | 1 job per submit | 1 job per submit | **1 batch = N jobs** (fan-out) |
| 입력 모드 | 엔진별 다양 | 엔진별 다양 | **이미지 N장 + 프롬프트 1개 통일** |
| 원본 저장 | UI 임시 | UI 임시 | **full ingest → raw_files (학습용)** |
| pair 추적 | 없음 | 없음 | **genai_jobs FK 2개** |
| UI | 1 화면 라디오 | 1 화면 라디오 | **2 탭 (Image→Video / Image→Image)** |
| 신규 컬럼 | `transfer_tool='kling'` 재사용 | `genai_engine` + `label_policy` | **+ `source_type`** |
| 신규 테이블 | 0 | `genai_jobs` | **`genai_batches` + `genai_jobs`** |
| build 분기 | label-free 단일 | label-free 단일 | **CASE: regular vs genai pairs** |
| sensor 동시성 | `duckdb_writer` tag 직렬화 | Postgres native | Postgres native |
| Imagen | 포함 | Imagen 4 + Imagen 3 fallback | **제거** (2026-06-30 discontinuation) |
