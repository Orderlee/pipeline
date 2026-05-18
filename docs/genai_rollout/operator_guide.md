# GenAI Studio — 운영 가이드 (Phase 5)

staging 안정화 + main 승격 직전 운영자가 확인할 항목 정리.

---

## 1. API 키 입력

`docker/.env.test` 의 다음 항목을 실제 값으로 교체 (git 미추적, 호스트 직접 편집).

| 키 | 발급처 | 비고 |
|----|--------|------|
| `KLING_ACCESS_KEY` / `KLING_SECRET_KEY` | klingai.com 콘솔 | JWT 서명용 (HS256) |
| `FAL_KEY` | fal.ai 대시보드 | Higgsfield 모델 호출 |
| `OPENAI_API_KEY` | platform.openai.com | gpt-image-1 호출 |
| `HIGGSFIELD_FAL_MODEL` | fal.ai 대시보드에서 정확한 모델 ID 확인 (Codex HIGH) | 기본 `fal-ai/higgsfield/i2v-soul` 검증 필요 |
| `GEMINI_GOOGLE_APPLICATION_CREDENTIALS` | 기존 `/app/credentials/your-gcp-project-*.json` 재사용 | Nanobanana 용 |
| `GENAI_BASIC_AUTH_USER` / `GENAI_BASIC_AUTH_PASS` | 운영자 임의 | 사내망 사용자 인증. 미설정 시 503 (또는 `GENAI_AUTH_DISABLED=true` 명시 우회) |
| `GENAI_INTERNAL_TOKEN` | 임의 secret (32자+ 권장) | Dagster sensor ↔ genai 컨테이너 통신용 |

키 입력 후:

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline_test/docker
docker compose --profile genai up -d genai
docker compose restart dagster dagster-code-server
```

## 2. e2e 검증 (mocking → 실 키)

### 2-1. Mocking 모드 e2e

키 미설정 상태에서도 모든 흐름 검증 가능.

```bash
# 컨테이너 부팅
docker compose --profile genai up -d genai

# UI 접속 → 사내망에서
http://10.0.0.10:8088/

# 또는 curl 로 직접
curl -u user:pass -F "engine=kling" -F "prompt=cat jumping" \
  -F "files=@/path/to/img1.png" -F "files=@/path/to/img2.png" \
  http://10.0.0.10:8088/genai/batches
```

확인:
- Postgres `genai_batches` / `genai_jobs` 에 row 생성
- `/nas/staging/incoming/genai/<batch_id>/originals/` 에 atomic write 된 파일
- (Kling/Higgsfield) 약 2초 후 `outputs/<seq>.mp4` (placeholder)
- (Nanobanana/GPT Image) submit 즉시 `outputs/<seq>.png`
- INGEST sensor 픽업 → `raw_files` 안에 `genai_source` / `genai_output` rows
- build asset 실행 시 `vlm-dataset/genai_<batch_id>/` 에 paired manifest

### 2-2. 실 키 검증 (엔진별 1회씩)

각 엔진 별 1 batch (1장 입력) 로 비용 최소화하며 확인:

```bash
# Kling
curl ... -F "engine=kling" -F "prompt=test" -F "files=@cat.png" /genai/batches
# 약 5-10분 후 (실 API 응답) outputs/001.mp4 안착 + status='done'

# Higgsfield
curl ... -F "engine=higgsfield" ...

# Nanobanana — 동기, 즉시 응답
curl ... -F "engine=nanobanana" ...

# GPT Image — 동기, 즉시 응답
curl ... -F "engine=gpt_image" ...
```

## 3. Dagster sensor 활성화

Dagster UI (`http://10.0.0.10:3031`) 에서 `genai_poll_sensor` 를 **수동 ON** 으로 전환.
default_status=STOPPED 라 자동 시작 안 됨.

상태 확인:
- 평소: `SkipReason: 폴링 대상 GenAI jobs 없음` 매 30s
- 비동기 batch 진행 중: `polled=N done=X failed=Y running=Z`
- 토큰 미설정: `GENAI_INTERNAL_TOKEN 미설정 — sensor 비활성`
- 컨테이너 다운: `genai 컨테이너 조회 실패`

## 4. 운영 한도 활성화 (선택)

기본 0 = off. 사내망 신뢰 가정. 외부망 노출 또는 비용 폭주 우려 시:

```env
GENAI_RATE_LIMIT_PER_MIN=10        # 사용자별 분당 10건
GENAI_DAILY_BATCH_LIMIT=50         # 사용자별 일별 50 batch
GENAI_DAILY_BYTES_LIMIT=5368709120 # 사용자별 일별 5GB
```

초과 시 429 응답.

⚠ **Single-container 전제** (Codex Q1 MED) — `GENAI_RATE_LIMIT_PER_MIN` 은 in-memory sliding
window 라 genai 컨테이너 인스턴스별로 분리됨. 워커 증설 시 Postgres/Redis 기반 분산 rate
state 로 이전 필수. 현재 staging/prod 모두 단일 컨테이너 정책 → 그대로 사용 가능.

## 5. 비용 모니터링

`/genai/costs?range=day|week|month` UI 또는:

```sql
-- 엔진별 7일 집계
SELECT b.engine,
       COUNT(j.*) FILTER (WHERE j.status='done') AS done,
       COUNT(j.*) FILTER (WHERE j.status='failed') AS failed,
       SUM(j.cost_units) AS total_cost
  FROM genai_jobs j
  JOIN genai_batches b ON b.batch_id = j.batch_id
 WHERE j.submitted_at > now() - interval '7 days'
 GROUP BY b.engine;
```

실 API 호출별 cost_units 정확도는 어댑터의 cost 추출 정확성에 의존. Phase 6 follow-up 으로 정밀 계산 가능.

## 6. 부분 실패 재시도

batch detail UI 에 `retry` 버튼 노출 (status='failed' 인 job 만).
또는:

```bash
curl -u user:pass -X POST http://10.0.0.10:8088/genai/jobs/<job_id>/retry
```

원본 input image 는 NAS 에 보존되어 있어야 함 (archive 이동 후엔 410).
재시도는 새 provider_job_id 발급 + status='submitted'.

## 7. main 승격 체크리스트

dev 에서 staging 안정화 후 main PR 생성 전:

- [ ] `KLING_*` / `FAL_KEY` / `OPENAI_API_KEY` / `GEMINI_*` 모두 prod `docker/.env` 에도 입력
- [ ] `GENAI_INTERNAL_TOKEN` 은 prod 와 staging 다른 값
- [ ] `GENAI_BASIC_AUTH_*` 채우거나 `GENAI_AUTH_DISABLED=false` 명시
- [ ] `GENAI_RATE_LIMIT_PER_MIN` / `GENAI_DAILY_*` prod 정책 결정
- [ ] `HIGGSFIELD_FAL_MODEL` 검증된 ID 로 교체
- [ ] e2e 4 엔진 각 1회 성공 (실 키)
- [ ] Dagster `genai_poll_sensor` ON 후 24시간 stable (SkipReason 정상 흐름)
- [ ] Postgres 에 `002_genai.sql` 적용 + 인덱스 4개 존재 확인
- [ ] `vlm-genai-jobs` MinIO 버킷은 사용 안 함 (NAS 만 사용 — v3 결정)
- [ ] `docs/genai_rollout/plan.md` 의 Phase 5 체크리스트 모두 ✓
- [ ] dev → main PR. 본문에 "GenAI Studio v0.1 — 4 엔진" + 변경 commit 범위 (51433041 ~ HEAD)
- [ ] PR 리뷰 통과 후 squash merge → 자동 deploy-production.yml 트리거

⚠ **금기 사항 재확인** (CLAUDE.md): prod 호스트의 `src/` / `configs/` / `scripts/` /
`docker-compose*.yaml` / `Dockerfile` 수동 편집 금지 — 다음 CI 배포의 `rsync --delete`
+ `git reset --hard` 로 소실됨. **반드시 git commit → push → CI 자동 배포** 경로로만 반영.
`.env` / `.env.test` 만 호스트에서 직접 편집 (git 미추적).

## 8. 트러블슈팅

| 증상 | 원인/조치 |
|------|-----------|
| UI 접속 시 503 | `GENAI_BASIC_AUTH_USER/PASS` 미설정. 또는 `GENAI_AUTH_DISABLED=true` 명시 |
| submit 시 401 | basic auth 자격 불일치 |
| submit 시 415 | 파일 확장자 화이트리스트 외 (png/jpg/webp 만) |
| submit 시 413 | 파일 크기 / batch 장수 초과 |
| submit 시 429 | rate-limit 또는 daily quota 초과 |
| job status 가 'pending' 에서 안 바뀜 | adapter.submit() 실패 또는 PG 트랜잭션 롤백 — `docker logs genai` 확인 |
| job status 'submitted' 에서 안 바뀜 | Dagster `genai_poll_sensor` 가 STOPPED. UI 에서 수동 ON 또는 `/internal/jobs/.../poll` 직접 호출 |
| outputs/_manifest.json 안 생김 | 모든 job 이 done/failed 가 아닌 상태. `genai_jobs` status 확인 |
| build asset 이 GenAI batch 를 못 잡음 | source_unit_name='genai_<batch_id>' 가 아닌 다른 값. 또는 `find_project_genai_pairs` 가 input/output asset_id NULL → ops_register 의 manifest items[].seq 매칭 warn 확인 |
| nanobanana 가 "block_reason" 으로 실패 | safety filter — 프롬프트/이미지 변경 |
| Higgsfield submit 'model not found' | `HIGGSFIELD_FAL_MODEL` ID 잘못. fal.ai 대시보드 확인 |

## 9. 알려진 제약 / Phase 6 follow-up 후보

- `fal-ai/higgsfield/i2v-soul` 모델 ID 확정 검증 필요
- Vertex SA 의 prod 권한 분리 (현재 staging/prod 동일 `your-gcp-project`)
- BackgroundTask 의 finalize 가 컨테이너 재시작 시 누락 가능 — 실제 production 안정화 후 sensor 가 fallback finalize 도 가능하게 확장
- partial retry 가 archive 이동된 원본은 못 다룸 — archive 에서 원복하는 path 추가 검토
- `GENAI_DAILY_BYTES_LIMIT` SQL 이 options_json 정규식 파싱 — 추후 별도 column 으로 분리 권장
