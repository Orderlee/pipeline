# GenAI Studio — Bulk Submit (N×M 대량 제출) 설계서

> 상태: 2026-05-13 dev 머지. PR 시리즈 (서버 → CLI → UI → wrapper) 합계 4단계.
> Codex 합의안: **Design C on top of A** (스키마 변경 없이, manifest JSON 명시 + 다중 batch 제출).

---

## 1. 사용 목적

대량의 이미지/프롬프트 조합을 한 번의 작업으로 제출해 결과를 묶음 단위로 관리.
"이미지 1장 + 프롬프트 1개 = batch 1개" 구조였던 기존 submit 의 N×M 확장.

## 2. 핵심 결정 (Codex 합의)

| 결정 | 채택안 | 이유 |
|------|--------|------|
| 데이터 모델 | **Design A** (multi-batch). N batch 가 같은 `bulk_group_id` 공유. | 스키마 무변경. 기존 `genai_batches.prompt` 한 컬럼 그대로. |
| 사용자 인터페이스 | **Design C** (canonical manifest JSON). 편의 입력은 CLI sugar. | 감사/재현 가능. dry-run 친화적. |
| 페어링 default | `paired` (1:1, N==M 필수) | "N:N" 한국어 ML 맥락 = items mapped 1:1. cartesian 폭주 방지. |
| 안전 게이트 | `--dry-run` (default) + 명시 `--confirm` | $3000+ 청구 사고 방지. |
| 한도 사전 체크 | CLI 가 `GET /genai/limits` 로 batch/bytes 잔여량 확인 후 제출 | 절반 진행 후 429 받는 UX 회피. |
| 그룹 식별 | `bulk_group_id` (영숫자/_/-/. 1~64자) — `options_json` 안에 저장 | 스키마 무변경, UI 칩 필터. |
| 운영 한도 노출 | `GET /genai/limits` JSON 신규 endpoint | per-user 24h 사용량 + 남은 headroom. PG 장애시 5xx (silent OK 금지). |

## 3. canonical manifest 포맷

```json
{
  "engine": "veo",
  "options": {"duration": "8", "aspect_ratio": "9:16"},
  "pairs": [
    {"image": "rel/path/a.png", "prompt": "..."},
    {"image": "rel/path/b.png", "prompt": "..."},
    {"image": null, "prompt": "txt2video prompt only"}
  ]
}
```

- `engine` — 5종 중 하나 (`kling`, `higgsfield`, `veo`, `nanobanana`, `gpt_image`).
- `options` — submit Form 의 `model_name` / `mode` / `duration` / `aspect_ratio` 등과 동일.
- `pairs[].image` — 절대 또는 상대 파일 경로. `null` 이면 text-only (Veo `mode=txt2video` 전용).
- `pairs[].prompt` — 비어있을 수 없음 (배포 측 가드 + CLI 측 검증).

## 4. CLI 진입점

```bash
# manifest 명시
./scripts/genai-cli.sh bulk-submit --file bulk.json --dry-run
./scripts/genai-cli.sh bulk-submit --file bulk.json --confirm

# 편의 입력 (prompts list + images dir)
./scripts/genai-cli.sh bulk-submit \
    --prompts-file prompts.json --images-from-dir ./imgs \
    --engine veo --pair-mode paired --dry-run

# veo txt2video bulk
./scripts/genai-cli.sh bulk-submit \
    --prompts-file p.json --text-only --engine veo --dry-run

# 셸 래퍼 (값을 파일 안에 미리 입력 후 실행)
./scripts/run-bulk.sh                 # dry-run
CONFIRM=true ./scripts/run-bulk.sh    # 실제 제출 (또는 파일 안에서 CONFIRM="true")
```

종료 코드:
- `0` — 성공 또는 dry-run
- `1` — 사용자 오류 / 사전 체크 실패 (한도 초과 등)
- `2` — 일부 batch 실패 (`partial`)

## 5. 서버 API

### `GET /genai/limits` (신규)
호출 사용자의 한도 현황. CLI bulk pre-check 용.

```json
{
  "limits": {"rate_limit_per_min": 0, "daily_batch_limit": 0, "daily_bytes_limit": 0},
  "usage": {
    "user": "user",
    "rate_limit":    {"per_min": 0, "used_60s": 0,  "remaining": null},
    "daily_batches": {"limit":   0, "used_24h": 26, "remaining": null},
    "daily_bytes":   {"limit":   0, "used_24h": 76013522, "remaining": null}
  },
  "per_batch": {"max_files": 20, "max_bytes_per_file": 52428800}
}
```

`remaining: null` = unlimited. PG 장애시 5xx 반환 (정직성 우선 — 절대 silent OK 금지).

### `POST /genai/batches` 확장
신규 form 필드: `bulk_group_id` (영숫자/_/-/. 1~64자). `options_json` 안에 저장됨.

### `GET /genai/batches?bulk_group_id=<bgi>` 확장
신규 query param. 일치하는 group 의 batch 만. 정규식 미일치 시 400.
PG TEXT 컬럼을 Python 측에서 파싱해 매칭 (malformed JSON 에 강건).
스캔 상한 `_LIST_BATCHES_MAX_SCAN = 10_000` rows.

## 6. UI

`/genai/batches` 페이지:
- 기존 `engine` / `status` chip 행에 **`group` chip 행** 추가
- 최근 500 batch 에서 추출한 distinct `bulk_group_id` 들 (top-20, 활성 그룹은 항상 포함)
- 각 row 에 group 컬럼 + 클릭 가능한 작은 chip (truncate 20자, hover title 풀텍스트)
- URL 공유 가능: `/genai/batches?engine=veo&status=succeeded&bulk_group_id=bgi-abc123`

submit 페이지 (`/`) 의 "최근 Batch" 사이드바는 변경 없음 — chip 줄이 너무 늘어나 시인성 저하.

## 7. 안전장치 정리

| 위치 | 동작 |
|------|------|
| CLI `--dry-run` (default) | `--confirm` 없으면 plan 만 출력하고 종료 |
| CLI 사전 체크 | `GET /genai/limits` → batch/bytes remaining 비교, 초과 시 거부 |
| CLI 파일 검증 | 모든 image 의 존재/크기/확장자 확인 후에야 첫 POST |
| CLI 입력 가드 | `--file` + `--prompts-file` 동시 사용 거부, `--text-only` + 다른 mode 거부 |
| 서버 정규식 | `bulk_group_id` POST & GET 모두 `[A-Za-z0-9_.-]{1,64}` 검사 |
| 서버 한도 | 기존 `check_rate_limit` + `check_daily_quota` 가 hard guard (CLI 사전 체크와 별개) |
| Throttle | `--sleep` (default 0.5s) batch 간 대기 |
| 에러 격리 | per-batch try/except (KeyboardInterrupt/SystemExit 만 propagate), `submitted[]` 정보 손실 방지 |

## 8. 비용 시나리오 (Veo 기준)

| 시나리오 | jobs | 예상 비용 (Veo 8s) |
|----------|------|--------------------|
| paired 30 prompts × 1 image | 30 | ~$96 |
| cartesian 5 images × 50 prompts | 250 | ~$800 |
| cartesian 20 images × 100 prompts | 2,000 | ~$6,400 |

→ **반드시 dry-run 으로 batches/jobs/비용 확인 후 `--confirm`**. 환경변수 `GENAI_DAILY_BATCH_LIMIT` 도 production 가기 전 적정값 설정 권장.

## 9. 향후 follow-up

| 항목 | 우선순위 | 비고 |
|------|----------|------|
| Per-job prompt (Design B) — `genai_jobs.prompt_override` 컬럼 신설 | 중 | 대량 cartesian 의 DB 효율 향상 |
| `options_json` → `JSONB` 컬럼 + GIN 인덱스 | 중 | bulk_group_id 검색 SQL 측 처리 가능 |
| per-bulk-run job-count 상한 (`GENAI_MAX_BULK_JOBS`) | 높 | production cost 가드 |
| UI 측 manifest 업로드 폼 | 낮 | CLI 사용자 주력이라 ROI 낮음 |
| 그룹 단위 retry/abort | 중 | bulk_group_id 기반 일괄 작업 |
| TZ 통일 (server now() vs PG submitted_at) | 중 | UTC 가정 검증 필요 |

## 10. PR 시리즈

| 단계 | PR | 핵심 |
|------|----|------|
| Step 1 | (이 PR 안) | 서버 `GET /genai/limits` + `bulk_group_id` Form 필드 + `_daily_usage` 리팩터 |
| Step 2 | (이 PR 안) | `bulk-submit` 서브커맨드 (manifest/편의 입력, dry-run/confirm, precheck) |
| Step 3 | (이 PR 안) | UI `bulk_group` chip 행 (+ pg.py iterative fetch, top-20 cap) |
| Step 4 | (이 PR 안) | `scripts/run-bulk.sh` (CONFIRM 게이트, 모드 A/B 자동 분기) |
| Step 5 | (이 PR 안) | 이 문서 |

각 단계마다 Codex 코드 리뷰 통과 (총 3회):
- Step 1.5: silent fallback 제거, `_daily_usage` 공유, regex `.` 허용
- Step 2.5: broad Exception 캐치, manifest+prompts 동시 사용 거부, --text-only 충돌 mode 거부
- Step 3.5: bulk_group_id GET regex 검증, iterative fetch (10K row 상한), `.gs-chip--tiny`, chip top-20 + pin
