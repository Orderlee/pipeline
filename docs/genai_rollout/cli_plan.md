# GenAI Studio CLI 계획서

**작성**: 2026-05-12 / Opus + Codex 합의
**대상 환경**: staging (`feature/genai-studio` branch)
**목적**: 웹 UI (`http://10.0.0.10:8088`) 동일 작업을 터미널에서 수행 가능한 CLI

---

## 1. 목표 / 비목표

### 포함
- 단일 명령 `genai-cli` (또는 `genai`) 로 batch 생성·조회·재시도·비용 집계
- 비대화 모드 (CI / cron / 스크립트 자동화)
- 동기적 진행 대기 (`--wait`) + 별도 `wait` 명령 (resume 가능)
- 환경변수 / config / 프롬프트 3단 인증
- 사람용 table + 기계용 JSON 양쪽 출력
- 디렉토리 일괄 입력 (`--images-from-dir`)

### 제외
- UI 의 visual feature (썸네일 미리보기, 토스트 등)
- CLI 자체의 비즈니스 로직 — REST API 가 single source of truth (CLI 는 thin client)
- 외부망 노출 — 사내망 + REST API auth 의 신뢰모델 그대로

---

## 2. 핵심 설계 결정 (Codex 합의)

| # | 영역 | 선택 | 사유 |
|---|------|------|------|
| 1 | 접근 방식 | **HTTP client** (`requests` 로 REST API 호출) | 컨테이너의 auth/quota/rate-limit 재사용. CLI 가 비즈니스 로직 비대해지지 않음 |
| 2 | 명령 구조 | **git-style 서브커맨드** (`genai submit kling`, `genai batches list`) | REST 리소스에 1:1 매핑, 명령어 기억 쉬움 |
| 3 | 진행 추적 | **`--wait` flag + 별도 `wait` 명령** | 사람 = 한 번에, CI = batch_id 보존 후 별도 resume |
| 4 | 인증 보관 | **env > config > prompt** 3단 우선순위 | env = CI, config = 대화형 운영자, prompt = 첫 실행 |
| 5 | 출력 포맷 | **TTY 자동감지 + `--json`/`--table` override** | CI/스크립트 안전, 사람 친화 |
| 6 | 일괄 입력 | **`--images-from-dir <DIR>`** (alphabetical sort) | shell glob 회피, 순서 명확, validation 가능 |
| 7 | 라이브러리 | **argparse (stdlib)** | 기존 `scripts/*.py` 와 일관성, 의존성 0 |

---

## 3. 명령어 표 (final)

| 명령 | 매핑 | 설명 |
|------|------|------|
| `genai-cli submit <engine> --image <PATH> [...]` | `POST /genai/batches` | 1장 이미지 batch 제출 |
| `genai-cli submit <engine> --images-from-dir <DIR> [...]` | `POST /genai/batches` | 디렉토리 안 N장 일괄 |
| `genai-cli batches list [--status X --engine Y --limit N]` | `GET /genai/batches` (API 추가 필요) | batch 목록 |
| `genai-cli batches show <batch_id>` | `GET /genai/batches/{id}` (API 추가 필요) | batch + jobs 상세 |
| `genai-cli wait <batch_id> [--timeout SEC --interval SEC]` | `GET /genai/batches/{id}` poll | done/failed/partial 까지 polling |
| `genai-cli jobs retry <job_id>` | `POST /genai/jobs/{id}/retry` | 실패 job 재시도 |
| `genai-cli costs [--range day|week|month]` | `GET /genai/costs` (JSON 응답 필요) | 비용/활동 집계 |
| `genai-cli engines` | `GET /healthz` | 사용 가능 엔진 목록 |
| `genai-cli config init` | (로컬) | `~/.config/genai-cli/config.toml` 초기화 |

### 공통 옵션
- `--base URL` — API base (default `http://10.0.0.10:8088` 또는 config)
- `--json` / `--table` — 출력 포맷 강제
- `-q` / `--quiet` — 에러만 표시
- `-v` / `--verbose` — debug 로그

### `submit <engine>` 옵션 (engine 별 매핑)
공통:
- `--prompt TEXT` (필수, 또는 `--prompt-file FILE`)
- `--image PATH` 또는 `--images-from-dir DIR`
- `--wait` — 결과 도착까지 대기 (default: 즉시 반환)
- `--timeout SEC` (default 900, `--wait` 와 함께)
- `--interval SEC` (default 10, polling 주기)
- `--max-batch INT` (default 20, dir 입력 시 한 batch 당 한도)

engine 별:
- **kling**: `--model`, `--mode std|pro|master`, `--duration 5|10`, `--aspect-ratio 16:9|9:16|1:1`
- **veo**: `--model`, `--aspect-ratio 16:9|9:16`, `--duration 4|6|8`
- **higgsfield / nanobanana / gpt_image**: 추가 옵션 없음

---

## 4. 출력 포맷

### TTY 자동감지
```python
if sys.stdout.isatty(): fmt = "table"
else: fmt = "json"
```
`--json` / `--table` 명시 시 강제.

### 예시 — `genai-cli batches list`

**table** (TTY):
```
BATCH_ID         ENGINE      STATUS        JOBS (✓/✗/T)   SUBMITTED
0bd7bea6-6f4    kling       running       0 / 0 / 1     2026-05-12 14:32:01
caffafc4-e93    nanobanana  failed        0 / 1 / 1     2026-05-12 15:00:00
```

**json** (pipe):
```json
[
  {"batch_id":"0bd7bea6-6f4","engine":"kling","status":"running","n_total":1, ...},
  {"batch_id":"caffafc4-e93","engine":"nanobanana","status":"failed","n_total":1, ...}
]
```

### 예시 — `genai-cli wait`

```
$ genai-cli wait caffafc4-e93
[polling] caffafc4-e93 · interval=10s · timeout=900s
running (0/0/1) ... 1m12s
failed  (0/1/1) ... 1m21s

batch caffafc4-e93 failed:
  job caffafc4-e93-001 → 400 INVALID_ARGUMENT...

exit code: 2
```

종료 코드:
- 0 = succeeded
- 1 = client error (인증/입력)
- 2 = job(s) failed
- 3 = partial success
- 4 = timeout (대기 중 한도 초과)

---

## 5. 인증 / 설정

### 우선순위 (env > config > prompt)

1. **env**: `GENAI_CLI_BASE`, `GENAI_CLI_USER`, `GENAI_CLI_PASS`
2. **config**: `~/.config/genai-cli/config.toml` (`chmod 0600`)
   ```toml
   [default]
   base = "http://10.0.0.10:8088"
   user = "user"
   password = "..."
   ```
3. **prompt**: 위 둘 모두 없으면 첫 실행 시 `getpass` 로 입력 받고 config 에 저장 여부 확인

### 보안
- 비밀번호는 **CLI flag 로 받지 않음** (shell history 누출 방지)
- config 파일 mode 0600 강제 (mode 0644 면 경고 후 거부)
- `--no-keyring` 으로 keyring 통합 비활성화 가능 (기본은 keyring 없음, 단순 config)

---

## 6. 디렉토리 입력 정렬

```
genai-cli submit kling --images-from-dir ./batch/ --prompt "..."
```

- 확장자 화이트리스트: `.png .jpg .jpeg .webp`
- 정렬: **alphabetical** (locale-independent), `seq_in_batch` 가 정렬 결과 그대로 매핑
- 20장 초과 시 에러 (또는 `--max-batch` 로 override → 그 한도 안에서 truncate)
- subdir 미재귀 (한 디렉토리 안만)

---

## 7. CI / 자동화 패턴

### 1장 batch + 대기 + 결과 다운로드
```bash
batch_id=$(genai-cli submit veo --image scene.png --prompt "..." --json | jq -r .batch_id)
genai-cli wait "$batch_id" --timeout 600
# 종료 코드로 분기
[[ $? -eq 0 ]] && echo "OK" || exit 1
```

### N장 batch 디렉토리 일괄
```bash
for engine in kling veo; do
  genai-cli submit "$engine" --images-from-dir ./batch_test/ \
    --prompt "$(cat prompt.txt)" --wait --timeout 900
done
```

### 야간 cron — 결과 batch 정리
```cron
30 23 * * * /usr/local/bin/genai-cli batches list --status done --json \
            | jq -r '.[].batch_id' \
            | xargs -I {} ./sync_outputs.sh {}
```

---

## 8. 코드 구조

```
scripts/genai_cli/
  __init__.py
  __main__.py            # python -m genai_cli 진입점
  cli.py                 # argparse 라우터
  client.py              # HTTPClient (requests + auth)
  config.py              # env > toml > prompt 우선순위
  output.py              # JSON/table 렌더링 (TTY 감지)
  commands/
    __init__.py
    submit.py            # submit <engine> ...
    batches.py           # batches list / show
    wait.py              # wait <batch_id>
    jobs.py              # jobs retry
    costs.py             # costs --range
    engines.py           # engines (healthz)
    config_cmd.py        # config init / show
tests/unit/genai_cli/
  test_config.py
  test_client.py
  test_output.py
  test_submit.py         # mock requests
```

배포:
- `scripts/genai_cli/` 자체로 실행 가능: `python -m genai_cli ...`
- 또는 `scripts/genai-cli` shim (shebang + `python -m genai_cli`)
- pyproject.toml 의 [project.scripts] 에 등록할지는 follow-up (현재는 stdlib 만으로)

---

## 9. REST API 변경 필요 항목

CLI 가 동작하려면 API 측에 일부 endpoint 가 JSON 응답을 지원해야 함:

| Endpoint | 현재 | 필요 |
|----------|------|------|
| `GET /healthz` | JSON ✓ | 그대로 |
| `POST /genai/batches` | HTTP 303 redirect (HTML) / JSON (Accept: json) ✓ | 그대로 |
| `GET /genai/batches` | HTML 만 | **JSON 응답 추가** (Accept: application/json 분기) |
| `GET /genai/batches/{id}` | HTML 만 | **JSON 응답 추가** |
| `GET /genai/costs` | HTML 만 | **JSON 응답 추가** |
| `POST /genai/jobs/{id}/retry` | JSON ✓ | 그대로 |

각 endpoint 에 Accept 헤더 분기 추가 (이미 `submit` 에서 사용 중인 패턴) — small change.

---

## 10. 위험 / 완화 (Codex 합의)

| # | 위험 | 등급 | 완화 |
|---|------|------|------|
| R1 | CLI 동작이 REST API semantic 와 drift | HIGH | CLI 를 thin client 로 유지 (1:1 endpoint 매핑, 로컬 로직 0) |
| R2 | async batch_id 분실 → wait 재개 불가 | HIGH | submit 직후 stdout 첫 줄에 batch_id 출력 (`--quiet` 와도 분리 stderr 가능) |
| R3 | 비밀번호가 shell history 에 노출 | MED | `--password` flag 금지. env / config / prompt 만 |
| R4 | config 파일이 git 에 commit | MED | `~/.config/...` 경로 사용, 운영자 git scope 밖 |
| R5 | TTY 자동감지가 일부 CI 환경에서 잘못 판단 | LOW | `--json`/`--table` 명시 override 항상 가능 |
| R6 | `submit` 시점에 컨테이너 다운 → connection error | LOW | retry 1-2회 + 명확한 에러 메시지 + exit code 1 |

---

## 11. 단계별 롤아웃

### Phase 1 — 핵심 client + submit (1세션, ~2시간)
- [ ] `scripts/genai_cli/` 디렉토리 + 골격 (cli.py, client.py, config.py)
- [ ] `genai-cli submit <engine> --image <PATH> --prompt "..."` 동작
- [ ] env / config / prompt 인증
- [ ] TTY auto-detect 출력
- [ ] e2e 1회 (mocking 모드 nanobanana 로 즉시 검증)

### Phase 2 — 나머지 명령 + 일괄 입력 (1세션)
- [ ] `--images-from-dir`
- [ ] `batches list / show`, `jobs retry`, `costs`, `engines`, `wait`
- [ ] REST API 의 JSON 응답 분기 추가 (4 endpoint)
- [ ] table renderer (자동 width)

### Phase 3 — 강화 + 테스트 (1세션)
- [ ] `config init` 인터랙티브
- [ ] unit tests (requests mock 으로 client 검증)
- [ ] 종료 코드 정리 (0/1/2/3/4)
- [ ] README + 운영 가이드

### Phase 4 — 운영
- [ ] staging 에서 실제 호출 1회 (각 엔진)
- [ ] cron / CI 통합 시나리오 검증
- [ ] main PR 머지 시 동봉

---

## 12. 검증 체크리스트

- [ ] `genai-cli submit nanobanana --image test.png --prompt "test" --wait` 동작 (동기 엔진)
- [ ] `genai-cli submit kling --image test.png --prompt "test"` 비동기, batch_id 즉시 출력
- [ ] `genai-cli wait <batch_id>` 별도 호출로 resume 가능
- [ ] `genai-cli batches list --status running --json | jq .[0]` 파이프 안전
- [ ] CI 환경 (TTY 없음) → JSON 자동
- [ ] config 파일 chmod 0644 시 경고
- [ ] `--images-from-dir` 가 20장 초과 시 명확한 에러
- [ ] 컨테이너 다운 시 connection error + exit 1
- [ ] 모든 endpoint 가 JSON / HTML 모두 응답 (Accept 헤더 분기)

---

## 13. 다음 단계

사용자 승인 후 진행:
1. **이 계획서 확인 + 명령어 구조/이름 검토**
2. **Phase 1 코드 작성** — staging 에서 실제 동작 가능한 minimum (submit + auth)
3. **Phase 2-3** 순차

승인 신호 주시면 Phase 1 코드 작성 시작합니다.
