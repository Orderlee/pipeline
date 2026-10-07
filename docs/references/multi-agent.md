# Multi-Agent Coding Environment

멀티 에이전트 작업의 **모델 tier 일반 원칙**(역할·라우팅·effort·에스컬레이션). 구체적 페르소나 명단과 라우팅표는 [`agent-teams.md`](agent-teams.md), 페르소나 정의 정본은 [`.claude/agents/*.md`](../../.claude/agents/) 다. 이 문서는 tier 위에 도메인 페르소나가 얹힌 구조의 하위 레이어다.

> **버전 표기 규칙**: 모델 버전 숫자는 문서에 박지 않는다(곧 stale). `opus`/`sonnet`/`haiku`/`fable` 별칭과 Codex tier 이름(`luna`/`terra`/`sol`/`astra`)만 쓴다. 실제 모델은 각 에이전트 frontmatter `model:` 이 진실이고 별칭은 그 시점의 최신으로 resolve 된다. Codex 최신 slug 는 `~/.codex/models_cache.json` 에서 호출 시점에 해석한다.

---

## 1. 아키텍처 개요

| Tier | 모델 | 역할 |
|---|---|---|
| 리더 | Opus | 오케스트레이터 + 고추론 판단(설계·모델 과학·QA 전략·보안) |
| 구현 | Sonnet | 도메인 구현 페르소나 다수, 연구 해석 |
| 관찰 | Haiku | 값싼 런타임 관찰(`ops-engineer`), 대량 증거 수집(`research-scout`) |
| 연구 최상위 | Fable | 연구 프레임 재설정 전용(`principal-research-architect`), 호출 매우 드묾 |
| 교차 검증 | Codex(GPT 계열) | 다른 패밀리의 검증자 / 듀얼 작성자 |

리더는 코드를 직접 많이 쓰지 않는다 — 분해, 라우팅, 통합, 최종 검토에 집중한다.

---

## 2. 모델 역할

### 2.1 Opus — 리더 아키텍트
- 페르소나: `cto`(오케스트레이터), `platform-architect`·`ai-modeler`·`qa-strategist`·`db-architect`·`security-architect`, `methodology-auditor`(독립 감사). main session 이 오케스트레이터일 때도 이 tier.
- 책임: 분해, 아키텍처·기술 선택, 라우팅, 통합, 최종 리뷰, 하위가 모두 실패한 문제. 호출 빈도 낮음(약 10–20%). 보일러플레이트·단일 파일 버그·테스트 추가에는 부르지 않는다.

### 2.2 Sonnet — 구현자
- 페르소나: `data-engineer`·`dataops-engineer`·`ai-data-engineer`·`ai-engineer`·`mlops-engineer`·`dagster-impl`·`perf-engineer`·`viz-engineer`(구현), `pipeline-explorer`·`deploy-auditor`·`tech-scout`(읽기 전용), `research-scientist`, `codex`·`codex-router`(Codex 연락 창구).
- 책임: 리더가 정한 인터페이스대로 구현·테스트·마이그레이션 파일·단일 파일 리팩토링. 호출 빈도 높음(약 55–65%).
- 제약: 자기 코드를 자기가 리뷰하지 않음, 아키텍처 결정 금지, 도메인 밖 변경은 리더에게 에스컬레이션.

### 2.3 Codex — 교차 검증자
- `codex` 페르소나(검증 연락 창구)와 `codex-router`(구현·진단 계약을 tier 로 라우팅)가 Codex MCP 로 호출. 모델은 **미고정** — `codex` 는 `model` 파라미터를 넘기지 않는다(CLI 기본값 = 계정이 쓸 수 있는 최신). `codex-router` 는 tier 별 최신 slug 를 호출 시점에 해석하고 구버전으로 fallback 하지 않는다.
- 핵심 원칙: **"또 다른 일꾼"이 아니라 "다른 관점"** — Sonnet 과 같은 작업을 동시에 시키지 않는다(병렬 풀이는 `codex_arbitration` 스킬, 패턴 B).
- 책임: 구현자 코드의 독립 리뷰, 어려운 문제의 두 번째 의견, Anthropic 패밀리 공통 사각지대 발견. 호출 빈도 중간(약 15–20%).
- effort 는 §3.3.

### 2.4 Haiku — 관찰자
- `ops-engineer`: 헬스체크, 실패 로그(JSONL) 트리아지, DB 상태 스냅샷, 컨테이너·센서 liveness, MinIO 객체 존재. **읽기·보고 전용** — 깨진 것은 고치지 않고 담당 페르소나로 라우팅만 한다. 고정 호출 비율 없음.

---

## 3. 라우팅 규칙

### 3.1 작업 유형별 매트릭스

| 작업 유형 | 1차 | 2차 / 검증 |
|---|---|---|
| 요구 분석·작업 분해, 아키텍처·기술 선택 | Opus | — |
| 모듈 구현, 단위 테스트, CRUD, 단일 파일 리팩토링, 문서, 대량 반복 변형 | Sonnet | (선택) Codex 리뷰 |
| 보안 민감 코드 | Sonnet | Codex 리뷰 + Opus 최종 |
| 어려운 알고리즘 | Sonnet + Codex 듀얼 | Opus 중재 |
| 까다로운 디버깅 | Sonnet | 실패 시 Codex → 둘 다 실패 시 Opus |
| 코드 리뷰 일반 / 머지 전 최종 | Codex / Opus | — |
| 런타임 헬스체크·상태 스냅샷·로그 트리아지 | Haiku | 이상 시 담당 tier 로 라우팅 |

### 3.3 Codex 추론 강도 매트릭스
사다리: `low < medium < high < xhigh < max < ultra`. MCP `effort` 파라미터는 `medium`/`high`/`xhigh` 만 받으므로(하향 전용) **`max`/`ultra` 는 호출마다 `config: {"model_reasoning_effort": "<level>"}` override 로 명시한다** — `~/.codex/config.toml` 은 사용자 터미널과 공유라 그 기본값에 기대지 않는다. 해석된 모델의 `supported_reasoning_levels` 에 없는 레벨이면 그 모델의 최고 레벨을 쓴다. `low` 는 검증자에 부적합해 **금지**(비용 통제는 호출 자체를 빼는 것으로).

| effort | 전달 | 용도 |
|---|---|---|
| `ultra` | config override | 보안·인증·암호화, 결제·트랜잭션, 어려운 알고리즘 정확성(패턴 B), 막힌 디버깅 second opinion, 스키마·마이그레이션 검증 |
| `max` (기본) | config override | 머지 전 일반 리뷰, 동시성·락·race, 외부 API 통합, 50줄 이상 변경 |
| `xhigh`/`high` | `effort` | `max` 가 과한 루틴 리뷰(검증자 재량) |
| `medium` | `effort` | 50줄 미만, 스타일·관용, 테스트 자체 품질, 문서·주석 정확성 |

명시 없으면 `max`. 두 행에 걸치면 더 높은 쪽(예: 보안 관련 작은 변경 → `ultra`).

---

## 4. 워크플로우 패턴

### 4.1 패턴 A — 계획·분배·통합(기본)
Opus 분해 → 서브에이전트 분배(병렬 가능) → (선택) Codex 리뷰 → Opus 통합·최종 검토.

### 4.2 패턴 B — 듀얼 솔루션 + 중재
같은 어려운 문제를 Sonnet 과 Codex 가 **서로의 답을 보지 않고** 풀고 Opus 가 비교·선택·합성. 알고리즘·까다로운 버그·설계 트레이드오프.

### 4.3 패턴 C — 작성자/리뷰어 분리
리뷰어는 작성자와 다른 패밀리. 페르소나 레벨에서도 동일 — 모델을 학습·서빙하는 페르소나가 그 검증까지 겸하지 않는다(`agent-teams.md` §3.6).

### 4.4 패턴 D — 에스컬레이션
Sonnet(최대 N회) → Codex → Opus. 최대 3단계.

---

## 5. 컨텍스트 관리

- 전체 코드베이스는 Opus(main)만 보유. 서브에이전트에는 작업에 직접 관련된 파일만 전달한다.
- 항상 포함: 작업 명세(목표·제약·성공 기준), 출력 스키마, 관련 파일, 해당 컨벤션, prod/staging 구분.
- 포함 금지: 무관한 파일·이력, 다른 서브에이전트의 원답(오염 방지 — 통합된 요약만), 개인정보·비밀키.

---

## 6. 출력 포맷

### 6.1 표준 응답 스키마(구현 페르소나)
```json
{"status": "success | partial | failed", "summary": "", "files_changed": [{"path": "", "action": "created | modified | deleted"}],
 "tests_added": [], "tests_passing": true, "open_questions": [], "assumptions": [], "next_steps_suggested": []}
```
읽기 전용·감사·연구 페르소나는 각자 정의된 리포트 형태를 쓰고 `files_changed` 는 비운다. `tests_passing` 은 실제 pytest 종료 코드여야 한다.

### 6.2 코드 변경
산문 지시("이 부분을 이렇게") 금지 — 파일 전체 또는 unified diff 만.

---

## 7. 에스컬레이션 정책

### 7.1 Sonnet → Codex
N회(기본 2) 시도 후 테스트 실패, 명시적 "확신 없음", 알고리즘 정확성이 중요한 작업.

### 7.2 Codex → Opus
두 모델 답이 의미 있게 다름, 둘 다 테스트 미통과, 보안·성능 영향이 큰 결정.

### 7.3 즉시 Opus
새 모듈·서비스 설계, 의존성 선택, 데이터 모델·API 계약 변경, 사용자의 "전체 검토" 요청.

---

## 8. Opus 부재 폴백

Opus 호출 불가(쿼터·비용·가용성) 시 Sonnet 페르소나(주로 `data-engineer`/`dagster-impl`)가 임시 오케스트레이터가 된다. 약한 영역: 복잡한 아키텍처 트레이드오프, 막힌 디버깅, 장시간 자율 실행, 보안 리뷰 최종 관문.
규칙: ① 작업을 더 잘게 쪼갠다 ② 불확실한 결정은 보류하고 [`.agent/decisions.log`](../../.agent/decisions.log) 에 근거와 함께 기록 ③ Codex 교차 검증 빈도를 높인다 ④ 데이터 모델·외부 API 계약·보안·prod 배포 영향·**모델 승격/eval-gate 판단**은 사용자 확인 후 진행(자동 승격 금지). Opus 복귀 시 보류 항목을 우선 재검토한다.

---

## 9. 실패 모드와 모니터링

- 가짜 성공 → 출력 스키마 강제 + 테스트를 실제로 실행해 검증. 컨텍스트 오염 → 원답 전달 금지. 무한 에스컬레이션 → 최대 3단계.
- 서브에이전트가 보고한 파일·라인·집계 수치는 틀리거나 조작될 수 있다 — 결정론 도구(grep, git, psql)로 표본 재현 후 채택.
- 호출 로그 수집·집계 하네스는 [`agent-teams.md`](agent-teams.md) §5.

---

## 10. 체크리스트

시작 전: 작업이 §3 매트릭스의 어디인가 · 전달 컨텍스트가 §5 를 따르는가 · 출력 스키마(§6)를 명시했는가.
마무리 전: 테스트를 실제로 실행해 통과했는가(자체 보고 X) · 보안·성능 민감 영역에 §3.3 추가 검증을 했는가 · Opus 부재 모드였다면 decisions.log 를 남겼는가.
