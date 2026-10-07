# Agent Teams — VLM Data Pipeline

멀티 에이전트의 모델 tier 일반 원칙은 [`multi-agent.md`](multi-agent.md), 이 문서는 **이 프로젝트의 페르소나 명단·라우팅표·시나리오**다. 페르소나 정의 정본은 [`.claude/agents/*.md`](../../.claude/agents/) (frontmatter `model:`/`effort:`/`tools:`) 이고, 아래 표의 Model/Effort/Mode 는 그것에서 뽑았다. 모델 버전 숫자는 박지 않는다(tier 별칭만).

## 1. 로스터 (24종)

페르소나 파일의 frontmatter 는 `description`(Agent 도구 목록으로 **매 세션 주입**되므로 1–2문장 유지)과 `triggers:`(라우터 훅만 읽는 쉼표 구분 키워드)를 분리한다. 새 페르소나를 추가할 때 `triggers:` 가 없으면 [`persona_router.py`](../../.claude/hooks/persona_router.py) 의 자동 제안 대상이 아니다(수동 위임만). ¹ = 현재 `triggers:` 없음.

| Plane | Persona | Model | Effort | Mode | Role |
|---|---|---|---|---|---|
| Decision / design | [`cto`](../../.claude/agents/cto.md) | opus | high | r/w | decompose/route, architecture & deploy-risk decisions, final review |
| Decision / design | [`ai-modeler`](../../.claude/agents/ai-modeler.md) | opus | high | r/w | experiment & eval-gate design, promote/reject decisions, GT policy |
| Decision / design | [`qa-strategist`](../../.claude/agents/qa-strategist.md) | opus | high | read-only | test-gap analysis and plans; delegates authoring/review |
| Decision / design | [`platform-architect`](../../.claude/agents/platform-architect.md) | opus | high | ADR-only | new-service / boundary integration design (ADR writes only) |
| Decision / design | [`db-architect`](../../.claude/agents/db-architect.md) | opus | high | read-only | Postgres schema/index/migration-file design review |
| Decision / design | [`security-architect`](../../.claude/agents/security-architect.md) | opus | high | read-only | threat model, secrets, PII/CCTV egress review |
| Decision / design | [`methodology-auditor`](../../.claude/agents/methodology-auditor.md) | opus | high | read-only | independent scientific-validity audit, verdict only |
| Data plane | [`data-engineer`](../../.claude/agents/data-engineer.md) | sonnet | medium | r/w | ingest→process pipeline, Postgres/MinIO/NAS plumbing, migration files |
| Data plane | [`dataops-engineer`](../../.claude/agents/dataops-engineer.md) | sonnet | medium | r/w | reconciliation, backfill, checksum/dedup ops, migrations-as-ops, quota |
| Data plane | [`ai-data-engineer`](../../.claude/agents/ai-data-engineer.md) | sonnet | medium | r/w | Gemini/LS labeling, GT projection, dataset build, pseudo-label QA, DVC |
| Model plane | [`ai-engineer`](../../.claude/agents/ai-engineer.md) | sonnet | medium | r/w | serving/inference containers, GPU topology, drain endpoints |
| Model plane | [`mlops-engineer`](../../.claude/agents/mlops-engineer.md) | sonnet | medium | r/w | trainer lifecycle, maintenance windows, registry, promote_*.py, MLflow |
| Implementation / audit / scouting | [`dagster-impl`](../../.claude/agents/dagster-impl.md) ¹ | sonnet | medium | r/w | default Dagster implementer when no domain persona fits |
| Implementation / audit / scouting | [`pipeline-explorer`](../../.claude/agents/pipeline-explorer.md) ¹ | sonnet | low | read-only | code navigation: where is X / trace this flow |
| Implementation / audit / scouting | [`deploy-auditor`](../../.claude/agents/deploy-auditor.md) | sonnet | medium | read-only | deploy blast radius + prod/staging drift, never deploys |
| Implementation / audit / scouting | [`tech-scout`](../../.claude/agents/tech-scout.md) | sonnet | medium | read-only | verify new tech against current docs; cto decides |
| Implementation / audit / scouting | [`perf-engineer`](../../.claude/agents/perf-engineer.md) | sonnet | medium | r/w | RAM/IO/VRAM contention diagnosis, measured tuning |
| Implementation / audit / scouting | [`viz-engineer`](../../.claude/agents/viz-engineer.md) | sonnet | medium | r/w | docker/analysis: FiftyOne + plugins, Streamlit, JupyterLab |
| Research lane | [`research-scout`](../../.claude/agents/research-scout.md) | haiku | — | read-only | bulk paper/evidence extraction, no conclusions |
| Research lane | [`research-scientist`](../../.claude/agents/research-scientist.md) | sonnet | medium | read-only | paper-to-spec, experiment design, result interpretation |
| Research lane | [`principal-research-architect`](../../.claude/agents/principal-research-architect.md) | fable | xhigh | read-only | research reframing, falsification plans (rare) |
| Observation | [`ops-engineer`](../../.claude/agents/ops-engineer.md) | haiku | — | read-only | runtime health, log/DB triage; reports and routes, never fixes |
| Cross-validation (Codex) | [`codex`](../../.claude/agents/codex.md) ¹ | sonnet | medium | read-only | cross-model reviewer via Codex MCP; never edits |
| Cross-validation (Codex) | [`codex-router`](../../.claude/agents/codex-router.md) | sonnet | medium | read-only | routes engineering contracts to a Codex tier (luna/terra/sol/astra) |

main session 은 직접 오케스트레이터로도 동작한다. [`.agent/skill/`](../../.agent/skill/) 는 페르소나를 조합하는 워크플로 매크로 — `codex_collab`, `codex_arbitration`, `codex_refactor`, `codex_db_migration`, `mlops-finetune`, `daily_worklog`, `staging_reset`(PG staging 초기화 절차 — 동봉 `scripts/reset_staging.sh` 는 DuckDB 시절 것이라 실행 금지). 스킬 목록 정본은 [`.agent/skill/INDEX.md`](../../.agent/skill/INDEX.md).

### 1.1 모델 배분

Opus = **what & why**(분해·설계·모델 과학·QA·보안 판단·중재, ~10–20%) · Sonnet = **how**(구현·데이터 플러밍·운영 자동화·탐색·감사, ~55–65%) · Haiku = **is-it-alive**(ops-engineer, 대량 수집 research-scout) · Fable = 연구 프레임 재설정(매우 드묾) · Codex = **is-it-correct**(교차 검증, ~15–20%). 상태 확인에 Opus/Sonnet 예산을 쓰지 않는다.

## 2. 작업 유형 → 페르소나 라우팅 (훅이 이 표를 가리킨다)

| 작업 | 1차 | 2차 / 검증 |
|---|---|---|
| 아키텍처·새 resource/bucket·cross-cutting | **cto** | (선택) codex 분석 |
| 새 기술·라이브러리·버전 업그레이드·"X 써도 돼?" | **tech-scout** (현재 문서로 검증, 기억 금지) | platform-architect(도입 시 통합 설계) → cto(adopt/reject) |
| 새 서비스·스택 통합·서비스 간 경계 | **platform-architect** | tech-scout(사실), cto(판정), 도메인 페르소나(구현) |
| ingest/dedup/dispatch/sensor/raw_files/manifest/ffprobe/phash | **data-engineer** | codex(50줄+) |
| 스키마·인덱스·쿼리 플랜 설계 리뷰 / 마이그레이션 파일 작성 | **db-architect** / data-engineer + `codex_db_migration` | codex `ultra`. hot table 마이그레이션은 배포 타이밍 = cto |
| 정합성·백필·dedup 정리·checksum·retention·NAS quota | **dataops-engineer** | codex(파괴적이면) |
| 라벨링·LS·GT·데이터셋 빌드·pseudo-label QA·DVC | **ai-data-engineer** | ai-modeler(학습에 쓰이면). `labels` 0행 ≠ 실패 |
| 서빙·추론·SAM3·embedding·GPU 할당·정비 drain | **ai-engineer** | codex `high` |
| 파인튜닝·eval-gate·승격 판단·실험 설계·GT 정책 | **ai-modeler** | codex(eval 로직) |
| trainer·정비 윈도우·model_registry·승격/롤백 자동화·MLflow·DVC infra | **mlops-engineer** | codex(승격 경로) |
| 런타임 헬스·로그/DB 트리아지·liveness·객체 존재 | **ops-engineer** | 이상 시 담당 페르소나로 |
| OOM·IO/VRAM 포화·용량 | **perf-engineer** | cto(구조적 해법), 해당 도메인(앱 비효율) |
| FiftyOne·Streamlit·analysis 스택·`user-*` 플러그인 | **viz-engineer** | db-architect(무거운 쿼리), perf-engineer(호스트 지연) |
| 보안·인증·시크릿·PII 유출 | **security-architect** | 도메인 수정 → codex `ultra` + cto |
| 테스트 전략·갭·계획 / 작성 | **qa-strategist** / 도메인 Sonnet·dagster-impl | codex `medium` (계획·리뷰) |
| 새 asset/sensor/op(도메인 불명) | **dagster-impl** | codex(50줄+) + qa-strategist(계획) |
| 단일 파일 버그(사소 → main 직접 / 비사소 → 도메인·`codex_collab`) · 모듈 분할(`codex_refactor`, 로직 불변) | 도메인 페르소나 | codex 리뷰 |
| 어려운 알고리즘·동시성 | `codex_arbitration`(Sonnet+Codex 듀얼 → cto) | multi-agent.md §4.2 |
| "어디 있나"·코드 흐름 | **pipeline-explorer** | — |
| 배포 전 영향 분석 / prod-staging drift | **deploy-auditor** (+ ops-engineer) | 🔴 이면 codex |
| staging 초기화 | 스킬 `staging_reset/SKILL.md` (PG 절차. 동봉 `reset_staging.sh` 실행 금지) | 사용자 승인 필수 |
| 리니지 검증·복구 | `dagster_lineage_fixer` | pipeline-explorer 범위 → 수정은 data-engineer/dagster-impl |
| MLOps 파인튜닝 end-to-end | `mlops-finetune` 스킬 | ai-modeler(결정) + mlops-engineer(운영). 승격은 수동 |
| 논문 탐색 → 해석 → 재설정 | research-scout → research-scientist → principal-research-architect | methodology-auditor(독립 감사). 구현은 codex-router |
| 방법론·재현·벤치마크 주장 검증 | **methodology-auditor** | — |
| 구현·진단 계약을 Codex 로 | **codex-router** | 실패 분류 후 tier 승격 |
| LS / Slack / 외부 API 연동 | ai-data-engineer(LS) 또는 data-engineer | codex `high` |

## 3. 시나리오별 호출 순서

- **3.1 기능 개발**: cto 분해 → pipeline-explorer(영향 파일) → 도메인 페르소나(구현+테스트) → (선택) codex 리뷰 → deploy-auditor(dev 머지 전) → PR → dev 배포 → :3031 검증 → dev→main.
- **3.2 어려운 버그(패턴 B)**: pipeline-explorer 압축 → `codex_arbitration`(도메인 페르소나·codex 독립 풀이) → cto 비교·선택 → 적용 + 회귀 테스트.
- **3.3 운영 장애**: ops-engineer 트리아지(보고만) → 분기: 데이터 불일치 → dataops-engineer, 코드 버그 → 도메인 페르소나, 리니지 → `dagster_lineage_fixer`, 호스트 부하 → perf-engineer → ops-engineer 재검증.
- **3.4 인프라 마이그레이션(경로·마운트)**: cto 계획(롤백 포함) → codex `ultra` → data-engineer 적용 → dataops-engineer 정합성 → deploy-auditor → staging 운영 후 prod.
- **3.5 QA 보강**: qa-strategist(P0/P1/P2 계획) → 사용자 승인 → 도메인 페르소나가 P0 작성 → codex `medium` 리뷰 → pytest 실제 실행 → staging 검증. 새 테스트 파일은 `.gitignore` allowlist 전엔 CI 에서 안 돈다.
- **3.6 MLOps 파인튜닝(모델 평면)**: cto(학습 윈도우 확인, prod 배포 보류) → ai-data-engineer(동결 스냅샷) → ai-modeler(설계·게이트·기준) → mlops-engineer(정비 drain → 독립 trainer 프로세스) → ai-modeler(게이트 판독, 승격/기각) → mlops-engineer(`promote_model.py`) → ai-engineer(서빙 경로+checksum 확인) → ops-engineer(health·warmup). 결정자가 자기 결과를 서빙하지 않는다(패턴 C).

## 4. 호출 규칙

- **프롬프트에 항상**: 목표 한 문장, 대상 파일 절대경로(구현·탐색 계열), 제약(호환·통과 기준·보존 동작·prod vs staging), 출력 형식(구현 계열=§6.1 JSON, 읽기 전용=각자의 리포트 형태).
- **절대 넘기지 말 것**: 다른 페르소나의 원답(오케스트레이터 요약만), `.env`/`credentials/`/토큰, 무관한 이력.
- **병렬**: 독립 정보 수집(explorer + auditor + ops). **순차**: 한 출력이 다음 입력일 때. 애매하면 순차.
- **에스컬레이션**: 구현자 `partial` → codex 검증; codex·구현자 불일치 → `codex_arbitration`/cto; 둘 다 실패 → cto; 도메인 경계·아키텍처 필요 → 조용히 넘지 말고 cto (multi-agent.md §7).
- Opus 부재 폴백은 [`multi-agent.md`](multi-agent.md) §8 — 모델 승격 판단은 어떤 폴백에서도 자동화하지 않는다.

## 5. 호출 로그 하네스

| 구성요소 | 경로 |
|---|---|
| PreToolUse 훅(Agent 호출마다) → JSONL(gitignore) | [`.claude/settings.json`](../../.claude/settings.json) → [`scripts/agent_log.py`](../../scripts/agent_log.py) → `.claude/agent-log.jsonl` |
| 집계 / 결정 로그(커밋됨) | `python3 scripts/agent_stats.py --since <date> [--json]` / [`.agent/decisions.log`](../../.agent/decisions.log) |
| 라우터 훅(UserPromptSubmit) | [`persona_router.py`](../../.claude/hooks/persona_router.py) — `triggers:` 합계 2점 이상이면 상위 3개 제안 |

분기 점검: Codex 호출 ~10% 미만이면 검증 부족, Opus ~25% 초과면 과분해. 페르소나별 롱테일은 정상 — 평면별 트래픽만 본다. 헬스 질문이 Sonnet/Opus 로 새면 라우팅 누수, `status: failed` ~10% 초과면 해당 페르소나 description/triggers 를 조인다.

## 6. 체크리스트

시작 전: 1차 담당이 §2 에서 분명한가(불분명하면 pipeline-explorer/ops-engineer 로 범위 확인) · §4 컨텍스트(prod vs staging 포함)를 담았는가.
마무리 전: 테스트를 실제로 돌렸는가(자체 보고 X) · 보안·마이그레이션·외부 API 는 `ultra` 를 거쳤는가 · 배포에 닿으면 deploy-auditor · 비사소 수정은 qa-strategist · 모델 승격은 ai-modeler 가 게이트를 판단했는가.
