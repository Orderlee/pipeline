# 파이프라인 현황 리포트 — 2026-07-01

> VLM 데이터 파이프라인(수집→중복제거→Gemini라벨→SAM3검출→LS검수→데이터셋빌드 + MLOps 파인튠 트랙)의 현재 상태·감사 결과·수정 현황·잔여 리스크 종합.
> 근거: 3개 감사([MLOps/DVC/pseudo](pipeline-flow-audit-2026-07-01.md), [코어 흐름](pipeline-flow-audit-2026-07-01-core.md), 멀티모델 whole-codebase §5) + 이번 세션 6건 수정 + git 상태.

---

## 1. Executive Summary

| 관점 | 상태 |
|------|------|
| **구조 건전성** | 🟢 골격 견고. 3개 감사 모두 "설계 골격은 대체로 옳다" 결론 — dedup 결정성·MinIO 키 도출·챔피언게이트·스냅샷 불변성·pin 원자성 검증됨. |
| **데이터 정합성** | 🟡 조용한 불변식 위반 2건(BUILD-1, SAM3-1) **수정 완료**. 잔여는 크래시-윈도우/재처리불가/관측성 성격(좁은 트리거). |
| **배포 상태** | 🔴 **로컬 main이 origin 대비 76 커밋 앞섬** (MLOps 스캐폴딩 전체 + 이번 6건, 전부 미push/미배포). push = 대규모 prod 재배포. |
| **핵심 병목** | 사람 GT. 자동 라벨은 풍부하나 학습용 검수(finalized) GT 축적이 MLOps/PE-Core 승격의 실제 gate. |

**한 줄**: 코어 파이프라인은 프로덕션 견고성이 높고, 이번 세션에 일상 운영에서 조용히 학습셋을 오염시키던 문제 2건(BUILD-1 폴더스코프 finalized 게이트, SAM3-1 검수결과 clobber)을 뿌리부터 막았다. 남은 위험은 대부분 크래시 복구/재처리 UX/관측성이며 데이터 손상은 좁은 트리거 뒤에 있다.

---

## 2. 이번 세션 수정 (6건, 로컬 main 커밋 2개, 미push)

### 커밋 `55e31c1` — BUILD-1 + 패턴 A (LABEL-1/2/3)

| ID | 문제 | 수정 |
|----|------|------|
| **BUILD-1** 🔴 | finalized 게이트가 폴더 스코프 → 폴더 내 1개만 검수돼도 미검수 형제 전체가 finalized 데이터셋에 유입(no-self-training 위반) + 영구 재빌드 제외 | `find_project_video_candidates` 게이트를 per-asset(`l.asset_id=r.asset_id`)로. image 쿼리·sensor readiness와 대칭 |
| **LABEL-1** 🟠 | routed가 `timestamp_status`만 세팅→MVP backstop이 dispatch-라벨 비디오를 Gemini 재호출·카테고리라벨 clobber | MVP 후보쿼리 + 센서 `gemini_pending` CTE에 `timestamp_status='pending'` 가드 |
| **LABEL-2** 🟠 | in-flight 가드가 `dispatch_stage_job` 누락 → 동일 폴더 동시 Gemini 처리 race | `AUTO_LABELING_TARGET_JOBS`에 `dispatch_stage_job` 추가 |
| **LABEL-3** 🟡 | routed Gemini 실패 = 영구 orphan(재시도 불가) | routed 후보쿼리 `IN ('pending','failed')` → 재-dispatch 시 재시도(operator-bound) |

### 커밋 `26d92b4` — INGEST-3 + SAM3-1

| ID | 문제 | 수정 |
|----|------|------|
| **INGEST-3** 🟡 | 파이프라인 예외 시 실패-마킹이 이미 completed(업로드+archive)된 행까지 `failed`로 되돌려 DB↔MinIO↔archive 어긋남 + 중복 재수집 | `batch_update_status(skip_completed=True)` — completed 행 제외 |
| **SAM3-1** 🟠(잠재) | 재-detection(shadow/primary)이 같은 PK로 auto 결과 upsert → 사람 검수 결과(review_status)·박스참조(labels_key) clobber | `image_labels` ON CONFLICT에 `WHERE review_status NOT IN ('reviewed','finalized')` 가드 |

**검증**: 신규 `tests/unit/test_pipeline_flow_audit_fixes.py` 9 tests(+2 dagster-skip). 각 수정마다 **red→green 확인**(옛 코드로 되돌리면 실패). ruff 0.7.4 clean. PG-only 경로만 수정(레거시 DuckDB 미변경). 테스트는 격리 ephemeral pgvector 컨테이너에서 실행(호스트 anaconda pydantic이 dagster import를 깨뜨려 로컬 dagster테스트는 CI에서 실행).

---

## 3. 코어 감사 잔여 발견 — 상태 (18확인/3유력/2반증 중)

| ID | 심각도 | 상태 | 사유 |
|----|--------|------|------|
| BUILD-1 | high | ✅ 수정 | — |
| LABEL-1/2/3 | high/med | ✅ 수정 | — |
| INGEST-3 | med | ✅ 수정 | — |
| SAM3-1 | high(잠재) | ✅ 수정(DB가드) | MinIO 키 분리·후보 제외는 shadow 전용(YOLO-off, 벤치 수동)이라 후속 |
| **DISPATCH-4** | med | ⏸ 보류 | 터미널 request_id 재시도 = upsert-on-retry + `dispatch_pipeline_runs` 라이프사이클 재설계 필요(1줄 아님) |
| **DISPATCH-3** | med | ⏸ 보류 | `from_archived` 추적 = archive_dispatch_sensor가 dispatch_requests 행 생성 + `_TARGET_JOBS`/stuck_guard에 `upload_label_job` 등록(행 라이프사이클 설계) |
| **INGEST-1** | med(유력) | ⏸ 보류 | orphan `uploading` dedup-poison = TTL 리퍼 센서 필요(ops 결정) |
| **DISPATCH-1** | med | ⏸ 보류 | 동시 최초-디스패치 TOCTOU = folder advisory lock 필요(infra 결정) |
| **BUILD-3** | med | ⏸ 보류 | `build_classification` 미배선 = finalize 센서 추가(자동화 여부 설계 결정) |
| DISPATCH-2/5 | low | ⏸ 보류 | 크래시-윈도우 = 리퍼 센서 계층(INGEST-1과 함께) |
| SAM3-3 | low | ⏸ 보류 | ls_task failed 터미널 = bounded 재픽업 |
| SAM3-4 | low | ⏸ 보류 | ls_sync DB-before-MinIO 순서 = 복구경로 있음, 저위험 |
| SAM3-2/BBOX-1 | low | 🚫 wontfix | `bbox_status`는 write-only(reader 0) = cosmetic, self-heal |
| BBOX-2 | low | 🚫 wontfix | raw_video_frame 바이패스 = 의도된 YOLO 전용 설계 |
| BUILD-4 | low | 🚫 wontfix | orphan `building` 행 = inert clutter(downstream 영향 반증됨) |
| INGEST-2 | low | ⏸ 보류 | stray 파일 archive = 미디어 무손실, JSONL 추적만 추가 |

**권장 다음 배치** (ROI순): ① 리퍼 센서 1개로 INGEST-1 + DISPATCH-2/5 + BUILD-4 흡수(stale `uploading`/`running`/`building` TTL 정리) → ② DISPATCH-3/4 재처리 경로(dispatch 상태머신 재설계) → ③ BUILD-3 classification 트리거.

---

## 4. 현재 파이프라인 운영 상태 (코드 외 맥락)

- **배포 drift**: 로컬 main = origin/main + **76 커밋**(MLOps 파인튠 스캐폴딩 전체 + DVC 버저닝 + PE-Core 승격 + 이번 6건). 전부 미push. → **push 시 대규모 prod 재배포**(dagster 무조건 재가동 = 라벨링 중단). 진행 중 192TB 배치 인제스트 고려해 정비 윈도우에 조율 필요.
- **MLOps 트랙**: "built-not-executed" 스캐폴딩(설계상 known). 학습루프/GPU 실가동 전. eval 스코어러·trainer↔registry 글루는 외부 학습루프 착수 시 배선(이전 감사 M-1/H-2/M-2).
- **핵심 병목**: 사람 GT(LS finalized). 자동 라벨 풍부하나 학습·PE-Core 승격은 검수 GT 축적이 실 gate.
- **환경**: prod(:3030)/staging(:3031) 독립 clone. 공유 SAM3 컨테이너, 공유 pipeline-network(postgres alias 충돌 주의). NAS_200tb(NFS) cutover 완료.

---

## 5. 멀티모델 whole-codebase 분석 (Codex + Opus + Sonnet 역할 분담)

> 방법: **Sonnet**(model=sonnet)이 이전 2개 감사가 다루지 않은 서브시스템(embed/gcp+genai/ls+gemini/DB resource layer/lib/cross-cutting) 6개를 병렬 추적 → **Codex**(agentType=codex)가 상위 발견을 적대적 검증 → **Opus**(model=opus)가 아키텍처 헬스로 종합.
> ⚠️ **워크플로 품질 주의**: sonnet 6개 중 3개(embed/lib/external)가 placeholder("test") 산출, 1개(ls_gemini)는 structured-output 재시도 한도 초과로 실패 — 해당 4개 영역은 **미커버**. db_resources·crosscut 2개만 실질 분석 → Codex 5건 확인. embed/ls/lib 영역은 후속 재실행 권장.

### 결론 (Opus 종합): "신뢰 신호 부식(confidence-signal decay)"
> 데이터 정합성 척추(spine)는 이전 2개 감사가 클린 판정. 신규 발견은 전부 **운영/인프라 봉투(envelope)** — 데이터를 오염시키진 않지만 팀이 의존하는 **신뢰 신호를 조용히 약화**시킴: 초록 CI가 "모든 테스트 통과"를 뜻하지 않고, "transient 재시도"가 가장 흔한 transient(pool exhausted)를 안 잡고, "재현 가능 빌드"가 캐시 살아있을 때만 재현됨.

| ID | 심각도 | 확인 | 발견 | 위치 |
|----|--------|------|------|------|
| **TEST-1** | 🔴 med(가장 systemic, **LIVE**) | Codex CONFIRMED | `.gitignore`가 `tests/unit/*` 블랭킷 무시 + per-file allowlist → **테스트 4개 현재 untracked**(`test_key_builders`, `test_label_qa_asset`, `test_label_qa_fiftyone`, `test_pseudo_label_qa`) — allow-line은 WIP `.gitignore`에 있으나 파일 미`git add` → **CI가 조용히 이들을 안 돌림**(actions/checkout=tracked만). "127/82 pass"가 실제 커버리지 과대. | `.gitignore:34-38` vs `:208-217` |
| **DB-1** | 🟠 med | Codex CONFIRMED | `_is_transient_pg_error`가 `psycopg2.pool.PoolError`(pool exhausted) 미분류 → 첫 시도에서 backoff 없이 re-raise(retry docstring 위반). `conn is None` 가드는 dead code. `pool_max=10`/`max_concurrent_runs=20`이라 도달 가능. | `postgres_base.py:29-37, 176-178` |
| **DEP-1** | 🟠 med | Codex CONFIRMED | `git+https://.../sam3.git` unpinned(sam3+trainer requirements), CI 미빌드, lockfile 없음 → 캐시 축출/재빌드 시 prod segmentation 동작 drift + trainer↔serving parity 가정 붕괴 가능(episodic). | `docker/sam3/requirements.txt:14`, `docker/trainer/requirements.txt:4` |
| **TEST-2** | 🟡 low | Codex CONFIRMED | `nas_health_sensor`가 실패-streak/쿨다운을 **모듈 전역**에 저장(cursor 아님) → daemon 재시작(=매 prod 배포)마다 리셋 → NAS 경보 지연. 테스트 0. | `sensor_nas_health.py:22-24` |
| **DB-2** | 🟡 low | Codex CONFIRMED | `connect()` retry/backoff·rollback-on-exception 유닛 커버리지 0(fake-pool 생성자만). | `postgres_base.py:170-207` |

**검증 통과(healthy)**: fail-forward `except`는 거의 전부 `noqa: BLE001`+사유 주석(정책 준수, tech debt 아님) · DB 풀 공유/`putconn` in finally/commit-rollback 계약 정상 · 마이그레이션 runner(AUTOCOMMIT single-query, DO $$ OK) 정상 · **SQL injection 없음**(~136 connect 사이트, f-string 동적조각은 하드코딩 식별자/`%s` IN-list만) · `.env` 미추적 · 이번 세션 4개 수정(BUILD-1/LABEL/INGEST-3/SAM3-1) 모두 유지·회귀 없음.

> **TEST-1은 지금 발생 중** — 다른 세션(pseudo-label QA/MLOps)의 미커밋 테스트 4개가 tracked 아님. 그 세션 작성자가 `git add` 해야 함(내 것 아님 → 함부로 커밋 안 함). 가장 먼저 처리 권장.

---

## 6. 종합 권고 (우선순위순)

1. **오늘 (LIVE)**: **TEST-1** — untracked 테스트 4개를 커밋(해당 WIP 작성자가 `git add` + `.gitignore` allow-line 동반). 또는 정책 반전(`tests/unit/*.py` default-allow) + CI 가드(`git ls-files tests/unit` == `find tests/unit -name test_*.py`). 초록 CI 신뢰 회복의 근본.
2. **데이터 정합성**: 없음 — 일상 조용한 오류(BUILD-1/SAM3-1/INGEST-3)는 이번 세션에 해소.
3. **push 조율**: 76 커밋 배포는 정비 윈도우 + 배치 인제스트 상태 확인 후(dagster 재가동=라벨링 중단). dev 경유 검증 권장(호스트는 main).
4. **운영 봉투 하드닝**(신규): DB-1(PoolError를 transient 분류에 추가 + dead branch 제거, 소규모) · DEP-1(sam3 `@<sha>` 핀 + `env_lock.json` 기록) · TEST-2(nas_health 상태를 cursor로 이전 + 테스트).
5. **다음 코어 배치**: 리퍼 센서 1개로 INGEST-1+DISPATCH-2/5+BUILD-4 흡수(§3 ①) → DISPATCH-3/4 재처리(§3 ②) → BUILD-3.
6. **MLOps**: 외부 학습루프/GPU 착수 시 glue 배선(이전 감사 체크리스트 M-1/H-2/M-2).
7. **병목 해소**: 사람 GT 축적 파이프라인(LS 검수 처리량) 투자가 학습/승격 실효성의 실제 lever.
8. **후속 분석**: 멀티모델 워크플로에서 embed/ls_gemini/lib 3개 영역 placeholder/실패로 미커버 → 재실행 필요.
