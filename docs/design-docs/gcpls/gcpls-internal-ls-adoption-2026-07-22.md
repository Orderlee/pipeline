# GCP 외주 Label Studio 기능의 내부 파이프라인 LS 적용 — 종합 기록

작성: 2026-07-22 | 작업자: eng-d (+ Claude Code) | 상태: **완료·운영 중**

관련 문서: [기능 인벤토리·검증 기록](gcpls-adoption-inventory.md) · [presign 인시던트·수정안](presign-renew-bugfix.md) · [운영 런북](../../runbook/labelstudio-ops.md)

---

## 1. 목적

GCP VM(`gcp-data-cpu-1`)에서 외주 라벨러용으로 운영·개발해 온 커스텀 Label Studio(리뷰 워크플로우, RBAC, 프로젝트 submit/중간검수, 실작업시간 측정 등)를 **내부 파이프라인에 연동된 LS에 그대로 적용**하고, 내부 LS 자동화가 퇴사자(eng-c) 개인 계정 토큰에 묶여 있던 의존을 제거한다. 운영 무중단·데이터 완전 보존이 전제.

## 2. 무엇이 바뀌었나

| 항목 | 이전 | 이후 |
|---|---|---|
| LS 이미지 | 순정 `heartexlabs/label-studio:latest` (1.23.0) | `labelstudio-internal:1.23.0-c2` = GCP VM 실배포본 재태그 (동일 베이스 1.23.0/2a9bfbc) |
| 기능 | 순정 | RBAC(labeler/reviewer/admin), 2단계 태스크 리뷰(+사유코드·감사이력), 프로젝트 submit/중간검수/한단계 반려, task 배정, 실작업시간(active_seconds), DM 리뷰·배정 컬럼 |
| LS 자동화 계정 | eng-c 개인 JWT PAT (파이프라인+agent 공용) | 서비스 계정 `svc-ls@example.com`(role=admin, superuser)의 PAT |
| eng-c 계정 | 활성, org owner, 토큰 사용 중 | 토큰 전량 무효화(401 확인) + `is_active=False` (행 삭제 금지 — org FK) |
| DB 스키마 | 순정 | +17 마이그레이션 (전부 가산적 — 컬럼/테이블/인덱스 추가만) |
| 역할 데이터 | (없음) | 내부 인원(eng-d, eng-e)+svc = admin / **신규 멤버 기본 labeler ⚠️** |

**내부 사용자에게 제한 없음**: 외주용 제한(기본거부 미들웨어, import/export AdminOnly 등)은 전부 역할 기반이며 labeler/reviewer에게만 작동 — 내부는 전원 admin.

## 3. 배포 방식 (GitHub/CI 미경유 — 중요)

user는 공용 계정(동료 주 사용)이라 그 GitHub 포크 개입을 피하기 위해 **로컬 untracked 운영**:

- 이미지 핀: `docker/docker-compose.labelstudio.local.yaml` (untracked) — tracked compose 원본은 순정 그대로
- 재현 빌드: `docker/labelstudio/` (Dockerfile = 순정 digest 핀 + 배포본 overlay 76파일 + FE dist 스테이지, 빌드 결과 = 배포본과 코드 100% 동일 검증)
- CI의 `rsync --delete`/`git reset --hard`는 tracked만 초기화 → 이 구성은 배포에도 생존
- **⚠️ LS 재기동은 반드시 세 개 `-f`** (local override 포함) — 빼면 순정으로 롤백됨. 명령은 런북 참조
- 작업 이력은 test 워크트리 로컬 브랜치 `feature/labelstudio-internal-adoption`에 보존 — 정식 레포 반영은 추후 별도 PR

## 4. 적용 절차 (실행된 순서)

1. **기준 스냅샷**: VM 실배포 이미지 `docker save|load` 반입 → 생성시각(ns)·파일 해시로 동일성 검증 → 순정과 전체 트리 diff로 커스터마이징 전량 파악, 마이그레이션 17종 가산성 정독 검증
2. **리허설** (격리 스택 `-p ls-rehearsal`): 신규 DB 마이그레이션 + **prod 실데이터 덤프 복원 후 마이그레이션**(부팅 ~20초, 에러 0) + 3역할 권한 매트릭스 + owner 비활성화 무부작용 + 웹훅 등록→발화→수신 전 체인
3. **토큰 회전**: svc 계정 생성(Django shell) + PAT 발급(`POST /api/token` 공식 경로) → `docker/.env`·`agent/.env.agent` 교체 → 소비 컨테이너 recreate (**restart 무효** — create-시점 env)
4. **컷오버**: LS DB pg_dump 백업 → 이미지 스왑(다운타임 ~15초, 볼륨·DB 재부착) → **즉시 role 승격** (누락 시 내부 인원 import/export 차단)
5. **검증** 및 구 토큰 무효화·eng-c 비활성·전 컨테이너 토큰 스윕(잔존 0)
6. **-c2 업데이트** (같은 날 저녁): VM 새 빌드 반입 → -c1 대비 diff(반려 한단계 되돌림, 1파일) → 재태그 → 스왑

## 5. 검증 결과 (파이프라인 무영향 확인 — 2026-07-22 저녁 최종)

| 검증 항목 | 결과 |
|---|---|
| LS 데이터 보존 | 프로젝트 36 / 태스크 26,455 / 어노테이션 3,551 — 컷오버 전후 동일 |
| **파이프라인 메타 DB (vlm_pipeline)** | **무접촉 확인** — labels 12,608 불변, image_label_annotations 24h 신규 0건 |
| 파이프라인 연동 E2E | svc 토큰 실 HTTP로 프로젝트 생성·import → 웹훅 등록 → 어노테이션 → 수신·시크릿 검증 → sync 체인(쓰기 0) 통과 후 정리 |
| Dagster | 전체 run SUCCESS(오늘 포함), 데몬 에러 0, `ls_task_create_sensor` RUNNING, 센서 tick 정상 |
| agent | reconciler 사이클 30/30 성공, 에러 0 |
| 컨테이너 | 전체 Up/healthy, 구 토큰 잔존 0 (전 컨테이너 env 해시 스윕) |
| git 안전성 | 우리가 만든 tracked 변경 0 (prod·test 워크트리) — 다음 CI 배포에 영향 없음 |
| 로그 | LS/ls-webhook/agent 최근 에러 0 |
| 구 토큰 | refresh 401 확인 (무효), svc 토큰 200 |

참고(우리 작업 아님): prod 워크트리에 동료의 로컬 tracked 수정(.agent/skill/*, CLAUDE.md, README 등)이 커밋되지 않은 채 존재 — **다음 CI prod 배포의 hard-reset에서 소실될 파일들**이므로 커밋 여부를 동료와 확인 권장.

## 6. 같은 날 처리한 부수 인시던트 (컷오버와 무관한 기존 버그)

라벨링된 프로젝트 미디어 미로드 신고 → 원인은 `src/gemini` presign 갱신 잡의 기존 버그 3종(이름 조회 30개 제한→빈 중복 프로젝트 63개 자동 증식, 이미지 프로젝트 인덱스 붕괴, task data 전체 교체). 조치: 스케줄 정지, 중복 63개 삭제, **ID 기반 수동 갱신 25,482건(오류 0)**. 상세·수정 diff: [presign-renew-bugfix](presign-renew-bugfix.md). **코드 픽스가 정식 배포되기 전까지 `ls_presign_renew_schedule`은 STOPPED 유지 + 7일 주기 수동 갱신 필요** (도구: `docker/labelstudio/tools/renew_by_id.py`).

## 7. 운영 체크리스트 (인수인계 요점)

- [ ] LS 재기동은 런북의 세 개 `-f` 명령으로만
- [ ] 신규 내부 멤버 가입 즉시 role=admin 승격 (기본 labeler = 전면 403)
- [ ] presign 수동 갱신 (버그픽스 배포 전까지, 7일 주기)
- [ ] GCP LS 업데이트 반영: VM 이미지 반입 → diff 검증 → `-cN` 재태그 → override 갱신 → 백업+스왑 (이번에 확립된 절차)
- [ ] 롤백: override의 image를 `-c1`(직전) 또는 순정 digest(전체)로 — 마이그레이션 가산적이라 무손실
- [ ] 미결: 웹훅 체계 복구(운영 결정 대기), presign 버그픽스 정식 반영, LS 관련 작업물 정식 PR

## 8. 백업·복구 지점

`/home/user/env-backups/`: `ls-internal-db-20260722-1414.dump`(컷오버 직전) · `ls-internal-db-20260722-1749.dump`(-c2 직전) · `docker-env-bak-*`/`agent-env-bak-*`(구 토큰 포함 — 안정화 후 삭제 권장)
