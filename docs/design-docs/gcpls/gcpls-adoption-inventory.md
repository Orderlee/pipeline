# gcpls 커스텀 Label Studio 내부 채택 — 기능 인벤토리 & 검증 기록

최초 검증 2026-07-21 (7/13 로컬 빌드 기준), 재검증 **2026-07-22 (GCP VM 실배포본 기준)**.
검증 방법: 배포 이미지 vs 순정 이미지의 **패키지 전체 트리** 파일 diff, 마이그레이션 전량 정독, 라이브 API/DB 조회.

## 스냅샷 핀 (컷오버 기준)

| 항목 | 값 |
|---|---|
| 베이스 | Label Studio **v1.23.0**, rev `2a9bfbc`, digest `sha256:aa461572e8f9d86a1bf9520c1db620204e86160fd2f80dd7e9d40ac84a8828ea` |
| 기준 스냅샷 | **GCP VM(gcp-data-cpu-1) 실배포 이미지** — VM image `0df4ee72d4c4`, 빌드 2026-07-21T07:02Z, 소스 커밋 `0656182` |
| 로컬 반입본 | `gcpls-deployed:0656182-20260721` — VM 배포본과 생성시각(ns)·핵심 파일 해시 일치 검증 |
| 내부 채택 태그 | **`labelstudio-internal:1.23.0-c1`** = 위 반입본 재태그 (컷오버는 검증된 이 바이트로) |
| 외주 소스 | 워크스테이션 eng-a 계정 로컬 git 레포 (VM에는 deploy 설정만 존재). FE 빌드 스테이지: VM `gcpls-fe:build` |

`docker/labelstudio/overlay/`의 75개 파일은 위 배포 이미지 추출본과 바이트 동일 검증 완료(cmp 전수). 빌드 산출물(`core/static_build/staticfiles.json`)은 오버레이에서 제외 — Dockerfile의 collectstatic이 재생성.

## 기능 인벤토리 (순정 대비 변경 전량 — 변경 24 + 신규 49 파일, 삭제 0)

| 기능 | 구현 | 내부 영향 |
|---|---|---|
| RBAC | `OrganizationMember.role` (labeler/reviewer/admin, **신규 멤버 기본 labeler**), `is_admin`=superuser∨org owner∨role admin | 내부 전원 role=admin 운영 (런북) |
| labeler 기본 거부 | `LabelerDefaultDenyMiddleware` — URL name 화이트리스트, **labeler에만 작동** | admin/reviewer 무접촉 |
| API 권한 | `DenyLabelers`(리뷰·submit API), `AdminOnly`(**data_import/data_export 전부** — reviewer도 차단) | 자동화 계정 admin 필수 |
| 태스크 배정 | `task.assignee` + labeler는 배정 태스크만 조회, DM assignee 컬럼(7/21 추가) | 미배정 시 순정 동일 |
| 2단계 태스크 리뷰 | `task.review_status`(pending/first_approved/approved/rejected), `TaskReviewHistory`(+reason_code), DM 리뷰 액션/컬럼 | 옵트인 기능 |
| 리뷰-재라벨 루프 | **전역 변경**: rejected면 `is_labeled=False`(재오픈), 재라벨 시 pending 복귀+이력 | 리뷰 미사용 시 순정 동일 |
| 프로젝트 submit/reject | `project.is_submitted/submitted_at` + `LabelStateHistory` (F1 버튼) | 옵트인 |
| 프로젝트 중간검수 (7/21 추가) | `project.is_mid_reviewed/mid_reviewed_at` + MID_SUBMITTED 이력 — 프로젝트 2단계 검수 | 옵트인 |
| 실작업시간 | `annotation.active_seconds` (FE 측정, 60s+ 무활동 제외), DM 평균 컬럼 | FE 포함이라 동작 |
| 기타 | whoami에 `role`, org dashboard API(admin 게이트), submit-state 읽기 API(labeler 허용·스코프 적용) | 가산적 |

## DB 마이그레이션 17개 — 전부 가산적 (롤백 안전)

- organizations `0007`(role 추가 — 기존 행 'reviewer' 백필) / `0008`(기본값 labeler — 앱 레벨)
- projects `0035`(is_submitted 등 + LabelStateHistory) / `0036`(note, choices) / **`0037`(is_mid_reviewed/mid_reviewed_at + choices — 7/21 추가, 가산적 확인)**
- tasks `0061`~`0068`(assignee FK, review_* 컬럼, TaskReviewHistory, approved 백필(RunPython/reverse_noop), choices, 인덱스, reason_code, active_seconds)

null/default 컬럼·새 테이블·인덱스·choices뿐 → **순정 롤백 시 잔여 스키마 무해**. LS는 부팅 시 자동 migrate(`server.py`) — 커스텀 이미지 첫 기동 = 마이그레이션 시점.

## 라이브 실측

- (07-21, prod) 프로젝트 86, 태스크 26,455, 어노테이션 3,551, LS 내부 DB(`pipeline-postgres-1`/airflow) 115MB → 마이그레이션·pg_dump 수 초~분.
- `LS_API_KEY`(eng-c 소유)는 **JWT PAT(refresh) 형식** — `resolve_auth_headers`가 legacy/JWT 모두 지원.
- **웹훅 전면 고장 상태**: 유효(헤더 포함) 웹훅 0개, agent reconciler의 headerless만 존재(수신부 403). 원인: `WEBHOOK_HOST=ls-webhook`(단일 세그먼트)을 LS URL 검증기가 거부. 운영은 Slack /sync-approve 수동 경로. → 런북 "웹훅 복구" (별도 결정).
- (07-22, GCP VM) 배포 스택: nginx + LS(custom-full) + postgres:15-alpine, deploy 설정은 `~/gcp-outsourced-ls` (git 아님 — 소스는 eng-a 로컬 레포).
- LS DB collation version 경고(2.41 vs 2.36) 존재 — 기존 상태, 마이그레이션 차단 아님.

## 스테이징 리허설 결과 (2026-07-22, `-p ls-rehearsal` 격리 스택)

1. **재현 빌드**: `docker/labelstudio/` 컨텍스트 빌드 == 배포 이미지 — 코드 트리 100% 동일, 유일 차이는 collectstatic 매니페스트 재생성분(334개 매핑 전부 동일).
2. **신규 DB 부팅**: 마이그레이션 17종 전부 적용, 신규 컬럼/테이블 확인.
3. **권한 스모크 (3역할 매트릭스)**: admin(생성 201/import 201/export 200), reviewer(생성 201, import·export 403=AdminOnly 실증), labeler(조회 200·스코프 격리, 생성·import·export 403), labeler→admin 승격 후 차단 해제, **owner is_active=False 시 타 사용자 무영향** 실증.
4. **prod 실데이터 복원 리허설**: 26,455 태스크 DB 위 커스텀 이미지 부팅 ~20초(≈컷오버 다운타임), 마이그레이션 에러 0, 기존 멤버 3명 role='reviewer' 백필·0064가 어노테이션 완료 3,551건을 'approved' 백필 — 사전 예측과 정확히 일치. 실계정 승격 리허설 통과.
5. **파이프라인 연동 리허설**: 공식 PAT 발급(POST /api/token) → `ls_webhook.py`의 JWT 교환(resolve_auth_headers) 정상 → 헤더 포함 웹훅 등록(IP URL, LS 검증기 통과) → 어노테이션 생성 → 수신부 시크릿 검증 → `[RECV] ANNOTATION_CREATED` → 후속 sync 트리거까지 전 체인 통과.
6. 발견: 스테이징 전용 compose 포트 매핑 quirk(런북 기재), 수동 `LSAPIToken.for_user()` 토큰은 교환 실패(공식 API 경로 필수).

## 리스크 레지스터

승인 플랜과 동일 — 핵심: R1/R2(role 승격 = 컷오버와 한 몸), R3/R4(스테이징 기본값이 prod DB·Dagster — 전용 env 필수, `-p pipeline` 금지), R6(토큰 교체는 recreate, restart 무효), R7(컷오버 직전 pg_dump), R16(LS_WEBHOOK_SECRET 불변).

## -c2 업데이트 (2026-07-22 저녁)

- 기준: GCP VM 2026-07-22T07:54Z 빌드(image `f7aeeb6c405b`) — 사용자 요청으로 반입.
- 델타(-c1 대비): **`projects/api.py` 1개 파일** — ProjectRejectAPI를 "한 단계 되돌림"으로 개선(final→mid→open, 트랜잭션 원자화, 태스크 review_status 보존). FE dist 동일(번들 해시 일치), **마이그레이션 0**, 권한 변화 0.
- 적용: `labelstudio-internal:1.23.0-c2` 재태그 → local override 갱신 → 스왑(다운타임 ~15초) → 스모크 통과(whoami/projects/데이터 보존 36·26,455·3,551). 롤백 = override를 `-c1`로.

## -c3 업데이트 (2026-07-24)

- 기준: GCP VM 2026-07-24T00:33Z 빌드(image `29209584ee31`).
- 델타(-c2 대비): **FE 번들 1개**(`apps/labelstudio/762.js`) — 비디오 구간 단축키 변경 (구간 생성 m→**a**, 구간 시작 수정 **s**, 끝 수정 **f**). 백엔드 트리 완전 동일, 마이그레이션 0. ※ 이번부터 FE 비교는 파일명 목록이 아닌 **내용 diff**로 검증 (번들 파일명이 내용 해시가 아님을 확인).
- 적용: `labelstudio-internal:1.23.0-c3` → override 갱신 → 스왑(~15초) → 데이터 보존(36 프로젝트/26,681 태스크/3,738 어노 — vest 226건 등 정상 증가분 포함)·whoami 200. 롤백 = override를 `-c2`로.

## -c4 업데이트 (2026-07-24)

- 기준: GCP VM 2026-07-24T01:30Z 빌드(image `31f14c23ed6d`). 델타(-c3 대비): FE 번들 1개(762.js) — **단축키 n 키 추가**. 백엔드 동일·마이그레이션 0.
- 적용: `-c4` 스왑(~15초), 데이터 보존(36/26,681/3,740)·whoami 200. 롤백 = override를 `-c3`으로.
