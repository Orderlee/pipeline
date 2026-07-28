# Label Studio 운영 런북 (labelstudio-internal)

LS 스택은 CI 관리 밖(수동 배포). compose 프로젝트 `pipeline`, 이미지 `labelstudio-internal:1.23.0-c1`
(= GCP 외주 LS 실배포본, 소스 커밋 0656182). 배경·검증 기록: [gcpls-adoption-inventory](../design-docs/gcpls/gcpls-adoption-inventory.md).

> **⚠️ 배포 방식 (2026-07-22)**: 이 변경은 GitHub/CI를 경유하지 않는 **로컬 untracked 운영**이다.
> 커스텀 이미지 핀은 `docker/docker-compose.labelstudio.local.yaml`(untracked)에만 있고 tracked compose
> 원본은 순정 그대로. CI 배포(rsync/git reset)는 tracked만 초기화하므로 이 구성은 살아남지만,
> **local override 없이 두 개 `-f`로만 `up` 하면 순정 이미지로 롤백**되니 LS 재기동은 반드시 이 런북의
> 명령(세 개 `-f`)으로. 이 런북·`docker/labelstudio/`·인벤토리 문서 자체도 untracked — 레포 정식 반영은
> 추후 별도 PR로.

## 이미지 빌드 (-c2 이후)

```bash
cd docker && docker compose -p pipeline --env-file .env \
  -f docker-compose.yaml -f docker-compose.labelstudio.yaml -f docker-compose.labelstudio.local.yaml build labelstudio
# 새 리비전: docker tag <built> labelstudio-internal:1.23.0-cN + compose image 라인 수정(git)
```

전제: 빌드 스테이지 이미지 `gcpls-deployed:0656182-20260721`이 로컬에 있어야 함 (FE dist 원천).

## 컷오버 절차 (prod)

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
# 1) LS 내부 DB 백업 (필수 — 부팅 시 자동 migrate 실행됨)
docker exec pipeline-postgres-1 pg_dump -U airflow -d airflow -Fc \
  > /home/user/env-backups/ls-internal-db-$(date +%Y%m%d-%H%M).dump
# 2) 이미지 스왑 (수십 초 다운타임 — 어노테이션 불가 시간대에)
docker compose -p pipeline --env-file .env \
  -f docker-compose.yaml -f docker-compose.labelstudio.yaml -f docker-compose.labelstudio.local.yaml up -d --no-deps labelstudio
# 3) [컷오버와 한 몸] 내부 전원 + 서비스 계정 role 승격 — 누락 시 import/export 차단됨
docker exec pipeline-labelstudio-1 python3 /label-studio/label_studio/manage.py shell -c "
from organizations.models import OrganizationMember
from users.models import User
ADMINS = ['eng-d@example.com', 'eng-e@example.com', 'svc-ls@example.com']
n = OrganizationMember.objects.filter(user__email__in=ADMINS).update(role='admin')
User.objects.filter(email='svc-ls@example.com').update(is_superuser=True)
print('promoted:', n)"
```

검증: UI 로그인/버전 → admin으로 import·export·프로젝트 생성 → 리뷰 UI 표시 → E2E 1건(task→어노테이션→finalize→labels 반영→post_review_clip_job) → 센서 green → 익일 presign 스케줄.

## 롤백

`docker-compose.labelstudio.local.yaml`의 image 라인을 `heartexlabs/label-studio@sha256:aa461572e8f9d86a1bf9520c1db620204e86160fd2f80dd7e9d40ac84a8828ea`로 바꾸고 (또는 세 번째 `-f`를 빼고) 같은 `up -d --no-deps labelstudio` 1회. 마이그레이션이 전부 가산적이라 데이터 무손실(잔여 컬럼/테이블은 순정이 무시). DB 복원은 최후 수단.

## 역할(role) 운영 — 함정 주의

- **신규 조직 멤버의 기본 role은 `labeler`** = 기본거부 미들웨어로 거의 전면 403. 내부 인원 가입 시 즉시 승격:
  `OrganizationMember.objects.filter(user__email='<email>').update(role='admin')` (또는 reviewer).
- import/export는 `AdminOnly` — **reviewer도 차단**. 자동화 계정은 반드시 admin(+superuser 권장).
- 외주 계정만 labeler로 두고, labeler에게는 `task.assignee` 배정 태스크만 보인다.

## 서비스 계정 · 토큰 발급 (리허설 검증된 절차)

```bash
# svc 계정 생성 + org 가입 (LS 컨테이너에서)
docker exec pipeline-labelstudio-1 python3 /label-studio/label_studio/manage.py shell -c "
from users.models import User
from organizations.models import Organization, OrganizationMember
org = Organization.objects.first()
u = User.objects.create_user(email='svc-ls@example.com', password='<강력한 비밀번호>')
u.active_organization = org; u.save()
OrganizationMember.objects.get_or_create(user=u, organization=org)"
# PAT(JWT) 발급 — UI와 동일한 공식 API 경로 (한 계정당 활성 토큰 1개, 중복 시 409)
docker exec pipeline-labelstudio-1 python3 /label-studio/label_studio/manage.py shell -c "
from users.models import User
from rest_framework.test import APIClient
cl = APIClient(); cl.force_authenticate(user=User.objects.get(email='svc-ls@example.com'))
print(cl.post('/api/token/').json()['token'])"
```

주의: 손으로 `LSAPIToken.for_user()` 발급하면 refresh 교환이 실패함(실측) — 반드시 위 API 경로로.

## LS API 토큰 회전

토큰은 3곳에서 소비되며 **모두 컨테이너 생성 시점 env — `docker restart` 무효, recreate 필수**:

```bash
# docker/.env 와 /home/user/work_p/agent/.env.agent 의 LS_API_KEY 교체 후:
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
docker compose -p pipeline --env-file .env -f docker-compose.yaml -f docker-compose.labelstudio.yaml -f docker-compose.labelstudio.local.yaml \
  up -d --no-deps --force-recreate ls-webhook
./scripts/compose-prod.sh up -d --force-recreate dagster-code-server dagster dagster-daemon  # 조용한 시간대
docker compose -f /home/user/work_p/agent/docker-compose.agent.yml up -d agent
```

검증: 새 토큰 whoami 200 / 센서 tick / agent 로그(토큰 오류는 크래시 없이 15분마다 에러 로그만 — 반드시 확인). `LS_WEBHOOK_SECRET`은 별개 — 바꾸면 전 프로젝트 웹훅 재등록 필요, 함부로 변경 금지.

## 스테이징 LS (전용 env 필수)

`.env.test`에는 LS 계열 변수가 없다. 스테이징 LS는 전용 env 파일로만 기동하고, 아래는 **prod 기본값이 배어 있어 반드시 오버라이드**:

```
LS_PORT=8085  LS_WEBHOOK_PORT=8005  LS_POSTGRE_HOST=pipeline-dev-postgres-1
DATAOPS_POSTGRES_DSN=<staging DB>       # 기본값 = prod vlm_pipeline!
DAGSTER_GRAPHQL_URL=<staging dagster>   # 기본값 = prod docker-dagster-1:3030!
LS_API_KEY=<스테이징 발급>  LS_WEBHOOK_SECRET=<스테이징 값>  MINIO_*=<staging>
```

기동: `docker compose -p pipeline-dev --env-file <staging-env> -f docker-compose.yaml -f docker-compose.labelstudio.yaml -f docker-compose.labelstudio.local.yaml up -d postgres labelstudio ls-webhook` — **`-p pipeline`으로 올리면 prod LS 볼륨/DB를 공유하므로 절대 금지.**

알려진 quirk (2026-07-22 리허설 실측): `LS_WEBHOOK_PORT`를 8003 외 값으로 두면 ports 매핑은 `호스트:${LS_WEBHOOK_PORT}→컨테이너:8003` 고정인데 서버는 컨테이너 안에서 `${LS_WEBHOOK_PORT}`로 listen → 어긋남. LS→웹훅 전달은 컨테이너 IP 직통이라 정상 동작하고, **호스트 경유 접근(curl localhost:8005 등)만 안 됨** — 헬스체크는 `docker exec <ctr> curl localhost:${LS_WEBHOOK_PORT}/health`로. MinIO 없이 띄우는 격리 리허설은 `ALLOW_INSECURE_DEFAULT_CREDS=1` 필요.

## 외주 포크와의 관계 · 출구 경로

- 이 오버레이는 **외주(GCP) LS와 동일 코드** — 내부/외주 분기는 코드가 아니라 역할(role) 데이터로만 존재한다. 외주 쪽 소스는 워크스테이션 eng-a 계정 로컬 레포가 원본(포크는 고정 방침).
- **출구 경로**: 포크를 다시 활발히 개발하거나 upstream(Label Studio) 버전업을 결정하는 시점이 오면, 그때 eng-a 소스+이력을 기반으로 **독립 포크 레포**(work_p 아래)로 승격한다. 그 전까지는 이 overlay가 내부/외주 공용 재현 소스.

## presign 갱신 (2026-07-22 인시던트 후 임시 운영)

**`ls_presign_renew_schedule`은 STOPPED 상태** — 갱신 잡의 3중 버그(프로젝트 이름 조회 30개 제한→빈 중복 자동 생성, 이미지 프로젝트 인덱스 붕괴, data 전체 교체)로 인해 코드 수정 배포 전까지 정지. 상세·수정 diff: [presign-renew-bugfix](../design-docs/gcpls/presign-renew-bugfix.md).

수정 배포 전까지 **URL 만료(7일) 전 주기적으로 수동 갱신** 필요:
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
docker cp docker/labelstudio/tools/renew_by_id.py docker-dagster-code-server-1:/tmp/
docker exec docker-dagster-code-server-1 bash -c \
  'PYTHONPATH=/:/src/python:/src/vlm python3 /tmp/renew_by_id.py <프로젝트ID[,ID...]>'
```

## 웹훅 복구 (미결 — 별도 결정)

2026-07-21 실측: 유효(헤더 포함) 웹훅 0개, agent가 만든 headerless만 존재(수신부 403). 원인: `WEBHOOK_HOST=ls-webhook`이 LS URL 검증기에 거부되어 파이프라인 등록이 실패해 온 것. 현재 운영은 Slack `/sync-approve` 수동 경로. 복구하려면: ① `WEBHOOK_HOST`를 검증기 통과 형식(IP/다중 세그먼트)으로 ② 프로젝트별 `python src/gemini/ls_webhook.py register --project <id>` ③ agent의 headerless 정리 + reconciler 헤더 지원(또는 비활성). 자동 동기화 동작이 바뀌므로 팀 결정 후 진행.

## 리뷰 상태 이원화 주의

LS의 `task.review_status`/프로젝트 submit·mid-review(신규)와 파이프라인 DB `labels.review_status`(ls_webhook finalize)는 **서로 모르는 별개 체계**. 리뷰 reject는 웹훅 이벤트를 발생시키지 않는다. 내부에서 LS 리뷰 기능을 도입하면 "리뷰 완료 → finalize" 순서를 팀 프로세스로 정의할 것.
