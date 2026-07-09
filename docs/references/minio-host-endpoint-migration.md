# NAS 10.0.0.36 → 10.0.0.51 이전 가이드

> **목적**: 기존 `10.0.0.36` (EOL) → 신규 ASUSTOR `10.0.0.51` (AS7216RDX-7561, 200TB) 이전 작업 전체 프로세스/체크리스트.
>
> **합의된 큰 그림**: MinIO 컨테이너 이전 + vlm-dataset NFS 마이그(B-narrow) + 나머지 CIFS 공유 NFS 통합(옵션 B). LS는 그대로 둠. 나머지 4개 MinIO 버킷도 그대로 운영.

---

## 0. 배경

- **10.0.0.36 NAS EOL** → 10.0.0.51 (Lockerstor R Pro Gen2, ASUSTOR ADM 5)로 통합
- **10.0.0.36 현재 운영 자원**:
  - MinIO prod (9000/9001) — 111 GiB / 26K objects
  - MinIO staging (9002/9003) — 11 GiB / 3K objects
  - CIFS 공유: `incoming`, `archive`, `staging`, `external`(=`nas`), `AI_data`(=`nas_192tb`), `alarm`
- **10.0.0.51 최종 디렉토리 구조** (옵션 B 확정):
  ```
  /volume1/data/                  ← NFS export (rw, Squash="모든 사용자를 admin에 매핑")
    ├── incoming/                 ← 구 10.0.0.36 /incoming
    ├── archive/                  ← 구 10.0.0.36 /archive
    ├── staging/{incoming,archive}/
    ├── datasets/                 ← vlm-dataset NFS 마이그(B-narrow) 대상
    └── _tmp/                     ← atomic write staging
  /volume1/minio_data_prod/       ← MinIO prod 데이터 (NAS 로컬, NFS 아님)
  /volume1/minio_data_staging/    ← MinIO staging
  ```

## 1. 3개 트랙

| 트랙 | 작업 | 책임 | 우선순위 |
|------|------|------|---------|
| **A. MinIO 이전** | 10.0.0.36 → 10.0.0.51 컨테이너 + 데이터 + endpoint | 사용자 + admin | 🔥 시급 (EOL) |
| **B. CIFS → NFS 단일 공유 통합** | incoming/archive/staging 등 데이터 이전 + 호스트 fstab/compose 변경 | admin (데이터) + 사용자 (fstab/compose) | 🟢 admin 일정 의존 |
| **C. vlm-dataset NFS 마이그 (B-narrow)** | `defs/build/assets.py` 분기 + DB UPDATE | 사용자 | 🟡 A 끝난 후, 별도 1-2주 |

→ **A부터** 진행. B는 admin 응답 받는 대로 병행. C는 A 종료 후 별도 트랙.

---

## 2. 진행 상태 체크리스트

### 사전 준비 (완료)

- [x] NAS admin 계정 발급
- [x] Docker Engine 28.1.1 설치 (수동 .apk → ADM UI)
- [x] Portainer CE 설치 (`https://10.0.0.51:9443`, 포트 8000 제외)
- [x] NFS export rw 권한 + Squash 매핑 적용
- [x] 호스트 `/etc/fstab` 영구 등록 (`10.0.0.51:/volume1/data → /home/user/mou/nas_200tb`)
- [x] 컨테이너 root + `user` 사용자 write 검증
- [x] MinIO 이미지 호스트에 `docker save` 완료 (`/tmp/minio_image.tar.gz`, 59 MB)
- [x] 10.0.0.36 데이터 메타 백업 (`/tmp/10.0.0.36_{prod,staging}_du.txt`)
- [x] 10.0.0.51 `/volume1/data` 안 서브폴더 6개 생성 (incoming/archive/staging/{incoming,archive}/datasets/_tmp)

### Track A — MinIO 이전

- [ ] **Step 1**. MinIO 이미지 NAS로 전송
- [ ] **Step 2**. MinIO 데이터 디렉토리 생성 (`/volume1/minio_data_{prod,staging}`)
- [ ] **Step 3**. Portainer Stack으로 MinIO 2개 컨테이너 배포
- [ ] **Step 4**. 헬스체크 (`/minio/health/live` 200)
- [ ] **Step 5**. 버킷 생성 (10.0.0.36과 동일 이름)
- [ ] **Step 6**. `mc mirror` 10.0.0.36 → 10.0.0.51 (1-4시간, 백그라운드)
- [ ] **Step 7**. 검증 (`mc du` diff)
- [ ] **Step 8**. `dev` 브랜치 PR — 22개 파일 `10.0.0.36` → `10.0.0.51`
- [ ] **Step 9**. Staging endpoint 변경 → e2e 검증 (1-2주)
- [ ] **Step 10**. Prod 컷오버 (다운타임 30분-1시간)
- [ ] **Step 11**. 10.0.0.36 MinIO ro 모드 유지 (1-2주, rollback 안전망)
- [ ] **Step 12**. 10.0.0.36 MinIO 데이터 삭제 + 컨테이너 종료

### Track B — CIFS 공유 이전 (admin 의존)

- [ ] admin에 10.0.0.36 → 10.0.0.51 데이터 마이그 요청
- [ ] 각 공유 데이터 크기 확인
- [ ] 마이그 일정 협의
- [ ] 데이터 마이그 완료
- [ ] 호스트 `/etc/fstab`에서 CIFS 라인 5줄 제거
- [ ] `docker-compose.yaml` bind mount source path 변경 (target 유지)
- [ ] `dev` 브랜치 PR
- [ ] 컷오버 (마운트 잠시 끊김)

### Track C — vlm-dataset NFS 마이그 (B-narrow)

- [ ] `src/vlm_pipeline/defs/build/assets.py` 분기 추가 (DATASET_BUCKET → NFS path)
- [ ] vlm-dataset 데이터 → `/nas_200tb/datasets/` 마이그
- [ ] DB UPDATE: `dataset_bucket='fs'`
- [ ] 학습 머신 read path 변경 (해당 시)
- [ ] e2e (build_dataset job)
- [ ] 검증 + 안정화 1주

---

## 3. 단계별 상세 명령

### Step 1 — MinIO 이미지 NAS로 전송 (5분)

```bash
# 호스트(10.0.0.10) SSH 세션에서
gunzip -c /tmp/minio_image.tar.gz | ssh admin@10.0.0.51 'sudo docker load'

# 첫 SSH라면 known_hosts 등록 위해 한 번 미리:
ssh admin@10.0.0.51 'echo ok'   # yes 입력
```

확인:
```bash
ssh admin@10.0.0.51 'sudo docker images | grep minio'
```

### Step 2 — 데이터 디렉토리

```bash
# NAS SSH
sudo mkdir -p /volume1/minio_data_prod /volume1/minio_data_staging
sudo chmod 755 /volume1/minio_data_prod /volume1/minio_data_staging
```

### Step 3 — MinIO Stack 배포 (Portainer)

`https://10.0.0.51:9443` → **Stacks → + Add stack** → 이름 `minio`:

```yaml
services:
  minio-prod:
    image: minio/minio:latest
    container_name: minio-prod
    restart: unless-stopped
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
      MINIO_PROMETHEUS_AUTH_TYPE: public
    volumes: ["/volume1/minio_data_prod:/data"]
    command: server /data --console-address ":9001"

  minio-staging:
    image: minio/minio:latest
    container_name: minio-staging
    restart: unless-stopped
    ports: ["9002:9000", "9003:9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
      MINIO_PROMETHEUS_AUTH_TYPE: public
    volumes: ["/volume1/minio_data_staging:/data"]
    command: server /data --console-address ":9001"
```

**Deploy the stack**.

### Step 4 — 헬스체크

```bash
curl -sf http://10.0.0.51:9000/minio/health/live && echo "prod OK"
curl -sf http://10.0.0.51:9002/minio/health/live && echo "staging OK"
```

### Step 5 — 버킷 생성

```bash
# 호스트(10.0.0.10)에서
docker exec docker-minio-1 mc alias set new-prod    http://10.0.0.51:9000 minioadmin minioadmin
docker exec docker-minio-1 mc alias set new-staging http://10.0.0.51:9002 minioadmin minioadmin

# 10.0.0.36에서 실제 버킷 목록 확인
docker exec docker-minio-1 mc ls old-prod
docker exec docker-minio-1 mc ls old-staging

# 같은 이름으로 생성 (5개 표준 버킷)
for b in vlm-raw vlm-labels vlm-processed vlm-dataset vlm-classification; do
  docker exec docker-minio-1 mc mb new-prod/$b
  docker exec docker-minio-1 mc mb new-staging/$b
done
```

### Step 6 — `mc mirror` (백그라운드)

```bash
docker exec docker-minio-1 mc mirror --preserve old-prod    new-prod    2>&1 | tee /tmp/mirror-prod.log
docker exec docker-minio-1 mc mirror --preserve old-staging new-staging 2>&1 | tee /tmp/mirror-staging.log
```

> 1-4시간 (네트워크/디스크 IO 따라). `--watch` 옵션으로 증분 추적도 가능.

### Step 7 — 검증

```bash
docker exec docker-minio-1 mc du --recursive new-prod    > /tmp/10.0.0.51_prod_du.txt
docker exec docker-minio-1 mc du --recursive new-staging > /tmp/10.0.0.51_staging_du.txt
diff /tmp/10.0.0.36_prod_du.txt    /tmp/10.0.0.51_prod_du.txt
diff /tmp/10.0.0.36_staging_du.txt /tmp/10.0.0.51_staging_du.txt
```

### Step 8 — Endpoint 변경 PR (`dev` 브랜치)

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline
git checkout dev && git pull
git checkout -b feat/migrate-minio-to-10.0.0.51

# 일괄 sed (CLAUDE.md, docs는 검토 후)
grep -rlE "172\.168\.47\.36" docker/.env.test.example docker/.env.test.template \
  docker/docker-compose.yaml docker/docker-compose.labelstudio.yaml \
  src/ configs/ scripts/ 2>/dev/null | \
  grep -v -E "\.pyc|dagster_home" | \
  xargs sed -i 's|172\.168\.47\.36|10.0.0.51|g'

git diff
git commit -m "feat(infra): migrate MinIO endpoint 10.0.0.36 → 10.0.0.51"
git push -u origin feat/migrate-minio-to-10.0.0.51
gh pr create --base dev --title "feat(infra): migrate MinIO to 10.0.0.51"
```

### Step 9 — Staging e2e 검증 (1-2주)

PR `dev` 머지 → CI 자동 배포 → staging 환경에서:
- sensor evaluate 정상 동작
- dispatch run 정상
- MinIO console (`:9003`) 객체 확인
- compute_logs에 `endpoint URL` 에러 없는지

### Step 10 — Prod 컷오버 시퀀스

> **사전 공지** (T-3d): 라벨러/사용자에 다운타임 안내

```bash
# T+0   Dagster sensor 모두 정지 (UI 또는 dagster CLI)
# T+5   in-flight run 완료 대기
# T+10  컨테이너 stop
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
docker compose stop dagster dagster-daemon dagster-code-server

# T+15  최종 delta mirror
docker exec docker-minio-1 mc mirror --preserve old-prod    new-prod
docker exec docker-minio-1 mc mirror --preserve old-staging new-staging

# 검증
docker exec docker-minio-1 mc du new-prod    | tail
docker exec docker-minio-1 mc du new-staging | tail

# T+25  .env 호스트 직접 수정 (git 미추적)
sed -i 's|172\.168\.47\.36|10.0.0.51|g' /home/user/work_p/Datapipeline-Data-data_pipeline/docker/.env

# T+30  main PR 머지 (Step 8 PR을 dev → main으로) — CI 자동 prod 배포 트리거
gh pr create --base main --head dev --title "feat(infra): migrate MinIO endpoint to 10.0.0.51"
# 또는 즉시 재기동
docker compose up -d

# T+40  헬스체크
curl -sf http://10.0.0.51:9000/minio/health/live && echo "prod OK"

# T+45  sensor 재가동 (Dagster UI)

# T+46  LS presigned URL 일괄 갱신 (라벨러 영상 즉시 복구)
#       — 기존 task의 data.video는 10.0.0.36 URL이라 끊김. 새 endpoint로 재발급 필요
#       — 자동 갱신 schedule(ls_presign_renew_job, 매일 05:00 KST)도 있지만 즉시 복구 권장
docker exec docker-dagster-1 python /src/vlm/gemini/ls_tasks.py renew --project-name <project>
# 또는 모든 active project에 대해 (Dagster UI 또는 LS UI에서 project name 확인 후 반복)
```

### Step 11 — 10.0.0.36 ro 모드 (rollback 안전망)

```bash
# 10.0.0.36에서 (admin)
# MinIO 컨테이너에 --read-only 또는 정책 ro 설정
# 또는 docker stop 후 데이터 보존
```

1-2주 운영 모니터링 후 안전 확인되면 Step 12.

### Step 12 — 10.0.0.36 정리

```bash
# admin에서
docker stop minio-prod-old minio-staging-old
docker rm   minio-prod-old minio-staging-old
# 데이터 삭제는 추가 1주 대기 후
```

---

## 4. NAS admin 요청 사항

### 즉시 요청 (Track A)

```
1. NFS export 권한: /volume1/data → rw + Squash="모든 사용자를 admin에 매핑"
   (완료)
2. 디렉토리 미리 생성:
   /volume1/minio_data_prod
   /volume1/minio_data_staging
```

### Track B 요청 (별도)

```
10.0.0.36의 다음 데이터를 10.0.0.51 /volume1/data 서브폴더로 옮겨주세요:

| 10.0.0.36 공유            | 10.0.0.51 위치                      |
|----------------------|-----------------------------|
| /volume1/incoming     | /volume1/data/incoming/         |
| /volume1/archive      | /volume1/data/archive/          |
| /volume1/staging/incoming | /volume1/data/staging/incoming/ |
| /volume1/staging/archive  | /volume1/data/staging/archive/  |
| /volume1/external (=nas)  | (필요 시 별도 협의)               |
| /volume1/AI_data (=nas_192tb) | (필요 시 별도 협의)            |
| /volume1/alarm        | (별도 계정 nas-alarm.cred 사용)   |

- 각 공유 데이터 크기 알려주세요
- 마이그 일정 협의 부탁드립니다
- 10.0.0.36의 다른 docker 컨테이너 목록도 함께 부탁드립니다 (외부에서 docker ps 못 봐서 미확인)
```

---

## 5. 트러블슈팅 메모 (2026-05-12 진행 중 발생한 이슈)

| 증상 | 원인 | 해결 |
|------|------|------|
| App Central 자동 설치 stalled | NAS 백엔드 hang | 수동 .apk 업로드 (App Central → ⚙️ → 수동 설치) |
| ADM UI 수동 설치도 stalled | 브라우저 세션 이슈 | 로그아웃 → 새 탭 → 재시도 |
| `apkg --install` 옵션 없음 | ADM의 `apkg`는 download만 지원, install은 GUI-only | 수동 설치 GUI 사용 |
| scp 속도 30 KB/s | macOS(192.168.0.x)와 NAS(10.0.0.x) 다른 서브넷, 가정용 라우터 경유 | 호스트(10.0.0.10) 경유 (같은 LAN) |
| Portainer 8000 포트 충돌 | ADM 웹 UI가 8000 사용 | `-p 8000:8000` 제외, 9443만 사용 |
| NFS 마운트 ro | 서버 측 export 권한 read-only | NAS admin이 NFS 권한 → rw 변경 |
| 컨테이너 root write 거부 가능성 | Squash 매핑 안 됨 | Squash → "모든 사용자를 admin에 매핑" |
| 신규 폴더 소유주 `root:root` (기존 `999:ping`) | Squash가 부분 매핑일 수도 | user(UID 1000)로 write 테스트로 검증 필요 |

---

## 6. 미해결 / 리스크

| 항목 | 상태 |
|------|------|
| 10.0.0.36의 CIFS 공유들 이전 일정 (incoming/archive/staging/nas/nas_192tb/alarm) | admin 확인 필요 |
| 각 공유 데이터 크기 | admin 확인 필요 |
| 10.0.0.36의 다른 docker 컨테이너 | 외부에서 확인 불가, admin 답 필요 |
| NAS NIC 1GbE 링크 (10GbE 미활용) | 별개 점검, 우선순위 낮음 |
| dispatch-agent 등 외부 서비스의 10.0.0.36 endpoint 의존성 | 마이그 전 검색 + 알림 필요 |
| Squash 매핑 정확성 (root → admin) | user(UID 1000) write 테스트로 검증 필요 |

---

## 7. 참고

### 환경 정보

| 항목 | Production | Staging |
|------|-----------|---------|
| Dagster UI | `http://10.0.0.10:3030` | `http://10.0.0.10:3031` |
| MinIO endpoint (이전 후) | `http://10.0.0.51:9000` | `http://10.0.0.51:9002` |
| MinIO Console | `:9001` | `:9003` |
| NAS NFS export | `10.0.0.51:/volume1/data` |  |
| 호스트 마운트 | `/home/user/mou/nas_200tb` |  |

### 자격 증명

- MinIO ROOT_USER / PASSWORD: `minioadmin` / `minioadmin` (마이그 후 변경 권장)
- ADM admin: NAS 관리자
- 호스트 `user`: 호스트(10.0.0.10) SSH

### 관련 문서

- [CLAUDE.md](../../CLAUDE.md) — 환경 이중 구조, MinIO 버킷/경로 정책
- [docs/references/dbeaver-pg-duckdb-test-guide.md](./dbeaver-pg-duckdb-test-guide.md) — staging Postgres 접속 가이드
- [docs/runbook.md](../runbook.md) — 일반 운영 매뉴얼

### 메모리 (Claude 세션)

- `~/.claude/projects/.../memory/project_nas_200tb.md` — 진행 상태 + 이력
- `~/.claude/projects/.../memory/project_hosts.md` — 호스트 IP/역할
