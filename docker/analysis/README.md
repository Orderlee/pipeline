# analysis 컨테이너 — JupyterLab + FiftyOne + Streamlit (임베딩 시각화/유사검색)

임베딩 파이프라인이 `image_embeddings`(pgvector)에 적재한 1024-d 벡터를 **FiftyOne** 과
**Streamlit 대시보드**로 시각화/클러스터/유사검색하는 분석 surface.

FiftyOne 메타데이터 store 는 **`fiftyone-mongo` 사이드카**를 사용한다. compose 선언은 `mongo:8.0`(마이너까지 고정)이지만,
지금 돌고 있는 컨테이너는 그 핀 이전에 만든 `mongo:8`(mongod 8.2.12)이다 — 아래 ⚠️ 두 번째 항목.
(번들 mongod 는 slim 베이스에서 미동작 → 2026-06-15 staging 검증 후 사이드카로 전환.
Dockerfile 상단 주석과 `ENV FIFTYONE_DATABASE_DIR` 는 그 시절 잔재이며, compose 가
`FIFTYONE_DATABASE_URI=mongodb://fiftyone-mongo:27017` 로 덮어쓴다.)

> ⚠️ **메이저 태그로 띄워 두면 죽는다.** compose 가 `mongo:8` 을 쓰던 동안 setFCV 를 아무도
> 실행하지 않아 FCV(featureCompatibilityVersion)가 7.0 에 머물렀고, 그 사이 태그가 8.0 → 8.2.12 로
> 흘렀다. 8.2 는 FCV 8.0 미만을 거부한다 → 2026-09-03 08:28 KST 컨테이너가 새로 만들어지자마자
> `exit 62` 크래시루프(재시작 11회)로 FiftyOne 전체가 죽었다. App 화면엔
> `AutoReconnect: Connection reset by peer` 로만 보여 원인이 가려졌다. 그래서 compose 를
> `mongo:8.0` 으로 박았다(`docker/docker-compose.yaml` `fiftyone-mongo` 서비스 주석 참고).
>
> ⚠️ **그 핀은 아직 한 번도 적용되지 않았다.** 복구는 데이터의 FCV 를 8.0 으로 올린 뒤 같은
> `mongo:8` 컨테이너를 다시 켠 것이라, 지금 `docker-fiftyone-mongo-1` 은 mongod **8.2.12** · FCV 8.0 으로 돈다.
> `deploy-stack.sh` 는 analysis 서비스를 `up -d --no-deps` 로만 올리고 이 사이드카는 부르지 않아
> 배포로는 recreate 되지 않는다 — 손으로 recreate 하는 순간 바이너리가 8.2.12 → 8.0 계열로 바뀐다.
> 그래서 recreate·태그 변경 전에는 FCV 가 새 바이너리가 받는 값인지 먼저 확인하고
> (`db.adminCommand({getParameter:1, featureCompatibilityVersion:1})`) 파일 백업부터 뜰 것.
> FCV 가 이미 8.0 이라 태그를 `mongo:7` 로 내려 되돌리는 길은 막혀 있고, 파일 백업이 유일한
> 되돌림 수단이다(백업 경로·루트 컨테이너 경유 필요성은 compose 주석).

## 기동

```bash
# 환경별 profile 활성 + 기동 (prod 예시; staging 은 compose-staging wrapper)
COMPOSE_PROFILES=analysis ./scripts/compose-prod.sh up -d analysis analysis-fiftyone analysis-streamlit
```

| surface | 서비스 / 컨테이너 | 컨테이너 포트 | prod 호스트 포트 | 자동 기동 |
|---|---|---|---|---|
| JupyterLab | `analysis` / `docker-analysis-1` | 8888 | `8888` | ✅ |
| FiftyOne App 좌석 1 (직결, 메모리 상한 없음) | `analysis-fiftyone` / `docker-analysis-fiftyone-1` | 5151 | `5158` (`FIFTYONE_PORT_1`) | ✅ |
| **FiftyOne 좌석 라우터 (nginx) — 다들 여기로 붙는다** | `analysis-fiftyone-proxy` / `docker-analysis-fiftyone-proxy-1` | 5151 / 5443 | `5153` (`FIFTYONE_PORT`) / `5443` (`FIFTYONE_TLS_PORT` — TLS 를 켜기 전엔 연결 거부) | ⚠️ 재시작만 |
| FiftyOne App 좌석 2~5 (상한 `FIFTYONE_SEAT_MEM`) | `analysis-fiftyone-N` / `docker-analysis-fiftyone-N-1` | 5151 | `5154`~`5157` (`FIFTYONE_PORT_{2..5}`) — prod 에는 좌석 2·3 만 있다 | ⚠️ 재시작만 |
| FiftyOne 동기화·좌석 배정·업로드 API (`/sync/*`·`/seat/*`·`/upload/*`) | `analysis-sync` / `docker-analysis-sync-1` | 8010 | 없음 — 브라우저는 라우터의 `/__seat_assign`·`/__upload/` 로만 닿는다 | ✅ |
| Streamlit 대시보드 | `analysis-streamlit` / `docker-analysis-streamlit-1` | 8501 | `8503` (`STREAMLIT_PORT`) | ✅ |
| FiftyOne 메타데이터 | `fiftyone-mongo` / `docker-fiftyone-mongo-1` | 27017 | 없음 | ⚠️ 재시작만 |

> ⚠️ **`:5153` 은 `analysis-fiftyone` 컨테이너가 아니라 좌석 라우터(nginx)의 포트다.** FiftyOne OSS 는
> 서버 상태가 프로세스 전역이라 한 프로세스를 여럿이 보면 A 의 데이터셋 전환이 B 화면을 끌어간다 —
> 그래서 1인 1프로세스(좌석)로 쪼갰고, 라우터가 `?seat=N`·IP 표·쿠키로 사람마다 다른 좌석에 보낸다.
> 좌석 1 의 `:5158` 은 라우터가 죽었을 때의 우회로 겸 디버깅용 직결 경로다. 좌석이 정해지지 않은 화면
> 접속은 `analysis-sync` 의 `/seat/assign` 이 빈 좌석을 골라 주므로, sync 가 죽으면 자동 배정 대신 정적
> 좌석 선택 페이지가 뜬다. 라우팅 규칙은 `nginx-seats.conf`·`seat-tls/routing.inc` 주석, 프로세스 격리·
> 메모리 상한의 이유는 compose 의 `x-fiftyone-seat` 앵커 위 주석이 정본이다.
>
> ⚠️ **"재시작만" = `restart: unless-stopped` 는 걸려 있지만 배포가 만들어 주지는 않는다.**
> `deploy-stack.sh` 는 `analysis`·`analysis-fiftyone`·`analysis-streamlit`·`analysis-sync` 넷만
> `up -d --no-deps` 하고, compose 는 명시 나열된 서비스만 올린다(위 기동 명령도 라우터를 올리지 않는다).
> 라우터·좌석 2~5·mongo 는 배포 밖에서 누군가 `up` 해 둔 덕에 있을 뿐이라, 지워지면 배포를 몇 번 돌려도
> 돌아오지 않는다 — 라우터가 없으면 `:5153` 전체가 죽는다. 복구:
> `COMPOSE_PROFILES=analysis ./scripts/compose-prod.sh up -d --no-deps analysis-fiftyone-proxy`.
> 좌석 4·5 는 compose·nginx·배정기에 정의돼 있지만 prod 에 컨테이너가 없다(2026-09-29 `docker ps -a`).
> 좌석도 `fiftyone_relaunch.py` 로 뜨므로 아래 「남은 천장」(App 자식만 죽으면 컨테이너는 살아서
> `unhealthy` 로만 남는다)이 좌석마다 따로 적용된다 — 좌석이 살아 있는지는 `docker ps` 의 health 로 볼 것.

JupyterLab token = `JUPYTER_TOKEN`, 미설정 시 토큰 없음 — 내부망 전용.
FiftyOne 이 처음 띄우는 데이터셋은 `FO_DATASET`(기본 `sourcei`) — App 안에서 언제든 전환 가능.

> ✅ **2026-08-18 P0 편입 완료.** 위 표의 세 프로세스가 각각 독립 서비스이고 `restart: unless-stopped`
> 라 죽으면 자동으로 다시 뜬다. **`docker exec -d` 로 손기동하던 절차는 폐기됐다.**
> 배포가 기동을 보증하는 건 `deploy-stack.sh` 의 `analysis_active()` 분기 한 줄뿐이다 —
> `up -d --no-deps analysis analysis-fiftyone analysis-streamlit analysis-sync`. 표에 없는
> `analysis-sync`(FiftyOne 증분 동기화 API, 내부 :8010)가 뒤에 추가돼 **4개**다.
> 좌석 1 은 `analysis-fiftyone` 이라 목록 안이지만, 좌석 2~5(`analysis-fiftyone-2`~`-5`)·
> 좌석 라우터(`analysis-fiftyone-proxy`)·`fiftyone-mongo` 는 목록 밖이라 **배포가 만들지도
> recreate 하지도 않는다** — 손으로 `up -d` 해야 생기고, 그 뒤로는 `restart: unless-stopped` 에만
> 기댄다. 그래서 `datapipeline-analysis:latest` 를 재빌드해도 이미 떠 있는 좌석은 옛 이미지로 남는다.
> 네 서비스가 `depends_on: fiftyone-mongo` 를 걸고 있어도 `--no-deps` 라 mongo 는 함께 올라오지 않는다.
>
> ⚠️ 남은 천장: `fiftyone_relaunch.py` 는 App 을 자식 프로세스로 띄우고 자신은 sleep 한다.
> **자식만 죽으면 컨테이너는 살아 있어** healthcheck 만 `unhealthy` 로 바뀌고 compose 는
> 재시작하지 않는다(`docker ps` 로 보인다). 실제로 관측되면 autoheal 사이드카를 붙일 것.

### 코드 반영 — `docker cp` 는 더 이상 쓰지 않는다

`docker/analysis/` 전체가 `/workspace` 로 **bind mount** 돼 있고, 플러그인 5종도
`__plugins__/` 아래로 각각 마운트돼 있다. 이 repo 가 곧 배포 repo(`DEPLOY_REPO_ROOT`)이므로
**여기서 파일을 고치는 순간 컨테이너 안 코드가 바뀐다.**

| 바꾼 것 | 반영 방법 |
|---|---|
| `*.py` (스크립트) | 없음 — 다음 `docker exec ... python` 실행이 새 코드를 읽는다 |
| 플러그인 `__init__.py` | 없음 — `FIFTYONE_PLUGINS_CACHE_ENABLED=true` 라도 `dir_state` 로 자동 무효화 |
| `embedding_dashboard.py` | 없음 — Streamlit 자동 리로드 |
| FiftyOne App 이 import 하는 모듈 | `docker restart docker-analysis-fiftyone-1` |
| `requirements.txt` / `Dockerfile` | `COMPOSE_PROFILES=analysis ./scripts/compose-prod.sh build analysis` 후 세 서비스 recreate |

> ⚠️ **컨테이너 안에서만 파일을 만들지 말 것.** bind mount 가 이미지 레이어의 `/workspace` 를
> 통째로 가리므로, repo 에 없는 파일은 **컨테이너에서 보이지도 않는다**. (2026-08-18 편입 시
> 컨테이너 전용으로 남아 있던 6파일 720줄을 repo 로 회수했다.)
>
> ⚠️ `docker/analysis/**` 는 배포 워크플로의 `paths-ignore` 대상이라 **분석 코드만 push 해도
> 배포가 돌지 않는다** — 라벨링이 끊기지 않는다는 뜻이고, 동시에 CI 가 코드를 날라주지도
> 않는다는 뜻이다. 이 repo 에서 직접 커밋하는 한 반영은 이미 끝나 있다.
>
> ⚠️ 배포는 `DEPLOY_REPO_ROOT` 를 `git reset --hard` 한다 — **이 repo 에 체크아웃된 브랜치가
> 무엇이든 그 브랜치가 리셋된다.** 미push 커밋이 있는 상태에서 다른 사람이 `main` 에 push 하면
> 소실된다. 작업 브랜치는 push 해 둘 것.

## FiftyOne 좌석 라우팅 (다중 사용자)

FiftyOne OSS 서버 상태는 **프로세스 전역 싱글턴**이다(`server/events/state.py` 의 `_state`) —
`set_dataset` 같은 mutation 이 발신자를 뺀 전 접속자에게 팬아웃된다(`server/mutation.py:118`,
설치본 1.19.0 기준 — 줄 번호는 버전 따라 움직인다). 접속자별 상태 분리가 없어 격리 단위는
프로세스뿐이라, "1인 1프로세스" 를 nginx 좌석 라우터 하나 뒤에 감춘다. 좌석(프로세스 분리)은
2026-08-31, 빈 좌석 자동 배정은 2026-09-03 에 붙었다.

| 구성요소 | 역할 |
|---|---|
| `analysis-fiftyone-proxy`(nginx) | 단일 접속점. `FIFTYONE_PORT`(prod `.env` 5153) + `FIFTYONE_TLS_PORT`(5443, TLS 켜기 전엔 연결 거부). 라우팅 본문 = `docker/analysis/nginx-seats.conf`(map) + `seat-tls/routing.inc`(location) |
| `analysis-fiftyone`(좌석1) · `-2`~`-5` | 좌석당 FiftyOne 프로세스 1개. 좌석1 만 메모리 상한 없음(무거운 프롬프트 분석 전용). 좌석1 직결 포트는 `FIFTYONE_PORT_1`(compose 기본 5158) — 프록시가 죽었을 때의 우회로 |
| `analysis-sync` `GET /seat/occupancy`, `GET /seat/assign` | 요청마다 좌석별 점유 카운터(각 좌석 `fiftyone_relaunch.py` 가 `:5160` 에 연다 — `:5151` 에 붙은 비루프백 연결 수)를 병렬 조회 + 빈 좌석 자동 배정(302). 호스트 포트가 없어 진단은 `docker exec docker-analysis-sync-1 curl -s localhost:8010/seat/occupancy` |

**배정 우선순위**: `?seat=N`(1년 쿠키를 박는다) > 관리자 지정 IP 표(`nginx-seats.conf` 의
`map $remote_addr $ip_seat`) > 쿠키 > 자동 배정기(`/seat/assign`) > 배정기 무응답 시 정적 선택 페이지.
IP 표가 쿠키보다 우선인 이유: 무거운 분석을 하던 사람이 자동배정으로 3g 상한 좌석에 앉아 OOM 으로
죽는 사고가 실제로 났었다(코드 주석, 2026-09-03). 그래서 표에 있는 사람은 쿠키가 무시된다 —
`?seat=N` 도 "그 방문만" 이다. ⚠️ `nginx-seats.conf` **머리 주석**(8줄)은 아직 "쿠키 > IP 표" 순서로
적혀 있다 — IP 우선으로 바꾼 c6e0af3 이 e7425df 의 머리 주석을 안 고쳤다. 정본은 map 체인이다.

자동 배정기는 **빈 좌석만** 준다 — 순서 `SEAT_AUTO_ORDER`(기본 `2,3,4,5,1`: 상한 있는 좌석부터,
무제한 좌석 1 은 마지막). 빈 좌석이 없으면 점유 현황 선택 페이지(200)로 떨어진다. 좌석 미정·죽은
좌석이어도 배정기로 보내는 건 **navigation 만**이고 API/SSE 는 JSON 오류(409/503)로 끊는다 — API 를
302 로 보내면 앱이 HTML 을 JSON 으로 파싱하다 죽는다(코드 주석, 2026-09-03 실측). nginx 정적 폴백
페이지는 좌석 1~3 만 나열한다.

⚠️ **컨테이너 Up ≠ 빈 좌석.** 카운터는 App(:5151)이 연결을 안 받으면 503 을 돌려주고 배정기는 그
좌석을 '상태 불명'으로 건너뛴다. `fiftyone_relaunch.py` 는 App 을 자식으로 띄우고 sleep 하므로 자식만
죽으면 컨테이너는 Up·`unhealthy` 로 남고 compose 는 재시작하지 않는다 — 좌석이 에러 없이 후보에서
빠진 채 방치된다. 2026-09-29 실측: 좌석 2·3 이 `Subprocess [... main.py ...] exited with error -9` 뒤
이 상태라 배정기 후보가 좌석 1 뿐이었다. 복구는 그 좌석 컨테이너 `docker restart`.

⚠️ **배포는 프록시·좌석 2~5 를 올리지 않는다.** `deploy-stack.sh` 는 `analysis analysis-fiftyone
analysis-streamlit analysis-sync` 만 `up -d` 한다(compose 는 명시한 서비스만 올린다). compose 가 5석을
정의해도 몇 석이 떠 있는지는 배포가 보증하지 않으니 `docker ps` 로 먼저 확인할 것.

좌석 2~5 메모리 상한은 `FIFTYONE_SEAT_MEM` — compose 기본 3g 는 sourcei 프롬프트 패널에서 OOM 이 나서
prod `.env` 가 6g 로 올려 뒀다(`.env` 주석). `.env` 는 git 미추적이라 이 키가 없는 환경은 3g 로 돈다.

⚠️ **`nginx-seats.conf` 는 단일 파일 bind mount** — 파일이 새 inode 로 교체되면 컨테이너는 옛
inode 를 계속 보고, `nginx -s reload` 가 성공해도 반영되지 않는다(코드 주석: 2026-09-03 에 reload
3회가 전부 무효였다고 기록됨). 반영 확인은 호스트/컨테이너 `stat -c %i` 비교, 확실한 반영은
`docker restart docker-analysis-fiftyone-proxy-1`. `seat-tls/routing.inc` 는 디렉토리 마운트라 이
함정이 없다(reload 로 반영).

HTTPS 는 기본 꺼짐 — `seat-tls/` 에 `tls.conf` 가 없으면 `include /etc/nginx/seat-tls/*.conf` 가
아무것도 못 잡는다. 꺼 둔 이유: 접속이 전부 사내 LAN raw IP 라(DNS·사내 CA 없음) 자체서명 인증서를
켜면 전원이 매번 브라우저 경고를 보는데, 인증이 붙는 게 아니라 얻는 보안이 없다. FiftyOne Enterprise
가 HTTPS 종단을 요구하므로 준비만 해 뒀다(`gen-seat-cert.sh` 주석).

⚠️ **`gen-seat-cert.sh` 가 쓴 `tls.conf` 를 그대로 켜지 말 것.** 스크립트(기본 호스트명
`fiftyone.user.local`)는 인증서와 함께 `routing.inc` include 가 **없는** proxy_pass 전용 server 블록을
`tls.conf` 로 덮어쓴다 — `nginx-seats.conf` 주석이 "예전 초안" 이라 부르는 바로 그 블록이다(M4 수정
543f150 은 당시 미추적이던 이 스크립트를 못 고쳤고, ded4011 에서 그 모양 그대로 편입됐다). 이 블록은
쿠키·IP 표에 안 걸리는 HTTPS 접속자를 전원 좌석 1(default)로 조용히 몰고, 자동 배정·죽은 좌석
폴백·`/__upload/` 도 없다. 켜는 순서: 스크립트(인증서 생성) → `tls.conf` 를 `seat-tls/tls.conf.example`
로 덮어쓰기 → `nginx -s reload` → 떠 있는 좌석 쿠키로 `X-Seat` 헤더 확인(옛 블록엔 `X-Seat` 자체가
없다). 라우팅을 TLS 블록에 복제하지도 말 것 — 한쪽만 고쳐져 조용히 갈라진다(`tls.conf.example` 주석).

브라우저 업로드 경로(`/__upload/*` → `analysis-sync` `/upload/*`, 바디 상한 20g·버퍼링 없음)는 좌석
라우팅 **밖**이다 — prefix `location` 이 `/` 보다 먼저 잡혀 좌석 FiftyOne 을 거치지 않는다(`routing.inc`).

## 사용 (노트북)

```python
import fiftyone_pgvector as fp
rows = fp.load_frame_embeddings(limit=5000)          # pgvector → 임베딩 로드
ds   = fp.build_fiftyone_dataset("frames", rows)      # FiftyOne 데이터셋 + UMAP 시각화
import fiftyone as fo; fo.launch_app(ds)               # FiftyOne App 에서 탐색

fp.search_by_text("a fire on the street", k=20)        # 텍스트→이미지 검색 (embedding-service /embed_text)
fp.search_by_image(rows[0]["image_id"], k=20)          # 이미지 유사 검색 (pgvector <=> cosine)
```

## ⚠️ 운영 주의

- **`/workspace` = 이 repo 의 `docker/analysis/`** (bind mount, 2026-08-18~). 이미지의
  `COPY` 2줄은 마운트에 가려 무의미해졌고 drift 도 사라졌다. 예전에 `docker cp` 로만 존재하던
  스크립트는 전부 repo 로 회수됨. **컨테이너 안에서 새 파일을 만들지 말 것** (repo 에 없으면 안 보인다).
- **MINIO_ENDPOINT**: presigned URL 이 **사용자 브라우저**에서 열려야 하므로
  `ANALYSIS_MINIO_ENDPOINT` 를 host-reachable 주소로 설정.
  현재 prod 값은 `http://10.0.0.51:9000`. 내부 docker 명(`minio:9000`)은 브라우저에서 미도달
  → FiftyOne App 에 이미지가 안 뜬다.
- **FiftyOne 메타데이터**: `fiftyone-mongo` 사이드카 + `./data/fiftyone/mongo` 볼륨에 영속.
  `down -v` 금지 (데이터셋 전부 소실).
- **성능 특성**: 텍스트 검색은 partial HNSW 로 ~50ms 수준. 반면 랜딩 페이지 KMeans 클러스터링은
  캐시 미스 시 수 분 걸린다 (캐시 키 = 임베딩 행 수 → 데이터 증가 시 자동 재계산).
  데모 전에는 미리 한 번 워밍업해 둘 것.
- **자격증명**: `MINIO_ACCESS_KEY`/`SECRET` 는 MinIO root 자격 재사용. read-only 키 분리는 후속 과제.
- **cron 주의**: 호스트 crontab 의 주기 작업은 반드시 `flock` 으로 감쌀 것
  (2026-07-06 `refresh_frames_labels` 오버랩 3중 중첩 → 스왑 쓰래싱으로 호스트 마비 사건).
  현재 등록된 `docker/analysis/` 작업은 `prompt_cos_cron.sh`(02:40 일일), `prompt_cos_batch.sh`(15분 간격),
  `bank_health.sh`(07:17 일일, 뱅크 태그 계약 점검, 2026-08-18 승인) **셋이다**(`crontab -l` 이 정본 —
  analysis 밖의 호스트 작업도 거기 같이 있다). `prompt_cos_*` 둘은 각자 별도 lock + 시간 가드(batch 가
  02~04시에 비켜줌) + 루트 디스크 가드를 갖고 있고, `bank_health.sh` 는 flock + `timeout`(기본 600s) +
  컨테이너 미기동 skip 뿐 시간·디스크 가드는 없다.
  **디스크나 PG 이상을 조사할 때 `prompt_cos_*` 둘을 먼저 의심할 것** — 문서에 없으면 원인에서 빠진다.
  `bank_health.sh` 는 PG 가 아니라 FiftyOne 데이터셋(mongo)을 읽는 점검이라 그 축에선 후순위지만,
  이 목록 자체가 최근까지 그것을 빠뜨리고 있었다.
- **requirements 전량 핀** (`da1f0a4`, 2026-09-03): 그전엔 `fiftyone` 만 핀돼 있어 재빌드가 곧 전체
  드리프트라 손 rebuild 를 못 했고, 그사이 이미지(07-28 빌드)와 requirements(08-21 fastapi 추가)가
  벌어졌다 — 손 `pip install` 로만 살아 있던 fastapi 를 09-02 23:28 recreate 로 잃고 `analysis-sync` 가
  크래시루프에 빠졌다(`RestartCount 16`). 핀이 계약인 이유는 **재빌드 시점을 analysis 쪽이 고르지 못해서**다.
  `docker/analysis/**` 만 바꾼 push 는 paths-ignore 라 배포가 아예 안 돌아 requirements 를 고쳐도
  이미지가 그대로이고(위 표의 손 rebuild 전까지), 반대로 무관한 변경으로 도는 배포도 `BUILD_REQUIRED` 면
  `deploy-stack.sh` 가 analysis 이미지를 함께 빌드하고(analysis profile 이 켜진 prod) `up -d` 가 이미지가
  바뀐 서비스를 recreate 한다 — 2026-09-14 마이그레이션 수정 배포(`34a8914`)가 analysis 4서비스를 이렇게
  재생성했다. Dockerfile 이 `fiftyone_pgvector.py`·`embedding_dashboard.py` 를 COPY 하므로 이 둘만
  바뀌어도 이미지는 바뀐다. 그러니 `requirements.txt` 상단 주석의 "CI 가 이 재빌드를 절대 돌리지
  않는다" 는 analysis 단독 push 에만 맞는 말이다. 지금은 전부 핀 고정 — 올릴 때는 한 번에 하나씩,
  패널(:5153) 로드까지 확인하고 올릴 것(matplotlib 3.11 이 boxplot 의 옛 인자 `labels` 를 제거해 —
  3.9 부터 `tick_labels` 로 개명돼 있었다 — 그림 스크립트가 죽은 전례가 있다).

## FiftyOne 플러그인 — Embeddings 패널 Enterprise 게이팅 우회

Embeddings 패널의 `+`(Compute visualization) 버튼은 OSS 에서 **항상** "Upgrade to
FiftyOne Enterprise" CTA 만 띄운다. `APP_MODE="fiftyone"` 이 **빌드타임 상수**라
minifier 가 실제 호출 분기를 지워버린 것이라, env·설정으로는 못 켠다
(`Embeddings-*.js` 에 `to:()=>{setShowCTA(true)}` 만 남아 있음).

하지만 그 버튼이 원래 부르는 건 앱 코드가 아니라 **오퍼레이터**이고, 동일 기능의
OSS 구현이 `@voxel51/brain` 플러그인에 있다. 번들 패치·포크 없이 플러그인 설치만으로 해결된다.

### 설치

**자체 플러그인 5종(`user-*`)은 설치 절차가 없다** — compose 가 `docker/analysis/plugins/<name>` 을
`__plugins__/<name>` 으로 각각 bind mount 한다(2026-08-18~). repo 가 정본이고, recreate 후
`docker cp` 재적용도 필요 없다.

공식 brain 플러그인만 1회 다운로드가 필요하다:

```bash
# 공식 brain 플러그인 (compute_visualization 등 16개 오퍼레이터)
docker exec docker-analysis-1 fiftyone plugins download \
  https://github.com/voxel51/fiftyone-plugins --plugin-names @voxel51/brain

docker exec docker-analysis-1 fiftyone plugins list   # 확인
```

- `/data/fiftyone/datasets/__plugins__/` 는 **bind mount 안이라 컨테이너 재생성에도 유지**된다
  (호스트 `docker/data/fiftyone/`). 다만 gitignore 대상이라 `docker/data/` 를 밀면
  `@voxel51/brain` 은 사라진다 → 위 명령으로 복구. `user-*` 는 repo 마운트라 영향 없음.
- **플러그인 캐시는 켜져 있어야 한다** (2026-08-14): 기본값(off)은 오퍼레이터 요청마다
  플러그인 모듈을 재임포트해 user-prompt-compare 의 603k행 번들 캐시·dedup 가드가 매번
  증발한다 (드롭다운 1회 = 왕복 20초+). `fiftyone_relaunch.py` 가
  `FIFTYONE_PLUGINS_CACHE_ENABLED=true` 를 세팅하고, **compose 의 `x-analysis-env` 에도
  같은 값이 박혀 있다** — `config.json` 이 recreate 로 사라져도 env 가 덮는다.
- 캐시가 켜져 있어도 플러그인 코드 반영에 App 재시작은 불필요 — 단 파일을 고친 뒤
  **플러그인 디렉토리를 touch** 해야 무효화가 잡힌다 (dir_state 가 플러그인 *디렉토리*
  mtime 기준: `docker exec docker-analysis-1 touch /data/fiftyone/datasets/__plugins__/user-prompt-compare`).
  ⚠️ **패널 이름을 바꾸거나 새 패널을 추가할 땐 App 을 재기동**하고 저장 워크스페이스도
  다시 저장할 것 — 서버 워커가 여러 개라 일부만 새 이름을 알면 그 워커가 응답할 때
  `Panel "<이름>" no longer exists!` 가 뜬다 (2026-08-14 실측).

### 분석 판독 규칙 (2026-08-14 감사 반영 — 어기면 화면이 조용히 거짓말한다)

- **`sourcei.category` 는 사람 정답이 아니다.** v1.0.8.0 모델의 argmax 예측이다
  (실측 3중: `argmax(cos_best_*)==category.label` 7,498/7,498 · `category.confidence` 전부
  null · `pred_v1_0_8_0.label` 과도 7,498/7,498). 사람 정답은 `ground_truth`(영상 단위).
  이 축으로 신버전을 채점하면 구버전 예측을 기준으로 삼는 **자기참조 평가**가 된다.
- **표시/모집단을 항상 확인**할 것. `-prompts` 는 603,318 중 20,000(3.3%)만 그린다. 채택점은
  전수 보존되고 미채택만 층화되므로 **화면의 채택 비율이 모집단보다 약 30배 부풀어 있다** —
  두 패널의 배너·범례가 이 숫자를 직접 표기한다(`미채택 9,207/592,526` 형태).
- 색칠 축을 바꾸면 배너가 **기준 축과 몇 장 상충하는지**, 그리고 값이 100% 동일한 축이
  있으면 그 사실을 스스로 알린다. 상충(같은 값 집합 안에서 갈림)과 세분값(기준에 없는 값)은
  다르게 표기된다 — `event_kind` 4,321장 차이는 상충 0장, 전부 세분값이다.

### `-prompts` 데이터셋의 임베딩 패널 (`user-image-embeddings`, 2026-08-14)

플러그인 하나가 패널 **2개**를 등록한다 — `image_embeddings`(프레임 좌표, 크로스 데이터셋
조인) + `sentence_embeddings`(현재 데이터셋의 문장 좌표).

**문장 텍스트 = Postgres 019 정본 (2026-08-19~).** 패널 3종(compare/embeddings/image-embeddings
— 로직은 byte-identical 사본, 한 곳을 고치면 셋 다 고칠 것)은 문장을 데이터셋 `text` 가 아니라
DB 에서 읽는다:
- 조인 = (`bank_version` 정규화, `gidx % 100000`) → `bank_sentences` → `image_embeddings(entity_type='prompt')`
- 데이터셋 `text` 는 npz 파생 **폴백**이고 43.3%가 자리표시자다
- **fail-closed 게이트 3종**(DB 보유 / 행수 일치 / 클래스 일치) — 하나라도 어긋나면 그 버전
  전체가 폴백되고 출처·사유가 배너에 실린다 (조용한 폴백 금지)
- kill-switch: `PROMPT_DB=off` (DB 장애 시 데이터셋 필드로 폴백)
- DSN env 후보: `BANK_DB_DSN` → `DATAOPS_POSTGRES_DSN`(compose 가 주입) → `POSTGRES_DSN` → `DATABASE_URL` `-prompts` compare 워크스페이스는
좌하=문장, 우=이미지다. 좌하를 네이티브 Embeddings 로 두면 brain key 를 매번 손으로 골라야
하거나(비워둘 때) 60만 점 렌더로 Chrome 이 죽는데(지정할 때), 자체 패널은 층화 서브샘플
20,000 점을 **6.4초에 자동으로** 그린다. 화면 총 렌더 점: 610,816 → 27,497.


`<X>-prompts` 의 `emb_viz` 는 **문장 좌표**다 (실측: gidx 603,318 개가 전부 고유, 같은
이미지를 공유하는 22,578 샘플의 좌표 std 9.17 — 이미지 기준이면 0). 그 화면에서 이미지
임베딩을 보려면 프레임 데이터셋(`<X>`) 좌표를 그려야 하는데 네이티브 Embeddings 패널은
크로스 데이터셋을 못 읽고, brain key 를 데이터셋 간에 기억하는 함정까지 있다. 그래서
자체 패널 `image_embeddings` 를 쓴다 — 프레임 좌표를 brain `sample_ids`→`id` 로 조인해
**이미지 단위(7,498 점)** 로 그리고, 색칠 축(정답 클래스·실내외·주야·카메라 …)은 대상
데이터셋 스키마에서 자동 발견한다. `-prompts` 워크스페이스의 좌하 네이티브 Embeddings 는
`brainResult` 를 **비워** 둔다 (문장 60만 점 자동 렌더 = 110초 + Chrome 크래시).

```bash
# 플러그인 파일 복사는 불필요(bind mount). selftest 만 돌린다.
docker exec docker-analysis-1 sh -c \
  'cd /data/fiftyone/datasets/__plugins__/user-image-embeddings && python __init__.py'  # selftest
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py workspace-compare
```
- 의존성(`umap-learn`, `scikit-learn`, `fiftyone-brain`, `psycopg2-binary`)은 이미지에 이미 포함.
  Postgres 접속은 `BANK_DB_DSN`/`DATAOPS_POSTGRES_DSN`/`POSTGRES_DSN`/`DATABASE_URL` 중 먼저
  설정된 값 — compose `x-analysis-env` 가 `DATAOPS_POSTGRES_DSN` 을 이미 주입한다.
  `PROMPT_DB=off` 로 이 경로를 끄면 옛 npz 폴백으로 되돌아간다.

### 쓰는 법

| 하고 싶은 것 | 방법 |
|---|---|
| 새 시각화(brain key) 추가 | Embeddings 툴바의 **Compute visualization (OSS)** 버튼 (또는 백틱 `` ` `` → 오퍼레이터 브라우저) |
| Color by 를 **두 필드 조합**으로 | 툴바의 **Color by 2 fields** 버튼 → 두 필드 선택 → `<a>__x__<b>` StringField 생성 후 Color by 에서 선택 |
| **축 좌표값**을 보고 싶을 때 | 툴바의 **좌표를 필드로 저장** 버튼 → `<key>_x`/`<key>_y` FloatField 생성 |
| 선택 샘플의 **미디어 파일 이동** | 오퍼레이터 `move_media` — ⚠️ **디스크 파일을 실제로 옮긴다** |
| 선택 샘플의 **미디어 파일 삭제** | 오퍼레이터 `delete_media` — ⛔ **디스크에서 영구 삭제, 되돌릴 수 없다** |

⚠️ **`move_media`/`delete_media` 는 파괴적이다.** 샘플만 지우는 것이 아니라 **원본 미디어 파일
자체**를 옮기거나 지운다. 한 번에 `MAX_FILE_OPS = 20_000` 건까지만 허용되고 확인 체크박스가
있지만 되돌리는 기능은 없다 — 뷰 필터를 먼저 확인하고 실행할 것.

로직 검증(자체 selftest): 두 플러그인 모두 컨테이너에서 직접 실행하면 불변식을 검사한다.
```bash
docker exec docker-analysis-1 python /data/fiftyone/datasets/__plugins__/user-embeddings/__init__.py
docker exec docker-analysis-1 python /data/fiftyone/datasets/__plugins__/user-prompt-probe/__init__.py
```

#### 축 눈금이 없는 이유

Embeddings 패널에는 축 표시 토글이 **없다** (Zoom/Pan/Autoscale/Select 만 제공). UMAP·t-SNE
축값은 재실행마다 통째로 바뀌어 해석이 불가능하므로 의도된 설계다 — 의미 있는 건 상대 거리와
군집 구조뿐이다. 좌표가 필요하면 **좌표를 필드로 저장** 버튼으로 뽑으면 사이드바 슬라이더 필터와
Color by 그라디언트로 쓸 수 있다 (`emb_viz_x`/`emb_viz_y` 는 미리 생성해 둠).

축에 의미를 부여하고 싶으면 **PCA** 를 쓰자 (주성분이라 분산 기여도 관점의 해석이 가능).
`emb_viz_pca` 가 이미 있다. 군집이 뭉개져 보이면 축값이 아니라 Color by 를 바꾸는 게 답이다.
노트북에서 축·격자를 완전히 통제하려면 `results.visualize(...).show(xaxis=..., yaxis=...)`
(plotly 백엔드는 `Figure.update_layout()` 인자를 그대로 받는다) 또는 `results.points` 로 직접 그린다.

**Compute visualization (OSS)** 프롬프트는 입력이 4개뿐이고, 실제로 채워야 하는 건
**Brain key 하나**다. 나머지는 이 데이터셋에 맞게 기본값이 들어가 있다:

| 입력 | 기본값 | 비고 |
|---|---|---|
| Brain key | (필수) | 왼쪽 드롭다운에 나타날 이름. 기존 이름이면 경고 후 덮어씀 |
| Embeddings | `embedding` | 숫자 `ListField`/`VectorField` 만 자동 수집 (`tags` 같은 문자열 리스트 제외) |
| Method | UMAP | t-SNE / PCA 선택 가능 |
| 대상 | 전체 데이터셋 | 현재 뷰(필터 적용분)로 전환 가능 |

> ⚠️ **`@voxel51/brain` 원본 프롬프트를 직접 쓰지 말 것** (백틱 → `compute_visualization`).
> 입력이 12개인데 **Embeddings 를 비운 채 Execute 하면 zoo 모델을 받으러 가서 실패하고,
> 내용 없는 brain key 만 등록된다** (`load_brain_results()` → `None`). 실제로 이렇게 생긴
> 빈 run 을 2건 정리했다. 원본을 쓸 거면 Embeddings 에 `embedding` 을 **직접 타이핑**해야
> 한다 — 이 필드는 `ListField` 라 원본의 자동완성 목록(`VectorField` 만 수집)에 안 뜬다.

### 알려진 제약

- **새 brain key·새 필드는 F5 후에 드롭다운에 나타난다.** 완료 후 `reload_dataset` 을
  트리거하지 않기 때문. 자동화를 시도했으나 (`ctx.ops.reload_dataset()`) App 이
  stale ref 로 크래시해서 (`TypeError: reading 'id'`) 뺐다.
- **delegated 실행 금지**: 원본 brain 프롬프트의 Execute 드롭다운에서 "Schedule" 을 고르면
  `fiftyone delegated launch` 워커가 없어 영원히 큐에 남는다. 기본값(즉시 실행) 유지.
- Color by 조합 필드는 **데이터셋 전체**에 쓴다 (필터된 뷰에만 쓰면 나머지가 `none` 이 됨).
  같은 쌍을 다시 실행하면 같은 이름으로 덮어쓴다.

## captions 데이터셋 — 키프레임 백필 (2026-07-28)

`captions` 는 캡션 1건당 샘플 1개이고, 이미지는 **그 영상의 대표 키프레임**을 쓴다.
그런데 키프레임 출처가 `image_metadata`(추출된 프레임)라서, 프레임 추출 대상(102,074 asset)과
Gemini 캡션 대상(4,235 asset)이 거의 겹치지 않아(교집합 481) **11,535/11,978(96.3%)** 이
320×240 짙은 회색 플레이스홀더였다 (`fiftyone_pgvector.py:576`). 실제 사진은 443건이었고
그마저 원본 영상 **11개**에서 나온 것이었다.

`backfill_caption_keyframes.py` 가 원본 영상에서 프레임 1장씩 뽑아 채운다.

```bash
docker exec -d docker-analysis-1 sh -c \
  'cd /workspace && CKF_BATCH=200 CKF_WORKERS=3 python backfill_caption_keyframes.py \
   > /data/fiftyone/ckf.log 2>&1'
```

- `/nas` 가 analysis 컨테이너에 마운트돼 있지 않으므로 **MinIO presigned URL 을 ffmpeg 에
  직접** 물린다 — HTTP range 로 앞부분만 읽어 영상 전체를 안 받는다 (실측 0.3s / 123KB).
- asset 별 1회 추출 → 그 asset 의 모든 캡션 filepath 로 복사. **같은 경로에 덮어쓰지 않고
  `_kf.jpg` 새 경로**로 쓴다 (덮어쓰면 브라우저가 옛 플레이스홀더를 캐시해 그대로 보인다).
- `metadata` 재계산 필수 — 플레이스홀더가 320×240 이었으므로 안 하면 종횡비가 깨진다.
- 실행 결과 (2026-07-28): asset 4,219개 성공 / **실패 0** / 캡션 11,489건 갱신 /
  **남은 플레이스홀더 0** / 5.4분 / 메모리 0.26GB · CPU 0.11코어.

### ⚠️ 이건 "보이게" 만든 것이고 "측정 가능하게"까지는 아니다

`caption_img_sim`(캡션↔이미지 cosine)은 여전히 **330건**만 채워져 있다.
`fetch_caption_image_sim()` 은 pgvector 의 **frame 임베딩**을 읽는데, ffmpeg 로 뽑은
키프레임은 `image_embeddings` 에 없기 때문이다. 커버리지를 늘리려면 추출한 키프레임을
embedding-service 로 임베딩해 `captions` 에 이미지 임베딩 필드를 추가해야 한다.

### 필드 이름 함정 — `embedding` 이 데이터셋마다 다르다

| 데이터셋 | `embedding` | `emb_viz` 의 의미 |
|---|---|---|
| `captions` (빌드 중간산출 — 현재 미실존) | **캡션 텍스트** 임베딩 (`entity_type='caption'`) | 텍스트 공간 지도 |
| `frames_full` (빌드 중간산출 — 현재 미실존) | **이미지** 임베딩 (`entity_type='frame'`) | 이미지 공간 지도 |

> ⚠️ 2026-08-19 개명 전에는 이미지 전용 `frames` 데이터셋도 있었다. 지금 `frames` 는
> **아래 통합 데이터셋**(구 `frames_captions`)을 가리킨다 — 옛 문서/스크립트에서 'frames' 를
> 보면 어느 쪽인지 날짜로 구분할 것.

검증법(둘 다 PE-Core-L14-336 1024-d 공유 공간): 저장된 벡터와 그 샘플 caption 을
`fp._embed_text()` 로 재임베딩해 cosine 을 보면 `captions` 는 **1.0000**,
`frames_full` 은 **0.158** 이다. 이름만 같고 모달리티가 다르므로 혼동 주의.

## frames — 이미지+캡션 통합 데이터셋 (2026-07-28 빌드 / 2026-08-19 `frames_captions` 에서 개명)

`frames_full`(이미지 187,994) + `captions`(텍스트 11,978) = **199,972 샘플**을 PE-Core 공유
1024-d 공간에 union. `modality` 필드(`frame`/`caption`)로 구분한다.

```bash
docker exec -d docker-analysis-1 sh -c 'cd /workspace && python merge_frames_captions.py   > /data/fiftyone/mfc.log 2>&1'
docker exec -d docker-analysis-1 sh -c 'cd /workspace && python enrich_frames_captions.py  > /data/fiftyone/efc.log 2>&1'
docker exec -d docker-analysis-1 sh -c 'cd /workspace && RCE_TR_WORKERS=3 python reembed_captions_en.py > /data/fiftyone/rce.log 2>&1'
```

### 왜 union 인가 — 다른 두 해석은 데이터가 죽인다

1. 프레임에 캡션 임베딩 붙이기 → **캡션 있는 프레임 264/187,994 (0.1%)**. 쌍이 없다.
2. 캡션 키프레임을 프레임 샘플로 추가 → ffmpeg 추출본은 `image_embeddings` 에 없어 벡터 부재.
3. **두 모달리티 union** ← 유일하게 가능. 복제는 `src.clone()` **서버사이드**(188K 12초,
   파이썬 왕복 금지). `points=` 정렬은 `values("id")` 순서로 배치를 만들어 보장.

### 필드 (PRIMITIVES)

| 필드 | 내용 | 커버리지 |
|---|---|---|
| `embedding` | 모달리티 native (프레임=이미지, 캡션=텍스트) — UMAP 입력 | 199,972 |
| `image_embedding` | 이미지 벡터. 캡션 샘플은 키프레임을 `/embed` 로 신규 임베딩 | 200,232 |
| `caption_embedding` | **영어 기준** 캡션 벡터. 프레임은 자기 영상 캡션 centroid | 캡션 전체 + 프레임 264 |
| `caption_embedding_ko` | 기존 한국어 벡터 (A/B 비교용 보존) | 11,978 |
| `caption_en` | Gemini 번역문 (표시는 여전히 `caption`=한국어) | 11,978 |
| `caption_img_sim` | 위 두 벡터 cosine. **330 → 12,242건** | 12,242 |

### ⚠️ 캡션 임베딩은 영어 기준이어야 한다

의미가 다른 4주제(낙상/화재연기/통상통행/신호위반) 캡션의 **판별격차**(같은주제 cos − 다른주제 cos):

| | 같은 주제 | 다른 주제 | 판별격차 |
|---|---|---|---|
| 한국어 | 0.9567 | 0.9494 | **+0.0073** (노이즈) |
| 영어 | 0.8536 | 0.7699 | **+0.0837** (11.5배) |

PE-Core 텍스트 타워가 한국어를 못 읽는다. 한국어 벡터로는 "사람이 쓰러짐"과 "오토바이가
지나감"을 구분할 수 없다. 전역으로도 한국어 캡션 effective rank **1.5/1024**, 무관 캡션끼리
pairwise cos 0.951. **절대 cosine 수준이 아니라 격차를 봐야 한다.**

### ⚠️ 번역 함정 — `translate_query_ko_en()` 을 배치에 쓰지 말 것

이 함수는 Vertex 호출 실패 시 **조용히 `_dict_substitute()`(사전 단어치환)로 폴백**한다.
그 결과 `"3명의 보행자가 횡단보도를 건너는 모습"` → `"3명의 pedestrian 가 crosswalk 를
건너는 모습"` 같은 반쪽 번역이 성공처럼 캐시에 저장된다. **실측 19.9%** 가 이렇게 오염됐고,
그대로 임베딩하면 한국어 붕괴를 물려받아 작업 전체가 무의미해진다.

`reembed_captions_en.py` 는 `fp._vertex_translate()` 를 **직접** 호출해 폴백을 우회하고,
**한글이 남은 출력을 실패로 간주**해 최대 3회 재시도(백오프)한다. 병렬도는 3 (rate limit
실패 자체를 줄이는 게 재시도보다 낫다). 수정 후 캐시 한글 잔존 **0건**.

번역·임베딩은 **고유 문장 단위**로 1회만 한다 (11,978건 → 고유 6,999건, 중복 42%).
디스크 캐시(`_caption_en.json`, `_en_vectors/*.npy`)로 중단 후 재개 가능.

## FiftyOne 플러그인 — 프롬프트 프로브 (`user-prompt-probe`)

오퍼레이터 `probe_prompt`. 문장 하나를 넣고 **그 문장이 실제로 어떤 프레임을 끌어오는지**
즉석에서 보는 도구. 배경 코사인(`bg_cos`)이 함께 나오는데, 이게 높으면 그 문장이 클래스가
아니라 **배경을 읽고 있다는 신호**("배경 자석")다.

- 설치 절차 없음 — compose bind mount (위 설치 절 참조).
- ⚠️ **선행 조건**: `probecache` 스테이지가 만든 `probe_bank_*`/`probe_bar_*` dataset.info·필드가
  없으면 "probe 캐시가 없습니다" 로 거부한다. 먼저 아래를 돌릴 것.

```bash
docker exec docker-analysis-1 nice -n 10 python /workspace/prompt_geometry.py probecache
```

### 오퍼레이터 5개 — 어느 데이터셋에서 어떤 버튼이 뜨나

같은 플러그인에 5개가 들어 있다. 번호(①~⑤)는 버튼 라벨에 박힌 번호다 — 아래 결과 화면의
「① 삭제 후보」 같은 번호와는 별개다.

⚠️ **스키마로 버튼을 거르는 건 ③·④ 뿐이다.** `resolve_placement` 가 `text` 필드 유무로
문장/프레임 데이터셋을 가른다. ①②⑤ 는 무게이트라 **어느 데이터셋 그리드에나 뜨고**, 전제(probe
캐시·`wave_iou_*` 필드)는 폼(`resolve_input`)이 열린 뒤에 검사해 사유를 보여준다 — 버튼이 떴다고
그 데이터셋에서 돈다는 뜻이 아니다. 왜 무게이트인가: ①에 게이트를 두면 캐시 없는 데이터셋에서
버튼 자체가 안 떠 안내문에 닿을 길이 없었고, ⑤ 의 `wave_iou_*` 확인은 `resolve_placement` 에서
하기엔 무겁고 위험하다 — 거기서 예외가 나면 배치 응답이 통째로 실패해 **모든 플러그인 버튼이
함께 사라진다**.

| 오퍼레이터 | 버튼 | 뜨는 곳 | 하는 일 |
|---|---|---|---|
| `generate_prompts` | ① 문장 생성 — 오탐/미검출 진단 + 초안 | 어디서나 (폼이 `probe_*` 캐시 요구 — 없으면 `probecache` 명령 안내) | 오탐/미검출 코호트 → LLM 후보 문장 → **같은 채점부로 즉시 채점** |
| `probe_prompt` | ② 프롬프트 프로브 — 내가 쓴 문장 채점 | 〃 | 손으로 쓴 문장 즉시 채점 |
| `export_bank_version` | ③ 뱅크 버전 만들기 — 선택한 문장 → CSV 내보내기 | 문장 데이터셋 (`<ds>-prompts`) | 선택/뷰/태그 → `authored_<ver>.csv` + provenance + 019 원장 |
| `explain_frames` | ④ 이 프레임에 뭐가 찍혔나 — PLM 서술 | 프레임 데이터셋 | 선택 프레임 또는 현재 뷰 앞에서부터(기본 최대 10장)를 PLM 에 보여 `plm_saw` 문자열 필드에 서술을 **써 넣는다**. 기본은 빈 것만 채운다(이어서 돌리기) |
| `explain_rule` | ⑤ 판정규칙 실시간 조절 — thr·디바운스 | 어디서나 | top-k(k, 기본 10) / 분포 IoU(thr 기본 0.15 · 디바운스 5중3) 파라미터를 폼에서 바꾸면 TP/FP/FN·정밀도·재현율·**N×N 혼동행렬**이 즉시 다시 계산된다 |

- **④ 는 GPU 를 쓴다.** PLM 은 embedding-service 의 `/caption`(호스트 GPU1)이고 장당 ≈3초 —
  20장이면 1분 동안 오퍼레이터가 응답하지 않는다. GPU1 은 SAM3 서빙 우선이라 여유가 없으면
  503 에서 멈추고 **그때까지 받은 것만 저장**한다. ⚠️ PLM 비활성(`PLM_ENABLED`)·GPU 정비 중도
  같은 503 이라 화면 문구는 전부 「GPU 양보」다 — 다시 돌려도 안 풀리면 그쪽을 의심할 것.
- **⑤ 의 두 규칙은 계산 경로가 다르다.** `dist_iou` 는 `wave_iou_*` 필드가 있고 현재 데이터와
  맞으면 그 필드, 없거나 어긋나면 온디맨드로 계산한다. `top-k` 는 순위표가 필요해 **항상**
  온디맨드(sourcei 기준 첫 계산 14~19초, 캐시 히트면 즉시)이고 디바운스가 없다. 표시순서·기본
  선택 = top-k(2026-09-22 지시, `e13e5a3`/`8c8f408`)라 캐시 미스면 모달을 열 때마다 이 비용이 든다.
- ⑤ 를 문장 데이터셋(`<ds>-prompts`)에서 열면 분포 IoU 는 베이스 프레임 데이터셋(`wave_iou_*`
  보유 시) **전량**으로 채점한다 — 사이드바 필터는 반영되지 않는다(화면에 고지).
- **미채점(gt<0) 프레임은 TP/FN/GT 열에서 빠지지만, 이벤트로 예측되면 FP 로는 잡힌다** — 어디로
  예측됐는지는 혼동행렬의 `(미채점)` 행이 보여준다.

> ⚠️ **`explain_rule` 은 아래 「판정규칙 3벌」 절의 배치 스테이지(`vote`/`wave`)와 다른 layer 다.**
> 배치는 결과를 프레임 필드(`vote_*`·`wave_iou_*` 등)와 npz/json 산출물로 남기는 오프라인 채점이고,
> 이 오퍼레이터는 같은 두 규칙을 화면에서 즉석 재계산할 뿐 그 필드를 고쳐 쓰지 않는다. 단
> **「결과를 뷰로 저장」(기본 꺼짐)을 켜면 데이터셋에 쓴다** — 판정≠GT 프레임을 저장 뷰(기본값이면
> `thr015_w5m3_<태그>` / `topk_k10_<뱅크>`)로 남기고, 같은 이름이 있으면 지우고 새로 만든다(좌석
> 공유 주의). 화면의 "제품 판정규칙"·"다수결" 수식어는 2026-09-22 사용자 지시로 뺐지만
> (`8ca05e2`/`e13e5a3`) 사실은 유효하다 — 제품 판정규칙은 분포 IoU 이고, top-k 숫자를 제품 성능으로
> 인용하지 말 것(아래 `generate_prompts` 경고의 −0.07 과 같은 이유).

`generate_prompts` 는 **처방이 반대인 두 모드를 분리**한다. 섞으면 "오탐 고치려고 이벤트 문장을
추가"하는 정반대 동작이 나오므로 선언 클래스를 모드가 고정한다.

- **오탐 줄이기(FP)** — 대상 = `GT normal` 인데 이벤트로 오판된 프레임. 선언 클래스 `normal` 고정.
- **미검출 줄이기(FN)** — 대상 = `GT 이벤트` 인데 normal 로 놓친 프레임. 선언 = 그 이벤트.

결과 화면의 **① 삭제 후보를 먼저 보라.** 코호트를 이기고 있는 문장을 점유율 순으로 세어 주는데,
한 문장이 수백 장을 독식하는 경우가 실측된 지배 패턴이고 그때 정답은 문장 추가가 아니라 그 문장
삭제다(개선 실측의 98.5%가 "나쁜 자석 제거" 기여).

> ⚠️ **채점 숫자는 top-k 규칙이다.** 제품 판정규칙(분포 IoU)과 상관이 **−0.07** 로 측정됐고,
> 선례(사람이 오탐 프레임을 보고 쓴 5문장, `sourceh_v2/work/banks/v1.0.8.0+night5/`)는 top-k
> **+3.53pp** 인데 제품 규칙에서는 **+0.046pp** 였다. 그래서 이 오퍼레이터는 **아무것도 저장하지
> 않는다** — 채택은 문장을 뱅크 CSV 로 넣고 `wave` 로 재채점한 뒤에 판단한다.

LLM 백엔드는 3종이다(`GEN_BACKENDS = ("vertex", "openai_compat", "plm")`). 새 설정 파일·compose
변경 없음.

| 백엔드 | 필요한 것 | 비고 |
|---|---|---|
| `vertex` (기본) | 없음 — `google-genai` 는 이미지에 baked, creds 는 `/app/credentials` ro 바인드, `GEMINI_PROJECT`/`GEMINI_LOCATION` env 상주 | 실측 이미지 6~8장 ≈ 3s (`thinking_budget=0` 강제) |
| `openai_compat` | env `PROMPT_GEN_BASE_URL` (+선택 `PROMPT_GEN_API_KEY`) | 로컬 vLLM/Ollama·외부 OpenAI 호환 API 공통. ⚠️ 이 호스트 가용 RAM 이 낮아 로컬 모델 상주는 비권장 — 코드 경로만 열려 있다 |
| `plm` | 없음 — embedding-service 의 `/caption`(로컬 PLM, PE-Lang 비전타워+Llama, GPU1) | 이미지 없이는 호출 불가. 모델은 서비스 `PLM_MODEL_ID` 가 정하므로 여기 모델명 인자는 무시된다. GPU1 은 SAM3 서빙이 우선이라 여유가 없으면 이 백엔드가 양보한다 — ④ `explain_frames` 와 GPU 를 공유 |

모델명은 오퍼레이터 드롭다운(`PROMPT_GEN_MODEL`, 기본 `gemini-2.5-flash`)에서 바꾼다.

### ③ 뱅크 버전 만들기 — 대상 고르는 5가지 방법

| 대상 | 쓰는 때 |
|---|---|
| **프로젝트 성능 상위 N개** (선택이 없을 때 기본) | "이 현장에서 잘 잡히는 문장만 모아 뱅크를 만들고 싶다" — 아래 참고 |
| 선택한 문장 | 그리드 체크박스·라쏘로 직접 고른 것 |
| 선택한 문장을 **뺀** 나머지 (`DROP`, 삭제본) | 원본 뱅크 버전 하나를 고르고 그 안에서 선택 문장만 뺀 사본을 만든다. 위 `generate_prompts` 결과의 「① 삭제 후보」가 가리킨 나쁜 자석을 **문장 데이터셋에서** 골라 지우는 실행 경로다 — 지울 문장(수백 개 규모)은 선택 상한 안에 들어오지만, 남길 문장을 고르는 쪽으로 뒤집으면 뱅크 대부분을 골라야 해서 상한 안에 표현이 안 되기 때문에 따로 둔 모드다. 지워지는 문장이 0개(선택이 다른 버전의 행)거나 전부 지워지면 거부한다. ⚠️ top-k 규칙에서는 클래스별 문장 수가 곧 사전확률이라, 한 클래스만 몰아 지우면 그 클래스를 통째로 못 잡게 될 수 있다 — 결과의 클래스별 개수(원본→삭제 후)를 확인하라 |
| 현재 뷰 전체 | 사이드바 필터로 좁힌 것 전부 |
| 이미 붙여둔 태그 | `bank:*` 태그를 미리 달아둔 경우 |

> ⚠️ `선택한 문장`·`삭제본` 은 **선택이 있을 때만** 뜨고, 선택이 있으면 기본값이 `선택한 문장` 으로
> 바뀐다 — 지울 문장을 라쏘한 뒤 대상을 `삭제본` 으로 바꾸지 않으면 **지우려던 문장만 담긴 뱅크**가
> 발행된다. 선택이 500개(`SELECTION_CAP`) 이상이면 두 모드 모두 거부된다: 패널이 라쏘를 그 상한에서
> 잘라 보내므로, 잘린 선택이 조용히 작은 뱅크·부분 삭제본이 되는 것을 막는 가드다(정확히 500개를 고른
> 정당한 선택도 막힌다). 그때는 라쏘를 나누거나, 사이드바 필터로 남길 집합을 만들어 「현재 뷰 전체」로
> 발행한다.

**프로젝트 성능 상위 N개**는 프레임 데이터셋의 `winner_gidx_<tag>`(프레임마다 이긴 문장) + `ground_truth`
를 **선택한 카메라로 자른 뒤에만** 집계해 순위를 만든다.

| 입력 | 뜻 |
|---|---|
| 원본 뱅크 버전 | 어느 뱅크의 문장 풀에서 고를지 |
| 프로젝트(카메라) | 이 프레임들에서만 성능을 집계. `전체` 가능 |
| 문장 개수 / 클래스별로 N개 | 켜두면 **클래스마다** N개 (한 클래스 독식 방지) |
| 최소 승수 | 이 프로젝트에서 최소 몇 장을 이겨야 후보로 볼지 |
| 정렬 기준 | 정확도 / **순이득(맞춘−틀린, 기본)** / 승수 |
| **미리보기만** (기본 ON) | 고른 문장 표만 보여주고 **아무것도 저장하지 않는다.** 확인 후 끄고 재실행 |

#### 채택 근거 수치 — 어느 값을 보고 고르나

결과 표에 이미지↔문장 수치가 함께 나온다. **프레임 필드만으로** 계산한다 (문장이 그 프레임을
top-1 로 이겼다면 그 프레임의 `cos_best_<그 클래스>` 가 곧 그 문장의 코사인이므로 임베딩 재계산 불필요).

| 컬럼 | 뜻 | 읽는 법 |
|---|---|---|
| `이긴 프레임` | 이 프로젝트에서 top-1 로 가져간 장수 | 분모다. 3장 미만은 통계가 아니다 |
| `정답 비율` | 이긴 프레임 중 GT 가 그 클래스인 비율 | **1순위 필터.** 0.9 미만은 자석이 새는 것 |
| `순이득` | 맞춘 − 틀린 (= 2×정답 − 승수) | 정확도와 분량을 한 축으로 본 값. 기본 정렬 |
| `코사인` | 이긴 프레임들에서의 평균 코사인 | 절대값 자체는 의미 약함(0.25~0.32 대역). **높은데 정답 비율이 낮으면 배경 자석** |
| `마진` | 같은 프레임에서 (자기 클래스 최고 − 2등 클래스 최고) | **가장 중요.** 판정을 뒤집은 여유폭. 실측 승리 마진 중앙값 ≈ **0.01** → 0.01 미만은 우연에 가깝고 뱅크가 조금 바뀌면 뒤집힌다 |
| `제품규칙 IoU` | 그 프레임들의 분포-IoU 평균 (낮을수록 탐지) | 제품 임계 기본 0.15. ⚠️ **프레임의 성질**이라 문장 개별 인과가 아니다. `normal` 은 정의상 값이 없다(빈칸 정상) |

**권장 채택 순서**: ① 정답 비율 ≥ 0.9 로 걸러라 → ② 그중 **마진 ≥ 0.02** 를 우선(0.01 근처는 보류)
→ ③ 이긴 프레임 ≥ 3 인지 확인 → ④ 이벤트 클래스는 제품규칙 IoU 가 임계(0.15)보다 낮은 쪽을 선호
→ ⑤ 코사인이 유독 높은데 정답 비율이 낮은 행은 **버려라**(배경 자석).

> ⚠️ **클래스 커버리지를 반드시 보라.** 카메라를 좁히면 그 현장에 없는 이벤트의 문장은 승수 0 으로
> 전부 걸러져 **fire/smoke 문장이 하나도 없는 뱅크**가 조용히 만들어진다 (실측: 상가 복도 카메라에서
> normal 만 선정). 결과의 「클래스 커버리지 점검」이 빠진 클래스를 이름으로 알려준다 — 그때는
> 프로젝트를 `전체` 로 하거나 최소 승수를 0 으로 낮춘다.
> ⚠️ 이 순위는 **그 프로젝트의 GT** 로 만든 값이라 그 현장에 적합(overfit)된 선택이다. 선정 조건
> (`rank: bank=… camera=… top_n=… sort_by=… missing_classes=…`)이 provenance 에 자동 기록된다.
> ⚠️ `winner_gidx_*` 는 구 표기(`v084`)와 신 표기(`v1084`)가 섞여 있고 gidx 오프셋 세대도 다르다 —
> 필드명은 둘 다 탐색하고, 조인은 `gidx % GIDX_OFFSET` 으로 맞춘다(등식 조인은 조용히 0건이 된다).
> ⚠️ 뱅크 문장 수가 `GIDX_OFFSET`(=100,000)을 넘으면 gidx 블록이 겹쳐 **다른 버전 문장으로의
> 조용한 오귀속**이 되므로, 2026-08-19부터 로더가 그 자리에서 `SystemExit` 로 죽는다
> (정확히 100,000행은 합법). 근본 해결은 OFFSET 증설 + 전량 재백필로 별건.

### 큐레이션 한 바퀴 (App → CSV → 벡터)

```bash
# 0) 선행: 문장 데이터셋 + probe 캐시
docker exec docker-analysis-1 nice -n 10 python /workspace/prompt_geometry.py promptmap
docker exec docker-analysis-1 nice -n 10 python /workspace/prompt_geometry.py probecache

# 1) App(:5153) 에서 `<ds>-prompts` 를 열고 라쏘/사이드바로 문장을 고른 뒤
#    [뱅크 버전 만들기] → 버전명 입력.  선택분에 `bank:<버전>` 태그가 자동으로 붙어 provenance 가 된다.
#    CSV 는 그 즉시 호스트에 나타난다: docker/data/fiftyone/sourceh/prompts/authored_<버전>.csv

# 2) 벡터화 (문장 수 × 7.5ms — App 을 막지 않도록 분리)
docker exec docker-analysis-1 python /workspace/prompt_geometry.py \
  bank --csv /data/fiftyone/sourceh/prompts/authored_<버전>.csv --version <버전>

# 3) 제품 규칙으로 재채점 (여기서 판단한다)
BANK_LIST=v1.0.8.0,<버전> docker exec docker-analysis-1 \
  python /workspace/prompt_geometry.py wave
```

터미널 없이 CLI 로만 1)을 하려면 App 에서 태그만 붙이고 아래를 쓴다.

```bash
docker exec docker-analysis-1 python /workspace/prompt_geometry.py \
  bankfrom --tag bank:<버전> --version <버전> --notes "왜 만들었는지"
```

> ⚠️ 파일명은 반드시 `authored_*` 다. `text_features_*` 로 쓰면 `prompt_bank_ledger.py` 의
> `VERSION_RE` 에 걸려 우리가 만든 뱅크가 **외부 공급 뱅크로 위장 등록**된다.
> 산출물은 컨테이너 root 소유라 호스트에서 지울 때는 `docker exec … rm` 이 필요하다.

#### 어느 데이터셋에서 지금 쓸 수 있나 (2026-08-12 실측)

| 데이터셋 | 버튼 | 상태 |
|---|---|---|
| `sourcei` (7,498) | 프로브 · 문장 생성 | ✅ probe 캐시 `v080` + `top_prompt_v1_0_8_0` + GT 4클래스. FP 는 적고(fire 6/smoke 0/falldown 27) **FN 이 많다**(smoke 1,117 · falldown 897 · fire 131) → 「미검출 줄이기」로 쓰는 데이터셋 |
| `sourcei-prompts` (603,318) | 뱅크 버전 만들기 | ⚠️ **자리표시자 261,244행(43.3%)** — 2026-08-11 재빌드가 27버전 문장을 `(텍스트 없음 #N)` 으로 덮었다. 2026-08-19부터 문장 텍스트 정본은 Postgres `bank_sentences` 이고 패널이 거기서 읽는다 — fail-closed 게이트 통과 21버전은 실문장, 거부 8버전은 자리표시자 폴백. **큐레이션은 게이트 통과 버전에서만 온전** |
| `frames` (199,972 = 프레임 187,994 + 캡션 11,978) | 뱅크 채점 · 캡션 연동 | ⚠️ **GT 40행(전부 normal)** = 대부분 도메인 `tier=no_gt`. 21도메인 채점·리뷰큐 대상. `daynight`/`environment` 는 distinct 1(죽은 축) — 다른 데이터셋의 필터 목록을 복사하지 말 것 |
| ~~`source-h`~~ / ~~`source-h-prompts`~~ | – | ⚠️ **2026-08-18 사용자 요청으로 데이터셋 자체가 삭제됨** (`bank_health.sh` 커밋 참조). GT 원장은 `sourceh_v2/work/ledger.jsonl` 에 잔존 — **재생성 금지.** 현재 유효 목록은 `bank_health.sh` 의 `DATASETS` 가 정본 |

> ⚠️ **`resolve_placement` 는 절대 예외를 던지면 안 된다.** `ctx.dataset` 이 `None` 인 시점에도
> 호출되는데, 예외가 나면 FiftyOne 이 `ExecutionResult(error=...)` 를 배치 배열에 실어 보내고
> (`operators/executor.py:635` + `operators/server.py:85` 의 `is not None` 통과) 프론트가 그 배열을
> 렌더하다 죽어 **툴바의 모든 플러그인 버튼이 함께 사라진다**. 2026-08-12 에 실제로 발생했다.
> 배치 판정은 `_has_field()` / `_probe_tags_safe()` 로만 하고, self-check 가 이 회귀를 잡는다.
> 서버 응답으로 직접 확인하려면:
> ```bash
> docker exec docker-analysis-1 sh -c 'curl -s -X POST http://localhost:5151/operators/resolve-placements \
>   -H "Content-Type: application/json" -d "{\"dataset_name\":\"sourcei\"}"' | head -c 400
> ```

## 프로젝트 업로드 킷 (`project_upload/`, 2026-09)

외부 작업자가 이미지 + 이미지/프롬프트 임베딩(+선택 GT)을 표준 번들(디렉토리, 또는 그것을 묶은
zip)로 올리면 sourcei 와 같은 FiftyOne 뷰(이미지 스캐터+문장 스캐터+버전선택+성능필드)를 프로젝트
단위로 만들어 주는 킷. 번들 구조 스펙 정본은 `docker/analysis/project_upload/UPLOAD_SPEC.md` — 이 절은 요약만.

| 반입 경로 | 어디서 | 상한 |
|---|---|---|
| ⓪ FiftyOne 앱 안 (그리드 툴바 `프로젝트 번들 임포트` 창) | 좌석 프로세스를 통과(base64, 파일 크기의 ≈3.7배 순간 점유) | `APP_MODAL_UPLOAD_MAX_MB` 기본 512MB |
| ① 브라우저 업로드 페이지 `/__upload/ui` (예 `http://<host>:5153/__upload/ui`) | 좌석 라우팅 **밖**, raw 스트리밍 | `UPLOAD_MAX_BYTES` 기본 20GiB |
| ② 네트워크 드라이브 / `docker cp` | Samba `[user]` 공유(user 계정, `\\<host>\user\work_p\Datapipeline-Data-data_pipeline\docker\data\fiftyone\uploads\<이름>`) 또는 `docker cp <dir> docker-analysis-1:/data/fiftyone/uploads/<이름>` | 전송 상한 없음(초대형용) |
| ③ URL 가져오기 `POST /upload/fetch` | 서버가 직접 다운로드, 좌석 안 거침 | 20GiB, SSRF 방어(사내 CIDR·포트 allowlist만 통과, 리다이렉트 홉마다 재검증) |

- ⚠️ **⓪ 상한의 정본은 플러그인 상수**(`plugins/user-embeddings/__init__.py` 의 `_MODAL_UPLOAD_MAX_MB`)다.
  `UPLOAD_SPEC.md` §1 과 `sync_api.py` 주석에 남은 "256MB / ≈3.4배" 는 614c26c(256→512) 이전 값이다.
  올리려면 nginx 좌석 경로 바디 상한(`seat-tls/routing.inc` 의 `client_max_body_size 1g`)이 이 값 ×1.34
  이상이어야 하고(nginx 만 올려서는 모달 상한이 안 오른다), 614c26c 는 1GB 를 좌석 OOM 으로 판단해
  올리지 않았다 — 그보다 큰 번들은 ①로.

엔드포인트는 전부 `analysis-sync`(`sync_api.py`) 담당: `/upload/validate` `/upload/ingest`
`/upload/bundles` `/upload/delete` `PUT /upload/archive` `/upload/fetch` `/upload/job` `/upload/ui`.
analysis-sync 자체는 호스트 포트가 없지만 nginx 가 `/__upload/*` 를 `/upload/*` 로 통째로 넘기므로
**이 8개는 전부 :5153 에서 열려 있고, 현재 전부 무인증이다.** `FIFTYONE_SYNC_TOKEN`(X-Internal-Token)은
validate/ingest/bundles/delete 에만 걸리는 옵션인데 compose 의 analysis 환경(`x-analysis-env`)에 키가 없어
꺼져 있고, archive/fetch/job/ui 는 코드상 토큰 검사 자체가 없다 — 그래서 `/upload/fetch` 는 주소 정책만으로
SSRF 를 막는다. ⚠️ 토큰을 켜도 archive/fetch 는 열린 채이고, `upload_ui.html` 은 토큰을 보내지 않아
브라우저 페이지의 목록·임포트·삭제만 401 로 깨진다.

- **삭제는 번들+데이터셋을 한 쌍으로** — 이미지를 제자리 참조하므로 번들만 지우면 썸네일이 전부 깨진
  데이터셋이 남는다. `/__upload/ui` 목록 체크박스 → `선택 삭제`, 또는
  `docker exec docker-analysis-1 python3 /workspace/project_upload/delete_bundle.py <이름>... --apply`
  (기본 dry-run). 대상은 이름이 아니라 marker(`ds.info["upload_kit"]["bundle"]`)로 골라 sourcei/frames
  같은 동명 데이터셋은 안 건드린다. **Postgres 에 등록한 프롬프트 뱅크(`register_bank_db.py`)는
  전역 레지스트리라 삭제되지 않고 남는다.**
- **`register_bank_db.py`**(`/workspace` 루트 — `project_upload/` 안이 아니다): 킷은 문장 벡터를 FiftyOne
  `sentence_embedding` 에만 넣으므로, 인제스트만으로는 compare 패널의 `cos` 열이 전부 `-`(PG 미등록) 다.
  이 스크립트로 Postgres 019 스키마(`prompt_banks`/`bank_sentences`, 없는 문장 벡터는
  `image_embeddings(entity_type='prompt')`)에 수동 등록해야 채워진다. 등록하면 그 뱅크가 모든 데이터셋의
  top-k 표에 한 줄로 늘어나는 전역 결정이라 킷이 자동으로 부르지 않는다(기본 dry-run, `--apply`).
- `sync_api.py` 는 `uvicorn --reload` 없이 뜬다 — `/upload/*` 핸들러를 고치면
  `docker restart docker-analysis-sync-1` 이 필요하다(`upload_ui.html` 은 요청마다 새로 읽어
  반영됨).
- `/nas/data/incoming` 에는 **절대 넣지 않는다** — auto-bootstrap 이 CCTV 원본으로 오인 수집한다.

## 스크립트 지도 — 어느 데이터셋이 어디서 나오는가

README 본문은 여러 데이터셋을 전제로 설명하는데, 그것들을 **만드는** 스크립트가 정리돼 있지
않으면 재현이 불가능하다. 진입점은 다음 9개다.

| 스크립트 | 만드는 것 | 스테이지 | 비고 |
|---|---|---|---|
| `prompt_eval.py` | 영상 단위 데이터셋 (871편) | `prompts` `media` `angle` `embed` `score` `dbwrite` `build` `report` `all` | 각 스테이지 멱등, 중단 후 재실행 가능 |
| `frames_eval.py` | 프레임 단위 재라벨 데이터셋 | `scan` `copy` `angle` `embed` `score` `build` `report` `all` | `--limit` 지원 |
| `bank_eval.sh` | 뱅크 **버전 비교** 원커맨드 | `analyze`→`gap`→`flips`→`prune`→`atlas`→`viz`→`guide`→`slim`→`report` | **순서 고정** — 앞 단계 산출을 뒤가 읽는다 |
| `ablate_fields.py` | 절/구/단어 절제 측정 | – | env `AB_PROFILE` `AB_TOPN` `AB_WORDS` `AB_RETRY`. `user-prompt-probe` 와 지표 정의를 공유 |
| `frames_bank_eval.sh` | 21도메인 뱅크 채점 사이클 | `ledger`→`gtsync`→`score`→`report` | 상세는 아래 「frames 프롬프트 뱅크 평가」 절 |
| `caption_prompt_link.py` | 캡션 11,978 ↔ 뱅크 문장 양방향 연동 | `link`(최근접 top1~3 + `cap_prompt_gidx_r1`) `enrich-prompts`(`-prompts` 에 캡션 편입 + `emb_viz_cap`) | 기본 dry-run, `--apply`. 벡터 정본 = 데이터셋 `caption_embedding`(영어) — pgvector `entity_type='caption'` 은 한국어 붕괴본이라 폴백 시 경고+기록 |
| `prompt_bank_load.py` | 뱅크 원장 → Postgres 019 적재 + 문장 벡터 흡수 | `load` `embed` `verify` | **패널 문장 정본의 적재·검증 경로**(88a3c5c~). `verify` = 적재 정합 4종 + 벡터 귀속 감사(문장 지문↔벡터 해시 1:1), 위반은 fail-soft(리포트만) |
| `prompt_cos_db.py` | 뱅크 버전 코사인 채점·규칙 비교 (`analysis.*` PG 원장) | `plan` `score` `wave` `topk` `affinity` `cluster` `phrase` `ridge` `cooc` `batch-*` `report` `notion` `selftest` | ⚠️ **일회성 아님** — 다른 분석 스크립트 36개가 이 파일을 import 한다. 래퍼 2종이 호스트 crontab 에 상시 등록돼 있다: `prompt_cos_cron.sh`(매일 02:40, 컨테이너에서 score→report→notion), `prompt_cos_batch.sh`(15분 간격, 호스트 anaconda — 벡터전용 뱅크 원본이 컨테이너에 미마운트). **둘 다 `flock` + 루트 디스크 가드**(`MIN_FREE_GB` 기본 8, 미달이면 스스로 중단 — 이 디스크에 PG 데이터가 있어 ENOSPC 가 프로덕션을 멎게 한다) |
| `prompts_ws_setup.py` | `<X>-prompts` 에 `compare` 워크스페이스를 살아있는 원본에서 미러 | – | `workspace-compare`(코드 재생성)와 목적이 다름 — 이쪽은 원본 복제. 멱등 |

- `bank_eval.sh` 사용례: `./docker/analysis/bank_eval.sh <기준버전> <신버전> [신버전 CSV경로]`
- `bank_eval.sh` 의 `flips`/`prune` 단계가 만드는 뷰(`30_fixed`/`31_broken`)와 산출물
  `prompt_authoring_guide.md` 는 개별 스테이지만 돌리면 생기지 않는다 — 버전 비교는 래퍼로 돌릴 것.

### 분석 표준화 계층 — `cohort.py` / `analysis_standard.py` / `prompt_standard.py`

위 9개는 데이터셋을 **만드는** 진입점이다. 이 셋은 만들어진 데이터셋을 **같은 정의·같은 경고로** 분석하고,
프롬프트를 같은 규칙으로 생성하게 하는 계층이다. README 에 없으면 새 현장 편입 절차가 코드 docstring 과 설계
문서(`docs/superpowers/specs/2026-09-16-analysis-standardization-design.md` ·
`docs/superpowers/plans/2026-09-16-analysis-standardization.md`)에만 남는다. `cohort.py` 는 그 사이클에서 새로 만든
모듈이고, `analysis_standard.py`·`prompt_standard.py` 는 설계서 §2 가 "이미 있는 표준화 씨앗"으로 꼽은 것이다.

| 모듈 | 역할 | 비고 |
|---|---|---|
| `cohort.py` | 코호트 레지스트리. 표준 러너 기준으로는 새 현장 편입이 `COHORTS` 한 줄로 끝난다 — `prompts`/`gt_field`/`group`/`negative_class`/`target_classes` 는 **전부 명시**한다(빠진 값을 추론하지 않는다) | CLI 없음. `analysis_standard.py` 가 `load_cohort()` 로 라이브 FiftyOne 에서 읽는다. 그 밖의 소비자: `prompt_geometry.py`(클래스 순서 대조) · `user-prompt-probe`(음성 클래스 이름) |
| `analysis_standard.py` | 표준 러너. S0~S5 고정 순서 스테이지(앞 단계의 경고가 뒤 단계의 해석을 바꾼다) + G1~G8 가드레일 | `docker exec docker-analysis-1 python3 /workspace/analysis_standard.py run --dataset sourcei` · `guardrails` 는 표만 출력. 산출 `/data/fiftyone/frames_bank/report/<코호트>/standard_report.json`(정본) · `standard_card.md` |
| `prompt_standard.py` | 프롬프트 생성·검증 규칙의 정본 — sourcei GT 에서 측정한 템플릿 형태 선택도·금칙 어휘·장소 어휘 억제·라벨-free 컷·검증 규칙. 손으로 쓴 Gemini 지시문 · `user-prompt-probe` 생성 오퍼레이터 · 뱅크 빌더 사전 컷, 이렇게 세 곳에 흩어져 있던 규칙을 한 곳에 고정했다(흩어지면 드리프트한다) | import 하는 쪽(main): `apo_loop.py` · `gen_full_bank.py` · `gen_intrusion_bank.py` · `prompt_rule_fields.py` · `plugins/user-prompt-probe`. CLI `rules` `generate` `validate` `probe` `selftest` |

- **군집키(`group`)는 자동 유도하지 않는다.** `sitej_subway` 는 `camera` 가 58대라 자동 유도하면 `camera` 를 고른다.
  그런데 이 코퍼스는 연출 동시녹화라 카메라 홀드아웃에 누수가 있고, 올바른 키는 `session` 이다. 군집키를 잘못 잡으면
  신뢰구간이 좁아져 거짓 유의가 나온다. 그래서 미등록 코호트는 `camera` 로 폴백하지 않고 `CohortRefused` 로 **거부**한다.
  거부도 `display_allowed=false` 아티팩트로 발행해 이전 성공본을 덮는다(발행을 생략하면 소비자가 낡은 표를 계속 그린다).
  `frames` 는 GT 부족·군집 필드 부재로 `REFUSED` 에 명시돼 있다.
- ⚠️ **"한 줄"은 표준 러너 기준이다.** `target_classes` 는 GT 라벨과 **글자 단위로** 같아야 한다. `sitej_subway` 의
  `intrustion` 은 오타지만 데이터의 정본 철자라, 고치면 조인이 깨진다. 또 `[negative_class] + target_classes` 순서가
  곧 정수 GT 다. 등록돼 있어도 군집 필드나 GT 필드가 없거나, 레지스트리 밖 GT 클래스나 빈 군집값이 있으면 `load_cohort()`
  가 거부한다(다른 필드로 대체하지 않는다). 프로브·`prompt_geometry.py`·`apo_loop.py` 는 `prompt_geometry.PROFILES`
  를 따로 읽는다(프로브 docstring 은 이 둘을 "두 정본"이라 부른다). 그래서 같은 현장을 거기에도 등록해야 한다. `gt_field`
  를 선언한 프로필은 `_assert_cohort_class_order` 가 두 곳의 클래스 순서를 대조하고, 다르면 `SystemExit` 로 멈춘다.
- ⚠️ **가드레일은 경고일 뿐 차단하지 않는다**(모듈 docstring: "판정은 사람이 하지만, 경고는 자동"). 발동해도 `fired`
  플래그만 남는다 — G3 가 발동해도 S3 뱅크별 채점표(`standard_card.md` · `S3_scoring.csv`)는 그대로 기록된다.
  G5 는 `macro_present` 가 구조적으로 적용하고, G8 은 정의만 있을 뿐 판정 코드가 없다. `evidence` 는 7,498장
  구코호트 기준의 **정적 문자열**이다. 라이브 판정은 `fired`/`detail` 만 볼 것
  (`docs/superpowers/specs/2026-09-16-icc-estimator-audit.md`).
- ⚠️ 라이브 경로(`load_cohort`)는 `gt`/`group` 만 싣고 점수 행렬은 싣지 않는다. 그래서 `--stages` 기본값이 `S0` 이다.
  S1~S5 는 점수 행렬이 필요해 아직 라이브에서 돌지 않고, 지금 라이브에서 판정되는 가드레일은 S0 의 G2 뿐이다.
  `--legacy-sourcei-npz` 는 7,498장 `preds.npz` 를 읽어 라이브와 어긋나므로 사실상 죽은 경로다.
- **아티팩트 발행 계약**: `publish_atomic` 은 `allow_nan=False` → `fsync` → `os.replace` 순서로 쓴다. 그래서 반쯤 쓰인 JSON
  이 노출되지 않고, 쓰다가 실패하면 임시 파일을 지우고 이전 버전을 그대로 둔다(NaN 은 파이썬은 읽지만 패널의 `JSON.parse` 는
  터진다). `finalize_status` 는 스테이지가 하나라도 `error` 면 `display_allowed=False` 를 붙인다. 요청하지 않은 스테이지는
  `skipped` 로 남겨 "안 돌린 것"과 "돌리다 죽은 것"을 구별한다. 이전에는 스테이지 예외를 기록만 하고 정상 report 로
  계속 진행해서, 죽은 스테이지가 든 결과를 정상으로 읽을 수 있었다. ⚠️ main 에는 아직 `display_allowed` 를 읽는 소비자가
  없다 — `standard_report.json` 을 읽는 쪽이 이 플래그를 직접 확인해야 한다.
- ⚠️ `prompt_standard` 로 새 현장 규칙을 만들 때: 현장 고유 금칙어는 전역 `BANNED` 가 아니라 `EnvProfile.banned` 에
  둔다(전역에 넣으면 새 현장이 남의 현장 규칙을 물려받는다). CLI 에서 현장 프로필 JSON 경로를 받는 것은
  `generate --env <경로>`(`load_env`) 뿐이다. `validate` 서브커맨드는 `ENVS[a.env]` 로 조회해 등록명만 받는다.
- ⚠️ `validate()` 는 `(kept, rejected, report)` **3개**를 돌려준다. 모듈 docstring 사용례처럼 2개로 언패킹하면
  `ValueError` 가 난다. `apo_loop.py` 의 `generate` 경로가 바로 그렇게 언패킹하고 이 오류를 `except Exception: pass` 로 삼킨다.
  그 결과 규칙 검사가 조용히 무력화돼 모든 문장이 통과(`rule_ok=True`)로 처리된다.

## frames 프롬프트 뱅크 평가 (frames_bank_eval.sh)

- 전체 사이클: `./docker/analysis/frames_bank_eval.sh` — 매핑이 비어 있으면 0단계(스탬프만)로
  정직하게 끝난다. **2026-08-19 기준 `bank_domain_map.yaml` 은 전 project(21개 도메인)로
  확장 시드되어 전부 채점이 돈다**(전부 동일 쌍 bank_a=v1.0.8.0 / bank_b=v1.0.8.4) —
  시드 절차는 이후 새 project 가 들어올 때만 필요하다.
  ⚠️ **'시드됨' ≠ '숫자를 인용할 수 있음'** — GT 는 아직 40행(전부 normal)뿐이라 대부분
  도메인의 `tier` 는 `no_gt`/`counts_only` 다 (min-n tier 절 참조).
  도메인 목록·뱅크 배정의 정본은 문서가 아니라 `bank_domain_map.yaml` 자체다.
  도메인을 열려면 `domains:` 를 노션 "프롬프트 버전/관리 체계 구축" 페이지 기준으로
  시드하고 뱅크 CSV 를 `--bank` 로 등록.
- GT(LS finalized)가 늘었을 때: 재채점 불필요 —
  `frames_bank_ledger.py` → `gtsync` → `report` 만 재실행 (래퍼 주석 참조).
- sourcej GT(patient/person)는 `class_crosswalk` 에 사상을 등재해야 GT 축에 편입된다.
- ⚠️ `slim` 스테이지는 source-h 전용(코드 가드 있음). `frames` 의 필드 정리는 수동으로만.
- 산출: FiftyOne 필드 6개(bank_*), 뷰 **도메인당 3개**(`bank: <도메인> scored/shifted/review-queue`
  — 21도메인이면 최대 63개 저장뷰), 리뷰큐 **도메인당 상한 500**(최대 10,500건 + `<도메인>_queue.json` 21개).
  도메인별 fail-forward — 한 도메인 실패해도 나머지는 계속 (실패는 `runs.jsonl` 기록),
  워크스페이스 `bank-eval`, 리포트 `/data/fiftyone/frames_bank/report/bank_eval_report.md`,
  런 원장 `/data/fiftyone/frames_bank/work/geometry/runs.jsonl`.

### min-n tier — 리포트의 `tier` 컬럼이 뜻하는 것

리포트에 도메인별 `tier` 가 찍히는데, 이건 **GT 표본이 그 숫자를 말할 자격이 있는지**를
게이팅한 결과다 (`prompt_geometry.py` `minn_tier()`). GT 가 적을 때 백분율을 그대로 보여주면
"2/3 = 66.7%" 같은 숫자가 실제 성능처럼 읽히는 것을 막기 위한 장치다.

| tier | 조건 (GT 이미지 수 `n`) | 리포트 표기 |
|---|---|---|
| `no_gt` | `n = 0` | **% 표시 금지** — 스탬프만 |
| `counts_only` | `0 < n < 30` | 건수만 (백분율 금지) |
| `exploratory` | `30 ≤ n < 100` | % 표시하되 탐색용 — 결론 근거로 쓰지 말 것 |
| `reportable` | `n ≥ 100` **그리고 소스영상 ≥ 30** | 보고 가능 |

⚠️ **`reportable` 에는 이미지 수 외에 두 번째 조건이 있다.** 이미지가 100장을 넘어도
소스영상이 30편 미만이면 `gtsync` 단계에서 `exploratory` 로 **강등**된다
(`prompt_geometry.py` 의 `reportable→exploratory 캡` 로그). 한두 영상에서 프레임을 많이 뽑아
100장을 채운 경우를 "충분한 표본"으로 오인하지 않기 위한 것 — 같은 영상의 프레임은 서로
독립 표본이 아니기 때문이다.

즉 `tier` 가 `reportable` 이 아니면 그 도메인의 수치는 **아직 근거로 인용할 수 없다.**
올리는 방법은 채점 재실행이 아니라 GT 를 늘리는 것뿐이다(사람 검수 확정 → `ledger` → `gtsync`).

## 프롬프트 관점 데이터셋 — `source-h-prompts` (promptmap)

> ⚠️ **이 데이터셋은 2026-08-18 삭제되어 아래 명령/URL 은 더 이상 유효하지 않다 — 이력 참고용.**
> 같은 구조가 `sourcei-prompts` 로 살아 있으니 실습은 그쪽에서.

프레임 관점(`top_prompt_*`, `winner_*`)의 뒤집힌 짝. **점 하나 = 문장 하나**라서 프롬프트를
카테고리별로 보고, 그 문장이 실제로 어떤 이미지에 붙는지 확인하는 용도.

```bash
# (bind mount 라 복사 불필요 — repo 파일이 곧 /workspace/prompt_geometry.py)
docker exec docker-analysis-1 nice -n 10 python /workspace/prompt_geometry.py promptmap
# → http://10.0.0.10:5153/datasets/source-h-prompts  (워크스페이스 `prompts` 선택)
# ⚠️ 데이터셋은 **헤더 선택기**로 바꿔야 한다 — App 은 접속 시 서버 세션의 현재
#    데이터셋에 스스로 동기화하므로 URL 만으로는 안 붙는다 (2026-08-20 실측).
#    전환 직후 stale 플롯을 의심하려면 배너의 **모집단 숫자**로 게이트할 것.
```

- 좌표(UMAP `emb_viz`)는 **문장끼리의 기하만** 뜻한다. 문장+이미지를 한 UMAP 에 올리는 건
  실측으로 기각돼 있다 (text↔image cos 중앙 0.147 vs text↔text 0.631 vs image↔image 0.756
  → modality 두 덩이가 되고 최근접 질의가 엔티티 타입 분류기가 된다. `stage_atlas` 도크스트링).
- 이미지 연결은 좌표가 아니라 표본 속성으로 준다: 썸네일 = 그 문장의 **최근접 프레임**,
  `match`(최근접 프레임 GT == 문장 클래스), `nearest_gt.confidence`(=cos), `nearest_key`.
- `wins`/`purity`/`n_cameras` 는 `prompt_frames_*.csv` 와 같은 정의(클래스별 best 의 전역
  argmax). 제품 판정규칙인 top-K 다수결(스테이지 `vote`, env `VOTE_K`)과는 다른 값이다
  — 위 「판정규칙 3벌」 표 참조. (`RULE`/`RULE_K` 는 프레임 예측 헬퍼용 별개 스코프.)
- 색칠은 `category`·`match`·`adopted`·`purity_tier`(전부 Classification → `.label`).
  `purity`/`wins` 는 연속값이라 App 에서 색이 안 나온다 — 정렬·필터용.
- brain_key 가 `emb_viz` 로 고정인 이유: Embeddings 패널이 키를 기억해서 다른 이름이면
  Color by 까지 죽는다.
- ⚠️ 뱅크 2벌(`BANK_A`/`BANK_B`)이 한 데이터셋에 같이 들어간다 — `bank_version` 으로 필터.
  같은 문장이 두 뱅크에 다 있으면 점이 겹치는데, 그 자체가 "무엇이 유지됐나" 신호다.

### 판정규칙 3벌 — argmax vs top-K 다수결 vs 분포 IoU(wave)

`source-h-prompts` 는 **같은 문장 점 위에 여러 규칙의 값을 나란히** 올린다.

⚠️ **`wins`/`purity`/`adopted` 는 top-K 다수결이 아니다.** 이 셋을 만드는 `atlas`/`promptmap` 은
`prompt_geometry.py` 의 `M.argmax(axis=1)` 로 **K=1 argmax 를 하드코딩**하고 있고 `RULE`/`RULE_K`
환경변수를 읽지 않는다. 제품의 top-K 다수결은 **별도 스테이지 `vote`** 이고 필드도 다르다.
표를 잘못 읽고 `wins`/`adopted` 를 "제품 규칙 결과"로 인용하면 **뱅크 버전 채택 판단이 틀어진다.**

| 규칙 | 스테이지 | 정체 | env | 문장별 지표 |
|---|---|---|---|---|
| **argmax (K=1)** | `atlas` · `promptmap` | 클래스별 best 의 전역 argmax. 옛 단일 체계 | 없음 (하드코딩) | `wins`·`purity`·`adopted` |
| **top-K 다수결** | `vote` | 상위 K개 문장의 클래스 다수결 = 제품 APO 규칙 | `VOTE_K`(기본 10) · `VOTE_KS`(1,3,5,10,20,50) | `vote_<k>`·`vote_margin_*`·`rule_flip_*` |
| **분포 IoU (wave)** | `wave` | 제품 `pe_inference/01_TuningFree_v2.py`. 클래스별 cos 히스토그램 vs normal 히스토그램의 면적 IoU < `WAVE_THR` → 발화 | `WAVE_BINS`(80) · `WAVE_THR`(0.15) | `wave_gain`·`wave_role` |

- `rule_flip_*` 는 **K=1 판정과 K=K 판정이 갈린 프레임**에 `"argmax→vote"` 형태로 붙는다 —
  두 규칙의 불일치를 눈으로 찾는 용도.
- `RULE`/`RULE_K` 환경변수는 위 3벌과 **다른 스코프**다: 문장 단위 스테이지가 아니라
  **프레임 예측 헬퍼**(`prompt_geometry.py` 의 "현재 판정규칙으로 프레임 예측")가 쓴다.
  `RULE=argmax` 로 두면 옛 동작으로 회귀 비교가 가능하다.

```bash
docker exec docker-analysis-1 nice -n 10 python /workspace/prompt_geometry.py wave
docker exec docker-analysis-1 nice -n 10 python /workspace/prompt_geometry.py promptmap   # wave 축 흡수
```

- env: `WAVE_BINS=80` `WAVE_THR=0.15` (pe_inference README 권장 실행값). `iou_mode='std'` 는 미구현.
- **모수가 다르다**: top-k 는 이긴 문장만 값이 있고(v080 채택 201/12,480 = 1.6%), wave 는 분포
  전체가 판정에 들어가 **모든 문장에 값이 있다**. "뱅크 실사용률 1.6%" 는 top-k 한정 결론이다.
- 문장별 wave 기여도 = LOO ΔIoU. **부호 해석이 역할에 따라 뒤집힌다** (이벤트 문장은 IoU 를
  낮춰야 유익, normal 문장은 높여야 유익) → raw float 은 `wave_gain`, 해석은 `wave_role` 이 담당.
  층화는 클래스 내 백분위 — 클래스별 문장 수 차이(normal 10,703 vs falldown 160)가 ΔIoU 절대
  크기를 바꾸므로 전역 임계는 클래스를 오분류한다.
- 12,480회 LOO 가 가능한 이유: IoU 는 히스토그램만 보므로 **같은 bin 의 문장은 ΔIoU 가 같다**
  → 프레임×클래스×bin(80) 만 계산한다. 이 지름길은 `selftest` 가 브루트포스와 대조한다.
- ⚠️ 디바운스(최근 5중 3↑) 미재현 — source-h 은 키프레임 집합이라 시간 이웃이 없다.
  프레임 필드 IoU 는 디바운스 **이전** 신호.
- 프레임 쪽 필드: `wave_pred_<vt>` · `wave_iou_<cls>_<tag>` · `wave_vs_topk_<tag>`
  (두 규칙이 갈린 프레임의 `topk→wave` 라벨) + slim 워크스페이스 `wave`.

## 이미지별 속성 (attrs) — 현재 1축: 실내/실외

```bash
docker exec docker-analysis-1 python /workspace/prompt_geometry.py attrs        # sourceh
BANK_PROFILE=frames docker exec ... python /workspace/prompt_geometry.py attrs  # 데이터셋 `frames`
```

- 기존 프레임 임베딩 + `/embed_text` 프로브만 쓴다 (새 모델·GPU 불필요). 축을 늘리려면
  `ATTR_AXES` dict 에 항목 추가 — **라벨당 문장 수를 같게** 유지할 것(사전확률 누수).
- 왜 DB 가 아닌가: `video_metadata.environment_type` 슬롯은 있지만 source-h 871편 전부
  `env_method='deferred'`/NULL 이다 (Places365 정지 + Gemini 씬 백필 미실행). 게다가 영상 단위.
- 필드: `environment`(Classification, confidence=margin) · `environment_margin`(1위−2위 cos).
- **자기검증 = 카메라 내 일관성** (고정 카메라니 갈리면 잡음). source-h 실측:
  area-a outdoor 99.9%(margin +0.031) · area-b outdoor 99.8%(+0.044) ·
  **ODCarea-a 54%(+0.0035 = 동전던지기)**. ODC 는 분류 실패가 아니라 장면 자체가
  창고 셔터 정면 + 옥외 아스팔트라 축이 정의되지 않는다 → margin 낮은 순 정렬로 걸러낼 것.
- ⚠️ source-h 에서 이 축의 정보량은 사실상 0 — 카메라 3대뿐이라 실내/실외는 `camera` 의 함수다
  (slim 이 `camera_angle`/`tilt_bin` 을 지운 것과 같은 이유). 도메인이 섞인 `frames` 에서 의미가 생긴다.

### attrs 축 4개 + 조건별 오탐·미탐 크로스탭 (노션 「데이터 임베딩 회의 내용 정리」 §3)

축: `environment`(실내/실외) · `daynight`(주간/야간) · `person`(사람 유/무) ·
`weather`(맑음/흐림/비/눈). 이상상황 카테고리는 `ground_truth` 담당.
산출: 필드 축마다 2개(`<축>` + `<축>_margin`) + `report/attrs_cross.md` + `work/geometry/attrs.json`.

**검증 결과 — 축마다 신뢰도가 다르다. 섞어 쓰면 안 된다.**

| 축 | 검증 | 판정 |
|---|---|---|
| `daynight` | 파일명 시각(`_YYYYMMDD_HHMMSS`) 대조 **98.6% 일치** (n=13,144) | ✅ 신뢰 |
| `person` | GT falldown **246/246 = 100% yes** (정의상 사람 있음), fire 96% | ✅ 신뢰 |
| `environment` | 카메라 내 99.8~99.9% (2대) / **ODC 54%** (margin +0.0035) | ⚠️ ODC 는 셔터정면+옥외아스팔트라 축 자체가 미정의 |
| `weather` | **날짜 내 일관성 65.8%** (같은 날 rain/overcast 50/50 분할) | ❌ rain↔overcast 는 노이즈. `clear` 만 날짜와 정합 |

- `weather` 는 게이트(`ATTR_GATES`)로 `daynight=day` + `environment=outdoor` 밖을 전부
  `undetermined`(47.1%) 처리한다. 게이트 없이 돌리면 **날씨가 아니라 밝기를 읽는다** —
  실측으로 야간 clear 0장, 야간 5,579장이 rain/overcast 로 임의 분할됐다.
- 게이트 축은 자기보다 **먼저** 계산돼야 한다 (`ATTR_AXES` dict 삽입순 = 계산순).
- 축 추가 시 라벨당 문장 수를 같게 (사전확률 누수). `person` 은 zero-shot 이 가장 약한 축 —
  작고 먼 인물은 전역 임베딩에 안 남는다. 부족하면 SAM3 `/segment` 로 교체(코드에 ponytail 주석).

**크로스탭에서 나온 것 (`report/attrs_cross.md`)**

- ⚠️ **acc 를 슬라이스끼리 비교 금지** — GT 이벤트/정상 구성이 슬라이스마다 다르다.
  비교 가능한 건 이벤트 대비 `FN%`, 정상 대비 `FP%`. 표에 `이벤트/정상` 열을 같이 낸다.
- **야간 5,579장 = 이벤트 0 / 정상 5,579.** 이벤트 프레임이 전부 주간이다 → "야간 정확도
  97.7~99.9%" 는 탐지할 게 없는 구간의 숫자다. **야간 이벤트 데이터가 없는 것이 최대 공백.**
- 실내 631장에서 wave v080 48.0% vs topk v084 86.8% — **실내에서 wave 가 크게 불리**.
- FN 은 `person=yes`(1,235~1,290) 와 `weather=clear`(1,000~1,156) 에 몰린다. clear 는
  주간 화재 촬영분이 몰린 구간이라 사실상 "밝은 주간 이벤트" 슬라이스로 읽어야 한다.
- 카메라별 최악은 ODC(wave v080 56.0%) — 실내 슬라이스와 같은 프레임군이다(ODC=실내 판정).

## source-i 실내 데이터셋 — `sourcei` (sourcei_build.py)

노션 「데이터 임베딩 회의 내용 정리」 §1(실내 데이터로 이동) 적용. 이벤트 구간만 프레임화.

```bash
# (bind mount 라 복사 불필요)
docker exec docker-analysis-1 python /workspace/sourcei_build.py all   # segments→frames→sam3→embed→build
# 뱅크 분석은 prompt_geometry 재사용 (v1.0.8.0 단일 뱅크)
BANK_A=v1.0.8.0 BANK_B=v1.0.8.0 ... prompt_geometry.py attach --profile sourcei
#   허용 스테이지: attach / vote / wave / promptmap / attrs (그 외는 코드가 거부)
```

**⚠️ 이건 recall 벤치마크가 아니라 오탐(FP) 스트레스 테스트다.** DB 실측 810 이벤트/109편에서
4클래스 GT 는 falldown 57 / fire 5 / smoke 6 **구간**뿐이고(fire 는 총 10초) normal 721 구간이
모수다. 대부분이 4클래스 어디에도 없는 실내 장면 → 뱅크가 여기서 이벤트를 부르면 그게 오탐이다.
**recall/F1 을 인용하면 안 된다.**

- **"넘어질 뻔함"(near_miss) 509건은 falldown 이 아니다** → 기본 GT normal. falldown 으로 세면
  없는 FN 을 만든다. 판단을 코드에 묻지 않고 `event_kind` 필드로 남기니 App 에서 뒤집어 볼 수 있다.
- GT 우선순위: 폴더(`/esfalldown|falldown|fire|smoke|normal/`, v2 만 있음) → 캡션 정규식 → 없음.
  캡션 규칙은 **`뻔` 을 `넘어지` 보다 먼저** 본다. `sourcei_v3` 102 이벤트는 캡션이 NULL +
  파일명이 uuid → `event_kind=unknown`, GT normal 로 들어간다 (모수로만 쓸 것).
- **영상을 내려받지 않는다** — presigned URL + ffmpeg `-ss/-to` Range 요청.
  실측: 3,600초 원격 mp4 에서 5초 구간 추출 **0.4초**. 789구간/3,844초 → 7,498장 **167초, 실패 0**.
  호스트 루트가 98%(여유 19GB)라 8.4GB 영상 사본을 만들 여유가 없었다. 프레임만 1.9GB.
- fps=2 는 제품 `pe_inference --model_input_fps 2` 와 맞춘 값. 구간 경계 ±0.5s 패딩.
- SAM3.1: **프레임을 지우지 않고 `sam3_hit` 플래그만** 남긴다 (미검출도 오탐 분석의 모수).
  "이벤트 구간만" 을 더 좁히려면 App 에서 `sam3_hit=hit` 필터.
- ⚠️ SAM3 응답 스키마: 라벨 `prompt_class` · 박스 `mask_bbox`(xyxy) · 크기 `image_size=[w,h]`.
  `prompt`/`label`/`width`/`height` 는 **없다** (처음 이 키로 파싱해 라벨을 통째로 잃었다).
- ⚠️ **공유 `docker-sam3-1` VRAM 누적 누수** — 장기 배치 중 워커 3개의 PyTorch 캐시가 16.85/16.88GB 까지 찬다.
  다 차면 **모든** `/segment` 가 `OOM → HTTP 500`(503 아님) 이 된다. CLAUDE.md 의
  "workers 3 ≈ 11.1GB" 는 stale.
  - **해상도 문제로 오진하지 말 것** — 1280/1024/896 어느 배율로 줄여도 40장 중 39~40장
    실패했고, 직전까지 성공했던 프레임도 전부 실패했다. 프레임·배율을 바꿔도 실패가
    유지되면 서비스 상태 문제다.
  - 복구: `POST /unload` **4~6회**(워커 3개라 1회는 한 워커만) → 16.85GB→6.35GB.
    다음 요청이 lazy reload 하므로 **prod 컨테이너 재시작 불필요**.
  - 예방: `SAM3_UNLOAD_EVERY=500` (500프레임마다 `/unload ×4`).
    실측 효과 **실패율 17%→0%, 1.36→0.52 s/frame**.
  - `SAM3_MAX_SIDE=1024` 는 피크 완화용으로 남긴다. **바꾸면 검출 민감도가 바뀌므로**
    데이터셋 안에서 섞지 말고 `sam3.jsonl` 을 지우고 전량 재처리할 것 (레코드에 `max_side` 기록).
- bbox 는 **축소 좌표계 그대로** 저장하고 `image_size` 로 정규화한다 (원본 복원 불필요).

## source-i 실내 데이터셋 (`sourcei` / `sourcei-prompts`)

`sourcei`(프레임 7,498) ↔ `sourcei-prompts`(문장 12,480)는 `sourcei.winner_gidx_v080` ↔
`sourcei-prompts.gidx` 조인으로 연결된다 (`sum(sourcei-prompts.wins) = 7,498 = sourcei.count()`).

- 프레임 필드 `wave_gain`/`wave_role`은 승자 문장 값의 **복사본**이다(실측 15/15 표본 바이트 일치,
  `winner_gidx_v080`↔`gidx` 조인) — 원 산출은 컨테이너 라이브 `/workspace/prompt_geometry.py:2523-2524`
  (`stage_promptmap`, 문장 단위 LOO gain), 프레임 복사는 git 미추적 1회성 스크립트
  `/tmp/symmetric.py:85`가 수행. 분석/Panel 은 `wave_gain`/`wave_role` 에 대해서는 `<DS>-prompts`
  쪽 필드를 정본으로 읽을 것. ⚠️ **문장 텍스트는 예외** — 2026-08-19부터 패널은 Postgres
  `bank_sentences` 를 정본으로 읽고 데이터셋 `text` 는 폴백이다(위 임베딩 패널 절 참조).
- ⚠️ 이 worktree의 `docker/analysis/prompt_geometry.py`(git HEAD)는 `stage_wave`/`stage_promptmap`
  자체가 없는 별개 버전이다(`git log --all` 에도 부재) — 위 두 스테이지는 컨테이너 `/workspace`에만
  존재하는 git-미추적 코드이므로, 이 필드들의 grep 근거는 반드시 라이브 컨테이너 경로여야 한다.

## FiftyOne App 설정 정본화 (`fiftyone_app_setup.py`, 색상/워크스페이스)

정본 `docker/analysis/fiftyone_app_setup.py` (git). 배포·실행:

서브커맨드는 아래 4개 외에 5개가 더 있다 (총 9): `dump <ds>` / `restore <file>` /
`slots <ds> [--apply]` / `filters <ds> [--apply] [--slots A,B]` / (아래 4개).
⚠️ **`filters --apply` 는 공유 호스트의 App 화면을 바꾼다** — 적용 전에 고지하고 `dump` 를
먼저 받아둘 것(기본 dry-run). 그리고 `prompt_geometry` 의 `stage_viz_frames` 와 **순서 의존** —
viz 를 먼저, `filters --apply` 를 나중에 (역순이면 판정 그룹이 비고 `bank_*` 가 접힌 뱅크
그룹으로 끌려간다). `filters` 는 실측 선정된 `CURATED_DATASETS`(현재 2개)만 허용.

```bash
# (bind mount 라 복사 불필요)
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py selftest              # 팔레트 위생 검사
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py colors [ds1,ds2,...]   # 기본 5개(코드 DEFAULT_DATASETS) — 그중 삭제된 2개는 "skip (없음)" 으로 지나간다(에러 아님). 96d91e1 이 frames 추가. 실제 목록: sourcei,sourcei-prompts,source-h,source-h-prompts
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py workspace              # sourcei: rules (Samples | Embeddings, rule_cross 불일치)
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py workspace-compare       # sourcei: compare (Samples | Embeddings | Prompt Compare, H1)
```

`/workspace` 는 bind mount 라 recreate 에도 남고, 위 명령의 산출물(`app_config.color_scheme`·
저장 워크스페이스)은 `fiftyone-mongo` 영속 볼륨에 있다 — **재배포 후 재실행은 불필요.**
재실행이 필요한 경우는 둘뿐: (a) 누군가 App UI 의 "Save as default" 로 색을 덮었을 때,
(b) `docker/data/` 를 밀어 mongo 볼륨이 사라졌을 때.

- **색상 스킴(R3)**: `CLASS_COLORS`(Okabe-Ito 색맹 안전 팔레트 기반) 를 전 데이터셋에 고정 적용.
  ⚠️ **App UI("Color settings" → 필드/값 색 수동 조정)로 바꾼 색은 기본적으로 세션 한정이며,
  사용자가 모달 안의 "Save as default" 를 직접 눌러야만 `dataset.app_config.color_scheme` 에
  영속되어 우리 Python 기본값을 덮어쓴다** (실측 확인, 2026-08-07 — 모달 하단에
  `Reset` / `Save as default` / `Clear default` 3버튼 존재). 즉 누군가 "Save as default" 를
  누르면 CLASS_COLORS 가 조용히 무효화될 수 있다 — **커스텀을 원상복구하려면 `CLASS_COLORS` 를
  코드에서 고친 뒤 `colors` 서브커맨드를 재실행**할 것 (App UI 로는 되돌릴 수 없음, Python
  쪽이 유일한 정본).
- **워크스페이스**: `rules`(Task 3, 판정규칙 불일치 프레임 탐색) / `compare`(Task 10, H1 확정안 —
  아래 user-prompt-compare 절 참고). 둘 다 `sourcei` 데이터셋에 저장됨.

### user-prompt-compare — 교차 데이터셋 비교 패널 (2026-08)

- **문장 해석 정본 = Postgres (2026-08-19, 88a3c5c~)** — npz 간접 참조는 은퇴. DSN env 필요
  (compose 가 `DATAOPS_POSTGRES_DSN` 주입), fail-closed 게이트 3종 통과 시 DB 문장, 위반
  버전은 통째 폴백 + 배너에 출처/사유 표기. kill-switch `PROMPT_DB=off`. 동일 로직이
  user-embeddings/user-image-embeddings 에 byte-identical 사본 — 한 곳 고치면 셋 다.
- 정본 `docker/analysis/plugins/user-prompt-compare/` → 배포:
  (bind mount 라 복사 불필요 — 디렉토리 touch 만으로 캐시가 무효화된다)
- 워크스페이스 `compare`(sourcei): Samples | Embeddings | Prompt Compare 3-패널(H1 확정안).
  모드 A=프레임↔문장(argmax_k1 조인, dist_iou 모드는 클릭 무효), 모드 B=같은
  데이터셋 그룹 overlay(`frames` 에서 project 비교), **모드 C=문장 군집**(좌표는 emb_viz(UMAP)
  그대로 두고 색만 MiniBatchKMeans 군집 id 로 칠한다, 기본 k=12).
  군집 공간은 조건부다 — 그려지는 점(버전 필터·NaN 좌표 제외 후)이 20,000 이하이고 DB 에서
  벡터를 90% 이상 끌어오면 **1024-d 임베딩 공간**, 아니면 **UMAP 좌표 공간**에서 군집한다.
  둘은 다른 질문("의미가 비슷한 문장끼리" vs "그림에서 뭉쳐 보이는 것")이라 어느 쪽을 썼는지
  배너가 밝힌다 — 버전 필터로 점 수가 20,000 을 넘나들면 군집 공간 자체가 바뀐다.
  ⚠️ **모드 C 는 품질 판정용이 아니다.** 과거 측정에서 군집 특이도는 이벤트 클래스가 아니라
  **장소 어휘**가 지배했다(같은 발견: `docs/analysis/prompt-embedding-analysis-design.md` §2) —
  군집이 잘 갈려 보여도 장소별로 갈린 것일 수 있으니 뱅크 우열의 근거로 쓰지 말 것. 자리는
  탐색·중복 발견용이다(2026-08-31 사용자와 합의한 위치).
  200,000점 상한: 전량 렌더 시 figure 13.85MB(ids 5.03MB 포함)가 플롯 이벤트마다 되돌아와
  왕복 ~69초였고 Chrome 을 죽인 적이 있다. ids 도 싣지 않는다 — 모드 C 는 클릭 조인을 하지
  않는다(점 클릭·lasso 로 프레임을 고르는 건 모드 A 뿐). 단 선택된 문장의 강조는 서브샘플에서
  탈락한 점까지 그린다(안 그러면 "선택이 안 먹었다"로 읽힌다).
- selftest(조인 불변식 + 패널 상태-머신 회귀 3종 — 개수는 늘어나므로 박지 않는다): `docker exec docker-analysis-1 python /data/fiftyone/datasets/__plugins__/user-prompt-compare/__init__.py`
  — FiftyOne 업그레이드 전 필수 게이트. 2026-08-27 부터 ① 표시 드롭다운 왕복(컨트롤 미러 에코 루프)
  ② 데이터셋 전환 후 stale 산점도(`_fig_key` 에 데이터셋 누락) ③ gidx 오프셋 세대 불일치 회귀가 함께 돈다.
  **실패 시 producer drift 뿐 아니라 패널 상태-머신 회귀도 의심할 것** — 원인이 다르면 고칠 파일도 다르다
  (전자는 `prompt_geometry.py`/DB 쪽, 후자는 플러그인 `_reconcile`/`_fig_key`/`gidx_shift`).
- 색상/워크스페이스 재설정: `python /workspace/fiftyone_app_setup.py colors|workspace|workspace-compare|workspace-fix`
  (`workspace-fix` = 전 데이터셋 워크스페이스 일괄 정규화 — Space>Panel 래핑/active_child=None 레거시가 빈 화면을 만든다. 멱등)
- **브라우저 검증**(2026-08-07, playwright): 워크스페이스 선택기의 기본 목록은 최근 항목만
  보여준다 — 새로 저장한 워크스페이스(`compare`)가 목록에 안 보이면 F5 로도 해결 안 되고,
  선택기의 "Search workspaces.." 검색창에 이름을 직접 타이핑해야 나온다(서버 조회는 정상,
  프론트 기본 목록만 최근 N개로 제한됨). 검색 결과 클릭 → 3-패널(Samples | Embeddings |
  Prompt Compare) 정상 렌더 + Samples 그리드 체크 → 우측 Prompt Compare 패널에 "선택"
  하이라이트(검정 circle-open 마커) 실시간 반영 확인.
  ⚠️ 워크스페이스 전환 직후 드물게 프론트엔드 Relay 스토어 레이스(`Error: entry is loading`,
  App 번들 자체 버그)로 패널 3개가 전부 빈 화면으로 뜨는 경우가 있었다 — 브라우저 탭을 완전히
  새로 열거나 페이지를 한 번 더 새로고침하면 해소된다. 서버(백엔드) 쪽 데이터·워크스페이스
  정의는 매번 정상이었음 (`fo.load_dataset("sourcei").list_workspaces()` 로 확인 가능) — 이
  현상이 나오면 재시도만 하면 되고 재작업 불필요.
- **RSS 실측**(App 서버 프로세스 `main.py --port 5151`, Task 5 와 동일 측정법):
  기존 세션에서 이미 Prompt Compare 패널을 열어 `load_prompt_bundle()` 캐시(`_CACHE`, 64MB
  상한)가 데워진 상태에서 `compare` 워크스페이스를 새 브라우저 세션으로 재오픈 →
  2,764,116 KB → 2,766,256 KB (**+2.1MB**, 예산 100MB 이내, 재오픈 후 3초 대기해도 추가 증가
  없음 = 누수 없음). `workspace-compare` 는 기존 3개 패널 타입(Samples/네이티브
  Embeddings/기존 구현된 Prompt Compare)을 배치만 할 뿐 새 서버측 캐싱을 추가하지 않으므로,
  콜드 캐시 최초 1회 비용은 Task 5 가 이미 실측한 **+35.0MB**(예산 이내)가 그대로 상한이다.
