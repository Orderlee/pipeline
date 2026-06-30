# FiftyOne 운영 런북

> `frames`/`captions` 임베딩 데이터셋 시각화·분석 surface(FiftyOne App + Streamlit 대시보드)의 기동·운영·트러블슈팅.
> 코드에서 알 수 있는 것은 생략하고, **운영하다 막히는 지점**과 **이번 세션(2026-06-30)에 라이브로 검증한 동작**을 중심으로 기록.
> 관련: [`prod-embedding-rollout.md`](prod-embedding-rollout.md) · [`embedding-backup-restore.md`](embedding-backup-restore.md) · [`hnsw-tuning.md`](hnsw-tuning.md) · [`2026-06-16-coordinated-prod-restart.md`](2026-06-16-coordinated-prod-restart.md)

---

## 1. 개요

FiftyOne = 프레임 임베딩(PE-Core-L14-336, 1024-d, pgvector 적재)을 **UMAP/PCA 2D 투영으로 시각화**하고, 라벨(SAM3 detection)·캡션·프로젝트·카테고리별로 탐색하는 도구. 모델 학습 데이터 품질 분석용(B2C 아님).

- **데이터셋**: `frames`(프레임 이미지, prod ≈187,994), `captions`(캡션, ≈11,978). MongoDB 사이드카에 메타데이터 영속.
- **두 surface**: FiftyOne App(:5153, 임베딩 산점도 + 그리드) / Streamlit 임베딩 대시보드(:8503, sunburst·UMAP·거리·검색 등).
- 임베딩 자체는 Postgres `image_embeddings`(pgvector)에 있고, FiftyOne은 시각화/탐색 레이어.

---

## 2. 아키텍처 & 접속

| 항목 | Production | Staging |
|------|-----------|---------|
| 컨테이너 | `docker-analysis-1` | `pipeline-test-analysis-1` |
| Mongo 사이드카 | `docker-fiftyone-mongo-1` | `pipeline-test-fiftyone-mongo-1` |
| JupyterLab | `:8888` | `:8889` (token=`<jupyter-token>`) |
| FiftyOne App | `:5153` (→컨테이너 :5151) | `:5152` |
| Streamlit 대시보드 | `:8503` (→컨테이너 :8501) | `:8502` |
| embedding-service | `:8004` (→ :8003) | `:8013` |
| compose profile | `analysis` (`COMPOSE_PROFILES`에 포함) | 동일 |

- **compose 서비스**: `docker/docker-compose.yaml`의 `analysis` + `fiftyone-mongo` (둘 다 `profiles: ["analysis"]`).
- **Mongo 데이터 영속**: `docker/data/fiftyone/mongo` 볼륨 → restart/reboot 에도 데이터셋·Saved View·태그 보존. (컨테이너 recreate 와 무관하게 살아남음)
- **미디어 캐시**: `docker/data/fiftyone` (MinIO 에서 받은 프레임 이미지, ~24GB).
- **MinIO**: `MINIO_ENDPOINT`(=`ANALYSIS_MINIO_ENDPOINT`, prod `http://10.0.0.51:9000`). presigned URL 이 **브라우저에서 도달**해야 이미지가 뜸 — 내부 `minio:9000` 쓰면 NoSuchKey/빈 이미지.
- 브라우저 접속: `http://10.0.0.10:5153/datasets/frames`.

### ⚠️ 컨테이너 launch 구조 (가장 흔한 함정)
compose `command` 는 **JupyterLab(:8888) 만 자동 기동**한다. **FiftyOne App(:5151) 과 Streamlit(:8501) 은 `docker exec` 로 수동 실행**하는 백그라운드 프로세스다.
→ 컨테이너 restart/reboot/recreate 시 이 두 프로세스는 **죽고 자동 복구 안 됨**. "FiftyOne/대시보드 안 켜진다"의 99% 원인. 아래 §3 으로 재기동.

---

## 3. 기동 / 재기동

### 3.0 먼저 무엇이 죽었는지 진단
```bash
# 컨테이너 살아있나
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | grep -E 'analysis|fiftyone|embed'
# 포트 응답 (호스트)
for p in 5153 8503 8888 8004; do echo ":$p -> $(curl -s -o /dev/null -w '%{http_code}' --max-time 4 http://127.0.0.1:$p/)"; done
#   5153/8503 = 000  → FiftyOne/Streamlit 프로세스 죽음 (컨테이너는 살아있어도). §3.1/§3.2 로 재기동
#   8888 = 302       → JupyterLab 정상(자동기동)
#   8004 정상이면 embedding-service OK (별도 컨테이너)
```
컨테이너 내부 프로세스 확인(컨테이너에 `ps`/`kill`/`ss` 바이너리 **없음** → `/proc` 사용):
```bash
docker exec docker-analysis-1 sh -lc '
for d in /proc/[0-9]*; do c=$(tr "\0" " " < "$d/cmdline" 2>/dev/null);
  case "$c" in *streamlit*|*fiftyone*|*jupyter*|*relaunch*) echo "PID ${d#/proc/}: $c";; esac; done'
```

### 3.1 FiftyOne App 재기동 (:5153)
**빌드된 `frames` 데이터셋이 이미 있으면** → app-only relaunch(재빌드 없음, ~30초, mongo 에서 로드):
```bash
CTR=docker-analysis-1
docker exec -d $CTR sh -c 'cd /workspace && PYTHONUNBUFFERED=1 python3 -u fiftyone_relaunch.py > /tmp/fiftyone_relaunch.log 2>&1'
# 확인: 로그에 APP_LAUNCHED + host :5153 → 200
docker exec $CTR sh -lc 'tail -n3 /tmp/fiftyone_relaunch.log'
until [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://127.0.0.1:5153/)" = "200" ]; do sleep 2; done; echo UP
```
- `fiftyone_relaunch.py` = `fo.load_dataset("frames")` + `launch_app(port=5151)` + keep-alive. **재빌드 안 함.**
- ⚠️ `fiftyone_prod_launch.py` 는 **풀 리빌드**(pgvector 로드+미디어 다운로드+UMAP, 1~3분, 그 동안 :5153 연결거부 정상) — 데이터셋이 없거나 새로 만들 때만.

### 3.2 Streamlit 임베딩 대시보드 재기동 (:8503) — Vertex 번역 env 포함
```bash
CTR=docker-analysis-1
docker exec -d $CTR sh -lc 'cd /workspace && \
  GOOGLE_APPLICATION_CREDENTIALS=/tmp/gemini-sa.json GEMINI_PROJECT=your-gcp-project GEMINI_LOCATION=us-central1 ENABLE_VERTEX_QUERY_TRANSLATION=1 \
  DASHBOARD_REFRESH_SEC=60 streamlit run embedding_dashboard.py --server.port 8501 --server.address 0.0.0.0 --server.headless true > /tmp/streamlit.log 2>&1'
until [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://127.0.0.1:8503/)" = "200" ]; do sleep 2; done; echo UP
```
- **Vertex KO→EN 쿼리번역**: env(`ENABLE_VERTEX_QUERY_TRANSLATION=1`/`GEMINI_PROJECT`/`GEMINI_LOCATION`/`GOOGLE_APPLICATION_CREDENTIALS=/tmp/gemini-sa.json`)는 **프로세스 env 라 relaunch 마다 재주입 필요**. creds(`/tmp/gemini-sa.json`, gemini-api@your-gcp-project)와 google-genai 는 컨테이너 writable layer 에 있어 **restart/reboot 엔 살아남고 recreate 에만 소실**. env 빠지면 크래시 아니라 **사전 fallback**(화재→fire 식 도메인어만, 임의 한국어 문장 번역 X).
- 검증: `docker exec $CTR sh -lc 'GOOGLE_APPLICATION_CREDENTIALS=/tmp/gemini-sa.json python3 -c "from google import genai; c=genai.Client(vertexai=True,project=\"your-gcp-project\",location=\"us-central1\"); print(c.models.generate_content(model=\"gemini-2.5-flash\",contents=\"Translate to English: 주차장에서 두 사람이 다투고 있다\").text)"'`

### 3.3 프로세스 정밀 종료 (포트 충돌/재기동 전)
컨테이너에 `kill`/`pkill` 없음 → `os.kill` 사용. **`pkill python` 절대 금지** (같은 컨테이너에 JupyterLab=PID1, Streamlit, FiftyOne, docker exec 백그라운드 작업이 공존).
- **Streamlit 만**: streamlit 트리(`sh -lc ... streamlit` 래퍼 + `python3 .../streamlit run`)를 SIGTERM → 안 죽으면 SIGKILL.
- **FiftyOne App 만**: 트리 = `sh wrapper`→`relaunch.py`→`fiftyone/service/main.py --51-service`→`main.py --port 5151`. **마지막 server 는 SIGTERM 무시 → SIGKILL 필요.**
```bash
# 예: fiftyone 트리만 종료 (jupyter/streamlit 보호) — cmdline 매칭 + 자식까지
docker exec docker-analysis-1 python3 -c "
import os, signal, time
me=os.getpid(); t=set()
for d in os.listdir('/proc'):
    if not d.isdigit() or int(d)==me: continue
    try: c=open(f'/proc/{d}/cmdline','rb').read().replace(b'\0',b' ').decode('utf-8','replace')
    except Exception: continue
    if 'fiftyone_relaunch' in c or ('fiftyone' in c and ('service' in c or '5151' in c or 'server' in c)): t.add(int(d))
for d in os.listdir('/proc'):
    if d.isdigit():
        try: pp=int(open(f'/proc/{d}/status').read().split('PPid:')[1].split()[0])
        except Exception: continue
        if pp in t: t.add(int(d))
for p in sorted(t):
    try: os.kill(p, signal.SIGTERM)
    except ProcessLookupError: pass
time.sleep(3)
for p in sorted(t):
    try: os.kill(p,0); os.kill(p,signal.SIGKILL)
    except ProcessLookupError: pass
print('killed', sorted(t), '| jupyter PID1 alive', os.path.exists('/proc/1/cmdline'))"
```
> ⚠️ 진단 스크립트 자체의 cmdline 에 'streamlit'/'fiftyone' 문자열이 들어가면 **자기 자신을 매칭**(self-match)한다. 카운트가 1 더 많게 보이거나 자기를 죽일 수 있음 → `os.getpid()` 제외 필수.
> ⚠️ harness 가 background `docker exec` 를 'killed' 로 표시해도 **컨테이너 내부 프로세스는 detach 되어 끝까지 완주**한다(예: 풀 빌드).

### 3.4 ⚠️ 컨테이너 recreate 금기 (`compose up -d analysis` 주의)
prod `datapipeline-analysis:latest` 이미지는 **2026-06-16 빌드라 stale**: 베이크 `/workspace/fiftyone_pgvector.py`=793줄(translate/하이브리드/DQ 없음), `embedding_dashboard.py` 아예 없음, google-genai 없음. 실행 컨테이너 `/workspace`=**2591줄 라이브 cp 분 + 런타임 google-genai**.
→ **`compose up -d analysis` 로 recreate 하면 이 라이브 코드가 전부 stale 이미지로 롤백**(대시보드 회귀, 번역/검색 코드 소실). compose 정의엔 Vertex env+creds 마운트가 완비돼 있으나 **이미지가 stale 해서 적용 불가**.
- **영구화 = 이미지 재빌드 선행**: host `docker/analysis/*.py`(라이브와 동일하게 유지) + requirements(google-genai 포함) → `./scripts/compose-prod.sh build analysis` → recreate. 빌드는 실행 컨테이너 미접촉(무중단). recreate 는 유지보수 창에(서비스 프로세스 재기동 필요).
- **CI 는 analysis 이미지를 재빌드하지 않음**(`docker/analysis/` 가 rebuild 트리거 glob 에 없음). 전부 수동.
- reboot/restart 는 writable layer 보존 → 코드·google-genai 살아남음(프로세스만 죽음, §3 으로 복구).

---

## 4. 데이터셋 구조 (`frames`)

- **샘플** = 프레임 이미지 1장. prod ≈187,994.
- **필드**: `image_id`·`entity_id`·`asset_id`·`minio_key`·`embedding`(1024-d) / `project`(source_unit_name) / `caption`·`daynight`·`environment` / `detection_class`·`normalized_class` / `detections`(SAM3 bbox) / `caption_cluster`·`hdbscan_cluster` 등.
- **brain runs**: `emb_viz`(UMAP 2D), `emb_viz_pca`(PCA 2D). `ds.list_brain_runs()` 로 확인. `points_field=None`(인덱스 미생성 — lasso 성능용 `create_index` 미적용, 정상).
- **`detection_class` vs `normalized_class`**: 현재 prod 에선 사실상 **1:1 거의 중복**(`fallen person`→`fall` 하나만 다름, 나머지 fire/smoke/patient/person/none 동일). 이유: 2026-06-29 COCO 통합이 소스에서 유사어를 이미 병합 → normalize 가 할 일이 거의 사라짐. `normalized_class` 는 **새 SAM3 run 의 유사어 재발(통합은 일회성 백필)에 대한 정규화 안전망**으로 잔존 가치.
- **staging 한계**: staging 은 `image_labels=0`(SAM3 검출 전무) → `detection_class`/`normalized_class`/클래스 분리도 측정 불가(코드 정상, 데이터 부재). uniqueness 등 라벨 무관 신호는 동작.

빠른 점검:
```bash
docker exec docker-analysis-1 python3 -c "
import fiftyone as fo; ds=fo.load_dataset('frames')
print('n', ds.count(), '| brain', ds.list_brain_runs())
print('project', len(ds.distinct('project')), '| saved_views', len(ds.list_saved_views()))
print('detection_class', dict(ds.count_values('detection_class')))"
```

---

## 5. 임베딩 패널 사용법 ⭐ (FiftyOne 1.17 — 라이브 검증)

### 5.1 핵심 원리: 패널은 *사이드바 필터* 가 아니라 *뷰(View)* 에만 subset 반응
- 서버 라우트 `/embeddings/plot`(`fiftyone/server/routes/embeddings.py`)은 `view = get_view(stages=data["view"], filters=data["filters"])` → `results.use_view(view)` → **뷰에 속한 포인트만(`_curr_points`)** 반환.
- 그러나 **App 클라이언트는 사이드바 `filters` 를 이 라우트로 전송하지 않는다**(`/embeddings/selection` 로만 보내 opacity greying). → **왼쪽 사이드바에서 필터하면 그리드만 줄고, 임베딩 패널은 전체 유지**(1.17 한계, 이후 changelog 에도 fix 없음).
- 따라서 **임베딩 패널을 특정 데이터로 좁히려면 반드시 "뷰"(상단 View bar 스테이지 / Saved View)로** 걸어야 한다. (live 증명: project=source-d 뷰 → 패널 2,136 포인트만, 사이드바 필터 → 변화 없음)

### 5.2 단일 프로젝트로 좁히기 — Saved View
좌상단 **Saved Views 드롭다운** → `proj: <name>` 선택 (프로젝트별 21개 사전 생성됨, §7.3). → 그리드+패널 동시 subset.

### 5.3 여러 프로젝트로 좁히기 — View bar `MatchTags` 스테이지 (Saved View 불필요)
Saved View 는 단일 선택이라 union 불가. 임의 조합은 **MatchTags 임시 스테이지**로(저장뷰 아님):
1. 상단 **View bar** 빈칸 클릭 → `MatchTags` 타이핑 → 뜨는 `MatchTags` 클릭
2. 첫 칸(placeholder `list,of,tags`)에 **프로젝트명 쉼표 구분 free-text** 입력: `source-d,ax_project`
   - ⚠️ 자동완성 드롭다운 **없음**(free-text). 프로젝트명을 정확히 입력.
3. **★ 스테이지 끝의 submit 화살표(→) 버튼 클릭** ← **이게 적용 트리거. Enter 로는 적용 안 됨!**
   → Unsaved view 에 그 프로젝트들만(예: 8,276), 임베딩 패널도 그것만.
- `all` 칸은 기본 `False` 유지 = "이 중 하나라도"(union). `True` 면 0건.
- ⚠️ **빈 태그로 commit 금지**(크래시, §6). 지울 땐 태그 비우지 말고 스테이지 통째 **X**.
- 순수 기본기능만(태그 없이)으로 동일: `Match` 스테이지에 `{ "project": { "$in": ["a","b"] } }`. ⚠️ `$eq` 아님(`$eq`는 값 1개 비교 → 배열/3인자 주면 0건 또는 오류).

### 5.4 카테고리별로 보기 — Color by + 범례 토글
1. 임베딩 패널 상단 **"Color by" → `detection_class`**(또는 `normalized_class`) 입력→Enter. → 포인트가 카테고리 색, 우측에 **카테고리 범례**(none/person/smoke/fire/fallen person/patient).
2. **범례에서 카테고리 토글**(Plotly 표준):
   - **더블클릭** = 그 카테고리만 격리
   - **단일클릭** = 카테고리 켜기/끄기(여러 개 조합)
   - 켜진 항목 더블클릭 = 전체 복원
- 범례 토글은 **패널 로컬 상태**(뷰/서버 무관, 크래시 없음, 타 세션 무영향).

### 5.5 완성 워크플로 — "여러 프로젝트 안에서 카테고리별 임베딩 분포"
| 단계 | 조작 | 효과 |
|---|---|---|
| ① 프로젝트 제한 | View bar **MatchTags**(`a,b` + → 버튼) | 그 프로젝트들만 |
| ② 카테고리 색 | Embeddings **Color by → `detection_class`** | 카테고리별 색 + 범례 |
| ③ 카테고리 선택 | **범례 더블/단일클릭** | 원하는 카테고리만 |

> 색을 카테고리에 써야 하므로, 프로젝트 제한은 ①(뷰)이 담당. "Color by project + 범례"는 색을 project 가 차지해 카테고리를 못 보므로 이 목적엔 부적합.

### 5.6 기타
- **텍스트→이미지 검색**: Streamlit 대시보드(:8503) "텍스트 검색" 탭(pgvector, Vertex KO→EN 번역). FiftyOne App 네이티브 텍스트검색은 별도 brain run 필요(현 frames 미빌드).
- **이미지→이미지 유사**: FiftyOne "Sort by similarity"(img_sim brain run).
- **lasso → 그리드**: 패널에서 영역 lasso → 그리드가 그 샘플로. 대용량 효율엔 `create_index` 권장(현재 미적용, 정상 동작).

---

## 6. 크래시 & 복구

**증상**: App 전체가 `TypeError: Cannot read properties of undefined (reading 'stack')` (`ErrorDisplayMarkup`) 로 흰/검은 에러 화면 + "Reset" 버튼.
- **원인**: 빈/invalid view-bar 스테이지(빈 `MatchTags`=placeholder `list,of,tags`, 또는 잘못된 `Match` JSON 예 `$eq`로 배열비교·괄호오타)가 임베딩 패널과 겹치면, 서버가 에러를 돌려주고 **1.17 의 에러 렌더러 자체가 또 깨지는 이중폴트**. 서버 로그엔 traceback 없음(순수 프론트엔드).
- **이 상태(unsaved view)는 서버 공유 세션이라 다른 클라이언트/탭에도 동일 크래시** 전파됨.
- **복구**: "Reset" 버튼은 같은 bad state 재로드라 효과 없을 수 있음. → **Saved View 를 한번 로드**하거나 `http://10.0.0.10:5153/datasets/frames?view=proj-source-d` 처럼 **`?view=<slug>` URL 로 이동**하면 그 bad unsaved-view 가 정상 뷰로 교체돼 복구됨. 이후 plain `/datasets/frames` 도 정상.
- **예방**: View bar 스테이지는 **유효한 값으로만 commit**. MatchTags 는 태그 채운 뒤 → 버튼. 다중값은 `$in` 사용.

---

## 7. 데이터셋 운영

### 7.1 풀 빌드 (전체 188K 재구축, OOM 방지 배치)
`fiftyone_full_build.py` — 미디어 ThreadPool 병렬DL + 청크 add + 라벨 bulk `set_values` + UMAP 샘플-fit(5만)→배치 transform. 임시셋 `frames_full` 에 빌드 후 rename swap(다운타임 최소). 적재 수 조절 = env `FFB_DATASET`/`FIFTYONE_FRAMES_LIMIT`. UMAP 재개만 = `fiftyone_umap_only.py`. **dagster 무관, 앱만 relaunch**.

### 7.2 라벨/프로젝트 갱신 (앱 재기동 불필요, in-place)
- `refresh_frames_labels.py` (prod cron): SAM3 detection/카테고리 갱신 → FiftyOne `set_values`. COCO 통합 등 라벨 변경분 반영.
- `attach_project(ds)` (in `fiftyone_pgvector.py`): `project` 필드 bulk `set_values` + **`make_project_saved_views(ds)` 자동 호출**.

### 7.3 프로젝트 Saved View + 태그 (멀티 프로젝트 선택 인프라)
`fiftyone_pgvector.make_project_saved_views(ds)` (idempotent, `attach_project`/`build_fiftyone_dataset` 에서 호출):
- **Saved View** `proj: <name>` ×21 — 단일 프로젝트 드롭다운 선택용.
- **sample tag** `<name>` (전 프레임에 프로젝트명 태그) — View bar `MatchTags` 다중선택용. ⚠️ `MatchTags`(기본기능)는 **태그**를 매칭하는데 `frames` 는 원래 sample 태그가 없어 이 태깅이 필요(project 는 field 일 뿐). 재빌드 시 자동 재생성, 결손 시 수동 재실행:
```bash
docker exec docker-analysis-1 sh -lc 'cd /workspace && python3 -c "import fiftyone as fo, fiftyone_pgvector as fp; fp.make_project_saved_views(fo.load_dataset(\"frames\"))"'
```

### 7.4 코드 배포 (host ↔ live /workspace drift)
- 실행 코드 = 컨테이너 `/workspace/*.py` (라이브 cp 분). host `docker/analysis/*.py` 와 **drift 가능** → 실행 코드 확인은 컨테이너 파일을 봐야 함.
- live hot 배포: `docker cp docker/analysis/X.py docker-analysis-1:/workspace/X.py`. 메인 streamlit/launcher 스크립트는 새 rerun 이 재컴파일(hot-reload), **import 된 무거운 모듈(fiftyone_pgvector)은 프로세스 relaunch 필요**.
- 영구화(이미지 bake): §3.4. ⚠️ host 변경은 commit 안 하면 다음 CI 배포 `git reset --hard` 로 소실(`docs/` 같은 untracked 신규파일은 보존되나 tracked 는 reset).

---

## 8. 트러블슈팅

| 증상 | 원인 | 조치 |
|------|------|------|
| `:5153`/`:8503` 연결거부(000) | 컨테이너 recreate/reboot 로 프로세스 죽음(자동복구 안 됨) | §3.1/§3.2 relaunch |
| App 통째 크래시(`...'stack'`) | 빈/invalid view-bar 스테이지 + 패널 | §6 — Saved View/`?view=` URL 로 복구 |
| 사이드바 필터해도 임베딩 패널 안 줄음 | 1.17 한계(사이드바 filters 미전파) | §5.1 — 뷰(MatchTags/Saved View)로 |
| `/datasets/frames` 0건 / 이미지 안 뜸 | MINIO_ENDPOINT 가 브라우저 미도달(`minio:9000`) | `ANALYSIS_MINIO_ENDPOINT=http://10.0.0.51:9000` |
| 대시보드 한국어 검색 번역 안 됨 | Vertex env 미주입(relaunch 시) | §3.2 env 포함 relaunch |
| recreate 후 대시보드 구버전/번역 사라짐 | stale 이미지로 롤백 | §3.4 — 이미지 재빌드 후 recreate |
| MatchTags 입력해도 적용 안 됨 | Enter 만 눌러서(commit 안 됨) | 스테이지 끝 **→ 버튼** 클릭 |
| Saved View/태그 새로 만들었는데 App 에 안 보임 | App 캐시 | 브라우저 하드리프레시 or `fiftyone_relaunch.py` |
| `labels`/카테고리 0 (staging) | staging image_labels=0 | 정상(검출 데이터 부재) |

---

## 9. 함정 요약 (Gotchas)

- compose `command` = Jupyter 만. FiftyOne/Streamlit 은 수동 `docker exec`(restart 마다 죽음).
- `pkill python` 금지(공존 프로세스 학살). `/proc` 열거 + `os.kill`, fiftyone server 는 SIGKILL.
- recreate = stale 이미지 롤백(라이브 코드+google-genai 소실). 재빌드 선행.
- 사이드바 필터 ≠ 임베딩 패널 subset. 뷰로만.
- Vertex 번역 env 는 ephemeral(relaunch 마다 재주입). creds/genai 는 writable layer(restart 생존, recreate 소실).
- `user` 유저 NAS 쿼터 → 호스트 직접 cp/mkdir "할당량 초과". docker root 경유.
- 서버 공유 세션: View/Spaces 상태는 모든 클라이언트 공유(한 명 깨지면 다 깨짐). 범례 토글·Color by 는 패널 로컬.

---

## 10. 부록 — 스크립트 레퍼런스 (`docker/analysis/`)

| 스크립트 | 용도 |
|---------|------|
| `fiftyone_relaunch.py` | 기존 `frames` 로 App 만 재기동(재빌드 X) |
| `fiftyone_prod_launch.py` | 풀 빌드 + App 기동(무거움) |
| `fiftyone_full_build.py` | 전체 188K OOM-safe 배치 빌드(임시셋→rename swap) |
| `fiftyone_umap_only.py` | UMAP 투영만 재계산/재개 |
| `fiftyone_pgvector.py` | 헬퍼 모듈(build/attach_*/make_project_saved_views/검색/DQ 등) |
| `embedding_dashboard.py` | Streamlit 대시보드 메인 |
| `refresh_frames_labels.py` | 라벨/카테고리 in-place 갱신(cron) |
| `refresh_caption_img_sim.py` | caption↔image 정합 갱신 |
| `backfill_prod_embeddings.py` | standalone 임베딩 backfill(dagster 비의존) |
