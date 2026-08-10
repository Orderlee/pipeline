# analysis 컨테이너 — JupyterLab + FiftyOne + Streamlit (임베딩 시각화/유사검색)

임베딩 파이프라인이 `image_embeddings`(pgvector)에 적재한 1024-d 벡터를 **FiftyOne** 과
**Streamlit 대시보드**로 시각화/클러스터/유사검색하는 분석 surface.

FiftyOne 메타데이터 store 는 **`fiftyone-mongo` 사이드카**(`mongo:7`)를 사용한다.
(번들 mongod 는 slim 베이스에서 미동작 → 2026-06-15 staging 검증 후 사이드카로 전환.
Dockerfile 상단 주석과 `ENV FIFTYONE_DATABASE_DIR` 는 그 시절 잔재이며, compose 가
`FIFTYONE_DATABASE_URI=mongodb://fiftyone-mongo:27017` 로 덮어쓴다.)

## 기동

```bash
# 환경별 profile 활성 + 기동 (prod 예시; staging 은 compose-staging wrapper)
COMPOSE_PROFILES=analysis ./scripts/compose-prod.sh up -d analysis
```

| surface | 컨테이너 포트 | prod 호스트 포트 | 자동 기동 |
|---|---|---|---|
| JupyterLab | 8888 | `8888` | ✅ (compose `command:`) |
| FiftyOne App | 5151 | `5153` (`FIFTYONE_PORT`) | ❌ 수동 |
| Streamlit 대시보드 | 8501 | `8503` (`STREAMLIT_PORT`) | ❌ 수동 |

JupyterLab token = `JUPYTER_TOKEN`, 미설정 시 토큰 없음 — 내부망 전용.

> ⚠️ **JupyterLab 만 자동 기동**한다. FiftyOne 과 Streamlit 은 포트만 열려 있고 프로세스는 안 뜬다.
> **컨테이너 재시작·recreate·호스트 재부팅 때마다 다시 띄워야 한다.**
>
> ⚠️ **배포는 이 컨테이너를 건드리지 않는다.** `deploy-stack.sh` 에 `analysis` 분기가 없고
> 재빌드 트리거 경로에도 `docker/analysis/` 가 없다 (2026-08-10 실측: analysis 컨테이너가
> dagster recreate 를 생존). 그래서 역방향 함정이 있다 — `docker/analysis/**` 는
> `paths-ignore` **밖**이라 이 디렉토리만 고쳐 main 에 push 하면 **dagster 3종만
> stop/rm/recreate 돼 라벨링이 끊기고 analysis 에는 아무 효과가 없다.**
> 분석 변경은 `dev` 로 보내거나 다른 배포와 묶고, 반영은 `docker cp` 로 한다.

```bash
docker exec -d docker-analysis-1 \
  streamlit run /workspace/embedding_dashboard.py --server.port 8501 --server.address 0.0.0.0
docker exec -d docker-analysis-1 python /workspace/fiftyone_prod_launch.py
```

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

- **`/workspace` 코드는 git 과 drift 한다.** 이미지에 COPY 되는 건 `fiftyone_pgvector.py` 와
  `embedding_dashboard.py` 둘뿐이고 (`Dockerfile:16-17`), 나머지 스크립트
  (`fiftyone_prod_launch.py`, `refresh_frames_labels.py`, `unify_coco_categories.py` 등)는
  실행 중인 컨테이너에 수동 복사된 것이다. 수정 사항은 반드시 repo 에도 반영할 것.
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
- **cron 주의**: 호스트 crontab 의 `refresh_frames_labels` 주기 작업은 반드시 `flock` 으로 감쌀 것.
  (2026-07-06 오버랩 3중 중첩 → 스왑 쓰래싱으로 호스트 마비 사건.)

## FiftyOne 플러그인 — Embeddings 패널 Enterprise 게이팅 우회

Embeddings 패널의 `+`(Compute visualization) 버튼은 OSS 에서 **항상** "Upgrade to
FiftyOne Enterprise" CTA 만 띄운다. `APP_MODE="fiftyone"` 이 **빌드타임 상수**라
minifier 가 실제 호출 분기를 지워버린 것이라, env·설정으로는 못 켠다
(`Embeddings-*.js` 에 `to:()=>{setShowCTA(true)}` 만 남아 있음).

하지만 그 버튼이 원래 부르는 건 앱 코드가 아니라 **오퍼레이터**이고, 동일 기능의
OSS 구현이 `@voxel51/brain` 플러그인에 있다. 번들 패치·포크 없이 플러그인 설치만으로 해결된다.

### 설치 (컨테이너 재생성 후 소실 시 재실행)

```bash
# 1) 공식 brain 플러그인 (compute_visualization 등 16개 오퍼레이터)
docker exec docker-analysis-1 fiftyone plugins download \
  https://github.com/voxel51/fiftyone-plugins --plugin-names @voxel51/brain

# 2) 자체 플러그인 (패널 버튼 2종) — repo 소스를 복사
docker cp docker/analysis/plugins/user-embeddings \
  docker-analysis-1:/data/fiftyone/datasets/__plugins__/

docker exec docker-analysis-1 fiftyone plugins list   # 확인
```

- 설치 위치 `/data/fiftyone/datasets/__plugins__/` 는 **bind mount 안이라 컨테이너
  재생성에도 유지**된다 (호스트 `docker/data/fiftyone/`). 다만 gitignore 대상이므로
  `docker/data/` 를 밀면 사라진다 → 위 명령으로 복구.
- FiftyOne App 재시작 불필요. 플러그인 디렉토리를 요청마다 스캔한다.
- 의존성(`umap-learn`, `scikit-learn`, `fiftyone-brain`)은 이미지에 이미 포함.

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
| `captions` | **캡션 텍스트** 임베딩 (`entity_type='caption'`) | 텍스트 공간 지도 |
| `frames` / `frames_full` | **이미지** 임베딩 (`entity_type='frame'`) | 이미지 공간 지도 |

검증법(둘 다 PE-Core-L14-336 1024-d 공유 공간): 저장된 벡터와 그 샘플 caption 을
`fp._embed_text()` 로 재임베딩해 cosine 을 보면 `captions` 는 **1.0000**,
`frames_full` 은 **0.158** 이다. 이름만 같고 모달리티가 다르므로 혼동 주의.

## frames_captions — 이미지+캡션 통합 데이터셋 (2026-07-28)

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

- 설치는 `user-embeddings` 와 동일한 `docker cp` 패턴 (위 설치 절 참조).
- ⚠️ **선행 조건**: `probecache` 스테이지가 만든 `probe_bank_*`/`probe_bar_*` dataset.info·필드가
  없으면 "probe 캐시가 없습니다" 로 거부한다. 먼저 아래를 돌릴 것.

```bash
docker exec docker-analysis-1 nice -n 10 python /workspace/prompt_geometry.py probecache
```

## 스크립트 지도 — 어느 데이터셋이 어디서 나오는가

README 본문은 여러 데이터셋을 전제로 설명하는데, 그것들을 **만드는** 스크립트가 정리돼 있지
않으면 재현이 불가능하다. 진입점은 다음 4개다.

| 스크립트 | 만드는 것 | 스테이지 | 비고 |
|---|---|---|---|
| `prompt_eval.py` | 영상 단위 데이터셋 (871편) | `prompts` `media` `angle` `embed` `score` `dbwrite` `build` `report` `all` | 각 스테이지 멱등, 중단 후 재실행 가능 |
| `frames_eval.py` | 프레임 단위 재라벨 데이터셋 | `scan` `copy` `angle` `embed` `score` `build` `report` `all` | `--limit` 지원 |
| `bank_eval.sh` | 뱅크 **버전 비교** 원커맨드 | `analyze`→`gap`→`flips`→`prune`→`atlas`→`viz`→`guide`→`slim`→`report` | **순서 고정** — 앞 단계 산출을 뒤가 읽는다 |
| `ablate_fields.py` | 절/구/단어 절제 측정 | – | env `AB_PROFILE` `AB_TOPN` `AB_WORDS` `AB_RETRY`. `user-prompt-probe` 와 지표 정의를 공유 |

- `bank_eval.sh` 사용례: `./docker/analysis/bank_eval.sh <기준버전> <신버전> [신버전 CSV경로]`
- `bank_eval.sh` 의 `flips`/`prune` 단계가 만드는 뷰(`30_fixed`/`31_broken`)와 산출물
  `prompt_authoring_guide.md` 는 개별 스테이지만 돌리면 생기지 않는다 — 버전 비교는 래퍼로 돌릴 것.

## frames_captions 프롬프트 뱅크 평가 (frames_bank_eval.sh)

- 전체 사이클: `./docker/analysis/frames_bank_eval.sh` — 매핑이 비어 있으면 0단계(스탬프만)로
  정직하게 끝난다. 도메인을 열려면 `bank_domain_map.yaml` 의 `domains:` 를 노션
  "프롬프트 버전/관리 체계 구축" 페이지 기준으로 시드하고 뱅크 CSV 를 `--bank` 로 등록.
- GT(LS finalized)가 늘었을 때: 재채점 불필요 —
  `frames_bank_ledger.py` → `gtsync` → `report` 만 재실행 (래퍼 주석 참조).
- sourcej GT(patient/person)는 `class_crosswalk` 에 사상을 등재해야 GT 축에 편입된다.
- ⚠️ `slim` 스테이지는 source-h 전용(코드 가드 있음). frames_captions 의 필드 정리는 수동으로만.
- 산출: FiftyOne 필드 6개(bank_*), 뷰 `bank: <도메인> scored/shifted/review-queue`,
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

프레임 관점(`top_prompt_*`, `winner_*`)의 뒤집힌 짝. **점 하나 = 문장 하나**라서 프롬프트를
카테고리별로 보고, 그 문장이 실제로 어떤 이미지에 붙는지 확인하는 용도.

```bash
docker cp docker/analysis/prompt_geometry.py docker-analysis-1:/workspace/prompt_geometry.py
docker exec docker-analysis-1 nice -n 10 python /workspace/prompt_geometry.py promptmap
# → http://10.0.0.10:5153/datasets/source-h-prompts  (워크스페이스 `prompts` 선택)
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
BANK_PROFILE=frames docker exec ... python /workspace/prompt_geometry.py attrs  # frames_captions
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
docker cp docker/analysis/sourcei_build.py docker-analysis-1:/workspace/
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
  `/tmp/symmetric.py:85`가 수행. 분석/Panel 은 `sourcei-prompts` 쪽 필드를 정본으로 읽을 것.
- ⚠️ 이 worktree의 `docker/analysis/prompt_geometry.py`(git HEAD)는 `stage_wave`/`stage_promptmap`
  자체가 없는 별개 버전이다(`git log --all` 에도 부재) — 위 두 스테이지는 컨테이너 `/workspace`에만
  존재하는 git-미추적 코드이므로, 이 필드들의 grep 근거는 반드시 라이브 컨테이너 경로여야 한다.

## FiftyOne App 설정 정본화 (`fiftyone_app_setup.py`, 색상/워크스페이스)

정본 `docker/analysis/fiftyone_app_setup.py` (git). 배포·실행:

```bash
docker cp docker/analysis/fiftyone_app_setup.py docker-analysis-1:/workspace/
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py selftest              # 팔레트 위생 검사
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py colors [ds1,ds2,...]   # 기본: sourcei,sourcei-prompts,source-h,source-h-prompts
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py workspace              # sourcei: rules (Samples | Embeddings, rule_cross 불일치)
docker exec docker-analysis-1 python /workspace/fiftyone_app_setup.py workspace-compare       # sourcei: compare (Samples | Embeddings | Prompt Compare, H1)
```

컨테이너 recreate 시 `/workspace` 는 매번 소실되므로(위 "운영 주의" 절 참고) 재배포 후 위 4개
명령을 다시 실행해야 한다.

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

- 정본 `docker/analysis/plugins/user-prompt-compare/` → 배포:
  `docker cp docker/analysis/plugins/user-prompt-compare/. docker-analysis-1:/data/fiftyone/datasets/__plugins__/user-prompt-compare/`
- 워크스페이스 `compare`(sourcei): Samples | Embeddings | Prompt Compare 3-패널(H1 확정안).
  모드 A=프레임↔문장(argmax_k1 조인, dist_iou 모드는 클릭 무효), 모드 B=같은
  데이터셋 그룹 overlay(frames_captions에서 project 비교).
- selftest(조인 불변식 3개): `docker exec docker-analysis-1 python /data/fiftyone/datasets/__plugins__/user-prompt-compare/__init__.py`
  — FiftyOne 업그레이드 전 필수 게이트. 실패 시 producer drift 의심.
- 색상/워크스페이스 재설정: `python /workspace/fiftyone_app_setup.py colors|workspace|workspace-compare|workspace-fix`
  (`workspace-fix` = 전 데이터셋 워크스페이스 일괄 정규화 — Space>Panel 래핑/active_child=None 레거시가 빈 화면을 만든다. 멱등)
  (컨테이너 recreate 후 재실행 필요 — 이 디렉토리 전체가 그렇듯)
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
