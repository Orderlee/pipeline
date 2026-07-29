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
> **prod 재배포마다 컨테이너가 recreate 되므로 배포 후 다시 띄워야 한다.**

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
