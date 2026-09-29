# 프로젝트 업로드 번들 표준 (v1)

외부 작업자가 **이미지 + 이미지 임베딩 + 프롬프트(문장) 임베딩 (+ 선택 GT)** 를 표준 번들로
업로드하면, sourcei 와 동일한 FiftyOne 뷰(이미지 스캐터 + 문장 스캐터 + 버전 선택 + 성능 필드)를
프로젝트(=데이터셋) 단위로 만들어 주는 킷의 **단일 진리 스펙**이다.

- GT 없음 → **좌표만**: 데이터셋 2개 + emb_viz + GT-free 채점(pred/margin/wins/adopted)
- GT 있음 → **좌표+성능**: 위에 더해 `ground_truth`/`pred_correct_*`/`match`/`purity` 계열

모든 스크립트는 analysis 컨테이너(`docker-analysis-1`) 안에서 실행한다 (torch 불필요, GPU 불필요).

---

## 1. 저장 경로 표준

| 항목 | 컨테이너 | 호스트 |
|---|---|---|
| 업로드 루트 | `/data/fiftyone/uploads/<데이터셋이름>/` | `docker/data/fiftyone/uploads/<데이터셋이름>/` |
| 채점/리포트 산출물 | `<번들>/_artifacts/` | 〃 |

- FiftyOne 은 이미지를 **복사 없이 제자리 참조**한다 → 인제스트 후 번들 디렉토리를 지우거나
  옮기면 데이터셋이 깨진다. 번들 위치가 곧 미디어 정본 위치다.
- **삭제**: 그래서 번들과 데이터셋은 **한 쌍으로** 지운다 — `/__upload/ui` 목록의 체크박스 →
  `선택 삭제`(확인창이 지워질 데이터셋까지 먼저 보여준다), 또는
  `python3 project_upload/delete_bundle.py <이름>... --apply` (기본 dry-run).
  대상은 이름이 아니라 marker(`upload_kit.bundle`) 로 고른다 — `--name` 으로 데이터셋 이름을
  바꿔 넣었어도 잡히고, marker 없는 데이터셋(sourcei·frames)은 동명이어도 안 지운다.
  PG 에 등록한 프롬프트 뱅크(`register_bank_db.py`)는 전역 레지스트리라 **남는다**.
- **반입 경로 ⓪ FiftyOne 앱 안 (가장 짧음, ≤`APP_MODAL_UPLOAD_MAX_MB` 기본 256MB)**: 그리드 툴바
  `프로젝트 번들 임포트` 창의 zip 입력. 파일이 base64 로 **좌석 프로세스를 통과**하므로(FiftyOne 파일
  입력의 구조적 제약 — 파일 크기의 ≈3.4배가 순간 점유) 상한이 있다. 업로드된 zip 은 좌석이
  analysis-sync `PUT /upload/archive` 로 넘겨 ①과 **같은 해제·검증 코드**를 탄다. 상한 초과는 ①로.
- **반입 경로 ① 브라우저 페이지(대용량)**: FiftyOne 주소 뒤에 `/__upload/ui` (예 `http://10.0.0.10:5153/__upload/ui`).
  번들 디렉토리를 **zip 하나**(루트 직치 또는 최상위 폴더 1개)로 올리면 analysis-sync 가 raw 스트리밍으로
  받아 `UPLOAD_ROOT/<이름>/` 에 해제(경로 탈출·심볼릭링크 거부, 상한 `UPLOAD_MAX_BYTES` 기본 20GiB)하고
  즉시 검증 결과를 보여준다. 같은 페이지에서 임포트 시작·진행 확인·완료 후 데이터셋 링크.
- **반입 경로 ② 네트워크 드라이브 / 컨테이너 경유(초대형)**: 호스트 Samba 가 `/home/user` 를 `[user]` 로 공유하므로
  업로드 루트는 PC 에서 `\\10.0.0.10\user\work_p\Datapipeline-Data-data_pipeline\docker\data\fiftyone\uploads\<이름>` 로
  보인다(user 계정). 번들 **폴더**를 그대로 복사하면 `/upload/bundles` 목록에 뜬다 — 복사가 끝난 뒤 검증할 것
  (도중엔 파일 누락으로 fail-closed). 이를 위해 `uploads/` 만 `chown 1000:1000`(2026-09-04) 해 두었다 —
  부모 `docker/data/fiftyone/` 은 여전히 root 소유. 컨테이너가 만드는 `<번들>/_artifacts/`·zip 경로로 해제된
  번들은 root 소유라 PC 에서 삭제/개명은 안 된다(`docker exec docker-analysis-1 rm -rf ...`). 서버 셸에서는:
  ```bash
  docker cp <로컬번들디렉토리> docker-analysis-1:/data/fiftyone/uploads/<데이터셋이름>
  ```
  NAS 공유(`\\10.0.0.51\data`)는 analysis 스택 컨테이너에 마운트돼 있지 않아 **업로드 루트로 못 쓴다**
  (compose 마운트 추가 + 좌석 재생성이 필요 — 접속 세션이 끊기므로 필요할 때만).
- **반입 경로 ③ URL 가져오기 (서버가 직접 내려받음)**: `/__upload/ui` 의 "1-B" 카드 또는 오퍼레이터 창의
  `zip URL` 칸. `POST /upload/fetch {url, name?, overwrite?}` → `202 {job_id}` → `GET /upload/job?job_id=` 폴링
  (`phase`/`downloaded`/`total`, 완료 시 `result` 가 ①의 200 본문과 동일). 바이트가 좌석을 안 거치므로
  **크기 제한이 없고**(상한은 ①과 같은 `UPLOAD_MAX_BYTES` 20GiB), 해제·경로검사·이름규칙·원자설치·검증은
  **①과 완전히 같은 코드**(`_install_archive`)를 탄다. `_busy` 를 점유하지 않아 다운로드 중에도 임포트가 된다.
  - 되는 URL: **사내** http/https 직링크(파일서버, MinIO presigned — 쿼리 서명 OK). 커스텀 인증 헤더는 미지원.
    로그인 페이지를 돌려주는 링크는 첫 청크의 `PK` 시그니처 검사에서 즉시 실패한다.
  - **주소 정책(SSRF 방어)**: 이 엔드포인트는 ①과 마찬가지로 무인증이고 컨테이너는 사내·인터넷 어디로든
    나갈 수 있으므로 주소로 막는다 — http/https만, 포트 allowlist(`UPLOAD_FETCH_ALLOW_PORTS`, 기본 80/443/9000/9001),
    **CIDR allowlist(`UPLOAD_FETCH_ALLOW_CIDRS`, 기본 `10.0.0.0/16`) 안의 주소만 허용**하고 그 밖은 공인 IP 포함
    전부 403. 루프백·링크로컬(169.254.169.254)·도커 브리지(172.x)는 기본값에 없으므로 자동 차단.
    리다이렉트는 따라가되 **홉마다 재검증**(최대 `UPLOAD_FETCH_MAX_REDIRECTS`=5), DNS 는 해석된 **모든** 주소가
    허용 대역이어야 통과. 실패해도 **원격 응답 본문·헤더는 기록하지 않는다**(상태코드·바이트 수만) —
    기록하면 무인증 사내 GET 프록시가 된다.
  - env: `UPLOAD_FETCH_ENABLED`(0 이면 기능 차단), `_ALLOW_CIDRS`, `_ALLOW_PORTS`, `_CONCURRENCY`(기본 1),
    `_CONNECT_TIMEOUT_S`(10), `_READ_TIMEOUT_S`(60), `_MAX_SECONDS`(7200), `_MAX_REDIRECTS`(5).
  - ⚠️ `sync_api.py` 는 uvicorn `--reload` 없이 뜬다 → 이 엔드포인트를 고치면
    **`docker restart docker-analysis-sync-1`** 필요. `upload_ui.html` 은 요청마다 읽어 새로고침만으로 반영.
- `/nas/data/incoming` 에는 **절대 넣지 않는다** (auto-bootstrap 이 CCTV 원본으로 오인 수집).
- 업로드 루트 밖의 번들로 인제스트하려면 `--allow-external-media` 를 명시해야 한다 (e2e 용).

## 2. 번들 구조

```
<데이터셋이름>/
  manifest.json            # 필수
  images/                  # 필수 — jpg/png, 하위 폴더 허용
  image_embeddings.npz     # 필수 — keys: key(str N), vec(float32 N×D)
  prompts.csv              # 필수 — UTF-8, 헤더 version,class,text
  prompt_embeddings.npz    # 필수 — keys: vec(float32 M×D), prompts.csv 행 순서와 1:1
  gt.csv                   # 선택 — 헤더 key,class[,camera]  (있으면 좌표+성능 모드)
```

### 2.1 manifest.json

```json
{
  "format_version": 1,
  "dataset": "my-project",
  "model_name": "PE-Core-L14-336",
  "embedding_dim": 1024,
  "gt_mode": "auto"
}
```

| 필드 | 필수 | 규칙 |
|---|---|---|
| `format_version` | ✅ | 정수 `1`. 다르면 fail |
| `dataset` | ✅ | `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$` (ASCII). `-prompts` 로 끝나면 안 됨(예약). CLI `--name` 이 오버라이드 |
| `model_name` | – | 정보성. 기본 `"unknown"`. 이미지·프롬프트 벡터는 **같은 인코더** 출력이어야 함 (다르면 코사인 무의미 — 킷은 검증 불가, 업로더 책임) |
| `embedding_dim` | – | 기본 1024. 두 npz 의 D 와 일치해야 함 |
| `gt_mode` | – | `auto`(기본)·`csv`·`folders`·`none`. `auto` = gt.csv 있으면 `csv`, 없으면 `none`. `folders` = `images/<class>/…` 폴더명이 GT (gt.csv 불필요) |

### 2.2 image_embeddings.npz

- `key`: str 배열 [N] — `images/` 기준 **POSIX 상대경로** (예: `cam1/0001.jpg`). 중복 금지.
- `vec`: float32 [N, D] — L2 정규화 권장(인제스트가 방어적으로 재정규화). NaN/Inf/영벡터 금지.
- `key` 가 가리키는 파일은 `images/` 안에 실재해야 함 (없으면 **fail**).
  `images/` 에 있으나 임베딩이 없는 이미지는 허용(경고) — 그 이미지는 **샘플 자체가 생성되지
  않는다** (데이터셋 = npz key 목록 기준).

### 2.3 prompts.csv + prompt_embeddings.npz

- CSV 헤더: `version,class,text` (UTF-8, 콤마, 표준 csv 인용). 행 = 문장 1개.
- `version`: `^[A-Za-z0-9][A-Za-z0-9.]{0,31}$` — **영숫자와 점만** (하이픈·언더스코어 금지:
  필드 접미사 변환이 정본 패널의 역해석과 갈라져 조인이 조용히 깨짐. 예: `v2.0-beta` ❌ →
  `v2.0.beta` ✅). 여러 버전 허용. **버전별로 행이 연속(그룹핑)** 되어야 함.
  버전당 문장 수 < 100,000 (gidx 블록 한계, 초과 시 fail).
- `class`: `^[A-Za-z0-9_][A-Za-z0-9_]{0,31}$` — **영숫자와 언더스코어만** (점·하이픈 금지:
  `cos_best_<class>` 필드명 충돌 방지). GT 클래스와 같은 표기 사용.
- `prompt_embeddings.npz` 의 `vec` float32 [M, D] 는 **CSV 행 순서와 1:1** (행 순서가 유일한
  대응 키 — 공급자 CSV 관례와 동일). M ≠ CSV 행수면 fail.
- 서로 다른 버전 태그가 필드명 sanitize 후 충돌하면 fail (예: `v1.0.8` vs `v10.8` → 둘 다 `v108`).

### 2.4 gt.csv (선택)

- 헤더: `key,class` 또는 `key,class,camera`. `key` = image_embeddings.npz 의 key 와 동일 표기.
- 일부 이미지에만 GT 가 있어도 됨(경고) — GT 없는 프레임은 `match='no_gt'` 처리, purity 집계 제외.
- gt.csv 의 key 가 임베딩 key 집합 밖이면 fail. class 는 prompts.csv 의 class 집합 밖이면 경고
  (그 클래스는 어떤 문장도 못 이기므로 항상 오답 처리됨을 리포트에 명시).

## 3. 산출물 (FiftyOne)

### 3.1 이미지 데이터셋 `<dataset>` — 항상

| 필드 | 타입 | 비고 |
|---|---|---|
| `upload_key` | str | 번들 key (조인/디버그) |
| `embedding` | list[float] | L2 정규화된 D-차원 (sourcei 관례와 동일 필드명) |
| brain `emb_viz` | – | 수동 UMAP(2D, cosine, seed 42) → `compute_visualization(view, points=…)` ID-keyed |
| `ground_truth` | Classification | **GT 모드만** |
| `camera` | str | gt.csv 에 camera 열 있을 때만 |

버전별 채점 필드 (GT-free 포함, 정본 `stage_attach` 미러 — `vt`=`version.replace('.','_')`,
`vtag`=`"v"+"".join(version.lstrip("vV").split("."))`):

| 필드 | 타입 | 모드 |
|---|---|---|
| `pred_<vt>` | Classification(label=예측클래스, confidence=best cos) | 항상 |
| `top_prompt_<vt>` | str (승자 문장 원문) | 항상 |
| `pred_margin_<vtag>` | float (클래스 top1−top2; 클래스가 1개뿐인 버전은 best cos 로 정의) | 항상 |
| `winner_gidx_<vtag>` | int (전역 gidx) | 항상 |
| `pred_correct_<vtag>` | Classification(correct/wrong) | GT 모드만 |
| `cos_best_<class>` / `attached_bank` | float / Classification | 항상 — **첫 버전(또는 `--attach`)에만** |

### 3.2 문장 데이터셋 `<dataset>-prompts` — 항상

샘플 = (버전 × 문장). `filepath` = 그 문장의 **최근접 프레임 실제 이미지** (코사인 argmax).

| 필드 | 타입 | 모드 |
|---|---|---|
| `text` / `category` / `bank_version` | str / Classification / Classification | 항상 |
| `gidx` | int = 블록×100000+로컬행 (블록 = CSV 등장 순서) | 항상 |
| `sentence_embedding` | list[float] | 항상 (OPT 관례 — user-embeddings 재계산 지원) |
| `wins` | int (그 버전에서 이 문장이 이긴 프레임 수) | 항상 |
| `adopted` | Classification(채택/미채택 = wins>0) | 항상 |
| `nearest_key` | str | 항상 |
| brain `emb_viz` | 문장 벡터 UMAP, ID-keyed | 항상 |
| `nearest_gt` | Classification(label=최근접 프레임 GT — GT 없는 프레임이면 `"no_gt"`, confidence=cos) | GT 모드만 |
| `match` | Classification(hit/miss/no_gt) | GT 모드만 |
| `purity` / `purity_tier` | float / Classification | GT 모드만, wins>0 이고 GT 있는 승리 프레임 존재 시 |
| `n_cameras` | int | GT+camera 열 있을 때만, wins>0 |

### 3.3 불변식 (채점 후 assert — 위반 시 비정상 종료)

- 버전별 `sum(wins) == 채점된 프레임 수` (완전분할 — `prompt_scores_export.py` run 단위 계약과 동일)
- `winner_gidx_<vtag>` 값은 그 버전 gidx 블록 `[블록×100000, 블록×100000+문장수)` 안
- emb_viz points 수 == 임베딩 보유 샘플 수

### 3.4 datasets 공통

- `persistent=True`, `ds.info["upload_kit"] = {"format_version":1, "bundle": <경로>, "gt_mode": …,
  "fingerprint": <이미지 key/벡터+문장 행/벡터의 sha256[:16]>}`
- **재채점 무결성**: `score_bundle.py` 재실행은 marker 의 `fingerprint` 와 현재 번들 지문이
  일치할 때만 진행 — 이미지/문장(npz·csv) 변경·재정렬·교체는 거부. **gt.csv 변경만 허용**
  (지문에 GT 는 포함되지 않음). 이미지/문장 변경 반영은 `--overwrite` 재인제스트.
- `--overwrite` 는 **`upload_kit` marker 가 있는 데이터셋에만** 허용 (sourcei/frames 등 기존 자산
  보호). marker 없는 이름과 충돌하면 무조건 거부. 삭제 전 저장된 뷰·워크스페이스 개수가 있으면
  stdout 에 경고를 남긴다(둘 다 백업 없이 함께 삭제됨 — §6).
- `<dataset>` 과 `<dataset>-prompts` 는 한 쌍으로 생성/삭제.
- **`compare` 워크스페이스**: 양쪽에 저장(`--skip-viz` 면 생략 — 패널이 emb_viz 좌표를 읽음). 구성은
  `fiftyone_app_setup._compare_space` 재사용(프레임=좌 Samples/image_embeddings·우 user_prompt_compare,
  문장=우 image_embeddings). `user_default_workspace`(on_dataset_open) 가 **이 이름**을 찾아 데이터셋을
  열 때 기본 화면으로 띄운다 — 없으면 App 기본(Samples 단독)으로 열린다.
- **채점 진행 상태**: `upload_kit["scoring"] = {"state": "running"|"ok", "started_at"/"finished_at",
  "attach", "versions", "fingerprint"}`. `score_bundle.py` 는 버전별 루프 진입 **직전** `state="running"`
  을 찍고, 전 버전 채점이 성공한 뒤에만 `state="ok"` 로 덮어쓴다 — 중간 실패(OOM/kill 등)로 루프가
  끊기면 `state="running"` 이 그대로 남아 "부분 채점됨, 재채점 필요"를 marker 만으로 판정할 수
  있다. `<dataset>`/`<dataset>-prompts` 양쪽에 동일하게 기록.

## 4. 실행

```bash
# 1) 검증만 (읽기 전용, exit 0/1)
docker exec docker-analysis-1 python3 /workspace/project_upload/validate_bundle.py \
    /data/fiftyone/uploads/<dataset>

# 2) 인제스트 (검증 → 데이터셋 2개 → emb_viz → compare 워크스페이스 → 채점(GT 자동 감지))
docker exec docker-analysis-1 python3 /workspace/project_upload/ingest_bundle.py \
    /data/fiftyone/uploads/<dataset> [--name X] [--overwrite] [--skip-viz] [--skip-scoring] \
    [--attach <version>] [--allow-external-media]

# 3) 채점만 재실행 (예: GT 추가 후)
docker exec docker-analysis-1 python3 /workspace/project_upload/score_bundle.py \
    /data/fiftyone/uploads/<dataset>
```

인제스트 리포트: `<번들>/_artifacts/ingest_report.json` — 단계별 counts + 경고 전량.
FiftyOne 앱(:5153)에서 헤더 선택기로 `<dataset>` 열면 user 패널 3종이 `<dataset>-prompts` 를
자동 파생 인식한다 (플러그인은 데이터셋 이름 비하드코딩 — 실측 확인됨).

`sync_api.py` (`/upload/ingest`) 경유 인제스트는 subprocess 의 stdout+stderr 전량을
`<번들>/_artifacts/ingest.log` 에도 영속 기록한다(실패/타임아웃 포함 — analysis-sync
재시작으로 in-memory job 이력이 사라져도 이 파일은 남는다). `user-embeddings` 의
"프로젝트 임포트 상태" 오퍼레이터는 job 이력이 없을 때 이 `ingest.log`/`ingest_report.json`
존재만으로 최선노력 상태를 보여주는 폴백을 갖는다.

## 5. 구현 계약 (내부)

- 공용 상수/로더 = `bundle_common.py` **만** 사용 (복제 금지).
- 코사인 = 내적 (양쪽 L2 재정규화 후). 청크: 프레임 1024 × 문장 2048 (정본 `bank_top2_stream`).
  유사도 행렬 전체를 메모리에 만들지 않는다.
- UMAP: `n_neighbors=min(15, N-1)`; N<5 이면 PCA(2) 폴백. `random_state=42, metric='cosine'`.
  brain 등록은 반드시 `ds.select(ids, ordered=True)` view + `points=` (ID-keyed).
- 예측 = argmax_k1 (클래스별 최고 코사인 중 최댓값). margin = top1−top2.
- 실패 정책 = fail-closed: 검증 오류 1건이라도 있으면 인제스트 시작 전에 중단.
  경고는 리포트에 남기고 진행.
- `score_bundle.run_scoring(dataset_name: str, bundle_dir: str, attach: str | None = None) -> dict`
  가 채점 진입점. ingest 가 마지막 단계에서 import 해 호출한다.

## 6. 알려진 한계 (v1)

- `--attach` 를 **클래스 집합이 다른** 버전으로 바꿔 재채점하면 이전 attach 의 `cos_best_<class>`
  필드가 남는다 (값은 이전 버전 것). 새 attach 필드는 정상 — 잔존 필드는 무시하거나 수동 삭제.
- GT 를 나중에 **추가**한 재채점은 지원. GT 를 **제거**(csv→none)한 재채점은 GT 파생 필드
  (`ground_truth`/`pred_correct_*`/`purity` 등)를 지우지 않는다 — 완전 제거는 `--overwrite` 재인제스트.
  이 경우 `score_bundle.py` 는 stdout 경고 + `scoring_report.json["stale_gt_fields"]` 로 marker의
  `gt_mode="none"` 과 실제로 남은 옛 GT 필드가 모순 상태임을 알린다(필드 자체는 지우지 않음).
- `--overwrite` 는 비원자적: 기존 쌍을 먼저 지우고 새로 만든다. 중간 실패 시 옛 데이터셋은
  이미 삭제된 상태 — 번들이 디스크에 있으므로 재실행으로 복구되지만, 그 사이 공백이 있다.
  삭제 대상에 저장된 뷰·워크스페이스가 있으면 함께(백업 없이) 사라진다 — stdout 경고만 남는다.
- 초대형 번들(수십만 장)은 emb_viz 등록의 대형 ID select 가 BSON 16MB 한계에 걸릴 수 있다
  (이 호스트 실측 이력 있음) — 권장 상한 ≤ 200,000장, 그 이상은 분할 업로드.
