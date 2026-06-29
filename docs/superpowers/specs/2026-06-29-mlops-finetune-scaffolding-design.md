# MLOps 파인튜닝 스캐폴딩 — 구성 방안 (Design Spec)

- **작성일:** 2026-06-29
- **상태:** Draft v4 (v2=Codex+4-렌즈 검수 / v3=PE-Core 승격 §8.1 + MLflow §7.5 / v4=DVC 데이터셋 버저닝 §7.6, Codex 협업 검증) → 사용자 리뷰 대기
- **범위:** **세팅(scaffolding)만**. end-to-end 골격을 완성하되 **실제 학습 실행·prod 가중치 승격은 하지 않음.** 모든 인프라 코드는 CI dev→staging→main 경로.
- **검수 이력:** Codex(OpenAI) 1패스 + 코드주장 검증·ML 타당성·스펙 위생·완전성 4개 렌즈 병렬 리뷰. 각 렌즈 지적을 본 v2에 반영. 코드베이스 주장은 실제 파일 read / `docker exec` 로 검증.

---

## 1. 목표

현재 **추론 전용**으로 쓰는 두 Meta 파운데이션 모델을 도메인(CCTV/보안 영상, fire/smoke 등) 데이터로 파인튜닝할 수 있는 **MLOps 골격**을 깐다.

- **SAM3** — Meta SAM 3.1 multiplex, text-prompt 세그멘테이션/검출. `docker/sam3/app.py` `/segment` (host GPU1).
- **PE-Core** — Meta Perception Encoder `PE-Core-L14-336` (open_clip), 1024-dim 임베딩. `docker/embedding/app.py` `/embed` (host GPU0).

### 확정된 결정 (사용자)

1. 학습셋은 **LS-finalized 라벨 + AL-큐레이션(후 어노테이트) 프레임 둘 다**에서 조립.
2. **둘 다 파인튠** 대상. 단 기본 학습 경로는 **LoRA/PEFT**, 풀파인튠은 `TRAIN_FULL_FT=1` opt-in (16GB 공유 GPU 현실).
3. **수동 트리거 + eval 게이트 + 수동 승격**, prod 박스 GPU.
4. **온디맨드** (스케줄 루프 없음).
5. **세팅만** — 이번 단계는 골격 구축까지. 실제 학습/승격은 다음 단계.

---

## 2. 핵심 원칙 (코드에 안 드러나는 운영 규칙)

- **분리(Decoupling):** 학습 데이터셋 = 라이브 라벨 흐름과 **완전히 분리된 동결(immutable) 스냅샷**. 스냅샷 생성 후 변하지 않음 → 재현성 + 프로덕션 라벨링과 무간섭.
- **자기학습 금지(No feedback loop):** 모델 자기 출력(`review_status='auto_generated'`, Gemini 캡션, `vlm-classification` 폴더 등 **모델 파생 라벨**)으로 절대 학습/평가하지 않음. **사람-확정(LS `finalized`) 또는 AL-선별-후-사람-어노테이트**만 GT로 사용. (⚠️ 예외: box→mask pseudo-label 을 SAM3 시드로 쓰려면 §7.1 의 명시 예외 절차를 따른다.)
- **인프라는 CI, 가중치는 수동:** MLOps 코드/스키마/컨테이너는 dev→staging→main CI. 학습된 가중치의 prod 승격만 사람이 수동으로(레지스트리 게이트 통과 후).
- **레지스트리가 진실:** 어떤 가중치가 서빙되는지의 source of truth는 `model_registry` 행, **파일시스템 심볼릭링크 아님** (CI `rsync --delete` + `git reset --hard`가 untracked 링크/파일을 날림). 단 가중치 파일 자체는 승격 스크립트가 MinIO→호스트 모델 볼륨으로 materialize (§8).
- **정비락은 fail-safe:** GPU 정비모드는 크래시·재배포에도 **자동 해제(auto-release)** 되어야 한다. fail-stuck(영구 정지) 금지 (§9).

---

## 3. 아키텍처 컴포넌트

| # | 컴포넌트 | 신규/재사용 | 역할 | 상세 |
|---|---------|-----------|------|------|
| 1 | `train_dataset_versions` 테이블 | 신규 (PG) | 동결·sealed 학습셋 버전 메타+체크섬+split 배정 | §5.1 |
| 2 | 스냅샷 빌더 asset `defs/train/dataset.py` | 신규 (`split_dataset.py` 브리지) | finalized+AL 후보 → 포맷 변환 → group-aware split → 동결 스냅샷 + 버전 행 | §7.2 |
| 3 | `vlm-trainer` 컨테이너 | 신규, profile-gated, 자동기동 X | SAM3 Hydra 하니스 저작(deps+config tree) / PE-Core open_clip FT. LoRA 기본, **LoRA→full-weight merge** 출력 | §7.3 |
| 4 | Eval + 게이트 asset `defs/train/eval.py` | 신규 (GT-anchored, 신규 mAP 필요) | sealed test split에서 candidate vs incumbent, promotable 판정 | §7.4 |
| 5 | `model_registry` 테이블 | 신규 (PG) | 모델 버전·lineage·metrics·status·checkpoint_key·env_lock | §5.2 |
| 6 | 서빙 로딩 + 승격 `scripts/promote_model.py` + Dagster op | 신규 (만들되 미실행) | MinIO→호스트 materialize + env + container recreate. 롤백 포함 | §8 |
| 7 | GPU 정비락 + 복구 | 신규 (서버사이드 플래그 + guard 센서) | 학습 전 GPU 서빙 unload·drain, 후 warmup. fail-safe 자동해제 | §9 |
| 8 | PE-Core 승격 트랙 | 신규 (재임베딩 asset + model_name별 HNSW + DEFAULT_MODEL 런타임 포인터) | challenger 가 이기면 전체 재임베딩 + 포인터 원자 전환으로 "더 나은 모델로 임베딩" (SAM `.pt` 교체의 PE 대응) | §8.1 |
| 9 | MLflow 실험 추적 | 신규 (profile-gated 서비스, PG backend + MinIO artifact) | 학습 파라미터·데이터셋 버전(=어떤 데이터로 학습)·메트릭·아티팩트 기록·UI 확인. **registry=승격 SoT, MLflow=추적/비교** | §7.5 |
| 10 | DVC 데이터셋 버저닝 + `dataset_catalog` | 신규 (DVC S3 remote=MinIO, 호스트 bare data repo + post-receive, 카탈로그 3테이블) | 엔지니어 큐레이션 데이터셋을 DVC로 버전·**커밋 메시지를 테이블에 기록** → 테이블만 보고 API pull·pin. `_dvc/`(큐레이션) ↔ `_trainsets/`(학습) 레이어 분리 | §7.6 |

> 컴포넌트 1·5(PG 테이블) 스키마 상세는 §5. §7 은 컴포넌트 2·3·4 의 동작, §8 은 컴포넌트 6, §9 는 컴포넌트 7 을 다룬다.

### 재사용 자산 (재발명 금지)
- `datasets`/`dataset_clips` 테이블, `build_dataset` asset 패턴 (`src/vlm_pipeline/defs/build/assets.py`)
- `split_dataset/split_dataset.py` — COCO→YOLO 변환 + `_split_records(records, train_ratio, seed)` (⚠️ per-record 2분할 + 셔플 only — **group-aware/3-way/안정정렬은 신규 브리지에서**, §7.2)
- `sam3_shadow_compare` asset (`defs/sam/assets.py`) + `lib/sam3_compare.py` — **IoU·coverage agreement 하니스 (mAP 미구현, GT 비교 아닌 YOLO-동의도)**. eval 게이트의 **2차 sanity 신호로만** 사용 (§7.4)
- `active_learning_queue()` (`docker/analysis/fiftyone_pgvector.py`) — pgvector HNSW pushdown 후보 선별
- `sam3/train/*` + `sam3/eval/*` — sam3 이미지 site-packages 에 **모듈 파일은 존재**(`train.py`,`trainer.py`,`loss/sam3_loss.py`,`optim/optimizer.py`,`data/sam3_image_dataset.py`). ⚠️ **그러나 turnkey 아님 (2026-06-29 `docker exec` 검증):** ① 학습 deps `hydra-core`/`submitit`/`omegaconf`/`peft` **미설치**(iopath·torch만 존재), ② config YAML **0개**(`configs/` 디렉토리도 없음) — `train.py` 가 `initialize_config_module`+`instantiate(cfg.trainer)` 로 **wheel 밖 config tree 를 요구**. ⇒ "래핑" 아니라 **`sam3.train` 모듈 위에 Hydra 하니스(deps 설치 + config tree 저작)를 작성**하는 작업(§7.3). ✅ box-only 는 데이터 레이어 지원 확인 — `sam3_image_dataset.py` 에 `load_segmentation: bool` + `segment` Optional, `bbox` 상시 → mask 없이 로드 가능(단 loss/model config 저작은 여전히 필요, §7.1)
- 컨테이너 패턴 (sam3/embedding처럼 profile-gated + HTTP), `lib/key_builders.py`, CI 배포
- `stuck_run_guard` 센서 패턴 — 정비락 자동해제 guard 의 모델 (§9)

---

## 4. (기술 현실 요약 — 검수에서 확정된 사실)

설계가 의존하는, 검증된 코드 사실:

- **두 서빙 컨테이너 모두 LoRA/PEFT 로딩 코드 없음.** SAM3 = `build_sam3_image_model(checkpoint_path=<로컬 경로>)` 풀가중치만. embedding = `open_clip.create_model_and_transforms("hf-hub:timm/PE-Core-L-14-336")` 하드코드, **checkpoint override env 전무**. → LoRA 어댑터를 그대로 서빙 불가 ⇒ **학습 끝에 LoRA를 base 에 merge 해 full-weight 체크포인트로 출력**(§7.3), 서빙은 풀가중치 교체만 (§8).
- **두 컨테이너 모두 MinIO 다운로드 코드 없음.** 체크포인트 materialize 는 승격 스크립트 책임 (§8).
- **`SAM3_CHECKPOINT_PATH` 는 컨테이너 로컬 파일 경로** (MinIO key 아님). embedding 은 경로 env 자체가 없음 → `EMBEDDING_CHECKPOINT_PATH` 신규 추가.
- **`image_embeddings` 는 `model_name` 으로 모델 구분** (`UNIQUE(entity_type, entity_id, model_name)`, 모든 read 가 `WHERE model_name=%(model)s`). dim 은 `vector(1024)` 고정.
- **AL/분석 표면은 cross-modal** (`fiftyone_pgvector.search_by_text` → `/embed_text` → frame 임베딩과 cosine). image/text 가 같은 공간에 있어야 함.
- 파이프라인은 **Postgres 로 cutover** 됨 — `duckdb_writer` 태그는 legacy.

---

## 5. 데이터 모델 (신규 마이그레이션)

> PG 마이그레이션 러너의 DO $$ block 한계 (memory `project_postgres_migration_runner_quirk`) — 새 migration 은 DO block 1개 또는 별 파일. 적용 후 `pg_constraint` 직접 확인.

### 5.1 `train_dataset_versions` (동결 스냅샷 — `datasets` 테이블과 분리)

> `datasets` 는 run마다 행 생성하는 live-build 의미라 동결 스냅샷과 섞으면 lineage 위험 → 별 테이블.

```
train_dataset_version_id   PK
created_at                 sealed timestamp
task                       'sam3_detection' | 'pe_core_embedding'
source_spec                JSONB  -- 쿼리/필터 (ls_finalized 조건, al 조인키, train_ratio 등)
class_map                  JSONB
group_key_field            TEXT   -- split group 키 (e.g. source_unit_name / parent asset_id)
split_assignment_key       MinIO key  -- group-aware, deterministic per-record 배정
split_ratios               JSONB  -- {"train":0.8,"val":0.1,"test":0.1}
manifest_key               MinIO key  -- 정렬된 (object_key, per-object checksum) 리스트
content_checksum           TEXT   -- 정렬 manifest + class_map + split + seed 해시
ls_count                   INT
al_confirmed_count         INT    -- 현재 0 가능 — 정직하게 보고
per_class_counts           JSONB  -- 클래스 불균형 추적
total_count                INT
seed                       INT
upstream_dataset_id        FK datasets(dataset_id) NULL
UNIQUE (task, content_checksum)   -- 동일 콘텐츠 재빌드 dedup (§7.2 H6)
```

### 5.2 `model_registry`

```
model_version_id           PK
model                      'sam3' | 'pe_core'
version                    TEXT (e.g. sam3-2026.06.29-lora-001)
train_dataset_version_id   FK train_dataset_versions
train_method               'lora' | 'full_ft' | 'contrastive_lora' | 'linear_probe'
git_sha                    TEXT
training_image_digest      TEXT
training_config            JSONB  -- hparams + seed (split seed 와 별개)
env_lock_key               MinIO key  -- torch/cuda/open_clip/peft/transformers freeze + nvidia driver
eval_config                JSONB  -- margin, 메트릭 정의
metrics                    JSONB  -- candidate eval (per-class 포함)
incumbent_metrics          JSONB  -- 비교 대상
incumbent_source           'promoted' | 'stock_base'  -- 첫 run 은 stock_base (§7.4 H3)
checkpoint_key             MinIO key  -- merged full-weight 체크포인트
artifact_checksum          TEXT
status                     'candidate'|'promotable'|'promoted'|'archived'|'rolled_back'
created_at, promoted_at
promoted_env               'prod'|'staging' NULL
mlflow_run_id              TEXT  -- MLflow run 교차링크 (params·dataset lineage·metrics·artifacts UI, §7.5)
```

### 5.3 `image_embeddings` 버전 분리 — `model_name` 인코딩 방식 채택

> 검수 HIGH-4/D5: 기존 read 가 전부 `model_name` 필터. 새 `model_version` 컬럼은 reader 미수정 시 **write-only 무력화**.

- **채택:** 파인튠 버전을 **`model_name` 값에 인코딩** (e.g. `facebook/PE-Core-L14-336@ft-2026.06.29-lora-001`). 기존 `WHERE model_name=%(model)s` reader 가 자연히 버전 격리 → 코드 변경 최소.
- 기존 ~26만 행은 그대로(`facebook/PE-Core-L14-336`) = stock 버전.
- dim 은 `vector(1024)` 고정 — 재학습 인코더는 **dim 1024 유지 필수** (아니면 신규 컬럼/테이블, §10 deferred).
- (별도 `model_version` 컬럼을 굳이 추가한다면 `fiftyone_pgvector` 검색/AL + `backfill_prod_embeddings` 의 모든 read 를 동시 수정해야 함 — 비권장.)
- **활성 버전 포인터**: AL/검색이 읽는 `DEFAULT_MODEL`(현 `docker/analysis/fiftyone_pgvector.py:23` 상수 `facebook/PE-Core-L14-336`)을 PG 런타임 설정으로 빼서 **원자 전환** 가능하게 한다 — PE-Core 승격(§8.1)의 핵심. `model_name`별 partial HNSW 도 §8.1 에서 신설.

---

### 5.4 `dataset_catalog` 3종 (DVC 큐레이션 인덱스 — §7.6)

> 마이그레이션 **016**. dataset_catalog ↔ train_dataset_versions 순환 FK 회피: 016 이 3 테이블 생성 + `ALTER train_dataset_versions ADD COLUMN dataset_catalog_id`(FK) 까지 수행(013 이후라 양 테이블 존재).

```
dataset_catalog               -- DVC 버전 1개(커밋당 .dvc out당) = 1행
  dataset_catalog_id UUID PK, task, dataset_name,
  status  -- pending | available | pinned | archived | invalid | pending_missing_dvc_objects
  -- git 정체성: data_repo_id, data_repo_url, git_rev, git_short_rev, git_ref, git_tag
  -- 커밋 메시지(핵심 요구): commit_subject, commit_message, commit_author_name, commit_author_email, committed_at, ingested_at
  -- DVC 포인터(.dvc YAML): dvc_file_path, dvc_out_path, dvc_md5, dvc_size_bytes, dvc_nfiles, dvc_remote_name, dvc_remote_url(s3://vlm-dataset/_dvc/…)
  -- 파생 링크: train_dataset_version_id FK(nullable, 스냅샷 빌드 후), content_checksum
  -- MLflow: mlflow_run_id
  UNIQUE(data_repo_id, git_rev, dvc_file_path, dvc_out_path)

dataset_catalog_aliases       -- 가변 pin: PK(task, alias 'current'|'production'…), dataset_catalog_id FK, pinned_by, pin_reason, pinned_at
dataset_catalog_pin_events    -- append-only 감사: event_id, task, alias, dataset_catalog_id, previous_dataset_catalog_id, pinned_by, pin_reason, pinned_at
```
+ `train_dataset_versions.dataset_catalog_id UUID FK NULL` — 큐레이션↔학습 두 레이어 연결.

---

## 6. 스토리지 레이아웃 (MinIO)

> 검수 MED + CLAUDE.md "5개 고정 버킷" 정책 — **새 버킷 안 만들고 prefix 사용.**

- 동결 학습셋: `vlm-dataset/_trainsets/<train_dataset_version_id>/{images,labels,splits,manifest.json}`
- 후보·머지 체크포인트: `vlm-dataset/_models/<model>/<version>/` (merged full-weight + `env_lock.json` + `train_log.jsonl` + `training_summary.json`)
- 승격된 가중치는 승격 스크립트가 호스트 모델 볼륨으로 다운로드 (`./docker/data/models/sam3/`, `./docker/data/models/pe-core/`), 서빙은 로컬 경로 로드 (§8).
- **MLflow** backend store = 동일 Postgres 의 별도 DB `mlflow`, artifact store = MinIO S3-호환 `s3://vlm-dataset/_mlflow/`(prefix, 새 버킷 X; `MLFLOW_S3_ENDPOINT_URL` 로 MinIO 지정) (§7.5).
- **DVC** remote = MinIO(S3 type + `endpointurl`, 기존 자격 재사용, 마운트 불필요) → `s3://vlm-dataset/_dvc/`. 데이터 전용 git = **호스트 bare repo** `/srv/data-repos/dvc-datasets.git`(앱 배포 `rsync --delete`+`reset --hard`와 격리). `.dvc`(YAML) 포인터만 git, 바이트는 MinIO `_dvc/` (§7.6).

---

## 7. 컴포넌트 상세 — 데이터·학습·평가

### 7.1 SAM3 supervision 선결 검증 (구현 전 1순위)

SAM 3.1 의 세그멘테이션 head 는 **마스크 GT 가 있어야 학습**되나, 우리 GT 는 COCO **bbox only** (`split_dataset` bbox만 emit, `image_label_annotations` 는 `bbox_x/y/w/h` 만, 서빙은 text-prompt only).

**검증 결과 (2026-06-29 `docker exec`):**
- ✅ **데이터 레이어는 box-only 지원** — `sam3/train/data/sam3_image_dataset.py` 에 `load_segmentation: bool` 플래그, `segment` 는 `Optional`(없으면 None), `bbox` 는 상시 텐서. `load_segmentation=False` 로 마스크 없이 로드 가능.
- ❓ **미확정(코드 정독 필요):** `loss/sam3_loss.py` + model config 가 mask loss 를 끄고 **detection/box loss 만으로 학습**되도록 구성 가능한지 — config YAML 이 wheel 에 없어(§3) 소스 정독으로 확인.

**분기:**
- (a) box-only loss 구성 가능 → **detection/box head 만** 파인튠 (mask head 동결). 1순위.
- (b) 불가 → **descope**(SAM3 트랙 보류) **또는** box→mask pseudo-label 을 base SAM3 로 생성하되 **§2 예외**로 명시 + 사람 리뷰 시드로만 (자기증류 위험 인지).

확인·결정은 §7.3 config tree 저작과 함께 — 추후로 미루지 않음.

### 7.2 스냅샷 빌더 (`defs/train/dataset.py`)
- **입력:**
  - LS: `v_finalized_labels` / `review_status='finalized'` `image_labels`.
  - AL: **AL 큐로 선별된 프레임 중, 사람이 후-어노테이트하여 `image_label_annotations` 에 확정 박스가 존재하는 것만** (AL-queue 멤버십 ∩ `image_label_annotations`). 현재 `image_label_annotations`=0행이므로 **AL 기여 0** 으로 정직 보고. (`image_labels` 에 288 finalized 행 존재하나 per-box 투영 테이블이 0행 — §10 백필 전까지 SAM3 bbox 기여 0.)
- **처리:**
  1. 후보를 **안정 키(`image_id`/object key)로 정렬** (셔플 입력 순서 비결정성 제거, 검수 H5).
  2. **group-aware 3-way split** — `group_key_field`(source 영상/parent asset_id)로 그룹핑해 한 영상의 프레임이 train/val/test 중 정확히 하나에만 (검수 HIGH-1). `_split_records`(per-record 2분할)는 그대로 못 씀 → `_split_groups(records, key_fn, ratios, seed)` 신규: 그룹키 셔플→3버킷 분배→레코드 확장. split 후 "한 그룹키가 두 split 에 없음" assert.
  3. **클래스 불균형 stratify** — 희소 클래스(fire/smoke)가 train·val·test 각각 최소 N개 포함되도록(아니면 정직 에러로 빌드 실패). `image_label_annotations.category` 로 카운트, `per_class_counts` 기록 (검수 LOW-1).
  4. 포맷 변환: COCO→YOLO(SAM3, `split_dataset.py` 브리지) / (frame, caption) 페어(PE-Core). `train_ratio`/`split_ratios` 는 `source_spec` 에서 (기본 0.8/0.1/0.1).
  5. `vlm-dataset/_trainsets/<ver>/` 에 **동결** 기록 + `train_dataset_versions` 행.
- **idempotency (검수 H6):** 쓰기 전 `content_checksum`(정렬 manifest+class_map+split+seed) 계산 → 동일 `(task, checksum)` 행 있으면 **no-op 반환** (단 `force_new=true` 제외). `_trainsets/<ver>/` 는 원본 복사 대신 **vlm-raw/processed/labels 원본 참조 + 불변성 노트**로 중복 회피 (원본 삭제 정책과 상충 시 복사로 전환 — 구현 시 택1 명시).
- **동시성:** `gpu_trainer`/`pg_writer` 태그 (run_coordinator 에 정의). `duckdb_writer`(legacy) 사용 안 함.

### 7.3 `vlm-trainer` 컨테이너 (`docker/trainer/`)
- profile-gated(`profiles: ["trainer"]`), **자동기동 안 함**.
- **SAM3:** `sam3/train` 모듈 위에 **Hydra 하니스 저작** — trainer 이미지에 `hydra-core`/`submitit`/`omegaconf`/`peft` 설치 + **config tree(trainer/model/dataset/optim/loss + LoRA 주입) 직접 작성**(wheel 에 config YAML 미포함, §3). 어댑터(COCO 스냅샷 → `sam3_image_dataset` 포맷, `load_segmentation=False`, §7.1 결정 반영). 기본 LoRA(타깃 모듈은 **`model.named_modules()` 실측**으로 decoder/backbone attention q/k/v/o; 작은 head 는 풀학습). `TRAIN_FULL_FT=1` → 풀FT(grad checkpointing).
- **PE-Core:** open_clip 파인튠. **기본 = contrastive image+text LoRA** (joint space 정렬 유지 — image-only LoRA 는 cross-modal AL 을 깸, 검수 HIGH-2). `linear_probe` 는 별도 image-image probe head 용도로만(공유 pgvector 공간 미투입). `TRAIN_FULL_FT=1` → contrastive 풀FT(+재임베딩 선결, §10).
- **LoRA→merge:** 학습 종료 시 **LoRA 를 base 에 merge → full-weight 체크포인트 출력** (서빙이 풀가중치만 로드하므로). `peft` 는 trainer requirements 에만 추가, 서빙엔 불필요.
- **trainable-param sanity:** PEFT 어댑터 주입 후 trainable param > 0 (regex miss 로 빈 어댑터 학습 방지) assert.
- **실행/동시성:** Dagster op 가 기동하되 **CI 재배포가 op 를 고아화**(memory `project_prod_deploy_dagster_restart`) → trainer 를 **Dagster run 라이프사이클과 분리된 독립 프로세스**로, 정비모드(§9)에서 배포 보류. 체크포인트→`vlm-dataset/_models/...`. `gpu_trainer` concurrency=1.
- **secrets (검수 M2):** `environment:` 블록에 `HF_TOKEN: ${HF_TOKEN:-}`, `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` (호스트 `.env` expand, 커밋 금지). embedding 은 런타임 HF_TOKEN 불필요했으므로 prod `.env` 에 `HF_TOKEN` 존재 보장 필요 — 트레이너 preflight 에서 누락 시 fail-fast.
- **관측성 (검수 M3):** per-step JSONL(`step,loss,lr,throughput`)을 stdout + `_models/<ver>/train_log.jsonl`. 최종은 `model_registry.metrics` + `training_summary.json`. Dagster op 가 start/heartbeat/end 를 asset metadata+log 로. 선택: `SLACK_WEBHOOK_URL` 알림. **MLflow 도입(§7.5)** — trainer 가 hyperparameter·dataset 버전(어떤 데이터로 학습)·metric·artifact 를 MLflow 에 기록. per-step JSONL/PG 관측은 보완으로 유지.

### 7.4 Eval + 게이트 (`defs/train/eval.py`)
- **SAM3:** sealed test split에서 **GT-anchored AP/mAP** (사람 GT 대비). `sam3/eval`(공식) 경로 우선; 없으면 **신규 mAP 구현 필요** — `sam3_shadow_compare` 는 mAP 미구현 + YOLO-동의도라 **게이트 불가, 2차 sanity 로만** (검수 MED-3/D1/M5).
- **PE-Core:** 게이트 메트릭 = **사람-검수 GT 에 대한 recall@k** (cross-modal text→image 우선). holdout = **§2-준수 LS-finalized (frame, text)/(frame, class) 페어만** (`vlm-classification` 등 Gemini/SAM3 파생은 eval GT 금지, 검수 MED-4). **결정(2026-06-29): proxy 메트릭(AL precision/클래스 분리도 등) 미사용 — 사람-검수 GT recall@k 만.** 현재 쿼리가능 GT≈0(검증: finalized 197 캡션 전부 NULL, `image_label_annotations` 0행) → **게이트는 사람-검수 GT < `eval_config.pe_core_min_gt`(N_min) 이면 abstain = 승격 불가**. 즉 GT 가 §12 백필/리뷰로 쌓일 때까지 **PE 승격은 실질 비활성**(메커니즘은 §8.1 에 구축). 작은 N 구간에선 bootstrap 신뢰구간 함께 기록.
- **첫-run baseline (검수 H3):** promoted 행 없으면 `incumbent_metrics` = **stock/base 모델을 동일 sealed split 에 통과시킨 점수**, `incumbent_source='stock_base'`.
- **게이트:** candidate 가 incumbent 를 **per-metric margin 이상** + **per-class non-regression floor**(평균이 클래스별 퇴행 숨김 방지) 충족 → `status='promotable'`. margin 은 `eval_config` 로 외부화, **기본값은 §11 미해결**.

### 7.5 MLflow 실험 추적 (학습 파라미터 + 데이터 lineage 기록·확인)

> 사용자 요구(2026-06-29): **학습 파라미터 값 + "어떤 데이터로 학습됐는지" 가 저장·확인 가능해야 함.** → MLflow 도입 (v2 의 "MLflow 미도입" 결정 변경).

- **역할 분담(중요):** `model_registry`(PG) = **승격·서빙·롤백의 source of truth**(게이트가 읽음). MLflow = **사람용 추적·비교 UI**(파라미터/메트릭/데이터 lineage 열람·run 비교). trainer 가 둘 다 1회에 기록 → 불일치 없음. **승격 게이트는 MLflow 가 아니라 registry 를 본다.**
- **MLflow 가 기록하는 것** (trainer 가 `mlflow.start_run()` 안에서):
  - **params** — hyperparameter 전부: `train_method`(lora/full_ft/contrastive_lora), lr, epochs, batch, LoRA rank/alpha/target_modules, seed, base model, `TRAIN_FULL_FT` 등.
  - **데이터 lineage** — `mlflow.log_input()`(MLflow Dataset) + params/tags 로 `train_dataset_version_id`·`content_checksum`·`manifest_key`·`ls_count`/`al_confirmed_count`/`per_class_counts`·`split_ratios`. → run 하나 열면 **정확히 어떤 동결 스냅샷(불변, 체크섬)으로 학습했는지** 확인. 스냅샷 manifest 가 개별 object key+checksum 까지 보유(§5.1) = end-to-end lineage.
  - **metrics** — eval 결과(SAM box mAP / PE recall@k, per-class).
  - **artifacts** — `env_lock.json`·`train_log.jsonl`·`training_summary.json`(이미 `_models/<ver>/` 에 있음, MLflow 에도 `log_artifact`).
  - 종료 시 MLflow `run_id` → `model_registry.mlflow_run_id`(교차링크, §5.2).
- **배포 형태:** `mlflow` 서비스 컨테이너, **profile-gated**(`profiles: ["mlflow"]`), 자동기동 X. backend store = 동일 Postgres 의 별도 `mlflow` DB, artifact store = MinIO `s3://vlm-dataset/_mlflow/`(§6). UI 포트 1개 노출.
- **fail-soft:** trainer 의 MLflow 로깅은 **MLflow 서버 unreachable 시 학습을 죽이지 않음**(경고만) — registry 기록이 SoT 이므로 추적 서버 장애가 학습/승격을 막지 않게.
- **스캐폴딩 범위:** 서비스 정의 + trainer MLflow 로깅 코드 + registry `mlflow_run_id` 컬럼(013 에 포함). 실제 run 기록은 학습 실행 시(게이트 뒤).

---

### 7.6 DVC 데이터셋 버저닝 + `dataset_catalog` (엔지니어 대면 큐레이션 레이어)

> 사용자 요구(2026-06-29): DVC로 데이터셋 버저닝, **버전을 테이블에 기록** → 엔지니어가 테이블만 보고 **API pull**, 더 나은 데이터셋 구축 시 **테이블 update로 버전 pin**, **git 커밋 메시지도 테이블에 기록**, MLflow 연동. **Codex 협업 검증 = 조건부 가능**(레이어 분리 시 "중복/2중 진실" 해소).

- **레이어 분리 (중복 방지 핵심 규칙):** DVC/`_dvc/` = 큐레이션 데이터셋 **source-of-truth**(git 포인터 + 커밋 의도). `dataset_catalog`(PG) = **쿼리 가능 인덱스**(커밋 메시지 포함). `_trainsets/`+`train_dataset_versions` = **파생 학습 스냅샷**. MLflow = run 링크. **어느 레이어도 다른 레이어 소유물을 소유하지 않음 — `_trainsets/` 스냅샷을 편집 가능한 큐레이션 데이터셋으로 취급 금지.**
- **저장/위치:** DVC remote = **MinIO**(S3 + `endpointurl`) → `s3://vlm-dataset/_dvc/`. 데이터 전용 git = **호스트 bare repo** `/srv/data-repos/dvc-datasets.git`(배포 경로와 격리). `.dvc`(YAML) 포인터만 git, 바이트는 MinIO. (외장하드 = NAS_200tb = MinIO 백킹과 동일 — 별도 디스크 없음, HDD SPOF 소멸.)
- **라이프사이클:**
  1. 큐레이터: `dvc add <dataset>` → `dvc push`(→MinIO `_dvc/`) → `git commit -m "<의도>"` → bare repo push.
  2. **post-receive 훅** → Dagster ingestion: `git log -1 --format=%H%n%s%n%b%n%an%n%ae%n%cI` + `.dvc`(YAML) 파싱 + **MinIO 객체 존재 검증** → `dataset_catalog` INSERT(`status='available'`, 커밋 subject/body·rev·author·md5). dvc push 누락 → `status='pending_missing_dvc_objects'` + 알림. **스케줄 reconciliation 센서**가 `git log` 스캔으로 누락 backstop(멱등 — `UNIQUE(data_repo_id,git_rev,dvc_file_path,dvc_out_path)`).
  3. 엔지니어: `dataset_catalog` 조회(**커밋 메시지까지**) → `pin()` API(raw UPDATE 아님) → `dataset_catalog_aliases`(task당 alias 1개) 트랜잭션 갱신 + `dataset_catalog_pin_events` append 감사.
  4. 스냅샷 빌더(§7.2): pin된 alias → `dvc get <bare-repo> <out_path> --rev <git_rev>`(MinIO에서 fetch) → split/freeze → `_trainsets/<id>/` + `train_dataset_versions`(FK `dataset_catalog_id` 역링크).
  5. trainer: 동결 MinIO 스냅샷 소비(live DVC 작업본 아님).
  6. MLflow: `dvc_catalog_id`·`dvc_git_rev`·`dvc_commit_subject`·`dvc_md5`·`content_checksum` 등 params/tags + `log_input`(source=`_trainsets/<id>/manifest.json`). **DVC 전용 MLflow DatasetSource 미존재 → 커스텀 안 만들고 tags로**(Codex 확인).
- **API pull(비-DVC 친화):** 테이블 row → `dvc get ... --rev` 또는 thin wrapper(`dataset pull --task sam3 --alias current`)가 rev/path resolve + 스트림 + checksum 검증.
- **스캐폴딩 범위:** 016 마이그레이션(카탈로그 3테이블 + `train_dataset_versions.dataset_catalog_id` FK) + bare repo/post-receive 설정 + dvc remote(S3→MinIO) config + ingestion 센서 + pin API + 스냅샷빌더 `dvc get` 연동 + MLflow 필드. 실제 큐레이션/pull/학습은 운영 시.

---

## 8. 서빙 가중치 로딩 + 승격 (`scripts/promote_model.py` + Dagster op) — 만들되 미실행

> 검수 C1/C2/C3: 두 서빙 다 LoRA·MinIO 로딩 코드 0. embedding 은 checkpoint override 자체 없음.

- **서빙 로딩 방식 = merged full-weight 교체** (LoRA 어댑터 서빙 코드 회피):
  - **SAM3:** 기존 `SAM3_CHECKPOINT_PATH`(로컬 경로) 그대로 — 승격이 새 `.pt` 를 그 경로에 놓고 recreate.
  - **embedding:** `open_clip_be.load()` 에 **`EMBEDDING_CHECKPOINT_PATH` env + `load_state_dict` 분기 신규 추가** (현재 HF Hub 고정, override 전무). model_name 값도 버전 인코딩(§5.3)로 갱신. **이 코드는 이번 스캐폴딩에 포함**(코드만, 기본값 현행 incumbent 유지, 미승격).
- **materialize (검수 C3):** `promote_model.py` 가 `model_registry.checkpoint_key` 를 MinIO→호스트 모델 볼륨 다운로드 + `artifact_checksum` 검증 → env 세팅 → `docker recreate`. 서빙 컨테이너는 MinIO-free 유지(현행과 동일). 승격은 볼륨 있는 prod 박스에서 실행.
- **승격:** `promotable` 행만 선택 → status `promoted`, `promoted_at`/`promoted_env` 기록. **롤백:** 이전 `promoted` 행 재승격 + recreate. 서빙 시작 로그에 resolved 경로 + checksum 출력.
- **레지스트리가 진실:** 심볼릭링크 금지 (§2).

### 8.1 PE-Core 승격 트랙 — champion-challenger via 버전드 dual-index

> 사용자 결정(2026-06-29): PE-Core 도 SAM 처럼 "성능 향상 시 그 모델로 임베딩". PE 는 라벨 산출물이 아니라 **벡터**라(검증됨: `/embed`·`/embed_text` 만, 소비처 AL큐·검색·FiftyOne 모두 코사인), SAM 의 `.pt` 교체에 대응하는 것은 **"AL/검색이 읽을 임베딩 버전(`model_name`) 포인터의 원자 전환"**. 메커니즘은 이번 스캐폴딩에 **구축**, 실제 재임베딩 실행·전환은 SAM 처럼 게이트 뒤.

**대응표:** SAM = 가중치 파일 교체 ↔ PE = 활성 `model_name` 포인터 전환. 옛 버전 벡터/인덱스는 롤백용 보존 → 전환은 포인터 UPDATE 한 번(원자적), 무중단.

**루프:**
1. **학습** = contrastive image+text LoRA→merge (§7.3, joint-space 보존 필수). 새 버전 `model_name = facebook/PE-Core-L14-336@ft-<ver>` (§5.3).
2. **평가** = sealed holdout 을 새 `model_name` 으로 임베딩 → **recall@k (사람-검수 GT only)** vs incumbent (§7.4). **GT < N_min 이면 abstain → 승격 안 함** (현재 GT≈0 → 실질 비활성).
3. **이기면 승격** = (a) **전체 코퍼스 재임베딩** under 새 `model_name` → (b) **`model_name`별 partial HNSW 빌드** → (c) **`DEFAULT_MODEL` 포인터 원자 전환** + 서빙 `EMBEDDING_CHECKPOINT_PATH` 교체+recreate. 옛 `model_name` 벡터/인덱스 보존.
4. **지면** = 포인터 그대로(incumbent), challenger 벡터 폐기/보류.
5. **롤백** = `DEFAULT_MODEL` 을 이전 `model_name` 으로 되돌림(옛 벡터 살아있어 즉시) + checkpoint 되돌림+recreate.

**이번 스캐폴딩에 구축할 4개 (코드만, 실행 게이트 뒤):**
- **(A) `DEFAULT_MODEL` 런타임 포인터** — 상수(`fiftyone_pgvector.py:23`)를 PG 런타임 설정(예: 작은 `active_embedding_model` 행/ `runtime_settings`)으로 빼고 AL/검색이 거기서 활성 `model_name` 을 읽게. 승격 = 이 행 원자 UPDATE. **기본값 = 현 stock `model_name`(무변경)**.
- **(B) 전체 코퍼스 재임베딩 asset** (`defs/embed/reembed.py`) — 새 `model_name` 으로 frame+caption 임베딩. **커버리지 기본 = incumbent 와 동일 집합**(현 frame 187,994 + caption 11,978; 전체 `image_metadata` 525,966 중 일부). 신규 프레임은 평소 embed asset 이 활성 `model_name` 으로 계속. ~수 시간 배치 — 정비 윈도우/백그라운드. **실행은 게이트 뒤**.
- **(C) `model_name`별 partial HNSW 마이그레이션** — 현 008 은 `entity_type`별만. 006 헤더가 예고한 `WHERE model_name=<ver>` partial index 추가. `_OPTIONAL_MIGRATIONS`(pgvector-gated, 비-pgvector prod 부팅 안전).
- **(D) recall@k 게이트 + 사람-검수 GT 로더** (§7.4) — GT < N_min 이면 abstain.

**SAM 과 다른, 받아들일 trade-off:**
- 승격에 **재임베딩+인덱스 빌드 배치(~수 시간)** 추가(SAM엔 없음). 자주 X.
- 게이트는 **사람-검수 (frame,text)/(frame,class) GT 가 쌓여야** 작동(현재 0 → §12 백필/리뷰 선결).
- 학습은 **image+text contrastive 강제**(image-only LoRA 면 text→image 검색 붕괴, 검증됨 §7.3).

---

## 9. GPU 정비락 + 복구 (fail-safe)

> 검수 H1/LOW-3/H2: idle-watcher 가 inbound 요청에 lazy-reload → unload 만으론 레이스. 크래시 시 영구 정지 위험.

- **서버사이드 정비 게이트 (두 app.py 신규):** `/maintenance/enter|exit` (또는 PG/마커 플래그 read). 설정 중 `/segment`·`/embed` 는 `503`, **lazy-reload 거부**(watcher·`_ensure_model_loaded` 가 플래그 확인). 센서 정지만으론 외부/헬스체크 콜러를 못 막으므로 **서빙 레이어에서 권위적**.
- **정비 프로토콜:** (1) GPU 서빙 sensor/job 정지 → (2) active run drain/cancel → (3) 서버 maintenance enter(+`/unload`) → (4) `nvidia-smi` free VRAM 검증 → (5) trainer 기동 → (6) 완료 후 maintenance exit + `/warmup` → (7) sensor 재개.
- **fail-safe 자동해제 (검수 H2):** 정비 플래그에 `owner_run_id`+`entered_at`+heartbeat/TTL. **guard 센서**(`stuck_run_guard` 패턴)가 플래그 stale(heartbeat 사망 or owner run 비RUNNING) 감지 시 자동 해제: sensor 재개 + `/warmup` + 플래그 clear. 수동 `scripts/clear_maintenance.sh` 런북. **"락은 fail-safe(자동해제), fail-stuck 아님"을 설계 요구사항으로 명시.**

---

## 10. 검증 경로·재현성·테스트·CI

### 10.1 검증 경로 (staging vs prod, 검수 M4)
- **staging 검증 가능** (`ENABLE_TRAINING=false`, prod GPU 무영향): 마이그레이션, 스냅샷 빌더, eval 게이트 로직(소형 fixture), 승격 dry-run, defs 로드, 정비 플래그 PG 메커니즘.
- **prod 전용, 통제된 수동 윈도우**: 실제 GPU 학습 + 공유 sam3 컨테이너 대상 `/unload`+`/warmup`. **공유 sam3(memory `project_sam3_shared_container`) 때문에 GPU 사이클의 진짜 staging dry-run 불가** = 수용된 리스크, fail-safe 락(§9) + 최소 첫-run 데이터셋으로 완화. 첫 실학습은 정비 윈도우 + 복구 런북 대기.

### 10.2 재현성 (검수 H4)
- trainer 가 **training seed**(split seed 와 별개: weight init/shuffle/dropout) 설정·기록(`training_config.seed`, `cudnn.deterministic`).
- **env_lock** JSON(torch/cuda/open_clip/peft/transformers freeze + `nvidia-smi driver_version` + `torch.version.cuda`)을 MinIO 아티팩트로(`env_lock_key`).
- **bit-exact 보장 안 함** (공유 GPU 비결정 커널) — seed 는 best-effort/감사용.

### 10.3 테스트 전략 (검수 H7)
프로젝트 패턴: `_DummyDB`/`_DummyMinIO`/`_DummyContext` 스텁 + `monkeypatch` 로 asset 내부 `_run_*` 직접 호출, `moto` mock_minio, real-PG fixture. **CI 에 GPU 없음.**
- 스냅샷 빌더: 2회 실행 `content_checksum` 동일 assert(H5/H6 락) + `al_confirmed_count=0` 정직성.
- eval 게이트: **첫-run stock_base 분기** + margin/non-regression 로직(스텁 메트릭).
- 승격: `promotable` 만 선택 + 레지스트리 필드 + `docker recreate` **dry-run/스텁**(CI 에서 prod 무변경).
- trainer: COCO→sam3 어댑터 + config 조립만(트레이너 mock). 실제 GPU run 은 `ENABLE_TRAINING`/수동 게이트, **CI 비대상**.

### 10.4 CI 통합 (검수 M1)
- `detect_image_rebuild` 는 `docker/<name>/` 명시 나열(와일드카드 없음) → **`^docker/trainer/` 를 `deploy-production.yml`·`deploy-test.yml` 양쪽 glob 에 추가**.
- `scripts/deploy/deploy-stack.sh` 에 `trainer_active()` + 조건부 build (sam3 패턴 미러).

---

## 11. 리스크 (검수 종합, 심각도)

| 심각도 | 리스크 | 대응 |
|------|-------|------|
| CRIT | LoRA·MinIO 체크포인트를 서빙이 못 로드 | LoRA→full-weight merge + 호스트 materialize (§7.3, §8) |
| HIGH | PE-Core image-only LoRA 가 cross-modal AL 붕괴 | 기본 contrastive image+text LoRA, cross-modal recall 게이트 (§7.3, §7.4) |
| HIGH | split leakage(같은 영상 frame 분산) + val 없음 | group-aware 3-way + 안정정렬 + stratify (§7.2) |
| HIGH | eval 게이트가 GT 아닌 YOLO-동의도/mAP 없음 | GT-anchored AP, stock_base baseline, sam3_shadow_compare 는 2차만 (§7.4) |
| HIGH | 정비락 fail-stuck → prod 라벨링 영구 정지 | 서버사이드 게이트 + heartbeat/TTL + guard 자동해제 (§9) |
| HIGH | PE-Core 승격 = 코퍼스 재임베딩 의무 (가중치 swap 아님) | versioned dual-index + DEFAULT_MODEL 원자 전환, 메커니즘 **구축** (§8.1) |
| HIGH | PE 게이트용 사람-검수 GT 현재 ≈0 | recall@k(사람GT only), GT<N_min abstain → GT 축적 전 PE 승격 비활성 (§7.4, §12 백필) |
| MED | SAM3 mask vs bbox GT | §7.1 선결 검증 + 분기 |
| MED | PE-Core holdout 부재/모델파생 | LS-finalized only, advisory (§7.4) |
| MED | VRAM 사이징 부정확 | PE-Core=ViT-L(풀FT 가능권), SAM3=multi-component(≠ViT-H); peak-VRAM 스모크 측정 (§7.3) |
| MED | trainer secrets / CI rebuild glob 누락 | env 블록 + glob 추가 (§7.3, §10.4) |
| MED | MLflow = 2차 저장소(분기 위험) | registry 가 승격 SoT, MLflow 는 추적 UI; trainer 가 둘 다 1회 기록; MLflow 로깅 fail-soft (§7.5) |
| HIGH | DVC 데이터 store-of-record = MinIO/NAS_200tb 백업 공백(기존 latent risk) | DVC 무관 운영 이슈지만 데이터셋 백업 갭이 됨 — restic/mc mirror 백업 cadence 확인 (§7.6) |
| MED | git commit ≠ dvc push (객체 누락) | ingestion 이 MinIO 객체 존재 검증 후에만 `available`; 누락은 `pending_missing_dvc_objects`+알림 (§7.6) |
| MED | DVC 운영부담(캐시/키/백업 cadence) + 메타 중복(git↔catalog↔MLflow) | 카탈로그=인덱스(경쟁 진실 아님), 런북 문서화; bare repo는 배포 경로와 격리 (§7.6) |
| LOW | 클래스 불균형 | stratified split (§7.2) |

**재임베딩 cheaper 대안 (검수 MED-6):** 전체 ~26만 재임베딩은 **충분하나 불필요**. ① **versioned dual-index** — 새 `model_name` 으로 신규 임베딩 기록 + partial HNSW(`WHERE model_name=<new>`, 006 헤더가 이미 예고) + AL/검색 기본 model_name 원자적 전환(old 는 롤백용 유지). active working set 만 증분 재임베딩. ② **shadow eval** — 샘플 subset 으로 top-k overlap 비교 후 전환. 전체 재임베딩은 old 버전 은퇴 시점에만.

---

## 12. 선결조건 / 이번 범위 밖 (Deferred)

- **`image_label_annotations` 백필** — 기존 288 finalized `image_labels` 의 COCO 를 per-box 투영(`src/gemini/ls_webhook_finalize.py:_project_bbox_after_finalize` 로직). SAM3 bbox 학습셋 전제.
- **PE-Core 승격 메커니즘** (재임베딩 asset + `model_name`별 HNSW + `DEFAULT_MODEL` 포인터 + recall@k 게이트) — §8.1 로 **이번 스캐폴딩에 구축**(코드/마이그레이션). 단 **실제 재임베딩 실행 + 포인터 전환은 게이트 뒤**(사람-검수 GT ≥ N_min 축적 + 승격 결정 시).
- **PE 게이트 활성화** — 사람-검수 (frame,text)/(frame,class) GT 축적 필요. 현재 0 → 백필(아래) + 리뷰 누적 전까지 PE 승격 abstain.
- **실제 학습 실행 + prod 승격** — 골격 완성 후 온디맨드 수동.
- **스케줄 재학습 루프, 멀티GPU 분산** — YAGNI. 카덴스/볼륨 정당화 시. (MLflow=§7.5, DVC 데이터셋 버저닝=§7.6 로 **도입** — deferred 아님.)

---

## 13. 빌드 순서 (writing-plans 입력)

> 각 step 은 컴포넌트 ID 가 아니라 작업 순서. staging(:3031) 정의로드/스모크 → dev→main. 어떤 step 도 prod 가중치 자동 승격 안 함.

1. **선결 검증:** `sam3.train` box-only supervision 지원 확인 (§7.1) — 결과에 따라 SAM3 트랙 분기.
2. 마이그레이션: `train_dataset_versions`(컴포넌트 1), `model_registry`(5), `image_embeddings` model_name 인코딩 정책(5.3) (DO block 분할).
3. 스냅샷 빌더(2) + `_split_groups` 브리지 + stratify + idempotency + 단위테스트 (§10.3).
4. `vlm-trainer` 컨테이너(3) 골격(SAM3 Hydra deps+config tree 저작 §7.3, LoRA→merge, secrets, 관측성) + compose + **CI rebuild glob/deploy guard**(§10.4).
5. Eval + 게이트(4): GT-anchored AP(+필요 시 mAP 신규), stock_base baseline.
6. 서빙 로딩+승격(6): `EMBEDDING_CHECKPOINT_PATH` 추가 + `promote_model.py`(materialize/recreate/rollback) — 만들되 미실행.
7. **PE-Core 승격 트랙(8, §8.1)**: `DEFAULT_MODEL` 런타임 포인터(PG) + 전체 재임베딩 asset(`defs/embed/reembed.py`) + `model_name`별 partial HNSW 마이그레이션(_OPTIONAL) + recall@k 게이트(GT<N_min abstain) — 메커니즘만, 실행 게이트 뒤.
8. **MLflow 추적(9, §7.5)**: `mlflow` 서비스(compose, profile-gated, PG `mlflow` DB backend + MinIO `_mlflow/` artifact) + trainer MLflow 로깅(params·dataset lineage·metrics·artifacts, fail-soft) + `model_registry.mlflow_run_id`(013 에 포함).
9. **DVC 데이터셋 버저닝(10, §7.6)**: 016 마이그레이션(`dataset_catalog` 3종 + `train_dataset_versions.dataset_catalog_id` FK) + bare repo/post-receive 훅 + dvc remote(S3→MinIO `_dvc/`) config + ingestion 센서(+reconciliation backstop) + pin API + 스냅샷빌더 `dvc get` 연동 + MLflow 필드.
10. GPU 정비락+복구(7): 서버사이드 게이트 + guard 센서 + `clear_maintenance.sh`.
11. 운영자 문서 (§14).

---

## 14. 운영자 문서 (안전-critical, 검수 M6)

- **CLAUDE.md MLOps 섹션:** 학습 트리거, eval 게이트 읽기, **MLflow UI(파라미터·학습 데이터 lineage 확인, §7.5)**, **승격+롤백** 명령, prod-GPU/공유-sam3 주의, idle-unload env 노브.
- **`.agent/skill/mlops-finetune/SKILL.md`:** 단계별 런북 — **정비락 복구(§9)**, "run 이 hung 인지 progressing 인지 판별"(§7.3 관측성), staging-vs-prod 검증 분리(§10.1).
- **`자주 쓰는 스크립트` 표:** `scripts/promote_model.py`, `scripts/clear_maintenance.sh`.
- 복구 런북은 **첫 실학습 전 blocking 산출물.**

---

## 15. 미해결 질문 (구현 전 확인)

1. **SAM3 box-only supervision** (§7.1) — `sam3/train` 정독. 미지원 시 트랙 descope/예외 결정.
2. **eval 게이트 margin 기본값** — SAM3 AP delta, PE-Core cross-modal recall delta, per-class non-regression floor 수치 (`eval_config` 기본).
3. **PE-Core 게이트 GT** (§7.4) — 결정됨: 사람-검수 recall@k only(proxy 미사용), GT<N_min abstain. **남은 결정: `N_min` 값 + GT 수집 경로** — (frame,class)=`image_label_annotations` projection prod 실행, (frame,text)=per-frame/event 캡션 리뷰 도입(현재 0).
4. **스냅샷 저장 = 복사 vs 참조** (§7.2) — 원본 삭제 정책과 불변성 trade-off 택1.
5. **정비모드 중 CI 배포 보류 강제** — 마커 파일 체크 step vs 운영 규칙.
6. **PE 재임베딩 커버리지** (§8.1-B) — 기본 'incumbent 동일 집합'(현 frame 187,994). 전체 `image_metadata` 525,966 로 확대할지 (커버리지↑ vs 배치시간↑).
7. **DVC 데이터 repo** (§7.6) — 결정됨: 호스트 bare repo `/srv/data-repos/dvc-datasets.git` + post-receive 훅(+reconciliation 센서 backstop). remote = MinIO `_dvc/`. **남은 결정: bare repo 호스트(어느 서버/경로) + dvc 자격(`.dvc/config.local`) 배치 + 백업 cadence(restic/mc mirror).**
