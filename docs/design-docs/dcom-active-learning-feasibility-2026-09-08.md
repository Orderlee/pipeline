# DCoM 능동학습 파이프라인 적용 타당성 + 실행계획

작성 2026-09-08 · 근거 = 이 저장소 코드 실측 + prod DB 실측 + 신규 실험 2건
(`docker/analysis/_dcom_delta0_probe.py`, `_dcom_coverage_curve.py`)
교차검토 = methodology-auditor(BLOCK) / codex-router(조건부 feasible)

---

## 0. 한 줄 결론

**논문의 coverage 절반만 적용 가치가 있고, competence·uncertainty 절반은 지금 적용
불가하다. 그리고 그보다 먼저, 능동학습 이전 단계에 정정해야 할 실측 결함이 3건 있다.**

기존 초안 `DCoM Integration Plan for pipeline.md`(PR 0~7, 신규 테이블 3개, lib 패키지,
Dagster 모듈, compose 마운트)는 **현재 데이터 규모 대비 과잉**이다. 아래 계획은 그 초안을
실측으로 정정하고 범위를 대폭 줄인 것이다.

> 🛑 **그리고 이 논의보다 앞서는 사실**: prod MinIO 5개 버킷이 전부 비어 있다(§1.1).
> 라벨 source-of-truth 가 없는 상태이므로 A0 판정이 나오기 전까지는 어떤 능동학습
> 논의도 실행 단계로 넘어갈 수 없다.

---

## 1. 실측 사실 (전부 재현 가능)

### 1.1 현행 AL 큐는 지금 **빈 리스트**를 반환한다

`active_learning_queue()` ([fiftyone_pgvector.py:2484](../../docker/analysis/fiftyone_pgvector.py#L2484))
는 FiftyOne `frames` 의 `normalized_class`(없으면 `detection_class`)로 fire/smoke 씨앗을
잡는다. 실측:

```
frames n=199,972 → normalized_class: none 187,994 / None 11,978
                   detection_class : none 187,994 / None 11,978
```

→ `seed_ids` 공집합 → [2515행](../../docker/analysis/fiftyone_pgvector.py#L2515) 조기 return.

**근본 원인 (2026-09-08 실측):** MinIO 5개 버킷이 **전부 비어 있다**(`list_objects_v2` KeyCount=0,
endpoint `http://10.0.0.51:9000`). PG 에는 SAM3 라벨이 455,014 image_id 만큼 남아 있지만
`labels_key` 가 가리키는 COCO JSON 객체는 `NoSuchKey` 다(8개 prefix 표본 전부 0/3).
NAS_primary 재구축(2026-08-31)에서 객체는 소실되고 DB 행만 살아남은 상태 그대로다.

그래서 `attach_labels`([:380](../../docker/analysis/fiftyone_pgvector.py#L380))가 재적재될 때마다
`_read_minio_json` 이 실패해 detections 가 빈 리스트가 되고 `detection_class='none'` 을 쓴다.
그런데 **`else` 분기가 `sample["detections"]` 를 지우지 않는다**
([:456](../../docker/analysis/fiftyone_pgvector.py#L456)) → 소실 이전에 붙은 stale 박스
**75,451건이 UI 에 그대로 보인다**(실측: `detections` non-null 75,451, 그 전부 길이>0,
그런데 `detection_class` 는 전부 `'none'`).

결과적으로 갈라진다:
- FiftyOne App: 박스가 **보인다** (stale)
- `detection_class`/`normalized_class` 로 필터·집계하는 코드: **전부 `'none'`** (AL 큐 포함)
- `fiftyone_label_refresh_schedule`(매일 03:00 KST)가 이 상태를 매일 재확인·재기록

이 저장소의 반복 결함 패턴(부재에 기댄 안전)의 가장 비싼 사례다 — 화면이 정상으로 보이므로
아무도 눈치채지 못한다.

### 1.2 baseline 의 가중치 3개 중 2개는 상수 0 이다

`frames` 스키마에 `uniqueness`·`representativeness` 필드가 **없다**(실측 스키마 덤프).
`_fo_scores_for_ids` 는 빈 dict 를 돌려주고 호출부가 0.0 으로 채운다
([:2556](../../docker/analysis/fiftyone_pgvector.py#L2556)). 따라서

```
al_score = 0.45*rare_sim + 0.30*0 + 0.25*0 = rare_sim 의 단조변환
```

**"0.45/0.30/0.25 fixed-weight heuristic" 이라는 표기는 문서에서 삭제해야 한다.**
실제 baseline 은 *rare-centroid kNN 검색* 이다. 이 표기를 남겨두면 이후 모든 비교가
straw man 이 된다.

### 1.3 사람 라벨(L)이 사실상 없다

| 항목 | 실측 |
|---|---|
| `image_labels` finalized | **288** |
| `image_labels` auto_generated (SAM3) | 454,726 |
| `image_label_annotations` | 1,558 |
| `image_embeddings` (entity_type='frame') | **188,190** |
| 그 프레임의 원본 영상 수 | 16,714 |

### 1.4 불확실성 신호는 못 쓴다

`active_learning_summary.json` 실측 AUC(신호→오답):
vote_entropy **0.509** / disagreement **0.486** / margin_cos **0.513** / margin_iou **0.579**.
margin_iou 만 0.55 를 넘지만 **클래스 조건부로 부호가 뒤집힌다**
(normal 0.141 ↔ falldown 0.980). 다수 클래스 normal 에서 역예측이므로 pooled 0.579 는
"오답 탐지력"이 아니라 **"normal 이냐 event 냐"를 읽는 클래스 탐지기**가 샌 값일 개연성이 크다.
(고전적 Simpson 이 아니라 qualitative interaction 이다.)

### 1.5 [신규 실험] δ0 는 클래스가 아니라 **카메라**를 잰다

sourcei GT 6,032장 / 15카메라, PE-Core 코사인 거리 (`_dcom_delta0_probe.py`):

| δ | π_class | π_camera | 평균 이웃 |
|---|---|---|---|
| 0.04 | 0.982 | **1.000** | 208 |
| **0.06** | **0.952** | **0.995** | 270 |
| 0.08 | 0.906 | 0.926 | 389 |
| 0.12 | 0.706 | 0.546 | 644 |

논문식 `δ0 = max{δ : π(δ) ≥ 0.95}` → **δ0 = 0.06**, 그 반경에서 ball 은 클래스보다
**카메라에 더 순수하다**(99.5% vs 95.2%). 즉 이 공간의 coverage 는 semantic manifold
coverage 가 아니라 **camera coverage** 다.

좋은 소식: 표현공간이 국소적으로는 클래스 일관적이다(95%). PE-Core 를 그대로 써도 된다.

### 1.6 [신규 실험] competence S_L 은 learner 상태가 아니라 **δ0 의 함수**다

greedy max-coverage 로 `C(|L|)` 와 초안 §23 파라미터(`k=30, a=0.9`)의 `S_L` 을 같이 찍은 것
(`_dcom_coverage_curve.py`):

| δ0 | \|L\|=25 | \|L\|=50 | \|L\|=200 | \|L\|=500 | 25장 시점 카메라 |
|---|---|---|---|---|---|
| 0.04 | 5.3e-6 (cov .49) | 7.5e-5 (.58) | 0.025 (.78) | **0.63** (.91) | 10/15 |
| **0.06** | 9.5e-4 (.67) | 0.014 (.76) | **0.80** (.94) | — | 12/15 |
| 0.10 | 0.238 (.86) | **0.82** (.94) | — | — | 11/15 |

읽는 법 두 가지:

1. **S_L 은 "항상 0"이 아니다.** δ0=0.10 이면 50장에서 켜지고 δ0=0.04 면 500장에서 켜진다.
   즉 competence 는 학습기 상태가 아니라 **내가 반경을 어떻게 잡았는지**를 되읽는 값이다.
   여기에 learner 가 아예 없으므로(zero-shot 프롬프트 뱅크는 L 에 대해 불변)
   `S_L` 을 "learner competence"라 부를 근거가 이 데이터에 없다.
2. **coverage 항이 너무 빨리 포화한다.** 25~50장만에 15대 중 10~13대 카메라를 건드리고
   coverage 0.5~0.9 에 도달한다. 그 뒤로 coverage 는 변별력을 잃고, 논문 설계대로면
   AUC≈0.5 짜리 uncertainty 로 넘어간다.

**따라서 DCoM 전체를 구현하면 "200장 카메라 라운드로빈 후 잡음으로 인계"가 된다.**

### 1.7 [정정 필요] 사람이 확인한 negative 가 조용히 버려진다

능동학습이 가장 원하는 프레임은 *SAM3 가 놓친 것* 인데, 그 프레임의 검수 결과를
저장할 경로가 없다. 코드 실측 3곳:

- [ls_sync_db.py:256](../../src/gemini/ls_sync_db.py#L256) — `UPDATE image_labels ... WHERE labels_key=%s`.
  SAM3 JSON 이 없어 parent 행이 없으면 **0행 갱신**(에러 아님).
- [ls_sync.py:330](../../src/gemini/ls_sync.py#L330) — `if rectangles or existing: write_json(...)`.
  주석에 명시: *"빈 검수는 기존 JSON이 있을 때만 … 없으면 생성 안 함"* → **MinIO 객체도 안 생김**.
- [012_finalized_label_view.sql:64](../../src/vlm_pipeline/sql/migrations/postgres/012_finalized_label_view.sql#L64) —
  bbox 분기가 `image_label_annotations` 에서 inner join → **박스 0개 finalized 는 0행**.

즉 "사람이 봤고 아무것도 없었다"는 가장 비싼 정보가 DB·MinIO·뷰 세 곳 모두에서 사라진다.
이 저장소의 반복 결함 패턴(부재에 기댄 안전 — 크래시 대신 조용한 오답) 그대로다.

### 1.8 [구현됨] Gemini 영문 캡션이 DB 에 적재되지 않고 있었다 → migration 025

Gemini `VIDEO_EVENT_SCHEMA` 는 `ko_caption` 과 `en_caption` 을 **둘 다 required** 로 받아왔는데
([gemini_prompts.py:109-110](../../src/vlm_pipeline/lib/gemini_prompts.py#L109)), 적재 시점에
`caption_text = ko_caption or en_caption or None` 으로 접혀 **한국어가 있으면 영문이 버려졌다.**
`labels` 테이블에 캡션 컬럼이 하나뿐이었기 때문이다.

그 결과 영문이 필요한 소비자가 매번 다시 번역했다 — `caption_embedding` asset 이
[gemini_translate.py](../../src/vlm_pipeline/lib/gemini_translate.py) 로 ko→en 을 50건씩 배치
재번역한다(PE-Core-L14-336 텍스트 인코더가 영어 중심이라 ko 원문은 cross-modal 정렬이 거의 0).

**조치 (2026-09-08 구현):**

| 파일 | 변경 |
|---|---|
| `sql/migrations/postgres/025_labels_caption_text_en.sql` | `ALTER TABLE labels ADD COLUMN IF NOT EXISTS caption_text_en text` (신규) |
| [`defs/process/captioning.py`](../../src/vlm_pipeline/defs/process/captioning.py), [`helpers_metadata.py`](../../src/vlm_pipeline/defs/process/helpers_metadata.py) | 행 빌더 2곳에 `"caption_text_en": en_caption or None` |
| [`resources/postgres_process.py`](../../src/vlm_pipeline/resources/postgres_process.py), [`postgres_labeling.py`](../../src/vlm_pipeline/resources/postgres_labeling.py) | `INSERT INTO labels` 2곳에 컬럼·플레이스홀더·`ON CONFLICT` SET |
| [`src/gemini/ls_sync_db.py`](../../src/gemini/ls_sync_db.py), [`ls_sync.py`](../../src/gemini/ls_sync.py) | **LS 검수 캡션 소실 정정** + `fps` 전달 (아래) |
| `tests/unit/test_labels_caption_en.py` | 회귀 테스트 10건 (+ `.gitignore` allowlist 편입) |

컬럼 의미 — `caption_text` 는 기존 표시용 폴백(ko 우선, 없으면 en)을 그대로 유지하고,
`caption_text_en` 은 **영문만 담고 폴백하지 않는다**(언어 혼재 방지).

**같이 고친 것 — LS 검수가 캡션을 통째로 지우고 있었다.**
`annotation_to_events`([ls_sync_converters.py:51](../../src/gemini/ls_sync_converters.py#L51))는
`category`/`duration`/`timestamp` 만 만든다 — TimelineLabels UI 에 캡션 필드가 없다. 그런데
`upsert_video_labels` 는 `labels_key` 기준으로 **전량 DELETE 후 캡션 없이 재INSERT** 했다.
즉 타임스탬프 검수 한 번에 ko·en 캡션이 둘 다 NULL 이 됐다. DELETE 전에 구간별 캡션을 떠 두고
**구간이 그대로인 이벤트에만** 되붙이도록 고쳤다(사람이 경계를 옮긴 이벤트는 캡션이 그 구간을
설명하지 않으므로 NULL 유지).

> ⚠️ **구간 비교를 정확 일치로 하면 안 된다** (codex 리뷰가 잡은 초판 버그).
> LS 왕복이 초→프레임→초로 양자화한다 —
> [`ls_tasks_create.py:187`](../../src/gemini/ls_tasks_create.py#L187) `round(sec * fps)` ↔
> [`ls_sync_converters.py:59`](../../src/gemini/ls_sync_converters.py#L59) `frame / fps`.
> 사람이 손대지 않아도 값이 달라진다. prod `labels` 5,000건 실측 끝점 정확일치율:
>
> | fps | 23.976 | 24 | 25 | **29.97** | 30 | 60 |
> |---|---|---|---|---|---|---|
> | 끝점 정확일치 | — | 79.5% | 75.0% | **21.3%** | 90.2% | 91.8% |
>
> 이벤트는 양 끝점이 모두 맞아야 하므로 실효 일치율은 그 제곱(29.97fps 에서 ~5%)이다.
> 양자화 오차는 끝점당 `0.5/fps` 로 **길이와 무관한 절대량**이라 IoU 같은 상대 척도가 아니라
> **절대 허용오차(1프레임 = 2×최대 오차)**로 비교한다. `fps` 를 `upsert_video_labels` 로
> 넘겨 정확히 계산한다.

**한계:**
- **기존 11,978행은 백필 불가.** 영문은 events JSON 에만 있었고 prod MinIO 가 비어 있다(§1.1).
  새로 도는 `clip_captioning`/`clip_timestamp` run 부터 채워진다.
- **MinIO events JSON 은 여전히 검수 시 캡션 없이 덮어써진다**([ls_sync.py:394](../../src/gemini/ls_sync.py#L394)).
  DB 는 이제 보존하지만 SoT 인 JSON 은 아니다 — 별건으로 남긴다.
- ~~`caption_embedding` 이 `caption_text_en` 을 아직 읽지 않는다~~ → **2026-09-09 반영됨.**
  caption 조회 3종이 `caption_text_en` 을 함께 반환하고, `build_caption_embedding_rows` 가
  저장된 영문이 **없는 행만** 모아 `translate` 를 한 번 호출한다(전부 있으면 Gemini 호출 0).
  `reembed` 경로도 저장된 영문을 쓰게 되어 본 경로와 기준이 맞았다.
  > ⚠️ **이 시점부터 caption 조회는 025 를 요구한다** — 미적용 DB 에서는 `UndefinedColumn` 으로
  > 실패한다(스크래치 DB 실측). 러너에 per-file try/except 가 없어 미적용 018/020/022/023 중
  > 하나가 실패하면 025 도 건너뛰므로, 018 의 실패 반경이 "기능이 조용히 안 됨"에서
  > "**`caption_embedding` asset 이 에러**"로 커졌다.

---

## 2. 판정

| 논문 구성요소 | 적용 가능? | 근거 |
|---|---|---|
| PE-Core 표현공간 재사용 | **가능** | §1.5 국소 클래스순도 95% |
| δ0 purity calibration | **가능, 단 의미 재해석** | §1.5 — 클래스가 아니라 카메라 반경 |
| coverage / ODR / greedy pruning | **가능, 가치 있음** | 기존 `CAP_PER_CLUSTER=12` 의 원리적 대체 |
| competence S_L 전환 | **불가** | §1.6 — learner 부재, δ0 의 함수일 뿐 |
| margin uncertainty | **불가** | §1.4 — AUC 0.49~0.58, 클래스별 부호 반전 |
| dynamic δ_i 갱신 | **불가** | learner pseudo-label 이 L 에 불변 |
| downstream 효용 측정 | **불가** | 사람 GT 288장, eval 채점부 `NotImplementedError` |

**결론: ProbCover(coverage-only)는 되고 DCoM 은 안 된다.**
그리고 명명 규율을 지킬 것 — competence 항이 비활성인 구간의 산출물을 "DCoM" 이라
부르면 안 된다. `coverage_only_v1` 로 기록한다.

---

## 3. 실행계획

### 단계 A — 능동학습 이전 정정 (DCoM 채택 여부와 무관하게 해야 함)

| # | 작업 | 파일 | 왜 |
|---|---|---|---|
| A0 | **MinIO 5개 버킷 전량 소실 대응** — 복구 가능 여부 판정(NAS 재구축 잔존물·백업), 불가면 `vlm-labels` SAM3 JSON 재생성 범위 산정. **AL 이전에 이것부터 결론이 나야 한다** | ops | §1.1 — SoT 부재 |
| A1 | `attach_labels` else 분기가 stale `detections` 를 지우도록 + MinIO 읽기 실패를 **집계해 크래시/경보**(현재 per-sample print 로 삼켜짐) + 씨앗 0개면 AL 큐가 크래시 | `fiftyone_pgvector.py:456`, `:2515` | §1.1 — UI 는 정상, 코드는 'none' |
| A2 | 문서·대시보드에서 `0.45/0.30/0.25` 표기 삭제, 실제 동작으로 정정 | `embedding_dashboard.py:748` | §1.2 — straw man 방지 |
| A3 | SAM3 JSON 없는 프레임의 **negative 검수 영속화**: parent `image_labels` 행 seed + 빈 검수도 COCO JSON 생성 + `v_finalized_labels` LEFT JOIN | `ls_sync_db.py`, `ls_sync.py`, migration 025 | §1.7 — 오라클 루프가 안 닫힘 |
| A4 | **평가 코호트 동결** — 무작위 표집 사람라벨 집합을 별도 tag 로 봉인, 어떤 AL 도 선택 불가 | migration 026 | AL 이 LS 를 먹이기 시작하면 되돌릴 수 없음 |
| A5 | **Gemini 영문 캡션 적재** (`labels.caption_text_en`) + LS 검수 캡션 보존 | migration 025 + 5파일 | §1.8 — **구현 완료, 배포 대기** |

A3·A4 는 **뒤로 미루면 안 된다.** A4 를 안 하고 AL 을 켜면 이후 생성되는 모든 사람
라벨이 selection-biased 가 되어 영구히 unbiased eval 로 못 쓴다.

### 단계 B — L/U 의미 정정 (초안 P0-1 이 맞다)

```sql
-- U = 임베딩 있고 사람이 아직 확정 안 한 프레임
SELECT e.entity_id AS image_id
FROM image_embeddings e
WHERE e.entity_type = 'frame'
  AND NOT EXISTS (SELECT 1 FROM image_labels il
                  WHERE il.image_id = e.entity_id AND il.review_status = 'finalized')
```

핵심: **`image_label_annotations` / `v_finalized_labels` 로 L 을 정의하면 안 된다** —
박스 단위라 "박스 0개 확정"(=사람이 확인한 negative)을 놓친다. parent `image_labels` 를 쓴다.
`normalized_class='none'` 은 weak detection 상태이지 human label 상태가 아니다.

### 단계 C — coverage-only 선택기 (여기까지가 지금 만들 것)

기존 `active_learning_queue()` 안에서 **점수식만 교체**한다. 새 lib 패키지·새 테이블·
Dagster 모듈·compose 마운트 **전부 불필요**.

```
후보 풀   = rare branch(HNSW near fire/smoke) ∪ global branch      # 초안 §15 유지
선택      = greedy marginal coverage (δ0=0.06 반경 내 미커버 이웃 수 최대)
           + 선택 시 그 이웃들 covered 처리 (ODR pruning)
제약      = max_per_asset / min_frame_gap_sec / max_per_project
```

- `CAP_PER_CLUSTER=12` 와 kmeans64 는 **버린다** — greedy marginal coverage 가 같은 일을
  데이터에서 유도된 반경으로 한다.
- **제약이 그래프보다 중요하다.** §1.5 의 이웃 대부분은 같은 클립의 인접 프레임이다.
  `max_per_asset`·`min_frame_gap_sec` 없이 그래프만 붙이면 이득 대부분이 사라진다.
- 그래프는 후보 풀(5k~30k) 한정. 188,190 전량 `graph_k=64` 는 ~1,200만 엣지라 불필요하다.
  **hnswlib 추가 불필요** — 기존 entity_type별 partial HNSW + `ef_search` 로 충분하다
  (`008_embedding_partial_indexes.sql`, `pgvector_index.py:41`).

### 단계 D — LS 임의 프레임 반출 (초안 P0-2 가 맞다)

`_create_image`([ls_tasks_create.py:409](../../src/gemini/ls_tasks_create.py#L409))는 건드리지
않는다. LS 태스크 원시 함수([:80](../../src/gemini/ls_tasks_create.py#L80))는 presigned URL 만
있으면 되고, SAM3 스캔은 *열거자* 에만 있다. 별도 서브커맨드로:

```
image_id → image_metadata(bucket,key) → presign → create_image_task
         → task.data 에 {image_id, al_round_id, al_rank, al_reason} 첨부
```

`fetch_existing_task_image_stems`(idempotency)와 presign 갱신 경로는 무수정으로 호환된다.
**단 A3 없이 D 만 하면 검수 결과가 저장되지 않는다** — 반드시 A3 선행.

### 단계 E — 하지 않을 것 (명시적 보류)

- competence S_L 혼합, uncertainty provider 추상화, dynamic δ_i, `active_learning_radii`
- `active_learning_rounds` / `active_learning_candidates` 테이블 (CSV + 라운드 tag 로 충분)
- `src/vlm_pipeline/lib/active_learning/` 패키지, `defs/active_learning/` Dagster 모듈
- compose 에 `../src:/repo-src:ro` 마운트 — production 이 DCoM 을 돌릴 일이 아직 없다.
  필요해지면 `analysis-sync` HTTP 경계를 쓴다(이미 `defs/viz` 가 그 패턴).
- 대시보드 strategy selector

---

## 4. 착수 전 사전검사 (사람 라벨 0장, 전부 오프라인)

| # | 검사 | 통과 기준 | 상태 |
|---|---|---|---|
| P1 | δ0 별 `C(\|L\|)` 와 S_L 활성화 지점 | competence 가 예산 구간에서 의미 있게 변하나 | **완료 — 실패** (§1.6, δ0 의 함수) |
| P2 | 무작위 표집 선형프로브 학습곡선 (\|L\|=16…512, 카메라 5-fold) | 기울기가 폴드 SD 대비 유의한 양수 | 미실행 |
| P3 | 카메라 군집 부트스트랩 AUC CI (4,000회) | margin_iou CI 하한 > 0.55 | 미실행 |
| P4 | 가중 within-class AUC + `gt==normal` 대조 AUC | ①≈0.5 & ②높음 → 신호가 사전확률을 읽는 것 | 미실행 |

P1 이 이미 실패했으므로 **uncertainty 분기는 착수 대상이 아니다.** P2~P4 는 "정말 못 쓰는가"를
확정하는 사후 검증이며, 단계 A~D 를 막지 않는다.

---

## 5. 측정 계획 (무엇을 주장할 수 있나)

**주장 가능** — selection-side 지표만:

| 지표 | 정의 | 비고 |
|---|---|---|
| rare-event yield @ budget | 사람 확정 (fire+smoke+falldown) / 검수 수 | **primary**. 독립단위는 프레임이 아니라 asset — `max_per_asset` 강제 필수. B=200·cap 3 이면 유효 n≈67/arm, MDE ≈ +18pp |
| coverage @ budget | δ0 반경 union / 풀 | 결정론적, 표집오차 거의 없음. **중간지표** |
| batch redundancy | 배치 내 mean-max cosine, cos>0.95 비율 | 결정론적 |
| source concentration | max project/camera/asset share | 지배적 분산원 직격 |
| 확정 rare 1건당 검수 시간 | LS 타임스탬프 | 실제 목적함수에 가장 가까움 |

**주장 금지**: budget별 macro-F1 / per-class AP / label efficiency / 전략 순위.
응답변수가 평평하고(지도 프로브 0.364±0.221 vs zero-shot 0.305~0.348), 유효표본이 32
(ICC 0.51~0.83, deff 232)이며, eval 채점부가 미구현이다.

**비교 방법**: arm 별로 L 을 분기시키면 rare centroid 가 갈려 후보 풀 자체가 달라진다
(재정렬 비교가 아니라 검색 쿼리 비교가 되어버림). 각 라운드에서 `A ∪ B` 를 라벨링하고
**대칭차집합에서만** 짝비교한다. 비용 `2B − |A∩B|`, 짝비교라 검정력이 낫다.
3번째 arm(**baseline + 동일 caps + greedy dedup**)이 없으면 "coverage 기여"와
"배치 중복 제거 기여"가 분리되지 않는다.

---

## 6. 순서

```
A0 MinIO 소실 판정            ← 라벨 SoT 가 없으면 AL 은 무의미. 최우선
A1 A2 A5 (즉시, 독립 — A5 는 구현 완료, 배포만 남음)
  → A4 평가 코호트 동결          ← 여기 전에 AL 을 LS 에 붙이면 안 됨
  → A3 negative 영속화 + migration 026
  → B  L/U 뷰
  → C  coverage-only 선택기 (오프라인 시뮬레이션 먼저: sourcei 오라클 풀, seed 5개)
  → D  LS 임의 프레임 반출
  → 소규모 실제 배치 → §5 지표로 판정
```

`026_active_learning_state.sql` 이 담을 것은 초안의 테이블 3개가 아니라
**A3 의 negative 영속화 + A4 의 eval 코호트 봉인 + `v_active_learning_pool` 뷰** 뿐이다.
(025 는 §1.8 의 `labels.caption_text_en` 이 선점했다.)

---

## 7. 남은 위험

- 카메라 62~65대를 확보하기 전에는 어떤 acquisition 비교도 유의해지지 않는다.
  분석 기법으로 해결되는 문제가 아니라 **설계 제약**이다(현재 GT 코호트 15대, fire 는 4대).
- rare-event yield 가 개선돼도 그것이 학습셋 품질 개선으로 이어진다는 연결고리는
  학습·평가 루프가 없어 미검증으로 남는다.
- rare centroid 를 SAM3 파생 라벨로 계산하는 현행 경로는 **자기학습 금지 정책과 충돌**한다
  ([:2504](../../docker/analysis/fiftyone_pgvector.py#L2504)). baseline·초안 §15 양쪽에 이미
  들어와 있다. 별도 판정 필요.
- sourcei GT 는 pgvector 에 연결돼 있지 않고(`entity_id` 필드 부재), normal 4,323장이
  Gemini caption 파생이라 GT 로 쓰면 자기학습 금지 정책을 스스로 위반한다.

---

## 부록 — 재현

```bash
# δ0 순도 곡선 (클래스 vs 카메라)
docker exec -i docker-analysis-1 nice -n 19 python3 /workspace/_dcom_delta0_probe.py

# greedy coverage 곡선 + S_L
docker exec -i docker-analysis-1 nice -n 19 python3 /workspace/_dcom_coverage_curve.py

# 산출물
docker/data/fiftyone/frames_bank/report/sourcei_gt/dcom_delta0_probe.json
docker/data/fiftyone/frames_bank/report/sourcei_gt/dcom_coverage_curve.json
```
