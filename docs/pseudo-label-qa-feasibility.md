# Pseudo-label 생성 & 품질평가 — 현 파이프라인 가능여부 진단

> 목적: "최고의 데이터파이프라인"으로 가기 위해, **현재 main 코드에서 pseudo-label 생성과 그 품질평가가 가능한가**를 정직하게 판정.
> 작성: 2026-07-01. 대상 브랜치 `main`. 3-모델 역할 분담 조사 후 종합.

## 역할 분담 (누가 무엇을 봤나)

| 모델 | 역할 | 산출 |
|------|------|------|
| **Sonnet** | 코드베이스 **매핑** — 생성/평가 구성요소가 어디에 뭐가 있는지 (파일:라인) | (A)(B)(C) 표의 근거 |
| **Codex** | **독립 가능여부 판단 + 갭·리스크** — self-training 금지 불변식과의 관계 정직 평가, AL seed 편향 코드 추적 | §2 티어분류·§3 갭·§4 리스크 |
| **Opus** | **종합 + 설계 판정** — 두 관점 대조, 최소 해제 경로 | 결론·§5 |

---

## 결론 (한 줄)

- **생성(generation): 지금 가능 — 이미 프로덕션에서 돌고 있음.** ✅
- **품질평가(quality eval): "지금 가능"은 advisory 신호(GT-free)뿐. 진짜 정확도 기반 정량평가는 지금 불가.** ⚠️
  - 막는 건 두 가지: ① 사람 GT(`image_label_annotations`)가 ~0행, ② 평가 게이트의 실제 스코어링 함수가 `NotImplementedError` 스텁.
- **정직한 표현**: *"QA 루프의 뼈대는 있으나, GT-기반 실제 수치는 단 한 번도 산출된 적 없음."* "튜닝만 하면 되는 QA 루프가 있다"가 **아님**.
- **유일한 unblocking action = 사람 GT 백필.** 단, 그 전에 AL 큐의 self-referential seed 편향을 먼저 고쳐야 (안 그러면 GT 셋 자체가 편향됨).

---

## (A) 생성 — label 종류별 가능여부

| Label | 가능? | 위치 | 상태 |
|-------|-------|------|------|
| **SAM3 bbox** (주 검출기) | ✅ **YES** | `defs/sam/detection_assets.py` → `image_labels` + `vlm-labels` COCO(per-box `score`) | 기본 ON, dispatch 구동 |
| **Gemini 이벤트/캡션**(video) | ✅ **YES** | `defs/label/timestamp.py:199+` → `labels`(`review_status='auto_generated'`) + `vlm-labels/<src>/events/*.json` | 기본 ON (`auto_labeling_sensor`). 0 events = 정상 |
| **Gemini video 분류** | ✅ YES | `defs/label/assets.py:62+` → `labels`(`video_classification_json`) | dispatch-gated (`outputs`/`categories` 태그 필요) |
| **PE-Core 임베딩** | ✅ YES(라벨은 아님, 신호 기질) | `defs/embed/assets.py` → pgvector `image_embeddings` | gated: `ENABLE_EMBEDDING=false` + 센서 STOPPED |
| **Places365 환경분류** | 🟡 **PARTIAL** | `lib/video_env.py:152+` → `video_metadata` env 컬럼 | ON. **단 DB row/JSON 조인 불가한 컬럼 형태** → QA 조인 취약 |
| **YOLO-World** | 🟡 비활성 | `defs/yolo/assets.py:97` `if not ENABLE_YOLO_DETECTION: skip` | **OFF** (기본 false, SAM3가 primary) |
| `vlm-classification` 버킷 | — 생성 아님 | `defs/build/classification.py` | 원본 **복사만**(DB/JSON 없음). 다운스트림 export |

**판정: 생성은 실재하고 대부분 돌고 있다.** 각 라벨은 provenance가 명확(`review_status='auto_generated'` vs `'finalized'`)해서 나중에 GT와 구분 가능 — 이게 QA의 전제조건인데 이미 충족.

---

## (B) 품질평가 — 3티어 분류 (Codex)

### (a) 지금 가능 — GT-free, advisory 전용 (올바르게 "신호"로만 쓰면 sound)
| 도구 | 위치 | 무엇 | 한계 |
|------|------|------|------|
| `compute_label_suspect` | `docker/analysis/fiftyone_pgvector.py:1446` | `0.6·kNN임베딩불일치 + 0.4·캡션모순` suspect score | **"의심 신호"지 오류율 아님** (미보정) |
| `caption_image_alignment` | 같은 파일 `:1274` | 캡션↔프레임 코사인 정합도 gap | 캡션 헬스 신호, 정확도 아님 |
| `sam3_shadow_compare` | `defs/sam/assets.py:29` | SAM3↔YOLO 동의도 | **게이트 아님**(CLAUDE.md "sanity 신호만"). + YOLO OFF라 입력 자체 없음 |
| `class_separation_report`, `active_learning_queue` | fiftyone 스크립트 | 임베딩 분리도 / 하드샘플 triage | 아래 (c) 참조 — AL은 seed 편향 있음 |

> ⚠️ 공통 한계: **전부 standalone 분석 스크립트**(`fiftyone_pgvector.py`)에만 있음. Dagster asset 아님, **DB write-back 없음**, 수동 실행. 새 pseudo-label에 자동 트리거 안 됨.

### (b) 원리상 sound하지만 GT 없어서 지금 inert
- **`train_eval_gate`** (`defs/train/eval.py` + `lib/train_eval_gate.py`): 결정 로직(margin + per-class floor + advisory)은 **실재하고 유닛테스트됨**. 그러나 실제 점수를 내는 `_score_candidate()`/`_score_incumbent()`가 **문자 그대로 `raise NotImplementedError`** ("GPU 스코어링은 prod 박스, CI는 monkeypatch"). → **GT-anchored mAP/recall@k 실측이 monkeypatch 밖에서 한 번도 안 돌았음.** 게이트는 완성된 빈 껍데기.
- **GT 소스**: `image_label_annotations` JOIN finalized `image_labels` (`postgres_train.py`가 model-derived row를 명시적으로 배제 — 불변식 준수). 스키마(migration 011)는 맞으나 **~0 행**.

### (c) 착각하면 unsound/금지 — 실제로 확인됨 (Codex 코드 추적)
- **`active_learning_queue`의 seed가 pseudo-label 전용**: `attach_labels()`가 `normalized_class`를 **SAM3 자신의 COCO 출력에서만** 채움(`_fetch_sam3_label_refs`→`vlm-labels`). 2658줄 파일 전체에 `review_status='finalized'`/`image_label_annotations` 참조 **0건**. → "rare-class 하드샘플 마이닝"이 모델이 이미 확신하는 것에 앵커됨 = **AL이 해야 할 일의 정반대**(blind spot이 아니라 이미 닮은 것을 우선 노출). *self-eval을 정책이 아니라 누락으로 위반* — 코드 키워드 검색으론 안 잡히는 은근한 self-reinforcement.
- SAM3↔YOLO 동의도를 정확도로 취급 = 상관된 편향의 합의 ≠ 정답.
- `suspect_score` 0~1을 오류율로 취급 = 미보정 휴리스틱.

---

## (C) GT 소스 (비교 기준)

| 소스 | 스키마 | 현황 |
|------|--------|------|
| `labels.review_status='finalized'` | 001_init | LS webhook finalize (`ls_webhook_finalize.py`) |
| `image_labels.review_status='finalized'` | 001_init | 동일 |
| **`image_label_annotations`** (per-box 사람 bbox) | 011 | **~0 행** (백필 전, memory `project_image_label_annotations`). ← **루트 블로커** |
| `v_finalized_labels` (3-grain 통합뷰) | 012 | caption/timestamp/bbox UNION |

> 라이브 카운트는 auto-mode가 prod-read로 차단 → 아래 부록 쿼리로 직접 확인 권장. 판정은 수치에 비의존(양 에이전트 모두 ~0 수렴).

---

## §3 갭 (블로킹 순위, Codex)

1. **사람 GT 미충전** (`image_label_annotations` ~0) — 루트 블로커. 아래 모든 sound 평가가 여기에 의존해 현재 inert.
2. **`train_eval_gate` 스코어링이 vaporware** — 결정 로직은 되나 수치 내는 두 함수가 `NotImplementedError`. "거의 완성"이 아니라 "구현 없는 인터페이스".
3. **닫힌 루프 배선 없음** — suspect/AL이 후보를 flag → LS 리뷰 → finalized → GT → 재-스코어로 **자동 연결되는 경로가 없음**. 각 조각이 섬.
4. **AL seed provenance가 pseudo-label 전용** (위 확인) — 방치 시 첫 GT 백필 자체가 편향되어 self-referential 루프가 GT 셋으로 전이.
5. **약한 생성기의 DB 레코드 부재** (Places365) — 스키마 없이는 QA 조인 불가.

## §4 리스크 (문서에 명시할 것)

- **동의도 ≠ 정확도**: 공유 편향 두 모델의 합의는 정답 아님.
- **불일치점수 ≠ 오류율**: suspect score 미보정.
- **누락에 의한 self-eval**: AL seed가 "eval"로 안 불리지만 spirit 위반.
- **클래스 불균형 노이즈**: fire/smoke rare → 첫 GT 소량·편중 → `per_class_floor`(-0.02/-0.05) 판정이 통계적으로 흔들림(rare-class 박스 몇 개로 승격 뒤집힘).
- **confidence 미보정**: `score_threshold` 등이 사람 GT로 fit된 적 없음 = 추정치.

---

## §5 최소 해제 경로 (Opus 종합 — ponytail)

지금 QA 프레임워크를 새로 짓지 마라. 조각은 이미 다 있다. **Pareto 액션은 딱 하나 + 선행 수정 하나:**

1. **[선행] AL seed를 finalized GT 기반으로 고친다** — `attach_labels`가 `normalized_class`를 SAM3 COCO가 아니라 `v_finalized_labels`(bbox grain)에서도 끌게. GT가 아직 0이면 그때만 pseudo fallback. 이걸 먼저 안 고치면 다음 단계가 편향된 대상을 사람에게 물림.
2. **[유일 unblocking] 사람 GT 백필** — LS finalize → `image_label_annotations` 채우기. 소량이라도. 이게 되는 순간 (b)티어(train_eval_gate)가 살아나고, advisory 신호를 실제 오류율로 **보정**할 수 있게 됨.
3. GT가 실재한 *다음에* — `_score_candidate/_score_incumbent` 실제 구현(prod GPU), 닫힌 루프 배선, threshold 보정. **지금 만들면 speculative**(GT 없이는 검증 불가).

> 즉 "가능하냐?"의 정직한 답: **생성은 오늘 가능. 품질평가는 advisory 신호만 오늘 가능하고, 진짜 정량 QA는 사람 GT 한 배치가 들어오는 순간 가능해진다 — 그 전엔 불가.** 코드를 더 짜는 게 아니라 **라벨을 사람이 확정하는 게 병목.**

---

## 구현 현황 (2026-07-01 — "GT 있다는 가정하 코드 구성" 완료분)

품질평가 코드는 **GPU 없이 저장 데이터만으로** 도는 스코어러 + asset 으로 구축됨 (GT 가 채워지면 즉시 동작):

| 구성요소 | 파일 | 내용 |
|---|---|---|
| 순수 스코어러 | `src/vlm_pipeline/lib/pseudo_label_qa.py` | `score_bbox_quality`(공간 IoU) · `score_timestamp_quality`(temporal IoU) → 클래스별 P/R/F1/TP/FP/FN. `box_map._iou` 재사용 |
| bbox QA asset | `defs/train/label_qa.py` `pseudo_label_bbox_qa` | pseudo=SAM3 원본 COCO(`sam3_segmentations/`, finalize 무손상) vs GT=`image_label_annotations`. **오늘 저장 데이터로 채점 가능** |
| timestamp QA asset | 동 파일 `pseudo_label_timestamp_qa` | pseudo=생성시점 스냅샷 vs GT=finalized `labels` 이벤트(event-level) |
| **timestamp 원본 보존** | `defs/label/timestamp.py` (생성 시 `*.pseudo.json` 스냅샷) + `key_builders.build_pseudo_events_key` | LS finalize 가 `events/`를 덮어써도 원본 pseudo 보존. **additive/best-effort** — 이 배포 이후 생성분부터 QA 가능(그 전 finalize 는 원본 소실=설계상 한계) |
| GT 쿼리 | `resources/postgres_train.py` `find_finalized_timestamp_events` | finalized 이벤트 구간 (bbox 는 기존 `find_sam3_finalized_bbox_candidates` 재사용) |
| 수치 표시 | 두 asset 이 per-class P/R/F1 표를 Dagster UI 메타데이터(`MetadataValue.md`)로 렌더 | 별도 대시보드 불필요 — Dagster materialization 이력이 추세도 제공 |
| **FP/FN 육안 확인** | `docker/analysis/label_qa_fiftyone.py` (standalone, eng-a 파일 무수정) | 사람 GT 있는 **모든 이미지를 MinIO 에서 직접** 격리 데이터셋 `pseudo_qa` 로 빌드(공유 frames 무관 → **frames 커버리지 밖 GT 도 전수 채점**; 이미지는 MEDIA_DIR 캐시 공유) → GT=`ground_truth`+SAM3 pseudo=`detections` → FiftyOne **네이티브 `evaluate_detections`** → 박스 tp/fp/fn 태깅 + P/R/F1. :5153 `pseudo_qa` 선택 후 `F("pseudo_qa")=="fp"` 필터 |
| 테스트 | `test_pseudo_label_qa.py`(10)·`test_label_qa_asset.py`(6)·`test_label_qa_fiftyone.py`(5) | 전부 mock/pure — 21 pass + 모듈 `--selftest`. fiftyone 테스트는 inline 파서=vlm_pipeline 원본 일치까지 검증(drift 방지) |

**아직 안 한 것(§5 순서상 GT 이후)**: eval_gate GPU 스코어링 실구현, AL seed GT 전환, advisory 신호 보정, 닫힌 루프 자동배선. GT 0 이면 asset 은 빈 리포트(정상).

⚠️ **timestamp 한계**: 원본 이벤트가 finalize 때 파괴되던 구조라, 위 보존 스냅샷 **배포 전에 이미 finalize 된 영상**은 pseudo 소실 → QA 불가(리포트에 `missing_pseudo` 로 집계). 신규 라벨부터 정상.

## 부록 — 라이브 GT 카운트 확인 (직접 실행)

auto-mode에서 막히면 `!` 프리픽스로:

```bash
! docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
SELECT 'image_label_annotations' t, count(*) FROM image_label_annotations
UNION ALL SELECT 'image_labels_finalized', count(*) FROM image_labels WHERE review_status='finalized'
UNION ALL SELECT 'labels_finalized', count(*) FROM labels WHERE review_status='finalized'
UNION ALL SELECT 'labels_auto_generated', count(*) FROM labels WHERE review_status='auto_generated';"
```

`image_label_annotations`가 0이면 위 §5-2가 최우선. >0이면 train_eval_gate 실구현 착수 가능.
