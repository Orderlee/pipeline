# 파이프라인 흐름 정합성 감사 — MLOps · DVC · Pseudo-label

> 2026-07-01. 초점: **로직·설계 정합성 + 3축 일관성**(실행가능성 아님). 브랜치 `main`(로컬, 미push).
> 방법: 6 에이전트 read-only — **Sonnet ×3**(흐름 end-to-end 추적) + **Codex ×3**(불변식 검증·버그 사냥) + **Opus** 종합.
> 각 발견은 독립 2개 에이전트가 잡았으면 **[확인]**, 1개면 [단독].

---

## 결론 (흐름별 verdict)

| 흐름 | 로직/설계 | 핵심 문제 |
|------|-----------|-----------|
| **Pseudo-label QA** | 🔴 **1개 치명적** + 나머지 정상 | **bbox QA가 pseudo≈GT 비교** → 무의미(P/R/F1 ~1.0로 왜곡). timestamp·스코어러·좌표변환은 정상 |
| **MLOps 파인튠** | 🟡 정직한 스캐폴딩 + 🔴 1개 조용한 no-op | 학습↔레지스트리 글루 전부 미배선(설계상 known) + **promote_pe_core `main()`이 no-op(false-success)** |
| **DVC 버저닝** | 🟠 lineage FK 단절 + 런타임 갭 | `dataset_catalog_id` FK 항상 NULL, `dvc[s3]` 미설치, MLflow `dvc_*` dead, reconciliation self-heal 불가 |

**3축 일관성**: GT 정의는 3축 공유로 일관 ✓. 그러나 (a) bbox QA 버그가 **no-self-eval 불변식을 사고로 위반**(사람GT vs 사람GT 비교로 붕괴) 🔴, (b) DVC→trainset→model→MLflow **lineage 체인이 FK에서 단절**(source_spec JSON으로만 추적 가능) ⚠️.

---

## 🔴 CRITICAL

### C-1. bbox pseudo-QA가 pseudo ≈ GT 를 비교 → 품질수치 무의미 [확인: S3+C3]
- **경로**: SAM3가 `sam3_segmentations/<stem>.json`에 원본 COCO 작성 → **LS 리뷰 `/sync`(finalize 前)가 `ls_sync.py:306-332`에서 그 동일 키를 사람수정본으로 in-place 덮어씀**(`build_reviewed_coco_json`이 annotations를 사람 박스로 완전 교체, merge 아님) → `ls_webhook_finalize.py:76-89 _project_bbox_after_finalize`가 그 덮어쓴 키에서 GT(`image_label_annotations`) 투영.
- **결과**: `label_qa.py:54 _fetch_pseudo_bbox`가 읽는 "pseudo" = 이미 사람수정본. GT도 같은 객체 파생 → **pseudo == GT**. SAM3가 놓친 박스를 사람이 그리면 QA는 "SAM3가 맞췄다"(tp=1, fn=0)고 보고 → **QA가 측정해야 할 바로 그 오류에 눈이 멂**. `docker/analysis/label_qa_fiftyone.py:210`도 동일.
- **작성자 주(정직)**: 이건 이번 세션에 내가 만든 코드고, 이전 아카이올로지에서 **video events 경로만 확인하고 image-bbox 리뷰 writeback을 놓쳐** "bbox 원본은 별도 키에 보존"이라 잘못 결론냈다. timestamp는 스냅샷으로 맞았지만 bbox는 틀렸다. 감사가 정확히 이걸 잡으려 했고 잡았다.
- **불변식 영향**: no-self-eval을 **사고로 위반**(사람GT를 사람GT와 비교).
- **수정**: timestamp와 동일하게 `build_pseudo_bbox_key` 추가 → SAM3 추론 직후 `.pseudo.json` write-once 스냅샷(`sam3_labeling.py`/`defs/sam/assets.py`) → `label_qa.py:_fetch_pseudo_bbox` + `label_qa_fiftyone.py`가 그 새 키를 읽게.

### C-2. `promote_pe_core.py main()`이 완전 no-op — PE-Core 승격이 아예 없음 [확인: S1+C1]
- `scripts/promote_pe_core.py:120-131`: `--apply`가 배너만 찍고 `return 0`. `flip_pointer`/`assert_reembed_complete`/`create_model_partial_hnsw`/`docker_recreate`를 하나도 안 부름(전부 정의·유닛테스트는 됨). 주석이 "E7-analogue 통합단계에서 조립"이라 인정 — 그 단계 미작성.
- **결과**: operator가 `promote_pe_core.py --apply` 실행 → 포인터 flip·서빙 교체 기대 → 아무 일 없음, exit 0, 에러 없음 = **조용한 false-success**. PE-Core 승격 불변식(원자 포인터 전환)은 실배선이 없어 검증 불가.

---

## 🟠 HIGH

### H-1. `train_dataset_versions.dataset_catalog_id` FK 항상 NULL [확인: S2+C2]
`resources/postgres_train.py:129-161 insert_train_dataset_version`의 INSERT 컬럼목록에 `dataset_catalog_id` **없음** → row dict에 넣어도 조용히 drop. `freeze_multi_project_trainset`(dataset.py:482-497)도 안 넣음. 마이그레이션 016이 FK 컬럼·제약을 추가하고 docstring(dataset.py:337)이 "여기 쓴다"고 하지만 **아무도 안 씀**. lineage는 `source_spec` JSONB 안에만 존재(FK join 경로는 죽음).

### H-2. MLflow `dvc_*` lineage 태그 = dead code [확인: S2+C2]
`docker/trainer/mlflow_logging.py:28-31`의 `dvc_catalog_id/dvc_git_rev/dvc_commit_subject/dvc_md5`를 `training_summary.json["dataset"]`에서 읽는데, **trainer(entrypoint/trainer_lib)·freeze 어느 쪽도 그 키를 안 채움**. H-1 때문에 채우고 싶어도 소스가 NULL. `test_mlflow_dvc_tags.py`가 dict를 hand-mock해서 초록불이지만 실배선 갭을 못 잡음(false confidence).

### H-3. dagster 이미지에 `dvc[s3]` 미설치 → `dvc get`(MinIO) 런타임 실패 [단독: S2]
`docker/app/requirements.txt:8`에 `dvc`(unpinned)는 있으나 `dvc-s3` 없음(live 확인: dagster-code-server에 dvc 3.67.1 있고 `import dvc_s3` 실패). `s3://` MinIO remote fetch가 pull 시점에 실패. **DVC.md의 "dvc 자체가 없음" 진단은 stale/틀림** — 실제는 `[s3]` extra 누락. trainer(`requirements.txt:18`)는 `dvc[s3]` 올바름.

### H-4. reconciliation 센서가 pending 행 self-heal 불가 [확인: S2+C2]
`postgres_train.py:243 insert_catalog_row`는 `ON CONFLICT DO NOTHING`. Dagster reconciliation 센서가 이걸 씀 → `pending_missing_dvc_objects` 행이 (지연된 dvc push 후에도) **영구 pending**. hook쪽 `ingest_to_catalog.py:44-49`는 `DO UPDATE ... WHERE status='pending...'`로 이미 고쳐놨는데("Codex BUG2" 주석) **공유 헬퍼엔 미반영** — 두 경로 불일치.

### H-5. `promote_model.py`가 registry commit 前 컨테이너 recreate [단독: C1]
`scripts/promote_model.py:283-302` 순서: env write → `docker_recreate`(서빙 교체·재기동) → DB transition → `commit()`. recreate와 commit 사이 crash 시 **새 체크포인트가 서빙 중인데 `model_registry`는 옛 행이 promoted** → "registry=truth" 복구관점 위반. 수정: DB commit을 먼저.

---

## 🟡 MEDIUM (대부분 정직한 스캐폴딩 — "숨은 회귀" 아님)

- **M-1. 학습↔레지스트리 글루 전부 미배선** [확인:S1+C1]: trainer가 `TRAIN_DATASET_VERSION_ID` 안 읽음, `_trainsets/<id>/`→`/trainset/` 다운로드 없음(볼륨도 없음), `insert_candidate_model_version` 안 부름, `training_summary.json` 아무도 안 씀. **단 설계문서·CLAUDE.md가 "built-not-executed 스캐폴딩"이라 명시** → 숨은 회귀 아니라 미완 seam. 학습루프 자체가 외부/미인스턴스.
- **M-2. `eval.py _score_candidate/_incumbent` = NotImplementedError** [확인:S1+C1]: 문서화된 known 스텁. prod 실행 시 hard-crash(조용한 오답 아님 — 그나마 나은 실패). gate 로직 자체는 정상이나 unreachable.
- **M-3. `eval.py`가 `pe_core_gate_decision`(GT-abstain) 미호출** [단독:C1]: "GT<min→abstain"이 유닛테스트에만. `_run_train_eval_gate`가 pe_core도 `evaluate_gate`(advisory=True) 직행 → GT=0이어도 promotable=True 가능(스텁 때문에 오늘은 도달 불가로 가려짐).
- **M-4. Tier-2 DVC 경로가 GT SQL필터 우회** [단독:C1]: `assemble_multi_project_coco`는 사람이 dvc commit한 COCO를 신뢰 — no-self-training이 **코드 강제 아니라 process-trust**(설계상 의도지만 미문서화된 신뢰경계).
- **M-5. `pin_alias`가 catalog status 미검사** [단독:C2]: `pending_missing_dvc_objects`/`invalid` 행도 pin 가능 → H-4와 겹쳐 깨진 pin 쉽게 생성, `dvc get` 늦은 실패. fast-fail 권장.

## 🟢 LOW (cosmetic/latent)

- **L-1** `merge_coco:51-53`가 미지 category_id annotation을 **카운터 없이** 조용히 drop(image-orphan은 카운트하는데 비대칭) [C2]
- **L-2** `freeze_multi_project_trainset`(Tier2)에 per-class stratify floor 없음(Tier1은 있음) → rare class가 한 split에 몰려도 무경고 freeze [C2]
- **L-3** reconciliation 센서 STOPPED 기본 + `DVC_DATA_REPO_PATH` compose 미배선 + git safe.directory 의존(hook·backstop 동시에 조용히 깨질 수 있음) [C2]
- **L-4** `pseudo_label_qa.py` docstring이 bbox 보존을 과대주장(C-1 관련 — 최소 doc 수정 필요) [S3]
- **L-5** `ls_sync_converters.py:85-93`의 `_sam3_key_from_image_key` 인라인 중복(원본 키빌더 변경 시 drift 위험) [S3]

---

## ✅ 검증됨 — 정상 (전부 부서진 게 아님)

- **챔피언-챌린저 게이트 로직**(`lib/train_eval_gate.py`): per-metric margin + per-class floor + stock_base cold-start + tie=veto + advisory. 정확·테스트됨 (단 C-2/M-2로 unreachable) [S1+C1]
- **스코어러 수학**(`pseudo_label_qa.py`): greedy 1:1 IoU 매칭(double-count 없음), temporal IoU, P/R/F1 zero-div 가드 — 견고 [C3]
- **좌표변환**(xywh↔xyxy px, ↔relative): label_qa.py·label_qa_fiftyone.py 양쪽 GT/pseudo 대칭 적용, mixup 없음 [C3]
- **timestamp pseudo 스냅샷**(`build_pseudo_events_key` + write-once 가드): 설계대로 작동 [S3+C3]
- **registry=truth 메커니즘**(env var, 심볼릭링크 아님, `.env`가 rsync 제외라 생존) [C1]
- **PE-Core 포인터 전환 read 격리**(per-model_name partial HNSW, 원자 UPDATE, reembed가 incumbent 벡터 미변경) — 메커니즘 자체는 sound (단 C-2로 배선 없음) [C1]
- **no-self-training 단일프로젝트 경로**(SQL이 model-derived 배제) [C1]
- **동결 스냅샷 불변**(SEALED 마커 마지막, checksum 멱등) [C1]
- **pin_alias 원자성**(FOR UPDATE, task-match 가드, 단일 txn, 데드락 없음) [C2]
- **content_checksum 결정성** + **merge_coco id remapping**(충돌 없음) [C2]

---

## 수정 우선순위 (Opus 권고)

**즉시 (correctness, 조용한 오류):**
1. **C-1 bbox `.pseudo.json` 스냅샷** — 내가 만든 QA의 실버그 + no-self-eval 위반. 안 고치면 bbox QA 숫자를 믿으면 안 됨. (timestamp와 동일 패턴, 작은 변경)
2. **C-2 `promote_pe_core.py main()` 배선** — 조용한 false-success 제거(최소한 미구현이면 loud error로).

**GT/학습 실가동 前까지 미룰 것 (지금 고쳐도 검증 불가):**
- H-1/H-2(lineage FK+MLflow) · H-5(promote 순서) · M-1~M-4 — 학습·승격을 실제로 돌리기 시작할 때 함께.
- H-3(`dvc[s3]`) · H-4(reconciliation self-heal) · M-5(pin status) — DVC Tier-2를 실제로 돌리기 직전에.

**결론**: 3 흐름 다 **설계 골격은 대체로 옳고**(스코어러·게이트로직·포인터격리·pin 원자성 등 검증됨), 문제는 (a) 내가 만든 **bbox QA 실버그 1개(즉시 수정 요)**, (b) MLOps/DVC의 **미배선 seam들**(대부분 정직한 스캐폴딩이나 promote_pe_core no-op·lineage FK·dvc[s3]는 실결함). 병목은 여전히 사람 GT.

---

## 수정 현황 (2026-07-01 — 3자 분담 구현: Sonnet×2 + Codex 제안 + Opus)

**✅ 수정 완료 (코드 + 테스트, 127 unit pass, ruff clean, 로컬 미push):**

| 항목 | 수정 내용 | 담당 |
|------|-----------|------|
| **C-1** | `build_pseudo_bbox_key` + SAM3 추론 직후 `.pseudo.json` write-once 스냅샷(`sam3_labeling.py`); QA 2곳이 라이브 키 대신 스냅샷 읽음(폴백 없음). sam3 detection asset 통합 테스트 통과 | Sonnet A |
| **C-2** | `promote_pe_core.py main()` 실배선 — select→reembed확인→partial HNSW→flip+registry전이→commit→(선택)recreate. import 실패/DSN없음/인자충돌 시 loud exit 2(silent 0 제거) | Codex 제안+Opus |
| **H-1** | `insert_train_dataset_version` INSERT에 `dataset_catalog_id` 추가; single-source 시 세팅, multi-source는 NULL(source_spec) | Sonnet B |
| **H-3** | `docker/app/requirements.txt` `dvc` → `dvc[s3]>=3.51,<4` | Opus |
| **H-4** | `insert_catalog_row` `DO NOTHING` → `DO UPDATE ... WHERE status='pending...'` (reconciliation self-heal) | Sonnet B |
| **H-5** | `promote_model.py` DB commit을 `docker_recreate` 前으로 + loud repair 메시지 | Codex 제안+Opus |
| **M-3** | `eval.py`가 pe_core 행에 `pe_core_gate_decision`(GT-abstain) 경유 | Opus |
| **M-4** | Tier-2 source_spec에 `gt_source='dvc_curated'` 마커(process-trust 경계 명시) | Sonnet B |
| **M-5** | `pin_alias`가 catalog status ∉('available','pinned') 시 fast-fail | Sonnet B |
| **L-1** | `coco_merge` 미지 category_id drop 카운트+provenance 노출 | Sonnet B |
| **L-2** | `freeze_multi_project_trainset` per-class floor → `starved_classes` 경고 리포팅(hard-fail 아님) | Sonnet B |
| **L-4/L-5** | 과대주장 docstring 정정 + ls_sync_converters 인라인 키빌더 drift-guard 테스트 | Sonnet A |
| (보너스) | 사전존재 깨진 테스트 `test_sam3_detection_asset` `_DummyMinIO` `upload_json`/`exists` 추가 | Opus |

**⏸ 의도적 미수정 (외부/ops 의존 — 조작하면 검증불가 코드가 됨, 정직하게 경계 표시):**

- **M-1**(trainer↔registry 글루)·**H-2**(MLflow `dvc_*` 채움)·**M-2**(GPU 스코어링): **repo에 없는 외부 SAM3/PE-Core 학습루프 + GPU** 의존. 매핑·게이트 로직 등 repo쪽 조각은 이미 있고 테스트됨. 외부 학습루프/GPU 착수 시 함께 배선 — 지금 글루를 더 짜면 CI/GPU 없이 못 도는 false-confidence 코드만 늘어남.
- **L-3**(reconciliation 센서/compose): bare repo를 dagster에 bind-mount + 센서 enable = **배포/재기동 결정**(ops-readiness). 코드 로직 버그 아님.

**남은 병목은 여전히 사람 GT** — 위 수정들은 GT/외부학습루프가 실가동될 때 올바르게 동작하도록 골격을 정확히 만든 것.

---

## 추후 작업 체크리스트 — 데이터/외부 학습루프/GPU 유입 시 (미수정 4개)

> 각 항목: **트리거**(언제 착수) · **할 일**(무엇) · **검증**(어떻게 확인). 코드 사이트에 `TODO(mlops-audit …)` 마커를 박아 이 표로 역참조하게 함.

### M-1 — trainer ↔ registry 글루 (`docker/trainer/entrypoint.py`, `trainer_lib.py`)
- **트리거**: 외부 SAM3/PE-Core 학습루프(Hydra harness)가 trainer 이미지에 실제로 인스턴스화됨 + 첫 실학습.
- **할 일**: (a) `TRAIN_DATASET_VERSION_ID` env 읽기(현재 무시), (b) `vlm-dataset/_trainsets/<id>/` → `/trainset/` 다운로드(현재 하드코드 경로 가정, 볼륨/다운로드 없음), (c) 학습 후 `insert_candidate_model_version` 호출(현재 미호출) + 산출물 `_models/<model>/<ver>/`(merged weight + `env_lock.json` + `train_log.jsonl` + `training_summary.json`) 업로드.
- **검증**: 정비 윈도우 `ENABLE_TRAINING=1` 소형 실학습 → `model_registry`에 `status='candidate'` 행 1개 생성 + `_models/<ver>/` 산출물 존재 확인. (골격은 loud dry-run으로 이미 secrets/wiring 검증됨.)

### H-2 — MLflow `dvc_*` lineage 채움 (`docker/trainer/mlflow_logging.py`, freeze/register 경로)
- **트리거**: M-1 완료(→ `training_summary.json` 생산자 존재) **+** DVC Tier-2 학습셋으로 학습.
- **할 일**: 매핑(`_LINEAGE_FIELDS`의 `dvc_catalog_id/dvc_git_rev/dvc_commit_subject/dvc_md5`)은 **이미 구현됨** — 생산자만 없음. register 단계가 `train_dataset_versions.dataset_catalog_id`(H-1으로 이제 기록됨) → `dataset_catalog`(git_rev/commit_subject/dvc_md5) 해석해 `training_summary["dataset"]`에 넣도록 배선.
- **검증**: DVC-sourced trainset으로 학습 → MLflow run(:5500) 페이지에 `dvc_git_rev`/`dvc_commit_subject` 파라미터 표시 확인. (단위: 해석 헬퍼를 mock db로 테스트.)

### M-2 — eval GPU 스코어링 (`src/vlm_pipeline/defs/train/eval.py` `_score_candidate`/`_score_incumbent`)
- **트리거**: 학습된 candidate 체크포인트 + sealed test split GT 존재 **+** prod GPU + `sam3.eval`/모델 로드 가능.
- **할 일**: 두 스텁(`NotImplementedError`)을 (a) `_trainsets/<id>/splits/test` GT 로드(테스트 가능 부분) + (b) candidate/incumbent 체크포인트 추론(GPU) → 이미 있는 `_score_sam3_predictions`(box mAP) / `_score_pe_core_recall`(recall@k) 호출로 구현. incumbent 없으면 `stock_base` 경로.
- **검증**: 정비 윈도우 prod GPU에서 `train_eval_gate` asset 실행 → `NotImplementedError` 없이 metrics 산출 + `model_registry.metrics`/`incumbent_metrics` 기록 + gate가 `promotable` 전이. (게이트 결정 로직 자체는 이미 검증됨.)

### L-3 — reconciliation 센서 / compose 배선 (`defs/train/catalog_ingest.py`, `docker/*compose*.yaml`)
- **트리거**: DVC Tier-2를 실제 운영(엔지니어들이 dvc push 시작).
- **할 일**: compose dagster 서비스에 bare repo(`/srv/data-repos/dvc-datasets.git`) bind-mount + `DVC_DATA_REPO_PATH` env, `git config --system --add safe.directory` 확정, reconciliation 센서 Dagster UI에서 enable. (재기동 동반 → 정비 윈도우.)
- **검증**: git push 없이 지연 `dvc push` 시나리오 → 센서 tick가 `pending_missing_dvc_objects`→`available` 전이 확인(H-4 self-heal과 연동).
