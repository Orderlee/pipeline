# Pseudo-label 생성 & 품질평가 — 사용자 운영 가이드

> "어떤 식으로 해야 하는가"의 실무 절차. 설계·감사는 `docs/pseudo-label-qa-feasibility.md`,
> `docs/pipeline-flow-audit-2026-07-01.md` 참조.
> **핵심 전제: 품질평가에는 사람 GT(리뷰 확정)가 필요하다. GT 0이면 리포트는 비어 있음(정상).**

---

## 흐름 한눈에

```
[생성=자동] SAM3 bbox + Gemini timestamp + 생성시점 .pseudo.json 스냅샷 보존
        │
        ▼
[GT=사람]  Label Studio 리뷰 → finalize → image_label_annotations / labels(finalized)
        │
        ▼
[평가=사용자]  수치: Dagster asset(P/R/F1 표)   ·   육안: FiftyOne(FP/FN 필터)
```

---

## 1. 생성 (자동 — 사용자는 dispatch만)

- 영상/이미지를 dispatch(outputs 에 `bbox`, `timestamp` 포함)하면 파이프라인이 자동 생성:
  - **SAM3 bbox** → `vlm-labels/<src>/sam3_segmentations/<stem>.json` + `image_labels` 행
  - **Gemini timestamp** → `vlm-labels/<src>/events/<stem>.json` + `labels` 행
- **품질평가용 원본 스냅샷도 생성 시점에 자동 보존**(write-once):
  - bbox: `sam3_segmentations/<stem>.pseudo.json`
  - timestamp: `events/<stem>.pseudo.json`
  - ⚠️ 이 스냅샷 기능 **배포 이후 생성분부터** 유효. 그 전 것은 원본이 이미 LS 리뷰로 덮어써져 평가 시 `missing_pseudo`로 집계됨.

## 2. GT 확보 (품질평가 전제 — 사람 리뷰)

- Label Studio에서 자동 라벨을 검수 → **finalize**.
  - bbox → `image_label_annotations` (per-box 사람 확정)
  - timestamp → `labels.review_status='finalized'`
- 소량이라도 finalize 돼야 평가가 숫자를 냄. GT 0 → asset 은 빈 리포트로 정상 종료.
- 라이브 카운트 확인:
  ```bash
  ! docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
  SELECT 'bbox_gt' t, count(*) FROM image_label_annotations
  UNION ALL SELECT 'ts_gt', count(*) FROM labels WHERE review_status='finalized';"
  ```

## 3. 품질평가 — 수치 (Dagster UI, GPU 불필요)

Dagster UI(**http://10.0.0.10:3030**) → Assets → 아래 asset **Materialize**:

| Asset | 대상 | config |
|-------|------|--------|
| `pseudo_label_bbox_qa` | SAM3 박스 vs 사람 GT | `folder`(특정 프로젝트만, 없으면 전체) · `iou_threshold`(기본 0.5) · `score_threshold`(기본 0) |
| `pseudo_label_timestamp_qa` | Gemini 이벤트 vs finalized labels | `folder` · `tiou_threshold`(기본 0.5) |

Launchpad config 예:
```yaml
ops:
  pseudo_label_bbox_qa:
    config:
      folder: source-d      # 생략 시 전체
      iou_threshold: 0.5
```
- **결과 = asset 실행 메타데이터**: 클래스별 `precision/recall/f1/tp/fp/fn` markdown 표 + `macro_f1` + `missing_pseudo`(스냅샷 없어 못 센 수). materialization 이력으로 배치 간 추세.

## 4. 품질평가 — 육안 (FiftyOne, 어느 박스가 틀렸나)

analysis 컨테이너(`docker-analysis-1`):
```bash
# 배포 시 파일 수동 복사(이미지에 baked 안 됨)
docker cp docker/analysis/label_qa_fiftyone.py docker-analysis-1:/workspace/
docker exec docker-analysis-1 python /workspace/label_qa_fiftyone.py --folder <src> --iou 0.5
```
→ 격리 데이터셋 `pseudo_qa` 생성(공유 `frames` 무손상). **http://10.0.0.10:5153** 앱:
- 좌상단 데이터셋 셀렉터 → `pseudo_qa`
- `detections` 필드 `pseudo_qa == "fp"` = **오검출**(모델이 헛친 박스)
- `ground_truth` 필드 `pseudo_qa == "fn"` = **미검출**(모델이 놓친 박스)
- 또는 Evaluation 패널에서 클래스별 P/R/F1 + TP/FP/FN 클릭 필터

## 5. 해석 & 조치

- **precision↓** = 헛검출 많음 → conf threshold 상향 / 카테고리 정리.
- **recall↓** = 놓침 많음 → FiftyOne에서 `fn` 프레임 확인 → 재라벨 대상 / 모델 파인튠 후보.
- 수치(Dagster)로 "어느 클래스가 약한지" → FiftyOne으로 "왜/어느 프레임인지" 순으로 좁힌다.
- ⚠️ **timestamp는 event-level만**(카테고리 컬럼 없음). bbox는 클래스별.

## 주의

- Dagster 수치와 FiftyOne 수치는 매칭 구현이 달라(내 `box_map` vs FiftyOne coco eval) 미세 차이 가능 — Dagster=공식 집계, FiftyOne=육안용.
- 자기평가 금지: 이 QA는 **pseudo vs 사람 GT**만 비교. pseudo끼리 비교(자기평가) 아님.
