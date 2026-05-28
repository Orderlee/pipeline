# 스프린트 Plan — 데이터 플랫폼 Foundation + 초기 분석

> **기간**: 2~3주 (2026-05-27 시작)
> **인원**: 3명
> **목표**: ML 데이터 드리븐 iteration 을 위한 **기본 인프라 구성** + **초기 데이터 분석** 까지.
> **비목표 (이번 스프린트 X)**: 모델 A/B 비교 framework, 오탐 mining 자동 loop, active learning 자동화, drift alert, curation 자동화 → 모두 **다음 스프린트**.
> **배경**: 이 파이프라인은 B2C/product 아님. 학습 데이터셋 구성·분석·오탐 개선·모델 개선용. BI 도구(Metabase/Grafana) 가 아니라 **CV/ML data tooling (FiftyOne 중심)** 이 맞음.

---

## 📌 0. 왜 지금인가

- 데이터 규모 아직 작음 (test×3 + appdata 2500) → 운영 안정화보다 **메트릭/평가 인프라 깔기 좋은 타이밍**
- 지금 인프라(PG + MinIO + Dagster)에 데이터는 쌓이는데 **"무엇이 들어있고, 라벨이 정확한가"** 를 보는 도구가 없음
- 이번 스프린트에 foundation 깔면, 데이터 늘어났을 때 같은 인프라 위에서 자동 분석/평가 가능

---

## 📌 1. 도구 스택 (확정)

| 도구 | 역할 | 비고 |
|---|---|---|
| **FiftyOne** (Voxel51) | CV dataset exploration + 모델 평가 + 유사도 | 오픈소스, image/video/bbox/classification native |
| **Jupyter** (표준 컨테이너) | ad-hoc 분석, FiftyOne 조작, 분포 노트북 | PG + MinIO + FiftyOne 사전 설정 |
| **pgvector** | 임베딩 저장 + 유사도 검색 | 기존 PG 에 extension 추가 |
| **임베딩 모델** | frame → vector (CLIP 또는 DINOv2) | open model, GPU 1 활용 |

**명시적 제외** (이번 스프린트 X): Metabase, Grafana, MLflow, W&B, CVAT. — 도입 시점은 각각 use case 명확해진 후.

---

## 📌 2. 트랙 구성 (3인)

| 트랙 | 담당 영역 | Foundation 산출물 | 초기 분석 산출물 |
|---|---|---|---|
| **A — FiftyOne + 모델 평가** | FiftyOne 도입, loader, eval | PG/MinIO → FiftyOne Dataset loader | Gold set + 모델 정확도 baseline |
| **B — Jupyter + 분포 분석** | 분석 환경, 분포 노트북 | Jupyter 표준 컨테이너 | 데이터셋 분포 리포트 (카테고리/카메라/duration/시간대) |
| **C — pgvector + 임베딩** | 벡터 인프라, 임베딩 추출 | pgvector + `image_embeddings` 테이블 | 유사도 검색 + 클러스터 데모 |

> cross-dependency: C 의 임베딩이 A 의 FiftyOne similarity / B 의 클러스터 분석에 쓰임. C 의 Week 1 임베딩 추출이 선행되면 A·B Week 2 작업이 풍부해짐. 단, A·B 는 임베딩 없이도 독립 진행 가능 (없으면 메타데이터 기반 분석).

---

## 📌 3. Week 1 — 인프라 구성

### A · FiftyOne 도입 + Dataset loader
- [ ] FiftyOne 컨테이너 prod/staging 스택에 추가 (또는 분석 전용 호스트)
- [ ] **PG → FiftyOne loader**: `raw_files` / `video_metadata` / `labels` / `image_labels` → FiftyOne `Sample` 변환
  - video sample: filepath(MinIO presigned 또는 NFS) + `labels`(events) + `video_metadata`
  - image sample: frame + `image_labels`(bbox) + `image_metadata`
- [ ] MinIO 객체 → FiftyOne media 접근 (presigned URL 또는 NFS 직접 mount)
- [ ] FiftyOne app UI 띄우고 appdata 일부 sample 시각 확인 (영상 + bbox overlay)

### B · Jupyter 표준 환경 + 접속 검증
- [ ] Jupyter 컨테이너 (PG psycopg + MinIO boto3 + FiftyOne + pandas/matplotlib/plotly 사전 설치)
- [ ] PG read-only user 분리 (`analytics_ro`) + 접속 헬퍼 노트북
- [ ] MinIO 접속 헬퍼 (버킷별 객체 list/download)
- [ ] 분석 노트북 템플릿 (재현 가능한 쿼리 → 차트 패턴)

### C · pgvector 활성화 + 임베딩 추출 파이프라인
- [ ] pgvector extension 활성화 (prod/staging PG)
- [ ] `image_embeddings` 테이블 설계 (`image_id`, `embedding vector(N)`, `model_name`, `created_at`)
- [ ] 임베딩 모델 선정 (CLIP ViT-B/32 또는 DINOv2 — GPU 1)
- [ ] 임베딩 추출 스크립트 (frame thumbnail → vector → PG 적재). 1차는 batch script, Dagster asset 화는 다음 스프린트
- [ ] 유사도 query 검증 (`SELECT ... ORDER BY embedding <-> $1 LIMIT 10`)

**Week 1 종료 기준**: FiftyOne 에서 sample 시각 확인 OK + Jupyter 에서 PG/MinIO 쿼리 OK + pgvector 유사도 query 동작.

---

## 📌 4. Week 2 — 초기 데이터 분석

### A · Gold set 구축 + 모델 평가 baseline
- [ ] **Gold set 설계**: 100~300 sample 추출 기준 (카테고리 균형 + 카메라 다양성 + edge case)
- [ ] ground truth 라벨 확보 (LS 기존 검수 활용 또는 수동)
- [ ] FiftyOne `evaluate_detections()` 로 SAM3 bbox precision/recall/IoU baseline
- [ ] Gemini 라벨 정확도 spot-check (gold set 영상의 event/caption 수동 대조)
- [ ] 평가 결과 PG 적재 스키마 (`model_eval_runs`) — 정기 측정 기반

### B · 데이터셋 분포 분석 리포트
- [ ] **차원별 분포 노트북**:
  - 카테고리별 (fire/smoke/weapon/violence/falldown) 이벤트 수
  - source_unit(카메라)별 검출률 / 평균 frames / 평균 events
  - duration 분포 / 해상도 분포 / 시간대(있으면) 분포
- [ ] **불균형 식별**: under/over-represented 카테고리·카메라 조합
- [ ] 분포 snapshot 저장 (`dataset_distribution_snapshots`) — 다음 스프린트 drift 비교 baseline
- [ ] 리포트 정리 (노트북 → 공유 가능한 형태)

### C · 임베딩 기반 유사도 / 클러스터 데모
- [ ] FiftyOne brain `compute_similarity` (pgvector backend 연결)
- [ ] FiftyOne brain `compute_visualization` (UMAP) — 임베딩 2D 시각화
- [ ] 클러스터 관찰: 어떤 그룹이 형성되나 (카메라별? 장면별?)
- [ ] "유사 sample 찾기" 데모 (sample 1개 → 유사 top-10)
- [ ] (시간 남으면) uniqueness 점수로 특이 sample 추출 데모

**Week 2 종료 기준**: Gold set 평가 baseline 숫자 확보 + 데이터셋 분포 리포트 1개 + 임베딩 유사도/클러스터 데모.

---

## 📌 5. Week 3 (buffer / 정리)

> Week 1-2 가 밀리면 흡수. 여유 있으면 아래 경량 작업.

- [ ] 분석 결과 종합 리포트 (분포 + 모델 baseline + 클러스터 관찰)
- [ ] Foundation 사용 가이드 doc (FiftyOne/Jupyter/pgvector 접속법 + 자주 쓰는 노트북)
- [ ] 다음 스프린트 입력 정리 (아래 §7 deferred 항목 우선순위)
- [ ] 회고

---

## 📌 6. 누적 Deliverable

| 영역 | 결과 |
|---|---|
| **인프라** | FiftyOne + Jupyter 표준 + pgvector + 임베딩 추출 스크립트 |
| **모델 평가** | Gold set (100~300) + SAM3 precision/recall/IoU baseline + Gemini spot-check |
| **데이터 분석** | 차원별 분포 리포트 + 불균형 식별 + 분포 snapshot baseline |
| **탐색** | 임베딩 유사도 검색 + UMAP 클러스터 시각화 |
| **문서** | Foundation 사용 가이드 + 분석 종합 리포트 |

---

## 📌 7. 다음 스프린트로 이연 (이번 X)

Foundation 위에서 자동화/심화하는 작업들:

| 항목 | 선행 조건 (이번 스프린트가 깔아줌) |
|---|---|
| 모델 A/B 비교 framework | Gold set + eval 스키마 |
| 오탐(FP)/누락(FN) mining loop | FiftyOne 평가 + 임베딩 유사도 |
| active learning 후보 자동 추출 | uniqueness/mistakenness + 임베딩 |
| drift 감지 + Slack alert | 분포 snapshot baseline |
| curation 자동화 (다음 라벨링 대상 제안) | 불균형 식별 + active learning |
| 임베딩 추출 Dagster asset 화 | 1차 batch 스크립트 |
| Claude Vision 등 모델 비교 추가 | 모델 비교 framework |

---

## 📌 8. 리스크 / 의존성

| 리스크 | 영향 | 대응 |
|---|---|---|
| FiftyOne media 접근 (MinIO presigned vs NFS mount) | A 트랙 Week 1 지연 | NFS 직접 mount 우선 시도 (`/nas/data` 단일 mount 로 단순해짐) |
| Gold set 라벨링 시간 underestimate | A 트랙 Week 2 밀림 | 100 부터 시작, 점진 확장 |
| 임베딩 모델 선정 지연 | C 트랙 Week 1 지연 | CLIP ViT-B/32 default 로 빠르게, 교체는 나중 |
| GPU 1 경합 (SAM3 + 임베딩 추출) | C 트랙 처리 지연 | off-peak batch 또는 소량부터 |
| 분석 요구사항 발산 | B 트랙 scope 초과 | "분포 리포트 1개" 로 MVP 한정, 심화는 다음 스프린트 |

---

## 📌 9. 인프라 메모 (2026-05-27 기준)

- NAS incoming/archive EXDEV 는 **docker 별도 bind-mount 가 원인**이었고 단일 `/nas/data` mount 로 해소됨 (PR #92) — archive_finalize 폴더 fast-path 정상화. 이번 스프린트 FiftyOne media 접근도 `/nas/data` 단일 mount 활용 가능.
- SAM3 는 단일 공유 컨테이너 `docker-sam3-1` (prod/staging 공유, PR #93) — 임베딩 추출 GPU 1 사용 시 SAM3 와 경합 고려.
- PG: prod `vlm_pipeline`, staging `vlm_pipeline_staging`. pgvector 는 양쪽 동일 적용.

---

## 📌 10. 참고

- [sprint-retro-2026-05-22.md](../sprint-retro-2026-05-22.md) — 직전 스프린트 (PG 전환 + NAS 마이그)
- [qa-scenarios-playbook.md](./qa-scenarios-playbook.md) — 운영 QA 시나리오 (운영 안정화 단계 진입 시)
- FiftyOne docs: https://docs.voxel51.com/
- pgvector: https://github.com/pgvector/pgvector
