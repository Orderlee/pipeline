# frames_captions 프롬프트 뱅크 기하 분석 확장 — 설계

- 날짜: 2026-07-31
- ⚠️ 2026-08-19 개명: 본문의 frames_captions = 현 `frames` — 파일명·`frames_bank` 경로는 개명
  대상 아님. `stage_slim` 금지는 코드 가드가 프로필 dataset 값으로 강제하므로 개명과 무관하게 유효.
- 상태: 사용자 승인됨 (접근안 1: 프로필 확장 + 원장 어댑터)
- 선행 작업: source-h 뱅크 기하 분석 (`docker/analysis/prompt_geometry.py`,
  `docs/prompt-analysis-report-2026-07-31.md`)
- 토론: cto 페르소나 + codex 2차 의견 — 수렴 (아키텍처 이견 없음, 사실 정정 반영)

## 1. 목표 / 비목표

**목표**: source-h에서 검증된 프롬프트 뱅크 분석(어느 뱅크가 이기나, 예측이 왜 바뀌나,
어디가 커버리지 공백인가)을 FiftyOne `frames_captions`의 프레임 187,994장에 이식한다.
GT(사람 검수)와 뱅크(도메인별 CSV)가 파이프라인에서 축적될수록 분석 축이 자동으로
열리는 **축적형** 구조로 만들고, 산출은 전부 FiftyOne에서 시각적으로 비교 가능해야 한다.

**비목표**:
- Dagster 편입 — 2단계로 보류. 그때도 원장 생산자만 asset이 되고 분석기는 그대로 남는다.
- source-h 전용 스테이지의 이식 — `analyze`(팩토리얼: 같은 도메인에 뱅크 2벌 필요),
  `guide`(FN 구조율: 클래스별 GT 분모 필요), `flips`(GT 필수), `ablate`(저작 루프 없음),
  `slim`(§5 파괴 위험). 자격이 생기면 source-h 프로필의 검증된 코드가 그대로 열린다.
- 캡션 모달리티 11,978건 채점 — 같은 `image_embedding` 필드에 **텍스트 벡터**가 들어
  있어 이미지처럼 채점하면 조용한 오염이다 (실측 확인됨).

## 2. 확정된 결정 (재론 금지)

1. **축적형 모델 (축이 2개: 뱅크, GT)** — 분석은 다음 사다리로 점진 활성화되며,
   매 실행 커버리지 스탬프를 찍는다:
   - 0단계 (매핑 없음): 모든 채점 스테이지 hard-skip, 스탬프만 출력. 가짜 산출물 없음.
   - 1단계 (도메인에 뱅크 매핑됨): 그 도메인 프레임에 GT-free 축 전부 —
     `bank_pred`/`bank_decision_margin`/`bank_shift`/공백지도/리뷰 큐.
   - 2단계 (finalized GT 축적): GT 의존 축(정오 판정, 플립, min-n 통과 시 recall) 활성화.
2. **project→도메인 뱅크 매핑 테이블** (YAML) — 뱅크 등록된 도메인만 채점.
3. **analysis 스택 표준화** (`docker/analysis`) — CI 배포 없음, `docker cp` 파일 단위 배포.
4. **접근안 1** — 단일 파일 `--profile` 파라미터화 + PG 원장 어댑터. 공용 lib 패키지 반대
   (drift 표면 최소화, 수학 포크 방지).

## 3. 실측 사실 (설계 근거)

| 사실 | 값 | 함의 |
|---|---|---|
| frames_captions 구성 | frame 187,994 + **caption 11,978** (modality 필드) | `modality=='frame'` 필터 필수 |
| GT 조인 키 | `image_id` → PG `image_labels`/`image_label_annotations` | 폴더 파싱 불필요, DB 정확 조인 |
| 오늘 GT | finalized 부모 288행 / **이미지 248장** / 박스 1,558개, 전부 sourcej, 카테고리 patient/person | GT 낟알 3종을 원장에 구분 기록; 뱅크 클래스와 무손실 대응 안 됨 |
| 뱅크 정본 | source-h(domain 8)만 CSV 확보. **source-h은 image_metadata 0행 → frames_captions에 없음** | day-1 은 (뱅크)∩(존재)∩(정합) = 공집합 |
| NAS 뱅크 | `/home/user/mou/userwatch/prompts/` 48버전, 도메인 0~13 | 매핑 시드는 노션 버전관리 페이지가 정본 |
| 임베딩 | PE-Core-L14-336 1024-d, 뱅크와 동일 인코더 (cosine=1.0 검증) | 채점 수학 그대로 이식 가능 |
| weak 라벨 | SAM3 auto_generated 454k; normalized_class 는 none/person/fall/… | 뱅크 클래스체계와 다름, GT 아님 |
| 호스트 | RAM available ~22G, 스왑 소진 이력, 루트 97% | 메모리 preflight 필수 |
| 온톨로지 불일치 (기존) | "person on the ground": FiftyOne→`person`, LS→`falldown`; LS는 smoking→smoke 병합 | fail-closed crosswalk 필요 |

## 4. 아키텍처 — 데이터 계약 불변, 생산자만 교체

```
[PG: image_labels ⨝ image_label_annotations ⨝ image_metadata]     [NAS 뱅크 CSV]
        │ (신규) frames_bank_ledger.py                                  │
        ▼                                                               ▼
   ledger.jsonl  ──────┐                                        bank 스테이지 → <ver>.npz
   (source-h과 동일 포맷)  │                                                │
                       ▼                                                ▼
        prompt_geometry.py --profile frames    ← bank_domain_map.yaml
                       │  score / gap / viz / report
                       ▼
        FiftyOne frames_captions: 필드 6개 덮어쓰기
          + ds.info["bank_run"] + run ledger append
```

원칙: **분석기는 배열만 알고, DB는 원장 생산자만 안다.** GT 소스가 폴더명(source-h)이든
PG 조인(frames)이든 `ledger.jsonl` 포맷이 같으므로 검증된 수학이 포크 없이 재사용된다.
`src/` 5계층 규율(생산자는 DB를 알고 분석기는 배열만)의 분석 스택 미러링이다.

## 5. 구성요소

### 5-1. `prompt_geometry.py` 수정

- `--profile sourceh|frames` → `{DATASET, ROOT, PROMPT_DIR, CLASS_NAMES, 뱅크쌍}` 교체.
  기본값 `sourceh` (기존 호출 무변경).
- `gt_class` nullable(-1)화 — 모든 GT 소비 지점에 `mask = gt >= 0` 강제 + n 출력.
- `class_sims()` 스트리밍화: 이미지 배치(1024) × 프롬프트 블록(2048) → 즉시 running
  max/argmax 로 접고 블록 폐기. 전체 유사도 행렬 미상주 (기존 코드는 `[N, prompts]`
  전체를 할당 — 200k×16,125 fp32 ≈ 12GB 로 스왑 쓰래싱 재연 위험).
  피크 ≈ 234MB. 영구 산출은 `[N,C] best + [N,C] argmax` ≈ 6.4MB.
- fp32 유지 (margin ~0.01, fp16 금지). 집계 CI 계산만 fp64.
- `--mem-budget-gb` (기본 4): 시작 시 `MemAvailable < 2×budget` 이면 **시작 거부**.
  `OMP_NUM_THREADS` 상한 설정 (공유 호스트 보호).
- **`stage_slim` 데이터셋 가드**: `DATASET != 'source-h'` 이면 즉시 abort. 하드코딩
  삭제 리스트(`SLIM_DROP_*`)가 frames_captions 의 `emb_viz`/`text_search`/`proj:` 뷰
  21개를 파괴하기 때문. (포팅에서 가장 조용하고 비싼 사고 지점 — cto·codex 공통 지적)
- 통계 스테이지(동일예산 등, S 상주 필요)는 frames 프로필에서 층화 서브샘플 20k 상한.

### 5-2. `frames_bank_ledger.py` 신규 (소형)

- FiftyOne 에서 `modality=='frame'` 샘플의 `(sample_id, image_id, project, minio_key)` 추출.
- PG **좌조인**: `image_labels`(finalized) 부모 기준 — 무박스 finalized 이미지는
  `__no_box_finalized__` 규칙으로 `normal` GT 보존 (inner join 이 이를 조용히 버리는
  기존 QA 쿼리 함정 회피).
- crosswalk 적용(§6) 후 source-h 과 **동일 포맷 `ledger.jsonl`** + `gt_snapshot_sha` 산출.
- 분석기는 psycopg 를 영원히 모른다.

### 5-3. `frames_bank_eval.sh` 신규 (원커맨드 래퍼)

- `./frames_bank_eval.sh [도메인...]` → ledger → bank → score → gap → viz → report.
- `bank_eval.sh` 컨벤션 유지: 파일 단위 `docker cp`, 이번엔 geometry + ledger
  어댑터 + YAML 3개를 복사 (ambient `/workspace` import 의존 금지).

### 5-4. `bank_domain_map.yaml` 신규 — 3중 매핑, fail-closed

```yaml
domains:
  fire_smoke:                 # 예시 — 실제 번호/버전은 노션 버전관리 페이지에서 시드
    projects: [fire_smoke]
    bank_a: <구버전>           # A/B 슬롯 2칸 고정 (버전당 컬럼 아님)
    bank_b: <신버전>
class_crosswalk:              # box category → frame class. 미등재 = 채점 제외(fail-closed)
  fire: fire
  smoke: smoke
  patient: __unmapped__       # crosswalk 등재 결정 전까지 GT 축 제외 (§9-3)
  __no_box_finalized__: normal
unsupported_classes: [smoking]  # 뱅크 프롬프트 0개 → status=unsupported ("0% recall" 표시 금지)
```

- SAM3 `none` → `normal` 승격 금지. 퍼지 문자열 조인 금지.
- crosswalk 는 버전드 (YAML 에 `crosswalk_version` 키) — 바뀌면 `gt_snapshot_sha` 도 바뀐다.

## 6. FiftyOne 표면 — 버전 중립 필드 6개 상한

| 필드 | 타입 | 의미 | GT 필요 |
|---|---|---|---|
| `bank_domain` | str/null | 채점 게이트 (null=미채점) | ✕ |
| `bank_pred` | str | 신뱅크(B슬롯) 승자 클래스 | ✕ |
| `bank_decision_margin` | float | top1−top2 (확신도, 비음수). **GT margin(부호 있는 `score(GT)−max(other)`)과 용어 분리** — 전 프레임 사분면은 "확신도 비교"이지 "누가 맞추나"가 아니다 | ✕ |
| `bank_shift` | str | A→B 예측 전이 라벨 (`fire→normal` 등; A 예측은 여기서 복원) | ✕ |
| `bank_gap` | int/null | 저확신 꼬리 군집 id (공백지도) | ✕ |
| `bank_gt` | str/null | finalized 사람검수 frame class (null=GT 없음, 정상) | ✓ |

- 안 만드는 것: `bank_pred_a`(shift 에서 복원), per-version margin, `class_best_*`,
  `why_*` 텍스트, SAM3 재인코딩(이미 `normalized_class` 존재).
- 버전 정체성: `ds.info["bank_run"] = {a, b, domains, n_scored, n_gt, run_id, ts}` +
  run ledger. 48버전 환경에서 버전-인-필드명은 스키마 누수.
- 뷰: 기존 `proj:` 컨벤션 확장 — `bank: <domain> <용도>`. 사이드바에 접힌 그룹 1개
  (`⑥ 프롬프트뱅크`). `active_fields` allowlist 는 건드리지 않는다.

## 7. GT·통계 게이트 (정직성 장치)

- **커버리지 스탬프**: 모든 스테이지 첫 줄에
  `뱅크 도메인 n / GT n(이미지 낟알) / 전체 187,994` — 자격 미달 시 이유 출력 + hard-skip.
  매핑이 비어 있는 day-1 은 §2-1 의 0단계(스탬프만)로 동작한다.
  (cto: "n=0 표가 작동하는 시스템처럼 보이는 것"이 최대 리스크)
- **min-n 게이트**: GT 0 → `NA/no_gt` (0% 표시 금지) · 1~29 → 건수만 · 30~99 → Wilson
  CI "탐색적" 표기 · ≥100 이미지(+소스영상 ≥30) → 보고 가능. "승자" 배지는 불일치쌍
  ≥25 + McNemar + **영상 단위** 부트스트랩 CI 가 0 제외일 때만 (프레임 iid 아님).
- **weak 라벨(SAM3)**: 지표명 `concordance` 고정 (`accuracy`/`recall` 금지 —
  `sam3_shadow_compare` 의 "게이트 아님" 선례 재사용). `bank_gt` 에 절대 미기입.
  허용 용도는 리뷰 큐 랭킹(`decision_margin 낮음 × weak 불일치`)까지 — 이 큐가 LS
  검수로 흘러 GT 가 축적되는 것이 "파이프라인 흐름 적용"의 실체다.
  LS 태스크에 SAM3 라벨 프리필 금지 (앵커링이 weak 를 GT 로 조용히 승격시킨다).
- **리뷰 큐의 실체 (v1 범위)**: 저장 뷰 `bank: <domain> review-queue` (랭킹 정렬) +
  report 스테이지의 상위 N 목록 + 선택적 CSV export 까지. **LS 태스크 생성 자체는
  기존 장치(`ls_tasks.py`)의 몫으로 v1 범위 밖** — 이 설계는 후보를 고르는 데까지만.

## 8. 재실행 계약 (GT 가 매주 늘어나는 세계)

- 필드는 항상 덮어쓰기 + **도메인 샤드 단위 clear-then-set** (이전 채점 id 집합 −
  현재 집합 → None). 매핑 변경으로 빠진 샘플의 stale 값이 가장 악질적인 분석 거짓말.
- 원장 1행 (append-only, 기존 ledger 컨벤션 옆):
  `{run_id, ts, profile, domain, bank_a, bank_b, n_scored, n_gt_images, n_gt_boxes,
  gt_snapshot_sha, crosswalk_version, mem_peak_gb, metrics}`.
- `score_run_id` 와 `gt_snapshot_id` 분리 — GT 만 늘면 188k 재채점 없이 GT 오버레이만
  재계산 (채점은 임베딩+뱅크+코드의 함수, GT 와 무관).
- 지표는 두 벌 보고: (a) 현재 GT 전체 기준 (b) **직전 런 GT 교집합 기준** — GT 구성
  변화(새 프로젝트/클래스 유입)가 뱅크 개선으로 위장되는 것을 차단.
- GT 시각은 `gt_observed_at` (조회 시각) — 스키마에 finalization watermark 가 없으므로
  "finalized_through" 를 암시하는 이름 금지.

## 9. 에러 처리 / 검증 / 선행조건

### 9-1. 에러 처리
- per-도메인 fail-forward: 한 도메인 실패해도 나머지 진행, 원장에 오류 기록.
- 메모리 preflight 거부(§5-1), NAS/PG 접근 실패 시 해당 스테이지만 skip + 원장 기록.

### 9-2. 검증 3종
1. **source-h 프로필 회귀**: 리팩터링 후 기존 `geometry.json` 수치 재현 (무손상 증명).
2. **frames 드라이런**: 매핑 빈 상태 → 전 채점 스테이지 hard-skip + 공백지도/리뷰큐만
   산출되는지 확인.
3. **파일럿 end-to-end**: 도메인 1개 매핑 등록 후 채점→필드 6개→뷰→리포트 확인.

### 9-3. 선행조건 (데이터/운영)
1. 노션 버전관리 페이지에서 frames_captions 실재 프로젝트의 도메인 번호·버전쌍 확정
   → 매핑 YAML 시드. 1차 후보: `fire_smoke` (3,464장, 클래스체계 최근접).
   확정 전까지 v1 은 공백지도+리뷰큐 모드로 동작 (그 자체로 유효한 산출물).
2. sourcej 288장의 GT 축 편입은 crosswalk 에 patient/person 사상을 **결정한 뒤에만**.
3. `stage_slim` 은 frames_captions 에서 영구 금지 (코드 가드 + 이 문서).

## 10. 페르소나·codex 토론 기록 (요지)

- **수렴**: 축적형 GT/도메인 샤딩/analysis 스택 통합 아키텍처 이견 없음. 스트리밍
  리덕션, fp32, min-n 게이트, fail-closed crosswalk, clear-then-set, 버전 중립 필드 공통 권고.
- **cto 고유 기여**: (뱅크)∩(존재)∩(정합)=공집합 발견, stage_slim 파괴 위험, v1 5스테이지
  축소, GT 교집합 델타, 단일파일 프로필 형태.
- **codex 고유 기여**: 모달리티 혼합(caption 11,978 텍스트 벡터), GT 낟알 3종(288/248/1,558),
  decision_margin vs gt_margin 용어 분리, 온톨로지 3벌 불일치 실례, 무박스 finalized
  좌조인, min-n 수치 제안, score_run/gt_snapshot 정체성 분리.
- **불채택**: codex 의 공용 lib 패키지+zipapp 번들 (배포 컨벤션과 충돌, 데이터셋 2개에
  과함 — 대신 래퍼가 필요 파일 전부를 명시적으로 `docker cp` 하는 것으로 ambient
  import 문제 해소).
