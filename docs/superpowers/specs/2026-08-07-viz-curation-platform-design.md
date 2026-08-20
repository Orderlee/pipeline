# 시각화·큐레이션 플랫폼 (FiftyOne 병행) — 설계/계획서

- 작성: 2026-08-07
- ⚠️ 2026-08-19 개명 — 본문의 frames_captions = 현 `frames`
- ⚠️ 2026-08-20 필드 세대 교체 — 본문의 `winner_gidx_v080` 필드는 리빌드가 삭제(현행은 신 태그
  `winner_gidx_v1080` 등). §불변식의 `set(frames.winner_gidx)⊆set(prompts.gidx)` 는 **gidx 블록
  세대가 같을 때만** raw 등식으로 성립 — 세대가 다르면 shift 보정 후 판정(소비자가 보정함).
- 검토 방식: 페르소나 병렬(pipeline-explorer / tech-scout / cto) + **codex 3라운드**
  (독립 의견 → cto 판정 반론 → 개정 판정 최종 검토). "자체 플랫폼" 포지션은 steelman 으로
  최강 형태를 세운 뒤 기각. 라이브 실측(도커 컨테이너·DB) 근거 병기.
- 선행 문서: `docs/apo-fiftyone-plan-2026-08-03.md` (Phase 0 진행 중) — 이 계획은 그 트랙의
  연장이며 충돌하지 않는다. 화면 `2-audit` 미완성분을 Phase 1 이 메운다.

---

## 1. 요구

### 원 요구
"FiftyOne 보다 좋은 플랫폼을 만들고 싶다. sourcei 와 sourcei-prompts 를 동시에 비교하고
싶은데 안 된다. sourcei samples+embeddings 창에 sourcei-prompts embeddings 창을 split
horizontal 로 붙여 비교 분석하고 싶다."

### 추가 요구 (2026-08-07 사용자)
| # | 요구 | 이 계획서의 대응 |
|---|---|---|
| R1 | FiftyOne 시각화마다 따로 코드를 만들어야 했다 — 파이프라인에 제대로 녹는, 현재 DB 를 활용하는 시각화 큐레이션 플랫폼 | §1.1 해석 분리 → R1a(거버넌스)+R1b(DB 직결 창) |
| R2 | 새 개발에 들어가면 지금은 기초단계니 FiftyOne 과 병행 | **병행 = 런타임 병행**(FiftyOne 유지+새 창 추가). 개발은 순차 — 2인 팀 2트랙 동시 개발은 둘 다 미완으로 끝난다 |
| R3 | Color by 시인성이 낮고 색이 랜덤 — 기본은 고시인성, 직접 커스텀 가능하게 | Phase 0 (신규 개발 아님 — §4) |
| R4 | topk 와 wave(분포 IoU) 둘 다 나와야 함 | Phase 0 절반 + Phase 1 규칙 토글 + Phase 2 rule 컬럼 (§4~6) |
| R5 | 프로젝트 단위 진행이 아니라, 전체 데이터에서 프로젝트를 선택 | Phase 2 근거 중 하나 (§6). 단 정정: 단일 데이터셋 안 project 선택은 FiftyOne 이 **이미 한다**(saved view 21개+`Match $in` 실측) — Phase 2 의 몫은 교차 데이터셋·규칙 교차·200K·DB 큐레이션 4가지 |
| R6 | 뱅크 문장들도 DB화해서 관리 | §6.2 확장 — 019 의 `sentence_storage='db_backed'` 모드 전환 + 멤버십 테이블. **2026-07 감사의 보류 해제 조건("두 번째 소비자가 프로그램으로 조회")이 이 요구로 충족됨** |
| R5-b | 프로젝트별 embedding 비교뷰 | 같은 데이터셋 내 project 비교 = **Phase 1 모드 B** (§5.1b — 같은 UMAP 좌표 공유라 가장 싼 모드, 조인 불필요). 교차 데이터셋 코호트 비교 = Phase 2 |

### 1.1 R1 해석 — 두 요구의 분리 (사용자 확인 반영)

R1 은 하나가 아니라 둘이다. 분리하지 않으면 잘못된 것을 만든다.

| | 실체 | 진짜 병목 | 해결 |
|---|---|---|---|
| **R1a** | 분석 코드가 파이프라인 밖 곁가지 — `prompt_geometry.py`(220KB) 등 대부분 git 미추적/`/workspace` 수동 복사본, 산출물은 CSV/FiftyOne 필드뿐 | 코드·산출물 **거버넌스** | 정본화 + 산출물 데이터 계약 (§6.2). UI 신규개발 아님 |
| **R1b** | "현재 DB 를 활용하는" 창 — 전체 데이터에서 project 선택(R5) | FiftyOne **데이터 모델 한계** | DB 직결 surface (Phase 2) |

R1a 가 더 싸고 더 급하다. R1a 없이 R1b 를 만들면 새 UI 도 CSV 를 읽는 **두 번째 곁가지**가
된다 (리스크 R7). 사용자 이해에 대한 보정 한 가지: 프레임 임베딩은 pgvector 에 100% 있지만,
뱅크 문장 벡터는 대부분 npz 파일이고(52버전 중 로컬 흡수 2개), topk/wave 점수는 CSV/FiftyOne
필드로만 존재한다 — "DB 활용"이 성립하려면 §6.2 의 데이터 계약이 먼저다.

⚠️ **"파이프라인에 녹인다" ≠ Dagster asset 편입.** 분석 코드가 `src/vlm_pipeline/` 에 들어가면
모든 변경이 prod 이미지 재빌드 + dagster recreate = **라벨링 중단**이다. 분석 표면은
`docker/analysis/` 에 머물고, "녹인다"는 **읽는 데이터가 DB 정본이 된다**로만 구현한다.

---

## 2. 판정 요약

### 기각 — "FiftyOne 을 넘는 자체 플랫폼 신규 구축"

steelman(최강 옹호) 논거 3개를 먼저 세웠다: (i) APO 고도화의 "운영팀·고객사용 프롬프트
엔지니어링 UI"는 FiftyOne 이 영원히 못 주는 제품 표면 (OSS 는 인증 경계가 없고
`fo.list_datasets()` 로 전 코호트 노출, `delete_media` 오퍼레이터 존재) (ii) 우리는 이미 App
번들 JS 패치 2곳·게이팅 우회 플러그인·mongo 캐시 런타임 패치·수동 기동으로 FiftyOne
유지보수를 부분 인수한 상태 (iii) 원본은 pgvector 이고 FiftyOne 은 매번 `overwrite=True` 로
재생성되는 파생 뷰다.

그럼에도 기각: 이 포지션이 이기려면 참이어야 할 5조건 중 3개 거짓 —
S1(고객사 UI 가 계약 산출물로 확정) 거짓, S2(다중 코호트 비교가 상시 워크플로) 거짓,
S3(연명 부채 > 연 X인월) 미측정, S4(안 쓰는 표면 실사용률 낮음) 참, S5(인력 2인 아님) 거짓.
정량(codex 바텀업): 광범위 parity **45~80인월** = 2인 풀타임 1.9~3.3년, 3년 TCO 63~116인월 =
2인 3년 용량의 88~161%, FiftyOne 라이선스 0원이라 절약분도 없음.

**기각의 실질 조건**: 조인·지표·규칙은 `prompt_geometry.py`(FiftyOne 비의존)와 DB 계약에
두고, 렌더러(FiftyOne이든 Phase 2 창이든)는 얇게 유지 — 미래 옵션 가격을 0 으로 유지한다.

### 기각 — 기타
- **joint UMAP**(문장+이미지 한 좌표계): 실측 기각 유지 (text↔image cos 중앙 0.147 vs
  text↔text 0.631 — modality 두 덩이). Phase 2 에서 "우리 렌더러니까 가능"이라는 논리로
  되살리지 말 것.
- grouped dataset / `merge_samples`: 설계 목적 불일치 (문서 검증).
- 창 2개(포트 분리): compose 가 `FIFTYONE_PORT` 하나만 노출해 **무개발이 아니다**
  (codex 소스 대조). blocking gate 로 쓰지 않고, H2 판정(아래) 시에만 실행.

### 채택 — 3단 사다리 (병행 운영, 순차 개발)

| Phase | 내용 | 공수 | 시점 |
|---|---|---|---|
| **0** | 색상(R3) + rule_cross 워크스페이스(R4 절반) + Panel 스파이크 go/no-go + 위생 | **1인일** | 즉시, 비차단 |
| **1** | FiftyOne Panel `user-prompt-compare` — 원 요구(split 비교, 모드 A) + 프로젝트별 비교(모드 B) | **5~7인일** | Phase 0 스파이크 go 시 |
| **1.5** | **R1a 거버넌스 게이트** — canonical 산출물 3종(§6.2 스키마와 컬럼 정렬) + validator, 분석 코드 git 추적화. 산출물이 CSV·NPZ·JSON·FiftyOne 필드로 분산돼 있어 "이름만 맞추면 공수 0"이 아니다 (codex 정정) | **2~4인일** | Phase 1 과 병행 가능. **Phase 2 착수의 선행 조건** — 이게 없으면 Phase 2 도 CSV 읽는 두 번째 곁가지(R7) |
| **2** | DB 직결 비교 surface (Streamlit :8503 확장) — R1b·R6 완결 | 스파이크 1 + 본공사 **9~16인일 ROM** (2D 투영 사전계산 경로 2~5 + handoff 오퍼레이터 1.5~2.5 포함) | 예정. Phase 1 실사용 관측 + Phase 1.5 완료 후 |
| 3 | 자체 플랫폼 | 45~80인월 | **하지 않음** |

### 사용자 확인 사항 (계획 승인 시 체크)

- ✅ **H1 확정** (2026-08-07 사용자): "Samples / 이미지 embedding / 프롬프트 embedding" 3-패널
  split + 프로젝트별 embedding 비교뷰(R5-b). H2(양쪽 네이티브 그리드)는 요구 아님으로 확정 —
  창 2개 경로는 만들지 않는다.
- ☑️ **Phase 2 정지 규칙** (§6.1) 동의 여부 — "FiftyOne 이 못 하는 4가지만 + 미디어 표면은 새로 만들지 않음".

---

## 3. 확정 사실 (라이브 실측)

| 사실 | 값 | 의미 |
|---|---|---|
| 조인이 산술적으로 닫혀 있음 | `sum(sourcei-prompts.wins)` = 7,498 = `sourcei.count()` | 프레임 전체가 채택 문장 314개에 완전분할 귀속. 새 지표·필드 0개로 연동 가능 |
| 채택률 | 314 / 12,480 (2.5%) | 미채택 97.5%는 정방향 조인이 빈다 — UI 에서 "예비군" 표기 필수 |
| 조인 필드 방향 | `sourcei`(프레임).`winner_gidx_v080` ↔ `sourcei-prompts`(문장).`gidx` | codex 소스 대조 확정 (`prompt_geometry.py:1288` / `:2516`) — 뒤집으면 안 됨 |
| 조인의 규칙 종속 | 이 귀속은 **K=1 전역 argmax** 기준 | 제품 규칙과 다른 값 — 배너 문구도 그렇게 |
| **규칙 3벌 명명 (혼용 금지)** | `argmax_k1`(프레임→문장 귀속, `winner_gidx_*` 산출) / `topk_vote`(제품 K=10 다수결, `wave_vs_topk` 의 비교 대상) / `dist_iou`(wave) | "topk"라는 한 단어가 K=1 귀속과 K=10 제품 규칙 둘 다로 불리고 있었다 (codex 소스 대조: `prompt_geometry.py:103` "argmax 는 그 규칙의 K=1 특수해", RULE_K=10). 코드·UI·스키마 전부 이 3벌 명칭 사용 |
| FiftyOne 의 project 선택 | **이미 동작** — saved view `proj: <name>` 21개, `Match {"project":{"$in":[...]}}` 실측 기록 (`docs/runbook/fiftyone-operations.md`) | R5 의 근거를 "구조적 불가"로 쓰면 틀린다 — Phase 2 근거는 §6 의 4가지로 정정 |
| Panel API | `fiftyone.operators.Panel`·`PlotlyView`·`split_panel(horizontal)`·`set_extended_selection` 전부 1.19.0 실존 | Phase 1 성립. 단 cross-dataset 로드는 비공식 조합 |
| ColorScheme | `fo.ColorScheme` 존재, `sourcei.color_scheme=None` | R3 는 세팅 문제지 개발 문제가 아님 |
| R4 필드 | 프레임: `wave_pred_*`·`wave_vs_topk_v080`·`rule_cross`·`pred_*`. 문장: `wave_gain`·`wave_role` | 두 규칙 표시 재료가 이미 있음 |
| DB 쪽 프롬프트 기반 | 마이그레이션 018/019/020 (generation_prompts, prompt_banks+bank_sentences, lineage 뷰) | 입력 계약은 이미 충족 — 019 의 `external_only` 포인터 설계 유지 |
| 호스트 | RAM 가용 17G, swap 1G 만재, 컨테이너 mem_limit 전무 | 코드 상한이 유일한 방어선 |

---

## 4. Phase 0 — 즉효 4건 (1인일, 비차단)

| | 작업 | 공수 |
|---|---|---|
| 0-1 | **ColorScheme (R3)**: `sourcei`/`sourcei-prompts`/`source-h`/`source-h-prompts` 4개에 같은 팔레트. 분류형은 색맹 안전 팔레트(Okabe-Ito 8색 또는 Tableau10) + **값→색 명시 매핑**(fire/smoke/falldown/normal 은 전 데이터셋·전 워크스페이스 동일색). 연속 float 은 Color by 에서 색이 안 나오는 실측 함정 → `margin_bin` 식 구간화 필드 병행. App Color 설정 UI 저장이 `app_config.color_scheme` 과 같은 경로인지 확인 — 되면 "직접 커스텀"은 개발 0, 안 되면 `set_color_scheme.py` 스크립트 1개. `active_fields` allowlist 정리 동반 | 0.5 |
| 0-2 | **rule_cross 워크스페이스 (R4 절반)**: 네이티브 Embeddings Color by=`rule_cross`(두 규칙이 갈리는 프레임) + sidebar `wave_vs_topk_v080` 필터를 워크스페이스로 저장. 필드 신규 0 | 0.2 |
| 0-3 | **Panel 스파이크 (go/no-go)**: ① 네이티브 Embeddings lasso 가 Panel `on_change` 로 도달하는가(이벤트 종류 확인) ② 콜백에서 `fo.load_dataset(B)` + PlotlyView 렌더 성립 ③ 12,480점 하이라이트 patch 왕복 체감 지연. **no-go 면 Phase 1 생략, Phase 2 직행** (R2 가 병행을 허용했으므로 이 경로가 열려 있다) | 0.3 |
| 0-4 | `sourcei` 프레임에 있는 `wave_gain`/`wave_role` 산출 경로 확인 — 정의상 문장 단위 양(LOO)이므로 승자 문장 값의 복사본이면 Panel 은 prompts 쪽에서만 읽는다 | 0.1 |

추가 권고 (0.1인일): `docker/analysis/requirements.txt` 의 `fiftyone` 무핀 → `fiftyone==1.19.0`
핀. 지금은 재빌드 한 번이 플러그인 3종 + 번들 패치 + brain_key 가정을 동시에 깰 수 있다 (리스크 R8).

---

## 5. Phase 1 — `user-prompt-compare` Panel (5~7인일, 모드 2벌)

### 5.1 구성 — 모드 A (프레임 ↔ 문장, 원 요구)
- 파일: `docker/analysis/plugins/user-prompt-compare/__init__.py` (+ `fiftyone.yml`)
- `sourcei` 세션에서 열림. Panel 이 서버사이드에서 `fo.load_dataset("sourcei-prompts")` 로드.
- 배치 (H1 확정안): **Samples / 네이티브 Embeddings(프레임 `emb_viz`) / Panel 문장 산점도**
  3-패널 워크스페이스 (`split_panel(horizontal)` + `save_workspace("compare")`).
  문장 산점도는 Plotly `scattergl`, 문장 `emb_viz` 좌표.

### 5.1b 모드 B — 프로젝트별 embedding 비교 (R5-b, +1인일)

같은 데이터셋(예: `frames_captions`)의 project 슬라이스 2개+를 서버사이드 뷰로 뽑아 **같은
`emb_viz` 좌표 위에** 나란히(서브플롯) 또는 겹쳐(overlay 토글) 그린다. 모드 A 와 결정적으로
다른 점: 슬라이스들이 **하나의 UMAP fit 을 공유하므로 좌표 직접 비교가 유효**하다 — "project
A 는 이 군집이 비었다" 같은 공간 비교가 여기서는 정당하다 (§5.4 배너 3 은 모드 A 전용).
조인 계약 불필요, 네이티브 Embeddings 5,000점 상한도 우회 — 기술적으로 가장 싼 모드.
project 슬라이스가 `MAX_POINTS` 초과 시(cohort-b 7.3만 등) 층화 서브샘플 + 경고 표시.

### 5.2 조인 계약 (코드 도크스트링에 그대로)
| 방향 | 동작 |
|---|---|
| 문장→프레임 | 문장 클릭 → `winner_gidx_v080 == gidx` 프레임을 좌측 하이라이트 (`set_extended_selection` 기본 / `set_view` 토글) |
| 프레임→문장 | 좌 선택 변화 → `winner_gidx_v080` 집합 → 우측 점 하이라이트 + 상위 문장 표(`wins`/`purity`/`n_cameras`/`wave_gain`) |

좌→우 실배선은 다단계다: `좌 lasso → 세션 selection 변경 → Panel on_change → id→winner_gidx
→gidx 매핑 → 우측 Plotly patch`. 이벤트 종류는 0-3 스파이크가 확정한다.

### 5.3 R4 배선 — 규칙 토글이 "클릭 가능 여부"를 바꾼다

토글 2개로 만들면 두 규칙을 같은 종류로 오독한다. 정직한 배선:

| 모드 | 좌표 | 색 | 크기 | 클릭 |
|---|---|---|---|---|
| `argmax_k1` | 문장 UMAP (불변) | `adopted`/`purity_tier` | `wins` | **프레임 하이라이트 가능** (귀속 존재) |
| `dist_iou` (wave) | 동일 좌표 | `wave_role` (클래스 내 백분위) | 균일 | **불가** — "이 규칙에는 프레임 귀속이 없습니다. 기여도는 전역 LOO(`wave_gain`)" 안내 |

R4("둘 다 나와야 함")의 정직한 이행 경로: Phase 0 `rule_cross` = 두 규칙 **불일치 요약**,
Phase 1 토글 = **전환 표시**, 진짜 **나란히(side-by-side) 비교는 Phase 2** 에서 완결된다
(`rule` 컬럼으로 같은 코호트·같은 뱅크의 두 run 을 두 트레이스로). 계획 승인 시 이 순서에
동의하는 것으로 간주한다.

### 5.4 UI 계약 — 오독 방지 3종 (배너 없는 배포 금지)
1. 규칙 배너: "이 조인은 **K=1 전역 argmax**(`argmax_k1`) 승자 기준 — 제품 판정규칙
   (`topk_vote` K=10 다수결, `dist_iou`)과 다른 값" (상단 고정).
2. **12,480 전체 표시**: 미채택 12,166 은 회색/저 opacity + "채택만 보기" 토글. 숨기면 문장
   공간 밀도를 오독. 미채택 클릭 시 "가져간 프레임 0 — **예비군**" + `wave_gain`/`purity`
   (실측: 예비군이 새 카메라 승자의 66% — 쓸모없음으로 보이게 하면 안 된다).
3. 좌우 UMAP 좌표계 공간 비교 금지 배너 (**모드 A 전용**): 두 UMAP 은 독립 fit. 연결은 선택
   하이라이트로만. 모드 B(같은 데이터셋 project 비교)는 반대로 "같은 좌표계 — 공간 비교 유효"
   안내를 띄운다. 모드마다 배너가 다른 것 자체가 오독 방지 장치다.

### 5.5 렌더/RAM 가드레일 (코드 상수)
- 콜백 내 UMAP/t-SNE fit 금지. 좌표는 사전계산 `emb_viz` 만 (brain_key 하드코딩).
- `scattergl` 강제, `MAX_POINTS=20_000`, 초과 시 층화 서브샘플+경고.
- `embedding`(1024-d) 필드 절대 미로드 — 좌표 N×2(≈100KB)+스칼라 메타만.
- base payload 는 mount 당 1회, 이후 `selectedpoints`/오버레이 patch 만 (트레이스 재전송 금지).
- 프로세스 캐시: key `(dataset, "emb_viz", last_modified_at)`, 엔트리 1, 상한 64MB.
- Panel 상주 예산 ≤100MB. 배포 조건: 패널 열기 전/후 App RSS 실측 첨부.
- `fiftyone.yml` `>=1.19,<1.20` (호환 게이트 — 설치 핀은 0-4 권고의 requirements 핀이 담당).

### 5.6 검증 — selftest (App 불필요, 업그레이드 게이트 겸용)
`python __init__.py` 로 불변식 3개 assert (`user-prompt-probe` `_self_check()` 패턴):
`sum(prompts.wins)==frames.count()` / `set(frames.winner_gidx_v080)⊆set(prompts.gidx)` /
`adopted ⟺ wins>0`. 셋째가 깨지면 producer drift 의심 — 이 계획보다 drift 해소가 먼저다.

### 5.7 배포·라우팅
- 정본 `docker/analysis/plugins/`(git) → `docker cp` → `/data/fiftyone/datasets/__plugins__/`.
  App 재시작 불필요, **prod dagster 재배포 미트리거** (라벨링 무중단).
- 구현 `ai-data-engineer` / 조인 의미 검수 `ai-modeler` / 성능·오독 리뷰 `codex` / 최종 `cto`.

---

## 6. Phase 2 — DB 직결 비교 surface (예정, Phase 1 실사용 + Phase 1.5 완료 후)

승격 근거 정정 (codex 소스 대조 반영): FiftyOne 은 **단일 데이터셋 안의 project 선택은 이미
한다** (saved view 21개 + `Match $in` 실측 — 운영 런북 기록). Phase 2 의 정직한 근거는 이 4가지다:
1. **교차 데이터셋 코호트 비교** (sourcei vs source-h vs frames_captions — FiftyOne 세션 모델의
   진짜 한계. 같은 데이터셋 안의 project 비교는 Phase 1 모드 B 가 먼저 해결)
2. **규칙 나란히 비교** (`argmax_k1` × `topk_vote` × `dist_iou` side-by-side — R4 의 완결)
3. **200K 초과 스케일 산점도** (Embeddings 패널 5,000점 상한 없음)
4. **R1b·R6** — DB 정본 위에서의 큐레이션 (뱅크 문장 관리·채택 판단이 Postgres 에 남는 창)

- 형태: 신규 앱이 아니라 **기존 Streamlit(:8503) 확장** (이미 pgvector 직결 + 팀 사용 중).
  데이터셋 스코프 필터(project/source_unit) 신설.
- 렌더러: 착수 시 **embedding-atlas Streamlit 컴포넌트 1인일 스파이크** 먼저 — (i) DataFrame
  2개 (ii) 안정 ID 선택 회수 (iii) 프로그래매틱 cross-highlight 확인, 안 되면 Plotly fallback.
  렌더러만 빌리고 조인·필터는 우리 것.
- 선행 조건: Phase 1.5 (R1a 게이트) + `fiftyone-mongo` mem_limit·wiredTiger 캐시 상한
  **compose 영구화** (프로세스가 하나 더 늘어나는 만큼 RAM 위생이 착수 조건).

### 6.0 2D 투영 사전계산 경로 (본공사 선행 — 빠지면 12GB 사고 재발)

`image_embeddings` 에는 `vector(1024)` 만 있고 2D 투영 컬럼이 없다. 200K×1024×4B ≈ **0.82GB**
를 매 렌더마다 로드하는 구조는 과거 188K→12GB 폭증 사고의 재발 경로다. Phase 2 는
**`projection_runs` + `projection_points(entity_id, x, y, project, model, params)`** 테이블
(또는 버전드 Parquet artifact)로 좌표를 먼저 물질화하고, 창은 사전계산 좌표만 읽는다
(Panel 의 §5.5 원칙과 동일). 이 producer/적재/인덱스가 ROM 의 +2~5인일이다.

### 6.1 정지 규칙 — FiftyOne 이 못 하는 4가지(§6 근거)만

**"새로 만들지 않는다" 원칙으로 표현한다** (codex 정정 — "썸네일은 전부 FiftyOne"은 이미
현실과 불일치: `embedding_dashboard.py` 에 4열 썸네일 그리드가 이미 있다): 미디어 브라우저의
**확장**(페이지네이션·라벨 오버레이·비디오 플레이어·태깅·뷰 DSL)을 새로 만들지 않는다.
기존 썸네일 그리드는 유지하되 선택점 미리보기 상한(`MAX_PREVIEW`)을 코드 상수로 박는다.
이 경계를 넘는 요구가 오면 그게 parity 경쟁 진입점이자 중단 판단 지점이다.
인증·멀티테넌시·고객사 노출은 Phase 2 에 없다 (S1 전환 시 별건 재론).

**미디어 상세가 필요할 때는 FiftyOne 으로 handoff 한다 — 딥링크가 아니라 계약으로**:
`?workspace=` URL 전환은 산발 크래시가 **서버 공유 세션을 타고 다른 탭까지 전파**되는 실측이
있어(운영 런북) 방어선이 될 수 없다. 대신: Phase 2 버튼이 ① `/datasets/<name>` 을 열고
② 버전드 handoff JSON(dataset, run_id, rule, predicate, entity ids)을 클립보드에 복사,
③ FiftyOne 쪽 소형 오퍼레이터 `user-open-handoff` 가 그것을 받아 `ctx.ops.show_samples(ids)`
실행 (자동 modal 오픈은 race 위험으로 금지). 공수 +1.5~2.5인일, ROM 에 포함.

### 6.2 데이터 계약 — 점수·뱅크 문장의 DB 승격 (R1a·R6 의 해결)

**원자 점수는 승격하지 않는다** — 프레임×문장 행렬은 계산 캐시다 (sourcei 9,360만 셀,
frames_captions 25억 셀 × 뱅크 52버전). 재현성의 진실은 입력 3개(프레임 임베딩 pgvector 100%
/ 뱅크 원본+checksum — **019 가 이미 담당** / 규칙 코드 버전)이고, 고정되면 점수는 결정적으로
재계산된다.

**뱅크 문장은 DB화한다 (R6)** — 019 가 이미 예비해 둔 경로다:
- **텍스트·멤버십·큐레이션 상태**: `prompt_banks.sentence_storage` 를 `'db_backed'` 로 전환하고
  `bank_sentences` 에 실제 문장 행을 적재 (`content_hash` 로 버전 간 중복 제거 — 52버전이
  문장을 대부분 공유하므로 고유 문장 수는 수만 단위). 다음 번호 마이그레이션에서
  `bank_sentence_membership(bank_id, sentence_ref, gidx, class_label)` 멤버십 테이블을 추가해
  "버전 A vs B 의 문장 diff"가 SQL 한 방이 되게 한다. 2026-07 감사가 이 레지스트리를 보류한
  해제 조건("두 번째 소비자가 프로그램으로 조회")은 R6 + Phase 2 창으로 충족됐다.
- **문장 벡터**: 전량 무분별 이관은 하지 않는다. 고유 문장(content_hash) 단위로, **분석에
  실제 쓰는 버전만** `image_embeddings`(entity_type=`'prompt'`) 에 흡수 — Phase 2 창이
  "이 문장의 최근접 프레임" 을 pgvector `<=>` SQL 로 바로 조회할 수 있게 된다.
  (재계산 비용도 낮다: /embed_text 7.5ms/문장 → 12,480문장 ≈ 94초.)
- **userwatch 원본 npz/CSV 는 읽기 전용 SoT 유지** — 제품(userwatch) 서빙 경로는 건드리지
  않는다. DB 는 "우리의 관리 계층"이고 원본의 대체가 아니다. `origin_uri`+`checksum` 으로
  원본↔DB 정합을 검증 가능하게 유지한다.

**집계 3층을 다음 번호 마이그레이션 후보로 승격** (018~020 이 아직 untracked 라 번호를 미리
박지 않는다 — codex):

| 테이블 | 키 | 내용 | 대응 |
|---|---|---|---|
| `prompt_eval_runs` | `run_id` | `bank_id`→019, `cohort_scope` JSONB, **`rule`**(`'argmax_k1'`\|`'topk_vote'`\|`'dist_iou'`), `rule_params`(K 등), `code_version`, `embedding_model`, `metrics` | R4 = rule 컬럼 |
| `prompt_sentence_stats` | `(run_id, gidx)` | `wins`·`purity`·`n_cameras`·`reach`·`gain`·`adopted` | 문장 표면 |
| `prompt_frame_pred` | `(run_id, frame_key)` | **`rule`(비정규화 복제)**·`pred_class`·`margin`·`winner_gidx` | 코호트 = `WHERE project` |

**계약의 핵심**: "귀속이 존재하는 규칙에서만 `winner_gidx` 가 산다"를 스키마가 강제한다.
PostgreSQL CHECK 는 다른 테이블을 참조할 수 없으므로(codex — 원안은 구현 불가였다) 자식에
`rule` 을 비정규화 복제하고 `(run_id, rule)` 복합 FK 로 부모와 일치를 강제한 뒤 행 단위 CHECK:
`(rule='argmax_k1' AND winner_gidx IS NOT NULL) OR (rule IN ('topk_vote','dist_iou') AND
winner_gidx IS NULL)`. `topk_vote`(K=10 다수결)에는 단일 승자가 정의되지 않으므로 NULL 이
맞다 — argmax_k1 만 귀속을 가진다.

이 3층이 생기면 CSV/FiftyOne 필드는 파생 뷰로 격하된다 — 그게 R1a 의 완결이다.

**시점 (codex 최종 판정 수용)**: 마이그레이션을 Phase 1 직후로 당기지 **않는다** — 빈
테이블은 CSV 관성을 못 막고, 배포 중단(dagster recreate)과 잘못 굳은 forward-only 스키마
비용만 즉시 발생한다. 올바른 시점은 **Phase 2 스파이크 go 와 같은 유지보수 배포**에서
migration + 첫 importer/writer + 첫 DB reader 를 한 번에 적용하는 것 (스키마가 실데이터·실소비자와
함께 태어난다). 그 전까지의 준비는 Phase 1.5 의 canonical 산출물 3종 + validator 가 담당한다.

마이그레이션 위생: `DO $$` 블록 파일당 1개(러너 결함, 005 사례), `CREATE OR REPLACE VIEW` 는
컬럼 변경 불가. migration 은 `src/vlm_pipeline/` 경로라 **prod 재빌드+라벨링 중단을 물므로**
배포 윈도우를 다른 main 머지와 합칠 것.

---

## 7. 하지 말 것 (기각 확정 — 되살리지 말 것)

1. 집계 마이그레이션을 소비자 없는 시점에 만들기 (죽은 테이블) — R6 로 소비자는 명시됐으나
   시점 판정은 확정: **Phase 2 스파이크 go 와 같은 배포에서 importer·reader 와 함께** (§6.2)
2. 뱅크 문장 벡터의 **전량 무분별** pgvector 이관 — 분석 대상 버전만 content_hash 중복 제거
   후 흡수 (§6.2 R6). userwatch 원본 npz 는 읽기 전용 SoT 유지
3. 프레임×문장 점수 행렬 DB 적재 (최대 25억 셀)
4. 분석 코드의 `src/vlm_pipeline/` 편입 (prod 재빌드 = 라벨링 중단)
5. Phase 2 에 인증·멀티테넌시·고객사 노출
6. FiftyOne 데이터셋 통합 (sourcei+frames_captions — 분모 오염)
7. joint UMAP 부활 (실측 기각 0.147)
8. 실시간/스트리밍 (배치 재계산 + 캐시로 충분)
9. React/JS Panel (팀 미경험 — 공수 상단 리스크), Panel 안 네이티브 그리드 재현 (불가 확정)

---

## 8. 리스크

1. **비공식 조합**: cross-dataset 서버사이드 로드는 1.19 문서에 없음. 완화: 버전 핀 2중
   (fiftyone.yml + requirements), selftest 를 업그레이드 게이트로.
2. **/workspace drift**: 정본↔런타임 이원화. 완화: 파일 상단 정본 경로 주석 + README sync.
3. **수동 기동 부채**: 재배포마다 App 재기동 — Phase 2 프로세스가 늘면 가중.
4. **공유 호스트 RAM**: swap 만재 + mem_limit 전무. `fiftyone-mongo` 상한 compose 영구화를
   Phase 2 선행 조건으로 격상.
5. **오독**: §5.4 배너 3종 + §6.2 NULL 계약. 배너 없는 배포 금지.
6. **producer drift**: selftest 불변식이 감지기. 깨지면 계획보다 drift 해소 먼저.
7. **R7 — Phase 2 가 두 번째 곁가지化**: Phase 1.5(R1a 게이트)를 실제로 하지 않으면 새
   창도 CSV 를 읽게 됨 — R1 을 못 푼 채 UI 만 늘어나는 실패 모드. Phase 1.5 를 Phase 2 의
   선행 조건으로 못 박은 이유.
8. **R8 — fiftyone 무핀 재빌드**: `docker/analysis/` 는 CI 재빌드 트리거 경로 안 — 핀 없이는
   임의 시점 최신화로 플러그인·패치·가정 동시 파손 가능. Phase 0 에서 핀.

---

## 9. 저장소 역할 원칙 (FiftyOne Mongo vs Postgres/pgvector)

| | FiftyOne Mongo (사이드카) | Postgres + pgvector |
|---|---|---|
| 정체 | 뷰어 내부 상태 — 샘플 메타, brain run 좌표, 태그, saved view. **일회용 렌더 캐시** (`overwrite=True` 재생성, 백업 없음, `down -v`=전소실) | **파이프라인 정본** — raw_files·labels·임베딩·프롬프트 계열. pg-backup 존재 |
| 벡터 검색 | 없음 (App in-memory) | partial HNSW ~50ms, SQL `<=>` |
| 조인 | 한 데이터셋 안에서만 | 전 파이프라인 테이블과 자유 조인 |

**판단 기준 한 줄: "사라지면 재계산 가능한가?"** — 가능(UMAP 좌표, 시각화 필드)이면 Mongo 에
있어도 된다. 불가능(사람의 채택/삭제 판단, GT, 뱅크 문장 원장)이면 반드시 Postgres.
FiftyOne 태그는 세션 중 임시 표식으로만 — 큐레이션 판단이 Mongo 에 갇히면 캐시에 원본을 쓰는 것.
단 Mongo 를 버릴 수는 없다 — 미디어 그리드·필터·오버레이·플레이어가 전부 그 위에서 돌고,
그 재구현 비용이 §2 의 45~80인월이다. 그래서 병행이다.

## 10. 참고

- 토론 기록: 라운드1(pipeline-explorer/tech-scout/codex 독립) → 라운드2(cto 판정+steelman →
  codex 반론 10건, 소스 대조 확정 4건: 조인 필드 방향·K=1 배너 문구·compose 단일 포트·
  requirements 무핀) → 라운드3(신규 요구 R1~R6 → cto 개정 → codex 최종 5건: 딥링크 기각→
  handoff 계약, 마이그레이션 시점=Phase 2 스파이크와 동시, cross-table CHECK 불가→비정규화
  복제+복합 FK, R5 "구조적 불가" 근거 오류 정정, R1a 실행 계획 누락→Phase 1.5 신설)
- 선행: `docs/apo-fiftyone-plan-2026-08-03.md`,
  `docs/superpowers/specs/2026-07-31-prompt-quality-viz-design.md`,
  `docs/runbook/fiftyone-operations.md` (project 뷰·`?view=` 복구 실측)
- 마이그레이션: `src/vlm_pipeline/sql/migrations/postgres/{018,019,020}_*.sql` (현재 untracked)
- ADR 권고: Phase 1 머지 시 `docs/references/adr-analysis-surface-scope.md` — "FiftyOne 은
  렌더러, 조인·지표는 prompt_geometry.py+DB 계약. 자체 플랫폼 기각 근거, 집계 스키마 초안,
  정지 규칙과 handoff 계약."
