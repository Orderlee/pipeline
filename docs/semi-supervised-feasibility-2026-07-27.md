# Semi-supervised Learning — 가능성 검토 및 방향 정립

> 작성일: 2026-07-27 · 선행 문서: [pseudo-label QA 진단](pseudo-label-qa-feasibility.md)(07-01) · [분석 스택 로드맵](analysis-stack-fiftyone-streamlit-roadmap-2026-07-27.md)(07-27)
> 질문: **"GT 248장 vs 미라벨 188K 프레임 — 이 비대칭을 semi-supervised learning(SSL)으로 활용할 수 있는가, 있다면 언제 어떤 형태로?"**

---

## 결론 (한 줄)

- 우리는 정확히 SSL이 가장 잘 듣는 low-label regime(라벨 0.1%대)에 있다 — **잠재 ROI는 실재**한다.
- 그러나 **train-time SSL(pseudo-label 학습)은 현행 자기학습 금지 불변식과 정면 충돌**하고, 충돌이 없더라도 **효과를 측정할 eval이 아직 없다**(게이트 스코어링 미구현 + sealed test GT 부족 + baseline 0회).
- 따라서 방향: **① 지금 = 불변식-호환 SSL(pre-annotation + AL 플라이휠)로 GT를 빠르게 쌓고, ② train-time SSL은 명시적 게이트 4개를 걸어 재검토 안건으로 봉인, ③ 라벨 전파·자동 승격류는 상시 기각.**
- 핵심 통찰: **SSL을 가능하게 만드는 최단 경로가 곧 기존 로드맵(GT 플라이휠)이다.** 새 워크스트림이 아니라 로드맵 P1-1에 스펙 한 줄(pre-annotation)을 추가하는 것.

---

## 1. 용어 정리 — SSL 스펙트럼 × 자기학습 금지 불변식

불변식(CLAUDE.md): *"모델 파생 라벨(`auto_generated`, Gemini 캡션, `vlm-classification`)로 학습/eval 금지. GT = LS `finalized` 또는 AL-선별-후-사람-어노테이트만."*

"SSL"이라 불리는 기법군은 불변식과의 충돌 여부가 갈린다:

| 기법군 | 미라벨 데이터 사용 방식 | 불변식 충돌 | 판정 |
|---|---|---|---|
| **(A) Pseudo-label pre-annotation** — SAM3 박스를 LS task에 predictions로 첨부, 사람이 수정·확정 | 모델 출력이 **사람 검수를 거쳐** GT화 | ❌ 없음 — 불변식의 GT 정의("사람-어노테이트")와 정확히 일치 | ✅ **즉시 채택** |
| **(B) Active Learning** — 임베딩 기반 하드샘플 선별 → 사람 라벨링 | 미라벨 풀에서 **무엇을 라벨할지만** 모델이 고름 | ❌ 없음 | ✅ 이미 로드맵 P1-1 |
| **(C) Self-supervised 도메인 사전학습** — 188K 프레임으로 backbone 적응 (라벨 자체를 안 씀) | 라벨 무사용 (contrastive/MAE류) | ❌ 없음 | 🟡 보류 (§4) |
| **(D) Train-time SSL** — FixMatch/Soft-Teacher/Unbiased-Teacher류: 모델 예측을 미라벨 프레임의 학습 타깃으로 사용 | 모델 파생 라벨이 **학습 신호**로 들어감 | ⚠️ **충돌** — 현행 문구는 eval뿐 아니라 *학습*도 금지 | 🔒 **게이트 후 재검토** (§3) |
| **(E) 라벨 전파 / 자동 GT 승격** — kNN 전파, confidence 기반 `finalized` 승격 | 모델 파생 라벨이 **GT 테이블**에 유입 | 🚫 정면 위반 | ❌ **상시 기각** (로드맵 §5에서 기확정) |

> 참고: Gemini 라벨은 "다른 모델이니 self-training이 아니다"는 논리가 성립하지 않음 — 불변식은 자기 출력이 아니라 **모델 파생 일반**을 금지 (Gemini 캡션 명시).

---

## 2. 왜 (D)는 지금 불가능한가 — 블로커 4개

정책을 개정하더라도, 오늘 train-time SSL을 돌리면 안 되는 이유:

1. **Supervised baseline이 0회** — `model_registry` 0행. SSL은 supervised baseline 위의 *delta*인데, 비교 대상이 없어 효과 귀속이 불가능.
2. **Eval이 inert** — `_score_candidate`/`_score_incumbent`가 `NotImplementedError`(07-01 진단 §3-2, 여전히 유효). SSL의 대표 실패 모드인 **confirmation bias**(다수 클래스 과잉확신 증폭, rare class 붕괴)는 GT-anchored per-class eval 없이는 **감지 자체가 불가능**. 지금 돌리면 좋아졌는지 나빠졌는지조차 모른다.
3. **Sealed test GT가 통계적으로 무의미** — GT ≈248 images / 1,558 boxes / **2 classes**(로드맵 §2.2 기준). per-class floor 판정이 박스 몇 개로 뒤집히는 수준(07-01 §4). fire/smoke rare class는 SSL이 가장 망가뜨리기 쉬운 지점인데 가장 측정 불가능한 지점이기도 함.
4. **GPU 예산** — teacher-student는 EMA teacher + strong-aug 이중 forward로 메모리 ~2×. 16GB 공유 GPU(서빙 drain 필수)에서 LoRA 기준으로도 빠듯하고, 정비 윈도우가 그만큼 길어짐.

> 즉 07-01 진단의 결론이 SSL에도 그대로 적용된다: **병목은 코드가 아니라 사람 GT.** SSL은 그 병목을 우회하는 마법이 아니라, 병목이 풀린 *다음에* 라벨 효율을 증폭하는 기법이다.

---

## 3. Train-time SSL 재검토 게이트 (전부 충족 시에만 안건 상정)

| # | 게이트 | 확인 방법 |
|---|---|---|
| G1 | 첫 supervised 학습 run 완료 + eval 게이트 실측 1회 통과 이력 | `model_registry`에 `metrics` 채워진 candidate ≥ 1행 |
| G2 | eval 스코어링 실구현 (NotImplementedError 해소) + sealed test split 동결 | `defs/train/eval.py` 실측 경로 CI 외 실행 확인 |
| G3 | Sealed test GT 최소량: **클래스당 ≥100 boxes** (per-class floor가 통계적으로 유의해지는 하한) | `image_label_annotations` 집계 |
| G4 | **정책 개정 승인** (CTO + ai-modeler): "pseudo-label은 train-only 신호로 허용, eval/GT 테이블(`image_label_annotations`, `review_status='finalized'`) 유입은 절대 불가, provenance 컬럼으로 학습셋 내 GT/pseudo 분리 기록" 조항 신설 | CLAUDE.md 불변식 개정 커밋 |

G1–G3는 기존 로드맵(P1 GT 플라이휠 + 첫 학습 run)이 그대로 달성 경로다. **SSL을 위해 새로 만들 것은 G4(정책 문서) 하나뿐.**

재상정 시 권장 형태(미리 좁혀둠): SAM3 LoRA 위 **Unbiased/Soft-Teacher류 1개만** 파일럿, unlabeled 풀은 188K 전체가 아닌 AL 큐 하위권(모델이 불확실한 프레임) 수만 장, eval은 동일 sealed split에서 supervised baseline 대비 per-class 비퇴행 확인. 실패 시 미련 없이 폐기(추가 GT 축적이 항상 대안).

---

## 4. (C) Self-supervised 도메인 사전학습 — 보류 사유

불변식 충돌은 없지만:

- PE-Core는 이미 대규모 사전학습된 강한 backbone — CCTV 도메인 적응의 한계 효용이 불확실.
- MAE/DINO급 재학습은 16GB에서 비현실적이고, LoRA-scale 적응은 가능하나 **효과 측정이 (D)와 동일하게 G1–G3에 막힘**.
- PE-Core 파인튠 트랙(재임베딩 + `@ft-*` 포인터 전환)이 이미 설계돼 있으므로, 그 첫 사이클에서 supervised 성적을 본 뒤에 판단해도 늦지 않음.

판정: **(D)와 같은 게이트에 종속, 우선순위는 (D)보다 낮음.** 별도 준비 작업 없음.

---

## 5. 지금 하는 것 — 불변식-호환 SSL (기존 로드맵에 +1)

### 5.1 P1-1 스펙 추가: LS pre-annotation (verify-don't-draw) ⭐

로드맵 P1-1(AL 큐 → LS task 배선)에 스펙 한 줄 추가:

> **LS task 생성 시 해당 이미지의 SAM3 COCO 박스를 LS `predictions` 필드로 첨부한다.**

- 검수자가 박스를 그리는 대신 **확인·수정**만 하면 됨 — 업계 통례상 어노테이션 처리량 3~10×. GT 병목(로드맵 §6-5 "검수 인력")을 도구 쪽에서 완화하는 유일한 지렛대.
- 데이터는 이미 있음: `vlm-labels/.../sam3_segmentations/` 원본 COCO(finalize 무손상 보존, 07-01 구현). 신규 생성 없이 첨부만.
- 불변식 안전: 사람이 수정·확정한 결과만 `finalized`/`image_label_annotations`로 유입 — 기존 webhook 경로 그대로.
- ⚠️ **앵커링 편향 리스크**: 검수자가 모델 박스에 끌려 누락 객체(FN)를 안 그릴 수 있음 → (i) 검수 가이드에 "빈 영역 스캔" 명시, (ii) 배치 일부(예: 10%)는 pre-annotation 없이 blind 검수로 섞어 FN율 대조. 이 대조가 곧 pseudo-label recall의 공짜 측정치가 됨.
- 공수: P1-1에 +1~2일 (LS predictions payload 구성 + 좌표계 변환 검증).

### 5.2 이미 계획된 것의 재확인 (SSL 관점 재해석)

| 항목 | SSL 관점 의미 |
|---|---|
| AL seed를 finalized GT 기반으로 수정 (07-01 §5-1) | (D) 재상정 시 unlabeled 풀 선별의 편향 제거 — **G4 이전에 반드시 선행** |
| GT KPI 탭 (P1-2) | G3 도달 시점을 팀이 같은 화면으로 봄 — SSL 게이트 진행률 = KPI 탭 그 자체 |
| 첫 스냅샷 → 첫 학습 run (로드맵 §6-4) | G1 달성 — SSL 논의의 전제 |

**추가 개발 항목은 5.1 하나. 나머지는 전부 기존 로드맵이 SSL 게이트를 겸한다.**

---

## 6. 의사결정 필요 사항

1. **P1-1에 pre-annotation 스펙 포함 여부** — 포함 권장(+1~2일). 미포함 시 검수 처리량이 그대로 병목으로 남아 G3 도달이 수개월 밀림.
2. **G4 정책 개정 논의 시점** — 지금 결정할 필요 없음. G1–G3 충족 시 CTO/ai-modeler 안건으로 자동 상정 (이 문서가 트리거 조건 명세).
3. **blind 검수 비율** — 5.1의 편향 대조용 배치 비율(제안: 10%). 라벨링 담당자 워크플로 협의 필요 (로드맵 §6-2와 같은 자리에서).

---

## 7. 기각 확정 (재론 방지)

| 항목 | 사유 |
|---|---|
| confidence threshold 기반 pseudo-label 자동 `finalized` 승격 | 불변식 정면 위반 (로드맵 §5 기확정 재확인) |
| 임베딩 kNN 라벨 전파 → GT 테이블 | 동일 — suspect score가 미보정 휴리스틱이듯 전파 confidence도 미보정 |
| Gemini 라벨을 "타 모델이므로 예외" 처리 | 불변식은 모델 파생 일반을 금지 — cross-model distillation도 동일 취급 |
| GT 없이 SSL 파일럿 "일단 돌려보기" | 측정 불가능한 실험 = 결과가 나와도 아무것도 배울 수 없음 (§2-2) |
