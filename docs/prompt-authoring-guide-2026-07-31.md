> ⚠️ 이 파일은 사본이다. **원본은 `docker/data/fiftyone/sourceh_v2/report/prompt_authoring_guide.md`** 이며
> `sourceh_prompt_geometry.py guide` 스테이지가 실행될 때마다 재생성된다.

# 프롬프트 작성 가이드 (자동 생성, 기준 뱅크: v1.0.8.4)

- 생성: 2026-07-31 06:41 | 프레임 13,144장
- **채택 기준(권고)**: 유발 FP ≤ 0.10% 인 후보 중 FN 구조율 최대. 구조율이 비슷하면 선택도(구조율÷FP) 높은 쪽.
- **FN 구조율** = 지금 놓치고 있는 사진 중에서, 이 문장을 넣으면 새로 맞추게 되는 비율 (높을수록 좋음)
- **유발 FP** = 이 문장이 엉뚱한 다른 종류의 사진까지 가져가 버리는 비율 (낮을수록 좋음)

## 작성 전에 꼭 알아야 할 것 — 문장은 자석이다

모델은 사진을 보고 **가장 비슷한 문장 하나**를 찾아 그 문장의 클래스로 답한다. 즉 문장 하나하나가 사진을 끌어당기는 **자석**이다. 좋은 자석은 자기 클래스 사진만 당기고, 나쁜 자석은 아무 사진이나 다 당긴다(= 만능 자석). 참고로 v1.0.8.0→v1.0.8.4 는 문장을 추가한 게 아니라 **전부 갈아엎은 것**이다(두 버전에 공통 문장 0개). 그 전면 교체가 승패를 어떻게 바꿨는지 전부 추적해 보니, 네 가지 경우뿐이었다:

**① 좋은 자석이 새로 생겨서 맞췄다** (개선 1,557장 중 18장)
> 예전엔 자동차 헤드라이트 반사 사진에 어울리는 문장이 없어서 모델이 '불'이라고 답했다. 새 버전에 "카메라 렌즈에 빛이 반사된다"는 문장이 생기자 정답(normal)을 찾았다.
> → **교훈: 모델이 틀리는 진짜 이유(반사, 헤드라이트, 렌즈 얼룩)를 그대로 문장으로 쓰면, 그 사진들을 정확히 데려올 수 있다.**

**② 나쁜 자석이 없어져서 맞췄다** (개선 1,557장 중 1,534장에 관여 — 대부분!)
> 실측 최악의 만능 자석은 v1.0.8.0 의 [smoke] «Visible smoke in the upper-right corner around the warehouse in the evening.» — 이 문장 **하나만 지워도 169장**이 저절로 정답이 된다 (이 문장이 가져간 490장 중 선언클래스가 실제 정답인 비율은 0%).
> → **교훈: 좋은 문장을 새로 쓰는 것만큼, 아무 데나 붙는 나쁜 문장을 지우는 게 중요하다. 나쁜 자석이 되기 쉬운 문장: 특정 물건 언급(빨간 가방/통), 위치·시간 수식(오른쪽 위에/저녁에), 두루뭉술한 장면 묘사(a clear view of...).**

**③ 지운 자석이 사실 일도 하고 있었다** (손상 443장 중 430장 — smoke 가 대부분)
> 위의 만능 자석을 지웠더니, 그 자석이 잡아주던 **진짜 사진들**이 갈 곳을 잃고 틀리기 시작했다.
> → **교훈: 나쁜 문장을 지울 때는, 그 문장이 맞추던 진짜 사진들을 대신 데려올 좋은 문장을 반드시 같이 넣어라.**

**④ 동점 승부가 우연히 뒤집혔다** (5장)
> 두 문장의 점수가 거의 같아서(0.005 이내) 순위만 살짝 바뀐 것. 운이다.
> → **교훈: 이 사진들은 문장 설계의 근거로 쓰지 말 것. 오히려 정답 라벨이 맞는지 다시 볼 후보다.**

정리: 아래 표의 후보 문장들은 위 교훈에 따라 ①처럼 데려오는 힘(FN 구조율)이 크고 ②의 만능 자석이 아닌 것(유발 FP 낮음)만 골라 채택한다. 삭제 쪽 랭킹은 `/data/fiftyone/sourceh_v2/report/prune_<version>.csv` 를 보라.

## falldown — 미검출 0프레임, 이벤트절(자동): “Someone is lying on the floor.”

| 장면어 | 템플릿 | FN 구조율 | 유발 FP | 선택도 | 판정 |
|---|---|---|---|---|---|
| warehouse | scene+event | 0.0% | 0.00% | 0x | 낮음 |
| warehouse | scene+state+event | 0.0% | 0.02% | 0x | 낮음 |
| construction site | scene+event | 0.0% | 0.02% | 0x | 낮음 |
| construction site | scene+state+event | 0.0% | 0.03% | 0x | 낮음 |
| parking lot | scene+event | 0.0% | 0.00% | 0x | 낮음 |
| parking lot | scene+state+event | 0.0% | 0.00% | 0x | 낮음 |
| rooftop | scene+event | 0.0% | 0.00% | 0x | 낮음 |
| rooftop | scene+state+event | 0.0% | 0.00% | 0x | 낮음 |
| storage yard | scene+event | 0.0% | 0.05% | 0x | 낮음 |
| storage yard | scene+state+event | 0.0% | 0.06% | 0x | 낮음 |

## fire — 미검출 253프레임, 이벤트절(자동): “Flames are burning.”

| 장면어 | 템플릿 | FN 구조율 | 유발 FP | 선택도 | 판정 |
|---|---|---|---|---|---|
| warehouse | scene+event | 0.0% | 0.10% | 0x | 낮음 |
| warehouse | scene+state+event | 0.0% | 0.02% | 0x | 낮음 |
| factory floor | scene+event | 0.0% | 0.00% | 0x | 낮음 |
| factory floor | scene+state+event | 0.0% | 0.02% | 0x | 낮음 |
| loading dock | scene+state+event | 11.5% | 1.27% | 9x | ⚠️ FP |
| industrial yard | scene+state+event | 4.7% | 1.22% | 4x | ⚠️ FP |
| loading dock | scene+event | 3.6% | 3.22% | 1x | ⚠️ FP |
| industrial yard | scene+event | 0.4% | 1.92% | 0x | ⚠️ FP |
| construction site | scene+event | 0.0% | 0.32% | 0x | ⚠️ FP |
| construction site | scene+state+event | 0.0% | 0.75% | 0x | ⚠️ FP |

## smoke — 미검출 510프레임, 이벤트절(자동): “Smoke is rising.”

| 장면어 | 템플릿 | FN 구조율 | 유발 FP | 선택도 | 판정 |
|---|---|---|---|---|---|
| warehouse | scene+event | 0.0% | 0.05% | 0x | 낮음 |
| construction site | scene+event | 0.0% | 0.03% | 0x | 낮음 |
| parking lot | scene+event | 0.0% | 0.07% | 0x | 낮음 |
| parking lot | scene+state+event | 0.0% | 0.09% | 0x | 낮음 |
| rooftop | scene+event | 0.0% | 0.05% | 0x | 낮음 |
| rooftop | scene+state+event | 0.0% | 0.03% | 0x | 낮음 |
| factory floor | scene+event | 0.0% | 0.00% | 0x | 낮음 |
| factory floor | scene+state+event | 0.0% | 0.00% | 0x | 낮음 |
| gas station | scene+event | 0.0% | 0.04% | 0x | 낮음 |
| gas station | scene+state+event | 0.0% | 0.04% | 0x | 낮음 |
