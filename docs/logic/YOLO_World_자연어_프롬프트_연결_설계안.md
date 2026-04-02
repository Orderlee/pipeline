# YOLO-World 자연어 프롬프트 연결 설계안

## 목적

- 현재 YOLO-World 경로에 자연어 기반 탐지 요청을 연결하기 위한 v1 설계 기준을 문서화한다.
- 본 문서는 추후 구현을 위한 설계안이며, 현재 단계에서는 코드 변경, 스키마 변경, API 변경을 포함하지 않는다.
- 기존 dispatch 중심 YOLO 파이프라인을 최대한 유지하면서, 자연어 입력을 YOLO-World가 소비 가능한 `classes` 목록으로 정규화하는 구조를 정의한다.

## 현행 구조 요약

- 현재 YOLO 실행 경로는 `dispatch JSON -> dispatch_sensor/service -> run tags -> defs/yolo/assets.py -> lib/yolo_world.py -> docker/yolo/app.py` 순으로 흐른다.
- dispatch에서 YOLO로 전달되는 입력은 다음 두 경로뿐이다.
  - 명시 `classes`
  - `categories -> classes` 파생
- Gemini용 `prompts`는 `timestamp`, `captioning` 단계에서만 사용되며 YOLO 단계에서는 사용하지 않는다.
- 현재 YOLO 서버는 자유문장 프롬프트를 직접 받지 않는다.
  - 클라이언트는 `classes_json`을 전송한다.
  - 서버는 이를 파싱한 뒤 `model.set_classes([...])`로 YOLO-World에 적용한다.
- 즉, 현재 파이프라인은 YOLO-World의 open-vocabulary 기능을 "텍스트 클래스/phrase 목록" 수준으로만 활용하고 있다.

## 문제 정의

- YOLO-World는 open-vocabulary 모델이지만, 현재 파이프라인은 자연어 문장을 직접 입력받아 탐지 대상으로 해석하는 구조가 아니다.
- 현재 구조에는 아래 중간 계층이 없다.
  - 사용자 자연어 요청
  - 자연어를 YOLO-friendly phrase/class로 정규화하는 LLM 해석
  - 최종 `classes`로 변환 후 기존 YOLO 경로에 주입
- 그 결과 사용자는 `"창고 입구에서 연기가 나는 장면"`, `"쓰러진 사람과 주변 구조물"` 같은 요청을 직접 넣을 수 없고, `classes` 또는 `categories`를 사전에 사람이 정제해서 넣어야 한다.

## 목표 동작

- v1 범위는 `dispatch` 경로에 한정한다.
- 사용자는 dispatch JSON에 YOLO용 자연어 입력을 별도 필드로 보낼 수 있다.
- 시스템은 해당 자연어를 1회 해석하여 짧은 영어 detection phrase 목록으로 정규화한다.
- 정규화 결과는 기존 YOLO 경로의 `classes`처럼 취급한다.
- YOLO HTTP API는 변경하지 않는다.
- YOLO stage는 자연어 원문이 아니라 최종 정규화된 `classes_json`만 받는다.

## 제안 아키텍처

### 1. 입력 분리

- Gemini용 입력과 YOLO용 입력을 분리한다.
  - `prompts`: Gemini 전용
  - `yolo_prompts`: YOLO 자연어 전용

### 2. 해석 시점

- 자연어 해석은 `dispatch ingress` 단계에서 수행한다.
- 구체적으로는 `defs/dispatch/service.py`의 request 준비 및 run 생성 직전 단계에서 수행한다.
- 한 번 해석된 결과를 request DB와 run tags에 저장하고, 이후 YOLO 단계는 재해석하지 않는다.

### 3. 해석 엔진

- 신규 외부 서비스는 두지 않는다.
- 기존 `lib/gemini.py`의 Gemini/Vertex 래퍼를 재사용해 text-only helper를 추가하는 방향으로 설계한다.
- YOLO 자연어 해석 전용 prompt는 `lib/gemini_prompts.py`에 별도 빌더로 둔다.

### 4. YOLO 연동 방식

- 최종 해석 결과는 `classes`와 동일한 shape의 문자열 배열로 만든다.
- 기존 `defs/yolo/assets.py`의 `_resolve_yolo_target_classes()`가 우선적으로 tags의 `classes`를 읽으므로, v1에서는 이 경로를 그대로 활용한다.
- 즉, 자연어 해석은 기존 YOLO 자산 로직을 대체하지 않고, 기존 입력을 더 잘 만들어 주는 전처리 계층으로 동작한다.

## 입력/출력 인터페이스 변경안

### 1. Dispatch JSON 입력

- 신규 필드
  - `yolo_prompts: list[str]`

예시:

```json
{
  "request_id": "req-001",
  "folder_name": "sample-folder",
  "labeling_method": ["bbox"],
  "yolo_prompts": [
    "창고 출입문 근처에서 연기가 발생한 장면",
    "바닥에 쓰러져 있는 사람"
  ]
}
```

### 2. 기존 필드 유지

- `prompts`
  - Gemini `timestamp`, `captioning` 전용으로 유지한다.
- `classes`
  - 명시 YOLO 클래스 또는 phrase 목록으로 유지한다.
- `categories`
  - 기존 파생 fallback 입력으로 유지한다.

### 3. 계획된 내부 저장 필드

- `staging_dispatch_requests.yolo_prompts`
- `staging_dispatch_requests.yolo_class_source`

### 4. 계획된 run tags

- `yolo_prompts`
- `yolo_class_source`

### 5. YOLO API 계약

- v1에서는 YOLO HTTP API를 변경하지 않는다.
- 최종 YOLO 서버 입력은 기존과 동일하게 `classes_json`만 사용한다.

## 처리 흐름

```text
dispatch JSON 수신
-> parse_dispatch_request_payload()
-> bbox 요청 여부 확인
-> yolo_prompts 존재 시 Gemini text resolver 실행
-> resolved classes 생성
-> fallback 정책 적용
-> staging_dispatch_requests 저장
-> run tags 생성
-> defs/yolo/assets.py 에서 기존처럼 classes 우선 해석
-> lib/yolo_world.py 가 classes_json 전송
-> docker/yolo/app.py 가 set_classes([...]) 적용 후 detection 수행
```

세부 순서는 아래와 같이 고정한다.

1. dispatch payload 정규화
2. `bbox` 요청 여부 확인
3. `yolo_prompts` 존재 시 text resolver 실행
4. resolver 결과가 유효하면 `classes`로 채택
5. 실패 또는 빈 결과면 fallback 적용
6. 최종 classes와 source를 request DB 및 run tags에 기록
7. 이후 YOLO stage는 기존 로직 그대로 수행

## 자연어 -> YOLO classes 변환 규칙

### 1. 출력 계약

- strict JSON만 허용한다.

```json
{
  "classes": ["smoke", "person lying on floor"]
}
```

### 2. 출력 규칙

- 영어 소문자만 사용
- 중복 제거
- 최대 12개 항목
- 각 항목은 1~5단어
- 시각적으로 탐지 가능한 대상만 포함
- 정책, 의도, 상황 설명, 행위 지시문은 제거

### 3. 변환 원칙

- 가능한 한 YOLO-World에 유리한 짧은 phrase로 만든다.
- 장황한 자연어 문장은 아래처럼 축약한다.
  - `"창고 출입문 근처에서 연기가 발생한 장면"` -> `["smoke", "warehouse door"]`
  - `"바닥에 쓰러져 있는 사람"` -> `["person lying on floor"]`
  - `"칼을 들고 위협하는 사람"` -> `["person with knife", "knife"]`
- 비가시적 개념은 제외한다.
  - `"수상한 상황"` -> 제외 또는 더 구체적 대상이 있을 때만 변환
  - `"위험해 보이는 분위기"` -> 제외

### 4. 예시

#### 예시 1. 한국어 자연어만 있는 경우

입력:

```json
{
  "labeling_method": ["bbox"],
  "yolo_prompts": [
    "창고 출입문 근처에서 연기가 퍼지는 장면",
    "바닥에 사람이 쓰러져 있음"
  ]
}
```

예상 해석:

```json
{
  "classes": ["smoke", "warehouse door", "person lying on floor"]
}
```

#### 예시 2. 자연어 + 명시 classes fallback

입력:

```json
{
  "labeling_method": ["bbox"],
  "yolo_prompts": ["위협 장면"],
  "classes": ["knife", "gun"]
}
```

해석 실패 시 최종 classes:

```json
{
  "classes": ["knife", "gun"]
}
```

#### 예시 3. 실패 케이스

입력:

```json
{
  "labeling_method": ["bbox"],
  "yolo_prompts": ["이상한 분위기"],
  "classes": [],
  "categories": []
}
```

처리 결과:

- 자연어 해석 결과 비어 있음
- fallback source 없음
- request reject

## 실패 처리 및 fallback 정책

### 1. 우선순위

아래 순서를 고정 정책으로 사용한다.

1. `yolo_prompts`
2. explicit `classes`
3. `categories -> classes`
4. 모두 없고 `bbox`가 요청된 경우 reject

### 2. 실패 조건

- Gemini text resolver 호출 실패
- JSON 파싱 실패
- `classes` 필드 누락
- 결과 배열이 비어 있음
- 결과 항목이 규칙을 만족하지 않음

### 3. 동작 규칙

- `yolo_prompts` 해석 성공
  - 최종 classes 사용
  - `yolo_class_source = "yolo_prompts_resolved"`
- `yolo_prompts` 해석 실패 + explicit `classes` 존재
  - explicit classes 사용
  - `yolo_class_source = "dispatch_classes_fallback"`
- `yolo_prompts` 해석 실패 + explicit `classes` 없음 + `categories` 존재
  - derived classes 사용
  - `yolo_class_source = "dispatch_categories_fallback"`
- 셋 다 없으면
  - request reject
  - 에러 코드 예: `yolo_prompt_resolution_failed_no_fallback`

## DB/태그/로그 반영안

### 1. staging_dispatch_requests 반영 예정 항목

- `yolo_prompts`
  - 원본 자연어 입력 저장
- `yolo_class_source`
  - 최종 classes가 어떤 경로로 결정되었는지 저장

### 2. run tags 반영 예정 항목

- `classes`
  - 최종 YOLO classes
- `yolo_prompts`
  - 원본 자연어 입력
- `yolo_class_source`
  - 결정 경로

### 3. 로그 반영안

- dispatch ingress 로그
  - `request_id`
  - `bbox requested`
  - `yolo_prompts count`
  - `resolved classes`
  - `yolo_class_source`
- YOLO asset 로그
  - 기존 `class_source` 로그에 `yolo_class_source`를 추가로 노출
  - 실제 YOLO 서버에는 최종 `classes_json`만 전송되었음을 명시

### 4. source 값 고정안

- `yolo_prompts_resolved`
- `dispatch_classes_fallback`
- `dispatch_categories_fallback`

## 비범위

- `labeling_specs` 기반 spec flow 확장
- YOLO HTTP API 변경
- YOLO 서버가 자연어 문장을 직접 받는 구조
- 자유문장 프롬프트를 YOLO runtime마다 실시간 재해석하는 구조
- 별도 LLM 마이크로서비스 도입
- migration SQL 작성 및 실제 스키마 반영
- README 또는 운영 문서 전반 수정

## 테스트 시나리오

### 1. yolo_prompts only

- `bbox` 요청
- `yolo_prompts`만 존재
- resolver가 유효 classes를 생성
- 최종 YOLO는 `classes_json`만 수신

### 2. yolo_prompts + classes

- `yolo_prompts` 해석 실패
- explicit `classes` 존재
- explicit classes로 정상 fallback

### 3. yolo_prompts + categories

- `yolo_prompts` 해석 실패
- explicit `classes` 없음
- `categories -> classes`로 fallback

### 4. invalid or empty natural-language resolution

- resolver 응답이 빈 배열 또는 잘못된 JSON
- fallback source 없으면 request reject

### 5. bbox 미요청

- `labeling_method`에 `bbox` 없음
- `yolo_prompts`가 있어도 무시
- Gemini `prompts` 동작에는 영향 없음

### 6. YOLO stage receives final classes_json only

- YOLO asset과 HTTP client는 자연어 원문을 사용하지 않음
- 최종 정규화된 `classes_json`만 전달됨을 검증

## 단계별 적용 계획

### 1단계. 문서 기준 인터페이스/스키마 반영

- dispatch JSON에 `yolo_prompts` 필드 추가
- `staging_dispatch_requests`에 추적 필드 추가
- run tags 설계 반영

### 2단계. Gemini text resolver + dispatch wiring

- `lib/gemini.py`에 text-only helper 추가
- `lib/gemini_prompts.py`에 YOLO resolver prompt 추가
- dispatch ingress에서 `yolo_prompts -> classes` 해석 및 fallback 적용

### 3단계. 운영 검증 및 prompt quality tuning

- 실제 요청 예시 기반 phrase 품질 조정
- 과도하게 추상적인 결과 제거 규칙 강화
- 자주 쓰는 도메인 phrase의 품질 보정

## 참고 메모

- 본 설계안은 현재 저장소의 실제 경로와 로직을 기준으로 작성했다.
- 구현 시에는 기존 `prompts`의 의미를 바꾸지 않고, YOLO 자연어 입력을 별도 필드로 분리하는 것을 전제로 한다.
- 별도 migration SQL은 이 문서 단계에서 작성하지 않으며, 추후 구현 단계에서 필요한 컬럼 추가만 정리한다.
