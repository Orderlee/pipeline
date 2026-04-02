# Linear MCP 서브이슈 생성 템플릿

작업 내용을 기반으로 Linear 서브이슈를 자동 생성할 때 쓰는 템플릿입니다.

## 1) 사용 전 체크

- Linear MCP 연결 상태 확인 (`linear` 서버가 Running)
- 워크스페이스 Team key 확인 (예: `ENG`, `DATA`)
- 부모 이슈 식별자 준비 (`ABC-123` 또는 issue UUID)

---

## 2) 생성 전 미리보기 템플릿 (권장)

아래를 그대로 복사해서, placeholder만 채워서 사용하세요.

```text
당신은 Linear 이슈 설계자입니다.
다음 작업 설명을 분석해 부모 이슈 아래에 넣을 서브이슈 초안을 먼저 제안하세요.
아직 실제 생성은 하지 마세요.

[입력]
- Team: <TEAM_KEY>
- Parent issue: <PARENT_ISSUE_ID_OR_KEY>
- 작업 설명:
<WORK_DESCRIPTION>
- 목표 완료일(옵션): <YYYY-MM-DD or none>
- 기본 라벨(옵션): <label1,label2,...>
- 원하는 서브이슈 개수: <N>

[요청]
1. 부모 이슈를 조회해 컨텍스트를 반영하세요.
2. 서브이슈를 4~8개 범위로 제안하세요.
3. 각 서브이슈에 아래를 포함하세요.
   - title
   - why (배경/목적)
   - scope (포함)
   - out_of_scope (제외)
   - acceptance_criteria (체크리스트 2~5개)
   - dependency (선행 이슈 key 또는 없음)
   - recommended_priority (urgent/high/medium/low)
4. 결과를 실행 순서 기준으로 표로 정리하세요.
5. 마지막에 "생성 진행 여부"를 질문하세요.
```

---

## 3) 즉시 생성 템플릿

아래는 승인 후 실제 생성까지 요청하는 버전입니다.

```text
당신은 Linear 이슈 운영자입니다.
다음 작업 설명을 기준으로 부모 이슈 아래 서브이슈를 생성하세요.

[입력]
- Team: <TEAM_KEY>
- Parent issue: <PARENT_ISSUE_ID_OR_KEY>
- 작업 설명:
<WORK_DESCRIPTION>
- 목표 완료일(옵션): <YYYY-MM-DD or none>
- 기본 라벨(옵션): <label1,label2,...>
- 원하는 서브이슈 개수: <N>

[실행 규칙]
1. 부모 이슈 조회 후 컨텍스트를 요약하세요.
2. 작업을 중복 없이 독립적인 서브이슈로 분해하세요.
3. 각 서브이슈 description은 아래 섹션을 포함하세요.
   - Context
   - Scope
   - Out of scope
   - Acceptance Criteria
   - Dependency
4. 서브이슈를 실제 생성하고 parent-child 관계를 연결하세요.
5. dependency가 있으면 이슈 간 선후행을 반영하세요.
6. 결과를 표로 반환하세요.
   - issue key
   - title
   - url
   - dependency
   - suggested order

[중요]
- 모호하면 생성 전에 1회만 확인 질문하세요.
- 중복 이슈는 만들지 마세요.
- 각 이슈는 검증 가능한 acceptance criteria를 포함하세요.
```

---

## 4) 빠른 예시

```text
Team: DATA
Parent issue: DATA-421
작업 설명: staging dispatch 센서의 실패 로그를 구조화하고, 재시도/알림/대시보드까지 연결
기본 라벨: staging,dispatch,observability
원하는 서브이슈 개수: 6
```

