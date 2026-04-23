<!--
PR 작성 가이드: docs/git-workflow-guide.md "빠른 참조 (체크카드)" 섹션 참고
-->

## Target 브랜치

- [ ] **`dev` 대상** — 스테이징(3031)에서 검증 예정. 검증 완료 후 별도로 `dev → main` PR 필요
- [ ] **`main` 대상** — 다음 중 하나:
  - [ ] dev에서 이미 검증 완료된 커밋 머지
  - [ ] 핫픽스(main 기반) — 머지 후 **`main → dev` 백머지 필수** (다음 릴리즈 회귀 방지)

## 변경 내용

<!-- 무엇을·왜 변경했는지 1-3문장. "어떻게"는 diff에서 보이므로 생략. -->

## 배포 영향

- [ ] `src/vlm_pipeline/**` 변경 — rsync만, 이미지 재빌드 없음 (빠름)
- [ ] `Dockerfile` / `docker/app/**` / `configs/**` / `scripts/**` / `gcp/**` / `split_dataset/**` / `src/python/**` 변경 — **이미지 재빌드** (느림)
- [ ] `*.md` (루트) / `docs/**` / `tests/**` 만 변경 — `paths-ignore`로 CI 미트리거 (정상)
- [ ] env 변경 필요 — `.env`/`.env.test`는 git 미추적. 호스트에서 직접 편집 + 해당 환경 Dagster 재기동

## 검증

- [ ] 로컬 또는 스테이징에서 기능 확인 (해당 시 증빙/스크린샷)
- [ ] 관련 유닛 테스트 통과 (`pytest tests/unit -q`)
- [ ] DuckDB 스키마/쿼리 변경이 있으면 `scripts/query_local_duckdb.py`로 샘플 확인
- [ ] 새 sensor/asset/schedule이면 tick 로그 또는 run materialization 확인

## 배포 후 체크 (머지 후 본인이 직접)

- [ ] [Actions run](https://github.com/Orderlee/Datapipeline-Data-data_pipeline/actions) success
- [ ] 해당 환경 Dagster UI 200 OK
  - PROD: http://172.168.42.6:3030/
  - STAGING: http://172.168.42.6:3031/
- [ ] code location **LOADED** (PythonError 없음)
- [ ] 변경 범위의 asset/sensor/schedule 정상 동작
- [ ] (핫픽스) `main → dev` 백머지 완료
- [ ] (main 머지 후 프로덕션 안정 확인) upstream 기여: `./tools/pr-to-upstream.sh`

## 관련 이슈 / 참고

<!-- Linear / GitHub Issue 링크, 참고 문서, 이전 PR 등 -->
