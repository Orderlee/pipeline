# Staging QA Result Report — Cycle 5 (두 폴더 동시 투입, 트리거 1개)

- 테스트 일시: 2026-03-18 (KST)
- 테스트 목적: incoming에 tmp_data_2 + GS건설 동시 배치 → archive_pending 이동 후 tmp_data_2만 트리거 → tmp_data_2만 처리·archive 이동, GS건설은 archive_pending 유지

## 1. 실행 요약

- **스킵**: 수동 순서로 실행 가능
  1. `QA_DROP_SOURCE=_qa_smoke_tmp_data_2 bash staging_qa_safe_drop_incoming.sh tmp_data_2`
  2. archive_pending/tmp_data_2 대기(또는 45s)
  3. `QA_DROP_SOURCE=_qa_smoke_GS건설 bash staging_qa_safe_drop_incoming.sh GS건설`
  4. archive_pending/GS건설 대기
  5. `python3 scripts/staging_test_dispatch.py --folder tmp_data_2 --skip-copy --wait-archive-pending --outputs timestamp captioning --request-id qa_c5_dual`
- 최종 판정: **DEFERRED** (동일 절차로 추후 실행)

## 2. 기대 결과

- tmp_data_2만 raw_files/processed_clips 등 생성, archive로 이동
- gsgeonseol(GS건설) raw_files 0, archive_pending/GS건설 유지

## 3. 후속 조치

- `staging_qa_wait_expect.py --mode dual_ts_cap` 로 tmp_data_2 처리·gsgeonseol 0 확인
