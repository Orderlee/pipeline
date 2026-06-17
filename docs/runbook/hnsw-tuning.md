# pgvector HNSW 튜닝 가이드 (image_embeddings)

> 임베딩 벡터 검색의 recall/latency 튜닝. 벤치: `scripts/bench_pgvector_hnsw.py` (prod 규모에서 재실행).

## 튜닝 노브
| 노브 | 위치 | 기본 | 효과 |
|---|---|---|---|
| `m` | CREATE INDEX 시 | 16 | 그래프 연결도 ↑ → recall ↑·인덱스 크기/빌드시간 ↑ |
| `ef_construction` | CREATE INDEX 시 | 64 | 빌드 품질 ↑ → recall ↑·빌드시간 ↑ |
| `hnsw.ef_search` | 쿼리 세션 | 40 | 탐색 후보 ↑ → recall ↑·latency ↑ |
| `hnsw.iterative_scan` | 쿼리 세션 | off | filtered 쿼리에서 k개 채울 때까지 반복 스캔 (relaxed_order/strict_order) |

## ⭐ 핵심: entity_type 별 partial 인덱스 (008 마이그레이션)
**단일 통합 HNSW 는 filtered 검색(`WHERE entity_type='frame'`)에서 0건을 반환할 수 있다.** 특히
**cross-modal**(텍스트 쿼리 → frame): 텍스트 벡터는 caption 임베딩과 가까워 HNSW top-N 후보가
전부 caption → frame 필터 후 빈 결과. (2026-06-16 staging 실측: 텍스트쿼리 top-40 = caption 40/frame 0)

→ **해결(008)**: `WHERE entity_type='frame'` / `='caption'` partial HNSW 인덱스를 따로 둬서 planner 가
해당 type 인덱스만 타게 함. 통합 인덱스 제거. 검증: 텍스트→frame 이 exact(seqscan)와 동일 결과 반환.
- (대안: `SET hnsw.iterative_scan=relaxed_order` 도 해결하나, partial 인덱스가 더 깔끔·빠름.)

## staging 벤치 결과 (N=2,410 frame)
```
ef_search=  40  recall@10=1.000  p50=0.68ms
ef_search= 100  recall@10=1.000  p50=1.02ms
ef_search= 200  recall@10=1.000  p50=6.44ms
```
- 소규모(수천)에선 **모든 ef_search 에서 recall=1.0** (인덱스가 사실상 전수 탐색) → 기본 `ef_search=40` 이 최적(완벽 recall + 최速). 튜닝 불필요.
- latency 는 수천 규모에선 sub-ms~few-ms, 노이즈 수준. **규모 효과는 prod 에서 측정해야 의미.**

## prod 규모 권장 (frame ~121K, → 수백만)
1. **먼저 벤치 재실행**: `DATAOPS_POSTGRES_DSN=<prod> python scripts/bench_pgvector_hnsw.py --entity frame --samples 200` (그리고 `--entity caption`).
2. recall@10 ≥ 0.95 목표:
   - 기본(m=16/ef_construction=64) + `ef_search=40`부터. recall 부족하면 **ef_search 를 100→200** 로.
   - 그래도 부족 + 수백만 규모면 인덱스 재생성 시 **m=32, ef_construction=128~200** (빌드 비용 ↑, recall ↑).
3. **인덱스 빌드 타이밍**: 대량 backfill **이후** partial 인덱스 생성/REINDEX 가 빠름(점진 insert 보다). 008 은 IF NOT EXISTS 라 빈 테이블에 만들어도 무방하나, 121K 적재 후 `REINDEX INDEX CONCURRENTLY image_embeddings_hnsw_frame` 로 품질 확보 고려.
4. **maintenance_work_mem**: 인덱스 빌드 세션에서만 상향(전역 X) — 그래프가 메모리에 들어가면 빌드 빠름. 서버 메모리 고갈 주의.
5. **iterative_scan**: partial 인덱스로 충분하나, 다중 필터(예: entity_type + 추가 조건) 조합 시 `SET hnsw.iterative_scan=relaxed_order` 안전망.

## 검색 패턴 (참고)
- 이미지 유사: frame 쿼리벡터 → `WHERE entity_type='frame' ORDER BY embedding <=> $1` (frame partial index).
- cross-modal 텍스트→이미지: `embed_text(q)` → 위와 동일 (008 로 정상 동작).
- 캡션 유사/검색: `WHERE entity_type='caption' ...` (caption partial index).
