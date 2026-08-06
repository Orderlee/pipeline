# 파이프라인 Postgres — pgduckdb 베이스 + pgvector 버전 고정.
#
# 왜 필요한가:
#   퍼블릭 베이스 이미지(pgduckdb/pgduckdb:15-v1.1.1)는 pgdg apt 에서 pgvector 를
#   "빌드 시점 최신"으로 받아 박제한다. 그래서 prod 는 pgvector 0.8.2 에 묶여 있고,
#   레포에는 그 사실을 기록한 빌드 정의가 없었다. staging 이 쓰던 로컬 이미지
#   datapipeline-pg-pgvector:15-v1.1.1 도 apt install 을 버전 없이 호출해 만든 것이라
#   (a) 언제 다시 빌드하느냐에 따라 다른 pgvector 가 들어가고
#   (b) git 에 없어서 `docker image prune` 한 번에 사라진다.
#   이 파일은 그 둘을 동시에 해결한다 — 추적되는 빌드 정의 + 결정적 버전.
#
# pgvector 버전 선택 근거 (upstream CHANGELOG):
#   0.8.3 — "Fixed possible index corruption with HNSW vacuuming"
#   0.8.4 — "Fixed 'hnsw graph not repaired' error" + vacuum 중 insert 오류
#   0.8.5 — 현재 stable (2026-07-08)
#   prod 의 0.8.2 는 위 두 vacuum 버그에 노출된다. 다만 image_embeddings 는 사실상
#   append-only 라(선택 쿼리가 NOT EXISTS 로 기존 행을 제외) dead tuple 이 거의 안 생겨
#   실제 트리거 확률은 낮다. 긴급 교체가 아니라 "첫 대량 벡터 삭제 전"에 올리면 된다.
#
# ⚠️ 이 이미지로 교체하려면 Postgres 재기동 = 전체 DB 다운타임이다.
#    라벨링/학습 창을 피해 계획된 정비 시간에만 수행할 것.
#
# 빌드:
#   docker build -t datapipeline-postgres:15-pgvector0.8.5 docker/postgres
# 적용 (정비 창에서):
#   docker/.env 의 POSTGRES_IMAGE 를 위 태그로 바꾸고
#   ./scripts/compose-prod.sh up -d --force-recreate postgres
#   이후 psql 에서: ALTER EXTENSION vector UPDATE;
#   검증: SELECT extversion FROM pg_extension WHERE extname='vector';
#
# POSTGRES_PRELOAD_LIBS=pg_duckdb 는 베이스 이미지 기능이라 그대로 유지된다.

FROM pgduckdb/pgduckdb:15-v1.1.1

# 정확한 apt 버전 문자열로 고정한다. 버전을 빼면 재현성이 깨진다.
# 사용 가능한 값 확인: apt-cache madison postgresql-15-pgvector
ARG PGVECTOR_APT_VERSION=0.8.5-1.pgdg12+1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        "postgresql-15-pgvector=${PGVECTOR_APT_VERSION}" \
    && rm -rf /var/lib/apt/lists/*
USER postgres
