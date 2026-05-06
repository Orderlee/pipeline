# PostgreSQL Migrations

이 디렉토리는 PostgreSQL 백엔드의 **forward-only 마이그레이션** 시퀀스를 보관한다. 부팅 시 `PostgresResource.ensure_schema()`가 미적용 마이그레이션을 순서대로 실행해 스키마를 수렴시킨다.

> 관련 문서:
> - 마이그레이션 plan 본문: `/home/user/.claude/plans/db-splendid-crayon.md`
> - 토폴로지 결정: [../../../../docs/references/db_migration_topology.md](../../../../docs/references/db_migration_topology.md)
> - Codex 권한 확장: [../../../../.agent/skill/codex_db_migration/SKILL.md](../../../../.agent/skill/codex_db_migration/SKILL.md)
> - Greenfield 스냅샷(읽기용): [../schema_postgres.sql](../schema_postgres.sql)

---

## 파일 명명 규칙

```
NNN_<short_kebab_name>.sql
```

- `NNN` — 3자리 0-padded 시퀀스 번호. 충돌 시 신규 PR이 더 높은 번호를 가져간다.
- `<short_kebab_name>` — 마이그레이션의 의도를 한 줄로 요약 (영문 소문자, 언더스코어 허용).

예시:
```
001_init.sql                    -- 부트스트랩 (모든 테이블 + 인덱스)
002_seed_staging_model_configs.sql  -- 시드 데이터
003_add_image_caption_score.sql -- 컬럼 추가
004_jsonb_to_text_rollback.sql  -- 사고 시 롤백 패턴 (만들 일이 없기를)
```

---

## 운영 원칙 (forward-only, idempotent)

### 1. **Forward-only**
- `DROP TABLE`, `TRUNCATE`, `ALTER COLUMN ... TYPE` 등 데이터 손실 가능 작업 **금지**.
- 컬럼 제거가 필요하면 **먼저 deprecation 플래그**를 추가하고 다음 릴리스에서 사용처를 정리한 뒤, 그 다음 마이그레이션에서 (정말로 필요하면) 제거한다.

### 2. **Idempotent (멱등)**
- 모든 `CREATE TABLE`은 `IF NOT EXISTS`.
- 모든 `ALTER TABLE`은 `IF EXISTS ... ADD COLUMN IF NOT EXISTS`.
- 모든 `CREATE INDEX`는 `IF NOT EXISTS`.
- 같은 마이그레이션을 여러 번 실행해도 같은 end-state여야 한다.

### 3. **트랜잭션 단위**
- 각 파일은 `BEGIN; ... COMMIT;`으로 감싼다. 실패 시 깔끔하게 롤백되어야 한다.
- `CREATE INDEX CONCURRENTLY` 등 트랜잭션 안에서 못 쓰는 DDL이 필요하면, 그 한 문장만 별도 파일로 분리해 트랜잭션 없이 실행한다.

### 4. **데이터 마이그레이션 분리**
- 스키마(`CREATE/ALTER`)와 데이터(`INSERT/UPDATE`)를 가능한 한 분리.
- 시드 데이터는 별도 `NNN_seed_*.sql`로.

### 5. **Codex 1차 작성, Sonnet 적용**
- `.agent/skill/codex_db_migration/SKILL.md`에 따라, 모든 신규 마이그레이션 파일은 Codex가 1차 작성한다(extra_high effort, T1 패턴).
- Sonnet/메인 에이전트는 받은 SQL을 적용·검증·테스트한다. 임의로 SQL을 다시 쓰지 않는다(이의는 Codex follow-up으로).

### 6. **검증 의무**
모든 신규 마이그레이션은 머지 전 다음을 통과해야 한다:

```bash
# (a) 빈 PG에 적용
docker run --rm -d --name pg-validate -e POSTGRES_PASSWORD=test -p 25432:5432 postgres:15
sleep 3
docker cp src/vlm_pipeline/sql/migrations/postgres/<NNN_*.sql> pg-validate:/tmp/
docker exec pg-validate psql -U postgres -f /tmp/<NNN_*.sql> -v ON_ERROR_STOP=on

# (b) 두 번째 적용 — 멱등성 검증 (같은 결과여야 함, 에러 0)
docker exec pg-validate psql -U postgres -f /tmp/<NNN_*.sql> -v ON_ERROR_STOP=on

# (c) 정리
docker rm -f pg-validate
```

---

## 적용 순서 (boot-time 동작)

1. `PostgresResource` 부팅 시 `ensure_schema()` 호출.
2. 내부 메타 테이블 `_pg_migrations(name TEXT PRIMARY KEY, applied_at TIMESTAMP)`을 검사하고 없으면 생성.
3. `migrations/postgres/` 디렉토리의 `*.sql` 파일을 정렬해 순회.
4. 이미 `_pg_migrations`에 기록된 파일은 skip.
5. 미적용 파일을 트랜잭션으로 실행, 성공 시 `_pg_migrations`에 INSERT.

> 구현은 Phase 1의 `postgres_migration.py`에서 작성된다. 이 README는 운영자/리뷰어가 마이그레이션을 새로 추가할 때 따라야 하는 규칙만 기술한다.

---

## 운영자가 새 마이그레이션을 추가할 때 (체크리스트)

- [ ] 다음 시퀀스 번호 할당 (`ls migrations/postgres/ | sort | tail -n 1`로 확인)
- [ ] `docs/references/multi-agent.md` §3.3에 따라 effort = `extra_high` 확정
- [ ] Codex에 T1 프롬프트로 SQL 1차 작성 위임 (`.agent/skill/codex_db_migration/SKILL.md` §2 참조)
- [ ] 받은 SQL을 그대로 파일에 적용(임의 수정 금지, 의문은 codex-reply follow-up)
- [ ] 위 §6 "검증 의무"의 (a)/(b)/(c) 모두 통과 확인
- [ ] PR 본문에 parity 표 + 멱등성 검증 결과 첨부
- [ ] `dev` 머지 전: Opus 1회 review
- [ ] `main` 승격 전: Opus 1회 review (cutover/롤백 영향 검토)

---

## 알려진 마이그레이션

| 번호 | 파일 | 목적 | 작성 |
|---|---|---|---|
| 001 | [001_init.sql](001_init.sql) | 15개 테이블 부트스트랩 + 인덱스 + ALTER guards | Codex |

향후 추가 예정(plan 본문 참조):
- 002: `staging_model_configs` 시드 (output_type별 기본 모델 매핑)
- 003+: 운영 중 발견되는 컬럼/인덱스 증분
