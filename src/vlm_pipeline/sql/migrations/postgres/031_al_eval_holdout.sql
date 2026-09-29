-- 031_al_eval_holdout.sql — AL 이 영구히 고를 수 없는 평가 홀드아웃을 봉인한다.
--
-- 왜 지금: `al_selections` 300 행이 전부 `ls_task_id IS NULL`(LS 미전송)이라 **지금만
-- unbiased eval 셋을 만들 여지가 있다.** 한 번 사람 라벨로 흘러가면 이후 사람 라벨은
-- selection-biased(AL 이 고른 것 위주)가 되어 unbiased eval 셋을 영구히 못 만든다
-- (설계: docs/design-docs/dcom-active-learning-feasibility-2026-09-08.md §3 A4).
--
-- 접근: 백필 스크립트·상태 테이블·드리프트 감시 없이, `GENERATED ALWAYS AS (...) STORED`
-- 생성 컬럼 하나로 규칙 자체를 스키마에 박는다. 새로 적재되는 al_frames 행도 INSERT 시점에
-- 자동으로 같은 규칙을 적용받는다 — "봉인 여부"가 별도 상태가 아니라 결정론적 함수이므로
-- 절대 drift 하지 않는다.
--
-- ⚠️ **group_key 단위로만 봉인한다. 프레임 단위 무작위 홀드아웃은 누수다.** 같은 영상/세션의
-- 인접 프레임이 train 과 test 양쪽에 들어가면 홀드아웃이 사실상 train 데이터를 재사용하는
-- 것과 같아진다 — 실측: sitej_certbody 는 연출 동시녹화라 **카메라** 홀드아웃조차 28% 누수했고,
-- group_key 를 **세션**으로 바꿔서야 해소됐다([[project-certbody-sitej-corpus]]). 그래서 해시 입력은
-- `frame_key`(프레임 고유값)가 아니라 `group_key`(홀드아웃 단위)를 쓴다.
--
-- ⚠️ **group_key 가 NULL 인 행 처리를 명시적으로 정의한다.** 2026-09-21 실측으로는
-- al_frames 81,433 행 전체가 group_key NOT NULL 이라 지금은 이 분기를 안 타지만, 향후 새
-- 코호트가 group_key 를 채우지 않고 적재하면 `cohort || '/' || group_key` 가 NULL 이 되어
-- md5 도 NULL 이 되고, 그 결과 `eval_holdout` 이 **조용히 NULL** 이 된다. `WHERE NOT
-- f.eval_holdout` 필터에서 NULL 은 false 취급되어(행이 안 걸러짐) 그 행들이 **영원히 봉인
-- 안 된 채 AL 풀에 남는 조용한 실패**가 된다 — 이 파이프라인이 반복해서 겪은 "부재에 기댄
-- 안전"([[project-safety-by-absence]]) 패턴 그대로다.
--   → 해결: `COALESCE(group_key, frame_key)` 로 그룹키가 없으면 **프레임 고유값으로 대체**해
--     해시 입력이 항상 NOT NULL 이도록 강제한다. `frame_key` 는 PK 구성요소라 NOT NULL 이
--     보장된다. 이는 의도적 트레이드오프다: group_key 를 채우지 않은 코호트는 (그 행에 한해)
--     세션/카메라 단위 누수-안전 보장이 **프레임 단위로 약화**된다. 그래도 결정 자체가
--     조용히 사라지는 것(NULL→미봉인)보다, 명시적으로 더 약한 보장으로 fail-open 하는 편이
--     낫다고 판단했다. **새 코호트 적재 스크립트는 group_key 를 반드시 채워야 한다** — 이
--     COALESCE 는 안전망이지 정책이 아니다.
--   → 2026-09-21 보강: 그 "정책"을 주석에서 제약으로 올린다. 아래 `ALTER COLUMN group_key
--     SET NOT NULL` 이 미래 적재기의 누락을 **INSERT 시점에 즉시 실패**시키므로, 위 COALESCE
--     분기는 이제 도달 불가능한 죽은 안전망이다(일부러 남긴다 — 제약이 어떤 경로로 풀려도
--     eval_holdout 이 NULL 로 새지는 않게). 리뷰 지적: 누수-안전 보장이 "코호트 적재기가
--     group_key 를 기억해서 채운다"에만 걸려 있으면, 정작 버그가 난 적재기의 행들이
--     조용히 프레임 단위로 약화된다 — 보호가 가장 필요한 행에서 보호가 사라지는 구조다.
--
-- 결정론적 해시 배정: `cohort || '/' || COALESCE(group_key, frame_key)` 의 md5 앞 32비트를
-- 부호 있는 int4 로 캐스트한 뒤(`'x' || substr(md5(...), 1, 8))::bit(32)::int` — 잘 알려진
-- PG idiom), `& 4294967295` 로 부호 확장을 걷어내 0..2^32-1 균등분포 bigint 를 얻고, 하위
-- ~20%(`< floor(2^32 * 0.20) = 858993459`)를 홀드아웃으로 정한다. md5/substr/bit 캐스트/
-- 비트연산 전부 IMMUTABLE 이라 생성 컬럼에 쓸 수 있다(계획서 지시사항). `cohort` 를 해시
-- 입력 접두어로 포함시켜 **코호트마다 독립적으로 20%를 뽑는다** — 코호트 A 의 group_key
-- 'cam01' 과 코호트 B 의 동일 문자열 'cam01' 이 같은 해시로 겹치지 않는다.
--
-- 실측 검증(2026-09-21, prod, 이 컬럼을 추가하지 않고 동일 표현식으로 SELECT 만 실행해
-- 확인 — 쓰기 없음): 13개 코호트 중 12개가 그룹 수 기준 대략 17~25% 사이로 봉인됐다.
-- ⚠️ `source-b_bbox_gt` 는 그룹이 1개뿐이라 **홀드아웃 0** (66 프레임 전부 weapon, 봉인 그룹
-- 0/1) — 그룹이 5개 미만인 코호트는 20% 규칙이 사실상 무의미하다는 뜻이고, 이번 범위(A4)는
-- 이 사실을 알리는 것까지다. 클래스 쏠림도 실측됨: `sourcea_bbox_gt` 의 no_helmet 295건은
-- 홀드아웃 0건(그 코호트의 봉인된 3개 그룹 중 no_helmet 을 가진 그룹이 없었다). 층화는 이번
-- 범위 밖이다 — al_select.py/채점 스크립트 소비자는 클래스별 홀드아웃 표본 수가 0일 수 있다는
-- 전제로 작성해야 한다.
--
-- ⚠️ **클래스별 봉인율 실측(2026-09-21) — 이 홀드아웃을 per-class eval 로 쓰지 마라.**
-- 큰 클래스는 20% 근처지만(weapon 21.2 · violence 19.4 · normal 20.7 · no_helmet 19.9 ·
-- smoke 19.5 · smoking 19.0), 나머지는 크게 벗어난다: **fire 14.1%(n=5,475)** ·
-- **falldown 17.3%(n=4,882)** · eating 34.6% · no_harness 11.1% · sittingdown 3.3%.
-- 원인은 해시 편향이 아니라 **그룹 크기 편차**다 — 그룹 단위 봉인은 한 그룹의 프레임을
-- 전부 함께 봉인하므로, 프레임 기준 비율은 "봉인된 그룹들이 얼마나 컸나"의 함수다.
-- 어떤 그룹 단위 홀드아웃을 써도 생기는 성질이고([[project-sourcei-gt-stat-power]] 의
-- deff 232 · ICC 0.51~0.83 과 같은 뿌리), 층화 표집으로만 해소된다.
-- → 이 컬럼의 계약은 **"AL 이 고를 수 없다"** 까지다. per-class 지표의 분모로 쓰려면
--   해당 클래스의 봉인 그룹 수를 먼저 세고, 부족하면 층화 재봉인이 선행돼야 한다.
--
-- 해시 자체는 검증됐다: 마스킹 항등식을 독립 유도(bit(64) 좌측 0패딩, 부호 비트 미개입)와
-- 200,011 입력(경계값 0x80000000·0xFFFFFFFF 포함)에서 대조해 불일치 0.
-- 실제 키 5,678개의 decile 균등성은 χ²=28.06(df=9, p≈0.001)로 약하게 벗어나나
-- (대조군 20회 최대 19.89), 해석 가능한 구조가 없고 봉인율 20%→21.4% 차이는 목적상 무의미하다.
--
-- 이 컬럼의 소비자(이번 파일 범위 밖, 각자 별도 작업):
--   - `al_select.py` 가 선별 풀에서 `AND NOT f.eval_holdout` 로 제외한다.
--   - 채점 스크립트가 고정 test split 으로 사용한다.
--
-- Forward-only, idempotent, DO 블록 미사용.
--
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'al_frames' AND column_name = 'eval_holdout')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'al_frames' AND column_name = 'eval_holdout' AND is_generated = 'ALWAYS')
-- @ASSERT_AFTER: SELECT NOT EXISTS (SELECT 1 FROM al_frames WHERE eval_holdout IS NULL)
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'al_frames' AND indexname = 'al_frames_eval_holdout_idx')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'al_frames' AND column_name = 'group_key' AND is_nullable = 'NO')

BEGIN;

-- NOT NULL 은 안전 조끼: frame_key 는 PK 구성요소라 이미 NOT NULL 이 구조적으로 보장되므로
-- COALESCE(group_key, frame_key) 가 NULL 이 될 수 없고, 따라서 이 생성식도 NULL 을 낼 수 없다.
-- 위 @ASSERT_AFTER 의 `eval_holdout IS NULL` 검사와 이중으로 이 불변식을 지킨다.
-- 누수-안전 단위(group_key)의 존재를 제약으로 강제한다. 2026-09-21 실측으로 81,433행 전체가
-- 이미 NOT NULL 이라 지금은 무영향이고, 아래 ADD COLUMN 이 어차피 테이블을 재작성하므로
-- 추가 비용도 없다. 이미 NOT NULL 인 컬럼에 대한 SET NOT NULL 은 no-op 이라 재실행 안전.
ALTER TABLE al_frames ALTER COLUMN group_key SET NOT NULL;

ALTER TABLE al_frames
  ADD COLUMN IF NOT EXISTS eval_holdout boolean NOT NULL
  GENERATED ALWAYS AS (
    (
      (
        (('x' || substr(md5(cohort || '/' || COALESCE(group_key, frame_key)), 1, 8))::bit(32)::int)::bigint
        & 4294967295
      ) < 858993459
    )
  ) STORED;

CREATE INDEX IF NOT EXISTS al_frames_eval_holdout_idx ON al_frames (cohort, eval_holdout);

COMMIT;
