-- 026_label_ontology_promotions.sql — 라벨 정본 확장분 1차 승격 (intrusion, no_harness)
--
-- ⚠️ RECONSTRUCTED FILE (2026-09-21). 이 파일은 prod DB `_pg_migrations` 에
-- `applied_at=2026-09-14 07:50:50` 로 기록돼 있으나, 어느 브랜치에도 커밋된 적이 없어
-- git history 에서 원본을 찾지 못했다(staging clone / docker/data/al_scripts / /tmp /
-- git stash·reflog(양쪽 repo) / analysis 컨테이너 전부 확인, 원본 없음).
--
-- 아래 내용은 **추측이 아니라 prod 실측값을 그대로 옮긴 것**이다 — 2026-09-21
-- `docker exec docker-postgres-1 psql ... label_classes / label_class_aliases` 로
-- canonical/description/dispatch_category/detect_phrases/alias 전체를 조회해 022 시드와
-- 차집합을 구했다(신규 2 canonical + 5 alias, 022 의 42 alias 와 합쳐 47 = 현재 행 수와 일치).
-- 두 신규 행의 `created_at` 이 초 단위까지 동일(`2026-09-14 07:50:50.333942+00`)해 원본이
-- **단일 다중-VALUES INSERT 1건**이었음을 뒷받침한다 — 그래서 "2문장, DO 블록 없음" 기억과
-- 맞춰 label_classes 1문장 + label_class_aliases 1문장으로 재구성했다.
-- **불확실한 부분(추측)**: 정확한 주석 문구·공백·ON CONFLICT 표현 등 SQL 텍스트 자체는
-- 복원 불가 — 022 의 스타일을 그대로 따랐다. 데이터(canonical/description/dispatch_category/
-- detect_phrases/alias 매핑)는 prod 실측이라 신뢰도 높음.
--
-- ⚠️ **SoT 드리프트 경고**: `src/vlm_pipeline/data/label_ontology.json` (코드 경로 정본)에는
-- 2026-09-21 현재 `intrusion`/`no_harness` 가 없다(13 classes 그대로). 즉 이 DB 투영이 JSON
-- 정본보다 앞서 있다 — 026 을 다시 적용해도 JSON 쪽 drift 는 해소되지 않는다. JSON 갱신은
-- 이 작업의 범위 밖이다(라벨 온톨로지 정본 수정은 dataops-engineer/해당 소유자 영역).
--
-- 승격 배경(기존 기록): 아카이브 PoC export 773 이벤트 중 314건(41%)이 정본 밖이라, 고객
-- 폴더명 `unauthorized_intrusion`/`safety_harness` 를 canonical 로 승격하고 alias 로 접었다.
-- 둘 다 `dispatch_category=false`(SAM3 text prompt 대상 아님 — 사건/부재이지 객체가 아님)라
-- `detect_phrases` 는 빈 배열. `normal` 에는 identity alias 를 추가했다(022 에서 누락).
--
-- Forward-only, idempotent, DO 블록 미사용.
--
-- @ASSERT_AFTER: SELECT COUNT(*) = 2 FROM label_classes WHERE canonical IN ('intrusion', 'no_harness')
-- @ASSERT_AFTER: SELECT COUNT(*) = 5 FROM label_class_aliases WHERE alias IN ('intrusion', 'unauthorized_intrusion', 'no_harness', 'safety_harness', 'normal')

BEGIN;

INSERT INTO label_classes (canonical, description, dispatch_category, detect_phrases)
VALUES
    (
        'intrusion',
        '허가 없이 통제구역·울타리 안으로 들어온 무단 침입. Gemini 이벤트 카테고리이며 SAM3 text prompt 대상이 아니다 — 침입은 객체가 아니라 사건이고, 물리적 발현은 climbing up·person 이 잡는다.',
        FALSE,
        ARRAY[]::TEXT[]
    ),
    (
        'no_harness',
        '고소작업 중 안전대(안전벨트) 미착용. Gemini 이벤트 카테고리이며 SAM3 text prompt 대상이 아니다 — 부재는 명사구로 검출되지 않는다. ⚠️ alias ''safety_harness'' 는 고객 원본 폴더명이고 ''착용''이 아니라 ''미착용 위반''을 뜻한다.',
        FALSE,
        ARRAY[]::TEXT[]
    )
ON CONFLICT (canonical) DO UPDATE
SET description = EXCLUDED.description,
    dispatch_category = EXCLUDED.dispatch_category,
    detect_phrases = EXCLUDED.detect_phrases;

INSERT INTO label_class_aliases (alias, canonical)
VALUES
    ('intrusion', 'intrusion'),
    ('unauthorized_intrusion', 'intrusion'),
    ('no_harness', 'no_harness'),
    ('safety_harness', 'no_harness'),
    ('normal', 'normal')
ON CONFLICT (alias) DO UPDATE
SET canonical = EXCLUDED.canonical;

COMMIT;
