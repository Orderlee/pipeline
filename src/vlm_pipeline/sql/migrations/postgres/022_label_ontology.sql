-- 022_label_ontology.sql — 라벨 정본을 분석 조인용 PostgreSQL 카탈로그로 투영한다.
--
-- 코드 경로의 유일한 정본(SoT)은 src/vlm_pipeline/data/label_ontology.json 이다.
-- 이 두 테이블은 image_label_annotations.category 와 prompt bank class_label 등을 canonical
-- 클래스에 조인하기 위한 read-side 파생 투영이며, 라벨 의미를 결정하는 authoritative 원장이 아니다.
--
-- 별칭 정책: JSON aliases 배열을 그대로 보존한다. 따라서 정본에 명시된 self-alias 8개도 넣되,
-- aliases 가 비어 있는 canonical 에 identity row 를 임의 생성하지 않는다. 특히 'smoking' 은
-- JSON 그대로 smoke 의 alias 이면서 별도 canonical 이기도 하다. 두 테이블의 PK namespace 가 달라
-- 스키마 충돌은 없지만 의미상 충돌은 미해결이므로, 소비자는 canonical 일치를 alias 보다 먼저
-- 적용해야 'smoking' 을 독립 canonical 로 해석할 수 있다.
--
-- Forward-only, idempotent, DO 블록 미사용.
--
-- @ASSERT_AFTER: SELECT to_regclass('label_classes') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('label_class_aliases') IS NOT NULL
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'label_classes_description_nonblank_check' AND conrelid = 'label_classes'::regclass AND contype = 'c')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'label_class_aliases_canonical_fkey' AND conrelid = 'label_class_aliases'::regclass AND confrelid = 'label_classes'::regclass AND contype = 'f' AND confupdtype = 'c')
-- @ASSERT_AFTER: SELECT COUNT(*) = 13 FROM label_classes WHERE canonical IN ('fire', 'smoke', 'smoking', 'falldown', 'weapon', 'violence', 'person', 'climbing up', 'patient', 'normal', 'class_5', 'class_6', 'class_7')
-- @ASSERT_AFTER: SELECT NOT EXISTS (SELECT 1 FROM label_classes WHERE description IS NULL OR btrim(description) = '')

BEGIN;

CREATE TABLE IF NOT EXISTS label_classes (
    canonical         TEXT PRIMARY KEY,
    description       TEXT NOT NULL,
    dispatch_category BOOLEAN NOT NULL DEFAULT FALSE,
    detect_phrases    TEXT[] NOT NULL DEFAULT '{}',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT label_classes_description_nonblank_check CHECK (btrim(description) <> '')
);

CREATE TABLE IF NOT EXISTS label_class_aliases (
    alias     TEXT PRIMARY KEY,
    canonical TEXT NOT NULL,
    CONSTRAINT label_class_aliases_canonical_fkey
        FOREIGN KEY (canonical) REFERENCES label_classes(canonical) ON UPDATE CASCADE
);

INSERT INTO label_classes (canonical, description, dispatch_category, detect_phrases)
VALUES
    (
        'fire',
        '화염·불꽃이 보이는 활성 화재.',
        TRUE,
        ARRAY['fire', 'flame', 'open flame']
    ),
    (
        'smoke',
        '연기·연무. 화염 없이 연기만 있는 경우를 포함한다.',
        TRUE,
        ARRAY['smoke', 'smoke cloud']
    ),
    (
        'smoking',
        '사람의 흡연 행위. Gemini 이벤트 전용 — SAM3 text prompt 대상이 아니다. ⚠️ 미해결 충돌: ls_tasks 에서는 ''smoking'' 이 smoke 의 동의어이지만 dispatch 에서는 독립 카테고리다.',
        TRUE,
        ARRAY['cigarette', 'smoking']
    ),
    (
        'falldown',
        '사람이 쓰러지거나 바닥에 누워 움직이지 않는 상태(낙상).',
        TRUE,
        ARRAY['fallen person', 'person lying down', 'person on the ground']
    ),
    (
        'weapon',
        '총기·도검·둔기 등 무기의 소지 또는 사용.',
        TRUE,
        ARRAY['gun', 'knife', 'baseball bat', 'sword', 'bat', 'dagger']
    ),
    (
        'violence',
        '사람 간 물리적 폭력·싸움·타격.',
        TRUE,
        ARRAY['fighting people', 'punching person', 'person hitting person']
    ),
    (
        'person',
        '사람 일반. 이벤트가 아니라 객체 클래스.',
        TRUE,
        ARRAY['person']
    ),
    (
        'climbing up',
        '구조물·울타리를 기어오르는 행위. LS normalizer 에만 존재하며 dispatch 카테고리로 쓰인 적 없다.',
        FALSE,
        ARRAY[]::TEXT[]
    ),
    (
        'patient',
        'VHC(요양·의료) 데이터의 환자. image_label_annotations 에 1,219 boxes 로 실재한다. ⚠️ 미해결: person 의 하위인지 별개 클래스인지 정해진 바 없다.',
        FALSE,
        ARRAY[]::TEXT[]
    ),
    (
        'normal',
        '이벤트 없음. 프롬프트 뱅크의 negative 클래스로만 쓰인다.',
        FALSE,
        ARRAY[]::TEXT[]
    ),
    (
        'class_5',
        'UNKNOWN — userwatch 공급 뱅크(v4/v5 계열)의 미상 클래스. 72 문장. 공급자 확인 필요.',
        FALSE,
        ARRAY[]::TEXT[]
    ),
    (
        'class_6',
        'UNKNOWN — userwatch 공급 뱅크의 미상 클래스. 91 문장. 공급자 확인 필요.',
        FALSE,
        ARRAY[]::TEXT[]
    ),
    (
        'class_7',
        'UNKNOWN — userwatch 공급 뱅크(v5.0.5.0+)의 미상 클래스. 80 문장. 공급자 확인 필요.',
        FALSE,
        ARRAY[]::TEXT[]
    )
ON CONFLICT (canonical) DO UPDATE
SET description = EXCLUDED.description,
    dispatch_category = EXCLUDED.dispatch_category,
    detect_phrases = EXCLUDED.detect_phrases;

INSERT INTO label_class_aliases (alias, canonical)
VALUES
    ('explosion', 'fire'),
    ('fire', 'fire'),
    ('flame', 'fire'),
    ('open flame', 'fire'),
    ('cigarette', 'smoke'),
    ('smoke', 'smoke'),
    ('smoke cloud', 'smoke'),
    -- JSON 의 알려진 의미 충돌을 숨기지 않고 그대로 투영한다. canonical-first 조회 필요.
    ('smoking', 'smoke'),
    ('deliberate_fall_from_wheelchair', 'falldown'),
    ('deliberate_lie_down', 'falldown'),
    ('deliberate_recovery', 'falldown'),
    ('fall', 'falldown'),
    ('fall_assistance', 'falldown'),
    ('fall_recovery', 'falldown'),
    ('fall_recovery_drill', 'falldown'),
    ('fall_risk', 'falldown'),
    ('fall_simulation', 'falldown'),
    ('falldown', 'falldown'),
    ('fallen person', 'falldown'),
    ('intentional_fall_simulation', 'falldown'),
    ('person lying down', 'falldown'),
    ('person on the ground', 'falldown'),
    ('person_lying_on_ground', 'falldown'),
    ('recovery_from_fall_simulation', 'falldown'),
    ('simulated_fall', 'falldown'),
    ('bat', 'weapon'),
    ('baseball bat', 'weapon'),
    ('dagger', 'weapon'),
    ('gun', 'weapon'),
    ('knife', 'weapon'),
    ('sword', 'weapon'),
    ('weapon', 'weapon'),
    ('fight', 'violence'),
    ('fighting people', 'violence'),
    ('person hitting person', 'violence'),
    ('punching person', 'violence'),
    ('violence', 'violence'),
    ('person', 'person'),
    ('climbing up', 'climbing up'),
    ('climbing_up', 'climbing up'),
    ('unsafe_climbing_activity', 'climbing up'),
    ('patient', 'patient')
ON CONFLICT (alias) DO UPDATE
SET canonical = EXCLUDED.canonical;

COMMIT;
