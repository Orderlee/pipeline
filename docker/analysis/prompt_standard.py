#!/usr/bin/env python3
"""프롬프트 생성 표준 — 보고서가 측정한 규칙을 **코드 한 곳**에 고정한다.

왜 이 모듈이 있나: 같은 규칙이 (a) 내가 손으로 쓴 Gemini 지시문, (b) FiftyOne
`@user/prompt-probe` 의 문장 생성 오퍼레이터, (c) 뱅크 빌더의 사전 컷 — 세 곳에 흩어져 있었다.
흩어지면 드리프트한다([[project_label_ontology]] 의 클래스 매핑 5중 복사와 같은 실패다).
**여기가 정본이고 나머지는 import 한다.**

담고 있는 것 (전부 sourcei GT 7,498 프레임 / 31뱅크 121,614 문장에서 측정된 값):
  · 클래스별 승리 템플릿 형태와 그 선택도            (보고서 §10)
  · 클래스별 금칙 어휘 — 특히 normal 의 자세 어휘     (§10: "엎드린 사람" normal 문장이 falldown 52프레임 강탈)
  · 장소 어휘 억제 규칙                              (§10 NMI 장소 0.586 vs 이벤트 0.149)
  · 생성 후 라벨-free 컷 비율 (사전 고정)             (§15 특이도 하위 25% = 무위험, §17 재현)
  · 검증 규칙 — 금칙어·길이·형태 쿼터·근접중복

쓰는 법
    from prompt_standard import sourcei, build_generation_prompt, validate, curate
    inst = build_generation_prompt(sourcei, "falldown", 90)      # → Gemini 지시문
    kept, rejected = validate(sentences, "falldown", sourcei)    # → 규칙 위반 걸러내기
    sel = curate(kept, vecs, stats)                              # → 라벨-free 컷 적용

CLI
    python3 prompt_standard.py rules                       # 규칙 덤프(사람이 읽는 용)
    python3 prompt_standard.py generate --env sourcei --cls falldown --n 90 > out.json
    python3 prompt_standard.py validate --cls normal --file cand.json
    python3 prompt_standard.py probe --file cand.json      # /embed_text → 벡터 + 라벨-free 지표
"""
from __future__ import annotations
import os, sys, re, json, argparse, subprocess, collections
from dataclasses import dataclass, field, asdict

CLASSES = ["normal", "falldown", "fire", "smoke"]
EVENTS = ["falldown", "fire", "smoke"]

# ══════════════════════════════════════════════════════════════════
# 1) 측정된 규칙 — 숫자는 전부 보고서 출처를 달아 둔다
# ══════════════════════════════════════════════════════════════════
FORMS = {                                   # 형태 이름 → 예시 접두
    "person_led": "A person …",
    "phenomenon_led": "A fire … / Smoke …",
    "scene_led": "It is a <place>. …",
    "camera_led": "cctv feed of … / surveillance view of …",
}

# §10 템플릿 선택도(hit/(hit+trap)) — 이 현장에서 실측. None = 그 형태의 표본이 없었음
SELECTIVITY = {
    "normal":   {"camera_led": 0.826, "scene_led": 0.578, "person_led": 0.511},
    "falldown": {"person_led": 0.998, "scene_led": 0.531},
    "fire":     {"phenomenon_led": 0.927},
    "smoke":    {"person_led": 0.977, "scene_led": 0.953, "phenomenon_led": 0.938, "camera_led": 0.925},
}
WINNING_FORM = {c: max(v, key=v.get) for c, v in SELECTIVITY.items()}
FORM_QUOTA = 0.70                            # 승리 형태가 차지해야 할 최소 비율

# §10 — normal 에 들어가면 이벤트 클래스를 강탈하는 어휘. 부정형도 금지("no smoke" 도 자석이다)
BANNED = {
    "normal": r"\b(lying|lie|lies|lay|laid|fallen|fall|falls|falling|collapse\w*|slump\w*|sprawl\w*|"
              r"unconscious|motionless|fire|flame\w*|burn\w*|smoke|smok\w*|haze|emergency|injur\w*|blaze)\b",
    "falldown": r"\bescalator\w*\b",          # 이 현장 최대 오탐원 — falldown 문장에서 배제
    "fire": r"\b(smoke|smok\w*|haze|smell)\b",
    "smoke": r"\b(flame\w*|fire|burning|blaze)\b",
}

# 클래스별로 반드시 담아야 하는 내용 (지시문에 그대로 들어간다)
MUST_DESCRIBE = {
    "normal": "일상 활동과 시각 조건. 사건이 아님을 **긍정문으로만** 서술",
    "falldown": "비자발적으로 쓰러진 몸 — 움직임 없음, 엎드림, 누움, 사지가 벌어짐, 방치됨. "
                "자발적인 쪼그림·앉음·숙임과 반드시 구별",
    "fire": "보이는 화염 — 열린 불꽃, 번지는 불, 타는 물체, 표면의 불빛. 크기를 다양하게(1/3은 작은 불꽃)",
    "smoke": "화염 없는 연기·연무 — 뒤가 가려지는 불투명한 기둥. 색과 농도를 다양하게",
}

LEN_MIN, LEN_MAX = 6, 20                    # 단어 수
DUP_COS = 0.95                              # 근접중복 임계 (§15 와 동일)

# 생성 후 사전 고정 컷 (§15·§17) — 클래스 **내** 분위수임에 주의
CUTS = dict(ms_top_pct=25, sd_bottom_pct=25, dedup_cos=DUP_COS)


@dataclass
class EnvProfile:
    """현장 프로필 — 새 현장을 추가할 때 이것만 쓰면 된다."""
    name: str
    description: str                         # 카메라·조명·해상도 등 물리 조건
    places: list                             # 장소 어휘 후보 (문장당 최대 1개, 골고루)
    confusers: list                          # 이 현장에서 실제로 오경보를 낸 것들 (normal 문장이 덮어야 함)
    known_traps: list = field(default_factory=list)   # 실측된 강탈 문장 (모방 금지 예시)
    classes: list = field(default_factory=lambda: list(CLASSES))


sourcei = EnvProfile(
    name="sourcei",
    description=(
        "A Korean department store complex. Fixed overhead CCTV, wide angle, moderate-to-low "
        "resolution, mixed lighting including dim basement and bright daylight atrium. 15 cameras."
    ),
    places=["retail sales floor", "shop aisle between clothing racks", "escalator landing",
            "back corridor", "concrete stairwell", "back-of-house storage room",
            "underground parking level", "food court seating area", "server room",
            "loading dock", "dim basement passage", "bright atrium walkway",
            "cosmetics counter area", "checkout area", "service elevator lobby"],
    confusers=["people riding escalators", "cleaning staff sweeping or mopping",
               "workers bending or crouching to pick items off the floor",
               "staff kneeling to stock a low shelf", "people sitting on steps or the floor deliberately",
               "people carrying or stacking boxes", "shoppers browsing and reaching for shelves",
               "employees pushing carts", "an empty corridor", "an empty stairwell",
               "parked cars and a car pulling in", "steam and vapor rising from food stalls",
               "bright sunlight glare and reflections on polished floors",
               "orange or red signage and warm lighting", "mist from a cleaning machine",
               "dust in a beam of light"],
    known_traps=["The man in the sweatshirt walked up an escalator",
                 "A man descends the escalator of a shopping mall while cleaning it",
                 "a worker bending to collect items from the floor",
                 "In a CCTV footage at a parking lot, a cleaner is sweeping the floor.",
                 "A man is lying forward on an escalator instead of standing upright."],
)

ENVS = {"sourcei": sourcei}


# ══════════════════════════════════════════════════════════════════
# 2) 생성 지시문
# ══════════════════════════════════════════════════════════════════
def build_generation_prompt(env: EnvProfile, cls: str, n: int) -> str:
    """Gemini/OpenAI 호환 백엔드에 그대로 넣는 지시문. 규칙 근거 수치를 본문에 실어
    모델이 '왜'를 알고 쓰게 한다 — 근거 없는 금지는 모델이 자주 어긴다(실측)."""
    assert cls in CLASSES, cls
    win = WINNING_FORM[cls]
    sel = SELECTIVITY[cls]
    sel_line = " ; ".join(f"{FORMS[f]} = {v:.3f}" for f, v in sorted(sel.items(), key=lambda kv: -kv[1]))
    L = [
        f"Return ONLY a JSON array of {n} English sentences. No prose, no markdown fence, no explanation.",
        "",
        f"Task: candidate CLIP text prompts for the class \"{cls}\" in a zero-shot CCTV classifier "
        "using PE-Core-L14-336 (shared image/text space, cosine similarity, max-pooled per class).",
        f"Deployment: {env.description}",
        f"Places in this site: {', '.join(env.places)}.",
        "",
        "MEASURED CONSTRAINTS (empirical, from human-labeled frames at this exact site — follow them):",
        f"1. Template form decides selectivity. For this class: {sel_line}.",
        f"   → At least {int(FORM_QUOTA*100)}% of the sentences must use the winning form: {FORMS[win]}",
        "2. Frame clusters carry PLACE information 4x more strongly than EVENT information "
        "(NMI 0.586 place vs 0.149 event). A place-heavy sentence becomes a place detector.",
        "   → At most one short place phrase per sentence, and vary the place across sentences.",
        f"3. Content requirement: {MUST_DESCRIBE[cls]}",
    ]
    if cls == "normal":
        L += ["4. CRITICAL: no posture, collapse, lying, falling, fire, smoke, flame or emergency "
              "vocabulary at all — not even negated. At this site the sentence "
              "\"A man is lying forward on an escalator instead of standing upright.\" is labeled "
              "normal and it steals 52 frames from the falldown class.",
              "5. Cover this site's actual confusers: " + "; ".join(env.confusers) + "."]
    else:
        banned_desc = {"falldown": "the word \"escalator\"", "fire": "smoke, haze or smell",
                       "smoke": "flame, fire or burning"}[cls]
        L += [f"4. Do NOT mention {banned_desc} anywhere — a separate class covers that."]
        if env.known_traps:
            L += ["5. These sentences already steal frames at this site. Do NOT imitate them:"] + \
                 [f"   - {t}" for t in env.known_traps[:5]]
    L += [
        f"6. Each sentence {LEN_MIN}-{LEN_MAX} words, one clause or two short clauses, plain present "
        "tense, no numbers, no proper nouns, no Korean, no camera IDs, no timestamps.",
        "7. No two sentences may differ only by a place word — vary the event description itself.",
        "",
        f"Output exactly {n} entries: [\"sentence\", \"sentence\", ...]",
    ]
    return "\n".join(L)


# ══════════════════════════════════════════════════════════════════
# 3) 검증 — 규칙 위반을 생성 직후에 거른다
# ══════════════════════════════════════════════════════════════════
def _form_of(s: str) -> str:
    """문장의 템플릿 형태. **주어 명사구의 head 로 판정**한다.

    ⚠️ 2026-08-28 수정: 예전 판정은 첫 단어만 봐서 `A small flame flickers …` 를 'other' 로
       떨어뜨렸다(형용사가 끼면 전부 놓친다). 형태 쿼터 검사와 §23 규칙 준수 채점이 이 함수를
       쓰므로, 놓치면 **생성이 규칙을 지켰는데도 미달로 보고**된다.
    """
    l = re.sub(r"^[\"'\s]+", "", (s or "").lower())
    if re.match(r"^(a\s+|an\s+|the\s+)?(cctv|surveillance|overhead camera|security camera|"
                r"camera feed|security feed|surveillance camera)", l):
        return "camera_led"
    if re.match(r"^(it is|this is|the scene|inside|in a|in the|at a|at the|on a|on the|"
                r"within a|within the)\b", l):
        return "scene_led"
    PERSON = r"(person|man|woman|male|female|customer|shopper|individual|employee|staff|worker|" \
             r"figure|body|someone|somebody|adult|elderly|child|shopper|cleaner|guard|visitor)"
    PHENOM = r"(fire|flame|flames|smoke|haze|plume|blaze|smog)"
    # 주어부 = 첫 동사 앞까지. 관사·형용사를 건너뛰고 head 명사를 찾는다.
    head = l.split(",")[0]
    m = re.match(r"^((?:a|an|the)\s+)?((?:[a-z-]+\s+){0,4}?)(" + PERSON + r"|" + PHENOM + r")\b", head)
    if m:
        w = m.group(3)
        return "person_led" if re.fullmatch(PERSON, w) else "phenomenon_led"
    if l.startswith(("someone", "somebody", "nobody")):
        return "person_led"
    return "other"


def validate(sentences, cls: str, env: EnvProfile = None):
    """반환 (kept, rejected). rejected 는 (문장, 사유) — 사유를 남겨야 프롬프트를 고칠 수 있다."""
    pat = re.compile(BANNED[cls], re.I) if cls in BANNED else None
    kept, rejected, seen = [], [], set()
    for s in sentences:
        s = (s or "").strip()
        if not s: continue
        if s in seen: rejected.append((s, "중복(문자 동일)")); continue
        seen.add(s)
        nw = len(s.split())
        if not (LEN_MIN <= nw <= LEN_MAX): rejected.append((s, f"길이 {nw} 단어 (허용 {LEN_MIN}~{LEN_MAX})")); continue
        if pat and pat.search(s): rejected.append((s, f"금칙어: {pat.search(s).group(0)}")); continue
        if re.search(r"\d", s): rejected.append((s, "숫자 포함")); continue
        if env:
            # ⚠️ 2026-08-28 수정: 예전엔 장소 후보의 **마지막 단어**를 그대로 세서, 후보 목록에
            #    같은 꼬리가 여러 번 나오면(`area` 3회 · `room` 2회) 문장에 한 번 나온 "area" 가
            #    3건으로 세어졌다. 그 결과 정상 문장이 "장소구 과다(3)" 로 떨어졌다 —
            #    `sourcei-prompts` 전량 판정에서 이 오탐만 32,654건이었다.
            #    고유 꼬리 단어를 단어경계로 세면 문장에 실제로 몇 개 들어있는지가 나온다.
            heads = {p.split()[-1].lower() for p in env.places}
            low = s.lower()
            hits = [h for h in heads if re.search(rf"\b{re.escape(h)}\b", low)]
            if len(hits) > 2: rejected.append((s, f"장소구 과다({len(hits)}: {', '.join(sorted(hits))})")); continue
        kept.append(s)
    forms = collections.Counter(_form_of(s) for s in kept)
    win = WINNING_FORM[cls]
    share = forms[win] / max(len(kept), 1)
    report = dict(n_in=len(sentences), n_kept=len(kept), n_rejected=len(rejected),
                  form_mix={k: v for k, v in forms.items()}, winning_form=win,
                  winning_share=round(share, 3), quota_ok=bool(share >= FORM_QUOTA))
    return kept, rejected, report


# ══════════════════════════════════════════════════════════════════
# 4) 프로브 — /embed_text 로 같은 공간에 올린다
# ══════════════════════════════════════════════════════════════════
EMBED_URL = os.environ.get("EMBED_URL", "http://embedding-service:8003/embed_text")

def embed_texts(texts, url: str = None, timeout: int = 300):
    """⚠️ `/embed_text` 는 form 필드 `text` 로 **한 문장씩** 받는다(웜 ~7.5ms).
    저장된 `entity_type='prompt'` 벡터와 cos=1.00000000 로 동일 인코더임이 실측됐다."""
    import urllib.parse, urllib.request
    import numpy as np
    url = url or EMBED_URL
    out = []
    for t in texts:
        body = urllib.parse.urlencode({"text": t}).encode()
        r = json.loads(urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=timeout).read())
        v = np.asarray(r["vector"], dtype="float32")
        out.append(v / np.linalg.norm(v))
    return np.stack(out) if out else None


def curate(texts, vecs, ms, sd, cuts: dict = None):
    """라벨-free 컷 — 클래스 **내** 분위수. 반환 유지 인덱스.
    ms=배경 평균 코사인(낮을수록 조용), sd=군집 간 특이도(높을수록 잘 가름)."""
    import numpy as np
    c = dict(CUTS); c.update(cuts or {})
    idx = np.arange(len(texts))
    keep = idx[(ms < np.percentile(ms, 100 - c["ms_top_pct"])) & (sd > np.percentile(sd, c["sd_bottom_pct"]))]
    if c.get("dedup_cos"):
        order = keep[np.argsort(ms[keep])]                 # 조용한 문장 우선 보존
        V = vecs[order]; sel, kept = [], []
        for j in range(len(order)):
            if kept and float(np.max(V[j] @ V[kept].T)) > c["dedup_cos"]: continue
            kept.append(j); sel.append(order[j])
        keep = np.array(sel)
    return keep


# ══════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════
def _gemini(instruction: str, model: str = "gemini-3.1-pro-preview") -> str:
    """호스트 `gemini` CLI 경유. 컨테이너 안에서는 백엔드가 없으니 파일로 주고받는다."""
    p = subprocess.run(["gemini", "-m", model, "-p", instruction], capture_output=True, text=True, timeout=900)
    if p.returncode != 0: raise RuntimeError(p.stderr[-500:])
    return p.stdout


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("rules")
    g = sub.add_parser("generate"); g.add_argument("--env", default="sourcei"); g.add_argument("--cls", required=True)
    g.add_argument("--n", type=int, default=90); g.add_argument("--print-only", action="store_true")
    g.add_argument("--model", default="gemini-3.1-pro-preview")
    v = sub.add_parser("validate"); v.add_argument("--cls", required=True); v.add_argument("--file", required=True)
    v.add_argument("--env", default="sourcei")
    p = sub.add_parser("probe"); p.add_argument("--file", required=True); p.add_argument("--out", default="probe.npz")
    a = ap.parse_args()

    if a.cmd == "rules":
        print(json.dumps(dict(winning_form=WINNING_FORM, selectivity=SELECTIVITY, banned=BANNED,
                              must_describe=MUST_DESCRIBE, form_quota=FORM_QUOTA,
                              length=[LEN_MIN, LEN_MAX], cuts=CUTS,
                              envs={k: asdict(v) for k, v in ENVS.items()}), ensure_ascii=False, indent=1))
    elif a.cmd == "generate":
        inst = build_generation_prompt(ENVS[a.env], a.cls, a.n)
        if a.print_only: print(inst); return
        raw = _gemini(inst, a.model)
        m = re.search(r"\[.*\]", raw, re.S)
        arr = json.loads(m.group(0)) if m else []
        kept, rej, rep = validate(arr, a.cls, ENVS[a.env])
        print(json.dumps(dict(cls=a.cls, env=a.env, report=rep, sentences=kept,
                              rejected=[{"text": t, "why": w} for t, w in rej]), ensure_ascii=False, indent=1))
    elif a.cmd == "validate":
        data = json.load(open(a.file))
        arr = data if isinstance(data, list) else data.get("sentences", [])
        kept, rej, rep = validate(arr, a.cls, ENVS[a.env])
        print(json.dumps(dict(report=rep, rejected=[{"text": t, "why": w} for t, w in rej]),
                         ensure_ascii=False, indent=1))
    elif a.cmd == "probe":
        import numpy as np
        data = json.load(open(a.file))
        arr = data if isinstance(data, list) else data.get("sentences", [])
        V = embed_texts(arr)
        np.savez_compressed(a.out, vecs=V, text=np.array(arr))
        print(json.dumps(dict(n=len(arr), dim=int(V.shape[1]), out=a.out), ensure_ascii=False))


def selftest():
    """규칙 판정의 조용한 오답 2종을 못질한다. `python3 prompt_standard.py selftest`."""
    # (1) 형태 판정 — 관사·형용사가 끼어도 head 명사로 잡아야 한다
    cases = [("A person lies motionless on the floor", "person_led"),
             ("A small flame flickers near a shelf", "phenomenon_led"),
             ("cctv feed of an empty corridor", "camera_led"),
             ("It is a dim basement passage with no one present", "scene_led"),
             ("Smoke drifts across the ceiling", "phenomenon_led"),
             ("An elderly shopper reaches for a low shelf", "person_led")]
    for t, want in cases:
        got = _form_of(t)
        assert got == want, f"_form_of({t!r}) = {got!r}, want {want!r}"
    # (2) 장소구 과다 — 꼬리 단어가 목록에 중복돼도 한 번 쓴 문장은 통과해야 한다
    one = "cctv feed of a person walking through the checkout area"
    kept, rej, _ = validate([one], "normal", sourcei)
    assert kept == [one], f"장소 1개 문장이 거부됨: {rej}"
    three = "cctv feed of the checkout area near the storage room on the parking level"
    kept, rej, _ = validate([three], "normal", sourcei)
    assert not kept and "장소구 과다" in rej[0][1], f"장소 3개가 통과됨: {kept}"
    # (3) 금칙어는 여전히 잡혀야 한다 (완화 회귀 방지)
    kept, rej, _ = validate(["A person is lying on the sales floor"], "normal", sourcei)
    assert not kept and "금칙어" in rej[0][1], rej
    print(f"selftest OK — 형태 {len(cases)}건 · 장소구 2건 · 금칙어 1건")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "selftest":
        selftest()
    else:
        main()
