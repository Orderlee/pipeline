#!/usr/bin/env python3
"""`intrustion` 클래스 문장 생성 — site-h역(지하철) 현장, 규칙은 prompt_standard 정본을 그대로 쓴다.

왜 별도 스크립트인가: `prompt_standard.py` 가 담고 있는 숫자는 **sourcei GT 7,498 프레임에서
측정된 것**뿐이다(모듈 첫 문단의 계약). intrustion 은 이 현장에 GT 가 없어 측정값이 없으므로
가짜 측정치를 정본에 박지 않고, **유추한 값임을 명시**해 여기서 주입한다.

유추 근거 (측정 아님):
  · 승리 형태 person_led — falldown 0.998 / smoke 0.977 처럼 **사람이 주어인 이벤트**는
    전부 person_led 가 이겼다. 장면 선행(`It is a switchgear room …`)은 falldown 에서 0.531 로
    떨어졌고, 침입은 그 실패 양식이 더 위험하다: 이 현장의 제한구역 카메라는 프레임 전체가
    항상 그 장소라 **장소 문장은 카메라 검출기가 된다**(§10 NMI 장소 0.586 vs 이벤트 0.149).
  · 금칙 — fire/smoke 어휘(별도 클래스) + 쓰러짐 어휘(falldown 강탈 방지).

실행(호스트 — gemini CLI 가 컨테이너에 없다):
    EMBED_URL=http://localhost:8004/embed_text \
    /home/user/anaconda3/bin/python gen_intrusion_bank.py --out gen_intrusion.json --n 500
"""
import os, sys, json, re, time, argparse, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np
import prompt_standard as ps
from gen_full_bank import gemini            # 같은 CLI 호출을 복제하지 않는다

CLS = "intrustion"                          # ⚠️ 원본 GT 폴더 표기(오타)를 그대로 따른다 — 채점 조인 키
HERE = os.path.dirname(os.path.abspath(__file__))

# ── 정본에 없는 클래스 규칙 주입 (측정값 아님 — 위 docstring 의 유추 근거) ──────────
ps.CLASSES.append(CLS)
ps.SELECTIVITY[CLS] = {"person_led": 0.998, "scene_led": 0.531}      # falldown 실측을 유추 적용
ps.WINNING_FORM[CLS] = "person_led"
ps.BANNED[CLS] = (r"\b(fire|flame\w*|burn\w*|smoke|smok\w*|haze|blaze|"
                  r"lying|lie|lies|lay|laid|fallen|fall|falls|falling|collapse\w*|"
                  r"slump\w*|sprawl\w*|unconscious|motionless|injur\w*)\b")
# MUST_DESCRIBE / 다양성 축은 **현장 프로필 JSON** 에서 온다 — 영상을 보고 쓴 것이 정본이고
# 코드에 박으면 현장마다 갈라진다 (prompt_standard 의 현장-JSON 규칙과 같은 이유).



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="gen_intrusion.json")
    ap.add_argument("--env", default=None, help="현장 프로필 JSON (기본 env_site-h.json)")
    ap.add_argument("--batches", type=int, default=6)
    ap.add_argument("--per-batch", type=int, default=140)
    ap.add_argument("--n", type=int, default=500, help="최종 목표 문장 수")
    ap.add_argument("--dedup", type=float, default=0.95, help="근접중복 코사인 상한")
    a = ap.parse_args()
    a.env = a.env or os.path.join(HERE, "env_site-h.json")
    t0 = time.time()
    def log(m): print(f"[{time.time()-t0:6.0f}s] {m}", flush=True)

    raw = json.load(open(a.env))
    env = ps.load_env(a.env)
    axes = raw["_axes"]; ps.MUST_DESCRIBE[CLS] = raw["_must_describe_en"]
    if raw.get("_banned_extra"):                       # 프로필이 클래스 금칙어를 덧붙일 수 있다
        ps.BANNED[CLS] += "|" + raw["_banned_extra"]
    places = env.places
    log(f"현장 {env.name} · 장소 {len(places)} · 축 {len(axes)} · 출처 {raw.get('_source','?')}")
    kept_all, rej_all, seen = [], [], set()
    for b in range(a.batches):
        axis = axes[b % len(axes)]
        sub = (places[(b * 5) % len(places):] + places)[:5]
        base = ps.build_generation_prompt(env, CLS, a.per_batch).replace(
            "MEASURED CONSTRAINTS (empirical, from human-labeled frames at this exact site",
            "CONSTRAINTS (measured at a comparable site for person-subject event classes; the "
            "selectivity number for this class is inferred from those, not measured here",
        )
        inst = base + "\n".join([
            "", "THIS BATCH ONLY:",
            f"- Vary primarily along this axis: {axis}.",
            f"- Use only these places (still at most one place phrase per sentence): {', '.join(sub)}.",
            "- Do not repeat sentence skeletons; each sentence should differ in its main verb or the "
            "state it describes, not only in the place word.",
        ])
        try:
            raw = gemini(inst)
        except Exception as e:                      # 배치 실패는 건너뛴다 (per-batch fail-forward)
            log(f"  배치 {b} 생성 실패: {str(e)[:140]}"); continue
        m = re.search(r"\[.*\]", raw, re.S)
        arr = json.loads(m.group(0)) if m else []
        kept, rej, rep = ps.validate(arr, CLS, env)
        fresh = [s for s in kept if s not in seen]
        seen.update(fresh); kept_all += fresh; rej_all += rej
        log(f"  배치 {b} 축={axis[:32]!r} → 수신 {len(arr)} 통과 {len(kept)} 신규 {len(fresh)} "
            f"누적 {len(kept_all)} · 승리형태 {rep['winning_share']:.0%}")

    if not kept_all:
        raise SystemExit("생성 0문장 — gemini CLI 확인")

    # ── 근접중복 컷 (§15 와 같은 규칙: 코사인 상한) ─────────────────────
    V = ps.embed_texts(kept_all)
    order = list(range(len(kept_all))); keep = []
    for j in order:
        if keep and float(np.max(V[j] @ V[keep].T)) > a.dedup: continue
        keep.append(j)
    sel = keep[:a.n]   # ponytail: 배치 순서대로 상한 — 축 커버리지 실측이 고르면 충분하고,
                       # 편중되면 배치 인덱스를 기록해 라운드로빈으로 뽑을 것
    texts = [kept_all[j] for j in sel]
    _k, _r, rep = ps.validate(texts, CLS, env)
    log(f"중복컷 {len(kept_all)} → {len(keep)} · 최종 {len(texts)} · 승리형태 {rep['winning_share']:.0%} "
        f"({'통과' if rep['quota_ok'] else '미달'}) · 형태 {rep['form_mix']}")

    np.savez_compressed(a.out.replace(".json", "_vectors.npz"),
                        texts=np.array(texts), vecs=V[sel].astype(np.float32))
    json.dump(dict(cls=CLS, env=env.name, n=len(texts), report=rep,
                   rules_source="prompt_standard.py (intrustion 규칙은 유추 — 스크립트 docstring 참조)",
                   rejected_examples=[{"text": t, "why": w} for t, w in rej_all[:20]],
                   sentences=texts),
              open(a.out, "w"), ensure_ascii=False, indent=1)
    log(f"→ {a.out} · 벡터 {a.out.replace('.json','_vectors.npz')}")


if __name__ == "__main__":
    main()
