#!/usr/bin/env python3
"""전 카테고리 **전량 생성** — 공급 문장 0, 생성 문장만으로 뱅크 후보를 만든다.

왜: §23 에서 만든 혼합 뱅크(공급 75%)는 성능은 1위였지만 **생성 규칙을 안 지켰다**
(승리 템플릿 비율 2~19%, 하한 70%). 원인은 선택을 라벨-free 통계로만 했고 공급 문장이
애초에 그 규칙으로 쓰이지 않았기 때문. 전량 생성이면 규칙은 정의상 지켜진다 —
남는 질문은 **성능과 분포-IoU 가 버티는가**다(§17: 생성 단독은 균질해서 PR-AUC 0.382 로 붕괴).

그래서 **균질성을 깨는 것이 이 스크립트의 핵심**이다:
  · 배치마다 **장소 부분집합을 회전**시킨다 (한 배치가 15곳을 다 쓰면 문장이 서로 닮는다)
  · 배치마다 **다양성 축**을 다르게 준다 (자세 / 가시성 / 주변인 / 조명 …)
  · 배치 간 중복은 텍스트 단위로 제거하고, 최종 선택에서 코사인 중복컷을 한 번 더 건다

호스트에서 실행한다 (gemini CLI 가 컨테이너에 없다):
    /home/user/anaconda3/bin/python3 gen_full_bank.py --out gen_full.json
"""
import os, sys, json, re, time, argparse, subprocess, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import prompt_standard as ps

MODEL = "gemini-3.1-pro-preview"
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

# 클래스별 다양성 축 — 배치마다 다른 축을 강조해 문장이 서로 닮지 않게 한다
AXES = {
    "falldown": ["body position (face-down, on the back, on the side, curled, limbs splayed)",
                 "visibility (partly hidden behind a rack, edge of frame, far from camera, clear view)",
                 "bystanders (alone and unattended, someone approaching, a crowd gathering, staff nearby)",
                 "lighting and floor (dim basement, bright reflective floor, mixed shadow, night)",
                 "duration cues (still for a long time, not responding, unmoving)",
                 "body extent (whole body visible, only legs visible, upper body only)"],
    "fire": ["flame size (a point flame, a small flame, a spreading fire, a large blaze)",
             "burning material (packaging, fabric, a bin, an appliance, cabling)",
             "location of the flame (on the floor, on a shelf, inside a machine, at a wall)",
             "visual effects (glow on nearby surfaces, flickering, sparks, bright core)",
             "occlusion (partly hidden behind a shelf, seen through a doorway, at the frame edge)",
             "stage (just starting, growing, well established)"],
    "smoke": ["color (white, grey, dark, blue-tinted)",
              "density (thin wisp, moderate haze, thick opaque plume)",
              "motion (rising, drifting sideways, pooling along the ceiling, filling the space)",
              "effect on visibility (slightly hazy, half obscured, almost nothing visible)",
              "source area (near the floor, from a doorway, from equipment, from a corner)",
              "extent (a small patch, one aisle, the whole room)"],
    "normal": ["escalator and stair activity", "cleaning and maintenance work",
               "workers bending, crouching or kneeling to handle goods",
               "shoppers browsing, queuing and carrying items",
               "empty spaces, parked vehicles and quiet corridors",
               "visual conditions: glare, reflections, steam, dust, warm or red lighting"],
}
PER_BATCH = {"falldown": 160, "fire": 160, "smoke": 160, "normal": 200}
N_BATCH = 6


def gemini(instruction, model=MODEL, timeout=900):
    p = subprocess.run(["gemini", "-m", model, "-p", instruction],
                       capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        raise RuntimeError((p.stderr or "")[-400:])
    return p.stdout


def batch_prompt(cls, n, axis, places):
    """표준 지시문 + 배치 고유의 다양성 지시. 장소를 좁혀 배치마다 다른 영역을 훑는다."""
    base = ps.build_generation_prompt(ps.sourcei, cls, n)
    extra = [
        "",
        "THIS BATCH ONLY:",
        f"- Vary primarily along this axis: {axis}.",
        f"- Use only these places (still at most one place phrase per sentence): {', '.join(places)}.",
        "- Do not repeat sentence skeletons from a generic template; each sentence should differ in "
        "its main verb or its described state, not only in the place word.",
    ]
    return base + "\n".join(extra)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="gen_full.json")
    ap.add_argument("--classes", default="normal,falldown,fire,smoke")
    ap.add_argument("--batches", type=int, default=N_BATCH)
    a = ap.parse_args()
    classes = [c for c in a.classes.split(",") if c]
    out, report = {}, {}
    places = ps.sourcei.places
    for cls in classes:
        kept_all, rej_all, seen = [], [], set()
        for b in range(a.batches):
            axis = AXES[cls][b % len(AXES[cls])]
            sub = places[(b * 5) % len(places):][:5] or places[:5]
            if len(sub) < 5: sub = (sub + places)[:5]
            n = PER_BATCH[cls]
            try:
                raw = gemini(batch_prompt(cls, n, axis, sub))
            except Exception as e:
                log(f"  {cls} 배치 {b} 생성 실패: {str(e)[:120]}"); continue
            m = re.search(r"\[.*\]", raw, re.S)
            arr = json.loads(m.group(0)) if m else []
            kept, rej, rep = ps.validate(arr, cls, ps.sourcei)
            fresh = [s for s in kept if s not in seen]
            seen.update(fresh); kept_all += fresh; rej_all += rej
            log(f"  {cls} 배치 {b} 축={axis[:34]!r} 장소{len(sub)} → 수신 {len(arr)} 통과 {len(kept)} "
                f"신규 {len(fresh)} 누적 {len(kept_all)} · 승리형태 {rep['winning_share']:.0%}")
        _k, _r, rep = ps.validate(kept_all, cls, ps.sourcei)
        out[cls] = kept_all
        report[cls] = dict(n=len(kept_all), rejected=len(rej_all), form=rep["form_mix"],
                           winning_form=rep["winning_form"], winning_share=rep["winning_share"],
                           quota_ok=rep["quota_ok"])
        log(f"{cls}: 최종 {len(kept_all)} · 승리형태 {rep['winning_share']:.0%} "
            f"({'통과' if rep['quota_ok'] else '미달'}) · 형태 {rep['form_mix']}")
    json.dump(dict(sentences=out, report=report, model=MODEL, batches=a.batches),
              open(a.out, "w"), ensure_ascii=False, indent=1)
    log(f"→ {a.out} 총 {sum(len(v) for v in out.values()):,}문장")


if __name__ == "__main__":
    main()
