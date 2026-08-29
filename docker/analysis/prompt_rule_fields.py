#!/usr/bin/env python3
"""규칙 준수 판정을 **FiftyOne 필드로** 기록한다 — 지금은 파일에만 있어 화면에서 안 보인다.

문제: `sourcei_optbank_prompts.py` 가 규칙을 채점하지만 결과가
`optbank/rulecheck.json` · `csv/53` · `fig/f51` 에만 남는다. FiftyOne 쪽은 옆 데이터셋
`sourcei-OPT-prompts` 의 `form` 필드 하나뿐이고, **compare 패널이 보는 `sourcei-prompts`
에는 그 필드가 없다.** 그래서 "규칙을 지켰나"를 App 에서 확인할 수 없었다.

여기서 `sourcei-prompts` 의 **텍스트 보유 문장 전량**에 세 필드를 쓴다:
  form        문장 템플릿 형태 (`prompt_standard._form_of` — 정본 재사용)
  rule_ok     그 클래스 규칙 통과 여부 ("통과" / "위반")
  rule_reason 위반 사유 1개 (금칙어·길이·숫자·장소구 과다) — 통과면 "-"

전 뱅크에 쓰는 이유: 우리 뱅크만 채점하면 "공급 뱅크는 규칙을 지키나"를 비교할 수 없다.
판정 규칙은 `prompt_standard` 한 곳에서 오므로 여기서 규칙을 새로 만들지 않는다.

⚠️ 자리표시자(`(텍스트 없음 #N)`) 문장은 텍스트가 없어 판정 불가 → "미판정" 으로 남긴다.
   비워 두면 사이드바에서 "통과"와 구별되지 않는다(부재를 통과로 읽는 사고).

기본 DRY-RUN. `--apply` 로 기록.
"""
import os, sys, re, json, time, collections
sys.path.insert(0, "/workspace")
import fiftyone as fo
import prompt_standard as ps

DS = os.environ.get("RULE_DS", "sourcei-prompts")
APPLY = "--apply" in sys.argv
PH = re.compile(r"^\(텍스트 없음 #\d+\)$")
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

def judge(text, cls):
    """단문 판정 — `ps.validate` 를 1문장 배치로 불러 사유 문자열까지 그대로 받는다."""
    kept, rej, _ = ps.validate([text], cls, ps.ENVS.get("sourcei"))
    return ("통과", "-") if kept else ("위반", rej[0][1] if rej else "알 수 없음")

ds = fo.load_dataset(DS)
txts, clss = ds.values(["text", "category.label"])
log(f"{DS}: {len(txts):,} 샘플")

forms, oks, reasons = [], [], []
stat = collections.Counter()
for t, c in zip(txts, clss):
    t = (t or "").strip()
    if not t or PH.match(t) or c not in ps.CLASSES:
        forms.append(None); oks.append("미판정"); reasons.append("텍스트 없음" if not t or PH.match(t) else f"클래스 밖({c})")
        stat["미판정"] += 1; continue
    f = ps._form_of(t)
    ok, why = judge(t, c)
    forms.append(f); oks.append(ok); reasons.append(why)
    stat[ok] += 1
    if ok == "위반": stat["사유:" + re.sub(r":.*", "", why)] += 1

log("판정: " + " · ".join(f"{k} {v:,}" for k, v in stat.most_common(8)))
# 클래스별 이기는 모양 비율 — 쿼터(0.70) 충족 여부를 뱅크별로 볼 수 있게 요약도 남긴다
per = collections.defaultdict(collections.Counter)
for f, c in zip(forms, clss):
    if f: per[c][f] += 1
for c in ps.CLASSES:
    tot = sum(per[c].values())
    if not tot: continue
    win = ps.WINNING_FORM[c]
    log(f"  {c:9} 이기는 모양 {win:15} {per[c][win]:,}/{tot:,} = {per[c][win]/tot:.1%} "
        f"(쿼터 {ps.FORM_QUOTA:.0%} {'충족' if per[c][win]/tot >= ps.FORM_QUOTA else '미달'})")

if not APPLY:
    log("DRY-RUN — --apply 로 기록"); print("DONE"); sys.exit(0)

ds.set_values("form", [fo.Classification(label=f) if f else None for f in forms])
ds.set_values("rule_ok", [fo.Classification(label=o) for o in oks])
ds.set_values("rule_reason", reasons)
ds.save()
log("필드 3종 기록: form · rule_ok · rule_reason")
json.dump({k: int(v) for k, v in stat.items()},
          open("/data/fiftyone/frames_bank/report/sourcei_gt/optbank/rule_fields.json", "w"),
          ensure_ascii=False, indent=1)
print("DONE")
