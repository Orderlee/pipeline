#!/usr/bin/env python3
"""프롬프트 평가 산출물 → 정본 3층 + 계약 검증기.

Phase 1.5(거버넌스 게이트) 산출물 2/2. [[prompt_bank_ledger.py]] 가 **입력**(뱅크 문장)을
정본화했다면 이 스크립트는 **출력**(점수·귀속)을 정본화한다. 스펙 §6.2 의 집계 3층
(`prompt_eval_runs` / `prompt_sentence_stats` / `prompt_frame_pred`) 컬럼에 맞춘 JSONL 을
내보내고, 그 위에 스키마가 강제할 계약을 **지금 파일 단계에서** 검사한다.

왜 DB 가 아니라 파일인가: migration 은 `src/vlm_pipeline/` 경로라 적용 = prod 재빌드 =
라벨링 중단이다. 스펙 §6.2 판정대로 마이그레이션은 Phase 2 스파이크 go 와 같은 배포에서
importer·reader 와 함께 태어난다. 그때까지 이 JSONL 이 계약의 실물이고, validator 가
"스키마가 아직 없어서 아무거나 들어가는" 구간을 메운다.

R1a 의 핵심 문제 — 같은 개념이 세 가지 이름으로 산다:
    pred_v1_0_8_0 / pred_margin_v080 / wave_pred_v1_0_8_4 …
버전 접미사가 필드마다 `v1_0_8_0` 스타일과 `v080` 스타일로 갈린다. 규칙을 발명하지 않고
**두 후보를 실제 스키마에 조회해서** 있는 쪽을 쓴다 (없으면 에러로 세운다).

사용:
    # 컨테이너 안 (fiftyone 필요)
    python3 prompt_scores_export.py export --dataset source-h --bank v1.0.8.0 --out /tmp/x
    # 호스트에서도 됨 (fiftyone 불필요)
    python3 prompt_scores_export.py validate /tmp/x
    python3 prompt_scores_export.py selftest

정본: docker/analysis/prompt_scores_export.py
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import Counter

RULES = ("argmax_k1", "topk_vote", "dist_iou")
CLASSES = ("normal", "falldown", "fire", "smoke", "smoking")

# 규칙 → (예측 필드, 마진 필드, 승자 필드). None = 그 규칙에 그 양이 존재하지 않는다.
# ⚠️ topk_vote 에 winner 가 없는 건 데이터 결손이 아니라 정의다 — K=10 다수결에는 단일
#    승자가 정의되지 않는다. dist_iou 도 프레임 귀속 자체가 없다 (스펙 §5.3).
RULE_FIELDS = {
    "argmax_k1": ("pred_{v}", "pred_margin_{v}", "winner_gidx_{v}"),
    "topk_vote": ("vote_{v}", "vote_margin_{v}", None),
    "dist_iou": ("wave_pred_{v}", None, None),
}
# 규칙별 문장 통계. argmax_k1 만 귀속 기반 양(wins/purity/n_cameras)을 갖는다.
RULE_SENTENCE_FIELDS = {
    "argmax_k1": {"wins": "wins", "purity": "purity", "n_cameras": "n_cameras",
                  "adopted": "adopted"},
    "dist_iou": {"gain": "wave_gain", "wave_role": "wave_role"},
    "topk_vote": {},
}


def suffixes(bank: str) -> list[str]:
    """`v1.0.8.0` → 실제로 쓰이는 두 접미사 표기 후보. 어느 쪽인지는 스키마가 정한다."""
    parts = bank.lstrip("vV").split(".")
    out = ["v" + "_".join(parts)]                    # v1_0_8_0 (pred_/vote_/wave_pred_)
    if len(parts) >= 3:
        out.append("v" + "".join(parts[-3:]))        # v080 (margin_/winner_gidx_)
    return out


def resolve(schema, template: str, bank: str) -> str | None:
    """필드 템플릿 + 뱅크 → 스키마에 실존하는 필드명. 없으면 None."""
    if template is None:
        return None
    for sfx in suffixes(bank):
        name = template.format(v=sfx)
        if name in schema:
            return name
    return None


def label_of(v):
    """Classification → label, 그 외는 그대로. 값을 발명하지 않는다."""
    return getattr(v, "label", v)


def run_id_for(dataset: str, bank: str, rule: str, code_version: str) -> str:
    h = hashlib.sha1(f"{dataset}|{bank}|{rule}|{code_version}".encode()).hexdigest()[:12]
    return f"pr-{h}"


# ────────────────────── export (fiftyone 필요) ──────────────────────
def cmd_export(args) -> int:
    import fiftyone as fo

    ds = fo.load_dataset(args.dataset)
    schema = ds.get_field_schema()
    prompts_name = args.prompts or f"{args.dataset}-prompts"
    prompts = fo.load_dataset(prompts_name) if prompts_name in fo.list_datasets() else None

    # frame_key: 데이터셋이 가진 안정 식별자를 우선 쓰고, 무엇을 썼는지 run 에 기록한다.
    key_field = next((f for f in ("image_id", "entity_id", "asset_id") if f in schema),
                     "filepath")
    code_version = args.code_version

    runs, sent_rows, frame_rows = [], [], []
    for rule in args.rules:
        pred_t, margin_t, winner_t = RULE_FIELDS[rule]
        f_pred = resolve(schema, pred_t, args.bank)
        if not f_pred:
            print(f"⏭  {rule}: 예측 필드 없음 ({pred_t.format(v='|'.join(suffixes(args.bank)))}) "
                  f"— 이 데이터셋은 이 규칙으로 채점된 적이 없다. 건너뜀")
            continue
        f_margin = resolve(schema, margin_t, args.bank)
        f_winner = resolve(schema, winner_t, args.bank)
        rid = run_id_for(args.dataset, args.bank, rule, code_version)

        need = [key_field, f_pred] + [f for f in (f_margin, f_winner, "ground_truth") if f]
        n, hit = 0, Counter()
        for s in ds.select_fields([f for f in need if f in schema or f == "filepath"]):
            pred = label_of(s.get_field(f_pred))
            if pred is None:
                continue
            n += 1
            hit[pred] += 1
            frame_rows.append({
                "run_id": rid,
                "rule": rule,                       # 비정규화 복제 — (run_id, rule) 복합 FK 용
                "frame_key": str(s.get_field(key_field)),
                "pred_class": pred,
                "margin": s.get_field(f_margin) if f_margin else None,
                "winner_gidx": s.get_field(f_winner) if f_winner else None,
                "gt_class": label_of(s.get_field("ground_truth"))
                            if "ground_truth" in schema else None,
            })

        # 문장 통계는 **요청한 뱅크 버전의 문장만** 붙인다. 거르지 않으면 v084 run 에
        # v080 문장 지표가 조용히 실린다 (실측: 두 prompts 데이터셋 모두 v1.0.8.0 만 보유).
        n_sent, prompts_bv = 0, None
        if prompts is not None and RULE_SENTENCE_FIELDS[rule]:
            pschema = prompts.get_field_schema()
            cols = {k: v for k, v in RULE_SENTENCE_FIELDS[rule].items() if v in pschema}
            has_bv = "bank_version" in pschema
            for p in prompts.select_fields(["gidx"] + (["bank_version"] if has_bv else [])
                                           + list(cols.values())):
                if has_bv and label_of(p.get_field("bank_version")) != args.bank:
                    continue
                row = {"run_id": rid, "gidx": p.get_field("gidx")}
                for out_name, fld in cols.items():
                    row[out_name] = label_of(p.get_field(fld))
                # 계약: adopted 는 불리언이어야 한다 (FiftyOne 에는 Classification 으로 산다)
                if isinstance(row.get("adopted"), str):
                    row["adopted"] = row["adopted"].lower() in ("true", "yes", "adopted", "1")
                sent_rows.append(row)
                n_sent += 1
            prompts_bv = args.bank if n_sent else None
            if not n_sent and cols:
                print(f"⚠️  {rule}: {prompts_name} 에 bank_version={args.bank} 문장이 0개 "
                      f"— 문장 통계 없이 프레임 예측만 내보낸다")

        runs.append({
            "run_id": rid,
            "rule": rule,
            "rule_params": {"k": 10} if rule == "topk_vote" else {},
            "bank_version": args.bank,
            "cohort_scope": {"dataset": args.dataset, "n_frames": n,
                             "frame_key_source": key_field,
                             "prompts_dataset": prompts_name if prompts else None,
                             "prompts_bank_version": prompts_bv,
                             "n_sentences": n_sent},
            "code_version": code_version,
            "embedding_model": args.embedding_model,
            "metrics": {"pred_distribution": dict(hit)},
            "source_fields": {"pred": f_pred, "margin": f_margin, "winner_gidx": f_winner},
        })
        print(f"✅ {rule:10s} run={rid} frames={n:,} "
              f"sentences={sum(1 for r in sent_rows if r['run_id'] == rid):,} "
              f"[{f_pred} / {f_margin} / {f_winner}]")

    if not runs:
        print("❌ 내보낼 run 이 없다 — --bank 표기나 데이터셋을 확인할 것")
        return 2
    os.makedirs(args.out, exist_ok=True)
    for name, rows in (("prompt_eval_runs.jsonl", runs),
                       ("prompt_sentence_stats.jsonl", sent_rows),
                       ("prompt_frame_pred.jsonl", frame_rows)):
        with open(os.path.join(args.out, name), "w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
        print(f"→ {os.path.join(args.out, name)} ({len(rows):,}행)")
    return validate_dir(args.out)


# ────────────────────── validate (fiftyone 불필요) ──────────────────────
def _load(d: str, name: str) -> list[dict]:
    p = os.path.join(d, name)
    if not os.path.exists(p):
        return []
    return [json.loads(line) for line in open(p, encoding="utf-8") if line.strip()]


def check(runs: list[dict], sents: list[dict], frames: list[dict]) -> list[str]:
    """스키마가 강제할 계약을 파일 단계에서 검사. 반환: 위반 메시지 목록."""
    bad: list[str] = []
    by_run = {r["run_id"]: r for r in runs}

    for r in runs:
        if r["rule"] not in RULES:
            bad.append(f"run {r['run_id']}: rule '{r['rule']}' 가 enum 밖")
        if not r.get("code_version"):
            bad.append(f"run {r['run_id']}: code_version 없음 — 재현 불가")
    if len(by_run) != len(runs):
        bad.append("run_id 중복 — (run_id) PK 위반")

    seen_keys = set()
    for f in frames:
        run = by_run.get(f["run_id"])
        if run is None:
            bad.append(f"frame_pred: 미등록 run_id {f['run_id']} (FK 위반)")
            continue
        if f.get("rule") != run["rule"]:
            bad.append(f"frame_pred {f['frame_key']}: rule 복제본이 run 과 불일치 "
                       f"({f.get('rule')} vs {run['rule']}) — (run_id, rule) 복합 FK 위반")
        # §6.2 의 핵심 CHECK — 귀속이 존재하는 규칙에서만 winner_gidx 가 산다
        has_winner = f.get("winner_gidx") is not None
        if run["rule"] == "argmax_k1" and not has_winner:
            bad.append(f"frame_pred {f['frame_key']}: argmax_k1 인데 winner_gidx 가 NULL")
        if run["rule"] != "argmax_k1" and has_winner:
            bad.append(f"frame_pred {f['frame_key']}: {run['rule']} 에는 프레임 귀속이 "
                       f"정의되지 않는데 winner_gidx 가 있다")
        if f.get("pred_class") not in CLASSES:
            bad.append(f"frame_pred {f['frame_key']}: pred_class '{f.get('pred_class')}' 가 enum 밖")
        k = (f["run_id"], f["frame_key"])
        if k in seen_keys:
            bad.append(f"frame_pred: (run_id, frame_key) 중복 {k} — PK 위반")
        seen_keys.add(k)

    gidx_by_run: dict[str, set] = {}
    for s in sents:
        run = by_run.get(s["run_id"])
        if run is None:
            bad.append(f"sentence_stats: 미등록 run_id {s['run_id']} (FK 위반)")
            continue
        gidx_by_run.setdefault(s["run_id"], set()).add(s.get("gidx"))

    # 계보 오염 방지 (run 당 1회): 문장 통계가 있는데 그 문장의 뱅크가 run 의 뱅크와 다르면
    # v084 run 에 v080 지표가 실리는 조용한 오류다 — 실측으로 한 번 실제 발생했다.
    for rid, known in gidx_by_run.items():
        run = by_run[rid]
        bv = (run.get("cohort_scope") or {}).get("prompts_bank_version")
        if bv != run.get("bank_version"):
            bad.append(f"run {rid}: 문장 통계의 뱅크({bv})가 run 의 "
                       f"뱅크({run.get('bank_version')})와 다르다 — 계보 오염")
        # wins 는 귀속 기반 양이므로 argmax_k1 에만 존재해야 한다
        if s.get("wins") is not None and run["rule"] != "argmax_k1":
            bad.append(f"sentence_stats gidx={s.get('gidx')}: {run['rule']} 에 wins 가 있다 "
                       f"— wins 는 argmax_k1 귀속에서만 정의된다")

    for rid, run in by_run.items():
        if run["rule"] != "argmax_k1":
            continue
        fr = [f for f in frames if f["run_id"] == rid]
        known = gidx_by_run.get(rid)
        if known:
            orphan = {f["winner_gidx"] for f in fr if f.get("winner_gidx") is not None} - known
            if orphan:
                bad.append(f"run {rid}: winner_gidx {len(orphan)}개가 문장 통계에 없다 "
                           f"(조인 미폐쇄, 예: {sorted(orphan)[:3]})")
            wins = sum(s.get("wins") or 0 for s in sents if s["run_id"] == rid)
            if wins and wins != len(fr):
                bad.append(f"run {rid}: sum(wins)={wins:,} ≠ 프레임 {len(fr):,} "
                           f"— 완전분할 불변식 붕괴 (producer drift 의심)")
    return bad


def validate_dir(d: str) -> int:
    runs = _load(d, "prompt_eval_runs.jsonl")
    sents = _load(d, "prompt_sentence_stats.jsonl")
    frames = _load(d, "prompt_frame_pred.jsonl")
    if not runs:
        print(f"❌ {d}: prompt_eval_runs.jsonl 이 없다")
        return 2
    bad = check(runs, sents, frames)
    print(f"검사 대상: run {len(runs)} / 문장통계 {len(sents):,} / 프레임예측 {len(frames):,}")
    if bad:
        print(f"❌ 계약 위반 {len(bad)}건")
        for m in bad[:20]:
            print(f"   - {m}")
        if len(bad) > 20:
            print(f"   … 외 {len(bad) - 20}건")
        return 1
    print("✅ 계약 통과 — winner_gidx NULL 규칙 · (run_id,rule) 복합 FK · 조인 폐쇄 · 완전분할")
    return 0


def cmd_validate(args) -> int:
    return validate_dir(args.dir)


# ────────────────────── selftest ──────────────────────
def cmd_selftest(_args) -> int:
    # 접미사 해석 — 두 표기 스타일이 다 후보로 나와야 한다 (R1a 의 실체)
    assert suffixes("v1.0.8.0") == ["v1_0_8_0", "v080"], suffixes("v1.0.8.0")
    assert suffixes("v1.0.8.4") == ["v1_0_8_4", "v084"]
    schema = {"pred_v1_0_8_0", "pred_margin_v080", "winner_gidx_v080"}
    assert resolve(schema, "pred_{v}", "v1.0.8.0") == "pred_v1_0_8_0"
    assert resolve(schema, "pred_margin_{v}", "v1.0.8.0") == "pred_margin_v080"
    assert resolve(schema, "vote_{v}", "v1.0.8.0") is None

    scope = {"prompts_bank_version": "v1.0.8.0"}
    good_run = {"run_id": "r1", "rule": "argmax_k1", "code_version": "abc",
                "bank_version": "v1.0.8.0", "cohort_scope": scope}
    wave_run = {"run_id": "r2", "rule": "dist_iou", "code_version": "abc",
                "bank_version": "v1.0.8.0", "cohort_scope": scope}
    fr = {"run_id": "r1", "rule": "argmax_k1", "frame_key": "f1",
          "pred_class": "fire", "margin": 0.1, "winner_gidx": 7}
    st = {"run_id": "r1", "gidx": 7, "wins": 1}
    assert check([good_run], [st], [fr]) == [], check([good_run], [st], [fr])

    # 1) argmax_k1 인데 winner 없음
    assert any("winner_gidx 가 NULL" in m for m in check([good_run], [st], [{**fr, "winner_gidx": None}]))
    # 2) 귀속 없는 규칙에 winner 있음 (§6.2 CHECK 의 반대 방향)
    assert any("귀속이 정의되지 않는데" in m
               for m in check([wave_run], [], [{**fr, "run_id": "r2", "rule": "dist_iou"}]))
    # 3) rule 복제본 불일치 = 복합 FK 위반
    assert any("복합 FK 위반" in m for m in check([good_run], [st], [{**fr, "rule": "topk_vote"}]))
    # 4) 조인 미폐쇄 — 승자 문장이 통계에 없음
    assert any("조인 미폐쇄" in m for m in check([good_run], [{**st, "gidx": 99}], [fr]))
    # 5) 완전분할 붕괴 — sum(wins) ≠ 프레임 수
    assert any("완전분할 불변식 붕괴" in m for m in check([good_run], [{**st, "wins": 5}], [fr]))
    # 6) wins 가 귀속 없는 규칙에 붙음
    assert any("wins 는 argmax_k1" in m
               for m in check([wave_run], [{"run_id": "r2", "gidx": 7, "wins": 1}], []))
    # 7) 미등록 run_id / enum 밖 클래스 / PK 중복
    assert any("FK 위반" in m for m in check([good_run], [], [{**fr, "run_id": "zzz"}]))
    assert any("enum 밖" in m for m in check([good_run], [st], [{**fr, "pred_class": "cat"}]))
    assert any("PK 위반" in m for m in check([good_run], [st], [fr, fr]))
    # 8) code_version 없으면 재현 불가
    assert any("code_version" in m for m in check([{**good_run, "code_version": ""}], [st], [fr]))
    # 9) 계보 오염 — v084 run 에 v080 문장 통계 (실제로 한 번 발생했던 오류)
    v084 = {**good_run, "bank_version": "v1.0.8.4"}
    assert any("계보 오염" in m for m in check([v084], [st], [fr])), check([v084], [st], [fr])
    print("✅ selftest 통과 (계약 9종 + 필드 해석 3종)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="command", required=True)

    e = sub.add_parser("export")
    e.add_argument("--dataset", required=True)
    e.add_argument("--bank", required=True, help="뱅크 버전 태그 (예: v1.0.8.0)")
    e.add_argument("--prompts", help="문장 데이터셋 (기본: <dataset>-prompts)")
    e.add_argument("--rules", default=",".join(RULES))
    e.add_argument("--out", required=True)
    e.add_argument("--code-version", default=os.environ.get("BANK_CODE_VERSION", "unpinned"),
                   help="산출 코드 커밋 SHA — 재현성의 3요소 중 하나")
    e.add_argument("--embedding-model", default="PE-Core-L14-336")
    e.set_defaults(func=cmd_export)

    v = sub.add_parser("validate")
    v.add_argument("dir")
    v.set_defaults(func=cmd_validate)

    sub.add_parser("selftest").set_defaults(func=cmd_selftest)

    args = ap.parse_args()
    if args.command == "export":
        args.rules = [r for r in args.rules.split(",") if r]
        unknown = set(args.rules) - set(RULES)
        if unknown:
            ap.error(f"알 수 없는 규칙 {unknown} — {RULES} 중에서 고를 것")
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
