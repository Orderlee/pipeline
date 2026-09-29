#!/usr/bin/env python3
"""APO 닫힌 루프 — 생성(PLM/cuda:1) → 즉시 채점(PE-Core/cuda:0) → 통과분만 채택.

왜 이 스크립트가 생겼나
    지금까지 생성과 채점이 분리돼 있었다. 문장을 눈감고 12,511개 만들어 놓고 나중에
    라벨-free 통계로 가지치기했고, 그 결과가 규칙으로 쓴 499문장에 졌다. 두 모델이
    같은 서비스에 있으면 순서를 바꿀 수 있다: 100개 뱉는 즉시 채점해서 통과분만 남긴다
    (rejection sampling). 생성량이 아니라 **검증률**이 품질을 만든다.

⚠️ 이 파일은 bind mount(`/workspace`) 라 커밋이 곧 실행 코드다. 배포를 트리거하지 않는다.
   서비스 쪽(`docker/embedding/`)은 반대로 **재빌드=배포=라벨링 중단**이니 분리해 뒀다.

GPU 규칙
    PE-Core = cuda:0, PLM = cuda:1(SAM3 소유 — PLM 이 항상 양보).
    단계 사이에 명시적으로 반납한다: 서술을 전부 끝내고 GPU1 을 비운 뒤 채점에 들어간다.
    `--keep` 를 주면 반납을 생략(연속 실행 시 재로딩 비용 회피).

서브커맨드
    selftest      배선 확인. **처음엔 반드시 이걸 먼저.**
    generate      오탐/미탐 프레임 → 문장 생성 → 검증 → 채택본 JSON
    explain       오탐 프레임 → (이긴 문장 ↔ 실제 장면) 대조표
    caption-all   프레임 전량 서술 → jsonl

예)
    docker exec -i docker-analysis-1 python3 /workspace/apo_loop.py selftest
    docker exec -i docker-analysis-1 python3 /workspace/apo_loop.py generate \
        --version v1.0.8.0 --cls falldown --role normal --n 50 --out /data/.../gen_verified.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

import numpy as np

sys.path.insert(0, "/workspace")
from embedding_client import EmbeddingClient  # noqa: E402

T0 = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - T0:6.1f}s] {msg}", flush=True)


# ── 프로파일 = 기존 정본 두 개를 그대로 쓴다 (세 번째 사본을 만들지 않는다) ──────
#   데이터 위치·조인키 : prompt_geometry.PROFILES   (sourceh / frames / sourcei)
#   생성 규칙(현장어휘) : prompt_standard.load_env  (등록명 또는 현장 JSON 경로)
# 새 현장 추가 = PROFILES 에 한 항목 + 현장 JSON 하나. 이 파일은 안 고친다.
class Ctx:
    def __init__(self, profile: str, env_spec: str | None):
        import prompt_geometry as pg
        import prompt_standard as ps

        if profile not in pg.PROFILES:
            raise SystemExit(f"모르는 프로파일 {profile!r} — 가능: {sorted(pg.PROFILES)}")
        d = pg.PROFILES[profile]
        self.name = profile
        self.root = d["root"]
        self.work = f"{self.root}/work"
        self.dataset = d["dataset"]
        self.pdataset = f"{d['dataset']}-prompts"
        self.key_join = d.get("key_join", "filepath_tail")
        self.classes = list(d["class_names"].values())
        self.group_field = d.get("group_field", "camera")
        self.gidx_offset = pg.GIDX_OFFSET
        self.ps = ps
        # 생성 규칙은 프로파일명으로 먼저 찾고, 없으면 --env 로 받은 JSON 경로.
        self.env = ps.load_env(env_spec or profile)

    def key(self, filepath, sample_id) -> str:
        """embed.npz 의 key. sourceh/sourcei 는 '<폴더>/<파일>', frames 는 샘플 id
        (실측: sourcei 6,032/6,032 · frames 187,994/199,972)."""
        if self.key_join == "sample_id":
            return str(sample_id)
        return "/".join(str(filepath).split("/")[-2:])


# ══════════════════════════════════════════════════════════════════════════
# 데이터 적재
# ══════════════════════════════════════════════════════════════════════════
def load_frames(ctx: "Ctx", pred_field: str | None = None) -> dict:
    """FiftyOne 메타 + embed.npz 벡터를 한 테이블로.

    벡터를 FiftyOne 에서 안 읽는 이유: `values("image_embedding")` 전량은 8GB/2.6h 다
    (memory: project_fiftyone_values_oom). npz 는 25MB.
    """
    import fiftyone as fo

    ds = fo.load_dataset(ctx.dataset)

    # GT 필드명이 데이터셋마다 다르다 (sourcei/source-h `ground_truth` vs frames `bank_gt`).
    # 가정하면 조용히 엉뚱한 축을 읽거나 ValueError 로 죽는다 — 실재하는 것을 고르고,
    # 없으면 무엇을 찾았는지 말하고 멈춘다.
    gt_field = next((f for f in ("ground_truth", "bank_gt") if ds.has_field(f)), None)
    if gt_field is None:
        raise SystemExit(f"{ctx.dataset} 에 GT 필드가 없다 (찾은 이름: ground_truth, bank_gt)")
    has_src = ds.has_field("gt_source")          # frames 에는 없다 — 선택 필드로 둔다
    has_group = ds.has_field(ctx.group_field)
    fields = ["filepath", "id", f"{gt_field}.label"]
    if has_src:
        fields.append("gt_source.label")
    if has_group:
        fields.append(ctx.group_field)
    if pred_field:
        fields.append(f"{pred_field}.label")

    vals = ds.values(fields)
    fp, sid, gt = vals[0], vals[1], vals[2]
    i = 3
    src = vals[i] if has_src else [None] * len(fp); i += 1 if has_src else 0
    cam = vals[i] if has_group else [None] * len(fp)
    pred = vals[-1] if pred_field else [None] * len(fp)
    ctx.gt_field = gt_field
    log(f"GT 필드={gt_field}"
        f"{'' if has_src else ' · gt_source 없음'}{'' if has_group else f' · {ctx.group_field} 없음'}")

    npz = np.load(f"{ctx.work}/embed.npz", allow_pickle=True)
    idx = {str(k): i for i, k in enumerate(npz["key"])}
    vec = npz["vec"]

    rows, miss = [], 0
    for f, sd, g, s, c, p in zip(fp, sid, gt, src, cam, pred):
        i = idx.get(ctx.key(f, sd))
        if i is None:
            miss += 1
            continue
        rows.append(dict(filepath=f, sid=sd, gt=g, gt_source=s, group=c, pred=p, vi=i))
    if miss:
        log(f"⚠️ 벡터 없는 프레임 {miss}건 (조인={ctx.key_join}) — embed.npz 가 오래됐거나 "
            f"임베딩 미생성분이다. sourcei 는 ledger_resync 로 갱신한다.")
    log(f"프레임 {len(rows):,} 적재 (벡터 {vec.shape})")
    return {"rows": rows, "vec": vec}


def pick_errors(tbl: dict, cls: str, role: str, n: int) -> list[dict]:
    """role=normal → 그 클래스로 **오탐**된 normal 프레임 (FP).
       role=event  → 그 클래스인데 normal 로 **미탐**된 프레임 (FN)."""
    rows = tbl["rows"]
    if role == "normal":
        cand = [r for r in rows if r["gt"] == "normal" and r["pred"] == cls]
    else:
        cand = [r for r in rows if r["gt"] == cls and r["pred"] in (None, "normal")]
    log(f"{role} 후보 {len(cand):,}건 (cls={cls}) → 상한 {n}")
    return cand[:n]


# ══════════════════════════════════════════════════════════════════════════
# 검증 — 생성 즉시 채점
# ══════════════════════════════════════════════════════════════════════════
def class_matrix(tbl: dict, cls: str) -> np.ndarray:
    ix = [r["vi"] for r in tbl["rows"] if r["gt"] == cls]
    return tbl["vec"][ix] if ix else np.zeros((0, tbl["vec"].shape[1]), dtype=np.float32)


def verify(vec_text: np.ndarray, near: np.ndarray, far: np.ndarray) -> dict:
    """문장 벡터가 near 쪽에 붙고 far 에서 떨어지는가. margin 이 판단 기준.

    임계 0.02 근거: 이 데이터에서 마진 중앙값이 0.01 이라 그 아래는 잡음이다
    (memory: project_viz_curation_phase01 — 승수만 보면 반대로 고른다).
    """
    v = vec_text / (np.linalg.norm(vec_text) + 1e-12)
    cn = float((near @ v).mean()) if len(near) else 0.0
    cf = float((far @ v).mean()) if len(far) else 0.0
    return {"cos_near": round(cn, 4), "cos_far": round(cf, 4), "margin": round(cn - cf, 4)}


# 생성기가 지시문을 되받아 쓴 문장 — 뱅크에 들어가면 안 된다. 2026-09-03 첫 실행에서
# "…but the detector incorrectly identifies it as 'falldown'" 이 마진 게이트를 통과했다.
# 마진은 장면 내용이 결정하므로 메타 어구가 붙어 있어도 통과한다 → 텍스트 규칙이 따로 필요.
_META_RE = __import__("re").compile(
    r"\b(detector|detect(s|ed|ion)?|classif\w*|model|label(s|ed)?|ground.?truth|"
    r"incorrect\w*|wrong\w*|false (positive|alarm)|surveillance frame|this (frame|image|photo))\b",
    __import__("re").I,
)
# "The frame shows a person …" 같은 액자 어구는 벗겨내면 쓸 만한 문장이 남는다.
_LEAD_RE = __import__("re").compile(
    r"^(the |this )?(frame|image|photo|picture|scene|video)\s+(shows|depicts|contains|displays)\s+", __import__("re").I
)


def split_sentences(text: str) -> list[str]:
    """생성 텍스트 → 뱅크 후보 문장. 액자 어구는 벗기고, 메타 서술은 버린다."""
    out = []
    for chunk in text.replace("\n", " ").split("."):
        s = " ".join(chunk.split()).strip(" -*•\t")
        s = _LEAD_RE.sub("", s)
        if not s:
            continue
        if _META_RE.search(s):          # 지시문 되받기 — 버린다
            continue
        s = s[0].upper() + s[1:]
        if 3 <= len(s.split()) <= 40:
            out.append(s if s.endswith(".") else s + ".")
    return out


# ══════════════════════════════════════════════════════════════════════════
# 서브커맨드
# ══════════════════════════════════════════════════════════════════════════
def cmd_selftest(args) -> int:
    ctx = Ctx(args.profile, args.env)
    log(f"프로파일 {ctx.name}: dataset={ctx.dataset} work={ctx.work} 조인={ctx.key_join} env={ctx.env.name}")
    cli = EmbeddingClient()
    h = cli._session.get(f"{cli.base_url}/health", timeout=20).json()
    log(f"health: slots={h.get('slots')} vram={h.get('vram_free_gb')}")
    if "plm" not in (h.get("slots") or {}):
        log("❌ PLM 슬롯 없음 — PLM_ENABLED 미설정이거나 이미지가 옛 버전이다.")
        return 2

    tbl = load_frames(ctx)
    r = tbl["rows"][0]
    img = open(r["filepath"], "rb").read()
    log(f"caption 대상: {ctx.key(r['filepath'], r['sid'])} (gt={r['gt']})")
    try:
        text = cli.caption(img, "Describe what is visible in this surveillance frame in one sentence.")
    except Exception as exc:                                   # noqa: BLE001
        log(f"❌ /caption 실패: {exc}")
        log("   VRAM 부족(503)이면 SAM3 가 GPU1 을 쓰는 중이다 — 나중에 재시도.")
        log("   그 외면 고칠 곳은 plm_be.PlmBackend._build_inputs 한 함수뿐이다.")
        return 3
    log(f"✅ PLM 문장: {text[:160]}")

    v = np.asarray(cli.embed_text(text), dtype=np.float32)
    assert v.shape == (1024,), f"임베딩 차원이 1024 가 아님: {v.shape}"
    cos = float(tbl["vec"][r["vi"]] @ (v / np.linalg.norm(v)))
    log(f"✅ 방금 만든 문장 ↔ 그 이미지 cos = {cos:.4f}")
    assert cos > 0.0, "자기 이미지와 음의 코사인 — 배선이 틀렸다(잘못된 프레임/벡터 정렬)"

    # 로드된 상태에서 재야 반납량이 보인다. 기동 직후 값과 비교하면 CUDA 컨텍스트(~0.9GB,
    # empty_cache 로 안 사라지고 프로세스 종료까지 남는다) 때문에 오히려 줄어 보인다.
    loaded = cli._session.get(f"{cli.base_url}/health", timeout=20).json()["vram_free_gb"]["cuda:1"]
    after = cli.release("plm")["vram_free_gb"]["cuda:1"]
    log(f"✅ 반납: cuda:1 free {loaded:.2f} → {after:.2f} GB (+{after - loaded:.2f})")
    assert after >= loaded, "unload 후 VRAM 이 안 늘었다 — 반납 경로가 동작하지 않는다"
    log("selftest 통과 — generate 로 진행 가능")
    return 0


def cmd_generate(args) -> int:
    ctx = Ctx(args.profile, args.env)
    ps = ctx.ps
    if args.cls not in ctx.classes:
        raise SystemExit(f"{ctx.name} 프로파일의 클래스가 아니다: {args.cls} — 가능 {ctx.classes}")
    log(f"프로파일 {ctx.name} · 현장규칙 {ctx.env.name}")
    cli = EmbeddingClient()
    tbl = load_frames(ctx, pred_field=args.pred_field or f"wave_pred_{args.version.replace('.', '_')}")
    targets = pick_errors(tbl, args.cls, args.role, args.n)
    if not targets:
        log("대상 0건 — 이 버전에서 그 오류 유형이 없다. --version/--cls 확인.")
        return 1

    # 생성 클래스: FP 를 흡수하려면 normal 문장을, FN 을 살리려면 이벤트 문장을 만든다.
    gen_cls = "normal" if args.role == "normal" else args.cls
    # ⚠️ 오판 클래스를 지시문에 넣지 않는다 — 모델이 "but the detector wrongly called it X" 를
    # 문장에 그대로 옮겨 적는다(2026-09-03 실측). 오탐 맥락은 **이미지 선별**에만 쓰고,
    # 생성 지시는 순수 서술로 둔다. 형태 지시는 prompt_standard 의 승리 템플릿을 따른다.
    subject = ("what the people and the space are doing" if args.role == "normal"
               else f"the visible evidence of {args.cls}")
    ask = (
        f"Describe {subject} in ONE plain present-tense sentence. "
        "Start with 'A person', 'People', 'It is', or the object itself. "
        "Do not mention cameras, detectors, frames, images, or labels. "
        "No place names, no speculation about intent."
    )

    # ── 1단계: 서술 (GPU1) ────────────────────────────────────────────────
    log(f"1단계 서술 — {len(targets)}장 (PLM, cuda:1)")
    said = []
    for i, r in enumerate(targets, 1):
        try:
            said.append((r, cli.caption(open(r["filepath"], "rb").read(), ask)))
        except Exception as exc:                               # noqa: BLE001
            log(f"  {i}/{len(targets)} 실패 — {exc}")          # per-file fail-forward
        if i % 10 == 0:
            log(f"  {i}/{len(targets)}")
    if not said:
        return 3

    if not args.keep:
        log(f"GPU1 반납: {cli.release('plm').get('vram_free_gb')}")

    # ── 2단계: 검증 (GPU0) ────────────────────────────────────────────────
    near = np.stack([tbl["vec"][r["vi"]] for r, _ in said])     # 생성 근거가 된 바로 그 프레임들
    far = class_matrix(tbl, args.cls if args.role == "normal" else "normal")
    log(f"2단계 검증 — near {near.shape[0]}장 / far {far.shape[0]}장 (PE-Core, cuda:0)")

    cands, seen = [], set()
    for r, text in said:
        for s in split_sentences(text):
            if s.lower() in seen:
                continue
            seen.add(s.lower())
            cands.append({"text": s, "src": ctx.key(r["filepath"], r["sid"])})
    log(f"후보 문장 {len(cands)} (중복 제거 후)")

    kept, dropped, vecs = [], [], {}
    for c in cands:
        v = np.asarray(cli.embed_text(c["text"]), dtype=np.float32)
        vecs[c["text"]] = v / (np.linalg.norm(v) + 1e-12)
        c.update(verify(v, near, far))
        ok_rule, why = True, ""
        try:                                                    # 기존 규칙 재사용 — 새로 안 쓴다
            keep_list, rej = ps.validate([c["text"]], gen_cls, ctx.env)
            ok_rule = bool(keep_list)
            why = "" if ok_rule else str(rej[0] if rej else "rule")
        except Exception:                                       # 규칙 모듈 시그니처가 달라도 채점은 계속
            pass
        c["rule_ok"], c["rule_reason"] = ok_rule, why
        (kept if (c["margin"] >= args.min_margin and ok_rule) else dropped).append(c)

    kept.sort(key=lambda x: -x["margin"])

    # 근접중복 컷 — "A man is sitting on the escalator" / "A person is sitting on an escalator"
    # 는 텍스트가 달라 exact 중복 제거를 통과한다(2026-09-03 실측: 채택 5개 중 3개가 같은 문장).
    # prompt_standard.DUP_COS(0.95) 와 같은 임계를 쓴다. 마진 높은 쪽을 남긴다(이미 정렬됨).
    uniq = []
    for c in kept:
        v = vecs[c["text"]]
        twin = next((u["text"] for u in uniq if float(v @ vecs[u["text"]]) >= ps.DUP_COS), None)
        if twin:
            c["dup_of"] = twin
            dropped.append(c)
        else:
            uniq.append(c)
    log(f"근접중복 컷(cos≥{ps.DUP_COS}): {len(kept)} → {len(uniq)}")
    kept = uniq

    out = {
        "profile": ctx.name, "env": ctx.env.name,
        "version": args.version, "cls": args.cls, "role": args.role, "gen_class": gen_cls,
        "n_images": len(said), "n_candidates": len(cands),
        "min_margin": args.min_margin, "kept": kept, "dropped": dropped,
    }
    dest = args.out or f"{ctx.work}/apo_gen_{args.cls}_{args.role}.json"
    with open(dest, "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    args.out = dest
    rate = 100 * len(kept) / max(len(cands), 1)
    log(f"✅ 채택 {len(kept)}/{len(cands)} ({rate:.1f}%) → {args.out}")
    for c in kept[:5]:
        log(f"   margin {c['margin']:+.4f}  {c['text'][:90]}")
    return 0


def cmd_explain(args) -> int:
    import fiftyone as fo

    ctx = Ctx(args.profile, args.env)
    vtag = args.version.replace("v", "").replace(".", "")
    wf = f"winner_gidx_v{vtag}"
    tbl = load_frames(ctx, pred_field=args.pred_field or f"wave_pred_{args.version.replace('.', '_')}")
    ds = fo.load_dataset(ctx.dataset)
    if not ds.has_field(wf):
        log(f"❌ {wf} 필드 없음 — 이 버전의 winner 축이 아직 없다.")
        return 1

    pds = fo.load_dataset(ctx.pdataset)
    sel = pds.match({"bank_version.label": args.version})
    gidx, texts = sel.values(["gidx", "text"])
    OFF = ctx.gidx_offset
    gmap = {int(g) % OFF: t for g, t in zip(gidx, texts) if g is not None}
    assert len(gmap) == len(gidx), (
        f"gidx % {OFF} 가 1:1 이 아니다 ({len(gmap)} != {len(gidx)}) — prompt_geometry.GIDX_OFFSET 확인"
    )

    fps, wins = ds.values(["filepath", wf])
    win_by_fp = {f: w for f, w in zip(fps, wins)}
    fp_rows = [r for r in tbl["rows"] if r["gt"] == "normal" and r["pred"] not in (None, "normal")][: args.n]
    log(f"오탐 {len(fp_rows)}건 분해")

    cli = EmbeddingClient()
    lines = [f"# 오탐 분해 — {args.version}", "",
             f"프레임 {len(fp_rows)}건. 왼쪽은 PE-Core 가 고른 문장, 오른쪽은 PLM 이 본 장면.", ""]
    for i, r in enumerate(fp_rows, 1):
        g = win_by_fp.get(r["filepath"])
        won = gmap.get(int(g) % OFF) if g is not None else None
        try:
            saw = cli.caption(open(r["filepath"], "rb").read(),
                              "Describe in one sentence exactly what is visible. No speculation.")
        except Exception as exc:                                # noqa: BLE001
            saw = f"(caption 실패: {exc})"
        lines += [f"## {i}. `{ctx.key(r['filepath'], r['sid'])}`  gt=normal → pred={r['pred']}  ({r['gt_source']})",
                  f"- **이긴 문장**: {won or '(winner 없음)'}",
                  f"- **실제 장면**: {saw}", ""]
        if i % 10 == 0:
            log(f"  {i}/{len(fp_rows)}")
    if not args.keep:
        cli.release("plm")
    dest = args.out or f"{ctx.work}/apo_fp_triage.md"
    with open(dest, "w") as f:
        f.write("\n".join(lines))
    args.out = dest
    log(f"✅ → {dest}")
    return 0


def cmd_caption_all(args) -> int:
    ctx = Ctx(args.profile, args.env)
    cli = EmbeddingClient()
    args.out = args.out or f"{ctx.work}/apo_captions.jsonl"
    tbl = load_frames(ctx)
    rows = tbl["rows"][: args.limit] if args.limit else tbl["rows"]
    done = set()
    if os.path.exists(args.out):                                # 재개 가능 — 18만 장은 한 번에 안 끝난다
        with open(args.out) as f:
            done = {json.loads(ln)["key"] for ln in f if ln.strip()}
        log(f"이어서: 이미 {len(done):,}건")
    log(f"서술 대상 {len(rows) - len(done):,}건")
    with open(args.out, "a") as f:
        for i, r in enumerate(rows, 1):
            k = ctx.key(r["filepath"], r["sid"])
            if k in done:
                continue
            try:
                t = cli.caption(open(r["filepath"], "rb").read(), args.prompt)
            except Exception as exc:                            # noqa: BLE001
                log(f"  {k} 실패 — {exc}")
                continue
            f.write(json.dumps({"key": k, "gt": r["gt"], "text": t}, ensure_ascii=False) + "\n")
            f.flush()
            if i % 50 == 0:
                log(f"  {i}/{len(rows)}")
    if not args.keep:
        cli.release("plm")
    log(f"✅ → {args.out}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", default="sourcei",
                    help="데이터 위치·조인키. prompt_geometry.PROFILES 의 키 (sourcei/frames/sourceh)")
    ap.add_argument("--env", default=None,
                    help="생성 규칙(현장 어휘). prompt_standard 등록명 또는 현장 JSON 경로. "
                         "생략하면 --profile 과 같은 이름으로 찾는다")
    ap.add_argument("--keep", action="store_true", help="단계 후 VRAM 반납 생략")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("selftest").set_defaults(fn=cmd_selftest)

    g = sub.add_parser("generate"); g.set_defaults(fn=cmd_generate)
    g.add_argument("--version", default="v1.0.8.0")
    g.add_argument("--cls", default="falldown")     # 유효성은 프로파일의 class_names 로 검사
    g.add_argument("--role", default="normal", choices=["normal", "event"])
    g.add_argument("--n", type=int, default=50)
    g.add_argument("--min-margin", type=float, default=0.02)
    g.add_argument("--pred-field", default=None)
    g.add_argument("--out", default=None)

    e = sub.add_parser("explain"); e.set_defaults(fn=cmd_explain)
    e.add_argument("--version", default="v1.0.8.0")
    e.add_argument("--n", type=int, default=30)
    e.add_argument("--pred-field", default=None)
    e.add_argument("--out", default=None)

    c = sub.add_parser("caption-all"); c.set_defaults(fn=cmd_caption_all)
    c.add_argument("--limit", type=int, default=0)
    c.add_argument("--prompt", default="Describe what is visible in this surveillance frame in one sentence.")
    c.add_argument("--out", default=None)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
