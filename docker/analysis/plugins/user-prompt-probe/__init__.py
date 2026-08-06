"""프롬프트 프로브 — App 안에서 후보 문장을 쓰고 **즉시** 채점한다.

## 왜 필요한가

지금 루프에는 구멍이 하나 있다. FiftyOne 에서 "이 군집이 안 잡힌다"를 **보고**,
문장을 **쓰고**, 점수를 **보는** 세 동작이 서로 다른 곳에 흩어져 있다 —
후보 문장을 하나 시험하려면 `prompt_geometry.py` 의 `PROBE_CANDIDATES` dict 에
손으로 써넣고 스테이지를 재실행해야 했다. 그 사이 "무엇을 보고 있었는지"가 날아간다.

이 오퍼레이터는 그 구멍만 메운다. 보던 화면 그대로에서 문장을 입력하면
`/embed_text`(7.5ms)로 임베딩해 **현재 뷰의 프레임들에 대해 판정 변화를 계산**한다.

## 어떻게 App 안에서 계산하나

뱅크(수만 문장 × 1024-d)는 App 프로세스에 못 올린다. 대신 `prompt_geometry.py probecache`
가 프레임마다 네 값을 미리 심어둔다 — 그것만 있으면 재채점이 **정확히** 재현된다.

    probe_bar_<tag>   top-K 마지막 코사인 = 진입 기준선
    probe_votes_<tag> 클래스별 현재 득표
    probe_topc_<tag>  클래스별 top-K 내 최고 코사인 (동표 해소)
    probe_out_<tag>   진입 시 밀려나는 문장의 클래스

후보 코사인 c 가 bar 를 넘으면 votes[cand]+1 / votes[out]−1, topc[cand]=max(topc, c) 로
갱신하고 `votes + (topc+2)/10` argmax — `bank_vote_stream` 과 같은 규칙이다.

## 무엇을 보고 판단하나

**진입률**만 높으면 안 된다. 배경을 서술한 문장도 진입률은 높다 — 그게 「배경 자석」이다.
그래서 세 가지를 함께 낸다: 진입률 / **순이득**(고친 수 − 망친 수) / **배경 코사인**.
배경 코사인은 같은 카메라의 `GT=normal` 프레임과의 평균 유사도다. 높으면 자석이다.

로직 검증: 컨테이너에서 `python __init__.py` (재채점 규칙 assert).
"""

import os

import numpy as np

import fiftyone as fo
import fiftyone.operators as foo
import fiftyone.operators.types as types

EMBED_URL = os.environ.get("EMBED_URL", "http://embedding-service:8003")

# App 프로세스에서 도는 동기 연산이라 상한을 둔다. 13k 프레임 × 1024-d = 54MB 로
# 충분히 빠르지만(<1s), 20만 프레임 데이터셋에서 그대로 돌면 앱이 멈춘다.
MAX_FRAMES = 40_000
BATCH = 8_000


def _tags(dataset):
    """probecache 가 심어둔 뱅크 태그 목록."""
    return sorted(
        k[len("probe_bank_"):]
        for k in (dataset.info or {})
        if k.startswith("probe_bank_")
    )


def _meta(dataset, tag):
    info = dataset.info or {}
    return (
        info.get(f"probe_classes_{tag}") or [],
        int(info.get(f"probe_k_{tag}") or 10),
        info.get(f"probe_bank_{tag}") or "?",
    )


def _embed_text(text):
    import requests

    # 응답은 {"vector": [...], "dim": 1024, "model_name": ...} — 프레임 임베딩과 같은 인코더
    r = requests.post(f"{EMBED_URL}/embed_text", data={"text": text}, timeout=120)
    r.raise_for_status()
    v = np.asarray(r.json()["vector"], dtype="float32").ravel()
    n = np.linalg.norm(v)
    return v / n if n else v


def rescore(cos, bar, votes, topc, out_c, cand_c):
    """후보 문장 1개를 넣었을 때의 새 예측. 규칙은 `bank_vote_stream` 과 동일.

    cos[N] · bar[N] · votes[N,C] · topc[N,C] · out_c[N] · cand_c(int)
    반환 (new_pred[N], entered[N] bool)
    """
    entered = cos > bar
    v = votes.astype(np.int32).copy()
    t = topc.astype(np.float32).copy()
    idx = np.flatnonzero(entered)
    if len(idx):
        v[idx, cand_c] += 1
        # 밀려나는 자리가 후보와 같은 클래스면 표 수는 그대로다
        v[idx, out_c[idx]] -= 1
        t[idx, cand_c] = np.maximum(t[idx, cand_c], cos[idx])
    return (v + (t + 2.0) / 10.0).argmax(axis=1), entered


class ProbePrompt(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="probe_prompt",
            label="프롬프트 프로브 — 문장 즉시 채점",
            dynamic=True,
        )

    def resolve_placement(self, ctx):
        # 그리드 툴바 — Embeddings 패널이 없는 상태에서도 항상 닿는다
        return types.Placement(
            types.Places.SAMPLES_GRID_ACTIONS,
            types.Button(label="프롬프트 프로브", icon="science", prompt=True),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        tags = _tags(ctx.dataset)
        if not tags:
            inputs.view(
                "none",
                types.Error(
                    label="probe 캐시가 없습니다 — "
                    "`prompt_geometry.py probecache` 를 먼저 실행하세요"
                ),
            )
            return types.Property(inputs)

        dd = types.DropdownView()
        for t in tags:
            _, k, bank = _meta(ctx.dataset, t)
            dd.add_choice(t, label=f"{bank} (k={k})")
        inputs.enum("tag", tags, default=tags[0], required=True, label="뱅크", view=dd)

        tag = ctx.params.get("tag") or tags[0]
        classes, k, bank = _meta(ctx.dataset, tag)

        inputs.str(
            "text",
            required=True,
            label="후보 문장",
            description="한 줄에 하나씩. 여러 개를 넣으면 **묶음으로** 평가합니다",
            view=types.TextFieldView(),
        )
        cd = types.DropdownView()
        for c in classes:
            cd.add_choice(c, label=c)
        inputs.enum(
            "cls", classes, required=True, label="선언 클래스",
            description="이 문장이 주장하는 클래스", view=cd,
        )

        radio = types.RadioGroup()
        radio.add_choice("CURRENT_VIEW", label="현재 뷰 (필터 적용분)")
        radio.add_choice("DATASET", label="전체 데이터셋")
        inputs.enum("target", radio.values(), default="CURRENT_VIEW",
                    required=True, label="대상", view=radio)

        view = ctx.view if ctx.view is not None else ctx.dataset.view()
        n = view.count() if ctx.params.get("target") != "DATASET" else ctx.dataset.count()
        if n > MAX_FRAMES:
            inputs.view("cap", types.Warning(
                label=f"{n:,}장 — 상한 {MAX_FRAMES:,} 초과. 뷰를 좁히세요"))
        else:
            inputs.view("info", types.Notice(
                label=f"{n:,}장에 대해 top-{k} 재채점 (뱅크 {bank})"))
        return types.Property(inputs, view=types.View(label="프롬프트 프로브"))

    def execute(self, ctx):
        tag = ctx.params["tag"]
        classes, k, bank = _meta(ctx.dataset, tag)
        cand_c = classes.index(ctx.params["cls"])
        texts = [t.strip() for t in str(ctx.params["text"]).splitlines() if t.strip()]
        if not texts:
            raise ValueError("문장을 입력하세요")

        view = ctx.dataset if ctx.params.get("target") == "DATASET" else (
            ctx.view if ctx.view is not None else ctx.dataset.view())
        n = view.count()
        if n > MAX_FRAMES:
            raise ValueError(f"{n:,}장은 상한 {MAX_FRAMES:,} 초과 — 뷰를 좁히세요")

        need = ["embedding", f"probe_bar_{tag}", f"probe_votes_{tag}",
                f"probe_topc_{tag}", f"probe_out_{tag}", "ground_truth.label", "camera"]
        emb, bar, votes, topc, out_c, gtl, cams = (view.values(f) for f in need)
        E = np.asarray(emb, dtype="float32")
        E /= np.linalg.norm(E, axis=1, keepdims=True) + 1e-12
        bar = np.asarray(bar, dtype="float32")
        votes = np.asarray(votes, dtype="int32")
        topc = np.asarray(topc, dtype="float32")
        out_c = np.asarray(out_c, dtype="int64")
        gt = np.array([classes.index(g) if g in classes else -1 for g in gtl])
        cams = np.asarray(cams)

        base = (votes + (topc + 2.0) / 10.0).argmax(axis=1)
        base_ok = base == gt

        # 배경 코사인 — 같은 카메라의 GT=normal 프레임과의 평균 유사도
        ni = classes.index("normal") if "normal" in classes else 0
        bg_mask = gt == ni

        rows, cur_v, cur_t, cur_out = [], votes, topc, out_c
        for txt in texts:
            e = _embed_text(txt)
            cos = E @ e
            new, entered = rescore(cos, bar, cur_v, cur_t, cur_out, cand_c)
            new_ok = new == gt
            fixed = int((~base_ok & new_ok).sum())
            broke = int((base_ok & ~new_ok).sum())
            rows.append({
                "text": txt[:90],
                "enter_rate": float(entered.mean()),
                "fixed": fixed,
                "broke": broke,
                "net": fixed - broke,
                "bg_cos": float(cos[bg_mask].mean()) if bg_mask.any() else 0.0,
                "max_cos": float(cos.max()),
            })
            # 묶음 평가: 앞 문장이 채택된 상태에서 다음 문장을 잰다
            idx = np.flatnonzero(entered)
            if len(idx):
                cur_v = cur_v.copy(); cur_t = cur_t.copy()
                cur_v[idx, cand_c] += 1
                cur_v[idx, cur_out[idx]] -= 1
                cur_t[idx, cand_c] = np.maximum(cur_t[idx, cand_c], cos[idx])

        final = (cur_v + (cur_t + 2.0) / 10.0).argmax(axis=1)
        return {
            "n": int(n), "bank": bank, "k": k, "cls": ctx.params["cls"],
            "base_acc": float(base_ok.mean()),
            "new_acc": float((final == gt).mean()),
            "total_net": int((final == gt).sum() - base_ok.sum()),
            "rows": rows,
        }

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.int("n", label="평가 프레임")
        outputs.str("bank", label="뱅크")
        outputs.str("cls", label="선언 클래스")
        outputs.float("base_acc", label="현재 정확도")
        outputs.float("new_acc", label="후보 채택 시 정확도")
        outputs.int("total_net", label="순이득 (묶음 전체)")
        tbl = types.TableView()
        tbl.add_column("text", label="문장")
        tbl.add_column("enter_rate", label="top-k 진입률")
        tbl.add_column("fixed", label="고친 프레임")
        tbl.add_column("broke", label="망친 프레임")
        tbl.add_column("net", label="순이득")
        tbl.add_column("bg_cos", label="배경 코사인 ↓")
        tbl.add_column("max_cos", label="최고 코사인")
        outputs.list("rows", types.Object(), label="문장별", view=tbl)
        outputs.view("hint", types.Notice(
            label="배경 코사인이 높으면 「배경 자석」입니다 — 진입률이 높아도 채택하지 마세요"))
        return types.Property(outputs, view=types.View(label="프로브 결과"))


def register(p):
    p.register(ProbePrompt)


def _self_check():
    """재채점 규칙만 검증 (App·임베딩 서비스 없이)."""
    C = 4
    # 프레임 3장: [0] 진입O·같은클래스 밀림, [1] 진입X, [2] 진입O·다른클래스 밀림
    bar = np.array([0.50, 0.90, 0.50], dtype="float32")
    cos = np.array([0.60, 0.10, 0.60], dtype="float32")
    votes = np.zeros((3, C), dtype="int32")
    votes[:, 0] = 6      # normal 6표
    votes[:, 2] = 4      # fire 4표
    topc = np.full((3, C), -2.0, dtype="float32")
    topc[:, 0] = 0.7
    topc[:, 2] = 0.55
    out_c = np.array([2, 0, 0], dtype="int64")   # 밀려날 자리
    new, entered = rescore(cos, bar, votes, topc, out_c, cand_c=2)

    assert entered.tolist() == [True, False, True], entered
    # [0] fire+1 / fire−1 → 6:4 그대로 normal
    assert new[0] == 0, new[0]
    # [1] 진입 실패 → 변화 없음
    assert new[1] == 0, new[1]
    # [2] fire+1 / normal−1 → 5:5 동표, topc fire 0.60 > normal 0.7? → normal 이 높다
    assert new[2] == 0, new[2]

    # 동표에서 후보 코사인이 더 높으면 뒤집힌다
    cos2 = np.array([0.60, 0.10, 0.95], dtype="float32")
    new2, _ = rescore(cos2, bar, votes, topc, out_c, cand_c=2)
    assert new2[2] == 2, new2[2]

    # 진입만 하고 아무것도 안 바뀌는 경우: 표차가 2 이상이면 1표로는 못 뒤집는다
    v3 = votes.copy(); v3[:, 0] = 8; v3[:, 2] = 2
    new3, _ = rescore(cos2, bar, v3, topc, out_c, cand_c=2)
    assert new3[2] == 0, new3[2]
    print("self-check OK")


if __name__ == "__main__":
    _self_check()
