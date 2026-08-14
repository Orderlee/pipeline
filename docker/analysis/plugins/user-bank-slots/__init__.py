"""뱅크 A/B 슬롯 교체 오퍼레이터 — 사이드바에서 볼 두 버전을 App 안에서 고른다.

왜 필요한가 (2026-08-14 사용자 지적):
    `prompt compare` 패널은 뱅크 버전 29개를 드롭다운으로 전환하는데 프레임 사이드바는
    A/B 두 개로 고정돼 있었다. 비대칭의 원인은 **저장 형태**다 —
      · 문장(`<ds>-prompts`): `bank_version` 이 **필드 값** (long) → 드롭다운이 자연스럽다
      · 프레임(`<ds>`):        버전이 **필드 이름** (wide, `wave_pred_v1_0_8_0` …)
    프레임쪽 29버전을 전부 사이드바에 올리면 flat 686경로 문제로 되돌아간다. long 으로
    바꾸는 안은 3단 경로 App 크래시 + 리스트 필터의 원소 간 AND 불가로 기각됐다.
    → 남는 길은 "슬롯은 2개, 대신 **App 에서 갈아끼운다**" 이고, 이 파일이 그것이다.

동작: 드롭다운 2개(A/B) → `pred_{wave,argmax,topk}_{a,b}` 재적재 + `bank_run.slots` 갱신
      + 사이드바 그룹 라벨/필드 설명 갱신 → 데이터셋 리로드. 실측 ~8~15초.

⚠️ 로직 사본을 만들지 않는다. 정본은 `/workspace/fiftyone_app_setup.py` 이고 여기서
   import 한다. 컨테이너 PYTHONPATH 에 `/workspace` 가 없어 경로를 명시 삽입한다
   (사본을 두면 그게 정확히 2026-08-14 태그 드리프트 사고의 재발이다 — 스펙 §3 D7).

정본: docker/analysis/plugins/user-bank-slots/__init__.py
설계 근거: docs/superpowers/specs/2026-08-14-fiftyone-bank-filter-schema-design.md §M9 · §10
"""
import sys

import fiftyone as fo
import fiftyone.operators as foo
import fiftyone.operators.types as types

WORKSPACE = "/workspace"
SUFFIX = "-prompts"


def _setup():
    """App 설정 정본 모듈. 없으면 None — 오퍼레이터가 이유를 화면에 띄운다."""
    if WORKSPACE not in sys.path:
        sys.path.insert(0, WORKSPACE)
    try:
        import fiftyone_app_setup as A
        return A
    except Exception:  # noqa: BLE001
        return None


def _is_frames_dataset(ctx):
    """프레임 데이터셋인가 — 문장 데이터셋에 뜨면 오조작을 부른다."""
    ds = getattr(ctx, "dataset", None)
    if ds is None or ds.name.endswith(SUFFIX):
        return False
    return any(f.startswith("winner_gidx_") for f in ds.get_field_schema())


def _versions(A, ds):
    """(정본 순서 버전 목록, 버전→문장수).

    ⚠️ `sorted()` 를 쓰면 안 된다 — 실측 순서에서 `v1.0.10.3` 이 `v1.0.8.4` **뒤**다.
    정본 순서는 gidx 블록에서 역산한다 (`bank_order`).
    """
    info = A.bank_order(f"{ds.name}{SUFFIX}") or {}
    return info.get("order", []), info.get("headroom", {})


class SetBankSlots(foo.Operator):
    """사이드바 A/B 슬롯에 올릴 뱅크 버전을 고른다."""

    @property
    def config(self):
        return foo.OperatorConfig(
            name="set_bank_slots",
            label="뱅크 슬롯 교체 — 사이드바 A/B 에 올릴 버전 고르기",
            dynamic=True,
        )

    def resolve_placement(self, ctx):
        if not _is_frames_dataset(ctx):
            return None
        return types.Placement(
            types.Places.SAMPLES_GRID_ACTIONS,
            types.Button(label="뱅크 슬롯 교체 (A/B)", icon="swap_horiz", prompt=True),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        A = _setup()
        if A is None:
            inputs.view("err", types.Error(
                label=f"{WORKSPACE}/fiftyone_app_setup.py 를 불러올 수 없습니다",
                description="호스트에서 `docker cp docker/analysis/fiftyone_app_setup.py "
                            "docker-analysis-1:/workspace/` 로 배포하세요"))
            return types.Property(inputs)
        if not _is_frames_dataset(ctx):
            inputs.view("err", types.Error(
                label="프레임 데이터셋에서 실행하세요",
                description="문장 데이터셋(`<이름>-prompts`)에는 슬롯이 없습니다"))
            return types.Property(inputs)

        versions, counts = _versions(A, ctx.dataset)
        if not versions:
            inputs.view("err", types.Error(
                label=f"{ctx.dataset.name}{SUFFIX} 에서 뱅크 버전을 찾지 못했습니다",
                description="`prompt_geometry.py promptmap` 이 먼저 돌아야 합니다"))
            return types.Property(inputs)

        cur_a, cur_b = A.read_slots(ctx.dataset)
        for key, label, cur in (("slot_a", "슬롯 A", cur_a), ("slot_b", "슬롯 B", cur_b)):
            ch = types.Dropdown(label=label)
            for v in versions:
                n = counts.get(v)
                ch.add_choice(v, label=f"{v}   ({n:,}문장)" if n else v)
            inputs.enum(key, ch.values(), required=True, view=ch,
                        default=cur if cur in versions else versions[0])

        inputs.view("note", types.Notice(
            label=f"버전 {len(versions)}개 · 현재 A={cur_a} B={cur_b}",
            description="교체하면 pred_wave_*/pred_argmax_* 슬롯이 다시 채워지고 "
                        "사이드바 그룹 라벨과 필드 설명이 함께 갱신됩니다 (약 10~20초). "
                        "원본 버전별 필드는 건드리지 않으므로 언제든 되돌릴 수 있습니다."))
        return types.Property(inputs, view=types.View(label="뱅크 슬롯 교체"))

    def execute(self, ctx):
        A = _setup()
        if A is None:
            raise RuntimeError(f"{WORKSPACE}/fiftyone_app_setup.py 를 불러올 수 없습니다")
        a = str(ctx.params["slot_a"])
        b = str(ctx.params["slot_b"])
        if a == b:
            raise ValueError(f"A 와 B 가 같은 버전({a})입니다 — 비교가 되지 않습니다")

        name = ctx.dataset.name
        A.cmd_slots([name], slots=(a, b), apply=True)
        A.cmd_filters([name], slots=(a, b), apply=True)

        ds = fo.load_dataset(name)
        sch = ds.get_field_schema()
        # 교체 결과를 숫자로 보여준다 — "무엇이 달라졌나" 가 이 조작의 목적이다
        rows, diff = [], {}
        for rule in ("wave", "argmax", "topk"):
            fa, fb = f"pred_{rule}_a", f"pred_{rule}_b"
            if fa not in sch or fb not in sch:
                continue
            va, vb = ds.values(fa + ".label"), ds.values(fb + ".label")
            gt = ds.values("ground_truth.label") if "ground_truth" in sch else [None] * len(va)
            n_diff = sum(1 for x, y in zip(va, vb) if x != y)
            fp_a = sum(1 for g, x in zip(gt, va) if g == "normal" and x not in (None, "normal"))
            fp_b = sum(1 for g, x in zip(gt, vb) if g == "normal" and x not in (None, "normal"))
            diff[rule] = n_diff
            rows.append({"rule": rule, "n_diff": n_diff, "fp_a": fp_a, "fp_b": fp_b})

        ctx.trigger("reload_dataset")
        return {"slot_a": a, "slot_b": b, "rows": rows,
                "summary": " · ".join(f"{k} 판정변화 {v:,}장" for k, v in diff.items())
                           or "비교 가능한 규칙 없음"}

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("slot_a", label="슬롯 A")
        outputs.str("slot_b", label="슬롯 B")
        outputs.str("summary", label="요약")
        t = types.TableView()
        t.add_column("rule", label="규칙")
        t.add_column("n_diff", label="A↔B 판정 다른 프레임")
        t.add_column("fp_a", label="오탐 A (GT=normal)")
        t.add_column("fp_b", label="오탐 B (GT=normal)")
        outputs.list("rows", types.Object(), view=t, label="규칙별 비교")
        return types.Property(outputs, view=types.View(label="슬롯 교체 완료"))


def register(p):
    p.register(SetBankSlots)
