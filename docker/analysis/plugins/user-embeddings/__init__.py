"""Embeddings 패널 보강 — OSS 에서 막힌 두 가지를 버튼으로 되살린다.

1. `compute_visualization` — OSS 는 패널의 `+` 를 Enterprise CTA 로 하드코딩해뒀다
   (`APP_MODE="fiftyone"` 빌드타임 상수 → minifier 가 실제 호출 분기를 지워버려
   번들 패치 없이는 못 되살림). 네이티브 placement API 로 같은 자리에 버튼을 더 단다.
   `@voxel51/brain` 원본 프롬프트는 입력이 12개인데다 embeddings 를 비우면 zoo
   모델을 받으러 가서 **빈 brain key 만 남기고 실패**한다 — 이 데이터셋에 맞춰
   임베딩 필드를 자동 선택하는 4-입력 폼으로 감쌌다.

2. `combine_color_fields` — Color by 는 필드 하나만 받는다. 두 필드를 합친
   파생 StringField 를 만들어 그걸 고르게 한다 (조합별 색 = 사실상 2-필드 색칠).

3. `save_visualization_coords` — 패널에는 축 눈금/라벨 토글이 없다 (UMAP/t-SNE 축값은
   재실행마다 바뀌어 해석 불가라 의도된 설계). 좌표가 필요하면 brain 결과의 points 를
   `<key>_x`/`<key>_y` FloatField 로 꺼낸다 — 사이드바 슬라이더 필터·Color by 그라디언트로
   쓸 수 있다.
"""

import contextlib
import gc
import random

import numpy as np

import fiftyone as fo
import fiftyone.brain as fob
import fiftyone.operators as foo
import fiftyone.operators.types as types

COMBO_SEPARATOR = " | "

# 이 오퍼레이터들은 **FiftyOne App 프로세스 안에서** 실행된다. 188K 데이터셋의
# 임베딩을 한 번에 올리면 12GB(list[float] 1024-d)라 앱과 호스트가 같이 죽는다
# (2026-07-28 실측: 가용 15GB→1GB, add 속도 404→0.1/s 붕괴). 그래서 전부 배치.
FIT_MAX = 30_000  # 이 이하면 통짜 계산, 초과하면 샘플-fit → 배치 transform
TBATCH = 10_000  # 임베딩 로드/변환 배치
SET_BATCH = 20_000  # set_values 배치


def _batches(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _thread_cap(max_threads=4):
    """계산 중에만 BLAS/OpenMP 스레드를 묶어 호스트 CPU 독점을 막는다."""
    try:
        import threadpoolctl

        return threadpoolctl.threadpool_limits(max_threads)
    except Exception:  # noqa: BLE001 — 없으면 캡 없이 진행
        return contextlib.nullcontext()


def _embeddings_of(dataset, ids, field):
    return np.asarray(
        dataset.select(ids, ordered=True).values(field), dtype="float32"
    )


def _set_values_batched(dataset, field, mapping_items):
    """{sample_id: value} 를 배치로 나눠 쓴다 — 188K 단일 bulk write 회피."""
    for chunk in _batches(mapping_items, SET_BATCH):
        dataset.set_values(field, dict(chunk), key_field="id")
        gc.collect()

METHODS = (
    ("umap", "UMAP", "비선형 — 국소 군집이 잘 갈린다 (기본)"),
    ("tsne", "t-SNE", "비선형 — 느리지만 촘촘한 군집에 강함"),
    ("pca", "PCA", "선형 — 즉시 계산, 전역 구조 보존"),
)


def _vector_fields(dataset):
    """임베딩으로 쓸 수 있는 필드. tags 같은 문자열 리스트는 제외."""
    numeric = (fo.FloatField, fo.IntField)
    out = []
    for name, field in dataset.get_field_schema().items():
        if isinstance(field, fo.VectorField):
            out.append(name)
        elif isinstance(field, fo.ListField) and isinstance(field.field, numeric):
            out.append(name)
    return out


class ComputeVisualization(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="compute_visualization",
            label="Compute visualization (OSS)",
            dynamic=True,
        )

    def resolve_placement(self, ctx):
        # ⚠️ EMBEDDINGS_ACTIONS 가 아니라 그리드 툴바에 둔다. 시각화 brain key 가
        # 0개인 데이터셋(예: 갓 빌드한 frames_full)에서 Embeddings 패널은 툴바 없이
        # Enterprise CTA 만 렌더한다 → EMBEDDINGS_ACTIONS 버튼이 **정작 필요한 순간에
        # 닿지 않는다.** 그리드 툴바는 항상 보이므로 두 상태 모두에서 사용 가능.
        return types.Placement(
            types.Places.SAMPLES_GRID_ACTIONS,
            types.Button(
                label="Compute visualization (OSS)", icon="add_chart", prompt=True
            ),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        fields = _vector_fields(ctx.dataset)

        if not fields:
            inputs.view(
                "none",
                types.Error(label="임베딩 필드가 없습니다 (숫자 ListField/VectorField)"),
            )
            return types.Property(inputs)

        inputs.str(
            "brain_key",
            required=True,
            label="Brain key",
            description="Embeddings 패널 왼쪽 드롭다운에 나타날 이름",
        )

        # ponytail: 임베딩 필드가 하나면 고를 게 없다 — 기본값으로 박고 폼에서 감춘다.
        embeddings_choices = types.DropdownView()
        for name in fields:
            embeddings_choices.add_choice(name, label=name)
        inputs.enum(
            "embeddings",
            fields,
            default=fields[0],
            required=True,
            label="Embeddings",
            description="이미 계산된 임베딩 필드",
            view=embeddings_choices,
        )

        method_choices = types.DropdownView()
        for value, label, desc in METHODS:
            method_choices.add_choice(value, label=label, description=desc)
        inputs.enum(
            "method",
            [m[0] for m in METHODS],
            default="umap",
            required=True,
            label="Method",
            view=method_choices,
        )

        target_choices = types.RadioGroup()
        target_choices.add_choice("DATASET", label="전체 데이터셋")
        target_choices.add_choice("CURRENT_VIEW", label="현재 뷰(필터 적용분)")
        inputs.enum(
            "target",
            target_choices.values(),
            default="DATASET",
            required=True,
            label="대상",
            view=target_choices,
        )

        brain_key = ctx.params.get("brain_key")
        if brain_key and brain_key in ctx.dataset.list_brain_runs():
            inputs.view(
                "dup",
                types.Warning(label=f"'{brain_key}' 는 이미 있습니다 — 덮어씁니다"),
            )

        return types.Property(
            inputs, view=types.View(label="Compute visualization (OSS)")
        )

    def execute(self, ctx):
        brain_key = ctx.params["brain_key"]
        embeddings = ctx.params["embeddings"]
        method = ctx.params.get("method", "umap")

        target = ctx.dataset
        if ctx.params.get("target") == "CURRENT_VIEW" and ctx.view is not None:
            target = ctx.view

        if brain_key in ctx.dataset.list_brain_runs():
            ctx.dataset.delete_brain_run(brain_key)

        n = target.count()
        with _thread_cap():
            if n <= FIT_MAX:
                fob.compute_visualization(
                    target,
                    embeddings=embeddings,
                    method=method,
                    brain_key=brain_key,
                    num_dims=2,
                )
            else:
                points = _big_projection(target, embeddings, method, n)
                fob.compute_visualization(target, points=points, brain_key=brain_key)

        return {"brain_key": brain_key, "count": n, "method": method}

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("brain_key", label="생성된 brain key")
        outputs.str("method", label="method")
        outputs.int("count", label="샘플 수")
        outputs.view(
            "hint", types.Notice(label="F5 로 새로고침한 뒤 왼쪽 드롭다운에서 선택하세요")
        )
        return types.Property(outputs, view=types.View(label="완료"))


def _big_projection(target, field, method, n):
    """FIT_MAX 초과 데이터셋용 배치 투영.

    `points=` 는 samples 기본 순서에 정렬돼야 하므로(sample_ids 인자 없음)
    `values("id")` 순서로 배치를 만들어 같은 순서로 채운다.
    """
    ids = target.values("id")
    dataset = target if isinstance(target, fo.Dataset) else target._dataset

    if method == "tsne":
        # sklearn TSNE 는 out-of-sample transform 이 없어 전량 fit 뿐인데
        # 188K 는 메모리·시간 모두 불가. 조용히 다른 결과를 내지 말고 거부한다.
        raise ValueError(
            f"t-SNE 는 {FIT_MAX:,}개 초과에서 지원하지 않습니다 (out-of-sample 변환 불가). "
            f"현재 {n:,}개 — UMAP/PCA 를 쓰거나 뷰를 좁혀 주세요."
        )

    pts = np.empty((n, 2), dtype="float32")

    if method == "pca":
        from sklearn.decomposition import IncrementalPCA

        ipca = IncrementalPCA(n_components=2)
        for batch in _batches(ids, TBATCH):
            X = _embeddings_of(dataset, batch, field)
            if len(X) >= 2:
                ipca.partial_fit(X)
            del X
            gc.collect()
        off = 0
        for batch in _batches(ids, TBATCH):
            X = _embeddings_of(dataset, batch, field)
            pts[off : off + len(batch)] = ipca.transform(X)
            off += len(batch)
            del X
            gc.collect()
        return pts

    import umap

    reducer = umap.UMAP(n_components=2, metric="cosine", low_memory=True, verbose=False)
    random.seed(42)
    fit_ids = [ids[i] for i in sorted(random.sample(range(n), min(FIT_MAX, n)))]
    Xf = _embeddings_of(dataset, fit_ids, field)
    reducer.fit(Xf)
    del Xf, fit_ids
    gc.collect()
    off = 0
    for batch in _batches(ids, TBATCH):
        X = _embeddings_of(dataset, batch, field)
        pts[off : off + len(batch)] = reducer.transform(X)
        off += len(batch)
        del X
        gc.collect()
    return pts


def _scalar_fields(dataset):
    """Color by 로 쓸 만한 스칼라 필드만. id/filepath 류 고유값은 색칠해도 의미 없다."""
    skip = {"id", "filepath", "minio_key", "image_id", "entity_id", "asset_id", "caption"}
    types_ok = (fo.StringField, fo.BooleanField, fo.IntField)
    return [
        name
        for name, field in dataset.get_field_schema().items()
        if isinstance(field, types_ok) and name not in skip
    ]


class CombineColorFields(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="combine_color_fields",
            label="Color by 2 fields",
            dynamic=True,
        )

    def resolve_placement(self, ctx):
        return types.Placement(
            types.Places.EMBEDDINGS_ACTIONS,
            types.Button(label="Color by 2 fields", icon="palette", prompt=True),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        choices = _scalar_fields(ctx.dataset)

        for key, label in (("field1", "First field"), ("field2", "Second field")):
            dropdown = types.DropdownView()
            for name in choices:
                dropdown.add_choice(name, label=name)
            inputs.enum(key, choices, required=True, label=label, view=dropdown)

        f1 = ctx.params.get("field1")
        f2 = ctx.params.get("field2")
        if f1 and f2:
            if f1 == f2:
                inputs.view("warn", types.Warning(label="서로 다른 두 필드를 고르세요"))
            else:
                inputs.view(
                    "preview",
                    types.Notice(label=f"생성될 필드: {_combo_name(f1, f2)}"),
                )

        return types.Property(inputs, view=types.View(label="Color by 2 fields"))

    def execute(self, ctx):
        f1 = ctx.params["field1"]
        f2 = ctx.params["field2"]
        if f1 == f2:
            raise ValueError("서로 다른 두 필드를 골라야 합니다")

        # ponytail: 뷰가 아니라 항상 데이터셋 전체에 쓴다. 필터된 뷰에만 쓰면
        # 나머지 샘플이 None 이 돼서 Color by 범례에 'none' 덩어리가 생긴다.
        dataset = ctx.dataset
        target = _combo_name(f1, f2)
        ids = dataset.values("id")
        v1 = dataset.values(f1)
        v2 = dataset.values(f2)
        items = [
            (sid, _fmt(a) + COMBO_SEPARATOR + _fmt(b))
            for sid, a, b in zip(ids, v1, v2)
        ]
        del v1, v2
        gc.collect()
        _set_values_batched(dataset, target, items)

        return {"field": target, "count": len(items)}

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("field", label="생성된 필드")
        outputs.int("count", label="적용된 샘플 수")
        outputs.view(
            "hint",
            types.Notice(label="F5 로 새로고침한 뒤 Color by 에서 선택하세요"),
        )
        return types.Property(outputs, view=types.View(label="완료"))


def _combo_name(f1, f2):
    return f"{f1}__x__{f2}"


def _fmt(value):
    return "none" if value is None else str(value)


def _visualization_keys(dataset):
    """points 를 가진 시각화 brain run 만. similarity 인덱스(text_search)는 제외."""
    keys = []
    for key in dataset.list_brain_runs():
        try:
            cls = dataset.get_brain_info(key).config.cls or ""
        except Exception:  # noqa: BLE001 — 손상된 run 은 조용히 건너뛴다
            continue
        if "visualization" in cls.lower():
            keys.append(key)
    return keys


class SaveVisualizationCoords(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="save_visualization_coords",
            label="좌표를 필드로 저장",
            dynamic=True,
        )

    def resolve_placement(self, ctx):
        return types.Placement(
            types.Places.EMBEDDINGS_ACTIONS,
            types.Button(label="좌표를 필드로 저장", icon="straighten", prompt=True),
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        keys = _visualization_keys(ctx.dataset)

        if not keys:
            inputs.view("none", types.Error(label="시각화 brain key 가 없습니다"))
            return types.Property(inputs)

        dropdown = types.DropdownView()
        for key in keys:
            dropdown.add_choice(key, label=key)
        inputs.enum(
            "brain_key",
            keys,
            default=keys[0],
            required=True,
            label="Brain key",
            description="이 시각화의 2D 좌표를 필드로 꺼냅니다",
            view=dropdown,
        )

        brain_key = ctx.params.get("brain_key")
        if brain_key:
            inputs.view(
                "preview",
                types.Notice(
                    label=f"생성될 필드: {brain_key}_x, {brain_key}_y"
                ),
            )

        return types.Property(inputs, view=types.View(label="좌표를 필드로 저장"))

    def execute(self, ctx):
        brain_key = ctx.params["brain_key"]
        results = ctx.dataset.load_brain_results(brain_key)
        if results is None:
            raise ValueError(f"'{brain_key}' 에 결과가 없습니다 (실패한 run)")

        points = results.points
        if points.shape[1] < 2:
            raise ValueError(f"2D 이상이어야 합니다 (num_dims={points.shape[1]})")

        # patches 기반 시각화면 sample_ids 가 없고 label_ids 를 쓴다.
        ids = getattr(results, "sample_ids", None)
        if ids is None:
            raise ValueError("patches 기반 시각화는 지원하지 않습니다")

        # 188K 단일 bulk write 를 피해 배치로 쓴다 (id 키라 순서 의존 없음).
        sids = [str(i) for i in ids]
        _set_values_batched(
            ctx.dataset, f"{brain_key}_x", list(zip(sids, points[:, 0].tolist()))
        )
        _set_values_batched(
            ctx.dataset, f"{brain_key}_y", list(zip(sids, points[:, 1].tolist()))
        )

        return {
            "fields": f"{brain_key}_x, {brain_key}_y",
            "count": len(ids),
        }

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("fields", label="생성된 필드")
        outputs.int("count", label="샘플 수")
        outputs.view(
            "hint",
            types.Notice(
                label="F5 후 사이드바에서 슬라이더 필터로, Color by 에서 그라디언트로 쓸 수 있습니다"
            ),
        )
        return types.Property(outputs, view=types.View(label="완료"))


def register(p):
    p.register(ComputeVisualization)
    p.register(CombineColorFields)
    p.register(SaveVisualizationCoords)
