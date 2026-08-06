#!/usr/bin/env python3
"""FiftyOne 데이터셋 표시층 자동 정리 — 사이드바 그룹 + 분석용 뷰 + 워크스페이스.

**왜 필요한가**: 데이터셋을 빌드하면 필드가 40~70개씩 평평하게 쏟아져 필터 사이드바에서
분석이 불가능하다. 이 모듈은 **필드별 카디널리티를 실측해 역할을 자동 판정**하고,
사이드바 그룹 + 노이즈 제외 뷰를 만들어 준다. 데이터셋마다 필드 목록을 손으로 나열하지
않으므로 어떤 데이터셋에도 붙는다.

**실측으로 확정한 FiftyOne 1.19 동작 2개 (2026-07-29, source-h 871샘플 DOM 검증)**
1. `app_config.sidebar_groups` 에서 경로를 빼는 것만으로는 **숨겨지지 않는다.**
   미배정 필드는 자동 생성 `PRIMITIVES` 그룹에 모여 사이드바 맨 아래에 그대로 붙는다
   (config 상 23개를 뺐는데 렌더된 필드는 77개였다). sidebar_groups 는 **그룹핑·순서**만
   통제한다. **실제 제거는 뷰의 `exclude_fields` 뿐이다** (77 → 56 확인).
2. `metadata` / `id` / `filepath` / `created_at` / `last_modified_at` 는 FiftyOne 기본
   필드라 `exclude_fields` 가 거부한다(ValueError) — 사이드바에서 절대 없어지지 않는다.

그래서 이 모듈은 **둘 다** 한다: 그룹으로 정리하고(순서·접힘), 노이즈는 `exclude_fields`
저장뷰(`00_analysis`)로 걷어낸다. 사용자는 그 뷰를 진입점으로 쓴다.

판정 규칙 (분석에 쓸모 있는가 = 필터/패싯이 만들어지는가):
  · 전부 null                          → noise
  · 고유값 1 (상수)                     → noise  (필터가 생성되지 않는다)
  · float 리스트 (임베딩)                → noise  (1024-d 를 사이드바에 둘 이유가 없다)
  · 문자열이고 고유값 비율 > 0.5 이며 경로/ID 형태 → noise
  · 고유값 ≤ CARD_LOW                   → filter (범주형 패싯)
  · 수치형                              → filter (범위 슬라이더). 고유값 많으면 binning 제안
  · 그 외 고카디널리티 텍스트             → detail (필터 무의미, 모달에서 읽음)

사용:
    import fiftyone_presentation as fp
    fp.apply(ds)                       # 기본 dry-run — 판정 결과만 출력
    fp.apply(ds, dry_run=False)        # 실제 적용
    fp.apply(ds, dry_run=False, overrides={"noise": ["caption_en"], "filter": ["project"]})

파이프라인 자동화: 데이터셋 빌더 마지막에 `fp.apply(ds, dry_run=False)` 를 호출한다.
멱등이라 매 빌드/리프레시마다 다시 불러도 안전하다.
현재 배선: `fiftyone_full_build.py` (frames_captions 전체 빌드) 끝.

**손으로 짠 버전과의 관계**: `prompt_eval.py` 는 자체 `_configure_sidebar`/`_save_workspace`
를 갖고 있다. 그쪽은 도메인 지식이 들어간 그룹명("⑥ 원점수 (버전간 직접비교 금지)" 처럼
**왜 접었는지**가 이름에 담긴)을 쓰므로 자동 판정으로 대체하지 않는다. 이 모듈은 도메인
지식이 없는 임의 데이터셋용 기본값이다.

⚠️ 샘플/필드를 삭제하지 않는다. 건드리는 것은 `app_config`(사이드바 그룹/활성필드),
저장뷰 1개, 워크스페이스뿐이며 모두 되돌릴 수 있다.
"""

from __future__ import annotations

import os

# FiftyOne 기본 필드 — exclude_fields 가 거부한다. 사이드바에서 없앨 수 없으므로 맨 끝 그룹으로.
UNREMOVABLE = ("id", "filepath", "created_at", "last_modified_at", "metadata", "tags")

CARD_LOW = 100  # 이하면 범주형 패싯으로 쓸 수 있다고 본다 (프로젝트명·클러스터 ID 포함)
HIGH_CARD_STR_RATIO = 0.5  # 문자열 고유값 비율이 이보다 크면 ID/경로로 간주
PROFILE_SAMPLE = 5000  # 카디널리티 추정 표본 (20만 샘플 전량 조회 회피)
BIN_SUGGEST_CARD = 200  # 수치형 고유값이 이보다 많으면 binning 을 제안


def _looks_like_key(name: str, values: list) -> bool:
    """경로·ID·해시처럼 보이는가 (패싯 가치 없음)."""
    if any(k in name.lower() for k in ("_id", "id", "key", "path", "checksum", "sha", "url")):
        return True
    strs = [v for v in values if isinstance(v, str)][:50]
    if strs and sum(1 for v in strs if v.startswith("/") or v.startswith("s3://")) > len(strs) / 2:
        return True
    return False


def profile(ds, sample_n: int = PROFILE_SAMPLE, seed: int = 51) -> dict[str, dict]:
    """필드별 카디널리티/null 실측. 표본 기반이라 20만 샘플에서도 가볍다.

    ⚠️ **`limit` 을 쓰면 안 된다** — 앞쪽 N개만 보므로, 특정 프로젝트 구간에서만 null 인
    필드가 '상수'로 오판된다(실측: frames_captions 에서 daynight/environment/modality/
    caption 이 전부 noise 로 잘못 분류됐다). 반드시 무작위 표본(`take`)을 쓴다.
    """
    view = ds.take(sample_n, seed=seed) if len(ds) > sample_n else ds
    # 사이드바는 Label 필드를 부모 경로로 묶는다 → 최상위 경로만 판정 (metadata.* 는 default)
    schema = {p: f for p, f in ds.get_field_schema(flat=True).items() if "." not in p}
    out: dict[str, dict] = {}
    for path, field in schema.items():
        try:
            vals = view.values(path)
        except Exception:  # noqa: BLE001 — 조회 불가 필드는 판정 대상에서 제외
            continue
        flat = []
        for v in vals:
            if isinstance(v, list):
                flat.extend(v)
            else:
                flat.append(v)
        nn = [v for v in flat if v is not None]
        # 임베딩 판정: 첫 값이 None 일 수 있으므로 **첫 non-None 리스트**를 찾아서 본다
        first_list = next((v for v in vals if isinstance(v, list) and v), None)
        is_vec = (
            first_list is not None
            and len(first_list) > 16
            and all(isinstance(x, (int, float)) for x in first_list[:3])
        )
        try:
            uniq = len({str(v) for v in nn})
        except Exception:  # noqa: BLE001
            uniq = -1
        tname = type(field).__name__
        rec = {
            "type": tname,
            "n": len(flat),
            "nulls": len(flat) - len(nn),
            "uniq": uniq,
            "is_vector": is_vec,
            "numeric": tname in ("IntField", "FloatField"),
            "sample": nn[:3],
            "exact": False,
        }
        # 표본에서 상수/전무처럼 보이면 **전수 확인**한다. 표본이 특정 구간에 몰려 있으면
        # 실제로는 값이 있는 유용한 필터를 noise 로 버리게 된다(실측: daynight/environment).
        # 저카디널리티 후보에만 count_values 를 쓰므로 20만 샘플에서도 가볍다.
        if uniq <= 1 and not is_vec and tname in ("StringField", "BooleanField", "IntField"):
            try:
                cv = ds.count_values(path)
                rec["uniq"] = len([k for k in cv if k is not None])
                rec["exact"] = True
                rec["sample"] = [k for k in cv if k is not None][:3]
            except Exception:  # noqa: BLE001
                pass
        out[path] = rec
    return out


def classify(prof: dict[str, dict], overrides: dict | None = None) -> dict[str, str]:
    """path → 'filter' | 'detail' | 'noise' | 'default'."""
    overrides = overrides or {}
    forced = {p: role for role in ("filter", "detail", "noise") for p in overrides.get(role, [])}
    roles: dict[str, str] = {}
    for path, s in prof.items():
        if path in forced:
            roles[path] = forced[path]
            continue
        if path in UNREMOVABLE or path.startswith("metadata."):
            roles[path] = "default"
        elif s["is_vector"]:
            roles[path] = "noise"
        elif "Embedded" in s["type"]:
            # Label 필드(Classification/Detections/…) — App 이 .label/.confidence 로 필터한다
            roles[path] = "filter"
        elif s["nulls"] == s["n"]:
            roles[path] = "noise"
        elif s["uniq"] <= 1:
            roles[path] = "noise"
        elif s["numeric"]:
            roles[path] = "filter"
        elif s["uniq"] <= CARD_LOW:
            roles[path] = "filter"
        elif _looks_like_key(path, s["sample"]) or s["uniq"] / max(1, s["n"] - s["nulls"]) > HIGH_CARD_STR_RATIO:
            roles[path] = "noise" if _looks_like_key(path, s["sample"]) else "detail"
        else:
            roles[path] = "detail"
    return roles


def apply(  # noqa: C901
    ds,
    *,
    analysis_view: str = "00_analysis",
    overrides: dict | None = None,
    workspaces: list[tuple[str, str, str]] | None = None,
    dry_run: bool = True,
    verbose: bool = True,
) -> dict:
    """사이드바 그룹 + 노이즈 제외 저장뷰(+워크스페이스)를 적용. 멱등.

    ⚠️ 샘플/필드를 **절대 삭제하지 않는다** — app_config, 저장뷰, 워크스페이스만 건드린다.
    """
    import fiftyone as fo

    prof = profile(ds)
    roles = classify(prof, overrides)

    filt = [p for p, r in roles.items() if r == "filter"]
    detail = [p for p, r in roles.items() if r == "detail"]
    noise = [p for p, r in roles.items() if r == "noise"]
    default = [p for p, r in roles.items() if r == "default"]

    # 라벨 필드는 항상 필터 쪽 앞에 (Classification/Detections 등)
    label_like = [p for p in filt + detail if "Embedded" in prof[p]["type"] and "." not in p]
    filt = label_like + [p for p in filt if p not in label_like]
    detail = [p for p in detail if p not in label_like]

    # 수치형 중 고유값이 많은 것 = binning 후보 (연속값은 App 이 카테고리 색상을 못 만든다)
    bin_candidates = [
        p for p in filt if prof[p]["numeric"] and prof[p]["uniq"] > BIN_SUGGEST_CARD
    ]

    if verbose:
        print(f"[{ds.name}] {len(ds)} 샘플 / 판정 대상 {len(prof)} 경로")
        print(f"  filter {len(filt)} / detail {len(detail)} / noise {len(noise)} "
              f"/ default(제거불가) {len(default)}")
        print(f"  noise → 제외: {', '.join(sorted(noise)) or '없음'}")
        if bin_candidates:
            print(f"  ⚠️ 연속 수치(색칠 불가, binning 권장): {', '.join(bin_candidates)}")

    if dry_run:
        if verbose:
            print("  dry-run — 아무것도 적용하지 않았다. dry_run=False 로 실제 적용.")
        return {"filter": filt, "detail": detail, "noise": noise, "default": default,
                "bin_candidates": bin_candidates, "applied": False}

    # ── 사이드바 그룹 (순서·접힘 통제) ──
    defaults = fo.DatasetAppConfig.default_sidebar_groups(ds)
    G = type(defaults[0])
    groups = []
    for g in defaults:  # tags 는 맨 위
        if g.name in ("tags", "label tags"):
            groups.append(g)
    if filt:
        groups.append(G(name="① 분석 (필터)", paths=filt, expanded=True))
    if detail:
        groups.append(G(name="② 상세 (읽기용)", paths=detail, expanded=False))
    if noise:
        # exclude 뷰를 안 쓰는 사람에게도 최소한 접혀 보이도록 그룹은 만들어 둔다
        groups.append(G(name="③ 분석 무관", paths=noise, expanded=False))
    for g in defaults:  # metadata 는 이름을 정확히 유지해야 중복 삽입되지 않는다
        if g.name == "metadata":
            groups.append(G(name="metadata", paths=g.paths, expanded=False))
    ds.app_config.sidebar_groups = groups
    ds.save()

    # ── 노이즈 제외 저장뷰: 사이드바에서 **실제로** 없애는 유일한 수단 ──
    excludable = []
    for p in noise:
        if p in UNREMOVABLE or "." in p:
            continue
        try:
            ds.exclude_fields([p]).first()
            excludable.append(p)
        except Exception:  # noqa: BLE001 — 기본 필드 등 제외 거부되는 것은 건너뛴다
            pass
    if excludable:
        view = ds.exclude_fields(excludable)
        if analysis_view in ds.list_saved_views():
            ds.delete_saved_view(analysis_view)
        ds.save_view(analysis_view, view)

    # ── 워크스페이스 (Samples ↔ Embeddings 좌우 분할) ──
    made_ws = []
    for name, brain, color_by in workspaces or []:
        if brain not in ds.list_brain_runs():
            continue
        try:
            space = fo.Space(
                children=[
                    fo.Space(children=[fo.Panel(type="Samples", pinned=True)]),
                    fo.Space(children=[fo.Panel(
                        type="Embeddings",
                        state={"brainResult": brain, "colorByField": color_by},
                    )]),
                ],
                orientation="horizontal",
            )
            if name in ds.list_workspaces():
                ds.delete_workspace(name)
            ds.save_workspace(name, space, description=f"Samples ↔ {brain} (색: {color_by})")
            made_ws.append(name)
        except Exception as exc:  # noqa: BLE001
            if verbose:
                print(f"  워크스페이스 '{name}' 실패: {type(exc).__name__}: {exc}")

    if verbose:
        print(f"  적용: 그룹 {len(groups)}개 / 저장뷰 '{analysis_view}' "
              f"({len(excludable)}필드 제외) / 워크스페이스 {made_ws or '없음'}")
        print(f"  → 분석은 '{analysis_view}' 뷰를 선택해서 시작할 것 "
              f"(그룹만으로는 필드가 숨겨지지 않는다)")

    return {"filter": filt, "detail": detail, "noise": noise, "default": default,
            "bin_candidates": bin_candidates, "excluded": excludable,
            "workspaces": made_ws, "applied": True}


def add_bin_field(ds, src: str, dst: str, width: float, *, unit: str = "") -> int:
    """연속 수치 필드를 구간 Classification 으로 묶는다 (색칠·층화용).

    App 은 고유값이 많은 float 을 카테고리 색상으로 못 만든다(실측: 고유값 628개 → 색 없음).
    구간 폭은 **측정 노이즈보다 크게** 잡아야 의미가 있다.
    """
    import fiftyone as fo

    vals = ds.values(src)
    updates = {}
    for sid, v in zip(ds.values("id"), vals):
        if v is None:
            label = "unknown"
        else:
            lo = int(float(v) // width) * width
            label = f"{lo:g}-{lo + width:g}{unit}"
        updates[sid] = fo.Classification(label=label)
    ds.set_values(dst, updates, key_field="id")
    ds.save()
    return len(updates)


if __name__ == "__main__":
    import sys

    import fiftyone as fo

    name = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("FO_DATASET", "frames_captions")
    apply(fo.load_dataset(name), dry_run="--apply" not in sys.argv)
