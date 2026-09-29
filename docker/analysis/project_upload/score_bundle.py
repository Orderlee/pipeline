"""업로드 번들 채점 — GT-free 필드는 항상, GT 필드는 GT 모드에서만. 정본 스펙: UPLOAD_SPEC.md §3.

역할 분담: `ingest_bundle.py` 가 데이터셋 2개(`<dataset>` / `<dataset>-prompts`)를 만들고
emb_viz 브레인·`upload_key`/`gidx`/`nearest_key`/`text`/`category`/`bank_version`/
`sentence_embedding` 등 **뼈대 필드**를 채운 뒤 이 파일의 `run_scoring()` 을 호출한다.
이 파일은 그 위에 **채점 파생 필드**(pred/margin/winner_gidx/wins/adopted/purity/…)만
얹는다 — 벡터는 항상 npz 에서 다시 읽는다(FiftyOne 임베딩 필드를 되읽지 않는다).

`--attach` 로 재실행하면 GT 를 나중에 추가했거나(`gt.csv` 를 뒤늦게 넣은 경우) 다른 버전을
attach 하고 싶을 때 채점만 다시 돌릴 수 있다 (프레임 정렬·npz 로딩부터 매번 재계산 —
캐시하지 않는다. 번들 규모가 대형이면 청크 유사도 계산이 지배 비용이라 재계산 비용은
어차피 O(N×M) 이고 정합성이 캐시 부정합보다 중요하다).

정본 참조: `prompt_geometry.stage_attach`(프레임 채점 필드) + `stage_promptmap`/
`refresh_sentence_metrics.py`(문장 채점 필드 wins/purity/nearest_gt/match) 의 미러.
"""
from __future__ import annotations

import argparse
import collections
import os
import sys
import time

import numpy as np

import bundle_common


# ── 순수 수학 (FiftyOne 비의존 — selftest 대상) ─────────────────────────────

def score_version(frame_vec: np.ndarray, sent_vec: np.ndarray, sent_cls, classes: list) -> dict:
    """한 버전(뱅크) 채점의 핵심 수학. FiftyOne 을 몰라도 되게 분리해 selftest 로 검증한다.

    frame_vec [Nf,D] · sent_vec [Ns,D] 는 L2 정규화 가정 (bundle_common 로더가 이미 재정규화
    해서 넘긴다). sent_cls 는 그 버전 문장들의 class 라벨(문자열/정수 등 hashable, sent_vec 과
    같은 순서·같은 길이). classes 는 그 버전에 등장하는 class 오름차순 리스트.

    반환 dict (전부 프레임 축 [Nf]):
      M            [Nf, C] float32 — chunked_group_max 의 클래스별 최고 코사인 (attach 의
                   cos_best_* 가 이 열을 그대로 쓴다)
      pred         classes 원소 dtype — argmax_k1 예측 클래스 (= classes[M.argmax(1)])
      best         float32 — top1 코사인 (= M.max(1))
      margin       float32 — top1−top2. **클래스 1개면 top2 가 존재하지 않아 정의상
                   best cos 를 그대로 margin 으로 쓴다** (호출자가 리포트에 명시)
      winner_local int64  — 승자 문장의 **버전-로컬** 인덱스 (sent_vec/classes 의 행 번호,
                   = argbest[i, pred_col[i]] — 전역 argmax 와 동치, prompt_geometry 정본 주석)
    """
    n_groups = len(classes)
    cls_to_g = {c: i for i, c in enumerate(classes)}
    groups = np.array([cls_to_g[c] for c in sent_cls], dtype=np.int64)
    M, argbest = bundle_common.chunked_group_max(frame_vec, sent_vec, groups, n_groups)
    pred_col = M.argmax(axis=1)
    rows_idx = np.arange(len(pred_col))
    best = M[rows_idx, pred_col].astype(np.float32)
    if n_groups == 1:
        margin = best.copy()
    else:
        order = np.sort(M, axis=1)
        margin = (order[:, -1] - order[:, -2]).astype(np.float32)
    classes_arr = np.array(classes, dtype=object)
    pred = classes_arr[pred_col]
    winner_local = argbest[rows_idx, pred_col]
    return {"M": M, "pred": pred, "best": best, "margin": margin, "winner_local": winner_local}


def _invariant(cond: bool, msg: str) -> None:
    """UPLOAD_SPEC §3.3 불변식 — 위반 시 즉시 SystemExit (fail-closed)."""
    if not cond:
        raise SystemExit(f"[불변식 위반] {msg}")


# ── FiftyOne 채점 진입점 ─────────────────────────────────────────────────

def run_scoring(dataset_name: str, bundle_dir: str, attach: str | None = None) -> dict:
    """`<dataset_name>`/`<dataset_name>-prompts` 에 채점 필드를 얹는다. ingest 가 호출하는 진입점.

    `attach` 는 CLI `--attach`/ingest `--attach` 전용 확장 인자(기본 None → CSV 첫 버전) —
    ingest_bundle.py 가 `run_scoring(name, bundle_dir)` 또는 `run_scoring(name, bundle_dir,
    attach=args.attach)` 로 호출하므로 이름을 `attach` 로 맞춘다 (계약: 두 필수 위치인자 +
    선택 키워드 하나).
    """
    import fiftyone as fo

    manifest = bundle_common.load_manifest(bundle_dir)
    dim = manifest["embedding_dim"]
    prompts_name = dataset_name + bundle_common.PROMPTS_SUFFIX
    print(f"[load] manifest: model_name={manifest['model_name']!r} embedding_dim={dim} "
          f"gt_mode(manifest)={manifest['gt_mode']!r}")

    existing = set(fo.list_datasets())
    if dataset_name not in existing or prompts_name not in existing:
        raise bundle_common.BundleError(
            f"데이터셋 쌍이 없음: {dataset_name!r}({dataset_name in existing}) / "
            f"{prompts_name!r}({prompts_name in existing}) — ingest_bundle.py 먼저 실행")
    ds = fo.load_dataset(dataset_name)
    pds = fo.load_dataset(prompts_name)
    for d, nm in ((ds, dataset_name), (pds, prompts_name)):
        marker = (d.info or {}).get("upload_kit")
        if marker is None:
            raise bundle_common.BundleError(f"{nm}: info['upload_kit'] marker 없음 — 업로드 킷 산출물이 아님")
        # marker 는 항상 dict 여야 함 — 손상/구형 marker 면 아래에서 .get()/[] 이 TypeError/
        # AttributeError 로 새나가기 전에 fail-closed 한국어 오류로 변환 (codex 리뷰 지적)
        if not isinstance(marker, dict):
            raise bundle_common.BundleError(
                f"{nm}: upload_kit marker 형식 불량({type(marker).__name__}) — --overwrite 재인제스트 필요")
    print(f"[load] 데이터셋 로드 완료: {dataset_name}({ds.count():,}장) / {prompts_name}({pds.count():,}행)")

    # 1) npz 로드 — 벡터는 항상 여기서만 읽는다 (FiftyOne embedding 필드 되읽기 금지)
    keys, frame_vec = bundle_common.load_image_npz(bundle_dir, dim)
    rows, sent_vec, versions = bundle_common.load_prompts(bundle_dir, dim)
    n_frames = len(keys)
    print(f"[load] npz: 프레임 {n_frames:,}개 · 문장 {len(rows):,}행 · 버전 {len(versions)}종 {versions}")

    attach_version = attach or versions[0]
    if attach_version not in versions:
        raise bundle_common.BundleError(f"--attach {attach_version!r} 가 prompts.csv 버전 목록에 없음: {versions}")

    # 번들 지문 대조 — 같은 행수의 몰래 교체/재정렬(모든 개수 불변식이 통과하는 조용한 오귀속
    # 경로)을 어떤 쓰기도 하기 전에 차단한다. gt.csv 는 지문에 안 들어가므로 GT 나중 추가는 통과.
    fp = bundle_common.bundle_fingerprint(keys, frame_vec, rows, sent_vec)
    stored_fp = (ds.info.get("upload_kit") or {}).get("fingerprint")
    if stored_fp is None:
        raise bundle_common.BundleError(
            f"{dataset_name}: marker 에 번들 지문이 없음 (구버전 ingest 산출물) — --overwrite 재인제스트 필요")
    if stored_fp != fp:
        raise bundle_common.BundleError(
            f"{dataset_name}: 번들 지문 불일치 (ingest {stored_fp} ≠ 현재 {fp}) — prompts.csv/"
            f"임베딩 npz 가 ingest 이후 변경·재정렬·교체됨. 재채점은 GT(gt.csv) 변경만 허용 — "
            f"이미지/문장 변경 반영은 ingest_bundle.py --overwrite 로 재인제스트")
    print(f"[align] 번들 지문 일치: {fp}")

    # 2) GT 모드 해석 (재실행마다 다시 — GT 나중 추가 지원)
    gt_mode = bundle_common.resolve_gt_mode(bundle_dir, manifest)
    gt_map, has_camera = bundle_common.load_gt(bundle_dir, keys, gt_mode)
    print(f"[gt] gt_mode={gt_mode} · GT 보유 {len(gt_map):,}/{n_frames:,}장 · camera 열={has_camera}")

    # 3) 프레임 정렬 — 이 파일에서 제일 중요한 계약: npz key 순서 == view 순서 == 이후 모든
    #    set_values 리스트 순서. 어긋나면 전 필드가 조용히 오답이 된다.
    # upload_key/id 를 한 번의 원자 조회로 뽑는다 — 별도 두 values() 호출을 zip 하면 공유
    # FiftyOne 세션(이 호스트는 멀티테넌트) 위에서 그 사이 쓰기가 끼어들 때 조용한 전량
    # 오정렬로 이어질 수 있다.
    ukeys, uids = ds.values(["upload_key", "id"])
    key_to_id = dict(zip(ukeys, uids))
    key_to_idx = {k: i for i, k in enumerate(keys)}
    missing = [k for k in keys if k not in key_to_id]
    if missing:
        raise bundle_common.BundleError(
            f"{dataset_name}: npz key {len(missing)}개가 upload_key 에 없음 (예: {missing[:3]})")
    _invariant(
        ds.count() == n_frames,
        f"{dataset_name}: 데이터셋 샘플수 {ds.count():,} ≠ npz 프레임수 {n_frames:,} "
        "(재채점 대상 불일치 — npz 가 ingest 이후 축소됐을 수 있음)",
    )
    ordered_ids = [key_to_id[k] for k in keys]
    view = ds.select(ordered_ids, ordered=True)
    _invariant(view.count() == n_frames, f"프레임 view {view.count()} ≠ npz 프레임 {n_frames}")
    print(f"[align] 프레임 view 정렬 완료 {n_frames:,}장 (npz 순서)")

    # GT 갱신 (csv/folders 일 때만 ground_truth/camera 를 다시 씀 — GT 재적재 재실행 지원)
    if gt_mode != "none":
        gt_vals = [fo.Classification(label=gt_map[k]["class"]) if k in gt_map else None for k in keys]
        _invariant(len(gt_vals) == n_frames, "ground_truth 값 리스트 길이 ≠ 프레임수")
        view.set_values("ground_truth", gt_vals)
        n_gt = sum(1 for v in gt_vals if v is not None)
        if has_camera:
            cam_vals = [gt_map[k]["camera"] if k in gt_map else None for k in keys]
            view.set_values("camera", cam_vals)
            print(f"[gt] ground_truth 갱신 {n_gt:,}/{n_frames:,} · camera 갱신")
        else:
            print(f"[gt] ground_truth 갱신 {n_gt:,}/{n_frames:,}")
    else:
        print("[gt] gt_mode=none — ground_truth/camera 갱신 생략")

    # gt.csv 를 나중에 지운 재채점(csv→none) — UPLOAD_SPEC.md §6 이 명시하듯 GT 파생 필드는
    # 지우지 않는다(완전 제거는 --overwrite 재인제스트 전용). 다만 marker(gt_mode="none")와
    # 실제 남은 필드가 조용히 모순되는 상태를 리포트/로그에서라도 보이게 한다 (major 리뷰 지적).
    stale_gt_fields: list[str] = []
    if gt_mode == "none":
        stale_gt_fields = [f for f in ("ground_truth", "camera") if f in ds.get_field_schema()]
        stale_gt_fields += [
            f"{prompts_name}.{f}" for f in ("purity", "purity_tier", "nearest_gt", "match", "n_cameras")
            if f in pds.get_field_schema()
        ]
        if stale_gt_fields:
            print(f"[gt] 경고: gt_mode=none 인데 이전 GT 채점 필드가 남아있음 {stale_gt_fields} — "
                  "값은 갱신되지 않은 옛 GT 기준(§6) — 완전 제거는 ingest_bundle.py --overwrite 재인제스트")

    # marker 존재는 위에서 이미 확인했다 — gt_mode 만 최신값으로 덮어쓴다 (GT 나중 추가 재실행 지원)
    # ds/pds 는 ingest_bundle.py 가 한 쌍으로 만든 marker 를 공유하므로 둘 다 갱신한다
    # (예전엔 ds 만 갱신해 -prompts 쪽 marker 의 gt_mode 가 ingest 시점 값에 영구히 고정됐다).
    # scoring 서브필드는 버전 루프 진입 **전** state="running" 으로 찍는다 — 버전 3/5 에서 죽으면
    # (OOM/kill 등) 루프 아래 set_values 들이 절반만 적용된 채 남는데, 그 상태에서 marker 를 안
    # 갱신하면 "gt_mode 갱신됨 + 옛 성공 scoring_report.json" 이라는 정상처럼 보이는 조합만 남는다
    # (critical 리뷰 지적). state="ok" 는 루프가 전량 성공한 뒤에만 덮어쓴다 — 중간 실패는 marker
    # 가 running 인 채로 영구히 남아 "부분 채점됨, 재채점 필요"를 marker 만으로 판정 가능해진다.
    scoring_running = {"state": "running", "started_at": time.time(), "attach": attach_version}
    ds.info["upload_kit"]["gt_mode"] = gt_mode
    ds.info["upload_kit"]["scoring"] = scoring_running
    ds.save()
    pds.info["upload_kit"]["gt_mode"] = gt_mode
    pds.info["upload_kit"]["scoring"] = scoring_running
    pds.save()

    _invariant(
        pds.count() == len(rows),
        f"{prompts_name}: 문장 샘플수 {pds.count():,} ≠ prompts.csv 행수 {len(rows):,} "
        "(재채점 대상 불일치 — prompts.csv 가 ingest 이후 변경됐을 수 있음)",
    )

    version_reports: list[dict] = []

    for b, v in enumerate(versions):
        idxs = [i for i, r in enumerate(rows) if r["version"] == v]
        idxs = np.asarray(idxs, dtype=np.int64)
        sent_vec_v = sent_vec[idxs]
        cls_v = [rows[int(i)]["class"] for i in idxs]
        texts_v = [rows[int(i)]["text"] for i in idxs]
        classes = sorted(set(cls_v))
        ns_v = len(idxs)
        lo, hi = b * bundle_common.GIDX_OFFSET, b * bundle_common.GIDX_OFFSET + ns_v

        res = score_version(frame_vec, sent_vec_v, cls_v, classes)
        pred, best, margin, winner_local = res["pred"], res["best"], res["margin"], res["winner_local"]

        vt_tag, vtag_tag = bundle_common.vt(v), bundle_common.vtag(v)
        pred_field = f"pred_{vt_tag}"
        top_prompt_field = f"top_prompt_{vt_tag}"
        margin_field = f"pred_margin_{vtag_tag}"
        gidx_field = f"winner_gidx_{vtag_tag}"
        correct_field = f"pred_correct_{vtag_tag}"

        gidx_vals = [int(lo + int(winner_local[i])) for i in range(n_frames)]
        _invariant(all(lo <= g < hi for g in gidx_vals),
                   f"{v}: winner_gidx 값이 gidx 블록 [{lo},{hi}) 밖")

        pred_vals = [fo.Classification(label=str(pred[i]), confidence=float(best[i])) for i in range(n_frames)]
        top_prompt_vals = [texts_v[int(winner_local[i])] for i in range(n_frames)]
        margin_vals = [float(margin[i]) for i in range(n_frames)]
        for vals in (pred_vals, top_prompt_vals, margin_vals, gidx_vals):
            _invariant(len(vals) == n_frames, f"{v}: set_values 값 리스트 길이 ≠ 프레임수")
        view.set_values(pred_field, pred_vals)
        view.set_values(top_prompt_field, top_prompt_vals)
        view.set_values(margin_field, margin_vals)
        view.set_values(gidx_field, gidx_vals)
        print(f"[score] {v}(block {b}): 문장 {ns_v:,} · 클래스 {classes} · "
              f"{pred_field}/{top_prompt_field}/{margin_field}/{gidx_field} 기록 {n_frames:,}장")

        n_correct_set = 0
        if gt_mode != "none":
            correct_vals = []
            for i in range(n_frames):
                g = gt_map.get(keys[i])
                if g is None:
                    correct_vals.append(None)
                else:
                    correct_vals.append(fo.Classification(label="correct" if str(pred[i]) == g["class"] else "wrong"))
                    n_correct_set += 1
            _invariant(len(correct_vals) == n_frames, f"{v}: pred_correct 값 리스트 길이 ≠ 프레임수")
            view.set_values(correct_field, correct_vals)
            print(f"[score] {v}: {correct_field} 기록 {n_correct_set:,}/{n_frames:,}장 (GT 보유분만)")

        # ── attach (기본 = CSV 첫 버전) ──
        if v == attach_version:
            for j, c in enumerate(classes):
                cos_field = f"cos_best_{bundle_common.class_suffix(c)}"
                cos_vals = [float(res["M"][i, j]) for i in range(n_frames)]
                _invariant(len(cos_vals) == n_frames, f"{v}: {cos_field} 값 리스트 길이 ≠ 프레임수")
                view.set_values(cos_field, cos_vals)
            attached_vals = [fo.Classification(label=v) for _ in range(n_frames)]
            view.set_values("attached_bank", attached_vals)
            print(f"[attach] {v}: cos_best_* {len(classes)}개 필드 + attached_bank 기록 ({n_frames:,}장)")

        # ── 문장 쪽 (그 버전 -prompts 샘플, bank_version.label 로 골라 gidx 로 정렬 매칭) ──
        # gidx 숫자 범위 [lo,hi) 만으로 매칭하면, 재채점 시점에 prompts.csv 의 버전 등장순이
        # ingest 시점과 달라져도(문장수는 동일) 오류 없이 통과해 버전이 뒤바뀐 채 채점될 수
        # 있다(major 리뷰 지적) — 그래서 gidx 범위 대신 bank_version.label==v 로 먼저 문장
        # 데이터셋 쪽을 좁힌 뒤, gidx % GIDX_OFFSET(블록 위치 무관 로컬 인덱스)로 정렬한다.
        pds_v = pds.match(fo.ViewField("bank_version.label") == v)
        vgidx, vpid = pds_v.values(["gidx", "id"])
        block_map: dict[int, str] = {}
        for g, pid in zip(vgidx, vpid):
            if g is None:
                continue
            local = int(g) % bundle_common.GIDX_OFFSET
            if local in block_map:
                raise bundle_common.BundleError(
                    f"{prompts_name}: 버전 {v!r} 안에서 local gidx {local} 중복 — 문장 뼈대 오염")
            block_map[local] = pid
        missing_local = [j for j in range(ns_v) if j not in block_map]
        if missing_local:
            raise bundle_common.BundleError(
                f"{prompts_name}: 버전 {v} bank_version={v!r} 문장 중 {len(missing_local)}개 슬롯이 없음 "
                f"(예: {missing_local[:5]}) — ingest_bundle.py 가 먼저 -prompts 뼈대를 만들어야 함")
        extra_local = sorted(k for k in block_map if k >= ns_v)
        if extra_local:
            raise bundle_common.BundleError(
                f"{prompts_name}: 버전 {v!r} 문장수 {ns_v} 인데 local gidx {extra_local[:5]} 초과분 존재 "
                "(재채점 시 prompts.csv 문장수가 ingest 시점보다 줄었을 수 있음)")
        ordered_pids = [block_map[j] for j in range(ns_v)]
        pview = pds.select(ordered_pids, ordered=True)
        _invariant(pview.count() == ns_v, f"{v}: 문장 view {pview.count()} ≠ 문장수 {ns_v}")

        wins = np.bincount(winner_local, minlength=ns_v).astype(np.int64)
        sum_wins = int(wins.sum())
        _invariant(sum_wins == n_frames, f"{v}: sum(wins)={sum_wins} ≠ 프레임수 {n_frames}")
        print(f"[score-sent] {v}: sum(wins)={sum_wins:,} (프레임수={n_frames:,})")

        wins_vals = [int(w) for w in wins]
        adopted_vals = [fo.Classification(label="채택" if w > 0 else "미채택") for w in wins]
        for vals in (wins_vals, adopted_vals):
            _invariant(len(vals) == ns_v, f"{v}: 문장 set_values 값 리스트 길이 ≠ 문장수")
        pview.set_values("wins", wins_vals)
        pview.set_values("adopted", adopted_vals)

        margin_is_best_cos = len(classes) == 1
        n_purity_set = 0
        if gt_mode != "none":
            won_frames: dict[int, list[int]] = collections.defaultdict(list)
            for i, g in enumerate(winner_local.tolist()):
                won_frames[g].append(i)

            purity_vals: list = []
            purity_tier_vals: list = []
            n_cam_vals: list = []
            for j in range(ns_v):
                fr = won_frames.get(j, [])
                gt_fr = [i for i in fr if keys[i] in gt_map]
                if gt_fr:
                    p = float(sum(1 for i in gt_fr if gt_map[keys[i]]["class"] == cls_v[j]) / len(gt_fr))
                    purity_vals.append(round(p, 4))
                    purity_tier_vals.append(fo.Classification(label=bundle_common.purity_bin(p)))
                    n_purity_set += 1
                else:
                    purity_vals.append(None)
                    purity_tier_vals.append(None)
                if has_camera and fr:
                    cams = {gt_map[keys[i]]["camera"] for i in fr
                            if keys[i] in gt_map and gt_map[keys[i]]["camera"]}
                    n_cam_vals.append(len(cams))
                else:
                    n_cam_vals.append(None)
            pview.set_values("purity", purity_vals)
            pview.set_values("purity_tier", purity_tier_vals)
            if has_camera:
                pview.set_values("n_cameras", n_cam_vals)

            # nearest_gt/match — 이미 저장된 nearest_key 필드로 최근접 프레임을 해석한다
            # (최근접 탐색 자체는 ingest_bundle.py 소관 — 여기서 다시 하지 않는다)
            nearest_keys_v = pview.values("nearest_key")
            _invariant(len(nearest_keys_v) == ns_v, f"{v}: nearest_key 값 리스트 길이 ≠ 문장수")
            nearest_gt_vals = []
            match_vals = []
            for j, nk in enumerate(nearest_keys_v):
                fi = key_to_idx.get(nk)
                if fi is None:
                    raise bundle_common.BundleError(
                        f"{prompts_name}: nearest_key {nk!r}({v}) 가 image_embeddings.npz key 집합 밖")
                cos_nj = float(np.dot(sent_vec_v[j], frame_vec[fi]))
                gtent = gt_map.get(nk)
                if gtent is None:
                    nearest_gt_vals.append(fo.Classification(label="no_gt", confidence=cos_nj))
                    match_vals.append(fo.Classification(label="no_gt"))
                else:
                    nearest_gt_vals.append(fo.Classification(label=gtent["class"], confidence=cos_nj))
                    match_vals.append(fo.Classification(
                        label="hit" if gtent["class"] == cls_v[j] else "miss"))
            pview.set_values("nearest_gt", nearest_gt_vals)
            pview.set_values("match", match_vals)
            print(f"[score-sent] {v}: purity {n_purity_set:,}/{ns_v:,}행(wins>0 & GT보유승자 존재) · "
                  f"nearest_gt/match {ns_v:,}행")
        else:
            print(f"[score-sent] {v}: wins/adopted {ns_v:,}행 (GT 없음 — purity/nearest_gt/match 생략)")

        version_reports.append({
            "version": v, "block": b, "n_sentences": ns_v, "classes": [str(c) for c in classes],
            "sum_wins": sum_wins, "gidx_range": [lo, hi], "margin_is_best_cos": margin_is_best_cos,
            "attached": v == attach_version, "n_pred_correct_set": n_correct_set,
        })

    # 버전 전량 성공 — marker 를 완료 상태로 덮어쓴다 (위 running marker 참고)
    scoring_done = {"state": "ok", "finished_at": time.time(), "attach": attach_version,
                     "versions": versions, "fingerprint": fp}
    ds.info["upload_kit"]["scoring"] = scoring_done
    ds.save()
    pds.info["upload_kit"]["scoring"] = scoring_done
    pds.save()

    report = {
        "bundle_dir": os.path.abspath(bundle_dir),
        "dataset": dataset_name,
        "prompts_dataset": prompts_name,
        "gt_mode": gt_mode,
        "has_camera": has_camera,
        "n_frames": n_frames,
        "n_sentences_total": len(rows),
        "attach_version": attach_version,
        # 여기 도달했다는 것 자체가 §3.3 의 3개 불변식(sum(wins)==프레임수 / winner_gidx 블록
        # 범위 / set_values 리스트 길이==프레임수, 전부 _invariant() 가드)을 통과했다는 뜻이다
        # — 위반 시 SystemExit 로 리포트 작성 전에 죽으므로 여기 값은 항상 True 다.
        "invariants_ok": True,
        "versions": version_reports,
        "stale_gt_fields": stale_gt_fields,
    }
    path = bundle_common.write_report(bundle_dir, "scoring_report.json", report)
    report["_report_path"] = path
    print(f"[report] {path}")
    return report


# ── 셀프테스트 (FiftyOne 없이 순수 numpy) ───────────────────────────────────

def _selftest() -> None:
    rng = np.random.default_rng(1)
    Nf, Ns, D = 20, 5, 6
    frame_vec = bundle_common.l2_normalize(rng.normal(size=(Nf, D)))
    sent_vec = bundle_common.l2_normalize(rng.normal(size=(Ns, D)))
    sent_cls = [0, 0, 1, 1, 1]                      # 클래스 2개 · 문장 5개
    classes = [0, 1]

    res = score_version(frame_vec, sent_vec, sent_cls, classes)
    S = frame_vec @ sent_vec.T                      # [Nf, Ns] 브루트포스 유사도

    for i in range(Nf):
        m_by_class, arg_by_class = [], []
        for c in classes:
            idx = np.flatnonzero(np.array(sent_cls) == c)
            j = idx[np.argmax(S[i, idx])]
            m_by_class.append(float(S[i, j]))
            arg_by_class.append(int(j))
        pred_col = int(np.argmax(m_by_class))
        best = m_by_class[pred_col]
        srt = sorted(m_by_class)
        margin = srt[-1] - srt[-2]
        assert res["pred"][i] == classes[pred_col]
        assert abs(float(res["best"][i]) - best) < 1e-5
        assert abs(float(res["margin"][i]) - margin) < 1e-5
        assert int(res["winner_local"][i]) == arg_by_class[pred_col]

    wins = np.bincount(res["winner_local"], minlength=Ns)
    assert int(wins.sum()) == Nf, "sum(wins) == 프레임수 불변식"

    # C==1 특수 케이스 — margin 정의상 best cos 와 동일
    res1 = score_version(frame_vec, sent_vec, [0, 0, 0, 0, 0], [0])
    assert np.allclose(res1["margin"], res1["best"]), "클래스 1개면 margin=best cos"
    assert res1["M"].shape == (Nf, 1)

    print("score_bundle selftest OK")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="업로드 번들 채점 — GT-free 는 항상, GT 필드는 GT 모드에서만")
    ap.add_argument("bundle_dir", nargs="?", help="번들 디렉토리 (예: /data/fiftyone/uploads/<dataset>)")
    ap.add_argument("--name", help="데이터셋 이름 (기본 manifest.json 의 dataset)")
    ap.add_argument("--attach", help="cos_best_*/attached_bank 를 붙일 버전 (기본 = CSV 첫 버전)")
    ap.add_argument("--selftest", action="store_true", help="FiftyOne 없이 순수 numpy 채점 수학만 검증")
    args = ap.parse_args(argv)

    if args.selftest:
        _selftest()
        return 0
    if not args.bundle_dir:
        ap.error("bundle_dir 필수 (--selftest 아니면)")

    try:
        manifest = bundle_common.load_manifest(args.bundle_dir)
        name = args.name or manifest["dataset"]
        run_scoring(name, args.bundle_dir, attach=args.attach)
    except bundle_common.BundleError as e:
        print(f"[오류] {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
