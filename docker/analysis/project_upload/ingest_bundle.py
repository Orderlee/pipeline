"""업로드 번들 → FiftyOne 데이터셋 쌍(<name>, <name>-prompts) 생성 + emb_viz + compare 워크스페이스 + (기본) 채점 호출.

정본 계약: UPLOAD_SPEC.md + bundle_common.py — 포맷/필드/상수는 bundle_common 만 사용한다.

CLI:
    python3 ingest_bundle.py <bundle_dir> [--name X] [--overwrite] [--skip-viz]
        [--skip-scoring] [--attach VERSION] [--allow-external-media]

⚠️ 기존 FiftyOne 데이터셋(sourcei, frames 등)을 절대 열어서 수정/삭제하지 않는다 — §4 충돌 정책이
   `upload_kit` marker 없는 이름은 --overwrite 여도 무조건 거부한다 (이 스크립트에서 제일 중요한 규칙).
"""
from __future__ import annotations

import argparse
import os
import sys

import numpy as np

import bundle_common as bc

import fiftyone as fo
import fiftyone.brain as fob


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="업로드 번들 → FiftyOne 데이터셋 쌍 인제스트")
    p.add_argument("bundle_dir", help="번들 디렉토리 (manifest.json 이 있는 경로)")
    p.add_argument("--name", help="데이터셋 이름 (기본: manifest.json 의 dataset)")
    p.add_argument("--overwrite", action="store_true", help="동명 데이터셋 쌍 재생성 (upload_kit marker 필수)")
    p.add_argument("--skip-viz", action="store_true",
                   help="emb_viz(UMAP) 단계 생략 — compare 워크스페이스도 저장하지 않아 Samples 단독으로 열림")
    p.add_argument("--skip-scoring", action="store_true", help="score_bundle 채점 단계 생략")
    p.add_argument("--attach", metavar="VERSION", help="cos_best_<class>/attached_bank 를 이 버전 문장으로 계산")
    p.add_argument(
        "--allow-external-media", action="store_true",
        help="번들이 UPLOAD_ROOT 밖에 있어도 진행 (e2e 전용, 프로덕션 반입은 금지)",
    )
    return p.parse_args()


def _abs_image_path(real_bundle: str, key: str) -> str:
    """번들 실경로 기준 images/<key> 절대경로 (key 는 bundle_common 이 이미 안전성 검증함)."""
    return os.path.join(real_bundle, bc.IMAGES_DIR, key)


def compute_viz(ds: fo.Dataset, ids: list[str], vecs: np.ndarray) -> int:
    """emb_viz(2D) 계산+등록.

    N>=5: UMAP(cosine, seed=UMAP_SEED, n_neighbors=min(15,N-1)). N in [2,4]: PCA(2) 폴백.
    N==1: 원점 고정. 기존 brain run 이 있으면 먼저 지운다. 반환값 = 등록된 점 수.
    """
    n = len(ids)
    if n == 0:
        return 0
    if ds.has_brain_run(bc.BRAIN_KEY):
        ds.delete_brain_run(bc.BRAIN_KEY)
    if n == 1:
        pts = np.array([[0.0, 0.0]])
    elif n < 5:
        from sklearn.decomposition import PCA

        pts = PCA(n_components=2, random_state=bc.UMAP_SEED).fit_transform(vecs)
    else:
        import umap

        reducer = umap.UMAP(
            n_components=2, metric="cosine", random_state=bc.UMAP_SEED,
            low_memory=True, n_neighbors=min(15, n - 1),
        )
        pts = reducer.fit_transform(vecs)
    pts = np.asarray(pts, dtype=np.float64)
    fob.compute_visualization(ds.select(ids, ordered=True), points=pts, brain_key=bc.BRAIN_KEY)
    # UPLOAD_SPEC.md §3.3 불변식(emb_viz points 수 == 임베딩 보유 샘플 수)을 여기서 직접 확인한다
    # — 지금까진 e2e verify 만 이걸 봤고 킷 코드 자체엔 런타임 assert 가 없었다.
    registered = len(np.asarray(ds.load_brain_results(bc.BRAIN_KEY).current_points))
    if registered != n:
        raise bc.BundleError(f"emb_viz 등록 불일치: 계산 포인트 {n} vs 등록됨 {registered}")
    return n


def save_compare_workspaces(img_ds: fo.Dataset, prompt_ds: fo.Dataset) -> list[str]:
    """두 데이터셋에 'compare' 워크스페이스 저장 → 반환 = 저장된 데이터셋 이름.

    App 은 데이터셋을 열 때 항상 Samples 단독 레이아웃으로 시작하고, user-prompt-compare 의
    `user_default_workspace`(on_dataset_open) 가 **정확히 'compare' 라는 이름의 워크스페이스가
    있을 때만** 그걸로 뒤집는다. 예전엔 킷이 이 저장을 안 해서 새로 임포트한 프로젝트만
    Samples 화면으로 열렸다(2026-09-04 `source-n` 보고). 레이아웃 정본은 fiftyone_app_setup.
    _compare_space 하나 — 여기서 복제하지 않고 그대로 부른다. 프레임 데이터셋 쪽 구성이
    짝(`<name>-prompts`) 존재를 요구하므로 7) 문장 데이터셋 이후에 호출한다.
    """
    # fiftyone_app_setup 은 상위 디렉토리(/workspace) 모듈 — 사용 직전 임포트, 모듈 상단은 상수만이라 부작용 없음
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from fiftyone_app_setup import _compare_space

    saved: list[str] = []
    for ds in (img_ds, prompt_ds):
        space, desc = _compare_space(fo, ds)
        ds.save_workspace("compare", space, description=desc, overwrite=True)
        assert "compare" in ds.list_workspaces()
        saved.append(ds.name)
    return saved


def _check_conflict(name: str, name_prompts: str, overwrite: bool) -> None:
    """이름 충돌 정책 — upload_kit marker 없는 기존 자산은 --overwrite 여도 무조건 거부.

    (sourcei/frames 등 기존 자산 보호 — 이 스크립트에서 제일 중요한 규칙.)
    """
    existing = [n for n in (name, name_prompts) if fo.dataset_exists(n)]
    if not existing:
        return
    if not overwrite:
        raise bc.BundleError(f"데이터셋 이미 존재: {existing} — 재생성하려면 --overwrite 필요")
    for n in existing:
        ds = fo.load_dataset(n)
        marker = (ds.info or {}).get("upload_kit")
        if not marker:
            raise bc.BundleError(
                f"'{n}' 은 upload_kit marker 가 없는 기존 자산 — --overwrite 로도 삭제하지 않음 "
                f"(sourcei/frames 등 보호 대상일 수 있음, 다른 이름을 쓰거나 수동 확인 필요)"
            )
        # 삭제 전 저장된 뷰/워크스페이스 개수 경고 — fo.delete_dataset 은 이들을 백업 없이
        # 함께 지운다(비원자적, UPLOAD_SPEC.md §6). 복구는 하지 않지만 최소한 조용히 사라지지
        # 않게 개수를 남긴다 (major 리뷰 지적).
        n_views, n_ws = len(ds.list_saved_views()), len(ds.list_workspaces())
        if n_views or n_ws:
            print(f"[4/9] 경고: '{n}' 저장된 뷰 {n_views}개 · 워크스페이스 {n_ws}개가 함께 삭제됩니다(복구 불가)")
    for n in existing:
        fo.delete_dataset(n)
        print(f"[4/9] 기존 데이터셋 삭제(marker 확인됨): {n}")


def run(args: argparse.Namespace) -> int:
    bundle_dir = args.bundle_dir
    name: str | None = None
    partial_created = False
    stage = "0/9(초기화)"  # except 블록에서 "몇 단계에서 죽었는지" 를 보여주기 위한 진행 표시기
    try:
        # 1) 검증 (fail-closed) ------------------------------------------------
        stage = "1/9(검증)"
        print(f"[1/9] 번들 검증: {bundle_dir}")
        import validate_bundle  # 같은 디렉토리 모듈 — 사용 직전 임포트

        vresult = validate_bundle.validate(bundle_dir)
        if not isinstance(vresult, dict):
            raise RuntimeError(f"validate_bundle.validate() 반환 타입 이상(dict 기대): {type(vresult)!r}")
        errors = list(vresult.get("errors") or [])
        warn_msgs = list(vresult.get("warnings") or [])
        if errors:
            print(f"[1/9] 검증 실패 — 오류 {len(errors)}건")
            for e in errors:
                print(f"  - {e}")
            return 1
        print(f"[1/9] 검증 통과 (경고 {len(warn_msgs)}건)")
        for w in warn_msgs:
            print(f"  ! {w}")

        # 2) 미디어 위치 정책 ----------------------------------------------------
        stage = "2/9(미디어 위치)"
        real_bundle = os.path.realpath(bundle_dir)
        real_root = os.path.realpath(bc.UPLOAD_ROOT)
        under_root = real_bundle == real_root or real_bundle.startswith(real_root + os.sep)
        if not under_root and not args.allow_external_media:
            raise bc.BundleError(
                f"번들이 UPLOAD_ROOT({bc.UPLOAD_ROOT}) 밖에 있음: {real_bundle} — "
                f"의도된 경우(e2e 등) --allow-external-media 를 명시하세요"
            )
        print(f"[2/9] 미디어 위치 확인 완료: {real_bundle}")

        # 3) manifest + 이름 결정 ------------------------------------------------
        stage = "3/9(manifest/이름)"
        manifest = bc.load_manifest(bundle_dir)
        dim = int(manifest["embedding_dim"])
        if args.name:
            if not bc.DATASET_NAME_RE.match(args.name):
                raise bc.BundleError(f"--name 규칙 위반: {args.name!r}")
            if args.name.endswith(bc.PROMPTS_SUFFIX):
                raise bc.BundleError(f"--name 은 {bc.PROMPTS_SUFFIX!r} 로 끝날 수 없음(예약): {args.name!r}")
            name = args.name
        else:
            name = manifest["dataset"]
        name_prompts = name + bc.PROMPTS_SUFFIX
        print(f"[3/9] 데이터셋 이름: {name} / {name_prompts} (embedding_dim={dim})")

        # 4) 충돌 정책 ------------------------------------------------------------
        stage = "4/9(충돌 정책)"
        _check_conflict(name, name_prompts, args.overwrite)
        print("[4/9] 충돌 정책 확인 완료")

        # 5) 이미지 데이터셋 --------------------------------------------------------
        stage = "5/9(이미지 데이터셋)"
        keys, img_vec = bc.load_image_npz(bundle_dir, dim)  # 이미지 0장이면 bc 가 이미 fail-closed
        gt_mode = bc.resolve_gt_mode(bundle_dir, manifest)
        gt, has_camera = bc.load_gt(bundle_dir, keys, gt_mode)
        n_gt = len(gt)
        # 문장도 여기서 미리 로드 — 번들 지문(fingerprint) 이 이미지+문장 양쪽을 묶으므로
        # marker 를 찍기 전에 계산해야 한다 (재채점 시 score_bundle 이 이 지문을 대조한다).
        rows, prompt_vec, versions = bc.load_prompts(bundle_dir, dim)
        if args.attach and args.attach not in versions:
            raise bc.BundleError(f"--attach 버전 {args.attach!r} 이 prompts.csv 버전 목록 {versions} 안에 없음")
        fingerprint = bc.bundle_fingerprint(keys, img_vec, rows, prompt_vec)

        # 비영속으로 먼저 만들고 marker 를 찍은 뒤에야 persistent=True 로 전환한다 — 그 사이
        # (OOM kill 등 SIGKILL)에 죽으면 FiftyOne 이 비영속 데이터셋을 스스로 회수할 수 있는
        # 상태로 남는다. 처음부터 persistent=True 로 만들면 marker 저장 전에 죽었을 때 marker
        # 없는 *영속* 좀비가 남아 --overwrite 로도 삭제되지 않는다 (major 리뷰 지적).
        img_ds = fo.Dataset(name)
        # marker 는 샘플 적재 전에 먼저 찍는다 — 중간에 실패해도 --overwrite 재시도가 막히지 않게.
        img_ds.info["upload_kit"] = bc.dataset_marker(bundle_dir, gt_mode, fingerprint=fingerprint)
        img_ds.save()
        img_ds.persistent = True
        partial_created = True

        img_samples = []
        img_ids: list[str] = []
        for i, key in enumerate(keys):
            s = fo.Sample(filepath=_abs_image_path(real_bundle, key))
            s["upload_key"] = key
            s["embedding"] = img_vec[i].tolist()
            g = gt.get(key)
            if g is not None:
                s["ground_truth"] = fo.Classification(label=g["class"])
                if has_camera and g.get("camera"):
                    s["camera"] = str(g["camera"])
            img_samples.append(s)
            if len(img_samples) >= 1000:
                img_ids.extend(img_ds.add_samples(img_samples))
                print(f"[5/9] 이미지 적재 {len(img_ids)}/{len(keys)}")
                img_samples = []
        if img_samples:
            img_ids.extend(img_ds.add_samples(img_samples))
        print(f"[5/9] 이미지 {len(keys)}장 적재 완료 (GT={gt_mode}, GT보유 {n_gt}장, camera={has_camera})")

        # 6) 이미지 emb_viz -----------------------------------------------------
        stage = "6/9(이미지 emb_viz)"
        if args.skip_viz:
            n_img_viz = 0
            print("[6/9] --skip-viz: 이미지 emb_viz 생략")
        else:
            n_img_viz = compute_viz(img_ds, img_ids, img_vec)
            print(f"[6/9] 이미지 emb_viz {n_img_viz}점 등록")

        # 7) 문장 데이터셋 (+ 문장 emb_viz, 6과 동일 함수) ----------------------------
        stage = "7/9(문장 데이터셋)"
        # rows/prompt_vec/versions 는 5) 에서 지문 계산과 함께 이미 로드됨

        # 문장별 최근접 프레임 = 이미지 전체를 단일 그룹으로 둔 argmax 코사인 (유사도 행렬 비상주)
        # 여기선 Q=문장·R=프레임이라 기본 인자(q_batch=FRAME_BATCH,r_block=SENT_BLOCK)를 그대로
        # 쓰면 축이 뒤바뀐다 — "프레임 배치 1024 × 문장 블록 2048"(UPLOAD_SPEC §5, bank_top2_stream
        # 미러) 을 실제로 지키려면 R(프레임) 쪽에 FRAME_BATCH, Q(문장) 쪽에 SENT_BLOCK 을 명시해야 함.
        _cos_best, argbest = bc.chunked_group_max(
            Q=prompt_vec, R=img_vec, groups=np.zeros(len(keys), dtype=np.int64), n_groups=1,
            q_batch=bc.SENT_BLOCK, r_block=bc.FRAME_BATCH,
        )
        nearest_idx = argbest[:, 0]

        block_of = {v: b for b, v in enumerate(versions)}
        local_ctr: dict[str, int] = {}

        # img_ds 와 동일하게 marker 를 먼저 찍은 뒤 persistent 전환 (위 img_ds 주석 참고)
        prompt_ds = fo.Dataset(name_prompts)
        prompt_ds.info["upload_kit"] = bc.dataset_marker(bundle_dir, gt_mode, fingerprint=fingerprint)
        prompt_ds.save()
        prompt_ds.persistent = True

        prompt_samples = []
        prompt_ids: list[str] = []
        for i, row in enumerate(rows):
            v = row["version"]
            local = local_ctr.get(v, 0)
            local_ctr[v] = local + 1
            gidx = block_of[v] * bc.GIDX_OFFSET + local
            ni = int(nearest_idx[i])
            nearest_key = keys[ni]
            s = fo.Sample(filepath=_abs_image_path(real_bundle, nearest_key))
            s["text"] = row["text"]
            s["category"] = fo.Classification(label=row["class"])
            s["bank_version"] = fo.Classification(label=v)
            s["gidx"] = gidx
            s["sentence_embedding"] = prompt_vec[i].tolist()
            s["nearest_key"] = nearest_key
            prompt_samples.append(s)
            if len(prompt_samples) >= 1000:
                prompt_ids.extend(prompt_ds.add_samples(prompt_samples))
                print(f"[7/9] 문장 적재 {len(prompt_ids)}/{len(rows)}")
                prompt_samples = []
        if prompt_samples:
            prompt_ids.extend(prompt_ds.add_samples(prompt_samples))
        print(f"[7/9] 문장 {len(rows)}개 적재 완료 (버전 {len(versions)}종: {versions})")

        if args.skip_viz:
            n_prompt_viz = 0
            print("[7/9] --skip-viz: 문장 emb_viz 생략")
        else:
            n_prompt_viz = compute_viz(prompt_ds, prompt_ids, prompt_vec)
            print(f"[7/9] 문장 emb_viz {n_prompt_viz}점 등록")

        # compare 워크스페이스 — 두 데이터셋 다 있고 emb_viz 가 있을 때만 (패널이 emb_viz 좌표를 읽는다;
        # fiftyone_app_setup workspace-compare 의 "emb_viz 없음 skip" 정책과 동일).
        stage = "7/9(compare 워크스페이스)"
        compare_saved: list[str] = []
        if args.skip_viz:
            print("[7/9] --skip-viz: compare 워크스페이스 생략 (emb_viz 없이는 패널이 빈 화면)")
        else:
            # 기본 화면은 편의 기능 — 데이터셋·emb_viz 는 이미 완성됐으니 여기서 죽어 채점/리포트를
            # 잃지 않는다(user_default_workspace 오퍼레이터·workspace-compare 와 같은 best-effort).
            # 실패는 warnings 로 리포트에 남고, 복구는 `fiftyone_app_setup.py workspace-compare <name>`.
            try:
                compare_saved = save_compare_workspaces(img_ds, prompt_ds)
                print(f"[7/9] compare 워크스페이스 저장: {compare_saved} (데이터셋 열 때 기본 화면)")
            except Exception as exc:  # noqa: BLE001 — ImportError/ValueError/DB 오류 모두 경고로 강등
                warn_msgs.append(
                    f"compare 워크스페이스 저장 실패({type(exc).__name__}: {exc}) — 데이터셋은 Samples 단독으로 "
                    f"열림. 복구: python3 /workspace/fiftyone_app_setup.py workspace-compare {name},{name_prompts}"
                )
                print(f"[7/9] 경고: {warn_msgs[-1]}")

        # 8) 채점 (기본 실행) -------------------------------------------------------
        stage = "8/9(채점)"
        scoring_result = None
        if args.skip_scoring:
            print("[8/9] --skip-scoring: 채점 생략")
        else:
            print("[8/9] 채점 시작: score_bundle.run_scoring")
            import score_bundle  # lazy import — 상단 임포트 금지 (UPLOAD_SPEC.md §5)

            if args.attach:
                scoring_result = score_bundle.run_scoring(name, bundle_dir, attach=args.attach)
            else:
                scoring_result = score_bundle.run_scoring(name, bundle_dir)
            print("[8/9] 채점 완료")

        # 9) 리포트 ---------------------------------------------------------------
        stage = "9/9(리포트)"
        report = {
            "name": name,
            "name_prompts": name_prompts,
            "bundle_dir": real_bundle,
            "gt_mode": gt_mode,
            "attach": args.attach,
            "skip_viz": args.skip_viz,
            "skip_scoring": args.skip_scoring,
            "counts": {
                "images": len(keys),
                "images_with_gt": n_gt,
                "has_camera": has_camera,
                "prompts": len(rows),
                "versions": versions,
                "image_viz_points": n_img_viz,
                "prompt_viz_points": n_prompt_viz,
            },
            "compare_workspace": compare_saved,
            "warnings": warn_msgs,
            "scoring": scoring_result,
        }
        report_path = bc.write_report(bundle_dir, "ingest_report.json", report)

        print(
            f"\n[완료] '{name}' 이미지 {len(keys)}장 / 문장 {len(rows)}개(버전 {len(versions)}종) "
            f"GT={gt_mode} viz={'ON' if not args.skip_viz else 'OFF'} "
            f"채점={'ON' if not args.skip_scoring else 'OFF'}"
        )
        print(f"[완료] 리포트: {report_path}")
        print(
            f"[완료] FiftyOne 앱(:5153) 헤더 선택기에서 '{name}' 선택 (문장 뷰는 '{name_prompts}' 자동 파생 인식"
            f"{', compare 워크스페이스가 기본으로 열림' if compare_saved else ''})"
        )
        return 0
    except Exception as exc:  # noqa: BLE001 — numpy/PIL/FiftyOne 이 던지는 raw ValueError/OSError 등도
        # 여기서 잡아야 partial_created 안내가 나간다 (bc.BundleError/RuntimeError 만 잡던 이전
        # 버전은 예: gt.csv camera 열 전량 공란 시 FiftyOne ValueError 가 그대로 새어나갔다).
        # SystemExit(score_bundle._invariant 의 §3.3 불변식 위반)은 BaseException 이라 그대로 통과한다.
        # traceback 전체 + 실패 단계(stage) 를 함께 남긴다 — 이전엔 str(exc) 한 줄뿐이라 어느
        # 단계·어느 줄에서 났는지 사후에 알 방법이 없었다(major 리뷰 지적). 이 stdout 은
        # sync_api._run_subprocess 가 tail 20줄만 메모리에 담던 것을 이제 <번들>/_artifacts/
        # ingest.log 에도 전량 흘리므로(sync_api.py) 컨테이너 재시작 후에도 원인 추적이 가능하다.
        import traceback

        print(f"\n[실패][{stage}] {exc}")
        traceback.print_exc()
        if partial_created and name:
            print(
                f"[안내] 데이터셋 '{name}' 이(가) 부분 생성된 상태입니다 — 문제를 해결한 뒤 "
                f"--overwrite 로 재실행하세요 (upload_kit marker 는 이미 있어 재시도 가능합니다)."
            )
        return 1


def main() -> int:
    return run(parse_args())


if __name__ == "__main__":
    sys.exit(main())
