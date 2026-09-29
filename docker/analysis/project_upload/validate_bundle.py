"""업로드 번들 검증 CLI — 읽기 전용. 정본 계약: UPLOAD_SPEC.md + bundle_common.py.

역할: 번들이 인제스트 가능한 형태인지 미리 확인한다. 번들에도 FiftyOne 에도 아무것도
쓰지 않는다. ingest_bundle.py 는 이 모듈의 validate() 를 그대로 재사용한다.

주의: validate() 자체는 fiftyone 이 없어도 동작해야 하므로, fiftyone 임포트는
_check_fiftyone_collision() 안에서만 지연 수행한다 (실패 시 조용히 skip).
"""
from __future__ import annotations

import argparse
import csv
import json
import os

import numpy as np

import bundle_common


# ── 개별 검사 헬퍼 ──────────────────────────────────────────────────────────

def _walk_images(bundle_dir: str) -> list[str]:
    """images/ 아래 실재하는 이미지 파일의 POSIX 상대경로 목록 (확장자 필터)."""
    root = os.path.join(bundle_dir, bundle_common.IMAGES_DIR)
    out: list[str] = []
    if not os.path.isdir(root):
        return out
    for dirpath, _dirs, files in os.walk(root):
        for fn in files:
            if os.path.splitext(fn)[1].lower() not in bundle_common.IMG_EXTS:
                continue
            rel = os.path.relpath(os.path.join(dirpath, fn), root)
            out.append(rel.replace(os.sep, "/"))
    return out


def _check_external_path(bundle_dir: str) -> str | None:
    """번들이 UPLOAD_ROOT 밖이면 경고 문자열, 안이면 None (항목 5)."""
    abs_bundle = os.path.realpath(bundle_dir)
    abs_root = os.path.realpath(bundle_common.UPLOAD_ROOT)
    try:
        inside = os.path.commonpath([abs_bundle, abs_root]) == abs_root
    except ValueError:
        inside = False  # 서로 다른 마운트/드라이브 등
    if inside:
        return None
    return (
        f"번들 경로가 업로드 루트({bundle_common.UPLOAD_ROOT}) 밖입니다: {abs_bundle} "
        "— 인제스트 시 --allow-external-media 필요"
    )


def _check_fiftyone_collision(dataset_name: str) -> list[str]:
    """<dataset>/<dataset>-prompts 기존 존재 여부 (항목 6). fiftyone 미가용 시 조용히 skip."""
    try:
        import fiftyone as fo
    except Exception:
        return []
    warns: list[str] = []
    try:
        existing = set(fo.list_datasets())
        for name in (dataset_name, dataset_name + bundle_common.PROMPTS_SUFFIX):
            if name not in existing:
                continue
            try:
                marker = bool(fo.load_dataset(name).info.get("upload_kit"))
            except Exception:
                marker = False
            if marker:
                warns.append(
                    f"FiftyOne 데이터셋 {name!r} 이미 존재 (upload_kit marker 확인됨) "
                    "— 인제스트 시 --overwrite 필요"
                )
            else:
                warns.append(
                    f"FiftyOne 데이터셋 {name!r} 이미 존재 (upload_kit marker 없음) "
                    "— 기존 자산으로 간주돼 인제스트가 거부됨"
                )
    except Exception:
        return []  # 연결 실패 등 — 판정 불가는 오류가 아니라 조용히 skip
    return warns


# ── 메인 검증 진입점 ────────────────────────────────────────────────────────

def validate(bundle_dir: str) -> dict:
    """번들을 읽기만 하며 검증한다. fiftyone 없이도 동작.

    반환: {"ok": bool, "errors": [str], "warnings": [str], "mode": str,
           "counts": {...}, "versions": [str]}
    """
    errors: list[str] = []
    warnings: list[str] = []
    counts = {
        "image_files_on_disk": 0,
        "image_npz_keys": 0,
        "missing_image_files": 0,
        "extra_image_files": 0,
        "prompt_rows": 0,
        "prompt_versions": 0,
        "gt_rows": 0,
        "gt_missing_images": 0,
        "gt_unknown_classes": 0,
    }
    versions: list[str] = []
    mode = "unknown"

    # 1) manifest — 파싱 실패하면 embedding_dim/dataset 이름을 못 얻어 후속 검사가
    #    전부 불가능하므로 이 시점까지만 채점하고 즉시 반환한다.
    try:
        manifest = bundle_common.load_manifest(bundle_dir)
    except bundle_common.BundleError as e:
        errors.append(str(e))
        return {"ok": False, "errors": errors, "warnings": warnings, "mode": mode,
                "counts": counts, "versions": versions}

    dim = manifest["embedding_dim"]
    dataset_name = manifest["dataset"]

    # 2) 이미지 임베딩 npz (계약: bundle_common.load_image_npz)
    image_keys: list[str] | None = None
    try:
        image_keys, _image_vec = bundle_common.load_image_npz(bundle_dir, dim)
        counts["image_npz_keys"] = len(image_keys)
    except bundle_common.BundleError as e:
        errors.append(str(e))
    except (OSError, ValueError) as e:  # noqa: BLE001 — 손상 npz(zipfile.BadZipFile 등)도
        # BundleError 가 아니라 raw traceback 으로 새나가던 것을 fail-closed 한국어 오류로 변환
        # (외부 업로더가 가장 잘 만드는 고장 형태 — codex 리뷰 지적).
        errors.append(f"{bundle_common.IMG_NPZ}: 로드 실패 ({type(e).__name__}: {e})")

    # 3) 프롬프트 csv+npz (계약: bundle_common.load_prompts)
    prompt_rows: list[dict] | None = None
    try:
        prompt_rows, _prompt_vec, versions = bundle_common.load_prompts(bundle_dir, dim)
        counts["prompt_rows"] = len(prompt_rows)
        counts["prompt_versions"] = len(versions)
    except bundle_common.BundleError as e:
        errors.append(str(e))
    except (OSError, ValueError) as e:  # noqa: BLE001 — CP949 등 비UTF-8 CSV(UnicodeDecodeError)·
        # 손상 npz 도 동일하게 fail-closed 한국어 오류로 변환
        errors.append(f"{bundle_common.PROMPTS_CSV}/{bundle_common.PROMPT_NPZ}: 로드 실패 ({type(e).__name__}: {e})")

    # 4) image npz key ↔ images/ 실재 파일 대조 (항목 2, 3) — bundle_common 이 검사하지
    #    않는 부분이라 여기서 직접 확인한다.
    if image_keys is not None:
        disk_files = set(_walk_images(bundle_dir))
        counts["image_files_on_disk"] = len(disk_files)
        missing = [k for k in image_keys if k not in disk_files]
        counts["missing_image_files"] = len(missing)
        if missing:
            errors.append(
                f"{bundle_common.IMG_NPZ}: key 가 가리키는 이미지 파일이 "
                f"{bundle_common.IMAGES_DIR}/ 안에 없음 {len(missing)}개 (예: {missing[:3]})"
            )
        extra = sorted(disk_files - set(image_keys))
        counts["extra_image_files"] = len(extra)
        if extra:
            warnings.append(
                f"{bundle_common.IMAGES_DIR}/ 에 있으나 {bundle_common.IMG_NPZ} 에 없는 이미지 "
                f"{len(extra)}개 (emb_viz 에서 제외됨, 예: {extra[:3]})"
            )

    # 5) GT 모드 해석 + 로드 (항목 4)
    try:
        mode = bundle_common.resolve_gt_mode(bundle_dir, manifest)
    except bundle_common.BundleError as e:
        errors.append(str(e))
        mode = "unknown"

    gt: dict = {}
    if mode not in ("none", "unknown"):
        if image_keys is None:
            warnings.append("이미지 임베딩(npz) 로드 실패로 GT 검증을 건너뜀")
        else:
            try:
                gt, _has_camera = bundle_common.load_gt(bundle_dir, image_keys, mode)
                counts["gt_rows"] = len(gt)
            except bundle_common.BundleError as e:
                errors.append(str(e))
            except (OSError, ValueError) as e:  # noqa: BLE001 — 비UTF-8 gt.csv 등도 fail-closed 오류로
                errors.append(f"{bundle_common.GT_CSV}: 로드 실패 ({type(e).__name__}: {e})")

    if gt:
        if prompt_rows is not None:
            prompt_classes = {r["class"] for r in prompt_rows}
            gt_classes = {v["class"] for v in gt.values()}
            unknown_classes = sorted(gt_classes - prompt_classes)
            counts["gt_unknown_classes"] = len(unknown_classes)
            if unknown_classes:
                warnings.append(
                    f"GT 클래스 중 {bundle_common.PROMPTS_CSV} class 집합에 없는 것 "
                    f"{len(unknown_classes)}개 {unknown_classes} — 해당 클래스는 어떤 문장도 못 이겨 "
                    "항상 오답 처리됨"
                )
        if image_keys is not None:
            missing_gt = [k for k in image_keys if k not in gt]
            counts["gt_missing_images"] = len(missing_gt)
            if missing_gt:
                warnings.append(
                    f"GT 없는 이미지 {len(missing_gt)}개 (match='no_gt' 처리, purity 집계 제외)"
                )

    # 6) 번들 경로가 업로드 루트 밖인지 (항목 5)
    ext_warn = _check_external_path(bundle_dir)
    if ext_warn:
        warnings.append(ext_warn)

    # 7) FiftyOne 이름 충돌 (항목 6, fiftyone 지연 임포트 — 실패 시 조용히 skip)
    warnings.extend(_check_fiftyone_collision(dataset_name))

    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "mode": mode,
        "counts": counts,
        "versions": versions,
    }


# ── CLI ─────────────────────────────────────────────────────────────────────

_MODE_LABEL = {
    "csv": "GT 있음 (gt.csv) — 좌표+성능",
    "folders": "GT 있음 (images/<class>/) — 좌표+성능",
    "none": "GT 없음 — 좌표만",
    "unknown": "판정 불가 (치명적 오류로 후속 검사 중단됨)",
}


def _print_report(bundle_dir: str, result: dict) -> None:
    print(f"=== 번들 검증: {bundle_dir} ===")
    print(f"모드: {_MODE_LABEL.get(result['mode'], result['mode'])}")
    if result["versions"]:
        print(f"프롬프트 버전({len(result['versions'])}개): {', '.join(result['versions'])}")
    print("통계:")
    for k, v in result["counts"].items():
        print(f"  {k}: {v}")
    print()
    if result["errors"]:
        print(f"[오류] {len(result['errors'])}건")
        for e in result["errors"]:
            print(f"  - {e}")
    else:
        print("[오류] 없음")
    if result["warnings"]:
        print(f"[경고] {len(result['warnings'])}건")
        for w in result["warnings"]:
            print(f"  - {w}")
    else:
        print("[경고] 없음")
    print()
    print("결과: PASS" if result["ok"] else f"결과: FAIL (오류 {len(result['errors'])}건)")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="업로드 번들 검증 (읽기 전용, exit 0=통과/1=오류)")
    ap.add_argument("bundle_dir", nargs="?", help="번들 디렉토리 경로")
    ap.add_argument("--json", action="store_true", help="사람용 리포트 대신 JSON 만 stdout 에 출력")
    ap.add_argument("--selftest", action="store_true", help="자체 테스트 실행 후 종료")
    args = ap.parse_args(argv)

    if args.selftest:
        _selftest()
        return 0

    if not args.bundle_dir:
        ap.error("bundle_dir 필요 (또는 --selftest)")

    result = validate(args.bundle_dir)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        _print_report(args.bundle_dir, result)
    return 0 if result["ok"] else 1


# ── 셀프테스트 ──────────────────────────────────────────────────────────────

def _selftest() -> None:
    """tempfile 로 최소 정상 번들 + 고장 케이스 2개를 검증한다.

    선택기: 데이터셋 이름은 "_upe2e_" 접두사를 쓰지 않는다 — DATASET_NAME_RE 가 첫 글자로
    영숫자만 허용해 언더스코어로 시작할 수 없고(§manifest.dataset), 이 테스트는 애초에
    FiftyOne 데이터셋을 생성/삭제하지 않는다(validate() 는 읽기 전용, 항목6도 list/read 뿐).
    """
    import tempfile

    from PIL import Image

    rng = np.random.default_rng(0)
    dim = 8

    def _base_bundle(root: str, n: int = 2) -> list[str]:
        """정상 번들 골격(manifest+images+prompts) 생성. image_embeddings.npz 는 호출자가 채움."""
        os.makedirs(os.path.join(root, bundle_common.IMAGES_DIR), exist_ok=True)
        keys = [f"img{i}.jpg" for i in range(n)]
        for i, k in enumerate(keys):
            Image.new("RGB", (4, 4), color=(i * 40, 10, 10)).save(
                os.path.join(root, bundle_common.IMAGES_DIR, k)
            )
        manifest = {
            "format_version": bundle_common.FORMAT_VERSION,
            "dataset": "validate-selftest-bundle",
            "model_name": "unit-test",
            "embedding_dim": dim,
            "gt_mode": "auto",
        }
        with open(os.path.join(root, bundle_common.MANIFEST), "w", encoding="utf-8") as f:
            json.dump(manifest, f)
        with open(os.path.join(root, bundle_common.PROMPTS_CSV), "w", encoding="utf-8", newline="") as f:
            wr = csv.writer(f)
            wr.writerow(["version", "class", "text"])
            wr.writerow(["v1", "fire", "a fire is burning"])
            wr.writerow(["v1", "normal", "nothing unusual"])
        pvec = rng.normal(size=(2, dim)).astype(np.float32)
        np.savez(os.path.join(root, bundle_common.PROMPT_NPZ), vec=pvec)
        return keys

    # --- 정상 번들: ok True ---
    with tempfile.TemporaryDirectory() as root:
        keys = _base_bundle(root)
        ivec = rng.normal(size=(len(keys), dim)).astype(np.float32)
        np.savez(os.path.join(root, bundle_common.IMG_NPZ), key=np.array(keys), vec=ivec)
        result = validate(root)
        assert result["ok"], f"정상 번들인데 오류 발생: {result['errors']}"
        assert result["mode"] == "none", result["mode"]
        assert result["counts"]["image_npz_keys"] == 2, result["counts"]
        assert result["counts"]["prompt_rows"] == 2, result["counts"]

    # --- 고장 1: image_embeddings.npz 행수 불일치 (key 2개 vs vec 1행) ---
    with tempfile.TemporaryDirectory() as root:
        keys = _base_bundle(root)
        ivec = rng.normal(size=(1, dim)).astype(np.float32)  # 의도적으로 행수 불일치
        np.savez(os.path.join(root, bundle_common.IMG_NPZ), key=np.array(keys), vec=ivec)
        result = validate(root)
        assert not result["ok"]
        assert any("행" in e and "vec" in e for e in result["errors"]), result["errors"]

    # --- 고장 2: 존재하지 않는 key (npz 엔 있으나 images/ 에 파일 없음) ---
    with tempfile.TemporaryDirectory() as root:
        keys = _base_bundle(root)
        keys_ghost = keys + ["ghost.jpg"]
        ivec = rng.normal(size=(len(keys_ghost), dim)).astype(np.float32)
        np.savez(os.path.join(root, bundle_common.IMG_NPZ), key=np.array(keys_ghost), vec=ivec)
        result = validate(root)
        assert not result["ok"]
        assert result["counts"]["missing_image_files"] == 1, result["counts"]
        assert any("ghost.jpg" in e for e in result["errors"]), result["errors"]

    # --- 고장 2b: 손상된 npz (zip 아닌 임의 바이트) — BundleError 아닌 예외도 fail-closed 오류로
    #     변환돼야 함 (raw traceback 으로 새나가면 안 됨, codex 리뷰 지적) ---
    with tempfile.TemporaryDirectory() as root:
        _base_bundle(root)
        with open(os.path.join(root, bundle_common.IMG_NPZ), "wb") as f:
            f.write(b"not a real npz file")
        result = validate(root)
        assert not result["ok"]
        assert any(bundle_common.IMG_NPZ in e for e in result["errors"]), result["errors"]

    # --- 고장 2c: CP949(비UTF-8) prompts.csv — UnicodeDecodeError 도 fail-closed 오류로 변환 ---
    with tempfile.TemporaryDirectory() as root:
        keys = _base_bundle(root)
        ivec = rng.normal(size=(len(keys), dim)).astype(np.float32)
        np.savez(os.path.join(root, bundle_common.IMG_NPZ), key=np.array(keys), vec=ivec)
        with open(os.path.join(root, bundle_common.PROMPTS_CSV), "wb") as f:
            f.write("version,class,text\r\nv1,fire,불이야\r\n".encode("cp949"))
        result = validate(root)
        assert not result["ok"]
        assert any("로드 실패" in e for e in result["errors"]), result["errors"]

    # --- 고장 3: 이미지 0장 — validate 도 ingest 와 동일하게 fail-closed 여야 함
    #     (예전엔 vec.shape=(0,D) 에서 check_vectors 가 오류 0건을 내 validate 만 PASS 하고
    #     ingest 는 바로 뒤에서 실패하는 fail-closed 위반이 있었다) ---
    with tempfile.TemporaryDirectory() as root:
        _base_bundle(root, n=0)
        np.savez(os.path.join(root, bundle_common.IMG_NPZ),
                 key=np.array([], dtype=str), vec=np.zeros((0, dim), np.float32))
        result = validate(root)
        assert not result["ok"], "이미지 0장인데 validate 가 PASS"
        assert any("이미지 0장" in e for e in result["errors"]), result["errors"]

    print("validate_bundle selftest OK")


if __name__ == "__main__":
    raise SystemExit(main())
