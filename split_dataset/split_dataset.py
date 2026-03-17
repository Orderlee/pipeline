"""
split_dataset.py usage examples:

1) Dry run (no writes), using config:
   python split_dataset.py \
     --config configs/global.yaml \
     --source-bucket image-data \
     --source-prefix coco/ \
     --assets-root-prefix "" \
     --assets-dir-prefix asset_ \
     --dest-bucket data-pipeline \
     --dest-prefix datasets/yolo-dataset \
     --dataset-name yolo-dataset \
     --train-ratio 0.9 \
     --seed 42 \
     --bucket-path s3://image-data \
     --dry-run

2) Actual write (creates labels next to images, and train/test/dataset.yaml in dest bucket):
   python split_dataset.py \
     --config configs/global.yaml \
     --source-bucket image-data \
     --source-prefix coco/ \
     --assets-root-prefix "" \
     --assets-dir-prefix asset_ \
     --dest-bucket data-pipeline \
     --dest-prefix datasets/yolo-dataset \
     --dataset-name yolo-dataset \
     --train-ratio 0.9 \
     --seed 42 \
     --bucket-path s3://image-data

3) Example for a layout where JSONs live in a bboxes/ subfolder:
   - Images: image-data/vietnam_data/processed_data/bat_as_weapon/20250812_AM_IS_CD_BW_1
   - JSONs : image-data/vietnam_data/processed_data/bat_as_weapon/20250812_AM_IS_CD_BW_1/bboxes
   python split_dataset.py \
     --config configs/global.yaml \
     --source-bucket image-data \
     --json-prefix vietnam_data/processed_data/bat_as_weapon/20250812_AM_IS_CD_BW_1/bboxes \
     --images-prefix vietnam_data/processed_data/bat_as_weapon/20250812_AM_IS_CD_BW_1 \
     --assets-dir-prefix "" \
     --dest-bucket data-pipeline \
     --dest-prefix datasets/yolo-bat-20250812 \
     --dataset-name yolo-bat-20250812 \
     --train-ratio 0.9 \
     --seed 42 \
     --bucket-path s3://image-data

4) Batch mode: iterate category folders under a root prefix and merge all JSONs per category
   - Images: image-data/vietnam_data/processed_data/bat_as_weapon/<folder>
   - JSONs : image-data/vietnam_data/processed_data/bat_as_weapon/<folder>/bboxes
   python split_dataset.py \
     --config configs/global.yaml \
     --source-bucket image-data \
     --batch-root-prefix vietnam_data/processed_data/bat_as_weapon \
     --bboxes-subdir bboxes \
     --assets-dir-prefix "" \
     --dest-bucket data-pipeline \
     --dest-prefix datasets/yolo-bat \
     --train-ratio 0.9 \
     --seed 42 \
     --bucket-path s3://image-data

5) Batch mode + merge multiple categories into one dataset (e.g. *_as_weapon -> weapon):
   python split_dataset.py \
     --config configs/global.yaml \
     --source-bucket image-data \
     --batch-root-prefix vietnam_data/processed_data \
     --bboxes-subdir bboxes \
     --assets-dir-prefix "" \
     --dest-bucket data-pipeline \
     --dest-prefix yolo_dataset2 \
     --merge-category-suffix _as_weapon \
     --merge-category-name weapon \
     --train-ratio 0.9 \
     --seed 42 \
     --bucket-path s3://image-data

Notes:
- This script writes YOLO label .txt files into the source bucket next to images.
- train.txt/test.txt/dataset.yaml are written into the destination bucket.
"""

import argparse
import json
import logging
import random
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import yaml


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_settings(config_path: Optional[str]):
    repo_root = _repo_root()
    sys.path.insert(0, str(repo_root / "src" / "python"))
    from common.config import get_settings  # pylint: disable=import-error

    # 상대경로면 repo 루트 기준으로 보정
    if config_path:
        cfg = Path(config_path)
        if not cfg.is_absolute():
            config_path = str(repo_root / cfg)

    return get_settings(config_path)


def _build_s3_client(endpoint: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _ensure_bucket(s3, bucket: str, create: bool) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchBucket"}:
            if not create:
                raise
            s3.create_bucket(Bucket=bucket)
        else:
            raise


def _list_objects(s3, bucket: str, prefix: str) -> Iterable[Tuple[str, int]]:
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []):
            key = obj.get("Key")
            if not key:
                continue
            size = int(obj.get("Size", 0))
            yield key, size

        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")


def _list_common_prefixes(s3, bucket: str, prefix: str) -> Iterable[str]:
    """List immediate child prefixes under the given prefix."""
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)

        for entry in resp.get("CommonPrefixes", []):
            child = entry.get("Prefix")
            if child:
                yield child

        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")


def _has_jsons(s3, bucket: str, prefix: str) -> bool:
    """Return True if any JSON file exists under prefix."""
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=50)
    for obj in resp.get("Contents", []):
        key = obj.get("Key") or ""
        if key.lower().endswith(".json"):
            return True
    return False


def _collect_json_keys(
    s3,
    bucket: str,
    prefixes: List[str],
    max_items: int,
) -> List[str]:
    """Collect JSON keys across multiple prefixes (deduplicated)."""
    seen = set()
    out: List[str] = []
    for prefix in prefixes:
        for key, size in _list_objects(s3, bucket, prefix):
            if size <= 0:
                continue
            if not _is_json(key):
                continue
            if key in seen:
                continue
            seen.add(key)
            out.append(key)
            if max_items and len(out) >= max_items:
                return out
    return out


def _build_basename_index_for_prefixes(
    s3,
    bucket: str,
    prefixes: List[str],
    assets_dir_prefix: str,
    max_scan: int,
    image_exts: set[str],
) -> Tuple[Dict[str, str], Dict[str, List[str]], set[str]]:
    """Build basename index across multiple prefixes."""
    basename_to_key: Dict[str, str] = {}
    duplicates: Dict[str, List[str]] = {}
    all_keys: set[str] = set()

    for prefix in prefixes:
        idx, dup = _build_basename_to_asset_key_index(
            s3=s3,
            bucket=bucket,
            assets_root_prefix=prefix,
            assets_dir_prefix=assets_dir_prefix,
            max_scan=max_scan,
            image_exts=image_exts,
        )

        for base, key in idx.items():
            all_keys.add(key)
            if base not in basename_to_key:
                basename_to_key[base] = key
            else:
                duplicates.setdefault(base, [basename_to_key[base]])
                if key not in duplicates[base]:
                    duplicates[base].append(key)

        for base, keys in dup.items():
            for key in keys:
                all_keys.add(key)
            if base not in duplicates:
                duplicates[base] = list(keys)
            else:
                for key in keys:
                    if key not in duplicates[base]:
                        duplicates[base].append(key)

    return basename_to_key, duplicates, all_keys


def _normalize_prefix(prefix: str) -> str:
    prefix = (prefix or "").strip("/")
    return f"{prefix}/" if prefix else ""


def _is_json(key: str) -> bool:
    return Path(key).suffix.lower() == ".json"


def _get_json_object(s3, bucket: str, key: str) -> dict:
    resp = s3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"].read()
    return json.loads(body.decode("utf-8"))


def _put_text_object(s3, bucket: str, key: str, text: str, dry_run: bool) -> None:
    if dry_run:
        return
    s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType="text/plain")


def _put_yaml_object(s3, bucket: str, key: str, payload: dict, dry_run: bool) -> None:
    if dry_run:
        return
    body = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False)
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"), ContentType="application/x-yaml")


def _split_records(records: List[dict], train_ratio: float, seed: int) -> Tuple[List[dict], List[dict]]:
    rng = random.Random(seed)
    rng.shuffle(records)
    total = len(records)
    if total == 0:
        return [], []
    train_count = int(total * train_ratio)
    train_count = max(1, min(train_count, total))
    test_count = total - train_count
    if total >= 2 and test_count == 0:
        train_count = total - 1
    return records[:train_count], records[train_count:]


def _merge_coco_jsons(coco_list: List[dict]) -> dict:
    """
    여러 COCO json 병합:
    - categories: name 기준 통합 + id 재부여
    - images: id 재부여
    - annotations: id 재부여 + image_id/category_id remap
    """
    merged: dict = {"info": {}, "licenses": [], "images": [], "annotations": [], "categories": []}

    if coco_list:
        merged["info"] = coco_list[0].get("info", {}) or {}
        merged["licenses"] = coco_list[0].get("licenses", []) or {}

    cat_name_to_new_id: Dict[str, int] = {}
    next_cat_id = 1
    next_img_id = 1
    next_ann_id = 1

    for coco in coco_list:
        categories = coco.get("categories", []) or []
        images = coco.get("images", []) or []
        annotations = coco.get("annotations", []) or []

        old_cat_to_new: Dict[int, int] = {}
        for cat in categories:
            name = str(cat.get("name", "")).strip()
            if not name:
                continue
            if name not in cat_name_to_new_id:
                cat_name_to_new_id[name] = next_cat_id
                merged["categories"].append(
                    {"id": next_cat_id, "name": name, "supercategory": cat.get("supercategory", "") or ""}
                )
                next_cat_id += 1

            old_id = int(cat.get("id", 0))
            if old_id > 0:
                old_cat_to_new[old_id] = cat_name_to_new_id[name]

        old_img_to_new: Dict[int, int] = {}
        for img in images:
            old_img_id = int(img.get("id", 0))
            new_img_id = next_img_id
            next_img_id += 1
            old_img_to_new[old_img_id] = new_img_id

            new_img = dict(img)
            new_img["id"] = new_img_id
            merged["images"].append(new_img)

        for ann in annotations:
            old_img_id = int(ann.get("image_id", 0))
            old_cat_id = int(ann.get("category_id", 0))
            if old_img_id not in old_img_to_new:
                continue
            if old_cat_id not in old_cat_to_new:
                continue

            new_ann = dict(ann)
            new_ann["id"] = next_ann_id
            next_ann_id += 1
            new_ann["image_id"] = old_img_to_new[old_img_id]
            new_ann["category_id"] = old_cat_to_new[old_cat_id]
            merged["annotations"].append(new_ann)

    return merged


def _safe_rel_path(p: str) -> str:
    return str(p or "").lstrip("/")


def _coco_bbox_to_yolo(bbox: List[float], img_w: int, img_h: int):
    """
    COCO bbox: [x_min, y_min, w, h] (pixels)
    YOLO: x_center/img_w, y_center/img_h, w/img_w, h/img_h
    """
    if not bbox or len(bbox) != 4:
        return None
    x, y, w, h = bbox
    if img_w <= 0 or img_h <= 0 or w <= 0 or h <= 0:
        return None

    xc = (x + w / 2.0) / img_w
    yc = (y + h / 2.0) / img_h
    ww = w / img_w
    hh = h / img_h

    def clamp(v: float) -> float:
        return max(0.0, min(1.0, float(v)))

    return clamp(xc), clamp(yc), clamp(ww), clamp(hh)


def _build_basename_to_asset_key_index(
    s3,
    bucket: str,
    assets_root_prefix: str,
    assets_dir_prefix: str,
    max_scan: int,
    image_exts: set[str],
) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """
    image-data 버킷에서 asset_*/ 아래 이미지를 스캔해서
    basename(파일명) -> full key(assets_root_prefix/asset_.../file.jpg) 인덱스를 만든다.
    """
    basename_to_key: Dict[str, str] = {}
    duplicates: Dict[str, List[str]] = {}

    prefix = _normalize_prefix(assets_root_prefix)
    scanned = 0

    for key, size in _list_objects(s3, bucket, prefix):
        if size <= 0:
            continue

        rel = key[len(prefix) :] if prefix else key
        rel = rel.lstrip("/")
        if not rel.startswith(assets_dir_prefix):
            continue

        ext = Path(key).suffix.lower()
        if ext not in image_exts:
            continue

        base = Path(key).name
        if base not in basename_to_key:
            basename_to_key[base] = key
        else:
            if base not in duplicates:
                duplicates[base] = [basename_to_key[base]]
            duplicates[base].append(key)

        scanned += 1
        if max_scan and scanned >= max_scan:
            break

    return basename_to_key, duplicates


def _build_dataset_yaml_txt(dest_prefix_path: str, class_names: List[str], include_val: bool) -> dict:
    """
    ✅ YOLO 이미지리스트 방식:
      path: (train.txt/test.txt가 있는 디렉토리)
      train: train.txt
      val/test: test.txt
    """
    payload = {
        "path": dest_prefix_path,
        "train": "train.txt",
        "test": "test.txt",
        "nc": len(class_names),
        "names": class_names,
    }
    return payload


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="COCO JSONs -> YOLO split. No images in dest. train/test.txt contain {bucket_path}/asset_*/img.jpg."
    )
    parser.add_argument("--config", default=None)

    parser.add_argument("--source-bucket", default="image-data")
    parser.add_argument("--source-prefix", default="")
    parser.add_argument("--json-prefix", default=None, help="COCO JSON prefix (overrides --source-prefix).")
    parser.add_argument("--batch-root-prefix", default=None, help="Batch mode root prefix (iterate child folders).")
    parser.add_argument("--bboxes-subdir", default="bboxes", help="Subdir name that contains COCO JSONs in each dataset folder.")
    parser.add_argument("--merge-category-suffix", default=None, help="Merge categories that end with this suffix.")
    parser.add_argument("--merge-category-name", default="merged", help="Dataset folder name for merged categories.")

    parser.add_argument("--assets-root-prefix", default="")
    parser.add_argument("--images-prefix", default=None, help="Image scan prefix (overrides --assets-root-prefix).")
    parser.add_argument("--assets-dir-prefix", default="asset_")
    parser.add_argument("--scan-max-images", type=int, default=0)
    parser.add_argument("--image-extensions", default=".jpg,.jpeg,.png,.bmp,.tif,.tiff")

    parser.add_argument("--dest-bucket", default="data-pipeline")
    parser.add_argument("--dest-prefix", default="")
    parser.add_argument("--dataset-name", default="yolo-dataset")

    parser.add_argument("--train-ratio", type=float, default=0.9)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-items", type=int, default=0)

    # ✅ train/test.txt에 들어갈 “버킷경로 prefix”
    # 예: s3://image-data  또는 /mnt/minio/image-data  또는 http://.../image-data
    parser.add_argument(
        "--bucket-path",
        required=True,
        help='train/test.txt에 기록할 prefix. 예) "s3://image-data" 또는 "/mnt/minio/image-data"',
    )

    parser.add_argument("--dry-run", action="store_true")

    parser.add_argument("--write-yaml", dest="write_yaml", action="store_true", default=True)
    parser.add_argument("--no-yaml", dest="write_yaml", action="store_false")

    parser.add_argument("--include-val", dest="include_val", action="store_true", default=True)
    parser.add_argument("--no-val", dest="include_val", action="store_false")

    parser.add_argument("--create-bucket", dest="create_bucket", action="store_true", default=True)
    parser.add_argument("--no-create-bucket", dest="create_bucket", action="store_false")

    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = _parse_args()

    if not 0 < args.train_ratio < 1:
        raise ValueError("train-ratio must be between 0 and 1.")

    settings = _load_settings(args.config)
    minio_cfg = settings.minio
    s3 = _build_s3_client(minio_cfg.endpoint, minio_cfg.access_key, minio_cfg.secret_key)

    _ensure_bucket(s3, args.dest_bucket, args.create_bucket)

    def process_dataset(
        json_prefix: str,
        images_prefix: str,
        dataset_name: str,
        dest_prefix: str,
        json_keys_override: Optional[List[str]] = None,
        images_prefixes_override: Optional[List[str]] = None,
    ) -> int:
        # 0) 이미지 인덱스 생성
        image_exts = {e.strip().lower() for e in args.image_extensions.split(",") if e.strip()}
        if not image_exts:
            raise ValueError("No image extensions provided.")

        assets_root_prefixes = [images_prefix]
        if images_prefixes_override:
            assets_root_prefixes = list(dict.fromkeys(images_prefixes_override))  # stable unique

        assets_dir_prefix = args.assets_dir_prefix
        basename_to_key, duplicates, all_keys = _build_basename_index_for_prefixes(
            s3=s3,
            bucket=args.source_bucket,
            prefixes=assets_root_prefixes,
            assets_dir_prefix=assets_dir_prefix,
            max_scan=args.scan_max_images,
            image_exts=image_exts,
        )

        # 실수 방지: images-prefix를 쓸 때 asset_ 필터가 과하게 걸릴 수 있음
        if not basename_to_key and assets_root_prefixes and assets_dir_prefix == "asset_":
            logging.warning(
                "No images indexed under images_prefix=%r with assets_dir_prefix='asset_'. Retrying without dir filter.",
                assets_root_prefixes,
            )
            assets_dir_prefix = ""
            basename_to_key, duplicates, all_keys = _build_basename_index_for_prefixes(
                s3=s3,
                bucket=args.source_bucket,
                prefixes=assets_root_prefixes,
                assets_dir_prefix=assets_dir_prefix,
                max_scan=args.scan_max_images,
                image_exts=image_exts,
            )

        # 실수 방지: assets_root_prefix로 못 찾으면 루트 재시도
        if not basename_to_key and assets_root_prefixes:
            logging.warning("No images indexed under assets_root_prefix=%r. Retrying with ''.", assets_root_prefixes)
            basename_to_key, duplicates, all_keys = _build_basename_index_for_prefixes(
                s3=s3,
                bucket=args.source_bucket,
                prefixes=[""],
                assets_dir_prefix=assets_dir_prefix,
                max_scan=args.scan_max_images,
                image_exts=image_exts,
            )

        logging.info("[%s] Indexed %d unique image basenames.", dataset_name, len(basename_to_key))
        if duplicates:
            logging.warning("[%s] Duplicate basenames=%d (ambiguous mapping).", dataset_name, len(duplicates))

        # 1) COCO JSON 목록 수집
        if json_keys_override is not None:
            json_keys = list(json_keys_override)
        else:
            json_keys = []
            for key, size in _list_objects(s3, args.source_bucket, json_prefix):
                if size <= 0:
                    continue
                if not _is_json(key):
                    continue
                json_keys.append(key)
                if args.max_items and len(json_keys) >= args.max_items:
                    break

        if not json_keys:
            logging.warning("[%s] No COCO JSON files found under %s.", dataset_name, json_prefix)
            return 1

        # 2) JSON 로드 + 병합
        coco_list: List[dict] = []
        for key in json_keys:
            try:
                coco_list.append(_get_json_object(s3, args.source_bucket, key))
            except Exception as exc:  # noqa: BLE001
                logging.warning("[%s] Failed to read %s (%s)", dataset_name, key, exc)

        if not coco_list:
            logging.warning("[%s] All JSON reads failed.", dataset_name)
            return 1

        merged = _merge_coco_jsons(coco_list)

        images = merged.get("images", []) or []
        annotations = merged.get("annotations", []) or []
        categories = merged.get("categories", []) or []
        if not images:
            logging.warning("[%s] Merged COCO has no images.", dataset_name)
            return 1

        # 3) class 매핑
        categories_sorted = sorted(categories, key=lambda c: int(c.get("id", 0)))
        cat_id_to_class_idx: Dict[int, int] = {}
        class_names: List[str] = []
        for idx, cat in enumerate(categories_sorted):
            cid = int(cat.get("id", 0))
            name = str(cat.get("name", f"class{idx}"))
            cat_id_to_class_idx[cid] = idx
            class_names.append(name)

        img_by_id: Dict[int, dict] = {int(img.get("id", 0)): img for img in images}
        anns_by_img: Dict[int, List[dict]] = {}
        for ann in annotations:
            iid = int(ann.get("image_id", 0))
            anns_by_img.setdefault(iid, []).append(ann)

        # 4) split
        train_imgs, test_imgs = _split_records(images, args.train_ratio, args.seed)
        train_ids = [int(img["id"]) for img in train_imgs]
        test_ids = [int(img["id"]) for img in test_imgs]

        def build_yolo_label_text(image_id: int) -> Optional[str]:
            img = img_by_id.get(image_id)
            if not img:
                return None
            img_w = int(img.get("width", 0) or 0)
            img_h = int(img.get("height", 0) or 0)
            if img_w <= 0 or img_h <= 0:
                return None

            lines: List[str] = []
            for ann in anns_by_img.get(image_id, []):
                cid = int(ann.get("category_id", 0))
                if cid not in cat_id_to_class_idx:
                    continue
                yolo = _coco_bbox_to_yolo(ann.get("bbox", None), img_w, img_h)
                if not yolo:
                    continue
                cls = cat_id_to_class_idx[cid]
                xc, yc, ww, hh = yolo
                lines.append(f"{cls} {xc:.6f} {yc:.6f} {ww:.6f} {hh:.6f}")
            return "\n".join(lines) + "\n"

        def resolve_image_key(file_name: str) -> Optional[str]:
            """
            COCO images[].file_name -> 실제 image-data key로 변환
            """
            rel = _safe_rel_path(file_name)
            if not rel:
                return None

            # 이미 asset_.../xxx.jpg 형태면 assets_root_prefix만 붙여주면 됨
            if assets_dir_prefix and rel.startswith(assets_dir_prefix):
                for root in assets_root_prefixes:
                    root_norm = _normalize_prefix(root)
                    candidate = f"{root_norm}{rel}" if root_norm else rel
                    if not all_keys or candidate in all_keys:
                        return candidate
                return None
            if not assets_dir_prefix:
                if "/" in rel:
                    if all_keys:
                        if rel in all_keys:
                            return rel
                        for root in assets_root_prefixes:
                            root_norm = _normalize_prefix(root)
                            candidate = f"{root_norm}{rel}" if root_norm else rel
                            if candidate in all_keys:
                                return candidate
                    else:
                        # fallback: try with first root
                        root_norm = _normalize_prefix(assets_root_prefixes[0]) if assets_root_prefixes else ""
                        if root_norm and rel.startswith(root_norm):
                            return rel
                        return f"{root_norm}{rel}" if root_norm else rel

            # 아니면 basename으로 인덱스 매칭
            base = Path(rel).name
            return basename_to_key.get(base)

        # ✅ bucket_path 정규화 (끝 슬래시 제거)
        bucket_path = args.bucket_path.rstrip("/")

        train_lines: List[str] = []
        test_lines: List[str] = []

        # 라벨은 "image-data"에 이미지 옆으로 생성
        # (YOLO가 가장 쉽게 찾는 구조)
        label_puts: List[Tuple[str, str]] = []

        missing_images = 0
        missing_hw = 0

        def schedule(split_name: str, ids: List[int]):
            nonlocal missing_images, missing_hw
            for image_id in ids:
                img = img_by_id[image_id]
                key = resolve_image_key(img.get("file_name", ""))
                if not key:
                    missing_images += 1
                    continue

                # train/test.txt에는 "{버킷경로}/{key}"로 기록
                line = f"{bucket_path}/{key.lstrip('/')}"
                if split_name == "train":
                    train_lines.append(line)
                else:
                    test_lines.append(line)

                # 라벨 key는 이미지 key의 suffix만 .txt로 변경 (이미지와 같은 폴더)
                label_key = str(Path(key).with_suffix(".txt"))
                label_text = build_yolo_label_text(image_id)
                if label_text is None:
                    missing_hw += 1
                    # 이미지 경로는 넣되 라벨 생성은 스킵
                    continue

                label_puts.append((label_key, label_text))

        schedule("train", train_ids)
        schedule("test", test_ids)

        logging.info("[%s] train.txt lines=%d, test.txt lines=%d", dataset_name, len(train_lines), len(test_lines))
        if missing_images:
            logging.warning("[%s] Unresolved images=%d (COCO file_name mismatch)", dataset_name, missing_images)
        if missing_hw:
            logging.warning("[%s] Skipped labels(no width/height)=%d", dataset_name, missing_hw)

        # 5) 업로드
        # (1) labels -> source-bucket(image-data)에 업로드 (이미지 옆)
        for label_key, label_text in label_puts:
            _put_text_object(s3, args.source_bucket, label_key, label_text, args.dry_run)

        # (2) train/test/dataset.yaml -> dest-bucket(data-pipeline)에 업로드
        _put_text_object(
            s3, args.dest_bucket, f"{dest_prefix}/train.txt", "\n".join(train_lines) + "\n", args.dry_run
        )
        _put_text_object(
            s3, args.dest_bucket, f"{dest_prefix}/test.txt", "\n".join(test_lines) + "\n", args.dry_run
        )

        if args.write_yaml:
            # path는 "로컬 디렉토리"를 쓰는 게 정석이지만,
            # 여기서는 data-pipeline에 yaml이 있으니 그 위치를 문자열로만 넣어둡니다.
            # (학습 시 yaml 파일을 로컬로 가져와서 쓰는 걸 권장)
            dataset_yaml = _build_dataset_yaml_txt(
                dest_prefix_path=f"s3://{args.dest_bucket}/{dest_prefix}",
                class_names=class_names,
                include_val=args.include_val,
            )
            _put_yaml_object(s3, args.dest_bucket, f"{dest_prefix}/dataset.yaml", dataset_yaml, args.dry_run)

        logging.info("[%s] Done. (NO images created in %s)", dataset_name, args.dest_bucket)
        logging.info("[%s] Labels were written next to images in %s bucket.", dataset_name, args.source_bucket)
        return 0

    # === batch mode ===
    if args.batch_root_prefix:
        batch_root = _normalize_prefix(args.batch_root_prefix)
        if not batch_root:
            raise ValueError("--batch-root-prefix must be a non-empty prefix.")

        base_dest_prefix = args.dest_prefix.strip("/") if args.dest_prefix else "datasets"
        category_prefixes = list(_list_common_prefixes(s3, args.source_bucket, batch_root))
        if not category_prefixes:
            category_prefixes = [batch_root]

        failures = 0
        bboxes_subdir = args.bboxes_subdir.strip("/")
        merge_suffix = (args.merge_category_suffix or "").strip()
        merge_name = (args.merge_category_name or "merged").strip()

        merge_categories = []
        if merge_suffix:
            for category in category_prefixes:
                category_name = Path(category.rstrip("/")).name or ""
                if category_name.endswith(merge_suffix):
                    merge_categories.append(category)

        for category in category_prefixes:
            category_name = Path(category.rstrip("/")).name or "category"
            if category in merge_categories:
                continue

            json_prefixes: List[str] = []

            # 1) Category itself might have bboxes/
            json_prefix = f"{category}{bboxes_subdir}/" if bboxes_subdir else category
            json_prefixes.append(json_prefix)

            # 2) Also include category/<dataset>/bboxes
            dataset_prefixes = list(_list_common_prefixes(s3, args.source_bucket, category))
            for dataset_prefix in dataset_prefixes:
                json_prefixes.append(f"{dataset_prefix}{bboxes_subdir}/" if bboxes_subdir else dataset_prefix)

            json_keys = _collect_json_keys(s3, args.source_bucket, json_prefixes, args.max_items)
            if not json_keys:
                logging.info("[%s] Skip (no JSONs found under %s).", category_name, category)
                continue

            dataset_name = category_name
            images_prefix = category
            dest_prefix = f"{base_dest_prefix}/{category_name}"
            logging.info("[%s] Processing images=%s (jsons=%d)", dataset_name, images_prefix, len(json_keys))
            rc = process_dataset(json_prefix, images_prefix, dataset_name, dest_prefix, json_keys_override=json_keys)
            if rc != 0:
                failures += 1

        if merge_categories:
            merge_json_prefixes: List[str] = []
            merge_image_prefixes: List[str] = []
            for category in merge_categories:
                merge_image_prefixes.append(category)
                json_prefix = f"{category}{bboxes_subdir}/" if bboxes_subdir else category
                merge_json_prefixes.append(json_prefix)
                dataset_prefixes = list(_list_common_prefixes(s3, args.source_bucket, category))
                for dataset_prefix in dataset_prefixes:
                    merge_json_prefixes.append(f"{dataset_prefix}{bboxes_subdir}/" if bboxes_subdir else dataset_prefix)

            merge_json_keys = _collect_json_keys(s3, args.source_bucket, merge_json_prefixes, args.max_items)
            if not merge_json_keys:
                logging.info("[merge:%s] Skip (no JSONs found).", merge_name)
            else:
                dest_prefix = f"{base_dest_prefix}/{merge_name}"
                logging.info(
                    "[merge:%s] Processing categories=%d jsons=%d",
                    merge_name,
                    len(merge_categories),
                    len(merge_json_keys),
                )
                rc = process_dataset(
                    json_prefix=merge_json_prefixes[0],
                    images_prefix=merge_image_prefixes[0],
                    dataset_name=merge_name,
                    dest_prefix=dest_prefix,
                    json_keys_override=merge_json_keys,
                    images_prefixes_override=merge_image_prefixes,
                )
                if rc != 0:
                    failures += 1

        return 1 if failures else 0

    # === single dataset mode ===
    source_prefix = _normalize_prefix(args.source_prefix)
    json_prefix = _normalize_prefix(args.json_prefix) if args.json_prefix is not None else source_prefix
    images_prefix = args.images_prefix if args.images_prefix is not None else args.assets_root_prefix
    dataset_name = args.dataset_name
    dest_prefix = args.dest_prefix.strip("/") if args.dest_prefix else f"datasets/{args.dataset_name}"

    return process_dataset(json_prefix, images_prefix, dataset_name, dest_prefix)


if __name__ == "__main__":
    raise SystemExit(main())
