#!/usr/bin/env python3
"""One-time prod SAM3 COCO category unification (vlm-labels/**/sam3_segmentations/*.json).

Remaps category NAMES per-file (id-agnostic — taxonomy varies per dispatch), merging
duplicate categories that collapse onto the same canonical name:

    flame, open flame      -> fire
    person lying down      -> fallen person
    person on the ground   -> person
    smoke cloud            -> smoke

Names not listed are left untouched. Idempotent: a second run rewrites nothing.

Modes:
    --selftest         run local asserts on synthetic docs, no network
    --dry-run          (default) scan + report, no writes
    --apply            back up each changed original then overwrite in MinIO
Options:
    --limit N          stop after N matching objects (sampling)
    --prefix P         only keys starting with P (canary on one source dir)
    --workers N        thread pool size (default 32)
    --backup-dir DIR   apply-mode backup root (default /data/fiftyone/_coco_unify_backup)
"""
from __future__ import annotations

import argparse
import json
import os
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

CANON: dict[str, str] = {
    "flame": "fire",
    "open flame": "fire",
    "person lying down": "fallen person",
    "person on the ground": "person",
    "smoke cloud": "smoke",
    "cigarette": "smoking",
}
BUCKET = "vlm-labels"
SEG_MARK = "sam3_segmentations/"
BACKUP_PREFIX = "_unify_backup/"   # server-side copy target; excluded from scan
_NAME_FIELDS = ("category", "category_name", "name", "label", "class", "class_name", "prompt_class")


def canon(name):
    """Remap a category name; non-sources returned unchanged (original text preserved)."""
    if not isinstance(name, str):
        return name
    key = name.strip().casefold()
    if not key:
        return name
    return CANON.get(key, name)


def transform(doc):
    """(new_doc, changed, remap_counter). Remap by name; merge dup categories keeping first-seen id."""
    rc: Counter = Counter()
    if not isinstance(doc, dict):
        return doc, False, rc
    changed = False
    doc = dict(doc)

    cats = doc.get("categories")
    oldid_to_newid: dict = {}
    if isinstance(cats, list) and cats:
        keeper_id_for_name: dict = {}   # canonical name -> id of first category that canon's to it
        new_cats: list = []
        oldid_to_oldname: dict = {}
        for c in cats:
            if not isinstance(c, dict):
                new_cats.append(c)
                continue
            oid = c.get("id")
            oname = c.get("name")
            oldid_to_oldname[oid] = oname
            cname = canon(oname)
            if cname not in keeper_id_for_name:
                keeper_id_for_name[cname] = oid
                nc = dict(c)
                nc["name"] = cname
                new_cats.append(nc)            # keep the keeper's original id
                if cname != oname:
                    changed = True
            else:
                changed = True                  # this category merges away (dropped)
            keep_id = keeper_id_for_name[cname]
            if oid is not None:
                oldid_to_newid[oid] = keep_id
                oldid_to_newid[str(oid)] = keep_id
        doc["categories"] = new_cats

        anns = doc.get("annotations")
        if isinstance(anns, list):
            new_anns = []
            for a in anns:
                if not isinstance(a, dict):
                    new_anns.append(a)
                    continue
                na = dict(a)
                cid = na.get("category_id")
                if cid in oldid_to_newid or str(cid) in oldid_to_newid:
                    new_cid = oldid_to_newid.get(cid, oldid_to_newid.get(str(cid)))
                    oldname = oldid_to_oldname.get(cid, oldid_to_oldname.get(str(cid)))
                    if canon(oldname) != oldname:
                        rc[f"{oldname} -> {canon(oldname)}"] += 1
                    if new_cid != cid:
                        na["category_id"] = new_cid
                        changed = True
                for f in _NAME_FIELDS:
                    if isinstance(na.get(f), str):
                        cv = canon(na[f])
                        if cv != na[f]:
                            na[f] = cv
                            changed = True
                new_anns.append(na)
            doc["annotations"] = new_anns

    # raw SAM3 detections[] shape (prompt_class etc.), when present
    dets = doc.get("detections")
    if isinstance(dets, list):
        new_dets = []
        for d in dets:
            if not isinstance(d, dict):
                new_dets.append(d)
                continue
            nd = dict(d)
            for f in _NAME_FIELDS:
                if isinstance(nd.get(f), str):
                    cv = canon(nd[f])
                    if cv != nd[f]:
                        if f in ("prompt_class", "category", "label", "class", "class_name", "name"):
                            rc[f"{nd[f]} -> {cv}"] += 1
                        nd[f] = cv
                        changed = True
            new_dets.append(nd)
        doc["detections"] = new_dets

    return doc, changed, rc


# ───────────────────────── selftest ─────────────────────────
def selftest():
    # 1) id-varying taxonomy + merge fire group + fall/person split
    doc = {
        "categories": [
            {"id": 1, "name": "fire", "supercategory": "object"},
            {"id": 2, "name": "flame", "supercategory": "object"},
            {"id": 3, "name": "open flame", "supercategory": "object"},
            {"id": 6, "name": "fallen person"},
            {"id": 7, "name": "person lying down"},
            {"id": 8, "name": "person on the ground"},
            {"id": 9, "name": "person"},
            {"id": 4, "name": "smoke"},
            {"id": 5, "name": "smoke cloud"},
        ],
        "annotations": [
            {"id": 1, "category_id": 2, "bbox": [0, 0, 1, 1]},   # flame -> fire(1)
            {"id": 2, "category_id": 3, "bbox": [0, 0, 1, 1]},   # open flame -> fire(1)
            {"id": 3, "category_id": 7, "bbox": [0, 0, 1, 1]},   # person lying down -> fallen person(6)
            {"id": 4, "category_id": 8, "bbox": [0, 0, 1, 1]},   # person on the ground -> person(9)
            {"id": 5, "category_id": 5, "bbox": [0, 0, 1, 1]},   # smoke cloud -> smoke(4)
            {"id": 6, "category_id": 1, "bbox": [0, 0, 1, 1]},   # fire stays
        ],
    }
    out, changed, rc = transform(doc)
    assert changed
    id2name = {c["id"]: c["name"] for c in out["categories"]}
    # surviving canonical category names (dup categories merged away)
    assert sorted({c["name"] for c in out["categories"]}) == ["fallen person", "fire", "person", "smoke"]
    # invariant that matters: each annotation now RESOLVES to the right canonical name (keeper id may vary)
    resolved = {x["id"]: id2name[x["category_id"]] for x in out["annotations"]}
    assert resolved[1] == "fire" and resolved[2] == "fire" and resolved[6] == "fire", resolved
    assert resolved[3] == "fallen person", resolved        # person lying down
    assert resolved[4] == "person", resolved               # person on the ground
    assert resolved[5] == "smoke", resolved                # smoke cloud
    assert rc["flame -> fire"] == 1 and rc["open flame -> fire"] == 1
    assert rc["person lying down -> fallen person"] == 1
    assert rc["person on the ground -> person"] == 1
    assert rc["smoke cloud -> smoke"] == 1

    # 2) file requesting ONLY flame (no pre-existing fire) -> renamed in place, id kept
    doc2 = {"categories": [{"id": 1, "name": "flame"}], "annotations": [{"id": 1, "category_id": 1, "bbox": [0, 0, 1, 1]}]}
    out2, ch2, _ = transform(doc2)
    assert ch2 and out2["categories"] == [{"id": 1, "name": "fire"}], out2
    assert out2["annotations"][0]["category_id"] == 1

    # 3) already canonical -> no change (idempotent)
    out3, ch3, _ = transform(out)
    assert not ch3, "second pass must be a no-op"

    # 4) nothing to remap (person + fire only, sequential ids) -> unchanged
    doc4 = {"categories": [{"id": 1, "name": "person"}, {"id": 2, "name": "fire"}], "annotations": [{"category_id": 2}]}
    out4, ch4, _ = transform(doc4)
    assert not ch4, out4

    # 5) raw detections[] shape with prompt_class
    doc5 = {"detections": [{"prompt_class": "open flame", "score": 0.9}, {"prompt_class": "person on the ground"}]}
    out5, ch5, rc5 = transform(doc5)
    assert ch5
    assert out5["detections"][0]["prompt_class"] == "fire"
    assert out5["detections"][1]["prompt_class"] == "person"

    # 6) case-insensitive + whitespace
    assert canon(" Open Flame ") == "fire"
    assert canon("PERSON ON THE GROUND") == "person"
    assert canon("dog") == "dog"           # untouched

    # 7) cigarette -> smoking; smoking stays (target, not a source)
    assert canon("cigarette") == "smoking"
    assert canon("smoking") == "smoking"
    doc7 = {
        "categories": [{"id": 1, "name": "cigarette"}, {"id": 2, "name": "smoking"}, {"id": 3, "name": "smoke"}],
        "annotations": [{"id": 1, "category_id": 1}, {"id": 2, "category_id": 2}, {"id": 3, "category_id": 3}],
    }
    out7, ch7, rc7 = transform(doc7)
    id2name7 = {c["id"]: c["name"] for c in out7["categories"]}
    resolved7 = {x["id"]: id2name7[x["category_id"]] for x in out7["annotations"]}
    assert ch7
    assert resolved7[1] == "smoking" and resolved7[2] == "smoking", resolved7  # cigarette+smoking -> smoking
    assert resolved7[3] == "smoke", resolved7                                   # smoke untouched (distinct group)
    assert rc7["cigarette -> smoking"] == 1
    print("selftest OK")


# ───────────────────────── prod scan ─────────────────────────
def _client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


def _iter_keys(client, prefix, limit):
    paginator = client.get_paginator("list_objects_v2")
    n = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix or ""):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if key.startswith(BACKUP_PREFIX):
                continue
            if SEG_MARK in key and key.endswith(".json"):
                yield key
                n += 1
                if limit and n >= limit:
                    return


def run(args):
    client = _client()
    apply = args.apply
    backup_root = None
    if apply:
        backup_root = BACKUP_PREFIX + time.strftime("%Y%m%d-%H%M%S") + "/"
        print(f"APPLY mode — server-side backups -> s3://{BUCKET}/{backup_root}", flush=True)
    else:
        print("DRY-RUN — no writes", flush=True)

    lock = threading.Lock()
    stats = {"scanned": 0, "changed": 0, "written": 0, "errors": 0}
    remap_total: Counter = Counter()
    cat_names_seen: Counter = Counter()      # every distinct category name across files (inventory)
    examples: list = []

    def handle(key):
        try:
            body = client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            doc = json.loads(body)
            for c in (doc.get("categories") or []):
                if isinstance(c, dict) and isinstance(c.get("name"), str):
                    with lock:
                        cat_names_seen[c["name"]] += 1
            new_doc, changed, rc = transform(doc)
            wrote = False
            if changed and apply:
                client.copy_object(
                    Bucket=BUCKET, Key=backup_root + key,
                    CopySource={"Bucket": BUCKET, "Key": key},
                )
                client.put_object(
                    Bucket=BUCKET, Key=key,
                    Body=json.dumps(new_doc, ensure_ascii=False).encode("utf-8"),
                    ContentType="application/json",
                )
                wrote = True
            with lock:
                stats["scanned"] += 1
                if changed:
                    stats["changed"] += 1
                    remap_total.update(rc)
                    if len(examples) < 8:
                        examples.append(key)
                if wrote:
                    stats["written"] += 1
                if stats["scanned"] % 5000 == 0:
                    print(f"  ...{stats['scanned']} scanned, {stats['changed']} changed", flush=True)
        except Exception as exc:  # noqa: BLE001 — per-object fail-forward
            with lock:
                stats["errors"] += 1
                if stats["errors"] <= 10:
                    print(f"  ERR {key}: {exc}", flush=True)

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(handle, k) for k in _iter_keys(client, args.prefix, args.limit)]
        for _ in as_completed(futs):
            pass

    dt = time.time() - t0
    print("\n=== RESULT ===", flush=True)
    print(f"scanned={stats['scanned']} changed={stats['changed']} written={stats['written']} "
          f"errors={stats['errors']} in {dt:.1f}s", flush=True)
    print("\n--- distinct category names present (count = files containing) ---")
    for name, c in cat_names_seen.most_common():
        mark = f"  ==> {canon(name)}" if canon(name) != name else ""
        print(f"  {c:>8}  {name!r}{mark}")
    print("\n--- annotation/detection remaps that (would) apply ---")
    for k, c in remap_total.most_common():
        print(f"  {c:>8}  {k}")
    if examples:
        print("\n--- example changed keys ---")
        for k in examples:
            print(f"  {k}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--prefix", default="")
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--backup-dir", default="/data/fiftyone/_coco_unify_backup")
    args = ap.parse_args()
    if args.selftest:
        selftest()
        return
    run(args)


if __name__ == "__main__":
    main()
