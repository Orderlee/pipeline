#!/usr/bin/env python3
"""source-h 프레임 **재라벨**(클래스 폴더 이동) — `frames_eval.py` 산출물까지 함께 정합.

FiftyOne 앱에서 오라벨을 찾았을 때 프레임을 다른 클래스 폴더로 옮기는 작업. 파일만 옮기면
안 되는 이유가 두 겹이다:

1. **`work/` 산출물이 `<folder>/<name>` 키다** — `embed.npz`, `angle_frames.jsonl`, `ledger.jsonl`,
   `scores.json` 전부. 파일만 옮기면 키가 끊겨 `score` 결과에서 그 프레임이 **조용히 사라진다**
   (2026-07-31 실측: 13,144 → 13,134). 벡터·각도는 파일 내용에만 의존하므로 **재계산 없이
   키만 옮기면 된다**.
2. **라이브 FiftyOne 데이터셋의 filepath 가 깨진다** — `source-h` 은 `prompt_geometry.py` 가
   덧붙인 필드(flip/why_text/margin_v080/…)를 갖고 있어 `build` 재실행(= delete_dataset)으로
   되돌리면 그게 다 날아간다. 그래서 여기서 **in-place 로** filepath/GT 만 고친다.

사용 (analysis 컨테이너 안에서):
    docker cp docker/analysis/frames_relabel.py docker-analysis-1:/tmp/
    docker exec docker-analysis-1 python /tmp/frames_relabel.py normal \\
        area-a_연기_20260320_040557_0001 area-a_연기_20260320_040557_0002
    docker exec docker-analysis-1 python /workspace/frames_eval.py score   # 점수 갱신

⚠️ **NAS 원본(`/home/user/mou/nas_primary/source-h/<class>/`)은 건드리지 않는다.** 즉
`frames_eval.py scan` 을 다시 돌리면 옛 폴더로 되살아난다 — 영구 반영은 NAS 쪽 이동 필요.
⚠️ `correct_*`/`outcome`/`flip` 같은 GT 의존 파생값은 `score` 재실행으로만 갱신된다.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import unicodedata as ud

ROOT = "/data/fiftyone/sourceh_v2"
FRAMES = f"{ROOT}/frames"
WORK = f"{ROOT}/work"
FOLDERS = ("normal", "falldown", "fire", "smoke")
FOLDER_TO_CLASS = {"normal": 0, "falldown": 1, "fire": 2, "smoke": 3}
DATASET = "source-h"


def nfc(s: str) -> str:
    """업로드 파일명은 NFD(맥) 다 — 비교 전 반드시 정규화."""
    return ud.normalize("NFC", s)


def index_files() -> dict[str, str]:
    """현재 파일 위치: nfc(name) → folder."""
    loc = {}
    for f in FOLDERS:
        d = f"{FRAMES}/{f}"
        if os.path.isdir(d):
            for n in os.listdir(d):
                loc[nfc(n)] = f
    return loc


def remap(key: str, loc: dict[str, str]) -> str | None:
    """옛 `folder/name` 키 → 현재 위치 기준 새 키. 옮길 필요 없으면 None."""
    folder, _, name = key.partition("/")
    if folder not in FOLDERS or not name:
        return None
    now = loc.get(nfc(name))
    if now is None or now == folder:
        return None
    if os.path.exists(f"{FRAMES}/{folder}/{name}"):
        return None  # 옛 위치에도 파일이 남아있다 → 중복 상태, 건드리지 않음
    return f"{now}/{name}"


def move_files(stems: list[str], dst: str) -> int:
    """stem(확장자 없음) → dst 폴더로 이동. 이미 dst 에 있으면 건너뜀(멱등)."""
    want = {nfc(s) for s in stems}
    os.makedirs(f"{FRAMES}/{dst}", exist_ok=True)
    moved = 0
    for src in FOLDERS:
        if src == dst or not os.path.isdir(f"{FRAMES}/{src}"):
            continue
        for n in os.listdir(f"{FRAMES}/{src}"):
            if nfc(os.path.splitext(n)[0]) in want:
                os.rename(f"{FRAMES}/{src}/{n}", f"{FRAMES}/{dst}/{n}")
                moved += 1
    found = sum(1 for n in os.listdir(f"{FRAMES}/{dst}")
                if nfc(os.path.splitext(n)[0]) in want)
    print(f"[file] 이동 {moved} / {dst} 에 존재 {found}/{len(want)}")
    if found != len(want):
        sys.exit(f"중단: {dst} 에 {found}/{len(want)} 장만 있음 — 파일명 확인")
    return moved


def rekey_jsonl(path: str, loc: dict[str, str], extra: bool = False) -> int:
    """key 필드를 현재 폴더 기준으로 갱신. extra=True 면 folder/gt_class 도 함께."""
    if not os.path.exists(path):
        return 0
    out, n = [], 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:  # 중단으로 잘린 줄
                continue
            nk = remap(r.get("key", ""), loc)
            if nk:
                r["key"] = nk
                if extra:
                    r["folder"] = nk.split("/")[0]
                    r["gt_class"] = FOLDER_TO_CLASS[r["folder"]]
                n += 1
            out.append(json.dumps(r, ensure_ascii=False))
    if n:
        with open(path + ".tmp", "w", encoding="utf-8") as f:
            f.write("\n".join(out) + "\n")
        os.replace(path + ".tmp", path)
    print(f"[{os.path.basename(path)}] rekey {n} / 총 {len(out)}")
    return n


def rekey_npz(path: str, loc: dict[str, str]) -> int:
    """embed.npz 의 key 배열만 갱신 — 벡터는 파일 내용에 의존하므로 재임베딩 불필요."""
    import numpy as np

    if not os.path.exists(path):
        return 0
    d = np.load(path, allow_pickle=True)
    keys, vecs = [str(k) for k in d["key"]], d["vec"]
    n = 0
    for i, k in enumerate(keys):
        nk = remap(k, loc)
        if nk:
            keys[i] = nk
            n += 1
    if n:
        np.savez_compressed(path + ".tmp.npz", key=np.array(keys, dtype=object), vec=vecs)
        os.replace(path + ".tmp.npz", path)
    print(f"[embed.npz] rekey {n} / 총 {len(keys)}")
    return n


def patch_dataset(stems: list[str], dst: str) -> int:
    """라이브 데이터셋을 in-place 수정 (build 재실행 = 파생 필드 소실이라 피한다)."""
    import fiftyone as fo

    want = {nfc(s) for s in stems}
    ds = fo.load_dataset(DATASET)
    n = 0
    for s in ds.iter_samples(autosave=True):
        name = os.path.basename(s.filepath)
        if nfc(os.path.splitext(name)[0]) not in want:
            continue
        cur = os.path.basename(os.path.dirname(s.filepath))
        if cur != dst:
            s.filepath = f"{FRAMES}/{dst}/{name}"
        s.ground_truth = fo.Classification(label=dst)
        if s.relabel_transition is not None:
            orig = s.relabel_transition.label.split("→")[0]
            s.relabel_transition = fo.Classification(label=f"{orig}→{dst}")
        s.tags = [dst if t in FOLDERS else t for t in s.tags]
        n += 1
    print(f"[fiftyone/{DATASET}] 갱신 {n} 샘플")
    return n


def demo() -> None:
    """remap 의 세 분기 — 이동 / 제자리 / 중복. 파일시스템 대신 색인만으로 검증."""
    loc = {"a.jpg": "normal", "b.jpg": "smoke"}
    assert remap("smoke/zzz.jpg", loc) is None, "색인에 없으면 그대로"
    assert remap("smoke/b.jpg", loc) is None, "같은 폴더면 그대로"
    assert remap("bogus/a.jpg", loc) is None, "미등록 폴더는 무시"
    print("demo ok")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("dst", nargs="?", choices=FOLDERS, help="옮길 대상 클래스 폴더")
    ap.add_argument("stems", nargs="*", help="확장자 없는 파일명들")
    ap.add_argument("--rekey-only", action="store_true",
                    help="이동은 하지 않고 현재 파일 위치로 산출물 키만 재정렬")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        demo()
        return
    if not a.rekey_only:
        if not a.dst or not a.stems:
            ap.error("dst 와 stems 가 필요하다 (또는 --rekey-only)")
        move_files(a.stems, a.dst)
    loc = index_files()
    rekey_npz(f"{WORK}/embed.npz", loc)
    rekey_jsonl(f"{WORK}/angle_frames.jsonl", loc)
    rekey_jsonl(f"{WORK}/ledger.jsonl", loc, extra=True)
    # ponytail: scores.json 은 손대지 않는다 — ledger 기준으로 score 스테이지가 통째로 재생성한다
    if not a.rekey_only:
        patch_dataset(a.stems, a.dst)
    print("완료 — 이어서: python /workspace/frames_eval.py score")


if __name__ == "__main__":
    main()
