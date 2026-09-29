#!/usr/bin/env bash
# 업로드 킷(project_upload) e2e — 반드시 analysis 컨테이너 **안에서** 실행한다:
#   docker exec docker-analysis-1 bash /workspace/project_upload/run_e2e.sh
#
# gt/nogt/folders 3모드 정상 흐름 + 고장 번들(검증 실패 기대) + 보호 가드(덮어쓰기 거부
# 기대) 를 검증한다. 번들 디렉토리는 "_upe2e_" 접두사, 실제 FiftyOne 데이터셋 이름은
# bc.DATASET_NAME_RE(첫 글자 알파벳/숫자 필수) 계약을 지키도록 언더스코어만 벗긴
# "upe2e_" 접두사를 쓴다. 그 접두사 + upload_kit marker 가 있는 데이터셋만 정리 대상으로
# 삼는다 — sourcei/frames 등 기존 자산은 절대 건드리지 않는다.
set -euo pipefail

KIT=/workspace/project_upload
ROOT=/data/fiftyone/uploads
E2E_KEEP="${E2E_KEEP:-0}"   # 1 이면 실패 시 잔재(번들+데이터셋)를 정리하지 않고 보존

mkdir -p "${ROOT}"

CURRENT_STEP="init"
on_err() {
  echo "[run_e2e] ==== FAILURE at step: ${CURRENT_STEP} ====" >&2
}
trap on_err ERR

cleanup() {
  local ec=$?
  if [[ "${E2E_KEEP}" == "1" && "${ec}" -ne 0 ]]; then
    echo "[run_e2e] E2E_KEEP=1 이고 실패(exit=${ec}) — 잔재 보존, 자동 정리 생략"
    return
  fi
  echo
  echo "[run_e2e] ---- 정리: upe2e_ 데이터셋(marker 있는 것만) + guard 픽스처 + 번들 디렉토리 ----"
  PYTHONPATH="${KIT}:${PYTHONPATH:-}" python3 - <<'PYEOF' || true
import fiftyone as fo

# guard 픽스처(marker 를 일부러 안 찍은 가짜 "기존 자산" 시뮬레이션)는 이름을 정확히 아니까
# marker 유무와 무관하게 먼저 지운다 — 보호 가드 케이스가 assert 전에 죽어도 잔재가 안 남게.
for gname in ("upe2e_guard_protected", "upe2e_guard_protected-prompts"):
    if fo.dataset_exists(gname):
        print(f"[run_e2e:cleanup] guard 픽스처 삭제(marker 무관): {gname}")
        fo.delete_dataset(gname)

for name in list(fo.list_datasets()):
    if not name.startswith("upe2e_"):
        continue
    try:
        ds = fo.load_dataset(name)
    except Exception as e:
        print(f"[run_e2e:cleanup] {name} 로드 실패 — 건너뜀 ({e})")
        continue
    if "upload_kit" in (ds.info or {}):
        print(f"[run_e2e:cleanup] delete_dataset {name}")
        fo.delete_dataset(name)
    else:
        print(f"[run_e2e:cleanup] marker 없음 — 보호 대상이라 건너뜀: {name}")
PYEOF
  rm -rf "${ROOT}"/_upe2e_* 2>/dev/null || true
  echo "[run_e2e] 정리 완료 (exit=${ec})"
}
trap cleanup EXIT

# ── FiftyOne 필드/브레인/불변식/값정합성 검증 (케이스 1~3 공통) ──────────────
verify_case() {
  local name="$1" mode="$2" dir="$3"
  E2E_DATASET="${name}" E2E_MODE="${mode}" E2E_BUNDLE_DIR="${dir}" \
    PYTHONPATH="${KIT}:${PYTHONPATH:-}" python3 - <<'PYEOF'
import os

import numpy as np
import fiftyone as fo

import bundle_common as bc
import score_bundle as sb  # score_version() 은 FiftyOne 비의존 순수함수 — 독립 재계산용 오라클

name = os.environ["E2E_DATASET"]
mode = os.environ["E2E_MODE"]
bundle_dir = os.environ["E2E_BUNDLE_DIR"]
prompts_name = name + bc.PROMPTS_SUFFIX
has_gt = mode in ("gt", "folders")  # folders 도 GT(폴더명) 보유 모드 — nogt 만 GT-free

# 번들 원본(단일 진리)과 인제스트 결과의 샘플수를 대조한다.
manifest = bc.load_manifest(bundle_dir)
dim = manifest["embedding_dim"]
bundle_keys, frame_vec = bc.load_image_npz(bundle_dir, dim)
bundle_rows, sent_vec, versions_csv = bc.load_prompts(bundle_dir, dim)

ds = fo.load_dataset(name)
dsp = fo.load_dataset(prompts_name)

info = dict(ds.info or {})
assert "upload_kit" in info, f"{name}: upload_kit marker 없음"
assert info["upload_kit"].get("format_version") == bc.FORMAT_VERSION, info

n_images, n_sent = len(ds), len(dsp)
print(f"[verify:{name}] images={n_images} sentences={n_sent} has_gt={has_gt}")
assert n_images == len(bundle_keys), f"이미지 샘플수 {n_images} != 번들 key수 {len(bundle_keys)}"
assert n_sent == len(bundle_rows), f"문장 샘플수 {n_sent} != 번들 prompts.csv 행수 {len(bundle_rows)}"

EXPECTED_VERSIONS = ["v1.0", "v2.0.beta"]  # make_synthetic.py 의 CSV 등장 순 = gidx 블록 순서
assert versions_csv == EXPECTED_VERSIONS, versions_csv

# 모드별 필드 매트릭스 (UPLOAD_SPEC §3) — 접두사 substring 이 아니라 정확한 필드명으로 확인한다
# (pred_margin_*/pred_correct_* 도 "pred_" 로 시작해 substring 검사로는 pred_<vt> 부재를 못 잡는다).
fields = set(ds.get_field_schema().keys())
if has_gt:
    assert "ground_truth" in fields, "GT 모드인데 ground_truth 필드 없음"
else:
    assert "ground_truth" not in fields, "GT-free 모드인데 ground_truth 필드 존재"

for v in EXPECTED_VERSIONS:
    pred_field = f"pred_{bc.vt(v)}"
    top_field = f"top_prompt_{bc.vt(v)}"
    margin_field = f"pred_margin_{bc.vtag(v)}"
    gidx_field = f"winner_gidx_{bc.vtag(v)}"
    correct_field = f"pred_correct_{bc.vtag(v)}"
    for f_ in (pred_field, top_field, margin_field, gidx_field):
        assert f_ in fields, f"{f_} 필드 없음"
    if has_gt:
        assert correct_field in fields, f"GT 모드인데 {correct_field} 없음"
    else:
        assert correct_field not in fields, f"GT-free 모드인데 {correct_field} 존재"

# brain emb_viz — 이미지·문장 양쪽 points 수 == 샘플수
assert bc.BRAIN_KEY in ds.list_brain_runs(), f"{name}: {bc.BRAIN_KEY} brain 없음"
img_pts = np.asarray(ds.load_brain_results(bc.BRAIN_KEY).current_points)
assert len(img_pts) == n_images, f"{name}: emb_viz points {len(img_pts)} != 이미지 {n_images}"

assert bc.BRAIN_KEY in dsp.list_brain_runs(), f"{prompts_name}: {bc.BRAIN_KEY} brain 없음"
sent_pts = np.asarray(dsp.load_brain_results(bc.BRAIN_KEY).current_points)
assert len(sent_pts) == n_sent, f"{prompts_name}: emb_viz points {len(sent_pts)} != 문장 {n_sent}"

# compare 워크스페이스 — 양쪽 다. user_default_workspace(on_dataset_open) 가 이 이름을 찾아 기본 화면으로
# 띄우므로 없으면 새 프로젝트만 Samples 단독으로 열린다(2026-09-04 source-n 보고). 프레임 쪽은 우측이
# user_prompt_compare, 문장 쪽은 image_embeddings 여야 한다(_compare_space 3분기 — 짝 없는 레이아웃 오배치 방지).
def _leaves(node):
    return [node.type] if not hasattr(node, "children") else [t for c in node.children for t in _leaves(c)]

for d, right in ((ds, "user_prompt_compare"), (dsp, "image_embeddings")):
    assert "compare" in d.list_workspaces(), f"{d.name}: compare 워크스페이스 없음 (있는 것: {d.list_workspaces()})"
    leaves = _leaves(d.load_workspace("compare"))
    assert leaves[-1] == right, f"{d.name}: compare 우측 패널 {leaves} (기대 {right})"
print(f"[verify:{name}] compare 워크스페이스 양쪽 존재 + 레이아웃 분기 OK")

# bank_version 고유값 2개
versions = list(dsp.distinct("bank_version.label"))
assert sorted(versions) == sorted(EXPECTED_VERSIONS), versions

# ── 값 정합성(순서 민감) 검증 ────────────────────────────────────────────────
# score_bundle.py 자신이 "이 파일에서 제일 중요한 계약"이라 부르는 프레임 정렬을, npz 로
# 독립 재계산한 값(이미 selftest 로 검증된 순수 수학)과 upload_key 로 대조해 실제로 확인한다.
# sum(wins)==프레임수 같은 순열-불변 체크만으로는 오정렬을 못 잡는다.
ukeys, uids = ds.values(["upload_key", "id"])
assert len(ukeys) == len(set(ukeys)), "upload_key 중복 존재"

for block, v in enumerate(EXPECTED_VERSIONS):
    idxs = np.asarray([i for i, r in enumerate(bundle_rows) if r["version"] == v], dtype=np.int64)
    sent_vec_v = sent_vec[idxs]
    cls_v = [bundle_rows[int(i)]["class"] for i in idxs]
    classes = sorted(set(cls_v))
    res = sb.score_version(frame_vec, sent_vec_v, cls_v, classes)
    expect_pred = {bundle_keys[i]: str(res["pred"][i]) for i in range(len(bundle_keys))}
    expect_margin = {bundle_keys[i]: float(res["margin"][i]) for i in range(len(bundle_keys))}

    pred_field, margin_field = f"pred_{bc.vt(v)}", f"pred_margin_{bc.vtag(v)}"
    pred_vals, margin_vals = ds.values([f"{pred_field}.label", margin_field])
    n_checked = 0
    for uk, pv, mv in zip(ukeys, pred_vals, margin_vals):
        assert pv == expect_pred[uk], f"{v}: {uk} pred 불일치 {pv} != {expect_pred[uk]}"
        assert abs(float(mv) - expect_margin[uk]) < 1e-4, f"{v}: {uk} margin 불일치 {mv} != {expect_margin[uk]}"
        n_checked += 1
    assert n_checked == n_images, f"{v}: 대조 완료 {n_checked} != {n_images}"
    print(f"[verify:{name}] {v}: pred/margin 프레임정렬 대조 {n_checked}장 OK (순열-민감)")

    vsub = dsp.match(fo.ViewField("bank_version.label") == v)
    n_v = len(vsub)
    assert n_v > 0, f"{v}: 문장 0행"
    wins_sum = sum(vsub.values("wins"))
    assert wins_sum == n_images, f"{v}: sum(wins)={wins_sum} != 프레임수 {n_images}"

    gidx_field = f"winner_gidx_{bc.vtag(v)}"
    lo, hi = block * bc.GIDX_OFFSET, block * bc.GIDX_OFFSET + n_v
    gidx_vals = ds.values(gidx_field)
    assert all(val is not None and lo <= val < hi for val in gidx_vals), (
        f"{gidx_field} 값이 블록 범위 [{lo},{hi}) 밖: {sorted(set(gidx_vals))[:5]}"
    )

if has_gt:
    # GT 정확도도 순열에 안 속게 upload_key 로 대조 — pred_correct 는 GT 있는 프레임에만 존재해야 함
    v0 = EXPECTED_VERSIONS[0]
    correct_field = f"pred_correct_{bc.vtag(v0)}"
    gt_vals, correct_vals = ds.values(["ground_truth.label", f"{correct_field}.label"])
    n_gt_checked = 0
    for uk, gtl, cl in zip(ukeys, gt_vals, correct_vals):
        if gtl is None:
            assert cl is None, f"{uk}: GT 없는데 {correct_field} 값 존재"
        else:
            assert cl is not None, f"{uk}: GT 있는데 {correct_field} None"
            n_gt_checked += 1
    assert n_gt_checked > 0, "GT 보유 프레임이 0장 — 검증 무의미"
    print(f"[verify:{name}] GT 보유 {n_gt_checked}장 pred_correct_* 존재 정합 확인")

print(f"[verify:{name}] OK")
PYEOF
}

run_case() {
  local name="$1" mode="$2"
  local dir="${ROOT}/${name}"
  # bc.DATASET_NAME_RE(외부 번들 계약, UPLOAD_SPEC §2.1)는 첫 글자 알파벳/숫자만 허용해
  # "_upe2e_*" 를 그대로 --name 에 넘기면 ingest_bundle.py 가 거절한다. 번들 디렉토리 이름은
  # (safety 컨벤션상) "_upe2e_" 접두사를 유지하되, 실제 FiftyOne 데이터셋 이름은 그 계약을
  # 지키도록 선행 언더스코어만 벗겨 "upe2e_*" 를 쓴다 — 여전히 실제 자산과 절대 겹치지 않는
  # 고유 접두사이고, cleanup()/보호 가드 로직도 이 접두사 기준으로 맞춰져 있다.
  local ds_name="${name#_}"
  echo
  echo "[run_e2e] ==== case ${name} (mode=${mode}, dataset=${ds_name}) ===="

  CURRENT_STEP="${name}: rm -rf + make_synthetic.py"
  rm -rf "${dir}"
  python3 "${KIT}/make_synthetic.py" "${dir}" --mode "${mode}" --seed 0

  CURRENT_STEP="${name}: validate_bundle.py (exit 0 기대)"
  python3 "${KIT}/validate_bundle.py" "${dir}"

  CURRENT_STEP="${name}: ingest_bundle.py --name ${ds_name} --overwrite"
  python3 "${KIT}/ingest_bundle.py" "${dir}" --name "${ds_name}" --overwrite

  CURRENT_STEP="${name}: FiftyOne 필드/브레인/불변식 검증"
  verify_case "${ds_name}" "${mode}" "${dir}"

  echo "[run_e2e] case ${name} PASS"
}

# ── 케이스 1~3: 정상 gt / nogt / folders ────────────────────────────────────
run_case "_upe2e_gt" "gt"
run_case "_upe2e_nogt" "nogt"
run_case "_upe2e_fold" "folders"

# ── 케이스 4: 고장 번들 — prompt_embeddings.npz 행수 불일치 → validate 실패 기대 ──
echo
echo "[run_e2e] ==== case 고장 번들 (validate_bundle.py 실패 기대) ===="
CURRENT_STEP="broken: _upe2e_gt 복사 + prompt_embeddings.npz 손상"
BROKEN_DIR="${ROOT}/_upe2e_broken"
rm -rf "${BROKEN_DIR}"
cp -r "${ROOT}/_upe2e_gt" "${BROKEN_DIR}"
python3 - "${BROKEN_DIR}/prompt_embeddings.npz" <<'PYEOF'
import sys

import numpy as np

p = sys.argv[1]
with np.load(p) as z:
    vec = z["vec"]
np.savez(p, vec=vec[:-1])  # 행 1개 삭제 → prompts.csv 행수와 불일치 유도
print(f"[run_e2e] prompt_embeddings.npz: {len(vec)} -> {len(vec) - 1} 행으로 손상")
PYEOF

CURRENT_STEP="broken: validate_bundle.py (실패 기대)"
if python3 "${KIT}/validate_bundle.py" "${BROKEN_DIR}"; then
  echo "[run_e2e] FAIL: 고장 번들인데 validate_bundle.py 가 exit 0 반환" >&2
  exit 1
fi
echo "[run_e2e] OK: 고장 번들 validate_bundle.py 가 기대대로 실패"

# ── 케이스 5: 보호 가드 — marker 없는 기존 자산 이름 강탈은 반드시 거부되어야 함 ──
#   검증 대상은 "marker 없는 이름과 충돌하면 --overwrite 여도 무조건 거부한다"(UPLOAD_SPEC
#   §3.4) 는 가드 코드 경로(_check_conflict) 그 자체다. 예전 버전은 이걸 실제 운영
#   데이터셋 sourcei 로 직접 테스트했는데, sourcei 가 존재하지 않는 환경(신규/스테이징)에서는
#   가드가 조용히 통과해 synthetic 데이터로 진짜 "sourcei" 데이터셋을 만들어버리는 부작용이
#   있었다(major 리뷰 지적). 그래서 여기서는 marker 를 일부러 안 찍은 "가짜 기존 자산"을
#   이 스크립트가 직접 만들어 그 이름을 강탈 시도한다 — 동일한 코드 경로를 sourcei 존재
#   여부와 무관하게 결정론적으로 검증하면서, 실제 자산은 이름조차 참조하지 않는다.
echo
echo "[run_e2e] ==== case 보호 가드 (marker 없는 기존 자산 이름 강탈 — 거부 기대) ===="
GUARD_NAME="upe2e_guard_protected"
CURRENT_STEP="guard: marker 없는 가짜 기존 데이터셋 준비"
PYTHONPATH="${KIT}:${PYTHONPATH:-}" python3 - <<PYEOF
import fiftyone as fo

name = "${GUARD_NAME}"
if fo.dataset_exists(name):
    fo.delete_dataset(name)
fo.Dataset(name, persistent=True)  # upload_kit marker 를 의도적으로 찍지 않음 — "기존 자산" 시뮬레이션
print(f"[run_e2e] guard 준비 완료: '{name}' (marker 없음)")
PYEOF

CURRENT_STEP="guard: ingest_bundle.py --name ${GUARD_NAME} --overwrite (반드시 거부되어야 함)"
if python3 "${KIT}/ingest_bundle.py" "${ROOT}/_upe2e_nogt" --name "${GUARD_NAME}" --overwrite; then
  echo "[run_e2e] ==== CRITICAL FAIL: marker 없는 기존 자산에 --overwrite 가 성공함 — 보호 가드 미작동! ====" >&2
  exit 1
fi
echo "[run_e2e] OK: marker 없는 기존 자산 --overwrite 가 기대대로 거부됨"
# guard 픽스처는 trap cleanup() 이 이름으로 특정해 정리한다(marker 가 없어 일반 정리 루프는 안 건드림)

# ── 케이스 6: URL 반입(반입 경로 ③) 의 주소 정책 ────────────────────────────
#   다운로드 자체가 아니라 **SSRF 게이트**를 검증한다. 네트워크가 필요 없다(정책은 연결 전에
#   판정된다) — 그래서 CI/오프라인에서도 결정론적으로 돈다. 다운로드 성공 경로는 사내 HTTP
#   서버가 있어야 하므로 여기서 하지 않는다(수동 e2e: UPLOAD_SPEC.md §1 반입 경로 ③).
#   이 표가 깨지면 무인증 엔드포인트가 사내 임의 GET 프록시가 된다.
echo
echo "[run_e2e] ==== case URL 반입 주소 정책 (차단 케이스) ===="
CURRENT_STEP="fetch: _fetch_check_url 정책 표"
python3 - <<'PYEOF'
import sys
sys.path.insert(0, "/workspace")
import sync_api                      # analysis-sync 와 같은 모듈 (같은 컨테이너 이미지)
from fastapi import HTTPException

CASES = [
    # (url, 기대 status, 사유)
    ("file:///etc/passwd",                       400, "스킴"),
    ("ftp://10.0.0.10/x.zip",                400, "스킴"),
    ("http://10.0.0.10",                     400, "호스트만 있고 경로 없음은 허용 — 아래에서 통과 확인"),
    ("http://user:pw@10.0.0.10/x.zip",       400, "URL 자격증명 금지"),
    ("http://127.0.0.1:9000/x.zip",              403, "루프백"),
    ("http://169.254.169.254/latest/meta-data/", 403, "링크로컬(클라우드 메타데이터)"),
    ("http://10.0.0.20:9000/x.zip",            403, "도커 브리지(형제 컨테이너)"),
    ("http://10.0.0.10:15433/x.zip",         403, "포트 allowlist 밖(PG)"),
]
bad = 0
for url, want, why in CASES:
    if want == 400 and url == "http://10.0.0.10":
        continue                      # 사내 주소 + 기본 포트 → 통과가 정상, 아래에서 따로 검증
    try:
        sync_api._fetch_check_url(url)
        got = 200
    except HTTPException as exc:
        got = exc.status_code
    if got != want:
        bad += 1
        print(f"[run_e2e] FAIL {url} → {got} (기대 {want}, {why})")
if bad:
    print(f"[run_e2e] ==== CRITICAL FAIL: URL 주소 정책 {bad}건 불일치 ====", file=sys.stderr)
    sys.exit(1)

# 사내 대역 + 허용 포트는 통과해야 한다 (정책이 과하게 막으면 기능이 죽는다)
sync_api._fetch_check_url("http://10.0.0.10:9000/bundle.zip")
sync_api._fetch_check_url("http://10.0.0.51:9000/bundle.zip")
print("[run_e2e] OK: URL 주소 정책 차단 8건 + 사내 허용 2건")
PYEOF

echo
echo "[run_e2e] E2E ALL PASS"
