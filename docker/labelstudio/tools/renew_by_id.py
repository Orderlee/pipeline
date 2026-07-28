# LS presigned URL 갱신 (ID 기반) — src/gemini 갱신 잡의 버그 3종 회피 임시 도구.
#   버그·배경: docs/design-docs/gcpls/presign-renew-bugfix.md (정식 수정 배포 후 이 도구·크론 제거)
# 사용 (dagster-code-server 컨테이너 안에서):
#   python3 renew_by_id.py all              # 전체 프로젝트, 만료 48h 이내만 재서명 (크론용)
#   python3 renew_by_id.py 680,690          # 지정 프로젝트만
#   python3 renew_by_id.py all 691200       # threshold(초) 지정 — 8일(691200)이면 전량 재서명
import os
import sys
import requests

sys.path.insert(0, "/src/vlm")
from gemini.ls_tasks_minio import (  # noqa: E402
    DEFAULT_PRESIGN_EXPIRES,
    build_minio_client,
    generate_presigned_url,
    is_url_expiring,
)
from gemini.ls_tasks import resolve_auth_headers  # noqa: E402
from urllib.parse import urlparse  # noqa: E402

LS = os.environ["LS_URL"]
KEY = os.environ["LS_API_KEY"]
minio = build_minio_client(os.environ["MINIO_ENDPOINT"], os.environ["MINIO_ACCESS_KEY"], os.environ["MINIO_SECRET_KEY"])
THRESH = int(sys.argv[2]) if len(sys.argv) > 2 else 48 * 3600  # 기본: 만료 48시간 이내만

h = resolve_auth_headers(LS, KEY)
if sys.argv[1] == "all":
    r = requests.get(f"{LS}/api/projects/", headers=h, params={"page_size": 1000}, timeout=30)
    r.raise_for_status()
    data = r.json()
    pids = [p["id"] for p in (data.get("results", data) if isinstance(data, dict) else data)]
else:
    pids = [int(x) for x in sys.argv[1].split(",")]

grand_r = grand_s = grand_e = 0
for pid in pids:
    h = resolve_auth_headers(LS, KEY)
    ren = sk = er = 0
    page = 1
    tasks_all = []
    while True:
        r = requests.get(f"{LS}/api/tasks/", headers=h, params={"project": pid, "page": page, "page_size": 500}, timeout=60)
        r.raise_for_status()
        data = r.json()
        ts = data if isinstance(data, list) else data.get("tasks", [])
        if not ts:
            break
        tasks_all.extend(ts)
        if isinstance(data, list) or len(ts) < 500:
            break
        page += 1
    for task in tasks_all:
        d = dict(task.get("data") or {})
        media_key = "video" if d.get("video") else ("image" if d.get("image") else None)
        if not media_key:
            sk += 1
            continue
        murl = d[media_key]
        if not is_url_expiring(murl, THRESH):
            sk += 1
            continue
        parts = urlparse(murl).path.lstrip("/").split("/", 1)
        if len(parts) < 2:
            sk += 1
            continue
        try:
            if (ren + er) % 400 == 0:
                h = resolve_auth_headers(LS, KEY)  # JWT access(~5분) 수명 대비 주기 재발급
            new_url = generate_presigned_url(minio, parts[0], parts[1], DEFAULT_PRESIGN_EXPIRES)
            d[media_key] = new_url  # 기존 data 키 전부 보존, 미디어 키만 교체
            resp = requests.patch(
                f"{LS}/api/tasks/{task['id']}/",
                headers={**h, "Content-Type": "application/json"}, json={"data": d}, timeout=30,
            )
            if resp.status_code == 401:
                h = resolve_auth_headers(LS, KEY)
                resp = requests.patch(
                    f"{LS}/api/tasks/{task['id']}/",
                    headers={**h, "Content-Type": "application/json"}, json={"data": d}, timeout=30,
                )
            resp.raise_for_status()
            ren += 1
        except Exception as e:
            er += 1
            if er <= 3:
                print(f"  [ERR] task {task.get('id')}: {e}", flush=True)
    grand_r += ren
    grand_s += sk
    grand_e += er
    print(f"[project {pid}] 갱신 {ren} / 스킵 {sk} / 오류 {er}", flush=True)
print(f"[TOTAL] 갱신 {grand_r} / 스킵 {grand_s} / 오류 {grand_e}")
