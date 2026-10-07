"""기존 'frames' 데이터셋으로 FiftyOne 앱만 재기동(빌드 없음) + keep-alive.

이미 빌드된 데이터셋(예: 전체 188K)을 앱에 띄울 때 사용. 텍스트검색 인덱스는 별도(무거움).
"""

import os
import time

# ⚠️ import fiftyone 전에 설정해야 한다 (fo.config 는 import 시점에 굳는다).
# 기본 false 는 **오퍼레이터 요청마다 플러그인 모듈을 재임포트** — user-prompt-compare 의
# 603k행 번들 _CACHE·변경 dedup 가드(_APPLIED)가 요청마다 증발해 드롭다운 한 번에
# 왕복 20초+ 가 됐다 (2026-08-14 실측). true 여도 플러그인 파일이 바뀌면 dir_state 로
# 자동 무효화되므로(fiftyone/operators/decorators.py plugins_cache) docker cp 후
# App 재기동 없이도 새 코드가 잡힌다 — 켜서 잃는 것이 없다.
os.environ.setdefault("FIFTYONE_PLUGINS_CACHE_ENABLED", "true")

import fiftyone as fo

# ── 좌석 점유 카운터 (2026-09-03) ─────────────────────────────────────────────
# 좌석 라우터의 자동 배정기(analysis-sync `/seat/assign`)가 "이 좌석에 붙은 브라우저가 있나"를
# 물어보는 곳. FiftyOne 서버(:5151, launch_app 이 띄우는 별도 자식 프로세스)와 같은 netns 라
# /proc/net/tcp 에서 :5151 로 들어온 ESTABLISHED 연결을 셀 수 있다 — 탭마다 SSE 1개가 상주하므로
# 0 이면 빈 좌석이다. 루프백(런처 Session·healthcheck curl)은 제외. 실패해도 앱 기동을 막지 않는다.
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

APP_PORT = 5151
OCC_PORT = int(os.getenv("SEAT_OCCUPANCY_PORT", "5160"))


def _is_loopback_hex(addr_hex: str) -> bool:
    # /proc/net/tcp 는 IPv4 를 리틀엔디언 8자리 hex 로 적는다 — 127.x.x.x 는 끝 두 자리가 7F.
    # tcp6 는 32비트 워드 4개(각 리틀엔디언): 순수 ::1 은 고정 문자열, IPv4-mapped(::ffff:a.b.c.d)는
    # 3번째 워드가 FFFF0000 이고 마지막 워드가 v4 주소. 네이티브 IPv6 원격에 v4 규칙을 적용하면
    # 13번째 바이트가 0x7F 인 정상 클라이언트가 1/256 확률로 루프백으로 오판된다 — 그래서 mapped 만.
    if len(addr_hex) == 8:
        return addr_hex[-2:] == "7F"
    if addr_hex == "00000000000000000000000001000000":
        return True
    return addr_hex[16:24] == "FFFF0000" and addr_hex[-2:] == "7F"


def _app_accepting() -> bool:
    """FiftyOne 서버(:5151)가 실제로 연결을 받는가. 루프백 연결이라 카운트엔 안 잡힌다."""
    try:
        with socket.create_connection(("127.0.0.1", APP_PORT), timeout=0.5):
            return True
    except OSError:
        return False


def _established_clients() -> int:
    n = 0
    for path in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(path) as f:
                lines = f.readlines()[1:]
        except OSError:
            continue
        for ln in lines:
            parts = ln.split()
            if len(parts) < 4 or parts[3] != "01":  # 01 = ESTABLISHED
                continue
            laddr, raddr = parts[1], parts[2]
            if int(laddr.rsplit(":", 1)[1], 16) != APP_PORT:
                continue
            if _is_loopback_hex(raddr.rsplit(":", 1)[0]):
                continue
            n += 1
    return n


class _OccupancyHandler(BaseHTTPRequestHandler):
    timeout = 3  # 요청줄을 안 보내는 연결이 핸들러 스레드를 붙잡지 못하게 (소켓 타임아웃)

    def do_GET(self):  # noqa: N802 — http.server 규약
        # 앱이 아직 listen 전이면 503 — 배정기는 이 좌석을 '상태 불명'으로 보고 건너뛴다.
        # 0 을 돌려주면 "완전히 빈 좌석"으로 최우선 배정돼 첫 사용자가 502 + 1년 쿠키에 박힌다.
        if not _app_accepting():
            body = json.dumps({"connections": None, "port": APP_PORT, "error": "app not accepting"}).encode()
            self.send_response(503)
        else:
            body = json.dumps({"connections": _established_clients(), "port": APP_PORT}).encode()
            self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):  # 배정기 폴링이 stdout 을 채우지 않게
        pass


def _start_occupancy_server() -> None:
    # 이 함수의 어떤 실패도 앱 기동을 막으면 안 된다 (PID 1) — 바인드·스레드 시작 전부 가드.
    try:
        srv = ThreadingHTTPServer(("0.0.0.0", OCC_PORT), _OccupancyHandler)
        srv.daemon_threads = True
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        print(f"occupancy server on :{OCC_PORT}", flush=True)
    except Exception as e:  # noqa: BLE001 — 포트 충돌/스레드 한도 등: 자동 배정만 이 좌석을 '상태 불명'으로 본다
        print(f"occupancy server disabled: {e}", flush=True)


# 어느 데이터셋을 띄울지 env 로 지정 (App 드롭다운에서 다른 데이터셋으로 전환은 언제든 가능).
# 하드코딩이면 통합 데이터셋을 띄울 때마다 스크립트를 고쳐야 했다.
DATASET = os.getenv("FO_DATASET", "frames")
ds = fo.load_dataset(DATASET)
print(f"loaded {DATASET} n={ds.count()} brain={ds.list_brain_runs()}", flush=True)
fo.launch_app(ds, address="0.0.0.0", port=5151)
# 카운터는 앱이 뜬 뒤에 연다 (핸들러의 :5151 준비 확인과 이중 안전) — 기동 중 "빈 좌석" 광고 방지.
_start_occupancy_server()
print("APP_LAUNCHED", flush=True)
# HTTP 는 resolve_input 동안 14~19초 멎을 수 있다 — 이벤트 루프와 무관한 TCP 연결만 확인한다.
app_failures = 0
while True:
    time.sleep(20)
    app_failures = 0 if _app_accepting() else app_failures + 1
    if app_failures >= 3:
        print("App listener unavailable for 3 checks — exiting for container restart", flush=True)
        raise SystemExit(1)
