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

# 어느 데이터셋을 띄울지 env 로 지정 (App 드롭다운에서 다른 데이터셋으로 전환은 언제든 가능).
# 하드코딩이면 통합 데이터셋을 띄울 때마다 스크립트를 고쳐야 했다.
DATASET = os.getenv("FO_DATASET", "frames")
ds = fo.load_dataset(DATASET)
print(f"loaded {DATASET} n={ds.count()} brain={ds.list_brain_runs()}", flush=True)
fo.launch_app(ds, address="0.0.0.0", port=5151)
print("APP_LAUNCHED", flush=True)
time.sleep(10 ** 9)
