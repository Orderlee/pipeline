"""기존 'frames' 데이터셋으로 FiftyOne 앱만 재기동(빌드 없음) + keep-alive.

이미 빌드된 데이터셋(예: 전체 188K)을 앱에 띄울 때 사용. 텍스트검색 인덱스는 별도(무거움).
"""

import os
import time

import fiftyone as fo

# 어느 데이터셋을 띄울지 env 로 지정 (App 드롭다운에서 다른 데이터셋으로 전환은 언제든 가능).
# 하드코딩이면 통합 데이터셋을 띄울 때마다 스크립트를 고쳐야 했다.
DATASET = os.getenv("FO_DATASET", "frames")
ds = fo.load_dataset(DATASET)
print(f"loaded {DATASET} n={ds.count()} brain={ds.list_brain_runs()}", flush=True)
fo.launch_app(ds, address="0.0.0.0", port=5151)
print("APP_LAUNCHED", flush=True)
time.sleep(10 ** 9)
