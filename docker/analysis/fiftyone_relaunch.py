"""기존 'frames' 데이터셋으로 FiftyOne 앱만 재기동(빌드 없음) + keep-alive.

이미 빌드된 데이터셋(예: 전체 188K)을 앱에 띄울 때 사용. 텍스트검색 인덱스는 별도(무거움).
"""

import time

import fiftyone as fo

ds = fo.load_dataset("frames")
print(f"loaded frames n={ds.count()} brain={ds.list_brain_runs()}", flush=True)
fo.launch_app(ds, address="0.0.0.0", port=5151)
print("APP_LAUNCHED", flush=True)
time.sleep(10 ** 9)
