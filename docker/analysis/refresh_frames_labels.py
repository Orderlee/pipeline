"""frames 데이터셋 라벨 갱신 (2h cron) — in-place.

사람이 라벨링한 SAM3 COCO 라벨을 다시 읽어 FiftyOne `detection_class`/`detections` 필드를
재적재한다. **라벨 원본(COCO/image_labels)은 수정하지 않고 읽기만** 한다(표시용 투영).
임베딩/UMAP/미디어는 불변 → FiftyOne 앱 중단 없이 라벨만 새로고침(앱은 새로고침 시 반영).

라벨링이 진행되며 'none' → fire/smoke/person 으로 채워지는 것을 추적한다.
주의: 프레임 샘플 집합 자체는 빌드 시점 고정(frame sensor 켜서 신규 임베딩 생기면 전체 재빌드 필요).
"""

from collections import Counter

import fiftyone as fo

import fiftyone_pgvector as fp

ds = fo.load_dataset("frames")
fp.attach_labels(ds)  # 기존 샘플에 현재 라벨 재적재 (read-only on source)
dist = Counter(s.get_field("detection_class") for s in ds.select_fields(["detection_class"]))
print(f"refreshed {ds.count()} frames; detection_class={dict(dist)}", flush=True)
