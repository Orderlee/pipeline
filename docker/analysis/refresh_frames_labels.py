"""frames 데이터셋 라벨 갱신 (2h cron) — in-place.

사람이 라벨링한 SAM3 COCO 라벨을 다시 읽어 FiftyOne `detection_class`/`detections` 필드를
재적재한다. **라벨 원본(COCO/image_labels)은 수정하지 않고 읽기만** 한다(표시용 투영).
임베딩/UMAP/미디어는 불변 → FiftyOne 앱 중단 없이 라벨만 새로고침(앱은 새로고침 시 반영).

라벨링이 진행되며 'none' → fire/smoke/person 으로 채워지는 것을 추적한다.
주의: 프레임 샘플 집합 자체는 빌드 시점 고정(frame sensor 켜서 신규 임베딩 생기면 전체 재빌드 필요).

## 2026-08-19 개명 이후 (이 스크립트가 다시 살아난다)

`fo.load_dataset("frames")` 는 여태 **부재 데이터셋**이라 매 tick 크래시했다. 개명
(frames_captions → frames)으로 정본을 가리키게 되면서 동작이 재개되는데, 정본은 옛 'frames'
(프레임 5,000장)와 달리 **프레임 187,994 + 캡션 11,978 = 199,972 혼합 모달리티**다.
그래서 두 가지를 여기서 다룬다:

  1. **모달리티** — `attach_labels()` 는 프레임 전용 필드(detection_class/normalized_class/
     daynight/environment/project)를 쓴다. 그 함수 안에 image_id 부재 샘플 skip 가드가
     들어갔고(전 호출자 보호), 여기서는 한 겹 더 얹어 **애초에 캡션을 뷰에서 빼** 불필요한
     문서 로드/조회 자체를 없앤다. 두 겹인 이유: 뷰 필터는 `modality` 필드에 의존하는데
     그 필드가 없는 데이터셋에서도 정확성이 무너지면 안 되기 때문이다.
  2. **규모** — `attach_labels()` 는 `list(ds)` 로 **전 문서를 한 번에** 올린다. 임베딩이
     1024-d list[float](행당 ~32KB)라 200k 를 통째로 잡으면 6~12GB 다 (같은 함정으로
     `fiftyone_full_build.py` 가 배치 빌더로 갈아엎힌 이력, 그 파일 상단 주석 참조).
     호스트는 62.5GB 공유 + oom_kill 이력이 있어 2h 마다 이걸 반복하면 위험하다.
     → 여기서 **id 배치로 잘라** attach_labels 를 여러 번 호출한다. 한 번에 메모리에 있는
     문서는 CHUNK 개뿐이고, 함수 자체는 손대지 않아 다른 호출자와 동작이 갈리지 않는다.

⚠️ 남은 비용(이번 범위 밖): `attach_labels()` 내부는 여전히 **샘플당 `save()` + SAM3 JSON
   순차 MinIO GET** 이다. 배치+병렬 버전은 `fiftyone_full_build.py:attach_labels_batched()`
   에 이미 있지만 그 파일은 top-level 빌드 스크립트라 import 가 불가능하다(공용 모듈로
   올리는 게 다음 단계). 2h 주기를 못 지키면 그때는 여기서 재실행 간격을 늘리거나 그
   배치 구현을 `fiftyone_pgvector` 로 승격할 것. cron 중첩 자체는 호스트 crontab 의 flock
   이 막고 있다(2026-07-06 3중 중첩 사건의 수습).
"""

import os
from collections import Counter

import fiftyone as fo
from fiftyone import ViewField as F

import fiftyone_pgvector as fp

CHUNK = int(os.getenv("RFL_CHUNK", "5000"))

ds = fo.load_dataset("frames")

# 캡션 모달리티 제외 — 프레임 전용 라벨 투영이다. `modality` 필드가 없는(개명 전 스키마의)
# 데이터셋에서는 전체를 대상으로 두고, 정확성은 attach_labels 의 image_id 가드에 맡긴다.
if "modality" in ds.get_field_schema():
    target = ds.match(F("modality") == "frame")
else:
    target = ds
ids = target.values("id")
print(f"target frames={len(ids)} / dataset={ds.count()} chunk={CHUNK}", flush=True)

for i in range(0, len(ids), CHUNK):
    batch = ids[i : i + CHUNK]
    fp.attach_labels(ds.select(batch))  # 기존 샘플에 현재 라벨 재적재 (read-only on source)
    print(f"  labels {min(i + CHUNK, len(ids))}/{len(ids)}", flush=True)

dist = Counter(target.select_fields(["detection_class"]).values("detection_class"))
print(f"refreshed {len(ids)} frames; detection_class={dict(dist)}", flush=True)
