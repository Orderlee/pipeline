# presign 갱신 파이프라인 버그 — 2026-07-22 인시던트 기록 & 수정안

## 증상 → 원인 (실측)

라벨링된 프로젝트의 미디어 미로드 (presigned URL 만료 + 구 MinIO IP). LS 이미지 교체와 무관한 `src/gemini` 기존 버그 3건의 합작:

1. **`ls_tasks_minio.py:find_or_create_project`** — `GET /api/projects/`를 페이지 파라미터 없이 호출 → **최신 30개만 검색**. 프로젝트 수가 페이지를 넘자(7/14~) 이름 조회 실패 → "없으니 생성" → **빈 중복 프로젝트 매일 자동 생성**(63개까지 증식, eng-c 토큰 명의). 이후 갱신 잡이 빈 중복만 "갱신 0건"으로 처리 → 실 프로젝트 URL 순차 만료.
2. **`fetch_existing_task_stems`** — `data.video` URL의 파일명 stem을 딕셔너리 키로 사용 → **이미지 프로젝트는 전 태스크가 빈 stem 하나로 붕괴**(1건만 잔존). 이미지 프로젝트 갱신은 애초에 동작한 적 없음.
3. **`update_task_url`** — `{"data": {"video": ..., "folder": ...}}`로 **data 전체 교체** → 이미지 태스크에 실행되면 `image` 키 유실. (7/22 태스크 83689·94947 2건 발생 → 즉시 복구 완료)
4. (부가) renew_all은 프로젝트당 토큰 재발급만 하므로 **대형 프로젝트(>5분) 처리 중 JWT access 만료** → 중간부터 401.

## 조치 완료 (2026-07-22)

- `ls_presign_renew_schedule` **STOPPED** (코드 수정 배포 전까지 유지 — 켜면 중복 재생성됨)
- 빈 중복 프로젝트 63개 삭제 (전건 태스크 0·어노테이션 0 확인 후)
- ID 기반 수동 갱신 실행: **25,482 태스크 재서명(현 MinIO 신 IP), 오류 0**. 973건은 presign 형식이 아닌 URL(프로젝트 21?·40·43·44·523 계열)이라 안전 스킵.
- 수동 갱신 도구 보존: `docker/labelstudio/tools/renew_by_id.py` — 사용:
  `docker cp docker/labelstudio/tools/renew_by_id.py docker-dagster-code-server-1:/tmp/ && docker exec docker-dagster-code-server-1 bash -c 'PYTHONPATH=/:/src/python:/src/vlm python3 /tmp/renew_by_id.py <pid[,pid...]>'`
  (id 기반 조회·data 병합 패치·400건마다 토큰 재발급 — 위 버그 3종 회피)

## 근본 수정안 (src/gemini — 정식 git 경로로 반영 필요)

```diff
--- a/src/gemini/ls_tasks_minio.py
+++ b/src/gemini/ls_tasks_minio.py
@@ def find_or_create_project(
-    resp = requests.get(f"{ls_url}/api/projects/", headers=headers)
+    # 전체 프로젝트에서 이름 검색 (기본 페이지네이션 30개 → 오래된 프로젝트 미검색 → 중복 생성 버그)
+    resp = requests.get(f"{ls_url}/api/projects/", headers=headers, params={"page_size": 1000})

@@ def fetch_existing_task_stems(
-        for task in tasks:
-            video_url = task.get("data", {}).get("video", "")
-            stem = Path(urlparse(video_url).path).stem
-            index[stem] = task
+        for task in tasks:
+            media_url = task.get("data", {}).get("video", "") or task.get("data", {}).get("image", "")
+            stem = Path(urlparse(media_url).path).stem
+            # 이미지 프로젝트에서 빈 stem 으로 전 태스크가 붕괴하던 버그 — task id 를 fallback 키로
+            index[stem or f"__task_{task['id']}"] = task

@@ def update_task_url(
-def update_task_url(ls_url: str, headers: dict, task_id: int, new_url: str, folder: str) -> None:
-    resp = requests.patch(
-        f"{ls_url}/api/tasks/{task_id}/",
-        headers={**headers, "Content-Type": "application/json"},
-        json={"data": {"video": new_url, "folder": folder}},
-    )
+def update_task_url(ls_url: str, headers: dict, task: dict, new_url: str) -> None:
+    # data 전체 교체 금지 — 기존 키 보존, 미디어 키만 갱신 (image 태스크에서 image 키 유실 버그)
+    data = dict(task.get("data") or {})
+    media_key = "video" if data.get("video") else "image"
+    data[media_key] = new_url
+    resp = requests.patch(
+        f"{ls_url}/api/tasks/{task['id']}/",
+        headers={**headers, "Content-Type": "application/json"},
+        json={"data": data},
+    )
```

(+ `cmd_renew` 호출부를 새 시그니처에 맞추고, 태스크 400건마다 `resolve_auth_headers` 재발급 추가 — `renew_by_id.py` 참조 구현.)

수정 머지·배포(이미지 리빌드 포함) 후 `ls_presign_renew_schedule` 재시작할 것. 테스트: `tests/unit`에 이미지 프로젝트 renew 케이스 추가 권장.

---

## 추가 발견 (2026-07-23): LS_MINIO_ENDPOINT 미설정 — 구 IP 폴백 인시던트

**증상**: Dagster 로그 `목록 조회 실패 (vlm-labels/...): Connect timeout ... http://10.0.0.51:9000` + LS 태스크 생성 실패로 빈 프로젝트 생성 (ppe_detection_vest id 815, source-g id 765 등).

**원인**: 2026-07-06 MinIO IP 개편 때 prod `docker/.env`에 `MINIO_ENDPOINT`만 갱신되고 **`LS_MINIO_ENDPOINT`는 추가되지 않음**. 그런데:
- `lib/minio_cross_sync.py`: `_DEFAULT_PRODUCTION_ENDPOINT = "http://10.0.0.51:9000"` (구 IP 하드코딩) — `LS_MINIO_ENDPOINT` 미설정 시 이 값 사용. `is_cross_sync_needed()`가 "현재(신IP) ≠ LS용(구IP)"로 오판 → prod에서 불필요한 교차동기화 시도 → 타임아웃.
- `defs/ls/sensor.py:216,360`: `--minio-endpoint`로 **이 구 IP를 create/renew에 전달** → 신규 태스크 URL이 구 IP로 서명(접근 불가), 일일 renew도 구 IP로 서명해 옴 (7/13 URL이 구 IP였던 이유).

**조치 (2026-07-23)**: prod `docker/.env`에 `LS_MINIO_ENDPOINT=http://10.0.0.51:9000` 추가 + dagster 3형제 recreate → `is_cross_sync_needed()=False` 확인. 실패 디스패치는 `ls_task_status='pending'` 리셋으로 재시도.

**남은 항목**:
- 코드 레벨: 구 IP 하드코딩 기본값 2곳(`minio_cross_sync.py:22`, gemini 모듈들의 `DEFAULT_MINIO_ENDPOINT`) 제거 또는 fail-loud로 — 별도 PR 후보.
- staging 가동 시 `.env.test`에도 `LS_MINIO_ENDPOINT`(prod 신 IP :9000) 필요 — staging은 진짜 cross-sync(:9002→:9000)를 쓰므로 필수.
- `source-g` 디스패치(`eng-c-20260715-source-g-bbox-02`, failed)도 동일 원인 — 재라벨링 원하면 pending 리셋으로 재시도 가능 (빈 프로젝트 765는 그 후 정리).
