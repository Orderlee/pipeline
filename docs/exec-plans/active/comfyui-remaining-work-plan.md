# ComfyUI 통합 — 잔여 작업 실행계획

**작성일:** 2026-09-21
**전제 설계서:** [comfyui-local-genai-pipeline-plan.md](comfyui-local-genai-pipeline-plan.md) (Phase A~F 정의)
**이 문서의 범위:** 설계서 Phase 정의는 그대로 두고, **2026-09-21 실측 기준으로 무엇이 남았고 어떤 순서로 해야 하는가**만 다룬다.

---

## 1. 2026-09-21 실측 상태

### 1.1 이번에 닫힌 것

| 항목 | 증거 |
|---|---|
| embedding-service 3일 장애 복구 | `/embed_text` 503 → 200, PE-Core 1024-d 반환. GPU0 12.4GB → 3.3GB |
| 좀비 job / 정체 배치 74건 | comfy_local jobs `pending/submitted/running` = 0 |
| 좀비 재발 방지 | `poll()` 에 벽시계 deadline(`COMFYUI_JOB_TIMEOUT_SECONDS`, 기본 900s). heartbeat 는 history 존재 시에만 |
| 프록시 allowlist 우회 | `./prompt`·`x/../interrupt` 등 7종 전부 403 (교체 전 200) |
| genai 빌드 컨텍스트 94GB | `docker/.dockerignore` 에 `data/` 추가 |
| 코드 git 편입 | 커밋 `1a84270` — 42 files, +4,262 |
| **파이프라인 진입점** | dagster 이미지 재빌드로 `ops_register` allowlist 반영. `raw_files` 에 comfy_local 1행 최초 생성 |

### 1.2 E2E 1회 관통 결과 (batch `b4fe9a6f-339`, smoke 1장)

| 단계 | 결과 |
|---|---|
| Promote (`label_policy=required`) | ✅ |
| dispatch manifest | ✅ `source_type=genai_output`, `genai_engine=comfy_local` |
| `dispatch_stage_job` | ✅ SUCCESS (9 steps) |
| `raw_files` 편입 | ✅ 1행, `ingest_status=completed`, raw_key 로마자 정규화 정상 |
| `image_metadata` | ✅ 1행, 1168×880, `vlm-raw` 키 세팅 |
| SAM3 bbox 검출 | ⚠️ **0건** (`image_labels` 0행) |
| LS 프로젝트 생성 | ✅ 822(image) / 821(video) |
| **LS task 생성** | ❌ **0건** — 사람이 볼 수 있는 작업이 없다 |

즉 **Phase D 완료기준은 아직 미충족**이다. 파이프라인은 관통했지만 마지막 한 칸에서 멈춘다.

### 1.3 LS task 0건의 원인 (확정)

`LS_TASK_GATE_ENABLED=true`, `LS_TASK_GATE_BYPASS_RATIO=0.02` (prod `.env`).
[`ls_task_gate.decide()`](../../../src/gemini/ls_task_gate.py)는 **자동 라벨 결과 0건인 후보를 98% 확률로 제외**한다.
SAM3 가 합성 연기 이미지에서 0건을 검출했으므로 우리 이미지는 게이트에서 탈락했다.

이것은 버그가 아니라 **설계와 정책의 충돌**이다:

- 게이트의 목적: 자동 검출이 비어 있는 프레임으로 라벨러 시간을 낭비하지 않는다.
- ComfyUI 의 목적: **자동 검출이 약한 클래스(연기·화재·쓰러짐)의 데이터를 사람이 검수해 채운다.**

게이트를 그대로 두면 **합성 데이터의 존재 이유 자체가 차단된다.** SAM3 가 이미 잘 잡는 것만 사람에게 가므로, 합성으로 메우려던 결손이 영원히 안 메워진다. 이 충돌의 해소가 잔여 작업의 첫 번째 결정 사항이다.

### 1.4 같은 run 에서 드러난 부수 문제

- **유령 video 프로젝트**: 요청은 `[captioning_image, bbox]` 였는데 LS 생성 단계 로그의 methods 는 `['bbox','captioning_image','captioning_video','timestamp_video']` 로 확장됐고, 이미지뿐인 배치에 video 프로젝트 821 이 빈 채로 생성됐다.
- **`ls_tasks create` 의 무언(無言) 실패**: stdout 이 비어 있는데 `ls_task_status='created'` 로 기록되고 job 은 SUCCESS. 0건 생성과 정상 생성을 호출부가 구분하지 못한다.
- **`force=true` 의 함정**: 재Promote 는 `from_archived=true` 로 archive 경로를 타므로, **한 번도 인제스트되지 않은 배치**에는 쓸 수 없다(`no archived rows` → failed 로 이동). 1차 Promote 가 실패한 배치는 `release_batch_promote_claim()` + incoming 잔여 폴더 삭제 후 **일반 Promote 로 재시도**해야 한다.

---

## 1.5 2026-09-21 결과 — P0 완료, E2E 관통

| 항목 | 상태 |
|---|---|
| P0-1 게이트 합성 면제 | ✅ |
| P0-2 LS task 생성까지 완주 | ✅ **LS 프로젝트 823 에 task 생성 확인** |
| P0-3 finalized 게이트 반증 | ✅ `review_status='auto_generated'`, `v_finalized_labels` 0건 |
| P0-4 0건 생성 가드 | ✅ (실제로 발동해 `skipped` 기록 — 옛 코드였다면 조용히 `created`) |
| P1-2 / P1-3 / P1-4 / P1-5 | ✅ |
| P2-1 / P2-2 / P2-3 | ✅ (P2-3 의 미측정 VRAM 항목은 P2-4 실측으로 채워짐) |
| P2-4 20장 품질 UAT | ◐ **생성·측정·기록 완료(2026-09-21) / 합격 판정은 운영자 대기** → [검토 시트](../../genai_rollout/comfyui-uat-20-review-sheet-2026-09-21.md) |
| P2-5 SAM3 합성 검출률 | ✅ (합성 표본 n=1 이라 검출률 자체는 산출 불가 — 아래 §P2-5) |

**E2E 실측 경로:** Promote → dispatch_stage_job SUCCESS → `raw_files` 1행(`genai_output`/
`comfy_local`) → `image_metadata` 1행 → **SAM3 1 detection** → `image_labels`
(`auto_generated`) → LS 프로젝트 823 task 생성 → 검수 전 dataset 게이트 0건.

### §1.3 의 진단 정정

"LS task 0건의 원인은 라벨러 게이트" 는 **불완전한 진단이었다.** 계측을 붙여 돌린 실측은
`gated_out=0`, "후보 0건" 이었다 — 게이트는 이 배치를 본 적이 없다. 관문은 직렬 3단이었다:

1. **SAM3 후보 자격** — `find_pending_images` 가 `image_role IN ('processed_clip_frame',
   'raw_video_frame')` 로만 뽑아 `source_image`(직접 인제스트된 정지 이미지)가 탈락.
   COCO JSON 이 안 생기니 image LS task 의 재료 자체가 없었다. **여기가 진짜 첫 관문.**
2. **라벨러 게이트** — 검출 0건이면 98% 제외. P0-1 이 연 곳. (실제로는 SAM3 가 1개를
   검출해 이번엔 `has_result` 로 통과했다.)
3. **pseudo 스냅샷 누수** — `.pseudo.json` 이 별개 stem 으로 색인돼 **같은 이미지가 두 번**
   task 가 됐다. ComfyUI 와 무관한 선행 버그(SAM3 JSON 39,056건 중 19,528건이 pseudo).

교훈: 게이트를 의심하기 전에 **후보가 그 게이트에 도달했는지**를 먼저 계측해야 한다.
"0건"에는 "걷어냈다"와 "애초에 없었다"가 섞여 있고, 둘은 완전히 다른 사건이다.

### 반영 상태

`docker/genai/`·`src/` 변경 모두 prod 에 재빌드·반영 완료(genai 2회, dagster 2회).
`docker/comfyui/` 변경(P2-2/P2-3)은 **아직 재빌드 안 함** — 재빌드 시 19.4GB 재해시 +
force-recreate 가 일어나므로 배포 타이밍을 골라야 한다.

## 2. 잔여 작업

우선순위는 **"사람 검수까지 실제로 도달하는가"** 를 기준으로 매겼다. 그것이 닫히기 전에는 품질 UAT 도 coverage 자동화도 의미가 없다.

### P0 — 합성 데이터가 사람에게 도달하게 한다

#### P0-1. 라벨러 게이트와 합성 데이터의 관계를 결정한다 *(사람 판단 필요)*

선택지:

| 안 | 내용 | 대가 |
|---|---|---|
| A | `source_type='genai_output'` 은 게이트를 면제 | 합성은 전량 사람에게 간다. 검수 부하가 생성량에 비례 |
| B | 합성 전용 bypass_ratio (예: 1.0) 를 별도 env 로 | A 와 사실상 같되 노브로 조절 가능 |
| C | 게이트 유지 + SAM3 검출률을 올린다 | 근본적이지만 연기·화재는 SAM3 가 약한 영역이라 불확실 |
| D | 합성은 LS 가 아니라 별도 검수 경로로 | 새 경로 구축 비용. `finalized` 게이트 재설계 필요 |

**권장: A 또는 B.** 근거는 §1.3 — 게이트의 전제(자동 검출이 되는 데이터)와 합성의 목적(자동 검출이 안 되는 데이터)이 정반대다. C 는 P2 로 병행하되 차단 해소 수단으로 삼지 않는다.

**완료기준:** 결정이 문서화되고, 선택한 안이 코드에 반영되며, 그 분기를 검증하는 단위 테스트가 추적 파일로 편입된다.

#### P0-2. E2E 를 LS task 생성까지 완주시킨다

P0-1 반영 후 batch `b4fe9a6f-339` 를 재Promote(§1.4 절차)하여 LS 프로젝트 822 에 task 1건이 실제로 생성되는 것을 API 로 확인한다.

**완료기준:** `GET /api/projects/822` 의 `task_number >= 1`, 그리고 검수자가 UI 에서 이미지를 열 수 있다(presigned URL 유효).

#### P0-3. `finalized` 게이트를 반증한다

검수 전 상태에서 dataset build 쿼리가 **0건**임을 확인한다. 설계서 Phase D 완료기준의 후반부다.

**완료기준:** `require_ls_finalized=true` 경로의 build 소스 쿼리가 이 asset 을 포함하지 않음을 SQL 로 보인다. 그 뒤 사람이 LS 에서 확정 → `finalized` 로 바뀌면 포함되는 것까지 한 번 확인한다.

#### P0-4. `ls_tasks create` 의 0건 생성을 실패로 만든다

호출부가 생성 건수를 받아서, 0건이면 `ls_task_status='created'` 로 기록하지 않고 사유를 남긴다(게이트 탈락 / presign 실패 / 후보 없음 구분).

**근거:** 이번 사건에서 job SUCCESS + `created` 기록 + task 0건이 동시에 성립했다. 같은 형태의 무언 실패가 과거에도 있었다([project_ls_minio_cred_guard] 이력).

**완료기준:** 0건일 때 job 이 실패하거나 최소한 경고 + 상태 미기록. 단위 테스트 편입.

### P1 — 검수 품질과 안전 경계

#### P1-1. LS task 에 synthetic provenance 를 표시한다

현재 `src/gemini/` 와 `defs/ls/` 전체에 `comfy_local`·`synthetic` 참조가 0건이다. 검수자가 실사 CCTV 와 합성본을 **구분할 수단이 없다.**

**완료기준:** LS task 에 출처 배지(엔진·batch_id·의도 이벤트)가 노출되고, 검수 항목에 설계서 Phase D.3 의 4개 판정(이벤트 실재 / 배경 보존 / artifact 허용 / bbox 정합)이 들어간다.

#### P1-2. Promote 의 이미지 기본값을 고친다

`docker/genai/templates/promote.html` 은 엔진 무관하게 `timestamp_video` 만 체크돼 있다. comfy_local 은 이미지 배치이므로 `[captioning_image, bbox]` 가 기본이어야 한다. 서버측에도 엔진별 강제/검증이 없다.

**같이 처리:** §1.4 의 methods 확장으로 생기는 유령 video 프로젝트. 이미지뿐인 배치에 video 프로젝트를 만들지 않는다.

#### P1-3. `maintenance_exit` 에 owner 검증을 넣는다

`docker/embedding/app.py` 의 `/maintenance/exit` 는 owner 를 받지 않는다. comfy job 하나가 **trainer·SAM3 의 정비 윈도우를 해제**할 수 있다. `scripts/clear_maintenance.sh` 의 강제 해제는 `force` 플래그로 유지한다.

#### P1-4. GPU lease 의 계약을 완성한다

- `lease_token` 이 발급만 되고 heartbeat/release 에서 검증되지 않는다 → 토큰 검증 추가
- `generation_gpu_leases` 가 `resource` 단일 PK 라 acquire 마다 덮어써서 **이력이 남지 않는다** → 이력 보존 여부 결정
- 경합·TTL steal·release 의 **SQL 레벨 테스트가 없다**(현재 monkeypatch 단위 테스트뿐)

#### P1-5. 프록시를 blocklist → allowlist 로 역전한다

dot-segment 구멍은 닫았지만 구조는 여전히 "위험한 것만 막는" 방식이다. `object_info`·`view`·`upload/image`·`history`·`system_stats`·`ws` 등 필요한 것만 통과시키는 방식으로 뒤집으면 custom node 가 라우트를 추가해도 안전 경계가 유지된다.

### P2 — 재현성과 품질

#### P2-1. provenance 의 거짓 값을 제거한다

FLUX 워크플로에는 `negative_prompt`/`cfg`/`denoise` 바인딩이 없는데 provenance 에는 항상 기록된다. 실행되지 않은 `cfg=6.0`, `denoise=0.85` 가 "사용한 것처럼" 남아 **재현성 기록으로서 거짓**이다. sampler 이름도 누락됐다.

**완료기준:** 실제 바인딩된 파라미터만 기록하고, 미바인딩은 명시적으로 `null`. sampler 추가.

#### P2-2. `COMFYUI_REQUIRE_MODELS` 의 자기모순을 해소한다

플래그 하나가 "모델 없으면 기동 실패"와 "healthcheck 통과 여부"를 동시에 지배해, 설계서 Phase B 완료기준 두 개("모델 없이도 healthy" / "manifest 불일치 시 unhealthy")가 **동시 성립할 수 없다.** 두 관심사를 분리하거나 완료기준을 현실에 맞게 고친다.

#### P2-3. `model_manifest.json` 을 설계서 요구대로 채운다

현재 파일명·SHA-256·size·license·source 만 있다. 설계서 B-3 이 요구한 **Comfy node revision·예상 VRAM·입력 schema** 가 없다.

#### P2-4. 20장 품질 UAT — **생성·기록 완료(2026-09-21), 판정은 운영자 대기**

워크플로별 10장(총 20장), 서로 다른 실제 CCTV 카메라 20대 대상으로 **생성·측정·기록까지 완료**했다.
**합격/불합격 판정은 하지 않았다** — 설계서 Phase D.3 의 4개 판정(이벤트 실재 / 배경·카메라 기하 보존 /
artifact 허용 / bbox·label 정합)은 사람 몫이고, 그 칸을 비워둔 검토 시트를 산출물로 남겼다.
**생성물은 promote 하지 않았다.**

> 📄 **운영자 검토 시트: [`docs/genai_rollout/comfyui-uat-20-review-sheet-2026-09-21.md`](../../genai_rollout/comfyui-uat-20-review-sheet-2026-09-21.md)**
> side-by-side 이미지: `/home/user/mou/nas_primary/genai_studio/_uat_p24_2026-09-21/`
> 배치 묶음: `genai_batches.options_json LIKE '%uat-p24-20260921%'` (20건)

##### 실행 결과

| 항목 | FLUX.2 Klein edit | SDXL inpaint |
|---|---|---|
| 제출 / 성공 / 실패 | 10 / **10** / 0 | 10 / **10** / 0 |
| 생성시간 min·median·max (ComfyUI 실행) | 12.02 / 12.31 / 12.95 s | 10.11 / 14.97 / 29.43 s |
| **GPU0 peak VRAM** (max / min) | **15.02 GB** / 13.45 GB | **12.64 GB** / 7.55 GB |

전 20건이 `job_status=done` 으로 끝났고 **재시도는 한 번도 필요하지 않았다.** 20건 동시 제출 →
`gpu0_comfy` lease 동시 1건 → 나머지 `pending` → `genai_poll_sensor` drain 경로가 설계대로 작동했다
(총 소요 약 20분).

##### 이 회차가 새로 알려준 것

1. **peak VRAM 을 처음으로 실측**했고 `docker/comfyui/model_manifest.json` 의 `peak_process_vram_gb`
   (전체·워크플로별)를 측정값으로 채웠다 — 측정 방법·한계를 같은 파일에 명시. P2-3 의 미측정 항목이 닫혔다.
   ⚠️ **FLUX 실측 peak 15.02 GB > 사전 점검 임계 `COMFYUI_MIN_FREE_VRAM_GB=13`** — 게이트를 통과하고도
   VRAM 이 모자랄 수 있다. 임계 상향 여부는 별도 결정 사항.
2. **두 워크플로의 실패 양상이 서로 반대**다. FLUX 는 요청 이벤트를 10/10 렌더하지만 **마스크가 없어
   전역 재생성**이라 인물 소실·간판 문자 변조·타임스탬프 변조 같은 배경 드리프트가 함께 온다.
   SDXL 은 마스크 밖이 구조적으로 보존되지만 마스크 안이 문맥과 무관하게 채워져 **사각 seam 10/10**,
   요청 이벤트 관측 6/10 이다. J1 과 J2 를 따로 채점해야 하는 이유가 여기 있다.
3. **FLUX 는 출력이 항상 약 1 MP** 다(노드 5 `ImageScaleToTotalPixels`). 4K 원본은 화소 88% 가 버려지고
   720×576 원본은 오히려 업스케일된다 — "원본 해상도 증강본"은 현재 워크플로로 성립하지 않는다.
4. **원본 풀이 MinIO 가 아니다.** 2026-08-31 NAS 재구축 이후 `vlm-processed`/`vlm-raw` 에 남은 최상위
   prefix 는 4개/5개뿐이고 후보 50건 중 48건이 `NoSuchKey` 였다. 20 장면 중 14 장면은 NAS archive
   원본에서 프레임을 추출해 채웠다. 합성 정례화 전에 **source 풀을 MinIO 로 복구할지 archive 기준으로
   배선할지** 결정이 필요하다.
5. **SDXL 프롬프트 결함(이번 UAT 의 자기 결함)**: 프롬프트를 `"CCTV security camera still, ..."` 로
   시작하게 써서 SDXL 의 CLIP 이 그 어구를 객체로 그렸다(`S13`·`S16` 에 돔형 감시카메라 생성).
   다음 회차에서는 장치를 가리키는 명사를 빼야 한다. 20장 상한 때문에 재생성하지 않았다.
6. `denoise` A/B: inpaint 출하 기본값 **0.85** 로 돌린 2건(`S13`·`S18`)은 마스크가 평평한 단색
   사각형으로 채워졌다. 다만 `1.0` 으로 돌린 8건 중에도 이벤트 미생성이 2건 있어 **denoise 만이
   원인은 아니다** — SDXL base 는 inpaint 전용 체크포인트가 아니다.

##### 남은 결정 (사람)

- 시트의 J1~J4 채점 → 합격 기준 확정(설계서 §9 미결 항목).
- 합격분 promote 여부. **보류 중에는 아무 것도 하지 않아도 된다** — promote 하지 않은 배치는
  `raw_files` 에 들어가지 않는다.
- SDXL inpaint 를 계속 쓸지(전용 inpaint 체크포인트 도입 / 프롬프트·마스크 규칙 재설계) 여부.
- `COMFYUI_MIN_FREE_VRAM_GB` 상향 여부.

#### P2-5. SAM3 의 합성 데이터 검출률을 측정한다 — **측정 실시(2026-09-21)**

**결론 먼저.** 합성본 검출률은 **낼 수 없다 — 표본 n=1.** 실사 baseline 과 클래스별 약점은
측정됐고, 그 결과는 P0-1 의 게이트 면제를 **현행대로 유지**하는 쪽을 지지한다. 다만 유지 근거는
§1.3 이 들었던 근거("SAM3 가 합성 연기에서 0건을 검출했다")가 **아니다** — 그 근거는 §1.5 에서
이미 반증됐고, 이번 측정이 반증을 한 번 더 확인했다. 유지 근거는 **구조적 논거**뿐이다.

##### (1) 합성본 표본 — 측정 불가 판정

| 항목 | 실측 |
|---|---|
| `genai_job_provenance` | **27행** (`flux2-klein-4b-edit-v1` 26 + `sdxl-inpaint-cctv-v1` 1) |
| NAS `genai_studio/*/*/outputs/` PNG | **26개** (11 배치, 전부 2026-09-18 생성) |
| `genai_jobs status='done'` (comfy_local) | 25 |
| `raw_files genai_engine='comfy_local'` | **1** |
| **SAM3 를 거친 합성본** | **1** |

게다가 26장 중 **이벤트가 실제로 요청된 것은 8장뿐**이다. 프롬프트를 전수 확인한 결과:

| 대상 클래스 | 장수 | 배치 |
|---|---|---|
| smoke | 5 | `b4fe9a6f-339`(편입됨), `441c3032-9e2`, `f17921e3-acc`, `db986921-52a`, `c0a15318-f82` |
| falldown | 3 | `39f55028-70f`, `ee09e4c0-2e2`, `2d526ef5-2b6` |
| **fire** | **0** | — |
| (프롬프트가 에러 문자열) | 17 | `2631bd77-291`(8), `09059277-ca9`(8), `905aab84-a0c`(1) |

17장은 `genai_batches.prompt` 가 `"approved Comfy graph required: graph structure does not
match an approved Comfy workflow"` / `"Describe the requested edit"` 라는 **에러·플레이스홀더
문자열**이다. 이벤트가 합성됐다고 가정할 수 없으므로 측정 코호트에 넣으면 안 된다
(`output_sha256 ≠ input_sha256` 이라 바이트는 다르지만, 그것이 이벤트 존재의 증거는 아니다).

→ **디스크에 있는 것을 전부 편입해도 smoke 5 / falldown 3 / fire 0.** "연기·화재·쓰러짐 각각의
검출률" 이라는 이 항목의 원래 질문은 **어떤 경로로도 지금 답할 수 없다.**

##### (2) 실사 baseline — 게이트 docstring 의 54.9% 는 정확했고, 지금은 52.85%

| 스냅샷 | 프레임 | 박스 0개 | 비율 |
|---|---|---|---|
| `created_at < 2026-09-10` (docstring 재현) | 454,726 | 249,686 | **54.91%** |
| 2026-09-21 현재 전량 | 474,110 | 250,590 | **52.85%** |

docstring 의 수치는 **자릿수까지 정확히 재현됐다.** 이후 19,384 프레임이 추가되며 비율은
2.06pp 내려갔지만 "라벨러가 여는 화면의 절반 이상이 비어 있다" 는 주장은 **여전히 성립**한다.

박스 수 분포: 0개 52.85% / 1개 12.57% / 2개 15.02% / 3–5개 15.15% / 6–10개 3.97% / 11개+ 0.44%.

##### (3) 코호트별 0건 비율 — 20배 편차

`source_unit_name` 기준 24개 코호트(200프레임 이상). 0건 비율은 **4.7% ~ 100%** 로 흩어진다.

| 코호트 | 프레임 | 0건 | 0건 비율 | 평균 박스 |
|---|---|---|---|---|
| site-b | 189,236 | 96,128 | 50.8% | 1.29 |
| cohort-b | 73,390 | 66,701 | **90.9%** | 0.22 |
| source-g | 49,179 | 31,164 | 63.4% | 1.02 |
| cohort-a | 33,767 | 10,878 | 32.2% | 1.61 |
| sourcep | 28,306 | 9,699 | 34.3% | 1.09 |
| SL_cam2_3 | 19,383 | 904 | **4.7%** | 5.27 |
| fire_smoke | 3,464 | 779 | 22.5% | 2.47 |
| AX_project | 6,140 | 5,766 | **93.9%** | 0.11 |
| site-f | 425 | 425 | **100.0%** | 0.00 |

→ 전체 평균 52.85% 는 **코호트 혼합의 산물**이지 SAM3 의 고유 성능이 아니다. 합성본을 이
평균과 비교하는 것은 의미가 없고, **동일 클래스의 실사 코호트와 비교**해야 한다.

##### (4) 클래스별 — 부분 측정만 가능, 그나마 **상한**이다

**의도된 조인 경로는 죽어 있다.** `image_labels → raw_files.spec_id → labeling_specs.categories`
는 `raw_files.spec_id` 가 **131,933행 전부 NULL**, `labeling_specs` **0행**,
`observed_categories` **0행** 이라 성립하지 않는다.

우회로를 찾았다 — `al_frames.frame_key = image_metadata.image_id` 가 11개 frame 코호트 중
3개에서 성립한다(`fire_smoke_gt` 3,464 / `archive_sourcep` 3,897 / `cohorta_outdoor_fall_pool`
3,052). 이 중 **사람 클래스 라벨 + SAM3 행을 동시에 가진 것은 `fire_smoke_gt` 하나**다
(`archive_sourcep` 3,897행은 SAM3 행이 0개).

| cls (사람 GT) | 프레임 | 박스 0개 | 0건 비율 | 평균 박스 |
|---|---|---|---|---|
| smoke | 1,560 | 446 | **28.6%** | 1.55 |
| fire | 1,525 | 315 | **20.7%** | 3.27 |
| normal | 372 | 18 | **4.8%** | 3.02 |
| smoking | 7 | 0 | 0.0% | 2.57 |

**이 수치는 검출률이 아니라 검출률의 상한이다.** `object_count` 는 *해당 dispatch 가 프롬프트한
개념 전부*의 박스 수이지 그 프레임의 GT 클래스 박스 수가 아니다. 살아남은 COCO JSON 실측
(`sl_cam2_3`)의 `categories` 는
`['person','vehicle','truck','forklift','car license plate','person with helmet']` 였다 —
연기 프레임에서 잡힌 박스가 사람일 수 있다. 즉 **smoke 의 실제 검출률 ≤ 71.4%, fire ≤ 79.3%.**

박스별 카테고리로 확정하는 것은 **불가능하다**: 2026-08-31 NAS 재구축으로 MinIO 가 비워져
`vlm-labels` 에 남은 prefix 는 `genai_b4fe9a6f-339`·`sourcep`·`sl_cam2_3`·
`sl_cam3_test_20260911`·`songpa` 5개뿐이고, `fire_smoke/sam3_segmentations/*.json` 은
`Object does not exist` 다.

그럼에도 **normal(4.8%) ≪ fire(20.7%) < smoke(28.6%)** 라는 순서 자체는 신호다. 사람이 많은
장면이라 박스가 잘 나온다는 반론은 이 순서를 설명하지 못한다 — 그렇다면 normal 이 가장 높아야
하는데 **가장 낮다.** 즉 이벤트 클래스가 normal 보다 3~6배 자주 빈손인 것은 프롬프트 구성의
부수효과가 아니라 **이벤트 클래스 자체의 약점**일 개연성이 높다. 이 방향은 P0-1 의 구조적 논거와
일치한다.

##### (5) 유일한 합성 표본 — 무엇을 증명하고 무엇을 증명하지 못하나

`image_id='8b6a5782-dcca-4d55-a03b-72418625a87a'`, batch `b4fe9a6f-339`, 1168×880.
COCO JSON 실측: `categories=[{'name':'smoke'}]`, annotation 1개, **score 0.6953**,
bbox `[192,346,189,251]` (프레임 면적의 약 4.6%).

- **증명하는 것:** 우연히 섞인 person 박스가 아니라 **클래스가 일치하는 진짜 smoke 검출**이다.
  그리고 §1.3 의 경험적 주장("SAM3 가 합성 연기 이미지에서 0건을 검출했다")은 **틀렸다** —
  §1.5 가 밝힌 대로 그것은 검출 실패가 아니라 `find_pending_images` 의 `image_role` 필터에
  막힌 **배관 실패**였다. P0-1 의 면제를 지탱하던 *경험적* 근거는 이것으로 소멸한다.
- **증명하지 못하는 것:** "합성본에서도 SAM3 가 잡는다". n=1 의 Clopper–Pearson 95% 구간은
  **[0.025, 1.0]** 이다. 참값이 5% 여도 95% 여도 이 관측과 모순되지 않는다. **이 1건을 면제
  불필요의 근거로 쓰면 안 된다.**

##### (6) P0-1 정책 판단 — `LS_TASK_GATE_SYNTHETIC_BYPASS_RATIO=1.0` **유지**

낮출 이유가 없다. 근거는 넷이다.

1. **현 생성량에서 이 노브는 사실상 무동작이다.** 합성본 총 26장, 편입 1장. 1.0 을 0.02 로
   바꿔도 영향받는 것은 손에 꼽는 장수다. 지금 내리는 것은 이득 없이 결정만 소비한다.
2. **작동하는 구간에서는 정확히 틀린 방향으로 작동한다.** 게이트는 `result_count==0` 에만
   개입한다. 0건이 나온 합성본 = SAM3 가 그 이벤트를 못 본 이미지 = **ComfyUI 를 돌린 이유
   그 자체**다. 이것을 98% 버리면 §1.3 의 구조적 모순이 그대로 재현된다. 경험적 근거가
   무너졌어도 **구조적 논거는 (4)의 클래스 순서가 오히려 보강했다.**
3. **면제의 비용은 유계이고 작다.** "합성 전량이 사람에게 간다" 는 오해다. 합성본이
   `fire_smoke_gt` 처럼 행동한다면 0건은 20~29% 뿐이고 나머지 71~79% 는 `has_result` 로
   **어차피 통과**한다. 면제의 *한계비용*은 생성 4장당 태스크 1장 수준이다.
4. **BYPASS 표본 오염 우려는 코드가 이미 해결했다.** `decide()` 가 `synthetic_bypass` 를
   `bypass_sample` 과 별도 reason 으로 분리하므로 게이트 오탈락률 추정용 무작위 표본은
   오염되지 않는다. 비율을 낮출 가장 강한 기술적 이유가 이미 사라져 있다.

**대신 통제해야 할 것은 비율이 아니라 생성량이다.** §4.3 의 "합성 검수 부하의 상한" 은 일/주
생성 상한으로 잡는 것이 맞다. 정보가 있는 케이스(0건)를 버리는 표집비로 부하를 조절하면
데이터의 목적을 훼손한다.

##### (7) 재측정 조건 — 필요 표본과 얻는 법

실사 비교군은 이미 있다(`fire_smoke_gt` fire 1,525 / smoke 1,560). **합성 쪽만 채우면 된다.**

| 목표 | 검정 | 클래스당 n | 총 n (fire/smoke/falldown) |
|---|---|---|---|
| 거친 스크리닝 — "파국적(≈35%)인가 baseline(≈75%)인가" | 2-proportion, α=.05, power=.80, δ=40pp | **21** | 63 |
| 결정적 판정 — 20pp 격차 탐지 (75% vs 55%) | 동일, δ=20pp | **86** | 258 |

비용: `flux2-klein-4b-edit-v1` 의 GPU 시간 중앙값 **29.9초/장**(p90 34.8초, n=26).
스크리닝 63장 ≈ **GPU0 32분**, 결정적 258장 ≈ **2.3시간**.

권장 순서:

1. **GPU 0원 단계 먼저** — NAS 에 이미 있는 **이벤트 프롬프트 8장(smoke 5 + falldown 3)** 을
   일반 Promote 로 편입한다(§1.4 절차, `force=true` 금지). SAM3 를 자동으로 타므로 추가
   생성 없이 n=1 → n=8 이 된다. **에러 프롬프트 17장은 반드시 제외** — 편입하면 실사에
   가까운 프레임이 합성 코호트를 오염시킨다.
2. n=8 이 baseline 대비 명백히 낮으면(예: 0건 비율 ≥ 60%) 그 시점에서 면제는 사실상 영구
   정책으로 확정하고 (7)의 추가 생성은 생략해도 된다.
3. 애매하면 스크리닝 63장을 생성한다. **fire 는 n=0 이라 어떤 결론도 새로 생성해야만 나온다.**
4. 합성 0건 비율이 실사 baseline 과 10pp 이내로 수렴하면 그때 `synthetic_bypass_ratio` 를
   일반값 0.02 로 되돌린다 — 그 시점에는 면제가 아무것도 사주지 않기 때문이다.

##### (8) 재현 쿼리

```sql
-- (2) baseline 및 docstring 재현
SELECT count(*), count(*) FILTER (WHERE object_count=0)
FROM image_labels WHERE label_tool='sam3' AND label_source='auto' AND created_at < '2026-09-10';

-- (3) 코호트별
SELECT r.source_unit_name, count(*), count(*) FILTER (WHERE l.object_count=0)
FROM image_labels l JOIN image_metadata m ON m.image_id=l.image_id
JOIN raw_files r ON r.asset_id=m.source_asset_id
WHERE l.label_tool='sam3' AND l.label_source='auto'
GROUP BY 1 HAVING count(*)>=200 ORDER BY 2 DESC;

-- (4) 클래스별 (유일한 우회 조인)
SELECT f.cls, count(*), count(*) FILTER (WHERE l.object_count=0)
FROM al_frames f JOIN image_metadata m ON m.image_id=f.frame_key
LEFT JOIN image_labels l ON l.image_id=m.image_id AND l.label_tool='sam3' AND l.label_source='auto'
WHERE f.cohort='fire_smoke_gt' GROUP BY 1;

-- (1) 합성 표본
SELECT b.batch_id, count(j.job_id) FILTER (WHERE j.status='done'), left(b.prompt,180)
FROM genai_batches b JOIN genai_jobs j ON j.batch_id=b.batch_id
WHERE b.engine='comfy_local' GROUP BY 1,3 ORDER BY 2 DESC;
```

```bash
find /home/user/mou/nas_primary/genai_studio -type f -path '*/outputs/*'   # 26 PNG + 11 manifest
mcli ls prodm/vlm-labels/                                              # 남은 prefix 5개
mcli cat prodm/vlm-labels/genai_b4fe9a6f-339/sam3_segmentations/jungangro_b1_daehapsil_ev1_20260531_030000_t171.json
```

### P3 — 환경 정합과 자동화

#### P3-1. staging 에 ComfyUI 를 올린다

현재 staging clone 에는 `docker/comfyui/` 자체가 없고 `pipeline-test-*` 컨테이너는 정지 상태다. prod 직행으로 운영돼 왔다. 커밋 `1a84270` 이 `dev` 에 들어가면 자동 배포되므로, 그때 설계서 Phase E 의 검증 항목(부팅·GPU0 격리·포트 비공개·queue contention·restart mid-job·rollback drill)을 staging 에서 수행한다.

**주의:** SAM3 와 달리 ComfyUI 는 prod·staging 공유 여부가 정해지지 않았다. GPU0 lease 가 단일 `gpu0_comfy` 리소스라 **공유하면 두 환경이 서로를 막는다.** 이 결정이 선행돼야 한다.

#### P3-2. Dagster GPU0 작업과 lease 상호배제 — ✅ 완료 (축소 설계)

**상호배제 기구는 만들지 않았다. 그게 맞는 판단이었다.**

실측 경합:
- comfy GPU0 점유 = 정상 job 12.8~60s(median ≈30s), 3일 duty cycle **약 0.3%**
- Dagster NVENC GPU0 duty cycle **약 0.02%** (30일 인제스트 1,962 비디오 × 0.3~0.5s ÷ 2 round-robin)
- → 독립 충돌확률 **≈ 6e-7**. NVENC 유닛과 CUDA 코어는 별개 하드웨어라 애초에 다투지도 않는다.
- Places365 torch 는 **prod 에서 한 번도 안 돈다** — `INGEST_DEFER_VIDEO_ENV_CLASSIFICATION=true`
  로 인라인 경로가 차단되고 `video_env_backfill_job` 은 0 runs ever.
  ⚠️ 단 `runtime_policy.py` 의 defer 조건이 `not is_staging` 이라 **staging 에서는 GPU0 에서 실제로 돈다.**

**대신 고친 것 — 같은 lease 조회 한 번이 닫는 실제 버그(오진):**
comfy 가 GPU0 를 잡으면 embedding-service 가 정비 모드로 내려가는데, Dagster 임베딩 asset 의
`wait_until_ready()` 가 503 을 삼키고 120초를 폴링한 뒤 `Failure("... systemic")` 으로 **run 을
실패**시킨다. `systemic` 은 틀린 라벨이다 — 원인이 comfy lease 면 상한 1,200s(실측 30s)로
자연 회복되는 **일시적** 상태다. 이제 임베딩 경로만 lease 를 **읽고**(SELECT 전용, 획득 없음)
defer/skip 으로 분기한다.

불변식:
- **획득하지 않는다** — Dagster 가 lease 를 잡으면 comfy 가 굶는 역방향 교착이 생긴다.
  테스트가 쓰기 SQL·acquire/release 이름 부재를 구조적으로 강제한다.
- **TTL 이 유일한 진실** — `expires_at` 지난 lease 는 `state='active'` 여도 막지 않는다.
  2026-09-18 의 3일 교착이 반대 방향으로 재현되지 않게.
- **조회 실패는 fail-open** — 030 미적용 환경·DB 순단에 임베딩이 통째로 멈추면 안 된다.
  이 lease 는 안전장치가 아니라 **진단 보조**다. 실제 GPU0 보호는 comfy 의 VRAM 하한과
  embedding 정비 게이트가 한다.
- NVENC·인제스트 본류는 **게이팅하지 않는다.** 안 다투는 자원 때문에 인제스트를 세우는 쪽이 해롭다.

#### P3-3. Phase A0 / F (coverage planner) — ✅ 구현 완료 (2026-09-21), **기본 꺼짐**

설계서가 의도적으로 보류한 범위였으나 "전부 구현" 결정으로 착수했다. 착수 조건(설계서 §7 게이트 0단계)이 아직 `blocked_*` 라는 사실은 **바뀌지 않았고**, 그래서 제어평면은 그 사실을 **사유로 출력**하도록 만들었다. 스키마·계산만 배포되고 아무것도 돌지 않는다.

| | |
|---|---|
| A0 (사실 계층) | `032_coverage_facts.sql` + 테스트 33건 — `51a7628`/`57d4189` |
| F (제어평면) | `033_coverage_control_plane.sql` + planner + dispatch sensor + 테스트 151건 — `3a7e1c2` |
| 기본 상태 | `synthetic_coverage_planner_schedule` **STOPPED** · `synthetic_campaign_dispatch_sensor` **STOPPED** · policy `draft`+`plan_only`+budget/cap 전부 0 |
| prod 적용 | **아직 안 됨** — `_pg_migrations` 최신 = `031_al_eval_holdout.sql`. 032→033 은 다음 이미지 재빌드 배포 부팅 때 순차 적용된다 |

##### 설계 의도 중 스키마로 내린 것

- **F.3 mode gate**: `policy_mode_at_plan` 이 `plan_only`/`disabled` 인 campaign 은 `planned`/`blocked`/`cancelled` 밖으로 전이 불가(CHECK). policy mode 를 나중에 올려도 **이미 만든 plan_only campaign 은 안 풀린다** — 재계획해야 한다. "승인 안 한 campaign 은 ComfyUI 요청 0건" 이 코드 약속이 아니라 제약이다.
- **§5.2 상한**: `planned_count` 가 deficit·reference·share_headroom·budget·campaign_cap 다섯 중 하나라도 넘으면 INSERT 거부.
- **사유 없는 defer 금지**: `state='deferred'` 는 `deferred_reason`(폐쇄 어휘 7개) 없이 존재할 수 없다.

##### planned=0 의 세 가지 뜻을 구분한다

`planned=0` 은 서로 다른 세 상태에서 나온다. 카운터 3개를 NOT NULL 로 박아 구분을 강제했다.

| 상황 | 증거 열 | 사유 |
|---|---|---|
| 사실 자체가 없음 — **오늘 prod** | `class_finalized_total = 0` | `blocked_no_finalized_facts` |
| 사실은 있으나 context 미검증 | `class_finalized_total > 0` ∧ `class_context_verified_total = 0` | `blocked_context_coverage` |
| 진짜 0 | `class_context_verified_total > 0` ∧ `real = 0` | `blocked_share_cap` |

오늘 §5.1 pilot policy 를 넣고 돌리면 4개 셀 전부 `blocked_no_finalized_facts` + campaign `status='blocked'` 가 나온다. **"비율 0" 이 아니라 사유가 남는 것**이 요점이다.

##### 계산에서 도출된 운영 사실

`R=0` 이면 share cap `a` 가 얼마든 headroom = 0 이다 — `(S+P)/(S+P)=1` 이라 `a<1` 인 어떤 cap 도 만족할 수 없다. 즉 **실데이터가 0 인 셀은 합성으로 부트스트랩할 수 없다.** 야간 outdoor(103건)처럼 실데이터가 있는 셀은 늘릴 수 있지만, 0건 셀은 사람이 먼저 실데이터를 넣어야 한다. 설계서 §5.3 의 "배경을 새로 발명하지 않는다" 가 계산식에서도 성립한다.

계산은 전부 `fractions.Fraction` 이다. float 이면 `0.35 × 20 = 7.000000000000001 → ceil 8` 로 튀어 같은 입력이 다른 계획을 낸다.

##### 스키마가 보장하지 **않는** 것

- share 합 = 1, target 이 선언 dimension 전부 보유 → 행 간 조건이라 CHECK 불가. `activate_policy()` 가 지키는 **코드 불변식**이다.
- snapshot 물리적 immutability(트리거 없음) — UPDATE 로 뚫린다.
- `reference_id` 유효성 — dispatcher 가 매 tick `v_generation_reference_candidates` 로 재확인해야 한다.
- 투영 job 없음. 전 테이블 빈 채로 배포된다.

##### 설계서와 어긋나 조정한 것 중 중요한 둘

1. **예산·campaign cap 을 셀 간 공유 자원으로 소진.** §5.2 의 식은 셀 단위라 그대로 쓰면 N 개 셀이 각각 전체 예산을 쓴다고 계산해 합계가 예산을 넘는다. `(priority, target_id)` 순으로 소진하고 소진된 셀은 `blocked_daily_budget` 을 남긴다. 설계서보다 **좁은 계획**만 나오므로 조임이지 어긋남이 아니다.
2. **planner 가 reference 를 고르지 않는다.** `reference_id=NULL` 자리표로 만들고 dispatch 시점에 확정한다 — 미리 못 박으면 하루 뒤 유효기간 만료·holdout 재지정된 reference 를 들고 있게 된다. **아래 미해결 항목과 직결.**

##### ⚠️ `reference_id` 에 FK 를 걸지 않은 이유 — 손 재현은 반증이 아니다

`synthetic_generation_tasks.reference_id`/`genai_batch_id` 에는 FK 가 없다. 걸면 `DELETE FROM image_metadata WHERE source_asset_id = ...`(인제스트 재적재)가 FK 위반으로 깨질 수 있기 때문이다.

**이 현상은 cascade 처리 순서에 의존한다.** 2026-09-21 재검증에서 최소 재현 SQL 과 손으로 만든 동등 상태는 **통과했고**, 그것을 근거로 FK 를 되돌렸다가 통합 테스트에서 실제 재적재 경로가 깨지는 것을 보고 되돌아왔다. 이미지를 하나씩 지우면 둘 다 통과하고, 한 문장이 둘을 같이 지울 때만 깨진다. 원인은 task 의 자식측 RI 검사 트리거가 `pg_trigger.tgattr` 가 **비어 있어**(실측) 그 행의 UPDATE 면 FK 컬럼을 안 건드려도 전부 깨어나는 데 있다.

**신뢰할 수 있는 재현은 `tests/integration/test_coverage_control_plane_migration_033.py::test_the_real_reingest_delete_order_still_works` 하나뿐이다.** "직접 해보니 되던데" 로 이 FK 를 되돌리지 말 것.

##### 미해결 — 결정 필요

**reference 를 누가 고르는가.** 세 선택지: (a) 센서가 dispatch 직전 후보 뷰에서 골라 task 에 기록, (b) GenAI 가 `dimensions_hash` 만 받아 고르고 응답에 `reference_id` 를 돌려줌, (c) planner 가 미리 고정(유효기간·holdout drift 위험). 정해지기 전까지 dispatch 는 **영구 defer** 다 — 조용한 실패가 아니라 `reference_unavailable` 사유가 남는 defer 로 설계했다.

GenAI 쪽에 필요한 계약(`POST /internal/coverage/dispatch`)은 미구현이다. 부재 시 task 는 `endpoint_unavailable` 로 남고 다음 tick 재평가되므로 지금 없어도 안전하다. GenAI 가 책임질 것: GPU0 lease acquire/heartbeat/release(Dagster 는 읽기만), source frame materialize → `comfy_local` submit, `label_policy=required` 강제(자동 Promote 금지), 재시도 가능(503/429)과 영구 실패(4xx) 구분.

---

## 3. 권장 순서

```
P0-1 (결정) → P0-2 → P0-4 → P0-3
   → P1-1, P1-2 (검수 품질)
   → P1-3, P1-4, P1-5 (안전 경계)
   → P2-1 ~ P2-3 (재현성)
   → P2-5 → P2-4 (품질 측정)            ← 2026-09-21 둘 다 실행됨. P2-4 는 사람 판정만 남음
   → P3-1 (staging) → P3-2 → P3-3      ← P3-2·P3-3 완료. P3-1 만 남음
```

P0 는 한 덩어리로 처리하는 편이 낫다 — 네 항목 모두 같은 run 을 반복 실행하며 검증하기 때문이다.

---

## 4. 사람이 결정해야 할 것

1. **라벨러 게이트와 합성 데이터** (P0-1) — 이 문서의 나머지가 여기에 걸려 있다.
2. **ComfyUI 를 prod·staging 이 공유할 것인가** (P3-1) — lease 가 단일 리소스라 공유 시 상호 차단.
3. **합성 검수 부하의 상한** — 게이트를 면제하면 생성량이 곧 검수 부하다. 일/주 상한을 정해야 P3-3 의 budget 설계가 가능하다.
4. **`030_comfy_local.sql` 의 번호 중복** — prod `_pg_migrations` 에 `030_al_frames_unit.sql`(파일 유실)과 공존한다. **리넘버하면 재실행 + `_REQUIRED_MIGRATIONS` 불일치**가 나므로 그대로 두는 것을 권장하나, 규약 위반을 남길지 여부는 결정 사항이다.
5. **P2-4 20장의 J1~J4 판정과 합격 기준** — 2026-09-21 에 20장을 전부 열어 **잠정 판정을 채웠고** 합격 기준도 제안했다([검토 시트](../../genai_rollout/comfyui-uat-20-review-sheet-2026-09-21.md) §5.0). 결론: **SDXL inpaint 불합격**(네 기준 전부 탈락, 3건은 마스크 안 장면을 통째로 교체), **FLUX 는 J2 만 걸린 조건부**(J1 10/10·J3 10/10·J4 10/10 인데 인물 소실 3건). 운영자는 이 판정에 동의/수정만 하면 된다.
6. **SDXL inpaint 를 계속 쓸 것인가** (P2-4 판정 후) — 판정 결과 **불합격**이라 선택지가 좁아졌다. §7.0.1 의 프롬프트 결함을 고쳐도 나머지 8건은 남으므로 **프롬프트 재설계만으로는 해결되지 않는다.** inpaint 전용 체크포인트 도입 / 워크플로 폐기 중 선택이다. 판정 근거는 검토 시트 §5.0.
7. **FLUX 의 J2(인물 소실) 를 어떻게 통과시킬 것인가** — 기준 완화가 아니라 측정 자동화를 권한다. 원본↔결과에 SAM3 사람 탐지를 돌려 **인물 수 감소 = 탈락** 으로 게이트하면 회차마다 흔들리지 않는다. 새 모델이 필요 없다.
8. **합성 증강 source 풀을 어디로 할 것인가** (P2-4 관측) — 2026-08-31 NAS 재구축 이후 `vlm-processed`/`vlm-raw` 가 사실상 비어 있어 20 장면 중 14 장면을 NAS archive 에서 직접 추출해야 했다. MinIO 복구(`reupload_minio_from_archive.py`) vs archive 기준 배선.
9. ~~**`COMFYUI_MIN_FREE_VRAM_GB` 상향 여부**~~ — ✅ **결정·적용 (2026-09-21, 커밋 `33cc673`)**. 13 → 14.5. 게이트는 PE-Core unload *후*에 재므로 embedding 이 아니라 GPU0 의 다른 입주자(`angle-dav2-1`)를 막는 장치다. warm FLUX 자기 소요 14.25 GB(총 15.02 − 베이스라인 0.77) 가 구값 13 을 넘어 free 13.0~14.25 GB 구간에서 통과 후 OOM 이 났다. ⚠️ **genai 컨테이너 재생성이 남아 실행 중 컨테이너는 아직 13 이다.**
10. ~~**coverage task 의 reference 를 누가 고르는가**~~ — ✅ **결정·구현 (2026-09-21, 커밋 `367846d`)**. **센서**가 dispatch 직전에 고른다. planner 는 drift 때문에 기각, GenAI 는 dimension 어휘를 컨테이너 밖으로 복제해야 해서 기각. 선택 술어를 planner 집계와 동일하게 맞추고 계약 테스트로 고정했다.

---

## 5. 검증에 쓸 명령

```bash
# 정체/교착 진단
curl -s localhost:8004/maintenance/status           # active:true 가 오래 지속되면 교착
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline \
  -c "SELECT * FROM generation_gpu_leases;"
docker exec docker-genai-1 python -c "
import urllib.request,json;print(json.load(urllib.request.urlopen('http://comfyui:8188/queue')))"

# 교착 복구 (DB 직접 UPDATE 말고 이 경로)
curl -u "$U:$P" -X POST http://localhost:8089/genai/batches/<batch_id>/cancel

# E2E 진행 확인
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline \
  -c "SELECT asset_id, raw_key, source_type, genai_engine, ingest_status
        FROM raw_files WHERE genai_engine='comfy_local';"

# LS 확인 (LS_API_KEY 는 refresh 토큰 — 교환 후 Bearer)
A=$(curl -s -X POST http://10.0.0.10:8084/api/token/refresh/ \
      -H 'Content-Type: application/json' -d "{\"refresh\":\"$LS_API_KEY\"}" | jq -r .access)
curl -s -H "Authorization: Bearer $A" \
  "http://10.0.0.10:8084/api/projects?ordering=-id&page_size=5"

# 1차 Promote 실패 배치의 재시도 (force 가 아니라 claim 해제)
docker exec -w /app docker-genai-1 python -c "
from db import pg; print(pg.release_batch_promote_claim('<batch_id>'))"
rm -rf /home/user/mou/nas_primary/incoming/genai_<batch_id>
```
