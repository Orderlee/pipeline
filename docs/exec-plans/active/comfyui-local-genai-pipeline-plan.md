# GPU0 전용 ComfyUI · GenAI Studio · 데이터 파이프라인 통합 계획

**작성일:** 2026-09-17  
**상태:** core/운영 모델 활성화 완료 · 기능 smoke 통과 · 20장 품질 UAT 대기 · coverage 자동화 보류  
**목표:** GPU0의 로컬 ComfyUI를 GenAI Studio의 제한된 이미지 생성 엔진 `comfy_local`로 노출하고, 생성본을 provenance와 사람 검수 게이트를 보존한 채 기존 dispatch/Label Studio/데이터셋 빌드 경로에 연결한다.

> **2026-09-17 자동 보충 검토 결론:** 실현 가능하다. 단, 첫 릴리스는 "자동 부족분 계산 → campaign 초안 생성 → 운영자 승인"까지로 제한한다. 생성 제출까지 완전 자동화하는 것은 기준 데이터·reference pool·검수 수용률이 검증된 뒤에만 활성화한다. 현재 prod의 `video_metadata` 131,932건 중 환경 6축이 모두 채워진 행은 0건(131,756건 deferred)이므로, 현 시점에서 환경 비율을 계산해 자동 생성하는 것은 금지한다.

> **2026-09-17 구현 기록:** 요청에 따라 자동 coverage/campaign 단계는 구현하지 않았다. GPU0 전용
> ComfyUI service, 두 allowlisted workflow, model checksum gate, GenAI `comfy_local` adapter/UI,
> PG provenance/GPU lease, embedding maintenance 연동, Promote 사람 검수 강제, build/deploy 계약과
> 테스트까지 구현했다. 2026-09-18 manifest 고정 모델 네 파일을 운영에 배치하고 전체 checksum,
> FLUX/SDXL inference, native canvas gateway, NAS/PG provenance와 GPU lease release smoke를 통과했다.
> workflow별 10장(총 20장) 품질 UAT는 미완료다. 운영 절차는
> [ComfyUI local runbook](../../runbook/comfyui-local-genai.md)을 따른다.
> 2026-09-18에는 GenAI Studio 안에 공식 ComfyUI frontend를 제한형 same-origin proxy로 넣은
> `Comfy Nodes` workspace도 추가했다. 두 승인 graph는 native node canvas에서 편집할 수 있지만,
> Run gateway가 구조·연결·고정 model/output을 template와 대조하고 승인 scalar 변경만 기존
> GenAI submission/provenance 경로로 전달한다. ComfyUI worker의 host port와 임의 graph 실행은
> 계속 허용하지 않는다.

> **2026-09-18 N×M bulk 추가:** GenAI Studio의 기존 Bulk planner에 `comfy_local`을 연결했다.
> FLUX Edit는 이미지 N장×프롬프트 M개의 cartesian 작업을 최대 50 jobs로 제출할 수 있고,
> SDXL Inpaint는 source와 동일 순서의 1:1 mask N장을 검증한 뒤 각 prompt 조합에서 재사용한다.
> 조합은 하나의 `bulk_group_id`로 추적하지만 GPU0 lease/concurrency=1 불변식에 따라 실제 생성은
> 순차 실행한다. 이는 coverage 자동화가 아니라 운영자가 명시적으로 검토·제출하는 수동 bulk다.
> Comfy Nodes 상단에도 `M×1 Batch`를 추가해 현재 승인 node graph의 prompt/seed/steps를 source
> M장에 직접 적용할 수 있다. 제출 시 graph allowlist를 서버에서 다시 검증하고 SDXL mask는
> source와 1:1로 검증하므로 native canvas의 안전 경계는 유지된다.

## 1. 범위와 비범위

### 이번 범위

- ComfyUI를 프로덕션/스테이징 Compose의 독립 profile 서비스로 배포한다.
- GPU0만 보이게 하고, local generation은 전역 동시성 1건으로 제한한다.
- GenAI Studio가 ComfyUI HTTP API를 호출하는 `comfy_local` 비동기 어댑터를 제공한다.
- 승인된 두 이미지 워크플로만 제공한다.
  1. `flux2-klein-4b-edit-v1` — FLUX.2 Klein 4B FP8, 이미지 편집
  2. `sdxl-inpaint-cctv-v1` — SDXL inpaint, 원본+마스크 기반 CCTV 장면 편집
- 모델/워크플로/프롬프트/시드/입력 checksum을 batch별 provenance로 영속한다.
- GenAI의 기존 격리 NAS → 명시적 Promote → dispatch → Label Studio → `finalized` 데이터셋 게이트를 유지한다.

### 이번 비범위

- FLUX.2 [dev], Qwen-Image CPU-offload, Wan 2.2 및 로컬 영상 생성
- 외부에서 접근 가능한 ComfyUI 웹 UI 또는 자유 JSON workflow 실행
- 자동 학습셋 편입, 자동 label-policy `none` 승격, 실사 CCTV 원본 대량 자동 변환
- ControlNet/Depth/Pose의 광범위한 custom node 설치. 이는 두 워크플로의 안정화 뒤 별도 phase로 다룬다.
- 검증되지 않은 scene metadata, `source_unit_name`, Gemini 초벌 라벨만으로 비율을 산정하거나 자동 생성하는 것.

## 2. 고정 아키텍처

```text
Browser
  │ Basic Auth
  ▼
GenAI Studio :8089
  │  engine=comfy_local, workflow_id allowlist
  │  POST /prompt · GET /history/{prompt_id} (internal only)
  ▼
ComfyUI (profile=comfyui, GPU0, queue=1, host port 없음)
  │
  ├── persistent models: docker/data/models/comfyui/
  └── temporary outputs: Comfy volume

GenAI finalize
  └── /nas/data/genai_studio/<date>/<batch>/{originals,controls,outputs,provenance}.json
       │  user explicitly chooses Promote (label_policy=required)
       ▼
/nas/data/incoming/.dispatch/pending/<request>.json
  └── existing dispatch_stage_job → Label Studio → finalized → build_dataset
```

ComfyUI는 `pipeline-network`와 GenAI가 공유하는 compose network에만 노출한다. 운영자가 문제를 조사할 필요가 있을 때도 기본 경로는 컨테이너 로그와 내부 API이며, `:8188`을 사내망에 공개하지 않는다. GenAI Studio만 사용자가 접근할 수 있는 생성 UI다.

## 3. 선행 결정과 운영 불변식

| 항목 | 결정 |
|---|---|
| GPU | ComfyUI는 `CUDA_VISIBLE_DEVICES=0`, `NVIDIA_VISIBLE_DEVICES=0`만 설정한다. GPU1/SAM3는 절대 사용하지 않는다. |
| GPU admission | PG advisory lock 기반 `comfy_local_gpu` 단일 lease와 Comfy 대기열 1건을 함께 사용한다. VRAM preflight가 기준 미만이면 job을 실패시키지 않고 `deferred`로 남긴다. |
| 서빙 공존 | admission 전 embedding-service GPU0 모델을 unload하고, lease 중 ComfyUI가 GPU0을 점유한다. lease 해제 뒤 embedding은 lazy reload한다. Dagster GPU0 작업과의 상호배제도 lease를 확인하도록 추가한다. |
| 모델 | 초기에는 FLUX.2 Klein 4B FP8와 SDXL inpaint의 고정 버전만 허용한다. 파일은 시작 시 자동 다운로드하지 않고 checksum 검증 후 operator가 준비한다. |
| 워크플로 | repository에서 JSON template를 버전 관리한다. Native frontend의 Run graph는 GenAI gateway가 template와 정확히 대조하며, 승인된 scalar 입력 변경만 adapter가 다시 bind한다. 임의 node/connection/model/output 변경은 거부한다. |
| 출력 | GenAI의 격리 NAS 경로를 계속 사용한다. `incoming/`에 직접 쓰지 않으며 Promote가 유일한 pipeline 진입점이다. |
| 학습 편입 | `label_policy=required`를 기본·권장값으로 강제하고 Label Studio 최종 확정 전에는 데이터셋에 포함하지 않는다. |
| 롤백 | `comfyui` profile 또는 `comfy_local` enable flag만 끄면 기존 Kling/Veo와 Dagster 경로에 영향 없이 중단된다. 생성된 NAS/PG provenance는 삭제하지 않는다. |
| 자동화 모드 | `disabled → plan_only → approval_required → auto_dispatch` 상태를 정책별로 둔다. 초기 production은 `plan_only` 또는 `approval_required`만 허용한다. |
| 분포 분모 | raw 파일·요청 수·자동 라벨 수가 아니라, **사람이 finalized한 image-event unit**만 coverage 분모/분자로 센다. 실패·미검수·반려 생성본은 부족분을 메운 것으로 계산하지 않는다. |
| 카메라/환경 | 첫 balance axis는 사람 검증이 끝난 `environment_type`, `daynight_type`, `weather`와 canonical event class만 쓴다. `camera_angle`은 현재 라벨러가 bin 붕괴를 보여 제외하고, `source_unit_name`은 카메라 ID가 아니므로 camera/site 비율의 키로 쓰지 않는다. |

## 4. 구현 단계

### Phase A — 계약과 GPU 안전장치

**목적:** local generation이 기존 GPU 추론과 경쟁하거나, 검수 전 데이터를 ingest하지 않도록 계약을 먼저 만든다.

1. `src/vlm_pipeline/sql/migrations/postgres/`에 새 migration을 추가한다.
   - `genai_batches.engine` 및 `raw_files.genai_engine` CHECK에 `comfy_local`을 추가한다.
   - workflow ID, template version/hash, model manifest hash, seed, input/control checksum, Comfy prompt ID, GPU lease timing을 보존할 구조를 정의한다. batch 단위 `options_json`으로 충분하지 않은 job별 값은 전용 JSONB provenance 컬럼 또는 별도 `genai_job_provenance` 테이블로 분리한다.
   - 기존 `002_genai.sql` / `003_genai_veo.sql`을 수정하지 않고 순방향 migration만 작성한다.
2. `docker/embedding/`과 Dagster의 GPU0 작업이 공통으로 확인할 최소 GPU lease API/DB helper를 설계·구현한다.
   - stale owner TTL, heartbeat, owner/job ID, 명시적 release를 포함한다.
   - 메모리 부족은 OOM 이후 복구가 아니라 제출 전 defer로 처리한다.
   - 기존 `gpu_maintenance_lock`은 SAM3/PE-Core 서빙 정비 상태 전용이므로 용도를 넓히지 않는다. Comfy task 경쟁 제어는 별도 `generation_gpu_leases`로 구현한다.
3. `comfy_local` 엔진 이름을 모든 allowlist에 일관되게 반영한다.
   - `docker/genai/jobs/promote.py`
   - `src/vlm_pipeline/defs/ingest/ops_register.py`
   - migration CHECK constraint 및 관련 테스트
4. `source_type=genai_output`, `genai_engine=comfy_local`이라는 기존 provenance 계약은 유지한다. 새 source type을 만들지 않는다.

**완료 기준:** migration을 적용한 빈 PG fixture와 기존 PG에 모두 재적용 가능하고, `comfy_local` batch는 Promote 전 raw_files에 생기지 않으며, lease 충돌 시 두 번째 job이 GPU를 점유하지 않는다.

### Phase A0 — coverage 사실 테이블과 reference pool 선행 구축

**목적:** "무엇이 얼마나 부족한가"를 raw media나 모델 추측이 아니라 검증 가능한 라벨·환경 사실로 계산하게 한다. 이 단계가 끝나기 전 자동 generation은 활성화하지 않는다.

1. finalized Label Studio 결과를 coverage용 단위로 투영한다.
   - `labels.caption_text`나 Gemini event JSON을 직접 count하지 않는다. 현재 labels 테이블에는 canonical event category가 구조화되어 있지 않고, finalized bbox는 주로 `patient`/`person` 객체이므로 fall-down/fire/smoke 비율의 정본이 될 수 없다.
   - Label Studio의 finalized event taxonomy를 `label_classes.canonical`로 정규화하여 `coverage_unit_facts`에 투영한다. grain은 `(image_id, canonical_class)`의 긍정 event 한 건이며, source label/LS task/finalized 시각을 보존한다.
   - synthetic output도 동일한 투영을 거쳐야 하며, `raw_files.source_type='genai_output'`와 `genai_engine='comfy_local'`은 origin 표시에만 사용한다. synthetic이라는 사실을 숨기거나 real count와 합쳐서만 보지 않는다.
2. 환경/카메라 context를 별도 fact로 검증한다.
   - `coverage_context_facts`는 `environment_type`, `daynight_type`, `weather`, `subject_scale`, `occlusion_state`, context source/model version, 검증 상태, reference asset/image를 기록한다.
   - 현행 `video_metadata`의 값은 read-side input으로만 쓰며, `deferred`, `indeterminate`, `unknown`은 target count에서 제외하고 별도 missing bucket으로 보고한다.
   - `environment_type/daynight_type/weather`은 기존 육안 검증을 통과한 뒤에만 pilot axis로 사용한다. `subject_scale`/`occlusion_state`는 별도 표본 검증 뒤 승격한다. `camera_angle`은 현재 사용 불가다.
   - camera/site 균형이 필요해지면 `camera_registry`와 `asset_camera_map`을 별도 구축한다. `source_unit_name`은 처리 단계/여러 카메라가 섞인 값이라 대체키로 금지한다.
3. Comfy 입력으로 쓰일 source는 `generation_reference_pool`에서만 선택한다.
   - reference image, 원 source asset/image, 검증된 context, 허용 workflow, 안전한 편집 영역(mask 또는 polygon), holdout 제외 여부, 승인자, 유효기간을 저장한다.
   - inpaint의 자동 실행에는 source마다 검증된 safe region이 필요하다. mask 없는 임의 CCTV에 "사람을 추가"하도록 맡기는 방식은 장면 훼손·출입구/안전설비 가림 위험 때문에 자동화 대상이 아니다.
   - reference는 평가 holdout camera/site에 속하거나, 실제 최종평가에 사용할 원본과 동일 사건/연속 프레임이면 안 된다.

**완료 기준:** 각 pilot class/context cell에서 (a) finalized event 사실, (b) 검증된 context, (c) 승인된 reference pool 수를 별도 집계할 수 있다. 어느 하나라도 0이거나 unknown이면 planner는 생성하지 않고 `blocked_*` 사유를 남긴다.

### Phase B — ComfyUI 서비스와 재현 가능한 모델/워크플로

**목적:** 운영 파이프라인과 독립적으로 재현 가능한 내부 ComfyUI worker를 만든다.

1. `docker/comfyui/`에 pin된 ComfyUI revision과 PyTorch/CUDA 조합의 Dockerfile 및 entrypoint를 추가한다.
   - 현재 GPU 검증이 된 CUDA 12.8/PyTorch 계열과 호환성을 확인한다.
   - ComfyUI-Manager나 런타임 `git pull`/`pip install`은 포함하지 않는다.
   - `/health` 또는 `/system_stats`를 이용한 healthcheck, structured startup log, model manifest 검증을 제공한다.
2. `docker/docker-compose.yaml`에 `profiles: ["comfyui"]` 서비스를 추가한다.
   - `gpus: all`을 쓰더라도 환경 변수와 device request로 GPU0만 노출되는지 compose config와 런타임 양쪽에서 검증한다.
   - 외부 `ports:`는 두지 않고 internal `expose: ["8188"]`만 둔다.
   - 모델은 `./data/models/comfyui:/models:ro`, Comfy 임시 output은 전용 named volume 또는 `./data/comfyui`로 분리한다.
   - NAS 전체 write mount는 ComfyUI에 주지 않는다. NAS 쓰기는 GenAI finalize만 담당한다.
3. workflow template와 model manifest를 git 추적한다.
   - `flux2-klein-4b-edit-v1.json`, `sdxl-inpaint-cctv-v1.json`
   - model 파일명, SHA-256, 라이선스 식별자, Comfy node revision, 예상 VRAM, 입력 schema를 명시한다.
   - SDXL inpaint는 source image와 동일 해상도의 binary mask를 요구하고, initial POC는 batch당 1 source+1 mask로 제한한다.
4. `scripts/deploy/deploy-stack.sh` 및 build 감지 workflow에 `comfyui` profile의 build/recreate/health lifecycle을 명시적으로 추가한다. 활성 profile이 아닐 때는 touch하지 않는다.

**완료 기준:** staging에서 `comfyui` 컨테이너는 GPU0만 본다. 모델 게이트는 **"기동 가능 여부"와 "건강 여부"를 다른 노브로 분리**한다 — `COMFYUI_FAIL_FAST_ON_MODEL_ERROR`(기본 `false`)가 부팅 중단을, `COMFYUI_REQUIRE_MODELS`(기본 `true`)가 health 판정을 지배한다. 모델이 아직 배치되지 않아도 컨테이너는 **기동**하고(crash-loop 는 사유를 보고할 채널 자체를 없앤다), `COMFYUI_REQUIRE_MODELS=false`인 bring-up 환경에서는 healthy 로 남는다. 반면 **manifest 불일치**(파일은 있으나 size/SHA-256 이 다름 · 경로 이탈 · manifest 손상)는 **두 플래그와 무관하게 항상 unhealthy** 다. 즉 "모델 없이도 healthy"는 미배치(bootstrap)에만, "불일치 시 unhealthy"는 무조건 적용된다 — 한 플래그가 둘을 동시에 지배해 두 기준이 동시 성립할 수 없던 자기모순은 이 severity 분리로 해소한다(잔여 작업 계획서 P2-2). ComfyUI를 끄거나 restart해도 GenAI, embedding, SAM3, Dagster는 살아 있다.

### Phase C — GenAI Studio `comfy_local` 어댑터와 제한된 UI

**목적:** 사용자가 GenAI Studio만 통해 안전한 template 실행을 요청하게 한다.

1. `docker/genai/adapters/comfy_local.py`를 추가하고 registry/UI tab/engine option에 연결한다.
   - `submit`: 입력을 ComfyUI input 영역에 안전한 unique 이름으로 upload하고 versioned template에 prompt·seed·input을 주입한 뒤 `/prompt`에서 `prompt_id`를 받는다.
   - `poll`: `/history/{prompt_id}`에서 완료/실패/실행 중을 판정하고 단 하나의 PNG 출력만 선택한다.
   - `download_result`: 내부 `/view` endpoint로만 다운로드한다. filename/path traversal, 예상 밖 media type, 다중 output은 거부한다.
   - timeout, Comfy restart, history 누락, GPU deferred, OOM을 재시도 가능한 상태와 영구 실패 상태로 구분한다.
2. Studio 입력을 엔진별 schema로 확장한다.
   - `comfy_local`에는 `workflow_id`, event preset, prompt, negative prompt, seed가 필요하다.
   - `sdxl-inpaint-cctv-v1`에는 source 1장 + mask 1장만 받는다. 다른 기존 엔진의 multi-image/bulk 동작은 회귀시키지 않는다.
   - UI는 workflow의 예상 효과·제약과 `synthetic / human review required` 배지를 표시한다.
3. GenAI의 기존 job/finalize path를 재사용한다.
   - output은 `outputs/`에 atomic write, batch 완료 시 manifest 작성이라는 현재 계약을 그대로 쓴다.
   - `provenance.json`에는 source/mask SHA-256, workflow ID·SHA, model manifest SHA, prompt/negative prompt, seed, sampler/steps/CFG, Comfy prompt ID, 생성 시각, output SHA-256을 쓴다.
   - prompt 또는 파일 이름만으로 원본 CCTV 신원정보가 불필요하게 로그에 복제되지 않도록 UI 표시용 값과 audit 값의 접근 범위를 분리한다.
4. 비용 탭은 `comfy_local`에 외부 API 비용을 표시하지 않고, GPU runtime/queue wait를 별도 운영 지표로 표시한다.

**완료 기준:** Studio 인증 사용자는 `comfy_local`의 두 template 중 하나만 선택 가능하고, 임의 workflow/파일경로를 보낼 수 없다. 생성 성공·실패·재시작 모두 PG/NAS 상태가 일치하며, 기존 Kling/Veo submission과 polling은 회귀가 없다.

### Phase D — Promote, 라벨링, 데이터셋 품질 게이트

**목적:** 합성 이미지가 실제 CCTV 데이터로 오인되거나 검수 없이 학습셋에 들어가는 것을 막는다.

1. existing Promote UI/API가 `comfy_local`을 허용하도록 갱신하고, 기본값을 다음으로 고정한다.
   - `label_policy=required`
   - image workflow의 `labeling_method=[captioning_image,bbox]`
   - operator가 event class/categories를 명시적으로 선택
2. dispatch manifest에 `source_type=genai_output`, `genai_engine=comfy_local`, batch/job 연결 및 provenance 파일 위치를 전달한다.
3. Label Studio task에 synthetic provenance 및 intended event를 표시한다. 검수자는 최소한 다음을 판정한다.
   - 요청한 이벤트가 실제로 보이는가
   - background/camera geometry가 보존됐는가
   - 마스크 경계·인체·불/연기 artifact가 허용 가능한가
   - bbox와 event label이 실제 생성 결과에 맞는가
4. dataset build의 `finalized` gate를 검증한다. `label_policy=none`, 미검수, 실패/partial batch는 training dataset에 들어가지 않아야 한다.
5. 모델 품질 평가는 합성 데이터만으로 하지 않는다.
   - 카메라/site 단위로 real-only holdout을 고정한다.
   - synthetic augmentation 비율별(real only / 10% / 25% 등) 탐지 성능을 비교한다.
   - fall-down, fire/smoke, smoking을 별도 strata로 보고 smoking처럼 작은 객체는 고해상도 crop 검수 조건을 추가한다.

**완료 기준:** 단일 Comfy batch를 Promote했을 때 raw_files와 image metadata에 `comfy_local` provenance가 연결되고, Label Studio finalized 전에는 build query 결과가 0건이다. finalized 후에만 의도한 클래스와 source type을 포함한 dataset manifest가 만들어진다.

### Phase E — staging 검증, 점진 rollout, rollback drill

1. `dev`/staging에서 모델을 미리 수동 배치하고 checksum 검증 뒤 compose profile을 켠다.
2. 아래 순서로 E2E를 시행한다.
   - service boot + GPU0 visibility + no-public-port 확인
   - FLUX Klein 1장 edit, SDXL inpaint 1장+mask
   - queue contention/defer, Comfy restart mid-job, insufficient VRAM, malformed history 응답
   - existing Kling/Veo 1건씩 regression
   - GenAI finalize → Promote → dispatch → Label Studio → finalized → dataset build
3. 20개 이상의 서로 다른 실제 CCTV 장면을 대상으로 operator QA를 실시한다. event별 성공/거절/재작업 사유와 peak VRAM·runtime을 기록한다.
4. 24시간 staging 관찰 후 prod에 승격한다. production은 첫 주 `comfy_local` batch 수와 동시성을 env upper limit으로 더 낮게 설정한다.
5. rollback drill: `comfyui` profile disable → GenAI Studio에서 engine 숨김 → 진행 중 job을 `failed_retryable`로 mark → GPU lease release → 기존 서비스 health 확인. 원본/출력/provenance는 삭제하지 않는다.

**완료 기준:** staging 24시간 동안 SAM3/embedding/Dagster의 OOM·지속적 5xx가 없고, rollback이 10분 이내에 기존 자동 라벨링을 정상 상태로 되돌린다.

### Phase F — 비율 기반 synthetic coverage planner와 campaign 자동화

**목적:** 사용자가 정의한 class/context 목표 비율의 부족분만, 예산·reference·품질 게이트 안에서 보충한다.

1. Dagster `synthetic_coverage_planner_schedule`을 추가한다.
   - 하루 1회 off-peak에 snapshot을 읽어 policy별 deficit을 계산한다. 기본 상태는 STOPPED이며, `coverage_ready` 검증을 통과한 policy만 수동으로 시작할 수 있다.
   - planner는 ComfyUI를 직접 호출하지 않는다. snapshot, candidate, campaign/task 행만 PostgresResource(`db`)로 기록한다.
   - 산출물이 없는 날은 SkipReason으로 끝나며, 모델을 깨우거나 GPU를 점유하지 않는다.
2. campaign dispatch를 planner와 분리한다.
   - `synthetic_campaign_dispatch_sensor`는 승인된 campaign의 예약 task를 한 번에 최대 1건만 GenAI의 authenticated internal endpoint에 보낸다.
   - GenAI가 trusted MinIO source frame을 materialize → `comfy_local` adapter submit → 기존 poll/finalize를 수행한다. Dagster가 ComfyUI graph를 직접 조작하지 않는다.
   - GPU lease, Comfy queue, 일/주 budget, reference availability 중 하나라도 부족하면 task는 `deferred`로 남고 다음 tick에서 재평가한다.
3. policy별 mode gate를 적용한다.
   - `plan_only`: campaign 초안과 예상 비용/VRAM/부족 cell만 만든다.
   - `approval_required`: 운영자가 campaign을 승인해야 dispatch 가능하다. POC와 초기 production의 기본값이다.
   - `auto_dispatch`: 동일 workflow·같은 context axis에서 사전 승인된 품질 기준과 수용률, rollback drill을 충족한 policy에 한해서만 허용한다.
4. Label Studio 결과로 feedback loop를 닫는다.
   - 최종 검수 `accepted` + finalized event/context만 coverage snapshot에 반영한다.
   - reject·artifact·wrong-event·wrong-context는 reservation을 해제하고 `generation_quality_reviews`의 reason code로 축적한다. 단순 재시도로 무한 생성하지 않으며 target/reference/workflow별 retry ceiling을 둔다.
   - 생성 수가 아니라 accepted/finalized 수를 다음 회차의 balance count로 사용한다.

**완료 기준:** planner가 target ratio와 final count에서 결정론적으로 같은 campaign 초안을 만들며, 승인하지 않은 campaign은 ComfyUI 요청을 0건 생성한다. accepted synthetic share와 budget cap을 초과하는 campaign은 생성 전에 block된다.

## 5. 자동 보충 계산 규칙

### 5.1 policy는 고정된 상호배타적 cell 집합만 다룬다

한 policy는 하나의 `balance_dimensions` 집합만 선언한다. 예를 들어 `['class','environment_type','daynight_type']`이며, 각 target row는 세 값 모두를 갖는다. wildcard가 섞인 중첩 target이나 서로 다른 차원의 target을 한 policy에 섞으면 동일 이미지가 여러 deficit을 메울 수 있으므로 금지한다. 다른 균형 목적은 별 policy로 분리한다.

첫 pilot의 예시는 다음처럼 작게 시작한다.

| class | environment | day/night | 목표 비율 |
|---|---|---|---|
| falldown | indoor | day | 0.35 |
| falldown | indoor | night | 0.25 |
| falldown | outdoor | day | 0.25 |
| falldown | outdoor | night | 0.15 |

`weather`는 target 비율이 아니라 reference 선택 filter로 먼저 쓴다. 충분한 validated observation가 쌓인 뒤 별 policy의 balance dimension으로 승격한다. 현재처럼 context 값이 deferred/unknown이면 해당 policy는 `blocked_context_coverage`가 되어야 하며, unknown을 균등 분배로 추정해서는 안 된다.

### 5.2 계획 horizon과 deficit

비율만으로는 자동 생성이 끝없이 늘어날 수 있으므로, 모든 policy는 명시적인 `horizon_finalized_total` 또는 cell별 `min_finalized_count`를 가져야 한다. snapshot 시점의 cell `i`에 대해:

```text
desired_i = max(min_finalized_count_i,
                ceil(horizon_finalized_total × target_share_i))
deficit_i = max(0, desired_i − eligible_finalized_i)
```

여기서 `eligible_finalized_i`는 event와 context가 모두 finalized/verified된 고유 image-event 수다. pending, auto-generated, output 파일만 존재하는 batch, failed/partial job은 0으로 계산한다.

생성 예약 수는 다음의 최솟값이다.

```text
planned_i = min(deficit_i,
                policy.max_per_campaign,
                policy.remaining_daily_budget,
                eligible_reference_pool_i,
                synthetic_share_headroom_i)
```

synthetic share는 cell마다 별도로 제한한다. `R`=real finalized, `S`=accepted synthetic, `P`=새 예약, 최대 비율 `a`일 때 다음을 만족하는 `P`만 허용한다.

```text
(S + P) / (R + S + P) ≤ a
```

예약 상태도 `P`에 포함해 동시에 여러 planner tick이 같은 여유를 초과 배정하지 못하게 한다. share cap·daily GPU seconds·max failures·reference 수 중 하나가 0이면 campaign은 부분 생성이 아니라 명시적 block/defer 사유를 남긴다.

### 5.3 자동화가 선택하는 것은 "이미 검증된 배경"이다

주변환경을 새로 text-to-image로 발명하지 않는다. planner는 target context와 일치하는 `generation_reference_pool`의 CCTV frame을 선택하고, 해당 frame의 pre-approved safe mask/region 안에서만 `sdxl-inpaint-cctv-v1` 또는 `flux2-klein-4b-edit-v1`를 실행한다. 따라서 생성 이미지의 context는 `inherited_from_reference`로 시작하고, 최종 검수에서 scene preservation이 통과한 경우에만 `verified`가 된다.

이 방식은 카메라 화각·압축 노이즈·조명·배경 비율을 실제 CCTV 분포에서 유지하고, "야간 CCTV"라는 텍스트 프롬프트만으로 가짜 환경을 대량 생산하는 편향을 피한다.

## 6. DB 설계

기존 `raw_files`/`video_metadata`/`image_metadata`/`genai_batches`/`genai_jobs`는 원본·처리·GenAI 실행의 정본 역할을 그대로 유지한다. 아래 테이블은 coverage와 campaign의 **제어·감사 계층**이며, 라벨의 정본을 대체하지 않는다. 모든 migration은 새 번호의 forward-only SQL로 작성하고 `@ASSERT_AFTER`를 둔다.

### 6.1 사실·reference 계층

| 테이블 | grain / 핵심 열 | 역할 |
|---|---|---|
| `coverage_unit_facts` | `(image_id, canonical_class, fact_source)`; `asset_id`, `label_id`, `ls_task_id`, `review_status`, `origin_kind`, `finalized_at` | finalized LS event를 planner가 셀 수 있는 image-event 단위로 투영한다. `origin_kind`은 real/synthetic을 구분하며 FK는 기존 image/raw/label과 연결한다. |
| `coverage_context_facts` | image 또는 asset당 1 context version; 6축, `context_source`, `model_version`, `verification_status`, `verified_at` | real 영상 메타데이터와 synthetic의 reference-inherited context를 공통 형태로 보관한다. deferred/unknown은 verified가 아니다. |
| `camera_registry` / `asset_camera_map` | 안정 `camera_id`와 asset mapping | 미래 site/camera ratio 및 split leakage 방지용. source unit 값을 그대로 넣지 않는다. 첫 pilot에서 불필요하면 비활성이다. |
| `generation_reference_pool` | reference image당 1행; `reference_id`, image/asset FK, `context_fact_id`, `safe_region_json`, allowed workflows, `holdout_excluded`, status | 자동 생성이 사용할 수 있는 승인 CCTV 배경 풀. raw path 대신 DB FK와 trusted MinIO object key로 materialize한다. |

`coverage_unit_facts`는 Label Studio submit/finalize 흐름에서 갱신하거나, 기존 finalized 결과를 backfill하는 projection job으로 채운다. `image_label_annotations.category`는 객체 class이므로 event fact를 대신할 수 없으며, event taxonomy가 LS form에 구조화되어 있지 않다면 그 form/API mapping을 먼저 추가해야 한다.

### 6.2 정책·snapshot·campaign 계층

| 테이블 | grain / 핵심 열 | 무결성 규칙 |
|---|---|---|
| `synthetic_coverage_policies` | policy version; `policy_id`, status, mode, `balance_dimensions`, `horizon_finalized_total`, daily/weekly GPU·job budget, `max_synthetic_share`, default workflow, schedule, `holdout_scope`, approved by/at | active policy는 승인자·nonzero horizon·정규화된 dimensions가 필수. 상태는 draft/active/paused/retired만 허용한다. |
| `synthetic_coverage_targets` | policy의 상호배타적 target cell; `target_id`, `dimensions_json`, `dimensions_hash`, `target_share`, `min_finalized_count`, per-campaign cap, priority | `(policy_id, dimensions_hash)` UNIQUE. policy activate 시 share 합=1, 선언된 dimension 전부 존재, target 간 중복 없음 검증. |
| `coverage_snapshots` | policy별 계산 시점; input query/config hash, as-of, status | snapshot은 immutable. 어떤 데이터/정책으로 campaign을 만들었는지 재현한다. |
| `coverage_snapshot_cells` | snapshot × target cell; real/synthetic finalized count, pending reservation, deficit, block reason, reference availability | target별 index. unknown/deferred/missing count는 별 열로 표시해 숫자 0과 관측 불가를 구분한다. |
| `synthetic_generation_campaigns` | snapshot에서 나온 1회 계획; campaign status, planned/accepted/rejected counts, budget reservation, idempotency key, approval/audit fields | 동일 policy + schedule bucket + snapshot input hash는 UNIQUE. 상태 전이는 planned→approved→dispatching→awaiting_review→closed 또는 blocked/cancelled만 허용한다. |

### 6.3 task·provenance·품질 계층

| 테이블 | grain / 핵심 열 | 역할 |
|---|---|---|
| `synthetic_generation_tasks` | campaign의 출력 1장; `task_id`, target/reference FK, workflow/template/model manifest hash, rendered prompt hash, seed, mask checksum, state, retry count, `genai_batch_id`/`genai_job_id`, GPU lease/runtime | dispatch와 GenAI execution을 연결한다. `(campaign_id, reference_id, workflow_id, seed)` UNIQUE로 중복 제출을 막는다. |
| `synthetic_prompt_templates` | template version; variables schema, renderer version, status, content hash | Gemini용 `generation_prompts`와 섞지 않는다. Comfy prompt와 negative prompt의 승인된 template 원장이다. |
| `generation_quality_reviews` | task별 generation-specific review; event fidelity, context preserved, artifact severity, duplicate/diversity result, reviewer, reason codes, LS task/label links | LS의 최종 annotation을 대체하지 않는 보조 audit다. coverage 반영은 `accepted`와 LS `finalized` 둘 다 충족할 때만 한다. |
| `generation_budget_events` | campaign/task별 reserve/consume/release event, GPU seconds, count, reason | 실패·반려 시 reservation 반환과 일/주 cap의 정확한 재집계를 보장한다. |
| `generation_gpu_leases` | resource=`gpu0_comfy`당 active lease; owner task, token, acquired/heartbeat/expires/released time, state | Comfy generation의 단일 실행권을 보장한다. 기존 `gpu_maintenance_lock`과 분리해 "서빙 정비"와 "짧은 생성 작업"의 fail-safe 규칙을 혼합하지 않는다. |

`genai_batches.options_json`에는 UI 옵션만 남기고, 자동화의 상태·quota·provenance를 모두 넣지 않는다. 그렇게 하면 JSON 파싱에 의존한 집계와 상태 전이가 생기므로 위 control-plane 테이블을 별도로 둔다.

### 6.4 핵심 뷰와 인덱스

1. `v_eligible_coverage_units`: `coverage_unit_facts` + verified `coverage_context_facts`를 결합한다. `finalized`, real/synthetic origin, policy dimension만 노출하며 planner의 유일한 count source다.
2. `v_generation_reference_candidates`: target cell과 pool을 join해 allowed workflow, safe region, holdout exclusion, context 일치, 최근 사용 횟수, duplicate risk를 계산한다.
3. `(policy_id, status, scheduled_for)`, `(campaign_id, state, priority)`, `(reference_id, status)`, `(snapshot_id, dimensions_hash)`에 partial index를 둔다. planner가 큰 raw/image 테이블을 매 tick 전체 스캔하지 않도록 snapshot은 일 단위 materialization 또는 incrementally refreshed aggregate를 사용한다.
4. DB write는 모두 `PostgresResource(db)`를 통해 수행한다. GenAI container의 direct SQL은 기존 batch/job lifecycle에 한정하고, policy 활성화·coverage snapshot·campaign state는 Dagster service layer가 소유한다.

## 7. 자동화 rollout 게이트

| 단계 | 허용 동작 | 차단 조건 |
|---|---|---|
| 0. Fact readiness | context/event/reference 현황만 측정 | deferred/unknown 과다, event taxonomy mapping 없음, holdout 구분 없음 |
| 1. Plan-only | daily snapshot, deficit/cost/expected output report | GPU·Comfy 호출 금지 |
| 2. Approval-required POC | 운영자 승인 campaign을 최대 1건씩 submit | acceptance/artifact/duplicate 기준 미달, lease/rollback 미검증 |
| 3. Limited auto-dispatch | 사전 승인 policy의 capped task만 submit | daily budget/share/retry/reference cap 중 하나 초과 |
| 4. Dataset impact evaluation | real-only holdout으로 synthetic 비율별 성능 비교 | synthetic only 평가, camera/site leakage, finalized 게이트 우회 |

자동 dispatch로 올라가더라도 자동 Promote와 자동 dataset inclusion은 하지 않는다. `comfy_local` output은 항상 사람이 Promote하고 Label Studio에서 finalized해야 한다.

## 8. 테스트 매트릭스

| 계층 | 필수 검증 |
|---|---|
| Unit | template allowlist, 파라미터 schema, prompt graph 주입, history parser, `/view` URL 검증, provenance checksum, GPU lease TTL/release |
| GenAI integration | `comfy_local` submit/poll/finalize, deferred 재제출, Comfy restart/OOM 처리, 기존 엔진 회귀 |
| DB/migration | fresh PG와 upgrade PG의 engine CHECK, raw_files provenance, duplicate Promote claim, migration idempotency |
| Pipeline integration | Promote payload → dispatch manifest → ops_register → Label Studio finalized gate → build source metadata |
| Runtime | GPU0만 노출, GPU1/SAM3 불변, no host port, service health, VRAM peak/queue/lease metrics |
| UAT | 원본과 결과 side-by-side 검수, event fidelity, camera geometry, artifact 거절, real-only holdout 성능 비교 |
| Coverage planner | target cell exclusivity/share validation, immutable snapshot 재현, unknown≠0 처리, deficit/share-cap 계산, budget reservation/release, idempotent schedule bucket |
| Campaign E2E | plan-only에서 GPU 호출 0건, approval gate, reference/holdout exclusion, deferred 재개, Comfy output→LS finalized 뒤에만 coverage count 반영 |

## 9. 배포 순서와 소유 파일

| Wave | 변경 영역 | 주요 파일 |
|---|---|---|
| 1 | 사실 projection·coverage schema·GPU lease | `src/vlm_pipeline/sql/migrations/postgres/`, `src/vlm_pipeline/defs/ingest/`, Label Studio finalize projection, GPU resource/helper, tests |
| 2 | Comfy worker | `docker/comfyui/`, `docker/docker-compose.yaml`, `scripts/deploy/deploy-stack.sh`, build detection workflow, workflow/model manifest |
| 3 | Studio adapter/UI | `docker/genai/adapters/`, `docker/genai/jobs/`, `docker/genai/app.py`, templates/static, tests |
| 4 | Promote/label quality | dispatch metadata, GenAI promote UI, Label Studio presentation/config, dataset integration tests, operator guide |
| 5 | coverage planner/campaign control plane | `src/vlm_pipeline/defs/genai/`, DB resource/service, GenAI internal campaign endpoint, schedule/sensor, coverage/report tests |
| 6 | staging→prod | `.env.test`/`.env` local configuration, model placement, E2E evidence, rollback record |

Waves 1 and 2 may begin in parallel only after the provenance schema and GPU lease contract are agreed. Waves 3–4 are sequential because the adapter depends on both contracts. Wave 5는 finalized event/context facts와 `comfy_local` provenance가 실제로 end-to-end로 남는 것을 확인한 뒤에 시작한다.

## 10. 설정 초안

다음은 git 추적 compose 기본값 또는 각 환경의 untracked `.env`에 둔다. 실제 model path/checksum은 operator가 승인한 뒤 입력한다.

```env
# profile activation: existing profiles...,comfyui
COMFYUI_ENABLED=true
COMFYUI_INTERNAL_URL=http://comfyui:8188
COMFYUI_MAX_CONCURRENT=1
COMFYUI_GPU_INDEX=0
COMFYUI_MIN_FREE_VRAM_GB=13
COMFYUI_JOB_TIMEOUT_SECONDS=900
COMFYUI_ALLOWED_WORKFLOWS=flux2-klein-4b-edit-v1,sdxl-inpaint-cctv-v1
GENAI_ENGINES_ENABLED=kling,veo,comfy_local
SYNTHETIC_COVERAGE_PLANNER_ENABLED=false
SYNTHETIC_COVERAGE_MODE=plan_only
SYNTHETIC_COVERAGE_MAX_CAMPAIGN_TASKS=0
```

`COMFYUI_MIN_FREE_VRAM_GB`와 timeout은 staging benchmark 결과로 확정한다. production/staging의 secret 및 모델 액세스 토큰은 git에 넣지 않는다.

## 11. 승인 게이트

구현 착수 전에 다음을 확정한다.

1. synthetic CCTV 이미지의 사용 목적이 FLUX.2 Klein 4B의 Apache-2.0 조건과 조직의 데이터 사용 정책에 부합하는지.
2. POC의 첫 이벤트 범위: `falldown` 단독으로 시작할지, `fire/smoke`까지 포함할지.
3. GPU0의 Comfy lease 동안 허용할 Dagster GPU0 작업의 정책: defer(권장) 또는 별도 정비 윈도우.
4. 20장 QA와 real-only holdout 비교를 통과 기준으로 삼을지, 그리고 허용 artifact/거절 기준.
5. 첫 `synthetic_coverage_policy`의 horizon, target cell 비율, cell별 synthetic share cap, GPU 시간·일일 task budget.
6. Label Studio가 finalized event taxonomy와 context-preservation 판정을 구조화해 반환할 방식.

이 여섯 가지가 정해지기 전에는 자동 campaign dispatch, production profile 활성화, 자동 coverage 보충을 수행하지 않는다.
