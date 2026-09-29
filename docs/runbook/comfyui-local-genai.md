# GPU0 ComfyUI · GenAI Studio 운영 가이드

## 현재 범위

이 구성은 GenAI Studio(`:8089`)에서만 호출할 수 있는 내부 이미지 생성 worker다.
ComfyUI에는 host port와 NAS mount가 없으며 다음 두 repository-owned workflow만 허용한다.

GenAI Studio 상단의 **Comfy Nodes** 탭은 공식 ComfyUI frontend를 same-origin reverse proxy로
표시한다. 캔버스 이동·확대·node 편집·이미지 업로드 UX는 ComfyUI와 동일하지만, Run 요청은
GenAI gateway가 가로챈다. gateway는 repository template와 node ID/class/input key/connection 및
고정 model/output 값을 대조하고 source, mask, prompt, negative prompt, seed, steps, CFG, denoise만
변경된 graph를 허용한다. 승인된 graph도 기존 `submit_batch`/adapter/provenance 경로로 다시
제출되므로 브라우저에서 node를 추가하거나 model loader를 바꿔도 실행되지 않는다.

모델이 아직 배치되지 않았거나 `comfy_local`이 비활성인 환경에서도 canvas는 `view/edit only`로
열 수 있다. 이 상태의 Run은 503으로 차단된다. ComfyUI worker는 여전히 host port 없이 내부
network에만 있고, queue clear·interrupt·model free는 GenAI가 관리하므로 frontend 요청은 403이다.

| workflow | 입력 | 모델 | 제한 |
|---|---|---|---|
| `flux2-klein-4b-edit-v1` | source 1장 + prompt | FLUX.2 Klein 4B FP8 | distilled 4 steps 고정 |
| `sdxl-inpaint-cctv-v1` | source 1장 + 동일 크기 binary PNG mask | SDXL base 1.0 | 흰색(255) 영역만 편집 |

## 여러 이미지 × 여러 프롬프트

`Comfy Nodes` 상단의 **N×M Bulk** 또는 GenAI Studio의 **Bulk** 탭에서 로컬 ComfyUI 대량
생성을 제출할 수 있다. `cartesian (N×M)`을 선택하면 다음 세 경우를 같은 방식으로 처리한다.

- 이미지 N장 × 프롬프트 1개 → N jobs
- 이미지 1장 × 프롬프트 M개 → M jobs
- 이미지 N장 × 프롬프트 M개 → N×M jobs

노드 캔버스에서 프롬프트와 seed/steps를 조정한 상태로 이미지 여러 장에 바로 적용하려면 상단의
**M×1 Batch**를 사용한다. 여기서 source M장을 선택하면 현재 승인 graph를 다시 검증한 뒤 node의
프롬프트와 설정을 M개 job에 공통 적용한다. SDXL은 source와 같은 개수의 mask를 같은 순서로
추가해야 한다. 임의 node/model 변경은 단건 Run과 동일하게 서버에서 거부된다.

프롬프트는 입력창에 한 줄에 하나씩 작성한다. 한 요청은 기본적으로 최대 50 jobs이며 모두 같은
`bulk_group_id`로 기록된다. GPU0 동시성은 계속 1이므로 jobs는 병렬 생성되지 않고 순차 처리된다.
`seed=0`은 job마다 실제 seed를 새로 뽑아 provenance에 저장한다.

`paired (1:1)`은 이미지와 프롬프트 수가 같을 때 `image[i] → prompt[i]`만 생성한다. 전체 조합이
필요하면 반드시 `cartesian`을 선택한다.

SDXL Inpaint에서는 원본과 마스크를 같은 순서로 같은 개수만큼 선택한다. `mask[i]`는 모든
프롬프트 조합에서 `image[i]`에 재사용된다. 서버는 각 source/mask의 해상도, 빈 마스크 여부,
0/255 이진값을 제출 전에 검증한다. FLUX workflow에는 마스크를 첨부할 수 없다.

coverage 기반 자동 생성은 이 구성에 포함되지 않는다. 모든 결과는 격리 폴더에 저장되고,
`label_policy=required` Promote와 Label Studio 사람 검수를 거쳐야만 데이터셋 경로로 진입한다.

### 2026-09-18 운영 활성화 상태

- manifest의 네 파일(FLUX.2 Klein FP8, Qwen 3 4B, FLUX.2 VAE, SDXL base)을 운영 호스트에
  배치했고 size/SHA-256 전체 검증을 통과했다.
- `COMFYUI_REQUIRE_MODELS=true`, `COMFYUI_VERIFY_MODEL_HASHES=true`로 기동하며
  `GENAI_ENGINES_ENABLED`에 `comfy_local`이 활성화되어 있다.
- FLUX 4-step, SDXL Inpaint 30-step 및 native canvas upload/Run gateway smoke가 성공했다.
  두 결과 모두 격리 NAS output, PG provenance checksum, GPU lease release까지 확인했다.
- 위 smoke는 기능 활성화 확인이다. 데이터 품질 승인에 필요한 workflow별 10장(총 20장) UAT와
  프롬프트/mask 품질 튜닝은 아직 별도 운영 검수 항목이다.

## 모델 준비

모델은 시작 시 다운로드하지 않는다. 승인된 파일을 다음 경로에 배치한다.

```text
docker/data/models/comfyui/
├── checkpoints/sd_xl_base_1.0_0.9vae.safetensors
├── diffusion_models/flux-2-klein-4b-fp8.safetensors
├── text_encoders/qwen_3_4b.safetensors
└── vae/flux2-vae.safetensors
```

정확한 source URL, size, SHA-256, license는
[`docker/comfyui/model_manifest.json`](../../docker/comfyui/model_manifest.json)이 정본이다.
총 모델 용량은 약 19.4 GB(18.1 GiB)다. 모델 사용 조건을 조직 차원에서 승인한 뒤 배치한다.

이미지를 먼저 빌드하고 모델 checksum을 검증한다.

```bash
COMPOSE_PROFILES=genai,comfyui ./scripts/compose-staging.sh build comfyui genai

docker run --rm \
  -v "$PWD/docker/data/models/comfyui:/models:ro" \
  --entrypoint python \
  datapipeline-comfyui:0.1 \
  /opt/service/validate_models.py
```

검증은 약 19.4 GB를 전부 읽기 때문에 처음 한 번은 시간이 걸린다. 파일이 없거나 size/hash가
다르면 컨테이너가 시작되지 않는다. 운영에서는 `COMFYUI_REQUIRE_MODELS=true`와
`COMFYUI_VERIFY_MODEL_HASHES=true`를 끄지 않는다.

## staging 활성화

`docker/.env.test`의 기존 값을 덮어쓰지 말고 다음 두 목록에 항목을 추가한다.

```env
COMPOSE_PROFILES=<기존 profiles>,genai,comfyui
GENAI_ENGINES_ENABLED=<기존 engines>,comfy_local

COMFYUI_REQUIRE_MODELS=true
COMFYUI_VERIFY_MODEL_HASHES=true
COMFYUI_MIN_FREE_VRAM_GB=13
COMFY_LOCAL_MAX_CONCURRENT=1
```

그 다음 `dev` branch의 test deploy workflow를 실행한다. workflow가 필요한 배포 환경 변수를
주입해 `scripts/deploy/deploy-stack.sh`를 호출한다. 해당 스크립트를 인자만 붙여 직접 실행하거나
`docker compose`를 직접 호출하지 않는다. 긴급한 staging 진단에서만 repository wrapper를 쓴다.

```bash
./scripts/compose-staging.sh up -d --build comfyui genai
```

Dagster code server 부팅의 `PostgresResource.ensure_schema()`가 `030_comfy_local.sql`을
forward-only로 적용한다. GenAI에 엔진을 노출하기 전에 아래가 모두 참인지 확인한다.

```bash
./scripts/compose-staging.sh exec postgres \
  psql -U airflow -d vlm_pipeline_staging -c \
  "SELECT name, applied_at FROM _pg_migrations WHERE name='030_comfy_local.sql';"

./scripts/compose-staging.sh ps comfyui genai embedding-service
./scripts/compose-staging.sh exec comfyui python /opt/service/healthcheck.py
./scripts/compose-staging.sh exec comfyui python -c \
  "import torch; print(torch.cuda.device_count(), torch.cuda.get_device_name(0))"
```

기대값은 migration 1행, `comfyui/genai/embedding-service` healthy, visible CUDA device 1개다.
ComfyUI의 `8188`은 host에 publish되지 않아야 한다.

## 수동 승인 테스트

1. GenAI Studio `http://10.0.0.10:8089/`의 `Comfy Nodes` 탭을 연다. 일반 form 방식은
   Submit 화면의 `Local ComfyUI (GPU0)`에서도 동일하게 사용할 수 있다.
2. 식별정보가 제거된 CCTV reference 1장으로 FLUX 편집을 실행한다.
3. 동일 source와 동일 크기의 0/255 binary PNG mask로 SDXL inpaint를 실행한다.
4. batch의 `provenance.json`에서 workflow/model manifest/input/mask/output SHA-256, seed,
   Comfy prompt ID, GPU 시각이 기록됐는지 확인한다.
5. Promote에서 `label_policy=required` 외의 값이 거부되는지 확인한다.
6. Label Studio finalized 전에는 dataset build 대상이 되지 않는지 확인한다.

실제 모델 UAT의 최소 승인 기준은 두 workflow 각각 10장, 총 20장이다. 다음을 함께 본다.

- 요청한 event가 실제로 보이는가
- 원 카메라 화각·배경·조명이 보존됐는가
- 인체, 연기/불, mask 경계 artifact가 허용 가능한가
- 생성 중 GPU1의 SAM3 작업과 메모리 사용량이 변하지 않는가
- 생성 종료 후 GPU0 embedding PE-Core가 정상 warmup되는가

현재 host의 driver는 `550.163.01`(reported CUDA 12.4)이고, container의 PyTorch
`2.8.0+cu128` CUDA kernel과 ComfyUI 0.36.0 기동은 확인됐다. ComfyUI는 cu130 최적화 경고를
출력하므로, 그 최적화는 사용하지 않는 상태로 위 20장 UAT의 peak VRAM과 latency를 반드시
기록한다. driver/container CUDA 승격은 이 rollout과 분리한다.

## 상태·장애 확인

```bash
./scripts/compose-staging.sh logs --tail 200 comfyui genai embedding-service

./scripts/compose-staging.sh exec postgres \
  psql -U airflow -d vlm_pipeline_staging -c \
  "SELECT resource, owner_job_id, state, heartbeat_at, expires_at, release_reason
     FROM generation_gpu_leases;"

./scripts/compose-staging.sh exec postgres \
  psql -U airflow -d vlm_pipeline_staging -c \
  "SELECT job_id, workflow_id, provider_prompt_id, gpu_started_at, gpu_completed_at,
          output_sha256
     FROM genai_job_provenance ORDER BY created_at DESC LIMIT 20;"
```

- `model validation failed`: manifest와 파일 경로/size/hash를 맞춘다. 검증을 우회하지 않는다.
- `GPU0 free VRAM below threshold`: job은 실패가 아니라 pending/deferred다. 기존 GPU0 작업이
  끝난 뒤 재시도된다.
- `cleared orphaned ComfyUI queue`: 이전 owner가 lease TTL을 넘긴 상태다. 다음 drain에서
  재시도되며, 반복되면 Comfy/GenAI 로그를 함께 확인한다.
- Comfy API가 끊기면 GPU lease와 embedding maintenance heartbeat를 연장하지 않는다.
  TTL 만료가 crash-recovery 경로다.
- 임의 workflow JSON, 다중 출력, PNG 이외 결과, path traversal metadata는 adapter가 거부한다.

## 중단과 롤백

데이터와 provenance를 삭제하지 않고 기능만 중단한다.

1. 환경의 `GENAI_ENGINES_ENABLED`에서 `comfy_local`을 제거한다.
2. `COMPOSE_PROFILES`에서 `comfyui`를 제거한다.
3. 정상 배포 경로로 GenAI를 재배포한다.
4. 필요하면 wrapper로 ComfyUI만 정지한다.

```bash
./scripts/compose-staging.sh stop comfyui
```

`030_comfy_local.sql`은 forward-only이므로 rollback 때 되돌리지 않는다. 기존 Kling/Veo 경로와
이미 생성된 NAS/PG provenance는 그대로 유지한다.
