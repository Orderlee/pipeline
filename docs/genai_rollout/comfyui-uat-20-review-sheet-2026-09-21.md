# ComfyUI 합성 CCTV — 20장 품질 UAT 운영자 검토 시트

**실행일:** 2026-09-21 (KST)  
**대상 작업:** [잔여 작업 실행계획](../exec-plans/active/comfyui-remaining-work-plan.md) **P2-4 (20장 품질 UAT)**  
**판정 기준 출처:** [ComfyUI 로컬 GenAI 파이프라인 계획](../exec-plans/active/comfyui-local-genai-pipeline-plan.md) Phase D.3  
**실행 환경:** prod `docker-comfyui-1`, 호스트 GPU0 (RTX A4000, 16,376 MiB), `--lowvram`  
**묶음 식별자:** `bulk_group_id = uat-p24-20260921` (genai_batches.options_json)

> ⚠️ **§5 표의 J1~J4 는 2026-09-21 에 Claude 가 20장을 직접 열어 채운 *잠정* 판정이다.**
> 확정 주체는 여전히 운영자다 — 동의하면 그대로 두고, 다르면 고치면 된다. 판정 기준은
> §5.0 에 있고 그 기준 자체도 제안이다. 각 행의 `사유` 는 관측한 근거이므로 판정을
> 뒤집을 때 무엇을 다르게 봤는지 적어 두면 다음 회차가 같은 논쟁을 반복하지 않는다.
> 생성 측이 기록한 것은 "무엇을 어떤 조건으로 만들었는가"와 "얼마나 걸렸고 VRAM 을 얼마나 썼는가"까지다.
> 합격 판정·재작업·파이프라인 편입(promote)은 이 시트를 읽은 사람이 정한다. **이번 회차 결과물은 promote 하지 않았다.**

---

## 1. 운영자가 판정할 4개 항목

| # | 판정 항목 | 질문 | 기입값 |
|---|---|---|---|
| **J1** | 이벤트 실재 | 요청한 이벤트(falldown / fire / smoke)가 실제로 보이는가 | O / X / 애매 |
| **J2** | 배경·카메라 기하 보존 | 카메라 위치·화각·원근·건축물·조명이 원본 그대로인가 | O / X / 애매 |
| **J3** | artifact 허용 | 마스크 경계·인체·불/연기 artifact 가 검수 가능한 수준인가 | 허용 / 거절 |
| **J4** | bbox·label 정합 | 이 결과물에 bbox 와 event label 을 정확히 붙일 수 있는가 | O / X |

거절 사유 코드는 설계서 Phase D 의 네 가지를 쓴다 — `reject` / `artifact` / `wrong-event` / `wrong-context`.
`generation_quality_reviews` 테이블에 쓰는 경로는 아직 배선돼 있지 않으므로 이번 회차는 이 문서에 직접 기입한다.

**참고용 side-by-side 이미지** (좌=원본, 우=생성물, SDXL 은 원본에 마스크 경계 표시):

```
/home/user/mou/nas_primary/genai_studio/_uat_p24_2026-09-21/<UAT_ID>_<class>_sbs.jpg
```

---

## 2. 원본 선정

### 2.1 선정 규칙

1. **서로 다른 실제 CCTV 카메라 20대.** 한 카메라에서 두 장 이상 고르지 않았다 (20 장면 = 20 카메라).
2. **평가 홀드아웃 배제.** `al_frames.eval_holdout` 은 `(cohort, group_key)` md5 해시 기반 생성 컬럼이다(`031_al_eval_holdout.sql`).
   - `sitej_subway_certbody` 소스는 `cohort=sitej_certbody` 에서 `bool_or(eval_holdout)=false` 인 카메라만 썼다. 이 코호트는 연출 동시녹화라 group_key 가 세션이고 카메라 단위 누수가 알려져 있어, 카메라 전체가 non-holdout 인 것만 채택했다.
   - MinIO 경유 소스는 `eval_holdout=true` 인 `asset_id` 를 제외한 후보 집합에서 뽑았다.
   - `cohort-a` 는 MinIO 잔존 프레임이 전량 `outdoor_fall` AL 풀(홀드아웃 포함)이라 **통째로 제외**했다.
3. **이벤트가 이미 들어있는 프레임 회피.** 주입할 이벤트가 원본에 이미 있으면 J1 판정이 불가능하므로 정상 상황 프레임만 골랐다.

### 2.2 ⚠️ MinIO 커버리지 제약 — 선정 경로가 계획과 달라진 이유

계획은 "`vlm-raw` 또는 `vlm-processed` 에서 고르라" 였다. 그러나 **2026-08-31 NAS_primary 재구축 이후 두 버킷에 남아 있는 최상위 prefix 는 각각 4개 / 5개뿐**이다.

```
vlm-processed/  →  sourcep/  sl_cam2_3/  songpa/  cohort-a/
vlm-raw/        →  genai_b4fe9a6f-339/  source-akane_video/  sourcep/  sl_cam2_3/  songpa/
```

`image_metadata` 에는 `raw_video_frame` 이 584,791 행 남아 있지만 객체 실물은 대부분 없다 — DB 에서 25개 source_unit 에 걸쳐 후보 50건을 뽑아 `head_object` 로 찍어보니 **48건이 NoSuchKey** 였다(DB 행은 무사, 객체만 공백).
결과적으로 MinIO 만으로는 서로 다른 카메라 20대를 채울 수 없어 2원 경로로 뽑았다.

| 취득 경로 | 장면 수 | 비고 |
|---|---|---|
| **MinIO `vlm-processed`** 프레임 객체를 그대로 사용 | 6 | 계획이 지정한 정규 경로. `image_metadata` 에 행이 있어 추적 가능 |
| **NAS archive 원본 영상에서 프레임 추출** (`ffmpeg -ss <t> -frames:v 1 -q:v 2`) | 14 | 같은 CCTV 의 원본 바이트. MinIO 공백을 우회하는 유일한 수단 |

> **운영 시사점:** 지금 합성 증강의 source 풀은 사실상 **MinIO 가 아니라 NAS archive** 다.
> 합성을 정례화하려면 (a) `scripts/reupload_minio_from_archive.py` 로 프레임 객체를 복구하거나
> (b) source 선정기를 archive 기준으로 배선하거나 중 하나를 결정해야 한다. **이 결정은 이 UAT 범위 밖이다.**

### 2.3 선정된 20 장면

| UAT ID | 워크플로 | 클래스 | source_unit | 카메라 | 장면 | 원본 해상도 | 원본 취득 경로 |
|---|---|---|---|---|---|---|---|
| `F01` | FLUX edit | falldown | SL_cam2_3 | cam2 | 물류 상하차장(주간, 4K) | 3840×2160 | MinIO `vlm-processed/sl_cam2_3/image/cam2_20260910_150219_00000001.jpg` |
| `F02` | FLUX edit | fire | SL_cam2_3 | cam3 | 물류 캐노피 통로(주간, 4K) | 3840×2160 | MinIO `vlm-processed/sl_cam2_3/image/cam3_20260910_155000_00000001.jpg` |
| `F03` | FLUX edit | smoke | sourcep | no_harness_cam | 옥외 회랑·정원(주간, 4K) | 3840×2160 | MinIO `vlm-processed/sourcep/no_harness/image/20150104_222725a_-_2024-11-05-0166_-_3of10_00000000_00090030_00000001.jpg` |
| `F04` | FLUX edit | fire | source-d | TC#2-2 | 건설현장 타워크레인 뷰(주간) | 2560×1440 | NAS `/home/user/mou/nas_secondary/GS/raw_videos/2026-02-06-135815_2026-02-06-145959_1000369$1$0$13.mp4` (ffmpeg@60s) |
| `F05` | FLUX edit | falldown | source-d | TC1-1_102dong | 건설현장 굴착부(주간) | 1920×1080 | NAS `/home/user/mou/nas_secondary/GS/raw_videos/2026-02-20-135910_2026-02-20-150330_1000390$1$0$0.mp4` (ffmpeg@60s) |
| `F06` | FLUX edit | falldown | sitej_subway_certbody | site-h_상행1 | 지하철 승강장(주간) | 1280×720 | NAS `/home/user/mou/nas_primary/archive/sitej_subway_certbody/jungangro_sanghaeng1_20260606_030000_t171.mp4` (ffmpeg@1s) |
| `F07` | FLUX edit | smoke | sitej_subway_certbody | site-h_화재감시B2 | 지하철 개집표구(주간) | 1280×720 | NAS `/home/user/mou/nas_primary/archive/sitej_subway_certbody/jungangro_hwajaegamsib2_20260607_100000_t171.mp4` (ffmpeg@1s) |
| `F08` | FLUX edit | fire | source-akane_video | kk_roadside_cam | 태국 도로변(야간) | 720×576 | NAS `/home/user/mou/nas_primary/archive/source-akane_video/20260815/000035be-872c-472c-8c83-6e6731dbbcd9.mp4` (ffmpeg@2s) |
| `F09` | FLUX edit | falldown | songpa | 팔각정어린이공원 | 어린이공원 놀이터(주간) | 1920×1080 | MinIO `vlm-processed/songpa/falldown/image/3611_3150671-2_garakbondong118palgakjeongeorinigongwon_merged_36123733_36347933_00000001.jpg` |
| `F10` | FLUX edit | smoke | songpa | 방이동_가로수화단 | 가로수 화단·상가(야간) | 1920×1080 | MinIO `vlm-processed/songpa/organized_videos/24hour_video/merged_videos/image/4525_1071325-2_bangidong_206-11_keopibin_ap_garosu_hwadan_merged_24734700_24740867_00000001.jpg` |
| `S11` | SDXL inpaint | falldown | sitej_subway_certbody | site-h_B1_종점_무대 | 지하 상가 복도(주간) | 1108×828 | NAS `/home/user/mou/nas_primary/archive/sitej_subway_certbody/jungangro_b1_jongjeom_mudae_20260531_040000_t857.mp4` (ffmpeg@1s) |
| `S12` | SDXL inpaint | fire | sitej_subway_certbody | site-h_B2_시점_대합실_EV3 | 엘리베이터 앞 대합실(주간) | 1108×828 | NAS `/home/user/mou/nas_primary/archive/sitej_subway_certbody/jungangro_b2_sijeom_daehapsil_ev3_20260531_030000_t171.mp4` (ffmpeg@1s) |
| `S13` | SDXL inpaint | falldown | sitej_subway_certbody | site-h_상선_시점_계단 | 환승 계단(주간) | 1108×828 | NAS `/home/user/mou/nas_primary/archive/sitej_subway_certbody/jungangro_sangseon_sijeom_gyedan_20260606_120000_t171.mp4` (ffmpeg@1s) |
| `S14` | SDXL inpaint | smoke | sitej_subway_certbody | site-h_1코너게이트IN | 대합실 개집표구(주간) | 1280×720 | NAS `/home/user/mou/nas_primary/archive/sitej_subway_certbody/jungangro_1koneogeiteuin_20260531_180000_t171.mp4` (ffmpeg@1s) |
| `S15` | SDXL inpaint | falldown | sitej_subway_certbody | site-h_B1_대합실_VL1 | 대합실 계단(주간) | 1108×828 | NAS `/home/user/mou/nas_primary/archive/sitej_subway_certbody/jungangro_b1_daehapsil_vl1_20260531_100000_t171.mp4` (ffmpeg@1s) |
| `S16` | SDXL inpaint | smoke | sitej_subway_certbody | site-h_2번_출구_외부 | 2번 출구 외부 계단(주간) | 1280×720 | NAS `/home/user/mou/nas_primary/archive/sitej_subway_certbody/jungangro_2beon_chulgu_oebu_20260531_180000_t343.mp4` (ffmpeg@1s) |
| `S17` | SDXL inpaint | fire | DTRO_실제이상상황 | loc-f_CH03 | 에스컬레이터 홀(야간) | 960×480 | NAS `/home/user/mou/nas_secondary/source-n/source-n_실제이상상황데이터/20260127_loc-f/CH03_20260127_21h33m19s.avi` (ffmpeg@2s) |
| `S18` | SDXL inpaint | falldown | songpa | 가락동529 | 야간 산책로 | 1920×1080 | NAS `/home/user/mou/nas_primary/archive/songpa/normal/3390_1161118-2 가락동 529_merged.mp4` (ffmpeg@5s) |
| `S19` | SDXL inpaint | fire | songpa | 방이동_비둘기어린이공원 | 공원 정자(야간) | 1920×1080 | NAS `/home/user/mou/nas_primary/archive/songpa/normal/3439_3070463-4 방이동 192-6 비둘기어린이공원_merged.mp4` (ffmpeg@5s) |
| `S20` | SDXL inpaint | smoke | sourcep | smoke_cam | 옥외 정원 통로(주간) | 1920×1080 | MinIO `vlm-processed/sourcep/smoke/image/20190102_012743a_00013833_00033333_00000001.jpg` |

---

## 3. SDXL inpaint 마스크 — 이번 UAT 한정 임시 규칙

> ⚠️ 설계서가 말하는 **"사전 승인된 safe region"은 아직 존재하지 않는다.** (승인 테이블도, 카메라별 등록 절차도 없다.)
> 아래는 **이번 20장 UAT 를 돌리기 위해 임시로 정한 규칙**이며, 운영 정책이 아니다. 정식 safe region 은 별도 결정 사항이다.

임시 규칙 3줄:

1. 마스크는 **원본과 동일 해상도의 단일 축정렬 사각형**, 픽셀값은 0 또는 255 뿐(안티에일리어싱 없음).
   `_validate_inpaint_pair()` 가 (a) 해상도 일치 (b) 비어있지 않음 (c) 0/255 이외 값 없음 을 제출 시 강제한다.
2. 위치는 **그 카메라에서 사람이 실제로 밟는 지면/바닥 평면** 위로 잡고, 클래스별 세로 밴드를 달리한다 — falldown 은 지면 접지 영역, fire 는 지면 접지 + 위로 약간, smoke 는 지면에서 위로 뻗는 세로로 긴 영역.
3. **기존 사람·차량·구조물을 덮지 않는다.** inpaint 는 마스크 안을 지우고 다시 그리므로 사람을 덮으면 그 사람이 삭제된다.

실제로 쓴 좌표(정규화 `[x0, y0, x1, y1]`, 원점 좌상단):

| UAT ID | 클래스 | 마스크 영역 (정규화) | 픽셀 크기 | 잡은 곳 |
|---|---|---|---|---|
| `S11` | falldown | `[0.30, 0.58, 0.72, 0.92]` | 466×282 | 복도 바닥 중앙 |
| `S12` | fire | `[0.34, 0.55, 0.66, 0.88]` | 354×274 | 엘리베이터 앞 바닥 중앙 |
| `S13` | falldown | `[0.32, 0.30, 0.70, 0.62]` | 421×265 | 계단 중단 |
| `S14` | smoke | `[0.28, 0.32, 0.68, 0.85]` | 512×382 | 대합실 바닥 중앙 상승 기둥 |
| `S15` | falldown | `[0.26, 0.30, 0.62, 0.70]` | 399×332 | 계단 중단 |
| `S16` | smoke | `[0.26, 0.55, 0.74, 0.95]` | 614×288 | 출입구 앞 보도 전경 |
| `S17` | fire | `[0.04, 0.55, 0.42, 0.95]` | 365×192 | 에스컬레이터 좌측 바닥 |
| `S18` | falldown | `[0.30, 0.62, 0.66, 0.95]` | 691×356 | 산책로 노면 중앙 |
| `S19` | fire | `[0.30, 0.48, 0.62, 0.85]` | 614×400 | 정자 앞 포장 바닥 |
| `S20` | smoke | `[0.04, 0.25, 0.38, 0.75]` | 653×540 | 좌측 화단·석재 구역 |

마스크 PNG 원본: `/home/user/mou/nas_primary/genai_studio/_uat_p24_2026-09-21/masks/` (이번 실행분 사본)

---

## 4. 프롬프트

### 4.1 두 워크플로는 프롬프트 형태가 다르다 (같게 쓰면 안 된다)

| | FLUX.2 Klein edit | SDXL inpaint |
|---|---|---|
| 텍스트 인코더 | Qwen3-4B (`Flux2TEModel_`) | CLIP-L + CLIP-G (SDXL) |
| 실효 토큰 한도 | 길다 — 문단 단위 지시문 수용 | **77 토큰**에서 잘림 |
| 프롬프트 성격 | **지시문** ("원본을 유지하고 …를 추가하라") | **묘사문** (마스크 안에 무엇이 있어야 하는지) |
| negative_prompt | **바인딩 없음** — 그래프가 `ConditioningZeroOut` 으로 음성 분기를 만든다 | 노드 5 `CLIPTextEncode` 에 바인딩 |
| 배경 보존 수단 | 프롬프트 문장에만 의존 (전체 이미지 재생성) | 구조적 — `VAEEncodeForInpaint` 의 noise_mask 가 마스크 밖을 보존 |

### 4.2 FLUX edit 프롬프트 템플릿

2026-09-18 에 실제 성공한 배치(`db986921-52a`)의 문장 구조를 그대로 따랐다 — 보존 대상 명사를 장면별로 갈아끼우고, 이벤트 지시문만 클래스별로 바꾼다.

```text
Edit the provided fixed CCTV frame.

Keep the entire original image unchanged. Preserve the exact camera position,
camera projection, field of view, perspective, framing, crop, geometry,
resolution, lighting, colors, background, loading-bay asphalt, painted pedestrian lane markings, parked truck, dock shutters, signage banners, and existing CCTV
compression noise.

Add exactly one adult person lying collapsed naturally on the open painted pedestrian lane on the left half of the
original image.

The person's full body is visible and is lying on their side in a realistic
fallen posture. The body is in direct contact with the ground with physically
correct shadows. Use natural human proportions, realistic clothing, and
realistic CCTV-scale detail. The person is not looking at the camera.

Add only this one person. Do not add standing people, duplicated people,
bystanders, blood, injuries, stretchers, or emergency responders.

Do not change the camera or surrounding scene. No fisheye effect, no lens
distortion, no vignette, no black borders, no zoom, no crop, no rotation,
no camera movement, no changed architecture, and no additional objects.
```

*(위는 `F01` 실제 제출문. 밑줄 친 부분에 해당하는 장면별 명사 목록과 위치 구절은 장면마다 다르다 — 전문은 §5 표의 `provenance.json` 링크에 그대로 남는다.)*

### 4.3 SDXL inpaint 프롬프트 (클래스별 고정)

**falldown**
```text
CCTV security camera still, one adult person lying collapsed on the ground, fallen on their side, full body flat on the floor surface, ordinary everyday clothing, contact shadow under the body, photorealistic, low-detail surveillance camera quality
```
**fire**
```text
CCTV security camera still, one small open fire burning on the ground, orange flames, thin smoke rising above the flame, warm light spill on the floor surface, dark scorch mark, photorealistic, low-detail surveillance camera quality
```
**smoke**
```text
CCTV security camera still, grey smoke plume rising from the ground, semi-transparent drifting haze, no flames, photorealistic, low-detail surveillance camera quality
```
**negative_prompt (전 클래스 공통)**
```text
illustration, painting, drawing, cartoon, anime, 3d render, text, caption, watermark, logo, distorted anatomy, extra limbs, extra heads, duplicated person, deformed hands, blurry, oversaturated, studio lighting, close-up portrait, floating object
```

---

## 5. 실행 결과 + 운영자 판정란

`생성시간` = ComfyUI 자신이 로그에 찍은 `Prompt executed in N seconds` — **순수 실행 시간**.  
`총 소요` = `genai_job_provenance.gpu_completed_at - gpu_started_at` — 제출부터 결과 회수까지. `genai_poll_sensor` 30초 tick 대기가 섞여 있어 **실행 성능 지표가 아니다.**  
`peak VRAM` = 그 구간 ±2초 동안 1초 주기 `nvidia-smi --query-gpu=index,memory.used` GPU0 최댓값(호스트 전체, 유휴 기저 ~700 MiB 포함).

| UAT ID | 워크플로 | 클래스 | seed | 출력 해상도 | 생성시간 | 총 소요 | GPU0 peak | 상태 | J1 이벤트 | J2 기하 | J3 artifact | J4 bbox | 사유 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `F01` | FLUX | falldown | `2026092101` | 1360x768 | 12.95 s | 15.8 s | 13.45 GB | done | O | O | 허용 | O | 배너 한글·P2 표지·바닥 도색 보존. 좌측 선반 적재물 소폭 변동 |
| `F02` | FLUX | fire | `2026092102` | 1360x768 | 12.37 s | 16.2 s | 15.01 GB | done | O | 애매 | 허용 | O | 불꽃+그을음 현실적. 차량 색조 변화, 좌측 게시판 내용 변조 |
| `F03` | FLUX | smoke | `2026092103` | 1360x768 | 12.43 s | 29.9 s | 13.46 GB | done | O | **X** | 허용 | O | **원본의 빨간 옷 인물 소실**, 실내 가구 변조 |
| `F04` | FLUX | fire | `2026092104` | 1360x768 | 12.16 s | 29.9 s | 13.46 GB | done | O | 애매 | 허용 | O | 건물·비계 보존. 라바콘 소실, 하단 적재물 재배치 |
| `F05` | FLUX | falldown | `2026092105` | 1360x768 | 12.25 s | 34.6 s | 13.46 GB | done | O | 애매 | 허용 | O | 타임스탬프·캡션 보존. **작업자 1명 소실**, 우측 패널 형상 변화 |
| `F06` | FLUX | falldown | `2026092106` | 1360x768 | 12.45 s | 29.9 s | 15.02 GB | done | O | O | 허용 | O | 최고 품질. 인명구조장비함·POCARI·스크린도어·점자블록 보존. 벤치 1개 추가 |
| `F07` | FLUX | smoke | `2026092107` | 1360x768 | 12.34 s | 29.9 s | 13.46 GB | done | O | **X** | 허용 | O | **배경 보행자 4명 전원 소실**, 사이니지 텍스트 변형 |
| `F08` | FLUX | fire | `2026092108` | 1136x912 | 12.28 s | 29.8 s | 14.99 GB | done | O | **X** | 허용 | O | 태국어 간판·전화번호·타임스탬프 보존. **오토바이 2인 + 주행 차량 소실** |
| `F09` | FLUX | falldown | `2026092109` | 1360x768 | 12.02 s | 29.9 s | 13.52 GB | done | O | 애매 | 허용 | O | 놀이터 구조 보존. 스프링 놀이기구 변형, 좌하단 인물 소실 |
| `F10` | FLUX | smoke | `2026092110` | 1360x768 | 12.16 s | 29.8 s | 13.46 GB | done | O | 애매 | 허용 | O | **양재대로71길 도로명판 문자 완전 보존**. 보행자 1명 소실 |
| `S11` | SDXL | falldown | `2026092111` | 1104x824 | 14.81 s | 30.3 s | 7.55 GB | done | O | X | **거절** | 애매 | 사람 누움 확인. 마스크가 평면 회색 슬래브 — 바닥 타일 소실, 사각 seam |
| `S12` | SDXL | fire | `2026092112` | 1104x824 | 14.75 s | 34.8 s | 7.55 GB | done | O | 애매 | **거절** | 애매 | 장작+숯 모닥불 — 지하철 대합실에 물리적으로 불가능. 점자블록 파괴 |
| `S13` | SDXL | falldown | `2026092113` | 1104x824 | 15.30 s | 23.0 s | 7.55 GB | done | **X** | X | **거절** | X | 계단이 **벽 + 벽걸이 카메라**로 대체. 넘어진 사람 없음 |
| `S14` | SDXL | smoke | `2026092114` | 1280x720 | 14.91 s | 30.0 s | 7.70 GB | done | O | **X** | **거절** | X | **마스크 안 개찰구 + 인물 약 6명 파괴**. 연기는 떠 있는 덩어리 |
| `S15` | SDXL | falldown | `2026092115` | 1104x824 | 14.91 s | 29.8 s | 7.55 GB | done | **X** | **X** | **거절** | X | 계단이 **사무실 카운터**로 대체. 사람 없음. 마스크 밖 'Disabled access' 문자 열화 |
| `S16` | SDXL | smoke | `2026092116` | 1280x720 | 15.03 s | 30.0 s | 7.55 GB | done | 애매 | X | **거절** | X | 연기 + **거대 돔형 감시카메라**(§7.0.1 프롬프트 귀책). 계단·점자블록 파괴 |
| `S17` | SDXL | fire | `2026092117` | 960x480 | 10.11 s | 29.8 s | 8.51 GB | done | 애매 | X | **거절** | X | 평면적 주황 덩어리. 캡션 '울산 ES #5' → '산 ES #5' (마스크가 문자 겹침) |
| `S18` | SDXL | falldown | `2026092118` | 1920x1080 | 28.72 s | 34.6 s | 7.92 GB | done | **X** | **X** | **거절** | X | 야간 노면이 **주간 밝기 베이지 사각형**으로 대체. 조명 불일치, 사람 식별 불가 |
| `S19` | SDXL | fire | `2026092119` | 1920x1080 | 28.58 s | 29.8 s | 12.64 GB | done | 애매 | X | **거절** | X | 불 + **정체불명 돔 물체**. 보도블록 파괴 |
| `S20` | SDXL | smoke | `2026092120` | 1920x1080 | 29.43 s | 34.4 s | 12.64 GB | done | **X** | **X** | **거절** | X | 정원이 **비포장도로 위 트럭**으로 대체. 연기 없음 |

### 5.0 판정 기준과 워크플로별 결론 (제안)

**장당 평균이 아니라 워크플로별로 판정한다.** 두 워크플로의 실패 양상이 서로 반대(§7.0)라
평균을 내면 둘 다 "중간"이 되어 아무 결정도 나오지 않는다.

| 기준 | 왜 이 선인가 |
|---|---|
| **J1 ≥ 8/10** | 이벤트가 없으면 붙일 라벨이 없다 — 데이터로서 가치가 0이다 |
| **J2: 하드 실패 0건** | 타임스탬프·간판 문자·인물 유무가 바뀐 건 1건도 허용 불가. 학습셋에 들어가면 모델이 "다른 장면"을 배운다 |
| **J3: 구조적 artifact 0건** | seam 이 10/10 이면 탐지기가 *"사각 경계 = 이벤트"* 지름길을 학습한다. 개별 허용/거절이 아니라 **비율**이 기준이다 |
| **J4 ≥ J1 통과분 전량** | bbox 를 못 붙이면 J1 이 O 여도 쓸 수 없다 |

#### 집계

| | FLUX.2 Klein edit | SDXL inpaint |
|---|---|---|
| **J1 이벤트** | **O 10 / 애매 0 / X 0** ✅ | O 3 / 애매 3 / **X 4** ❌ |
| **J2 기하** | O 2 / 애매 5 / **X 3** ❌ | 애매 1 / **X 9** ❌ |
| **J3 artifact** | **허용 10 / 거절 0** ✅ | **거절 10 / 허용 0** ❌ |
| **J4 bbox** | **O 10** ✅ | 애매 2 / **X 8** ❌ |

#### 결론

**SDXL inpaint — 불합격. 현 설정으로는 파이프라인에 넣지 말 것.** 네 기준을 전부 탈락했다.
`S13`(계단 → 벽+카메라) · `S15`(계단 → 사무실 카운터) · `S20`(정원 → 비포장도로 위 트럭)은
마스크 안의 장면을 **통째로 다른 장면으로 교체**했다. 이건 정도의 문제가 아니라 종류의 문제다.
근본 원인은 프롬프트가 아니라 체크포인트다 — SDXL **base** 는 inpaint 전용 가중치가 아니라
마스크 안을 "주변 문맥을 이어서 채우는" 능력이 없다. §7.0.1 의 프롬프트 결함(`S16`)을 고쳐도
나머지 8건은 그대로 남는다.

**FLUX.2 Klein edit — J2 하나만 걸린 조건부.** J1·J3·J4 를 전부 통과했고 배경 보존도
**문자 수준에서는 놀랍게 정확하다** — `F10` 의 도로명판 "양재대로71길 / Yangjae-daero 71-gil"
과 "1-24 / 1-1↔", `F08` 의 태국어 간판과 전화번호 081-6017648, `F05` 의 타임스탬프가 전부
그대로다. 실패는 **인물·소품이 조용히 사라지는** 쪽이다(F03 빨간 옷 인물, F07 보행자 4명,
F08 오토바이 2인+차량). 3/10 이 하드 실패라 현재 기준으로는 미달이다.

**J2 를 통과시키려면 기준을 완화하는 게 아니라 측정을 붙여야 한다.** "인물이 사라졌는가" 는
눈으로 세면 회차마다 흔들린다. 원본↔결과에 사람 탐지를 돌려 **인물 수 감소 = 즉시 탈락** 으로
자동화하는 것이 다음 회차의 최소 작업이다. 현재 파이프라인에 SAM3 가 이미 있으므로 새 모델이
필요하지 않다.

#### 이 판정이 뒤집은 관측 하나

§7.0 은 SDXL 의 장점을 "마스크 밖 구조적 보존"으로 적었다. **문자 가독성에서는 성립하지 않는다** —
`S15` 의 "Disabled access" 와 `S17` 의 캡션 "울산 ES #5"(→"산 ES #5")가 열화했다. `S17` 은
마스크가 글자에 걸쳐 있어 설명되지만 `S15` 는 마스크 밖이다. §7.2 의 8px 격자 내림 리샘플이
더 그럴듯한 원인이라 inpaint 유출로 단정하지는 않는다. 어느 쪽이든 **"마스크 밖은 안전하다"를
문자에까지 확장하지 말 것.**

---

### 5.1 생성 측 관측 메모 (판정 아님 — 확인용)

> 아래는 side-by-side 를 눈으로 훑어 **관측된 사실**만 적은 것이다. J1~J4 판정을 대신하지 않으며,
> 운영자가 원본 이미지를 직접 보고 확인해야 한다. 여기 적힌 내용이 틀렸다고 판단되면 그쪽이 맞다.

| UAT ID | 워크플로 | 클래스 | 관측된 것 |
|---|---|---|---|
| `F01` | FLUX | falldown | 요청 이벤트(쓰러진 사람) 렌더됨. 원본에 서 있던 작업자 1명과 좌상단 인물들이 사라짐. 우측 현수막 한글 문구가 다른 글자로 바뀜. |
| `F02` | FLUX | fire | 불꽃+그을음+지면 광원 반사 렌더됨. 차량 색이 은색→갈색 계열로 이동. 좌측 게시판 내용이 다른 문서로 바뀜. |
| `F03` | FLUX | smoke | 연기 기둥 렌더됨. 회랑 안쪽 인물 2명 소실, 우측에 원본에 없던 벤치 생성, 소화기 위치 변경. |
| `F04` | FLUX | fire | 지면 화염 렌더됨. **타임스탬프 오버레이가 `2026-02-06`→`2026-12-06` 으로 변조됨.** 자재 배치와 외벽 안전망 색이 재배열됨. |
| `F05` | FLUX | falldown | 슬래브 위 쓰러진 작업자 렌더됨(원거리·소형). 타임스탬프 보존. 원본 작업자 위치 이동. |
| `F06` | FLUX | falldown | 승강장 바닥에 쓰러진 사람 렌더됨. 장면 보존 양호. 좌측에 원본에 없던 벤치 생성. |
| `F07` | FLUX | smoke | 개집표구 앞 연기 기둥 렌더됨. 안쪽 대합실 인물 다수 소실. 게이트 색/배치 일부 변경. |
| `F08` | FLUX | fire | 노면 화염+그을음 렌더됨. 태국어 간판 문자 대체로 보존. 원본의 오토바이 주행 인물 소실. **원본 720×576 → 출력 1136×912 로 업스케일**돼 원본 카메라가 내지 않는 선명도가 됨. |
| `F09` | FLUX | falldown | 놀이터 바닥에 쓰러진 사람 렌더됨. 놀이기구 구조 대체로 보존, 원본의 소형 인물 1 소실. |
| `F10` | FLUX | smoke | 화단에서 피어오르는 연기 렌더됨. 야간 노출/색온도 유지. 도로·차량 보존 양호. |
| `S11` | SDXL | falldown | 쓰러진 사람 형태가 들어갔으나 **마스크 내부 바닥 텍스처가 평평한 회색으로 치환**되고 사각 경계(seam)가 뚜렷. |
| `S12` | SDXL | fire | 화염+숯 더미 렌더됨. 사각 경계 뚜렷, 불의 크기·원근이 장면 스케일과 불일치. |
| `S13` | SDXL | falldown | *(denoise=0.85, 출하 기본값 대조군)* **사람 없음.** 마스크가 벽면 패널+돔형 감시카메라로 채워짐. 계단이 사라짐. |
| `S14` | SDXL | smoke | 연기는 렌더됐으나 **마스크 안에 있던 개집표구가 삭제되고 벽으로 대체**됨. |
| `S15` | SDXL | falldown | **사람 없음.** 마스크가 녹색 패널/문짝으로 채워지고 계단이 사라짐. |
| `S16` | SDXL | smoke | 연기 렌더됨. 동시에 **원본에 없는 돔형 CCTV 카메라가 크게 생성**됨 — 프롬프트의 "CCTV security camera" 어구를 객체로 그린 것으로 보임. |
| `S17` | SDXL | fire | 화염 렌더됨. 마스크 상단 경계가 직선으로 보이고 하단 오버레이 문자 일부가 덮임. |
| `S18` | SDXL | falldown | *(denoise=0.85, 출하 기본값 대조군)* **사람 없음.** 마스크가 균일한 베이지 사각형으로 채워짐 — 지워진 회색이 충분히 재생성되지 않은 전형적 형태. |
| `S19` | SDXL | fire | 불타는 드럼통이 렌더됨. 야간이라 경계가 비교적 덜 보이나 **요청하지 않은 객체(드럼통)** 가 추가됨. |
| `S20` | SDXL | smoke | **연기 없음.** 마스크가 주변과 무관한 트럭 장면으로 채워지고 사각 경계가 뚜렷. |

### 5.2 장면별 원본 ↔ 결과 경로 (NAS)

| UAT ID | 원본 (제출된 바이트) | 결과 | provenance |
|---|---|---|---|
| `F01` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/cf55c5b8-30a/originals/001.jpg` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/cf55c5b8-30a/outputs/slA__cam2.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/cf55c5b8-30a/provenance.json` |
| `F02` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/3f310895-398/originals/001.jpg` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/3f310895-398/outputs/slB__cam3.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/3f310895-398/provenance.json` |
| `F03` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/3bd0d735-028/originals/001.jpg` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/3bd0d735-028/outputs/sourcepB__no_harness.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/3bd0d735-028/provenance.json` |
| `F04` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/8aa950d3-94d/originals/001.jpg` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/8aa950d3-94d/outputs/gs__1000369_13.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/8aa950d3-94d/provenance.json` |
| `F05` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/c3c928de-426/originals/001.jpg` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/c3c928de-426/outputs/gs__1000390_0.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/c3c928de-426/provenance.json` |
| `F06` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/410d32b2-720/originals/001.jpg` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/410d32b2-720/outputs/sitej__jungangro_sanghaeng1.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/410d32b2-720/provenance.json` |
| `F07` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/a605593f-6a6/originals/001.jpg` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/a605593f-6a6/outputs/sitej__jungangro_hwajaegamsib2.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/a605593f-6a6/provenance.json` |
| `F08` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/875e5ee1-ea0/originals/001.jpg` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/875e5ee1-ea0/outputs/sourcea__000035be.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/875e5ee1-ea0/provenance.json` |
| `F09` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/4a416ebe-1a5/originals/001.jpg` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/4a416ebe-1a5/outputs/songpaC__palgak_park.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/4a416ebe-1a5/provenance.json` |
| `F10` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/5d919a6b-322/originals/001.jpg` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/5d919a6b-322/outputs/songpaB__bangi_street.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/5d919a6b-322/provenance.json` |
| `S11` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/90e46f35-beb/originals/001.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/90e46f35-beb/outputs/S1_sitej_b1_jongjeom_mudae.src.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/90e46f35-beb/provenance.json` |
| `S12` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/dcc3b9c0-aa3/originals/001.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/dcc3b9c0-aa3/outputs/S2_sitej_b2_sijeom_ev3.src.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/dcc3b9c0-aa3/provenance.json` |
| `S13` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/a58f5356-a00/originals/001.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/a58f5356-a00/outputs/S3_sitej_sangseon_gyedan.src.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/a58f5356-a00/provenance.json` |
| `S14` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/ebd395e5-08c/originals/001.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/ebd395e5-08c/outputs/S4_sitej_1koneogeiteuin.src.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/ebd395e5-08c/provenance.json` |
| `S15` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/b2a916d4-b12/originals/001.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/b2a916d4-b12/outputs/S5_sitej_b1_daehapsil_vl1.src.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/b2a916d4-b12/provenance.json` |
| `S16` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/7452ab86-c48/originals/001.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/7452ab86-c48/outputs/S6_sitej_2beon_chulgu_oebu.src.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/7452ab86-c48/provenance.json` |
| `S17` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/fb06a300-cbf/originals/001.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/fb06a300-cbf/outputs/S7_dtro_yongsan_CH03.src.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/fb06a300-cbf/provenance.json` |
| `S18` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/06c5e302-057/originals/001.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/06c5e302-057/outputs/S8_songpa_garakdong529.src.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/06c5e302-057/provenance.json` |
| `S19` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/9a13f8f5-931/originals/001.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/9a13f8f5-931/outputs/S9_songpa_bangidong_park.src.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/9a13f8f5-931/provenance.json` |
| `S20` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/2cfbedb2-d7f/originals/001.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/2cfbedb2-d7f/outputs/S10_sourcep_smokecam.src.png` | `/home/user/mou/nas_primary/genai_studio/2026-09-21/2cfbedb2-d7f/provenance.json` |

제출 직전 원본은 `<batch>/originals/001.<ext>` 로 NAS 에 보존된다 — MinIO/archive 에서 가져온 바이트와 동일하다.
각 배치의 `provenance.json` 에 프롬프트 전문·seed·workflow SHA-256·입력/출력 SHA-256 이 남는다.

---

## 6. 워크플로별 실측 집계

| 지표 | FLUX.2 Klein edit | SDXL inpaint |
|---|---|---|
| 제출 | 10 | 10 |
| 성공 (`done`) | **10** | **10** |
| 실패 (`failed`) | 0 | 0 |
| 미종료 | 0 | 0 |
| **생성시간 min / median / max** (ComfyUI 실행) | **12.02 / 12.31 / 12.95 s** | **10.11 / 14.97 / 29.43 s** |
| 총 소요 min / median / max (센서 지연 포함) | 15.8 / 29.9 / 34.6 s | 23.0 / 30.0 / 34.8 s |
| **GPU0 peak VRAM (max)** | **15.02 GB** (15379 MiB) | **12.64 GB** (12943 MiB) |
| GPU0 peak VRAM (min) | 13.45 GB | 7.55 GB |
| 샘플 수 (런타임/VRAM) | 10 / 10 | 10 / 10 |

측정 방식: 20건을 한 번에 제출해도 `gpu0_comfy` lease 가 동시 1건이라 실행 구간이 겹치지 않으므로, 각 구간의 GPU0 최댓값을 그 job 의 peak 로 잡았다.
다만 ComfyUI 가 실행 사이에 가중치를 완전히 내리지 않으므로 **직전 워크플로의 캐시 잔량이 섞인다** — §7.5 의 한계를 반드시 같이 읽을 것.

**생성시간에서 드러난 것:**

- FLUX 는 원본 해상도와 무관하게 12.02~12.95 s 로 거의 일정하다 — 노드 5 가 항상 1 MP 로 맞추기 때문이다(§7.1).
- SDXL 은 원본 해상도에 그대로 비례한다 — 960×480 10.11 s → 1108×828 약 14.8 s → 1920×1080 약 29 s. **풀 HD 카메라에서는 FLUX 의 2.3배**가 든다.
- 표의 `총 소요`(약 30 s 로 몰림)는 30초 센서 tick 때문이며 성능 지표가 아니다. 처리량을 올리고 싶으면 모델이 아니라 `GENAI_POLL_INTERVAL_SECONDS` 를 봐야 한다.

`docker/comfyui/model_manifest.json` 의 `vram.peak_process_vram_gb` 와 워크플로별 `peak_process_vram_gb` 를 이 값으로 채웠다(출처를 같은 파일에 명시).

---

## 7. 판정 전에 알고 봐야 할 관측 사실

아래는 **측정된 사실**이고 합격/불합격 판정이 아니다. 판정할 때 artifact 의 원인을 모델 탓으로 오귀속하지 않도록 먼저 읽어야 한다.

### 7.0 두 워크플로의 결과 성격이 갈렸다 (관측 요약)

20건 모두 **기술적으로는 성공**했다(`job_status=done`, 실패 0, 재시도 0). 그러나 나온 그림의 성격은 다르다.

| | FLUX.2 Klein edit (10장) | SDXL inpaint (10장) |
|---|---|---|
| 요청 이벤트가 화면에 보이는가 (생성 측 관측) | 10 / 10 | 6 / 10 (falldown 4건 중 3건은 사람이 아예 안 나옴) |
| 마스크 사각 경계(seam) | 해당 없음 (마스크 없음) | **10 / 10 에서 식별 가능** (야간 `S19` 는 상대적으로 약함) |
| 요청하지 않은 객체 생성 | 소품 수준 (벤치 등) | 돔형 CCTV 카메라·벽면 패널·트럭·드럼통 등 **장면과 무관한 객체** |
| 마스크 밖/배경 변형 | **전역 드리프트** — 인물 소실, 간판 문자 변조, 타임스탬프 변조 | 구조적으로 보존 (마스크 밖 불변) |
| 출력 해상도 | 항상 약 1 MP (원본과 다름) | 원본과 같음(8px 격자 내림) |

두 가지 실패 양상이 **서로 반대**라는 점이 이번 회차의 핵심 관측이다 —
FLUX 는 "이벤트는 잘 만들지만 배경을 조용히 바꾸고", SDXL 은 "배경은 지키지만 마스크 안을 문맥 없이 채운다."
J1 과 J2 를 따로 채점해야 하는 이유가 여기 있다.

### 7.0.1 SDXL 프롬프트에 들어간 "CCTV security camera" 는 이 UAT 의 결함이다

SDXL 프롬프트를 `"CCTV security camera still, ..."` 로 시작하게 썼다. SDXL 의 CLIP 은 이 어구를
**스타일 지시가 아니라 그려야 할 객체**로 해석했고, `S13`·`S16` 에서 원본에 없는 **돔형 감시카메라가 실제로 그려졌다.**

- 이 artifact 의 원인은 워크플로나 체크포인트가 아니라 **프롬프트 문구**다. J3 을 채점할 때 이 두 건은 그렇게 귀속해야 한다.
- FLUX 쪽은 같은 문제가 없다 — Qwen3 인코더가 "Edit the provided fixed CCTV frame" 을 지시문으로 읽는다.
- 다음 회차에서는 SDXL 프롬프트에서 카메라/장치를 가리키는 명사를 빼고 `surveillance footage look` 같은 스타일 어휘만 남겨야 한다.
- 이번 회차는 **20장 상한** 때문에 재생성하지 않았다. 재생성 = 21장째다.

### 7.1 FLUX edit 은 출력이 1 MP 로 고정된다 — 원본 해상도가 보존되지 않는다

워크플로 노드 5 `ImageScaleToTotalPixels(megapixels=1.0)` 가 입력을 항상 약 1 MP 로 리스케일하고, 출력은 그 크기로 나온다.

| UAT ID | 원본 | 출력 | 화소 비율 (출력/원본) |
|---|---|---|---|
| `F01` | 3840×2160 | 1360×768 | 0.126× (축소) |
| `F02` | 3840×2160 | 1360×768 | 0.126× (축소) |
| `F03` | 3840×2160 | 1360×768 | 0.126× (축소) |
| `F04` | 2560×1440 | 1360×768 | 0.283× (축소) |
| `F05` | 1920×1080 | 1360×768 | 0.504× (축소) |
| `F06` | 1280×720 | 1360×768 | 1.133× (**업스케일**) |
| `F07` | 1280×720 | 1360×768 | 1.133× (**업스케일**) |
| `F08` | 720×576 | 1136×912 | 2.498× (**업스케일**) |
| `F09` | 1920×1080 | 1360×768 | 0.504× (축소) |
| `F10` | 1920×1080 | 1360×768 | 0.504× (축소) |

**양방향이다** — 4K 원본은 약 88% 의 화소가 버려지고, 720×576 원본(`F08`)은 반대로 **2.5배 업스케일**돼
그 카메라가 실제로는 내지 않는 선명도의 프레임이 나온다. 저해상도 현장 데이터를 늘리려고 합성을 쓰면
해상도 분포가 실사와 어긋난 표본이 학습셋에 들어간다.

**판정 시 함의:** 4K 원본(`F01`·`F02`·`F03`)은 화소의 약 88% 가 버려진 상태로 판정하게 된다.
J4(bbox·label 정합)를 볼 때, 이 결과물의 bbox 는 **원본 프레임 좌표계가 아니라 1 MP 출력 좌표계**에 붙는다.
합성본은 `raw_files` 에 독립 asset 으로 들어가므로 파이프라인 정합성 자체는 깨지지 않지만,
"원본 CCTV 와 동일 해상도의 증강본"을 기대했다면 **그 기대는 지금 워크플로로는 성립하지 않는다.**

### 7.2 SDXL inpaint 는 출력이 8 픽셀 격자로 내림된다

마스크는 **원본 해상도로 검증**되는데 출력은 8의 배수로 잘린 크기로 나온다. 둘이 어긋나는 카메라가 있다.

| UAT ID | 원본 | 출력 | 차이 |
|---|---|---|---|
| `S11` | 1108×828 | 1104×824 | −4×−4 px |
| `S12` | 1108×828 | 1104×824 | −4×−4 px |
| `S13` | 1108×828 | 1104×824 | −4×−4 px |
| `S15` | 1108×828 | 1104×824 | −4×−4 px |

VAE 가 8 의 배수만 다루기 때문에 **해상도가 8 의 배수가 아닌 카메라는 우하단 최대 7 px 이 잘린다.**
마스크는 원본 해상도로 검증되지만 출력은 잘린 크기라, 마스크 좌표를 그대로 bbox 로 재사용하면 최대 7 px 어긋난다(J4 확인 필요).

### 7.3 `denoise` 기본값 A/B — 두 장은 일부러 출하 기본값(0.85)으로 돌렸다

`VAEEncodeForInpaint` 는 마스크 안을 회색으로 지운 뒤 noise_mask 를 붙여 넘긴다.
이 조합에서는 관례적으로 `denoise=1.0` 을 쓴다 — 0.85 면 지워진 회색이 충분히 지워지지 않아 흐린 얼룩으로 남을 수 있다.
어댑터/매니페스트의 기본값은 **0.85** 이므로, 10장 중 2장(`S13`, `S18`)만 기본값 0.85 로, 나머지는 1.0 으로 돌려 **같은 시트 안에서 비교 가능**하게 했다.

| UAT ID | denoise | 비교 목적 |
|---|---|---|
| `S11` | 1.0 | 권장값 |
| `S12` | 1.0 | 권장값 |
| `S13` | 0.85 | 출하 기본값 — 대조군 |
| `S14` | 1.0 | 권장값 |
| `S15` | 1.0 | 권장값 |
| `S16` | 1.0 | 권장값 |
| `S17` | 1.0 | 권장값 |
| `S18` | 0.85 | 출하 기본값 — 대조군 |
| `S19` | 1.0 | 권장값 |
| `S20` | 1.0 | 권장값 |

§5.1 의 관측 메모에 두 대조군(`S13`·`S18`)이 어떻게 나왔는지 적어 뒀다. 다만 **denoise 만으로는 설명되지 않는다** —
`1.0` 으로 돌린 8장 중에도 요청 이벤트가 나오지 않은 것이 2장(`S15`·`S20`) 있다. SDXL **base** 는 inpaint 전용
체크포인트가 아니라서, denoise 를 고쳐도 마스크 안 문맥 추론이 해결되지는 않을 가능성이 있다.

> **운영자에게 묻는 것:** 0.85 결과 두 장이 1.0 결과들보다 눈에 띄게 나쁘면,
> `docker/genai/adapters/comfy_local.py` 의 inpaint 기본 denoise 를 1.0 으로 바꿀지 결정해야 한다.
> (자동 생성 측은 이 비교를 만들어 놓기만 했고 판정하지 않았다.)

### 7.4 FLUX edit 은 "배경 보존"이 구조적 보장이 아니라 프롬프트 의존이다

FLUX 워크플로는 전체 이미지를 다시 생성한다 — 마스크가 없다. 배경이 유지되는 이유는
`ReferenceLatent` + 프롬프트의 보존 지시문뿐이고, **원본 픽셀이 복사되는 경로는 없다.**
반면 SDXL inpaint 는 `VAEEncodeForInpaint` 의 noise_mask 때문에 마스크 밖이 구조적으로 보존된다(VAE 왕복 손실만 있음).
J2(기하 보존)를 볼 때 두 워크플로는 **성질이 다른 위험**을 가진다:

- FLUX: 원본에 있던 사람/사물이 **조용히 사라지거나 바뀔 수 있다** (전역 재생성).
- SDXL: 마스크 밖은 안전하지만 **마스크 경계선(seam)이 보일 수 있다**.

### 7.5 peak VRAM 측정의 한계

ComfyUI 는 `--lowvram` + RAM pressure cache 로 동작해 실행 사이에 가중치를 완전히 내리지 않는다.
따라서 **직전에 어떤 워크플로가 돌았는지에 따라 같은 워크플로도 peak 가 달라진다.**
이번 실측에서 SDXL 의 peak 는 7727–12943 MiB 로 흩어졌고, 상단은 직전 FLUX 실행의 캐시 잔량이 섞인 값으로 보인다.

- **고립 추정값**으로는 각 워크플로의 **min** 을 쓰는 편이 안전하다.
- **용량 계획**에는 **max** 를 써야 한다 — 실제 운영은 두 워크플로가 섞여 돌기 때문이다.
- FLUX max 15379 MiB 는 16,376 MiB 카드에서 **여유가 약 997 MiB 밖에 없다.** 같은 GPU0 을 쓰는 embedding-service 가 정비 모드로 비켜주지 않으면 OOM 여지가 있다.
- 어댑터의 사전 점검 임계값 `COMFYUI_MIN_FREE_VRAM_GB=13` 은 FLUX 실측 peak(15.02 GB)보다 **낮다.** 임계를 통과하고도 부족할 수 있다 — 상향 검토 대상(이 UAT 의 결정 사항 아님).

### 7.6 스케줄링은 설계대로 동작했다 (실패가 아님)

20건을 1분 안에 제출했고, `gpu0_comfy` lease 가 동시 1건이라 나머지는 전부
`[deferred] comfy_local 동시 작업 한도 대기 (max=1)` 로 `pending` 에 남았다.
Dagster `genai_poll_sensor`(30초 tick)가 `/internal/jobs/submit-pending` 으로 한 건씩 drain 했다.
**이 pending 은 장애가 아니다.** 2026-09-18 에 이것을 실패로 오인해 재제출한 결과 쓰레기 배치 6개·74 pending 이 쌓인 이력이 있다.

---

## 8. 재현 / 후속 명령

```bash
# 이 UAT 묶음의 상태
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
  SELECT b.batch_id, b.status, j.status, p.seed, p.gpu_started_at, p.gpu_completed_at
    FROM genai_batches b JOIN genai_jobs j USING(batch_id)
    LEFT JOIN genai_job_provenance p ON p.job_id=j.job_id
   WHERE b.options_json LIKE '%uat-p24-20260921%' ORDER BY b.submitted_at;"

# 결과물 디렉토리 (batch 별)
ls /home/user/mou/nas_primary/genai_studio/2026-09-21/*/outputs/

# side-by-side 검토 이미지 + 실행 아티팩트
ls /home/user/mou/nas_primary/genai_studio/_uat_p24_2026-09-21/
```

검토 디렉토리에 같이 둔 것:

| 파일 | 내용 |
|---|---|
| `<UAT_ID>_<class>_sbs.jpg` | 좌=원본(SDXL 은 마스크 경계 표시) / 우=생성물 |
| `masks/*.mask.png` | 제출한 binary 마스크 원본 |
| `uat_submission_manifest.json` | 20건의 제출 조건 전체 — 프롬프트 전문·seed·steps·cfg·denoise·원본 출처·마스크 좌표 |
| `uat_results.json` | 20건의 실행 결과 — job/batch id, provider prompt id, 입력/출력 SHA-256, 생성시간, peak VRAM |
| `sdxl_mask_rects.json` | 마스크 정규화 좌표 정의 |
| `gpu0_vram_samples_1hz.csv` | UAT 전 구간 1초 주기 GPU0/GPU1 VRAM 원시 샘플 (peak 값의 근거) |

### 8.1 이 결과물을 파이프라인에 넣기로 결정했다면

**아직 promote 하지 않았다.** 합격 판정 후 편입하려면 배치별로:

```bash
# label_policy=required, labeling_method=[captioning_image,bbox] 로 promote
curl -u "$U:$P" -X POST http://localhost:8089/genai/batches/<batch_id>/promote-to-labeling ...
```

거절하기로 했다면 아무 것도 하지 않아도 된다 — promote 하지 않은 배치는 `raw_files` 에 들어가지 않고
`genai_studio/2026-09-21/<batch_id>/` 안에만 남는다.

---

## 9. 이 UAT 가 답하지 않은 것

- **합격 기준 자체가 아직 없다.** 설계서 §9 는 "20장 QA 를 통과 기준으로 삼을지, 허용 artifact 기준은 무엇인지"를 **미결 결정 사항**으로 남겨뒀다. 이 시트는 그 결정을 내리는 데 필요한 재료일 뿐이다.
- **실검수 부하는 재지 않았다.** 20장을 LS 에서 실제로 검수하는 데 사람 시간이 얼마나 드는지는 promote 이후에만 측정된다.
- **탐지 성능 기여는 재지 않았다.** 설계서 Phase D.5 의 real-only holdout 대비 비교(합성 0% / 10% / 25%)는 별개 작업이며, 그 전에 합성본이 `finalized` 까지 가야 한다.
- **P2-5(SAM3 의 합성 검출률)와는 독립이다.** 이 시트의 J1 은 사람이 보는 이벤트 실재이지, SAM3 가 잡느냐가 아니다.

