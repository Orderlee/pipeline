# CLAUDE.md — VLM Data Pipeline

> 코드로 알 수 있는 것은 쓰지 않는다. 모르면 사고 나는 규칙·환경 사실·포인터만. 세부는 `.claude/rules/*.md`(로컬, 경로 매칭 시 로드)·README·docs.

CCTV 영상·이미지 수집 → 중복제거 → Gemini(Vertex) 이벤트 라벨링 → SAM3 bbox → Label Studio 검수 → 학습셋.
**Dagster + PostgreSQL + MinIO.** DuckDB/MotherDuck 는 write path 아님(2026-05-19 PG cutover, 잔재는 `scripts/archive/`). YOLO 비활성, bbox 는 SAM3.

## 에이전트 규칙
- 코드 짜기 전 `.agent/skill/<name>/SKILL.md` 먼저 검색·준수.
- 페르소나 라우팅 훅(`.claude/hooks/persona_router.py`)은 `.claude/agents/*.md` frontmatter `triggers:` 한 줄만 읽는다(없으면 수동 위임만: codex·dagster-impl·pipeline-explorer). 라우팅표 `docs/references/agent-teams.md` §2, tier `multi-agent.md`.
- 이 호스트의 PATH `python` 은 깨진 venv(arm64) — `/usr/bin/python3` 또는 `/home/user/anaconda3/bin/python` 명시.
- `.env`·credential 의 비밀 값은 문서·로그에 옮기지 않는다(키 이름은 OK).

## 1. 이 checkout = prod 배포 루트
- CI 배포가 **이 디렉토리**에 `rsync -a --delete`(src·configs·gcp·scripts·split_dataset·docker/app·compose·Dockerfile) + `git reset --hard <SHA>` 를 건다(staging clone 도 dev push 마다 동일). tracked 수정은 reset 으로, **untracked 새 파일은 rsync 로** 다음 배포에 소실되고 **체크아웃 브랜치도 덮인다**(push 전 branch·`git status` 확인; worktree 도 격리 아님). 반영은 commit→push 뿐.
- 실행 코드 = 이미지 안 `COPY src/` — 호스트 src 를 고쳐도 컨테이너는 안 바뀐다. 예외: `docker/analysis/`(`/workspace` bind → 미커밋 워킹트리 편집도 즉시 라이브), `pipeline-ls-webhook-1`(호스트 `src/` ro bind — 재시작 시 반영).
- **`main` push = dagster 3종 stop→rm→recreate = 진행 중 라벨링 run 중단.** 재빌드 여부와 무관(deploy-stack.sh 의 이 구간은 `BUILD_REQUIRED` 가드 밖). paths-ignore 예외(`*.md` 는 루트만 — `.claude/**` 는 배포 트리거): `docs/**`, `*.md`, `tests/**`, `.cursor/**`, `.agent/**`, `.github/copilot-instructions.md`, `.github/workflows/claude*.yml`, `docker/analysis/**`. 학습·정비 윈도우 중 배포 보류.
- 러너는 `Orderlee/…` fork 에만 — upstream(upstream-org) 머지는 배포 안 됨. `main` force-push 금지.
- 브랜치: `feature/*` → `dev`(스테이징 자동 배포) → `main`(prod). 핫픽스 `fix/*` → `main` → `dev` 백머지. **push·핫픽스 전 `git rev-list --left-right --count origin/main...main`** — 로컬 main 에 미push 커밋이 쌓여 로컬 빌드로 운영될 수 있다(2026-10-07: 42) — origin 기준 fix 는 이를 걷어내고 로컬 main push 는 전부 배포.
- compose 는 wrapper 만: `./scripts/compose-prod.sh` / `./scripts/compose-staging.sh`(스테이징 clone 에서). 직접 `docker compose` = project/env-file 누락 → prod 컨테이너 조작 또는 DSN 없는 crashloop(실발생).
- `.env`/`.env.test`/`pyproject.toml` 은 git 미추적. `.env` 변경 = 호스트 편집 + 해당 서비스 recreate.
- 상세: README §배포(CI/CD), `scripts/deploy/deploy-stack.sh`, 롤백 `scripts/deploy/rollback.sh`.

## 2. 환경 식별자
| | Production (`main`) | Staging (`dev`) |
|---|---|---|
| Dagster UI | `http://10.0.0.10:3030` | `:3031` — 상시 기동 아님, 무응답 ≠ 장애 |
| repo / compose project | `…/Datapipeline-Data-data_pipeline` / `docker` (`docker-*`) | `…_test` / `pipeline-test` (`pipeline-test-*`) |
| PostgreSQL | `vlm_pipeline` @ `docker-postgres-1`, 호스트 `:15433` | `vlm_pipeline_staging` @ `pipeline-test-postgres-1`, `:15432` |
| MinIO (NAS 박스; 로컬 `minio` 컨테이너 아님) | `http://10.0.0.51:9000` (콘솔 9001) | `:9002` (9003) |
| NAS 루트 → 컨테이너 `/nas/data` | `/home/user/mou/nas_primary` | `…/nas_primary/staging` |
| env | `docker/.env` | `docker/.env.test` |

- DB 읽기: `docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "SELECT …"`.
- **`docker-postgres-1` recreate 금지**: pgvector 가 이미지가 아니라 컨테이너 레이어에 dpkg 설치돼 recreate·`POSTGRES_IMAGE` 교체 시 소실(배포 `up -d postgres` 도 정의가 바뀌면 recreate).
- `docker-sam3-1`(:8002) 은 **prod·staging 공유**(staging `SAM3_API_URL` 이 이 컨테이너) — 정비·재시작은 양쪽에 영향.
- Label Studio 는 별도 compose project `pipeline`(`pipeline-labelstudio-1`, UI :8084). **LS 앱 DB = `pipeline-postgres-1` 의 `airflow`**(파이프라인 DB 아님). `postgres` alias 컨테이너가 둘이라 `POSTGRE_HOST` 는 컨테이너명 명시.
- 호스트≠컨테이너 포트: embedding `8004→8003`, genai `8089→8088`, mlflow `5500→5000`, postgres `15433→5432`, FiftyOne 프록시 `5153→5151`. 전체 포트·GPU 표: README §Infrastructure.
- NAS_primary = CIFS `//10.0.0.51/data`. incoming/archive/manifest 는 `/nas/data` **단일 바인드 안의 서브경로**(쪼개면 archive 이동이 `EXDEV` 전체 복사). `user` 유저는 NAS quota → 호스트 직접 `cp` 는 "할당량 초과", 컨테이너(root) 경유.

## 3. 코딩 규칙
- Python 3.10+, ruff **0.7.4**(CI 핀, line 120). conventional commits — "무엇·왜".
- Import 방향: `definitions*.py` → `defs/`(assets·sensors) → `resources/`·`lib/`(L1-2, 순수 Python). `lib/` 에서 `dagster`/`defs`/`resources`/`ops` import 금지(lazy 도) — `scripts/check_lib_layer_imports.py` 가 CI 첫 step·pre-commit 에서 차단. `lib/spec_config.py` 는 태그 파싱만(DB 의존은 `defs/spec/config_resolver.py`), 키 빌더는 `lib/key_builders.py`.
- DB write 는 `PostgresResource`(`db`) 경유, 센서는 `lib/sensor_db.py` read-only. `@asset` 우선. 파일 오류는 per-file fail-forward.
- **테스트 allowlist 함정**: `.gitignore` 가 `tests/unit/*` 를 blanket 무시하고 `!tests/unit/<파일>` 로만 편입한다. 새 테스트는 allowlist 에 넣지 않으면 **CI 가 영원히 안 돌린다** — 로컬 초록 ≠ CI. 확인은 `git ls-files tests/`. 낡은 테스트는 수치 갱신이 아니라 불변식으로 고친다.
- CI 테스트 `tests/unit/test_claudemd_*.py` 가 이 문서의 MLOps 절 제목·심볼과 스크립트 표를, `test_mlops_skill_runbook.py` 가 mlops SKILL.md 헤딩을 단언한다 — 해당 토큰 삭제 금지.

## 4. 데이터 불변식
- MinIO 버킷 5개 고정 `vlm-raw`·`vlm-labels`·`vlm-processed`·`vlm-dataset`·`vlm-classification`. `raw_key = <source_unit_name>/<rel_path>`(sanitize 로마자가 정본, `YYYY/MM` prefix 금지). 라벨 JSON 정본은 `vlm-labels` 만. 키 패턴: README §MinIO.
- **`labels` 는 per-event 행**(이벤트 1개 = 1행; 0 events = 0 rows) — 행 수 0 ≠ 실패. 라벨링 완료 지표 = `video_metadata.timestamp_status='completed'` + `timestamp_label_key` + `vlm-labels/<source>/events/*.json`(빈 배열도 업로드). bbox 완료 = `bbox_status='completed'` + `image_labels`(0 검출 정상). Gemini 실호출 여부는 `clip_timestamp` step 소요(20 videos ≈ 90~120s, 0s = skip).
- 파일 오류 `file_missing`/`empty_file`/`ffprobe_failed` → DB 미삽입 + archive 미이동(`<manifest_dir>/failed/*.jsonl` 만). transient 는 retry manifest. **archive 이동이 끝난 파일만 `ingest_status='completed'`** — 이 상태가 dedup·build·labeling 쿼리를 전부 게이트한다. 중복 = `checksum` UNIQUE + 이미지 pHash Hamming ≤5(`dup_group_id`, run 계속); pHash **계산 실패**는 `gated_failed` 로 run 실패.
- **자기학습 금지**: 모델 파생 라벨(`review_status='auto_generated'`, Gemini 캡션, `vlm-classification`)로 학습·eval 금지. GT = LS `finalized`(`image_label_annotations`) 또는 사람 어노테이션. `DATASET_REQUIRE_LS_FINALIZED=1` 유지. AL 은 `al_frames.label_source` 가 게이트, `eval_holdout` 은 봉인용 — per-class eval 분모로 쓰지 말 것.
- SAM3 결과는 `image_labels`(`label_tool='sam3'`) + `vlm-labels/<source>/sam3_segmentations/`. 검수 전 스냅샷 `*.pseudo.json`(write-once) 이 pseudo-label QA 정본 — 라이브 JSON 은 LS 검수가 덮어쓴다.
- 라벨 온톨로지 정본 `src/vlm_pipeline/data/label_ontology.json`(13) — 매핑 수정은 JSON 만(parity test 가 파생본 강제). DB `label_classes` 는 15(026 승격분; parity 는 022 만 읽어 못 잡음).
- PG migration(`src/vlm_pipeline/sql/migrations/postgres/`): forward-only, **파일명 = 적용 키**(개명 금지, `030_*` 두 파일 유지). 러너는 한 파일 실패 시 **뒤 번호 전부 정지**하고 적용된 파일의 `@ASSERT_AFTER` 도 매 실행 재검증 → 인덱스 드롭 전 `grep -r ASSERT_AFTER`. 적용 시점은 배포가 아니라 첫 asset/센서 실행. 미커밋 마이그레이션을 prod 에 먼저 적용하지 말 것(rsync 로 파일 소실 → fresh DB 재현 불가).

## 5. GPU 공유 계약 (16GB ×2)
- NVENC 재인코딩은 GPU0/1 round-robin(CUDA 와 별 유닛). GPU0: dagster torch + embedding PE-Core(`cuda:0`) + ComfyUI + `angle-dav2-1`. GPU1: **SAM3**(`SAM3_WORKERS=3` ≈ 11GB; 4 에서 OOM 이력 — 올리기 전 확인) + embedding PLM 슬롯(`PLM_DEVICE=cuda:1`, free < `PLM_MIN_FREE_GB`=9 면 503 — SAM3 상주 시 사실상 못 뜸) + trainer.
- 학습 전 서빙 drain: `POST /maintenance/enter` 를 SAM3 `:8002` **와** embedding `:8004` 둘 다(빼먹으면 `/caption` 이 GPU1 로 들어옴) → 학습 → `/maintenance/exit` + `/warmup`. 복구 `scripts/clear_maintenance.sh [sam3|pe_core|all]`. SAM3 정비 플래그는 컨테이너 파일 — `restart` 로는 안 풀리고(TTL 까지 503) recreate·`/maintenance/exit` 로 풀린다.
- ComfyUI 입장 임계 `COMFYUI_MIN_FREE_VRAM_GB=14.5` 는 PE-Core unload **후** 측정(막는 대상은 `angle-dav2`). 런북 `docs/runbook/comfyui-local-genai.md`.

## MLOps — 파인튜닝 트랙
- 불변식: 서빙 가중치 = `model_registry` 의 `status='promoted'` 행(심볼릭링크 아님 — rsync 가 지움). 학습셋 = `train_dataset_versions` + `vlm-dataset/_trainsets/<id>/` 동결 스냅샷. CI 는 학습 안 함 — 실제 학습은 `ENABLE_TRAINING=1`(미설정 = dry-run), `gpu_trainer` 태그 동시 1.
- 학습은 Dagster run 과 분리된 `COMPOSE_PROFILES=trainer ./scripts/compose-prod.sh run --rm trainer`(배포는 trainer 를 절대 기동/recreate 안 함). eval 게이트 통과 → `status='promotable'` → `scripts/promote_model.py --model sam3 --model-version-id <id> --apply`(기본 dry-run; `--rollback` 은 직전 archived 자동 선택).
- ⚠️ turnkey 아님: eval 채점부 `_score_candidate/_score_incumbent` 는 `NotImplementedError`. SAM3 승격은 compose 리터럴 경로의 바이트 덮어쓰기로 동작(env 아님). PE-Core 는 `scripts/promote_pe_core.py`(재임베딩 → `embedding_active_model` 포인터 전환). MLflow(`:5500`)는 `COMPOSE_PROFILES`·배포 밖 — 재부팅엔 restart 정책으로 복귀하나 삭제되면 수동 기동(trainer 는 fail-soft).
- 런북 `.agent/skill/mlops-finetune/SKILL.md`, 설계 `docs/superpowers/specs/2026-06-29-mlops-finetune-scaffolding-design.md`.

## 6. 기본값 함정 (조용한 중단)
- `dispatch_sensor`·`production_agent_dispatch_sensor` 는 **기본 STOPPED** — `dagster_home/storage` 초기화 후 UI 에서 다시 켜지 않으면 자동 라벨링이 조용히 멈춘다.
- `pg_writer` 태그 limit 은 붙은 asset 이 없어 no-op(`gpu_trainer` 만 실사용).
- 센서는 NAS `OSError/PermissionError/TimeoutError` 를 graceful skip 한다 — 침묵 ≠ 정상. 스테이징 초기화는 `.agent/skill/staging_reset/SKILL.md`; staging `incoming/archive` 원본은 명시 요청 없이 삭제 금지.

## 자주 쓰는 스크립트
| 스크립트 | 용도 |
|---|---|
| `scripts/promote_model.py` / `scripts/promote_pe_core.py` | 모델 승격·롤백(기본 dry-run, `--apply`) |
| `scripts/clear_maintenance.sh` | GPU 정비락 강제 해제 + `/maintenance/exit` + `/warmup` |
| `scripts/dataset_pull.py` | DVC pin 해석 → `dvc get`(기본 dry-run) |
| `scripts/repair_unsanitized_raw_keys.py` | 비정규 MinIO 키 → 정본 `raw_key` 서버사이드 복사(기본 dry-run) |
| `scripts/{backfill_video_metadata,cleanup_duplicate_assets,recompute_archive_checksums,reupload_minio_from_archive}.py` | 백필·중복 정리·재해시·재업로드 |
| `scripts/archive/*` | 폐기(DuckDB 시절) — 운영 명령으로 안내 금지 |

## 어디를 읽나
- `README.md`(§환경·§Infrastructure 포트/GPU 표·§MinIO·§Database Schema·§배포), 목차 `docs/index.md`, 장애 대응 `docs/runbook.md` + `docs/runbook/`.
- 에이전트 진입점 `AGENTS.md`, 리뷰 기준 `REVIEW.md`.
- rules(`.claude/rules/`, 로컬·경로 매칭 로드): deployment · postgres · ingest · label-studio · media-services · analysis-fiftyone · mlops.
