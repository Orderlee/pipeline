# DVC 데이터셋 버저닝 — 운영 런북

> 큐레이션 데이터셋을 DVC로 버전 관리하는 운영 절차. 2-tier:
> **데이터 엔지니어** = 프로젝트별 데이터셋 생성·버전 / **AI 엔지니어** = 여러 프로젝트 조합 → 학습셋.
> 설계: `docs/superpowers/specs/2026-06-29-mlops-finetune-scaffolding-design.md` §5.4/§7.6.

## 아키텍처 (3계층 — 서로 소유물 안 겹침)
| 계층 | 무엇 | 위치 |
|------|------|------|
| 바이트 | 실제 데이터(이미지/라벨) | MinIO `s3://vlm-dataset/_dvc/` (10.0.0.51:9000, content-addressed) |
| 포인터 | `.dvc` YAML + 커밋 | bare git repo `/srv/data-repos/dvc-datasets.git` (앱 배포와 격리) |
| 인덱스 | 쿼리 가능 카탈로그 + 커밋메시지 | PG `vlm_pipeline.dataset_catalog` (10.0.0.10) |

⚠️ **두 host**: git/pointer = 10.0.0.10(datapipeline), bytes = 10.0.0.51(MinIO). 엔지니어는 10.0.0.10에 SSH해서 작업 → dvc push/pull이 10.0.0.51 MinIO로. MinIO host엔 SSH·docker 권한 불필요(S3 키만).

## 현재 구축 상태 (2026-06-30)
- 그룹 `dvc-data`(user,eng-a,eng-b) · bare repo + post-receive 훅 · 공유 venv `/srv/data-repos/dvc-venv`(dvc[s3]+psycopg2+boto3) · `/srv/data-repos/dvc-ingest.env`(DSN→localhost:15433, MinIO 자격) · 시드 `.dvc/config`(MinIO remote) 완료.
- **남은 1줄(전 유저+훅 필수)**: `sudo git config --system --add safe.directory /srv/data-repos/dvc-datasets.git`
  (없으면 eng-a/eng-b push + 훅 git이 'dubious ownership'으로 막혀 인제스트 무동작. user는 --global로 이미 됨.)
- `setup_shared_dvc.sh`에 위 줄 포함 — 재실행해도 됨(멱등).

---

## Tier 1 — 데이터 엔지니어: 프로젝트 데이터셋 버전

### 최초 1회 (워크스테이션 = 10.0.0.10 SSH)
```bash
source /srv/data-repos/dvc-venv/bin/activate            # 공유 dvc (또는 자기 dvc[s3])
git clone /srv/data-repos/dvc-datasets.git && cd dvc-datasets   # 시드에 MinIO remote 이미 설정됨
dvc remote modify --local minio access_key_id  <MINIO_ACCESS_KEY>     # docker/.env 공유 키
dvc remote modify --local minio secret_access_key <MINIO_SECRET_KEY>  # .dvc/config.local (git 미추적)
```

### 데이터셋 올리기 (버전 1개 = 1 커밋)
```bash
mkdir -p data && cp -r <검수완료_프레임+COCO> data/<project>          # 권장: data/<project>/{images, coco.json}
dvc add data/<project>            # → data/<project>.dvc 포인터(md5)
dvc push                          # 바이트 → MinIO _dvc/
git add data/<project>.dvc data/.gitignore
git commit -m "curate: <project> v3 — <무엇을 왜>"   # 커밋 메시지 = 버전 의도 (카탈로그에 보존)
git push                          # → 훅 → dataset_catalog 에 버전 행 자동 INSERT
```
→ push 즉시 `dataset_catalog`에 행 생성: `git_rev`+`dvc_md5`+`commit_subject/message`+`status`.

### 받기 / 카탈로그 확인
```bash
git pull && dvc pull                                  # 포인터 + 바이트
psql "$PG_DSN" -c "SELECT git_short_rev, dvc_out_path, status, commit_subject, ingested_at \
  FROM dataset_catalog WHERE data_repo_id='dvc-datasets' ORDER BY ingested_at DESC LIMIT 10;"
```
- `status='available'` = MinIO 객체 확인됨(정상). `'pending_missing_dvc_objects'` = `dvc push` 누락 → 다시 push 후 재트리거(같은 커밋 재push 또는 reconciliation).

---

## Tier 2 — AI 엔지니어: 여러 프로젝트 조합 → 학습셋

### 1) 쓸 프로젝트 버전 고정 (pin)
```python
# pin_alias() API 만 사용 (raw UPDATE 금지). task당 alias 1개(current).
db.pin_alias(task="source-d", alias="current", dataset_catalog_id="<UUID>", pinned_by="you")
```
조회: `SELECT * FROM dataset_catalog_aliases;` / pin 이력: `dataset_catalog_pin_events`.

### 2) 조합 학습셋 빌드 (Dagster `build_trainset`)
Launchpad config:
```yaml
ops:
  build_trainset:
    config:
      task: sam3_detection
      sources: ["source-d:current", "incheon:current"]   # 조합할 프로젝트 pin
      class_allowlist: ["fire", "smoke"]                    # (선택) 쓸 클래스만
      class_remap: ["flame=fire"]                           # (선택) 동의어 통합
```
→ pinned DVC 데이터셋 pull → `merge_coco`(union+allowlist/remap) → 불변 `train_dataset_versions`
(`content_checksum` + `source_spec`=어느 프로젝트 어느 git_rev/md5). `sources` 비우면 기존 PG-folder 경로.

⚠️ **실행 런타임 의존**: `dvc get`이 **dagster 이미지에 dvc 필요**(현재 미포함). → dagster 이미지에 `dvc[s3]` 추가(재빌드) 또는 trainer(dvc 보유)에서 수행. 로직/배선/config 는 완료, 실행만 이 인프라 의존.

### 3) lineage 추적
`train_dataset_versions.source_spec` = 조합한 프로젝트 DVC 버전들 → 학습 시 `model_registry`+MLflow `dvc_*` 태그까지 이어짐. "이 모델 = 이 프로젝트들의 이 버전" 끝까지 추적.

---

## 트러블슈팅
- **훅이 catalog에 행을 안 만듦**: ① `sudo git config --system --add safe.directory ...` 누락(dubious ownership) ② `dvc-ingest.env` 부재/DSN 오류 ③ venv에 psycopg2/boto3 없음. 훅은 전 구간 **fail-soft**(push는 안 막힘)라 조용히 skip — `git push` 출력의 `[post-receive]`/`[ingest]` 줄 확인.
- **status가 계속 pending**: `dvc push`(바이트 업로드)를 git push 전에 안 했음 → 워크스테이션에서 `dvc push` 후 재트리거. (DO UPDATE라 재인제스트 시 available로 복구.)
- **dvc push 권한/쿼터**: `user` NAS 쿼터는 무관 — dvc가 MinIO로 직접 PUT(MinIO 키만 유효하면 됨).
- **다른 엔지니어가 clone/push 거부**: safe.directory system 등록 필요(위 1줄).

## 열린 운영 결정
- **백업**: `vlm-dataset/_dvc/` restic/mc mirror 주기 (큐레이션 SoT 바이트, prod MinIO 백업 갭과 함께).
- **저지연 통지**: 현재 push→훅 직접 인제스트(즉시). reconciliation 센서(git log 스캔 backstop)는 Dagster 배포 시 ON 가능(STOPPED 기본).

## 참고 명령
```bash
# 카탈로그/버전
psql "$PG_DSN" -c "SELECT task, dataset_name, git_short_rev, status, commit_subject FROM dataset_catalog ORDER BY ingested_at DESC LIMIT 20;"
# 수동 인제스트(훅 우회, 특정 rev)
. /srv/data-repos/dvc-ingest.env && /srv/data-repos/dvc-venv/bin/python \
  /home/user/work_p/Datapipeline-Data-data_pipeline/scripts/dvc/ingest_to_catalog.py \
  --repo /srv/data-repos/dvc-datasets.git --rev <rev> --only data/<project>.dvc
# pull (카탈로그 pin 해석 → dvc get)
python scripts/dataset_pull.py --task <project> --alias current --dest /data/pull/<project> --no-dry-run
```
