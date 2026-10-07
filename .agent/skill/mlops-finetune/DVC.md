# DVC 큐레이션 데이터셋 버전

큐레이션 데이터의 bytes는 MinIO `vlm-dataset/_dvc/`, pointer는 bare Git repo, 검색 인덱스는 PostgreSQL `dataset_catalog`에 둔다. 학습용 동결 스냅샷은 `train_dataset_versions`다.

## 데이터 엔지니어

1. DVC repo에서 `data/<project>`에 검수 완료 데이터와 라벨을 둔다.
2. `dvc add` → `dvc push`로 bytes를 먼저 올린다.
3. `.dvc` pointer를 commit/push한다. post-receive가 catalog ingest를 수행하므로, push 뒤 `dataset_catalog.status`가 `available`인지 확인한다.

`pending_missing_dvc_objects`면 Git push 전에 `dvc push`가 빠진 것이다. bytes를 올리고 ingest를 재시도한다. hook은 fail-soft이므로 push 출력과 hook 환경을 확인한다.

## AI 엔지니어

- alias는 raw SQL이 아니라 `pin_alias()`로 고정한다.
- `build_trainset`의 `sources`로 프로젝트 alias들을 조합한다. 결과 checksum과 source lineage는 불변 `train_dataset_versions`에 저장된다.
- DVC source를 Dagster에서 읽으려면 실행 이미지에 DVC/S3 의존성이 필요하다. 없으면 실행을 진행하지 말고 이미지를 정비한다.

인증값·DSN은 문서나 명령 예시에 넣지 않는다. 수동 ingest/pull은 운영 데이터와 외부 저장소를 변경하므로 대상 revision과 destination을 확정한 뒤 수행한다.
