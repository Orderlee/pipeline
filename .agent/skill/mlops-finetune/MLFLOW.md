# MLflow 학습 추적

`model_registry`가 승격 SoT이고 MLflow는 fail-soft 추적이다. MLflow 장애가 학습·승격을 막아서는 안 된다.

| 용도 | 주소 |
|---|---|
| 호스트 UI | `http://localhost:${MLFLOW_PORT:-5500}` |
| trainer 내부 | `http://mlflow:5000` |

- Compose service `mlflow`는 PostgreSQL의 별도 `mlflow` DB와 `vlm-dataset/_mlflow/` artifact prefix를 사용한다.
- trainer는 hyperparameter, train dataset lineage/checksum, metric, `_models/<version>/`의 요약 artifact를 기록한다. 이미지 데이터 자체는 올리지 않는다.
- 필요 시 prod wrapper로 해당 서비스만 `scripts/compose-prod.sh --profile mlflow up -d --build mlflow` 기동한다. 전체 `up -d`는 Dagster 재생성을 유발할 수 있으므로 쓰지 않는다.

## 점검

- UI에 run이 없으면 `MLFLOW_TRACKING_URI`와 mlflow container/network를 확인한다. 실패 로그는 fail-soft이며 registry 기록을 우선 확인한다.
- artifact 실패는 MinIO endpoint·권한·`vlm-dataset` prefix를 확인한다.
- DB 생성·Compose 기동은 운영 상태를 바꾸므로 정비 창과 사용자 승인 범위에서만 한다.
