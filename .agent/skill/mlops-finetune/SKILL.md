# MLOps fine-tune — 운영 런북

SAM3/PE-Core 학습은 Dagster run과 분리된 `trainer` 프로세스다. 공유 GPU의 SAM3 정비는 prod·staging 서빙에 모두 영향하므로 승인된 정비 창에서만 한다.

## 9. 정비락 복구

1. 먼저 Dagster guard sensor의 tick과 소유 run 상태를 확인한다. heartbeat가 죽었거나 run이 비실행이면 guard가 자동 해제한다.
2. 자동 복구가 실패할 때만 다음을 실행한다. 인자는 옵션이 아니라 positional `sam3 | pe_core | all`이고 기본은 `all`이다.

```bash
bash scripts/clear_maintenance.sh sam3
```

3. 스크립트는 PG lock clear를 best-effort로 수행하고 `/maintenance/exit`(force)·`/warmup`·status를 호출한다. 호스트 기본 URL은 SAM3 `http://localhost:8002`, embedding `http://localhost:8004`이다. non-zero면 실제 해제를 가정하지 말고 URL·서빙 상태를 확인한다.

## run 이 progressing 인지 hung 인지 판별

- trainer stdout와 `vlm-dataset/_models/<version>/train_log.jsonl`의 step/timestamp가 계속 증가하는지 본다. `loss=NaN`은 발산이다.
- GPU utilization/메모리와 Dagster heartbeat도 함께 본다. 모두 정지했을 때만 hung으로 판정한다.
- trainer를 중지한 뒤 정비락 복구를 수행한다. 부분 `_models/<version>/`은 registry 후보로 등록하지 않는다.

## staging vs prod 검증 분리

- staging·CI는 `ENABLE_TRAINING=false` 로 두고 migration, dataset checksum/split, eval gate, promote dry-run, defs load만 검증한다.
- 실제 GPU 학습·unload/warmup은 통제된 prod 창에서만 한다. prod 배포는 Dagster 서비스를 재생성하므로 학습 중 보류한다.
- 승격 SoT는 `model_registry`; MLflow는 fail-soft 보조 추적이다. 상세: [MLFLOW.md](MLFLOW.md). 큐레이션 버전: [DVC.md](DVC.md).
