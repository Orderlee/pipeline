#!/usr/bin/env bash
set -euo pipefail

python /opt/service/validate_models.py

exec python main.py \
  --listen 0.0.0.0 \
  --port 8188 \
  --disable-auto-launch \
  --preview-method none \
  --lowvram \
  --input-directory /data/input \
  --output-directory /data/output \
  --user-directory /data/user \
  --extra-model-paths-config /opt/service/extra_model_paths.yaml
