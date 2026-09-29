FROM pytorch/pytorch:2.8.0-cuda12.8-cudnn9-runtime

ARG COMFYUI_COMMIT=ee71d5c4993f29086b27fde1629a945ae48425bf

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && git clone --filter=blob:none https://github.com/Comfy-Org/ComfyUI.git /opt/ComfyUI \
    && git -C /opt/ComfyUI checkout --detach "${COMFYUI_COMMIT}" \
    && rm -rf /opt/ComfyUI/.git \
    && pip install -r /opt/ComfyUI/requirements.txt \
    && apt-get purge -y --auto-remove git \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh healthcheck.py validate_models.py extra_model_paths.yaml model_manifest.json /opt/service/
COPY workflows/ /opt/service/workflows/

RUN chmod 0555 /opt/service/entrypoint.sh \
    && mkdir -p /data/input /data/output /data/user

WORKDIR /opt/ComfyUI
EXPOSE 8188

ENTRYPOINT ["/opt/service/entrypoint.sh"]
