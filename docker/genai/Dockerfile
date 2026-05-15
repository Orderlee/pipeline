FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

# HTMX 가 static/ 에 없으면 CDN 폴백 — 실제 prod 에선 사전 다운로드 권장
RUN test -f /app/static/htmx.min.js || \
    (apt-get update && apt-get install -y wget && \
     wget -q -O /app/static/htmx.min.js https://unpkg.com/htmx.org@1.9.12/dist/htmx.min.js && \
     apt-get purge -y wget && rm -rf /var/lib/apt/lists/*)

EXPOSE 8088

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8088"]
