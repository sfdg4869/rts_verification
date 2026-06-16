# QA Portal / RTS Check API
FROM python:3.12-slim-bookworm

WORKDIR /app

ARG ORACLE_CLIENT_ARCHIVE=instantclient.placeholder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    unzip \
    libaio1 \
    libnsl2 \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ${ORACLE_CLIENT_ARCHIVE} /tmp/instantclient.zip
RUN set -eux; \
    if unzip -t /tmp/instantclient.zip >/dev/null 2>&1; then \
        mkdir -p /opt/oracle; \
        unzip -q /tmp/instantclient.zip -d /opt/oracle; \
        instantclient_dir="$(find /opt/oracle -maxdepth 1 -mindepth 1 -type d -name 'instantclient*' | head -n 1)"; \
        if [ -z "$instantclient_dir" ]; then \
            echo "Oracle Instant Client directory not found after unzip" >&2; \
            exit 1; \
        fi; \
        echo "$instantclient_dir" > /etc/ld.so.conf.d/oracle-instantclient.conf; \
        ldconfig; \
        echo "Installed Oracle Instant Client from staged zip: $instantclient_dir"; \
    else \
        echo "No staged Oracle Instant Client zip detected; building without bundled Instant Client."; \
    fi; \
    rm -f /tmp/instantclient.zip

COPY . .

# Repo 점검 Job 등 장시간 요청용
EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "--threads", "4", "--timeout", "0", "wsgi:application"]
