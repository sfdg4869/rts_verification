# QA Portal / RTS Check API
# Oracle Thick: put instantclient-basic-linuxx64.zip next to this Dockerfile before build.
#   cp /home/maxgauge/oracle/instantclient-basic-linuxx64.zip ./
#   docker compose build web
FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Unzipped folder is renamed to /opt/oracle/instantclient (any IC zip version).
ENV ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient

COPY instantclient-basic-linuxx64.zip /tmp/instantclient-basic-linuxx64.zip

RUN apt-get update && apt-get install -y --no-install-recommends \
    libaio1 unzip ca-certificates \
    openjdk-17-jre-headless \
    && mkdir -p /opt/oracle \
    && cd /opt/oracle \
    && unzip -q /tmp/instantclient-basic-linuxx64.zip \
    && rm -f /tmp/instantclient-basic-linuxx64.zip \
    && IC="$(ls -d instantclient_* | head -1)" \
    && test -n "$IC" \
    && mv "$IC" instantclient \
    && echo "/opt/oracle/instantclient" > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN rm -f /app/instantclient-basic-linuxx64.zip

EXPOSE 5000

# Access log to stdout so docker compose logs shows each HTTP request.
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "1", "--threads", "4", "--timeout", "0", "--access-logfile", "-", "--error-logfile", "-", "--log-level", "info", "wsgi:application"]
