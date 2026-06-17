# syntax=docker/dockerfile:1.25
FROM python:3.14-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN pip install --no-cache-dir uv

WORKDIR /build
COPY pyproject.toml README.md LICENSE ./
COPY src ./src

RUN uv venv /opt/venv \
 && . /opt/venv/bin/activate \
 && uv pip install --no-cache .

FROM python:3.14-slim

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN groupadd --system --gid 1000 app \
 && useradd --system --uid 1000 --gid app --home /app --shell /usr/sbin/nologin app

COPY --from=builder /opt/venv /opt/venv

USER app
WORKDIR /app

ENTRYPOINT ["nats-archive-compactor"]
