FROM python:3.13-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt
RUN useradd --create-home app
COPY app ./app
COPY alembic.ini ./
COPY migrations ./migrations
USER app

FROM base AS tests
USER root
COPY requirements-dev.txt ./
RUN python -m pip install --no-cache-dir -r requirements-dev.txt
COPY pyproject.toml ./
COPY tests ./tests
COPY scripts ./scripts
USER app
CMD ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider"]

FROM base AS runtime
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
