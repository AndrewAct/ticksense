FROM python:3.13-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv --no-cache-dir

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY api/ ./api/

RUN uv sync --package ticksense-api --no-dev

EXPOSE 8000
CMD ["uv", "run", "--package", "ticksense-api", \
     "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
