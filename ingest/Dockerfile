FROM python:3.12-slim

RUN pip install uv --no-cache-dir

WORKDIR /app

# Copy workspace root for uv workspace resolution
COPY pyproject.toml uv.lock ./
COPY ingest/ ./ingest/

RUN uv sync --package ticksense-ingest --no-dev

CMD ["uv", "run", "--package", "ticksense-ingest", "python", "-m", "ingest.main"]
