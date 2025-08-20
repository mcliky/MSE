# Dockerfile (MES)
FROM python:3.11-slim

WORKDIR /app

# minimal tools (curl for healthcheck)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# runtime deps (no local requirements.txt needed)
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" sqlalchemy httpx pydantic

# writable dir for SQLite
RUN mkdir -p /data && chmod 777 /data

EXPOSE 8300

HEALTHCHECK --interval=15s --timeout=3s --retries=5 --start-period=5s \
  CMD curl -fsS http://127.0.0.1:8300/health || exit 1

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8300"]
