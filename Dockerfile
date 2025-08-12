# ---- Base image ----
FROM python:3.11-slim

# System setup
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY app /app/app

# Optional: run as non-root
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8300

# Dev-friendly hot reload (as seen in your logs)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8300", "--reload"]
