# Stage 1: Build Angular Frontend
FROM node:20-slim AS frontend-build
WORKDIR /app/frontend

# Copy package manifests for efficient layer caching
COPY frontend/package*.json ./
RUN npm ci || npm install

# Copy frontend source and build production bundle
COPY frontend/ ./
RUN npm run build

# Stage 2: Backend & Final Image
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    OLLAMA_BASE_URL=http://host.docker.internal:11434 \
    OLLAMA_MODEL=qwen3:8b

# Install system dependencies (build tools, audio, curl for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    espeak \
    ffmpeg \
    libasound2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend application and database template/structure
COPY backend/ /app/backend/
COPY database/ /app/database/

# Copy built Angular distribution from Stage 1
COPY --from=frontend-build /app/frontend/dist /app/frontend/dist

EXPOSE 8000

# Health check using built-in /health endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
