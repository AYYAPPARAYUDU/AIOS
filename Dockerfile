# Stage 1: Build Angular Frontend
FROM node:20-slim AS frontend-build
WORKDIR /app/frontend
COPY frontend/package.json ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# Stage 2: Backend & Final Image
FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    OLLAMA_BASE_URL=http://host.docker.internal:11434 \
    OLLAMA_MODEL=qwen3:8b

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

# Copy backend and database folders
COPY backend/ /app/backend/
COPY database/ /app/database/

# Copy built Angular distribution from Stage 1
COPY --from=frontend-build /app/frontend/dist/jarvis-aios/browser /app/frontend/dist/jarvis-aios/browser

EXPOSE 8000

CMD ["uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
