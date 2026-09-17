@echo off
title JARVIS AIOS - Angular Neural Operating System & Custom LLM ABHI
color 0B

echo ==============================================================================
echo                 J.A.R.V.I.S.  A I O S  -  A N G U L A R  V 2 . 0
echo       Local-First Multi-Agent Laptop Operating System & Custom LLM (ABHI)
echo ==============================================================================
echo.

:: 1. Check Python virtual environment
if not exist ".venv\Scripts\python.exe" (
    echo [*] Setting up Python virtual environment...
    python -m venv .venv
    echo [*] Installing dependencies from requirements.txt...
    .\.venv\Scripts\python -m pip install --upgrade pip
    .\.venv\Scripts\python -m pip install -r requirements.txt
)

:: 2. Check Angular frontend build
if not exist "frontend\dist\jarvis-aios\browser\index.html" (
    if not exist "frontend\dist\jarvis-aios\index.html" (
        echo [*] Building Angular Frontend...
        cd frontend
        call npm install
        call npm run build
        cd ..
    )
)

:: 3. Verify Ollama & Register / Pre-warm Custom LLM ABHI
echo [*] Checking Ollama engine connection...
curl -s http://127.0.0.1:11434/api/tags >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [!] Starting Ollama service in background...
    start /b ollama serve >nul 2>&1
    timeout /t 3 /nobreak >nul 2>&1
)

echo [*] Verifying custom LLM 'abhi'...
call ollama create abhi -f Modelfile >nul 2>&1

echo [*] Pre-warming Custom LLM (abhi) onto 100%% GPU VRAM...
start /b ollama run abhi --keepalive 24h "" >nul 2>&1

:: 4. Launch JARVIS in Browser
start http://localhost:8000

:: 5. Start FastAPI Backend Server
echo [*] Launching JARVIS Core Server on http://localhost:8000 ...
.\.venv\Scripts\python -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload

pause
