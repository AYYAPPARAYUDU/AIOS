#!/usr/bin/env bash
echo "=============================================================================="
echo "                     J.A.R.V.I.S.  A I O S  -  V 2 . 0"
echo "=============================================================================="

if [ ! -d ".venv" ]; then
    echo "[*] Creating virtual environment..."
    python3 -m venv .venv
    ./.venv/bin/pip install --upgrade pip
    ./.venv/bin/pip install -r requirements.txt
fi

echo "[*] Starting JARVIS AIOS Server..."
./.venv/bin/uvicorn backend.app.main:app --host 0.0.0.0 --port 8000
