# 🤖 J.A.R.V.I.S. AIOS — Local-First Multi-Agent Neural Operating System

An advanced, Iron Man-inspired AI operating system interface and multi-agent controller designed for total laptop control with native OS execution, 50GB local storage knowledge database, local Ollama LLM integration (`qwen3:8b`), and an interactive 3D WebGL Holographic visualizer.

---

## 🌟 Key Features

### 1. 🌐 Complete Laptop & OS Control
- **Hardware Telemetry**: Real-time monitoring of CPU per-core load, RAM usage, Battery state, Disk drives, and Network I/O speeds.
- **Process Manager**: Real-time process explorer with memory/CPU sorting and one-click process termination.
- **System Settings**: Instant control over system volume, display brightness, screen lock, and power states (sleep, restart, shutdown).
- **App Launcher**: Instant launching and window focusing for Chrome, VS Code, Windows Terminal, Notepad, Calculator, Explorer, and custom applications.
- **Direct PowerShell Runner**: Execute system commands in real time with stdout/stderr capture and safety guards.
- **GUI Automation**: Screen capture to PNG/base64 thumbnail, automated mouse movements/clicks, and keyboard hotkeys.

### 2. 🧠 Multi-Agent Cognitive Swarm
- **JARVIS Prime (Supervisor)**: High-level reasoning, intent decomposition, and context synthesis.
- **Mark-IV OS Controller**: Handles hardware actions, volume, brightness, process management, and app launching.
- **Vault-50 Storage Manager**: Manages the 50GB local storage database, semantic indexing, and knowledge queries.
- **Cortex Web Researcher**: Web scraping, information lookup, and URL fetching.
- **Specter GUI Automator**: Screen analysis, mouse/keyboard automation, and vision workflows.

### 3. 🗄️ 50GB Local Storage DB & Knowledge Vault
- Allocated 50GB local storage pool with quota enforcement.
- Deep laptop file indexer that indexes documents, code, logs, and notes into an embedded SQLite knowledge database.
- Fast keyword and semantic search across indexed files.

### 4. 🔮 3D Holographic Companion Avatar
- Built with Three.js WebGL with concentric gimbal rings, quantum arc core, and 200+ orbital particle nodes.
- **Audio Reactive**: Pulses and reacts in real time to voice frequencies and speaking states.
- **Mouse Parallax**: Hologram core tracks cursor movements smoothly.
- Dynamic states for Idle, Listening, Thinking, Executing OS commands, and Speaking.

### 5. 🗣️ Voice Engine (STT & TTS)
- Web Speech Recognition for voice commands with dedicated microphone toggle.
- Integrated Text-to-Speech synthesis with voice tone configurations.

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+ (Python 3.14 recommended)
- Ollama installed with `qwen3:8b` (`ollama run qwen3:8b`)

### Option A: Windows 1-Click Launcher
Double-click `start.bat` or run:
```cmd
start.bat
```

### Option B: Manual Launch
```bash
# 1. Create and activate virtual environment
python -m venv .venv
# On Windows:
.\.venv\Scripts\activate
# On Linux/macOS:
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start JARVIS
python -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000
```
Open **http://localhost:8000** in your browser.

### Option C: Docker Deployment
```bash
docker-compose up --build -d
```

---

## 🛠️ API Documentation
Once running, explore interactive Swagger API docs at **http://localhost:8000/api/docs**.
