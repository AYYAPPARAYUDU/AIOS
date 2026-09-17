import os
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from backend.app.config import settings
from backend.app.core.llm import ollama_client
from backend.app.routes import api_system, api_agents, api_storage, api_voice, api_clone, api_mcp, ws_hub

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Pre-warm Ollama model onto GPU immediately
    asyncio.create_task(ollama_client.warm_up())
    yield
    # Shutdown

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.VERSION,
    description="JARVIS AIOS - Complete Local-First Autonomous Cognitive Operating System & AI Clone",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    lifespan=lifespan
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    return {
        "status": "online",
        "app": settings.APP_NAME,
        "version": settings.VERSION,
        "model": settings.OLLAMA_MODEL,
        "storage_pool_gb": round(settings.STORAGE_MAX_BYTES / (1024**3), 1),
        "clone_engine": "active_evolving"
    }

# Include API Routers
app.include_router(api_system.router)
app.include_router(api_agents.router)
app.include_router(api_storage.router)
app.include_router(api_voice.router)
app.include_router(api_clone.router)
app.include_router(api_mcp.router)
app.include_router(ws_hub.router)

# Serve storage files and screenshots
app.mount("/static/screenshots", StaticFiles(directory=str(settings.SCREENSHOTS_DIR)), name="screenshots")
app.mount("/static/storage", StaticFiles(directory=str(settings.STORAGE_POOL_DIR)), name="storage")

# Serve Angular production distribution if built, else fallback
angular_dist_browser = settings.BASE_DIR / "frontend" / "dist" / "jarvis-aios" / "browser"
angular_dist = settings.BASE_DIR / "frontend" / "dist" / "jarvis-aios"
frontend_src = settings.BASE_DIR / "frontend"

if angular_dist_browser.exists():
    app.mount("/", StaticFiles(directory=str(angular_dist_browser), html=True), name="frontend")
elif angular_dist.exists():
    app.mount("/", StaticFiles(directory=str(angular_dist), html=True), name="frontend")
elif frontend_src.exists():
    app.mount("/", StaticFiles(directory=str(frontend_src), html=True), name="frontend")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.app.main:app", host=settings.HOST, port=settings.PORT, reload=False)
