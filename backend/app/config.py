import os
from pathlib import Path
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # App Information
    APP_NAME: str = "JARVIS AIOS"
    VERSION: str = "2.0.0"
    ENVIRONMENT: str = "production"
    DEBUG: bool = False
    
    # Server settings
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    CORS_ORIGINS: list[str] = ["*"]
    
    # Ollama LLM Settings
    OLLAMA_BASE_URL: str = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
    OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL", "qwen3:8b")
    OLLAMA_EMBED_MODEL: str = os.getenv("OLLAMA_EMBED_MODEL", "qwen3:8b")
    OLLAMA_TIMEOUT: float = 120.0
    
    # Local Storage Pool (50GB Local Storage DB)
    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent
    STORAGE_POOL_DIR: Path = BASE_DIR / "database" / "local_store"
    STORAGE_MAX_BYTES: int = 50 * 1024 * 1024 * 1024  # 50 GB
    SQLITE_DB_PATH: Path = BASE_DIR / "database" / "jarvis_memory.db"
    SCREENSHOTS_DIR: Path = BASE_DIR / "database" / "screenshots"
    TEMP_DIR: Path = BASE_DIR / "database" / "temp"
    
    # OS Controller Settings
    ENABLE_SHELL_EXECUTION: bool = True
    COMMAND_TIMEOUT_SECONDS: int = 45
    ALLOWED_DRIVES: list[str] = ["C:\\", "D:\\", "E:\\", "F:\\"]
    
    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

# Ensure critical directories exist
settings.STORAGE_POOL_DIR.mkdir(parents=True, exist_ok=True)
settings.SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
settings.TEMP_DIR.mkdir(parents=True, exist_ok=True)
(settings.BASE_DIR / "database").mkdir(parents=True, exist_ok=True)
