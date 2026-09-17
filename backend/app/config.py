import os
from pathlib import Path
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # App Information
    APP_NAME: str = "JARVIS AIOS"
    VERSION: str = "3.5.0"
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
    
    # Local Storage Pool (200GB Local Storage DB)
    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent
    DATABASE_DIR: Path = BASE_DIR / "database"
    STORAGE_POOL_DIR: Path = DATABASE_DIR / "knowledge_vault"
    STORAGE_LEGACY_DIR: Path = DATABASE_DIR / "local_store"
    CLONE_MEMORY_DIR: Path = DATABASE_DIR / "clone_memory"
    VECTOR_INDEX_DIR: Path = DATABASE_DIR / "vector_index"
    SCREENSHOTS_DIR: Path = DATABASE_DIR / "screenshots"
    TEMP_DIR: Path = DATABASE_DIR / "temp"
    AUDIT_DIR: Path = DATABASE_DIR / "audit_logs"
    SQLITE_DB_PATH: Path = DATABASE_DIR / "jarvis_memory.db"
    STORAGE_MAX_BYTES: int = 200 * 1024 * 1024 * 1024  # 200 GB
    
    # OS Controller Settings
    ENABLE_SHELL_EXECUTION: bool = True
    COMMAND_TIMEOUT_SECONDS: int = 45
    ALLOWED_DRIVES: list[str] = ["C:\\", "D:\\", "E:\\", "F:\\", "/"]
    
    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

# Ensure all database directories exist in structured order
settings.DATABASE_DIR.mkdir(parents=True, exist_ok=True)
settings.STORAGE_POOL_DIR.mkdir(parents=True, exist_ok=True)
settings.CLONE_MEMORY_DIR.mkdir(parents=True, exist_ok=True)
settings.VECTOR_INDEX_DIR.mkdir(parents=True, exist_ok=True)
settings.SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
settings.TEMP_DIR.mkdir(parents=True, exist_ok=True)
settings.AUDIT_DIR.mkdir(parents=True, exist_ok=True)
