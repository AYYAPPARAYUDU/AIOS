import os
import shutil
import math
import mimetypes
from pathlib import Path
from typing import Optional, Any
from backend.app.config import settings
from backend.app.storage.db import db

class StorageManager:
    def __init__(self, pool_dir: Path = settings.STORAGE_POOL_DIR, max_bytes: int = settings.STORAGE_MAX_BYTES):
        self.pool_dir = pool_dir
        self.max_bytes = max_bytes
        self.pool_dir.mkdir(parents=True, exist_ok=True)

    def get_pool_stats(self) -> dict[str, Any]:
        """Calculates current size, quota, and file statistics of the 50GB storage pool."""
        total_used = 0
        file_count = 0
        categories: dict[str, int] = {}
        
        for root, _, files in os.walk(self.pool_dir):
            for f in files:
                fp = os.path.join(root, f)
                try:
                    sz = os.path.getsize(fp)
                    total_used += sz
                    file_count += 1
                    ext = Path(f).suffix.lower() or "other"
                    categories[ext] = categories.get(ext, 0) + sz
                except OSError:
                    pass
        
        free_bytes = max(0, self.max_bytes - total_used)
        used_percentage = round((total_used / self.max_bytes) * 100, 2)
        
        # Also get host drive total/free stats
        host_disk = shutil.disk_usage(self.pool_dir)
        
        return {
            "pool_path": str(self.pool_dir),
            "max_quota_bytes": self.max_bytes,
            "max_quota_gb": round(self.max_bytes / (1024**3), 2),
            "used_bytes": total_used,
            "used_mb": round(total_used / (1024**2), 2),
            "used_gb": round(total_used / (1024**3), 4),
            "free_bytes": free_bytes,
            "free_gb": round(free_bytes / (1024**3), 2),
            "used_percentage": used_percentage,
            "total_files": file_count,
            "category_distribution": categories,
            "host_disk_total_gb": round(host_disk.total / (1024**3), 2),
            "host_disk_free_gb": round(host_disk.free / (1024**3), 2),
            "indexed_database_records": db.count_indexed_files()
        }

    def save_file_to_pool(self, filename: str, content: bytes, subfolder: str = "documents") -> dict[str, Any]:
        """Saves a file into the local 50GB storage pool with quota enforcement."""
        incoming_size = len(content)
        stats = self.get_pool_stats()
        
        if stats["used_bytes"] + incoming_size > self.max_bytes:
            raise ValueError(f"Storage pool quota of {stats['max_quota_gb']}GB exceeded! Available: {stats['free_gb']}GB")
            
        target_dir = self.pool_dir / subfolder
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Sanitize filename
        clean_name = Path(filename).name
        target_path = target_dir / clean_name
        
        # If exists, append timestamp
        if target_path.exists():
            stem = target_path.stem
            suffix = target_path.suffix
            import time
            target_path = target_dir / f"{stem}_{int(time.time())}{suffix}"
            
        with open(target_path, "wb") as f:
            f.write(content)
            
        # Index file in SQLite database
        self.index_single_file(target_path)
        
        return {
            "saved": True,
            "path": str(target_path),
            "filename": target_path.name,
            "size": incoming_size
        }

    def list_pool_files(self, subfolder: str = "") -> list[dict[str, Any]]:
        """Lists files and folders inside the storage pool."""
        scan_dir = self.pool_dir / subfolder if subfolder else self.pool_dir
        if not scan_dir.exists():
            return []
            
        items = []
        try:
            for entry in os.scandir(scan_dir):
                is_dir = entry.is_dir()
                stat = entry.stat()
                items.append({
                    "name": entry.name,
                    "path": entry.path,
                    "is_directory": is_dir,
                    "size_bytes": 0 if is_dir else stat.st_size,
                    "size_formatted": self._format_size(stat.st_size if not is_dir else 0),
                    "modified": stat.st_mtime,
                    "extension": Path(entry.name).suffix.lower() if not is_dir else "folder"
                })
        except OSError:
            pass
        return sorted(items, key=lambda x: (not x["is_directory"], x["name"].lower()))

    def delete_pool_file(self, file_path_str: str) -> bool:
        """Deletes a file from the local storage pool."""
        target = Path(file_path_str).resolve()
        if not str(target).startswith(str(self.pool_dir.resolve())):
            raise PermissionError("Access denied: Cannot delete files outside storage pool")
        if target.is_file():
            target.unlink()
            return True
        elif target.is_dir():
            shutil.rmtree(target)
            return True
        return False

    def index_single_file(self, file_path: Path):
        """Indexes a single file: extracts summary and metadata, stores in DB."""
        try:
            stat = file_path.stat()
            ext = file_path.suffix.lower()
            mime, _ = mimetypes.guess_type(str(file_path))
            category = self._categorize_extension(ext)
            
            summary = ""
            # If text readable, extract preview
            if ext in [".txt", ".md", ".py", ".js", ".html", ".css", ".json", ".csv", ".log", ".yaml", ".yml", ".sh", ".bat"]:
                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        text = f.read(4000)
                        summary = text.strip()[:1000]
                except Exception:
                    pass
            
            keywords = f"{file_path.stem} {ext} {category}"
            db.index_file(
                file_path=str(file_path.resolve()),
                file_name=file_path.name,
                file_size=stat.st_size,
                extension=ext,
                category=category,
                summary=summary,
                keywords=keywords,
                embedding=None,
                last_modified=stat.st_mtime
            )
        except Exception:
            pass

    def scan_and_index_directory(self, target_dir: str, max_files: int = 500) -> int:
        """Recursively scans and indexes files from any user directory into the local knowledge database."""
        path_obj = Path(target_dir).resolve()
        if not path_obj.exists():
            return 0
            
        count = 0
        for root, dirs, files in os.walk(path_obj):
            # Skip hidden / heavy system folders
            dirs[:] = [d for d in dirs if not d.startswith(".") and d not in ["node_modules", ".git", "venv", ".venv", "__pycache__", "$Recycle.Bin"]]
            for f in files:
                if count >= max_files:
                    break
                fp = Path(root) / f
                try:
                    self.index_single_file(fp)
                    count += 1
                except Exception:
                    pass
            if count >= max_files:
                break
        return count

    def semantic_or_keyword_search(self, query: str) -> list[dict[str, Any]]:
        """Searches indexed laptop files for relevant documents and content."""
        return db.search_indexed_files(query, limit=20)

    @staticmethod
    def _categorize_extension(ext: str) -> str:
        ext = ext.lower()
        if ext in [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".svg", ".ico"]:
            return "image"
        if ext in [".mp4", ".mkv", ".avi", ".mov", ".wmv"]:
            return "video"
        if ext in [".mp3", ".wav", ".aac", ".flac", ".ogg", ".m4a"]:
            return "audio"
        if ext in [".pdf", ".doc", ".docx", ".txt", ".rtf", ".odt", ".epub"]:
            return "document"
        if ext in [".py", ".js", ".ts", ".html", ".css", ".json", ".c", ".cpp", ".java", ".go", ".rs", ".sql", ".sh", ".bat", ".ps1"]:
            return "code"
        if ext in [".zip", ".rar", ".7z", ".tar", ".gz"]:
            return "archive"
        if ext in [".exe", ".msi", ".dll"]:
            return "executable"
        return "other"

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        if size_bytes == 0:
            return "0 B"
        size_name = ("B", "KB", "MB", "GB", "TB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_name[i]}"

storage_mgr = StorageManager()
