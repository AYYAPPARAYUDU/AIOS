import os
import shutil
import ctypes
import psutil
from typing import Any
from backend.app.os_control.actions import actions
from backend.app.os_control.apps import app_manager
from backend.app.storage.db import db

class SystemOptimizer:
    """Manages memory purging, system cleanup, and automated AI macro protocols."""

    @staticmethod
    def purge_ram() -> dict[str, Any]:
        """Frees unneeded RAM working sets across accessible processes."""
        freed_processes = 0
        if os.name == 'nt':
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    handle = ctypes.windll.kernel32.OpenProcess(0x1F0FFF, False, proc.info['pid'])
                    if handle:
                        ctypes.windll.psapi.EmptyWorkingSet(handle)
                        ctypes.windll.kernel32.CloseHandle(handle)
                        freed_processes += 1
                except Exception:
                    pass
        mem = psutil.virtual_memory()
        db.log_audit("SYSTEM_ACTION", f"RAM Purge (emptied {freed_processes} process working sets)", "SystemOptimizer", "SUCCESS")
        return {
            "status": "success",
            "processes_optimized": freed_processes,
            "current_memory_percent": mem.percent,
            "free_gb": round(mem.available / (1024**3), 2)
        }

    @staticmethod
    def clean_temp_files() -> dict[str, Any]:
        """Cleans Windows temp directory files."""
        temp_dir = os.environ.get('TEMP', r'C:\Windows\Temp')
        deleted_count = 0
        freed_bytes = 0
        try:
            for item in os.listdir(temp_dir):
                item_path = os.path.join(temp_dir, item)
                try:
                    if os.path.isfile(item_path):
                        sz = os.path.getsize(item_path)
                        os.unlink(item_path)
                        freed_bytes += sz
                        deleted_count += 1
                except Exception:
                    pass
        except Exception:
            pass
        return {
            "status": "success",
            "files_deleted": deleted_count,
            "freed_mb": round(freed_bytes / (1024**2), 2)
        }

    @staticmethod
    def execute_macro(macro_name: str) -> dict[str, Any]:
        """Executes complex multi-action AI system protocols."""
        m = macro_name.lower().strip()
        
        if m in ["dev_mode", "developer"]:
            app_manager.launch_app("vscode")
            app_manager.launch_app("terminal")
            actions.set_brightness(80)
            actions.set_volume(60)
            return {"macro": "dev_mode", "status": "Protocol engaged: VS Code & Terminal active, Brightness 80%."}

        elif m in ["focus_mode", "deep_work"]:
            actions.mute_volume(True)
            actions.set_brightness(70)
            SystemOptimizer.purge_ram()
            return {"macro": "focus_mode", "status": "Focus protocol active: Muted distractions, optimized RAM."}

        elif m in ["night_mode", "dim"]:
            actions.set_brightness(25)
            actions.set_volume(30)
            return {"macro": "night_mode", "status": "Night protocol active: Display dimmed to 25%, volume reduced."}

        elif m in ["clean_system", "purge"]:
            r1 = SystemOptimizer.purge_ram()
            r2 = SystemOptimizer.clean_temp_files()
            return {"macro": "clean_system", "ram": r1, "temp": r2}

        return {"error": f"Unknown macro: {macro_name}"}

system_optimizer = SystemOptimizer()
