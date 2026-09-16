import os
import subprocess
import shutil
from typing import Optional, Any
from backend.app.storage.db import db

# Common Windows application aliases and paths
APP_PRESETS: dict[str, list[str]] = {
    "chrome": ["chrome.exe", "chrome", "google-chrome", r"C:\Program Files\Google\Chrome\Application\chrome.exe", r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"],
    "vscode": ["code.cmd", "code.exe", "code", r"C:\Users\AYYAPPA RAYUDU\AppData\Local\Programs\Microsoft VS Code\Code.exe"],
    "notepad": ["notepad.exe", "notepad"],
    "calc": ["calc.exe", "calculator"],
    "calculator": ["calc.exe"],
    "explorer": ["explorer.exe"],
    "file_explorer": ["explorer.exe"],
    "terminal": ["wt.exe", "powershell.exe", "cmd.exe"],
    "powershell": ["powershell.exe"],
    "cmd": ["cmd.exe"],
    "spotify": ["spotify.exe", r"C:\Users\AYYAPPA RAYUDU\AppData\Roaming\Spotify\Spotify.exe"],
    "edge": ["msedge.exe", r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"],
    "taskmanager": ["taskmgr.exe"],
    "settings": ["ms-settings:"],
    "control_panel": ["control.exe"]
}

class AppManager:
    @staticmethod
    def launch_app(app_name: str, arguments: Optional[str] = None) -> dict[str, Any]:
        """Launches an application by name or path."""
        clean_key = app_name.lower().strip()
        candidates = APP_PRESETS.get(clean_key, [app_name])
        
        launched = False
        launched_cmd = ""
        error_msg = ""
        
        for candidate in candidates:
            if candidate.endswith(":"):
                try:
                    os.startfile(candidate)
                    launched = True
                    launched_cmd = candidate
                    break
                except Exception as e:
                    error_msg = str(e)
                    continue

            if shutil.which(candidate) or os.path.exists(candidate):
                try:
                    cmd = [candidate]
                    if arguments:
                        cmd.extend(arguments.split())
                    subprocess.Popen(cmd, shell=False)
                    launched = True
                    launched_cmd = candidate
                    break
                except Exception as e:
                    error_msg = str(e)
                    continue

        if not launched:
            try:
                cmd_str = f"start {app_name}"
                if arguments:
                    cmd_str += f" {arguments}"
                subprocess.Popen(cmd_str, shell=True)
                launched = True
                launched_cmd = f"start {app_name}"
            except Exception as e:
                error_msg = str(e)

        status = "SUCCESS" if launched else "FAILED"
        db.log_audit("APP_LAUNCH", f"Launch {app_name} (target: {launched_cmd})", "AppManager", status, error_msg)
        
        return {"status": "success" if launched else "failed", "app": app_name, "command": launched_cmd}

    @staticmethod
    def get_open_windows() -> list[dict[str, Any]]:
        windows = []
        try:
            import pygetwindow as gw
            for w in gw.getAllWindows():
                if w.title and w.visible:
                    windows.append({
                        "title": w.title,
                        "is_active": w.isActive,
                        "is_minimized": w.isMinimized
                    })
        except Exception:
            pass
        return windows

app_manager = AppManager()
