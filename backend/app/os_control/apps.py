import os
import subprocess
import shutil
from typing import Optional, Any
from backend.app.storage.db import db

# Common Windows application aliases and known absolute paths
APP_PATHS: dict[str, list[str]] = {
    "chrome": [
        "chrome", "google-chrome",
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe")
    ],
    "vscode": [
        "code", "code.cmd",
        os.path.expandvars(r"%LOCALAPPDATA%\Programs\Microsoft VS Code\Code.exe"),
        r"C:\Program Files\Microsoft VS Code\Code.exe"
    ],
    "notepad": ["notepad.exe", "notepad"],
    "calc": ["calc.exe", "calc"],
    "calculator": ["calc.exe"],
    "explorer": ["explorer.exe", "explorer"],
    "terminal": ["wt.exe", "powershell.exe", "cmd.exe"],
    "powershell": ["powershell.exe"],
    "cmd": ["cmd.exe"],
    "spotify": [
        "spotify.exe", "spotify",
        os.path.expandvars(r"%APPDATA%\Spotify\Spotify.exe")
    ],
    "edge": [
        "msedge.exe", "msedge",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"
    ],
    "taskmanager": ["taskmgr.exe"],
    "settings": ["ms-settings:"],
    "control_panel": ["control.exe"]
}

class AppManager:
    @staticmethod
    def launch_app(app_name: str, arguments: Optional[str] = None) -> dict[str, Any]:
        """Launches an application with multi-layer Windows path resolution and shell execution."""
        clean_key = app_name.lower().strip()
        candidates = APP_PATHS.get(clean_key, [app_name])
        
        launched = False
        launched_cmd = ""
        error_msg = ""
        
        # Method 1: Check known absolute paths or URI schemes
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

            if os.path.isabs(candidate) and os.path.exists(candidate):
                try:
                    cmd = [candidate]
                    if arguments:
                        cmd.extend(arguments.split())
                    subprocess.Popen(cmd)
                    launched = True
                    launched_cmd = candidate
                    break
                except Exception as e:
                    error_msg = str(e)
                    continue

        # Method 2: Use Windows Shell 'start' command
        if not launched:
            try:
                target = candidates[0] if candidates else app_name
                # Windows start with empty title string
                cmd_str = f'start "" "{target}"'
                if arguments:
                    cmd_str += f' {arguments}'
                subprocess.Popen(cmd_str, shell=True)
                launched = True
                launched_cmd = cmd_str
            except Exception as e:
                error_msg = str(e)

        # Method 3: Fallback os.system or direct startfile
        if not launched:
            try:
                os.startfile(app_name)
                launched = True
                launched_cmd = app_name
            except Exception as e:
                error_msg = str(e)

        status = "SUCCESS" if launched else "FAILED"
        db.log_audit("APP_LAUNCH", f"Launch {app_name} (target: {launched_cmd})", "AppManager", status, error_msg)
        
        return {
            "status": "success" if launched else "failed",
            "app": app_name,
            "command": launched_cmd,
            "error": error_msg if not launched else None
        }

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
