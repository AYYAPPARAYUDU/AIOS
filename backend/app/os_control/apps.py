import os
import subprocess
import shutil
from typing import Optional, Any
from backend.app.storage.db import db

# Common Windows application aliases
APP_PRESETS: dict[str, list[str]] = {
    "chrome": ["chrome", "google-chrome", r"C:\Program Files\Google\Chrome\Application\chrome.exe", r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"],
    "vscode": ["code", r"C:\Users\AYYAPPA RAYUDU\AppData\Local\Programs\Microsoft VS Code\Code.exe"],
    "notepad": ["notepad.exe", "notepad"],
    "calc": ["calc.exe", "calc"],
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
    """Manages launching, listing, and focusing desktop applications."""

    @staticmethod
    def launch_app(app_name: str, arguments: Optional[str] = None) -> dict[str, Any]:
        """Launches an application by name, executable path, or common alias."""
        clean_key = app_name.lower().strip()
        candidates = APP_PRESETS.get(clean_key, [app_name])
        
        launched = False
        launched_cmd = ""
        error_msg = ""
        
        for candidate in candidates:
            # Check if URI scheme (like ms-settings:)
            if candidate.endswith(":"):
                try:
                    os.startfile(candidate)
                    launched = True
                    launched_cmd = candidate
                    break
                except Exception as e:
                    error_msg = str(e)
                    continue

            # Check if in PATH
            if shutil.which(candidate):
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

            # Check if full path exists
            if os.path.exists(candidate):
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
            # Try generic start via shell
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
        
        if launched:
            return {"status": "success", "app": app_name, "command": launched_cmd}
        else:
            return {"status": "failed", "app": app_name, "error": error_msg or "Application not found"}

    @staticmethod
    def get_open_windows() -> list[dict[str, Any]]:
        """Lists active top-level application windows."""
        windows = []
        try:
            import pygetwindow as gw
            for w in gw.getAllWindows():
                if w.title and w.visible:
                    windows.append({
                        "title": w.title,
                        "left": w.left,
                        "top": w.top,
                        "width": w.width,
                        "height": w.height,
                        "is_active": w.isActive,
                        "is_minimized": w.isMinimized,
                        "is_maximized": w.isMaximized
                    })
        except Exception:
            pass
        return windows

    @staticmethod
    def focus_window(title_keyword: str) -> dict[str, Any]:
        """Brings a window containing title_keyword to the foreground."""
        try:
            import pygetwindow as gw
            for w in gw.getWindowsWithTitle(title_keyword):
                if w.title:
                    try:
                        if w.isMinimized:
                            w.restore()
                        w.activate()
                        return {"status": "success", "focused": w.title}
                    except Exception:
                        pass
        except Exception as e:
            return {"status": "failed", "error": str(e)}
        return {"status": "not_found", "message": f"No window matching '{title_keyword}'"}

app_manager = AppManager()
