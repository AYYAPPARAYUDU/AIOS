import os
import subprocess
import webbrowser
from typing import Optional, Any
from backend.app.storage.db import db

# Common Windows application aliases, URIs, and known absolute paths
APP_PATHS: dict[str, list[str]] = {
    "whatsapp": [
        "whatsapp:",
        "whatsapp.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\WhatsApp\WhatsApp.exe"),
        os.path.expandvars(r"%ProgramFiles%\WindowsApps\WhatsApp*"),
        "https://web.whatsapp.com"
    ],
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
    "youtube": ["https://www.youtube.com"],
    "github": ["https://github.com"],
    "gmail": ["https://mail.google.com"],
    "telegram": [
        "telegram:",
        "telegram.exe",
        os.path.expandvars(r"%APPDATA%\Telegram Desktop\Telegram.exe"),
        "https://web.telegram.org"
    ],
    "discord": [
        "discord:",
        "discord.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\Discord\Update.exe --processStart Discord.exe"),
        "https://discord.com/app"
    ],
    "spotify": [
        "spotify.exe", "spotify", "spotify:",
        os.path.expandvars(r"%APPDATA%\Spotify\Spotify.exe"),
        "https://open.spotify.com"
    ],
    "edge": [
        "msedge.exe", "msedge",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"
    ],
    "taskmanager": ["taskmgr.exe"],
    "settings": ["ms-settings:"],
    "control_panel": ["control.exe"],
    "paint": ["mspaint.exe"],
    "wordpad": ["write.exe"],
    "chatgpt": ["https://chatgpt.com"],
    "twitter": ["https://twitter.com"],
    "x": ["https://x.com"],
    "instagram": ["https://www.instagram.com"],
    "linkedin": ["https://www.linkedin.com"],
    "reddit": ["https://www.reddit.com"],
    "netflix": ["https://www.netflix.com"]
}

def _open_url_robust(url: str) -> bool:
    """Robustly opens a web URL using Windows startfile with webbrowser and cmd fallbacks."""
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
    try:
        os.startfile(url)
        return True
    except Exception:
        try:
            webbrowser.open(url)
            return True
        except Exception:
            try:
                subprocess.Popen(f'start "" "{url}"', shell=True)
                return True
            except Exception:
                return False

class AppManager:
    @staticmethod
    def launch_app(app_name: str, arguments: Optional[str] = None) -> dict[str, Any]:
        """Launches an application or URL with zero limitations."""
        clean_key = app_name.lower().strip()

        # If it's a direct web URL or domain
        if clean_key.startswith("http://") or clean_key.startswith("https://"):
            _open_url_robust(app_name)
            db.log_audit("APP_LAUNCH", f"Open Web URL: {app_name}", "AppManager", "SUCCESS")
            return {"status": "success", "app": app_name, "type": "web_url"}

        import urllib.parse
        if clean_key == "youtube":
            yt_url = f"https://www.youtube.com/results?search_query={urllib.parse.quote_plus(arguments)}" if arguments else "https://www.youtube.com"
            _open_url_robust(yt_url)
            db.log_audit("APP_LAUNCH", f"YouTube Launch: {arguments or 'Home'}", "AppManager", "SUCCESS")
            return {"status": "success", "app": "youtube", "target": yt_url, "query": arguments}

        if clean_key == "github":
            gh_url = f"https://github.com/search?q={urllib.parse.quote_plus(arguments)}" if arguments else "https://github.com"
            _open_url_robust(gh_url)
            db.log_audit("APP_LAUNCH", f"GitHub Launch: {arguments or 'Home'}", "AppManager", "SUCCESS")
            return {"status": "success", "app": "github", "target": gh_url, "query": arguments}

        if clean_key in ["google", "web", "browser"] and arguments:
            g_url = f"https://www.google.com/search?q={urllib.parse.quote_plus(arguments)}"
            _open_url_robust(g_url)
            db.log_audit("APP_LAUNCH", f"Google Search: {arguments}", "AppManager", "SUCCESS")
            return {"status": "success", "app": "google", "target": g_url, "query": arguments}

        candidates = APP_PATHS.get(clean_key, [app_name])
        launched = False
        launched_cmd = ""
        error_msg = ""
        
        # Method 1: Check known absolute paths or URI schemes
        for candidate in candidates:
            if candidate.startswith("http://") or candidate.startswith("https://"):
                _open_url_robust(candidate)
                launched = True
                launched_cmd = candidate
                break

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
                cmd_str = f'start "" "{target}"'
                if arguments:
                    cmd_str += f' {arguments}'
                subprocess.Popen(cmd_str, shell=True)
                launched = True
                launched_cmd = cmd_str
            except Exception as e:
                error_msg = str(e)

        # Method 3: Fallback startfile
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
