import os
import sys
import subprocess
import webbrowser
from typing import Optional, Any
from backend.app.storage.db import db

# Common application aliases, URIs, and known paths
APP_PATHS: dict[str, list[str]] = {
    "whatsapp": [
        "whatsapp:",
        "whatsapp.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\WhatsApp\WhatsApp.exe") if os.name == 'nt' else "",
        os.path.expandvars(r"%ProgramFiles%\WindowsApps\WhatsApp*") if os.name == 'nt' else "",
        "https://web.whatsapp.com"
    ],
    "chrome": [
        "chrome", "google-chrome", "google-chrome-stable", "chromium", "chromium-browser",
        r"C:\Program Files\Google\Chrome\Application\chrome.exe" if os.name == 'nt' else "",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe" if os.name == 'nt' else "",
        os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe") if os.name == 'nt' else ""
    ],
    "vscode": [
        "code", "code.cmd",
        os.path.expandvars(r"%LOCALAPPDATA%\Programs\Microsoft VS Code\Code.exe") if os.name == 'nt' else "",
        r"C:\Program Files\Microsoft VS Code\Code.exe" if os.name == 'nt' else ""
    ],
    "notepad": ["notepad.exe", "notepad", "gedit", "nano"],
    "calc": ["calc.exe", "calc", "gnome-calculator", "kcalc"],
    "calculator": ["calc.exe", "calc", "gnome-calculator"],
    "explorer": ["explorer.exe", "explorer", "nautilus", "dolphin", "thunar"],
    "terminal": ["wt.exe", "powershell.exe", "cmd.exe", "x-terminal-emulator", "gnome-terminal"],
    "powershell": ["powershell.exe", "pwsh"],
    "cmd": ["cmd.exe", "bash"],
    "youtube": ["https://www.youtube.com"],
    "github": ["https://github.com"],
    "gmail": ["https://mail.google.com"],
    "telegram": [
        "telegram:",
        "telegram.exe", "telegram-desktop",
        os.path.expandvars(r"%APPDATA%\Telegram Desktop\Telegram.exe") if os.name == 'nt' else "",
        "https://web.telegram.org"
    ],
    "discord": [
        "discord:",
        "discord.exe", "discord",
        os.path.expandvars(r"%LOCALAPPDATA%\Discord\Update.exe --processStart Discord.exe") if os.name == 'nt' else "",
        "https://discord.com/app"
    ],
    "spotify": [
        "spotify.exe", "spotify", "spotify:",
        os.path.expandvars(r"%APPDATA%\Spotify\Spotify.exe") if os.name == 'nt' else "",
        "https://open.spotify.com"
    ],
    "edge": [
        "msedge.exe", "msedge", "microsoft-edge",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" if os.name == 'nt' else "",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe" if os.name == 'nt' else ""
    ],
    "taskmanager": ["taskmgr.exe", "htop", "top"],
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

# Clean out empty strings in lists
for k in APP_PATHS:
    APP_PATHS[k] = [p for p in APP_PATHS[k] if p]

def _open_url_robust(url: str) -> bool:
    """Robustly opens a web URL using OS shell, direct browsers, and Python webbrowser."""
    if not url.startswith("http://") and not url.startswith("https://") and not url.endswith(":"):
        url = "https://" + url
    
    # 1. Direct Python webbrowser standard library
    try:
        if webbrowser.open(url, new=2):
            return True
    except Exception:
        pass

    # 2. Windows specific startfile / start
    if os.name == 'nt':
        try:
            os.startfile(url)
            return True
        except Exception:
            pass

        try:
            subprocess.Popen(f'start "" "{url}"', shell=True)
            return True
        except Exception:
            pass

        try:
            subprocess.Popen(["explorer.exe", url])
            return True
        except Exception:
            pass

        known_browsers = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
            os.path.expandvars(r"%LOCALAPPDATA%\Programs\Opera\launcher.exe"),
            r"C:\Program Files\Mozilla Firefox\firefox.exe"
        ]
        for b_path in known_browsers:
            if os.path.exists(b_path):
                try:
                    subprocess.Popen([b_path, url])
                    return True
                except Exception:
                    continue

    # 3. Linux / macOS fallbacks
    else:
        for opener in ["xdg-open", "open", "google-chrome", "firefox"]:
            try:
                subprocess.Popen([opener, url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                return True
            except Exception:
                continue

    return False

class AppManager:
    @staticmethod
    def launch_app(app_name: str, arguments: Optional[str] = None) -> dict[str, Any]:
        """Launches an application or URL with zero latency and ultra-robust fallbacks."""
        clean_key = app_name.lower().strip()

        # If it's a direct web URL or domain
        if clean_key.startswith("http://") or clean_key.startswith("https://"):
            _open_url_robust(app_name)
            db.log_audit("APP_LAUNCH", f"Open Web URL: {app_name}", "AppManager", "SUCCESS")
            return {"status": "success", "app": app_name, "type": "web_url"}

        import urllib.parse
        if clean_key in ["youtube", "open youtube", "yt", "play youtube"]:
            yt_url = f"https://www.youtube.com/results?search_query={urllib.parse.quote_plus(arguments)}" if arguments else "https://www.youtube.com"
            _open_url_robust(yt_url)
            db.log_audit("APP_LAUNCH", f"YouTube Launch: {arguments or 'Home'}", "AppManager", "SUCCESS")
            return {"status": "success", "app": "youtube", "target": yt_url, "query": arguments}

        if clean_key in ["github", "open github", "git"]:
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
        
        # Method 1: Check known absolute paths, URLs, or URI schemes
        for candidate in candidates:
            if not candidate:
                continue

            if candidate.startswith("http://") or candidate.startswith("https://"):
                _open_url_robust(candidate)
                launched = True
                launched_cmd = candidate
                break

            if candidate.endswith(":"):
                if os.name == 'nt':
                    try:
                        os.startfile(candidate)
                        launched = True
                        launched_cmd = candidate
                        break
                    except Exception as e:
                        error_msg = str(e)
                _open_url_robust(candidate)
                launched = True
                launched_cmd = candidate
                break

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

        # Method 2: Shell launch
        if not launched:
            target = candidates[0] if candidates else app_name
            if os.name == 'nt':
                try:
                    cmd_str = f'start "" "{target}"'
                    if arguments:
                        cmd_str += f' {arguments}'
                    subprocess.Popen(cmd_str, shell=True)
                    launched = True
                    launched_cmd = cmd_str
                except Exception as e:
                    error_msg = str(e)
            else:
                for runner in ["xdg-open", target]:
                    try:
                        subprocess.Popen([runner, target] if runner == "xdg-open" else [target], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        launched = True
                        launched_cmd = target
                        break
                    except Exception as e:
                        error_msg = str(e)

        # Method 3: Fallback startfile on Windows
        if not launched and os.name == 'nt':
            try:
                os.startfile(app_name)
                launched = True
                launched_cmd = app_name
            except Exception as e:
                error_msg = str(e)

        # Method 4: Fallback web search if desktop app not found
        if not launched:
            fallback_url = f"https://www.google.com/search?q={urllib.parse.quote_plus(app_name)}"
            _open_url_robust(fallback_url)
            launched = True
            launched_cmd = fallback_url

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
