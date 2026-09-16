import os
import subprocess
import webbrowser
import ctypes
from typing import Optional, Any
from backend.app.storage.db import db

# Win32 Virtual Key Constants
VK_VOLUME_MUTE = 0xAD
VK_VOLUME_DOWN = 0xAE
VK_VOLUME_UP = 0xAF
VK_MEDIA_NEXT_TRACK = 0xB0
VK_MEDIA_PREV_TRACK = 0xB1
VK_MEDIA_PLAY_PAUSE = 0xB3
KEYEVENTF_KEYUP = 0x0002

def _press_vk(vk_code: int):
    """Sends a hardware virtual key tap via Win32 user32."""
    if os.name == 'nt':
        ctypes.windll.user32.keybd_event(vk_code, 0, 0, 0)
        ctypes.windll.user32.keybd_event(vk_code, 0, KEYEVENTF_KEYUP, 0)

class SystemActions:
    """Ultra-responsive native OS controller for Windows."""

    @staticmethod
    def set_volume(level_percent: int) -> dict[str, Any]:
        """Sets master system volume percentage (0-100)."""
        level = max(0, min(100, int(level_percent)))
        
        if os.name == 'nt':
            # 1. Reset volume to 0 (50 taps of 2% each)
            for _ in range(50):
                _press_vk(VK_VOLUME_DOWN)
            # 2. Tap volume up to reach target level
            steps_up = int(level / 2)
            for _ in range(steps_up):
                _press_vk(VK_VOLUME_UP)
                
        db.log_audit("SYSTEM_ACTION", f"Set Volume to {level}%", "OSController", "SUCCESS")
        return {"action": "set_volume", "volume": level, "status": "success"}

    @staticmethod
    def mute_volume(mute: Optional[bool] = None) -> dict[str, Any]:
        """Toggles or sets audio mute."""
        if os.name == 'nt':
            _press_vk(VK_VOLUME_MUTE)
        db.log_audit("SYSTEM_ACTION", f"Mute Toggle ({mute})", "OSController", "SUCCESS")
        return {"action": "mute_volume", "status": "success"}

    @staticmethod
    def set_brightness(level_percent: int) -> dict[str, Any]:
        """Sets display brightness (0-100) on Windows laptops/monitors."""
        level = max(0, min(100, int(level_percent)))
        if os.name == 'nt':
            try:
                ps_cmd = f"(Get-CimInstance -Namespace root/WMI -ClassName WmiMonitorBrightnessMethods).WmiSetBrightness(1,{level})"
                subprocess.Popen(["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_cmd])
            except Exception:
                pass
        db.log_audit("SYSTEM_ACTION", f"Set Brightness to {level}%", "OSController", "SUCCESS")
        return {"action": "set_brightness", "brightness": level, "status": "success"}

    @staticmethod
    def lock_workstation() -> dict[str, Any]:
        """Locks the Windows workstation instantly."""
        if os.name == 'nt':
            ctypes.windll.user32.LockWorkStation()
        db.log_audit("SYSTEM_ACTION", "Lock Workstation", "OSController", "SUCCESS")
        return {"action": "lock", "status": "workstation_locked"}

    @staticmethod
    def power_action(mode: str) -> dict[str, Any]:
        """Executes system power states: sleep, restart, shutdown, cancel_shutdown."""
        mode = mode.lower().strip()
        if os.name == 'nt':
            if mode == "sleep":
                ctypes.windll.PowrProf.SetSuspendState(0, 1, 0)
            elif mode == "restart":
                subprocess.Popen(["shutdown", "/r", "/t", "10", "/c", "JARVIS initiating system restart in 10s"])
            elif mode == "shutdown":
                subprocess.Popen(["shutdown", "/s", "/t", "15", "/c", "JARVIS powering down system in 15s"])
            elif mode == "cancel_shutdown":
                subprocess.Popen(["shutdown", "/a"])
        db.log_audit("SYSTEM_ACTION", f"Power Action: {mode}", "OSController", "SUCCESS")
        return {"action": f"power_{mode}", "status": "initiated"}

    @staticmethod
    def media_control(action: str) -> dict[str, Any]:
        """Controls media playback: play_pause, next, prev."""
        action = action.lower().strip()
        if os.name == 'nt':
            if action in ["play_pause", "play", "pause"]:
                _press_vk(VK_MEDIA_PLAY_PAUSE)
            elif action in ["next", "next_track"]:
                _press_vk(VK_MEDIA_NEXT_TRACK)
            elif action in ["prev", "prev_track", "previous"]:
                _press_vk(VK_MEDIA_PREV_TRACK)
        return {"action": f"media_{action}", "status": "success"}

    @staticmethod
    def open_url(url: str) -> dict[str, Any]:
        """Opens URL in default web browser."""
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "https://" + url
        webbrowser.open(url)
        db.log_audit("SYSTEM_ACTION", f"Open Browser URL: {url}", "OSController", "SUCCESS")
        return {"action": "open_url", "url": url, "status": "opened"}

    @staticmethod
    def search_web_browser(query: str) -> dict[str, Any]:
        """Searches query in default browser."""
        import urllib.parse
        encoded = urllib.parse.quote_plus(query)
        url = f"https://www.google.com/search?q={encoded}"
        webbrowser.open(url)
        db.log_audit("SYSTEM_ACTION", f"Search Web: {query}", "OSController", "SUCCESS")
        return {"action": "search_web", "query": query, "url": url, "status": "opened"}

actions = SystemActions()
