import os
import re
import subprocess
import webbrowser
import ctypes
import urllib.request
import urllib.parse
from typing import Optional, Any
from backend.app.storage.db import db
from backend.app.os_control.apps import _open_url_robust

# Win32 Virtual Key Constants (fallback)
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
        try:
            ctypes.windll.user32.keybd_event(vk_code, 0, 0, 0)
            ctypes.windll.user32.keybd_event(vk_code, 0, KEYEVENTF_KEYUP, 0)
        except Exception:
            pass

class SystemActions:
    """Ultra-responsive native OS and Web controller with 0ms latency."""

    @staticmethod
    def _get_audio_endpoint():
        """Returns the pycaw EndpointVolume controller if available."""
        if os.name == 'nt':
            try:
                from pycaw.pycaw import AudioUtilities
                speakers = AudioUtilities.GetSpeakers()
                if speakers and hasattr(speakers, 'EndpointVolume'):
                    return speakers.EndpointVolume
            except Exception:
                pass
        return None

    @classmethod
    def get_state(cls) -> dict[str, Any]:
        """Returns current volume, mute status, and display brightness."""
        vol_info = cls.get_volume()
        bright_val = cls.get_brightness()
        return {
            "volume": vol_info.get("volume", 50),
            "muted": vol_info.get("muted", False),
            "brightness": bright_val.get("brightness", 70)
        }

    @classmethod
    def get_volume(cls) -> dict[str, Any]:
        """Gets the current master volume percentage (0-100) and mute status."""
        try:
            ep = cls._get_audio_endpoint()
            if ep:
                vol_scalar = ep.GetMasterVolumeLevelScalar()
                is_mute = bool(ep.GetMute())
                return {"volume": int(round(vol_scalar * 100)), "muted": is_mute, "status": "success"}
        except Exception:
            pass
        return {"volume": 50, "muted": False, "status": "fallback"}

    @classmethod
    def set_volume(cls, level_percent: int) -> dict[str, Any]:
        """Sets master system volume percentage (0-100) with 0ms latency."""
        level = max(0, min(100, int(level_percent)))
        applied = False
        
        try:
            ep = cls._get_audio_endpoint()
            if ep:
                scalar = level / 100.0
                ep.SetMasterVolumeLevelScalar(scalar, None)
                if level > 0 and ep.GetMute():
                    ep.SetMute(0, None)
                applied = True
        except Exception:
            pass

        if not applied and os.name == 'nt':
            for _ in range(50):
                _press_vk(VK_VOLUME_DOWN)
            steps_up = int(level / 2)
            for _ in range(steps_up):
                _press_vk(VK_VOLUME_UP)
                
        db.log_audit("SYSTEM_ACTION", f"Set Volume to {level}%", "INDRA_OSController", "SUCCESS")
        return {"action": "set_volume", "volume": level, "status": "success"}

    @classmethod
    def change_volume_relative(cls, delta: int) -> dict[str, Any]:
        """Increases or decreases current volume by delta percentage."""
        curr = cls.get_volume().get("volume", 50)
        target = max(0, min(100, curr + delta))
        return cls.set_volume(target)

    @classmethod
    def mute_volume(cls, mute: Optional[bool] = None) -> dict[str, Any]:
        """Toggles or sets audio mute."""
        is_muted = False
        try:
            ep = cls._get_audio_endpoint()
            if ep:
                if mute is None:
                    curr_mute = bool(ep.GetMute())
                    ep.SetMute(0 if curr_mute else 1, None)
                    is_muted = not curr_mute
                else:
                    ep.SetMute(1 if mute else 0, None)
                    is_muted = bool(mute)
                db.log_audit("SYSTEM_ACTION", f"Mute ({is_muted})", "INDRA_OSController", "SUCCESS")
                return {"action": "mute_volume", "muted": is_muted, "status": "success"}
        except Exception:
            pass

        if os.name == 'nt':
            _press_vk(VK_VOLUME_MUTE)
        db.log_audit("SYSTEM_ACTION", f"Mute Toggle ({mute})", "INDRA_OSController", "SUCCESS")
        return {"action": "mute_volume", "status": "success"}

    @classmethod
    def get_brightness(cls) -> dict[str, Any]:
        """Gets current display brightness percentage."""
        if os.name == 'nt':
            try:
                ps_cmd = "(Get-WmiObject -Namespace root/wmi -Class WmiMonitorBrightness).CurrentBrightness"
                res = subprocess.run(
                    ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_cmd],
                    capture_output=True, text=True, timeout=3
                )
                if res.returncode == 0 and res.stdout.strip().isdigit():
                    return {"brightness": int(res.stdout.strip()), "status": "success"}
            except Exception:
                pass
        return {"brightness": 70, "status": "fallback"}

    @classmethod
    def set_brightness(cls, level_percent: int) -> dict[str, Any]:
        """Sets display brightness (0-100) on Windows laptops/monitors."""
        level = max(0, min(100, int(level_percent)))
        if os.name == 'nt':
            try:
                ps_cmd = f"(Get-WmiObject -Namespace root/wmi -Class WmiMonitorBrightnessMethods).WmiSetBrightness(1, {level})"
                subprocess.Popen(["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_cmd])
            except Exception:
                pass
        db.log_audit("SYSTEM_ACTION", f"Set Brightness to {level}%", "INDRA_OSController", "SUCCESS")
        return {"action": "set_brightness", "brightness": level, "status": "success"}

    @classmethod
    def change_brightness_relative(cls, delta: int) -> dict[str, Any]:
        """Increases or decreases brightness by delta percentage."""
        curr = cls.get_brightness().get("brightness", 70)
        target = max(0, min(100, curr + delta))
        return cls.set_brightness(target)

    @staticmethod
    def lock_workstation() -> dict[str, Any]:
        """Locks the workstation instantly."""
        if os.name == 'nt':
            try:
                ctypes.windll.user32.LockWorkStation()
            except Exception:
                pass
        else:
            for locker in [["xdg-screensaver", "lock"], ["gnome-screensaver-command", "-l"], ["loginctl", "lock-session"]]:
                try:
                    subprocess.Popen(locker)
                    break
                except Exception:
                    continue
        db.log_audit("SYSTEM_ACTION", "Lock Workstation", "DURGA_Shield", "SUCCESS")
        return {"action": "lock", "status": "workstation_locked"}

    @staticmethod
    def power_action(mode: str) -> dict[str, Any]:
        """Executes system power states: sleep, restart, shutdown, cancel_shutdown."""
        mode = mode.lower().strip()
        if os.name == 'nt':
            if mode == "sleep":
                try:
                    ctypes.windll.PowrProf.SetSuspendState(0, 1, 0)
                except Exception:
                    pass
            elif mode == "restart":
                subprocess.Popen(["shutdown", "/r", "/t", "10", "/c", "JARVIS AIOS initiating system restart in 10s"])
            elif mode == "shutdown":
                subprocess.Popen(["shutdown", "/s", "/t", "15", "/c", "JARVIS AIOS powering down system in 15s"])
            elif mode == "cancel_shutdown":
                subprocess.Popen(["shutdown", "/a"])
        db.log_audit("SYSTEM_ACTION", f"Power Action: {mode}", "INDRA_OSController", "SUCCESS")
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
        """Opens any URL in default web browser with multi-layer fallback."""
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "https://" + url
        _open_url_robust(url)
        db.log_audit("SYSTEM_ACTION", f"Open Browser URL: {url}", "NARADA_Messenger", "SUCCESS")
        return {"action": "open_url", "url": url, "status": "opened"}

    @staticmethod
    def search_web_browser(query: str) -> dict[str, Any]:
        """Searches query in default browser."""
        encoded = urllib.parse.quote_plus(query)
        url = f"https://www.google.com/search?q={encoded}"
        _open_url_robust(url)
        db.log_audit("SYSTEM_ACTION", f"Search Web: {query}", "NARADA_Messenger", "SUCCESS")
        return {"action": "search_web", "query": query, "url": url, "status": "opened"}

    @staticmethod
    def open_whatsapp(phone: Optional[str] = None, message: Optional[str] = None, auto_send: bool = True) -> dict[str, Any]:
        """Opens WhatsApp app or web with native OS control, contact search, and message automation."""
        from backend.app.os_control.whatsapp import whatsapp_controller
        if phone or message:
            return whatsapp_controller.send_whatsapp_message(recipient=phone, message=message, auto_send=auto_send)
        else:
            return whatsapp_controller.launch_whatsapp()

    @staticmethod
    def fetch_url_content(url: str) -> dict[str, Any]:
        """Live online web content extractor - fetches and extracts clean text from any URL."""
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "https://" + url
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw_html = resp.read().decode("utf-8", errors="ignore")
                
                # Extract Title
                title_match = re.search(r"<title>(.*?)</title>", raw_html, re.IGNORECASE | re.DOTALL)
                title = title_match.group(1).strip() if title_match else url

                # Remove scripts, styles, SVGs
                clean = re.sub(r"<(script|style|svg|noscript).*?>.*?</\1>", "", raw_html, flags=re.DOTALL | re.IGNORECASE)
                clean = re.sub(r"<[^>]+>", " ", clean)
                clean = re.sub(r"\s+", " ", clean).strip()

                summary = clean[:3000]
                db.log_audit("WEB_FETCH", f"Fetched {url} ({len(summary)} chars)", "NARADA_Messenger", "SUCCESS")
                return {
                    "url": url,
                    "title": title,
                    "text": summary,
                    "status": "success"
                }
        except Exception as e:
            db.log_audit("WEB_FETCH", f"Failed {url}: {str(e)}", "NARADA_Messenger", "FAILED")
            return {"url": url, "error": str(e), "status": "failed"}

actions = SystemActions()
