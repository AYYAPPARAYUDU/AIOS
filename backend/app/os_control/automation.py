import time
import base64
import io
from pathlib import Path
from typing import Optional, Any
from backend.app.config import settings
from backend.app.storage.db import db

class AutomationController:
    """Provides GUI automation (mouse, keyboard, screen capture) for OS control."""

    @staticmethod
    def get_screen_info() -> dict[str, Any]:
        """Returns screen dimensions and cursor coordinates."""
        try:
            import pyautogui
            w, h = pyautogui.size()
            x, y = pyautogui.position()
            return {
                "screen_width": w,
                "screen_height": h,
                "mouse_x": x,
                "mouse_y": y
            }
        except Exception as e:
            return {"error": str(e), "screen_width": 1920, "screen_height": 1080, "mouse_x": 0, "mouse_y": 0}

    @staticmethod
    def take_screenshot(save_name: Optional[str] = None) -> dict[str, Any]:
        """Captures the screen and saves it as a PNG file and returns base64 thumbnail."""
        try:
            import pyautogui
            screenshot = pyautogui.screenshot()
            
            timestamp = int(time.time())
            filename = save_name if save_name else f"screenshot_{timestamp}.png"
            if not filename.endswith(".png"):
                filename += ".png"
                
            filepath = settings.SCREENSHOTS_DIR / filename
            screenshot.save(str(filepath))
            
            # Generate thumbnail base64 for immediate UI display
            buffered = io.BytesIO()
            # Resize slightly for fast preview transfer
            thumb = screenshot.resize((640, int(640 * screenshot.height / screenshot.width)))
            thumb.save(buffered, format="JPEG", quality=70)
            b64_str = base64.b64encode(buffered.getvalue()).decode("utf-8")
            
            db.log_audit("AUTOMATION", f"Screenshot saved: {filename}", "AutomationController", "SUCCESS")
            
            return {
                "status": "success",
                "filename": filename,
                "filepath": str(filepath),
                "preview_base64": f"data:image/jpeg;base64,{b64_str}",
                "width": screenshot.width,
                "height": screenshot.height
            }
        except Exception as e:
            db.log_audit("AUTOMATION", "Screenshot capture failed", "AutomationController", "FAILED", str(e))
            return {"status": "error", "error": str(e)}

    @staticmethod
    def click(x: Optional[int] = None, y: Optional[int] = None, clicks: int = 1, button: str = "left") -> dict[str, Any]:
        """Moves cursor to (x, y) if specified and clicks."""
        try:
            import pyautogui
            if x is not None and y is not None:
                pyautogui.click(x=x, y=y, clicks=clicks, button=button)
            else:
                pyautogui.click(clicks=clicks, button=button)
            return {"status": "success", "action": f"click_{button}", "x": x, "y": y, "clicks": clicks}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    @staticmethod
    def type_text(text: str, interval: float = 0.02) -> dict[str, Any]:
        """Types string into currently focused application."""
        try:
            import pyautogui
            pyautogui.write(text, interval=interval)
            return {"status": "success", "typed_length": len(text)}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    @staticmethod
    def press_hotkey(keys: list[str]) -> dict[str, Any]:
        """Presses a hotkey combination (e.g. ['ctrl', 'c'], ['win', 'd'], ['alt', 'f4'])."""
        try:
            import pyautogui
            pyautogui.hotkey(*keys)
            return {"status": "success", "keys": keys}
        except Exception as e:
            return {"status": "error", "error": str(e)}

automation_controller = AutomationController()
