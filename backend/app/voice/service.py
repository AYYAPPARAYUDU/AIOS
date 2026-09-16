import os
import threading
from typing import Optional, Any
from backend.app.config import settings

class VoiceService:
    def __init__(self):
        self._lock = threading.Lock()

    def speak_text_local(self, text: str, voice_rate: int = 190) -> dict[str, Any]:
        """Uses local pyttsx3 engine to speak out text in a background thread."""
        def _worker():
            with self._lock:
                try:
                    import pyttsx3
                    engine = pyttsx3.init()
                    engine.setProperty('rate', voice_rate)
                    # Try to select British / male / robotic voice if available
                    voices = engine.getProperty('voices')
                    for v in voices:
                        if "david" in v.name.lower() or "george" in v.name.lower() or "jarvis" in v.name.lower():
                            engine.setProperty('voice', v.id)
                            break
                    engine.say(text)
                    engine.runAndWait()
                except Exception:
                    pass

        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        return {"status": "speaking", "text": text}

    def get_voice_profiles(self) -> list[dict[str, Any]]:
        """Lists available voice profiles."""
        profiles = [
            {"id": "jarvis_uk", "name": "JARVIS British Butler", "pitch": 0.95, "rate": 1.05, "lang": "en-GB"},
            {"id": "cyber_ai", "name": "Cybernetic Neural Core", "pitch": 0.85, "rate": 1.1, "lang": "en-US"},
            {"id": "iron_man", "name": "Stark Industries Arc AI", "pitch": 1.0, "rate": 1.0, "lang": "en-US"}
        ]
        return profiles

voice_service = VoiceService()
