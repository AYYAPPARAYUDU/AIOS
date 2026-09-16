import os
import io
import asyncio
import threading
import logging
from typing import Optional, Any
from backend.app.config import settings

logger = logging.getLogger(__name__)

class VoiceService:
    def __init__(self):
        self._lock = threading.Lock()
        self.default_lady_voice = "en-IN-NeerjaNeural"  # Expressive, clear celestial Indian English lady voice
        self.alternate_lady_voice = "en-US-AriaNeural"  # Universal crisp neural lady voice

    async def generate_speech_audio(self, text: str, voice: Optional[str] = None) -> bytes:
        """Synthesizes high-fidelity natural lady voice audio via edge-tts."""
        chosen_voice = voice or self.default_lady_voice
        try:
            import edge_tts
            communicate = edge_tts.Communicate(text, chosen_voice)
            audio_stream = io.BytesIO()
            async for chunk in communicate.stream():
                if chunk["type"] == "audio":
                    audio_stream.write(chunk["data"])
            audio_stream.seek(0)
            return audio_stream.read()
        except Exception as e:
            logger.warning(f"edge-tts error: {e}, falling back to alternate voice")
            try:
                import edge_tts
                communicate = edge_tts.Communicate(text, self.alternate_lady_voice)
                audio_stream = io.BytesIO()
                async for chunk in communicate.stream():
                    if chunk["type"] == "audio":
                        audio_stream.write(chunk["data"])
                audio_stream.seek(0)
                return audio_stream.read()
            except Exception as e2:
                logger.error(f"Neural TTS failed: {e2}")
                return b""

    def speak_text_local(self, text: str, voice_rate: int = 180) -> dict[str, Any]:
        """Offline fallback: uses pyttsx3 with Windows Zira / female voice."""
        def _worker():
            with self._lock:
                try:
                    import pyttsx3
                    engine = pyttsx3.init()
                    engine.setProperty('rate', voice_rate)
                    voices = engine.getProperty('voices')
                    # Select female voice (Zira, Hazel, Catherine, etc.)
                    for v in voices:
                        v_lower = v.name.lower()
                        if "zira" in v_lower or "female" in v_lower or "hazel" in v_lower or "catherine" in v_lower or "susan" in v_lower:
                            engine.setProperty('voice', v.id)
                            break
                    engine.say(text)
                    engine.runAndWait()
                except Exception as e:
                    logger.error(f"Local pyttsx3 failed: {e}")

        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        return {"status": "speaking", "text": text}

    def transcribe_audio_bytes(self, audio_bytes: bytes) -> dict[str, Any]:
        """Transcribes incoming audio file bytes via SpeechRecognition / Whisper."""
        try:
            import speech_recognition as sr
            r = sr.Recognizer()
            with io.BytesIO(audio_bytes) as audio_file:
                with sr.AudioFile(audio_file) as source:
                    audio_data = r.record(source)
                    text = r.recognize_google(audio_data)
                    return {"status": "success", "transcript": text}
        except Exception as e:
            return {"status": "error", "message": str(e), "transcript": ""}

    def get_voice_profiles(self) -> list[dict[str, Any]]:
        """Lists available voice profiles."""
        return [
            {"id": "en-IN-NeerjaNeural", "name": "Abhi Celestial Lady (Neerja)", "gender": "Female", "lang": "en-IN"},
            {"id": "en-US-AriaNeural", "name": "Saraswati Wisdom Lady (Aria)", "gender": "Female", "lang": "en-US"},
            {"id": "en-GB-SoniaNeural", "name": "Durga Sovereign Lady (Sonia)", "gender": "Female", "lang": "en-GB"},
            {"id": "en-US-JennyNeural", "name": "Lakshmi Radiant Lady (Jenny)", "gender": "Female", "lang": "en-US"}
        ]

voice_service = VoiceService()
