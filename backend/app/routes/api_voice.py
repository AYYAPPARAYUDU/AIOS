from fastapi import APIRouter, UploadFile, File, Response, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
import io
from backend.app.voice.service import voice_service

router = APIRouter(prefix="/api/voice", tags=["Voice & Audio"])

class SpeakRequest(BaseModel):
    text: str
    voice: Optional[str] = "en-IN-NeerjaNeural"
    rate: Optional[int] = 180

class TtsRequest(BaseModel):
    text: str
    voice: Optional[str] = "en-IN-NeerjaNeural"

@router.post("/tts")
async def generate_tts(req: TtsRequest):
    """Returns neural lady speech audio stream in MP3 format."""
    audio_bytes = await voice_service.generate_speech_audio(req.text, req.voice)
    if not audio_bytes:
        return Response(content=b"", status_code=500, media_type="text/plain")
    return StreamingResponse(io.BytesIO(audio_bytes), media_type="audio/mpeg")

@router.get("/tts")
async def generate_tts_get(text: str = Query(...), voice: str = Query(default="en-IN-NeerjaNeural")):
    """GET endpoint for easy direct audio player streaming."""
    audio_bytes = await voice_service.generate_speech_audio(text, voice)
    if not audio_bytes:
        return Response(content=b"", status_code=500, media_type="text/plain")
    return StreamingResponse(io.BytesIO(audio_bytes), media_type="audio/mpeg")

@router.post("/speak")
async def trigger_local_speech(req: SpeakRequest):
    """Plays speech on local hardware speaker using female voice."""
    return voice_service.speak_text_local(req.text, req.rate or 180)

@router.post("/stt")
async def speech_to_text(file: UploadFile = File(...)):
    """Transcribes uploaded audio into text."""
    content = await file.read()
    return voice_service.transcribe_audio_bytes(content)

@router.get("/profiles")
async def get_profiles():
    return {"profiles": voice_service.get_voice_profiles()}
