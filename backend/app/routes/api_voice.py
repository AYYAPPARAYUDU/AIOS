from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from backend.app.voice.service import voice_service

router = APIRouter(prefix="/api/voice", tags=["Voice & Audio"])

class SpeakRequest(BaseModel):
    text: str
    rate: Optional[int] = 190

@router.post("/speak")
async def trigger_speech(req: SpeakRequest):
    return voice_service.speak_text_local(req.text, req.rate or 190)

@router.get("/profiles")
async def get_profiles():
    return {"profiles": voice_service.get_voice_profiles()}
