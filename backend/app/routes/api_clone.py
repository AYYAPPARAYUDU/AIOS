from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, Any
from backend.app.clone.clone_service import clone_service

router = APIRouter(prefix="/api/clone", tags=["Self-Learning AI Clone"])

class CloneChatRequest(BaseModel):
    message: str
    conversation_id: Optional[str] = "clone_session"

class UpdateTraitRequest(BaseModel):
    key: str
    value: Any

@router.get("/profile")
async def get_clone_profile():
    """Returns the continuous learning profile and evolution score of the clone."""
    return clone_service.get_profile()

@router.post("/chat")
async def chat_with_clone(req: CloneChatRequest):
    """Direct cognitive dialogue with the user's authentic AI Digital Clone."""
    result = await clone_service.generate_clone_response(
        user_query=req.message,
        conversation_id=req.conversation_id or "clone_session"
    )
    return result

@router.post("/reflect")
async def trigger_daily_reflection():
    """Forces an immediate daily self-evolution checkpoint."""
    res = await clone_service.trigger_reflection()
    return res
