from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, Any, Dict

try:
    from backend.app.clone.clone_service import clone_service
    from backend.app.clone.evolution_analytics import evolution_analytics
    from backend.app.clone.perception_engine import perception_engine
    from backend.app.clone.continuous_learner import continuous_learner
except ImportError:
    from app.clone.clone_service import clone_service
    from app.clone.evolution_analytics import evolution_analytics
    from app.clone.perception_engine import perception_engine
    from app.clone.continuous_learner import continuous_learner

router = APIRouter(prefix="/api/clone", tags=["Self-Learning AI Clone"])

class CloneChatRequest(BaseModel):
    message: str
    conversation_id: Optional[str] = "clone_session"

class UpdateTraitRequest(BaseModel):
    key: str
    value: Any

class PerceptionTelemetryRequest(BaseModel):
    visual_data: Optional[Dict[str, Any]] = None
    audio_data: Optional[Dict[str, Any]] = None

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

@router.get("/evolution")
async def get_evolution_analytics():
    """Returns dynamic daily/monthly training curves, sector radar distribution, and loss convergence."""
    return evolution_analytics.get_evolution_metrics()

@router.post("/perceive")
async def process_perception(req: PerceptionTelemetryRequest):
    """Processes live camera & microphone telemetry to detect user emotion and mirror feelings."""
    res = perception_engine.process_telemetry(req.visual_data, req.audio_data)
    return res

@router.get("/perception-state")
async def get_perception_state():
    """Returns the current perception state and clone emotional mirror narrative."""
    return perception_engine.get_state()

@router.get("/learner-status")
async def get_continuous_learner_status():
    """Returns the live status of the continuous online micro-learning loop."""
    return continuous_learner.get_status()
