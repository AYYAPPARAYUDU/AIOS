from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Any
from backend.app.core.agents import agent_orchestrator
from backend.app.core.llm import ollama_client
from backend.app.storage.db import db

router = APIRouter(prefix="/api/agents", tags=["Multi-Agent System & LLM"])

class ChatRequest(BaseModel):
    message: str
    target_agent: Optional[str] = "supervisor"
    conversation_id: Optional[str] = "main_session"
    model: Optional[str] = None

class DispatchRequest(BaseModel):
    agent_id: str
    instruction: str
    conversation_id: Optional[str] = "main_session"

class ToolExecuteRequest(BaseModel):
    tool: str
    parameters: dict[str, Any]

@router.get("/status")
async def get_agents_status():
    return {
        "agents": agent_orchestrator.get_agents_status(),
        "recent_logs": agent_orchestrator.agent_logs[-15:],
        "llm_health": await ollama_client.check_health()
    }

@router.post("/chat")
async def chat_with_jarvis(req: ChatRequest):
    result = await agent_orchestrator.process_user_query(
        query=req.message,
        target_agent=req.target_agent or "supervisor",
        conversation_id=req.conversation_id or "main_session"
    )
    return result

@router.post("/dispatch")
async def dispatch_agent(req: DispatchRequest):
    result = await agent_orchestrator.process_user_query(
        query=req.instruction,
        target_agent=req.agent_id,
        conversation_id=req.conversation_id or "main_session"
    )
    return result

@router.post("/tool-call")
async def run_direct_tool(req: ToolExecuteRequest):
    res = await agent_orchestrator.execute_tool(req.tool, req.parameters)
    return {"tool": req.tool, "result": res}

@router.get("/messages")
async def get_conversation_history(conversation_id: str = "main_session", limit: int = 50):
    return {
        "conversation_id": conversation_id,
        "messages": db.get_messages(conversation_id, limit=limit)
    }

@router.get("/logs")
async def get_agent_logs():
    return {"logs": agent_orchestrator.agent_logs[-30:]}

@router.get("/memories")
async def list_memories(category: Optional[str] = None):
    return {"memories": db.list_memories(category=category)}
